// config_reload.go implements debounced configuration hot reload.
// It detects material changes and reloads clients when the config changes.
package watcher

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"reflect"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher/diff"
	"gopkg.in/yaml.v3"

	log "github.com/sirupsen/logrus"
)

var loadConfigFileFunc = config.LoadConfigForReload

type configReloadResult struct {
	sourceHash     string
	sourceSnapshot []byte
}

func (w *Watcher) stopConfigReloadTimer() {
	w.configReloadMu.Lock()
	if w.configReloadTimer != nil {
		w.configReloadTimer.Stop()
		w.configReloadTimer = nil
	}
	w.configReloadMu.Unlock()
}

func (w *Watcher) scheduleConfigReload() {
	w.configReloadMu.Lock()
	defer w.configReloadMu.Unlock()
	if w.configReloadTimer != nil {
		w.configReloadTimer.Stop()
	}
	w.configReloadTimer = time.AfterFunc(configReloadDebounce, func() {
		w.configReloadMu.Lock()
		w.configReloadTimer = nil
		w.configReloadMu.Unlock()
		w.reloadConfigIfChanged()
	})
}

// ReloadConfigIfChanged runs the same config reload path used by filesystem events.
func (w *Watcher) ReloadConfigIfChanged() {
	if w == nil {
		return
	}
	w.reloadConfigIfChanged()
}

func (w *Watcher) reloadConfigIfChanged() {
	// File events and post-save reloads may arrive together; an older read must
	// not apply after a newer one.
	w.configApplyMu.Lock()
	defer w.configApplyMu.Unlock()

	data, err := os.ReadFile(w.configPath)
	if err != nil {
		log.Errorf("failed to read config file for hash check: %v", err)
		return
	}
	if len(data) == 0 {
		log.Debugf("ignoring empty config file write event")
		return
	}
	sum := sha256.Sum256(data)
	newHash := hex.EncodeToString(sum[:])

	w.clientsMutex.RLock()
	currentHash := w.lastConfigHash
	w.clientsMutex.RUnlock()

	if currentHash != "" && currentHash == newHash {
		log.Debugf("config file content unchanged (hash match), skipping reload")
		return
	}
	log.Infof("config file changed, reloading: %s", w.configPath)
	if applied := w.reloadConfigSnapshot(); applied != nil {
		// Record the bytes the reload actually parsed, not the change-check read or a
		// later re-read: a write landing between them must not count as applied.
		w.clientsMutex.Lock()
		appliedHash := newHash
		if applied.sourceHash != "" {
			appliedHash = applied.sourceHash
		}
		w.lastConfigHash = appliedHash
		w.clientsMutex.Unlock()
		w.persistConfigAsync(applied.sourceSnapshot)
	}
}

func (w *Watcher) reloadConfig() bool {
	return w.reloadConfigSnapshot() != nil
}

func (w *Watcher) reloadConfigSnapshot() *configReloadResult {
	log.Debug("=========================== CONFIG RELOAD ============================")
	log.Debugf("starting config reload from: %s", w.configPath)

	w.clientsMutex.RLock()
	previousSecret := ""
	if w.config != nil {
		previousSecret = w.config.RemoteManagement.SecretKey
	}
	w.clientsMutex.RUnlock()
	newConfig, errLoadConfig := loadConfigFileFunc(w.configPath, previousSecret)
	if errLoadConfig != nil {
		log.Errorf("failed to reload config: %v", errLoadConfig)
		return nil
	}
	// A save may truncate the file after the change check. Use the bytes the
	// loader actually parsed, before replacing live config or auth directories.
	if newConfig.SourceHash == config.ComputeSourceHash(nil) {
		log.Debug("ignoring config truncated during reload")
		return nil
	}
	result := &configReloadResult{sourceHash: newConfig.SourceHash, sourceSnapshot: newConfig.SourceSnapshot}
	// Source bytes belong to this publication and its one persistence request,
	// not to long-lived runtime configs and their cloned snapshots.
	newConfig.SourceSnapshot = nil

	if w.mirroredAuthDir != "" {
		newConfig.AuthDir = w.mirroredAuthDir
	} else {
		if resolvedAuthDir, errResolveAuthDir := util.ResolveAuthDir(newConfig.AuthDir); errResolveAuthDir != nil {
			log.Errorf("failed to resolve auth directory from config: %v", errResolveAuthDir)
		} else {
			newConfig.AuthDir = resolvedAuthDir
		}
	}

	w.clientsMutex.Lock()
	if w.config != nil && w.config.RemoteManagement.SecretKey != "" && newConfig.RemoteManagement.SecretKey == "" && !newConfig.ManagementSecretKeyExplicitlyEmpty {
		w.clientsMutex.Unlock()
		log.Warn("ignoring config reload without an explicit empty management key; use secret-key: \"\" followed by a YAML document end to disable management")
		return nil
	}
	currentSecret := ""
	if w.config != nil {
		currentSecret = w.config.RemoteManagement.SecretKey
	}
	if currentSecret != newConfig.RemoteManagement.SecretKey && !newConfig.SourceHasDocumentEnd {
		w.clientsMutex.Unlock()
		log.Warn("ignoring management key change without a final YAML document end (...)")
		return nil
	}
	if w.config != nil && (currentSecret != "" || newConfig.RemoteManagement.SecretKey != "") && w.config.RemoteManagement.AllowRemote != newConfig.RemoteManagement.AllowRemote && !newConfig.SourceHasDocumentEnd {
		w.clientsMutex.Unlock()
		log.Warn("ignoring remote management access change without a final YAML document end (...)")
		return nil
	}
	var oldConfig *config.Config
	_ = yaml.Unmarshal(w.oldConfigYaml, &oldConfig)
	w.oldConfigYaml, _ = yaml.Marshal(newConfig)
	w.config = newConfig
	w.clientsMutex.Unlock()

	var affectedOAuthProviders []string
	if oldConfig != nil {
		_, affectedOAuthProviders = diff.DiffOAuthExcludedModelChanges(oldConfig.OAuthExcludedModels, newConfig.OAuthExcludedModels)
	}

	util.SetLogLevel(newConfig)
	if oldConfig != nil && oldConfig.Debug != newConfig.Debug {
		log.Debugf("log level updated - debug mode changed from %t to %t", oldConfig.Debug, newConfig.Debug)
	}

	if oldConfig != nil {
		details := diff.BuildConfigChangeDetails(oldConfig, newConfig)
		if len(details) > 0 {
			log.Info("config changes detected:")
			for _, d := range details {
				log.Infof("  %s", d)
			}
		} else {
			log.Debugf("no material config field changes detected")
		}
	}

	authDirChanged := oldConfig == nil || oldConfig.AuthDir != newConfig.AuthDir
	retryConfigChanged := oldConfig != nil && (oldConfig.RequestRetry != newConfig.RequestRetry || oldConfig.MaxRetryInterval != newConfig.MaxRetryInterval || oldConfig.MaxRetryCredentials != newConfig.MaxRetryCredentials)
	forceAuthRefresh := oldConfig != nil && (oldConfig.ForceModelPrefix != newConfig.ForceModelPrefix || !reflect.DeepEqual(oldConfig.OAuthModelAlias, newConfig.OAuthModelAlias) || !reflect.DeepEqual(oldConfig.OAuthSettings, newConfig.OAuthSettings) || retryConfigChanged)

	log.Infof("config successfully reloaded, triggering client reload")
	w.reloadClients(authDirChanged, affectedOAuthProviders, forceAuthRefresh)
	return result
}
