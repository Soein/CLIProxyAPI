package watcher

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"golang.org/x/crypto/bcrypt"
)

func newManagementSecretWatcher(t *testing.T, section string) (*Watcher, string, *int) {
	t.Helper()
	dir := t.TempDir()
	authDir := filepath.Join(dir, "auths")
	if errMkdir := os.Mkdir(authDir, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	secret, errHash := bcrypt.GenerateFromPassword([]byte("previous-secret"), bcrypt.MinCost)
	if errHash != nil {
		t.Fatal(errHash)
	}
	path := filepath.Join(dir, "config.yaml")
	prefix := fmt.Sprintf("auth-dir: %q\nrequest-retry: 3\n", filepath.ToSlash(authDir))
	raw := prefix + fmt.Sprintf("%s:\n  allow-remote: true\n  secret-key: %q\n", section, secret)
	if errWrite := os.WriteFile(path, []byte(raw), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	cfg, errLoad := config.LoadConfig(path)
	if errLoad != nil {
		t.Fatal(errLoad)
	}
	calls := new(int)
	w := &Watcher{configPath: path, authDir: authDir, reloadCallback: func(*config.Config) { *calls++ }}
	w.SetConfig(cfg)
	w.lastConfigHash = cfg.SourceHash
	return w, prefix, calls
}

func TestReloadConfigPreservesActiveManagementDuringPartialWrites(t *testing.T) {
	for _, section := range []string{"remote-management", "management"} {
		t.Run(section, func(t *testing.T) {
			w, prefix, calls := newManagementSecretWatcher(t, section)
			previous, previousHash := w.config, w.lastConfigHash
			stages := []string{
				"",
				section + ":\n",
				section + ": {}\n",
				section + ":\n  allow-remote: true\n",
				section + ":\n  secret-key:",
				section + ":\n  secret-key:   \n",
				section + ":\n  secret-key: null\n",
				section + ":\n  secret-key: |\n",
				section + ":\n  secret-key: \"",
				section + ":\n  secret-key: new-secret-prefix",
				section + ":\n  secret-key: \"\"\n",
				section + ":\n  secret-key: \"new-key\"\n",
				section + ":\n  secret-key: |\n    new-key\n",
				section + ":\n  secret-key: >\n    new-key\n",
				section + ":\n  secret-key: new-key\n    continuation\n",
				section + ":\n  secret-key: \"\"\n---\n...\n",
			}
			for _, stage := range stages {
				raw := prefix + "debug: true\n" + stage
				if errWrite := os.WriteFile(w.configPath, []byte(raw), 0o600); errWrite != nil {
					t.Fatal(errWrite)
				}
				// Re-reading an unchanged prefix is not evidence that its writer finished.
				w.ReloadConfigIfChanged()
				w.ReloadConfigIfChanged()
				if w.config != previous || w.lastConfigHash != previousHash || *calls != 0 {
					t.Fatalf("partial %q changed live configuration: same config=%v, same hash=%v, callbacks=%d", stage, w.config == previous, w.lastConfigHash == previousHash, *calls)
				}
				got, errRead := os.ReadFile(w.configPath)
				if errRead != nil || string(got) != raw {
					t.Fatalf("reload rewrote a file being edited: %q, error=%v", got, errRead)
				}
			}
		})
	}
}

func TestReloadConfigAllowsExplicitManagementDisable(t *testing.T) {
	for _, section := range []string{"remote-management", "management"} {
		for _, empty := range []string{`""`, `''`} {
			t.Run(section+empty, func(t *testing.T) {
				w, prefix, calls := newManagementSecretWatcher(t, section)
				raw := prefix + "debug: true\n" + section + ":\n  secret-key: " + empty + "\n...\n"
				if errWrite := os.WriteFile(w.configPath, []byte(raw), 0o600); errWrite != nil {
					t.Fatal(errWrite)
				}
				w.ReloadConfigIfChanged()
				if w.config.RemoteManagement.SecretKey != "" || !w.config.Debug || *calls != 1 {
					t.Fatalf("explicit disable was not applied: secret present=%v, debug=%v, callbacks=%d", w.config.RemoteManagement.SecretKey != "", w.config.Debug, *calls)
				}
			})
		}
	}
}

func TestReloadConfigDoesNotRewritePlaintextWhileWriterContinues(t *testing.T) {
	for _, section := range []string{"remote-management", "management"} {
		t.Run(section, func(t *testing.T) {
			w, prefix, calls := newManagementSecretWatcher(t, section)
			f, errOpen := os.OpenFile(w.configPath, os.O_WRONLY|os.O_TRUNC, 0o600)
			if errOpen != nil {
				t.Fatal(errOpen)
			}
			t.Cleanup(func() { _ = f.Close() })
			raw := prefix + section + ":\n  allow-remote: true\n  secret-key: new-secret-prefix"
			if _, errWrite := f.WriteString(raw); errWrite != nil {
				t.Fatal(errWrite)
			}
			w.ReloadConfigIfChanged()
			if *calls != 0 {
				t.Fatal("unterminated plaintext key changed live configuration")
			}
			for i, suffix := range []string{"-complete\n", "debug: true\n", "...\n"} {
				if _, errWrite := f.WriteString(suffix); errWrite != nil {
					t.Fatal(errWrite)
				}
				raw += suffix
				w.ReloadConfigIfChanged()
				got, errRead := os.ReadFile(w.configPath)
				if errRead != nil || string(got) != raw {
					t.Fatalf("reload damaged an in-place write: %q, error=%v", got, errRead)
				}
				if i < 2 {
					if *calls != 0 {
						t.Fatal("a key change was applied before the document end")
					}
					continue
				}
				if errCompare := bcrypt.CompareHashAndPassword([]byte(w.config.RemoteManagement.SecretKey), []byte("new-secret-prefix-complete")); errCompare != nil {
					t.Fatalf("completed key was not applied: %v", errCompare)
				}
			}
			if !w.config.Debug || *calls != 1 {
				t.Fatalf("writer continuation was not applied: debug=%v, callbacks=%d", w.config.Debug, *calls)
			}
		})
	}
}

func TestReloadConfigWithoutActiveManagementAllowsOmittedSecret(t *testing.T) {
	for _, initial := range []*config.Config{nil, {RemoteManagement: config.RemoteManagement{AllowRemote: true}}} {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.yaml")
		if errWrite := os.WriteFile(path, []byte(fmt.Sprintf("auth-dir: %q\ndebug: true\n", filepath.ToSlash(dir))), 0o600); errWrite != nil {
			t.Fatal(errWrite)
		}
		calls := 0
		w := &Watcher{configPath: path, authDir: dir, reloadCallback: func(*config.Config) { calls++ }}
		w.SetConfig(initial)
		w.ReloadConfigIfChanged()
		if w.config == nil || !w.config.Debug || calls != 1 {
			t.Fatalf("no-secret configuration was rejected: config=%v, callbacks=%d", w.config, calls)
		}
	}
}

func TestReloadConfigRequiresDocumentEndToEnableManagement(t *testing.T) {
	for _, scalar := range []string{"new-key-prefix", "\"new-key\"\n", "|\n    new-key\n", ">\n    new-key\n", "new-key\n    continuation\n"} {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.yaml")
		calls := 0
		w := &Watcher{configPath: path, authDir: dir, reloadCallback: func(*config.Config) { calls++ }}
		w.SetConfig(&config.Config{AuthDir: dir})
		raw := fmt.Sprintf("auth-dir: %q\nmanagement:\n  secret-key: %s", filepath.ToSlash(dir), scalar)
		if errWrite := os.WriteFile(path, []byte(raw), 0o600); errWrite != nil {
			t.Fatal(errWrite)
		}
		w.ReloadConfigIfChanged()
		if calls != 0 || w.config.RemoteManagement.SecretKey != "" {
			t.Fatalf("unfinished document enabled management for %q", scalar)
		}
		if errWrite := os.WriteFile(path, []byte(raw+"\n...\n"), 0o600); errWrite != nil {
			t.Fatal(errWrite)
		}
		w.ReloadConfigIfChanged()
		if calls != 1 || w.config.RemoteManagement.SecretKey == "" {
			t.Fatalf("completed document did not enable management for %q", scalar)
		}
	}
}

func TestReloadConfigUnchangedPlaintextKeyDoesNotRequireDocumentEnd(t *testing.T) {
	w, prefix, calls := newManagementSecretWatcher(t, "management")
	previousSecret := w.config.RemoteManagement.SecretKey
	raw := prefix + "management:\n  allow-remote: true\n  secret-key: previous-secret\ndebug: true\n"
	if errWrite := os.WriteFile(w.configPath, []byte(raw), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	w.ReloadConfigIfChanged()
	if *calls != 1 || !w.config.Debug || w.config.RemoteManagement.SecretKey != previousSecret {
		t.Fatal("unchanged plaintext key was treated as a rotation")
	}
	if len(w.config.SourceSnapshot) != 0 {
		t.Fatal("source snapshot leaked into the live runtime configuration")
	}
	got, errRead := os.ReadFile(w.configPath)
	if errRead != nil || string(got) != raw {
		t.Fatalf("plaintext source was rewritten: %q, error=%v", got, errRead)
	}
}

func TestReloadConfigSourceIdentitySurvivesCallbackSetConfig(t *testing.T) {
	w, prefix, _ := newManagementSecretWatcher(t, "management")
	raw := prefix + fmt.Sprintf("management: {allow-remote: true, secret-key: %q}\ndebug: true\n", w.config.RemoteManagement.SecretKey)
	if errWrite := os.WriteFile(w.configPath, []byte(raw), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	w.reloadCallback = func(cfg *config.Config) {
		other := cfg.CloneForRuntime()
		other.SourceHash = "newer-service-snapshot"
		other.SourceSnapshot = []byte("newer-service-snapshot")
		w.SetConfig(other)
	}
	w.ReloadConfigIfChanged()
	if w.lastConfigHash != config.ComputeSourceHash([]byte(raw)) {
		t.Fatal("reload recorded a callback's replacement config as its parsed source")
	}
}

func TestReloadConfigRequiresDocumentEndForManagementRemoteAccessChange(t *testing.T) {
	for _, previousRemote := range []bool{true, false} {
		w, prefix, calls := newManagementSecretWatcher(t, "management")
		w.config.RemoteManagement.AllowRemote = previousRemote
		previous := w.config
		raw := prefix + fmt.Sprintf("management: {secret-key: %q, allow-remote: %t}\ndebug: true\n", previous.RemoteManagement.SecretKey, !previousRemote)
		if errWrite := os.WriteFile(w.configPath, []byte(raw), 0o600); errWrite != nil {
			t.Fatal(errWrite)
		}
		w.ReloadConfigIfChanged()
		if *calls != 0 || w.config != previous {
			t.Fatal("unfinished document changed remote management access")
		}
		if errWrite := os.WriteFile(w.configPath, []byte(raw+"...\n"), 0o600); errWrite != nil {
			t.Fatal(errWrite)
		}
		w.ReloadConfigIfChanged()
		if *calls != 1 || w.config.RemoteManagement.AllowRemote == previousRemote {
			t.Fatal("completed document did not change remote management access")
		}
	}
}
