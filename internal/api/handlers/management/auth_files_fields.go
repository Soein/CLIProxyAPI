package management

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/authfilelock"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/credentialweight"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v7/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

var errAuthFileChanged = errors.New("auth file changed concurrently")

// PatchAuthFileStatus toggles the disabled state of an auth file
func (h *Handler) PatchAuthFileStatus(c *gin.Context) {
	if h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}

	var req struct {
		Name      string   `json:"name"`
		Names     []string `json:"names"`
		AuthIndex string   `json:"auth_index"`
		Disabled  *bool    `json:"disabled"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	name := strings.TrimSpace(req.Name)
	authIndex := strings.TrimSpace(req.AuthIndex)
	names := make([]string, 0, len(req.Names)+1)
	seenNames := make(map[string]struct{}, len(req.Names)+1)
	for _, candidate := range req.Names {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if _, exists := seenNames[candidate]; exists {
			continue
		}
		seenNames[candidate] = struct{}{}
		names = append(names, candidate)
	}
	if name != "" && len(names) > 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name and names are mutually exclusive"})
		return
	}
	if name != "" {
		names = append(names, name)
	}
	if len(names) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name or names is required"})
		return
	}
	if authIndex != "" && name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "auth_index requires name"})
		return
	}
	if req.Disabled == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "disabled is required"})
		return
	}

	ctx := c.Request.Context()
	auths := h.authManager.List()
	authByID := make(map[string]*coreauth.Auth, len(auths))
	authByFileName := make(map[string]*coreauth.Auth, len(auths))
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		if id := strings.TrimSpace(auth.ID); id != "" {
			authByID[id] = auth
		}
		if fileName := strings.TrimSpace(auth.FileName); fileName != "" {
			if _, exists := authByFileName[fileName]; !exists {
				authByFileName[fileName] = auth
			}
		}
	}
	resolved := make([]*coreauth.Auth, 0, len(names))
	for _, targetName := range names {
		var targetAuth *coreauth.Auth
		if authIndex != "" {
			targetAuth, _ = h.lookupAuthFile(targetName, authIndex)
		} else {
			targetAuth = authByID[targetName]
			if targetAuth == nil {
				targetAuth = authByFileName[targetName]
			}
		}
		if targetAuth == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("auth file not found: %s", targetName)})
			return
		}
		if coreauth.IsPluginVirtualAuth(targetAuth) && !isPluginVirtualSourceDelete(targetName, targetAuth) {
			c.JSON(http.StatusConflict, gin.H{"error": errPluginVirtualAuth.Error()})
			return
		}
		resolved = append(resolved, targetAuth)
	}

	var configTarget *coreauth.Auth
	for _, targetAuth := range resolved {
		if coreauth.IsConfigAPIKeyAuth(targetAuth) {
			configTarget = targetAuth
			break
		}
	}
	if configTarget != nil && len(resolved) > 1 {
		c.JSON(http.StatusConflict, gin.H{"error": "config api key cannot be updated in a multi-auth request"})
		return
	}
	if configTarget != nil {
		h.mu.Lock()
		handled, errToggle := toggleConfigAPIKeyExcludedAll(h.cfg, configTarget, *req.Disabled)
		if errToggle != nil {
			h.mu.Unlock()
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to update config api key: %v", errToggle)})
			return
		}
		if !handled {
			h.mu.Unlock()
			c.JSON(http.StatusNotFound, gin.H{"error": "config api key entry not found"})
			return
		}
		cfgSnapshot, okSnapshot := h.saveConfigAndSnapshotLocked(c)
		h.mu.Unlock()
		if !okSnapshot {
			return
		}
		h.reloadConfigAfterManagementSave(ctx, cfgSnapshot)
		if h.tokenStore != nil {
			_ = h.tokenStore.Delete(ctx, configTarget.ID)
		}
		c.JSON(http.StatusOK, gin.H{
			"status":           "ok",
			"disabled":         *req.Disabled,
			"via":              "config:excluded-models",
			"excluded_pattern": configAPIKeyDisablePattern,
		})
		return
	}

	sourcePaths := make(map[string]string)
	runtimeIDs := make([]string, 0, len(resolved))
	seenRuntimeIDs := make(map[string]struct{}, len(resolved))
	addRuntimeID := func(id string) {
		id = strings.TrimSpace(id)
		if id == "" {
			return
		}
		if _, exists := seenRuntimeIDs[id]; exists {
			return
		}
		seenRuntimeIDs[id] = struct{}{}
		runtimeIDs = append(runtimeIDs, id)
	}
	for _, targetAuth := range resolved {
		if !coreauth.IsPluginVirtualAuth(targetAuth) {
			addRuntimeID(targetAuth.ID)
			continue
		}
		sourcePath := strings.TrimSpace(authAttribute(targetAuth, coreauth.AttributeVirtualSource))
		if sourcePath == "" {
			sourcePath = strings.TrimSpace(authAttribute(targetAuth, "path"))
		}
		if sourcePath == "" {
			c.JSON(http.StatusConflict, gin.H{"error": errPluginVirtualAuth.Error()})
			return
		}
		sourceKey := authFilePathKey(sourcePath)
		if sourceKey == "" {
			c.JSON(http.StatusConflict, gin.H{"error": errPluginVirtualAuth.Error()})
			return
		}
		sourcePaths[sourceKey] = sourcePath
	}
	if len(sourcePaths) > 0 {
		for _, auth := range auths {
			if auth == nil {
				continue
			}
			_, pathMatch := sourcePaths[authFilePathKey(authAttribute(auth, "path"))]
			_, virtualSourceMatch := sourcePaths[authFilePathKey(authAttribute(auth, coreauth.AttributeVirtualSource))]
			if pathMatch || virtualSourceMatch {
				addRuntimeID(auth.ID)
			}
		}
	}
	sourceKeys := make([]string, 0, len(sourcePaths))
	for sourceKey := range sourcePaths {
		sourceKeys = append(sourceKeys, sourceKey)
	}
	sort.Strings(sourceKeys)
	sourceLockPaths := make([]string, 0, len(sourceKeys))
	for _, sourceKey := range sourceKeys {
		sourceLockPaths = append(sourceLockPaths, sourcePaths[sourceKey])
	}
	unlockSources := authfilelock.Lock(sourceLockPaths...)
	sourceLocksHeld := true
	releaseSourceLocks := func() {
		if !sourceLocksHeld {
			return
		}
		unlockSources()
		sourceLocksHeld = false
	}
	defer releaseSourceLocks()

	preparedSources := make(map[string]preparedSourceAuthFile, len(sourceKeys))
	for _, sourceKey := range sourceKeys {
		sourcePath := sourcePaths[sourceKey]
		original, updatedRaw, errPrepare := prepareSourceAuthFileDisabled(sourcePath, *req.Disabled)
		if errPrepare != nil {
			status := http.StatusInternalServerError
			if os.IsNotExist(errPrepare) {
				status = http.StatusNotFound
			}
			c.JSON(status, gin.H{"error": fmt.Sprintf("failed to update source auth file: %v", errPrepare)})
			return
		}
		preparedSources[sourceKey] = preparedSourceAuthFile{path: sourcePath, original: original, updated: updatedRaw}
	}
	writtenSourceKeys := make([]string, 0, len(sourceKeys))
	rollbackSources := func(failClosed bool) error {
		rollbackErrors := make([]error, 0)
		for index := len(writtenSourceKeys) - 1; index >= 0; index-- {
			sourceKey := writtenSourceKeys[index]
			prepared := preparedSources[sourceKey]
			errRollback := writeSourceAuthFileAtomicallyIfUnchanged(prepared.path, prepared.updated, prepared.original)
			if failClosed && errors.Is(errRollback, errAuthFileChanged) {
				errRollback = writeLatestSourceAuthFileDisabled(prepared.path)
			}
			if errRollback != nil {
				rollbackErrors = append(rollbackErrors, fmt.Errorf("restore source auth file %q: %w", prepared.path, errRollback))
			}
		}
		return errors.Join(rollbackErrors...)
	}
	for _, sourceKey := range sourceKeys {
		prepared := preparedSources[sourceKey]
		if errWrite := writeSourceAuthFileAtomicallyIfUnchanged(prepared.path, prepared.original, prepared.updated); errWrite != nil {
			errWrite = errors.Join(errWrite, rollbackSources(false))
			status := http.StatusInternalServerError
			if errors.Is(errWrite, errAuthFileChanged) {
				status = http.StatusConflict
			} else if os.IsNotExist(errWrite) {
				status = http.StatusNotFound
			}
			c.JSON(status, gin.H{"error": fmt.Sprintf("failed to update source auth file: %v", errWrite)})
			return
		}
		writtenSourceKeys = append(writtenSourceKeys, sourceKey)
	}
	releaseSourceLocks()

	updated, errSetDisabled := h.authManager.SetDisabled(ctx, runtimeIDs, *req.Disabled)
	if errSetDisabled != nil {
		if !*req.Disabled {
			unlockRollback := authfilelock.Lock(sourceLockPaths...)
			errSourceRollback := rollbackSources(true)
			unlockRollback()

			if errors.Is(errSetDisabled, coreauth.ErrAuthStoreCommitUnknown) {
				compensationBase := context.Background()
				if ctx != nil {
					compensationBase = context.WithoutCancel(ctx)
				}
				compensationCtx, cancelCompensation := context.WithTimeout(compensationBase, authStatusCompensationTimeout)
				_, errCompensate := h.authManager.SetDisabled(compensationCtx, runtimeIDs, true)
				if errCompensate == nil {
					if _, okDisabled := authsAtDisabledState(h.authManager, runtimeIDs, true); !okDisabled {
						errCompensate = errors.New("fail-closed compensation did not converge to disabled state")
					}
				}
				cancelCompensation()
				if errCompensate != nil {
					errCompensate = fmt.Errorf("fail-closed compensation: %w", errCompensate)
				}
				errSetDisabled = errors.Join(errSetDisabled, errSourceRollback, errCompensate)
				if errSourceRollback != nil || errCompensate != nil {
					c.JSON(http.StatusInternalServerError, gin.H{
						"error": "failed to persist auth status; credential state could not be confirmed: " + errSetDisabled.Error(),
					})
					return
				}
			} else {
				errSetDisabled = errors.Join(errSetDisabled, errSourceRollback)
			}
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to persist auth status; affected credentials remain disabled: " + errSetDisabled.Error(),
		})
		return
	}

	response := gin.H{"status": "ok", "disabled": *req.Disabled}
	if len(names) > 1 {
		response["updated"] = len(updated)
	}
	c.JSON(http.StatusOK, response)
}

func authsAtDisabledState(manager *coreauth.Manager, ids []string, disabled bool) ([]*coreauth.Auth, bool) {
	if manager == nil || len(ids) == 0 {
		return nil, false
	}
	authByID := make(map[string]*coreauth.Auth)
	for _, auth := range manager.List() {
		if auth != nil {
			authByID[auth.ID] = auth
		}
	}
	resolved := make([]*coreauth.Auth, 0, len(ids))
	for _, id := range ids {
		auth := authByID[id]
		if auth == nil || auth.Disabled != disabled {
			return nil, false
		}
		if disabled {
			if auth.Status != coreauth.StatusDisabled {
				return nil, false
			}
		} else if auth.Status != coreauth.StatusActive {
			return nil, false
		}
		resolved = append(resolved, auth)
	}
	return resolved, true
}

type preparedSourceAuthFile struct {
	path     string
	original []byte
	updated  []byte
}

func prepareSourceAuthFileDisabled(path string, disabled bool) ([]byte, []byte, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil, fmt.Errorf("source auth path is empty")
	}
	data, errRead := os.ReadFile(path)
	if errRead != nil {
		return nil, nil, errRead
	}
	metadata := make(map[string]any)
	if len(bytes.TrimSpace(data)) > 0 {
		if errUnmarshal := json.Unmarshal(data, &metadata); errUnmarshal != nil {
			return nil, nil, fmt.Errorf("invalid auth file: %w", errUnmarshal)
		}
	}
	if metadata == nil {
		metadata = make(map[string]any)
	}
	metadata["disabled"] = disabled
	raw, errMarshal := json.Marshal(metadata)
	if errMarshal != nil {
		return nil, nil, fmt.Errorf("marshal auth file: %w", errMarshal)
	}
	return data, raw, nil
}

func writeLatestSourceAuthFileDisabled(path string) error {
	current, disabledRaw, errPrepare := prepareSourceAuthFileDisabled(path, true)
	if errPrepare != nil {
		return errPrepare
	}
	return writeSourceAuthFileAtomicallyIfUnchanged(path, current, disabledRaw)
}

func writeSourceAuthFileAtomicallyIfUnchanged(path string, expected, raw []byte) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("source auth path is empty")
	}
	tmp, errCreate := os.CreateTemp(filepath.Dir(path), ".auth-status-*.tmp")
	if errCreate != nil {
		return errCreate
	}
	tmpPath := tmp.Name()
	defer func() {
		if errRemove := os.Remove(tmpPath); errRemove != nil && !os.IsNotExist(errRemove) {
			log.WithError(errRemove).Warn("failed to remove temporary auth status file")
		}
	}()
	closeWithError := func(cause error) error {
		if errClose := tmp.Close(); errClose != nil {
			return errors.Join(cause, fmt.Errorf("close temporary auth status file: %w", errClose))
		}
		return cause
	}
	if errChmod := tmp.Chmod(0o600); errChmod != nil {
		return closeWithError(errChmod)
	}
	if _, errWrite := tmp.Write(raw); errWrite != nil {
		return closeWithError(errWrite)
	}
	if errSync := tmp.Sync(); errSync != nil {
		return closeWithError(errSync)
	}
	if errClose := tmp.Close(); errClose != nil {
		return errClose
	}
	current, errRead := os.ReadFile(path)
	if errRead != nil {
		return errRead
	}
	if !bytes.Equal(current, expected) {
		return fmt.Errorf("%w: %s", errAuthFileChanged, path)
	}
	if errRename := os.Rename(tmpPath, path); errRename != nil {
		return errRename
	}
	return nil
}

func applyAuthDisabledState(auth *coreauth.Auth, disabled bool) {
	if auth == nil {
		return
	}
	auth.Disabled = disabled
	if disabled {
		auth.Status = coreauth.StatusDisabled
		auth.StatusMessage = "disabled via management API"
	} else {
		auth.Status = coreauth.StatusActive
		auth.StatusMessage = ""
	}
	auth.UpdatedAt = time.Now()
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	auth.Metadata["disabled"] = disabled
}

// PatchAuthFileFields updates arbitrary metadata fields of an auth file.
func (h *Handler) PatchAuthFileFields(c *gin.Context) {
	if h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}

	var req map[string]json.RawMessage
	decoder := json.NewDecoder(c.Request.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	nameRaw, ok := req["name"]
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	var nameValue string
	if err := json.Unmarshal(nameRaw, &nameValue); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	name := strings.TrimSpace(nameValue)
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	delete(req, "name")

	ctx := c.Request.Context()

	// Find auth by name or ID
	var targetAuth *coreauth.Auth
	if auth, ok := h.authManager.GetByID(name); ok {
		targetAuth = auth
	} else {
		auths := h.authManager.List()
		for _, auth := range auths {
			if auth.FileName == name {
				targetAuth = auth
				break
			}
		}
	}

	if targetAuth == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "auth file not found"})
		return
	}
	if coreauth.IsPluginVirtualAuth(targetAuth) {
		c.JSON(http.StatusConflict, gin.H{"error": errPluginVirtualAuth.Error()})
		return
	}

	changed := false
	touchedRoots := make(map[string]struct{}, len(req))
	for key, rawValue := range req {
		fieldPath := strings.TrimSpace(key)
		if fieldPath == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "field name is required"})
			return
		}
		value, errDecode := decodeAuthFileFieldValue(rawValue)
		if errDecode != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid field %s", fieldPath)})
			return
		}
		if targetAuth.Metadata == nil {
			targetAuth.Metadata = make(map[string]any)
		}

		if fieldPath == coreauth.AttributeWeight {
			if value == nil {
				delete(targetAuth.Metadata, coreauth.AttributeWeight)
			} else {
				if _, okNumber := value.(json.Number); !okNumber {
					c.JSON(http.StatusBadRequest, gin.H{"error": "weight must be an integer"})
					return
				}
				weight, errWeight := credentialweight.ParseValue(value)
				if errWeight != nil {
					c.JSON(http.StatusBadRequest, gin.H{"error": errWeight.Error()})
					return
				}
				targetAuth.Metadata[coreauth.AttributeWeight] = weight
			}
		} else if rootAuthFileField(fieldPath) == coreauth.AttributeWeight {
			c.JSON(http.StatusBadRequest, gin.H{"error": "weight does not support nested fields"})
			return
		} else if fieldPath == "headers" {
			applyAuthFileHeadersPatch(targetAuth, value)
		} else if errSet := setAuthFileMetadataValue(targetAuth.Metadata, fieldPath, value); errSet != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errSet.Error()})
			return
		}
		if root := rootAuthFileField(fieldPath); root != "" {
			touchedRoots[root] = struct{}{}
		}
		changed = true
	}
	if changed {
		syncAuthFileMetadataFields(targetAuth, touchedRoots)
	}

	if !changed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no fields to update"})
		return
	}

	targetAuth.UpdatedAt = time.Now()

	if _, err := h.authManager.Update(ctx, targetAuth); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to update auth: %v", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func decodeAuthFileFieldValue(raw json.RawMessage) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func rootAuthFileField(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	if idx := strings.Index(path, "."); idx >= 0 {
		return strings.TrimSpace(path[:idx])
	}
	return path
}

func setAuthFileMetadataValue(metadata map[string]any, path string, value any) error {
	if metadata == nil {
		return fmt.Errorf("metadata is nil")
	}
	parts := strings.Split(path, ".")
	current := metadata
	for i, rawPart := range parts {
		part := strings.TrimSpace(rawPart)
		if part == "" {
			return fmt.Errorf("invalid field path: %s", path)
		}
		if i == len(parts)-1 {
			current[part] = value
			return nil
		}
		next, ok := current[part].(map[string]any)
		if !ok {
			next = make(map[string]any)
			current[part] = next
		}
		current = next
	}
	return nil
}

func applyAuthFileHeadersPatch(auth *coreauth.Auth, value any) {
	if auth == nil {
		return
	}
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	headersPatch, ok := authFileHeadersStringMap(value)
	if !ok {
		auth.Metadata["headers"] = value
		return
	}

	existingHeaders := coreauth.ExtractCustomHeadersFromMetadata(auth.Metadata)
	nextHeaders := make(map[string]string, len(existingHeaders))
	for key, val := range existingHeaders {
		nextHeaders[key] = val
	}
	for key, value := range headersPatch {
		name := strings.TrimSpace(key)
		if name == "" {
			continue
		}
		val := strings.TrimSpace(value)
		if val == "" {
			delete(nextHeaders, name)
			continue
		}
		nextHeaders[name] = val
	}

	if len(nextHeaders) == 0 {
		delete(auth.Metadata, "headers")
		return
	}
	metaHeaders := make(map[string]any, len(nextHeaders))
	for key, value := range nextHeaders {
		metaHeaders[key] = value
	}
	auth.Metadata["headers"] = metaHeaders
}

func authFileHeadersStringMap(value any) (map[string]string, bool) {
	switch typed := value.(type) {
	case map[string]string:
		return typed, true
	case map[string]any:
		out := make(map[string]string, len(typed))
		for key, rawValue := range typed {
			value, ok := rawValue.(string)
			if !ok {
				return nil, false
			}
			out[key] = value
		}
		return out, true
	default:
		return nil, false
	}
}

func syncAuthFileMetadataFields(auth *coreauth.Auth, touchedRoots map[string]struct{}) {
	if auth == nil || len(touchedRoots) == 0 {
		return
	}
	if _, ok := touchedRoots["prefix"]; ok {
		if prefix, okString := auth.Metadata["prefix"].(string); okString {
			auth.Prefix = strings.TrimSpace(prefix)
		}
	}
	if _, ok := touchedRoots["proxy_url"]; ok {
		if proxyURL, okString := auth.Metadata["proxy_url"].(string); okString {
			auth.ProxyURL = strings.TrimSpace(proxyURL)
		}
	}
	if _, ok := touchedRoots["headers"]; ok {
		syncAuthFileHeaderAttributes(auth)
	}
	if _, ok := touchedRoots["priority"]; ok {
		syncAuthFilePriorityAttribute(auth)
	}
	if _, ok := touchedRoots[coreauth.AttributeWeight]; ok {
		syncAuthFileWeightAttribute(auth)
	}
	if _, ok := touchedRoots["note"]; ok {
		syncAuthFileNoteAttribute(auth)
	}
	if _, ok := touchedRoots["websockets"]; ok {
		syncAuthFileWebsocketsAttribute(auth)
	}
	if _, ok := touchedRoots["disabled"]; ok {
		syncAuthFileDisabledState(auth)
	}
}

func syncAuthFileHeaderAttributes(auth *coreauth.Auth) {
	if auth == nil {
		return
	}
	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	for key := range auth.Attributes {
		if strings.HasPrefix(key, "header:") {
			delete(auth.Attributes, key)
		}
	}
	for name, value := range coreauth.ExtractCustomHeadersFromMetadata(auth.Metadata) {
		auth.Attributes["header:"+name] = value
	}
}

func syncAuthFilePriorityAttribute(auth *coreauth.Auth) {
	if auth == nil {
		return
	}
	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	priority, ok := authFileIntValue(auth.Metadata["priority"])
	if !ok {
		delete(auth.Attributes, "priority")
		return
	}
	if priority == 0 {
		delete(auth.Attributes, "priority")
		return
	}
	auth.Attributes["priority"] = strconv.Itoa(priority)
}

func syncAuthFileWeightAttribute(auth *coreauth.Auth) {
	if auth == nil {
		return
	}
	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	weight, errWeight := credentialweight.ParseValue(auth.Metadata[coreauth.AttributeWeight])
	if errWeight != nil {
		delete(auth.Attributes, coreauth.AttributeWeight)
		return
	}
	auth.Attributes[coreauth.AttributeWeight] = strconv.FormatInt(weight, 10)
}

func authFileIntValue(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	case json.Number:
		if i, err := typed.Int64(); err == nil {
			return int(i), true
		}
	case string:
		if i, err := strconv.Atoi(strings.TrimSpace(typed)); err == nil {
			return i, true
		}
	}
	return 0, false
}

func syncAuthFileNoteAttribute(auth *coreauth.Auth) {
	if auth == nil {
		return
	}
	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	note, ok := auth.Metadata["note"].(string)
	if !ok {
		delete(auth.Attributes, "note")
		return
	}
	note = strings.TrimSpace(note)
	if note == "" {
		delete(auth.Attributes, "note")
		return
	}
	auth.Attributes["note"] = note
}

func syncAuthFileWebsocketsAttribute(auth *coreauth.Auth) {
	if auth == nil {
		return
	}
	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	websockets, ok := authFileBoolValue(auth.Metadata["websockets"])
	if !ok {
		delete(auth.Attributes, "websockets")
		return
	}
	auth.Attributes["websockets"] = strconv.FormatBool(websockets)
}

func authFileBoolValue(value any) (bool, bool) {
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		parsed, errParse := strconv.ParseBool(strings.TrimSpace(typed))
		if errParse == nil {
			return parsed, true
		}
	}
	return false, false
}

func syncAuthFileDisabledState(auth *coreauth.Auth) {
	if auth == nil {
		return
	}
	disabled, ok := authFileBoolValue(auth.Metadata["disabled"])
	if !ok {
		return
	}
	auth.Disabled = disabled
	if disabled {
		auth.Status = coreauth.StatusDisabled
		if strings.TrimSpace(auth.StatusMessage) == "" {
			auth.StatusMessage = "disabled via management API"
		}
		return
	}
	auth.Status = coreauth.StatusActive
	auth.StatusMessage = ""
}

func (h *Handler) authIDsForPath(path string, fallbackID string) []string {
	if h == nil || h.authManager == nil {
		return nil
	}
	ids := make([]string, 0, 1)
	seen := make(map[string]struct{})
	add := func(id string) {
		id = strings.TrimSpace(id)
		if id == "" {
			return
		}
		if _, exists := seen[id]; exists {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	for _, auth := range h.authManager.List() {
		if auth == nil {
			continue
		}
		if sameAuthFilePath(authAttribute(auth, "path"), path) || sameAuthFilePath(authAttribute(auth, coreauth.AttributeVirtualSource), path) {
			add(auth.ID)
		}
	}
	if len(ids) > 0 {
		return ids
	}
	if strings.TrimSpace(fallbackID) != "" {
		if _, ok := h.authManager.GetByID(fallbackID); ok {
			add(fallbackID)
			return ids
		}
	}
	authID := h.authIDForPath(path)
	if _, ok := h.authManager.GetByID(authID); ok {
		add(authID)
	}
	return ids
}

func sameAuthFilePath(left, right string) bool {
	left = cleanAuthFilePath(left)
	right = cleanAuthFilePath(right)
	if left == "" || right == "" {
		return false
	}
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}

func authFilePathKey(path string) string {
	path = cleanAuthFilePath(path)
	if runtime.GOOS == "windows" {
		path = strings.ToLower(path)
	}
	return path
}

func cleanAuthFilePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	if abs, errAbs := filepath.Abs(path); errAbs == nil && strings.TrimSpace(abs) != "" {
		path = abs
	}
	return filepath.Clean(path)
}

func (h *Handler) deleteTokenRecord(ctx context.Context, path string, expectedGeneration uint64) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("auth path is empty")
	}
	store := h.tokenStoreWithBaseDir()
	if store == nil {
		return fmt.Errorf("token store unavailable")
	}
	if tombstoneStore, ok := store.(coreauth.AuthTombstoneStore); ok {
		_, errTombstone := tombstoneStore.Tombstone(ctx, path, expectedGeneration)
		return errTombstone
	}
	return store.Delete(ctx, path)
}

func (h *Handler) deleteTokenRecordAndAuths(ctx context.Context, path string, fallbackID string) error {
	if h == nil || h.authManager == nil {
		expectedGeneration, errGeneration := h.authStoreGenerationForDelete(ctx, path, fallbackID, nil)
		if errGeneration != nil {
			return errGeneration
		}
		return h.deleteTokenRecord(ctx, path, expectedGeneration)
	}
	ids := h.authIDsForPath(path, fallbackID)
	expectedGeneration, errGeneration := h.authStoreGenerationForDelete(ctx, path, fallbackID, ids)
	if errGeneration != nil {
		return errGeneration
	}
	return h.authManager.DeleteAuths(ctx, ids, func(deleteCtx context.Context) error {
		return h.deleteTokenRecord(deleteCtx, path, expectedGeneration)
	})
}

func (h *Handler) authStoreGenerationForDelete(ctx context.Context, path, fallbackID string, ids []string) (uint64, error) {
	var expected uint64
	merge := func(generation uint64) error {
		if generation == 0 {
			return nil
		}
		if expected != 0 && expected != generation {
			return fmt.Errorf("auth generation changed while preparing delete: %d != %d", expected, generation)
		}
		expected = generation
		return nil
	}
	if h != nil && h.authManager != nil {
		for _, id := range ids {
			if auth, ok := h.authManager.GetByID(id); ok && auth != nil {
				if errMerge := merge(auth.StoreGeneration()); errMerge != nil {
					return 0, errMerge
				}
			}
		}
	}
	if expected != 0 {
		return expected, nil
	}
	store := h.tokenStoreWithBaseDir()
	reader, ok := store.(coreauth.AuthByIDStore)
	if !ok {
		return 0, nil
	}
	candidates := uniqueAuthFileNames(append([]string{path, filepath.Base(path), fallbackID}, ids...))
	for _, id := range candidates {
		auth, errRead := reader.GetByID(ctx, id)
		if errRead != nil {
			return 0, fmt.Errorf("read auth generation before delete: %w", errRead)
		}
		if auth == nil {
			continue
		}
		if errMerge := merge(auth.StoreGeneration()); errMerge != nil {
			return 0, errMerge
		}
		if expected != 0 {
			return expected, nil
		}
	}
	return expected, nil
}

func (h *Handler) tokenStoreWithBaseDir() coreauth.Store {
	if h == nil {
		return nil
	}
	store := h.tokenStore
	if store == nil {
		store = sdkAuth.GetTokenStore()
		h.tokenStore = store
	}
	if h.cfg != nil {
		if dirSetter, ok := store.(interface{ SetBaseDir(string) }); ok {
			dirSetter.SetBaseDir(h.cfg.AuthDir)
		}
	}
	return store
}

func (h *Handler) mergeExistingAuthFileMetadata(record *coreauth.Auth) error {
	if h == nil || record == nil {
		return nil
	}
	var existingMap map[string]any

	if h.cfg != nil && strings.TrimSpace(h.cfg.AuthDir) != "" {
		targetFile := record.FileName
		if targetFile == "" {
			targetFile = record.ID
		}
		if targetFile != "" {
			raw, errRead := coreauth.ReadAuthFile(h.cfg.AuthDir, targetFile)
			if errRead != nil && !os.IsNotExist(errRead) {
				return fmt.Errorf("read existing credential: %w", errRead)
			}
			if len(raw) > 0 {
				_ = json.Unmarshal(raw, &existingMap)
			}
		}
	}

	if existingMap == nil && h.authManager != nil {
		if existing, ok := h.authManager.GetByID(record.ID); ok && existing != nil && existing.Metadata != nil {
			existingMap = existing.Metadata
		} else {
			for _, auth := range h.authManager.List() {
				if auth != nil && auth.FileName == record.FileName && auth.Metadata != nil {
					existingMap = auth.Metadata
					break
				}
			}
		}
	}

	if len(existingMap) > 0 {
		coreauth.MergeExistingAuthMetadata(record, existingMap)
	}
	return nil
}

func (h *Handler) saveTokenRecord(ctx context.Context, record *coreauth.Auth) (string, error) {
	if record == nil {
		return "", fmt.Errorf("token record is nil")
	}
	if errMerge := h.mergeExistingAuthFileMetadata(record); errMerge != nil {
		return "", errMerge
	}
	store := h.tokenStoreWithBaseDir()
	if store == nil {
		return "", fmt.Errorf("token store unavailable")
	}
	if h.postAuthHook != nil {
		if err := h.postAuthHook(ctx, record); err != nil {
			return "", fmt.Errorf("post-auth hook failed: %w", err)
		}
	}
	savedPath, errSave := coreauth.PersistExplicitAuth(ctx, store, record)
	if errSave != nil {
		return savedPath, errSave
	}
	if h.postAuthPersistHook != nil {
		if errHook := h.postAuthPersistHook(ctx, record); errHook != nil {
			return savedPath, fmt.Errorf("post-auth persist hook failed: %w", errHook)
		}
	}
	return savedPath, nil
}

func (h *Handler) beginExplicitAuthOperation(ctx context.Context) (context.Context, error) {
	store := h.tokenStoreWithBaseDir()
	return coreauth.BeginExplicitAuthOperation(ctx, store)
}
