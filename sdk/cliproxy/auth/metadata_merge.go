package auth

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

// IsAuthTokenPayloadKey returns true if key is a credential or token lifecycle field
// that should not overwrite newly acquired OAuth credentials during metadata merge.
func IsAuthTokenPayloadKey(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "access_token", "refresh_token", "id_token", "session_id",
		"expired", "last_refresh", "expires_in", "timestamp",
		"account_id",
		"token_type", "user_code", "verification_uri", "verification_uri_complete":
		return true
	default:
		return false
	}
}

// MergeExistingAuthMetadata merges user-configured metadata fields from existingMap
// into target.Metadata and target.Storage if target does not already define them.
func MergeExistingAuthMetadata(target *Auth, existingMap map[string]any) {
	if target == nil || len(existingMap) == 0 {
		return
	}
	if target.Metadata == nil {
		target.Metadata = make(map[string]any)
	}
	storageKeys := storageOwnedMetadataKeys(target.Storage)
	for k, v := range existingMap {
		normalizedKey := strings.ToLower(strings.TrimSpace(k))
		if IsAuthTokenPayloadKey(normalizedKey) {
			continue
		}
		if _, storageOwned := storageKeys[normalizedKey]; storageOwned {
			continue
		}
		if _, exists := target.Metadata[k]; !exists {
			target.Metadata[k] = v
		}
	}
	if setter, ok := target.Storage.(interface{ SetMetadata(map[string]any) }); ok {
		setter.SetMetadata(target.Metadata)
	}
}

func storageOwnedMetadataKeys(storage any) map[string]struct{} {
	if storage == nil {
		return nil
	}
	keys := make(map[string]struct{})
	addStorageJSONFieldKeys(reflect.TypeOf(storage), keys, make(map[reflect.Type]struct{}))

	if provider, ok := storage.(interface{ RawJSON() []byte }); ok {
		addStoragePayloadKeys(provider.RawJSON(), keys)
	}
	raw, errMarshal := json.Marshal(storage)
	if errMarshal == nil {
		addStoragePayloadKeys(raw, keys)
	}
	return keys
}

func addStoragePayloadKeys(raw []byte, keys map[string]struct{}) {
	if len(raw) == 0 {
		return
	}
	var payload map[string]any
	if errUnmarshal := json.Unmarshal(raw, &payload); errUnmarshal != nil {
		return
	}
	for key := range payload {
		key = strings.ToLower(strings.TrimSpace(key))
		if key != "" {
			keys[key] = struct{}{}
		}
	}
}

func addStorageJSONFieldKeys(storageType reflect.Type, keys map[string]struct{}, visited map[reflect.Type]struct{}) {
	for storageType.Kind() == reflect.Pointer {
		storageType = storageType.Elem()
	}
	if storageType.Kind() != reflect.Struct {
		return
	}
	if _, seen := visited[storageType]; seen {
		return
	}
	visited[storageType] = struct{}{}
	for i := 0; i < storageType.NumField(); i++ {
		field := storageType.Field(i)
		if !field.IsExported() {
			continue
		}
		tag := field.Tag.Get("json")
		name, _, _ := strings.Cut(tag, ",")
		if name == "-" {
			continue
		}
		if name == "" && field.Anonymous {
			anonymousType := field.Type
			for anonymousType.Kind() == reflect.Pointer {
				anonymousType = anonymousType.Elem()
			}
			if anonymousType.Kind() == reflect.Struct {
				addStorageJSONFieldKeys(field.Type, keys, visited)
				continue
			}
		}
		if name == "" {
			name = field.Name
		}
		name = strings.ToLower(strings.TrimSpace(name))
		if name != "" {
			keys[name] = struct{}{}
		}
	}
}

// ResolveAuthFilePath resolves candidate relative to baseDir and rejects lexical
// and symbolic-link escapes. The returned path remains under the caller's base
// directory while preserving legal absolute paths already inside that directory.
func ResolveAuthFilePath(baseDir, candidate string) (string, error) {
	baseDir = strings.TrimSpace(baseDir)
	candidate = strings.TrimSpace(candidate)
	if baseDir == "" {
		return "", fmt.Errorf("auth path: base directory is empty")
	}
	if candidate == "" {
		return "", fmt.Errorf("auth path: candidate is empty")
	}

	absBase, errAbsBase := filepath.Abs(baseDir)
	if errAbsBase != nil {
		return "", fmt.Errorf("auth path: resolve base directory: %w", errAbsBase)
	}
	absCandidate := candidate
	if !filepath.IsAbs(absCandidate) {
		absCandidate = filepath.Join(absBase, absCandidate)
	}
	var errAbsCandidate error
	absCandidate, errAbsCandidate = filepath.Abs(absCandidate)
	if errAbsCandidate != nil {
		return "", fmt.Errorf("auth path: resolve candidate: %w", errAbsCandidate)
	}
	if !pathWithinBase(absBase, absCandidate) {
		return "", fmt.Errorf("auth path: candidate escapes base directory")
	}

	resolvedBase, errResolvedBase := resolveExistingPathPrefix(absBase)
	if errResolvedBase != nil {
		return "", fmt.Errorf("auth path: resolve base directory links: %w", errResolvedBase)
	}
	resolvedCandidate, errResolvedCandidate := resolveExistingPathPrefix(absCandidate)
	if errResolvedCandidate != nil {
		return "", fmt.Errorf("auth path: resolve candidate links: %w", errResolvedCandidate)
	}
	if !pathWithinBase(resolvedBase, resolvedCandidate) {
		return "", fmt.Errorf("auth path: candidate link escapes base directory")
	}
	return filepath.Clean(absCandidate), nil
}

// ReadAuthFile reads candidate through an os.Root anchored at baseDir so the
// containment check and the file access cannot be separated by a symlink race.
func ReadAuthFile(baseDir, candidate string) ([]byte, error) {
	return withAuthFileRoot(baseDir, candidate, func(root *os.Root, name string) ([]byte, error) {
		return root.ReadFile(name)
	})
}

// StatAuthFile returns file information for candidate through an os.Root
// anchored at baseDir.
func StatAuthFile(baseDir, candidate string) (os.FileInfo, error) {
	return withAuthFileRoot(baseDir, candidate, func(root *os.Root, name string) (os.FileInfo, error) {
		return root.Stat(name)
	})
}

// WriteAuthFile writes candidate through an os.Root anchored at baseDir.
func WriteAuthFile(baseDir, candidate string, data []byte, perm os.FileMode) error {
	if errMkdir := os.MkdirAll(strings.TrimSpace(baseDir), 0o700); errMkdir != nil {
		return fmt.Errorf("auth path: create base directory: %w", errMkdir)
	}
	_, err := withAuthFileRoot(baseDir, candidate, func(root *os.Root, name string) (struct{}, error) {
		if errMkdir := root.MkdirAll(filepath.Dir(name), 0o700); errMkdir != nil {
			return struct{}{}, errMkdir
		}
		return struct{}{}, root.WriteFile(name, data, perm)
	})
	return err
}

// MkdirAuthFileParent creates candidate's parent directories through an os.Root
// anchored at baseDir.
func MkdirAuthFileParent(baseDir, candidate string, perm os.FileMode) error {
	if errMkdir := os.MkdirAll(strings.TrimSpace(baseDir), perm); errMkdir != nil {
		return fmt.Errorf("auth path: create base directory: %w", errMkdir)
	}
	_, err := withAuthFileRoot(baseDir, candidate, func(root *os.Root, name string) (struct{}, error) {
		return struct{}{}, root.MkdirAll(filepath.Dir(name), perm)
	})
	return err
}

// RemoveAuthFile removes candidate through an os.Root anchored at baseDir.
func RemoveAuthFile(baseDir, candidate string) error {
	_, err := withAuthFileRoot(baseDir, candidate, func(root *os.Root, name string) (struct{}, error) {
		return struct{}{}, root.Remove(name)
	})
	return err
}

func withAuthFileRoot[T any](baseDir, candidate string, operation func(*os.Root, string) (T, error)) (value T, err error) {
	absPath, errResolve := ResolveAuthFilePath(baseDir, candidate)
	if errResolve != nil {
		return value, errResolve
	}
	absBase, errAbsBase := filepath.Abs(strings.TrimSpace(baseDir))
	if errAbsBase != nil {
		return value, fmt.Errorf("auth path: resolve base directory: %w", errAbsBase)
	}
	name, errRel := filepath.Rel(absBase, absPath)
	if errRel != nil {
		return value, fmt.Errorf("auth path: resolve rooted name: %w", errRel)
	}
	root, errOpenRoot := os.OpenRoot(absBase)
	if errOpenRoot != nil {
		return value, fmt.Errorf("auth path: open base directory: %w", errOpenRoot)
	}
	defer func() {
		if errClose := root.Close(); errClose != nil && err == nil {
			err = fmt.Errorf("auth path: close base directory: %w", errClose)
		}
	}()
	value, err = operation(root, name)
	return value, err
}

func resolveExistingPathPrefix(path string) (string, error) {
	path = filepath.Clean(path)
	current := path
	missing := make([]string, 0, 2)
	for {
		_, errLstat := os.Lstat(current)
		if errLstat == nil {
			resolved, errEval := filepath.EvalSymlinks(current)
			if errEval != nil {
				return "", errEval
			}
			for i := len(missing) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, missing[i])
			}
			return filepath.Clean(resolved), nil
		}
		if !os.IsNotExist(errLstat) {
			return "", errLstat
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", errLstat
		}
		missing = append(missing, filepath.Base(current))
		current = parent
	}
}

func pathWithinBase(baseDir, candidate string) bool {
	rel, errRel := filepath.Rel(baseDir, candidate)
	if errRel != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

// MergePreparedAuth merges prepared request auth updates into current without modifying
// refresh lifecycle fields (such as LastRefreshedAt, LastError, or cooldown status).
func MergePreparedAuth(base, current, updated *Auth) *Auth {
	return mergeAuthContent(base, current, updated)
}

// MergeRefreshedAuth merges the refresh results from updated (derived from base)
// into the latest runtime auth current, preserving concurrent user modifications
// and active cooldowns.
func MergeRefreshedAuth(base, current, updated *Auth) *Auth {
	merged := mergeAuthContent(base, current, updated)
	if merged == nil || current == nil || updated == nil {
		return merged
	}
	if base != nil && current.RegistrationEpoch != base.RegistrationEpoch {
		return merged
	}

	// 1. Refresh Lifecycle Timestamps
	if !updated.LastRefreshedAt.IsZero() {
		merged.LastRefreshedAt = updated.LastRefreshedAt
	}
	if !updated.NextRefreshAfter.IsZero() || (base != nil && !base.NextRefreshAfter.IsZero()) {
		merged.NextRefreshAfter = updated.NextRefreshAfter
	}

	// 2. Error and Status recovery
	baseErrMsg := ""
	if base != nil && base.LastError != nil {
		baseErrMsg = base.LastError.Message
	}
	currentErrMsg := ""
	if current.LastError != nil {
		currentErrMsg = current.LastError.Message
	}
	hasNewConcurrentError := currentErrMsg != "" && currentErrMsg != baseErrMsg

	// 2. Disabled status three-way merge
	baseDisabled := base != nil && (base.Disabled || base.Status == StatusDisabled)
	currentDisabled := current.Disabled || current.Status == StatusDisabled
	updatedDisabled := updated.Disabled || updated.Status == StatusDisabled

	disabledChangedByExecutor := updatedDisabled != baseDisabled
	disabledChangedByUser := currentDisabled != baseDisabled

	finalDisabled := currentDisabled
	if disabledChangedByExecutor && !disabledChangedByUser {
		finalDisabled = updatedDisabled
	}

	if finalDisabled {
		merged.Disabled = true
		merged.Status = StatusDisabled
		merged.Metadata["disabled"] = true
	} else {
		merged.Disabled = false
		if merged.Status == StatusDisabled {
			merged.Status = StatusActive
		}
		merged.Metadata["disabled"] = false

		if hasNewConcurrentError {
			// A new error occurred concurrently (e.g. 503, 429, timeout). Preserve it.
			merged.LastError = current.LastError
			merged.Status = current.Status
			merged.Unavailable = current.Unavailable
			merged.StatusMessage = current.StatusMessage
		} else if current.Quota.Exceeded && current.Quota.Reason == "credential_quota" && current.Quota.NextRecoverAt.After(time.Now()) {
			// Preserve active credential quota
			merged.Unavailable = current.Unavailable
			merged.Status = current.Status
			merged.StatusMessage = current.StatusMessage
		} else if current.Unavailable && current.NextRetryAfter.After(time.Now()) {
			// Preserve active cooldown
			merged.Unavailable = current.Unavailable
			merged.Status = current.Status
			merged.StatusMessage = current.StatusMessage
		} else if updated.Status == StatusActive || updated.Status == "" {
			// Successful refresh clears previous auth error and restores active
			merged.Status = StatusActive
			merged.Unavailable = false
			merged.StatusMessage = ""
			merged.LastError = nil
		}
	}

	// 3. ModelStates: three-way merge to preserve concurrent cooldown/quota
	recoveredUnauthorizedModel := false
	var baseModels map[string]*ModelState
	if base != nil {
		baseModels = base.ModelStates
	}
	if updated.ModelStates != nil {
		if merged.ModelStates == nil {
			merged.ModelStates = make(map[string]*ModelState)
		}
		for model, updState := range updated.ModelStates {
			baseState := baseModels[model]
			currentState := current.ModelStates[model]

			changedByExecutor := !reflect.DeepEqual(baseState, updState)
			changedByUser := !reflect.DeepEqual(baseState, currentState)

			if changedByExecutor && !changedByUser {
				merged.ModelStates[model] = updState
				if baseState != nil && isUnauthorizedAuthError(baseState.LastError) && modelStateIsClean(updState) {
					recoveredUnauthorizedModel = true
				}
			}
		}
		if baseModels != nil {
			for model, baseState := range baseModels {
				if _, inUpdated := updated.ModelStates[model]; !inUpdated {
					if currentState, ok := current.ModelStates[model]; ok {
						if reflect.DeepEqual(baseState, currentState) {
							delete(merged.ModelStates, model)
							if baseState != nil && isUnauthorizedAuthError(baseState.LastError) {
								recoveredUnauthorizedModel = true
							}
						}
					}
				}
			}
		}
	}

	if recoveredUnauthorizedModel && !finalDisabled && base != nil && isUnauthorizedAuthError(current.LastError) &&
		reflect.DeepEqual(base.LastError, current.LastError) &&
		updated.LastError == nil && (updated.Status == StatusActive || updated.Status == "") {
		for _, state := range merged.ModelStates {
			if state != nil && isUnauthorizedAuthError(state.LastError) {
				return merged
			}
		}
		// A recovered model must not leave its old 401 in the auth aggregate:
		// that combination disables automatic refresh once NextRefreshAfter clears.
		// Recompute from merged models so unrelated quota cooldowns remain intact.
		merged = merged.Clone()
		updateAggregatedAvailability(merged, time.Now())
		merged.LastError = nil
		merged.StatusMessage = ""
		merged.Status = StatusActive
	}

	return merged
}

func mergeAuthContent(base, current, updated *Auth) *Auth {
	if current == nil {
		if updated != nil {
			return updated.Clone()
		}
		if base != nil {
			return base.Clone()
		}
		return nil
	}
	if updated == nil {
		return current.Clone()
	}
	if base != nil && current.RegistrationEpoch != base.RegistrationEpoch {
		// Stale update from a previous registration cycle; keep current state.
		return current.Clone()
	}

	merged := current.Clone()
	if merged.Metadata == nil {
		merged.Metadata = make(map[string]any)
	}

	var baseMeta map[string]any
	if base != nil {
		baseMeta = base.Metadata
	}

	// 1. Three-way merge for Metadata (excluding proxy_url which has dedicated canonical merge)
	if updated.Metadata != nil {
		for k, v := range updated.Metadata {
			if strings.EqualFold(strings.TrimSpace(k), "proxy_url") {
				continue
			}
			baseVal, hadInBase := baseMeta[k]
			currentVal, hadInCurrent := current.Metadata[k]

			changedByExecutor := !hadInBase || !reflect.DeepEqual(baseVal, v)
			changedByUser := hadInBase != hadInCurrent || (hadInBase && !reflect.DeepEqual(baseVal, currentVal))

			if changedByExecutor {
				// Apply executor change if user didn't modify it, or if it is a token payload field
				if !changedByUser || IsAuthTokenPayloadKey(k) {
					merged.Metadata[k] = v
				}
			}
		}
		// Deletions by executor: only delete if user didn't modify the field concurrently
		if baseMeta != nil {
			for k, baseVal := range baseMeta {
				if strings.EqualFold(strings.TrimSpace(k), "proxy_url") {
					continue
				}
				if _, inUpdated := updated.Metadata[k]; !inUpdated {
					if currentVal, ok := current.Metadata[k]; ok {
						if reflect.DeepEqual(baseVal, currentVal) {
							delete(merged.Metadata, k)
						}
					}
				}
			}
		}
	}

	// 2. Storage and Runtime
	if updated.Storage != nil {
		merged.Storage = updated.Storage
	}
	if updated.Runtime != nil {
		merged.Runtime = updated.Runtime
	}

	// 3. ProxyURL three-way merge (supporting both struct field and metadata modifications)
	baseStruct := ""
	if base != nil {
		baseStruct = strings.TrimSpace(base.ProxyURL)
	}
	currentStruct := strings.TrimSpace(current.ProxyURL)
	updatedStruct := strings.TrimSpace(updated.ProxyURL)

	baseMetaProxy := ""
	if base != nil && base.Metadata != nil {
		if s, ok := base.Metadata["proxy_url"].(string); ok {
			baseMetaProxy = strings.TrimSpace(s)
		}
	}
	currentMetaProxy := ""
	if current.Metadata != nil {
		if s, ok := current.Metadata["proxy_url"].(string); ok {
			currentMetaProxy = strings.TrimSpace(s)
		}
	}
	updatedMetaProxy := ""
	if updated.Metadata != nil {
		if s, ok := updated.Metadata["proxy_url"].(string); ok {
			updatedMetaProxy = strings.TrimSpace(s)
		}
	}

	userChangedStruct := currentStruct != baseStruct
	userChangedMeta := currentMetaProxy != baseMetaProxy
	execChangedStruct := updatedStruct != baseStruct
	execChangedMeta := updatedMetaProxy != baseMetaProxy

	finalProxy := currentStruct
	if currentMetaProxy != "" && currentStruct == "" && !userChangedStruct {
		finalProxy = currentMetaProxy
	}

	if userChangedStruct || userChangedMeta {
		// User modified proxy concurrently; user takes precedence over executor.
		if userChangedStruct && !userChangedMeta {
			finalProxy = currentStruct
		} else if userChangedMeta && !userChangedStruct {
			finalProxy = currentMetaProxy
		} else if currentStruct != "" {
			finalProxy = currentStruct
		} else {
			finalProxy = currentMetaProxy
		}
	} else if execChangedStruct || execChangedMeta {
		// Executor modified proxy and user did not touch it.
		if execChangedStruct && !execChangedMeta {
			finalProxy = updatedStruct
		} else if execChangedMeta && !execChangedStruct {
			finalProxy = updatedMetaProxy
		} else if updatedStruct != "" {
			finalProxy = updatedStruct
		} else {
			finalProxy = updatedMetaProxy
		}
	}

	if finalProxy != "" {
		merged.ProxyURL = finalProxy
		merged.Metadata["proxy_url"] = finalProxy
	} else {
		merged.ProxyURL = ""
		delete(merged.Metadata, "proxy_url")
	}

	// 4. Prefix (three-way merge, user modification takes precedence)
	basePrefix := ""
	if base != nil {
		basePrefix = strings.TrimSpace(base.Prefix)
	}
	currentPrefix := strings.TrimSpace(current.Prefix)
	updatedPrefix := strings.TrimSpace(updated.Prefix)

	if updatedPrefix != basePrefix && currentPrefix == basePrefix {
		merged.Prefix = updatedPrefix
	} else {
		merged.Prefix = currentPrefix
	}

	// 5. Attributes (three-way merge)
	if updated.Attributes != nil {
		if merged.Attributes == nil {
			merged.Attributes = make(map[string]string)
		}
		var baseAttrs map[string]string
		if base != nil {
			baseAttrs = base.Attributes
		}
		for k, v := range updated.Attributes {
			baseVal, hadInBase := baseAttrs[k]
			currentVal, hadInCurrent := current.Attributes[k]

			changedByExecutor := !hadInBase || baseVal != v
			changedByUser := hadInBase != hadInCurrent || (hadInBase && baseVal != currentVal)

			if changedByExecutor && !changedByUser {
				merged.Attributes[k] = v
			}
		}
		if baseAttrs != nil {
			for k, baseVal := range baseAttrs {
				if _, inUpdated := updated.Attributes[k]; !inUpdated {
					if currentVal, ok := current.Attributes[k]; ok {
						if baseVal == currentVal {
							delete(merged.Attributes, k)
						}
					}
				}
			}
		}
	}

	return merged
}
