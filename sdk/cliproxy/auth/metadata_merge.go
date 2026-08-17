package auth

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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
