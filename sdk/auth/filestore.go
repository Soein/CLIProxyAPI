package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	baseauth "github.com/router-for-me/CLIProxyAPI/v7/internal/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/authfilelock"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/misc"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

// PluginAuthParser parses auth JSON owned by plugin providers.
type PluginAuthParser interface {
	ParseAuth(context.Context, pluginapi.AuthParseRequest) (*cliproxyauth.Auth, bool, error)
}

// PluginMultiAuthParser expands one auth JSON payload into multiple plugin auth records.
// Returning handled=true with an empty slice means the plugin intentionally suppresses built-in parsing.
type PluginMultiAuthParser interface {
	ParseAuths(context.Context, pluginapi.AuthParseRequest) ([]*cliproxyauth.Auth, bool, error)
}

type pluginAuthParserHolder struct {
	parser PluginAuthParser
}

var pluginAuthParserValue atomic.Value

// RegisterPluginAuthParser registers the current plugin auth parser.
func RegisterPluginAuthParser(parser PluginAuthParser) {
	pluginAuthParserValue.Store(pluginAuthParserHolder{parser: parser})
}

func currentPluginAuthParser() PluginAuthParser {
	value := pluginAuthParserValue.Load()
	if value == nil {
		return nil
	}
	holder, ok := value.(pluginAuthParserHolder)
	if !ok {
		return nil
	}
	return holder.parser
}

// FileTokenStore persists token records and auth metadata using the filesystem
// as backing storage. TokenJSONMarshaler implementations are written through an
// os.Root anchored at baseDir. Path-only TokenStorage implementations remain a
// trusted legacy boundary responsible for their own filesystem containment.
type FileTokenStore struct {
	mu      sync.Mutex
	dirLock sync.RWMutex
	baseDir string
}

// NewFileTokenStore creates a token store that saves credentials to disk through the
// TokenStorage implementation embedded in the token record.
func NewFileTokenStore() *FileTokenStore {
	return &FileTokenStore{}
}

// SetBaseDir updates the default directory used for auth JSON persistence when no explicit path is provided.
func (s *FileTokenStore) SetBaseDir(dir string) {
	s.dirLock.Lock()
	s.baseDir = strings.TrimSpace(dir)
	s.dirLock.Unlock()
}

// Save persists token storage and metadata to the resolved auth file path.
func (s *FileTokenStore) Save(ctx context.Context, auth *cliproxyauth.Auth) (string, error) {
	if auth == nil {
		return "", fmt.Errorf("auth filestore: auth is nil")
	}
	cliproxyauth.NormalizeCredentialMetadata(auth.Metadata)
	if errWeight := cliproxyauth.ValidateAuthWeight(auth); errWeight != nil {
		return "", fmt.Errorf("auth filestore: %w", errWeight)
	}

	path, err := s.resolveAuthPath(auth)
	if err != nil {
		return "", err
	}
	if path == "" {
		return "", fmt.Errorf("auth filestore: missing file path attribute for %s", auth.ID)
	}
	unlockPath := authfilelock.Lock(path)
	defer unlockPath()
	baseDir := s.baseDirSnapshot()

	if auth.Disabled {
		if _, statErr := statAuthFile(baseDir, path); os.IsNotExist(statErr) {
			return "", nil
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err = mkdirAuthFileParent(baseDir, path, 0o700); err != nil {
		return "", fmt.Errorf("auth filestore: create dir failed: %w", err)
	}

	// metadataSetter is a private interface for TokenStorage implementations that support metadata injection.
	type metadataSetter interface {
		SetMetadata(map[string]any)
	}

	switch {
	case auth.Storage != nil:
		if auth.Metadata == nil {
			auth.Metadata = make(map[string]any)
		}
		auth.Metadata["disabled"] = auth.Disabled
		if setter, ok := auth.Storage.(metadataSetter); ok {
			setter.SetMetadata(auth.Metadata)
		}
		if marshaler, ok := auth.Storage.(baseauth.TokenJSONMarshaler); ok {
			payload, errPayload := marshaler.MarshalTokenJSON()
			if errPayload != nil {
				return "", fmt.Errorf("auth filestore: marshal token payload: %w", errPayload)
			}
			if !json.Valid(payload) {
				return "", fmt.Errorf("auth filestore: token payload is not valid JSON")
			}
			if existing, errRead := readAuthFile(baseDir, path); errRead == nil {
				if jsonEqual(existing, payload) {
					break
				}
			} else if !os.IsNotExist(errRead) {
				return "", fmt.Errorf("auth filestore: read existing token payload: %w", errRead)
			}
			misc.LogSavingCredentials(path)
			if errWrite := writeAuthFile(baseDir, path, payload, 0o600); errWrite != nil {
				return "", fmt.Errorf("auth filestore: write token payload: %w", errWrite)
			}
		} else if err = auth.Storage.SaveTokenToFile(path); err != nil {
			return "", err
		}
	case auth.Metadata != nil:
		auth.Metadata["disabled"] = auth.Disabled
		raw, errMarshal := json.Marshal(auth.Metadata)
		if errMarshal != nil {
			return "", fmt.Errorf("auth filestore: marshal metadata failed: %w", errMarshal)
		}
		if existing, errRead := readAuthFile(baseDir, path); errRead == nil {
			if jsonEqual(existing, raw) {
				break
			}
			if errWrite := writeAuthFile(baseDir, path, raw, 0o600); errWrite != nil {
				return "", fmt.Errorf("auth filestore: write existing failed: %w", errWrite)
			}
			break
		} else if !os.IsNotExist(errRead) {
			return "", fmt.Errorf("auth filestore: read existing failed: %w", errRead)
		}
		if errWrite := writeAuthFile(baseDir, path, raw, 0o600); errWrite != nil {
			return "", fmt.Errorf("auth filestore: write file failed: %w", errWrite)
		}
	default:
		return "", fmt.Errorf("auth filestore: nothing to persist for %s", auth.ID)
	}

	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	auth.Attributes[cliproxyauth.AttributePath] = path
	auth.Attributes[cliproxyauth.AttributeSource] = path
	auth.Attributes[cliproxyauth.AttributeSourceBackend] = cliproxyauth.AuthSourceFile

	if strings.TrimSpace(auth.FileName) == "" {
		auth.FileName = auth.ID
	}

	return path, nil
}

// List enumerates all auth JSON files under the configured directory.
func (s *FileTokenStore) List(ctx context.Context) ([]*cliproxyauth.Auth, error) {
	dir := s.baseDirSnapshot()
	if dir == "" {
		return nil, fmt.Errorf("auth filestore: directory not configured")
	}
	entries := make([]*cliproxyauth.Auth, 0)
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".json") {
			return nil
		}
		auths, errReadAuths := s.readAuthFiles(path, dir)
		if errReadAuths != nil {
			return nil
		}
		if len(auths) > 0 {
			entries = append(entries, auths...)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// Delete removes the auth file.
func (s *FileTokenStore) Delete(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("auth filestore: id is empty")
	}
	path, err := s.resolveDeletePath(id)
	if err != nil {
		return err
	}
	if err = removeAuthFile(s.baseDirSnapshot(), path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("auth filestore: delete failed: %w", err)
	}
	return nil
}

func (s *FileTokenStore) resolveDeletePath(id string) (string, error) {
	dir := s.baseDirSnapshot()
	if dir != "" {
		candidate := id
		if !filepath.IsAbs(candidate) {
			candidate = filepath.Join(dir, candidate)
		}
		return cliproxyauth.ResolveAuthFilePath(dir, candidate)
	}
	if strings.ContainsRune(id, os.PathSeparator) || filepath.IsAbs(id) {
		return id, nil
	}
	return "", fmt.Errorf("auth filestore: directory not configured")
}

func (s *FileTokenStore) readAuthFiles(path, baseDir string) ([]*cliproxyauth.Auth, error) {
	if strings.TrimSpace(baseDir) != "" {
		var errResolve error
		path, errResolve = cliproxyauth.ResolveAuthFilePath(baseDir, path)
		if errResolve != nil {
			return nil, errResolve
		}
	}
	data, err := readAuthFile(baseDir, path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	if len(data) == 0 {
		return nil, nil
	}
	metadata := make(map[string]any)
	if err = json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("unmarshal auth json: %w", err)
	}
	cliproxyauth.NormalizeCredentialMetadata(metadata)
	if errWeight := cliproxyauth.ValidateAuthWeight(&cliproxyauth.Auth{Metadata: metadata}); errWeight != nil {
		return nil, errWeight
	}
	provider, _ := metadata["type"].(string)
	provider = strings.TrimSpace(provider)
	if strings.EqualFold(provider, "gemini") {
		return nil, nil
	}
	info, errStat := statAuthFile(baseDir, path)
	if errStat != nil {
		return nil, fmt.Errorf("stat file: %w", errStat)
	}
	if parser := currentPluginAuthParser(); parser != nil {
		auths, handled, errParse := parsePluginAuthFile(parser, pluginapi.AuthParseRequest{
			Provider: provider,
			Path:     path,
			FileName: s.idFor(path, baseDir),
			RawJSON:  data,
		})
		if errParse == nil && handled {
			auths = compactPluginAuths(auths)
			if len(auths) == 0 {
				return nil, nil
			}
			disabled, _ := metadata["disabled"].(bool)
			for index, auth := range auths {
				if auth == nil {
					continue
				}
				cliproxyauth.NormalizeCredentialMetadata(auth.Metadata)
				if len(auths) > 1 {
					cliproxyauth.MarkPluginVirtualAuth(auth, path, index)
				}
				auth.CreatedAt = info.ModTime()
				auth.UpdatedAt = info.ModTime()
				if auth.Attributes == nil {
					auth.Attributes = make(map[string]string)
				}
				auth.Attributes[cliproxyauth.AttributePath] = path
				auth.Attributes[cliproxyauth.AttributeSource] = path
				auth.Attributes[cliproxyauth.AttributeSourceBackend] = cliproxyauth.AuthSourceFile
				if disabled {
					auth.Disabled = true
					auth.Status = cliproxyauth.StatusDisabled
					if auth.Metadata == nil {
						auth.Metadata = make(map[string]any)
					}
					auth.Metadata["disabled"] = true
				}
				if errWeight := cliproxyauth.ApplyAuthWeightMetadata(auth, metadata); errWeight != nil {
					return nil, errWeight
				}
				cliproxyauth.ApplyCustomHeadersFromMetadata(auth)
			}
			return auths, nil
		}
	}
	if provider == "" {
		provider = "unknown"
	}
	if provider == "antigravity" {
		projectID := ""
		if pid, ok := metadata["project_id"].(string); ok {
			projectID = strings.TrimSpace(pid)
		}
		if projectID == "" {
			accessToken := extractAccessToken(metadata)
			if accessToken != "" {
				fetchedProjectID, errFetch := FetchAntigravityProjectID(context.Background(), accessToken, http.DefaultClient)
				if errFetch == nil && strings.TrimSpace(fetchedProjectID) != "" {
					metadata["project_id"] = strings.TrimSpace(fetchedProjectID)
					if raw, errMarshal := json.Marshal(metadata); errMarshal == nil {
						_ = writeAuthFile(baseDir, path, raw, 0o600)
					}
				}
			}
		}
	}
	info, errStat = statAuthFile(baseDir, path)
	if errStat != nil {
		return nil, fmt.Errorf("stat file: %w", errStat)
	}
	id := s.idFor(path, baseDir)
	disabled, _ := metadata["disabled"].(bool)
	status := cliproxyauth.StatusActive
	if disabled {
		status = cliproxyauth.StatusDisabled
	}
	auth := &cliproxyauth.Auth{
		ID:       id,
		Provider: provider,
		FileName: id,
		Label:    s.labelFor(metadata),
		Status:   status,
		Disabled: disabled,
		Attributes: map[string]string{
			cliproxyauth.AttributePath:          path,
			cliproxyauth.AttributeSource:        path,
			cliproxyauth.AttributeSourceBackend: cliproxyauth.AuthSourceFile,
		},
		Metadata:         metadata,
		CreatedAt:        info.ModTime(),
		UpdatedAt:        info.ModTime(),
		LastRefreshedAt:  time.Time{},
		NextRefreshAfter: time.Time{},
	}
	if email, ok := metadata["email"].(string); ok && email != "" {
		auth.Attributes["email"] = email
	}
	cliproxyauth.ApplyCustomHeadersFromMetadata(auth)
	return []*cliproxyauth.Auth{auth}, nil
}

func (s *FileTokenStore) readAuthFile(path, baseDir string) (*cliproxyauth.Auth, error) {
	auths, errReadAuths := s.readAuthFiles(path, baseDir)
	if errReadAuths != nil || len(auths) == 0 {
		return nil, errReadAuths
	}
	return auths[0], nil
}

func parsePluginAuthFile(parser PluginAuthParser, req pluginapi.AuthParseRequest) ([]*cliproxyauth.Auth, bool, error) {
	if parser == nil {
		return nil, false, nil
	}
	if multiParser, ok := parser.(PluginMultiAuthParser); ok {
		return multiParser.ParseAuths(context.Background(), req)
	}
	auth, handled, errParse := parser.ParseAuth(context.Background(), req)
	if errParse != nil || !handled || auth == nil {
		return nil, handled, errParse
	}
	return []*cliproxyauth.Auth{auth}, true, nil
}

func compactPluginAuths(auths []*cliproxyauth.Auth) []*cliproxyauth.Auth {
	if len(auths) == 0 {
		return nil
	}
	out := auths[:0]
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		if errWeight := cliproxyauth.ValidateAuthWeight(auth); errWeight != nil {
			continue
		}
		out = append(out, auth)
	}
	return out
}

func (s *FileTokenStore) idFor(path, baseDir string) string {
	id := path
	if baseDir != "" {
		if rel, errRel := filepath.Rel(baseDir, path); errRel == nil && rel != "" {
			id = rel
		}
	}
	// On Windows, normalize ID casing to avoid duplicate auth entries caused by case-insensitive paths.
	if runtime.GOOS == "windows" {
		id = strings.ToLower(id)
	}
	return id
}

func (s *FileTokenStore) resolveAuthPath(auth *cliproxyauth.Auth) (string, error) {
	if auth == nil {
		return "", fmt.Errorf("auth filestore: auth is nil")
	}
	resolve := func(candidate string) (string, error) {
		dir := s.baseDirSnapshot()
		if dir == "" {
			return candidate, nil
		}
		if !filepath.IsAbs(candidate) {
			candidate = filepath.Join(dir, candidate)
		}
		return cliproxyauth.ResolveAuthFilePath(dir, candidate)
	}
	if auth.Attributes != nil {
		if p := strings.TrimSpace(auth.Attributes["path"]); p != "" {
			return resolve(p)
		}
	}
	if fileName := strings.TrimSpace(auth.FileName); fileName != "" {
		return resolve(fileName)
	}
	if auth.ID == "" {
		return "", fmt.Errorf("auth filestore: missing id")
	}
	return resolve(auth.ID)
}

func (s *FileTokenStore) labelFor(metadata map[string]any) string {
	if metadata == nil {
		return ""
	}
	if v, ok := metadata["label"].(string); ok && v != "" {
		return v
	}
	if v, ok := metadata["email"].(string); ok && v != "" {
		return v
	}
	if project, ok := metadata["project_id"].(string); ok && project != "" {
		return project
	}
	return ""
}

func (s *FileTokenStore) baseDirSnapshot() string {
	s.dirLock.RLock()
	defer s.dirLock.RUnlock()
	return s.baseDir
}

func readAuthFile(baseDir, path string) ([]byte, error) {
	if strings.TrimSpace(baseDir) == "" {
		return os.ReadFile(path)
	}
	return cliproxyauth.ReadAuthFile(baseDir, path)
}

func statAuthFile(baseDir, path string) (os.FileInfo, error) {
	if strings.TrimSpace(baseDir) == "" {
		return os.Stat(path)
	}
	return cliproxyauth.StatAuthFile(baseDir, path)
}

func writeAuthFile(baseDir, path string, data []byte, perm os.FileMode) error {
	if strings.TrimSpace(baseDir) == "" {
		return os.WriteFile(path, data, perm)
	}
	return cliproxyauth.WriteAuthFile(baseDir, path, data, perm)
}

func mkdirAuthFileParent(baseDir, path string, perm os.FileMode) error {
	if strings.TrimSpace(baseDir) == "" {
		return os.MkdirAll(filepath.Dir(path), perm)
	}
	return cliproxyauth.MkdirAuthFileParent(baseDir, path, perm)
}

func removeAuthFile(baseDir, path string) error {
	if strings.TrimSpace(baseDir) == "" {
		return os.Remove(path)
	}
	return cliproxyauth.RemoveAuthFile(baseDir, path)
}

func extractAccessToken(metadata map[string]any) string {
	if at, ok := metadata["access_token"].(string); ok {
		if v := strings.TrimSpace(at); v != "" {
			return v
		}
	}
	if tokenMap, ok := metadata["token"].(map[string]any); ok {
		if at, ok := tokenMap["access_token"].(string); ok {
			if v := strings.TrimSpace(at); v != "" {
				return v
			}
		}
	}
	return ""
}

// jsonEqual compares two JSON blobs by parsing them into Go objects and deep comparing.
func jsonEqual(a, b []byte) bool {
	var objA any
	var objB any
	if err := json.Unmarshal(a, &objA); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &objB); err != nil {
		return false
	}
	return deepEqualJSON(objA, objB)
}

func deepEqualJSON(a, b any) bool {
	switch valA := a.(type) {
	case map[string]any:
		valB, ok := b.(map[string]any)
		if !ok || len(valA) != len(valB) {
			return false
		}
		for key, subA := range valA {
			subB, ok1 := valB[key]
			if !ok1 || !deepEqualJSON(subA, subB) {
				return false
			}
		}
		return true
	case []any:
		sliceB, ok := b.([]any)
		if !ok || len(valA) != len(sliceB) {
			return false
		}
		for i := range valA {
			if !deepEqualJSON(valA[i], sliceB[i]) {
				return false
			}
		}
		return true
	case float64:
		valB, ok := b.(float64)
		if !ok {
			return false
		}
		return valA == valB
	case string:
		valB, ok := b.(string)
		if !ok {
			return false
		}
		return valA == valB
	case bool:
		valB, ok := b.(bool)
		if !ok {
			return false
		}
		return valA == valB
	case nil:
		return b == nil
	default:
		return false
	}
}
