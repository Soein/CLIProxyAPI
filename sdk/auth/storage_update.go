package auth

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/claude"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/kimi"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/meta"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/vertex"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/xai"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// CloneTokenStorageForUpdate isolates known serializers and explicitly clonable
// SDK writers before metadata injection. Unknown writers may contain locks or
// shared mutable state and cannot be copied safely by reflection.
func CloneTokenStorageForUpdate(storage coreauth.TokenStorage) (coreauth.TokenStorage, bool) {
	if cloner, ok := storage.(coreauth.TokenStorageCloner); ok {
		cloned := cloner.CloneTokenStorage()
		return cloned, cloned != nil
	}
	switch typed := storage.(type) {
	case *claude.ClaudeTokenStorage:
		cloned := *typed
		return &cloned, true
	case *codex.CodexTokenStorage:
		cloned := *typed
		return &cloned, true
	case *kimi.KimiTokenStorage:
		cloned := *typed
		return &cloned, true
	case *meta.MetaTokenStorage:
		cloned := *typed
		return &cloned, true
	case *vertex.VertexCredentialStorage:
		cloned := *typed
		return &cloned, true
	case *xai.TokenStorage:
		cloned := *typed
		return &cloned, true
	default:
		return nil, false
	}
}

// WriteTokenPayload preserves Meta's atomic publication while other serializers
// retain the file store's existing write behavior. The payload is written as-is.
func WriteTokenPayload(baseDir, path string, payload []byte, storage coreauth.TokenStorage) (err error) {
	if _, isMeta := storage.(*meta.MetaTokenStorage); !isMeta {
		return writeAuthFile(baseDir, path, payload, 0o600)
	}
	if baseDir == "" {
		baseDir = filepath.Dir(path)
	}
	absBase, errAbs := filepath.Abs(baseDir)
	if errAbs != nil {
		return errAbs
	}
	absPath, errAbsPath := filepath.Abs(path)
	if errAbsPath != nil {
		return errAbsPath
	}
	name, errRel := filepath.Rel(absBase, absPath)
	if errRel != nil {
		return errRel
	}
	root, errRoot := os.OpenRoot(absBase)
	if errRoot != nil {
		return errRoot
	}
	defer func() {
		if errClose := root.Close(); errClose != nil && err == nil {
			err = fmt.Errorf("auth filestore: close base directory: %w", errClose)
		}
	}()
	tempName := filepath.Join(filepath.Dir(name), ".meta-token-"+rand.Text())
	file, errOpen := root.OpenFile(tempName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if errOpen != nil {
		return fmt.Errorf("meta token storage: create token file: %w", errOpen)
	}
	defer func() { _ = root.Remove(tempName) }()
	_, errWrite := file.Write(payload)
	errClose := file.Close()
	if errWrite != nil {
		return fmt.Errorf("meta token storage: write token file: %w", errWrite)
	}
	if errClose != nil {
		return fmt.Errorf("meta token storage: close token file: %w", errClose)
	}
	if errRename := root.Rename(tempName, name); errRename != nil {
		return fmt.Errorf("meta token storage: replace token file: %w", errRename)
	}
	return nil
}
