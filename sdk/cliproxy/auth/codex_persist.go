package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/codex"
)

// IsCodexAuthRecord reports whether auth represents a Codex credential record.
func IsCodexAuthRecord(auth *Auth) bool {
	if auth == nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return true
	}
	if auth.Metadata != nil {
		if t, ok := auth.Metadata["type"].(string); ok && strings.EqualFold(strings.TrimSpace(t), "codex") {
			return true
		}
	}
	if _, ok := auth.Storage.(*codex.CodexTokenStorage); ok {
		return true
	}
	return false
}

// CodexAuthFileIO keeps credential merging independent of the storage destination.
// WithLock must serialize read-modify-write operations for the source path.
type CodexAuthFileIO struct {
	WithLock  func(string, func() error) error
	ReadFile  func(string) ([]byte, error)
	WriteJSON func(string, map[string]any) error
	// WriteUnchanged also emits an unchanged payload when preparing a new destination.
	WriteUnchanged bool
}

// PersistCodexAuthFile persists a Codex auth record using inter-process locking,
// read-before-write merging, and scoped refresh patches when present.
// It returns handled=false when auth is not a Codex credential record.
func PersistCodexAuthFile(ctx context.Context, path string, auth *Auth) (bool, error) {
	return PersistCodexAuthFileWithIO(ctx, path, auth, CodexAuthFileIO{
		WithLock: codex.WithFileLock, ReadFile: os.ReadFile, WriteJSON: codex.WriteJSONFileAtomic,
	})
}

// PersistCodexAuthFileWithIO applies the same credential merge using caller-owned IO.
// This supports rooted filesystem access and database staging without replacing the source file.
func PersistCodexAuthFileWithIO(ctx context.Context, path string, auth *Auth, fileIO CodexAuthFileIO) (bool, error) {
	if !IsCodexAuthRecord(auth) {
		return false, nil
	}
	if fileIO.WithLock == nil || fileIO.ReadFile == nil || fileIO.WriteJSON == nil {
		return true, fmt.Errorf("auth filestore: incomplete Codex file IO")
	}

	type metadataSetter interface {
		SetMetadata(map[string]any)
	}

	switch {
	case auth.CodexRefreshPatch != nil:
		errRefresh := fileIO.WithLock(path, func() error {
			existing, errRead := fileIO.ReadFile(path)
			if errRead != nil {
				if os.IsNotExist(errRead) {
					if auth.Metadata == nil {
						auth.Metadata = make(map[string]any)
					}
					auth.Metadata["disabled"] = auth.Disabled
					NormalizeCredentialMetadata(auth.Metadata)
					return fileIO.WriteJSON(path, auth.Metadata)
				}
				return fmt.Errorf("auth filestore: read refresh source failed: %w", errRead)
			}
			var diskMap map[string]any
			if errUnmarshal := json.Unmarshal(existing, &diskMap); errUnmarshal != nil || diskMap == nil {
				return fmt.Errorf("auth filestore: invalid refresh source JSON")
			}
			merged := auth.CodexRefreshPatch.Apply(diskMap)
			NormalizeCredentialMetadata(merged)
			if errWrite := fileIO.WriteJSON(path, merged); errWrite != nil {
				return errWrite
			}
			auth.Metadata = merged
			return nil
		})
		if errRefresh != nil {
			return true, errRefresh
		}
		auth.CodexRefreshPatch = nil
		auth.Storage = nil
		if disabled, ok := auth.Metadata["disabled"].(bool); ok {
			auth.Disabled = disabled
		}
		return true, nil

	case auth.Storage != nil:
		if auth.Metadata == nil {
			auth.Metadata = make(map[string]any)
		}
		auth.Metadata["disabled"] = auth.Disabled
		if storage, ok := auth.Storage.(*codex.CodexTokenStorage); ok && storage != nil {
			clonedStorage := *storage
			clonedStorage.ReplaceCredentials = HasAuthCreationIntent(ctx)
			auth.Storage = &clonedStorage
		}
		if setter, ok := auth.Storage.(metadataSetter); ok {
			setter.SetMetadata(auth.Metadata)
		}
		if marshaler, ok := auth.Storage.(interface{ MarshalTokenJSON() ([]byte, error) }); ok {
			payload, errMarshal := marshaler.MarshalTokenJSON()
			if errMarshal != nil {
				return true, errMarshal
			}
			var incoming map[string]any
			if errUnmarshal := json.Unmarshal(payload, &incoming); errUnmarshal != nil || incoming == nil {
				return true, fmt.Errorf("auth filestore: invalid Codex token payload")
			}
			errSave := fileIO.WithLock(path, func() error {
				data := incoming
				existing, errRead := fileIO.ReadFile(path)
				if errRead != nil && !os.IsNotExist(errRead) {
					return fmt.Errorf("auth filestore: read token source failed: %w", errRead)
				}
				if errRead == nil {
					var diskMap map[string]any
					if errUnmarshal := json.Unmarshal(existing, &diskMap); errUnmarshal != nil || diskMap == nil {
						return fmt.Errorf("auth filestore: invalid token source JSON")
					}
					storage, ok := auth.Storage.(*codex.CodexTokenStorage)
					if !ok {
						storage = &codex.CodexTokenStorage{ReplaceCredentials: HasAuthCreationIntent(ctx)}
					}
					data = storage.MergeWithDiskMap(diskMap, incoming)
				}
				if errWrite := fileIO.WriteJSON(path, data); errWrite != nil {
					return errWrite
				}
				auth.Metadata = codex.MergeCodexCredentialsFromDisk(data, auth.Metadata)
				NormalizeCredentialMetadata(auth.Metadata)
				return nil
			})
			return true, errSave
		}
		// Path-only token storage retains its existing trusted filesystem boundary.
		if errSave := auth.Storage.SaveTokenToFile(path); errSave != nil {
			return true, errSave
		}
		if persistedBytes, errReadPersisted := fileIO.ReadFile(path); errReadPersisted == nil && len(persistedBytes) > 0 {
			var diskMap map[string]any
			if errUnmarshal := json.Unmarshal(persistedBytes, &diskMap); errUnmarshal == nil && len(diskMap) > 0 {
				auth.Metadata = codex.MergeCodexCredentialsFromDisk(diskMap, auth.Metadata)
				NormalizeCredentialMetadata(auth.Metadata)
			}
		}
		return true, nil

	case auth.Metadata != nil:
		auth.Metadata["disabled"] = auth.Disabled
		errLock := fileIO.WithLock(path, func() error {
			existing, errRead := fileIO.ReadFile(path)
			if errRead == nil && len(existing) > 0 {
				var diskMap map[string]any
				if errUnmarshal := json.Unmarshal(existing, &diskMap); errUnmarshal == nil && len(diskMap) > 0 {
					if !HasAuthCreationIntent(ctx) {
						auth.Metadata = codex.MergeCodexCredentialsFromDisk(diskMap, auth.Metadata)
					}
					auth.Metadata["disabled"] = auth.Disabled
					NormalizeCredentialMetadata(auth.Metadata)
					if raw, errMarshal := json.Marshal(auth.Metadata); errMarshal == nil && !fileIO.WriteUnchanged && string(existing) == string(raw) {
						return nil
					}
				}
			} else if errRead != nil && !os.IsNotExist(errRead) {
				return fmt.Errorf("auth filestore: read existing failed: %w", errRead)
			}
			return fileIO.WriteJSON(path, auth.Metadata)
		})
		if errLock != nil {
			return true, errLock
		}
		return true, nil

	default:
		return false, nil
	}
}
