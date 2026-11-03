package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	baseauth "github.com/router-for-me/CLIProxyAPI/v8/internal/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/misc"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/auth"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func validateMirroredCredentialUpdate(ctx context.Context, path string) (bool, error) {
	guarded := false
	errValidate := cliproxyauth.ValidateAuthFileUpdate(ctx, path, func() ([]byte, error) {
		guarded = true
		data, errRead := os.ReadFile(path)
		if os.IsNotExist(errRead) {
			return nil, nil
		}
		return data, errRead
	})
	return guarded, errValidate
}

func readMirroredCredentialForUpdate(ctx context.Context, path string) ([]byte, error) {
	data, errRead := os.ReadFile(path)
	if errRead == nil || os.IsNotExist(errRead) {
		if errValidate := cliproxyauth.ValidateAuthFileUpdate(ctx, path, func() ([]byte, error) { return data, nil }); errValidate != nil {
			return nil, errValidate
		}
	}
	return data, errRead
}

// persistMirroredCredential runs under the backend's existing auth-file lock.
// Only exact serialized payloads can establish a successful-save witness.
func persistMirroredCredential(ctx context.Context, path string, auth *cliproxyauth.Auth) error {
	guarded, errValidate := validateMirroredCredentialUpdate(ctx, path)
	if errValidate != nil {
		return errValidate
	}
	if guarded && auth.Storage != nil {
		storage, okClone := sdkauth.CloneTokenStorageForUpdate(auth.Storage)
		if _, okMarshal := storage.(baseauth.TokenJSONMarshaler); !okClone || !okMarshal {
			return fmt.Errorf("auth filestore: token storage must support private cloning and JSON serialization for edits: %w", cliproxyauth.ErrAuthStorageEditUnsupported)
		}
		auth.Storage = storage
	}
	if handled, errCodex := cliproxyauth.PersistCodexAuthFileWithIO(ctx, path, auth, cliproxyauth.CodexAuthFileIO{
		WithLock:  codex.WithFileLock,
		ReadFile:  func(path string) ([]byte, error) { return readMirroredCredentialForUpdate(ctx, path) },
		WriteJSON: codex.WriteJSONFileAtomic,
	}); handled {
		return errCodex
	}

	switch {
	case auth.Storage != nil:
		if auth.Metadata == nil {
			auth.Metadata = make(map[string]any)
		}
		auth.Metadata["disabled"] = auth.Disabled
		if setter, ok := auth.Storage.(interface{ SetMetadata(map[string]any) }); ok {
			setter.SetMetadata(auth.Metadata)
		}
		if marshaler, ok := auth.Storage.(baseauth.TokenJSONMarshaler); ok {
			payload, errPayload := marshaler.MarshalTokenJSON()
			if errPayload != nil {
				return fmt.Errorf("auth filestore: marshal token payload: %w", errPayload)
			}
			if !json.Valid(payload) {
				return fmt.Errorf("auth filestore: token payload is not valid JSON")
			}
			misc.LogSavingCredentials(path)
			return writeMirroredCredentialPayload(ctx, path, auth, payload)
		}
		return auth.Storage.SaveTokenToFile(path)
	case auth.Metadata != nil:
		auth.Metadata["disabled"] = auth.Disabled
		payload, errMarshal := json.Marshal(auth.Metadata)
		if errMarshal != nil {
			return fmt.Errorf("auth filestore: marshal metadata failed: %w", errMarshal)
		}
		return writeMirroredCredentialPayload(ctx, path, auth, payload)
	default:
		return fmt.Errorf("auth filestore: nothing to persist for %s", auth.ID)
	}
}

func writeMirroredCredentialPayload(ctx context.Context, path string, auth *cliproxyauth.Auth, payload []byte) error {
	if existing, errRead := readMirroredCredentialForUpdate(ctx, path); errRead == nil {
		if jsonEqual(existing, payload) {
			auth.RecordPersistedFileSnapshot(path, existing)
			return nil
		}
	} else if !os.IsNotExist(errRead) {
		return fmt.Errorf("auth filestore: read existing failed: %w", errRead)
	}
	if auth.Storage != nil {
		if errWrite := sdkauth.WriteTokenPayload("", path, payload, auth.Storage); errWrite != nil {
			return fmt.Errorf("auth filestore: write token payload: %w", errWrite)
		}
	} else {
		tmp := path + ".tmp"
		if errWrite := os.WriteFile(tmp, payload, 0o600); errWrite != nil {
			return fmt.Errorf("auth filestore: write temp failed: %w", errWrite)
		}
		if errRename := os.Rename(tmp, path); errRename != nil {
			return fmt.Errorf("auth filestore: rename failed: %w", errRename)
		}
	}
	auth.RecordPersistedFileSnapshot(path, payload)
	return nil
}
