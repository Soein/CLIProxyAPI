package auth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
)

type authFileUpdateBaseKey struct{}

// ErrAuthFileSourceUnavailable marks an edit whose source was removed or emptied.
// Reloading a mirror-backed store may recreate the file, so callers must wait
// for a fresh file observation instead of performing a synchronizing store list.
var ErrAuthFileSourceUnavailable = errors.New("auth file source is missing or empty")

// ErrAuthStorageEditUnsupported reports a writer that cannot isolate an edit.
// This is a capability failure, not evidence that the credential changed.
var ErrAuthStorageEditUnsupported = errors.New("auth token storage does not support private metadata edits")

// IsAuthModification reports a store call made for a private Manager.Modify edit.
func IsAuthModification(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	base, _ := ctx.Value(authFileUpdateBaseKey{}).(*Auth)
	return base != nil
}

func withAuthFileUpdateBase(ctx context.Context, base *Auth) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, authFileUpdateBaseKey{}, base)
}

// A pending management disable is published before waiting for file persistence.
// Its status intent must survive a metadata edit that was already saving.
func (m *Manager) canMergePendingDisableLocked(before, current *Auth) bool {
	if before == nil || current == nil || authIsDisabled(before) || !authIsDisabled(current) {
		return false
	}
	if _, pending := m.pendingDisabledPersistence[current.ID]; !pending {
		return false
	}
	if before.CredentialVersion != current.CredentialVersion || before.StoreGeneration() != current.StoreGeneration() ||
		before.Provider != current.Provider || before.Prefix != current.Prefix || before.ProxyURL != current.ProxyURL || before.FileName != current.FileName || before.Label != current.Label ||
		!reflect.DeepEqual(before.Attributes, current.Attributes) || !reflect.DeepEqual(before.Storage, current.Storage) || !reflect.DeepEqual(before.ProxyConnectionPool, current.ProxyConnectionPool) {
		return false
	}
	oldMetadata, currentMetadata := cloneMetadataMap(before.Metadata), cloneMetadataMap(current.Metadata)
	delete(oldMetadata, "disabled")
	delete(currentMetadata, "disabled")
	return reflect.DeepEqual(oldMetadata, currentMetadata)
}

// ValidateAuthFileUpdate rejects a manager edit when the file no longer matches
// the source observed by its callback. Stores must call it while holding their
// read-modify-write lock, before injecting metadata or writing token storage.
// readFile is only called for a Manager.Modify edit that needs source validation.
func ValidateAuthFileUpdate(ctx context.Context, path string, readFile func() ([]byte, error)) error {
	if ctx == nil {
		return nil
	}
	base, _ := ctx.Value(authFileUpdateBaseKey{}).(*Auth)
	if base == nil {
		return nil
	}
	data, errRead := readFile()
	if errRead != nil {
		return fmt.Errorf("read auth edit source: %w", errRead)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return errors.Join(ErrAuthStoreConflict, ErrAuthFileSourceUnavailable)
	}
	if base.fileUpdatePath != path || base.fileUpdateDigest == "" || canonicalAuthFileDigest(data) != base.fileUpdateDigest {
		return fmt.Errorf("auth file changed since it was loaded; reload and retry the edit: %w", ErrAuthStoreConflict)
	}
	return nil
}

func inheritAuthFileUpdateSource(current, updated *Auth) {
	if updated.fileUpdateDigest != "" || current.fileUpdateDigest == "" || current.Provider != updated.Provider ||
		current.FileSnapshotPath() != updated.FileSnapshotPath() || current.Disabled != updated.Disabled || current.Prefix != updated.Prefix || current.ProxyURL != updated.ProxyURL {
		return
	}
	base, next := cloneMetadataMap(current.Metadata), cloneMetadataMap(updated.Metadata)
	for _, metadata := range []map[string]any{base, next} {
		if disabled, ok := metadata["disabled"].(bool); ok && !disabled {
			delete(metadata, "disabled")
		}
	}
	if reflect.DeepEqual(base, next) {
		updated.fileUpdatePath, updated.fileUpdateDigest = current.fileUpdatePath, current.fileUpdateDigest
	}
}
