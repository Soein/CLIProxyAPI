package store

import (
	"context"
	"fmt"
)

// PersistConfigSnapshot uploads accepted, sanitized config bytes without reading or
// replacing the local config file. Empty snapshots are not deletion requests.
func (s *ObjectTokenStore) PersistConfigSnapshot(ctx context.Context, snapshot []byte) error {
	if len(snapshot) == 0 {
		return fmt.Errorf("object store: empty config snapshot")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putObject(ctx, objectStoreConfigKey, snapshot, "application/x-yaml")
}

// PersistConfigSnapshot mirrors accepted, sanitized config bytes without reading or
// replacing the local config file. Empty snapshots are not deletion requests.
func (s *PostgresStore) PersistConfigSnapshot(ctx context.Context, snapshot []byte) error {
	if len(snapshot) == 0 {
		return fmt.Errorf("postgres store: empty config snapshot")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persistConfig(ctx, snapshot)
}
