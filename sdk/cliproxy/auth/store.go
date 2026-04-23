package auth

import "context"

// Store abstracts persistence of Auth state across restarts.
type Store interface {
	// List returns all auth records stored in the backend.
	List(ctx context.Context) ([]*Auth, error)
	// Save persists the provided auth record, replacing any existing one with same ID.
	Save(ctx context.Context, auth *Auth) (string, error)
	// Delete removes the auth record identified by id.
	Delete(ctx context.Context, id string) error
}

// GetStore exposes the underlying store so hosts can feature-detect optional
// capabilities (e.g. Postgres-backed stores also implement DB()/DSN() for the
// cluster package). Returns nil when the Manager has no store wired.
func (m *Manager) GetStore() Store {
	if m == nil {
		return nil
	}
	return m.store
}
