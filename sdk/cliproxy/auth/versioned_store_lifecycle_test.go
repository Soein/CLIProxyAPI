package auth

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type lifecycleVersionedStore struct {
	mu sync.Mutex

	row                *Auth
	restoreGeneration  uint64
	saveErr            error
	restoreCalls       int
	restoreExpected    uint64
	saveVersionedCalls int
}

func (s *lifecycleVersionedStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.row == nil {
		return nil, nil
	}
	return []*Auth{s.row.Clone()}, nil
}

func (s *lifecycleVersionedStore) GetByID(_ context.Context, id string) (*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.row == nil || s.row.ID != id {
		return nil, nil
	}
	return s.row.Clone(), nil
}

func (s *lifecycleVersionedStore) Save(ctx context.Context, auth *Auth) (string, error) {
	path, _, err := s.SaveVersioned(ctx, auth, auth.StoreGeneration())
	return path, err
}

func (s *lifecycleVersionedStore) SaveVersioned(_ context.Context, auth *Auth, expectedGeneration uint64) (string, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveVersionedCalls++
	if s.saveErr != nil {
		return "", 0, s.saveErr
	}
	if auth == nil {
		return "", 0, nil
	}
	next := expectedGeneration + 1
	s.row = auth.Clone()
	s.row.SetStoreGeneration(next)
	return auth.ID, next, nil
}

func (s *lifecycleVersionedStore) Restore(_ context.Context, auth *Auth, expectedGeneration uint64) (string, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.restoreCalls++
	s.restoreExpected = expectedGeneration
	if auth == nil {
		return "", 0, nil
	}
	generation := s.restoreGeneration
	if generation == 0 {
		generation = 1
	}
	s.row = auth.Clone()
	s.row.SetStoreGeneration(generation)
	return auth.ID, generation, nil
}

type fencedLifecycleStore struct {
	*lifecycleVersionedStore
	fence uint64
	state AuthLifecycleState
}

func (s *fencedLifecycleStore) AuthLifecycleFence(context.Context) (uint64, error) {
	return s.fence, nil
}

func (s *fencedLifecycleStore) GetAuthLifecycle(context.Context, string) (AuthLifecycleState, error) {
	return s.state, nil
}

func (s *lifecycleVersionedStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.row != nil && s.row.ID == id {
		s.row = nil
	}
	return nil
}

func TestManagerRegisterUsesExplicitRestoreForNewVersionedAuth(t *testing.T) {
	store := &lifecycleVersionedStore{restoreGeneration: 7}
	manager := NewManager(store, nil, nil)

	registered, err := manager.Register(context.Background(), &Auth{
		ID:       "restored.json",
		Provider: "xai",
		Metadata: map[string]any{"type": "xai"},
	})
	if err != nil {
		t.Fatalf("Register() error: %v", err)
	}
	if registered == nil || registered.StoreGeneration() != 7 {
		t.Fatalf("Register() generation = %v, want 7", registered)
	}
	if store.restoreCalls != 1 || store.saveVersionedCalls != 0 {
		t.Fatalf("Restore/SaveVersioned calls = %d/%d, want 1/0", store.restoreCalls, store.saveVersionedCalls)
	}
}

func TestManagerRegisterUpdatesAuthoritativeActiveRowWhenRuntimeWasMissing(t *testing.T) {
	current := &Auth{
		ID:       "active-without-runtime.json",
		Provider: "xai",
		Metadata: map[string]any{"type": "xai", "token": "old"},
	}
	current.SetStoreGeneration(5)
	store := &lifecycleVersionedStore{row: current.Clone()}
	manager := NewManager(store, nil, nil)

	registered, err := manager.Register(context.Background(), &Auth{
		ID:       current.ID,
		Provider: "xai",
		Metadata: map[string]any{"type": "xai", "token": "new"},
	})
	if err != nil {
		t.Fatalf("Register() error: %v", err)
	}
	if registered == nil || registered.StoreGeneration() != 6 {
		t.Fatalf("Register() generation = %v, want 6", registered)
	}
	if store.saveVersionedCalls != 1 || store.restoreCalls != 0 {
		t.Fatalf("SaveVersioned/Restore calls = %d/%d, want 1/0", store.saveVersionedCalls, store.restoreCalls)
	}
}

func TestManagerWatcherRegisterCannotRestoreVersionedAuth(t *testing.T) {
	store := &lifecycleVersionedStore{restoreGeneration: 7}
	manager := NewManager(store, nil, nil)

	if _, err := manager.Register(WithSkipPersist(context.Background()), &Auth{
		ID:       "watcher.json",
		Provider: "xai",
		Metadata: map[string]any{"type": "xai"},
	}); err != nil {
		t.Fatalf("Register(WithSkipPersist) error: %v", err)
	}
	if store.restoreCalls != 0 || store.saveVersionedCalls != 0 {
		t.Fatalf("Restore/SaveVersioned calls = %d/%d, want 0/0", store.restoreCalls, store.saveVersionedCalls)
	}
}

func TestManagerDeletedSaveReloadsAndDropsRuntimeAuth(t *testing.T) {
	store := &lifecycleVersionedStore{saveErr: ErrAuthStoreDeleted}
	manager := NewManager(store, nil, nil)
	seed := &Auth{
		ID:       "deleted.json",
		Provider: "xai",
		Metadata: map[string]any{"type": "xai"},
	}
	seed.SetStoreGeneration(4)
	if _, err := manager.Register(WithSkipPersist(context.Background()), seed); err != nil {
		t.Fatalf("seed Register() error: %v", err)
	}

	update := seed.Clone()
	update.Label = "stale update"
	_, err := manager.Update(context.Background(), update)
	if !errors.Is(err, ErrAuthStoreDeleted) {
		t.Fatalf("Update() error = %v, want ErrAuthStoreDeleted", err)
	}
	if current, ok := manager.GetByID(seed.ID); ok || current != nil {
		t.Fatalf("deleted auth remained in runtime: %#v", current)
	}
}

func TestPersistExplicitAuthUpdatesActiveRowWithAuthoritativeGeneration(t *testing.T) {
	current := &Auth{
		ID:       "active.json",
		Provider: "xai",
		Metadata: map[string]any{"type": "xai", "token": "old"},
	}
	current.SetStoreGeneration(5)
	store := &lifecycleVersionedStore{row: current}
	incoming := &Auth{
		ID:       current.ID,
		Provider: "xai",
		Metadata: map[string]any{"type": "xai", "token": "new"},
	}

	path, err := PersistExplicitAuth(context.Background(), store, incoming)
	if err != nil {
		t.Fatalf("PersistExplicitAuth() error: %v", err)
	}
	if path != current.ID || incoming.StoreGeneration() != 6 {
		t.Fatalf("PersistExplicitAuth() = (%q, generation %d), want (%q, 6)", path, incoming.StoreGeneration(), current.ID)
	}
	if store.saveVersionedCalls != 1 || store.restoreCalls != 0 {
		t.Fatalf("SaveVersioned/Restore calls = %d/%d, want 1/0", store.saveVersionedCalls, store.restoreCalls)
	}
}

func TestPersistExplicitAuthRestoresMissingOrTombstonedRow(t *testing.T) {
	store := &lifecycleVersionedStore{restoreGeneration: 9}
	incoming := &Auth{
		ID:       "tombstoned.json",
		Provider: "xai",
		Metadata: map[string]any{"type": "xai", "token": "new"},
	}

	path, err := PersistExplicitAuth(context.Background(), store, incoming)
	if err != nil {
		t.Fatalf("PersistExplicitAuth() error: %v", err)
	}
	if path != incoming.ID || incoming.StoreGeneration() != 9 {
		t.Fatalf("PersistExplicitAuth() = (%q, generation %d), want (%q, 9)", path, incoming.StoreGeneration(), incoming.ID)
	}
	if store.restoreCalls != 1 || store.saveVersionedCalls != 0 {
		t.Fatalf("Restore/SaveVersioned calls = %d/%d, want 1/0", store.restoreCalls, store.saveVersionedCalls)
	}
}

func TestManagerSetDisabledDeletedStoreRowDropsRuntimeAuth(t *testing.T) {
	for _, disabling := range []bool{true, false} {
		disabling := disabling
		t.Run(map[bool]string{true: "disable", false: "enable"}[disabling], func(t *testing.T) {
			store := &lifecycleVersionedStore{saveErr: ErrAuthStoreDeleted}
			manager := NewManager(store, nil, nil)
			seed := &Auth{
				ID:       "deleted-status.json",
				Provider: "xai",
				Disabled: !disabling,
				Status:   StatusActive,
				Metadata: map[string]any{"type": "xai", "disabled": !disabling},
			}
			if seed.Disabled {
				seed.Status = StatusDisabled
			}
			seed.SetStoreGeneration(4)
			if _, errRegister := manager.Register(WithSkipPersist(context.Background()), seed); errRegister != nil {
				t.Fatalf("seed Register() error: %v", errRegister)
			}

			_, errSet := manager.SetDisabled(context.Background(), []string{seed.ID}, disabling)
			if !errors.Is(errSet, ErrAuthStoreDeleted) {
				t.Fatalf("SetDisabled() error = %v, want ErrAuthStoreDeleted", errSet)
			}
			if current, exists := manager.GetByID(seed.ID); exists || current != nil {
				t.Fatalf("deleted auth remained after SetDisabled: %#v", current)
			}
		})
	}
}

func TestPersistExplicitAuthLifecycleFenceProtectsDeleteAndRestore(t *testing.T) {
	const fence uint64 = 10
	newAuth := func() *Auth {
		return &Auth{ID: "fenced.json", Provider: "xai", Metadata: map[string]any{"type": "xai"}}
	}

	t.Run("delete after operation wins", func(t *testing.T) {
		base := &lifecycleVersionedStore{restoreGeneration: 7}
		store := &fencedLifecycleStore{
			lifecycleVersionedStore: base,
			fence:                   fence,
			state: AuthLifecycleState{
				Exists: true, Deleted: true, Generation: 6, LifecycleVersion: fence + 1,
			},
		}
		ctx := WithExplicitAuthOperationFence(context.Background(), fence)
		if _, err := PersistExplicitAuth(ctx, store, newAuth()); !errors.Is(err, ErrAuthStoreDeleted) {
			t.Fatalf("PersistExplicitAuth() error = %v, want ErrAuthStoreDeleted", err)
		}
		if base.restoreCalls != 0 {
			t.Fatalf("Restore calls = %d, want 0", base.restoreCalls)
		}
	})

	t.Run("preexisting tombstone restores exact generation", func(t *testing.T) {
		base := &lifecycleVersionedStore{restoreGeneration: 7}
		store := &fencedLifecycleStore{
			lifecycleVersionedStore: base,
			fence:                   fence,
			state: AuthLifecycleState{
				Exists: true, Deleted: true, Generation: 6, LifecycleVersion: fence - 1,
			},
		}
		ctx, errBegin := BeginExplicitAuthOperation(context.Background(), store)
		if errBegin != nil {
			t.Fatalf("BeginExplicitAuthOperation() error: %v", errBegin)
		}
		auth := newAuth()
		if _, err := PersistExplicitAuth(ctx, store, auth); err != nil {
			t.Fatalf("PersistExplicitAuth() error: %v", err)
		}
		if base.restoreCalls != 1 || base.restoreExpected != 6 || auth.StoreGeneration() != 7 {
			t.Fatalf("Restore calls/expected/generation = %d/%d/%d, want 1/6/7", base.restoreCalls, base.restoreExpected, auth.StoreGeneration())
		}
	})

	t.Run("active update after operation wins", func(t *testing.T) {
		base := &lifecycleVersionedStore{}
		store := &fencedLifecycleStore{
			lifecycleVersionedStore: base,
			fence:                   fence,
			state: AuthLifecycleState{
				Exists: true, Generation: 8, LifecycleVersion: fence + 1,
			},
		}
		ctx := WithExplicitAuthOperationFence(context.Background(), fence)
		if _, err := PersistExplicitAuth(ctx, store, newAuth()); !errors.Is(err, ErrAuthStoreConflict) {
			t.Fatalf("PersistExplicitAuth() error = %v, want ErrAuthStoreConflict", err)
		}
		if base.saveVersionedCalls != 0 {
			t.Fatalf("SaveVersioned calls = %d, want 0", base.saveVersionedCalls)
		}
	})
}
