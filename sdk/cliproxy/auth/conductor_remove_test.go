package auth

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type blockingAuthDeleteStore struct {
	mu            sync.Mutex
	stored        *Auth
	deleteStarted chan struct{}
	allowDelete   chan struct{}
	deleteErr     error
}

type blockingOlderSaveStore struct {
	mu            sync.Mutex
	stored        *Auth
	saveStarted   chan struct{}
	allowSave     chan struct{}
	deleteStarted chan struct{}
}

type persistenceReentrantRemovalHook struct {
	manager *Manager
	called  chan struct{}
}

func (*persistenceReentrantRemovalHook) OnAuthRegistered(context.Context, *Auth) {}
func (*persistenceReentrantRemovalHook) OnAuthUpdated(context.Context, *Auth)    {}
func (*persistenceReentrantRemovalHook) OnResult(context.Context, Result)        {}

func (h *persistenceReentrantRemovalHook) OnAuthRemoved(_ context.Context, authID string) {
	lock := h.manager.authPersistLock(authID)
	lock.Lock()
	lock.Unlock()
	close(h.called)
}

func newBlockingOlderSaveStore(auth *Auth) *blockingOlderSaveStore {
	return &blockingOlderSaveStore{
		stored:        auth.Clone(),
		saveStarted:   make(chan struct{}),
		allowSave:     make(chan struct{}),
		deleteStarted: make(chan struct{}),
	}
}

func (s *blockingOlderSaveStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stored == nil {
		return nil, nil
	}
	return []*Auth{s.stored.Clone()}, nil
}

func (s *blockingOlderSaveStore) Save(_ context.Context, auth *Auth) (string, error) {
	close(s.saveStarted)
	<-s.allowSave
	s.mu.Lock()
	s.stored = auth.Clone()
	s.mu.Unlock()
	return auth.ID, nil
}

func (s *blockingOlderSaveStore) Delete(context.Context, string) error {
	close(s.deleteStarted)
	s.mu.Lock()
	s.stored = nil
	s.mu.Unlock()
	return nil
}

func newBlockingAuthDeleteStore(auth *Auth) *blockingAuthDeleteStore {
	return &blockingAuthDeleteStore{
		stored:        auth.Clone(),
		deleteStarted: make(chan struct{}),
		allowDelete:   make(chan struct{}),
	}
}

func (s *blockingAuthDeleteStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stored == nil {
		return nil, nil
	}
	return []*Auth{s.stored.Clone()}, nil
}

func (s *blockingAuthDeleteStore) Save(_ context.Context, auth *Auth) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stored = auth.Clone()
	return auth.ID, nil
}

func (s *blockingAuthDeleteStore) Delete(context.Context, string) error {
	close(s.deleteStarted)
	<-s.allowDelete
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.mu.Lock()
	s.stored = nil
	s.mu.Unlock()
	return nil
}

func TestManager_Remove_DeletesRuntimeAuth(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	ctx := context.Background()

	auth := &Auth{
		ID:       "remove-runtime-auth",
		Provider: "claude",
		Status:   StatusActive,
		Metadata: map[string]any{"email": "x@example.com"},
	}
	if _, errRegister := manager.Register(ctx, auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	manager.Remove(ctx, auth.ID)

	if _, ok := manager.GetByID(auth.ID); ok {
		t.Fatalf("expected auth %q to be removed", auth.ID)
	}
}

func TestManager_DeleteAuths_SerializesDeleteWithOlderSave(t *testing.T) {
	ctx := context.Background()
	auth := &Auth{
		ID:       "atomic-delete-auth",
		Provider: "codex",
		Status:   StatusActive,
		Metadata: map[string]any{"access_token": "old"},
	}
	store := newBlockingAuthDeleteStore(auth)
	manager := NewManager(store, nil, nil)
	if _, errRegister := manager.Register(WithSkipPersist(ctx), auth.Clone()); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- manager.DeleteAuths(ctx, []string{auth.ID}, func(deleteCtx context.Context) error {
			return store.Delete(deleteCtx, auth.ID)
		})
	}()
	<-store.deleteStarted

	updated := auth.Clone()
	updated.Metadata["access_token"] = "new"
	updateDone := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(ctx, updated)
		updateDone <- errUpdate
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, ok := manager.GetByID(auth.ID)
		if ok && current.Metadata["access_token"] == "new" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("concurrent Update did not publish before delete completed")
		}
		time.Sleep(time.Millisecond)
	}

	close(store.allowDelete)
	if errDelete := <-deleteDone; errDelete != nil {
		t.Fatalf("DeleteAuths() error = %v", errDelete)
	}
	if errUpdate := <-updateDone; errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	if _, ok := manager.GetByID(auth.ID); ok {
		t.Fatalf("runtime auth %q was restored by an older Save", auth.ID)
	}
	store.mu.Lock()
	persisted := store.stored
	store.mu.Unlock()
	if persisted != nil {
		t.Fatalf("persisted auth was restored after delete: %#v", persisted)
	}
	manager.mu.RLock()
	_, marked := manager.persistenceInFlightRevisions[auth.ID]
	manager.mu.RUnlock()
	if marked {
		t.Fatalf("persistence generation marker for %q was not cleared", auth.ID)
	}
}

func TestManager_DeleteAuths_WaitsForOlderSaveThenDeletesLast(t *testing.T) {
	ctx := context.Background()
	auth := &Auth{
		ID:       "older-save-delete-auth",
		Provider: "codex",
		Status:   StatusActive,
		Metadata: map[string]any{"access_token": "old"},
	}
	store := newBlockingOlderSaveStore(auth)
	manager := NewManager(store, nil, nil)
	if _, errRegister := manager.Register(WithSkipPersist(ctx), auth.Clone()); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	updated := auth.Clone()
	updated.Metadata["access_token"] = "new"
	updateDone := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(ctx, updated)
		updateDone <- errUpdate
	}()
	<-store.saveStarted

	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- manager.DeleteAuths(ctx, []string{auth.ID}, func(deleteCtx context.Context) error {
			return store.Delete(deleteCtx, auth.ID)
		})
	}()
	select {
	case <-store.deleteStarted:
		t.Fatal("persistent delete ran while the older Save still held the per-auth lock")
	case <-time.After(25 * time.Millisecond):
	}

	close(store.allowSave)
	if errUpdate := <-updateDone; errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	if errDelete := <-deleteDone; errDelete != nil {
		t.Fatalf("DeleteAuths() error = %v", errDelete)
	}
	if _, ok := manager.GetByID(auth.ID); ok {
		t.Fatalf("runtime auth %q remained after ordered delete", auth.ID)
	}
	store.mu.Lock()
	persisted := store.stored
	store.mu.Unlock()
	if persisted != nil {
		t.Fatalf("older Save recreated persisted auth after delete: %#v", persisted)
	}
}

func TestManager_DeleteAuths_PersistenceFailureKeepsRuntime(t *testing.T) {
	ctx := context.Background()
	auth := &Auth{
		ID:       "failed-delete-auth",
		Provider: "codex",
		Status:   StatusActive,
		Metadata: map[string]any{"access_token": "token"},
	}
	deleteErr := errors.New("delete failed")
	store := newBlockingAuthDeleteStore(auth)
	store.deleteErr = deleteErr
	manager := NewManager(store, nil, nil)
	if _, errRegister := manager.Register(WithSkipPersist(ctx), auth.Clone()); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- manager.DeleteAuths(ctx, []string{auth.ID}, func(deleteCtx context.Context) error {
			return store.Delete(deleteCtx, auth.ID)
		})
	}()
	<-store.deleteStarted
	close(store.allowDelete)
	if errDelete := <-deleteDone; !errors.Is(errDelete, deleteErr) {
		t.Fatalf("DeleteAuths() error = %v, want errors.Is(_, %v)", errDelete, deleteErr)
	}
	if _, ok := manager.GetByID(auth.ID); !ok {
		t.Fatalf("runtime auth %q removed after persistence failure", auth.ID)
	}
}

func TestManager_DeleteAuths_CommitUnknownReloadsAuthoritativeDeletion(t *testing.T) {
	ctx := context.Background()
	auth := &Auth{
		ID:       "unknown-delete-auth",
		Provider: "codex",
		Status:   StatusActive,
		Metadata: map[string]any{"access_token": "token"},
	}
	store := newBlockingAuthDeleteStore(auth)
	manager := NewManager(store, nil, nil)
	if _, errRegister := manager.Register(WithSkipPersist(ctx), auth.Clone()); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	errDelete := manager.DeleteAuths(ctx, []string{auth.ID}, func(context.Context) error {
		store.mu.Lock()
		store.stored = nil
		store.mu.Unlock()
		return ErrAuthStoreCommitUnknown
	})
	if !errors.Is(errDelete, ErrAuthStoreCommitUnknown) {
		t.Fatalf("DeleteAuths() error = %v, want outcome unknown", errDelete)
	}
	if current, ok := manager.GetByID(auth.ID); ok || current != nil {
		t.Fatalf("authoritatively deleted auth remained after outcome unknown: %#v", current)
	}
}

func TestManager_DeleteAuths_ReleasesPersistenceLockBeforeRemovalHook(t *testing.T) {
	ctx := context.Background()
	hook := &persistenceReentrantRemovalHook{called: make(chan struct{})}
	manager := NewManager(nil, nil, hook)
	hook.manager = manager
	auth := &Auth{
		ID:       "reentrant-removal-hook",
		Provider: "codex",
		Status:   StatusActive,
		Metadata: map[string]any{"access_token": "token"},
	}
	if _, errRegister := manager.Register(WithSkipPersist(ctx), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- manager.DeleteAuths(ctx, []string{auth.ID}, func(context.Context) error { return nil })
	}()
	select {
	case errDelete := <-deleteDone:
		if errDelete != nil {
			t.Fatalf("DeleteAuths() error = %v", errDelete)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DeleteAuths deadlocked while removal hook re-entered the per-auth persistence lock")
	}
	select {
	case <-hook.called:
	default:
		t.Fatal("removal hook was not called")
	}
}

func TestManager_DeleteAuths_StaleMarkResultSnapshotCannotRestoreScheduler(t *testing.T) {
	ctx := WithSkipPersist(context.Background())
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	auth := &Auth{
		ID:       "stale-mark-result-delete",
		Provider: "gemini",
		Status:   StatusActive,
		Metadata: map[string]any{"access_token": "token"},
	}
	if _, errRegister := manager.Register(ctx, auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	manager.MarkResult(ctx, Result{AuthID: auth.ID, Provider: auth.Provider, Success: true})
	staleSnapshot, ok := manager.GetByID(auth.ID)
	if !ok {
		t.Fatalf("GetByID(%q) found = false", auth.ID)
	}

	if errDelete := manager.DeleteAuths(ctx, []string{auth.ID}, func(context.Context) error { return nil }); errDelete != nil {
		t.Fatalf("DeleteAuths() error = %v", errDelete)
	}
	// Model the schedulerUpsert that was already queued by MarkResult when the
	// delete committed. Its snapshot predates the deletion tombstone.
	manager.schedulerUpsert(staleSnapshot)

	manager.scheduler.mu.Lock()
	_, scheduled := manager.scheduler.authProviders[auth.ID]
	version := manager.scheduler.authVersions[auth.ID]
	manager.scheduler.mu.Unlock()
	if scheduled {
		t.Fatalf("stale MarkResult snapshot restored scheduler auth %q", auth.ID)
	}
	if !version.disabled || version.revision <= staleSnapshot.revision {
		t.Fatalf("scheduler deletion watermark = %#v, want disabled revision > %d", version, staleSnapshot.revision)
	}
}

func TestManager_Update_MissingAuthIsNoOp(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	ctx := context.Background()

	auth := &Auth{
		ID:       "missing-update-auth",
		Provider: "claude",
		Status:   StatusActive,
	}
	if _, errRegister := manager.Register(ctx, auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	manager.Remove(ctx, auth.ID)

	updated, errUpdate := manager.Update(ctx, &Auth{
		ID:       auth.ID,
		Provider: "claude",
		Status:   StatusDisabled,
		Disabled: true,
	})
	if errUpdate != nil {
		t.Fatalf("update removed auth: %v", errUpdate)
	}
	if updated != nil {
		t.Fatalf("expected update on removed auth to be no-op, got %#v", updated)
	}
	if _, ok := manager.GetByID(auth.ID); ok {
		t.Fatalf("expected removed auth to stay absent after late update")
	}
}

func TestManager_Remove_UnschedulesAutoRefresh(t *testing.T) {
	ctx := context.Background()

	manager := NewManager(nil, nil, nil)
	loop := newAuthAutoRefreshLoop(manager, time.Second, 1)
	manager.mu.Lock()
	manager.refreshLoop = loop
	manager.mu.Unlock()

	lead := 10 * time.Minute
	setRefreshLeadFactory(t, "provider-lead-expiry", func() *time.Duration {
		d := lead
		return &d
	})

	auth := &Auth{
		ID:       "remove-refresh-auth",
		Provider: "provider-lead-expiry",
		Metadata: map[string]any{
			"email":      "x@example.com",
			"expires_at": time.Now().Add(time.Hour).Format(time.RFC3339),
		},
	}
	if _, errRegister := manager.Register(ctx, auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	now := time.Now()
	if _, ok := nextRefreshCheckAt(now, auth, time.Second); !ok {
		t.Fatalf("expected auth to be scheduled before removal")
	}
	loop.applyDirty(now)
	loop.mu.Lock()
	if _, ok := loop.index[auth.ID]; !ok {
		loop.mu.Unlock()
		t.Fatalf("expected auth %q to be present in auto-refresh index before removal", auth.ID)
	}
	loop.mu.Unlock()

	manager.Remove(ctx, auth.ID)

	if _, ok := manager.GetByID(auth.ID); ok {
		t.Fatalf("expected auth to be removed")
	}
	loop.mu.Lock()
	if _, ok := loop.index[auth.ID]; ok {
		loop.mu.Unlock()
		t.Fatalf("expected auth %q to be removed from auto-refresh index", auth.ID)
	}
	loop.mu.Unlock()
}
