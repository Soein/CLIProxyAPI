package auth

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type blockingConflictReloadStore struct {
	mu            sync.Mutex
	authoritative *Auth
	persistErr    error
	reloadErr     error
	reloadStarted chan struct{}
	reloadRelease chan struct{}
	reloadOnce    sync.Once
}

func (s *blockingConflictReloadStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return []*Auth{s.authoritative.Clone()}, nil
}

func (s *blockingConflictReloadStore) GetByID(ctx context.Context, id string) (*Auth, error) {
	s.reloadOnce.Do(func() { close(s.reloadStarted) })
	select {
	case <-s.reloadRelease:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reloadErr != nil {
		return nil, s.reloadErr
	}
	if s.authoritative == nil || s.authoritative.ID != id {
		return nil, nil
	}
	return s.authoritative.Clone(), nil
}

func (s *blockingConflictReloadStore) Save(ctx context.Context, auth *Auth) (string, error) {
	path, _, errSave := s.SaveVersioned(ctx, auth, auth.StoreGeneration())
	return path, errSave
}

func (s *blockingConflictReloadStore) SaveVersioned(context.Context, *Auth, uint64) (string, uint64, error) {
	return "", 0, s.persistErr
}

func (*blockingConflictReloadStore) Delete(context.Context, string) error { return nil }

func (s *blockingConflictReloadStore) WithAuthoritativeAuthBatch(ctx context.Context, ids []string, finalize func(map[string]AuthAuthoritativeState) error) error {
	states := make(map[string]AuthAuthoritativeState, len(ids))
	for _, id := range ids {
		auth, errRead := s.GetByID(ctx, id)
		if errRead != nil {
			return errRead
		}
		if auth == nil {
			states[id] = AuthAuthoritativeState{}
			continue
		}
		states[id] = AuthAuthoritativeState{Auth: auth, Exists: true, Generation: auth.StoreGeneration()}
	}
	return finalize(states)
}

type blockingConflictAuthoritativeListStore struct {
	mu            sync.Mutex
	authoritative *Auth
	persistErr    error
	reloadStarted chan struct{}
	reloadRelease chan struct{}
	reloadOnce    sync.Once
}

func (s *blockingConflictAuthoritativeListStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return []*Auth{s.authoritative.Clone()}, nil
}

func (s *blockingConflictAuthoritativeListStore) ListAuthoritative(ctx context.Context) ([]*Auth, error) {
	s.reloadOnce.Do(func() { close(s.reloadStarted) })
	select {
	case <-s.reloadRelease:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return []*Auth{s.authoritative.Clone()}, nil
}

func (s *blockingConflictAuthoritativeListStore) Save(ctx context.Context, auth *Auth) (string, error) {
	path, _, errSave := s.SaveVersioned(ctx, auth, auth.StoreGeneration())
	return path, errSave
}

func (s *blockingConflictAuthoritativeListStore) SaveVersioned(context.Context, *Auth, uint64) (string, uint64, error) {
	return "", 0, s.persistErr
}

func (*blockingConflictAuthoritativeListStore) Delete(context.Context, string) error { return nil }

func (s *blockingConflictAuthoritativeListStore) WithAuthoritativeAuthBatch(ctx context.Context, ids []string, finalize func(map[string]AuthAuthoritativeState) error) error {
	auths, errList := s.ListAuthoritative(ctx)
	if errList != nil {
		return errList
	}
	byID := make(map[string]*Auth, len(auths))
	for _, auth := range auths {
		if auth != nil {
			byID[auth.ID] = auth
		}
	}
	states := make(map[string]AuthAuthoritativeState, len(ids))
	for _, id := range ids {
		auth := byID[id]
		if auth == nil {
			states[id] = AuthAuthoritativeState{}
			continue
		}
		states[id] = AuthAuthoritativeState{Auth: auth.Clone(), Exists: true, Generation: auth.StoreGeneration()}
	}
	return finalize(states)
}

func TestManagerUpdateConflictFailsClosedWhileAuthoritativeReloadIsBlocked(t *testing.T) {
	tests := []struct {
		name       string
		persistErr error
	}{
		{name: "generation conflict", persistErr: ErrAuthStoreConflict},
		{name: "deleted row", persistErr: ErrAuthStoreDeleted},
		{name: "commit outcome unknown", persistErr: ErrAuthStoreCommitUnknown},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authID := "conflict-fail-close-" + string(rune('a'+index))
			model := "conflict-fail-close-model-" + string(rune('a'+index))
			persistErr := test.persistErr
			if errors.Is(persistErr, ErrAuthStoreCommitUnknown) {
				persistErr = NewAuthStoreCommitUnknown(map[string]uint64{authID: 2}, persistErr)
			}
			authoritative := &Auth{
				ID:       authID,
				Provider: "test",
				Status:   StatusActive,
				Metadata: map[string]any{"access_token": "authoritative"},
			}
			authoritative.SetStoreGeneration(2)
			store := &blockingConflictReloadStore{
				authoritative: authoritative,
				persistErr:    persistErr,
				reloadStarted: make(chan struct{}),
				reloadRelease: make(chan struct{}),
			}
			manager := NewManager(store, &RoundRobinSelector{}, nil)
			manager.RegisterExecutor(schedulerTestExecutor{})
			registerSchedulerModels(t, "test", model, authID)
			seed := authoritative.Clone()
			seed.SetStoreGeneration(1)
			if _, errRegister := manager.Register(WithSkipPersist(context.Background()), seed); errRegister != nil {
				t.Fatalf("Register(seed) error = %v", errRegister)
			}

			updateDone := make(chan error, 1)
			go func() {
				_, errUpdate := manager.Update(context.Background(), &Auth{
					ID:       authID,
					Provider: "test",
					Status:   StatusActive,
					Metadata: map[string]any{"access_token": "local-stale"},
				})
				updateDone <- errUpdate
			}()
			<-store.reloadStarted

			current, ok := manager.GetByID(authID)
			if !ok || current == nil || !current.Disabled || current.Status != StatusDisabled {
				t.Fatalf("runtime auth while reload blocked = %#v, want disabled", current)
			}
			if scheduled, errPick := manager.scheduler.pickSingle(context.Background(), "test", model, cliproxyexecutor.Options{}, nil); errPick == nil || scheduled != nil {
				t.Fatalf("scheduler pick while reload blocked = %#v, %v; want no auth", scheduled, errPick)
			}
			if selected, errSelect := manager.SelectAuth(context.Background(), "test", model, cliproxyexecutor.Options{}); errSelect == nil || selected != nil {
				t.Fatalf("SelectAuth() while reload blocked = %#v, %v; want no auth", selected, errSelect)
			}

			close(store.reloadRelease)
			if errUpdate := <-updateDone; !errors.Is(errUpdate, test.persistErr) {
				t.Fatalf("Update() error = %v, want %v", errUpdate, test.persistErr)
			}
			current, ok = manager.GetByID(authID)
			if !ok || current == nil || current.Disabled || current.Status != StatusActive || current.Metadata["access_token"] != "authoritative" {
				t.Fatalf("runtime auth after reload = %#v, want authoritative active auth", current)
			}
			selected, errSelect := manager.SelectAuth(context.Background(), "test", model, cliproxyexecutor.Options{})
			if errSelect != nil || selected == nil || selected.ID != authID {
				t.Fatalf("SelectAuth() after reload = %#v, %v; want %q", selected, errSelect, authID)
			}
		})
	}
}

func TestManagerUpdateConflictFailsClosedWhileAuthoritativeListIsBlocked(t *testing.T) {
	const (
		authID = "conflict-list-fail-close"
		model  = "conflict-list-fail-close-model"
	)
	authoritative := &Auth{
		ID:       authID,
		Provider: "test",
		Status:   StatusActive,
		Metadata: map[string]any{"access_token": "authoritative"},
	}
	authoritative.SetStoreGeneration(2)
	store := &blockingConflictAuthoritativeListStore{
		authoritative: authoritative,
		persistErr:    ErrAuthStoreConflict,
		reloadStarted: make(chan struct{}),
		reloadRelease: make(chan struct{}),
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	manager.RegisterExecutor(schedulerTestExecutor{})
	registerSchedulerModels(t, "test", model, authID)
	seed := authoritative.Clone()
	seed.SetStoreGeneration(1)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), seed); errRegister != nil {
		t.Fatalf("Register(seed) error = %v", errRegister)
	}

	updateDone := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(context.Background(), &Auth{
			ID:       authID,
			Provider: "test",
			Status:   StatusActive,
			Metadata: map[string]any{"access_token": "local-stale"},
		})
		updateDone <- errUpdate
	}()
	<-store.reloadStarted

	current, ok := manager.GetByID(authID)
	if !ok || current == nil || !current.Disabled || current.Status != StatusDisabled {
		t.Fatalf("runtime auth while authoritative list blocked = %#v, want disabled", current)
	}
	if selected, errSelect := manager.SelectAuth(context.Background(), "test", model, cliproxyexecutor.Options{}); errSelect == nil || selected != nil {
		t.Fatalf("SelectAuth() while authoritative list blocked = %#v, %v; want no auth", selected, errSelect)
	}

	close(store.reloadRelease)
	if errUpdate := <-updateDone; !errors.Is(errUpdate, ErrAuthStoreConflict) {
		t.Fatalf("Update() error = %v, want ErrAuthStoreConflict", errUpdate)
	}
	current, ok = manager.GetByID(authID)
	if !ok || current == nil || current.Disabled || current.Status != StatusActive || current.Metadata["access_token"] != "authoritative" {
		t.Fatalf("runtime auth after authoritative list = %#v, want active authoritative auth", current)
	}
}

func TestManagerUpdateConflictReloadErrorRemainsFailedClosed(t *testing.T) {
	const (
		authID = "conflict-reload-error"
		model  = "conflict-reload-error-model"
	)
	reloadErr := errors.New("authoritative writer unavailable")
	authoritative := &Auth{ID: authID, Provider: "test", Status: StatusActive, Metadata: map[string]any{"access_token": "authoritative"}}
	authoritative.SetStoreGeneration(2)
	release := make(chan struct{})
	close(release)
	store := &blockingConflictReloadStore{
		authoritative: authoritative,
		persistErr:    ErrAuthStoreConflict,
		reloadErr:     reloadErr,
		reloadStarted: make(chan struct{}),
		reloadRelease: release,
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	manager.RegisterExecutor(schedulerTestExecutor{})
	registerSchedulerModels(t, "test", model, authID)
	seed := authoritative.Clone()
	seed.SetStoreGeneration(1)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), seed); errRegister != nil {
		t.Fatalf("Register(seed) error = %v", errRegister)
	}

	if _, errUpdate := manager.Update(context.Background(), &Auth{
		ID: authID, Provider: "test", Status: StatusActive, Metadata: map[string]any{"access_token": "local-stale"},
	}); !errors.Is(errUpdate, ErrAuthStoreConflict) {
		t.Fatalf("Update() error = %v, want ErrAuthStoreConflict", errUpdate)
	}
	current, ok := manager.GetByID(authID)
	if !ok || current == nil || !current.Disabled || current.Status != StatusDisabled {
		t.Fatalf("runtime auth after reload error = %#v, want disabled", current)
	}
	if selected, errSelect := manager.SelectAuth(context.Background(), "test", model, cliproxyexecutor.Options{}); errSelect == nil || selected != nil {
		t.Fatalf("SelectAuth() after reload error = %#v, %v; want no auth", selected, errSelect)
	}
}

func TestManagerUpdateConflictReloadTimeoutRemainsFailedClosed(t *testing.T) {
	const authID = "conflict-reload-timeout"
	authoritative := &Auth{ID: authID, Provider: "test", Status: StatusActive, Metadata: map[string]any{"access_token": "authoritative"}}
	authoritative.SetStoreGeneration(2)
	store := &blockingConflictReloadStore{
		authoritative: authoritative,
		persistErr:    ErrAuthStoreConflict,
		reloadStarted: make(chan struct{}),
		reloadRelease: make(chan struct{}),
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	seed := authoritative.Clone()
	seed.SetStoreGeneration(1)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), seed); errRegister != nil {
		t.Fatalf("Register(seed) error = %v", errRegister)
	}

	startedAt := time.Now()
	updateDone := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(context.Background(), &Auth{
			ID: authID, Provider: "test", Status: StatusActive, Metadata: map[string]any{"access_token": "local-stale"},
		})
		updateDone <- errUpdate
	}()
	<-store.reloadStarted
	current, _ := manager.GetByID(authID)
	if current == nil || !current.Disabled || current.Status != StatusDisabled {
		t.Fatalf("runtime auth while reload waits for timeout = %#v, want disabled", current)
	}
	if errUpdate := <-updateDone; !errors.Is(errUpdate, ErrAuthStoreConflict) {
		t.Fatalf("Update() error = %v, want ErrAuthStoreConflict", errUpdate)
	}
	elapsed := time.Since(startedAt)
	if elapsed < authStoreConflictReloadTimeout-500*time.Millisecond || elapsed > authStoreConflictReloadTimeout+2*time.Second {
		t.Fatalf("authoritative reload elapsed = %s, want fixed timeout near %s", elapsed, authStoreConflictReloadTimeout)
	}
	current, _ = manager.GetByID(authID)
	if current == nil || !current.Disabled || current.Status != StatusDisabled {
		t.Fatalf("runtime auth after reload timeout = %#v, want disabled", current)
	}
}
