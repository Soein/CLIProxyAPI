package auth

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type reloadMutationHook struct {
	NoopHook
	manager *Manager
	called  bool
	err     error
}

func (h *reloadMutationHook) update(ctx context.Context, auth *Auth) {
	if h.called {
		return
	}
	h.called = true
	auth.Label = "hook-update"
	_, h.err = h.manager.Update(ctx, auth)
}

func (h *reloadMutationHook) OnAuthRegistered(ctx context.Context, auth *Auth) {
	h.update(ctx, auth)
}

func (h *reloadMutationHook) OnAuthUpdated(ctx context.Context, auth *Auth) {
	h.update(ctx, auth)
}

func TestManagerLoadLifecycleHooksCanReenterMutation(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "registered", true: "updated"}[existing], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				input := &Auth{ID: "load-hook", Provider: "audit", Status: StatusActive, Metadata: map[string]any{"type": "audit"}}
				store := &fakeStore{all: map[string]*Auth{input.ID: input.Clone()}}
				manager := NewManager(store, nil, nil)
				if existing {
					if _, err := manager.Register(WithSkipPersist(ctx), input.Clone()); err != nil {
						t.Fatal(err)
					}
				}
				hook := &reloadMutationHook{manager: manager}
				manager.hook = hook
				done := make(chan error, 1)
				go func() { done <- manager.Load(ctx) }()
				synctest.Wait()
				select {
				case err := <-done:
					if err != nil || hook.err != nil {
						t.Fatalf("load error=%v, hook mutation error=%v", err, hook.err)
					}
				default:
					t.Fatal("Load held the reload barrier during its mutation hook")
				}
				current, _ := manager.GetByID(input.ID)
				if !hook.called || current.Label != "hook-update" {
					t.Fatal("reload hook did not publish its update")
				}
			})
		})
	}
}

func TestManagerLegacyMarkResultAppliesAfterCredentialRotation(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil, nil, nil)
	initial, err := manager.Register(ctx, &Auth{ID: "legacy-result", Provider: "audit", Metadata: map[string]any{"access_token": "old"}})
	if err != nil {
		t.Fatal(err)
	}
	updated := initial.Clone()
	updated.Metadata["access_token"] = "new"
	if _, errUpdate := manager.Update(ctx, updated); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	manager.MarkResult(ctx, Result{AuthID: initial.ID, Provider: initial.Provider, Model: "model", Error: &Error{HTTPStatus: http.StatusServiceUnavailable}})
	current, _ := manager.GetByID(initial.ID)
	if current.Failed != 1 || current.ModelStates["model"] == nil || current.ModelStates["model"].LastError == nil {
		t.Fatal("legacy result without an execution snapshot was discarded")
	}
}

func TestManagerReloadByIDRejectsResultFromPriorCredential(t *testing.T) {
	ctx := context.Background()
	input := &Auth{ID: "reloaded-result", Provider: "audit", Metadata: map[string]any{"access_token": "old"}}
	store := &fakeIndexedStore{fakeStore: &fakeStore{all: map[string]*Auth{input.ID: input.Clone()}}}
	manager := NewManager(store, nil, nil)
	initial, err := manager.Register(WithSkipPersist(ctx), input.Clone())
	if err != nil {
		t.Fatal(err)
	}
	store.all[input.ID].Metadata["access_token"] = "new"
	if errReload := manager.ReloadByID(ctx, input.ID); errReload != nil {
		t.Fatal(errReload)
	}
	manager.MarkResult(withExecutionResultAuth(ctx, initial), Result{AuthID: initial.ID, Provider: initial.Provider, Model: "model", Error: &Error{HTTPStatus: http.StatusServiceUnavailable}})
	current, _ := manager.GetByID(input.ID)
	if current.CredentialVersion <= initial.CredentialVersion || current.Failed != 0 || len(current.ModelStates) != 0 {
		t.Fatal("authoritative credential reload accepted an obsolete request result")
	}
}

func TestManagerStaleUnauthorizedDoesNotRejectReplacementToken(t *testing.T) {
	ctx := context.Background()
	executor := &countingRefreshExecutor{id: issue6199RefreshProvider}
	manager, _ := newIssue6199RefreshLoop(executor)
	previous, err := manager.Register(ctx, &Auth{
		ID: "replaced-same-token", Provider: issue6199RefreshProvider,
		Metadata: map[string]any{"access_token": "same-token", "refresh_token": "refresh"},
	})
	if err != nil {
		t.Fatal(err)
	}
	manager.Remove(ctx, previous.ID)
	if _, errRegister := manager.Register(ctx, previous.Clone()); errRegister != nil {
		t.Fatal(errRegister)
	}
	if _, errRefresh := manager.refreshAuthForRequestAtEpoch(ctx, previous.ID, "same-token", previous.RegistrationEpoch); errRefresh == nil {
		t.Fatal("stale registration epoch was accepted")
	}
	current, _ := manager.GetByID(previous.ID)
	if current.RegistrationEpoch == previous.RegistrationEpoch || current.RejectedAccessToken != "" || !current.HasValidAccessToken(time.Now()) {
		t.Fatal("obsolete unauthorized response rejected the replacement registration's token")
	}
	if executor.refreshCalls.Load() != 0 {
		t.Fatal("obsolete unauthorized response refreshed the replacement registration")
	}
}

func TestNilManagerLoad(t *testing.T) {
	var manager *Manager
	if err := manager.Load(nil); err != nil {
		t.Fatal(err)
	}
}

type blockedConflictRecoveryStore struct {
	*fakeIndexedStore
	entered       chan struct{}
	release       chan struct{}
	failFirstSave bool
	once          sync.Once
}

func (s *blockedConflictRecoveryStore) Save(_ context.Context, auth *Auth) (string, error) {
	firstSave := false
	s.once.Do(func() {
		firstSave = true
		close(s.entered)
	})
	if firstSave {
		<-s.release
		if s.failFirstSave {
			return "", ErrAuthStoreConflict
		}
	}
	s.mu.Lock()
	s.all[auth.ID] = auth.Clone()
	s.mu.Unlock()
	return "", nil
}

func TestManagerConflictRecoveryHooksCanReenterMutation(t *testing.T) {
	operations := []struct {
		name       string
		mode       updateAuthMode
		concurrent bool
	}{
		{name: "reset-quota"},
		{name: "meta-prepare-save-conflict", mode: updateModePrepare},
		{name: "meta-refresh-save-conflict", mode: updateModeRefresh},
		{name: "meta-prepare-concurrent-credential", mode: updateModePrepare, concurrent: true},
		{name: "meta-refresh-concurrent-credential", mode: updateModeRefresh, concurrent: true},
	}
	for _, operation := range operations {
		for _, action := range []string{"Update", "Load"} {
			t.Run(operation.name+"/"+action, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx := context.Background()
					input := &Auth{
						ID: "conflict-hook", Provider: "meta", Status: StatusActive,
						Metadata: map[string]any{"type": "meta", "api_key": "original"},
					}
					store := &blockedConflictRecoveryStore{
						fakeIndexedStore: &fakeIndexedStore{fakeStore: &fakeStore{all: map[string]*Auth{input.ID: input.Clone()}}},
						entered:          make(chan struct{}), release: make(chan struct{}), failFirstSave: !operation.concurrent,
					}
					manager := NewManager(store, nil, nil)
					base, errRegister := manager.Register(WithSkipPersist(ctx), input.Clone())
					if errRegister != nil {
						t.Fatal(errRegister)
					}
					updated := base.Clone()
					updated.Metadata["api_key"] = "minted"
					done := make(chan error, 1)
					go func() {
						if operation.name == "reset-quota" {
							_, _, errReset := manager.ResetQuota(ctx, base.ID)
							done <- errReset
							return
						}
						_, errUpdate := manager.updateInternal(ctx, base, updated, operation.mode)
						done <- errUpdate
					}()
					<-store.entered
					if operation.concurrent {
						current, _ := manager.GetByID(base.ID)
						current.Metadata["api_key"] = "operator"
						if _, errUpdate := manager.Update(WithSkipPersist(ctx), current); errUpdate != nil {
							t.Fatal(errUpdate)
						}
					}
					hook := &reentrantTestHook{}
					called := false
					var errHook error
					hook.onUpdated = func(hookCtx context.Context, auth *Auth) {
						called = true
						hook.onUpdated = nil
						if action == "Load" {
							errHook = manager.Load(hookCtx)
						} else {
							auth.Label = "hook-update"
							_, errHook = manager.Update(hookCtx, auth)
						}
					}
					manager.hook = hook
					close(store.release)
					synctest.Wait()
					select {
					case errOperation := <-done:
						if !errors.Is(errOperation, ErrAuthStoreConflict) {
							t.Fatalf("mutation error = %v, want ErrAuthStoreConflict", errOperation)
						}
					default:
						t.Fatal("conflict recovery held a gate during its hook")
					}
					if !called || errHook != nil {
						t.Fatalf("recovery hook called=%t, error=%v", called, errHook)
					}
					current, exists := manager.GetByID(base.ID)
					if !exists || authIsDisabled(current) || (action == "Update" && current.Label != "hook-update") {
						t.Fatal("recovery hook did not retain its authoritative update")
					}
				})
			})
		}
	}
}
