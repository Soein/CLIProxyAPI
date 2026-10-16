package auth

import (
	"context"
	"net/http"
	"testing"
	"testing/synctest"
	"time"
)

func TestRegistrationPublicationSerializesWatcherUpdate(t *testing.T) {
	ctx := WithSkipPersist(context.Background())
	manager := NewManager(nil, nil, nil)
	base, err := manager.Register(ctx, &Auth{ID: "publication-watcher", Provider: "audit", Prefix: "old"})
	if err != nil {
		t.Fatal(err)
	}
	updated := base.Clone()
	updated.Prefix = "new"
	updateDone := make(chan error, 1)
	blockedWatcher := false
	published := manager.PublishIfRegistrationCurrent(base.ID, base.RegistrationEpoch, base.Prefix, base.Provider, false, base.AuthKind(), base.Attributes, func() bool {
		// A watcher bypasses the mutation gate and commits under m.mu. Probe
		// that exact commit barrier without timers or scheduler assumptions.
		if manager.mu.TryLock() {
			manager.mu.Unlock()
		} else {
			blockedWatcher = true
		}
		go func() {
			_, errUpdate := manager.Update(ctx, updated)
			updateDone <- errUpdate
		}()
		return true
	})
	if errUpdate := <-updateDone; errUpdate != nil {
		t.Fatal(errUpdate)
	}
	if !published || !blockedWatcher {
		t.Fatal("publication did not protect its identity from watcher commits")
	}
	called := false
	if manager.PublishIfRegistrationCurrent(base.ID, base.RegistrationEpoch, base.Prefix, base.Provider, false, base.AuthKind(), base.Attributes, func() bool { called = true; return true }) || called {
		t.Fatal("old registration published after watcher changed its prefix")
	}
}

func TestRegistrationUpdateRechecksWatcherIdentityAtCommit(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil, nil, nil)
	base, err := manager.Register(ctx, &Auth{ID: "update-watcher", Provider: "audit", Prefix: "old"})
	if err != nil {
		t.Fatal(err)
	}
	releaseMutation := manager.lockAuthMutation(base.ID)
	defer releaseMutation()
	watcher := base.Clone()
	watcher.Prefix = "new"
	if _, errUpdate := manager.Update(WithSkipPersist(ctx), watcher); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	plugin := base.Clone()
	plugin.Label = "stale-plugin-result"
	updated, committed, errUpdate := manager.updateInternalWithMutation(WithSkipPersist(ctx), base, plugin, updateModeReplace, releaseMutation)
	if errUpdate != nil || committed || updated != nil {
		t.Fatalf("stale plugin update = (%+v, %v, %v)", updated, committed, errUpdate)
	}
	current, _ := manager.GetByID(base.ID)
	if current.Prefix != "new" || current.Label != "" {
		t.Fatalf("stale plugin result replaced watcher identity: %+v", current)
	}
}

func TestRegistrationUpdateHookCanReenterMutation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		manager := NewManager(nil, nil, nil)
		base, err := manager.Register(ctx, &Auth{ID: "registration-hook", Provider: "audit"})
		if err != nil {
			t.Fatal(err)
		}
		hook := &reloadMutationHook{manager: manager}
		manager.hook = hook
		updated := base.Clone()
		updated.Label = "plugin-update"
		_, committed, errUpdate := manager.UpdateIfRegistrationCurrent(ctx, base, updated)
		if errUpdate != nil || !committed || !hook.called || hook.err != nil {
			t.Fatalf("registration update committed=%v error=%v hook=%+v", committed, errUpdate, hook)
		}
		current, _ := manager.GetByID(base.ID)
		if current.Label != "hook-update" {
			t.Fatal("registration update hook did not publish its reentrant mutation")
		}
	})
}

func TestMetaMintPreservesConcurrentAuthCooldownDispatchFence(t *testing.T) {
	withQuotaCooldownEnabled(t)
	ctx := context.Background()
	store := &blockingSaveStore{
		lifecycleVersionedStore: lifecycleVersionedStore{restoreGeneration: 7},
		saveStarted:             make(chan struct{}, 1),
		saveRelease:             make(chan struct{}),
	}
	manager := NewManager(store, nil, nil)
	base, err := manager.Register(ctx, &Auth{ID: "meta-cooldown-fence", Provider: "meta", Status: StatusActive, Metadata: map[string]any{"access_token": "old-token"}})
	if err != nil {
		t.Fatal(err)
	}
	staleCtx := withExecutionResultSeq(ctx, manager.nextExecutionSeq())
	updated := base.Clone()
	updated.Metadata["access_token"] = "minted-token"
	updateDone := make(chan error, 1)
	go func() {
		_, errUpdate := manager.UpdatePreparedAuth(ctx, base, updated)
		updateDone <- errUpdate
	}()
	<-store.saveStarted
	manager.MarkResult(withExecutionResultSeq(ctx, manager.nextExecutionSeq()), Result{
		AuthID: base.ID, Provider: base.Provider,
		Error: &Error{HTTPStatus: http.StatusTooManyRequests, Message: "quota exhausted"},
	})
	close(store.saveRelease)
	if errUpdate := <-updateDone; errUpdate != nil {
		t.Fatal(errUpdate)
	}
	manager.MarkResult(staleCtx, Result{AuthID: base.ID, Provider: base.Provider, Success: true})
	current, _ := manager.GetByID(base.ID)
	if !current.Quota.Exceeded || !current.NextRetryAfter.After(time.Now()) {
		t.Fatalf("Meta mint dropped the concurrent auth cooldown fence: %+v", current)
	}
	manager.MarkResult(withExecutionResultSeq(ctx, manager.nextExecutionSeq()), Result{AuthID: base.ID, Provider: base.Provider, Success: true})
	current, _ = manager.GetByID(base.ID)
	if current.Quota.Exceeded || !current.NextRetryAfter.IsZero() {
		t.Fatal("a post-cooldown success did not prove recovery")
	}
}

func TestReloadPreservesAuthCooldownDispatchFence(t *testing.T) {
	for _, reload := range []string{"all", "single"} {
		t.Run(reload, func(t *testing.T) {
			withQuotaCooldownEnabled(t)
			ctx := context.Background()
			auth := &Auth{ID: "reload-cooldown-fence", Provider: "audit", Status: StatusActive, Metadata: map[string]any{"access_token": "token"}}
			store := &fakeIndexedStore{fakeStore: &fakeStore{all: map[string]*Auth{auth.ID: auth.Clone()}}}
			manager := NewManager(store, nil, nil)
			if _, errRegister := manager.Register(WithSkipPersist(ctx), auth); errRegister != nil {
				t.Fatal(errRegister)
			}
			staleCtx := withExecutionResultSeq(ctx, manager.nextExecutionSeq())
			manager.MarkResult(withExecutionResultSeq(ctx, manager.nextExecutionSeq()), Result{AuthID: auth.ID, Provider: auth.Provider, Error: &Error{HTTPStatus: http.StatusTooManyRequests}})
			var errReload error
			if reload == "all" {
				errReload = manager.Load(ctx)
			} else {
				errReload = manager.ReloadByID(ctx, auth.ID)
			}
			if errReload != nil {
				t.Fatal(errReload)
			}
			manager.MarkResult(staleCtx, Result{AuthID: auth.ID, Provider: auth.Provider, Success: true})
			current, _ := manager.GetByID(auth.ID)
			if !current.Quota.Exceeded || !current.NextRetryAfter.After(time.Now()) {
				t.Fatalf("reload lost the live cooldown dispatch fence: %+v", current)
			}
		})
	}
}

type authScopedSessionCleanupExecutor struct {
	schedulerTestExecutor
	closedAuths    []string
	closedSessions []string
}

func (e *authScopedSessionCleanupExecutor) CloseAuthExecutionSessions(authID string) {
	e.closedAuths = append(e.closedAuths, authID)
}

func (e *authScopedSessionCleanupExecutor) CloseExecutionSession(sessionID string) {
	e.closedSessions = append(e.closedSessions, sessionID)
}

func TestCredentialRemovalPreservesOtherAuthExecutionSessions(t *testing.T) {
	for _, removal := range []string{"remove", "delete", "reload", "load"} {
		t.Run(removal, func(t *testing.T) {
			ctx := context.Background()
			auth := &Auth{ID: "removed-session-owner", Provider: "audit", Status: StatusActive}
			other := &Auth{ID: "retained-session-owner", Provider: "audit", Status: StatusActive}
			store := &fakeIndexedStore{fakeStore: &fakeStore{all: map[string]*Auth{auth.ID: auth.Clone(), other.ID: other.Clone()}}}
			manager := NewManager(store, nil, nil)
			executor := &authScopedSessionCleanupExecutor{schedulerTestExecutor: schedulerTestExecutor{provider: "audit"}}
			manager.RegisterExecutor(executor)
			for _, entry := range []*Auth{auth, other} {
				if _, errRegister := manager.Register(WithSkipPersist(ctx), entry); errRegister != nil {
					t.Fatal(errRegister)
				}
			}
			delete(store.all, auth.ID)
			var errRemove error
			switch removal {
			case "remove":
				manager.Remove(ctx, auth.ID)
			case "delete":
				errRemove = manager.DeleteAuths(ctx, []string{auth.ID}, func(context.Context) error { return nil })
			case "reload":
				errRemove = manager.ReloadByID(ctx, auth.ID)
			case "load":
				errRemove = manager.Load(ctx)
			}
			if errRemove != nil {
				t.Fatal(errRemove)
			}
			if len(executor.closedSessions) != 0 || len(executor.closedAuths) != 1 || executor.closedAuths[0] != auth.ID {
				t.Fatalf("credential removal closed unrelated sessions: auths=%v sessions=%v", executor.closedAuths, executor.closedSessions)
			}
			if _, found := manager.GetByID(other.ID); !found {
				t.Fatal("unrelated credential was removed")
			}
		})
	}
}
