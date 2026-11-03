package cliproxy

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

type activationFileStore struct {
	*sdkauth.FileTokenStore
	entered chan struct{}
	release chan struct{}
}

func (s *activationFileStore) Save(ctx context.Context, auth *coreauth.Auth) (string, error) {
	path, errSave := s.FileTokenStore.Save(ctx, auth)
	if errSave == nil && auth.Metadata["access_token"] == "background" {
		close(s.entered)
		<-s.release
	}
	return path, errSave
}

func activationSnapshot(t *testing.T, w *watcher.Watcher, path string) *coreauth.Auth {
	t.Helper()
	auths, errSnapshot := w.SnapshotAuthFile(path)
	if errSnapshot != nil || len(auths) != 1 || !auths[0].HasFileSnapshot() {
		t.Fatalf("snapshot: auths=%v err=%v", auths, errSnapshot)
	}
	return auths[0]
}

func TestPostPersistFileLoginWaitsForActivation(t *testing.T) {
	for _, mode := range []string{"before_watcher_start", "inline_before_queue", "queue_claimed_revision", "service_shutdown", "file_removed_during_wait", "file_superseded_during_wait"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			w, errWatcher := watcher.NewWatcher("", dir, nil)
			if errWatcher != nil {
				t.Fatal(errWatcher)
			}
			defer func() { _ = w.Stop() }()
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				path := filepath.Join(dir, "activation.json")
				if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"initial"}`), 0o600); errWrite != nil {
					t.Fatal(errWrite)
				}
				cfg := &config.Config{AuthDir: dir}
				w.SetConfig(cfg)
				store := &activationFileStore{FileTokenStore: sdkauth.NewFileTokenStore(), entered: make(chan struct{}), release: make(chan struct{})}
				store.SetBaseDir(dir)
				manager := coreauth.NewManager(store, nil, nil)
				base, errRegister := manager.Register(coreauth.WithSkipPersist(ctx), activationSnapshot(t, w, path))
				if errRegister != nil {
					t.Fatal(errRegister)
				}
				defer GlobalModelRegistry().UnregisterClient(base.ID)
				lifetime, cancelLifetime := context.WithCancel(ctx)
				defer cancelLifetime()
				service := &Service{cfg: cfg, coreManager: manager, antigravityContext: lifetime}
				if mode == "inline_before_queue" || mode == "queue_claimed_revision" {
					queue := make(chan watcher.AuthUpdate, 16)
					w.SetAuthUpdateQueue(queue)
					defer w.SetAuthUpdateQueue(nil)
					pendingObserved := make(chan struct{})
					var pendingOnce sync.Once
					service.watcher = &WatcherWrapper{
						dispatchPersistedAuthWithRev: func(update *watcher.AuthUpdate) (bool, uint64) {
							ok, rev := w.DispatchPersistedAuthUpdateWithRevision(update)
							if mode == "queue_claimed_revision" {
								<-pendingObserved
								// The real consumer has claimed this revision before the
								// synchronous login can begin its inline delivery.
							}
							return ok, rev
						},
						rereadDiscardedFileSnapshot: func(auth *coreauth.Auth) {
							pendingOnce.Do(func() { close(pendingObserved) })
						},
					}
					if mode == "queue_claimed_revision" {
						service.authUpdates = queue
						consumeCtx, cancel := context.WithCancel(ctx)
						consumed := make(chan struct{})
						go func() { defer close(consumed); service.consumeAuthUpdates(consumeCtx) }()
						defer func() { cancel(); <-consumed }()
					}
				}
				background := base.Clone()
				background.Metadata["access_token"] = "background"
				saveDone := make(chan struct{})
				var errBackground error
				go func() {
					defer close(saveDone)
					_, errBackground = manager.Update(ctx, background)
				}()
				<-store.entered
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(store.release) }) }
				defer func() { release(); <-saveDone }()
				login := base.Clone()
				login.Metadata["access_token"] = "new-login"
				if _, errPersist := coreauth.PersistExplicitAuth(coreauth.WithAuthCreationIntent(ctx), store, login); errPersist != nil {
					t.Fatal(errPersist)
				}
				fresh := activationSnapshot(t, w, path)
				requestCtx, cancelRequest := context.WithCancel(ctx)
				cancelRequest()
				hookDone := make(chan struct{})
				var errHook error
				go func() { defer close(hookDone); errHook = service.runtimeAuthSyncHook()(requestCtx, fresh) }()
				synctest.Wait()
				select {
				case <-hookDone:
					t.Fatal("post-persist login returned before the pending save allowed activation")
				default:
				}
				if mode == "service_shutdown" {
					cancelLifetime()
					<-hookDone
					if !errors.Is(errHook, context.Canceled) {
						t.Fatalf("shutdown did not cancel pending activation: %v", errHook)
					}
					return
				}
				if mode == "file_removed_during_wait" {
					if errRemove := os.Remove(path); errRemove != nil {
						t.Fatal(errRemove)
					}
				}
				if mode == "file_superseded_during_wait" {
					if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"newer-login"}`), 0o600); errWrite != nil {
						t.Fatal(errWrite)
					}
				}
				release()
				<-saveDone
				<-hookDone
				if mode == "file_removed_during_wait" || mode == "file_superseded_during_wait" {
					if !errors.Is(errHook, coreauth.ErrAuthStoreConflict) {
						t.Fatalf("stale login observation did not fail: %v", errHook)
					}
					return
				}
				if errBackground != nil || errHook != nil {
					t.Fatalf("background=%v activation=%v", errBackground, errHook)
				}
				current, _ := manager.GetByID(base.ID)
				if current.Metadata["access_token"] != "new-login" || len(GlobalModelRegistry().GetModelsForClient(base.ID)) == 0 {
					t.Fatalf("login returned before runtime/model activation: token=%v", current.Metadata["access_token"])
				}
				manager.RegisterExecutor(&syncTestExecutor{})
				response, errExecute := manager.Execute(ctx, []string{"codex"}, cliproxyexecutor.Request{Model: "gpt-6-astra"}, cliproxyexecutor.Options{})
				if errExecute != nil || string(response.Payload) != "new-login" {
					t.Fatalf("scheduler did not expose the logged-in credential: payload=%q err=%v", response.Payload, errExecute)
				}
			})
		})
	}
}

func TestPostPersistFileLoginRejectsMissingOrSupersededFile(t *testing.T) {
	for _, initialAuth := range []bool{false, true} {
		for _, removed := range []bool{false, true} {
			name := "superseded"
			if removed {
				name = "removed"
			}
			if initialAuth {
				name += "/existing"
			} else {
				name += "/absent"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				path := filepath.Join(dir, "login.json")
				if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"new-login"}`), 0o600); errWrite != nil {
					t.Fatal(errWrite)
				}
				w, errWatcher := watcher.NewWatcher("", dir, nil)
				if errWatcher != nil {
					t.Fatal(errWatcher)
				}
				defer func() { _ = w.Stop() }()
				cfg := &config.Config{AuthDir: dir}
				w.SetConfig(cfg)
				manager := coreauth.NewManager(nil, nil, nil)
				snapshot := activationSnapshot(t, w, path)
				if initialAuth {
					if _, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), snapshot.Clone()); errRegister != nil {
						t.Fatal(errRegister)
					}
				}
				defer GlobalModelRegistry().UnregisterClient(snapshot.ID)
				service := &Service{cfg: cfg, coreManager: manager, watcher: &WatcherWrapper{
					dispatchPersistedAuthWithRev: func(update *watcher.AuthUpdate) (bool, uint64) {
						ok, rev := w.DispatchPersistedAuthUpdateWithRevision(update)
						if removed {
							if errRemove := os.Remove(path); errRemove != nil {
								t.Fatal(errRemove)
							}
						} else if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"newer-login"}`), 0o600); errWrite != nil {
							t.Fatal(errWrite)
						}
						return ok, rev
					},
				}}
				if errHook := service.runtimeAuthSyncHook()(context.Background(), snapshot); !errors.Is(errHook, coreauth.ErrAuthStoreConflict) {
					t.Fatalf("superseded file login reported success: err=%v", errHook)
				}
				if _, exists := manager.GetByID(snapshot.ID); !initialAuth && exists {
					t.Fatal("discarded login snapshot registered an absent credential")
				}
			})
		}
	}
}

func TestPostPersistFileLoginDoesNotRepeatConsumedRegistration(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, superseded := range []bool{false, true} {
			name := "absent"
			if existing {
				name = "existing"
			}
			if superseded {
				name += "/newer_queue_revision"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				path := filepath.Join(dir, "consumed.json")
				data := []byte(`{"type":"codex","access_token":"new-login"}`)
				if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
					t.Fatal(errWrite)
				}
				w, errWatcher := watcher.NewWatcher("", dir, nil)
				if errWatcher != nil {
					t.Fatal(errWatcher)
				}
				defer func() { _ = w.Stop() }()
				cfg := &config.Config{AuthDir: dir}
				w.SetConfig(cfg)
				queue := make(chan watcher.AuthUpdate, 16)
				w.SetAuthUpdateQueue(queue)
				manager := coreauth.NewManager(nil, nil, nil)
				snapshot := activationSnapshot(t, w, path)
				if existing {
					if _, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), snapshot.Clone()); errRegister != nil {
						t.Fatal(errRegister)
					}
				}
				defer GlobalModelRegistry().UnregisterClient(snapshot.ID)
				service := &Service{cfg: cfg, coreManager: manager}
				var consumedGeneration, consumedEpoch uint64
				service.watcher = &WatcherWrapper{dispatchPersistedAuthWithRev: func(update *watcher.AuthUpdate) (bool, uint64) {
					ok, rev := w.DispatchPersistedAuthUpdateWithRevision(update)
					service.handleAuthUpdate(context.Background(), <-queue)
					priorResult := service.authRevisions[authUpdateID(*update)].result
					if superseded {
						if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"newer-login"}`), 0o600); errWrite != nil {
							t.Fatal(errWrite)
						}
						next := watcher.AuthUpdate{Action: watcher.AuthUpdateActionModify, ID: snapshot.ID, Auth: activationSnapshot(t, w, path)}
						w.DispatchPersistedAuthUpdateWithRevision(&next)
						service.handleAuthUpdate(context.Background(), <-queue)
					} else {
						reread := watcher.AuthUpdate{Action: watcher.AuthUpdateActionModify, ID: snapshot.ID, Auth: activationSnapshot(t, w, path)}
						w.DispatchPersistedAuthUpdateWithRevision(&reread)
						service.handleAuthUpdate(context.Background(), <-queue)
					}
					current, _ := manager.GetByID(snapshot.ID)
					consumedGeneration = current.Generation
					consumedEpoch = GlobalModelRegistry().ClientRegistrationEpoch(snapshot.ID)
					retry := *update
					retry.SetRevision(0)
					service.handleAuthUpdatesIfResultCurrent(context.Background(), []watcher.AuthUpdate{retry}, priorResult)
					return ok, rev
				}}
				errHook := service.runtimeAuthSyncHook()(context.Background(), snapshot)
				if (!superseded && errHook != nil) || (superseded && !errors.Is(errHook, coreauth.ErrAuthStoreConflict)) {
					t.Fatalf("consumed login result: superseded=%v err=%v", superseded, errHook)
				}
				current, _ := manager.GetByID(snapshot.ID)
				if current.Generation != consumedGeneration || GlobalModelRegistry().ClientRegistrationEpoch(snapshot.ID) != consumedEpoch {
					t.Fatal("synchronous hook repeated the already-consumed update or model registration")
				}
				if !superseded {
					manager.RegisterExecutor(&syncTestExecutor{})
					response, errExecute := manager.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: "gpt-6-astra"}, cliproxyexecutor.Options{})
					if errExecute != nil || string(response.Payload) != "new-login" {
						t.Fatalf("consumed login was not schedulable: payload=%q err=%v", response.Payload, errExecute)
					}
				}
			})
		}
	}
}

func TestPostPersistFileLoginKeepsAuthoritativeVersionedReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "versioned.json")
	data := []byte(`{"type":"codex","access_token":"new-login"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	w, errWatcher := watcher.NewWatcher("", dir, nil)
	if errWatcher != nil {
		t.Fatal(errWatcher)
	}
	defer func() { _ = w.Stop() }()
	cfg := &config.Config{AuthDir: dir}
	w.SetConfig(cfg)
	snapshot := activationSnapshot(t, w, path)
	authoritative := snapshot.Clone()
	authoritative.ClearFileSnapshot()
	authoritative.Attributes[coreauth.AttributeSourceBackend] = coreauth.AuthSourcePostgres
	authoritative.SetStoreGeneration(4)
	store := &serviceAuthReloadStore{auth: authoritative}
	manager := coreauth.NewManager(store, nil, nil)
	service := &Service{cfg: cfg, coreManager: manager}
	defer GlobalModelRegistry().UnregisterClient(snapshot.ID)
	if errHook := service.runtimeAuthSyncHook()(context.Background(), snapshot); errHook != nil {
		t.Fatal(errHook)
	}
	current, exists := manager.GetByID(snapshot.ID)
	if !exists || current.StoreGeneration() != 4 || current.Attributes[coreauth.AttributeSourceBackend] != coreauth.AuthSourcePostgres {
		t.Fatalf("versioned login did not preserve its authoritative row: %+v", current)
	}
}

func TestAuthUpdateResultsDoNotRetainTransientRuntimeAuths(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	for _, id := range []string{"transient-one", "transient-two"} {
		service.handleAuthUpdate(context.Background(), watcher.AuthUpdate{
			Action: watcher.AuthUpdateActionAdd, ID: id,
			Auth: &coreauth.Auth{ID: id, Provider: "runtime-test", Status: coreauth.StatusActive},
		})
		service.handleAuthUpdate(context.Background(), watcher.AuthUpdate{Action: watcher.AuthUpdateActionDelete, ID: id})
	}
	if len(service.authRevisions) != 0 {
		t.Fatal("unversioned runtime updates retained login completion state")
	}
}
