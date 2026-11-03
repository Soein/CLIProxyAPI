package cliproxy

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

type loginCompletionRegistrationHook struct {
	coreauth.NoopHook
	onRegistered func(context.Context, *coreauth.Auth)
}

func (h *loginCompletionRegistrationHook) OnAuthRegistered(ctx context.Context, auth *coreauth.Auth) {
	if h.onRegistered != nil {
		h.onRegistered(ctx, auth)
	}
}

func newLoginCompletionPluginFixture(t *testing.T, data string, handle func(int, pluginapi.AuthModelRequest) pluginapi.ModelResponse) (*Service, *coreauth.Manager, *issue6718ModelProvider, *coreauth.Auth) {
	t.Helper()
	service, manager, plugin, _ := newIssue6718PluginService(t, "codex", handle)
	dir := t.TempDir()
	service.cfg.AuthDir = dir
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(dir)
	manager.SetStore(store)
	path := filepath.Join(dir, "plugin-login.json")
	if errWrite := os.WriteFile(path, []byte(data), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	w, errWatcher := watcher.NewWatcher("", dir, nil)
	if errWatcher != nil {
		t.Fatal(errWatcher)
	}
	t.Cleanup(func() { _ = w.Stop() })
	w.SetConfig(service.cfg)
	snapshot := activationSnapshot(t, w, path)
	reg := GlobalModelRegistry()
	reg.UnregisterClient(snapshot.ID)
	t.Cleanup(func() { reg.UnregisterClient(snapshot.ID) })
	return service, manager, plugin, snapshot
}

func TestPostPersistFileLoginRequiresCompletedModelPublication(t *testing.T) {
	for _, mode := range []string{"control", "same_credential_update_before_publication"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "publication.json")
			if errWrite := os.WriteFile(path, []byte(`{"type":"claude","disabled":false,"access_token":"login-token"}`), 0o600); errWrite != nil {
				t.Fatal(errWrite)
			}
			cfg := &config.Config{AuthDir: dir}
			w, errWatcher := watcher.NewWatcher("", dir, nil)
			if errWatcher != nil {
				t.Fatal(errWatcher)
			}
			t.Cleanup(func() { _ = w.Stop() })
			w.SetConfig(cfg)
			snapshot := activationSnapshot(t, w, path)
			reg := GlobalModelRegistry()
			reg.UnregisterClient(snapshot.ID)
			t.Cleanup(func() { reg.UnregisterClient(snapshot.ID) })
			store := sdkauth.NewFileTokenStore()
			store.SetBaseDir(dir)
			hook := &loginCompletionRegistrationHook{}
			manager := coreauth.NewManager(store, nil, hook)
			updated := false
			if mode != "control" {
				hook.onRegistered = func(_ context.Context, registered *coreauth.Auth) {
					if len(reg.GetModelsForClient(snapshot.ID)) != 0 {
						t.Fatal("registration callback ran after model publication")
					}
					current, ok := manager.GetByID(snapshot.ID)
					if !ok {
						t.Fatal("registered credential missing")
					}
					current, errUpdate := manager.Update(context.Background(), current)
					if errUpdate != nil {
						t.Fatal(errUpdate)
					}
					if current.Generation <= registered.Generation {
						t.Fatal("same-credential update did not advance the runtime generation")
					}
					updated = true
				}
			}
			service := &Service{cfg: cfg, coreManager: manager}
			errHook := service.runtimeAuthSyncHook()(context.Background(), snapshot)
			current, ok := manager.GetByID(snapshot.ID)
			if !ok || current.Metadata["access_token"] != "login-token" || !snapshot.FileSnapshotMatchesDisk() {
				t.Fatal("fixture changed the persisted credential")
			}
			if mode != "control" && !updated {
				t.Fatal("same-credential update callback was not reached")
			}
			models := len(reg.GetModelsForClient(snapshot.ID))
			t.Logf("intervening_update=%v generation=%d models=%d hook_error=%v", updated, current.Generation, models, errHook)
			if mode == "control" && errHook != nil {
				t.Fatalf("ordinary persisted login failed: %v", errHook)
			}
			if errHook == nil && models == 0 {
				t.Fatal("login reported success before its models were published")
			}
			if errHook != nil && !errors.Is(errHook, coreauth.ErrAuthStoreConflict) {
				t.Fatalf("superseded model registration returned an unexplained error: %v", errHook)
			}
		})
	}
}

func TestPostPersistFileLoginAcceptsModelDiscoveryEnrichment(t *testing.T) {
	for _, mode := range []string{"control", "auth_update_attributes", "auth_update_metadata", "manager_update_attributes", "persisted_model_cache", "persisted_model_cache_watcher_reread", "persisted_model_cache_then_auth_update_attributes", "persisted_model_cache_then_auth_update_metadata"} {
		t.Run(mode, func(t *testing.T) {
			const provider = "codex"
			const model = "login-completion-model"
			var manager *coreauth.Manager
			service, configuredManager, plugin, snapshot := newLoginCompletionPluginFixture(t, `{"type":"codex","access_token":"login-token"}`, func(call int, req pluginapi.AuthModelRequest) pluginapi.ModelResponse {
				response := pluginapi.ModelResponse{Provider: provider, Models: []pluginapi.ModelInfo{{ID: model}}}
				switch mode {
				case "auth_update_attributes":
					response.AuthUpdate = pluginapi.AuthData{Provider: provider, Attributes: map[string]string{"note": "model discovery complete"}}
				case "auth_update_metadata":
					response.AuthUpdate = pluginapi.AuthData{Provider: provider, Metadata: map[string]any{"model_cache": "discovered"}}
				case "manager_update_attributes":
					if call == 1 {
						current, ok := manager.GetByID(req.AuthID)
						if !ok {
							t.Fatal("credential missing during model discovery writeback")
						}
						current.Attributes["note"] = "model discovery complete"
						if _, errUpdate := manager.Update(context.Background(), current); errUpdate != nil {
							t.Fatal(errUpdate)
						}
					}
				case "persisted_model_cache", "persisted_model_cache_watcher_reread", "persisted_model_cache_then_auth_update_attributes", "persisted_model_cache_then_auth_update_metadata":
					if call == 1 {
						current, ok := manager.GetByID(req.AuthID)
						if !ok {
							t.Fatal("credential missing during model discovery")
						}
						if saved, _ := manager.PersistMetadataEntry(context.Background(), req.AuthID, current.RegistrationEpoch, "model_cache", "discovered", nil); !saved {
							t.Fatal("model discovery metadata was not persisted")
						}
					}
					if mode == "persisted_model_cache_then_auth_update_attributes" {
						response.AuthUpdate = pluginapi.AuthData{Provider: provider, Attributes: map[string]string{"note": "model discovery complete"}}
					} else if mode == "persisted_model_cache_then_auth_update_metadata" {
						response.AuthUpdate = pluginapi.AuthData{Provider: provider, Metadata: map[string]any{"model_cache": "discovered"}}
					}
				}
				return response
			})
			manager = configuredManager
			reg := GlobalModelRegistry()
			if mode == "persisted_model_cache_watcher_reread" {
				w, errWatcher := watcher.NewWatcher("", service.cfg.AuthDir, nil)
				if errWatcher != nil {
					t.Fatal(errWatcher)
				}
				t.Cleanup(func() { _ = w.Stop() })
				w.SetConfig(service.cfg)
				queue := make(chan watcher.AuthUpdate, 16)
				w.SetAuthUpdateQueue(queue)
				service.watcher = &WatcherWrapper{dispatchPersistedAuthWithRev: func(update *watcher.AuthUpdate) (bool, uint64) {
					ok, revision := w.DispatchPersistedAuthUpdateWithRevision(update)
					if !ok || revision == 0 {
						t.Fatal("original login notification was not queued")
					}
					service.handleAuthUpdate(context.Background(), <-queue)
					reread := activationSnapshot(t, w, snapshot.FileSnapshotPath())
					if reread.Metadata["model_cache"] != "discovered" || snapshot.FileSnapshotMatchesDisk() {
						t.Fatal("watcher did not read the completed metadata writeback")
					}
					next := watcher.AuthUpdate{Action: watcher.AuthUpdateActionModify, ID: snapshot.ID, Auth: reread}
					queued, rereadRevision := w.DispatchPersistedAuthUpdateWithRevision(&next)
					if !queued || rereadRevision <= revision {
						t.Fatal("metadata readback was not assigned a newer watcher revision")
					}
					service.handleAuthUpdate(context.Background(), <-queue)
					return ok, revision
				}}
			}

			errHook := service.runtimeAuthSyncHook()(context.Background(), snapshot)
			current, ok := manager.GetByID(snapshot.ID)
			if !ok || current.Metadata["access_token"] != "login-token" {
				t.Fatal("model discovery replaced the runtime credential")
			}
			data, errRead := os.ReadFile(snapshot.FileSnapshotPath())
			if errRead != nil {
				t.Fatal(errRead)
			}
			var persisted map[string]any
			if errDecode := json.Unmarshal(data, &persisted); errDecode != nil {
				t.Fatal(errDecode)
			}
			if persisted["access_token"] != "login-token" {
				t.Fatal("model discovery replaced the persisted credential")
			}
			if plugin.calls == 0 || !reg.ClientSupportsModel(snapshot.ID, model) {
				t.Fatal("model discovery did not publish the discovered model")
			}
			if (mode == "auth_update_attributes" || mode == "manager_update_attributes" || mode == "persisted_model_cache_then_auth_update_attributes") && current.Attributes["note"] != "model discovery complete" {
				t.Fatal("model discovery attribute update was not applied")
			}
			if mode == "manager_update_attributes" && plugin.calls < 2 {
				t.Fatal("model discovery writeback did not require reconciliation")
			}
			if (mode == "persisted_model_cache" || mode == "persisted_model_cache_watcher_reread" || mode == "persisted_model_cache_then_auth_update_attributes" || mode == "persisted_model_cache_then_auth_update_metadata") && persisted["model_cache"] != "discovered" {
				t.Fatal("model discovery cache write was not preserved")
			}
			if mode != "control" && mode != "auth_update_attributes" && mode != "manager_update_attributes" && current.Metadata["model_cache"] != "discovered" {
				t.Fatal("model discovery update rolled back the runtime cache")
			}
			if mode == "persisted_model_cache_watcher_reread" && plugin.calls < 2 {
				t.Fatal("watcher readback did not publish the enriched credential")
			}
			t.Logf("model_calls=%d runtime_note=%q disk_model_cache=%v hook_error=%v", plugin.calls, current.Attributes["note"], persisted["model_cache"], errHook)
			if errHook != nil {
				t.Fatalf("active login was rejected after legitimate model discovery enrichment: %v", errHook)
			}
		})
	}
}

func TestPostPersistFileLoginWaitsForBlockedModelDiscovery(t *testing.T) {
	var entered, release chan struct{}
	service, _, _, snapshot := newLoginCompletionPluginFixture(t, `{"type":"codex","access_token":"login-token"}`, func(call int, _ pluginapi.AuthModelRequest) pluginapi.ModelResponse {
		if call == 1 {
			close(entered)
		}
		<-release
		return pluginapi.ModelResponse{Provider: "codex", Models: []pluginapi.ModelInfo{{ID: "completed-model"}}}
	})
	synctest.Test(t, func(t *testing.T) {
		entered = make(chan struct{})
		release = make(chan struct{})
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
		}()
		done := make(chan error, 1)
		go func() { done <- service.runtimeAuthSyncHook()(context.Background(), snapshot) }()
		<-entered
		synctest.Wait()
		select {
		case errHook := <-done:
			t.Fatalf("login completed while model discovery was blocked: %v", errHook)
		default:
		}
		if len(GlobalModelRegistry().GetModelsForClient(snapshot.ID)) != 0 {
			t.Fatal("model discovery published before it was released")
		}
		close(release)
		if errHook := <-done; errHook != nil {
			t.Fatalf("completed model discovery did not finish login: %v", errHook)
		}
		if !GlobalModelRegistry().ClientSupportsModel(snapshot.ID, "completed-model") {
			t.Fatal("login completed without the discovered model")
		}
	})
}

func TestPostPersistFileLoginRejectsRepeatedlyFencedModelPublication(t *testing.T) {
	reg := GlobalModelRegistry()
	service, manager, plugin, snapshot := newLoginCompletionPluginFixture(t, `{"type":"codex","access_token":"login-token"}`, func(_ int, req pluginapi.AuthModelRequest) pluginapi.ModelResponse {
		reg.RegisterClient(req.AuthID, "codex", []*ModelInfo{{ID: "intervening-model"}})
		reg.UnregisterClient(req.AuthID)
		return pluginapi.ModelResponse{Provider: "codex", Models: []pluginapi.ModelInfo{{ID: "fenced-model"}}}
	})
	errHook := service.runtimeAuthSyncHook()(context.Background(), snapshot)
	if plugin.calls < 2 {
		t.Fatal("fixture did not fence both discovery and reconciliation")
	}
	if len(reg.GetModelsForClient(snapshot.ID)) != 0 {
		t.Fatal("fenced model computation unexpectedly published")
	}
	current, ok := manager.GetByID(snapshot.ID)
	if !ok || current.Metadata["access_token"] != "login-token" || !snapshot.FileSnapshotMatchesDisk() {
		t.Fatal("fixture replaced the login credential")
	}
	if !errors.Is(errHook, coreauth.ErrAuthStoreConflict) {
		t.Fatalf("login succeeded after every model publication was fenced out: %v", errHook)
	}
	t.Logf("model_calls=%d models=0 hook_error=%v", plugin.calls, errHook)
}

func TestPostPersistFileLoginAcceptsIntentionalEmptyModelPublication(t *testing.T) {
	for _, mode := range []string{"empty_catalog", "disabled_credential"} {
		t.Run(mode, func(t *testing.T) {
			data := `{"type":"codex","access_token":"login-token"}`
			if mode == "disabled_credential" {
				data = `{"type":"codex","access_token":"login-token","disabled":true}`
			}
			service, manager, plugin, snapshot := newLoginCompletionPluginFixture(t, data, func(int, pluginapi.AuthModelRequest) pluginapi.ModelResponse {
				return pluginapi.ModelResponse{Provider: "codex"}
			})
			reg := GlobalModelRegistry()
			reg.RegisterClient(snapshot.ID, "codex", []*ModelInfo{{ID: "previous-model"}})
			if errHook := service.runtimeAuthSyncHook()(context.Background(), snapshot); errHook != nil {
				t.Fatalf("intentional model removal did not complete login: %v", errHook)
			}
			current, ok := manager.GetByID(snapshot.ID)
			if !ok || current.Metadata["access_token"] != "login-token" {
				t.Fatal("intentional model removal lost the credential")
			}
			if current.Disabled != (mode == "disabled_credential") {
				t.Fatal("login did not preserve the credential's disabled state")
			}
			if mode == "empty_catalog" && plugin.calls == 0 {
				t.Fatal("empty model discovery callback was not reached")
			}
			if len(reg.GetModelsForClient(snapshot.ID)) != 0 {
				t.Fatal("intentional empty publication retained stale models")
			}
		})
	}
}

func TestPostPersistFileLoginIgnoresLaterRevisionOfDiscardedSnapshot(t *testing.T) {
	for _, mode := range []string{"stale_read", "replacement_credential"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "delayed.json")
			if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"old-read"}`), 0o600); errWrite != nil {
				t.Fatal(errWrite)
			}
			cfg := &config.Config{AuthDir: dir}
			w, errWatcher := watcher.NewWatcher("", dir, nil)
			if errWatcher != nil {
				t.Fatal(errWatcher)
			}
			t.Cleanup(func() { _ = w.Stop() })
			w.SetConfig(cfg)
			queue := make(chan watcher.AuthUpdate, 16)
			w.SetAuthUpdateQueue(queue)
			oldSnapshot := activationSnapshot(t, w, path)
			if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"new-login"}`), 0o600); errWrite != nil {
				t.Fatal(errWrite)
			}
			snapshot := activationSnapshot(t, w, path)
			if oldSnapshot.ID != snapshot.ID || oldSnapshot.FileSnapshotMatchesDisk() {
				t.Fatal("fixture did not retain a stale observation for the same credential ID")
			}
			manager := coreauth.NewManager(nil, nil, nil)
			reg := GlobalModelRegistry()
			reg.UnregisterClient(snapshot.ID)
			t.Cleanup(func() { reg.UnregisterClient(snapshot.ID) })
			service := &Service{cfg: cfg, coreManager: manager}
			var consumedGeneration, consumedEpoch uint64
			service.watcher = &WatcherWrapper{dispatchPersistedAuthWithRev: func(update *watcher.AuthUpdate) (bool, uint64) {
				ok, revision := w.DispatchPersistedAuthUpdateWithRevision(update)
				if !ok || revision == 0 {
					t.Fatal("persisted login was not queued with a revision")
				}
				service.handleAuthUpdate(context.Background(), <-queue)
				if !reg.ClientSupportsModel(snapshot.ID, "gpt-6-astra") {
					t.Fatal("new login was not activated before the delayed observation")
				}
				delayed := oldSnapshot
				if mode == "replacement_credential" {
					if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"replacement-login"}`), 0o600); errWrite != nil {
						t.Fatal(errWrite)
					}
					delayed = activationSnapshot(t, w, path)
				}
				next := watcher.AuthUpdate{Action: watcher.AuthUpdateActionModify, ID: snapshot.ID, Auth: delayed}
				queued, delayedRevision := w.DispatchPersistedAuthUpdateWithRevision(&next)
				if !queued || delayedRevision <= revision {
					t.Fatal("delayed observation did not receive a newer watcher revision")
				}
				service.handleAuthUpdate(context.Background(), <-queue)
				current, exists := manager.GetByID(snapshot.ID)
				if !exists {
					t.Fatal("active login disappeared after the delayed observation")
				}
				consumedGeneration = current.Generation
				consumedEpoch = reg.ClientRegistrationEpoch(snapshot.ID)
				return ok, revision
			}}

			errHook := service.runtimeAuthSyncHook()(context.Background(), snapshot)
			if mode == "replacement_credential" {
				if !errors.Is(errHook, coreauth.ErrAuthStoreConflict) {
					t.Fatalf("replaced login did not report a conflict: %v", errHook)
				}
			} else if errHook != nil {
				t.Fatalf("discarded old observation invalidated the completed new login: %v", errHook)
			}
			current, ok := manager.GetByID(snapshot.ID)
			wantToken := "new-login"
			if mode == "replacement_credential" {
				wantToken = "replacement-login"
			}
			if !ok || current.Metadata["access_token"] != wantToken {
				t.Fatalf("delayed observation left the wrong active credential: want %q, got %#v", wantToken, current)
			}
			if !reg.ClientSupportsModel(snapshot.ID, "gpt-6-astra") {
				t.Fatal("delayed observation removed the active credential's models")
			}
			if current.Generation != consumedGeneration || reg.ClientRegistrationEpoch(snapshot.ID) != consumedEpoch {
				t.Fatal("login completion repeated an already-consumed update or model registration")
			}
			t.Logf("mode=%s generation=%d registration_epoch=%d hook_error=%v", mode, current.Generation, consumedEpoch, errHook)
		})
	}
}

func TestPostPersistFileLoginAcceptsOwnedAntigravityRefresh(t *testing.T) {
	for _, cache := range []string{"warm_cache", "cold_cache"} {
		for _, publisher := range []string{"native_refresh", "external_override"} {
			t.Run(cache+"/"+publisher, func(t *testing.T) {
				resetAntigravityCapabilityCache()
				t.Cleanup(resetAntigravityCapabilityCache)
				const model = "gemini-3.1-flash-lite"
				probeStarted, releaseProbe := make(chan struct{}), make(chan struct{})
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(releaseProbe) }) }
				var requests atomic.Int32
				previousTransport := http.DefaultTransport
				http.DefaultTransport = antigravityReviewTransport(func(req *http.Request) (*http.Response, error) {
					if requests.Add(1) == 1 {
						close(probeStarted)
					}
					if cache == "cold_cache" {
						select {
						case <-releaseProbe:
						case <-req.Context().Done():
							return nil, req.Context().Err()
						}
					}
					return antigravityVersionTestResponse(req, http.StatusOK, `{"models":{"gemini-3.1-flash-lite":{}}}`), nil
				})
				t.Cleanup(func() { http.DefaultTransport = previousTransport })
				dir := t.TempDir()
				path := filepath.Join(dir, "antigravity-login.json")
				data := `{"type":"antigravity","access_token":"login-token","project_id":"login-project","base_url":"https://login-models.invalid"}`
				if errWrite := os.WriteFile(path, []byte(data), 0o600); errWrite != nil {
					t.Fatal(errWrite)
				}
				cfg := &config.Config{AuthDir: dir}
				w, errWatcher := watcher.NewWatcher("", dir, nil)
				if errWatcher != nil {
					t.Fatal(errWatcher)
				}
				t.Cleanup(func() { _ = w.Stop() })
				w.SetConfig(cfg)
				queue := make(chan watcher.AuthUpdate, 16)
				w.SetAuthUpdateQueue(queue)
				snapshot := activationSnapshot(t, w, path)
				store := sdkauth.NewFileTokenStore()
				store.SetBaseDir(dir)
				manager := coreauth.NewManager(store, nil, nil)
				ctx, cancel := context.WithCancel(context.Background())
				service := &Service{cfg: cfg, coreManager: manager, antigravityContext: ctx}
				reg := GlobalModelRegistry()
				reg.UnregisterClient(snapshot.ID)
				t.Cleanup(func() { reg.UnregisterClient(snapshot.ID) })
				t.Cleanup(func() { unblock(); cancel(); service.WaitAntigravityProbes() })
				if cache == "warm_cache" {
					registered, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), snapshot.Clone())
					if errRegister != nil {
						t.Fatal(errRegister)
					}
					hints := service.fetchAntigravityModelCapabilityHintsForAuth(context.Background(), registered)
					if _, ok := hints.ModelIDs[model]; !ok {
						t.Fatal("native HTTP discovery did not prime the capability cache")
					}
				}
				startEpoch := reg.ClientRegistrationEpoch(snapshot.ID)
				var nativeEpoch uint64
				service.watcher = &WatcherWrapper{dispatchPersistedAuthWithRev: func(update *watcher.AuthUpdate) (bool, uint64) {
					ok, revision := w.DispatchPersistedAuthUpdateWithRevision(update)
					if !ok || revision == 0 {
						t.Fatal("persisted Antigravity login was not queued")
					}
					service.handleAuthUpdate(context.Background(), <-queue)
					if cache == "cold_cache" {
						<-probeStarted
						if len(reg.GetModelsForClient(snapshot.ID)) != 0 {
							t.Fatal("cold registration exposed models before capability discovery")
						}
						if reg.ClientRegistrationEpoch(snapshot.ID) != startEpoch+1 {
							t.Fatal("cold registration did not first publish its intentional empty catalog")
						}
						unblock()
					}
					service.WaitAntigravityProbes()
					nativeEpoch = reg.ClientRegistrationEpoch(snapshot.ID)
					if nativeEpoch < startEpoch+2 || !reg.ClientSupportsModel(snapshot.ID, model) {
						t.Fatal("native refresh did not replace the registration's initial publication")
					}
					if publisher == "external_override" {
						reg.RegisterClient(snapshot.ID, "antigravity", []*ModelInfo{{ID: "external-model"}})
					}
					return ok, revision
				}}
				errHook := service.runtimeAuthSyncHook()(context.Background(), snapshot)
				current, exists := manager.GetByID(snapshot.ID)
				if !exists || current.Metadata["access_token"] != "login-token" {
					t.Fatal("native capability discovery replaced the credential")
				}
				if requests.Load() != 1 {
					t.Fatalf("unexpected native HTTP request count: %d", requests.Load())
				}
				t.Logf("cache=%s publisher=%s start_epoch=%d native_epoch=%d final_epoch=%d hook_error=%v", cache, publisher, startEpoch, nativeEpoch, reg.ClientRegistrationEpoch(snapshot.ID), errHook)
				if publisher == "external_override" {
					if !errors.Is(errHook, coreauth.ErrAuthStoreConflict) || !reg.ClientSupportsModel(snapshot.ID, "external-model") {
						t.Fatalf("login borrowed an unrelated publisher's model registration: %v", errHook)
					}
				} else if errHook != nil || !reg.ClientSupportsModel(snapshot.ID, model) {
					t.Fatalf("login rejected its own completed native model refresh: %v", errHook)
				}
			})
		}
	}
}

func TestPostPersistFileLoginDoesNotBorrowAnotherCatalogPublication(t *testing.T) {
	for _, catalog := range []string{"catalog-a", "catalog-b"} {
		t.Run(catalog, func(t *testing.T) {
			service, manager, plugin, snapshot := newLoginCompletionPluginFixture(t, `{"type":"codex","access_token":"login-token","plugin_catalog":"catalog-a"}`, func(_ int, req pluginapi.AuthModelRequest) pluginapi.ModelResponse {
				model, ok := req.Metadata["plugin_catalog"].(string)
				if !ok {
					t.Fatal("model discovery did not receive the file catalog metadata")
				}
				return pluginapi.ModelResponse{Provider: "codex", Models: []pluginapi.ModelInfo{{ID: model}}}
			})
			w, errWatcher := watcher.NewWatcher("", service.cfg.AuthDir, nil)
			if errWatcher != nil {
				t.Fatal(errWatcher)
			}
			t.Cleanup(func() { _ = w.Stop() })
			w.SetConfig(service.cfg)
			queue := make(chan watcher.AuthUpdate, 16)
			w.SetAuthUpdateQueue(queue)
			data, errMarshal := json.Marshal(map[string]any{"type": "codex", "access_token": "login-token", "plugin_catalog": catalog})
			if errMarshal != nil {
				t.Fatal(errMarshal)
			}
			if errWrite := os.WriteFile(snapshot.FileSnapshotPath(), data, 0o600); errWrite != nil {
				t.Fatal(errWrite)
			}
			newSnapshot := activationSnapshot(t, w, snapshot.FileSnapshotPath())
			if newSnapshot.ID != snapshot.ID || coreauth.CredentialsChanged(newSnapshot, snapshot) || !maps.Equal(newSnapshot.Attributes, snapshot.Attributes) {
				t.Fatal("catalog-only file replacement changed credential identity or source attributes")
			}
			reg := GlobalModelRegistry()
			var consumedGeneration, consumedEpoch uint64
			service.watcher = &WatcherWrapper{dispatchPersistedAuthWithRev: func(update *watcher.AuthUpdate) (bool, uint64) {
				ok, revision := w.DispatchPersistedAuthUpdateWithRevision(update)
				if !ok || revision == 0 {
					t.Fatal("original login notification was not queued")
				}
				service.handleAuthUpdate(context.Background(), <-queue)
				next := watcher.AuthUpdate{Action: watcher.AuthUpdateActionModify, ID: snapshot.ID, Auth: newSnapshot}
				queued, nextRevision := w.DispatchPersistedAuthUpdateWithRevision(&next)
				if !queued || nextRevision <= revision {
					t.Fatal("replacement catalog was not assigned a newer watcher revision")
				}
				service.handleAuthUpdate(context.Background(), <-queue)
				current, exists := manager.GetByID(snapshot.ID)
				if !exists || current.Metadata["plugin_catalog"] != catalog || !reg.ClientSupportsModel(snapshot.ID, catalog) {
					t.Fatal("replacement catalog was not activated before old login completion")
				}
				consumedGeneration = current.Generation
				consumedEpoch = reg.ClientRegistrationEpoch(snapshot.ID)
				return ok, revision
			}}
			errHook := service.runtimeAuthSyncHook()(context.Background(), snapshot)
			if catalog == "catalog-b" {
				if !errors.Is(errHook, coreauth.ErrAuthStoreConflict) {
					t.Fatalf("old login reported success using another file's catalog publication: %v", errHook)
				}
				if reg.ClientSupportsModel(snapshot.ID, "catalog-a") {
					t.Fatal("old login restored its obsolete model catalog")
				}
			} else if errHook != nil {
				t.Fatalf("matching catalog notification did not complete login: %v", errHook)
			}
			current, exists := manager.GetByID(snapshot.ID)
			if !exists || current.Metadata["access_token"] != "login-token" || current.Metadata["plugin_catalog"] != catalog || !reg.ClientSupportsModel(snapshot.ID, catalog) {
				t.Fatal("login completion changed the active catalog or credential")
			}
			if current.Generation != consumedGeneration || reg.ClientRegistrationEpoch(snapshot.ID) != consumedEpoch {
				t.Fatal("old login repeated a consumed catalog registration")
			}
			t.Logf("catalog=%s model_calls=%d generation=%d hook_error=%v", catalog, plugin.calls, current.Generation, errHook)
		})
	}
}
