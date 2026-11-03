package cliproxy

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

type loginProvenanceStore struct {
	*sdkauth.FileTokenStore
	afterWrite func(*coreauth.Auth)
	once       sync.Once
}

func (s *loginProvenanceStore) Save(ctx context.Context, auth *coreauth.Auth) (string, error) {
	path, errSave := s.FileTokenStore.Save(ctx, auth)
	if errSave == nil && s.afterWrite != nil {
		s.once.Do(func() { s.afterWrite(auth) })
	}
	return path, errSave
}

func TestPostPersistFileLoginPreservesProvenanceAcrossSaveNotifications(t *testing.T) {
	for _, tt := range []struct {
		name, operation, notification string
		conflict                      bool
	}{
		{name: "metadata_control", operation: "metadata"},
		{name: "metadata_reentrant_notification", operation: "metadata", notification: "reentrant"},
		{name: "ordinary_update_control", operation: "note"},
		{name: "ordinary_update_completed_notification", operation: "note", notification: "completed"},
		{name: "ordinary_update_reentrant_notification", operation: "note", notification: "reentrant"},
		{name: "catalog_replacement_without_notification", operation: "catalog", conflict: true},
		{name: "catalog_replacement_completed_notification", operation: "catalog", notification: "completed", conflict: true},
		{name: "account_replacement_completed_notification", operation: "account", notification: "completed", conflict: true},
		{name: "skip_persist_and_independent_matching_file", operation: "skip_persist", conflict: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var manager *coreauth.Manager
			var path string
			service, configuredManager, _, snapshot := newLoginCompletionPluginFixture(t,
				`{"type":"codex","access_token":"login-token","account_id":"account-a","plugin_catalog":"catalog-a"}`,
				func(call int, req pluginapi.AuthModelRequest) pluginapi.ModelResponse {
					if call == 1 {
						current, exists := manager.GetByID(req.AuthID)
						if !exists {
							t.Fatal("credential missing during model discovery")
						}
						if tt.operation == "metadata" {
							if saved, _ := manager.PersistMetadataEntry(context.Background(), req.AuthID, current.RegistrationEpoch, "model_cache", "discovered", nil); !saved {
								t.Fatal("model metadata was not persisted")
							}
						} else {
							current.Attributes["note"] = "model discovery complete"
							switch tt.operation {
							case "catalog", "skip_persist":
								current.Metadata["plugin_catalog"] = "catalog-b"
							case "account":
								current.Metadata["account_id"] = "account-b"
							}
							ctx := context.Background()
							if tt.operation == "skip_persist" {
								ctx = coreauth.WithSkipPersist(ctx)
							}
							updated, errUpdate := manager.Update(ctx, current)
							if errUpdate != nil {
								t.Fatal(errUpdate)
							}
							if tt.operation == "skip_persist" {
								raw, errMarshal := json.Marshal(updated.Metadata)
								if errMarshal != nil {
									t.Fatal(errMarshal)
								}
								if errWrite := os.WriteFile(path, raw, 0o600); errWrite != nil {
									t.Fatal(errWrite)
								}
							}
						}
					}
					catalog, _ := req.Metadata["plugin_catalog"].(string)
					account, _ := req.Metadata["account_id"].(string)
					return pluginapi.ModelResponse{Provider: "codex", Models: []pluginapi.ModelInfo{{ID: catalog + "-" + account}}}
				})
			manager = configuredManager
			path = snapshot.FileSnapshotPath()
			w, errWatcher := watcher.NewWatcher("", service.cfg.AuthDir, nil)
			if errWatcher != nil {
				t.Fatal(errWatcher)
			}
			t.Cleanup(func() { _ = w.Stop() })
			w.SetConfig(service.cfg)
			queue := make(chan watcher.AuthUpdate, 16)
			w.SetAuthUpdateQueue(queue)
			notifications := 0
			deliver := func() {
				reread := activationSnapshot(t, w, path)
				next := watcher.AuthUpdate{Action: watcher.AuthUpdateActionModify, ID: snapshot.ID, Auth: reread}
				queued, revision := w.DispatchPersistedAuthUpdateWithRevision(&next)
				if !queued || revision == 0 {
					t.Fatal("persisted notification was not queued")
				}
				service.handleAuthUpdate(context.Background(), <-queue)
				notifications++
			}
			store := &loginProvenanceStore{FileTokenStore: sdkauth.NewFileTokenStore()}
			store.SetBaseDir(service.cfg.AuthDir)
			manager.SetStore(store)
			if tt.notification == "reentrant" {
				store.afterWrite = func(saved *coreauth.Auth) {
					live, _ := manager.GetByID(snapshot.ID)
					reread := activationSnapshot(t, w, path)
					if !saved.HasPersistedFileSnapshot(reread) || live.HasPersistedFileSnapshot(reread) {
						t.Fatal("notification was not between the actual file write and manager witness publication")
					}
					deliver()
				}
			}
			service.watcher = &WatcherWrapper{dispatchPersistedAuthWithRev: func(update *watcher.AuthUpdate) (bool, uint64) {
				queued, revision := w.DispatchPersistedAuthUpdateWithRevision(update)
				if !queued || revision == 0 {
					t.Fatal("original login was not queued")
				}
				service.handleAuthUpdate(context.Background(), <-queue)
				if tt.notification == "completed" {
					deliver()
				}
				return queued, revision
			}}
			errHook := service.runtimeAuthSyncHook()(context.Background(), snapshot)
			current, exists := manager.GetByID(snapshot.ID)
			if !exists || current.Metadata["access_token"] != "login-token" {
				t.Fatal("login completion changed the credential")
			}
			raw, errRead := os.ReadFile(path)
			if errRead != nil {
				t.Fatal(errRead)
			}
			var disk map[string]any
			if errUnmarshal := json.Unmarshal(raw, &disk); errUnmarshal != nil {
				t.Fatal(errUnmarshal)
			}
			if disk["access_token"] != "login-token" || snapshot.FileSnapshotMatchesDisk() {
				t.Fatal("fixture did not change only the persisted observation")
			}
			wantCatalog, wantAccount := "catalog-a", "account-a"
			if tt.operation == "catalog" || tt.operation == "skip_persist" {
				wantCatalog = "catalog-b"
			}
			if tt.operation == "account" {
				wantAccount = "account-b"
			}
			if disk["plugin_catalog"] != wantCatalog || disk["account_id"] != wantAccount ||
				current.Metadata["plugin_catalog"] != wantCatalog || current.Metadata["account_id"] != wantAccount ||
				!GlobalModelRegistry().ClientSupportsModel(snapshot.ID, wantCatalog+"-"+wantAccount) {
				t.Fatal("persisted metadata, active credential, and model catalog do not agree")
			}
			if tt.notification != "" && notifications != 1 {
				t.Fatal("persisted notification was not delivered exactly once")
			}
			if tt.conflict && !errors.Is(errHook, coreauth.ErrAuthStoreConflict) {
				t.Fatalf("business replacement reused the original file provenance: %v", errHook)
			}
			if !tt.conflict && errHook != nil {
				t.Fatalf("same-source persisted notification invalidated the completed login: %v", errHook)
			}
			t.Logf("operation=%s notification=%s notifications=%d model=%s-%s hook=%v", tt.operation, tt.notification, notifications, wantCatalog, wantAccount, errHook)
		})
	}
}

func TestPostPersistFileLoginRejectsConditionalUpdateFromReplacedObservation(t *testing.T) {
	for _, field := range []string{"plugin_catalog", "account_id", "access_token"} {
		t.Run(field, func(t *testing.T) {
			service, manager, _, snapshot := newLoginCompletionPluginFixture(t,
				`{"type":"codex","access_token":"login-token","account_id":"account-a","plugin_catalog":"catalog-a"}`, nil)
			base, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), snapshot.Clone())
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			metadata := map[string]any{"type": "codex", "access_token": "login-token", "account_id": "account-a", "plugin_catalog": "catalog-a"}
			metadata[field] = "replacement"
			raw, errMarshal := json.Marshal(metadata)
			if errMarshal != nil {
				t.Fatal(errMarshal)
			}
			if errWrite := os.WriteFile(snapshot.FileSnapshotPath(), raw, 0o600); errWrite != nil {
				t.Fatal(errWrite)
			}
			w, errWatcher := watcher.NewWatcher("", service.cfg.AuthDir, nil)
			if errWatcher != nil {
				t.Fatal(errWatcher)
			}
			t.Cleanup(func() { _ = w.Stop() })
			w.SetConfig(service.cfg)
			replacement, errUpdate := manager.Update(coreauth.WithSkipPersist(context.Background()), activationSnapshot(t, w, snapshot.FileSnapshotPath()))
			if errUpdate != nil {
				t.Fatal(errUpdate)
			}
			// Plugin AuthData cannot carry the manager's private revision fields.
			encoded, errEncode := json.Marshal(base)
			if errEncode != nil {
				t.Fatal(errEncode)
			}
			var stale coreauth.Auth
			if errDecode := json.Unmarshal(encoded, &stale); errDecode != nil {
				t.Fatal(errDecode)
			}
			stale.FileName = base.FileName
			stale.Generation, stale.RegistrationEpoch, stale.CredentialVersion = 0, 0, 0
			stale.Metadata["model_cache"] = "stale discovery"
			_, committed, errConditional := manager.UpdateIfRegistrationCurrent(coreauth.WithSkipPersist(context.Background()), base, &stale)
			if errConditional != nil || committed {
				t.Fatalf("stale conditional discovery committed after %s replacement: committed=%v err=%v", field, committed, errConditional)
			}
			current, exists := manager.GetByID(snapshot.ID)
			if !exists || current.Generation != replacement.Generation || current.Metadata[field] != "replacement" || current.Metadata["model_cache"] != nil {
				t.Fatal("stale conditional update modified the replacement observation")
			}
		})
	}
}
