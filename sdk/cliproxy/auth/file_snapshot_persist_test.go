package auth_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	sdkauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

type snapshotOrderingFileStore struct {
	*sdkauth.FileTokenStore
	beforeSave func() error
}

func (s *snapshotOrderingFileStore) Save(ctx context.Context, auth *coreauth.Auth) (string, error) {
	if hook := s.beforeSave; hook != nil {
		if errHook := hook(); errHook != nil {
			return "", errHook
		}
	}
	return s.FileTokenStore.Save(ctx, auth)
}

func TestFileSnapshotCannotReplacePendingFileSave(t *testing.T) {
	for _, failSave := range []bool{false, true} {
		name := "saved"
		if failSave {
			name = "failed_save_can_reconcile"
		}
		t.Run(name, func(t *testing.T) {
			manager, originalStore, path := newTrafficManager(t)
			store := &snapshotOrderingFileStore{FileTokenStore: originalStore.FileTokenStore}
			manager.SetStore(store)
			stale := readTrafficSnapshot(t, path, "new-model")
			newer, _ := manager.GetByID(trafficAuthID)
			newer.Prefix = "pfx-2"
			newer.Metadata["prefix"] = "pfx-2"
			entered, releaseSave := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseSave) }) }
			errSave := errors.New("disk full")
			store.beforeSave = func() error {
				close(entered)
				<-releaseSave
				if failSave {
					return errSave
				}
				return nil
			}
			done := make(chan struct{})
			var errUpdate error
			t.Cleanup(func() {
				release()
				<-done
			})
			go func() {
				defer close(done)
				_, errUpdate = manager.Update(context.Background(), newer)
			}()
			<-entered
			before, _ := manager.GetByID(trafficAuthID)
			if before.Prefix != "pfx-2" || diskField(t, path, "prefix") != "pfx-1" {
				t.Fatal("new runtime must be published before the file write")
			}
			current := requireDiscarded(t, manager, stale)
			if current.Prefix != "pfx-2" || !stale.HasFileSnapshot() {
				t.Fatal("pending save lost its runtime candidate or discarded binding")
			}
			release()
			<-done
			if (failSave && !errors.Is(errUpdate, errSave)) || (!failSave && errUpdate != nil) {
				t.Fatalf("save result = %v, failSave=%v", errUpdate, failSave)
			}
			store.beforeSave = nil
			if failSave {
				requireApplied(t, manager, stale)
			} else {
				requireDiscarded(t, manager, stale)
				requireApplied(t, manager, readTrafficSnapshot(t, path, "new-model"))
			}
			want := "pfx-2"
			if failSave {
				want = "pfx-1"
			}
			if current, _ := manager.GetByID(trafficAuthID); current.Prefix != want || diskField(t, path, "prefix") != want {
				t.Fatalf("final runtime prefix=%q disk=%v, want %q", current.Prefix, diskField(t, path, "prefix"), want)
			}
		})
	}
}

func TestFileSnapshotStoreCallbackCanReenterManager(t *testing.T) {
	manager, originalStore, path := newTrafficManager(t)
	store := &snapshotOrderingFileStore{FileTokenStore: originalStore.FileTokenStore}
	manager.SetStore(store)
	snapshot := readTrafficSnapshot(t, path, "new-model")
	store.beforeSave = func() error {
		requireDiscarded(t, manager, snapshot)
		return nil
	}
	newer, _ := manager.GetByID(trafficAuthID)
	newer.Prefix = "pfx-2"
	newer.Metadata["prefix"] = "pfx-2"
	if _, errUpdate := manager.Update(context.Background(), newer); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	if current, _ := manager.GetByID(trafficAuthID); current.Prefix != "pfx-2" || diskField(t, path, "prefix") != "pfx-2" {
		t.Fatalf("callback replaced pending credential: runtime prefix=%q disk=%v", current.Prefix, diskField(t, path, "prefix"))
	}
}

func TestFileSnapshotDefersDuringMetaMint(t *testing.T) {
	for _, operation := range []string{"prepare", "refresh"} {
		for _, failSave := range []bool{false, true} {
			name := operation + "/saved"
			if failSave {
				name = operation + "/failed_save"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				dir := t.TempDir()
				path := filepath.Join(dir, "meta.json")
				store := &snapshotOrderingFileStore{FileTokenStore: sdkauth.NewFileTokenStore()}
				store.SetBaseDir(dir)
				manager := coreauth.NewManager(store, nil, nil)
				base, errRegister := manager.Register(ctx, &coreauth.Auth{
					ID: "meta.json", FileName: "meta.json", Provider: "meta", Status: coreauth.StatusActive,
					Metadata: map[string]any{"type": "meta", "access_token": "old-token"},
				})
				if errRegister != nil {
					t.Fatal(errRegister)
				}
				data, errRead := os.ReadFile(path)
				if errRead != nil {
					t.Fatal(errRead)
				}
				snapshot := &coreauth.Auth{
					ID: base.ID, Provider: base.Provider, Status: coreauth.StatusActive,
					Metadata: map[string]any{"type": "meta", "access_token": "old-token"},
				}
				snapshot.BindFileSnapshot(path, data)
				errSave := errors.New("mint save failed")
				store.beforeSave = func() error {
					current, _ := manager.GetByID(base.ID)
					if current.Metadata["access_token"] != "old-token" {
						t.Fatal("Meta exposed its private candidate before persistence completed")
					}
					returned := requireDiscarded(t, manager, snapshot)
					if returned.Metadata["access_token"] != "old-token" || snapshot.FileSnapshotPersistenceDone() == nil {
						t.Fatal("Meta file snapshot was not deferred on the private candidate's persistence")
					}
					manager.MarkResult(ctx, coreauth.Result{AuthID: base.ID, Provider: base.Provider, Success: true})
					if failSave {
						return errSave
					}
					return nil
				}
				minted := base.Clone()
				minted.Metadata["access_token"] = "minted-token"
				var errUpdate error
				if operation == "prepare" {
					_, errUpdate = manager.UpdatePreparedAuth(ctx, base, minted)
				} else {
					_, errUpdate = manager.UpdateRefreshedAuth(ctx, base, minted)
				}
				if (!failSave && errUpdate != nil) || (failSave && !errors.Is(errUpdate, errSave)) {
					t.Fatalf("unexpected Meta save result: %v", errUpdate)
				}
				select {
				case <-snapshot.FileSnapshotPersistenceDone():
				default:
					t.Fatal("Meta persistence completion did not release the deferred snapshot")
				}
				want := "minted-token"
				if failSave {
					want = "old-token"
				}
				if current, _ := manager.GetByID(base.ID); current.Metadata["access_token"] != want || current.Success != 1 || diskField(t, path, "access_token") != want {
					t.Fatalf("Meta transaction lost credentials or concurrent result: token=%v success=%d disk=%v, want %q and one success", current.Metadata["access_token"], current.Success, diskField(t, path, "access_token"), want)
				}
			})
		}
	}
}
