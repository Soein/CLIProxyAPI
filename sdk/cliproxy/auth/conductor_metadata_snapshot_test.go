package auth

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
)

type metadataSnapshotStore struct {
	path   string
	onSave func(context.Context) error
}

func (*metadataSnapshotStore) List(context.Context) ([]*Auth, error) { return nil, nil }
func (s *metadataSnapshotStore) Delete(context.Context, string) error {
	return os.Remove(s.path)
}
func (s *metadataSnapshotStore) Save(ctx context.Context, auth *Auth) (string, error) {
	if isMetadataPersistContext(ctx) && s.onSave != nil {
		if errSave := s.onSave(ctx); errSave != nil {
			return "", errSave
		}
	}
	data, errMarshal := json.Marshal(auth.Metadata)
	if errMarshal != nil {
		return "", errMarshal
	}
	return s.path, os.WriteFile(s.path, data, 0o600)
}

func newMetadataSnapshotManager(t *testing.T) (*Manager, *Auth, *metadataSnapshotStore) {
	t.Helper()
	store := &metadataSnapshotStore{path: filepath.Join(t.TempDir(), "metadata.json")}
	manager := NewManager(store, nil, nil)
	seed, errRegister := manager.Register(t.Context(), &Auth{
		ID: "metadata.json", Provider: "antigravity", Status: StatusActive,
		Metadata: map[string]any{"access_token": "old"},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	return manager, seed, store
}

type versionedMetadataSnapshotStore struct {
	*metadataSnapshotStore
	current *Auth
}

func (s *versionedMetadataSnapshotStore) SaveVersioned(ctx context.Context, auth *Auth, expected uint64) (string, uint64, error) {
	path, errSave := s.Save(ctx, auth)
	if errSave != nil {
		return "", 0, errSave
	}
	s.current = auth.Clone()
	s.current.SetStoreGeneration(expected + 1)
	return path, expected + 1, nil
}

func (s *versionedMetadataSnapshotStore) GetByID(context.Context, string) (*Auth, error) {
	return s.current.Clone(), nil
}

func newVersionedMetadataSnapshotManager(t *testing.T) (*Manager, *Auth, *metadataSnapshotStore) {
	t.Helper()
	fileStore := &metadataSnapshotStore{path: filepath.Join(t.TempDir(), "metadata.json")}
	manager := NewManager(&versionedMetadataSnapshotStore{metadataSnapshotStore: fileStore}, nil, nil)
	seed, errRegister := manager.Register(t.Context(), metadataPersistenceAuth("metadata.json", "antigravity"))
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	return manager, seed, fileStore
}

func metadataFileSnapshot(t *testing.T, seed *Auth, path string) *Auth {
	t.Helper()
	data, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	snapshot := &Auth{
		ID: seed.ID, Provider: seed.Provider, Status: StatusActive,
		Label: "file observation", Metadata: map[string]any{"access_token": "old"},
	}
	snapshot.BindFileSnapshot(path, data)
	return snapshot
}

func pauseMetadataSnapshotValidation(t *testing.T, manager *Manager, snapshot *Auth) (func(), <-chan error) {
	t.Helper()
	readStarted := make(chan struct{})
	releaseRead := make(chan struct{})
	updateDone := make(chan error, 1)
	originalRead := readAuthSnapshotFileFunc
	readAuthSnapshotFileFunc = func(path string) ([]byte, error) {
		data, errRead := originalRead(path)
		close(readStarted)
		<-releaseRead
		return data, errRead
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRead) }) }
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, errUpdate := manager.Update(WithSkipPersist(t.Context()), snapshot)
		updateDone <- errUpdate
	}()
	t.Cleanup(func() {
		release()
		<-finished
		readAuthSnapshotFileFunc = originalRead
	})
	<-readStarted
	return release, updateDone
}

func TestPersistMetadataEntryFencesCompletedFileSnapshot(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			manager, seed, store := newMetadataSnapshotManager(t)
			if fail {
				store.onSave = func(context.Context) error { return errors.New("save failed") }
			}
			snapshot := metadataFileSnapshot(t, seed, store.path)
			releaseRead, updateDone := pauseMetadataSnapshotValidation(t, manager, snapshot)
			if saved, preempted := manager.PersistMetadataEntry(t.Context(), seed.ID, seed.RegistrationEpoch, "access_token", "new", nil); saved == fail || preempted {
				t.Fatalf("metadata persistence = (%v, %v)", saved, preempted)
			}
			releaseRead()
			if errUpdate := <-updateDone; errUpdate != nil {
				t.Fatal(errUpdate)
			}
			current, _ := manager.GetByID(seed.ID)
			if fail {
				if current.Metadata["access_token"] != "old" || current.Label != "file observation" || snapshot.HasFileSnapshot() {
					t.Fatalf("failed metadata Save invalidated unchanged disk observation: %+v bound=%v", current, snapshot.HasFileSnapshot())
				}
			} else if current.Metadata["access_token"] != "new" || current.Generation != seed.Generation || !snapshot.HasFileSnapshot() {
				t.Fatalf("old file observation replaced metadata: token=%v generation=%d bound=%v", current.Metadata["access_token"], current.Generation, snapshot.HasFileSnapshot())
			}
		})
	}
}

func TestPersistMetadataEntryFencesFileSnapshotAfterPreemptedCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		manager, seed, store := newMetadataSnapshotManager(t)
		other := seed.Clone()
		other.ID = "other.json"
		other, errRegister := manager.Register(WithSkipPersist(t.Context()), other)
		if errRegister != nil {
			t.Fatal(errRegister)
		}
		snapshot := metadataFileSnapshot(t, seed, store.path)
		releaseRead, updateDone := pauseMetadataSnapshotValidation(t, manager, snapshot)
		entered, releaseSave := make(chan struct{}), make(chan struct{})
		canceledBeforeCommit := false
		store.onSave = func(ctx context.Context) error {
			close(entered)
			<-releaseSave
			canceledBeforeCommit = errors.Is(ctx.Err(), context.Canceled)
			return nil
		}
		metadataDone := make(chan [2]bool, 1)
		go func() {
			saved, preempted := manager.PersistMetadataEntry(t.Context(), seed.ID, seed.RegistrationEpoch, "access_token", "new", nil)
			metadataDone <- [2]bool{saved, preempted}
		}()
		<-entered
		foregroundCtx, cancelForeground := context.WithCancel(t.Context())
		defer cancelForeground()
		foregroundDone := make(chan error, 1)
		go func() {
			other.Label = "foreground update"
			_, errUpdate := manager.Update(foregroundCtx, other)
			foregroundDone <- errUpdate
		}()
		synctest.Wait()
		cancelForeground()
		if errForeground := <-foregroundDone; !errors.Is(errForeground, context.Canceled) {
			t.Errorf("foreground mutation error=%v", errForeground)
		}
		close(releaseSave)
		if result := <-metadataDone; result != [2]bool{true, false} || !canceledBeforeCommit {
			t.Errorf("detached metadata commit=%v canceled=%v", result, canceledBeforeCommit)
		}
		releaseRead()
		if errUpdate := <-updateDone; errUpdate != nil {
			t.Fatal(errUpdate)
		}
		current, _ := manager.GetByID(seed.ID)
		if current.Metadata["access_token"] != "new" || current.Generation != seed.Generation || !snapshot.HasFileSnapshot() {
			t.Fatalf("old file observation replaced preempted commit: token=%v generation=%d bound=%v", current.Metadata["access_token"], current.Generation, snapshot.HasFileSnapshot())
		}
	})
}

func TestPersistMetadataEntryFencesFileSnapshotDuringSave(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		manager, seed, store := newMetadataSnapshotManager(t)
		snapshot := metadataFileSnapshot(t, seed, store.path)
		releaseRead, updateDone := pauseMetadataSnapshotValidation(t, manager, snapshot)
		entered, releaseSave := make(chan struct{}), make(chan struct{})
		store.onSave = func(context.Context) error {
			close(entered)
			<-releaseSave
			return nil
		}
		metadataDone := make(chan bool, 1)
		go func() {
			saved, _ := manager.PersistMetadataEntry(t.Context(), seed.ID, seed.RegistrationEpoch, "access_token", "new", nil)
			metadataDone <- saved
		}()
		<-entered
		releaseRead()
		errUpdate := <-updateDone
		current, _ := manager.GetByID(seed.ID)
		retained := snapshot.HasFileSnapshot()
		close(releaseSave)
		if !<-metadataDone {
			t.Error("metadata save failed")
		}
		if errUpdate != nil || current.Generation != seed.Generation || !retained {
			t.Fatalf("file observation applied during metadata Save: error=%v generation=%d bound=%v", errUpdate, current.Generation, retained)
		}
	})
}

func TestPersistMetadataEntryFileSaveDefersFreshSnapshotRead(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fail  bool
		setup func(*testing.T) (*Manager, *Auth, *metadataSnapshotStore)
	}{
		{name: "unversioned success", setup: newMetadataSnapshotManager},
		{name: "unversioned failure", fail: true, setup: newMetadataSnapshotManager},
		{name: "versioned success", setup: newVersionedMetadataSnapshotManager},
		{name: "versioned failure", fail: true, setup: newVersionedMetadataSnapshotManager},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				manager, seed, store := tc.setup(t)
				snapshot := metadataFileSnapshot(t, seed, store.path)
				entered, releaseSave := make(chan struct{}), make(chan struct{})
				store.onSave = func(context.Context) error {
					close(entered)
					<-releaseSave
					if tc.fail {
						return errors.New("save failed")
					}
					return nil
				}
				metadataDone := make(chan bool, 1)
				go func() {
					saved, _ := manager.PersistMetadataEntry(t.Context(), seed.ID, seed.RegistrationEpoch, "access_token", "new", nil)
					metadataDone <- saved
				}()
				<-entered
				originalRead := readAuthSnapshotFileFunc
				reads := 0
				readAuthSnapshotFileFunc = func(path string) ([]byte, error) {
					reads++
					return originalRead(path)
				}
				_, errUpdate := manager.Update(WithSkipPersist(t.Context()), snapshot)
				readAuthSnapshotFileFunc = originalRead
				waiting := snapshot.fileSnapshotPersistenceDone
				retained := snapshot.HasFileSnapshot()
				close(releaseSave)
				saved := <-metadataDone
				if errUpdate != nil || reads != 0 || !retained || waiting == nil {
					t.Errorf("snapshot read during metadata Save: error=%v reads=%d bound=%v waiting=%v", errUpdate, reads, retained, waiting != nil)
				}
				if saved == tc.fail {
					t.Errorf("metadata saved=%v, fail=%v", saved, tc.fail)
				}
				if waiting != nil {
					select {
					case <-waiting:
					default:
						t.Error("metadata Save did not release snapshot waiters")
					}
				}
				if tc.fail {
					if _, errRetry := manager.Update(WithSkipPersist(t.Context()), snapshot); errRetry != nil || snapshot.HasFileSnapshot() {
						t.Errorf("unchanged disk observation did not apply after failed Save: %v", errRetry)
					}
				}
			})
		})
	}
}

func TestPersistMetadataEntryFileSavePreservesExistingPersistenceMarker(t *testing.T) {
	for _, newer := range []bool{false, true} {
		t.Run(map[bool]string{false: "same revision", true: "newer revision"}[newer], func(t *testing.T) {
			manager, seed, _ := newMetadataSnapshotManager(t)
			marker := seed.Clone()
			if newer {
				marker.durableRevision++
			}
			manager.mu.Lock()
			manager.markPersistenceInFlightLocked(t.Context(), marker)
			done := manager.persistenceInFlightDone[seed.ID]
			manager.mu.Unlock()
			if saved, _ := manager.PersistMetadataEntry(t.Context(), seed.ID, seed.RegistrationEpoch, "hint", "models", nil); !saved {
				t.Fatal("metadata save failed")
			}
			manager.mu.RLock()
			retained := manager.persistenceInFlightDone[seed.ID] == done && manager.persistenceInFlightRevisions[seed.ID] == marker.durableRevision
			manager.mu.RUnlock()
			if !retained {
				t.Error("metadata save replaced or cleared another persistence marker")
			}
			select {
			case <-done:
				t.Error("metadata save released another persistence wait channel")
			default:
			}
			manager.clearPersistenceInFlight(seed.ID, marker.durableRevision)
		})
	}
}

func TestPersistMetadataEntryFileSavePreservesSDKUpdateOrdering(t *testing.T) {
	manager, seed, _ := newMetadataSnapshotManager(t)
	update := seed.Clone()
	update.Label = "SDK update"
	if saved, _ := manager.PersistMetadataEntry(t.Context(), seed.ID, seed.RegistrationEpoch, "hint", "models", nil); !saved {
		t.Fatal("metadata save failed")
	}
	updated, errUpdate := manager.Update(t.Context(), update)
	if errUpdate != nil || updated.Label != update.Label || updated.Generation != seed.Generation+1 {
		t.Fatalf("metadata save fenced ordinary SDK update: auth=%+v error=%v", updated, errUpdate)
	}
}

func TestPersistMetadataEntryVersionedSaveFencesFileSnapshot(t *testing.T) {
	manager, seed, store := newVersionedMetadataSnapshotManager(t)
	snapshot := metadataFileSnapshot(t, seed, store.path)
	releaseRead, updateDone := pauseMetadataSnapshotValidation(t, manager, snapshot)
	if saved, preempted := manager.PersistMetadataEntry(t.Context(), seed.ID, seed.RegistrationEpoch, "access_token", "new", nil); !saved || preempted {
		t.Fatalf("metadata persistence = (%v, %v)", saved, preempted)
	}
	releaseRead()
	if errUpdate := <-updateDone; errUpdate != nil {
		t.Fatal(errUpdate)
	}
	current, _ := manager.GetByID(seed.ID)
	if current.Metadata["access_token"] != "new" || current.Generation != seed.Generation || current.durableRevision != seed.durableRevision || current.StoreGeneration() != seed.StoreGeneration()+1 || !snapshot.HasFileSnapshot() {
		t.Fatalf("old file observation replaced versioned metadata: %+v bound=%v", current, snapshot.HasFileSnapshot())
	}
}

func TestPersistMetadataEntryVersionedSaveAllowsReentrantReload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		manager, seed, store := newVersionedMetadataSnapshotManager(t)
		reloaded := false
		store.onSave = func(ctx context.Context) error {
			if errReload := manager.ReloadByID(ctx, seed.ID); errReload != nil {
				return errReload
			}
			reloaded = true
			return nil
		}
		if saved, preempted := manager.PersistMetadataEntry(t.Context(), seed.ID, seed.RegistrationEpoch, "hint", "models", nil); !saved || preempted || !reloaded {
			t.Fatalf("reentrant metadata persistence = (%v, %v), reloaded=%v", saved, preempted, reloaded)
		}
		current, _ := manager.GetByID(seed.ID)
		if current.Metadata["hint"] != "models" || current.StoreGeneration() != seed.StoreGeneration()+1 {
			t.Fatalf("reentrant reload lost metadata commit: %+v", current)
		}
	})
}
