package auth

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/authfilelock"
)

func registerFileSnapshot(t *testing.T, path string) *Auth {
	t.Helper()
	data, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	var metadata map[string]any
	if errDecode := json.Unmarshal(data, &metadata); errDecode != nil {
		t.Fatal(errDecode)
	}
	auth := &Auth{ID: filepath.Base(path), FileName: filepath.Base(path), Provider: "audit", Status: StatusActive, Metadata: metadata}
	auth.BindFileSnapshot(path, data)
	return auth
}

func TestFileSnapshotRegisterExplicitCreateAndReplace(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "credential.json")
	manager := NewManager(&snapshotOrderingStore{path: path}, nil, nil)
	for i, token := range []string{"created-token", "replacement-token"} {
		metadata := map[string]any{"access_token": token}
		data, errMarshal := json.Marshal(metadata)
		if errMarshal != nil {
			t.Fatal(errMarshal)
		}
		auth := &Auth{ID: filepath.Base(path), FileName: filepath.Base(path), Provider: "audit", Status: StatusActive, Metadata: metadata}
		auth.BindFileSnapshot(path, data)
		observation := auth.Clone()
		if _, errObserve := manager.Register(WithSkipPersist(ctx), observation); errObserve != nil || !observation.HasFileSnapshot() {
			t.Fatalf("unpersisted file observation was accepted: bound=%v err=%v", observation.HasFileSnapshot(), errObserve)
		}
		if i == 0 {
			if _, errStat := os.Stat(path); !os.IsNotExist(errStat) {
				t.Fatalf("file observation created the missing source: %v", errStat)
			}
		}
		current, errRegister := manager.Register(ctx, auth)
		if errRegister != nil || current == nil || current.Metadata["access_token"] != token || auth.Generation != 1 || auth.RegistrationEpoch != uint64(i+1) || auth.CredentialVersion != uint64(i+1) || auth.HasFileSnapshot() {
			t.Fatalf("explicit registration did not create or replace the credential: auth=%+v bound=%v err=%v", current, auth.HasFileSnapshot(), errRegister)
		}
		if persisted := registerFileSnapshot(t, path); persisted.Metadata["access_token"] != token {
			t.Fatalf("explicit registration did not persist %q: got %v", token, persisted.Metadata["access_token"])
		}
	}
}

type registerSnapshotCallbackStore struct {
	snapshotOrderingStore
	beforeSave func()
}

func (s *registerSnapshotCallbackStore) Save(ctx context.Context, auth *Auth) (string, error) {
	unlock := authfilelock.Lock(s.path)
	defer unlock()
	if s.beforeSave != nil {
		s.beforeSave()
	}
	return s.snapshotOrderingStore.Save(ctx, auth)
}

func TestFileSnapshotRegisterDefersReentrantSaveAndPreservesLifecycle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "credential.json")
	store := &registerSnapshotCallbackStore{snapshotOrderingStore: snapshotOrderingStore{path: path}}
	manager := NewManager(store, nil, nil)
	base, errRegister := manager.Register(ctx, &Auth{ID: filepath.Base(path), Provider: "audit", Status: StatusActive, Metadata: map[string]any{"access_token": "old-token"}})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	snapshot := registerFileSnapshot(t, path)
	store.beforeSave = func() {
		current, errSnapshot := manager.Register(WithSkipPersist(ctx), snapshot)
		if errSnapshot != nil || current == nil || current.Metadata["access_token"] != "new-token" || !snapshot.HasFileSnapshot() || snapshot.FileSnapshotPersistenceDone() == nil {
			t.Fatalf("reentrant registration lost pending save: auth=%+v bound=%v done=%v err=%v", current, snapshot.HasFileSnapshot(), snapshot.FileSnapshotPersistenceDone(), errSnapshot)
		}
	}
	newer := base.Clone()
	newer.Metadata["access_token"] = "new-token"
	if _, errUpdate := manager.Update(ctx, newer); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	store.beforeSave = nil
	select {
	case <-snapshot.FileSnapshotPersistenceDone():
	default:
		t.Fatal("deferred registration did not observe save completion")
	}
	if _, errRetry := manager.Register(WithSkipPersist(ctx), snapshot); errRetry != nil || !snapshot.HasFileSnapshot() {
		t.Fatalf("old file observation was not rejected after save: bound=%v err=%v", snapshot.HasFileSnapshot(), errRetry)
	}
	manager.MarkResult(ctx, Result{AuthID: base.ID, Provider: base.Provider, Success: true})
	before, _ := manager.GetByID(base.ID)
	current, errCurrent := manager.Register(WithSkipPersist(ctx), registerFileSnapshot(t, path))
	if errCurrent != nil || current == nil || current.Metadata["access_token"] != "new-token" || current.Success != 1 || current.RegistrationEpoch != base.RegistrationEpoch || current.Generation != before.Generation+1 {
		t.Fatalf("current file observation reset existing lifecycle: auth=%+v err=%v", current, errCurrent)
	}
}

func TestFileSnapshotRegisterDefersAbsentPendingPersistence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "credential.json")
	writeRegisterSnapshotFile(t, path, "old-token")
	snapshot := registerFileSnapshot(t, path)
	manager := NewManager(&snapshotOrderingStore{path: path}, nil, nil)
	pending := &Auth{ID: snapshot.ID, Provider: snapshot.Provider, Metadata: map[string]any{"access_token": "pending-token"}}
	manager.mu.Lock()
	pending.durableRevision = manager.nextAuthDurableRevisionLocked()
	manager.markPersistenceInFlightLocked(ctx, pending)
	manager.mu.Unlock()
	t.Cleanup(func() { manager.clearPersistenceInFlight(pending.ID, pending.durableRevision) })
	current, errRegister := manager.Register(WithSkipPersist(ctx), snapshot)
	if errRegister != nil || current != nil || !snapshot.HasFileSnapshot() || snapshot.FileSnapshotPersistenceDone() == nil {
		t.Fatalf("absent pending save was not deferred: auth=%+v bound=%v done=%v err=%v", current, snapshot.HasFileSnapshot(), snapshot.FileSnapshotPersistenceDone(), errRegister)
	}
	if _, ok := manager.GetByID(snapshot.ID); ok {
		t.Fatal("pending save allowed absent file snapshot registration")
	}
	manager.clearPersistenceInFlight(pending.ID, pending.durableRevision)
	select {
	case <-snapshot.FileSnapshotPersistenceDone():
	default:
		t.Fatal("absent deferred registration lost completion notification")
	}
}

func writeRegisterSnapshotFile(t *testing.T, path, token string) {
	t.Helper()
	data, errMarshal := json.Marshal(map[string]any{"access_token": token})
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
}

func TestFileSnapshotRegisterRejectsDeletedAndChangedFiles(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "credential.json")
	manager := NewManager(&snapshotOrderingStore{path: path}, nil, nil)
	writeRegisterSnapshotFile(t, path, "old-token")
	initial := registerFileSnapshot(t, path)
	base, errRegister := manager.Register(WithSkipPersist(ctx), initial)
	if errRegister != nil || base == nil {
		t.Fatalf("register initial file: auth=%v err=%v", base, errRegister)
	}
	if initial.Generation != 1 || initial.RegistrationEpoch != base.RegistrationEpoch || initial.CredentialVersion != 1 || initial.HasFileSnapshot() {
		t.Fatalf("accepted registration did not populate its input: generation=%d epoch=%d version=%d bound=%v", initial.Generation, initial.RegistrationEpoch, initial.CredentialVersion, initial.HasFileSnapshot())
	}
	stale := registerFileSnapshot(t, path)
	if errDelete := manager.DeleteAuths(ctx, []string{base.ID}, func(context.Context) error { return os.Remove(path) }); errDelete != nil {
		t.Fatal(errDelete)
	}
	for _, state := range []string{"deleted", "recreated"} {
		if state == "recreated" {
			writeRegisterSnapshotFile(t, path, "new-token")
		}
		returned, errRetry := manager.Register(WithSkipPersist(ctx), stale)
		if errRetry != nil || returned != nil || !stale.HasFileSnapshot() {
			t.Fatalf("%s stale retry: auth=%v bound=%v err=%v", state, returned, stale.HasFileSnapshot(), errRetry)
		}
		if _, ok := manager.GetByID(base.ID); ok {
			t.Fatalf("%s file retry restored a removed auth", state)
		}
	}
	recreated := registerFileSnapshot(t, path)
	current, errRecreate := manager.Register(WithSkipPersist(ctx), recreated)
	if errRecreate != nil || current == nil || current.Metadata["access_token"] != "new-token" || recreated.HasFileSnapshot() || current.RegistrationEpoch <= base.RegistrationEpoch {
		t.Fatalf("new file observation was not accepted: auth=%+v bound=%v err=%v", current, recreated.HasFileSnapshot(), errRecreate)
	}
}

func TestFileSnapshotRegisterFencesConcurrentLifecycle(t *testing.T) {
	for _, mutation := range []string{"none", "delete_absent", "register", "register_remove"} {
		t.Run(mutation, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "credential.json")
			writeRegisterSnapshotFile(t, path, "old-token")
			manager := NewManager(&snapshotOrderingStore{path: path}, nil, nil)
			snapshot := registerFileSnapshot(t, path)
			readStarted, releaseRead, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseRead) }) }
			originalRead := readAuthSnapshotFileFunc
			readAuthSnapshotFileFunc = func(path string) ([]byte, error) {
				data, errRead := originalRead(path)
				close(readStarted)
				<-releaseRead
				return data, errRead
			}
			t.Cleanup(func() {
				release()
				<-done
				readAuthSnapshotFileFunc = originalRead
			})
			var returned *Auth
			var errRegister error
			go func() {
				defer close(done)
				returned, errRegister = manager.Register(WithSkipPersist(ctx), snapshot)
			}()
			select {
			case <-readStarted:
			case <-done:
				t.Fatal("registration returned before validating the backing file")
			}
			switch mutation {
			case "delete_absent":
				if errDelete := manager.DeleteAuths(ctx, []string{snapshot.ID}, func(context.Context) error { return os.Remove(path) }); errDelete != nil {
					t.Fatal(errDelete)
				}
			case "register", "register_remove":
				if _, errNew := manager.Register(ctx, &Auth{ID: snapshot.ID, Provider: snapshot.Provider, Status: StatusActive, Metadata: map[string]any{"access_token": "new-token"}}); errNew != nil {
					t.Fatal(errNew)
				}
				if mutation == "register_remove" {
					manager.Remove(ctx, snapshot.ID)
					writeRegisterSnapshotFile(t, path, "old-token")
				}
			}
			release()
			<-done
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			current, ok := manager.GetByID(snapshot.ID)
			switch mutation {
			case "none":
				if !ok || current.Metadata["access_token"] != "old-token" || snapshot.HasFileSnapshot() {
					t.Fatalf("current new file was not accepted: auth=%+v bound=%v", current, snapshot.HasFileSnapshot())
				}
			case "register":
				if !ok || current.Metadata["access_token"] != "new-token" || !snapshot.HasFileSnapshot() || returned == nil || returned.Metadata["access_token"] != "new-token" {
					t.Fatalf("snapshot replaced concurrent registration: auth=%+v returned=%+v bound=%v", current, returned, snapshot.HasFileSnapshot())
				}
			default:
				if ok || returned != nil || !snapshot.HasFileSnapshot() {
					t.Fatalf("snapshot survived intervening lifecycle: auth=%+v returned=%+v bound=%v", current, returned, snapshot.HasFileSnapshot())
				}
			}
		})
	}
}
