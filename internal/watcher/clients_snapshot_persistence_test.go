package watcher

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

type pendingSnapshotFileStore struct {
	*sdkauth.FileTokenStore
	beforeSave func() error
}

func (s *pendingSnapshotFileStore) Save(ctx context.Context, auth *coreauth.Auth) (string, error) {
	if s.beforeSave != nil {
		if errSave := s.beforeSave(); errSave != nil {
			return "", errSave
		}
	}
	return s.FileTokenStore.Save(ctx, auth)
}

func receiveSnapshotUpdate(t *testing.T, queue <-chan AuthUpdate) AuthUpdate {
	t.Helper()
	select {
	case update := <-queue:
		return update
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for file snapshot update")
		return AuthUpdate{}
	}
}

func TestDiscardedFileSnapshotRereadWaitsForPersistence(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failSave bool
		stop     bool
		replace  bool
	}{
		{name: "saved"},
		{name: "failed_save", failSave: true},
		{name: "stopped", stop: true},
		{name: "replaced", failSave: true, replace: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			path := filepath.Join(dir, "credential.json")
			if errWrite := os.WriteFile(path, []byte(`{"type":"claude","prefix":"old"}`), 0o600); errWrite != nil {
				t.Fatal(errWrite)
			}
			w, errWatcher := NewWatcher("", dir, nil)
			if errWatcher != nil {
				t.Fatal(errWatcher)
			}
			t.Cleanup(func() { _ = w.Stop() })
			w.SetConfig(&config.Config{AuthDir: dir})
			queue := make(chan AuthUpdate, 16)
			w.SetAuthUpdateQueue(queue)
			w.addOrUpdateClient(path)
			initial := receiveSnapshotUpdate(t, queue)
			store := &pendingSnapshotFileStore{FileTokenStore: sdkauth.NewFileTokenStore()}
			store.SetBaseDir(dir)
			manager := coreauth.NewManager(store, nil, nil)
			base, errRegister := manager.Register(coreauth.WithSkipPersist(ctx), initial.Auth.Clone())
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			snapshot := initial.Auth.Clone()
			entered, releaseSave := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseSave) }) }
			errSave := errors.New("disk full")
			store.beforeSave = func() error {
				close(entered)
				<-releaseSave
				if tc.failSave {
					return errSave
				}
				return nil
			}
			newer := base.Clone()
			newer.Prefix = "new"
			newer.Metadata["prefix"] = "new"
			saveDone := make(chan struct{})
			var errPersist error
			t.Cleanup(func() {
				release()
				<-saveDone
			})
			go func() {
				defer close(saveDone)
				_, errPersist = manager.Update(ctx, newer)
			}()
			<-entered
			current, errUpdate := manager.Update(coreauth.WithSkipPersist(ctx), snapshot)
			if errUpdate != nil || current.Prefix != "new" || !snapshot.HasFileSnapshot() {
				t.Fatalf("pending snapshot was not discarded: auth=%v err=%v", current, errUpdate)
			}
			w.clientsMutex.RLock()
			beforeRevision := w.authRevisions[base.ID]
			w.clientsMutex.RUnlock()
			for range 10 {
				w.RereadDiscardedFileSnapshot(snapshot)
			}
			w.clientsMutex.RLock()
			duringRevision := w.authRevisions[base.ID]
			pendingRereads := len(w.deferredFileSnapshotRereads)
			w.clientsMutex.RUnlock()
			if duringRevision != beforeRevision || pendingRereads != 1 {
				t.Fatalf("reread was not coalesced while persistence was pending: revision %d -> %d, pending=%d", beforeRevision, duringRevision, pendingRereads)
			}
			if tc.stop {
				if errStop := w.Stop(); errStop != nil {
					t.Fatal(errStop)
				}
			}
			var replacement AuthUpdate
			if tc.replace {
				if errWrite := os.WriteFile(path, []byte(`{"type":"claude","prefix":"external"}`), 0o600); errWrite != nil {
					t.Fatal(errWrite)
				}
				w.addOrUpdateClient(path)
				replacement = receiveSnapshotUpdate(t, queue)
			}
			release()
			<-saveDone
			if (!tc.failSave && errPersist != nil) || (tc.failSave && !errors.Is(errPersist, errSave)) {
				t.Fatalf("unexpected persistence result: %v", errPersist)
			}
			w.fileSnapshotRereadWG.Wait()
			if tc.stop {
				w.RereadDiscardedFileSnapshot(snapshot)
				w.clientsMutex.RLock()
				afterRevision := w.authRevisions[base.ID]
				w.clientsMutex.RUnlock()
				if afterRevision != beforeRevision {
					t.Fatal("stopped watcher reread a deferred snapshot")
				}
				return
			}
			resent := replacement
			if !tc.replace {
				resent = receiveSnapshotUpdate(t, queue)
			}
			applied, errApply := manager.Update(coreauth.WithSkipPersist(ctx), resent.Auth)
			want := "new"
			if tc.failSave {
				want = "old"
			}
			if tc.replace {
				want = "external"
			}
			if errApply != nil || applied.Prefix != want || resent.Auth.HasFileSnapshot() {
				t.Fatalf("deferred reread did not reconcile file: prefix=%q bound=%v err=%v, want %q", applied.Prefix, resent.Auth.HasFileSnapshot(), errApply, want)
			}
			w.clientsMutex.RLock()
			afterRevision := w.authRevisions[base.ID]
			w.clientsMutex.RUnlock()
			if afterRevision != beforeRevision+1 {
				t.Fatalf("duplicate deferred rereads: revision %d -> %d", beforeRevision, afterRevision)
			}
		})
	}
}
