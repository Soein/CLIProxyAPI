package watcher

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/authfilelock"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func TestWatcherDefersFileEventsDuringAuthUpload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "accounts.json")
	w, errNewWatcher := NewWatcher(filepath.Join(dir, "config.yaml"), dir, nil)
	if errNewWatcher != nil {
		t.Fatal(errNewWatcher)
	}
	t.Cleanup(func() { _ = w.Stop() })
	w.SetConfig(&config.Config{AuthDir: dir})
	w.SetPluginAuthParser(&fileDeleteTestParser{})
	updates := make(chan AuthUpdate, 8)
	w.SetAuthUpdateQueue(updates)
	readUpdate := func() AuthUpdate {
		t.Helper()
		select {
		case update := <-updates:
			return update
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for file auth update")
			return AuthUpdate{}
		}
	}
	writeWatcherUploadTestFile(t, path, `{"type":"plugin","ids":["a","b"]}`)
	w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Create})
	readUpdate()
	readUpdate()

	finishUpload := authfilelock.BeginUpload(path)
	t.Cleanup(func() {
		if finishUpload != nil {
			finishUpload()
		}
	})
	writeWatcherUploadTestFile(t, path, `{"type":"plugin","ids":["b","c"]}`)
	w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Write})
	w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Write})
	assertWatcherCommittedUploadIDs(t, w)

	finishUpload()
	finishUpload = nil
	want := map[string]AuthUpdateAction{"a": AuthUpdateActionDelete, "b": AuthUpdateActionModify, "c": AuthUpdateActionAdd}
	for range len(want) {
		update := readUpdate()
		if update.Action != want[update.ID] || update.IsFileRemoval() {
			t.Fatalf("unexpected committed file update: id=%q action=%s fileRemoval=%t", update.ID, update.Action, update.IsFileRemoval())
		}
		delete(want, update.ID)
	}
}

func TestWatcherReloadPreservesCommittedAuthsDuringUpload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "accounts.json")
	w := &Watcher{
		authDir:          dir,
		config:           &config.Config{AuthDir: dir},
		pluginAuthParser: &fileDeleteTestParser{},
	}
	writeWatcherUploadTestFile(t, path, `{"type":"plugin","ids":["a","b"]}`)
	w.addOrUpdateClient(path)

	finishUpload := authfilelock.BeginUpload(path)
	defer finishUpload()
	writeWatcherUploadTestFile(t, path, `{"type":"plugin","ids":["b","c"]}`)
	w.reloadClients(true, nil, false)
	assertWatcherCommittedUploadIDs(t, w)
	if _, cached := w.fileAuthsByPath[w.normalizeAuthPath(path)]["c"]; cached {
		t.Fatal("reload indexed a tentative plugin auth")
	}
}

func TestWatcherPreservesAuthsDuringPendingMirrorRemoval(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "accounts.json")
	w := &Watcher{
		authDir:          dir,
		config:           &config.Config{AuthDir: dir},
		pluginAuthParser: &fileDeleteTestParser{},
	}
	const initial = `{"type":"plugin","ids":["a","b"]}`
	writeWatcherUploadTestFile(t, path, initial)
	w.addOrUpdateClient(path)

	finishUpload := authfilelock.BeginUpload(path)
	t.Cleanup(func() {
		if finishUpload != nil {
			finishUpload()
		}
	})
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatal(errRemove)
	}
	w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Remove})
	assertWatcherCommittedUploadIDs(t, w)

	writeWatcherUploadTestFile(t, path, initial)
	finishUpload()
	finishUpload = nil
	w.fileSnapshotRereadWG.Wait()
	assertWatcherCommittedUploadIDs(t, w)
}

func TestSnapshotAuthFileRejectsPendingUpload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "account.json")
	w := &Watcher{authDir: dir, config: &config.Config{AuthDir: dir}}
	finishUpload := authfilelock.BeginUpload(path)
	defer finishUpload()
	writeWatcherUploadTestFile(t, path, `{"type":"codex"}`)
	auths, errSnapshot := w.SnapshotAuthFile(path)
	if errSnapshot == nil || len(auths) != 0 {
		t.Fatalf("snapshot during auth upload returned %d auths, err=%v", len(auths), errSnapshot)
	}
}

func writeWatcherUploadTestFile(t *testing.T, path, data string) {
	t.Helper()
	if errWrite := os.WriteFile(path, []byte(data), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
}

func assertWatcherCommittedUploadIDs(t *testing.T, w *Watcher) {
	t.Helper()
	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	if as, bs, cs := w.currentAuths["a"] != nil, w.currentAuths["b"] != nil, w.currentAuths["c"] != nil; !as || !bs || cs {
		t.Fatalf("expected committed IDs a and b during upload, got a=%t b=%t c=%t", as, bs, cs)
	}
}
