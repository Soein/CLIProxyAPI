package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestObjectTokenStoreManagerLoadWaitsForUploadRollback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "uploaded.json")
	ctx := context.Background()
	const previous = `{"type":"qwen","access_token":"previous"}`
	const pending = `{"type":"qwen","access_token":"pending"}`
	if errWrite := os.WriteFile(path, []byte(previous), 0o600); errWrite != nil {
		t.Fatalf("seed source: %v", errWrite)
	}
	backendEntered := make(chan struct{})
	releaseBackend := make(chan struct{})
	errRejected := errors.New("object store rejected upload")
	store := &ObjectTokenStore{authDir: dir, putObjectFn: func(context.Context, string, []byte, string) error {
		close(backendEntered)
		<-releaseBackend
		return errRejected
	}}
	manager := cliproxyauth.NewManager(store, nil, nil)
	uploadDone := make(chan error, 1)
	go func() {
		uploadDone <- store.PersistAuthFileUpload(ctx, path, path, func(persist func() error) error {
			if errWrite := os.WriteFile(path, []byte(pending), 0o600); errWrite != nil {
				return errWrite
			}
			if errPersist := persist(); errPersist != nil {
				return errors.Join(errPersist, os.WriteFile(path, []byte(previous), 0o600))
			}
			return nil
		})
	}()
	select {
	case <-backendEntered:
	case errUpload := <-uploadDone:
		t.Fatalf("upload exited before reaching backend: %v", errUpload)
	}
	loadStarted := make(chan struct{})
	loadDone := make(chan error, 1)
	go func() {
		close(loadStarted)
		loadDone <- manager.Load(ctx)
	}()
	<-loadStarted
	var errLoad error
	select {
	case errLoad = <-loadDone:
		close(releaseBackend)
		<-uploadDone
		t.Fatalf("manager load completed while credential was still tentative: %v", errLoad)
	case <-time.After(100 * time.Millisecond):
		close(releaseBackend)
	}
	if errUpload := <-uploadDone; !errors.Is(errUpload, errRejected) {
		t.Fatalf("upload = %v, want backend rejection", errUpload)
	}
	if errLoad = <-loadDone; errLoad != nil {
		t.Fatalf("load after rollback: %v", errLoad)
	}
	if auth, _ := manager.GetByID(filepath.Base(path)); auth == nil || auth.Metadata["access_token"] != "previous" {
		t.Fatalf("manager loaded unexpected credential after rollback: %+v", auth)
	}
}
