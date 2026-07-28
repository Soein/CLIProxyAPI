package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/authfilelock"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func TestBackendStoresSaveWaitsForAuthFileLock(t *testing.T) {
	objectAuthDir := t.TempDir()
	gitAuthDir := t.TempDir()
	gitStore := NewGitTokenStore("", "", "", "")
	gitStore.SetBaseDir(gitAuthDir)
	postgresStore := newPostgresBatchTestStore(t, &postgresBatchDriverState{upsertRows: 1})

	tests := []struct {
		name  string
		store cliproxyauth.Store
		path  string
	}{
		{
			name: "object",
			store: &ObjectTokenStore{
				authDir: objectAuthDir,
			},
			path: filepath.Join(objectAuthDir, "locked.json"),
		},
		{
			name:  "postgres",
			store: postgresStore,
			path:  filepath.Join(postgresStore.authDir, "locked.json"),
		},
		{
			name:  "git",
			store: gitStore,
			path:  filepath.Join(gitAuthDir, "locked.json"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertSaveWaitsForAuthFileLock(t, test.store, test.path)
		})
	}
}

func assertSaveWaitsForAuthFileLock(t *testing.T, store cliproxyauth.Store, path string) {
	t.Helper()

	unlock := authfilelock.Lock(path)
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()

	result := make(chan error, 1)
	go func() {
		_, err := store.Save(context.Background(), &cliproxyauth.Auth{
			ID:       filepath.Base(path),
			FileName: filepath.Base(path),
			Disabled: true,
			Metadata: map[string]any{"type": "test"},
		})
		result <- err
	}()

	select {
	case err := <-result:
		t.Fatalf("Save returned while the auth file lock was held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	unlock()
	locked = false
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Save() error after releasing auth file lock: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Save did not finish after releasing auth file lock")
	}
}
