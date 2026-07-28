package auth

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/authfilelock"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

type testTokenStorage struct {
	meta map[string]any
}

func (s *testTokenStorage) SetMetadata(meta map[string]any) { s.meta = meta }

func (s *testTokenStorage) SaveTokenToFile(authFilePath string) error {
	raw, err := json.Marshal(s.meta)
	if err != nil {
		return err
	}
	return os.WriteFile(authFilePath, raw, 0o600)
}

func TestFileTokenStore_Save_DisabledPersistsFlagForTokenStorage(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	path := filepath.Join(baseDir, "disabled.json")

	if err := os.WriteFile(path, []byte(`{"type":"test","disabled":true}`), 0o600); err != nil {
		t.Fatalf("seed auth file: %v", err)
	}

	store := NewFileTokenStore()
	store.SetBaseDir(baseDir)
	storage := &testTokenStorage{}

	auth := &cliproxyauth.Auth{
		ID:       "disabled.json",
		Provider: "test",
		FileName: "disabled.json",
		Disabled: true,
		Storage:  storage,
		Metadata: map[string]any{"type": "test"},
	}

	if _, err := store.Save(ctx, auth); err != nil {
		t.Fatalf("Save() error: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read auth file: %v", err)
	}
	var meta map[string]any
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatalf("unmarshal auth file: %v", err)
	}
	if disabled, _ := meta["disabled"].(bool); !disabled {
		t.Fatalf("disabled=%v, want true (raw=%s)", meta["disabled"], string(raw))
	}
}

func TestFileTokenStore_SaveWaitsForAuthFileLock(t *testing.T) {
	baseDir := t.TempDir()
	path := filepath.Join(baseDir, "locked.json")
	if err := os.WriteFile(path, []byte(`{"value":"old"}`), 0o600); err != nil {
		t.Fatalf("seed auth file: %v", err)
	}

	store := NewFileTokenStore()
	store.SetBaseDir(baseDir)
	auth := &cliproxyauth.Auth{
		ID:       "locked.json",
		FileName: "locked.json",
		Metadata: map[string]any{"value": "new"},
	}

	unlock := authfilelock.Lock(path)
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()

	result := make(chan error, 1)
	go func() {
		_, err := store.Save(context.Background(), auth)
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
