package auth

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

type dummyAuthenticator struct {
	provider string
	record   *coreauth.Auth
}

type recordingStore struct {
	saved   bool
	record  *coreauth.Auth
	baseDir string
}

func (*recordingStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }

func (s *recordingStore) Save(_ context.Context, auth *coreauth.Auth) (string, error) {
	s.saved = true
	s.record = auth
	return auth.FileName, nil
}

func (*recordingStore) Delete(context.Context, string) error { return nil }

func (s *recordingStore) SetBaseDir(dir string) { s.baseDir = dir }

func (d *dummyAuthenticator) Provider() string {
	return d.provider
}

func (d *dummyAuthenticator) Login(ctx context.Context, cfg *config.Config, opts *LoginOptions) (*coreauth.Auth, error) {
	return d.record, nil
}

func (d *dummyAuthenticator) RefreshLead() *time.Duration {
	return nil
}

func TestManagerLogin_PreservesExistingAuthFileMetadata(t *testing.T) {
	authDir := t.TempDir()
	fileName := "demo.json"
	filePath := filepath.Join(authDir, fileName)

	// Pre-populate existing auth file with custom settings
	existing := map[string]any{
		"type":         "demo",
		"email":        "user@example.com",
		"access_token": "old-token",
		"account_id":   "old-account",
		"prefix":       "my-prefix",
		"websockets":   false,
		"note":         "important note",
		"weight":       float64(10),
	}
	raw, errMarshal := json.Marshal(existing)
	if errMarshal != nil {
		t.Fatalf("marshal error: %v", errMarshal)
	}
	if errWrite := os.WriteFile(filePath, raw, 0o600); errWrite != nil {
		t.Fatalf("write error: %v", errWrite)
	}

	newRecord := &coreauth.Auth{
		ID:       fileName,
		FileName: fileName,
		Provider: "demo",
		Metadata: map[string]any{
			"type":         "demo",
			"email":        "user@example.com",
			"access_token": "new-token",
			"account_id":   "new-account",
		},
	}

	store := NewFileTokenStore()
	store.SetBaseDir(authDir)

	auth := &dummyAuthenticator{
		provider: "demo",
		record:   newRecord,
	}

	mgr := NewManager(store, auth)
	cfg := &config.Config{
		AuthDir: authDir,
	}

	_, savedPath, errLogin := mgr.Login(context.Background(), "demo", cfg, nil)
	if errLogin != nil {
		t.Fatalf("Login error: %v", errLogin)
	}
	if savedPath != filePath {
		t.Fatalf("savedPath = %s, want %s", savedPath, filePath)
	}

	savedRaw, errRead := os.ReadFile(filePath)
	if errRead != nil {
		t.Fatalf("ReadFile error: %v", errRead)
	}
	var saved map[string]any
	if errUnmarshal := json.Unmarshal(savedRaw, &saved); errUnmarshal != nil {
		t.Fatalf("Unmarshal error: %v", errUnmarshal)
	}

	if saved["access_token"] != "new-token" {
		t.Errorf("access_token = %v, want new-token", saved["access_token"])
	}
	if saved["account_id"] != "new-account" {
		t.Errorf("account_id = %v, want new-account", saved["account_id"])
	}
	if saved["prefix"] != "my-prefix" {
		t.Errorf("prefix = %v, want my-prefix", saved["prefix"])
	}
	if saved["websockets"] != false {
		t.Errorf("websockets = %v, want false", saved["websockets"])
	}
	if saved["note"] != "important note" {
		t.Errorf("note = %v, want important note", saved["note"])
	}
	if saved["weight"] != float64(10) {
		t.Errorf("weight = %v, want 10", saved["weight"])
	}
}

func TestManagerLoginRejectsAuthPathEscapesBeforeReadingMetadata(t *testing.T) {
	authDir := t.TempDir()
	outsideDir := t.TempDir()
	outsidePath := filepath.Join(outsideDir, "outside.json")
	if errWrite := os.WriteFile(outsidePath, []byte(`{"note":"outside"}`), 0o600); errWrite != nil {
		t.Fatalf("write outside auth file: %v", errWrite)
	}
	linkDir := filepath.Join(authDir, "link")
	if errLink := os.Symlink(outsideDir, linkDir); errLink != nil {
		t.Skipf("symlink unavailable: %v", errLink)
	}
	linkFile := filepath.Join(authDir, "linked.json")
	if errLink := os.Symlink(outsidePath, linkFile); errLink != nil {
		t.Skipf("file symlink unavailable: %v", errLink)
	}
	relativeOutsidePath, errRel := filepath.Rel(authDir, outsidePath)
	if errRel != nil {
		t.Fatalf("resolve relative outside path: %v", errRel)
	}

	tests := []struct {
		name     string
		fileName string
	}{
		{name: "parent traversal", fileName: relativeOutsidePath},
		{name: "absolute outside", fileName: outsidePath},
		{name: "symlink parent", fileName: filepath.Join("link", "outside.json")},
		{name: "symlink target", fileName: "linked.json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &recordingStore{}
			record := &coreauth.Auth{
				ID:       tt.fileName,
				FileName: tt.fileName,
				Provider: "demo",
				Metadata: map[string]any{"type": "demo"},
			}
			mgr := NewManager(store, &dummyAuthenticator{provider: "demo", record: record})

			if _, _, errLogin := mgr.Login(context.Background(), "demo", &config.Config{AuthDir: authDir}, nil); errLogin == nil {
				t.Fatal("Login() accepted an auth path outside AuthDir")
			}
			if store.saved {
				t.Fatal("Login() persisted a record after rejecting its auth path")
			}
			if _, ok := record.Metadata["note"]; ok {
				t.Fatal("Login() merged metadata read from outside AuthDir")
			}
		})
	}
}

func TestManagerLoginAllowsAbsolutePathInsideAuthDir(t *testing.T) {
	authDir := t.TempDir()
	filePath := filepath.Join(authDir, "inside.json")
	if errWrite := os.WriteFile(filePath, []byte(`{"note":"preserved"}`), 0o600); errWrite != nil {
		t.Fatalf("write existing auth file: %v", errWrite)
	}
	store := &recordingStore{}
	record := &coreauth.Auth{
		ID:       filePath,
		FileName: filePath,
		Provider: "demo",
		Metadata: map[string]any{"type": "demo"},
	}
	mgr := NewManager(store, &dummyAuthenticator{provider: "demo", record: record})

	if _, _, errLogin := mgr.Login(context.Background(), "demo", &config.Config{AuthDir: authDir}, nil); errLogin != nil {
		t.Fatalf("Login() error = %v", errLogin)
	}
	if !store.saved {
		t.Fatal("Login() did not persist a legal absolute auth path")
	}
	if got := record.Metadata["note"]; got != "preserved" {
		t.Fatalf("note = %v, want preserved", got)
	}
}
