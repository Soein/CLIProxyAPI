package auth

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestFileTokenStoreCodexRejectsParentSymlinkSwap(t *testing.T) {
	baseDir := t.TempDir()
	outsideDir := t.TempDir()
	targetParent := filepath.Join(baseDir, "nested")
	if errMkdir := os.Mkdir(targetParent, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	storage := &swappingPayloadStorage{parent: targetParent, outsideDir: outsideDir}
	store := NewFileTokenStore()
	store.SetBaseDir(baseDir)
	auth := &cliproxyauth.Auth{
		ID: filepath.Join("nested", "auth.json"), Provider: "codex", Storage: storage,
	}
	if _, errSave := store.Save(context.Background(), auth); errSave == nil {
		t.Fatal("Save accepted an outside symlink installed during token serialization")
	}
	if storage.legacySaveUsed {
		t.Fatal("Save bypassed rooted IO through the legacy storage callback")
	}
	entries, errRead := os.ReadDir(outsideDir)
	if errRead != nil {
		t.Fatal(errRead)
	}
	if len(entries) != 0 {
		t.Fatalf("credential or lockfile escaped AuthDir: %v", entries)
	}
}

func TestFileTokenStoreCodexRejectsLockfileSymlinkEscape(t *testing.T) {
	baseDir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.lock")
	if errWrite := os.WriteFile(outside, []byte("unchanged"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errLink := os.Symlink(outside, filepath.Join(baseDir, "codex.json.lock")); errLink != nil {
		t.Skipf("symlink unavailable: %v", errLink)
	}
	store := NewFileTokenStore()
	store.SetBaseDir(baseDir)
	if _, errSave := store.Save(context.Background(), codexRefreshFixture()); errSave == nil {
		t.Fatal("Save accepted a lockfile pointing outside AuthDir")
	}
	if _, errStat := os.Stat(filepath.Join(baseDir, "codex.json")); !os.IsNotExist(errStat) {
		t.Fatalf("credential was created despite lock failure: %v", errStat)
	}
	data, errRead := os.ReadFile(outside)
	if errRead != nil || string(data) != "unchanged" {
		t.Fatalf("outside lockfile changed: %q, %v", data, errRead)
	}
}
