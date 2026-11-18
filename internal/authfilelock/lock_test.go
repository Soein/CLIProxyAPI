package authfilelock

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadCommittedFileDefersAnotherSymlinkToPendingTarget(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.json")
	uploadPath := filepath.Join(dir, "upload.json")
	observerPath := filepath.Join(dir, "observer.json")
	if errWrite := os.WriteFile(source, []byte(`{"type":"vendor"}`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	for _, alias := range []string{uploadPath, observerPath} {
		if errLink := os.Symlink(source, alias); errLink != nil {
			t.Skipf("symlinks unavailable: %v", errLink)
		}
	}
	resolved, errResolve := filepath.EvalSymlinks(source)
	if errResolve != nil {
		t.Fatal(errResolve)
	}
	finishUpload := BeginUpload(uploadPath, resolved)
	defer finishUpload()

	data, pending, errRead := ReadCommittedFile(observerPath, os.ReadFile)
	if data != nil || pending == nil || errRead != nil {
		t.Fatalf("observer received %d bytes, pending = %t, error = %v", len(data), pending != nil, errRead)
	}
}

func TestLockSerializesCanonicalPathAliases(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "source.json")
	alias := filepath.Join(dir, "nested", "..", "source.json")

	unlockFirst := Lock(path)
	acquired := make(chan struct{})
	done := make(chan struct{})
	go func() {
		unlockSecond := Lock(alias)
		close(acquired)
		unlockSecond()
		close(done)
	}()

	select {
	case <-acquired:
		t.Fatal("canonical path alias acquired before the first lock was released")
	case <-time.After(50 * time.Millisecond):
	}
	unlockFirst()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canonical path alias did not acquire after release")
	}
}
