package authfilelock

import (
	"path/filepath"
	"testing"
	"time"
)

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
