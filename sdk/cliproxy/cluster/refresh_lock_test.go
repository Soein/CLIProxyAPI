package cluster

import (
	"context"
	"errors"
	"testing"
)

func TestAuthIDLockKeys_Deterministic(t *testing.T) {
	c1, i1 := authIDLockKeys("codex-001@example.com")
	c2, i2 := authIDLockKeys("codex-001@example.com")
	if c1 != c2 || i1 != i2 {
		t.Fatalf("expected deterministic keys, got (%d,%d) vs (%d,%d)", c1, i1, c2, i2)
	}
	if c1 != authRefreshLockClass {
		t.Fatalf("class must be authRefreshLockClass=%d, got %d", authRefreshLockClass, c1)
	}
}

func TestAuthIDLockKeys_Distinct(t *testing.T) {
	_, a := authIDLockKeys("codex-001")
	_, b := authIDLockKeys("codex-002")
	if a == b {
		t.Fatalf("expected distinct id keys, both were %d", a)
	}
}

// Per review feedback, nil locker must surface an explicit error rather than
// silently pretending a lock was acquired — otherwise a misconfigured cluster
// would race with no protection at all.
func TestPgAuthRefreshLocker_NilReceiver_ReturnsError(t *testing.T) {
	var l *PgAuthRefreshLocker
	release, ok, err := l.TryLock(context.Background(), "any")
	if !errors.Is(err, ErrNilDB) {
		t.Fatalf("expected ErrNilDB, got %v", err)
	}
	if ok {
		t.Fatal("nil locker must not report ok=true")
	}
	if release != nil {
		t.Fatal("nil locker must not return a release func")
	}
}

func TestPgAuthRefreshLocker_ZeroValue_ReturnsError(t *testing.T) {
	l := &PgAuthRefreshLocker{}
	_, ok, err := l.TryLock(context.Background(), "any")
	if !errors.Is(err, ErrNilDB) {
		t.Fatalf("expected ErrNilDB, got %v", err)
	}
	if ok {
		t.Fatal("zero-value locker must not report ok=true")
	}
}
