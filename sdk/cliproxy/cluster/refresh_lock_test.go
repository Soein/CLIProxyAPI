package cluster

import "testing"

func TestAuthIDLockKey_Deterministic(t *testing.T) {
	a := authIDLockKey("codex-001@example.com")
	b := authIDLockKey("codex-001@example.com")
	if a != b {
		t.Fatalf("expected deterministic key, got %d vs %d", a, b)
	}
}

func TestAuthIDLockKey_Distinct(t *testing.T) {
	a := authIDLockKey("codex-001")
	b := authIDLockKey("codex-002")
	if a == b {
		t.Fatalf("expected distinct keys for distinct ids, both were %d", a)
	}
}

func TestPgAuthRefreshLocker_NilDB_NoOp(t *testing.T) {
	// Zero-value locker (no DB) should succeed and return a harmless release,
	// so single-instance callers need not branch on the field.
	var l *PgAuthRefreshLocker
	release, ok, err := l.TryLock(nil, "any")
	if err != nil {
		t.Fatalf("nil locker should not error: %v", err)
	}
	if !ok {
		t.Fatal("nil locker should succeed")
	}
	release() // must not panic
}
