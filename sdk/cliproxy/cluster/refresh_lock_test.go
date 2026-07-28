package cluster

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

const refreshLockDriverName = "cliproxy-refresh-lock-test"

var (
	registerRefreshLockDriver sync.Once
	refreshLockDriverStates   sync.Map
)

type refreshLockDriver struct{}

type refreshLockDriverState struct {
	mu          sync.Mutex
	acquireGot  bool
	acquireErr  error
	unlockGot   bool
	unlockErr   error
	closeCount  int
	unlockCount int
}

type refreshLockConn struct {
	state *refreshLockDriverState
}

type refreshLockRows struct {
	value bool
	done  bool
}

func (refreshLockDriver) Open(name string) (driver.Conn, error) {
	value, ok := refreshLockDriverStates.Load(name)
	if !ok {
		return nil, errors.New("missing refresh lock driver state")
	}
	return &refreshLockConn{state: value.(*refreshLockDriverState)}, nil
}

func (*refreshLockConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (*refreshLockConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (c *refreshLockConn) Close() error {
	c.state.mu.Lock()
	c.state.closeCount++
	c.state.mu.Unlock()
	return nil
}

func (c *refreshLockConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if strings.Contains(query, "pg_try_advisory_lock") {
		if c.state.acquireErr != nil {
			return nil, c.state.acquireErr
		}
		return &refreshLockRows{value: c.state.acquireGot}, nil
	}
	if strings.Contains(query, "pg_advisory_unlock") {
		c.state.unlockCount++
		if c.state.unlockErr != nil {
			return nil, c.state.unlockErr
		}
		return &refreshLockRows{value: c.state.unlockGot}, nil
	}
	return nil, errors.New("unexpected refresh lock query")
}

func (c *refreshLockConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if strings.Contains(query, "pg_advisory_unlock") {
		c.state.unlockCount++
		return driver.RowsAffected(1), c.state.unlockErr
	}
	return nil, errors.New("unexpected refresh lock exec")
}

func (*refreshLockRows) Columns() []string { return []string{"locked"} }
func (*refreshLockRows) Close() error      { return nil }
func (r *refreshLockRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = r.value
	return nil
}

func newRefreshLockTestDB(t *testing.T, state *refreshLockDriverState) *sql.DB {
	t.Helper()
	registerRefreshLockDriver.Do(func() { sql.Register(refreshLockDriverName, refreshLockDriver{}) })
	name := strings.ReplaceAll(t.Name(), "/", "-")
	refreshLockDriverStates.Store(name, state)
	t.Cleanup(func() { refreshLockDriverStates.Delete(name) })
	db, errOpen := sql.Open(refreshLockDriverName, name)
	if errOpen != nil {
		t.Fatalf("sql.Open() error: %v", errOpen)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

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

func TestPgAuthRefreshLocker_AcquireUnknownDiscardsConnection(t *testing.T) {
	queryErr := errors.New("acquire acknowledgement lost")
	state := &refreshLockDriverState{acquireErr: queryErr}
	locker := NewPgAuthRefreshLocker(newRefreshLockTestDB(t, state))
	if _, ok, errLock := locker.TryLock(context.Background(), "auth"); ok || !errors.Is(errLock, queryErr) {
		t.Fatalf("TryLock() = (%v, %v), want false/query error", ok, errLock)
	}
	state.mu.Lock()
	closeCount := state.closeCount
	state.mu.Unlock()
	if closeCount != 1 {
		t.Fatalf("discarded driver connections = %d, want 1", closeCount)
	}
}

func TestPgAuthRefreshLocker_UnlockFailureDiscardsConnection(t *testing.T) {
	tests := []struct {
		name      string
		unlockGot bool
		unlockErr error
	}{
		{name: "query-error", unlockErr: errors.New("unlock acknowledgement lost")},
		{name: "server-returned-false"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := &refreshLockDriverState{acquireGot: true, unlockGot: tc.unlockGot, unlockErr: tc.unlockErr}
			locker := NewPgAuthRefreshLocker(newRefreshLockTestDB(t, state))
			release, ok, errLock := locker.TryLock(context.Background(), "auth")
			if errLock != nil || !ok || release == nil {
				t.Fatalf("TryLock() = (release:%v, %v, %v), want true/true/nil", release != nil, ok, errLock)
			}
			release()
			state.mu.Lock()
			closeCount := state.closeCount
			unlockCount := state.unlockCount
			state.mu.Unlock()
			if unlockCount != 1 || closeCount != 1 {
				t.Fatalf("unlock/discard counts = %d/%d, want 1/1", unlockCount, closeCount)
			}
		})
	}
}
