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
	"time"
)

const leaderDriverName = "cliproxy-leader-test"

var (
	registerLeaderDriver sync.Once
	leaderDriverStates   sync.Map
)

type leaderDriver struct{}

type leaderDriverState struct {
	mu           sync.Mutex
	acquireCount int
	probeCount   int
	closeCount   int
	probeErr     error
	cancel       context.CancelFunc
}

type leaderConn struct {
	state *leaderDriverState
}

type leaderRows struct {
	value driver.Value
	done  bool
}

func (leaderDriver) Open(name string) (driver.Conn, error) {
	value, ok := leaderDriverStates.Load(name)
	if !ok {
		return nil, errors.New("missing leader driver state")
	}
	return &leaderConn{state: value.(*leaderDriverState)}, nil
}

func (*leaderConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (*leaderConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (c *leaderConn) Close() error {
	c.state.mu.Lock()
	c.state.closeCount++
	c.state.mu.Unlock()
	return nil
}

func (c *leaderConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if strings.Contains(query, "pg_try_advisory_lock") {
		c.state.acquireCount++
		if c.state.acquireCount > 1 && c.state.probeErr != nil {
			if c.state.cancel != nil {
				c.state.cancel()
			}
			return nil, c.state.probeErr
		}
		return &leaderRows{value: true}, nil
	}
	if strings.TrimSpace(query) == "SELECT 1" {
		c.state.probeCount++
		if c.state.probeErr != nil {
			if c.state.cancel != nil {
				c.state.cancel()
			}
			return nil, c.state.probeErr
		}
		return &leaderRows{value: int64(1)}, nil
	}
	return nil, errors.New("unexpected leader query")
}

func (*leaderConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

func (*leaderRows) Columns() []string { return []string{"value"} }
func (*leaderRows) Close() error      { return nil }
func (r *leaderRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = r.value
	return nil
}

func newLeaderTestDB(t *testing.T, state *leaderDriverState) *sql.DB {
	t.Helper()
	registerLeaderDriver.Do(func() { sql.Register(leaderDriverName, leaderDriver{}) })
	name := strings.ReplaceAll(t.Name(), "/", "-")
	leaderDriverStates.Store(name, state)
	t.Cleanup(func() { leaderDriverStates.Delete(name) })
	db, errOpen := sql.Open(leaderDriverName, name)
	if errOpen != nil {
		t.Fatalf("sql.Open() error: %v", errOpen)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestLeaderElectorRunDoesNotReenterLockAndDiscardsOnExit(t *testing.T) {
	state := &leaderDriverState{}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer cancel()
	elector := New(Config{DB: newLeaderTestDB(t, state), NodeID: "node", Interval: 5 * time.Millisecond, OnLoss: func() {}})
	if errRun := elector.Run(ctx); !errors.Is(errRun, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want deadline", errRun)
	}
	state.mu.Lock()
	acquireCount, probeCount, closeCount := state.acquireCount, state.probeCount, state.closeCount
	state.mu.Unlock()
	if acquireCount != 1 || probeCount == 0 || closeCount != 1 {
		t.Fatalf("acquire/probe/discard = %d/%d/%d, want 1/>0/1", acquireCount, probeCount, closeCount)
	}
}

func TestLeaderElectorCanceledProbeDiscardsWithoutOnLoss(t *testing.T) {
	probeErr := errors.New("leader session probe failed")
	ctx, cancel := context.WithCancel(context.Background())
	state := &leaderDriverState{probeErr: probeErr, cancel: cancel}
	onLoss := make(chan struct{}, 1)
	elector := New(Config{DB: newLeaderTestDB(t, state), NodeID: "node", Interval: time.Millisecond, OnLoss: func() { onLoss <- struct{}{} }})
	if errRun := elector.Run(ctx); !errors.Is(errRun, context.Canceled) {
		t.Fatalf("Run() error = %v, want canceled after probe failure", errRun)
	}
	state.mu.Lock()
	acquireCount, probeCount, closeCount := state.acquireCount, state.probeCount, state.closeCount
	state.mu.Unlock()
	if acquireCount != 1 || probeCount != 1 || closeCount != 1 {
		t.Fatalf("acquire/probe/discard = %d/%d/%d, want 1/1/1", acquireCount, probeCount, closeCount)
	}
	select {
	case <-onLoss:
		t.Fatal("normal context cancellation invoked OnLoss")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestLeaderElectorProbeErrorDiscardsSession(t *testing.T) {
	state := &leaderDriverState{probeErr: errors.New("leader session probe failed")}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	elector := New(Config{DB: newLeaderTestDB(t, state), NodeID: "node", Interval: time.Millisecond, OnLoss: func() {}})
	if errRun := elector.Run(ctx); !errors.Is(errRun, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want deadline after probe retries", errRun)
	}
	state.mu.Lock()
	probeCount, closeCount := state.probeCount, state.closeCount
	state.mu.Unlock()
	if probeCount == 0 || closeCount == 0 {
		t.Fatalf("probe/discard = %d/%d, want both positive", probeCount, closeCount)
	}
}
