package cluster

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"sync"
	"testing"
	"time"
)

var registerSlowSuccessDriver sync.Once
var slowSuccessControls sync.Map

type slowSuccessControl struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type slowSuccessDriver struct{}

type slowSuccessConn struct {
	control *slowSuccessControl
}

type slowSuccessRows struct {
	returned bool
}

func (slowSuccessDriver) Open(name string) (driver.Conn, error) {
	control, _ := slowSuccessControls.Load(name)
	return &slowSuccessConn{control: control.(*slowSuccessControl)}, nil
}

func (*slowSuccessConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (*slowSuccessConn) Close() error { return nil }

func (*slowSuccessConn) Begin() (driver.Tx, error) { return nil, driver.ErrSkip }

func (c *slowSuccessConn) ExecContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	if err := c.delayFirstOperation(ctx); err != nil {
		return nil, err
	}
	return driver.RowsAffected(1), nil
}

func (c *slowSuccessConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	if err := c.delayFirstOperation(ctx); err != nil {
		return nil, err
	}
	return &slowSuccessRows{}, nil
}

func (c *slowSuccessConn) delayFirstOperation(ctx context.Context) error {
	wait := false
	c.control.once.Do(func() {
		wait = true
		close(c.control.started)
	})
	if !wait {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.control.release:
		return nil
	}
}

func (*slowSuccessRows) Columns() []string {
	return []string{"node_id", "weight", "endpoint"}
}

func (*slowSuccessRows) Close() error { return nil }

func (r *slowSuccessRows) Next(dest []driver.Value) error {
	if r.returned {
		return io.EOF
	}
	r.returned = true
	dest[0] = "sj"
	dest[1] = int64(100)
	dest[2] = "https://sj.example.test"
	return nil
}

func TestRegistrarHeartbeatReportsDatabaseOperationStart(t *testing.T) {
	db, control := openSlowSuccessDB(t)
	startedAt := time.Now()
	completedAt := startedAt.Add(time.Hour)
	timeSource := func() time.Time {
		select {
		case <-control.release:
			return completedAt
		default:
			return startedAt
		}
	}
	reported := make(chan time.Time, 1)
	registrar, errRegistrar := NewRegistrar(RegistrarConfig{
		DB:                db,
		NodeID:            "sj",
		Endpoint:          "https://sj.example.test",
		Interval:          time.Hour,
		TimeSource:        timeSource,
		OnActiveHeartbeat: func(at time.Time) { reported <- at },
	})
	if errRegistrar != nil {
		t.Fatalf("NewRegistrar(): %v", errRegistrar)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		registrar.Run(ctx)
	}()
	waitForSlowOperation(t, control)
	close(control.release)
	select {
	case got := <-reported:
		if !got.Equal(startedAt) {
			t.Fatalf("heartbeat freshness time = %v, want operation start %v (completion %v)", got, startedAt, completedAt)
		}
	case <-time.After(time.Second):
		t.Fatal("successful heartbeat did not report freshness")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("registrar did not stop after cancellation")
	}
}

func TestRingWatcherRefreshReportsDatabaseOperationStart(t *testing.T) {
	db, control := openSlowSuccessDB(t)
	startedAt := time.Now()
	completedAt := startedAt.Add(time.Hour)
	timeSource := func() time.Time {
		select {
		case <-control.release:
			return completedAt
		default:
			return startedAt
		}
	}

	var reportedAt time.Time
	watcher := &RingWatcher{
		DB:         db,
		Ring:       NewAuthRing("sj"),
		TimeSource: timeSource,
		OnRefresh:  func(at time.Time) { reportedAt = at },
	}
	refreshDone := make(chan error, 1)
	go func() { refreshDone <- watcher.refresh(context.Background(), defaultRingStaleness) }()
	waitForSlowOperation(t, control)
	close(control.release)
	select {
	case errRefresh := <-refreshDone:
		if errRefresh != nil {
			t.Fatalf("refresh(): %v", errRefresh)
		}
	case <-time.After(time.Second):
		t.Fatal("ring refresh did not finish")
	}
	if !reportedAt.Equal(startedAt) {
		t.Fatalf("ring freshness time = %v, want query start %v (completion %v)", reportedAt, startedAt, completedAt)
	}
}

func openSlowSuccessDB(t *testing.T) (*sql.DB, *slowSuccessControl) {
	t.Helper()
	registerSlowSuccessDriver.Do(func() {
		sql.Register("cluster-slow-success-test", slowSuccessDriver{})
	})
	name := t.Name()
	control := &slowSuccessControl{started: make(chan struct{}), release: make(chan struct{})}
	slowSuccessControls.Store(name, control)
	t.Cleanup(func() { slowSuccessControls.Delete(name) })
	db, errOpen := sql.Open("cluster-slow-success-test", name)
	if errOpen != nil {
		t.Fatalf("sql.Open(): %v", errOpen)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, control
}

func waitForSlowOperation(t *testing.T, control *slowSuccessControl) {
	t.Helper()
	select {
	case <-control.started:
	case <-time.After(time.Second):
		t.Fatal("database operation did not start")
	}
}

var _ driver.ExecerContext = (*slowSuccessConn)(nil)
var _ driver.QueryerContext = (*slowSuccessConn)(nil)
