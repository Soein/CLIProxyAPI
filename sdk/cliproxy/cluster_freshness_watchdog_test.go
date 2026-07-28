package cliproxy

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/cluster"
)

var registerFreshnessWatchdogDriver sync.Once
var freshnessWatchdogControls sync.Map

type freshnessWatchdogControl struct {
	probes atomic.Int32
}

type freshnessWatchdogDriver struct{}

type freshnessWatchdogConn struct {
	control *freshnessWatchdogControl
}

type freshnessWatchdogRows struct {
	returned bool
}

func (freshnessWatchdogDriver) Open(name string) (driver.Conn, error) {
	control, _ := freshnessWatchdogControls.Load(name)
	return &freshnessWatchdogConn{control: control.(*freshnessWatchdogControl)}, nil
}

func (*freshnessWatchdogConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (*freshnessWatchdogConn) Close() error { return nil }

func (*freshnessWatchdogConn) Begin() (driver.Tx, error) { return nil, driver.ErrSkip }

func (c *freshnessWatchdogConn) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(query) != "SELECT 1" {
		return nil, errors.New("freshness watchdog test only supports SELECT 1")
	}
	c.control.probes.Add(1)
	return &freshnessWatchdogRows{}, nil
}

func (*freshnessWatchdogRows) Columns() []string { return []string{"one"} }

func (*freshnessWatchdogRows) Close() error { return nil }

func (r *freshnessWatchdogRows) Next(dest []driver.Value) error {
	if r.returned {
		return io.EOF
	}
	r.returned = true
	dest[0] = int64(1)
	return nil
}

func TestClusterFreshnessWatchdogFailsClosedWhileLeaseProbeSucceeds(t *testing.T) {
	tests := []struct {
		name       string
		wantError  string
		setBudgets func(*clusterFreshnessBudgets)
		keepFresh  func(*clusterFreshness)
	}{
		{
			name:      "registrar heartbeat expires",
			wantError: "registrar heartbeat freshness expired",
			setBudgets: func(b *clusterFreshnessBudgets) {
				b.heartbeat = 30 * time.Millisecond
				b.ringRefresh = time.Second
			},
			keepFresh: func(f *clusterFreshness) { f.recordRingRefreshAt(time.Now()) },
		},
		{
			name:      "ring refresh expires",
			wantError: "ring refresh freshness expired",
			setBudgets: func(b *clusterFreshnessBudgets) {
				b.heartbeat = time.Second
				b.ringRefresh = 30 * time.Millisecond
			},
			keepFresh: func(f *clusterFreshness) { f.recordHeartbeatAt(time.Now()) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, control := openFreshnessWatchdogDB(t)
			conn, errConn := db.Conn(context.Background())
			if errConn != nil {
				t.Fatalf("db.Conn(): %v", errConn)
			}

			ring := cluster.NewAuthRing("node-a")
			ring.Rebuild([]cluster.RingMember{{NodeID: "node-a", Weight: 100}})
			service := &Service{
				clusterNodeLease: &clusterNodeLease{conn: conn, nodeID: "node-a"},
				clusterAuthRing:  ring,
				clusterErr:       make(chan error, 1),
			}
			freshness := newClusterFreshness(time.Now())
			budgets := clusterFreshnessBudgets{}
			tt.setBudgets(&budgets)
			tt.keepFresh(freshness)

			clusterCtx, cancelCluster := context.WithCancel(context.Background())
			started := time.Now()
			service.startClusterNodeLeaseProbe(context.Background(), clusterCtx, cancelCluster, 5*time.Millisecond, freshness, budgets)
			t.Cleanup(func() {
				service.stopClusterNodeLeaseProbe()
				_ = conn.Close()
				_ = db.Close()
			})

			select {
			case errFatal := <-service.clusterErr:
				if !strings.Contains(errFatal.Error(), tt.wantError) {
					t.Fatalf("fatal error = %v, want %q", errFatal, tt.wantError)
				}
			case <-time.After(time.Second):
				t.Fatal("freshness expiry did not reach the fatal cluster path")
			}
			if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
				t.Fatalf("fatal detection took %s, want before simulated 500ms ring-staleness", elapsed)
			}
			if got := control.probes.Load(); got < 2 {
				t.Fatalf("successful lease SELECT 1 probes = %d, want continuing successes before freshness expiry", got)
			}
			if ring.Ready() {
				t.Fatal("freshness expiry did not terminally fail the auth ring closed")
			}
		})
	}
}

func TestClusterFreshnessTimingToleratesOneMissAndRejectsUnsafeWindow(t *testing.T) {
	registrarInterval := 10 * time.Second
	probeInterval := 5 * time.Second
	budgets, errTiming := clusterFreshnessTiming(registrarInterval, probeInterval, 30*time.Second)
	if errTiming != nil {
		t.Fatalf("default timing rejected: %v", errTiming)
	}

	started := time.Now()
	freshness := newClusterFreshness(started)
	// One missed heartbeat is followed by the next success at 2R; one missed
	// ring poll is followed by success at 2P. Both remain inside their budgets.
	freshness.recordRingRefreshAt(started.Add(2 * probeInterval))
	if errFreshness := freshness.check(started.Add(2*registrarInterval), budgets); errFreshness != nil {
		t.Fatalf("single heartbeat miss was not tolerated: %v", errFreshness)
	}
	freshness.recordHeartbeatAt(started.Add(2 * registrarInterval))
	if errFreshness := freshness.check(started.Add(5*probeInterval-time.Nanosecond), budgets); errFreshness != nil {
		t.Fatalf("single ring refresh miss was not tolerated: %v", errFreshness)
	}

	if _, errUnsafe := clusterFreshnessTiming(registrarInterval, probeInterval, 27500*time.Millisecond); errUnsafe == nil {
		t.Fatal("heartbeat observation equal to ring staleness was accepted")
	}
	if _, errUnsafe := clusterFreshnessTiming(time.Second, probeInterval, 20*time.Second); errUnsafe == nil {
		t.Fatal("ring observation equal to ring staleness was accepted")
	}
	if got := clusterNodeLeaseProbeInterval(10*time.Second, 20*time.Second); got != 4*time.Second {
		t.Fatalf("lease probe interval = %s, want ring-staleness/5 safety margin", got)
	}
}

func TestClusterFreshnessExpiresFromDatabaseOperationStart(t *testing.T) {
	now := time.Now()
	freshness := newClusterFreshness(now)
	freshness.recordHeartbeatAt(now.Add(-40 * time.Millisecond))
	budgets := clusterFreshnessBudgets{
		heartbeat:   30 * time.Millisecond,
		ringRefresh: time.Second,
	}
	if errFreshness := freshness.check(now, budgets); errFreshness == nil || !strings.Contains(errFreshness.Error(), "heartbeat freshness expired") {
		t.Fatalf("freshness check = %v, want expiry based on pre-write timestamp", errFreshness)
	}
}

func TestClusterFreshnessWatchdogCancellationIsNonfatal(t *testing.T) {
	db, control := openFreshnessWatchdogDB(t)
	conn, errConn := db.Conn(context.Background())
	if errConn != nil {
		t.Fatalf("db.Conn(): %v", errConn)
	}
	service := &Service{
		clusterNodeLease: &clusterNodeLease{conn: conn, nodeID: "node-a"},
		clusterErr:       make(chan error, 1),
	}
	freshness := newClusterFreshness(time.Now())
	budgets := clusterFreshnessBudgets{heartbeat: time.Second, ringRefresh: time.Second}
	clusterCtx, cancelCluster := context.WithCancel(context.Background())
	service.startClusterNodeLeaseProbe(context.Background(), clusterCtx, cancelCluster, 5*time.Millisecond, freshness, budgets)
	t.Cleanup(func() {
		service.stopClusterNodeLeaseProbe()
		_ = conn.Close()
		_ = db.Close()
	})

	deadline := time.Now().Add(time.Second)
	for control.probes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if control.probes.Load() == 0 {
		t.Fatal("lease watchdog did not start")
	}
	cancelCluster()
	service.stopClusterNodeLeaseProbe()
	select {
	case errFatal := <-service.clusterErr:
		t.Fatalf("normal cancellation reported fatal cluster error: %v", errFatal)
	default:
	}
}

func TestClusterRunContextCancellationFailsRingClosedBeforeLeaseProbeStops(t *testing.T) {
	db, _ := openFreshnessWatchdogDB(t)
	conn, errConn := db.Conn(context.Background())
	if errConn != nil {
		t.Fatalf("db.Conn(): %v", errConn)
	}
	ring := cluster.NewAuthRing("node-a")
	ring.Rebuild([]cluster.RingMember{{NodeID: "node-a", Weight: 100}})
	service := &Service{
		clusterNodeLease: &clusterNodeLease{conn: conn, nodeID: "node-a"},
		clusterAuthRing:  ring,
		clusterErr:       make(chan error, 1),
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	clusterCtx, cancelCluster := context.WithCancel(context.Background())
	service.startClusterNodeLeaseProbe(
		runCtx,
		clusterCtx,
		cancelCluster,
		time.Second,
		newClusterFreshness(time.Now()),
		clusterFreshnessBudgets{heartbeat: time.Second, ringRefresh: time.Second},
	)
	t.Cleanup(func() {
		cancelRun()
		cancelCluster()
		service.stopClusterNodeLeaseProbe()
		_ = conn.Close()
		_ = db.Close()
	})

	cancelRun()
	service.stopClusterNodeLeaseProbe()
	if ring.Ready() {
		t.Fatal("run context cancellation stopped lease monitoring before failing the auth ring closed")
	}
}

func openFreshnessWatchdogDB(t *testing.T) (*sql.DB, *freshnessWatchdogControl) {
	t.Helper()
	registerFreshnessWatchdogDriver.Do(func() {
		sql.Register("cliproxy-freshness-watchdog-test", freshnessWatchdogDriver{})
	})
	name := t.Name()
	control := &freshnessWatchdogControl{}
	freshnessWatchdogControls.Store(name, control)
	t.Cleanup(func() { freshnessWatchdogControls.Delete(name) })
	db, errOpen := sql.Open("cliproxy-freshness-watchdog-test", name)
	if errOpen != nil {
		t.Fatalf("sql.Open(): %v", errOpen)
	}
	return db, control
}

var _ driver.QueryerContext = (*freshnessWatchdogConn)(nil)
