package cluster

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var registerRingWatcherDriver sync.Once
var ringWatcherLastQuery atomic.Value
var ringWatcherLastMembershipQuery atomic.Value
var ringWatcherQueryFailures atomic.Int32

type ringWatcherTestDriver struct{}

type ringWatcherTestConn struct{}

type ringWatcherTestRows struct {
	columns []string
	rows    [][]driver.Value
	index   int
}

type ringWatcherTestTx struct{}

var ringWatcherMembershipState = struct {
	sync.Mutex
	epoch       int64
	fingerprint []byte
	stalenessMS int64
}{stalenessMS: defaultRingStaleness.Milliseconds()}

func (ringWatcherTestDriver) Open(string) (driver.Conn, error) {
	return ringWatcherTestConn{}, nil
}

func (ringWatcherTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (ringWatcherTestConn) Close() error { return nil }

func (ringWatcherTestConn) Begin() (driver.Tx, error) { return ringWatcherTestTx{}, nil }

func (ringWatcherTestTx) Commit() error   { return nil }
func (ringWatcherTestTx) Rollback() error { return nil }

func (ringWatcherTestConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	ringWatcherLastQuery.Store(query)
	if strings.Contains(query, "UPDATE cluster_membership_state") {
		ringWatcherMembershipState.Lock()
		if strings.Contains(query, "SET staleness_ms") {
			ringWatcherMembershipState.stalenessMS = args[0].Value.(int64)
		} else {
			ringWatcherMembershipState.epoch = args[0].Value.(int64)
			ringWatcherMembershipState.fingerprint = slices.Clone(args[1].Value.([]byte))
		}
		ringWatcherMembershipState.Unlock()
	}
	return driver.RowsAffected(1), nil
}

func (ringWatcherTestConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	ringWatcherLastQuery.Store(query)
	for {
		remaining := ringWatcherQueryFailures.Load()
		if remaining <= 0 {
			break
		}
		if ringWatcherQueryFailures.CompareAndSwap(remaining, remaining-1) {
			return nil, errors.New("simulated refresh failure")
		}
	}
	if strings.Contains(query, "FROM cluster_membership_state") {
		ringWatcherMembershipState.Lock()
		defer ringWatcherMembershipState.Unlock()
		return &ringWatcherTestRows{
			columns: []string{"epoch", "fingerprint", "staleness_ms"},
			rows: [][]driver.Value{{
				ringWatcherMembershipState.epoch,
				slices.Clone(ringWatcherMembershipState.fingerprint),
				ringWatcherMembershipState.stalenessMS,
			}},
		}, nil
	}
	ringWatcherLastMembershipQuery.Store(query)
	return &ringWatcherTestRows{
		columns: []string{"node_id", "weight", "endpoint"},
		rows: [][]driver.Value{
			{"sj", int64(100), "https://sj.example.test"},
			{"la", int64(100), "  \t"},
		},
	}, nil
}

func (r *ringWatcherTestRows) Columns() []string {
	return r.columns
}

func (*ringWatcherTestRows) Close() error { return nil }

func (r *ringWatcherTestRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index++
	return nil
}

func TestMembersChanged_SameSetSameWeight(t *testing.T) {
	a := []RingMember{{"sj", 100}, {"la", 100}}
	b := []RingMember{{"la", 100}, {"sj", 100}} // order-insensitive
	if membersChanged(a, b) {
		t.Error("same set same weight should not count as changed")
	}
}

func TestMembersChanged_DifferentCount(t *testing.T) {
	a := []RingMember{{"sj", 100}, {"la", 100}}
	b := []RingMember{{"sj", 100}, {"la", 100}, {"fra", 100}}
	if !membersChanged(a, b) {
		t.Error("different count should count as changed")
	}
}

func TestMembersChanged_WeightOnly(t *testing.T) {
	a := []RingMember{{"sj", 100}}
	b := []RingMember{{"sj", 50}}
	// Weight delta MUST trigger a rebuild — operator bumping weight
	// should take effect within one poll cycle, not wait for membership
	// change.
	if !membersChanged(a, b) {
		t.Error("weight-only change must count as changed")
	}
}

func TestMembersChanged_NodeReplaced(t *testing.T) {
	a := []RingMember{{"sj", 100}, {"la", 100}}
	b := []RingMember{{"sj", 100}, {"fra", 100}}
	if !membersChanged(a, b) {
		t.Error("one node swapped should count as changed")
	}
}

func TestMembersChanged_BothEmpty(t *testing.T) {
	if membersChanged(nil, nil) {
		t.Error("nil/nil should not count as changed")
	}
}

// Run with nil DB must return cleanly (no panic, no hang). Regression
// guard against misconfig.
func TestRingWatcher_NilDB(t *testing.T) {
	w := &RingWatcher{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel
	w.Run(ctx)
}

func TestRingWatcher_NilReceiverSafe(t *testing.T) {
	var w *RingWatcher
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w.Run(ctx) // must not panic
}

func TestRingWatcher_ExcludesMembersWithBlankEndpoint(t *testing.T) {
	registerRingWatcherDriver.Do(func() {
		sql.Register("cluster-ring-watcher-test", ringWatcherTestDriver{})
	})
	db, err := sql.Open("cluster-ring-watcher-test", "")
	if err != nil {
		t.Fatalf("open test DB: %v", err)
	}
	defer db.Close()

	ring := NewAuthRing("sj")
	watcher := &RingWatcher{DB: db, Ring: ring, PollInterval: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		watcher.Run(ctx)
	}()

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for !ring.Ready() {
		select {
		case <-deadline.C:
			cancel()
			<-done
			t.Fatal("ring did not become ready after initial refresh")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	<-done

	members := ring.Members()
	if len(members) != 1 || members[0].NodeID != "sj" {
		t.Fatalf("blank endpoint member must be excluded, got %+v", members)
	}
	query, _ := ringWatcherLastMembershipQuery.Load().(string)
	if !strings.Contains(query, "NULLIF(BTRIM(endpoint), '') IS NOT NULL") {
		t.Fatalf("membership query must exclude blank endpoints, got %q", query)
	}
}

func TestRingWatcher_ReportsSuccessfulUnchangedRefresh(t *testing.T) {
	registerRingWatcherDriver.Do(func() {
		sql.Register("cluster-ring-watcher-test", ringWatcherTestDriver{})
	})
	db, err := sql.Open("cluster-ring-watcher-test", "")
	if err != nil {
		t.Fatalf("open test DB: %v", err)
	}
	defer db.Close()

	var refreshes atomic.Int32
	var changes atomic.Int32
	watcher := &RingWatcher{
		DB:        db,
		Ring:      NewAuthRing("sj"),
		OnRefresh: func(time.Time) { refreshes.Add(1) },
		OnChange:  func() { changes.Add(1) },
	}
	if errRefresh := watcher.refresh(context.Background(), defaultRingStaleness); errRefresh != nil {
		t.Fatalf("first refresh: %v", errRefresh)
	}
	if errRefresh := watcher.refresh(context.Background(), defaultRingStaleness); errRefresh != nil {
		t.Fatalf("unchanged refresh: %v", errRefresh)
	}
	if got := refreshes.Load(); got != 2 {
		t.Fatalf("successful refresh callbacks = %d, want 2", got)
	}
	if got := changes.Load(); got != 1 {
		t.Fatalf("membership change callbacks = %d, want 1", got)
	}
}

func TestRingWatcher_HealthyQuietListenerResetsBackoffAfterSynchronizedRefresh(t *testing.T) {
	registerRingWatcherDriver.Do(func() {
		sql.Register("cluster-ring-watcher-test", ringWatcherTestDriver{})
	})
	db, err := sql.Open("cluster-ring-watcher-test", "")
	if err != nil {
		t.Fatalf("open test DB: %v", err)
	}
	defer db.Close()

	backoffs := []time.Duration{time.Second, 3 * time.Second, 10 * time.Second, 30 * time.Second}
	ringWatcherQueryFailures.Store(1)
	defer ringWatcherQueryFailures.Store(0)
	var listenCalls int
	var waits []time.Duration
	watcher := &RingWatcher{
		DB:       db,
		Ring:     NewAuthRing("sj"),
		backoffs: backoffs,
		listen: func(_ context.Context, _ chan<- struct{}, onListening func() error) error {
			listenCalls++
			if listenCalls == 1 {
				return errors.New("simulated connect failure")
			}
			if errListening := onListening(); errListening != nil {
				return errListening
			}
			return errors.New("simulated quiet listener disconnect")
		},
		wait: func(_ context.Context, delay time.Duration) bool {
			waits = append(waits, delay)
			return len(waits) < 3
		},
	}

	watcher.listenLoop(context.Background(), make(chan struct{}, 1), defaultRingStaleness)

	want := []time.Duration{time.Second, 3 * time.Second, time.Second}
	if !slices.Equal(waits, want) {
		t.Fatalf("retry waits = %v, want %v", waits, want)
	}
}

func TestDrain_EmptyChannel(t *testing.T) {
	ch := make(chan struct{}, 2)
	drain(ch) // must not block
	ch <- struct{}{}
	ch <- struct{}{}
	drain(ch)
	select {
	case <-ch:
		t.Error("drain should have emptied channel")
	default:
	}
}

func TestFormatMembers(t *testing.T) {
	got := formatMembers([]RingMember{{"sj", 100}, {"la", 50}})
	if got != "sj,la" {
		t.Errorf("unexpected format: %q", got)
	}
	if formatMembers(nil) != "" {
		t.Errorf("empty should produce empty string")
	}
}

func resetRingWatcherMembershipState(epoch int64, staleness time.Duration, fingerprint []byte) {
	ringWatcherMembershipState.Lock()
	defer ringWatcherMembershipState.Unlock()
	ringWatcherMembershipState.epoch = epoch
	ringWatcherMembershipState.stalenessMS = staleness.Milliseconds()
	ringWatcherMembershipState.fingerprint = slices.Clone(fingerprint)
}

func TestRingWatcherRefreshAdvancesDatabaseEpochOnlyForFingerprintChange(t *testing.T) {
	registerRingWatcherDriver.Do(func() {
		sql.Register("cluster-ring-watcher-test", ringWatcherTestDriver{})
	})
	resetRingWatcherMembershipState(7, defaultRingStaleness, nil)
	db, err := sql.Open("cluster-ring-watcher-test", "")
	if err != nil {
		t.Fatalf("open test DB: %v", err)
	}
	defer db.Close()

	ring := NewAuthRing("sj")
	watcher := &RingWatcher{DB: db, Ring: ring}
	if errRefresh := watcher.refresh(context.Background(), defaultRingStaleness); errRefresh != nil {
		t.Fatalf("changed refresh: %v", errRefresh)
	}
	if got := ring.Decision("auth-a").Epoch; got != 8 {
		t.Fatalf("changed fingerprint epoch = %d, want 8", got)
	}
	if errRefresh := watcher.refresh(context.Background(), defaultRingStaleness); errRefresh != nil {
		t.Fatalf("unchanged refresh: %v", errRefresh)
	}
	if got := ring.Decision("auth-a").Epoch; got != 8 {
		t.Fatalf("unchanged fingerprint epoch = %d, want 8", got)
	}
	query, _ := ringWatcherLastMembershipQuery.Load().(string)
	if !strings.Contains(query, "status IN ('active', 'joining')") {
		t.Fatalf("membership query must include active and joining members: %q", query)
	}
}

func TestRingWatcherStalenessMismatchFailsClosed(t *testing.T) {
	registerRingWatcherDriver.Do(func() {
		sql.Register("cluster-ring-watcher-test", ringWatcherTestDriver{})
	})
	resetRingWatcherMembershipState(23, 45*time.Second, nil)
	db, err := sql.Open("cluster-ring-watcher-test", "")
	if err != nil {
		t.Fatalf("open test DB: %v", err)
	}
	defer db.Close()

	ring := NewAuthRing("sj")
	ring.RebuildAt(22, []RingMember{{NodeID: "sj", Weight: 100}})
	watcher := &RingWatcher{DB: db, Ring: ring}
	err = watcher.refresh(context.Background(), defaultRingStaleness)
	if err == nil {
		t.Fatal("staleness mismatch must fail refresh")
	}
	decision := ring.Decision("auth-a")
	if decision.Ready || decision.Epoch != 23 {
		t.Fatalf("staleness mismatch decision = %+v, want epoch=23 ready=false", decision)
	}
}

func TestRingWatcherInitializesUnsetClusterStaleness(t *testing.T) {
	registerRingWatcherDriver.Do(func() {
		sql.Register("cluster-ring-watcher-test", ringWatcherTestDriver{})
	})
	resetRingWatcherMembershipState(0, 0, nil)
	db, err := sql.Open("cluster-ring-watcher-test", "")
	if err != nil {
		t.Fatalf("open test DB: %v", err)
	}
	defer db.Close()

	ring := NewAuthRing("sj")
	watcher := &RingWatcher{DB: db, Ring: ring, RequireAuthority: true}
	if err = watcher.refresh(context.Background(), 45*time.Second); err != nil {
		t.Fatalf("initialize staleness: %v", err)
	}
	if !ring.Ready() {
		t.Fatal("zero staleness singleton should be initialized, not failed closed")
	}
	ringWatcherMembershipState.Lock()
	got := ringWatcherMembershipState.stalenessMS
	ringWatcherMembershipState.Unlock()
	if got != (45 * time.Second).Milliseconds() {
		t.Fatalf("initialized staleness_ms = %d, want %d", got, (45 * time.Second).Milliseconds())
	}
}

func TestRingWatcherRequiredAuthorityFailsClosedWhenEpochUnavailable(t *testing.T) {
	registerRingWatcherDriver.Do(func() {
		sql.Register("cluster-ring-watcher-test", ringWatcherTestDriver{})
	})
	resetRingWatcherMembershipState(4, defaultRingStaleness, nil)
	db, err := sql.Open("cluster-ring-watcher-test", "")
	if err != nil {
		t.Fatalf("open test DB: %v", err)
	}
	defer db.Close()

	ring := NewAuthRing("sj")
	ring.RebuildAt(4, []RingMember{{NodeID: "sj", Weight: 100}})
	ringWatcherQueryFailures.Store(1)
	defer ringWatcherQueryFailures.Store(0)
	watcher := &RingWatcher{DB: db, Ring: ring, RequireAuthority: true}
	if err = watcher.refresh(context.Background(), defaultRingStaleness); err == nil {
		t.Fatal("missing epoch authority must fail refresh")
	}
	if decision := ring.Decision("auth-a"); decision.Ready {
		t.Fatalf("missing epoch authority retained a ready ring: %+v", decision)
	}
}

func TestMembershipFingerprintIsCanonicalAndWeightSensitive(t *testing.T) {
	first := membershipFingerprint([]RingMember{{NodeID: "sj", Weight: 100}, {NodeID: "la", Weight: 50}})
	reordered := membershipFingerprint([]RingMember{{NodeID: "la", Weight: 50}, {NodeID: "sj", Weight: 100}})
	if !slices.Equal(first, reordered) {
		t.Fatal("fingerprint must be independent of query order")
	}
	changed := membershipFingerprint([]RingMember{{NodeID: "la", Weight: 51}, {NodeID: "sj", Weight: 100}})
	if slices.Equal(first, changed) {
		t.Fatal("weight change must alter membership fingerprint")
	}
}

func TestRingWatcherSerializesCommitPublishWithLaterFailClose(t *testing.T) {
	registerRingWatcherDriver.Do(func() {
		sql.Register("cluster-ring-watcher-test", ringWatcherTestDriver{})
	})
	members := []RingMember{{NodeID: "sj", Weight: 100}}
	resetRingWatcherMembershipState(31, defaultRingStaleness, membershipFingerprint(members))
	db, err := sql.Open("cluster-ring-watcher-test", "")
	if err != nil {
		t.Fatalf("open test DB: %v", err)
	}
	defer db.Close()

	ring := NewAuthRing("sj")
	ring.RebuildAt(31, members)
	committed := make(chan struct{})
	releasePublish := make(chan struct{})
	var commitHooks atomic.Int32
	watcher := &RingWatcher{
		DB:               db,
		Ring:             ring,
		RequireAuthority: true,
		afterCommit: func() {
			if commitHooks.Add(1) == 1 {
				close(committed)
				<-releasePublish
			}
		},
	}

	firstDone := make(chan error, 1)
	go func() { firstDone <- watcher.refresh(context.Background(), defaultRingStaleness) }()
	<-committed
	if watcher.refreshMu.TryLock() {
		watcher.refreshMu.Unlock()
		t.Fatal("refresh serialization lock was released before local publish")
	}

	ringWatcherQueryFailures.Store(1)
	defer ringWatcherQueryFailures.Store(0)
	secondCalling := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		close(secondCalling)
		secondDone <- watcher.refresh(context.Background(), defaultRingStaleness)
	}()
	<-secondCalling
	close(releasePublish)
	if err = <-firstDone; err != nil {
		t.Fatalf("earlier successful refresh: %v", err)
	}
	if err = <-secondDone; err == nil {
		t.Fatal("later refresh must surface the injected authority failure")
	}
	if decision := ring.Decision("auth-a"); decision.Ready {
		t.Fatalf("older successful publish overrode later fail-close: %+v", decision)
	}

	if err = watcher.refresh(context.Background(), defaultRingStaleness); err != nil {
		t.Fatalf("genuinely later recovery refresh: %v", err)
	}
	if decision := ring.Decision("auth-a"); !decision.Ready || decision.Epoch != 31 {
		t.Fatalf("later successful refresh did not recover ring: %+v", decision)
	}
}
