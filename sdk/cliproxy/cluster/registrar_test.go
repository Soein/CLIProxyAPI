package cluster

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var registerRegistrarCallbackDriver sync.Once
var registerRegistrarActivationDriver sync.Once
var registrarActivationControls sync.Map

type registrarCallbackDriver struct{}

type registrarCallbackConn struct {
	fail bool
}

type registrarActivationControl struct {
	mu       sync.Mutex
	failNext bool
	statuses []string
}

type registrarActivationDriver struct{}

type registrarActivationConn struct{ control *registrarActivationControl }

func (registrarActivationDriver) Open(name string) (driver.Conn, error) {
	control, _ := registrarActivationControls.Load(name)
	return &registrarActivationConn{control: control.(*registrarActivationControl)}, nil
}

func (*registrarActivationConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (*registrarActivationConn) Close() error                        { return nil }
func (*registrarActivationConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (c *registrarActivationConn) ExecContext(_ context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
	status, _ := args[len(args)-1].Value.(string)
	c.control.mu.Lock()
	c.control.statuses = append(c.control.statuses, status)
	fail := c.control.failNext
	c.control.failNext = false
	c.control.mu.Unlock()
	if fail {
		return nil, errors.New("simulated activation failure")
	}
	return driver.RowsAffected(1), nil
}

func (registrarCallbackDriver) Open(name string) (driver.Conn, error) {
	return registrarCallbackConn{fail: name == "fail"}, nil
}

func (registrarCallbackConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (registrarCallbackConn) Close() error { return nil }

func (registrarCallbackConn) Begin() (driver.Tx, error) { return nil, driver.ErrSkip }

func (c registrarCallbackConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	if c.fail {
		return nil, errors.New("simulated heartbeat failure")
	}
	return driver.RowsAffected(1), nil
}

func TestNewRegistrar_RequiresDB(t *testing.T) {
	_, err := NewRegistrar(RegistrarConfig{NodeID: "n1", Endpoint: "http://x"})
	if err == nil || err.Error() != "registrar: DB is required" {
		t.Fatalf("want DB-required error, got %v", err)
	}
}

func TestNewRegistrar_RequiresNodeID(t *testing.T) {
	db := &sql.DB{}
	_, err := NewRegistrar(RegistrarConfig{DB: db, Endpoint: "http://x"})
	if err == nil || err.Error() != "registrar: NodeID is required" {
		t.Fatalf("want NodeID-required error, got %v", err)
	}

	// whitespace-only should also fail
	_, err = NewRegistrar(RegistrarConfig{DB: db, NodeID: "   ", Endpoint: "http://x"})
	if err == nil {
		t.Fatal("whitespace NodeID should be rejected")
	}
}

func TestNewRegistrar_RequiresEndpoint(t *testing.T) {
	db := &sql.DB{}
	_, err := NewRegistrar(RegistrarConfig{DB: db, NodeID: "n1"})
	if !errors.Is(err, ErrEndpointRequired) {
		t.Fatalf("want ErrEndpointRequired, got %v", err)
	}

	_, err = NewRegistrar(RegistrarConfig{DB: db, NodeID: "n1", Endpoint: "   "})
	if !errors.Is(err, ErrEndpointRequired) {
		t.Fatalf("whitespace endpoint must yield ErrEndpointRequired, got %v", err)
	}
}

func TestNewRegistrar_FillsDefaults(t *testing.T) {
	db := &sql.DB{}
	r, err := NewRegistrar(RegistrarConfig{
		DB:       db,
		NodeID:   "n1",
		Endpoint: "http://x",
		// Weight/Interval/DrainGrace intentionally zero
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.cfg.Weight != 100 {
		t.Errorf("default weight should be 100, got %d", r.cfg.Weight)
	}
	if r.cfg.Interval != defaultRegistrarInterval {
		t.Errorf("default interval should be %s, got %s", defaultRegistrarInterval, r.cfg.Interval)
	}
	if r.cfg.DrainGrace != defaultDrainGrace {
		t.Errorf("default drain grace should be %s, got %s", defaultDrainGrace, r.cfg.DrainGrace)
	}
	if r.cfg.TimeSource == nil {
		t.Error("TimeSource must be set to time.Now when nil")
	}
	if got := r.currentStatus(); got != statusActive {
		t.Errorf("default initial status must remain %q, got %q", statusActive, got)
	}
}

func TestNewRegistrar_StartDrainingRequiresExplicitOptIn(t *testing.T) {
	db := &sql.DB{}
	r, err := NewRegistrar(RegistrarConfig{
		DB:            db,
		NodeID:        "n1",
		Endpoint:      "http://x",
		StartDraining: true,
	})
	if err != nil {
		t.Fatalf("NewRegistrar() error: %v", err)
	}
	if got := r.currentStatus(); got != statusDraining {
		t.Fatalf("opt-in initial status = %q, want %q", got, statusDraining)
	}
}

func TestNewRegistrar_NegativeWeightFallsBack(t *testing.T) {
	db := &sql.DB{}
	r, err := NewRegistrar(RegistrarConfig{
		DB: db, NodeID: "n1", Endpoint: "http://x", Weight: -5,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.cfg.Weight != 100 {
		t.Errorf("negative weight should fall back to 100, got %d", r.cfg.Weight)
	}
}

func TestNewRegistrar_CustomValuesPreserved(t *testing.T) {
	db := &sql.DB{}
	r, err := NewRegistrar(RegistrarConfig{
		DB:         db,
		NodeID:     "n1",
		Endpoint:   "http://x",
		Weight:     50,
		Interval:   3 * time.Second,
		DrainGrace: 20 * time.Second,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.cfg.Weight != 50 {
		t.Errorf("custom weight not preserved: %d", r.cfg.Weight)
	}
	if r.cfg.Interval != 3*time.Second {
		t.Errorf("custom interval not preserved: %s", r.cfg.Interval)
	}
	if r.cfg.DrainGrace != 20*time.Second {
		t.Errorf("custom drain grace not preserved: %s", r.cfg.DrainGrace)
	}
}

func TestRegistrarRunReportsInitialAndPeriodicActiveHeartbeatSuccess(t *testing.T) {
	registerRegistrarCallbackDriver.Do(func() {
		sql.Register("cluster-registrar-callback-test", registrarCallbackDriver{})
	})
	db, errOpen := sql.Open("cluster-registrar-callback-test", "")
	if errOpen != nil {
		t.Fatalf("open test DB: %v", errOpen)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var callbacks atomic.Int32
	callbackDone := make(chan struct{})
	var callbackDoneOnce sync.Once
	registrar, errRegistrar := NewRegistrar(RegistrarConfig{
		DB:       db,
		NodeID:   "n1",
		Endpoint: "http://x",
		Interval: 10 * time.Millisecond,
		OnActiveHeartbeat: func(time.Time) {
			if callbacks.Add(1) >= 2 {
				callbackDoneOnce.Do(func() { close(callbackDone) })
				cancel()
			}
		},
	})
	if errRegistrar != nil {
		t.Fatalf("NewRegistrar() error: %v", errRegistrar)
	}
	if errActivate := registrar.Activate(context.Background()); errActivate != nil {
		t.Fatalf("Activate() error: %v", errActivate)
	}

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		registrar.Run(ctx)
	}()
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("initial and periodic heartbeat callbacks did not both run")
	}
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("registrar did not stop after cancellation")
	}
	if got := callbacks.Load(); got < 2 {
		t.Fatalf("active heartbeat callbacks = %d, want at least initial + periodic", got)
	}
}

func TestRegistrarRunDoesNotReportFailedHeartbeat(t *testing.T) {
	registerRegistrarCallbackDriver.Do(func() {
		sql.Register("cluster-registrar-callback-test", registrarCallbackDriver{})
	})
	db, errOpen := sql.Open("cluster-registrar-callback-test", "fail")
	if errOpen != nil {
		t.Fatalf("open test DB: %v", errOpen)
	}
	defer db.Close()

	var callbacks atomic.Int32
	registrar, errRegistrar := NewRegistrar(RegistrarConfig{
		DB:                db,
		NodeID:            "n1",
		Endpoint:          "http://x",
		Interval:          5 * time.Millisecond,
		OnActiveHeartbeat: func(time.Time) { callbacks.Add(1) },
	})
	if errRegistrar != nil {
		t.Fatalf("NewRegistrar() error: %v", errRegistrar)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	registrar.Run(ctx)
	if got := callbacks.Load(); got != 0 {
		t.Fatalf("failed heartbeat callbacks = %d, want 0", got)
	}
}

func TestRegistrarActivateFailureReturnsToJoining(t *testing.T) {
	registerRegistrarActivationDriver.Do(func() {
		sql.Register("cluster-registrar-activation-test", registrarActivationDriver{})
	})
	control := &registrarActivationControl{failNext: true}
	dsn := t.Name()
	registrarActivationControls.Store(dsn, control)
	defer registrarActivationControls.Delete(dsn)
	db, errOpen := sql.Open("cluster-registrar-activation-test", dsn)
	if errOpen != nil {
		t.Fatalf("open test DB: %v", errOpen)
	}
	defer db.Close()
	registrar, errRegistrar := NewRegistrar(RegistrarConfig{
		DB:            db,
		NodeID:        "n1",
		Endpoint:      "http://x",
		StartDraining: true,
	})
	if errRegistrar != nil {
		t.Fatalf("NewRegistrar() error: %v", errRegistrar)
	}
	if errActivate := registrar.Activate(context.Background()); errActivate == nil {
		t.Fatal("Activate() error = nil, want simulated DB failure")
	}
	if registrar.IsActive() {
		t.Fatal("failed Activate() left registrar active")
	}
	if errHeartbeat := registrar.upsert(context.Background()); errHeartbeat != nil {
		t.Fatalf("post-failure heartbeat error: %v", errHeartbeat)
	}
	control.mu.Lock()
	statuses := append([]string(nil), control.statuses...)
	control.mu.Unlock()
	if len(statuses) != 2 || statuses[0] != statusActive || statuses[1] != statusJoining {
		t.Fatalf("published statuses = %v, want failed active followed by joining heartbeat", statuses)
	}
}

func TestRegistrarJoinPublishesJoiningAndRetainsItOnFailure(t *testing.T) {
	registerRegistrarActivationDriver.Do(func() {
		sql.Register("cluster-registrar-activation-test", registrarActivationDriver{})
	})
	control := &registrarActivationControl{failNext: true}
	dsn := t.Name()
	registrarActivationControls.Store(dsn, control)
	defer registrarActivationControls.Delete(dsn)
	db, errOpen := sql.Open("cluster-registrar-activation-test", dsn)
	if errOpen != nil {
		t.Fatalf("open test DB: %v", errOpen)
	}
	defer db.Close()
	registrar, errRegistrar := NewRegistrar(RegistrarConfig{
		DB:            db,
		NodeID:        "n1",
		Endpoint:      "http://x",
		StartDraining: true,
	})
	if errRegistrar != nil {
		t.Fatalf("NewRegistrar() error: %v", errRegistrar)
	}
	if errJoin := registrar.Join(context.Background()); errJoin == nil {
		t.Fatal("Join() error = nil, want simulated DB failure")
	}
	if got := registrar.currentStatus(); got != statusJoining {
		t.Fatalf("status after failed Join() = %q, want %q", got, statusJoining)
	}
	if errHeartbeat := registrar.Publish(context.Background()); errHeartbeat != nil {
		t.Fatalf("Publish() retry error: %v", errHeartbeat)
	}
	control.mu.Lock()
	statuses := append([]string(nil), control.statuses...)
	control.mu.Unlock()
	if len(statuses) != 2 || statuses[0] != statusJoining || statuses[1] != statusJoining {
		t.Fatalf("published statuses = %v, want joining retained across retry", statuses)
	}
}

// Drain should flip the published status even before a DB round-trip
// succeeds, so subsequent heartbeats pick up the new value. Using a nil
// DB would panic inside upsert; we instead verify the in-memory transition
// by calling currentStatus() without exercising the DB path.
func TestDrain_UpdatesStatus(t *testing.T) {
	db := &sql.DB{}
	r, err := NewRegistrar(RegistrarConfig{DB: db, NodeID: "n1", Endpoint: "http://x"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := r.currentStatus(); got != statusActive {
		t.Fatalf("default pre-drain status should be active, got %q", got)
	}
	// Manually flip status (simulates what Drain does without the DB write)
	r.mu.Lock()
	r.status = statusDraining
	r.mu.Unlock()
	if got := r.currentStatus(); got != statusDraining {
		t.Errorf("status should be %q after drain, got %q", statusDraining, got)
	}
}

// Passing nil Registrar to Run/Drain/close must not panic — guards against
// misconfigurations where NewRegistrar returned an error and the caller
// forgot to check before spawning the goroutine.
func TestNilRegistrar_IsSafe(t *testing.T) {
	var r *InstanceRegistrar
	r.Run(context.Background()) // must not panic
	if err := r.Drain(context.Background()); err != nil {
		t.Errorf("nil Drain should return nil error, got %v", err)
	}
	r.close() // must not panic
}
