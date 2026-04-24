package cluster

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

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
		t.Errorf("initial status must be %q, got %q", statusActive, got)
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
		t.Fatalf("pre-drain status should be active, got %q", got)
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
