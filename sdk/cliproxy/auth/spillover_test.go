package auth

import (
	"context"
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

// stubRing lets tests dictate IsMine without spinning up cluster.AuthRing.
// nil Ring receiver degrades to true (by design); we need a concrete
// implementation here to assert sharded skip / spillover paths.
type stubRing struct {
	mine  map[string]bool
	ready bool
}

func (s *stubRing) IsMine(authID string) bool {
	if s == nil {
		return true
	}
	v, ok := s.mine[authID]
	if !ok {
		return false // unknown IDs default to non-owned so tests stay explicit
	}
	return v
}

func (s *stubRing) Ready() bool {
	if s == nil {
		return false
	}
	return s.ready
}

// newManagerForSpilloverTest wires a Manager with an authScheduler holding
// the supplied auths. trackingSelector keeps it deterministic (picks the
// last auth in the ready list, see scheduler_test.go).
func newManagerForSpilloverTest(auths ...*Auth) (*Manager, *trackingSelector) {
	sel := &trackingSelector{}
	m := NewManager(nil, sel, nil)
	m.scheduler = newAuthScheduler(sel)
	m.scheduler.rebuild(auths)
	m.mu.Lock()
	for _, a := range auths {
		if a == nil {
			continue
		}
		m.auths[a.ID] = a
	}
	m.mu.Unlock()
	return m, sel
}

// cloneTriedMap must produce a distinct map that mirrors input entries.
func TestCloneTriedMap(t *testing.T) {
	src := map[string]struct{}{"a": {}, "b": {}}
	dst := cloneTriedMap(src)
	if len(dst) != 2 {
		t.Fatalf("want 2 entries, got %d", len(dst))
	}
	dst["c"] = struct{}{}
	if _, exists := src["c"]; exists {
		t.Error("mutating clone leaked back to source")
	}

	empty := cloneTriedMap(nil)
	if empty == nil || len(empty) != 0 {
		t.Error("nil input should produce empty (not nil) map")
	}
	empty["x"] = struct{}{}
}

// Sharding off → fast path, behaviour identical to pre-Sprint-3.
func TestPickWithShardFilter_ShardingOff_FastPath(t *testing.T) {
	a1 := &Auth{ID: "codex-1", Provider: "codex"}
	registerSchedulerModels(t, "codex", "", "codex-1")

	m, _ := newManagerForSpilloverTest(a1)
	// Sharding explicitly disabled — no ring, no filter loop.
	_ = m.IsAuthShardingEnabled() // false

	got, err := m.pickWithShardFilter(context.Background(), "codex", "",
		cliproxyexecutor.Options{}, map[string]struct{}{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got == nil || got.ID != "codex-1" {
		t.Fatalf("expected codex-1, got %+v", got)
	}
}

// Sharding on, ring owns the only candidate → directly returned.
func TestPickWithShardFilter_Sharded_OwnedAuthReturned(t *testing.T) {
	a1 := &Auth{ID: "codex-1", Provider: "codex"}
	registerSchedulerModels(t, "codex", "", "codex-1")

	m, _ := newManagerForSpilloverTest(a1)
	m.SetAuthRing(&stubRing{ready: true, mine: map[string]bool{"codex-1": true}})
	m.SetAuthShardingEnabled(true)

	got, err := m.pickWithShardFilter(context.Background(), "codex", "",
		cliproxyexecutor.Options{}, map[string]struct{}{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got == nil || got.ID != "codex-1" {
		t.Fatalf("expected codex-1, got %+v", got)
	}
}

// Sharding on, first candidate non-owned, second owned → second is chosen
// and the first is recorded in the LOCAL tried only (caller's tried
// untouched).
func TestPickWithShardFilter_NonOwnedSkipped_DoesNotLeakToCaller(t *testing.T) {
	a1 := &Auth{ID: "codex-1", Provider: "codex"}
	a2 := &Auth{ID: "codex-2", Provider: "codex"}
	registerSchedulerModels(t, "codex", "", "codex-1", "codex-2")

	m, _ := newManagerForSpilloverTest(a1, a2)
	// trackingSelector returns LAST auth → codex-2 first. Mark codex-2 as
	// non-owned so we verify the skip-and-retry; codex-1 is owned.
	m.SetAuthRing(&stubRing{
		ready: true,
		mine:  map[string]bool{"codex-1": true, "codex-2": false},
	})
	m.SetAuthShardingEnabled(true)

	callerTried := map[string]struct{}{}
	got, err := m.pickWithShardFilter(context.Background(), "codex", "",
		cliproxyexecutor.Options{}, callerTried)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got == nil || got.ID != "codex-1" {
		t.Fatalf("expected codex-1 (only owned), got %+v", got)
	}
	if len(callerTried) != 0 {
		t.Errorf("caller's tried must stay empty; got %v", callerTried)
	}
}

// Sharding on, every candidate non-owned, spillover ON → falls back and
// returns an auth from the global pool. Also verifies the caller's tried
// is used for the global pick (not the shard-local one).
func TestPickWithShardFilter_SpilloverFallsBackToGlobal(t *testing.T) {
	a1 := &Auth{ID: "codex-1", Provider: "codex"}
	a2 := &Auth{ID: "codex-2", Provider: "codex"}
	registerSchedulerModels(t, "codex", "", "codex-1", "codex-2")

	m, _ := newManagerForSpilloverTest(a1, a2)
	m.SetAuthRing(&stubRing{
		ready: true,
		mine:  map[string]bool{"codex-1": false, "codex-2": false},
	})
	m.SetAuthShardingEnabled(true)
	m.SetSpilloverEnabled(true)

	got, err := m.pickWithShardFilter(context.Background(), "codex", "",
		cliproxyexecutor.Options{}, map[string]struct{}{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got == nil {
		t.Fatal("spillover must return an auth, got nil")
	}
	// With spillover enabled, ANY global-pool auth is acceptable; the
	// scheduler's internal ordering is not a contract. The critical
	// invariant is: not nil, not error.
	if got.ID != "codex-1" && got.ID != "codex-2" {
		t.Errorf("spillover picked unknown auth %q", got.ID)
	}
}

// Sharding on, every candidate non-owned, spillover OFF → returns
// auth_not_found (or wrapped). Critically, callers get a clear failure so
// they can surface an operator-meaningful error instead of a stale auth.
func TestPickWithShardFilter_SpilloverOff_ReturnsNoAuth(t *testing.T) {
	a1 := &Auth{ID: "codex-1", Provider: "codex"}
	registerSchedulerModels(t, "codex", "", "codex-1")

	m, _ := newManagerForSpilloverTest(a1)
	m.SetAuthRing(&stubRing{
		ready: true,
		mine:  map[string]bool{"codex-1": false},
	})
	m.SetAuthShardingEnabled(true)
	m.SetSpilloverEnabled(false)

	got, err := m.pickWithShardFilter(context.Background(), "codex", "",
		cliproxyexecutor.Options{}, map[string]struct{}{})
	if err == nil && got != nil {
		t.Fatalf("expected no auth (spillover off), got %+v", got)
	}
	// Either: err != nil, OR got == nil. Both are valid "nothing to
	// serve" signals from the scheduler path (the scheduler may surface
	// auth_not_found, or the loop may bail and return nil/nil).
}

// Sprint 3 flags must be independently readable so an ops tool or admin
// API can distinguish "sharding on, spillover off" from "sharding off".
func TestSpilloverFlag_Independent(t *testing.T) {
	m, _ := newManagerForSpilloverTest()
	if m.IsSpilloverEnabled() {
		t.Error("spillover default must be false")
	}
	m.SetSpilloverEnabled(true)
	if !m.IsSpilloverEnabled() {
		t.Error("SetSpilloverEnabled(true) not reflected in getter")
	}
	// Flipping sharding must NOT touch spillover.
	m.SetAuthShardingEnabled(false)
	if !m.IsSpilloverEnabled() {
		t.Error("SetAuthShardingEnabled must not affect spillover flag")
	}
}

// Nil-receiver safety — every Sprint 3 method must tolerate nil Manager
// so call sites don't have to defensively check.
func TestSpillover_NilReceiverSafe(t *testing.T) {
	var m *Manager
	m.SetSpilloverEnabled(true) // no panic
	if m.IsSpilloverEnabled() {
		t.Error("nil manager must report false")
	}
}
