package cluster

import (
	"fmt"
	"math"
	"sync"
	"testing"
)

// Deterministic across calls — critical because different replicas must
// agree on who owns an auth.
func TestAuthRing_OwnerDeterministic(t *testing.T) {
	r := NewAuthRing("sj")
	r.Rebuild([]RingMember{
		{NodeID: "sj", Weight: 100},
		{NodeID: "la", Weight: 100},
		{NodeID: "fra", Weight: 100},
		{NodeID: "lon", Weight: 100},
	})

	// Same key must map to same owner in 1000 calls.
	want := r.Owner("codex-001")
	for i := 0; i < 1000; i++ {
		if got := r.Owner("codex-001"); got != want {
			t.Fatalf("call %d: owner flipped %q -> %q", i, want, got)
		}
	}
}

// Different ring instances with the same membership must agree — cross-
// replica agreement is the whole point.
func TestAuthRing_OwnerAgreement(t *testing.T) {
	members := []RingMember{
		{NodeID: "sj", Weight: 50},
		{NodeID: "la", Weight: 100},
		{NodeID: "fra", Weight: 100},
		{NodeID: "lon", Weight: 50},
	}
	r1 := NewAuthRing("sj")
	r1.Rebuild(members)
	r2 := NewAuthRing("la") // different "me"
	r2.Rebuild(members)

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("auth-%03d", i)
		if r1.Owner(key) != r2.Owner(key) {
			t.Fatalf("ring disagreement on %q: r1=%s r2=%s", key, r1.Owner(key), r2.Owner(key))
		}
	}
}

// Removing one node must only reshuffle ~1/N of keys. Rendezvous guarantee:
// only keys whose winner was the removed node change owner.
func TestAuthRing_MinimalDisruptionOnRemoval(t *testing.T) {
	r := NewAuthRing("sj")
	before := []RingMember{
		{NodeID: "sj", Weight: 100},
		{NodeID: "la", Weight: 100},
		{NodeID: "fra", Weight: 100},
		{NodeID: "lon", Weight: 100},
	}
	r.Rebuild(before)
	beforeMap := map[string]string{}
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("auth-%03d", i)
		beforeMap[key] = r.Owner(key)
	}

	// Remove "lon"
	r.Rebuild([]RingMember{
		{NodeID: "sj", Weight: 100},
		{NodeID: "la", Weight: 100},
		{NodeID: "fra", Weight: 100},
	})
	changed := 0
	for key, prevOwner := range beforeMap {
		if newOwner := r.Owner(key); newOwner != prevOwner {
			changed++
			// Only keys whose previous owner was "lon" should migrate.
			if prevOwner != "lon" {
				t.Errorf("key %q migrated from %q (non-removed!) to %q", key, prevOwner, newOwner)
			}
		}
	}
	// ~1/4 of keys should have changed (those that were on lon). Allow
	// some variance because 1000 keys is not infinite.
	if changed < 150 || changed > 350 {
		t.Errorf("changed=%d out of 1000; want ~250 (1/4), HRW broken?", changed)
	}
}

// Weight skew: a 2x-weighted node should get ~2x the keys.
func TestAuthRing_WeightRespected(t *testing.T) {
	r := NewAuthRing("x")
	r.Rebuild([]RingMember{
		{NodeID: "strong", Weight: 200},
		{NodeID: "weak", Weight: 100},
	})
	counts := map[string]int{}
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("auth-%05d", i)
		counts[r.Owner(key)]++
	}
	// Expect strong:weak ≈ 2:1. Allow 15% slack for hash variance.
	ratio := float64(counts["strong"]) / float64(counts["weak"])
	if ratio < 1.7 || ratio > 2.3 {
		t.Errorf("expected ~2x weighted ratio, got strong=%d weak=%d ratio=%.2f",
			counts["strong"], counts["weak"], ratio)
	}
}

// Nil receiver and unbuilt ring both degrade to IsMine=true so bootstrap
// never black-holes requests.
func TestAuthRing_IsMineDegradation(t *testing.T) {
	var nilRing *AuthRing
	if !nilRing.IsMine("anything") {
		t.Error("nil ring must IsMine=true (degradation)")
	}

	empty := NewAuthRing("sj")
	if !empty.IsMine("anything") {
		t.Error("unbuilt ring must IsMine=true (bootstrap window)")
	}
	if empty.Ready() {
		t.Error("unbuilt ring must Ready=false")
	}

	// Empty member list after Rebuild — still treated as not ready.
	empty.Rebuild(nil)
	if !empty.IsMine("anything") {
		t.Error("empty-membership ring must IsMine=true")
	}
	if empty.Ready() {
		t.Error("empty-membership ring must Ready=false")
	}
}

// When my node is NOT in the ring (e.g. I'm draining), IsMine must return
// false so I stop claiming ownership — lets other nodes take over.
func TestAuthRing_IsMineFalseWhenSelfAbsent(t *testing.T) {
	r := NewAuthRing("sj")
	r.Rebuild([]RingMember{
		{NodeID: "la", Weight: 100},
		{NodeID: "fra", Weight: 100},
	})
	if r.IsMine("codex-001") {
		t.Error("IsMine must be false when my NodeID is absent from membership")
	}
	if !r.Ready() {
		t.Error("Ready must be true when membership is non-empty")
	}
}

// Empty myNodeID is a misconfig (no way to claim) — degrade to IsMine=true
// so the replica falls back to pre-sharding behavior rather than silently
// serving nothing.
func TestAuthRing_EmptyMyNodeIDDegrades(t *testing.T) {
	r := NewAuthRing("") // misconfig
	r.Rebuild([]RingMember{{NodeID: "la", Weight: 100}})
	if !r.IsMine("anything") {
		t.Error("empty myNodeID must degrade to IsMine=true")
	}
}

// Weight<=0 must be clamped to 100 (matches NewRegistrar invariant).
func TestAuthRing_ZeroWeightClamped(t *testing.T) {
	r := NewAuthRing("sj")
	r.Rebuild([]RingMember{
		{NodeID: "sj", Weight: 0},
		{NodeID: "la", Weight: -5},
	})
	// Both should have weight 100 post-clamp → even split (~50/50).
	counts := map[string]int{}
	for i := 0; i < 5000; i++ {
		counts[r.Owner(fmt.Sprintf("auth-%04d", i))]++
	}
	ratio := float64(counts["sj"]) / float64(counts["la"])
	if math.Abs(ratio-1.0) > 0.15 {
		t.Errorf("zero/negative weight not clamped: sj=%d la=%d ratio=%.2f",
			counts["sj"], counts["la"], ratio)
	}
}

// Duplicate NodeIDs must be coalesced (last-write-wins). Otherwise a
// replica erroneously registered twice would dominate.
func TestAuthRing_DuplicateMembersCoalesced(t *testing.T) {
	r := NewAuthRing("x")
	r.Rebuild([]RingMember{
		{NodeID: "sj", Weight: 50},
		{NodeID: "sj", Weight: 100},
		{NodeID: "la", Weight: 100},
	})
	members := r.Members()
	if len(members) != 2 {
		t.Fatalf("expected 2 unique members, got %d: %+v", len(members), members)
	}
	for _, m := range members {
		if m.NodeID == "sj" && m.Weight != 100 {
			t.Errorf("sj weight should be 100 (last write), got %d", m.Weight)
		}
	}
}

// Concurrent read/write: Rebuild must be safe to call while IsMine is in
// flight. atomic.Pointer swap guarantees observers always see a complete
// snapshot.
func TestAuthRing_ConcurrentSafe(t *testing.T) {
	r := NewAuthRing("sj")
	r.Rebuild([]RingMember{
		{NodeID: "sj", Weight: 100},
		{NodeID: "la", Weight: 100},
	})
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer: toggles membership every 100µs.
	wg.Add(1)
	go func() {
		defer wg.Done()
		toggle := true
		for {
			select {
			case <-stop:
				return
			default:
				if toggle {
					r.Rebuild([]RingMember{
						{NodeID: "sj", Weight: 100},
						{NodeID: "la", Weight: 100},
						{NodeID: "fra", Weight: 100},
					})
				} else {
					r.Rebuild([]RingMember{
						{NodeID: "sj", Weight: 100},
						{NodeID: "la", Weight: 100},
					})
				}
				toggle = !toggle
			}
		}
	}()

	// 10 readers.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				_ = r.IsMine("codex-001")
				_ = r.Owner("codex-002")
			}
		}()
	}

	// Let the race run briefly.
	for i := 0; i < 100; i++ {
		_ = r.Members()
	}
	close(stop)
	wg.Wait()
}

func TestAuthRing_OwnerEmptyWhenUnbuilt(t *testing.T) {
	r := NewAuthRing("sj")
	if got := r.Owner("x"); got != "" {
		t.Errorf("unbuilt ring Owner should be empty, got %q", got)
	}
}
