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

// A nil, unbuilt, or empty ring cannot prove ownership and must fail closed.
func TestAuthRing_IsMineFailsClosedUntilReady(t *testing.T) {
	var nilRing *AuthRing
	if nilRing.IsMine("anything") {
		t.Error("nil ring must not claim ownership")
	}

	empty := NewAuthRing("sj")
	if empty.IsMine("anything") {
		t.Error("unbuilt ring must not claim ownership")
	}
	if empty.Ready() {
		t.Error("unbuilt ring must Ready=false")
	}

	// Empty member list after Rebuild is still not ready.
	empty.Rebuild(nil)
	if empty.IsMine("anything") {
		t.Error("empty-membership ring must not claim ownership")
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
	if r.Ready() {
		t.Error("Ready must be false when the local node is absent")
	}
}

// Empty myNodeID is a misconfiguration: the replica cannot prove that it is
// any member in the ring and therefore must not claim ownership.
func TestAuthRing_EmptyMyNodeIDFailsClosed(t *testing.T) {
	r := NewAuthRing("") // misconfig
	r.Rebuild([]RingMember{{NodeID: "la", Weight: 100}})
	if r.IsMine("anything") {
		t.Error("empty myNodeID must not claim ownership")
	}
	if r.Ready() {
		t.Error("ring with empty myNodeID must not be ready")
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

func TestAuthRing_FailClosedIsTerminal(t *testing.T) {
	r := NewAuthRing("sj")
	members := []RingMember{{NodeID: "sj", Weight: 100}}
	r.Rebuild(members)
	if !r.Ready() {
		t.Fatal("ring should be ready before terminal fail-close")
	}

	r.FailClosed()
	r.Rebuild(members)

	if r.Ready() {
		t.Fatal("terminal fail-close must reject later rebuilds")
	}
	if got := r.Owner("auth-a"); got != "" {
		t.Fatalf("Owner() after terminal fail-close = %q, want empty", got)
	}
	if got := r.Members(); len(got) != 0 {
		t.Fatalf("Members() after terminal fail-close = %+v, want empty", got)
	}
}

func TestAuthRing_FailClosedLinearizesWithConcurrentRebuild(t *testing.T) {
	r := NewAuthRing("sj")
	members := []RingMember{{NodeID: "sj", Weight: 100}}
	r.Rebuild(members)

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 1000; j++ {
				r.Rebuild(members)
			}
		}()
	}
	close(start)
	r.FailClosed()
	wg.Wait()

	if r.Ready() || r.Owner("auth-a") != "" || len(r.Members()) != 0 {
		t.Fatal("concurrent rebuild restored a terminally failed ring")
	}
}

func TestAuthRing_OwnerEmptyWhenUnbuilt(t *testing.T) {
	r := NewAuthRing("sj")
	if got := r.Owner("x"); got != "" {
		t.Errorf("unbuilt ring Owner should be empty, got %q", got)
	}
}

func TestAuthRingDecisionPublishesEpochOwnerAndReadinessAtomically(t *testing.T) {
	r := NewAuthRing("sj")
	r.RebuildAt(41, []RingMember{{NodeID: "sj", Weight: 100}})

	decision := r.Decision("auth-a")
	if decision.Epoch != 41 || decision.Owner != "sj" || !decision.Ready {
		t.Fatalf("Decision() = %+v, want epoch=41 owner=sj ready=true", decision)
	}
}

func TestAuthRingRebuildAtRejectsOlderEpoch(t *testing.T) {
	r := NewAuthRing("sj")
	r.RebuildAt(9, []RingMember{{NodeID: "sj", Weight: 100}})
	r.RebuildAt(8, []RingMember{{NodeID: "la", Weight: 100}})

	decision := r.Decision("auth-a")
	if decision.Epoch != 9 || decision.Owner != "sj" || !decision.Ready {
		t.Fatalf("older publication replaced current snapshot: %+v", decision)
	}
}

func TestAuthRingDecisionFailsClosedWhenLocalNodeAbsent(t *testing.T) {
	r := NewAuthRing("sj")
	r.RebuildAt(12, []RingMember{{NodeID: "la", Weight: 100}})

	decision := r.Decision("auth-a")
	if decision.Epoch != 12 || decision.Owner != "la" || decision.Ready {
		t.Fatalf("Decision() = %+v, want epoch=12 owner=la ready=false", decision)
	}
}

func TestAuthRingDecisionNeverMixesEpochAndOwner(t *testing.T) {
	r := NewAuthRing("sj")
	r.RebuildAt(1, []RingMember{{NodeID: "sj", Weight: 100}})
	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for epoch := int64(2); epoch <= 2000; epoch++ {
			member := RingMember{NodeID: "la", Weight: 100}
			if epoch%2 == 1 {
				member.NodeID = "sj"
			}
			r.RebuildAt(epoch, []RingMember{member})
		}
		close(stop)
	}()
	for {
		decision := r.Decision("auth-a")
		if decision.Epoch%2 == 1 {
			if decision.Owner != "sj" || !decision.Ready {
				t.Fatalf("mixed odd-epoch decision: %+v", decision)
			}
		} else if decision.Owner != "la" || decision.Ready {
			t.Fatalf("mixed even-epoch decision: %+v", decision)
		}
		select {
		case <-stop:
			writer.Wait()
			return
		default:
		}
	}
}
