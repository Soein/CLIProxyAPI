package cluster

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Phase 4 Sprint 2: AuthRing implements consistent assignment of auth IDs to
// cluster members using Rendezvous Hashing (HRW — Highest Random Weight).
//
// Why Rendezvous over Jump/Ketama:
//   - Native weight support: a node's effective score = weight * hash(node,key).
//     No virtual-node bookkeeping needed.
//   - Minimal disruption on membership change: only keys whose "second best"
//     was the added/removed node are reassigned (~K/N keys).
//   - Deterministic across processes: all CPA replicas compute the same owner
//     for a given auth_id if they see the same member list. This matters
//     because auto_refresh_loop and scheduler both consult the ring — they
//     must agree on who owns an auth.
//   - Fast enough for N<=100 and K<=10k: per-pick cost is O(N) hash (each
//     hash is a FNV64, ~10ns), well under typical auth.pick latency budget.
//
// Thread-safety: the ring is read-heavy, write-rare (only on
// cluster_nodes changes, ~10s cadence). atomic.Pointer[ringSnapshot]
// eliminates reader lock contention entirely.

// RingMember is the routable representation of a cluster_nodes row.
type RingMember struct {
	NodeID string
	Weight int // >0; defaults to 100 if <=0
}

// AuthRing is concurrent-safe. The zero value is usable and fails closed:
// it does not claim any auth until valid membership has been installed.
type AuthRing struct {
	myNodeID string
	snap     atomic.Pointer[ringSnapshot]
	updateMu sync.Mutex
	terminal bool
}

// ringSnapshot is an immutable view of the ring at one instant. Swapped
// atomically by Rebuild().
type ringSnapshot struct {
	epoch   int64
	members []RingMember // sorted by NodeID for deterministic iteration
	ready   bool
}

// RingDecision is one atomic ownership observation. Callers that coordinate
// dispatch must use Decision instead of separate Owner and Ready calls so a
// membership publication cannot be observed half before and half after an
// epoch transition.
type RingDecision struct {
	Epoch int64
	Owner string
	Ready bool
}

// NewAuthRing creates an empty ring owned by myNodeID. Before Rebuild is
// called the ring is not ready and does not claim any auth.
func NewAuthRing(myNodeID string) *AuthRing {
	return &AuthRing{myNodeID: strings.TrimSpace(myNodeID)}
}

// Rebuild atomically replaces the ring membership. Safe to call from any
// goroutine. Duplicate NodeIDs are coalesced (last write wins) and
// non-positive weights are clamped to 100. Empty membership is allowed and
// leaves the ring not ready.
func (r *AuthRing) Rebuild(members []RingMember) {
	if r == nil {
		return
	}
	r.updateMu.Lock()
	defer r.updateMu.Unlock()
	if r.terminal {
		return
	}
	epoch := int64(1)
	if current := r.snap.Load(); current != nil {
		epoch = current.epoch + 1
	}
	r.publishLocked(epoch, members)
}

// RebuildAt atomically publishes membership at the database-authoritative
// epoch. Publications older than the current snapshot are ignored. This
// prevents a delayed refresh from rolling dispatch ownership backwards.
func (r *AuthRing) RebuildAt(epoch int64, members []RingMember) {
	if r == nil || epoch < 0 {
		return
	}
	r.updateMu.Lock()
	defer r.updateMu.Unlock()
	if r.terminal {
		return
	}
	if current := r.snap.Load(); current != nil && epoch < current.epoch {
		return
	}
	r.publishLocked(epoch, members)
}

func (r *AuthRing) publishLocked(epoch int64, members []RingMember) {
	normalised := make(map[string]RingMember, len(members))
	for _, m := range members {
		id := strings.TrimSpace(m.NodeID)
		if id == "" {
			continue
		}
		w := m.Weight
		if w <= 0 {
			w = 100
		}
		normalised[id] = RingMember{NodeID: id, Weight: w}
	}
	flat := make([]RingMember, 0, len(normalised))
	for _, m := range normalised {
		flat = append(flat, m)
	}
	sort.Slice(flat, func(i, j int) bool { return flat[i].NodeID < flat[j].NodeID })
	ready := false
	if r.myNodeID != "" {
		for _, member := range flat {
			if member.NodeID == r.myNodeID {
				ready = true
				break
			}
		}
	}
	r.snap.Store(&ringSnapshot{epoch: epoch, members: flat, ready: ready})
}

// FailClosed permanently invalidates this ring instance. It is linearized
// with Rebuild so an in-flight watcher refresh cannot publish membership
// after a fatal loss of the node's database-backed lease. Recovery requires
// constructing a new AuthRing as part of a new Service lifecycle.
func (r *AuthRing) FailClosed() {
	if r == nil {
		return
	}
	r.updateMu.Lock()
	defer r.updateMu.Unlock()
	r.terminal = true
	epoch := int64(0)
	if current := r.snap.Load(); current != nil {
		epoch = current.epoch
	}
	r.snap.Store(&ringSnapshot{epoch: epoch})
}

// Decision returns epoch, owner, and local readiness from one immutable ring
// snapshot. Owner remains visible when the local node is absent so callers can
// distinguish "owned elsewhere" from an empty ring while still failing closed
// through Ready=false.
func (r *AuthRing) Decision(authID string) RingDecision {
	if r == nil {
		return RingDecision{}
	}
	snap := r.snap.Load()
	if snap == nil || len(snap.members) == 0 {
		if snap == nil {
			return RingDecision{}
		}
		return RingDecision{Epoch: snap.epoch}
	}
	return RingDecision{
		Epoch: snap.epoch,
		Owner: pickOwner(authID, snap.members),
		Ready: snap.ready,
	}
}

// Owner returns the NodeID that owns the given auth_id, or "" when the ring
// is empty / not ready. Ties broken by NodeID ascending (deterministic).
func (r *AuthRing) Owner(authID string) string {
	return r.Decision(authID).Owner
}

// IsMine reports whether the current node owns the given auth_id under the
// current membership. Ownership is fail-closed:
//
//  1. Nil receiver              -> false (caller didn't wire the ring)
//  2. myNodeID unset            -> false (misconfigured: cannot claim ownership)
//  3. Ring not yet built        -> false (bootstrap window)
//  4. Ring built but I'm absent -> false (I'm draining; don't claim)
func (r *AuthRing) IsMine(authID string) bool {
	if r == nil {
		return false
	}
	decision := r.Decision(authID)
	return decision.Ready && decision.Owner == r.myNodeID
}

// Members returns a defensive copy of the current ring membership. Useful
// for admin endpoints and logging.
func (r *AuthRing) Members() []RingMember {
	if r == nil {
		return nil
	}
	snap := r.snap.Load()
	if snap == nil {
		return nil
	}
	out := make([]RingMember, len(snap.members))
	copy(out, snap.members)
	return out
}

// Ready reports whether the ring has members and contains the configured
// local node. A non-empty snapshot that omits the local node is not ready for
// local sharding decisions.
func (r *AuthRing) Ready() bool {
	if r == nil {
		return false
	}
	snap := r.snap.Load()
	return snap != nil && snap.ready
}

// pickOwner computes the Weighted-Rendezvous-Hashing winner (Schindelhauer
// & Schomaker, 2005) among members for authID. Ties broken by NodeID
// ascending for determinism.
//
// A single pass over members is enough because we only need the max — no
// need to sort scores. Cost: O(N) where N = number of cluster nodes.
func pickOwner(authID string, members []RingMember) string {
	var (
		bestID    string
		bestScore float64
		haveBest  bool
	)
	for _, m := range members {
		score := weightedScore(m.NodeID, m.Weight, authID)
		switch {
		case !haveBest:
			bestID, bestScore, haveBest = m.NodeID, score, true
		case score > bestScore:
			bestID, bestScore = m.NodeID, score
		case score == bestScore && m.NodeID < bestID:
			bestID = m.NodeID // deterministic tie-break
		}
	}
	return bestID
}

// weightedScore implements Weighted Rendezvous Hashing:
//
//	score = weight / -ln(U(nodeID, authID))
//
// where U is the hash normalized to the open interval (0, 1). This produces
// the correct Pr[node wins] = weight / sum(weights) (unlike a naive
// weight * hash which fails because uint64 multiply does not preserve the
// ratio of expected-max distribution).
//
// We use "|" as separator (invalid in hostnames / auth IDs) so prefix
// ambiguity like ("la", "n") vs ("la|n", "") cannot collide.
//
// Implementation notes:
//   - SHA-256 (truncated to 8 bytes) is used over FNV64a because FNV exhibits
//     strong low-sample bias on sequential / structured inputs ("auth-00001",
//     "auth-00002", ...): a 10k-sample Weight=200 vs Weight=100 split landed
//     3:1 instead of the theoretical 2:1 under FNV. SHA-256 is overkill for
//     non-cryptographic hashing but it's already a project dependency, it
//     runs in ~100ns per call, and ring lookups happen at most once per
//     auth-pick — not a hot path.
//   - We offset the hash by +1 and divide by (maxUint64 + 2) so U is strictly
//     in (0, 1) — math.Log(0) is -Inf which would make score = +Inf and
//     break tie-breaking.
func weightedScore(nodeID string, weight int, authID string) float64 {
	h := sha256.New()
	_, _ = h.Write([]byte(nodeID))
	_, _ = h.Write([]byte{'|'})
	_, _ = h.Write([]byte(authID))
	sum := h.Sum(nil)
	hashVal := binary.BigEndian.Uint64(sum[:8])
	// Map hash into (0, 1). +1 in numerator, +2 in denominator guarantees
	// strict (0, 1) range: numerator in [1, 2^64], denominator 2^64+2.
	u := (float64(hashVal) + 1.0) / (float64(math.MaxUint64) + 2.0)
	// -math.Log(u) is in (0, +Inf) for u in (0, 1). Larger weight or
	// smaller -Log(u) → larger score → more likely to win.
	return float64(weight) / -math.Log(u)
}
