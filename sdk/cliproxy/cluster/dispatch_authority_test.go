package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

type fakeDispatchLease struct {
	instanceID      string
	membershipEpoch int64
	ownerEpoch      int64
	leaseUntil      time.Time
}

type fakeDispatchBackend struct {
	mu       sync.Mutex
	now      time.Time
	epoch    int64
	leases   map[string]fakeDispatchLease
	omit     map[string]bool
	fail     bool
	delay    time.Duration
	calls    atomic.Int64
	releases atomic.Int64
}

type cancelAfterAcquireBackend struct {
	inner            *fakeDispatchBackend
	acquired         chan struct{}
	acquiredOnce     sync.Once
	canceledReleases atomic.Int32
	validReleases    atomic.Int32
}

func (b *cancelAfterAcquireBackend) acquire(ctx context.Context, request dispatchLeaseRequest) (dispatchLeaseResult, error) {
	result, err := b.inner.acquire(context.Background(), request)
	b.acquiredOnce.Do(func() { close(b.acquired) })
	<-ctx.Done()
	return result, err
}

func (b *cancelAfterAcquireBackend) release(ctx context.Context, keys []dispatchLeaseKey) error {
	if err := ctx.Err(); err != nil {
		b.canceledReleases.Add(1)
		return err
	}
	b.validReleases.Add(1)
	return b.inner.release(ctx, keys)
}

func newFakeDispatchBackend(epoch int64, now time.Time) *fakeDispatchBackend {
	return &fakeDispatchBackend{now: now, epoch: epoch, leases: make(map[string]fakeDispatchLease)}
}

func (b *fakeDispatchBackend) acquire(_ context.Context, request dispatchLeaseRequest) (dispatchLeaseResult, error) {
	b.calls.Add(1)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fail {
		return dispatchLeaseResult{}, errors.New("simulated database failure")
	}
	operationNow := b.now
	result := dispatchLeaseResult{currentEpoch: b.epoch}
	if request.membershipEpoch != b.epoch {
		b.now = operationNow.Add(b.delay)
		result.databaseNow = b.now
		return result, nil
	}
	for _, authID := range request.authIDs {
		lease, exists := b.leases[authID]
		sameInstance := exists && lease.instanceID == request.instanceID
		sameOwner := sameInstance && lease.membershipEpoch == request.membershipEpoch
		if exists && !sameInstance && lease.leaseUntil.After(operationNow) {
			continue
		}
		ownerEpoch := int64(1)
		if exists {
			ownerEpoch = lease.ownerEpoch
			if !sameOwner {
				ownerEpoch++
			}
		}
		lease = fakeDispatchLease{
			instanceID:      request.instanceID,
			membershipEpoch: request.membershipEpoch,
			ownerEpoch:      ownerEpoch,
			leaseUntil:      operationNow.Add(request.ttl),
		}
		b.leases[authID] = lease
		if b.omit[authID] {
			continue
		}
		result.grants = append(result.grants, dispatchLeaseGrant{
			authID:          authID,
			membershipEpoch: lease.membershipEpoch,
			ownerEpoch:      lease.ownerEpoch,
			leaseUntil:      lease.leaseUntil,
		})
	}
	b.now = operationNow.Add(b.delay)
	result.databaseNow = b.now
	return result, nil
}

func (b *fakeDispatchBackend) release(_ context.Context, keys []dispatchLeaseKey) error {
	b.releases.Add(1)
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, key := range keys {
		lease, ok := b.leases[key.authID]
		if ok && lease.instanceID == key.instanceID && lease.ownerEpoch == key.ownerEpoch {
			lease.leaseUntil = b.now
			b.leases[key.authID] = lease
		}
	}
	return nil
}

func newTestAuthority(t *testing.T, nodeID string, ring *AuthRing, backend *fakeDispatchBackend, now *time.Time, authIDs ...string) *PgDispatchAuthority {
	t.Helper()
	authority, err := newPgDispatchAuthority(PgDispatchAuthorityConfig{
		NodeID:        nodeID,
		Ring:          ring,
		RingStaleness: 30 * time.Second,
		AuthIDs:       func() []string { return append([]string(nil), authIDs...) },
		TimeSource:    func() time.Time { return *now },
		SafetyGuard:   500 * time.Millisecond,
	}, backend, false)
	if err != nil {
		t.Fatalf("new authority: %v", err)
	}
	t.Cleanup(func() { _ = authority.Close(context.Background()) })
	return authority
}

func TestPgDispatchAuthorityTwoCoordinatorsCannotAdmitSameOwnerEpoch(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	ring := NewAuthRing("node-a")
	ring.RebuildAt(4, []RingMember{{NodeID: "node-a", Weight: 100}})
	backend := newFakeDispatchBackend(4, now)
	a := newTestAuthority(t, "node-a", ring, backend, &now, "auth-a")
	b := newTestAuthority(t, "node-a", ring, backend, &now, "auth-a")

	if err := a.syncOnce(context.Background()); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	if err := b.syncOnce(context.Background()); !errors.Is(err, errDispatchReconciliationIncomplete) {
		t.Fatalf("second sync error = %v, want incomplete reconciliation", err)
	}
	if _, ok := a.Admit("auth-a"); !ok {
		t.Fatal("lease holder must admit")
	}
	if _, ok := b.Admit("auth-a"); ok {
		t.Fatal("second coordinator must not admit the same auth")
	}
}

func TestPgDispatchAuthorityUsesFreshInstanceAndBoundedLeaseTiming(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	ring := NewAuthRing("node-a")
	ring.RebuildAt(1, []RingMember{{NodeID: "node-a", Weight: 100}})
	backend := newFakeDispatchBackend(1, now)
	first := newTestAuthority(t, "node-a", ring, backend, &now)
	second := newTestAuthority(t, "node-a", ring, backend, &now)
	if first.instanceID == second.instanceID {
		t.Fatal("each authority startup must use a fresh instance UUID")
	}
	if _, err := uuid.Parse(first.instanceID); err != nil {
		t.Fatalf("instance identity is not a UUID: %v", err)
	}
	if first.ttl != 15*time.Second || first.interval != 5*time.Second || first.safetyGuard < 500*time.Millisecond {
		t.Fatalf("lease timing ttl=%s interval=%s guard=%s", first.ttl, first.interval, first.safetyGuard)
	}
}

func TestPgDispatchAuthorityMigratesSameInstanceToNewEpochInOneSync(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	ring := NewAuthRing("node-a")
	ring.RebuildAt(10, []RingMember{{NodeID: "node-a", Weight: 100}})
	backend := newFakeDispatchBackend(10, now)
	authority := newTestAuthority(t, "node-a", ring, backend, &now, "auth-a")
	if err := authority.syncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	oldGrant := authority.snapshot.Load().grants["auth-a"]

	ring.RebuildAt(11, []RingMember{{NodeID: "node-a", Weight: 100}})
	backend.epoch = 11
	if _, ok := authority.Admit("auth-a"); ok {
		t.Fatal("old ring epoch must stop new dispatch immediately")
	}
	if err := authority.syncOnce(context.Background()); err != nil {
		t.Fatalf("single epoch-handoff sync: %v", err)
	}
	if !authority.Ready() {
		t.Fatal("single same-instance epoch handoff must reconcile readiness")
	}
	if _, ok := authority.Admit("auth-a"); !ok {
		t.Fatal("single same-instance epoch handoff must restore admission")
	}
	newGrant := authority.snapshot.Load().grants["auth-a"]
	if newGrant.ownerEpoch <= oldGrant.ownerEpoch {
		t.Fatalf("owner epoch did not advance across membership epoch: old=%d new=%d", oldGrant.ownerEpoch, newGrant.ownerEpoch)
	}
	if err := backend.release(context.Background(), []dispatchLeaseKey{{authID: "auth-a", instanceID: authority.instanceID, ownerEpoch: oldGrant.ownerEpoch}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := authority.Admit("auth-a"); !ok {
		t.Fatal("stale same-instance release cut off the new owner epoch")
	}
}

func TestPgDispatchAuthorityCloseRetriesExactReleaseAfterCanceledInFlightSync(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	ring := NewAuthRing("node-a")
	ring.RebuildAt(9, []RingMember{{NodeID: "node-a", Weight: 100}})
	inner := newFakeDispatchBackend(9, now)
	backend := &cancelAfterAcquireBackend{inner: inner, acquired: make(chan struct{})}
	authority, err := newPgDispatchAuthority(PgDispatchAuthorityConfig{
		NodeID:        "node-a",
		Ring:          ring,
		RingStaleness: 30 * time.Second,
		AuthIDs:       func() []string { return []string{"auth-a"} },
		TimeSource:    func() time.Time { return now },
		SafetyGuard:   500 * time.Millisecond,
	}, backend, true)
	if err != nil {
		t.Fatalf("new authority: %v", err)
	}
	select {
	case <-backend.acquired:
	case <-time.After(time.Second):
		t.Fatal("background reconciliation did not acquire the lease")
	}

	authority.CloseAdmissions()
	ring.FailClosed()
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = authority.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if backend.canceledReleases.Load() == 0 {
		t.Fatal("test did not exercise the canceled in-flight release")
	}
	if backend.validReleases.Load() == 0 {
		t.Fatal("Close did not retry exact release with its valid context")
	}
	inner.mu.Lock()
	lease := inner.leases["auth-a"]
	leaseActive := lease.leaseUntil.After(inner.now)
	inner.mu.Unlock()
	if leaseActive {
		t.Fatalf("Close left acquired lease active: %+v", lease)
	}
}

func TestPgDispatchAuthorityFailsClosedWhenDatabaseEpochLagsRing(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	ring := NewAuthRing("node-a")
	ring.RebuildAt(8, []RingMember{{NodeID: "node-a", Weight: 100}})
	backend := newFakeDispatchBackend(7, now)
	authority := newTestAuthority(t, "node-a", ring, backend, &now, "auth-a")

	if err := authority.syncOnce(context.Background()); err == nil {
		t.Fatal("database epoch behind the ring must make reconciliation incomplete")
	}
	if authority.Ready() {
		t.Fatal("authority must not become ready against a stale database epoch")
	}
	if _, ok := authority.Admit("auth-a"); ok {
		t.Fatal("authority must fail closed against a stale database epoch")
	}
	if len(backend.leases) != 0 {
		t.Fatalf("stale database epoch created leases: %#v", backend.leases)
	}
}

func TestPgDispatchAuthorityStaleReleaseDoesNotCutOffNewOwner(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	ring := NewAuthRing("node-a")
	ring.RebuildAt(3, []RingMember{{NodeID: "node-a", Weight: 100}})
	backend := newFakeDispatchBackend(3, now)
	old := newTestAuthority(t, "node-a", ring, backend, &now, "auth-a")
	newOwner := newTestAuthority(t, "node-a", ring, backend, &now, "auth-a")
	if err := old.syncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	oldGrant := old.snapshot.Load().grants["auth-a"]
	now = now.Add(16 * time.Second)
	backend.now = now
	if err := newOwner.syncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := backend.release(context.Background(), []dispatchLeaseKey{{authID: "auth-a", instanceID: old.instanceID, ownerEpoch: oldGrant.ownerEpoch}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := newOwner.Admit("auth-a"); !ok {
		t.Fatal("stale release removed the newer owner")
	}
}

func TestPgDispatchAuthoritySlowResponseUsesConservativeDeadline(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	ring := NewAuthRing("node-a")
	ring.RebuildAt(2, []RingMember{{NodeID: "node-a", Weight: 100}})
	backend := newFakeDispatchBackend(2, now)
	backend.delay = 14 * time.Second
	authority := newTestAuthority(t, "node-a", ring, backend, &now, "auth-a")
	if err := authority.syncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(14500 * time.Millisecond)
	if _, ok := authority.Admit("auth-a"); ok {
		t.Fatal("slow acquire response must not create an optimistic local deadline")
	}
}

func TestPgDispatchAuthorityRenewFailureExpiresWithoutExtension(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	ring := NewAuthRing("node-a")
	ring.RebuildAt(2, []RingMember{{NodeID: "node-a", Weight: 100}})
	backend := newFakeDispatchBackend(2, now)
	authority := newTestAuthority(t, "node-a", ring, backend, &now, "auth-a")
	if err := authority.syncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend.fail = true
	now = now.Add(5 * time.Second)
	if err := authority.syncOnce(context.Background()); err == nil {
		t.Fatal("renew failure must surface")
	}
	now = now.Add(10 * time.Second)
	if _, ok := authority.Admit("auth-a"); ok {
		t.Fatal("failed renewal must not extend the old grant")
	}
}

func TestPgDispatchAuthorityPartialGrantIsNotReadyAndPreservesOldDeadline(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	ring := NewAuthRing("node-a")
	ring.RebuildAt(2, []RingMember{{NodeID: "node-a", Weight: 100}})
	backend := newFakeDispatchBackend(2, now)
	authority := newTestAuthority(t, "node-a", ring, backend, &now, "auth-a", "auth-b")
	if err := authority.syncOnce(context.Background()); err != nil {
		t.Fatalf("initial full reconciliation: %v", err)
	}
	oldGrant := authority.snapshot.Load().grants["auth-b"]

	backend.omit = map[string]bool{"auth-b": true}
	now = now.Add(time.Second)
	backend.now = now
	if err := authority.syncOnce(context.Background()); !errors.Is(err, errDispatchReconciliationIncomplete) {
		t.Fatalf("partial grant error = %v, want incomplete reconciliation", err)
	}
	if authority.Ready() {
		t.Fatal("partial grant response must not mark the authority ready")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := authority.WaitReady(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitReady partial reconciliation error = %v, want deadline exceeded", err)
	}
	retained, ok := authority.snapshot.Load().grants["auth-b"]
	if !ok {
		t.Fatal("partial renewal dropped an existing unexpired grant")
	}
	if !retained.localDeadline.Equal(oldGrant.localDeadline) || !retained.databaseDeadline.Equal(oldGrant.databaseDeadline) {
		t.Fatalf("partial renewal extended old deadline: old=%+v retained=%+v", oldGrant, retained)
	}
	if release, admitted := authority.Admit("auth-b"); !admitted {
		t.Fatal("existing unexpired grant should continue admitting while Ready=false")
	} else {
		release()
	}
}

func TestPgDispatchAuthorityConcurrentAdmitHasNoDatabaseRTT(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	ring := NewAuthRing("node-a")
	ring.RebuildAt(6, []RingMember{{NodeID: "node-a", Weight: 100}})
	backend := newFakeDispatchBackend(6, now)
	authIDs := make([]string, 1000)
	for i := range authIDs {
		authIDs[i] = fmt.Sprintf("auth-%04d", i)
	}
	authority := newTestAuthority(t, "node-a", ring, backend, &now, authIDs...)
	if err := authority.syncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	callsBefore := backend.calls.Load()
	if callsBefore != 2 {
		t.Fatalf("1000 auths used %d acquire batches, want 2 (batch size 512)", callsBefore)
	}
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(authID string) {
			defer wg.Done()
			release, ok := authority.Admit(authID)
			if !ok {
				t.Errorf("Admit(%q) rejected", authID)
				return
			}
			release()
		}(authIDs[i])
	}
	wg.Wait()
	if got := backend.calls.Load(); got != callsBefore {
		t.Fatalf("Admit performed database calls: before=%d after=%d", callsBefore, got)
	}
}

func TestPgDispatchAuthorityWaitReadyTracksCurrentRingEpoch(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	ring := NewAuthRing("node-a")
	ring.RebuildAt(1, []RingMember{{NodeID: "node-a", Weight: 100}})
	backend := newFakeDispatchBackend(1, now)
	authority := newTestAuthority(t, "node-a", ring, backend, &now, "auth-a")
	if err := authority.syncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := authority.WaitReady(context.Background()); err != nil {
		t.Fatalf("WaitReady current epoch: %v", err)
	}

	ring.RebuildAt(2, []RingMember{{NodeID: "node-a", Weight: 100}})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := authority.WaitReady(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitReady stale snapshot error = %v, want deadline", err)
	}
	backend.epoch = 2
	now = now.Add(16 * time.Second)
	backend.now = now
	if err := authority.syncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := authority.WaitReady(context.Background()); err != nil {
		t.Fatalf("WaitReady new epoch: %v", err)
	}
}

func TestPgDispatchAuthorityCloseAdmissionsIsImmediate(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	ring := NewAuthRing("node-a")
	ring.RebuildAt(1, []RingMember{{NodeID: "node-a", Weight: 100}})
	backend := newFakeDispatchBackend(1, now)
	authority := newTestAuthority(t, "node-a", ring, backend, &now, "auth-a")
	if err := authority.syncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	authority.CloseAdmissions()
	if _, ok := authority.Admit("auth-a"); ok {
		t.Fatal("CloseAdmissions must synchronously stop new admissions")
	}
}
