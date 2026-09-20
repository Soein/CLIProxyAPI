package helps

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestCodexDuplexLeaseIdempotentRelease(t *testing.T) {
	var count atomic.Int32
	release := func() {
		count.Add(1)
	}
	lease := NewCodexDuplexLease(release)
	if lease == nil {
		t.Fatal("expected non-nil lease")
	}

	const routines = 10
	var wg sync.WaitGroup
	wg.Add(routines)
	for i := 0; i < routines; i++ {
		go func() {
			defer wg.Done()
			lease.Release()
		}()
	}
	wg.Wait()

	if count.Load() != 1 {
		t.Fatalf("expected release called exactly once, got %d", count.Load())
	}
}

func TestCodexDuplexLeaseNilSafe(t *testing.T) {
	var nilLease *CodexDuplexLease
	nilLease.Release() // must not panic

	leaseWithNilCallback := NewCodexDuplexLease(nil)
	if leaseWithNilCallback != nil {
		t.Fatal("expected nil lease for nil callback")
	}
}

func TestCodexDuplexLeaseNilRetain(t *testing.T) {
	var nilLease *CodexDuplexLease
	if r := nilLease.Retain(); r != nil {
		t.Fatal("expected nil when retaining nil lease")
	}
}

func TestReleaseCodexDuplexLeases(t *testing.T) {
	var count1, count2 atomic.Int32
	l1 := NewCodexDuplexLease(func() { count1.Add(1) })
	l2 := NewCodexDuplexLease(func() { count2.Add(1) })

	leases := []*CodexDuplexLease{l1, nil, l2, l1}
	ReleaseCodexDuplexLeases(leases)

	if count1.Load() != 1 {
		t.Fatalf("expected count1 = 1, got %d", count1.Load())
	}
	if count2.Load() != 1 {
		t.Fatalf("expected count2 = 1, got %d", count2.Load())
	}

	// Calling again should not increment
	ReleaseCodexDuplexLeases(leases)
	if count1.Load() != 1 || count2.Load() != 1 {
		t.Fatalf("expected counts unchanged on second release, got count1=%d, count2=%d", count1.Load(), count2.Load())
	}
}

func TestCodexDuplexLeaseGroupNilAndEmpty(t *testing.T) {
	if g := NewCodexDuplexLeaseGroup(); g != nil {
		t.Fatal("expected nil group for empty args")
	}
	if g := NewCodexDuplexLeaseGroup(nil, nil); g != nil {
		t.Fatal("expected nil group for all-nil args")
	}
}

func TestCodexDuplexLeaseGroupSingle(t *testing.T) {
	var count atomic.Int32
	l := NewCodexDuplexLease(func() { count.Add(1) })
	g := NewCodexDuplexLeaseGroup(nil, l, nil)
	if g != l {
		t.Fatal("expected single non-nil lease returned directly")
	}
	g.Release()
	if count.Load() != 1 {
		t.Fatalf("expected count = 1, got %d", count.Load())
	}
}

func TestCodexDuplexLeaseGroupMultiple(t *testing.T) {
	var count1, count2 atomic.Int32
	l1 := NewCodexDuplexLease(func() { count1.Add(1) })
	l2 := NewCodexDuplexLease(func() { count2.Add(1) })

	g := NewCodexDuplexLeaseGroup(l1, nil, l2)
	if g == nil {
		t.Fatal("expected non-nil group")
	}

	const routines = 10
	var wg sync.WaitGroup
	wg.Add(routines)
	for i := 0; i < routines; i++ {
		go func() {
			defer wg.Done()
			g.Release()
		}()
	}
	wg.Wait()

	if count1.Load() != 1 {
		t.Fatalf("expected count1 = 1, got %d", count1.Load())
	}
	if count2.Load() != 1 {
		t.Fatalf("expected count2 = 1, got %d", count2.Load())
	}

	// Idempotent second release
	g.Release()
	if count1.Load() != 1 || count2.Load() != 1 {
		t.Fatalf("expected counts unchanged on repeated release, got count1=%d, count2=%d", count1.Load(), count2.Load())
	}
}

func TestCodexDuplexLeaseRetainDelaysRelease(t *testing.T) {
	var count atomic.Int32
	release := func() {
		count.Add(1)
	}
	l1 := NewCodexDuplexLease(release)
	if l1 == nil {
		t.Fatal("expected non-nil lease")
	}

	l2 := l1.Retain()
	if l2 == nil {
		t.Fatal("expected non-nil retained lease")
	}
	if l1 == l2 {
		t.Fatal("expected independent handle from Retain")
	}

	// Release l1: root callback must not be invoked yet because l2 is still live
	l1.Release()
	if count.Load() != 0 {
		t.Fatalf("expected release not called while retained handle is live, got %d", count.Load())
	}

	// Idempotent release of l1
	l1.Release()
	if count.Load() != 0 {
		t.Fatalf("expected release still not called on repeated l1 release, got %d", count.Load())
	}

	// Release l2: now root callback should be invoked exactly once
	l2.Release()
	if count.Load() != 1 {
		t.Fatalf("expected release called exactly once after all handles released, got %d", count.Load())
	}

	// Idempotent release of l2
	l2.Release()
	if count.Load() != 1 {
		t.Fatalf("expected count unchanged on repeated l2 release, got %d", count.Load())
	}
}

func TestCodexDuplexLeaseReleasedCannotBeRetained(t *testing.T) {
	var count atomic.Int32
	l := NewCodexDuplexLease(func() { count.Add(1) })
	l.Release()
	if count.Load() != 1 {
		t.Fatalf("expected count = 1, got %d", count.Load())
	}

	// Cannot retain an already released handle
	if r := l.Retain(); r != nil {
		t.Fatal("expected nil when retaining a released lease handle")
	}
	if count.Load() != 1 {
		t.Fatalf("expected count still = 1, got %d", count.Load())
	}
}

func TestCodexDuplexLeaseGroupRetain(t *testing.T) {
	var count1, count2 atomic.Int32
	l1 := NewCodexDuplexLease(func() { count1.Add(1) })
	l2 := NewCodexDuplexLease(func() { count2.Add(1) })

	g1 := NewCodexDuplexLeaseGroup(l1, l2)
	if g1 == nil {
		t.Fatal("expected non-nil group")
	}

	g2 := g1.Retain()
	if g2 == nil {
		t.Fatal("expected non-nil retained group")
	}

	// Releasing g1 must not release roots because g2 still holds them
	g1.Release()
	if count1.Load() != 0 || count2.Load() != 0 {
		t.Fatalf("expected roots not released while g2 is live, got count1=%d, count2=%d", count1.Load(), count2.Load())
	}

	// Cannot retain released group g1
	if r := g1.Retain(); r != nil {
		t.Fatal("expected nil when retaining released group")
	}

	// Releasing g2 releases the underlying roots
	g2.Release()
	if count1.Load() != 1 || count2.Load() != 1 {
		t.Fatalf("expected roots released after g2 released, got count1=%d, count2=%d", count1.Load(), count2.Load())
	}

	// Idempotent repeated release
	g2.Release()
	if count1.Load() != 1 || count2.Load() != 1 {
		t.Fatalf("expected counts unchanged on repeated g2 release, got count1=%d, count2=%d", count1.Load(), count2.Load())
	}
}

func TestCodexDuplexLeaseConcurrentRetainAndRelease(t *testing.T) {
	var count atomic.Int32
	root := NewCodexDuplexLease(func() { count.Add(1) })

	const numWorkers = 20
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	handles := make([]*CodexDuplexLease, numWorkers)
	for i := 0; i < numWorkers; i++ {
		handles[i] = root.Retain()
		if handles[i] == nil {
			t.Fatal("failed to retain handle")
		}
	}
	// Release the initial root handle
	root.Release()
	if count.Load() != 0 {
		t.Fatalf("expected root not released while worker handles live, got %d", count.Load())
	}

	for i := 0; i < numWorkers; i++ {
		h := handles[i]
		go func() {
			defer wg.Done()
			// Each worker may retain and release
			sub := h.Retain()
			h.Release()
			if sub != nil {
				sub.Release()
			}
		}()
	}
	wg.Wait()

	if count.Load() != 1 {
		t.Fatalf("expected root callback called exactly once, got %d", count.Load())
	}
}

func TestCodexDuplexLeaseGroupRepeatedFanout(t *testing.T) {
	var count1, count2 atomic.Int32
	l1 := NewCodexDuplexLease(func() { count1.Add(1) })
	l2 := NewCodexDuplexLease(func() { count2.Add(1) })

	current := NewCodexDuplexLeaseGroup(l1, l2)
	const rounds = 100
	for i := 0; i < rounds; i++ {
		r1 := current.Retain()
		r2 := current.Retain()
		if r1 == nil || r2 == nil {
			t.Fatalf("round %d: failed to retain", i)
		}
		current.Release()
		current = NewCodexDuplexLeaseGroup(r1, r2)
	}

	if count1.Load() != 0 || count2.Load() != 0 {
		t.Fatalf("underlying callbacks called prematurely: count1=%d, count2=%d", count1.Load(), count2.Load())
	}

	current.Release()
	if count1.Load() != 1 || count2.Load() != 1 {
		t.Fatalf("expected underlying callbacks called exactly once, got count1=%d, count2=%d", count1.Load(), count2.Load())
	}
}
