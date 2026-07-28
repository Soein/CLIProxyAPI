package usage

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestPhaseTrackerCapturesSelectionAndAttemptSnapshots(t *testing.T) {
	base := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	now := base
	tracker := newPhaseTracker(func() time.Time { return now })

	now = base.Add(10 * time.Millisecond)
	selection := tracker.BeginAuthSelection()
	tracker.MarkAffinityOutcome(" HIT ")
	now = base.Add(17 * time.Millisecond)
	selection.Complete()
	selection.Complete()

	now = base.Add(20 * time.Millisecond)
	first := tracker.BeginAttempt()
	if first.Attempt != 1 || first.RequestElapsedToUpstreamStart != 20*time.Millisecond {
		t.Fatalf("first attempt = %+v", first)
	}
	if first.AuthSelection != 7*time.Millisecond || first.AffinityOutcome != AffinityOutcomeHit {
		t.Fatalf("first selection snapshot = %+v", first)
	}

	now = base.Add(25 * time.Millisecond)
	secondSelection := tracker.BeginAuthSelection()
	now = base.Add(28 * time.Millisecond)
	secondSelection.Complete()
	now = base.Add(30 * time.Millisecond)
	second := tracker.BeginAttempt()
	if second.Attempt != 2 || second.AuthSelection != 3*time.Millisecond {
		t.Fatalf("second attempt = %+v", second)
	}
	if second.AffinityOutcome != AffinityOutcomeNone {
		t.Fatalf("second affinity outcome = %q, want none", second.AffinityOutcome)
	}
}

func TestPhaseTrackerAffinityOutcomeIsBounded(t *testing.T) {
	tracker := NewPhaseTracker()
	tracker.MarkAffinityOutcome("unexpected-high-cardinality-value")
	if got := tracker.BeginAttempt().AffinityOutcome; got != AffinityOutcomeNone {
		t.Fatalf("affinity outcome = %q, want none", got)
	}
}

func TestEnablePhasesPreservesExistingTracker(t *testing.T) {
	ctx := EnablePhases(context.Background())
	first := PhaseTrackerFromContext(ctx)
	if first == nil {
		t.Fatal("PhaseTrackerFromContext() = nil")
	}
	secondCtx := EnablePhases(ctx)
	if got := PhaseTrackerFromContext(secondCtx); got != first {
		t.Fatal("EnablePhases() replaced existing tracker")
	}
}

func TestPhaseTrackerBeginAttemptConcurrentOrdinals(t *testing.T) {
	tracker := NewPhaseTracker()
	const count = 64
	results := make(chan int, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- tracker.BeginAttempt().Attempt
		}()
	}
	wg.Wait()
	close(results)

	seen := make(map[int]bool, count)
	for attempt := range results {
		seen[attempt] = true
	}
	if len(seen) != count || !seen[1] || !seen[count] {
		t.Fatalf("attempt ordinals = %v", seen)
	}
}
