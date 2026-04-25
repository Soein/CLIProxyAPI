package usage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

type stubStore struct {
	mu     sync.Mutex
	events []UsageEventRow
	deltas []UsageRollupDelta
}

func (s *stubStore) FlushBatch(ctx context.Context, e []UsageEventRow, d []UsageRollupDelta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e...)
	s.deltas = append(s.deltas, d...)
	return nil
}
func (s *stubStore) eventCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

type errStore struct{ count int }

func (e *errStore) FlushBatch(_ context.Context, _ []UsageEventRow, _ []UsageRollupDelta) error {
	e.count++
	return errors.New("simulated PG outage")
}

func TestPGSink_BatchSizeTriggersImmediateFlush(t *testing.T) {
	store := &stubStore{}
	sink := NewPGSink(store, "node-1", PGSinkOptions{
		BatchSize: 3, FlushInterval: time.Hour,
		MaxBufferEvents: 1000, MaxBufferRollups: 1000,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink.Start(ctx)
	defer sink.Stop(context.Background())

	rec := coreusage.Record{
		Provider: "p", Model: "m", APIKey: "k",
		RequestedAt: time.Now(),
		Detail:      coreusage.Detail{TotalTokens: 10},
	}
	for i := 0; i < 3; i++ {
		sink.HandleUsage(ctx, rec)
	}
	// Wait for async flush.
	deadline := time.Now().Add(2 * time.Second)
	for store.eventCount() < 3 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := store.eventCount(); got != 3 {
		t.Fatalf("want 3 flushed events, got %d", got)
	}
}

func TestPGSink_PeriodicFlush(t *testing.T) {
	store := &stubStore{}
	sink := NewPGSink(store, "node-1", PGSinkOptions{
		BatchSize: 10000, FlushInterval: 50 * time.Millisecond,
		MaxBufferEvents: 1000, MaxBufferRollups: 1000,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink.Start(ctx)
	defer sink.Stop(context.Background())

	rec := coreusage.Record{Provider: "p", Model: "m", APIKey: "k", RequestedAt: time.Now()}
	sink.HandleUsage(ctx, rec)
	// Wait for the periodic tick to drain.
	deadline := time.Now().Add(2 * time.Second)
	for store.eventCount() < 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := store.eventCount(); got != 1 {
		t.Fatalf("want 1 flushed event from tick, got %d", got)
	}
}

func TestPGSink_DropsOldestWhenBufferAtCap(t *testing.T) {
	sink := NewPGSink(&errStore{}, "node-1", PGSinkOptions{
		BatchSize: 5, FlushInterval: 10 * time.Millisecond,
		MaxBufferEvents: 10, MaxBufferRollups: 10,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink.Start(ctx)
	defer sink.Stop(context.Background())

	now := time.Now()
	for i := 0; i < 50; i++ {
		// Slightly different timestamps -> different dedup hashes
		// -> distinct rollup keys (different minute buckets) prevented
		// by Truncate(minute), so they land in same key. The events
		// buffer cap-eviction is what we're proving.
		rec := coreusage.Record{
			Provider: "p", Model: "m", APIKey: "k",
			RequestedAt: now.Add(time.Duration(i) * time.Microsecond),
			Detail:      coreusage.Detail{TotalTokens: int64(i + 1)},
		}
		sink.HandleUsage(ctx, rec)
	}
	// Allow flush attempts (all failing).
	time.Sleep(100 * time.Millisecond)
	if got := sink.DroppedCount(); got == 0 {
		t.Fatalf("expected drops > 0 with errStore at cap, got 0")
	}
}

func TestPGSink_StopDrainsRemainingEvents(t *testing.T) {
	store := &stubStore{}
	sink := NewPGSink(store, "n", PGSinkOptions{
		BatchSize: 10000, FlushInterval: time.Hour,
		MaxBufferEvents: 1000, MaxBufferRollups: 1000,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink.Start(ctx)

	rec := coreusage.Record{Provider: "p", Model: "m", APIKey: "k", RequestedAt: time.Now()}
	for i := 0; i < 5; i++ {
		sink.HandleUsage(ctx, rec)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	sink.Stop(stopCtx)

	if got := store.eventCount(); got != 5 {
		t.Fatalf("Stop should drain pending events; want 5, got %d", got)
	}
}

func TestPGSink_RollupMergesAcrossSameBucket(t *testing.T) {
	store := &stubStore{}
	sink := NewPGSink(store, "n", PGSinkOptions{
		BatchSize: 100, FlushInterval: 50 * time.Millisecond,
		MaxBufferEvents: 1000, MaxBufferRollups: 1000,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink.Start(ctx)
	defer sink.Stop(context.Background())

	now := time.Now().UTC().Truncate(time.Minute)
	for i := 0; i < 5; i++ {
		// All timestamps fall in the same minute bucket.
		rec := coreusage.Record{
			Provider: "p", Model: "m", APIKey: "k",
			RequestedAt: now.Add(time.Duration(i) * time.Second),
			Detail:      coreusage.Detail{TotalTokens: 10},
		}
		sink.HandleUsage(ctx, rec)
	}
	deadline := time.Now().Add(2 * time.Second)
	for store.eventCount() < 5 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.deltas) != 1 {
		t.Fatalf("expected 1 merged delta for same bucket, got %d", len(store.deltas))
	}
	d := store.deltas[0]
	if d.RequestCount != 5 || d.TotalTokens != 50 {
		t.Fatalf("expected 5 req / 50 tokens merged, got %+v", d)
	}
}
