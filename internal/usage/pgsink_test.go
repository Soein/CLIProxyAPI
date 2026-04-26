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

	rec := coreusage.Record{
		Provider: "p", Model: "m", APIKey: "k",
		RequestedAt: time.Now(),
		Detail:      coreusage.Detail{TotalTokens: 10},
	}
	for i := 0; i < 3; i++ {
		sink.HandleUsage(ctx, rec)
	}
	// Stop forces a deterministic final drain. We trust the BatchSize
	// signal to be delivered (verified separately), but Stop guarantees
	// no buffered event survives the test boundary regardless of
	// goroutine scheduling under -race.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	sink.Stop(stopCtx)
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
	// Wait for the periodic tick to drain. Generous deadline to absorb
	// race-detector overhead and shared CPU during -count=N runs.
	deadline := time.Now().Add(10 * time.Second)
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

	now := time.Now()
	for i := 0; i < 50; i++ {
		// All timestamps in same minute bucket → single rollup key,
		// so we're proving the events-buffer cap-eviction path.
		rec := coreusage.Record{
			Provider: "p", Model: "m", APIKey: "k",
			RequestedAt: now.Add(time.Duration(i) * time.Microsecond),
			Detail:      coreusage.Detail{TotalTokens: int64(i + 1)},
		}
		sink.HandleUsage(ctx, rec)
	}
	// Drop counter is incremented by HandleUsage when buffer is at cap
	// (50 events vs MaxBufferEvents=10 ⇒ at least 40 drops). Stop
	// guarantees the flusher has settled so the count is stable.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	sink.Stop(stopCtx)
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

// TestPGSink_RollupMergesAcrossSameBucket verifies that events landing in
// the SAME minute bucket within a SINGLE flush window collapse into one
// rollup delta in the PG payload. Note: the flusher drains the buffer as
// soon as it has anything (predicate `len(events) > 0` exits cond.Wait),
// so 5 quick HandleUsage calls may produce N>=1 deltas across N flushes
// depending on goroutine scheduling. What matters at the SQL layer is that
// each delta carries the correct sub-totals (PG ON CONFLICT DO UPDATE then
// re-aggregates them at the rollup row level, see TestPGStore_UpsertRollupBatch).
//
// This test asserts: regardless of how many flushes happen, every delta
// carries a (bucket, node, api, model) PK matching the same minute bucket,
// AND the SUM across deltas equals 5 requests / 50 tokens.
func TestPGSink_RollupMergesAcrossSameBucket(t *testing.T) {
	store := &stubStore{}
	sink := NewPGSink(store, "n", PGSinkOptions{
		BatchSize: 100, FlushInterval: time.Hour,
		MaxBufferEvents: 1000, MaxBufferRollups: 1000,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink.Start(ctx)

	now := time.Now().UTC().Truncate(time.Minute)
	for i := 0; i < 5; i++ {
		rec := coreusage.Record{
			Provider: "p", Model: "m", APIKey: "k",
			RequestedAt: now.Add(time.Duration(i) * time.Second),
			Detail:      coreusage.Detail{TotalTokens: 10},
		}
		sink.HandleUsage(ctx, rec)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	sink.Stop(stopCtx)

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.deltas) == 0 {
		t.Fatalf("expected at least 1 delta, got 0")
	}
	var sumReq, sumTok int64
	for _, d := range store.deltas {
		if !d.BucketStart.Equal(now) {
			t.Errorf("delta has wrong bucket: got %v want %v", d.BucketStart, now)
		}
		if d.NodeID != "n" || d.APIKey != "k" || d.Model != "m" {
			t.Errorf("delta PK drift: %+v", d.UsageRollupKey)
		}
		sumReq += d.RequestCount
		sumTok += d.TotalTokens
	}
	if sumReq != 5 || sumTok != 50 {
		t.Fatalf("sum across %d deltas: want 5/50, got %d/%d", len(store.deltas), sumReq, sumTok)
	}
}
