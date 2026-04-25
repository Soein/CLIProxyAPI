// Package usage — PG-backed plugin sink.
//
// PGSink is a coreusage.Plugin that buffers usage records in memory and
// flushes them to PG every PGSinkOptions.FlushInterval (or sooner if either
// buffer reaches BatchSize). It is registered alongside the legacy
// in-memory plugin (LoggerPlugin), not in place of it — when
// usage.backend=dual, both run; when usage.backend=pg, the in-memory plugin
// still serves as a hot RPM/TPM cache.
//
// Registration is via AttachPGSink (attach.go), called from cluster_bootstrap
// once the PG handle is known. Never register from init() — the import
// graph cannot reason about whether PG is available at init time.
package usage

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

// PGSinkOptions tunes the buffer/flush behavior. All fields have sane
// defaults applied by AttachPGSink — callers may pass a zero-value struct.
type PGSinkOptions struct {
	BatchSize        int           // events/rollups that trigger an immediate flush
	FlushInterval    time.Duration // periodic flush even when below BatchSize
	MaxBufferEvents  int           // hard cap on event buffer; oldest dropped past this
	MaxBufferRollups int           // hard cap on rollup keys; oldest dropped past this
}

// flushStore is the subset of PGStore that PGSink needs. Lets tests stub
// the storage layer with a synchronous in-process recorder.
type flushStore interface {
	FlushBatch(ctx context.Context, events []UsageEventRow, deltas []UsageRollupDelta) error
}

// PGSink implements coreusage.Plugin and is the bridge from the runtime's
// per-request usage records into PG batched writes.
type PGSink struct {
	store  flushStore
	nodeID string
	opts   PGSinkOptions

	mu     sync.Mutex
	cond   *sync.Cond
	events []UsageEventRow
	rollup map[UsageRollupKey]*UsageRollupDelta

	startOnce sync.Once
	stopOnce  sync.Once
	closed    bool
	done      chan struct{}

	dropped atomic.Int64
}

// NewPGSink constructs a PGSink. It does NOT start the background flusher —
// callers must invoke Start(ctx). This split keeps the constructor cheap so
// tests can build sinks without spawning goroutines.
func NewPGSink(store flushStore, nodeID string, opts PGSinkOptions) *PGSink {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 1000
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = 10 * time.Second
	}
	if opts.MaxBufferEvents <= 0 {
		opts.MaxBufferEvents = 50000
	}
	if opts.MaxBufferRollups <= 0 {
		opts.MaxBufferRollups = 10000
	}
	s := &PGSink{
		store:  store,
		nodeID: nodeID,
		opts:   opts,
		rollup: make(map[UsageRollupKey]*UsageRollupDelta),
		done:   make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// HandleUsage implements coreusage.Plugin. Synchronous, lock-bounded:
// appends to buffer, signals flusher if BatchSize reached. Hot-path —
// must be O(1) under contention.
func (s *PGSink) HandleUsage(ctx context.Context, record coreusage.Record) {
	if s == nil {
		return
	}
	row, delta := s.translate(record)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}

	// Buffer event with cap-based eviction.
	if len(s.events) >= s.opts.MaxBufferEvents {
		// drop oldest; record drop counter
		s.events = s.events[1:]
		s.dropped.Add(1)
	}
	s.events = append(s.events, row)

	// Merge rollup delta with cap-based eviction.
	if existing, ok := s.rollup[delta.UsageRollupKey]; ok {
		existing.RequestCount += delta.RequestCount
		existing.SuccessCount += delta.SuccessCount
		existing.FailureCount += delta.FailureCount
		existing.InputTokens += delta.InputTokens
		existing.OutputTokens += delta.OutputTokens
		existing.ReasoningTokens += delta.ReasoningTokens
		existing.CachedTokens += delta.CachedTokens
		existing.TotalTokens += delta.TotalTokens
		existing.LatencyMsSum += delta.LatencyMsSum
	} else {
		if len(s.rollup) >= s.opts.MaxBufferRollups {
			// drop one arbitrary key (Go map iteration order)
			for k := range s.rollup {
				delete(s.rollup, k)
				s.dropped.Add(1)
				break
			}
		}
		d := delta
		s.rollup[delta.UsageRollupKey] = &d
	}

	if len(s.events) >= s.opts.BatchSize || len(s.rollup) >= s.opts.BatchSize {
		s.cond.Signal()
	}
}

// Start launches the background flusher. Idempotent.
func (s *PGSink) Start(ctx context.Context) {
	if s == nil {
		return
	}
	s.startOnce.Do(func() {
		go s.flushLoop(ctx)
	})
}

// Stop signals the flusher to exit, waits for it to drain, then returns.
// Drains the buffer with one final flush so no events are lost on graceful
// shutdown. Idempotent.
func (s *PGSink) Stop(ctx context.Context) {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.cond.Broadcast()
		s.mu.Unlock()
		select {
		case <-s.done:
		case <-ctx.Done():
			log.Warn("usage PGSink: Stop ctx canceled before drain")
		}
	})
}

// DroppedCount returns the cumulative number of events/rollup-keys dropped
// due to buffer cap. Zero in healthy steady state; positive values mean PG
// flushes are not keeping up.
func (s *PGSink) DroppedCount() int64 { return s.dropped.Load() }

// flushLoop is the long-running goroutine that periodically drains buffers
// to PG. Exits when the sink is closed AND the buffer is empty.
func (s *PGSink) flushLoop(ctx context.Context) {
	defer close(s.done)

	t := time.NewTicker(s.opts.FlushInterval)
	defer t.Stop()

	tickCh := make(chan struct{}, 1)
	// fan tick channel into a receive-friendly trigger consumed alongside
	// cond.Wait. Cond.Wait can't select with a channel directly, so we use
	// a small bridge: a separate goroutine that converts ticker ticks into
	// cond.Broadcast.
	tickerDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-t.C:
				s.mu.Lock()
				select {
				case tickCh <- struct{}{}:
				default:
				}
				s.cond.Broadcast()
				s.mu.Unlock()
			case <-tickerDone:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	defer close(tickerDone)

	for {
		s.mu.Lock()
		// Wait until something to flush, closed, or context canceled.
		for !s.closed && len(s.events) == 0 && len(s.rollup) == 0 && ctx.Err() == nil {
			s.cond.Wait()
		}
		// Drain: swap buffers under lock.
		events := s.events
		s.events = nil
		rollupMap := s.rollup
		s.rollup = make(map[UsageRollupKey]*UsageRollupDelta)
		closed := s.closed
		s.mu.Unlock()

		// Drain tick channel non-blocking.
		select {
		case <-tickCh:
		default:
		}

		// Convert rollup map to slice.
		deltas := make([]UsageRollupDelta, 0, len(rollupMap))
		for _, d := range rollupMap {
			deltas = append(deltas, *d)
		}

		if len(events) > 0 || len(deltas) > 0 {
			if err := s.store.FlushBatch(ctx, events, deltas); err != nil {
				log.Warnf("usage PGSink: flush failed (will retry); events=%d rollups=%d: %v",
					len(events), len(deltas), err)
				// Re-prepend with cap-aware eviction.
				s.mu.Lock()
				s.events = appendCapped(s.events, events, s.opts.MaxBufferEvents, &s.dropped)
				for k, d := range rollupMap {
					if existing, ok := s.rollup[k]; ok {
						existing.RequestCount += d.RequestCount
						existing.SuccessCount += d.SuccessCount
						existing.FailureCount += d.FailureCount
						existing.InputTokens += d.InputTokens
						existing.OutputTokens += d.OutputTokens
						existing.ReasoningTokens += d.ReasoningTokens
						existing.CachedTokens += d.CachedTokens
						existing.TotalTokens += d.TotalTokens
						existing.LatencyMsSum += d.LatencyMsSum
					} else {
						if len(s.rollup) >= s.opts.MaxBufferRollups {
							for k0 := range s.rollup {
								delete(s.rollup, k0)
								s.dropped.Add(1)
								break
							}
						}
						copy := *d
						s.rollup[k] = &copy
					}
				}
				s.mu.Unlock()
			}
		}

		if closed {
			return
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// translate converts a coreusage.Record into the on-disk shapes. Pure
// function for testability.
func (s *PGSink) translate(rec coreusage.Record) (UsageEventRow, UsageRollupDelta) {
	apiKey := rec.APIKey
	if apiKey == "" {
		apiKey = "unknown"
	}
	model := rec.Model
	if model == "" {
		model = "unknown"
	}
	ts := rec.RequestedAt
	if ts.IsZero() {
		ts = time.Now()
	}
	ts = ts.UTC()

	// Mirror logger_plugin's normaliseDetail: ensure TotalTokens is
	// reconstructed if zero. Use the same helper so dedup matches.
	stats := normaliseTokenStats(TokenStats{
		InputTokens:     rec.Detail.InputTokens,
		OutputTokens:    rec.Detail.OutputTokens,
		ReasoningTokens: rec.Detail.ReasoningTokens,
		CachedTokens:    rec.Detail.CachedTokens,
		TotalTokens:     rec.Detail.TotalTokens,
	})

	// Build a RequestDetail in the same shape DedupHash expects.
	rd := RequestDetail{
		Timestamp: ts,
		LatencyMs: rec.Latency.Milliseconds(),
		Source:    rec.Source,
		AuthIndex: rec.AuthIndex,
		Failed:    rec.Failed,
		Tokens:    stats,
	}

	row := UsageEventRow{
		OccurredAt:      ts,
		NodeID:          s.nodeID,
		APIKey:          apiKey,
		Provider:        rec.Provider,
		Model:           model,
		Source:          rec.Source,
		AuthID:          rec.AuthID,
		AuthIndex:       rec.AuthIndex,
		AuthType:        rec.AuthType,
		Failed:          rec.Failed,
		LatencyMs:       rd.LatencyMs,
		InputTokens:     stats.InputTokens,
		OutputTokens:    stats.OutputTokens,
		ReasoningTokens: stats.ReasoningTokens,
		CachedTokens:    stats.CachedTokens,
		TotalTokens:     stats.TotalTokens,
		DedupHash:       DedupHash(apiKey, model, rd),
	}

	delta := UsageRollupDelta{
		UsageRollupKey: UsageRollupKey{
			BucketStart: ts.Truncate(time.Minute),
			NodeID:      s.nodeID,
			APIKey:      apiKey,
			Model:       model,
		},
		RequestCount:    1,
		InputTokens:     stats.InputTokens,
		OutputTokens:    stats.OutputTokens,
		ReasoningTokens: stats.ReasoningTokens,
		CachedTokens:    stats.CachedTokens,
		TotalTokens:     stats.TotalTokens,
		LatencyMsSum:    rd.LatencyMs,
	}
	if rec.Failed {
		delta.FailureCount = 1
	} else {
		delta.SuccessCount = 1
	}
	return row, delta
}

// appendCapped re-prepends previously-flushed events back onto the live
// buffer, dropping the oldest when over MaxBufferEvents. Used only on
// flush failure.
func appendCapped(live, requeue []UsageEventRow, max int, dropped *atomic.Int64) []UsageEventRow {
	combined := make([]UsageEventRow, 0, len(requeue)+len(live))
	combined = append(combined, requeue...)
	combined = append(combined, live...)
	if len(combined) > max {
		drop := len(combined) - max
		dropped.Add(int64(drop))
		combined = combined[drop:]
	}
	return combined
}
