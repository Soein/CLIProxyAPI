package usage

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
)

// Cleanup is the leader-gated TTL pruner for usage_events and
// usage_minute_rollup. Runs on every node that bootstraps cluster mode
// with backend != "memory", but only the cluster leader actually
// executes the DELETEs — followers tick and skip. This keeps the
// cleanup cost from multiplying by replica count and avoids the noisy
// "cleanup deleted N rows" log fanout.
//
// Why leader-gated rather than DB-locked:
//   - The cluster already runs a leader elector (advisory lock); reusing
//     it is one less moving part.
//   - PG can run multiple concurrent DELETEs without correctness issues,
//     but the noise (each replica logging the same delete count) makes
//     incident triage harder.
//
// Hourly tick is conservative. Even with 10k req/s × 4 nodes, the events
// table only grows by ~3.5M rows/day → daily delete batch is small.
type Cleanup struct {
	Store         *PGStore
	IsLeader      func() bool
	EventTTLDays  int
	RollupTTLDays int
	Interval      time.Duration
}

// Run blocks until ctx is cancelled. Safe to call once per process.
// First tick fires after Interval (not immediately) — a node that just
// joined shouldn't pre-empt a fresh deploy with a heavy DELETE.
func (c *Cleanup) Run(ctx context.Context) {
	if c == nil || c.Store == nil {
		return
	}
	if c.Interval <= 0 {
		c.Interval = time.Hour
	}
	t := time.NewTicker(c.Interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.runOnce(ctx)
		}
	}
}

// runOnce performs one cleanup pass. Exposed (lowercase, package-internal)
// so tests can drive it deterministically without spinning up a real
// ticker.
func (c *Cleanup) runOnce(ctx context.Context) {
	if c.IsLeader != nil && !c.IsLeader() {
		return
	}
	if c.EventTTLDays > 0 {
		cutoff := time.Now().Add(-time.Duration(c.EventTTLDays) * 24 * time.Hour)
		if n, err := c.Store.CleanupEvents(ctx, cutoff); err != nil {
			log.Errorf("usage cleanup events: %v", err)
		} else if n > 0 {
			log.Infof("usage cleanup: deleted %d events older than %s", n, cutoff.UTC().Format(time.RFC3339))
		}
	}
	if c.RollupTTLDays > 0 {
		cutoff := time.Now().Add(-time.Duration(c.RollupTTLDays) * 24 * time.Hour)
		if n, err := c.Store.CleanupRollups(ctx, cutoff); err != nil {
			log.Errorf("usage cleanup rollups: %v", err)
		} else if n > 0 {
			log.Infof("usage cleanup: deleted %d rollups older than %s", n, cutoff.UTC().Format(time.RFC3339))
		}
	}
}
