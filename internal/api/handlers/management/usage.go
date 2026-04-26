package management

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
)

type usageExportPayload struct {
	Version    int                      `json:"version"`
	ExportedAt time.Time                `json:"exported_at"`
	Usage      usage.StatisticsSnapshot `json:"usage"`
}

type usageImportPayload struct {
	Version int                      `json:"version"`
	Usage   usage.StatisticsSnapshot `json:"usage"`
}

// GetUsageStatistics serves /v0/management/usage. Behavior depends on
// cfg.Usage.Backend:
//
//   - "" / "memory" / "dual"   →  returns the in-memory per-node snapshot
//                                 (legacy shape; what the existing UI
//                                 already consumes)
//   - "pg" + h.pgUsage != nil  →  returns cluster-aggregated PG payload
//                                 with both legacy `usage.*` fields AND
//                                 a new `cluster.*` block of pre-aggregated
//                                 series (sparkline / health grid / api &
//                                 credential breakdowns)
//
// "dual" is intentionally on the in-memory path: dual mode means we're
// double-writing to PG to warm the rollup tables, but the read path hasn't
// flipped yet (rollout safety — verify writes look right before trusting
// PG for reads).
func (h *Handler) GetUsageStatistics(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusOK, gin.H{
			"usage":           usage.StatisticsSnapshot{},
			"failed_requests": int64(0),
		})
		return
	}

	backend := ""
	if h.cfg != nil {
		backend = strings.ToLower(strings.TrimSpace(h.cfg.Usage.Backend))
	}
	if backend == "pg" && h.pgUsage != nil {
		h.serveClusterUsage(c)
		return
	}

	var snapshot usage.StatisticsSnapshot
	if h.usageStats != nil {
		snapshot = h.usageStats.Snapshot()
	}
	c.Header("X-Usage-Backend", backendLabel(backend))
	c.JSON(http.StatusOK, gin.H{
		"usage":           snapshot,
		"failed_requests": snapshot.FailureCount,
	})
}

// serveClusterUsage builds the PG-aggregated payload. Cached for 5s to
// absorb dashboard refresh storms (4 admin tabs polling at the same time
// would otherwise turn into 4× the SQL load).
func (h *Handler) serveClusterUsage(c *gin.Context) {
	rng := parseUsageRange(c.Request)
	cacheKey := rng.cacheKey()

	if v, ok := h.usageQueryCache.Get(cacheKey); ok {
		c.Header("X-Usage-Cache", "hit")
		c.Header("X-Usage-Backend", "pg")
		c.JSON(http.StatusOK, v)
		return
	}

	// 5s timeout per /usage call: sparkline + 7×24 grid + breakdown +
	// totals + trend + (optional) details + node count = up to 7 PG
	// round-trips. Each query is cheap (indexed scans) so 5s is generous;
	// past it the user-facing refresh would feel broken anyway.
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	totals, err := h.pgUsage.QueryClusterTotals(ctx, rng.From, rng.To)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	trend, err := h.pgUsage.QueryClusterTrend(ctx, rng.From, rng.To, rng.Granularity)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	apiBreakdown, err := h.pgUsage.QueryAPIBreakdown(ctx, rng.From, rng.To)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// Sparkline always shows last hour regardless of selected range —
	// the StatCards component is "what's happening right now".
	sparkFrom := rng.To.Add(-time.Hour)
	sparkline, err := h.pgUsage.QuerySparklineSeries(ctx, sparkFrom, rng.To)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// Health grid is always 7 days ending at rng.To for the same reason.
	healthFrom := rng.To.Add(-7 * 24 * time.Hour)
	healthGrid, err := h.pgUsage.QueryServiceHealthGrid(ctx, healthFrom, rng.To)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	creds, err := h.pgUsage.QueryCredentialBreakdown(ctx, rng.From, rng.To)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	nodeCount, err := h.pgUsage.QueryActiveNodeCount(ctx, rng.From, rng.To)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var details []usage.UsageEventRow
	if rng.Include == "details" {
		// Limit at 500 — enough to fill RequestEventsDetailsCard's table
		// without bloating cache memory or response payload. Pagination
		// uses the same endpoint with from/to + limit/offset (future
		// param; UI doesn't paginate today).
		details, err = h.pgUsage.QueryEventDetails(ctx, rng.From, rng.To, 500, 0, "", "")
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	payload := gin.H{
		"usage": gin.H{
			"total_requests":   totals.TotalRequests,
			"success_count":    totals.SuccessCount,
			"failure_count":    totals.FailureCount,
			"total_tokens":     totals.TotalTokens,
			"apis":             buildLegacyAPIs(apiBreakdown, details),
			"requests_by_day":  buildSeriesByDay(trend, "requests"),
			"requests_by_hour": buildSeriesByHour(trend, "requests"),
			"tokens_by_day":    buildSeriesByDay(trend, "tokens"),
			"tokens_by_hour":   buildSeriesByHour(trend, "tokens"),
		},
		"failed_requests": totals.FailureCount,
		"cluster": gin.H{
			"aggregated": true,
			"node_count": nodeCount,
			"range": gin.H{
				"from":        rng.From.Format(time.RFC3339),
				"to":          rng.To.Format(time.RFC3339),
				"granularity": rng.Granularity,
			},
			"trend":                trend,
			"sparkline":            sparkline,
			"health_grid":          healthGrid,
			"api_breakdown":        apiBreakdown,
			"credential_breakdown": creds,
			"average_latency_ms":   avgLatency(totals),
		},
	}
	if rng.Include == "details" {
		payload["details"] = details
	}

	h.usageQueryCache.Put(cacheKey, payload, 5*time.Second)
	c.Header("X-Usage-Cache", "miss")
	c.Header("X-Usage-Backend", "pg")
	c.JSON(http.StatusOK, payload)
}

// backendLabel maps internal cfg.Usage.Backend to the response header
// value. Empty/unknown reports as "memory" so old admin tooling that
// didn't set the field still gets a meaningful answer.
func backendLabel(backend string) string {
	switch backend {
	case "pg", "dual":
		return backend
	default:
		return "memory"
	}
}

// ExportUsageStatistics returns a complete usage snapshot for
// backup/migration. In PG mode (backend=pg) we rebuild the legacy
// snapshot shape from PG events + rollup totals so old CLIs that import
// this payload elsewhere still work. In memory/dual modes we serve the
// in-memory snapshot directly (the canonical source on those paths).
func (h *Handler) ExportUsageStatistics(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusOK, usageExportPayload{
			Version:    1,
			ExportedAt: time.Now().UTC(),
		})
		return
	}

	backend := ""
	if h.cfg != nil {
		backend = strings.ToLower(strings.TrimSpace(h.cfg.Usage.Backend))
	}
	if backend == "pg" && h.pgUsage != nil {
		h.exportFromPG(c)
		return
	}

	var snapshot usage.StatisticsSnapshot
	if h.usageStats != nil {
		snapshot = h.usageStats.Snapshot()
	}
	c.Header("X-Usage-Backend", backendLabel(backend))
	c.JSON(http.StatusOK, usageExportPayload{
		Version:    1,
		ExportedAt: time.Now().UTC(),
		Usage:      snapshot,
	})
}

// exportFromPG builds a StatisticsSnapshot from PG. Bounded by event TTL
// (default 7 days) — caller can override via ?from/to. The full request
// detail array is included (this is an admin-triggered export, not a UI
// refresh, so the size is acceptable).
func (h *Handler) exportFromPG(c *gin.Context) {
	rng := parseUsageRange(c.Request)
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	totals, err := h.pgUsage.QueryClusterTotals(ctx, rng.From, rng.To)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	apiBreakdown, err := h.pgUsage.QueryAPIBreakdown(ctx, rng.From, rng.To)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	trend, err := h.pgUsage.QueryClusterTrend(ctx, rng.From, rng.To, "hour")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// Pull all events in range — 5000 row cap protects against pathological
	// exports (admin can paginate via from/to if they truly need more).
	events, err := h.pgUsage.QueryEventDetails(ctx, rng.From, rng.To, 5000, 0, "", "")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	snap := usage.StatisticsSnapshot{
		TotalRequests:  totals.TotalRequests,
		SuccessCount:   totals.SuccessCount,
		FailureCount:   totals.FailureCount,
		TotalTokens:    totals.TotalTokens,
		APIs:           buildLegacyAPIs(apiBreakdown, events),
		RequestsByDay:  buildSeriesByDay(trend, "requests"),
		RequestsByHour: buildSeriesByHour(trend, "requests"),
		TokensByDay:    buildSeriesByDay(trend, "tokens"),
		TokensByHour:   buildSeriesByHour(trend, "tokens"),
	}
	c.Header("X-Usage-Backend", "pg")
	c.JSON(http.StatusOK, usageExportPayload{
		Version:    1,
		ExportedAt: time.Now().UTC(),
		Usage:      snap,
	})
}

// ImportUsageStatistics merges a previously exported usage snapshot. In
// PG mode the imported events go through PGStore.InsertEventsBatch (with
// dedup_hash deduplication) and then RebuildRollupForRange recomputes
// the affected minute buckets. In memory/dual modes we keep the legacy
// MergeSnapshot path.
func (h *Handler) ImportUsageStatistics(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "usage statistics unavailable"})
		return
	}

	data, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		return
	}

	var payload usageImportPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	if payload.Version != 0 && payload.Version != 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported version"})
		return
	}

	backend := ""
	if h.cfg != nil {
		backend = strings.ToLower(strings.TrimSpace(h.cfg.Usage.Backend))
	}
	if backend == "pg" && h.pgUsage != nil {
		h.importToPG(c, payload)
		return
	}

	if h.usageStats == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "usage statistics unavailable"})
		return
	}
	result := h.usageStats.MergeSnapshot(payload.Usage)
	snapshot := h.usageStats.Snapshot()
	c.Header("X-Usage-Backend", backendLabel(backend))
	c.JSON(http.StatusOK, gin.H{
		"added":           result.Added,
		"skipped":         result.Skipped,
		"total_requests":  snapshot.TotalRequests,
		"failed_requests": snapshot.FailureCount,
	})
}

// importToPG translates the legacy snapshot's per-request details into
// usage_events rows + a single RebuildRollupForRange over the spanned
// range. Idempotent on retry (dedup_hash UNIQUE INDEX absorbs duplicates).
func (h *Handler) importToPG(c *gin.Context, payload usageImportPayload) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()

	// Determine the node_id to attribute imported rows to. We use a
	// fixed sentinel so admins can later identify (and optionally purge)
	// imported data without touching live-traffic rows.
	const importNodeID = "imported"

	rows, minTS, maxTS := flattenSnapshotToEvents(payload.Usage, importNodeID)
	if len(rows) == 0 {
		c.Header("X-Usage-Backend", "pg")
		c.JSON(http.StatusOK, gin.H{
			"added": 0, "skipped": 0,
			"total_requests":  int64(0),
			"failed_requests": int64(0),
		})
		return
	}

	if err := h.pgUsage.InsertEventsBatch(ctx, rows); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// Pad the range by one minute on each side: RebuildRollupForRange's
	// WHERE uses < (exclusive upper), and the rebuild query uses
	// date_trunc('minute', occurred_at) which can drop sub-minute parts.
	if err := h.pgUsage.RebuildRollupForRange(ctx,
		minTS.Add(-time.Minute), maxTS.Add(time.Minute)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	totals, _ := h.pgUsage.QueryClusterTotals(ctx,
		minTS.Add(-time.Minute), maxTS.Add(time.Minute))
	c.Header("X-Usage-Backend", "pg")
	c.JSON(http.StatusOK, gin.H{
		"added":           len(rows),
		"skipped":         0, // PG dedup is silent; "added" includes attempted
		"total_requests":  totals.TotalRequests,
		"failed_requests": totals.FailureCount,
	})
}

// flattenSnapshotToEvents walks the legacy snapshot and emits one
// UsageEventRow per RequestDetail. Returns the rows + min/max occurred_at
// timestamps for RebuildRollupForRange.
func flattenSnapshotToEvents(snap usage.StatisticsSnapshot, nodeID string) ([]usage.UsageEventRow, time.Time, time.Time) {
	var rows []usage.UsageEventRow
	var minTS, maxTS time.Time
	for apiKey, api := range snap.APIs {
		for model, m := range api.Models {
			for _, d := range m.Details {
				ts := d.Timestamp
				if ts.IsZero() {
					continue
				}
				if minTS.IsZero() || ts.Before(minTS) {
					minTS = ts
				}
				if maxTS.IsZero() || ts.After(maxTS) {
					maxTS = ts
				}
				rows = append(rows, usage.UsageEventRow{
					OccurredAt:      ts.UTC(),
					NodeID:          nodeID,
					APIKey:          apiKey,
					Model:           model,
					Source:          d.Source,
					AuthIndex:       d.AuthIndex,
					Failed:          d.Failed,
					LatencyMs:       d.LatencyMs,
					InputTokens:     d.Tokens.InputTokens,
					OutputTokens:    d.Tokens.OutputTokens,
					ReasoningTokens: d.Tokens.ReasoningTokens,
					CachedTokens:    d.Tokens.CachedTokens,
					TotalTokens:     d.Tokens.TotalTokens,
					DedupHash:       usage.DedupHash(apiKey, model, d),
				})
			}
		}
	}
	return rows, minTS, maxTS
}
