package management

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
)

// usageRange is the parsed query parameters for /v0/management/usage in
// PG-backed mode. The shape mirrors the front-end request: explicit
// from/to (RFC3339), bucket granularity, and an opt-in include flag.
//
// Validation rules (defensive — admin UI is not the only caller):
//   - to defaults to now (UTC), from defaults to to-7d
//   - range capped at 90d (rollup TTL is 90d; longer requests would
//     scan empty space and inflate query cost)
//   - if from > to, fall back to to-1h (avoid negative-duration SQL)
//   - granularity normalized to "hour" or "day" (anything else = hour)
//   - include accepts only "details" (any other value drops to "")
type usageRange struct {
	From, To    time.Time
	Granularity string
	Include     string
}

// parseUsageRange extracts the time-range parameters from the request URL.
// All time values are normalized to UTC so cacheKey() and downstream PG
// queries are timezone-stable across nodes.
func parseUsageRange(r *http.Request) usageRange {
	q := r.URL.Query()

	to := time.Now().UTC()
	if t, err := time.Parse(time.RFC3339, q.Get("to")); err == nil {
		to = t.UTC()
	}
	from := to.Add(-7 * 24 * time.Hour)
	if t, err := time.Parse(time.RFC3339, q.Get("from")); err == nil {
		from = t.UTC()
	}

	// Cap at rollup TTL window. Beyond this we'd be scanning rows that
	// have already been deleted by the cleanup goroutine.
	if to.Sub(from) > 90*24*time.Hour {
		from = to.Add(-90 * 24 * time.Hour)
	}
	// Reject inverted/zero ranges before they hit SQL.
	if !to.After(from) {
		from = to.Add(-time.Hour)
	}

	g := strings.ToLower(strings.TrimSpace(q.Get("granularity")))
	if g != "day" {
		g = "hour"
	}

	inc := q.Get("include")
	if inc != "details" {
		inc = ""
	}

	return usageRange{From: from, To: to, Granularity: g, Include: inc}
}

// cacheKey builds a stable string suitable for usageQueryCache. Same query
// → same key → cache hit; different include flag → different key (the
// payload shape changes when details are present).
func (r usageRange) cacheKey() string {
	return fmt.Sprintf("%s|%s|%s|%s",
		r.From.Format(time.RFC3339),
		r.To.Format(time.RFC3339),
		r.Granularity,
		r.Include,
	)
}

// =============================================================================
// Payload builders for serveClusterUsage.
//
// These shape PG aggregates into the legacy `usage.*` snapshot fields the
// existing UI/CLIs expect. The new `cluster.*` block carries the same data
// pre-aggregated for the new UI panels — both live side-by-side during
// rolling fork rollout. Eventually the legacy `usage.apis[].models[].details`
// will be empty in PG mode (details are opt-in via ?include=details) but
// the totals/series fields remain populated for backward-compat.
// =============================================================================

// buildLegacyAPIs reconstructs the snapshot.APIs map from PG aggregates.
//
// For the cluster path the per-request `details[]` array would be huge
// (tens of MB across 4 nodes × 7 days), so it stays empty unless the
// caller passed `?include=details`. The aggregated counts still flow
// through so the UI's number cards and per-API totals are correct.
//
// SuccessCount/FailureCount come from APIBreakdown's success_count/
// failure_count rollup columns — without them, the frontend's
// detail-iteration fallback in getApiStats would compute 0/0 because
// details[] is empty in the no-include=details path.
func buildLegacyAPIs(breakdown []usage.APIBreakdown, details []usage.UsageEventRow) map[string]usage.APISnapshot {
	apis := make(map[string]usage.APISnapshot)
	for _, b := range breakdown {
		api, ok := apis[b.APIKey]
		if !ok {
			api = usage.APISnapshot{Models: make(map[string]usage.ModelSnapshot)}
		}
		api.TotalRequests += b.Requests
		api.SuccessCount += b.SuccessCount
		api.FailureCount += b.FailureCount
		api.TotalTokens += b.Tokens
		model := api.Models[b.Model]
		model.TotalRequests += b.Requests
		model.SuccessCount += b.SuccessCount
		model.FailureCount += b.FailureCount
		model.TotalTokens += b.Tokens
		api.Models[b.Model] = model
		apis[b.APIKey] = api
	}
	// Attach details only when caller requested them (keeps payload small
	// for the default refresh path). Details ride alongside the already-
	// populated success/failure counts; the frontend uses explicit counts
	// (hasExplicitCounts==true) and only iterates details for cost calc.
	for _, e := range details {
		api, ok := apis[e.APIKey]
		if !ok {
			api = usage.APISnapshot{Models: make(map[string]usage.ModelSnapshot)}
		}
		if api.Models == nil {
			api.Models = make(map[string]usage.ModelSnapshot)
		}
		m := api.Models[e.Model]
		m.Details = append(m.Details, usage.RequestDetail{
			Timestamp: e.OccurredAt,
			LatencyMs: e.LatencyMs,
			Source:    e.Source,
			AuthIndex: e.AuthIndex,
			Failed:    e.Failed,
			Tokens: usage.TokenStats{
				InputTokens:     e.InputTokens,
				OutputTokens:    e.OutputTokens,
				ReasoningTokens: e.ReasoningTokens,
				CachedTokens:    e.CachedTokens,
				TotalTokens:     e.TotalTokens,
			},
		})
		api.Models[e.Model] = m
		apis[e.APIKey] = api
	}
	return apis
}

// buildSeriesByDay reduces a trend series to a {YYYY-MM-DD: total} map.
// The metric arg picks "requests" or "tokens".
func buildSeriesByDay(trend []usage.TrendPoint, metric string) map[string]int64 {
	out := make(map[string]int64)
	for _, p := range trend {
		key := p.Bucket.UTC().Format("2006-01-02")
		switch metric {
		case "requests":
			out[key] += p.Requests
		case "tokens":
			out[key] += p.Tokens
		}
	}
	return out
}

// buildSeriesByHour reduces trend to {"0".."23": total}. Hour-of-day
// (not full datetime) — the legacy UI expects this shape for its
// "activity by hour of day" chart.
func buildSeriesByHour(trend []usage.TrendPoint, metric string) map[string]int64 {
	out := make(map[string]int64)
	for _, p := range trend {
		key := fmt.Sprintf("%d", p.Bucket.UTC().Hour())
		switch metric {
		case "requests":
			out[key] += p.Requests
		case "tokens":
			out[key] += p.Tokens
		}
	}
	return out
}

// avgLatency returns mean per-request latency in ms over the totals
// window. Returns 0 when no requests landed.
func avgLatency(t usage.ClusterTotals) int64 {
	if t.TotalRequests <= 0 {
		return 0
	}
	return t.LatencyMsSum / t.TotalRequests
}
