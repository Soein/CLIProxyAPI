package management

import (
	"fmt"
	"net/http"
	"strings"
	"time"
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
