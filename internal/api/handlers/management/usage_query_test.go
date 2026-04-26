package management

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseUsageRange_Defaults(t *testing.T) {
	r := parseUsageRange(httptest.NewRequest("GET", "/usage", nil))
	if r.Granularity != "hour" {
		t.Fatalf("granularity = %q, want hour", r.Granularity)
	}
	if got := r.To.Sub(r.From); got != 7*24*time.Hour {
		t.Fatalf("range = %v, want 168h", got)
	}
	if r.Include != "" {
		t.Fatalf("include = %q, want empty", r.Include)
	}
}

func TestParseUsageRange_Cap90Days(t *testing.T) {
	req := httptest.NewRequest("GET",
		"/usage?from=2020-01-01T00:00:00Z&to=2026-04-25T00:00:00Z", nil)
	r := parseUsageRange(req)
	if got := r.To.Sub(r.From); got > 90*24*time.Hour {
		t.Fatalf("range not capped: %v", got)
	}
}

func TestParseUsageRange_GranularityValidation(t *testing.T) {
	// Anything not "day" normalizes to "hour" (covers "minute", typos, empty).
	cases := map[string]string{
		"day":     "day",
		"hour":    "hour",
		"minute":  "hour",
		"":        "hour",
		"DAY":     "day",
		"garbage": "hour",
	}
	for in, want := range cases {
		req := httptest.NewRequest("GET", "/usage?granularity="+in, nil)
		if got := parseUsageRange(req).Granularity; got != want {
			t.Errorf("granularity(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseUsageRange_IncludeDetails(t *testing.T) {
	req := httptest.NewRequest("GET", "/usage?include=details", nil)
	if r := parseUsageRange(req); r.Include != "details" {
		t.Fatalf("include = %q, want details", r.Include)
	}
	// Anything other than "details" is dropped (defense against query-string
	// injection driving expensive code paths by accident).
	req = httptest.NewRequest("GET", "/usage?include=everything", nil)
	if r := parseUsageRange(req); r.Include != "" {
		t.Fatalf("include = %q, want empty", r.Include)
	}
}

func TestParseUsageRange_InvalidFromToOrder(t *testing.T) {
	// from > to is nonsense — handler must produce a valid range, not
	// negative-duration garbage that breaks downstream SQL.
	req := httptest.NewRequest("GET",
		"/usage?from=2026-04-25T12:00:00Z&to=2026-04-25T10:00:00Z", nil)
	r := parseUsageRange(req)
	if !r.To.After(r.From) {
		t.Fatalf("to(%v) must be after from(%v)", r.To, r.From)
	}
}

func TestUsageRange_CacheKey_Stable(t *testing.T) {
	a := usageRange{
		From:        time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		To:          time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC),
		Granularity: "hour",
		Include:     "",
	}
	b := usageRange{
		From:        time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		To:          time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC),
		Granularity: "hour",
		Include:     "",
	}
	if a.cacheKey() != b.cacheKey() {
		t.Fatalf("cacheKey not stable: %q vs %q", a.cacheKey(), b.cacheKey())
	}
	c := a
	c.Include = "details"
	if a.cacheKey() == c.cacheKey() {
		t.Fatalf("cacheKey collision: include should affect key")
	}
}
