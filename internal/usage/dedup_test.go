package usage

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
)

// TestDedupHashStableForKnownInput pins the byte output of DedupHash against
// a fixed input. If this test fails after a refactor, the dedup logic has
// drifted — PG's uq_usage_events_dedup and in-memory MergeSnapshot will
// disagree. Bump dedupHashFormatVersion AND re-pin the hash before merging.
func TestDedupHashStableForKnownInput(t *testing.T) {
	fixed := time.Date(2026, 4, 25, 12, 34, 56, 0, time.UTC)
	got := hex.EncodeToString(DedupHash("api-1", "gpt-5", RequestDetail{
		Timestamp: fixed,
		Source:    "src",
		AuthIndex: "idx",
		Failed:    false,
		Tokens:    TokenStats{InputTokens: 10, OutputTokens: 20, TotalTokens: 30},
	}))
	const want = "e49fc088f132cbf036f907dba9525741f9f8c8e17133e28ae4f70d2c1347e2f7"
	if got != want {
		t.Fatalf("hash drift: got %s, want %s", got, want)
	}
}

// TestDedupHash_MatchesLegacyDedupKey is a defense-in-depth: confirms that
// SHA-256 over the legacy dedupKey() string is identical to DedupHash. They
// share dedupSourceString so this is true by construction, but if anyone
// ever forks them the test catches it.
func TestDedupHash_MatchesLegacyDedupKey(t *testing.T) {
	cases := []RequestDetail{
		{Timestamp: time.Now().UTC(), Source: "s", AuthIndex: "0", Failed: false, Tokens: TokenStats{InputTokens: 1, TotalTokens: 1}},
		{Timestamp: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), Source: "", AuthIndex: "", Failed: true},
	}
	for i, d := range cases {
		legacy := dedupKey("api", "model", d)
		legacySum := sha256.Sum256([]byte(legacy))
		newHash := DedupHash("api", "model", d)
		if hex.EncodeToString(legacySum[:]) != hex.EncodeToString(newHash) {
			t.Errorf("case %d: legacy=%x new=%x", i, legacySum[:], newHash)
		}
	}
}
