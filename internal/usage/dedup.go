package usage

import (
	"crypto/sha256"
	"fmt"
	"time"
)

// dedupHashFormatVersion: bump if the field set, ordering, or normalization
// rules change. The TestDedupHashStableForKnownInput pin catches drift.
const dedupHashFormatVersion = 1

// DedupHash returns the SHA-256 of the same byte string used by the legacy
// in-memory dedupKey function (see logger_plugin.go). Order and normalization
// MUST stay byte-equivalent, otherwise PG dedup (uq_usage_events_dedup) and
// in-memory MergeSnapshot dedup will diverge and either drop legitimate rows
// or accept duplicates.
//
// Inputs:
//   - apiName, modelName: the same keys used in apis[apiName].models[modelName]
//   - detail: timestamp, source, auth_index, failed, raw token stats. Tokens
//     are passed through normaliseTokenStats (same as dedupKey) so that a
//     missing TotalTokens is reconstructed identically.
func DedupHash(apiName, modelName string, detail RequestDetail) []byte {
	s := dedupSourceString(apiName, modelName, detail)
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}

// dedupSourceString is exposed (lowercase, package-internal) so logger_plugin's
// dedupKey can call it for byte parity. Must match the historical format
// exactly.
func dedupSourceString(apiName, modelName string, detail RequestDetail) string {
	timestamp := detail.Timestamp.UTC().Format(time.RFC3339Nano)
	tokens := normaliseTokenStats(detail.Tokens)
	return fmt.Sprintf(
		"%s|%s|%s|%s|%s|%t|%d|%d|%d|%d|%d",
		apiName,
		modelName,
		timestamp,
		detail.Source,
		detail.AuthIndex,
		detail.Failed,
		tokens.InputTokens,
		tokens.OutputTokens,
		tokens.ReasoningTokens,
		tokens.CachedTokens,
		tokens.TotalTokens,
	)
}
