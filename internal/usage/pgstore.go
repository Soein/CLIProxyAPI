// Package usage — PG persistence layer for usage statistics.
//
// PGStore wraps a *sql.DB and exposes only the queries needed by the PGSink
// (write path) and the management API (read path). It is intentionally a
// thin layer over database/sql: the schema is owned by
// internal/store/postgresstore.go EnsureSchema, the dedup hash by
// internal/usage/dedup.go, and the buffer/flush logic by pgsink.go. This
// file holds nothing but SQL.
package usage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// UsageEventRow is one row in usage_events. Fields map 1:1 to columns.
type UsageEventRow struct {
	OccurredAt      time.Time
	NodeID          string
	APIKey          string
	Provider        string
	Model           string
	Source          string
	AuthID          string
	AuthIndex       string
	AuthType        string
	Failed          bool
	LatencyMs       int64
	InputTokens     int64
	OutputTokens    int64
	ReasoningTokens int64
	CachedTokens    int64
	TotalTokens     int64
	DedupHash       []byte
}

// UsageRollupKey is the PK tuple of usage_minute_rollup.
type UsageRollupKey struct {
	BucketStart time.Time
	NodeID      string
	APIKey      string
	Model       string
}

// UsageRollupDelta is one flush diff: counters to add to a rollup row.
type UsageRollupDelta struct {
	UsageRollupKey
	RequestCount    int64
	SuccessCount    int64
	FailureCount    int64
	InputTokens     int64
	OutputTokens    int64
	ReasoningTokens int64
	CachedTokens    int64
	TotalTokens     int64
	LatencyMsSum    int64
}

// PGStore is a thin wrapper over *sql.DB with usage-specific helpers.
type PGStore struct {
	db *sql.DB
}

// NewPGStore returns a PGStore bound to db. The DB connection lifetime is
// owned by the caller (typically the postgresstore.PostgresStore).
func NewPGStore(db *sql.DB) *PGStore { return &PGStore{db: db} }

// DB returns the underlying *sql.DB so query helpers in handlers can run
// custom SQL without re-opening a pool.
func (s *PGStore) DB() *sql.DB { return s.db }

// EnsureSchema is a test-only convenience that creates the usage tables on a
// bare PG. Production paths run postgresstore.EnsureSchema first, which owns
// the canonical DDL and additional constraints. Keep the two in sync.
func (s *PGStore) EnsureSchema(ctx context.Context) error {
	for _, ddl := range schemaDDL {
		if _, err := s.db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("usage EnsureSchema: %w", err)
		}
	}
	return nil
}

// sqlExec lets bulk* helpers accept either *sql.DB or *sql.Tx.
type sqlExec interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// InsertEventsBatch issues one multi-row INSERT with ON CONFLICT DO NOTHING
// on dedup_hash. Idempotent on retry.
func (s *PGStore) InsertEventsBatch(ctx context.Context, rows []UsageEventRow) error {
	return bulkInsertEvents(ctx, s.db, rows)
}

// UpsertRollupBatch issues one multi-row INSERT ... ON CONFLICT DO UPDATE
// adding deltas to existing counters. NOT retry-safe on its own — callers
// must drain their buffer atomically (the PGSink flusher swaps buffers
// under lock before calling this so a partial retry doesn't double-count).
func (s *PGStore) UpsertRollupBatch(ctx context.Context, deltas []UsageRollupDelta) error {
	return bulkUpsertRollup(ctx, s.db, deltas)
}

// FlushBatch wraps both writes in a single transaction so events and rollup
// can never disagree (e.g. PG dies between the two statements).
func (s *PGStore) FlushBatch(ctx context.Context, events []UsageEventRow, deltas []UsageRollupDelta) error {
	if len(events) == 0 && len(deltas) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("usage FlushBatch begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := bulkInsertEvents(ctx, tx, events); err != nil {
		return err
	}
	if err := bulkUpsertRollup(ctx, tx, deltas); err != nil {
		return err
	}
	return tx.Commit()
}

func bulkInsertEvents(ctx context.Context, exec sqlExec, rows []UsageEventRow) error {
	if len(rows) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.WriteString(`INSERT INTO usage_events (occurred_at, node_id, api_key, provider, model, source, auth_id, auth_index, auth_type, failed, latency_ms, input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens, dedup_hash) VALUES `)
	args := make([]any, 0, len(rows)*17)
	for i, r := range rows {
		if i > 0 {
			sb.WriteByte(',')
		}
		n := i * 17
		fmt.Fprintf(&sb,
			"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			n+1, n+2, n+3, n+4, n+5, n+6, n+7, n+8, n+9, n+10, n+11, n+12, n+13, n+14, n+15, n+16, n+17,
		)
		args = append(args,
			r.OccurredAt.UTC(), r.NodeID, r.APIKey, r.Provider, r.Model, r.Source,
			r.AuthID, r.AuthIndex, r.AuthType, r.Failed, r.LatencyMs,
			r.InputTokens, r.OutputTokens, r.ReasoningTokens, r.CachedTokens, r.TotalTokens,
			r.DedupHash,
		)
	}
	sb.WriteString(` ON CONFLICT (dedup_hash) DO NOTHING`)
	if _, err := exec.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("usage_events insert: %w", err)
	}
	return nil
}

func bulkUpsertRollup(ctx context.Context, exec sqlExec, deltas []UsageRollupDelta) error {
	if len(deltas) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.WriteString(`INSERT INTO usage_minute_rollup (bucket_start, node_id, api_key, model, request_count, success_count, failure_count, input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens, latency_ms_sum) VALUES `)
	args := make([]any, 0, len(deltas)*13)
	for i, d := range deltas {
		if i > 0 {
			sb.WriteByte(',')
		}
		n := i * 13
		fmt.Fprintf(&sb,
			"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			n+1, n+2, n+3, n+4, n+5, n+6, n+7, n+8, n+9, n+10, n+11, n+12, n+13,
		)
		args = append(args,
			d.BucketStart.UTC(), d.NodeID, d.APIKey, d.Model,
			d.RequestCount, d.SuccessCount, d.FailureCount,
			d.InputTokens, d.OutputTokens, d.ReasoningTokens, d.CachedTokens, d.TotalTokens,
			d.LatencyMsSum,
		)
	}
	sb.WriteString(` ON CONFLICT (bucket_start, node_id, api_key, model) DO UPDATE SET
		request_count    = usage_minute_rollup.request_count    + EXCLUDED.request_count,
		success_count    = usage_minute_rollup.success_count    + EXCLUDED.success_count,
		failure_count    = usage_minute_rollup.failure_count    + EXCLUDED.failure_count,
		input_tokens     = usage_minute_rollup.input_tokens     + EXCLUDED.input_tokens,
		output_tokens    = usage_minute_rollup.output_tokens    + EXCLUDED.output_tokens,
		reasoning_tokens = usage_minute_rollup.reasoning_tokens + EXCLUDED.reasoning_tokens,
		cached_tokens    = usage_minute_rollup.cached_tokens    + EXCLUDED.cached_tokens,
		total_tokens     = usage_minute_rollup.total_tokens     + EXCLUDED.total_tokens,
		latency_ms_sum   = usage_minute_rollup.latency_ms_sum   + EXCLUDED.latency_ms_sum,
		updated_at       = NOW()`)
	if _, err := exec.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("usage_rollup upsert: %w", err)
	}
	return nil
}

// CleanupEvents deletes rows with occurred_at < olderThan. Returns rows
// affected. Called by the leader-gated cleanup goroutine on hourly tick.
func (s *PGStore) CleanupEvents(ctx context.Context, olderThan time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM usage_events WHERE occurred_at < $1`, olderThan.UTC())
	if err != nil {
		return 0, fmt.Errorf("usage_events cleanup: %w", err)
	}
	return res.RowsAffected()
}

// CleanupRollups deletes rollup rows with bucket_start < olderThan.
func (s *PGStore) CleanupRollups(ctx context.Context, olderThan time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM usage_minute_rollup WHERE bucket_start < $1`, olderThan.UTC())
	if err != nil {
		return 0, fmt.Errorf("usage_rollup cleanup: %w", err)
	}
	return res.RowsAffected()
}

// =============================================================================
// Read path — cluster-aggregated queries used by /v0/management/usage.
//
// Design rules:
//   - All queries are bounded by (from, to) range to keep them under the
//     90-day rollup TTL.
//   - All time params are sent in UTC; PG stores TIMESTAMPTZ but mixing
//     timezones at the boundary leads to off-by-N-hour bugs when the API
//     returns 'YYYY-MM-DD' strings to the UI.
//   - Cluster aggregation (SUM across all node_id) happens in PG, not
//     server-side, so the wire payload stays tiny even with 4-N nodes.
//   - LIMIT 200 on breakdown queries keeps the response capped — admin
//     UI cards never show >50 rows anyway, and 200 leaves headroom for
//     "show all" toggles before we'd need pagination.
// =============================================================================

// ClusterTotals is the cluster-wide rollup of the requested range.
// CachedTokens/ReasoningTokens flow through so the StatCards "缓存
// Tokens / 思考 Tokens" cells in PG mode show real numbers (not 0)
// — without these, frontend would have to dig into details[] which is
// empty in default PG payload.
type ClusterTotals struct {
	TotalRequests   int64
	SuccessCount    int64
	FailureCount    int64
	TotalTokens     int64
	CachedTokens    int64
	ReasoningTokens int64
	LatencyMsSum    int64
}

// QueryClusterTotals sums all rollup rows in [from, to). Returns zeros (no
// error) on empty range — the UI shows "no data" cards in that case.
func (s *PGStore) QueryClusterTotals(ctx context.Context, from, to time.Time) (ClusterTotals, error) {
	var t ClusterTotals
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(request_count),0),
		       COALESCE(SUM(success_count),0),
		       COALESCE(SUM(failure_count),0),
		       COALESCE(SUM(total_tokens),0),
		       COALESCE(SUM(cached_tokens),0),
		       COALESCE(SUM(reasoning_tokens),0),
		       COALESCE(SUM(latency_ms_sum),0)
		FROM usage_minute_rollup
		WHERE bucket_start >= $1 AND bucket_start < $2`,
		from.UTC(), to.UTC()).
		Scan(&t.TotalRequests, &t.SuccessCount, &t.FailureCount,
			&t.TotalTokens, &t.CachedTokens, &t.ReasoningTokens, &t.LatencyMsSum)
	if err != nil {
		return ClusterTotals{}, fmt.Errorf("usage QueryClusterTotals: %w", err)
	}
	return t, nil
}

// TrendPoint is one bucket on the requests/tokens trend chart. Granularity
// is parameterised by the caller (date_trunc('hour'|'day', bucket_start)).
//
// InputTokens/OutputTokens/CachedTokens/ReasoningTokens flow through so
// the front-end TokenBreakdownChart can stack 4 categories per bucket
// in PG mode (without these the chart would have no data because PG
// payload's details[] is empty).
type TrendPoint struct {
	Bucket          time.Time `json:"bucket"`
	Requests        int64     `json:"requests"`
	Tokens          int64     `json:"tokens"`
	InputTokens     int64     `json:"input_tokens"`
	OutputTokens    int64     `json:"output_tokens"`
	CachedTokens    int64     `json:"cached_tokens"`
	ReasoningTokens int64     `json:"reasoning_tokens"`
}

// QueryClusterTrend returns one TrendPoint per bucket in [from, to). The
// granularity is restricted to "hour" or "day" — anything else falls back
// to "hour" so a typo can't smuggle arbitrary SQL into date_trunc.
func (s *PGStore) QueryClusterTrend(ctx context.Context, from, to time.Time, granularity string) ([]TrendPoint, error) {
	trunc := "hour"
	if granularity == "day" {
		trunc = "day"
	}
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT date_trunc('%s', bucket_start) AS bucket,
		       COALESCE(SUM(request_count),0),
		       COALESCE(SUM(total_tokens),0),
		       COALESCE(SUM(input_tokens),0),
		       COALESCE(SUM(output_tokens),0),
		       COALESCE(SUM(cached_tokens),0),
		       COALESCE(SUM(reasoning_tokens),0)
		FROM usage_minute_rollup
		WHERE bucket_start >= $1 AND bucket_start < $2
		GROUP BY bucket
		ORDER BY bucket`, trunc), from.UTC(), to.UTC())
	if err != nil {
		return nil, fmt.Errorf("usage QueryClusterTrend: %w", err)
	}
	defer rows.Close()
	var out []TrendPoint
	for rows.Next() {
		var p TrendPoint
		if err := rows.Scan(&p.Bucket, &p.Requests, &p.Tokens,
			&p.InputTokens, &p.OutputTokens, &p.CachedTokens, &p.ReasoningTokens); err != nil {
			return nil, fmt.Errorf("usage QueryClusterTrend scan: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// APIBreakdown is one (api_key, model) pair with its summed activity.
// SuccessCount/FailureCount are needed because the legacy frontend
// derives them from the (now-empty in PG mode) details[] array — without
// them, ApiDetailsCard / ModelStatsCard would show 0/0 for every row.
type APIBreakdown struct {
	APIKey       string `json:"api_key"`
	Model        string `json:"model"`
	Requests     int64  `json:"requests"`
	SuccessCount int64  `json:"success_count"`
	FailureCount int64  `json:"failure_count"`
	Tokens       int64  `json:"tokens"`
}

// QueryAPIBreakdown returns the top-200 (api,model) pairs ordered by total
// tokens — that ordering matches what the ApiDetailsCard / ModelStatsCard
// already show, so the UI doesn't need to re-sort.
func (s *PGStore) QueryAPIBreakdown(ctx context.Context, from, to time.Time) ([]APIBreakdown, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT api_key, model,
		       COALESCE(SUM(request_count),0),
		       COALESCE(SUM(success_count),0),
		       COALESCE(SUM(failure_count),0),
		       COALESCE(SUM(total_tokens),0)
		FROM usage_minute_rollup
		WHERE bucket_start >= $1 AND bucket_start < $2
		GROUP BY api_key, model
		ORDER BY SUM(total_tokens) DESC
		LIMIT 200`, from.UTC(), to.UTC())
	if err != nil {
		return nil, fmt.Errorf("usage QueryAPIBreakdown: %w", err)
	}
	defer rows.Close()
	var out []APIBreakdown
	for rows.Next() {
		var a APIBreakdown
		if err := rows.Scan(&a.APIKey, &a.Model, &a.Requests,
			&a.SuccessCount, &a.FailureCount, &a.Tokens); err != nil {
			return nil, fmt.Errorf("usage QueryAPIBreakdown scan: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// SparklinePoint is a per-minute aggregated bucket (cluster-wide). Used by
// StatCards which show "last hour activity" mini-charts.
type SparklinePoint struct {
	Bucket   time.Time `json:"bucket"`
	Requests int64     `json:"requests"`
	Tokens   int64     `json:"tokens"`
}

// QuerySparklineSeries returns per-minute aggregates without date_trunc —
// the rollup is already minute-granular so we just GROUP BY bucket_start.
func (s *PGStore) QuerySparklineSeries(ctx context.Context, from, to time.Time) ([]SparklinePoint, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT bucket_start,
		       COALESCE(SUM(request_count),0),
		       COALESCE(SUM(total_tokens),0)
		FROM usage_minute_rollup
		WHERE bucket_start >= $1 AND bucket_start < $2
		GROUP BY bucket_start
		ORDER BY bucket_start`, from.UTC(), to.UTC())
	if err != nil {
		return nil, fmt.Errorf("usage QuerySparklineSeries: %w", err)
	}
	defer rows.Close()
	var out []SparklinePoint
	for rows.Next() {
		var p SparklinePoint
		if err := rows.Scan(&p.Bucket, &p.Requests, &p.Tokens); err != nil {
			return nil, fmt.Errorf("usage QuerySparklineSeries scan: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// HealthBucket is one hour-cell of the 7×24 service-health grid.
type HealthBucket struct {
	HourStart    time.Time `json:"hour_start"`
	RequestCount int64     `json:"request_count"`
	SuccessCount int64     `json:"success_count"`
	FailureCount int64     `json:"failure_count"`
}

// QueryServiceHealthGrid returns one HealthBucket per hour in [from, to).
// Empty hours produce no row — the UI draws those as "no traffic" cells.
func (s *PGStore) QueryServiceHealthGrid(ctx context.Context, from, to time.Time) ([]HealthBucket, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT date_trunc('hour', bucket_start) AS hr,
		       COALESCE(SUM(request_count),0),
		       COALESCE(SUM(success_count),0),
		       COALESCE(SUM(failure_count),0)
		FROM usage_minute_rollup
		WHERE bucket_start >= $1 AND bucket_start < $2
		GROUP BY hr
		ORDER BY hr`, from.UTC(), to.UTC())
	if err != nil {
		return nil, fmt.Errorf("usage QueryServiceHealthGrid: %w", err)
	}
	defer rows.Close()
	var out []HealthBucket
	for rows.Next() {
		var b HealthBucket
		if err := rows.Scan(&b.HourStart, &b.RequestCount, &b.SuccessCount, &b.FailureCount); err != nil {
			return nil, fmt.Errorf("usage QueryServiceHealthGrid scan: %w", err)
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// CredentialBreakdown is one (source, auth_index) pair. This reads
// usage_events (raw rows) — NOT the rollup — because rollup PK doesn't
// carry source/auth_index. Bounded by event TTL (default 7 days), so
// front-end must request a smaller window for credential queries.
type CredentialBreakdown struct {
	Source       string `json:"source"`
	AuthIndex    string `json:"auth_index"`
	Requests     int64  `json:"requests"`
	Tokens       int64  `json:"tokens"`
	SuccessCount int64  `json:"success_count"`
	FailureCount int64  `json:"failure_count"`
}

func (s *PGStore) QueryCredentialBreakdown(ctx context.Context, from, to time.Time) ([]CredentialBreakdown, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT source, auth_index,
		       COUNT(*),
		       COALESCE(SUM(total_tokens),0),
		       SUM(CASE WHEN failed THEN 0 ELSE 1 END),
		       SUM(CASE WHEN failed THEN 1 ELSE 0 END)
		FROM usage_events
		WHERE occurred_at >= $1 AND occurred_at < $2
		GROUP BY source, auth_index
		ORDER BY COUNT(*) DESC
		LIMIT 200`, from.UTC(), to.UTC())
	if err != nil {
		return nil, fmt.Errorf("usage QueryCredentialBreakdown: %w", err)
	}
	defer rows.Close()
	var out []CredentialBreakdown
	for rows.Next() {
		var b CredentialBreakdown
		if err := rows.Scan(&b.Source, &b.AuthIndex,
			&b.Requests, &b.Tokens, &b.SuccessCount, &b.FailureCount); err != nil {
			return nil, fmt.Errorf("usage QueryCredentialBreakdown scan: %w", err)
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// QueryEventDetails returns raw event rows for the /usage?include=details
// path. Bounded by limit (caller passes 500 by default) and ordered by
// occurred_at DESC so the most recent activity is on page 1. apiFilter
// and modelFilter are exact-match (empty string = no filter).
func (s *PGStore) QueryEventDetails(ctx context.Context, from, to time.Time, limit, offset int, apiFilter, modelFilter string) ([]UsageEventRow, error) {
	if limit <= 0 || limit > 5000 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	// Build WHERE dynamically — we want the indexed (occurred_at, api_key,
	// model) prefix to kick in when filters are present, but skip the
	// extra predicates entirely otherwise.
	var sb strings.Builder
	sb.WriteString(`SELECT occurred_at, node_id, api_key, provider, model,
		source, auth_id, auth_index, auth_type, failed, latency_ms,
		input_tokens, output_tokens, reasoning_tokens, cached_tokens,
		total_tokens, dedup_hash
		FROM usage_events
		WHERE occurred_at >= $1 AND occurred_at < $2`)
	args := []any{from.UTC(), to.UTC()}
	if apiFilter != "" {
		args = append(args, apiFilter)
		fmt.Fprintf(&sb, ` AND api_key = $%d`, len(args))
	}
	if modelFilter != "" {
		args = append(args, modelFilter)
		fmt.Fprintf(&sb, ` AND model = $%d`, len(args))
	}
	args = append(args, limit, offset)
	fmt.Fprintf(&sb, ` ORDER BY occurred_at DESC LIMIT $%d OFFSET $%d`,
		len(args)-1, len(args))

	rows, err := s.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("usage QueryEventDetails: %w", err)
	}
	defer rows.Close()
	var out []UsageEventRow
	for rows.Next() {
		var r UsageEventRow
		if err := rows.Scan(
			&r.OccurredAt, &r.NodeID, &r.APIKey, &r.Provider, &r.Model,
			&r.Source, &r.AuthID, &r.AuthIndex, &r.AuthType, &r.Failed, &r.LatencyMs,
			&r.InputTokens, &r.OutputTokens, &r.ReasoningTokens, &r.CachedTokens,
			&r.TotalTokens, &r.DedupHash,
		); err != nil {
			return nil, fmt.Errorf("usage QueryEventDetails scan: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RebuildRollupForRange recomputes usage_minute_rollup from raw
// usage_events for [from, to). The conflict update uses EXCLUDED.* (not
// the additive +EXCLUDED form bulkUpsertRollup uses) — this REPLACES
// the affected rollup rows from raw events, which is the correct
// semantics for an administrative bulk-import: the source of truth is
// the imported event stream, not the stale rollup totals.
//
// NOT used in the hot path. Imports are one-shot operations.
func (s *PGStore) RebuildRollupForRange(ctx context.Context, from, to time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		WITH agg AS (
			SELECT date_trunc('minute', occurred_at) AS bucket,
			       node_id, api_key, model,
			       COUNT(*) AS req,
			       SUM(CASE WHEN failed THEN 0 ELSE 1 END) AS succ,
			       SUM(CASE WHEN failed THEN 1 ELSE 0 END) AS fail,
			       COALESCE(SUM(input_tokens),0)     AS itok,
			       COALESCE(SUM(output_tokens),0)    AS otok,
			       COALESCE(SUM(reasoning_tokens),0) AS rtok,
			       COALESCE(SUM(cached_tokens),0)    AS ctok,
			       COALESCE(SUM(total_tokens),0)    AS ttok,
			       COALESCE(SUM(latency_ms),0)      AS lat
			FROM usage_events
			WHERE occurred_at >= $1 AND occurred_at < $2
			GROUP BY bucket, node_id, api_key, model
		)
		INSERT INTO usage_minute_rollup AS r
			(bucket_start, node_id, api_key, model,
			 request_count, success_count, failure_count,
			 input_tokens, output_tokens, reasoning_tokens, cached_tokens,
			 total_tokens, latency_ms_sum)
		SELECT bucket, node_id, api_key, model, req, succ, fail,
		       itok, otok, rtok, ctok, ttok, lat
		FROM agg
		ON CONFLICT (bucket_start, node_id, api_key, model) DO UPDATE SET
			request_count    = EXCLUDED.request_count,
			success_count    = EXCLUDED.success_count,
			failure_count    = EXCLUDED.failure_count,
			input_tokens     = EXCLUDED.input_tokens,
			output_tokens    = EXCLUDED.output_tokens,
			reasoning_tokens = EXCLUDED.reasoning_tokens,
			cached_tokens    = EXCLUDED.cached_tokens,
			total_tokens     = EXCLUDED.total_tokens,
			latency_ms_sum   = EXCLUDED.latency_ms_sum,
			updated_at       = NOW()`, from.UTC(), to.UTC())
	if err != nil {
		return fmt.Errorf("usage RebuildRollupForRange: %w", err)
	}
	return nil
}

// QueryActiveNodeCount returns COUNT(DISTINCT node_id) over rollup rows in
// [from, to). Reflects who actually contributed traffic in the window —
// more truthful than counting cluster_nodes membership (a node can be in
// the ring but idle).
func (s *PGStore) QueryActiveNodeCount(ctx context.Context, from, to time.Time) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT node_id)
		FROM usage_minute_rollup
		WHERE bucket_start >= $1 AND bucket_start < $2`,
		from.UTC(), to.UTC()).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("usage QueryActiveNodeCount: %w", err)
	}
	return n, nil
}

// QueryClusterNodeCount returns the number of cluster_nodes rows whose
// last_heartbeat is fresh (within stalenessSeconds). This is the "ring
// size" — how many nodes are alive in the cluster, regardless of whether
// they served any LLM traffic in the query window. The UI uses this
// alongside QueryActiveNodeCount to show "2/4 nodes contributing".
func (s *PGStore) QueryClusterNodeCount(ctx context.Context, stalenessSeconds int) (int, error) {
	if stalenessSeconds <= 0 {
		stalenessSeconds = 60
	}
	var n int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM cluster_nodes
		WHERE last_heartbeat > NOW() - ($1 || ' seconds')::INTERVAL`,
		fmt.Sprintf("%d", stalenessSeconds)).Scan(&n)
	if err != nil {
		// Table may not exist on legacy deployments — return 0, not error,
		// so the UI degrades gracefully (badge just shows active count
		// without the "/N" suffix).
		return 0, nil
	}
	return n, nil
}

// schemaDDL mirrors the usage-related DDL embedded in
// postgresstore.EnsureSchema. Used only by PGStore.EnsureSchema for tests
// that bring up a bare PG. KEEP IN SYNC with postgresstore.go.
var schemaDDL = []string{
	`CREATE TABLE IF NOT EXISTS usage_events (
		id               BIGSERIAL PRIMARY KEY,
		occurred_at      TIMESTAMPTZ NOT NULL,
		node_id          TEXT NOT NULL,
		api_key          TEXT NOT NULL,
		provider         TEXT NOT NULL DEFAULT '',
		model            TEXT NOT NULL DEFAULT 'unknown',
		source           TEXT NOT NULL DEFAULT '',
		auth_id          TEXT NOT NULL DEFAULT '',
		auth_index       TEXT NOT NULL DEFAULT '',
		auth_type        TEXT NOT NULL DEFAULT '',
		failed           BOOLEAN NOT NULL,
		latency_ms       BIGINT NOT NULL DEFAULT 0,
		input_tokens     BIGINT NOT NULL DEFAULT 0,
		output_tokens    BIGINT NOT NULL DEFAULT 0,
		reasoning_tokens BIGINT NOT NULL DEFAULT 0,
		cached_tokens    BIGINT NOT NULL DEFAULT 0,
		total_tokens     BIGINT NOT NULL DEFAULT 0,
		dedup_hash       BYTEA NOT NULL,
		inserted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`,
	`CREATE INDEX IF NOT EXISTS idx_usage_events_occurred_at ON usage_events(occurred_at DESC)`,
	`CREATE INDEX IF NOT EXISTS idx_usage_events_api_model   ON usage_events(api_key, model, occurred_at DESC)`,
	`CREATE INDEX IF NOT EXISTS idx_usage_events_node        ON usage_events(node_id, occurred_at DESC)`,
	`CREATE INDEX IF NOT EXISTS idx_usage_events_source      ON usage_events(source, occurred_at DESC)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS uq_usage_events_dedup ON usage_events(dedup_hash)`,
	`CREATE TABLE IF NOT EXISTS usage_minute_rollup (
		bucket_start     TIMESTAMPTZ NOT NULL,
		node_id          TEXT NOT NULL,
		api_key          TEXT NOT NULL,
		model            TEXT NOT NULL,
		request_count    BIGINT NOT NULL DEFAULT 0,
		success_count    BIGINT NOT NULL DEFAULT 0,
		failure_count    BIGINT NOT NULL DEFAULT 0,
		input_tokens     BIGINT NOT NULL DEFAULT 0,
		output_tokens    BIGINT NOT NULL DEFAULT 0,
		reasoning_tokens BIGINT NOT NULL DEFAULT 0,
		cached_tokens    BIGINT NOT NULL DEFAULT 0,
		total_tokens     BIGINT NOT NULL DEFAULT 0,
		latency_ms_sum   BIGINT NOT NULL DEFAULT 0,
		updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		PRIMARY KEY (bucket_start, node_id, api_key, model)
	)`,
	`CREATE INDEX IF NOT EXISTS idx_usage_rollup_bucket ON usage_minute_rollup(bucket_start DESC)`,
	`CREATE INDEX IF NOT EXISTS idx_usage_rollup_api    ON usage_minute_rollup(api_key, bucket_start DESC)`,
	`CREATE INDEX IF NOT EXISTS idx_usage_rollup_model  ON usage_minute_rollup(model, bucket_start DESC)`,
}
