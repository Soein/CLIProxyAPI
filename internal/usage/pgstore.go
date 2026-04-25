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
