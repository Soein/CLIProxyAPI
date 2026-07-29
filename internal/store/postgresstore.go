package store

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/authfilelock"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/misc"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const (
	defaultConfigTable                     = "config_store"
	defaultAuthTable                       = "auth_store"
	defaultCooldownTable                   = "cooldown_store"
	defaultAuthBatchCommitTable            = "auth_batch_commits"
	defaultAuthLifecycleClockTable         = "auth_lifecycle_clock"
	defaultConfigKey                       = "config"
	postgresAuthGenerationPayloadKey       = "__cliproxy_internal_postgres_version"
	authLifecycleLockClass           int32 = 4
	authLifecycleLockKey             int32 = 0
)

var authCommitConfirmationTimeout = 5 * time.Second

const defaultAuthDetachedTransactionTimeout = 30 * time.Second

// authDetachedTransactionTimeout bounds lifecycle mutations after caller
// cancellation has been detached. It is a variable so the watchdog can be
// exercised without slow tests; production keeps the constant default.
var authDetachedTransactionTimeout = defaultAuthDetachedTransactionTimeout

type postgresDedicatedTransaction struct {
	ctx            context.Context
	cancel         context.CancelFunc
	cancelParent   context.CancelFunc
	conn           *sql.Conn
	tx             *sql.Tx
	watchdogDone   chan struct{}
	finishOnce     sync.Once
	deadlineClosed atomic.Bool
}

func (s *PostgresStore) beginDetachedAuthTransaction(ctx context.Context) (*postgresDedicatedTransaction, error) {
	parent := context.Background()
	if ctx != nil {
		parent = context.WithoutCancel(ctx)
	}
	timeout := authDetachedTransactionTimeout
	if timeout <= 0 {
		timeout = defaultAuthDetachedTransactionTimeout
	}
	detachedCtx, cancelDetached := context.WithTimeout(parent, timeout)
	transaction, errBegin := s.beginDedicatedTransaction(detachedCtx)
	if errBegin != nil {
		cancelDetached()
		return nil, errBegin
	}
	transaction.cancelParent = cancelDetached
	return transaction, nil
}

func (s *PostgresStore) beginDedicatedTransaction(ctx context.Context) (*postgresDedicatedTransaction, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	txCtx, cancel := context.WithCancel(ctx)
	conn, errConn := s.db.Conn(txCtx)
	if errConn != nil {
		cancel()
		return nil, errConn
	}
	var rawCloser interface{ Close() error }
	if errRaw := conn.Raw(func(rawConn any) error {
		var ok bool
		rawCloser, ok = rawConn.(interface{ Close() error })
		if !ok {
			return errors.New("postgres driver connection cannot be closed")
		}
		return nil
	}); errRaw != nil {
		cancel()
		_ = conn.Close()
		return nil, errRaw
	}
	transaction := &postgresDedicatedTransaction{
		ctx:          txCtx,
		cancel:       cancel,
		conn:         conn,
		watchdogDone: make(chan struct{}),
	}
	// database/sql cannot cancel driver.Tx.Commit because Commit has no context.
	// Keep the physical driver connection so the deadline can interrupt Begin,
	// statements, and Commit even when a driver ignores the context itself.
	go func() {
		<-txCtx.Done()
		if errors.Is(txCtx.Err(), context.DeadlineExceeded) {
			transaction.deadlineClosed.Store(true)
			if errClose := rawCloser.Close(); errClose != nil {
				log.WithError(errClose).Warn("postgres store: close expired dedicated transaction connection failed")
			}
		}
		close(transaction.watchdogDone)
	}()
	tx, errBegin := conn.BeginTx(txCtx, nil)
	if errBegin != nil {
		transaction.finish()
		return nil, errBegin
	}
	transaction.tx = tx
	return transaction, nil
}

func (transaction *postgresDedicatedTransaction) finish() {
	if transaction == nil {
		return
	}
	transaction.finishOnce.Do(func() {
		transaction.cancel()
		if transaction.cancelParent != nil {
			transaction.cancelParent()
		}
		<-transaction.watchdogDone
		if transaction.deadlineClosed.Load() {
			// Returning ErrBadConn from Raw makes database/sql discard the wrapper
			// instead of putting the already-closed physical connection back in the pool.
			_ = transaction.conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		if errClose := transaction.conn.Close(); errClose != nil {
			log.WithError(errClose).Warn("postgres store: close dedicated transaction connection failed")
		}
	})
}

func (transaction *postgresDedicatedTransaction) rollback() {
	if transaction == nil {
		return
	}
	if transaction.tx != nil {
		_ = transaction.tx.Rollback()
	}
	transaction.finish()
}

func (transaction *postgresDedicatedTransaction) commit() error {
	if transaction == nil || transaction.tx == nil {
		return errors.New("transaction is unavailable")
	}
	errCommit := transaction.tx.Commit()
	transaction.finish()
	return errCommit
}

// PostgresStoreConfig captures configuration required to initialize a Postgres-backed store.
type PostgresStoreConfig struct {
	DSN string
	// ReadDSN is an optional read-only DSN used for non-auth SELECT-heavy paths
	// such as syncConfigFromDatabase. Authentication lifecycle reads always use
	// the writer because replica lag can reintroduce a tombstoned credential. When
	// non-empty and reachable, queries that don't mutate state are routed to
	// this pool so cold-start full-table scans can hit a local read replica
	// rather than crossing the WAN to the write leader. Writes and DDL
	// (EnsureSchema, persistAuth, persistConfig, *Delete*) always use DSN.
	// When empty or the pool fails its initial ping, reads transparently
	// fall back to DSN.
	ReadDSN       string
	Schema        string
	ConfigTable   string
	AuthTable     string
	CooldownTable string
	SpoolDir      string
}

// PostgresStore persists configuration and authentication metadata using PostgreSQL as backend
// while mirroring data to a local workspace so existing file-based workflows continue to operate.
type PostgresStore struct {
	db *sql.DB
	// readDB, when non-nil, points at a dedicated read-only pool (e.g. an HAProxy
	// read backend pointing to local replicas). Reads opt in via readPool();
	// writes always go through db.
	readDB        *sql.DB
	cfg           PostgresStoreConfig
	spoolRoot     string
	configPath    string
	authDir       string
	nodeID        string
	cooldownStore *postgresCooldownStateStore
	mu            sync.Mutex
	generationMu  sync.RWMutex
	// authGenerations tracks generations installed into this node's local
	// mirrors. Plain List/Get reads do not advance it: a delayed watcher delete
	// must retain the generation of the mirror that actually disappeared.
	authGenerations map[string]uint64
}

var (
	_ cliproxyauth.VersionedAuthStore          = (*PostgresStore)(nil)
	_ cliproxyauth.AuthTombstoneStore          = (*PostgresStore)(nil)
	_ cliproxyauth.AuthRestoreStore            = (*PostgresStore)(nil)
	_ cliproxyauth.AuthLifecycleStore          = (*PostgresStore)(nil)
	_ cliproxyauth.AuthAuthoritativeBatchStore = (*PostgresStore)(nil)
	_ cliproxyauth.AuthDeletedMirrorStore      = (*PostgresStore)(nil)
	_ cliproxyauth.AuthMirrorReconciler        = (*PostgresStore)(nil)
)

// NodeID sets an identifier recorded in last_writer on UPSERT. Used in cluster
// mode to trace which replica last modified a row; optional.
func (s *PostgresStore) NodeID() string {
	if s == nil {
		return ""
	}
	return s.nodeID
}

// SetNodeID updates the writer identity used when persisting rows.
func (s *PostgresStore) SetNodeID(id string) {
	if s == nil {
		return
	}
	s.nodeID = id
}

// nodeWriter returns the value written into the last_writer column. A hostname
// fallback keeps rows meaningful even when node_id was not wired.
func (s *PostgresStore) nodeWriter() any {
	if s == nil || strings.TrimSpace(s.nodeID) == "" {
		if h, err := os.Hostname(); err == nil && h != "" {
			return h
		}
		return "unknown"
	}
	return s.nodeID
}

func (s *PostgresStore) knownAuthGeneration(id string) (uint64, bool) {
	if s == nil {
		return 0, false
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return 0, false
	}
	s.generationMu.RLock()
	generation, ok := s.authGenerations[id]
	s.generationMu.RUnlock()
	return generation, ok
}

func (s *PostgresStore) rememberAuthGeneration(id string, generation uint64) {
	if s == nil || generation == 0 {
		return
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return
	}
	s.generationMu.Lock()
	if s.authGenerations == nil {
		s.authGenerations = make(map[string]uint64)
	}
	if generation > s.authGenerations[id] {
		s.authGenerations[id] = generation
	}
	s.generationMu.Unlock()
}

// NewPostgresStore establishes a connection to PostgreSQL and prepares the local workspace.
func NewPostgresStore(ctx context.Context, cfg PostgresStoreConfig) (*PostgresStore, error) {
	trimmedDSN := strings.TrimSpace(cfg.DSN)
	if trimmedDSN == "" {
		return nil, fmt.Errorf("postgres store: DSN is required")
	}
	cfg.DSN = trimmedDSN
	cfg.Schema = strings.TrimSpace(cfg.Schema)
	if cfg.ConfigTable == "" {
		cfg.ConfigTable = defaultConfigTable
	}
	if cfg.AuthTable == "" {
		cfg.AuthTable = defaultAuthTable
	}
	if cfg.CooldownTable == "" {
		cfg.CooldownTable = defaultCooldownTable
	}

	spoolRoot := strings.TrimSpace(cfg.SpoolDir)
	if spoolRoot == "" {
		if cwd, err := os.Getwd(); err == nil {
			spoolRoot = filepath.Join(cwd, "pgstore")
		} else {
			spoolRoot = filepath.Join(os.TempDir(), "pgstore")
		}
	}
	absSpool, err := filepath.Abs(spoolRoot)
	if err != nil {
		return nil, fmt.Errorf("postgres store: resolve spool directory: %w", err)
	}
	configDir := filepath.Join(absSpool, "config")
	authDir := filepath.Join(absSpool, "auths")
	if err = os.MkdirAll(configDir, 0o700); err != nil {
		return nil, fmt.Errorf("postgres store: create config directory: %w", err)
	}
	if err = os.MkdirAll(authDir, 0o700); err != nil {
		return nil, fmt.Errorf("postgres store: create auth directory: %w", err)
	}

	db, err := openPostgresDB(cfg.DSN, cfg.Schema)
	if err != nil {
		return nil, fmt.Errorf("postgres store: open database connection: %w", err)
	}
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres store: ping database: %w", err)
	}

	// Optional read pool: only opened when ReadDSN is set and differs from DSN.
	// A failed ping is non-fatal — we log a warning and let readPool() fall back
	// to the write pool, keeping the store usable even if the replica is
	// temporarily unreachable.
	var readDB *sql.DB
	trimmedRead := strings.TrimSpace(cfg.ReadDSN)
	if trimmedRead != "" && trimmedRead != cfg.DSN {
		cfg.ReadDSN = trimmedRead
		rdb, errOpen := openPostgresDB(trimmedRead, cfg.Schema)
		if errOpen != nil {
			log.WithError(errOpen).Warn("postgres store: open read database failed; reads will fall back to write pool")
		} else if errPing := rdb.PingContext(ctx); errPing != nil {
			_ = rdb.Close()
			log.WithError(errPing).Warn("postgres store: read database ping failed; reads will fall back to write pool")
		} else {
			readDB = rdb
		}
	} else {
		// Normalize so s.cfg.ReadDSN reflects the trimmed value (or empty
		// string when DSN==ReadDSN), keeping s.cfg internally consistent.
		cfg.ReadDSN = ""
	}

	store := &PostgresStore{
		db:              db,
		readDB:          readDB,
		cfg:             cfg,
		spoolRoot:       absSpool,
		configPath:      filepath.Join(configDir, "config.yaml"),
		authDir:         authDir,
		authGenerations: make(map[string]uint64),
	}
	store.cooldownStore = &postgresCooldownStateStore{store: store}
	return store, nil
}

func openPostgresDB(dsn, schema string) (*sql.DB, error) {
	connConfig, err := postgresConnConfig(dsn, schema)
	if err != nil {
		return nil, err
	}
	return stdlib.OpenDB(*connConfig), nil
}

func postgresConnConfig(dsn, schema string) (*pgx.ConnConfig, error) {
	connConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if schema != "" {
		if connConfig.RuntimeParams == nil {
			connConfig.RuntimeParams = make(map[string]string)
		}
		// Startup parameters are applied to every physical connection opened by
		// database/sql. Quoting the schema as one PostgreSQL identifier prevents
		// commas or quotes in configuration from adding fallback schemas.
		connConfig.RuntimeParams["search_path"] = pgx.Identifier{schema}.Sanitize()
	}
	return connConfig, nil
}

// Close releases the underlying database connections. Read pool errors are
// joined with the write pool error so a failure in either path is surfaced
// rather than silently dropped.
func (s *PostgresStore) Close() error {
	if s == nil {
		return nil
	}
	var errs []error
	// readDB != db is a defensive guard against a future refactor that hands
	// the same *sql.DB to both fields; today NewPostgresStore always opens
	// readDB via a separate sql.Open call so the pointers diverge by
	// construction.
	if s.readDB != nil && s.readDB != s.db {
		if err := s.readDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("read pool: %w", err))
		}
	}
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// readPool returns the pool used for read-only queries. Falls back to the
// write pool when no read replica is configured or the replica failed its
// initial ping. Callers must ensure the store is initialized; nil-receiver
// behavior matches the rest of the read path (panic), keeping the convention
// uniform across List / GetByID / syncAuth* / syncConfig*.
func (s *PostgresStore) readPool() *sql.DB {
	if s.readDB != nil {
		return s.readDB
	}
	return s.db
}

// DB returns the underlying *sql.DB. Exposed so cluster-mode coordinators
// (leader elector, advisory lock refresh locker, LISTEN/NOTIFY subscriber)
// can share the pool instead of opening a second one. Returns nil on a
// zero-value store.
func (s *PostgresStore) DB() *sql.DB {
	if s == nil {
		return nil
	}
	return s.db
}

// DSN returns the original connection string supplied at construction. Used
// by the ChangeSubscriber which needs its own pgx-native connection (LISTEN
// is not supported via database/sql).
func (s *PostgresStore) DSN() string {
	if s == nil {
		return ""
	}
	return s.cfg.DSN
}

// EnsureSchema creates the required tables (and schema when provided).
func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres store: not initialized")
	}
	if schema := strings.TrimSpace(s.cfg.Schema); schema != "" {
		query := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", quoteIdentifier(schema))
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("postgres store: create schema: %w", err)
		}
	}
	configTable := s.fullTableName(s.cfg.ConfigTable)
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			content TEXT NOT NULL,
			version BIGINT NOT NULL DEFAULT 0,
			last_writer TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, configTable)); err != nil {
		return fmt.Errorf("postgres store: create config table: %w", err)
	}
	// HA additive columns (idempotent) for clusters upgraded from pre-HA schema.
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0",
		configTable,
	)); err != nil {
		return fmt.Errorf("postgres store: add version col to config table: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN IF NOT EXISTS last_writer TEXT",
		configTable,
	)); err != nil {
		return fmt.Errorf("postgres store: add last_writer col to config table: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SMALLINT PRIMARY KEY CHECK (id = 1),
			value BIGINT NOT NULL CHECK (value >= 0)
		)
	`, s.fullTableName(defaultAuthLifecycleClockTable))); err != nil {
		return fmt.Errorf("postgres store: create auth lifecycle clock: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (id, value) VALUES (1, 0) ON CONFLICT (id) DO NOTHING",
		s.fullTableName(defaultAuthLifecycleClockTable),
	)); err != nil {
		return fmt.Errorf("postgres store: initialize auth lifecycle clock: %w", err)
	}

	authTable := s.fullTableName(s.cfg.AuthTable)
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			content JSONB NOT NULL,
			version BIGINT NOT NULL DEFAULT 0,
			lifecycle_version BIGINT NOT NULL DEFAULT 0,
			deleted BOOLEAN NOT NULL DEFAULT FALSE,
			deleted_at TIMESTAMPTZ,
			last_writer TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, authTable)); err != nil {
		return fmt.Errorf("postgres store: create auth table: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0",
		authTable,
	)); err != nil {
		return fmt.Errorf("postgres store: add version col to auth table: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		"UPDATE %s SET version = 1 WHERE version = 0",
		authTable,
	)); err != nil {
		return fmt.Errorf("postgres store: initialize auth generations: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN IF NOT EXISTS last_writer TEXT",
		authTable,
	)); err != nil {
		return fmt.Errorf("postgres store: add last_writer col to auth table: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN IF NOT EXISTS deleted BOOLEAN NOT NULL DEFAULT FALSE",
		authTable,
	)); err != nil {
		return fmt.Errorf("postgres store: add deleted col to auth table: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ",
		authTable,
	)); err != nil {
		return fmt.Errorf("postgres store: add deleted_at col to auth table: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN IF NOT EXISTS lifecycle_version BIGINT NOT NULL DEFAULT 0",
		authTable,
	)); err != nil {
		return fmt.Errorf("postgres store: add lifecycle_version col to auth table: %w", err)
	}
	backfillTx, errBackfillBegin := s.db.BeginTx(ctx, nil)
	if errBackfillBegin != nil {
		return fmt.Errorf("postgres store: begin auth lifecycle backfill: %w", errBackfillBegin)
	}
	defer func() { _ = backfillTx.Rollback() }()
	if errLock := s.lockAuthLifecycle(ctx, backfillTx); errLock != nil {
		return fmt.Errorf("postgres store: lock auth lifecycle backfill: %w", errLock)
	}
	if _, errHighWater := backfillTx.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET value = GREATEST(value, COALESCE((SELECT MAX(lifecycle_version) FROM %s), 0))
		WHERE id = 1
	`, s.fullTableName(defaultAuthLifecycleClockTable), authTable)); errHighWater != nil {
		return fmt.Errorf("postgres store: synchronize auth lifecycle high-water mark: %w", errHighWater)
	}
	if _, errAdvance := backfillTx.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET value = value + 1
		WHERE id = 1
	`, s.fullTableName(defaultAuthLifecycleClockTable))); errAdvance != nil {
		return fmt.Errorf("postgres store: allocate auth lifecycle backfill version: %w", errAdvance)
	}
	if _, errBackfill := backfillTx.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET lifecycle_version = (SELECT value FROM %s WHERE id = 1)
		WHERE lifecycle_version = 0
	`, authTable, s.fullTableName(defaultAuthLifecycleClockTable))); errBackfill != nil {
		return fmt.Errorf("postgres store: initialize auth lifecycle versions: %w", errBackfill)
	}
	if errBackfillCommit := backfillTx.Commit(); errBackfillCommit != nil {
		return fmt.Errorf("postgres store: commit auth lifecycle backfill: %w", errBackfillCommit)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id UUID PRIMARY KEY,
			committed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, s.fullTableName(defaultAuthBatchCommitTable))); err != nil {
		return fmt.Errorf("postgres store: create auth batch commit table: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		"DELETE FROM %s WHERE committed_at < NOW() - INTERVAL '24 hours'",
		s.fullTableName(defaultAuthBatchCommitTable),
	)); err != nil {
		log.WithError(err).Warn("postgres store: prune expired auth batch commit markers failed")
	}

	// cluster_nodes serves two roles in cluster mode:
	//   (1) LeaderElector heartbeat (role/metadata)
	//   (2) InstanceRegistrar routing metadata (endpoint/weight/status) —
	//       consumed by new-api's consistent-hash router (Phase 4).
	// Each writer owns its own columns via ON CONFLICT DO UPDATE SET so the
	// two goroutines do not clobber each other.
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS cluster_nodes (
			node_id         TEXT PRIMARY KEY,
			role            TEXT,
			region          TEXT,
			last_heartbeat  TIMESTAMPTZ NOT NULL,
			metadata        JSONB,
			endpoint        TEXT,
			weight          INT  NOT NULL DEFAULT 100,
			status          TEXT NOT NULL DEFAULT 'active'
		)
	`); err != nil {
		return fmt.Errorf("postgres store: create cluster_nodes: %w", err)
	}
	// Upgrade path for clusters initialized pre-Phase-4 (role NOT NULL,
	// no endpoint/weight/status). ALTER ... DROP NOT NULL is idempotent and
	// no-op when the column is already nullable.
	for _, stmt := range []string{
		"ALTER TABLE cluster_nodes ALTER COLUMN role DROP NOT NULL",
		"ALTER TABLE cluster_nodes ADD COLUMN IF NOT EXISTS endpoint TEXT",
		"ALTER TABLE cluster_nodes ADD COLUMN IF NOT EXISTS weight INT NOT NULL DEFAULT 100",
		"ALTER TABLE cluster_nodes ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'",
	} {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("postgres store: alter cluster_nodes: %w", err)
		}
	}
	if _, err := s.db.ExecContext(ctx,
		"CREATE INDEX IF NOT EXISTS idx_cluster_nodes_heartbeat ON cluster_nodes(last_heartbeat DESC)",
	); err != nil {
		return fmt.Errorf("postgres store: create cluster_nodes index: %w", err)
	}
	if _, err := s.db.ExecContext(ctx,
		"CREATE INDEX IF NOT EXISTS idx_cluster_nodes_status ON cluster_nodes(status, last_heartbeat DESC)",
	); err != nil {
		return fmt.Errorf("postgres store: create cluster_nodes status index: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS cluster_membership_state (
			id           SMALLINT PRIMARY KEY CHECK (id = 1),
			epoch        BIGINT NOT NULL DEFAULT 0 CHECK (epoch >= 0),
			fingerprint  BYTEA NOT NULL DEFAULT ''::bytea,
			staleness_ms BIGINT NOT NULL DEFAULT 0 CHECK (staleness_ms >= 0),
			updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		return fmt.Errorf("postgres store: create cluster membership state: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO cluster_membership_state (id)
		VALUES (1)
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		return fmt.Errorf("postgres store: initialize cluster membership state: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS auth_dispatch_leases (
			auth_id           TEXT PRIMARY KEY,
			owner_node_id     TEXT NOT NULL,
			owner_instance_id UUID NOT NULL,
			membership_epoch  BIGINT NOT NULL,
			owner_epoch       BIGINT NOT NULL CHECK (owner_epoch > 0),
			lease_until       TIMESTAMPTZ NOT NULL,
			updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		return fmt.Errorf("postgres store: create auth dispatch leases: %w", err)
	}
	// Phase 4: NOTIFY channel consumed by new-api's HashRing watcher. Fires
	// on routing-relevant column changes only (endpoint/weight/status) plus
	// DELETE — we deliberately skip UPDATE of role/last_heartbeat to avoid
	// flooding the channel with every 5s leader heartbeat.
	// The function suppresses no-op UPDATE notifies: InstanceRegistrar
	// re-writes identical endpoint/weight/status every 10s, and without
	// this guard each heartbeat would fire a NOTIFY and every replica
	// would execute a full refresh query (N² behavior). INSERT/DELETE
	// still notify unconditionally. A trigger-level WHEN can't express
	// this because WHEN can only reference OLD on UPDATE/DELETE and NEW
	// on INSERT/UPDATE — mixed INSERT OR UPDATE OR DELETE triggers can't
	// reference both.
	if _, err := s.db.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION notify_cpa_instance_changed()
		RETURNS TRIGGER AS $BODY$
		BEGIN
			IF TG_OP = 'UPDATE'
			   AND OLD.endpoint IS NOT DISTINCT FROM NEW.endpoint
			   AND OLD.weight   IS NOT DISTINCT FROM NEW.weight
			   AND OLD.status   IS NOT DISTINCT FROM NEW.status THEN
				RETURN NULL;
			END IF;
			PERFORM pg_notify('cpa_instance_changed', COALESCE(NEW.node_id, OLD.node_id));
			RETURN COALESCE(NEW, OLD);
		END;
		$BODY$ LANGUAGE plpgsql
	`); err != nil {
		return fmt.Errorf("postgres store: create notify_cpa_instance_changed function: %w", err)
	}
	if _, err := s.db.ExecContext(ctx,
		"DROP TRIGGER IF EXISTS trg_cpa_instance_changed ON cluster_nodes",
	); err != nil {
		return fmt.Errorf("postgres store: drop old trg_cpa_instance_changed: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		CREATE TRIGGER trg_cpa_instance_changed
			AFTER INSERT OR UPDATE OF endpoint, weight, status OR DELETE
			ON cluster_nodes
			FOR EACH ROW EXECUTE FUNCTION notify_cpa_instance_changed()
	`); err != nil {
		return fmt.Errorf("postgres store: create trg_cpa_instance_changed: %w", err)
	}

	// usage_events: raw per-request rows. Drives the detail panel and
	// /usage/export when usage.backend=pg. Pruned by the leader-gated cleanup
	// goroutine to UsageConfig.EventRetentionDays (default 7d).
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS usage_events (
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
		)
	`); err != nil {
		return fmt.Errorf("postgres store: create usage_events: %w", err)
	}
	for _, ddl := range []string{
		`CREATE INDEX IF NOT EXISTS idx_usage_events_occurred_at ON usage_events(occurred_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_events_api_model   ON usage_events(api_key, model, occurred_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_events_node        ON usage_events(node_id, occurred_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_events_source      ON usage_events(source, occurred_at DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_usage_events_dedup ON usage_events(dedup_hash)`,
	} {
		if _, err := s.db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("postgres store: create usage_events index: %w", err)
		}
	}

	// usage_minute_rollup: per-(minute, node, api, model). Drives the totals,
	// trend, sparkline, service-health-grid, and breakdown queries powering
	// the management UI. PK avoids cross-node UPSERT contention because each
	// node only writes its own node_id rows; cluster aggregation is a SUM at
	// read time.
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS usage_minute_rollup (
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
		)
	`); err != nil {
		return fmt.Errorf("postgres store: create usage_minute_rollup: %w", err)
	}
	for _, ddl := range []string{
		`CREATE INDEX IF NOT EXISTS idx_usage_rollup_bucket ON usage_minute_rollup(bucket_start DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_rollup_api    ON usage_minute_rollup(api_key, bucket_start DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_rollup_model  ON usage_minute_rollup(model, bucket_start DESC)`,
	} {
		if _, err := s.db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("postgres store: create usage_rollup index: %w", err)
		}
	}

	// codex_automation_state: per-(kind, node) heartbeat for the leader-
	// gated-or-sharded codex weekly/hourly automation loops. The
	// management /codex-*-automation/status endpoint reads MAX(last_run_at)
	// across the cluster so any node's UI shows the cluster-wide latest
	// check time — not just whatever node happened to receive the request.
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS codex_automation_state (
			kind         TEXT NOT NULL,
			node_id      TEXT NOT NULL,
			last_run_at  TIMESTAMPTZ NOT NULL,
			updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (kind, node_id)
		)
	`); err != nil {
		return fmt.Errorf("postgres store: create codex_automation_state: %w", err)
	}
	cooldownTable := s.fullTableName(s.cfg.CooldownTable)
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			auth_id TEXT NOT NULL,
			model TEXT NOT NULL DEFAULT '',
			content JSONB NOT NULL,
			deleted BOOLEAN NOT NULL DEFAULT FALSE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (auth_id, model)
		)
	`, cooldownTable)); err != nil {
		return fmt.Errorf("postgres store: create cooldown table: %w", err)
	}
	return nil
}

// Bootstrap synchronizes configuration and auth records between PostgreSQL and the local workspace.
func (s *PostgresStore) Bootstrap(ctx context.Context, exampleConfigPath string) error {
	if err := s.EnsureSchema(ctx); err != nil {
		return err
	}
	if err := s.syncConfigFromDatabase(ctx, exampleConfigPath); err != nil {
		return err
	}
	if err := s.syncAuthFromDatabase(ctx); err != nil {
		return err
	}
	return nil
}

// ConfigPath returns the managed configuration file path inside the spool directory.
func (s *PostgresStore) ConfigPath() string {
	if s == nil {
		return ""
	}
	return s.configPath
}

// AuthDir returns the local directory containing mirrored auth files.
func (s *PostgresStore) AuthDir() string {
	if s == nil {
		return ""
	}
	return s.authDir
}

// WorkDir exposes the root spool directory used for mirroring.
func (s *PostgresStore) WorkDir() string {
	if s == nil {
		return ""
	}
	return s.spoolRoot
}

// SetBaseDir implements the optional interface used by authenticators; it is a no-op because
// the Postgres-backed store controls its own workspace.
func (s *PostgresStore) SetBaseDir(string) {}

// Save persists authentication metadata to disk and PostgreSQL.
func (s *PostgresStore) Save(ctx context.Context, auth *cliproxyauth.Auth) (string, error) {
	if auth == nil {
		return "", fmt.Errorf("postgres store: auth is nil")
	}
	path, _, err := s.SaveVersioned(ctx, auth, auth.StoreGeneration())
	return path, err
}

// SaveVersioned persists an auth with generation compare-and-swap semantics.
// Generation zero inserts only; positive generations update only the matching
// active row. Tombstones can only be reactivated through Restore.
func (s *PostgresStore) SaveVersioned(ctx context.Context, auth *cliproxyauth.Auth, expectedGeneration uint64) (string, uint64, error) {
	if auth == nil {
		return "", 0, fmt.Errorf("postgres store: auth is nil")
	}
	if errWeight := cliproxyauth.ValidateAuthWeight(auth); errWeight != nil {
		return "", 0, fmt.Errorf("postgres store: %w", errWeight)
	}

	path, relID, errIdentity := s.resolveCanonicalAuthIdentity(auth)
	if errIdentity != nil {
		return "", 0, errIdentity
	}
	unlockPath := authfilelock.Lock(path)
	defer unlockPath()

	s.mu.Lock()
	defer s.mu.Unlock()

	tempPath, data, errPrepare := s.prepareAuthTemp(auth, path, relID)
	if errPrepare != nil {
		return "", 0, errPrepare
	}
	defer func() { _ = os.Remove(tempPath) }()

	newGeneration, _, errPersist := s.persistAuthExpected(ctx, relID, data, expectedGeneration)
	if errPersist != nil {
		if errors.Is(errPersist, cliproxyauth.ErrAuthStoreCommitUnknown) {
			auth.SetStoreGeneration(newGeneration)
			return path, newGeneration, errPersist
		}
		return "", 0, errPersist
	}

	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	auth.Attributes[cliproxyauth.AttributePath] = path
	auth.Attributes[cliproxyauth.AttributeSourceBackend] = cliproxyauth.AuthSourcePostgres

	if strings.TrimSpace(auth.FileName) == "" {
		auth.FileName = auth.ID
	}
	auth.SetStoreGeneration(newGeneration)

	if errMirror := installAuthMirror(tempPath, path, data, newGeneration); errMirror != nil {
		log.WithError(errMirror).Errorf("postgres store: install committed auth mirror %s failed", auth.ID)
		return path, newGeneration, nil
	}
	s.rememberAuthGeneration(relID, newGeneration)
	tempPath = ""
	return path, newGeneration, nil
}

// Restore explicitly creates an auth or reactivates a tombstoned auth. It is
// separate from SaveVersioned so a stale runtime snapshot cannot recreate a
// credential after a durable delete.
func (s *PostgresStore) Restore(ctx context.Context, auth *cliproxyauth.Auth, expectedGeneration uint64) (string, uint64, error) {
	if auth == nil {
		return "", 0, fmt.Errorf("postgres store: auth is nil")
	}
	if errWeight := cliproxyauth.ValidateAuthWeight(auth); errWeight != nil {
		return "", 0, fmt.Errorf("postgres store: %w", errWeight)
	}
	path, relID, errIdentity := s.resolveCanonicalAuthIdentity(auth)
	if errIdentity != nil {
		return "", 0, errIdentity
	}
	unlockPath := authfilelock.Lock(path)
	defer unlockPath()

	s.mu.Lock()
	defer s.mu.Unlock()

	tempPath, data, errPrepare := s.prepareAuthTemp(auth, path, relID)
	if errPrepare != nil {
		return "", 0, errPrepare
	}
	defer func() { _ = os.Remove(tempPath) }()

	newGeneration, errRestore := s.persistAuthRestore(ctx, relID, data, expectedGeneration)
	if errRestore != nil {
		if errors.Is(errRestore, cliproxyauth.ErrAuthStoreCommitUnknown) {
			auth.SetStoreGeneration(newGeneration)
			return path, newGeneration, errRestore
		}
		return "", 0, errRestore
	}
	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	auth.Attributes[cliproxyauth.AttributePath] = path
	auth.Attributes[cliproxyauth.AttributeSourceBackend] = cliproxyauth.AuthSourcePostgres
	if strings.TrimSpace(auth.FileName) == "" {
		auth.FileName = auth.ID
	}
	auth.SetStoreGeneration(newGeneration)

	if errMirror := installAuthMirror(tempPath, path, data, newGeneration); errMirror != nil {
		log.WithError(errMirror).Errorf("postgres store: install restored auth mirror %s failed", auth.ID)
		return path, newGeneration, nil
	}
	s.rememberAuthGeneration(relID, newGeneration)
	tempPath = ""
	return path, newGeneration, nil
}

type postgresAuthMetadataSetter interface {
	SetMetadata(map[string]any)
}

func (s *PostgresStore) prepareAuthTemp(auth *cliproxyauth.Auth, path, relID string) (tempPath string, data []byte, err error) {
	if errMkdir := os.MkdirAll(filepath.Dir(path), 0o700); errMkdir != nil {
		return "", nil, fmt.Errorf("postgres store: create auth directory: %w", errMkdir)
	}
	temp, errTemp := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".batch-*")
	if errTemp != nil {
		return "", nil, fmt.Errorf("postgres store: create temp auth file: %w", errTemp)
	}
	tempPath = temp.Name()
	if errClose := temp.Close(); errClose != nil {
		_ = os.Remove(tempPath)
		return "", nil, fmt.Errorf("postgres store: close temp auth file: %w", errClose)
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tempPath)
		}
	}()

	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	auth.Metadata["disabled"] = auth.Disabled
	if auth.Storage != nil {
		// Some token stores require a nonexistent destination and create the
		// file themselves rather than truncating an existing temporary file.
		if errRemove := os.Remove(tempPath); errRemove != nil && !errors.Is(errRemove, fs.ErrNotExist) {
			return "", nil, fmt.Errorf("postgres store: prepare token temp path %s: %w", relID, errRemove)
		}
		if setter, ok := auth.Storage.(postgresAuthMetadataSetter); ok {
			setter.SetMetadata(auth.Metadata)
		}
		if errSave := auth.Storage.SaveTokenToFile(tempPath); errSave != nil {
			return "", nil, fmt.Errorf("postgres store: serialize auth %s: %w", relID, errSave)
		}
	} else {
		raw, errMarshal := json.Marshal(auth.Metadata)
		if errMarshal != nil {
			return "", nil, fmt.Errorf("postgres store: marshal auth %s: %w", relID, errMarshal)
		}
		if errWrite := os.WriteFile(tempPath, raw, 0o600); errWrite != nil {
			return "", nil, fmt.Errorf("postgres store: write temp auth %s: %w", relID, errWrite)
		}
	}
	if errChmod := os.Chmod(tempPath, 0o600); errChmod != nil {
		return "", nil, fmt.Errorf("postgres store: secure temp auth %s: %w", relID, errChmod)
	}
	data, err = os.ReadFile(tempPath)
	if err != nil {
		return "", nil, fmt.Errorf("postgres store: read temp auth %s: %w", relID, err)
	}
	return tempPath, data, nil
}

func databaseAuthContent(data []byte) ([]byte, error) {
	metadata := make(map[string]any)
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("postgres store: decode auth content: %w", err)
	}
	delete(metadata, postgresAuthGenerationPayloadKey)
	clean, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("postgres store: encode auth content: %w", err)
	}
	return clean, nil
}

func installAuthMirror(tempPath, path string, data []byte, generation uint64) error {
	mirror, errMirror := authMirrorContent(data, generation)
	if errMirror != nil {
		return errMirror
	}
	if errWrite := os.WriteFile(tempPath, mirror, 0o600); errWrite != nil {
		return fmt.Errorf("write mirror content: %w", errWrite)
	}
	if errRename := os.Rename(tempPath, path); errRename != nil {
		return fmt.Errorf("rename mirror: %w", errRename)
	}
	return nil
}

func authMirrorContent(data []byte, generation uint64) ([]byte, error) {
	metadata := make(map[string]any)
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("decode mirror content: %w", err)
	}
	metadata[postgresAuthGenerationPayloadKey] = generation
	mirror, errMarshal := json.Marshal(metadata)
	if errMarshal != nil {
		return nil, fmt.Errorf("encode mirror content: %w", errMarshal)
	}
	return mirror, nil
}

// scrubAndRemoveAuthMirror replaces a committed tombstone's local mirror with
// a non-sensitive marker before unlinking it. If unlink fails, the remaining
// file contains only the trusted store generation and no credential material.
func scrubAndRemoveAuthMirror(path string, generation uint64, remove func(string) error) error {
	if remove == nil {
		remove = os.Remove
	}
	temp, errTemp := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tombstone-*")
	if errTemp != nil {
		errRemove := remove(path)
		if errRemove == nil || errors.Is(errRemove, fs.ErrNotExist) {
			return nil
		}
		return errors.Join(fmt.Errorf("create tombstone mirror: %w", errTemp), errRemove)
	}
	tempPath := temp.Name()
	if errClose := temp.Close(); errClose != nil {
		_ = os.Remove(tempPath)
		errRemove := remove(path)
		if errRemove == nil || errors.Is(errRemove, fs.ErrNotExist) {
			return nil
		}
		return errors.Join(fmt.Errorf("close tombstone mirror: %w", errClose), errRemove)
	}
	defer func() { _ = os.Remove(tempPath) }()

	errScrub := installAuthMirror(tempPath, path, []byte(`{}`), generation)
	errRemove := remove(path)
	if errRemove == nil || errors.Is(errRemove, fs.ErrNotExist) {
		return nil
	}
	if errScrub != nil {
		return errors.Join(fmt.Errorf("scrub auth mirror: %w", errScrub), errRemove)
	}
	return errRemove
}

// SaveBatch persists authentication metadata in one authoritative database
// transaction. Local mirror files are prepared before the transaction and are
// only installed after the caller finalizes a successful commit.
func (s *PostgresStore) SaveBatch(ctx context.Context, auths []*cliproxyauth.Auth, finalize func(commit func() error) error) error {
	if s == nil || s.db == nil {
		return errors.New("postgres store: not initialized")
	}
	if finalize == nil {
		return errors.New("postgres store: batch finalizer is nil")
	}

	type batchAuth struct {
		auth               *cliproxyauth.Auth
		path               string
		relID              string
		tempPath           string
		data               []byte
		expectedGeneration uint64
		newGeneration      uint64
		changed            bool
	}

	prepared := make([]batchAuth, 0, len(auths))
	paths := make([]string, 0, len(auths))
	for _, auth := range auths {
		if auth == nil {
			return errors.New("postgres store: batch auth is nil")
		}
		if errWeight := cliproxyauth.ValidateAuthWeight(auth); errWeight != nil {
			return fmt.Errorf("postgres store: %w", errWeight)
		}
		path, relID, errIdentity := s.resolveCanonicalAuthIdentity(auth)
		if errIdentity != nil {
			return errIdentity
		}
		prepared = append(prepared, batchAuth{auth: auth, path: path, relID: relID, expectedGeneration: auth.StoreGeneration()})
		paths = append(paths, path)
	}
	sort.Slice(prepared, func(i, j int) bool {
		if prepared[i].path == prepared[j].path {
			return prepared[i].relID < prepared[j].relID
		}
		return prepared[i].path < prepared[j].path
	})

	unlockPaths := authfilelock.Lock(paths...)
	defer unlockPaths()
	s.mu.Lock()
	defer s.mu.Unlock()

	removeTemps := func() {
		for i := range prepared {
			if prepared[i].tempPath != "" {
				_ = os.Remove(prepared[i].tempPath)
			}
		}
	}
	defer removeTemps()

	for i := range prepared {
		item := &prepared[i]
		tempPath, data, errPrepare := s.prepareAuthTemp(item.auth, item.path, item.relID)
		if errPrepare != nil {
			return errPrepare
		}
		item.tempPath = tempPath
		item.data = data

		if item.auth.Attributes == nil {
			item.auth.Attributes = make(map[string]string)
		}
		item.auth.Attributes[cliproxyauth.AttributePath] = item.path
		item.auth.Attributes[cliproxyauth.AttributeSourceBackend] = cliproxyauth.AuthSourcePostgres
		if strings.TrimSpace(item.auth.FileName) == "" {
			item.auth.FileName = item.auth.ID
		}
	}

	transaction, errBegin := s.beginDetachedAuthTransaction(ctx)
	if errBegin != nil {
		return fmt.Errorf("postgres store: begin auth batch: %w", errBegin)
	}
	defer transaction.rollback()
	txCtx := transaction.ctx
	tx := transaction.tx
	lifecycleVersion, errLifecycle := s.nextAuthLifecycleVersion(txCtx, tx)
	if errLifecycle != nil {
		return fmt.Errorf("postgres store: allocate auth batch lifecycle version: %w", errLifecycle)
	}

	for i := range prepared {
		item := &prepared[i]
		newGeneration, changed, errUpsert := s.upsertAuthRow(txCtx, tx, item.relID, item.data, item.expectedGeneration, lifecycleVersion)
		if errUpsert != nil {
			return fmt.Errorf("postgres store: upsert batch auth %s: %w", item.relID, errUpsert)
		}
		item.newGeneration = newGeneration
		item.changed = changed
	}
	batchID := uuid.NewString()
	if errMarker := s.insertAuthCommitMarker(txCtx, tx, batchID); errMarker != nil {
		return fmt.Errorf("postgres store: insert auth batch commit marker: %w", errMarker)
	}
	for i := range prepared {
		item := &prepared[i]
		if !item.changed {
			continue
		}
		if _, errNotify := tx.ExecContext(txCtx, "SELECT pg_notify('cliproxy_auth_changed', $1)", item.relID); errNotify != nil {
			return fmt.Errorf("postgres store: notify auth batch %s: %w", item.relID, errNotify)
		}
	}
	for i := range prepared {
		// The Manager publishes these exact snapshots only after commit returns.
		// Set the returned generation before finalize so the published state is
		// immediately ready for the next CAS without relying on self-NOTIFY.
		prepared[i].auth.SetStoreGeneration(prepared[i].newGeneration)
	}
	candidateGenerations := make(map[string]uint64, len(prepared))
	for i := range prepared {
		candidateGenerations[prepared[i].relID] = prepared[i].newGeneration
	}

	var (
		commitOnce sync.Once
		commitErr  error
		committed  bool
	)
	commit := func() error {
		commitOnce.Do(func() {
			commitErr = transaction.commit()
			if commitErr == nil {
				committed = true
				s.cleanupAuthCommitMarker(txCtx, batchID, "auth batch")
				return
			}
			commitAckErr := commitErr
			verifyCtx, cancelVerify := authCommitConfirmationContext(txCtx)
			markerCommitted, errVerify := s.verifyAuthCommitMarker(verifyCtx, batchID)
			cancelVerify()
			if errVerify != nil {
				commitErr = cliproxyauth.NewAuthStoreCommitUnknown(
					candidateGenerations,
					errors.Join(
						fmt.Errorf("postgres store: commit auth batch: %w", commitAckErr),
						fmt.Errorf("postgres store: verify auth batch commit marker: %w", errVerify),
					),
				)
				return
			}
			if markerCommitted {
				log.WithError(commitAckErr).Warn("postgres store: auth batch commit acknowledgement failed but durable marker confirms commit")
				s.cleanupAuthCommitMarker(txCtx, batchID, "auth batch")
				commitErr = nil
				committed = true
				return
			}
			commitErr = fmt.Errorf("postgres store: commit auth batch: %w", commitAckErr)
		})
		return commitErr
	}
	errFinalize := finalize(commit)
	if committed {
		if errFinalize != nil {
			log.WithError(errFinalize).Error("postgres store: batch finalizer returned an error after commit; keeping committed state")
		}
	} else {
		if !errors.Is(commitErr, cliproxyauth.ErrAuthStoreCommitUnknown) && !errors.Is(errFinalize, cliproxyauth.ErrAuthStoreCommitUnknown) {
			for i := range prepared {
				prepared[i].auth.SetStoreGeneration(prepared[i].expectedGeneration)
			}
		}
		if errFinalize != nil {
			return fmt.Errorf("postgres store: finalize auth batch: %w", errFinalize)
		}
		if commitErr != nil {
			return fmt.Errorf("postgres store: commit auth batch: %w", commitErr)
		}
		return errors.New("postgres store: batch finalizer returned without committing")
	}

	for i := range prepared {
		item := &prepared[i]
		if errMirror := installAuthMirror(item.tempPath, item.path, item.data, item.newGeneration); errMirror != nil {
			log.WithError(errMirror).Errorf("postgres store: install committed auth mirror %s failed", item.relID)
			continue
		}
		s.rememberAuthGeneration(item.relID, item.newGeneration)
		item.tempPath = ""
	}
	return nil
}

func (s *PostgresStore) insertAuthCommitMarker(ctx context.Context, execer postgresAuthLifecycleExecer, markerID string) error {
	query := fmt.Sprintf("INSERT INTO %s (id, committed_at) VALUES ($1, NOW())", s.fullTableName(defaultAuthBatchCommitTable))
	if _, errMarker := execer.ExecContext(ctx, query, markerID); errMarker != nil {
		return errMarker
	}
	return nil
}

func (s *PostgresStore) cleanupAuthCommitMarker(ctx context.Context, markerID, operation string) {
	cleanupCtx, cancelCleanup := authCommitConfirmationContext(ctx)
	defer cancelCleanup()
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", s.fullTableName(defaultAuthBatchCommitTable))
	if _, errDelete := s.db.ExecContext(cleanupCtx, query, markerID); errDelete != nil {
		log.WithError(errDelete).Warnf("postgres store: delete confirmed %s commit marker failed", operation)
	}
}

func authCommitConfirmationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	parent := context.Background()
	if ctx != nil {
		parent = context.WithoutCancel(ctx)
	}
	return context.WithTimeout(parent, authCommitConfirmationTimeout)
}

func (s *PostgresStore) commitDedicatedAuthTransaction(ctx context.Context, transaction *postgresDedicatedTransaction, markerID, operation string, candidates map[string]uint64) error {
	if transaction == nil || transaction.tx == nil {
		return fmt.Errorf("postgres store: commit %s: transaction is unavailable", operation)
	}
	commitErr := transaction.commit()
	if commitErr == nil {
		s.cleanupAuthCommitMarker(ctx, markerID, operation)
		return nil
	}
	verifyCtx, cancelVerify := authCommitConfirmationContext(ctx)
	defer cancelVerify()
	committed, verifyErr := s.verifyAuthCommitMarker(verifyCtx, markerID)
	if verifyErr != nil {
		return cliproxyauth.NewAuthStoreCommitUnknown(
			candidates,
			errors.Join(
				fmt.Errorf("postgres store: commit %s: %w", operation, commitErr),
				fmt.Errorf("postgres store: verify %s commit marker: %w", operation, verifyErr),
			),
		)
	}
	if !committed {
		return fmt.Errorf("postgres store: commit %s: %w", operation, commitErr)
	}
	log.WithError(commitErr).Warnf("postgres store: %s commit acknowledgement failed but serialized marker confirms commit", operation)
	s.cleanupAuthCommitMarker(ctx, markerID, operation)
	return nil
}

// verifyAuthCommitMarker linearizes behind the transaction whose Commit
// acknowledgement was lost. Both transactions take the same xact advisory
// lock, so once this transaction acquires it the original has definitively
// committed or rolled back; marker absence is no longer a timing guess.
func (s *PostgresStore) verifyAuthCommitMarker(ctx context.Context, markerID string) (bool, error) {
	if s == nil || s.db == nil {
		return false, errors.New("postgres store: not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	transaction, errBegin := s.beginDedicatedTransaction(ctx)
	if errBegin != nil {
		return false, fmt.Errorf("begin auth commit verification: %w", errBegin)
	}
	defer transaction.rollback()
	txCtx := transaction.ctx
	tx := transaction.tx
	if errLock := s.lockAuthLifecycle(txCtx, tx); errLock != nil {
		return false, errLock
	}
	query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s WHERE id = $1)", s.fullTableName(defaultAuthBatchCommitTable))
	var exists bool
	if errQuery := tx.QueryRowContext(txCtx, query, markerID).Scan(&exists); errQuery != nil {
		return false, fmt.Errorf("query auth commit marker: %w", errQuery)
	}
	if errCommit := transaction.commit(); errCommit != nil {
		// The lifecycle lock and SELECT already made the marker result
		// definitive. A lost acknowledgement while closing this read-only
		// transaction cannot change the original mutation's outcome.
		log.WithError(errCommit).Warn("postgres store: auth commit verification close acknowledgement failed")
	}
	return exists, nil
}

// List enumerates all auth records from the writer. Authentication lifecycle
// state is an admission boundary: a lagging replica must never reintroduce a
// credential that has already been tombstoned on the writer.
func (s *PostgresStore) List(ctx context.Context) ([]*cliproxyauth.Auth, error) {
	return s.listAuths(ctx, s.db)
}

// ListAuthoritative enumerates auth records from the write pool. Cluster
// reconciliation uses this path after reconnects and periodically so read
// replica lag cannot roll an in-memory credential back to an older snapshot.
func (s *PostgresStore) ListAuthoritative(ctx context.Context) ([]*cliproxyauth.Auth, error) {
	return s.listAuths(ctx, s.db)
}

// WithAuthoritativeAuthBatch reads a strict writer snapshot while holding the
// same transaction-scoped lifecycle advisory lock used by every auth mutation.
// finalize therefore observes one serialization point for the entire batch.
func (s *PostgresStore) WithAuthoritativeAuthBatch(ctx context.Context, ids []string, finalize func(map[string]cliproxyauth.AuthAuthoritativeState) error) error {
	if s == nil || s.db == nil {
		return errors.New("postgres store: not initialized")
	}
	if finalize == nil {
		return errors.New("postgres store: authoritative batch finalizer is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	type requestedAuth struct {
		id    string
		relID string
	}
	requested := make([]requestedAuth, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		path, errPath := s.resolveDeletePath(id)
		if errPath != nil {
			return errPath
		}
		relID, errRel := s.relativeAuthID(path)
		if errRel != nil {
			return errRel
		}
		seen[id] = struct{}{}
		requested = append(requested, requestedAuth{id: id, relID: relID})
	}
	sort.Slice(requested, func(i, j int) bool { return requested[i].relID < requested[j].relID })

	tx, errBegin := s.db.BeginTx(ctx, nil)
	if errBegin != nil {
		return fmt.Errorf("postgres store: begin authoritative auth batch: %w", errBegin)
	}
	defer func() { _ = tx.Rollback() }()
	if errLock := s.lockAuthLifecycle(ctx, tx); errLock != nil {
		return errLock
	}

	query := fmt.Sprintf(
		"SELECT id, content, version, deleted, created_at, updated_at FROM %s WHERE id = $1",
		s.fullTableName(s.cfg.AuthTable),
	)
	states := make(map[string]cliproxyauth.AuthAuthoritativeState, len(requested))
	for _, item := range requested {
		var (
			rowID      string
			payload    string
			generation int64
			deleted    bool
			createdAt  time.Time
			updatedAt  time.Time
		)
		errScan := tx.QueryRowContext(ctx, query, item.relID).Scan(&rowID, &payload, &generation, &deleted, &createdAt, &updatedAt)
		if errors.Is(errScan, sql.ErrNoRows) {
			states[item.id] = cliproxyauth.AuthAuthoritativeState{}
			continue
		}
		if errScan != nil {
			return fmt.Errorf("postgres store: read authoritative auth %s: %w", item.relID, errScan)
		}
		if generation <= 0 {
			return fmt.Errorf("postgres store: invalid authoritative auth generation %d for %s", generation, item.relID)
		}
		state := cliproxyauth.AuthAuthoritativeState{
			Exists:     true,
			Deleted:    deleted,
			Generation: uint64(generation),
		}
		if !deleted {
			auth, built := s.buildAuthFromRow(rowID, payload, createdAt, updatedAt, uint64(generation))
			if !built || auth == nil {
				return fmt.Errorf("postgres store: invalid authoritative auth payload for %s", item.relID)
			}
			state.Auth = auth
		}
		states[item.id] = state
	}

	if errFinalize := finalize(states); errFinalize != nil {
		return errFinalize
	}
	if errCommit := tx.Commit(); errCommit != nil {
		// The locked SELECT and in-memory finalizer already linearized the
		// publication. A read-only commit acknowledgement cannot alter it.
		log.WithError(errCommit).Warn("postgres store: close authoritative auth batch transaction failed after publication")
	}
	return nil
}

func (s *PostgresStore) listAuths(ctx context.Context, pool *sql.DB) ([]*cliproxyauth.Auth, error) {
	if pool == nil {
		return nil, errors.New("postgres store: auth read pool is not initialized")
	}
	query := fmt.Sprintf(
		"SELECT id, content, version, created_at, updated_at FROM %s WHERE deleted = FALSE ORDER BY id",
		s.fullTableName(s.cfg.AuthTable),
	)
	rows, err := pool.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("postgres store: list auth: %w", err)
	}
	defer rows.Close()

	auths := make([]*cliproxyauth.Auth, 0, 32)
	for rows.Next() {
		var (
			id         string
			payload    string
			generation uint64
			createdAt  time.Time
			updatedAt  time.Time
		)
		if err = rows.Scan(&id, &payload, &generation, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("postgres store: scan auth row: %w", err)
		}
		auth, built := s.buildAuthFromRow(id, payload, createdAt, updatedAt, generation)
		if !built {
			continue
		}
		auths = append(auths, auth)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres store: iterate auth rows: %w", err)
	}
	return auths, nil
}

// GetByID returns a single auth row by ID for cluster-mode precise reload on
// NOTIFY. Returns (nil, nil) when the row does not exist — callers interpret
// that as "deleted, drop from in-memory cache". This is the cheap path
// consumed by Manager.ReloadByID to avoid a full List() on every NOTIFY.
func (s *PostgresStore) GetByID(ctx context.Context, id string) (*cliproxyauth.Auth, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, nil
	}
	query := fmt.Sprintf(
		"SELECT id, content, version, created_at, updated_at FROM %s WHERE id = $1 AND deleted = FALSE",
		s.fullTableName(s.cfg.AuthTable),
	)
	var (
		rowID      string
		payload    string
		generation uint64
		createdAt  time.Time
		updatedAt  time.Time
	)
	// Always read from the write pool here. NOTIFY-driven ReloadByID is
	// triggered by a write that just landed on the leader; routing this lookup
	// to a read replica risks observing pre-write state during replication
	// lag and (per the (nil, nil) contract above) silently dropping a
	// just-upserted row from the in-memory cache.
	err := s.db.QueryRowContext(ctx, query, id).Scan(&rowID, &payload, &generation, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("postgres store: get auth %s: %w", id, err)
	}
	auth, built := s.buildAuthFromRow(rowID, payload, createdAt, updatedAt, generation)
	if !built {
		// Row exists but payload invalid / outside spool — surface as
		// "not found" so the Manager drops the cached copy; a subsequent
		// Save will correct it.
		return nil, nil
	}
	return auth, nil
}

// AuthLifecycleFence returns a commit-ordered version from the writer. The
// same transaction advisory lock and clock row are used by every auth
// mutation, so a later committed tombstone or credential update always has a
// greater lifecycle version than an already-started explicit operation.
func (s *PostgresStore) AuthLifecycleFence(ctx context.Context) (uint64, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("postgres store: not initialized")
	}
	tx, errBegin := s.db.BeginTx(ctx, nil)
	if errBegin != nil {
		return 0, fmt.Errorf("postgres store: begin auth lifecycle fence: %w", errBegin)
	}
	defer func() { _ = tx.Rollback() }()
	fence, errFence := s.nextAuthLifecycleVersion(ctx, tx)
	if errFence != nil {
		return 0, fmt.Errorf("postgres store: read auth lifecycle fence: %w", errFence)
	}
	if errCommit := tx.Commit(); errCommit != nil {
		return 0, fmt.Errorf("postgres store: commit auth lifecycle fence: %w", errCommit)
	}
	return fence, nil
}

// GetAuthLifecycle reads active rows and tombstones from the writer.
func (s *PostgresStore) GetAuthLifecycle(ctx context.Context, id string) (cliproxyauth.AuthLifecycleState, error) {
	if s == nil || s.db == nil {
		return cliproxyauth.AuthLifecycleState{}, errors.New("postgres store: not initialized")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return cliproxyauth.AuthLifecycleState{}, nil
	}
	path, errPath := s.resolveDeletePath(id)
	if errPath != nil {
		return cliproxyauth.AuthLifecycleState{}, errPath
	}
	relID, errRel := s.relativeAuthID(path)
	if errRel != nil {
		return cliproxyauth.AuthLifecycleState{}, errRel
	}
	query := fmt.Sprintf("SELECT version, lifecycle_version, deleted, updated_at, deleted_at FROM %s WHERE id = $1", s.fullTableName(s.cfg.AuthTable))
	var (
		generation       int64
		lifecycleVersion int64
		deleted          bool
		updatedAt        time.Time
		deletedAt        sql.NullTime
	)
	if err := s.db.QueryRowContext(ctx, query, relID).Scan(&generation, &lifecycleVersion, &deleted, &updatedAt, &deletedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return cliproxyauth.AuthLifecycleState{}, nil
		}
		return cliproxyauth.AuthLifecycleState{}, fmt.Errorf("postgres store: read auth lifecycle %s: %w", relID, err)
	}
	if generation < 0 {
		return cliproxyauth.AuthLifecycleState{}, fmt.Errorf("postgres store: negative auth generation for %s", relID)
	}
	if lifecycleVersion < 0 {
		return cliproxyauth.AuthLifecycleState{}, fmt.Errorf("postgres store: negative auth lifecycle version for %s", relID)
	}
	state := cliproxyauth.AuthLifecycleState{
		Exists:           true,
		Deleted:          deleted,
		Generation:       uint64(generation),
		LifecycleVersion: uint64(lifecycleVersion),
		UpdatedAt:        updatedAt,
	}
	if deletedAt.Valid {
		state.DeletedAt = deletedAt.Time
	}
	return state, nil
}

// buildAuthFromRow decodes a single auth row into a *cliproxyauth.Auth.
// Shared by List and GetByID so the shape of the in-memory auth stays
// consistent across the two code paths. Returns (_, false) when the row
// should be skipped (invalid path or bad JSON) — callers treat that as
// "not present".
func (s *PostgresStore) buildAuthFromRow(id, payload string, createdAt, updatedAt time.Time, generations ...uint64) (*cliproxyauth.Auth, bool) {
	path, errPath := s.absoluteAuthPath(id)
	if errPath != nil {
		log.WithError(errPath).Warnf("postgres store: skipping auth %s outside spool", id)
		return nil, false
	}
	metadata := make(map[string]any)
	if err := json.Unmarshal([]byte(payload), &metadata); err != nil {
		log.WithError(err).Warnf("postgres store: skipping auth %s with invalid json", id)
		return nil, false
	}
	delete(metadata, postgresAuthGenerationPayloadKey)
	if errWeight := cliproxyauth.ValidateAuthWeight(&cliproxyauth.Auth{Metadata: metadata}); errWeight != nil {
		log.WithError(errWeight).Warnf("postgres store: skipping auth %s with invalid weight", id)
		return nil, false
	}
	provider := strings.TrimSpace(valueAsString(metadata["type"]))
	if provider == "" {
		provider = "unknown"
	}
	attr := map[string]string{"path": path, cliproxyauth.AttributeSourceBackend: cliproxyauth.AuthSourcePostgres}
	if email := strings.TrimSpace(valueAsString(metadata["email"])); email != "" {
		attr["email"] = email
	}
	// Restore the Disabled bool from the persisted JSON. Save writes
	// metadata["disabled"] = auth.Disabled before persisting; reversing it
	// here keeps the in-memory Auth aligned with PG after a restart, so
	// dispatch continues to honor a disabled state.
	disabled, _ := metadata["disabled"].(bool)
	status := cliproxyauth.StatusActive
	if disabled {
		status = cliproxyauth.StatusDisabled
	}
	auth := &cliproxyauth.Auth{
		ID:               normalizeAuthID(id),
		Provider:         provider,
		FileName:         normalizeAuthID(id),
		Label:            labelFor(metadata),
		Status:           status,
		Disabled:         disabled,
		Attributes:       attr,
		Metadata:         metadata,
		CreatedAt:        createdAt,
		UpdatedAt:        updatedAt,
		LastRefreshedAt:  time.Time{},
		NextRefreshAfter: time.Time{},
	}
	if len(generations) > 0 {
		auth.SetStoreGeneration(generations[0])
	}
	cliproxyauth.ApplyCustomHeadersFromMetadata(auth)
	return auth, true
}

func (s *PostgresStore) Delete(ctx context.Context, id string) error {
	if s == nil || s.db == nil {
		return errors.New("postgres store: not initialized")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("postgres store: id is empty")
	}
	path, errPath := s.resolveDeletePath(id)
	if errPath != nil {
		return errPath
	}
	relID, errRel := s.relativeAuthID(path)
	if errRel != nil {
		return errRel
	}

	// Delete is the legacy explicit-management path and has no generation
	// argument. Prefer the generation captured when this node installed the
	// mirror. Only when no trusted mirror generation exists may this explicit
	// path read the writer to select a CAS generation. Watcher deletes never use
	// this fallback; syncAuthFile requires a generation captured before delete.
	expectedGeneration, known := s.knownAuthGeneration(relID)
	if !known {
		currentGeneration, deleted, errState := s.currentAuthRowState(ctx, s.db, relID)
		switch {
		case errors.Is(errState, sql.ErrNoRows):
			expectedGeneration = 0
		case errState != nil:
			return fmt.Errorf("postgres store: inspect auth before delete %s: %w", relID, errState)
		case deleted:
			expectedGeneration = 0
		default:
			expectedGeneration = currentGeneration
		}
	}
	_, errTombstone := s.Tombstone(ctx, path, expectedGeneration)
	return errTombstone
}

// Tombstone durably removes an auth while preserving its generation. A zero
// expected generation can create a missing tombstone or replay an existing
// tombstone, but it never deletes an active row. Positive generations delete
// only the matching active row.
func (s *PostgresStore) Tombstone(ctx context.Context, id string, expectedGeneration uint64) (uint64, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("postgres store: not initialized")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return 0, fmt.Errorf("postgres store: id is empty")
	}
	path, err := s.resolveDeletePath(id)
	if err != nil {
		return 0, err
	}
	unlockPath := authfilelock.Lock(path)
	defer unlockPath()

	s.mu.Lock()
	defer s.mu.Unlock()

	relID, err := s.relativeAuthID(path)
	if err != nil {
		return 0, err
	}
	newGeneration, _, errTombstone := s.persistAuthTombstone(ctx, relID, expectedGeneration, true)
	if errTombstone != nil {
		if errors.Is(errTombstone, cliproxyauth.ErrAuthStoreCommitUnknown) {
			return newGeneration, errTombstone
		}
		return 0, errTombstone
	}
	s.rememberAuthGeneration(relID, newGeneration)
	if errRemove := scrubAndRemoveAuthMirror(path, newGeneration, os.Remove); errRemove != nil {
		// The tombstone is already committed and authoritative; a local mirror
		// cleanup failure must not make callers keep dispatching the credential.
		log.WithError(errRemove).Errorf("postgres store: remove tombstoned auth mirror %s failed", relID)
	}
	return newGeneration, nil
}

type postgresAuthMirrorState struct {
	exists     bool
	deleted    bool
	generation uint64
	content    []byte
}

func (s *PostgresStore) authoritativeAuthMirrorState(ctx context.Context, relID string) (postgresAuthMirrorState, error) {
	query := fmt.Sprintf("SELECT content, version, deleted FROM %s WHERE id = $1", s.fullTableName(s.cfg.AuthTable))
	var (
		content    []byte
		generation int64
		deleted    bool
	)
	if err := s.db.QueryRowContext(ctx, query, relID).Scan(&content, &generation, &deleted); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return postgresAuthMirrorState{}, nil
		}
		return postgresAuthMirrorState{}, err
	}
	if generation <= 0 {
		return postgresAuthMirrorState{}, fmt.Errorf("postgres store: invalid auth generation %d for %s", generation, relID)
	}
	return postgresAuthMirrorState{exists: true, deleted: deleted, generation: uint64(generation), content: content}, nil
}

// ReconcileAuthMirror converges one local mirror with the writer while holding
// the same local locks used by Save/Restore, so an in-process mutation cannot
// race the authoritative read or file update.
func (s *PostgresStore) ReconcileAuthMirror(ctx context.Context, id string) error {
	if s == nil || s.db == nil {
		return errors.New("postgres store: not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	path, errPath := s.resolveDeletePath(id)
	if errPath != nil {
		return errPath
	}
	relID, errRel := s.relativeAuthID(path)
	if errRel != nil {
		return errRel
	}
	unlockPath := authfilelock.Lock(path)
	defer unlockPath()
	s.mu.Lock()
	defer s.mu.Unlock()

	state, errState := s.authoritativeAuthMirrorState(ctx, relID)
	if errState != nil {
		return fmt.Errorf("postgres store: recheck auth mirror %s: %w", relID, errState)
	}
	if state.exists && !state.deleted {
		desired, errDesired := authMirrorContent(state.content, state.generation)
		if errDesired != nil {
			return fmt.Errorf("postgres store: build authoritative auth mirror %s: %w", relID, errDesired)
		}
		if current, errRead := os.ReadFile(path); errRead == nil && bytes.Equal(current, desired) {
			s.rememberAuthGeneration(relID, state.generation)
			return nil
		} else if errRead != nil && !errors.Is(errRead, fs.ErrNotExist) {
			return fmt.Errorf("postgres store: read local auth mirror %s: %w", relID, errRead)
		}
		if errMkdir := os.MkdirAll(filepath.Dir(path), 0o700); errMkdir != nil {
			return fmt.Errorf("postgres store: prepare auth mirror directory %s: %w", relID, errMkdir)
		}
		temp, errTemp := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".reconcile-*")
		if errTemp != nil {
			return fmt.Errorf("postgres store: create auth mirror temp %s: %w", relID, errTemp)
		}
		tempPath := temp.Name()
		if errClose := temp.Close(); errClose != nil {
			_ = os.Remove(tempPath)
			return fmt.Errorf("postgres store: close auth mirror temp %s: %w", relID, errClose)
		}
		defer func() { _ = os.Remove(tempPath) }()
		if errInstall := installAuthMirror(tempPath, path, state.content, state.generation); errInstall != nil {
			return fmt.Errorf("postgres store: install authoritative auth mirror %s: %w", relID, errInstall)
		}
		s.rememberAuthGeneration(relID, state.generation)
		return nil
	}
	generation := state.generation
	if generation == 0 {
		generation, _ = s.knownAuthGeneration(relID)
	}
	if state.exists {
		s.rememberAuthGeneration(relID, generation)
	}
	if _, errStat := os.Stat(path); errors.Is(errStat, fs.ErrNotExist) {
		return nil
	} else if errStat != nil {
		return fmt.Errorf("postgres store: inspect non-active auth mirror %s: %w", relID, errStat)
	}
	if errRemove := scrubAndRemoveAuthMirror(path, generation, os.Remove); errRemove != nil {
		return fmt.Errorf("postgres store: scrub non-active auth mirror %s: %w", relID, errRemove)
	}
	return nil
}

// ScrubDeletedAuthMirror is retained for compatibility with older cluster
// subscribers. New callers should use ReconcileAuthMirror so remote active
// updates also refresh local credential mirrors.
func (s *PostgresStore) ScrubDeletedAuthMirror(ctx context.Context, id string) error {
	return s.ReconcileAuthMirror(ctx, id)
}

// ReconcileAuthMirrors discovers both writer rows and local-only spool files,
// then converges each ID independently using the single-ID lock ordering.
func (s *PostgresStore) ReconcileAuthMirrors(ctx context.Context) error {
	if s == nil || s.db == nil {
		return errors.New("postgres store: not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ids := make(map[string]struct{})
	query := fmt.Sprintf("SELECT id FROM %s ORDER BY id", s.fullTableName(s.cfg.AuthTable))
	rows, errRows := s.db.QueryContext(ctx, query)
	if errRows != nil {
		return fmt.Errorf("postgres store: list authoritative auth mirror IDs: %w", errRows)
	}
	for rows.Next() {
		var id string
		if errScan := rows.Scan(&id); errScan != nil {
			_ = rows.Close()
			return fmt.Errorf("postgres store: scan authoritative auth mirror ID: %w", errScan)
		}
		id = strings.TrimSpace(id)
		if id != "" {
			ids[id] = struct{}{}
		}
	}
	if errIter := rows.Err(); errIter != nil {
		_ = rows.Close()
		return fmt.Errorf("postgres store: iterate authoritative auth mirror IDs: %w", errIter)
	}
	if errClose := rows.Close(); errClose != nil {
		return fmt.Errorf("postgres store: close authoritative auth mirror IDs: %w", errClose)
	}

	errWalk := filepath.WalkDir(s.authDir, func(path string, entry fs.DirEntry, errWalk error) error {
		if errWalk != nil {
			return errWalk
		}
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			return nil
		}
		relID, errRel := s.relativeAuthID(path)
		if errRel != nil {
			return errRel
		}
		ids[relID] = struct{}{}
		return nil
	})
	if errWalk != nil && !errors.Is(errWalk, fs.ErrNotExist) {
		return fmt.Errorf("postgres store: list local auth mirrors: %w", errWalk)
	}
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	for _, id := range ordered {
		if errReconcile := s.ReconcileAuthMirror(ctx, id); errReconcile != nil {
			return errReconcile
		}
	}
	return nil
}

// PersistAuthFiles stores the provided auth file changes in PostgreSQL.
func (s *PostgresStore) PersistAuthFiles(ctx context.Context, _ string, paths ...string) error {
	if len(paths) == 0 {
		return nil
	}

	for _, p := range paths {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			continue
		}
		relID, err := s.relativeAuthID(trimmed)
		if err != nil {
			// Attempt to resolve absolute path under authDir.
			abs := trimmed
			if !filepath.IsAbs(abs) {
				abs = filepath.Join(s.authDir, trimmed)
			}
			relID, err = s.relativeAuthID(abs)
			if err != nil {
				log.WithError(err).Warnf("postgres store: ignoring auth path %s", trimmed)
				continue
			}
			trimmed = abs
		}
		unlockPath := authfilelock.Lock(trimmed)
		s.mu.Lock()
		err = s.syncAuthFile(ctx, relID, trimmed)
		s.mu.Unlock()
		unlockPath()
		if err != nil {
			return err
		}
	}
	return nil
}

// PersistConfig mirrors the local configuration file to PostgreSQL.
func (s *PostgresStore) PersistConfig(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.configPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return s.deleteConfigRecord(ctx)
		}
		return fmt.Errorf("postgres store: read config file: %w", err)
	}
	return s.persistConfig(ctx, data)
}

// SyncConfigAuthoritative refreshes the local config mirror from the write
// pool. Cluster subscribers use it after NOTIFY/reconnect so read replica lag
// cannot apply an older configuration on a peer.
func (s *PostgresStore) SyncConfigAuthoritative(ctx context.Context) error {
	if s == nil || s.db == nil {
		return errors.New("postgres store: config write pool is not initialized")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	query := fmt.Sprintf("SELECT content FROM %s WHERE id = $1", s.fullTableName(s.cfg.ConfigTable))
	var content string
	if err := s.db.QueryRowContext(ctx, query, defaultConfigKey).Scan(&content); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("postgres store: authoritative config row does not exist")
		}
		return fmt.Errorf("postgres store: load authoritative config: %w", err)
	}
	normalized := []byte(normalizeLineEndings(content))
	if current, errRead := os.ReadFile(s.configPath); errRead == nil && string(current) == string(normalized) {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.configPath), 0o700); err != nil {
		return fmt.Errorf("postgres store: prepare config directory: %w", err)
	}
	// Keep the existing inode: the config watcher watches the file itself,
	// so replacing it with rename can detach future fsnotify events.
	if err := os.WriteFile(s.configPath, normalized, 0o600); err != nil {
		return fmt.Errorf("postgres store: write authoritative config: %w", err)
	}
	return nil
}

// syncConfigFromDatabase writes the database-stored config to disk or seeds the database from template.
func (s *PostgresStore) syncConfigFromDatabase(ctx context.Context, exampleConfigPath string) error {
	query := fmt.Sprintf("SELECT content FROM %s WHERE id = $1", s.fullTableName(s.cfg.ConfigTable))
	var content string
	err := s.readPool().QueryRowContext(ctx, query, defaultConfigKey).Scan(&content)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, errStat := os.Stat(s.configPath); errors.Is(errStat, fs.ErrNotExist) {
			if exampleConfigPath != "" {
				if errCopy := misc.CopyConfigTemplate(exampleConfigPath, s.configPath); errCopy != nil {
					return fmt.Errorf("postgres store: copy example config: %w", errCopy)
				}
			} else {
				if errCreate := os.MkdirAll(filepath.Dir(s.configPath), 0o700); errCreate != nil {
					return fmt.Errorf("postgres store: prepare config directory: %w", errCreate)
				}
				if errWrite := os.WriteFile(s.configPath, []byte{}, 0o600); errWrite != nil {
					return fmt.Errorf("postgres store: create empty config: %w", errWrite)
				}
			}
		}
		data, errRead := os.ReadFile(s.configPath)
		if errRead != nil {
			return fmt.Errorf("postgres store: read local config: %w", errRead)
		}
		if errPersist := s.persistConfig(ctx, data); errPersist != nil {
			return errPersist
		}
	case err != nil:
		return fmt.Errorf("postgres store: load config from database: %w", err)
	default:
		if err = os.MkdirAll(filepath.Dir(s.configPath), 0o700); err != nil {
			return fmt.Errorf("postgres store: prepare config directory: %w", err)
		}
		normalized := normalizeLineEndings(content)
		if err = os.WriteFile(s.configPath, []byte(normalized), 0o600); err != nil {
			return fmt.Errorf("postgres store: write config to spool: %w", err)
		}
	}
	return nil
}

// syncAuthFromDatabase populates the local auth directory from PostgreSQL data.
//
// Auth bootstrap deliberately reads from the writer. A read replica may lag a
// tombstone and leave both a dispatchable runtime row and an old token mirror
// on a newly started node.
func (s *PostgresStore) syncAuthFromDatabase(ctx context.Context) error {
	query := fmt.Sprintf("SELECT id, content, version FROM %s WHERE deleted = FALSE", s.fullTableName(s.cfg.AuthTable))
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("postgres store: load auth from database: %w", err)
	}
	defer rows.Close()

	if err = os.RemoveAll(s.authDir); err != nil {
		return fmt.Errorf("postgres store: reset auth directory: %w", err)
	}
	if err = os.MkdirAll(s.authDir, 0o700); err != nil {
		return fmt.Errorf("postgres store: recreate auth directory: %w", err)
	}

	for rows.Next() {
		var (
			id         string
			payload    string
			generation uint64
		)
		if err = rows.Scan(&id, &payload, &generation); err != nil {
			return fmt.Errorf("postgres store: scan auth row: %w", err)
		}
		path, errPath := s.absoluteAuthPath(id)
		if errPath != nil {
			log.WithError(errPath).Warnf("postgres store: skipping auth %s outside spool", id)
			continue
		}
		if err = os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return fmt.Errorf("postgres store: create auth subdir: %w", err)
		}
		mirror, errMirror := authMirrorContent([]byte(payload), generation)
		if errMirror != nil {
			return fmt.Errorf("postgres store: build auth mirror %s: %w", id, errMirror)
		}
		if err = os.WriteFile(path, mirror, 0o600); err != nil {
			return fmt.Errorf("postgres store: write auth file: %w", err)
		}
		s.rememberAuthGeneration(id, generation)
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("postgres store: iterate auth rows: %w", err)
	}
	return nil
}

func (s *PostgresStore) syncAuthFile(ctx context.Context, relID, path string) error {
	expectedGeneration, hasExpectedGeneration := s.knownAuthGeneration(relID)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return s.persistWatcherTombstone(ctx, relID, expectedGeneration, hasExpectedGeneration)
		}
		return fmt.Errorf("postgres store: read auth file: %w", err)
	}
	if len(data) == 0 {
		return s.persistWatcherTombstone(ctx, relID, expectedGeneration, hasExpectedGeneration)
	}
	metadata := make(map[string]any)
	if errDecode := json.Unmarshal(data, &metadata); errDecode != nil {
		return fmt.Errorf("postgres store: decode auth file %s: %w", relID, errDecode)
	}
	// The JSON field is mirror metadata, not an authority boundary. External
	// file writers can forge it, so watcher CAS uses only generations learned
	// directly from PostgreSQL or from a successful local commit.
	delete(metadata, postgresAuthGenerationPayloadKey)
	clean, errMarshal := json.Marshal(metadata)
	if errMarshal != nil {
		return fmt.Errorf("postgres store: encode auth file %s: %w", relID, errMarshal)
	}
	newGeneration, changed, errPersist := s.persistAuthExpected(ctx, relID, clean, expectedGeneration)
	if errPersist == nil && changed {
		temp, errTemp := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".generation-*")
		if errTemp != nil {
			return fmt.Errorf("postgres store: create auth generation mirror %s: %w", relID, errTemp)
		}
		tempPath := temp.Name()
		if errClose := temp.Close(); errClose != nil {
			_ = os.Remove(tempPath)
			return fmt.Errorf("postgres store: close auth generation mirror %s: %w", relID, errClose)
		}
		defer func() { _ = os.Remove(tempPath) }()
		if errMirror := installAuthMirror(tempPath, path, clean, newGeneration); errMirror != nil {
			return fmt.Errorf("postgres store: install auth generation mirror %s: %w", relID, errMirror)
		}
		s.rememberAuthGeneration(relID, newGeneration)
	}
	return errPersist
}

func (s *PostgresStore) persistWatcherTombstone(ctx context.Context, relID string, expectedGeneration uint64, known bool) error {
	if !known || expectedGeneration == 0 {
		return fmt.Errorf("%w for %s (watcher has no trusted generation)", cliproxyauth.ErrAuthStoreConflict, relID)
	}
	newGeneration, _, errTombstone := s.persistAuthTombstone(ctx, relID, expectedGeneration, false)
	if errTombstone != nil {
		return errTombstone
	}
	s.rememberAuthGeneration(relID, newGeneration)
	return nil
}

type postgresAuthRowQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type postgresAuthLifecycleExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (s *PostgresStore) lockAuthLifecycle(ctx context.Context, execer postgresAuthLifecycleExecer) error {
	if execer == nil {
		return errors.New("postgres store: auth lifecycle transaction is unavailable")
	}
	if _, errLock := execer.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1, $2)", authLifecycleLockClass, authLifecycleLockKey); errLock != nil {
		return fmt.Errorf("postgres store: acquire auth lifecycle lock: %w", errLock)
	}
	return nil
}

func (s *PostgresStore) nextAuthLifecycleVersion(ctx context.Context, tx *sql.Tx) (uint64, error) {
	if tx == nil {
		return 0, errors.New("postgres store: auth lifecycle transaction is unavailable")
	}
	if errLock := s.lockAuthLifecycle(ctx, tx); errLock != nil {
		return 0, errLock
	}
	return s.advanceAuthLifecycleVersion(ctx, tx)
}

func (s *PostgresStore) advanceAuthLifecycleVersion(ctx context.Context, tx *sql.Tx) (uint64, error) {
	if tx == nil {
		return 0, errors.New("postgres store: auth lifecycle transaction is unavailable")
	}
	query := fmt.Sprintf(
		"UPDATE %s SET value = value + 1 WHERE id = 1 RETURNING value",
		s.fullTableName(defaultAuthLifecycleClockTable),
	)
	var version int64
	if errNext := tx.QueryRowContext(ctx, query).Scan(&version); errNext != nil {
		return 0, fmt.Errorf("postgres store: advance auth lifecycle clock: %w", errNext)
	}
	if version <= 0 {
		return 0, fmt.Errorf("postgres store: invalid auth lifecycle version %d", version)
	}
	return uint64(version), nil
}

// persistAuth is the compatibility path for new file imports. Generation zero
// is insert-only and therefore cannot overwrite an existing row.
func (s *PostgresStore) persistAuth(ctx context.Context, relID string, data []byte) error {
	_, _, err := s.persistAuthExpected(ctx, relID, data, 0)
	return err
}

func (s *PostgresStore) persistAuthExpected(ctx context.Context, relID string, data []byte, expectedGeneration uint64) (uint64, bool, error) {
	if s == nil || s.db == nil {
		return 0, false, errors.New("postgres store: not initialized")
	}
	transaction, errBegin := s.beginDetachedAuthTransaction(ctx)
	if errBegin != nil {
		return 0, false, fmt.Errorf("postgres store: begin auth save: %w", errBegin)
	}
	defer transaction.rollback()
	txCtx := transaction.ctx
	tx := transaction.tx
	lifecycleVersion, errLifecycle := s.nextAuthLifecycleVersion(txCtx, tx)
	if errLifecycle != nil {
		return 0, false, fmt.Errorf("postgres store: allocate auth save lifecycle version: %w", errLifecycle)
	}
	newGeneration, changed, err := s.upsertAuthRow(txCtx, tx, relID, data, expectedGeneration, lifecycleVersion)
	if err != nil {
		return 0, false, err
	}
	markerID := uuid.NewString()
	if errMarker := s.insertAuthCommitMarker(txCtx, tx, markerID); errMarker != nil {
		return 0, false, fmt.Errorf("postgres store: insert auth save commit marker: %w", errMarker)
	}
	if changed {
		if _, errNotify := tx.ExecContext(txCtx, "SELECT pg_notify('cliproxy_auth_changed', $1)", relID); errNotify != nil {
			return 0, false, fmt.Errorf("postgres store: notify auth save: %w", errNotify)
		}
	}
	if errCommit := s.commitDedicatedAuthTransaction(txCtx, transaction, markerID, "auth save", map[string]uint64{relID: newGeneration}); errCommit != nil {
		if errors.Is(errCommit, cliproxyauth.ErrAuthStoreCommitUnknown) {
			return newGeneration, changed, errCommit
		}
		return 0, false, errCommit
	}
	return newGeneration, changed, nil
}

func (s *PostgresStore) upsertAuthRow(ctx context.Context, querier postgresAuthRowQuerier, relID string, data []byte, expectedGeneration, lifecycleVersion uint64) (uint64, bool, error) {
	expected, errExpected := postgresGenerationInt64(relID, expectedGeneration)
	if errExpected != nil {
		return 0, false, errExpected
	}
	clean, errClean := databaseAuthContent(data)
	if errClean != nil {
		return 0, false, errClean
	}
	lifecycle, errLifecycle := postgresGenerationInt64(relID, lifecycleVersion)
	if errLifecycle != nil || lifecycle == 0 {
		return 0, false, fmt.Errorf("postgres store: invalid auth lifecycle version for %s", relID)
	}
	authTable := s.fullTableName(s.cfg.AuthTable)
	if expectedGeneration == 0 {
		query := fmt.Sprintf(`
			INSERT INTO %s (id, content, version, lifecycle_version, deleted, deleted_at, last_writer, created_at, updated_at)
			VALUES ($1, $2, 1, $4, FALSE, NULL, $3, NOW(), NOW())
			ON CONFLICT (id) DO NOTHING
			RETURNING version
		`, authTable)
		var newGeneration int64
		errInsert := querier.QueryRowContext(ctx, query, relID, json.RawMessage(clean), s.nodeWriter(), lifecycle).Scan(&newGeneration)
		switch {
		case errInsert == nil:
			return uint64(newGeneration), true, nil
		case !errors.Is(errInsert, sql.ErrNoRows):
			return 0, false, fmt.Errorf("postgres store: insert auth record: %w", errInsert)
		default:
			return s.authSaveConflict(ctx, querier, relID, expectedGeneration)
		}
	}

	// The exact-version UPDATE is the linearization point. A second statement
	// classifies failures using a fresh READ COMMITTED snapshot, avoiding the
	// stale fallback-row problem of a data-modifying CTE after lock waits.
	query := fmt.Sprintf(`
		UPDATE %s
		SET content = $2,
			version = CASE WHEN content IS DISTINCT FROM $2 THEN version + 1 ELSE version END,
			lifecycle_version = CASE WHEN content IS DISTINCT FROM $2 THEN $5 ELSE lifecycle_version END,
			last_writer = CASE WHEN content IS DISTINCT FROM $2 THEN $3 ELSE last_writer END,
			updated_at = CASE WHEN content IS DISTINCT FROM $2 THEN NOW() ELSE updated_at END
		WHERE id = $1 AND version = $4 AND deleted = FALSE
		RETURNING version, version <> $4
	`, authTable)
	var (
		newGeneration int64
		changed       bool
	)
	errUpdate := querier.QueryRowContext(ctx, query, relID, json.RawMessage(clean), s.nodeWriter(), expected, lifecycle).Scan(&newGeneration, &changed)
	switch {
	case errUpdate == nil:
		return uint64(newGeneration), changed, nil
	default:
		if !errors.Is(errUpdate, sql.ErrNoRows) {
			return 0, false, fmt.Errorf("postgres store: update auth record: %w", errUpdate)
		}
		return s.authSaveConflict(ctx, querier, relID, expectedGeneration)
	}
}

func postgresGenerationInt64(relID string, generation uint64) (int64, error) {
	if generation > uint64(^uint64(0)>>1) {
		return 0, fmt.Errorf("postgres store: auth generation out of range for %s", relID)
	}
	return int64(generation), nil
}

func (s *PostgresStore) currentAuthRowState(ctx context.Context, querier postgresAuthRowQuerier, relID string) (uint64, bool, error) {
	query := fmt.Sprintf("SELECT version, deleted FROM %s WHERE id = $1", s.fullTableName(s.cfg.AuthTable))
	var (
		generation int64
		deleted    bool
	)
	if err := querier.QueryRowContext(ctx, query, relID).Scan(&generation, &deleted); err != nil {
		return 0, false, err
	}
	if generation < 0 {
		return 0, false, fmt.Errorf("postgres store: negative auth generation for %s", relID)
	}
	return uint64(generation), deleted, nil
}

func (s *PostgresStore) authSaveConflict(ctx context.Context, querier postgresAuthRowQuerier, relID string, expectedGeneration uint64) (uint64, bool, error) {
	currentGeneration, deleted, errState := s.currentAuthRowState(ctx, querier, relID)
	if errors.Is(errState, sql.ErrNoRows) {
		return 0, false, fmt.Errorf("%w for %s (expected generation %d; row missing)", cliproxyauth.ErrAuthStoreConflict, relID, expectedGeneration)
	}
	if errState != nil {
		return 0, false, fmt.Errorf("postgres store: inspect auth record %s: %w", relID, errState)
	}
	if deleted {
		return 0, false, fmt.Errorf("%w for %s (generation %d)", cliproxyauth.ErrAuthStoreDeleted, relID, currentGeneration)
	}
	return 0, false, fmt.Errorf("%w for %s (expected generation %d, current %d)", cliproxyauth.ErrAuthStoreConflict, relID, expectedGeneration, currentGeneration)
}

func (s *PostgresStore) persistAuthTombstone(ctx context.Context, relID string, expectedGeneration uint64, renewDeleted bool) (uint64, bool, error) {
	if s == nil || s.db == nil {
		return 0, false, errors.New("postgres store: not initialized")
	}
	transaction, errBegin := s.beginDetachedAuthTransaction(ctx)
	if errBegin != nil {
		return 0, false, fmt.Errorf("postgres store: begin auth tombstone: %w", errBegin)
	}
	defer transaction.rollback()
	txCtx := transaction.ctx
	tx := transaction.tx
	lifecycleVersion, errLifecycle := s.nextAuthLifecycleVersion(txCtx, tx)
	if errLifecycle != nil {
		return 0, false, fmt.Errorf("postgres store: allocate auth tombstone lifecycle version: %w", errLifecycle)
	}

	newGeneration, changed, errTombstone := s.tombstoneAuthRow(txCtx, tx, relID, expectedGeneration, lifecycleVersion, renewDeleted)
	if errTombstone != nil {
		return 0, false, errTombstone
	}
	markerID := uuid.NewString()
	if errMarker := s.insertAuthCommitMarker(txCtx, tx, markerID); errMarker != nil {
		return 0, false, fmt.Errorf("postgres store: insert auth tombstone commit marker: %w", errMarker)
	}
	if changed {
		if _, errNotify := tx.ExecContext(txCtx, "SELECT pg_notify('cliproxy_auth_changed', $1)", relID); errNotify != nil {
			return 0, false, fmt.Errorf("postgres store: notify auth tombstone: %w", errNotify)
		}
	}
	if errCommit := s.commitDedicatedAuthTransaction(txCtx, transaction, markerID, "auth tombstone", map[string]uint64{relID: newGeneration}); errCommit != nil {
		if errors.Is(errCommit, cliproxyauth.ErrAuthStoreCommitUnknown) {
			return newGeneration, changed, errCommit
		}
		return 0, false, errCommit
	}
	return newGeneration, changed, nil
}

func (s *PostgresStore) tombstoneAuthRow(ctx context.Context, querier postgresAuthRowQuerier, relID string, expectedGeneration, lifecycleVersion uint64, renewDeleted bool) (uint64, bool, error) {
	expected, errExpected := postgresGenerationInt64(relID, expectedGeneration)
	if errExpected != nil {
		return 0, false, errExpected
	}
	lifecycle, errLifecycle := postgresGenerationInt64(relID, lifecycleVersion)
	if errLifecycle != nil || lifecycle == 0 {
		return 0, false, fmt.Errorf("postgres store: invalid auth lifecycle version for %s", relID)
	}
	authTable := s.fullTableName(s.cfg.AuthTable)
	if expectedGeneration == 0 {
		query := fmt.Sprintf(`
			INSERT INTO %s (id, content, version, lifecycle_version, deleted, deleted_at, last_writer, created_at, updated_at)
			VALUES ($1, '{}'::jsonb, 1, $3, TRUE, NOW(), $2, NOW(), NOW())
			ON CONFLICT (id) DO NOTHING
			RETURNING version
		`, authTable)
		var generation int64
		errInsert := querier.QueryRowContext(ctx, query, relID, s.nodeWriter(), lifecycle).Scan(&generation)
		switch {
		case errInsert == nil:
			return uint64(generation), true, nil
		case !errors.Is(errInsert, sql.ErrNoRows):
			return 0, false, fmt.Errorf("postgres store: insert auth tombstone: %w", errInsert)
		}
		currentGeneration, deleted, errState := s.currentAuthRowState(ctx, querier, relID)
		if errState != nil {
			return 0, false, fmt.Errorf("postgres store: inspect auth tombstone %s: %w", relID, errState)
		}
		if deleted {
			if renewDeleted {
				return s.renewAuthTombstone(ctx, querier, relID, lifecycle)
			}
			return currentGeneration, false, nil
		}
		return 0, false, fmt.Errorf("%w for %s (expected generation 0, current %d)", cliproxyauth.ErrAuthStoreConflict, relID, currentGeneration)
	}

	query := fmt.Sprintf(`
		UPDATE %s
		SET content = '{}'::jsonb,
			version = version + 1,
			lifecycle_version = $4,
			deleted = TRUE,
			deleted_at = NOW(),
			last_writer = $2,
			updated_at = NOW()
		WHERE id = $1 AND version = $3 AND deleted = FALSE
		RETURNING version
	`, authTable)
	var generation int64
	errUpdate := querier.QueryRowContext(ctx, query, relID, s.nodeWriter(), expected, lifecycle).Scan(&generation)
	switch {
	case errUpdate == nil:
		return uint64(generation), true, nil
	case !errors.Is(errUpdate, sql.ErrNoRows):
		return 0, false, fmt.Errorf("postgres store: update auth tombstone: %w", errUpdate)
	}
	currentGeneration, deleted, errState := s.currentAuthRowState(ctx, querier, relID)
	if errors.Is(errState, sql.ErrNoRows) {
		return 0, false, fmt.Errorf("%w for %s (expected generation %d; row missing)", cliproxyauth.ErrAuthStoreConflict, relID, expectedGeneration)
	}
	if errState != nil {
		return 0, false, fmt.Errorf("postgres store: inspect auth tombstone %s: %w", relID, errState)
	}
	if deleted {
		if renewDeleted {
			return s.renewAuthTombstone(ctx, querier, relID, lifecycle)
		}
		return currentGeneration, false, nil
	}
	return 0, false, fmt.Errorf("%w for %s (expected generation %d, current %d)", cliproxyauth.ErrAuthStoreConflict, relID, expectedGeneration, currentGeneration)
}

func (s *PostgresStore) renewAuthTombstone(ctx context.Context, querier postgresAuthRowQuerier, relID string, lifecycleVersion int64) (uint64, bool, error) {
	query := fmt.Sprintf(`
		UPDATE %s
		SET version = version + 1,
			lifecycle_version = $3,
			deleted_at = NOW(),
			last_writer = $2,
			updated_at = NOW()
		WHERE id = $1 AND deleted = TRUE
		RETURNING version
	`, s.fullTableName(s.cfg.AuthTable))
	var generation int64
	if errRenew := querier.QueryRowContext(ctx, query, relID, s.nodeWriter(), lifecycleVersion).Scan(&generation); errRenew != nil {
		if errors.Is(errRenew, sql.ErrNoRows) {
			return 0, false, fmt.Errorf("%w for %s (tombstone changed while renewing delete intent)", cliproxyauth.ErrAuthStoreConflict, relID)
		}
		return 0, false, fmt.Errorf("postgres store: renew auth tombstone %s: %w", relID, errRenew)
	}
	if generation < 0 {
		return 0, false, fmt.Errorf("postgres store: negative auth generation for %s", relID)
	}
	return uint64(generation), true, nil
}

func (s *PostgresStore) persistAuthRestore(ctx context.Context, relID string, data []byte, expectedGeneration uint64) (uint64, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("postgres store: not initialized")
	}
	transaction, errBegin := s.beginDetachedAuthTransaction(ctx)
	if errBegin != nil {
		return 0, fmt.Errorf("postgres store: begin auth restore: %w", errBegin)
	}
	defer transaction.rollback()
	txCtx := transaction.ctx
	tx := transaction.tx
	lifecycleVersion, errLifecycle := s.nextAuthLifecycleVersion(txCtx, tx)
	if errLifecycle != nil {
		return 0, fmt.Errorf("postgres store: allocate auth restore lifecycle version: %w", errLifecycle)
	}

	newGeneration, errRestore := s.restoreAuthRow(txCtx, tx, relID, data, expectedGeneration, lifecycleVersion)
	if errRestore != nil {
		return 0, errRestore
	}
	markerID := uuid.NewString()
	if errMarker := s.insertAuthCommitMarker(txCtx, tx, markerID); errMarker != nil {
		return 0, fmt.Errorf("postgres store: insert auth restore commit marker: %w", errMarker)
	}
	if _, errNotify := tx.ExecContext(txCtx, "SELECT pg_notify('cliproxy_auth_changed', $1)", relID); errNotify != nil {
		return 0, fmt.Errorf("postgres store: notify auth restore: %w", errNotify)
	}
	if errCommit := s.commitDedicatedAuthTransaction(txCtx, transaction, markerID, "auth restore", map[string]uint64{relID: newGeneration}); errCommit != nil {
		if errors.Is(errCommit, cliproxyauth.ErrAuthStoreCommitUnknown) {
			return newGeneration, errCommit
		}
		return 0, errCommit
	}
	return newGeneration, nil
}

func (s *PostgresStore) restoreAuthRow(ctx context.Context, querier postgresAuthRowQuerier, relID string, data []byte, expectedGeneration, lifecycleVersion uint64) (uint64, error) {
	clean, errClean := databaseAuthContent(data)
	if errClean != nil {
		return 0, errClean
	}
	lifecycle, errLifecycle := postgresGenerationInt64(relID, lifecycleVersion)
	if errLifecycle != nil || lifecycle == 0 {
		return 0, fmt.Errorf("postgres store: invalid auth lifecycle version for %s", relID)
	}
	authTable := s.fullTableName(s.cfg.AuthTable)
	if expectedGeneration == 0 {
		query := fmt.Sprintf(`
			INSERT INTO %s (id, content, version, lifecycle_version, deleted, deleted_at, last_writer, created_at, updated_at)
			VALUES ($1, $2, 1, $4, FALSE, NULL, $3, NOW(), NOW())
			ON CONFLICT (id) DO NOTHING
			RETURNING version
		`, authTable)
		var generation int64
		errInsert := querier.QueryRowContext(ctx, query, relID, json.RawMessage(clean), s.nodeWriter(), lifecycle).Scan(&generation)
		if errInsert == nil {
			return uint64(generation), nil
		}
		if !errors.Is(errInsert, sql.ErrNoRows) {
			return 0, fmt.Errorf("postgres store: create auth record: %w", errInsert)
		}
		currentGeneration, deleted, errState := s.currentAuthRowState(ctx, querier, relID)
		if errState != nil {
			return 0, fmt.Errorf("postgres store: inspect auth create conflict %s: %w", relID, errState)
		}
		if deleted {
			return 0, fmt.Errorf("%w for %s (tombstone generation %d)", cliproxyauth.ErrAuthStoreDeleted, relID, currentGeneration)
		}
		return 0, fmt.Errorf("%w for %s (active generation %d)", cliproxyauth.ErrAuthStoreConflict, relID, currentGeneration)
	}
	expected, errExpected := postgresGenerationInt64(relID, expectedGeneration)
	if errExpected != nil {
		return 0, errExpected
	}
	query := fmt.Sprintf(`
		UPDATE %s
		SET content = $2,
			version = version + 1,
			lifecycle_version = $5,
			deleted = FALSE,
			deleted_at = NULL,
			last_writer = $3,
			updated_at = NOW()
		WHERE id = $1 AND deleted = TRUE AND version = $4
		RETURNING version
	`, authTable)
	var generation int64
	errRestore := querier.QueryRowContext(ctx, query, relID, json.RawMessage(clean), s.nodeWriter(), expected, lifecycle).Scan(&generation)
	if errRestore == nil {
		return uint64(generation), nil
	}
	if !errors.Is(errRestore, sql.ErrNoRows) {
		return 0, fmt.Errorf("postgres store: restore auth record: %w", errRestore)
	}
	currentGeneration, deleted, errState := s.currentAuthRowState(ctx, querier, relID)
	if errors.Is(errState, sql.ErrNoRows) {
		return 0, fmt.Errorf("%w for %s (expected tombstone generation %d; row missing)", cliproxyauth.ErrAuthStoreConflict, relID, expectedGeneration)
	}
	if errState != nil {
		return 0, fmt.Errorf("postgres store: inspect auth restore conflict %s: %w", relID, errState)
	}
	if !deleted {
		return 0, fmt.Errorf("%w for %s (active generation %d)", cliproxyauth.ErrAuthStoreConflict, relID, currentGeneration)
	}
	return 0, fmt.Errorf("%w for %s (expected tombstone generation %d, current %d)", cliproxyauth.ErrAuthStoreConflict, relID, expectedGeneration, currentGeneration)
}

// persistConfig UPSERTs the single config row. Same non-optimistic-lock
// caveat as persistAuth applies; callers expected to serialize writes via
// Manager / host-level coordination.
func (s *PostgresStore) persistConfig(ctx context.Context, data []byte) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (id, content, version, last_writer, created_at, updated_at)
		VALUES ($1, $2, 1, $3, NOW(), NOW())
		ON CONFLICT (id)
		DO UPDATE SET content = EXCLUDED.content, version = %s.version + 1, last_writer = EXCLUDED.last_writer, updated_at = NOW()
	`, s.fullTableName(s.cfg.ConfigTable), s.fullTableName(s.cfg.ConfigTable))
	normalized := normalizeLineEndings(string(data))
	if _, err := s.db.ExecContext(ctx, query, defaultConfigKey, normalized, s.nodeWriter()); err != nil {
		return fmt.Errorf("postgres store: upsert config: %w", err)
	}
	// Best-effort NOTIFY to peers in cluster mode.
	if _, err := s.db.ExecContext(ctx, "SELECT pg_notify('cliproxy_config_changed', '')"); err != nil {
		log.WithError(err).Debug("pg_notify(cliproxy_config_changed) failed")
	}
	return nil
}

func (s *PostgresStore) deleteConfigRecord(ctx context.Context) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", s.fullTableName(s.cfg.ConfigTable))
	if _, err := s.db.ExecContext(ctx, query, defaultConfigKey); err != nil {
		return fmt.Errorf("postgres store: delete config: %w", err)
	}
	return nil
}

func (s *PostgresStore) resolveAuthPath(auth *cliproxyauth.Auth) (string, error) {
	if auth == nil {
		return "", fmt.Errorf("postgres store: auth is nil")
	}
	if auth.Attributes != nil {
		if p := strings.TrimSpace(auth.Attributes["path"]); p != "" {
			return p, nil
		}
	}
	if fileName := strings.TrimSpace(auth.FileName); fileName != "" {
		if filepath.IsAbs(fileName) {
			return fileName, nil
		}
		return filepath.Join(s.authDir, fileName), nil
	}
	if auth.ID == "" {
		return "", fmt.Errorf("postgres store: missing id")
	}
	if filepath.IsAbs(auth.ID) {
		return auth.ID, nil
	}
	return filepath.Join(s.authDir, filepath.FromSlash(auth.ID)), nil
}

// resolveCanonicalAuthIdentity keeps the runtime Auth.ID, PostgreSQL row key,
// commit-candidate key, and authoritative lookup key identical. Persisting an
// auth under a FileName/path-derived row key that differs from Auth.ID would
// make outcome-unknown convergence unable to prove which logical auth won.
func (s *PostgresStore) resolveCanonicalAuthIdentity(auth *cliproxyauth.Auth) (path, relID string, err error) {
	if auth == nil {
		return "", "", fmt.Errorf("postgres store: auth is nil")
	}
	path, err = s.resolveAuthPath(auth)
	if err != nil {
		return "", "", err
	}
	if path == "" {
		return "", "", fmt.Errorf("postgres store: missing file path attribute for %s", auth.ID)
	}
	relID, err = s.relativeAuthID(path)
	if err != nil {
		return "", "", err
	}
	normalizedID := normalizeAuthID(auth.ID)
	if strings.TrimSpace(auth.ID) == "" || normalizedID != relID {
		return "", "", fmt.Errorf(
			"postgres store: canonical auth id mismatch: auth id %q normalizes to %q but path resolves to %q",
			auth.ID,
			normalizedID,
			relID,
		)
	}
	return path, relID, nil
}

func (s *PostgresStore) resolveDeletePath(id string) (string, error) {
	if filepath.IsAbs(id) {
		return id, nil
	}
	return filepath.Join(s.authDir, filepath.FromSlash(id)), nil
}

func (s *PostgresStore) relativeAuthID(path string) (string, error) {
	if s == nil {
		return "", fmt.Errorf("postgres store: store not initialized")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(s.authDir, path)
	}
	clean := filepath.Clean(path)
	rel, err := filepath.Rel(s.authDir, clean)
	if err != nil {
		return "", fmt.Errorf("postgres store: compute relative path: %w", err)
	}
	if strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("postgres store: path %s outside managed directory", path)
	}
	return filepath.ToSlash(rel), nil
}

func (s *PostgresStore) absoluteAuthPath(id string) (string, error) {
	if s == nil {
		return "", fmt.Errorf("postgres store: store not initialized")
	}
	clean := filepath.Clean(filepath.FromSlash(id))
	if strings.HasPrefix(clean, "..") {
		return "", fmt.Errorf("postgres store: invalid auth identifier %s", id)
	}
	path := filepath.Join(s.authDir, clean)
	rel, err := filepath.Rel(s.authDir, path)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("postgres store: resolved auth path escapes auth directory")
	}
	return path, nil
}

func (s *PostgresStore) fullTableName(name string) string {
	if strings.TrimSpace(s.cfg.Schema) == "" {
		return quoteIdentifier(name)
	}
	return quoteIdentifier(s.cfg.Schema) + "." + quoteIdentifier(name)
}

func quoteIdentifier(identifier string) string {
	replaced := strings.ReplaceAll(identifier, "\"", "\"\"")
	return "\"" + replaced + "\""
}

func authPayloadGeneration(value any) (uint64, bool) {
	switch typed := value.(type) {
	case float64:
		generation := uint64(typed)
		return generation, typed >= 0 && float64(generation) == typed
	case json.Number:
		generation, err := typed.Int64()
		return uint64(generation), err == nil && generation >= 0
	case int64:
		return uint64(typed), typed >= 0
	case int:
		return uint64(typed), typed >= 0
	case uint64:
		return typed, true
	default:
		return 0, false
	}
}

func valueAsString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case fmt.Stringer:
		return t.String()
	default:
		return ""
	}
}

func labelFor(metadata map[string]any) string {
	if metadata == nil {
		return ""
	}
	if v := strings.TrimSpace(valueAsString(metadata["label"])); v != "" {
		return v
	}
	if v := strings.TrimSpace(valueAsString(metadata["email"])); v != "" {
		return v
	}
	if v := strings.TrimSpace(valueAsString(metadata["project_id"])); v != "" {
		return v
	}
	return ""
}

func normalizeAuthID(id string) string {
	return filepath.ToSlash(filepath.Clean(id))
}

func normalizeLineEndings(s string) string {
	if s == "" {
		return s
	}
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	return s
}
