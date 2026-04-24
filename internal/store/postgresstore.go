package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/misc"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const (
	defaultConfigTable = "config_store"
	defaultAuthTable   = "auth_store"
	defaultConfigKey   = "config"
)

// PostgresStoreConfig captures configuration required to initialize a Postgres-backed store.
type PostgresStoreConfig struct {
	DSN         string
	Schema      string
	ConfigTable string
	AuthTable   string
	SpoolDir    string
}

// PostgresStore persists configuration and authentication metadata using PostgreSQL as backend
// while mirroring data to a local workspace so existing file-based workflows continue to operate.
type PostgresStore struct {
	db         *sql.DB
	cfg        PostgresStoreConfig
	spoolRoot  string
	configPath string
	authDir    string
	nodeID     string
	mu         sync.Mutex
}

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

// NewPostgresStore establishes a connection to PostgreSQL and prepares the local workspace.
func NewPostgresStore(ctx context.Context, cfg PostgresStoreConfig) (*PostgresStore, error) {
	trimmedDSN := strings.TrimSpace(cfg.DSN)
	if trimmedDSN == "" {
		return nil, fmt.Errorf("postgres store: DSN is required")
	}
	cfg.DSN = trimmedDSN
	if cfg.ConfigTable == "" {
		cfg.ConfigTable = defaultConfigTable
	}
	if cfg.AuthTable == "" {
		cfg.AuthTable = defaultAuthTable
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

	db, err := sql.Open("pgx", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres store: open database connection: %w", err)
	}
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres store: ping database: %w", err)
	}

	store := &PostgresStore{
		db:         db,
		cfg:        cfg,
		spoolRoot:  absSpool,
		configPath: filepath.Join(configDir, "config.yaml"),
		authDir:    authDir,
	}
	return store, nil
}

// Close releases the underlying database connection.
func (s *PostgresStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
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

	authTable := s.fullTableName(s.cfg.AuthTable)
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			content JSONB NOT NULL,
			version BIGINT NOT NULL DEFAULT 0,
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
		"ALTER TABLE %s ADD COLUMN IF NOT EXISTS last_writer TEXT",
		authTable,
	)); err != nil {
		return fmt.Errorf("postgres store: add last_writer col to auth table: %w", err)
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

	path, err := s.resolveAuthPath(auth)
	if err != nil {
		return "", err
	}
	if path == "" {
		return "", fmt.Errorf("postgres store: missing file path attribute for %s", auth.ID)
	}

	if auth.Disabled {
		if _, statErr := os.Stat(path); errors.Is(statErr, fs.ErrNotExist) {
			return "", nil
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err = os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", fmt.Errorf("postgres store: create auth directory: %w", err)
	}

	switch {
	case auth.Storage != nil:
		if err = auth.Storage.SaveTokenToFile(path); err != nil {
			return "", err
		}
	case auth.Metadata != nil:
		raw, errMarshal := json.Marshal(auth.Metadata)
		if errMarshal != nil {
			return "", fmt.Errorf("postgres store: marshal metadata: %w", errMarshal)
		}
		if existing, errRead := os.ReadFile(path); errRead == nil {
			if jsonEqual(existing, raw) {
				return path, nil
			}
		} else if errRead != nil && !errors.Is(errRead, fs.ErrNotExist) {
			return "", fmt.Errorf("postgres store: read existing metadata: %w", errRead)
		}
		tmp := path + ".tmp"
		if errWrite := os.WriteFile(tmp, raw, 0o600); errWrite != nil {
			return "", fmt.Errorf("postgres store: write temp auth file: %w", errWrite)
		}
		if errRename := os.Rename(tmp, path); errRename != nil {
			return "", fmt.Errorf("postgres store: rename auth file: %w", errRename)
		}
	default:
		return "", fmt.Errorf("postgres store: nothing to persist for %s", auth.ID)
	}

	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	auth.Attributes["path"] = path

	if strings.TrimSpace(auth.FileName) == "" {
		auth.FileName = auth.ID
	}

	relID, err := s.relativeAuthID(path)
	if err != nil {
		return "", err
	}
	if err = s.upsertAuthRecord(ctx, relID, path); err != nil {
		return "", err
	}
	return path, nil
}

// List enumerates all auth records stored in PostgreSQL.
func (s *PostgresStore) List(ctx context.Context) ([]*cliproxyauth.Auth, error) {
	query := fmt.Sprintf("SELECT id, content, created_at, updated_at FROM %s ORDER BY id", s.fullTableName(s.cfg.AuthTable))
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("postgres store: list auth: %w", err)
	}
	defer rows.Close()

	auths := make([]*cliproxyauth.Auth, 0, 32)
	for rows.Next() {
		var (
			id        string
			payload   string
			createdAt time.Time
			updatedAt time.Time
		)
		if err = rows.Scan(&id, &payload, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("postgres store: scan auth row: %w", err)
		}
		auth, built := s.buildAuthFromRow(id, payload, createdAt, updatedAt)
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
		"SELECT id, content, created_at, updated_at FROM %s WHERE id = $1",
		s.fullTableName(s.cfg.AuthTable),
	)
	var (
		rowID     string
		payload   string
		createdAt time.Time
		updatedAt time.Time
	)
	err := s.db.QueryRowContext(ctx, query, id).Scan(&rowID, &payload, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("postgres store: get auth %s: %w", id, err)
	}
	auth, built := s.buildAuthFromRow(rowID, payload, createdAt, updatedAt)
	if !built {
		// Row exists but payload invalid / outside spool — surface as
		// "not found" so the Manager drops the cached copy; a subsequent
		// Save will correct it.
		return nil, nil
	}
	return auth, nil
}

// buildAuthFromRow decodes a single auth row into a *cliproxyauth.Auth.
// Shared by List and GetByID so the shape of the in-memory auth stays
// consistent across the two code paths. Returns (_, false) when the row
// should be skipped (invalid path or bad JSON) — callers treat that as
// "not present".
func (s *PostgresStore) buildAuthFromRow(id, payload string, createdAt, updatedAt time.Time) (*cliproxyauth.Auth, bool) {
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
	provider := strings.TrimSpace(valueAsString(metadata["type"]))
	if provider == "" {
		provider = "unknown"
	}
	attr := map[string]string{"path": path}
	if email := strings.TrimSpace(valueAsString(metadata["email"])); email != "" {
		attr["email"] = email
	}
	auth := &cliproxyauth.Auth{
		ID:               normalizeAuthID(id),
		Provider:         provider,
		FileName:         normalizeAuthID(id),
		Label:            labelFor(metadata),
		Status:           cliproxyauth.StatusActive,
		Attributes:       attr,
		Metadata:         metadata,
		CreatedAt:        createdAt,
		UpdatedAt:        updatedAt,
		LastRefreshedAt:  time.Time{},
		NextRefreshAfter: time.Time{},
	}
	cliproxyauth.ApplyCustomHeadersFromMetadata(auth)
	return auth, true
}

// Delete removes an auth file and the corresponding database record.
func (s *PostgresStore) Delete(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("postgres store: id is empty")
	}
	path, err := s.resolveDeletePath(id)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err = os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("postgres store: delete auth file: %w", err)
	}
	relID, err := s.relativeAuthID(path)
	if err != nil {
		return err
	}
	return s.deleteAuthRecord(ctx, relID)
}

// PersistAuthFiles stores the provided auth file changes in PostgreSQL.
func (s *PostgresStore) PersistAuthFiles(ctx context.Context, _ string, paths ...string) error {
	if len(paths) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

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
		if err = s.syncAuthFile(ctx, relID, trimmed); err != nil {
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

// syncConfigFromDatabase writes the database-stored config to disk or seeds the database from template.
func (s *PostgresStore) syncConfigFromDatabase(ctx context.Context, exampleConfigPath string) error {
	query := fmt.Sprintf("SELECT content FROM %s WHERE id = $1", s.fullTableName(s.cfg.ConfigTable))
	var content string
	err := s.db.QueryRowContext(ctx, query, defaultConfigKey).Scan(&content)
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
func (s *PostgresStore) syncAuthFromDatabase(ctx context.Context) error {
	query := fmt.Sprintf("SELECT id, content FROM %s", s.fullTableName(s.cfg.AuthTable))
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
			id      string
			payload string
		)
		if err = rows.Scan(&id, &payload); err != nil {
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
		if err = os.WriteFile(path, []byte(payload), 0o600); err != nil {
			return fmt.Errorf("postgres store: write auth file: %w", err)
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("postgres store: iterate auth rows: %w", err)
	}
	return nil
}

func (s *PostgresStore) syncAuthFile(ctx context.Context, relID, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return s.deleteAuthRecord(ctx, relID)
		}
		return fmt.Errorf("postgres store: read auth file: %w", err)
	}
	if len(data) == 0 {
		return s.deleteAuthRecord(ctx, relID)
	}
	return s.persistAuth(ctx, relID, data)
}

func (s *PostgresStore) upsertAuthRecord(ctx context.Context, relID, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("postgres store: read auth file: %w", err)
	}
	if len(data) == 0 {
		return s.deleteAuthRecord(ctx, relID)
	}
	return s.persistAuth(ctx, relID, data)
}

// persistAuth UPSERTs the auth record and bumps the version counter.
//
// NOTE: this is NOT an optimistic-lock UPSERT — it has no
// "WHERE version = $expected" guard, so two concurrent writers would silently
// clobber each other. Cross-instance mutual exclusion is provided at a higher
// layer via auth.AuthRefreshLocker in Manager.refreshAuthOnce. The
// version/last_writer columns here are an audit trail, not a concurrency
// control mechanism.
func (s *PostgresStore) persistAuth(ctx context.Context, relID string, data []byte) error {
	jsonPayload := json.RawMessage(data)
	query := fmt.Sprintf(`
		INSERT INTO %s (id, content, version, last_writer, created_at, updated_at)
		VALUES ($1, $2, 1, $3, NOW(), NOW())
		ON CONFLICT (id)
		DO UPDATE SET content = EXCLUDED.content, version = %s.version + 1, last_writer = EXCLUDED.last_writer, updated_at = NOW()
	`, s.fullTableName(s.cfg.AuthTable), s.fullTableName(s.cfg.AuthTable))
	if _, err := s.db.ExecContext(ctx, query, relID, jsonPayload, s.nodeWriter()); err != nil {
		return fmt.Errorf("postgres store: upsert auth record: %w", err)
	}
	// Best-effort NOTIFY to peers in cluster mode; harmless in single-instance.
	// Log at debug level so operators can spot misconfig without log spam.
	if _, err := s.db.ExecContext(ctx, "SELECT pg_notify('cliproxy_auth_changed', $1)", relID); err != nil {
		log.WithError(err).Debugf("pg_notify(cliproxy_auth_changed, %s) failed", relID)
	}
	return nil
}

func (s *PostgresStore) deleteAuthRecord(ctx context.Context, relID string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", s.fullTableName(s.cfg.AuthTable))
	if _, err := s.db.ExecContext(ctx, query, relID); err != nil {
		return fmt.Errorf("postgres store: delete auth record: %w", err)
	}
	return nil
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

func (s *PostgresStore) resolveDeletePath(id string) (string, error) {
	if strings.ContainsRune(id, os.PathSeparator) || filepath.IsAbs(id) {
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
