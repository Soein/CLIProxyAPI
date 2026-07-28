package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

const authoritativeListDriverName = "postgresstore-authoritative-list-test"

var registerAuthoritativeListDriver sync.Once

type authoritativeListDriver struct{}

type authoritativeListConn struct {
	source string
}

type authoritativeListTx struct{}

type authoritativeListRows struct {
	source string
	done   bool
}

type authoritativeConfigRows struct {
	source string
	done   bool
}

type authoritativeFenceRows struct {
	source string
	done   bool
}

type authoritativeLifecycleRows struct {
	source string
	done   bool
}

type authoritativeBatchRows struct {
	id   string
	done bool
}

func (authoritativeListDriver) Open(name string) (driver.Conn, error) {
	return &authoritativeListConn{source: name}, nil
}

func (*authoritativeListConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (*authoritativeListConn) Close() error { return nil }

func (*authoritativeListConn) Begin() (driver.Tx, error) { return &authoritativeListTx{}, nil }

func (*authoritativeListTx) Commit() error   { return nil }
func (*authoritativeListTx) Rollback() error { return nil }

func (*authoritativeListConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return &authoritativeListTx{}, nil
}

func (*authoritativeListConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

func (c *authoritativeListConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(query, defaultAuthLifecycleClockTable) && strings.Contains(query, "RETURNING value") {
		return &authoritativeFenceRows{source: c.source}, nil
	}
	if strings.Contains(query, "SELECT version, lifecycle_version, deleted, updated_at, deleted_at") {
		return &authoritativeLifecycleRows{source: c.source}, nil
	}
	if strings.Contains(query, "SELECT id, content, version, deleted, created_at, updated_at") {
		id := ""
		if len(args) > 0 {
			id, _ = args[0].Value.(string)
		}
		return &authoritativeBatchRows{id: id}, nil
	}
	if strings.Contains(query, "SELECT content") {
		return &authoritativeConfigRows{source: c.source}, nil
	}
	return &authoritativeListRows{source: c.source}, nil
}

func (*authoritativeBatchRows) Columns() []string {
	return []string{"id", "content", "version", "deleted", "created_at", "updated_at"}
}

func (*authoritativeBatchRows) Close() error { return nil }

func (r *authoritativeBatchRows) Next(dest []driver.Value) error {
	if r.done || r.id == "missing.json" {
		return io.EOF
	}
	r.done = true
	now := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	dest[0] = r.id
	dest[1] = `{"type":"xai","disabled":false}`
	dest[2] = int64(9)
	dest[3] = r.id == "tombstone.json"
	dest[4] = now
	dest[5] = now
	return nil
}

func (*authoritativeListRows) Columns() []string {
	return []string{"id", "content", "version", "created_at", "updated_at"}
}

func (*authoritativeListRows) Close() error { return nil }

func (r *authoritativeListRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	now := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	dest[0] = r.source + ".json"
	dest[1] = `{"type":"xai","access_token":"` + r.source + `"}`
	dest[2] = int64(7)
	dest[3] = now
	dest[4] = now
	return nil
}

func (*authoritativeConfigRows) Columns() []string { return []string{"content"} }

func (*authoritativeConfigRows) Close() error { return nil }

func (r *authoritativeConfigRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = "host: 127.0.0.1\nsource: " + r.source + "\n"
	return nil
}

func (*authoritativeFenceRows) Columns() []string { return []string{"clock_timestamp"} }

func (*authoritativeFenceRows) Close() error { return nil }

func (r *authoritativeFenceRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	version := int64(10)
	if r.source == "writer" {
		version = 20
	}
	dest[0] = version
	return nil
}

func (*authoritativeLifecycleRows) Columns() []string {
	return []string{"version", "lifecycle_version", "deleted", "updated_at", "deleted_at"}
}

func (*authoritativeLifecycleRows) Close() error { return nil }

func (r *authoritativeLifecycleRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	updatedAt := time.Date(2026, 7, 20, 2, 5, 0, 0, time.UTC)
	deletedAt := time.Date(2026, 7, 20, 2, 6, 0, 0, time.UTC)
	if r.source == "writer" {
		dest[0] = int64(7)
		dest[1] = int64(19)
		dest[2] = true
		dest[3] = updatedAt
		dest[4] = deletedAt
		return nil
	}
	dest[0] = int64(3)
	dest[1] = int64(9)
	dest[2] = false
	dest[3] = updatedAt.Add(-time.Hour)
	dest[4] = nil
	return nil
}

func TestPostgresStoreAuthListsUseWriterPool(t *testing.T) {
	registerAuthoritativeListDriver.Do(func() {
		sql.Register(authoritativeListDriverName, authoritativeListDriver{})
	})
	writer, errWriter := sql.Open(authoritativeListDriverName, "writer")
	if errWriter != nil {
		t.Fatalf("open writer DB: %v", errWriter)
	}
	defer writer.Close()
	reader, errReader := sql.Open(authoritativeListDriverName, "reader")
	if errReader != nil {
		t.Fatalf("open reader DB: %v", errReader)
	}
	defer reader.Close()

	store := &PostgresStore{
		db:      writer,
		readDB:  reader,
		cfg:     PostgresStoreConfig{AuthTable: defaultAuthTable},
		authDir: t.TempDir(),
	}
	readAuths, errRead := store.List(context.Background())
	if errRead != nil {
		t.Fatalf("List() error: %v", errRead)
	}
	if len(readAuths) != 1 || readAuths[0].ID != "writer.json" {
		t.Fatalf("List() auths = %+v, want writer row", readAuths)
	}
	if readAuths[0].StoreGeneration() != 7 {
		t.Fatalf("List() generation = %d, want 7", readAuths[0].StoreGeneration())
	}

	authoritative, errAuthoritative := store.ListAuthoritative(context.Background())
	if errAuthoritative != nil {
		t.Fatalf("ListAuthoritative() error: %v", errAuthoritative)
	}
	if len(authoritative) != 1 || authoritative[0].ID != "writer.json" {
		t.Fatalf("ListAuthoritative() auths = %+v, want writer row", authoritative)
	}
	if authoritative[0].StoreGeneration() != 7 {
		t.Fatalf("ListAuthoritative() generation = %d, want 7", authoritative[0].StoreGeneration())
	}
}

func TestPostgresStoreSyncConfigAuthoritativeUsesWriterPool(t *testing.T) {
	registerAuthoritativeListDriver.Do(func() {
		sql.Register(authoritativeListDriverName, authoritativeListDriver{})
	})
	writer, errWriter := sql.Open(authoritativeListDriverName, "writer")
	if errWriter != nil {
		t.Fatalf("open writer DB: %v", errWriter)
	}
	defer writer.Close()
	reader, errReader := sql.Open(authoritativeListDriverName, "reader")
	if errReader != nil {
		t.Fatalf("open reader DB: %v", errReader)
	}
	defer reader.Close()

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("source: reader\n"), 0o600); errWrite != nil {
		t.Fatalf("seed config: %v", errWrite)
	}
	store := &PostgresStore{
		db:         writer,
		readDB:     reader,
		cfg:        PostgresStoreConfig{ConfigTable: defaultConfigTable},
		configPath: configPath,
	}
	if errSync := store.SyncConfigAuthoritative(context.Background()); errSync != nil {
		t.Fatalf("SyncConfigAuthoritative() error: %v", errSync)
	}
	got, errRead := os.ReadFile(configPath)
	if errRead != nil {
		t.Fatalf("read synchronized config: %v", errRead)
	}
	if !strings.Contains(string(got), "source: writer") {
		t.Fatalf("synchronized config = %q, want writer content", got)
	}
}

func TestPostgresStoreLifecycleFenceAndStateUseWriterPool(t *testing.T) {
	registerAuthoritativeListDriver.Do(func() {
		sql.Register(authoritativeListDriverName, authoritativeListDriver{})
	})
	writer, errWriter := sql.Open(authoritativeListDriverName, "writer")
	if errWriter != nil {
		t.Fatalf("open writer DB: %v", errWriter)
	}
	defer writer.Close()
	reader, errReader := sql.Open(authoritativeListDriverName, "reader")
	if errReader != nil {
		t.Fatalf("open reader DB: %v", errReader)
	}
	defer reader.Close()

	store := &PostgresStore{
		db:      writer,
		readDB:  reader,
		cfg:     PostgresStoreConfig{AuthTable: defaultAuthTable},
		authDir: t.TempDir(),
	}
	fence, errFence := store.AuthLifecycleFence(context.Background())
	if errFence != nil {
		t.Fatalf("AuthLifecycleFence() error: %v", errFence)
	}
	const wantFence uint64 = 20
	if fence != wantFence {
		t.Fatalf("AuthLifecycleFence() = %v, want writer lifecycle version %v", fence, wantFence)
	}

	state, errState := store.GetAuthLifecycle(context.Background(), "writer.json")
	if errState != nil {
		t.Fatalf("GetAuthLifecycle() error: %v", errState)
	}
	if !state.Exists || !state.Deleted || state.Generation != 7 || state.LifecycleVersion != 19 {
		t.Fatalf("GetAuthLifecycle() = %#v, want writer tombstone generation 7", state)
	}
	wantDeletedAt := time.Date(2026, 7, 20, 2, 6, 0, 0, time.UTC)
	if !state.DeletedAt.Equal(wantDeletedAt) {
		t.Fatalf("GetAuthLifecycle() deleted_at = %v, want %v", state.DeletedAt, wantDeletedAt)
	}
}

func TestPostgresStoreWithAuthoritativeAuthBatchIncludesActiveTombstoneAndMissing(t *testing.T) {
	registerAuthoritativeListDriver.Do(func() {
		sql.Register(authoritativeListDriverName, authoritativeListDriver{})
	})
	db, errOpen := sql.Open(authoritativeListDriverName, "writer")
	if errOpen != nil {
		t.Fatalf("open writer DB: %v", errOpen)
	}
	defer db.Close()
	store := &PostgresStore{
		db:      db,
		cfg:     PostgresStoreConfig{AuthTable: defaultAuthTable},
		authDir: t.TempDir(),
	}
	ids := []string{"active.json", "tombstone.json", "missing.json"}
	finalizeCalls := 0
	errBatch := store.WithAuthoritativeAuthBatch(context.Background(), ids, func(states map[string]cliproxyauth.AuthAuthoritativeState) error {
		finalizeCalls++
		active := states[ids[0]]
		if !active.Exists || active.Deleted || active.Generation != 9 || active.Auth == nil || active.Auth.StoreGeneration() != 9 {
			t.Fatalf("active state = %#v", active)
		}
		tombstone := states[ids[1]]
		if !tombstone.Exists || !tombstone.Deleted || tombstone.Generation != 9 || tombstone.Auth != nil {
			t.Fatalf("tombstone state = %#v", tombstone)
		}
		missing := states[ids[2]]
		if missing.Exists || missing.Deleted || missing.Generation != 0 || missing.Auth != nil {
			t.Fatalf("missing state = %#v", missing)
		}
		return nil
	})
	if errBatch != nil {
		t.Fatalf("WithAuthoritativeAuthBatch() error: %v", errBatch)
	}
	if finalizeCalls != 1 {
		t.Fatalf("finalize calls = %d, want 1", finalizeCalls)
	}
}
