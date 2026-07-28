package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

const postgresBatchDriverName = "postgresstore-batch-test"

var (
	registerPostgresBatchDriver sync.Once
	postgresBatchStates         sync.Map
)

type postgresBatchDriver struct{}

type postgresBatchDriverState struct {
	mu                        sync.Mutex
	openCount                 int
	closeCount                int
	closedConnectionIDs       []int
	blockStage                string
	blockStarted              chan struct{}
	blockRelease              chan struct{}
	beginCount                int
	beginCanceled             bool
	verificationBeginErr      error
	commitCount               int
	rollbackCount             int
	commitErr                 error
	commitApplied             bool
	verificationCommitErr     error
	upsertIDs                 []string
	upsertQueries             []string
	upsertExpected            []int64
	upsertPayloads            []string
	upsertRows                int64
	upsertVersion             int64
	upsertConflictID          string
	upsertMissingID           string
	upsertDeletedID           string
	markerInsertCount         int
	markerID                  string
	markerExists              bool
	markerVisibleAfter        int
	markerQueryCount          int
	markerQueryErr            error
	markerQueryWaitForContext bool
	markerDeleteCount         int
	markerDeleteErr           error
	markerEventOrder          []string
	notifyCount               int
	notifyArgCount            int
	notifyIDs                 []string
	notifyErr                 error
	lifecycleClock            int64
	lifecycleLockCount        int
	verificationLockErr       error
	authStateQueryCount       int
	authStateDeletedSequence  []bool
	authMirrorPayload         string
	authoritativeIDs          []string
	readQueries               []string
	execQueries               []string
	allowSchema               bool
}

type postgresBatchConn struct {
	state  *postgresBatchDriverState
	id     int
	closed chan struct{}
	close  sync.Once
}

type postgresBatchTx struct {
	conn *postgresBatchConn
}

type postgresBatchRows struct {
	columns []string
	values  []driver.Value
	done    bool
}

type postgresBatchMultiRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (postgresBatchDriver) Open(name string) (driver.Conn, error) {
	value, ok := postgresBatchStates.Load(name)
	if !ok {
		return nil, errors.New("missing postgres batch driver state")
	}
	state, ok := value.(*postgresBatchDriverState)
	if !ok || state == nil {
		return nil, errors.New("invalid postgres batch driver state")
	}
	state.mu.Lock()
	state.openCount++
	id := state.openCount
	state.mu.Unlock()
	return &postgresBatchConn{state: state, id: id, closed: make(chan struct{})}, nil
}

func (*postgresBatchConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }

func (c *postgresBatchConn) Close() error {
	c.close.Do(func() {
		c.state.mu.Lock()
		c.state.closeCount++
		c.state.closedConnectionIDs = append(c.state.closedConnectionIDs, c.id)
		c.state.mu.Unlock()
		close(c.closed)
	})
	return nil
}

func (*postgresBatchConn) Begin() (driver.Tx, error) { return nil, driver.ErrSkip }

func (c *postgresBatchConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	c.state.mu.Lock()
	c.state.beginCount++
	c.state.beginCanceled = ctx.Err() != nil
	block := c.id == 1 && c.state.blockStage == "begin"
	started := c.state.blockStarted
	release := c.state.blockRelease
	if c.state.beginCount > 1 && c.state.verificationBeginErr != nil {
		c.state.mu.Unlock()
		return nil, c.state.verificationBeginErr
	}
	c.state.mu.Unlock()
	if block {
		signalPostgresBatchBlock(started)
		select {
		case <-c.closed:
			return nil, driver.ErrBadConn
		case <-release:
		}
	}
	return &postgresBatchTx{conn: c}, nil
}

func (c *postgresBatchConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.state.mu.Lock()
	block := c.id == 1 && c.state.blockStage == "exec"
	started := c.state.blockStarted
	release := c.state.blockRelease
	c.state.mu.Unlock()
	if block {
		signalPostgresBatchBlock(started)
		select {
		case <-c.closed:
			return nil, driver.ErrBadConn
		case <-release:
		}
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if c.state.allowSchema {
		c.state.execQueries = append(c.state.execQueries, query)
		return driver.RowsAffected(1), nil
	}
	if strings.Contains(query, "pg_advisory_xact_lock") {
		c.state.lifecycleLockCount++
		if c.state.lifecycleLockCount > 1 && c.state.verificationLockErr != nil {
			return nil, c.state.verificationLockErr
		}
		return driver.RowsAffected(1), nil
	}
	if strings.Contains(query, "pg_notify") {
		c.state.notifyCount++
		c.state.notifyArgCount = len(args)
		if len(args) > 0 {
			if id, ok := args[0].Value.(string); ok {
				c.state.notifyIDs = append(c.state.notifyIDs, id)
			}
		}
		return driver.RowsAffected(1), c.state.notifyErr
	}
	if strings.Contains(query, "DELETE FROM") && strings.Contains(query, defaultAuthBatchCommitTable) {
		c.state.markerDeleteCount++
		c.state.markerEventOrder = append(c.state.markerEventOrder, "delete")
		return driver.RowsAffected(1), c.state.markerDeleteErr
	}
	if strings.Contains(query, defaultAuthBatchCommitTable) {
		c.state.markerInsertCount++
		if len(args) > 0 {
			c.state.markerID, _ = args[0].Value.(string)
		}
		return driver.RowsAffected(1), nil
	}
	return nil, errors.New("unexpected batch test ExecContext query")
}

func (c *postgresBatchConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.state.mu.Lock()
	if strings.Contains(query, "SELECT EXISTS") && c.state.markerQueryWaitForContext {
		c.state.markerQueryCount++
		c.state.mu.Unlock()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	defer c.state.mu.Unlock()
	if strings.Contains(query, defaultAuthLifecycleClockTable) && strings.Contains(query, "RETURNING value") {
		c.state.lifecycleClock++
		return &postgresBatchRows{columns: []string{"value"}, values: []driver.Value{c.state.lifecycleClock}}, nil
	}
	if strings.Contains(query, "SELECT EXISTS") {
		c.state.markerQueryCount++
		c.state.markerEventOrder = append(c.state.markerEventOrder, "select")
		if c.state.markerQueryErr != nil {
			return nil, c.state.markerQueryErr
		}
		exists := c.state.markerExists
		if c.state.markerVisibleAfter > 0 && c.state.markerQueryCount >= c.state.markerVisibleAfter {
			exists = true
		}
		return &postgresBatchRows{columns: []string{"exists"}, values: []driver.Value{exists}}, nil
	}
	if strings.Contains(query, "SELECT id, content, version, created_at, updated_at") {
		c.state.readQueries = append(c.state.readQueries, query)
		return &postgresBatchRows{columns: []string{"id", "content", "version", "created_at", "updated_at"}}, nil
	}
	if strings.Contains(query, "SELECT id, content, version FROM") {
		c.state.readQueries = append(c.state.readQueries, query)
		return &postgresBatchRows{columns: []string{"id", "content", "version"}}, nil
	}
	if strings.Contains(query, "SELECT id FROM") && strings.Contains(query, "ORDER BY id") {
		values := make([][]driver.Value, 0, len(c.state.authoritativeIDs))
		for _, authoritativeID := range c.state.authoritativeIDs {
			values = append(values, []driver.Value{authoritativeID})
		}
		return &postgresBatchMultiRows{columns: []string{"id"}, values: values}, nil
	}
	id := ""
	if len(args) > 0 {
		id, _ = args[0].Value.(string)
	}
	if strings.Contains(query, "SELECT version, deleted FROM") {
		c.state.authStateQueryCount++
		if id == c.state.upsertMissingID {
			return &postgresBatchRows{columns: []string{"version", "deleted"}}, nil
		}
		version := c.state.upsertVersion
		if version == 0 {
			version = 4
		}
		deleted := id == c.state.upsertDeletedID
		if index := c.state.authStateQueryCount - 1; index >= 0 && index < len(c.state.authStateDeletedSequence) {
			deleted = c.state.authStateDeletedSequence[index]
		}
		return &postgresBatchRows{
			columns: []string{"version", "deleted"},
			values:  []driver.Value{version, deleted},
		}, nil
	}
	if strings.Contains(query, "SELECT content, version, deleted FROM") {
		c.state.authStateQueryCount++
		if id == c.state.upsertMissingID {
			return &postgresBatchRows{columns: []string{"content", "version", "deleted"}}, nil
		}
		version := c.state.upsertVersion
		if version == 0 {
			version = 4
		}
		deleted := id == c.state.upsertDeletedID
		if index := c.state.authStateQueryCount - 1; index >= 0 && index < len(c.state.authStateDeletedSequence) {
			deleted = c.state.authStateDeletedSequence[index]
		}
		payload := c.state.authMirrorPayload
		if payload == "" {
			payload = `{"type":"codex"}`
		}
		return &postgresBatchRows{
			columns: []string{"content", "version", "deleted"},
			values:  []driver.Value{payload, version, deleted},
		}, nil
	}

	c.state.upsertQueries = append(c.state.upsertQueries, query)
	c.state.upsertIDs = append(c.state.upsertIDs, id)
	if len(args) > 1 {
		if payload, ok := args[1].Value.([]byte); ok {
			c.state.upsertPayloads = append(c.state.upsertPayloads, string(payload))
		}
	}
	isRestore := strings.Contains(query, "WHERE id = $1 AND deleted = TRUE AND version = $4")
	isTombstoneUpdate := strings.Contains(query, "WHERE id = $1 AND version = $3 AND deleted = FALSE")
	isTombstoneRenew := strings.Contains(query, "WHERE id = $1 AND deleted = TRUE") && !isRestore
	if strings.Contains(query, "RETURNING version, version <>") && len(args) > 3 {
		if expected, ok := args[3].Value.(int64); ok {
			c.state.upsertExpected = append(c.state.upsertExpected, expected)
		}
	} else if isRestore && len(args) > 3 {
		if expected, ok := args[3].Value.(int64); ok {
			c.state.upsertExpected = append(c.state.upsertExpected, expected)
		}
	} else if isTombstoneUpdate && len(args) > 2 {
		if expected, ok := args[2].Value.(int64); ok {
			c.state.upsertExpected = append(c.state.upsertExpected, expected)
		}
	}
	if id == c.state.upsertConflictID || id == c.state.upsertMissingID || id == c.state.upsertDeletedID {
		if isTombstoneRenew && id == c.state.upsertDeletedID {
			version := c.state.upsertVersion
			if version == 0 {
				version = 4
			}
			return &postgresBatchRows{columns: []string{"version"}, values: []driver.Value{version + 1}}, nil
		}
		if strings.Contains(query, "RETURNING version, version <>") {
			return &postgresBatchRows{columns: []string{"version", "changed"}}, nil
		}
		return &postgresBatchRows{columns: []string{"version"}}, nil
	}
	changed := c.state.upsertRows > 0
	version := c.state.upsertVersion
	if version == 0 {
		version = 1
		if strings.Contains(query, "RETURNING version, version <>") && len(args) > 3 {
			if expected, ok := args[3].Value.(int64); ok {
				version = expected
				if changed {
					version++
				}
			}
		} else if isRestore && len(args) > 3 {
			if expected, ok := args[3].Value.(int64); ok {
				version = expected + 1
			}
		} else if isTombstoneUpdate && len(args) > 2 {
			if expected, ok := args[2].Value.(int64); ok {
				version = expected + 1
			}
		}
	}
	if strings.Contains(query, "RETURNING version, version <>") {
		return &postgresBatchRows{
			columns: []string{"version", "changed"},
			values:  []driver.Value{version, changed},
		}, nil
	}
	return &postgresBatchRows{
		columns: []string{"version"},
		values:  []driver.Value{version},
	}, nil
}

func (tx *postgresBatchTx) Commit() error {
	state := tx.conn.state
	state.mu.Lock()
	state.commitCount++
	commitCount := state.commitCount
	if commitCount == 1 && state.commitApplied {
		state.markerExists = true
	}
	block := tx.conn.id == 1 && state.blockStage == "commit"
	started := state.blockStarted
	release := state.blockRelease
	commitErr := state.commitErr
	verificationCommitErr := state.verificationCommitErr
	state.mu.Unlock()
	if block {
		signalPostgresBatchBlock(started)
		select {
		case <-tx.conn.closed:
			return driver.ErrBadConn
		case <-release:
		}
	}
	if commitCount == 1 {
		return commitErr
	}
	return verificationCommitErr
}

func signalPostgresBatchBlock(started chan struct{}) {
	if started == nil {
		return
	}
	select {
	case started <- struct{}{}:
	default:
	}
}

func (r *postgresBatchRows) Columns() []string { return r.columns }

func (*postgresBatchRows) Close() error { return nil }

func (r *postgresBatchRows) Next(dest []driver.Value) error {
	if r.done || len(r.values) == 0 {
		return io.EOF
	}
	r.done = true
	copy(dest, r.values)
	return nil
}

func (r *postgresBatchMultiRows) Columns() []string { return r.columns }
func (*postgresBatchMultiRows) Close() error        { return nil }
func (r *postgresBatchMultiRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

func (tx *postgresBatchTx) Rollback() error {
	tx.conn.state.mu.Lock()
	defer tx.conn.state.mu.Unlock()
	tx.conn.state.rollbackCount++
	return nil
}

func openPostgresBatchTestDB(t *testing.T, state *postgresBatchDriverState) *sql.DB {
	t.Helper()
	registerPostgresBatchDriver.Do(func() {
		sql.Register(postgresBatchDriverName, postgresBatchDriver{})
	})
	name := strings.ReplaceAll(t.Name(), "/", "-")
	postgresBatchStates.Store(name, state)
	t.Cleanup(func() { postgresBatchStates.Delete(name) })
	db, errOpen := sql.Open(postgresBatchDriverName, name)
	if errOpen != nil {
		t.Fatalf("open batch test DB: %v", errOpen)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newPostgresBatchTestStore(t *testing.T, state *postgresBatchDriverState) *PostgresStore {
	t.Helper()
	return &PostgresStore{
		db:      openPostgresBatchTestDB(t, state),
		cfg:     PostgresStoreConfig{AuthTable: defaultAuthTable},
		authDir: t.TempDir(),
	}
}

func TestPostgresStoreSaveBatchCommitsOnceAndNotifiesEachChangedID(t *testing.T) {
	state := &postgresBatchDriverState{upsertRows: 1}
	store := newPostgresBatchTestStore(t, state)
	authB := newBatchAuth("b.json", true)
	authA := newBatchAuth("a.json", false)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finalizeCalls := 0
	err := store.SaveBatch(ctx, []*cliproxyauth.Auth{authB, authA}, func(commit func() error) error {
		finalizeCalls++
		if _, errStat := os.Stat(filepath.Join(store.authDir, "a.json")); !errors.Is(errStat, os.ErrNotExist) {
			t.Fatalf("live mirror exists before commit: %v", errStat)
		}
		cancel()
		if errCommit := commit(); errCommit != nil {
			return errCommit
		}
		return commit()
	})
	if err != nil {
		t.Fatalf("SaveBatch() error: %v", err)
	}
	if finalizeCalls != 1 {
		t.Fatalf("finalize calls = %d, want 1", finalizeCalls)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.beginCanceled {
		t.Fatal("transaction inherited caller cancellation")
	}
	if state.commitCount != 1 {
		t.Fatalf("driver commit calls = %d, want 1", state.commitCount)
	}
	if state.rollbackCount != 0 {
		t.Fatalf("driver rollback calls = %d, want 0", state.rollbackCount)
	}
	if got := strings.Join(state.upsertIDs, ","); got != "a.json,b.json" {
		t.Fatalf("upsert order = %q, want a.json,b.json", got)
	}
	if state.notifyCount != 2 || state.notifyArgCount != 1 {
		t.Fatalf("notify count/args = %d/%d, want 2/1", state.notifyCount, state.notifyArgCount)
	}
	if got := strings.Join(state.notifyIDs, ","); got != "a.json,b.json" {
		t.Fatalf("notify IDs = %q, want a.json,b.json", got)
	}
	if state.markerInsertCount != 1 || state.markerID == "" {
		t.Fatalf("marker inserts/id = %d/%q, want one durable marker", state.markerInsertCount, state.markerID)
	}
	for _, query := range state.upsertQueries {
		if !strings.Contains(query, "ON CONFLICT (id) DO NOTHING") {
			t.Fatalf("batch insert lacks insert-only guard: %s", query)
		}
	}
	for _, name := range []string{"a.json", "b.json"} {
		if _, errStat := os.Stat(filepath.Join(store.authDir, name)); errStat != nil {
			t.Fatalf("committed mirror %s: %v", name, errStat)
		}
	}
	if authA.StoreGeneration() != 1 || authB.StoreGeneration() != 1 {
		t.Fatalf("committed generations = %d/%d, want 1/1", authA.StoreGeneration(), authB.StoreGeneration())
	}
	if _, leaked := authA.Attributes[postgresAuthGenerationPayloadKey]; leaked {
		t.Fatal("committed generation leaked into auth attributes")
	}
}

func TestPostgresStoreSaveBatchRollsBackWithoutCommit(t *testing.T) {
	state := &postgresBatchDriverState{upsertRows: 1}
	store := newPostgresBatchTestStore(t, state)
	livePath := filepath.Join(store.authDir, "a.json")
	if errWrite := os.WriteFile(livePath, []byte(`{"disabled":false,"old":true}`), 0o600); errWrite != nil {
		t.Fatalf("seed mirror: %v", errWrite)
	}

	auth := newBatchAuth("a.json", true)
	auth.SetStoreGeneration(4)
	err := store.SaveBatch(context.Background(), []*cliproxyauth.Auth{auth}, func(func() error) error {
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "without committing") {
		t.Fatalf("SaveBatch() error = %v, want missing commit error", err)
	}
	state.mu.Lock()
	rollbackCount := state.rollbackCount
	commitCount := state.commitCount
	state.mu.Unlock()
	if rollbackCount != 1 || commitCount != 0 {
		t.Fatalf("commit/rollback calls = %d/%d, want 0/1", commitCount, rollbackCount)
	}
	got, errRead := os.ReadFile(livePath)
	if errRead != nil {
		t.Fatalf("read mirror: %v", errRead)
	}
	if !strings.Contains(string(got), `"old":true`) {
		t.Fatalf("uncommitted mirror changed: %s", got)
	}
	if auth.StoreGeneration() != 4 {
		t.Fatalf("uncommitted generation = %d, want original 4", auth.StoreGeneration())
	}
}

func TestPostgresStoreSaveBatchKeepsCommittedStateWhenFinalizerReturnsError(t *testing.T) {
	state := &postgresBatchDriverState{upsertRows: 1}
	store := newPostgresBatchTestStore(t, state)
	finalizeErr := errors.New("late finalizer error")

	err := store.SaveBatch(context.Background(), []*cliproxyauth.Auth{newBatchAuth("a.json", false)}, func(commit func() error) error {
		if errCommit := commit(); errCommit != nil {
			return errCommit
		}
		return finalizeErr
	})
	if err != nil {
		t.Fatalf("SaveBatch() error after successful commit = %v, want nil", err)
	}
	state.mu.Lock()
	commitCount := state.commitCount
	rollbackCount := state.rollbackCount
	state.mu.Unlock()
	if commitCount != 1 || rollbackCount != 0 {
		t.Fatalf("commit/rollback calls = %d/%d, want 1/0", commitCount, rollbackCount)
	}
	if _, errStat := os.Stat(filepath.Join(store.authDir, "a.json")); errStat != nil {
		t.Fatalf("committed mirror not installed: %v", errStat)
	}
}

func TestPostgresStoreSaveVersionedSkipsNotifyForUnchangedContent(t *testing.T) {
	state := &postgresBatchDriverState{upsertRows: 0}
	store := newPostgresBatchTestStore(t, state)
	auth := newBatchAuth("same.json", false)
	auth.SetStoreGeneration(3)
	if _, _, err := store.SaveVersioned(context.Background(), auth, 3); err != nil {
		t.Fatalf("SaveVersioned() error: %v", err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.notifyCount != 0 {
		t.Fatalf("notify count = %d, want 0", state.notifyCount)
	}
	if len(state.upsertQueries) != 1 || !strings.Contains(state.upsertQueries[0], "IS DISTINCT FROM") {
		t.Fatalf("upsert query missing content guard: %q", state.upsertQueries)
	}
}

func TestPostgresStoreSaveRejectsStaleVersionWithoutNotify(t *testing.T) {
	state := &postgresBatchDriverState{upsertRows: 1, upsertConflictID: "a.json"}
	store := newPostgresBatchTestStore(t, state)
	livePath := filepath.Join(store.authDir, "a.json")
	oldContent := []byte(`{"type":"codex","disabled":false}`)
	if errWrite := os.WriteFile(livePath, oldContent, 0o600); errWrite != nil {
		t.Fatalf("seed live auth: %v", errWrite)
	}
	auth := newBatchAuth("a.json", true)
	auth.SetStoreGeneration(3)

	_, err := store.Save(context.Background(), auth)
	if !errors.Is(err, cliproxyauth.ErrAuthStoreConflict) {
		t.Fatalf("Save() error = %v, want version conflict", err)
	}
	state.mu.Lock()
	notifyCount := state.notifyCount
	state.mu.Unlock()
	if notifyCount != 0 {
		t.Fatalf("notify count = %d, want 0", notifyCount)
	}
	got, errRead := os.ReadFile(livePath)
	if errRead != nil {
		t.Fatalf("read live auth: %v", errRead)
	}
	if string(got) != string(oldContent) {
		t.Fatalf("stale Save changed live mirror: %s", got)
	}
}

func TestPostgresStoreSaveBatchRollsBackVersionConflictBeforeNotify(t *testing.T) {
	state := &postgresBatchDriverState{upsertRows: 1, upsertConflictID: "b.json"}
	store := newPostgresBatchTestStore(t, state)
	authA := newBatchAuth("a.json", true)
	authA.SetStoreGeneration(4)
	authB := newBatchAuth("b.json", true)
	authB.SetStoreGeneration(2)
	finalizeCalls := 0

	err := store.SaveBatch(context.Background(), []*cliproxyauth.Auth{authB, authA}, func(commit func() error) error {
		finalizeCalls++
		return commit()
	})
	if !errors.Is(err, cliproxyauth.ErrAuthStoreConflict) {
		t.Fatalf("SaveBatch() error = %v, want version conflict", err)
	}
	state.mu.Lock()
	rollbackCount := state.rollbackCount
	notifyCount := state.notifyCount
	markerInsertCount := state.markerInsertCount
	state.mu.Unlock()
	if finalizeCalls != 0 {
		t.Fatalf("finalize calls = %d, want 0", finalizeCalls)
	}
	if rollbackCount != 1 || notifyCount != 0 || markerInsertCount != 0 {
		t.Fatalf("rollback/notify/marker = %d/%d/%d, want 1/0/0", rollbackCount, notifyCount, markerInsertCount)
	}
	for _, name := range []string{"a.json", "b.json"} {
		if _, errStat := os.Stat(filepath.Join(store.authDir, name)); !errors.Is(errStat, os.ErrNotExist) {
			t.Fatalf("uncommitted mirror %s exists: %v", name, errStat)
		}
	}
}

func TestPostgresStoreSaveBatchCommitAckErrorUsesDurableMarker(t *testing.T) {
	ackErr := errors.New("commit acknowledgement lost")
	state := &postgresBatchDriverState{upsertRows: 1, commitErr: ackErr, commitApplied: true}
	store := newPostgresBatchTestStore(t, state)
	auth := newBatchAuth("a.json", true)
	finalizeCalls := 0

	err := store.SaveBatch(context.Background(), []*cliproxyauth.Auth{auth}, func(commit func() error) error {
		finalizeCalls++
		return commit()
	})
	if err != nil {
		t.Fatalf("SaveBatch() error = %v, want commit-wins success", err)
	}
	state.mu.Lock()
	commitCount := state.commitCount
	markerQueryCount := state.markerQueryCount
	markerDeleteCount := state.markerDeleteCount
	markerEventOrder := append([]string(nil), state.markerEventOrder...)
	state.mu.Unlock()
	if finalizeCalls != 1 || commitCount != 2 || markerQueryCount != 1 || markerDeleteCount != 1 {
		t.Fatalf("finalize/commit/marker query/delete = %d/%d/%d/%d, want 1/2/1/1", finalizeCalls, commitCount, markerQueryCount, markerDeleteCount)
	}
	if strings.Join(markerEventOrder, ",") != "select,delete" {
		t.Fatalf("marker event order = %v, want verification before delete", markerEventOrder)
	}
	if auth.StoreGeneration() != 1 {
		t.Fatalf("committed generation = %d, want 1", auth.StoreGeneration())
	}
	if _, errStat := os.Stat(filepath.Join(store.authDir, "a.json")); errStat != nil {
		t.Fatalf("commit-wins mirror not installed: %v", errStat)
	}
}

func TestPostgresStoreCommitAckVerificationFailureIsOutcomeUnknown(t *testing.T) {
	ackErr := errors.New("commit acknowledgement lost")
	verifyErr := errors.New("commit verification unavailable")
	tests := []struct {
		name  string
		state *postgresBatchDriverState
	}{
		{
			name: "begin",
			state: &postgresBatchDriverState{
				upsertRows:           1,
				commitErr:            ackErr,
				commitApplied:        true,
				verificationBeginErr: verifyErr,
			},
		},
		{
			name: "lock",
			state: &postgresBatchDriverState{
				upsertRows:          1,
				commitErr:           ackErr,
				commitApplied:       true,
				verificationLockErr: verifyErr,
			},
		},
		{
			name: "select",
			state: &postgresBatchDriverState{
				upsertRows:     1,
				commitErr:      ackErr,
				commitApplied:  true,
				markerQueryErr: verifyErr,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newPostgresBatchTestStore(t, tc.state)
			auth := newBatchAuth("unknown-save.json", false)
			path, generation, errSave := store.SaveVersioned(context.Background(), auth, 0)
			if !errors.Is(errSave, cliproxyauth.ErrAuthStoreCommitUnknown) {
				t.Fatalf("SaveVersioned() error = %v, want outcome unknown", errSave)
			}
			if path == "" || generation != 1 || auth.StoreGeneration() != 1 {
				t.Fatalf("SaveVersioned() candidate = (%q, %d, auth %d), want non-empty/1/1", path, generation, auth.StoreGeneration())
			}
			candidate, ok := cliproxyauth.AuthStoreCommitCandidateGeneration(errSave, auth.ID)
			if !ok || candidate != 1 {
				t.Fatalf("candidate generation = (%d, %v), want (1, true)", candidate, ok)
			}
			if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
				t.Fatalf("outcome-unknown save installed local mirror: %v", errStat)
			}
		})
	}
}

func TestPostgresStoreSingleMutationCommitUnknownRetainsCandidateGeneration(t *testing.T) {
	tests := []struct {
		name           string
		id             string
		expected       uint64
		upsertVersion  int64
		wantGeneration uint64
		run            func(*PostgresStore, *cliproxyauth.Auth, uint64) (uint64, error)
	}{
		{
			name:           "save",
			id:             "unknown-save-candidate.json",
			wantGeneration: 1,
			run: func(store *PostgresStore, auth *cliproxyauth.Auth, expected uint64) (uint64, error) {
				_, generation, errSave := store.SaveVersioned(context.Background(), auth, expected)
				return generation, errSave
			},
		},
		{
			name:           "tombstone",
			id:             "unknown-tombstone-candidate.json",
			expected:       5,
			wantGeneration: 6,
			run: func(store *PostgresStore, auth *cliproxyauth.Auth, expected uint64) (uint64, error) {
				return store.Tombstone(context.Background(), auth.ID, expected)
			},
		},
		{
			name:           "restore",
			id:             "unknown-restore-candidate.json",
			expected:       6,
			upsertVersion:  7,
			wantGeneration: 7,
			run: func(store *PostgresStore, auth *cliproxyauth.Auth, expected uint64) (uint64, error) {
				_, generation, errRestore := store.Restore(context.Background(), auth, expected)
				return generation, errRestore
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := &postgresBatchDriverState{
				upsertRows:     1,
				upsertVersion:  tc.upsertVersion,
				commitErr:      errors.New("commit acknowledgement lost"),
				commitApplied:  true,
				markerQueryErr: errors.New("marker verification unavailable"),
			}
			store := newPostgresBatchTestStore(t, state)
			auth := newBatchAuth(tc.id, false)
			auth.SetStoreGeneration(tc.expected)
			generation, errMutation := tc.run(store, auth, tc.expected)
			if !errors.Is(errMutation, cliproxyauth.ErrAuthStoreCommitUnknown) || generation != tc.wantGeneration {
				t.Fatalf("mutation = (%d, %v), want candidate %d and outcome unknown", generation, errMutation, tc.wantGeneration)
			}
			candidate, ok := cliproxyauth.AuthStoreCommitCandidateGeneration(errMutation, tc.id)
			if !ok || candidate != tc.wantGeneration {
				t.Fatalf("candidate helper = (%d, %v), want (%d, true)", candidate, ok, tc.wantGeneration)
			}
		})
	}
}

func TestPostgresStoreSaveBatchCommitUnknownPreservesCandidateGeneration(t *testing.T) {
	state := &postgresBatchDriverState{
		upsertRows:     1,
		commitErr:      errors.New("commit acknowledgement lost"),
		commitApplied:  true,
		markerQueryErr: errors.New("marker select unavailable"),
	}
	store := newPostgresBatchTestStore(t, state)
	auth := newBatchAuth("unknown-batch.json", false)
	errSave := store.SaveBatch(context.Background(), []*cliproxyauth.Auth{auth}, func(commit func() error) error {
		return commit()
	})
	if !errors.Is(errSave, cliproxyauth.ErrAuthStoreCommitUnknown) {
		t.Fatalf("SaveBatch() error = %v, want outcome unknown", errSave)
	}
	if auth.StoreGeneration() != 1 {
		t.Fatalf("SaveBatch() candidate generation = %d, want 1", auth.StoreGeneration())
	}
	if candidate, ok := cliproxyauth.AuthStoreCommitCandidateGeneration(errSave, auth.ID); !ok || candidate != 1 {
		t.Fatalf("batch candidate generation = (%d, %v), want (1, true)", candidate, ok)
	}
}

func TestPostgresStoreMarkerResultSurvivesVerifierCommitFailure(t *testing.T) {
	state := &postgresBatchDriverState{
		upsertRows:            1,
		commitErr:             errors.New("commit acknowledgement lost"),
		commitApplied:         true,
		verificationCommitErr: errors.New("verification commit acknowledgement lost"),
	}
	store := newPostgresBatchTestStore(t, state)
	auth := newBatchAuth("verified-save.json", false)
	if _, generation, errSave := store.SaveVersioned(context.Background(), auth, 0); errSave != nil || generation != 1 {
		t.Fatalf("SaveVersioned() = (%d, %v), want definitive committed generation 1", generation, errSave)
	}
}

func TestPostgresStoreCommitVerificationTimeoutIsBounded(t *testing.T) {
	originalTimeout := authCommitConfirmationTimeout
	authCommitConfirmationTimeout = 50 * time.Millisecond
	t.Cleanup(func() { authCommitConfirmationTimeout = originalTimeout })

	state := &postgresBatchDriverState{
		upsertRows:                1,
		commitErr:                 errors.New("commit acknowledgement lost"),
		commitApplied:             true,
		markerQueryWaitForContext: true,
	}
	store := newPostgresBatchTestStore(t, state)
	started := time.Now()
	_, generation, errSave := store.SaveVersioned(context.Background(), newBatchAuth("timeout.json", false), 0)
	if !errors.Is(errSave, cliproxyauth.ErrAuthStoreCommitUnknown) || generation != 1 {
		t.Fatalf("SaveVersioned() = (%d, %v), want candidate 1 and outcome unknown", generation, errSave)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("commit verification held store locks for %s, want bounded return", elapsed)
	}
}

func TestPostgresStoreBlockedDetachedCommitIsBoundedAndPreservesCandidate(t *testing.T) {
	originalTransactionTimeout := authDetachedTransactionTimeout
	authDetachedTransactionTimeout = 50 * time.Millisecond
	t.Cleanup(func() { authDetachedTransactionTimeout = originalTransactionTimeout })

	state := &postgresBatchDriverState{
		upsertRows:     1,
		blockStage:     "commit",
		blockStarted:   make(chan struct{}, 1),
		blockRelease:   make(chan struct{}),
		commitApplied:  true,
		markerQueryErr: errors.New("marker verification unavailable"),
	}
	store := newPostgresBatchTestStore(t, state)
	auth := newBatchAuth("blocked-commit.json", false)
	result := make(chan error, 1)
	go func() {
		result <- store.SaveBatch(context.Background(), []*cliproxyauth.Auth{auth}, func(commit func() error) error {
			return commit()
		})
	}()

	select {
	case <-state.blockStarted:
	case <-time.After(time.Second):
		t.Fatal("commit did not reach fake driver")
	}

	var errSave error
	select {
	case errSave = <-result:
	case <-time.After(time.Second):
		close(state.blockRelease)
		<-result
		t.Fatal("blocked commit exceeded detached transaction deadline")
	}
	if !errors.Is(errSave, cliproxyauth.ErrAuthStoreCommitUnknown) {
		t.Fatalf("SaveBatch() error = %v, want outcome unknown", errSave)
	}
	if candidate, ok := cliproxyauth.AuthStoreCommitCandidateGeneration(errSave, auth.ID); !ok || candidate != 1 {
		t.Fatalf("candidate generation = (%d, %v), want (1, true)", candidate, ok)
	}
	if auth.StoreGeneration() != 1 {
		t.Fatalf("auth generation = %d, want candidate 1", auth.StoreGeneration())
	}
	if _, errStat := os.Stat(filepath.Join(store.authDir, auth.ID)); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("outcome-unknown save installed local mirror: %v", errStat)
	}
	if !store.mu.TryLock() {
		t.Fatal("blocked commit retained PostgresStore.mu after returning")
	}
	store.mu.Unlock()

	state.mu.Lock()
	closeCount := state.closeCount
	closedIDs := append([]int(nil), state.closedConnectionIDs...)
	openCount := state.openCount
	state.mu.Unlock()
	if closeCount == 0 || len(closedIDs) == 0 || closedIDs[0] != 1 {
		t.Fatalf("closed physical connections = %v, want transaction connection 1 retired", closedIDs)
	}
	if openCount < 2 {
		t.Fatalf("physical connection opens = %d, want marker verification on a fresh connection", openCount)
	}
}

func TestPostgresStoreBlockedDetachedSingleCommitPreservesCandidate(t *testing.T) {
	originalTransactionTimeout := authDetachedTransactionTimeout
	authDetachedTransactionTimeout = 50 * time.Millisecond
	t.Cleanup(func() { authDetachedTransactionTimeout = originalTransactionTimeout })

	state := &postgresBatchDriverState{
		upsertRows:     1,
		blockStage:     "commit",
		blockStarted:   make(chan struct{}, 1),
		blockRelease:   make(chan struct{}),
		commitApplied:  true,
		markerQueryErr: errors.New("marker verification unavailable"),
	}
	store := newPostgresBatchTestStore(t, state)
	auth := newBatchAuth("blocked-single-commit.json", false)
	type saveResult struct {
		path       string
		generation uint64
		err        error
	}
	result := make(chan saveResult, 1)
	go func() {
		path, generation, errSave := store.SaveVersioned(context.Background(), auth, 0)
		result <- saveResult{path: path, generation: generation, err: errSave}
	}()

	select {
	case <-state.blockStarted:
	case <-time.After(time.Second):
		t.Fatal("single commit did not reach fake driver")
	}

	var saved saveResult
	select {
	case saved = <-result:
	case <-time.After(time.Second):
		close(state.blockRelease)
		<-result
		t.Fatal("blocked single commit exceeded detached transaction deadline")
	}
	if !errors.Is(saved.err, cliproxyauth.ErrAuthStoreCommitUnknown) || saved.path == "" || saved.generation != 1 {
		t.Fatalf("SaveVersioned() = (%q, %d, %v), want candidate generation 1 and outcome unknown", saved.path, saved.generation, saved.err)
	}
	if candidate, ok := cliproxyauth.AuthStoreCommitCandidateGeneration(saved.err, auth.ID); !ok || candidate != 1 {
		t.Fatalf("candidate generation = (%d, %v), want (1, true)", candidate, ok)
	}
	if auth.StoreGeneration() != 1 {
		t.Fatalf("auth generation = %d, want candidate 1", auth.StoreGeneration())
	}
	if _, errStat := os.Stat(saved.path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("outcome-unknown save installed local mirror: %v", errStat)
	}
	if !store.mu.TryLock() {
		t.Fatal("blocked single commit retained PostgresStore.mu after returning")
	}
	store.mu.Unlock()
}

func TestPostgresStoreBlockedDetachedTransactionSetupIsBounded(t *testing.T) {
	originalTransactionTimeout := authDetachedTransactionTimeout
	authDetachedTransactionTimeout = 50 * time.Millisecond
	t.Cleanup(func() { authDetachedTransactionTimeout = originalTransactionTimeout })

	for _, stage := range []string{"begin", "exec"} {
		t.Run(stage, func(t *testing.T) {
			state := &postgresBatchDriverState{
				upsertRows:   1,
				blockStage:   stage,
				blockStarted: make(chan struct{}, 1),
				blockRelease: make(chan struct{}),
			}
			store := newPostgresBatchTestStore(t, state)
			result := make(chan error, 1)
			go func() {
				result <- store.SaveBatch(context.Background(), []*cliproxyauth.Auth{newBatchAuth("blocked-"+stage+".json", false)}, func(commit func() error) error {
					return commit()
				})
			}()

			select {
			case <-state.blockStarted:
			case <-time.After(time.Second):
				t.Fatalf("%s did not reach fake driver", stage)
			}

			select {
			case errSave := <-result:
				if errSave == nil {
					t.Fatalf("blocked %s returned nil error", stage)
				}
			case <-time.After(time.Second):
				close(state.blockRelease)
				<-result
				t.Fatalf("blocked %s exceeded detached transaction deadline", stage)
			}
			if !store.mu.TryLock() {
				t.Fatalf("blocked %s retained PostgresStore.mu after returning", stage)
			}
			store.mu.Unlock()

			conn, errConn := store.db.Conn(context.Background())
			if errConn != nil {
				t.Fatalf("acquire connection after blocked %s: %v", stage, errConn)
			}
			var nextConnectionID int
			if errRaw := conn.Raw(func(rawConn any) error {
				nextConnectionID = rawConn.(*postgresBatchConn).id
				return nil
			}); errRaw != nil {
				t.Fatalf("inspect connection after blocked %s: %v", stage, errRaw)
			}
			if errClose := conn.Close(); errClose != nil {
				t.Fatalf("close connection after blocked %s: %v", stage, errClose)
			}
			if nextConnectionID == 1 {
				t.Fatalf("blocked %s transaction connection was returned to the pool", stage)
			}
		})
	}
}

func TestPostgresStoreSingleSaveCommitAckUsesSerializedMarker(t *testing.T) {
	ackErr := errors.New("commit acknowledgement lost")
	for _, tc := range []struct {
		name          string
		commitApplied bool
		wantErr       bool
		wantDelete    int
	}{
		{name: "committed", commitApplied: true, wantDelete: 1},
		{name: "rolled-back", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := &postgresBatchDriverState{
				upsertRows:    1,
				commitErr:     ackErr,
				commitApplied: tc.commitApplied,
			}
			store := newPostgresBatchTestStore(t, state)
			auth := newBatchAuth("single-save.json", false)
			path, generation, errSave := store.SaveVersioned(context.Background(), auth, 0)
			if tc.wantErr {
				if !errors.Is(errSave, ackErr) {
					t.Fatalf("SaveVersioned() error = %v, want commit error", errSave)
				}
				if path != "" || generation != 0 || auth.StoreGeneration() != 0 {
					t.Fatalf("rolled-back SaveVersioned() = (%q, %d, auth %d)", path, generation, auth.StoreGeneration())
				}
			} else {
				if errSave != nil || path == "" || generation != 1 || auth.StoreGeneration() != 1 {
					t.Fatalf("committed SaveVersioned() = (%q, %d, auth %d, %v)", path, generation, auth.StoreGeneration(), errSave)
				}
			}
			state.mu.Lock()
			commitCount := state.commitCount
			markerQueryCount := state.markerQueryCount
			markerDeleteCount := state.markerDeleteCount
			markerEventOrder := append([]string(nil), state.markerEventOrder...)
			lockCount := state.lifecycleLockCount
			state.mu.Unlock()
			if commitCount != 2 || markerQueryCount != 1 || lockCount != 2 {
				t.Fatalf("commit/marker/lock = %d/%d/%d, want 2/1/2", commitCount, markerQueryCount, lockCount)
			}
			if markerDeleteCount != tc.wantDelete {
				t.Fatalf("marker deletes = %d, want %d", markerDeleteCount, tc.wantDelete)
			}
			if tc.wantDelete == 1 && strings.Join(markerEventOrder, ",") != "select,delete" {
				t.Fatalf("marker event order = %v, want verification before delete", markerEventOrder)
			}
		})
	}
}

func TestPostgresStoreSaveBatchSerializedMarkerAbsenceConfirmsRollback(t *testing.T) {
	ackErr := errors.New("commit acknowledgement lost")
	state := &postgresBatchDriverState{
		upsertRows: 1,
		commitErr:  ackErr,
	}
	store := newPostgresBatchTestStore(t, state)
	auth := newBatchAuth("rolled-back-marker.json", true)

	err := store.SaveBatch(context.Background(), []*cliproxyauth.Auth{auth}, func(commit func() error) error {
		return commit()
	})
	if !errors.Is(err, ackErr) {
		t.Fatalf("SaveBatch() error = %v, want commit error after definitive marker absence", err)
	}
	state.mu.Lock()
	markerQueryCount := state.markerQueryCount
	markerDeleteCount := state.markerDeleteCount
	state.mu.Unlock()
	if markerQueryCount != 1 {
		t.Fatalf("marker query count = %d, want one serialized check", markerQueryCount)
	}
	if markerDeleteCount != 0 {
		t.Fatalf("rolled-back marker delete count = %d, want 0", markerDeleteCount)
	}
	if auth.StoreGeneration() != 0 {
		t.Fatalf("rolled-back generation = %d, want original 0", auth.StoreGeneration())
	}
	if _, errStat := os.Stat(filepath.Join(store.authDir, auth.ID)); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("rolled-back marker installed mirror: %v", errStat)
	}
}

func TestPostgresStoreBuildAuthFromRowUsesOnlyTrustedVersion(t *testing.T) {
	store := &PostgresStore{authDir: t.TempDir()}
	payload := `{"type":"codex","__cliproxy_internal_postgres_version":7}`
	auth, ok := store.buildAuthFromRow("a.json", payload, time.Time{}, time.Time{}, 7)
	if !ok {
		t.Fatal("buildAuthFromRow() returned ok=false")
	}
	if got := auth.StoreGeneration(); got != 7 {
		t.Fatalf("store generation = %d, want 7", got)
	}
	if _, leaked := auth.Metadata[postgresAuthGenerationPayloadKey]; leaked {
		t.Fatal("synthetic postgres version leaked into auth metadata")
	}
	if _, leaked := auth.Attributes[postgresAuthGenerationPayloadKey]; leaked {
		t.Fatal("synthetic postgres version leaked into auth attributes")
	}
	forged, ok := store.buildAuthFromRow("forged.json", payload, time.Time{}, time.Time{})
	if !ok {
		t.Fatal("buildAuthFromRow(forged) returned ok=false")
	}
	if got := forged.StoreGeneration(); got != 0 {
		t.Fatalf("untrusted payload generation = %d, want 0", got)
	}
}

func newBatchAuth(id string, disabled bool) *cliproxyauth.Auth {
	return &cliproxyauth.Auth{
		ID:       id,
		FileName: id,
		Disabled: disabled,
		Metadata: map[string]any{"type": "codex", "disabled": disabled},
	}
}

func assertNoPostgresAuthMutation(t *testing.T, state *postgresBatchDriverState) {
	t.Helper()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.openCount != 0 || state.beginCount != 0 || len(state.upsertIDs) != 0 || state.markerInsertCount != 0 || state.notifyCount != 0 {
		t.Fatalf("database activity = open:%d begin:%d upserts:%v markers:%d notify:%d, want none",
			state.openCount, state.beginCount, state.upsertIDs, state.markerInsertCount, state.notifyCount)
	}
}

func TestPostgresStoreSingleSaveRejectsNonCanonicalAuthIDBeforeDatabaseUse(t *testing.T) {
	tests := []struct {
		name string
		auth *cliproxyauth.Auth
	}{
		{
			name: "file name differs",
			auth: &cliproxyauth.Auth{
				ID:       "logical.json",
				FileName: "stored.json",
				Metadata: map[string]any{"type": "codex"},
			},
		},
		{
			name: "path differs",
			auth: &cliproxyauth.Auth{
				ID:       "logical.json",
				Metadata: map[string]any{"type": "codex"},
				Attributes: map[string]string{
					cliproxyauth.AttributePath: "stored.json",
				},
			},
		},
	}
	for _, test := range tests {
		for _, operation := range []struct {
			name string
			run  func(*PostgresStore, *cliproxyauth.Auth) error
		}{
			{name: "Save", run: func(store *PostgresStore, auth *cliproxyauth.Auth) error {
				_, errSave := store.Save(context.Background(), auth)
				return errSave
			}},
			{name: "SaveVersioned", run: func(store *PostgresStore, auth *cliproxyauth.Auth) error {
				_, _, errSave := store.SaveVersioned(context.Background(), auth, 0)
				return errSave
			}},
		} {
			operation := operation
			t.Run(test.name+"/"+operation.name, func(t *testing.T) {
				state := &postgresBatchDriverState{upsertRows: 1}
				store := newPostgresBatchTestStore(t, state)
				errSave := operation.run(store, test.auth.Clone())
				if errSave == nil || !strings.Contains(errSave.Error(), "canonical auth id") {
					t.Fatalf("%s() error = %v, want canonical auth id rejection", operation.name, errSave)
				}
				assertNoPostgresAuthMutation(t, state)
			})
		}
	}
}

func TestPostgresStoreRestoreRejectsNonCanonicalAuthIDBeforeDatabaseUse(t *testing.T) {
	tests := []struct {
		name string
		auth *cliproxyauth.Auth
	}{
		{name: "file name differs", auth: &cliproxyauth.Auth{
			ID: "logical.json", FileName: "restored.json", Metadata: map[string]any{"type": "codex"},
		}},
		{name: "path differs", auth: &cliproxyauth.Auth{
			ID: "logical.json", Metadata: map[string]any{"type": "codex"},
			Attributes: map[string]string{cliproxyauth.AttributePath: "restored.json"},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := &postgresBatchDriverState{upsertRows: 1}
			store := newPostgresBatchTestStore(t, state)
			if _, _, errRestore := store.Restore(context.Background(), test.auth, 0); errRestore == nil || !strings.Contains(errRestore.Error(), "canonical auth id") {
				t.Fatalf("Restore() error = %v, want canonical auth id rejection", errRestore)
			}
			assertNoPostgresAuthMutation(t, state)
		})
	}
}

func TestPostgresStoreSaveBatchRejectsAnyNonCanonicalAuthIDBeforeDatabaseUse(t *testing.T) {
	tests := []struct {
		name string
		auth *cliproxyauth.Auth
	}{
		{name: "file name differs", auth: &cliproxyauth.Auth{
			ID: "logical.json", FileName: "stored.json", Metadata: map[string]any{"type": "codex"},
		}},
		{name: "path differs", auth: &cliproxyauth.Auth{
			ID: "logical.json", Metadata: map[string]any{"type": "codex"},
			Attributes: map[string]string{cliproxyauth.AttributePath: "stored.json"},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := &postgresBatchDriverState{upsertRows: 1}
			store := newPostgresBatchTestStore(t, state)
			valid := newBatchAuth("valid.json", false)
			finalizeCalled := false
			errBatch := store.SaveBatch(context.Background(), []*cliproxyauth.Auth{valid, test.auth}, func(func() error) error {
				finalizeCalled = true
				return nil
			})
			if errBatch == nil || !strings.Contains(errBatch.Error(), "canonical auth id") {
				t.Fatalf("SaveBatch() error = %v, want canonical auth id rejection", errBatch)
			}
			if finalizeCalled {
				t.Fatal("SaveBatch() invoked finalize for a non-canonical batch")
			}
			assertNoPostgresAuthMutation(t, state)
		})
	}
}

func TestPostgresStoreCanonicalAuthIDAllowsNormalizedAndNestedPaths(t *testing.T) {
	tests := []struct {
		name     string
		authID   string
		fileName string
		wantRow  string
	}{
		{name: "equivalent normalized id", authID: "nested/../nested/token.json", fileName: "nested/token.json", wantRow: "nested/token.json"},
		{name: "equivalent normalized path", authID: "nested/token.json", fileName: "nested/./token.json", wantRow: "nested/token.json"},
		{name: "nested path", authID: "teams/red/token.json", fileName: "teams/red/token.json", wantRow: "teams/red/token.json"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := &postgresBatchDriverState{upsertRows: 1}
			store := newPostgresBatchTestStore(t, state)
			auth := &cliproxyauth.Auth{
				ID:       test.authID,
				FileName: test.fileName,
				Metadata: map[string]any{"type": "codex"},
			}
			if _, _, errSave := store.SaveVersioned(context.Background(), auth, 0); errSave != nil {
				t.Fatalf("SaveVersioned() error: %v", errSave)
			}
			state.mu.Lock()
			gotRows := append([]string(nil), state.upsertIDs...)
			state.mu.Unlock()
			if len(gotRows) != 1 || gotRows[0] != test.wantRow {
				t.Fatalf("database row IDs = %v, want [%s]", gotRows, test.wantRow)
			}
		})
	}
}

func TestPostgresStoreVersionedSaveNeverInsertsForMissingPositiveGeneration(t *testing.T) {
	state := &postgresBatchDriverState{upsertMissingID: "missing.json"}
	store := newPostgresBatchTestStore(t, state)
	auth := newBatchAuth("missing.json", false)
	auth.SetStoreGeneration(4)
	if _, _, err := store.SaveVersioned(context.Background(), auth, 4); !errors.Is(err, cliproxyauth.ErrAuthStoreConflict) {
		t.Fatalf("SaveVersioned(missing, expected=4) error = %v, want conflict", err)
	}
}

func TestPostgresStoreDisabledSaveDoesNotRequireExistingMirror(t *testing.T) {
	state := &postgresBatchDriverState{upsertRows: 1}
	store := newPostgresBatchTestStore(t, state)
	auth := newBatchAuth("disabled-no-mirror.json", true)
	if _, generation, err := store.SaveVersioned(context.Background(), auth, 0); err != nil || generation != 1 {
		t.Fatalf("SaveVersioned(disabled without mirror) = (%d, %v), want (1, nil)", generation, err)
	}
	if _, errStat := os.Stat(filepath.Join(store.authDir, auth.ID)); errStat != nil {
		t.Fatalf("committed disabled mirror missing: %v", errStat)
	}
}

func TestPostgresStorePersistAuthFilesAdvancesMirrorGenerationForConsecutiveWrites(t *testing.T) {
	state := &postgresBatchDriverState{upsertRows: 1, upsertVersion: 1}
	store := newPostgresBatchTestStore(t, state)
	path := filepath.Join(store.authDir, "watcher.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"one"}`), 0o600); errWrite != nil {
		t.Fatalf("write first mirror: %v", errWrite)
	}
	if err := store.PersistAuthFiles(context.Background(), "", path); err != nil {
		t.Fatalf("PersistAuthFiles(first) error: %v", err)
	}
	first := readMirrorGeneration(t, path)
	if first != 1 {
		t.Fatalf("first mirror generation = %d, want 1", first)
	}

	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"two","__cliproxy_internal_postgres_version":1}`), 0o600); errWrite != nil {
		t.Fatalf("write second mirror: %v", errWrite)
	}
	state.mu.Lock()
	state.upsertVersion = 2
	state.mu.Unlock()
	if err := store.PersistAuthFiles(context.Background(), "", path); err != nil {
		t.Fatalf("PersistAuthFiles(second) error: %v", err)
	}
	if second := readMirrorGeneration(t, path); second != 2 {
		t.Fatalf("second mirror generation = %d, want 2", second)
	}
}

func readMirrorGeneration(t *testing.T, path string) uint64 {
	t.Helper()
	raw, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read mirror: %v", errRead)
	}
	metadata := make(map[string]any)
	if errJSON := json.Unmarshal(raw, &metadata); errJSON != nil {
		t.Fatalf("decode mirror: %v", errJSON)
	}
	generation, ok := authPayloadGeneration(metadata[postgresAuthGenerationPayloadKey])
	if !ok {
		t.Fatalf("mirror generation missing/invalid: %s", raw)
	}
	return generation
}

func TestPostgresStoreNotifyFailureRollsBackSave(t *testing.T) {
	notifyErr := errors.New("notify unavailable")
	state := &postgresBatchDriverState{upsertRows: 1, notifyErr: notifyErr}
	store := newPostgresBatchTestStore(t, state)
	if _, _, err := store.SaveVersioned(context.Background(), newBatchAuth("notify.json", false), 0); !errors.Is(err, notifyErr) {
		t.Fatalf("SaveVersioned() error = %v, want notify error", err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.commitCount != 0 || state.rollbackCount != 1 {
		t.Fatalf("commit/rollback = %d/%d, want 0/1", state.commitCount, state.rollbackCount)
	}
}

func TestPostgresStoreSaveVersionedRejectsTombstone(t *testing.T) {
	for _, expected := range []uint64{0, 4} {
		t.Run(fmt.Sprintf("expected-%d", expected), func(t *testing.T) {
			state := &postgresBatchDriverState{
				upsertDeletedID: "deleted.json",
				upsertVersion:   5,
			}
			store := newPostgresBatchTestStore(t, state)
			auth := newBatchAuth("deleted.json", false)
			auth.SetStoreGeneration(expected)
			if _, _, err := store.SaveVersioned(context.Background(), auth, expected); !errors.Is(err, cliproxyauth.ErrAuthStoreDeleted) {
				t.Fatalf("SaveVersioned(expected=%d) error = %v, want deleted", expected, err)
			}
			state.mu.Lock()
			defer state.mu.Unlock()
			if state.commitCount != 0 || state.rollbackCount != 1 || state.notifyCount != 0 {
				t.Fatalf("commit/rollback/notify = %d/%d/%d, want 0/1/0", state.commitCount, state.rollbackCount, state.notifyCount)
			}
		})
	}
}

func TestPostgresStoreTombstoneGenerationSemantics(t *testing.T) {
	tests := []struct {
		name           string
		state          *postgresBatchDriverState
		expected       uint64
		wantGeneration uint64
		wantErr        error
		wantNotify     int
	}{
		{
			name:           "missing-row-creates-tombstone",
			state:          &postgresBatchDriverState{upsertRows: 1},
			expected:       0,
			wantGeneration: 1,
			wantNotify:     1,
		},
		{
			name:     "zero-cannot-delete-active",
			state:    &postgresBatchDriverState{upsertConflictID: "auth.json", upsertVersion: 4},
			expected: 0,
			wantErr:  cliproxyauth.ErrAuthStoreConflict,
		},
		{
			name:           "zero-renews-existing-tombstone",
			state:          &postgresBatchDriverState{upsertDeletedID: "auth.json", upsertVersion: 5},
			expected:       0,
			wantGeneration: 6,
			wantNotify:     1,
		},
		{
			name:           "positive-deletes-exact-active-generation",
			state:          &postgresBatchDriverState{upsertRows: 1},
			expected:       5,
			wantGeneration: 6,
			wantNotify:     1,
		},
		{
			name:           "positive-renews-existing-tombstone",
			state:          &postgresBatchDriverState{upsertDeletedID: "auth.json", upsertVersion: 6},
			expected:       5,
			wantGeneration: 7,
			wantNotify:     1,
		},
		{
			name:     "positive-missing-row-conflicts",
			state:    &postgresBatchDriverState{upsertMissingID: "auth.json"},
			expected: 5,
			wantErr:  cliproxyauth.ErrAuthStoreConflict,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := tc.state
			store := newPostgresBatchTestStore(t, state)
			generation, err := store.Tombstone(context.Background(), "auth.json", tc.expected)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Tombstone() error = %v, want %v", err, tc.wantErr)
			}
			if generation != tc.wantGeneration {
				t.Fatalf("Tombstone() generation = %d, want %d", generation, tc.wantGeneration)
			}
			state.mu.Lock()
			defer state.mu.Unlock()
			if state.notifyCount != tc.wantNotify {
				t.Fatalf("notify count = %d, want %d", state.notifyCount, tc.wantNotify)
			}
			if tc.wantErr == nil && state.commitCount != 1 {
				t.Fatalf("commit count = %d, want 1", state.commitCount)
			}
			if tc.wantNotify == 1 {
				if len(state.notifyIDs) != 1 || state.notifyIDs[0] != "auth.json" {
					t.Fatalf("notify IDs = %v, want [auth.json]", state.notifyIDs)
				}
				query := strings.Join(state.upsertQueries, "\n")
				if !strings.Contains(query, "content = '{}'::jsonb") && !strings.Contains(query, "VALUES ($1, '{}'::jsonb") {
					t.Fatalf("tombstone query does not clear content: %s", query)
				}
			}
		})
	}
}

func TestPostgresStoreRestoreCreatesOrReactivatesOnly(t *testing.T) {
	tests := []struct {
		name           string
		state          *postgresBatchDriverState
		expected       uint64
		wantGeneration uint64
		wantErr        error
	}{
		{
			name:           "create-missing",
			state:          &postgresBatchDriverState{upsertRows: 1},
			wantGeneration: 1,
		},
		{
			name:           "reactivate-tombstone",
			state:          &postgresBatchDriverState{upsertRows: 1, upsertVersion: 7},
			expected:       6,
			wantGeneration: 7,
		},
		{
			name:     "reject-active",
			state:    &postgresBatchDriverState{upsertConflictID: "restore.json", upsertVersion: 5},
			expected: 4,
			wantErr:  cliproxyauth.ErrAuthStoreConflict,
		},
		{
			name:     "reject-stale-tombstone-generation",
			state:    &postgresBatchDriverState{upsertDeletedID: "restore.json", upsertVersion: 8},
			expected: 6,
			wantErr:  cliproxyauth.ErrAuthStoreConflict,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := tc.state
			store := newPostgresBatchTestStore(t, state)
			auth := newBatchAuth("restore.json", false)
			path, generation, err := store.Restore(context.Background(), auth, tc.expected)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Restore() error = %v, want %v", err, tc.wantErr)
			}
			if generation != tc.wantGeneration {
				t.Fatalf("Restore() generation = %d, want %d", generation, tc.wantGeneration)
			}
			state.mu.Lock()
			query := strings.Join(state.upsertQueries, "\n")
			notifyCount := state.notifyCount
			commitCount := state.commitCount
			state.mu.Unlock()
			if tc.expected == 0 {
				if !strings.Contains(query, "ON CONFLICT (id) DO NOTHING") {
					t.Fatalf("create query is not insert-only: %s", query)
				}
			} else {
				if !strings.Contains(query, "deleted = TRUE AND version = $4") {
					t.Fatalf("Restore query lacks tombstone generation guard: %s", query)
				}
				if count := strings.Count(query, "version = version + 1"); count != 1 {
					t.Fatalf("Restore query assigns version %d times, want exactly once: %s", count, query)
				}
			}
			if tc.wantErr != nil {
				if notifyCount != 0 || commitCount != 0 {
					t.Fatalf("rejected Restore notify/commit = %d/%d, want 0/0", notifyCount, commitCount)
				}
				return
			}
			if path == "" || auth.StoreGeneration() != tc.wantGeneration {
				t.Fatalf("Restore path/generation = %q/%d, want non-empty/%d", path, auth.StoreGeneration(), tc.wantGeneration)
			}
			if notifyCount != 1 || commitCount != 1 {
				t.Fatalf("Restore notify/commit = %d/%d, want 1/1", notifyCount, commitCount)
			}
			if mirrorGeneration := readMirrorGeneration(t, path); mirrorGeneration != tc.wantGeneration {
				t.Fatalf("restored mirror generation = %d, want %d", mirrorGeneration, tc.wantGeneration)
			}
		})
	}
}

func TestPostgresStoreTombstoneAndRestoreNotifyFailureRollBack(t *testing.T) {
	notifyErr := errors.New("notify unavailable")
	tests := []struct {
		name string
		run  func(*PostgresStore) error
	}{
		{
			name: "tombstone",
			run: func(store *PostgresStore) error {
				_, err := store.Tombstone(context.Background(), "notify-delete.json", 0)
				return err
			},
		},
		{
			name: "restore",
			run: func(store *PostgresStore) error {
				_, _, err := store.Restore(context.Background(), newBatchAuth("notify-restore.json", false), 0)
				return err
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := &postgresBatchDriverState{upsertRows: 1, notifyErr: notifyErr}
			store := newPostgresBatchTestStore(t, state)
			if err := tc.run(store); !errors.Is(err, notifyErr) {
				t.Fatalf("operation error = %v, want notify error", err)
			}
			state.mu.Lock()
			defer state.mu.Unlock()
			if state.commitCount != 0 || state.rollbackCount != 1 {
				t.Fatalf("commit/rollback = %d/%d, want 0/1", state.commitCount, state.rollbackCount)
			}
		})
	}
}

func TestPostgresStoreDeleteKeepsMirrorWhenTombstoneRollsBack(t *testing.T) {
	notifyErr := errors.New("notify unavailable")
	state := &postgresBatchDriverState{upsertRows: 1, notifyErr: notifyErr}
	store := newPostgresBatchTestStore(t, state)
	store.rememberAuthGeneration("delete.json", 4)
	path := filepath.Join(store.authDir, "delete.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("seed mirror: %v", errWrite)
	}
	if err := store.Delete(context.Background(), path); !errors.Is(err, notifyErr) {
		t.Fatalf("Delete() error = %v, want notify error", err)
	}
	if _, errStat := os.Stat(path); errStat != nil {
		t.Fatalf("mirror removed before durable tombstone: %v", errStat)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.commitCount != 0 || state.rollbackCount != 1 {
		t.Fatalf("commit/rollback = %d/%d, want 0/1", state.commitCount, state.rollbackCount)
	}
}

func TestScrubAndRemoveAuthMirrorLeavesNoSecretsWhenUnlinkFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delete.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"secret"}`), 0o600); errWrite != nil {
		t.Fatalf("seed mirror: %v", errWrite)
	}
	unlinkErr := errors.New("unlink denied")
	if err := scrubAndRemoveAuthMirror(path, 8, func(string) error { return unlinkErr }); !errors.Is(err, unlinkErr) {
		t.Fatalf("scrubAndRemoveAuthMirror() error = %v, want unlink error", err)
	}
	raw, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read scrubbed mirror: %v", errRead)
	}
	if strings.Contains(string(raw), "secret") || strings.Contains(string(raw), "access_token") {
		t.Fatalf("scrubbed mirror retained credential material: %s", raw)
	}
	var metadata map[string]any
	if errJSON := json.Unmarshal(raw, &metadata); errJSON != nil {
		t.Fatalf("decode scrubbed mirror: %v", errJSON)
	}
	if len(metadata) != 1 || metadata[postgresAuthGenerationPayloadKey] != float64(8) {
		t.Fatalf("scrubbed mirror = %#v, want generation-only marker", metadata)
	}
}

func TestPostgresStoreReconcileAuthMirrorConvergesWriterState(t *testing.T) {
	tests := []struct {
		name            string
		deletedSequence []bool
		wantRemoved     bool
	}{
		{name: "remote-tombstone", deletedSequence: []bool{true}, wantRemoved: true},
		{name: "restored-before-locked-read", deletedSequence: []bool{false}},
		{name: "remote-active-update", deletedSequence: []bool{false}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := &postgresBatchDriverState{
				upsertVersion:            8,
				authStateDeletedSequence: append([]bool(nil), tc.deletedSequence...),
				authMirrorPayload:        `{"type":"codex","access_token":"writer-token"}`,
			}
			store := newPostgresBatchTestStore(t, state)
			path := filepath.Join(store.authDir, "peer.json")
			if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"peer-secret"}`), 0o600); errWrite != nil {
				t.Fatalf("seed peer mirror: %v", errWrite)
			}

			if errReconcile := store.ReconcileAuthMirror(context.Background(), "peer.json"); errReconcile != nil {
				t.Fatalf("ReconcileAuthMirror() error: %v", errReconcile)
			}
			_, errStat := os.Stat(path)
			if tc.wantRemoved {
				if !errors.Is(errStat, os.ErrNotExist) {
					t.Fatalf("tombstoned peer mirror remains: %v", errStat)
				}
				if generation, ok := store.knownAuthGeneration("peer.json"); !ok || generation != 8 {
					t.Fatalf("remembered tombstone generation = (%d, %v), want (8, true)", generation, ok)
				}
				return
			}
			if errStat != nil {
				t.Fatalf("active/restored mirror removed: %v", errStat)
			}
			raw, errRead := os.ReadFile(path)
			if errRead != nil || !strings.Contains(string(raw), "writer-token") || strings.Contains(string(raw), "peer-secret") {
				t.Fatalf("active/restored mirror did not converge to writer: %q, %v", raw, errRead)
			}
		})
	}
}

func TestPostgresStoreReconcileAuthMirrorSkipsIdenticalActiveMirror(t *testing.T) {
	const payload = `{"type":"codex","access_token":"same-token"}`
	state := &postgresBatchDriverState{upsertVersion: 8, authMirrorPayload: payload}
	store := newPostgresBatchTestStore(t, state)
	path := filepath.Join(store.authDir, "same.json")
	desired, errDesired := authMirrorContent([]byte(payload), 8)
	if errDesired != nil {
		t.Fatalf("authMirrorContent() error: %v", errDesired)
	}
	if errWrite := os.WriteFile(path, desired, 0o600); errWrite != nil {
		t.Fatalf("seed identical mirror: %v", errWrite)
	}
	fixedModTime := time.Unix(1_700_000_000, 0)
	if errTimes := os.Chtimes(path, fixedModTime, fixedModTime); errTimes != nil {
		t.Fatalf("set mirror mtime: %v", errTimes)
	}

	if errReconcile := store.ReconcileAuthMirror(context.Background(), "same.json"); errReconcile != nil {
		t.Fatalf("ReconcileAuthMirror() error: %v", errReconcile)
	}
	info, errStat := os.Stat(path)
	if errStat != nil {
		t.Fatalf("stat identical mirror: %v", errStat)
	}
	if !info.ModTime().Equal(fixedModTime) {
		t.Fatalf("identical mirror mtime changed from %s to %s", fixedModTime, info.ModTime())
	}
}

func TestPostgresStoreReconcileAuthMirrorsScrubsLocalOnlyStaleToken(t *testing.T) {
	state := &postgresBatchDriverState{upsertMissingID: "stale.json"}
	store := newPostgresBatchTestStore(t, state)
	path := filepath.Join(store.authDir, "stale.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"stale-token"}`), 0o600); errWrite != nil {
		t.Fatalf("seed stale mirror: %v", errWrite)
	}

	if errReconcile := store.ReconcileAuthMirrors(context.Background()); errReconcile != nil {
		t.Fatalf("ReconcileAuthMirrors() error: %v", errReconcile)
	}
	if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("local-only stale mirror remains after full reconcile: %v", errStat)
	}
}

func TestPostgresStoreMirrorInstallFailureKeepsPreviousWatcherGeneration(t *testing.T) {
	state := &postgresBatchDriverState{upsertRows: 1}
	store := newPostgresBatchTestStore(t, state)
	store.rememberAuthGeneration("blocked.json", 4)
	blockedPath := filepath.Join(store.authDir, "blocked.json")
	if errMkdir := os.Mkdir(blockedPath, 0o700); errMkdir != nil {
		t.Fatalf("create blocking mirror directory: %v", errMkdir)
	}
	if errWrite := os.WriteFile(filepath.Join(blockedPath, "keep"), []byte("x"), 0o600); errWrite != nil {
		t.Fatalf("seed non-empty blocking directory: %v", errWrite)
	}
	auth := newBatchAuth("blocked.json", false)
	auth.SetStoreGeneration(4)
	if _, generation, err := store.SaveVersioned(context.Background(), auth, 4); err != nil || generation != 5 {
		t.Fatalf("SaveVersioned() = (%d, %v), want committed generation 5", generation, err)
	}
	if generation, ok := store.knownAuthGeneration("blocked.json"); !ok || generation != 4 {
		t.Fatalf("watcher generation after failed mirror install = (%d, %v), want previous (4, true)", generation, ok)
	}
}

func TestPostgresStoreWatcherDeleteRequiresCapturedGeneration(t *testing.T) {
	t.Run("unknown-generation-does-not-delete", func(t *testing.T) {
		state := &postgresBatchDriverState{upsertRows: 1}
		store := newPostgresBatchTestStore(t, state)
		path := filepath.Join(store.authDir, "unknown.json")
		if err := store.PersistAuthFiles(context.Background(), "", path); !errors.Is(err, cliproxyauth.ErrAuthStoreConflict) {
			t.Fatalf("PersistAuthFiles() error = %v, want conflict", err)
		}
		state.mu.Lock()
		defer state.mu.Unlock()
		if state.beginCount != 0 || state.notifyCount != 0 {
			t.Fatalf("begin/notify = %d/%d, want 0/0", state.beginCount, state.notifyCount)
		}
	})

	t.Run("stale-generation-cannot-delete-restored-row", func(t *testing.T) {
		state := &postgresBatchDriverState{
			upsertConflictID: "stale.json",
			upsertVersion:    6,
		}
		store := newPostgresBatchTestStore(t, state)
		store.rememberAuthGeneration("stale.json", 4)
		path := filepath.Join(store.authDir, "stale.json")
		if err := store.PersistAuthFiles(context.Background(), "", path); !errors.Is(err, cliproxyauth.ErrAuthStoreConflict) {
			t.Fatalf("PersistAuthFiles() error = %v, want conflict", err)
		}
		state.mu.Lock()
		defer state.mu.Unlock()
		if len(state.upsertExpected) != 1 || state.upsertExpected[0] != 4 {
			t.Fatalf("watcher tombstone expected generations = %v, want [4]", state.upsertExpected)
		}
		if state.notifyCount != 0 || state.commitCount != 0 {
			t.Fatalf("notify/commit = %d/%d, want 0/0", state.notifyCount, state.commitCount)
		}
	})
}

func TestPostgresStoreWatcherIgnoresForgedPayloadGeneration(t *testing.T) {
	state := &postgresBatchDriverState{upsertRows: 1, upsertVersion: 4}
	store := newPostgresBatchTestStore(t, state)
	store.rememberAuthGeneration("forged.json", 3)
	path := filepath.Join(store.authDir, "forged.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"token","__cliproxy_internal_postgres_version":999}`), 0o600); errWrite != nil {
		t.Fatalf("write forged mirror: %v", errWrite)
	}
	if err := store.PersistAuthFiles(context.Background(), "", path); err != nil {
		t.Fatalf("PersistAuthFiles() error: %v", err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.upsertExpected) != 1 || state.upsertExpected[0] != 3 {
		t.Fatalf("watcher expected generations = %v, want [3]", state.upsertExpected)
	}
	if len(state.upsertPayloads) != 1 || strings.Contains(state.upsertPayloads[0], postgresAuthGenerationPayloadKey) {
		t.Fatalf("database payload retained forged generation: %v", state.upsertPayloads)
	}
}

func TestPostgresStoreAuthReadsFilterTombstones(t *testing.T) {
	state := &postgresBatchDriverState{}
	store := newPostgresBatchTestStore(t, state)
	if _, err := store.List(context.Background()); err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if _, err := store.GetByID(context.Background(), "deleted.json"); err != nil {
		t.Fatalf("GetByID() error: %v", err)
	}
	if err := store.syncAuthFromDatabase(context.Background()); err != nil {
		t.Fatalf("syncAuthFromDatabase() error: %v", err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.readQueries) != 3 {
		t.Fatalf("auth read query count = %d, want 3", len(state.readQueries))
	}
	for _, query := range state.readQueries {
		if !strings.Contains(query, "deleted = FALSE") {
			t.Fatalf("auth read does not filter tombstones: %s", query)
		}
	}
}

func TestPostgresStoreEnsureSchemaAddsAuthTombstoneColumns(t *testing.T) {
	state := &postgresBatchDriverState{allowSchema: true}
	store := newPostgresBatchTestStore(t, state)
	store.cfg.ConfigTable = defaultConfigTable
	if err := store.EnsureSchema(context.Background()); err != nil {
		t.Fatalf("EnsureSchema() error: %v", err)
	}
	state.mu.Lock()
	queries := strings.Join(state.execQueries, "\n")
	state.mu.Unlock()
	for _, clause := range []string{
		"auth_lifecycle_clock",
		"lifecycle_version BIGINT NOT NULL DEFAULT 0",
		"deleted BOOLEAN NOT NULL DEFAULT FALSE",
		"deleted_at TIMESTAMPTZ",
		"ADD COLUMN IF NOT EXISTS deleted BOOLEAN NOT NULL DEFAULT FALSE",
		"ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ",
		"GREATEST(value, COALESCE((SELECT MAX(lifecycle_version)",
		"CREATE TABLE IF NOT EXISTS cluster_membership_state",
		"id           SMALLINT PRIMARY KEY CHECK (id = 1)",
		"epoch        BIGINT NOT NULL DEFAULT 0 CHECK (epoch >= 0)",
		"fingerprint  BYTEA NOT NULL DEFAULT ''::bytea",
		"staleness_ms BIGINT NOT NULL DEFAULT 0 CHECK (staleness_ms >= 0)",
		"INSERT INTO cluster_membership_state (id)",
		"ON CONFLICT (id) DO NOTHING",
		"CREATE TABLE IF NOT EXISTS auth_dispatch_leases",
		"auth_id           TEXT PRIMARY KEY",
		"owner_instance_id UUID NOT NULL",
		"owner_epoch       BIGINT NOT NULL CHECK (owner_epoch > 0)",
		"lease_until       TIMESTAMPTZ NOT NULL",
	} {
		if !strings.Contains(queries, clause) {
			t.Fatalf("schema queries missing %q", clause)
		}
	}
}
