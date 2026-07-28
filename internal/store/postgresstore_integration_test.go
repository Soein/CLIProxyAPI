package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

const postgresIntegrationDSNEnv = "CLIPROXY_POSTGRES_TEST_DSN"

// TestPostgresStoreIntegrationLifecycle exercises the HA lifecycle SQL against
// a real PostgreSQL server. It is opt-in so normal unit tests remain hermetic.
func TestPostgresStoreIntegrationLifecycle(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(postgresIntegrationDSNEnv))
	if dsn == "" {
		t.Skipf("set %s to run the real PostgreSQL lifecycle test", postgresIntegrationDSNEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	schema := fmt.Sprintf("cliproxy_ha_%d_%d", os.Getpid(), time.Now().UnixNano())
	storeA := newPostgresIntegrationStore(t, ctx, dsn, schema, "node-a")
	storeB := newPostgresIntegrationStore(t, ctx, dsn, schema, "node-b")
	t.Cleanup(func() {
		_ = storeB.Close()
		_ = storeA.Close()
		cleanupDB, errOpen := sql.Open("pgx", dsn)
		if errOpen != nil {
			t.Logf("open PostgreSQL cleanup connection: %v", errOpen)
			return
		}
		defer func() { _ = cleanupDB.Close() }()
		if _, errDrop := cleanupDB.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+quoteIdentifier(schema)+" CASCADE"); errDrop != nil {
			t.Logf("drop PostgreSQL integration schema: %v", errDrop)
		}
	})

	if errSchema := storeA.EnsureSchema(ctx); errSchema != nil {
		t.Fatalf("store A EnsureSchema() error: %v", errSchema)
	}
	if errSchema := storeB.EnsureSchema(ctx); errSchema != nil {
		t.Fatalf("store B EnsureSchema() error: %v", errSchema)
	}

	const authID = "shared.json"
	created := integrationAuth(authID, "old-token")
	_, generation1, errCreate := storeA.Restore(ctx, created, 0)
	if errCreate != nil || generation1 != 1 {
		t.Fatalf("Restore(create) = generation %d, error %v; want 1, nil", generation1, errCreate)
	}
	if errMirror := storeB.ReconcileAuthMirror(ctx, authID); errMirror != nil {
		t.Fatalf("store B initial ReconcileAuthMirror() error: %v", errMirror)
	}
	assertIntegrationMirrorToken(t, filepath.Join(storeB.AuthDir(), authID), "old-token")

	oldOperationCtx, errFence := cliproxyauth.BeginExplicitAuthOperation(ctx, storeB)
	if errFence != nil {
		t.Fatalf("BeginExplicitAuthOperation() error: %v", errFence)
	}
	generation2, errDelete := storeA.Tombstone(ctx, authID, generation1)
	if errDelete != nil || generation2 != 2 {
		t.Fatalf("Tombstone() = generation %d, error %v; want 2, nil", generation2, errDelete)
	}
	if _, errStaleRestore := cliproxyauth.PersistExplicitAuth(oldOperationCtx, storeB, integrationAuth(authID, "stale-token")); !errors.Is(errStaleRestore, cliproxyauth.ErrAuthStoreDeleted) {
		t.Fatalf("old explicit operation error = %v, want ErrAuthStoreDeleted", errStaleRestore)
	}
	if errMirror := storeB.ReconcileAuthMirror(ctx, authID); errMirror != nil {
		t.Fatalf("store B tombstone ReconcileAuthMirror() error: %v", errMirror)
	}
	if _, errStat := os.Stat(filepath.Join(storeB.AuthDir(), authID)); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("peer mirror stat error = %v, want not-exist after tombstone", errStat)
	}

	freshOperationCtx, errFence := cliproxyauth.BeginExplicitAuthOperation(ctx, storeB)
	if errFence != nil {
		t.Fatalf("fresh BeginExplicitAuthOperation() error: %v", errFence)
	}
	restored := integrationAuth(authID, "new-token")
	if _, errRestore := cliproxyauth.PersistExplicitAuth(freshOperationCtx, storeB, restored); errRestore != nil {
		t.Fatalf("fresh PersistExplicitAuth() error: %v", errRestore)
	}
	if restored.StoreGeneration() != 3 {
		t.Fatalf("restored generation = %d, want 3", restored.StoreGeneration())
	}
	if errMirror := storeA.ReconcileAuthMirror(ctx, authID); errMirror != nil {
		t.Fatalf("store A active ReconcileAuthMirror() error: %v", errMirror)
	}
	assertIntegrationMirrorToken(t, filepath.Join(storeA.AuthDir(), authID), "new-token")

	stale := integrationAuth(authID, "must-not-win")
	stale.SetStoreGeneration(generation1)
	if _, _, errSave := storeA.SaveVersioned(ctx, stale, generation1); !errors.Is(errSave, cliproxyauth.ErrAuthStoreConflict) {
		t.Fatalf("stale SaveVersioned() error = %v, want ErrAuthStoreConflict", errSave)
	}

	generation4, errDelete := storeA.Tombstone(ctx, authID, restored.StoreGeneration())
	if errDelete != nil || generation4 != 4 {
		t.Fatalf("second Tombstone() = generation %d, error %v; want 4, nil", generation4, errDelete)
	}
	generation5, errRenew := storeB.Tombstone(ctx, authID, generation4)
	if errRenew != nil || generation5 != 5 {
		t.Fatalf("repeated Tombstone() = generation %d, error %v; want 5, nil", generation5, errRenew)
	}
	state, errState := storeA.GetAuthLifecycle(ctx, authID)
	if errState != nil {
		t.Fatalf("GetAuthLifecycle() error: %v", errState)
	}
	if !state.Exists || !state.Deleted || state.Generation != generation5 || state.LifecycleVersion == 0 {
		t.Fatalf("final lifecycle state = %#v, want tombstone generation %d", state, generation5)
	}

	var markers int
	markerQuery := "SELECT COUNT(*) FROM " + storeA.fullTableName(defaultAuthBatchCommitTable)
	if errCount := storeA.db.QueryRowContext(ctx, markerQuery).Scan(&markers); errCount != nil {
		t.Fatalf("count commit markers: %v", errCount)
	}
	if markers != 0 {
		t.Fatalf("confirmed commit markers retained = %d, want 0", markers)
	}
}

func TestPostgresStoreIntegrationSchemaIsolation(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(postgresIntegrationDSNEnv))
	if dsn == "" {
		t.Skipf("set %s to run the real PostgreSQL schema-isolation test", postgresIntegrationDSNEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	suffix := fmt.Sprintf("%d_%d", os.Getpid(), time.Now().UnixNano())
	schemaA := "cliproxy_isolation_a_" + suffix
	schemaB := "cliproxy_isolation_b_" + suffix
	storeA := newPostgresIntegrationStore(t, ctx, dsn, schemaA, "node-a")
	storeB := newPostgresIntegrationStore(t, ctx, dsn, schemaB, "node-b")
	t.Cleanup(func() {
		_ = storeB.Close()
		_ = storeA.Close()
		cleanupDB, errOpen := sql.Open("pgx", dsn)
		if errOpen != nil {
			t.Logf("open PostgreSQL cleanup connection: %v", errOpen)
			return
		}
		defer func() { _ = cleanupDB.Close() }()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, schema := range []string{schemaA, schemaB} {
			if _, errDrop := cleanupDB.ExecContext(cleanupCtx, "DROP SCHEMA IF EXISTS "+quoteIdentifier(schema)+" CASCADE"); errDrop != nil {
				t.Logf("drop PostgreSQL integration schema %s: %v", schema, errDrop)
			}
		}
	})

	for name, integrationStore := range map[string]*PostgresStore{"A": storeA, "B": storeB} {
		if errSchema := integrationStore.EnsureSchema(ctx); errSchema != nil {
			t.Fatalf("store %s EnsureSchema() error: %v", name, errSchema)
		}
	}
	assertPostgresIntegrationSearchPath(t, ctx, storeA.DB(), schemaA)
	assertPostgresIntegrationSearchPath(t, ctx, storeB.DB(), schemaB)
	assertPostgresIntegrationTables(t, ctx, storeA.DB(), schemaA)
	assertPostgresIntegrationTables(t, ctx, storeB.DB(), schemaB)

	if _, errEpoch := storeA.DB().ExecContext(ctx, "UPDATE cluster_membership_state SET epoch = 11 WHERE id = 1"); errEpoch != nil {
		t.Fatalf("set store A membership epoch: %v", errEpoch)
	}
	if _, errEpoch := storeB.DB().ExecContext(ctx, "UPDATE cluster_membership_state SET epoch = 22 WHERE id = 1"); errEpoch != nil {
		t.Fatalf("set store B membership epoch: %v", errEpoch)
	}
	assertPostgresIntegrationEpoch(t, ctx, storeA.DB(), 11)
	assertPostgresIntegrationEpoch(t, ctx, storeB.DB(), 22)

	const insertLease = `
		INSERT INTO auth_dispatch_leases
			(auth_id, owner_node_id, owner_instance_id, membership_epoch, owner_epoch, lease_until)
		VALUES ($1, $2, $3, $4, 1, clock_timestamp() + INTERVAL '1 minute')
	`
	if _, errLease := storeA.DB().ExecContext(ctx, insertLease, "shared-auth", "node-a", "00000000-0000-0000-0000-000000000001", 11); errLease != nil {
		t.Fatalf("insert store A dispatch lease: %v", errLease)
	}
	if _, errLease := storeB.DB().ExecContext(ctx, insertLease, "shared-auth", "node-b", "00000000-0000-0000-0000-000000000002", 22); errLease != nil {
		t.Fatalf("insert store B dispatch lease: %v", errLease)
	}
	assertPostgresIntegrationLease(t, ctx, storeA.DB(), "node-a", 11)
	assertPostgresIntegrationLease(t, ctx, storeB.DB(), "node-b", 22)
}

func assertPostgresIntegrationSearchPath(t *testing.T, ctx context.Context, db *sql.DB, schema string) {
	t.Helper()
	connections := make([]*sql.Conn, 0, 3)
	defer func() {
		for _, connection := range connections {
			_ = connection.Close()
		}
	}()
	for index := 0; index < 3; index++ {
		connection, errConn := db.Conn(ctx)
		if errConn != nil {
			t.Fatalf("open physical connection %d for %s: %v", index, schema, errConn)
		}
		connections = append(connections, connection)
		var searchPath string
		if errShow := connection.QueryRowContext(ctx, "SHOW search_path").Scan(&searchPath); errShow != nil {
			t.Fatalf("show search_path on physical connection %d for %s: %v", index, schema, errShow)
		}
		if want := quoteIdentifier(schema); searchPath != want {
			t.Fatalf("search_path on physical connection %d for %s = %q, want %q", index, schema, searchPath, want)
		}
	}
}

func assertPostgresIntegrationTables(t *testing.T, ctx context.Context, db *sql.DB, schema string) {
	t.Helper()
	var count int
	if errCount := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = $1
		  AND table_name IN ('cluster_membership_state', 'auth_dispatch_leases', 'usage_events')
	`, schema).Scan(&count); errCount != nil {
		t.Fatalf("count isolated tables for %s: %v", schema, errCount)
	}
	if count != 3 {
		t.Fatalf("isolated cluster/usage tables in %s = %d, want 3", schema, count)
	}
}

func assertPostgresIntegrationEpoch(t *testing.T, ctx context.Context, db *sql.DB, want int64) {
	t.Helper()
	var epoch int64
	if errEpoch := db.QueryRowContext(ctx, "SELECT epoch FROM cluster_membership_state WHERE id = 1").Scan(&epoch); errEpoch != nil {
		t.Fatalf("load membership epoch: %v", errEpoch)
	}
	if epoch != want {
		t.Fatalf("membership epoch = %d, want %d", epoch, want)
	}
}

func assertPostgresIntegrationLease(t *testing.T, ctx context.Context, db *sql.DB, wantNode string, wantEpoch int64) {
	t.Helper()
	var nodeID string
	var membershipEpoch int64
	if errLease := db.QueryRowContext(ctx, `
		SELECT owner_node_id, membership_epoch
		FROM auth_dispatch_leases
		WHERE auth_id = 'shared-auth'
	`).Scan(&nodeID, &membershipEpoch); errLease != nil {
		t.Fatalf("load isolated dispatch lease: %v", errLease)
	}
	if nodeID != wantNode || membershipEpoch != wantEpoch {
		t.Fatalf("dispatch lease = node %q epoch %d, want node %q epoch %d", nodeID, membershipEpoch, wantNode, wantEpoch)
	}
}

func newPostgresIntegrationStore(t *testing.T, ctx context.Context, dsn, schema, nodeID string) *PostgresStore {
	t.Helper()
	store, errStore := NewPostgresStore(ctx, PostgresStoreConfig{
		DSN:      dsn,
		Schema:   schema,
		SpoolDir: t.TempDir(),
	})
	if errStore != nil {
		t.Fatalf("NewPostgresStore(%s) error: %v", nodeID, errStore)
	}
	store.SetNodeID(nodeID)
	return store
}

func integrationAuth(id, token string) *cliproxyauth.Auth {
	return &cliproxyauth.Auth{
		ID:       id,
		FileName: id,
		Provider: "xai",
		Metadata: map[string]any{
			"type":         "xai",
			"access_token": token,
		},
	}
}

func assertIntegrationMirrorToken(t *testing.T, path, want string) {
	t.Helper()
	content, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read mirror %s: %v", path, errRead)
	}
	if !strings.Contains(string(content), want) {
		t.Fatalf("mirror %s does not contain expected token", path)
	}
}
