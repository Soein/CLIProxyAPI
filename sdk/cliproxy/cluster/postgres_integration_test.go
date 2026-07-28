package cluster

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/store"
)

const postgresIntegrationDSNEnv = "CLIPROXY_POSTGRES_TEST_DSN"

func TestPostgresIntegrationRefreshLockAndLeaderHandoff(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(postgresIntegrationDSNEnv))
	if dsn == "" {
		t.Skipf("set %s to run the real PostgreSQL cluster-lock test", postgresIntegrationDSNEnv)
	}
	db, errOpen := sql.Open("pgx", dsn)
	if errOpen != nil {
		t.Fatalf("sql.Open() error: %v", errOpen)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if errPing := db.PingContext(ctx); errPing != nil {
		t.Fatalf("PingContext() error: %v", errPing)
	}

	lockerA := NewPgAuthRefreshLocker(db)
	lockerB := NewPgAuthRefreshLocker(db)
	releaseA, acquiredA, errLock := lockerA.TryLock(ctx, "shared-auth")
	if errLock != nil || !acquiredA || releaseA == nil {
		t.Fatalf("first TryLock() = acquired %v, release %v, error %v", acquiredA, releaseA != nil, errLock)
	}
	if releaseB, acquiredB, errSecond := lockerB.TryLock(ctx, "shared-auth"); errSecond != nil || acquiredB || releaseB != nil {
		t.Fatalf("contended TryLock() = acquired %v, release %v, error %v; want false, nil, nil", acquiredB, releaseB != nil, errSecond)
	}
	releaseA()
	releaseB, acquiredB, errSecond := lockerB.TryLock(ctx, "shared-auth")
	if errSecond != nil || !acquiredB || releaseB == nil {
		t.Fatalf("TryLock() after release = acquired %v, release %v, error %v", acquiredB, releaseB != nil, errSecond)
	}
	releaseB()

	var losses atomic.Int32
	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelA()
	defer cancelB()
	electorA := New(Config{DB: db, NodeID: "integration-a", Interval: 20 * time.Millisecond, OnLoss: func() { losses.Add(1) }})
	electorB := New(Config{DB: db, NodeID: "integration-b", Interval: 20 * time.Millisecond, OnLoss: func() { losses.Add(1) }})
	doneA := make(chan error, 1)
	doneB := make(chan error, 1)
	go func() { doneA <- electorA.Run(ctxA) }()
	go func() { doneB <- electorB.Run(ctxB) }()
	waitForSingleIntegrationLeader(t, electorA, electorB)

	var follower *LeaderElector
	var followerDone <-chan error
	if electorA.IsLeader() {
		follower = electorB
		followerDone = doneB
		cancelA()
		waitForIntegrationRunExit(t, doneA)
	} else {
		follower = electorA
		followerDone = doneA
		cancelB()
		waitForIntegrationRunExit(t, doneB)
	}
	waitForIntegrationLeadership(t, follower)
	if losses.Load() != 0 {
		t.Fatalf("normal leader cancellation invoked OnLoss %d time(s), want 0", losses.Load())
	}
	if follower == electorA {
		cancelA()
	} else {
		cancelB()
	}
	waitForIntegrationRunExit(t, followerDone)
}

func TestPostgresIntegrationDispatchAuthorityFencing(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(postgresIntegrationDSNEnv))
	if dsn == "" {
		t.Skipf("set %s to run the real PostgreSQL dispatch-authority test", postgresIntegrationDSNEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db := newDispatchIntegrationDatabase(t, ctx, dsn)
	backendA := &pgDispatchLeaseBackend{db: db}
	backendB := &pgDispatchLeaseBackend{db: db}

	t.Run("concurrent authorities grant only one coordinator", func(t *testing.T) {
		resetDispatchIntegrationState(t, ctx, db, 1)
		ring := NewAuthRing("node-a")
		ring.RebuildAt(1, []RingMember{{NodeID: "node-a", Weight: 100}})
		config := PgDispatchAuthorityConfig{
			NodeID:        "node-a",
			Ring:          ring,
			RingStaleness: 30 * time.Second,
			AuthIDs:       func() []string { return []string{"shared-auth"} },
		}
		authorityA, errA := newPgDispatchAuthority(config, backendA, false)
		if errA != nil {
			t.Fatalf("new authority A: %v", errA)
		}
		authorityB, errB := newPgDispatchAuthority(config, backendB, false)
		if errB != nil {
			t.Fatalf("new authority B: %v", errB)
		}
		t.Cleanup(func() {
			_ = authorityB.Close(context.Background())
			_ = authorityA.Close(context.Background())
		})

		start := make(chan struct{})
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		for _, authority := range []*PgDispatchAuthority{authorityA, authorityB} {
			wg.Add(1)
			go func(authority *PgDispatchAuthority) {
				defer wg.Done()
				<-start
				errs <- authority.syncOnce(ctx)
			}(authority)
		}
		close(start)
		wg.Wait()
		close(errs)
		var reconciled, incomplete int
		for errSync := range errs {
			switch {
			case errSync == nil:
				reconciled++
			case errors.Is(errSync, errDispatchReconciliationIncomplete):
				incomplete++
			default:
				t.Fatalf("concurrent syncOnce() error: %v", errSync)
			}
		}
		if reconciled != 1 || incomplete != 1 {
			t.Fatalf("concurrent sync outcomes = reconciled %d, incomplete %d; want 1, 1", reconciled, incomplete)
		}

		granted := 0
		if _, ok := authorityA.Admit("shared-auth"); ok {
			granted++
		}
		if _, ok := authorityB.Admit("shared-auth"); ok {
			granted++
		}
		if granted != 1 {
			t.Fatalf("concurrent authorities admitted %d coordinators, want exactly 1", granted)
		}
	})

	t.Run("stale membership epoch leaves lease unchanged", func(t *testing.T) {
		resetDispatchIntegrationState(t, ctx, db, 3)
		instanceA := uuid.NewString()
		grant := acquireDispatchIntegrationLease(t, ctx, backendA, "stale-epoch-auth", instanceA, 3)
		before := loadDispatchIntegrationLease(t, ctx, db, "stale-epoch-auth")

		result, errAcquire := backendB.acquire(ctx, dispatchLeaseRequest{
			authIDs:         []string{"stale-epoch-auth"},
			nodeID:          "node-b",
			instanceID:      uuid.NewString(),
			membershipEpoch: 2,
			ttl:             15 * time.Second,
		})
		if errAcquire != nil {
			t.Fatalf("stale acquire: %v", errAcquire)
		}
		if result.currentEpoch != 3 || len(result.grants) != 0 {
			t.Fatalf("stale acquire result = epoch %d, grants %d; want 3, 0", result.currentEpoch, len(result.grants))
		}
		after := loadDispatchIntegrationLease(t, ctx, db, "stale-epoch-auth")
		if before.instanceID != after.instanceID ||
			before.membershipEpoch != after.membershipEpoch ||
			before.ownerEpoch != after.ownerEpoch ||
			!before.leaseUntil.Equal(after.leaseUntil) ||
			!before.updatedAt.Equal(after.updatedAt) {
			t.Fatalf("stale membership request changed lease:\n before=%+v\n after=%+v", before, after)
		}
		if grant.ownerEpoch != before.ownerEpoch {
			t.Fatalf("initial grant owner epoch = %d, row = %d", grant.ownerEpoch, before.ownerEpoch)
		}
	})

	t.Run("same instance migrates lease across membership epoch once", func(t *testing.T) {
		const (
			initialEpoch = int64(6)
			nextEpoch    = int64(7)
		)
		resetDispatchIntegrationState(t, ctx, db, initialEpoch)
		instanceA := uuid.NewString()
		instanceB := uuid.NewString()
		initialGrant := acquireDispatchIntegrationLease(t, ctx, backendA, "epoch-migration-auth", instanceA, initialEpoch)
		if _, errEpoch := db.ExecContext(ctx, `
			UPDATE cluster_membership_state
			SET epoch = $1, updated_at = clock_timestamp()
			WHERE id = 1
		`, nextEpoch); errEpoch != nil {
			t.Fatalf("advance membership singleton: %v", errEpoch)
		}

		migrated := acquireDispatchIntegrationLease(t, ctx, backendA, "epoch-migration-auth", instanceA, nextEpoch)
		if migrated.membershipEpoch != nextEpoch || migrated.ownerEpoch != initialGrant.ownerEpoch+1 {
			t.Fatalf("migrated grant = membership epoch %d owner epoch %d; want %d, %d", migrated.membershipEpoch, migrated.ownerEpoch, nextEpoch, initialGrant.ownerEpoch+1)
		}
		migratedRow := loadDispatchIntegrationLease(t, ctx, db, "epoch-migration-auth")
		if migratedRow.instanceID != instanceA || migratedRow.membershipEpoch != nextEpoch || migratedRow.ownerEpoch != migrated.ownerEpoch || !migratedRow.active {
			t.Fatalf("migrated lease row = %+v; want instance %s membership epoch %d owner epoch %d active", migratedRow, instanceA, nextEpoch, migrated.ownerEpoch)
		}

		blocked, errBlocked := backendB.acquire(ctx, dispatchLeaseRequest{
			authIDs:         []string{"epoch-migration-auth"},
			nodeID:          "node-b",
			instanceID:      instanceB,
			membershipEpoch: nextEpoch,
			ttl:             15 * time.Second,
		})
		if errBlocked != nil || blocked.currentEpoch != nextEpoch || len(blocked.grants) != 0 {
			t.Fatalf("different-instance acquire = epoch %d grants %d error %v; want %d, 0, nil", blocked.currentEpoch, len(blocked.grants), errBlocked, nextEpoch)
		}
		if afterBlocked := loadDispatchIntegrationLease(t, ctx, db, "epoch-migration-auth"); !sameDispatchIntegrationLeaseRow(afterBlocked, migratedRow) {
			t.Fatalf("different-instance acquire changed unexpired lease:\n before=%+v\n after=%+v", migratedRow, afterBlocked)
		}

		stale, errStale := backendA.acquire(ctx, dispatchLeaseRequest{
			authIDs:         []string{"epoch-migration-auth"},
			nodeID:          "node-a",
			instanceID:      instanceA,
			membershipEpoch: initialEpoch,
			ttl:             15 * time.Second,
		})
		if errStale != nil || stale.currentEpoch != nextEpoch || len(stale.grants) != 0 {
			t.Fatalf("stale same-instance acquire = epoch %d grants %d error %v; want %d, 0, nil", stale.currentEpoch, len(stale.grants), errStale, nextEpoch)
		}
		if afterStale := loadDispatchIntegrationLease(t, ctx, db, "epoch-migration-auth"); !sameDispatchIntegrationLeaseRow(afterStale, migratedRow) {
			t.Fatalf("stale membership acquire changed migrated lease:\n before=%+v\n after=%+v", migratedRow, afterStale)
		}
	})

	t.Run("expiry handoff rejects stale release", func(t *testing.T) {
		resetDispatchIntegrationState(t, ctx, db, 4)
		instanceA := uuid.NewString()
		instanceB := uuid.NewString()
		oldGrant := acquireDispatchIntegrationLease(t, ctx, backendA, "expiry-auth", instanceA, 4)
		blocked, errBlocked := backendB.acquire(ctx, dispatchLeaseRequest{
			authIDs:         []string{"expiry-auth"},
			nodeID:          "node-b",
			instanceID:      instanceB,
			membershipEpoch: 4,
			ttl:             15 * time.Second,
		})
		if errBlocked != nil || len(blocked.grants) != 0 {
			t.Fatalf("unexpired handoff = grants %d, error %v; want 0, nil", len(blocked.grants), errBlocked)
		}
		if _, errExpire := db.ExecContext(ctx, `
			UPDATE auth_dispatch_leases
			SET lease_until = clock_timestamp() - INTERVAL '1 second'
			WHERE auth_id = $1
		`, "expiry-auth"); errExpire != nil {
			t.Fatalf("expire lease: %v", errExpire)
		}
		newGrant := acquireDispatchIntegrationLease(t, ctx, backendB, "expiry-auth", instanceB, 4)
		if newGrant.ownerEpoch != oldGrant.ownerEpoch+1 {
			t.Fatalf("handoff owner epoch = %d, want %d", newGrant.ownerEpoch, oldGrant.ownerEpoch+1)
		}
		if errRelease := backendA.release(ctx, []dispatchLeaseKey{{
			authID: "expiry-auth", instanceID: instanceA, ownerEpoch: oldGrant.ownerEpoch,
		}}); errRelease != nil {
			t.Fatalf("stale conditional release: %v", errRelease)
		}
		row := loadDispatchIntegrationLease(t, ctx, db, "expiry-auth")
		if row.instanceID != instanceB || row.ownerEpoch != newGrant.ownerEpoch || !row.active {
			t.Fatalf("stale release truncated successor lease: %+v", row)
		}
	})

	t.Run("release enables immediate handoff", func(t *testing.T) {
		resetDispatchIntegrationState(t, ctx, db, 5)
		instanceA := uuid.NewString()
		instanceB := uuid.NewString()
		oldGrant := acquireDispatchIntegrationLease(t, ctx, backendA, "release-auth", instanceA, 5)
		if errRelease := backendA.release(ctx, []dispatchLeaseKey{{
			authID: "release-auth", instanceID: instanceA, ownerEpoch: oldGrant.ownerEpoch,
		}}); errRelease != nil {
			t.Fatalf("release current lease: %v", errRelease)
		}
		newGrant := acquireDispatchIntegrationLease(t, ctx, backendB, "release-auth", instanceB, 5)
		if newGrant.ownerEpoch != oldGrant.ownerEpoch+1 {
			t.Fatalf("released handoff owner epoch = %d, want %d", newGrant.ownerEpoch, oldGrant.ownerEpoch+1)
		}
	})
}

type dispatchIntegrationLeaseRow struct {
	instanceID      string
	membershipEpoch int64
	ownerEpoch      int64
	leaseUntil      time.Time
	updatedAt       time.Time
	active          bool
}

func sameDispatchIntegrationLeaseRow(first, second dispatchIntegrationLeaseRow) bool {
	return first.instanceID == second.instanceID &&
		first.membershipEpoch == second.membershipEpoch &&
		first.ownerEpoch == second.ownerEpoch &&
		first.leaseUntil.Equal(second.leaseUntil) &&
		first.updatedAt.Equal(second.updatedAt) &&
		first.active == second.active
}

func newDispatchIntegrationDatabase(t *testing.T, ctx context.Context, dsn string) *sql.DB {
	t.Helper()
	schema := fmt.Sprintf("cliproxy_dispatch_%d_%d", os.Getpid(), time.Now().UnixNano())
	postgresStore, errStore := store.NewPostgresStore(ctx, store.PostgresStoreConfig{
		DSN:      dsn,
		Schema:   schema,
		SpoolDir: t.TempDir(),
	})
	if errStore != nil {
		t.Fatalf("NewPostgresStore() error: %v", errStore)
	}
	if gotDSN := postgresStore.DSN(); gotDSN != dsn {
		_ = postgresStore.Close()
		t.Fatalf("PostgresStore.DSN() = %q, want original DSN %q", gotDSN, dsn)
	}
	t.Cleanup(func() {
		if errClose := postgresStore.Close(); errClose != nil {
			t.Logf("close dispatch integration store: %v", errClose)
		}
		cleanupDB, errOpen := sql.Open("pgx", dsn)
		if errOpen != nil {
			t.Logf("open dispatch integration cleanup connection: %v", errOpen)
			return
		}
		defer func() { _ = cleanupDB.Close() }()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, errDrop := cleanupDB.ExecContext(cleanupCtx, `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`); errDrop != nil {
			t.Logf("drop dispatch integration schema: %v", errDrop)
		}
	})
	if errSchema := postgresStore.EnsureSchema(ctx); errSchema != nil {
		t.Fatalf("EnsureSchema() error: %v", errSchema)
	}
	return postgresStore.DB()
}

func resetDispatchIntegrationState(t *testing.T, ctx context.Context, db *sql.DB, epoch int64) {
	t.Helper()
	if _, errDelete := db.ExecContext(ctx, "DELETE FROM auth_dispatch_leases"); errDelete != nil {
		t.Fatalf("clear dispatch leases: %v", errDelete)
	}
	if _, errEpoch := db.ExecContext(ctx, `
		UPDATE cluster_membership_state
		SET epoch = $1, fingerprint = ''::bytea, staleness_ms = 30000, updated_at = clock_timestamp()
		WHERE id = 1
	`, epoch); errEpoch != nil {
		t.Fatalf("set membership epoch: %v", errEpoch)
	}
}

func acquireDispatchIntegrationLease(t *testing.T, ctx context.Context, backend *pgDispatchLeaseBackend, authID, instanceID string, epoch int64) dispatchLeaseGrant {
	t.Helper()
	result, errAcquire := backend.acquire(ctx, dispatchLeaseRequest{
		authIDs:         []string{authID},
		nodeID:          "integration-node",
		instanceID:      instanceID,
		membershipEpoch: epoch,
		ttl:             15 * time.Second,
	})
	if errAcquire != nil {
		t.Fatalf("acquire %s: %v", authID, errAcquire)
	}
	if result.currentEpoch != epoch || len(result.grants) != 1 {
		t.Fatalf("acquire %s = epoch %d, grants %d; want %d, 1", authID, result.currentEpoch, len(result.grants), epoch)
	}
	return result.grants[0]
}

func loadDispatchIntegrationLease(t *testing.T, ctx context.Context, db *sql.DB, authID string) dispatchIntegrationLeaseRow {
	t.Helper()
	var row dispatchIntegrationLeaseRow
	if errScan := db.QueryRowContext(ctx, `
		SELECT owner_instance_id::text, membership_epoch, owner_epoch, lease_until, updated_at,
		       lease_until > clock_timestamp()
		FROM auth_dispatch_leases
		WHERE auth_id = $1
	`, authID).Scan(&row.instanceID, &row.membershipEpoch, &row.ownerEpoch, &row.leaseUntil, &row.updatedAt, &row.active); errScan != nil {
		t.Fatalf("load dispatch lease %s: %v", authID, errScan)
	}
	return row
}

func waitForSingleIntegrationLeader(t *testing.T, first, second *LeaderElector) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if first.IsLeader() != second.IsLeader() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("leader election did not converge: first=%v second=%v", first.IsLeader(), second.IsLeader())
		case <-ticker.C:
		}
	}
}

func waitForIntegrationLeadership(t *testing.T, elector *LeaderElector) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !elector.IsLeader() {
		select {
		case <-deadline.C:
			t.Fatal("follower did not acquire leader lock after the old session exited")
		case <-ticker.C:
		}
	}
}

func waitForIntegrationRunExit(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("LeaderElector.Run() did not exit after cancellation")
	}
}
