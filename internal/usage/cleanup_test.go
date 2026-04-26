package usage

import (
	"context"
	"testing"
	"time"
)

func TestCleanup_FollowerSkipsDelete(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()
	store := NewPGStore(db)
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	defer cleanupTestRows(ctx, db, "cleanup-follower")

	old := time.Now().UTC().Add(-30 * 24 * time.Hour)
	row := UsageEventRow{
		OccurredAt: old, NodeID: "cleanup-follower", APIKey: "k", Model: "m",
		Failed: false, DedupHash: DedupHash("k", "m", RequestDetail{Timestamp: old}),
	}
	if err := store.InsertEventsBatch(ctx, []UsageEventRow{row}); err != nil {
		t.Fatal(err)
	}

	c := &Cleanup{
		Store:        store,
		IsLeader:     func() bool { return false },
		EventTTLDays: 7,
	}
	c.runOnce(ctx)

	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM usage_events WHERE node_id='cleanup-follower'`,
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("follower should not delete, got %d remaining", n)
	}
}

func TestCleanup_LeaderDeletesPastTTL(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()
	store := NewPGStore(db)
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	defer cleanupTestRows(ctx, db, "cleanup-leader")

	old := time.Now().UTC().Add(-30 * 24 * time.Hour)
	fresh := time.Now().UTC()
	rows := []UsageEventRow{
		{OccurredAt: old, NodeID: "cleanup-leader", APIKey: "k", Model: "m",
			Failed: false, DedupHash: DedupHash("k", "m", RequestDetail{Timestamp: old})},
		{OccurredAt: fresh, NodeID: "cleanup-leader", APIKey: "k", Model: "m",
			Failed: false, DedupHash: DedupHash("k", "m", RequestDetail{Timestamp: fresh})},
	}
	if err := store.InsertEventsBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}

	c := &Cleanup{
		Store:        store,
		IsLeader:     func() bool { return true },
		EventTTLDays: 7,
	}
	c.runOnce(ctx)

	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM usage_events WHERE node_id='cleanup-leader'`,
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 fresh row remaining, got %d", n)
	}
}

func TestCleanup_NoLeaderGate_RunsByDefault(t *testing.T) {
	// When IsLeader is nil (single-instance mode), cleanup runs.
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()
	store := NewPGStore(db)
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	defer cleanupTestRows(ctx, db, "cleanup-singleton")

	old := time.Now().UTC().Add(-30 * 24 * time.Hour)
	row := UsageEventRow{
		OccurredAt: old, NodeID: "cleanup-singleton", APIKey: "k", Model: "m",
		Failed: false, DedupHash: DedupHash("k", "m", RequestDetail{Timestamp: old}),
	}
	if err := store.InsertEventsBatch(ctx, []UsageEventRow{row}); err != nil {
		t.Fatal(err)
	}

	c := &Cleanup{Store: store, EventTTLDays: 7}
	c.runOnce(ctx)

	var n int
	db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM usage_events WHERE node_id='cleanup-singleton'`,
	).Scan(&n)
	if n != 0 {
		t.Fatalf("singleton mode should clean up, got %d remaining", n)
	}
}

func TestCleanup_ZeroTTL_Skips(t *testing.T) {
	// EventTTLDays=0 means "don't run that branch" — defensive against
	// misconfigured retention.
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()
	store := NewPGStore(db)
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	defer cleanupTestRows(ctx, db, "cleanup-zero-ttl")

	old := time.Now().UTC().Add(-30 * 24 * time.Hour)
	row := UsageEventRow{
		OccurredAt: old, NodeID: "cleanup-zero-ttl", APIKey: "k", Model: "m",
		Failed: false, DedupHash: DedupHash("k", "m", RequestDetail{Timestamp: old}),
	}
	if err := store.InsertEventsBatch(ctx, []UsageEventRow{row}); err != nil {
		t.Fatal(err)
	}

	c := &Cleanup{Store: store, IsLeader: func() bool { return true }} // EventTTLDays=0
	c.runOnce(ctx)

	var n int
	db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM usage_events WHERE node_id='cleanup-zero-ttl'`,
	).Scan(&n)
	if n != 1 {
		t.Fatalf("zero TTL should skip, got %d remaining", n)
	}
}
