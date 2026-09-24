package auth

import (
	"context"
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestManagerLoadSyncsSchedulerWithoutStructuralVersionChange(t *testing.T) {
	ctx := context.Background()
	store := &schedulerLoadStore{auths: []*Auth{{ID: "load-first", Provider: "gemini"}}}
	manager := NewManager(store, nil, nil)
	if err := manager.Load(ctx); err != nil {
		t.Fatal(err)
	}
	structuralEpoch := manager.structuralEpoch.Load()
	if manager.currentVersion() != manager.syncedVersion.Load() {
		t.Fatal("initial load did not synchronize scheduler version")
	}

	store.auths = []*Auth{{ID: "load-second", Provider: "gemini"}}
	if err := manager.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if manager.structuralEpoch.Load() != structuralEpoch {
		t.Fatal("load unexpectedly changed structural version")
	}
	picked, err := manager.scheduler.pickSingle(ctx, "gemini", "", cliproxyexecutor.Options{}, nil)
	if err != nil || picked == nil || picked.ID != "load-second" {
		t.Fatalf("scheduler did not publish reloaded auth: picked=%v, err=%v", picked, err)
	}
}

func TestManagerSyncSchedulerRepairsSameVersionIndexDrift(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil, nil, nil)
	if _, err := manager.Register(WithSkipPersist(ctx), &Auth{ID: "sync-repair", Provider: "gemini"}); err != nil {
		t.Fatal(err)
	}
	manager.SyncScheduler()
	if manager.currentVersion() != manager.syncedVersion.Load() {
		t.Fatal("initial sync did not synchronize scheduler version")
	}

	manager.scheduler.mu.Lock()
	delete(manager.scheduler.providers, "gemini")
	delete(manager.scheduler.authProviders, "sync-repair")
	manager.scheduler.mu.Unlock()
	manager.SyncScheduler()
	picked, err := manager.scheduler.pickSingle(ctx, "gemini", "", cliproxyexecutor.Options{}, nil)
	if err != nil || picked == nil || picked.ID != "sync-repair" {
		t.Fatalf("explicit sync did not repair scheduler index: picked=%v, err=%v", picked, err)
	}
}
