package auth

import (
	"context"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestSchedulerResultPreservesRevisionTombstones(t *testing.T) {
	for _, statusOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "removal", true: "status-disabled"}[statusOnly], func(t *testing.T) {
			scheduler := newAuthScheduler(&RoundRobinSelector{})
			active := &Auth{ID: "result-tombstone", Provider: "gemini", Status: StatusActive, revision: 4}
			scheduler.upsertAuth(active)
			if statusOnly {
				disabled := active.Clone()
				disabled.Status = StatusDisabled
				disabled.revision = 5
				scheduler.upsertAuthResult(disabled, nil, true)
			} else {
				scheduler.removeAuthAtRevision(active.ID, 5)
			}

			for _, revision := range []uint64{4, 5} {
				stale := active.Clone()
				stale.revision = revision
				scheduler.upsertAuthResult(stale, nil, false)
				if picked, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil); errPick == nil || picked != nil {
					t.Fatalf("result revision %d resurrected tombstoned auth: (%v, %v)", revision, picked, errPick)
				}
			}

			newer := active.Clone()
			newer.revision = 6
			scheduler.upsertAuthResult(newer, nil, false)
			if picked, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil); errPick != nil || picked == nil {
				t.Fatalf("newer result failed to restore auth: (%v, %v)", picked, errPick)
			}
		})
	}
}

func TestSchedulerResultStaleRevisionUpdatesTargetWithLatestSnapshot(t *testing.T) {
	const authID, provider = "result-revision-order", "gemini"
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(authID, provider, []*registry.ModelInfo{{ID: "model-a"}, {ID: "model-b"}})
	t.Cleanup(func() { reg.UnregisterClient(authID) })
	scheduler := newAuthScheduler(&RoundRobinSelector{})
	active := &Auth{ID: authID, Provider: provider, Status: StatusActive, revision: 1}
	scheduler.upsertAuth(active)
	for _, model := range []string{"model-a", "model-b"} {
		if _, errPick := scheduler.pickSingle(context.Background(), provider, model, cliproxyexecutor.Options{}, nil); errPick != nil {
			t.Fatal(errPick)
		}
	}

	latest := active.Clone()
	latest.revision = 3
	latest.ModelStates = map[string]*ModelState{
		"model-a": {Unavailable: true, Status: StatusError, NextRetryAfter: time.Now().Add(time.Hour)},
	}
	scheduler.upsertAuthResult(latest, []string{"model-b"}, false)
	if scheduler.providers[provider].modelShards["model-a"].entries[authID].state != scheduledStateReady {
		t.Fatal("unrelated model-a shard was updated before its result arrived")
	}

	stale := active.Clone()
	stale.revision = 2
	scheduler.upsertAuthResult(stale, []string{"model-a"}, false)
	entry := scheduler.providers[provider].modelShards["model-a"].entries[authID]
	if entry.auth != latest || entry.state == scheduledStateReady {
		t.Fatalf("stale revision did not synchronize model-a from latest snapshot: %+v", entry)
	}
	if scheduler.authVersions[authID].revision != latest.revision {
		t.Fatal("stale result regressed revision watermark")
	}
}

func TestSchedulerBatchRefreshesRegistryEpochForResultUpdates(t *testing.T) {
	const authID, provider = "result-batch-registry", "gemini"
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(authID, provider, []*registry.ModelInfo{{ID: "old-model"}})
	t.Cleanup(func() { reg.UnregisterClient(authID) })
	scheduler := newAuthScheduler(&RoundRobinSelector{})
	active := &Auth{ID: authID, Provider: provider, Status: StatusActive, revision: 1}
	scheduler.upsertBatch([]*Auth{active})
	if _, errPick := scheduler.pickSingle(context.Background(), provider, "old-model", cliproxyexecutor.Options{}, nil); errPick != nil {
		t.Fatal(errPick)
	}

	reg.RegisterClient(authID, provider, []*registry.ModelInfo{{ID: "new-model"}})
	newer := active.Clone()
	newer.revision = 2
	scheduler.upsertBatch([]*Auth{newer})
	meta := scheduler.providers[provider].auths[authID]
	if meta.registryEpoch != reg.ClientRegistrationEpoch(authID) {
		t.Fatal("batch update did not refresh registry epoch")
	}
	if _, errPick := scheduler.pickSingle(context.Background(), provider, "old-model", cliproxyexecutor.Options{}, nil); errPick == nil {
		t.Fatal("batch update left removed model selectable")
	}
	newer = newer.Clone()
	newer.revision++
	scheduler.upsertAuthResult(newer, []string{"new-model"}, false)
	if _, errPick := scheduler.pickSingle(context.Background(), provider, "new-model", cliproxyexecutor.Options{}, nil); errPick != nil {
		t.Fatalf("result following batch failed to retain new model: %v", errPick)
	}
}

func TestManagerResultTargetsLegacyRouteModelArgument(t *testing.T) {
	const authID, provider = "result-legacy-route", "gemini"
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(authID, provider, []*registry.ModelInfo{{ID: "route-model"}, {ID: "target-model"}, {ID: "other-model"}})
	t.Cleanup(func() { reg.UnregisterClient(authID) })
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: authID, Provider: provider, Status: StatusActive}); errRegister != nil {
		t.Fatal(errRegister)
	}
	for _, model := range []string{"route-model", "target-model", "other-model"} {
		if _, errPick := manager.scheduler.pickSingle(context.Background(), provider, model, cliproxyexecutor.Options{}, nil); errPick != nil {
			t.Fatal(errPick)
		}
	}
	state := manager.scheduler.providers[provider]
	routeMeta := state.modelShards["route-model"].entries[authID].meta
	otherMeta := state.modelShards["other-model"].entries[authID].meta
	manager.markResult(context.Background(), Result{AuthID: authID, Provider: provider, Model: "target-model", Success: true}, resultSessionAffinity{}, true, "route-model", false)
	if state.modelShards["route-model"].entries[authID].meta == routeMeta {
		t.Fatal("legacy route argument did not synchronize the route shard")
	}
	if state.modelShards["other-model"].entries[authID].meta != otherMeta {
		t.Fatal("route update touched an unrelated model shard")
	}
}

func TestManagerLifecyclePublishesIsolatedSchedulerSnapshot(t *testing.T) {
	for _, lifecycle := range []string{"register", "update"} {
		t.Run(lifecycle, func(t *testing.T) {
			manager := NewManager(nil, &RoundRobinSelector{}, nil)
			candidate := &Auth{
				ID: "lifecycle-snapshot-" + lifecycle, Provider: "gemini", Status: StatusActive,
				ModelStates: map[string]*ModelState{"model-a": {Status: StatusActive}},
			}
			registered, errRegister := manager.Register(WithSkipPersist(context.Background()), candidate)
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			if lifecycle == "update" {
				if _, errUpdate := manager.Update(WithSkipPersist(context.Background()), registered); errUpdate != nil {
					t.Fatal(errUpdate)
				}
			}

			manager.scheduler.mu.Lock()
			snapshot := manager.scheduler.providers["gemini"].auths[candidate.ID].auth
			manager.scheduler.mu.Unlock()
			manager.mu.Lock()
			live := manager.auths[candidate.ID]
			live.Status = StatusError
			live.Unavailable = true
			live.ModelStates["model-a"].Unavailable = true
			manager.mu.Unlock()

			if snapshot == live {
				t.Fatal("scheduler holds the manager's mutable auth pointer")
			}
			if snapshot.Status != StatusActive || snapshot.Unavailable || snapshot.ModelStates["model-a"].Unavailable {
				t.Fatal("manager mutation changed the published scheduler snapshot")
			}
		})
	}
}
