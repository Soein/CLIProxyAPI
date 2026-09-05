package auth

import (
	"context"
	"testing"
)

func TestManagerMergedUpdatesPreserveCASAndConcurrentDisable(t *testing.T) {
	for _, mode := range []updateAuthMode{updateModeRefresh, updateModePrepare} {
		t.Run(map[updateAuthMode]string{updateModeRefresh: "refresh", updateModePrepare: "prepare"}[mode], func(t *testing.T) {
			ctx := context.Background()
			store := &lifecycleVersionedStore{restoreGeneration: 7}
			manager := NewManager(store, nil, nil)
			base, errRegister := manager.Register(ctx, &Auth{
				ID: "merge-cas", Provider: "codex", Status: StatusActive,
				Metadata: map[string]any{"access_token": "old", "note": "original"},
			})
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			concurrent := base.Clone()
			concurrent.ProxyURL = "http://user.proxy"
			concurrent.Metadata["note"] = "user edit"
			if _, errUpdate := manager.Update(ctx, concurrent); errUpdate != nil {
				t.Fatal(errUpdate)
			}
			if _, errDisable := manager.SetDisabled(ctx, []string{base.ID}, true); errDisable != nil {
				t.Fatal(errDisable)
			}
			current, _ := manager.GetByID(base.ID)
			updated := base.Clone()
			updated.Metadata["access_token"] = "fresh"
			updated.Metadata["project_id"] = "discovered"
			merged, errMerge := manager.updateInternal(ctx, base, updated, mode)
			if errMerge != nil || merged == nil {
				t.Fatalf("merge = %v, %v", merged, errMerge)
			}
			if merged.StoreGeneration() != current.StoreGeneration()+1 || merged.Generation <= current.Generation {
				t.Fatalf("generations = store %d, runtime %d; previous store %d, runtime %d", merged.StoreGeneration(), merged.Generation, current.StoreGeneration(), current.Generation)
			}
			persisted, errGet := store.GetByID(ctx, base.ID)
			if errGet != nil || persisted == nil {
				t.Fatalf("persisted = %v, %v", persisted, errGet)
			}
			for _, auth := range []*Auth{merged, persisted} {
				if auth.Metadata["access_token"] != "fresh" || auth.Metadata["project_id"] != "discovered" || auth.Metadata["note"] != "user edit" || auth.ProxyURL != "http://user.proxy" {
					t.Fatalf("lost merged credential or user edit: %+v", auth)
				}
				if !auth.Disabled || auth.Status != StatusDisabled || auth.Metadata["disabled"] != true {
					t.Fatalf("lost concurrent disable: %+v", auth)
				}
			}
			if store.restoreCalls != 1 || store.saveVersionedCalls != 3 {
				t.Fatalf("restore calls = %d, CAS calls = %d; want 1 and 3", store.restoreCalls, store.saveVersionedCalls)
			}
		})
	}
}

func TestManagerMergedUpdatesDoNotRestoreDeletedCredentials(t *testing.T) {
	for _, mode := range []updateAuthMode{updateModeRefresh, updateModePrepare} {
		t.Run(map[updateAuthMode]string{updateModeRefresh: "refresh", updateModePrepare: "prepare"}[mode], func(t *testing.T) {
			ctx := context.Background()
			store := &lifecycleVersionedStore{restoreGeneration: 7}
			manager := NewManager(store, nil, nil)
			base, errRegister := manager.Register(ctx, &Auth{ID: "deleted-merge", Provider: "codex", Metadata: map[string]any{"access_token": "old"}})
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			if errDelete := manager.DeleteAuths(ctx, []string{base.ID}, func(ctx context.Context) error { return store.Delete(ctx, base.ID) }); errDelete != nil {
				t.Fatal(errDelete)
			}
			updated := base.Clone()
			updated.Metadata["access_token"] = "fresh"
			merged, errMerge := manager.updateInternal(ctx, base, updated, mode)
			if errMerge != nil || merged != nil {
				t.Fatalf("deleted merge = %v, %v; want nil, nil", merged, errMerge)
			}
			if current, ok := manager.GetByID(base.ID); ok || current != nil {
				t.Fatalf("deleted credential returned to runtime: %+v", current)
			}
			if persisted, _ := store.GetByID(ctx, base.ID); persisted != nil || store.restoreCalls != 1 || store.saveVersionedCalls != 0 {
				t.Fatalf("deleted credential was persisted: %+v; restores %d, CAS writes %d", persisted, store.restoreCalls, store.saveVersionedCalls)
			}
		})
	}
}

func TestManagerPreparedAuthRemovedDuringPreparationIsNotReturned(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil, nil, nil)
	base, errRegister := manager.Register(ctx, &Auth{ID: "deleted-preparation", Provider: "codex", Metadata: map[string]any{"access_token": "old"}})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	executor := &testPrepareExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "codex"},
		started:                       make(chan struct{}), release: make(chan struct{}),
	}
	done := make(chan struct{})
	var prepared *Auth
	var errPrepare error
	go func() {
		prepared, errPrepare = manager.prepareRequestAuth(ctx, executor, base)
		close(done)
	}()
	<-executor.started
	manager.Remove(ctx, base.ID)
	close(executor.release)
	<-done
	if prepared != nil || errPrepare == nil {
		t.Fatalf("prepare after removal = %v, %v; want nil and error", prepared, errPrepare)
	}
}

func TestManagerRefreshedAuthPreservesExecutorModelStateDeletion(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil, nil, nil)
	base, errRegister := manager.Register(ctx, &Auth{
		ID: "model-state-deletion", Provider: "codex", Status: StatusActive,
		Metadata:    map[string]any{"access_token": "old"},
		ModelStates: map[string]*ModelState{"model": {Status: StatusError}},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	updated := base.Clone()
	updated.ModelStates = map[string]*ModelState{}
	merged, errMerge := manager.UpdateRefreshedAuth(ctx, base, updated)
	if errMerge != nil || merged == nil {
		t.Fatalf("merge = %v, %v", merged, errMerge)
	}
	if len(merged.ModelStates) != 0 {
		t.Fatalf("deleted model states were restored: %+v", merged.ModelStates)
	}
}
