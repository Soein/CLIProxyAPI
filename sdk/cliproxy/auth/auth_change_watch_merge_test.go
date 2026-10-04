package auth

import (
	"context"
	"sync"
	"testing"
)

func TestAuthChangeWatchMetaMintNotifiesAfterPublication(t *testing.T) {
	for _, mode := range []string{"prepare", "refresh"} {
		t.Run(mode, func(t *testing.T) {
			store := &blockingUnversionedStore{
				unversionedMemoryStore: &unversionedMemoryStore{auths: make(map[string]*Auth)},
				saveStarted:            make(chan struct{}, 1),
				saveRelease:            make(chan struct{}),
			}
			manager := NewManager(store, nil, nil)
			base, err := manager.Register(t.Context(), &Auth{
				ID: "meta-watch", Provider: "meta", Status: StatusActive,
				Metadata: map[string]any{"access_token": "old"},
			})
			if err != nil {
				t.Fatal(err)
			}
			changes, stop := manager.WatchAuthChanges(base.ID)
			defer stop()
			store.block = true
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(store.saveRelease) }) }
			defer release()
			updated := base.Clone()
			updated.Metadata["access_token"] = "new"
			done := make(chan error, 1)
			go func() {
				var errUpdate error
				if mode == "prepare" {
					_, errUpdate = manager.UpdatePreparedAuth(t.Context(), base, updated)
				} else {
					_, errUpdate = manager.UpdateRefreshedAuth(t.Context(), base, updated)
				}
				done <- errUpdate
			}()
			<-store.saveStarted
			select {
			case <-changes:
				t.Fatal("private mint candidate notified before persistence and publication")
			default:
			}
			current, _ := manager.GetByID(base.ID)
			if authAccessToken(current) != "old" {
				t.Fatal("private mint candidate became visible before persistence")
			}
			release()
			if err = <-done; err != nil {
				t.Fatal(err)
			}
			select {
			case <-changes:
			default:
				t.Fatal("published mint did not notify watcher")
			}
			current, _ = manager.GetByID(base.ID)
			if authAccessToken(current) != "new" {
				t.Fatal("watcher notification did not refer to the published mint")
			}
		})
	}
}

func TestAuthChangeWatchCoversHAStoreReloadAndMutations(t *testing.T) {
	store := &lifecycleVersionedStore{}
	manager := NewManager(store, nil, nil)
	auth, err := manager.Register(t.Context(), &Auth{
		ID: "ha-watch", Provider: "antigravity", Status: StatusActive,
		Metadata: map[string]any{"access_token": "old"},
	})
	if err != nil {
		t.Fatal(err)
	}
	changes, stop := manager.WatchAuthChanges(auth.ID)
	defer stop()
	assertChanged := func() {
		t.Helper()
		select {
		case <-changes:
		default:
			t.Fatal("structural HA mutation did not notify watcher")
		}
	}

	remote := auth.Clone()
	remote.Metadata["access_token"] = "remote"
	if _, _, err = store.SaveVersioned(t.Context(), remote, remote.StoreGeneration()); err != nil {
		t.Fatal(err)
	}
	if err = manager.ReloadByID(t.Context(), auth.ID); err != nil {
		t.Fatal(err)
	}
	assertChanged()
	current, _ := manager.GetByID(auth.ID)
	if authAccessToken(current) != "remote" {
		t.Fatal("store reload notification did not refer to the new token")
	}

	for _, disabled := range []bool{true, false} {
		if _, err = manager.SetDisabled(t.Context(), []string{auth.ID}, disabled); err != nil {
			t.Fatal(err)
		}
		assertChanged()
	}
	manager.MarkResult(t.Context(), Result{AuthID: auth.ID, Provider: auth.Provider, Success: true})
	select {
	case <-changes:
		t.Fatal("request accounting invalidated a structural auth watcher")
	default:
	}
	if err = manager.DeleteAuths(t.Context(), []string{auth.ID}, func(ctx context.Context) error {
		return store.Delete(ctx, auth.ID)
	}); err != nil {
		t.Fatal(err)
	}
	assertChanged()
}

func TestAuthChangeWatchCoversStoreReloadRemoval(t *testing.T) {
	store := &lifecycleVersionedStore{}
	manager := NewManager(store, nil, nil)
	auth, err := manager.Register(t.Context(), &Auth{
		ID: "removed-watch", Provider: "antigravity", Metadata: map[string]any{"access_token": "old"},
	})
	if err != nil {
		t.Fatal(err)
	}
	changes, stop := manager.WatchAuthChanges(auth.ID)
	defer stop()
	if err = store.Delete(t.Context(), auth.ID); err != nil {
		t.Fatal(err)
	}
	if err = manager.ReloadByID(t.Context(), auth.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-changes:
	default:
		t.Fatal("store reload removal did not notify watcher")
	}
	if _, exists := manager.GetByID(auth.ID); exists {
		t.Fatal("removed credential remains published")
	}
}
