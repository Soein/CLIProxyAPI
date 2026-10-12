package auth

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
)

func metadataPersistenceAuth(id, provider string) *Auth {
	auth := &Auth{ID: id, Provider: provider, Status: StatusActive, Metadata: map[string]any{"type": provider, "access_token": "old"}}
	auth.SetStoreGeneration(5)
	return auth
}

func TestPersistMetadataEntryPreservesVersionedLifecycle(t *testing.T) {
	store := &lifecycleVersionedStore{}
	manager := NewManager(store, nil, nil)
	seed, errRegister := manager.Register(WithSkipPersist(t.Context()), metadataPersistenceAuth("metadata.json", "antigravity"))
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	for i := uint64(1); i <= 2; i++ {
		if saved, preempted := manager.PersistMetadataEntry(WithSkipPersist(t.Context()), seed.ID, seed.RegistrationEpoch, "hint", i, nil); !saved || preempted {
			t.Fatalf("PersistMetadataEntry() = (%v, %v)", saved, preempted)
		}
		current, _ := manager.GetByID(seed.ID)
		if current.StoreGeneration() != 5+i || current.Generation != seed.Generation || current.CredentialVersion != seed.CredentialVersion || current.RegistrationEpoch != seed.RegistrationEpoch || current.durableRevision != seed.durableRevision {
			t.Fatalf("metadata save changed lifecycle or lost store generation: %+v", current)
		}
		if current.Metadata["hint"] != i {
			t.Fatalf("hint = %v, want %d", current.Metadata["hint"], i)
		}
	}
}

type blockedMetadataVersionedStore struct {
	*lifecycleVersionedStore
	entered chan struct{}
	release chan struct{}
}

func (s *blockedMetadataVersionedStore) SaveVersioned(ctx context.Context, auth *Auth, expected uint64) (string, uint64, error) {
	if isMetadataPersistContext(ctx) {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return "", 0, ctx.Err()
		}
	}
	return s.lifecycleVersionedStore.SaveVersioned(ctx, auth, expected)
}

func TestPersistMetadataEntryPreservesConcurrentWatcherUpdate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &blockedMetadataVersionedStore{lifecycleVersionedStore: &lifecycleVersionedStore{}, entered: make(chan struct{}), release: make(chan struct{})}
		manager := NewManager(store, nil, nil)
		seed, errRegister := manager.Register(WithSkipPersist(t.Context()), metadataPersistenceAuth("metadata.json", "antigravity"))
		if errRegister != nil {
			t.Fatal(errRegister)
		}
		done := make(chan bool, 1)
		go func() {
			saved, _ := manager.PersistMetadataEntry(t.Context(), seed.ID, seed.RegistrationEpoch, "hint", "models", nil)
			done <- saved
		}()
		<-store.entered
		update := seed.Clone()
		update.Metadata["access_token"] = "new"
		updated, errUpdate := manager.Update(WithSkipPersist(t.Context()), update)
		if errUpdate != nil {
			t.Fatal(errUpdate)
		}
		close(store.release)
		if !<-done {
			t.Fatal("metadata save failed")
		}
		current, _ := manager.GetByID(seed.ID)
		if current.Metadata["access_token"] != "new" || current.CredentialVersion != updated.CredentialVersion || current.durableRevision != updated.durableRevision || current.revision != updated.revision || current.Generation != updated.Generation {
			t.Fatalf("metadata publication rolled back concurrent update: %+v", current)
		}
		if current.StoreGeneration() != 6 || current.Metadata["hint"] != "models" {
			t.Fatalf("metadata publication lost committed delta: %+v", current)
		}
	})
}

func TestPersistMetadataEntryInvalidatedHintRetainsCommittedGeneration(t *testing.T) {
	for _, tc := range []struct {
		name              string
		watcherGeneration uint64
		wantGeneration    uint64
	}{
		{name: "unversioned watcher", wantGeneration: 6},
		{name: "newer versioned watcher", watcherGeneration: 9, wantGeneration: 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := &blockedMetadataVersionedStore{lifecycleVersionedStore: &lifecycleVersionedStore{}, entered: make(chan struct{}), release: make(chan struct{})}
				manager := NewManager(store, nil, nil)
				seed, errRegister := manager.Register(WithSkipPersist(t.Context()), metadataPersistenceAuth("metadata.json", "antigravity"))
				if errRegister != nil {
					t.Fatal(errRegister)
				}
				done := make(chan bool, 1)
				go func() {
					saved, _ := manager.PersistMetadataEntry(t.Context(), seed.ID, seed.RegistrationEpoch, "hint", "old token models", func(current *Auth) bool {
						return current.Metadata["access_token"] == "old"
					})
					done <- saved
				}()
				<-store.entered
				update := seed.Clone()
				update.Metadata["access_token"] = "new"
				update.SetStoreGeneration(tc.watcherGeneration)
				if _, errUpdate := manager.Update(WithSkipPersist(t.Context()), update); errUpdate != nil {
					t.Fatal(errUpdate)
				}
				close(store.release)
				if <-done {
					t.Fatal("invalidated metadata save was accepted")
				}
				current, _ := manager.GetByID(seed.ID)
				if current.StoreGeneration() != tc.wantGeneration || current.Metadata["access_token"] != "new" || current.Metadata["hint"] != nil {
					t.Fatalf("invalidated hint lost committed generation or changed credentials: %+v", current)
				}
			})
		})
	}
}

func TestPersistMetadataEntryVersionedSaveIsPreemptedByForeground(t *testing.T) {
	for _, operation := range []string{"same auth update", "other auth update", "other auth restore", "other meta prepare", "same meta prepare"} {
		t.Run(operation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := &blockedMetadataVersionedStore{lifecycleVersionedStore: &lifecycleVersionedStore{}, entered: make(chan struct{})}
				manager := NewManager(store, nil, nil)
				provider := "antigravity"
				if operation == "same meta prepare" {
					provider = "meta"
				}
				seed, errRegister := manager.Register(WithSkipPersist(t.Context()), metadataPersistenceAuth("metadata.json", provider))
				if errRegister != nil {
					t.Fatal(errRegister)
				}
				other, errOther := manager.Register(WithSkipPersist(t.Context()), metadataPersistenceAuth("other.json", "meta"))
				if errOther != nil {
					t.Fatal(errOther)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				metadataDone := make(chan [2]bool, 1)
				go func() {
					saved, preempted := manager.PersistMetadataEntry(ctx, seed.ID, seed.RegistrationEpoch, "hint", "models", nil)
					metadataDone <- [2]bool{saved, preempted}
				}()
				<-store.entered
				foregroundDone := make(chan error, 1)
				go func() {
					var errOperation error
					switch operation {
					case "same auth update":
						update := seed.Clone()
						update.Label = "foreground"
						_, errOperation = manager.Update(ctx, update)
					case "other auth update":
						update := other.Clone()
						update.Label = "foreground"
						_, errOperation = manager.Update(ctx, update)
					case "other auth restore":
						_, errOperation = manager.Register(ctx, &Auth{ID: "new.json", Provider: "xai", Metadata: map[string]any{"type": "xai"}})
					case "other meta prepare":
						update := other.Clone()
						update.Metadata["access_token"] = "minted"
						_, errOperation = manager.UpdatePreparedAuth(ctx, other, update)
					case "same meta prepare":
						update := seed.Clone()
						update.Metadata["access_token"] = "minted"
						_, errOperation = manager.UpdatePreparedAuth(ctx, seed, update)
					}
					foregroundDone <- errOperation
				}()
				synctest.Wait()
				select {
				case result := <-metadataDone:
					if result != [2]bool{false, true} {
						t.Errorf("metadata result = %v, want [false true]", result)
					}
				default:
					t.Error("foreground operation did not preempt metadata save")
					cancel()
					<-metadataDone
				}
				if errForeground := <-foregroundDone; errForeground != nil {
					t.Errorf("foreground operation failed: %v", errForeground)
				}
			})
		})
	}
}

type conflictingMetadataStore struct {
	*authoritativeConvergenceStore
	errSave error
}

func (s *conflictingMetadataStore) SaveVersioned(context.Context, *Auth, uint64) (string, uint64, error) {
	return "", 0, s.errSave
}

func TestPersistMetadataEntryReloadsAuthoritativeConflict(t *testing.T) {
	for _, kind := range []string{"conflict", "deleted", "commit unknown"} {
		t.Run(kind, func(t *testing.T) {
			seed := metadataPersistenceAuth("metadata.json", "antigravity")
			authoritative := seed.Clone()
			authoritative.Metadata["access_token"] = "external"
			authoritative.SetStoreGeneration(7)
			state := AuthAuthoritativeState{Auth: authoritative, Exists: true, Generation: 7}
			errSave := ErrAuthStoreConflict
			if kind == "deleted" {
				state = AuthAuthoritativeState{Exists: true, Deleted: true, Generation: 7}
				errSave = ErrAuthStoreDeleted
			} else if kind == "commit unknown" {
				errSave = NewAuthStoreCommitUnknown(map[string]uint64{seed.ID: 7}, errors.New("ack lost"))
			}
			store := &conflictingMetadataStore{authoritativeConvergenceStore: &authoritativeConvergenceStore{rows: map[string]AuthAuthoritativeState{seed.ID: state}}, errSave: errSave}
			manager := NewManager(store, nil, nil)
			registered, errRegister := manager.Register(WithSkipPersist(t.Context()), seed)
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			if saved, preempted := manager.PersistMetadataEntry(t.Context(), seed.ID, registered.RegistrationEpoch, "hint", "stale", nil); saved || preempted {
				t.Fatalf("PersistMetadataEntry() = (%v, %v)", saved, preempted)
			}
			current, exists := manager.GetByID(seed.ID)
			if kind == "deleted" {
				if exists {
					t.Fatalf("deleted credential remained: %+v", current)
				}
				return
			}
			if !exists || current.StoreGeneration() != 7 || current.Metadata["access_token"] != "external" || current.Metadata["hint"] != nil {
				t.Fatalf("failed to converge to authoritative credential: %+v", current)
			}
		})
	}
}
