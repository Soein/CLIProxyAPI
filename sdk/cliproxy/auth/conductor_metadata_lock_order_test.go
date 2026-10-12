package auth

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
)

func TestMetadataPersistenceDoesNotHoldAuthMutexWhileWaitingForMetaForeground(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		manager := NewManager(&lifecycleVersionedStore{}, nil, nil)
		seed, errRegister := manager.Register(WithSkipPersist(t.Context()), metadataPersistenceAuth("meta-lock-order.json", "meta"))
		if errRegister != nil {
			t.Fatal(errRegister)
		}
		// Pause Meta's real lifecycle lock sequence between the foreground gate
		// and its persistence mutex, a valid goroutine scheduling interleaving.
		releaseMeta, errMeta := manager.lockAuthLifecycleContext(t.Context(), seed, updateModePrepare)
		if errMeta != nil {
			t.Fatal(errMeta)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		metadataDone := make(chan struct{})
		go func() {
			manager.PersistMetadataEntry(ctx, seed.ID, seed.RegistrationEpoch, "hint", "models", nil)
			close(metadataDone)
		}()
		synctest.Wait()
		persistenceLock := manager.authPersistLock(seed.ID)
		mutexHeld := !persistenceLock.TryLock()
		if !mutexHeld {
			persistenceLock.Unlock()
		} else {
			state := manager.globalMetadataSaveState()
			state.mu.Lock()
			t.Logf("Meta foreground active=%d; metadata registered globally=%v", state.fgActive, state.cancel != nil)
			state.mu.Unlock()
			t.Error("metadata save holds persistence mutex while waiting for Meta's foreground gate")
			// Cancel before attempting the contended mutex: Mutex waits are not
			// durably blocked in synctest and would prevent Wait from returning.
			cancel()
			<-metadataDone
		}
		unlock := manager.lockAuthPersistence([]string{seed.ID})
		unlock()
		releaseMeta()
		if !mutexHeld {
			<-metadataDone
		}
	})
}

type uncancellableMetadataStore struct {
	*blockedMetadataVersionedStore
}

func (s *uncancellableMetadataStore) SaveVersioned(ctx context.Context, auth *Auth, expected uint64) (string, uint64, error) {
	if isMetadataPersistContext(ctx) {
		close(s.entered)
		<-s.release
	}
	return s.lifecycleVersionedStore.SaveVersioned(ctx, auth, expected)
}

func TestMetadataForegroundCancellationPreservesDisabledAdmission(t *testing.T) {
	for _, operation := range []string{"delete", "disable", "enable"} {
		t.Run(operation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := &uncancellableMetadataStore{blockedMetadataVersionedStore: &blockedMetadataVersionedStore{
					lifecycleVersionedStore: &lifecycleVersionedStore{}, entered: make(chan struct{}), release: make(chan struct{}),
				}}
				manager := NewManager(store, nil, nil)
				seed := metadataPersistenceAuth("uncancellable.json", "antigravity")
				if operation == "enable" {
					seed.Disabled = true
					seed.Status = StatusDisabled
				}
				seed, errRegister := manager.Register(WithSkipPersist(t.Context()), seed)
				if errRegister != nil {
					t.Fatal(errRegister)
				}
				metadataDone := make(chan struct{})
				go func() {
					manager.PersistMetadataEntry(t.Context(), seed.ID, seed.RegistrationEpoch, "hint", "models", nil)
					close(metadataDone)
				}()
				<-store.entered
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				foregroundDone := make(chan error, 1)
				go func() {
					var errOperation error
					if operation == "delete" {
						errOperation = manager.DeleteAuths(ctx, []string{seed.ID}, func(ctx context.Context) error { return store.Delete(ctx, seed.ID) })
					} else {
						_, errOperation = manager.SetDisabled(ctx, []string{seed.ID}, operation == "disable")
					}
					foregroundDone <- errOperation
				}()
				synctest.Wait()
				current, exists := manager.GetByID(seed.ID)
				if !exists || current.Disabled != (operation != "delete") {
					t.Errorf("waiting %s changed admission incorrectly: %+v", operation, current)
				}
				cancel()
				synctest.Wait()
				select {
				case errOperation := <-foregroundDone:
					if !errors.Is(errOperation, context.Canceled) {
						t.Errorf("foreground cancellation error = %v", errOperation)
					}
				default:
					t.Error("foreground cancellation waited for uncancellable metadata storage")
					close(store.release)
					<-metadataDone
					<-foregroundDone
					return
				}
				close(store.release)
				<-metadataDone
			})
		})
	}
}

type metadataReentrantLifecycleHook struct {
	manager *Manager
	target  string
	result  chan [2]bool
}

func (*metadataReentrantLifecycleHook) OnAuthRegistered(context.Context, *Auth) {}
func (*metadataReentrantLifecycleHook) OnResult(context.Context, Result)        {}

func (h *metadataReentrantLifecycleHook) persist(ctx context.Context) {
	saved, preempted := h.manager.PersistMetadataEntry(ctx, h.target, 0, "hook_hint", "models", nil)
	h.result <- [2]bool{saved, preempted}
}

func (h *metadataReentrantLifecycleHook) OnAuthUpdated(ctx context.Context, _ *Auth) {
	h.persist(ctx)
}

func (h *metadataReentrantLifecycleHook) OnAuthRemoved(ctx context.Context, _ string) {
	h.persist(ctx)
}

func TestMetadataForegroundGateReleasedBeforeLifecycleHooks(t *testing.T) {
	for _, operation := range []string{"delete", "disable", "enable", "atomic disable", "atomic enable", "meta prepare"} {
		t.Run(operation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				baseStore := &blockedMetadataVersionedStore{lifecycleVersionedStore: &lifecycleVersionedStore{}, entered: make(chan struct{}), release: make(chan struct{})}
				close(baseStore.release)
				var store Store = baseStore.lifecycleVersionedStore
				if operation == "atomic disable" || operation == "atomic enable" {
					store = &blockedMetadataAtomicStore{blockedMetadataVersionedStore: baseStore}
				}
				hook := &metadataReentrantLifecycleHook{target: "hook-target.json", result: make(chan [2]bool, 1)}
				manager := NewManager(store, nil, hook)
				hook.manager = manager
				provider := "antigravity"
				if operation == "meta prepare" {
					provider = "meta"
				}
				seed := metadataPersistenceAuth("hook-source.json", provider)
				if operation == "enable" || operation == "atomic enable" {
					seed.Disabled = true
					seed.Status = StatusDisabled
				}
				seed, errRegister := manager.Register(WithSkipPersist(t.Context()), seed)
				if errRegister != nil {
					t.Fatal(errRegister)
				}
				if _, errTarget := manager.Register(WithSkipPersist(t.Context()), metadataPersistenceAuth(hook.target, "antigravity")); errTarget != nil {
					t.Fatal(errTarget)
				}
				var errOperation error
				switch operation {
				case "delete":
					errOperation = manager.DeleteAuths(t.Context(), []string{seed.ID}, func(ctx context.Context) error { return store.Delete(ctx, seed.ID) })
				case "meta prepare":
					update := seed.Clone()
					update.Metadata["access_token"] = "minted"
					_, errOperation = manager.UpdatePreparedAuth(t.Context(), seed, update)
				default:
					_, errOperation = manager.SetDisabled(t.Context(), []string{seed.ID}, operation == "disable" || operation == "atomic disable")
				}
				if errOperation != nil {
					t.Fatal(errOperation)
				}
				if result := <-hook.result; result != [2]bool{true, false} {
					t.Fatalf("reentrant metadata persistence = %v, want [true false]", result)
				}
			})
		})
	}
}

type blockedMetadataAtomicStore struct {
	*blockedMetadataVersionedStore
}

func (s *blockedMetadataAtomicStore) SaveBatch(ctx context.Context, auths []*Auth, finalize func(func() error) error) error {
	return finalize(func() error {
		for _, auth := range auths {
			_, generation, errSave := s.SaveVersioned(ctx, auth, auth.StoreGeneration())
			if errSave != nil {
				return errSave
			}
			auth.SetStoreGeneration(generation)
		}
		return nil
	})
}

func TestMetadataPersistenceIsPreemptedBeforeDeleteOrDisabledLocks(t *testing.T) {
	for _, operation := range []string{"delete", "disable", "enable", "atomic disable", "atomic enable"} {
		t.Run(operation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				baseStore := &blockedMetadataVersionedStore{lifecycleVersionedStore: &lifecycleVersionedStore{}, entered: make(chan struct{})}
				var store Store = baseStore
				if operation == "atomic disable" || operation == "atomic enable" {
					store = &blockedMetadataAtomicStore{blockedMetadataVersionedStore: baseStore}
				}
				manager := NewManager(store, nil, nil)
				seed := metadataPersistenceAuth("blocked-management.json", "antigravity")
				if operation == "enable" || operation == "atomic enable" {
					seed.Disabled = true
					seed.Status = StatusDisabled
				}
				seed, errRegister := manager.Register(WithSkipPersist(t.Context()), seed)
				if errRegister != nil {
					t.Fatal(errRegister)
				}
				metadataCtx, cancelMetadata := context.WithCancel(t.Context())
				defer cancelMetadata()
				metadataDone := make(chan [2]bool, 1)
				go func() {
					saved, preempted := manager.PersistMetadataEntry(metadataCtx, seed.ID, seed.RegistrationEpoch, "hint", "models", nil)
					metadataDone <- [2]bool{saved, preempted}
				}()
				<-baseStore.entered
				foregroundCtx, cancelForeground := context.WithCancel(t.Context())
				defer cancelForeground()
				foregroundDone := make(chan error, 1)
				go func() {
					var errOperation error
					if operation == "delete" {
						errOperation = manager.DeleteAuths(foregroundCtx, []string{seed.ID}, func(ctx context.Context) error { return store.Delete(ctx, seed.ID) })
					} else {
						_, errOperation = manager.SetDisabled(foregroundCtx, []string{seed.ID}, operation == "disable" || operation == "atomic disable")
					}
					foregroundDone <- errOperation
				}()
				synctest.Wait()
				select {
				case errOperation := <-foregroundDone:
					if errOperation != nil {
						t.Errorf("foreground operation: %v", errOperation)
					}
					if result := <-metadataDone; result != [2]bool{false, true} {
						t.Errorf("metadata result = %v, want [false true]", result)
					}
				default:
					current, _ := manager.GetByID(seed.ID)
					if current == nil {
						t.Error("runtime credential disappeared before persistent deletion")
					} else {
						t.Logf("runtime while management waits: disabled=%v status=%s", current.Disabled, current.Status)
					}
					t.Error("foreground operation did not preempt context-aware background metadata save")
					cancelForeground()
					synctest.Wait()
					select {
					case <-foregroundDone:
					default:
						t.Error("foreground context cancellation did not interrupt persistence mutex wait")
						cancelMetadata()
						<-foregroundDone
					}
					cancelMetadata()
					<-metadataDone
				}
			})
		})
	}
}
