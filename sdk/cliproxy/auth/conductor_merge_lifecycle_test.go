package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	internalregistry "github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
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

type metaPrepareExecutor struct {
	started chan struct{}
	release chan struct{}
}

func (e *metaPrepareExecutor) ShouldPrepareRequestAuth(auth *Auth) bool {
	return auth == nil || auth.Metadata == nil || auth.Metadata["api_key"] == nil
}

func (e *metaPrepareExecutor) PrepareRequestAuth(ctx context.Context, auth *Auth) (*Auth, error) {
	if e.started != nil {
		close(e.started)
	}
	if e.release != nil {
		<-e.release
	}
	updated := auth.Clone()
	if updated.Metadata == nil {
		updated.Metadata = make(map[string]any)
	}
	updated.Metadata["api_key"] = "minted-meta-key"
	return updated, nil
}

func TestManagerMetaMintPersistenceFailure(t *testing.T) {
	ctx := context.Background()
	store := &lifecycleVersionedStore{restoreGeneration: 7}
	manager := NewManager(store, nil, nil)
	base, errRegister := manager.Register(ctx, &Auth{
		ID:       "meta-mint-failure",
		Provider: "meta",
		Status:   StatusActive,
		Metadata: map[string]any{"access_token": "token"},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}

	// Now configure the store to fail writes
	store.saveErr = errors.New("simulated persist failure")

	updated := base.Clone()
	updated.Metadata["api_key"] = "minted-meta-key"

	saved, errUpdate := manager.UpdatePreparedAuth(ctx, base, updated)
	if errUpdate == nil || !strings.Contains(errUpdate.Error(), "persist meta auth") {
		t.Fatalf("expected persist meta auth error, got saved=%v, err=%v", saved, errUpdate)
	}

	current, ok := manager.GetByID(base.ID)
	if !ok || current == nil {
		t.Fatal("expected base auth to remain registered after failed mint persistence")
	}
	if current.Metadata["api_key"] == "minted-meta-key" {
		t.Fatal("unpersisted minted key was installed into manager auths")
	}
}

func TestManagerMetaMintRemovedDuringPreparation(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil, nil, nil)
	base, errRegister := manager.Register(ctx, &Auth{
		ID:       "meta-mint-removal",
		Provider: "meta",
		Status:   StatusActive,
		Metadata: map[string]any{"access_token": "token"},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}

	executor := &metaPrepareExecutor{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}

	done := make(chan struct{})
	var prepared *Auth
	var errPrepare error
	go func() {
		prepared, errPrepare = manager.PrepareRequestAuth(ctx, executor, base)
		close(done)
	}()

	<-executor.started
	manager.Remove(ctx, base.ID)
	close(executor.release)
	<-done

	if prepared != nil || errPrepare == nil {
		t.Fatalf("prepare after removal = %v, %v; want nil and error", prepared, errPrepare)
	}
	if !strings.Contains(errPrepare.Error(), "credential removed during mint") {
		t.Fatalf("expected credential removed during mint error, got: %v", errPrepare)
	}
	if current, ok := manager.GetByID(base.ID); ok || current != nil {
		t.Fatalf("removed auth resurrected in manager: %+v", current)
	}
}

type blockingSaveStore struct {
	lifecycleVersionedStore
	saveStarted chan struct{}
	saveRelease chan struct{}
}

func (s *blockingSaveStore) Save(ctx context.Context, auth *Auth) (string, error) {
	if s.saveStarted != nil {
		select {
		case s.saveStarted <- struct{}{}:
		default:
		}
	}
	if s.saveRelease != nil {
		<-s.saveRelease
	}
	return s.lifecycleVersionedStore.Save(ctx, auth)
}

func (s *blockingSaveStore) SaveVersioned(ctx context.Context, auth *Auth, expectedGeneration uint64) (string, uint64, error) {
	if s.saveStarted != nil {
		select {
		case s.saveStarted <- struct{}{}:
		default:
		}
	}
	if s.saveRelease != nil {
		<-s.saveRelease
	}
	return s.lifecycleVersionedStore.SaveVersioned(ctx, auth, expectedGeneration)
}

func TestManagerMetaMint_BlockedSaveDoesNotExposeMintedKey(t *testing.T) {
	ctx := context.Background()
	store := &blockingSaveStore{
		lifecycleVersionedStore: lifecycleVersionedStore{restoreGeneration: 7},
		saveStarted:             make(chan struct{}, 1),
		saveRelease:             make(chan struct{}),
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	manager.RegisterExecutor(schedulerTestExecutor{provider: "meta"})
	base, errRegister := manager.Register(ctx, &Auth{
		ID:       "meta-blocked-save",
		Provider: "meta",
		Status:   StatusActive,
		Metadata: map[string]any{"access_token": "original-token"},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	internalregistry.GetGlobalRegistry().RegisterClient(base.ID, "meta", []*internalregistry.ModelInfo{{ID: "any-model"}})
	t.Cleanup(func() {
		internalregistry.GetGlobalRegistry().UnregisterClient(base.ID)
	})

	updated := base.Clone()
	updated.Metadata["api_key"] = "minted-secret-key"

	updateDone := make(chan struct{})
	var saved *Auth
	var errUpdate error
	go func() {
		saved, errUpdate = manager.UpdatePreparedAuth(ctx, base, updated)
		close(updateDone)
	}()

	<-store.saveStarted

	// 1. GetByID must NOT see the uncommitted minted api_key
	current, ok := manager.GetByID(base.ID)
	if !ok || current == nil {
		t.Fatal("expected current auth to exist while save is blocked")
	}
	if current.Metadata["api_key"] != nil {
		t.Fatalf("uncommitted minted api_key exposed via GetByID: %v", current.Metadata["api_key"])
	}
	if current.Metadata["access_token"] != "original-token" {
		t.Fatalf("access_token = %v, want original-token", current.Metadata["access_token"])
	}

	// 2. Concurrent selection sees the old auth without minted key
	selected, errSelect := manager.SelectAuth(ctx, "meta", "any-model", cliproxyexecutor.Options{})
	if errSelect != nil {
		t.Fatalf("concurrent SelectAuth error: %v", errSelect)
	}
	if selected == nil || selected.Metadata["api_key"] != nil {
		t.Fatalf("concurrent SelectAuth exposed minted key: %+v", selected)
	}

	// 3. Complete persistence
	close(store.saveRelease)
	<-updateDone
	if errUpdate != nil {
		t.Fatalf("UpdatePreparedAuth error: %v", errUpdate)
	}
	if saved == nil || saved.Metadata["api_key"] != "minted-secret-key" {
		t.Fatalf("saved auth missing minted-secret-key: %+v", saved)
	}

	// Now GetByID sees the committed minted key
	currentCommitted, okCommitted := manager.GetByID(base.ID)
	if !okCommitted || currentCommitted.Metadata["api_key"] != "minted-secret-key" {
		t.Fatalf("expected minted-secret-key after commit, got: %+v", currentCommitted)
	}
}

func TestManagerMetaMint_SaveDeleteReRegisterOrdering(t *testing.T) {
	ctx := context.Background()
	store := &blockingSaveStore{
		lifecycleVersionedStore: lifecycleVersionedStore{restoreGeneration: 7},
		saveStarted:             make(chan struct{}, 1),
		saveRelease:             make(chan struct{}),
	}
	manager := NewManager(store, nil, nil)
	base, errRegister := manager.Register(ctx, &Auth{
		ID:       "meta-delete-rereg",
		Provider: "meta",
		Status:   StatusActive,
		Metadata: map[string]any{"access_token": "original-token"},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}

	updated := base.Clone()
	updated.Metadata["api_key"] = "minted-secret-key"

	updateDone := make(chan struct{})
	var errUpdate error
	go func() {
		defer close(updateDone)
		_, errUpdate = manager.UpdatePreparedAuth(ctx, base, updated)
	}()

	select {
	case <-store.saveStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for saveStarted")
	}

	// Remove while save is blocked
	manager.Remove(ctx, base.ID)

	// Run registration asynchronously because Register needs persistence lock held by blocked save
	var (
		reRegistered *Auth
		errReReg     error
	)
	regDone := make(chan struct{})
	go func() {
		defer close(regDone)
		reRegistered, errReReg = manager.Register(ctx, &Auth{
			ID:       base.ID,
			Provider: "meta",
			Status:   StatusActive,
			Metadata: map[string]any{"access_token": "re-registered-token"},
		})
	}()

	// Cleanup guaranteeing no stranded goroutine on failure
	var saveReleased bool
	t.Cleanup(func() {
		if !saveReleased {
			close(store.saveRelease)
		}
		<-updateDone
		<-regDone
	})

	// Release save in deterministic order
	saveReleased = true
	close(store.saveRelease)

	select {
	case <-updateDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for updateDone")
	}

	if errUpdate == nil {
		t.Fatal("expected UpdatePreparedAuth to fail after credential removal/re-registration")
	}

	select {
	case <-regDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for regDone")
	}
	if errReReg != nil {
		t.Fatalf("Register error: %v", errReReg)
	}

	current, ok := manager.GetByID(base.ID)
	if !ok || current == nil {
		t.Fatal("expected re-registered auth to be present")
	}
	if current.Metadata["api_key"] == "minted-secret-key" {
		t.Fatal("minted candidate from previous registration cycle resurrected into re-registered auth")
	}
	if current.Metadata["access_token"] != "re-registered-token" {
		t.Fatalf("access_token = %v, want re-registered-token", current.Metadata["access_token"])
	}
	if current.RegistrationEpoch != reRegistered.RegistrationEpoch {
		t.Fatalf("RegistrationEpoch = %d, want %d", current.RegistrationEpoch, reRegistered.RegistrationEpoch)
	}
}

func TestManagerMetaMint_SaveDeleteAuthsSerialized(t *testing.T) {
	ctx := context.Background()
	store := &blockingSaveStore{
		lifecycleVersionedStore: lifecycleVersionedStore{restoreGeneration: 7},
		saveStarted:             make(chan struct{}, 1),
		saveRelease:             make(chan struct{}),
	}
	manager := NewManager(store, nil, nil)
	base, errRegister := manager.Register(ctx, &Auth{
		ID:       "meta-delete-auths-ser",
		Provider: "meta",
		Status:   StatusActive,
		Metadata: map[string]any{"access_token": "original-token"},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}

	updated := base.Clone()
	updated.Metadata["api_key"] = "minted-secret-key"

	updateDone := make(chan struct{})
	go func() {
		_, _ = manager.UpdatePreparedAuth(ctx, base, updated)
		close(updateDone)
	}()

	<-store.saveStarted

	deleteStarted := make(chan struct{}, 1)
	deleteDone := make(chan struct{})
	go func() {
		select {
		case deleteStarted <- struct{}{}:
		default:
		}
		_ = manager.DeleteAuths(ctx, []string{base.ID}, func(deleteCtx context.Context) error {
			return store.Delete(deleteCtx, base.ID)
		})
		close(deleteDone)
	}()

	<-deleteStarted
	// DeleteAuths must be waiting on the per-ID persistence lock held by Meta mint save!
	select {
	case <-deleteDone:
		t.Fatal("DeleteAuths completed before Meta mint save was released (lock not shared)")
	default:
	}

	// Release save
	close(store.saveRelease)
	<-updateDone
	<-deleteDone

	// After DeleteAuths finishes, auth is deleted!
	if current, ok := manager.GetByID(base.ID); ok || current != nil {
		t.Fatalf("auth remained in manager after DeleteAuths: %+v", current)
	}
}

func TestManagerMetaMint_Old401ClearedOnSuccessfulSave(t *testing.T) {
	ctx := context.Background()
	store := &lifecycleVersionedStore{restoreGeneration: 3}
	manager := NewManager(store, nil, nil)
	base, errRegister := manager.Register(ctx, &Auth{
		ID:          "meta-clear-401",
		Provider:    "meta",
		Status:      StatusError,
		Unavailable: true,
		LastError: &Error{
			Code:       "unauthorized",
			Message:    "401 Unauthorized",
			HTTPStatus: http.StatusUnauthorized,
		},
		ModelStates: map[string]*ModelState{
			"meta-llama-3": {
				Unavailable: true,
				LastError: &Error{
					Code:       "unauthorized",
					Message:    "401 Unauthorized",
					HTTPStatus: http.StatusUnauthorized,
				},
			},
		},
		Metadata: map[string]any{"access_token": "old-token"},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}

	refreshed := base.Clone()
	refreshed.Metadata["access_token"] = "refreshed-token"
	refreshed.Metadata["api_key"] = "minted-key"
	refreshed.LastError = nil
	refreshed.Unavailable = false
	refreshed.Status = StatusActive
	refreshed.ModelStates = map[string]*ModelState{
		"meta-llama-3": {
			Unavailable: false,
			LastError:   nil,
		},
	}

	saved, errUpdate := manager.UpdateRefreshedAuth(ctx, base, refreshed)
	if errUpdate != nil {
		t.Fatalf("UpdateRefreshedAuth failed: %v", errUpdate)
	}

	// 1. Returned saved auth must have 401 cleared and be active
	if saved.LastError != nil {
		t.Fatalf("expected LastError to be nil, got: %v", saved.LastError)
	}
	if saved.Unavailable {
		t.Fatal("expected Unavailable to be false")
	}
	if saved.Status != StatusActive {
		t.Fatalf("expected StatusActive, got: %v", saved.Status)
	}
	if ms, ok := saved.ModelStates["meta-llama-3"]; !ok || ms.LastError != nil || ms.Unavailable {
		t.Fatalf("expected model state 401 cleared, got: %+v", ms)
	}

	// 2. Visible auth in manager must have 401 cleared and be active
	current, ok := manager.GetByID(base.ID)
	if !ok || current == nil {
		t.Fatal("expected current auth to exist")
	}
	if current.LastError != nil {
		t.Fatalf("expected current LastError to be nil, got: %v", current.LastError)
	}
	if current.Unavailable {
		t.Fatal("expected current Unavailable to be false")
	}
	if current.Status != StatusActive {
		t.Fatalf("expected current StatusActive, got: %v", current.Status)
	}
	if ms, ok := current.ModelStates["meta-llama-3"]; !ok || ms.LastError != nil || ms.Unavailable {
		t.Fatalf("expected current model state 401 cleared, got: %+v", ms)
	}
	if current.Metadata["api_key"] != "minted-key" {
		t.Fatalf("expected minted-key, got: %v", current.Metadata["api_key"])
	}
	if current.Generation <= base.Generation {
		t.Fatalf("expected monotonic generation > %d, got %d", base.Generation, current.Generation)
	}
}

type unversionedMemoryStore struct {
	mu    sync.Mutex
	auths map[string]*Auth
}

func (s *unversionedMemoryStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := make([]*Auth, 0, len(s.auths))
	for _, a := range s.auths {
		res = append(res, a.Clone())
	}
	return res, nil
}

func (s *unversionedMemoryStore) Save(_ context.Context, auth *Auth) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.auths[auth.ID] = auth.Clone()
	return auth.ID, nil
}

func (s *unversionedMemoryStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.auths, id)
	return nil
}

func (s *unversionedMemoryStore) GetByID(_ context.Context, id string) (*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.auths[id]; ok {
		return a.Clone(), nil
	}
	return nil, nil
}

type blockingUnversionedStore struct {
	*unversionedMemoryStore
	block       bool
	saveStarted chan struct{}
	saveRelease chan struct{}
}

func (s *blockingUnversionedStore) Save(ctx context.Context, auth *Auth) (string, error) {
	if s.block {
		if s.saveStarted != nil {
			select {
			case s.saveStarted <- struct{}{}:
			default:
			}
		}
		if s.saveRelease != nil {
			<-s.saveRelease
		}
	}
	return s.unversionedMemoryStore.Save(ctx, auth)
}

func TestManagerMetaMint_NotesOnlyUpdateRacingMintCannotRevertToken(t *testing.T) {
	for _, versioned := range []bool{true, false} {
		name := "versioned"
		if !versioned {
			name = "unversioned"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var (
				store       Store
				saveStarted = make(chan struct{}, 1)
				saveRelease = make(chan struct{})
				getStored   func(id string) (*Auth, error)
			)
			if versioned {
				vStore := &blockingSaveStore{
					lifecycleVersionedStore: lifecycleVersionedStore{restoreGeneration: 5},
					saveStarted:             saveStarted,
					saveRelease:             saveRelease,
				}
				store = vStore
				getStored = func(id string) (*Auth, error) {
					return vStore.GetByID(ctx, id)
				}
			} else {
				uStore := &blockingUnversionedStore{
					unversionedMemoryStore: &unversionedMemoryStore{auths: make(map[string]*Auth)},
					saveStarted:            saveStarted,
					saveRelease:            saveRelease,
				}
				store = uStore
				getStored = func(id string) (*Auth, error) {
					return uStore.GetByID(ctx, id)
				}
			}

			manager := NewManager(store, nil, nil)
			base, errRegister := manager.Register(ctx, &Auth{
				ID:       "meta-race-notes-" + name,
				Provider: "meta",
				Status:   StatusActive,
				Metadata: map[string]any{"access_token": "original-token"},
			})
			if errRegister != nil {
				t.Fatal(errRegister)
			}

			if !versioned {
				store.(*blockingUnversionedStore).block = true
			}

			minted := base.Clone()
			minted.Metadata["api_key"] = "minted-api-key"

			mintDone := make(chan struct{})
			var (
				savedMint *Auth
				errMint   error
			)
			go func() {
				defer close(mintDone)
				savedMint, errMint = manager.UpdatePreparedAuth(ctx, base, minted)
			}()

			select {
			case <-saveStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for mint saveStarted")
			}

			// While mint save is blocked in store, normal Update publishes operator notes
			notesAuth := base.Clone()
			notesAuth.Metadata["notes"] = "operator-notes-val"

			notesDone := make(chan struct{})
			var errNotes error
			go func() {
				defer close(notesDone)
				_, errNotes = manager.Update(ctx, notesAuth)
			}()

			// Give notes Update time to publish into m.auths under m.mu
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				curr, ok := manager.GetByID(base.ID)
				if ok && curr != nil && curr.Metadata["notes"] == "operator-notes-val" {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}

			// Release mint save
			close(saveRelease)

			select {
			case <-mintDone:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for mintDone")
			}
			if errMint != nil {
				t.Fatalf("mint UpdatePreparedAuth failed: %v", errMint)
			}
			if savedMint.Metadata["api_key"] != "minted-api-key" {
				t.Fatalf("savedMint missing minted-api-key: %+v", savedMint.Metadata)
			}

			select {
			case <-notesDone:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for notesDone")
			}
			if errNotes != nil {
				t.Fatalf("notes Update failed: %v", errNotes)
			}

			// Assert final visible state in manager
			finalAuth, ok := manager.GetByID(base.ID)
			if !ok || finalAuth == nil {
				t.Fatal("expected auth to exist")
			}
			if finalAuth.Metadata["api_key"] != "minted-api-key" {
				t.Fatalf("minted key was reverted! Metadata = %+v", finalAuth.Metadata)
			}
			if finalAuth.Metadata["notes"] != "operator-notes-val" {
				t.Fatalf("operator notes were lost! Metadata = %+v", finalAuth.Metadata)
			}

			// Assert state in store as well
			storedAuth, errGet := getStored(base.ID)
			if errGet != nil || storedAuth == nil {
				t.Fatalf("failed to get stored auth: %v", errGet)
			}
			if storedAuth.Metadata["api_key"] != "minted-api-key" {
				t.Fatalf("stored auth missing minted key: %+v", storedAuth.Metadata)
			}
			if storedAuth.Metadata["notes"] != "operator-notes-val" {
				t.Fatalf("stored auth missing notes: %+v", storedAuth.Metadata)
			}
		})
	}
}

type reentrantTestHook struct {
	NoopHook
	onUpdated func(ctx context.Context, auth *Auth)
}

func (h *reentrantTestHook) OnAuthUpdated(ctx context.Context, auth *Auth) {
	if h.onUpdated != nil {
		h.onUpdated(ctx, auth)
	}
}

func TestManagerMetaMint_ReentrantHookUpdateDeleteAuths(t *testing.T) {
	ctx := context.Background()
	store := &lifecycleVersionedStore{restoreGeneration: 1}
	hook := &reentrantTestHook{}
	manager := NewManager(store, nil, hook)

	base, errRegister := manager.Register(ctx, &Auth{
		ID:       "meta-reentrant-hook",
		Provider: "meta",
		Status:   StatusActive,
		Metadata: map[string]any{"access_token": "original-token"},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}

	// 1. Reentrant Update inside OnAuthUpdated must not deadlock
	reentrantUpdated := false
	hook.onUpdated = func(hCtx context.Context, a *Auth) {
		if a.ID == base.ID && a.Metadata["api_key"] == "minted-key-1" && !reentrantUpdated {
			reentrantUpdated = true
			reentrantAuth := a.Clone()
			reentrantAuth.Metadata["hook_field"] = "hook-updated-value"
			_, errUpd := manager.Update(hCtx, reentrantAuth)
			if errUpd != nil {
				t.Errorf("reentrant manager.Update failed: %v", errUpd)
			}
		}
	}

	updated := base.Clone()
	updated.Metadata["api_key"] = "minted-key-1"
	_, errMint := manager.UpdatePreparedAuth(ctx, base, updated)
	if errMint != nil {
		t.Fatalf("UpdatePreparedAuth failed: %v", errMint)
	}
	if !reentrantUpdated {
		t.Fatal("expected reentrant update to have executed")
	}

	current, ok := manager.GetByID(base.ID)
	if !ok || current == nil {
		t.Fatal("expected current auth to exist")
	}
	if current.Metadata["hook_field"] != "hook-updated-value" {
		t.Fatalf("hook_field = %v, want hook-updated-value", current.Metadata["hook_field"])
	}
	if current.Metadata["api_key"] != "minted-key-1" {
		t.Fatalf("api_key = %v, want minted-key-1", current.Metadata["api_key"])
	}

	// 2. Reentrant DeleteAuths inside OnAuthUpdated must not deadlock
	reentrantDeleted := false
	hook.onUpdated = func(hCtx context.Context, a *Auth) {
		if a.ID == base.ID && a.Metadata["api_key"] == "minted-key-2" && !reentrantDeleted {
			reentrantDeleted = true
			errDel := manager.DeleteAuths(hCtx, []string{a.ID}, func(dCtx context.Context) error {
				return store.Delete(dCtx, a.ID)
			})
			if errDel != nil {
				t.Errorf("reentrant manager.DeleteAuths failed: %v", errDel)
			}
		}
	}

	updated2 := current.Clone()
	updated2.Metadata["api_key"] = "minted-key-2"
	_, errMint2 := manager.UpdatePreparedAuth(ctx, current, updated2)
	if errMint2 != nil {
		t.Fatalf("UpdatePreparedAuth 2 failed: %v", errMint2)
	}
	if !reentrantDeleted {
		t.Fatal("expected reentrant delete to have executed")
	}
	if _, okDeleted := manager.GetByID(base.ID); okDeleted {
		t.Fatal("expected auth to be deleted by reentrant DeleteAuths")
	}
}
