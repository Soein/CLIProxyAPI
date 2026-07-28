package auth

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type haCaptureHook struct {
	NoopHook
	registered []string
	updated    []string
	removed    []string
}

type haCountingIndexedStore struct {
	*fakeIndexedStore
	saves int
}

func (s *haCountingIndexedStore) Save(context.Context, *Auth) (string, error) {
	s.saves++
	return "", nil
}

func (h *haCaptureHook) OnAuthRegistered(_ context.Context, auth *Auth) {
	h.registered = append(h.registered, auth.ID)
}

func (h *haCaptureHook) OnAuthUpdated(_ context.Context, auth *Auth) {
	h.updated = append(h.updated, auth.ID)
}

func (h *haCaptureHook) OnAuthRemoved(_ context.Context, authID string) {
	h.removed = append(h.removed, authID)
}

func TestReloadByID_MergesPersistedCredentialsWithLocalRuntimeState(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	inner := &fakeStore{all: map[string]*Auth{
		"shared": {
			ID:         "shared",
			Provider:   "codex",
			Label:      "persisted-new-label",
			Attributes: map[string]string{"credential": "new"},
			Metadata:   map[string]any{"access_token": "new-token"},
			Status:     StatusActive,
		},
	}}
	store := &haCountingIndexedStore{fakeIndexedStore: &fakeIndexedStore{fakeStore: inner}}
	hook := &haCaptureHook{}
	manager := NewManager(store, &RoundRobinSelector{}, hook)
	local := &Auth{
		ID:             "shared",
		Provider:       "codex",
		Label:          "old-label",
		Index:          "local-index",
		Status:         StatusError,
		StatusMessage:  "local cooldown",
		Unavailable:    true,
		NextRetryAfter: now.Add(time.Minute),
		Quota:          QuotaState{Exceeded: true, BackoffLevel: 4, NextRecoverAt: now.Add(time.Minute)},
		LastError:      &Error{Code: "rate_limit", Message: "local failure"},
		ModelStates: map[string]*ModelState{
			"gpt-5": {Status: StatusError, Unavailable: true, NextRetryAfter: now.Add(2 * time.Minute)},
		},
		Runtime: struct{ Name string }{Name: "local-runtime"},
		Success: 11,
		Failed:  7,
	}
	local.indexAssigned = true
	local.recordRecentRequest(now, true)
	manager.mu.Lock()
	manager.auths[local.ID] = local.Clone()
	manager.mu.Unlock()

	if err := manager.ReloadByID(context.Background(), local.ID); err != nil {
		t.Fatalf("ReloadByID() error = %v", err)
	}
	reloaded, ok := manager.GetByID(local.ID)
	if !ok || reloaded == nil {
		t.Fatal("ReloadByID() removed auth unexpectedly")
	}
	if reloaded.Label != "persisted-new-label" || reloaded.Attributes["credential"] != "new" || reloaded.Metadata["access_token"] != "new-token" {
		t.Fatalf("persisted credential changes not applied: %#v", reloaded)
	}
	if reloaded.Index != "local-index" || !reloaded.indexAssigned {
		t.Fatalf("runtime index = (%q, %v), want (local-index, true)", reloaded.Index, reloaded.indexAssigned)
	}
	if reloaded.Status != StatusError || !reloaded.Unavailable || !reloaded.NextRetryAfter.Equal(local.NextRetryAfter) || reloaded.Quota.BackoffLevel != 4 {
		t.Fatalf("top-level cooldown state not preserved: %#v", reloaded)
	}
	if state := reloaded.ModelStates["gpt-5"]; state == nil || !state.Unavailable || !state.NextRetryAfter.Equal(local.ModelStates["gpt-5"].NextRetryAfter) {
		t.Fatalf("model cooldown state not preserved: %#v", reloaded.ModelStates)
	}
	if reloaded.Success != 11 || reloaded.Failed != 7 || reloaded.Runtime == nil {
		t.Fatalf("local counters/runtime not preserved: success=%d failed=%d runtime=%#v", reloaded.Success, reloaded.Failed, reloaded.Runtime)
	}
	recent := reloaded.RecentRequestsSnapshot(now)
	if len(recent) == 0 || recent[len(recent)-1].Success != 1 {
		t.Fatalf("recent request state not preserved: %#v", recent)
	}
	if len(hook.updated) != 1 || hook.updated[0] != local.ID || len(hook.registered) != 0 {
		t.Fatalf("ReloadByID() hooks = registered:%v updated:%v", hook.registered, hook.updated)
	}
	if store.saves != 0 {
		t.Fatalf("ReloadByID() persisted reconciled auth %d time(s), want 0", store.saves)
	}
}

func TestLoad_MergesLocalRuntimeStateDuringFullReconciliation(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	store := &fakeStore{all: map[string]*Auth{
		"shared": {
			ID:         "shared",
			Provider:   "codex",
			Label:      "persisted-label",
			Attributes: map[string]string{"credential": "new"},
			Status:     StatusActive,
		},
	}}
	hook := &haCaptureHook{}
	manager := NewManager(store, nil, hook)
	local := &Auth{
		ID:             "shared",
		Provider:       "codex",
		Index:          "local-index",
		Status:         StatusError,
		Unavailable:    true,
		NextRetryAfter: now.Add(time.Minute),
		ModelStates: map[string]*ModelState{
			"gpt-5": {Status: StatusError, Unavailable: true, NextRetryAfter: now.Add(2 * time.Minute)},
		},
		Success: 5,
		Failed:  3,
	}
	local.indexAssigned = true
	local.recordRecentRequest(now, false)
	manager.mu.Lock()
	manager.auths[local.ID] = local.Clone()
	manager.mu.Unlock()

	if err := manager.Load(context.Background()); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	loaded, ok := manager.GetByID(local.ID)
	if !ok || loaded == nil {
		t.Fatal("Load() removed shared auth")
	}
	if loaded.Label != "persisted-label" || loaded.Attributes["credential"] != "new" {
		t.Fatalf("persisted fields not applied: %#v", loaded)
	}
	if loaded.Index != "local-index" || loaded.Status != StatusError || !loaded.Unavailable || !loaded.NextRetryAfter.Equal(local.NextRetryAfter) {
		t.Fatalf("local runtime state not preserved: %#v", loaded)
	}
	if loaded.Success != 5 || loaded.Failed != 3 || loaded.ModelStates["gpt-5"] == nil {
		t.Fatalf("local counters/model state not preserved: %#v", loaded)
	}
	if len(hook.updated) != 1 || hook.updated[0] != local.ID || len(hook.registered) != 0 {
		t.Fatalf("Load() hooks = registered:%v updated:%v", hook.registered, hook.updated)
	}
}

func TestLoad_PreservesExplicitlyNonPersistentAuths(t *testing.T) {
	store := &fakeStore{all: map[string]*Auth{}}
	hook := &haCaptureHook{}
	manager := NewManager(store, nil, hook)
	configAPIKey := &Auth{
		ID:       "config-api-key",
		Provider: "xai",
		Attributes: map[string]string{
			AttributeAPIKey:   "secret",
			AttributeAuthKind: AuthKindAPIKey,
			AttributeSource:   "config:xai[0]",
		},
	}
	runtimeOnly := &Auth{
		ID:       "runtime-only",
		Provider: "xai",
		Attributes: map[string]string{
			AttributeRuntimeOnly: "true",
		},
	}
	pluginVirtual := &Auth{ID: "plugin-virtual", Provider: "plugin"}
	MarkPluginVirtualAuth(pluginVirtual, "plugin.json", 0)
	manager.mu.Lock()
	for _, auth := range []*Auth{configAPIKey, runtimeOnly, pluginVirtual, {ID: "persisted-missing", Provider: "codex"}} {
		manager.auths[auth.ID] = auth.Clone()
	}
	manager.mu.Unlock()

	if err := manager.Load(context.Background()); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	for _, id := range []string{configAPIKey.ID, runtimeOnly.ID, pluginVirtual.ID} {
		if _, ok := manager.GetByID(id); !ok {
			t.Fatalf("Load() removed explicitly non-persistent auth %q", id)
		}
	}
	if _, ok := manager.GetByID("persisted-missing"); ok {
		t.Fatal("Load() retained ordinary auth missing from store")
	}
	if len(hook.removed) != 1 || hook.removed[0] != "persisted-missing" {
		t.Fatalf("Load() removed hooks = %v, want [persisted-missing]", hook.removed)
	}
}

type blockingListStore struct {
	started chan struct{}
	release chan struct{}
}

func (s *blockingListStore) List(context.Context) ([]*Auth, error) {
	close(s.started)
	<-s.release
	return []*Auth{{ID: "shared", Provider: "codex"}}, nil
}

type blockingGetByIDStore struct {
	started chan struct{}
	release chan struct{}
	fetched *Auth
}

func (*blockingGetByIDStore) List(context.Context) ([]*Auth, error) { return nil, nil }
func (s *blockingGetByIDStore) GetByID(context.Context, string) (*Auth, error) {
	close(s.started)
	<-s.release
	return s.fetched.Clone(), nil
}
func (*blockingGetByIDStore) Save(context.Context, *Auth) (string, error) { return "", nil }
func (*blockingGetByIDStore) Delete(context.Context, string) error        { return nil }

func TestReloadByID_DoesNotOverwriteMutationCompletedDuringFetch(t *testing.T) {
	store := &blockingGetByIDStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
		fetched: &Auth{ID: "shared", Provider: "codex", Label: "stale-store"},
	}
	manager := NewManager(store, nil, nil)
	manager.mu.Lock()
	manager.auths["shared"] = &Auth{ID: "shared", Provider: "codex", Label: "before", revision: 1}
	manager.authRevision = 1
	manager.mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- manager.ReloadByID(context.Background(), "shared") }()
	<-store.started
	if _, err := manager.Update(WithSkipPersist(context.Background()), &Auth{ID: "shared", Provider: "codex", Label: "concurrent"}); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	close(store.release)
	if err := <-done; err != nil {
		t.Fatalf("ReloadByID() error = %v", err)
	}
	loaded, ok := manager.GetByID("shared")
	if !ok || loaded.Label != "concurrent" {
		t.Fatalf("ReloadByID() overwrote concurrent mutation: %#v", loaded)
	}
}

type blockingVersionedTombstoneStore struct {
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	reads   int
}

func (s *blockingVersionedTombstoneStore) List(context.Context) ([]*Auth, error) {
	return nil, nil
}

func (s *blockingVersionedTombstoneStore) GetByID(context.Context, string) (*Auth, error) {
	s.mu.Lock()
	s.reads++
	read := s.reads
	s.mu.Unlock()
	if read == 1 {
		close(s.started)
		<-s.release
	}
	return nil, nil
}

func (*blockingVersionedTombstoneStore) Save(context.Context, *Auth) (string, error) {
	return "", nil
}

func (*blockingVersionedTombstoneStore) SaveVersioned(context.Context, *Auth, uint64) (string, uint64, error) {
	return "", 0, nil
}

func (*blockingVersionedTombstoneStore) Delete(context.Context, string) error { return nil }

func TestReloadByID_RetriesAuthoritativeTombstoneAfterSkipPersistUpdate(t *testing.T) {
	store := &blockingVersionedTombstoneStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := NewManager(store, nil, nil)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{
		ID:       "shared",
		Provider: "codex",
		Label:    "stale-local",
	}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}

	done := make(chan error, 1)
	go func() { done <- manager.ReloadByID(context.Background(), "shared") }()
	<-store.started
	if _, errUpdate := manager.Update(WithSkipPersist(context.Background()), &Auth{
		ID:       "shared",
		Provider: "codex",
		Label:    "skip-persist-update",
	}); errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	close(store.release)
	if errReload := <-done; errReload != nil {
		t.Fatalf("ReloadByID() error = %v", errReload)
	}
	if _, exists := manager.GetByID("shared"); exists {
		t.Fatal("ReloadByID() retained auth after authoritative tombstone")
	}
	store.mu.Lock()
	reads := store.reads
	store.mu.Unlock()
	if reads < 2 {
		t.Fatalf("GetByID() reads = %d, want retry after skip-persist update", reads)
	}
}

func TestReloadByID_AppliesExternalCredentialAfterRepeatedVolatileMutationsDuringFetch(t *testing.T) {
	store := &blockingGetByIDStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
		fetched: &Auth{
			ID:       "shared",
			Provider: "codex",
			Label:    "external",
			Metadata: map[string]any{"access_token": "external-token"},
		},
	}
	manager := NewManager(store, nil, nil)
	runtimeValue := &struct{ Name string }{Name: "local-runtime"}
	if _, err := manager.Register(WithSkipPersist(context.Background()), &Auth{
		ID:       "shared",
		Provider: "codex",
		Label:    "local",
		Metadata: map[string]any{"access_token": "local-token"},
		Runtime:  runtimeValue,
	}); err != nil {
		t.Fatalf("Register(seed) error = %v", err)
	}
	baseline, _ := manager.GetByID("shared")

	reloadDone := make(chan error, 1)
	go func() { reloadDone <- manager.ReloadByID(context.Background(), "shared") }()
	<-store.started

	const resultCount = 16
	lastRevision := baseline.revision
	for index := 0; index < resultCount; index++ {
		manager.MarkResult(WithSkipPersist(context.Background()), Result{AuthID: "shared", Provider: "codex", Success: true})
		current, ok := manager.GetByID("shared")
		if !ok || current.revision <= lastRevision {
			t.Fatalf("MarkResult(%d) revision = %d, want > %d", index, current.revision, lastRevision)
		}
		if current.durableRevision != baseline.durableRevision {
			t.Fatalf("MarkResult(%d) durable revision = %d, want %d", index, current.durableRevision, baseline.durableRevision)
		}
		lastRevision = current.revision
	}

	close(store.release)
	if err := <-reloadDone; err != nil {
		t.Fatalf("ReloadByID() error = %v", err)
	}
	reloaded, ok := manager.GetByID("shared")
	if !ok || reloaded.Label != "external" || reloaded.Metadata["access_token"] != "external-token" {
		t.Fatalf("ReloadByID() did not apply external credentials: %#v", reloaded)
	}
	if reloaded.Success != resultCount || reloaded.Runtime != runtimeValue {
		t.Fatalf("ReloadByID() runtime state = success:%d runtime:%#v, want success:%d runtime:%#v", reloaded.Success, reloaded.Runtime, resultCount, runtimeValue)
	}
	if reloaded.durableRevision <= baseline.durableRevision {
		t.Fatalf("ReloadByID() durable revision = %d, want > %d", reloaded.durableRevision, baseline.durableRevision)
	}
}

type authoritativeListTestStore struct {
	listCalls          int
	authoritativeCalls int
}

type blockingAuthoritativeListStore struct {
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	saves   []*Auth
}

type reversePersistenceStore struct {
	mu          sync.Mutex
	stored      *Auth
	saveStarted chan struct{}
	releaseSave chan struct{}
	readStarted chan struct{}
	releaseRead chan struct{}
	saveErr     error
	saveOnce    sync.Once
	readOnce    sync.Once
}

func newReversePersistenceStore(stored *Auth) *reversePersistenceStore {
	return &reversePersistenceStore{
		stored:      stored.Clone(),
		saveStarted: make(chan struct{}),
		releaseSave: make(chan struct{}),
		readStarted: make(chan struct{}),
		releaseRead: make(chan struct{}),
	}
}

func (s *reversePersistenceStore) staleSnapshot() *Auth {
	s.mu.Lock()
	snapshot := s.stored.Clone()
	s.mu.Unlock()
	s.readOnce.Do(func() { close(s.readStarted) })
	<-s.releaseRead
	return snapshot
}

func (s *reversePersistenceStore) List(context.Context) ([]*Auth, error) {
	return s.ListAuthoritative(context.Background())
}

func (s *reversePersistenceStore) ListAuthoritative(context.Context) ([]*Auth, error) {
	snapshot := s.staleSnapshot()
	if snapshot == nil {
		return nil, nil
	}
	return []*Auth{snapshot}, nil
}

func (s *reversePersistenceStore) GetByID(context.Context, string) (*Auth, error) {
	return s.staleSnapshot(), nil
}

func (s *reversePersistenceStore) Save(_ context.Context, auth *Auth) (string, error) {
	s.saveOnce.Do(func() { close(s.saveStarted) })
	<-s.releaseSave
	if s.saveErr != nil {
		return "", s.saveErr
	}
	s.mu.Lock()
	s.stored = auth.Clone()
	s.mu.Unlock()
	return "", nil
}

func (*reversePersistenceStore) Delete(context.Context, string) error { return nil }

func assertPersistenceMarker(t *testing.T, manager *Manager, id string, revision uint64, want bool) {
	t.Helper()
	manager.mu.RLock()
	markedRevision, marked := manager.persistenceInFlightRevisions[id]
	manager.mu.RUnlock()
	if marked != want || (want && markedRevision != revision) {
		t.Fatalf("persistence marker for %q = (%d, %v), want (%d, %v)", id, markedRevision, marked, revision, want)
	}
}

func TestPersistenceMarkerOlderRevisionCannotClearNewer(t *testing.T) {
	manager := NewManager(&fakeStore{}, nil, nil)
	manager.mu.Lock()
	manager.markPersistenceInFlightLocked(context.Background(), &Auth{ID: "shared", durableRevision: 10, Metadata: map[string]any{}})
	manager.markPersistenceInFlightLocked(context.Background(), &Auth{ID: "shared", durableRevision: 11, Metadata: map[string]any{}})
	manager.mu.Unlock()

	manager.clearPersistenceInFlight("shared", 10)
	assertPersistenceMarker(t, manager, "shared", 11, true)
	manager.clearPersistenceInFlight("shared", 11)
	assertPersistenceMarker(t, manager, "shared", 11, false)
}

func (*blockingAuthoritativeListStore) List(context.Context) ([]*Auth, error) {
	return nil, nil
}

func (s *blockingAuthoritativeListStore) ListAuthoritative(context.Context) ([]*Auth, error) {
	close(s.started)
	<-s.release
	return []*Auth{{ID: "shared", Provider: "codex", Label: "stale-store"}}, nil
}

func (s *blockingAuthoritativeListStore) Save(_ context.Context, auth *Auth) (string, error) {
	s.mu.Lock()
	s.saves = append(s.saves, auth.Clone())
	s.mu.Unlock()
	return "", nil
}

func (*blockingAuthoritativeListStore) Delete(context.Context, string) error { return nil }

func (s *blockingAuthoritativeListStore) savedAuths() []*Auth {
	s.mu.Lock()
	defer s.mu.Unlock()
	saved := make([]*Auth, 0, len(s.saves))
	for _, auth := range s.saves {
		saved = append(saved, auth.Clone())
	}
	return saved
}

func waitForAuthLabel(t *testing.T, manager *Manager, id, label string) *Auth {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if auth, ok := manager.GetByID(id); ok && auth.Label == label {
			return auth
		}
		time.Sleep(time.Millisecond)
	}
	auth, _ := manager.GetByID(id)
	t.Fatalf("auth %q did not reach label %q: %#v", id, label, auth)
	return nil
}

func (s *authoritativeListTestStore) List(context.Context) ([]*Auth, error) {
	s.listCalls++
	return []*Auth{{ID: "stale", Provider: "codex"}}, nil
}

func (s *authoritativeListTestStore) ListAuthoritative(context.Context) ([]*Auth, error) {
	s.authoritativeCalls++
	return []*Auth{{ID: "writer", Provider: "codex"}}, nil
}

func (*authoritativeListTestStore) Save(context.Context, *Auth) (string, error) { return "", nil }
func (*authoritativeListTestStore) Delete(context.Context, string) error        { return nil }

func TestLoad_UsesNormalStoreSnapshot(t *testing.T) {
	store := &authoritativeListTestStore{}
	manager := NewManager(store, nil, nil)
	if err := manager.Load(context.Background()); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if store.authoritativeCalls != 0 || store.listCalls != 1 {
		t.Fatalf("Load() calls = authoritative:%d list:%d, want 0/1", store.authoritativeCalls, store.listCalls)
	}
	if _, ok := manager.GetByID("stale"); !ok {
		t.Fatal("Load() did not apply normal read snapshot")
	}
}

func TestReconcile_PrefersAuthoritativeStoreSnapshot(t *testing.T) {
	store := &authoritativeListTestStore{}
	manager := NewManager(store, nil, nil)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "stale", Provider: "codex"}); errRegister != nil {
		t.Fatalf("Register(stale) error = %v", errRegister)
	}
	staleSchedulerSnapshot, okStale := manager.GetByID("stale")
	if !okStale {
		t.Fatal("GetByID(stale) found = false")
	}
	if err := manager.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if store.authoritativeCalls != 1 || store.listCalls != 0 {
		t.Fatalf("Reconcile() calls = authoritative:%d list:%d, want 1/0", store.authoritativeCalls, store.listCalls)
	}
	if _, ok := manager.GetByID("writer"); !ok {
		t.Fatal("Reconcile() did not apply writer-backed snapshot")
	}
	if _, ok := manager.GetByID("stale"); ok {
		t.Fatal("Reconcile() applied stale read-replica snapshot")
	}
	manager.schedulerUpsert(staleSchedulerSnapshot)
	manager.scheduler.mu.Lock()
	_, scheduled := manager.scheduler.authProviders["stale"]
	version := manager.scheduler.authVersions["stale"]
	manager.scheduler.mu.Unlock()
	if scheduled {
		t.Fatal("Reconcile() allowed a delayed old scheduler snapshot to restore removed auth")
	}
	if !version.disabled || version.revision <= staleSchedulerSnapshot.revision {
		t.Fatalf("Reconcile() scheduler tombstone = %#v, want disabled revision > %d", version, staleSchedulerSnapshot.revision)
	}
}

func TestReconcile_DoesNotRestoreAuthRemovedDuringAuthoritativeList(t *testing.T) {
	store := &blockingAuthoritativeListStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := NewManager(store, nil, nil)
	if _, err := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "shared", Provider: "codex", Label: "before"}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- manager.Reconcile(context.Background()) }()
	<-store.started
	manager.Remove(context.Background(), "shared")
	close(store.release)
	if err := <-done; err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if restored, ok := manager.GetByID("shared"); ok {
		t.Fatalf("Reconcile() restored auth removed during authoritative List: %#v", restored)
	}
}

func TestReconcile_PreservesConcurrentUpdateRevisionUntilPersisted(t *testing.T) {
	store := &blockingAuthoritativeListStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := NewManager(store, nil, nil)
	if _, err := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "shared", Provider: "codex", Label: "before"}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	unlockPersistence := manager.lockAuthPersistence([]string{"shared"})
	unlocked := false
	defer func() {
		if !unlocked {
			unlockPersistence()
		}
	}()

	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- manager.Reconcile(context.Background()) }()
	<-store.started

	updateDone := make(chan error, 1)
	go func() {
		_, err := manager.Update(context.Background(), &Auth{
			ID:       "shared",
			Provider: "codex",
			Label:    "concurrent-update",
			Metadata: map[string]any{"access_token": "updated"},
		})
		updateDone <- err
	}()
	winning := waitForAuthLabel(t, manager, "shared", "concurrent-update")

	close(store.release)
	if err := <-reconcileDone; err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	unlockPersistence()
	unlocked = true
	if err := <-updateDone; err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	current, ok := manager.GetByID("shared")
	if !ok || current.Label != "concurrent-update" || current.Metadata["access_token"] != "updated" {
		t.Fatalf("Reconcile() did not preserve concurrent update: %#v", current)
	}
	if current.revision != winning.revision {
		t.Fatalf("Reconcile() changed concurrent update revision from %d to %d", winning.revision, current.revision)
	}
	saved := store.savedAuths()
	if len(saved) != 1 || saved[0].ID != "shared" || saved[0].Label != "concurrent-update" || saved[0].revision != winning.revision || saved[0].Metadata["access_token"] != "updated" {
		t.Fatalf("saved auths = %#v, want one concurrent update", saved)
	}
}

func TestReconcile_PreservesConcurrentRegisterRevisionUntilPersisted(t *testing.T) {
	store := &blockingAuthoritativeListStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := NewManager(store, nil, nil)

	unlockPersistence := manager.lockAuthPersistence([]string{"new-auth"})
	unlocked := false
	defer func() {
		if !unlocked {
			unlockPersistence()
		}
	}()

	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- manager.Reconcile(context.Background()) }()
	<-store.started

	registerDone := make(chan error, 1)
	go func() {
		_, err := manager.Register(context.Background(), &Auth{
			ID:       "new-auth",
			Provider: "codex",
			Label:    "concurrent-register",
			Metadata: map[string]any{"access_token": "new"},
		})
		registerDone <- err
	}()
	winning := waitForAuthLabel(t, manager, "new-auth", "concurrent-register")

	close(store.release)
	if err := <-reconcileDone; err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	unlockPersistence()
	unlocked = true
	if err := <-registerDone; err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	current, ok := manager.GetByID("new-auth")
	if !ok || current.Label != "concurrent-register" || current.Metadata["access_token"] != "new" {
		t.Fatalf("Reconcile() did not preserve concurrent register: %#v", current)
	}
	if current.revision != winning.revision {
		t.Fatalf("Reconcile() changed concurrent register revision from %d to %d", winning.revision, current.revision)
	}
	saved := store.savedAuths()
	if len(saved) != 1 || saved[0].ID != "new-auth" || saved[0].Label != "concurrent-register" || saved[0].revision != winning.revision || saved[0].Metadata["access_token"] != "new" {
		t.Fatalf("saved auths = %#v, want one concurrent register", saved)
	}
}

func TestReconcile_PreservesRegisterPublishedBeforeStoreReadUntilPersistCompletes(t *testing.T) {
	store := newReversePersistenceStore(nil)
	manager := NewManager(store, nil, nil)

	registerDone := make(chan error, 1)
	go func() {
		_, err := manager.Register(context.Background(), &Auth{
			ID:       "new-auth",
			Provider: "codex",
			Label:    "local-register",
			Metadata: map[string]any{"access_token": "new"},
		})
		registerDone <- err
	}()
	<-store.saveStarted
	published := waitForAuthLabel(t, manager, "new-auth", "local-register")
	assertPersistenceMarker(t, manager, "new-auth", published.durableRevision, true)

	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- manager.Reconcile(context.Background()) }()
	<-store.readStarted

	// The authoritative read has captured the missing pre-save row. Complete
	// persistence and clear the marker before allowing that stale read to apply.
	close(store.releaseSave)
	if err := <-registerDone; err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	assertPersistenceMarker(t, manager, "new-auth", published.durableRevision, false)
	close(store.releaseRead)
	if err := <-reconcileDone; err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	current, ok := manager.GetByID("new-auth")
	if !ok || current.Label != "local-register" || current.revision != published.revision {
		t.Fatalf("Reconcile() replaced in-flight register: %#v", current)
	}
}

func TestReconcile_PreservesUpdatePublishedBeforeStoreReadWhenPersistFails(t *testing.T) {
	store := newReversePersistenceStore(&Auth{
		ID:       "shared",
		Provider: "codex",
		Label:    "stale-store",
		Metadata: map[string]any{"access_token": "old"},
	})
	persistErr := errors.New("save failed")
	store.saveErr = persistErr
	manager := NewManager(store, nil, nil)
	if _, err := manager.Register(WithSkipPersist(context.Background()), store.stored.Clone()); err != nil {
		t.Fatalf("Register(seed) error = %v", err)
	}

	updateDone := make(chan error, 1)
	go func() {
		_, err := manager.Update(context.Background(), &Auth{
			ID:       "shared",
			Provider: "codex",
			Label:    "local-update",
			Metadata: map[string]any{"access_token": "new"},
		})
		updateDone <- err
	}()
	<-store.saveStarted
	published := waitForAuthLabel(t, manager, "shared", "local-update")
	assertPersistenceMarker(t, manager, "shared", published.durableRevision, true)

	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- manager.Reconcile(context.Background()) }()
	<-store.readStarted
	close(store.releaseSave)
	if err := <-updateDone; !errors.Is(err, persistErr) {
		t.Fatalf("Update() error = %v, want errors.Is(_, %v)", err, persistErr)
	}
	assertPersistenceMarker(t, manager, "shared", published.durableRevision, false)
	close(store.releaseRead)
	if err := <-reconcileDone; err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	current, ok := manager.GetByID("shared")
	if !ok || current.Label != "local-update" || current.Metadata["access_token"] != "new" || current.revision != published.revision {
		t.Fatalf("Reconcile() replaced failed in-flight update: %#v", current)
	}
}

func TestReconcile_PreservesUpdatePublishedBeforeStoreReadAfterPersistSucceeds(t *testing.T) {
	store := newReversePersistenceStore(&Auth{
		ID:       "shared",
		Provider: "codex",
		Label:    "stale-store",
		Metadata: map[string]any{"access_token": "old"},
	})
	manager := NewManager(store, nil, nil)
	if _, err := manager.Register(WithSkipPersist(context.Background()), store.stored.Clone()); err != nil {
		t.Fatalf("Register(seed) error = %v", err)
	}

	updateDone := make(chan error, 1)
	go func() {
		_, err := manager.Update(context.Background(), &Auth{
			ID:       "shared",
			Provider: "codex",
			Label:    "local-update",
			Metadata: map[string]any{"access_token": "new"},
		})
		updateDone <- err
	}()
	<-store.saveStarted
	published := waitForAuthLabel(t, manager, "shared", "local-update")
	assertPersistenceMarker(t, manager, "shared", published.durableRevision, true)

	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- manager.Reconcile(context.Background()) }()
	<-store.readStarted
	close(store.releaseSave)
	if err := <-updateDone; err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	assertPersistenceMarker(t, manager, "shared", published.durableRevision, false)
	close(store.releaseRead)
	if err := <-reconcileDone; err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	current, ok := manager.GetByID("shared")
	if !ok || current.Label != "local-update" || current.Metadata["access_token"] != "new" || current.durableRevision != published.durableRevision {
		t.Fatalf("Reconcile() replaced persisted update: %#v", current)
	}
	store.mu.Lock()
	persisted := store.stored.Clone()
	store.mu.Unlock()
	if persisted == nil || persisted.Label != "local-update" || persisted.Metadata["access_token"] != "new" {
		t.Fatalf("persisted auth = %#v, want successful local update", persisted)
	}
}

func TestReloadByID_PreservesRevisionPendingWhenStoreReadStarted(t *testing.T) {
	store := newReversePersistenceStore(&Auth{
		ID:       "shared",
		Provider: "codex",
		Label:    "stale-store",
		Metadata: map[string]any{"access_token": "old"},
	})
	manager := NewManager(store, nil, nil)
	if _, err := manager.Register(WithSkipPersist(context.Background()), store.stored.Clone()); err != nil {
		t.Fatalf("Register(seed) error = %v", err)
	}

	updateDone := make(chan error, 1)
	go func() {
		_, err := manager.Update(context.Background(), &Auth{
			ID:       "shared",
			Provider: "codex",
			Label:    "local-update",
			Metadata: map[string]any{"access_token": "new"},
		})
		updateDone <- err
	}()
	<-store.saveStarted
	published := waitForAuthLabel(t, manager, "shared", "local-update")

	reloadDone := make(chan error, 1)
	go func() { reloadDone <- manager.ReloadByID(context.Background(), "shared") }()
	<-store.readStarted
	close(store.releaseSave)
	if err := <-updateDone; err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	assertPersistenceMarker(t, manager, "shared", published.durableRevision, false)
	close(store.releaseRead)
	if err := <-reloadDone; err != nil {
		t.Fatalf("ReloadByID() error = %v", err)
	}

	current, ok := manager.GetByID("shared")
	if !ok || current.Label != "local-update" || current.Metadata["access_token"] != "new" || current.durableRevision < published.durableRevision {
		t.Fatalf("ReloadByID() replaced in-flight update: %#v", current)
	}
}

type committedThenBlockedVersionedStore struct {
	mu                sync.Mutex
	stored            *Auth
	saveCommitted     chan struct{}
	releaseSaveReturn chan struct{}
}

func (s *committedThenBlockedVersionedStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stored == nil {
		return nil, nil
	}
	return []*Auth{s.stored.Clone()}, nil
}

func (s *committedThenBlockedVersionedStore) GetByID(context.Context, string) (*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stored == nil {
		return nil, nil
	}
	return s.stored.Clone(), nil
}

func (s *committedThenBlockedVersionedStore) Save(ctx context.Context, auth *Auth) (string, error) {
	path, _, errSave := s.SaveVersioned(ctx, auth, auth.StoreGeneration())
	return path, errSave
}

func (s *committedThenBlockedVersionedStore) SaveVersioned(_ context.Context, auth *Auth, expected uint64) (string, uint64, error) {
	s.mu.Lock()
	if s.stored == nil || s.stored.StoreGeneration() != expected {
		s.mu.Unlock()
		return "", 0, ErrAuthStoreConflict
	}
	committed := auth.Clone()
	committed.SetStoreGeneration(expected + 1)
	s.stored = committed
	s.mu.Unlock()
	close(s.saveCommitted)
	<-s.releaseSaveReturn
	return auth.ID, expected + 1, nil
}

func (*committedThenBlockedVersionedStore) Delete(context.Context, string) error { return nil }

func (s *committedThenBlockedVersionedStore) tombstone() {
	s.mu.Lock()
	s.stored = nil
	s.mu.Unlock()
}

func TestReloadByIDWaitsForLocalCommitThenAppliesExternalTombstone(t *testing.T) {
	seed := &Auth{
		ID:       "shared",
		Provider: "codex",
		Label:    "old",
		Metadata: map[string]any{"access_token": "old"},
	}
	seed.SetStoreGeneration(1)
	store := &committedThenBlockedVersionedStore{
		stored:            seed.Clone(),
		saveCommitted:     make(chan struct{}),
		releaseSaveReturn: make(chan struct{}),
	}
	manager := NewManager(store, nil, nil)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), seed.Clone()); errRegister != nil {
		t.Fatalf("Register(seed) error: %v", errRegister)
	}

	updateDone := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(context.Background(), &Auth{
			ID:       "shared",
			Provider: "codex",
			Label:    "locally-committed",
			Metadata: map[string]any{"access_token": "new"},
		})
		updateDone <- errUpdate
	}()
	<-store.saveCommitted
	store.tombstone()

	reloadDone := make(chan error, 1)
	go func() { reloadDone <- manager.ReloadByID(context.Background(), "shared") }()
	close(store.releaseSaveReturn)
	if errUpdate := <-updateDone; errUpdate != nil {
		t.Fatalf("Update() error: %v", errUpdate)
	}
	if errReload := <-reloadDone; errReload != nil {
		t.Fatalf("ReloadByID() error: %v", errReload)
	}
	if auth, exists := manager.GetByID("shared"); exists || auth != nil {
		t.Fatalf("external tombstone was swallowed while persistence settled: %#v", auth)
	}
}

func TestUpdateDurablePersistSurvivesResultRevisionAdvance(t *testing.T) {
	store := &blockingAuthoritativeListStore{}
	manager := NewManager(store, nil, nil)
	if _, err := manager.Register(WithSkipPersist(context.Background()), &Auth{
		ID:       "shared",
		Provider: "codex",
		Label:    "before",
		Metadata: map[string]any{"access_token": "old"},
	}); err != nil {
		t.Fatalf("Register(seed) error = %v", err)
	}

	unlockPersistence := manager.lockAuthPersistence([]string{"shared"})
	updateDone := make(chan error, 1)
	go func() {
		_, err := manager.Update(context.Background(), &Auth{
			ID:       "shared",
			Provider: "codex",
			Label:    "durable-update",
			Metadata: map[string]any{"access_token": "new"},
		})
		updateDone <- err
	}()
	published := waitForAuthLabel(t, manager, "shared", "durable-update")

	manager.MarkResult(WithSkipPersist(context.Background()), Result{AuthID: "shared", Provider: "codex", Success: true})
	afterResult, _ := manager.GetByID("shared")
	if afterResult.revision <= published.revision || afterResult.durableRevision != published.durableRevision {
		t.Fatalf("result revisions = runtime:%d durable:%d, want runtime > %d and durable %d", afterResult.revision, afterResult.durableRevision, published.revision, published.durableRevision)
	}

	unlockPersistence()
	if err := <-updateDone; err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	saved := store.savedAuths()
	if len(saved) != 1 || saved[0].Metadata["access_token"] != "new" || saved[0].Success != 1 || saved[0].revision != afterResult.revision {
		t.Fatalf("saved auths = %#v, want latest runtime snapshot with new token", saved)
	}
	assertPersistenceMarker(t, manager, "shared", published.durableRevision, false)
}

func TestReloadByID_AppliesExternalCredentialAcrossVolatileRevision(t *testing.T) {
	inner := &fakeStore{all: map[string]*Auth{
		"shared": {
			ID:       "shared",
			Provider: "codex",
			Label:    "external",
			Metadata: map[string]any{"access_token": "external-token"},
		},
	}}
	store := &fakeIndexedStore{fakeStore: inner}
	manager := NewManager(store, nil, nil)
	runtimeValue := &struct{ Name string }{Name: "local-runtime"}
	if _, err := manager.Register(WithSkipPersist(context.Background()), &Auth{
		ID:       "shared",
		Provider: "codex",
		Label:    "local",
		Metadata: map[string]any{"access_token": "local-token"},
		Runtime:  runtimeValue,
	}); err != nil {
		t.Fatalf("Register(seed) error = %v", err)
	}
	before, _ := manager.GetByID("shared")
	manager.MarkResult(WithSkipPersist(context.Background()), Result{AuthID: "shared", Provider: "codex", Success: true})
	afterResult, _ := manager.GetByID("shared")
	if afterResult.revision <= before.revision || afterResult.durableRevision != before.durableRevision {
		t.Fatalf("volatile result changed wrong generation: before=%#v after=%#v", before, afterResult)
	}

	if err := manager.ReloadByID(context.Background(), "shared"); err != nil {
		t.Fatalf("ReloadByID() error = %v", err)
	}
	reloaded, ok := manager.GetByID("shared")
	if !ok || reloaded.Label != "external" || reloaded.Metadata["access_token"] != "external-token" {
		t.Fatalf("ReloadByID() did not apply external credentials: %#v", reloaded)
	}
	if reloaded.Success != 1 || reloaded.Runtime != runtimeValue {
		t.Fatalf("ReloadByID() lost local runtime state: success=%d runtime=%#v", reloaded.Success, reloaded.Runtime)
	}
}

func TestEnableIgnoresVolatileRevisionAndPreservesRuntimeCounters(t *testing.T) {
	store := newReversePersistenceStore(&Auth{
		ID:       "shared",
		Provider: "codex",
		Disabled: true,
		Status:   StatusDisabled,
		Metadata: map[string]any{"access_token": "token", "disabled": true},
	})
	manager := NewManager(store, nil, nil)
	runtimeValue := &struct{ Name string }{Name: "runtime"}
	if _, err := manager.Register(WithSkipPersist(context.Background()), &Auth{
		ID:       "shared",
		Provider: "codex",
		Disabled: true,
		Status:   StatusDisabled,
		Metadata: map[string]any{"access_token": "token", "disabled": true},
		Runtime:  runtimeValue,
	}); err != nil {
		t.Fatalf("Register(seed) error = %v", err)
	}

	enableDone := make(chan error, 1)
	go func() {
		_, err := manager.SetDisabled(context.Background(), []string{"shared"}, false)
		enableDone <- err
	}()
	<-store.saveStarted

	resultDone := make(chan struct{})
	go func() {
		manager.MarkResult(WithSkipPersist(context.Background()), Result{AuthID: "shared", Provider: "codex", Success: true})
		close(resultDone)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		current, _ := manager.GetByID("shared")
		if current.Success == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("MarkResult() did not publish while enable persistence was blocked")
		}
		time.Sleep(time.Millisecond)
	}

	close(store.releaseSave)
	if err := <-enableDone; err != nil {
		t.Fatalf("SetDisabled(enable) error = %v", err)
	}
	<-resultDone
	current, ok := manager.GetByID("shared")
	if !ok || current.Disabled || current.Status == StatusDisabled {
		t.Fatalf("SetDisabled(enable) did not publish active auth: %#v", current)
	}
	if current.Success != 1 || current.Runtime != runtimeValue {
		t.Fatalf("SetDisabled(enable) lost runtime state: success=%d runtime=%#v", current.Success, current.Runtime)
	}
}

func (*blockingListStore) Save(context.Context, *Auth) (string, error) { return "", nil }
func (*blockingListStore) Delete(context.Context, string) error        { return nil }

func TestLoad_DoesNotHoldManagerLockDuringStoreList(t *testing.T) {
	store := &blockingListStore{started: make(chan struct{}), release: make(chan struct{})}
	manager := NewManager(store, nil, nil)
	manager.mu.Lock()
	manager.auths["shared"] = &Auth{ID: "shared", Provider: "codex"}
	manager.mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- manager.Load(context.Background()) }()
	<-store.started
	lookupDone := make(chan struct{})
	go func() {
		manager.GetByID("shared")
		close(lookupDone)
	}()
	select {
	case <-lookupDone:
	case <-time.After(time.Second):
		t.Fatal("GetByID blocked while Load waited for store.List")
	}
	close(store.release)
	if err := <-done; err != nil {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoad_DoesNotOverwriteMutationCompletedDuringStoreList(t *testing.T) {
	store := &blockingListStore{started: make(chan struct{}), release: make(chan struct{})}
	manager := NewManager(store, nil, nil)
	manager.mu.Lock()
	manager.auths["shared"] = &Auth{ID: "shared", Provider: "codex", Label: "before", revision: 1}
	manager.authRevision = 1
	manager.mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- manager.Load(context.Background()) }()
	<-store.started
	if _, err := manager.Update(WithSkipPersist(context.Background()), &Auth{ID: "shared", Provider: "codex", Label: "concurrent"}); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	close(store.release)
	if err := <-done; err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	loaded, ok := manager.GetByID("shared")
	if !ok || loaded.Label != "concurrent" {
		t.Fatalf("Load() overwrote concurrent mutation: %#v", loaded)
	}
}

func TestReloadByID_RemovalEmitsLifecycleHookWithoutPersisting(t *testing.T) {
	inner := &fakeStore{all: map[string]*Auth{}}
	store := &haCountingIndexedStore{fakeIndexedStore: &fakeIndexedStore{fakeStore: inner}}
	hook := &haCaptureHook{}
	manager := NewManager(store, nil, hook)
	manager.mu.Lock()
	manager.authRevision = 7
	manager.auths["removed"] = &Auth{ID: "removed", Provider: "codex", revision: 7}
	manager.homeRuntimeAuths["session"] = map[string]*Auth{"removed": {ID: "removed", Provider: "codex"}}
	staleSchedulerSnapshot := manager.auths["removed"].Clone()
	manager.mu.Unlock()

	if err := manager.ReloadByID(context.Background(), "removed"); err != nil {
		t.Fatalf("ReloadByID() error = %v", err)
	}
	if len(hook.removed) != 1 || hook.removed[0] != "removed" {
		t.Fatalf("ReloadByID() removed hooks = %v, want [removed]", hook.removed)
	}
	if _, ok := manager.GetExecutionSessionAuthByID("session", "removed"); ok {
		t.Fatal("ReloadByID() retained removed auth in Home session cache")
	}
	if store.saves != 0 {
		t.Fatalf("ReloadByID() persisted deletion reconciliation %d time(s), want 0", store.saves)
	}
	manager.schedulerUpsert(staleSchedulerSnapshot)
	manager.scheduler.mu.Lock()
	_, scheduled := manager.scheduler.authProviders["removed"]
	version := manager.scheduler.authVersions["removed"]
	manager.scheduler.mu.Unlock()
	if scheduled {
		t.Fatal("ReloadByID() allowed a delayed old scheduler snapshot to restore removed auth")
	}
	if !version.disabled || version.revision <= staleSchedulerSnapshot.revision {
		t.Fatalf("ReloadByID() scheduler tombstone = %#v, want disabled revision > %d", version, staleSchedulerSnapshot.revision)
	}
}

func TestReloadByID_MissingRuntimeStillAdvancesSchedulerDeletionWatermark(t *testing.T) {
	inner := &fakeStore{all: map[string]*Auth{}}
	store := &haCountingIndexedStore{fakeIndexedStore: &fakeIndexedStore{fakeStore: inner}}
	manager := NewManager(store, nil, nil)
	staleSchedulerSnapshot := &Auth{ID: "already-missing", Provider: "codex", revision: 9}
	manager.mu.Lock()
	manager.authRevision = staleSchedulerSnapshot.revision
	manager.mu.Unlock()
	manager.schedulerUpsert(staleSchedulerSnapshot)

	if errReload := manager.ReloadByID(context.Background(), staleSchedulerSnapshot.ID); errReload != nil {
		t.Fatalf("ReloadByID() error = %v", errReload)
	}
	manager.schedulerUpsert(staleSchedulerSnapshot)

	manager.scheduler.mu.Lock()
	_, scheduled := manager.scheduler.authProviders[staleSchedulerSnapshot.ID]
	version := manager.scheduler.authVersions[staleSchedulerSnapshot.ID]
	manager.scheduler.mu.Unlock()
	if scheduled {
		t.Fatal("ReloadByID() restored an already-missing auth from a delayed scheduler snapshot")
	}
	if !version.disabled || version.revision <= staleSchedulerSnapshot.revision {
		t.Fatalf("ReloadByID() scheduler tombstone = %#v, want disabled revision > %d", version, staleSchedulerSnapshot.revision)
	}
}
