package auth

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type disabledBatchSetter interface {
	SetDisabled(ctx context.Context, ids []string, disabled bool) ([]*Auth, error)
}

func requireDisabledBatchSetter(t *testing.T, manager *Manager) disabledBatchSetter {
	t.Helper()
	setter, ok := any(manager).(disabledBatchSetter)
	if !ok {
		t.Fatalf("Manager does not implement SetDisabled(context.Context, []string, bool) ([]*Auth, error)")
	}
	return setter
}

func TestManagerSetDisabledDisablesBatchAndRemovesItFromScheduling(t *testing.T) {
	const authCount = 256

	ctx := WithSkipPersist(context.Background())
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	disabledIDs := make([]string, 0, authCount-1)
	for index := 0; index < authCount; index++ {
		id := fmt.Sprintf("batch-auth-%03d", index)
		if _, errRegister := manager.Register(ctx, &Auth{ID: id, Provider: "gemini"}); errRegister != nil {
			t.Fatalf("Register(%q) error = %v", id, errRegister)
		}
		if index < authCount-1 {
			disabledIDs = append(disabledIDs, id)
		}
	}

	// Pick once before the mutation so the scheduler's provider/model shard is
	// materialized before the batch update.
	if _, errPick := manager.scheduler.pickSingle(ctx, "gemini", "", cliproxyexecutor.Options{}, nil); errPick != nil {
		t.Fatalf("scheduler.pickSingle() before SetDisabled error = %v", errPick)
	}

	updated, errSetDisabled := requireDisabledBatchSetter(t, manager).SetDisabled(ctx, disabledIDs, true)
	if errSetDisabled != nil {
		t.Fatalf("SetDisabled() error = %v", errSetDisabled)
	}
	if len(updated) != authCount-1 {
		t.Fatalf("len(SetDisabled() result) = %d, want %d", len(updated), authCount-1)
	}

	picked, errPick := manager.scheduler.pickSingle(ctx, "gemini", "", cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("scheduler.pickSingle() after SetDisabled error = %v", errPick)
	}
	if picked == nil || picked.ID != "batch-auth-255" {
		t.Fatalf("scheduler.pickSingle() after SetDisabled auth = %v, want batch-auth-255", picked)
	}
}

type disableBatchFailingStore struct {
	saveErr error
}

func (*disableBatchFailingStore) List(context.Context) ([]*Auth, error) { return nil, nil }

func (s *disableBatchFailingStore) Save(context.Context, *Auth) (string, error) {
	return "", s.saveErr
}

func (*disableBatchFailingStore) Delete(context.Context, string) error { return nil }

type disableFailureReloadStore struct {
	mu      sync.Mutex
	current *Auth
	saveErr error
}

func (s *disableFailureReloadStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		return nil, nil
	}
	return []*Auth{s.current.Clone()}, nil
}

func (s *disableFailureReloadStore) GetByID(_ context.Context, id string) (*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil || s.current.ID != id {
		return nil, nil
	}
	return s.current.Clone(), nil
}

func (s *disableFailureReloadStore) Save(_ context.Context, auth *Auth) (string, error) {
	if auth != nil && auth.Disabled && s.saveErr != nil {
		return "", s.saveErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.current = auth.Clone()
	return auth.ID, nil
}

func (*disableFailureReloadStore) Delete(context.Context, string) error { return nil }

type blockingDisableReloadStore struct {
	mu             sync.Mutex
	current        *Auth
	saveStarted    chan struct{}
	allowSave      chan struct{}
	fetchCaptured  chan struct{}
	releaseFetch   chan struct{}
	saveStartedOne sync.Once
	blockFetchOne  sync.Once
}

func newBlockingDisableReloadStore(auth *Auth) *blockingDisableReloadStore {
	return &blockingDisableReloadStore{
		current:       auth.Clone(),
		saveStarted:   make(chan struct{}),
		allowSave:     make(chan struct{}),
		fetchCaptured: make(chan struct{}),
		releaseFetch:  make(chan struct{}),
	}
}

func (s *blockingDisableReloadStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		return nil, nil
	}
	return []*Auth{s.current.Clone()}, nil
}

func (s *blockingDisableReloadStore) GetByID(_ context.Context, id string) (*Auth, error) {
	s.mu.Lock()
	var snapshot *Auth
	if s.current != nil && s.current.ID == id {
		snapshot = s.current.Clone()
	}
	s.mu.Unlock()
	s.blockFetchOne.Do(func() {
		close(s.fetchCaptured)
		<-s.releaseFetch
	})
	return snapshot, nil
}

func (s *blockingDisableReloadStore) Save(_ context.Context, auth *Auth) (string, error) {
	s.saveStartedOne.Do(func() { close(s.saveStarted) })
	<-s.allowSave
	s.mu.Lock()
	s.current = auth.Clone()
	s.mu.Unlock()
	return auth.ID, nil
}

func (*blockingDisableReloadStore) Delete(context.Context, string) error { return nil }

type enableRollbackFailureStore struct {
	mu                sync.Mutex
	current           *Auth
	failDisabled      bool
	rollbackErr       error
	fetchCaptured     chan struct{}
	releaseFetch      chan struct{}
	activeSaved       chan struct{}
	allowActiveReturn chan struct{}
	blockFetchOne     sync.Once
	activeSaveOne     sync.Once
}

func newEnableRollbackFailureStore(auth *Auth, rollbackErr error) *enableRollbackFailureStore {
	return &enableRollbackFailureStore{
		current:           auth.Clone(),
		rollbackErr:       rollbackErr,
		fetchCaptured:     make(chan struct{}),
		releaseFetch:      make(chan struct{}),
		activeSaved:       make(chan struct{}),
		allowActiveReturn: make(chan struct{}),
	}
}

func (s *enableRollbackFailureStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		return nil, nil
	}
	return []*Auth{s.current.Clone()}, nil
}

func (s *enableRollbackFailureStore) GetByID(_ context.Context, id string) (*Auth, error) {
	s.mu.Lock()
	var snapshot *Auth
	if s.current != nil && s.current.ID == id {
		snapshot = s.current.Clone()
	}
	s.mu.Unlock()
	s.blockFetchOne.Do(func() {
		close(s.fetchCaptured)
		<-s.releaseFetch
	})
	return snapshot, nil
}

func (s *enableRollbackFailureStore) Save(_ context.Context, auth *Auth) (string, error) {
	if auth.Disabled {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.failDisabled {
			return "", s.rollbackErr
		}
		s.current = auth.Clone()
		return auth.ID, nil
	}
	s.mu.Lock()
	s.current = auth.Clone()
	s.mu.Unlock()
	s.activeSaveOne.Do(func() { close(s.activeSaved) })
	<-s.allowActiveReturn
	return auth.ID, nil
}

func (*enableRollbackFailureStore) Delete(context.Context, string) error { return nil }

type reentrantUpdateHook struct {
	NoopHook
	mu        sync.Mutex
	manager   *Manager
	reentered bool
	done      chan error
}

func (h *reentrantUpdateHook) OnAuthUpdated(ctx context.Context, auth *Auth) {
	if auth == nil || auth.Disabled {
		return
	}
	h.mu.Lock()
	if h.reentered {
		h.mu.Unlock()
		return
	}
	h.reentered = true
	h.mu.Unlock()
	_, errUpdate := h.manager.Update(ctx, auth.Clone())
	h.done <- errUpdate
}

func TestManagerSetDisabledReturnsPersistenceErrorAndKeepsRuntimeDisabled(t *testing.T) {
	persistErr := errors.New("save disabled auth failed")
	manager := NewManager(&disableBatchFailingStore{saveErr: persistErr}, &RoundRobinSelector{}, nil)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "persist-failure", Provider: "gemini"}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}

	_, errSetDisabled := requireDisabledBatchSetter(t, manager).SetDisabled(context.Background(), []string{"persist-failure"}, true)
	if !errors.Is(errSetDisabled, persistErr) {
		t.Fatalf("SetDisabled() error = %v, want errors.Is(_, %v)", errSetDisabled, persistErr)
	}

	got, ok := manager.GetByID("persist-failure")
	if !ok {
		t.Fatalf("GetByID(%q) found = false", "persist-failure")
	}
	if !got.Disabled {
		t.Fatalf("GetByID(%q).Disabled = false, want true", "persist-failure")
	}
}

func TestManagerSetDisabledPersistenceFailureSurvivesReloadByID(t *testing.T) {
	persistErr := errors.New("save disabled auth failed")
	const authID = "disable-failure-reload"
	persistedActive := &Auth{
		ID:       authID,
		Provider: "gemini",
		Status:   StatusActive,
		Metadata: map[string]any{"type": "gemini"},
	}
	store := &disableFailureReloadStore{
		current: persistedActive.Clone(),
		saveErr: persistErr,
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), persistedActive.Clone()); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}

	if _, errDisable := manager.SetDisabled(context.Background(), []string{authID}, true); !errors.Is(errDisable, persistErr) {
		t.Fatalf("SetDisabled(disabled=true) error = %v, want errors.Is(_, %v)", errDisable, persistErr)
	}
	if errReload := manager.ReloadByID(context.Background(), authID); errReload != nil {
		t.Fatalf("ReloadByID() error = %v", errReload)
	}

	got, ok := manager.GetByID(authID)
	if !ok || got == nil || !got.Disabled || got.Status != StatusDisabled {
		t.Fatalf("runtime auth after failed disable and reload = %#v, want disabled", got)
	}
	if manager.HasProviderAuth("gemini") {
		t.Fatal("HasProviderAuth(gemini) = true after failed disable and reload, want false")
	}
}

func TestManagerSetDisabledPersistenceFailureSurvivesReconcile(t *testing.T) {
	persistErr := errors.New("save disabled auth failed")
	const authID = "disable-failure-reconcile"
	persistedActive := &Auth{
		ID:       authID,
		Provider: "gemini",
		Status:   StatusActive,
		Metadata: map[string]any{"type": "gemini"},
	}
	store := &disableFailureReloadStore{
		current: persistedActive.Clone(),
		saveErr: persistErr,
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), persistedActive.Clone()); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}

	if _, errDisable := manager.SetDisabled(context.Background(), []string{authID}, true); !errors.Is(errDisable, persistErr) {
		t.Fatalf("SetDisabled(disabled=true) error = %v, want errors.Is(_, %v)", errDisable, persistErr)
	}
	if errReconcile := manager.Reconcile(context.Background()); errReconcile != nil {
		t.Fatalf("Reconcile() error = %v", errReconcile)
	}

	got, ok := manager.GetByID(authID)
	if !ok || got == nil || !got.Disabled || got.Status != StatusDisabled {
		t.Fatalf("runtime auth after failed disable and reconcile = %#v, want disabled", got)
	}
}

func TestManagerSetDisabledPersistenceFailureSurvivesActiveUpdate(t *testing.T) {
	persistErr := errors.New("save disabled auth failed")
	const authID = "disable-failure-update"
	persistedActive := &Auth{
		ID:       authID,
		Provider: "gemini",
		Status:   StatusActive,
		Metadata: map[string]any{"type": "gemini"},
	}
	store := &disableFailureReloadStore{
		current: persistedActive.Clone(),
		saveErr: persistErr,
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), persistedActive.Clone()); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if _, errDisable := manager.SetDisabled(context.Background(), []string{authID}, true); !errors.Is(errDisable, persistErr) {
		t.Fatalf("SetDisabled(disabled=true) error = %v, want errors.Is(_, %v)", errDisable, persistErr)
	}

	if _, errUpdate := manager.Update(WithSkipPersist(context.Background()), persistedActive.Clone()); errUpdate != nil {
		t.Fatalf("Update(active snapshot) error = %v", errUpdate)
	}
	got, ok := manager.GetByID(authID)
	if !ok || got == nil || !got.Disabled || got.Status != StatusDisabled {
		t.Fatalf("runtime auth after failed disable and active update = %#v, want disabled", got)
	}
	if manager.HasProviderAuth("gemini") {
		t.Fatal("HasProviderAuth(gemini) = true after failed disable and active update, want false")
	}
}

func TestManagerBeginEnableTransitionBlocksWatcherUntilExplicitEnableStarts(t *testing.T) {
	const authID = "source-enable-transition"
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	active := &Auth{
		ID:       authID,
		Provider: "gemini",
		Status:   StatusActive,
		Metadata: map[string]any{"type": "gemini"},
	}
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), active.Clone()); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if _, errDisable := manager.SetDisabled(WithSkipPersist(context.Background()), []string{authID}, true); errDisable != nil {
		t.Fatalf("SetDisabled(disabled=true) error = %v", errDisable)
	}

	endTransition := manager.BeginEnableTransition([]string{authID})
	if _, errUpdate := manager.Update(WithSkipPersist(context.Background()), active.Clone()); errUpdate != nil {
		endTransition()
		t.Fatalf("Update(active watcher snapshot) error = %v", errUpdate)
	}
	got, ok := manager.GetByID(authID)
	if !ok || got == nil || !got.Disabled || got.Status != StatusDisabled {
		endTransition()
		t.Fatalf("runtime auth during enable transition = %#v, want disabled", got)
	}

	endTransition()
	if _, errUpdate := manager.Update(WithSkipPersist(context.Background()), active.Clone()); errUpdate != nil {
		t.Fatalf("Update(active watcher snapshot after transition) error = %v", errUpdate)
	}
	got, ok = manager.GetByID(authID)
	if !ok || got == nil || got.Disabled || got.Status == StatusDisabled {
		t.Fatalf("runtime auth after enable transition = %#v, want active", got)
	}
}

func TestManagerSetDisabledPendingGuardKeepsNewUpdateFields(t *testing.T) {
	persistErr := errors.New("save disabled auth failed")
	const authID = "disable-pending-new-update"
	store := &disableFailureReloadStore{
		current: &Auth{
			ID:       authID,
			Provider: "gemini",
			Status:   StatusActive,
			Metadata: map[string]any{"type": "gemini"},
		},
		saveErr: persistErr,
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), store.current.Clone()); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if _, errDisable := manager.SetDisabled(context.Background(), []string{authID}, true); !errors.Is(errDisable, persistErr) {
		t.Fatalf("SetDisabled(disabled=true) error = %v, want errors.Is(_, %v)", errDisable, persistErr)
	}

	activeUpdate, ok := manager.GetByID(authID)
	if !ok || activeUpdate == nil {
		t.Fatalf("GetByID(%q) found = false", authID)
	}
	disabledRevision := activeUpdate.revision
	activeUpdate.Label = "rotated-credentials"
	activeUpdate.Attributes = map[string]string{"credential": "new"}
	activeUpdate.Disabled = false
	activeUpdate.Status = StatusActive
	activeUpdate.StatusMessage = ""
	activeUpdate.Metadata["disabled"] = false
	if _, errUpdate := manager.Update(WithSkipPersist(context.Background()), activeUpdate); errUpdate != nil {
		t.Fatalf("Update(new active fields) error = %v", errUpdate)
	}

	got, ok := manager.GetByID(authID)
	if !ok || got == nil {
		t.Fatalf("GetByID(%q) found = false", authID)
	}
	if got.Label != "rotated-credentials" || got.Attributes["credential"] != "new" {
		t.Fatalf("new update fields were discarded: %#v", got)
	}
	if !got.Disabled || got.Status != StatusDisabled || got.Metadata["disabled"] != true {
		t.Fatalf("new active update reopened pending disable: %#v", got)
	}
	if got.revision <= disabledRevision {
		t.Fatalf("Update() revision = %d, want greater than disabled revision %d", got.revision, disabledRevision)
	}
}

func TestManagerSetDisabledIgnoresInFlightActiveReloadUntilStoreConfirmsDisabled(t *testing.T) {
	const authID = "disable-inflight-reload"
	persistedActive := &Auth{
		ID:       authID,
		Provider: "gemini",
		Status:   StatusActive,
		Metadata: map[string]any{"type": "gemini"},
	}
	store := newBlockingDisableReloadStore(persistedActive)
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), persistedActive.Clone()); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}

	disableDone := make(chan error, 1)
	go func() {
		_, errDisable := manager.SetDisabled(context.Background(), []string{authID}, true)
		disableDone <- errDisable
	}()
	<-store.saveStarted

	reloadDone := make(chan error, 1)
	go func() {
		reloadDone <- manager.ReloadByID(context.Background(), authID)
	}()
	<-store.fetchCaptured

	close(store.allowSave)
	if errDisable := <-disableDone; errDisable != nil {
		t.Fatalf("SetDisabled(disabled=true) error = %v", errDisable)
	}
	close(store.releaseFetch)
	if errReload := <-reloadDone; errReload != nil {
		t.Fatalf("ReloadByID(stale active) error = %v", errReload)
	}
	got, ok := manager.GetByID(authID)
	if !ok || got == nil || !got.Disabled || got.Status != StatusDisabled {
		t.Fatalf("runtime auth after in-flight active reload = %#v, want disabled", got)
	}

	if errReload := manager.ReloadByID(context.Background(), authID); errReload != nil {
		t.Fatalf("ReloadByID(persisted disabled) error = %v", errReload)
	}
	store.mu.Lock()
	store.current = persistedActive.Clone()
	store.mu.Unlock()
	if errReload := manager.ReloadByID(context.Background(), authID); errReload != nil {
		t.Fatalf("ReloadByID(new active) error = %v", errReload)
	}
	got, ok = manager.GetByID(authID)
	if !ok || got == nil || got.Disabled || got.Status == StatusDisabled {
		t.Fatalf("runtime auth after confirmed disable followed by active reload = %#v, want active", got)
	}
}

func TestManagerSetDisabledInFlightReloadPersistsNewCredentialWithoutAdvancingRevision(t *testing.T) {
	const authID = "disable-inflight-new-credential"
	oldActive := &Auth{
		ID:         authID,
		Provider:   "gemini",
		Label:      "old-credential",
		Status:     StatusActive,
		Attributes: map[string]string{"credential": "old"},
		Metadata:   map[string]any{"access_token": "old", "type": "gemini"},
	}
	store := newBlockingDisableReloadStore(oldActive)
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), oldActive.Clone()); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}

	disableDone := make(chan error, 1)
	go func() {
		_, errDisable := manager.SetDisabled(context.Background(), []string{authID}, true)
		disableDone <- errDisable
	}()
	<-store.saveStarted
	disabled, ok := manager.GetByID(authID)
	if !ok || disabled == nil || !disabled.Disabled {
		t.Fatalf("runtime auth while disable save is blocked = %#v", disabled)
	}
	disabledRevision := disabled.revision

	store.mu.Lock()
	store.current = newActiveCredentialSnapshot(authID)
	store.mu.Unlock()
	reloadDone := make(chan error, 1)
	go func() { reloadDone <- manager.ReloadByID(context.Background(), authID) }()
	<-store.fetchCaptured

	close(store.allowSave)
	if errDisable := <-disableDone; errDisable != nil {
		t.Fatalf("SetDisabled(disabled=true) error = %v", errDisable)
	}
	close(store.releaseFetch)
	if errReload := <-reloadDone; errReload != nil {
		t.Fatalf("ReloadByID(new active credential) error = %v", errReload)
	}

	current, ok := manager.GetByID(authID)
	if !ok || current == nil || current.Metadata["access_token"] != "new" || !current.Disabled || current.Status != StatusDisabled {
		t.Fatalf("runtime auth after in-flight reload = %#v, want new credential and disabled", current)
	}
	if current.revision != disabledRevision {
		t.Fatalf("ReloadByID() revision = %d, want pending disable revision %d", current.revision, disabledRevision)
	}
	store.mu.Lock()
	persisted := store.current.Clone()
	store.mu.Unlock()
	if persisted == nil || persisted.Metadata["access_token"] != "new" || !persisted.Disabled || persisted.Status != StatusDisabled {
		t.Fatalf("persisted auth after in-flight reload = %#v, want new credential and disabled", persisted)
	}
}

func TestManagerSetDisabledFailedPersistenceGuardClearsOnRemoval(t *testing.T) {
	persistErr := errors.New("save disabled auth failed")
	const authID = "disable-failure-removal"
	persistedActive := &Auth{
		ID:       authID,
		Provider: "gemini",
		Status:   StatusActive,
		Metadata: map[string]any{"type": "gemini"},
	}
	store := &disableFailureReloadStore{
		current: persistedActive.Clone(),
		saveErr: persistErr,
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), persistedActive.Clone()); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if _, errDisable := manager.SetDisabled(context.Background(), []string{authID}, true); !errors.Is(errDisable, persistErr) {
		t.Fatalf("SetDisabled(disabled=true) error = %v, want errors.Is(_, %v)", errDisable, persistErr)
	}

	store.mu.Lock()
	store.current = nil
	store.mu.Unlock()
	if errReload := manager.ReloadByID(context.Background(), authID); errReload != nil {
		t.Fatalf("ReloadByID(removed) error = %v", errReload)
	}
	if _, ok := manager.GetByID(authID); ok {
		t.Fatalf("GetByID(%q) found after store removal", authID)
	}

	store.mu.Lock()
	store.current = persistedActive.Clone()
	store.mu.Unlock()
	if errReload := manager.ReloadByID(context.Background(), authID); errReload != nil {
		t.Fatalf("ReloadByID(recreated) error = %v", errReload)
	}
	got, ok := manager.GetByID(authID)
	if !ok || got == nil || got.Disabled || got.Status == StatusDisabled {
		t.Fatalf("recreated runtime auth = %#v, want active", got)
	}
}

func TestManagerSetDisabledFailedPersistenceGuardClearsOnSuccessfulEnable(t *testing.T) {
	persistErr := errors.New("save disabled auth failed")
	const authID = "disable-failure-enable"
	persistedActive := &Auth{
		ID:       authID,
		Provider: "gemini",
		Status:   StatusActive,
		Metadata: map[string]any{"type": "gemini"},
	}
	store := &disableFailureReloadStore{
		current: persistedActive.Clone(),
		saveErr: persistErr,
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), persistedActive.Clone()); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if _, errDisable := manager.SetDisabled(context.Background(), []string{authID}, true); !errors.Is(errDisable, persistErr) {
		t.Fatalf("SetDisabled(disabled=true) error = %v, want errors.Is(_, %v)", errDisable, persistErr)
	}

	store.saveErr = nil
	if _, errEnable := manager.SetDisabled(context.Background(), []string{authID}, false); errEnable != nil {
		t.Fatalf("SetDisabled(disabled=false) error = %v", errEnable)
	}
	if errReload := manager.ReloadByID(context.Background(), authID); errReload != nil {
		t.Fatalf("ReloadByID(active) error = %v", errReload)
	}
	got, ok := manager.GetByID(authID)
	if !ok || got == nil || got.Disabled || got.Status == StatusDisabled {
		t.Fatalf("runtime auth after successful enable and reload = %#v, want active", got)
	}
}

func TestManagerSetDisabledEnableConflictRollbackFailureRemainsFailClosed(t *testing.T) {
	rollbackErr := errors.New("restore disabled auth failed")
	const authID = "enable-conflict-rollback-failure"
	persistedActive := &Auth{
		ID:       authID,
		Provider: "gemini",
		Status:   StatusActive,
		Metadata: map[string]any{"type": "gemini"},
	}
	store := newEnableRollbackFailureStore(persistedActive, rollbackErr)
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), persistedActive.Clone()); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if _, errDisable := manager.SetDisabled(context.Background(), []string{authID}, true); errDisable != nil {
		t.Fatalf("SetDisabled(disabled=true) error = %v", errDisable)
	}
	store.mu.Lock()
	store.failDisabled = true
	store.mu.Unlock()

	enableDone := make(chan error, 1)
	go func() {
		_, errEnable := manager.SetDisabled(context.Background(), []string{authID}, false)
		enableDone <- errEnable
	}()
	<-store.activeSaved

	updateDone := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(context.Background(), &Auth{
			ID:       authID,
			Provider: "gemini",
			Label:    "durable-conflict",
			Status:   StatusActive,
			Metadata: map[string]any{"type": "gemini"},
		})
		updateDone <- errUpdate
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		current, _ := manager.GetByID(authID)
		if current != nil && current.Label == "durable-conflict" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Update() did not create a durable enable conflict")
		}
		runtime.Gosched()
	}
	close(store.allowActiveReturn)
	if errEnable := <-enableDone; !errors.Is(errEnable, rollbackErr) {
		t.Fatalf("SetDisabled(disabled=false) error = %v, want errors.Is(_, %v)", errEnable, rollbackErr)
	}
	if errUpdate := <-updateDone; !errors.Is(errUpdate, rollbackErr) {
		t.Fatalf("Update() error = %v, want errors.Is(_, %v)", errUpdate, rollbackErr)
	}

	close(store.releaseFetch)
	if errReload := manager.ReloadByID(context.Background(), authID); errReload != nil {
		t.Fatalf("ReloadByID(active after rollback failure) error = %v", errReload)
	}
	got, ok := manager.GetByID(authID)
	if !ok || got == nil || !got.Disabled || got.Status != StatusDisabled {
		t.Fatalf("runtime auth after enable rollback failure and reload = %#v, want disabled", got)
	}
}

func TestManagerSetDisabledEnablePersistenceFailureKeepsRuntimeDisabled(t *testing.T) {
	persistErr := errors.New("save enabled auth failed")
	manager := NewManager(&disableBatchFailingStore{saveErr: persistErr}, &RoundRobinSelector{}, nil)
	ctx := context.Background()
	const authID = "enable-persist-failure"

	if _, errRegister := manager.Register(WithSkipPersist(ctx), &Auth{ID: authID, Provider: "gemini"}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if _, errDisable := manager.SetDisabled(WithSkipPersist(ctx), []string{authID}, true); errDisable != nil {
		t.Fatalf("SetDisabled(disabled=true) error = %v", errDisable)
	}

	_, errEnable := manager.SetDisabled(ctx, []string{authID}, false)
	if !errors.Is(errEnable, persistErr) {
		t.Fatalf("SetDisabled(disabled=false) error = %v, want errors.Is(_, %v)", errEnable, persistErr)
	}

	got, ok := manager.GetByID(authID)
	if !ok || got == nil {
		t.Fatalf("GetByID(%q) found = false", authID)
	}
	if !got.Disabled || got.Status != StatusDisabled {
		t.Fatalf("GetByID(%q) disabled/status = %v/%s, want true/%s", authID, got.Disabled, got.Status, StatusDisabled)
	}

	if manager.HasProviderAuth("gemini") {
		t.Fatal("HasProviderAuth(gemini) = true after enable persistence failure, want false")
	}
}

func TestManagerSetDisabledEnableReleasesPersistenceLockBeforeHook(t *testing.T) {
	hook := &reentrantUpdateHook{done: make(chan error, 1)}
	manager := NewManager(nil, &RoundRobinSelector{}, hook)
	hook.manager = manager
	ctx := context.Background()
	const authID = "reentrant-hook"

	if _, errRegister := manager.Register(ctx, &Auth{ID: authID, Provider: "gemini"}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if _, errDisable := manager.SetDisabled(ctx, []string{authID}, true); errDisable != nil {
		t.Fatalf("SetDisabled(disabled=true) error = %v", errDisable)
	}

	enableDone := make(chan error, 1)
	go func() {
		_, errEnable := manager.SetDisabled(ctx, []string{authID}, false)
		enableDone <- errEnable
	}()

	select {
	case errEnable := <-enableDone:
		if errEnable != nil {
			t.Fatalf("SetDisabled(disabled=false) error = %v", errEnable)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SetDisabled(disabled=false) deadlocked in reentrant hook")
	}
	select {
	case errUpdate := <-hook.done:
		if errUpdate != nil {
			t.Fatalf("reentrant Update() error = %v", errUpdate)
		}
	default:
		t.Fatal("OnAuthUpdated did not reenter Manager.Update")
	}
}

type blockingOrderedStore struct {
	mu                    sync.Mutex
	current               *Auth
	activeSaveEntered     chan struct{}
	releaseActiveSave     chan struct{}
	disabledSaveCommitted chan struct{}
	activeSaveOnce        sync.Once
	disabledSaveOnce      sync.Once
}

func newBlockingOrderedStore() *blockingOrderedStore {
	return &blockingOrderedStore{
		activeSaveEntered:     make(chan struct{}),
		releaseActiveSave:     make(chan struct{}),
		disabledSaveCommitted: make(chan struct{}),
	}
}

func (s *blockingOrderedStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		return nil, nil
	}
	return []*Auth{s.current.Clone()}, nil
}

func (s *blockingOrderedStore) GetByID(_ context.Context, id string) (*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil || s.current.ID != id {
		return nil, nil
	}
	return s.current.Clone(), nil
}

func (s *blockingOrderedStore) Save(ctx context.Context, auth *Auth) (string, error) {
	snapshot := auth.Clone()
	if !snapshot.Disabled {
		s.activeSaveOnce.Do(func() { close(s.activeSaveEntered) })
		select {
		case <-s.releaseActiveSave:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	s.mu.Lock()
	s.current = snapshot
	s.mu.Unlock()
	if snapshot.Disabled {
		s.disabledSaveOnce.Do(func() { close(s.disabledSaveCommitted) })
	}
	return snapshot.ID, nil
}

func (*blockingOrderedStore) Delete(context.Context, string) error { return nil }

func startBlockedCredentialEnable(t *testing.T, authID string) (*Manager, *blockingOrderedStore, <-chan error) {
	t.Helper()
	store := newBlockingOrderedStore()
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	ctx := context.Background()
	if _, errRegister := manager.Register(WithSkipPersist(ctx), &Auth{
		ID:         authID,
		Provider:   "gemini",
		Label:      "old-credential",
		Attributes: map[string]string{"credential": "old"},
		Metadata:   map[string]any{"access_token": "old", "type": "gemini"},
	}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if _, errDisable := manager.SetDisabled(WithSkipPersist(ctx), []string{authID}, true); errDisable != nil {
		t.Fatalf("SetDisabled(disabled=true) error = %v", errDisable)
	}
	disabled, ok := manager.GetByID(authID)
	if !ok || disabled == nil {
		t.Fatalf("GetByID(%q) found = false", authID)
	}
	store.mu.Lock()
	store.current = disabled.Clone()
	store.mu.Unlock()

	enableDone := make(chan error, 1)
	go func() {
		_, errEnable := manager.SetDisabled(ctx, []string{authID}, false)
		enableDone <- errEnable
	}()
	<-store.activeSaveEntered
	return manager, store, enableDone
}

func newActiveCredentialSnapshot(authID string) *Auth {
	return &Auth{
		ID:         authID,
		Provider:   "gemini",
		Label:      "new-credential",
		Status:     StatusActive,
		Attributes: map[string]string{"credential": "new"},
		Metadata:   map[string]any{"access_token": "new", "type": "gemini"},
	}
}

func finishBlockedCredentialEnable(t *testing.T, manager *Manager, store *blockingOrderedStore, authID string, enableDone <-chan error) {
	t.Helper()
	close(store.releaseActiveSave)
	if errEnable := <-enableDone; errEnable == nil {
		t.Fatal("SetDisabled(disabled=false) error = nil after credential changed during enable, want conflict")
	}

	current, ok := manager.GetByID(authID)
	if !ok || current == nil {
		t.Fatalf("GetByID(%q) found = false", authID)
	}
	if current.Label != "new-credential" || current.Attributes["credential"] != "new" || current.Metadata["access_token"] != "new" {
		t.Fatalf("runtime auth lost new credential fields: %#v", current)
	}
	if !current.Disabled || current.Status != StatusDisabled || current.Metadata["disabled"] != true {
		t.Fatalf("runtime auth reopened during failed enable: %#v", current)
	}

	persisted, errList := store.List(context.Background())
	if errList != nil {
		t.Fatalf("Store.List() error = %v", errList)
	}
	if len(persisted) != 1 || persisted[0] == nil {
		t.Fatalf("Store.List() = %#v, want one auth", persisted)
	}
	if persisted[0].Label != "new-credential" || persisted[0].Attributes["credential"] != "new" || persisted[0].Metadata["access_token"] != "new" {
		t.Fatalf("persisted auth lost new credential fields: %#v", persisted[0])
	}
	if !persisted[0].Disabled || persisted[0].Status != StatusDisabled || persisted[0].Metadata["disabled"] != true {
		t.Fatalf("persisted auth after failed enable = %#v, want disabled", persisted[0])
	}
}

func waitForCredentialMutation(manager *Manager, authID string, mutationDone <-chan error) (bool, error, bool) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		current, ok := manager.GetByID(authID)
		if ok && current != nil && current.Metadata["access_token"] == "new" {
			return true, nil, false
		}
		select {
		case errMutation := <-mutationDone:
			return false, errMutation, true
		default:
			runtime.Gosched()
		}
	}
	return false, nil, false
}

func TestManagerEnableTransitionUpdateMergesNewCredentialAndConflicts(t *testing.T) {
	const authID = "enable-update-new-credential"
	manager, store, enableDone := startBlockedCredentialEnable(t, authID)
	mutationDone := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(WithSkipPersist(context.Background()), newActiveCredentialSnapshot(authID))
		mutationDone <- errUpdate
	}()
	mutated, errUpdate, updateDone := waitForCredentialMutation(manager, authID, mutationDone)
	finishBlockedCredentialEnable(t, manager, store, authID, enableDone)
	if !updateDone {
		errUpdate = <-mutationDone
	}
	if errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	if !mutated {
		t.Fatal("Update() did not publish new credential before enable completed")
	}
}

func TestManagerEnableTransitionRegisterMergesNewCredentialAndConflicts(t *testing.T) {
	const authID = "enable-register-new-credential"
	manager, store, enableDone := startBlockedCredentialEnable(t, authID)
	mutationDone := make(chan error, 1)
	go func() {
		_, errRegister := manager.Register(WithSkipPersist(context.Background()), newActiveCredentialSnapshot(authID))
		mutationDone <- errRegister
	}()
	mutated, errRegister, registerDone := waitForCredentialMutation(manager, authID, mutationDone)
	finishBlockedCredentialEnable(t, manager, store, authID, enableDone)
	if !registerDone {
		errRegister = <-mutationDone
	}
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if !mutated {
		t.Fatal("Register() did not publish new credential before enable completed")
	}
}

func TestManagerEnableTransitionReloadByIDMergesNewCredentialAndConflicts(t *testing.T) {
	const authID = "enable-reload-new-credential"
	manager, store, enableDone := startBlockedCredentialEnable(t, authID)
	store.mu.Lock()
	store.current = newActiveCredentialSnapshot(authID)
	store.mu.Unlock()
	if errReload := manager.ReloadByID(context.Background(), authID); errReload != nil {
		close(store.releaseActiveSave)
		t.Fatalf("ReloadByID() error = %v", errReload)
	}
	finishBlockedCredentialEnable(t, manager, store, authID, enableDone)
}

func TestManagerEnableTransitionReconcileMergesNewCredentialAndConflicts(t *testing.T) {
	const authID = "enable-reconcile-new-credential"
	manager, store, enableDone := startBlockedCredentialEnable(t, authID)
	store.mu.Lock()
	store.current = newActiveCredentialSnapshot(authID)
	store.mu.Unlock()
	if errReconcile := manager.Reconcile(context.Background()); errReconcile != nil {
		close(store.releaseActiveSave)
		t.Fatalf("Reconcile() error = %v", errReconcile)
	}
	current, ok := manager.GetByID(authID)
	if !ok || current == nil || current.Metadata["access_token"] != "new" || !current.Disabled || current.Status != StatusDisabled {
		close(store.releaseActiveSave)
		<-enableDone
		t.Fatalf("runtime auth after Reconcile() = %#v, want new credential and disabled", current)
	}
	finishBlockedCredentialEnable(t, manager, store, authID, enableDone)
}

type disableRevisionAdvanceStore struct {
	mu                sync.Mutex
	current           *Auth
	activeSaveEntered chan struct{}
	releaseActiveSave chan struct{}
	activeSaveOnce    sync.Once
}

func newDisableRevisionAdvanceStore() *disableRevisionAdvanceStore {
	return &disableRevisionAdvanceStore{
		activeSaveEntered: make(chan struct{}),
		releaseActiveSave: make(chan struct{}),
	}
}

func (s *disableRevisionAdvanceStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		return nil, nil
	}
	return []*Auth{s.current.Clone()}, nil
}

func (s *disableRevisionAdvanceStore) Save(ctx context.Context, auth *Auth) (string, error) {
	snapshot := auth.Clone()
	if !snapshot.Disabled {
		s.activeSaveOnce.Do(func() { close(s.activeSaveEntered) })
		select {
		case <-s.releaseActiveSave:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	s.current = snapshot
	s.mu.Unlock()
	return snapshot.ID, nil
}

func (*disableRevisionAdvanceStore) Delete(context.Context, string) error { return nil }

type partialEnableStore struct {
	mu              sync.Mutex
	current         map[string]*Auth
	failID          string
	activeErr       error
	afterActiveSave func(*Auth)
}

func (s *partialEnableStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	auths := make([]*Auth, 0, len(s.current))
	for _, auth := range s.current {
		auths = append(auths, auth.Clone())
	}
	return auths, nil
}

func (s *partialEnableStore) Save(_ context.Context, auth *Auth) (string, error) {
	s.mu.Lock()
	if auth.ID == s.failID && !auth.Disabled {
		s.mu.Unlock()
		return "", s.activeErr
	}
	s.current[auth.ID] = auth.Clone()
	afterActiveSave := s.afterActiveSave
	s.mu.Unlock()
	if !auth.Disabled && afterActiveSave != nil {
		afterActiveSave(auth.Clone())
	}
	return auth.ID, nil
}

func (*partialEnableStore) Delete(context.Context, string) error { return nil }

type cancelingEnableStore struct {
	mu        sync.Mutex
	current   map[string]*Auth
	cancel    context.CancelFunc
	cancelled bool
}

func (s *cancelingEnableStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	auths := make([]*Auth, 0, len(s.current))
	for _, auth := range s.current {
		auths = append(auths, auth.Clone())
	}
	return auths, nil
}

func (s *cancelingEnableStore) Save(ctx context.Context, auth *Auth) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.current[auth.ID] = auth.Clone()
	if !auth.Disabled && !s.cancelled {
		s.cancelled = true
		s.cancel()
	}
	return auth.ID, nil
}

func (*cancelingEnableStore) Delete(context.Context, string) error { return nil }

func TestManagerSetDisabledPreventsEarlierActiveSaveFromWinning(t *testing.T) {
	store := newBlockingOrderedStore()
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	ctx := context.Background()
	const authID = "persist-order"

	if _, errRegister := manager.Register(WithSkipPersist(ctx), &Auth{
		ID:       authID,
		Provider: "gemini",
		Metadata: map[string]any{"type": "gemini"},
	}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	activeSnapshot, ok := manager.GetByID(authID)
	if !ok || activeSnapshot == nil {
		t.Fatalf("GetByID(%q) found = false", authID)
	}

	activeUpdateDone := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(ctx, activeSnapshot)
		activeUpdateDone <- errUpdate
	}()
	<-store.activeSaveEntered

	disableDone := make(chan error, 1)
	go func() {
		_, errDisable := manager.SetDisabled(ctx, []string{authID}, true)
		disableDone <- errDisable
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		current, found := manager.GetByID(authID)
		if found && current != nil && current.Disabled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("SetDisabled() did not publish disabled runtime state")
		}
		runtime.Gosched()
	}

	// The unfixed implementation permits the disabled save to overtake the
	// already-started active save. A serialized implementation instead waits
	// for the active save, so release it after a short bounded observation.
	select {
	case <-store.disabledSaveCommitted:
	case <-time.After(250 * time.Millisecond):
	}
	close(store.releaseActiveSave)

	if errUpdate := <-activeUpdateDone; errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	if errDisable := <-disableDone; errDisable != nil {
		t.Fatalf("SetDisabled() error = %v", errDisable)
	}

	persisted, errList := store.List(ctx)
	if errList != nil {
		t.Fatalf("Store.List() error = %v", errList)
	}
	if len(persisted) != 1 || persisted[0] == nil {
		t.Fatalf("Store.List() = %#v, want one auth", persisted)
	}
	if !persisted[0].Disabled || persisted[0].Status != StatusDisabled {
		t.Fatalf("persisted auth disabled/status = %v/%s, want true/%s", persisted[0].Disabled, persisted[0].Status, StatusDisabled)
	}
}

func TestManagerSetDisabledPersistsLatestDisabledSnapshotAfterRevisionAdvances(t *testing.T) {
	store := newDisableRevisionAdvanceStore()
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	const authID = "disable-revision-advance"
	active := &Auth{
		ID:       authID,
		Provider: "gemini",
		Status:   StatusActive,
		Metadata: map[string]any{"type": "gemini"},
	}
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), active); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	activeSnapshot, _ := manager.GetByID(authID)

	updateDone := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(context.Background(), activeSnapshot)
		updateDone <- errUpdate
	}()
	<-store.activeSaveEntered

	disableDone := make(chan error, 1)
	go func() {
		_, errDisable := manager.SetDisabled(context.Background(), []string{authID}, true)
		disableDone <- errDisable
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		current, ok := manager.GetByID(authID)
		if ok && current != nil && current.Disabled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("SetDisabled() did not publish disabled runtime state")
		}
		runtime.Gosched()
	}

	markCtx, cancelMark := context.WithCancel(context.Background())
	cancelMark()
	markDone := make(chan struct{})
	go func() {
		manager.MarkResult(markCtx, Result{AuthID: authID, Provider: "gemini", Success: true})
		close(markDone)
	}()
	for {
		current, _ := manager.GetByID(authID)
		if current != nil && current.Success == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("MarkResult() did not advance the disabled auth revision")
		}
		runtime.Gosched()
	}

	close(store.releaseActiveSave)
	if errUpdate := <-updateDone; errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	if errDisable := <-disableDone; errDisable != nil {
		t.Fatalf("SetDisabled() error = %v", errDisable)
	}
	<-markDone

	persisted, errList := store.List(context.Background())
	if errList != nil {
		t.Fatalf("Store.List() error = %v", errList)
	}
	if len(persisted) != 1 || persisted[0] == nil || !persisted[0].Disabled || persisted[0].Status != StatusDisabled {
		t.Fatalf("persisted auth after revision advance = %#v, want disabled", persisted)
	}
}

func TestManagerSetDisabledEnableMergesConcurrentPersistedDisabledCredentialAndConflicts(t *testing.T) {
	store := newBlockingOrderedStore()
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	ctx := context.Background()
	const authID = "enable-conflict"

	if _, errRegister := manager.Register(WithSkipPersist(ctx), &Auth{
		ID:       authID,
		Provider: "gemini",
		Metadata: map[string]any{"type": "gemini"},
	}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if _, errDisable := manager.SetDisabled(WithSkipPersist(ctx), []string{authID}, true); errDisable != nil {
		t.Fatalf("SetDisabled(disabled=true) error = %v", errDisable)
	}
	disabledSnapshot, ok := manager.GetByID(authID)
	if !ok || disabledSnapshot == nil {
		t.Fatalf("GetByID(%q) found = false", authID)
	}
	disabledSnapshot.Label = "new-credential"
	disabledSnapshot.Attributes = map[string]string{"credential": "new"}
	disabledSnapshot.Metadata["access_token"] = "new"
	store.mu.Lock()
	store.current = disabledSnapshot.Clone()
	store.mu.Unlock()

	enableDone := make(chan error, 1)
	go func() {
		_, errEnable := manager.SetDisabled(ctx, []string{authID}, false)
		enableDone <- errEnable
	}()
	<-store.activeSaveEntered

	if errReload := manager.ReloadByID(ctx, authID); errReload != nil {
		close(store.releaseActiveSave)
		t.Fatalf("ReloadByID() error = %v", errReload)
	}
	close(store.releaseActiveSave)
	if errEnable := <-enableDone; errEnable == nil {
		t.Fatal("SetDisabled(disabled=false) error = nil after disabled credential reload, want conflict")
	}

	persisted, errList := store.List(ctx)
	if errList != nil {
		t.Fatalf("Store.List() error = %v", errList)
	}
	if len(persisted) != 1 || persisted[0] == nil {
		t.Fatalf("Store.List() = %#v, want one auth", persisted)
	}
	if persisted[0].Label != "new-credential" || persisted[0].Attributes["credential"] != "new" || persisted[0].Metadata["access_token"] != "new" {
		t.Fatalf("persisted auth lost disabled credential reload: %#v", persisted[0])
	}
	if !persisted[0].Disabled || persisted[0].Status != StatusDisabled {
		t.Fatalf("persisted auth after enable conflict = %#v, want disabled", persisted[0])
	}
	runtimeAuth, ok := manager.GetByID(authID)
	if !ok || runtimeAuth == nil || runtimeAuth.Label != "new-credential" || runtimeAuth.Metadata["access_token"] != "new" || !runtimeAuth.Disabled || runtimeAuth.Status != StatusDisabled {
		t.Fatalf("runtime auth after enable conflict = %#v, want new credential and disabled", runtimeAuth)
	}
}

func TestManagerSetDisabledBatchEnableFailureRestoresEarlierSaves(t *testing.T) {
	persistErr := errors.New("second active save failed")
	store := &partialEnableStore{
		current:   make(map[string]*Auth),
		failID:    "enable-b",
		activeErr: persistErr,
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	ctx := context.Background()
	ids := []string{"enable-a", "enable-b"}

	for _, id := range ids {
		if _, errRegister := manager.Register(WithSkipPersist(ctx), &Auth{
			ID:       id,
			Provider: "gemini",
			Metadata: map[string]any{"type": "gemini"},
		}); errRegister != nil {
			t.Fatalf("Register(%q) error = %v", id, errRegister)
		}
	}
	if _, errDisable := manager.SetDisabled(WithSkipPersist(ctx), ids, true); errDisable != nil {
		t.Fatalf("SetDisabled(disabled=true) error = %v", errDisable)
	}
	store.mu.Lock()
	for _, id := range ids {
		auth, _ := manager.GetByID(id)
		store.current[id] = auth
	}
	store.mu.Unlock()

	_, errEnable := manager.SetDisabled(ctx, ids, false)
	if !errors.Is(errEnable, persistErr) {
		t.Fatalf("SetDisabled(disabled=false) error = %v, want errors.Is(_, %v)", errEnable, persistErr)
	}

	persisted, errList := store.List(ctx)
	if errList != nil {
		t.Fatalf("Store.List() error = %v", errList)
	}
	if len(persisted) != len(ids) {
		t.Fatalf("len(Store.List()) = %d, want %d", len(persisted), len(ids))
	}
	for _, auth := range persisted {
		if auth == nil || !auth.Disabled || auth.Status != StatusDisabled {
			t.Fatalf("persisted auth after partial enable failure = %#v, want disabled", auth)
		}
	}
	for _, id := range ids {
		auth, ok := manager.GetByID(id)
		if !ok || auth == nil || !auth.Disabled || auth.Status != StatusDisabled {
			t.Fatalf("runtime auth %q after partial enable failure = %#v, want disabled", id, auth)
		}
	}
}

func TestManagerSetDisabledBatchEnableDoesNotPublishPartialStoreReload(t *testing.T) {
	persistErr := errors.New("second active save failed")
	store := &partialEnableStore{
		current:   make(map[string]*Auth),
		failID:    "enable-b",
		activeErr: persistErr,
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	ctx := context.Background()
	ids := []string{"enable-a", "enable-b"}

	for _, id := range ids {
		if _, errRegister := manager.Register(WithSkipPersist(ctx), &Auth{
			ID:       id,
			Provider: "gemini",
			Metadata: map[string]any{"type": "gemini"},
		}); errRegister != nil {
			t.Fatalf("Register(%q) error = %v", id, errRegister)
		}
	}
	if _, errDisable := manager.SetDisabled(WithSkipPersist(ctx), ids, true); errDisable != nil {
		t.Fatalf("SetDisabled(disabled=true) error = %v", errDisable)
	}
	store.mu.Lock()
	for _, id := range ids {
		auth, _ := manager.GetByID(id)
		store.current[id] = auth
	}
	store.mu.Unlock()

	var reloadErr error
	store.afterActiveSave = func(auth *Auth) {
		if auth.ID == ids[0] {
			reloadErr = manager.Reconcile(ctx)
		}
	}
	_, errEnable := manager.SetDisabled(ctx, ids, false)
	if reloadErr != nil {
		t.Fatalf("Reconcile() during partial enable error = %v", reloadErr)
	}
	if !errors.Is(errEnable, persistErr) {
		t.Fatalf("SetDisabled(disabled=false) error = %v, want errors.Is(_, %v)", errEnable, persistErr)
	}
	for _, id := range ids {
		auth, ok := manager.GetByID(id)
		if !ok || auth == nil || !auth.Disabled || auth.Status != StatusDisabled {
			t.Fatalf("runtime auth %q after partial enable reload = %#v, want disabled", id, auth)
		}
	}
}

func TestManagerSetDisabledRollbackIgnoresCanceledRequestContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &cancelingEnableStore{
		current: make(map[string]*Auth),
		cancel:  cancel,
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	ids := []string{"cancel-a", "cancel-b"}

	for _, id := range ids {
		if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{
			ID:       id,
			Provider: "gemini",
			Metadata: map[string]any{"type": "gemini"},
		}); errRegister != nil {
			t.Fatalf("Register(%q) error = %v", id, errRegister)
		}
	}
	if _, errDisable := manager.SetDisabled(WithSkipPersist(context.Background()), ids, true); errDisable != nil {
		t.Fatalf("SetDisabled(disabled=true) error = %v", errDisable)
	}
	store.mu.Lock()
	for _, id := range ids {
		auth, _ := manager.GetByID(id)
		store.current[id] = auth
	}
	store.mu.Unlock()

	_, errEnable := manager.SetDisabled(ctx, ids, false)
	if !errors.Is(errEnable, context.Canceled) {
		t.Fatalf("SetDisabled(disabled=false) error = %v, want context.Canceled", errEnable)
	}

	persisted, errList := store.List(context.Background())
	if errList != nil {
		t.Fatalf("Store.List() error = %v", errList)
	}
	if len(persisted) != len(ids) {
		t.Fatalf("len(Store.List()) = %d, want %d", len(persisted), len(ids))
	}
	for _, auth := range persisted {
		if auth == nil || !auth.Disabled || auth.Status != StatusDisabled {
			t.Fatalf("persisted auth after canceled enable = %#v, want disabled", auth)
		}
	}
}

func TestManagerSetDisabledCanceledEnableDoesNotPublishActive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &cancelingEnableStore{
		current: make(map[string]*Auth),
		cancel:  cancel,
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	const authID = "cancel-final-save"

	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{
		ID:       authID,
		Provider: "gemini",
		Metadata: map[string]any{"type": "gemini"},
	}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if _, errDisable := manager.SetDisabled(WithSkipPersist(context.Background()), []string{authID}, true); errDisable != nil {
		t.Fatalf("SetDisabled(disabled=true) error = %v", errDisable)
	}
	disabledSnapshot, ok := manager.GetByID(authID)
	if !ok || disabledSnapshot == nil {
		t.Fatalf("GetByID(%q) found = false", authID)
	}
	store.mu.Lock()
	store.current[authID] = disabledSnapshot.Clone()
	store.mu.Unlock()

	_, errEnable := manager.SetDisabled(ctx, []string{authID}, false)
	if !errors.Is(errEnable, context.Canceled) {
		t.Fatalf("SetDisabled(disabled=false) error = %v, want context.Canceled", errEnable)
	}

	runtimeAuth, ok := manager.GetByID(authID)
	if !ok || runtimeAuth == nil || !runtimeAuth.Disabled || runtimeAuth.Status != StatusDisabled {
		t.Fatalf("runtime auth after canceled enable = %#v, want disabled", runtimeAuth)
	}
	persisted, errList := store.List(context.Background())
	if errList != nil {
		t.Fatalf("Store.List() error = %v", errList)
	}
	if len(persisted) != 1 || persisted[0] == nil || !persisted[0].Disabled || persisted[0].Status != StatusDisabled {
		t.Fatalf("persisted auth after canceled enable = %#v, want disabled", persisted)
	}
}

type haAtomicBatchObservation struct {
	payload     string
	auths       map[string]*Auth
	admittedAny bool
	reloadErr   error
}

type haAtomicBatchStore struct {
	mu                  sync.Mutex
	rows                map[string]*Auth
	failID              string
	saveErr             error
	commitErr           error
	beforeBatchFinalize func()
	cancelOnBatchCommit context.CancelFunc
	notify              func(string)
}

type generationCASBatchStore struct {
	mu   sync.Mutex
	rows map[string]*Auth
}

func newGenerationCASBatchStore(auth *Auth, generation uint64) *generationCASBatchStore {
	row := auth.Clone()
	row.SetStoreGeneration(generation)
	return &generationCASBatchStore{rows: map[string]*Auth{row.ID: row}}
}

func (s *generationCASBatchStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*Auth, 0, len(s.rows))
	for _, auth := range s.rows {
		result = append(result, auth.Clone())
	}
	return result, nil
}

func (s *generationCASBatchStore) GetByID(_ context.Context, id string) (*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rows[id].Clone(), nil
}

func (s *generationCASBatchStore) Save(ctx context.Context, auth *Auth) (string, error) {
	path, _, err := s.SaveVersioned(ctx, auth, auth.StoreGeneration())
	return path, err
}

func (s *generationCASBatchStore) SaveVersioned(ctx context.Context, auth *Auth, expectedGeneration uint64) (string, uint64, error) {
	if auth.StoreGeneration() != expectedGeneration {
		return "", 0, ErrAuthStoreConflict
	}
	err := s.SaveBatch(ctx, []*Auth{auth}, func(commit func() error) error { return commit() })
	return auth.ID, auth.StoreGeneration(), err
}

func (*generationCASBatchStore) Delete(context.Context, string) error { return nil }

func (s *generationCASBatchStore) SaveBatch(_ context.Context, auths []*Auth, finalize func(commit func() error) error) error {
	s.mu.Lock()
	prepared := make(map[string]*Auth, len(auths))
	for _, auth := range auths {
		current := s.rows[auth.ID]
		if current == nil || auth.StoreGeneration() != current.StoreGeneration() {
			s.mu.Unlock()
			return ErrAuthStoreConflict
		}
		next := auth.Clone()
		next.SetStoreGeneration(current.StoreGeneration() + 1)
		auth.SetStoreGeneration(next.StoreGeneration())
		prepared[auth.ID] = next
	}
	s.mu.Unlock()

	return finalize(func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		for id, auth := range prepared {
			s.rows[id] = auth.Clone()
		}
		return nil
	})
}

func TestManagerSetDisabledImmediatelyEnablesWithCommittedGeneration(t *testing.T) {
	const authID = "generation-disable-enable"
	store := newGenerationCASBatchStore(newHAStatusAuth(authID, false), 5)
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	if err := manager.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile() error: %v", err)
	}

	if _, errDisable := manager.SetDisabled(context.Background(), []string{authID}, true); errDisable != nil {
		t.Fatalf("SetDisabled(disabled=true) error: %v", errDisable)
	}
	disabled, ok := manager.GetByID(authID)
	if !ok || disabled == nil || !disabled.Disabled {
		t.Fatalf("disabled runtime auth = %#v", disabled)
	}
	if disabled.StoreGeneration() != 6 {
		t.Fatalf("disabled StoreGeneration() = %d, want 6", disabled.StoreGeneration())
	}

	if _, errEnable := manager.SetDisabled(context.Background(), []string{authID}, false); errEnable != nil {
		t.Fatalf("immediate SetDisabled(disabled=false) error: %v", errEnable)
	}
	enabled, ok := manager.GetByID(authID)
	if !ok || enabled == nil || enabled.Disabled || enabled.Status != StatusActive {
		t.Fatalf("enabled runtime auth = %#v", enabled)
	}
	if enabled.StoreGeneration() != 7 {
		t.Fatalf("enabled StoreGeneration() = %d, want 7", enabled.StoreGeneration())
	}
}

func TestManagerConsecutiveUpdatesUseLatestCommittedGeneration(t *testing.T) {
	const authID = "generation-consecutive-update"
	store := newGenerationCASBatchStore(newHAStatusAuth(authID, false), 5)
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	if err := manager.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile() error: %v", err)
	}

	first, _ := manager.GetByID(authID)
	first.Metadata["access_token"] = "first"
	if _, errUpdate := manager.Update(context.Background(), first); errUpdate != nil {
		t.Fatalf("first Update() error: %v", errUpdate)
	}
	second, _ := manager.GetByID(authID)
	if second.StoreGeneration() != 6 {
		t.Fatalf("generation after first Update() = %d, want 6", second.StoreGeneration())
	}
	second.Metadata["access_token"] = "second"
	if _, errUpdate := manager.Update(context.Background(), second); errUpdate != nil {
		t.Fatalf("second Update() error: %v", errUpdate)
	}
	current, _ := manager.GetByID(authID)
	if current.StoreGeneration() != 7 {
		t.Fatalf("generation after second Update() = %d, want 7", current.StoreGeneration())
	}
}

func newHAAtomicBatchStore(auths ...*Auth) *haAtomicBatchStore {
	rows := make(map[string]*Auth, len(auths))
	for _, auth := range auths {
		if auth != nil {
			rows[auth.ID] = auth.Clone()
		}
	}
	return &haAtomicBatchStore{rows: rows}
}

func (s *haAtomicBatchStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*Auth, 0, len(s.rows))
	for _, auth := range s.rows {
		result = append(result, auth.Clone())
	}
	return result, nil
}

func (s *haAtomicBatchStore) GetByID(_ context.Context, id string) (*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	auth := s.rows[id]
	if auth == nil {
		return nil, nil
	}
	return auth.Clone(), nil
}

// Save deliberately models the legacy Postgres behavior: each committed row
// emits its own ID notification and can therefore expose a partial batch.
func (s *haAtomicBatchStore) Save(_ context.Context, auth *Auth) (string, error) {
	if auth == nil {
		return "", nil
	}
	s.mu.Lock()
	if auth.ID == s.failID {
		err := s.saveErr
		s.mu.Unlock()
		return "", err
	}
	s.rows[auth.ID] = auth.Clone()
	notify := s.notify
	cancel := s.cancelOnBatchCommit
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if notify != nil {
		notify(auth.ID)
	}
	return auth.ID, nil
}

func (s *haAtomicBatchStore) Delete(context.Context, string) error { return nil }

// SaveBatch models the optional HA capability expected by Manager. It checks
// all configured failures before finalization, commits every row in one swap,
// and emits one full-reload notification only after finalization succeeds.
func (s *haAtomicBatchStore) SaveBatch(ctx context.Context, auths []*Auth, finalize func(commit func() error) error) error {
	s.mu.Lock()
	failID := s.failID
	saveErr := s.saveErr
	for _, auth := range auths {
		if auth != nil && auth.ID == failID {
			s.mu.Unlock()
			return saveErr
		}
	}
	s.mu.Unlock()
	if s.beforeBatchFinalize != nil {
		s.beforeBatchFinalize()
	}

	commit := func() error {
		if s.commitErr != nil {
			return s.commitErr
		}
		next := make(map[string]*Auth, len(s.rows)+len(auths))
		s.mu.Lock()
		for id, auth := range s.rows {
			next[id] = auth.Clone()
		}
		for _, auth := range auths {
			if auth != nil {
				next[auth.ID] = auth.Clone()
			}
		}
		s.rows = next
		cancel := s.cancelOnBatchCommit
		s.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return nil
	}
	if err := finalize(commit); err != nil {
		return err
	}
	s.mu.Lock()
	notify := s.notify
	s.mu.Unlock()
	if notify != nil {
		notify("")
	}
	return nil
}

func newHAStatusAuth(id string, disabled bool) *Auth {
	status := StatusActive
	if disabled {
		status = StatusDisabled
	}
	return &Auth{
		ID:       id,
		Provider: "gemini",
		Disabled: disabled,
		Status:   status,
		Metadata: map[string]any{
			"type":     "gemini",
			"disabled": disabled,
		},
	}
}

func observeHABatchNotifications(t *testing.T, store *haAtomicBatchStore, peer *Manager, ids []string) *[]haAtomicBatchObservation {
	t.Helper()
	observations := &[]haAtomicBatchObservation{}
	store.mu.Lock()
	store.notify = func(payload string) {
		observation := haAtomicBatchObservation{
			payload: payload,
			auths:   make(map[string]*Auth, len(ids)),
		}
		observation.reloadErr = peer.ReloadByID(context.Background(), payload)
		for _, id := range ids {
			if auth, ok := peer.GetByID(id); ok {
				observation.auths[id] = auth
			}
		}
		_, errPick := peer.scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
		observation.admittedAny = errPick == nil
		*observations = append(*observations, observation)
	}
	store.mu.Unlock()
	return observations
}

func newHAStatusManagers(t *testing.T, store *haAtomicBatchStore) (*Manager, *Manager) {
	t.Helper()
	writer := NewManager(store, &RoundRobinSelector{}, nil)
	peer := NewManager(store, &RoundRobinSelector{}, nil)
	if err := writer.Reconcile(context.Background()); err != nil {
		t.Fatalf("writer.Reconcile() error = %v", err)
	}
	if err := peer.Reconcile(context.Background()); err != nil {
		t.Fatalf("peer.Reconcile() error = %v", err)
	}
	return writer, peer
}

func TestManagerSetDisabledFailedHAEnableNeverPublishesActiveAuthToPeer(t *testing.T) {
	ids := []string{"ha-enable-a", "ha-enable-b"}
	persistErr := errors.New("batch preflight failed")
	store := newHAAtomicBatchStore(newHAStatusAuth(ids[0], true), newHAStatusAuth(ids[1], true))
	store.failID = ids[1]
	store.saveErr = persistErr
	writer, peer := newHAStatusManagers(t, store)
	observations := observeHABatchNotifications(t, store, peer, ids)

	_, errEnable := writer.SetDisabled(context.Background(), ids, false)
	if !errors.Is(errEnable, persistErr) {
		t.Fatalf("SetDisabled(disabled=false) error = %v, want errors.Is(_, %v)", errEnable, persistErr)
	}
	for index, observation := range *observations {
		if observation.reloadErr != nil {
			t.Fatalf("peer observation %d ReloadByID(%q) error = %v", index, observation.payload, observation.reloadErr)
		}
		if observation.admittedAny {
			t.Fatalf("peer observation %d admitted an auth after failed atomic enable", index)
		}
		for _, id := range ids {
			auth := observation.auths[id]
			if auth != nil && (!auth.Disabled || auth.Status != StatusDisabled) {
				t.Fatalf("peer observation %d auth %q = %#v, want disabled throughout failed enable", index, id, auth)
			}
		}
	}
}

func TestManagerSetDisabledSuccessfulHAEnablePublishesOneAllActiveObservation(t *testing.T) {
	ids := []string{"ha-enable-a", "ha-enable-b"}
	store := newHAAtomicBatchStore(newHAStatusAuth(ids[0], true), newHAStatusAuth(ids[1], true))
	writer, peer := newHAStatusManagers(t, store)
	observations := observeHABatchNotifications(t, store, peer, ids)

	if _, errEnable := writer.SetDisabled(context.Background(), ids, false); errEnable != nil {
		t.Fatalf("SetDisabled(disabled=false) error = %v", errEnable)
	}
	if len(*observations) != 1 {
		t.Fatalf("peer observation count = %d, want 1", len(*observations))
	}
	observation := (*observations)[0]
	if observation.reloadErr != nil {
		t.Fatalf("peer ReloadByID(%q) error = %v", observation.payload, observation.reloadErr)
	}
	if observation.payload != "" {
		t.Fatalf("peer notification payload = %q, want empty batch payload", observation.payload)
	}
	if !observation.admittedAny {
		t.Fatal("peer did not admit an auth after successful atomic enable")
	}
	for _, id := range ids {
		auth := observation.auths[id]
		if auth == nil || auth.Disabled || auth.Status != StatusActive {
			t.Fatalf("peer auth %q = %#v, want active in sole observation", id, auth)
		}
	}
}

type unknownCommitBatchStore struct {
	mu                     sync.Mutex
	rows                   map[string]*Auth
	reloadDeadlineObserved chan time.Time
}

func (s *unknownCommitBatchStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	auths := make([]*Auth, 0, len(s.rows))
	for _, auth := range s.rows {
		auths = append(auths, auth.Clone())
	}
	return auths, nil
}

func (s *unknownCommitBatchStore) GetByID(ctx context.Context, id string) (*Auth, error) {
	if s.reloadDeadlineObserved != nil {
		deadline, _ := ctx.Deadline()
		s.reloadDeadlineObserved <- deadline
		return nil, errors.New("injected authoritative reload stop")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rows[id] == nil {
		return nil, nil
	}
	return s.rows[id].Clone(), nil
}

func (*unknownCommitBatchStore) Save(context.Context, *Auth) (string, error) { return "", nil }
func (*unknownCommitBatchStore) Delete(context.Context, string) error        { return nil }

func (s *unknownCommitBatchStore) SaveBatch(_ context.Context, auths []*Auth, finalize func(func() error) error) error {
	return finalize(func() error {
		candidates := make(map[string]uint64, len(auths))
		s.mu.Lock()
		for _, auth := range auths {
			if auth != nil {
				generation := auth.StoreGeneration() + 1
				auth.SetStoreGeneration(generation)
				s.rows[auth.ID] = auth.Clone()
				candidates[auth.ID] = generation
			}
		}
		s.mu.Unlock()
		return NewAuthStoreCommitUnknown(candidates, errors.New("commit acknowledgement lost"))
	})
}

func (s *unknownCommitBatchStore) WithAuthoritativeAuthBatch(ctx context.Context, ids []string, finalize func(map[string]AuthAuthoritativeState) error) error {
	if s.reloadDeadlineObserved != nil {
		deadline, _ := ctx.Deadline()
		for range ids {
			s.reloadDeadlineObserved <- deadline
		}
		return errors.New("injected authoritative reload stop")
	}
	s.mu.Lock()
	states := make(map[string]AuthAuthoritativeState, len(ids))
	for _, id := range ids {
		auth := s.rows[id]
		if auth == nil {
			states[id] = AuthAuthoritativeState{}
			continue
		}
		states[id] = AuthAuthoritativeState{Auth: auth.Clone(), Exists: true, Generation: auth.StoreGeneration()}
	}
	s.mu.Unlock()
	return finalize(states)
}

func TestManagerSetDisabledBatchCommitUnknownReloadsWriterState(t *testing.T) {
	ids := []string{"unknown-enable-a", "unknown-enable-b"}
	store := &unknownCommitBatchStore{rows: map[string]*Auth{
		ids[0]: newHAStatusAuth(ids[0], true),
		ids[1]: newHAStatusAuth(ids[1], true),
	}}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	if errReconcile := manager.Reconcile(context.Background()); errReconcile != nil {
		t.Fatalf("Reconcile() error: %v", errReconcile)
	}

	if _, errEnable := manager.SetDisabled(context.Background(), ids, false); errEnable != nil {
		t.Fatalf("SetDisabled() error = %v, want internally converged success", errEnable)
	}
	for _, id := range ids {
		auth, ok := manager.GetByID(id)
		if !ok || auth == nil || auth.Disabled || auth.Status != StatusActive {
			t.Fatalf("writer-reloaded auth %q = %#v, want active commit winner", id, auth)
		}
	}
}

func TestManagerSetDisabledCommitUnknownReloadIgnoresCallerDeadline(t *testing.T) {
	const authID = "unknown-enable-deadline"
	deadlineObserved := make(chan time.Time, 1)
	store := &unknownCommitBatchStore{
		rows:                   map[string]*Auth{authID: newHAStatusAuth(authID, true)},
		reloadDeadlineObserved: deadlineObserved,
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	if errReconcile := manager.Reconcile(context.Background()); errReconcile != nil {
		t.Fatalf("Reconcile() error: %v", errReconcile)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	startedAt := time.Now()
	if _, errEnable := manager.SetDisabled(ctx, []string{authID}, false); !errors.Is(errEnable, ErrAuthStoreCommitUnknown) {
		t.Fatalf("SetDisabled() error = %v, want outcome unknown", errEnable)
	}
	if elapsed := time.Since(startedAt); elapsed > 500*time.Millisecond {
		t.Fatalf("SetDisabled() took %s after reload returned immediately", elapsed)
	}
	deadline := <-deadlineObserved
	remaining := time.Until(deadline)
	if deadline.IsZero() || remaining < authStoreConflictReloadTimeout-time.Second || remaining > authStoreConflictReloadTimeout+time.Second {
		t.Fatalf("authoritative reload deadline = %v (remaining %s), want detached timeout near %s", deadline, remaining, authStoreConflictReloadTimeout)
	}
}

func TestManagerSetDisabledCommitUnknownReloadAddsFallbackDeadline(t *testing.T) {
	const authID = "unknown-enable-fallback-deadline"
	deadlineObserved := make(chan time.Time, 1)
	store := &unknownCommitBatchStore{
		rows:                   map[string]*Auth{authID: newHAStatusAuth(authID, true)},
		reloadDeadlineObserved: deadlineObserved,
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	if errReconcile := manager.Reconcile(context.Background()); errReconcile != nil {
		t.Fatalf("Reconcile() error: %v", errReconcile)
	}

	if _, errEnable := manager.SetDisabled(context.Background(), []string{authID}, false); !errors.Is(errEnable, ErrAuthStoreCommitUnknown) {
		t.Fatalf("SetDisabled() error = %v, want outcome unknown", errEnable)
	}
	deadline := <-deadlineObserved
	remaining := time.Until(deadline)
	if deadline.IsZero() || remaining <= 0 || remaining > authStoreConflictReloadTimeout+time.Second {
		t.Fatalf("authoritative reload deadline = %v (remaining %s), want bounded fallback", deadline, remaining)
	}
}

func TestManagerSetDisabledCommitUnknownReloadSharesFallbackDeadlineAcrossBatch(t *testing.T) {
	ids := []string{"unknown-enable-batch-deadline-a", "unknown-enable-batch-deadline-b"}
	deadlineObserved := make(chan time.Time, len(ids))
	store := &unknownCommitBatchStore{
		rows: map[string]*Auth{
			ids[0]: newHAStatusAuth(ids[0], true),
			ids[1]: newHAStatusAuth(ids[1], true),
		},
		reloadDeadlineObserved: deadlineObserved,
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	if errReconcile := manager.Reconcile(context.Background()); errReconcile != nil {
		t.Fatalf("Reconcile() error: %v", errReconcile)
	}

	if _, errEnable := manager.SetDisabled(context.Background(), ids, false); !errors.Is(errEnable, ErrAuthStoreCommitUnknown) {
		t.Fatalf("SetDisabled() error = %v, want outcome unknown", errEnable)
	}
	firstDeadline := <-deadlineObserved
	secondDeadline := <-deadlineObserved
	if firstDeadline.IsZero() || !firstDeadline.Equal(secondDeadline) {
		t.Fatalf("reload deadlines = (%v, %v), want one shared batch deadline", firstDeadline, secondDeadline)
	}
}

func TestManagerSetDisabledSuccessfulHADisablePublishesOneAllDisabledObservation(t *testing.T) {
	ids := []string{"ha-disable-a", "ha-disable-b"}
	store := newHAAtomicBatchStore(newHAStatusAuth(ids[0], false), newHAStatusAuth(ids[1], false))
	writer, peer := newHAStatusManagers(t, store)
	observations := observeHABatchNotifications(t, store, peer, ids)

	if _, errDisable := writer.SetDisabled(context.Background(), ids, true); errDisable != nil {
		t.Fatalf("SetDisabled(disabled=true) error = %v", errDisable)
	}
	if len(*observations) != 1 {
		t.Fatalf("peer observation count = %d, want 1", len(*observations))
	}
	observation := (*observations)[0]
	if observation.reloadErr != nil {
		t.Fatalf("peer ReloadByID(%q) error = %v", observation.payload, observation.reloadErr)
	}
	if observation.payload != "" {
		t.Fatalf("peer notification payload = %q, want empty batch payload", observation.payload)
	}
	if observation.admittedAny {
		t.Fatal("peer admitted an auth after successful atomic disable")
	}
	for _, id := range ids {
		auth := observation.auths[id]
		if auth == nil || !auth.Disabled || auth.Status != StatusDisabled {
			t.Fatalf("peer auth %q = %#v, want disabled in sole observation", id, auth)
		}
	}
}

func TestManagerSetDisabledHAEnableCommitWinsLateContextCancellation(t *testing.T) {
	ids := []string{"ha-cancel-a", "ha-cancel-b"}
	store := newHAAtomicBatchStore(newHAStatusAuth(ids[0], true), newHAStatusAuth(ids[1], true))
	writer, _ := newHAStatusManagers(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.cancelOnBatchCommit = cancel

	updated, errEnable := writer.SetDisabled(ctx, ids, false)
	if errEnable != nil {
		t.Fatalf("SetDisabled(disabled=false) error = %v after committed batch, want nil", errEnable)
	}
	if len(updated) != len(ids) {
		t.Fatalf("len(SetDisabled() result) = %d, want %d", len(updated), len(ids))
	}
	for _, id := range ids {
		auth, ok := writer.GetByID(id)
		if !ok || auth == nil || auth.Disabled || auth.Status != StatusActive {
			t.Fatalf("writer auth %q = %#v, want active after commit-wins cancellation", id, auth)
		}
	}
}

func TestManagerSetDisabledHAEnableCancellationBeforeCommitKeepsPeerDisabled(t *testing.T) {
	ids := []string{"ha-cancel-before-a", "ha-cancel-before-b"}
	store := newHAAtomicBatchStore(newHAStatusAuth(ids[0], true), newHAStatusAuth(ids[1], true))
	writer, peer := newHAStatusManagers(t, store)
	observations := observeHABatchNotifications(t, store, peer, ids)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.beforeBatchFinalize = cancel

	_, errEnable := writer.SetDisabled(ctx, ids, false)
	if !errors.Is(errEnable, context.Canceled) {
		t.Fatalf("SetDisabled(disabled=false) error = %v, want context.Canceled", errEnable)
	}
	if len(*observations) != 0 {
		t.Fatalf("peer observations = %#v, want none before canceled commit", *observations)
	}
	for _, manager := range []*Manager{writer, peer} {
		for _, id := range ids {
			auth, ok := manager.GetByID(id)
			if !ok || auth == nil || !auth.Disabled || auth.Status != StatusDisabled {
				t.Fatalf("manager auth %q = %#v, want disabled after pre-commit cancellation", id, auth)
			}
		}
	}
}

func TestManagerSetDisabledHAEnableConflictBeforeCommitKeepsPeerDisabled(t *testing.T) {
	ids := []string{"ha-conflict-a", "ha-conflict-b"}
	store := newHAAtomicBatchStore(newHAStatusAuth(ids[0], true), newHAStatusAuth(ids[1], true))
	writer, peer := newHAStatusManagers(t, store)
	observations := observeHABatchNotifications(t, store, peer, ids)
	var updateErr error
	store.beforeBatchFinalize = func() {
		current, _ := writer.GetByID(ids[0])
		current.Label = "refreshed-during-enable"
		_, updateErr = writer.Update(WithSkipPersist(context.Background()), current)
	}

	_, errEnable := writer.SetDisabled(context.Background(), ids, false)
	if errEnable == nil || !strings.Contains(errEnable.Error(), "changed while enabling") {
		t.Fatalf("SetDisabled(disabled=false) error = %v, want durable revision conflict", errEnable)
	}
	if updateErr != nil {
		t.Fatalf("Update() during batch prepare error = %v", updateErr)
	}
	if len(*observations) != 0 {
		t.Fatalf("peer observations = %#v, want none before conflicted commit", *observations)
	}
	for _, id := range ids {
		auth, ok := peer.GetByID(id)
		if !ok || auth == nil || !auth.Disabled || auth.Status != StatusDisabled {
			t.Fatalf("peer auth %q = %#v, want disabled after commit conflict", id, auth)
		}
	}
}

func TestManagerSetDisabledHAEnableCommitFailureKeepsPeerDisabled(t *testing.T) {
	ids := []string{"ha-commit-fail-a", "ha-commit-fail-b"}
	commitErr := errors.New("batch commit failed")
	store := newHAAtomicBatchStore(newHAStatusAuth(ids[0], true), newHAStatusAuth(ids[1], true))
	store.commitErr = commitErr
	writer, peer := newHAStatusManagers(t, store)
	observations := observeHABatchNotifications(t, store, peer, ids)

	_, errEnable := writer.SetDisabled(context.Background(), ids, false)
	if !errors.Is(errEnable, commitErr) {
		t.Fatalf("SetDisabled(disabled=false) error = %v, want errors.Is(_, %v)", errEnable, commitErr)
	}
	if len(*observations) != 0 {
		t.Fatalf("peer observations = %#v, want none after failed commit", *observations)
	}
	for _, manager := range []*Manager{writer, peer} {
		for _, id := range ids {
			auth, ok := manager.GetByID(id)
			if !ok || auth == nil || !auth.Disabled || auth.Status != StatusDisabled {
				t.Fatalf("manager auth %q = %#v, want disabled after commit failure", id, auth)
			}
		}
	}
}
