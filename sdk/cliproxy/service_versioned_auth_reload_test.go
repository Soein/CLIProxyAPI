package cliproxy

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/watcher"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

type blockingModelRegistrationTransport struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (t *blockingModelRegistrationTransport) RoundTrip(*http.Request) (*http.Response, error) {
	t.once.Do(func() { close(t.started) })
	<-t.release
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(`{"webSearchModelIds":[]}`)),
	}, nil
}

type serviceAuthReloadStore struct {
	mu             sync.Mutex
	auth           *coreauth.Auth
	saveCalls      int
	versionedCalls int
	restoreCalls   int
	scrubbedIDs    []string
	scrubErr       error
}

func (s *serviceAuthReloadStore) List(context.Context) ([]*coreauth.Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.auth == nil {
		return nil, nil
	}
	return []*coreauth.Auth{s.auth.Clone()}, nil
}

func (s *serviceAuthReloadStore) GetByID(_ context.Context, id string) (*coreauth.Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.auth == nil || s.auth.ID != id {
		return nil, nil
	}
	return s.auth.Clone(), nil
}

func (s *serviceAuthReloadStore) Save(_ context.Context, auth *coreauth.Auth) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveCalls++
	s.auth = auth.Clone()
	return "auth.json", nil
}

func (s *serviceAuthReloadStore) SaveVersioned(_ context.Context, auth *coreauth.Auth, _ uint64) (string, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.versionedCalls++
	s.auth = auth.Clone()
	return "auth.json", 1, nil
}

func (s *serviceAuthReloadStore) Restore(_ context.Context, auth *coreauth.Auth, _ uint64) (string, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.restoreCalls++
	s.auth = auth.Clone()
	s.auth.SetStoreGeneration(1)
	return "auth.json", 1, nil
}

func (s *serviceAuthReloadStore) Delete(context.Context, string) error { return nil }

func (s *serviceAuthReloadStore) ScrubDeletedAuthMirror(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scrubbedIDs = append(s.scrubbedIDs, id)
	return s.scrubErr
}

func (s *serviceAuthReloadStore) setAuth(auth *coreauth.Auth) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if auth == nil {
		s.auth = nil
		return
	}
	s.auth = auth.Clone()
}

func (s *serviceAuthReloadStore) persistenceCalls() (int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveCalls, s.versionedCalls, s.restoreCalls
}

func (s *serviceAuthReloadStore) scrubCalls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.scrubbedIDs...)
}

type serviceNonVersionedStore struct {
	mu        sync.Mutex
	saveCalls int
}

type serviceMirrorReconcileStore struct {
	*serviceAuthReloadStore
	mirrorMu  sync.Mutex
	singleIDs []string
	fullCalls int
	fullErr   error
}

func (s *serviceMirrorReconcileStore) ReconcileAuthMirror(_ context.Context, id string) error {
	s.mirrorMu.Lock()
	s.singleIDs = append(s.singleIDs, id)
	s.mirrorMu.Unlock()
	return nil
}

func (s *serviceMirrorReconcileStore) ReconcileAuthMirrors(context.Context) error {
	s.mirrorMu.Lock()
	s.fullCalls++
	err := s.fullErr
	s.mirrorMu.Unlock()
	return err
}

func (s *serviceMirrorReconcileStore) mirrorCalls() ([]string, int) {
	s.mirrorMu.Lock()
	defer s.mirrorMu.Unlock()
	return append([]string(nil), s.singleIDs...), s.fullCalls
}

func (*serviceNonVersionedStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }
func (s *serviceNonVersionedStore) Save(context.Context, *coreauth.Auth) (string, error) {
	s.mu.Lock()
	s.saveCalls++
	s.mu.Unlock()
	return "auth.json", nil
}
func (*serviceNonVersionedStore) Delete(context.Context, string) error { return nil }

func TestServiceReloadAuthRuntimeByIDRegistersAndRemovesRemoteAuth(t *testing.T) {
	authID := "remote-auth-runtime"
	store := &serviceAuthReloadStore{}
	store.setAuth(&coreauth.Auth{
		ID:       authID,
		Provider: "claude",
		Status:   coreauth.StatusActive,
	})
	manager := coreauth.NewManager(store, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	exists, errReload := service.reloadAuthRuntimeByID(context.Background(), authID)
	if errReload != nil {
		t.Fatalf("reloadAuthRuntimeByID(add) error = %v", errReload)
	}
	if !exists {
		t.Fatal("reloadAuthRuntimeByID(add) reported missing auth")
	}
	models := registry.GetGlobalRegistry().GetModelsForClient(authID)
	if len(models) == 0 {
		t.Fatal("remote auth did not register models")
	}
	selected, errSelect := manager.SelectAuth(context.Background(), "claude", models[0].ID, cliproxyexecutor.Options{})
	if errSelect != nil {
		t.Fatalf("SelectAuth() after remote add error = %v", errSelect)
	}
	if selected == nil || selected.ID != authID {
		t.Fatalf("SelectAuth() = %#v, want auth %q", selected, authID)
	}

	store.setAuth(nil)
	exists, errReload = service.reloadAuthRuntimeByID(context.Background(), authID)
	if errReload != nil {
		t.Fatalf("reloadAuthRuntimeByID(delete) error = %v", errReload)
	}
	if exists {
		t.Fatal("reloadAuthRuntimeByID(delete) reported existing auth")
	}
	if _, found := manager.GetByID(authID); found {
		t.Fatal("remote delete retained manager auth")
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(authID); len(models) != 0 {
		t.Fatalf("remote delete retained %d registered model(s)", len(models))
	}
	if scrubbed := store.scrubCalls(); len(scrubbed) != 1 || scrubbed[0] != authID {
		t.Fatalf("scrubbed auth mirrors = %v, want [%s]", scrubbed, authID)
	}
}

func TestServiceAuthMirrorReconcilerRunsSingleAndFullConvergence(t *testing.T) {
	authID := "mirror-reconcile-auth"
	baseStore := &serviceAuthReloadStore{}
	baseStore.setAuth(&coreauth.Auth{ID: authID, Provider: "claude", Status: coreauth.StatusActive})
	store := &serviceMirrorReconcileStore{serviceAuthReloadStore: baseStore}
	manager := coreauth.NewManager(store, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	if _, errReload := service.reloadAuthRuntimeByID(context.Background(), authID); errReload != nil {
		t.Fatalf("reloadAuthRuntimeByID() error = %v", errReload)
	}
	if errFull := service.reconcileAuthMirrors(context.Background()); errFull != nil {
		t.Fatalf("reconcileAuthMirrors() error = %v", errFull)
	}
	singleIDs, fullCalls := store.mirrorCalls()
	if len(singleIDs) != 1 || singleIDs[0] != authID {
		t.Fatalf("single mirror reconciles = %v, want [%s]", singleIDs, authID)
	}
	if fullCalls != 1 {
		t.Fatalf("full mirror reconciles = %d, want 1", fullCalls)
	}
}

func TestServiceReconcileAuthRuntimeHealsMissedAddAndDeleteNotifications(t *testing.T) {
	authID := "missed-notify-auth"
	store := &serviceAuthReloadStore{}
	manager := coreauth.NewManager(store, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	auth := &coreauth.Auth{
		ID:       authID,
		Provider: "claude",
		Status:   coreauth.StatusActive,
	}
	auth.SetStoreGeneration(1)
	store.setAuth(auth)
	if errReconcile := service.reconcileAuthRuntime(context.Background()); errReconcile != nil {
		t.Fatalf("reconcileAuthRuntime(add) error = %v", errReconcile)
	}
	if _, exists := manager.GetByID(authID); !exists {
		t.Fatal("missed add did not reach Manager")
	}
	models := registry.GetGlobalRegistry().GetModelsForClient(authID)
	if len(models) == 0 {
		t.Fatal("missed add did not register models")
	}
	selected, errSelect := manager.SelectAuth(context.Background(), "claude", models[0].ID, cliproxyexecutor.Options{})
	if errSelect != nil || selected == nil || selected.ID != authID {
		t.Fatalf("SelectAuth() after reconcile = (%#v, %v), want %q", selected, errSelect, authID)
	}

	store.setAuth(nil)
	if errReconcile := service.reconcileAuthRuntime(context.Background()); errReconcile != nil {
		t.Fatalf("reconcileAuthRuntime(delete) error = %v", errReconcile)
	}
	if _, exists := manager.GetByID(authID); exists {
		t.Fatal("missed delete retained Manager auth")
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(authID); len(models) != 0 {
		t.Fatalf("missed delete retained %d registered model(s)", len(models))
	}
	if saves, versioned, restores := store.persistenceCalls(); saves != 0 || versioned != 0 || restores != 0 {
		t.Fatalf("reconcile persisted auth: Save=%d SaveVersioned=%d Restore=%d", saves, versioned, restores)
	}
}

func TestServiceReconcileAuthRuntimeDoesNotRestoreModelsAfterConcurrentDelete(t *testing.T) {
	authID := "reconcile-delete-race"
	store := &serviceAuthReloadStore{}
	auth := &coreauth.Auth{
		ID:       authID,
		Provider: "antigravity",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"access_token": "test-token"},
	}
	auth.SetStoreGeneration(1)
	store.setAuth(auth)
	manager := coreauth.NewManager(store, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	transport := &blockingModelRegistrationTransport{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	previousTransport := http.DefaultTransport
	http.DefaultTransport = transport
	t.Cleanup(func() { http.DefaultTransport = previousTransport })

	reconcileErr := make(chan error, 1)
	go func() { reconcileErr <- service.reconcileAuthRuntime(context.Background()) }()
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("reconcile did not reach model registration")
	}
	manager.Remove(coreauth.WithSkipPersist(context.Background()), authID)
	close(transport.release)
	if errReconcile := <-reconcileErr; errReconcile != nil {
		t.Fatalf("reconcileAuthRuntime() error = %v", errReconcile)
	}
	if _, exists := manager.GetByID(authID); exists {
		t.Fatal("concurrent delete did not remove Manager auth")
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(authID); len(models) != 0 {
		t.Fatalf("concurrent delete was resurrected with %d registered model(s)", len(models))
	}
}

func TestServiceReconcileAuthRuntimeRefreshesConcurrentProviderReplacement(t *testing.T) {
	authID := "reconcile-provider-race"
	store := &serviceAuthReloadStore{}
	auth := &coreauth.Auth{
		ID:       authID,
		Provider: "antigravity",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"access_token": "test-token"},
	}
	auth.SetStoreGeneration(1)
	store.setAuth(auth)
	manager := coreauth.NewManager(store, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	transport := &blockingModelRegistrationTransport{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	previousTransport := http.DefaultTransport
	http.DefaultTransport = transport
	t.Cleanup(func() { http.DefaultTransport = previousTransport })

	reconcileErr := make(chan error, 1)
	go func() { reconcileErr <- service.reconcileAuthRuntime(context.Background()) }()
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("reconcile did not reach stale-provider model registration")
	}
	replacement, exists := manager.GetByID(authID)
	if !exists {
		t.Fatal("Manager auth disappeared before provider replacement")
	}
	replacement.Provider = "claude"
	replacement.Metadata = nil
	if _, errUpdate := manager.Update(coreauth.WithSkipPersist(context.Background()), replacement); errUpdate != nil {
		t.Fatalf("concurrent provider Update() error = %v", errUpdate)
	}
	close(transport.release)
	if errReconcile := <-reconcileErr; errReconcile != nil {
		t.Fatalf("reconcileAuthRuntime() error = %v", errReconcile)
	}
	latest, exists := manager.GetByID(authID)
	if !exists || latest.Provider != "claude" {
		t.Fatalf("latest auth = %#v, want claude provider", latest)
	}
	claudeModels := registry.GetClaudeModels()
	if len(claudeModels) == 0 || !registry.GetGlobalRegistry().ClientSupportsModel(authID, claudeModels[0].ID) {
		t.Fatal("concurrent provider replacement retained stale model registration")
	}
}

func TestServiceVersionedRuntimeOnlyAddKeepsSnapshotBehavior(t *testing.T) {
	store := &serviceAuthReloadStore{}
	manager := coreauth.NewManager(store, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	authID := "versioned-runtime-only-auth"
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	service.handleAuthUpdate(context.Background(), watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionAdd,
		ID:     authID,
		Auth: &coreauth.Auth{
			ID:       authID,
			Provider: "aistudio",
			Status:   coreauth.StatusActive,
			Attributes: map[string]string{
				coreauth.AttributeRuntimeOnly: "true",
			},
			Metadata: map[string]any{"source": "websocket"},
		},
	})

	loaded, exists := manager.GetByID(authID)
	if !exists || loaded == nil || loaded.AuthSourceKind() != coreauth.AuthSourceMemory {
		t.Fatalf("runtime-only auth = %#v, want in-memory watcher snapshot", loaded)
	}
	if saves, versioned, restores := store.persistenceCalls(); saves != 0 || versioned != 0 || restores != 0 {
		t.Fatalf("runtime-only handling persisted auth: Save=%d SaveVersioned=%d Restore=%d", saves, versioned, restores)
	}
}

func TestServiceVersionedWatcherCannotRestoreAuthoritativeTombstone(t *testing.T) {
	store := &serviceAuthReloadStore{}
	manager := coreauth.NewManager(store, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	authID := "tombstoned-watcher-auth.json"

	service.handleAuthUpdate(context.Background(), watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionAdd,
		ID:     authID,
		Auth: &coreauth.Auth{
			ID:       authID,
			FileName: authID,
			Provider: "xai",
			Status:   coreauth.StatusActive,
			Attributes: map[string]string{
				coreauth.AttributeSourceBackend: coreauth.AuthSourcePostgres,
			},
			Metadata: map[string]any{"type": "xai", "token": "stale"},
		},
	})

	if current, exists := manager.GetByID(authID); exists || current != nil {
		t.Fatalf("watcher restored tombstoned auth: %#v", current)
	}
	if saves, versioned, restores := store.persistenceCalls(); saves != 0 || versioned != 0 || restores != 0 {
		t.Fatalf("watcher persisted tombstoned auth: Save=%d SaveVersioned=%d Restore=%d", saves, versioned, restores)
	}
}

func TestServiceNonVersionedWatcherAddKeepsSnapshotBehaviorWithoutPersisting(t *testing.T) {
	store := &serviceNonVersionedStore{}
	manager := coreauth.NewManager(store, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	authID := "non-versioned-watcher-auth"
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	service.handleAuthUpdate(context.Background(), watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionAdd,
		ID:     authID,
		Auth: &coreauth.Auth{
			ID:       authID,
			Provider: "claude",
			Label:    "file snapshot",
			Status:   coreauth.StatusActive,
			Metadata: map[string]any{"source": "watcher"},
		},
	})

	loaded, exists := manager.GetByID(authID)
	if !exists || loaded == nil || loaded.Label != "file snapshot" {
		t.Fatalf("non-versioned watcher auth = %#v, want file snapshot", loaded)
	}
	store.mu.Lock()
	saveCalls := store.saveCalls
	store.mu.Unlock()
	if saveCalls != 0 {
		t.Fatalf("inline watcher handling called Store.Save %d time(s)", saveCalls)
	}
}
