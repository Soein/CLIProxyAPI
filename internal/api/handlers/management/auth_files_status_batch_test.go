package management

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/authfilelock"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v7/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

type statusFailingStore struct {
	err error
}

type statusBlockingFailStore struct {
	err     error
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
	calls   int
}

type statusCommitUnknownStore struct {
	mu                  sync.Mutex
	rows                map[string]*coreauth.Auth
	unknownOnce         bool
	applyUnknownAgain   bool
	rollbackUnknownOnce bool
	outcomes            []statusSaveOutcome
	history             []statusSaveRecord
	cancelOnCall        int
	cancel              context.CancelFunc
	contextErrors       []error
	saveCalls           int
}

type statusStaleReloadVersionedStore struct {
	mu        sync.Mutex
	row       *coreauth.Auth
	reloadRow *coreauth.Auth
	saveCalls int
}

type statusSaveOutcome struct {
	apply   bool
	unknown bool
}

type statusSaveRecord struct {
	id       string
	disabled bool
	applied  bool
	unknown  bool
}

func (*statusFailingStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }

func (s *statusFailingStore) Save(context.Context, *coreauth.Auth) (string, error) {
	return "", s.err
}

func (*statusFailingStore) Delete(context.Context, string) error { return nil }

func (*statusBlockingFailStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }

func (s *statusBlockingFailStore) Save(context.Context, *coreauth.Auth) (string, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call == 1 {
		close(s.entered)
		<-s.release
		return "", s.err
	}
	return "", nil
}

func (*statusBlockingFailStore) Delete(context.Context, string) error { return nil }

func (s *statusCommitUnknownStore) List(context.Context) ([]*coreauth.Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	auths := make([]*coreauth.Auth, 0, len(s.rows))
	for _, auth := range s.rows {
		if auth != nil {
			auths = append(auths, auth.Clone())
		}
	}
	return auths, nil
}

func (s *statusCommitUnknownStore) Save(ctx context.Context, auth *coreauth.Auth) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveCalls++
	if s.saveCalls == s.cancelOnCall && s.cancel != nil {
		s.cancel()
	}
	s.contextErrors = append(s.contextErrors, ctx.Err())
	if len(s.outcomes) > 0 {
		outcome := s.outcomes[0]
		s.outcomes = s.outcomes[1:]
		record := statusSaveRecord{applied: outcome.apply, unknown: outcome.unknown}
		if auth != nil {
			record.id = auth.ID
			record.disabled = auth.Disabled
			if outcome.apply {
				auth.SetStoreGeneration(s.nextGenerationLocked(auth.ID))
				s.rows[auth.ID] = auth.Clone()
			}
		}
		s.history = append(s.history, record)
		if outcome.unknown {
			return "", s.unknownErrorLocked(auth)
		}
		if auth == nil {
			return "", nil
		}
		return auth.ID, nil
	}
	if s.unknownOnce {
		s.unknownOnce = false
		if auth != nil {
			auth.SetStoreGeneration(s.nextGenerationLocked(auth.ID))
			s.rows[auth.ID] = auth.Clone()
		}
		return "", s.unknownErrorLocked(auth)
	}
	if s.applyUnknownAgain {
		s.applyUnknownAgain = false
		if auth != nil {
			auth.SetStoreGeneration(s.nextGenerationLocked(auth.ID))
			s.rows[auth.ID] = auth.Clone()
		}
		return "", s.unknownErrorLocked(auth)
	}
	if s.rollbackUnknownOnce {
		s.rollbackUnknownOnce = false
		return "", s.unknownErrorLocked(auth)
	}
	if auth == nil {
		return "", nil
	}
	s.rows[auth.ID] = auth.Clone()
	return auth.ID, nil
}

func (s *statusCommitUnknownStore) nextGenerationLocked(id string) uint64 {
	if current := s.rows[id]; current != nil {
		return current.StoreGeneration() + 1
	}
	return 1
}

func (s *statusCommitUnknownStore) unknownErrorLocked(auth *coreauth.Auth) error {
	if auth == nil {
		return coreauth.ErrAuthStoreCommitUnknown
	}
	generation := auth.StoreGeneration()
	if generation == 0 {
		generation = s.nextGenerationLocked(auth.ID)
	}
	return coreauth.NewAuthStoreCommitUnknown(map[string]uint64{auth.ID: generation}, errors.New("commit acknowledgement lost"))
}

func (*statusCommitUnknownStore) Delete(context.Context, string) error { return nil }

func (s *statusCommitUnknownStore) ListAuthoritative(ctx context.Context) ([]*coreauth.Auth, error) {
	return s.List(ctx)
}

func (s *statusCommitUnknownStore) WithAuthoritativeAuthBatch(_ context.Context, ids []string, finalize func(map[string]coreauth.AuthAuthoritativeState) error) error {
	s.mu.Lock()
	states := make(map[string]coreauth.AuthAuthoritativeState, len(ids))
	for _, id := range ids {
		auth := s.rows[id]
		if auth == nil {
			states[id] = coreauth.AuthAuthoritativeState{}
			continue
		}
		states[id] = coreauth.AuthAuthoritativeState{Auth: auth.Clone(), Exists: true, Generation: auth.StoreGeneration()}
	}
	s.mu.Unlock()
	return finalize(states)
}

func (s *statusCommitUnknownStore) auth(id string) *coreauth.Auth {
	s.mu.Lock()
	defer s.mu.Unlock()
	if auth := s.rows[id]; auth != nil {
		return auth.Clone()
	}
	return nil
}

func (s *statusCommitUnknownStore) saveHistory() []statusSaveRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]statusSaveRecord(nil), s.history...)
}

func (s *statusCommitUnknownStore) savedContextErrors() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.contextErrors...)
}

func (s *statusStaleReloadVersionedStore) List(context.Context) ([]*coreauth.Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.row == nil {
		return nil, nil
	}
	return []*coreauth.Auth{s.row.Clone()}, nil
}

func (s *statusStaleReloadVersionedStore) ListAuthoritative(ctx context.Context) ([]*coreauth.Auth, error) {
	return s.List(ctx)
}

func (s *statusStaleReloadVersionedStore) WithAuthoritativeAuthBatch(_ context.Context, ids []string, finalize func(map[string]coreauth.AuthAuthoritativeState) error) error {
	s.mu.Lock()
	states := make(map[string]coreauth.AuthAuthoritativeState, len(ids))
	for _, id := range ids {
		auth := s.row
		if auth == nil || auth.ID != id {
			states[id] = coreauth.AuthAuthoritativeState{}
			continue
		}
		states[id] = coreauth.AuthAuthoritativeState{Auth: auth.Clone(), Exists: true, Generation: auth.StoreGeneration()}
	}
	s.mu.Unlock()
	return finalize(states)
}

func (s *statusStaleReloadVersionedStore) GetByID(_ context.Context, id string) (*coreauth.Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reloadRow == nil || s.reloadRow.ID != id {
		return nil, nil
	}
	return s.reloadRow.Clone(), nil
}

func (s *statusStaleReloadVersionedStore) Save(ctx context.Context, auth *coreauth.Auth) (string, error) {
	path, _, errSave := s.SaveVersioned(ctx, auth, auth.StoreGeneration())
	return path, errSave
}

func (s *statusStaleReloadVersionedStore) SaveVersioned(_ context.Context, auth *coreauth.Auth, expected uint64) (string, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveCalls++
	if s.saveCalls > 1 {
		return "", 0, coreauth.ErrAuthStoreConflict
	}
	candidate := expected + 1
	committed := auth.Clone()
	committed.SetStoreGeneration(candidate)
	s.row = committed
	auth.SetStoreGeneration(candidate)
	return auth.ID, candidate, coreauth.NewAuthStoreCommitUnknown(map[string]uint64{auth.ID: candidate}, errors.New("commit verification unavailable"))
}

func (*statusStaleReloadVersionedStore) Delete(context.Context, string) error { return nil }

func TestPatchAuthFileStatus_DisablesAllNamedRuntimeAuths(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	for _, name := range []string{"a.json", "b.json"} {
		if _, errRegister := manager.Register(context.Background(), &coreauth.Auth{
			ID:       name,
			FileName: name,
			Provider: "codex",
		}); errRegister != nil {
			t.Fatalf("register auth %s: %v", name, errRegister)
		}
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodPatch,
		"/v0/management/auth-files/status",
		strings.NewReader(`{"names":["a.json","b.json"],"disabled":true}`),
	)
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.PatchAuthFileStatus(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	for _, name := range []string{"a.json", "b.json"} {
		auth, ok := manager.GetByID(name)
		if !ok || auth == nil {
			t.Fatalf("expected auth %s to remain registered", name)
		}
		if !auth.Disabled || auth.Status != coreauth.StatusDisabled {
			t.Fatalf("auth %s disabled/status = %v/%s, want true/%s", name, auth.Disabled, auth.Status, coreauth.StatusDisabled)
		}
	}
}

func TestPatchAuthFileStatus_AcceptsLegacyName(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	if _, errRegister := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "legacy.json",
		FileName: "legacy.json",
		Provider: "codex",
	}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodPatch,
		"/v0/management/auth-files/status",
		strings.NewReader(`{"name":"legacy.json","disabled":true}`),
	)
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.PatchAuthFileStatus(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	auth, ok := manager.GetByID("legacy.json")
	if !ok || auth == nil {
		t.Fatal("expected legacy auth to remain registered")
	}
	if !auth.Disabled || auth.Status != coreauth.StatusDisabled {
		t.Fatalf("legacy auth disabled/status = %v/%s, want true/%s", auth.Disabled, auth.Status, coreauth.StatusDisabled)
	}
}

func TestPatchAuthFileStatus_ResolvesBatchByFileName(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	for index, name := range []string{"file-a.json", "file-b.json"} {
		if _, errRegister := manager.Register(context.Background(), &coreauth.Auth{
			ID:       "runtime-auth-" + string(rune('a'+index)),
			FileName: name,
			Provider: "codex",
		}); errRegister != nil {
			t.Fatalf("register auth %s: %v", name, errRegister)
		}
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodPatch,
		"/v0/management/auth-files/status",
		strings.NewReader(`{"names":["file-a.json","file-b.json"],"disabled":true}`),
	)
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.PatchAuthFileStatus(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	for _, id := range []string{"runtime-auth-a", "runtime-auth-b"} {
		auth, ok := manager.GetByID(id)
		if !ok || auth == nil || !auth.Disabled {
			t.Fatalf("auth %q after batch status = %#v, want disabled", id, auth)
		}
	}
}

func TestPatchAuthFileStatus_PreflightsAllPluginSourcesBeforeWriting(t *testing.T) {
	authDir := t.TempDir()
	validName := "a-valid-source.json"
	invalidName := "z-invalid-source.json"
	validPath := filepath.Join(authDir, validName)
	invalidPath := filepath.Join(authDir, invalidName)
	validOriginal := []byte(`{"type":"gemini-cli","disabled":false}`)
	if errWrite := os.WriteFile(validPath, validOriginal, 0o600); errWrite != nil {
		t.Fatalf("write valid source: %v", errWrite)
	}
	if errWrite := os.WriteFile(invalidPath, []byte(`{"type":`), 0o600); errWrite != nil {
		t.Fatalf("write invalid source: %v", errWrite)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	for _, item := range []struct {
		name string
		id   string
	}{
		{name: validName, id: validName},
		{name: invalidName, id: invalidName},
	} {
		if _, errRegister := manager.Register(context.Background(), pluginVirtualAuthForTest(authDir, item.name, item.id)); errRegister != nil {
			t.Fatalf("register plugin source %s: %v", item.name, errRegister)
		}
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodPatch,
		"/v0/management/auth-files/status",
		strings.NewReader(`{"names":["a-valid-source.json","z-invalid-source.json"],"disabled":true}`),
	)
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.PatchAuthFileStatus(ctx)

	if rec.Code == http.StatusOK {
		t.Fatalf("status = %d, want source validation failure body=%s", rec.Code, rec.Body.String())
	}
	validAfter, errRead := os.ReadFile(validPath)
	if errRead != nil {
		t.Fatalf("read valid source: %v", errRead)
	}
	if string(validAfter) != string(validOriginal) {
		t.Fatalf("valid source changed before all sources passed validation: got %s want %s", validAfter, validOriginal)
	}
	for _, id := range []string{validName, invalidName} {
		auth, ok := manager.GetByID(id)
		if !ok || auth == nil {
			t.Fatalf("GetByID(%q) found = false", id)
		}
		if auth.Disabled || auth.Status == coreauth.StatusDisabled {
			t.Fatalf("auth %q changed after source preflight failure: %#v", id, auth)
		}
	}
}

func TestPatchAuthFileStatus_EnableFailureRestoresPluginSource(t *testing.T) {
	authDir := t.TempDir()
	sourceName := "plugin-source.json"
	sourcePath := filepath.Join(authDir, sourceName)
	sourceOriginal := []byte(`{"type":"gemini-cli","disabled":true}`)
	if errWrite := os.WriteFile(sourcePath, sourceOriginal, 0o600); errWrite != nil {
		t.Fatalf("write plugin source: %v", errWrite)
	}

	persistErr := errors.New("ordinary auth enable failed")
	manager := coreauth.NewManager(&statusFailingStore{err: persistErr}, nil, nil)
	pluginAuth := pluginVirtualAuthForTest(authDir, sourceName, sourceName)
	pluginAuth.Disabled = true
	pluginAuth.Status = coreauth.StatusDisabled
	pluginAuth.Metadata["disabled"] = true
	ordinaryAuth := &coreauth.Auth{
		ID:       "ordinary-runtime",
		FileName: "ordinary.json",
		Provider: "codex",
		Disabled: true,
		Status:   coreauth.StatusDisabled,
		Metadata: map[string]any{"type": "codex", "disabled": true},
	}
	for _, auth := range []*coreauth.Auth{pluginAuth, ordinaryAuth} {
		if _, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), auth); errRegister != nil {
			t.Fatalf("register auth %q: %v", auth.ID, errRegister)
		}
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodPatch,
		"/v0/management/auth-files/status",
		strings.NewReader(`{"names":["plugin-source.json","ordinary.json"],"disabled":false}`),
	)
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.PatchAuthFileStatus(ctx)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	sourceAfter, errRead := os.ReadFile(sourcePath)
	if errRead != nil {
		t.Fatalf("read plugin source: %v", errRead)
	}
	if string(sourceAfter) != string(sourceOriginal) {
		t.Fatalf("plugin source after failed enable = %s, want original %s", sourceAfter, sourceOriginal)
	}
	for _, id := range []string{sourceName, ordinaryAuth.ID} {
		auth, ok := manager.GetByID(id)
		if !ok || auth == nil || !auth.Disabled || auth.Status != coreauth.StatusDisabled {
			t.Fatalf("runtime auth %q after failed enable = %#v, want disabled", id, auth)
		}
	}
}

func TestPatchAuthFileStatus_EnableCommitUnknownResolvedActiveReturnsSuccess(t *testing.T) {
	const authID = "commit-unknown-active.json"
	store := &statusCommitUnknownStore{
		rows: map[string]*coreauth.Auth{
			authID: {
				ID:       authID,
				FileName: authID,
				Provider: "codex",
				Disabled: true,
				Status:   coreauth.StatusDisabled,
				Metadata: map[string]any{"type": "codex", "disabled": true},
			},
		},
		unknownOnce: true,
	}
	manager := coreauth.NewManager(store, nil, nil)
	if errReconcile := manager.Reconcile(context.Background()); errReconcile != nil {
		t.Fatalf("Reconcile() error: %v", errReconcile)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodPatch,
		"/v0/management/auth-files/status",
		strings.NewReader(`{"name":"commit-unknown-active.json","disabled":false}`),
	)
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.PatchAuthFileStatus(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	for _, auth := range []*coreauth.Auth{store.auth(authID)} {
		if auth == nil || auth.Disabled || auth.Status != coreauth.StatusActive {
			t.Fatalf("durable auth after resolved outcome unknown = %#v, want active", auth)
		}
	}
	current, ok := manager.GetByID(authID)
	if !ok || current == nil || current.Disabled || current.Status != coreauth.StatusActive {
		t.Fatalf("runtime auth after resolved outcome unknown = %#v, want active", current)
	}
}

func TestPatchAuthFileStatus_PluginEnableCommitUnknownResolvedActiveReturnsSuccess(t *testing.T) {
	authDir := t.TempDir()
	sourceName := "plugin-source.json"
	sourcePath := filepath.Join(authDir, sourceName)
	if errWrite := os.WriteFile(sourcePath, []byte(`{"type":"gemini-cli","disabled":true}`), 0o600); errWrite != nil {
		t.Fatalf("write plugin source: %v", errWrite)
	}

	ordinaryID := "ordinary-runtime"
	store := &statusCommitUnknownStore{
		rows: map[string]*coreauth.Auth{
			ordinaryID: {
				ID:         ordinaryID,
				FileName:   "ordinary.json",
				Provider:   "codex",
				Disabled:   true,
				Status:     coreauth.StatusDisabled,
				Metadata:   map[string]any{"type": "codex", "disabled": true},
				Attributes: map[string]string{"path": sourcePath},
			},
		},
		unknownOnce: true,
	}
	manager := coreauth.NewManager(store, nil, nil)
	if errReconcile := manager.Reconcile(context.Background()); errReconcile != nil {
		t.Fatalf("Reconcile() error: %v", errReconcile)
	}
	pluginAuth := pluginVirtualAuthForTest(authDir, sourceName, sourceName)
	pluginAuth.Disabled = true
	pluginAuth.Status = coreauth.StatusDisabled
	pluginAuth.Metadata["disabled"] = true
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), pluginAuth); errRegister != nil {
		t.Fatalf("register plugin auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodPatch,
		"/v0/management/auth-files/status",
		strings.NewReader(`{"name":"plugin-source.json","disabled":false}`),
	)
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.PatchAuthFileStatus(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	sourceRaw, errRead := os.ReadFile(sourcePath)
	if errRead != nil {
		t.Fatalf("read plugin source: %v", errRead)
	}
	var sourceMetadata map[string]any
	if errUnmarshal := json.Unmarshal(sourceRaw, &sourceMetadata); errUnmarshal != nil {
		t.Fatalf("unmarshal plugin source: %v", errUnmarshal)
	}
	if sourceMetadata["disabled"] != false {
		t.Fatalf("plugin source after resolved outcome = %s, want disabled=false", sourceRaw)
	}
	if durable := store.auth(ordinaryID); durable == nil || durable.Disabled || durable.Status != coreauth.StatusActive {
		t.Fatalf("durable ordinary auth after resolved outcome = %#v, want active", durable)
	}
	for _, id := range []string{sourceName, ordinaryID} {
		auth, ok := manager.GetByID(id)
		if !ok || auth == nil || auth.Disabled || auth.Status != coreauth.StatusActive {
			t.Fatalf("runtime auth %q after resolved outcome = %#v, want active", id, auth)
		}
	}
}

func TestPatchAuthFileStatus_PluginEnableUnknownPublishesWriterGenerationAtomically(t *testing.T) {
	authDir := t.TempDir()
	sourceName := "plugin-source.json"
	sourcePath := filepath.Join(authDir, sourceName)
	if errWrite := os.WriteFile(sourcePath, []byte(`{"type":"gemini-cli","disabled":true}`), 0o600); errWrite != nil {
		t.Fatalf("write plugin source: %v", errWrite)
	}

	ordinary := &coreauth.Auth{
		ID:         "ordinary-runtime",
		FileName:   "ordinary.json",
		Provider:   "codex",
		Disabled:   true,
		Status:     coreauth.StatusDisabled,
		Metadata:   map[string]any{"type": "codex", "disabled": true},
		Attributes: map[string]string{"path": sourcePath},
	}
	ordinary.SetStoreGeneration(1)
	store := &statusStaleReloadVersionedStore{row: ordinary.Clone(), reloadRow: ordinary.Clone()}
	manager := coreauth.NewManager(store, nil, nil)
	if errReconcile := manager.Reconcile(context.Background()); errReconcile != nil {
		t.Fatalf("Reconcile() error: %v", errReconcile)
	}
	pluginAuth := pluginVirtualAuthForTest(authDir, sourceName, sourceName)
	pluginAuth.Disabled = true
	pluginAuth.Status = coreauth.StatusDisabled
	pluginAuth.Metadata["disabled"] = true
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), pluginAuth); errRegister != nil {
		t.Fatalf("register plugin auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/status", strings.NewReader(`{"name":"plugin-source.json","disabled":false}`))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.PatchAuthFileStatus(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("response = %d %s, want successful authoritative convergence", rec.Code, rec.Body.String())
	}
	store.mu.Lock()
	writer := store.row.Clone()
	store.mu.Unlock()
	writerGeneration := uint64(0)
	if writer != nil {
		writerGeneration = writer.StoreGeneration()
	}
	if writer == nil || writer.Disabled || writerGeneration != 2 {
		t.Fatalf("writer auth = %#v generation=%d, want active candidate generation 2", writer, writerGeneration)
	}
	for _, id := range []string{sourceName, ordinary.ID} {
		auth, ok := manager.GetByID(id)
		if !ok || auth == nil || auth.Disabled || auth.Status != coreauth.StatusActive {
			t.Fatalf("runtime auth %q = %#v, want atomically active", id, auth)
		}
	}
	loaded, _ := manager.GetByID(ordinary.ID)
	if loaded.StoreGeneration() != 2 {
		t.Fatalf("runtime writer generation = %d, want 2", loaded.StoreGeneration())
	}
}

func TestPatchAuthFileStatus_EnableCommitUnknownPartialStateCompensatesDisabled(t *testing.T) {
	authDir := t.TempDir()
	sourceName := "plugin-source.json"
	sourcePath := filepath.Join(authDir, sourceName)
	if errWrite := os.WriteFile(sourcePath, []byte(`{"type":"gemini-cli","disabled":true}`), 0o600); errWrite != nil {
		t.Fatalf("write plugin source: %v", errWrite)
	}

	ordinaryIDs := []string{"ordinary-a", "ordinary-b", "ordinary-c"}
	rows := make(map[string]*coreauth.Auth, len(ordinaryIDs))
	for _, ordinaryID := range ordinaryIDs {
		rows[ordinaryID] = &coreauth.Auth{
			ID:         ordinaryID,
			FileName:   ordinaryID + ".json",
			Provider:   "codex",
			Disabled:   true,
			Status:     coreauth.StatusDisabled,
			Metadata:   map[string]any{"type": "codex", "disabled": true},
			Attributes: map[string]string{"path": sourcePath},
		}
	}
	store := &statusCommitUnknownStore{
		rows: rows,
		outcomes: []statusSaveOutcome{
			{apply: true},                // first enable commits
			{unknown: true},              // second enable rolls back with unknown outcome
			{apply: true},                // third enable commits
			{apply: true, unknown: true}, // first compensation commits but loses ACK
			{apply: true},                // second compensation commits
			{apply: true, unknown: true}, // third compensation commits but loses ACK
		},
	}
	manager := coreauth.NewManager(store, nil, nil)
	if errReconcile := manager.Reconcile(context.Background()); errReconcile != nil {
		t.Fatalf("Reconcile() error: %v", errReconcile)
	}
	pluginAuth := pluginVirtualAuthForTest(authDir, sourceName, sourceName)
	pluginAuth.Disabled = true
	pluginAuth.Status = coreauth.StatusDisabled
	pluginAuth.Metadata["disabled"] = true
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), pluginAuth); errRegister != nil {
		t.Fatalf("register plugin auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodPatch,
		"/v0/management/auth-files/status",
		strings.NewReader(`{"name":"plugin-source.json","disabled":false}`),
	)
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.PatchAuthFileStatus(ctx)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	sourceRaw, errRead := os.ReadFile(sourcePath)
	if errRead != nil {
		t.Fatalf("read plugin source: %v", errRead)
	}
	var sourceMetadata map[string]any
	if errUnmarshal := json.Unmarshal(sourceRaw, &sourceMetadata); errUnmarshal != nil {
		t.Fatalf("unmarshal plugin source: %v", errUnmarshal)
	}
	if sourceMetadata["disabled"] != true {
		t.Fatalf("plugin source after partial outcome = %s, want disabled=true", sourceRaw)
	}
	history := store.saveHistory()
	if len(history) != 6 {
		t.Fatalf("save history = %#v, want three enable and three compensation attempts", history)
	}
	enabledApplied := make(map[string]struct{})
	compensated := make(map[string]struct{})
	for _, record := range history {
		if record.applied && !record.disabled {
			enabledApplied[record.id] = struct{}{}
		}
		if record.applied && record.disabled {
			compensated[record.id] = struct{}{}
		}
	}
	if len(enabledApplied) != 2 {
		t.Fatalf("ordinary auths enabled before partial failure = %v, want exactly two", enabledApplied)
	}
	if len(compensated) != len(ordinaryIDs) {
		t.Fatalf("ordinary auths durably compensated = %v, want all %v", compensated, ordinaryIDs)
	}
	for _, ordinaryID := range ordinaryIDs {
		if durable := store.auth(ordinaryID); durable == nil || !durable.Disabled || durable.Status != coreauth.StatusDisabled {
			t.Fatalf("durable ordinary auth %q after compensation = %#v, want disabled", ordinaryID, durable)
		}
	}
	for _, id := range append([]string{sourceName}, ordinaryIDs...) {
		auth, ok := manager.GetByID(id)
		if !ok || auth == nil || !auth.Disabled || auth.Status != coreauth.StatusDisabled {
			t.Fatalf("runtime auth %q after compensation = %#v, want disabled", id, auth)
		}
	}
}

func TestPatchAuthFileStatus_EnableUnknownCompensationIgnoresRequestCancellation(t *testing.T) {
	authDir := t.TempDir()
	sourceName := "plugin-source.json"
	sourcePath := filepath.Join(authDir, sourceName)
	if errWrite := os.WriteFile(sourcePath, []byte(`{"type":"gemini-cli","disabled":true}`), 0o600); errWrite != nil {
		t.Fatalf("write plugin source: %v", errWrite)
	}
	ordinaryIDs := []string{"ordinary-a", "ordinary-b"}
	rows := make(map[string]*coreauth.Auth, len(ordinaryIDs))
	for _, id := range ordinaryIDs {
		rows[id] = &coreauth.Auth{
			ID:         id,
			FileName:   id + ".json",
			Provider:   "codex",
			Disabled:   true,
			Status:     coreauth.StatusDisabled,
			Metadata:   map[string]any{"type": "codex", "disabled": true},
			Attributes: map[string]string{"path": sourcePath},
		}
	}
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	store := &statusCommitUnknownStore{
		rows:         rows,
		cancelOnCall: 2,
		cancel:       cancelRequest,
		outcomes: []statusSaveOutcome{
			{apply: true},
			{unknown: true},
			{apply: true},
			{apply: true},
		},
	}
	manager := coreauth.NewManager(store, nil, nil)
	if errReconcile := manager.Reconcile(context.Background()); errReconcile != nil {
		t.Fatalf("Reconcile() error: %v", errReconcile)
	}
	pluginAuth := pluginVirtualAuthForTest(authDir, sourceName, sourceName)
	pluginAuth.Disabled = true
	pluginAuth.Status = coreauth.StatusDisabled
	pluginAuth.Metadata["disabled"] = true
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), pluginAuth); errRegister != nil {
		t.Fatalf("register plugin auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/status", strings.NewReader(`{"name":"plugin-source.json","disabled":false}`)).WithContext(requestCtx)
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.PatchAuthFileStatus(ctx)

	if requestCtx.Err() == nil {
		t.Fatal("request context was not canceled by the injected unknown save")
	}
	contextErrors := store.savedContextErrors()
	if len(contextErrors) != 4 || contextErrors[2] != nil || contextErrors[3] != nil {
		t.Fatalf("save context errors = %v, want detached compensation contexts for calls 3 and 4", contextErrors)
	}
	for _, id := range ordinaryIDs {
		if durable := store.auth(id); durable == nil || !durable.Disabled || durable.Status != coreauth.StatusDisabled {
			t.Fatalf("durable auth %q after detached compensation = %#v, want disabled", id, durable)
		}
	}
}

func TestPatchAuthFileStatus_EnableCommitUnknownCompensationRollbackReportsUnconfirmed(t *testing.T) {
	authDir := t.TempDir()
	sourceName := "plugin-source.json"
	sourcePath := filepath.Join(authDir, sourceName)
	if errWrite := os.WriteFile(sourcePath, []byte(`{"type":"gemini-cli","disabled":true}`), 0o600); errWrite != nil {
		t.Fatalf("write plugin source: %v", errWrite)
	}

	ordinaryIDs := []string{"ordinary-a", "ordinary-b"}
	rows := make(map[string]*coreauth.Auth, len(ordinaryIDs))
	for _, ordinaryID := range ordinaryIDs {
		rows[ordinaryID] = &coreauth.Auth{
			ID:         ordinaryID,
			FileName:   ordinaryID + ".json",
			Provider:   "codex",
			Disabled:   true,
			Status:     coreauth.StatusDisabled,
			Metadata:   map[string]any{"type": "codex", "disabled": true},
			Attributes: map[string]string{"path": sourcePath},
		}
	}
	store := &statusCommitUnknownStore{
		rows: rows,
		outcomes: []statusSaveOutcome{
			{apply: true},   // one enable commits
			{unknown: true}, // the other enable rolls back with unknown outcome
			{unknown: true}, // compensation for the active row rolls back
			{apply: true},   // the already-disabled row remains disabled
		},
	}
	manager := coreauth.NewManager(store, nil, nil)
	if errReconcile := manager.Reconcile(context.Background()); errReconcile != nil {
		t.Fatalf("Reconcile() error: %v", errReconcile)
	}
	pluginAuth := pluginVirtualAuthForTest(authDir, sourceName, sourceName)
	pluginAuth.Disabled = true
	pluginAuth.Status = coreauth.StatusDisabled
	pluginAuth.Metadata["disabled"] = true
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), pluginAuth); errRegister != nil {
		t.Fatalf("register plugin auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodPatch,
		"/v0/management/auth-files/status",
		strings.NewReader(`{"name":"plugin-source.json","disabled":false}`),
	)
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.PatchAuthFileStatus(ctx)

	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "credential state could not be confirmed") {
		t.Fatalf("response = %d %s, want unconfirmed-state error", rec.Code, rec.Body.String())
	}
	activeDurable := 0
	for _, ordinaryID := range ordinaryIDs {
		durable := store.auth(ordinaryID)
		if durable != nil && !durable.Disabled && durable.Status == coreauth.StatusActive {
			activeDurable++
		}
	}
	if activeDurable != 1 {
		t.Fatalf("active durable auth count = %d, want one after compensation rollback", activeDurable)
	}
	for _, id := range append([]string{sourceName}, ordinaryIDs...) {
		auth, ok := manager.GetByID(id)
		if !ok || auth == nil || !auth.Disabled || auth.Status != coreauth.StatusDisabled {
			t.Fatalf("runtime auth %q after uncertain compensation = %#v, want local fail-closed", id, auth)
		}
	}
}

func TestPatchAuthFileStatus_PreservesSourceUpdateFromLockedWriter(t *testing.T) {
	authDir := t.TempDir()
	sourceName := "plugin-source.json"
	sourcePath := filepath.Join(authDir, sourceName)
	original := []byte(`{"type":"gemini-cli","token":"old","disabled":false}`)
	concurrent := []byte(`{"type":"gemini-cli","token":"new","disabled":false}`)
	if errWrite := os.WriteFile(sourcePath, original, 0o600); errWrite != nil {
		t.Fatalf("write original source: %v", errWrite)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	if _, errRegister := manager.Register(context.Background(), pluginVirtualAuthForTest(authDir, sourceName, sourceName)); errRegister != nil {
		t.Fatalf("register plugin source: %v", errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodPatch,
		"/v0/management/auth-files/status",
		strings.NewReader(`{"name":"plugin-source.json","disabled":true}`),
	)
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	unlockSource := authfilelock.Lock(sourcePath)
	locked := true
	defer func() {
		if locked {
			unlockSource()
		}
	}()
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		h.PatchAuthFileStatus(ctx)
		close(done)
	}()
	<-started
	select {
	case <-done:
		t.Fatal("status update completed while the source writer held the shared path lock")
	case <-time.After(100 * time.Millisecond):
	}
	if errWrite := os.WriteFile(sourcePath, concurrent, 0o600); errWrite != nil {
		t.Fatalf("write concurrent source update: %v", errWrite)
	}
	unlockSource()
	locked = false
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("status update did not complete after the source writer released the shared path lock")
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	after, errRead := os.ReadFile(sourcePath)
	if errRead != nil {
		t.Fatalf("read source: %v", errRead)
	}
	var metadata map[string]any
	if errUnmarshal := json.Unmarshal(after, &metadata); errUnmarshal != nil {
		t.Fatalf("unmarshal source: %v", errUnmarshal)
	}
	if metadata["token"] != "new" || metadata["disabled"] != true {
		t.Fatalf("source after status update = %s, want new token and disabled=true", after)
	}
}

func TestPatchAuthFileStatus_EnableRollbackPreservesConcurrentTokenAndRestoresDisabled(t *testing.T) {
	authDir := t.TempDir()
	sourceName := "plugin-source.json"
	sourcePath := filepath.Join(authDir, sourceName)
	original := []byte(`{"type":"gemini-cli","token":"old","disabled":true}`)
	concurrent := []byte(`{"type":"gemini-cli","token":"new","disabled":false}`)
	if errWrite := os.WriteFile(sourcePath, original, 0o600); errWrite != nil {
		t.Fatalf("write original source: %v", errWrite)
	}

	store := &statusBlockingFailStore{
		err:     errors.New("ordinary auth enable failed"),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := coreauth.NewManager(store, nil, nil)
	pluginAuth := pluginVirtualAuthForTest(authDir, sourceName, sourceName)
	pluginAuth.Disabled = true
	pluginAuth.Status = coreauth.StatusDisabled
	pluginAuth.Metadata["disabled"] = true
	ordinaryAuth := &coreauth.Auth{
		ID:       "ordinary-runtime",
		FileName: "ordinary.json",
		Provider: "codex",
		Disabled: true,
		Status:   coreauth.StatusDisabled,
		Metadata: map[string]any{"type": "codex", "disabled": true},
	}
	for _, auth := range []*coreauth.Auth{pluginAuth, ordinaryAuth} {
		if _, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), auth); errRegister != nil {
			t.Fatalf("register auth %q: %v", auth.ID, errRegister)
		}
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodPatch,
		"/v0/management/auth-files/status",
		strings.NewReader(`{"names":["plugin-source.json","ordinary.json"],"disabled":false}`),
	)
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	done := make(chan struct{})
	go func() {
		h.PatchAuthFileStatus(ctx)
		close(done)
	}()

	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("enable did not reach persistence")
	}
	if errWrite := os.WriteFile(sourcePath, concurrent, 0o600); errWrite != nil {
		t.Fatalf("write concurrent source update: %v", errWrite)
	}
	close(store.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("enable did not complete after persistence failure")
	}

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	after, errRead := os.ReadFile(sourcePath)
	if errRead != nil {
		t.Fatalf("read source: %v", errRead)
	}
	var metadata map[string]any
	if errUnmarshal := json.Unmarshal(after, &metadata); errUnmarshal != nil {
		t.Fatalf("unmarshal source: %v", errUnmarshal)
	}
	if metadata["token"] != "new" || metadata["disabled"] != true {
		t.Fatalf("source after failed enable = %s, want new token and disabled=true", after)
	}
	fileStore := sdkAuth.NewFileTokenStore()
	fileStore.SetBaseDir(authDir)
	reloaded, errList := fileStore.List(context.Background())
	if errList != nil {
		t.Fatalf("reload auth files: %v", errList)
	}
	foundReloadedDisabled := false
	for _, auth := range reloaded {
		if auth != nil && auth.FileName == sourceName && auth.Disabled && auth.Status == coreauth.StatusDisabled {
			foundReloadedDisabled = true
			break
		}
	}
	if !foundReloadedDisabled {
		t.Fatalf("reloaded auths = %#v, want %s to remain disabled", reloaded, sourceName)
	}
	for _, id := range []string{sourceName, ordinaryAuth.ID} {
		auth, ok := manager.GetByID(id)
		if !ok || auth == nil || !auth.Disabled || auth.Status != coreauth.StatusDisabled {
			t.Fatalf("runtime auth %q after failed enable = %#v, want disabled", id, auth)
		}
	}
}

func TestWriteSourceAuthFileAtomicallyIfUnchangedPreservesConcurrentUpdate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plugin-source.json")
	original := []byte(`{"type":"gemini-cli","token":"old","disabled":true}`)
	concurrent := []byte(`{"type":"gemini-cli","token":"new","disabled":true}`)
	updated := []byte(`{"type":"gemini-cli","token":"old","disabled":false}`)
	if errWrite := os.WriteFile(path, original, 0o600); errWrite != nil {
		t.Fatalf("write original source: %v", errWrite)
	}
	if errWrite := os.WriteFile(path, concurrent, 0o600); errWrite != nil {
		t.Fatalf("write concurrent source: %v", errWrite)
	}

	errWrite := writeSourceAuthFileAtomicallyIfUnchanged(path, original, updated)
	if !errors.Is(errWrite, errAuthFileChanged) {
		t.Fatalf("writeSourceAuthFileAtomicallyIfUnchanged() error = %v, want errors.Is(_, %v)", errWrite, errAuthFileChanged)
	}
	after, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read source: %v", errRead)
	}
	if string(after) != string(concurrent) {
		t.Fatalf("source after rejected write = %s, want concurrent content %s", after, concurrent)
	}
}
