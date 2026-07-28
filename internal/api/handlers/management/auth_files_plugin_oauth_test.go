package management

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/pluginhost"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func TestPluginLoginPollAuthsExpandsMultipleAuths(t *testing.T) {
	host := pluginhost.New()
	resp := pluginapi.AuthLoginPollResponse{
		Status: pluginapi.AuthLoginStatusSuccess,
		Auths: []pluginapi.AuthData{
			{
				Provider:    "gemini-cli",
				ID:          "geminicli.json",
				FileName:    "geminicli.json",
				StorageJSON: []byte(`{"type":"gemini-cli"}`),
			},
			{
				Provider:    "gemini-cli",
				ID:          "geminicli-project-a.json",
				FileName:    "geminicli-project-a.json",
				StorageJSON: []byte(`{"type":"gemini-cli","project_id":"project-a"}`),
				Metadata:    map[string]any{"project_id": "project-a"},
			},
		},
	}

	records := pluginLoginPollAuths(host, resp)
	if len(records) != 2 {
		t.Fatalf("pluginLoginPollAuths() len = %d, want two records", len(records))
	}
	if records[0].ID != "geminicli.json" || records[1].ID != "geminicli-project-a.json" {
		t.Fatalf("records = %#v, want both plugin auths", records)
	}
	if gotProject := records[1].Metadata["project_id"]; gotProject != "project-a" {
		t.Fatalf("project_id = %#v, want project-a", gotProject)
	}
}

func TestSavePluginLoginRecordsRollsBackSavedAuthsOnFailure(t *testing.T) {
	store := &pluginLoginRollbackStore{failAt: 2}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, nil)
	h.tokenStore = store

	records := []*coreauth.Auth{
		{
			ID:       "geminicli.json",
			FileName: "geminicli.json",
			Provider: "gemini-cli",
			Metadata: map[string]any{"type": "gemini-cli"},
		},
		{
			ID:       "geminicli-project-a.json",
			FileName: "geminicli-project-a.json",
			Provider: "gemini-cli",
			Metadata: map[string]any{"type": "gemini-cli", "project_id": "project-a"},
		},
	}

	errSave := h.savePluginLoginRecords(context.Background(), records)
	if errSave == nil {
		t.Fatal("savePluginLoginRecords() error = nil, want rollback-triggering error")
	}
	if len(store.saved) != 2 {
		t.Fatalf("saved len = %d, want two attempted saves", len(store.saved))
	}
	if !store.deleted["geminicli.json"] || !store.deleted["geminicli-project-a.json"] {
		t.Fatalf("deleted = %#v, want both saved auths rolled back", store.deleted)
	}
}

func TestSavePluginLoginRecordsRestoresExistingVersionedAuthOnFailure(t *testing.T) {
	const existingID = "existing.json"
	store := newPluginLoginVersionedRollbackStore(&coreauth.Auth{
		ID:       existingID,
		FileName: existingID,
		Provider: "gemini-cli",
		Metadata: map[string]any{"access_token": "old-token"},
	})
	store.rows[existingID].SetStoreGeneration(5)
	store.failAt = 2

	manager := coreauth.NewManager(store, nil, nil)
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), store.rows[existingID].Clone()); errRegister != nil {
		t.Fatalf("seed existing runtime auth: %v", errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	h.tokenStore = store
	h.postAuthPersistHook = pluginLoginRuntimeSyncHook(manager)
	errSave := h.savePluginLoginRecords(context.Background(), []*coreauth.Auth{
		{
			ID:       existingID,
			FileName: existingID,
			Provider: "gemini-cli",
			Metadata: map[string]any{"access_token": "new-token"},
		},
		{
			ID:       "fails.json",
			FileName: "fails.json",
			Provider: "gemini-cli",
			Metadata: map[string]any{"access_token": "never-committed"},
		},
	})
	if errSave == nil {
		t.Fatal("savePluginLoginRecords() error = nil, want second-save failure")
	}
	restored, ok := store.rows[existingID]
	if !ok || restored == nil {
		t.Fatalf("existing auth %q was removed during rollback", existingID)
	}
	if got := restored.Metadata["access_token"]; got != "old-token" {
		t.Fatalf("restored access_token = %#v, want old-token", got)
	}
	if restored.StoreGeneration() != 7 {
		t.Fatalf("restored generation = %d, want CAS rollback generation 7", restored.StoreGeneration())
	}
	if store.tombstoned[existingID] != 0 {
		t.Fatalf("existing auth was tombstoned at generation %d", store.tombstoned[existingID])
	}
	runtimeAuth, exists := manager.GetByID(existingID)
	runtimeGeneration := uint64(0)
	if runtimeAuth != nil {
		runtimeGeneration = runtimeAuth.StoreGeneration()
	}
	if !exists || runtimeAuth == nil || runtimeAuth.Metadata["access_token"] != "old-token" || runtimeGeneration != 7 {
		t.Fatalf("runtime rollback auth = %#v generation=%d", runtimeAuth, runtimeGeneration)
	}
}

func TestSavePluginLoginRecordsTombstonesNewVersionedAuthOnFailure(t *testing.T) {
	store := newPluginLoginVersionedRollbackStore()
	store.failAt = 2
	manager := coreauth.NewManager(store, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	h.tokenStore = store
	h.postAuthPersistHook = pluginLoginRuntimeSyncHook(manager)

	errSave := h.savePluginLoginRecords(context.Background(), []*coreauth.Auth{
		{
			ID:       "created.json",
			FileName: "created.json",
			Provider: "gemini-cli",
			Metadata: map[string]any{"access_token": "new-token"},
		},
		{
			ID:       "fails.json",
			FileName: "fails.json",
			Provider: "gemini-cli",
			Metadata: map[string]any{"access_token": "never-committed"},
		},
	})
	if errSave == nil {
		t.Fatal("savePluginLoginRecords() error = nil, want second-save failure")
	}
	if _, ok := store.rows["created.json"]; ok {
		t.Fatal("new auth remained active after rollback")
	}
	if store.tombstoned["created.json"] != 2 {
		t.Fatalf("new auth tombstone generation = %d, want 2", store.tombstoned["created.json"])
	}
	if _, exists := manager.GetByID("created.json"); exists {
		t.Fatal("new auth remained in runtime after rollback")
	}
}

func TestSavePluginLoginRecordsRollsBackAfterRequestContextCancellation(t *testing.T) {
	tests := []struct {
		name     string
		existing *coreauth.Auth
		assert   func(t *testing.T, store *pluginLoginVersionedRollbackStore)
	}{
		{
			name: "restore existing auth",
			existing: &coreauth.Auth{
				ID:       "committed.json",
				FileName: "committed.json",
				Provider: "gemini-cli",
				Metadata: map[string]any{"access_token": "old-token"},
			},
			assert: func(t *testing.T, store *pluginLoginVersionedRollbackStore) {
				t.Helper()
				restored := store.rows["committed.json"]
				if restored == nil || restored.Metadata["access_token"] != "old-token" {
					t.Fatalf("restored auth = %#v, want old snapshot", restored)
				}
			},
		},
		{
			name: "tombstone new auth",
			assert: func(t *testing.T, store *pluginLoginVersionedRollbackStore) {
				t.Helper()
				if _, exists := store.rows["committed.json"]; exists {
					t.Fatal("new auth remained active after rollback")
				}
				if store.tombstoned["committed.json"] == 0 {
					t.Fatal("new auth was not tombstoned during rollback")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newPluginLoginVersionedRollbackStore(tt.existing)
			if tt.existing != nil {
				store.rows[tt.existing.ID].SetStoreGeneration(5)
			}
			ctx, cancel := context.WithCancel(context.Background())
			store.cancelAt = 2
			store.cancel = cancel

			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, nil)
			h.tokenStore = store
			errSave := h.savePluginLoginRecords(ctx, []*coreauth.Auth{
				{
					ID:       "committed.json",
					FileName: "committed.json",
					Provider: "gemini-cli",
					Metadata: map[string]any{"access_token": "new-token"},
				},
				{
					ID:       "fails.json",
					FileName: "fails.json",
					Provider: "gemini-cli",
					Metadata: map[string]any{"access_token": "never-committed"},
				},
			})
			if !errors.Is(errSave, context.Canceled) {
				t.Fatalf("savePluginLoginRecords() error = %v, want context canceled", errSave)
			}
			tt.assert(t, store)
		})
	}
}

func TestSavePluginLoginRecordsRollsBackCommitUnknownCandidate(t *testing.T) {
	tests := []struct {
		name     string
		existing *coreauth.Auth
		assert   func(t *testing.T, store *pluginLoginVersionedRollbackStore)
	}{
		{
			name: "restore existing auth",
			existing: &coreauth.Auth{
				ID:       "unknown.json",
				FileName: "unknown.json",
				Provider: "gemini-cli",
				Metadata: map[string]any{"access_token": "old-token"},
			},
			assert: func(t *testing.T, store *pluginLoginVersionedRollbackStore) {
				t.Helper()
				restored := store.rows["unknown.json"]
				if restored == nil || restored.Metadata["access_token"] != "old-token" {
					t.Fatalf("restored auth = %#v, want old snapshot", restored)
				}
			},
		},
		{
			name: "tombstone new auth",
			assert: func(t *testing.T, store *pluginLoginVersionedRollbackStore) {
				t.Helper()
				if _, exists := store.rows["unknown.json"]; exists {
					t.Fatal("outcome-unknown auth remained active after rollback")
				}
				if store.tombstoned["unknown.json"] != 2 {
					t.Fatalf("outcome-unknown auth tombstone generation = %d, want 2", store.tombstoned["unknown.json"])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newPluginLoginVersionedRollbackStore(tt.existing)
			if tt.existing != nil {
				store.rows[tt.existing.ID].SetStoreGeneration(5)
			}
			store.commitUnknownAt = 1
			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, nil)
			h.tokenStore = store

			errSave := h.savePluginLoginRecords(context.Background(), []*coreauth.Auth{{
				ID:       "unknown.json",
				FileName: "unknown.json",
				Provider: "gemini-cli",
				Metadata: map[string]any{"access_token": "new-token"},
			}})
			if !errors.Is(errSave, coreauth.ErrAuthStoreCommitUnknown) {
				t.Fatalf("savePluginLoginRecords() error = %v, want commit outcome unknown", errSave)
			}
			tt.assert(t, store)
		})
	}
}

func TestSavePluginLoginRecordsCommitUnknownWithoutCandidateFailsClosed(t *testing.T) {
	const id = "unknown-without-candidate.json"
	store := newPluginLoginVersionedRollbackStore(&coreauth.Auth{
		ID:       id,
		FileName: id,
		Provider: "gemini-cli",
		Metadata: map[string]any{"access_token": "old-token"},
	})
	store.rows[id].SetStoreGeneration(5)
	store.commitUnknownAt = 1
	store.omitCommitCandidate = true

	manager := coreauth.NewManager(store, nil, nil)
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), store.rows[id].Clone()); errRegister != nil {
		t.Fatalf("seed runtime auth: %v", errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	h.tokenStore = store

	errSave := h.savePluginLoginRecords(context.Background(), []*coreauth.Auth{{
		ID:       id,
		FileName: id,
		Provider: "gemini-cli",
		Metadata: map[string]any{"access_token": "new-token"},
	}})
	if !errors.Is(errSave, coreauth.ErrAuthStoreCommitUnknown) {
		t.Fatalf("savePluginLoginRecords() error = %v, want commit outcome unknown", errSave)
	}
	if !strings.Contains(errSave.Error(), "has no committed generation") {
		t.Fatalf("savePluginLoginRecords() error = %v, want missing-generation rollback error", errSave)
	}
	if _, exists := manager.GetByID(id); exists {
		t.Fatal("runtime auth remained available after commit candidate was unavailable")
	}
	if committed := store.rows[id]; committed == nil || committed.Metadata["access_token"] != "new-token" {
		t.Fatalf("durable auth = %#v, want fake store to have committed before returning outcome unknown", committed)
	}
}

func pluginLoginRuntimeSyncHook(manager *coreauth.Manager) coreauth.PostAuthHook {
	return func(ctx context.Context, auth *coreauth.Auth) error {
		ctx = coreauth.WithSkipPersist(ctx)
		if _, exists := manager.GetByID(auth.ID); exists {
			_, errUpdate := manager.Update(ctx, auth)
			return errUpdate
		}
		_, errRegister := manager.Register(ctx, auth)
		return errRegister
	}
}

func TestPatchPluginVirtualAuthStatusReturnsConflictForVirtualChild(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	auth := pluginVirtualAuthForTest(t.TempDir(), "source.json", "auth-1")
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register virtual auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/status", strings.NewReader(`{"name":"auth-1","disabled":true}`))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.PatchAuthFileStatus(ctx)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusConflict, rec.Body.String())
	}
}

func TestPatchPluginVirtualSourceStatusDisablesAllExpandedAuths(t *testing.T) {
	authDir := t.TempDir()
	fileName := "source.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"gemini-cli","disabled":false}`), 0o600); errWrite != nil {
		t.Fatalf("write source auth file: %v", errWrite)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	for _, id := range []string{"source.json", "virtual-project-a"} {
		auth := pluginVirtualAuthForTest(authDir, fileName, id)
		if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register virtual auth %s: %v", id, errRegister)
		}
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/status", strings.NewReader(`{"name":"source.json","disabled":true}`))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.PatchAuthFileStatus(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	raw, errRead := os.ReadFile(filePath)
	if errRead != nil {
		t.Fatalf("read source auth file: %v", errRead)
	}
	if !strings.Contains(string(raw), `"disabled":true`) {
		t.Fatalf("source auth file = %s, want disabled:true", string(raw))
	}
	for _, id := range []string{"source.json", "virtual-project-a"} {
		auth, ok := manager.GetByID(id)
		if !ok || auth == nil {
			t.Fatalf("expected auth %s to remain registered", id)
		}
		if !auth.Disabled || auth.Status != coreauth.StatusDisabled {
			t.Fatalf("auth %s disabled/status = %v/%s, want disabled", id, auth.Disabled, auth.Status)
		}
	}
}

func TestPatchPluginVirtualAuthFieldsReturnsConflict(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	auth := pluginVirtualAuthForTest(t.TempDir(), "source.json", "auth-1")
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register virtual auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(`{"name":"auth-1","note":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.PatchAuthFileFields(ctx)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusConflict, rec.Body.String())
	}
}

func TestDeletePluginVirtualSourceRemovesExpandedRuntimeAuths(t *testing.T) {
	authDir := t.TempDir()
	fileName := "source.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"gemini-cli"}`), 0o600); errWrite != nil {
		t.Fatalf("write source auth file: %v", errWrite)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	for _, id := range []string{"auth-1", "auth-2"} {
		auth := pluginVirtualAuthForTest(authDir, fileName, id)
		if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register virtual auth %s: %v", id, errRegister)
		}
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	ctx.Request = req

	h.DeleteAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if _, errStat := os.Stat(filePath); !os.IsNotExist(errStat) {
		t.Fatalf("expected source auth file to be removed, stat err: %v", errStat)
	}
	for _, id := range []string{"auth-1", "auth-2"} {
		if _, ok := manager.GetByID(id); ok {
			t.Fatalf("expected virtual auth %s to be removed", id)
		}
	}
}

func pluginVirtualAuthForTest(authDir, fileName, id string) *coreauth.Auth {
	filePath := filepath.Join(authDir, fileName)
	auth := &coreauth.Auth{
		ID:       id,
		FileName: fileName,
		Provider: "gemini-cli",
		Attributes: map[string]string{
			"path": filePath,
		},
		Metadata: map[string]any{
			"type": "gemini-cli",
		},
	}
	coreauth.MarkPluginVirtualAuth(auth, filePath, 0)
	return auth
}

type pluginLoginRollbackStore struct {
	failAt  int
	saved   []string
	deleted map[string]bool
}

func (s *pluginLoginRollbackStore) List(context.Context) ([]*coreauth.Auth, error) {
	return nil, nil
}

func (s *pluginLoginRollbackStore) Save(_ context.Context, auth *coreauth.Auth) (string, error) {
	path := strings.TrimSpace(auth.FileName)
	if path == "" {
		path = strings.TrimSpace(auth.ID)
	}
	s.saved = append(s.saved, path)
	if len(s.saved) == s.failAt {
		return path, errors.New("save failed after write")
	}
	return path, nil
}

func (s *pluginLoginRollbackStore) Delete(_ context.Context, id string) error {
	if s.deleted == nil {
		s.deleted = make(map[string]bool)
	}
	s.deleted[id] = true
	return nil
}

func (s *pluginLoginRollbackStore) SetBaseDir(string) {}

type pluginLoginVersionedRollbackStore struct {
	rows                map[string]*coreauth.Auth
	tombstoned          map[string]uint64
	attempts            int
	failAt              int
	cancelAt            int
	cancel              context.CancelFunc
	commitUnknownAt     int
	omitCommitCandidate bool
}

func newPluginLoginVersionedRollbackStore(auths ...*coreauth.Auth) *pluginLoginVersionedRollbackStore {
	store := &pluginLoginVersionedRollbackStore{
		rows:       make(map[string]*coreauth.Auth, len(auths)),
		tombstoned: make(map[string]uint64),
	}
	for _, auth := range auths {
		if auth != nil {
			store.rows[auth.ID] = auth.Clone()
		}
	}
	return store
}

func (s *pluginLoginVersionedRollbackStore) List(context.Context) ([]*coreauth.Auth, error) {
	auths := make([]*coreauth.Auth, 0, len(s.rows))
	for _, auth := range s.rows {
		auths = append(auths, auth.Clone())
	}
	return auths, nil
}

func (s *pluginLoginVersionedRollbackStore) GetByID(_ context.Context, id string) (*coreauth.Auth, error) {
	auth := s.rows[id]
	if auth == nil {
		return nil, nil
	}
	return auth.Clone(), nil
}

func (s *pluginLoginVersionedRollbackStore) Save(ctx context.Context, auth *coreauth.Auth) (string, error) {
	path, _, err := s.SaveVersioned(ctx, auth, auth.StoreGeneration())
	return path, err
}

func (s *pluginLoginVersionedRollbackStore) SaveVersioned(ctx context.Context, auth *coreauth.Auth, expected uint64) (string, uint64, error) {
	s.attempts++
	if s.attempts == s.cancelAt {
		s.cancel()
	}
	if err := ctx.Err(); err != nil {
		return "", 0, err
	}
	if s.attempts == s.failAt {
		return "", 0, errors.New("injected versioned save failure")
	}
	current := s.rows[auth.ID]
	if current == nil || current.StoreGeneration() != expected {
		return "", 0, coreauth.ErrAuthStoreConflict
	}
	updated := auth.Clone()
	generation := expected + 1
	updated.SetStoreGeneration(generation)
	s.rows[auth.ID] = updated
	if s.attempts == s.commitUnknownAt {
		candidates := map[string]uint64{auth.ID: generation}
		if s.omitCommitCandidate {
			candidates = nil
		}
		return "", 0, coreauth.NewAuthStoreCommitUnknown(candidates, errors.New("injected marker verification failure"))
	}
	return auth.ID, generation, nil
}

func (s *pluginLoginVersionedRollbackStore) Restore(ctx context.Context, auth *coreauth.Auth, expected uint64) (string, uint64, error) {
	s.attempts++
	if s.attempts == s.cancelAt {
		s.cancel()
	}
	if err := ctx.Err(); err != nil {
		return "", 0, err
	}
	if s.attempts == s.failAt {
		return "", 0, errors.New("injected versioned restore failure")
	}
	if current := s.rows[auth.ID]; current != nil {
		return "", 0, coreauth.ErrAuthStoreConflict
	}
	if tombstoneGeneration := s.tombstoned[auth.ID]; tombstoneGeneration != expected {
		if expected != 0 || tombstoneGeneration != 0 {
			return "", 0, coreauth.ErrAuthStoreConflict
		}
	}
	generation := uint64(1)
	if expected > 0 {
		generation = expected + 1
	}
	restored := auth.Clone()
	restored.SetStoreGeneration(generation)
	s.rows[auth.ID] = restored
	delete(s.tombstoned, auth.ID)
	if s.attempts == s.commitUnknownAt {
		candidates := map[string]uint64{auth.ID: generation}
		if s.omitCommitCandidate {
			candidates = nil
		}
		return "", 0, coreauth.NewAuthStoreCommitUnknown(candidates, errors.New("injected marker verification failure"))
	}
	return auth.ID, generation, nil
}

func (s *pluginLoginVersionedRollbackStore) Tombstone(ctx context.Context, id string, expected uint64) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	current := s.rows[id]
	if current == nil || current.StoreGeneration() != expected {
		return 0, coreauth.ErrAuthStoreConflict
	}
	delete(s.rows, id)
	s.tombstoned[id] = expected + 1
	return expected + 1, nil
}

func (s *pluginLoginVersionedRollbackStore) Delete(ctx context.Context, id string) error {
	current := s.rows[id]
	if current == nil {
		return nil
	}
	_, err := s.Tombstone(ctx, id, current.StoreGeneration())
	return err
}

func (*pluginLoginVersionedRollbackStore) SetBaseDir(string) {}
