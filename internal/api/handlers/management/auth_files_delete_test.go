package management

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

type blockingManagementDeleteStore struct {
	mu            sync.Mutex
	stored        *coreauth.Auth
	deleteStarted chan struct{}
	allowDelete   chan struct{}
}

type generationTombstoneDeleteStore struct {
	mu          sync.Mutex
	row         *coreauth.Auth
	tombstoneID string
	expected    uint64
}

func (s *generationTombstoneDeleteStore) List(context.Context) ([]*coreauth.Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.row == nil {
		return nil, nil
	}
	return []*coreauth.Auth{s.row.Clone()}, nil
}

func (s *generationTombstoneDeleteStore) ListAuthoritative(ctx context.Context) ([]*coreauth.Auth, error) {
	return s.List(ctx)
}

func (s *generationTombstoneDeleteStore) GetByID(_ context.Context, id string) (*coreauth.Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.row == nil || filepath.Base(id) != filepath.Base(s.row.ID) {
		return nil, nil
	}
	return s.row.Clone(), nil
}

func (s *generationTombstoneDeleteStore) Save(_ context.Context, auth *coreauth.Auth) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.row = auth.Clone()
	return auth.ID, nil
}

func (*generationTombstoneDeleteStore) Delete(context.Context, string) error {
	return errors.New("physical Delete must not be used by a tombstone store")
}

func (s *generationTombstoneDeleteStore) Tombstone(_ context.Context, id string, expectedGeneration uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tombstoneID = id
	s.expected = expectedGeneration
	if s.row == nil {
		if expectedGeneration != 0 {
			return 0, coreauth.ErrAuthStoreConflict
		}
		return 1, nil
	}
	if expectedGeneration != s.row.StoreGeneration() {
		return 0, coreauth.ErrAuthStoreConflict
	}
	next := expectedGeneration + 1
	s.row = nil
	return next, nil
}

func (*generationTombstoneDeleteStore) SetBaseDir(string) {}

func newBlockingManagementDeleteStore(auth *coreauth.Auth) *blockingManagementDeleteStore {
	return &blockingManagementDeleteStore{
		stored:        auth.Clone(),
		deleteStarted: make(chan struct{}),
		allowDelete:   make(chan struct{}),
	}
}

func (s *blockingManagementDeleteStore) List(context.Context) ([]*coreauth.Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stored == nil {
		return nil, nil
	}
	return []*coreauth.Auth{s.stored.Clone()}, nil
}

func (s *blockingManagementDeleteStore) Save(_ context.Context, auth *coreauth.Auth) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stored = auth.Clone()
	return auth.ID, nil
}

func (s *blockingManagementDeleteStore) Delete(context.Context, string) error {
	close(s.deleteStarted)
	<-s.allowDelete
	s.mu.Lock()
	s.stored = nil
	s.mu.Unlock()
	return nil
}

func (*blockingManagementDeleteStore) SetBaseDir(string) {}

func TestDeleteAuthFile_UsesAuthPathFromManager(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	tempDir := t.TempDir()
	authDir := filepath.Join(tempDir, "auth")
	externalDir := filepath.Join(tempDir, "external")
	if errMkdirAuth := os.MkdirAll(authDir, 0o700); errMkdirAuth != nil {
		t.Fatalf("failed to create auth dir: %v", errMkdirAuth)
	}
	if errMkdirExternal := os.MkdirAll(externalDir, 0o700); errMkdirExternal != nil {
		t.Fatalf("failed to create external dir: %v", errMkdirExternal)
	}

	fileName := "codex-user@example.com-plus.json"
	shadowPath := filepath.Join(authDir, fileName)
	realPath := filepath.Join(externalDir, fileName)
	if errWriteShadow := os.WriteFile(shadowPath, []byte(`{"type":"codex","email":"shadow@example.com"}`), 0o600); errWriteShadow != nil {
		t.Fatalf("failed to write shadow file: %v", errWriteShadow)
	}
	if errWriteReal := os.WriteFile(realPath, []byte(`{"type":"codex","email":"real@example.com"}`), 0o600); errWriteReal != nil {
		t.Fatalf("failed to write real file: %v", errWriteReal)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	record := &coreauth.Auth{
		ID:          "legacy/" + fileName,
		FileName:    fileName,
		Provider:    "codex",
		Status:      coreauth.StatusError,
		Unavailable: true,
		Attributes: map[string]string{
			"path": realPath,
		},
		Metadata: map[string]any{
			"type":  "codex",
			"email": "real@example.com",
		},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("failed to register auth record: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	deleteRec := httptest.NewRecorder()
	deleteCtx, _ := gin.CreateTestContext(deleteRec)
	deleteReq := httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	deleteCtx.Request = deleteReq
	h.DeleteAuthFile(deleteCtx)

	if deleteRec.Code != http.StatusOK {
		t.Fatalf("expected delete status %d, got %d with body %s", http.StatusOK, deleteRec.Code, deleteRec.Body.String())
	}
	if _, errStatReal := os.Stat(realPath); !os.IsNotExist(errStatReal) {
		t.Fatalf("expected managed auth file to be removed, stat err: %v", errStatReal)
	}
	if _, errStatShadow := os.Stat(shadowPath); errStatShadow != nil {
		t.Fatalf("expected shadow auth file to remain, stat err: %v", errStatShadow)
	}

	listRec := httptest.NewRecorder()
	listCtx, _ := gin.CreateTestContext(listRec)
	listReq := httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil)
	listCtx.Request = listReq
	h.ListAuthFiles(listCtx)

	if listRec.Code != http.StatusOK {
		t.Fatalf("expected list status %d, got %d with body %s", http.StatusOK, listRec.Code, listRec.Body.String())
	}
	var listPayload map[string]any
	if errUnmarshal := json.Unmarshal(listRec.Body.Bytes(), &listPayload); errUnmarshal != nil {
		t.Fatalf("failed to decode list payload: %v", errUnmarshal)
	}
	filesRaw, ok := listPayload["files"].([]any)
	if !ok {
		t.Fatalf("expected files array, payload: %#v", listPayload)
	}
	if len(filesRaw) != 0 {
		t.Fatalf("expected removed auth to be hidden from list, got %d entries", len(filesRaw))
	}
}

// TestDeleteAuthFile_ClusterModeMissingLocalFile covers the bug where a
// node in cluster mode receives a DELETE for an auth that was uploaded on
// a different node — the local file therefore doesn't exist, but the
// authManager (synced via PG NOTIFY) and the token store both have the
// record. Before the fix, os.Remove ENOENT short-circuited to 404; after
// the fix the handler falls through to the PG-layer delete.
func TestDeleteAuthFile_ClusterModeMissingLocalFile(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	fileName := "codex-cluster@example.com-plus.json"
	// IMPORTANT: do NOT create the local file — simulate the "uploaded on
	// another node" scenario.

	manager := coreauth.NewManager(nil, nil, nil)
	record := &coreauth.Auth{
		ID:       fileName,
		FileName: fileName,
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"type": "codex"},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("failed to register auth record: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	// memoryAuthStore.Delete on a non-existent ID returns nil — the test
	// proves the handler reaches PG-layer delete despite ENOENT, which is
	// the cluster-mode invariant.
	h.tokenStore = &memoryAuthStore{}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on cluster-mode delete (file missing locally), got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestDeleteAuthFile_UsesTrustedGenerationForTombstone(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	for _, withRuntime := range []bool{true, false} {
		withRuntime := withRuntime
		t.Run(map[bool]string{true: "manager snapshot", false: "authoritative store read"}[withRuntime], func(t *testing.T) {
			authDir := t.TempDir()
			fileName := "versioned-delete.json"
			record := &coreauth.Auth{
				ID:         fileName,
				FileName:   fileName,
				Provider:   "codex",
				Status:     coreauth.StatusActive,
				Attributes: map[string]string{"path": filepath.Join(authDir, fileName)},
				Metadata:   map[string]any{"type": "codex"},
			}
			record.SetStoreGeneration(12)
			store := &generationTombstoneDeleteStore{row: record.Clone()}
			manager := coreauth.NewManager(store, nil, nil)
			if withRuntime {
				if _, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), record.Clone()); errRegister != nil {
					t.Fatalf("Register() error: %v", errRegister)
				}
			}
			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
			h.tokenStore = store

			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
			h.DeleteAuthFile(ctx)

			if recorder.Code != http.StatusOK {
				t.Fatalf("DELETE status = %d, body = %s", recorder.Code, recorder.Body.String())
			}
			store.mu.Lock()
			expected := store.expected
			tombstoneID := store.tombstoneID
			remaining := store.row
			store.mu.Unlock()
			if expected != 12 {
				t.Fatalf("Tombstone expected generation = %d, want 12", expected)
			}
			if filepath.Base(tombstoneID) != fileName {
				t.Fatalf("Tombstone id = %q, want %q", tombstoneID, fileName)
			}
			if remaining != nil {
				t.Fatalf("active row remained after tombstone: %#v", remaining)
			}
		})
	}
}

func TestDeleteAuthFileAllIncludesAuthoritativeRowsWithoutLocalMirrors(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	record := &coreauth.Auth{
		ID:       "remote-only.json",
		FileName: "remote-only.json",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"type": "codex"},
	}
	record.SetStoreGeneration(23)
	store := &generationTombstoneDeleteStore{row: record.Clone()}
	manager := coreauth.NewManager(store, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = store

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?all=true", nil)
	h.DeleteAuthFile(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("DELETE all status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Deleted int `json:"deleted"`
	}
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &response); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if response.Deleted != 1 {
		t.Fatalf("deleted = %d, want 1", response.Deleted)
	}
	store.mu.Lock()
	expected := store.expected
	remaining := store.row
	store.mu.Unlock()
	if expected != 23 || remaining != nil {
		t.Fatalf("Tombstone(expected=%d) remaining=%#v, want expected=23 and no active row", expected, remaining)
	}
}

func TestDeleteAuthFile_FallbackToAuthDirPath(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	authDir := t.TempDir()
	fileName := "fallback-user.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("failed to write auth file: %v", errWrite)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	deleteRec := httptest.NewRecorder()
	deleteCtx, _ := gin.CreateTestContext(deleteRec)
	deleteReq := httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	deleteCtx.Request = deleteReq
	h.DeleteAuthFile(deleteCtx)

	if deleteRec.Code != http.StatusOK {
		t.Fatalf("expected delete status %d, got %d with body %s", http.StatusOK, deleteRec.Code, deleteRec.Body.String())
	}
	if _, errStat := os.Stat(filePath); !os.IsNotExist(errStat) {
		t.Fatalf("expected auth file to be removed from auth dir, stat err: %v", errStat)
	}
}

func TestDeleteAuthFile_RemovesRuntimeAuth(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	authDir := t.TempDir()
	fileName := "runtime-remove-user.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex","email":"runtime@example.com"}`), 0o600); errWrite != nil {
		t.Fatalf("failed to write auth file: %v", errWrite)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	record := &coreauth.Auth{
		ID:       "runtime-remove-auth",
		FileName: fileName,
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"path": filePath,
		},
		Metadata: map[string]any{
			"type":  "codex",
			"email": "runtime@example.com",
		},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("failed to register auth record: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	deleteRec := httptest.NewRecorder()
	deleteCtx, _ := gin.CreateTestContext(deleteRec)
	deleteReq := httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	deleteCtx.Request = deleteReq
	h.DeleteAuthFile(deleteCtx)

	if deleteRec.Code != http.StatusOK {
		t.Fatalf("expected delete status %d, got %d with body %s", http.StatusOK, deleteRec.Code, deleteRec.Body.String())
	}
	if _, ok := manager.GetByID(record.ID); ok {
		t.Fatalf("expected runtime auth %q to be removed", record.ID)
	}
}

func TestDeleteAuthFile_DoesNotAllowConcurrentSaveToRestoreAuth(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	fileName := "atomic-delete-user.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex","access_token":"old"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	auth := &coreauth.Auth{
		ID:       "atomic-delete-runtime",
		FileName: fileName,
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"path": filePath,
		},
		Metadata: map[string]any{"type": "codex", "access_token": "old"},
	}
	store := newBlockingManagementDeleteStore(auth)
	manager := coreauth.NewManager(store, nil, nil)
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), auth.Clone()); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = store

	deleteRec := httptest.NewRecorder()
	deleteCtx, _ := gin.CreateTestContext(deleteRec)
	deleteCtx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	deleteDone := make(chan struct{})
	go func() {
		h.DeleteAuthFile(deleteCtx)
		close(deleteDone)
	}()
	<-store.deleteStarted

	updated := auth.Clone()
	updated.Metadata["access_token"] = "new"
	updateDone := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(context.Background(), updated)
		updateDone <- errUpdate
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, ok := manager.GetByID(auth.ID)
		if ok && current.Metadata["access_token"] == "new" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("concurrent Update did not publish before delete completed")
		}
		time.Sleep(time.Millisecond)
	}

	close(store.allowDelete)
	<-deleteDone
	if errUpdate := <-updateDone; errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	if _, ok := manager.GetByID(auth.ID); ok {
		t.Fatalf("runtime auth %q was restored by concurrent Save", auth.ID)
	}
	store.mu.Lock()
	persisted := store.stored
	store.mu.Unlock()
	if persisted != nil {
		t.Fatalf("token store auth was restored after delete: %#v", persisted)
	}
}
