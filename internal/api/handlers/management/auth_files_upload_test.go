package management

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/authfilelock"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

type uploadPathLockingStore struct {
	path  string
	saved chan struct{}
}

func (*uploadPathLockingStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }

func (s *uploadPathLockingStore) Save(context.Context, *coreauth.Auth) (string, error) {
	unlock := authfilelock.Lock(s.path)
	unlock()
	close(s.saved)
	return s.path, nil
}

func (*uploadPathLockingStore) Delete(context.Context, string) error { return nil }

type uploadTombstoneRestoreStore struct {
	fence            uint64
	tombstoneVersion uint64
	restoreCalls     int
	restoreExpected  uint64
	active           *coreauth.Auth
}

func (*uploadTombstoneRestoreStore) List(context.Context) ([]*coreauth.Auth, error) {
	return nil, nil
}

func (*uploadTombstoneRestoreStore) GetByID(context.Context, string) (*coreauth.Auth, error) {
	return nil, nil
}

func (*uploadTombstoneRestoreStore) Save(context.Context, *coreauth.Auth) (string, error) {
	return "", coreauth.ErrAuthStoreDeleted
}

func (*uploadTombstoneRestoreStore) SaveVersioned(context.Context, *coreauth.Auth, uint64) (string, uint64, error) {
	return "", 0, coreauth.ErrAuthStoreDeleted
}

func (s *uploadTombstoneRestoreStore) Restore(_ context.Context, auth *coreauth.Auth, expected uint64) (string, uint64, error) {
	s.restoreCalls++
	s.restoreExpected = expected
	s.active = auth.Clone()
	s.active.SetStoreGeneration(expected + 1)
	return auth.ID, expected + 1, nil
}

func (*uploadTombstoneRestoreStore) Delete(context.Context, string) error { return nil }

func (s *uploadTombstoneRestoreStore) AuthLifecycleFence(context.Context) (uint64, error) {
	return s.fence, nil
}

func (s *uploadTombstoneRestoreStore) GetAuthLifecycle(context.Context, string) (coreauth.AuthLifecycleState, error) {
	return coreauth.AuthLifecycleState{
		Exists:           true,
		Deleted:          true,
		Generation:       6,
		LifecycleVersion: s.tombstoneVersion,
	}, nil
}

func TestUploadAuthFile_PreservesPriorityAttributes(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	content := `{"type":"codex","email":"midai0530@gmail.com","priority":98}`

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "codex-midai0530@gmail.com-plus.json")
	if err != nil {
		t.Fatalf("failed to create multipart file: %v", err)
	}
	if _, err = part.Write([]byte(content)); err != nil {
		t.Fatalf("failed to write multipart content: %v", err)
	}
	if err = writer.Close(); err != nil {
		t.Fatalf("failed to close multipart writer: %v", err)
	}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected upload status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var payload map[string]any
	if err = json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if status, _ := payload["status"].(string); status != "ok" {
		t.Fatalf("expected status ok, got %#v", payload["status"])
	}

	auth, ok := manager.GetByID("codex-midai0530@gmail.com-plus.json")
	if !ok || auth == nil {
		t.Fatalf("expected uploaded auth record to exist")
	}
	if got := auth.Attributes["priority"]; got != "98" {
		t.Fatalf("priority attribute = %q, want %q", got, "98")
	}
	if got := auth.Metadata["priority"]; got != float64(98) {
		t.Fatalf("priority metadata = %#v, want 98", got)
	}
}

func TestUploadAuthFile_HonorsSharedPathLock(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	name := "locked.json"
	path := filepath.Join(authDir, name)
	store := &uploadPathLockingStore{path: path, saved: make(chan struct{})}
	manager := coreauth.NewManager(store, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, errCreate := writer.CreateFormFile("file", name)
	if errCreate != nil {
		t.Fatalf("create multipart file: %v", errCreate)
	}
	if _, errWrite := part.Write([]byte(`{"type":"codex","email":"locked@example.com"}`)); errWrite != nil {
		t.Fatalf("write multipart content: %v", errWrite)
	}
	if errClose := writer.Close(); errClose != nil {
		t.Fatalf("close multipart writer: %v", errClose)
	}
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	ctx.Request = req

	unlock := authfilelock.Lock(path)
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()
	done := make(chan struct{})
	started := make(chan struct{})
	go func() {
		close(started)
		h.UploadAuthFile(ctx)
		close(done)
	}()
	<-started
	select {
	case <-done:
		t.Fatal("upload completed while the shared auth file lock was held")
	case <-time.After(100 * time.Millisecond):
	}
	if _, errStat := os.Stat(path); !os.IsNotExist(errStat) {
		t.Fatalf("auth file was written while the shared lock was held: %v", errStat)
	}

	unlock()
	locked = false
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("upload did not complete after the shared auth file lock was released")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	select {
	case <-store.saved:
	default:
		t.Fatal("upload did not release the shared path lock before manager persistence")
	}
}

func TestUploadAuthFileRestoresWriterTombstoneDespiteStaleLocalRuntime(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	authID := "stale-runtime.json"
	const fence uint64 = 10
	store := &uploadTombstoneRestoreStore{
		fence:            fence,
		tombstoneVersion: fence - 1,
	}
	manager := coreauth.NewManager(store, nil, nil)
	stale := &coreauth.Auth{
		ID:       authID,
		FileName: authID,
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"type": "codex", "access_token": "stale-token"},
	}
	stale.SetStoreGeneration(5)
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), stale); errRegister != nil {
		t.Fatalf("seed stale runtime: %v", errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = store

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, errCreate := writer.CreateFormFile("file", authID)
	if errCreate != nil {
		t.Fatalf("create multipart file: %v", errCreate)
	}
	if _, errWrite := part.Write([]byte(`{"type":"codex","access_token":"fresh-token"}`)); errWrite != nil {
		t.Fatalf("write multipart content: %v", errWrite)
	}
	if errClose := writer.Close(); errClose != nil {
		t.Fatalf("close multipart writer: %v", errClose)
	}
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	ginCtx.Request = request

	h.UploadAuthFile(ginCtx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if store.restoreCalls != 1 || store.restoreExpected != 6 {
		t.Fatalf("Restore calls/expected = %d/%d, want 1/6", store.restoreCalls, store.restoreExpected)
	}
	current, exists := manager.GetByID(authID)
	if !exists || current == nil {
		t.Fatal("restored auth missing from runtime")
	}
	if current.StoreGeneration() != 7 || current.Metadata["access_token"] != "fresh-token" {
		t.Fatalf("restored runtime auth = %#v generation=%d", current.Metadata, current.StoreGeneration())
	}
}
