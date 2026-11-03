package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/meta"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

type unsupportedEditTokenStorage struct{ metadata map[string]any }

func (s *unsupportedEditTokenStorage) SetMetadata(metadata map[string]any) { s.metadata = metadata }
func (s *unsupportedEditTokenStorage) MarshalTokenJSON() ([]byte, error) {
	return json.Marshal(s.metadata)
}
func (s *unsupportedEditTokenStorage) SaveTokenToFile(path string) error {
	data, errMarshal := s.MarshalTokenJSON()
	if errMarshal != nil {
		return errMarshal
	}
	return os.WriteFile(path, data, 0o600)
}

func TestPatchAuthFileFieldsReportsUnsupportedPrivateStorage(t *testing.T) {
	dir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(dir)
	manager := coreauth.NewManager(store, nil, nil)
	base, errRegister := manager.Register(context.Background(), &coreauth.Auth{ID: "sdk.json", FileName: "sdk.json", Provider: "claude", Status: coreauth.StatusActive,
		Metadata: map[string]any{"type": "claude", "access_token": "initial"}, Storage: &unsupportedEditTokenStorage{}})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: dir}, manager)
	router := gin.New()
	router.PATCH("/v8/management/credentials/fields", h.PatchAuthFileFields)
	patch := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPatch, "/v8/management/credentials/fields", strings.NewReader(`{"name":"sdk.json","note":"operator"}`))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(rec, req)
		return rec
	}
	for range 2 {
		rec := patch()
		live, _ := manager.GetByID(base.ID)
		if rec.Code != http.StatusUnprocessableEntity || live.Generation != base.Generation || live.Disabled || live.Metadata["note"] != nil {
			t.Fatalf("unsupported edit status=%v body=%s runtime generation=%v", rec.Code, rec.Body.String(), live.Generation)
		}
	}
	path := filepath.Join(dir, base.ID)
	if errWrite := os.WriteFile(path, []byte(`{"type":"claude","access_token":"rotated"}`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if rec := patch(); rec.Code != http.StatusConflict {
		t.Fatalf("real rotation must take priority over writer capability: status=%v body=%s", rec.Code, rec.Body.String())
	}
	if rec := patch(); rec.Code != http.StatusOK {
		t.Fatalf("fresh file-backed retry failed: status=%v body=%s", rec.Code, rec.Body.String())
	}
	data, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	var metadata map[string]any
	if errDecode := json.Unmarshal(data, &metadata); errDecode != nil || metadata["access_token"] != "rotated" || metadata["note"] != "operator" {
		t.Fatalf("retry lost the new credential or edit: %s err=%v", data, errDecode)
	}
}

func TestPatchAuthFileFieldsMetaRelativePathWithoutBaseDir(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	manager := coreauth.NewManager(sdkauth.NewFileTokenStore(), nil, nil)
	if _, errRegister := manager.Register(context.Background(), &coreauth.Auth{ID: "meta.json", FileName: "meta.json", Provider: "meta", Status: coreauth.StatusActive,
		Metadata: map[string]any{"type": "meta", "access_token": "LLM|initial"}, Storage: &meta.MetaTokenStorage{AccessToken: "LLM|initial"}}); errRegister != nil {
		t.Fatal(errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: dir}, manager)
	router := gin.New()
	router.PATCH("/v8/management/credentials/fields", h.PatchAuthFileFields)
	for _, note := range []string{"first", "second"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPatch, "/v8/management/credentials/fields", strings.NewReader(`{"name":"meta.json","note":"`+note+`"}`))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("relative Meta PATCH status=%v body=%s", rec.Code, rec.Body.String())
		}
		data, errRead := os.ReadFile("meta.json")
		if errRead != nil {
			t.Fatal(errRead)
		}
		var metadata map[string]any
		if errDecode := json.Unmarshal(data, &metadata); errDecode != nil || metadata["note"] != note || metadata["access_token"] != "LLM|initial" {
			t.Fatalf("relative Meta PATCH lost credential or note: %s err=%v", data, errDecode)
		}
	}
}
