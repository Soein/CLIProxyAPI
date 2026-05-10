package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func setupKiroPKCEHandler(t *testing.T) (*Handler, *gin.Engine) {
	gin.SetMode(gin.TestMode)
	h := &Handler{kiroSessions: newKiroSessionStore(0)}
	r := gin.New()
	r.POST("/v0/management/auth/kiro/login/pkce/start", h.PostKiroPKCEStart)
	return h, r
}

func TestPostKiroPKCEStart(t *testing.T) {
	_, r := setupKiroPKCEHandler(t)
	body := strings.NewReader(`{"provider":"google"}`)
	req := httptest.NewRequest("POST", "/v0/management/auth/kiro/login/pkce/start", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["session_id"] == "" || resp["auth_url"] == "" {
		t.Errorf("missing fields: %+v", resp)
	}
	if !strings.Contains(resp["auth_url"], "idp=Google") {
		t.Errorf("auth_url missing idp: %s", resp["auth_url"])
	}
}

func TestPostKiroPKCEStartInvalidProvider(t *testing.T) {
	_, r := setupKiroPKCEHandler(t)
	body := strings.NewReader(`{"provider":"facebook"}`)
	req := httptest.NewRequest("POST", "/v0/management/auth/kiro/login/pkce/start", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}
