package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	internalkiro "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/kiro"
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

func setupKiroDeviceHandler(t *testing.T) (*Handler, *gin.Engine) {
	gin.SetMode(gin.TestMode)
	h := &Handler{kiroSessions: newKiroSessionStore(10 * time.Minute)}
	r := gin.New()
	r.POST("/v0/management/auth/kiro/login/device/start", h.PostKiroDeviceStart)
	r.GET("/v0/management/auth/kiro/login/device/:sid", h.GetKiroDeviceStatus)
	return h, r
}

func TestGetKiroDeviceStatusReturnsPending(t *testing.T) {
	h, r := setupKiroDeviceHandler(t)
	sid := h.kiroSessions.NewDeviceSession("c", "s", "dc", "ABCD", "https://verify")

	req := httptest.NewRequest("GET", "/v0/management/auth/kiro/login/device/"+sid, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("status = %d", w.Code)
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "pending" {
		t.Errorf("status = %v", resp["status"])
	}
	if resp["user_code"] != "ABCD" {
		t.Errorf("user_code = %v", resp["user_code"])
	}
}

func TestGetKiroDeviceStatusReturnsSuccess(t *testing.T) {
	h, r := setupKiroDeviceHandler(t)
	sid := h.kiroSessions.NewDeviceSession("c", "s", "dc", "ABCD", "https://verify")
	h.kiroSessions.CompleteDevice(sid, &internalkiro.Credentials{AccessToken: "at"}, nil)

	req := httptest.NewRequest("GET", "/v0/management/auth/kiro/login/device/"+sid, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "success" {
		t.Errorf("status = %v", resp["status"])
	}
}

func TestGetKiroDeviceStatusUnknownSession(t *testing.T) {
	_, r := setupKiroDeviceHandler(t)
	req := httptest.NewRequest("GET", "/v0/management/auth/kiro/login/device/nonexistent", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d; want 404", w.Code)
	}
}
