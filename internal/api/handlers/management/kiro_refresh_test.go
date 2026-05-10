package management

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestPostKiroRefreshNotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &Handler{cfg: &config.Config{AuthDir: t.TempDir()}}
	r := gin.New()
	r.POST("/v0/management/auth/kiro/:name/refresh", h.PostKiroRefresh)

	req := httptest.NewRequest("POST", "/v0/management/auth/kiro/missing.json/refresh", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Errorf("status = %d; want 404", w.Code)
	}
}
