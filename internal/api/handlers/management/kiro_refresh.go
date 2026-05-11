package management

import (
	"net/http"
	"path/filepath"

	"github.com/gin-gonic/gin"
	internalkiro "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/kiro"
)

// PostKiroRefresh manually refreshes the access token for a Kiro credential file.
//
// URL: POST /v0/management/auth/kiro/{name}/refresh
// Response: {"status":"ok","expires_at":"..."} or {"error":"..."}
func (h *Handler) PostKiroRefresh(c *gin.Context) {
	if h == nil || h.cfg == nil || h.cfg.AuthDir == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "auth dir not configured"})
		return
	}
	name := c.Param("name")
	if isUnsafeAuthFileName(name) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid name"})
		return
	}
	full := filepath.Join(h.cfg.AuthDir, name)

	creds, err := internalkiro.LoadCredentials(full)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "credential not found: " + err.Error()})
		return
	}

	r := internalkiro.NewRefresher(nil)
	updated, err := r.Refresh(c.Request.Context(), creds)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "refresh failed: " + err.Error()})
		return
	}

	if err := internalkiro.SaveCredentials(full, updated); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "save failed: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":     "ok",
		"expires_at": updated.ExpiresAt,
	})
}
