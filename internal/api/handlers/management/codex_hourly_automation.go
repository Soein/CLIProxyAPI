package management

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexhourly"
)

func (h *Handler) GetCodexHourlyAutomationEnabled(c *gin.Context) {
	enabled := false
	if h != nil && h.cfg != nil {
		enabled = h.cfg.CodexHourlyAutomation.Enabled
	}
	c.JSON(http.StatusOK, gin.H{"enabled": enabled})
}

func (h *Handler) PutCodexHourlyAutomationEnabled(c *gin.Context) {
	h.updateBoolField(c, func(v bool) {
		h.cfg.CodexHourlyAutomation.Enabled = v
	})
}

func (h *Handler) GetCodexHourlyAutomationIntervalSeconds(c *gin.Context) {
	interval := 0
	if h != nil && h.cfg != nil {
		interval = h.cfg.CodexHourlyAutomation.IntervalSeconds
	}
	c.JSON(http.StatusOK, gin.H{"interval-seconds": interval})
}

func (h *Handler) PutCodexHourlyAutomationIntervalSeconds(c *gin.Context) {
	h.updateIntField(c, func(v int) {
		h.cfg.CodexHourlyAutomation.IntervalSeconds = v
	})
}

func (h *Handler) GetCodexHourlyAutomationStatus(c *gin.Context) {
	status := codexhourly.Status{}
	if h != nil && h.cfg != nil {
		status.Enabled = h.cfg.CodexHourlyAutomation.Enabled
	}
	if h != nil && h.codexHourlyStatus != nil {
		status = h.codexHourlyStatus()
	}
	// See codex_weekly_automation.go for the cluster overlay rationale.
	if h != nil && h.codexAutomationReader != nil {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		if t, err := h.codexAutomationReader.GetCodexAutomationLatest(ctx, "hourly"); err == nil && !t.IsZero() {
			ts := t
			if status.LastCheckedAt == nil || ts.After(*status.LastCheckedAt) {
				status.LastCheckedAt = &ts
			}
		}
	}
	c.JSON(http.StatusOK, status)
}
