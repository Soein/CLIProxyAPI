package management

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/codexweekly"
)

func (h *Handler) GetCodexWeeklyAutomationEnabled(c *gin.Context) {
	enabled := false
	if h != nil && h.cfg != nil {
		enabled = h.cfg.CodexWeeklyAutomation.Enabled
	}
	c.JSON(http.StatusOK, gin.H{"enabled": enabled})
}

func (h *Handler) PutCodexWeeklyAutomationEnabled(c *gin.Context) {
	h.updateBoolField(c, func(v bool) {
		h.cfg.CodexWeeklyAutomation.Enabled = v
	})
}

func (h *Handler) GetCodexWeeklyAutomationIntervalSeconds(c *gin.Context) {
	interval := 0
	if h != nil && h.cfg != nil {
		interval = h.cfg.CodexWeeklyAutomation.IntervalSeconds
	}
	c.JSON(http.StatusOK, gin.H{"interval-seconds": interval})
}

func (h *Handler) PutCodexWeeklyAutomationIntervalSeconds(c *gin.Context) {
	h.updateIntField(c, func(v int) {
		h.cfg.CodexWeeklyAutomation.IntervalSeconds = v
	})
}

func (h *Handler) GetCodexWeeklyAutomationStatus(c *gin.Context) {
	status := codexweekly.Status{}
	if h != nil && h.cfg != nil {
		status.Enabled = h.cfg.CodexWeeklyAutomation.Enabled
	}
	if h != nil && h.codexWeeklyStatus != nil {
		status = h.codexWeeklyStatus()
	}
	// Cluster overlay: when this node is a follower (or different shard
	// owner) it has never run weekly RunOnce so LastCheckedAt is nil.
	// Take MAX(last_run_at) across all nodes from PG instead.
	if h != nil && h.codexAutomationReader != nil {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		if t, err := h.codexAutomationReader.GetCodexAutomationLatest(ctx, "weekly"); err == nil && !t.IsZero() {
			ts := t
			if status.LastCheckedAt == nil || ts.After(*status.LastCheckedAt) {
				status.LastCheckedAt = &ts
			}
		}
	}
	c.JSON(http.StatusOK, status)
}
