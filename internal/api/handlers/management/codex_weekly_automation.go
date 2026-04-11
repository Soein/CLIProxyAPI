package management

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexweekly"
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
	c.JSON(http.StatusOK, status)
}
