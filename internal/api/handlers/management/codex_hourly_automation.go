package management

import (
	"net/http"

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
	c.JSON(http.StatusOK, status)
}
