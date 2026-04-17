package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexhourly"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestCodexHourlyAutomationHandlers_UpdateConfigAndExposeStatus(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	lastCheckedAt := time.Date(2026, 4, 12, 0, 30, 0, 0, time.UTC)
	h := &Handler{
		cfg: &config.Config{
			CodexHourlyAutomation: config.CodexHourlyAutomation{
				Enabled:         false,
				IntervalSeconds: 300,
			},
		},
		configFilePath: writeTestConfigFile(t),
	}
	h.SetCodexHourlyAutomationStatusProvider(func() codexhourly.Status {
		return codexhourly.Status{
			Enabled:           true,
			Running:           true,
			LastCheckedAt:     &lastCheckedAt,
			AutoDisabledCount: 3,
		}
	})

	t.Run("put enabled", func(t *testing.T) {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPut, "/v0/management/codex-hourly-automation/enabled", strings.NewReader(`{"value":true}`))
		c.Request.Header.Set("Content-Type", "application/json")

		h.PutCodexHourlyAutomationEnabled(c)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		if !h.cfg.CodexHourlyAutomation.Enabled {
			t.Fatal("expected enabled to be updated to true")
		}
	})

	t.Run("put interval", func(t *testing.T) {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPut, "/v0/management/codex-hourly-automation/interval-seconds", strings.NewReader(`{"value":600}`))
		c.Request.Header.Set("Content-Type", "application/json")

		h.PutCodexHourlyAutomationIntervalSeconds(c)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		if h.cfg.CodexHourlyAutomation.IntervalSeconds != 600 {
			t.Fatalf("interval = %d, want 600", h.cfg.CodexHourlyAutomation.IntervalSeconds)
		}
	})

	t.Run("get status", func(t *testing.T) {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodGet, "/v0/management/codex-hourly-automation/status", nil)

		h.GetCodexHourlyAutomationStatus(c)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var payload codexhourly.Status
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("unmarshal status response: %v", err)
		}
		if !payload.Enabled || !payload.Running {
			t.Fatalf("status payload = %+v, want enabled=true running=true", payload)
		}
		if payload.LastCheckedAt == nil || !payload.LastCheckedAt.Equal(lastCheckedAt) {
			t.Fatalf("last checked at = %v, want %v", payload.LastCheckedAt, lastCheckedAt)
		}
		if payload.AutoDisabledCount != 3 {
			t.Fatalf("auto disabled count = %d, want 3", payload.AutoDisabledCount)
		}
	})
}
