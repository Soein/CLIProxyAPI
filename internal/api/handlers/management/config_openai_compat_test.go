package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestGetOpenAICompatIncludesDisableCooling(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	requestRetry := 0
	disableCooling := true
	h := NewHandlerWithoutConfigFilePath(&config.Config{
		OpenAICompatibility: []config.OpenAICompatibility{
			{
				Name:    "Mimo CN",
				BaseURL: "https://token-plan-cn.xiaomimimo.com/v1",
				APIKeyEntries: []config.OpenAICompatibilityAPIKey{
					{APIKey: "test-key"},
				},
				Models: []config.OpenAICompatibilityModel{
					{Name: "mimo-v2.5", Alias: ""},
				},
				SupportPromptCacheKey: true,
				DisableCooling:        &disableCooling,
				RequestRetry:          &requestRetry,
			},
		},
	}, nil)

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/openai-compatibility", nil)
	h.GetOpenAICompat(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var body struct {
		OpenAICompatibility []struct {
			SupportPromptCacheKey *bool `json:"support-prompt-cache-key"`
			DisableCooling        *bool `json:"disable-cooling"`
			RequestRetry          *int  `json:"request-retry"`
		} `json:"openai-compatibility"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(body.OpenAICompatibility) != 1 {
		t.Fatalf("expected 1 openai-compatibility entry, got %d", len(body.OpenAICompatibility))
	}
	if body.OpenAICompatibility[0].SupportPromptCacheKey == nil || !*body.OpenAICompatibility[0].SupportPromptCacheKey {
		t.Fatalf("expected support-prompt-cache-key to be present and true, got %#v", body.OpenAICompatibility[0].SupportPromptCacheKey)
	}
	if body.OpenAICompatibility[0].DisableCooling == nil || !*body.OpenAICompatibility[0].DisableCooling {
		t.Fatalf("expected disable-cooling to be present and true, got %#v", body.OpenAICompatibility[0].DisableCooling)
	}
	if body.OpenAICompatibility[0].RequestRetry == nil || *body.OpenAICompatibility[0].RequestRetry != 0 {
		t.Fatalf("expected request-retry to be present and 0, got %#v", body.OpenAICompatibility[0].RequestRetry)
	}
}

func TestPatchOpenAICompatRejectsInvalidRequestScopedErrors(t *testing.T) {
	tests := []struct {
		name    string
		rule    string
		wantErr string
	}{
		{name: "invalid status", rule: `{"status":600,"match":["body"],"action":"stop"}`, wantErr: "status must be between 100 and 599"},
		{name: "unknown action", rule: `{"status":400,"match":["body"],"action":"retry"}`, wantErr: "action must be one of"},
		{name: "empty matcher", rule: `{"status":400,"match":["  "],"action":"stop"}`, wantErr: "at least one non-empty matcher is required"},
		{name: "malformed regex", rule: `{"status":400,"match-regexr":["secret-("],"action":"stop"}`, wantErr: "match-regexr[0] must be a valid regular expression"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{Name: "provider", BaseURL: "https://example.com/v1"}}}
			h := &Handler{cfg: cfg, configFilePath: writeTestConfigFile(t)}
			rec := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(rec)
			body := `{"index":0,"value":{"request-scoped-errors":[` + test.rule + `]}}`
			ctx.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/openai-compatibility", strings.NewReader(body))
			ctx.Request.Header.Set("Content-Type", "application/json")

			h.PatchOpenAICompat(ctx)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), test.wantErr) {
				t.Fatalf("body = %q, want substring %q", rec.Body.String(), test.wantErr)
			}
			if strings.Contains(rec.Body.String(), "secret-(") {
				t.Fatalf("response leaked matcher content: %s", rec.Body.String())
			}
			if len(cfg.OpenAICompatibility[0].RequestScopedErrors) != 0 {
				t.Fatal("invalid PATCH changed config")
			}
		})
	}
}

func TestPutOpenAICompatRejectsInvalidRequestScopedErrors(t *testing.T) {
	h := &Handler{cfg: &config.Config{}, configFilePath: writeTestConfigFile(t)}
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(http.MethodPut, "/v0/management/openai-compatibility", strings.NewReader(`[{"name":"provider","base-url":"https://example.com/v1","request-scoped-errors":[{"status":400,"match":["body"],"action":"retry"}]}]`))
	ctx.Request.Header.Set("Content-Type", "application/json")

	h.PutOpenAICompat(ctx)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if len(h.cfg.OpenAICompatibility) != 0 {
		t.Fatal("invalid PUT changed config")
	}
}
