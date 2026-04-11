package codexweekly

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type fakeAuthManager struct {
	auths        map[string]*coreauth.Auth
	responseBody map[string]string
	updateCalls  int
}

func (f *fakeAuthManager) List() []*coreauth.Auth {
	out := make([]*coreauth.Auth, 0, len(f.auths))
	for _, auth := range f.auths {
		out = append(out, auth.Clone())
	}
	return out
}

func (f *fakeAuthManager) Update(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	f.updateCalls++
	f.auths[auth.ID] = auth.Clone()
	return auth.Clone(), nil
}

func (f *fakeAuthManager) NewHttpRequest(
	ctx context.Context,
	_ *coreauth.Auth,
	method, targetURL string,
	body []byte,
	headers http.Header,
) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, targetURL, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header = headers.Clone()
	return req, nil
}

func (f *fakeAuthManager) HttpRequest(_ context.Context, auth *coreauth.Auth, _ *http.Request) (*http.Response, error) {
	body := f.responseBody[auth.ID]
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}, nil
}

func TestAutomationRunOnce_DisablesCodexAuthWhenWeeklyLimitReached(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 12, 0, 0, 0, 0, time.UTC)
	manager := &fakeAuthManager{
		auths: map[string]*coreauth.Auth{
			"codex-auth": {
				ID:       "codex-auth",
				Provider: "codex",
				FileName: "codex-auth.json",
				Status:   coreauth.StatusActive,
				Metadata: map[string]any{
					"account_id":   "acct_123",
					"access_token": "token",
				},
			},
		},
		responseBody: map[string]string{
			"codex-auth": `{
				"rate_limit": {
					"limit_reached": true,
					"primary_window": {"limit_window_seconds": 18000},
					"secondary_window": {"limit_window_seconds": 604800}
				}
			}`,
		},
	}
	cfg := &internalconfig.Config{
		CodexWeeklyAutomation: internalconfig.CodexWeeklyAutomation{
			Enabled:         true,
			IntervalSeconds: 300,
		},
	}

	automation := NewAutomation(manager, func() *internalconfig.Config { return cfg })
	automation.now = func() time.Time { return now }

	automation.RunOnce(context.Background())

	auth := manager.auths["codex-auth"]
	if auth == nil {
		t.Fatal("updated auth = nil")
	}
	if !auth.Disabled {
		t.Fatal("expected auth to be disabled after weekly limit")
	}
	if auth.Status != coreauth.StatusDisabled {
		t.Fatalf("status = %s, want %s", auth.Status, coreauth.StatusDisabled)
	}
	if !HasAutoDisabledMarker(auth) {
		t.Fatal("expected auto-disabled marker to be written")
	}
	if auth.StatusMessage != StatusMessageAutoDisabled {
		t.Fatalf("status message = %q, want %q", auth.StatusMessage, StatusMessageAutoDisabled)
	}
	status := automation.Status()
	if !status.Enabled {
		t.Fatal("expected automation status enabled")
	}
	if status.AutoDisabledCount != 1 {
		t.Fatalf("auto disabled count = %d, want 1", status.AutoDisabledCount)
	}
	if status.LastCheckedAt == nil || !status.LastCheckedAt.Equal(now) {
		t.Fatalf("last checked at = %v, want %v", status.LastCheckedAt, now)
	}
}

func TestAutomationRunOnce_ReenablesOnlyAutoDisabledAuthAfterWeeklyRecovery(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 12, 1, 0, 0, 0, time.UTC)
	manager := &fakeAuthManager{
		auths: map[string]*coreauth.Auth{
			"auto-disabled": {
				ID:            "auto-disabled",
				Provider:      "codex",
				FileName:      "auto-disabled.json",
				Status:        coreauth.StatusDisabled,
				Disabled:      true,
				StatusMessage: StatusMessageAutoDisabled,
				Metadata: map[string]any{
					"account_id":                  "acct_auto",
					"access_token":                "token",
					AutoDisabledMetadataKey:       true,
					AutoDisabledAtMetadataKey:     now.Add(-time.Hour).Format(time.RFC3339),
					AutoDisabledReasonMetadataKey: "weekly_limit",
				},
			},
			"manual-disabled": {
				ID:            "manual-disabled",
				Provider:      "codex",
				FileName:      "manual-disabled.json",
				Status:        coreauth.StatusDisabled,
				Disabled:      true,
				StatusMessage: "disabled via management API",
				Metadata: map[string]any{
					"account_id":   "acct_manual",
					"access_token": "token",
				},
			},
		},
		responseBody: map[string]string{
			"auto-disabled": `{
				"rate_limit": {
					"allowed": true,
					"primary_window": {"limit_window_seconds": 18000},
					"secondary_window": {"limit_window_seconds": 604800}
				}
			}`,
			"manual-disabled": `{
				"rate_limit": {
					"allowed": true,
					"primary_window": {"limit_window_seconds": 18000},
					"secondary_window": {"limit_window_seconds": 604800}
				}
			}`,
		},
	}
	cfg := &internalconfig.Config{
		CodexWeeklyAutomation: internalconfig.CodexWeeklyAutomation{
			Enabled:         true,
			IntervalSeconds: 300,
		},
	}

	automation := NewAutomation(manager, func() *internalconfig.Config { return cfg })
	automation.now = func() time.Time { return now }

	automation.RunOnce(context.Background())

	autoDisabled := manager.auths["auto-disabled"]
	if autoDisabled.Disabled {
		t.Fatal("expected auto-disabled auth to be re-enabled")
	}
	if autoDisabled.Status != coreauth.StatusActive {
		t.Fatalf("status = %s, want %s", autoDisabled.Status, coreauth.StatusActive)
	}
	if HasAutoDisabledMarker(autoDisabled) {
		t.Fatal("expected auto-disabled marker to be cleared after recovery")
	}

	manualDisabled := manager.auths["manual-disabled"]
	if !manualDisabled.Disabled {
		t.Fatal("expected manual-disabled auth to remain disabled")
	}
	if manualDisabled.StatusMessage != "disabled via management API" {
		t.Fatalf("manual status message = %q, want manual disable message", manualDisabled.StatusMessage)
	}
}

func TestAutomationRunOnce_DoesNotDisableExcludedCodexAuth(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 12, 2, 0, 0, 0, time.UTC)
	manager := &fakeAuthManager{
		auths: map[string]*coreauth.Auth{
			"excluded-auth": {
				ID:       "excluded-auth",
				Provider: "codex",
				FileName: "excluded-auth.json",
				Status:   coreauth.StatusActive,
				Metadata: map[string]any{
					"account_id":                  "acct_excluded",
					"access_token":                "token",
					AutomationExcludedMetadataKey: true,
				},
			},
		},
		responseBody: map[string]string{
			"excluded-auth": `{
				"rate_limit": {
					"limit_reached": true,
					"primary_window": {"limit_window_seconds": 18000},
					"secondary_window": {"limit_window_seconds": 604800}
				}
			}`,
		},
	}
	cfg := &internalconfig.Config{
		CodexWeeklyAutomation: internalconfig.CodexWeeklyAutomation{
			Enabled:         true,
			IntervalSeconds: 300,
		},
	}

	automation := NewAutomation(manager, func() *internalconfig.Config { return cfg })
	automation.now = func() time.Time { return now }

	automation.RunOnce(context.Background())

	auth := manager.auths["excluded-auth"]
	if auth.Disabled {
		t.Fatal("expected excluded auth to remain enabled")
	}
	if HasAutoDisabledMarker(auth) {
		t.Fatal("expected excluded auth to never get auto-disabled marker")
	}
}

func TestAutomationRunOnce_DoesNotReenableExcludedAutoDisabledCodexAuth(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 12, 3, 0, 0, 0, time.UTC)
	manager := &fakeAuthManager{
		auths: map[string]*coreauth.Auth{
			"excluded-auto-disabled": {
				ID:            "excluded-auto-disabled",
				Provider:      "codex",
				FileName:      "excluded-auto-disabled.json",
				Status:        coreauth.StatusDisabled,
				Disabled:      true,
				StatusMessage: StatusMessageAutoDisabled,
				Metadata: map[string]any{
					"account_id":                  "acct_excluded_auto",
					"access_token":                "token",
					AutoDisabledMetadataKey:       true,
					AutoDisabledAtMetadataKey:     now.Add(-time.Hour).Format(time.RFC3339),
					AutoDisabledReasonMetadataKey: "weekly_limit",
					AutomationExcludedMetadataKey: true,
				},
			},
		},
		responseBody: map[string]string{
			"excluded-auto-disabled": `{
				"rate_limit": {
					"allowed": true,
					"primary_window": {"limit_window_seconds": 18000},
					"secondary_window": {"limit_window_seconds": 604800}
				}
			}`,
		},
	}
	cfg := &internalconfig.Config{
		CodexWeeklyAutomation: internalconfig.CodexWeeklyAutomation{
			Enabled:         true,
			IntervalSeconds: 300,
		},
	}

	automation := NewAutomation(manager, func() *internalconfig.Config { return cfg })
	automation.now = func() time.Time { return now }

	automation.RunOnce(context.Background())

	auth := manager.auths["excluded-auto-disabled"]
	if !auth.Disabled {
		t.Fatal("expected excluded auto-disabled auth to stay disabled")
	}
	if !HasAutoDisabledMarker(auth) {
		t.Fatal("expected auto-disabled marker to remain while excluded")
	}
}
