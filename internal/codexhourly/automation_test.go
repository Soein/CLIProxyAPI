package codexhourly

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	codexweekly "github.com/router-for-me/CLIProxyAPI/v6/internal/codexweekly"
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

func TestAutomationRunOnce_DisablesCodexAuthWhenHourlyLimitReached(t *testing.T) {
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
					"primary_window": {"limit_window_seconds": 18000, "limit_reached": true},
					"secondary_window": {"limit_window_seconds": 604800, "limit_reached": false}
				}
			}`,
		},
	}
	cfg := &internalconfig.Config{
		CodexHourlyAutomation: internalconfig.CodexHourlyAutomation{
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
		t.Fatal("expected auth to be disabled after hourly limit")
	}
	if auth.Status != coreauth.StatusDisabled {
		t.Fatalf("status = %s, want %s", auth.Status, coreauth.StatusDisabled)
	}
	if auth.StatusMessage != StatusMessageAutoDisabled {
		t.Fatalf("status message = %q, want %q", auth.StatusMessage, StatusMessageAutoDisabled)
	}
	if auth.Metadata == nil || auth.Metadata[AutoDisabledAtMetadataKey] == nil {
		t.Fatal("expected audit timestamp codex_hourly_auto_disabled_at to be written")
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

// TestAutomationRunOnce_ReenablesOnlyHourlyAutoDisabledAfterRecovery 验证 self-gate:
// 只启用带 codex_hourly_auto_disabled_at 标记的 auth,其他来源禁用的 auth 不动。
func TestAutomationRunOnce_ReenablesOnlyHourlyAutoDisabledAfterRecovery(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 12, 1, 0, 0, 0, time.UTC)
	manager := &fakeAuthManager{
		auths: map[string]*coreauth.Auth{
			"hourly-auto-disabled": {
				ID:            "hourly-auto-disabled",
				Provider:      "codex",
				FileName:      "hourly-auto-disabled.json",
				Status:        coreauth.StatusDisabled,
				Disabled:      true,
				StatusMessage: StatusMessageAutoDisabled,
				Metadata: map[string]any{
					"account_id":                "acct_hourly",
					"access_token":              "token",
					AutoDisabledAtMetadataKey:   now.Add(-10 * time.Minute).Format(time.RFC3339),
				},
			},
			"weekly-disabled": {
				ID:            "weekly-disabled",
				Provider:      "codex",
				FileName:      "weekly-disabled.json",
				Status:        coreauth.StatusDisabled,
				Disabled:      true,
				StatusMessage: "disabled via codex weekly automation",
				Metadata: map[string]any{
					"account_id":                                 "acct_weekly",
					"access_token":                               "token",
					codexweekly.AutoDisabledAtMetadataKey:        now.Add(-time.Hour).Format(time.RFC3339),
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
			"hourly-auto-disabled": `{
				"rate_limit": {
					"allowed": true,
					"primary_window": {"limit_window_seconds": 18000},
					"secondary_window": {"limit_window_seconds": 604800}
				}
			}`,
			"weekly-disabled": `{
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
		CodexHourlyAutomation: internalconfig.CodexHourlyAutomation{
			Enabled:         true,
			IntervalSeconds: 300,
		},
	}

	automation := NewAutomation(manager, func() *internalconfig.Config { return cfg })
	automation.now = func() time.Time { return now }

	automation.RunOnce(context.Background())

	hourly := manager.auths["hourly-auto-disabled"]
	if hourly.Disabled {
		t.Fatal("expected hourly-auto-disabled auth to be re-enabled after recovery")
	}
	if _, ok := hourly.Metadata[AutoDisabledAtMetadataKey]; ok {
		t.Fatal("expected codex_hourly_auto_disabled_at to be cleared after recovery")
	}

	// 交互隔离: 不触碰 weekly 禁用的 auth。
	weekly := manager.auths["weekly-disabled"]
	if !weekly.Disabled {
		t.Fatal("expected weekly-disabled auth to remain disabled (hourly must not touch it)")
	}
	if _, ok := weekly.Metadata[codexweekly.AutoDisabledAtMetadataKey]; !ok {
		t.Fatal("expected weekly audit timestamp to remain untouched by hourly automation")
	}

	// 交互隔离: 不触碰手动禁用的 auth。
	manual := manager.auths["manual-disabled"]
	if !manual.Disabled {
		t.Fatal("expected manually-disabled auth to remain disabled")
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
					"account_id":   "acct_excluded",
					"access_token": "token",
					codexweekly.AutomationExcludedMetadataKey: true,
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
		CodexHourlyAutomation: internalconfig.CodexHourlyAutomation{
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
	if auth.StatusMessage == StatusMessageAutoDisabled {
		t.Fatal("expected excluded auth to never get auto-disabled status message")
	}
}

// TestHourlyLimitReached_OnlyTriggersFor18000Window 验证窗口时长门禁:
// 同样的 limit_reached=true,如果 primary_window 是 604800 而非 18000,hourly 不应命中。
func TestHourlyLimitReached_OnlyTriggersFor18000Window(t *testing.T) {
	t.Parallel()

	// primary=18000 -> 命中
	hitBody := []byte(`{"rate_limit":{"limit_reached":true,"primary_window":{"limit_window_seconds":18000}}}`)
	if !hourlyLimitReached(hitBody) {
		t.Fatal("expected hourlyLimitReached=true for primary 18000 with limit_reached")
	}

	// primary=604800 -> 不命中 hourly
	weeklyOnlyBody := []byte(`{"rate_limit":{"limit_reached":true,"primary_window":{"limit_window_seconds":604800}}}`)
	if hourlyLimitReached(weeklyOnlyBody) {
		t.Fatal("expected hourlyLimitReached=false for primary 604800 (weekly-only)")
	}

	// 无 limit_reached,但 allowed=false
	disallowedBody := []byte(`{"rate_limit":{"allowed":false,"primary_window":{"limit_window_seconds":18000}}}`)
	if !hourlyLimitReached(disallowedBody) {
		t.Fatal("expected hourlyLimitReached=true when allowed=false on 18000 window")
	}
}

// TestHourlyLimitReached_WindowScoped 是针对"周限命中不应被 hourly 误触发"bug 的
// 回归测试。对称于 codexweekly.TestWeeklyLimitReached_WindowScoped。
func TestHourlyLimitReached_WindowScoped(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		body string
		want bool
	}{
		{
			name: "only secondary (weekly) hit - must not trigger hourly",
			body: `{
				"rate_limit": {
					"limit_reached": true,
					"primary_window":   {"limit_window_seconds": 18000,  "limit_reached": false},
					"secondary_window": {"limit_window_seconds": 604800, "limit_reached": true}
				}
			}`,
			want: false,
		},
		{
			name: "only primary (5h) hit - must trigger hourly",
			body: `{
				"rate_limit": {
					"limit_reached": true,
					"primary_window":   {"limit_window_seconds": 18000,  "limit_reached": true},
					"secondary_window": {"limit_window_seconds": 604800, "limit_reached": false}
				}
			}`,
			want: true,
		},
		{
			name: "5h window uses used_percent=100 signal",
			body: `{
				"rate_limit": {
					"limit_reached": true,
					"primary_window":   {"limit_window_seconds": 18000,  "used_percent": 100},
					"secondary_window": {"limit_window_seconds": 604800, "used_percent": 10}
				}
			}`,
			want: true,
		},
		{
			name: "both windows below used_percent=100",
			body: `{
				"rate_limit": {
					"limit_reached": false,
					"primary_window":   {"limit_window_seconds": 18000,  "used_percent": 68},
					"secondary_window": {"limit_window_seconds": 604800, "used_percent": 20}
				}
			}`,
			want: false,
		},
		{
			name: "legacy single 5h window with top-level limit_reached",
			body: `{
				"rate_limit": {
					"limit_reached": true,
					"primary_window":   {"limit_window_seconds": 18000}
				}
			}`,
			want: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := hourlyLimitReached([]byte(tc.body)); got != tc.want {
				t.Fatalf("hourlyLimitReached = %v, want %v", got, tc.want)
			}
		})
	}
}
