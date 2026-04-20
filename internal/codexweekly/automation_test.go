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
	"github.com/tidwall/gjson"
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
					"primary_window": {"limit_window_seconds": 18000, "limit_reached": false},
					"secondary_window": {"limit_window_seconds": 604800, "limit_reached": true}
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
	if auth.StatusMessage != StatusMessageAutoDisabled {
		t.Fatalf("status message = %q, want %q", auth.StatusMessage, StatusMessageAutoDisabled)
	}
	if auth.Metadata == nil || auth.Metadata[AutoDisabledAtMetadataKey] == nil {
		t.Fatal("expected audit timestamp codex_weekly_auto_disabled_at to be written")
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

// TestAutomationRunOnce_ReenablesOnlyWeeklyAutoDisabledAfterRecovery 验证新的 self-gate 语义:
// 只有带 weekly 自身审计标记 (codex_weekly_auto_disabled_at 或旧 codex_weekly_auto_disabled) 的
// auth 在周限恢复后会被自动启用;手动禁用的 auth 或被其他 automation(hourly)禁用的 auth
// 不会被 weekly 错误启用。
// 同时验证旧版 marker 字段被 autoReenable 兼容清理。
func TestAutomationRunOnce_ReenablesOnlyWeeklyAutoDisabledAfterRecovery(t *testing.T) {
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
					"account_id":                "acct_auto",
					"access_token":              "token",
					legacyAutoDisabledMarkerKey: true,
					AutoDisabledAtMetadataKey:   now.Add(-time.Hour).Format(time.RFC3339),
					legacyAutoReasonKey:         "weekly_limit",
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
			"hourly-disabled": {
				ID:            "hourly-disabled",
				Provider:      "codex",
				FileName:      "hourly-disabled.json",
				Status:        coreauth.StatusDisabled,
				Disabled:      true,
				StatusMessage: "disabled via codex hourly automation",
				Metadata: map[string]any{
					"account_id":                      "acct_hourly",
					"access_token":                    "token",
					"codex_hourly_auto_disabled_at":   now.Add(-10 * time.Minute).Format(time.RFC3339),
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
			"hourly-disabled": `{
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
	if autoDisabled.StatusMessage != "" {
		t.Fatalf("status message = %q, want empty after re-enable", autoDisabled.StatusMessage)
	}
	if _, ok := autoDisabled.Metadata[AutoDisabledAtMetadataKey]; ok {
		t.Fatal("expected auto_disabled_at audit timestamp to be cleared after recovery")
	}
	if _, ok := autoDisabled.Metadata[legacyAutoDisabledMarkerKey]; ok {
		t.Fatal("expected legacy marker codex_weekly_auto_disabled to be cleaned up")
	}
	if _, ok := autoDisabled.Metadata[legacyAutoReasonKey]; ok {
		t.Fatal("expected legacy reason codex_weekly_auto_reason to be cleaned up")
	}

	// self-gate 语义: 手动禁用的账号不应被 weekly automation 启用。
	manualDisabled := manager.auths["manual-disabled"]
	if !manualDisabled.Disabled {
		t.Fatal("expected manually-disabled auth to remain disabled (no weekly self-marker)")
	}

	// 交互隔离: weekly 不应启用被 hourly 禁用的账号。
	hourlyDisabled := manager.auths["hourly-disabled"]
	if !hourlyDisabled.Disabled {
		t.Fatal("expected hourly-disabled auth to remain disabled (weekly must not touch it)")
	}
	if _, ok := hourlyDisabled.Metadata["codex_hourly_auto_disabled_at"]; !ok {
		t.Fatal("expected hourly audit timestamp to remain untouched by weekly automation")
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
	if auth.StatusMessage == StatusMessageAutoDisabled {
		t.Fatal("expected excluded auth to never get auto-disabled status message")
	}
	if auth.Metadata != nil {
		if _, ok := auth.Metadata[AutoDisabledAtMetadataKey]; ok {
			t.Fatal("expected excluded auth to never get auto_disabled_at timestamp")
		}
	}
}

// TestAutomationRunOnce_DoesNotDisableLegacyExcludedCodexAuth 验证向后兼容:
// 持有旧字段 codex_weekly_automation_excluded=true 的 auth 应当仍然被识别为 excluded。
func TestAutomationRunOnce_DoesNotDisableLegacyExcludedCodexAuth(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 12, 4, 0, 0, 0, time.UTC)
	manager := &fakeAuthManager{
		auths: map[string]*coreauth.Auth{
			"legacy-excluded": {
				ID:       "legacy-excluded",
				Provider: "codex",
				FileName: "legacy-excluded.json",
				Status:   coreauth.StatusActive,
				Metadata: map[string]any{
					"account_id":                        "acct_legacy",
					"access_token":                      "token",
					LegacyWeeklyAutomationExcludedKey:   true,
					LegacyWeeklyAutomationExcludedAtKey: now.Add(-time.Hour).Format(time.RFC3339),
				},
			},
		},
		responseBody: map[string]string{
			"legacy-excluded": `{
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

	auth := manager.auths["legacy-excluded"]
	if auth.Disabled {
		t.Fatal("expected legacy-excluded auth to remain enabled (legacy field recognized)")
	}
}

// TestSetAutomationExcluded_MigratesLegacyFieldToNewKey 验证写时迁移:
// SetAutomationExcluded 会清理旧字段并只写新字段。
func TestSetAutomationExcluded_MigratesLegacyFieldToNewKey(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 12, 5, 0, 0, 0, time.UTC)
	auth := &coreauth.Auth{
		Metadata: map[string]any{
			LegacyWeeklyAutomationExcludedKey:   true,
			LegacyWeeklyAutomationExcludedAtKey: now.Add(-time.Hour).Format(time.RFC3339),
		},
	}

	SetAutomationExcluded(auth, true, now)

	if v, ok := auth.Metadata[AutomationExcludedMetadataKey]; !ok || v != true {
		t.Fatalf("expected new key %s=true, got %v", AutomationExcludedMetadataKey, auth.Metadata[AutomationExcludedMetadataKey])
	}
	if _, ok := auth.Metadata[LegacyWeeklyAutomationExcludedKey]; ok {
		t.Fatal("expected legacy key to be removed after migration")
	}
	if _, ok := auth.Metadata[LegacyWeeklyAutomationExcludedAtKey]; ok {
		t.Fatal("expected legacy at-key to be removed after migration")
	}

	SetAutomationExcluded(auth, false, now)

	if _, ok := auth.Metadata[AutomationExcludedMetadataKey]; ok {
		t.Fatal("expected new key to be removed when excluded=false")
	}
	if _, ok := auth.Metadata[AutomationExcludedAtMetadataKey]; ok {
		t.Fatal("expected new at-key to be removed when excluded=false")
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
					AutoDisabledAtMetadataKey:     now.Add(-time.Hour).Format(time.RFC3339),
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
	if auth.StatusMessage != StatusMessageAutoDisabled {
		t.Fatalf("expected auto-disabled status message to remain while excluded, got %q", auth.StatusMessage)
	}
	if auth.Metadata == nil || auth.Metadata[AutoDisabledAtMetadataKey] == nil {
		t.Fatal("expected auto_disabled_at timestamp to remain while excluded")
	}
}

// TestWeeklyLimitReached_WindowScoped 是针对"5h 命中不应被 weekly 误触发"bug
// 的回归测试。生产中 Codex wham/usage 响应里 primary_window (5h) 与
// secondary_window (week) 共存,顶层 limit_reached=true 只代表至少一个窗口命中,
// 过去的实现直接读顶层值,会把 5h 命中误判为 weekly 命中并自动禁用账号。
func TestWeeklyLimitReached_WindowScoped(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		body string
		want bool
	}{
		{
			name: "only primary (5h) hit, secondary (weekly) not hit - must not trigger weekly",
			body: `{
				"rate_limit": {
					"limit_reached": true,
					"primary_window":   {"limit_window_seconds": 18000,  "limit_reached": true},
					"secondary_window": {"limit_window_seconds": 604800, "limit_reached": false}
				}
			}`,
			want: false,
		},
		{
			name: "only secondary (weekly) hit - must trigger weekly",
			body: `{
				"rate_limit": {
					"limit_reached": true,
					"primary_window":   {"limit_window_seconds": 18000,  "limit_reached": false},
					"secondary_window": {"limit_window_seconds": 604800, "limit_reached": true}
				}
			}`,
			want: true,
		},
		{
			name: "weekly window uses used_percent=100 signal",
			body: `{
				"rate_limit": {
					"limit_reached": true,
					"primary_window":   {"limit_window_seconds": 18000,  "used_percent": 68},
					"secondary_window": {"limit_window_seconds": 604800, "used_percent": 100}
				}
			}`,
			want: true,
		},
		{
			name: "both windows below used_percent=100",
			body: `{
				"rate_limit": {
					"limit_reached": false,
					"primary_window":   {"limit_window_seconds": 18000,  "used_percent": 52},
					"secondary_window": {"limit_window_seconds": 604800, "used_percent": 0}
				}
			}`,
			want: false,
		},
		{
			name: "legacy single weekly window with top-level limit_reached",
			body: `{
				"rate_limit": {
					"limit_reached": true,
					"primary_window":   {"limit_window_seconds": 604800}
				}
			}`,
			want: true,
		},
		{
			name: "legacy top-level without durations",
			body: `{
				"rate_limit": {
					"limit_reached": true,
					"primary_window":   {},
					"secondary_window": {}
				}
			}`,
			want: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := weeklyLimitReached([]byte(tc.body)); got != tc.want {
				t.Fatalf("weeklyLimitReached = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestWindowReached 覆盖共享 helper 的优先级判定。
func TestWindowReached(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		json    string
		want    bool
		wantOk  bool
	}{
		{"empty node", `{}`, false, false},
		{"limit_reached true wins", `{"limit_reached": true, "used_percent": 0}`, true, true},
		{"limit_reached false wins", `{"limit_reached": false, "used_percent": 100}`, false, true},
		{"camelCase limitReached", `{"limitReached": true}`, true, true},
		{"allowed false -> reached", `{"allowed": false}`, true, true},
		{"allowed true -> not reached", `{"allowed": true}`, false, true},
		{"used_percent >= 100", `{"used_percent": 100}`, true, true},
		{"used_percent < 100", `{"used_percent": 99}`, false, true},
		{"no signal", `{"limit_window_seconds": 604800}`, false, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := WindowReached(gjson.Parse(tc.json))
			if got != tc.want || ok != tc.wantOk {
				t.Fatalf("WindowReached = (%v, %v), want (%v, %v)", got, ok, tc.want, tc.wantOk)
			}
		})
	}
}
