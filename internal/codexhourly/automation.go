// Package codexhourly 提供 Codex 5h (primary_window, limit_window_seconds=18000)
// 限额的周期性主动探测与自动禁用/恢复机制,结构与 codexweekly 包严格对称。
package codexhourly

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	codexweekly "github.com/router-for-me/CLIProxyAPI/v6/internal/codexweekly"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

const (
	// AutoDisabledAtMetadataKey 记录 hourly automation 最近一次自动禁用该 auth 的时间戳 (审计用)。
	AutoDisabledAtMetadataKey = "codex_hourly_auto_disabled_at"
	// StatusMessageAutoDisabled 是 hourly automation 标注 auth.StatusMessage 的固定文案。
	StatusMessageAutoDisabled = "disabled via codex hourly automation"
	defaultInterval           = 5 * time.Minute
	pollTick                  = 1 * time.Second
	hourlyWindowSeconds       = 18000
	usageURL                  = "https://chatgpt.com/backend-api/wham/usage"
	codexUsageUserAgent       = "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal"
)

type authManager interface {
	List() []*coreauth.Auth
	Update(context.Context, *coreauth.Auth) (*coreauth.Auth, error)
	NewHttpRequest(context.Context, *coreauth.Auth, string, string, []byte, http.Header) (*http.Request, error)
	HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error)
}

// Status 汇报 hourly automation 的运行时状态,与 codexweekly.Status 对称。
type Status struct {
	Enabled           bool       `json:"enabled"`
	Running           bool       `json:"running"`
	LastCheckedAt     *time.Time `json:"last_checked_at"`
	AutoDisabledCount int        `json:"auto_disabled_count"`
}

// Automation 控制 hourly 窗口的轮询检测,结构镜像 codexweekly.Automation。
type Automation struct {
	manager   authManager
	getConfig func() *internalconfig.Config

	mu            sync.RWMutex
	lastCheckedAt time.Time
	started       bool
	checking      bool
	cancel        context.CancelFunc

	now func() time.Time
}

// NewAutomation 构造 hourly automation 实例。
func NewAutomation(manager authManager, getConfig func() *internalconfig.Config) *Automation {
	return &Automation{
		manager:   manager,
		getConfig: getConfig,
		now:       time.Now,
	}
}

// Start 启动 ticker loop。
func (a *Automation) Start(parent context.Context) {
	if a == nil {
		return
	}
	if parent == nil {
		parent = context.Background()
	}
	a.Stop()
	ctx, cancel := context.WithCancel(parent)
	a.mu.Lock()
	a.cancel = cancel
	a.started = true
	a.mu.Unlock()

	go func() {
		ticker := time.NewTicker(pollTick)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				a.mu.Lock()
				a.started = false
				a.cancel = nil
				a.mu.Unlock()
				return
			case <-ticker.C:
				cfg := a.currentConfig()
				if cfg == nil || !cfg.CodexHourlyAutomation.Enabled {
					continue
				}
				interval := automationInterval(cfg)
				if lastCheckedAt := a.lastChecked(); !lastCheckedAt.IsZero() && a.now().Sub(lastCheckedAt) < interval {
					continue
				}
				a.RunOnce(ctx)
			}
		}
	}()
}

// Stop 关停 ticker loop。
func (a *Automation) Stop() {
	if a == nil {
		return
	}
	a.mu.Lock()
	cancel := a.cancel
	a.cancel = nil
	a.started = false
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Status 汇报当前状态。
func (a *Automation) Status() Status {
	if a == nil {
		return Status{}
	}
	cfg := a.currentConfig()
	enabled := cfg != nil && cfg.CodexHourlyAutomation.Enabled

	a.mu.RLock()
	lastCheckedAt := a.lastCheckedAt
	started := a.started
	a.mu.RUnlock()

	status := Status{
		Enabled:           enabled,
		Running:           started && enabled,
		AutoDisabledCount: countAutoDisabledAuths(a.listAuths()),
	}
	if !lastCheckedAt.IsZero() {
		ts := lastCheckedAt
		status.LastCheckedAt = &ts
	}
	return status
}

// RunOnce 执行一次全量检查。
func (a *Automation) RunOnce(ctx context.Context) {
	if a == nil || a.manager == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	a.mu.Lock()
	if a.checking {
		a.mu.Unlock()
		return
	}
	a.checking = true
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.checking = false
		a.mu.Unlock()
	}()

	auths := a.listAuths()
	log.Infof("codex hourly automation: RunOnce started, total auths from manager=%d", len(auths))
	var codexCount, managedCount, excludedCount, checkedCount, reachedCount, reenabledCount int
	providerCounts := make(map[string]int)
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		providerCounts[strings.ToLower(strings.TrimSpace(auth.Provider))]++
		if strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
			codexCount++
		}
		if !shouldManageAuth(auth) {
			continue
		}
		managedCount++
		if codexweekly.IsAutomationExcluded(auth) {
			excludedCount++
		}
		accountID := resolveAccountID(auth)
		tokenLen := 0
		if auth.Metadata != nil {
			if v, ok := auth.Metadata["access_token"].(string); ok {
				tokenLen = len(v)
			}
		}
		log.Infof("codex hourly automation: managed auth id=%s provider=%s disabled=%v account_id_len=%d token_len=%d excluded=%v",
			redactAuthID(auth), auth.Provider, auth.Disabled, len(accountID), tokenLen, codexweekly.IsAutomationExcluded(auth))
		limitReached, err := a.checkHourlyLimit(ctx, auth)
		if err != nil {
			log.WithError(err).Warnf("codex hourly automation: check failed for auth %s", redactAuthID(auth))
			continue
		}
		checkedCount++
		log.Infof("codex hourly automation: check result auth=%s limit_reached=%v", redactAuthID(auth), limitReached)
		if limitReached {
			reachedCount++
			a.autoDisable(ctx, auth)
			continue
		}
		reenabledCount++
		a.autoReenable(ctx, auth)
	}
	log.Infof("codex hourly automation: RunOnce complete provider_counts=%v codex=%d managed=%d excluded=%d checked=%d reached=%d reenabled_path=%d",
		providerCounts, codexCount, managedCount, excludedCount, checkedCount, reachedCount, reenabledCount)

	a.mu.Lock()
	a.lastCheckedAt = a.now()
	a.mu.Unlock()
}

func (a *Automation) checkHourlyLimit(ctx context.Context, auth *coreauth.Auth) (bool, error) {
	accountID := resolveAccountID(auth)
	if accountID == "" {
		log.Warnf("codex hourly automation: empty account_id for auth %s (skipping silently)", redactAuthID(auth))
		return false, nil
	}

	headers := make(http.Header)
	headers.Set("Chatgpt-Account-Id", accountID)
	headers.Set("Content-Type", "application/json")
	headers.Set("User-Agent", codexUsageUserAgent)
	req, err := a.manager.NewHttpRequest(ctx, auth, http.MethodGet, usageURL, nil, headers)
	if err != nil {
		log.WithError(err).Warnf("codex hourly automation: NewHttpRequest failed for %s", redactAuthID(auth))
		return false, err
	}
	log.Infof("codex hourly automation: sending wham/usage for %s (account_id=%s) Authorization_set=%v",
		redactAuthID(auth), accountID, req.Header.Get("Authorization") != "")
	resp, err := a.manager.HttpRequest(ctx, auth, req)
	if err != nil {
		log.WithError(err).Warnf("codex hourly automation: HttpRequest failed for %s", redactAuthID(auth))
		return false, err
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.WithError(errClose).Debug("codex hourly automation: failed to close usage response body")
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	log.Infof("codex hourly automation: wham/usage response auth=%s status=%d body_preview=%s",
		redactAuthID(auth), resp.StatusCode, truncateForLog(body, 300))
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return false, &coreauth.Error{Code: "usage_request_failed", Message: strings.TrimSpace(string(body))}
	}

	reached := hourlyLimitReached(body)
	log.Infof("codex hourly automation: hourlyLimitReached=%v for auth=%s", reached, redactAuthID(auth))
	return reached, nil
}

func truncateForLog(b []byte, n int) string {
	s := string(b)
	if len(s) > n {
		return s[:n] + "...(truncated)"
	}
	return s
}

// autoDisable 禁用 5h 命中的 auth,写入 codex_hourly_auto_disabled_at 审计时间戳。
func (a *Automation) autoDisable(ctx context.Context, auth *coreauth.Auth) {
	if auth == nil {
		return
	}
	if codexweekly.IsAutomationExcluded(auth) {
		return
	}
	if auth.Disabled {
		return
	}

	next := auth.Clone()
	next.Disabled = true
	next.Status = coreauth.StatusDisabled
	next.StatusMessage = StatusMessageAutoDisabled
	if next.Metadata == nil {
		next.Metadata = make(map[string]any)
	}
	next.Metadata[AutoDisabledAtMetadataKey] = a.now().UTC().Format(time.RFC3339)
	next.UpdatedAt = a.now()

	if _, err := a.manager.Update(ctx, next); err != nil {
		log.WithError(err).Warnf("codex hourly automation: failed to disable auth %s", redactAuthID(auth))
	}
}

// autoReenable 只启用**本 automation 自己禁用**的账号 (通过 codex_hourly_auto_disabled_at 识别),
// 避免在 weekly/hourly 共存场景下错误启用其他来源禁用的 auth。
// 前置门禁: excluded=true 跳过;auth 未禁用跳过;不含自有 auto_disabled_at 标记跳过。
func (a *Automation) autoReenable(ctx context.Context, auth *coreauth.Auth) {
	if auth == nil {
		return
	}
	if codexweekly.IsAutomationExcluded(auth) {
		return
	}
	if !auth.Disabled {
		return
	}
	if !hasSelfDisabledMarker(auth) {
		return
	}

	next := auth.Clone()
	next.Disabled = false
	next.Status = coreauth.StatusActive
	next.StatusMessage = ""
	if next.Metadata != nil {
		delete(next.Metadata, AutoDisabledAtMetadataKey)
	}
	next.UpdatedAt = a.now()

	if _, err := a.manager.Update(ctx, next); err != nil {
		log.WithError(err).Warnf("codex hourly automation: failed to re-enable auth %s", redactAuthID(auth))
	}
}

func hasSelfDisabledMarker(auth *coreauth.Auth) bool {
	if auth == nil || auth.Metadata == nil {
		return false
	}
	_, ok := auth.Metadata[AutoDisabledAtMetadataKey]
	return ok
}

func (a *Automation) currentConfig() *internalconfig.Config {
	if a == nil || a.getConfig == nil {
		return nil
	}
	return a.getConfig()
}

func (a *Automation) lastChecked() time.Time {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.lastCheckedAt
}

func (a *Automation) listAuths() []*coreauth.Auth {
	if a == nil || a.manager == nil {
		return nil
	}
	return a.manager.List()
}

func shouldManageAuth(auth *coreauth.Auth) bool {
	if auth == nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return false
	}
	if strings.TrimSpace(auth.FileName) == "" {
		return false
	}
	if auth.Attributes != nil && strings.EqualFold(strings.TrimSpace(auth.Attributes["runtime_only"]), "true") {
		return false
	}
	return true
}

func resolveAccountID(auth *coreauth.Auth) string {
	if auth == nil || auth.Metadata == nil {
		return ""
	}
	if accountID, ok := auth.Metadata["account_id"].(string); ok && strings.TrimSpace(accountID) != "" {
		return strings.TrimSpace(accountID)
	}
	if idToken, ok := auth.Metadata["id_token"].(string); ok && strings.TrimSpace(idToken) != "" {
		if claims, err := codexauth.ParseJWTToken(idToken); err == nil && claims != nil {
			return strings.TrimSpace(claims.CodexAuthInfo.ChatgptAccountID)
		}
	}
	return ""
}

// hourlyLimitReached 解析 wham/usage 响应,当 primary_window (5h) 任一 rate_limit 命中时返回 true。
func hourlyLimitReached(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	if rateLimitHourlyReached(gjson.GetBytes(body, "rate_limit")) {
		return true
	}
	if rateLimitHourlyReached(gjson.GetBytes(body, "code_review_rate_limit")) {
		return true
	}
	for _, item := range gjson.GetBytes(body, "additional_rate_limits").Array() {
		if rateLimitHourlyReached(item.Get("rate_limit")) {
			return true
		}
	}
	return false
}

// rateLimitHourlyReached 判定单个 rate_limit 节点是否命中 5h 窗口。
// 语义镜像 codexweekly.rateLimitWeeklyReached,但窗口时长匹配 hourlyWindowSeconds。
// 5h 一般落在 primary_window,但也检测 secondary_window 做冗余保护。
func rateLimitHourlyReached(limitInfo gjson.Result) bool {
	if !limitInfo.Exists() {
		return false
	}
	limitReached := limitInfo.Get("limit_reached").Bool() || limitInfo.Get("limitReached").Bool()
	allowed := true
	allowedKnown := false
	if result := limitInfo.Get("allowed"); result.Exists() {
		allowed = result.Bool()
		allowedKnown = true
	}

	primary := limitInfo.Get("primary_window")
	if !primary.Exists() {
		primary = limitInfo.Get("primaryWindow")
	}
	secondary := limitInfo.Get("secondary_window")
	if !secondary.Exists() {
		secondary = limitInfo.Get("secondaryWindow")
	}

	if windowIsHourly(primary) || windowIsHourly(secondary) {
		return limitReached || (allowedKnown && !allowed)
	}
	return false
}

func windowIsHourly(window gjson.Result) bool {
	if !window.Exists() {
		return false
	}
	return windowDurationSeconds(window) == hourlyWindowSeconds
}

func windowDurationSeconds(window gjson.Result) int64 {
	if !window.Exists() {
		return 0
	}
	if result := window.Get("limit_window_seconds"); result.Exists() {
		return result.Int()
	}
	if result := window.Get("limitWindowSeconds"); result.Exists() {
		return result.Int()
	}
	return 0
}

func automationInterval(cfg *internalconfig.Config) time.Duration {
	if cfg == nil {
		return defaultInterval
	}
	seconds := cfg.CodexHourlyAutomation.IntervalSeconds
	if seconds <= 0 {
		return defaultInterval
	}
	return time.Duration(seconds) * time.Second
}

// countAutoDisabledAuths 统计当前处于 "被 hourly automation 自动禁用" 状态的账号数。
// 通过 AutoDisabledAtMetadataKey (codex_hourly_auto_disabled_at) 审计时间戳识别。
func countAutoDisabledAuths(auths []*coreauth.Auth) int {
	count := 0
	for _, auth := range auths {
		if auth == nil || !shouldManageAuth(auth) {
			continue
		}
		if !auth.Disabled || auth.Metadata == nil {
			continue
		}
		if _, ok := auth.Metadata[AutoDisabledAtMetadataKey]; ok {
			count++
		}
	}
	return count
}

func redactAuthID(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	if auth.FileName != "" {
		return auth.FileName
	}
	return auth.ID
}
