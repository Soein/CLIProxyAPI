package codexweekly

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

const (
	// AutoDisabledAtMetadataKey 记录 weekly automation 最近一次自动禁用该 auth 的时间戳 (审计用)。
	AutoDisabledAtMetadataKey = "codex_weekly_auto_disabled_at"
	// AutomationExcludedMetadataKey 是新版统一排除字段,同时控制 weekly + hourly automation。
	AutomationExcludedMetadataKey = "codex_automation_excluded"
	// AutomationExcludedAtMetadataKey 是新版排除时间戳。
	AutomationExcludedAtMetadataKey = "codex_automation_excluded_at"
	// LegacyWeeklyAutomationExcludedKey / LegacyWeeklyAutomationExcludedAtKey 是旧版
	// weekly 专用排除字段,保留用于向后兼容读写与同步清理。
	LegacyWeeklyAutomationExcludedKey   = "codex_weekly_automation_excluded"
	LegacyWeeklyAutomationExcludedAtKey = "codex_weekly_automation_excluded_at"
	StatusMessageAutoDisabled           = "disabled via codex weekly automation"
	defaultInterval                     = 5 * time.Minute
	pollTick                            = 1 * time.Second
	weeklyWindowSeconds                 = 604800
	usageURL                            = "https://chatgpt.com/backend-api/wham/usage"
	codexUsageUserAgent                 = "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal"

	// legacyAutoDisabledMarkerKey / legacyAutoReasonKey 是旧版 marker 字段,
	// 仅在 autoReenable 路径做兼容清理,不再参与决策。
	legacyAutoDisabledMarkerKey = "codex_weekly_auto_disabled"
	legacyAutoReasonKey         = "codex_weekly_auto_reason"
)

type authManager interface {
	List() []*coreauth.Auth
	Update(context.Context, *coreauth.Auth) (*coreauth.Auth, error)
	NewHttpRequest(context.Context, *coreauth.Auth, string, string, []byte, http.Header) (*http.Request, error)
	HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error)
}

type Status struct {
	Enabled           bool       `json:"enabled"`
	Running           bool       `json:"running"`
	LastCheckedAt     *time.Time `json:"last_checked_at"`
	AutoDisabledCount int        `json:"auto_disabled_count"`
}

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

func NewAutomation(manager authManager, getConfig func() *internalconfig.Config) *Automation {
	return &Automation{
		manager:   manager,
		getConfig: getConfig,
		now:       time.Now,
	}
}

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
				if cfg == nil || !cfg.CodexWeeklyAutomation.Enabled {
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

func (a *Automation) Status() Status {
	if a == nil {
		return Status{}
	}
	cfg := a.currentConfig()
	enabled := cfg != nil && cfg.CodexWeeklyAutomation.Enabled

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
	log.Infof("codex weekly automation: RunOnce started, total auths from manager=%d", len(auths))
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
		if IsAutomationExcluded(auth) {
			excludedCount++
		}
		accountID := resolveAccountID(auth)
		tokenLen := 0
		if auth.Metadata != nil {
			if v, ok := auth.Metadata["access_token"].(string); ok {
				tokenLen = len(v)
			}
		}
		log.Infof("codex weekly automation: managed auth id=%s provider=%s disabled=%v account_id_len=%d token_len=%d excluded=%v",
			redactAuthID(auth), auth.Provider, auth.Disabled, len(accountID), tokenLen, IsAutomationExcluded(auth))
		limitReached, err := a.checkWeeklyLimit(ctx, auth)
		if err != nil {
			log.WithError(err).Warnf("codex weekly automation: check failed for auth %s", redactAuthID(auth))
			continue
		}
		checkedCount++
		log.Infof("codex weekly automation: check result auth=%s limit_reached=%v", redactAuthID(auth), limitReached)
		if limitReached {
			reachedCount++
			a.autoDisable(ctx, auth)
			continue
		}
		reenabledCount++
		a.autoReenable(ctx, auth)
	}
	log.Infof("codex weekly automation: RunOnce complete provider_counts=%v codex=%d managed=%d excluded=%d checked=%d reached=%d reenabled_path=%d",
		providerCounts, codexCount, managedCount, excludedCount, checkedCount, reachedCount, reenabledCount)

	a.mu.Lock()
	a.lastCheckedAt = a.now()
	a.mu.Unlock()
}

func (a *Automation) checkWeeklyLimit(ctx context.Context, auth *coreauth.Auth) (bool, error) {
	accountID := resolveAccountID(auth)
	if accountID == "" {
		log.Warnf("codex weekly automation: empty account_id for auth %s (skipping silently)", redactAuthID(auth))
		return false, nil
	}

	headers := make(http.Header)
	headers.Set("Chatgpt-Account-Id", accountID)
	headers.Set("Content-Type", "application/json")
	headers.Set("User-Agent", codexUsageUserAgent)
	req, err := a.manager.NewHttpRequest(ctx, auth, http.MethodGet, usageURL, nil, headers)
	if err != nil {
		log.WithError(err).Warnf("codex weekly automation: NewHttpRequest failed for %s", redactAuthID(auth))
		return false, err
	}
	log.Infof("codex weekly automation: sending wham/usage for %s (account_id=%s) Authorization_set=%v",
		redactAuthID(auth), accountID, req.Header.Get("Authorization") != "")
	resp, err := a.manager.HttpRequest(ctx, auth, req)
	if err != nil {
		log.WithError(err).Warnf("codex weekly automation: HttpRequest failed for %s", redactAuthID(auth))
		return false, err
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.WithError(errClose).Debug("codex weekly automation: failed to close usage response body")
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	log.Infof("codex weekly automation: wham/usage response auth=%s status=%d body_preview=%s",
		redactAuthID(auth), resp.StatusCode, truncateForLog(body, 300))
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return false, &coreauth.Error{Code: "usage_request_failed", Message: strings.TrimSpace(string(body))}
	}

	reached := weeklyLimitReached(body)
	log.Infof("codex weekly automation: weeklyLimitReached=%v for auth=%s", reached, redactAuthID(auth))
	return reached, nil
}

func truncateForLog(b []byte, n int) string {
	s := string(b)
	if len(s) > n {
		return s[:n] + "...(truncated)"
	}
	return s
}

// autoDisable 在账号当前启用且未被用户排除在自动化外时,禁用该账号并写入审计时间戳。
// 单门禁模型: excluded 是唯一的 "不要碰" 信号。
func (a *Automation) autoDisable(ctx context.Context, auth *coreauth.Auth) {
	if auth == nil {
		return
	}
	if IsAutomationExcluded(auth) {
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
		log.WithError(err).Warnf("codex weekly automation: failed to disable auth %s", redactAuthID(auth))
	}
}

// autoReenable 只启用**本 automation 自己禁用**的账号 (通过 AutoDisabledAtMetadataKey 识别),
// 避免在 weekly/hourly 共存场景下,weekly 错误启用被 hourly 禁用的账号,或错误启用用户手动禁用的账号。
// 前置门禁: excluded=true 跳过;auth 未禁用跳过;不含自有 auto_disabled_at 标记跳过。
func (a *Automation) autoReenable(ctx context.Context, auth *coreauth.Auth) {
	if auth == nil {
		return
	}
	if IsAutomationExcluded(auth) {
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
		delete(next.Metadata, legacyAutoDisabledMarkerKey)
		delete(next.Metadata, legacyAutoReasonKey)
	}
	next.UpdatedAt = a.now()

	if _, err := a.manager.Update(ctx, next); err != nil {
		log.WithError(err).Warnf("codex weekly automation: failed to re-enable auth %s", redactAuthID(auth))
	}
}

// hasSelfDisabledMarker 判断 auth 是否含有 weekly automation 自身的"自动禁用"审计标记。
// 兼容旧版 boolean marker codex_weekly_auto_disabled: true。
func hasSelfDisabledMarker(auth *coreauth.Auth) bool {
	if auth == nil || auth.Metadata == nil {
		return false
	}
	if _, ok := auth.Metadata[AutoDisabledAtMetadataKey]; ok {
		return true
	}
	if v, ok := auth.Metadata[legacyAutoDisabledMarkerKey]; ok {
		switch tv := v.(type) {
		case bool:
			return tv
		case string:
			return strings.EqualFold(strings.TrimSpace(tv), "true")
		}
	}
	return false
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

// IsAutomationExcluded 读取"排除出 codex 自动化"标记,同时作用于 weekly + hourly。
// 读取顺序:优先新字段 codex_automation_excluded,回落旧字段 codex_weekly_automation_excluded。
func IsAutomationExcluded(auth *coreauth.Auth) bool {
	if auth == nil || auth.Metadata == nil {
		return false
	}
	if v, ok := readBoolish(auth.Metadata[AutomationExcludedMetadataKey]); ok {
		return v
	}
	if v, ok := readBoolish(auth.Metadata[LegacyWeeklyAutomationExcludedKey]); ok {
		return v
	}
	return false
}

// SetAutomationExcluded 写入"排除出 codex 自动化"标记,统一写入新字段并同步清理旧字段,
// 避免两份值漂移。excluded=false 时同时清除新旧字段。
func SetAutomationExcluded(auth *coreauth.Auth, excluded bool, now time.Time) {
	if auth == nil {
		return
	}
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	// 始终清理旧字段,避免新/旧值不一致。
	delete(auth.Metadata, LegacyWeeklyAutomationExcludedKey)
	delete(auth.Metadata, LegacyWeeklyAutomationExcludedAtKey)
	if !excluded {
		delete(auth.Metadata, AutomationExcludedMetadataKey)
		delete(auth.Metadata, AutomationExcludedAtMetadataKey)
		return
	}
	auth.Metadata[AutomationExcludedMetadataKey] = true
	auth.Metadata[AutomationExcludedAtMetadataKey] = now.UTC().Format(time.RFC3339)
}

// readBoolish 把 metadata 里的 bool/string 值解析成 bool。第二个返回值表示该键是否存在。
func readBoolish(raw any) (bool, bool) {
	if raw == nil {
		return false, false
	}
	switch v := raw.(type) {
	case bool:
		return v, true
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return false, false
		}
		return strings.EqualFold(trimmed, "true"), true
	default:
		return false, false
	}
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

func weeklyLimitReached(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	if rateLimitWeeklyReached(gjson.GetBytes(body, "rate_limit")) {
		return true
	}
	if rateLimitWeeklyReached(gjson.GetBytes(body, "code_review_rate_limit")) {
		return true
	}

	for _, item := range gjson.GetBytes(body, "additional_rate_limits").Array() {
		if rateLimitWeeklyReached(item.Get("rate_limit")) {
			return true
		}
	}
	return false
}

func rateLimitWeeklyReached(limitInfo gjson.Result) bool {
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

	// 优先读取目标窗口(weekly)自身的命中信号,避免 5h 命中顺带把 weekly 触发。
	if windowIsWeekly(primary) {
		if reached, ok := WindowReached(primary); ok {
			return reached
		}
	}
	if windowIsWeekly(secondary) {
		if reached, ok := WindowReached(secondary); ok {
			return reached
		}
	}

	// 仅当 weekly 目标窗口无明确信号,且不存在不同时长的对手窗口(hourly)时,
	// 才沿用顶层 limit_reached - 避免歧义:当 5h+周两个窗口共存时无法从顶层推断是谁触发。
	hasWeeklyTarget := windowIsWeekly(primary) || windowIsWeekly(secondary)
	hasPeerWindow := hasPeerNonWeekly(primary, secondary)
	if hasWeeklyTarget && !hasPeerWindow {
		return limitReached || (allowedKnown && !allowed)
	}

	// Legacy fallback: 两个窗口都没有时长信息时,沿用顶层 limit_reached。
	if secondary.Exists() && !windowHasDuration(primary) && !windowHasDuration(secondary) {
		return limitReached || (allowedKnown && !allowed)
	}
	return false
}

// hasPeerNonWeekly 判断两个窗口是否构成"周 + 非周"的共存组合,
// 此时顶层 limit_reached 可能由任一窗口触发,读顶层会产生歧义。
func hasPeerNonWeekly(primary, secondary gjson.Result) bool {
	if windowIsWeekly(primary) && windowHasDuration(secondary) && !windowIsWeekly(secondary) {
		return true
	}
	if windowIsWeekly(secondary) && windowHasDuration(primary) && !windowIsWeekly(primary) {
		return true
	}
	return false
}

// WindowReached 读取单个 rate_limit window 节点的命中信号,
// 供 weekly/hourly 两套 automation 共享使用。ok=false 表示节点未提供任何可判定信号。
// 判定优先级:
//  1. limit_reached / limitReached 布尔字段
//  2. allowed=false (仅当字段存在)
//  3. used_percent / usedPercent >= 100
func WindowReached(window gjson.Result) (reached bool, ok bool) {
	if !window.Exists() {
		return false, false
	}
	if v := window.Get("limit_reached"); v.Exists() {
		return v.Bool(), true
	}
	if v := window.Get("limitReached"); v.Exists() {
		return v.Bool(), true
	}
	if v := window.Get("allowed"); v.Exists() {
		return !v.Bool(), true
	}
	if v := window.Get("used_percent"); v.Exists() {
		return v.Float() >= 100, true
	}
	if v := window.Get("usedPercent"); v.Exists() {
		return v.Float() >= 100, true
	}
	return false, false
}

func windowIsWeekly(window gjson.Result) bool {
	if !window.Exists() {
		return false
	}
	return windowDurationSeconds(window) == weeklyWindowSeconds
}

func windowHasDuration(window gjson.Result) bool {
	if !window.Exists() {
		return false
	}
	return window.Get("limit_window_seconds").Exists() || window.Get("limitWindowSeconds").Exists()
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
	seconds := cfg.CodexWeeklyAutomation.IntervalSeconds
	if seconds <= 0 {
		return defaultInterval
	}
	return time.Duration(seconds) * time.Second
}

// countAutoDisabledAuths 统计当前处于 "被 automation 自动禁用" 状态的账号数。
// 通过 AutoDisabledAtMetadataKey 审计时间戳识别 (该字段持久化到磁盘,
// 容器重启后依然可见;而 status_message 不随 auth 文件落盘)。
// 兼容旧版 marker codex_weekly_auto_disabled: true。
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
			continue
		}
		// 兼容旧版 boolean marker
		if v, ok := auth.Metadata[legacyAutoDisabledMarkerKey]; ok {
			switch tv := v.(type) {
			case bool:
				if tv {
					count++
				}
			case string:
				if strings.EqualFold(strings.TrimSpace(tv), "true") {
					count++
				}
			}
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
