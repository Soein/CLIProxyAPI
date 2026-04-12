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
	AutoDisabledMetadataKey         = "codex_weekly_auto_disabled"
	AutoDisabledAtMetadataKey       = "codex_weekly_auto_disabled_at"
	AutoDisabledReasonMetadataKey   = "codex_weekly_auto_reason"
	AutomationExcludedMetadataKey   = "codex_weekly_automation_excluded"
	AutomationExcludedAtMetadataKey = "codex_weekly_automation_excluded_at"
	StatusMessageAutoDisabled       = "disabled via codex weekly automation"
	defaultInterval                 = 5 * time.Minute
	pollTick                        = 1 * time.Second
	weeklyWindowSeconds             = 604800
	usageURL                        = "https://chatgpt.com/backend-api/wham/usage"
	codexUsageUserAgent             = "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal"
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
		log.Infof("codex weekly automation: managed auth id=%s provider=%s disabled=%v account_id_len=%d token_len=%d excluded=%v has_marker=%v",
			redactAuthID(auth), auth.Provider, auth.Disabled, len(accountID), tokenLen, IsAutomationExcluded(auth), HasAutoDisabledMarker(auth))
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

func (a *Automation) autoDisable(ctx context.Context, auth *coreauth.Auth) {
	if auth == nil {
		return
	}
	if IsAutomationExcluded(auth) {
		return
	}
	if auth.Disabled && HasAutoDisabledMarker(auth) {
		return
	}
	if auth.Disabled && !HasAutoDisabledMarker(auth) {
		return
	}

	next := auth.Clone()
	next.Disabled = true
	next.Status = coreauth.StatusDisabled
	next.StatusMessage = StatusMessageAutoDisabled
	if next.Metadata == nil {
		next.Metadata = make(map[string]any)
	}
	next.Metadata[AutoDisabledMetadataKey] = true
	next.Metadata[AutoDisabledAtMetadataKey] = a.now().UTC().Format(time.RFC3339)
	next.Metadata[AutoDisabledReasonMetadataKey] = "weekly_limit"
	next.UpdatedAt = a.now()

	if _, err := a.manager.Update(ctx, next); err != nil {
		log.WithError(err).Warnf("codex weekly automation: failed to disable auth %s", redactAuthID(auth))
	}
}

func (a *Automation) autoReenable(ctx context.Context, auth *coreauth.Auth) {
	if auth == nil || !HasAutoDisabledMarker(auth) {
		return
	}
	if IsAutomationExcluded(auth) {
		return
	}
	if !auth.Disabled {
		next := auth.Clone()
		ClearAutoDisabledMetadata(next)
		next.UpdatedAt = a.now()
		if _, err := a.manager.Update(ctx, next); err != nil {
			log.WithError(err).Warnf("codex weekly automation: failed to clear stale auto marker for auth %s", redactAuthID(auth))
		}
		return
	}
	if auth.StatusMessage != StatusMessageAutoDisabled {
		return
	}

	next := auth.Clone()
	next.Disabled = false
	next.Status = coreauth.StatusActive
	next.StatusMessage = ""
	ClearAutoDisabledMetadata(next)
	next.UpdatedAt = a.now()

	if _, err := a.manager.Update(ctx, next); err != nil {
		log.WithError(err).Warnf("codex weekly automation: failed to re-enable auth %s", redactAuthID(auth))
	}
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

func HasAutoDisabledMarker(auth *coreauth.Auth) bool {
	if auth == nil || auth.Metadata == nil {
		return false
	}
	raw, ok := auth.Metadata[AutoDisabledMetadataKey]
	if !ok {
		return false
	}
	switch v := raw.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "true")
	default:
		return false
	}
}

func ClearAutoDisabledMetadata(auth *coreauth.Auth) {
	if auth == nil || auth.Metadata == nil {
		return
	}
	delete(auth.Metadata, AutoDisabledMetadataKey)
	delete(auth.Metadata, AutoDisabledAtMetadataKey)
	delete(auth.Metadata, AutoDisabledReasonMetadataKey)
}

func IsAutomationExcluded(auth *coreauth.Auth) bool {
	if auth == nil || auth.Metadata == nil {
		return false
	}
	raw, ok := auth.Metadata[AutomationExcludedMetadataKey]
	if !ok {
		return false
	}
	switch v := raw.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "true")
	default:
		return false
	}
}

func SetAutomationExcluded(auth *coreauth.Auth, excluded bool, now time.Time) {
	if auth == nil {
		return
	}
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	if !excluded {
		delete(auth.Metadata, AutomationExcludedMetadataKey)
		delete(auth.Metadata, AutomationExcludedAtMetadataKey)
		return
	}
	auth.Metadata[AutomationExcludedMetadataKey] = true
	auth.Metadata[AutomationExcludedAtMetadataKey] = now.UTC().Format(time.RFC3339)
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

	if windowIsWeekly(primary) || windowIsWeekly(secondary) {
		return limitReached || (allowedKnown && !allowed)
	}

	// Legacy fallback: if the duration is absent, treat the secondary window as weekly.
	if secondary.Exists() && !windowHasDuration(primary) && !windowHasDuration(secondary) {
		return limitReached || (allowedKnown && !allowed)
	}
	return false
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

func countAutoDisabledAuths(auths []*coreauth.Auth) int {
	count := 0
	for _, auth := range auths {
		if auth == nil || !shouldManageAuth(auth) {
			continue
		}
		if auth.Disabled && HasAutoDisabledMarker(auth) {
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
