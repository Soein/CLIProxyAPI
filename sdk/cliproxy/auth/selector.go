package auth

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/credentialweight"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	cliproxysession "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/session"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

// RoundRobinSelector provides a simple provider scoped round-robin selection strategy.
//
// Rotation continues from the identity of the previous pick rather than from a numeric
// index. Candidate slices shrink whenever a retry excludes already tried credentials or a
// credential enters cooldown, and indexing a monotonic counter into a shrinking slice
// silently re-seats the rotation, which starves some credentials and hammers others.
type RoundRobinSelector struct {
	mu         sync.Mutex
	lastPicked map[string]string
	maxKeys    int
}

// WeightedRoundRobinSelector provides smooth weighted round-robin selection.
type WeightedRoundRobinSelector struct {
	mu      sync.Mutex
	states  map[string]*smoothWeightedState
	maxKeys int
}

type smoothWeightedState struct {
	current map[string]int64
	weights map[string]int64
}

type weightedSelectorStateModelKey struct{}

func withWeightedSelectorStateModel(ctx context.Context, selector Selector, routeModel string) context.Context {
	if _, ok := selector.(*WeightedRoundRobinSelector); !ok || strings.TrimSpace(routeModel) == "" {
		return ctx
	}
	return context.WithValue(ctx, weightedSelectorStateModelKey{}, routeModel)
}

func weightedSelectorStateModel(ctx context.Context, availabilityModel string) string {
	if ctx != nil {
		if routeModel, ok := ctx.Value(weightedSelectorStateModelKey{}).(string); ok && strings.TrimSpace(routeModel) != "" {
			return routeModel
		}
	}
	return availabilityModel
}

// FillFirstSelector selects the first available credential (deterministic ordering).
// This "burns" one account before moving to the next, which can help stagger
// rolling-window subscription caps (e.g. chat message limits).
type FillFirstSelector struct{}

type blockReason int

const (
	blockReasonNone blockReason = iota
	blockReasonCooldown
	blockReasonDisabled
	blockReasonOther
)

type modelCooldownError struct {
	model    string
	resetIn  time.Duration
	provider string
}

func newModelCooldownError(model, provider string, resetIn time.Duration) *modelCooldownError {
	if resetIn < 0 {
		resetIn = 0
	}
	return &modelCooldownError{
		model:    model,
		provider: provider,
		resetIn:  resetIn,
	}
}

func (e *modelCooldownError) Error() string {
	modelName := e.model
	if modelName == "" {
		modelName = "requested model"
	}
	message := fmt.Sprintf("All credentials for model %s are cooling down", modelName)
	if e.provider != "" {
		message = fmt.Sprintf("%s via provider %s", message, e.provider)
	}
	resetSeconds := int(math.Ceil(e.resetIn.Seconds()))
	if resetSeconds < 0 {
		resetSeconds = 0
	}
	displayDuration := e.resetIn
	if displayDuration > 0 && displayDuration < time.Second {
		displayDuration = time.Second
	} else {
		displayDuration = displayDuration.Round(time.Second)
	}
	errorBody := map[string]any{
		"code":          "model_cooldown",
		"message":       message,
		"model":         e.model,
		"reset_time":    displayDuration.String(),
		"reset_seconds": resetSeconds,
	}
	if e.provider != "" {
		errorBody["provider"] = e.provider
	}
	payload := map[string]any{"error": errorBody}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Sprintf(`{"error":{"code":"model_cooldown","message":"%s"}}`, message)
	}
	return string(data)
}

func (e *modelCooldownError) StatusCode() int {
	return http.StatusTooManyRequests
}

func (e *modelCooldownError) Headers() http.Header {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	resetSeconds := int(math.Ceil(e.resetIn.Seconds()))
	if resetSeconds < 0 {
		resetSeconds = 0
	}
	headers.Set("Retry-After", strconv.Itoa(resetSeconds))
	return headers
}

func authPriority(auth *Auth) int {
	if auth == nil || auth.Attributes == nil {
		return 0
	}
	raw := strings.TrimSpace(auth.Attributes["priority"])
	if raw == "" {
		return 0
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return parsed
}

func authWeight(auth *Auth) int64 {
	if auth == nil {
		return credentialweight.Default
	}
	if rawWeight, ok := auth.Attributes[AttributeWeight]; ok && strings.TrimSpace(rawWeight) != "" {
		weight, errParse := credentialweight.ParseString(rawWeight)
		if errParse != nil {
			return 0
		}
		return weight
	}
	if rawWeight, ok := auth.Metadata[AttributeWeight]; ok {
		weight, errParse := credentialweight.ParseValue(rawWeight)
		if errParse != nil {
			return 0
		}
		return weight
	}
	return credentialweight.Default
}

func canonicalModelKey(model string) string {
	model = strings.TrimSpace(model)
	if model == "" {
		return ""
	}
	parsed := thinking.ParseSuffix(model)
	modelName := strings.TrimSpace(parsed.ModelName)
	if modelName == "" {
		return model
	}
	return modelName
}

func authWebsocketsEnabled(auth *Auth) bool {
	if auth == nil {
		return false
	}
	if len(auth.Attributes) > 0 {
		if raw := strings.TrimSpace(auth.Attributes["websockets"]); raw != "" {
			parsed, errParse := strconv.ParseBool(raw)
			if errParse == nil {
				return parsed
			}
		}
	}
	if len(auth.Metadata) == 0 {
		return false
	}
	raw, ok := auth.Metadata["websockets"]
	if !ok || raw == nil {
		return false
	}
	switch v := raw.(type) {
	case bool:
		return v
	case string:
		parsed, errParse := strconv.ParseBool(strings.TrimSpace(v))
		if errParse == nil {
			return parsed
		}
	default:
	}
	return false
}

func preferWebsocketAuths(ctx context.Context, provider string, available []*Auth) []*Auth {
	if len(available) == 0 {
		return available
	}
	if !cliproxyexecutor.DownstreamWebsocket(ctx) {
		return available
	}
	if !providerPrefersWebsocketTransport(provider) {
		return available
	}

	wsEnabled := make([]*Auth, 0, len(available))
	for i := 0; i < len(available); i++ {
		candidate := available[i]
		if authWebsocketsEnabled(candidate) {
			wsEnabled = append(wsEnabled, candidate)
		}
	}
	if len(wsEnabled) > 0 {
		return wsEnabled
	}
	return available
}

func collectAvailableByPriority(auths []*Auth, model string, now time.Time) (available map[int][]*Auth, cooldownCount int, earliest time.Time) {
	available = make(map[int][]*Auth)
	for i := 0; i < len(auths); i++ {
		candidate := auths[i]
		blocked, reason, next := isAuthBlockedForModel(candidate, model, now)
		if !blocked {
			priority := authPriority(candidate)
			available[priority] = append(available[priority], candidate)
			continue
		}
		if reason == blockReasonCooldown {
			cooldownCount++
			if !next.IsZero() && (earliest.IsZero() || next.Before(earliest)) {
				earliest = next
			}
		}
	}
	return available, cooldownCount, earliest
}

func getAvailableAuths(auths []*Auth, provider, model string, now time.Time) ([]*Auth, error) {
	return getAvailableAuthsWithPriorityMode(auths, provider, model, now, false)
}

func getAvailableAuthsAcrossPriorities(auths []*Auth, provider, model string, now time.Time) ([]*Auth, error) {
	return getAvailableAuthsWithPriorityMode(auths, provider, model, now, true)
}

func getAvailableAuthsWithPriorityMode(auths []*Auth, provider, model string, now time.Time, allPriorities bool) ([]*Auth, error) {
	if len(auths) == 0 {
		return nil, &Error{Code: "auth_not_found", Message: "no auth candidates"}
	}

	availableByPriority, cooldownCount, earliest := collectAvailableByPriority(auths, model, now)
	if len(availableByPriority) == 0 {
		if cooldownCount == len(auths) && !earliest.IsZero() {
			providerForError := provider
			if providerForError == "mixed" {
				providerForError = ""
			}
			resetIn := earliest.Sub(now)
			if resetIn < 0 {
				resetIn = 0
			}
			return nil, newModelCooldownError(model, providerForError, resetIn)
		}
		return nil, &Error{Code: "auth_unavailable", Message: "no auth available"}
	}

	return availableAuthsFromPriorityBuckets(availableByPriority, allPriorities), nil
}

// availableAuthsFromPriorityBuckets flattens availability buckets into a stable, ID-sorted slice.
// When allPriorities is false only the highest available priority tier is returned.
// When allPriorities is true every tier is merged, so the result carries no priority ordering:
// use it for membership checks or feed it to highestPriorityAuths, never as a priority-ordered
// selection order.
func availableAuthsFromPriorityBuckets(availableByPriority map[int][]*Auth, allPriorities bool) []*Auth {
	var candidates []*Auth
	if allPriorities {
		total := 0
		for _, bucket := range availableByPriority {
			total += len(bucket)
		}
		candidates = make([]*Auth, 0, total)
		for _, bucket := range availableByPriority {
			candidates = append(candidates, bucket...)
		}
	} else {
		bestPriority := 0
		found := false
		for priority := range availableByPriority {
			if !found || priority > bestPriority {
				bestPriority = priority
				found = true
			}
		}
		bucket := availableByPriority[bestPriority]
		candidates = make([]*Auth, 0, len(bucket))
		candidates = append(candidates, bucket...)
	}
	if len(candidates) > 1 {
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].ID < candidates[j].ID })
	}
	return candidates
}

// highestPriorityAuths narrows an availability slice to its highest priority tier while
// preserving the input order. The input slice is returned unchanged when every candidate
// already shares the highest priority, so the common single-tier case allocates nothing.
func highestPriorityAuths(auths []*Auth) []*Auth {
	if len(auths) <= 1 {
		return auths
	}
	bestPriority := 0
	bestCount := 0
	for _, auth := range auths {
		priority := authPriority(auth)
		switch {
		case bestCount == 0 || priority > bestPriority:
			bestPriority = priority
			bestCount = 1
		case priority == bestPriority:
			bestCount++
		}
	}
	if bestCount == len(auths) {
		return auths
	}
	highest := make([]*Auth, 0, bestCount)
	for _, auth := range auths {
		if authPriority(auth) == bestPriority {
			highest = append(highest, auth)
		}
	}
	return highest
}

// Pick selects the next available auth for the provider in a round-robin manner.
func (s *RoundRobinSelector) Pick(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	_ = opts
	now := time.Now()
	available, err := getAvailableAuths(auths, provider, model, now)
	if err != nil {
		return nil, err
	}
	available = preferWebsocketAuths(ctx, provider, available)
	key := provider + ":" + canonicalModelKey(model)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastPicked == nil {
		s.lastPicked = make(map[string]string)
	}
	limit := s.maxKeys
	if limit <= 0 {
		limit = 4096
	}

	s.ensureRotationKey(key, limit)
	picked := available[successorIndex(available, s.lastPicked[key])]
	s.lastPicked[key] = picked.ID
	return picked, nil
}

// successorIndex returns the index of the first candidate ordered after lastID, wrapping to
// the start of the ring. Candidates arrive sorted by ID, so this resumes the rotation at the
// credential that follows the previous pick even when candidates were filtered out in
// between. An empty lastID starts at the head.
func successorIndex(available []*Auth, lastID string) int {
	if lastID == "" {
		return 0
	}
	index := sort.Search(len(available), func(i int) bool { return available[i].ID > lastID })
	if index >= len(available) {
		return 0
	}
	return index
}

// ensureRotationKey ensures the rotation map has capacity for the given key.
// Must be called with s.mu held.
func (s *RoundRobinSelector) ensureRotationKey(key string, limit int) {
	if _, ok := s.lastPicked[key]; !ok && len(s.lastPicked) >= limit {
		s.lastPicked = make(map[string]string)
	}
}

func positiveWeightAuths(auths []*Auth) []*Auth {
	weightedCandidates := make([]*Auth, 0, len(auths))
	for _, auth := range auths {
		if authWeight(auth) > 0 {
			weightedCandidates = append(weightedCandidates, auth)
		}
	}
	return weightedCandidates
}

// Pick selects the next available auth using smooth weighted round-robin.
func (s *WeightedRoundRobinSelector) Pick(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	_ = opts
	available, errAvailable := getAvailableAuths(positiveWeightAuths(auths), provider, model, time.Now())
	if errAvailable != nil {
		return nil, errAvailable
	}
	available = preferWebsocketAuths(ctx, provider, available)
	stateModel := weightedSelectorStateModel(ctx, model)
	key := provider + ":" + canonicalModelKey(stateModel)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.states == nil {
		s.states = make(map[string]*smoothWeightedState)
	}
	limit := s.maxKeys
	if limit <= 0 {
		limit = 4096
	}
	if _, ok := s.states[key]; !ok && len(s.states) >= limit {
		s.states = make(map[string]*smoothWeightedState)
	}
	state := s.states[key]
	if state == nil {
		state = &smoothWeightedState{}
		s.states[key] = state
	}
	weights := authWeightVector(available)
	state.prepare(weights)
	picked := pickSmoothWeightedAuth(available, state.current)
	if picked == nil {
		return nil, &Error{Code: "auth_unavailable", Message: "no auth available with positive weight"}
	}
	return picked, nil
}

// maxSmoothWeightedStateEntries bounds a single accumulator map so credentials that are
// removed permanently cannot leak entries. Real pools stay far below this bound, so the
// transient subsets produced by retry exclusions and cooldowns are never pruned.
const maxSmoothWeightedStateEntries = 1024

// prepare syncs the configured weights into the state without discarding accumulated
// credits. Credits are reset only when a credential's configured weight actually changes,
// never when the candidate set shrinks temporarily (retry exclusions, cooldowns, session
// affinity), because discarding credits there would collapse selection onto the first
// candidate in slice order.
func (s *smoothWeightedState) prepare(weights map[string]int64) {
	if s.current == nil || weightsConfigChanged(s.weights, weights) {
		s.current = make(map[string]int64, len(weights))
	}
	if s.weights == nil {
		s.weights = make(map[string]int64, len(weights))
	}
	for authID, weight := range weights {
		s.weights[authID] = weight
	}
	s.pruneStale(weights)
}

// pruneStale drops entries for credentials outside the current candidate set, but only
// once a map exceeds the safety bound, so ordinary transient exclusions keep their credits.
func (s *smoothWeightedState) pruneStale(weights map[string]int64) {
	if len(s.current) <= maxSmoothWeightedStateEntries && len(s.weights) <= maxSmoothWeightedStateEntries {
		return
	}
	for authID := range s.current {
		if _, ok := weights[authID]; !ok {
			delete(s.current, authID)
		}
	}
	for authID := range s.weights {
		if _, ok := weights[authID]; !ok {
			delete(s.weights, authID)
		}
	}
}

// weightsConfigChanged reports whether any credential present in both vectors has a
// different configured weight. Credentials that are merely missing from one side are
// ignored, since a candidate subset is not a configuration change.
func weightsConfigChanged(left, right map[string]int64) bool {
	if len(left) == 0 {
		return false
	}
	for authID, weight := range right {
		if previous, ok := left[authID]; ok && previous != weight {
			return true
		}
	}
	return false
}

func authWeightVector(auths []*Auth) map[string]int64 {
	weights := make(map[string]int64, len(auths))
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		if weight := authWeight(auth); weight > 0 {
			weights[auth.ID] = weight
		}
	}
	return weights
}

func pickSmoothWeightedAuth(auths []*Auth, current map[string]int64) *Auth {
	var picked *Auth
	var pickedCurrent int64
	var totalWeight int64
	for _, auth := range auths {
		weight := authWeight(auth)
		if auth == nil || weight <= 0 {
			continue
		}
		current[auth.ID] = saturatingAddInt64(current[auth.ID], weight)
		totalWeight = saturatingAddInt64(totalWeight, weight)
		if picked == nil || current[auth.ID] > pickedCurrent {
			picked = auth
			pickedCurrent = current[auth.ID]
		}
	}
	if picked == nil {
		return nil
	}
	current[picked.ID] = saturatingAddInt64(current[picked.ID], -totalWeight)
	return picked
}

func saturatingAddInt64(value, delta int64) int64 {
	if delta > 0 && value > math.MaxInt64-delta {
		return math.MaxInt64
	}
	if delta < 0 && value < math.MinInt64-delta {
		return math.MinInt64
	}
	return value + delta
}

// Pick selects the first available auth for the provider in a deterministic manner.
func (s *FillFirstSelector) Pick(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	_ = opts
	now := time.Now()
	available, err := getAvailableAuths(auths, provider, model, now)
	if err != nil {
		return nil, err
	}
	available = preferWebsocketAuths(ctx, provider, available)
	return available[0], nil
}

func isAuthBlockedForModel(auth *Auth, model string, now time.Time) (bool, blockReason, time.Time) {
	if auth == nil {
		return true, blockReasonOther, time.Time{}
	}
	if auth.Disabled || auth.Status == StatusDisabled {
		return true, blockReasonDisabled, time.Time{}
	}
	if auth.Quota.Exceeded && auth.Quota.Reason == "credential_quota" && auth.Quota.NextRecoverAt.After(now) {
		return true, blockReasonCooldown, auth.Quota.NextRecoverAt
	}
	if model != "" {
		if len(auth.ModelStates) > 0 {
			modelKey := canonicalModelKey(model)
			matched := false
			blocked := false
			blockedReason := blockReasonNone
			nextRetry := time.Time{}
			for stateModel, state := range auth.ModelStates {
				if state == nil || canonicalModelKey(stateModel) != modelKey {
					continue
				}
				matched = true
				if state.Status == StatusDisabled {
					return true, blockReasonDisabled, time.Time{}
				}
				stateBlocked, reason, next := availabilityBlock(state.Unavailable, state.Quota.Exceeded, state.NextRetryAfter, state.Quota.NextRecoverAt, now)
				if !stateBlocked {
					continue
				}
				if next.IsZero() {
					return true, reason, time.Time{}
				}
				if !blocked || next.After(nextRetry) || (next.Equal(nextRetry) && reason == blockReasonCooldown) {
					blocked = true
					blockedReason = reason
					nextRetry = next
				}
			}
			if matched {
				return blocked, blockedReason, nextRetry
			}
			return false, blockReasonNone, time.Time{}
		}
		return availabilityBlock(auth.Unavailable, auth.Quota.Exceeded, auth.NextRetryAfter, auth.Quota.NextRecoverAt, now)
	}
	return availabilityBlock(auth.Unavailable, auth.Quota.Exceeded, auth.NextRetryAfter, auth.Quota.NextRecoverAt, now)
}

func availabilityBlock(unavailable, quotaExceeded bool, nextRetryAfter, nextRecoverAt, now time.Time) (bool, blockReason, time.Time) {
	if !unavailable && !quotaExceeded {
		return false, blockReasonNone, time.Time{}
	}

	hasRecoveryTime := !nextRetryAfter.IsZero() || !nextRecoverAt.IsZero()
	var next time.Time
	for _, candidate := range []time.Time{nextRetryAfter, nextRecoverAt} {
		if candidate.After(now) && (next.IsZero() || candidate.After(next)) {
			next = candidate
		}
	}
	if !next.IsZero() {
		if quotaExceeded {
			return true, blockReasonCooldown, next
		}
		return true, blockReasonOther, next
	}
	if hasRecoveryTime {
		return false, blockReasonNone, time.Time{}
	}
	return true, blockReasonOther, time.Time{}
}

// SessionAffinitySelector wraps another selector with session-sticky behavior.
// It extracts session ID from multiple sources and maintains session-to-auth
// mappings with automatic failover when the bound auth becomes unavailable.
type SessionAffinitySelector struct {
	fallback Selector
	cache    *SessionCache
	pickMu   [64]sync.Mutex
	hits     atomic.Uint64
	failover atomic.Uint64
}

type sessionAffinityStats struct {
	Hits      uint64
	Failovers uint64
}

// SessionAffinityConfig configures the session affinity selector.
type SessionAffinityConfig struct {
	Fallback Selector
	TTL      time.Duration
}

// NewSessionAffinitySelector creates a new session-aware selector.
func NewSessionAffinitySelector(fallback Selector) *SessionAffinitySelector {
	return NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback: fallback,
		TTL:      time.Hour,
	})
}

// NewSessionAffinitySelectorWithConfig creates a selector with custom configuration.
func NewSessionAffinitySelectorWithConfig(cfg SessionAffinityConfig) *SessionAffinitySelector {
	if cfg.Fallback == nil {
		cfg.Fallback = &RoundRobinSelector{}
	}
	if cfg.TTL <= 0 {
		cfg.TTL = time.Hour
	}
	return &SessionAffinitySelector{
		fallback: cfg.Fallback,
		cache:    NewSessionCache(cfg.TTL),
	}
}

// Pick selects an auth with session affinity when possible.
// Explicit Claude Code, Codex, OpenCode, pi, and request-body session signals
// precede execution metadata, stable derived identity, and the legacy hash fallback.
//
// An established binding outranks credential priority: a bound credential that is still
// available is reused even when a higher-priority credential recovers. Credential priority
// applies to cold bindings, requests without a session, and genuine bound-credential
// failover, so the fallback selector only ever receives the highest available priority tier.
//
// Note: The cache key includes provider, session ID, and model to handle cases where
// a session uses multiple models (e.g., gemini-2.5-pro and gemini-3-flash-preview)
// that may be supported by different auth credentials, and to avoid cross-provider conflicts.
func (s *SessionAffinitySelector) Pick(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	return s.pick(ctx, provider, model, opts, auths, nil, "")
}

func (s *SessionAffinitySelector) pickForExecution(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, auths []*Auth, inflight func(string) int, preferredAuthID string) (*Auth, error) {
	return s.pick(ctx, provider, model, opts, auths, inflight, preferredAuthID)
}

func (s *SessionAffinitySelector) pick(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, auths []*Auth, inflight func(string) int, preferredAuthID string) (*Auth, error) {
	entry := selectorLogEntry(ctx)
	if opts.Metadata == nil {
		opts.Metadata = make(map[string]any)
	}
	opts.Metadata[cliproxyexecutor.SessionAffinityProviderMetadataKey] = provider
	opts.Metadata[cliproxyexecutor.SessionAffinityModelMetadataKey] = model
	keys := sessionAffinityKeysForRequest(ctx, provider, model, opts)
	primaryID, fallbackID := keys.primaryID, keys.fallbackID
	now := time.Now()
	availabilityCandidates := auths
	if _, weighted := s.fallback.(*WeightedRoundRobinSelector); weighted {
		availabilityCandidates = positiveWeightAuths(auths)
	}
	if primaryID == "" {
		fallbackAuths, errAvailable := getAvailableAuths(availabilityCandidates, provider, model, now)
		if errAvailable != nil {
			return nil, errAvailable
		}
		usage.MarkAffinityOutcome(ctx, string(usage.AffinityOutcomeNone))
		entry.Debugf("session-affinity: no session ID extracted, falling back to default selector | provider=%s model=%s", provider, model)
		return s.pickFallback(ctx, provider, model, opts, fallbackAuths, inflight, preferredAuthID)
	}

	// A single availability pass serves both lookups: the bound credential is validated against
	// every priority tier, while the fallback selector keeps seeing only the highest tier.
	available, err := getAvailableAuthsAcrossPriorities(availabilityCandidates, provider, model, now)
	if err != nil {
		return nil, err
	}
	fallbackAuths := highestPriorityAuths(available)

	cacheKey, fallbackKey := keys.primaryKey, keys.fallbackKey
	unlock := s.lockSessionKeys(cacheKey, fallbackKey)
	defer unlock()

	bind := func(authID string) {
		if fallbackKey != "" {
			s.cache.SetAliases(authID, cacheKey, fallbackKey)
			return
		}
		s.cache.Set(cacheKey, authID)
	}

	if cachedAuthID, ok := s.cache.GetAndRefresh(cacheKey); ok {
		for _, auth := range available {
			if auth.ID == cachedAuthID {
				bind(auth.ID)
				usage.MarkAffinityOutcome(ctx, string(usage.AffinityOutcomeHit))
				s.hits.Add(1)
				entry.Infof("session-affinity: cache hit | session=%s auth=%s provider=%s model=%s", truncateSessionID(primaryID), auth.ID, provider, model)
				return auth, nil
			}
		}
		// Cached auth not available, reselect via fallback selector for even distribution
		auth, err := s.pickFallback(ctx, provider, model, opts, fallbackAuths, inflight, preferredAuthID)
		if err != nil {
			return nil, err
		}
		bind(auth.ID)
		usage.MarkAffinityOutcome(ctx, string(usage.AffinityOutcomeFailover))
		s.failover.Add(1)
		entry.Infof("session-affinity: cache hit but auth unavailable, reselected | session=%s auth=%s provider=%s model=%s", truncateSessionID(primaryID), auth.ID, provider, model)
		return auth, nil
	}

	if fallbackKey != "" {
		if cachedAuthID, ok := s.cache.Get(fallbackKey); ok {
			for _, auth := range available {
				if auth.ID == cachedAuthID {
					bind(auth.ID)
					usage.MarkAffinityOutcome(ctx, string(usage.AffinityOutcomeFallbackHit))
					s.hits.Add(1)
					entry.Infof("session-affinity: fallback cache hit | session=%s fallback=%s auth=%s provider=%s model=%s", truncateSessionID(primaryID), truncateSessionID(fallbackID), auth.ID, provider, model)
					return auth, nil
				}
			}
		}
	}

	auth, err := s.pickFallback(ctx, provider, model, opts, fallbackAuths, inflight, preferredAuthID)
	if err != nil {
		return nil, err
	}
	bind(auth.ID)
	usage.MarkAffinityOutcome(ctx, string(usage.AffinityOutcomeMiss))
	entry.Infof("session-affinity: cache miss, new binding | session=%s auth=%s provider=%s model=%s", truncateSessionID(primaryID), auth.ID, provider, model)
	return auth, nil
}

func (s *SessionAffinitySelector) lockSessionKeys(primaryKey, fallbackKey string) func() {
	primaryIndex := sessionAffinityLockIndex(primaryKey, len(s.pickMu))
	fallbackIndex := primaryIndex
	if fallbackKey != "" {
		fallbackIndex = sessionAffinityLockIndex(fallbackKey, len(s.pickMu))
	}
	if primaryIndex == fallbackIndex {
		s.pickMu[primaryIndex].Lock()
		return s.pickMu[primaryIndex].Unlock
	}
	if primaryIndex > fallbackIndex {
		primaryIndex, fallbackIndex = fallbackIndex, primaryIndex
	}
	s.pickMu[primaryIndex].Lock()
	s.pickMu[fallbackIndex].Lock()
	return func() {
		s.pickMu[fallbackIndex].Unlock()
		s.pickMu[primaryIndex].Unlock()
	}
}

func sessionAffinityLockIndex(key string, stripes int) int {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(key))
	return int(hash.Sum64() % uint64(stripes))
}

func (s *SessionAffinitySelector) pickFallback(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, auths []*Auth, inflight func(string) int, preferredAuthID string) (*Auth, error) {
	if inflight == nil {
		return s.fallback.Pick(ctx, provider, model, opts, auths)
	}
	available, err := getAvailableAuths(auths, provider, model, time.Now())
	if err != nil {
		return nil, err
	}
	available = preferWebsocketAuths(ctx, provider, available)
	if preferred := authByID(available, preferredAuthID); preferred != nil {
		available = []*Auth{preferred}
	} else {
		available = leastInflightAuths(available, inflight)
	}
	return s.fallback.Pick(ctx, provider, model, opts, available)
}

func (s *SessionAffinitySelector) stats() sessionAffinityStats {
	if s == nil {
		return sessionAffinityStats{}
	}
	return sessionAffinityStats{Hits: s.hits.Load(), Failovers: s.failover.Load()}
}

func selectorLogEntry(ctx context.Context) *log.Entry {
	if ctx == nil {
		return log.NewEntry(log.StandardLogger())
	}
	if reqID := logging.GetRequestID(ctx); reqID != "" {
		return log.WithField("request_id", reqID)
	}
	return log.NewEntry(log.StandardLogger())
}

// truncateSessionID shortens session ID for logging (first 8 chars + "...")
func truncateSessionID(id string) string {
	if len(id) <= 20 {
		return id
	}
	return id[:8] + "..."
}

// Stop releases resources held by the selector.
func (s *SessionAffinitySelector) Stop() {
	if s.cache != nil {
		s.cache.Stop()
	}
}

// InvalidateAuth removes all session bindings for a specific auth.
// Called when an auth becomes rate-limited or unavailable.
func (s *SessionAffinitySelector) InvalidateAuth(authID string) {
	if s.cache != nil {
		s.cache.InvalidateAuth(authID)
	}
}

// OnResult handles session affinity binding or release based on execution outcome.
func (s *SessionAffinitySelector) OnResult(res Result) {
	if s == nil || s.cache == nil || res.AuthID == "" {
		return
	}
	state := sessionAffinityResultForRequest(context.Background(), res.Provider, res.Model, res.Options)
	s.onResult(res, state)
}

func (s *SessionAffinitySelector) onResult(res Result, state resultSessionAffinity) {
	if state.primaryKey == "" {
		return
	}
	if res.Success {
		s.cache.Touch(state.primaryKey, res.AuthID)
		if state.fallbackKey != "" {
			s.cache.Touch(state.fallbackKey, res.AuthID)
		}
		return
	}

	if res.Error != nil && shouldSkipCredentialCooldown(res.Error) {
		return
	}

	s.cache.CompareAndDelete(state.primaryKey, res.AuthID)
	if state.fallbackKey != "" {
		s.cache.CompareAndDelete(state.fallbackKey, res.AuthID)
	}
}

type sessionAffinityKeys struct {
	primaryID   string
	fallbackID  string
	primaryKey  string
	fallbackKey string
}

func sessionAffinityKeysForRequest(ctx context.Context, provider, model string, opts cliproxyexecutor.Options) sessionAffinityKeys {
	primaryID, fallbackID := extractSessionIDsForProvider(ctx, provider, opts)
	keys := sessionAffinityKeys{primaryID: primaryID, fallbackID: fallbackID}
	if primaryID == "" {
		return keys
	}
	modelKey := canonicalModelKey(model)
	keys.primaryKey = sessionAffinityCacheKey(provider, primaryID, modelKey)
	if fallbackID != "" && fallbackID != primaryID {
		keys.fallbackKey = sessionAffinityCacheKey(provider, fallbackID, modelKey)
	}
	return keys
}

func sessionAffinityCacheKey(provider, sessionID, model string) string {
	return fmt.Sprintf("affinity:v2:%d:%s:%d:%s:%d:%s", len(provider), provider, len(sessionID), sessionID, len(model), model)
}

func sessionAffinityResultForRequest(ctx context.Context, provider, model string, opts cliproxyexecutor.Options) resultSessionAffinity {
	if raw := metadataStringValue(opts.Metadata, cliproxyexecutor.SessionAffinityProviderMetadataKey); raw != "" {
		provider = raw
	}
	if raw := metadataStringValue(opts.Metadata, cliproxyexecutor.SessionAffinityModelMetadataKey); raw != "" {
		model = raw
	}
	keys := sessionAffinityKeysForRequest(ctx, provider, model, opts)
	return resultSessionAffinity{
		primaryKey:  keys.primaryKey,
		fallbackKey: keys.fallbackKey,
		provider:    provider,
		model:       model,
		options:     cloneSessionAffinityOptions(opts),
	}
}

func cloneSessionAffinityOptions(opts cliproxyexecutor.Options) cliproxyexecutor.Options {
	cloned := opts
	cloned.Headers = cloneHTTPHeader(opts.Headers)
	if opts.Query != nil {
		cloned.Query = make(map[string][]string, len(opts.Query))
		for key, values := range opts.Query {
			cloned.Query[key] = append([]string(nil), values...)
		}
	}
	cloned.OriginalRequest = bytes.Clone(opts.OriginalRequest)
	cloned.Metadata = cloneSessionAffinityMetadata(opts.Metadata)
	return cloned
}

func cloneSessionAffinityMetadata(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	clonedValue, ok := cloneSessionAffinityMetadataGraph(reflect.ValueOf(src))
	if !ok {
		return sessionAffinityMetadataFallback(src)
	}
	cloned, ok := clonedValue.Interface().(map[string]any)
	if !ok {
		return sessionAffinityMetadataFallback(src)
	}
	return cloned
}

const (
	maxSessionAffinityMetadataCloneNodes = 4096
	maxSessionAffinityMetadataCloneDepth = 64
	maxSessionAffinityMetadataCloneItems = 4096
	maxSessionAffinityMetadataCloneBytes = 1 << 20
)

type sessionAffinityMetadataVisit struct {
	typ      reflect.Type
	kind     reflect.Kind
	pointer  uintptr
	length   int
	capacity int
}

type sessionAffinityMetadataCloner struct {
	visited map[sessionAffinityMetadataVisit]reflect.Value
	nodes   int
	items   int
	bytes   uint64
}

func cloneSessionAffinityMetadataGraph(value reflect.Value) (cloned reflect.Value, ok bool) {
	defer func() {
		if recover() != nil {
			cloned = reflect.Value{}
			ok = false
		}
	}()
	cloner := sessionAffinityMetadataCloner{visited: make(map[sessionAffinityMetadataVisit]reflect.Value)}
	return cloner.clone(value, 0)
}

func (c *sessionAffinityMetadataCloner) clone(value reflect.Value, depth int) (reflect.Value, bool) {
	if !value.IsValid() {
		return value, true
	}
	if depth > maxSessionAffinityMetadataCloneDepth || c.nodes >= maxSessionAffinityMetadataCloneNodes {
		return reflect.Value{}, false
	}
	c.nodes++

	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type()), true
		}
		clonedElement, ok := c.clone(value.Elem(), depth+1)
		if !ok {
			return reflect.Value{}, false
		}
		cloned := reflect.New(value.Type()).Elem()
		cloned.Set(clonedElement)
		return cloned, true
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type()), true
		}
		visit := sessionAffinityMetadataVisit{typ: value.Type(), kind: value.Kind(), pointer: value.Pointer()}
		if cloned, exists := c.visited[visit]; exists {
			return cloned, true
		}
		if !c.reserveContainer(value.Len(), 2, value.Type().Key().Size()+value.Type().Elem().Size()) {
			return reflect.Value{}, false
		}
		cloned := reflect.MakeMapWithSize(value.Type(), value.Len())
		c.visited[visit] = cloned
		iterator := value.MapRange()
		for iterator.Next() {
			clonedKey, okKey := c.clone(iterator.Key(), depth+1)
			clonedValue, okValue := c.clone(iterator.Value(), depth+1)
			if !okKey || !okValue {
				return reflect.Value{}, false
			}
			cloned.SetMapIndex(clonedKey, clonedValue)
		}
		return cloned, true
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type()), true
		}
		visit := sessionAffinityMetadataVisit{typ: value.Type(), kind: value.Kind(), pointer: value.Pointer(), length: value.Len(), capacity: value.Cap()}
		if cloned, exists := c.visited[visit]; exists {
			return cloned, true
		}
		if !c.reserveContainer(value.Len(), 1, value.Type().Elem().Size()) {
			return reflect.Value{}, false
		}
		cloned := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		c.visited[visit] = cloned
		for index := 0; index < value.Len(); index++ {
			clonedElement, ok := c.clone(value.Index(index), depth+1)
			if !ok {
				return reflect.Value{}, false
			}
			cloned.Index(index).Set(clonedElement)
		}
		return cloned, true
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type()), true
		}
		visit := sessionAffinityMetadataVisit{typ: value.Type(), kind: value.Kind(), pointer: value.Pointer()}
		if cloned, exists := c.visited[visit]; exists {
			return cloned, true
		}
		if !c.reserveBytes(value.Type().Elem().Size()) {
			return reflect.Value{}, false
		}
		cloned := reflect.New(value.Type().Elem())
		c.visited[visit] = cloned
		clonedElement, ok := c.clone(value.Elem(), depth+1)
		if !ok {
			return reflect.Value{}, false
		}
		cloned.Elem().Set(clonedElement)
		return cloned, true
	case reflect.Struct:
		if !c.reserveBytes(value.Type().Size()) {
			return reflect.Value{}, false
		}
		cloned := reflect.New(value.Type()).Elem()
		for index := 0; index < value.NumField(); index++ {
			if value.Type().Field(index).PkgPath != "" {
				return reflect.Value{}, false
			}
			clonedField, ok := c.clone(value.Field(index), depth+1)
			if !ok {
				return reflect.Value{}, false
			}
			cloned.Field(index).Set(clonedField)
		}
		return cloned, true
	case reflect.Array:
		if !c.reserveContainer(value.Len(), 1, value.Type().Elem().Size()) {
			return reflect.Value{}, false
		}
		cloned := reflect.New(value.Type()).Elem()
		for index := 0; index < value.Len(); index++ {
			clonedElement, ok := c.clone(value.Index(index), depth+1)
			if !ok {
				return reflect.Value{}, false
			}
			cloned.Index(index).Set(clonedElement)
		}
		return cloned, true
	case reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return reflect.Value{}, false
	default:
		// Scalar values are immutable and safe to share.
		return value, true
	}
}

func (c *sessionAffinityMetadataCloner) reserveContainer(length, nodeMultiplier int, itemSize uintptr) bool {
	if length < 0 || length > maxSessionAffinityMetadataCloneItems-c.items {
		return false
	}
	if nodeMultiplier > 0 && length > (maxSessionAffinityMetadataCloneNodes-c.nodes)/nodeMultiplier {
		return false
	}
	if !c.reserveBytesForItems(length, itemSize) {
		return false
	}
	c.items += length
	return true
}

func (c *sessionAffinityMetadataCloner) reserveBytesForItems(length int, itemSize uintptr) bool {
	if length == 0 || itemSize == 0 {
		return true
	}
	remaining := uint64(maxSessionAffinityMetadataCloneBytes) - c.bytes
	width := uint64(itemSize)
	if uint64(length) > remaining/width {
		return false
	}
	c.bytes += uint64(length) * width
	return true
}

func (c *sessionAffinityMetadataCloner) reserveBytes(size uintptr) bool {
	remaining := uint64(maxSessionAffinityMetadataCloneBytes) - c.bytes
	if uint64(size) > remaining {
		return false
	}
	c.bytes += uint64(size)
	return true
}

func sessionAffinityMetadataFallback(src map[string]any) map[string]any {
	keys := [...]string{
		cliproxyexecutor.CallerScopeMetadataKey,
		cliproxyexecutor.ExecutionSessionMetadataKey,
		cliproxyexecutor.DerivedSessionIDMetadataKey,
		cliproxyexecutor.SessionAffinityProviderMetadataKey,
		cliproxyexecutor.SessionAffinityModelMetadataKey,
	}
	fallback := make(map[string]any, len(keys))
	for _, key := range keys {
		if value, ok := src[key].(string); ok {
			fallback[key] = value
		}
	}
	return fallback
}

// normalizedSessionCandidate validates an explicit client-provided session signal.
// It keeps opaque printable IDs intact while rejecting values that are unsafe or
// implausibly large for routing keys and logs.
func normalizedSessionCandidate(raw string) string {
	return cliproxysession.NormalizeExplicitID(raw)
}

func sessionHeaderValue(headers http.Header, name string) string {
	if headers == nil {
		return ""
	}
	if value := normalizedSessionCandidate(headers.Get(name)); value != "" {
		return value
	}
	for key, values := range headers {
		if !strings.EqualFold(key, name) {
			continue
		}
		for _, raw := range values {
			if value := normalizedSessionCandidate(raw); value != "" {
				return value
			}
		}
	}
	return ""
}

// ExtractSessionID extracts a session identifier from explicit client signals,
// then falls back to execution metadata, derived identity, and message history.
// Priority order:
//  1. X-Claude-Code-Session-Id
//  2. Claude Code metadata.user_id session
//  3. Session-Id / Session_id (Codex and compatible clients)
//  4. X-Session-ID
//  5. X-Session-Affinity (OpenCode)
//  6. X-Client-Request-Id (pi Responses)
//  7. session_id / sessionId
//  8. prompt_cache_key, with conversation / conversation.id as an alias
//  9. metadata.user_id and conversation_id legacy body fields
//  10. explicit execution session metadata
//  11. stable context-derived session identity
//  12. stable hash from initial message content
func ExtractSessionID(headers http.Header, payload []byte, metadata map[string]any) string {
	primary, _ := extractSessionIDs(headers, payload, metadata)
	return primary
}

// extractSessionIDs returns (primaryID, fallbackID) for session affinity.
// fallbackID preserves an earlier binding when a stronger body identifier appears
// later, and lets callers bind both identifiers when both are present.
func extractSessionIDs(headers http.Header, payload []byte, metadata map[string]any) (string, string) {
	if primaryID, fallbackID := extractExplicitSessionIDs(headers, payload); primaryID != "" {
		return primaryID, fallbackID
	}
	if len(payload) > 0 {
		if userID := normalizedSessionCandidate(gjson.GetBytes(payload, "metadata.user_id").String()); userID != "" {
			return "user:" + userID, ""
		}
		if conversationID := normalizedSessionCandidate(gjson.GetBytes(payload, "conversation_id").String()); conversationID != "" {
			return "conv:" + conversationID, ""
		}
	}
	if executionID, ok := metadata[cliproxyexecutor.ExecutionSessionMetadataKey].(string); ok {
		if executionID = normalizedSessionCandidate(executionID); executionID != "" {
			return "execution:" + executionID, ""
		}
	}
	if derivedID := normalizedSessionCandidate(cliproxysession.DerivedID(metadata)); derivedID != "" {
		return "derived:" + derivedID, ""
	}
	if len(payload) == 0 {
		return "", ""
	}
	return extractMessageHashIDs(payload)
}

func extractExplicitSessionIDs(headers http.Header, payload []byte) (string, string) {
	if sid := sessionHeaderValue(headers, "X-Claude-Code-Session-Id"); sid != "" {
		return "claude:" + sid, ""
	}
	if sid := cliproxysession.ClaudeMetadataSessionID(payload); sid != "" {
		return "claude:" + sid, ""
	}
	if sid := sessionHeaderValue(headers, "Session-Id"); sid != "" {
		return "codex:" + sid, ""
	}
	if sid := sessionHeaderValue(headers, "Session_id"); sid != "" {
		return "codex:" + sid, ""
	}
	if sid := sessionHeaderValue(headers, "X-Session-ID"); sid != "" {
		return "header:" + sid, ""
	}
	if sid := sessionHeaderValue(headers, "X-Session-Affinity"); sid != "" {
		return "affinity:" + sid, ""
	}
	if sid := sessionHeaderValue(headers, "X-Client-Request-Id"); sid != "" {
		return "clientreq:" + sid, ""
	}
	if len(payload) == 0 {
		return "", ""
	}
	for _, path := range []string{"session_id", "sessionId"} {
		if sid := normalizedSessionCandidate(gjson.GetBytes(payload, path).String()); sid != "" {
			return "session:" + sid, ""
		}
	}
	conversationID := ""
	conversation := gjson.GetBytes(payload, "conversation")
	if sid := normalizedSessionCandidate(conversation.Get("id").String()); sid != "" {
		conversationID = "conv:" + sid
	} else if conversation.Type == gjson.String {
		if sid := normalizedSessionCandidate(conversation.String()); sid != "" {
			conversationID = "conv:" + sid
		}
	}
	if sid := normalizedSessionCandidate(gjson.GetBytes(payload, "prompt_cache_key").String()); sid != "" {
		return "pck:" + sid, conversationID
	}
	if conversationID != "" {
		return conversationID, ""
	}
	return "", ""
}

func extractSessionIDsForProvider(ctx context.Context, provider string, opts cliproxyexecutor.Options) (string, string) {
	finalize := func(primaryID, fallbackID string) (string, string) {
		return scopeAffinitySessionIDs(opts.Metadata, primaryID, fallbackID)
	}
	explicitPrimary, explicitFallback := extractExplicitSessionIDs(opts.Headers, opts.OriginalRequest)
	if explicitPrimary != "" && !strings.HasPrefix(explicitPrimary, "pck:") {
		return finalize(explicitPrimary, explicitFallback)
	}
	if !strings.EqualFold(strings.TrimSpace(provider), "xai") {
		return finalize(extractSessionIDs(opts.Headers, opts.OriginalRequest, opts.Metadata))
	}
	if executionSessionID := metadataStringValue(opts.Metadata, cliproxyexecutor.ExecutionSessionMetadataKey); executionSessionID != "" {
		return finalize("xai:exec:"+executionSessionID, "")
	}
	if opts.SourceFormat == sdktranslator.FormatOpenAIResponse {
		promptCacheKey := strings.TrimSpace(gjson.GetBytes(opts.OriginalRequest, "prompt_cache_key").String())
		callerScope := metadataStringValue(opts.Metadata, cliproxyexecutor.CallerScopeMetadataKey)
		if callerScope == "" {
			callerScope = callerAPIKeyScope(ctx)
		}
		if promptCacheKey != "" && callerScope != "" {
			digest := sha256.Sum256([]byte("cli-proxy-api:xai:prompt-cache-affinity\x00" + callerScope + "\x00" + promptCacheKey))
			return finalize(fmt.Sprintf("xai:pck:%x", digest), explicitFallback)
		}
	}
	if strings.HasPrefix(explicitPrimary, "pck:") {
		if explicitFallback != "" {
			return finalize(explicitFallback, "")
		}
		return "", ""
	}
	primaryID, fallbackID := extractSessionIDs(opts.Headers, opts.OriginalRequest, opts.Metadata)
	if strings.HasPrefix(primaryID, "pck:") {
		return "", ""
	}
	return finalize(primaryID, fallbackID)
}

func scopeAffinitySessionIDs(metadata map[string]any, primaryID, fallbackID string) (string, string) {
	callerScope := metadataStringValue(metadata, cliproxyexecutor.CallerScopeMetadataKey)
	if callerScope == "" || primaryID == "" {
		return primaryID, fallbackID
	}
	scopeID := func(sessionID string) string {
		if sessionID == "" {
			return ""
		}
		digest := sha256.Sum256([]byte("cli-proxy-api:session-affinity-caller:v1\x00" + callerScope + "\x00" + sessionID))
		return fmt.Sprintf("caller:%x", digest)
	}
	return scopeID(primaryID), scopeID(fallbackID)
}

func metadataStringValue(metadata map[string]any, key string) string {
	if len(metadata) == 0 {
		return ""
	}
	switch value := metadata[key].(type) {
	case string:
		return strings.TrimSpace(value)
	case []byte:
		return strings.TrimSpace(string(value))
	default:
		return ""
	}
}

func callerAPIKeyScope(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	ginCtx, ok := ctx.Value("gin").(interface{ Get(string) (any, bool) })
	if !ok || ginCtx == nil {
		return ""
	}
	raw, exists := ginCtx.Get("userApiKey")
	if !exists {
		return ""
	}
	apiKey, ok := raw.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(apiKey)
}

func extractMessageHashIDs(payload []byte) (primaryID, fallbackID string) {
	var systemPrompt, firstUserMsg, firstAssistantMsg string

	// OpenAI/Claude messages format
	messages := gjson.GetBytes(payload, "messages")
	if messages.Exists() && messages.IsArray() {
		messages.ForEach(func(_, msg gjson.Result) bool {
			role := msg.Get("role").String()
			content := extractMessageContent(msg.Get("content"))
			if content == "" {
				return true
			}

			switch role {
			case "system":
				if systemPrompt == "" {
					systemPrompt = truncateString(content, 100)
				}
			case "user":
				if firstUserMsg == "" {
					firstUserMsg = truncateString(content, 100)
				}
			case "assistant":
				if firstAssistantMsg == "" {
					firstAssistantMsg = truncateString(content, 100)
				}
			}

			if systemPrompt != "" && firstUserMsg != "" && firstAssistantMsg != "" {
				return false
			}
			return true
		})
	}

	// Claude API: top-level "system" field (array or string)
	if systemPrompt == "" {
		topSystem := gjson.GetBytes(payload, "system")
		if topSystem.Exists() {
			if topSystem.IsArray() {
				topSystem.ForEach(func(_, part gjson.Result) bool {
					if text := part.Get("text").String(); text != "" && systemPrompt == "" {
						systemPrompt = truncateString(text, 100)
						return false
					}
					return true
				})
			} else if topSystem.Type == gjson.String {
				systemPrompt = truncateString(topSystem.String(), 100)
			}
		}
	}

	// Gemini format
	if systemPrompt == "" && firstUserMsg == "" {
		sysInstr := gjson.GetBytes(payload, "systemInstruction.parts")
		if sysInstr.Exists() && sysInstr.IsArray() {
			sysInstr.ForEach(func(_, part gjson.Result) bool {
				if text := part.Get("text").String(); text != "" && systemPrompt == "" {
					systemPrompt = truncateString(text, 100)
					return false
				}
				return true
			})
		}

		contents := gjson.GetBytes(payload, "contents")
		if contents.Exists() && contents.IsArray() {
			contents.ForEach(func(_, msg gjson.Result) bool {
				role := msg.Get("role").String()
				msg.Get("parts").ForEach(func(_, part gjson.Result) bool {
					text := part.Get("text").String()
					if text == "" {
						return true
					}
					switch role {
					case "user":
						if firstUserMsg == "" {
							firstUserMsg = truncateString(text, 100)
						}
					case "model":
						if firstAssistantMsg == "" {
							firstAssistantMsg = truncateString(text, 100)
						}
					}
					return false
				})
				if firstUserMsg != "" && firstAssistantMsg != "" {
					return false
				}
				return true
			})
		}
	}

	// OpenAI Responses API format (v1/responses)
	if systemPrompt == "" && firstUserMsg == "" {
		if instr := gjson.GetBytes(payload, "instructions").String(); instr != "" {
			systemPrompt = truncateString(instr, 100)
		}

		input := gjson.GetBytes(payload, "input")
		if input.Exists() && input.IsArray() {
			input.ForEach(func(_, item gjson.Result) bool {
				itemType := item.Get("type").String()
				if itemType == "reasoning" {
					return true
				}
				// Skip non-message typed items (function_call, function_call_output, etc.)
				// but allow items with no type that have a role (inline message format).
				if itemType != "" && itemType != "message" {
					return true
				}

				role := item.Get("role").String()
				if itemType == "" && role == "" {
					return true
				}

				// Handle both string content and array content (multimodal).
				content := item.Get("content")
				var text string
				if content.Type == gjson.String {
					text = content.String()
				} else {
					text = extractResponsesAPIContent(content)
				}
				if text == "" {
					return true
				}

				switch role {
				case "developer", "system":
					if systemPrompt == "" {
						systemPrompt = truncateString(text, 100)
					}
				case "user":
					if firstUserMsg == "" {
						firstUserMsg = truncateString(text, 100)
					}
				case "assistant":
					if firstAssistantMsg == "" {
						firstAssistantMsg = truncateString(text, 100)
					}
				}

				if firstUserMsg != "" && firstAssistantMsg != "" {
					return false
				}
				return true
			})
		}
	}

	if systemPrompt == "" && firstUserMsg == "" {
		return "", ""
	}

	shortHash := computeSessionHash(systemPrompt, firstUserMsg, "")
	if firstAssistantMsg == "" {
		return shortHash, ""
	}

	fullHash := computeSessionHash(systemPrompt, firstUserMsg, firstAssistantMsg)
	return fullHash, shortHash
}

func computeSessionHash(systemPrompt, userMsg, assistantMsg string) string {
	h := fnv.New64a()
	if systemPrompt != "" {
		h.Write([]byte("sys:" + systemPrompt + "\n"))
	}
	if userMsg != "" {
		h.Write([]byte("usr:" + userMsg + "\n"))
	}
	if assistantMsg != "" {
		h.Write([]byte("ast:" + assistantMsg + "\n"))
	}
	return fmt.Sprintf("msg:%016x", h.Sum64())
}

func truncateString(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen]
	}
	return s
}

// extractMessageContent extracts text content from a message content field.
// Handles both string content and array content (multimodal messages).
// For array content, extracts text from all text-type elements.
func extractMessageContent(content gjson.Result) string {
	// String content: "Hello world"
	if content.Type == gjson.String {
		return content.String()
	}

	// Array content: [{"type":"text","text":"Hello"},{"type":"image",...}]
	if content.IsArray() {
		var texts []string
		content.ForEach(func(_, part gjson.Result) bool {
			// Handle Claude format: {"type":"text","text":"content"}
			if part.Get("type").String() == "text" {
				if text := part.Get("text").String(); text != "" {
					texts = append(texts, text)
				}
			}
			// Handle OpenAI format: {"type":"text","text":"content"}
			// Same structure as Claude, already handled above
			return true
		})
		if len(texts) > 0 {
			return strings.Join(texts, " ")
		}
	}

	return ""
}

func extractResponsesAPIContent(content gjson.Result) string {
	if !content.IsArray() {
		return ""
	}
	var texts []string
	content.ForEach(func(_, part gjson.Result) bool {
		partType := part.Get("type").String()
		if partType == "input_text" || partType == "output_text" || partType == "text" {
			if text := part.Get("text").String(); text != "" {
				texts = append(texts, text)
			}
		}
		return true
	})
	if len(texts) > 0 {
		return strings.Join(texts, " ")
	}
	return ""
}

// extractSessionID is kept for backward compatibility.
// Deprecated: Use ExtractSessionID instead.
func extractSessionID(payload []byte) string {
	return ExtractSessionID(nil, payload, nil)
}
