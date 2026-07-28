package auth

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/singleflight"
)

// ProviderExecutor defines the contract required by Manager to execute provider calls.
type ProviderExecutor interface {
	// Identifier returns the provider key handled by this executor.
	Identifier() string
	// Execute handles non-streaming execution and returns the provider response payload.
	Execute(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error)
	// ExecuteStream handles streaming execution and returns a StreamResult containing
	// upstream headers and a channel of provider chunks.
	ExecuteStream(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error)
	// Refresh attempts to refresh provider credentials and returns the updated auth state.
	Refresh(ctx context.Context, auth *Auth) (*Auth, error)
	// CountTokens returns the token count for the given request.
	CountTokens(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error)
	// HttpRequest injects provider credentials into the supplied HTTP request and executes it.
	// Callers must close the response body when non-nil.
	HttpRequest(ctx context.Context, auth *Auth, req *http.Request) (*http.Response, error)
}

// RequestAuthPreparer lets an executor update missing auth metadata immediately
// before a request. Manager serializes and persists returned updates.
type RequestAuthPreparer interface {
	ShouldPrepareRequestAuth(auth *Auth) bool
	PrepareRequestAuth(ctx context.Context, auth *Auth) (*Auth, error)
}

// ExecutionSessionCloser allows executors to release per-session runtime resources.
type ExecutionSessionCloser interface {
	CloseExecutionSession(sessionID string)
}

// Result captures execution outcome used to adjust auth state.
type Result struct {
	// AuthID references the auth that produced this result.
	AuthID string
	// Provider is copied for convenience when emitting hooks.
	Provider string
	// Model is the upstream model identifier used for the request.
	Model string
	// Success marks whether the execution succeeded.
	Success bool
	// RetryAfter carries a provider supplied retry hint (e.g. 429 retryDelay).
	RetryAfter *time.Duration
	// Error describes the failure when Success is false.
	Error *Error
}

// Selector chooses an auth candidate for execution.
type Selector interface {
	Pick(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, auths []*Auth) (*Auth, error)
}

type PluginScheduler interface {
	PickAuth(context.Context, pluginapi.SchedulerPickRequest) (pluginapi.SchedulerPickResponse, bool, error)
}

type pluginSchedulerState interface {
	HasScheduler() bool
}

// StoppableSelector is an optional interface for selectors that hold resources.
// Selectors that implement this interface will have Stop called during shutdown.
type StoppableSelector interface {
	Selector
	Stop()
}

// Hook captures lifecycle callbacks for observing auth changes.
type Hook interface {
	// OnAuthRegistered fires when a new auth is registered.
	OnAuthRegistered(ctx context.Context, auth *Auth)
	// OnAuthUpdated fires when an existing auth changes state.
	OnAuthUpdated(ctx context.Context, auth *Auth)
	// OnResult fires when execution result is recorded.
	OnResult(ctx context.Context, result Result)
}

// NoopHook provides optional hook defaults.
type NoopHook struct{}

// OnAuthRegistered implements Hook.
func (NoopHook) OnAuthRegistered(context.Context, *Auth) {}

// OnAuthUpdated implements Hook.
func (NoopHook) OnAuthUpdated(context.Context, *Auth) {}

// OnResult implements Hook.
func (NoopHook) OnResult(context.Context, Result) {}

// Manager orchestrates auth lifecycle, selection, execution, and persistence.
type Manager struct {
	store                     Store
	cooldownStore             CooldownStateStore
	pendingCooldownStateStore CooldownStateStore
	executors                 map[string]ProviderExecutor
	selector                  Selector
	hook                      Hook
	mu                        sync.RWMutex
	configCooldownMu          sync.Mutex
	auths                     map[string]*Auth
	scheduler                 *authScheduler
	// pluginScheduler runs outside m.mu before falling back to native selection.
	pluginScheduler PluginScheduler
	// homeRuntimeAuths retains legacy session auth lookups for non-execution callers.
	homeRuntimeAuths map[string]map[string]*Auth
	// homeRuntimeAuthOwners prevents a stale selection from clearing a replacement auth.
	homeRuntimeAuthOwners map[string]map[string]*HomeDispatchSelection
	// homeSessionSelections owns retained Home selections for websocket sessions.
	homeSessionSelections map[string]map[homeSessionSelectionKey]*HomeDispatchSelection
	homeSessionLocks      sync.Map
	homeSessionAliases    homeSessionAliasCache
	// providerOffsets tracks per-model provider rotation state for multi-provider routing.
	providerOffsets             map[string]int
	homeDispatchBundle          atomic.Pointer[HomeDispatchBundle]
	homeInFlightPublisherConfig atomic.Pointer[HomeInFlightPublisherConfig]

	// Retry controls request retry behavior.
	requestRetry        atomic.Int32
	maxRetryCredentials atomic.Int32
	maxRetryInterval    atomic.Int64

	// oauthModelAlias stores global OAuth model alias mappings (alias -> upstream name) keyed by channel.
	oauthModelAlias atomic.Value

	// apiKeyModelAlias caches resolved model alias mappings for API-key auths.
	// Keyed by auth.ID, value is alias(lower) -> upstream model (including suffix).
	apiKeyModelAlias atomic.Value

	// modelPoolOffsets tracks per-auth alias pool rotation state.
	modelPoolOffsets map[string]int

	// runtimeConfig stores the latest application config for request-time decisions.
	// It is initialized in NewManager; never Load() before first Store().
	runtimeConfig atomic.Value

	// Optional HTTP RoundTripper provider injected by host.
	rtProvider RoundTripperProvider

	// Auto refresh state
	refreshCancel context.CancelFunc
	refreshLoop   *authAutoRefreshLoop
	refreshSF     singleflight.Group

	clusterMu           sync.RWMutex
	authRefreshLocker   AuthRefreshLocker
	leaderGate          LeaderGate
	authRing            AuthRingView
	authShardingEnabled bool
	spilloverEnabled    bool

	requestPrepareLocks sync.Map
	// refreshLocks serializes credential refresh per auth ID so concurrent
	// 401 recoveries and auto-refresh workers do not race the same refresh_token.
	refreshLocks sync.Map
}

// LeaderGate reports whether this process should run singleton background loops.
type LeaderGate interface{ IsLeader() bool }

// AuthRefreshLocker coordinates credential refresh across cluster replicas.
type AuthRefreshLocker interface {
	TryLock(ctx context.Context, authID string) (release func(), ok bool, err error)
}

func (m *Manager) SetLeaderGate(g LeaderGate) {
	if m == nil {
		return
	}
	m.clusterMu.Lock()
	m.leaderGate = g
	m.clusterMu.Unlock()
}
func (m *Manager) IsLeader() bool {
	if m == nil {
		return true
	}
	m.clusterMu.RLock()
	g := m.leaderGate
	m.clusterMu.RUnlock()
	return g == nil || g.IsLeader()
}
func (m *Manager) SetAuthRefreshLocker(l AuthRefreshLocker) {
	if m == nil {
		return
	}
	m.clusterMu.Lock()
	m.authRefreshLocker = l
	m.clusterMu.Unlock()
}
func (m *Manager) currentAuthRefreshLocker() AuthRefreshLocker {
	if m == nil {
		return nil
	}
	m.clusterMu.RLock()
	defer m.clusterMu.RUnlock()
	return m.authRefreshLocker
}
func (m *Manager) SetAuthRing(ring AuthRingView) {
	if m == nil {
		return
	}
	m.clusterMu.Lock()
	m.authRing = ring
	m.clusterMu.Unlock()
}
func (m *Manager) SetAuthShardingEnabled(enabled bool) {
	if m == nil {
		return
	}
	m.clusterMu.Lock()
	m.authShardingEnabled = enabled
	m.clusterMu.Unlock()
}
func (m *Manager) IsAuthShardingEnabled() bool {
	if m == nil {
		return false
	}
	m.clusterMu.RLock()
	defer m.clusterMu.RUnlock()
	return m.authShardingEnabled
}
func (m *Manager) SetSpilloverEnabled(enabled bool) {
	if m == nil {
		return
	}
	m.clusterMu.Lock()
	m.spilloverEnabled = enabled
	m.clusterMu.Unlock()
}
func (m *Manager) IsSpilloverEnabled() bool {
	if m == nil {
		return false
	}
	m.clusterMu.RLock()
	defer m.clusterMu.RUnlock()
	return m.spilloverEnabled
}
func (m *Manager) OwnsAuth(authID string) bool {
	if m == nil {
		return true
	}
	m.clusterMu.RLock()
	enabled, ring := m.authShardingEnabled, m.authRing
	m.clusterMu.RUnlock()
	return !enabled || ring == nil || ring.IsMine(authID)
}
func (m *Manager) OwnsAuthStrict(authID string) bool {
	if m == nil {
		return true
	}
	m.clusterMu.RLock()
	enabled, ring := m.authShardingEnabled, m.authRing
	m.clusterMu.RUnlock()
	return !enabled || ring == nil || (ring.Ready() && ring.IsMine(authID))
}
func (m *Manager) ShouldRefreshLocally(authID string) bool {
	if m == nil {
		return true
	}
	m.clusterMu.RLock()
	enabled, ring, gate := m.authShardingEnabled, m.authRing, m.leaderGate
	m.clusterMu.RUnlock()
	if enabled && ring != nil && ring.Ready() {
		return ring.IsMine(authID)
	}
	return gate == nil || gate.IsLeader()
}
func (m *Manager) SyncScheduler() {
	if m != nil {
		m.syncScheduler()
	}
}

type authByIDStore interface {
	GetByID(ctx context.Context, id string) (*Auth, error)
}

// ReloadByID refreshes one auth from stores that support indexed lookup.
func (m *Manager) ReloadByID(ctx context.Context, id string) error {
	if m == nil || m.store == nil {
		return nil
	}
	if strings.TrimSpace(id) == "" {
		return m.Load(ctx)
	}
	byID, ok := m.store.(authByIDStore)
	if !ok {
		return m.Load(ctx)
	}
	fetched, err := byID.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if fetched != nil {
		fetched.EnsureIndex()
	}
	m.mu.Lock()
	var schedulerCopy *Auth
	if fetched == nil {
		delete(m.auths, id)
	} else {
		cloned := fetched.Clone()
		m.auths[id] = cloned
		schedulerCopy = cloned.Clone()
	}
	m.mu.Unlock()
	if m.scheduler != nil {
		if schedulerCopy != nil {
			m.scheduler.upsertAuth(schedulerCopy)
		} else {
			m.scheduler.removeAuth(id)
		}
	}
	return nil
}

const maxShardPickAttempts = 64

func cloneTriedMap(src map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(src)+4)
	for id := range src {
		out[id] = struct{}{}
	}
	return out
}

func (m *Manager) pickWithShardFilter(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, tried map[string]struct{}) (*Auth, error) {
	if !m.IsAuthShardingEnabled() {
		selected, err := m.scheduler.pickSingle(ctx, provider, model, opts, tried)
		if err != nil && model != "" && shouldRetrySchedulerPick(err) {
			m.syncScheduler()
			selected, err = m.scheduler.pickSingle(ctx, provider, model, opts, tried)
		}
		return selected, err
	}
	shardTried := cloneTriedMap(tried)
	var selected *Auth
	var errPick error
	didModelSync := false
	for attempt := 0; attempt < maxShardPickAttempts; attempt++ {
		selected, errPick = m.scheduler.pickSingle(ctx, provider, model, opts, shardTried)
		if errPick != nil && model != "" && !didModelSync && shouldRetrySchedulerPick(errPick) {
			m.syncScheduler()
			didModelSync = true
			selected, errPick = m.scheduler.pickSingle(ctx, provider, model, opts, shardTried)
		}
		if errPick != nil || selected == nil {
			break
		}
		if m.OwnsAuth(selected.ID) {
			return selected, nil
		}
		shardTried[selected.ID] = struct{}{}
		selected = nil
	}
	if m.IsSpilloverEnabled() {
		globalSelected, globalErr := m.scheduler.pickSingle(ctx, provider, model, opts, tried)
		if globalErr == nil && globalSelected != nil {
			log.Warnf("cluster: spillover — local shard exhausted for provider=%s model=%s, using auth %s", provider, model, globalSelected.ID)
			return globalSelected, nil
		}
		return nil, globalErr
	}
	return selected, errPick
}

func (m *Manager) pickMixedWithShardFilter(ctx context.Context, providers []string, model string, opts cliproxyexecutor.Options, tried map[string]struct{}) (*Auth, string, error) {
	if !m.IsAuthShardingEnabled() {
		selected, provider, err := m.scheduler.pickMixed(ctx, providers, model, opts, tried)
		if err != nil && model != "" && shouldRetrySchedulerPick(err) {
			m.syncScheduler()
			selected, provider, err = m.scheduler.pickMixed(ctx, providers, model, opts, tried)
		}
		return selected, provider, err
	}
	shardTried := cloneTriedMap(tried)
	var selected *Auth
	var provider string
	var errPick error
	didModelSync := false
	for attempt := 0; attempt < maxShardPickAttempts; attempt++ {
		selected, provider, errPick = m.scheduler.pickMixed(ctx, providers, model, opts, shardTried)
		if errPick != nil && model != "" && !didModelSync && shouldRetrySchedulerPick(errPick) {
			m.syncScheduler()
			didModelSync = true
			selected, provider, errPick = m.scheduler.pickMixed(ctx, providers, model, opts, shardTried)
		}
		if errPick != nil || selected == nil {
			break
		}
		if m.OwnsAuth(selected.ID) {
			return selected, provider, nil
		}
		shardTried[selected.ID] = struct{}{}
		selected = nil
	}
	if m.IsSpilloverEnabled() {
		globalSelected, globalProvider, globalErr := m.scheduler.pickMixed(ctx, providers, model, opts, tried)
		if globalErr == nil && globalSelected != nil {
			log.Warnf("cluster: spillover — local shard exhausted for providers=%v model=%s, using auth %s", providers, model, globalSelected.ID)
			return globalSelected, globalProvider, nil
		}
		return nil, "", globalErr
	}
	return selected, provider, errPick
}

// NewManager constructs a manager with optional custom selector and hook.
func NewManager(store Store, selector Selector, hook Hook) *Manager {
	if selector == nil {
		selector = &RoundRobinSelector{}
	}
	if hook == nil {
		hook = NoopHook{}
	}
	manager := &Manager{
		store:                 store,
		executors:             make(map[string]ProviderExecutor),
		selector:              selector,
		hook:                  hook,
		auths:                 make(map[string]*Auth),
		homeRuntimeAuths:      make(map[string]map[string]*Auth),
		homeRuntimeAuthOwners: make(map[string]map[string]*HomeDispatchSelection),
		homeSessionSelections: make(map[string]map[homeSessionSelectionKey]*HomeDispatchSelection),
		providerOffsets:       make(map[string]int),
		modelPoolOffsets:      make(map[string]int),
	}
	// atomic.Value requires non-nil initial value.
	manager.runtimeConfig.Store(&internalconfig.Config{})
	manager.apiKeyModelAlias.Store(apiKeyModelAliasTable(nil))
	defaultInFlightConfig, errInFlightConfig := HomeInFlightPublisherConfigFromConfig(internalconfig.DefaultCredentialInFlightConfig())
	if errInFlightConfig == nil {
		manager.ApplyHomeInFlightPublisherConfig(defaultInFlightConfig)
	}
	manager.scheduler = newAuthScheduler(selector)
	return manager
}
