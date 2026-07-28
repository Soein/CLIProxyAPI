// Package cliproxy provides the core service implementation for the CLI Proxy API.
// It includes service lifecycle management, authentication handling, file watching,
// and integration with various AI service providers through a unified interface.
package cliproxy

import (
	"context"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/api"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/home"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/homeplugins"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/pluginhost"
	internalusage "github.com/router-for-me/CLIProxyAPI/v7/internal/usage"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/watcher"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/wsrelay"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v7/sdk/access"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v7/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/cluster"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executionregistry"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	sdkpluginstore "github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginstore"
)

// Service wraps the proxy server lifecycle so external programs can embed the CLI proxy.
// It manages the complete lifecycle including authentication, file watching, HTTP server,
// and integration with various AI service providers.
type Service struct {
	// cfg holds the current application configuration.
	cfg *config.Config

	// cfgMu protects concurrent access to the configuration.
	cfgMu sync.RWMutex

	// configUpdateMu serializes config updates across watcher + home.
	configUpdateMu sync.Mutex

	// configRuntimeMu orders side-effecting runtime application after config commits.
	configRuntimeMu        sync.Mutex
	executorRegistrationMu sync.Mutex
	configSequence         uint64
	appliedRoutingState    *routingRuntimeState

	// configPath is the path to the configuration file.
	configPath string

	// tokenProvider handles loading token-based clients.
	tokenProvider TokenClientProvider

	// apiKeyProvider handles loading API key-based clients.
	apiKeyProvider APIKeyClientProvider

	// watcherFactory creates file watcher instances.
	watcherFactory WatcherFactory

	// hooks provides lifecycle callbacks.
	hooks Hooks

	// serverOptions contains additional server configuration options.
	serverOptions []api.ServerOption

	// server is the HTTP API server instance.
	server *api.Server

	// pprofServer manages the optional pprof HTTP debug server.
	pprofServer *pprofServer

	// serverErr channel for server startup/shutdown errors.
	serverErr chan error

	// watcher handles file system monitoring.
	watcher *WatcherWrapper

	// watcherCancel cancels the watcher context.
	watcherCancel context.CancelFunc

	// authUpdates channel for authentication updates.
	authUpdates chan watcher.AuthUpdate

	// authQueueStop cancels the auth update queue processing.
	authQueueStop context.CancelFunc

	// authManager handles legacy authentication operations.
	authManager *sdkAuth.Manager

	// accessManager handles request authentication providers.
	accessManager *sdkaccess.Manager

	// coreManager handles core authentication and execution.
	coreManager *coreauth.Manager

	// cooldownStateStore persists runtime cooldown state when enabled.
	cooldownStateStore coreauth.CooldownStateStore

	// pluginHost owns dynamic plugin lifecycle and runtime capability adapters.
	pluginHost *pluginhost.Host

	// wsGateway manages websocket Gemini providers.
	wsGateway *wsrelay.Manager

	// lifecycleMu serializes Run publication with shutdown initiation. Once
	// stopping is set, Run may not publish or start a new API server.
	lifecycleMu sync.Mutex
	runCancel   context.CancelFunc

	// stopping marks shutdown as in progress so worker startup can bail out.
	stopping bool

	// shutdownStarted guards the one-time shutdown path.
	shutdownStarted bool

	// shutdownDone is closed when shutdown finishes.
	shutdownDone chan struct{}

	// shutdownErr stores the first shutdown error.
	shutdownErr error

	// startupDone is closed when startup work completes.
	startupDone chan struct{}

	// startupDoneOnce closes startupDone exactly once.
	startupDoneOnce sync.Once

	// leaseReleaseOnce ensures the delayed node lease release path runs once.
	leaseReleaseOnce sync.Once

	// clusterCancel stops the cluster-mode goroutines (leader elector +
	// change subscriber). nil when cluster mode is disabled.
	clusterCancel context.CancelFunc

	// clusterErr carries fatal cluster lifecycle errors back to Run.
	clusterErr chan error

	// clusterNodeLease keeps the node-id advisory lock while cluster mode runs.
	clusterNodeLease *clusterNodeLease

	// clusterNodeLeaseProbeCancel stops the node-id lease probe loop.
	clusterNodeLeaseProbeCancel context.CancelFunc

	// clusterNodeLeaseProbeDone is closed when the node-id lease probe exits.
	clusterNodeLeaseProbeDone chan struct{}

	// clusterFatal marks a terminal cluster failure.
	clusterFatal bool

	// clusterFatalErr stores the first terminal cluster failure.
	clusterFatalErr error

	// fatalClusterCancel cancels the active cluster bootstrap context.
	fatalClusterCancel context.CancelFunc

	// clusterStopOnce closes the cluster dispatch authority once.
	clusterStopOnce sync.Once

	// clusterStopErr stores the first dispatch authority shutdown error.
	clusterStopErr error

	// coreAutoRefreshStartDone joins concurrent refresh startup and stop.
	coreAutoRefreshStartDone chan struct{}

	// clusterDispatchAuthority gates dispatch admission for strict cluster mode.
	clusterDispatchAuthority *cluster.PgDispatchAuthority

	// clusterDispatchFactory builds the dispatch authority used by cluster mode.
	clusterDispatchFactory func(cluster.PgDispatchAuthorityConfig) (*cluster.PgDispatchAuthority, error)

	// clusterSubscriberFactory overrides the default change subscriber.
	clusterSubscriberFactory func(string, cluster.Handlers) *cluster.ChangeSubscriber

	// clusterActivate runs the active cluster-serving path after bootstrap.
	clusterActivate func(context.Context) error

	// clusterRegistrar publishes this replica's routing metadata to the
	// shared cluster_nodes table.
	clusterRegistrar *cluster.InstanceRegistrar

	// clusterAuthRing is the per-auth consistent hash ring.
	clusterAuthRing *cluster.AuthRing

	// usageSink is the PG-backed usage statistics plugin.
	usageSink *internalusage.PGSink

	homeLifecycleMu              sync.Mutex
	homeOwnershipMu              sync.Mutex
	homeConfigCommitMu           sync.Mutex
	homeConfigStageHook          func()
	homeConfigCommitHook         func()
	homeConfigRuntimeHook        func()
	applyPprofConfigContextFn    func(context.Context, *config.Config) bool
	updateServerClientsContextFn func(context.Context, *config.Config) bool
	homeSupervisor               *homeSubscriberSupervisor
	homeMu                       sync.Mutex
	homeGeneration               uint64
	homeClient                   *home.Client
	homeRegistry                 *executionregistry.Registry
	homeDispatchBundle           *coreauth.HomeDispatchBundle
	homeDrainBound               time.Duration
	homeCancel                   context.CancelFunc
	homeLogForwarder             homeLogForwarder
	homeLogForwarderClient       *home.Client
	homePluginSyncMu             sync.Mutex
	homePluginSyncKey            string
	homePluginSyncFetch          func(context.Context, sdkpluginstore.PluginSyncRequest) (sdkpluginstore.PluginSyncResponse, error)
	homePluginDeleteTask         func(context.Context, *config.Config, home.PluginTask) homeplugins.SyncReport
}
