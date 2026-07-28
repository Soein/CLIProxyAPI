package cliproxy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/api"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/home"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/redisqueue"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v7/sdk/access"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v7/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	log "github.com/sirupsen/logrus"
)

// Run starts the service and blocks until the context is cancelled or the server stops.
// It initializes all components including authentication, file watching, HTTP server,
// and starts processing requests. The method blocks until the context is cancelled.
//
// Parameters:
//   - ctx: The context for controlling the service lifecycle
//
// Returns:
//   - error: An error if the service fails to start or run
func (s *Service) Run(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("cliproxy: service is nil")
	}
	if errStrict := s.validateStrictClusterMode(); errStrict != nil {
		return fmt.Errorf("cliproxy: invalid cluster configuration: %w", errStrict)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, runCancel := context.WithCancel(ctx)
	s.lifecycleMu.Lock()
	if s.stopping {
		s.lifecycleMu.Unlock()
		runCancel()
		return context.Canceled
	}
	if s.runCancel != nil {
		s.lifecycleMu.Unlock()
		runCancel()
		return errors.New("cliproxy: service is already running")
	}
	s.runCancel = runCancel
	s.startupDone = make(chan struct{})
	s.lifecycleMu.Unlock()
	defer func() {
		runCancel()
		s.lifecycleMu.Lock()
		s.runCancel = nil
		s.lifecycleMu.Unlock()
	}()
	ctx = runCtx

	usage.StartDefault(ctx)
	homeEnabled := s.cfg != nil && s.cfg.Home.Enabled
	if homeEnabled {
		forceHomeRuntimeConfig(s.cfg)
		redisqueue.SetUsageStatisticsEnabled(true)
	}

	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()
		if err := s.Shutdown(shutdownCtx); err != nil {
			log.Errorf("service shutdown returned error: %v", err)
		}
	}()
	defer s.markStartupDone()

	if !homeEnabled {
		if errEnsureAuthDir := s.ensureAuthDir(); errEnsureAuthDir != nil {
			return errEnsureAuthDir
		}
	}

	s.applyRetryConfig(s.cfg)
	s.configureCooldownStateStore(s.cfg)

	s.registerPluginAuthParser()
	if s.coreManager != nil && !homeEnabled {
		if errLoad := s.coreManager.Load(ctx); errLoad != nil {
			log.Warnf("failed to load auth store: %v", errLoad)
		}
		s.registerConfigAPIKeyAuths(coreauth.WithSkipPersist(ctx), s.cfg)
		if s.cfg.SaveCooldownStatus {
			if errRestoreCooldown := s.coreManager.RestoreCooldownStates(ctx); errRestoreCooldown != nil {
				log.Warnf("failed to restore cooldown state: %v", errRestoreCooldown)
			}
		}
	}

	// Cluster mode must fail closed: silently falling back to single-instance
	// behavior defeats auth ownership and refresh coordination guarantees.
	if s.cfg != nil && s.cfg.Cluster.Enabled && s.coreManager != nil {
		s.clusterErr = make(chan error, 1)
		if errBootstrap := s.bootstrapCluster(ctx); errBootstrap != nil {
			return fmt.Errorf("cliproxy: cluster bootstrap failed: %w", errBootstrap)
		}
	}

	if !homeEnabled {
		tokenResult, err := s.tokenProvider.Load(ctx, s.cfg)
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		if tokenResult == nil {
			tokenResult = &TokenClientResult{}
		}

		apiKeyResult, err := s.apiKeyProvider.Load(ctx, s.cfg)
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		if apiKeyResult == nil {
			apiKeyResult = &APIKeyClientResult{}
		}
	}

	// legacy clients removed; no caches to refresh

	s.ensureWebsocketGateway()
	if homeEnabled {
		s.registerAvailableExecutors(ctx, executorRegistrationOptions{
			includeBaseline: true,
		})
		// Home mode does not expose in-process Redis RESP usage output; usage is forwarded to home instead.
		redisqueue.SetEnabled(true)
	}

	// Publish the server under the lifecycle lock so Shutdown either observes
	// it or prevents this Run from starting it after shutdown cleanup.
	server := api.NewServer(s.cfg, s.coreManager, s.accessManager, s.configPath, s.serverOptions...)
	s.lifecycleMu.Lock()
	if s.stopping {
		s.lifecycleMu.Unlock()
		return context.Canceled
	}
	s.server = server
	s.lifecycleMu.Unlock()
	s.syncPluginRuntimeConfig(ctx)
	if homeEnabled {
		s.syncPluginModelRuntime(ctx)
	}

	if s.authManager == nil {
		s.authManager = newDefaultAuthManager()
	}

	if homeEnabled {
		s.startHomeSubscriber(ctx)
	}

	if s.wsGateway != nil {
		server.AttachWebsocketRoute(s.wsGateway.Path(), s.wsGateway.Handler())
		server.SetWebsocketAuthChangeHandler(func(oldEnabled, newEnabled bool) {
			if oldEnabled == newEnabled {
				return
			}
			if !oldEnabled && newEnabled {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if errStop := s.wsGateway.Stop(ctx); errStop != nil {
					log.Warnf("failed to reset websocket connections after ws-auth change %t -> %t: %v", oldEnabled, newEnabled, errStop)
					return
				}
				log.Debugf("ws-auth enabled; existing websocket sessions terminated to enforce authentication")
				return
			}
			log.Debugf("ws-auth disabled; existing websocket sessions remain connected")
		})
	}

	if s.hooks.OnBeforeStart != nil {
		s.hooks.OnBeforeStart(s.cfg)
	}
	if errRunCtx := ctx.Err(); errRunCtx != nil {
		return errRunCtx
	}
	select {
	case errCluster := <-s.clusterErr:
		return errCluster
	default:
	}

	s.lifecycleMu.Lock()
	if s.stopping || ctx.Err() != nil {
		s.lifecycleMu.Unlock()
		return context.Canceled
	}
	s.serverErr = make(chan error, 1)
	go func() {
		if errStart := server.Start(); errStart != nil {
			s.serverErr <- errStart
		} else {
			s.serverErr <- nil
		}
	}()
	s.lifecycleMu.Unlock()

	var startTerminationErr error
	runDone := ctx.Done()
	clusterErr := s.clusterErr
	waitingForStart := true
	for waitingForStart {
		select {
		case errStarted := <-server.Started():
			if startTerminationErr != nil {
				return startTerminationErr
			}
			if errStarted != nil {
				return errStarted
			}
			waitingForStart = false
		case <-runDone:
			if startTerminationErr == nil {
				startTerminationErr = ctx.Err()
			}
			runDone = nil
		case errCluster := <-clusterErr:
			if startTerminationErr == nil {
				startTerminationErr = errCluster
			}
			clusterErr = nil
		case errServer := <-s.serverErr:
			if startTerminationErr != nil {
				return startTerminationErr
			}
			if errFatal := s.currentClusterFatalError(); errFatal != nil {
				return errFatal
			}
			return errServer
		}
	}
	if errRunCtx := ctx.Err(); errRunCtx != nil {
		return errRunCtx
	}
	select {
	case errCluster := <-s.clusterErr:
		return errCluster
	default:
	}
	if errFatal := s.currentClusterFatalError(); errFatal != nil {
		return errFatal
	}
	if errActivate := s.activateClusterServing(ctx); errActivate != nil {
		if errStop := server.ForceStop(); errStop != nil {
			log.WithError(errStop).Warn("failed to force-stop API server after cluster activation failure")
		}
		return fmt.Errorf("cliproxy: activate cluster serving: %w", errActivate)
	}
	s.markStartupDone()
	fmt.Printf("API server started successfully on: %s:%d\n", s.cfg.Host, s.cfg.Port)

	s.applyPprofConfig(s.cfg)

	if s.hooks.OnAfterStart != nil {
		s.hooks.OnAfterStart(s)
	}

	if !homeEnabled {
		var watcherWrapper *WatcherWrapper
		reloadCallback := func(newCfg *config.Config) { s.applyWatcherConfigUpdate(newCfg) }

		watcherWrapper, errCreate := s.watcherFactory(s.configPath, s.cfg.AuthDir, reloadCallback)
		if errCreate != nil {
			return fmt.Errorf("cliproxy: failed to create watcher: %w", errCreate)
		}
		s.watcher = watcherWrapper
		s.ensureAuthUpdateQueue(ctx)
		if s.authUpdates != nil {
			watcherWrapper.SetAuthUpdateQueue(s.authUpdates)
		}
		watcherWrapper.SetConfig(s.cfg)
		s.registerPluginAuthParser()

		watcherCtx, watcherCancel := context.WithCancel(context.Background())
		s.watcherCancel = watcherCancel
		if errStart := watcherWrapper.Start(watcherCtx); errStart != nil {
			return fmt.Errorf("cliproxy: failed to start watcher: %w", errStart)
		}
		log.Info("file watcher started for config and auth directory changes")
		s.syncPluginModelRuntime(ctx)
	}

	s.registerModelRefreshCallback()

	// Prefer core auth manager auto refresh if available.
	if s.coreManager != nil && !homeEnabled {
		interval := 15 * time.Minute
		if s.startCoreAutoRefresh(context.Background(), interval) {
			log.Infof("core auth auto-refresh started (interval=%s)", interval)
		}
	}
	if errRunCtx := ctx.Err(); errRunCtx != nil {
		return errRunCtx
	}
	select {
	case errCluster := <-s.clusterErr:
		return errCluster
	default:
	}

	select {
	case <-ctx.Done():
		log.Debug("service context cancelled, shutting down...")
		return ctx.Err()
	case errCluster := <-s.clusterErr:
		return errCluster
	case errServer := <-s.serverErr:
		if errFatal := s.currentClusterFatalError(); errFatal != nil {
			return errFatal
		}
		return errServer
	}
}

// Shutdown gracefully stops background workers and the HTTP server.
// It ensures all resources are properly cleaned up and connections are closed.
// The shutdown is idempotent and can be called multiple times safely.
//
// Parameters:
//   - ctx: The context for controlling the shutdown timeout
//
// Returns:
//   - error: An error if shutdown fails
func (s *Service) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	s.lifecycleMu.Lock()
	s.stopping = true
	runCancel := s.runCancel
	startupDone := s.startupDone
	s.lifecycleMu.Unlock()
	if runCancel != nil {
		runCancel()
	}
	if startupDone != nil {
		select {
		case <-startupDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	s.lifecycleMu.Lock()
	if s.shutdownDone == nil {
		s.shutdownDone = make(chan struct{})
	}
	shutdownDone := s.shutdownDone
	if s.shutdownStarted {
		s.lifecycleMu.Unlock()
		select {
		case <-shutdownDone:
			s.lifecycleMu.Lock()
			errShutdown := s.shutdownErr
			s.lifecycleMu.Unlock()
			return errShutdown
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.shutdownStarted = true
	s.lifecycleMu.Unlock()

	shutdownErr := s.shutdown(ctx)
	s.lifecycleMu.Lock()
	s.shutdownErr = shutdownErr
	close(shutdownDone)
	s.lifecycleMu.Unlock()
	return shutdownErr
}

func (s *Service) markStartupDone() {
	if s == nil {
		return
	}
	s.lifecycleMu.Lock()
	startupDone := s.startupDone
	s.lifecycleMu.Unlock()
	if startupDone != nil {
		s.startupDoneOnce.Do(func() { close(startupDone) })
	}
}

func (s *Service) shutdown(ctx context.Context) error {
	var shutdownErr error
	s.lifecycleMu.Lock()
	s.stopping = true
	s.lifecycleMu.Unlock()

	s.homeLifecycleMu.Lock()
	if supervisor := s.homeSupervisor; supervisor != nil {
		s.homeConfigCommitMu.Lock()
		supervisor.cancel()
		s.homeConfigCommitMu.Unlock()
		<-supervisor.done
	}
	s.homeMu.Lock()
	homeCancel := s.homeCancel
	homeClient := s.homeClient
	homeRegistry := s.homeRegistry
	homeDispatchBundle := s.homeDispatchBundle
	homeForwarder := s.homeLogForwarder
	homeForwarderClient := s.homeLogForwarderClient
	s.homeGeneration++
	s.homeCancel = nil
	s.homeClient = nil
	s.homeRegistry = nil
	s.homeDispatchBundle = nil
	s.homeDrainBound = 0
	s.homeLogForwarder = nil
	s.homeLogForwarderClient = nil
	s.homeMu.Unlock()
	if s.coreManager != nil {
		s.coreManager.ClearHomeDispatchBundle(homeDispatchBundle)
	}
	home.ClearCurrentIf(homeClient)
	if homeCancel != nil {
		homeCancel()
	}
	if homeRegistry != nil {
		if errClose := homeRegistry.Close(); errClose != nil {
			log.WithError(errClose).Warn("failed to close Home execution registry during shutdown")
		}
	}
	if homeClient != nil {
		homeClient.Close()
	}
	if homeForwarder != nil {
		if homeForwarderClient == homeClient {
			homeForwarder.Deactivate(homeClient)
		}
		homeForwarder.Stop()
	}
	s.homeLifecycleMu.Unlock()

	// Close the dispatch start fence before invalidating scheduler ownership.
	s.closeClusterAdmissions()
	s.stopCoreAutoRefresh()
	// Stop selecting sharded auths before monitoring or leases are released.
	s.failCloseClusterServing()
	if s.watcherCancel != nil {
		s.watcherCancel()
	}
	if s.clusterRegistrar != nil {
		drainCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		if errDrain := s.clusterRegistrar.Drain(drainCtx); errDrain != nil {
			log.WithError(errDrain).Warn("cluster: drain notification failed; front-door will wait for staleness threshold")
		}
		cancel()
	}
	if s.usageSink != nil {
		drainCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		s.usageSink.Stop(drainCtx)
		cancel()
	}
	s.stopClusterNodeLeaseProbe()
	if s.watcher != nil {
		if err := s.watcher.Stop(); err != nil {
			log.Errorf("failed to stop file watcher: %v", err)
			shutdownErr = err
		}
	}
	if s.wsGateway != nil {
		if err := s.wsGateway.Stop(ctx); err != nil {
			log.Errorf("failed to stop websocket gateway: %v", err)
			if shutdownErr == nil {
				shutdownErr = err
			}
		}
	}
	if s.authQueueStop != nil {
		s.authQueueStop()
		s.authQueueStop = nil
	}

	if errShutdownPprof := s.shutdownPprof(ctx); errShutdownPprof != nil {
		log.Errorf("failed to stop pprof server: %v", errShutdownPprof)
		if shutdownErr == nil {
			shutdownErr = errShutdownPprof
		}
	}

	// no legacy clients to persist

	s.lifecycleMu.Lock()
	server := s.server
	s.lifecycleMu.Unlock()
	apiStopped := server == nil
	if server != nil {
		shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := server.Stop(shutdownCtx); err != nil {
			log.Errorf("error stopping API server: %v", err)
			if shutdownErr == nil {
				shutdownErr = err
			}
		}
		apiStopped = server.IsStopped()
		if !apiStopped && shutdownErr == nil {
			shutdownErr = errors.New("API server stop could not be confirmed")
		}
	}

	if apiStopped {
		if errFinalize := s.finalizeClusterStop(ctx); errFinalize != nil {
			log.WithError(errFinalize).Warn("cluster: failed to finalize dispatch authority")
			if shutdownErr == nil {
				shutdownErr = errFinalize
			}
		}
		if errRelease := s.releaseClusterNodeLease(ctx); errRelease != nil {
			log.WithError(errRelease).Warn("cluster: failed to release node-id lease")
			if shutdownErr == nil {
				shutdownErr = errRelease
			}
		}
	} else {
		s.lifecycleMu.Lock()
		clusterCoordinationRetained := s.clusterNodeLease != nil || s.clusterDispatchAuthority != nil || s.clusterCancel != nil
		s.lifecycleMu.Unlock()
		if clusterCoordinationRetained {
			log.Warn("cluster: retaining coordination and node leases until all API handlers exit")
			s.releaseClusterNodeLeaseAfterStop(server)
		}
	}

	if s.pluginHost != nil {
		sdktranslator.SetPluginHooks(nil)
		sdkAuth.RegisterPluginAuthParser(nil)
		if s.watcher != nil {
			s.watcher.SetPluginAuthParser(nil)
		}
		s.pluginHost.ApplyConfig(ctx, &config.Config{})
		s.pluginHost.RegisterModels(ctx, registry.GetGlobalRegistry())
		s.registerAvailableExecutors(ctx, executorRegistrationOptions{
			includePlugins: true,
		})
		s.pluginHost.RegisterFrontendAuthProviders()
		s.pluginHost.ShutdownAllContext(ctx)
		if s.accessManager != nil {
			s.accessManager.SetProviders(sdkaccess.RegisteredProviders())
		}
	}

	usage.StopDefault()
	return shutdownErr
}

func (s *Service) ensureAuthDir() error {
	info, err := os.Stat(s.cfg.AuthDir)
	if err != nil {
		if os.IsNotExist(err) {
			if mkErr := os.MkdirAll(s.cfg.AuthDir, 0o755); mkErr != nil {
				return fmt.Errorf("cliproxy: failed to create auth directory %s: %w", s.cfg.AuthDir, mkErr)
			}
			log.Infof("created missing auth directory: %s", s.cfg.AuthDir)
			return nil
		}
		return fmt.Errorf("cliproxy: error checking auth directory %s: %w", s.cfg.AuthDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("cliproxy: auth path exists but is not a directory: %s", s.cfg.AuthDir)
	}
	return nil
}
