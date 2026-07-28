package cliproxy

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgconn"
	internalapi "github.com/router-for-me/CLIProxyAPI/v7/internal/api"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v7/sdk/access"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/cluster"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

func TestServiceRunNodeLeaseProbeFailureFailsClosed(t *testing.T) {
	service, control := newProbeTestService(t)
	runErr := make(chan error, 1)
	go func() { runErr <- service.Run(context.Background()) }()

	waitForProbeStart(t, control)
	service.clusterAuthRing.Rebuild([]cluster.RingMember{{NodeID: "node-a", Weight: 100}})
	service.coreManager.SyncScheduler()
	if !service.clusterAuthRing.Ready() || !service.coreManager.OwnsAuth("auth-a") {
		t.Fatal("auth ring should own local auth before the lease probe fails")
	}

	close(control.failProbe)
	select {
	case errRun := <-runErr:
		if errRun == nil || !strings.Contains(errRun.Error(), "fatal cluster error") || !strings.Contains(errRun.Error(), "lease liveness probe") {
			t.Fatalf("Run() error = %v, want fatal node-id lease probe error", errRun)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not observe the node-id lease probe failure")
	}
	if service.clusterAuthRing.Ready() || service.coreManager.OwnsAuth("auth-a") {
		t.Fatal("node-id lease probe failure did not fail the local auth ring closed")
	}
	if service.clusterNodeLease != nil {
		t.Fatal("Run() returned before Shutdown released the node-id lease")
	}
}

func TestServiceRunCancellationDoesNotReportNodeLeaseFailure(t *testing.T) {
	service, control := newProbeTestService(t)
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- service.Run(ctx) }()

	waitForProbeStart(t, control)
	cancel()
	select {
	case errRun := <-runErr:
		if !errors.Is(errRun, context.Canceled) {
			t.Fatalf("Run() error = %v, want context.Canceled", errRun)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not stop after cancellation")
	}
	select {
	case errCluster := <-service.clusterErr:
		t.Fatalf("normal cancellation reported fatal cluster error: %v", errCluster)
	default:
	}
	if service.clusterNodeLease != nil {
		t.Fatal("Run() returned before Shutdown released the node-id lease")
	}
}

func TestServiceShutdownForceClosesRequestsBeforeReleasingNodeLease(t *testing.T) {
	listener, errListen := net.Listen("tcp", "127.0.0.1:0")
	if errListen != nil {
		t.Fatalf("net.Listen() error = %v", errListen)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if errClose := listener.Close(); errClose != nil {
		t.Fatalf("listener.Close() error = %v", errClose)
	}

	handlerStarted := make(chan struct{})
	handlerCanceled := make(chan struct{})
	handlerRelease := make(chan struct{})
	handlerExited := make(chan struct{})
	cfg := &config.Config{
		Host:    "127.0.0.1",
		Port:    port,
		AuthDir: t.TempDir(),
		Debug:   true,
		Cluster: internalconfig.ClusterConfig{
			Enabled:                true,
			RingStalenessThreshold: "300ms",
		},
	}
	server := internalapi.NewServer(
		cfg,
		coreauth.NewManager(nil, nil, nil),
		sdkaccess.NewManager(),
		t.TempDir()+"/config.yaml",
		internalapi.WithMiddleware(func(c *gin.Context) {
			if c.Request.URL.Path != "/healthz" {
				c.Next()
				return
			}
			close(handlerStarted)
			<-c.Request.Context().Done()
			close(handlerCanceled)
			<-handlerRelease
			close(handlerExited)
			c.Abort()
		}),
	)
	startErr := make(chan error, 1)
	go func() {
		startErr <- server.Start()
	}()
	select {
	case errStarted := <-server.Started():
		if errStarted != nil {
			t.Fatalf("Started() error = %v, want nil", errStarted)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not start")
	}

	db, unlockControl := openBootstrapTestDBWithUnlockObserver(t, handlerExited, server.IsStopped)
	lease, errAcquire := acquireClusterNodeLease(context.Background(), db, "node-shutdown-order")
	if errAcquire != nil {
		t.Fatalf("acquireClusterNodeLease() error = %v", errAcquire)
	}
	service := &Service{server: server, clusterNodeLease: lease, clusterErr: make(chan error, 1)}

	requestDone := make(chan error, 1)
	go func() {
		client := &http.Client{Transport: &http.Transport{Proxy: nil}}
		response, errGet := client.Get("http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(port)) + "/healthz")
		if response != nil {
			_ = response.Body.Close()
		}
		requestDone <- errGet
	}()
	select {
	case <-handlerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("blocking handler did not start")
	}

	clusterCtx, cancelCluster := context.WithCancel(context.Background())
	service.reportClusterFatal(errors.New("simulated lease probe failure"), cancelCluster)
	select {
	case <-handlerCanceled:
	case <-time.After(time.Second):
		t.Fatal("fatal lease probe did not force-cancel the active request")
	}
	select {
	case <-unlockControl.unlockObserved:
		t.Fatal("node-id lease was released while a canceled handler was still running")
	default:
	}
	if clusterCtx.Err() != nil {
		t.Fatal("fatal lease probe canceled cluster membership before the active handler drained")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	errShutdown := service.Shutdown(shutdownCtx)
	cancel()
	if !errors.Is(errShutdown, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() error = %v, want context deadline exceeded from graceful shutdown", errShutdown)
	}
	select {
	case <-unlockControl.unlockObserved:
		t.Fatal("node-id lease was released before the delayed handler exited")
	default:
	}
	if clusterCtx.Err() != nil {
		t.Fatal("cluster membership stopped during the bounded force-close window")
	}
	repeatErr := service.Shutdown(context.Background())
	if repeatErr == nil || repeatErr.Error() != errShutdown.Error() {
		t.Fatalf("repeated Shutdown() error = %v, want stable %v", repeatErr, errShutdown)
	}
	close(handlerRelease)
	select {
	case <-unlockControl.unlockObserved:
	case <-time.After(5 * time.Second):
		t.Fatal("node-id lease was not eventually released after the delayed handler exited")
	}
	select {
	case <-clusterCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("cluster membership was not stopped after the handler drained")
	}
	if unlockControl.unlockBeforeHandlerExit.Load() {
		t.Fatal("node-id lease was unlocked before the force-closed handler exited")
	}
	if unlockControl.unlockBeforeServerExit.Load() {
		t.Fatal("node-id lease was unlocked before the server serving loop exited")
	}
	select {
	case errStart := <-startErr:
		if errStart != nil {
			t.Fatalf("Start() error after Shutdown() = %v", errStart)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server serving loop did not exit before Shutdown() returned")
	}
	select {
	case <-requestDone:
	case <-time.After(5 * time.Second):
		t.Fatal("client request did not return after Shutdown()")
	}
}

func TestServiceShutdownDuringRunStartupPreventsServerPublication(t *testing.T) {
	provider := &blockingTokenProvider{
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
	service := &Service{
		cfg: &config.Config{
			Host:    "127.0.0.1",
			Port:    0,
			AuthDir: t.TempDir(),
		},
		configPath:     t.TempDir() + "/config.yaml",
		coreManager:    coreauth.NewManager(nil, nil, nil),
		accessManager:  sdkaccess.NewManager(),
		tokenProvider:  provider,
		apiKeyProvider: NewAPIKeyClientProvider(),
	}
	runErr := make(chan error, 1)
	go func() { runErr <- service.Run(context.Background()) }()
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("Run() did not reach the blocking startup provider")
	}

	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- service.Shutdown(context.Background()) }()
	select {
	case <-provider.canceled:
	case <-time.After(time.Second):
		t.Fatal("Shutdown() did not cancel the in-progress Run")
	}
	close(provider.release)
	select {
	case errShutdown := <-shutdownErr:
		if errShutdown != nil {
			t.Fatalf("Shutdown() during startup error = %v", errShutdown)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown() did not finish after startup unwound")
	}
	select {
	case errRun := <-runErr:
		if !errors.Is(errRun, context.Canceled) {
			t.Fatalf("Run() error = %v, want context.Canceled", errRun)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() continued after startup shutdown")
	}
	service.lifecycleMu.Lock()
	server := service.server
	service.lifecycleMu.Unlock()
	if server != nil {
		t.Fatal("Run() published an API server after Shutdown() began")
	}
}

func TestServiceRunWaitsForSubscriberReadyBeforePublishingServer(t *testing.T) {
	service, probeControl := newProbeTestService(t)
	resyncStarted := make(chan struct{})
	releaseResync := make(chan struct{})
	afterStart := make(chan struct{})
	service.hooks.OnAfterStart = func(*Service) { close(afterStart) }
	service.clusterSubscriberFactory = func(dsn string, handlers cluster.Handlers) *cluster.ChangeSubscriber {
		originalResync := handlers.OnResync
		handlers.OnResync = func(ctx context.Context) error {
			close(resyncStarted)
			select {
			case <-releaseResync:
				return originalResync(ctx)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return newReadyTestSubscriber(dsn, handlers)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- service.Run(ctx) }()
	select {
	case <-resyncStarted:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("subscriber did not begin its initial authoritative resync")
	}
	service.lifecycleMu.Lock()
	server := service.server
	service.lifecycleMu.Unlock()
	if server != nil {
		cancel()
		t.Fatal("API server was published before subscriber readiness")
	}
	if service.clusterRegistrar == nil || service.clusterRegistrar.IsActive() {
		cancel()
		t.Fatal("registrar became active before subscriber readiness and API startup")
	}
	select {
	case <-probeControl.probeStarted:
		cancel()
		t.Fatal("lease serving watchdog started before registrar activation")
	default:
	}
	close(releaseResync)
	select {
	case <-afterStart:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("service did not start after subscriber became ready")
	}
	if !service.clusterRegistrar.IsActive() {
		cancel()
		t.Fatal("registrar did not activate after API Started() succeeded")
	}
	waitForProbeStart(t, probeControl)
	cancel()
	select {
	case errRun := <-runErr:
		if !errors.Is(errRun, context.Canceled) {
			t.Fatalf("Run() error = %v, want context.Canceled", errRun)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not roll back canceled subscriber startup")
	}
	if service.clusterNodeLease != nil {
		t.Fatal("canceled subscriber startup retained the node lease")
	}
}

func TestServiceRunForceStopsListenerWhenClusterActivationFails(t *testing.T) {
	wantErr := errors.New("simulated cluster activation failure")
	activateCalled := make(chan struct{})
	service := &Service{
		cfg: &config.Config{
			Host:    "127.0.0.1",
			Port:    0,
			AuthDir: t.TempDir(),
		},
		configPath:     t.TempDir() + "/config.yaml",
		coreManager:    coreauth.NewManager(nil, nil, nil),
		accessManager:  sdkaccess.NewManager(),
		tokenProvider:  &recordingTokenProvider{},
		apiKeyProvider: NewAPIKeyClientProvider(),
		clusterActivate: func(context.Context) error {
			close(activateCalled)
			return wantErr
		},
	}
	errRun := service.Run(context.Background())
	if !errors.Is(errRun, wantErr) {
		t.Fatalf("Run() error = %v, want activation failure", errRun)
	}
	select {
	case <-activateCalled:
	default:
		t.Fatal("Run() did not attempt cluster activation after listener startup")
	}
	service.lifecycleMu.Lock()
	server := service.server
	service.lifecycleMu.Unlock()
	if server == nil || !server.IsStopped() {
		t.Fatal("activation failure returned before force-stopping the API server")
	}
}

func TestNodeLeaseProbeFailureCannotBeOverwrittenByDelayedRingRefresh(t *testing.T) {
	db, control := openBootstrapTestDBWithDelayedRingRefresh(t)
	store := &clusterBootstrapStore{db: db, dsn: "postgres://bootstrap-delayed-probe-test"}
	manager := coreauth.NewManager(store, nil, nil)
	service := &Service{
		cfg: &config.Config{Cluster: internalconfig.ClusterConfig{
			Enabled:                true,
			NodeID:                 "node-a",
			Endpoint:               "http://127.0.0.1:8317",
			AuthSharding:           true,
			ProbeInterval:          "10ms",
			RegistrarInterval:      "1s",
			RingStalenessThreshold: "3s",
			RingPollInterval:       "1s",
		}},
		coreManager:              manager,
		clusterSubscriberFactory: newReadyTestSubscriber,
	}
	t.Cleanup(service.rollbackClusterBootstrap)

	if errBootstrap := service.bootstrapCluster(context.Background()); errBootstrap != nil {
		t.Fatalf("bootstrapCluster() error = %v", errBootstrap)
	}
	// This test deliberately blocks the watcher's first database refresh. Seed
	// the same authoritative epoch so activation can reconcile independently;
	// the delayed watcher publication remains the behavior under test.
	service.clusterAuthRing.RebuildAt(2, []cluster.RingMember{{NodeID: "node-a", Weight: 100}})
	service.clusterDispatchAuthority.Wake()
	if errActivate := service.activateClusterServing(context.Background()); errActivate != nil {
		t.Fatalf("activateClusterServing() error = %v", errActivate)
	}
	waitForProbeStart(t, control)
	select {
	case <-control.ringRefreshStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("ring refresh did not enter the delayed query")
	}
	service.clusterAuthRing.Rebuild([]cluster.RingMember{{NodeID: "node-a", Weight: 100}})
	manager.SyncScheduler()
	close(control.failProbe)
	select {
	case <-service.clusterErr:
	case <-time.After(5 * time.Second):
		t.Fatal("lease probe failure was not reported")
	}
	control.releaseDelayedRingRefresh()
	select {
	case <-control.ringRefreshReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("delayed ring refresh did not return")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && service.clusterAuthRing.Ready() {
		time.Sleep(time.Millisecond)
	}
	// Give the watcher's OnChange callback time to run after scanning the row.
	time.Sleep(20 * time.Millisecond)
	if service.clusterAuthRing.Ready() || manager.OwnsAuth("auth-a") {
		t.Fatal("delayed ring refresh restored ownership after terminal fail-closed")
	}
}

func newProbeTestService(t *testing.T) (*Service, *bootstrapProbeControl) {
	t.Helper()
	db, control := openBootstrapTestDBWithProbe(t)
	store := &clusterBootstrapStore{db: db, dsn: "postgres://bootstrap-probe-test"}
	return &Service{
		cfg: &config.Config{
			Host:    "127.0.0.1",
			Port:    0,
			AuthDir: t.TempDir(),
			Cluster: internalconfig.ClusterConfig{
				Enabled:                true,
				NodeID:                 "node-a",
				Endpoint:               "http://127.0.0.1:8317",
				AuthSharding:           true,
				ProbeInterval:          "10ms",
				RegistrarInterval:      "1s",
				RingStalenessThreshold: "3s",
				RingPollInterval:       "1s",
			},
		},
		configPath:     t.TempDir() + "/config.yaml",
		coreManager:    coreauth.NewManager(store, nil, nil),
		accessManager:  sdkaccess.NewManager(),
		tokenProvider:  &recordingTokenProvider{},
		apiKeyProvider: NewAPIKeyClientProvider(),
		watcherFactory: func(string, string, func(*config.Config)) (*WatcherWrapper, error) {
			return &WatcherWrapper{}, nil
		},
		clusterSubscriberFactory: newReadyTestSubscriber,
	}, control
}

func waitForProbeStart(t *testing.T, control *bootstrapProbeControl) {
	t.Helper()
	select {
	case <-control.probeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("node-id lease liveness probe did not start")
	}
}

func TestServiceRunClusterBootstrapFailureFailsStartup(t *testing.T) {
	provider := &recordingTokenProvider{}
	manager := coreauth.NewManager(&plainBootstrapStore{}, nil, nil)
	service := &Service{
		cfg: &config.Config{
			AuthDir: t.TempDir(),
			Cluster: internalconfig.ClusterConfig{Enabled: true},
		},
		coreManager:   manager,
		tokenProvider: provider,
	}

	errRun := service.Run(context.Background())
	if errRun == nil || !strings.Contains(errRun.Error(), "cluster bootstrap") {
		t.Fatalf("Run() error = %v, want cluster bootstrap failure", errRun)
	}
	if provider.calls.Load() != 0 {
		t.Fatalf("token provider calls = %d, want 0 after cluster bootstrap failure", provider.calls.Load())
	}
}

func TestServiceRunClusterDisabledKeepsExistingStartupPath(t *testing.T) {
	wantErr := errors.New("token provider stopped startup")
	provider := &recordingTokenProvider{err: wantErr}
	manager := coreauth.NewManager(&plainBootstrapStore{}, nil, nil)
	service := &Service{
		cfg: &config.Config{
			AuthDir: t.TempDir(),
		},
		coreManager:   manager,
		tokenProvider: provider,
	}

	errRun := service.Run(context.Background())
	if !errors.Is(errRun, wantErr) {
		t.Fatalf("Run() error = %v, want %v", errRun, wantErr)
	}
	if provider.calls.Load() != 1 {
		t.Fatalf("token provider calls = %d, want 1 with cluster disabled", provider.calls.Load())
	}
}

func TestBootstrapClusterAuthShardingRequiresEndpointAndCleansUp(t *testing.T) {
	db := openBootstrapTestDB(t, false)
	store := &clusterBootstrapStore{db: db, dsn: "postgres://bootstrap-test"}
	service := &Service{
		cfg: &config.Config{Cluster: internalconfig.ClusterConfig{
			Enabled:      true,
			NodeID:       "node-a",
			AuthSharding: true,
		}},
		coreManager: coreauth.NewManager(store, nil, nil),
	}

	errBootstrap := service.bootstrapCluster(context.Background())
	if errBootstrap == nil || !strings.Contains(errBootstrap.Error(), "endpoint") {
		t.Fatalf("bootstrapCluster() error = %v, want missing endpoint", errBootstrap)
	}
	assertClusterBootstrapRolledBack(t, service)
}

func TestBootstrapClusterAuthShardingRequiresExplicitNodeID(t *testing.T) {
	db := openBootstrapTestDB(t, false)
	store := &clusterBootstrapStore{db: db, dsn: "postgres://bootstrap-test"}
	service := &Service{
		cfg: &config.Config{Cluster: internalconfig.ClusterConfig{
			Enabled:      true,
			Endpoint:     "http://127.0.0.1:8317",
			AuthSharding: true,
		}},
		coreManager: coreauth.NewManager(store, nil, nil),
	}

	errBootstrap := service.bootstrapCluster(context.Background())
	if errBootstrap == nil || !strings.Contains(errBootstrap.Error(), "explicit non-empty node-id") {
		t.Fatalf("bootstrapCluster() error = %v, want explicit node-id failure", errBootstrap)
	}
	assertClusterBootstrapRolledBack(t, service)
}

func TestBootstrapClusterAuthShardingRequiresUniqueEffectiveNodeID(t *testing.T) {
	db := openBootstrapTestDB(t, true)
	store := &clusterBootstrapStore{db: db, dsn: "postgres://bootstrap-test"}
	service := &Service{
		cfg: &config.Config{Cluster: internalconfig.ClusterConfig{
			Enabled:      true,
			NodeID:       "node-already-running",
			Endpoint:     "http://127.0.0.1:8317",
			AuthSharding: true,
		}},
		coreManager: coreauth.NewManager(store, nil, nil),
	}

	errBootstrap := service.bootstrapCluster(context.Background())
	if errBootstrap == nil || !strings.Contains(errBootstrap.Error(), "already leased") {
		t.Fatalf("bootstrapCluster() error = %v, want leased node-id failure", errBootstrap)
	}
	assertClusterBootstrapRolledBack(t, service)
}

func TestBootstrapClusterAuthShardingRejectsFreshActiveNodeID(t *testing.T) {
	db := openBootstrapTestDBWithActiveNode(t)
	store := &clusterBootstrapStore{db: db, dsn: "postgres://bootstrap-test"}
	service := &Service{
		cfg: &config.Config{Cluster: internalconfig.ClusterConfig{
			Enabled:      true,
			NodeID:       "node-already-active",
			Endpoint:     "http://127.0.0.1:8317",
			AuthSharding: true,
		}},
		coreManager: coreauth.NewManager(store, nil, nil),
	}

	errBootstrap := service.bootstrapCluster(context.Background())
	if errBootstrap == nil || !strings.Contains(errBootstrap.Error(), "already active") {
		t.Fatalf("bootstrapCluster() error = %v, want active node-id failure", errBootstrap)
	}
	assertClusterBootstrapRolledBack(t, service)
}

func TestBootstrapClusterAuthShardingPublishesDrainingSynchronouslyBeforeClusterLoops(t *testing.T) {
	db, control := openBootstrapTestDBWithStartupObserver(t, false)
	store := &clusterBootstrapStore{db: db, dsn: "postgres://startup-order-test"}
	service := &Service{
		cfg: &config.Config{Cluster: internalconfig.ClusterConfig{
			Enabled:                true,
			NodeID:                 "node-stale-row",
			Endpoint:               "http://127.0.0.1:8317",
			AuthSharding:           true,
			ProbeInterval:          "10ms",
			RegistrarInterval:      "1s",
			RingStalenessThreshold: "3s",
		}},
		coreManager:              coreauth.NewManager(store, nil, nil),
		clusterSubscriberFactory: newReadyTestSubscriber,
	}
	t.Cleanup(service.rollbackClusterBootstrap)

	ctx := context.WithValue(context.Background(), bootstrapStartupContextKey{}, true)
	if errBootstrap := service.bootstrapCluster(ctx); errBootstrap != nil {
		t.Fatalf("bootstrapCluster() error = %v", errBootstrap)
	}

	operations := control.snapshot()
	leaseIndex := slices.Index(operations, "node-lease")
	publishIndex := slices.Index(operations, "routing:draining:sync")
	if leaseIndex < 0 || publishIndex != leaseIndex+1 {
		t.Fatalf("startup operations = %v, want synchronous draining publish immediately after node lease", operations)
	}
	for _, operation := range operations[:publishIndex] {
		if operation == "leader-probe" || operation == "ring-refresh" || operation == "routing:draining:async" {
			t.Fatalf("startup operations = %v, cluster loop %q started before synchronous draining publish", operations, operation)
		}
	}
}

func TestBootstrapClusterAuthShardingFailsClosedWhenInitialDrainingPublishFails(t *testing.T) {
	db, control := openBootstrapTestDBWithStartupObserver(t, true)
	store := &clusterBootstrapStore{db: db, dsn: "postgres://startup-publish-failure-test"}
	service := &Service{
		cfg: &config.Config{Cluster: internalconfig.ClusterConfig{
			Enabled:                true,
			NodeID:                 "node-stale-row",
			Endpoint:               "http://127.0.0.1:8317",
			AuthSharding:           true,
			ProbeInterval:          "10ms",
			RegistrarInterval:      "1s",
			RingStalenessThreshold: "3s",
		}},
		coreManager:              coreauth.NewManager(store, nil, nil),
		clusterSubscriberFactory: newReadyTestSubscriber,
	}

	ctx := context.WithValue(context.Background(), bootstrapStartupContextKey{}, true)
	errBootstrap := service.bootstrapCluster(ctx)
	if errBootstrap == nil || !strings.Contains(errBootstrap.Error(), "initial draining") {
		t.Fatalf("bootstrapCluster() error = %v, want initial draining publication failure", errBootstrap)
	}
	operations := control.snapshot()
	if slices.Contains(operations, "leader-probe") || slices.Contains(operations, "ring-refresh") || slices.Contains(operations, "routing:draining:async") {
		t.Fatalf("cluster loops started after failed draining publication: %v", operations)
	}
	assertClusterBootstrapRolledBack(t, service)
}

func TestBootstrapClusterWithoutAuthShardingKeepsAsyncActiveRegistration(t *testing.T) {
	db, control := openBootstrapTestDBWithStartupObserver(t, true)
	store := &clusterBootstrapStore{db: db, dsn: "postgres://non-sharding-startup-test"}
	service := &Service{
		cfg: &config.Config{Cluster: internalconfig.ClusterConfig{
			Enabled:      true,
			NodeID:       "node-non-sharding",
			Endpoint:     "http://127.0.0.1:8317",
			Weight:       100,
			AuthSharding: false,
		}},
		coreManager:              coreauth.NewManager(store, nil, nil),
		clusterSubscriberFactory: newReadyTestSubscriber,
	}
	t.Cleanup(service.rollbackClusterBootstrap)

	ctx := context.WithValue(context.Background(), bootstrapStartupContextKey{}, true)
	if errBootstrap := service.bootstrapCluster(ctx); errBootstrap != nil {
		t.Fatalf("bootstrapCluster() error = %v, want legacy best-effort registration", errBootstrap)
	}
	if service.clusterRegistrar == nil || !service.clusterRegistrar.IsActive() {
		t.Fatal("non-sharding registrar did not preserve active startup state")
	}
	for _, operation := range control.snapshot() {
		if strings.HasPrefix(operation, "routing:") && strings.HasSuffix(operation, ":sync") {
			t.Fatalf("non-sharding bootstrap unexpectedly required synchronous routing publication: %v", control.snapshot())
		}
	}
}

func TestRollbackClusterBootstrapCancelsAndReleasesResources(t *testing.T) {
	db := openBootstrapTestDB(t, false)
	lease, errAcquire := acquireClusterNodeLease(context.Background(), db, "node-a")
	if errAcquire != nil {
		t.Fatalf("acquireClusterNodeLease() error = %v", errAcquire)
	}
	clusterCtx, cancel := context.WithCancel(context.Background())
	service := &Service{
		clusterCancel:    cancel,
		clusterNodeLease: lease,
		coreManager:      coreauth.NewManager(nil, nil, nil),
	}

	service.rollbackClusterBootstrap()

	select {
	case <-clusterCtx.Done():
	default:
		t.Fatal("cluster context remains active after rollback")
	}
	assertClusterBootstrapRolledBack(t, service)
}

func TestBootstrapClusterAuthShardingFailsClosedUntilRingIsReady(t *testing.T) {
	db := openBootstrapTestDB(t, false)
	store := &clusterBootstrapStore{db: db, dsn: "postgres://%"}
	manager := coreauth.NewManager(store, nil, nil)
	service := &Service{
		cfg: &config.Config{Cluster: internalconfig.ClusterConfig{
			Enabled:      true,
			NodeID:       "node-a",
			Endpoint:     "http://127.0.0.1:8317",
			AuthSharding: true,
		}},
		coreManager:              manager,
		clusterSubscriberFactory: newReadyTestSubscriber,
	}
	t.Cleanup(service.rollbackClusterBootstrap)

	if errBootstrap := service.bootstrapCluster(context.Background()); errBootstrap != nil {
		t.Fatalf("bootstrapCluster() error = %v", errBootstrap)
	}
	if service.clusterAuthRing == nil || service.clusterAuthRing.Ready() {
		t.Fatal("auth ring should remain unready before its first valid membership snapshot")
	}
	if manager.OwnsAuth("auth-a") {
		t.Fatal("auth sharding claimed an auth before ring readiness")
	}
}

func TestBootstrapClusterDoesNotBecomeReadyWhenInitialMirrorReconcileFails(t *testing.T) {
	db := openBootstrapTestDB(t, false)
	wantErr := errors.New("simulated full mirror reconcile failure")
	store := &clusterBootstrapStore{db: db, dsn: "postgres://mirror-ready-failure", mirrorFullErr: wantErr}
	service := &Service{
		cfg: &config.Config{Cluster: internalconfig.ClusterConfig{
			Enabled:                true,
			NodeID:                 "node-a",
			Endpoint:               "http://127.0.0.1:8317",
			AuthSharding:           true,
			RegistrarInterval:      "10ms",
			ProbeInterval:          "10ms",
			RingStalenessThreshold: "3s",
		}},
		coreManager:              coreauth.NewManager(store, nil, nil),
		clusterSubscriberFactory: newReadyTestSubscriber,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	errBootstrap := service.bootstrapCluster(ctx)
	if !errors.Is(errBootstrap, context.DeadlineExceeded) {
		t.Fatalf("bootstrapCluster() error = %v, want context deadline while readiness remains closed", errBootstrap)
	}
	if store.mirrorFullCalls.Load() == 0 {
		t.Fatal("initial subscriber resync did not invoke full mirror reconciliation")
	}
	assertClusterBootstrapRolledBack(t, service)
}

func assertClusterBootstrapRolledBack(t *testing.T, service *Service) {
	t.Helper()
	if service.clusterCancel != nil {
		t.Fatal("clusterCancel retained after failed bootstrap")
	}
	if service.clusterRegistrar != nil {
		t.Fatal("clusterRegistrar retained after failed bootstrap")
	}
	if service.clusterAuthRing != nil {
		t.Fatal("clusterAuthRing retained after failed bootstrap")
	}
	if service.clusterDispatchAuthority != nil {
		t.Fatal("clusterDispatchAuthority retained after failed bootstrap")
	}
	if service.clusterNodeLease != nil {
		t.Fatal("clusterNodeLease retained after failed bootstrap")
	}
}

type recordingTokenProvider struct {
	calls atomic.Int32
	err   error
}

type blockingTokenProvider struct {
	started    chan struct{}
	release    chan struct{}
	canceled   chan struct{}
	once       sync.Once
	cancelOnce sync.Once
}

type readyTestSubscriberConn struct{}

func (*readyTestSubscriberConn) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (*readyTestSubscriberConn) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*readyTestSubscriberConn) Close(context.Context) error { return nil }

func newReadyTestSubscriber(dsn string, handlers cluster.Handlers) *cluster.ChangeSubscriber {
	return &cluster.ChangeSubscriber{
		DSN:      dsn,
		Handlers: handlers,
		Connect: func(context.Context, string) (cluster.SubscriberConnection, error) {
			return &readyTestSubscriberConn{}, nil
		},
	}
}

func (p *blockingTokenProvider) Load(ctx context.Context, _ *config.Config) (*TokenClientResult, error) {
	p.once.Do(func() { close(p.started) })
	select {
	case <-ctx.Done():
		p.cancelOnce.Do(func() {
			if p.canceled != nil {
				close(p.canceled)
			}
		})
		<-p.release
	case <-p.release:
	}
	return &TokenClientResult{}, nil
}

func (p *recordingTokenProvider) Load(context.Context, *config.Config) (*TokenClientResult, error) {
	p.calls.Add(1)
	return &TokenClientResult{}, p.err
}

type plainBootstrapStore struct{}

func (*plainBootstrapStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }
func (*plainBootstrapStore) Save(context.Context, *coreauth.Auth) (string, error) {
	return "", nil
}
func (*plainBootstrapStore) Delete(context.Context, string) error { return nil }

type clusterBootstrapStore struct {
	db              *sql.DB
	dsn             string
	node            string
	mirrorFullErr   error
	mirrorFullCalls atomic.Int32
}

func (*clusterBootstrapStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }
func (*clusterBootstrapStore) Save(context.Context, *coreauth.Auth) (string, error) {
	return "", nil
}
func (*clusterBootstrapStore) Delete(context.Context, string) error              { return nil }
func (s *clusterBootstrapStore) DB() *sql.DB                                     { return s.db }
func (s *clusterBootstrapStore) DSN() string                                     { return s.dsn }
func (s *clusterBootstrapStore) SetNodeID(nodeID string)                         { s.node = nodeID }
func (*clusterBootstrapStore) ReconcileAuthMirror(context.Context, string) error { return nil }
func (s *clusterBootstrapStore) ReconcileAuthMirrors(context.Context) error {
	s.mirrorFullCalls.Add(1)
	return s.mirrorFullErr
}

var registerBootstrapDriver sync.Once

func openBootstrapTestDB(t *testing.T, rejectNodeLease bool) *sql.DB {
	t.Helper()
	registerBootstrapDriver.Do(func() {
		sql.Register("cliproxy-bootstrap-test", bootstrapTestDriver{})
	})
	dsn := "accept"
	if rejectNodeLease {
		dsn = "reject-node-lease"
	}
	db, errOpen := sql.Open("cliproxy-bootstrap-test", dsn)
	if errOpen != nil {
		t.Fatalf("sql.Open() error = %v", errOpen)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func openBootstrapTestDBWithActiveNode(t *testing.T) *sql.DB {
	t.Helper()
	registerBootstrapDriver.Do(func() {
		sql.Register("cliproxy-bootstrap-test", bootstrapTestDriver{})
	})
	db, errOpen := sql.Open("cliproxy-bootstrap-test", "active-node")
	if errOpen != nil {
		t.Fatalf("sql.Open() error = %v", errOpen)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func openBootstrapTestDBWithProbe(t *testing.T) (*sql.DB, *bootstrapProbeControl) {
	return openBootstrapTestDBWithProbeControl(t, "probe-test", false)
}

func openBootstrapTestDBWithDelayedRingRefresh(t *testing.T) (*sql.DB, *bootstrapProbeControl) {
	return openBootstrapTestDBWithProbeControl(t, "delayed-probe-test", true)
}

type bootstrapUnlockControl struct {
	handlerExited           <-chan struct{}
	serverStopped           func() bool
	unlockObserved          chan struct{}
	unlockBeforeHandlerExit atomic.Bool
	unlockBeforeServerExit  atomic.Bool
	unlockOnce              sync.Once
}

var bootstrapUnlockControls sync.Map

func openBootstrapTestDBWithUnlockObserver(t *testing.T, handlerExited <-chan struct{}, serverStopped func() bool) (*sql.DB, *bootstrapUnlockControl) {
	t.Helper()
	registerBootstrapDriver.Do(func() {
		sql.Register("cliproxy-bootstrap-test", bootstrapTestDriver{})
	})
	dsn := "unlock-observer-" + strings.ReplaceAll(t.Name(), "/", "-")
	control := &bootstrapUnlockControl{
		handlerExited:  handlerExited,
		serverStopped:  serverStopped,
		unlockObserved: make(chan struct{}),
	}
	bootstrapUnlockControls.Store(dsn, control)
	t.Cleanup(func() { bootstrapUnlockControls.Delete(dsn) })
	db, errOpen := sql.Open("cliproxy-bootstrap-test", dsn)
	if errOpen != nil {
		t.Fatalf("sql.Open() error = %v", errOpen)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, control
}

func openBootstrapTestDBWithProbeControl(t *testing.T, dsn string, delayRingRefresh bool) (*sql.DB, *bootstrapProbeControl) {
	t.Helper()
	registerBootstrapDriver.Do(func() {
		sql.Register("cliproxy-bootstrap-test", bootstrapTestDriver{})
	})
	control := &bootstrapProbeControl{
		probeStarted:        make(chan struct{}),
		failProbe:           make(chan struct{}),
		delayRingRefresh:    delayRingRefresh,
		ringRefreshStarted:  make(chan struct{}),
		releaseRingRefresh:  make(chan struct{}),
		ringRefreshReturned: make(chan struct{}),
	}
	t.Cleanup(control.releaseDelayedRingRefresh)
	bootstrapProbeControls.Store(dsn, control)
	t.Cleanup(func() { bootstrapProbeControls.Delete(dsn) })
	db, errOpen := sql.Open("cliproxy-bootstrap-test", dsn)
	if errOpen != nil {
		t.Fatalf("sql.Open() error = %v", errOpen)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, control
}

type bootstrapProbeControl struct {
	probeStarted        chan struct{}
	failProbe           chan struct{}
	startOnce           sync.Once
	delayRingRefresh    bool
	ringRefreshStarted  chan struct{}
	releaseRingRefresh  chan struct{}
	ringRefreshReturned chan struct{}
	ringStartOnce       sync.Once
	ringReturnOnce      sync.Once
	ringReleaseOnce     sync.Once
}

type bootstrapStartupContextKey struct{}

type bootstrapStartupControl struct {
	mu          sync.Mutex
	operations  []string
	failRouting bool
}

func (c *bootstrapStartupControl) record(operation string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.operations = append(c.operations, operation)
	c.mu.Unlock()
}

func (c *bootstrapStartupControl) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.operations...)
}

func (c *bootstrapProbeControl) releaseDelayedRingRefresh() {
	if c == nil || !c.delayRingRefresh {
		return
	}
	c.ringReleaseOnce.Do(func() { close(c.releaseRingRefresh) })
}

var bootstrapProbeControls sync.Map
var bootstrapStartupControls sync.Map

func openBootstrapTestDBWithStartupObserver(t *testing.T, failRouting bool) (*sql.DB, *bootstrapStartupControl) {
	t.Helper()
	registerBootstrapDriver.Do(func() {
		sql.Register("cliproxy-bootstrap-test", bootstrapTestDriver{})
	})
	dsn := "startup-observer-" + strings.ReplaceAll(t.Name(), "/", "-")
	control := &bootstrapStartupControl{failRouting: failRouting}
	bootstrapStartupControls.Store(dsn, control)
	t.Cleanup(func() { bootstrapStartupControls.Delete(dsn) })
	db, errOpen := sql.Open("cliproxy-bootstrap-test", dsn)
	if errOpen != nil {
		t.Fatalf("sql.Open() error = %v", errOpen)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, control
}

type bootstrapTestDriver struct{}

func (bootstrapTestDriver) Open(name string) (driver.Conn, error) {
	var probeControl *bootstrapProbeControl
	if control, ok := bootstrapProbeControls.Load(name); ok {
		probeControl, _ = control.(*bootstrapProbeControl)
	}
	var unlockControl *bootstrapUnlockControl
	if control, ok := bootstrapUnlockControls.Load(name); ok {
		unlockControl, _ = control.(*bootstrapUnlockControl)
	}
	var startupControl *bootstrapStartupControl
	if control, ok := bootstrapStartupControls.Load(name); ok {
		startupControl, _ = control.(*bootstrapStartupControl)
	}
	return &bootstrapTestConn{
		rejectNodeLease: name == "reject-node-lease",
		activeNode:      name == "active-node",
		probeControl:    probeControl,
		unlockControl:   unlockControl,
		startupControl:  startupControl,
	}, nil
}

type bootstrapTestConn struct {
	rejectNodeLease bool
	activeNode      bool
	nodeLease       bool
	probeControl    *bootstrapProbeControl
	unlockControl   *bootstrapUnlockControl
	startupControl  *bootstrapStartupControl
}

func (*bootstrapTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare not supported")
}
func (*bootstrapTestConn) Close() error { return nil }
func (*bootstrapTestConn) Begin() (driver.Tx, error) {
	return bootstrapTestTx{}, nil
}
func (*bootstrapTestConn) CheckNamedValue(*driver.NamedValue) error { return nil }
func (c *bootstrapTestConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if strings.Contains(query, "UPDATE auth_dispatch_leases") {
		c.startupControl.record("dispatch-release")
	}
	if c.startupControl != nil && strings.Contains(query, "endpoint") && strings.Contains(query, "status") {
		status, _ := args[len(args)-1].Value.(string)
		mode := "async"
		if ctx.Value(bootstrapStartupContextKey{}) == true {
			mode = "sync"
		}
		c.startupControl.record("routing:" + status + ":" + mode)
		if c.startupControl.failRouting {
			return nil, errors.New("simulated initial routing publication failure")
		}
	}
	return driver.RowsAffected(1), nil
}
func (c *bootstrapTestConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(query, "pg_try_advisory_lock") {
		gotLock := true
		isNodeLease := false
		if len(args) > 0 {
			switch lockClass := args[0].Value.(type) {
			case int32:
				isNodeLease = lockClass == clusterNodeLockClass
			case int64:
				isNodeLease = lockClass == int64(clusterNodeLockClass)
			}
		}
		if isNodeLease {
			c.startupControl.record("node-lease")
			c.nodeLease = true
			if c.rejectNodeLease {
				gotLock = false
			}
		} else {
			c.startupControl.record("leader-probe")
		}
		return &bootstrapTestRows{columns: []string{"locked"}, values: [][]driver.Value{{gotLock}}}, nil
	}
	if strings.Contains(query, "pg_advisory_unlock") {
		c.startupControl.record("node-unlock")
		if c.unlockControl != nil {
			select {
			case <-c.unlockControl.handlerExited:
			default:
				c.unlockControl.unlockBeforeHandlerExit.Store(true)
			}
			if c.unlockControl.serverStopped == nil || !c.unlockControl.serverStopped() {
				c.unlockControl.unlockBeforeServerExit.Store(true)
			}
			c.unlockControl.unlockOnce.Do(func() { close(c.unlockControl.unlockObserved) })
		}
		return &bootstrapTestRows{columns: []string{"unlocked"}, values: [][]driver.Value{{true}}}, nil
	}
	if strings.Contains(query, "SELECT EXISTS") {
		return &bootstrapTestRows{columns: []string{"exists"}, values: [][]driver.Value{{c.activeNode}}}, nil
	}
	if strings.Contains(query, "SELECT epoch, fingerprint, staleness_ms") {
		return &bootstrapTestRows{
			columns: []string{"epoch", "fingerprint", "staleness_ms"},
			values:  [][]driver.Value{{int64(1), []byte(nil), int64(0)}},
		}, nil
	}
	if strings.Contains(query, "LEFT JOIN granted") {
		c.startupControl.record("dispatch-acquire")
		var authID driver.Value
		var membershipEpoch driver.Value
		var ownerEpoch driver.Value
		var leaseUntil driver.Value
		if len(args) > 0 {
			if authIDs, ok := args[0].Value.([]string); ok && len(authIDs) > 0 {
				authID = authIDs[0]
				membershipEpoch = int64(2)
				ownerEpoch = int64(1)
				leaseUntil = time.Now().Add(3 * time.Second)
			}
		}
		return &bootstrapTestRows{
			columns: []string{"epoch", "now", "auth_id", "membership_epoch", "owner_epoch", "lease_until"},
			values:  [][]driver.Value{{int64(2), time.Now(), authID, membershipEpoch, ownerEpoch, leaseUntil}},
		}, nil
	}
	if strings.Contains(query, "FROM cluster_nodes") {
		c.startupControl.record("ring-refresh")
		if c.probeControl != nil {
			if c.probeControl.delayRingRefresh {
				c.probeControl.ringStartOnce.Do(func() { close(c.probeControl.ringRefreshStarted) })
				<-c.probeControl.releaseRingRefresh
				c.probeControl.ringReturnOnce.Do(func() { close(c.probeControl.ringRefreshReturned) })
			}
			return &bootstrapTestRows{
				columns: []string{"node_id", "weight", "endpoint"},
				values:  [][]driver.Value{{"node-a", int64(100), "http://127.0.0.1:8317"}},
			}, nil
		}
		if c.startupControl != nil {
			return &bootstrapTestRows{
				columns: []string{"node_id", "weight", "endpoint"},
				values:  [][]driver.Value{{"node-a", int64(100), "http://127.0.0.1:8317"}},
			}, nil
		}
		return &bootstrapTestRows{columns: []string{"node_id", "weight", "endpoint"}}, nil
	}
	if strings.TrimSpace(query) == "SELECT 1" && c.probeControl != nil && c.nodeLease {
		c.probeControl.startOnce.Do(func() { close(c.probeControl.probeStarted) })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.probeControl.failProbe:
			return nil, errors.New("simulated lease connection failure")
		}
	}
	if strings.TrimSpace(query) == "SELECT 1" {
		return &bootstrapTestRows{columns: []string{"value"}, values: [][]driver.Value{{int64(1)}}}, nil
	}
	return &bootstrapTestRows{columns: []string{"value"}}, nil
}

type bootstrapTestTx struct{}

func (bootstrapTestTx) Commit() error   { return nil }
func (bootstrapTestTx) Rollback() error { return nil }

type bootstrapTestRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *bootstrapTestRows) Columns() []string { return r.columns }
func (*bootstrapTestRows) Close() error        { return nil }
func (r *bootstrapTestRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

var _ driver.ExecerContext = (*bootstrapTestConn)(nil)
var _ driver.QueryerContext = (*bootstrapTestConn)(nil)
var _ driver.NamedValueChecker = (*bootstrapTestConn)(nil)
