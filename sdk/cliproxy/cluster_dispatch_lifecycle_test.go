package cliproxy

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/cluster"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

type admissionObservingSelector struct {
	authority      *cluster.PgDispatchAuthority
	closedWhenStop chan bool
}

func (s *admissionObservingSelector) Pick(context.Context, string, string, cliproxyexecutor.Options, []*coreauth.Auth) (*coreauth.Auth, error) {
	return nil, nil
}

func (s *admissionObservingSelector) Stop() {
	_, admitted := s.authority.Admit("auth-a")
	s.closedWhenStop <- !admitted
}

func TestBootstrapClusterStrictDispatchWiresAuthorityAndJoiningLifecycle(t *testing.T) {
	db, control := openBootstrapTestDBWithStartupObserver(t, false)
	store := &clusterBootstrapStore{db: db, dsn: "postgres://strict-dispatch-lifecycle-test"}
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
			RingPollInterval:       "10ms",
		}},
		coreManager:              manager,
		clusterSubscriberFactory: newReadyTestSubscriber,
	}
	t.Cleanup(service.rollbackClusterBootstrap)

	bootstrapCtx := context.WithValue(context.Background(), bootstrapStartupContextKey{}, true)
	if errBootstrap := service.bootstrapCluster(bootstrapCtx); errBootstrap != nil {
		t.Fatalf("bootstrapCluster() error = %v", errBootstrap)
	}
	if service.clusterDispatchAuthority == nil {
		t.Fatal("bootstrap did not retain the PostgreSQL dispatch authority")
	}
	activateCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if errActivate := service.activateClusterServing(activateCtx); errActivate != nil {
		t.Fatalf("activateClusterServing() error = %v", errActivate)
	}
	if !service.clusterDispatchAuthority.Ready() || !service.clusterRegistrar.IsActive() {
		t.Fatal("serving activated before ring and dispatch authority were ready")
	}

	operations := control.snapshot()
	drainingIndex := slices.Index(operations, "routing:draining:sync")
	joiningIndex := slices.Index(operations, "routing:joining:async")
	activeIndex := slices.Index(operations, "routing:active:async")
	if drainingIndex < 0 || joiningIndex <= drainingIndex || activeIndex <= joiningIndex {
		t.Fatalf("routing lifecycle operations = %v, want draining -> joining -> active", operations)
	}
}

func TestBootstrapClusterDispatchAuthorityFactoryFailureRollsBack(t *testing.T) {
	db := openBootstrapTestDB(t, false)
	wantErr := errors.New("simulated dispatch authority construction failure")
	service := &Service{
		cfg: &config.Config{Cluster: internalconfig.ClusterConfig{
			Enabled:                true,
			NodeID:                 "node-a",
			Endpoint:               "http://127.0.0.1:8317",
			AuthSharding:           true,
			ProbeInterval:          "10ms",
			RegistrarInterval:      "1s",
			RingStalenessThreshold: "3s",
		}},
		coreManager: coreauth.NewManager(&clusterBootstrapStore{db: db, dsn: "postgres://factory-failure-test"}, nil, nil),
		clusterDispatchFactory: func(cluster.PgDispatchAuthorityConfig) (*cluster.PgDispatchAuthority, error) {
			return nil, wantErr
		},
	}

	errBootstrap := service.bootstrapCluster(context.Background())
	if !errors.Is(errBootstrap, wantErr) {
		t.Fatalf("bootstrapCluster() error = %v, want %v", errBootstrap, wantErr)
	}
	assertClusterBootstrapRolledBack(t, service)
}

func TestClusterActivationWaitIsBoundedWhenJoiningRefreshIsMissed(t *testing.T) {
	db := openBootstrapTestDB(t, false)
	service := &Service{
		cfg: &config.Config{Cluster: internalconfig.ClusterConfig{
			Enabled:                true,
			NodeID:                 "node-a",
			Endpoint:               "http://127.0.0.1:8317",
			AuthSharding:           true,
			ProbeInterval:          "10ms",
			RegistrarInterval:      "1s",
			RingStalenessThreshold: "3s",
			RingPollInterval:       "10ms",
		}},
		coreManager:              coreauth.NewManager(&clusterBootstrapStore{db: db, dsn: "postgres://bounded-activation-test"}, nil, nil),
		clusterSubscriberFactory: newReadyTestSubscriber,
	}
	t.Cleanup(service.rollbackClusterBootstrap)
	if errBootstrap := service.bootstrapCluster(context.Background()); errBootstrap != nil {
		t.Fatalf("bootstrapCluster() error = %v", errBootstrap)
	}

	activateCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	errActivate := service.activateClusterServing(activateCtx)
	if !errors.Is(errActivate, context.DeadlineExceeded) {
		t.Fatalf("activateClusterServing() error = %v, want bounded context deadline", errActivate)
	}
	if service.clusterRegistrar.IsActive() {
		t.Fatal("registrar became active without a current-epoch ring and dispatch reconciliation")
	}
}

func TestServiceShutdownReleasesDispatchLeaseBeforeClusterAndNodeLease(t *testing.T) {
	db, control := openBootstrapTestDBWithStartupObserver(t, false)
	ring := cluster.NewAuthRing("node-a")
	ring.RebuildAt(2, []cluster.RingMember{{NodeID: "node-a", Weight: 100}})
	authority, errAuthority := cluster.NewPgDispatchAuthority(cluster.PgDispatchAuthorityConfig{
		DB:            db,
		NodeID:        "node-a",
		Ring:          ring,
		RingStaleness: 3 * time.Second,
		AuthIDs:       func() []string { return []string{"auth-a"} },
	})
	if errAuthority != nil {
		t.Fatalf("NewPgDispatchAuthority() error = %v", errAuthority)
	}
	authority.Wake()
	readyCtx, readyCancel := context.WithTimeout(context.Background(), time.Second)
	defer readyCancel()
	if errReady := authority.WaitReady(readyCtx); errReady != nil {
		t.Fatalf("WaitReady() error = %v", errReady)
	}
	nodeLease, errLease := acquireClusterNodeLease(context.Background(), db, "node-a")
	if errLease != nil {
		t.Fatalf("acquireClusterNodeLease() error = %v", errLease)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetDispatchAuthority(authority)
	clusterCtx, clusterCancel := context.WithCancel(context.Background())
	service := &Service{
		coreManager:              manager,
		clusterAuthRing:          ring,
		clusterDispatchAuthority: authority,
		clusterNodeLease:         nodeLease,
		clusterCancel: func() {
			control.record("cluster-cancel")
			clusterCancel()
		},
	}

	if errShutdown := service.shutdown(context.Background()); errShutdown != nil {
		t.Fatalf("shutdown() error = %v", errShutdown)
	}
	if service.clusterDispatchAuthority != nil || service.clusterNodeLease != nil || service.clusterCancel != nil {
		t.Fatal("shutdown retained strict dispatch lifecycle resources")
	}
	select {
	case <-clusterCtx.Done():
	default:
		t.Fatal("shutdown did not cancel cluster coordination")
	}
	operations := control.snapshot()
	dispatchRelease := slices.Index(operations, "dispatch-release")
	clusterStop := slices.Index(operations, "cluster-cancel")
	nodeUnlock := slices.Index(operations, "node-unlock")
	if dispatchRelease < 0 || clusterStop <= dispatchRelease || nodeUnlock <= clusterStop {
		t.Fatalf("shutdown operations = %v, want dispatch release -> cluster cancel -> node unlock", operations)
	}
}

func TestClusterFatalClosesAdmissionsBeforeStoppingAutoRefresh(t *testing.T) {
	db := openBootstrapTestDB(t, false)
	ring := cluster.NewAuthRing("node-a")
	ring.RebuildAt(2, []cluster.RingMember{{NodeID: "node-a", Weight: 100}})
	authority, errAuthority := cluster.NewPgDispatchAuthority(cluster.PgDispatchAuthorityConfig{
		DB:            db,
		NodeID:        "node-a",
		Ring:          ring,
		RingStaleness: 3 * time.Second,
		AuthIDs:       func() []string { return []string{"auth-a"} },
	})
	if errAuthority != nil {
		t.Fatalf("NewPgDispatchAuthority() error = %v", errAuthority)
	}
	authority.Wake()
	readyCtx, readyCancel := context.WithTimeout(context.Background(), time.Second)
	defer readyCancel()
	if errReady := authority.WaitReady(readyCtx); errReady != nil {
		t.Fatalf("WaitReady() error = %v", errReady)
	}

	closedWhenStop := make(chan bool, 1)
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetSelector(&admissionObservingSelector{
		authority:      authority,
		closedWhenStop: closedWhenStop,
	})
	manager.SetDispatchAuthority(authority)
	service := &Service{
		coreManager:              manager,
		clusterAuthRing:          ring,
		clusterDispatchAuthority: authority,
		clusterErr:               make(chan error, 1),
	}

	service.reportClusterFatal(errors.New("simulated lease probe failure"), nil)

	select {
	case closed := <-closedWhenStop:
		if !closed {
			t.Fatal("auto-refresh stopped before dispatch admissions were closed")
		}
	default:
		t.Fatal("fatal cluster failure did not stop the auth auto-refresh worker")
	}
	if service.startCoreAutoRefresh(context.Background(), time.Minute) {
		t.Fatal("fatal cluster failure allowed the auth auto-refresh worker to restart")
	}
}

func TestClusterBootstrapRollbackClosesAdmissionsBeforeStoppingAutoRefresh(t *testing.T) {
	db := openBootstrapTestDB(t, false)
	ring := cluster.NewAuthRing("node-a")
	ring.RebuildAt(2, []cluster.RingMember{{NodeID: "node-a", Weight: 100}})
	authority, errAuthority := cluster.NewPgDispatchAuthority(cluster.PgDispatchAuthorityConfig{
		DB:            db,
		NodeID:        "node-a",
		Ring:          ring,
		RingStaleness: 3 * time.Second,
		AuthIDs:       func() []string { return []string{"auth-a"} },
	})
	if errAuthority != nil {
		t.Fatalf("NewPgDispatchAuthority() error = %v", errAuthority)
	}
	authority.Wake()
	readyCtx, readyCancel := context.WithTimeout(context.Background(), time.Second)
	defer readyCancel()
	if errReady := authority.WaitReady(readyCtx); errReady != nil {
		t.Fatalf("WaitReady() error = %v", errReady)
	}

	closedWhenStop := make(chan bool, 1)
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetSelector(&admissionObservingSelector{
		authority:      authority,
		closedWhenStop: closedWhenStop,
	})
	manager.SetDispatchAuthority(authority)
	service := &Service{
		coreManager:              manager,
		clusterAuthRing:          ring,
		clusterDispatchAuthority: authority,
	}

	service.rollbackClusterBootstrap()

	select {
	case closed := <-closedWhenStop:
		if !closed {
			t.Fatal("rollback stopped auto-refresh before dispatch admissions were closed")
		}
	default:
		t.Fatal("cluster bootstrap rollback did not stop the auth auto-refresh worker")
	}
}

func TestCoreAutoRefreshFatalStopLinearizesWithConcurrentStart(t *testing.T) {
	service := &Service{}
	startEntered := make(chan struct{})
	allowStart := make(chan struct{})
	startResult := make(chan bool, 1)
	go func() {
		startResult <- service.startCoreAutoRefreshWith(func() {
			close(startEntered)
			<-allowStart
		})
	}()

	select {
	case <-startEntered:
	case <-time.After(time.Second):
		t.Fatal("auto-refresh start did not enter")
	}

	service.lifecycleMu.Lock()
	service.clusterFatal = true
	service.lifecycleMu.Unlock()
	stopCalled := make(chan struct{})
	stopDone := make(chan struct{})
	go func() {
		service.stopCoreAutoRefreshWith(func() { close(stopCalled) })
		close(stopDone)
	}()

	select {
	case <-stopCalled:
		t.Fatal("fatal stop ran before the in-progress start completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(allowStart)

	select {
	case started := <-startResult:
		if started {
			t.Fatal("concurrent start reported success after fatal state was published")
		}
	case <-time.After(time.Second):
		t.Fatal("auto-refresh start did not finish")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("fatal stop did not run after startup completed")
	}

	var restarted atomic.Bool
	if service.startCoreAutoRefreshWith(func() { restarted.Store(true) }) {
		t.Fatal("fatal service allowed auto-refresh restart")
	}
	if restarted.Load() {
		t.Fatal("fatal service invoked auto-refresh start callback")
	}
}

func TestStrictClusterConfigurationRejectedBeforeStartup(t *testing.T) {
	tests := []struct {
		name       string
		cluster    internalconfig.ClusterConfig
		home       bool
		wantSubstr string
	}{
		{
			name:       "spillover",
			cluster:    internalconfig.ClusterConfig{Enabled: true, AuthSharding: true, Spillover: true},
			wantSubstr: "spillover must be false",
		},
		{
			name:       "home",
			cluster:    internalconfig.ClusterConfig{Enabled: true, AuthSharding: true},
			home:       true,
			wantSubstr: "incompatible with Home mode",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &Service{cfg: &config.Config{
				AuthDir: t.TempDir(),
				Cluster: tt.cluster,
			}}
			service.cfg.Home.Enabled = tt.home
			errRun := service.Run(context.Background())
			if errRun == nil || !strings.Contains(errRun.Error(), tt.wantSubstr) {
				t.Fatalf("Run() error = %v, want substring %q", errRun, tt.wantSubstr)
			}
		})
	}
}

func TestStrictClusterConfigurationIgnoredWhenClusterDisabled(t *testing.T) {
	service := &Service{cfg: &config.Config{
		Cluster: internalconfig.ClusterConfig{
			Enabled:      false,
			AuthSharding: true,
			Spillover:    true,
		},
	}}
	service.cfg.Home.Enabled = true
	if errStrict := service.validateStrictClusterMode(); errStrict != nil {
		t.Fatalf("validateStrictClusterMode() error = %v, want disabled cluster fields ignored", errStrict)
	}
}

func TestBootstrapClusterAuthShardingRejectsInvalidStrictDurations(t *testing.T) {
	tests := []struct {
		name       string
		configure  func(*internalconfig.ClusterConfig)
		wantSubstr string
	}{
		{"registrar", func(c *internalconfig.ClusterConfig) { c.RegistrarInterval = "invalid" }, "registrar-interval"},
		{"probe", func(c *internalconfig.ClusterConfig) { c.ProbeInterval = "0s" }, "probe-interval"},
		{"ring staleness", func(c *internalconfig.ClusterConfig) { c.RingStalenessThreshold = "invalid" }, "ring-staleness"},
		{"ring poll", func(c *internalconfig.ClusterConfig) { c.RingPollInterval = "-1s" }, "ring-poll-interval"},
		{
			"dispatch lease guard",
			func(c *internalconfig.ClusterConfig) {
				c.RegistrarInterval = "100ms"
				c.ProbeInterval = "100ms"
				c.RingStalenessThreshold = "1s"
			},
			"dispatch lease TTL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openBootstrapTestDB(t, false)
			clusterCfg := internalconfig.ClusterConfig{
				Enabled:                true,
				NodeID:                 "node-a",
				Endpoint:               "http://127.0.0.1:8317",
				AuthSharding:           true,
				ProbeInterval:          "10ms",
				RegistrarInterval:      "1s",
				RingStalenessThreshold: "3s",
			}
			tt.configure(&clusterCfg)
			service := &Service{
				cfg:         &config.Config{Cluster: clusterCfg},
				coreManager: coreauth.NewManager(&clusterBootstrapStore{db: db, dsn: "postgres://strict-duration-test"}, nil, nil),
			}
			errBootstrap := service.bootstrapCluster(context.Background())
			if errBootstrap == nil || !strings.Contains(errBootstrap.Error(), tt.wantSubstr) {
				t.Fatalf("bootstrapCluster() error = %v, want substring %q", errBootstrap, tt.wantSubstr)
			}
			assertClusterBootstrapRolledBack(t, service)
		})
	}
}
