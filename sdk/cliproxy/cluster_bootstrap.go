package cliproxy

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	internalusage "github.com/router-for-me/CLIProxyAPI/v7/internal/usage"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/cluster"
	log "github.com/sirupsen/logrus"
)

// clusterCapableStore is satisfied by stores able to share their connection
// pool with cluster coordinators. internal/store.PostgresStore implements it.
// Defined here as a local interface so sdk/cliproxy does not depend on the
// internal/store package concrete type.
type clusterCapableStore interface {
	DB() *sql.DB
	DSN() string
}

// nodeIDSetter is implemented by stores that record a writer identity in
// each UPSERT (for audit/troubleshooting in multi-node deployments).
type nodeIDSetter interface {
	SetNodeID(id string)
}

type clusterConfigSynchronizer interface {
	SyncConfigAuthoritative(ctx context.Context) error
}

const clusterNodeLockClass int32 = 3

const (
	dispatchLeaseMaximumTTL       = 15 * time.Second
	dispatchLeaseMinimumSafeGuard = 500 * time.Millisecond
)

// clusterNodeLease holds a session-level advisory lock for the lifetime of
// an auth-sharding replica. The active-row check below gives operators a
// useful stale-node error; this lock closes the concurrent-start race between
// that check and the first registrar heartbeat.
type clusterNodeLease struct {
	conn   *sql.Conn
	nodeID string
}

type clusterFreshnessBudgets struct {
	heartbeat   time.Duration
	ringRefresh time.Duration
}

// clusterFreshness tracks serving-path success using time values that retain
// Go's monotonic clock reading. It starts both clocks at bootstrap so an
// initially broken path receives the same bounded grace period as a later
// transient miss, rather than remaining unmonitored until its first success.
type clusterFreshness struct {
	mu              sync.Mutex
	lastHeartbeat   time.Time
	lastRingRefresh time.Time
}

func newClusterFreshness(now time.Time) *clusterFreshness {
	return &clusterFreshness{lastHeartbeat: now, lastRingRefresh: now}
}

func (f *clusterFreshness) recordHeartbeatAt(now time.Time) {
	if f == nil {
		return
	}
	f.mu.Lock()
	f.lastHeartbeat = now
	f.mu.Unlock()
}

func (f *clusterFreshness) recordRingRefreshAt(now time.Time) {
	if f == nil {
		return
	}
	f.mu.Lock()
	f.lastRingRefresh = now
	f.mu.Unlock()
}

func (f *clusterFreshness) check(now time.Time, budgets clusterFreshnessBudgets) error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	heartbeatAge := now.Sub(f.lastHeartbeat)
	ringRefreshAge := now.Sub(f.lastRingRefresh)
	f.mu.Unlock()
	if heartbeatAge >= budgets.heartbeat {
		return fmt.Errorf("registrar heartbeat freshness expired: age=%s budget=%s", heartbeatAge, budgets.heartbeat)
	}
	if ringRefreshAge >= budgets.ringRefresh {
		return fmt.Errorf("ring refresh freshness expired: age=%s budget=%s", ringRefreshAge, budgets.ringRefresh)
	}
	return nil
}

// bootstrapCluster wires LeaderElector + PgAuthRefreshLocker + ChangeSubscriber
// into the coreManager when cfg.Cluster.Enabled and the backing store is
// Postgres. Bootstrap is atomic: on any failure, all partially-created
// resources and manager wiring are rolled back before the error is returned.
// Idempotent guard: calling twice returns an error instead of leaking the
// previous cluster goroutines.
func (s *Service) bootstrapCluster(ctx context.Context) error {
	if s == nil || s.cfg == nil || s.coreManager == nil {
		return errors.New("cluster mode: service is not fully initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.lifecycleMu.Lock()
	clusterStarted := s.clusterCancel != nil
	s.lifecycleMu.Unlock()
	if clusterStarted {
		return errors.New("cluster mode: bootstrapCluster already called; would leak goroutines")
	}
	if errStrict := s.validateStrictClusterMode(); errStrict != nil {
		return errStrict
	}
	if s.clusterErr == nil {
		s.clusterErr = make(chan error, 1)
	}
	store := s.coreManager.GetStore()
	if store == nil {
		return errors.New("cluster mode: coreManager has no store wired")
	}
	pg, ok := store.(clusterCapableStore)
	if !ok {
		return fmt.Errorf("cluster mode requires a Postgres-backed store; got %T", store)
	}
	db := pg.DB()
	if db == nil {
		return errors.New("cluster mode: Postgres store returned nil *sql.DB")
	}
	dsn := strings.TrimSpace(pg.DSN())
	if dsn == "" {
		return errors.New("cluster mode: Postgres store returned empty DSN")
	}

	nodeID := strings.TrimSpace(s.cfg.Cluster.NodeID)
	if s.cfg.Cluster.AuthSharding && nodeID == "" {
		return errors.New("cluster mode: auth-sharding requires an explicit non-empty node-id")
	}
	if nodeID == "" {
		if h, err := os.Hostname(); err == nil && h != "" {
			nodeID = h
		} else {
			nodeID = "cliproxy-node"
		}
	}
	if nodeID == "" {
		return errors.New("cluster mode: effective node-id is empty")
	}

	regInterval, errDuration := parseClusterDuration(s.cfg.Cluster.RegistrarInterval, 10*time.Second, "registrar-interval", s.cfg.Cluster.AuthSharding)
	if errDuration != nil {
		return errDuration
	}
	ringStaleness, errDuration := parseClusterDuration(s.cfg.Cluster.RingStalenessThreshold, 30*time.Second, "ring-staleness", s.cfg.Cluster.AuthSharding)
	if errDuration != nil {
		return errDuration
	}
	ringPoll, errDuration := parseClusterDuration(s.cfg.Cluster.RingPollInterval, 30*time.Second, "ring-poll-interval", s.cfg.Cluster.AuthSharding)
	if errDuration != nil {
		return errDuration
	}
	// Default matches cluster.LeaderElector's own fallback; resolving it here
	// keeps both the startup log and the freshness timing math truthful.
	probeInterval, errDuration := parseClusterDuration(s.cfg.Cluster.ProbeInterval, 5*time.Second, "probe-interval", s.cfg.Cluster.AuthSharding)
	if errDuration != nil {
		return errDuration
	}
	leaseProbeInterval := clusterNodeLeaseProbeInterval(probeInterval, ringStaleness)
	var freshness *clusterFreshness
	var freshnessBudgets clusterFreshnessBudgets
	if s.cfg.Cluster.AuthSharding {
		if strings.TrimSpace(s.cfg.Cluster.Endpoint) == "" {
			return errors.New("cluster mode: auth-sharding requires a non-empty endpoint")
		}
		var errTiming error
		freshnessBudgets, errTiming = clusterFreshnessTiming(regInterval, leaseProbeInterval, ringStaleness)
		if errTiming != nil {
			return fmt.Errorf("cluster mode: unsafe auth-sharding timing: %w", errTiming)
		}
		if ringPoll > leaseProbeInterval {
			log.Infof("cluster: capping ring-poll-interval from %s to lease probe cadence %s for auth-sharding freshness", ringPoll, leaseProbeInterval)
			ringPoll = leaseProbeInterval
		}
		dispatchTTL := ringStaleness / 2
		if dispatchTTL > dispatchLeaseMaximumTTL {
			dispatchTTL = dispatchLeaseMaximumTTL
		}
		if dispatchTTL <= dispatchLeaseMinimumSafeGuard {
			return fmt.Errorf("cluster mode: ring-staleness %s yields dispatch lease TTL %s; TTL must exceed %s", ringStaleness, dispatchTTL, dispatchLeaseMinimumSafeGuard)
		}
		freshness = newClusterFreshness(time.Now())
		if err := ensureClusterNodeIDAvailable(ctx, db, nodeID, ringStaleness); err != nil {
			return err
		}
		lease, errAcquire := acquireClusterNodeLease(ctx, db, nodeID)
		if errAcquire != nil {
			return errAcquire
		}
		s.lifecycleMu.Lock()
		s.clusterNodeLease = lease
		s.lifecycleMu.Unlock()
	}

	// Establish a rollback boundary before wiring any manager state or
	// starting goroutines. Future bootstrap steps can safely return errors
	// without leaving this Service half-clustered.
	clusterCtx, cancel := context.WithCancel(context.Background())
	s.lifecycleMu.Lock()
	s.clusterCancel = cancel
	s.lifecycleMu.Unlock()
	committed := false
	defer func() {
		if committed {
			return
		}
		s.rollbackClusterBootstrap()
	}()

	// Identify this node on every row we write.
	if setter, ok := store.(nodeIDSetter); ok {
		setter.SetNodeID(nodeID)
	}

	// Phase 4: publish routing metadata to cluster_nodes. Absent endpoint is
	// allowed only without auth sharding: the node still participates in
	// leader election and refresh locks, but is invisible to front-door
	// routing. Sharding validates endpoint above and always registers. Its
	// initial draining row is published synchronously after acquiring the
	// node-id lease and before any cluster loop can refresh a stale active row.
	if strings.TrimSpace(s.cfg.Cluster.Endpoint) == "" {
		log.Warn("cluster: endpoint not configured; node will not be routable by front-door (Phase 4 routing disabled for this replica)")
	} else {
		registrar, err := cluster.NewRegistrar(cluster.RegistrarConfig{
			DB:            db,
			NodeID:        nodeID,
			Region:        s.cfg.Cluster.Region,
			Endpoint:      s.cfg.Cluster.Endpoint,
			Weight:        s.cfg.Cluster.Weight,
			Interval:      regInterval,
			StartDraining: s.cfg.Cluster.AuthSharding,
			OnActiveHeartbeat: func(startedAt time.Time) {
				freshness.recordHeartbeatAt(startedAt)
			},
		})
		if err != nil {
			return fmt.Errorf("cluster mode: instance registrar: %w", err)
		}
		s.lifecycleMu.Lock()
		s.clusterRegistrar = registrar
		s.lifecycleMu.Unlock()
		if s.cfg.Cluster.AuthSharding {
			if errPublish := registrar.Publish(ctx); errPublish != nil {
				return fmt.Errorf("cluster mode: initial draining routing publication: %w", errPublish)
			}
		}
	}

	// Leader elector — singleton background loops only run on leader. It must
	// start after the auth-sharding draining barrier above because its heartbeat
	// preserves routing columns while refreshing last_heartbeat.
	elector := cluster.New(cluster.Config{
		DB:       db,
		NodeID:   nodeID,
		Region:   s.cfg.Cluster.Region,
		Interval: probeInterval,
	})
	s.coreManager.SetLeaderGate(elector)
	go func() {
		if err := elector.Run(clusterCtx); err != nil && !errors.Is(err, context.Canceled) {
			log.WithError(err).Warn("cluster: leader elector exited")
		}
	}()

	// Per-auth advisory lock — serializes token refresh cross-replica.
	s.coreManager.SetAuthRefreshLocker(cluster.NewPgAuthRefreshLocker(db))
	if registrar := s.clusterRegistrarSnapshot(); registrar != nil {
		go registrar.Run(clusterCtx)
	}

	// Phase 4 Sprint 2: auth ring + watcher. We ALWAYS create the ring and
	// watcher, even when AuthSharding is off in config, so ring membership
	// observability is always available and flipping the flag later needs
	// only a restart (not a redeploy). The watcher is cheap: one LISTEN
	// connection + a 30s safety-net poll.
	//
	// Crucially, SetAuthShardingEnabled is driven by config and read ONCE
	// at bootstrap. A config hot-reload does NOT re-invoke these setters;
	// operators must restart the process to flip the flag. The config.yaml
	// comment spells this out, but enforcement via code watcher is a
	// follow-up (out of scope for Sprint 2).
	//
	// Bootstrap ordering: coreManager.Load() ran synchronously before this
	// method, so the scheduler initially contains all auths while the ring is
	// empty. AuthRing fails closed until its first valid snapshot; synchronize
	// immediately after enabling sharding so no request can escape through
	// that bootstrap window. The watcher's OnChange restores the local shard.
	ring := cluster.NewAuthRing(nodeID)
	s.lifecycleMu.Lock()
	s.clusterAuthRing = ring
	s.lifecycleMu.Unlock()
	s.coreManager.SetAuthRing(ring)
	s.coreManager.SetAuthShardingEnabled(s.cfg.Cluster.AuthSharding)
	s.coreManager.SetSpilloverEnabled(s.cfg.Cluster.Spillover)
	var dispatchAuthority *cluster.PgDispatchAuthority
	if s.cfg.Cluster.AuthSharding {
		s.coreManager.SyncScheduler()
		authorityFactory := s.clusterDispatchFactory
		if authorityFactory == nil {
			authorityFactory = cluster.NewPgDispatchAuthority
		}
		var errAuthority error
		dispatchAuthority, errAuthority = authorityFactory(cluster.PgDispatchAuthorityConfig{
			DB:            db,
			NodeID:        nodeID,
			Ring:          ring,
			RingStaleness: ringStaleness,
			AuthIDs:       s.coreManager.ListDispatchAuthIDs,
		})
		if errAuthority != nil {
			return fmt.Errorf("cluster mode: dispatch authority: %w", errAuthority)
		}
		s.lifecycleMu.Lock()
		s.clusterDispatchAuthority = dispatchAuthority
		s.lifecycleMu.Unlock()
		s.coreManager.SetDispatchAuthority(dispatchAuthority)
	}

	if ringStaleness <= regInterval*2 {
		log.Warnf("cluster: ring-staleness=%s is <= 2×registrar-interval=%s; instances may flap out of the ring during heartbeats",
			ringStaleness, regInterval*2)
	}
	watcher := &cluster.RingWatcher{
		DB:                 db,
		DSN:                dsn,
		Ring:               ring,
		StalenessThreshold: ringStaleness,
		PollInterval:       ringPoll,
		RequireAuthority:   s.cfg.Cluster.AuthSharding,
		OnChange: func() {
			// Kick the scheduler so newly-owned auths pick up traffic and
			// newly-lost auths stop receiving it. Cheap: runs under the
			// scheduler mutex, no network calls.
			s.coreManager.SyncScheduler()
			if dispatchAuthority != nil {
				dispatchAuthority.Wake()
			}
		},
		OnRefresh: func(startedAt time.Time) {
			freshness.recordRingRefreshAt(startedAt)
		},
	}
	go watcher.Run(clusterCtx)

	// LISTEN/NOTIFY subscriber — peers reload changed rows and periodically
	// perform a full reconciliation to heal reconnect/drop gaps.
	subscriber := &cluster.ChangeSubscriber{
		DSN: dsn,
		Handlers: cluster.Handlers{
			OnAuthChanged: func(subCtx context.Context, authID string) error {
				log.Debugf("cluster: auth_changed notify id=%s; reloading", authID)
				// Reload the authoritative row and converge the Service-owned
				// executor/model/session state as well as Manager scheduling.
				if _, errReload := s.reloadAuthRuntimeByID(subCtx, authID); errReload != nil {
					return errReload
				}
				s.coreManager.RefreshAPIKeyModelAlias()
				s.syncPluginRuntime(coreauth.WithSkipPersist(subCtx))
				return nil
			},
			OnConfigChanged: func(subCtx context.Context) error {
				return s.syncClusterConfig(subCtx, store)
			},
			OnResync: func(subCtx context.Context) error {
				// LISTEN/NOTIFY is edge-triggered and its bounded queue may
				// drop bursts. A full load after reconnect and periodically
				// reconciles missed auth and config updates and deletions,
				// including Service-owned executors/models/sessions.
				return errors.Join(
					s.reconcileAuthRuntime(subCtx),
					s.reconcileAuthMirrors(subCtx),
					s.syncClusterConfig(subCtx, store),
				)
			},
		},
	}
	if s.clusterSubscriberFactory != nil {
		subscriber = s.clusterSubscriberFactory(dsn, subscriber.Handlers)
		if subscriber == nil {
			return errors.New("cluster mode: change subscriber factory returned nil")
		}
	}
	subscriberReady := subscriber.Ready()
	subscriberStartupErr := subscriber.StartupErrors()
	go subscriber.Run(clusterCtx)
	select {
	case <-subscriberReady:
	case errStartup := <-subscriberStartupErr:
		return fmt.Errorf("cluster mode: change subscriber startup: %w", errStartup)
	case errCluster := <-s.clusterErr:
		return errCluster
	case <-ctx.Done():
		return ctx.Err()
	}
	activate := func(runCtx context.Context) error {
		if !s.cfg.Cluster.AuthSharding {
			return nil
		}
		startupTimeout := clusterStartupReconcileTimeout(ringPoll)
		startupCtx, startupCancel := context.WithTimeout(runCtx, startupTimeout)
		defer startupCancel()
		registrar := s.clusterRegistrarSnapshot()
		if registrar == nil {
			return errors.New("cluster mode: auth-sharding registrar is unavailable")
		}
		if errJoin := registrar.Join(startupCtx); errJoin != nil {
			return fmt.Errorf("publish joining state: %w", errJoin)
		}
		dispatchAuthority.Wake()
		if errReady := dispatchAuthority.WaitReady(startupCtx); errReady != nil {
			return fmt.Errorf("wait for current-epoch dispatch authority reconciliation (timeout %s): %w", startupTimeout, errReady)
		}
		if errActivate := registrar.Activate(startupCtx); errActivate != nil {
			return errActivate
		}
		s.startClusterNodeLeaseProbe(runCtx, clusterCtx, cancel, leaseProbeInterval, freshness, freshnessBudgets)
		return nil
	}
	s.lifecycleMu.Lock()
	s.clusterActivate = activate
	s.lifecycleMu.Unlock()

	// PG-backed usage statistics sink. Skipped when usage.backend=memory
	// (the default). The sink registers as a coreusage.Plugin alongside
	// the existing in-memory LoggerPlugin so writes go to both during
	// rollout (mode=dual) before the read path flips to PG.
	uc := s.cfg.Usage.WithDefaults()
	if usageSink, errSink := internalusage.AttachPGSink(
		clusterCtx, db, nodeID, uc.Backend,
		internalusage.PGSinkOptions{
			BatchSize:        uc.FlushBatchSize,
			FlushInterval:    internalusage.ParseFlushInterval(uc.FlushInterval),
			MaxBufferEvents:  50000,
			MaxBufferRollups: 10000,
		},
	); errSink != nil {
		log.Errorf("usage: AttachPGSink failed: %v", errSink)
	} else if usageSink != nil {
		s.usageSink = usageSink
	}

	// Leader-gated TTL cleanup. Spawned even on followers — the cleanup
	// loop ticks everywhere but only the leader's tick performs the
	// actual DELETE. This avoids a special-case "is this the leader?"
	// at bootstrap when leadership can change later. Cheap when idle
	// (one ticker + a leader gate check per hour).
	if uc.Backend != "memory" {
		cleanup := &internalusage.Cleanup{
			Store:         internalusage.NewPGStore(db),
			IsLeader:      func() bool { return s.coreManager.IsLeader() },
			EventTTLDays:  uc.EventRetentionDays,
			RollupTTLDays: uc.RollupRetentionDays,
			Interval:      time.Hour,
		}
		go cleanup.Run(clusterCtx)
		log.Infof("usage cleanup: leader-gated, interval=1h event_ttl=%dd rollup_ttl=%dd",
			uc.EventRetentionDays, uc.RollupRetentionDays)
	}

	log.WithFields(log.Fields{
		"node_id":        nodeID,
		"region":         s.cfg.Cluster.Region,
		"probe_interval": probeInterval,
		"auth_sharding":  s.cfg.Cluster.AuthSharding,
		"ring_staleness": ringStaleness,
		"ring_poll":      ringPoll,
		"usage_backend":  uc.Backend,
	}).Info("cluster mode enabled")
	committed = true
	return nil
}

func (s *Service) activateClusterServing(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.lifecycleMu.Lock()
	activate := s.clusterActivate
	s.lifecycleMu.Unlock()
	if activate == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return activate(ctx)
}

func clusterStartupReconcileTimeout(ringPoll time.Duration) time.Duration {
	timeout := 2*ringPoll + 5*time.Second
	if timeout < 10*time.Second {
		return 10 * time.Second
	}
	if timeout > 30*time.Second {
		return 30 * time.Second
	}
	return timeout
}

func (s *Service) syncClusterConfig(ctx context.Context, store any) error {
	synchronizer, ok := store.(clusterConfigSynchronizer)
	if !ok || synchronizer == nil {
		return nil
	}
	if err := synchronizer.SyncConfigAuthoritative(ctx); err != nil {
		return err
	}
	// The subscriber can establish LISTEN before the file watcher is built.
	// Calling this on every reconciliation lets the first post-start tick load
	// the mirrored config even when its contents were already current on disk.
	if s != nil && s.watcher != nil {
		s.reloadConfigFromWatcher()
	}
	return nil
}

func ensureClusterNodeIDAvailable(ctx context.Context, db *sql.DB, nodeID string, staleness time.Duration) error {
	var active bool
	err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM cluster_nodes
			WHERE node_id = $1
			  AND status = 'active'
			  AND last_heartbeat > NOW() - $2::interval
		)
	`, nodeID, staleness.String()).Scan(&active)
	if err != nil {
		return fmt.Errorf("cluster mode: validate unique node-id %q: %w", nodeID, err)
	}
	if active {
		return fmt.Errorf("cluster mode: unique node-id %q is already active; wait for ring-staleness or choose another node-id", nodeID)
	}
	return nil
}

func acquireClusterNodeLease(ctx context.Context, db *sql.DB, nodeID string) (*clusterNodeLease, error) {
	conn, errConn := db.Conn(ctx)
	if errConn != nil {
		return nil, fmt.Errorf("cluster mode: acquire connection for unique node-id %q: %w", nodeID, errConn)
	}
	var acquired bool
	errLock := conn.QueryRowContext(ctx,
		"SELECT pg_try_advisory_lock($1::integer, hashtext($2))",
		clusterNodeLockClass, nodeID,
	).Scan(&acquired)
	if errLock != nil {
		discardSQLConn(conn)
		return nil, fmt.Errorf("cluster mode: acquire unique node-id %q lease: %w", nodeID, errLock)
	}
	if !acquired {
		_ = conn.Close()
		return nil, fmt.Errorf("cluster mode: unique node-id %q is already leased by another replica", nodeID)
	}
	return &clusterNodeLease{conn: conn, nodeID: nodeID}, nil
}

func (l *clusterNodeLease) release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}
	var unlocked bool
	errUnlock := l.conn.QueryRowContext(ctx,
		"SELECT pg_advisory_unlock($1::integer, hashtext($2))",
		clusterNodeLockClass, l.nodeID,
	).Scan(&unlocked)
	if errUnlock != nil || !unlocked {
		// The server may still hold the session lock when the unlock result is
		// ambiguous. Mark the driver connection bad so database/sql discards it
		// instead of returning a potentially locked session to the pool.
		discardSQLConn(l.conn)
		l.conn = nil
		if errUnlock != nil {
			return fmt.Errorf("unlock node-id %q: %w", l.nodeID, errUnlock)
		}
		return fmt.Errorf("unlock node-id %q: advisory lock was not held", l.nodeID)
	}
	errClose := l.conn.Close()
	l.conn = nil
	if errClose != nil {
		return fmt.Errorf("close node-id %q connection: %w", l.nodeID, errClose)
	}
	return nil
}

// monitor periodically proves both the exact database session holding the
// advisory lease and the independent registrar/ring query paths are healthy.
// Re-acquiring the advisory lock here would increment PostgreSQL's re-entrant
// lock count and hide a dead lease, so its direct probe is a bounded SELECT 1.
func (l *clusterNodeLease) monitor(ctx context.Context, interval time.Duration, freshness *clusterFreshness, budgets clusterFreshnessBudgets) error {
	if l == nil || l.conn == nil {
		return errors.New("node-id lease connection is unavailable")
	}
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// Check before the bounded query so a slow-but-successful SELECT 1
			// cannot add another full probe interval to freshness detection.
			if errFreshness := clusterFreshnessError(ctx, l.nodeID, freshness, budgets); errFreshness != nil {
				return errFreshness
			}
			probeCtx, cancel := context.WithTimeout(ctx, interval)
			var one int
			errProbe := l.conn.QueryRowContext(probeCtx, "SELECT 1").Scan(&one)
			cancel()
			if errProbe != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("node-id %q lease liveness probe: %w", l.nodeID, errProbe)
			}
			if one != 1 {
				return fmt.Errorf("node-id %q lease liveness probe returned %d, want 1", l.nodeID, one)
			}
			// Recheck after success to catch freshness that expired while the
			// query was in flight without waiting for the next ticker.
			if errFreshness := clusterFreshnessError(ctx, l.nodeID, freshness, budgets); errFreshness != nil {
				return errFreshness
			}
		}
	}
}

func clusterFreshnessError(ctx context.Context, nodeID string, freshness *clusterFreshness, budgets clusterFreshnessBudgets) error {
	if ctx.Err() != nil {
		return nil
	}
	if errFreshness := freshness.check(time.Now(), budgets); errFreshness != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("node-id %q serving-path watchdog: %w", nodeID, errFreshness)
	}
	return nil
}

// clusterNodeLeaseProbeInterval keeps both the cadence and per-query timeout
// comfortably below the membership staleness window. A lost lease therefore
// fails this replica closed before peers may treat its old heartbeat as stale.
func clusterNodeLeaseProbeInterval(configured, ringStaleness time.Duration) time.Duration {
	interval := configured
	if interval <= 0 {
		interval = 5 * time.Second
	}
	// Keep a full probe period of margin beyond the four periods needed for
	// two ring polls, grace, and final observation. The timing validator below
	// also rejects equality at the staleness boundary.
	if ceiling := ringStaleness / 5; ceiling > 0 && interval > ceiling {
		interval = ceiling
	}
	return interval
}

// clusterFreshnessTiming tolerates one complete miss on each independent
// serving path. With registrar cadence R and watchdog cadence P:
//
//   - heartbeat budget = 2R+P/2; observed fatal before 2R+3P/2
//   - ring budget      = 3P;     observed fatal before 4P
//
// Both upper bounds must fit inside ring staleness so this node stops serving
// strictly before peers may evict its last heartbeat. The additional P/2 in
// the heartbeat budget prevents scheduling order at the second-success
// boundary from turning one transient miss into a false fatal.
func clusterFreshnessTiming(registrarInterval, probeInterval, ringStaleness time.Duration) (clusterFreshnessBudgets, error) {
	if registrarInterval <= 0 || probeInterval <= 0 || ringStaleness <= 0 {
		return clusterFreshnessBudgets{}, errors.New("registrar, probe, and ring-staleness intervals must all be positive")
	}
	if probeInterval > ringStaleness/4 {
		return clusterFreshnessBudgets{}, fmt.Errorf("4×lease-probe-interval exceeds ring-staleness (%s > %s)", probeInterval, ringStaleness/4)
	}
	if 4*probeInterval >= ringStaleness {
		return clusterFreshnessBudgets{}, fmt.Errorf("4×lease-probe-interval=%s must be strictly less than ring-staleness=%s", 4*probeInterval, ringStaleness)
	}
	if registrarInterval >= ringStaleness/2 {
		return clusterFreshnessBudgets{}, fmt.Errorf("registrar-interval=%s leaves no two-heartbeat room inside ring-staleness=%s", registrarInterval, ringStaleness)
	}
	heartbeatHeadroom := ringStaleness - 2*registrarInterval
	heartbeatObservationSlack := probeInterval + probeInterval/2
	if heartbeatObservationSlack >= heartbeatHeadroom {
		return clusterFreshnessBudgets{}, fmt.Errorf("2×registrar-interval + 3×lease-probe-interval/2 must be strictly less than ring-staleness (headroom %s, need > %s)", heartbeatHeadroom, heartbeatObservationSlack)
	}
	return clusterFreshnessBudgets{
		heartbeat:   2*registrarInterval + probeInterval/2,
		ringRefresh: 3 * probeInterval,
	}, nil
}

func (s *Service) startClusterNodeLeaseProbe(runCtx, clusterCtx context.Context, cancelCluster context.CancelFunc, interval time.Duration, freshness *clusterFreshness, budgets clusterFreshnessBudgets) {
	if s == nil {
		return
	}
	s.lifecycleMu.Lock()
	lease := s.clusterNodeLease
	s.lifecycleMu.Unlock()
	if lease == nil {
		return
	}
	probeCtx, cancel := context.WithCancel(clusterCtx)
	done := make(chan struct{})
	s.lifecycleMu.Lock()
	s.clusterNodeLeaseProbeCancel = cancel
	s.clusterNodeLeaseProbeDone = done
	s.lifecycleMu.Unlock()
	stopOnRunCancel := context.AfterFunc(runCtx, func() {
		s.failCloseClusterServing()
		cancel()
	})

	go func() {
		defer close(done)
		defer func() {
			if runCtx.Err() != nil {
				s.failCloseClusterServing()
			}
			stopOnRunCancel()
		}()
		if errProbe := lease.monitor(probeCtx, interval, freshness, budgets); errProbe != nil {
			if probeCtx.Err() != nil || runCtx.Err() != nil {
				return
			}
			s.reportClusterFatal(errProbe, cancelCluster)
		}
	}()
}

func (s *Service) reportClusterFatal(errCluster error, cancelCluster context.CancelFunc) {
	if s == nil || errCluster == nil {
		return
	}
	// Stop new dispatch starts before invalidating scheduler ownership. Already
	// admitted work keeps its grant until the API has drained.
	s.closeClusterAdmissions()
	fatalErr := fmt.Errorf("cliproxy: fatal cluster error: %w", errCluster)
	s.lifecycleMu.Lock()
	server := s.server
	s.clusterFatal = true
	if s.clusterFatalErr == nil {
		s.clusterFatalErr = fatalErr
	}
	s.fatalClusterCancel = cancelCluster
	s.lifecycleMu.Unlock()
	// Publishing the terminal state before stopping linearizes against a
	// concurrent startup: either startup finishes first and this stop wins, or
	// startup observes clusterFatal and never creates the worker.
	s.stopCoreAutoRefresh()
	// Invalidate ownership before notifying Run. Even if shutdown takes time,
	// no request or refresh may continue using the stale shard assignment.
	s.failCloseClusterServing()
	if server != nil {
		// Stop admission and cancel active HTTP request contexts before the
		// replacement can acquire this node's lease. Handlers that ignore
		// cancellation remain tracked, and keep the lease until they return.
		if errStop := server.ForceStop(); errStop != nil {
			log.WithError(errStop).Warn("cluster: failed to force-stop API server after fatal lease probe")
		}
	}
	if server == nil || server.IsStopped() {
		if errStop := s.finalizeClusterStop(context.Background()); errStop != nil {
			log.WithError(errStop).Warn("cluster: failed to finalize dispatch authority after fatal stop")
		}
	}
	select {
	case s.clusterErr <- fatalErr:
	default:
		log.WithError(fatalErr).Error("cluster: additional fatal error dropped")
	}
}

func (s *Service) closeClusterAdmissions() {
	if s == nil {
		return
	}
	s.lifecycleMu.Lock()
	authority := s.clusterDispatchAuthority
	s.lifecycleMu.Unlock()
	if authority != nil {
		authority.CloseAdmissions()
	}
}

func (s *Service) failCloseClusterServing() {
	if s == nil {
		return
	}
	s.lifecycleMu.Lock()
	ring := s.clusterAuthRing
	s.lifecycleMu.Unlock()
	if ring != nil {
		ring.FailClosed()
	}
	if s.coreManager != nil {
		s.coreManager.SyncScheduler()
	}
}

func (s *Service) stopClusterNodeLeaseProbe() {
	if s == nil {
		return
	}
	s.lifecycleMu.Lock()
	cancelProbe := s.clusterNodeLeaseProbeCancel
	probeDone := s.clusterNodeLeaseProbeDone
	s.clusterNodeLeaseProbeCancel = nil
	s.clusterNodeLeaseProbeDone = nil
	s.lifecycleMu.Unlock()
	if cancelProbe != nil {
		cancelProbe()
	}
	if probeDone != nil {
		<-probeDone
	}
}

func discardSQLConn(conn *sql.Conn) {
	if conn == nil {
		return
	}
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	_ = conn.Close()
}

func (s *Service) rollbackClusterBootstrap() {
	if s == nil {
		return
	}
	s.closeClusterAdmissions()
	s.stopCoreAutoRefresh()
	s.failCloseClusterServing()
	s.lifecycleMu.Lock()
	cancelCluster := s.clusterCancel
	s.clusterCancel = nil
	s.fatalClusterCancel = nil
	s.lifecycleMu.Unlock()
	if cancelCluster != nil {
		cancelCluster()
	}
	s.stopClusterNodeLeaseProbe()
	s.lifecycleMu.Lock()
	authority := s.clusterDispatchAuthority
	s.clusterDispatchAuthority = nil
	s.lifecycleMu.Unlock()
	if authority != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if errClose := authority.Close(closeCtx); errClose != nil {
			log.WithError(errClose).Warn("cluster: failed to close dispatch authority after bootstrap failure")
		}
		cancel()
	}
	if s.coreManager != nil {
		s.coreManager.SetDispatchAuthority(nil)
	}
	if s.usageSink != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		s.usageSink.Stop(stopCtx)
		cancel()
		s.usageSink = nil
	}
	s.lifecycleMu.Lock()
	lease := s.clusterNodeLease
	s.clusterNodeLease = nil
	s.lifecycleMu.Unlock()
	if lease != nil {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := lease.release(releaseCtx); err != nil {
			log.WithError(err).Warn("cluster: failed to release node-id lease after bootstrap failure")
		}
		cancel()
	}
	s.lifecycleMu.Lock()
	s.clusterRegistrar = nil
	s.clusterAuthRing = nil
	s.clusterActivate = nil
	s.lifecycleMu.Unlock()
	if s.coreManager != nil {
		s.coreManager.SetLeaderGate(nil)
		s.coreManager.SetAuthRefreshLocker(nil)
		s.coreManager.SetAuthRing(nil)
		s.coreManager.SetAuthShardingEnabled(false)
		s.coreManager.SetSpilloverEnabled(false)
		s.coreManager.SyncScheduler()
	}
}

func (s *Service) clusterRegistrarSnapshot() *cluster.InstanceRegistrar {
	if s == nil {
		return nil
	}
	s.lifecycleMu.Lock()
	registrar := s.clusterRegistrar
	s.lifecycleMu.Unlock()
	return registrar
}

func (s *Service) validateStrictClusterMode() error {
	if s == nil || s.cfg == nil || !s.cfg.Cluster.Enabled || !s.cfg.Cluster.AuthSharding {
		return nil
	}
	if s.cfg.Cluster.Spillover {
		return errors.New("cluster mode: auth-sharding requires strict dispatch ownership; spillover must be false")
	}
	if s.cfg.Home.Enabled {
		return errors.New("cluster mode: auth-sharding is incompatible with Home mode")
	}
	return nil
}

func parseClusterDuration(raw string, defaultValue time.Duration, fieldName string, strict bool) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return defaultValue, nil
	}
	d, err := time.ParseDuration(trimmed)
	if err == nil && d > 0 {
		return d, nil
	}
	if strict {
		return 0, fmt.Errorf("cluster mode: auth-sharding requires a positive %s duration; got %q", fieldName, raw)
	}
	log.Warnf("cluster: invalid %s %q; using default %s", fieldName, raw, defaultValue)
	return defaultValue, nil
}

// parseDurationOr parses a Go duration string; on empty/invalid input it
// returns defaultValue and logs a warning naming fieldName so operators can
// grep the startup log for "invalid <field>".
func parseDurationOr(raw string, defaultValue time.Duration, fieldName string) time.Duration {
	d, _ := parseClusterDuration(raw, defaultValue, fieldName, false)
	return d
}
