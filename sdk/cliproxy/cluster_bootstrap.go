package cliproxy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/cluster"
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

// bootstrapCluster wires LeaderElector + PgAuthRefreshLocker + ChangeSubscriber
// into the coreManager when cfg.Cluster.Enabled and the backing store is
// Postgres. On any failure the caller falls back to single-instance mode.
// Idempotent guard: calling twice returns an error instead of leaking the
// previous cluster goroutines.
func (s *Service) bootstrapCluster(ctx context.Context) error {
	if s.clusterCancel != nil {
		return errors.New("cluster mode: bootstrapCluster already called; would leak goroutines")
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
	if nodeID == "" {
		if h, err := os.Hostname(); err == nil && h != "" {
			nodeID = h
		} else {
			nodeID = "cliproxy-node"
		}
	}

	// Default matches cluster.LeaderElector's own fallback; we resolve it
	// here so the startup log is truthful ("probe_interval=5s") even when
	// the user left the field empty.
	probeInterval := 5 * time.Second
	if v := strings.TrimSpace(s.cfg.Cluster.ProbeInterval); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			probeInterval = d
		} else {
			log.Warnf("cluster: invalid probe-interval %q; using default %s", v, probeInterval)
		}
	}

	// Identify this node on every row we write.
	if setter, ok := store.(nodeIDSetter); ok {
		setter.SetNodeID(nodeID)
	}

	// Cancellable context so Shutdown can tear down the coordinator
	// goroutines without waiting for the service-wide ctx to be cancelled.
	clusterCtx, cancel := context.WithCancel(context.Background())
	s.clusterCancel = cancel

	// Leader elector — singleton background loops only run on leader.
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

	// Phase 4: publish routing metadata to cluster_nodes. Absent endpoint is
	// treated as "opt out of cross-instance routing" — the node still
	// participates in leader election and refresh locks, it is just
	// invisible to new-api's consistent-hash router.
	regInterval := parseDurationOr(s.cfg.Cluster.RegistrarInterval, 10*time.Second, "registrar-interval")
	if strings.TrimSpace(s.cfg.Cluster.Endpoint) == "" {
		log.Warn("cluster: endpoint not configured; node will not be routable by front-door (Phase 4 routing disabled for this replica)")
	} else {
		registrar, err := cluster.NewRegistrar(cluster.RegistrarConfig{
			DB:       db,
			NodeID:   nodeID,
			Region:   s.cfg.Cluster.Region,
			Endpoint: s.cfg.Cluster.Endpoint,
			Weight:   s.cfg.Cluster.Weight,
			Interval: regInterval,
		})
		if err != nil {
			return fmt.Errorf("cluster mode: instance registrar: %w", err)
		}
		s.clusterRegistrar = registrar
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
	// Bootstrap ordering: coreManager.Load() ran synchronously before
	// bootstrapCluster() — so the scheduler holds ALL auths when we
	// reach this point, while the ring is empty. That "full scheduler +
	// empty ring" state is correct, not a bug:
	//   - AuthSharding=false: ring membership doesn't matter, scheduler
	//     serves everything (legacy behavior).
	//   - AuthSharding=true:  ring.Ready()==false makes IsMine=true (the
	//     deliberate bootstrap-window degradation), so the scheduler
	//     still serves everything until the watcher completes its first
	//     refresh and fires OnChange → SyncScheduler rebuilds with the
	//     real membership. The window is ~goroutine-schedule + one PG
	//     RTT = low milliseconds on a healthy system.
	ring := cluster.NewAuthRing(nodeID)
	s.clusterAuthRing = ring
	s.coreManager.SetAuthRing(ring)
	s.coreManager.SetAuthShardingEnabled(s.cfg.Cluster.AuthSharding)

	ringStaleness := parseDurationOr(s.cfg.Cluster.RingStalenessThreshold, 30*time.Second, "ring-staleness")
	ringPoll := parseDurationOr(s.cfg.Cluster.RingPollInterval, 30*time.Second, "ring-poll-interval")
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
		OnChange: func() {
			// Kick the scheduler so newly-owned auths pick up traffic and
			// newly-lost auths stop receiving it. Cheap: runs under the
			// scheduler mutex, no network calls.
			s.coreManager.SyncScheduler()
		},
	}
	go watcher.Run(clusterCtx)

	// LISTEN/NOTIFY subscriber — peers reload changed rows.
	// Phase 3 initial integration: trigger a full Load on any auth change.
	// This is pragmatic (O(all auths) per notify) but correct; switching to
	// precise per-ID reload is a Phase 3b optimization.
	subscriber := &cluster.ChangeSubscriber{
		DSN: dsn,
		Handlers: cluster.Handlers{
			OnAuthChanged: func(subCtx context.Context, authID string) error {
				log.Debugf("cluster: auth_changed notify id=%s; reloading", authID)
				// ReloadByID performs a cheap indexed fetch when the
				// store supports it (Postgres does) and falls back to
				// full Load for stores that do not. Either way the
				// in-memory map + scheduler converge.
				return s.coreManager.ReloadByID(subCtx, authID)
			},
			OnConfigChanged: func(subCtx context.Context) error {
				// Config hot reload across replicas is Phase 3b; for now
				// we just log — existing per-instance watcher still picks
				// up filesystem config changes.
				log.Info("cluster: config_changed notify received (no-op in Phase 3)")
				return nil
			},
		},
	}
	go subscriber.Run(clusterCtx)

	log.WithFields(log.Fields{
		"node_id":         nodeID,
		"region":          s.cfg.Cluster.Region,
		"probe_interval":  probeInterval,
		"auth_sharding":   s.cfg.Cluster.AuthSharding,
		"ring_staleness":  ringStaleness,
		"ring_poll":       ringPoll,
	}).Info("cluster mode enabled")
	return nil
}

// parseDurationOr parses a Go duration string; on empty/invalid input it
// returns defaultValue and logs a warning naming fieldName so operators can
// grep the startup log for "invalid <field>".
func parseDurationOr(raw string, defaultValue time.Duration, fieldName string) time.Duration {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return defaultValue
	}
	d, err := time.ParseDuration(trimmed)
	if err != nil || d <= 0 {
		log.Warnf("cluster: invalid %s %q; using default %s", fieldName, raw, defaultValue)
		return defaultValue
	}
	return d
}
