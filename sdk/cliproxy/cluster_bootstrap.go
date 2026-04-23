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
func (s *Service) bootstrapCluster(ctx context.Context) error {
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

	var probeInterval time.Duration
	if v := strings.TrimSpace(s.cfg.Cluster.ProbeInterval); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			probeInterval = d
		} else {
			log.Warnf("cluster: invalid probe-interval %q; using default", v)
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
		"node_id":        nodeID,
		"region":         s.cfg.Cluster.Region,
		"probe_interval": probeInterval,
	}).Info("cluster mode enabled")
	return nil
}
