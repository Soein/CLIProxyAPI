package cluster

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// leaderAdvisoryLockKey is the Postgres advisory lock key used for
	// cluster-wide leader election. Chosen as a fixed 32-bit integer so the
	// same key is reused across processes. Do not reuse for other purposes.
	leaderAdvisoryLockKey int64 = 0xC11F_0001

	// nodeTableName is the UPSERT target for heartbeat tracking.
	nodeTableName = "cluster_nodes"
)

// LeaderElector keeps one process holding a pg_try_advisory_lock so that
// background tasks (token refresh loop, usage automations, model updater) only
// run on a single instance cluster-wide. It also records a heartbeat in the
// cluster_nodes table so operators can inspect membership via SQL.
//
// The elector uses a dedicated database connection; the advisory lock is held
// for the lifetime of that connection, so a crash or network partition causes
// Postgres to release the lock and another node can take over.
type LeaderElector struct {
	db       *sql.DB
	nodeID   string
	region   string
	interval time.Duration
	onLoss   func() // invoked when leadership is lost; defaults to os.Exit(3)

	isLeader atomic.Bool
}

// Config holds construction parameters. All fields are optional except db.
type Config struct {
	DB       *sql.DB
	NodeID   string        // stable identifier; hostname+pid is a good default
	Region   string        // free-form region tag (e.g. "fra", "lon")
	Interval time.Duration // probe interval; default 5s
	OnLoss   func()        // called when we lose leadership; default: exit(3)
}

// New constructs a LeaderElector with defaults filled in.
func New(cfg Config) *LeaderElector {
	interval := cfg.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	onLoss := cfg.OnLoss
	if onLoss == nil {
		onLoss = func() {
			log.Warn("lost leadership, exiting for clean restart")
			os.Exit(3)
		}
	}
	return &LeaderElector{
		db:       cfg.DB,
		nodeID:   cfg.NodeID,
		region:   cfg.Region,
		interval: interval,
		onLoss:   onLoss,
	}
}

// IsLeader reports whether this node currently holds the advisory lock.
// Cheap (atomic load); safe to call from hot paths.
func (le *LeaderElector) IsLeader() bool {
	if le == nil {
		return false
	}
	return le.isLeader.Load()
}

// Run keeps a dedicated connection open, periodically attempting the advisory
// lock and refreshing the heartbeat. Returns when ctx is cancelled.
func (le *LeaderElector) Run(ctx context.Context) error {
	if le == nil || le.db == nil {
		return nil
	}
	conn, err := le.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	ticker := time.NewTicker(le.interval)
	defer ticker.Stop()

	for {
		if err := le.probe(ctx, conn); err != nil && ctx.Err() == nil {
			log.WithError(err).Debug("leader probe failed")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (le *LeaderElector) probe(ctx context.Context, conn *sql.Conn) error {
	var got bool
	if err := conn.QueryRowContext(ctx,
		"SELECT pg_try_advisory_lock($1)", leaderAdvisoryLockKey,
	).Scan(&got); err != nil {
		return err
	}

	was := le.isLeader.Load()
	le.isLeader.Store(got)

	role := "follower"
	if got {
		role = "leader"
	}
	meta, _ := json.Marshal(map[string]string{"pid": os.Getenv("HOSTNAME")})
	if _, err := conn.ExecContext(ctx, `
		INSERT INTO `+nodeTableName+` (node_id, role, region, last_heartbeat, metadata)
		VALUES ($1, $2, $3, NOW(), $4)
		ON CONFLICT (node_id) DO UPDATE SET
			role = EXCLUDED.role,
			region = EXCLUDED.region,
			last_heartbeat = NOW(),
			metadata = EXCLUDED.metadata
	`, le.nodeID, role, le.region, meta); err != nil {
		return err
	}

	switch {
	case got && !was:
		log.Infof("became leader: node=%s region=%s", le.nodeID, le.region)
	case !got && was:
		le.onLoss()
	}
	return nil
}
