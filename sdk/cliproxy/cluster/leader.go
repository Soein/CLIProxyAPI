package cluster

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// leaderLockClass / leaderLockID form the (int, int) pair passed to
	// pg_try_advisory_lock. A two-argument lock carves out a distinct namespace
	// from the single-int64 version used by auth refresh, so key collisions are
	// impossible by construction.
	leaderLockClass int32 = 1
	leaderLockID    int32 = 1

	// nodeTableName is the UPSERT target for heartbeat tracking.
	nodeTableName = "cluster_nodes"
)

// LeaderElector keeps one process holding a pg_try_advisory_lock so that
// background tasks (token refresh loop, usage cleanup, model updater) only
// run on a single instance cluster-wide. It also records a heartbeat in the
// cluster_nodes table so operators can inspect membership via SQL.
//
// The elector uses a dedicated database connection; the advisory lock is held
// for the lifetime of that connection, so a crash or network partition causes
// Postgres to release the lock and another node can take over. When the
// connection itself errors we proactively demote locally so the caller does
// not keep thinking it is leader while PG has already released the lock.
type LeaderElector struct {
	db       *sql.DB
	nodeID   string
	region   string
	interval time.Duration
	onLoss   func() // invoked (asynchronously) when leadership is lost

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
	nodeID := cfg.NodeID
	if nodeID == "" {
		if h, err := os.Hostname(); err == nil && h != "" {
			nodeID = h
		} else {
			nodeID = "unknown"
		}
	}
	return &LeaderElector{
		db:       cfg.DB,
		nodeID:   nodeID,
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
// lock and refreshing the heartbeat. Returns when ctx is cancelled. Rebuilds
// the connection automatically if it goes bad; demotes the local isLeader flag
// whenever a probe fails so the process stops believing it is leader while PG
// has already released the session-level lock.
func (le *LeaderElector) Run(ctx context.Context) error {
	if le == nil || le.db == nil {
		return nil
	}

	ticker := time.NewTicker(le.interval)
	defer ticker.Stop()
	defer le.isLeader.Store(false)

	var conn *sql.Conn
	defer func() {
		if conn != nil {
			discardSessionLockConn(conn)
		}
	}()
	holdsLock := false

	for {
		// Ensure we have a live dedicated connection. Anything wrong with it
		// (including a previous probe failure) forces rebuild on the next tick.
		if conn == nil {
			c, err := le.db.Conn(ctx)
			if err != nil {
				le.demote("cannot acquire db conn", err)
				if sleepWithCtx(ctx, ticker) {
					return ctx.Err()
				}
				continue
			}
			conn = c
			holdsLock = false
		}

		var errProbe error
		if holdsLock {
			errProbe = le.probeLeaderSession(ctx, conn)
		} else {
			holdsLock, errProbe = le.tryAcquireLeader(ctx, conn)
		}
		if errProbe != nil {
			if ctx.Err() != nil {
				le.isLeader.Store(false)
				discardSessionLockConn(conn)
				conn = nil
				return ctx.Err()
			}
			le.demote("probe failed", errProbe)
			discardSessionLockConn(conn)
			conn = nil
			holdsLock = false
		}

		if sleepWithCtx(ctx, ticker) {
			return ctx.Err()
		}
	}
}

// sleepWithCtx waits for the next tick or ctx done. Returns true if ctx was
// cancelled.
func sleepWithCtx(ctx context.Context, ticker *time.Ticker) bool {
	select {
	case <-ctx.Done():
		return true
	case <-ticker.C:
		return false
	}
}

// demote flips isLeader to false if we thought we were leader, and fires
// onLoss asynchronously so a slow callback does not block the probe loop.
func (le *LeaderElector) demote(reason string, err error) {
	was := le.isLeader.Swap(false)
	if was {
		log.WithError(err).Warnf("demoting self: %s", reason)
		go le.onLoss()
	} else {
		log.WithError(err).Debugf("leader probe error (not leader): %s", reason)
	}
}

func (le *LeaderElector) tryAcquireLeader(ctx context.Context, conn *sql.Conn) (bool, error) {
	var got bool
	if err := conn.QueryRowContext(ctx,
		"SELECT pg_try_advisory_lock($1, $2)", leaderLockClass, leaderLockID,
	).Scan(&got); err != nil {
		return false, err
	}

	was := le.isLeader.Swap(got)
	le.heartbeat(ctx, conn, got)
	if got && !was {
		log.Infof("became leader: node=%s region=%s", le.nodeID, le.region)
	}
	return got, nil
}

// probeLeaderSession checks that the dedicated PostgreSQL session remains
// usable without reacquiring the session advisory lock. Repeated
// pg_try_advisory_lock calls are reentrant and would otherwise increment the
// server-side lock count on every tick.
func (le *LeaderElector) probeLeaderSession(ctx context.Context, conn *sql.Conn) error {
	var one int
	if err := conn.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		return err
	}
	if one != 1 {
		return errors.New("cluster: invalid leader session probe result")
	}
	le.isLeader.Store(true)
	le.heartbeat(ctx, conn, true)
	return nil
}

func (le *LeaderElector) heartbeat(ctx context.Context, conn *sql.Conn, leader bool) {
	role := "follower"
	if leader {
		role = "leader"
	}
	host, _ := os.Hostname()
	meta, _ := json.Marshal(map[string]string{"hostname": host})
	if _, err := conn.ExecContext(ctx, `
		INSERT INTO `+nodeTableName+` (node_id, role, region, last_heartbeat, metadata)
		VALUES ($1, $2, $3, NOW(), $4)
		ON CONFLICT (node_id) DO UPDATE SET
			role = EXCLUDED.role,
			region = EXCLUDED.region,
			last_heartbeat = NOW(),
			metadata = EXCLUDED.metadata
	`, le.nodeID, role, le.region, meta); err != nil {
		log.WithError(err).Warnf("cluster_nodes heartbeat upsert failed (keeping leader state); node=%s", le.nodeID)
	}
}
