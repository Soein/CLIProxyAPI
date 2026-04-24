package cluster

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// Phase 4: InstanceRegistrar publishes this replica's routing metadata
// (endpoint/weight/status) to the shared cluster_nodes table. The front-door
// router (new-api) subscribes to NOTIFY cpa_instance_changed + periodic
// polling of the active rows to build a consistent-hash ring from account_id
// to cpa instance.
//
// InstanceRegistrar writes only the columns it owns (endpoint/weight/status/
// last_heartbeat) via ON CONFLICT DO UPDATE SET, so it can safely coexist
// with LeaderElector, which writes role/region/metadata on the same row.
//
// Status lifecycle:
//
//	active    -- normal serving state; Run() maintains via periodic UPSERT
//	draining  -- graceful shutdown initiated; new-api stops sending traffic
//	              but in-flight requests continue. Triggered by Drain().
//	down      -- set on final UPSERT during Close(); new-api removes from ring.
//
// Failure semantics: a heartbeat UPSERT that errors is logged and the loop
// continues — we do NOT exit, because a transient PG blip should not take
// this replica out of the ring (new-api's staleness check handles true
// failures via last_heartbeat threshold).
const (
	statusActive   = "active"
	statusDraining = "draining"
	statusDown     = "down"

	defaultRegistrarInterval = 10 * time.Second
	defaultDrainGrace        = 30 * time.Second
	shutdownWriteTimeout     = 5 * time.Second
)

// RegistrarConfig parameterises InstanceRegistrar. DB/NodeID/Endpoint are
// required; everything else has a sane default.
type RegistrarConfig struct {
	DB          *sql.DB
	NodeID      string
	Region      string
	Endpoint    string
	Weight      int
	Interval    time.Duration // heartbeat cadence; default 10s
	DrainGrace  time.Duration // how long to sit in 'draining' before 'down'; default 30s
	TimeSource  func() time.Time
}

// InstanceRegistrar keeps one row in cluster_nodes up-to-date for the
// lifetime of the process.
type InstanceRegistrar struct {
	cfg RegistrarConfig

	mu     sync.Mutex
	status string // current published status; guarded by mu
}

// NewRegistrar returns a registrar ready for Run. Callers must check
// ErrEndpointRequired — an empty endpoint is almost always a misconfiguration
// (the node would appear in cluster_nodes but be unroutable) and silently
// proceeding would mask the bug until Phase 4 traffic routing goes live.
func NewRegistrar(cfg RegistrarConfig) (*InstanceRegistrar, error) {
	if cfg.DB == nil {
		return nil, errors.New("registrar: DB is required")
	}
	if strings.TrimSpace(cfg.NodeID) == "" {
		return nil, errors.New("registrar: NodeID is required")
	}
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, ErrEndpointRequired
	}
	if cfg.Weight <= 0 {
		cfg.Weight = 100
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultRegistrarInterval
	}
	if cfg.DrainGrace <= 0 {
		cfg.DrainGrace = defaultDrainGrace
	}
	if cfg.TimeSource == nil {
		cfg.TimeSource = time.Now
	}
	return &InstanceRegistrar{cfg: cfg, status: statusActive}, nil
}

// ErrEndpointRequired is returned by NewRegistrar when Endpoint is empty.
// Callers that deliberately want an un-routable "invisible" node (legacy
// single-instance mode with cluster.enabled=true) should check for this
// sentinel and proceed without a registrar.
var ErrEndpointRequired = errors.New("registrar: Endpoint is required for instance routing")

// Run blocks until ctx is cancelled. It performs an initial UPSERT, then
// refreshes the heartbeat at Interval. Ctx cancellation triggers Close()
// with detached background ctx so the row is marked 'down' even when
// service ctx has already been torn down.
func (r *InstanceRegistrar) Run(ctx context.Context) {
	if r == nil {
		return
	}

	if err := r.upsert(ctx); err != nil {
		log.WithError(err).Warnf("registrar: initial upsert failed; node=%s (will retry)", r.cfg.NodeID)
	} else {
		log.WithFields(log.Fields{
			"node_id":  r.cfg.NodeID,
			"endpoint": r.cfg.Endpoint,
			"weight":   r.cfg.Weight,
			"region":   r.cfg.Region,
			"interval": r.cfg.Interval,
		}).Info("registrar: instance registered")
	}

	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.close()
			return
		case <-ticker.C:
			if err := r.upsert(ctx); err != nil {
				// Non-fatal: new-api's staleness window is much wider than
				// one missed heartbeat. Log and carry on.
				log.WithError(err).Debugf("registrar: heartbeat upsert failed; node=%s", r.cfg.NodeID)
			}
		}
	}
}

// Drain transitions the node to 'draining' so the front-door stops routing
// new traffic. Call this before initiating graceful shutdown of HTTP
// handlers. Subsequent heartbeats keep the row in 'draining' until Close()
// sets 'down'.
func (r *InstanceRegistrar) Drain(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	r.status = statusDraining
	r.mu.Unlock()
	return r.upsert(ctx)
}

// close is called on ctx cancellation. Uses a detached context so PG still
// accepts the 'down' write even when the service context was just cancelled.
func (r *InstanceRegistrar) close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.status = statusDown
	r.mu.Unlock()
	closeCtx, cancel := context.WithTimeout(context.Background(), shutdownWriteTimeout)
	defer cancel()
	if err := r.upsert(closeCtx); err != nil {
		log.WithError(err).Warnf("registrar: final 'down' write failed; node=%s", r.cfg.NodeID)
	} else {
		log.Infof("registrar: marked down; node=%s", r.cfg.NodeID)
	}
}

// currentStatus reads the published status under mu so tests and Drain()
// stay consistent with the Run loop.
func (r *InstanceRegistrar) currentStatus() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status == "" {
		return statusActive
	}
	return r.status
}

// upsert writes this node's routing columns. Critically, ON CONFLICT only
// touches endpoint/weight/status/last_heartbeat. Role/region/metadata are
// owned by LeaderElector and must NOT be clobbered here. Region is also
// written (as a safe first-insert) but excluded from the UPDATE set.
//
// The INSERT supplies placeholder values for columns owned by other writers
// (role='member', metadata='{}') only for the no-row-yet case; after the
// first LeaderElector probe those columns will be overwritten correctly.
func (r *InstanceRegistrar) upsert(ctx context.Context) error {
	const q = `
INSERT INTO cluster_nodes
    (node_id, role, region, last_heartbeat, metadata, endpoint, weight, status)
VALUES
    ($1, NULL, $2, NOW(), NULL, $3, $4, $5)
ON CONFLICT (node_id) DO UPDATE SET
    endpoint       = EXCLUDED.endpoint,
    weight         = EXCLUDED.weight,
    status         = EXCLUDED.status,
    last_heartbeat = NOW()
`
	_, err := r.cfg.DB.ExecContext(ctx, q,
		r.cfg.NodeID,
		r.cfg.Region,
		r.cfg.Endpoint,
		r.cfg.Weight,
		r.currentStatus(),
	)
	return err
}
