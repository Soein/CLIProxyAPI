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
// polling of membership rows to build a consistent-hash ring from account_id
// to cpa instance.
//
// InstanceRegistrar writes only the columns it owns (endpoint/weight/status/
// last_heartbeat) via ON CONFLICT DO UPDATE SET, so it can safely coexist
// with LeaderElector, which writes role/region/metadata on the same row.
//
// Status lifecycle:
//
//	draining -- startup safety barrier or graceful shutdown; not routable
//	joining  -- API listener is ready and ownership is reconciling
//	active   -- ownership is reconciled and new traffic may be admitted
//	down     -- set on final UPSERT during Close(); removed from the ring
//
// Failure semantics: a heartbeat UPSERT that errors is logged and the loop
// continues — we do NOT exit, because a transient PG blip should not take
// this replica out of the ring (new-api's staleness check handles true
// failures via last_heartbeat threshold).
const (
	statusActive   = "active"
	statusJoining  = "joining"
	statusDraining = "draining"
	statusDown     = "down"

	defaultRegistrarInterval = 10 * time.Second
	defaultDrainGrace        = 30 * time.Second
	shutdownWriteTimeout     = 5 * time.Second
)

// RegistrarConfig parameterises InstanceRegistrar. DB/NodeID/Endpoint are
// required; everything else has a sane default.
type RegistrarConfig struct {
	DB         *sql.DB
	NodeID     string
	Region     string
	Endpoint   string
	Weight     int
	Interval   time.Duration // heartbeat cadence; default 10s
	DrainGrace time.Duration // how long to sit in 'draining' before 'down'; default 30s
	TimeSource func() time.Time
	// StartDraining keeps the node unroutable until Activate succeeds. The
	// zero value preserves standalone Registrar behavior of starting active.
	StartDraining bool
	// OnActiveHeartbeat is called after each successful active heartbeat made
	// by Run, including the initial registration. Drain and shutdown writes do
	// not count as serving-path freshness and therefore do not invoke it. The
	// timestamp is captured before the database write starts so response delay
	// cannot make the database heartbeat appear newer than it is.
	OnActiveHeartbeat func(startedAt time.Time)
}

// InstanceRegistrar keeps one row in cluster_nodes up-to-date for the
// lifetime of the process.
type InstanceRegistrar struct {
	cfg RegistrarConfig

	mu      sync.Mutex
	writeMu sync.Mutex
	status  string // current published status; guarded by mu
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
	status := statusActive
	if cfg.StartDraining {
		status = statusDraining
	}
	return &InstanceRegistrar{cfg: cfg, status: status}, nil
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

	if err := r.Publish(ctx); err != nil {
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
			if err := r.Publish(ctx); err != nil {
				// Non-fatal: new-api's staleness window is much wider than
				// one missed heartbeat. Log and carry on.
				log.WithError(err).Debugf("registrar: heartbeat upsert failed; node=%s", r.cfg.NodeID)
			}
		}
	}
}

// Publish synchronously writes the registrar's current routing state. Run
// uses the same operation for its initial registration and heartbeats;
// bootstrap callers can use it as a fail-closed publication barrier before
// starting any component that might refresh a stale cluster_nodes row.
func (r *InstanceRegistrar) Publish(ctx context.Context) error {
	if r == nil {
		return nil
	}
	status, startedAt, err := r.upsertCurrentStatus(ctx)
	if err != nil {
		return err
	}
	r.reportActiveHeartbeat(status, startedAt)
	return nil
}

// Drain transitions the node to 'draining' so the front-door stops routing
// new traffic. Call this before initiating graceful shutdown of HTTP
// handlers. Subsequent heartbeats keep the row in 'draining' until Close()
// sets 'down'.
func (r *InstanceRegistrar) Drain(ctx context.Context) error {
	if r == nil {
		return nil
	}
	return r.transition(ctx, statusDraining, statusDraining)
}

// Join publishes the node as a ring member after the API listener is ready,
// while keeping it unavailable for front-door traffic until Activate succeeds.
// A failed write leaves the in-memory state joining so Run can retry it.
func (r *InstanceRegistrar) Join(ctx context.Context) error {
	if r == nil {
		return nil
	}
	return r.transition(ctx, statusJoining, statusJoining)
}

// Activate publishes the node as routable after the API listener and dispatch
// ownership are ready. The write is serialized with Run's heartbeat UPSERT so
// an older write cannot overwrite the active transition.
func (r *InstanceRegistrar) Activate(ctx context.Context) error {
	if r == nil {
		return nil
	}
	status, startedAt, errActivate := r.transitionWithResult(ctx, statusActive, statusJoining)
	if errActivate != nil {
		return errActivate
	}
	r.reportActiveHeartbeat(status, startedAt)
	return nil
}

// IsActive reports whether this registrar has crossed the serving barrier.
func (r *InstanceRegistrar) IsActive() bool {
	return r != nil && r.currentStatus() == statusActive
}

// close is called on ctx cancellation. Uses a detached context so PG still
// accepts the 'down' write even when the service context was just cancelled.
func (r *InstanceRegistrar) close() {
	if r == nil {
		return
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), shutdownWriteTimeout)
	defer cancel()
	if err := r.transition(closeCtx, statusDown, statusDown); err != nil {
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
	_, _, err := r.upsertCurrentStatus(ctx)
	return err
}

// upsertCurrentStatus returns the exact status value sent to Postgres so Run
// only reports freshness for successful active heartbeats. Reading the status
// once also prevents a concurrent Drain from making the callback describe a
// different write than the one that actually completed.
func (r *InstanceRegistrar) upsertCurrentStatus(ctx context.Context) (string, time.Time, error) {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	return r.upsertCurrentStatusLocked(ctx)
}

func (r *InstanceRegistrar) upsertCurrentStatusLocked(ctx context.Context) (string, time.Time, error) {
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
	status := r.currentStatus()
	startedAt := r.cfg.TimeSource()
	_, err := r.cfg.DB.ExecContext(ctx, q,
		r.cfg.NodeID,
		r.cfg.Region,
		r.cfg.Endpoint,
		r.cfg.Weight,
		status,
	)
	return status, startedAt, err
}

func (r *InstanceRegistrar) transition(ctx context.Context, nextStatus, failureStatus string) error {
	_, _, err := r.transitionWithResult(ctx, nextStatus, failureStatus)
	return err
}

// transitionWithResult serializes status changes with every heartbeat write.
// failureStatus is installed before releasing writeMu so no concurrent Publish
// can resurrect the state that failed to reach Postgres.
func (r *InstanceRegistrar) transitionWithResult(ctx context.Context, nextStatus, failureStatus string) (string, time.Time, error) {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	r.mu.Lock()
	r.status = nextStatus
	r.mu.Unlock()
	status, startedAt, err := r.upsertCurrentStatusLocked(ctx)
	if err != nil {
		r.mu.Lock()
		r.status = failureStatus
		r.mu.Unlock()
	}
	return status, startedAt, err
}

func (r *InstanceRegistrar) reportActiveHeartbeat(status string, startedAt time.Time) {
	if status == statusActive && r.cfg.OnActiveHeartbeat != nil {
		r.cfg.OnActiveHeartbeat(startedAt)
	}
}
