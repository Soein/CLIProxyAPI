package cluster

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
)

// ChannelInstanceChanged is the Postgres LISTEN channel used by the
// notify_cpa_instance_changed trigger (see postgresstore.go EnsureSchema).
// Payload = node_id. RingWatcher refreshes its snapshot on any notification.
const ChannelInstanceChanged = "cpa_instance_changed"

// Defaults — matching the advisory guidance in config.go.
const (
	defaultRingStaleness    = 30 * time.Second
	defaultRingPollInterval = 30 * time.Second
	ringConnectTimeout      = 5 * time.Second
)

// RingWatcher keeps an AuthRing in sync with the shared cluster_nodes table.
//
// Two independent refresh triggers:
//
//  1. NOTIFY-driven (low-latency, near-realtime). A pgx listener subscribes
//     to cpa_instance_changed; every notification schedules a refresh.
//  2. Poll-driven (safety net). Every PollInterval we re-query cluster_nodes
//     regardless of NOTIFY activity. Covers the corner case where the
//     listener connection silently drops a message or is temporarily
//     disconnected. PollInterval defaults to 30s — long enough to not load
//     PG unnecessarily, short enough that a missed NOTIFY heals well before
//     user-visible impact.
//
// StalenessThreshold excludes rows whose last_heartbeat is older than now -
// threshold. This is the cluster's "is-this-node-alive" check: if a
// replica's Registrar stops writing (network partition, crash, GC stall),
// peers will evict it from the ring within StalenessThreshold.
//
// Ring updates are atomic (AuthRing uses atomic.Pointer swap internally) so
// readers never see a partial ring.
type RingWatcher struct {
	DB                 *sql.DB
	DSN                string
	Ring               *AuthRing
	StalenessThreshold time.Duration
	PollInterval       time.Duration
	// OnChange, if set, is invoked every time the ring is rebuilt with
	// MEMBERS CHANGING (not just heartbeat refresh). Used by the scheduler
	// to kick a re-sync so newly-acquired or newly-lost auths take effect
	// without waiting for the next request. Keep the callback cheap — it
	// runs on the watcher goroutine.
	OnChange func()
}

// Run blocks until ctx is cancelled. Safe to call with nil DB — in that
// case we log a warning and return (caller bug; ring stays empty so IsMine
// degrades to true).
func (w *RingWatcher) Run(ctx context.Context) {
	if w == nil {
		return
	}
	if w.DB == nil {
		log.Warn("ring watcher: DB is nil; ring will remain empty (IsMine degrades to true)")
		return
	}
	if w.Ring == nil {
		log.Warn("ring watcher: Ring is nil; cannot publish updates")
		return
	}
	staleness := w.StalenessThreshold
	if staleness <= 0 {
		staleness = defaultRingStaleness
	}
	pollInterval := w.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultRingPollInterval
	}

	// Initial load so the ring is populated before we process any NOTIFY.
	// Failure here is non-fatal: the ring stays empty (IsMine=true
	// degradation) and the next tick will retry.
	if err := w.refresh(ctx, staleness); err != nil {
		log.WithError(err).Warn("ring watcher: initial refresh failed; ring empty until next tick")
	}

	// Channel used by the listener goroutine to signal "something changed,
	// please refresh." Buffered so a fast NOTIFY burst doesn't block the
	// listener on the refresher.
	ping := make(chan struct{}, 1)

	// LISTEN goroutine. Only started if DSN is set — pure poll mode works
	// too (slower convergence but correct).
	if strings.TrimSpace(w.DSN) != "" {
		go w.listenLoop(ctx, ping)
	} else {
		log.Info("ring watcher: no DSN; falling back to poll-only mode")
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.refresh(ctx, staleness); err != nil {
				log.WithError(err).Debug("ring watcher: poll refresh failed")
			}
		case <-ping:
			// Coalesce: drain any queued pings so we do at most ONE
			// refresh per burst.
			drain(ping)
			if err := w.refresh(ctx, staleness); err != nil {
				log.WithError(err).Debug("ring watcher: notify-driven refresh failed")
			}
		}
	}
}

// listenLoop connects with pgx, LISTENs on ChannelInstanceChanged, and
// pings the main loop on every notification. Reconnects with backoff on
// error — matches the pattern used by ChangeSubscriber.
func (w *RingWatcher) listenLoop(ctx context.Context, ping chan<- struct{}) {
	backoffs := []time.Duration{time.Second, 3 * time.Second, 10 * time.Second, 30 * time.Second}
	attempt := 0
	for {
		err := w.listenOnce(ctx, ping, func() {
			// First notification received cleanly -> reset backoff.
			attempt = 0
		})
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			wait := backoffs[attempt]
			if attempt < len(backoffs)-1 {
				attempt++
			}
			log.WithError(err).Warnf("ring watcher: listener error; reconnecting in %s", wait)
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
		}
	}
}

func (w *RingWatcher) listenOnce(ctx context.Context, ping chan<- struct{}, onFirstNotif func()) error {
	connectCtx, cancel := context.WithTimeout(ctx, ringConnectTimeout)
	conn, err := pgx.Connect(connectCtx, w.DSN)
	cancel()
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), ringConnectTimeout)
		defer closeCancel()
		_ = conn.Close(closeCtx)
	}()

	if _, err := conn.Exec(ctx, "LISTEN "+ChannelInstanceChanged); err != nil {
		return err
	}

	firstNotif := true
	for {
		_, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if firstNotif {
			firstNotif = false
			if onFirstNotif != nil {
				onFirstNotif()
			}
		}
		select {
		case ping <- struct{}{}:
		default:
			// Already a ping pending — refresher will pick up both.
		}
	}
}

// refresh queries active cluster_nodes rows and rebuilds the ring if the
// membership changed (by the canonical member-set, NOT by heartbeat-only
// updates). Returns nil on DB error during ctx cancellation to avoid log
// noise at shutdown.
func (w *RingWatcher) refresh(ctx context.Context, staleness time.Duration) error {
	rows, err := w.DB.QueryContext(ctx, `
		SELECT node_id, COALESCE(weight, 100)
		FROM cluster_nodes
		WHERE status = 'active'
		  AND last_heartbeat > NOW() - $1::interval
		ORDER BY node_id
	`, staleness.String())
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil
		}
		return err
	}
	defer rows.Close()

	var members []RingMember
	for rows.Next() {
		var m RingMember
		if err := rows.Scan(&m.NodeID, &m.Weight); err != nil {
			return err
		}
		members = append(members, m)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if !membersChanged(w.Ring.Members(), members) {
		return nil
	}
	w.Ring.Rebuild(members)
	log.WithField("members", formatMembers(members)).Info("ring watcher: membership updated")
	if w.OnChange != nil {
		// Run the callback synchronously — it's expected to be cheap (kick
		// a channel). If it needs to do real work, it should spawn its
		// own goroutine.
		w.OnChange()
	}
	return nil
}

// membersChanged compares by NodeID set only — ignoring weight changes so
// the scheduler doesn't thrash on every heartbeat. Weight deltas still
// matter for future auths but they'll pick up via the next real membership
// change or poll tick.
//
// Actually wait: weight changes DO matter. If operator bumps a node's
// weight in cluster_nodes, we want the ring to reflect it immediately so
// ownership rebalances. So compare both NodeID AND weight.
func membersChanged(prev, curr []RingMember) bool {
	if len(prev) != len(curr) {
		return true
	}
	prevMap := make(map[string]int, len(prev))
	for _, m := range prev {
		prevMap[m.NodeID] = m.Weight
	}
	for _, m := range curr {
		if w, ok := prevMap[m.NodeID]; !ok || w != m.Weight {
			return true
		}
	}
	return false
}

func drain(ch chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func formatMembers(members []RingMember) string {
	parts := make([]string, 0, len(members))
	for _, m := range members {
		parts = append(parts, m.NodeID)
	}
	return strings.Join(parts, ",")
}
