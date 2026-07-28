package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
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
	// RequireAuthority disables the legacy non-transactional refresh fallback.
	// Cluster auth sharding must set this so missing epoch authority fails
	// closed instead of publishing process-local epochs.
	RequireAuthority bool
	// OnChange, if set, is invoked every time the ring is rebuilt with
	// MEMBERS CHANGING (not just heartbeat refresh). Used by the scheduler
	// to kick a re-sync so newly-acquired or newly-lost auths take effect
	// without waiting for the next request. Keep the callback cheap — it
	// runs on the watcher goroutine.
	OnChange func()
	// OnRefresh, if set, is invoked after every successful full refresh,
	// including snapshots whose membership is unchanged. It is used by the
	// cluster watchdog to distinguish a healthy query path from a stale ring.
	// The timestamp is captured before QueryContext so a slow response cannot
	// make the returned snapshot appear newer than it is.
	OnRefresh  func(startedAt time.Time)
	TimeSource func() time.Time

	backoffs []time.Duration
	listen   func(ctx context.Context, ping chan<- struct{}, onListening func() error) error
	wait     func(ctx context.Context, delay time.Duration) bool

	// refreshMu serializes the full database-read-to-local-publication cycle.
	// Run and the LISTEN synchronization path can request refresh concurrently;
	// keeping commit and publish in one critical section prevents an older
	// successful refresh from undoing a later fail-close at the same epoch.
	refreshMu sync.Mutex
	// afterCommit is a deterministic test seam for the commit/publish boundary.
	afterCommit func()
}

// Run blocks until ctx is cancelled. Safe to call with nil DB — in that
// case we log a warning and return (caller bug; the ring stays unready and
// ownership checks fail closed).
func (w *RingWatcher) Run(ctx context.Context) {
	if w == nil {
		return
	}
	if w.DB == nil {
		log.Warn("ring watcher: DB is nil; ring will remain unready and fail closed")
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
	// Failure here is non-fatal: the ring stays unready and fails closed;
	// the next tick will retry.
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
		go w.listenLoop(ctx, ping, staleness)
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
func (w *RingWatcher) listenLoop(ctx context.Context, ping chan<- struct{}, staleness time.Duration) {
	backoffs := w.backoffs
	if len(backoffs) == 0 {
		backoffs = []time.Duration{time.Second, 3 * time.Second, 10 * time.Second, 30 * time.Second}
	}
	listen := w.listen
	if listen == nil {
		listen = w.listenOnce
	}
	waitForRetry := w.wait
	if waitForRetry == nil {
		waitForRetry = func(ctx context.Context, delay time.Duration) bool {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(delay):
				return true
			}
		}
	}
	attempt := 0
	for {
		err := listen(ctx, ping, func() error {
			// LISTEN plus a successful full refresh proves the quiet connection
			// is healthy; no notification is required to reset backoff.
			if err := w.refresh(ctx, staleness); err != nil {
				return err
			}
			attempt = 0
			return nil
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
			if !waitForRetry(ctx, wait) {
				return
			}
		}
	}
}

func (w *RingWatcher) listenOnce(ctx context.Context, ping chan<- struct{}, onListening func() error) error {
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
	if onListening != nil {
		if err := onListening(); err != nil {
			return err
		}
	}

	for {
		_, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
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
	w.refreshMu.Lock()
	defer w.refreshMu.Unlock()

	timeSource := w.TimeSource
	if timeSource == nil {
		timeSource = time.Now
	}
	startedAt := timeSource()
	tx, err := w.DB.BeginTx(ctx, nil)
	if err != nil {
		// A few lightweight in-process database/sql drivers used by embedders
		// expose QueryContext but no transaction support. Preserve their legacy
		// single-instance behavior; PostgreSQL always supports the strict path.
		if errors.Is(err, driver.ErrSkip) && !w.RequireAuthority {
			return w.refreshWithoutMembershipAuthority(ctx, staleness, startedAt)
		}
		if w.RequireAuthority {
			current := w.Ring.Decision("")
			w.Ring.RebuildAt(current.Epoch, nil)
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil
		}
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var (
		epoch             int64
		storedFingerprint []byte
		stalenessMS       int64
	)
	if err = tx.QueryRowContext(ctx, `
		SELECT epoch, fingerprint, staleness_ms
		FROM cluster_membership_state
		WHERE id = 1
		FOR UPDATE
	`).Scan(&epoch, &storedFingerprint, &stalenessMS); err != nil {
		if w.RequireAuthority {
			current := w.Ring.Decision("")
			w.Ring.RebuildAt(current.Epoch, nil)
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil
		}
		return fmt.Errorf("ring watcher: lock membership state: %w", err)
	}
	if stalenessMS == 0 {
		if _, err = tx.ExecContext(ctx, `
			UPDATE cluster_membership_state
			SET staleness_ms = $1, updated_at = clock_timestamp()
			WHERE id = 1 AND staleness_ms = 0
		`, staleness.Milliseconds()); err != nil {
			return fmt.Errorf("ring watcher: initialize cluster staleness: %w", err)
		}
		stalenessMS = staleness.Milliseconds()
	}
	if stalenessMS != staleness.Milliseconds() {
		w.Ring.RebuildAt(epoch, nil)
		return fmt.Errorf("ring watcher: configured staleness %dms differs from cluster %dms", staleness.Milliseconds(), stalenessMS)
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT node_id, COALESCE(weight, 100), endpoint
		FROM cluster_nodes
		WHERE status IN ('active', 'joining')
		  AND last_heartbeat > clock_timestamp() - make_interval(secs => $1::double precision / 1000.0)
		  AND NULLIF(BTRIM(endpoint), '') IS NOT NULL
		ORDER BY node_id
	`, staleness.Milliseconds())
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
		var endpoint string
		if err := rows.Scan(&m.NodeID, &m.Weight, &endpoint); err != nil {
			return err
		}
		// Keep a second defensive check in case a compatible store does not
		// enforce PostgreSQL's BTRIM predicate exactly as expected.
		if strings.TrimSpace(endpoint) == "" {
			continue
		}
		members = append(members, m)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}

	fingerprint := membershipFingerprint(members)
	if !bytes.Equal(storedFingerprint, fingerprint) {
		epoch++
		if _, err = tx.ExecContext(ctx, `
			UPDATE cluster_membership_state
			SET epoch = $1, fingerprint = $2, updated_at = clock_timestamp()
			WHERE id = 1
		`, epoch, fingerprint); err != nil {
			return fmt.Errorf("ring watcher: advance membership epoch: %w", err)
		}
	}
	if err = tx.Commit(); err != nil {
		if w.RequireAuthority {
			current := w.Ring.Decision("")
			w.Ring.RebuildAt(current.Epoch, nil)
		}
		return fmt.Errorf("ring watcher: commit membership refresh: %w", err)
	}
	if w.afterCommit != nil {
		w.afterCommit()
	}

	localChanged := membersChanged(w.Ring.Members(), members) || w.Ring.Decision("").Epoch != epoch
	w.Ring.RebuildAt(epoch, members)
	if w.OnRefresh != nil {
		w.OnRefresh(startedAt)
	}
	if localChanged {
		log.WithFields(log.Fields{"epoch": epoch, "members": formatMembers(members)}).Info("ring watcher: membership updated")
	}
	if localChanged && w.OnChange != nil {
		// Run the callback synchronously — it's expected to be cheap (kick
		// a channel). If it needs to do real work, it should spawn its
		// own goroutine.
		w.OnChange()
	}
	return nil
}

func (w *RingWatcher) refreshWithoutMembershipAuthority(ctx context.Context, staleness time.Duration, startedAt time.Time) error {
	rows, err := w.DB.QueryContext(ctx, `
		SELECT node_id, COALESCE(weight, 100), endpoint
		FROM cluster_nodes
		WHERE status IN ('active', 'joining')
		  AND last_heartbeat > clock_timestamp() - make_interval(secs => $1::double precision / 1000.0)
		  AND NULLIF(BTRIM(endpoint), '') IS NOT NULL
		ORDER BY node_id
	`, staleness.Milliseconds())
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil
		}
		return err
	}
	defer rows.Close()
	members := make([]RingMember, 0)
	for rows.Next() {
		var member RingMember
		var endpoint string
		if err = rows.Scan(&member.NodeID, &member.Weight, &endpoint); err != nil {
			return err
		}
		if strings.TrimSpace(endpoint) != "" {
			members = append(members, member)
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	changed := membersChanged(w.Ring.Members(), members)
	if changed {
		w.Ring.Rebuild(members)
		if w.OnChange != nil {
			w.OnChange()
		}
	}
	if w.OnRefresh != nil {
		w.OnRefresh(startedAt)
	}
	return nil
}

// membershipFingerprint hashes the canonical (node_id, weight) sequence.
// Endpoint and heartbeat values decide eligibility but are deliberately not
// encoded: once a member is eligible, only fields affecting HRW ownership may
// advance the dispatch epoch.
func membershipFingerprint(members []RingMember) []byte {
	canonical := append([]RingMember(nil), members...)
	sort.Slice(canonical, func(i, j int) bool {
		if canonical[i].NodeID == canonical[j].NodeID {
			return canonical[i].Weight < canonical[j].Weight
		}
		return canonical[i].NodeID < canonical[j].NodeID
	})
	h := sha256.New()
	var encoded [8]byte
	for _, member := range canonical {
		binary.BigEndian.PutUint64(encoded[:], uint64(len(member.NodeID)))
		_, _ = h.Write(encoded[:])
		_, _ = h.Write([]byte(member.NodeID))
		binary.BigEndian.PutUint64(encoded[:], uint64(member.Weight))
		_, _ = h.Write(encoded[:])
	}
	return h.Sum(nil)
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
