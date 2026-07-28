package cluster

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const (
	dispatchLeaseMaxTTL      = 15 * time.Second
	dispatchLeaseBatchSize   = 512
	dispatchMinimumSafeGuard = 500 * time.Millisecond
)

var errDispatchReconciliationIncomplete = errors.New("dispatch authority: incomplete lease reconciliation")

// PgDispatchAuthorityConfig configures the database-backed dispatch fence.
// AuthIDs must return a point-in-time copy or an otherwise immutable slice.
type PgDispatchAuthorityConfig struct {
	DB            *sql.DB
	NodeID        string
	Ring          *AuthRing
	RingStaleness time.Duration
	AuthIDs       func() []string
	TimeSource    func() time.Time
	SafetyGuard   time.Duration
}

type dispatchLeaseRequest struct {
	authIDs         []string
	nodeID          string
	instanceID      string
	membershipEpoch int64
	ttl             time.Duration
}

type dispatchLeaseGrant struct {
	authID          string
	membershipEpoch int64
	ownerEpoch      int64
	leaseUntil      time.Time
}

type dispatchLeaseResult struct {
	currentEpoch int64
	databaseNow  time.Time
	grants       []dispatchLeaseGrant
}

type dispatchLeaseKey struct {
	authID     string
	instanceID string
	ownerEpoch int64
}

type dispatchLeaseBackend interface {
	acquire(context.Context, dispatchLeaseRequest) (dispatchLeaseResult, error)
	release(context.Context, []dispatchLeaseKey) error
}

type localDispatchGrant struct {
	membershipEpoch  int64
	ownerEpoch       int64
	databaseDeadline time.Time
	localDeadline    time.Time
}

type dispatchGrantSnapshot struct {
	epoch      int64
	reconciled bool
	grants     map[string]localDispatchGrant
}

// PgDispatchAuthority keeps an immutable, atomically published grant map.
// Admit performs no database calls and takes no mutexes.
type PgDispatchAuthority struct {
	nodeID      string
	instanceID  string
	ring        *AuthRing
	authIDs     func() []string
	now         func() time.Time
	ttl         time.Duration
	interval    time.Duration
	safetyGuard time.Duration
	backend     dispatchLeaseBackend

	snapshot atomic.Pointer[dispatchGrantSnapshot]
	closed   atomic.Bool
	ready    atomic.Bool

	wake   chan struct{}
	cancel context.CancelFunc
	done   chan struct{}

	syncMu sync.Mutex
	// pendingReleases is guarded by syncMu. It retains exact fencing keys
	// whose best-effort release failed, including releases attempted with a
	// canceled reconciliation context during shutdown.
	pendingReleases map[dispatchLeaseKey]struct{}
	notifyMu        sync.Mutex
	notify          chan struct{}
}

// NewPgDispatchAuthority constructs and starts an authority with a fresh UUID
// instance identity. The caller owns it and must eventually call Close.
func NewPgDispatchAuthority(config PgDispatchAuthorityConfig) (*PgDispatchAuthority, error) {
	if config.DB == nil {
		return nil, errors.New("dispatch authority: DB is required")
	}
	return newPgDispatchAuthority(config, &pgDispatchLeaseBackend{db: config.DB}, true)
}

func newPgDispatchAuthority(config PgDispatchAuthorityConfig, backend dispatchLeaseBackend, start bool) (*PgDispatchAuthority, error) {
	nodeID := strings.TrimSpace(config.NodeID)
	if nodeID == "" {
		return nil, errors.New("dispatch authority: NodeID is required")
	}
	if config.Ring == nil {
		return nil, errors.New("dispatch authority: Ring is required")
	}
	if config.AuthIDs == nil {
		return nil, errors.New("dispatch authority: AuthIDs is required")
	}
	if backend == nil {
		return nil, errors.New("dispatch authority: lease backend is required")
	}
	if config.RingStaleness <= 0 {
		return nil, errors.New("dispatch authority: RingStaleness must be positive")
	}
	ttl := config.RingStaleness / 2
	if ttl > dispatchLeaseMaxTTL {
		ttl = dispatchLeaseMaxTTL
	}
	guard := config.SafetyGuard
	if guard < dispatchMinimumSafeGuard {
		guard = dispatchMinimumSafeGuard
	}
	if ttl <= guard {
		return nil, fmt.Errorf("dispatch authority: lease TTL %s must exceed safety guard %s", ttl, guard)
	}
	now := config.TimeSource
	if now == nil {
		now = time.Now
	}
	ctx, cancel := context.WithCancel(context.Background())
	authority := &PgDispatchAuthority{
		nodeID:          nodeID,
		instanceID:      uuid.NewString(),
		ring:            config.Ring,
		authIDs:         config.AuthIDs,
		now:             now,
		ttl:             ttl,
		interval:        ttl / 3,
		safetyGuard:     guard,
		backend:         backend,
		pendingReleases: make(map[dispatchLeaseKey]struct{}),
		wake:            make(chan struct{}, 1),
		cancel:          cancel,
		done:            make(chan struct{}),
		notify:          make(chan struct{}),
	}
	authority.snapshot.Store(&dispatchGrantSnapshot{grants: map[string]localDispatchGrant{}})
	if start {
		go authority.run(ctx)
	} else {
		close(authority.done)
	}
	return authority, nil
}

func (a *PgDispatchAuthority) run(ctx context.Context) {
	defer close(a.done)
	if err := a.syncOnce(ctx); err != nil && ctx.Err() == nil {
		log.WithError(err).Warn("dispatch authority: initial lease reconciliation failed")
	}
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-a.wake:
		}
		if err := a.syncOnce(ctx); err != nil && ctx.Err() == nil {
			log.WithError(err).Debug("dispatch authority: lease reconciliation failed")
		}
	}
}

// Admit checks only immutable local state. The returned release is currently
// a no-op: admission is a start fence, and already admitted work is allowed to
// drain across lease or ring transitions.
func (a *PgDispatchAuthority) Admit(authID string) (func(), bool) {
	if a == nil || a.closed.Load() {
		return nil, false
	}
	decision := a.ring.Decision(authID)
	if !decision.Ready || decision.Owner != a.nodeID {
		return nil, false
	}
	snapshot := a.snapshot.Load()
	if snapshot == nil || snapshot.epoch != decision.Epoch {
		return nil, false
	}
	grant, ok := snapshot.grants[authID]
	if !ok || grant.membershipEpoch != decision.Epoch {
		return nil, false
	}
	now := a.now()
	if !now.Before(grant.localDeadline) || !now.Before(grant.databaseDeadline) {
		return nil, false
	}
	return func() {}, true
}

// Wake requests prompt background reconciliation without blocking the caller.
func (a *PgDispatchAuthority) Wake() {
	if a == nil || a.closed.Load() {
		return
	}
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

// Ready is true only when the published grants were reconciled against the
// ring's current epoch.
func (a *PgDispatchAuthority) Ready() bool {
	if a == nil || a.closed.Load() || !a.ready.Load() {
		return false
	}
	decision := a.ring.Decision("")
	snapshot := a.snapshot.Load()
	return decision.Ready && snapshot != nil && snapshot.reconciled && snapshot.epoch == decision.Epoch
}

// WaitReady waits for a current-epoch grant snapshot without polling.
func (a *PgDispatchAuthority) WaitReady(ctx context.Context) error {
	if a == nil {
		return errors.New("dispatch authority: nil authority")
	}
	for {
		if a.Ready() {
			return nil
		}
		if a.closed.Load() {
			return errors.New("dispatch authority: admissions closed")
		}
		a.notifyMu.Lock()
		if a.Ready() {
			a.notifyMu.Unlock()
			return nil
		}
		if a.closed.Load() {
			a.notifyMu.Unlock()
			return errors.New("dispatch authority: admissions closed")
		}
		notify := a.notify
		a.notifyMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

// CloseAdmissions immediately prevents all new dispatch starts.
func (a *PgDispatchAuthority) CloseAdmissions() {
	if a == nil {
		return
	}
	if a.closed.CompareAndSwap(false, true) {
		a.ready.Store(false)
		a.signalStateChange()
	}
}

// Close stops reconciliation and best-effort releases this instance's exact
// owner epochs. Conditional expiry cannot truncate a successor's lease.
func (a *PgDispatchAuthority) Close(ctx context.Context) error {
	if a == nil {
		return nil
	}
	a.CloseAdmissions()
	a.cancel()
	select {
	case <-a.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	a.syncMu.Lock()
	defer a.syncMu.Unlock()
	snapshot := a.snapshot.Swap(&dispatchGrantSnapshot{grants: map[string]localDispatchGrant{}})
	keys := append(a.releaseKeys(snapshot), a.pendingReleaseKeys()...)
	keys = uniqueReleaseKeys(keys)
	if len(keys) == 0 {
		return nil
	}
	if err := a.backend.release(ctx, keys); err != nil {
		a.rememberPendingReleases(keys)
		return fmt.Errorf("dispatch authority: release leases: %w", err)
	}
	a.forgetPendingReleases(keys)
	return nil
}

func (a *PgDispatchAuthority) syncOnce(ctx context.Context) error {
	a.syncMu.Lock()
	defer a.syncMu.Unlock()
	if a.closed.Load() {
		return errors.New("dispatch authority: admissions closed")
	}
	ringState := a.ring.Decision("")
	previous := a.snapshot.Load()
	if !ringState.Ready {
		a.publish(&dispatchGrantSnapshot{epoch: ringState.Epoch, grants: map[string]localDispatchGrant{}})
		a.releaseBestEffort(ctx, a.releaseKeys(previous))
		return nil
	}

	desired := a.ownedAuthIDs(ringState.Epoch)
	next := &dispatchGrantSnapshot{epoch: ringState.Epoch, reconciled: true, grants: make(map[string]localDispatchGrant, len(desired))}
	allSucceeded := true
	for offset := 0; offset < len(desired) || offset == 0; offset += dispatchLeaseBatchSize {
		end := offset + dispatchLeaseBatchSize
		if end > len(desired) {
			end = len(desired)
		}
		batch := desired[offset:end]
		startedAt := a.now()
		result, err := a.backend.acquire(ctx, dispatchLeaseRequest{
			authIDs:         batch,
			nodeID:          a.nodeID,
			instanceID:      a.instanceID,
			membershipEpoch: ringState.Epoch,
			ttl:             a.ttl,
		})
		if err != nil {
			allSucceeded = false
			for _, authID := range batch {
				if grant, ok := previous.grants[authID]; ok && grant.membershipEpoch == ringState.Epoch {
					next.grants[authID] = grant
				}
			}
			if len(desired) == 0 {
				break
			}
			continue
		}
		if result.currentEpoch != ringState.Epoch {
			allSucceeded = false
		} else {
			requested := make(map[string]struct{}, len(batch))
			for _, authID := range batch {
				requested[authID] = struct{}{}
			}
			granted := make(map[string]struct{}, len(result.grants))
			for _, grant := range result.grants {
				if _, ok := requested[grant.authID]; !ok {
					continue
				}
				remaining := grant.leaseUntil.Sub(result.databaseNow) - a.safetyGuard
				if grant.membershipEpoch != ringState.Epoch || remaining <= 0 {
					continue
				}
				granted[grant.authID] = struct{}{}
				next.grants[grant.authID] = localDispatchGrant{
					membershipEpoch:  grant.membershipEpoch,
					ownerEpoch:       grant.ownerEpoch,
					databaseDeadline: grant.leaseUntil.Add(-a.safetyGuard),
					localDeadline:    startedAt.Add(remaining),
				}
			}
			completedAt := a.now()
			for _, authID := range batch {
				if _, ok := granted[authID]; ok {
					continue
				}
				allSucceeded = false
				if previousGrant, ok := previous.grants[authID]; ok &&
					previousGrant.membershipEpoch == ringState.Epoch &&
					completedAt.Before(previousGrant.localDeadline) &&
					completedAt.Before(previousGrant.databaseDeadline) {
					next.grants[authID] = previousGrant
				}
			}
		}
		if len(desired) == 0 {
			break
		}
	}

	currentRing := a.ring.Decision("")
	if !currentRing.Ready || currentRing.Epoch != ringState.Epoch {
		keys := append(a.releaseKeys(next), a.releaseKeys(previous)...)
		a.releaseBestEffort(ctx, uniqueReleaseKeys(keys))
		a.publish(&dispatchGrantSnapshot{epoch: currentRing.Epoch, grants: map[string]localDispatchGrant{}})
		return nil
	}
	next.reconciled = allSucceeded
	a.publish(next)
	a.releaseBestEffort(ctx, releaseDifference(previous, next, a.instanceID))
	if !allSucceeded {
		return errDispatchReconciliationIncomplete
	}
	return nil
}

func (a *PgDispatchAuthority) ownedAuthIDs(epoch int64) []string {
	seen := make(map[string]struct{})
	for _, rawID := range a.authIDs() {
		authID := strings.TrimSpace(rawID)
		if authID == "" {
			continue
		}
		decision := a.ring.Decision(authID)
		if decision.Ready && decision.Epoch == epoch && decision.Owner == a.nodeID {
			seen[authID] = struct{}{}
		}
	}
	ids := make([]string, 0, len(seen))
	for authID := range seen {
		ids = append(ids, authID)
	}
	sort.Strings(ids)
	return ids
}

func (a *PgDispatchAuthority) publish(snapshot *dispatchGrantSnapshot) {
	a.snapshot.Store(snapshot)
	a.ready.Store(snapshot.reconciled)
	a.signalStateChange()
}

func (a *PgDispatchAuthority) signalStateChange() {
	a.notifyMu.Lock()
	close(a.notify)
	a.notify = make(chan struct{})
	a.notifyMu.Unlock()
}

func (a *PgDispatchAuthority) releaseKeys(snapshot *dispatchGrantSnapshot) []dispatchLeaseKey {
	if snapshot == nil {
		return nil
	}
	keys := make([]dispatchLeaseKey, 0, len(snapshot.grants))
	for authID, grant := range snapshot.grants {
		keys = append(keys, dispatchLeaseKey{authID: authID, instanceID: a.instanceID, ownerEpoch: grant.ownerEpoch})
	}
	return keys
}

func releaseDifference(previous, next *dispatchGrantSnapshot, instanceID string) []dispatchLeaseKey {
	if previous == nil {
		return nil
	}
	keys := make([]dispatchLeaseKey, 0)
	for authID, grant := range previous.grants {
		nextGrant, retained := next.grants[authID]
		if retained && nextGrant.ownerEpoch == grant.ownerEpoch {
			continue
		}
		keys = append(keys, dispatchLeaseKey{authID: authID, instanceID: instanceID, ownerEpoch: grant.ownerEpoch})
	}
	return keys
}

func (a *PgDispatchAuthority) releaseBestEffort(ctx context.Context, keys []dispatchLeaseKey) {
	if len(keys) == 0 {
		return
	}
	if err := a.backend.release(ctx, keys); err != nil {
		a.rememberPendingReleases(keys)
		if ctx.Err() == nil {
			log.WithError(err).Debug("dispatch authority: best-effort lease release failed")
		}
		return
	}
	a.forgetPendingReleases(keys)
}

func (a *PgDispatchAuthority) rememberPendingReleases(keys []dispatchLeaseKey) {
	for _, key := range keys {
		a.pendingReleases[key] = struct{}{}
	}
}

func (a *PgDispatchAuthority) forgetPendingReleases(keys []dispatchLeaseKey) {
	for _, key := range keys {
		delete(a.pendingReleases, key)
	}
}

func (a *PgDispatchAuthority) pendingReleaseKeys() []dispatchLeaseKey {
	keys := make([]dispatchLeaseKey, 0, len(a.pendingReleases))
	for key := range a.pendingReleases {
		keys = append(keys, key)
	}
	return keys
}

func uniqueReleaseKeys(keys []dispatchLeaseKey) []dispatchLeaseKey {
	if len(keys) < 2 {
		return keys
	}
	seen := make(map[dispatchLeaseKey]struct{}, len(keys))
	unique := make([]dispatchLeaseKey, 0, len(keys))
	for _, key := range keys {
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, key)
	}
	return unique
}

type pgDispatchLeaseBackend struct {
	db *sql.DB
}

func (b *pgDispatchLeaseBackend) acquire(ctx context.Context, request dispatchLeaseRequest) (dispatchLeaseResult, error) {
	rows, err := b.db.QueryContext(ctx, `
		WITH current_state AS (
			SELECT epoch FROM cluster_membership_state WHERE id = 1 FOR SHARE
		), requested(auth_id) AS (
			SELECT UNNEST($1::text[])
		), granted AS (
			INSERT INTO auth_dispatch_leases (
				auth_id, owner_node_id, owner_instance_id, membership_epoch,
				owner_epoch, lease_until, updated_at
			)
			SELECT r.auth_id, $2, $3::uuid, $4, 1,
				clock_timestamp() + make_interval(secs => $5::double precision / 1000.0), clock_timestamp()
			FROM requested r, current_state s
			WHERE s.epoch = $4
			ON CONFLICT (auth_id) DO UPDATE SET
				owner_node_id = EXCLUDED.owner_node_id,
				owner_instance_id = EXCLUDED.owner_instance_id,
				membership_epoch = EXCLUDED.membership_epoch,
				owner_epoch = CASE
					WHEN auth_dispatch_leases.owner_instance_id = EXCLUDED.owner_instance_id
					 AND auth_dispatch_leases.membership_epoch = EXCLUDED.membership_epoch
					THEN auth_dispatch_leases.owner_epoch
					ELSE auth_dispatch_leases.owner_epoch + 1
				END,
				lease_until = EXCLUDED.lease_until,
				updated_at = clock_timestamp()
			WHERE (
				auth_dispatch_leases.owner_instance_id = EXCLUDED.owner_instance_id
				OR auth_dispatch_leases.lease_until <= clock_timestamp()
			)
			RETURNING auth_id, membership_epoch, owner_epoch, lease_until
		)
		SELECT s.epoch, clock_timestamp(), g.auth_id, g.membership_epoch, g.owner_epoch, g.lease_until
		FROM current_state s
		LEFT JOIN granted g ON TRUE
	`, request.authIDs, request.nodeID, request.instanceID, request.membershipEpoch, request.ttl.Milliseconds())
	if err != nil {
		return dispatchLeaseResult{}, fmt.Errorf("acquire dispatch leases: %w", err)
	}
	defer rows.Close()
	var result dispatchLeaseResult
	sawState := false
	for rows.Next() {
		sawState = true
		var (
			authID          sql.NullString
			membershipEpoch sql.NullInt64
			ownerEpoch      sql.NullInt64
			leaseUntil      sql.NullTime
		)
		if err = rows.Scan(&result.currentEpoch, &result.databaseNow, &authID, &membershipEpoch, &ownerEpoch, &leaseUntil); err != nil {
			return dispatchLeaseResult{}, fmt.Errorf("scan dispatch lease grant: %w", err)
		}
		if authID.Valid && membershipEpoch.Valid && ownerEpoch.Valid && leaseUntil.Valid {
			result.grants = append(result.grants, dispatchLeaseGrant{
				authID: authID.String, membershipEpoch: membershipEpoch.Int64,
				ownerEpoch: ownerEpoch.Int64, leaseUntil: leaseUntil.Time,
			})
		}
	}
	if err = rows.Err(); err != nil {
		return dispatchLeaseResult{}, fmt.Errorf("iterate dispatch lease grants: %w", err)
	}
	if !sawState {
		return dispatchLeaseResult{}, errors.New("acquire dispatch leases: cluster membership singleton is missing")
	}
	return result, nil
}

func (b *pgDispatchLeaseBackend) release(ctx context.Context, keys []dispatchLeaseKey) error {
	if len(keys) == 0 {
		return nil
	}
	authIDs := make([]string, len(keys))
	ownerEpochs := make([]int64, len(keys))
	instanceID := keys[0].instanceID
	for i, key := range keys {
		authIDs[i] = key.authID
		ownerEpochs[i] = key.ownerEpoch
	}
	_, err := b.db.ExecContext(ctx, `
		UPDATE auth_dispatch_leases AS leases
		SET lease_until = LEAST(leases.lease_until, clock_timestamp()),
			updated_at = clock_timestamp()
		FROM UNNEST($1::text[], $2::bigint[]) AS stale(auth_id, owner_epoch)
		WHERE leases.auth_id = stale.auth_id
		  AND leases.owner_instance_id = $3::uuid
		  AND leases.owner_epoch = stale.owner_epoch
	`, authIDs, ownerEpochs, instanceID)
	return err
}
