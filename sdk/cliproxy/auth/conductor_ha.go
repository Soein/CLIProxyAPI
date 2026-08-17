package auth

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	log "github.com/sirupsen/logrus"
)

const authStoreConflictReloadTimeout = 5 * time.Second

func (m *Manager) authOwnershipPredicate() (func(string) bool, bool) {
	if m == nil {
		return nil, false
	}
	m.clusterMu.RLock()
	enabled := m.authShardingEnabled
	ring := m.authRing
	spillover := m.spilloverEnabled
	m.clusterMu.RUnlock()
	if !enabled {
		return nil, false
	}
	if ring == nil || !ring.Ready() {
		return func(string) bool { return false }, false
	}
	return ring.IsMine, spillover
}

func (m *Manager) BeginEnableTransition(ids []string) func() {
	if m == nil {
		return func() {}
	}
	normalizedIDs := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		normalizedIDs = append(normalizedIDs, id)
	}
	if len(normalizedIDs) == 0 {
		return func() {}
	}
	m.mu.Lock()
	m.beginEnableTransitionLocked(normalizedIDs)
	m.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() { m.endEnableTransition(normalizedIDs) })
	}
}

func (m *Manager) beginEnableTransitionLocked(ids []string) {
	if m.enablingTransitions == nil {
		m.enablingTransitions = make(map[string]int)
	}
	for _, id := range ids {
		m.enablingTransitions[id]++
	}
}

func (m *Manager) endEnableTransition(ids []string) {
	if m == nil || len(ids) == 0 {
		return
	}
	m.mu.Lock()
	m.endEnableTransitionLocked(ids)
	m.mu.Unlock()
}

func (m *Manager) endEnableTransitionLocked(ids []string) {
	for _, id := range ids {
		remaining := m.enablingTransitions[id] - 1
		if remaining > 0 {
			m.enablingTransitions[id] = remaining
		} else {
			delete(m.enablingTransitions, id)
		}
	}
}

func (m *Manager) shouldTrackDisabledPersistenceLocked(ctx context.Context, auth *Auth) bool {
	return m.shouldPersistAuthLocked(ctx, auth)
}

func (m *Manager) shouldPersistAuthLocked(ctx context.Context, auth *Auth) bool {
	return m.store != nil && !shouldSkipPersist(ctx) && auth != nil && !isExplicitlyNonPersistentAuth(auth) && auth.Metadata != nil
}

func (m *Manager) shouldPersistAuth(ctx context.Context, auth *Auth) bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	shouldPersist := m.shouldPersistAuthLocked(ctx, auth)
	m.mu.RUnlock()
	return shouldPersist
}

type authAtomicBatchStore interface {
	SaveBatch(ctx context.Context, auths []*Auth, finalize func(commit func() error) error) error
}

func (m *Manager) SetDisabled(ctx context.Context, ids []string, disabled bool) ([]*Auth, error) {
	if m == nil {
		return nil, nil
	}

	normalizedIDs := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		normalizedIDs = append(normalizedIDs, id)
	}
	if len(normalizedIDs) == 0 {
		return []*Auth{}, nil
	}
	updated := make([]*Auth, 0, len(normalizedIDs))
	var unlockPersistence func()
	if !disabled {
		unlockPersistence = m.lockAuthPersistence(normalizedIDs)
	}
	releasePersistence := func() {
		if unlockPersistence == nil {
			return
		}
		unlockPersistence()
		unlockPersistence = nil
	}
	defer releasePersistence()
	enableTransitionActive := false
	defer func() {
		if enableTransitionActive {
			m.endEnableTransition(normalizedIDs)
		}
	}()
	convergeStoreConflict := func(persistErr error) ([]*Auth, bool) {
		if !authStoreRequiresAuthoritativeReload(persistErr) {
			return nil, false
		}
		// Reload may re-enter persistence while merging a pending disable, so
		// release lifecycle locks and close the enable transition first.
		releasePersistence()
		if enableTransitionActive {
			m.endEnableTransition(normalizedIDs)
			enableTransitionActive = false
		}
		desired := make(map[string]*Auth, len(updated))
		for _, auth := range updated {
			if auth != nil {
				desired[auth.ID] = auth.Clone()
			}
		}
		return m.convergeAuthStoreConflicts(ctx, normalizedIDs, persistErr, desired, &disabled)
	}

	now := time.Now()
	originals := make([]*Auth, 0, len(normalizedIDs))
	expectedDurableRevisions := make([]uint64, 0, len(normalizedIDs))
	cooldownStateChanged := false
	m.mu.Lock()
	for _, id := range normalizedIDs {
		if auth := m.auths[id]; auth == nil {
			m.mu.Unlock()
			return nil, fmt.Errorf("auth %q not found", id)
		}
	}
	if !disabled {
		m.beginEnableTransitionLocked(normalizedIDs)
		enableTransitionActive = true
	}
	for _, id := range normalizedIDs {
		auth := m.auths[id]
		if !disabled {
			expectedDurableRevisions = append(expectedDurableRevisions, auth.durableRevision)
			originals = append(originals, auth.Clone())
			auth = auth.Clone()
		}
		auth.Disabled = disabled
		if disabled {
			auth.Status = StatusDisabled
			auth.StatusMessage = "disabled via management API"
		} else {
			auth.Status = StatusActive
			auth.StatusMessage = ""
		}
		if auth.Metadata == nil {
			auth.Metadata = make(map[string]any)
		}
		auth.Metadata["disabled"] = disabled
		auth.UpdatedAt = now
		if clearCooldownStateForAuth(auth, now) {
			cooldownStateChanged = true
		}
		if disabled {
			auth.revision = m.nextAuthRevisionLocked()
			auth.durableRevision = m.nextAuthDurableRevisionLocked()
			if m.shouldTrackDisabledPersistenceLocked(ctx, auth) {
				if m.pendingDisabledPersistence == nil {
					m.pendingDisabledPersistence = make(map[string]struct{})
				}
				m.pendingDisabledPersistence[id] = struct{}{}
			} else {
				delete(m.pendingDisabledPersistence, id)
			}
		}
		updated = append(updated, auth.Clone())
	}
	if disabled && m.scheduler != nil {
		m.scheduler.applyBatch(updated, nil)
	}
	m.mu.Unlock()

	if !disabled {
		rollbackPersistence := func(cause error) error {
			rollbackSnapshots := make([]*Auth, 0, len(originals))
			m.mu.Lock()
			for _, original := range originals {
				if original == nil {
					continue
				}
				current := m.auths[original.ID]
				if (original.Disabled || original.Status == StatusDisabled) && current != nil && (current.Disabled || current.Status == StatusDisabled) {
					if m.pendingDisabledPersistence == nil {
						m.pendingDisabledPersistence = make(map[string]struct{})
					}
					m.pendingDisabledPersistence[original.ID] = struct{}{}
					rollbackSnapshots = append(rollbackSnapshots, current.Clone())
					continue
				}
				rollbackSnapshots = append(rollbackSnapshots, original.Clone())
			}
			m.mu.Unlock()

			rollbackCtx := context.Background()
			if ctx != nil {
				rollbackCtx = context.WithoutCancel(ctx)
			}
			rollbackErrors := []error{cause}
			for _, snapshot := range rollbackSnapshots {
				if errRestore := m.persist(rollbackCtx, snapshot); errRestore != nil {
					rollbackErrors = append(rollbackErrors, fmt.Errorf("restore auth %q after failed enable: %w", snapshot.ID, errRestore))
				}
			}
			return errors.Join(rollbackErrors...)
		}

		batchStore, batchAuths, useAtomicBatch := m.atomicBatchPersistence(ctx, updated)
		if useAtomicBatch {
			finalizeCalled := false
			committed := false
			errPersist := batchStore.SaveBatch(ctx, batchAuths, func(commit func() error) error {
				finalizeCalled = true
				if commit == nil {
					return errors.New("auth batch commit callback is nil")
				}

				m.mu.Lock()
				defer m.mu.Unlock()
				for index, id := range normalizedIDs {
					current := m.auths[id]
					if current == nil || current.durableRevision != expectedDurableRevisions[index] {
						return fmt.Errorf("auth %q changed while enabling", id)
					}
				}
				if ctx != nil {
					if errCtx := ctx.Err(); errCtx != nil {
						return errCtx
					}
				}
				if errCommit := commit(); errCommit != nil {
					return errCommit
				}
				committed = true
				for index, id := range normalizedIDs {
					current := m.auths[id]
					updated[index].Runtime = current.Runtime
					updated[index].Success = current.Success
					updated[index].Failed = current.Failed
					updated[index].recentRequests = current.recentRequests
					updated[index].revision = m.nextAuthRevisionLocked()
					updated[index].durableRevision = m.nextAuthDurableRevisionLocked()
					m.auths[id] = updated[index].Clone()
					delete(m.pendingDisabledPersistence, id)
				}
				if m.scheduler != nil {
					m.scheduler.applyBatch(updated, nil)
				}
				m.endEnableTransitionLocked(normalizedIDs)
				enableTransitionActive = false
				return nil
			})
			if committed {
				if errPersist != nil {
					log.WithError(errPersist).Error("auth batch store returned an error after commit; treating the committed batch as successful")
				}
			} else {
				if errPersist == nil {
					if !finalizeCalled {
						errPersist = errors.New("auth batch store did not invoke finalize")
					} else {
						errPersist = errors.New("auth batch store returned without committing")
					}
				}
				persistErr := fmt.Errorf("persist auth batch: %w", errPersist)
				if converged, okConverged := convergeStoreConflict(persistErr); okConverged && errors.Is(errPersist, ErrAuthStoreCommitUnknown) {
					return converged, nil
				}
				return nil, persistErr
			}
		} else {
			persistErrors := make([]error, 0)
			for _, auth := range updated {
				if errPersist := m.persist(ctx, auth); errPersist != nil {
					persistErrors = append(persistErrors, fmt.Errorf("persist auth %q: %w", auth.ID, errPersist))
				}
			}
			if errPersist := errors.Join(persistErrors...); errPersist != nil {
				if errors.Is(errPersist, ErrAuthStoreCommitUnknown) {
					if converged, okConverged := convergeStoreConflict(errPersist); okConverged {
						return converged, nil
					}
					return nil, errPersist
				}
				rollbackErr := rollbackPersistence(errPersist)
				convergeStoreConflict(rollbackErr)
				return nil, rollbackErr
			}
			if ctx != nil {
				if errCtx := ctx.Err(); errCtx != nil {
					return nil, rollbackPersistence(errCtx)
				}
			}

			var conflictErr error
			m.mu.Lock()
			for index, id := range normalizedIDs {
				current := m.auths[id]
				if current == nil || current.durableRevision != expectedDurableRevisions[index] {
					conflictErr = fmt.Errorf("auth %q changed while enabling", id)
					break
				}
			}
			if conflictErr == nil && ctx != nil {
				conflictErr = ctx.Err()
			}
			if conflictErr == nil {
				for index, id := range normalizedIDs {
					current := m.auths[id]
					updated[index].Runtime = current.Runtime
					updated[index].Success = current.Success
					updated[index].Failed = current.Failed
					updated[index].recentRequests = current.recentRequests
					updated[index].revision = m.nextAuthRevisionLocked()
					updated[index].durableRevision = m.nextAuthDurableRevisionLocked()
					m.auths[id] = updated[index].Clone()
					delete(m.pendingDisabledPersistence, id)
				}
				if m.scheduler != nil {
					m.scheduler.applyBatch(updated, nil)
				}
				m.endEnableTransitionLocked(normalizedIDs)
				enableTransitionActive = false
			}
			m.mu.Unlock()
			if conflictErr != nil {
				return nil, rollbackPersistence(conflictErr)
			}
		}
	}

	if !shouldDeferAPIKeyModelAliasRebuild(ctx) {
		m.rebuildAPIKeyModelAliasFromRuntimeConfig()
	}
	for _, auth := range updated {
		m.queueRefreshReschedule(auth.ID)
	}
	releasePersistence()

	persistErrors := make([]error, 0)
	batchPersisted := false
	if disabled {
		var errBatch error
		batchPersisted, errBatch = m.persistPendingDisabledAtomicBatch(ctx, normalizedIDs)
		if errBatch != nil {
			persistErrors = append(persistErrors, fmt.Errorf("persist disabled auth batch: %w", errBatch))
		}
	}
	for _, auth := range updated {
		if disabled && !batchPersisted {
			if errPersist := m.persistPendingDisabled(ctx, auth.ID); errPersist != nil {
				persistErrors = append(persistErrors, fmt.Errorf("persist auth %q: %w", auth.ID, errPersist))
			}
		}
		m.hook.OnAuthUpdated(ctx, auth.Clone())
	}
	if cooldownStateChanged {
		m.persistCooldownStates(ctx)
	}
	errPersist := errors.Join(persistErrors...)
	if converged, okConverged := convergeStoreConflict(errPersist); okConverged && errors.Is(errPersist, ErrAuthStoreCommitUnknown) {
		return converged, nil
	}
	return updated, errPersist
}

func (m *Manager) atomicBatchPersistence(ctx context.Context, auths []*Auth) (authAtomicBatchStore, []*Auth, bool) {
	if m == nil || shouldSkipPersist(ctx) {
		return nil, nil, false
	}
	m.mu.RLock()
	store := m.store
	m.mu.RUnlock()
	batchStore, ok := store.(authAtomicBatchStore)
	if !ok || batchStore == nil {
		return nil, nil, false
	}
	persistable := make([]*Auth, 0, len(auths))
	for _, auth := range auths {
		if auth == nil || auth.Metadata == nil || isExplicitlyNonPersistentAuth(auth) {
			continue
		}
		persistable = append(persistable, auth)
	}
	if len(persistable) == 0 {
		return nil, nil, false
	}
	return batchStore, persistable, true
}

func (m *Manager) persistPendingDisabledAtomicBatch(ctx context.Context, ids []string) (bool, error) {
	if m == nil || shouldSkipPersist(ctx) {
		return false, nil
	}
	m.mu.RLock()
	store := m.store
	m.mu.RUnlock()
	batchStore, ok := store.(authAtomicBatchStore)
	if !ok || batchStore == nil {
		return false, nil
	}

	unlock := m.lockAuthPersistence(ids)
	defer unlock()

	m.mu.RLock()
	snapshots := make([]*Auth, 0, len(ids))
	expectedGenerations := make(map[string]uint64, len(ids))
	for _, id := range ids {
		_, pending := m.pendingDisabledPersistence[id]
		current := m.auths[id]
		if !pending || !authIsDisabled(current) || current.Metadata == nil || isExplicitlyNonPersistentAuth(current) {
			continue
		}
		snapshot := current.Clone()
		snapshots = append(snapshots, snapshot)
		expectedGenerations[id] = snapshot.StoreGeneration()
	}
	m.mu.RUnlock()
	if len(snapshots) == 0 {
		return true, nil
	}

	finalizeCalled := false
	committed := false
	errPersist := batchStore.SaveBatch(ctx, snapshots, func(commit func() error) error {
		finalizeCalled = true
		if commit == nil {
			return errors.New("auth batch commit callback is nil")
		}
		if ctx != nil {
			if errCtx := ctx.Err(); errCtx != nil {
				return errCtx
			}
		}
		if errCommit := commit(); errCommit != nil {
			return errCommit
		}
		committed = true
		for _, snapshot := range snapshots {
			if snapshot == nil {
				continue
			}
			m.mergeCommittedStoreGeneration(snapshot, expectedGenerations[snapshot.ID], snapshot.StoreGeneration())
		}
		return nil
	})
	if committed {
		if errPersist != nil {
			log.WithError(errPersist).Error("auth batch store returned an error after disabled batch commit; treating the committed batch as successful")
		}
		return true, nil
	}
	if errPersist == nil {
		if !finalizeCalled {
			errPersist = errors.New("auth batch store did not invoke finalize")
		} else {
			errPersist = errors.New("auth batch store returned without committing")
		}
	}
	return true, errPersist
}

type removedAuthState struct {
	id       string
	provider string
	revision uint64
}

func (m *Manager) DeleteAuths(ctx context.Context, ids []string, deletePersistent func(context.Context) error) error {
	if deletePersistent == nil {
		return errors.New("auth persistent delete callback is nil")
	}
	if m == nil {
		return deletePersistent(ctx)
	}

	unlockPersistence := m.lockAuthPersistence(ids)
	if errDelete := deletePersistent(ctx); errDelete != nil {
		unlockPersistence()
		if authStoreRequiresAuthoritativeReload(errDelete) {
			m.reloadAfterAuthStoreConflicts(ctx, ids, errDelete)
		}
		return errDelete
	}

	m.mu.Lock()
	removed := m.removeAuthsLocked(ids)
	m.mu.Unlock()
	// Release persistence locks before callbacks and other cleanup. Hooks may
	// re-enter Manager lifecycle methods and must never run under these locks.
	unlockPersistence()
	m.cleanupRemovedAuths(ctx, removed)
	return nil
}

func (m *Manager) removeAuthsLocked(ids []string) []removedAuthState {
	removed := make([]removedAuthState, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}

		// A successful persistent delete supersedes every locally published
		// generation, including one whose stale Save is waiting on the lock.
		m.clearPersistenceInFlightLocked(id, 0)
		delete(m.pendingDisabledPersistence, id)
		delete(m.enablingTransitions, id)
		if m.modelPoolOffsets != nil {
			delete(m.modelPoolOffsets, id)
		}
		for sessionID, sessionAuths := range m.homeRuntimeAuths {
			if sessionAuths == nil {
				continue
			}
			delete(sessionAuths, id)
			if len(sessionAuths) == 0 {
				delete(m.homeRuntimeAuths, sessionID)
			}
		}

		existing := m.auths[id]
		if existing == nil {
			continue
		}
		delete(m.auths, id)
		removed = append(removed, removedAuthState{
			id:       id,
			provider: strings.TrimSpace(existing.Provider),
			revision: m.nextAuthRevisionLocked(),
		})
	}
	return removed
}

func (m *Manager) cleanupRemovedAuths(ctx context.Context, removed []removedAuthState) {
	if m == nil || len(removed) == 0 {
		return
	}

	if !shouldDeferAPIKeyModelAliasRebuild(ctx) {
		m.rebuildAPIKeyModelAliasFromRuntimeConfig()
	}
	for _, state := range removed {
		if m.scheduler != nil {
			m.scheduler.removeAuthAtRevision(state.id, state.revision)
		}
		m.queueRefreshUnschedule(state.id)
		m.invalidateSessionAffinity(state.id)
		if removalHook, okRemoval := m.hook.(AuthRemovalHook); okRemoval {
			removalHook.OnAuthRemoved(ctx, state.id)
		}
		if state.provider != "" {
			if exec, ok := m.Executor(state.provider); ok && exec != nil {
				if closer, okCloser := exec.(ExecutionSessionCloser); okCloser {
					closer.CloseExecutionSession(CloseAllExecutionSessionsID)
				}
			}
		}
	}
	m.wakeDispatchAuthority()
	m.persistCooldownStates(ctx)
}

type authAuthoritativeListStore interface {
	ListAuthoritative(ctx context.Context) ([]*Auth, error)
}

func mergePersistedAuthRuntime(persisted, current *Auth) *Auth {
	if persisted == nil {
		return nil
	}
	merged := persisted.Clone()
	if current == nil {
		merged.EnsureIndex()
		return merged
	}

	local := current.Clone()
	if local.indexAssigned || strings.TrimSpace(local.Index) != "" {
		merged.Index = local.Index
		merged.indexAssigned = local.indexAssigned
	} else {
		merged.EnsureIndex()
	}
	if merged.Storage == nil {
		merged.Storage = local.Storage
	}
	if strings.TrimSpace(merged.FileName) == "" {
		merged.FileName = local.FileName
	}
	merged.Runtime = local.Runtime
	merged.Success = local.Success
	merged.Failed = local.Failed
	merged.recentRequests = local.recentRequests

	persistedDisabled := merged.Disabled || merged.Status == StatusDisabled
	currentDisabled := local.Disabled || local.Status == StatusDisabled
	if !persistedDisabled && !currentDisabled {
		merged.Status = local.Status
		merged.StatusMessage = local.StatusMessage
		merged.Unavailable = local.Unavailable
		merged.Quota = local.Quota
		merged.LastError = local.LastError
		merged.NextRetryAfter = local.NextRetryAfter
		merged.ModelStates = local.ModelStates
	}
	return merged
}

func authIsDisabled(auth *Auth) bool {
	return auth != nil && (auth.Disabled || auth.Status == StatusDisabled)
}

func applyDisabledRuntimeState(snapshot, current *Auth) {
	if snapshot == nil {
		return
	}
	snapshot.Disabled = true
	snapshot.Status = StatusDisabled
	if current != nil {
		snapshot.StatusMessage = current.StatusMessage
		snapshot.Unavailable = current.Unavailable
		snapshot.Quota = current.Quota
		snapshot.LastError = current.LastError
		snapshot.NextRetryAfter = current.NextRetryAfter
		snapshot.ModelStates = current.ModelStates
		snapshot.Runtime = current.Runtime
	}
	if snapshot.Metadata == nil {
		snapshot.Metadata = make(map[string]any)
	}
	snapshot.Metadata["disabled"] = true
}

func (m *Manager) carryDisabledAdmissionLocked(id string, snapshot, current *Auth) {
	if m == nil || snapshot == nil || !m.shouldCarryDisabledRuntimeLocked(id, current) {
		return
	}
	applyDisabledRuntimeState(snapshot, current)
}

func (m *Manager) shouldCarryDisabledRuntimeLocked(id string, current *Auth) bool {
	if m == nil {
		return false
	}
	_, pendingDisable := m.pendingDisabledPersistence[id]
	enabling := m.enablingTransitions[id] > 0
	if current == nil {
		return enabling
	}
	return authIsDisabled(current) && (pendingDisable || enabling)
}

func (m *Manager) shouldRetainDisabledSnapshotLocked(id string, snapshot, current *Auth) bool {
	if m == nil {
		return false
	}
	_, pendingDisable := m.pendingDisabledPersistence[id]
	enabling := m.enablingTransitions[id] > 0
	currentDisabled := authIsDisabled(current)
	if snapshot == nil {
		if enabling && currentDisabled {
			return true
		}
		if !enabling {
			delete(m.pendingDisabledPersistence, id)
		}
		return false
	}
	snapshotDisabled := authIsDisabled(snapshot)
	if current == nil {
		if enabling && !snapshotDisabled {
			return true
		}
		delete(m.pendingDisabledPersistence, id)
		return false
	}
	if !currentDisabled {
		if !enabling {
			delete(m.pendingDisabledPersistence, id)
		}
		return false
	}
	if snapshotDisabled {
		if pendingDisable {
			delete(m.pendingDisabledPersistence, id)
		}
		return enabling
	}
	return pendingDisable || enabling
}

func (m *Manager) mergeFailClosedStoreSnapshotLocked(id string, snapshot, current *Auth) (*Auth, bool) {
	if !m.shouldRetainDisabledSnapshotLocked(id, snapshot, current) {
		return nil, false
	}
	if snapshot == nil {
		return current.Clone(), true
	}
	merged := mergePersistedAuthRuntime(snapshot, current)
	applyDisabledRuntimeState(merged, current)
	merged.durableRevision = m.nextAuthDurableRevisionLocked()
	if m.enablingTransitions[id] > 0 || current == nil {
		merged.revision = m.nextAuthRevisionLocked()
	} else {
		merged.revision = current.revision
	}
	return merged, true
}

func (m *Manager) Reconcile(ctx context.Context) error {
	return m.load(ctx, true)
}

func (m *Manager) load(ctx context.Context, authoritative bool) error {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	store := m.store
	baselineDurableRevisions := make(map[string]uint64, len(m.auths))
	baselinePersistenceInFlight := make(map[string]uint64, len(m.persistenceInFlightRevisions))
	for id, auth := range m.auths {
		if auth != nil {
			baselineDurableRevisions[id] = auth.durableRevision
		}
	}
	for id, revision := range m.persistenceInFlightRevisions {
		baselinePersistenceInFlight[id] = revision
	}
	m.mu.RUnlock()
	if store == nil {
		return nil
	}
	var (
		items []*Auth
		err   error
	)
	if authoritativeStore, ok := store.(authAuthoritativeListStore); authoritative && ok {
		items, err = authoritativeStore.ListAuthoritative(ctx)
	} else {
		items, err = store.List(ctx)
	}
	if err != nil {
		return err
	}

	m.mu.Lock()
	previous := m.auths
	next := make(map[string]*Auth, len(items))
	loadedSnapshots := make([]*Auth, 0, len(items))
	registeredIDs := make(map[string]struct{}, len(items))
	persistMergedDisabledIDs := make(map[string]struct{})
	for _, auth := range items {
		if auth == nil || auth.ID == "" {
			continue
		}
		if errWeight := ValidateAuthWeight(auth); errWeight != nil {
			continue
		}
		current := previous[auth.ID]
		baselineDurableRevision, existedAtStart := baselineDurableRevisions[auth.ID]
		if existedAtStart && current == nil {
			// The auth was removed while the store snapshot was in flight. Treat
			// its absence as a local tombstone so a stale row cannot restore it.
			continue
		}
		var merged *Auth
		persistenceWasInFlight := baselineDurableRevision != 0 && baselinePersistenceInFlight[auth.ID] == baselineDurableRevision
		if current != nil && (!existedAtStart || current.durableRevision != baselineDurableRevision || persistenceWasInFlight) {
			// A local mutation won the race with the store round trip. Keep it
			// intact, including its durable generation, so an in-flight
			// persistPublishedIfCurrent can still save it. A subsequent
			// reconciliation can apply the store snapshot.
			merged = current.Clone()
		} else if retained, shouldRetain := m.mergeFailClosedStoreSnapshotLocked(auth.ID, auth, current); shouldRetain {
			merged = retained
			if !authIsDisabled(auth) {
				if _, pendingDisable := m.pendingDisabledPersistence[auth.ID]; pendingDisable && m.enablingTransitions[auth.ID] == 0 {
					persistMergedDisabledIDs[auth.ID] = struct{}{}
				}
			}
		} else {
			merged = mergePersistedAuthRuntime(auth, current)
			merged.revision = m.nextAuthRevisionLocked()
			merged.durableRevision = m.nextAuthDurableRevisionLocked()
		}
		next[auth.ID] = merged
		loadedSnapshots = append(loadedSnapshots, merged.Clone())
		if previous[auth.ID] == nil {
			registeredIDs[auth.ID] = struct{}{}
		}
	}
	// Store reconciliation must not evict auths whose lifecycle is explicitly
	// owned by runtime config or plugins rather than by the backing store.
	for id, auth := range previous {
		if auth == nil {
			continue
		}
		if next[id] != nil {
			continue
		}
		if m.shouldRetainDisabledSnapshotLocked(id, nil, auth) {
			next[id] = auth.Clone()
			continue
		}
		baselineDurableRevision, existedAtStart := baselineDurableRevisions[id]
		changedDuringList := !existedAtStart || auth.durableRevision != baselineDurableRevision
		persistenceWasInFlight := baselineDurableRevision != 0 && baselinePersistenceInFlight[id] == baselineDurableRevision
		if !changedDuringList && !persistenceWasInFlight && !isExplicitlyNonPersistentAuth(auth) {
			continue
		}
		next[id] = auth.Clone()
	}
	removedIDs := make([]string, 0)
	removedRevisions := make(map[string]uint64)
	removedProviders := make(map[string]string)
	for id := range previous {
		if next[id] == nil {
			removedRevisions[id] = m.nextAuthRevisionLocked()
			m.clearPersistenceInFlightLocked(id, 0)
			delete(m.pendingDisabledPersistence, id)
			delete(m.enablingTransitions, id)
			removedIDs = append(removedIDs, id)
			if previous[id] != nil {
				removedProviders[id] = strings.TrimSpace(previous[id].Provider)
			}
			delete(m.modelPoolOffsets, id)
			for sessionID, sessionAuths := range m.homeRuntimeAuths {
				delete(sessionAuths, id)
				if len(sessionAuths) == 0 {
					delete(m.homeRuntimeAuths, sessionID)
				}
			}
		}
	}
	m.auths = next
	cfg, _ := m.runtimeConfig.Load().(*internalconfig.Config)
	if cfg == nil {
		cfg = &internalconfig.Config{}
	}
	m.rebuildAPIKeyModelAliasLocked(cfg)
	m.mu.Unlock()
	m.syncScheduler()
	if m.scheduler != nil {
		for _, id := range removedIDs {
			m.scheduler.removeAuthAtRevision(id, removedRevisions[id])
		}
	}
	for _, auth := range loadedSnapshots {
		m.queueRefreshReschedule(auth.ID)
		if _, registered := registeredIDs[auth.ID]; registered {
			m.hook.OnAuthRegistered(ctx, auth.Clone())
		} else {
			m.hook.OnAuthUpdated(ctx, auth.Clone())
		}
	}
	for _, id := range removedIDs {
		m.queueRefreshUnschedule(id)
		m.invalidateSessionAffinity(id)
		if removalHook, okRemoval := m.hook.(AuthRemovalHook); okRemoval {
			removalHook.OnAuthRemoved(ctx, id)
		}
		if executor, okExecutor := m.Executor(removedProviders[id]); okExecutor {
			if closer, okCloser := executor.(ExecutionSessionCloser); okCloser {
				closer.CloseExecutionSession(CloseAllExecutionSessionsID)
			}
		}
	}
	for id := range persistMergedDisabledIDs {
		if errPersist := m.persistPendingDisabled(ctx, id); errPersist != nil {
			logEntryWithRequestID(ctx).WithField("auth_id", id).Warnf("failed to persist merged disabled auth: %v", errPersist)
		}
	}
	return nil
}

func (m *Manager) pickWithShardFilterAndInflight(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, tried map[string]struct{}, inflight func(string) int, preferredAuthID string) (*Auth, error) {
	ownership, spilloverEnabled := m.authOwnershipPredicate()
	pick := func(filter func(string) bool) (*Auth, error) {
		selected, errPick := m.scheduler.pickSingleWithFilterAndInflight(ctx, provider, model, opts, tried, inflight, preferredAuthID, filter)
		if errPick != nil && model != "" && shouldRetrySchedulerPick(errPick) {
			m.syncScheduler()
			selected, errPick = m.scheduler.pickSingleWithFilterAndInflight(ctx, provider, model, opts, tried, inflight, preferredAuthID, filter)
		}
		return selected, errPick
	}

	selected, errPick := pick(ownership)
	if errPick == nil && selected != nil {
		return selected, nil
	}
	if ownership != nil && spilloverEnabled {
		globalSelected, globalErr := m.scheduler.pickSingleWithInflight(ctx, provider, model, opts, tried, inflight, preferredAuthID)
		if globalErr == nil && globalSelected != nil {
			log.Warnf("cluster: spillover — local shard exhausted for provider=%s model=%s, using auth %s",
				provider, model, globalSelected.ID)
			return globalSelected, nil
		}
		return nil, globalErr
	}
	return selected, errPick
}

func (m *Manager) admitScheduledAuth(selected *Auth, provider, model string) *Auth {
	if m == nil || selected == nil || strings.TrimSpace(selected.ID) == "" {
		return nil
	}
	ownershipAllowed, releaseOwnership := m.lockAuthAdmissionOwnership(selected.ID)
	if !ownershipAllowed {
		return nil
	}
	defer releaseOwnership()

	m.mu.RLock()
	current := m.auths[selected.ID]
	if !m.authAdmissibleLocked(current, provider, model) {
		m.mu.RUnlock()
		return nil
	}
	// A successful return is the dispatch admission point. Disabling prevents
	// later admissions but intentionally does not cancel requests that already
	// crossed this point; those requests drain to completion.
	if current.indexAssigned {
		authCopy := current.Clone()
		m.mu.RUnlock()
		return authCopy
	}
	m.mu.RUnlock()

	// Index assignment mutates the manager-owned auth. Revalidate after
	// acquiring the write lock because the auth may have changed in the gap.
	m.mu.Lock()
	defer m.mu.Unlock()
	current = m.auths[selected.ID]
	if !m.authAdmissibleLocked(current, provider, model) {
		return nil
	}
	current.EnsureIndex()
	return current.Clone()
}

func (m *Manager) lockAuthAdmissionOwnership(authID string) (bool, func()) {
	m.clusterMu.RLock()
	if !m.authShardingEnabled {
		return true, m.clusterMu.RUnlock
	}
	ring := m.authRing
	if ring == nil || !ring.Ready() {
		m.clusterMu.RUnlock()
		return false, func() {}
	}
	if !m.spilloverEnabled && !ring.IsMine(authID) {
		m.clusterMu.RUnlock()
		return false, func() {}
	}
	return true, m.clusterMu.RUnlock
}

func (m *Manager) authAdmissibleLocked(current *Auth, provider, model string) bool {
	if current == nil || executorKeyFromAuth(current) != provider || current.Disabled || current.Status == StatusDisabled {
		return false
	}
	checkModel := model
	if strings.TrimSpace(model) != "" {
		checkModel = m.selectionModelForAuth(current, model)
	}
	blocked, _, _ := isAuthBlockedForModel(current, checkModel, time.Now())
	return !blocked
}

func markRejectedAuthTried(tried map[string]struct{}, selected *Auth) bool {
	if selected == nil {
		return false
	}
	authID := strings.TrimSpace(selected.ID)
	if authID == "" {
		return false
	}
	if _, exists := tried[authID]; exists {
		return false
	}
	tried[authID] = struct{}{}
	return true
}

func (m *Manager) pickNextMixedLegacyWithInflight(ctx context.Context, providers []string, model string, opts cliproxyexecutor.Options, tried map[string]struct{}, inflight func(string) int, preferredAuthID string) (*Auth, ProviderExecutor, string, error) {
	if m.HomeEnabled() {
		return m.pickNextViaHome(ctx, model, opts, tried)
	}
	if tried == nil {
		tried = make(map[string]struct{})
	}

	pinnedAuthID := pinnedAuthIDFromMetadata(opts.Metadata)
	disallowFreeAuth := disallowFreeAuthFromMetadata(opts.Metadata)

	providerSet := make(map[string]struct{}, len(providers))
	for _, provider := range providers {
		p := strings.TrimSpace(strings.ToLower(provider))
		if p == "" {
			continue
		}
		providerSet[p] = struct{}{}
	}
	if len(providerSet) == 0 {
		return nil, nil, "", &Error{Code: "provider_not_found", Message: "no provider supplied"}
	}

	modelKey := strings.TrimSpace(model)
	// Always use base model name (without thinking suffix) for auth matching.
	if modelKey != "" {
		parsed := thinking.ParseSuffix(modelKey)
		if parsed.ModelName != "" {
			modelKey = strings.TrimSpace(parsed.ModelName)
		}
	}
	registryRef := registry.GetGlobalRegistry()
	ownership, spilloverEnabled := m.authOwnershipPredicate()
	spillover := func() bool {
		if ownership == nil || !spilloverEnabled {
			return false
		}
		ownership = nil
		return true
	}
	for {
		m.mu.RLock()
		pluginScheduler := m.pluginScheduler
		candidates := make([]*Auth, 0, len(m.auths))
		for _, candidate := range m.auths {
			if candidate == nil || candidate.Disabled {
				continue
			}
			if ownership != nil && !ownership(candidate.ID) {
				continue
			}
			if pinnedAuthID != "" && candidate.ID != pinnedAuthID {
				continue
			}
			if disallowFreeAuth && isFreeCodexAuth(candidate) {
				continue
			}
			providerKey := executorKeyFromAuth(candidate)
			if providerKey == "" {
				continue
			}
			if _, ok := providerSet[providerKey]; !ok {
				continue
			}
			if _, used := tried[candidate.ID]; used {
				continue
			}
			if _, ok := m.executors[providerKey]; !ok {
				continue
			}
			if modelKey != "" && !m.authSupportsRouteModel(registryRef, candidate, model) {
				continue
			}
			candidates = append(candidates, candidate)
		}
		if len(candidates) == 0 {
			m.mu.RUnlock()
			if spillover() {
				continue
			}
			return nil, nil, "", &Error{Code: "auth_not_found", Message: "no auth available"}
		}
		candidateSnapshots := cloneAuthSlice(candidates)
		m.mu.RUnlock()

		available, errAvailable := m.availableAuthsForRouteModel(candidateSnapshots, "mixed", model, time.Now())
		if errAvailable != nil {
			if spillover() {
				continue
			}
			return nil, nil, "", errAvailable
		}
		available = cloneAuthSlice(available)

		selected, handled, errPick := m.pickViaPluginScheduler(ctx, pluginScheduler, "mixed", providers, model, opts, tried, available)
		if errPick != nil {
			return nil, nil, "", errPick
		}
		if !handled {
			selectorLease := m.acquireSelectorReadLease()
			selector := selectorLease.selector
			_, selectorAuths, errSelectorAvailable := m.availableAuthsForSelector(selector, candidateSnapshots, "mixed", model, time.Now())
			if errSelectorAvailable != nil {
				selectorLease.Release()
				if spillover() {
					continue
				}
				return nil, nil, "", errSelectorAvailable
			}
			selectorProvider := "mixed"
			if _, sessionAffinity := selector.(*SessionAffinitySelector); sessionAffinity && len(providerSet) == 1 {
				if _, xaiOnly := providerSet["xai"]; xaiOnly {
					selectorProvider = "xai"
				}
			}
			if sessionAffinity, okSession := selector.(*SessionAffinitySelector); okSession && inflight != nil && selectorProvider == "xai" {
				selected, errPick = sessionAffinity.pickForExecution(ctx, selectorProvider, model, opts, selectorAuths, inflight, preferredAuthID)
			} else {
				selectionAuths := selectorAuths
				if inflight != nil && selectorProvider == "xai" && isBuiltInSelector(selector) {
					selectionAuths = preferWebsocketAuths(ctx, selectorProvider, selectionAuths)
					if preferred := authByID(selectionAuths, preferredAuthID); preferred != nil {
						selectionAuths = []*Auth{preferred}
					} else {
						selectionAuths = leastInflightAuths(selectionAuths, inflight)
					}
				}
				selectorCtx := withWeightedSelectorStateModel(ctx, selector, model)
				selected, errPick = selector.Pick(selectorCtx, selectorProvider, selectionArgForSelector(selector, model), opts, selectionAuths)
			}
			if errPick != nil {
				if isBuiltInSelector(selector) {
					errPick = restoreModelCooldownErrorModel(errPick, model)
				}
				selectorLease.Release()
				return nil, nil, "", errPick
			}
			selectorLease.Release()
		}
		if selected == nil {
			return nil, nil, "", &Error{Code: "auth_not_found", Message: "selector returned no auth"}
		}
		providerKey := executorKeyFromAuth(selected)
		authCopy := m.admitScheduledAuth(selected, providerKey, model)
		if authCopy == nil {
			if !markRejectedAuthTried(tried, selected) {
				return nil, nil, "", &Error{Code: "auth_not_found", Message: "selector repeatedly returned an ineligible auth"}
			}
			continue
		}
		executor, okExecutor := m.Executor(providerKey)
		if !okExecutor {
			return nil, nil, "", &Error{Code: "executor_not_found", Message: "executor not registered"}
		}
		return authCopy, executor, providerKey, nil
	}
}

func (m *Manager) pickNextMixedWithInflight(ctx context.Context, providers []string, model string, opts cliproxyexecutor.Options, tried map[string]struct{}, inflight func(string) int, preferredAuthID string) (*Auth, ProviderExecutor, string, error) {
	if m.HomeEnabled() {
		return m.pickNextViaHome(ctx, model, opts, tried)
	}

	if m.hasPluginScheduler() {
		return m.pickNextMixedLegacyWithInflight(ctx, providers, model, opts, tried, inflight, preferredAuthID)
	}
	selectorLease, useFastPath := m.acquireSchedulerFastPathLease()
	if !useFastPath {
		return m.pickNextMixedLegacyWithInflight(ctx, providers, model, opts, tried, inflight, preferredAuthID)
	}

	eligibleProviders := make([]string, 0, len(providers))
	seenProviders := make(map[string]struct{}, len(providers))
	for _, provider := range providers {
		providerKey := strings.TrimSpace(strings.ToLower(provider))
		if providerKey == "" {
			continue
		}
		if _, seen := seenProviders[providerKey]; seen {
			continue
		}
		if _, okExecutor := m.Executor(providerKey); !okExecutor {
			continue
		}
		seenProviders[providerKey] = struct{}{}
		eligibleProviders = append(eligibleProviders, providerKey)
	}
	if len(eligibleProviders) == 0 {
		selectorLease.Release()
		return nil, nil, "", &Error{Code: "auth_not_found", Message: "no auth available"}
	}
	if strings.TrimSpace(model) != "" {
		providerSet := make(map[string]struct{}, len(eligibleProviders))
		for _, providerKey := range eligibleProviders {
			providerSet[providerKey] = struct{}{}
		}
		m.mu.RLock()
		for _, candidate := range m.auths {
			if candidate == nil || candidate.Disabled {
				continue
			}
			if _, ok := providerSet[executorKeyFromAuth(candidate)]; !ok {
				continue
			}
			if _, used := tried[candidate.ID]; used {
				continue
			}
			if m.routeAwareSelectionRequired(candidate, model) {
				m.mu.RUnlock()
				selectorLease.Release()
				return m.pickNextMixedLegacyWithInflight(ctx, providers, model, opts, tried, inflight, preferredAuthID)
			}
		}
		m.mu.RUnlock()
	}
	defer selectorLease.Release()

	disallowFreeAuth := disallowFreeAuthFromMetadata(opts.Metadata)
	for {
		var (
			selected    *Auth
			providerKey string
			errPick     error
		)
		if inflight != nil && len(eligibleProviders) == 1 && eligibleProviders[0] == "xai" {
			providerKey = "xai"
			selected, errPick = m.pickWithShardFilterAndInflight(ctx, providerKey, model, opts, tried, inflight, preferredAuthID)
		} else {
			selected, providerKey, errPick = m.pickMixedWithShardFilter(ctx, eligibleProviders, model, opts, tried)
		}
		if errPick != nil {
			return nil, nil, "", errPick
		}
		if selected == nil {
			return nil, nil, "", &Error{Code: "auth_not_found", Message: "selector returned no auth"}
		}
		authCopy := m.admitScheduledAuth(selected, providerKey, model)
		if authCopy == nil {
			if tried == nil {
				tried = make(map[string]struct{})
			}
			if selected.ID != "" {
				tried[selected.ID] = struct{}{}
			}
			continue
		}
		if disallowFreeAuth && isFreeCodexAuth(authCopy) {
			if tried == nil {
				tried = make(map[string]struct{})
			}
			tried[authCopy.ID] = struct{}{}
			continue
		}
		executor, okExecutor := m.Executor(providerKey)
		if !okExecutor {
			return nil, nil, "", &Error{Code: "executor_not_found", Message: "executor not registered"}
		}
		return authCopy, executor, providerKey, nil
	}
}

func (m *Manager) pickNextMixedForExecution(ctx context.Context, providers []string, model string, opts cliproxyexecutor.Options, fallbackPayload []byte, tried map[string]struct{}) (*Auth, ProviderExecutor, string, *authLease, error) {
	selection := coreusage.BeginAuthSelection(ctx)
	defer selection.Complete()

	if m.HomeEnabled() || m.hasPluginScheduler() || !singleProviderIsXAI(providers) {
		auth, executor, provider, err := m.pickNextMixed(ctx, providers, model, opts, tried)
		return auth, executor, provider, nil, err
	}
	if !m.useSchedulerFastPath() {
		selectorLease := m.acquireSelectorReadLease()
		sessionAffinity, okSessionAffinity := selectorLease.selector.(*SessionAffinitySelector)
		builtInFallback := okSessionAffinity && isBuiltInSelector(sessionAffinity.fallback)
		selectorLease.Release()
		if !builtInFallback {
			auth, executor, provider, err := m.pickNextMixed(ctx, providers, model, opts, tried)
			return auth, executor, provider, nil, err
		}
	}

	preferredAuthID := xaiContinuityPreferredAuth(ctx, model, opts, fallbackPayload)
	selected, lease, err := m.xaiInflight.acquire(func(inflight func(string) int) (executionPick, error) {
		auth, executor, provider, errPick := m.pickNextMixedWithInflight(ctx, providers, model, opts, tried, inflight, preferredAuthID)
		return executionPick{auth: auth, executor: executor, provider: provider}, errPick
	})
	return selected.auth, selected.executor, selected.provider, lease, err
}

func shouldEnableXAIResponsePhases(providers []string, opts cliproxyexecutor.Options) bool {
	return opts.SourceFormat == sdktranslator.FormatOpenAIResponse &&
		opts.Alt != "responses/compact" &&
		singleProviderIsXAI(providers)
}

func singleProviderIsXAI(providers []string) bool {
	found := false
	for _, provider := range providers {
		provider = strings.ToLower(strings.TrimSpace(provider))
		if provider == "" {
			continue
		}
		if provider != "xai" {
			return false
		}
		found = true
	}
	return found
}

func (m *Manager) markPersistenceInFlightLocked(ctx context.Context, auth *Auth) {
	if m == nil || auth == nil || strings.TrimSpace(auth.ID) == "" || auth.durableRevision == 0 || !m.shouldPersistAuthLocked(ctx, auth) {
		return
	}
	if m.persistenceInFlightRevisions == nil {
		m.persistenceInFlightRevisions = make(map[string]uint64)
	}
	if m.persistenceInFlightDone == nil {
		m.persistenceInFlightDone = make(map[string]chan struct{})
	}
	if previousDone := m.persistenceInFlightDone[auth.ID]; previousDone != nil {
		close(previousDone)
	}
	m.persistenceInFlightRevisions[auth.ID] = auth.durableRevision
	m.persistenceInFlightDone[auth.ID] = make(chan struct{})
}

func (m *Manager) clearPersistenceInFlight(authID string, durableRevision uint64) {
	if m == nil || strings.TrimSpace(authID) == "" || durableRevision == 0 {
		return
	}
	m.mu.Lock()
	m.clearPersistenceInFlightLocked(authID, durableRevision)
	m.mu.Unlock()
}

func (m *Manager) clearPersistenceInFlightLocked(authID string, durableRevision uint64) {
	if m == nil || strings.TrimSpace(authID) == "" {
		return
	}
	if durableRevision > 0 && m.persistenceInFlightRevisions[authID] != durableRevision {
		return
	}
	delete(m.persistenceInFlightRevisions, authID)
	if done := m.persistenceInFlightDone[authID]; done != nil {
		close(done)
		delete(m.persistenceInFlightDone, authID)
	}
}

func waitForPersistenceInFlight(ctx context.Context, done <-chan struct{}) error {
	if done == nil {
		return nil
	}
	if ctx == nil {
		<-done
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) authPersistLock(id string) *sync.Mutex {
	id = strings.TrimSpace(id)
	lockValue, _ := m.persistLocks.LoadOrStore(id, &sync.Mutex{})
	lock, _ := lockValue.(*sync.Mutex)
	if lock == nil {
		lock = &sync.Mutex{}
		m.persistLocks.Store(id, lock)
	}
	return lock
}

func (m *Manager) lockAuthPersistence(ids []string) func() {
	if m == nil || len(ids) == 0 {
		return func() {}
	}
	ordered := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	locks := make([]*sync.Mutex, 0, len(ordered))
	for _, id := range ordered {
		lock := m.authPersistLock(id)
		lock.Lock()
		locks = append(locks, lock)
	}
	return func() {
		for index := len(locks) - 1; index >= 0; index-- {
			locks[index].Unlock()
		}
	}
}

func (m *Manager) persistPublishedIfCurrent(ctx context.Context, auth *Auth) error {
	return m.persistPublishedWithModeIfCurrent(ctx, auth, false)
}

func (m *Manager) persistRegisteredIfCurrent(ctx context.Context, auth *Auth, registeringNew bool) error {
	return m.persistPublishedWithModeIfCurrent(ctx, auth, registeringNew)
}

func (m *Manager) persistPublishedWithModeIfCurrent(ctx context.Context, auth *Auth, allowRestore bool) error {
	if m == nil || auth == nil {
		return nil
	}
	defer m.clearPersistenceInFlight(auth.ID, auth.durableRevision)
	if !m.shouldPersistAuth(ctx, auth) {
		return nil
	}
	unlock := m.lockAuthPersistence([]string{auth.ID})
	defer unlock()

	m.mu.RLock()
	current := m.auths[auth.ID]
	isCurrent := current != nil && current.durableRevision == auth.durableRevision
	isMarked := m.persistenceInFlightRevisions[auth.ID] == auth.durableRevision
	var snapshot *Auth
	if isCurrent && isMarked {
		snapshot = current.Clone()
	}
	m.mu.RUnlock()
	if snapshot == nil {
		return nil
	}
	if allowRestore && snapshot.StoreGeneration() == 0 {
		return m.persistRestore(ctx, snapshot)
	}
	return m.persist(ctx, snapshot)
}

func (m *Manager) persistPendingDisabled(ctx context.Context, authID string) error {
	if m == nil || strings.TrimSpace(authID) == "" {
		return nil
	}
	unlock := m.lockAuthPersistence([]string{authID})
	defer unlock()

	m.mu.RLock()
	_, pending := m.pendingDisabledPersistence[authID]
	current := m.auths[authID]
	if !pending || !authIsDisabled(current) {
		m.mu.RUnlock()
		return nil
	}
	snapshot := current.Clone()
	m.mu.RUnlock()
	return m.persist(ctx, snapshot)
}

func isExplicitlyNonPersistentAuth(auth *Auth) bool {
	if auth == nil {
		return false
	}
	if IsConfigAPIKeyAuth(auth) || IsPluginVirtualAuth(auth) {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(authAttribute(auth, AttributeRuntimeOnly)), "true")
}

func (m *Manager) persistRestore(ctx context.Context, auth *Auth) error {
	if m == nil || auth == nil {
		return nil
	}
	m.mu.RLock()
	store := m.store
	m.mu.RUnlock()
	if store == nil || shouldSkipPersist(ctx) || isExplicitlyNonPersistentAuth(auth) || auth.Metadata == nil {
		return nil
	}
	if _, ok := store.(VersionedAuthStore); !ok {
		return m.persist(ctx, auth)
	}
	expectedGeneration := auth.StoreGeneration()
	if _, errPersist := PersistExplicitAuth(ctx, store, auth); errPersist != nil {
		return errPersist
	}
	m.mergeCommittedStoreGeneration(auth, expectedGeneration, auth.StoreGeneration())
	return nil
}

func (m *Manager) mergeCommittedStoreGeneration(snapshot *Auth, expectedGeneration, committedGeneration uint64) {
	if m == nil || snapshot == nil || committedGeneration < expectedGeneration {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.auths[snapshot.ID]
	if current == nil || current.StoreGeneration() >= committedGeneration {
		return
	}
	if current.StoreGeneration() == expectedGeneration || current.durableRevision == snapshot.durableRevision {
		current.SetStoreGeneration(committedGeneration)
	}
}

func authStoreRequiresAuthoritativeReload(err error) bool {
	return errors.Is(err, ErrAuthStoreConflict) || errors.Is(err, ErrAuthStoreDeleted) || errors.Is(err, ErrAuthStoreCommitUnknown)
}

func (m *Manager) failCloseAuthStoreConflicts(authIDs []string) ([]string, map[string]uint64) {
	if m == nil {
		return nil, nil
	}
	normalized := make([]string, 0, len(authIDs))
	seen := make(map[string]struct{}, len(authIDs))
	for _, authID := range authIDs {
		authID = strings.TrimSpace(authID)
		if authID == "" {
			continue
		}
		if _, exists := seen[authID]; exists {
			continue
		}
		seen[authID] = struct{}{}
		normalized = append(normalized, authID)
	}
	if len(normalized) == 0 {
		return nil, nil
	}
	baselines := make(map[string]uint64, len(normalized))
	removals := make(map[string]uint64, len(normalized))
	m.mu.Lock()
	for _, authID := range normalized {
		revision := m.nextAuthRevisionLocked()
		if current := m.auths[authID]; current != nil {
			disabled := current.Clone()
			disabled.Disabled = true
			disabled.Status = StatusDisabled
			disabled.StatusMessage = "durable auth conflict; authoritative reload required"
			disabled.revision = revision
			m.auths[authID] = disabled
			baselines[authID] = disabled.durableRevision
		}
		removals[authID] = revision
	}
	// Keep runtime publication and scheduler tombstones in the same admission
	// critical section. applyBatch holds one scheduler lock for the whole set.
	if m.scheduler != nil {
		m.scheduler.applyBatch(nil, removals)
	}
	m.mu.Unlock()
	return normalized, baselines
}

func (m *Manager) reloadAfterAuthStoreConflict(ctx context.Context, authID string, persistErr error) {
	m.reloadAfterAuthStoreConflicts(ctx, []string{authID}, persistErr)
}

func (m *Manager) reloadAfterAuthStoreConflicts(ctx context.Context, authIDs []string, persistErr error) {
	m.convergeAuthStoreConflicts(ctx, authIDs, persistErr, nil, nil)
}

func (m *Manager) convergeAuthStoreConflicts(ctx context.Context, authIDs []string, persistErr error, desired map[string]*Auth, desiredDisabled *bool) ([]*Auth, bool) {
	if m == nil || !authStoreRequiresAuthoritativeReload(persistErr) {
		return nil, false
	}
	authIDs, baselines := m.failCloseAuthStoreConflicts(authIDs)
	if len(authIDs) == 0 {
		return nil, false
	}

	reloadCtx, cancelReload := authStoreConflictReloadContext(ctx)
	defer cancelReload()

	m.mu.RLock()
	store := m.store
	persistentIDs := make([]string, 0, len(authIDs))
	nonPersistentIDs := make([]string, 0, len(authIDs))
	for _, authID := range authIDs {
		current := m.auths[authID]
		if current != nil && isExplicitlyNonPersistentAuth(current) {
			nonPersistentIDs = append(nonPersistentIDs, authID)
			continue
		}
		persistentIDs = append(persistentIDs, authID)
	}
	m.mu.RUnlock()
	if store == nil || len(persistentIDs) == 0 {
		logEntryWithRequestID(reloadCtx).WithError(persistErr).Error("auth store is unavailable for authoritative batch convergence; affected auths remain disabled")
		return nil, false
	}
	_, versionedStore := store.(VersionedAuthStore)

	var (
		published []*Auth
		removed   []removedAuthState
		finalized bool
	)
	errBatch := withAuthoritativeAuthBatch(reloadCtx, store, persistentIDs, func(states map[string]AuthAuthoritativeState) error {
		for _, authID := range persistentIDs {
			state, exists := states[authID]
			if !exists {
				return fmt.Errorf("authoritative batch omitted auth %q", authID)
			}
			if state.Exists && state.Generation == 0 && versionedStore {
				return fmt.Errorf("authoritative auth %q has no generation", authID)
			}
			if state.Auth != nil && state.Auth.StoreGeneration() != state.Generation {
				return fmt.Errorf("authoritative auth %q generation mismatch", authID)
			}
			if versionedStore && errors.Is(persistErr, ErrAuthStoreCommitUnknown) {
				candidate, okCandidate := AuthStoreCommitCandidateGeneration(persistErr, authID)
				if !okCandidate {
					return fmt.Errorf("auth %q has no commit candidate generation", authID)
				}
				if !state.Exists || state.Generation < candidate {
					return fmt.Errorf("auth %q authoritative generation %d is older than candidate %d", authID, state.Generation, candidate)
				}
			}
			if desiredDisabled != nil {
				if !*desiredDisabled && errors.Is(persistErr, ErrAuthStoreCommitUnknown) && (!state.Exists || state.Deleted || state.Auth == nil) {
					return fmt.Errorf("auth %q has no active authoritative row for enable", authID)
				}
				if state.Exists && !state.Deleted && (state.Auth == nil || authIsDisabled(state.Auth) != *desiredDisabled) {
					return fmt.Errorf("auth %q did not reach desired disabled state %t", authID, *desiredDisabled)
				}
			}
			if state.Exists && !state.Deleted && state.Auth == nil {
				return fmt.Errorf("authoritative active auth %q has no valid payload", authID)
			}
		}
		if desiredDisabled != nil {
			for _, authID := range nonPersistentIDs {
				candidate := desired[authID]
				if candidate == nil || authIsDisabled(candidate) != *desiredDisabled {
					return fmt.Errorf("non-persistent auth %q has no desired runtime candidate", authID)
				}
			}
		}

		m.mu.Lock()
		defer m.mu.Unlock()
		for _, authID := range authIDs {
			current := m.auths[authID]
			if current == nil || current.durableRevision != baselines[authID] || !authIsDisabled(current) {
				return fmt.Errorf("auth %q status intent changed during authoritative convergence", authID)
			}
		}

		published = make([]*Auth, 0, len(authIDs))
		removals := make(map[string]uint64)
		for _, authID := range authIDs {
			current := m.auths[authID]
			state, persistent := states[authID]
			if persistent && (!state.Exists || state.Deleted) {
				revision := m.nextAuthRevisionLocked()
				removals[authID] = revision
				removed = append(removed, removedAuthState{id: authID, provider: current.Provider, revision: revision})
				m.clearPersistenceInFlightLocked(authID, 0)
				delete(m.pendingDisabledPersistence, authID)
				delete(m.enablingTransitions, authID)
				delete(m.auths, authID)
				delete(m.modelPoolOffsets, authID)
				continue
			}
			var next *Auth
			if persistent {
				next = mergePersistedAuthRuntime(state.Auth, current)
			} else {
				next = desired[authID].Clone()
				next.Runtime = current.Runtime
				next.Success = current.Success
				next.Failed = current.Failed
				next.recentRequests = current.recentRequests
			}
			next.revision = m.nextAuthRevisionLocked()
			next.durableRevision = m.nextAuthDurableRevisionLocked()
			m.auths[authID] = next
			delete(m.pendingDisabledPersistence, authID)
			published = append(published, next.Clone())
		}
		if m.scheduler != nil {
			m.scheduler.applyBatch(published, removals)
		}
		finalized = true
		return nil
	})
	if !finalized {
		if errBatch != nil {
			logEntryWithRequestID(reloadCtx).WithError(errBatch).Error("authoritative auth batch convergence failed; affected auths remain disabled")
		}
		return nil, false
	}
	if errBatch != nil {
		logEntryWithRequestID(reloadCtx).WithError(errBatch).Warn("authoritative auth batch store returned an error after publication")
	}
	m.wakeDispatchAuthority()

	for _, auth := range published {
		m.queueRefreshReschedule(auth.ID)
		m.hook.OnAuthUpdated(reloadCtx, auth.Clone())
	}
	for _, item := range removed {
		m.queueRefreshUnschedule(item.id)
		m.invalidateSessionAffinity(item.id)
		if removalHook, okRemoval := m.hook.(AuthRemovalHook); okRemoval {
			removalHook.OnAuthRemoved(reloadCtx, item.id)
		}
		if executor, okExecutor := m.Executor(item.provider); okExecutor {
			if closer, okCloser := executor.(ExecutionSessionCloser); okCloser {
				closer.CloseExecutionSession(CloseAllExecutionSessionsID)
			}
		}
	}
	return published, true
}

func withAuthoritativeAuthBatch(ctx context.Context, store Store, ids []string, finalize func(map[string]AuthAuthoritativeState) error) error {
	if batchStore, ok := store.(AuthAuthoritativeBatchStore); ok && batchStore != nil {
		return batchStore.WithAuthoritativeAuthBatch(ctx, ids, finalize)
	}
	states := make(map[string]AuthAuthoritativeState, len(ids))
	if lister, ok := store.(AuthAuthoritativeListStore); ok && lister != nil {
		auths, errList := lister.ListAuthoritative(ctx)
		if errList != nil {
			return errList
		}
		byID := make(map[string]*Auth, len(auths))
		for _, auth := range auths {
			if auth != nil {
				byID[auth.ID] = auth
			}
		}
		for _, id := range ids {
			auth := byID[id]
			if auth == nil {
				states[id] = AuthAuthoritativeState{}
				continue
			}
			states[id] = AuthAuthoritativeState{Auth: auth.Clone(), Exists: true, Generation: auth.StoreGeneration()}
		}
		return finalize(states)
	}
	if byID, ok := store.(authByIDStore); ok && byID != nil {
		for _, id := range ids {
			auth, errRead := byID.GetByID(ctx, id)
			if errRead != nil {
				return errRead
			}
			if auth == nil {
				states[id] = AuthAuthoritativeState{}
				continue
			}
			states[id] = AuthAuthoritativeState{Auth: auth, Exists: true, Generation: auth.StoreGeneration()}
		}
		return finalize(states)
	}
	auths, errList := store.List(ctx)
	if errList != nil {
		return errList
	}
	byID := make(map[string]*Auth, len(auths))
	for _, auth := range auths {
		if auth != nil {
			byID[auth.ID] = auth
		}
	}
	for _, id := range ids {
		auth := byID[id]
		if auth == nil {
			states[id] = AuthAuthoritativeState{}
			continue
		}
		states[id] = AuthAuthoritativeState{Auth: auth.Clone(), Exists: true, Generation: auth.StoreGeneration()}
	}
	return finalize(states)
}

func authStoreConflictReloadContext(ctx context.Context) (context.Context, context.CancelFunc) {
	reloadBase := context.Background()
	if ctx != nil {
		reloadBase = context.WithoutCancel(ctx)
	}
	return context.WithTimeout(reloadBase, authStoreConflictReloadTimeout)
}
