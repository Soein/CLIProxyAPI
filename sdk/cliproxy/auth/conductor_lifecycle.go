package auth

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
)

// SetRetryConfig updates additional credential retry rounds, the per-round credential limit, and the cooldown wait interval.
func (m *Manager) SetRetryConfig(retry int, maxRetryInterval time.Duration, maxRetryCredentials int) {
	if m == nil {
		return
	}
	if retry < 0 {
		retry = 0
	}
	if maxRetryCredentials < 0 {
		maxRetryCredentials = 0
	}
	if maxRetryInterval < 0 {
		maxRetryInterval = 0
	}
	m.requestRetry.Store(int32(retry))
	m.maxRetryCredentials.Store(int32(maxRetryCredentials))
	m.maxRetryInterval.Store(maxRetryInterval.Nanoseconds())
}

// RegisterExecutor registers a provider executor with the manager.
func (m *Manager) RegisterExecutor(executor ProviderExecutor) {
	if executor == nil {
		return
	}
	provider := strings.TrimSpace(executor.Identifier())
	if provider == "" {
		return
	}

	var replaced ProviderExecutor
	var toReschedule []string
	m.mu.Lock()
	replaced = m.executors[provider]
	m.executors[provider] = executor
	for id, auth := range m.auths {
		if auth != nil && strings.EqualFold(executorKeyFromAuth(auth), provider) {
			toReschedule = append(toReschedule, id)
		}
	}
	m.mu.Unlock()

	for _, id := range toReschedule {
		m.queueRefreshReschedule(id)
	}

	if replaced == nil || replaced == executor {
		return
	}
	if closer, ok := replaced.(ExecutionSessionCloser); ok && closer != nil {
		closer.CloseExecutionSession(CloseAllExecutionSessionsID)
	}
}

// UnregisterExecutor removes the executor associated with the provider key.
func (m *Manager) UnregisterExecutor(provider string) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "" {
		return
	}
	m.mu.Lock()
	delete(m.executors, provider)
	m.mu.Unlock()
}

// beginAuthRegistrationLocked starts a new registration lifecycle for auth.
// The caller must hold m.mu.
func (m *Manager) beginAuthRegistrationLocked(auth, current *Auth) {
	if m == nil || auth == nil || strings.TrimSpace(auth.ID) == "" {
		return
	}
	if m.authEpochs == nil {
		m.authEpochs = make(map[string]uint64)
	}
	epoch := max(m.authEpochs[auth.ID], auth.RegistrationEpoch)
	if current != nil {
		epoch = max(epoch, current.RegistrationEpoch)
	}
	epoch++
	m.authEpochs[auth.ID] = epoch
	auth.RegistrationEpoch = epoch
	auth.Generation = 1
}

// advanceAuthGenerationLocked publishes auth as a newer mutation in the
// current registration lifecycle. The caller must hold m.mu.
func (m *Manager) advanceAuthGenerationLocked(auth, current *Auth) {
	if m == nil || auth == nil || strings.TrimSpace(auth.ID) == "" {
		return
	}
	if m.authEpochs == nil {
		m.authEpochs = make(map[string]uint64)
	}
	epoch := max(m.authEpochs[auth.ID], auth.RegistrationEpoch)
	generation := auth.Generation
	if current != nil {
		epoch = max(epoch, current.RegistrationEpoch)
		generation = max(generation, current.Generation)
	}
	if epoch == 0 {
		epoch = 1
	}
	m.authEpochs[auth.ID] = epoch
	auth.RegistrationEpoch = epoch
	auth.Generation = generation + 1
}

// advanceAuthRemovalEpochLocked records a tombstone for the lifecycle being
// removed. The caller must hold m.mu.
func (m *Manager) advanceAuthRemovalEpochLocked(id string, current *Auth) uint64 {
	if m == nil || strings.TrimSpace(id) == "" {
		return 0
	}
	if m.authEpochs == nil {
		m.authEpochs = make(map[string]uint64)
	}
	epoch := m.authEpochs[id]
	if current != nil {
		epoch = max(epoch, current.RegistrationEpoch)
	}
	epoch++
	m.authEpochs[id] = epoch
	return epoch
}

// Register inserts a new auth entry into the manager.
func (m *Manager) Register(ctx context.Context, auth *Auth) (*Auth, error) {
	if auth == nil {
		return nil, nil
	}
	NormalizeCredentialMetadata(auth.Metadata)
	if errWeight := ValidateAuthWeight(auth); errWeight != nil {
		return nil, fmt.Errorf("register auth: %w", errWeight)
	}
	if auth.ID == "" {
		auth.ID = uuid.NewString()
	}
	auth.discardStoreGenerationMetadata()
	now := time.Now()
	if auth.Generation == 0 {
		auth.Generation = 1
	}
	if auth.CreatedAt.IsZero() {
		auth.CreatedAt = now
	}
	auth.UpdatedAt = now
	cooldownStateChanged := normalizeModelStates(auth)
	if m.cooldownDisabledForAuth(auth) || auth.Disabled || auth.Status == StatusDisabled {
		cooldownStateChanged = clearCooldownStateForAuth(auth, now) || cooldownStateChanged
	}
	auth.EnsureIndex()
	m.mu.Lock()
	existing := m.auths[auth.ID]
	registeringNew := existing == nil
	if existing != nil && auth.StoreGeneration() == 0 {
		auth.SetStoreGeneration(existing.StoreGeneration())
	}
	if existing != nil && m.shouldCarryDisabledRuntimeLocked(auth.ID, existing) {
		auth = mergePersistedAuthRuntime(auth, existing)
	}
	m.carryDisabledAdmissionLocked(auth.ID, auth, existing)
	if existing == nil {
		delete(m.pendingDisabledPersistence, auth.ID)
	}
	if m.authEpochs == nil {
		m.authEpochs = make(map[string]uint64)
	}
	if existing != nil && existing.RegistrationEpoch > m.authEpochs[auth.ID] {
		m.authEpochs[auth.ID] = existing.RegistrationEpoch
	}
	if auth.RegistrationEpoch > m.authEpochs[auth.ID] {
		m.authEpochs[auth.ID] = auth.RegistrationEpoch
	}
	m.authEpochs[auth.ID]++
	auth.RegistrationEpoch = m.authEpochs[auth.ID]
	auth.Generation = 1
	auth.revision = m.nextAuthRevisionLocked()
	auth.durableRevision = m.nextAuthDurableRevisionLocked()
	authClone := auth.Clone()
	m.auths[auth.ID] = authClone
	m.markPersistenceInFlightLocked(ctx, authClone)
	var schedulerSnapshot *Auth
	if m.scheduler != nil {
		schedulerSnapshot = authClone.Clone()
	}
	m.mu.Unlock()
	if !shouldDeferAPIKeyModelAliasRebuild(ctx) {
		m.rebuildAPIKeyModelAliasFromRuntimeConfig()
	}
	if m.scheduler != nil {
		m.schedulerUpsert(schedulerSnapshot)
	}
	if registeringNew {
		m.wakeDispatchAuthority()
	}
	m.queueRefreshReschedule(auth.ID)
	errPersist := m.persistRegisteredIfCurrent(ctx, authClone, registeringNew)
	if errPersist != nil {
		m.reloadAfterAuthStoreConflict(ctx, auth.ID, errPersist)
		current, _ := m.GetByID(auth.ID)
		return current, fmt.Errorf("persist registered auth %q: %w", auth.ID, errPersist)
	}
	committed, okCommitted := m.GetByID(auth.ID)
	if !okCommitted || committed == nil {
		committed = auth.Clone()
	}
	m.hook.OnAuthRegistered(ctx, committed.Clone())
	if cooldownStateChanged {
		m.persistCooldownStates(context.Background())
	}
	return committed, nil
}

type updateAuthMode int

const (
	updateModeReplace updateAuthMode = iota
	updateModeRefresh
	updateModePrepare
)

// UpdatePreparedAuth atomically merges request preparation results into the latest runtime auth
// under the manager lock, preserving concurrent modifications without modifying refresh lifecycle fields.
func (m *Manager) UpdatePreparedAuth(ctx context.Context, base, updated *Auth) (*Auth, error) {
	return m.updateInternal(ctx, base, updated, updateModePrepare)
}

// UpdateRefreshedAuth atomically merges refresh results into the latest runtime auth
// under the manager lock, preserving concurrent modifications (proxy_url, notes, weights, etc.).
func (m *Manager) UpdateRefreshedAuth(ctx context.Context, base, updated *Auth) (*Auth, error) {
	return m.updateInternal(ctx, base, updated, updateModeRefresh)
}

// Update replaces an existing auth entry and notifies hooks.
func (m *Manager) Update(ctx context.Context, auth *Auth) (*Auth, error) {
	if auth == nil || auth.ID == "" {
		return nil, nil
	}
	auth = auth.Clone()
	return m.updateInternal(ctx, nil, auth, updateModeReplace)
}

func (m *Manager) updateInternal(ctx context.Context, base, auth *Auth, mode updateAuthMode) (*Auth, error) {
	if auth == nil || auth.ID == "" {
		return nil, nil
	}
	NormalizeCredentialMetadata(auth.Metadata)
	if errWeight := ValidateAuthWeight(auth); errWeight != nil {
		return nil, fmt.Errorf("update auth: %w", errWeight)
	}
	auth.discardStoreGenerationMetadata()

	persistMetaMint := (mode == updateModePrepare || mode == updateModeRefresh) &&
		(strings.EqualFold(strings.TrimSpace(auth.Provider), "meta") || (base != nil && strings.EqualFold(strings.TrimSpace(base.Provider), "meta")))
	var unlockPersistence func()
	defer func() {
		if unlockPersistence != nil {
			unlockPersistence()
		}
	}()
	if persistMetaMint {
		unlockPersistence = m.lockAuthPersistence([]string{auth.ID})
	}

	m.mu.Lock()
	existing, ok := m.auths[auth.ID]
	if !ok || existing == nil {
		m.mu.Unlock()
		return nil, nil
	}
	if !persistMetaMint && (mode == updateModePrepare || mode == updateModeRefresh) && strings.EqualFold(strings.TrimSpace(existing.Provider), "meta") {
		persistMetaMint = true
		m.mu.Unlock()
		unlockPersistence = m.lockAuthPersistence([]string{auth.ID})
		m.mu.Lock()
		existing, ok = m.auths[auth.ID]
		if !ok || existing == nil {
			m.mu.Unlock()
			return nil, nil
		}
	}
	if m.authEpochs == nil {
		m.authEpochs = make(map[string]uint64)
	}
	if existing.RegistrationEpoch > m.authEpochs[auth.ID] {
		m.authEpochs[auth.ID] = existing.RegistrationEpoch
	}
	if (mode == updateModeRefresh || mode == updateModePrepare) && base != nil && existing.RegistrationEpoch != base.RegistrationEpoch {
		m.mu.Unlock()
		return nil, fmt.Errorf("update auth %s: stale registration epoch %d != %d", auth.ID, base.RegistrationEpoch, existing.RegistrationEpoch)
	}
	if mode == updateModeRefresh {
		merged := MergeRefreshedAuth(base, existing, auth)
		if merged != nil {
			auth = merged
			NormalizeCredentialMetadata(auth.Metadata)
		}
	} else if mode == updateModePrepare {
		merged := MergePreparedAuth(base, existing, auth)
		if merged != nil {
			auth = merged
			NormalizeCredentialMetadata(auth.Metadata)
		}
	}
	if auth.RegistrationEpoch != 0 && auth.RegistrationEpoch < m.authEpochs[auth.ID] {
		m.mu.Unlock()
		return nil, fmt.Errorf("update auth %s: stale registration epoch %d < %d", auth.ID, auth.RegistrationEpoch, m.authEpochs[auth.ID])
	}
	if auth.RegistrationEpoch >= m.authEpochs[auth.ID] {
		m.authEpochs[auth.ID] = auth.RegistrationEpoch
	} else if auth.RegistrationEpoch == 0 {
		auth.RegistrationEpoch = m.authEpochs[auth.ID]
	}
	if auth.durableRevision != 0 && auth.durableRevision < existing.durableRevision {
		current := existing.Clone()
		m.mu.Unlock()
		return current, nil
	}
	incomingGeneration := auth.StoreGeneration()
	existingGeneration := existing.StoreGeneration()
	if incomingGeneration > 0 && existingGeneration > 0 && incomingGeneration < existingGeneration {
		current := existing.Clone()
		m.mu.Unlock()
		return current, nil
	}
	unversionedWatcherSnapshot := incomingGeneration == 0 && existingGeneration > 0
	if unversionedWatcherSnapshot {
		auth.SetStoreGeneration(existing.StoreGeneration())
	}
	if unversionedWatcherSnapshot && authIsDisabled(existing) && !authIsDisabled(auth) {
		// File watcher snapshots without the private Postgres generation cannot
		// prove they are newer than a durable disable. Preserve credential fields
		// while keeping admission closed; only SetDisabled(false) is an explicit
		// local enable operation.
		applyDisabledRuntimeState(auth, existing)
	}
	m.carryDisabledAdmissionLocked(auth.ID, auth, existing)
	if !auth.indexAssigned && auth.Index == "" {
		auth.Index = existing.Index
		auth.indexAssigned = existing.indexAssigned
	}
	auth.Success = existing.Success
	auth.Failed = existing.Failed
	auth.recentRequests = existing.recentRequests
	if auth.Generation <= existing.Generation {
		auth.Generation = existing.Generation + 1
	} else {
		auth.Generation++
	}
	cooldownStateChanged := false
	if !existing.Disabled && existing.Status != StatusDisabled && !auth.Disabled && auth.Status != StatusDisabled {
		if mode == updateModeReplace && len(auth.ModelStates) == 0 && len(existing.ModelStates) > 0 {
			auth.ModelStates = existing.ModelStates
		}
		credChanged := CredentialsChanged(existing, auth)
		if credChanged && mode != updateModeRefresh {
			if hasUnauthorizedAuthFailure(existing) || (auth.LastError != nil && isUnauthorizedError(auth.LastError)) {
				auth.Unavailable = false
				auth.LastError = nil
				auth.StatusMessage = ""
				auth.Status = StatusActive
			}
			resumed := clearUnauthorizedModelStates(auth, time.Now())
			if len(resumed) > 0 {
				cooldownStateChanged = true
			}
		}
		if existing.Quota.Exceeded && existing.Quota.Reason == "credential_quota" && existing.Quota.NextRecoverAt.After(time.Now()) {
			auth.Unavailable = existing.Unavailable
			auth.NextRetryAfter = existing.NextRetryAfter
			auth.Quota = existing.Quota
			if auth.Status == StatusActive {
				auth.Status = existing.Status
			}
		}
	}
	now := time.Now()
	auth.UpdatedAt = now
	cooldownStateChanged = normalizeModelStates(auth) || cooldownStateChanged
	if m.cooldownDisabledForAuth(auth) || auth.Disabled || auth.Status == StatusDisabled {
		cooldownStateChanged = clearCooldownStateForAuth(auth, now) || cooldownStateChanged
	}
	auth.EnsureIndex()
	auth.revision = m.nextAuthRevisionLocked()
	auth.durableRevision = m.nextAuthDurableRevisionLocked()

	if persistMetaMint {
		preSaveAuth := existing.Clone()
		preSaveDurableRevision := auth.durableRevision
		candidate := auth.Clone()
		m.markPersistenceInFlightLocked(ctx, candidate)
		// Keep existing in m.auths while persisting private candidate
		m.mu.Unlock()

		if errPersist := m.persistCandidate(ctx, candidate); errPersist != nil {
			m.clearPersistenceInFlight(candidate.ID, preSaveDurableRevision)
			if unlockPersistence != nil {
				unlockPersistence()
				unlockPersistence = nil
			}
			m.reloadAfterAuthStoreConflict(ctx, candidate.ID, errPersist)
			return nil, fmt.Errorf("persist meta auth: %w", errPersist)
		}

		m.mu.Lock()
		current := m.auths[candidate.ID]
		if current == nil {
			m.clearPersistenceInFlightLocked(candidate.ID, preSaveDurableRevision)
			m.mu.Unlock()
			return nil, fmt.Errorf("prepare meta auth: credential removed during mint")
		}
		if current.RegistrationEpoch != candidate.RegistrationEpoch {
			m.clearPersistenceInFlightLocked(candidate.ID, preSaveDurableRevision)
			m.mu.Unlock()
			return nil, fmt.Errorf("prepare meta auth: credential removed during mint")
		}
		if current.durableRevision > candidate.durableRevision {
			if CredentialsChanged(preSaveAuth, current) {
				// Operator updated credentials concurrently; candidate is obsolete.
				m.clearPersistenceInFlightLocked(candidate.ID, preSaveDurableRevision)
				m.mu.Unlock()
				if unlockPersistence != nil {
					unlockPersistence()
					unlockPersistence = nil
				}
				m.reloadAfterAuthStoreConflict(ctx, candidate.ID, ErrAuthStoreConflict)
				return nil, fmt.Errorf("prepare meta auth: concurrent credential change: %w", ErrAuthStoreConflict)
			}

			// Notes or operator metadata was updated. Preserve concurrent operator changes
			// without rolling back fresh minted credentials.
			var reconciled *Auth
			if mode == updateModeRefresh {
				reconciled = MergeRefreshedAuth(preSaveAuth, current, candidate)
			} else {
				reconciled = MergePreparedAuth(preSaveAuth, current, candidate)
			}
			if reconciled == nil {
				reconciled = current.Clone()
			}
			reconciled.durableRevision = current.durableRevision
			reconciled.SetStoreGeneration(candidate.StoreGeneration())
			candidate = reconciled
		}

		// Reconcile runtime-only result state relative to pre-save snapshot
		candidate.Success = current.Success
		candidate.Failed = current.Failed
		candidate.recentRequests = current.recentRequests

		// ModelStates: reconcile concurrent changes relative to pre-save snapshot
		if candidate.ModelStates == nil && len(current.ModelStates) > 0 {
			candidate.ModelStates = make(map[string]*ModelState, len(current.ModelStates))
		}
		for mName, curMS := range current.ModelStates {
			var preMS *ModelState
			if preSaveAuth != nil && preSaveAuth.ModelStates != nil {
				preMS = preSaveAuth.ModelStates[mName]
			}
			if !reflect.DeepEqual(curMS, preMS) {
				if curMS != nil {
					candidate.ModelStates[mName] = curMS.Clone()
				} else {
					delete(candidate.ModelStates, mName)
				}
			}
		}

		// Quota: preserve active quota if new or extended
		if current.Quota.Exceeded && current.Quota.NextRecoverAt.After(time.Now()) {
			if preSaveAuth == nil || !preSaveAuth.Quota.Exceeded || current.Quota.NextRecoverAt.After(preSaveAuth.Quota.NextRecoverAt) {
				candidate.Quota = current.Quota.Clone()
				candidate.Unavailable = current.Unavailable
				candidate.NextRetryAfter = current.NextRetryAfter
				if candidate.Status == StatusActive {
					candidate.Status = current.Status
				}
			}
		}

		// LastError and Unavailable: unchanged old 401 must stay cleared.
		// Reconcile only if current has an independently newer error or cooldown relative to preSaveAuth.
		var preErr *Error
		if preSaveAuth != nil {
			preErr = preSaveAuth.LastError
		}
		if !reflect.DeepEqual(current.LastError, preErr) && current.LastError != nil {
			candidate.Unavailable = current.Unavailable
			candidate.LastError = current.LastError
			candidate.StatusMessage = current.StatusMessage
			candidate.Status = current.Status
			candidate.NextRetryAfter = current.NextRetryAfter
		} else if preSaveAuth != nil && !preSaveAuth.Unavailable && current.Unavailable {
			candidate.Unavailable = current.Unavailable
			candidate.Status = current.Status
			candidate.StatusMessage = current.StatusMessage
			candidate.NextRetryAfter = current.NextRetryAfter
		}

		// Disabled: preserve operator disable
		if current.Disabled || current.Status == StatusDisabled {
			candidate.Disabled = current.Disabled
			candidate.Status = current.Status
			if candidate.Metadata != nil {
				candidate.Metadata["disabled"] = current.Disabled
			}
		}

		// Generation must remain monotonic
		if candidate.Generation <= current.Generation {
			candidate.Generation = current.Generation + 1
		}

		candidate.Runtime = current.Runtime
		candidate.revision = m.nextAuthRevisionLocked()

		committed := candidate.Clone()
		m.auths[candidate.ID] = committed.Clone()
		m.clearPersistenceInFlightLocked(candidate.ID, preSaveDurableRevision)
		m.mu.Unlock()

		if unlockPersistence != nil {
			unlockPersistence()
			unlockPersistence = nil
		}

		if !shouldDeferAPIKeyModelAliasRebuild(ctx) {
			m.rebuildAPIKeyModelAliasFromRuntimeConfig()
		}
		if m.scheduler != nil {
			m.schedulerUpsert(committed)
		}
		m.queueRefreshReschedule(candidate.ID)
		m.hook.OnAuthUpdated(ctx, committed.Clone())
		if cooldownStateChanged {
			m.persistCooldownStates(context.Background())
		}
		return committed, nil
	}

	authClone := auth.Clone()
	m.auths[auth.ID] = authClone
	m.markPersistenceInFlightLocked(ctx, authClone)
	var schedulerSnapshot *Auth
	if m.scheduler != nil {
		schedulerSnapshot = authClone.Clone()
	}
	m.mu.Unlock()
	if !shouldDeferAPIKeyModelAliasRebuild(ctx) {
		m.rebuildAPIKeyModelAliasFromRuntimeConfig()
	}
	if schedulerSnapshot != nil {
		m.schedulerUpsert(schedulerSnapshot)
	}
	m.queueRefreshReschedule(auth.ID)
	if errPersist := m.persistPublishedIfCurrent(ctx, authClone); errPersist != nil {
		m.reloadAfterAuthStoreConflict(ctx, auth.ID, errPersist)
		current, _ := m.GetByID(auth.ID)
		return current, fmt.Errorf("persist updated auth %q: %w", auth.ID, errPersist)
	}
	committed, okCommitted := m.GetByID(auth.ID)
	if !okCommitted || committed == nil {
		committed = auth.Clone()
	}
	m.hook.OnAuthUpdated(ctx, committed.Clone())
	if cooldownStateChanged {
		m.persistCooldownStates(context.Background())
	}
	return committed, nil
}

// Remove deletes an auth from runtime state without persisting.
// Disk and token-store deletion must be handled by the caller.
func (m *Manager) Remove(ctx context.Context, id string) {
	if m == nil {
		return
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return
	}
	_ = ctx

	m.mu.Lock()
	existing := m.auths[id]
	if existing == nil {
		m.mu.Unlock()
		return
	}
	provider := strings.TrimSpace(existing.Provider)
	delete(m.auths, id)
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
	if m.authEpochs == nil {
		m.authEpochs = make(map[string]uint64)
	}
	if existing.RegistrationEpoch > m.authEpochs[id] {
		m.authEpochs[id] = existing.RegistrationEpoch
	}
	m.authEpochs[id]++
	tombstoneEpoch := m.authEpochs[id]
	m.nextAuthRevisionLocked()
	m.mu.Unlock()

	if !shouldDeferAPIKeyModelAliasRebuild(ctx) {
		m.rebuildAPIKeyModelAliasFromRuntimeConfig()
	}
	if m.scheduler != nil {
		m.scheduler.RecordRemovalTombstone(id, tombstoneEpoch)
	}
	m.wakeDispatchAuthority()
	m.queueRefreshUnschedule(id)
	m.invalidateSessionAffinity(id)

	if provider != "" {
		if exec, ok := m.Executor(provider); ok && exec != nil {
			if closer, okCloser := exec.(ExecutionSessionCloser); okCloser {
				closer.CloseExecutionSession(CloseAllExecutionSessionsID)
			}
		}
	}
	m.persistCooldownStates(context.Background())
}

func (m *Manager) invalidateSessionAffinity(authID string) {
	if m == nil || authID == "" {
		return
	}
	lease := m.acquireSelectorReadLease()
	defer lease.Release()
	if invalidator, ok := lease.selector.(interface{ InvalidateAuth(string) }); ok && invalidator != nil {
		invalidator.InvalidateAuth(authID)
	}
}

func (m *Manager) Load(ctx context.Context) error {
	return m.load(ctx, false)
}

func (m *Manager) persistCandidate(ctx context.Context, candidate *Auth) error {
	if m == nil || candidate == nil {
		return nil
	}
	if errWeight := ValidateAuthWeight(candidate); errWeight != nil {
		return fmt.Errorf("persist auth: %w", errWeight)
	}
	m.mu.RLock()
	store := m.store
	m.mu.RUnlock()
	if store == nil || shouldSkipPersist(ctx) || isExplicitlyNonPersistentAuth(candidate) || candidate.Metadata == nil {
		return nil
	}
	if versioned, ok := store.(VersionedAuthStore); ok {
		expectedGeneration := candidate.StoreGeneration()
		_, generation, errSave := versioned.SaveVersioned(ctx, candidate, expectedGeneration)
		if errSave != nil {
			return errSave
		}
		candidate.SetStoreGeneration(generation)
		return nil
	}
	_, err := store.Save(ctx, candidate)
	return err
}

func (m *Manager) persist(ctx context.Context, auth *Auth) error {
	if m == nil || auth == nil {
		return nil
	}
	if errWeight := ValidateAuthWeight(auth); errWeight != nil {
		return fmt.Errorf("persist auth: %w", errWeight)
	}
	m.mu.RLock()
	store := m.store
	m.mu.RUnlock()
	if store == nil || shouldSkipPersist(ctx) || isExplicitlyNonPersistentAuth(auth) || auth.Metadata == nil {
		return nil
	}
	if versioned, ok := store.(VersionedAuthStore); ok {
		expectedGeneration := auth.StoreGeneration()
		_, generation, errSave := versioned.SaveVersioned(ctx, auth, expectedGeneration)
		if errSave != nil {
			return errSave
		}
		auth.SetStoreGeneration(generation)
		m.mergeCommittedStoreGeneration(auth, expectedGeneration, generation)
		return nil
	}
	_, err := store.Save(ctx, auth)
	return err
}
