package auth

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
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
	if sharer, ok := replaced.(ExecutionSessionSharer); ok && sharer != nil && sharer.SharesExecutionSessionsWith(executor) {
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
// When persistence is skipped, bound file snapshots for existing IDs reconcile
// through Update. Discarded observations retain their binding and return the
// current auth (nil if absent) without an error; FileSnapshotPersistenceDone may
// defer their next file read. Ordinary registrations may create or replace files.
func (m *Manager) Register(ctx context.Context, auth *Auth) (*Auth, error) {
	if auth == nil {
		return nil, nil
	}
	input := auth
	isFileSnapshot := shouldSkipPersist(ctx) && auth.HasFileSnapshot()
	if isFileSnapshot {
		auth = auth.Clone()
	}
	NormalizeCredentialMetadata(auth.Metadata)
	if errWeight := ValidateAuthWeight(auth); errWeight != nil {
		return nil, fmt.Errorf("register auth: %w", errWeight)
	}
	if auth.ID == "" {
		auth.ID = uuid.NewString()
	}
	auth.discardStoreGenerationMetadata()
	releaseMutation, errMutation := m.lockAuthLifecycleContext(ctx, auth, updateModeReplace)
	if errMutation != nil {
		return nil, errMutation
	}
	defer releaseMutation()
	var fileSnapshotEpoch uint64
	if isFileSnapshot {
		m.mu.RLock()
		existing := m.auths[auth.ID]
		fileSnapshotEpoch = m.authEpochs[auth.ID]
		input.fileSnapshotPersistenceDone = m.pendingFileSnapshotPersistenceLocked(auth.ID)
		if existing != nil {
			m.mu.RUnlock()
			releaseMutation()
			return m.Update(ctx, input)
		}
		m.mu.RUnlock()
		if input.fileSnapshotPersistenceDone != nil {
			return nil, nil
		}
		// Validate without m.mu or a persistence lock: Store.Save may deliver
		// watcher snapshots reentrantly. Fence the observation before publishing.
		if !auth.FileSnapshotMatchesDisk() {
			m.mu.RLock()
			current := m.auths[auth.ID].Clone()
			input.fileSnapshotPersistenceDone = m.pendingFileSnapshotPersistenceLocked(auth.ID)
			m.mu.RUnlock()
			return current, nil
		}
	}
	now := time.Now()
	if auth.Generation == 0 {
		auth.Generation = 1
	}
	if auth.CreatedAt.IsZero() {
		auth.CreatedAt = now
	}
	if auth.AdoptedAt.IsZero() {
		auth.AdoptedAt = now
	}
	if auth.AdoptedGrantHash == "" {
		auth.AdoptedGrantHash = AuthGrantFingerprint(auth)
	}
	auth.UpdatedAt = now
	cooldownStateChanged := normalizeModelStates(auth)
	if m.cooldownDisabledForAuth(auth) || auth.Disabled || auth.Status == StatusDisabled {
		cooldownStateChanged = clearCooldownStateForAuth(auth, now) || cooldownStateChanged
	}
	auth.EnsureIndex()
	m.mu.Lock()
	existing := m.auths[auth.ID]
	if isFileSnapshot {
		input.fileSnapshotPersistenceDone = m.pendingFileSnapshotPersistenceLocked(auth.ID)
		if existing != nil || m.authEpochs[auth.ID] != fileSnapshotEpoch || input.fileSnapshotPersistenceDone != nil {
			current := existing.Clone()
			m.mu.Unlock()
			return current, nil
		}
	}
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
	if existing, exists := m.auths[auth.ID]; exists && existing != nil {
		auth.CredentialVersion = max(auth.CredentialVersion, existing.CredentialVersion) + 1
	} else if auth.CredentialVersion == 0 {
		auth.CredentialVersion = 1
	}
	auth.Generation = 1
	auth.revision = m.nextAuthRevisionLocked()
	auth.durableRevision = m.nextAuthDurableRevisionLocked()
	auth.ClearFileSnapshot()
	if isFileSnapshot {
		*input = *auth
	}
	authClone := auth.Clone()
	m.auths[auth.ID] = authClone
	m.markPersistenceInFlightLocked(ctx, authClone)
	m.notifyAuthChangeLocked(auth.ID)
	// Snapshot before unlocking: MarkResult mutates the published auth in place.
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
	m.structuralEpoch.Add(1)
	m.queueRefreshReschedule(auth.ID)
	errPersist := m.persistRegisteredIfCurrent(ctx, authClone, registeringNew)
	if errPersist != nil {
		log.WithFields(log.Fields{"auth_id": auth.ID, "credential": auth.ID, "provider": auth.Provider}).Warnf("failed to persist registered auth %s (%s): %v", auth.Provider, auth.ID, errPersist)
		releaseMutation()
		m.reloadAfterAuthStoreConflict(ctx, auth.ID, errPersist)
		current, _ := m.GetByID(auth.ID)
		return current, fmt.Errorf("persist registered auth %q: %w", auth.ID, errPersist)
	}
	committed, okCommitted := m.GetByID(auth.ID)
	if !okCommitted || committed == nil {
		committed = auth.Clone()
	}
	releaseMutation()
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
	return m.updateInternal(ctx, nil, auth, updateModeReplace)
}

// Modify loads the latest auth for id while holding the per-auth mutation gate,
// applies edit to a clone of that auth, and commits the result through the
// standard persistence and publication path. Identity and ordering metadata
// remain manager-owned even when edit replaces the whole snapshot. It returns
// (nil, false, nil) if the auth is not present when the gate is acquired.
func (m *Manager) Modify(ctx context.Context, id string, expectedEpoch uint64, edit func(*Auth) error) (*Auth, bool, error) {
	id = strings.TrimSpace(id)
	if m == nil || id == "" || edit == nil {
		return nil, false, nil
	}
	releaseMutation, errMutation := m.lockAuthMutationContext(ctx, id)
	if errMutation != nil {
		return nil, false, errMutation
	}
	defer releaseMutation()

	m.mu.RLock()
	existing, ok := m.auths[id]
	var current *Auth
	var currentEpoch uint64
	if ok && existing != nil {
		current = existing.Clone()
		currentEpoch = existing.RegistrationEpoch
		if m.authEpochs != nil && m.authEpochs[id] > currentEpoch {
			currentEpoch = m.authEpochs[id]
		}
	}
	m.mu.RUnlock()
	if current == nil {
		return nil, false, nil
	}
	if expectedEpoch != 0 && expectedEpoch < currentEpoch {
		return nil, false, fmt.Errorf("update auth %s: stale registration epoch %d < %d", id, expectedEpoch, currentEpoch)
	}
	managerState := *current
	if errEdit := edit(current); errEdit != nil {
		return nil, false, errEdit
	}
	current.ID = managerState.ID
	current.RegistrationEpoch = managerState.RegistrationEpoch
	current.CredentialVersion = managerState.CredentialVersion
	current.Generation = managerState.Generation
	current.revision = managerState.revision
	current.durableRevision = managerState.durableRevision
	current.storeGeneration = managerState.storeGeneration
	current.ClearFileSnapshot()
	if expectedEpoch != 0 {
		current.RegistrationEpoch = expectedEpoch
	}
	committedAuth, didCommit, errUpdate := m.updateInternalWithMutation(ctx, nil, current, updateModeReplace, releaseMutation)
	return committedAuth, errUpdate == nil && didCommit && committedAuth != nil, errUpdate
}

func (m *Manager) updateInternal(ctx context.Context, base, auth *Auth, mode updateAuthMode) (*Auth, error) {
	if auth == nil || auth.ID == "" {
		return nil, nil
	}
	input := auth
	auth = auth.Clone()
	releaseMutation, errMutation := m.lockAuthLifecycleContext(ctx, auth, mode)
	if errMutation != nil {
		return nil, errMutation
	}
	defer releaseMutation()
	committed, didCommit, errUpdate := m.updateInternalWithMutation(ctx, base, auth, mode, releaseMutation)
	if didCommit {
		input.ClearFileSnapshot()
	} else if input.HasFileSnapshot() {
		input.fileSnapshotPersistenceDone = auth.fileSnapshotPersistenceDone
	}
	return committed, errUpdate
}

// updateInternalWithMutation commits while the caller owns the lifecycle gate.
// It releases that gate before reentrant hooks or conflict reloads. The boolean
// distinguishes a committed update from a stale snapshot returned unchanged.
func (m *Manager) updateInternalWithMutation(ctx context.Context, base, auth *Auth, mode updateAuthMode, releaseMutation func()) (*Auth, bool, error) {
	if auth == nil || auth.ID == "" {
		return nil, false, nil
	}
	NormalizeCredentialMetadata(auth.Metadata)
	if errWeight := ValidateAuthWeight(auth); errWeight != nil {
		return nil, false, fmt.Errorf("update auth: %w", errWeight)
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

	isFileSnapshot := mode == updateModeReplace && auth.HasFileSnapshot()
	var fileSnapshotEpoch, fileSnapshotRevision, fileSaveRevision uint64
	if isFileSnapshot {
		auth.fileSnapshotPersistenceDone = nil
		m.mu.RLock()
		existing, ok := m.auths[auth.ID]
		if !ok || existing == nil {
			m.mu.RUnlock()
			return nil, false, nil
		}
		auth.fileSnapshotPersistenceDone = m.pendingFileSnapshotPersistenceLocked(auth.ID)
		if auth.fileSnapshotPersistenceDone != nil {
			current := existing.Clone()
			m.mu.RUnlock()
			return current, false, nil
		}
		fileSnapshotEpoch, fileSnapshotRevision = existing.RegistrationEpoch, existing.durableRevision
		fileSaveRevision = existing.fileSaveRevision
		m.mu.RUnlock()
		// Watchers bypass the mutation gate, so fence the disk observation with
		// durable state rather than runtime-only result revisions. Never hold m.mu
		// across filesystem I/O or wait here: Store.Save may reenter this path.
		if !auth.FileSnapshotMatchesDisk() {
			m.mu.RLock()
			current := m.auths[auth.ID].Clone()
			auth.fileSnapshotPersistenceDone = m.pendingFileSnapshotPersistenceLocked(auth.ID)
			m.mu.RUnlock()
			return current, false, nil
		}
	}
	m.mu.Lock()
	existing, ok := m.auths[auth.ID]
	if !ok || existing == nil {
		m.mu.Unlock()
		return nil, false, nil
	}
	if isFileSnapshot {
		auth.fileSnapshotPersistenceDone = m.pendingFileSnapshotPersistenceLocked(auth.ID)
		if auth.fileSnapshotPersistenceDone != nil || existing.RegistrationEpoch != fileSnapshotEpoch || existing.durableRevision != fileSnapshotRevision || existing.fileSaveRevision != fileSaveRevision {
			current := existing.Clone()
			m.mu.Unlock()
			return current, false, nil
		}
	}
	// Watcher snapshots may bypass the mutation gate. Check their identity
	// changes under the same manager lock used for publication.
	if mode == updateModeReplace && base != nil && !modelRegistrationInputsMatch(existing, base.RegistrationEpoch, base.Prefix, base.Provider, base.Disabled || base.Status == StatusDisabled, base.AuthKind(), base.Attributes) {
		m.mu.Unlock()
		return nil, false, nil
	}
	if !persistMetaMint && (mode == updateModePrepare || mode == updateModeRefresh) && strings.EqualFold(strings.TrimSpace(existing.Provider), "meta") {
		persistMetaMint = true
		m.mu.Unlock()
		unlockPersistence = m.lockAuthPersistence([]string{auth.ID})
		m.mu.Lock()
		existing, ok = m.auths[auth.ID]
		if !ok || existing == nil {
			m.mu.Unlock()
			return nil, false, nil
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
		return nil, false, fmt.Errorf("update auth %s: stale registration epoch %d != %d", auth.ID, base.RegistrationEpoch, existing.RegistrationEpoch)
	}
	// Do not let an in-flight refresh overwrite credentials committed after its snapshot.
	if mode == updateModeRefresh && base != nil && (existing.CredentialVersion != base.CredentialVersion || CredentialsChanged(base, existing)) {
		if auth.CodexRefreshPatch == nil || !auth.CodexRefreshPatch.CanApplyTo(existing.Metadata) {
			current := existing.Clone()
			m.mu.Unlock()
			return current, false, nil
		}
	}
	if mode == updateModeRefresh {
		merged := MergeRefreshedAuth(base, existing, auth)
		if merged != nil {
			if merged.Disabled || merged.Status == StatusDisabled {
				merged.RefreshFailures = auth.RefreshFailures
			}
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
		return nil, false, fmt.Errorf("update auth %s: stale registration epoch %d < %d", auth.ID, auth.RegistrationEpoch, m.authEpochs[auth.ID])
	}
	if auth.RegistrationEpoch >= m.authEpochs[auth.ID] {
		m.authEpochs[auth.ID] = auth.RegistrationEpoch
	} else if auth.RegistrationEpoch == 0 {
		auth.RegistrationEpoch = m.authEpochs[auth.ID]
	}
	if auth.durableRevision != 0 && auth.durableRevision < existing.durableRevision {
		current := existing.Clone()
		m.mu.Unlock()
		return current, false, nil
	}
	incomingGeneration := auth.StoreGeneration()
	existingGeneration := existing.StoreGeneration()
	if incomingGeneration > 0 && existingGeneration > 0 && incomingGeneration < existingGeneration {
		current := existing.Clone()
		m.mu.Unlock()
		return current, false, nil
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
	if isFileSnapshot && auth.CreatedAt.IsZero() {
		auth.CreatedAt = existing.CreatedAt
	}
	auth.Success = existing.Success
	auth.Failed = existing.Failed
	auth.recentRequests = existing.recentRequests
	if auth.Generation <= existing.Generation {
		auth.Generation = existing.Generation + 1
	} else {
		auth.Generation++
	}
	existingVersion := existing.CredentialVersion
	if existingVersion == 0 {
		existingVersion = 1
	}
	credChanged := CredentialsChanged(existing, auth)
	if credChanged {
		auth.CredentialVersion = max(auth.CredentialVersion, existingVersion) + 1
	} else {
		auth.CredentialVersion = existingVersion
	}
	cooldownStateChanged := false
	now := time.Now()
	if !existing.Disabled && existing.Status != StatusDisabled && !auth.Disabled && auth.Status != StatusDisabled {
		if isFileSnapshot {
			auth.ModelStates = existing.ModelStates
			auth.LastRefreshedAt = existing.LastRefreshedAt
			if !credChanged {
				auth.NextRefreshAfter = existing.NextRefreshAfter
				auth.RefreshFailures = existing.RefreshFailures
			}
		} else if mode == updateModeReplace && len(auth.ModelStates) == 0 && len(existing.ModelStates) > 0 {
			auth.ModelStates = existing.ModelStates
		}
		if isFileSnapshot || (credChanged && mode == updateModeReplace && existing.Quota.Exceeded && existing.Quota.NextRecoverAt.After(now) && !auth.Quota.NextRecoverAt.After(now)) {
			if (auth.Status == "" || auth.Status == StatusActive) && existing.Status != "" {
				auth.Status = existing.Status
			}
			if auth.StatusMessage == "" {
				auth.StatusMessage = existing.StatusMessage
			}
			auth.Unavailable = existing.Unavailable
			auth.NextRetryAfter = existing.NextRetryAfter
			if auth.LastError == nil && existing.LastError != nil {
				auth.LastError = cloneError(existing.LastError)
			}
			observation := mergeQuotaObservation(auth.Quota, existing.Quota)
			auth.Quota = existing.Quota.Clone()
			auth.Quota.ObservedAt = observation.ObservedAt
			auth.Quota.Signals = observation.Signals
			auth.cooldownRecordedSeq = existing.cooldownRecordedSeq
		}
		if credChanged || mode == updateModeRefresh {
			applyPreservedRejectedTokens(existing, auth, mode == updateModeRefresh)
			applyPreservedRenewalState(existing, auth, mode, now)
			if auth.RejectedAccessToken == "" || authAccessToken(auth) != auth.RejectedAccessToken {
				auth.RefreshFailures = 0
				if credChanged && mode != updateModeRefresh && auth.NextRefreshAfter.Equal(existing.NextRefreshAfter) {
					if authHasUnchangedRejectedRefreshToken(auth) {
						if exp, ok := auth.AccessTokenExpirationTime(); ok && !exp.IsZero() && exp.After(now) {
							auth.NextRefreshAfter = exp
						} else {
							auth.NextRefreshAfter = time.Time{}
						}
					} else {
						auth.NextRefreshAfter = time.Time{}
					}
				}
				canServeWithAccessToken := !auth.RequiresSignIn || auth.HasValidAccessToken(now)
				isAuthOrGrantFailure := hasUnauthorizedAuthFailure(existing) ||
					existing.RequiresSignIn ||
					(existing.RefreshError != nil && isTerminalRefreshError(existing, existing.RefreshError)) ||
					(auth.LastError != nil && (isUnauthorizedError(auth.LastError) || isInvalidGrantResultError(auth.LastError) || isTerminalRefreshError(auth, auth.LastError)))
				if canServeWithAccessToken && mode != updateModeRefresh && !auth.Quota.NextRecoverAt.After(now) &&
					(isAuthOrGrantFailure || (auth.Unavailable && !auth.NextRetryAfter.After(now))) {
					auth.Unavailable = false
					auth.NextRetryAfter = time.Time{}
					auth.cooldownRecordedSeq = 0
					auth.LastError = nil
					auth.StatusMessage = ""
					auth.Status = StatusActive
					cooldownStateChanged = true
				} else if canServeWithAccessToken && mode != updateModeRefresh && isAuthOrGrantFailure && auth.Quota.Exceeded && auth.Quota.NextRecoverAt.After(now) {
					auth.Unavailable = true
					if !auth.NextRetryAfter.After(now) {
						auth.NextRetryAfter = auth.Quota.NextRecoverAt
					}
					auth.LastError = nil
					auth.StatusMessage = auth.Quota.Reason
					cooldownStateChanged = true
				}
				if canServeWithAccessToken && mode != updateModeRefresh {
					resumed := clearUnauthorizedModelStates(auth, now)
					if len(resumed) > 0 {
						cooldownStateChanged = true
					}
				} else if auth.RequiresSignIn && !auth.HasValidAccessToken(now) {
					auth.Unavailable = true
					auth.Status = StatusError
					if auth.StatusMessage == "" {
						if existing.StatusMessage != "" {
							auth.StatusMessage = existing.StatusMessage
						} else {
							auth.StatusMessage = "unauthorized (refresh token invalid)"
						}
					}
					if auth.LastError == nil {
						if existing.LastError != nil {
							auth.LastError = cloneError(existing.LastError)
						} else if auth.RefreshError != nil {
							auth.LastError = &Error{Code: "unauthorized", Message: auth.RefreshError.Message, HTTPStatus: http.StatusUnauthorized}
						}
					}
				}
			}
		} else {
			applyPreservedRejectedTokens(existing, auth, false)
			applyPreservedRenewalState(existing, auth, mode, now)
		}
		// A refresh that confirms quota restoration (issue #6690) releases the
		// credential-wide cooldown early; otherwise the active credential
		// quota below is preserved exactly as before.
		devinQuotaRestored := mode == updateModeRefresh && applyDevinQuotaRestoration(auth, existing, now)
		if devinQuotaRestored {
			cooldownStateChanged = true
		} else if existing.Quota.Exceeded && existing.Quota.Reason == "credential_quota" && existing.Quota.NextRecoverAt.After(now) {
			auth.Unavailable = existing.Unavailable
			auth.NextRetryAfter = existing.NextRetryAfter
			// Preserve cooldown fields; keep the newest observation snapshot.
			observation := mergeQuotaObservation(auth.Quota, existing.Quota)
			auth.Quota = existing.Quota
			auth.Quota.ObservedAt = observation.ObservedAt
			auth.Quota.Signals = observation.Signals
			auth.cooldownRecordedSeq = existing.cooldownRecordedSeq
			if auth.Status == StatusActive {
				auth.Status = existing.Status
			}
		}
	} else {
		applyPreservedRejectedTokens(existing, auth, mode == updateModeRefresh)
		applyPreservedRenewalState(existing, auth, mode, now)
		// Refresh updates must retain their outcome rather than the pending job marker.
		if mode != updateModeRefresh {
			if !credChanged {
				auth.LastRefreshedAt = existing.LastRefreshedAt
				auth.NextRefreshAfter = existing.NextRefreshAfter
				auth.RefreshFailures = existing.RefreshFailures
			} else if authHasUnchangedRejectedRefreshToken(auth) {
				if exp, ok := auth.AccessTokenExpirationTime(); ok && !exp.IsZero() && exp.After(now) {
					auth.NextRefreshAfter = exp
				} else {
					auth.NextRefreshAfter = time.Time{}
				}
			}
		}
	}
	auth.UpdatedAt = now
	cooldownStateChanged = normalizeModelStates(auth) || cooldownStateChanged
	if m.cooldownDisabledForAuth(auth) || auth.Disabled || auth.Status == StatusDisabled {
		cooldownStateChanged = clearCooldownStateForAuth(auth, now) || cooldownStateChanged
	}
	if existing.Metadata != nil {
		if persistedHints, hasHints := existing.Metadata["antigravity_capability_hints"]; hasHints {
			if auth.Metadata == nil {
				auth.Metadata = make(map[string]any)
			}
			auth.Metadata["antigravity_capability_hints"] = persistedHints
		}
	}
	auth.EnsureIndex()
	auth.revision = m.nextAuthRevisionLocked()
	auth.durableRevision = m.nextAuthDurableRevisionLocked()
	auth.ClearFileSnapshot()

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
			releaseMutation()
			m.reloadAfterAuthStoreConflict(ctx, candidate.ID, errPersist)
			return nil, false, fmt.Errorf("persist meta auth: %w", errPersist)
		}

		m.mu.Lock()
		current := m.auths[candidate.ID]
		if current == nil {
			m.clearPersistenceInFlightLocked(candidate.ID, preSaveDurableRevision)
			m.mu.Unlock()
			return nil, false, fmt.Errorf("prepare meta auth: credential removed during mint")
		}
		if current.RegistrationEpoch != candidate.RegistrationEpoch {
			m.clearPersistenceInFlightLocked(candidate.ID, preSaveDurableRevision)
			m.mu.Unlock()
			return nil, false, fmt.Errorf("prepare meta auth: credential removed during mint")
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
				releaseMutation()
				m.reloadAfterAuthStoreConflict(ctx, candidate.ID, ErrAuthStoreConflict)
				return nil, false, fmt.Errorf("prepare meta auth: concurrent credential change: %w", ErrAuthStoreConflict)
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
		candidate.cooldownRecordedSeq = current.cooldownRecordedSeq

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
		if preSaveAuth != nil && preSaveAuth.ModelStates != nil {
			for mName := range preSaveAuth.ModelStates {
				if _, ok := current.ModelStates[mName]; !ok {
					delete(candidate.ModelStates, mName)
				}
			}
		}

		// Quota: preserve active quota if new or extended, or clear if cleared concurrently
		if current.Quota.Exceeded && current.Quota.NextRecoverAt.After(time.Now()) {
			if preSaveAuth == nil || !preSaveAuth.Quota.Exceeded || current.Quota.NextRecoverAt.After(preSaveAuth.Quota.NextRecoverAt) {
				candidate.Quota = current.Quota.Clone()
				candidate.Unavailable = current.Unavailable
				candidate.NextRetryAfter = current.NextRetryAfter
				if candidate.Status == StatusActive {
					candidate.Status = current.Status
				}
			}
		} else if preSaveAuth != nil && preSaveAuth.Quota.Exceeded && !current.Quota.Exceeded {
			candidate.Quota = current.Quota.Clone()
			candidate.Unavailable = current.Unavailable
			candidate.NextRetryAfter = current.NextRetryAfter
			if candidate.Status != StatusDisabled && !candidate.Disabled {
				candidate.Status = current.Status
			}
		}

		// LastError and Unavailable: unchanged old 401 must stay cleared.
		// Reconcile runtime field deltas relative to preSaveAuth in both directions:
		// newer concurrent errors/cooldowns are preserved, and concurrent success
		// clears are reconciled without resurrecting unchanged pre-save errors.
		var preErr *Error
		if preSaveAuth != nil {
			preErr = preSaveAuth.LastError
		}
		if !reflect.DeepEqual(current.LastError, preErr) {
			if current.LastError != nil {
				// Concurrent newer error occurred during blocked save.
				candidate.Unavailable = current.Unavailable
				candidate.LastError = cloneError(current.LastError)
				candidate.StatusMessage = current.StatusMessage
				candidate.Status = current.Status
				candidate.NextRetryAfter = current.NextRetryAfter
			} else {
				// Concurrent success cleared LastError during blocked save.
				candidate.LastError = nil
				candidate.StatusMessage = current.StatusMessage
				candidate.Unavailable = current.Unavailable
				candidate.NextRetryAfter = current.NextRetryAfter
				if candidate.Status != StatusDisabled && !candidate.Disabled {
					candidate.Status = current.Status
				}
			}
		} else if preSaveAuth != nil && !preSaveAuth.Unavailable && current.Unavailable {
			// Concurrent cooldown or quota marked auth unavailable without changing LastError.
			candidate.Unavailable = current.Unavailable
			candidate.Status = current.Status
			candidate.StatusMessage = current.StatusMessage
			candidate.NextRetryAfter = current.NextRetryAfter
		} else if preSaveAuth != nil && preSaveAuth.Unavailable && !current.Unavailable {
			// Concurrent recovery cleared unavailable state without changing LastError.
			candidate.Unavailable = false
			candidate.NextRetryAfter = current.NextRetryAfter
			if candidate.Status != StatusDisabled && !candidate.Disabled {
				candidate.Status = current.Status
				candidate.StatusMessage = current.StatusMessage
			}
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
		m.notifyAuthChangeLocked(candidate.ID)
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
		releaseMutation()
		m.hook.OnAuthUpdated(ctx, committed.Clone())
		if cooldownStateChanged {
			m.persistCooldownStates(context.Background())
		}
		return committed, true, nil
	}

	authClone := auth.Clone()
	m.auths[auth.ID] = authClone
	m.markPersistenceInFlightLocked(ctx, authClone)
	m.notifyAuthChangeLocked(auth.ID)
	// Snapshot before unlocking: MarkResult mutates the published auth in place.
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
	m.structuralEpoch.Add(1)
	m.queueRefreshReschedule(auth.ID)
	if errPersist := m.persistPublishedIfCurrent(ctx, authClone); errPersist != nil {
		log.WithFields(log.Fields{"auth_id": auth.ID, "credential": auth.ID, "provider": auth.Provider}).Warnf("failed to persist updated auth %s (%s): %v", auth.Provider, auth.ID, errPersist)
		releaseMutation()
		m.reloadAfterAuthStoreConflict(ctx, auth.ID, errPersist)
		current, _ := m.GetByID(auth.ID)
		return current, false, fmt.Errorf("persist updated auth %q: %w", auth.ID, errPersist)
	}
	committed, okCommitted := m.GetByID(auth.ID)
	if !okCommitted || committed == nil {
		committed = auth.Clone()
	}
	releaseMutation()
	m.hook.OnAuthUpdated(ctx, committed.Clone())
	if cooldownStateChanged {
		m.persistCooldownStates(context.Background())
	}
	return committed, true, nil
}

func equalErrorPtr(a, b *Error) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Code == b.Code && a.Message == b.Message && a.Retryable == b.Retryable && a.HTTPStatus == b.HTTPStatus
}

// Remove deletes an auth from runtime state without persisting.
// Disk and token-store deletion must be handled by the caller.
func (m *Manager) Remove(ctx context.Context, id string) {
	m.RemoveIf(ctx, id, nil)
}

// RemoveIf deletes an auth from runtime state while holding the per-auth
// mutation gate when predicate is nil or returns true for the current auth.
func (m *Manager) RemoveIf(ctx context.Context, id string, predicate func(*Auth) bool) bool {
	if m == nil {
		return false
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return false
	}
	releaseMutation := m.lockAuthMutation(id)
	defer releaseMutation()

	if predicate != nil {
		m.mu.RLock()
		snapshot := m.auths[id]
		if snapshot != nil {
			snapshot = snapshot.Clone()
		}
		m.mu.RUnlock()
		if !predicate(snapshot) {
			return false
		}
	}

	m.mu.Lock()
	existing := m.auths[id]
	if existing == nil {
		m.mu.Unlock()
		return false
	}
	provider := strings.TrimSpace(existing.Provider)
	delete(m.auths, id)
	m.notifyAuthChangeLocked(id)
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
	releaseMutation()

	if !shouldDeferAPIKeyModelAliasRebuild(ctx) {
		m.rebuildAPIKeyModelAliasFromRuntimeConfig()
	}
	if m.scheduler != nil {
		m.scheduler.RecordRemovalTombstone(id, tombstoneEpoch)
	}
	m.wakeDispatchAuthority()
	m.structuralEpoch.Add(1)
	m.queueRefreshUnschedule(id)
	m.invalidateSessionAffinity(id)

	m.closeAuthExecutionSessions(provider, id)
	m.persistCooldownStates(context.Background())
	return true
}

// closeAuthExecutionSessions limits removal cleanup to the affected credential
// when the executor supports it, preserving other credentials' live sessions.
func (m *Manager) closeAuthExecutionSessions(provider, authID string) {
	if provider == "" {
		return
	}
	exec, ok := m.Executor(provider)
	if !ok || exec == nil {
		return
	}
	if closer, ok := exec.(AuthExecutionSessionCloser); ok && closer != nil {
		closer.CloseAuthExecutionSessions(authID)
	} else if closer, ok := exec.(ExecutionSessionCloser); ok && closer != nil {
		closer.CloseExecutionSession(CloseAllExecutionSessionsID)
	}
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
	if m == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if errAcquire := m.authLoadGate.Acquire(ctx, math.MaxInt64); errAcquire != nil {
		return errAcquire
	}
	var once sync.Once
	release := func() { once.Do(func() { m.authLoadGate.Release(math.MaxInt64) }) }
	defer release()
	return m.load(ctx, false, release)
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
	releaseFg, errWait := m.beginForegroundStoreSave(ctx)
	if errWait != nil {
		return errWait
	}
	defer releaseFg()
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
	releaseFg, errWait := m.beginForegroundStoreSave(ctx)
	if errWait != nil {
		return errWait
	}
	defer releaseFg()
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
