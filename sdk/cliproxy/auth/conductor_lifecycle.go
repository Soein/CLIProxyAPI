package auth

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// SetRetryConfig updates retry attempts, credential retry limit and cooldown wait interval.
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
	m.mu.Lock()
	replaced = m.executors[provider]
	m.executors[provider] = executor
	m.mu.Unlock()

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

// Register inserts a new auth entry into the manager.
func (m *Manager) Register(ctx context.Context, auth *Auth) (*Auth, error) {
	if auth == nil {
		return nil, nil
	}
	if errWeight := ValidateAuthWeight(auth); errWeight != nil {
		return nil, fmt.Errorf("register auth: %w", errWeight)
	}
	if auth.ID == "" {
		auth.ID = uuid.NewString()
	}
	auth.discardStoreGenerationMetadata()
	now := time.Now()
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
	auth.revision = m.nextAuthRevisionLocked()
	auth.durableRevision = m.nextAuthDurableRevisionLocked()
	authClone := auth.Clone()
	m.auths[auth.ID] = authClone
	m.markPersistenceInFlightLocked(ctx, authClone)
	m.mu.Unlock()
	if !shouldDeferAPIKeyModelAliasRebuild(ctx) {
		m.rebuildAPIKeyModelAliasFromRuntimeConfig()
	}
	if m.scheduler != nil {
		m.schedulerUpsert(authClone)
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
		m.persistCooldownStates(ctx)
	}
	return committed, nil
}

// Update replaces an existing auth entry and notifies hooks.
func (m *Manager) Update(ctx context.Context, auth *Auth) (*Auth, error) {
	if auth == nil || auth.ID == "" {
		return nil, nil
	}
	if errWeight := ValidateAuthWeight(auth); errWeight != nil {
		return nil, fmt.Errorf("update auth: %w", errWeight)
	}
	auth.discardStoreGenerationMetadata()
	m.mu.Lock()
	existing, ok := m.auths[auth.ID]
	if !ok || existing == nil {
		m.mu.Unlock()
		return nil, nil
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
	if !existing.Disabled && existing.Status != StatusDisabled && !auth.Disabled && auth.Status != StatusDisabled {
		if len(auth.ModelStates) == 0 && len(existing.ModelStates) > 0 {
			auth.ModelStates = existing.ModelStates
		}
	}
	now := time.Now()
	cooldownStateChanged := normalizeModelStates(auth)
	if m.cooldownDisabledForAuth(auth) || auth.Disabled || auth.Status == StatusDisabled {
		cooldownStateChanged = clearCooldownStateForAuth(auth, now) || cooldownStateChanged
	}
	auth.EnsureIndex()
	auth.revision = m.nextAuthRevisionLocked()
	auth.durableRevision = m.nextAuthDurableRevisionLocked()
	authClone := auth.Clone()
	m.auths[auth.ID] = authClone
	m.markPersistenceInFlightLocked(ctx, authClone)
	m.mu.Unlock()
	if !shouldDeferAPIKeyModelAliasRebuild(ctx) {
		m.rebuildAPIKeyModelAliasFromRuntimeConfig()
	}
	if m.scheduler != nil {
		m.schedulerUpsert(authClone)
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
		m.persistCooldownStates(ctx)
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
	m.mu.Unlock()

	if !shouldDeferAPIKeyModelAliasRebuild(ctx) {
		m.rebuildAPIKeyModelAliasFromRuntimeConfig()
	}
	if m.scheduler != nil {
		m.scheduler.removeAuth(id)
	}
	m.queueRefreshUnschedule(id)
	m.invalidateSessionAffinity(id)

	if provider != "" {
		if exec, ok := m.Executor(provider); ok && exec != nil {
			if closer, okCloser := exec.(ExecutionSessionCloser); okCloser {
				closer.CloseExecutionSession(CloseAllExecutionSessionsID)
			}
		}
	}
	m.persistCooldownStates(ctx)
}

func (m *Manager) invalidateSessionAffinity(authID string) {
	if m == nil || authID == "" {
		return
	}
	if invalidator, ok := m.selector.(interface{ InvalidateAuth(string) }); ok && invalidator != nil {
		invalidator.InvalidateAuth(authID)
	}
}

func (m *Manager) Load(ctx context.Context) error {
	return m.load(ctx, false)
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
