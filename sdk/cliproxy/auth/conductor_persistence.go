package auth

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

type executionResultSeqContextKey struct{}

// withExecutionResultSeq keeps dispatch ordering private without extending Result,
// whose exported fields support legacy unkeyed literals.
func withExecutionResultSeq(ctx context.Context, seq uint64) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, executionResultSeqContextKey{}, seq)
}

func executionResultSeqFromContext(ctx context.Context) uint64 {
	if ctx == nil {
		return 0
	}
	seq, _ := ctx.Value(executionResultSeqContextKey{}).(uint64)
	return seq
}

type executionResultAuthContextKey struct{}

type executionResultAuthSnapshot struct {
	id                string
	credentialVersion uint64
	registrationEpoch uint64
}

func withExecutionResultAuth(ctx context.Context, auth *Auth) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if auth == nil {
		return ctx
	}
	return context.WithValue(ctx, executionResultAuthContextKey{}, executionResultAuthSnapshot{
		id: auth.ID, credentialVersion: auth.CredentialVersion, registrationEpoch: auth.RegistrationEpoch,
	})
}

func executionResultAuthFromContext(ctx context.Context) (executionResultAuthSnapshot, bool) {
	if ctx == nil {
		return executionResultAuthSnapshot{}, false
	}
	snapshot, ok := ctx.Value(executionResultAuthContextKey{}).(executionResultAuthSnapshot)
	return snapshot, ok
}

func (m *Manager) mergePublishedSaveDelta(before, saved *Auth) {
	var snapshot *Auth
	m.mu.Lock()
	current := m.auths[saved.ID]
	if current != nil && current.RegistrationEpoch == before.RegistrationEpoch && current.durableRevision == before.durableRevision {
		mergeAuthSaveDelta(current, before, saved, true)
		snapshot = current.Clone()
	}
	m.mu.Unlock()
	if snapshot != nil && m.scheduler != nil {
		m.schedulerUpsert(snapshot)
	}
}

// lockAuthLifecycleContext preserves watcher reconciliation and Meta's private
// mint transaction. Watchers do not save; Meta merges live runtime results after
// saving its candidate while the reload barrier protects the complete transaction.
func (m *Manager) lockAuthLifecycleContext(ctx context.Context, auth *Auth, mode updateAuthMode) (func(), error) {
	if shouldSkipPersist(ctx) {
		return func() {}, nil
	}
	if (mode != updateModePrepare && mode != updateModeRefresh) || !strings.EqualFold(strings.TrimSpace(auth.Provider), "meta") {
		return m.lockAuthMutationContext(ctx, auth.ID)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if errAcquire := m.authLoadGate.Acquire(ctx, 1); errAcquire != nil {
		return nil, errAcquire
	}
	releaseForeground, errForeground := m.beginForegroundStoreSave(ctx)
	if errForeground != nil {
		m.authLoadGate.Release(1)
		return nil, errForeground
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			releaseForeground()
			m.authLoadGate.Release(1)
		})
	}, nil
}

// lockAuthMutation acquires the reload barrier, then the credential gate, before
// m.mu. Keep both until publication completes. Load never takes credential or
// refresh locks. Store callbacks may read the manager, but must not mutate it.
func (m *Manager) lockAuthMutation(id string) func() {
	release, _ := m.lockAuthMutationContext(context.Background(), id)
	return release
}

type metadataPersistPreemptState struct {
	mu       sync.Mutex
	waiters  int
	fgActive int
	fgIdle   chan struct{}
	cancel   context.CancelFunc
	done     chan struct{}
}

type metadataPersistContextKey struct{}

func withMetadataPersistContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, metadataPersistContextKey{}, true)
}

func isMetadataPersistContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(metadataPersistContextKey{}).(bool)
	return v
}

func (m *Manager) metadataPersistPreempt(id string) *metadataPersistPreemptState {
	val, _ := m.metadataPersistCancels.LoadOrStore(id, &metadataPersistPreemptState{})
	return val.(*metadataPersistPreemptState)
}

func (m *Manager) globalMetadataSaveState() *metadataPersistPreemptState {
	val, _ := m.metadataSaveGate.LoadOrStore("state", &metadataPersistPreemptState{})
	return val.(*metadataPersistPreemptState)
}

func (m *Manager) acquireMetadataSaveGate(ctx context.Context, cancel context.CancelFunc) (func(), bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	val, _ := m.metadataSaveGate.LoadOrStore("gate", make(chan struct{}, 1))
	gate := val.(chan struct{})
	select {
	case gate <- struct{}{}:
		if ctx.Err() != nil {
			<-gate
			return nil, false
		}
	case <-ctx.Done():
		return nil, false
	}

	gState := m.globalMetadataSaveState()
	for {
		gState.mu.Lock()
		if ctx.Err() != nil {
			gState.mu.Unlock()
			<-gate
			return nil, false
		}
		if gState.fgActive == 0 {
			done := make(chan struct{})
			gState.done = done
			gState.cancel = cancel
			gState.mu.Unlock()
			var once sync.Once
			release := func() {
				once.Do(func() {
					gState.mu.Lock()
					gState.cancel = nil
					gState.done = nil
					gState.mu.Unlock()
					close(done)
					<-gate
				})
			}
			return release, true
		}
		fgIdle := gState.fgIdle
		gState.mu.Unlock()
		select {
		case <-fgIdle:
		case <-ctx.Done():
			<-gate
			return nil, false
		}
	}
}

func (m *Manager) beginForegroundStoreSave(ctx context.Context) (func(), error) {
	if m == nil || isMetadataPersistContext(ctx) {
		return func() {}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	gState := m.globalMetadataSaveState()
	gState.mu.Lock()
	if gState.fgActive == 0 {
		gState.fgIdle = make(chan struct{})
	}
	gState.fgActive++
	done := gState.done
	cancel := gState.cancel
	gState.mu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(func() {
			gState.mu.Lock()
			gState.fgActive--
			if gState.fgActive == 0 && gState.fgIdle != nil {
				close(gState.fgIdle)
				gState.fgIdle = nil
			}
			gState.mu.Unlock()
		})
	}
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
			if errCtx := ctx.Err(); errCtx != nil {
				release()
				return nil, errCtx
			}
		case <-ctx.Done():
			release()
			return nil, ctx.Err()
		}
	} else if errCtx := ctx.Err(); errCtx != nil {
		release()
		return nil, errCtx
	}
	return release, nil
}

// lockAuthMutationContext lets fallible mutations cancel either lock wait.
func (m *Manager) lockAuthMutationContext(ctx context.Context, id string) (func(), error) {
	return m.lockAuthMutationWithPersistCancel(ctx, id, nil)
}

func (m *Manager) lockAuthMutationWithPersistCancel(ctx context.Context, id string, persistCancel context.CancelFunc) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if errAcquire := m.authLoadGate.Acquire(ctx, 1); errAcquire != nil {
		return nil, errAcquire
	}
	value, _ := m.authMutationLocks.LoadOrStore(id, make(chan struct{}, 1))
	gate := value.(chan struct{})
	state := m.metadataPersistPreempt(id)
	if persistCancel == nil {
		state.mu.Lock()
		state.waiters++
		cancel := state.cancel
		state.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		defer func() {
			state.mu.Lock()
			state.waiters--
			state.mu.Unlock()
		}()
	}
	select {
	case gate <- struct{}{}:
		// Cancellation may race with the gate becoming available.
		if errContext := ctx.Err(); errContext != nil {
			<-gate
			m.authLoadGate.Release(1)
			return nil, errContext
		}
	case <-ctx.Done():
		m.authLoadGate.Release(1)
		return nil, ctx.Err()
	}
	if persistCancel != nil {
		state.mu.Lock()
		if state.waiters > 0 {
			state.mu.Unlock()
			persistCancel()
		} else {
			state.cancel = persistCancel
			state.mu.Unlock()
		}
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			if persistCancel != nil {
				state.mu.Lock()
				state.cancel = nil
				state.mu.Unlock()
			}
			<-gate
			m.authLoadGate.Release(1)
		})
	}, nil
}

// mergeAuthSaveDelta handles all exported value fields, not a store-specific
// allowlist. Map entries are independent fields; nested values are atomic.
// Identity/version fields belong exclusively to the manager. Store-owned shared
// objects must be replaced rather than mutated in place (see Store.Save).
func mergeAuthSaveDelta(target, before, after *Auth, preserveConcurrent bool) {
	dst := reflect.ValueOf(target).Elem()
	base := reflect.ValueOf(before).Elem()
	next := reflect.ValueOf(after).Elem()
	for i := 0; i < dst.NumField(); i++ {
		field := dst.Type().Field(i)
		if field.PkgPath != "" || field.Name == "ID" || field.Name == "RegistrationEpoch" || field.Name == "CredentialVersion" || field.Name == "Generation" || field.Name == "RejectedAccessToken" {
			continue
		}
		d, b, n := dst.Field(i), base.Field(i), next.Field(i)
		if reflect.DeepEqual(b.Interface(), n.Interface()) {
			continue
		}
		if reflect.DeepEqual(d.Interface(), b.Interface()) {
			d.Set(n)
			continue
		}
		if d.Kind() == reflect.Map {
			keys := make(map[any]reflect.Value)
			for _, key := range b.MapKeys() {
				keys[key.Interface()] = key
			}
			for _, key := range n.MapKeys() {
				keys[key.Interface()] = key
			}
			for _, key := range keys {
				oldValue, newValue := b.MapIndex(key), n.MapIndex(key)
				if equalSaveValue(oldValue, newValue) || (preserveConcurrent && !equalSaveValue(d.MapIndex(key), oldValue)) {
					continue
				}
				if d.IsNil() {
					d.Set(reflect.MakeMap(d.Type()))
				}
				d.SetMapIndex(key, newValue)
			}
			continue
		}
		if !preserveConcurrent {
			d.Set(n)
		}
	}
}

func equalSaveValue(a, b reflect.Value) bool {
	if !a.IsValid() || !b.IsValid() {
		return a.IsValid() == b.IsValid()
	}
	return reflect.DeepEqual(a.Interface(), b.Interface())
}

// PersistMetadataEntry updates a single metadata entry on the runtime auth for
// the given registration epoch and persists it to the backing store without
// advancing Generation or firing credential-lifecycle hooks. It reports whether
// the metadata was accepted and whether a foreground mutation preempted it.
// Foreground mutations request cancellation of in-flight saves. Immediate
// preemption requires the store to honor cancellation; detached transactions
// may finish their commit before foreground persistence can proceed.
func (m *Manager) PersistMetadataEntry(ctx context.Context, id string, expectedEpoch uint64, key string, value any, validate func(existing *Auth) bool) (bool, bool) {
	if m == nil || id == "" || key == "" {
		return false, false
	}
	ctx = WithoutSkipPersist(ctx)
	if errContext := ctx.Err(); errContext != nil {
		return false, false
	}
	m.mu.RLock()
	existing := m.auths[id]
	if existing == nil || (expectedEpoch != 0 && existing.RegistrationEpoch != expectedEpoch) || (validate != nil && !validate(existing)) {
		m.mu.RUnlock()
		return false, false
	}
	m.mu.RUnlock()

	persistCtx, cancelPersist := context.WithCancel(withMetadataPersistContext(ctx))
	defer cancelPersist()
	var preempted atomic.Bool
	preemptFn := func() {
		preempted.Store(true)
		cancelPersist()
	}
	releaseMutation, errMutation := m.lockAuthMutationWithPersistCancel(persistCtx, id, preemptFn)
	if errMutation != nil {
		return false, preempted.Load() && ctx.Err() == nil
	}
	defer releaseMutation()

	releaseSaveGate, okGate := m.acquireMetadataSaveGate(persistCtx, preemptFn)
	if !okGate {
		return false, preempted.Load() && ctx.Err() == nil
	}
	defer releaseSaveGate()

	unlockPersistence := m.lockAuthPersistence([]string{id})
	defer func() {
		if unlockPersistence != nil {
			unlockPersistence()
		}
	}()

	m.mu.Lock()
	existing = m.auths[id]
	if existing == nil || (expectedEpoch != 0 && existing.RegistrationEpoch != expectedEpoch) || (validate != nil && !validate(existing)) {
		m.mu.Unlock()
		return false, false
	}
	if preempted.Load() {
		m.mu.Unlock()
		return false, ctx.Err() == nil
	}
	existingBeforeSave := existing.Clone()
	updated := existing.Clone()
	if updated.Metadata == nil {
		updated.Metadata = make(map[string]any)
	}
	updated.Metadata[key] = value
	m.mu.Unlock()
	errPersist := m.persistCandidate(persistCtx, updated)
	m.mu.Lock()
	if errPersist != nil {
		if !preempted.Load() {
			log.WithFields(log.Fields{"auth_id": updated.ID, "credential": updated.ID, "provider": updated.Provider}).Warnf("failed to persist metadata for auth %s (%s): %v", updated.Provider, updated.ID, errPersist)
		}
		m.mu.Unlock()
		unlockPersistence()
		unlockPersistence = nil
		releaseSaveGate()
		releaseMutation()
		m.reloadAfterAuthStoreConflict(ctx, id, errPersist)
		return false, preempted.Load() && ctx.Err() == nil
	}
	current := m.auths[id]
	if current == nil || current.RegistrationEpoch != updated.RegistrationEpoch {
		m.mu.Unlock()
		return false, false
	}
	if current.StoreGeneration() == existingBeforeSave.StoreGeneration() && updated.StoreGeneration() > current.StoreGeneration() {
		current = current.Clone()
		current.SetStoreGeneration(updated.StoreGeneration())
	}
	if validate != nil && !validate(current) {
		invalidated := current.Clone()
		if invalidated.Metadata != nil {
			delete(invalidated.Metadata, key)
		}
		m.auths[id] = invalidated
		schedulerSnapshot := invalidated.Clone()
		m.mu.Unlock()
		if m.scheduler != nil {
			m.schedulerUpsert(schedulerSnapshot)
		}
		return false, false
	}
	published := current.Clone()
	mergeAuthSaveDelta(published, existingBeforeSave, updated, true)
	m.auths[id] = published
	schedulerSnapshot := published.Clone()
	m.mu.Unlock()
	if m.scheduler != nil {
		m.schedulerUpsert(schedulerSnapshot)
	}
	return true, false
}
