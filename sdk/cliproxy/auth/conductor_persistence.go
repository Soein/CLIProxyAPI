package auth

import (
	"context"
	"reflect"
	"strings"
	"sync"
)

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
	var once sync.Once
	return func() { once.Do(func() { m.authLoadGate.Release(1) }) }, nil
}

// lockAuthMutation acquires the reload barrier, then the credential gate, before
// m.mu. Keep both until publication completes. Load never takes credential or
// refresh locks. Store callbacks may read the manager, but must not mutate it.
func (m *Manager) lockAuthMutation(id string) func() {
	release, _ := m.lockAuthMutationContext(context.Background(), id)
	return release
}

// lockAuthMutationContext lets fallible mutations cancel either lock wait.
func (m *Manager) lockAuthMutationContext(ctx context.Context, id string) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if errAcquire := m.authLoadGate.Acquire(ctx, 1); errAcquire != nil {
		return nil, errAcquire
	}
	value, _ := m.authMutationLocks.LoadOrStore(id, make(chan struct{}, 1))
	gate := value.(chan struct{})
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
	var once sync.Once
	return func() {
		once.Do(func() {
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
