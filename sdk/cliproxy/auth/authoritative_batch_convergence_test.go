package auth

import (
	"context"
	"errors"
	"sync"
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type authoritativeConvergenceStore struct {
	mu            sync.Mutex
	rows          map[string]AuthAuthoritativeState
	blockFinalize chan struct{}
	entered       chan struct{}
	finalizeErr   error
}

func (s *authoritativeConvergenceStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*Auth, 0, len(s.rows))
	for _, state := range s.rows {
		if state.Auth != nil {
			result = append(result, state.Auth.Clone())
		}
	}
	return result, nil
}

func (*authoritativeConvergenceStore) Save(context.Context, *Auth) (string, error) {
	return "", nil
}

func (*authoritativeConvergenceStore) SaveVersioned(context.Context, *Auth, uint64) (string, uint64, error) {
	return "", 0, nil
}

func (*authoritativeConvergenceStore) Delete(context.Context, string) error { return nil }

func (s *authoritativeConvergenceStore) WithAuthoritativeAuthBatch(ctx context.Context, ids []string, finalize func(map[string]AuthAuthoritativeState) error) error {
	if s.entered != nil {
		close(s.entered)
	}
	if s.blockFinalize != nil {
		select {
		case <-s.blockFinalize:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.finalizeErr != nil {
		return s.finalizeErr
	}
	s.mu.Lock()
	snapshot := make(map[string]AuthAuthoritativeState, len(ids))
	for _, id := range ids {
		state := s.rows[id]
		if state.Auth != nil {
			state.Auth = state.Auth.Clone()
		}
		snapshot[id] = state
	}
	s.mu.Unlock()
	return finalize(snapshot)
}

func convergenceAuth(id string, disabled bool, generation uint64) *Auth {
	auth := newHAStatusAuth(id, disabled)
	auth.SetStoreGeneration(generation)
	return auth
}

func TestManagerOutcomeUnknownBatchDoesNotOpenFirstAuthBeforeSecondIsValidated(t *testing.T) {
	ids := []string{"converge-a", "converge-b"}
	store := &authoritativeConvergenceStore{
		rows: map[string]AuthAuthoritativeState{
			ids[0]: {Auth: convergenceAuth(ids[0], false, 2), Exists: true, Generation: 2},
			ids[1]: {Auth: convergenceAuth(ids[1], false, 2), Exists: true, Generation: 2},
		},
		entered:       make(chan struct{}),
		blockFinalize: make(chan struct{}),
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	for _, id := range ids {
		if _, errRegister := manager.Register(WithSkipPersist(context.Background()), convergenceAuth(id, true, 1)); errRegister != nil {
			t.Fatalf("Register(%q) error: %v", id, errRegister)
		}
	}

	done := make(chan struct{})
	go func() {
		manager.reloadAfterAuthStoreConflicts(context.Background(), ids, errors.Join(
			NewAuthStoreCommitUnknown(map[string]uint64{ids[0]: 2, ids[1]: 2}, errors.New("ack lost")),
		))
		close(done)
	}()
	<-store.entered
	for _, id := range ids {
		current, _ := manager.GetByID(id)
		if current == nil || !authIsDisabled(current) {
			t.Fatalf("auth %q while batch confirm blocked = %#v, want disabled", id, current)
		}
	}
	if picked, errPick := manager.scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil); errPick == nil || picked != nil {
		t.Fatalf("scheduler admitted %#v while batch confirm blocked", picked)
	}
	close(store.blockFinalize)
	<-done
	for _, id := range ids {
		current, _ := manager.GetByID(id)
		if current == nil || authIsDisabled(current) {
			t.Fatalf("auth %q after batch confirm = %#v, want active", id, current)
		}
	}
}

func TestManagerOutcomeUnknownBatchCandidateMissingKeepsWholeBatchDisabled(t *testing.T) {
	ids := []string{"candidate-a", "candidate-b"}
	store := &authoritativeConvergenceStore{rows: map[string]AuthAuthoritativeState{
		ids[0]: {Auth: convergenceAuth(ids[0], false, 2), Exists: true, Generation: 2},
		ids[1]: {Auth: convergenceAuth(ids[1], false, 2), Exists: true, Generation: 2},
	}}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	for _, id := range ids {
		_, _ = manager.Register(WithSkipPersist(context.Background()), convergenceAuth(id, true, 1))
	}

	manager.reloadAfterAuthStoreConflicts(context.Background(), ids,
		NewAuthStoreCommitUnknown(map[string]uint64{ids[0]: 2}, errors.New("ack lost")))
	for _, id := range ids {
		current, _ := manager.GetByID(id)
		if current == nil || !authIsDisabled(current) {
			t.Fatalf("auth %q after incomplete candidate set = %#v, want disabled", id, current)
		}
	}
}

func TestManagerOutcomeUnknownBatchRejectsTombstoneAndMissingRows(t *testing.T) {
	ids := []string{"tombstone", "missing"}
	store := &authoritativeConvergenceStore{rows: map[string]AuthAuthoritativeState{
		ids[0]: {Exists: true, Deleted: true, Generation: 2},
		ids[1]: {},
	}}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	for _, id := range ids {
		_, _ = manager.Register(WithSkipPersist(context.Background()), convergenceAuth(id, true, 1))
	}

	manager.reloadAfterAuthStoreConflicts(context.Background(), ids,
		NewAuthStoreCommitUnknown(map[string]uint64{ids[0]: 2, ids[1]: 2}, errors.New("ack lost")))
	for _, id := range ids {
		current, _ := manager.GetByID(id)
		if current == nil || !authIsDisabled(current) {
			t.Fatalf("auth %q after non-active authoritative row = %#v, want disabled", id, current)
		}
	}
}

func TestManagerOutcomeUnknownBatchConcurrentLocalDisableWinsStatusIntentCAS(t *testing.T) {
	const id = "concurrent-disable"
	store := &authoritativeConvergenceStore{
		rows: map[string]AuthAuthoritativeState{
			id: {Auth: convergenceAuth(id, false, 2), Exists: true, Generation: 2},
		},
		entered:       make(chan struct{}),
		blockFinalize: make(chan struct{}),
	}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	_, _ = manager.Register(WithSkipPersist(context.Background()), convergenceAuth(id, true, 1))
	errUnknown := NewAuthStoreCommitUnknown(map[string]uint64{id: 2}, errors.New("ack lost"))
	done := make(chan struct{})
	go func() {
		manager.reloadAfterAuthStoreConflicts(context.Background(), []string{id}, errUnknown)
		close(done)
	}()
	<-store.entered
	if _, errDisable := manager.SetDisabled(WithSkipPersist(context.Background()), []string{id}, true); errDisable != nil {
		t.Fatalf("concurrent SetDisabled(true) error: %v", errDisable)
	}
	close(store.blockFinalize)
	<-done
	current, _ := manager.GetByID(id)
	if current == nil || !authIsDisabled(current) {
		t.Fatalf("auth after concurrent disable = %#v, want disabled intent winner", current)
	}
}
