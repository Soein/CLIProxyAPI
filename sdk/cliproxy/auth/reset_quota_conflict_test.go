package auth

import (
	"context"
	"errors"
	"testing"
)

type resetQuotaConflictStore struct {
	row     *Auth
	saveErr error
	getErr  error
}

func (s *resetQuotaConflictStore) List(context.Context) ([]*Auth, error) {
	if s.row == nil {
		return nil, nil
	}
	return []*Auth{s.row.Clone()}, nil
}

func (s *resetQuotaConflictStore) GetByID(_ context.Context, id string) (*Auth, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	if s.row == nil || s.row.ID != id {
		return nil, nil
	}
	return s.row.Clone(), nil
}

func (s *resetQuotaConflictStore) Save(ctx context.Context, auth *Auth) (string, error) {
	path, _, errSave := s.SaveVersioned(ctx, auth, auth.StoreGeneration())
	return path, errSave
}

func (s *resetQuotaConflictStore) SaveVersioned(_ context.Context, _ *Auth, _ uint64) (string, uint64, error) {
	return "", 0, s.saveErr
}

func (*resetQuotaConflictStore) Delete(context.Context, string) error { return nil }

func resetQuotaConflictSeed(id string) *Auth {
	auth := &Auth{
		ID:            id,
		Provider:      "xai",
		Label:         "stale local",
		Status:        StatusError,
		StatusMessage: "quota exhausted",
		Unavailable:   true,
		Quota:         QuotaState{Exceeded: true, Reason: "quota"},
		Metadata:      map[string]any{"type": "xai", "token": "stale"},
	}
	auth.SetStoreGeneration(4)
	return auth
}

func TestManagerResetQuotaConflictReloadsAuthoritativeAuth(t *testing.T) {
	seed := resetQuotaConflictSeed("reset-conflict.json")
	authoritative := seed.Clone()
	authoritative.Label = "authoritative remote"
	authoritative.Disabled = true
	authoritative.Status = StatusDisabled
	authoritative.StatusMessage = "disabled remotely"
	authoritative.Metadata["token"] = "remote"
	authoritative.SetStoreGeneration(5)

	store := &resetQuotaConflictStore{row: authoritative, saveErr: ErrAuthStoreConflict}
	manager := NewManager(store, nil, nil)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), seed); errRegister != nil {
		t.Fatalf("Register(seed) error: %v", errRegister)
	}

	if _, _, errReset := manager.ResetQuota(context.Background(), seed.ID); !errors.Is(errReset, ErrAuthStoreConflict) {
		t.Fatalf("ResetQuota() error = %v, want ErrAuthStoreConflict", errReset)
	}

	current, exists := manager.GetByID(seed.ID)
	if !exists || current == nil {
		t.Fatal("authoritative auth missing after ResetQuota conflict")
	}
	if current.Label != authoritative.Label || current.Metadata["token"] != "remote" || current.StoreGeneration() != 5 {
		t.Fatalf("auth after conflict = %#v, want authoritative row", current)
	}
	if !current.Disabled || current.Status != StatusDisabled || current.StatusMessage != "disabled remotely" {
		t.Fatalf("auth admission reopened after conflict: %#v", current)
	}
}

func TestManagerResetQuotaCommitUnknownReloadsAuthoritativeAuth(t *testing.T) {
	seed := resetQuotaConflictSeed("reset-unknown.json")
	authoritative := seed.Clone()
	authoritative.Label = "commit-winner"
	authoritative.Metadata["token"] = "committed"
	authoritative.SetStoreGeneration(5)

	store := &resetQuotaConflictStore{
		row: authoritative,
		saveErr: NewAuthStoreCommitUnknown(
			map[string]uint64{seed.ID: authoritative.StoreGeneration()},
			errors.New("commit acknowledgement lost"),
		),
	}
	manager := NewManager(store, nil, nil)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), seed); errRegister != nil {
		t.Fatalf("Register(seed) error: %v", errRegister)
	}

	if _, _, errReset := manager.ResetQuota(context.Background(), seed.ID); !errors.Is(errReset, ErrAuthStoreCommitUnknown) {
		t.Fatalf("ResetQuota() error = %v, want ErrAuthStoreCommitUnknown", errReset)
	}
	current, exists := manager.GetByID(seed.ID)
	if !exists || current == nil || current.Label != "commit-winner" || current.Metadata["token"] != "committed" || current.StoreGeneration() != 5 {
		t.Fatalf("auth after outcome unknown = %#v, want authoritative committed row", current)
	}
}

func TestManagerResetQuotaDeletedStoreRowDropsRuntimeAuth(t *testing.T) {
	seed := resetQuotaConflictSeed("reset-deleted.json")
	store := &resetQuotaConflictStore{saveErr: ErrAuthStoreDeleted}
	manager := NewManager(store, nil, nil)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), seed); errRegister != nil {
		t.Fatalf("Register(seed) error: %v", errRegister)
	}

	if _, _, errReset := manager.ResetQuota(context.Background(), seed.ID); !errors.Is(errReset, ErrAuthStoreDeleted) {
		t.Fatalf("ResetQuota() error = %v, want ErrAuthStoreDeleted", errReset)
	}
	if current, exists := manager.GetByID(seed.ID); exists || current != nil {
		t.Fatalf("deleted auth remained after ResetQuota: %#v", current)
	}
}

func TestManagerResetQuotaDeletedStoreReloadFailureFailsClosed(t *testing.T) {
	seed := resetQuotaConflictSeed("reset-reload-failure.json")
	store := &resetQuotaConflictStore{
		saveErr: ErrAuthStoreDeleted,
		getErr:  errors.New("authoritative store unavailable"),
	}
	manager := NewManager(store, nil, nil)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), seed); errRegister != nil {
		t.Fatalf("Register(seed) error: %v", errRegister)
	}

	if _, _, errReset := manager.ResetQuota(context.Background(), seed.ID); !errors.Is(errReset, ErrAuthStoreDeleted) {
		t.Fatalf("ResetQuota() error = %v, want ErrAuthStoreDeleted", errReset)
	}
	current, exists := manager.GetByID(seed.ID)
	if !exists || current == nil {
		t.Fatal("auth unexpectedly removed when authoritative reload failed")
	}
	if !current.Disabled || current.Status != StatusDisabled || current.StatusMessage != "durable auth conflict; authoritative reload required" {
		t.Fatalf("auth did not fail closed after reload failure: %#v", current)
	}
}
