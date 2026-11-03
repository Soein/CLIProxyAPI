package auth

import (
	"context"
	"testing"
)

type modifyObservationStore struct {
	save func(context.Context, *Auth) (string, error)
}

func (s *modifyObservationStore) List(context.Context) ([]*Auth, error) { return nil, nil }
func (s *modifyObservationStore) Delete(context.Context, string) error  { return nil }
func (s *modifyObservationStore) Save(ctx context.Context, auth *Auth) (string, error) {
	return s.save(ctx, auth)
}

func TestManagerModifyPreservesResultsDuringPrivateSave(t *testing.T) {
	for _, outcome := range []string{"success", "unauthorized"} {
		t.Run(outcome, func(t *testing.T) {
			ctx := context.Background()
			manager := NewManager(nil, nil, nil)
			base, errRegister := manager.Register(ctx, &Auth{ID: "credential", Provider: "claude", Status: StatusActive,
				Metadata: map[string]any{"type": "claude", "access_token": "initial", "refresh_token": "initial"}})
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			manager.SetStore(&modifyObservationStore{save: func(_ context.Context, candidate *Auth) (string, error) {
				live, _ := manager.GetByID(base.ID)
				if live.Metadata["note"] != nil {
					t.Error("candidate became visible before Save returned")
				}
				if outcome == "success" {
					manager.MarkResult(withCompletedExecutionResult(ctx, true), Result{AuthID: base.ID, Provider: base.Provider, Success: true})
				} else {
					manager.markRejectedAccessToken(base.ID, "initial", base.RegistrationEpoch)
				}
				return "", nil
			}})
			_, committed, errModify := manager.Modify(ctx, base.ID, base.RegistrationEpoch, func(candidate *Auth) error {
				candidate.Metadata["note"] = "operator"
				return nil
			})
			if errModify != nil || !committed {
				t.Fatalf("Modify() committed=%v err=%v", committed, errModify)
			}
			live, _ := manager.GetByID(base.ID)
			if live.Metadata["note"] != "operator" {
				t.Fatal("successful edit was not published")
			}
			if outcome == "success" && (!live.InferenceVerified || live.LastInferenceVerifiedAt.IsZero() || live.Success != 1) {
				t.Fatalf("inference result lost: verified=%v at=%v success=%v", live.InferenceVerified, live.LastInferenceVerifiedAt, live.Success)
			}
			if outcome == "unauthorized" && live.RejectedAccessToken != "initial" {
				t.Fatal("concurrent access-token rejection lost")
			}
		})
	}
}
