package auth

import (
	"context"
	"net/http"
	"strconv"
	"testing"
)

func TestManagerAuthSyncPreservesExecutionResultLifecycle(t *testing.T) {
	for _, operation := range []string{"Reconcile", "Load", "ReloadByID", "Update"} {
		for _, transition := range []string{"unchanged", "credential-replaced", "removed-and-recreated"} {
			for _, status := range []int{http.StatusUnauthorized, http.StatusTooManyRequests} {
				t.Run(operation+"/"+transition+"/"+strconv.Itoa(status), func(t *testing.T) {
					ctx := context.Background()
					input := &Auth{
						ID: "sync-result", Provider: "audit", Status: StatusActive,
						Metadata: map[string]any{"access_token": "original"},
					}
					store := &fakeIndexedStore{fakeStore: &fakeStore{all: map[string]*Auth{input.ID: input.Clone()}}}
					manager := NewManager(store, nil, nil)
					initial, errRegister := manager.Register(WithSkipPersist(ctx), input.Clone())
					if errRegister != nil {
						t.Fatal(errRegister)
					}
					syncAuth := func() {
						var err error
						switch operation {
						case "Reconcile":
							err = manager.Reconcile(ctx)
						case "Load":
							err = manager.Load(ctx)
						case "ReloadByID":
							err = manager.ReloadByID(ctx, input.ID)
						case "Update":
							current, _ := manager.GetByID(input.ID)
							current.Label = "metadata update"
							if transition == "credential-replaced" {
								current.Metadata["access_token"] = "replacement"
							}
							_, err = manager.Update(WithSkipPersist(ctx), current)
						}
						if err != nil {
							t.Fatal(err)
						}
					}
					if transition == "credential-replaced" {
						store.all[input.ID].Metadata["access_token"] = "replacement"
					} else if transition == "removed-and-recreated" {
						if operation == "Update" {
							manager.Remove(ctx, input.ID)
						} else {
							delete(store.all, input.ID)
							syncAuth()
						}
						if _, exists := manager.GetByID(input.ID); exists {
							t.Fatal("removed credential remained registered")
						}
						store.all[input.ID] = input.Clone()
						if operation == "Update" {
							if _, errReRegister := manager.Register(WithSkipPersist(ctx), input.Clone()); errReRegister != nil {
								t.Fatal(errReRegister)
							}
						}
					}
					syncAuth()
					beforeResult, _ := manager.GetByID(input.ID)
					manager.MarkResult(withExecutionResultAuth(ctx, initial), Result{
						AuthID: initial.ID, Provider: initial.Provider, Model: "model",
						Error: &Error{HTTPStatus: status, Message: "in-flight response"},
					})
					current, _ := manager.GetByID(input.ID)
					if transition == "unchanged" {
						if beforeResult.RegistrationEpoch != initial.RegistrationEpoch || beforeResult.CredentialVersion != initial.CredentialVersion {
							t.Errorf("unchanged credential advanced execution identity: epoch %d -> %d, version %d -> %d", initial.RegistrationEpoch, beforeResult.RegistrationEpoch, initial.CredentialVersion, beforeResult.CredentialVersion)
						}
						state := current.ModelStates["model"]
						if current.Failed != 1 || current.LastError == nil || current.LastError.HTTPStatus != status || state == nil || !state.Unavailable || state.NextRetryAfter.IsZero() {
							t.Fatal("unchanged credential sync discarded the in-flight error and cooldown")
						}
						if status == http.StatusTooManyRequests && (!current.Quota.Exceeded || !state.Quota.Exceeded) {
							t.Fatal("unchanged credential sync discarded the in-flight quota failure")
						}
					} else {
						if transition == "credential-replaced" && beforeResult.CredentialVersion <= initial.CredentialVersion {
							t.Fatal("credential replacement did not advance its version")
						}
						if transition == "removed-and-recreated" && beforeResult.RegistrationEpoch <= initial.RegistrationEpoch {
							t.Fatal("credential recreation did not advance its registration epoch")
						}
						if current.Failed != 0 || current.LastError != nil || current.Quota.Exceeded || len(current.ModelStates) != 0 {
							t.Fatal("replacement credential accepted an obsolete in-flight error")
						}
					}
				})
			}
		}
	}
}
