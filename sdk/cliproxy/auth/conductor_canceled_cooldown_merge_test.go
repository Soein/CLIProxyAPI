package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type canceledCooldownLifecycleExecutor struct {
	issue6503StreamExecutor
	beforeCancel func(*Auth)
	bootstrap    bool
}

func (e *canceledCooldownLifecycleExecutor) ExecuteStream(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.beforeCancel(auth)
	e.cancel()
	if !e.bootstrap {
		return nil, e.err
	}
	chunks := make(chan cliproxyexecutor.StreamChunk, 1)
	chunks <- cliproxyexecutor.StreamChunk{Err: e.err}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

type canceledCooldownSnapshotHook struct {
	NoopHook
	snapshot executionResultAuthSnapshot
	hasAuth  bool
	ctxErr   error
	results  int
}

func (h *canceledCooldownSnapshotHook) OnResult(ctx context.Context, _ Result) {
	h.snapshot, h.hasAuth = executionResultAuthFromContext(ctx)
	h.ctxErr = ctx.Err()
	h.results++
}

func TestManagerCanceledUpstreamCooldownRejectsSupersededCredential(t *testing.T) {
	for _, transition := range []string{"credential-replaced", "same-token-reregistered"} {
		for _, bootstrap := range []bool{false, true} {
			path := "executor-error"
			if bootstrap {
				path = "bootstrap-error"
			}
			t.Run(transition+"/"+path, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				hook := &canceledCooldownSnapshotHook{}
				manager := NewManager(nil, nil, hook)
				manager.SetRetryConfig(0, 0, 0)
				input := &Auth{
					ID: "canceled-cooldown-" + uuid.NewString(), Provider: "issue6503", Status: StatusActive,
					Metadata: map[string]any{"access_token": "same-token"},
				}
				initial, errRegister := manager.Register(context.Background(), input)
				if errRegister != nil {
					t.Fatal(errRegister)
				}
				model := "canceled-cooldown-model-" + uuid.NewString()
				registry.GetGlobalRegistry().RegisterClient(initial.ID, initial.Provider, []*registry.ModelInfo{{ID: model}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(initial.ID) })
				manager.RegisterExecutor(&canceledCooldownLifecycleExecutor{
					issue6503StreamExecutor: issue6503StreamExecutor{cancel: cancel, err: issue6503QuotaError{retryAfter: 10 * time.Minute}},
					bootstrap:               bootstrap,
					beforeCancel: func(selected *Auth) {
						if selected.CredentialVersion != initial.CredentialVersion || selected.RegistrationEpoch != initial.RegistrationEpoch {
							t.Fatal("executor did not receive the initial credential identity")
						}
						if transition == "credential-replaced" {
							updated := selected.Clone()
							updated.Metadata["access_token"] = "replacement"
							if _, errUpdate := manager.Update(context.Background(), updated); errUpdate != nil {
								t.Fatal(errUpdate)
							}
						} else {
							manager.Remove(context.Background(), selected.ID)
							if _, errReRegister := manager.Register(context.Background(), selected.Clone()); errReRegister != nil {
								t.Fatal(errReRegister)
							}
						}
					},
				})

				_, errStream := manager.ExecuteStream(ctx, []string{initial.Provider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				if !errors.Is(errStream, context.Canceled) {
					t.Fatalf("ExecuteStream() error = %v, want context.Canceled", errStream)
				}
				current, ok := manager.GetByID(initial.ID)
				if !ok {
					t.Fatal("replacement credential is missing")
				}
				if transition == "credential-replaced" && current.CredentialVersion <= initial.CredentialVersion {
					t.Fatal("credential replacement did not advance its version")
				}
				if transition == "same-token-reregistered" && current.RegistrationEpoch <= initial.RegistrationEpoch {
					t.Fatal("same-token registration did not advance its epoch")
				}
				if current.Unavailable || current.Quota.Exceeded || !current.NextRetryAfter.IsZero() || current.LastError != nil || current.Failed != 0 || len(current.ModelStates) != 0 {
					t.Fatalf("replacement accepted an obsolete canceled cooldown: %+v", current)
				}
				if hook.results != 1 || hook.ctxErr != nil || !hook.hasAuth || hook.snapshot.id != initial.ID || hook.snapshot.credentialVersion != initial.CredentialVersion || hook.snapshot.registrationEpoch != initial.RegistrationEpoch {
					t.Fatalf("cooldown result lost its uncanceled execution identity: results=%d ctxErr=%v snapshot=%+v", hook.results, hook.ctxErr, hook.snapshot)
				}
			})
		}
	}
}
