package auth

import (
	"bytes"
	"context"
	"errors"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	"net/http"
	"os"
	"reflect"
	"testing"
)

func TestBindCodexSIWCRejectsVersionedStoreWithoutMutation(t *testing.T) {
	manager, base, path, hook := setupSIWCBinding(t, "auth.json")
	before, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	manager.store = &lifecycleVersionedStore{row: base.Clone()}
	if _, errBind := manager.BindCodexSIWC(context.Background(), base.ID, base.RegistrationEpoch, "account", bindingTokens("new-siwc")); errBind == nil {
		t.Fatal("versioned auth store must not be bypassed by file binding")
	}
	after, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	current, _ := manager.GetByID(base.ID)
	if !bytes.Equal(before, after) || !reflect.DeepEqual(base, current) || hook.updates != 0 {
		t.Fatal("rejected file binding changed durable or runtime auth")
	}
}

func TestVerifyInferenceHonorsDispatchAdmission(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(map[bool]string{false: "stream", true: "fallback"}[fallback], func(t *testing.T) {
			executor := &dispatchExecutor{}
			manager, auth, hook, model := newDispatchFixture(t, executor)
			authority := &dispatchAuthorityStub{}
			if fallback {
				executor.streamStarts = append(executor.streamStarts, func() (*cliproxyexecutor.StreamResult, error) {
					return nil, &Error{HTTPStatus: http.StatusNotImplemented}
				})
				authority.decisions = []bool{true, false}
			}
			manager.SetDispatchAuthority(authority)
			if _, errVerify := manager.VerifyInference(context.Background(), auth.ID, model); !errors.Is(errVerify, ErrDispatchAdmissionRejected) {
				t.Fatalf("VerifyInference() error = %v, want admission rejection", errVerify)
			}
			executeCalls, _, streamCalls, _, _ := executor.calls()
			expectedStreams := 0
			if fallback {
				expectedStreams = 1
			}
			if executeCalls != 0 || streamCalls != expectedStreams || hook.results.Load() != 0 {
				t.Fatalf("rejected verification had side effects: execute=%d stream=%d results=%d", executeCalls, streamCalls, hook.results.Load())
			}
			current, _ := manager.GetByID(auth.ID)
			if current.InferenceVerified || current.Success != 0 || current.Failed != 0 {
				t.Fatal("rejected verification changed auth results")
			}
		})
	}
}

func TestManagerModifyReplacesBusinessSnapshotWithoutRollingBackVersions(t *testing.T) {
	for _, source := range []string{"runtime_clone", "file_snapshot"} {
		t.Run(source, func(t *testing.T) {
			ctx := context.Background()
			store := &lifecycleVersionedStore{}
			manager := NewManager(store, nil, nil)
			base, errRegister := manager.Register(ctx, &Auth{
				ID: "modify-snapshot", Provider: "codex", Status: StatusActive,
				Metadata: map[string]any{"access_token": "original-token"},
			})
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			replacement := base.Clone()
			replacement.Metadata["access_token"] = "replacement-token"
			latest, errUpdate := manager.Update(ctx, replacement)
			if errUpdate != nil {
				t.Fatal(errUpdate)
			}
			restore := base.Clone()
			expectedEpoch := latest.RegistrationEpoch
			if source == "file_snapshot" {
				restore.RegistrationEpoch = 0
				restore.Generation = 0
				restore.BindFileSnapshot("missing-file.json", []byte(`{"access_token":"original-token"}`))
				expectedEpoch = 0
			}
			modified, committed, errModify := manager.Modify(ctx, latest.ID, expectedEpoch, func(current *Auth) error {
				*current = *restore.Clone()
				current.Metadata["note"] = "restored by callback"
				return nil
			})
			if errModify != nil || !committed || modified == nil {
				t.Fatalf("Modify() committed = %v, auth = %v, error = %v", committed, modified != nil, errModify)
			}
			if modified.Metadata["access_token"] != "original-token" || modified.Metadata["note"] != "restored by callback" {
				t.Fatalf("callback replacement was not published: %+v", modified.Metadata)
			}
			if modified.RegistrationEpoch != latest.RegistrationEpoch || modified.CredentialVersion <= latest.CredentialVersion || modified.Generation <= latest.Generation || modified.StoreGeneration() != latest.StoreGeneration()+1 || modified.HasFileSnapshot() {
				t.Fatalf("Modify() rolled back runtime or store versions: before epoch/version/generation/store = %d/%d/%d/%d, after = %d/%d/%d/%d", latest.RegistrationEpoch, latest.CredentialVersion, latest.Generation, latest.StoreGeneration(), modified.RegistrationEpoch, modified.CredentialVersion, modified.Generation, modified.StoreGeneration())
			}
			persisted, errRead := store.GetByID(ctx, latest.ID)
			if errRead != nil || persisted == nil || persisted.Metadata["access_token"] != "original-token" || persisted.Metadata["note"] != "restored by callback" {
				t.Fatalf("callback replacement was not persisted: auth = %v, error = %v", persisted, errRead)
			}

			stale := base.Clone()
			stale.Metadata["access_token"] = "stale-external-token"
			if _, errStale := manager.Update(ctx, stale); errStale != nil {
				t.Fatal(errStale)
			}
			current, _ := manager.GetByID(latest.ID)
			if current.Metadata["access_token"] != "original-token" || current.Generation != modified.Generation || current.StoreGeneration() != modified.StoreGeneration() {
				t.Fatal("stale external Update bypassed version fencing after Modify")
			}
		})
	}
}
