package auth

import (
	"context"
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

func homeGenerationOptions() cliproxyexecutor.Options {
	return cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FormatGemini,
		OriginalRequest: []byte(`{"contents":[{"role":"user","parts":[{"text":"step 1"}]},{"role":"model","parts":[{"text":"ack 1"}]},{"role":"user","parts":[{"text":"step 2"}]}]}`),
		Metadata: map[string]any{
			cliproxyexecutor.CallerScopeMetadataKey:             "home-generation-regression",
			cliproxyexecutor.SessionAffinityProviderMetadataKey: "google",
			cliproxyexecutor.SessionAffinityModelMetadataKey:    "gemini-2.5-pro",
		},
	}
}

func TestHomeDispatchLateFailureKeepsNewerLCPBinding(t *testing.T) {
	for _, warm := range []bool{false, true} {
		name := "cold_placeholder"
		if warm {
			name = "warm_match"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			selector := NewSessionAffinitySelector(&RoundRobinSelector{})
			t.Cleanup(selector.Stop)
			manager := NewManager(nil, selector, nil)
			auth := &Auth{ID: "home-generation-auth"}
			report := func(opts cliproxyexecutor.Options, success bool) {
				result := Result{AuthID: auth.ID, Provider: "google", Model: "gemini-2.5-pro", Options: opts, Success: success}
				if !success {
					result.Error = &Error{HTTPStatus: 500, Message: "late failure"}
				}
				manager.reportHomeResult(ctx, result, auth)
			}

			if warm {
				initial := homeGenerationOptions()
				if sessionID, _ := manager.homeDispatchSessionIDs(initial); sessionID == "" {
					t.Fatal("initial Home dispatch did not create a session")
				}
				report(initial, true)
			}
			late := homeGenerationOptions()
			sessionID, _ := manager.homeDispatchSessionIDs(late)
			if sessionID == "" {
				t.Fatal("late Home dispatch did not create a session")
			}
			lateGeneration, ok := late.Metadata[cliproxyexecutor.LCPAccessGenerationMetadataKey].(uint64)
			if !ok || lateGeneration == 0 {
				t.Errorf("late Home dispatch LCP generation = %v, want nonzero uint64", late.Metadata[cliproxyexecutor.LCPAccessGenerationMetadataKey])
			}

			newer := homeGenerationOptions()
			if newerID, _ := manager.homeDispatchSessionIDs(newer); newerID != sessionID {
				t.Fatalf("newer dispatch session = %q, want %q", newerID, sessionID)
			}
			report(newer, true)
			before, _, beforeOK := selector.matcher.LookupSession(sessionID)
			if !beforeOK || len(before) != 1 || before[0] != auth.ID {
				t.Fatalf("newer success binding = %v, found = %t", before, beforeOK)
			}
			if got := late.Metadata[cliproxyexecutor.LCPAccessGenerationMetadataKey]; got != lateGeneration {
				t.Errorf("late request generation changed from %d to %v", lateGeneration, got)
			}

			report(late, false)
			after, _, afterOK := selector.matcher.LookupSession(sessionID)
			if !afterOK || len(after) != 1 || after[0] != auth.ID {
				t.Errorf("late failure removed newer binding: auths = %v, found = %t", after, afterOK)
			}
		})
	}
}
