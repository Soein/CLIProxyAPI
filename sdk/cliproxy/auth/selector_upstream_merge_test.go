package auth

import (
	"context"
	"net/http"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

func TestSessionAffinityScopedChildFailurePreservesParent(t *testing.T) {
	for _, fork := range []bool{false, true} {
		for _, inherit := range []bool{false, true} {
			name := "subagent"
			if fork {
				name = "fork"
			}
			if inherit {
				name += "-inherit"
			} else {
				name += "-isolate"
			}
			t.Run(name, func(t *testing.T) {
				selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
					TTL: time.Minute, SubagentAffinity: &inherit,
				})
				defer selector.Stop()
				ctx := context.Background()
				auths := []*Auth{{ID: "auth-a"}, {ID: "auth-b"}}
				parent := cliproxyexecutor.Options{
					Headers:  http.Header{"Session-Id": {"parent"}},
					Metadata: map[string]any{cliproxyexecutor.CallerScopeMetadataKey: "caller-a"},
				}
				parentAuth, errParent := selector.Pick(ctx, "codex", "model", parent, auths)
				if errParent != nil || parentAuth == nil {
					t.Fatalf("parent Pick() = %v, %v", parentAuth, errParent)
				}
				child := cliproxyexecutor.Options{
					Headers:  http.Header{"Session-Id": {"parent"}, "Thread-Id": {"child"}},
					Metadata: map[string]any{cliproxyexecutor.CallerScopeMetadataKey: "caller-a"},
				}
				if fork {
					child.Headers.Set("X-Codex-Turn-Metadata", `{"forked_from_thread_id":"parent"}`)
				}
				childAuth, errChild := selector.Pick(ctx, "codex", "model", child, auths)
				if errChild != nil || childAuth == nil {
					t.Fatalf("child Pick() = %v, %v", childAuth, errChild)
				}
				if got := childAuth.ID == parentAuth.ID; got != (fork || inherit) {
					t.Fatalf("child inherits = %v, want %v", got, fork || inherit)
				}
				parentKeys := sessionAffinityKeysForRequest(ctx, "codex", "model", parent)
				childKeys := sessionAffinityKeysForRequest(ctx, "codex", "model", child)
				if parentKeys.primaryKey == childKeys.primaryKey {
					t.Fatal("child and parent share a cache key")
				}
				otherCaller := cloneSessionAffinityOptions(child)
				otherCaller.Metadata[cliproxyexecutor.CallerScopeMetadataKey] = "caller-b"
				if keys := sessionAffinityKeysForRequest(ctx, "codex", "model", otherCaller); keys.primaryKey == childKeys.primaryKey || keys.fallbackKey == childKeys.fallbackKey {
					t.Fatal("child or parent cache key crosses caller scopes")
				}
				// Force bounded metadata fallback before capturing the execution result.
				child.Metadata["unsupported"] = make(chan int)
				state := sessionAffinityResultForRequest(ctx, "codex", "model", child)
				child.Headers.Set("Thread-Id", "tampered")
				child.Metadata[cliproxyexecutor.CallerScopeMetadataKey] = "tampered"
				result := resultForSessionAffinity(Result{AuthID: childAuth.ID, Success: false}, state)
				selector.onResult(result, state)
				if _, ok := selector.cache.Get(childKeys.primaryKey); ok {
					t.Fatal("failed child binding remains")
				}
				if got, ok := selector.cache.Get(parentKeys.primaryKey); !ok || got != parentAuth.ID {
					t.Fatalf("parent binding after child failure = (%q, %v)", got, ok)
				}
			})
		}
	}
}

func TestSessionAffinityLCPStaleFailurePreservesNewerBindingWithBoundedSnapshot(t *testing.T) {
	selector := NewSessionAffinitySelector(nil)
	defer selector.Stop()
	ctx := context.Background()
	auths := []*Auth{{ID: "auth-a"}, {ID: "auth-b"}}
	opts := cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FormatOpenAI,
		OriginalRequest: []byte(`{"messages":[{"role":"user","content":"stable prefix"}]}`),
		Metadata: map[string]any{
			cliproxyexecutor.CallerScopeMetadataKey: "caller",
			"unsupported":                           make(chan int),
		},
	}
	auth, errPick := selector.Pick(ctx, "openai", "model", opts, auths)
	if errPick != nil || auth == nil {
		t.Fatalf("first Pick() = %v, %v", auth, errPick)
	}
	state := sessionAffinityResultForRequest(ctx, "openai", "model", opts)
	generation, _ := state.options.Metadata[cliproxyexecutor.LCPAccessGenerationMetadataKey].(uint64)
	if generation == 0 {
		t.Fatal("bounded snapshot lost the LCP access generation")
	}
	newOpts := cloneSessionAffinityOptions(opts)
	if _, errNext := selector.Pick(ctx, "openai", "model", newOpts, auths); errNext != nil {
		t.Fatal(errNext)
	}
	selector.OnResult(Result{AuthID: auth.ID, Provider: "openai", Model: "model", Options: newOpts, Success: true})
	selector.onResult(resultForSessionAffinity(Result{AuthID: auth.ID, Success: false}, state), state)
	fingerprints, minPrefix := lcpFingerprintsFromMetadata(state.options.Metadata)
	match, ok := selector.matcher.MatchFingerprints(lcpAffinityNamespace("openai", "model", opts.Metadata), fingerprints, minPrefix)
	if !ok || match.AuthID != auth.ID {
		t.Fatalf("stale failure evicted newer LCP binding: match=%+v, found=%v", match, ok)
	}
}

func TestSessionAffinitySnapshotOmitsDispatchCallbackWithoutDroppingMetadata(t *testing.T) {
	for name, callbackKeys := range map[string][]string{
		"auth":  {cliproxyexecutor.SelectedAuthCallbackMetadataKey},
		"index": {cliproxyexecutor.SelectedAuthIndexCallbackMetadataKey},
		"both":  {cliproxyexecutor.SelectedAuthCallbackMetadataKey, cliproxyexecutor.SelectedAuthIndexCallbackMetadataKey},
	} {
		t.Run(name, func(t *testing.T) {
			metadata := map[string]any{"custom": map[string]any{"values": []int{1, 2}}}
			for _, key := range callbackKeys {
				metadata[key] = func(string) { t.Fatal("snapshot invoked dispatch callback") }
			}
			snapshot := cloneSessionAffinityMetadata(metadata)
			custom, ok := snapshot["custom"].(map[string]any)
			if !ok {
				t.Fatalf("snapshot lost custom metadata: %#v", snapshot)
			}
			metadata["custom"].(map[string]any)["values"].([]int)[0] = 9
			if got := custom["values"].([]int)[0]; got != 1 {
				t.Fatalf("snapshot shares custom metadata: got %d, want 1", got)
			}
			for _, key := range callbackKeys {
				if _, exists := snapshot[key]; exists {
					t.Fatalf("snapshot retains dispatch callback %q", key)
				}
				if _, exists := metadata[key]; !exists {
					t.Fatalf("snapshot removed original dispatch callback %q", key)
				}
			}
		})
	}
}
