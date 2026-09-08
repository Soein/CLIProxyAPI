package auth

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestSessionAffinityHierarchySurvivesResultCaptureAndContextSync(t *testing.T) {
	for _, callerScope := range []string{"", "caller-a"} {
		for _, tc := range []struct {
			name          string
			parentHeaders http.Header
			childHeaders  http.Header
			payload       []byte
			wantParent    bool
		}{
			{
				name: "codex-subagent", wantParent: true,
				parentHeaders: http.Header{"Session-Id": {"parent"}},
				childHeaders:  http.Header{"Session-Id": {"parent"}, "Thread-Id": {"child"}},
			},
			{
				name: "codex-fork", wantParent: true,
				parentHeaders: http.Header{"Session-Id": {"parent"}},
				childHeaders: http.Header{
					"Session-Id":            {"child"},
					"X-Codex-Turn-Metadata": {`{"forked_from_thread_id":"parent"}`},
				},
			},
			{
				name: "long-claude-subagent", wantParent: true,
				parentHeaders: http.Header{"X-Claude-Code-Session-Id": {strings.Repeat("s", 200)}},
				childHeaders: http.Header{
					"X-Claude-Code-Session-Id": {strings.Repeat("s", 200)},
					"X-Claude-Code-Agent-Id":   {strings.Repeat("a", 100)},
				},
			},
			{
				name:    "prompt-cache-conversation-alias",
				payload: []byte(`{"prompt_cache_key":"cache","conversation":"conversation"}`),
			},
		} {
			t.Run(tc.name+"/"+callerScope, func(t *testing.T) {
				ctx := context.Background()
				selector := NewSessionAffinitySelector(nil)
				t.Cleanup(selector.Stop)
				auths := []*Auth{{ID: "auth-a"}, {ID: "auth-b"}}
				parent := cliproxyexecutor.Options{
					Headers:  tc.parentHeaders.Clone(),
					Metadata: map[string]any{cliproxyexecutor.CallerScopeMetadataKey: callerScope},
				}
				if _, errPick := selector.Pick(ctx, "codex", "model", parent, auths); errPick != nil {
					t.Fatal(errPick)
				}
				wantParent, _ := parent.Metadata[cliproxyexecutor.CanonicalSessionIDMetadataKey].(string)
				child := cliproxyexecutor.Options{
					Headers: tc.childHeaders.Clone(), OriginalRequest: tc.payload,
					Metadata: map[string]any{
						cliproxyexecutor.CallerScopeMetadataKey:     callerScope,
						cliproxyexecutor.ParentSessionIDMetadataKey: "stale:parent",
					},
				}
				if _, errPick := selector.Pick(ctx, "codex", "model", child, auths); errPick != nil {
					t.Fatal(errPick)
				}
				if !tc.wantParent {
					wantParent = ""
				}
				assertParent := func(label string) {
					t.Helper()
					meta := logging.GetClientRequestMetadata(syncMetadataSessionToContext(ctx, child.Metadata))
					if meta.ParentSessionID != wantParent {
						t.Fatalf("%s parent = %q, want %q", label, meta.ParentSessionID, wantParent)
					}
					if tc.wantParent && !isHierarchyParent(meta.SessionID, meta.ParentSessionID) {
						t.Fatalf("%s hierarchy rejected after context sync: %+v", label, meta)
					}
					if len(meta.SessionID) > 256 || len(meta.ParentSessionID) > 256 {
						t.Fatalf("%s hierarchy exceeds identity bounds: %+v", label, meta)
					}
				}
				assertParent("after pick")
				state := sessionAffinityResultForRequest(ctx, "codex", "model", child)
				assertParent("after result capture")
				if got, _ := state.options.Metadata[cliproxyexecutor.ParentSessionIDMetadataKey].(string); got != wantParent {
					t.Fatalf("result snapshot parent = %q, want %q", got, wantParent)
				}
			})
		}
	}
}

func TestHASharding_AliasCooldownPreservesBoundedSessionProvenance(t *testing.T) {
	withQuotaCooldownEnabled(t)
	for _, path := range []string{"single", "mixed"} {
		for _, spillover := range []bool{false, true} {
			name := path + "/strict"
			if spillover {
				name = path + "/spillover"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				const provider, alias, target = "codex", "ha-alias", "ha-target"
				selector := NewSessionAffinitySelector(&WeightedRoundRobinSelector{})
				t.Cleanup(selector.Stop)
				manager := newHAShardingManager(t, selector, provider)
				manager.SetSpilloverEnabled(spillover)
				retryAfter := time.Hour
				cool := func(id, model string) {
					manager.MarkResult(ctx, Result{
						AuthID: id, Provider: provider, Model: model, RetryAfter: &retryAfter,
						Error: &Error{HTTPStatus: http.StatusTooManyRequests, Message: "model quota"},
					})
				}
				for _, id := range []string{"local", "remote"} {
					priority := "1"
					if id == "remote" {
						priority = "9"
					}
					registry.GetGlobalRegistry().RegisterClient(id, provider, []*registry.ModelInfo{{ID: alias}, {ID: target}})
					t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
					if _, errRegister := manager.Register(ctx, &Auth{
						ID: id, Provider: provider, Status: StatusActive,
						Attributes: map[string]string{"priority": priority, AttributeWeight: "1"},
					}); errRegister != nil {
						t.Fatal(errRegister)
					}
					cool(id, alias)
				}
				manager.SetOAuthModelAlias(map[string][]internalconfig.OAuthModelAlias{
					provider: {{Name: target, Alias: alias, Fork: true}},
				})
				options := func() cliproxyexecutor.Options {
					return cliproxyexecutor.Options{
						Headers: http.Header{
							"X-Claude-Code-Session-Id": {strings.Repeat("s", 200)},
							"X-Claude-Code-Agent-Id":   {strings.Repeat("a", 100)},
						},
						Metadata: map[string]any{},
					}
				}
				pick := func(opts cliproxyexecutor.Options) (*Auth, error) {
					if path == "single" {
						auth, _, errPick := manager.pickNextLegacy(ctx, provider, alias, opts, nil)
						return auth, errPick
					}
					auth, _, _, errPick := manager.pickNextMixedLegacy(ctx, []string{provider}, alias, opts, nil)
					return auth, errPick
				}
				opts := options()
				selected, errPick := pick(opts)
				if errPick != nil || selected == nil || selected.ID != "local" {
					t.Fatalf("healthy aliased target selection = (%v, %v), want local", selected, errPick)
				}
				canonicalID, _ := opts.Metadata[cliproxyexecutor.CanonicalSessionIDMetadataKey].(string)
				if len(canonicalID) > 256 || !strings.Contains(canonicalID, "#") {
					t.Fatalf("composite session identity was not bounded: %q", canonicalID)
				}
				state := sessionAffinityResultForRequest(ctx, provider, alias, opts)
				if got, ok := selector.cache.Get(state.primaryKey); !ok || got != selected.ID {
					t.Fatalf("captured result key binding = (%q, %v), want local", got, ok)
				}
				opts.Headers.Set("X-Claude-Code-Agent-Id", "tampered")
				opts.Metadata[cliproxyexecutor.SessionAffinityProviderMetadataKey] = "tampered"
				selector.onResult(resultForSessionAffinity(Result{AuthID: selected.ID}, state), state)
				if _, ok := selector.cache.Get(state.primaryKey); ok {
					t.Fatal("failed bounded session binding survived result cleanup")
				}
				cool("local", target)
				selected, errPick = pick(options())
				if spillover {
					if errPick != nil || selected == nil || selected.ID != "remote" {
						t.Fatalf("target cooldown spillover = (%v, %v), want remote", selected, errPick)
					}
				} else if selected != nil || statusCodeFromError(errPick) != http.StatusTooManyRequests {
					t.Fatalf("strict ownership target cooldown = (%v, %v), want 429", selected, errPick)
				}
			})
		}
	}
}
