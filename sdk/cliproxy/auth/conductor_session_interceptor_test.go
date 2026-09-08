package auth

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	internallogging "github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestApplyRequestAfterAuthInterceptorPreservesSelectedSession(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, scope := range []string{"", "caller-a"} {
			for _, tc := range []struct {
				name     string
				headers  http.Header
				body     string
				selected string
				parent   string
				response cliproxyexecutor.RequestAfterAuthInterceptResponse
			}{
				{
					name:     "codex body child and header parent",
					headers:  http.Header{"Session-Id": {"parent"}},
					body:     `{"thread_id":"child"}`,
					selected: "codex:child", parent: "codex:parent",
					response: cliproxyexecutor.RequestAfterAuthInterceptResponse{Headers: http.Header{"X-Trace-ID": {"trace"}}},
				},
				{
					name:     "claude body outranks generic header",
					headers:  http.Header{"Session-Id": {"alias"}},
					body:     `{"metadata":{"user_id":"{\"session_id\":\"child\",\"parent_session_id\":\"parent\"}"}}`,
					selected: "claude:child", parent: "claude:parent",
					response: cliproxyexecutor.RequestAfterAuthInterceptResponse{Headers: http.Header{"X-Trace-ID": {"trace"}}},
				},
				{
					name:    "selector resolved alias",
					headers: http.Header{"X-Session-Id": {"raw-alias"}},
					body:    `{}`, selected: "header:resolved-session",
					response: cliproxyexecutor.RequestAfterAuthInterceptResponse{Headers: http.Header{"X-Trace-ID": {"trace"}}},
				},
				{
					name:     "unrelated header cleared",
					headers:  http.Header{"Session-Id": {"parent"}, "X-Trace-Id": {"trace"}},
					body:     `{"thread_id":"child"}`,
					selected: "codex:child", parent: "codex:parent",
					response: cliproxyexecutor.RequestAfterAuthInterceptResponse{ClearHeaders: []string{"X-Trace-Id"}},
				},
				{
					name:     "unrelated body changed",
					headers:  http.Header{"Session-Id": {"parent"}},
					body:     `{"thread_id":"child"}`,
					selected: "codex:child", parent: "codex:parent",
					response: cliproxyexecutor.RequestAfterAuthInterceptResponse{Body: []byte(`{"thread_id":"child","temperature":0.5}`)},
				},
			} {
				t.Run(fmt.Sprintf("%s/stream=%t/scope=%s", tc.name, stream, scope), func(t *testing.T) {
					metadata := map[string]any{cliproxyexecutor.CallerScopeMetadataKey: scope}
					selected, parent := scopeAffinitySessionIDs(metadata, tc.selected, tc.parent)
					metadata[cliproxyexecutor.CanonicalSessionIDMetadataKey] = selected
					if parent != "" {
						metadata[cliproxyexecutor.ParentSessionIDMetadataKey] = parent
					}
					_, got, err := applyRequestAfterAuthInterceptor(context.Background(), nil, "codex", cliproxyexecutor.Request{Payload: []byte(`{}`)}, cliproxyexecutor.Options{
						Stream: stream, Headers: tc.headers, OriginalRequest: []byte(tc.body), Metadata: metadata,
						RequestAfterAuthInterceptor: func(_ context.Context, request cliproxyexecutor.RequestAfterAuthInterceptRequest) cliproxyexecutor.RequestAfterAuthInterceptResponse {
							if request.Stream != stream {
								t.Errorf("stream flag = %t, want %t", request.Stream, stream)
							}
							return tc.response
						},
					}, "test-model")
					if err != nil {
						t.Fatal(err)
					}
					meta := internallogging.GetClientRequestMetadata(syncMetadataSessionToContext(context.Background(), got.Metadata))
					if meta.SessionID != selected || meta.ParentSessionID != parent {
						t.Fatalf("reported session = (%q, %q), want selected (%q, %q)", meta.SessionID, meta.ParentSessionID, selected, parent)
					}
				})
			}
		}
	}
}

func TestApplyRequestAfterAuthInterceptorUpdatesChangedSession(t *testing.T) {
	for _, scope := range []string{"", "caller-a"} {
		for _, tc := range []struct {
			name     string
			headers  http.Header
			body     string
			response cliproxyexecutor.RequestAfterAuthInterceptResponse
			want     string
			parent   string
		}{
			{
				name:    "header child changed with body parent",
				headers: http.Header{"X-Session-Id": {"old"}}, body: `{"parent_session_id":"parent"}`,
				response: cliproxyexecutor.RequestAfterAuthInterceptResponse{Headers: http.Header{"x-session-id": {"new"}}},
				want:     "header:new", parent: "header:parent",
			},
			{
				name:    "body child changed",
				headers: http.Header{"Session-Id": {"parent"}}, body: `{"thread_id":"old"}`,
				response: cliproxyexecutor.RequestAfterAuthInterceptResponse{Body: []byte(`{"thread_id":"new"}`)},
				want:     "codex:new", parent: "codex:parent",
			},
			{
				name:    "new root removes previous parent",
				headers: http.Header{"X-Session-Id": {"old"}}, body: `{}`,
				response: cliproxyexecutor.RequestAfterAuthInterceptResponse{Headers: http.Header{"X-Session-Id": {"new"}}},
				want:     "header:new",
			},
			{
				name:    "clear header removes stale lcp",
				headers: http.Header{"x-session-id": {"old"}}, body: `{}`,
				response: cliproxyexecutor.RequestAfterAuthInterceptResponse{ClearHeaders: []string{"X-Session-ID"}},
			},
			{
				name:    "empty header removes identity",
				headers: http.Header{"X-Session-Id": {"old"}}, body: `{}`,
				response: cliproxyexecutor.RequestAfterAuthInterceptResponse{Headers: http.Header{"x-session-id": {""}}},
			},
			{
				name:     "body removes identity",
				body:     `{"session_id":"old"}`,
				response: cliproxyexecutor.RequestAfterAuthInterceptResponse{Body: []byte(`{}`)},
			},
			{
				name:     "cache alias does not become parent",
				body:     `{"prompt_cache_key":"old","conversation":{"id":"alias"}}`,
				response: cliproxyexecutor.RequestAfterAuthInterceptResponse{Body: []byte(`{"prompt_cache_key":"new","conversation":{"id":"alias"}}`)},
				want:     "pck:new",
			},
		} {
			t.Run(tc.name+"/"+scope, func(t *testing.T) {
				metadata := map[string]any{
					cliproxyexecutor.CallerScopeMetadataKey:          scope,
					cliproxyexecutor.CanonicalSessionIDMetadataKey:   "picked-old",
					cliproxyexecutor.ParentSessionIDMetadataKey:      "picked-parent",
					cliproxyexecutor.LCPAffinitySessionIDMetadataKey: "lcp:stale",
				}
				_, got, err := applyRequestAfterAuthInterceptor(context.Background(), nil, "openai", cliproxyexecutor.Request{Payload: []byte(tc.body)}, cliproxyexecutor.Options{
					Headers: tc.headers, Metadata: metadata,
					RequestAfterAuthInterceptor: func(context.Context, cliproxyexecutor.RequestAfterAuthInterceptRequest) cliproxyexecutor.RequestAfterAuthInterceptResponse {
						return tc.response
					},
				}, "test-model")
				if err != nil {
					t.Fatal(err)
				}
				want, parent := scopeAffinitySessionIDs(metadata, tc.want, tc.parent)
				meta := internallogging.GetClientRequestMetadata(syncMetadataSessionToContext(context.Background(), got.Metadata))
				if meta.SessionID != want || meta.ParentSessionID != parent {
					t.Fatalf("reported session = (%q, %q), want (%q, %q)", meta.SessionID, meta.ParentSessionID, want, parent)
				}
				if _, ok := got.Metadata[cliproxyexecutor.LCPAffinitySessionIDMetadataKey]; ok {
					t.Fatal("stale LCP identity retained after explicit session changed")
				}
			})
		}
	}
}

func TestApplyRequestAfterAuthInterceptorLongScopedIdentityMatchesSelector(t *testing.T) {
	for _, provider := range []string{"claude", "codex"} {
		t.Run(provider, func(t *testing.T) {
			selector := NewSessionAffinitySelector(nil)
			t.Cleanup(selector.Stop)
			ctx := context.Background()
			sessionID := strings.Repeat("s", 200)
			agentID := strings.Repeat("a", 100)
			parentAgentID := strings.Repeat("p", 100)
			headers := make(http.Header)
			response := cliproxyexecutor.RequestAfterAuthInterceptResponse{}
			if provider == "claude" {
				headers.Set("X-Claude-Code-Session-Id", sessionID)
				headers.Set("X-Claude-Code-Agent-Id", "old")
				headers.Set("X-Claude-Code-Parent-Agent-Id", parentAgentID)
				response.Headers = http.Header{"X-Claude-Code-Agent-Id": {agentID}}
			} else {
				headers.Set("Session-Id", sessionID)
				headers.Set("X-Codex-Turn-Metadata", `{"agent_name":"old","subagent_kind":"thread_spawn"}`)
				response.Headers = http.Header{"X-Codex-Turn-Metadata": {fmt.Sprintf(`{"agent_name":%q,"subagent_kind":"thread_spawn"}`, agentID)}}
			}
			opts := cliproxyexecutor.Options{
				Headers:         headers,
				OriginalRequest: []byte(`{}`),
				Metadata:        map[string]any{cliproxyexecutor.CallerScopeMetadataKey: "caller-a"},
			}
			auths := []*Auth{{ID: "auth-a"}}
			if _, err := selector.Pick(ctx, provider, "model", opts, auths); err != nil {
				t.Fatal(err)
			}
			previousCanonical := opts.Metadata[cliproxyexecutor.CanonicalSessionIDMetadataKey]
			previousParent := opts.Metadata[cliproxyexecutor.ParentSessionIDMetadataKey]
			opts.RequestAfterAuthInterceptor = func(context.Context, cliproxyexecutor.RequestAfterAuthInterceptRequest) cliproxyexecutor.RequestAfterAuthInterceptResponse {
				return response
			}
			_, got, err := applyRequestAfterAuthInterceptor(ctx, nil, provider, cliproxyexecutor.Request{}, opts, "model")
			if err != nil {
				t.Fatal(err)
			}
			next := cliproxyexecutor.Options{
				Headers:         mergeRequestHeaders(headers, response.Headers, nil),
				OriginalRequest: []byte(`{}`),
				Metadata:        map[string]any{cliproxyexecutor.CallerScopeMetadataKey: "caller-a"},
			}
			if _, err := selector.Pick(ctx, provider, "model", next, auths); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{cliproxyexecutor.CanonicalSessionIDMetadataKey, cliproxyexecutor.ParentSessionIDMetadataKey} {
				if got.Metadata[key] != next.Metadata[key] {
					t.Errorf("interceptor %s = %v, next selector = %v", key, got.Metadata[key], next.Metadata[key])
				}
			}
			if opts.Metadata[cliproxyexecutor.CanonicalSessionIDMetadataKey] != previousCanonical || opts.Metadata[cliproxyexecutor.ParentSessionIDMetadataKey] != previousParent {
				t.Error("interceptor changed original picked metadata")
			}
		})
	}
}
