package auth

import (
	"context"
	"fmt"
	"sync"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

type kimiMixedRecordingExecutor struct {
	refreshMockExecutor
	mu      sync.Mutex
	authIDs []string
}

func (e *kimiMixedRecordingExecutor) Execute(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.mu.Lock()
	e.authIDs = append(e.authIDs, auth.ID)
	e.mu.Unlock()
	return e.refreshMockExecutor.Execute(ctx, auth, req, opts)
}

func (e *kimiMixedRecordingExecutor) ExecuteStream(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.mu.Lock()
	e.authIDs = append(e.authIDs, auth.ID)
	e.mu.Unlock()
	return e.refreshMockExecutor.ExecuteStream(ctx, auth, req, opts)
}

func (e *kimiMixedRecordingExecutor) selectedAuthIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.authIDs...)
}

func assertKimiMixedStream(t *testing.T, stream *cliproxyexecutor.StreamResult) {
	t.Helper()
	if stream.Chunks == nil {
		t.Error("ExecuteStream returned a nil chunk channel")
		return
	}
	chunks := 0
	for chunk := range stream.Chunks {
		if chunk.Err != nil {
			t.Errorf("stream chunk error: %v", chunk.Err)
		}
		if len(chunk.Payload) > 0 {
			chunks++
		}
	}
	if chunks == 0 {
		t.Error("ExecuteStream returned no payload chunks")
	}
}

func TestManagerMixedKimiAliasesUseOneExecutorAndSeparateAccountPools(t *testing.T) {
	const model = "kimi-mixed-alias-model"
	paths := []struct {
		name     string
		selector func() Selector
		plugin   bool
	}{
		{name: "session_affinity", selector: func() Selector { return NewSessionAffinitySelector(&RoundRobinSelector{}) }},
		{name: "custom_selector", selector: func() Selector { return lastAuthSelector{} }},
		{name: "plugin_scheduler", selector: func() Selector { return &RoundRobinSelector{} }, plugin: true},
	}
	for pathIndex, path := range paths {
		for caseIndex, tc := range []struct {
			name, requested, aiProvider, comProvider, wantPool string
		}{
			{name: "canonical_ai", requested: "kimi-ai", aiProvider: "kimi-ai", comProvider: "kimi", wantPool: "ai"},
			{name: "raw_ai", requested: "kimi.ai", aiProvider: "kimi.ai", comProvider: "kimi.com", wantPool: "ai"},
			{name: "canonical_com", requested: "kimi", aiProvider: "kimi-ai", comProvider: "kimi", wantPool: "com"},
			{name: "raw_com", requested: "kimi.com", aiProvider: "kimi.ai", comProvider: "kimi.com", wantPool: "com"},
		} {
			t.Run(path.name+"/"+tc.name, func(t *testing.T) {
				ctx := context.Background()
				selector := path.selector()
				if stoppable, ok := selector.(StoppableSelector); ok {
					t.Cleanup(stoppable.Stop)
				}
				manager := NewManager(nil, selector, nil)
				executor := &kimiMixedRecordingExecutor{refreshMockExecutor: refreshMockExecutor{id: "kimi"}}
				manager.RegisterExecutor(executor)
				ids := map[string]string{
					"ai":  fmt.Sprintf("kimi-mixed-%d-%d-ai", pathIndex, caseIndex),
					"com": fmt.Sprintf("kimi-mixed-%d-%d-com", pathIndex, caseIndex),
				}
				for _, pool := range []struct{ id, provider string }{
					{id: ids["ai"], provider: tc.aiProvider},
					{id: ids["com"], provider: tc.comProvider},
				} {
					registry.GetGlobalRegistry().RegisterClient(pool.id, pool.provider, []*registry.ModelInfo{{ID: model}})
					t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(pool.id) })
					if _, err := manager.Register(WithSkipPersist(ctx), &Auth{ID: pool.id, Provider: pool.provider, Status: StatusActive}); err != nil {
						t.Fatal(err)
					}
					manager.RefreshSchedulerEntry(pool.id)
				}
				var plugin *fakePluginScheduler
				if path.plugin {
					plugin = &fakePluginScheduler{
						resp:    pluginapi.SchedulerPickResponse{Handled: true, AuthID: ids[tc.wantPool]},
						handled: true,
					}
					manager.SetPluginScheduler(plugin)
				}
				req := cliproxyexecutor.Request{Model: model}
				if _, err := manager.Execute(ctx, []string{tc.requested}, req, cliproxyexecutor.Options{}); err != nil {
					t.Errorf("Execute(%q): %v", tc.requested, err)
				}
				streamCtx, cancel := context.WithCancel(ctx)
				t.Cleanup(cancel)
				stream, err := manager.ExecuteStream(streamCtx, []string{tc.requested}, req, cliproxyexecutor.Options{Stream: true})
				if err != nil || stream == nil {
					cancel()
					t.Errorf("ExecuteStream(%q) = (%v, %v)", tc.requested, stream, err)
				} else {
					assertKimiMixedStream(t, stream)
					cancel()
				}
				if got := executor.selectedAuthIDs(); len(got) != 2 || got[0] != ids[tc.wantPool] || got[1] != ids[tc.wantPool] {
					t.Errorf("selected auth IDs = %v, want only %s", got, ids[tc.wantPool])
				}
				if plugin != nil && plugin.calls == 0 {
					t.Error("plugin scheduler was not called")
				}
				if plugin != nil {
					for _, request := range plugin.requests {
						if len(request.Candidates) != 1 || request.Candidates[0].ID != ids[tc.wantPool] {
							t.Errorf("plugin candidates = %v, want only %s", request.Candidates, ids[tc.wantPool])
						}
					}
				}
			})
		}
	}
}

func TestManagerMixedKimiRawAliasRouteAwareFallback(t *testing.T) {
	const routeModel = "kimi-mixed-route-alias"
	const targetModel = "kimi-mixed-upstream-target"
	ctx := context.Background()
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	executor := &kimiMixedRecordingExecutor{refreshMockExecutor: refreshMockExecutor{id: "kimi"}}
	manager.RegisterExecutor(executor)
	for _, candidate := range []*Auth{
		{ID: "kimi-mixed-route-ai", Provider: "kimi.ai", Status: StatusActive, Attributes: map[string]string{AttributeAuthKind: "oauth"}},
		{ID: "kimi-mixed-route-com", Provider: "kimi.com", Status: StatusActive, Attributes: map[string]string{AttributeAuthKind: "oauth"}},
	} {
		registry.GetGlobalRegistry().RegisterClient(candidate.ID, candidate.Provider, []*registry.ModelInfo{{ID: targetModel}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(candidate.ID) })
		if _, err := manager.Register(WithSkipPersist(ctx), candidate); err != nil {
			t.Fatal(err)
		}
		manager.RefreshSchedulerEntry(candidate.ID)
	}
	manager.SetOAuthModelAlias(map[string][]internalconfig.OAuthModelAlias{
		"kimi.ai": {{Name: targetModel, Alias: routeModel, Fork: true}},
	})
	if !manager.routeAwareSelectionRequired(&Auth{Provider: "kimi.ai", Attributes: map[string]string{AttributeAuthKind: "oauth"}}, routeModel) {
		t.Fatal("fixture did not require route-aware legacy selection")
	}
	selected, _, provider, err := manager.pickNextMixed(ctx, []string{"kimi.ai"}, routeModel, cliproxyexecutor.Options{}, nil)
	if err != nil || selected == nil || selected.ID != "kimi-mixed-route-ai" || provider != "kimi-ai" {
		t.Fatalf("route-aware mixed pick = (%v, %q, %v), want kimi.ai account", selected, provider, err)
	}
	req := cliproxyexecutor.Request{Model: routeModel}
	if _, err := manager.Execute(ctx, []string{"kimi.ai"}, req, cliproxyexecutor.Options{}); err != nil {
		t.Fatal(err)
	}
	streamCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	stream, err := manager.ExecuteStream(streamCtx, []string{"kimi.ai"}, req, cliproxyexecutor.Options{Stream: true})
	if err != nil || stream == nil {
		cancel()
		t.Fatalf("route-aware ExecuteStream = (%v, %v)", stream, err)
	}
	assertKimiMixedStream(t, stream)
	cancel()
	if got := executor.selectedAuthIDs(); len(got) != 2 || got[0] != "kimi-mixed-route-ai" || got[1] != "kimi-mixed-route-ai" {
		t.Fatalf("route-aware selected auth IDs = %v, want only kimi.ai account", got)
	}
}
