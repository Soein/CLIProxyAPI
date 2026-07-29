package auth

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

type haTestExecutor struct {
	schedulerTestExecutor
	id string
}

func (e haTestExecutor) Identifier() string { return e.id }

type preferRemoteSelector struct {
	candidates []string
}

type blockingOwnedRing struct {
	checked chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *blockingOwnedRing) IsMine(string) bool {
	r.once.Do(func() { close(r.checked) })
	<-r.release
	return true
}

func (*blockingOwnedRing) Ready() bool { return true }

func (s *preferRemoteSelector) Pick(_ context.Context, _, _ string, _ cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	s.candidates = s.candidates[:0]
	var fallback *Auth
	for _, candidate := range auths {
		if candidate == nil {
			continue
		}
		s.candidates = append(s.candidates, candidate.ID)
		fallback = candidate
		if candidate.ID == "remote" {
			return candidate, nil
		}
	}
	return fallback, nil
}

func newHAShardingManager(t *testing.T, selector Selector, providers ...string) *Manager {
	t.Helper()
	manager := NewManager(nil, selector, nil)
	for _, provider := range providers {
		manager.RegisterExecutor(haTestExecutor{id: provider})
	}
	manager.SetAuthRing(&stubRing{ready: true, mine: map[string]bool{"local": true, "remote": false}})
	manager.SetAuthShardingEnabled(true)
	return manager
}

func registerHAAuth(t *testing.T, manager *Manager, id, provider string) {
	t.Helper()
	if _, err := manager.Register(context.Background(), &Auth{ID: id, Provider: provider, Status: StatusActive}); err != nil {
		t.Fatalf("Register(%s): %v", id, err)
	}
}

func TestHASharding_FastSchedulerScalesPastSixtyFourCandidates(t *testing.T) {
	auths := make([]*Auth, 0, 66)
	mine := make(map[string]bool, 66)
	for index := 0; index < 65; index++ {
		id := fmt.Sprintf("remote-%03d", index)
		auths = append(auths, &Auth{ID: id, Provider: "codex", Status: StatusActive})
		mine[id] = false
	}
	auths = append(auths, &Auth{ID: "zz-local", Provider: "codex", Status: StatusActive})
	mine["zz-local"] = true

	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.scheduler.rebuild(auths, 0)
	manager.SetAuthRing(&stubRing{ready: true, mine: mine})
	manager.SetAuthShardingEnabled(true)

	selected, err := manager.pickWithShardFilter(context.Background(), "codex", "", cliproxyexecutor.Options{}, nil)
	if err != nil {
		t.Fatalf("pickWithShardFilter() error = %v", err)
	}
	if selected == nil || selected.ID != "zz-local" {
		t.Fatalf("pickWithShardFilter() auth = %#v, want zz-local", selected)
	}
}

func TestHASharding_OwnershipFailsClosedUntilRingReady(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	if !manager.OwnsAuth("auth-1") {
		t.Fatal("OwnsAuth() = false with sharding disabled, want true")
	}
	manager.SetAuthShardingEnabled(true)
	if manager.OwnsAuth("auth-1") {
		t.Fatal("OwnsAuth() = true without a ring, want fail-closed false")
	}
	manager.SetAuthRing(&stubRing{ready: false, mine: map[string]bool{"auth-1": true}})
	if manager.OwnsAuth("auth-1") {
		t.Fatal("OwnsAuth() = true with an unready ring, want fail-closed false")
	}
	manager.SetAuthRing(&stubRing{ready: true, mine: map[string]bool{"auth-1": true}})
	if !manager.OwnsAuth("auth-1") {
		t.Fatal("OwnsAuth() = false for an owned auth on a ready ring, want true")
	}
}

func TestHASharding_StrictAdmissionRejectsAuthLostAfterSchedulerPick(t *testing.T) {
	const (
		authID = "ownership-lost-after-pick"
		model  = "ownership-lost-after-pick-model"
	)
	ring := &blockingOwnedRing{checked: make(chan struct{}), release: make(chan struct{})}
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.RegisterExecutor(schedulerTestExecutor{})
	manager.SetAuthRing(ring)
	manager.SetAuthShardingEnabled(true)
	manager.SetSpilloverEnabled(false)
	registerSchedulerModels(t, "test", model, authID)
	if _, errRegister := manager.Register(context.Background(), &Auth{ID: authID, Provider: "test", Status: StatusActive}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}

	type selectionResult struct {
		auth *Auth
		err  error
	}
	selected := make(chan selectionResult, 1)
	go func() {
		auth, errSelect := manager.SelectAuth(context.Background(), "test", model, cliproxyexecutor.Options{})
		selected <- selectionResult{auth: auth, err: errSelect}
	}()
	<-ring.checked
	manager.SetAuthRing(&stubRing{ready: true, mine: map[string]bool{authID: false}})
	close(ring.release)

	select {
	case result := <-selected:
		if result.auth != nil || result.err == nil {
			t.Fatalf("SelectAuth() after ownership loss = (%#v, %v), want strict rejection", result.auth, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("SelectAuth() did not finish after ownership changed")
	}
}

func TestHASharding_UnreadyRingCannotSpillOver(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.scheduler.rebuild([]*Auth{{ID: "remote", Provider: "codex", Status: StatusActive}}, 0)
	manager.SetAuthShardingEnabled(true)
	manager.SetSpilloverEnabled(true)
	manager.SetAuthRing(&stubRing{ready: false, mine: map[string]bool{"remote": true}})

	selected, err := manager.pickWithShardFilter(context.Background(), "codex", "", cliproxyexecutor.Options{}, nil)
	if selected != nil || err == nil {
		t.Fatalf("pickWithShardFilter() = (%#v, %v), want fail-closed error without spillover", selected, err)
	}
}

func TestHASharding_LegacySelectionPathsOnlyExposeOwnedCandidates(t *testing.T) {
	t.Run("single custom selector", func(t *testing.T) {
		selector := &preferRemoteSelector{}
		manager := newHAShardingManager(t, selector, "test")
		registerHAAuth(t, manager, "local", "test")
		registerHAAuth(t, manager, "remote", "test")

		selected, _, err := manager.pickNext(context.Background(), "test", "", cliproxyexecutor.Options{}, nil)
		if err != nil {
			t.Fatalf("pickNext() error = %v", err)
		}
		if selected == nil || selected.ID != "local" {
			t.Fatalf("pickNext() auth = %#v, want local", selected)
		}
		if len(selector.candidates) != 1 || selector.candidates[0] != "local" {
			t.Fatalf("custom selector candidates = %v, want [local]", selector.candidates)
		}
	})

	t.Run("mixed custom selector", func(t *testing.T) {
		selector := &preferRemoteSelector{}
		manager := newHAShardingManager(t, selector, "test-a", "test-b")
		registerHAAuth(t, manager, "local", "test-a")
		registerHAAuth(t, manager, "remote", "test-b")

		selected, _, provider, err := manager.pickNextMixed(context.Background(), []string{"test-a", "test-b"}, "", cliproxyexecutor.Options{}, nil)
		if err != nil {
			t.Fatalf("pickNextMixed() error = %v", err)
		}
		if selected == nil || selected.ID != "local" || provider != "test-a" {
			t.Fatalf("pickNextMixed() = (%#v, %q), want (local, test-a)", selected, provider)
		}
		if len(selector.candidates) != 1 || selector.candidates[0] != "local" {
			t.Fatalf("custom selector candidates = %v, want [local]", selector.candidates)
		}
	})

	t.Run("session affinity cache", func(t *testing.T) {
		selector := NewSessionAffinitySelector(&RoundRobinSelector{})
		opts := cliproxyexecutor.Options{OriginalRequest: []byte(`{"metadata":{"user_id":"user_account__session_aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}}`)}
		if selected, err := selector.Pick(context.Background(), "test", "", opts, []*Auth{{ID: "remote"}}); err != nil || selected == nil {
			t.Fatalf("prime session affinity: selected=%#v err=%v", selected, err)
		}
		manager := newHAShardingManager(t, selector, "test")
		registerHAAuth(t, manager, "local", "test")
		registerHAAuth(t, manager, "remote", "test")

		selected, _, err := manager.pickNext(context.Background(), "test", "", opts, nil)
		if err != nil {
			t.Fatalf("pickNext() error = %v", err)
		}
		if selected == nil || selected.ID != "local" {
			t.Fatalf("pickNext() auth = %#v, want local despite cached remote binding", selected)
		}
	})
}

func TestHASharding_PluginSchedulerOnlyReceivesOwnedCandidates(t *testing.T) {
	newPlugin := func() *fakePluginScheduler {
		plugin := &fakePluginScheduler{handled: true}
		plugin.pick = func(_ context.Context, req pluginapi.SchedulerPickRequest) (pluginapi.SchedulerPickResponse, bool, error) {
			for _, candidate := range req.Candidates {
				if candidate.ID == "remote" {
					return pluginapi.SchedulerPickResponse{Handled: true, AuthID: "remote"}, true, nil
				}
			}
			return pluginapi.SchedulerPickResponse{Handled: true, AuthID: "local"}, true, nil
		}
		return plugin
	}

	t.Run("single", func(t *testing.T) {
		plugin := newPlugin()
		manager := newHAShardingManager(t, &RoundRobinSelector{}, "test")
		manager.SetPluginScheduler(plugin)
		registerHAAuth(t, manager, "local", "test")
		registerHAAuth(t, manager, "remote", "test")

		selected, _, err := manager.pickNext(context.Background(), "test", "", cliproxyexecutor.Options{}, nil)
		if err != nil {
			t.Fatalf("pickNext() error = %v", err)
		}
		if selected == nil || selected.ID != "local" {
			t.Fatalf("pickNext() auth = %#v, want local", selected)
		}
		if len(plugin.requests) != 1 || len(plugin.requests[0].Candidates) != 1 || plugin.requests[0].Candidates[0].ID != "local" {
			t.Fatalf("plugin candidates = %#v, want only local", plugin.requests)
		}
	})

	t.Run("mixed", func(t *testing.T) {
		plugin := newPlugin()
		manager := newHAShardingManager(t, &RoundRobinSelector{}, "test-a", "test-b")
		manager.SetPluginScheduler(plugin)
		registerHAAuth(t, manager, "local", "test-a")
		registerHAAuth(t, manager, "remote", "test-b")

		selected, _, provider, err := manager.pickNextMixed(context.Background(), []string{"test-a", "test-b"}, "", cliproxyexecutor.Options{}, nil)
		if err != nil {
			t.Fatalf("pickNextMixed() error = %v", err)
		}
		if selected == nil || selected.ID != "local" || provider != "test-a" {
			t.Fatalf("pickNextMixed() = (%#v, %q), want (local, test-a)", selected, provider)
		}
		if len(plugin.requests) != 1 || len(plugin.requests[0].Candidates) != 1 || plugin.requests[0].Candidates[0].ID != "local" {
			t.Fatalf("plugin candidates = %#v, want only local", plugin.requests)
		}
	})
}

func TestHASharding_PluginSchedulerDelegateBuiltinOnlyUsesOwnedCandidates(t *testing.T) {
	newManager := func(t *testing.T, providers ...string) (*Manager, *fakePluginScheduler) {
		t.Helper()
		manager := NewManager(nil, &FillFirstSelector{}, nil)
		for _, provider := range providers {
			manager.RegisterExecutor(haTestExecutor{id: provider})
		}
		manager.SetAuthRing(&stubRing{ready: true, mine: map[string]bool{"a-remote": false, "z-local": true}})
		manager.SetAuthShardingEnabled(true)
		manager.SetSpilloverEnabled(true)
		plugin := &fakePluginScheduler{
			resp:    pluginapi.SchedulerPickResponse{Handled: true, DelegateBuiltin: pluginapi.SchedulerBuiltinRoundRobin},
			handled: true,
		}
		manager.SetPluginScheduler(plugin)
		return manager, plugin
	}

	t.Run("single", func(t *testing.T) {
		manager, plugin := newManager(t, "test")
		registerHAAuth(t, manager, "a-remote", "test")
		registerHAAuth(t, manager, "z-local", "test")

		selected, _, err := manager.pickNext(context.Background(), "test", "", cliproxyexecutor.Options{}, nil)
		if err != nil {
			t.Fatalf("pickNext() error = %v", err)
		}
		if selected == nil || selected.ID != "z-local" {
			t.Fatalf("pickNext() auth = %#v, want z-local", selected)
		}
		if len(plugin.requests) != 1 || len(plugin.requests[0].Candidates) != 1 || plugin.requests[0].Candidates[0].ID != "z-local" {
			t.Fatalf("plugin candidates = %#v, want only z-local", plugin.requests)
		}
	})

	t.Run("mixed", func(t *testing.T) {
		manager, plugin := newManager(t, "test-a", "test-b")
		registerHAAuth(t, manager, "a-remote", "test-a")
		registerHAAuth(t, manager, "z-local", "test-b")

		selected, _, provider, err := manager.pickNextMixed(context.Background(), []string{"test-a", "test-b"}, "", cliproxyexecutor.Options{}, nil)
		if err != nil {
			t.Fatalf("pickNextMixed() error = %v", err)
		}
		if selected == nil || selected.ID != "z-local" || provider != "test-b" {
			t.Fatalf("pickNextMixed() = (%#v, %q), want (z-local, test-b)", selected, provider)
		}
		if len(plugin.requests) != 1 || len(plugin.requests[0].Candidates) != 1 || plugin.requests[0].Candidates[0].ID != "z-local" {
			t.Fatalf("plugin candidates = %#v, want only z-local", plugin.requests)
		}
	})
}

func TestHASharding_RouteAwareLegacySelectionEnforcesOwnership(t *testing.T) {
	const (
		provider = "codex"
		alias    = "route-alias"
		upstream = "route-upstream"
	)
	manager := newHAShardingManager(t, &RoundRobinSelector{}, provider)
	manager.SetOAuthModelAlias(map[string][]internalconfig.OAuthModelAlias{
		provider: {{Name: upstream, Alias: alias}},
	})
	registerHAAuth(t, manager, "local", provider)
	registerHAAuth(t, manager, "remote", provider)
	reg := registry.GetGlobalRegistry()
	for _, id := range []string{"local", "remote"} {
		reg.RegisterClient(id, provider, []*registry.ModelInfo{{ID: upstream}})
		t.Cleanup(func() { reg.UnregisterClient(id) })
	}

	selected, _, err := manager.pickNext(context.Background(), provider, alias, cliproxyexecutor.Options{}, nil)
	if err != nil {
		t.Fatalf("pickNext() error = %v", err)
	}
	if selected == nil || selected.ID != "local" {
		t.Fatalf("route-aware pickNext() auth = %#v, want local", selected)
	}
}

func TestHASharding_LegacySpilloverOnlyAfterOwnedCandidatesUnavailable(t *testing.T) {
	selector := &preferRemoteSelector{}
	manager := newHAShardingManager(t, selector, "test")
	manager.SetSpilloverEnabled(true)
	if _, err := manager.Register(context.Background(), &Auth{
		ID:             "local",
		Provider:       "test",
		Status:         StatusError,
		Unavailable:    true,
		NextRetryAfter: time.Now().Add(time.Minute),
	}); err != nil {
		t.Fatalf("Register(local): %v", err)
	}
	registerHAAuth(t, manager, "remote", "test")

	selected, _, err := manager.pickNext(context.Background(), "test", "", cliproxyexecutor.Options{}, nil)
	if err != nil {
		t.Fatalf("pickNext() error = %v", err)
	}
	if selected == nil || selected.ID != "remote" {
		t.Fatalf("spillover pickNext() auth = %#v, want remote", selected)
	}
}
