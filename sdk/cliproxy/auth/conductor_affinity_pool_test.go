package auth

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type affinityPoolResultSelector struct {
	*SessionAffinitySelector
	mu           sync.Mutex
	finalResults []Result
}

func (s *affinityPoolResultSelector) OnResult(result Result) {
	intermediate, _ := result.Options.Metadata["session_affinity_intermediate"].(bool)
	if !intermediate {
		s.mu.Lock()
		s.finalResults = append(s.finalResults, result)
		s.mu.Unlock()
	}
	s.SessionAffinitySelector.OnResult(result)
}

func (s *affinityPoolResultSelector) FinalResults() []Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Result(nil), s.finalResults...)
}

func newAffinityPoolManager(t *testing.T, executor *openAICompatPoolExecutor) (*Manager, *affinityPoolResultSelector, string, string) {
	t.Helper()
	alias := "affinity-pool-model"
	authID := "affinity-pool-auth-" + t.Name()
	selector := &affinityPoolResultSelector{SessionAffinitySelector: NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback: &RoundRobinSelector{},
		TTL:      time.Hour,
	})}
	t.Cleanup(selector.Stop)
	manager := NewManager(nil, selector, nil)
	manager.SetConfig(&internalconfig.Config{OpenAICompatibility: []internalconfig.OpenAICompatibility{{
		Name: "pool",
		Models: []internalconfig.OpenAICompatibilityModel{
			{Name: "upstream-a", Alias: alias},
			{Name: "upstream-b", Alias: alias},
		},
	}}})
	manager.RegisterExecutor(executor)
	auth := &Auth{
		ID:       authID,
		Provider: openAICompatPoolProviderKey,
		Status:   StatusActive,
		Attributes: map[string]string{
			"api_key":      "test-key",
			"compat_name":  "pool",
			"provider_key": openAICompatPoolProviderKey,
		},
		Metadata: map[string]any{"disable_cooling": true},
	}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, openAICompatPoolProviderKey, []*registry.ModelInfo{{ID: alias}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
	return manager, selector, alias, authID
}

func TestModelPoolLaterSuccessPreservesAffinityBinding(t *testing.T) {
	poolErr := &Error{HTTPStatus: http.StatusBadGateway, Message: "first model failed"}
	tests := []struct {
		name string
		run  func(*Manager, string) error
		exec *openAICompatPoolExecutor
	}{
		{
			name: "execute",
			exec: &openAICompatPoolExecutor{id: openAICompatPoolProviderKey, executeErrors: map[string]error{"upstream-a": poolErr}},
			run: func(manager *Manager, alias string) error {
				_, errExecute := manager.Execute(context.Background(), []string{openAICompatPoolProviderKey}, cliproxyexecutor.Request{Model: alias}, cliproxyexecutor.Options{Headers: http.Header{"X-Session-Id": {"pool-success"}}})
				return errExecute
			},
		},
		{
			name: "stream bootstrap",
			exec: &openAICompatPoolExecutor{id: openAICompatPoolProviderKey, streamFirstErrors: map[string]error{"upstream-a": poolErr}},
			run: func(manager *Manager, alias string) error {
				result, errStream := manager.ExecuteStream(context.Background(), []string{openAICompatPoolProviderKey}, cliproxyexecutor.Request{Model: alias}, cliproxyexecutor.Options{Headers: http.Header{"X-Session-Id": {"pool-success"}}, Stream: true})
				if errStream != nil {
					return errStream
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						return chunk.Err
					}
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager, selector, alias, authID := newAffinityPoolManager(t, tt.exec)
			if errRun := tt.run(manager, alias); errRun != nil {
				t.Fatalf("execution error = %v", errRun)
			}
			key := "mixed::header:pool-success::" + alias
			if got, ok := selector.cache.Get(key); !ok || got != authID {
				t.Fatalf("affinity binding = %q, %v; want %q, true", got, ok, authID)
			}
			final := selector.FinalResults()
			if len(final) != 1 || !final[0].Success {
				t.Fatalf("final affinity outcomes = %+v, want one success", final)
			}
		})
	}
}

func TestModelPoolAllFailuresRemoveAffinityOnlyOnFinalOutcome(t *testing.T) {
	poolErr := &Error{HTTPStatus: http.StatusBadGateway, Message: "model failed"}
	tests := []struct {
		name string
		run  func(*Manager, string) error
		exec *openAICompatPoolExecutor
	}{
		{
			name: "execute",
			exec: &openAICompatPoolExecutor{id: openAICompatPoolProviderKey, executeErrors: map[string]error{"upstream-a": poolErr, "upstream-b": poolErr}},
			run: func(manager *Manager, alias string) error {
				_, errExecute := manager.Execute(context.Background(), []string{openAICompatPoolProviderKey}, cliproxyexecutor.Request{Model: alias}, cliproxyexecutor.Options{Headers: http.Header{"X-Session-Id": {"pool-failure"}}})
				return errExecute
			},
		},
		{
			name: "stream bootstrap",
			exec: &openAICompatPoolExecutor{id: openAICompatPoolProviderKey, streamFirstErrors: map[string]error{"upstream-a": poolErr, "upstream-b": poolErr}},
			run: func(manager *Manager, alias string) error {
				result, errStream := manager.ExecuteStream(context.Background(), []string{openAICompatPoolProviderKey}, cliproxyexecutor.Request{Model: alias}, cliproxyexecutor.Options{Headers: http.Header{"X-Session-Id": {"pool-failure"}}, Stream: true})
				if errStream != nil {
					return errStream
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						return chunk.Err
					}
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager, selector, alias, _ := newAffinityPoolManager(t, tt.exec)
			if errRun := tt.run(manager, alias); errRun == nil {
				t.Fatal("execution error = nil, want pool failure")
			}
			key := "mixed::header:pool-failure::" + alias
			if _, ok := selector.cache.Get(key); ok {
				t.Fatalf("failed pool retained affinity binding %q", key)
			}
			final := selector.FinalResults()
			if len(final) != 1 || final[0].Success {
				t.Fatalf("final affinity outcomes = %+v, want one failure", final)
			}
		})
	}
}

type requestScopedPoolExecutor struct {
	mu    sync.Mutex
	calls []string
}

func (*requestScopedPoolExecutor) Identifier() string { return openAICompatPoolProviderKey }

func (e *requestScopedPoolExecutor) record(auth *Auth, req cliproxyexecutor.Request) error {
	e.mu.Lock()
	e.calls = append(e.calls, auth.ID+"|"+req.Model)
	e.mu.Unlock()
	if auth.ID == "request-scoped-auth-a" {
		return customStatusError{code: http.StatusBadRequest, msg: "advance_credential"}
	}
	return nil
}

func (e *requestScopedPoolExecutor) Execute(_ context.Context, auth *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if errExecute := e.record(auth, req); errExecute != nil {
		return cliproxyexecutor.Response{}, errExecute
	}
	return cliproxyexecutor.Response{Payload: []byte("ok")}, nil
}

func (e *requestScopedPoolExecutor) CountTokens(_ context.Context, auth *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if errCount := e.record(auth, req); errCount != nil {
		return cliproxyexecutor.Response{}, errCount
	}
	return cliproxyexecutor.Response{Payload: []byte("ok")}, nil
}

func (e *requestScopedPoolExecutor) ExecuteStream(_ context.Context, auth *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	if errStream := e.record(auth, req); errStream != nil {
		return nil, errStream
	}
	chunks := make(chan cliproxyexecutor.StreamChunk, 1)
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("ok")}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (*requestScopedPoolExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}
func (*requestScopedPoolExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *requestScopedPoolExecutor) Calls() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.calls...)
}

func newRequestScopedPoolManager(t *testing.T, action string) (*Manager, *requestScopedPoolExecutor, string) {
	t.Helper()
	alias := "request-scoped-pool"
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(&internalconfig.Config{OpenAICompatibility: []internalconfig.OpenAICompatibility{{
		Name: "pool",
		Models: []internalconfig.OpenAICompatibilityModel{
			{Name: "upstream-a", Alias: alias},
			{Name: "upstream-b", Alias: alias},
		},
	}}})
	executor := &requestScopedPoolExecutor{}
	manager.RegisterExecutor(executor)
	auths := []*Auth{
		{
			ID: "request-scoped-auth-a", Provider: openAICompatPoolProviderKey, Status: StatusActive,
			Attributes: map[string]string{"api_key": "a", "compat_name": "pool", "provider_key": openAICompatPoolProviderKey, "priority": "10"},
			Metadata:   map[string]any{"request_scoped_errors": []internalconfig.RequestScopedErrorRule{{Status: http.StatusBadRequest, Match: []string{"advance_credential"}, Action: action}}},
		},
		{
			ID: "request-scoped-auth-b", Provider: openAICompatPoolProviderKey, Status: StatusActive,
			Attributes: map[string]string{"api_key": "b", "compat_name": "pool", "provider_key": openAICompatPoolProviderKey},
		},
	}
	for _, auth := range auths {
		if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("Register(%s) error = %v", auth.ID, errRegister)
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, openAICompatPoolProviderKey, []*registry.ModelInfo{{ID: alias}})
		authID := auth.ID
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
	}
	return manager, executor, alias
}

func TestRequestScopedContinueAdvancesCredentialBeforeNextPoolModel(t *testing.T) {
	for _, action := range []string{RequestScopedActionContinue, RequestScopedActionContinueAndCooldown} {
		for _, path := range []string{"execute", "count", "stream"} {
			t.Run(action+"/"+path, func(t *testing.T) {
				manager, executor, alias := newRequestScopedPoolManager(t, action)
				var errRun error
				switch path {
				case "execute":
					_, errRun = manager.Execute(context.Background(), []string{openAICompatPoolProviderKey}, cliproxyexecutor.Request{Model: alias}, cliproxyexecutor.Options{})
				case "count":
					_, errRun = manager.ExecuteCount(context.Background(), []string{openAICompatPoolProviderKey}, cliproxyexecutor.Request{Model: alias}, cliproxyexecutor.Options{})
				case "stream":
					var result *cliproxyexecutor.StreamResult
					result, errRun = manager.ExecuteStream(context.Background(), []string{openAICompatPoolProviderKey}, cliproxyexecutor.Request{Model: alias}, cliproxyexecutor.Options{Stream: true})
					if errRun == nil {
						for range result.Chunks {
						}
					}
				}
				if errRun != nil {
					t.Fatalf("execution error = %v", errRun)
				}
				calls := executor.Calls()
				if len(calls) != 2 {
					t.Fatalf("calls = %v, want one call per credential", calls)
				}
				if calls[0][:len("request-scoped-auth-a|")] != "request-scoped-auth-a|" || calls[1][:len("request-scoped-auth-b|")] != "request-scoped-auth-b|" {
					t.Fatalf("calls = %v, want credential A then credential B", calls)
				}
			})
		}
	}
}
