package auth

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type failExecutor struct {
	provider string
	calls    atomic.Int32
}

func TestSessionAffinityXAIOnResultUsesPickedKeys(t *testing.T) {
	tests := []struct {
		name         string
		opts         cliproxyexecutor.Options
		wantFallback bool
	}{
		{
			name: "execution session",
			opts: cliproxyexecutor.Options{Metadata: map[string]any{
				cliproxyexecutor.ExecutionSessionMetadataKey: "execution-session",
			}},
		},
		{
			name:         "caller scoped prompt cache key",
			wantFallback: true,
			opts: cliproxyexecutor.Options{
				SourceFormat:    "openai-response",
				OriginalRequest: []byte(`{"prompt_cache_key":"shared-prompt","conversation":{"id":"conversation-alias"}}`),
				Metadata: map[string]any{
					cliproxyexecutor.CallerScopeMetadataKey: "irreversible-caller-scope",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			affinity := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{TTL: time.Hour})
			defer affinity.Stop()
			auth := &Auth{ID: "xai-auth", Provider: "xai", Status: StatusActive}

			selected, errPick := affinity.Pick(context.Background(), "xai", "grok-model", tt.opts, []*Auth{auth})
			if errPick != nil {
				t.Fatalf("Pick() error = %v", errPick)
			}
			if selected.ID != auth.ID {
				t.Fatalf("Pick() auth = %q, want %q", selected.ID, auth.ID)
			}
			before := affinityCacheExpirations(affinity)
			if len(before) != 1 && !tt.wantFallback || len(before) != 2 && tt.wantFallback {
				t.Fatalf("picked cache entries = %d, want fallback=%v", len(before), tt.wantFallback)
			}
			for key := range tt.opts.Metadata {
				if strings.HasPrefix(key, "session_affinity_") && key != cliproxyexecutor.SessionAffinityProviderMetadataKey && key != cliproxyexecutor.SessionAffinityModelMetadataKey {
					t.Fatalf("internal affinity key leaked through Options.Metadata: %q", key)
				}
			}

			time.Sleep(time.Millisecond)
			affinity.OnResult(Result{AuthID: auth.ID, Provider: "xai", Model: "grok-model", Success: true, Options: tt.opts})
			after := affinityCacheExpirations(affinity)
			for key, beforeExpiry := range before {
				if !after[key].After(beforeExpiry) {
					t.Fatalf("success did not touch picked binding %q: before=%v after=%v", key, beforeExpiry, after[key])
				}
			}

			affinity.OnResult(Result{
				AuthID: auth.ID, Provider: "xai", Model: "grok-model", Success: false,
				Error: &Error{HTTPStatus: http.StatusInternalServerError, Message: "failed"}, Options: tt.opts,
			})
			if remaining := affinityCacheExpirations(affinity); len(remaining) != 0 {
				t.Fatalf("failure retained picked bindings: %v", remaining)
			}
		})
	}
}

func TestSessionAffinityExplicitSessionIsCallerScoped(t *testing.T) {
	affinity := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &RoundRobinSelector{}, TTL: time.Hour})
	defer affinity.Stop()
	auths := []*Auth{
		{ID: "auth-a", Provider: "claude", Status: StatusActive},
		{ID: "auth-b", Provider: "claude", Status: StatusActive},
	}
	pick := func(scope string) string {
		opts := cliproxyexecutor.Options{
			Headers:  http.Header{"X-Session-Id": {"shared-session"}},
			Metadata: map[string]any{},
		}
		if scope != "" {
			opts.Metadata[cliproxyexecutor.CallerScopeMetadataKey] = scope
		}
		selected, errPick := affinity.Pick(context.Background(), "claude", "claude-model", opts, auths)
		if errPick != nil {
			t.Fatalf("Pick(%q) error = %v", scope, errPick)
		}
		return selected.ID
	}

	callerA := pick("caller-a")
	afterA := affinityCacheExpirations(affinity)
	callerB := pick("caller-b")
	afterB := affinityCacheExpirations(affinity)
	if len(afterA) != 1 || len(afterB) != 2 {
		t.Fatalf("caller-scoped cache sizes = %d then %d, want 1 then 2", len(afterA), len(afterB))
	}
	bindings := make(map[string]bool)
	affinity.cache.mu.RLock()
	for _, entry := range affinity.cache.entries {
		bindings[entry.authID] = true
	}
	affinity.cache.mu.RUnlock()
	if !bindings[callerA] || !bindings[callerB] {
		t.Fatalf("caller-scoped cache bindings = %v, want %q and %q", bindings, callerA, callerB)
	}
	if got := pick("caller-a"); got != callerA || len(affinityCacheExpirations(affinity)) != 2 {
		t.Fatalf("caller A binding changed from %q to %q", callerA, got)
	}

	unscopedFirst := pick("")
	unscopedCount := len(affinityCacheExpirations(affinity))
	if got := pick(""); got != unscopedFirst || len(affinityCacheExpirations(affinity)) != unscopedCount {
		t.Fatalf("trusted empty-scope binding changed from %q to %q", unscopedFirst, got)
	}
}

func affinityCacheExpirations(affinity *SessionAffinitySelector) map[string]time.Time {
	affinity.cache.mu.RLock()
	defer affinity.cache.mu.RUnlock()
	out := make(map[string]time.Time, len(affinity.cache.entries))
	for key, entry := range affinity.cache.entries {
		out[key] = entry.expiresAt
	}
	return out
}

type metadataTamperingHook struct {
	NoopHook
	sawPrivateMetadata bool
}

func (h *metadataTamperingHook) OnResult(_ context.Context, result Result) {
	for key := range result.Options.Metadata {
		if key == "session_affinity_primary_key" || key == "session_affinity_fallback_key" || key == "session_affinity_intermediate" {
			h.sawPrivateMetadata = true
		}
		delete(result.Options.Metadata, key)
	}
	result.Options.Metadata[cliproxyexecutor.SessionAffinityProviderMetadataKey] = "tampered-provider"
	result.Options.Metadata[cliproxyexecutor.SessionAffinityModelMetadataKey] = "tampered-model"
}

func TestSessionAffinityResultKeysArePrivateFromHooks(t *testing.T) {
	affinity := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{TTL: time.Hour})
	defer affinity.Stop()
	hook := &metadataTamperingHook{}
	manager := NewManager(nil, affinity, hook)
	auth := &Auth{ID: "xai-private-auth", Provider: "xai", Status: StatusActive}
	opts := cliproxyexecutor.Options{Metadata: map[string]any{
		cliproxyexecutor.ExecutionSessionMetadataKey: "private-result-session",
	}}
	if _, errPick := affinity.Pick(context.Background(), "xai", "grok-private", opts, []*Auth{auth}); errPick != nil {
		t.Fatalf("Pick() error = %v", errPick)
	}
	keys := affinityCacheExpirations(affinity)
	manager.MarkResult(context.Background(), Result{
		AuthID: auth.ID, Provider: "xai", Model: "grok-private", Success: false,
		Error: &Error{HTTPStatus: http.StatusInternalServerError, Message: "failed"}, Options: opts,
	})
	if hook.sawPrivateMetadata {
		t.Fatal("Hook observed private session-affinity metadata")
	}
	for key := range keys {
		if _, ok := affinity.cache.Get(key); ok {
			t.Fatal("Hook metadata mutation prevented affinity cleanup")
		}
	}
}

type affinitySnapshotTamperingHook struct {
	NoopHook
}

func (*affinitySnapshotTamperingHook) OnResult(_ context.Context, result Result) {
	result.Options.Headers.Set("X-Session-Id", "hook-session")
	result.Options.Query.Set("tenant", "hook-tenant")
	copy(result.Options.OriginalRequest, []byte(`{"session_id":"hook"}`))
	result.Options.Metadata[cliproxyexecutor.CallerScopeMetadataKey] = "hook-caller"
	result.Options.Metadata["nested"].(map[string]any)["values"].([]any)[0] = "hook-nested"
	result.Options.Metadata["typed-map"].(map[string]int)["value"] = 40
	result.Options.Metadata["typed-slice"].([]int)[0] = 50
	result.Options.Metadata["typed-pointer"].(*affinityCloneContainer).Values[0] = 60
	result.Options.Metadata["cyclic-map"].(map[string]any)["value"] = "hook-cycle"
	result.Options.Metadata["cyclic-slice"].([]any)[0] = "hook-cycle"
}

type affinitySnapshotInspectingWrapper struct {
	*SessionAffinitySelector
	result Result
}

func (s *affinitySnapshotInspectingWrapper) OnResult(result Result) {
	s.result = result
	s.SessionAffinitySelector.OnResult(result)
}

func TestSessionAffinityWrapperReceivesIndependentPickSnapshot(t *testing.T) {
	const (
		provider = "snapshot-provider"
		model    = "snapshot-model"
		authID   = "snapshot-auth"
	)
	affinity := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{TTL: time.Hour})
	defer affinity.Stop()
	wrapper := &affinitySnapshotInspectingWrapper{SessionAffinitySelector: affinity}
	manager := NewManager(nil, wrapper, &affinitySnapshotTamperingHook{})
	manager.RegisterExecutor(&failExecutor{provider: provider})
	auth := &Auth{ID: authID, Provider: provider, Status: StatusActive, Metadata: map[string]any{"disable_cooling": true}}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	originalBody := []byte(`{"session_id":"picked-body","input":"original"}`)
	typedMap := map[string]int{"value": 4}
	typedSlice := []int{5}
	typedPointer := &affinityCloneContainer{Values: []int{6}}
	cyclicMap := map[string]any{"value": "picked-cycle"}
	cyclicMap["self"] = cyclicMap
	cyclicSlice := make([]any, 1)
	cyclicSlice[0] = cyclicSlice
	opts := cliproxyexecutor.Options{
		Headers:         http.Header{"X-Session-Id": {"picked-session"}, "X-Original": {"header"}},
		Query:           map[string][]string{"tenant": {"picked-tenant"}},
		OriginalRequest: append([]byte(nil), originalBody...),
		Metadata: map[string]any{
			cliproxyexecutor.CallerScopeMetadataKey: "picked-caller",
			"nested":                                map[string]any{"values": []any{"picked-nested"}},
			"typed-map":                             typedMap,
			"typed-slice":                           typedSlice,
			"typed-pointer":                         typedPointer,
			"cyclic-map":                            cyclicMap,
			"cyclic-slice":                          cyclicSlice,
		},
		RequestAfterAuthInterceptor: func(context.Context, cliproxyexecutor.RequestAfterAuthInterceptRequest) cliproxyexecutor.RequestAfterAuthInterceptResponse {
			return cliproxyexecutor.RequestAfterAuthInterceptResponse{
				Headers: http.Header{"X-Session-Id": {"interceptor-session"}},
				Body:    []byte(`{"session_id":"interceptor-body"}`),
			}
		},
	}
	_, errExecute := manager.Execute(context.Background(), []string{provider}, cliproxyexecutor.Request{Model: model, Payload: originalBody}, opts)
	if errExecute == nil {
		t.Fatal("Execute() error = nil, want upstream failure")
	}

	got := wrapper.result.Options
	if got.Headers.Get("X-Session-Id") != "picked-session" || got.Headers.Get("X-Original") != "header" {
		t.Fatalf("selector headers = %v, want original pick headers", got.Headers)
	}
	if got.Query.Get("tenant") != "picked-tenant" {
		t.Fatalf("selector query = %v, want picked-tenant", got.Query)
	}
	if string(got.OriginalRequest) != string(originalBody) {
		t.Fatalf("selector OriginalRequest = %s, want %s", got.OriginalRequest, originalBody)
	}
	if got.Metadata[cliproxyexecutor.CallerScopeMetadataKey] != "picked-caller" {
		t.Fatalf("selector caller scope = %v, want picked-caller", got.Metadata[cliproxyexecutor.CallerScopeMetadataKey])
	}
	if nested := got.Metadata["nested"].(map[string]any)["values"].([]any)[0]; nested != "picked-nested" {
		t.Fatalf("selector nested metadata = %v, want picked-nested", nested)
	}
	if value := got.Metadata["typed-map"].(map[string]int)["value"]; value != 4 {
		t.Fatalf("selector typed map value = %d, want 4", value)
	}
	if value := got.Metadata["typed-slice"].([]int)[0]; value != 5 {
		t.Fatalf("selector typed slice value = %d, want 5", value)
	}
	if value := got.Metadata["typed-pointer"].(*affinityCloneContainer).Values[0]; value != 6 {
		t.Fatalf("selector typed pointer value = %d, want 6", value)
	}
	if value := got.Metadata["cyclic-map"].(map[string]any)["value"]; value != "picked-cycle" {
		t.Fatalf("selector cyclic map value = %v, want picked-cycle", value)
	}
	if _, ok := got.Metadata["cyclic-slice"].([]any)[0].([]any); !ok {
		t.Fatal("selector cyclic slice was changed by Hook")
	}
	if remaining := affinityCacheExpirations(affinity); len(remaining) != 0 {
		t.Fatalf("wrapper fallback failed to remove picked binding: %v", remaining)
	}
}

type errorTamperingHook struct {
	NoopHook
	code       string
	httpStatus int
}

func (h *errorTamperingHook) OnResult(_ context.Context, result Result) {
	if result.Error == nil {
		return
	}
	result.Error.Code = h.code
	result.Error.HTTPStatus = h.httpStatus
}

func TestSessionAffinityResultErrorIsPrivateFromHooks(t *testing.T) {
	tests := []struct {
		name           string
		customSelector bool
		original       *Error
		hookCode       string
		hookStatus     int
		wantBinding    bool
	}{
		{
			name:        "built-in selector deletes credential failure",
			original:    &Error{Message: "failed", HTTPStatus: http.StatusInternalServerError},
			hookCode:    ErrorCodeRequestScoped,
			hookStatus:  http.StatusBadRequest,
			wantBinding: false,
		},
		{
			name:        "built-in selector preserves request failure",
			original:    &Error{Code: ErrorCodeRequestScoped, Message: "invalid", HTTPStatus: http.StatusBadRequest},
			hookStatus:  http.StatusInternalServerError,
			wantBinding: true,
		},
		{
			name:           "custom selector deletes credential failure",
			customSelector: true,
			original:       &Error{Message: "failed", HTTPStatus: http.StatusInternalServerError},
			hookCode:       ErrorCodeRequestScoped,
			hookStatus:     http.StatusBadRequest,
			wantBinding:    false,
		},
		{
			name:           "custom selector preserves request failure",
			customSelector: true,
			original:       &Error{Code: ErrorCodeRequestScoped, Message: "invalid", HTTPStatus: http.StatusBadRequest},
			hookStatus:     http.StatusInternalServerError,
			wantBinding:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			affinity := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{TTL: time.Hour})
			defer affinity.Stop()
			var selector Selector = affinity
			var wrapper *affinitySnapshotInspectingWrapper
			if tt.customSelector {
				wrapper = &affinitySnapshotInspectingWrapper{SessionAffinitySelector: affinity}
				selector = wrapper
			}
			manager := NewManager(nil, selector, &errorTamperingHook{code: tt.hookCode, httpStatus: tt.hookStatus})
			opts := cliproxyexecutor.Options{Headers: http.Header{"X-Session-Id": {"error-hook-session"}}}
			state := sessionAffinityResultForRequest(context.Background(), "provider", "model", opts)
			affinity.cache.Set(state.primaryKey, "auth")
			originalCode, originalStatus := tt.original.Code, tt.original.HTTPStatus

			manager.MarkResult(context.Background(), Result{
				AuthID: "auth", Provider: "provider", Model: "model", Success: false, Error: tt.original, Options: opts,
			})

			_, bindingExists := affinity.cache.Get(state.primaryKey)
			if bindingExists != tt.wantBinding {
				t.Fatalf("binding exists = %v, want %v", bindingExists, tt.wantBinding)
			}
			if wrapper != nil {
				if wrapper.result.Error == tt.original {
					t.Fatal("custom selector received Hook-shared Error pointer")
				}
				if wrapper.result.Error == nil || wrapper.result.Error.Code != originalCode || wrapper.result.Error.HTTPStatus != originalStatus {
					t.Fatalf("custom selector error = %#v, want code %q status %d", wrapper.result.Error, originalCode, originalStatus)
				}
			}
		})
	}
}

type affinityCloneContainer struct {
	Values []int
}

func TestCloneSessionAffinityMetadataClonesTypedContainersAndPointers(t *testing.T) {
	typedMap := map[string]int{"value": 1}
	typedSlice := []int{2}
	typedPointer := &affinityCloneContainer{Values: []int{3}}
	original := map[string]any{
		"map":     typedMap,
		"slice":   typedSlice,
		"pointer": typedPointer,
	}
	cloned := cloneSessionAffinityMetadata(original)

	typedMap["value"] = 10
	typedSlice[0] = 20
	typedPointer.Values[0] = 30
	if got := cloned["map"].(map[string]int)["value"]; got != 1 {
		t.Fatalf("cloned typed map value = %d, want 1", got)
	}
	if got := cloned["slice"].([]int)[0]; got != 2 {
		t.Fatalf("cloned typed slice value = %d, want 2", got)
	}
	clonedPointer := cloned["pointer"].(*affinityCloneContainer)
	if clonedPointer == typedPointer || clonedPointer.Values[0] != 3 {
		t.Fatalf("cloned pointer = %#v, want independent value 3", clonedPointer)
	}
}

func TestCloneSessionAffinityMetadataPreservesCycles(t *testing.T) {
	cyclicMap := map[string]any{"value": "original"}
	cyclicMap["self"] = cyclicMap
	cyclicSlice := make([]any, 1)
	cyclicSlice[0] = cyclicSlice

	cloned := cloneSessionAffinityMetadata(map[string]any{"map": cyclicMap, "slice": cyclicSlice})
	clonedMap := cloned["map"].(map[string]any)
	cyclicMap["value"] = "mutated"
	if clonedMap["value"] != "original" {
		t.Fatalf("cloned cyclic map value = %v, want original", clonedMap["value"])
	}
	clonedSelf := clonedMap["self"].(map[string]any)
	clonedSelf["cycle-marker"] = true
	if clonedMap["cycle-marker"] != true {
		t.Fatal("cloned map cycle was not preserved")
	}

	clonedSlice := cloned["slice"].([]any)
	cyclicSlice[0] = "mutated"
	clonedSelfSlice := clonedSlice[0].([]any)
	clonedSelfSlice[0] = "cycle-marker"
	if clonedSlice[0] != "cycle-marker" {
		t.Fatal("cloned slice cycle was not preserved")
	}
}

func TestCloneSessionAffinityMetadataBudgetFallsBackToAffinityStrings(t *testing.T) {
	deep := map[string]any{"leaf": "value"}
	for range 256 {
		deep = map[string]any{"next": deep}
	}
	original := map[string]any{
		cliproxyexecutor.CallerScopeMetadataKey:             "caller",
		cliproxyexecutor.ExecutionSessionMetadataKey:        "execution",
		cliproxyexecutor.DerivedSessionIDMetadataKey:        "derived",
		cliproxyexecutor.SessionAffinityProviderMetadataKey: "provider",
		cliproxyexecutor.SessionAffinityModelMetadataKey:    "model",
		"deep": deep,
	}
	cloned := cloneSessionAffinityMetadata(original)
	if len(cloned) != 5 {
		t.Fatalf("budget fallback metadata = %#v, want five affinity strings", cloned)
	}
	if cloned[cliproxyexecutor.CallerScopeMetadataKey] != "caller" || cloned[cliproxyexecutor.ExecutionSessionMetadataKey] != "execution" {
		t.Fatalf("budget fallback lost affinity metadata: %#v", cloned)
	}

	original["deep"] = make([]int, maxSessionAffinityMetadataCloneNodes+1)
	cloned = cloneSessionAffinityMetadata(original)
	if len(cloned) != 5 {
		t.Fatalf("large metadata fallback = %#v, want five affinity strings", cloned)
	}
}

func TestCloneSessionAffinityMetadataLargeContainerDoesNotAllocateInputSize(t *testing.T) {
	large := make([]int, 1<<17)
	original := map[string]any{
		cliproxyexecutor.ExecutionSessionMetadataKey: "execution",
		"large": large,
	}
	result := testing.Benchmark(func(b *testing.B) {
		for range b.N {
			cloned := cloneSessionAffinityMetadata(original)
			if len(cloned) != 1 || cloned[cliproxyexecutor.ExecutionSessionMetadataKey] != "execution" {
				b.Fatalf("large metadata fallback = %#v", cloned)
			}
		}
	})
	if allocated := result.AllocedBytesPerOp(); allocated >= int64(len(large))*int64(unsafe.Sizeof(large[0]))/4 {
		t.Fatalf("clone allocated %d bytes/op for %d-byte input container", allocated, len(large)*int(unsafe.Sizeof(large[0])))
	}
}

func TestCloneSessionAffinityMetadataUnsupportedReferencesFallBack(t *testing.T) {
	marker := 1
	tests := map[string]any{
		"func":           func() {},
		"chan":           make(chan int),
		"unsafe-pointer": unsafe.Pointer(&marker),
	}
	for name, unsupported := range tests {
		t.Run(name, func(t *testing.T) {
			original := map[string]any{
				cliproxyexecutor.ExecutionSessionMetadataKey: "execution",
				"unsupported": unsupported,
			}
			cloned := cloneSessionAffinityMetadata(original)
			if len(cloned) != 1 || cloned[cliproxyexecutor.ExecutionSessionMetadataKey] != "execution" {
				t.Fatalf("unsupported reference clone = %#v, want affinity-only fallback", cloned)
			}
		})
	}
}

type callbackTamperingAffinitySelector struct {
	*SessionAffinitySelector
	pickedOptions cliproxyexecutor.Options
}

func (s *callbackTamperingAffinitySelector) Pick(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	s.pickedOptions = opts
	return s.SessionAffinitySelector.Pick(ctx, provider, model, opts, auths)
}

func (s *callbackTamperingAffinitySelector) tamperPickedOptions() {
	s.pickedOptions.Headers.Set("X-Session-Id", "tampered-session")
	s.pickedOptions.Metadata[cliproxyexecutor.SessionAffinityProviderMetadataKey] = "tampered-provider"
	s.pickedOptions.Metadata[cliproxyexecutor.SessionAffinityModelMetadataKey] = "tampered-model"
}

type callbackTamperFailureExecutor struct {
	provider string
}

func (e *callbackTamperFailureExecutor) Identifier() string { return e.provider }
func (*callbackTamperFailureExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, &Error{Message: "failed", HTTPStatus: http.StatusInternalServerError}
}
func (*callbackTamperFailureExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, &Error{Message: "failed", HTTPStatus: http.StatusInternalServerError}
}
func (*callbackTamperFailureExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}
func (*callbackTamperFailureExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, &Error{Message: "failed", HTTPStatus: http.StatusInternalServerError}
}
func (*callbackTamperFailureExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func TestSelectedAuthCallbackCannotTamperPickedAffinityState(t *testing.T) {
	operations := map[string]func(*Manager, string, cliproxyexecutor.Options) error{
		"execute": func(manager *Manager, provider string, opts cliproxyexecutor.Options) error {
			_, err := manager.Execute(context.Background(), []string{provider}, cliproxyexecutor.Request{Model: "model"}, opts)
			return err
		},
		"count-tokens": func(manager *Manager, provider string, opts cliproxyexecutor.Options) error {
			_, err := manager.ExecuteCount(context.Background(), []string{provider}, cliproxyexecutor.Request{Model: "model"}, opts)
			return err
		},
		"stream": func(manager *Manager, provider string, opts cliproxyexecutor.Options) error {
			_, err := manager.ExecuteStream(context.Background(), []string{provider}, cliproxyexecutor.Request{Model: "model"}, opts)
			return err
		},
	}

	for name, execute := range operations {
		t.Run(name, func(t *testing.T) {
			const provider = "callback-tamper-provider"
			affinity := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{TTL: time.Hour})
			defer affinity.Stop()
			selector := &callbackTamperingAffinitySelector{SessionAffinitySelector: affinity}
			manager := NewManager(nil, selector, nil)
			manager.SetRetryConfig(0, 0, 1)
			manager.RegisterExecutor(&callbackTamperFailureExecutor{provider: provider})
			auth := &Auth{ID: "callback-tamper-auth", Provider: provider, Status: StatusActive, Metadata: map[string]any{"disable_cooling": true}}
			if _, errRegister := manager.Register(WithSkipPersist(context.Background()), auth); errRegister != nil {
				t.Fatalf("Register() error = %v", errRegister)
			}
			registry.GetGlobalRegistry().RegisterClient(auth.ID, provider, []*registry.ModelInfo{{ID: "model"}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
			callbackCalls := 0
			opts := cliproxyexecutor.Options{
				Headers: http.Header{"X-Session-Id": {"picked-session"}},
				Metadata: map[string]any{
					cliproxyexecutor.SelectedAuthCallbackMetadataKey: func(string) {
						callbackCalls++
						selector.tamperPickedOptions()
					},
				},
			}

			if errExecute := execute(manager, provider, opts); errExecute == nil {
				t.Fatal("execution error = nil, want upstream failure")
			}
			if callbackCalls != 1 {
				t.Fatalf("selected-auth callback calls = %d, want 1", callbackCalls)
			}
			if remaining := affinityCacheExpirations(affinity); len(remaining) != 0 {
				t.Fatalf("callback mutation prevented picked affinity cleanup: %v", remaining)
			}
		})
	}
}

type sequentialSelector struct {
	n int
}

func (s *sequentialSelector) Pick(_ context.Context, _, _ string, _ cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	selected := auths[s.n%len(auths)]
	s.n++
	return selected, nil
}

func TestSessionAffinityStructuredKeyDoesNotCollideOnSeparators(t *testing.T) {
	fallback := &sequentialSelector{}
	affinity := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: fallback, TTL: time.Hour})
	defer affinity.Stop()
	auths := []*Auth{{ID: "auth-a", Status: StatusActive}, {ID: "auth-b", Status: StatusActive}}
	pick := func(sessionID, model string) *Auth {
		opts := cliproxyexecutor.Options{Headers: http.Header{"X-Session-Id": {sessionID}}, Metadata: map[string]any{}}
		selected, errPick := affinity.Pick(context.Background(), "provider", model, opts, auths)
		if errPick != nil {
			t.Fatalf("Pick(%q, %q) error = %v", sessionID, model, errPick)
		}
		return selected
	}
	first := pick("foo::bar", "baz")
	second := pick("foo", "bar::baz")
	if first.ID == second.ID || fallback.n != 2 {
		t.Fatalf("selections = %q, %q with %d fallback calls; want independent cold bindings", first.ID, second.ID, fallback.n)
	}
	if got := len(affinityCacheExpirations(affinity)); got != 2 {
		t.Fatalf("structured affinity cache entries = %d, want 2", got)
	}
}

func (e *failExecutor) Identifier() string { return e.provider }
func (e *failExecutor) Execute(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.calls.Add(1)
	return cliproxyexecutor.Response{}, &Error{HTTPStatus: http.StatusInternalServerError, Message: "upstream failure"}
}
func (e *failExecutor) ExecuteStream(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.calls.Add(1)
	return nil, &Error{HTTPStatus: http.StatusInternalServerError, Message: "upstream failure"}
}
func (e *failExecutor) Refresh(ctx context.Context, auth *Auth) (*Auth, error) { return auth, nil }
func (e *failExecutor) CountTokens(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}
func (e *failExecutor) HttpRequest(ctx context.Context, auth *Auth, req *http.Request) (*http.Response, error) {
	return nil, nil
}

type successExecutor struct {
	provider string
	calls    atomic.Int32
}

func (e *successExecutor) Identifier() string { return e.provider }
func (e *successExecutor) Execute(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.calls.Add(1)
	return cliproxyexecutor.Response{Payload: []byte(`{"ok":true}`)}, nil
}
func (e *successExecutor) ExecuteStream(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.calls.Add(1)
	return nil, nil
}
func (e *successExecutor) Refresh(ctx context.Context, auth *Auth) (*Auth, error) { return auth, nil }
func (e *successExecutor) CountTokens(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}
func (e *successExecutor) HttpRequest(ctx context.Context, auth *Auth, req *http.Request) (*http.Response, error) {
	return nil, nil
}

func TestManagerSessionAffinityMixedPoolNilMetadataPropagatesFailureCleanup(t *testing.T) {
	ctx := context.Background()
	p1 := "affinity-p1"
	p2 := "affinity-p2"
	model := "test-model"
	auth1ID := "auth-1"
	auth2ID := "auth-2"

	manager := NewManager(nil, nil, nil)
	affinity := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback: &RoundRobinSelector{},
		TTL:      time.Hour,
	})
	defer affinity.Stop()
	manager.SetSelector(affinity)
	failExec := &failExecutor{provider: p1}
	succExec := &successExecutor{provider: p2}
	manager.RegisterExecutor(failExec)
	manager.RegisterExecutor(succExec)

	for _, auth := range []*Auth{
		{
			ID:       auth1ID,
			Provider: p1,
			Status:   StatusActive,
			Metadata: map[string]any{"disable_cooling": true}, // Disable cooling so availability remains active, relying on session affinity unbind
		},
		{
			ID:       auth2ID,
			Provider: p2,
			Status:   StatusActive,
			Metadata: map[string]any{"disable_cooling": true},
		},
	} {
		if _, errRegister := manager.Register(WithSkipPersist(ctx), auth); errRegister != nil {
			t.Fatalf("Register(%s): %v", auth.ID, errRegister)
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	}

	// Inbound request with explicitly nil Metadata, only session header
	req := cliproxyexecutor.Request{Model: model}
	opts := cliproxyexecutor.Options{
		Headers: http.Header{"X-Session-Id": []string{"sess-mixed-1"}},
	}
	if opts.Metadata != nil {
		t.Fatalf("expected test initial opts.Metadata to be nil")
	}

	// 1. Execute request: auth-1 is selected, fails, and execution falls over to auth-2 which succeeds.
	resp, errExec := manager.Execute(ctx, []string{p1, p2}, req, opts)
	if errExec != nil {
		t.Fatalf("first Execute failed: %v", errExec)
	}
	if string(resp.Payload) != `{"ok":true}` {
		t.Fatalf("first Execute payload = %s, want ok", string(resp.Payload))
	}
	if failExec.calls.Load() != 1 {
		t.Fatalf("expected failExec called 1 time, got %d", failExec.calls.Load())
	}
	if succExec.calls.Load() != 1 {
		t.Fatalf("expected succExec called 1 time, got %d", succExec.calls.Load())
	}

	// Verify the affinity cache has auth-2 bound under the "mixed" namespace
	mixedKey := sessionAffinityCacheKey("mixed", "header:sess-mixed-1", model)
	cachedAuthID, ok := affinity.cache.Get(mixedKey)
	if !ok {
		t.Fatalf("expected mixed cache key to be bound to auth-2, but not found in cache")
	}
	if cachedAuthID != auth2ID {
		t.Fatalf("expected mixed cache key to be bound to %q, got %q", auth2ID, cachedAuthID)
	}

	// Verify mismatched provider cache key was NOT used
	if _, okP1 := affinity.cache.Get(sessionAffinityCacheKey(p1, "header:sess-mixed-1", model)); okP1 {
		t.Fatalf("unexpected p1 provider cache key created")
	}

	// 2. Second Execute call with fresh request and nil Metadata for the SAME session
	opts2 := cliproxyexecutor.Options{
		Headers: http.Header{"X-Session-Id": []string{"sess-mixed-1"}},
	}
	resp2, errExec2 := manager.Execute(ctx, []string{p1, p2}, req, opts2)
	if errExec2 != nil {
		t.Fatalf("second Execute failed: %v", errExec2)
	}
	if string(resp2.Payload) != `{"ok":true}` {
		t.Fatalf("second Execute payload = %s, want ok", string(resp2.Payload))
	}
	// failExec call count must remain 1 because session affinity directly picked auth-2
	if failExec.calls.Load() != 1 {
		t.Fatalf("expected failExec to not be called on second request, call count = %d", failExec.calls.Load())
	}
	if succExec.calls.Load() != 2 {
		t.Fatalf("expected succExec called 2 times, got %d", succExec.calls.Load())
	}
}

func TestSessionAffinityAtomicCompareAndDeleteProtectsReboundSession(t *testing.T) {
	cache := NewSessionCache(time.Hour)
	defer cache.Stop()

	sessionKey := "mixed::sess-rebound::model-x"

	// 1. Initial binding to auth-A
	cache.Set(sessionKey, "auth-A")
	if got, ok := cache.Get(sessionKey); !ok || got != "auth-A" {
		t.Fatalf("Get() = %q, %v; want %q, true", got, ok, "auth-A")
	}

	// 2. Session rebinds to auth-B
	cache.Set(sessionKey, "auth-B")
	if got, ok := cache.Get(sessionKey); !ok || got != "auth-B" {
		t.Fatalf("Get() = %q, %v; want %q, true", got, ok, "auth-B")
	}

	// 3. Stale failure for auth-A tries to delete
	deleted := cache.CompareAndDelete(sessionKey, "auth-A")
	if deleted {
		t.Fatalf("CompareAndDelete with stale auth-A unexpectedly returned true")
	}
	// Session must still be bound to auth-B
	if got, ok := cache.Get(sessionKey); !ok || got != "auth-B" {
		t.Fatalf("Get() after stale delete attempt = %q, %v; want %q, true", got, ok, "auth-B")
	}

	// 4. Valid failure for auth-B deletes
	deletedValid := cache.CompareAndDelete(sessionKey, "auth-B")
	if !deletedValid {
		t.Fatalf("CompareAndDelete with active auth-B returned false")
	}
	if _, ok := cache.Get(sessionKey); ok {
		t.Fatalf("sessionKey still present in cache after valid CompareAndDelete")
	}
}

func TestSessionAffinityDelayedSuccessDoesNotOverwriteReboundAuth(t *testing.T) {
	affinity := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback: &RoundRobinSelector{},
		TTL:      time.Hour,
	})
	defer affinity.Stop()

	sessionKey := sessionAffinityCacheKey("mixed", "header:sess-delay-success", "model-x")

	// 1. Initially auth-A is bound
	affinity.cache.Set(sessionKey, "auth-A")

	// 2. Session rebinds to auth-B
	affinity.cache.Set(sessionKey, "auth-B")

	// 3. A delayed success for auth-A arrives
	opts := cliproxyexecutor.Options{
		Headers: http.Header{"X-Session-Id": []string{"sess-delay-success"}},
		Metadata: map[string]any{
			cliproxyexecutor.SessionAffinityProviderMetadataKey: "mixed",
			cliproxyexecutor.SessionAffinityModelMetadataKey:    "model-x",
		},
	}
	affinity.OnResult(Result{
		AuthID:   "auth-A",
		Provider: "provider-a",
		Model:    "model-x",
		Success:  true,
		Options:  opts,
	})

	// 4. Cache must remain bound to auth-B, not overwritten by auth-A
	got, ok := affinity.cache.Get(sessionKey)
	if !ok || got != "auth-B" {
		t.Fatalf("cache binding = %q, %v; want auth-B, true (delayed success of auth-A must not overwrite auth-B)", got, ok)
	}
}

func TestSessionAffinityOnResultWithMismatchedNamespaceFailsToUnbind(t *testing.T) {
	affinity := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback: &RoundRobinSelector{},
		TTL:      time.Hour,
	})
	defer affinity.Stop()

	sessionID := "header:sess-ns-1"
	model := "test-model"
	authID := "auth-1"

	// Bind under "mixed" namespace
	mixedKey := sessionAffinityCacheKey("mixed", sessionID, model)
	affinity.cache.Set(mixedKey, authID)

	// Call OnResult with options carrying the propagated "mixed" namespace
	res := Result{
		AuthID:   authID,
		Provider: "gemini", // actual provider
		Model:    model,
		Success:  false,
		Error:    &Error{HTTPStatus: http.StatusInternalServerError},
		Options: cliproxyexecutor.Options{
			Headers: http.Header{"X-Session-Id": []string{"sess-ns-1"}},
			Metadata: map[string]any{
				cliproxyexecutor.SessionAffinityProviderMetadataKey: "mixed",
				cliproxyexecutor.SessionAffinityModelMetadataKey:    model,
			},
		},
	}

	affinity.OnResult(res)

	// Verify mixedKey is cleanly removed
	if _, ok := affinity.cache.Get(mixedKey); ok {
		t.Fatalf("expected mixed key to be removed after OnResult with propagated namespace")
	}
}

func TestSessionCacheCapacityBounding(t *testing.T) {
	t.Parallel()

	maxEntries := 10
	cache := NewSessionCacheWithCapacity(time.Hour, maxEntries)
	defer cache.Stop()

	for i := 0; i < 20; i++ {
		cache.Set(fmt.Sprintf("session-%d", i), fmt.Sprintf("auth-%d", i))
	}

	if cache.Len() > maxEntries {
		t.Fatalf("cache.Len() = %d, want <= %d", cache.Len(), maxEntries)
	}
}
