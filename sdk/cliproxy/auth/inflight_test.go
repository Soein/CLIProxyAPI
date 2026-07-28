package auth

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	internalcache "github.com/router-for-me/CLIProxyAPI/v7/internal/cache"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

type xaiInflightTestExecutor struct {
	mu             sync.Mutex
	selected       []string
	executeStart   chan string
	executeRelease <-chan struct{}
	countStart     chan string
	countRelease   <-chan struct{}
	streamRelease  <-chan struct{}
	streamError    error
	phaseTracking  chan bool
}

type providerCaptureSelector struct {
	provider string
}

func (s *providerCaptureSelector) Pick(_ context.Context, provider, _ string, _ cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	s.provider = provider
	return auths[0], nil
}

func (e *xaiInflightTestExecutor) Identifier() string { return "xai" }

func (e *xaiInflightTestExecutor) record(authID string) {
	e.mu.Lock()
	e.selected = append(e.selected, authID)
	e.mu.Unlock()
}

func (e *xaiInflightTestExecutor) Execute(ctx context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.record(auth.ID)
	if e.phaseTracking != nil {
		e.phaseTracking <- coreusage.PhaseTrackerFromContext(ctx) != nil
	}
	if e.executeStart != nil {
		e.executeStart <- auth.ID
	}
	if e.executeRelease != nil {
		select {
		case <-ctx.Done():
			return cliproxyexecutor.Response{}, ctx.Err()
		case <-e.executeRelease:
		}
	}
	return cliproxyexecutor.Response{Payload: []byte(`{"ok":true}`)}, nil
}

func (e *xaiInflightTestExecutor) ExecuteStream(ctx context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.record(auth.ID)
	if e.phaseTracking != nil {
		e.phaseTracking <- coreusage.PhaseTrackerFromContext(ctx) != nil
	}
	chunks := make(chan cliproxyexecutor.StreamChunk, 1)
	if e.streamError != nil {
		chunks <- cliproxyexecutor.StreamChunk{Err: e.streamError}
		close(chunks)
		return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
	}
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte(`{"type":"response.output_text.delta","delta":"ok"}`)}
	go func() {
		defer close(chunks)
		if e.streamRelease == nil {
			return
		}
		select {
		case <-ctx.Done():
		case <-e.streamRelease:
		}
	}()
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

type xaiPrepareFailureExecutor struct {
	*xaiInflightTestExecutor
	err error
}

func (e *xaiPrepareFailureExecutor) ShouldPrepareRequestAuth(*Auth) bool { return true }

func (e *xaiPrepareFailureExecutor) PrepareRequestAuth(context.Context, *Auth) (*Auth, error) {
	return nil, e.err
}

func (e *xaiInflightTestExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (e *xaiInflightTestExecutor) CountTokens(ctx context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if e.countStart != nil {
		e.countStart <- auth.ID
	}
	if e.countRelease != nil {
		select {
		case <-ctx.Done():
			return cliproxyexecutor.Response{}, ctx.Err()
		case <-e.countRelease:
		}
	}
	return cliproxyexecutor.Response{}, nil
}

func (e *xaiInflightTestExecutor) HttpRequest(_ context.Context, _ *Auth, _ *http.Request) (*http.Response, error) {
	return nil, nil
}

func newXAIInflightTestManager(t *testing.T, selector Selector, executor ProviderExecutor, model string, auths ...*Auth) *Manager {
	t.Helper()
	ids := make([]string, 0, len(auths))
	for _, auth := range auths {
		ids = append(ids, auth.ID)
	}
	registerSchedulerModels(t, "xai", model, ids...)
	manager := NewManager(nil, selector, nil)
	manager.RegisterExecutor(executor)
	for _, auth := range auths {
		if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("Register(%s) error = %v", auth.ID, errRegister)
		}
	}
	return manager
}

func TestManagerExecuteXAILeastInflightDistributesBusyRequests(t *testing.T) {
	model := "grok-inflight-distribution"
	release := make(chan struct{})
	executor := &xaiInflightTestExecutor{executeStart: make(chan string, 2), executeRelease: release}
	manager := newXAIInflightTestManager(t, &RoundRobinSelector{}, executor, model,
		&Auth{ID: "xai-a", Provider: "xai"},
		&Auth{ID: "xai-b", Provider: "xai"},
	)

	errCh := make(chan error, 2)
	for range 2 {
		go func() {
			_, errExec := manager.Execute(context.Background(), []string{"xai"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
			errCh <- errExec
		}()
		<-executor.executeStart
	}

	executor.mu.Lock()
	selected := append([]string(nil), executor.selected...)
	executor.mu.Unlock()
	if len(selected) != 2 || selected[0] == selected[1] {
		t.Fatalf("busy selections = %v, want two different auths", selected)
	}
	close(release)
	for range 2 {
		if errExec := <-errCh; errExec != nil {
			t.Fatalf("Execute() error = %v", errExec)
		}
	}
}

func TestManagerEnablesPhasesOnlyForXAIResponsesExecution(t *testing.T) {
	model := "grok-manager-phase-tracking"
	executor := &xaiInflightTestExecutor{phaseTracking: make(chan bool, 3)}
	manager := newXAIInflightTestManager(t, &RoundRobinSelector{}, executor, model, &Auth{ID: "xai-a", Provider: "xai"})
	request := cliproxyexecutor.Request{Model: model}

	if _, errExec := manager.Execute(context.Background(), []string{"xai"}, request, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}); errExec != nil {
		t.Fatalf("Execute() error = %v", errExec)
	}
	if tracked := <-executor.phaseTracking; !tracked {
		t.Fatal("xAI Responses Execute() did not enable phase tracking")
	}

	stream, errStream := manager.ExecuteStream(context.Background(), []string{"xai"}, request, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if errStream != nil {
		t.Fatalf("ExecuteStream() error = %v", errStream)
	}
	for range stream.Chunks {
	}
	if tracked := <-executor.phaseTracking; !tracked {
		t.Fatal("xAI Responses ExecuteStream() did not enable phase tracking")
	}

	if _, errExec := manager.Execute(context.Background(), []string{"xai"}, request, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAI}); errExec != nil {
		t.Fatalf("non-Responses Execute() error = %v", errExec)
	}
	if tracked := <-executor.phaseTracking; tracked {
		t.Fatal("non-Responses Execute() unexpectedly enabled phase tracking")
	}
	if shouldEnableXAIResponsePhases([]string{"gemini"}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}) {
		t.Fatal("non-xAI provider unexpectedly enabled phase tracking")
	}
}

func TestManagerXAIExecutionSelectionCapturesAffinityOutcome(t *testing.T) {
	model := "grok-manager-affinity-phases"
	selector := NewSessionAffinitySelector(&RoundRobinSelector{})
	t.Cleanup(selector.Stop)
	manager := newXAIInflightTestManager(t, selector, &xaiInflightTestExecutor{}, model,
		&Auth{ID: "xai-a", Provider: "xai"},
		&Auth{ID: "xai-b", Provider: "xai"},
	)
	ctx := coreusage.EnablePhases(xaiAffinityTestContext("phase-caller"))
	opts := cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FormatOpenAIResponse,
		OriginalRequest: []byte(`{"prompt_cache_key":"phase-session"}`),
	}

	_, _, _, lease, errPick := manager.pickNextMixedForExecution(ctx, []string{"xai"}, model, opts, nil, map[string]struct{}{})
	if errPick != nil {
		t.Fatalf("first pickNextMixedForExecution() error = %v", errPick)
	}
	lease.Release()
	first := coreusage.PhaseTrackerFromContext(ctx).BeginAttempt()
	if first.AuthSelection <= 0 || first.AffinityOutcome != coreusage.AffinityOutcomeMiss {
		t.Fatalf("first selection seed = %+v, want positive auth duration and miss", first)
	}

	_, _, _, lease, errPick = manager.pickNextMixedForExecution(ctx, []string{"xai"}, model, opts, nil, map[string]struct{}{})
	if errPick != nil {
		t.Fatalf("second pickNextMixedForExecution() error = %v", errPick)
	}
	lease.Release()
	second := coreusage.PhaseTrackerFromContext(ctx).BeginAttempt()
	if second.AuthSelection <= 0 || second.AffinityOutcome != coreusage.AffinityOutcomeHit {
		t.Fatalf("second selection seed = %+v, want positive auth duration and hit", second)
	}
}

func TestManagerSingleXAICustomAndPluginSchedulersRemainMixed(t *testing.T) {
	model := "grok-custom-mixed"
	executor := &xaiInflightTestExecutor{}
	custom := &providerCaptureSelector{}
	manager := newXAIInflightTestManager(t, custom, executor, model, &Auth{ID: "xai-a", Provider: "xai"})
	if _, errExec := manager.Execute(context.Background(), []string{"xai"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{}); errExec != nil {
		t.Fatalf("custom Execute() error = %v", errExec)
	}
	if custom.provider != "mixed" {
		t.Fatalf("custom selector provider = %q, want mixed", custom.provider)
	}

	plugin := &fakePluginScheduler{resp: pluginapi.SchedulerPickResponse{Handled: true, AuthID: "xai-a"}, handled: true}
	manager.SetPluginScheduler(plugin)
	if _, errExec := manager.Execute(context.Background(), []string{"xai"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{}); errExec != nil {
		t.Fatalf("plugin Execute() error = %v", errExec)
	}
	if len(plugin.requests) != 1 || plugin.requests[0].Provider != "" || len(plugin.requests[0].Providers) != 1 || plugin.requests[0].Providers[0] != "xai" {
		t.Fatalf("plugin scheduler request = %#v, want mixed request with xai provider list", plugin.requests)
	}
}

func TestManagerExecuteXAIHigherPriorityWinsOverInflight(t *testing.T) {
	model := "grok-inflight-priority"
	release := make(chan struct{})
	executor := &xaiInflightTestExecutor{executeStart: make(chan string, 2), executeRelease: release}
	manager := newXAIInflightTestManager(t, &RoundRobinSelector{}, executor, model,
		&Auth{ID: "xai-high", Provider: "xai", Attributes: map[string]string{"priority": "10"}},
		&Auth{ID: "xai-low", Provider: "xai", Attributes: map[string]string{"priority": "0"}},
	)

	errCh := make(chan error, 2)
	for range 2 {
		go func() {
			_, errExec := manager.Execute(context.Background(), []string{"xai"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
			errCh <- errExec
		}()
		if selected := <-executor.executeStart; selected != "xai-high" {
			t.Fatalf("selected auth = %q, want xai-high", selected)
		}
	}
	close(release)
	for range 2 {
		if errExec := <-errCh; errExec != nil {
			t.Fatalf("Execute() error = %v", errExec)
		}
	}
}

func TestManagerXAIFillFirstBreaksOnlyInflightTies(t *testing.T) {
	model := "grok-inflight-fill-first"
	executor := &xaiInflightTestExecutor{}
	manager := newXAIInflightTestManager(t, &FillFirstSelector{}, executor, model,
		&Auth{ID: "xai-a", Provider: "xai"},
		&Auth{ID: "xai-b", Provider: "xai"},
	)
	first, _, _, firstLease, errPick := manager.pickNextMixedForExecution(context.Background(), []string{"xai"}, model, cliproxyexecutor.Options{}, nil, map[string]struct{}{})
	if errPick != nil {
		t.Fatalf("first fill-first pick error = %v", errPick)
	}
	second, _, _, secondLease, errPick := manager.pickNextMixedForExecution(context.Background(), []string{"xai"}, model, cliproxyexecutor.Options{}, nil, map[string]struct{}{})
	if errPick != nil {
		t.Fatalf("second fill-first pick error = %v", errPick)
	}
	if first.ID != "xai-a" || second.ID != "xai-b" {
		t.Fatalf("busy fill-first picks = (%s, %s), want (xai-a, xai-b)", first.ID, second.ID)
	}
	firstLease.Release()
	secondLease.Release()
	third, _, _, thirdLease, errPick := manager.pickNextMixedForExecution(context.Background(), []string{"xai"}, model, cliproxyexecutor.Options{}, nil, map[string]struct{}{})
	if errPick != nil {
		t.Fatalf("third fill-first pick error = %v", errPick)
	}
	defer thirdLease.Release()
	if third.ID != "xai-a" {
		t.Fatalf("idle fill-first pick = %s, want xai-a", third.ID)
	}
}

func TestManagerExecuteCountXAIDoesNotAcquireLease(t *testing.T) {
	model := "grok-count-no-lease"
	release := make(chan struct{})
	executor := &xaiInflightTestExecutor{countStart: make(chan string, 1), countRelease: release}
	manager := newXAIInflightTestManager(t, &RoundRobinSelector{}, executor, model, &Auth{ID: "xai-a", Provider: "xai"})
	errCh := make(chan error, 1)
	go func() {
		_, errCount := manager.ExecuteCount(context.Background(), []string{"xai"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		errCh <- errCount
	}()
	if selected := <-executor.countStart; selected != "xai-a" {
		t.Fatalf("CountTokens selected %q, want xai-a", selected)
	}
	if got := manager.xaiInflight.count("xai-a"); got != 0 {
		t.Fatalf("inflight during CountTokens = %d, want 0", got)
	}
	close(release)
	if errCount := <-errCh; errCount != nil {
		t.Fatalf("ExecuteCount() error = %v", errCount)
	}
}

func TestManagerExecuteXAIPrepareFailureReleasesLease(t *testing.T) {
	model := "grok-prepare-failure-lease"
	executor := &xaiPrepareFailureExecutor{xaiInflightTestExecutor: &xaiInflightTestExecutor{}, err: errors.New("prepare failed")}
	manager := newXAIInflightTestManager(t, &RoundRobinSelector{}, executor, model, &Auth{ID: "xai-a", Provider: "xai"})
	if _, errExec := manager.Execute(context.Background(), []string{"xai"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{}); errExec == nil {
		t.Fatal("Execute() error = nil, want prepare failure")
	}
	if got := manager.xaiInflight.count("xai-a"); got != 0 {
		t.Fatalf("inflight after prepare failure = %d, want 0", got)
	}
}

func TestManagerExecuteXAIPromptCacheAffinityStaysStickyWhenBusy(t *testing.T) {
	model := "grok-affinity-busy"
	release := make(chan struct{})
	executor := &xaiInflightTestExecutor{executeStart: make(chan string, 2), executeRelease: release}
	selector := NewSessionAffinitySelector(&RoundRobinSelector{})
	t.Cleanup(selector.Stop)
	manager := newXAIInflightTestManager(t, selector, executor, model,
		&Auth{ID: "xai-a", Provider: "xai"},
		&Auth{ID: "xai-b", Provider: "xai"},
	)
	ctx := xaiAffinityTestContext("caller-affinity")
	opts := cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FormatOpenAIResponse,
		OriginalRequest: []byte(`{"prompt_cache_key":"same-session"}`),
	}

	errCh := make(chan error, 2)
	for range 2 {
		go func() {
			_, errExec := manager.Execute(ctx, []string{"xai"}, cliproxyexecutor.Request{Model: model}, opts)
			errCh <- errExec
		}()
	}
	first := <-executor.executeStart
	second := <-executor.executeStart
	if first != second {
		t.Fatalf("same prompt_cache_key selected %q then %q, want sticky auth", first, second)
	}
	if stats := selector.stats(); stats.Hits == 0 {
		t.Fatalf("affinity stats = %+v, want at least one hit", stats)
	}
	close(release)
	for range 2 {
		if errExec := <-errCh; errExec != nil {
			t.Fatalf("Execute() error = %v", errExec)
		}
	}
}

func TestManagerExecuteXAIAffinityMissAndFailoverPreferWebsocket(t *testing.T) {
	model := "grok-affinity-websocket"
	executor := &xaiInflightTestExecutor{}
	selector := NewSessionAffinitySelector(&RoundRobinSelector{})
	t.Cleanup(selector.Stop)
	manager := newXAIInflightTestManager(t, selector, executor, model,
		&Auth{ID: "xai-a-http", Provider: "xai"},
		&Auth{ID: "xai-b-ws", Provider: "xai", Attributes: map[string]string{"websockets": "true"}},
	)
	ctx := cliproxyexecutor.WithDownstreamWebsocket(xaiAffinityTestContext("caller-websocket"))
	opts := cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FormatOpenAIResponse,
		OriginalRequest: []byte(`{"prompt_cache_key":"websocket-session"}`),
	}
	if _, errExec := manager.Execute(ctx, []string{"xai"}, cliproxyexecutor.Request{Model: model}, opts); errExec != nil {
		t.Fatalf("first Execute() error = %v", errExec)
	}

	executor.mu.Lock()
	first := executor.selected[len(executor.selected)-1]
	executor.mu.Unlock()
	if first != "xai-b-ws" {
		t.Fatalf("affinity miss selected %q, want websocket auth", first)
	}

	retryAfter := time.Hour
	manager.MarkResult(ctx, Result{AuthID: "xai-b-ws", Provider: "xai", Model: model, Success: false, RetryAfter: &retryAfter, Error: &Error{Code: "quota", Message: "quota", HTTPStatus: http.StatusTooManyRequests}})
	if _, errExec := manager.Execute(ctx, []string{"xai"}, cliproxyexecutor.Request{Model: model}, opts); errExec != nil {
		t.Fatalf("failover Execute() error = %v", errExec)
	}
	executor.mu.Lock()
	second := executor.selected[len(executor.selected)-1]
	executor.mu.Unlock()
	if second != "xai-a-http" {
		t.Fatalf("affinity failover selected %q, want xai-a-http", second)
	}
	if stats := selector.stats(); stats.Failovers != 1 {
		t.Fatalf("affinity stats = %+v, want one failover", stats)
	}
}

func TestManagerExecuteStreamXAILeaseLivesUntilStreamEnds(t *testing.T) {
	model := "grok-stream-lease"
	release := make(chan struct{})
	executor := &xaiInflightTestExecutor{streamRelease: release}
	manager := newXAIInflightTestManager(t, &RoundRobinSelector{}, executor, model, &Auth{ID: "xai-a", Provider: "xai"})

	result, errStream := manager.ExecuteStream(context.Background(), []string{"xai"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
	if errStream != nil {
		t.Fatalf("ExecuteStream() error = %v", errStream)
	}
	if got := manager.xaiInflight.count("xai-a"); got != 1 {
		t.Fatalf("inflight during stream = %d, want 1", got)
	}
	if chunk := <-result.Chunks; chunk.Err != nil || len(chunk.Payload) == 0 {
		t.Fatalf("first stream chunk = %#v", chunk)
	}
	close(release)
	for range result.Chunks {
	}
	if got := manager.xaiInflight.count("xai-a"); got != 0 {
		t.Fatalf("inflight after stream = %d, want 0", got)
	}
}

func TestManagerExecuteStreamXAILeaseReleasesOnCancel(t *testing.T) {
	model := "grok-stream-cancel-lease"
	release := make(chan struct{})
	executor := &xaiInflightTestExecutor{streamRelease: release}
	manager := newXAIInflightTestManager(t, &RoundRobinSelector{}, executor, model, &Auth{ID: "xai-a", Provider: "xai"})
	ctx, cancel := context.WithCancel(context.Background())

	result, errStream := manager.ExecuteStream(ctx, []string{"xai"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
	if errStream != nil {
		t.Fatalf("ExecuteStream() error = %v", errStream)
	}
	if got := manager.xaiInflight.count("xai-a"); got != 1 {
		t.Fatalf("inflight during stream = %d, want 1", got)
	}
	cancel()
	for range result.Chunks {
	}
	if got := manager.xaiInflight.count("xai-a"); got != 0 {
		t.Fatalf("inflight after cancel = %d, want 0", got)
	}
}

func TestManagerExecuteStreamXAIBootstrapErrorReleasesLease(t *testing.T) {
	model := "grok-stream-bootstrap-error-lease"
	executor := &xaiInflightTestExecutor{streamError: errors.New("bootstrap failed")}
	manager := newXAIInflightTestManager(t, &RoundRobinSelector{}, executor, model, &Auth{ID: "xai-a", Provider: "xai"})

	result, errStream := manager.ExecuteStream(context.Background(), []string{"xai"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
	if errStream != nil {
		t.Fatalf("ExecuteStream() error = %v", errStream)
	}
	chunk, ok := <-result.Chunks
	if !ok || chunk.Err == nil {
		t.Fatalf("bootstrap stream chunk = %#v, %v; want error", chunk, ok)
	}
	if got := manager.xaiInflight.count("xai-a"); got != 0 {
		t.Fatalf("inflight after bootstrap error = %d, want 0", got)
	}
}

func TestManagerXAIContinuityPreferredAuthAndCooldownFallback(t *testing.T) {
	internalcache.ClearXAIResponseContinuityCache()
	t.Cleanup(internalcache.ClearXAIResponseContinuityCache)
	model := "grok-response-continuity"
	executor := &xaiInflightTestExecutor{}
	manager := newXAIInflightTestManager(t, &RoundRobinSelector{}, executor, model,
		&Auth{ID: "xai-a", Provider: "xai"},
		&Auth{ID: "xai-b", Provider: "xai"},
	)
	ctx := xaiAffinityTestContext("continuity-caller")
	scope := internalcache.XAIResponseContinuityCallerScope("continuity-caller", "")
	if !internalcache.StoreXAIResponseContinuity(ctx, scope, "xai", model, "resp-a", internalcache.XAIResponseContinuity{AuthID: "xai-a", UpstreamKind: "http:official"}) {
		t.Fatal("StoreXAIResponseContinuity() = false")
	}
	opts := cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FormatOpenAIResponse,
		OriginalRequest: []byte(`{"previous_response_id":"resp-a"}`),
	}

	first, _, _, firstLease, errPick := manager.pickNextMixedForExecution(ctx, []string{"xai"}, model, opts, nil, map[string]struct{}{})
	if errPick != nil {
		t.Fatalf("first continuity pick error = %v", errPick)
	}
	second, _, _, secondLease, errPick := manager.pickNextMixedForExecution(ctx, []string{"xai"}, model, opts, nil, map[string]struct{}{})
	if errPick != nil {
		t.Fatalf("second continuity pick error = %v", errPick)
	}
	if first.ID != "xai-a" || second.ID != "xai-a" {
		t.Fatalf("continuity picks = (%s, %s), want xai-a despite inflight", first.ID, second.ID)
	}
	firstLease.Release()
	secondLease.Release()

	retryAfter := time.Hour
	manager.MarkResult(ctx, Result{AuthID: "xai-a", Provider: "xai", Model: model, Success: false, RetryAfter: &retryAfter, Error: &Error{Code: "quota", Message: "quota", HTTPStatus: http.StatusTooManyRequests}})
	fallback, _, _, fallbackLease, errPick := manager.pickNextMixedForExecution(ctx, []string{"xai"}, model, opts, nil, map[string]struct{}{})
	if errPick != nil {
		t.Fatalf("cooldown continuity pick error = %v", errPick)
	}
	defer fallbackLease.Release()
	if fallback.ID != "xai-b" {
		t.Fatalf("cooldown continuity pick = %s, want xai-b", fallback.ID)
	}
}

func TestManagerXAIContinuityDoesNotOverrideHigherPriority(t *testing.T) {
	internalcache.ClearXAIResponseContinuityCache()
	t.Cleanup(internalcache.ClearXAIResponseContinuityCache)
	model := "grok-response-continuity-priority"
	executor := &xaiInflightTestExecutor{}
	manager := newXAIInflightTestManager(t, &RoundRobinSelector{}, executor, model,
		&Auth{ID: "xai-low", Provider: "xai", Attributes: map[string]string{"priority": "0"}},
		&Auth{ID: "xai-high", Provider: "xai", Attributes: map[string]string{"priority": "10"}},
	)
	ctx := xaiAffinityTestContext("continuity-priority-caller")
	scope := internalcache.XAIResponseContinuityCallerScope("continuity-priority-caller", "")
	if !internalcache.StoreXAIResponseContinuity(ctx, scope, "xai", model, "resp-low", internalcache.XAIResponseContinuity{AuthID: "xai-low", UpstreamKind: "http:official"}) {
		t.Fatal("StoreXAIResponseContinuity() = false")
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, OriginalRequest: []byte(`{"previous_response_id":"resp-low"}`)}
	selected, _, _, lease, errPick := manager.pickNextMixedForExecution(ctx, []string{"xai"}, model, opts, nil, map[string]struct{}{})
	if errPick != nil {
		t.Fatalf("continuity priority pick error = %v", errPick)
	}
	defer lease.Release()
	if selected.ID != "xai-high" {
		t.Fatalf("continuity priority pick = %s, want xai-high", selected.ID)
	}
}

func TestManagerXAISessionBindingPreferredAuthAfterPreviousLookupMiss(t *testing.T) {
	internalcache.ClearXAIResponseContinuityCache()
	t.Cleanup(internalcache.ClearXAIResponseContinuityCache)
	model := "grok-session-binding-continuity"
	manager := newXAIInflightTestManager(t, &RoundRobinSelector{}, &xaiInflightTestExecutor{}, model,
		&Auth{ID: "xai-a", Provider: "xai"},
		&Auth{ID: "xai-b", Provider: "xai"},
	)
	ctx := xaiAffinityTestContext("binding-caller")
	scope := internalcache.XAIResponseContinuityCallerScope("binding-caller", "")
	bindingID := internalcache.XAIResponseContinuitySessionBindingID("binding-pck", "")
	if !internalcache.StoreXAIResponseContinuity(ctx, scope, "xai", model, bindingID, internalcache.XAIResponseContinuity{AuthID: "xai-a", UpstreamKind: "http:official"}) {
		t.Fatal("StoreXAIResponseContinuity() = false")
	}
	opts := cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FormatOpenAIResponse,
		OriginalRequest: []byte(`{"previous_response_id":"resp-cache-miss","prompt_cache_key":"binding-pck"}`),
	}

	first, _, _, firstLease, errPick := manager.pickNextMixedForExecution(ctx, []string{"xai"}, model, opts, nil, map[string]struct{}{})
	if errPick != nil {
		t.Fatalf("first binding pick error = %v", errPick)
	}
	second, _, _, secondLease, errPick := manager.pickNextMixedForExecution(ctx, []string{"xai"}, model, opts, nil, map[string]struct{}{})
	if errPick != nil {
		t.Fatalf("second binding pick error = %v", errPick)
	}
	if first.ID != "xai-a" || second.ID != "xai-a" {
		t.Fatalf("binding picks = (%s, %s), want xai-a despite inflight", first.ID, second.ID)
	}
	firstLease.Release()
	secondLease.Release()

	fallback, _, _, fallbackLease, errPick := manager.pickNextMixedForExecution(ctx, []string{"xai"}, model, opts, nil, map[string]struct{}{"xai-a": {}})
	if errPick != nil {
		t.Fatalf("tried binding fallback error = %v", errPick)
	}
	defer fallbackLease.Release()
	if fallback.ID != "xai-b" {
		t.Fatalf("tried binding fallback = %s, want xai-b", fallback.ID)
	}
}

func TestManagerXAISessionBindingDoesNotOverridePriorityOrWebsocketPreference(t *testing.T) {
	internalcache.ClearXAIResponseContinuityCache()
	t.Cleanup(internalcache.ClearXAIResponseContinuityCache)

	t.Run("priority", func(t *testing.T) {
		model := "grok-session-binding-priority"
		manager := newXAIInflightTestManager(t, &RoundRobinSelector{}, &xaiInflightTestExecutor{}, model,
			&Auth{ID: "xai-low", Provider: "xai", Attributes: map[string]string{"priority": "0"}},
			&Auth{ID: "xai-high", Provider: "xai", Attributes: map[string]string{"priority": "10"}},
		)
		ctx := xaiAffinityTestContext("binding-priority-caller")
		scope := internalcache.XAIResponseContinuityCallerScope("binding-priority-caller", "")
		bindingID := internalcache.XAIResponseContinuitySessionBindingID("priority-pck", "")
		if !internalcache.StoreXAIResponseContinuity(ctx, scope, "xai", model, bindingID, internalcache.XAIResponseContinuity{AuthID: "xai-low", UpstreamKind: "http:official"}) {
			t.Fatal("StoreXAIResponseContinuity() = false")
		}
		opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, OriginalRequest: []byte(`{"prompt_cache_key":"priority-pck"}`)}
		selected, _, _, lease, errPick := manager.pickNextMixedForExecution(ctx, []string{"xai"}, model, opts, nil, map[string]struct{}{})
		if errPick != nil {
			t.Fatalf("priority binding pick error = %v", errPick)
		}
		defer lease.Release()
		if selected.ID != "xai-high" {
			t.Fatalf("priority binding pick = %s, want xai-high", selected.ID)
		}
	})

	t.Run("websocket", func(t *testing.T) {
		model := "grok-session-binding-websocket"
		manager := newXAIInflightTestManager(t, &RoundRobinSelector{}, &xaiInflightTestExecutor{}, model,
			&Auth{ID: "xai-http", Provider: "xai"},
			&Auth{ID: "xai-ws", Provider: "xai", Attributes: map[string]string{"websockets": "true"}},
		)
		baseCtx := xaiAffinityTestContext("binding-websocket-caller")
		scope := internalcache.XAIResponseContinuityCallerScope("binding-websocket-caller", "")
		bindingID := internalcache.XAIResponseContinuitySessionBindingID("websocket-pck", "")
		if !internalcache.StoreXAIResponseContinuity(baseCtx, scope, "xai", model, bindingID, internalcache.XAIResponseContinuity{AuthID: "xai-http", UpstreamKind: "http:official"}) {
			t.Fatal("StoreXAIResponseContinuity() = false")
		}
		ctx := cliproxyexecutor.WithDownstreamWebsocket(baseCtx)
		opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, OriginalRequest: []byte(`{"prompt_cache_key":"websocket-pck"}`)}
		selected, _, _, lease, errPick := manager.pickNextMixedForExecution(ctx, []string{"xai"}, model, opts, nil, map[string]struct{}{})
		if errPick != nil {
			t.Fatalf("websocket binding pick error = %v", errPick)
		}
		defer lease.Release()
		if selected.ID != "xai-ws" {
			t.Fatalf("websocket binding pick = %s, want xai-ws", selected.ID)
		}
	})
}

func TestXAIContinuityPreferredAuthUsesTrustedExecutionBindingAndFailsClosedOnCacheError(t *testing.T) {
	internalcache.ClearXAIResponseContinuityCache()
	t.Cleanup(internalcache.ClearXAIResponseContinuityCache)
	model := "grok-execution-binding"
	executionSessionID := "trusted-execution"
	scope := internalcache.XAIResponseContinuityCallerScope("", executionSessionID)
	bindingID := internalcache.XAIResponseContinuitySessionBindingID("", executionSessionID)
	if !internalcache.StoreXAIResponseContinuity(context.Background(), scope, "xai", model, bindingID, internalcache.XAIResponseContinuity{AuthID: "xai-a", UpstreamKind: "http:official"}) {
		t.Fatal("StoreXAIResponseContinuity() = false")
	}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: executionSessionID,
		},
	}
	if got := xaiContinuityPreferredAuth(context.Background(), model, opts, nil); got != "xai-a" {
		t.Fatalf("execution binding preferred auth = %q, want xai-a", got)
	}

	previousGetter := getXAIResponseContinuityForRouting
	getXAIResponseContinuityForRouting = func(context.Context, string, string, string, string) (internalcache.XAIResponseContinuity, bool, error) {
		return internalcache.XAIResponseContinuity{}, false, errors.New("kv unavailable")
	}
	t.Cleanup(func() { getXAIResponseContinuityForRouting = previousGetter })
	if got := xaiContinuityPreferredAuth(context.Background(), model, opts, nil); got != "" {
		t.Fatalf("cache error preferred auth = %q, want fail closed", got)
	}
}
