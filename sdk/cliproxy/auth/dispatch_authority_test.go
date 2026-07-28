package auth

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type dispatchAuthorityStub struct {
	mu        sync.Mutex
	decisions []bool
	defaultOK bool
	closed    bool
	admitCh   chan struct{}
	admits    int
	releases  int
	wakes     int
}

func (a *dispatchAuthorityStub) Admit(string) (func(), bool) {
	a.mu.Lock()
	a.admits++
	ok := a.defaultOK && !a.closed
	if len(a.decisions) > 0 {
		ok = a.decisions[0] && !a.closed
		a.decisions = a.decisions[1:]
	}
	admitCh := a.admitCh
	a.mu.Unlock()
	if admitCh != nil {
		select {
		case admitCh <- struct{}{}:
		default:
		}
	}
	if !ok {
		return nil, false
	}
	return func() {
		a.mu.Lock()
		a.releases++
		a.mu.Unlock()
	}, true
}

func (a *dispatchAuthorityStub) Wake() {
	a.mu.Lock()
	a.wakes++
	a.mu.Unlock()
}

func (*dispatchAuthorityStub) Ready() bool                     { return true }
func (*dispatchAuthorityStub) WaitReady(context.Context) error { return nil }
func (a *dispatchAuthorityStub) CloseAdmissions() {
	a.mu.Lock()
	a.closed = true
	a.mu.Unlock()
}
func (a *dispatchAuthorityStub) counts() (int, int, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.admits, a.releases, a.wakes
}

type dispatchHook struct{ results atomic.Int64 }

func (*dispatchHook) OnAuthRegistered(context.Context, *Auth) {}
func (*dispatchHook) OnAuthUpdated(context.Context, *Auth)    {}
func (h *dispatchHook) OnResult(context.Context, Result)      { h.results.Add(1) }

type dispatchExecutor struct {
	mu sync.Mutex

	executeCalls int
	countCalls   int
	streamCalls  int
	httpCalls    int
	refreshCalls int

	executeErrors []error
	countErrors   []error
	streamStarts  []func() (*cliproxyexecutor.StreamResult, error)
}

func (*dispatchExecutor) Identifier() string { return "dispatch-test" }

func (e *dispatchExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.executeCalls++
	if len(e.executeErrors) > 0 {
		err := e.executeErrors[0]
		e.executeErrors = e.executeErrors[1:]
		return cliproxyexecutor.Response{}, err
	}
	return cliproxyexecutor.Response{Payload: []byte("ok")}, nil
}

func (e *dispatchExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.mu.Lock()
	e.streamCalls++
	var start func() (*cliproxyexecutor.StreamResult, error)
	if len(e.streamStarts) > 0 {
		start = e.streamStarts[0]
		e.streamStarts = e.streamStarts[1:]
	}
	e.mu.Unlock()
	if start != nil {
		return start()
	}
	chunks := make(chan cliproxyexecutor.StreamChunk, 1)
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("ok")}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *dispatchExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	e.mu.Lock()
	e.refreshCalls++
	e.mu.Unlock()
	updated := auth.Clone()
	updated.Metadata["access_token"] = "fresh"
	return updated, nil
}

func (e *dispatchExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.countCalls++
	if len(e.countErrors) > 0 {
		err := e.countErrors[0]
		e.countErrors = e.countErrors[1:]
		return cliproxyexecutor.Response{}, err
	}
	return cliproxyexecutor.Response{Payload: []byte("1")}, nil
}

func (e *dispatchExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	e.mu.Lock()
	e.httpCalls++
	e.mu.Unlock()
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
}

func (e *dispatchExecutor) calls() (int, int, int, int, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.executeCalls, e.countCalls, e.streamCalls, e.httpCalls, e.refreshCalls
}

func newDispatchFixture(t *testing.T, executor *dispatchExecutor) (*Manager, *Auth, *dispatchHook, string) {
	t.Helper()
	const model = "dispatch-model"
	auth := &Auth{
		ID:       "dispatch-auth",
		Provider: "dispatch-test",
		Metadata: map[string]any{"access_token": "stale", "refresh_token": "refresh"},
	}
	hook := &dispatchHook{}
	manager := NewManager(nil, nil, hook)
	manager.RegisterExecutor(executor)
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	return manager, auth, hook, model
}

func TestDispatchAuthorityRejectionHasNoExecutionOrResultSideEffects(t *testing.T) {
	executor := &dispatchExecutor{}
	manager, auth, hook, model := newDispatchFixture(t, executor)
	authority := &dispatchAuthorityStub{}
	manager.SetDispatchAuthority(authority)

	before, _ := manager.GetByID(auth.ID)
	_, errExecute := manager.Execute(context.Background(), []string{auth.Provider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if !errors.Is(errExecute, ErrDispatchAdmissionRejected) {
		t.Fatalf("Execute() error = %v, want ErrDispatchAdmissionRejected", errExecute)
	}
	var authErr *Error
	if !errors.As(errExecute, &authErr) || authErr.Code != "auth_unavailable" || authErr.HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("Execute() admission error = %#v, want auth_unavailable/503", errExecute)
	}
	after, _ := manager.GetByID(auth.ID)
	if executeCalls, _, _, _, _ := executor.calls(); executeCalls != 0 {
		t.Fatalf("executor calls = %d, want 0", executeCalls)
	}
	if hook.results.Load() != 0 {
		t.Fatalf("result hook calls = %d, want 0", hook.results.Load())
	}
	if after.Success != before.Success || after.Failed != before.Failed || after.Status != before.Status || after.StatusMessage != before.StatusMessage || len(after.ModelStates) != len(before.ModelStates) {
		t.Fatalf("auth runtime changed after admission rejection: before=%+v after=%+v", before, after)
	}
}

func TestDispatchAuthorityRechecksUnauthorizedRetries(t *testing.T) {
	unauthorized := &Error{HTTPStatus: http.StatusUnauthorized, Message: "unauthorized"}
	tests := []struct {
		name     string
		prepare  func(*dispatchExecutor)
		execute  func(*Manager, *Auth, string) error
		wantCall func(*dispatchExecutor) int
	}{
		{
			name:    "execute",
			prepare: func(executor *dispatchExecutor) { executor.executeErrors = []error{unauthorized} },
			execute: func(manager *Manager, auth *Auth, model string) error {
				_, err := manager.Execute(context.Background(), []string{auth.Provider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				return err
			},
			wantCall: func(executor *dispatchExecutor) int { calls, _, _, _, _ := executor.calls(); return calls },
		},
		{
			name:    "count tokens",
			prepare: func(executor *dispatchExecutor) { executor.countErrors = []error{unauthorized} },
			execute: func(manager *Manager, auth *Auth, model string) error {
				_, err := manager.ExecuteCount(context.Background(), []string{auth.Provider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				return err
			},
			wantCall: func(executor *dispatchExecutor) int { _, calls, _, _, _ := executor.calls(); return calls },
		},
		{
			name: "stream immediate",
			prepare: func(executor *dispatchExecutor) {
				executor.streamStarts = []func() (*cliproxyexecutor.StreamResult, error){func() (*cliproxyexecutor.StreamResult, error) { return nil, unauthorized }}
			},
			execute: func(manager *Manager, auth *Auth, model string) error {
				_, err := manager.ExecuteStream(context.Background(), []string{auth.Provider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				return err
			},
			wantCall: func(executor *dispatchExecutor) int { _, _, calls, _, _ := executor.calls(); return calls },
		},
		{
			name: "stream bootstrap",
			prepare: func(executor *dispatchExecutor) {
				executor.streamStarts = []func() (*cliproxyexecutor.StreamResult, error){func() (*cliproxyexecutor.StreamResult, error) {
					chunks := make(chan cliproxyexecutor.StreamChunk, 1)
					chunks <- cliproxyexecutor.StreamChunk{Err: unauthorized}
					close(chunks)
					return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
				}}
			},
			execute: func(manager *Manager, auth *Auth, model string) error {
				_, err := manager.ExecuteStream(context.Background(), []string{auth.Provider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				return err
			},
			wantCall: func(executor *dispatchExecutor) int { _, _, calls, _, _ := executor.calls(); return calls },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := &dispatchExecutor{}
			test.prepare(executor)
			manager, auth, hook, model := newDispatchFixture(t, executor)
			authority := &dispatchAuthorityStub{decisions: []bool{true, false}}
			manager.SetDispatchAuthority(authority)

			errExecute := test.execute(manager, auth, model)
			if !errors.Is(errExecute, ErrDispatchAdmissionRejected) {
				t.Fatalf("error = %v, want ErrDispatchAdmissionRejected", errExecute)
			}
			if calls := test.wantCall(executor); calls != 1 {
				t.Fatalf("executor calls = %d, want 1", calls)
			}
			if _, _, _, _, refreshCalls := executor.calls(); refreshCalls != 0 {
				t.Fatalf("refresh calls = %d, want 0 when refresh admission is rejected", refreshCalls)
			}
			admits, releases, _ := authority.counts()
			if admits != 2 || releases != 1 {
				t.Fatalf("authority admits/releases = %d/%d, want 2/1", admits, releases)
			}
			if hook.results.Load() != 0 {
				t.Fatalf("result hook calls = %d, want 0", hook.results.Load())
			}
		})
	}
}

func TestDispatchAuthorityRejectsRequestAuthPreparationWithoutSideEffects(t *testing.T) {
	const model = "dispatch-prepare-model"
	store := &requestPrepareStore{}
	executor := &requestPrepareExecutor{}
	hook := &dispatchHook{}
	manager := NewManager(store, nil, hook)
	manager.RegisterExecutor(executor)
	auth := &Auth{
		ID:       "dispatch-prepare-auth",
		Provider: "antigravity",
		Metadata: map[string]any{"access_token": "unchanged"},
	}
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	authority := &dispatchAuthorityStub{}
	manager.SetDispatchAuthority(authority)

	_, errExecute := manager.Execute(context.Background(), []string{auth.Provider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if !errors.Is(errExecute, ErrDispatchAdmissionRejected) {
		t.Fatalf("Execute() error = %v, want ErrDispatchAdmissionRejected", errExecute)
	}
	if got := executor.prepareCalls.Load(); got != 0 {
		t.Fatalf("PrepareRequestAuth() calls = %d, want 0", got)
	}
	if got := executor.executeCalls.Load(); got != 0 {
		t.Fatalf("Execute() calls = %d, want 0", got)
	}
	if got := store.saveCount.Load(); got != 0 {
		t.Fatalf("Store.Save() calls = %d, want 0", got)
	}
	if got := hook.results.Load(); got != 0 {
		t.Fatalf("result hook calls = %d, want 0", got)
	}
	current, _ := manager.GetByID(auth.ID)
	if got := testStringValue(current.Metadata["project_id"]); got != "" {
		t.Fatalf("project_id = %q after rejected preparation, want empty", got)
	}
	if admits, releases, _ := authority.counts(); admits != 1 || releases != 0 {
		t.Fatalf("authority admits/releases = %d/%d, want 1/0", admits, releases)
	}
}

type nestedDispatchAdmissionPreparer struct {
	admitted bool
}

func (*nestedDispatchAdmissionPreparer) ShouldPrepareRequestAuth(*Auth) bool { return true }

func (p *nestedDispatchAdmissionPreparer) PrepareRequestAuth(ctx context.Context, auth *Auth) (*Auth, error) {
	release, ok := cliproxyexecutor.AdmitDispatch(ctx, auth.ID)
	p.admitted = ok
	if release != nil {
		release()
	}
	return auth, nil
}

func TestDispatchAuthorityContextLetsExecutorFenceDetachedWork(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	authority := &dispatchAuthorityStub{decisions: []bool{true, true}}
	manager.SetDispatchAuthority(authority)
	preparer := &nestedDispatchAdmissionPreparer{}
	auth := &Auth{ID: "detached-dispatch-auth"}

	if _, errPrepare := manager.prepareRequestAuthWithDispatchAdmission(context.Background(), preparer, auth); errPrepare != nil {
		t.Fatalf("prepareRequestAuthWithDispatchAdmission() error = %v", errPrepare)
	}
	if !preparer.admitted {
		t.Fatal("executor-owned detached work was not admitted")
	}
	admits, releases, _ := authority.counts()
	if admits != 2 || releases != 2 {
		t.Fatalf("authority admits/releases = %d/%d, want 2/2", admits, releases)
	}
}

func TestDispatchAuthorityClosedStopsAutoRefreshWorkerWithoutSideEffects(t *testing.T) {
	store := &dispatchCountingStore{}
	executor := &dispatchExecutor{}
	hook := &dispatchHook{}
	manager := NewManager(store, nil, hook)
	manager.RegisterExecutor(executor)
	auth := &Auth{
		ID:       "dispatch-auto-refresh-auth",
		Provider: "dispatch-test",
		Metadata: map[string]any{"access_token": "stale", "refresh_token": "refresh"},
	}
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	authority := &dispatchAuthorityStub{defaultOK: true, admitCh: make(chan struct{}, 1)}
	manager.SetDispatchAuthority(authority)
	authority.CloseAdmissions()

	ctx, cancel := context.WithCancel(context.Background())
	loop := newAuthAutoRefreshLoop(manager, time.Second, 1)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		loop.worker(ctx)
	}()
	loop.jobs <- auth.ID
	select {
	case <-authority.admitCh:
	case <-time.After(time.Second):
		cancel()
		<-workerDone
		t.Fatal("auto-refresh worker did not reach dispatch admission")
	}
	cancel()
	<-workerDone

	if _, _, _, _, refreshCalls := executor.calls(); refreshCalls != 0 {
		t.Fatalf("Refresh() calls = %d, want 0 after CloseAdmissions", refreshCalls)
	}
	if calls := store.calls.Load(); calls != 0 {
		t.Fatalf("store calls = %d, want 0 after rejected auto-refresh", calls)
	}
	if got := hook.results.Load(); got != 0 {
		t.Fatalf("result hook calls = %d, want 0", got)
	}
	current, _ := manager.GetByID(auth.ID)
	if got := authAccessToken(current); got != "stale" {
		t.Fatalf("access token = %q, want unchanged stale token", got)
	}
}

func TestDispatchAuthorityRejectsManualRefreshWithoutCredentialMutation(t *testing.T) {
	store := &dispatchCountingStore{}
	executor := &dispatchExecutor{}
	manager := NewManager(store, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &Auth{
		ID:       "dispatch-manual-refresh-auth",
		Provider: "dispatch-test",
		Metadata: map[string]any{"access_token": "stale", "refresh_token": "refresh"},
	}
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	manager.SetDispatchAuthority(&dispatchAuthorityStub{})

	if _, errRefresh := manager.refreshAuthForRequest(context.Background(), auth.ID, ""); !errors.Is(errRefresh, ErrDispatchAdmissionRejected) {
		t.Fatalf("refreshAuthForRequest() error = %v, want ErrDispatchAdmissionRejected", errRefresh)
	}
	if _, _, _, _, refreshCalls := executor.calls(); refreshCalls != 0 {
		t.Fatalf("Refresh() calls = %d, want 0", refreshCalls)
	}
	if calls := store.calls.Load(); calls != 0 {
		t.Fatalf("store calls = %d, want 0", calls)
	}
	current, _ := manager.GetByID(auth.ID)
	if got := authAccessToken(current); got != "stale" {
		t.Fatalf("access token = %q, want unchanged stale token", got)
	}
	if current.LastError != nil || !current.NextRefreshAfter.IsZero() {
		t.Fatalf("refresh rejection changed auth result state: LastError=%v NextRefreshAfter=%v", current.LastError, current.NextRefreshAfter)
	}
}

type dispatchLeaderGate bool

func (g dispatchLeaderGate) IsLeader() bool { return bool(g) }

func TestShouldRefreshLocallyShardingFailsClosedWithoutReadyOwnership(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.SetLeaderGate(dispatchLeaderGate(true))
	manager.SetAuthShardingEnabled(true)

	if manager.ShouldRefreshLocally("auth") {
		t.Fatal("ShouldRefreshLocally() = true without ring while sharding enabled; leader fallback must be disabled")
	}
	manager.SetAuthRing(&stubRing{ready: false, mine: map[string]bool{"auth": true}})
	if manager.ShouldRefreshLocally("auth") {
		t.Fatal("ShouldRefreshLocally() = true with unready ring while sharding enabled")
	}
	manager.SetAuthRing(&stubRing{ready: true, mine: map[string]bool{"auth": false}})
	if manager.ShouldRefreshLocally("auth") {
		t.Fatal("ShouldRefreshLocally() = true for non-owner while sharding enabled")
	}
	manager.SetAuthRing(&stubRing{ready: true, mine: map[string]bool{"auth": true}})
	if !manager.ShouldRefreshLocally("auth") {
		t.Fatal("ShouldRefreshLocally() = false for owner in ready ring")
	}

	manager.SetAuthShardingEnabled(false)
	if !manager.ShouldRefreshLocally("auth") {
		t.Fatal("ShouldRefreshLocally() = false for leader with sharding disabled")
	}
	manager.SetLeaderGate(dispatchLeaderGate(false))
	if manager.ShouldRefreshLocally("auth") {
		t.Fatal("ShouldRefreshLocally() = true for non-leader with sharding disabled")
	}
}

func TestDispatchAuthorityGatesHttpRequest(t *testing.T) {
	executor := &dispatchExecutor{}
	manager, auth, _, _ := newDispatchFixture(t, executor)
	manager.SetDispatchAuthority(&dispatchAuthorityStub{})
	req, errRequest := http.NewRequest(http.MethodGet, "https://example.test", nil)
	if errRequest != nil {
		t.Fatal(errRequest)
	}
	if _, errHTTP := manager.HttpRequest(context.Background(), auth, req); !errors.Is(errHTTP, ErrDispatchAdmissionRejected) {
		t.Fatalf("HttpRequest() error = %v, want ErrDispatchAdmissionRejected", errHTTP)
	}
	if _, _, _, httpCalls, _ := executor.calls(); httpCalls != 0 {
		t.Fatalf("HTTP executor calls = %d, want 0", httpCalls)
	}
}

func TestDispatchAuthorityRechecksAntigravityCreditsFallback(t *testing.T) {
	const (
		upstreamModel = "gemini-3-flash-preview"
		aliasModel    = "claude-haiku-4-5-20251001"
	)
	tests := []struct {
		name    string
		execute func(*Manager) error
		calls   func(*forceMappingCreditsFallbackExecutor) int
	}{
		{
			name: "unary",
			execute: func(manager *Manager) error {
				_, err := manager.Execute(context.Background(), []string{"antigravity"}, cliproxyexecutor.Request{Model: aliasModel}, cliproxyexecutor.Options{})
				return err
			},
			calls: func(executor *forceMappingCreditsFallbackExecutor) int { return len(executor.ExecuteModels()) },
		},
		{
			name: "stream",
			execute: func(manager *Manager) error {
				_, err := manager.ExecuteStream(context.Background(), []string{"antigravity"}, cliproxyexecutor.Request{Model: aliasModel}, cliproxyexecutor.Options{})
				return err
			},
			calls: func(executor *forceMappingCreditsFallbackExecutor) int { return len(executor.StreamModels()) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager, executor := setupForceMappingCreditsFallbackManager(t, upstreamModel, aliasModel)
			authority := &dispatchAuthorityStub{decisions: []bool{true, false}}
			manager.SetDispatchAuthority(authority)

			if errExecute := test.execute(manager); !errors.Is(errExecute, ErrDispatchAdmissionRejected) {
				t.Fatalf("execute error = %v, want ErrDispatchAdmissionRejected", errExecute)
			}
			if calls := test.calls(executor); calls != 1 {
				t.Fatalf("executor calls = %d, want only the initial attempt", calls)
			}
			admits, releases, _ := authority.counts()
			if admits != 2 || releases != 1 {
				t.Fatalf("authority admits/releases = %d/%d, want 2/1", admits, releases)
			}
		})
	}
}

type dispatchCountingStore struct{ calls atomic.Int64 }

func (s *dispatchCountingStore) List(context.Context) ([]*Auth, error) {
	s.calls.Add(1)
	return nil, nil
}
func (s *dispatchCountingStore) Save(context.Context, *Auth) (string, error) {
	s.calls.Add(1)
	return "", nil
}
func (s *dispatchCountingStore) Delete(context.Context, string) error { s.calls.Add(1); return nil }

func TestDispatchAuthorityHotPathDoesNotUseManagerLockOrStore(t *testing.T) {
	store := &dispatchCountingStore{}
	manager := NewManager(store, nil, nil)
	authority := &dispatchAuthorityStub{defaultOK: true}
	manager.SetDispatchAuthority(authority)

	manager.mu.Lock()
	var workers sync.WaitGroup
	workers.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer workers.Done()
			release, errAdmit := manager.admitDispatch("auth")
			if errAdmit == nil && release != nil {
				release()
			}
		}()
	}
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	select {
	case <-done:
		manager.mu.Unlock()
	case <-time.After(2 * time.Second):
		manager.mu.Unlock()
		t.Fatal("admission hot path blocked on Manager.mu")
	}

	admits, releases, _ := authority.counts()
	if admits != 1000 || releases != 1000 {
		t.Fatalf("authority admits/releases = %d/%d, want 1000/1000", admits, releases)
	}
	if calls := store.calls.Load(); calls != 0 {
		t.Fatalf("store calls = %d, want 0", calls)
	}
}

func TestSetDispatchAuthorityTypedNilClearsAndListIsStable(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.auths["b"] = &Auth{ID: "b"}
	manager.auths["a"] = &Auth{ID: "a"}
	manager.homeRuntimeAuths["session"] = map[string]*Auth{"c": {ID: "c"}, "a": {ID: "a"}}

	ids := manager.ListDispatchAuthIDs()
	if len(ids) != 3 || ids[0] != "a" || ids[1] != "b" || ids[2] != "c" {
		t.Fatalf("ListDispatchAuthIDs() = %v, want [a b c]", ids)
	}
	ids[0] = "changed"
	if got := manager.ListDispatchAuthIDs()[0]; got != "a" {
		t.Fatalf("ListDispatchAuthIDs() leaked internal slice, first ID = %q", got)
	}

	authority := &dispatchAuthorityStub{defaultOK: true}
	manager.SetDispatchAuthority(authority)
	var typedNil *dispatchAuthorityStub
	manager.SetDispatchAuthority(typedNil)
	if release, errAdmit := manager.admitDispatch("a"); errAdmit != nil || release != nil {
		t.Fatalf("admit after typed-nil clear returned release=%t error=%v, want compatibility pass-through", release != nil, errAdmit)
	}
}

type tombstonedEnableStore struct {
	authID string
	state  AuthAuthoritativeState
	cause  error
}

func (*tombstonedEnableStore) List(context.Context) ([]*Auth, error)       { return nil, nil }
func (*tombstonedEnableStore) Save(context.Context, *Auth) (string, error) { return "", nil }
func (*tombstonedEnableStore) SaveVersioned(context.Context, *Auth, uint64) (string, uint64, error) {
	return "", 0, nil
}
func (*tombstonedEnableStore) Delete(context.Context, string) error { return nil }

func (s *tombstonedEnableStore) SaveBatch(_ context.Context, _ []*Auth, finalize func(func() error) error) error {
	return finalize(func() error {
		return NewAuthStoreCommitUnknown(map[string]uint64{s.authID: 2}, s.cause)
	})
}

func (s *tombstonedEnableStore) WithAuthoritativeAuthBatch(_ context.Context, ids []string, finalize func(map[string]AuthAuthoritativeState) error) error {
	states := make(map[string]AuthAuthoritativeState, len(ids))
	for _, id := range ids {
		states[id] = s.state
	}
	return finalize(states)
}

func TestEnableCommitUnknownRejectsMissingOrTombstonedAuthoritativeRow(t *testing.T) {
	tests := []struct {
		name  string
		state AuthAuthoritativeState
	}{
		{name: "missing", state: AuthAuthoritativeState{}},
		{name: "tombstoned", state: AuthAuthoritativeState{Exists: true, Deleted: true, Generation: 2}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const authID = "enable-conflict-auth"
			cause := errors.New("commit acknowledgement lost")
			store := &tombstonedEnableStore{authID: authID, state: test.state, cause: cause}
			manager := NewManager(store, nil, nil)
			auth := &Auth{ID: authID, Provider: "dispatch-test", Disabled: true, Status: StatusDisabled}
			auth.SetStoreGeneration(1)
			if _, errRegister := manager.Register(WithSkipPersist(context.Background()), auth); errRegister != nil {
				t.Fatalf("Register() error = %v", errRegister)
			}

			if _, errEnable := manager.SetDisabled(context.Background(), []string{authID}, false); !errors.Is(errEnable, ErrAuthStoreCommitUnknown) {
				t.Fatalf("SetDisabled(false) error = %v, want commit-unknown error", errEnable)
			}
			current, ok := manager.GetByID(authID)
			if !ok || current == nil || !authIsDisabled(current) {
				t.Fatalf("auth after rejected convergence = %#v, want present and disabled", current)
			}
		})
	}
}
