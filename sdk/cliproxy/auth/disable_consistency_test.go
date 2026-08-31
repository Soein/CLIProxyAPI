package auth

import (
	"context"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type blockingSnapshotSelector struct {
	entered chan struct{}
	release chan struct{}
}

type drainSemanticsExecutor struct {
	schedulerTestExecutor
	executed chan string
}

func (e *drainSemanticsExecutor) Identifier() string { return "drain-test" }

func (e *drainSemanticsExecutor) Execute(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.executed <- auth.ID
	return cliproxyexecutor.Response{Payload: []byte(`{"ok":true}`)}, nil
}

func (s *blockingSnapshotSelector) Pick(_ context.Context, _, _ string, _ cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	close(s.entered)
	<-s.release
	if len(auths) == 0 {
		return nil, nil
	}
	return auths[0], nil
}

func TestManagerSelectionRejectsAuthDisabledBeforeSchedulerSync(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	ctx := context.Background()
	const authID = "auth-disable-before-scheduler-sync"
	manager.executors["gemini"] = schedulerTestExecutor{}

	if _, errRegister := manager.Register(ctx, &Auth{ID: authID, Provider: "gemini"}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	manager.scheduler.mu.Lock()
	type pickResult struct {
		auth *Auth
		err  error
	}
	pickStarted := make(chan struct{})
	pickDone := make(chan pickResult, 1)
	go func() {
		close(pickStarted)
		selected, _, errPick := manager.pickNext(ctx, "gemini", "", cliproxyexecutor.Options{}, nil)
		pickDone <- pickResult{auth: selected, err: errPick}
	}()
	<-pickStarted
	// Keep the scheduler locked long enough for pickNext to become the oldest
	// mutex waiter before Update reaches scheduler synchronization.
	time.Sleep(20 * time.Millisecond)

	updateDone := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(ctx, &Auth{ID: authID, Provider: "gemini", Disabled: true, Status: StatusDisabled})
		updateDone <- errUpdate
	}()

	managerStateUpdated := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		current, ok := manager.GetByID(authID)
		if ok && current != nil && current.Disabled {
			managerStateUpdated = true
			break
		}
		time.Sleep(time.Millisecond)
	}

	manager.scheduler.mu.Unlock()

	picked := <-pickDone
	if errUpdate := <-updateDone; errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	if !managerStateUpdated {
		t.Fatal("manager did not publish disabled state before scheduler synchronization")
	}
	if picked.err == nil && picked.auth != nil && picked.auth.ID == authID {
		t.Fatalf("selection returned disabled auth %q before scheduler synchronization", picked.auth.ID)
	}
}

func TestManagerSelectionRejectsLateActiveSnapshotAfterDisable(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	ctx := context.Background()
	const authID = "auth-late-active-snapshot"

	if _, errRegister := manager.Register(ctx, &Auth{ID: authID, Provider: "gemini"}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	activeSnapshot, ok := manager.GetByID(authID)
	if !ok || activeSnapshot == nil {
		t.Fatal("GetByID() did not return registered auth")
	}

	if _, errUpdate := manager.Update(ctx, &Auth{ID: authID, Provider: "gemini", Disabled: true, Status: StatusDisabled}); errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}

	manager.scheduler.upsertAuth(activeSnapshot)
	selected, errPick := manager.scheduler.pickSingle(ctx, "gemini", "", cliproxyexecutor.Options{}, nil)
	if errPick == nil && selected != nil && selected.ID == authID {
		t.Fatalf("selection returned disabled auth %q after late active snapshot", selected.ID)
	}
}

func TestManagerLegacySelectionRejectsAuthDisabledAfterCandidateSnapshot(t *testing.T) {
	selector := &blockingSnapshotSelector{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := NewManager(nil, selector, nil)
	manager.executors["gemini"] = schedulerTestExecutor{}
	ctx := context.Background()
	const authID = "auth-disabled-after-legacy-snapshot"

	if _, errRegister := manager.Register(ctx, &Auth{ID: authID, Provider: "gemini"}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}

	type legacyPickResult struct {
		auth *Auth
		err  error
	}
	pickDone := make(chan legacyPickResult, 1)
	go func() {
		selected, _, errPick := manager.pickNext(ctx, "gemini", "", cliproxyexecutor.Options{}, nil)
		pickDone <- legacyPickResult{auth: selected, err: errPick}
	}()

	<-selector.entered
	if _, errUpdate := manager.Update(ctx, &Auth{ID: authID, Provider: "gemini", Disabled: true, Status: StatusDisabled}); errUpdate != nil {
		close(selector.release)
		t.Fatalf("Update() error = %v", errUpdate)
	}
	close(selector.release)

	picked := <-pickDone
	if picked.err == nil && picked.auth != nil && picked.auth.ID == authID {
		t.Fatalf("legacy selection returned disabled auth %q from stale candidate snapshot", picked.auth.ID)
	}
}

func TestManagerSetDisabledDrainsRequestAdmittedBeforeDisable(t *testing.T) {
	const (
		provider = "drain-test"
		model    = "drain-test-model"
		authID   = "drain-test-auth"
	)
	registerSchedulerModels(t, provider, model, authID)

	executor := &drainSemanticsExecutor{executed: make(chan string, 2)}
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.RegisterExecutor(executor)
	if _, errRegister := manager.Register(context.Background(), &Auth{ID: authID, Provider: provider}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}

	interceptorEntered := make(chan struct{})
	releaseInterceptor := make(chan struct{})
	executeDone := make(chan error, 1)
	go func() {
		_, errExecute := manager.Execute(
			context.Background(),
			[]string{provider},
			cliproxyexecutor.Request{Model: model},
			cliproxyexecutor.Options{RequestAfterAuthInterceptor: func(context.Context, cliproxyexecutor.RequestAfterAuthInterceptRequest) cliproxyexecutor.RequestAfterAuthInterceptResponse {
				close(interceptorEntered)
				<-releaseInterceptor
				return cliproxyexecutor.RequestAfterAuthInterceptResponse{}
			}},
		)
		executeDone <- errExecute
	}()

	<-interceptorEntered
	if _, errDisable := manager.SetDisabled(context.Background(), []string{authID}, true); errDisable != nil {
		close(releaseInterceptor)
		t.Fatalf("SetDisabled() error = %v", errDisable)
	}
	close(releaseInterceptor)
	if errExecute := <-executeDone; errExecute != nil {
		t.Fatalf("request admitted before SetDisabled() error = %v", errExecute)
	}
	if executedAuthID := <-executor.executed; executedAuthID != authID {
		t.Fatalf("executor auth ID = %q, want %q", executedAuthID, authID)
	}

	if _, errExecute := manager.Execute(context.Background(), []string{provider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{}); errExecute == nil {
		t.Fatal("request started after SetDisabled() unexpectedly executed")
	}
	select {
	case executedAuthID := <-executor.executed:
		t.Fatalf("executor received disabled auth %q for a new request", executedAuthID)
	default:
	}
}

func TestManagerSetDisabledAdvancesGenerationAndPublishesRegistryState(t *testing.T) {
	const (
		provider = "set-disabled-consistency"
		model    = "set-disabled-consistency-model"
		authID   = "set-disabled-consistency-auth"
	)
	ctx := context.Background()
	registerSchedulerModels(t, provider, model, authID)
	reg := registry.GetGlobalRegistry()
	manager := NewManager(nil, &RoundRobinSelector{}, nil)

	registered, errRegister := manager.Register(ctx, &Auth{ID: authID, Provider: provider, Status: StatusActive})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	manager.ReconcileRegistryModelStates(ctx, authID)
	if reg.IsModelSuspendedForClient(authID, model) {
		t.Fatal("registered active auth model is unexpectedly suspended")
	}

	disabled, errDisable := manager.SetDisabled(ctx, []string{authID}, true)
	if errDisable != nil {
		t.Fatalf("SetDisabled(true) error = %v", errDisable)
	}
	if len(disabled) != 1 || disabled[0] == nil {
		t.Fatalf("SetDisabled(true) result = %#v, want one auth", disabled)
	}
	if disabled[0].Generation <= registered.Generation {
		t.Fatalf("disabled Generation = %d, want > %d", disabled[0].Generation, registered.Generation)
	}
	if !reg.IsModelSuspendedForClient(authID, model) {
		t.Fatal("disabled auth model was not suspended in registry")
	}

	enabled, errEnable := manager.SetDisabled(ctx, []string{authID}, false)
	if errEnable != nil {
		t.Fatalf("SetDisabled(false) error = %v", errEnable)
	}
	if len(enabled) != 1 || enabled[0] == nil {
		t.Fatalf("SetDisabled(false) result = %#v, want one auth", enabled)
	}
	if enabled[0].Generation <= disabled[0].Generation {
		t.Fatalf("enabled Generation = %d, want > %d", enabled[0].Generation, disabled[0].Generation)
	}
	if reg.IsModelSuspendedForClient(authID, model) {
		t.Fatal("enabled auth model remained suspended in registry")
	}
	regEpoch := reg.ClientRegistrationEpoch(authID)
	if reg.ApplyClientModelProjections(authID, regEpoch, disabled[0].Generation, []registry.ClientModelProjection{{ModelID: model, Suspended: true}}) {
		t.Fatal("registry accepted a delayed projection from the older disabled generation")
	}
	if reg.IsModelSuspendedForClient(authID, model) {
		t.Fatal("delayed disabled projection overwrote the enabled registry state")
	}
}

func TestManagerSetDisabledEnableOutrunsConcurrentMarkResult(t *testing.T) {
	const (
		provider = "gemini"
		model    = "enable-concurrent-result-model"
		authID   = "enable-concurrent-result-auth"
	)
	ctx := context.Background()
	registerSchedulerModels(t, provider, model, authID)
	reg := registry.GetGlobalRegistry()
	store := newBlockingOrderedStore()
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	manager.RegisterExecutor(schedulerProviderTestExecutor{provider: provider})

	if _, errRegister := manager.Register(WithSkipPersist(ctx), &Auth{ID: authID, Provider: provider, Status: StatusActive}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	manager.RefreshSchedulerEntry(authID)
	if _, errDisable := manager.SetDisabled(WithSkipPersist(ctx), []string{authID}, true); errDisable != nil {
		t.Fatalf("SetDisabled(true) error = %v", errDisable)
	}
	disabled, ok := manager.GetByID(authID)
	if !ok || disabled == nil {
		t.Fatal("GetByID() did not return disabled auth")
	}
	store.mu.Lock()
	store.current = disabled.Clone()
	store.mu.Unlock()

	enableDone := make(chan error, 1)
	go func() {
		_, errEnable := manager.SetDisabled(ctx, []string{authID}, false)
		enableDone <- errEnable
	}()
	<-store.activeSaveEntered

	manager.MarkResult(ctx, Result{AuthID: authID, Provider: provider, Model: model, Success: true})
	marked, ok := manager.GetByID(authID)
	if !ok || marked == nil || !marked.Disabled {
		t.Fatalf("auth during blocked enable = %#v, want disabled", marked)
	}
	close(store.releaseActiveSave)
	if errEnable := <-enableDone; errEnable != nil {
		t.Fatalf("SetDisabled(false) error = %v", errEnable)
	}

	enabled, ok := manager.GetByID(authID)
	if !ok || enabled == nil || enabled.Disabled || enabled.Status != StatusActive {
		t.Fatalf("auth after enable = %#v, want active", enabled)
	}
	if enabled.Generation <= marked.Generation {
		t.Fatalf("enabled Generation = %d, want > concurrent result generation %d", enabled.Generation, marked.Generation)
	}
	if reg.IsModelSuspendedForClient(authID, model) {
		t.Fatal("registry model remained suspended after concurrent enable")
	}
	if _, errExecute := manager.Execute(ctx, []string{provider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{}); errExecute != nil {
		t.Fatalf("Execute() after concurrent enable error = %v", errExecute)
	}
}
