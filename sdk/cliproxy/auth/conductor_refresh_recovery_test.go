package auth

import (
	"context"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestManager_RefreshRecoveryKeepsAutoRefreshScheduled(t *testing.T) {
	ctx := context.Background()
	manager, executor, primary, backup, model := newUnauthorizedRefreshFixture(t, true)
	store := newMemoryAuthTestStore()
	manager.SetStore(store)
	now := time.Now()
	expiry := now.Add(2 * time.Hour).Truncate(time.Second)
	primary.Status = StatusActive
	primary.LastRefreshedAt = now
	primary.Metadata["expired"] = expiry.Format(time.RFC3339)
	primary.Metadata["refresh_interval_seconds"] = 900
	if _, errUpdate := manager.Update(ctx, primary); errUpdate != nil {
		t.Fatalf("set unexpired primary token: %v", errUpdate)
	}

	// Execute drives the request 401, synchronous refresh failure, and MarkResult.
	response, errExecute := manager.Execute(ctx, []string{"codex"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if errExecute != nil || string(response.Payload) != backup.ID+":backup-access-token" {
		t.Fatalf("request fallback = %q, %v; want backup response", response.Payload, errExecute)
	}
	if executor.RefreshCalls() != 1 {
		t.Fatalf("request refresh calls = %d, want 1", executor.RefreshCalls())
	}
	failed, ok := manager.GetByID(primary.ID)
	if !ok || failed == nil {
		t.Fatal("primary missing after request failure")
	}
	if !failed.HasValidAccessToken(now) || authAccessToken(failed) != "stale-access-token" {
		t.Fatal("failed refresh must retain the unexpired access token")
	}
	state := failed.ModelStates[model]
	if state == nil || state.LastError == nil || state.LastError.HTTPStatus != http.StatusUnauthorized || !state.Unavailable || !state.NextRetryAfter.After(now) {
		t.Fatalf("request 401 did not establish model cooldown: %+v", state)
	}
	if failed.LastError == nil || failed.LastError.HTTPStatus != http.StatusUnauthorized || !failed.Unavailable || failed.Status != StatusError {
		t.Fatalf("request 401 did not establish aggregate failure: %+v", failed)
	}
	if failed.NextRefreshAfter.IsZero() || !failed.NextRefreshAfter.Before(expiry) {
		t.Fatalf("refresh retry = %v, want retry before expiry %v", failed.NextRefreshAfter, expiry)
	}
	if next, scheduled := nextRefreshCheckAt(now, failed, time.Minute); !scheduled || !next.Equal(failed.NextRefreshAfter) {
		t.Fatalf("failed refresh schedule = %v, %v; want %v", next, scheduled, failed.NextRefreshAfter)
	}

	executor.mu.Lock()
	executor.refreshFail = false
	executor.mu.Unlock()
	// The background worker enters refreshAuthOnce without a failed request token.
	if _, errRefresh := manager.refreshAuthOnce(ctx, primary.ID, ""); errRefresh != nil {
		t.Fatalf("background refresh: %v", errRefresh)
	}
	if executor.RefreshCalls() != 2 {
		t.Fatalf("total refresh calls = %d, want 2", executor.RefreshCalls())
	}
	recovered, ok := manager.GetByID(primary.ID)
	if !ok || recovered == nil {
		t.Fatal("primary missing after successful refresh")
	}
	persisted, errList := store.List(ctx)
	if errList != nil {
		t.Fatalf("read persisted auths: %v", errList)
	}
	var saved *Auth
	for _, candidate := range persisted {
		if candidate.ID == primary.ID {
			saved = candidate
			break
		}
	}
	if saved == nil {
		t.Fatal("successful refresh was not persisted")
	}
	for name, auth := range map[string]*Auth{"manager": recovered, "store": saved} {
		t.Run(name, func(t *testing.T) {
			if authAccessToken(auth) != "fresh-access-token" || !modelStateIsClean(auth.ModelStates[model]) {
				t.Fatal("successful refresh did not replace token and clear model 401")
			}
			checkAt := auth.LastRefreshedAt
			next, scheduled := nextRefreshCheckAt(checkAt, auth, time.Minute)
			if !scheduled {
				t.Fatalf("successful refresh dropped from scheduler: status=%s unavailable=%v old401=%v nextRefreshZero=%v", auth.Status, auth.Unavailable, hasUnauthorizedAuthFailure(auth), auth.NextRefreshAfter.IsZero())
			}
			if auth.Status != StatusActive || auth.Unavailable || auth.LastError != nil || auth.StatusMessage != "" || !auth.NextRetryAfter.IsZero() || !auth.NextRefreshAfter.IsZero() {
				t.Fatalf("successful refresh retained aggregate failure or backoff: %+v", auth)
			}
			if want := checkAt.Add(15 * time.Minute); !next.Equal(want) || !next.Before(expiry) {
				t.Fatalf("next refresh = %v, want %v before expiry %v", next, want, expiry)
			}
			if manager.shouldRefresh(auth, next.Add(-time.Nanosecond)) || !manager.shouldRefresh(auth, next) {
				t.Fatal("refresh must become due exactly at the next scheduled time")
			}
		})
	}
	if t.Failed() {
		return
	}

	loop := newAuthAutoRefreshLoop(manager, time.Minute, 1)
	loop.rebuild(recovered.LastRefreshedAt)
	item := loop.index[primary.ID]
	if item == nil {
		t.Fatal("recovered auth missing from automatic refresh queue")
	}
	// Advance only the scheduler's explicit clock; no worker or wall-clock wait.
	loop.handleDueAuth(ctx, item.next.Add(-time.Nanosecond), primary.ID)
	select {
	case id := <-loop.jobs:
		t.Fatalf("refresh dispatched before due time for %s", id)
	default:
	}
	loop.handleDueAuth(ctx, item.next, primary.ID)
	select {
	case id := <-loop.jobs:
		if id != primary.ID {
			t.Fatalf("scheduled auth = %q, want %q", id, primary.ID)
		}
	default:
		t.Fatal("recovered auth was not dispatched again before token expiry")
	}
}

func TestManager_RefreshRecoveryPreservesConcurrentState(t *testing.T) {
	for _, scenario := range []string{"disabled", "new_401", "model_429"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			store := newMemoryAuthTestStore()
			manager := NewManager(store, nil, nil)
			executor := &blockingSuccessRefreshExecutor{
				schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "codex"},
				started:                       make(chan struct{}), release: make(chan struct{}),
			}
			manager.RegisterExecutor(executor)
			const authID, model = "refresh-concurrent-recovery", "recovery-model"
			if _, errRegister := manager.Register(ctx, &Auth{
				ID: authID, Provider: "codex", Status: StatusActive,
				Metadata: map[string]any{
					"access_token": "old-access-token", "refresh_token": "refresh-token",
					"expired":                  time.Now().Add(2 * time.Hour).Format(time.RFC3339),
					"refresh_interval_seconds": 900,
				},
			}); errRegister != nil {
				t.Fatalf("register auth: %v", errRegister)
			}
			manager.MarkResult(ctx, Result{
				AuthID: authID, Provider: "codex", Model: model,
				Error: &Error{HTTPStatus: http.StatusUnauthorized, Message: "old request 401"},
			})

			done := make(chan struct{})
			var errRefresh error
			go func() {
				_, errRefresh = manager.refreshAuthForRequest(ctx, authID, "old-access-token")
				close(done)
			}()
			var release sync.Once
			t.Cleanup(func() {
				release.Do(func() { close(executor.release) })
				<-done
			})
			<-executor.started

			concurrentModel := model
			switch scenario {
			case "disabled":
				if _, errDisable := manager.SetDisabled(ctx, []string{authID}, true); errDisable != nil {
					t.Fatalf("disable during refresh: %v", errDisable)
				}
			case "new_401":
				manager.MarkResult(ctx, Result{
					AuthID: authID, Provider: "codex", Model: model,
					Error: &Error{HTTPStatus: http.StatusUnauthorized, Message: "concurrent request 401"},
				})
			case "model_429":
				concurrentModel = "quota-model"
				retryAfter := 30 * time.Minute
				manager.MarkResult(ctx, Result{
					AuthID: authID, Provider: "codex", Model: concurrentModel, RetryAfter: &retryAfter,
					Error: &Error{HTTPStatus: http.StatusTooManyRequests, Message: "concurrent quota 429"},
				})
			}
			concurrent, ok := manager.GetByID(authID)
			if !ok || concurrent == nil {
				t.Fatal("auth missing after concurrent mutation")
			}
			release.Do(func() { close(executor.release) })
			<-done
			if errRefresh != nil {
				t.Fatalf("request refresh: %v", errRefresh)
			}
			recovered, ok := manager.GetByID(authID)
			if !ok || recovered == nil {
				t.Fatal("auth missing after refresh")
			}
			persisted, errList := store.List(ctx)
			if errList != nil || len(persisted) != 1 {
				t.Fatalf("persisted auth count = %d, error = %v; want one auth", len(persisted), errList)
			}
			for _, auth := range []*Auth{recovered, persisted[0]} {
				if authAccessToken(auth) != "refreshed-access-token" {
					t.Fatal("concurrent mutation prevented refreshed token persistence")
				}
				if scenario == "disabled" {
					if !auth.Disabled || auth.Status != StatusDisabled || auth.Metadata["disabled"] != true {
						t.Fatalf("refresh undid concurrent disable: %+v", auth)
					}
					if !reflect.DeepEqual(auth.ModelStates[model], concurrent.ModelStates[model]) {
						t.Fatal("refresh overwrote model state changed by concurrent disable")
					}
				} else {
					if !reflect.DeepEqual(auth.ModelStates[concurrentModel], concurrent.ModelStates[concurrentModel]) {
						t.Fatalf("refresh changed concurrent model failure: got %+v, want %+v", auth.ModelStates[concurrentModel], concurrent.ModelStates[concurrentModel])
					}
					if auth.LastError == nil || auth.LastError.Message != concurrent.LastError.Message || auth.Status != StatusError {
						t.Fatalf("refresh cleared concurrent aggregate failure: %+v", auth)
					}
				}
				if scenario == "model_429" && !modelStateIsClean(auth.ModelStates[model]) {
					t.Fatalf("old model 401 survived successful refresh: %+v", auth.ModelStates[model])
				}
			}
		})
	}
}
