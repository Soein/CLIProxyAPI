package auth

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type autoRefreshRejectLocker struct {
	attempts atomic.Int32
	err      error
}

func (l *autoRefreshRejectLocker) TryLock(context.Context, string) (func(), bool, error) {
	l.attempts.Add(1)
	return nil, false, l.err
}

func TestAutoRefreshDefersAcquisitionOrAdmissionRejection(t *testing.T) {
	for _, scenario := range []string{"lock_denied", "lock_error", "admission_rejected"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				executor := &countingRefreshExecutor{id: issue6199RefreshProvider}
				manager, loop := newIssue6199RefreshLoop(executor)
				locker := &autoRefreshRejectLocker{}
				authority := &dispatchAuthorityStub{}
				if scenario == "admission_rejected" {
					manager.SetDispatchAuthority(authority)
				} else {
					if scenario == "lock_error" {
						locker.err = errors.New("refresh lock temporarily unavailable")
					}
					manager.SetAuthRefreshLocker(locker)
				}
				now := time.Now()
				registerIssue6199ExpiredAuth(t, manager, "rejected-refresh", now)
				loop.rebuild(now)
				ctx, cancelCtx := context.WithCancel(context.Background())
				defer cancelCtx()
				loop.handleDue(ctx, now)
				go loop.worker(ctx)
				synctest.Wait()
				auth, _ := manager.GetByID("rejected-refresh")
				wantRetry := now.Add(loop.interval)
				if !auth.NextRefreshAfter.Equal(wantRetry) {
					t.Errorf("next refresh = %v, want %v after rejected attempt", auth.NextRefreshAfter, wantRetry)
				}
				if len(manager.refreshJobs) != 0 {
					t.Fatal("rejected attempt retained a pending refresh job")
				}
				if auth.LastError != nil || authAccessToken(auth) != "expired-access" {
					t.Fatalf("rejected attempt mutated credential/error state: %+v", auth)
				}
				loop.applyDirty(now)
				if next, ok := loop.peek(); !ok || !next.Equal(wantRetry) {
					t.Errorf("heap refresh = (%v, %v), want (%v, true)", next, ok, wantRetry)
				}
				loop.handleDue(ctx, now)
				loop.handleDue(ctx, wantRetry.Add(-time.Nanosecond))
				synctest.Wait()
				if scenario == "admission_rejected" {
					if admits, _, _ := authority.counts(); admits != 1 {
						t.Fatalf("admission attempts = %d, want 1 before retry time", admits)
					}
				} else if got := locker.attempts.Load(); got != 1 {
					t.Fatalf("lock attempts = %d, want 1 before retry time", got)
				}
				if got := executor.refreshCalls.Load(); got != 0 {
					t.Fatalf("executor refresh calls = %d, want 0 after rejected attempt", got)
				}
			})
		})
	}
}

type autoRefreshOutcomeExecutor struct {
	countingRefreshExecutor
	err error
}

func (e *autoRefreshOutcomeExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	e.refreshCalls.Add(1)
	if e.err != nil {
		return nil, e.err
	}
	auth.Metadata["access_token"] = "refreshed-access"
	auth.Metadata["expires_at"] = time.Now().Add(time.Hour).Format(time.RFC3339)
	return auth, nil
}

func TestAutoRefreshPreservesCompletedOutcomeSchedule(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		err       error
		wantDelay time.Duration
	}{
		{name: "success", wantDelay: time.Second},
		{name: "upstream_failure", err: oauthStatusError{code: http.StatusServiceUnavailable, msg: "temporarily unavailable"}, wantDelay: refreshFailureBackoff},
		{name: "invalid_grant_matching_pending_marker", err: oauthStatusError{code: http.StatusBadRequest, msg: `{"error":"invalid_grant"}`}, wantDelay: invalidGrantBackoffBase},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				executor := &autoRefreshOutcomeExecutor{countingRefreshExecutor: countingRefreshExecutor{id: issue6199RefreshProvider}, err: scenario.err}
				manager, loop := newIssue6199RefreshLoop(executor)
				now := time.Now()
				registerIssue6199ExpiredAuth(t, manager, "completed-refresh", now)
				loop.rebuild(now)
				ctx, cancelCtx := context.WithCancel(context.Background())
				defer cancelCtx()
				loop.handleDue(ctx, now)
				go loop.worker(ctx)
				synctest.Wait()
				if len(manager.refreshJobs) != 0 {
					t.Fatal("completed refresh retained a pending job")
				}
				auth, _ := manager.GetByID("completed-refresh")
				if scenario.err == nil {
					if !auth.NextRefreshAfter.IsZero() || authAccessToken(auth) != "refreshed-access" {
						t.Fatalf("successful refresh acquired an extra backoff or lost its token: %+v", auth)
					}
				} else if want := now.Add(scenario.wantDelay); !auth.NextRefreshAfter.Equal(want) {
					t.Fatalf("failed refresh backoff = %v, want %v", auth.NextRefreshAfter, want)
				}
				loop.applyDirty(now)
				if next, ok := loop.peek(); !ok || !next.Equal(now.Add(scenario.wantDelay)) {
					t.Fatalf("completed refresh heap time = (%v, %v), want %v", next, ok, now.Add(scenario.wantDelay))
				}
				if got := executor.refreshCalls.Load(); got != 1 {
					t.Fatalf("executor refresh calls = %d, want 1", got)
				}
			})
		})
	}
}

func TestFinishRefreshJobPreservesUpdatedOrReregisteredMarker(t *testing.T) {
	for _, reregister := range []bool{false, true} {
		t.Run(map[bool]string{false: "updated", true: "reregistered"}[reregister], func(t *testing.T) {
			manager, loop := newIssue6199RefreshLoop(&countingRefreshExecutor{id: issue6199RefreshProvider})
			now := time.Now()
			registerIssue6199ExpiredAuth(t, manager, "newer-marker", now)
			auth, _ := manager.GetByID("newer-marker")
			job := manager.markRefreshPending(loop, auth.ID, auth.RegistrationEpoch, now)
			if job == nil {
				t.Fatal("failed to create pending refresh job")
			}
			auth, _ = manager.GetByID(auth.ID)
			auth.Metadata["operator_note"] = "updated while refresh was queued"
			var updated *Auth
			var errUpdate error
			if reregister {
				updated, errUpdate = manager.Register(context.Background(), auth)
			} else {
				updated, errUpdate = manager.Update(context.Background(), auth)
			}
			if errUpdate != nil {
				t.Fatalf("publish newer auth: %v", errUpdate)
			}
			manager.finishRefreshJob(job, now.Add(loop.interval), false)
			current, _ := manager.GetByID(auth.ID)
			if !current.NextRefreshAfter.Equal(job.pendingUntil) || current.Generation != updated.Generation || current.RegistrationEpoch != updated.RegistrationEpoch {
				t.Fatalf("old job overwrote a newer marker: epoch=%d/%d generation=%d/%d next=%v/%v", current.RegistrationEpoch, updated.RegistrationEpoch, current.Generation, updated.Generation, current.NextRefreshAfter, updated.NextRefreshAfter)
			}
		})
	}
}
