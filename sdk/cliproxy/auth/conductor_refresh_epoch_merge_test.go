package auth

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type epochMergeRefreshLocker struct {
	attempts atomic.Int32
	release  <-chan struct{}
}

func (l *epochMergeRefreshLocker) TryLock(context.Context, string) (func(), bool, error) {
	if l.attempts.Add(1) > 1 {
		<-l.release
	}
	return func() {}, true, nil
}

type epochMergeRefreshExecutor struct {
	countingRefreshExecutor
	started chan struct{}
	release <-chan struct{}
}

func (e *epochMergeRefreshExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	if e.refreshCalls.Add(1) == 1 {
		close(e.started)
		<-e.release
	}
	auth.Metadata["access_token"] = "refreshed-" + authAccessToken(auth)
	return auth, nil
}

func TestRefreshAuthAtEpochRejectsReplacementBeforeRefresh(t *testing.T) {
	executor := &countingRefreshExecutor{id: issue6199RefreshProvider}
	manager, _ := newIssue6199RefreshLoop(executor)
	registerIssue6199ExpiredAuth(t, manager, "stale-epoch", time.Now())
	previous, _ := manager.GetByID("stale-epoch")
	manager.Remove(context.Background(), previous.ID)
	registerIssue6199ExpiredAuth(t, manager, previous.ID, time.Now())
	if _, errRefresh := manager.refreshAuthForRequestAtEpoch(context.Background(), previous.ID, "", previous.RegistrationEpoch); errRefresh == nil {
		t.Fatal("stale registration epoch was accepted")
	}
	if got := executor.refreshCalls.Load(); got != 0 {
		t.Fatalf("stale registration refreshed the replacement %d times", got)
	}
}

func TestRefreshAuthSuccessClearsInvalidGrantFailureCount(t *testing.T) {
	executor := &countingRefreshExecutor{id: issue6199RefreshProvider}
	manager, _ := newIssue6199RefreshLoop(executor)
	registerIssue6199ExpiredAuth(t, manager, "recovered-invalid-grant", time.Now())
	auth, _ := manager.GetByID("recovered-invalid-grant")
	auth.RefreshFailures = 3
	if _, errUpdate := manager.Update(context.Background(), auth); errUpdate != nil {
		t.Fatalf("set refresh failure count: %v", errUpdate)
	}
	refreshed, errRefresh := manager.ForceRefreshAuth(context.Background(), auth.ID)
	if errRefresh != nil {
		t.Fatalf("refresh after invalid grant: %v", errRefresh)
	}
	if refreshed.RefreshFailures != 0 {
		t.Fatalf("refresh failures after success = %d, want 0", refreshed.RefreshFailures)
	}
}

func TestRefreshAuthSingleflightSeparatesRegistrationEpochs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		executor := &epochMergeRefreshExecutor{
			countingRefreshExecutor: countingRefreshExecutor{id: issue6199RefreshProvider},
			started:                 make(chan struct{}),
			release:                 release,
		}
		locker := &epochMergeRefreshLocker{release: release}
		manager, _ := newIssue6199RefreshLoop(executor)
		manager.SetAuthRefreshLocker(locker)
		registerIssue6199ExpiredAuth(t, manager, "reregistered", time.Now())
		previous, _ := manager.GetByID("reregistered")
		oldDone := make(chan error, 1)
		go func() {
			_, errRefresh := manager.refreshAuthForRequestAtEpoch(context.Background(), previous.ID, "", previous.RegistrationEpoch)
			oldDone <- errRefresh
		}()
		<-executor.started
		manager.Remove(context.Background(), previous.ID)
		registerIssue6199ExpiredAuth(t, manager, previous.ID, time.Now())
		current, _ := manager.GetByID(previous.ID)
		current.Metadata["access_token"] = "replacement-access"
		if _, errUpdate := manager.Update(context.Background(), current); errUpdate != nil {
			t.Fatalf("update replacement token: %v", errUpdate)
		}
		newDone := make(chan error, 1)
		go func() {
			_, errRefresh := manager.ForceRefreshAuth(context.Background(), previous.ID)
			newDone <- errRefresh
		}()
		synctest.Wait()
		if got := locker.attempts.Load(); got != 2 {
			t.Errorf("refresh lock attempts = %d, want 2 independent registration lifecycles", got)
		}
		close(release)
		if errRefresh := <-oldDone; errRefresh == nil {
			t.Error("late refresh from removed registration succeeded")
		}
		if errRefresh := <-newDone; errRefresh != nil {
			t.Fatalf("replacement refresh failed: %v", errRefresh)
		}
		refreshed, _ := manager.GetByID(previous.ID)
		if refreshed.RegistrationEpoch != current.RegistrationEpoch || authAccessToken(refreshed) != "refreshed-replacement-access" {
			t.Fatalf("replacement lifecycle was corrupted: epoch=%d token=%q", refreshed.RegistrationEpoch, authAccessToken(refreshed))
		}
	})
}

func TestRefreshAuthSingleflightSharesBackgroundAndRequestEpoch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		executor := &epochMergeRefreshExecutor{
			countingRefreshExecutor: countingRefreshExecutor{id: issue6199RefreshProvider},
			started:                 make(chan struct{}),
			release:                 release,
		}
		locker := &epochMergeRefreshLocker{release: release}
		manager, _ := newIssue6199RefreshLoop(executor)
		manager.SetAuthRefreshLocker(locker)
		registerIssue6199ExpiredAuth(t, manager, "shared-epoch", time.Now())
		auth, _ := manager.GetByID("shared-epoch")
		done := make(chan error, 2)
		go func() {
			_, errRefresh := manager.refreshAuthForRequestAtEpoch(context.Background(), auth.ID, "", auth.RegistrationEpoch)
			done <- errRefresh
		}()
		<-executor.started
		go func() {
			_, errRefresh := manager.refreshAuthForRequest(context.Background(), auth.ID, authAccessToken(auth))
			done <- errRefresh
		}()
		synctest.Wait()
		if got := locker.attempts.Load(); got != 1 {
			t.Errorf("refresh lock attempts = %d, want shared singleflight", got)
		}
		close(release)
		for i := 0; i < 2; i++ {
			if errRefresh := <-done; errRefresh != nil {
				t.Fatalf("shared refresh failed: %v", errRefresh)
			}
		}
		if got := executor.refreshCalls.Load(); got != 1 {
			t.Fatalf("refresh calls = %d, want 1", got)
		}
	})
}

func TestAutoRefreshNonOwnerDefersAndReleasesPendingJob(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		executor := &countingRefreshExecutor{id: issue6199RefreshProvider}
		manager, loop := newIssue6199RefreshLoop(executor)
		manager.SetAuthShardingEnabled(true)
		manager.SetAuthRing(&stubRing{ready: true, mine: map[string]bool{"remote-owner": true}})
		now := time.Now()
		registerIssue6199ExpiredAuth(t, manager, "remote-owner", now)
		loop.rebuild(now)
		loop.handleDue(context.Background(), now)
		if got := len(loop.jobs); got != 1 {
			t.Fatalf("queued refresh jobs = %d, want 1 before ownership loss", got)
		}
		manager.SetAuthRing(&stubRing{ready: true, mine: map[string]bool{}})
		ctx, cancelCtx := context.WithCancel(context.Background())
		defer cancelCtx()
		go loop.worker(ctx)
		synctest.Wait()
		if got := executor.refreshCalls.Load(); got != 0 {
			t.Fatalf("non-owner refreshed credential %d times", got)
		}
		if len(manager.refreshJobs) != 0 {
			t.Fatal("non-owner retained pending refresh job")
		}
		auth, _ := manager.GetByID("remote-owner")
		if want := now.Add(loop.interval); !auth.NextRefreshAfter.Equal(want) {
			t.Fatalf("deferred refresh = %v, want %v", auth.NextRefreshAfter, want)
		}
		loop.applyDirty(now)
		if wait, ok := loop.nextWait(now); !ok || wait != loop.interval {
			t.Fatalf("next refresh wait = (%v, %v), want (%v, true)", wait, ok, loop.interval)
		}
	})
}
