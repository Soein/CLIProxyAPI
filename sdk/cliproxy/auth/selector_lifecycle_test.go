package auth

import (
	"context"
	"sync"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type selectorLifecycleOperation string

const (
	selectorLifecyclePick       selectorLifecycleOperation = "pick"
	selectorLifecycleResult     selectorLifecycleOperation = "result"
	selectorLifecycleInvalidate selectorLifecycleOperation = "invalidate"
	selectorLifecycleStop       selectorLifecycleOperation = "stop"
)

type blockingLifecycleSelector struct {
	target      selectorLifecycleOperation
	started     chan struct{}
	release     chan struct{}
	overlapped  chan struct{}
	stopCalled  chan struct{}
	startOnce   sync.Once
	overlapOnce sync.Once
	stopOnce    sync.Once
	mu          sync.Mutex
	active      int
	stopCalls   int
}

func newBlockingLifecycleSelector(target selectorLifecycleOperation) *blockingLifecycleSelector {
	return &blockingLifecycleSelector{
		target:     target,
		started:    make(chan struct{}),
		release:    make(chan struct{}),
		overlapped: make(chan struct{}),
		stopCalled: make(chan struct{}),
	}
}

func (s *blockingLifecycleSelector) block(operation selectorLifecycleOperation) {
	if s == nil || s.target != operation {
		return
	}
	s.mu.Lock()
	s.active++
	if s.active > 1 {
		s.overlapOnce.Do(func() { close(s.overlapped) })
	}
	s.startOnce.Do(func() { close(s.started) })
	s.mu.Unlock()

	<-s.release

	s.mu.Lock()
	s.active--
	s.mu.Unlock()
}

func (s *blockingLifecycleSelector) Pick(_ context.Context, _, _ string, _ cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	s.block(selectorLifecyclePick)
	if len(auths) == 0 {
		return nil, nil
	}
	return auths[0], nil
}

func (s *blockingLifecycleSelector) OnResult(Result) {
	s.block(selectorLifecycleResult)
}

func (s *blockingLifecycleSelector) InvalidateAuth(string) {
	s.block(selectorLifecycleInvalidate)
}

func (s *blockingLifecycleSelector) Stop() {
	s.mu.Lock()
	s.stopCalls++
	s.stopOnce.Do(func() { close(s.stopCalled) })
	s.mu.Unlock()
	s.block(selectorLifecycleStop)
}

func (s *blockingLifecycleSelector) stopCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopCalls
}

func TestManagerSelectorReplacementWaitsForActiveUsers(t *testing.T) {
	tests := []struct {
		name      string
		operation selectorLifecycleOperation
		start     func(*Manager) <-chan error
	}{
		{
			name:      "pick",
			operation: selectorLifecyclePick,
			start: func(manager *Manager) <-chan error {
				manager.RegisterExecutor(schedulerTestExecutor{provider: "test"})
				manager.mu.Lock()
				manager.auths["auth-1"] = &Auth{ID: "auth-1", Provider: "test"}
				manager.mu.Unlock()
				done := make(chan error, 1)
				go func() {
					_, err := manager.SelectAuth(context.Background(), "test", "", cliproxyexecutor.Options{})
					done <- err
				}()
				return done
			},
		},
		{
			name:      "mixed pick",
			operation: selectorLifecyclePick,
			start: func(manager *Manager) <-chan error {
				manager.RegisterExecutor(schedulerTestExecutor{provider: "test"})
				manager.mu.Lock()
				manager.auths["auth-1"] = &Auth{ID: "auth-1", Provider: "test"}
				manager.mu.Unlock()
				done := make(chan error, 1)
				go func() {
					_, _, _, err := manager.pickNextMixedLegacy(context.Background(), []string{"test"}, "", cliproxyexecutor.Options{}, nil)
					done <- err
				}()
				return done
			},
		},
		{
			name:      "mark result",
			operation: selectorLifecycleResult,
			start: func(manager *Manager) <-chan error {
				done := make(chan error, 1)
				go func() {
					manager.MarkResult(context.Background(), Result{AuthID: "auth-1", Success: true})
					done <- nil
				}()
				return done
			},
		},
		{
			name:      "remove auth",
			operation: selectorLifecycleInvalidate,
			start: func(manager *Manager) <-chan error {
				manager.mu.Lock()
				manager.auths["auth-1"] = &Auth{ID: "auth-1", Provider: "test"}
				manager.mu.Unlock()
				done := make(chan error, 1)
				go func() {
					manager.Remove(context.Background(), "auth-1")
					done <- nil
				}()
				return done
			},
		},
		{
			name:      "stop auto refresh",
			operation: selectorLifecycleStop,
			start: func(manager *Manager) <-chan error {
				done := make(chan error, 1)
				go func() {
					manager.StopAutoRefresh()
					done <- nil
				}()
				return done
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			selector := newBlockingLifecycleSelector(test.operation)
			manager := NewManager(nil, selector, nil)
			operationDone := test.start(manager)
			awaitSelectorLifecycleSignal(t, selector.started, "selector operation did not start")

			replacementDone := make(chan struct{})
			go func() {
				manager.SetSelector(&RoundRobinSelector{})
				close(replacementDone)
			}()

			var failure string
			if test.operation == selectorLifecycleStop {
				select {
				case <-selector.overlapped:
					failure = "replacement stopped the selector concurrently with StopAutoRefresh"
				case <-replacementDone:
					failure = "selector replacement completed while StopAutoRefresh was still active"
				case <-time.After(100 * time.Millisecond):
				}
			} else {
				select {
				case <-selector.stopCalled:
					failure = "replacement stopped the selector while it was still active"
				case <-replacementDone:
					failure = "selector replacement completed while the old selector was still active"
				case <-time.After(100 * time.Millisecond):
				}
			}

			close(selector.release)
			awaitSelectorLifecycleSignal(t, replacementDone, "selector replacement did not complete")
			if err := awaitSelectorLifecycleResult(t, operationDone); err != nil {
				t.Fatalf("selector operation failed: %v", err)
			}
			if failure != "" {
				t.Fatal(failure)
			}
			wantStopCalls := 1
			if test.operation == selectorLifecycleStop {
				wantStopCalls = 2
			}
			if gotStopCalls := selector.stopCallCount(); gotStopCalls != wantStopCalls {
				t.Fatalf("Stop call count = %d, want %d", gotStopCalls, wantStopCalls)
			}
		})
	}
}

func awaitSelectorLifecycleSignal(t *testing.T, signal <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal(failure)
	}
}

func awaitSelectorLifecycleResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("selector operation did not complete")
		return nil
	}
}
