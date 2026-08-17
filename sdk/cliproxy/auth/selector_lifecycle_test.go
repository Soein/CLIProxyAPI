package auth

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
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

type blockingSelectorReadyRing struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *blockingSelectorReadyRing) Ready() bool {
	r.once.Do(func() { close(r.started) })
	<-r.release
	return true
}

func (*blockingSelectorReadyRing) IsMine(string) bool { return true }

type selectorReplacingPluginScheduler struct {
	manager *Manager
}

func (s *selectorReplacingPluginScheduler) PickAuth(context.Context, pluginapi.SchedulerPickRequest) (pluginapi.SchedulerPickResponse, bool, error) {
	s.manager.SetSelector(&FillFirstSelector{})
	return pluginapi.SchedulerPickResponse{}, false, nil
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
			if !awaitSelectorWriterQueued(t, manager, replacementDone) {
				failure = "selector replacement completed while the old selector was still active"
			}
			if test.operation == selectorLifecycleStop {
				select {
				case <-selector.overlapped:
					failure = "replacement stopped the selector concurrently with StopAutoRefresh"
				case <-replacementDone:
					failure = "selector replacement completed while StopAutoRefresh was still active"
				default:
				}
			} else {
				select {
				case <-selector.stopCalled:
					failure = "replacement stopped the selector while it was still active"
				case <-replacementDone:
					failure = "selector replacement completed while the old selector was still active"
				default:
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

func TestManagerFastPathKeepsSelectorGenerationThroughSchedulerPick(t *testing.T) {
	tests := []struct {
		name string
		pick func(*Manager) error
	}{
		{
			name: "single provider",
			pick: func(manager *Manager) error {
				_, _, err := manager.pickNext(context.Background(), "test", "", cliproxyexecutor.Options{}, nil)
				return err
			},
		},
		{
			name: "mixed provider",
			pick: func(manager *Manager) error {
				_, _, _, err := manager.pickNextMixedWithInflight(context.Background(), []string{"test"}, "", cliproxyexecutor.Options{}, nil, nil, "")
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			initialSelector := &FillFirstSelector{}
			manager := NewManager(nil, initialSelector, nil)
			manager.RegisterExecutor(schedulerTestExecutor{provider: "test"})
			manager.mu.Lock()
			auth := &Auth{ID: "auth-1", Provider: "test", Status: StatusActive}
			manager.auths[auth.ID] = auth
			manager.mu.Unlock()
			manager.schedulerUpsert(auth.Clone())

			ring := &blockingSelectorReadyRing{started: make(chan struct{}), release: make(chan struct{})}
			manager.SetAuthRing(ring)
			manager.SetAuthShardingEnabled(true)
			manager.scheduler.mu.Lock()

			pickDone := make(chan error, 1)
			go func() { pickDone <- test.pick(manager) }()
			awaitSelectorLifecycleSignal(t, ring.started, "fast-path pick did not reach scheduler preparation")

			replacementDone := make(chan struct{})
			go func() {
				manager.SetSelector(newBlockingLifecycleSelector(""))
				close(replacementDone)
			}()
			replacementCompletedEarly := !awaitSelectorWriterQueued(t, manager, replacementDone)
			manager.mu.RLock()
			selectorChangedEarly := !isSameSelector(manager.selector, initialSelector)
			manager.mu.RUnlock()

			manager.scheduler.mu.Unlock()
			close(ring.release)
			if err := awaitSelectorLifecycleResult(t, pickDone); err != nil {
				t.Fatalf("fast-path pick failed: %v", err)
			}
			awaitSelectorLifecycleSignal(t, replacementDone, "selector replacement did not complete")
			if replacementCompletedEarly || selectorChangedEarly {
				t.Fatal("selector generation changed before the scheduler pick completed")
			}
		})
	}
}

func TestManagerPluginSchedulerCanReplaceSelector(t *testing.T) {
	tests := []struct {
		name string
		pick func(*Manager) error
	}{
		{
			name: "single provider",
			pick: func(manager *Manager) error {
				_, err := manager.SelectAuth(context.Background(), "test", "", cliproxyexecutor.Options{})
				return err
			},
		},
		{
			name: "mixed provider",
			pick: func(manager *Manager) error {
				_, _, _, err := manager.pickNextMixedLegacy(context.Background(), []string{"test"}, "", cliproxyexecutor.Options{}, nil)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := NewManager(nil, &RoundRobinSelector{}, nil)
			manager.RegisterExecutor(schedulerTestExecutor{provider: "test"})
			manager.mu.Lock()
			manager.auths["auth-1"] = &Auth{ID: "auth-1", Provider: "test", Status: StatusActive}
			manager.mu.Unlock()
			manager.SetPluginScheduler(&selectorReplacingPluginScheduler{manager: manager})

			done := make(chan error, 1)
			go func() { done <- test.pick(manager) }()
			if err := awaitSelectorLifecycleResult(t, done); err != nil {
				t.Fatalf("selection failed after plugin replaced selector: %v", err)
			}
			if _, ok := manager.Selector().(*FillFirstSelector); !ok {
				t.Fatalf("selector = %T, want *FillFirstSelector", manager.Selector())
			}
		})
	}
}

func awaitSelectorWriterQueued(t *testing.T, manager *Manager, replacementDone <-chan struct{}) bool {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		if !manager.selectorMu.TryRLock() {
			return true
		}
		manager.selectorMu.RUnlock()
		select {
		case <-replacementDone:
			return false
		case <-timer.C:
			t.Fatal("selector replacement writer was not queued")
			return false
		default:
			runtime.Gosched()
		}
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
