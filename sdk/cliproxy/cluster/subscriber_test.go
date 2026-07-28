package cluster

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type subscriberTestConn struct {
	waitErr      error
	notification *pgconn.Notification
}

type readinessSubscriberConn struct {
	execCalls *atomic.Int32
	waitCalls *atomic.Int32
}

func (c *readinessSubscriberConn) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	c.execCalls.Add(1)
	return pgconn.CommandTag{}, nil
}

func (c *readinessSubscriberConn) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	c.waitCalls.Add(1)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*readinessSubscriberConn) Close(context.Context) error { return nil }

func TestChangeSubscriber_ReadyRequiresListenThenSuccessfulAuthoritativeResync(t *testing.T) {
	var connectCalls atomic.Int32
	var execCalls atomic.Int32
	var waitCalls atomic.Int32
	var resyncCalls atomic.Int32
	secondResyncStarted := make(chan struct{})
	releaseSecondResync := make(chan struct{})
	subscriber := &ChangeSubscriber{
		DSN:      "postgres://test",
		Backoffs: []time.Duration{time.Millisecond},
		Handlers: Handlers{OnResync: func(context.Context) error {
			call := resyncCalls.Add(1)
			if got, want := execCalls.Load(), call*2; got != want {
				return errors.New("resync ran before both LISTEN statements")
			}
			if call == 1 {
				return errors.New("simulated authoritative resync failure")
			}
			close(secondResyncStarted)
			<-releaseSecondResync
			return nil
		}},
		connect: func(context.Context, string) (subscriberConnection, error) {
			connectCalls.Add(1)
			return &readinessSubscriberConn{execCalls: &execCalls, waitCalls: &waitCalls}, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		subscriber.Run(ctx)
	}()
	select {
	case <-secondResyncStarted:
	case <-time.After(time.Second):
		t.Fatal("subscriber did not reconnect immediately after the failed initial resync")
	}
	select {
	case <-subscriber.Ready():
		t.Fatal("subscriber became ready before the first successful resync")
	default:
	}
	if got := waitCalls.Load(); got != 0 {
		t.Fatalf("WaitForNotification calls before successful resync = %d, want 0", got)
	}
	close(releaseSecondResync)
	select {
	case <-subscriber.Ready():
	case <-time.After(time.Second):
		t.Fatal("subscriber did not become ready after successful resync")
	}
	waitForAtomicCount(t, &waitCalls, 1)
	if got := connectCalls.Load(); got != 2 {
		t.Fatalf("connection attempts = %d, want 2", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("subscriber did not stop after cancellation")
	}
}

func TestChangeSubscriber_BackoffResetsOnlyAfterSuccessfulInitialResync(t *testing.T) {
	backoffs := []time.Duration{time.Second, 3 * time.Second, 10 * time.Second, 30 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var resyncCalls atomic.Int32
	var waits []time.Duration
	subscriber := &ChangeSubscriber{
		DSN:      "postgres://test",
		Backoffs: backoffs,
		Handlers: Handlers{OnResync: func(context.Context) error {
			if resyncCalls.Add(1) <= int32(len(backoffs)) {
				return errors.New("simulated authoritative resync failure")
			}
			return nil
		}},
		connect: func(context.Context, string) (subscriberConnection, error) {
			return &subscriberTestConn{waitErr: errors.New("simulated quiet connection disconnect")}, nil
		},
		wait: func(_ context.Context, delay time.Duration) bool {
			waits = append(waits, delay)
			if len(waits) == len(backoffs)+1 {
				cancel()
				return false
			}
			return true
		},
	}

	subscriber.Run(ctx)

	want := append(slices.Clone(backoffs), time.Second)
	if !slices.Equal(waits, want) {
		t.Fatalf("retry waits = %v, want %v", waits, want)
	}
}

func (*subscriberTestConn) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (c *subscriberTestConn) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	if c.notification != nil {
		notification := c.notification
		c.notification = nil
		return notification, nil
	}
	if c.waitErr != nil {
		return nil, c.waitErr
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*subscriberTestConn) Close(context.Context) error { return nil }

func TestChangeSubscriber_RunWaitsForPeriodicResyncToExit(t *testing.T) {
	resyncStarted := make(chan struct{})
	resyncExited := make(chan struct{})
	var resyncCount atomic.Int32
	subscriber := &ChangeSubscriber{
		DSN:               "postgres://test",
		ReconcileInterval: time.Millisecond,
		Handlers: Handlers{
			OnResync: func(ctx context.Context) error {
				if resyncCount.Add(1) == 1 {
					return nil
				}
				close(resyncStarted)
				<-ctx.Done()
				time.Sleep(25 * time.Millisecond)
				close(resyncExited)
				return nil
			},
		},
		connect: func(context.Context, string) (subscriberConnection, error) {
			return &subscriberTestConn{}, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		subscriber.Run(ctx)
	}()
	select {
	case <-resyncStarted:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("periodic resync did not start")
	}
	cancel()
	select {
	case <-done:
		select {
		case <-resyncExited:
		default:
			t.Fatal("Run returned before periodic resync exited")
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber did not stop after cancellation")
	}
}

func TestChangeSubscriber_RunWaitsForDispatchHandlerToExit(t *testing.T) {
	handlerStarted := make(chan struct{})
	handlerExited := make(chan struct{})
	subscriber := &ChangeSubscriber{
		DSN:               "postgres://test",
		ReconcileInterval: time.Hour,
		Handlers: Handlers{
			OnAuthChanged: func(ctx context.Context, _ string) error {
				close(handlerStarted)
				<-ctx.Done()
				time.Sleep(25 * time.Millisecond)
				close(handlerExited)
				return nil
			},
		},
		connect: func(context.Context, string) (subscriberConnection, error) {
			return &subscriberTestConn{notification: &pgconn.Notification{
				Channel: ChannelAuthChanged,
				Payload: "auth-1",
			}}, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		subscriber.Run(ctx)
	}()
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("dispatch handler did not start")
	}
	cancel()
	select {
	case <-done:
		select {
		case <-handlerExited:
		default:
			t.Fatal("Run returned before dispatch handler exited")
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber did not stop after cancellation")
	}
}

func TestChangeSubscriber_ResyncsAfterEverySuccessfulListen(t *testing.T) {
	connections := []subscriberConnection{
		&subscriberTestConn{waitErr: errors.New("connection lost")},
		&subscriberTestConn{},
	}
	var connectCount atomic.Int32
	var resyncCount atomic.Int32
	subscriber := &ChangeSubscriber{
		DSN:               "postgres://test",
		Backoffs:          []time.Duration{time.Millisecond},
		ReconcileInterval: time.Hour,
		Handlers: Handlers{
			OnResync: func(context.Context) error {
				resyncCount.Add(1)
				return nil
			},
		},
		connect: func(context.Context, string) (subscriberConnection, error) {
			index := int(connectCount.Add(1)) - 1
			if index >= len(connections) {
				return connections[len(connections)-1], nil
			}
			return connections[index], nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		subscriber.Run(ctx)
	}()
	waitForAtomicCount(t, &resyncCount, 2)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("subscriber did not stop after cancellation")
	}
}

func TestChangeSubscriber_PeriodicallyReconcilesWithoutNotifications(t *testing.T) {
	var resyncCount atomic.Int32
	subscriber := &ChangeSubscriber{
		DSN:               "postgres://test",
		ReconcileInterval: 5 * time.Millisecond,
		Handlers: Handlers{
			OnResync: func(context.Context) error {
				resyncCount.Add(1)
				return nil
			},
		},
		connect: func(context.Context, string) (subscriberConnection, error) {
			return &subscriberTestConn{}, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		subscriber.Run(ctx)
	}()
	waitForAtomicCount(t, &resyncCount, 2)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("subscriber did not stop after cancellation")
	}
}

func waitForAtomicCount(t *testing.T, count *atomic.Int32, want int32) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for count.Load() < want {
		select {
		case <-deadline.C:
			t.Fatalf("callback count = %d, want at least %d", count.Load(), want)
		case <-ticker.C:
		}
	}
}
