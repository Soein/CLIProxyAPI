package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	log "github.com/sirupsen/logrus"
)

// ChannelAuthChanged and ChannelConfigChanged are the Postgres LISTEN channels
// used by the persistence layer to broadcast mutations. Handlers are invoked
// with the NOTIFY payload (auth ID for auth changes, empty for config).
const (
	ChannelAuthChanged       = "cliproxy_auth_changed"
	ChannelConfigChanged     = "cliproxy_config_changed"
	defaultReconcileInterval = 30 * time.Second
)

// Handlers is the callback set used by ChangeSubscriber.
type Handlers struct {
	OnAuthChanged   func(ctx context.Context, authID string) error
	OnConfigChanged func(ctx context.Context) error
	// OnResync performs a full reconciliation after LISTEN is established
	// and periodically thereafter. It closes gaps caused by reconnects or
	// notifications dropped from the bounded dispatch queue.
	OnResync func(ctx context.Context) error
}

type SubscriberConnection interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	WaitForNotification(ctx context.Context) (*pgconn.Notification, error)
	Close(ctx context.Context) error
}

type subscriberConnection = SubscriberConnection

// ChangeSubscriber connects to Postgres with pgx (which supports LISTEN) and
// dispatches NOTIFY payloads to the provided handlers. It reconnects with
// backoff on errors. Dispatch runs on a worker goroutine with a bounded
// buffer so a slow handler cannot stall the NOTIFY consumer — critical for
// avoiding server-side buffer overflow and disconnection.
type ChangeSubscriber struct {
	DSN      string
	Handlers Handlers
	Backoffs []time.Duration // default: 1s, 3s, 10s, 30s
	// ReconcileInterval bounds how long state can remain stale after a
	// missed or dropped notification. Values <= 0 default to 30 seconds.
	ReconcileInterval time.Duration

	// DispatchBuffer is the capacity of the in-process NOTIFY queue. When
	// full, events are dropped with a warning rather than blocking the
	// consumer loop. Defaults to 256 when zero.
	DispatchBuffer int
	// Connect overrides the pgx connection factory.
	Connect func(ctx context.Context, dsn string) (SubscriberConnection, error)

	connect        func(ctx context.Context, dsn string) (SubscriberConnection, error)
	wait           func(ctx context.Context, delay time.Duration) bool
	resyncMu       sync.Mutex
	readyInit      sync.Once
	readySignal    sync.Once
	ready          chan struct{}
	startupErrInit sync.Once
	startupErr     chan error
}

// Ready closes after both LISTEN statements were accepted and the first
// authoritative OnResync completed successfully. This
// ordering closes the startup gap without losing notifications: changes that
// happen during the resync remain queued on the already-listening connection.
func (s *ChangeSubscriber) Ready() <-chan struct{} {
	if s == nil {
		result := make(chan struct{})
		close(result)
		return result
	}
	s.readyInit.Do(func() {
		s.ready = make(chan struct{})
	})
	return s.ready
}

func (s *ChangeSubscriber) signalReady() {
	s.Ready()
	s.readySignal.Do(func() {
		close(s.ready)
	})
}

// StartupErrors reports unrecoverable subscriber configuration errors. Normal
// connection and resync failures are retried with backoff and do not make the
// subscriber ready.
func (s *ChangeSubscriber) StartupErrors() <-chan error {
	if s == nil {
		result := make(chan error, 1)
		result <- errors.New("cluster subscriber is nil")
		close(result)
		return result
	}
	s.startupErrInit.Do(func() { s.startupErr = make(chan error, 1) })
	return s.startupErr
}

func (s *ChangeSubscriber) signalStartupError(err error) {
	s.StartupErrors()
	select {
	case s.startupErr <- err:
	default:
	}
}

// Run blocks until ctx is cancelled.
func (s *ChangeSubscriber) Run(ctx context.Context) {
	if s == nil {
		return
	}
	if s.DSN == "" {
		s.signalStartupError(errors.New("cluster subscriber DSN is empty"))
		return
	}
	backoffs := s.Backoffs
	if len(backoffs) == 0 {
		backoffs = []time.Duration{time.Second, 3 * time.Second, 10 * time.Second, 30 * time.Second}
	}
	bufferSize := s.DispatchBuffer
	if bufferSize <= 0 {
		bufferSize = 256
	}

	// One long-lived worker drains dispatch events so handler latency never
	// blocks the LISTEN loop or the pgx event stream.
	events := make(chan *pgconn.Notification, bufferSize)
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		s.dispatchLoop(ctx, events)
	}()
	defer func() {
		close(events)
		workers.Wait()
	}()

	reconcileInterval := s.ReconcileInterval
	if reconcileInterval <= 0 {
		reconcileInterval = defaultReconcileInterval
	}
	if s.Handlers.OnResync != nil {
		workers.Add(1)
		go func() {
			defer workers.Done()
			s.reconcileLoop(ctx, reconcileInterval)
		}()
	}

	attempt := 0
	waitForRetry := s.wait
	if waitForRetry == nil {
		waitForRetry = func(ctx context.Context, delay time.Duration) bool {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(delay):
				return true
			}
		}
	}
	for {
		err := s.runOnce(ctx, events, func() error {
			if errResync := s.resync(ctx, "subscription established"); errResync != nil {
				return errResync
			}
			// Reset only after LISTEN and the authoritative resync both
			// succeed. Otherwise repeated resync failures must keep backing off.
			attempt = 0
			s.signalReady()
			return nil
		})
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			wait := backoffs[attempt]
			if attempt < len(backoffs)-1 {
				attempt++
			}
			log.WithError(err).Warnf("subscriber error; reconnecting in %s", wait)
			if !waitForRetry(ctx, wait) {
				return
			}
			continue
		}
	}
}

func (s *ChangeSubscriber) runOnce(ctx context.Context, events chan<- *pgconn.Notification, onListening func() error) error {
	connect := s.connect
	if s.Connect != nil {
		connect = s.Connect
	}
	if connect == nil {
		connect = func(ctx context.Context, dsn string) (subscriberConnection, error) {
			return pgx.Connect(ctx, dsn)
		}
	}
	conn, err := connect(ctx, s.DSN)
	if err != nil {
		return err
	}
	defer func() {
		// Close on a detached background ctx so cancellation still releases
		// the connection promptly server-side.
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	for _, ch := range []string{ChannelAuthChanged, ChannelConfigChanged} {
		if _, err := conn.Exec(ctx, "LISTEN "+ch); err != nil {
			return err
		}
	}
	if onListening != nil {
		if err := onListening(); err != nil {
			return fmt.Errorf("initial authoritative resync: %w", err)
		}
	}

	for {
		notif, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		select {
		case events <- notif:
		default:
			log.Warnf("subscriber: dispatch buffer full; dropping %s(%q)", notif.Channel, notif.Payload)
		}
	}
}

func (s *ChangeSubscriber) reconcileLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.resync(ctx, "periodic reconciliation"); err != nil {
				log.WithError(err).Warn("subscriber: periodic OnResync failed")
			}
		}
	}
}

func (s *ChangeSubscriber) resync(ctx context.Context, reason string) error {
	if s == nil || s.Handlers.OnResync == nil || ctx.Err() != nil {
		return ctx.Err()
	}
	s.resyncMu.Lock()
	defer s.resyncMu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err := s.Handlers.OnResync(ctx); err != nil {
		log.WithError(err).Warnf("subscriber: OnResync failed after %s", reason)
		return err
	}
	return nil
}

func (s *ChangeSubscriber) dispatchLoop(ctx context.Context, events <-chan *pgconn.Notification) {
	for notif := range events {
		s.dispatch(ctx, notif.Channel, notif.Payload)
	}
}

func (s *ChangeSubscriber) dispatch(ctx context.Context, channel, payload string) {
	switch channel {
	case ChannelAuthChanged:
		if s.Handlers.OnAuthChanged != nil {
			if err := s.Handlers.OnAuthChanged(ctx, payload); err != nil {
				log.WithError(err).Warnf("OnAuthChanged(%s) failed", payload)
			}
		}
	case ChannelConfigChanged:
		if s.Handlers.OnConfigChanged != nil {
			if err := s.Handlers.OnConfigChanged(ctx); err != nil {
				log.WithError(err).Warn("OnConfigChanged failed")
			}
		}
	}
}
