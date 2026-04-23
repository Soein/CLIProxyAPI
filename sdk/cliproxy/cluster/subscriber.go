package cluster

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	log "github.com/sirupsen/logrus"
)

// ChannelAuthChanged and ChannelConfigChanged are the Postgres LISTEN channels
// used by the persistence layer to broadcast mutations. Handlers are invoked
// with the NOTIFY payload (auth ID for auth changes, empty for config).
const (
	ChannelAuthChanged   = "cliproxy_auth_changed"
	ChannelConfigChanged = "cliproxy_config_changed"
)

// Handlers is the callback set used by ChangeSubscriber.
type Handlers struct {
	OnAuthChanged   func(ctx context.Context, authID string) error
	OnConfigChanged func(ctx context.Context) error
}

// ChangeSubscriber connects to Postgres with pgx (which supports LISTEN) and
// dispatches NOTIFY payloads to the provided handlers. It reconnects with
// backoff on errors. Dispatch runs on a worker goroutine with a bounded
// buffer so a slow handler cannot stall the NOTIFY consumer — critical for
// avoiding server-side buffer overflow and disconnection.
type ChangeSubscriber struct {
	DSN      string
	Handlers Handlers
	Backoffs []time.Duration // default: 1s, 3s, 10s, 30s

	// DispatchBuffer is the capacity of the in-process NOTIFY queue. When
	// full, events are dropped with a warning rather than blocking the
	// consumer loop. Defaults to 256 when zero.
	DispatchBuffer int
}

// Run blocks until ctx is cancelled.
func (s *ChangeSubscriber) Run(ctx context.Context) {
	if s == nil || s.DSN == "" {
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
	go s.dispatchLoop(ctx, events)

	attempt := 0
	for {
		err := s.runOnce(ctx, events, func() {
			// Reset the backoff counter after we successfully receive at
			// least one notification — the previous failure is considered
			// cleared. Without this, transient errors cumulatively ramp
			// to the max backoff and never recover.
			attempt = 0
		})
		if ctx.Err() != nil {
			close(events)
			return
		}
		if err != nil {
			wait := backoffs[attempt]
			if attempt < len(backoffs)-1 {
				attempt++
			}
			log.WithError(err).Warnf("subscriber error; reconnecting in %s", wait)
			select {
			case <-ctx.Done():
				close(events)
				return
			case <-time.After(wait):
			}
			continue
		}
	}
}

func (s *ChangeSubscriber) runOnce(ctx context.Context, events chan<- *pgconn.Notification, onFirstNotif func()) error {
	conn, err := pgx.Connect(ctx, s.DSN)
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

	firstNotif := true
	for {
		notif, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if firstNotif {
			firstNotif = false
			if onFirstNotif != nil {
				onFirstNotif()
			}
		}
		select {
		case events <- notif:
		default:
			log.Warnf("subscriber: dispatch buffer full; dropping %s(%q)", notif.Channel, notif.Payload)
		}
	}
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
