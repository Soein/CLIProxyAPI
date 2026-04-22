package cluster

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
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
// backoff on errors.
type ChangeSubscriber struct {
	DSN      string
	Handlers Handlers
	Backoffs []time.Duration // default: 1s, 3s, 10s, 30s
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
	attempt := 0
	for {
		if err := s.runOnce(ctx); err != nil && ctx.Err() == nil {
			wait := backoffs[attempt]
			if attempt < len(backoffs)-1 {
				attempt++
			}
			log.WithError(err).Warnf("subscriber error; reconnecting in %s", wait)
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			continue
		}
		if ctx.Err() != nil {
			return
		}
		attempt = 0
	}
}

func (s *ChangeSubscriber) runOnce(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, s.DSN)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	for _, ch := range []string{ChannelAuthChanged, ChannelConfigChanged} {
		if _, err := conn.Exec(ctx, "LISTEN "+ch); err != nil {
			return err
		}
	}

	for {
		notif, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
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
