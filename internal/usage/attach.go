package usage

import (
	"context"
	"database/sql"
	"strings"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

// AttachPGSink registers a PG-backed usage sink on the global usage manager
// and starts its background flusher. Returns the live PGSink (so callers
// can drain it on Shutdown) or nil when registration is skipped.
//
// Skip conditions:
//   - mode == "memory" (the default; no PG persistence wanted)
//   - db == nil (cluster bootstrap got nil pool — should not happen)
//
// In modes "dual" and "pg" the sink is registered ALONGSIDE the existing
// in-memory plugin. The in-memory plugin still serves /usage in modes
// memory and dual; mode pg switches the read path to PG (handled at the
// management handler layer, not here).
func AttachPGSink(ctx context.Context, db *sql.DB, nodeID string, mode string, opts PGSinkOptions) (*PGSink, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" || mode == "memory" {
		return nil, nil
	}
	if db == nil {
		log.Warn("usage AttachPGSink: nil DB — falling back to memory")
		return nil, nil
	}
	if mode != "dual" && mode != "pg" {
		log.Warnf("usage AttachPGSink: unknown mode %q — falling back to memory", mode)
		return nil, nil
	}

	// *PGStore satisfies the flushStore interface directly (its FlushBatch
	// signature matches), so no adapter is needed.
	sink := NewPGSink(NewPGStore(db), nodeID, opts)
	sink.Start(ctx)
	coreusage.RegisterPlugin(sink)
	log.Infof(
		"usage: PGSink registered node=%s mode=%s flush=%s batch=%d",
		nodeID, mode, opts.FlushInterval, opts.BatchSize,
	)
	return sink, nil
}

// ParseFlushInterval parses a duration string with a fallback. Used by
// cluster_bootstrap so a misconfigured FlushInterval doesn't take down
// the service.
func ParseFlushInterval(s string) time.Duration {
	if d, err := time.ParseDuration(strings.TrimSpace(s)); err == nil && d > 0 {
		return d
	}
	return 10 * time.Second
}
