// Package cluster provides multi-node coordination primitives used when the
// cli-proxy-api server runs in cluster mode against a shared PostgreSQL
// instance. It covers leader election via pg_try_advisory_lock, LISTEN/NOTIFY
// change propagation, and cluster membership heartbeats.
//
// Phase 1 (this package) is intentionally minimal: a leader elector is
// provided but Manager integration and change subscriber live in Phase 2.
package cluster
