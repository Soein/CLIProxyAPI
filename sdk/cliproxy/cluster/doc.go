// Package cluster provides multi-node coordination primitives used when the
// cli-proxy-api server runs in cluster mode against a shared PostgreSQL
// instance:
//
//   - LeaderElector: pg_try_advisory_lock(1, 1) keeps one process holding a
//     server-side lock so singleton background loops (auto refresh, usage
//     automations, model updater) run on exactly one replica. Heartbeat rows
//     are written to cluster_nodes for operator visibility.
//
//   - PgAuthRefreshLocker: pg_try_advisory_lock(2, hash32(authID)) serializes
//     OAuth token refresh for a given auth across replicas. Implements
//     auth.AuthRefreshLocker which Manager consumes via SetAuthRefreshLocker.
//
//   - ChangeSubscriber: pgx-based LISTEN loop for the cliproxy_auth_changed
//     and cliproxy_config_changed channels. Integration with Manager is left
//     to the host (cmd/server wiring) so this package has no hard dependency
//     on the auth package internals.
//
// All facilities are opt-in: in single-instance deployments the host simply
// does not construct any of these and Manager keeps its original behavior.
package cluster
