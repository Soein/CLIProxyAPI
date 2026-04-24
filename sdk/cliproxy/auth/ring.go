package auth

// Phase 4 Sprint 2: AuthRingView is the narrow interface Manager/scheduler
// consume to decide whether an auth is owned by the local replica. The
// concrete implementation lives in sdk/cliproxy/cluster.AuthRing — defining
// the interface here keeps the auth package free of a cluster import and
// preserves the existing cluster→auth dependency direction.
//
// Degradation semantics (implemented by cluster.AuthRing and RELIED ON by
// scheduler/refresh paths):
//
//   - nil ring or Ready()=false -> IsMine returns true for all IDs. The
//     replica behaves exactly like Phase 1-3 (no sharding). This is the
//     safe bootstrap window before cluster_nodes has been loaded.
//   - Ready()=true but local NodeID absent from membership -> IsMine
//     returns false. The replica has been drained/demoted; it stops
//     claiming auths so peers can take over cleanly.
//
// These rules exist so a black-hole bug (no replica serves auth X) is
// strictly harder to produce than a double-serve bug (two replicas both
// claim X for a brief window): double-serve is tolerated by Phase 2's
// advisory-lock protection, while black-holes surface as user-facing 500s.
type AuthRingView interface {
	// IsMine reports whether the current replica owns the given auth_id
	// under the current ring membership. See degradation semantics above.
	IsMine(authID string) bool

	// Ready reports whether the ring has been populated at least once and
	// contains at least one member. Used by callers that want to
	// distinguish bootstrap (IsMine=true degradation) from genuine
	// ownership decisions.
	Ready() bool
}
