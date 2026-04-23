package cluster

import (
	"context"
	"database/sql"
	"errors"
	"hash/fnv"
	"time"
)

// authRefreshLockClass is the first argument of PostgreSQL's two-argument
// pg_try_advisory_lock. It namespaces per-auth locks away from the leader
// election lock (class=1), eliminating collision risk between them. The
// second argument is a per-auth hash derived from the auth ID.
const authRefreshLockClass int32 = 2

// releaseTimeout caps how long pg_advisory_unlock may run when the caller's
// context is already cancelled. Keeps defer release() from stalling the
// goroutine forever if PG is unresponsive.
const releaseTimeout = 5 * time.Second

// ErrNilDB is returned by TryLock on a zero-value locker to make misuse loud
// instead of silently granting a phantom lock.
var ErrNilDB = errors.New("cluster: PgAuthRefreshLocker has no *sql.DB")

// PgAuthRefreshLocker implements sdk/cliproxy/auth.AuthRefreshLocker using
// PostgreSQL pg_try_advisory_lock(int4, int4). Each successful TryLock holds a
// dedicated *sql.Conn so the lock is released by pg_advisory_unlock or by
// server-side session teardown if the process crashes.
type PgAuthRefreshLocker struct {
	db *sql.DB
}

// NewPgAuthRefreshLocker returns a locker backed by the given *sql.DB.
func NewPgAuthRefreshLocker(db *sql.DB) *PgAuthRefreshLocker {
	return &PgAuthRefreshLocker{db: db}
}

// TryLock attempts a non-blocking advisory lock scoped to authID.
//
//	ok == true  -> caller holds the lock, MUST call release()
//	ok == false -> another node holds it, caller should skip this round
//	err != nil  -> db error; caller should log and proceed without lock
//
// A nil receiver or zero-value locker returns ErrNilDB rather than silently
// succeeding — callers that legitimately want single-instance semantics
// should not install a locker at all.
func (p *PgAuthRefreshLocker) TryLock(ctx context.Context, authID string) (func(), bool, error) {
	if p == nil || p.db == nil {
		return nil, false, ErrNilDB
	}
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}
	classKey, idKey := authIDLockKeys(authID)
	var got bool
	if err := conn.QueryRowContext(ctx,
		"SELECT pg_try_advisory_lock($1, $2)", classKey, idKey,
	).Scan(&got); err != nil {
		_ = conn.Close()
		return nil, false, err
	}
	if !got {
		_ = conn.Close()
		return nil, false, nil
	}
	release := func() {
		// Use a bounded background context so we release even if the caller's
		// ctx is cancelled, but don't hang forever if PG is unresponsive.
		rctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
		defer cancel()
		_, _ = conn.ExecContext(rctx,
			"SELECT pg_advisory_unlock($1, $2)", classKey, idKey)
		_ = conn.Close()
	}
	return release, true, nil
}

// authIDLockKeys returns the (int4, int4) pair used as the advisory lock key
// for a given auth ID. Class is fixed to authRefreshLockClass so per-auth
// locks never collide with the leader lock (class=1). The second int is the
// low 32 bits of fnv1a64(authID).
func authIDLockKeys(id string) (int32, int32) {
	h := fnv.New64a()
	_, _ = h.Write([]byte("cliproxy:auth:"))
	_, _ = h.Write([]byte(id))
	return authRefreshLockClass, int32(h.Sum64())
}
