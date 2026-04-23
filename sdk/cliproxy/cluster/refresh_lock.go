package cluster

import (
	"context"
	"database/sql"
	"hash/fnv"
)

// PgAuthRefreshLocker implements sdk/cliproxy/auth.AuthRefreshLocker using
// PostgreSQL pg_try_advisory_lock. The lock key is derived from the auth ID
// via FNV-1a to spread collisions across the 64-bit space. Each successful
// TryLock holds a dedicated *sql.Conn so the lock is released when Unlock or
// Close runs — a process crash also releases it server-side.
type PgAuthRefreshLocker struct {
	db *sql.DB
}

// NewPgAuthRefreshLocker returns a locker backed by the given *sql.DB. The
// pool is expected to be the same one used by the rest of the store layer.
func NewPgAuthRefreshLocker(db *sql.DB) *PgAuthRefreshLocker {
	return &PgAuthRefreshLocker{db: db}
}

// TryLock attempts a non-blocking advisory lock scoped to authID.
//
//	ok == true  -> caller holds the lock, MUST call release()
//	ok == false -> another node holds it, caller should skip this round
//	err != nil  -> db error; caller should log and proceed without lock
func (p *PgAuthRefreshLocker) TryLock(ctx context.Context, authID string) (func(), bool, error) {
	if p == nil || p.db == nil {
		return func() {}, true, nil
	}
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}
	key := authIDLockKey(authID)
	var got bool
	if err := conn.QueryRowContext(ctx,
		"SELECT pg_try_advisory_lock($1)", key,
	).Scan(&got); err != nil {
		_ = conn.Close()
		return nil, false, err
	}
	if !got {
		_ = conn.Close()
		return nil, false, nil
	}
	release := func() {
		// Background context so release succeeds even if caller's ctx was
		// cancelled — otherwise we'd leak the session-level lock.
		_, _ = conn.ExecContext(context.Background(),
			"SELECT pg_advisory_unlock($1)", key)
		_ = conn.Close()
	}
	return release, true, nil
}

// authIDLockKey maps a string auth ID to a 64-bit signed integer Postgres
// advisory-lock key. "cliproxy:auth:" prefix keeps us out of the key space a
// user might choose for other purposes.
func authIDLockKey(id string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("cliproxy:auth:"))
	_, _ = h.Write([]byte(id))
	return int64(h.Sum64())
}
