package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrAuthStoreConflict reports that a durable auth generation no longer
// matches the caller's snapshot. Callers must reload authoritative state;
// retrying the same snapshot can overwrite a newer credential.
var ErrAuthStoreConflict = errors.New("auth store generation conflict")

// ErrAuthStoreDeleted reports that an auth ID is protected by a durable
// tombstone. Ordinary saves must never recreate it.
var ErrAuthStoreDeleted = errors.New("auth store record deleted")

// ErrAuthStoreCommitUnknown reports that a durable mutation reached Commit,
// but the store could not determine whether PostgreSQL committed or rolled it
// back. Callers must reload authoritative state before publishing or routing
// the affected auths.
var ErrAuthStoreCommitUnknown = errors.New("auth store commit outcome unknown")

// AuthStoreCommitUnknownError preserves the generations returned by the DML
// before Commit acknowledgement and marker verification both became
// unavailable. A candidate is not proof of commit; it is retained so callers
// can correlate the uncertain write with the authoritative row they reload.
type AuthStoreCommitUnknownError struct {
	CandidateGenerations map[string]uint64
	Cause                error
}

func (e *AuthStoreCommitUnknownError) Error() string {
	if e == nil || e.Cause == nil {
		return ErrAuthStoreCommitUnknown.Error()
	}
	return fmt.Sprintf("%s: %v", ErrAuthStoreCommitUnknown, e.Cause)
}

func (e *AuthStoreCommitUnknownError) Unwrap() []error {
	if e == nil || e.Cause == nil {
		return []error{ErrAuthStoreCommitUnknown}
	}
	return []error{ErrAuthStoreCommitUnknown, e.Cause}
}

// NewAuthStoreCommitUnknown constructs an outcome-unknown error while copying
// the candidate map so later snapshot mutation cannot change error metadata.
func NewAuthStoreCommitUnknown(candidates map[string]uint64, cause error) error {
	copied := make(map[string]uint64, len(candidates))
	for id, generation := range candidates {
		id = strings.TrimSpace(id)
		if id != "" && generation > 0 {
			copied[id] = generation
		}
	}
	return &AuthStoreCommitUnknownError{CandidateGenerations: copied, Cause: cause}
}

// AuthStoreCommitCandidateGeneration returns the generation produced by DML
// for the exact normalized id anywhere in an unknown commit error tree.
func AuthStoreCommitCandidateGeneration(err error, id string) (uint64, bool) {
	id = strings.TrimSpace(id)
	if err == nil || id == "" {
		return 0, false
	}

	candidates := make(map[string]uint64)
	var collect func(error)
	collect = func(current error) {
		if current == nil {
			return
		}
		if unknown, ok := current.(*AuthStoreCommitUnknownError); ok && unknown != nil {
			for candidateID, generation := range unknown.CandidateGenerations {
				candidateID = strings.TrimSpace(candidateID)
				if candidateID != "" && generation > candidates[candidateID] {
					candidates[candidateID] = generation
				}
			}
		}
		switch wrapped := current.(type) {
		case interface{ Unwrap() []error }:
			for _, nested := range wrapped.Unwrap() {
				collect(nested)
			}
		case interface{ Unwrap() error }:
			collect(wrapped.Unwrap())
		}
	}
	collect(err)

	generation, ok := candidates[id]
	if !ok || generation == 0 {
		return 0, false
	}
	return generation, true
}

// Store abstracts persistence of Auth state across restarts.
type Store interface {
	// List returns all auth records stored in the backend.
	List(ctx context.Context) ([]*Auth, error)
	// Save persists the provided auth record, replacing any existing one with same ID.
	Save(ctx context.Context, auth *Auth) (string, error)
	// Delete removes the auth record identified by id.
	Delete(ctx context.Context, id string) error
}

// VersionedAuthStore adds compare-and-swap persistence to Store. Generation
// zero means the caller is creating the row for the first time; it never
// updates an existing row. Positive generations update only the matching
// active row. Implementations return the committed generation.
type VersionedAuthStore interface {
	SaveVersioned(ctx context.Context, auth *Auth, expectedGeneration uint64) (path string, generation uint64, err error)
}

// AuthTombstoneStore durably removes an auth while preserving its generation.
type AuthTombstoneStore interface {
	Tombstone(ctx context.Context, id string, expectedGeneration uint64) (generation uint64, err error)
}

// AuthRestoreStore is the explicit path for creating a new auth or restoring
// a tombstoned one. SaveVersioned cannot restore.
type AuthRestoreStore interface {
	// expectedGeneration is zero for insert-only create. Restoring a
	// tombstone requires its exact positive generation.
	Restore(ctx context.Context, auth *Auth, expectedGeneration uint64) (path string, generation uint64, err error)
}

// AuthDeletedMirrorStore removes a local auth mirror only after the writer
// authoritatively confirms the row is still tombstoned. Cluster subscribers
// use this optional capability after a remote delete notification.
type AuthDeletedMirrorStore interface {
	ScrubDeletedAuthMirror(ctx context.Context, id string) error
}

// AuthMirrorReconciler converges PostgreSQL-backed local mirror files with the
// writer. A single-ID reconcile installs current active content or scrubs a
// tombstoned/missing mirror; the full reconcile also discovers stale local
// files that have no corresponding Manager runtime entry.
type AuthMirrorReconciler interface {
	ReconcileAuthMirror(ctx context.Context, id string) error
	ReconcileAuthMirrors(ctx context.Context) error
}

// AuthLifecycleState is a writer-backed view of one auth row, including
// tombstones that GetByID intentionally hides.
type AuthLifecycleState struct {
	Exists           bool
	Deleted          bool
	Generation       uint64
	LifecycleVersion uint64
	UpdatedAt        time.Time
	DeletedAt        time.Time
}

// AuthLifecycleStore provides the lifecycle fence used by long-running
// explicit login/import operations. Fence and row lifecycle versions must be
// allocated by one authoritative, commit-ordered sequence.
type AuthLifecycleStore interface {
	AuthLifecycleFence(ctx context.Context) (uint64, error)
	GetAuthLifecycle(ctx context.Context, id string) (AuthLifecycleState, error)
}

type explicitAuthOperationFenceKey struct{}

// BeginExplicitAuthOperation captures a writer-clock fence before a
// potentially long login, upload, or import begins. A tombstone or credential
// update committed after this fence wins over the older operation.
func BeginExplicitAuthOperation(ctx context.Context, store Store) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ExplicitAuthOperationFence(ctx); ok {
		return ctx, nil
	}
	lifecycleStore, ok := store.(AuthLifecycleStore)
	if !ok {
		return ctx, nil
	}
	fence, errFence := lifecycleStore.AuthLifecycleFence(ctx)
	if errFence != nil {
		return ctx, fmt.Errorf("read auth lifecycle fence: %w", errFence)
	}
	if fence == 0 {
		return ctx, errors.New("auth lifecycle fence is zero")
	}
	return WithExplicitAuthOperationFence(ctx, fence), nil
}

// WithExplicitAuthOperationFence restores a previously captured fence, for
// example when an OAuth session completes on a later polling request.
func WithExplicitAuthOperationFence(ctx context.Context, fence uint64) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if fence == 0 {
		return ctx
	}
	return context.WithValue(ctx, explicitAuthOperationFenceKey{}, fence)
}

// ExplicitAuthOperationFence returns the lifecycle fence carried by ctx.
func ExplicitAuthOperationFence(ctx context.Context) (uint64, bool) {
	if ctx == nil {
		return 0, false
	}
	fence, ok := ctx.Value(explicitAuthOperationFenceKey{}).(uint64)
	return fence, ok && fence > 0
}

// AuthByIDStore reads one authoritative active auth row. Tombstones are
// reported as nil so explicit persistence can distinguish active CAS updates
// from create/restore operations.
type AuthByIDStore interface {
	GetByID(ctx context.Context, id string) (*Auth, error)
}

// AuthAuthoritativeListStore returns a writer-backed snapshot for lifecycle
// operations that must not miss rows because of read-replica lag.
type AuthAuthoritativeListStore interface {
	ListAuthoritative(ctx context.Context) ([]*Auth, error)
}

// AuthAuthoritativeState is one writer-serialized auth row. Exists=false
// distinguishes a missing row from a tombstone; Auth is populated only for a
// valid active row. Generation is retained for all existing rows.
type AuthAuthoritativeState struct {
	Auth       *Auth
	Exists     bool
	Deleted    bool
	Generation uint64
}

// AuthAuthoritativeBatchStore runs finalize against one writer snapshot while
// the store's lifecycle serialization lock is held. Implementations must
// include an entry for every normalized requested ID, including missing rows.
// finalize must not perform store I/O because it executes inside the store
// transaction.
type AuthAuthoritativeBatchStore interface {
	WithAuthoritativeAuthBatch(ctx context.Context, ids []string, finalize func(map[string]AuthAuthoritativeState) error) error
}

// PersistExplicitAuth persists a credential produced by an explicit user
// action such as login, upload, or import. Active rows are updated with the
// generation just read from the authoritative store; missing or tombstoned
// rows use Restore. Background runtime and watcher paths must not call this.
func PersistExplicitAuth(ctx context.Context, store Store, auth *Auth) (string, error) {
	if store == nil || auth == nil {
		return "", nil
	}
	versioned, hasVersioned := store.(VersionedAuthStore)
	if hasVersioned && auth.StoreGeneration() > 0 {
		path, generation, errSave := versioned.SaveVersioned(ctx, auth, auth.StoreGeneration())
		if errSave == nil {
			auth.SetStoreGeneration(generation)
		}
		return path, errSave
	}
	if hasVersioned {
		if lifecycleStore, ok := store.(AuthLifecycleStore); ok {
			state, errRead := lifecycleStore.GetAuthLifecycle(ctx, auth.ID)
			if errRead != nil {
				return "", errRead
			}
			if state.Exists {
				fence, hasFence := ExplicitAuthOperationFence(ctx)
				if state.Deleted {
					if !hasFence || state.LifecycleVersion > fence {
						return "", fmt.Errorf("%w for %s: tombstone is newer than explicit operation", ErrAuthStoreDeleted, auth.ID)
					}
					restoreStore, canRestore := store.(AuthRestoreStore)
					if !canRestore {
						return "", fmt.Errorf("%w for %s: store cannot restore tombstone", ErrAuthStoreDeleted, auth.ID)
					}
					path, generation, errRestore := restoreStore.Restore(ctx, auth, state.Generation)
					if errRestore == nil {
						auth.SetStoreGeneration(generation)
					}
					return path, errRestore
				}
				if hasFence && state.LifecycleVersion > fence {
					return "", fmt.Errorf("%w for %s: active row changed after explicit operation began", ErrAuthStoreConflict, auth.ID)
				}
				path, generation, errSave := versioned.SaveVersioned(ctx, auth, state.Generation)
				if errSave == nil {
					auth.SetStoreGeneration(generation)
				}
				return path, errSave
			}
			if restoreStore, canRestore := store.(AuthRestoreStore); canRestore {
				path, generation, errRestore := restoreStore.Restore(ctx, auth, 0)
				if errRestore == nil {
					auth.SetStoreGeneration(generation)
				}
				return path, errRestore
			}
		}
		if reader, ok := store.(AuthByIDStore); ok {
			current, errRead := reader.GetByID(ctx, auth.ID)
			if errRead != nil {
				return "", errRead
			}
			if current != nil {
				expectedGeneration := current.StoreGeneration()
				path, generation, errSave := versioned.SaveVersioned(ctx, auth, expectedGeneration)
				if errSave == nil {
					auth.SetStoreGeneration(generation)
				}
				return path, errSave
			}
		}
		if restoreStore, ok := store.(AuthRestoreStore); ok {
			path, generation, errRestore := restoreStore.Restore(ctx, auth, 0)
			if errRestore == nil {
				auth.SetStoreGeneration(generation)
			}
			return path, errRestore
		}
		path, generation, errSave := versioned.SaveVersioned(ctx, auth, 0)
		if errSave == nil {
			auth.SetStoreGeneration(generation)
		}
		return path, errSave
	}
	return store.Save(ctx, auth)
}

// GetStore exposes the underlying store so hosts can feature-detect optional
// capabilities (e.g. Postgres-backed stores also implement DB()/DSN() for the
// cluster package). Returns nil when the Manager has no store wired.
func (m *Manager) GetStore() Store {
	if m == nil {
		return nil
	}
	return m.store
}
