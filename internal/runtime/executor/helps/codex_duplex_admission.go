package helps

import "sync"

// duplexAdmissionRoot tracks the underlying dispatch admission release callback
// with reference counting. The release callback is invoked at most once, strictly
// after the last reference is released, and outside any helper or metadata locks.
type duplexAdmissionRoot struct {
	mu       sync.Mutex
	refs     int
	release  func()
	released bool
}

func (r *duplexAdmissionRoot) retainRef() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released || r.refs <= 0 {
		return false
	}
	r.refs++
	return true
}

func (r *duplexAdmissionRoot) releaseRef() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.refs--
	var toCall func()
	if r.refs <= 0 && !r.released {
		r.released = true
		toCall = r.release
	}
	r.mu.Unlock()

	if toCall != nil {
		toCall()
	}
}

// CodexDuplexLease represents an individual owner's handle to an admitted dispatch
// reservation. Multiple handles can point to the same underlying admission root
// via Retain. Releasing a handle is thread-safe and idempotent for that handle.
// When all handles pointing to a root have been released, the root's release
// callback is executed exactly once outside any helper locks.
//
// Transfer semantics:
// Ownership of a handle can be moved directly or passed into NewCodexDuplexLeaseGroup.
// To share concurrent ownership across multiple components, callers must call Retain
// to obtain an independent handle.
//
// Nil leases represent unmanaged or borrowed admissions (e.g. the initial conductor
// admission). Calling Retain on a nil lease safely returns nil.
type CodexDuplexLease struct {
	mu       sync.Mutex
	released bool
	root     *duplexAdmissionRoot
}

// NewCodexDuplexLease creates a new CodexDuplexLease handle with an initial reference
// count of 1. A nil callback produces a nil lease.
func NewCodexDuplexLease(release func()) *CodexDuplexLease {
	if release == nil {
		return nil
	}
	return &CodexDuplexLease{
		root: &duplexAdmissionRoot{
			release: release,
			refs:    1,
		},
	}
}

// Retain creates an independent handle referencing the same underlying admission root
// as this lease, incrementing the root reference count.
//
// Retain returns nil if the receiver is nil, if the receiver handle has already been
// released, or if the underlying root has already been fully released. It never
// resurrects a released handle or root.
func (l *CodexDuplexLease) Retain() *CodexDuplexLease {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	if l.root == nil {
		return nil
	}
	if !l.root.retainRef() {
		return nil
	}
	return &CodexDuplexLease{root: l.root}
}

// Release releases this handle. It is idempotent: subsequent calls on the same
// handle are no-ops. If this handle was the last reference to the root, the root's
// release callback is executed.
func (l *CodexDuplexLease) Release() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return
	}
	l.released = true
	root := l.root
	l.mu.Unlock()

	if root != nil {
		root.releaseRef()
	}
}

// ReleaseCodexDuplexLeases releases all non-nil leases in the slice.
func ReleaseCodexDuplexLeases(leases []*CodexDuplexLease) {
	for _, l := range leases {
		if l != nil {
			l.Release()
		}
	}
}

// NewCodexDuplexLeaseGroup combines multiple leases into a single composite lease.
// Ownership of each non-nil lease is transferred to the group. When the group lease
// is released, all non-nil child leases are released exactly once. If exactly one
// non-nil lease is provided, it is returned directly.
func NewCodexDuplexLeaseGroup(leases ...*CodexDuplexLease) *CodexDuplexLease {
	var nonNil []*CodexDuplexLease
	for _, l := range leases {
		if l != nil {
			nonNil = append(nonNil, l)
		}
	}
	if len(nonNil) == 0 {
		return nil
	}
	if len(nonNil) == 1 {
		return nonNil[0]
	}
	return NewCodexDuplexLease(func() {
		ReleaseCodexDuplexLeases(nonNil)
	})
}
