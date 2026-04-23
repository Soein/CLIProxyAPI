package auth

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// fakeStore implements Store. When indexed=true it also implements
// authByIDStore so ReloadByID takes the fast path.
type fakeStore struct {
	mu      sync.Mutex
	all     map[string]*Auth
	indexed bool
	getErr  error
	calls   struct {
		list    int
		getByID int
	}
}

func (f *fakeStore) List(_ context.Context) ([]*Auth, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls.list++
	out := make([]*Auth, 0, len(f.all))
	for _, a := range f.all {
		out = append(out, a.Clone())
	}
	return out, nil
}

func (f *fakeStore) Save(_ context.Context, _ *Auth) (string, error) { return "", nil }
func (f *fakeStore) Delete(_ context.Context, _ string) error        { return nil }

// Indexed variant exposes GetByID so ReloadByID can skip the full List.
type fakeIndexedStore struct {
	*fakeStore
}

func (f *fakeIndexedStore) GetByID(_ context.Context, id string) (*Auth, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls.getByID++
	if f.getErr != nil {
		return nil, f.getErr
	}
	a, ok := f.all[id]
	if !ok {
		return nil, nil
	}
	return a.Clone(), nil
}

func newManagerWithStore(store Store) *Manager {
	m := NewManager(store, &RoundRobinSelector{}, nil)
	return m
}

func TestReloadByID_InsertsThroughFastPath(t *testing.T) {
	inner := &fakeStore{all: map[string]*Auth{
		"codex-1": {ID: "codex-1", Provider: "codex"},
	}}
	store := &fakeIndexedStore{fakeStore: inner}
	m := newManagerWithStore(store)

	if err := m.ReloadByID(context.Background(), "codex-1"); err != nil {
		t.Fatalf("ReloadByID: %v", err)
	}
	if inner.calls.list != 0 {
		t.Fatalf("expected no List call when GetByID available, got %d", inner.calls.list)
	}
	if inner.calls.getByID != 1 {
		t.Fatalf("expected 1 GetByID call, got %d", inner.calls.getByID)
	}
	if _, ok := m.GetByID("codex-1"); !ok {
		t.Fatal("expected codex-1 to be present in manager cache")
	}
}

func TestReloadByID_DropsOnMissingRow(t *testing.T) {
	inner := &fakeStore{all: map[string]*Auth{
		"codex-1": {ID: "codex-1", Provider: "codex"},
	}}
	store := &fakeIndexedStore{fakeStore: inner}
	m := newManagerWithStore(store)
	// Warm the cache.
	_ = m.ReloadByID(context.Background(), "codex-1")
	if _, ok := m.GetByID("codex-1"); !ok {
		t.Fatal("precondition: cache should contain codex-1")
	}

	// Simulate row deletion in the store.
	inner.mu.Lock()
	delete(inner.all, "codex-1")
	inner.mu.Unlock()

	if err := m.ReloadByID(context.Background(), "codex-1"); err != nil {
		t.Fatalf("ReloadByID: %v", err)
	}
	if _, ok := m.GetByID("codex-1"); ok {
		t.Fatal("expected codex-1 to be evicted after row removal")
	}
}

func TestReloadByID_FallbackToLoadWhenNoGetByID(t *testing.T) {
	store := &fakeStore{all: map[string]*Auth{
		"codex-1": {ID: "codex-1", Provider: "codex"},
	}}
	m := newManagerWithStore(store)

	if err := m.ReloadByID(context.Background(), "codex-1"); err != nil {
		t.Fatalf("ReloadByID: %v", err)
	}
	// Basic-store path must have gone through List.
	if store.calls.list == 0 {
		t.Fatal("expected List fallback when store has no GetByID")
	}
	if _, ok := m.GetByID("codex-1"); !ok {
		t.Fatal("expected codex-1 to be loaded via fallback Load")
	}
}

func TestReloadByID_EmptyPayloadFallsBackToFullLoad(t *testing.T) {
	inner := &fakeStore{all: map[string]*Auth{
		"codex-1": {ID: "codex-1", Provider: "codex"},
	}}
	store := &fakeIndexedStore{fakeStore: inner}
	m := newManagerWithStore(store)

	if err := m.ReloadByID(context.Background(), ""); err != nil {
		t.Fatalf("ReloadByID(\"\"): %v", err)
	}
	if inner.calls.list != 1 {
		t.Fatalf("expected 1 List call for empty id, got %d", inner.calls.list)
	}
	if inner.calls.getByID != 0 {
		t.Fatalf("expected no GetByID call for empty id, got %d", inner.calls.getByID)
	}
}

func TestReloadByID_SurfacesStoreError(t *testing.T) {
	want := errors.New("boom")
	inner := &fakeStore{all: map[string]*Auth{}, getErr: want}
	store := &fakeIndexedStore{fakeStore: inner}
	m := newManagerWithStore(store)

	err := m.ReloadByID(context.Background(), "codex-1")
	if !errors.Is(err, want) {
		t.Fatalf("expected wrapped boom error, got %v", err)
	}
}
