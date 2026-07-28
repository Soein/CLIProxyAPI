package usage

import (
	"context"
	"strings"
	"sync"
	"time"
)

// AffinityOutcome describes how session affinity affected auth selection.
type AffinityOutcome string

const (
	AffinityOutcomeNone        = "none"
	AffinityOutcomeMiss        = "miss"
	AffinityOutcomeHit         = "hit"
	AffinityOutcomeFallbackHit = "fallback_hit"
	AffinityOutcomeFailover    = "failover"
)

// PhaseTimings contains an immutable snapshot of one upstream attempt. All
// durations are measured from either the request root or the attempt's
// upstream dispatch point, as documented on each field.
type PhaseTimings struct {
	// Attempt is the one-based attempt ordinal within the request.
	Attempt int
	// RequestElapsedToUpstreamStart is measured from request phase tracking start.
	RequestElapsedToUpstreamStart time.Duration
	// AuthSelection is the duration of auth selection for this attempt.
	AuthSelection time.Duration
	// ResponseHeaders is measured from upstream dispatch until headers arrive.
	ResponseHeaders time.Duration
	// FirstEvent is measured from upstream dispatch until the first protocol event.
	FirstEvent time.Duration
	// FirstSemanticToken is measured from upstream dispatch until user-visible content.
	FirstSemanticToken time.Duration
	// Terminal is measured from upstream dispatch until a terminal event.
	Terminal time.Duration
	// ResponseHeadersObserved distinguishes an immediate header response from no response.
	ResponseHeadersObserved bool
	// TransportReused reports whether the HTTP connection was reused.
	TransportReused bool
	// AffinityOutcome records the normalized session-affinity selection outcome.
	AffinityOutcome AffinityOutcome
	// TerminalKind identifies the terminal event, for example completed or failed.
	TerminalKind string
}

// AttemptSeed is the immutable request-level state captured when an upstream
// attempt starts. Executors normally consume it through NewExecutorUsageReporter.
type AttemptSeed struct {
	Attempt                       int
	RequestElapsedToUpstreamStart time.Duration
	AuthSelection                 time.Duration
	AffinityOutcome               AffinityOutcome
}

// PhaseTracker coordinates auth-selection and attempt timing for one request.
// It is safe for concurrent use.
type PhaseTracker struct {
	mu sync.Mutex

	now       func() time.Time
	startedAt time.Time
	attempt   int

	selectionGeneration uint64
	selectionActive     bool
	affinityOutcome     AffinityOutcome
	lastAuthSelection   time.Duration
	lastAffinityOutcome AffinityOutcome
}

// AuthSelection tracks one auth-selection interval. Complete is idempotent.
type AuthSelection struct {
	tracker    *PhaseTracker
	startedAt  time.Time
	generation uint64
	once       sync.Once
}

type phaseTrackerContextKey struct{}

// NewPhaseTracker starts request phase tracking at the current time.
func NewPhaseTracker() *PhaseTracker {
	return newPhaseTracker(time.Now)
}

func newPhaseTracker(now func() time.Time) *PhaseTracker {
	if now == nil {
		now = time.Now
	}
	startedAt := now()
	return &PhaseTracker{
		now:                 now,
		startedAt:           startedAt,
		affinityOutcome:     AffinityOutcomeNone,
		lastAffinityOutcome: AffinityOutcomeNone,
	}
}

// EnablePhases returns a context carrying a request-scoped PhaseTracker.
// Existing tracking is preserved so nested callers share attempt ordinals.
func EnablePhases(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if PhaseTrackerFromContext(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, phaseTrackerContextKey{}, NewPhaseTracker())
}

// WithPhaseTracker attaches tracker to ctx. A nil tracker leaves ctx unchanged.
func WithPhaseTracker(ctx context.Context, tracker *PhaseTracker) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if tracker == nil {
		return ctx
	}
	return context.WithValue(ctx, phaseTrackerContextKey{}, tracker)
}

// PhaseTrackerFromContext returns the request phase tracker, if enabled.
func PhaseTrackerFromContext(ctx context.Context) *PhaseTracker {
	if ctx == nil {
		return nil
	}
	tracker, _ := ctx.Value(phaseTrackerContextKey{}).(*PhaseTracker)
	return tracker
}

// BeginAuthSelection starts timing the next auth selection and resets its
// affinity outcome to none.
func (t *PhaseTracker) BeginAuthSelection() *AuthSelection {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	t.selectionGeneration++
	generation := t.selectionGeneration
	t.selectionActive = true
	t.affinityOutcome = AffinityOutcomeNone
	startedAt := t.now()
	t.mu.Unlock()
	return &AuthSelection{tracker: t, startedAt: startedAt, generation: generation}
}

// BeginAuthSelection starts auth-selection timing on the tracker in ctx.
func BeginAuthSelection(ctx context.Context) *AuthSelection {
	return PhaseTrackerFromContext(ctx).BeginAuthSelection()
}

// MarkAffinityOutcome records the current auth selection's normalized affinity outcome.
// Unknown values are stored as none to keep metrics cardinality bounded.
func (t *PhaseTracker) MarkAffinityOutcome(outcome string) {
	if t == nil {
		return
	}
	normalized := normalizeAffinityOutcome(outcome)
	t.mu.Lock()
	if t.selectionActive {
		t.affinityOutcome = normalized
	} else {
		t.lastAffinityOutcome = normalized
	}
	t.mu.Unlock()
}

// MarkAffinityOutcome records an affinity outcome on the tracker in ctx.
func MarkAffinityOutcome(ctx context.Context, outcome string) {
	PhaseTrackerFromContext(ctx).MarkAffinityOutcome(outcome)
}

// Complete records the selection duration and the latest affinity outcome.
func (s *AuthSelection) Complete() {
	if s == nil || s.tracker == nil {
		return
	}
	s.once.Do(func() {
		t := s.tracker
		t.mu.Lock()
		if t.selectionGeneration == s.generation {
			t.lastAuthSelection = nonNegativeDuration(t.now().Sub(s.startedAt))
			t.lastAffinityOutcome = t.affinityOutcome
			t.selectionActive = false
		}
		t.mu.Unlock()
	})
}

// BeginAttempt captures request-level phase state for a new upstream attempt.
func (t *PhaseTracker) BeginAttempt() AttemptSeed {
	if t == nil {
		return AttemptSeed{}
	}
	t.mu.Lock()
	now := t.now()
	t.attempt++
	seed := AttemptSeed{
		Attempt:                       t.attempt,
		RequestElapsedToUpstreamStart: nonNegativeDuration(now.Sub(t.startedAt)),
		AuthSelection:                 t.lastAuthSelection,
		AffinityOutcome:               t.lastAffinityOutcome,
	}
	t.lastAuthSelection = 0
	t.lastAffinityOutcome = AffinityOutcomeNone
	t.mu.Unlock()
	return seed
}

func normalizeAffinityOutcome(outcome string) AffinityOutcome {
	switch AffinityOutcome(strings.ToLower(strings.TrimSpace(outcome))) {
	case AffinityOutcomeMiss:
		return AffinityOutcomeMiss
	case AffinityOutcomeHit:
		return AffinityOutcomeHit
	case AffinityOutcomeFallbackHit:
		return AffinityOutcomeFallbackHit
	case AffinityOutcomeFailover:
		return AffinityOutcomeFailover
	default:
		return AffinityOutcomeNone
	}
}

func nonNegativeDuration(value time.Duration) time.Duration {
	if value < 0 {
		return 0
	}
	return value
}
