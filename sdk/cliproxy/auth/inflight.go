package auth

import (
	"context"
	"strings"
	"sync"

	internalcache "github.com/router-for-me/CLIProxyAPI/v7/internal/cache"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

// authLease tracks one active xAI execution. It is intentionally private and
// manager-local so custom selectors, plugin schedulers, and Home keep their
// existing routing semantics.
type authLease struct {
	tracker *xaiInflightTracker
	authID  string
	once    sync.Once
}

func (l *authLease) Release() {
	if l == nil || l.tracker == nil || l.authID == "" {
		return
	}
	l.once.Do(func() {
		l.tracker.release(l.authID)
	})
}

type executionPick struct {
	auth     *Auth
	executor ProviderExecutor
	provider string
}

// xaiInflightTracker serializes xAI execution selection with lease creation.
// Holding this lock across the pick makes concurrent least-inflight decisions
// atomic without adding counters to Auth or the public Selector contract.
type xaiInflightTracker struct {
	mu     sync.Mutex
	counts map[string]int
}

var getXAIResponseContinuityForRouting = internalcache.GetXAIResponseContinuityRequired

func (t *xaiInflightTracker) acquire(pick func(inflight func(string) int) (executionPick, error)) (executionPick, *authLease, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.counts == nil {
		t.counts = make(map[string]int)
	}
	selected, err := pick(func(authID string) int {
		return t.counts[authID]
	})
	if err != nil || selected.auth == nil || selected.auth.ID == "" {
		return selected, nil, err
	}
	t.counts[selected.auth.ID]++
	return selected, &authLease{tracker: t, authID: selected.auth.ID}, nil
}

func (t *xaiInflightTracker) release(authID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	count := t.counts[authID]
	if count <= 1 {
		delete(t.counts, authID)
		return
	}
	t.counts[authID] = count - 1
}

func (t *xaiInflightTracker) count(authID string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.counts[authID]
}

func leastInflightAuths(auths []*Auth, inflight func(string) int) []*Auth {
	if len(auths) < 2 || inflight == nil {
		return auths
	}
	minimum := 0
	found := false
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		count := inflight(auth.ID)
		if !found || count < minimum {
			minimum = count
			found = true
		}
	}
	if !found {
		return auths
	}
	selected := make([]*Auth, 0, len(auths))
	for _, auth := range auths {
		if auth != nil && inflight(auth.ID) == minimum {
			selected = append(selected, auth)
		}
	}
	return selected
}

func authByID(auths []*Auth, authID string) *Auth {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return nil
	}
	for _, auth := range auths {
		if auth != nil && auth.ID == authID {
			return auth
		}
	}
	return nil
}

func xaiContinuityPreferredAuth(ctx context.Context, model string, opts cliproxyexecutor.Options, fallbackPayload []byte) string {
	if opts.SourceFormat != sdktranslator.FormatOpenAIResponse {
		return ""
	}
	payload := opts.OriginalRequest
	if len(payload) == 0 {
		payload = fallbackPayload
	}
	executionSessionID := metadataStringValue(opts.Metadata, cliproxyexecutor.ExecutionSessionMetadataKey)
	callerScope := internalcache.XAIResponseContinuityCallerScope(callerAPIKeyScope(ctx), executionSessionID)
	if callerScope == "" {
		return ""
	}
	modelKey := canonicalModelKey(model)
	previousResponseID := strings.TrimSpace(gjson.GetBytes(payload, "previous_response_id").String())
	if previousResponseID == "" && len(fallbackPayload) > 0 {
		previousResponseID = strings.TrimSpace(gjson.GetBytes(fallbackPayload, "previous_response_id").String())
	}
	if previousResponseID != "" {
		continuity, found, errGet := getXAIResponseContinuityForRouting(ctx, callerScope, "xai", modelKey, previousResponseID)
		if errGet != nil {
			return ""
		}
		if found {
			return strings.TrimSpace(continuity.AuthID)
		}
	}

	promptCacheKey := strings.TrimSpace(gjson.GetBytes(payload, "prompt_cache_key").String())
	if promptCacheKey == "" && len(fallbackPayload) > 0 {
		promptCacheKey = strings.TrimSpace(gjson.GetBytes(fallbackPayload, "prompt_cache_key").String())
	}
	bindingID := internalcache.XAIResponseContinuitySessionBindingID(promptCacheKey, executionSessionID)
	if bindingID == "" {
		return ""
	}
	continuity, found, errGet := getXAIResponseContinuityForRouting(ctx, callerScope, "xai", modelKey, bindingID)
	if errGet != nil || !found {
		return ""
	}
	return strings.TrimSpace(continuity.AuthID)
}
