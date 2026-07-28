package cliproxy

import (
	"net/http"
	"strings"
	"sync"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
)

const (
	defaultRoundTripperTTL      = 30 * time.Minute
	defaultRoundTripperCapacity = 128
)

type roundTripperCacheEntry struct {
	transport *http.Transport
	lastUsed  time.Time
}

// defaultRoundTripperProvider returns a per-auth HTTP RoundTripper based on
// the Auth.ProxyURL value. It caches transports per normalized proxy setting.
type defaultRoundTripperProvider struct {
	mu             sync.Mutex
	cache          map[string]roundTripperCacheEntry
	ttl            time.Duration
	capacity       int
	now            func() time.Time
	closeIdle      func(*http.Transport)
	buildTransport func(string) (*http.Transport, error)
}

func newDefaultRoundTripperProvider() *defaultRoundTripperProvider {
	return &defaultRoundTripperProvider{
		cache:     make(map[string]roundTripperCacheEntry),
		ttl:       defaultRoundTripperTTL,
		capacity:  defaultRoundTripperCapacity,
		now:       time.Now,
		closeIdle: func(transport *http.Transport) { transport.CloseIdleConnections() },
		buildTransport: func(proxyURL string) (*http.Transport, error) {
			transport, _, errBuild := proxyutil.BuildHTTPTransport(proxyURL)
			return transport, errBuild
		},
	}
}

// RoundTripperFor implements coreauth.RoundTripperProvider.
func (p *defaultRoundTripperProvider) RoundTripperFor(auth *coreauth.Auth) http.RoundTripper {
	if auth == nil {
		return nil
	}
	proxyStr := strings.TrimSpace(auth.ProxyURL)
	if proxyStr == "" {
		return nil
	}
	setting, errParse := proxyutil.Parse(proxyStr)
	if errParse != nil {
		log.Errorf("%v", errParse)
		return nil
	}
	cacheKey := normalizedProxyCacheKey(setting)

	p.mu.Lock()
	now := p.now()
	stale := p.removeExpiredLocked(now)
	if entry, ok := p.cache[cacheKey]; ok {
		entry.lastUsed = now
		p.cache[cacheKey] = entry
		p.mu.Unlock()
		p.closeAll(stale)
		return entry.transport
	}

	transport, errBuild := p.buildTransport(setting.Raw)
	if errBuild != nil {
		p.mu.Unlock()
		p.closeAll(stale)
		log.Errorf("%v", errBuild)
		return nil
	}
	if transport == nil {
		p.mu.Unlock()
		p.closeAll(stale)
		return nil
	}
	if p.capacity > 0 {
		if len(p.cache) >= p.capacity {
			if evicted := p.removeOldestLocked(); evicted != nil {
				stale = append(stale, evicted)
			}
		}
		p.cache[cacheKey] = roundTripperCacheEntry{transport: transport, lastUsed: now}
	}
	p.mu.Unlock()
	p.closeAll(stale)
	return transport
}

func normalizedProxyCacheKey(setting proxyutil.Setting) string {
	if setting.Mode == proxyutil.ModeDirect {
		return "direct"
	}
	if setting.Mode == proxyutil.ModeProxy && setting.URL != nil {
		normalizedURL := *setting.URL
		normalizedURL.Scheme = strings.ToLower(normalizedURL.Scheme)
		normalizedURL.Host = strings.ToLower(normalizedURL.Host)
		return "proxy:" + normalizedURL.String()
	}
	return setting.Raw
}

func (p *defaultRoundTripperProvider) removeExpiredLocked(now time.Time) []*http.Transport {
	if p.ttl <= 0 {
		return nil
	}
	var expired []*http.Transport
	for key, entry := range p.cache {
		if now.Sub(entry.lastUsed) < p.ttl {
			continue
		}
		delete(p.cache, key)
		expired = append(expired, entry.transport)
	}
	return expired
}

func (p *defaultRoundTripperProvider) removeOldestLocked() *http.Transport {
	var oldestKey string
	var oldest roundTripperCacheEntry
	found := false
	for key, entry := range p.cache {
		if !found || entry.lastUsed.Before(oldest.lastUsed) {
			oldestKey = key
			oldest = entry
			found = true
		}
	}
	if !found {
		return nil
	}
	delete(p.cache, oldestKey)
	return oldest.transport
}

func (p *defaultRoundTripperProvider) closeAll(transports []*http.Transport) {
	if p.closeIdle == nil {
		return
	}
	for _, transport := range transports {
		if transport != nil {
			p.closeIdle(transport)
		}
	}
}
