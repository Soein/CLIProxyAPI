package helps

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
)

const (
	fallbackProxyTransportTTL      = 30 * time.Minute
	fallbackProxyTransportCapacity = 128
)

type proxyTransportCacheEntry struct {
	transport *http.Transport
	lastUsed  time.Time
}

type proxyTransportCache struct {
	mu        sync.Mutex
	entries   map[string]proxyTransportCacheEntry
	ttl       time.Duration
	capacity  int
	now       func() time.Time
	closeIdle func(*http.Transport)
}

var fallbackProxyTransports = newProxyTransportCache(
	fallbackProxyTransportTTL,
	fallbackProxyTransportCapacity,
	time.Now,
	func(transport *http.Transport) { transport.CloseIdleConnections() },
)

func newProxyTransportCache(ttl time.Duration, capacity int, now func() time.Time, closeIdle func(*http.Transport)) *proxyTransportCache {
	return &proxyTransportCache{
		entries:   make(map[string]proxyTransportCacheEntry),
		ttl:       ttl,
		capacity:  capacity,
		now:       now,
		closeIdle: closeIdle,
	}
}

func (c *proxyTransportCache) getOrBuild(key string, builder func() (*http.Transport, error)) (*http.Transport, error) {
	c.mu.Lock()
	now := c.now()
	stale := c.removeExpiredLocked(now)
	if entry, ok := c.entries[key]; ok {
		entry.lastUsed = now
		c.entries[key] = entry
		c.mu.Unlock()
		c.closeAll(stale)
		return entry.transport, nil
	}

	transport, errBuild := builder()
	if errBuild != nil {
		c.mu.Unlock()
		c.closeAll(stale)
		return nil, errBuild
	}
	if transport == nil {
		c.mu.Unlock()
		c.closeAll(stale)
		return nil, nil
	}
	if c.capacity > 0 {
		if len(c.entries) >= c.capacity {
			if evicted := c.removeOldestLocked(); evicted != nil {
				stale = append(stale, evicted)
			}
		}
		c.entries[key] = proxyTransportCacheEntry{transport: transport, lastUsed: now}
	}
	c.mu.Unlock()
	c.closeAll(stale)
	return transport, nil
}

func (c *proxyTransportCache) removeExpiredLocked(now time.Time) []*http.Transport {
	if c.ttl <= 0 {
		return nil
	}
	var expired []*http.Transport
	for key, entry := range c.entries {
		if now.Sub(entry.lastUsed) < c.ttl {
			continue
		}
		delete(c.entries, key)
		expired = append(expired, entry.transport)
	}
	return expired
}

func (c *proxyTransportCache) removeOldestLocked() *http.Transport {
	var oldestKey string
	var oldest proxyTransportCacheEntry
	found := false
	for key, entry := range c.entries {
		if !found || entry.lastUsed.Before(oldest.lastUsed) {
			oldestKey = key
			oldest = entry
			found = true
		}
	}
	if !found {
		return nil
	}
	delete(c.entries, oldestKey)
	return oldest.transport
}

func (c *proxyTransportCache) closeAll(transports []*http.Transport) {
	if c.closeIdle == nil {
		return
	}
	for _, transport := range transports {
		if transport != nil {
			c.closeIdle(transport)
		}
	}
}

// NewProxyAwareHTTPClient creates an HTTP client with proper proxy configuration priority:
// 1. Reuse the RoundTripper from context when available
// 2. Use auth.ProxyURL if configured
// 3. Use cfg.ProxyURL if auth proxy is not configured
//
// Parameters:
//   - ctx: The context containing optional RoundTripper
//   - cfg: The application configuration
//   - auth: The authentication information
//   - timeout: The client timeout (0 means no timeout)
//
// Returns:
//   - *http.Client: An HTTP client with configured proxy or transport
func NewProxyAwareHTTPClient(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, timeout time.Duration) *http.Client {
	httpClient := &http.Client{}
	if timeout > 0 {
		httpClient.Timeout = timeout
	}

	// Priority 1: Reuse the RoundTripper selected and cached by the auth conductor.
	if rt, ok := ctx.Value("cliproxy.roundtripper").(http.RoundTripper); ok && rt != nil {
		httpClient.Transport = rt
		return httpClient
	}

	// Priority 2: Use auth.ProxyURL if configured
	var proxyURL string
	if auth != nil {
		proxyURL = strings.TrimSpace(auth.ProxyURL)
	}

	// Priority 3: Use cfg.ProxyURL if auth proxy is not configured
	if proxyURL == "" && cfg != nil {
		proxyURL = strings.TrimSpace(cfg.ProxyURL)
	}

	// If we have a proxy URL configured, set up the transport
	if proxyURL != "" {
		transport := buildProxyTransport(proxyURL)
		if transport != nil {
			httpClient.Transport = transport
			return httpClient
		}
		// If proxy setup failed, log and fall through to the default transport.
		log.Debugf("failed to setup proxy from URL: %s, falling back to default transport", proxyutil.Redact(proxyURL))
	}

	return httpClient
}

// buildProxyTransport creates an HTTP transport configured for the given proxy URL.
// It supports SOCKS5, HTTP, and HTTPS proxy protocols.
//
// Parameters:
//   - proxyURL: The proxy URL string (e.g., "socks5://user:pass@host:port", "http://host:port")
//
// Returns:
//   - *http.Transport: A configured transport, or nil if the proxy URL is invalid
func buildProxyTransport(proxyURL string) *http.Transport {
	setting, errParse := proxyutil.Parse(proxyURL)
	if errParse != nil {
		log.Errorf("%v", errParse)
		return nil
	}

	cacheKey := proxyTransportCacheKey(setting)
	transport, errBuild := fallbackProxyTransports.getOrBuild(cacheKey, func() (*http.Transport, error) {
		transport, _, errBuild := proxyutil.BuildHTTPTransport(setting.Raw)
		return transport, errBuild
	})
	if errBuild != nil {
		log.Errorf("%v", errBuild)
		return nil
	}
	if transport == nil {
		return nil
	}
	return transport
}

func proxyTransportCacheKey(setting proxyutil.Setting) string {
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
