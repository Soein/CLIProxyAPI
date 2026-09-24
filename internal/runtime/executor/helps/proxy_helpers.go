package helps

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
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
// 1. Use the execution-scoped request proxy if configured (highest priority)
// 2. Reuse the RoundTripper selected by the auth conductor when available
// 3. Use auth.ProxyURL if configured
// 4. Use cfg.ProxyURL if auth proxy is not configured
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

	if cliproxyexecutor.RequestProxyURL(ctx) == "" && ctx != nil {
		if rt, ok := ctx.Value("cliproxy.roundtripper").(http.RoundTripper); ok && rt != nil {
			httpClient.Transport = rt
			return httpClient
		}
	}

	proxyURL := effectiveProxyURL(ctx, cfg, auth)

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

var devinTransportCache = NewTransportCache[string](DefaultTransportCacheCapacity)

// NewDevinHTTPClient creates an HTTP client customized for Devin Connect-RPC upstream.
// Suppresses automatic Accept-Encoding: gzip while preserving connection reuse across requests.
func NewDevinHTTPClient(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, timeout time.Duration) *http.Client {
	// A request proxy replaces both the injected round tripper and credential/global proxy.
	// Respect explicitly injected context RoundTripper only when no request override is set.
	if cliproxyexecutor.RequestProxyURL(ctx) == "" && ctx != nil {
		if rt, ok := ctx.Value("cliproxy.roundtripper").(http.RoundTripper); ok && rt != nil {
			if tr, ok := rt.(*http.Transport); ok {
				key := fmt.Sprintf("rt:%p", tr)
				cloned, err := devinTransportCache.Get(key, func() (*http.Transport, error) {
					c := tr.Clone()
					c.DisableCompression = true
					return c, nil
				})
				if err == nil && cloned != nil {
					return &http.Client{
						Transport: cloned,
						Timeout:   timeout,
					}
				}
			}
			return &http.Client{
				Transport: devinNoGzipRoundTripper{base: rt},
				Timeout:   timeout,
			}
		}
	}

	proxyURL := effectiveProxyURL(ctx, cfg, auth)

	tr, err := devinTransportCache.Get(proxyURL, func() (*http.Transport, error) {
		var base *http.Transport
		if proxyURL != "" {
			base = buildProxyTransport(proxyURL)
		}
		if base == nil {
			if dt, ok := http.DefaultTransport.(*http.Transport); ok {
				base = dt.Clone()
			} else {
				base = &http.Transport{}
			}
		}
		base.DisableCompression = true
		return base, nil
	})
	if err != nil || tr == nil {
		tr = &http.Transport{DisableCompression: true}
	}

	return &http.Client{
		Transport: tr,
		Timeout:   timeout,
	}
}

type devinNoGzipRoundTripper struct {
	base http.RoundTripper
}

func (rt devinNoGzipRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Accept-Encoding") == "" {
		req.Header.Set("Accept-Encoding", "identity")
	}
	return rt.base.RoundTrip(req)
}

func effectiveProxyURL(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth) string {
	if proxyURL := cliproxyexecutor.RequestProxyURL(ctx); proxyURL != "" {
		return proxyURL
	}
	if auth != nil {
		if proxyURL := strings.TrimSpace(auth.ProxyURL); proxyURL != "" {
			return proxyURL
		}
	}
	if cfg != nil {
		return strings.TrimSpace(cfg.ProxyURL)
	}
	return ""
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
