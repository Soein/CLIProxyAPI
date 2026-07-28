package helps

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

type proxyHelperRoundTripper struct{}

func (*proxyHelperRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, nil
}

func TestNewProxyAwareHTTPClientReusesContextRoundTripper(t *testing.T) {
	t.Parallel()

	roundTripper := &proxyHelperRoundTripper{}
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", roundTripper)
	const contextOnlyProxy = "http://context-only-proxy.example.invalid:48080"
	client := NewProxyAwareHTTPClient(
		ctx,
		&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"}},
		&cliproxyauth.Auth{ProxyURL: contextOnlyProxy},
		0,
	)

	if client.Transport != roundTripper {
		t.Fatalf("transport = %T (%p), want context RoundTripper %T (%p)", client.Transport, client.Transport, roundTripper, roundTripper)
	}
	fallbackProxyTransports.mu.Lock()
	_, cached := fallbackProxyTransports.entries["proxy:"+contextOnlyProxy]
	fallbackProxyTransports.mu.Unlock()
	if cached {
		t.Fatal("context RoundTripper path unexpectedly populated the fallback proxy cache")
	}
}

func TestProxyTransportCacheSlidingTTL(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_000, 0)
	var builds int
	var closed []*http.Transport
	cache := newProxyTransportCache(10*time.Minute, 8, func() time.Time { return now }, func(transport *http.Transport) {
		closed = append(closed, transport)
	})
	builder := func() (*http.Transport, error) {
		builds++
		return &http.Transport{}, nil
	}

	first, err := cache.getOrBuild("proxy-a", builder)
	if err != nil {
		t.Fatalf("first getOrBuild() error = %v", err)
	}
	now = now.Add(9 * time.Minute)
	second, err := cache.getOrBuild("proxy-a", builder)
	if err != nil {
		t.Fatalf("second getOrBuild() error = %v", err)
	}
	now = now.Add(9 * time.Minute)
	third, err := cache.getOrBuild("proxy-a", builder)
	if err != nil {
		t.Fatalf("third getOrBuild() error = %v", err)
	}
	if first != second || second != third || builds != 1 {
		t.Fatalf("sliding TTL transports = %p, %p, %p; builds = %d, want one cached transport", first, second, third, builds)
	}

	now = now.Add(10 * time.Minute)
	rebuilt, err := cache.getOrBuild("proxy-a", builder)
	if err != nil {
		t.Fatalf("expired getOrBuild() error = %v", err)
	}
	if rebuilt == first || builds != 2 {
		t.Fatalf("expired transport = %p, first = %p, builds = %d; want rebuilt transport", rebuilt, first, builds)
	}
	if len(closed) != 1 || closed[0] != first {
		t.Fatalf("closed transports = %p, want expired transport %p", closed, first)
	}
}

func TestProxyTransportCacheEvictsLeastRecentlyUsedAtCapacity(t *testing.T) {
	t.Parallel()

	now := time.Unix(2_000, 0)
	var closed []*http.Transport
	cache := newProxyTransportCache(time.Hour, 2, func() time.Time { return now }, func(transport *http.Transport) {
		closed = append(closed, transport)
	})
	build := func() (*http.Transport, error) { return &http.Transport{}, nil }

	first, _ := cache.getOrBuild("first", build)
	now = now.Add(time.Minute)
	second, _ := cache.getOrBuild("second", build)
	now = now.Add(time.Minute)
	if _, err := cache.getOrBuild("first", build); err != nil {
		t.Fatalf("refresh first getOrBuild() error = %v", err)
	}
	now = now.Add(time.Minute)
	third, err := cache.getOrBuild("third", build)
	if err != nil {
		t.Fatalf("third getOrBuild() error = %v", err)
	}

	if third == first || third == second {
		t.Fatal("new cache entry unexpectedly reused an existing transport")
	}
	if len(closed) != 1 || closed[0] != second {
		t.Fatalf("closed transports = %p, want least recently used transport %p", closed, second)
	}
	cache.mu.Lock()
	_, hasFirst := cache.entries["first"]
	_, hasSecond := cache.entries["second"]
	_, hasThird := cache.entries["third"]
	cache.mu.Unlock()
	if !hasFirst || hasSecond || !hasThird {
		t.Fatalf("cache keys: first=%t second=%t third=%t, want true false true", hasFirst, hasSecond, hasThird)
	}
}

func TestProxyTransportCacheConcurrentMissBuildsOnce(t *testing.T) {
	t.Parallel()

	cache := newProxyTransportCache(time.Hour, 8, time.Now, func(*http.Transport) {})
	want := &http.Transport{}
	var builds atomic.Int32
	builderStarted := make(chan struct{})
	releaseBuilder := make(chan struct{})
	builder := func() (*http.Transport, error) {
		if builds.Add(1) == 1 {
			close(builderStarted)
		}
		<-releaseBuilder
		return want, nil
	}

	const callers = 64
	results := make(chan *http.Transport, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			transport, err := cache.getOrBuild("shared", builder)
			results <- transport
			errs <- err
		}()
	}
	<-builderStarted
	close(releaseBuilder)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("getOrBuild() error = %v", err)
		}
	}
	for transport := range results {
		if transport != want {
			t.Fatalf("transport = %p, want %p", transport, want)
		}
	}
	if got := builds.Load(); got != 1 {
		t.Fatalf("builder calls = %d, want 1", got)
	}
}

func TestProxyTransportCacheClosesOutsideLock(t *testing.T) {
	t.Parallel()

	now := time.Unix(3_000, 0)
	var cache *proxyTransportCache
	closedOutsideLock := false
	cache = newProxyTransportCache(time.Minute, 1, func() time.Time { return now }, func(*http.Transport) {
		if cache.mu.TryLock() {
			closedOutsideLock = true
			cache.mu.Unlock()
		}
	})
	build := func() (*http.Transport, error) { return &http.Transport{}, nil }
	if _, err := cache.getOrBuild("first", build); err != nil {
		t.Fatalf("first getOrBuild() error = %v", err)
	}
	now = now.Add(time.Minute)
	if _, err := cache.getOrBuild("second", build); err != nil {
		t.Fatalf("second getOrBuild() error = %v", err)
	}
	if !closedOutsideLock {
		t.Fatal("evicted transport was closed while cache mutex was held")
	}
}

func TestNewProxyAwareHTTPClientBuildsConfiguredProxyWithoutContextRoundTripper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		authProxy   string
		globalProxy string
		wantProxy   string
	}{
		{
			name:        "auth proxy overrides global proxy",
			authProxy:   "http://auth-proxy.example.com:8080",
			globalProxy: "http://global-proxy.example.com:8080",
			wantProxy:   "http://auth-proxy.example.com:8080",
		},
		{
			name:        "global proxy is used without auth proxy",
			globalProxy: "http://global-proxy.example.com:8080",
			wantProxy:   "http://global-proxy.example.com:8080",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := NewProxyAwareHTTPClient(
				context.Background(),
				&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: tt.globalProxy}},
				&cliproxyauth.Auth{ProxyURL: tt.authProxy},
				0,
			)

			transport, ok := client.Transport.(*http.Transport)
			if !ok {
				t.Fatalf("transport type = %T, want *http.Transport", client.Transport)
			}
			request := &http.Request{URL: &url.URL{Scheme: "https", Host: "api.x.ai"}}
			proxyURL, errProxy := transport.Proxy(request)
			if errProxy != nil {
				t.Fatalf("transport.Proxy returned error: %v", errProxy)
			}
			if proxyURL == nil || proxyURL.String() != tt.wantProxy {
				t.Fatalf("proxy URL = %v, want %q", proxyURL, tt.wantProxy)
			}
		})
	}
}

func TestNewProxyAwareHTTPClientInvalidProxyFallsBackToDefaultTransport(t *testing.T) {
	t.Parallel()

	client := NewProxyAwareHTTPClient(
		context.Background(),
		nil,
		&cliproxyauth.Auth{ProxyURL: "://invalid"},
		0,
	)

	if client.Transport != nil {
		t.Fatalf("transport type = %T, want nil default transport", client.Transport)
	}
}

func TestNewProxyAwareHTTPClientReusesFallbackProxyTransport(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "  http://shared-proxy.example.com:8080  "}}
	first := NewProxyAwareHTTPClient(context.Background(), cfg, nil, 0)
	second := NewProxyAwareHTTPClient(context.Background(), cfg, nil, 0)

	if first.Transport == nil || first.Transport != second.Transport {
		t.Fatalf("transports = %p and %p, want the same cached transport", first.Transport, second.Transport)
	}
}

func TestNewProxyAwareHTTPClientIsolatesFallbackProxyTransports(t *testing.T) {
	t.Parallel()

	first := NewProxyAwareHTTPClient(
		context.Background(),
		&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://first-proxy.example.com:8080"}},
		nil,
		0,
	)
	second := NewProxyAwareHTTPClient(
		context.Background(),
		&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://second-proxy.example.com:8080"}},
		nil,
		0,
	)

	if first.Transport == second.Transport {
		t.Fatalf("transports = %p and %p, want different transports for different proxies", first.Transport, second.Transport)
	}
}

func TestNewProxyAwareHTTPClientConcurrentlyReusesFallbackProxyTransport(t *testing.T) {
	t.Parallel()

	const callers = 64
	cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://concurrent-proxy.example.com:8080"}}
	transports := make(chan http.RoundTripper, callers)
	var wg sync.WaitGroup

	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := NewProxyAwareHTTPClient(context.Background(), cfg, nil, 0)
			transports <- client.Transport
		}()
	}

	wg.Wait()
	close(transports)

	var want http.RoundTripper
	for transport := range transports {
		if want == nil {
			want = transport
			continue
		}
		if transport != want {
			t.Fatalf("transport = %p, want cached transport %p", transport, want)
		}
	}
}

func TestNewProxyAwareHTTPClientReusesDirectTransport(t *testing.T) {
	t.Parallel()

	auth := &cliproxyauth.Auth{ProxyURL: "direct"}
	first := NewProxyAwareHTTPClient(context.Background(), nil, auth, 0)
	second := NewProxyAwareHTTPClient(context.Background(), nil, auth, 0)

	if first.Transport == nil || first.Transport != second.Transport {
		t.Fatalf("transports = %p and %p, want the same cached direct transport", first.Transport, second.Transport)
	}
	transport, ok := first.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", first.Transport)
	}
	if transport.Proxy != nil {
		t.Fatal("expected cached direct transport to disable proxy function")
	}
}

func TestNewProxyAwareHTTPClientDirectBypassesGlobalProxy(t *testing.T) {
	t.Parallel()

	client := NewProxyAwareHTTPClient(
		context.Background(),
		&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"}},
		&cliproxyauth.Auth{ProxyURL: "direct"},
		0,
	)

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", client.Transport)
	}
	if transport.Proxy != nil {
		t.Fatal("expected direct transport to disable proxy function")
	}
}
