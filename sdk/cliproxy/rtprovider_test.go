package cliproxy

import (
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func TestRoundTripperForConcurrentColdMissBuildsOnce(t *testing.T) {
	t.Parallel()

	const callers = 32
	provider := newDefaultRoundTripperProvider()
	buildStarted := make(chan struct{})
	releaseBuild := make(chan struct{})
	var buildStartedOnce sync.Once
	var buildCount atomic.Int32
	provider.buildTransport = func(string) (*http.Transport, error) {
		buildCount.Add(1)
		buildStartedOnce.Do(func() { close(buildStarted) })
		<-releaseBuild
		return &http.Transport{}, nil
	}

	start := make(chan struct{})
	results := make([]http.RoundTripper, callers)
	var ready sync.WaitGroup
	var done sync.WaitGroup
	ready.Add(callers)
	done.Add(callers)
	for i := range callers {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			results[i] = provider.RoundTripperFor(&coreauth.Auth{ProxyURL: "http://shared-proxy.example.com:8080"})
		}()
	}

	ready.Wait()
	close(start)
	select {
	case <-buildStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for transport build")
	}
	time.Sleep(20 * time.Millisecond)
	close(releaseBuild)
	done.Wait()

	if got := buildCount.Load(); got != 1 {
		t.Fatalf("transport build count = %d, want 1", got)
	}
	want := results[0]
	if want == nil {
		t.Fatal("first RoundTripperFor result is nil")
	}
	for i, result := range results[1:] {
		if result != want {
			t.Fatalf("result %d = %p, want shared transport %p", i+1, result, want)
		}
	}
}

func TestRoundTripperForNormalizesProxyCacheKey(t *testing.T) {
	t.Parallel()

	provider := newDefaultRoundTripperProvider()
	var buildCount int
	provider.buildTransport = func(string) (*http.Transport, error) {
		buildCount++
		return &http.Transport{}, nil
	}

	first := provider.RoundTripperFor(&coreauth.Auth{ProxyURL: "  HTTP://Proxy.Example.COM:8080  "})
	second := provider.RoundTripperFor(&coreauth.Auth{ProxyURL: "http://proxy.example.com:8080"})
	if first == nil || first != second {
		t.Fatalf("transports = %p and %p, want the same normalized cache entry", first, second)
	}
	if buildCount != 1 {
		t.Fatalf("transport build count = %d, want 1", buildCount)
	}
}

func TestRoundTripperForRebuildsExpiredTransport(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_000, 0)
	provider := newDefaultRoundTripperProvider()
	provider.ttl = 10 * time.Minute
	provider.now = func() time.Time { return now }
	var buildCount int
	provider.buildTransport = func(string) (*http.Transport, error) {
		buildCount++
		return &http.Transport{}, nil
	}
	var closed []*http.Transport
	provider.closeIdle = func(transport *http.Transport) {
		closed = append(closed, transport)
	}

	first := provider.RoundTripperFor(&coreauth.Auth{ProxyURL: "http://ttl-proxy.example.com:8080"})
	now = now.Add(9 * time.Minute)
	if refreshed := provider.RoundTripperFor(&coreauth.Auth{ProxyURL: "http://ttl-proxy.example.com:8080"}); refreshed != first {
		t.Fatalf("transport before sliding TTL = %p, want %p", refreshed, first)
	}
	now = now.Add(10 * time.Minute)
	rebuilt := provider.RoundTripperFor(&coreauth.Auth{ProxyURL: "http://ttl-proxy.example.com:8080"})

	if rebuilt == nil || rebuilt == first || buildCount != 2 {
		t.Fatalf("rebuilt transport = %p, first = %p, builds = %d; want a second transport", rebuilt, first, buildCount)
	}
	if len(closed) != 1 || closed[0] != first {
		t.Fatalf("closed transports = %p, want expired transport %p", closed, first)
	}
}

func TestRoundTripperForEvictsLeastRecentlyUsedAtCapacity(t *testing.T) {
	t.Parallel()

	now := time.Unix(2_000, 0)
	provider := newDefaultRoundTripperProvider()
	provider.capacity = 2
	provider.ttl = time.Hour
	provider.now = func() time.Time { return now }
	provider.buildTransport = func(string) (*http.Transport, error) {
		return &http.Transport{}, nil
	}
	var closed []*http.Transport
	provider.closeIdle = func(transport *http.Transport) {
		closed = append(closed, transport)
	}

	first := provider.RoundTripperFor(&coreauth.Auth{ProxyURL: "http://first-proxy.example.com:8080"})
	now = now.Add(time.Minute)
	second := provider.RoundTripperFor(&coreauth.Auth{ProxyURL: "http://second-proxy.example.com:8080"})
	now = now.Add(time.Minute)
	if refreshed := provider.RoundTripperFor(&coreauth.Auth{ProxyURL: "http://first-proxy.example.com:8080"}); refreshed != first {
		t.Fatalf("refreshed transport = %p, want %p", refreshed, first)
	}
	now = now.Add(time.Minute)
	third := provider.RoundTripperFor(&coreauth.Auth{ProxyURL: "http://third-proxy.example.com:8080"})

	if third == nil || third == first || third == second {
		t.Fatalf("third transport = %p, want a new transport", third)
	}
	if len(closed) != 1 || closed[0] != second {
		t.Fatalf("closed transports = %p, want least recently used transport %p", closed, second)
	}
	provider.mu.Lock()
	cacheSize := len(provider.cache)
	provider.mu.Unlock()
	if cacheSize != 2 {
		t.Fatalf("cache size = %d, want 2", cacheSize)
	}
}

func TestRoundTripperForDirectBypassesProxy(t *testing.T) {
	t.Parallel()

	provider := newDefaultRoundTripperProvider()
	rt := provider.RoundTripperFor(&coreauth.Auth{ProxyURL: "direct"})
	transport, ok := rt.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", rt)
	}
	if transport.Proxy != nil {
		t.Fatal("expected direct transport to disable proxy function")
	}
}
