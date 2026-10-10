package registry

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestCatalogFetcherSources(t *testing.T) {
	for _, tc := range []struct {
		name     string
		data     []byte
		validate func([]byte) error
	}{
		{"general", embeddedModelsJSON, validateCatalogBytes},
		{"codex", embeddedCodexClientModelsJSON, ValidateCodexClientModelsJSON},
		{"devin", embeddedDevinModelsJSON, validateDevinCatalogBytes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var officialHits atomic.Int32
			official := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { officialHits.Add(1); _, _ = w.Write(tc.data) }))
			defer official.Close()
			custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(tc.data) }))
			defer custom.Close()
			broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("invalid")) }))
			defer broken.Close()
			path := filepath.Join(t.TempDir(), "catalog.json")
			if errWrite := os.WriteFile(path, tc.data, 0600); errWrite != nil {
				t.Fatal(errWrite)
			}
			fetch := catalogFetcher(tc.data, []string{official.URL}, tc.validate)
			for _, source := range []string{embeddedCatalogSource, custom.URL, path} {
				data, errFetch := fetch(context.Background(), source)
				if errFetch != nil || string(data) != string(tc.data) {
					t.Fatalf("source %s: %v", source, errFetch)
				}
			}
			for _, source := range []string{broken.URL, path + ".missing"} {
				if _, errFetch := fetch(context.Background(), source); errFetch == nil {
					t.Fatal("accepted invalid custom source")
				}
			}
			if officialHits.Load() != 0 {
				t.Fatal("custom sources fell back to official URL")
			}
			if _, errFetch := fetch(context.Background(), ""); errFetch != nil {
				t.Fatal(errFetch)
			}
			if officialHits.Load() != 1 {
				t.Fatal("default source was not fetched")
			}
			if errWrite := os.WriteFile(path, []byte("invalid"), 0600); errWrite != nil {
				t.Fatal(errWrite)
			}
			if _, errFetch := fetch(context.Background(), path); errFetch == nil {
				t.Fatal("file was not reread")
			}
		})
	}
}

func TestCatalogSourceSwitchRejectsStalePublication(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	published := make(chan string, 3)
	u := &catalogUpdater{
		generation: 1,
		fetch: func(ctx context.Context, source string) ([]byte, error) {
			if source == "old" {
				close(started)
				<-release
			}
			return []byte(source), nil
		},
		publish: func(data []byte) ([]string, error) { published <- string(data); return nil, nil },
	}
	go func() { u.refresh(ctx, "old", 1); close(done) }()
	<-started
	u.configure(ctx, "new")
	select {
	case value := <-published:
		if value != "new" {
			t.Fatalf("published %s", value)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("new source blocked behind old fetch")
	}
	close(release)
	<-done
	select {
	case value := <-published:
		t.Fatalf("stale publication: %s", value)
	default:
	}
	u.configure(ctx, "new")
	u.mu.Lock()
	generation := u.generation
	u.mu.Unlock()
	if generation != 2 {
		t.Fatal("unchanged source restarted")
	}
}

func TestCatalogRefreshKeepsLastValidData(t *testing.T) {
	last := "last valid"
	u := &catalogUpdater{
		fetch:   func(context.Context, string) ([]byte, error) { return nil, errors.New("unavailable") },
		publish: func(data []byte) ([]string, error) { last = string(data); return nil, nil },
	}
	u.refresh(context.Background(), "", 0)
	if last != "last valid" {
		t.Fatal("failed fetch changed catalog")
	}
	u.fetch = func(context.Context, string) ([]byte, error) { return []byte("invalid"), nil }
	u.publish = func([]byte) ([]string, error) { return nil, errors.New("invalid") }
	u.refresh(context.Background(), "", 0)
	if last != "last valid" {
		t.Fatal("invalid catalog changed data")
	}
}

func TestHomeDevinCatalogLoadsSelectedSource(t *testing.T) {
	for _, tc := range []struct {
		name         string
		local        bool
		kind         string
		wantOfficial int32
		wantCustom   int32
	}{
		{name: "default remote", wantOfficial: 1},
		{name: "local embedded", local: true},
		{name: "explicit path", kind: "path"},
		{name: "local explicit path", local: true, kind: "path"},
		{name: "explicit URL", kind: "url", wantCustom: 1},
		{name: "local explicit URL", local: true, kind: "url", wantCustom: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var officialHits, customHits atomic.Int32
			official := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				officialHits.Add(1)
				_, _ = w.Write(embeddedDevinModelsJSON)
			}))
			defer official.Close()
			custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				customHits.Add(1)
				_, _ = w.Write(embeddedDevinModelsJSON)
			}))
			defer custom.Close()
			source := ""
			switch tc.kind {
			case "path":
				source = filepath.Join(t.TempDir(), "devin.json")
				if errWrite := os.WriteFile(source, embeddedDevinModelsJSON, 0600); errWrite != nil {
					t.Fatal(errWrite)
				}
			case "url":
				source = custom.URL
			}
			configured := CatalogSources{DevinCatalog: source}
			if errValidate := configured.Validate(); errValidate != nil {
				t.Fatal(errValidate)
			}
			selected := effectiveCatalogSources(configured, tc.local, true)
			published := make(chan []byte, 1)
			updater := &catalogUpdater{
				fetch: catalogFetcher(embeddedDevinModelsJSON, []string{official.URL}, validateDevinCatalogBytes),
				publish: func(data []byte) ([]string, error) {
					if _, errValidate := ValidateDevinModelsJSON(data); errValidate != nil {
						return nil, errValidate
					}
					published <- bytes.Clone(data)
					return nil, nil
				},
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			updater.configure(ctx, selected.DevinCatalog)
			select {
			case data := <-published:
				if !bytes.Equal(data, embeddedDevinModelsJSON) {
					t.Fatal("selected Devin catalog was not loaded")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Home Devin catalog did not load")
			}
			if officialHits.Load() != tc.wantOfficial || customHits.Load() != tc.wantCustom {
				t.Fatalf("catalog fetches official=%d custom=%d, want %d/%d", officialHits.Load(), customHits.Load(), tc.wantOfficial, tc.wantCustom)
			}
		})
	}
}
