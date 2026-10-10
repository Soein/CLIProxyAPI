package registry

import (
	"context"
	"testing"
)

func TestEffectiveCatalogSources(t *testing.T) {
	for _, local := range []bool{false, true} {
		for _, home := range []bool{false, true} {
			for mask := 0; mask < 8; mask++ {
				sources := CatalogSources{}
				fields := []*string{&sources.Catalog, &sources.CodexCatalog, &sources.DevinCatalog}
				for index, field := range fields {
					if mask&(1<<index) != 0 {
						*field = "https://example.com/catalog.json"
					}
				}
				got := effectiveCatalogSources(sources, local, home)
				values := []string{got.Catalog, got.CodexCatalog, got.DevinCatalog}
				for index, field := range fields {
					want := *field
					if want == "" && local {
						want = embeddedCatalogSource
					}
					if index == 0 && home {
						want = "disabled"
					}
					if values[index] != want {
						t.Fatalf("local=%v home=%v mask=%d catalog=%d: got %q want %q", local, home, mask, index, values[index], want)
					}
				}
			}
		}
	}
}

func TestDisabledCatalogDoesNotFetch(t *testing.T) {
	u := &catalogUpdater{fetch: func(context.Context, string) ([]byte, error) { t.Error("disabled catalog fetched"); return nil, nil }}
	u.configure(context.Background(), "disabled")
	defer u.cancel()
	if u.source != "disabled" {
		t.Fatal("Home source not disabled")
	}
}

func TestHomeDevinCatalogSources(t *testing.T) {
	path := "/catalogs/devin.json"
	for _, tc := range []struct {
		name   string
		local  bool
		source string
		want   string
	}{
		{name: "default remote"},
		{name: "local embedded", local: true, want: embeddedCatalogSource},
		{name: "explicit path", source: path, want: path},
		{name: "local explicit path", local: true, source: path, want: path},
		{name: "explicit URL", source: "https://example.com/devin.json", want: "https://example.com/devin.json"},
		{name: "local explicit URL", local: true, source: "https://example.com/devin.json", want: "https://example.com/devin.json"},
		{name: "internal disabled", source: "disabled", want: "disabled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := effectiveCatalogSources(CatalogSources{DevinCatalog: tc.source}, tc.local, true)
			if got.Catalog != "disabled" || got.DevinCatalog != tc.want {
				t.Fatalf("Home sources = %+v, want general disabled and Devin %q", got, tc.want)
			}
		})
	}
}
