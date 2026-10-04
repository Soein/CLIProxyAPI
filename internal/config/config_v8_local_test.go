package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestLegacySavePreservesClusterAndUsageLayout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	raw := []byte("port: 8317\ncluster: {enabled: true, node-id: replica-a}\nusage: {backend: pg, flush-batch-size: 250}\n")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	want, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg := want.CloneForRuntime()
	for i := 0; i < 2; i++ {
		cfg.Port++
		if err = SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		data, errRead := os.ReadFile(path)
		if errRead != nil {
			t.Fatal(errRead)
		}
		var doc yaml.Node
		if err = yaml.Unmarshal(data, &doc); err != nil {
			t.Fatal(err)
		}
		root := doc.Content[0]
		if IsV8ConfigLayout(root) || yamlPath(root, "config-version") != nil || yamlPath(root, "port") == nil {
			t.Fatalf("saving local legacy settings migrated the layout: %s", data)
		}
		cfg, err = LoadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Port != want.Port+i+1 || !reflect.DeepEqual(cfg.Cluster, want.Cluster) || !reflect.DeepEqual(cfg.Usage, want.Usage) {
			t.Fatalf("legacy save changed effective values: %+v", cfg)
		}
	}
}

func TestV8HistoricalXAIToolLimitsRemainShared(t *testing.T) {
	for _, tc := range []struct {
		name, raw  string
		maxTools   int
		namespaces []string
	}{
		{"legacy", "xai: {max-tools: 150, preferred-tool-namespaces: [functions]}\n", 150, []string{"functions"}},
		{"historical OAuth", "oauth: {providers: {xai: {max-tools: 150, preferred-tool-namespaces: [functions]}}}\n", 150, []string{"functions"}},
		{"canonical", "upstream: {xai: {max-tools: 150, preferred-tool-namespaces: [functions]}}\n", 150, []string{"functions"}},
		{"canonical explicit defaults", "oauth: {providers: {xai: {max-tools: 150, preferred-tool-namespaces: [functions]}}}\nupstream: {xai: {max-tools: 0, preferred-tool-namespaces: []}}\n", 0, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseConfigBytes([]byte(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			migrated, _, err := NormalizeConfigLayout([]byte(tc.raw), true)
			if err != nil {
				t.Fatal(err)
			}
			if err = ValidateV8Config(migrated); err != nil {
				t.Fatal(err)
			}
			var doc yaml.Node
			if err = yaml.Unmarshal(migrated, &doc); err != nil {
				t.Fatal(err)
			}
			if yamlPath(doc.Content[0], "upstream.xai.max-tools") == nil || yamlPath(doc.Content[0], "upstream.xai.preferred-tool-namespaces") == nil || yamlPath(doc.Content[0], "oauth.providers.xai") != nil {
				t.Fatalf("migration lost canonical tool limits: %s", migrated)
			}
			snapshot, err := yaml.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			for name, data := range map[string][]byte{"original": []byte(tc.raw), "migrated": migrated, "snapshot": snapshot} {
				restored, errParse := ParseConfigBytes(data)
				if errParse != nil {
					t.Fatal(errParse)
				}
				for _, view := range []*Config{restored, restored.ForAPIKey()} {
					if view.XAI.MaxTools != tc.maxTools || !reflect.DeepEqual(view.XAI.PreferredToolNamespaces, tc.namespaces) {
						t.Fatalf("%s changed shared xAI tool limits: %+v", name, view.XAI)
					}
				}
			}
		})
	}
}

func TestV8MigrationPreservesClusterUsageAndRequestScopedErrors(t *testing.T) {
	legacy := []byte(`port: 8317
cluster:
  enabled: true
  node-id: replica-a
  region: test-region
  probe-interval: 3s
  endpoint: http://replica-a:8317
  weight: 80
  registrar-interval: 6s
  auth-sharding: true
  spillover: false
  ring-staleness: 24s
  ring-poll-interval: 12s
usage:
  backend: pg
  flush-interval: 2s
  flush-batch-size: 250
  event-retention-days: 3
  rollup-retention-days: 30
  query-cache-ttl: 4s
xai:
  max-tools: 150
  preferred-tool-namespaces: [functions]
codex:
  response-steering: true
  stream-bootstrap-timeout: 20s
codex-api-key:
  - api-key: test-key
    base-url: https://example.com/v1
    weight: 2
    request-scoped-errors:
      - status: 400
        match: [context_window_exceeded]
        action: stop-and-cooldown
oauth-request-scoped-errors:
  codex:
    - status: 400
      match-regexr: ["^bad request$"]
      action: stop
`)
	want, err := ParseConfigBytes(legacy)
	if err != nil {
		t.Fatal(err)
	}
	migrated, changed, err := NormalizeConfigLayout(legacy, true)
	if err != nil || !changed {
		t.Fatalf("migrate legacy config: changed=%v error=%v", changed, err)
	}
	if err = ValidateV8Config(migrated); err != nil {
		t.Fatalf("validate migrated config: %v", err)
	}
	assertLocalSettings := func(t *testing.T, got *Config) {
		t.Helper()
		if !reflect.DeepEqual(got.Cluster, want.Cluster) || !reflect.DeepEqual(got.Usage, want.Usage) {
			t.Fatalf("cluster/usage changed: cluster=%+v usage=%+v", got.Cluster, got.Usage)
		}
		if !reflect.DeepEqual(got.Codex, want.Codex) || !reflect.DeepEqual(got.XAI, want.XAI) {
			t.Fatalf("provider settings changed: codex=%+v xai=%+v", got.Codex, got.XAI)
		}
		if !reflect.DeepEqual(got.CodexKey, want.CodexKey) || !reflect.DeepEqual(got.OAuthRequestScopedErrors, want.OAuthRequestScopedErrors) {
			t.Fatal("credential weights or request-scoped error rules changed")
		}
	}
	for _, layout := range []struct {
		name string
		data []byte
	}{
		{"legacy", legacy}, {"v8", migrated},
	} {
		t.Run(layout.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if errWrite := os.WriteFile(path, layout.data, 0600); errWrite != nil {
				t.Fatal(errWrite)
			}
			cfg, errLoad := LoadConfig(path)
			if errLoad != nil {
				t.Fatal(errLoad)
			}
			assertLocalSettings(t, cfg)
			for _, migrate := range []bool{false, true, false} {
				if errSave := SaveConfigPreserveComments(path, cfg, migrate); errSave != nil {
					t.Fatal(errSave)
				}
				cfg, errLoad = LoadConfig(path)
				if errLoad != nil {
					t.Fatal(errLoad)
				}
				assertLocalSettings(t, cfg)
			}
			data, errRead := os.ReadFile(path)
			if errRead != nil {
				t.Fatal(errRead)
			}
			if errValidate := ValidateV8Config(data); errValidate != nil {
				t.Fatalf("saved migration is not valid v8 config: %v", errValidate)
			}
		})
	}
}
