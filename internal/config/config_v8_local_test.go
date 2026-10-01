package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

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
