package auth

import (
	"context"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestManagerConfigBoundariesRejectInvalidRequestScopedErrorRules(t *testing.T) {
	validCfg := requestScopedValidationConfig("valid")
	invalidCfg := requestScopedValidationConfig("   ")

	t.Run("SetConfig", func(t *testing.T) {
		manager := NewManager(nil, nil, nil)
		manager.SetConfig(validCfg)
		manager.SetConfig(invalidCfg)
		assertManagerRequestScopedMatch(t, manager, "valid")
	})

	t.Run("SetConfigSnapshot", func(t *testing.T) {
		manager := NewManager(nil, nil, nil)
		manager.SetConfig(validCfg)
		manager.SetConfigSnapshot(invalidCfg)
		assertManagerRequestScopedMatch(t, manager, "valid")
	})

	t.Run("ApplyConfigWithCooldownStateStore", func(t *testing.T) {
		manager := NewManager(nil, nil, nil)
		manager.SetConfig(validCfg)
		if manager.ApplyConfigWithCooldownStateStore(context.Background(), invalidCfg, nil) {
			t.Fatal("ApplyConfigWithCooldownStateStore() accepted invalid request-scoped error rules")
		}
		assertManagerRequestScopedMatch(t, manager, "valid")
	})
}

func requestScopedValidationConfig(match string) *internalconfig.Config {
	return &internalconfig.Config{
		ClaudeKey: []internalconfig.ClaudeKey{{
			APIKey: "claude-key",
			RequestScopedErrors: []internalconfig.RequestScopedErrorRule{{
				Status: 400,
				Match:  []string{match},
				Action: RequestScopedActionStop,
			}},
		}},
	}
}

func assertManagerRequestScopedMatch(t *testing.T, manager *Manager, want string) {
	t.Helper()
	cfg := manager.runtimeConfigSnapshot()
	if cfg == nil || len(cfg.ClaudeKey) != 1 || len(cfg.ClaudeKey[0].RequestScopedErrors) != 1 {
		t.Fatalf("runtime config = %#v, want one Claude request-scoped rule", cfg)
	}
	rule := cfg.ClaudeKey[0].RequestScopedErrors[0]
	if len(rule.Match) != 1 || rule.Match[0] != want {
		t.Fatalf("runtime request-scoped match = %#v, want [%q]", rule.Match, want)
	}
}
