package cliproxy

import (
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestServiceRejectsInvalidRequestScopedErrorRulesConfigCommit(t *testing.T) {
	originalCfg := &internalconfig.Config{}
	service := &Service{cfg: originalCfg}
	newCfg := &internalconfig.Config{
		CodexKey: []internalconfig.CodexKey{{
			APIKey: "codex-key",
			RequestScopedErrors: []internalconfig.RequestScopedErrorRule{{
				Status: 400,
				Match:  []string{"body"},
				Action: "retry",
			}},
		}},
	}

	if service.applyConfigUpdateWithAuthSynthesis(nil, newCfg, true) {
		t.Fatal("hot config application accepted invalid request-scoped error rules")
	}
	if service.cfg != originalCfg {
		t.Fatal("invalid hot config replaced the active config")
	}
	if service.configSequence != 0 {
		t.Fatalf("config sequence = %d, want 0", service.configSequence)
	}
}
