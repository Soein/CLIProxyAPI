package cliproxy

import (
	"strings"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestBuilderBuildRejectsInvalidRequestScopedErrorRules(t *testing.T) {
	cfg := &internalconfig.Config{
		ClaudeKey: []internalconfig.ClaudeKey{{
			APIKey: "claude-key",
			RequestScopedErrors: []internalconfig.RequestScopedErrorRule{{
				Status: 400,
				Match:  []string{"body"},
				Action: "retry",
			}},
		}},
	}

	service, errBuild := NewBuilder().
		WithConfig(cfg).
		WithConfigPath(t.TempDir() + "/config.yaml").
		Build()
	if errBuild == nil {
		t.Fatal("Build() accepted invalid request-scoped error rules")
	}
	if service != nil {
		t.Fatal("Build() returned a service for invalid request-scoped error rules")
	}
	if !strings.Contains(errBuild.Error(), "cliproxy: validate request-scoped error rules: claude-api-key[0].request-scoped-errors[0].action") {
		t.Fatalf("Build() error = %q, want contextual request-scoped rule path", errBuild)
	}
}
