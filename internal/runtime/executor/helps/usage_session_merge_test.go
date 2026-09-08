package helps

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
)

func TestUsageReporterPreservesCallerScopedSelectedHierarchy(t *testing.T) {
	for _, child := range []bool{false, true} {
		name := "parent"
		if child {
			name = "child"
		}
		t.Run(name, func(t *testing.T) {
			selector := cliproxyauth.NewSessionAffinitySelector(&cliproxyauth.RoundRobinSelector{})
			t.Cleanup(selector.Stop)
			headers := http.Header{"X-Claude-Code-Session-Id": {"parent-session"}}
			if child {
				headers.Set("X-Claude-Code-Agent-Id", "child-agent")
			}
			opts := cliproxyexecutor.Options{
				Headers:  headers,
				Metadata: map[string]any{cliproxyexecutor.CallerScopeMetadataKey: "caller-a"},
			}
			selected, errPick := selector.Pick(context.Background(), "claude", "model", opts, []*cliproxyauth.Auth{
				{ID: "usage-hierarchy", Provider: "claude", Status: cliproxyauth.StatusActive},
			})
			if errPick != nil || selected == nil {
				t.Fatalf("selection failed: %v", errPick)
			}
			sessionID, _ := opts.Metadata[cliproxyexecutor.CanonicalSessionIDMetadataKey].(string)
			parentID, _ := opts.Metadata[cliproxyexecutor.ParentSessionIDMetadataKey].(string)
			ctx := logging.WithClientRequestMetadata(context.Background(), logging.ClientRequestMetadata{
				SessionID: sessionID, ParentSessionID: parentID,
			})
			reporter := NewUsageReporter(ctx, "claude", "model", nil)
			record := reporter.buildRecord(usage.Detail{TotalTokens: 1}, false, usage.Failure{})
			if record.SessionID != sessionID || !strings.HasPrefix(record.SessionID, "caller:") {
				t.Fatalf("usage lost caller-scoped identity: %q", record.SessionID)
			}
			if child {
				if !strings.HasPrefix(record.ParentSessionID, "caller:") || record.ParentSessionID != parentID || parentID == sessionID {
					t.Fatalf("usage lost selected parent hierarchy: session=%q parent=%q selected parent=%q", record.SessionID, record.ParentSessionID, parentID)
				}
			} else if record.ParentSessionID != "" {
				t.Fatalf("root session acquired a parent: %q", record.ParentSessionID)
			}
		})
	}
}
