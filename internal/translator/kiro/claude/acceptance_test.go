package claude

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// TestAcceptance_RoundTrip exercises both the request-side and the response-
// side translators in one test. We:
//  1. Build a Claude request → translate to Kiro body.
//  2. Simulate a Kiro frame stream → translate back to Anthropic SSE events.
//  3. Verify the SSE stream is well-formed.
func TestAcceptance_RoundTrip(t *testing.T) {
	// 1. Request side.
	in := []byte(`{
		"model":"claude-sonnet-4-5",
		"system":"Be concise.",
		"messages":[
			{"role":"user","content":"What's 2+2?"}
		]
	}`)
	kiroBody := ConvertClaudeRequestToKiro("claude-sonnet-4.5", in, true)

	var body KiroRequestBody
	if err := json.Unmarshal(kiroBody, &body); err != nil {
		t.Fatalf("kiro body unmarshal: %v", err)
	}
	if body.ConversationState.CurrentMessage.UserInputMessage.ModelID != "claude-sonnet-4.5" {
		t.Errorf("modelId mismatch: %s", body.ConversationState.CurrentMessage.UserInputMessage.ModelID)
	}
	if !strings.Contains(body.ConversationState.CurrentMessage.UserInputMessage.Content, "Be concise") {
		t.Errorf("system not embedded: %s", body.ConversationState.CurrentMessage.UserInputMessage.Content)
	}

	// 2. Response side: simulate a 3-frame Kiro stream.
	frames := [][]byte{
		[]byte(`{"text":"4"}`),
		[]byte(`{"text":" exact"}`),
		[]byte(`{"contextUsagePercentage":1.5}`),
	}
	param := new(any)
	var allLines [][]byte
	for _, f := range frames {
		lines := ConvertKiroResponseToClaude(context.Background(), "claude-sonnet-4.5",
			in, kiroBody, f, param)
		allLines = append(allLines, lines...)
	}

	merged := strings.Join(asStrings(allLines), "\n")
	// Must include message_start exactly once (count SSE event: lines, not JSON occurrences).
	if got := strings.Count(merged, "event: message_start"); got != 1 {
		t.Errorf("expected 1 message_start SSE event, got %d in:\n%s", got, merged)
	}
	// Must include both text deltas.
	if !strings.Contains(merged, `"text":"4"`) || !strings.Contains(merged, `"text":" exact"`) {
		t.Errorf("text deltas missing:\n%s", merged)
	}
	// Must include usage delta.
	if !strings.Contains(merged, "output_tokens") {
		t.Errorf("usage delta missing:\n%s", merged)
	}
}

func asStrings(b [][]byte) []string {
	out := make([]string, len(b))
	for i, x := range b {
		out[i] = string(x)
	}
	return out
}
