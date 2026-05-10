package claude

import (
	"context"
	"strings"
	"testing"
)

func TestConvertContentFrameToTextDelta(t *testing.T) {
	param := new(any) // translator passes a *any for stateful sequence numbers
	frame := []byte(`{"text":"Hello"}`)
	got := ConvertKiroResponseToClaude(context.Background(), "claude-sonnet-4.5",
		nil, nil, frame, param)
	if len(got) == 0 {
		t.Fatal("expected at least one SSE line")
	}
	joined := string(joinLines(got))
	if !strings.Contains(joined, `"type":"content_block_delta"`) {
		t.Errorf("missing content_block_delta in: %s", joined)
	}
	if !strings.Contains(joined, `"text":"Hello"`) {
		t.Errorf("missing text in: %s", joined)
	}
}

func TestConvertToolUseFrame(t *testing.T) {
	param := new(any)
	frame := []byte(`{"toolUseId":"t1","name":"get_weather","input":{}}`)
	got := ConvertKiroResponseToClaude(context.Background(), "claude-sonnet-4.5",
		nil, nil, frame, param)
	joined := string(joinLines(got))
	if !strings.Contains(joined, `"type":"content_block_start"`) {
		t.Errorf("missing content_block_start: %s", joined)
	}
	if !strings.Contains(joined, `"name":"get_weather"`) {
		t.Errorf("missing tool name: %s", joined)
	}
}

func TestConvertContextUsageEmitsUsageDelta(t *testing.T) {
	param := new(any)
	frame := []byte(`{"contextUsagePercentage":12.5}`)
	got := ConvertKiroResponseToClaude(context.Background(), "claude-sonnet-4.5",
		nil, nil, frame, param)
	joined := string(joinLines(got))
	if !strings.Contains(joined, "output_tokens") {
		t.Errorf("expected usage delta with output_tokens; got: %s", joined)
	}
}

func joinLines(lines [][]byte) []byte {
	var out []byte
	for _, l := range lines {
		out = append(out, l...)
		out = append(out, '\n')
	}
	return out
}
