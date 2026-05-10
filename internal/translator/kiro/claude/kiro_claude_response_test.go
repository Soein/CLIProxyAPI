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

func TestConvertToolUseInputDelta(t *testing.T) {
	param := new(any)
	// First emit toolUse start to register the toolUseId.
	_ = ConvertKiroResponseToClaude(context.Background(), "claude-sonnet-4.5", nil, nil,
		[]byte(`{"toolUseId":"t1","name":"get_weather","input":{}}`), param)
	// Then input delta.
	got := ConvertKiroResponseToClaude(context.Background(), "claude-sonnet-4.5", nil, nil,
		[]byte(`{"toolUseId":"t1","input":"{\"city\":\"Tokyo\"}"}`), param)
	joined := string(joinLines(got))
	if !strings.Contains(joined, `"type":"input_json_delta"`) {
		t.Errorf("missing input_json_delta in: %s", joined)
	}
	if !strings.Contains(joined, `"partial_json"`) {
		t.Errorf("missing partial_json field in: %s", joined)
	}
}

func TestConvertToolUseStop(t *testing.T) {
	param := new(any)
	_ = ConvertKiroResponseToClaude(context.Background(), "claude-sonnet-4.5", nil, nil,
		[]byte(`{"toolUseId":"t1","name":"get_weather","input":{}}`), param)
	got := ConvertKiroResponseToClaude(context.Background(), "claude-sonnet-4.5", nil, nil,
		[]byte(`{"toolUseId":"t1"}`), param)
	joined := string(joinLines(got))
	if !strings.Contains(joined, `"type":"content_block_stop"`) {
		t.Errorf("missing content_block_stop in: %s", joined)
	}
}

func TestConvertContextUsageEmitsStreamEnd(t *testing.T) {
	param := new(any)
	// Emit one text frame so a text block is open.
	_ = ConvertKiroResponseToClaude(context.Background(), "claude-sonnet-4.5", nil, nil,
		[]byte(`{"text":"hello"}`), param)
	// Now contextUsage signals end of stream.
	got := ConvertKiroResponseToClaude(context.Background(), "claude-sonnet-4.5", nil, nil,
		[]byte(`{"contextUsagePercentage":1.5}`), param)
	joined := string(joinLines(got))
	if !strings.Contains(joined, `"type":"content_block_stop"`) {
		t.Errorf("missing content_block_stop in: %s", joined)
	}
	if !strings.Contains(joined, `"type":"message_delta"`) {
		t.Errorf("missing message_delta in: %s", joined)
	}
	if !strings.Contains(joined, `"stop_reason":"end_turn"`) {
		t.Errorf("missing end_turn stop_reason in: %s", joined)
	}
	if !strings.Contains(joined, `"type":"message_stop"`) {
		t.Errorf("missing message_stop in: %s", joined)
	}
}

func TestConvertContextUsageWithToolStopReason(t *testing.T) {
	param := new(any)
	// Open tool_use block.
	_ = ConvertKiroResponseToClaude(context.Background(), "claude-sonnet-4.5", nil, nil,
		[]byte(`{"toolUseId":"t1","name":"get_weather","input":{}}`), param)
	// End stream.
	got := ConvertKiroResponseToClaude(context.Background(), "claude-sonnet-4.5", nil, nil,
		[]byte(`{"contextUsagePercentage":1.0}`), param)
	joined := string(joinLines(got))
	if !strings.Contains(joined, `"stop_reason":"tool_use"`) {
		t.Errorf("expected tool_use stop_reason in: %s", joined)
	}
}

func TestConvertNonStreamReturnsAnthropicJSON(t *testing.T) {
	// Non-stream variant should aggregate frames into Anthropic Messages JSON.
	param := new(any)
	frames := [][]byte{
		[]byte(`{"text":"Hello "}`),
		[]byte(`{"text":"world"}`),
		[]byte(`{"contextUsagePercentage":1.0}`),
	}
	var lastOut []byte
	for _, f := range frames {
		lastOut = ConvertKiroResponseToClaudeNonStream(context.Background(),
			"claude-sonnet-4.5", nil, nil, f, param)
	}
	// Final output should be a single Anthropic message JSON.
	if !strings.Contains(string(lastOut), `"role":"assistant"`) {
		t.Errorf("non-stream missing role=assistant: %s", lastOut)
	}
	if !strings.Contains(string(lastOut), "Hello world") {
		t.Errorf("non-stream missing concatenated text: %s", lastOut)
	}
	if !strings.Contains(string(lastOut), `"stop_reason"`) {
		t.Errorf("non-stream missing stop_reason: %s", lastOut)
	}
}
