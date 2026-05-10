package claude

import (
	"context"
	"encoding/json"
	"fmt"
)

// streamState is the per-request stateful object passed via *any in the
// translator interface. It tracks block indices, accumulated tokens, and
// whether we've emitted message_start.
type streamState struct {
	MessageStarted bool
	NextBlockIndex int
	OutputTokens   int
	// Maps Kiro toolUseId → block index (so toolUseStop closes the right block).
	ToolUseIndex map[string]int
}

func ensureState(param *any) *streamState {
	if param == nil {
		return &streamState{ToolUseIndex: map[string]int{}}
	}
	if s, ok := (*param).(*streamState); ok {
		return s
	}
	s := &streamState{ToolUseIndex: map[string]int{}}
	*param = s
	return s
}

// ConvertKiroResponseToClaude turns one Kiro frame's JSON payload into one or
// more Anthropic SSE event lines. It is called once per frame by the executor.
//
// Implements sdktranslator.ResponseStreamTransform.
func ConvertKiroResponseToClaude(_ context.Context, _ string, _, _, rawJSON []byte, param *any) [][]byte {
	state := ensureState(param)
	out := [][]byte{}

	// Emit message_start exactly once, on the first frame.
	if !state.MessageStarted {
		state.MessageStarted = true
		out = append(out, []byte(`event: message_start
data: {"type":"message_start","message":{"id":"msg_kiro","type":"message","role":"assistant","content":[],"model":"","usage":{"input_tokens":0,"output_tokens":0}}}`))
	}

	// Detect frame kind by JSON keys.
	var frame map[string]json.RawMessage
	if err := json.Unmarshal(rawJSON, &frame); err != nil {
		return out
	}

	switch {
	case hasKey(frame, "text") || hasKey(frame, "content"):
		var text string
		if v, ok := frame["text"]; ok {
			_ = json.Unmarshal(v, &text)
		} else {
			_ = json.Unmarshal(frame["content"], &text)
		}
		// Open a text block on first text frame.
		if state.NextBlockIndex == 0 {
			out = append(out, sseLine("content_block_start", map[string]any{
				"type":          "content_block_start",
				"index":         0,
				"content_block": map[string]any{"type": "text", "text": ""},
			}))
			state.NextBlockIndex = 1
		}
		out = append(out, sseLine("content_block_delta", map[string]any{
			"type":  "content_block_delta",
			"index": 0,
			"delta": map[string]any{"type": "text_delta", "text": text},
		}))

	case hasKey(frame, "toolUseId") && hasKey(frame, "name"):
		var id, name string
		_ = json.Unmarshal(frame["toolUseId"], &id)
		_ = json.Unmarshal(frame["name"], &name)
		idx := state.NextBlockIndex
		state.ToolUseIndex[id] = idx
		state.NextBlockIndex++
		out = append(out, sseLine("content_block_start", map[string]any{
			"type":  "content_block_start",
			"index": idx,
			"content_block": map[string]any{
				"type":  "tool_use",
				"id":    id,
				"name":  name,
				"input": map[string]any{},
			},
		}))

	case hasKey(frame, "contextUsagePercentage"):
		var pct float64
		_ = json.Unmarshal(frame["contextUsagePercentage"], &pct)
		// Crude estimate: assume 200k context, so output_tokens ≈ pct/100 * 200000.
		state.OutputTokens = int(pct / 100.0 * 200_000)
		out = append(out, sseLine("message_delta", map[string]any{
			"type":  "message_delta",
			"delta": map[string]any{},
			"usage": map[string]any{"output_tokens": state.OutputTokens},
		}))
	}

	return out
}

// ConvertKiroResponseToClaudeNonStream is a thin wrapper that ignores stream
// state and returns the final Anthropic Messages JSON. M2c may extend this;
// for M2b we just emit the input frame as-is so the registration compiles.
func ConvertKiroResponseToClaudeNonStream(_ context.Context, _ string, _, _, rawJSON []byte, _ *any) []byte {
	return rawJSON
}

// ClaudeTokenCount is a no-op for now; M2c may compute tokens.
func ClaudeTokenCount(_ context.Context, count int64) []byte {
	return []byte(fmt.Sprintf(`{"input_tokens":0,"output_tokens":%d}`, count))
}

func sseLine(eventType string, payload map[string]any) []byte {
	body, _ := json.Marshal(payload)
	return []byte(fmt.Sprintf("event: %s\ndata: %s", eventType, body))
}

func hasKey(m map[string]json.RawMessage, key string) bool {
	_, ok := m[key]
	return ok
}
