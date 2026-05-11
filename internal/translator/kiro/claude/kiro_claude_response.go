package claude

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// streamState is the per-request stateful object passed via *any in the
// translator interface.
type streamState struct {
	MessageStarted bool
	NextBlockIndex int
	OutputTokens   int
	TextBlockOpen  bool           // index 0 reserved for text
	ToolUseIndex   map[string]int // toolUseId → block index
	ToolUseOpen    map[string]bool
	StreamEnded    bool
	// Non-stream aggregator state
	NonStreamText  strings.Builder
	NonStreamTools []nonStreamTool
}

type nonStreamTool struct {
	ID    string
	Name  string
	Input strings.Builder
}

func ensureState(param *any) *streamState {
	if param == nil {
		return newState()
	}
	if s, ok := (*param).(*streamState); ok {
		return s
	}
	s := newState()
	*param = s
	return s
}

func newState() *streamState {
	return &streamState{
		ToolUseIndex: map[string]int{},
		ToolUseOpen:  map[string]bool{},
	}
}

// ConvertKiroResponseToClaude turns one Kiro frame's JSON payload into one or
// more Anthropic SSE event lines. It is called once per frame by the executor.
//
// Implements sdktranslator.ResponseStreamTransform.
//
// Frame kinds handled:
//   - text/content      → content_block_delta (text_delta) into block 0
//   - toolUseId+name    → content_block_start (tool_use) into next index
//   - toolUseId+input   → content_block_delta (input_json_delta)
//   - toolUseId only    → content_block_stop for that tool block
//   - contextUsage…     → terminal: stop any open blocks, message_delta,
//     message_stop. Future frames are no-ops.
func ConvertKiroResponseToClaude(_ context.Context, _ string, _, _, rawJSON []byte, param *any) [][]byte {
	state := ensureState(param)
	if state.StreamEnded {
		return nil
	}
	out := [][]byte{}

	if !state.MessageStarted {
		state.MessageStarted = true
		out = append(out, []byte(`event: message_start
data: {"type":"message_start","message":{"id":"msg_kiro","type":"message","role":"assistant","content":[],"model":"","usage":{"input_tokens":0,"output_tokens":0}}}`))
	}

	var frame map[string]json.RawMessage
	if err := json.Unmarshal(rawJSON, &frame); err != nil {
		return out
	}

	switch {
	// 1. Tool use STOP (only toolUseId, no name/input fields).
	case hasKey(frame, "toolUseId") && !hasKey(frame, "name") && !hasKey(frame, "input"):
		var id string
		_ = json.Unmarshal(frame["toolUseId"], &id)
		if idx, ok := state.ToolUseIndex[id]; ok && state.ToolUseOpen[id] {
			out = append(out, sseLine("content_block_stop", map[string]any{
				"type":  "content_block_stop",
				"index": idx,
			}))
			state.ToolUseOpen[id] = false
		}

	// 2. Tool use INPUT delta (toolUseId + input but no name).
	case hasKey(frame, "toolUseId") && hasKey(frame, "input") && !hasKey(frame, "name"):
		var id string
		_ = json.Unmarshal(frame["toolUseId"], &id)
		idx, ok := state.ToolUseIndex[id]
		if !ok {
			break
		}
		// input may be a JSON string (partial) or an object. Forward as
		// partial_json to match Anthropic streaming convention.
		var partial string
		if err := json.Unmarshal(frame["input"], &partial); err != nil {
			// Object form — re-marshal to compact string.
			partial = string(frame["input"])
		}
		out = append(out, sseLine("content_block_delta", map[string]any{
			"type":  "content_block_delta",
			"index": idx,
			"delta": map[string]any{"type": "input_json_delta", "partial_json": partial},
		}))
		// Aggregate for non-stream mode.
		for i := range state.NonStreamTools {
			if state.NonStreamTools[i].ID == id {
				state.NonStreamTools[i].Input.WriteString(partial)
				break
			}
		}

	// 3. Tool use START (toolUseId + name).
	case hasKey(frame, "toolUseId") && hasKey(frame, "name"):
		var id, name string
		_ = json.Unmarshal(frame["toolUseId"], &id)
		_ = json.Unmarshal(frame["name"], &name)
		// Close the text block first if still open (Anthropic disallows
		// interleaved deltas across content blocks at the same index).
		if state.TextBlockOpen {
			out = append(out, sseLine("content_block_stop", map[string]any{
				"type":  "content_block_stop",
				"index": 0,
			}))
			state.TextBlockOpen = false
		}
		idx := state.NextBlockIndex
		if idx == 0 {
			idx = 1
			state.NextBlockIndex = 1
		}
		state.ToolUseIndex[id] = idx
		state.ToolUseOpen[id] = true
		state.NextBlockIndex = idx + 1
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
		// Track for non-stream mode.
		state.NonStreamTools = append(state.NonStreamTools, nonStreamTool{ID: id, Name: name})

	// 4. Text delta.
	case hasKey(frame, "text") || hasKey(frame, "content"):
		var text string
		if v, ok := frame["text"]; ok {
			_ = json.Unmarshal(v, &text)
		} else {
			_ = json.Unmarshal(frame["content"], &text)
		}
		if !state.TextBlockOpen {
			out = append(out, sseLine("content_block_start", map[string]any{
				"type":          "content_block_start",
				"index":         0,
				"content_block": map[string]any{"type": "text", "text": ""},
			}))
			state.TextBlockOpen = true
			if state.NextBlockIndex == 0 {
				state.NextBlockIndex = 1
			}
		}
		out = append(out, sseLine("content_block_delta", map[string]any{
			"type":  "content_block_delta",
			"index": 0,
			"delta": map[string]any{"type": "text_delta", "text": text},
		}))
		state.NonStreamText.WriteString(text)

	// 5. Stream-end signal.
	case hasKey(frame, "contextUsagePercentage"):
		var pct float64
		_ = json.Unmarshal(frame["contextUsagePercentage"], &pct)
		state.OutputTokens = int(pct / 100.0 * 200_000)

		// Close any open text block.
		if state.TextBlockOpen {
			out = append(out, sseLine("content_block_stop", map[string]any{
				"type":  "content_block_stop",
				"index": 0,
			}))
			state.TextBlockOpen = false
		}
		// Close any open tool blocks (Kiro may end stream without explicit
		// per-tool stop frames).
		for id, open := range state.ToolUseOpen {
			if open {
				out = append(out, sseLine("content_block_stop", map[string]any{
					"type":  "content_block_stop",
					"index": state.ToolUseIndex[id],
				}))
				state.ToolUseOpen[id] = false
			}
		}

		stopReason := "end_turn"
		if len(state.NonStreamTools) > 0 {
			stopReason = "tool_use"
		}
		out = append(out, sseLine("message_delta", map[string]any{
			"type":  "message_delta",
			"delta": map[string]any{"stop_reason": stopReason, "stop_sequence": nil},
			"usage": map[string]any{"output_tokens": state.OutputTokens},
		}))
		out = append(out, sseLine("message_stop", map[string]any{
			"type": "message_stop",
		}))
		state.StreamEnded = true
	}

	return out
}

// ConvertKiroResponseToClaudeNonStream aggregates frames into a single
// Anthropic Messages-compatible JSON response. It piggybacks on the same
// state machine (the executor calls it for each frame; the FINAL frame's
// return value is the full response).
//
// Implements sdktranslator.ResponseNonStreamTransform.
func ConvertKiroResponseToClaudeNonStream(ctx context.Context, model string, originalReq, transReq, rawJSON []byte, param *any) []byte {
	// Reuse the streaming translator to update state. Discard SSE lines.
	_ = ConvertKiroResponseToClaude(ctx, model, originalReq, transReq, rawJSON, param)

	state := ensureState(param)
	if !state.StreamEnded {
		// Only emit the final JSON when we know the stream ended.
		return nil
	}

	// Build content array.
	content := []map[string]any{}
	if state.NonStreamText.Len() > 0 {
		content = append(content, map[string]any{
			"type": "text",
			"text": state.NonStreamText.String(),
		})
	}
	for _, tool := range state.NonStreamTools {
		var inputObj any
		raw := strings.TrimSpace(tool.Input.String())
		if raw != "" {
			if err := json.Unmarshal([]byte(raw), &inputObj); err != nil {
				inputObj = raw
			}
		} else {
			inputObj = map[string]any{}
		}
		content = append(content, map[string]any{
			"type":  "tool_use",
			"id":    tool.ID,
			"name":  tool.Name,
			"input": inputObj,
		})
	}

	stopReason := "end_turn"
	if len(state.NonStreamTools) > 0 {
		stopReason = "tool_use"
	}
	resp := map[string]any{
		"id":            "msg_kiro",
		"type":          "message",
		"role":          "assistant",
		"model":         model,
		"content":       content,
		"stop_reason":   stopReason,
		"stop_sequence": nil,
		"usage":         map[string]any{"input_tokens": 0, "output_tokens": state.OutputTokens},
	}
	out, _ := json.Marshal(resp)
	return out
}

// ClaudeTokenCount is a placeholder used by the translator registry.
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
