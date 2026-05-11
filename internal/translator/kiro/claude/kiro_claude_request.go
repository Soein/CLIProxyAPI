package claude

import (
	"encoding/json"

	"github.com/google/uuid"
)

// ConvertClaudeRequestToKiro 将 Anthropic Messages JSON 请求转换为
// Kiro generateAssistantResponse JSON 请求体。
// model 参数为 Kiro 侧模型 ID（如 "claude-sonnet-4.5"）。
// 实现 sdktranslator.RequestTransform 接口。
func ConvertClaudeRequestToKiro(model string, rawJSON []byte, _ bool) []byte {
	req, err := ParseAnthropicRequest(rawJSON)
	if err != nil {
		return []byte(`{}`)
	}

	merged := MergeAdjacentSameRole(req.Messages)

	var history []HistoryEntry
	var lastUser *AnthropicMessage
	for i := 0; i < len(merged); i++ {
		m := merged[i]
		if i == len(merged)-1 && m.Role == "user" {
			lastUser = &m
			break
		}
		history = append(history, toHistoryEntry(m))
	}

	var lastUserText string
	if lastUser != nil {
		lastUserText = anthropicContentToString(lastUser.Content)
	}

	// Identity override is ALWAYS injected as defense against Kiro self-
	// identifying ("I'm Kiro IDE..."). User-supplied system prompt (if any)
	// is appended inside the override; the user message comes last.
	systemPrompt := ComposeSystem(req.System)
	combined := systemPrompt
	if lastUserText != "" {
		if combined != "" {
			combined += "\n"
		}
		combined += lastUserText
	}

	var ctx *UserInputMessageContext
	if len(req.Tools) > 0 {
		nameMap := NewToolNameMap()
		var tools []KiroTool
		for _, t := range req.Tools {
			alias, aliased := AliasToolName(t.Name)
			if aliased {
				nameMap.Add(alias, t.Name)
			}
			tools = append(tools, KiroTool{
				ToolSpecification: ToolSpecification{
					Name:        alias,
					Description: TruncateToolDescription(t.Description),
					InputSchema: t.InputSchema,
				},
			})
		}
		ctx = &UserInputMessageContext{Tools: tools}
	}

	if lastUser != nil {
		results := extractToolResults(lastUser.Content)
		if len(results) > 0 {
			if ctx == nil {
				ctx = &UserInputMessageContext{}
			}
			ctx.ToolResults = results
		}
	}

	body := KiroRequestBody{
		ConversationState: ConversationState{
			AgentTaskType:   "vibe",
			ChatTriggerType: "MANUAL",
			ConversationID:  uuid.NewString(),
			CurrentMessage: CurrentMessage{
				UserInputMessage: UserInputMessage{
					Content:                 combined,
					ModelID:                 model,
					Origin:                  "AI_EDITOR",
					UserInputMessageContext: ctx,
				},
			},
			History: history,
		},
	}

	out, _ := json.Marshal(body)
	return out
}

func toHistoryEntry(m AnthropicMessage) HistoryEntry {
	switch m.Role {
	case "user":
		return HistoryEntry{
			UserInputMessage: &UserInputMessage{
				Content: anthropicContentToString(m.Content),
				Origin:  "AI_EDITOR",
			},
		}
	case "assistant":
		return HistoryEntry{
			AssistantResponseMessage: &AssistantResponseMessage{
				Content: anthropicContentToString(m.Content),
			},
		}
	default:
		return HistoryEntry{}
	}
}

// anthropicContentToString 将 Anthropic 的 content 字段（字符串或块数组）展开为纯字符串。
func anthropicContentToString(raw json.RawMessage) string {
	if isJSONArray(raw) {
		var blocks []map[string]any
		if err := json.Unmarshal(raw, &blocks); err != nil {
			return ""
		}
		var sb []byte
		for _, blk := range blocks {
			if blk["type"] == "text" {
				if t, ok := blk["text"].(string); ok {
					if len(sb) > 0 {
						sb = append(sb, '\n')
					}
					sb = append(sb, t...)
				}
			}
		}
		return string(sb)
	}
	var s string
	_ = json.Unmarshal(raw, &s)
	return s
}

// extractToolResults 从 Anthropic content 数组中提取 tool_result 块。
func extractToolResults(raw json.RawMessage) []KiroToolResult {
	if !isJSONArray(raw) {
		return nil
	}
	var blocks []map[string]any
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil
	}
	var out []KiroToolResult
	for _, blk := range blocks {
		if blk["type"] != "tool_result" {
			continue
		}
		id, _ := blk["tool_use_id"].(string)
		var contentRaw json.RawMessage
		if c, ok := blk["content"]; ok {
			contentRaw, _ = json.Marshal(c)
		}
		out = append(out, KiroToolResult{
			ToolUseID: id,
			Status:    "success",
			Content:   contentRaw,
		})
	}
	return out
}
