// Package claude provides the Anthropic Messages ↔ Kiro conversationState
// translator. Kiro speaks Anthropic-shaped requests natively, so this package
// is the bridge that handles message merging, tool aliasing, system prompt
// injection, and event-stream → SSE conversion.
package claude

import (
	"encoding/json"
	"fmt"
)

// --- Kiro upstream request structures ---

// KiroRequestBody is the JSON body POSTed to the Kiro generateAssistantResponse
// endpoint. ProfileArn is empty for builder_id auth.
type KiroRequestBody struct {
	ProfileArn        string            `json:"profileArn,omitempty"`
	ConversationState ConversationState `json:"conversationState"`
}

// ConversationState wraps the entire conversation context for one request.
type ConversationState struct {
	AgentTaskType   string         `json:"agentTaskType"`
	ChatTriggerType string         `json:"chatTriggerType"`
	ConversationID  string         `json:"conversationId"`
	CurrentMessage  CurrentMessage `json:"currentMessage"`
	History         []HistoryEntry `json:"history,omitempty"`
}

// CurrentMessage is the active user turn.
type CurrentMessage struct {
	UserInputMessage UserInputMessage `json:"userInputMessage"`
}

// UserInputMessage carries one user turn's content + metadata.
type UserInputMessage struct {
	Content                 string                   `json:"content"`
	ModelID                 string                   `json:"modelId,omitempty"`
	Origin                  string                   `json:"origin,omitempty"`
	Images                  []KiroImage              `json:"images,omitempty"`
	UserInputMessageContext *UserInputMessageContext `json:"userInputMessageContext,omitempty"`
}

// UserInputMessageContext holds tool definitions and tool results.
type UserInputMessageContext struct {
	Tools       []KiroTool       `json:"tools,omitempty"`
	ToolResults []KiroToolResult `json:"toolResults,omitempty"`
}

// KiroTool wraps a single tool spec.
type KiroTool struct {
	ToolSpecification ToolSpecification `json:"toolSpecification"`
}

// ToolSpecification is the Kiro-specific shape for tool definitions.
type ToolSpecification struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

// KiroToolResult mirrors Anthropic's tool_result content block.
type KiroToolResult struct {
	ToolUseID string          `json:"toolUseId"`
	Status    string          `json:"status,omitempty"`
	Content   json.RawMessage `json:"content"`
}

// KiroImage is the upload format Kiro accepts inline.
type KiroImage struct {
	Format string          `json:"format"`
	Source KiroImageSource `json:"source"`
}

// KiroImageSource holds the base64-encoded image bytes.
type KiroImageSource struct {
	Bytes string `json:"bytes"`
}

// HistoryEntry is one past turn — exactly one of UserInputMessage or
// AssistantResponseMessage is set.
type HistoryEntry struct {
	UserInputMessage         *UserInputMessage         `json:"userInputMessage,omitempty"`
	AssistantResponseMessage *AssistantResponseMessage `json:"assistantResponseMessage,omitempty"`
}

// AssistantResponseMessage is one past assistant turn.
type AssistantResponseMessage struct {
	Content  string        `json:"content"`
	ToolUses []KiroToolUse `json:"toolUses,omitempty"`
}

// KiroToolUse mirrors Anthropic's tool_use content block.
type KiroToolUse struct {
	ToolUseID string          `json:"toolUseId"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
}

// --- Anthropic incoming structures (subset of the public schema we need) ---

// AnthropicRequest is the parsed shape of inbound Anthropic Messages JSON.
type AnthropicRequest struct {
	Model     string             `json:"model"`
	MaxTokens int                `json:"max_tokens,omitempty"`
	System    json.RawMessage    `json:"system,omitempty"` // string OR []{type,text}
	Messages  []AnthropicMessage `json:"messages"`
	Tools     []AnthropicTool    `json:"tools,omitempty"`
	Stream    bool               `json:"stream,omitempty"`
	Thinking  *AnthropicThinking `json:"thinking,omitempty"`
}

// AnthropicMessage is one user/assistant turn from Anthropic.
type AnthropicMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"` // string OR []block
}

// AnthropicTool is one Anthropic tool definition.
type AnthropicTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

// AnthropicThinking carries the optional thinking budget.
type AnthropicThinking struct {
	Type         string `json:"type"`
	BudgetTokens int    `json:"budget_tokens,omitempty"`
}

// ParseAnthropicRequest unmarshals the inbound JSON into AnthropicRequest.
func ParseAnthropicRequest(raw []byte) (*AnthropicRequest, error) {
	var req AnthropicRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, fmt.Errorf("kiro/claude: parse anthropic request: %w", err)
	}
	return &req, nil
}
