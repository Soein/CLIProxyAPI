package claude

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestConvertSimpleRequest(t *testing.T) {
	in := []byte(`{
		"model":"claude-sonnet-4-5",
		"messages":[{"role":"user","content":"hello"}]
	}`)
	out := ConvertClaudeRequestToKiro("claude-sonnet-4.5", in, true)

	var body KiroRequestBody
	if err := json.Unmarshal(out, &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body.ConversationState.AgentTaskType != "vibe" {
		t.Errorf("agentTaskType = %q; want vibe", body.ConversationState.AgentTaskType)
	}
	if body.ConversationState.ChatTriggerType != "MANUAL" {
		t.Errorf("chatTriggerType = %q; want MANUAL", body.ConversationState.ChatTriggerType)
	}
	if body.ConversationState.ConversationID == "" {
		t.Errorf("conversationId empty")
	}
	if body.ConversationState.CurrentMessage.UserInputMessage.Content != "hello" {
		t.Errorf("content = %q; want hello", body.ConversationState.CurrentMessage.UserInputMessage.Content)
	}
	if body.ConversationState.CurrentMessage.UserInputMessage.ModelID != "claude-sonnet-4.5" {
		t.Errorf("modelId = %q; want claude-sonnet-4.5", body.ConversationState.CurrentMessage.UserInputMessage.ModelID)
	}
	if body.ConversationState.CurrentMessage.UserInputMessage.Origin != "AI_EDITOR" {
		t.Errorf("origin = %q; want AI_EDITOR", body.ConversationState.CurrentMessage.UserInputMessage.Origin)
	}
}

func TestConvertSystemPromptInjected(t *testing.T) {
	in := []byte(`{
		"model":"claude-sonnet-4-5",
		"system":"You are senior dev",
		"messages":[{"role":"user","content":"hi"}]
	}`)
	out := ConvertClaudeRequestToKiro("claude-sonnet-4.5", in, true)
	var body KiroRequestBody
	_ = json.Unmarshal(out, &body)
	content := body.ConversationState.CurrentMessage.UserInputMessage.Content
	if !strings.Contains(content, "<CRITICAL_OVERRIDE>") {
		t.Errorf("identity override missing from content: %q", content)
	}
	if !strings.Contains(content, "senior dev") {
		t.Errorf("user system text missing: %q", content)
	}
	if !strings.HasSuffix(content, "hi") {
		t.Errorf("user message should be at end: %q", content)
	}
}

func TestConvertHistoryAndCurrent(t *testing.T) {
	in := []byte(`{
		"model":"claude-sonnet-4-5",
		"messages":[
			{"role":"user","content":"q1"},
			{"role":"assistant","content":"a1"},
			{"role":"user","content":"q2"}
		]
	}`)
	out := ConvertClaudeRequestToKiro("claude-sonnet-4.5", in, true)
	var body KiroRequestBody
	_ = json.Unmarshal(out, &body)
	if body.ConversationState.CurrentMessage.UserInputMessage.Content != "q2" {
		t.Errorf("current message should be q2; got %q", body.ConversationState.CurrentMessage.UserInputMessage.Content)
	}
	if len(body.ConversationState.History) != 2 {
		t.Fatalf("history len = %d; want 2", len(body.ConversationState.History))
	}
	if body.ConversationState.History[0].UserInputMessage == nil ||
		body.ConversationState.History[0].UserInputMessage.Content != "q1" {
		t.Errorf("history[0] = %+v", body.ConversationState.History[0])
	}
	if body.ConversationState.History[1].AssistantResponseMessage == nil ||
		body.ConversationState.History[1].AssistantResponseMessage.Content != "a1" {
		t.Errorf("history[1] = %+v", body.ConversationState.History[1])
	}
}

func TestConvertWithToolsAliasesLongName(t *testing.T) {
	longName := strings.Repeat("x", 80)
	in := []byte(`{
		"model":"claude-sonnet-4-5",
		"messages":[{"role":"user","content":"x"}],
		"tools":[{"name":"` + longName + `","description":"d","input_schema":{"type":"object"}}]
	}`)
	out := ConvertClaudeRequestToKiro("claude-sonnet-4.5", in, true)
	var body KiroRequestBody
	_ = json.Unmarshal(out, &body)
	tools := body.ConversationState.CurrentMessage.UserInputMessage.UserInputMessageContext.Tools
	if len(tools) != 1 {
		t.Fatalf("tools len = %d; want 1", len(tools))
	}
	gotName := tools[0].ToolSpecification.Name
	if len(gotName) > MaxToolNameLength {
		t.Errorf("aliased name still > %d chars: %q", MaxToolNameLength, gotName)
	}
	if gotName == longName {
		t.Errorf("name not aliased")
	}
}
