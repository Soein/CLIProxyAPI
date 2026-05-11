package claude

import (
	"encoding/json"
	"testing"
)

func TestKiroRequestBodyMarshalShape(t *testing.T) {
	body := KiroRequestBody{
		ProfileArn: "arn:profile",
		ConversationState: ConversationState{
			AgentTaskType:   "vibe",
			ChatTriggerType: "MANUAL",
			ConversationID:  "c1",
			CurrentMessage: CurrentMessage{
				UserInputMessage: UserInputMessage{
					Content: "hi",
					ModelID: "claude-sonnet-4.5",
					Origin:  "AI_EDITOR",
				},
			},
		},
	}
	out, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"profileArn":"arn:profile","conversationState":{"agentTaskType":"vibe","chatTriggerType":"MANUAL","conversationId":"c1","currentMessage":{"userInputMessage":{"content":"hi","modelId":"claude-sonnet-4.5","origin":"AI_EDITOR"}}}}`
	if string(out) != want {
		t.Errorf("got: %s\nwant: %s", out, want)
	}
}

func TestAnthropicRequestParseSimple(t *testing.T) {
	raw := []byte(`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hello"}]}`)
	got, err := ParseAnthropicRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Model != "claude-sonnet-4-5" {
		t.Errorf("Model = %q; want claude-sonnet-4-5", got.Model)
	}
	if len(got.Messages) != 1 || got.Messages[0].Role != "user" {
		t.Errorf("Messages = %+v", got.Messages)
	}
}
