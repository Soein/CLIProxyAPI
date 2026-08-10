package responses

import (
	"context"
	"testing"

	"github.com/tidwall/gjson"
)

func TestConvertCodexResponseToOpenAIResponses_TransformsErrorEvent(t *testing.T) {
	out := ConvertCodexResponseToOpenAIResponses(
		context.Background(),
		"gpt-5.4",
		nil,
		nil,
		[]byte(`data: {"error":{"message":"boom","type":"invalid_request_error"}}`),
		nil,
	)

	if len(out) != 1 {
		t.Fatalf("chunks = %d, want 1", len(out))
	}

	if got := gjson.GetBytes(out[0], "type").String(); got != "error" {
		t.Fatalf("type = %q, want %q: %s", got, "error", string(out[0]))
	}

	if got := gjson.GetBytes(out[0], "error.message").String(); got != "boom" {
		t.Fatalf("error.message = %q, want %q: %s", got, "boom", string(out[0]))
	}
}

func TestConvertCodexResponseToOpenAIResponses_CreatedIncludesOriginalRequestModel(t *testing.T) {
	request := []byte(`{"model":"original-codex-model"}`)
	translatedRequest := []byte(`{"model":"translated-codex-model"}`)
	for eventName, raw := range map[string][]byte{
		"response.created":     []byte(`data: {"type":"response.created","response":{"id":"resp_1"}}`),
		"response.in_progress": []byte(`data: {"type":"response.in_progress","response":{"id":"resp_1"}}`),
	} {
		outputs := ConvertCodexResponseToOpenAIResponses(context.Background(), "fallback-model", request, translatedRequest, raw, nil)
		if len(outputs) != 1 {
			t.Fatalf("%s outputs = %d, want 1", eventName, len(outputs))
		}
		if got := gjson.GetBytes(outputs[0], "response.model").String(); got != "original-codex-model" {
			t.Fatalf("%s models = %q, want original-codex-model; payload=%s", eventName, got, outputs[0])
		}
	}
}

func TestConvertCodexResponseToOpenAIResponsesNonStreamIncomplete(t *testing.T) {
	raw := []byte(`{"type":"response.incomplete","response":{"id":"resp_1","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}`)

	out := ConvertCodexResponseToOpenAIResponsesNonStream(context.Background(), "gpt-5.5", nil, nil, raw, nil)

	if got := gjson.GetBytes(out, "status").String(); got != "incomplete" {
		t.Fatalf("status = %q, want incomplete; payload=%s", got, out)
	}
	if got := gjson.GetBytes(out, "incomplete_details.reason").String(); got != "max_output_tokens" {
		t.Fatalf("incomplete reason = %q, want max_output_tokens; payload=%s", got, out)
	}
}
