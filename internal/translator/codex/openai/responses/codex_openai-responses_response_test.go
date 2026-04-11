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
