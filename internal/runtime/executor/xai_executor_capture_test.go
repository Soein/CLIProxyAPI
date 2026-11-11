package executor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/MoeForge/moego/ojson"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

type xaiCaptureRoundTripper func(*http.Request) (*http.Response, error)

func (f xaiCaptureRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type xaiCaptureCheckpointReader struct {
	io.Reader
	checkpoint func()
}

func (r *xaiCaptureCheckpointReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF && r.checkpoint != nil {
		r.checkpoint()
		r.checkpoint = nil
	}
	return n, err
}

func TestXAIExecutorExecuteCapturesSSEIncrementallyWithinLoggingLimits(t *testing.T) {
	for _, policy := range []string{"deferred", "commercial", "disabled"} {
		t.Run(policy, func(t *testing.T) {
			capture := logging.NewDeferredAPICapture()
			ctx := logging.WithDeferredAPICapture(context.Background(), capture)
			cfg := &config.Config{}
			if policy == "commercial" {
				cfg.CommercialMode = true
			} else if policy == "disabled" {
				ctx = logging.WithDeferredAPIRequestCaptureDisabled(ctx)
			}
			prefix := strings.Repeat(": heartbeat\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"working\"}\n\n", 4096)
			// Non-SSE lines after an SSE marker must not restart bare-JSON capture.
			prefix += strings.Repeat("{\"ignored\":\""+strings.Repeat("x", 1024)+"\"}\n", 128)
			checked := false
			var capturedLength, capturedCapacity int
			reader := &xaiCaptureCheckpointReader{
				Reader: strings.NewReader(prefix),
				checkpoint: func() {
					checked = true
					length, capacity, truncated := capture.AttemptBodyStats(0)
					capturedLength, capturedCapacity = length, capacity
					if policy != "deferred" {
						if length != 0 || capacity != 0 || capture.TotalResponseBodyBytes() != 0 {
							t.Errorf("disabled response capture retained body: length=%d capacity=%d", length, capacity)
						}
						return
					}
					if length == 0 || !truncated {
						t.Errorf("sustained SSE was not captured before terminal: length=%d truncated=%t", length, truncated)
					}
					if length > logging.MaxDeferredAPIResponseBodyBytes {
						t.Errorf("response capture exceeded per-attempt bound: length=%d", length)
					}
					if capture.TotalResponseBodyBytes() > logging.MaxTotalDeferredAPIResponseBodyBytes {
						t.Error("response capture exceeded per-request bound")
					}
				},
			}
			tail := &xaiCaptureCheckpointReader{
				Reader: strings.NewReader(prefix),
				checkpoint: func() {
					length, capacity, _ := capture.AttemptBodyStats(0)
					if length != capturedLength || capacity != capturedCapacity {
						t.Errorf("capture grew after reaching its bound: length %d -> %d capacity %d -> %d", capturedLength, length, capturedCapacity, capacity)
					}
				},
			}
			terminal := "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_capture\",\"status\":\"completed\",\"output\":[]}}\n\n"
			ctx = context.WithValue(ctx, "cliproxy.roundtripper", xaiCaptureRoundTripper(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": {"text/event-stream"}},
					Body:       io.NopCloser(io.MultiReader(reader, tail, strings.NewReader(terminal))),
					Request:    req,
				}, nil
			}))
			resp, err := NewXAIExecutor(cfg).Execute(ctx, &cliproxyauth.Auth{
				ID: "xai-capture", Attributes: map[string]string{"base_url": "https://xai.example"},
			}, cliproxyexecutor.Request{
				Model: "grok-4.7", Payload: []byte(`{"input":"hello"}`),
			}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if !checked || ojson.GetBytes(resp.Payload, "status").String() != "completed" {
				t.Fatalf("long SSE did not reach completed response: checked=%t payload=%s", checked, resp.Payload)
			}
		})
	}
}

func TestXAIExecutorExecuteBoundsBareJSONFallback(t *testing.T) {
	for _, contentType := range []string{"application/json", "text/event-stream", ""} {
		t.Run(contentType, func(t *testing.T) {
			for _, oversized := range []bool{false, true} {
				name := "within limit"
				message := "Request blocked."
				if oversized {
					name = "over limit"
					message = strings.Repeat("x", logging.MaxDeferredAPIResponseBodyBytes)
				}
				t.Run(name, func(t *testing.T) {
					body := "{\n\"code\":\"permission-denied\",\n\"error\":\"" + message + "\"\n}"
					ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", xaiCaptureRoundTripper(func(req *http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
					}))
					_, err := NewXAIExecutor(&config.Config{}).Execute(ctx, &cliproxyauth.Auth{
						ID: "xai-json-capture", Attributes: map[string]string{"base_url": "https://xai.example"},
					}, cliproxyexecutor.Request{Model: "grok-4.7", Payload: []byte(`{"input":"hello"}`)}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
					if err == nil {
						t.Fatal("Execute() error = nil, want bare-JSON upstream error")
					}
					var status cliproxyexecutor.StatusError
					if !errors.As(err, &status) {
						t.Fatalf("error has no status: %T %v", err, err)
					}
					if oversized {
						var scoped cliproxyexecutor.RequestScopedError
						if status.StatusCode() != http.StatusBadGateway || !errors.As(err, &scoped) || !scoped.IsRequestScoped() || !strings.Contains(err.Error(), "exceeds") {
							t.Fatalf("oversized bare JSON error = %T %v, want explicit request-scoped 502 size error", err, err)
						}
						return
					}
					if status.StatusCode() != http.StatusForbidden || ojson.Get(err.Error(), "error.message").String() != "Request blocked." {
						t.Fatalf("bare JSON error lost classification or details: %v", err)
					}
				})
			}
		})
	}
}
