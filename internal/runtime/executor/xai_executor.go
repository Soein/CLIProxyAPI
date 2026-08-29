package executor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var (
	xaiDataTag  = []byte("data:")
	xaiEventTag = []byte("event:")
)

func isXAITerminalResponseEvent(eventType string) bool {
	switch eventType {
	case "response.completed", "response.incomplete", "response.failed", "response.done", "error":
		return true
	default:
		return false
	}
}

func xaiNormalizeTerminalResponseData(eventData []byte) []byte {
	if gjson.GetBytes(eventData, "type").String() != "response.done" {
		return eventData
	}
	normalized, errSet := sjson.SetBytes(eventData, "type", "response.completed")
	if errSet != nil {
		return eventData
	}
	return normalized
}

func xaiTerminalFailureErr(eventData []byte) (statusErr, bool) {
	body, ok := codexTerminalFailureBody(eventData)
	if !ok {
		return statusErr{}, false
	}
	status := int(gjson.GetBytes(eventData, "status").Int())
	if status < 400 || status > 599 {
		status = int(gjson.GetBytes(eventData, "status_code").Int())
	}
	if status < 400 || status > 599 {
		status = codexTerminalFailureStatus(body)
	}
	terminalErr := xaiStatusErr(status, body)
	if status == http.StatusBadGateway {
		freeUsageErr := xaiStatusErr(http.StatusTooManyRequests, body)
		if freeUsageErr.RetryAfter() != nil {
			terminalErr = freeUsageErr
		}
	}
	return terminalErr, true
}

func xaiTerminalKind(eventType string) string {
	switch eventType {
	case "response.completed":
		return "completed"
	case "response.incomplete":
		return "incomplete"
	case "response.failed":
		return "failed"
	case "response.done":
		return "done"
	case "error":
		return "error"
	default:
		return ""
	}
}

func xaiFailureTerminalKind(ctx context.Context, err error) string {
	if (ctx != nil && ctx.Err() != nil) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "canceled"
	}
	return "error"
}

type xaiResponsePhaseMarker struct {
	reporter      *helps.UsageReporter
	firstEvent    bool
	firstSemantic bool
}

func (m *xaiResponsePhaseMarker) mark(eventType string, eventData []byte, allowUntyped bool) {
	if m == nil || m.reporter == nil || len(eventData) == 0 || (eventType == "" && !allowUntyped) {
		return
	}
	if !m.firstEvent {
		m.reporter.MarkFirstEvent()
		m.firstEvent = true
	}
	if m.firstSemantic || eventType == "" {
		return
	}
	if xaiResponseEventHasSemanticContent(eventType, eventData) {
		m.reporter.MarkFirstSemanticToken()
		m.firstSemantic = true
	}
}

func xaiResponseEventHasSemanticContent(eventType string, eventData []byte) bool {
	switch eventType {
	case "response.output_text.delta",
		"response.reasoning_text.delta",
		"response.reasoning_summary_text.delta",
		"response.function_call_arguments.delta",
		"response.custom_tool_call_input.delta":
		return gjson.GetBytes(eventData, "delta").String() != ""
	case "response.output_item.done":
		return xaiOutputItemHasSemanticContent(gjson.GetBytes(eventData, "item"))
	case "response.completed", "response.incomplete", "response.done":
		for _, item := range gjson.GetBytes(eventData, "response.output").Array() {
			if xaiOutputItemHasSemanticContent(item) {
				return true
			}
		}
	}
	return false
}

func xaiOutputItemHasSemanticContent(item gjson.Result) bool {
	switch item.Get("type").String() {
	case "message":
		for _, content := range item.Get("content").Array() {
			if content.Get("type").String() == "output_text" && content.Get("text").String() != "" {
				return true
			}
		}
	case "reasoning":
		for _, path := range []string{"summary", "content"} {
			for _, content := range item.Get(path).Array() {
				if content.Get("text").String() != "" {
					return true
				}
			}
		}
	case "function_call":
		return item.Get("arguments").String() != ""
	case "custom_tool_call":
		return item.Get("input").String() != ""
	}
	return false
}

const (
	xaiImageHandlerType        = "openai-image"
	xaiVideoHandlerType        = "openai-video"
	xaiCustomToolType          = "custom"
	xaiFunctionToolType        = "function"
	xaiImageGenerationToolType = "image_generation"
	xaiNamespaceToolType       = "namespace"
	xaiToolSearchType          = "tool_search"
	xaiWebSearchToolType       = "web_search"
	xaiXSearchToolType         = "x_search"
	xaiMaxToolsPerRequest      = 200
	xaiWebSearchUpstreamCost   = 4
	xaiMaxTools                = xaiMaxToolsPerRequest
	// Codex Desktop injects codex_app.automation_update with a large oneOf+$ref
	// schema. xAI's free/build Responses path accepts the HTTP request but never
	// emits SSE when that schema is present, so Desktop hangs on "thinking".
	xaiCodexAppNamespaceName    = "codex_app"
	xaiAutomationUpdateToolName = "automation_update"
	// Permissive placeholder schema: keeps the tool callable without the hang.
	xaiSafeFunctionParameters   = `{"type":"object","properties":{},"additionalProperties":true}`
	xaiImagesGenerationsPath    = "/images/generations"
	xaiImagesEditsPath          = "/images/edits"
	xaiDefaultImageEndpointPath = xaiImagesGenerationsPath
	xaiVideosGenerationsPath    = "/videos/generations"
	xaiVideosEditsPath          = "/videos/edits"
	xaiVideosExtensionsPath     = "/videos/extensions"
	xaiVideosPath               = "/videos"
	xaiIdempotencyKeyMetaKey    = "idempotency_key"
	xaiComposerModelPrefix      = "grok-composer-"
	xaiTokenAuthHeader          = "X-XAI-Token-Auth"
	xaiTokenAuthValue           = "xai-grok-cli"
	xaiClientVersionHeader      = "x-grok-client-version"
	// Keep in sync with the current Grok CLI client version that chat-proxy expects.
	xaiClientVersionValue         = "0.2.120"
	xaiClientIdentifierHeader     = "x-grok-client-identifier"
	xaiClientIdentifierValue      = "grok-shell"
	xaiAuthenticateResponseHeader = "x-authenticateresponse"
	xaiAuthenticateResponseValue  = "authenticate-response"
	// xaiUsingAPIAttr enables the official API path for non-media HTTP chat.
	xaiUsingAPIAttr = "using_api"
	// Private executor metadata used to suppress local encrypted reasoning replay
	// when an opaque upstream response already supplies the prior turn, or when
	// opaque continuity validation failed closed.
	xaiSkipReasoningReplayMetadataKey = "xai_skip_reasoning_replay"
	// xaiScrubUntrustedInputMetadataKey requests a second credential-bound
	// input scrub after payload config and before selected-auth replay.
	xaiScrubUntrustedInputMetadataKey = "xai_scrub_untrusted_input"
)

// xaiXSearchToolJSON is the native X Search tool injected when enabled by config.
// Internal subtool traces are still filtered downstream when this tool is present.
var xaiXSearchToolJSON = []byte(`{"type":"x_search"}`)

// XAIExecutor is a stateless executor for xAI Grok's Responses API.
type XAIExecutor struct {
	cfg *config.Config
}

// NewXAIExecutor creates a new xAI executor.
func NewXAIExecutor(cfg *config.Config) *XAIExecutor {
	return &XAIExecutor{cfg: cfg}
}

// Identifier returns the provider identifier.
func (e *XAIExecutor) Identifier() string {
	return "xai"
}

// PrepareRequest injects xAI credentials into the outgoing HTTP request.
func (e *XAIExecutor) PrepareRequest(req *http.Request, auth *cliproxyauth.Auth) error {
	if req == nil {
		return nil
	}
	token, _ := xaiCreds(auth)
	if strings.TrimSpace(token) != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	} else {
		req.Header.Del("Authorization")
	}
	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	util.ApplyCustomHeadersFromAttrs(req, attrs)
	return nil
}

// HttpRequest injects xAI credentials into the request and executes it.
func (e *XAIExecutor) HttpRequest(ctx context.Context, auth *cliproxyauth.Auth, req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, fmt.Errorf("xai executor: request is nil")
	}
	if ctx == nil {
		ctx = req.Context()
	}
	httpReq := req.WithContext(ctx)
	if errPrepare := e.PrepareRequest(httpReq, auth); errPrepare != nil {
		return nil, errPrepare
	}
	httpClient := helps.NewProxyAwareHTTPClient(ctx, e.cfg, auth, 0)
	return httpClient.Do(httpReq)
}
