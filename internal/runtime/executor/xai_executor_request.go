package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/uuid"
	xaiauth "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/xai"
	internalcache "github.com/router-for-me/CLIProxyAPI/v7/internal/cache"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type xaiPreparedRequest struct {
	baseModel             string
	from                  sdktranslator.Format
	responseFormat        sdktranslator.Format
	to                    sdktranslator.Format
	originalPayload       []byte
	body                  []byte
	namespaceTools        map[string]xaiNamespaceToolRef
	clientDeclaredTools   map[xaiClientToolKey]struct{}
	sessionID             string
	replayScope           xaiReasoningReplayScope
	filterInternalXSearch bool
}

type xaiHTTPResponseContinuity struct {
	enabled          bool
	callerScope      string
	authID           string
	upstreamKind     string
	previousID       string
	sessionBindingID string
	opaqueAllowed    bool
	scrubOpaque      bool
}

var (
	getXAIResponseContinuityRequired = internalcache.GetXAIResponseContinuityRequired
	storeXAIResponseContinuity       = internalcache.StoreXAIResponseContinuity
)

type xaiNamespaceToolRef struct {
	namespace string
	name      string
}

// xaiClientToolKey identifies a client-declared callable tool using the
// post-restore Responses shape (short name + optional namespace) and the
// effective upstream tool type after normalizeXAITool (client custom tools are
// sent as function). Response call types are matched against this effective
// kind so internal custom_tool_call traces are not exempted merely because a
// client declared an ordinary function/custom tool with the same short name,
// while legitimate function_call responses for normalized custom tools are kept.
type xaiClientToolKey struct {
	namespace string
	name      string
	toolType  string
}

func (e *XAIExecutor) prepareResponsesRequest(ctx context.Context, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, stream bool) (*xaiPreparedRequest, error) {
	return e.prepareResponsesRequestTo(ctx, req, opts, stream, sdktranslator.FormatCodex)
}

func (e *XAIExecutor) prepareResponsesRequestTo(ctx context.Context, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, stream bool, to sdktranslator.Format) (*xaiPreparedRequest, error) {
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	from := opts.SourceFormat
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	originalPayloadSource := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayloadSource = opts.OriginalRequest
	}
	originalPayload := bytes.Clone(originalPayloadSource)
	originalTranslated := helps.TranslateRequestWithAPIKeyModelCompatibility(ctx, opts.Headers, e.cfg, from, to, baseModel, originalPayload, stream, helps.APIKeyModelIsCompat(req))
	originalTranslated = preserveXAIResponsesOutputControls(originalTranslated, originalPayload, from)
	body := helps.TranslateRequestWithAPIKeyModelCompatibility(ctx, opts.Headers, e.cfg, from, to, baseModel, bytes.Clone(req.Payload), stream, helps.APIKeyModelIsCompat(req))
	body = preserveXAIResponsesOutputControls(body, req.Payload, from)

	var err error
	body, err = helps.ApplyRequestThinking(body, req, opts, from.String(), e.Identifier(), e.Identifier())
	if err != nil {
		return nil, err
	}

	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	requestPath := helps.PayloadRequestPath(opts)
	body = helps.ApplyPayloadConfigWithRequest(e.cfg, baseModel, to.String(), from.String(), "", body, originalTranslated, requestedModel, requestPath, opts.Headers)
	if xaiMetadataBool(opts.Metadata, xaiScrubUntrustedInputMetadataKey) {
		body = xaiStripOpaqueContinuityState(body)
	}
	body = helps.SetStringIfDifferent(body, "model", baseModel)
	body = helps.SetBoolIfDifferent(body, "stream", stream)
	body, _ = sjson.DeleteBytes(body, "previous_response_id")
	body, _ = sjson.DeleteBytes(body, "prompt_cache_retention")
	body, _ = sjson.DeleteBytes(body, "safety_identifier")
	body, _ = sjson.DeleteBytes(body, "stream_options")
	body = helps.RewriteCodexMultiAgentV2Input(ctx, opts.Headers, body, e.cfg)
	namespaceTools := collectXAINamespaceToolRefs(body)
	// Collect before normalizeXAITools flattens namespace wrappers so keys match
	// the post-restore (namespace, short-name) shape used by the response filter.
	clientDeclaredTools := collectXAIClientDeclaredToolKeys(body)
	var xaiCfg config.XAIConfig
	if e.cfg != nil {
		xaiCfg = e.cfg.XAI
	}
	body = normalizeXAIToolsWithConfig(body, xaiCfg)
	body = promoteXAIAdditionalTools(body)
	// Drop choices that point at tools removed by normalizeXAITools before any
	// configured x_search injection, so no surviving choice references a deleted tool.
	body = normalizeXAINamespaceToolChoice(body)
	body = normalizeXAIForcedWebSearchToolChoice(body)
	body = pruneXAIOrphanedToolChoice(body)
	body = normalizeXAIToolChoiceForTools(body)
	if xaiCfg.InjectXSearch {
		body = ensureXAINativeXSearchTool(body)
	}
	var replayScope xaiReasoningReplayScope
	body, replayScope, err = applyXAIReasoningReplayCacheRequired(ctx, from, req, opts, body)
	if err != nil {
		return nil, err
	}
	body = normalizeXAIInputCustomToolCalls(body)
	body = normalizeXAIInputNamespaceToolCalls(body)
	body = normalizeXAIInputReasoningItems(body)
	body = sanitizeXAIInputEncryptedContent(body)
	body = normalizeCodexInstructions(body)
	body = sanitizeXAIResponsesBody(body, baseModel)
	body = normalizeXAIImageRefs(body)

	sessionID, errSession := xaiResolveComposerSessionID(ctx, req, opts, baseModel)
	if errSession != nil {
		return nil, errSession
	}
	if sessionID != "" {
		body = helps.SetStringIfDifferent(body, "prompt_cache_key", sessionID)
	}

	return &xaiPreparedRequest{
		baseModel:             baseModel,
		from:                  from,
		responseFormat:        responseFormat,
		to:                    to,
		originalPayload:       originalPayload,
		body:                  body,
		namespaceTools:        namespaceTools,
		clientDeclaredTools:   clientDeclaredTools,
		sessionID:             sessionID,
		replayScope:           replayScope,
		filterInternalXSearch: xaiRequestHasNativeXSearch(body),
	}, nil
}

func xaiOptionsWithSelectedAuth(opts cliproxyexecutor.Options, auth *cliproxyauth.Auth) cliproxyexecutor.Options {
	selectedAuthID := ""
	if auth != nil {
		selectedAuthID = strings.TrimSpace(auth.ID)
	}
	if len(opts.Metadata) == 0 && selectedAuthID == "" {
		return opts
	}
	metadata := make(map[string]any, len(opts.Metadata)+1)
	for key, value := range opts.Metadata {
		metadata[key] = value
	}
	if selectedAuthID == "" {
		delete(metadata, cliproxyexecutor.SelectedAuthMetadataKey)
	} else {
		metadata[cliproxyexecutor.SelectedAuthMetadataKey] = selectedAuthID
	}
	opts.Metadata = metadata
	return opts
}

func prepareXAIHTTPResponseContinuity(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, baseURL string) (cliproxyexecutor.Request, cliproxyexecutor.Options, xaiHTTPResponseContinuity) {
	decision := xaiHTTPResponseContinuity{}
	if !sourceFormatEqual(opts.SourceFormat, sdktranslator.FormatOpenAIResponse) || opts.Alt == "responses/compact" {
		return req, opts, decision
	}
	if strings.TrimSpace(gjson.GetBytes(req.Payload, "prompt_cache_key").String()) == "" {
		if promptCacheKey := strings.TrimSpace(gjson.GetBytes(opts.OriginalRequest, "prompt_cache_key").String()); promptCacheKey != "" {
			req.Payload, _ = sjson.SetBytes(req.Payload, "prompt_cache_key", promptCacheKey)
		}
	}
	authID := ""
	if auth != nil {
		authID = strings.TrimSpace(auth.ID)
	}
	callerScope := internalcache.XAIResponseContinuityCallerScope(helps.APIKeyFromContext(ctx), xaiTrustedExecutionSessionID(req, opts))
	decision = xaiHTTPResponseContinuity{
		enabled: true, callerScope: callerScope, authID: authID,
		upstreamKind: xaiHTTPUpstreamKind(baseURL), sessionBindingID: xaiHTTPContinuitySessionBindingID(req, opts),
	}
	previousID := xaiRequestString(req.Payload, opts.OriginalRequest, "previous_response_id")
	if previousID == "" {
		if !xaiInputHasCredentialBoundState(req.Payload) && !xaiInputHasCredentialBoundState(opts.OriginalRequest) {
			return req, opts, decision
		}
		if callerScope == "" || authID == "" || decision.sessionBindingID == "" {
			return xaiScrubHTTPInputContinuity(req, opts, decision)
		}
		entry, found, errGet := getXAIResponseContinuityRequired(ctx, callerScope, "xai", thinking.ParseSuffix(req.Model).ModelName, decision.sessionBindingID)
		if errGet != nil {
			log.Warnf("xai response session continuity cache read failed; dropping credential-bound input: %v", errGet)
		}
		if errGet != nil || !found || entry.AuthID != authID || entry.UpstreamKind != decision.upstreamKind {
			return xaiScrubHTTPInputContinuity(req, opts, decision)
		}
		return req, opts, decision
	}
	entry, found, errGet := getXAIResponseContinuityRequired(ctx, callerScope, "xai", thinking.ParseSuffix(req.Model).ModelName, previousID)
	if errGet != nil {
		log.Warnf("xai response continuity cache read failed; dropping opaque state: %v", errGet)
	}
	if errGet != nil || !found || callerScope == "" || authID == "" || entry.AuthID != authID || entry.UpstreamKind != decision.upstreamKind {
		req.Payload = xaiStripOpaqueContinuityState(req.Payload)
		opts = xaiOptionsWithMetadataBool(opts, xaiSkipReasoningReplayMetadataKey, true)
		decision.scrubOpaque = true
		return req, opts, decision
	}
	promptCacheKey := xaiRequestString(req.Payload, opts.OriginalRequest, "prompt_cache_key")
	if promptCacheKey == "" {
		promptCacheKey = strings.TrimSpace(entry.PromptCacheKey)
	}
	if promptCacheKey != "" && strings.TrimSpace(gjson.GetBytes(req.Payload, "prompt_cache_key").String()) == "" {
		req.Payload, _ = sjson.SetBytes(req.Payload, "prompt_cache_key", promptCacheKey)
	}
	if entry.OpaqueReusable {
		decision.previousID = previousID
		decision.opaqueAllowed = true
		opts = xaiOptionsWithMetadataBool(opts, xaiSkipReasoningReplayMetadataKey, true)
	}
	return req, opts, decision
}

func xaiScrubHTTPInputContinuity(req cliproxyexecutor.Request, opts cliproxyexecutor.Options, decision xaiHTTPResponseContinuity) (cliproxyexecutor.Request, cliproxyexecutor.Options, xaiHTTPResponseContinuity) {
	req.Payload = xaiStripOpaqueContinuityState(req.Payload)
	opts.OriginalRequest = xaiStripOpaqueContinuityState(opts.OriginalRequest)
	opts = xaiOptionsWithMetadataBool(opts, xaiScrubUntrustedInputMetadataKey, true)
	return req, opts, decision
}

func applyXAIHTTPResponseContinuityToPrepared(prepared *xaiPreparedRequest, continuity xaiHTTPResponseContinuity) {
	if prepared == nil {
		return
	}
	if continuity.scrubOpaque {
		prepared.body = xaiStripOpaqueContinuityState(prepared.body)
		return
	}
	if !continuity.opaqueAllowed || continuity.previousID == "" {
		return
	}
	prepared.body, _ = sjson.SetBytes(prepared.body, "previous_response_id", continuity.previousID)
	prepared.body, _ = sjson.DeleteBytes(prepared.body, "instructions")
}

func cacheXAIHTTPResponseContinuity(ctx context.Context, continuity xaiHTTPResponseContinuity, prepared *xaiPreparedRequest, completedData []byte) {
	if !continuity.enabled || continuity.callerScope == "" || continuity.authID == "" || continuity.upstreamKind == "" || prepared == nil {
		return
	}
	storeResult := gjson.GetBytes(prepared.body, "store")
	entry := internalcache.XAIResponseContinuity{
		AuthID: continuity.authID, PromptCacheKey: strings.TrimSpace(gjson.GetBytes(prepared.body, "prompt_cache_key").String()),
		UpstreamKind: continuity.upstreamKind, OpaqueReusable: continuity.upstreamKind != "http:cli-chat-proxy" && storeResult.Type == gjson.True,
	}
	if responseID := strings.TrimSpace(gjson.GetBytes(completedData, "response.id").String()); responseID != "" {
		storeXAIResponseContinuity(ctx, continuity.callerScope, "xai", prepared.baseModel, responseID, entry)
	}
	bindingID := continuity.sessionBindingID
	if bindingID == "" && entry.PromptCacheKey != "" {
		bindingID = internalcache.XAIResponseContinuitySessionBindingID(entry.PromptCacheKey, "")
	}
	if bindingID != "" {
		storeXAIResponseContinuity(ctx, continuity.callerScope, "xai", prepared.baseModel, bindingID, entry)
	}
}

func xaiTrustedExecutionSessionID(req cliproxyexecutor.Request, opts cliproxyexecutor.Options) string {
	if value := xaiMetadataString(opts.Metadata, cliproxyexecutor.ExecutionSessionMetadataKey); value != "" {
		return value
	}
	return xaiMetadataString(req.Metadata, cliproxyexecutor.ExecutionSessionMetadataKey)
}

func xaiRequestString(primary, fallback []byte, path string) string {
	if value := strings.TrimSpace(gjson.GetBytes(primary, path).String()); value != "" {
		return value
	}
	return strings.TrimSpace(gjson.GetBytes(fallback, path).String())
}

func xaiHTTPContinuitySessionBindingID(req cliproxyexecutor.Request, opts cliproxyexecutor.Options) string {
	return internalcache.XAIResponseContinuitySessionBindingID(xaiRequestString(req.Payload, opts.OriginalRequest, "prompt_cache_key"), xaiTrustedExecutionSessionID(req, opts))
}

func xaiInputHasCredentialBoundState(payload []byte) bool {
	input := gjson.GetBytes(payload, "input")
	if !input.IsArray() {
		return false
	}
	for _, item := range input.Array() {
		if item.Get("type").String() == "compaction" || item.Get("encrypted_content").Exists() {
			return true
		}
	}
	return false
}

func xaiStripOpaqueContinuityState(payload []byte) []byte {
	updated, _ := sjson.DeleteBytes(payload, "previous_response_id")
	input := gjson.GetBytes(updated, "input")
	if !input.IsArray() {
		return updated
	}
	items := make([]json.RawMessage, 0, len(input.Array()))
	for _, item := range input.Array() {
		if item.Get("type").String() == "compaction" {
			continue
		}
		itemRaw, errDelete := sjson.DeleteBytes([]byte(item.Raw), "encrypted_content")
		if errDelete != nil {
			itemRaw = []byte(item.Raw)
		}
		items = append(items, json.RawMessage(itemRaw))
	}
	rawInput, errMarshal := json.Marshal(items)
	if errMarshal != nil {
		return updated
	}
	out, errSet := sjson.SetRawBytes(updated, "input", rawInput)
	if errSet != nil {
		return updated
	}
	return out
}

func xaiOptionsWithMetadataBool(opts cliproxyexecutor.Options, key string, value bool) cliproxyexecutor.Options {
	metadata := make(map[string]any, len(opts.Metadata)+1)
	for metadataKey, metadataValue := range opts.Metadata {
		metadata[metadataKey] = metadataValue
	}
	metadata[key] = value
	opts.Metadata = metadata
	return opts
}

func xaiMetadataBool(metadata map[string]any, key string) bool {
	value, ok := metadata[key]
	if !ok {
		return false
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		parsed, errParse := strconv.ParseBool(strings.TrimSpace(typed))
		return errParse == nil && parsed
	default:
		return false
	}
}

func (e *XAIExecutor) recordXAIRequest(ctx context.Context, auth *cliproxyauth.Auth, url string, headers http.Header, body []byte) {
	var authID, authLabel, authType, authValue string
	if auth != nil {
		authID = auth.ID
		authLabel = auth.Label
		authType, authValue = auth.AccountInfo()
	}
	helps.RecordAPIRequest(ctx, e.cfg, helps.UpstreamRequestLog{
		URL:       url,
		Method:    http.MethodPost,
		Headers:   headers,
		Body:      body,
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	})
}

func xaiCreds(auth *cliproxyauth.Auth) (token, baseURL string) {
	if auth == nil {
		return "", ""
	}
	if auth.Attributes != nil {
		token = strings.TrimSpace(auth.Attributes["api_key"])
		baseURL = strings.TrimSpace(auth.Attributes["base_url"])
	}
	if auth.Metadata != nil {
		if token == "" {
			token = xaiMetadataString(auth.Metadata, "access_token")
		}
		if baseURL == "" {
			baseURL = xaiMetadataString(auth.Metadata, "base_url")
		}
	}
	return token, baseURL
}

// xaiUsingAPI reports whether this xAI auth should use the official API path
// for non-media HTTP chat. OAuth defaults to false to use Grok Build.
func xaiUsingAPI(auth *cliproxyauth.Auth) bool {
	if auth == nil {
		return true
	}
	if len(auth.Attributes) > 0 {
		if raw := strings.TrimSpace(auth.Attributes[xaiUsingAPIAttr]); raw != "" {
			parsed, errParse := strconv.ParseBool(raw)
			if errParse == nil {
				return parsed
			}
		}
	}
	if len(auth.Metadata) > 0 {
		raw, ok := auth.Metadata[xaiUsingAPIAttr]
		if ok && raw != nil {
			switch v := raw.(type) {
			case bool:
				return v
			case string:
				parsed, errParse := strconv.ParseBool(strings.TrimSpace(v))
				if errParse == nil {
					return parsed
				}
			default:
			}
		}
	}
	if raw := strings.TrimSpace(auth.Attributes["auth_kind"]); raw != "" {
		return !strings.EqualFold(raw, "oauth")
	}
	return !strings.EqualFold(xaiMetadataString(auth.Metadata, "auth_kind"), "oauth")
}

// xaiChatBaseURL returns the base URL for non-image/video xAI HTTP chat requests.
// When auth using_api is true, the official API base URL logic is used. When it
// is false (including its OAuth default), empty or official default base_url is
// rewritten to the CLI chat-proxy endpoint; an explicit non-default base_url is
// still honored.
// Websocket and compact transports intentionally do not use this helper:
// cli-chat-proxy only accepts HTTP POST chat and does not implement
// /responses/compact (404) or websocket upgrades (405).
func xaiChatBaseURL(auth *cliproxyauth.Auth) string {
	_, baseURL := xaiCreds(auth)
	if xaiUsingAPI(auth) {
		if baseURL == "" {
			return xaiauth.DefaultAPIBaseURL
		}
		return baseURL
	}
	if baseURL != "" && !xaiIsDefaultAPIBaseURL(baseURL) {
		return baseURL
	}
	return xaiauth.CLIChatProxyBaseURL
}

// xaiCompactBaseURL returns the base URL for xAI /responses/compact requests.
// Compact must stay on the official API (or an explicit non-CLI-proxy base_url).
// Reusing xaiChatBaseURL would pin OAuth traffic to cli-chat-proxy, which returns
// 404 for /responses/compact and then cools down the auth pool as not_found.
func xaiCompactBaseURL(auth *cliproxyauth.Auth) string {
	_, baseURL := xaiCreds(auth)
	if baseURL == "" || xaiIsCLIChatProxyBaseURL(baseURL) {
		return xaiauth.DefaultAPIBaseURL
	}
	return baseURL
}

func xaiNormalizeBaseURL(baseURL string) string {
	return strings.TrimRight(strings.TrimSpace(baseURL), "/")
}

func xaiIsDefaultAPIBaseURL(baseURL string) bool {
	return xaiNormalizeBaseURL(baseURL) == xaiNormalizeBaseURL(xaiauth.DefaultAPIBaseURL)
}

func xaiIsCLIChatProxyBaseURL(baseURL string) bool {
	return xaiNormalizeBaseURL(baseURL) == xaiNormalizeBaseURL(xaiauth.CLIChatProxyBaseURL)
}

func xaiHTTPUpstreamKind(baseURL string) string {
	normalized := xaiContinuityNormalizedBaseURL(baseURL)
	switch normalized {
	case xaiContinuityNormalizedBaseURL(xaiauth.DefaultAPIBaseURL):
		return "http:official"
	case xaiContinuityNormalizedBaseURL(xaiauth.CLIChatProxyBaseURL):
		return "http:cli-chat-proxy"
	default:
		sum := sha256.Sum256([]byte(normalized))
		return fmt.Sprintf("http:custom:%x", sum[:])
	}
}

func xaiContinuityNormalizedBaseURL(baseURL string) string {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	parsed, errParse := url.Parse(baseURL)
	if errParse != nil || parsed.Scheme == "" || parsed.Host == "" {
		return strings.ToLower(baseURL)
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	parsed.Host = strings.ToLower(parsed.Host)
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawPath = strings.TrimRight(parsed.RawPath, "/")
	parsed.Fragment = ""
	return parsed.String()
}

// xaiBaseURLSource classifies a resolved xAI base URL for logging.
func xaiBaseURLSource(baseURL string) string {
	switch {
	case xaiIsDefaultAPIBaseURL(baseURL):
		return "DefaultAPIBaseURL"
	case xaiIsCLIChatProxyBaseURL(baseURL):
		return "CLIChatProxyBaseURL"
	default:
		return "custom"
	}
}

// logXAIResolvedBaseURL emits a console log for the resolved upstream base URL.
func logXAIResolvedBaseURL(ctx context.Context, baseURL string) {
	helps.LogWithRequestID(ctx).Infof("xai: using base_url=%s source=%s", baseURL, xaiBaseURLSource(baseURL))
}

func applyXAIHeaders(r *http.Request, auth *cliproxyauth.Auth, token string, stream bool, sessionID string, clientHeaders ...http.Header) {
	applyXAIDefaultHeaders(r, token, stream, sessionID)
	applyXAICustomHeaders(r, auth, clientHeaders...)
}

func applyXAIDefaultHeaders(r *http.Request, token string, stream bool, sessionID string) {
	r.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(token) != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if stream {
		r.Header.Set("Accept", "text/event-stream")
	} else {
		r.Header.Set("Accept", "application/json")
	}
	r.Header.Set("Connection", "Keep-Alive")
	if sessionID != "" {
		r.Header.Set("x-grok-conv-id", sessionID)
	}
}

func applyXAICustomHeaders(r *http.Request, auth *cliproxyauth.Auth, clientHeaders ...http.Header) {
	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	util.ApplyCustomHeadersFromAttrs(r, attrs, clientHeaders...)
}

// applyXAIChatHeaders applies standard xAI headers for non-image/video chat
// requests. When using_api is true, this matches the standard
// applyXAIHeaders behavior. CLI chat-proxy identity headers are only attached
// when using_api is false and the resolved chat base URL is the official CLI
// chat-proxy endpoint.
func applyXAIChatHeaders(r *http.Request, auth *cliproxyauth.Auth, token string, stream bool, sessionID string, clientHeaders ...http.Header) {
	if xaiUsingAPI(auth) {
		applyXAIHeaders(r, auth, token, stream, sessionID, clientHeaders...)
		return
	}
	applyXAIDefaultHeaders(r, token, stream, sessionID)
	if xaiIsCLIChatProxyBaseURL(xaiChatBaseURL(auth)) {
		r.Header.Set(xaiTokenAuthHeader, xaiTokenAuthValue)
		r.Header.Set(xaiClientVersionHeader, xaiClientVersionValue)
		r.Header.Set("User-Agent", "xai-grok-workspace/"+xaiClientVersionValue)
		r.Header.Set(xaiClientIdentifierHeader, xaiClientIdentifierValue)
		r.Header.Set(xaiAuthenticateResponseHeader, xaiAuthenticateResponseValue)
	}
	applyXAICustomHeaders(r, auth, clientHeaders...)
}

func xaiResolveComposerSessionID(ctx context.Context, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, baseModel string) (string, error) {
	if sessionID := xaiExecutionSessionID(req, opts); sessionID != "" {
		return sessionID, nil
	}
	if !xaiRequiresIsolatedConversation(baseModel) {
		return "", nil
	}
	cached, ok, errCache := helps.ClaudeCodePromptCache(ctx, baseModel, req.Payload, opts.Headers)
	if errCache != nil {
		return "", errCache
	}
	if ok {
		return cached.ID, nil
	}
	return uuid.NewString(), nil
}

func xaiExecutionSessionID(req cliproxyexecutor.Request, opts cliproxyexecutor.Options) string {
	if value := xaiMetadataString(opts.Metadata, cliproxyexecutor.ExecutionSessionMetadataKey); value != "" {
		return value
	}
	if value := xaiMetadataString(req.Metadata, cliproxyexecutor.ExecutionSessionMetadataKey); value != "" {
		return value
	}
	if promptCacheKey := gjson.GetBytes(req.Payload, "prompt_cache_key"); promptCacheKey.Exists() {
		if value := strings.TrimSpace(promptCacheKey.String()); value != "" {
			return value
		}
	}
	return helps.DerivedSessionUUID("xai", opts.Metadata, req.Metadata)
}

func xaiRequiresIsolatedConversation(model string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(model)), xaiComposerModelPrefix)
}

func xaiImageEndpointPath(opts cliproxyexecutor.Options) string {
	if opts.SourceFormat.String() != xaiImageHandlerType {
		return ""
	}

	path := xaiMetadataString(opts.Metadata, cliproxyexecutor.RequestPathMetadataKey)
	if strings.HasSuffix(path, "/images/edits") {
		return xaiImagesEditsPath
	}
	if strings.HasSuffix(path, "/images/generations") {
		return xaiImagesGenerationsPath
	}
	return xaiDefaultImageEndpointPath
}

// normalizeXAIImageRefs rewrites OpenAI-style image object fields to the xAI
// image API shape before the payload is sent upstream:
//
//	{"image":{"image_url":"https://..."}} → {"image":{"url":"https://..."}}
//
// Applies to image / images / reference_images anywhere in the JSON tree,
// including nested objects and array items. Does not rewrite chat content
// parts shaped as {"type":"image_url","image_url":{...}}.
func normalizeXAIImageRefs(body []byte) []byte {
	if !gjson.ValidBytes(body) {
		return body
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var payload any
	if errDecode := decoder.Decode(&payload); errDecode != nil {
		return body
	}

	if !normalizeXAIImageRefsValue(payload) {
		return body
	}
	normalized, errMarshal := json.Marshal(payload)
	if errMarshal != nil {
		return body
	}
	return normalized
}

func normalizeXAIImageRefsValue(value any) bool {
	changed := false
	switch node := value.(type) {
	case map[string]any:
		for key, child := range node {
			switch key {
			case "image":
				changed = normalizeXAIImageRef(child) || changed
			case "images", "reference_images":
				if refs, ok := child.([]any); ok {
					for _, ref := range refs {
						changed = normalizeXAIImageRef(ref) || changed
					}
				}
			}
			changed = normalizeXAIImageRefsValue(child) || changed
		}
	case []any:
		for _, child := range node {
			changed = normalizeXAIImageRefsValue(child) || changed
		}
	}
	return changed
}

func normalizeXAIImageRef(value any) bool {
	ref, ok := value.(map[string]any)
	if !ok {
		return false
	}

	originalURL, _ := ref["url"].(string)
	url := strings.TrimSpace(originalURL)
	imageURL, hasImageURL := ref["image_url"]
	if url == "" {
		switch imageURL := imageURL.(type) {
		case string:
			url = strings.TrimSpace(imageURL)
		case map[string]any:
			url, _ = imageURL["url"].(string)
			url = strings.TrimSpace(url)
		}
	}
	if url == "" {
		return false
	}
	if url == originalURL && !hasImageURL {
		return false
	}

	// Always emit the xAI field name and drop the OpenAI alias.
	ref["url"] = url
	delete(ref, "image_url")
	return true
}

func xaiIsVideoRequest(opts cliproxyexecutor.Options) bool {
	return opts.SourceFormat.String() == xaiVideoHandlerType
}

func xaiVideoEndpointPath(opts cliproxyexecutor.Options) string {
	if !xaiIsVideoRequest(opts) {
		return ""
	}
	path := xaiMetadataString(opts.Metadata, cliproxyexecutor.RequestPathMetadataKey)
	if strings.HasSuffix(path, "/videos/edits") {
		return xaiVideosEditsPath
	}
	if strings.HasSuffix(path, "/videos/extensions") {
		return xaiVideosExtensionsPath
	}
	if strings.HasSuffix(path, "/videos/generations") {
		return xaiVideosGenerationsPath
	}
	return ""
}

func xaiMetadataString(meta map[string]any, key string) string {
	if len(meta) == 0 || key == "" {
		return ""
	}
	value, ok := meta[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

func preserveXAIResponsesOutputControls(body, source []byte, from sdktranslator.Format) []byte {
	var maxOutputTokens gjson.Result
	switch from {
	case sdktranslator.FormatOpenAI:
		maxOutputTokens = gjson.GetBytes(source, "max_completion_tokens")
		if !maxOutputTokens.Exists() || maxOutputTokens.Type == gjson.Null {
			maxOutputTokens = gjson.GetBytes(source, "max_tokens")
		}
	case sdktranslator.FormatOpenAIResponse:
		maxOutputTokens = gjson.GetBytes(source, "max_output_tokens")
	default:
		return body
	}

	if maxOutputTokens.Exists() && maxOutputTokens.Type != gjson.Null {
		body, _ = sjson.SetRawBytes(body, "max_output_tokens", []byte(maxOutputTokens.Raw))
	}
	for _, field := range []string{"temperature", "top_p", "top_k"} {
		value := gjson.GetBytes(source, field)
		if value.Exists() && value.Type != gjson.Null {
			body, _ = sjson.SetRawBytes(body, field, []byte(value.Raw))
		}
	}
	return body
}

func sanitizeXAIResponsesBody(body []byte, model string) []byte {
	// stop is supported by Chat Completions but not by xAI's Responses API.
	body, _ = sjson.DeleteBytes(body, "stop")
	if !xaiSupportsReasoningEffort(model) {
		if gjson.GetBytes(body, "reasoning.effort").Exists() {
			log.Debugf("xai: stripping reasoning.effort for model %s (no thinking levels in model registry)", model)
		}
		body, _ = sjson.DeleteBytes(body, "reasoning.effort")
		if reasoning := gjson.GetBytes(body, "reasoning"); reasoning.Exists() && reasoning.IsObject() && len(reasoning.Map()) == 0 {
			body, _ = sjson.DeleteBytes(body, "reasoning")
		}
	}
	return body
}

type xaiNormalizedTool struct {
	raw       []byte
	name      string
	namespace string
	group     int
}

type xaiNormalizedToolGroup struct {
	path    string
	tools   []xaiNormalizedTool
	changed bool
}

type xaiToolSelectionKey struct {
	toolType      string
	qualifiedName string
}

func xaiToolUpstreamCost(tool xaiNormalizedTool) int {
	if gjson.GetBytes(tool.raw, "type").String() == xaiWebSearchToolType {
		return xaiWebSearchUpstreamCost
	}
	return 1
}

func xaiToolsUpstreamCost(tools []xaiNormalizedTool) int {
	total := 0
	for _, tool := range tools {
		total += xaiToolUpstreamCost(tool)
	}
	return total
}

// ensureXAINativeXSearchTool appends {"type":"x_search"} when the final tools
// list does not already include native X Search. When tool_choice restricts the
// model to allowed_tools, x_search is also added there (without duplicates) so
// Grok can select the injected tool. When injection is enabled, HTTP and websocket
// executors both prepare payloads through prepareResponsesRequestTo, so this runs
// once before the body is submitted upstream.
func ensureXAINativeXSearchTool(body []byte) []byte {
	if !gjson.ValidBytes(body) {
		return body
	}
	if !xaiRequestHasNativeXSearch(body) {
		tools := gjson.GetBytes(body, "tools")
		if !tools.Exists() || !tools.IsArray() {
			body, _ = sjson.SetRawBytes(body, "tools", []byte(`[{"type":"x_search"}]`))
		} else {
			body, _ = sjson.SetRawBytes(body, "tools.-1", xaiXSearchToolJSON)
		}
	}
	return ensureXAINativeXSearchAllowedTools(body)
}

// ensureXAINativeXSearchAllowedTools appends x_search to tool_choice.tools when
// the choice mode is allowed_tools and x_search is not already listed.
func ensureXAINativeXSearchAllowedTools(body []byte) []byte {
	choice := gjson.GetBytes(body, "tool_choice")
	if !choice.IsObject() || choice.Get("type").String() != "allowed_tools" {
		return body
	}
	allowed := choice.Get("tools")
	if !allowed.Exists() || !allowed.IsArray() {
		body, _ = sjson.SetRawBytes(body, "tool_choice.tools", []byte(`[{"type":"x_search"}]`))
		return body
	}
	for _, tool := range allowed.Array() {
		if strings.TrimSpace(tool.Get("type").String()) == xaiXSearchToolType {
			return body
		}
	}
	body, _ = sjson.SetRawBytes(body, "tool_choice.tools.-1", xaiXSearchToolJSON)
	return body
}

// normalizeXAIForcedWebSearchToolChoice rewrites Codex's hosted-tool choice
// into the allowed_tools form accepted by xAI's ModelToolChoice schema.
func normalizeXAIForcedWebSearchToolChoice(body []byte) []byte {
	choice := gjson.GetBytes(body, "tool_choice")
	if !choice.IsObject() || strings.TrimSpace(choice.Get("type").String()) != xaiWebSearchToolType {
		return body
	}

	allowedChoice := []byte(`{"type":"allowed_tools","mode":"required","tools":[]}`)
	allowedChoice, errSetAllowed := sjson.SetRawBytes(allowedChoice, "tools.-1", []byte(choice.Raw))
	if errSetAllowed != nil {
		return body
	}
	updated, errSetChoice := sjson.SetRawBytes(body, "tool_choice", allowedChoice)
	if errSetChoice != nil {
		return body
	}
	return updated
}

// pruneXAIOrphanedToolChoice removes tool_choice entries that no longer match
// any remaining tool after normalizeXAITools filtering. Forced choices that
// reference a deleted tool are dropped entirely; allowed_tools lists keep only
// choices that still resolve against the post-normalization tools set.
func pruneXAIOrphanedToolChoice(body []byte) []byte {
	if !gjson.ValidBytes(body) {
		return body
	}
	choice := gjson.GetBytes(body, "tool_choice")
	if !choice.Exists() {
		return body
	}
	available := collectXAIAvailableToolChoiceKeys(body)
	if choice.Type == gjson.String {
		// auto / none / required are not tool references.
		return body
	}
	if !choice.IsObject() {
		return body
	}
	choiceType := strings.TrimSpace(choice.Get("type").String())
	switch choiceType {
	case "allowed_tools":
		return pruneXAIAllowedToolsChoice(body, available)
	default:
		if choiceType == "" {
			return body
		}
		if xaiToolChoiceMatchesAvailable(choice, available) {
			return body
		}
		body, _ = sjson.DeleteBytes(body, "tool_choice")
		return body
	}
}

func pruneXAIAllowedToolsChoice(body []byte, available map[xaiToolChoiceKey]struct{}) []byte {
	allowed := gjson.GetBytes(body, "tool_choice.tools")
	if !allowed.Exists() || !allowed.IsArray() {
		body, _ = sjson.DeleteBytes(body, "tool_choice")
		return body
	}
	allowedItems := allowed.Array()
	filtered := make([][]byte, 0, len(allowedItems))
	changed := false
	for _, tool := range allowedItems {
		if !xaiToolChoiceMatchesAvailable(tool, available) {
			changed = true
			continue
		}
		filtered = append(filtered, []byte(tool.Raw))
	}
	if !changed {
		return body
	}
	if len(filtered) == 0 {
		body, _ = sjson.DeleteBytes(body, "tool_choice")
		return body
	}
	body, _ = sjson.SetRawBytes(body, "tool_choice.tools", helps.JoinRawJSONArray(filtered))
	return body
}

// xaiToolChoiceKey identifies a selectable tool the way xAI tool_choice entries
// reference it after namespace qualification: type alone for host tools, or
// type+name for function tools.
type xaiToolChoiceKey struct {
	toolType string
	name     string
}

func collectXAIAvailableToolChoiceKeys(body []byte) map[xaiToolChoiceKey]struct{} {
	keys := make(map[xaiToolChoiceKey]struct{})
	collect := func(tools gjson.Result) {
		if !tools.IsArray() {
			return
		}
		for _, tool := range tools.Array() {
			toolType := strings.TrimSpace(tool.Get("type").String())
			if toolType == "" {
				continue
			}
			key := xaiToolChoiceKey{toolType: toolType}
			if toolType == xaiFunctionToolType || toolType == xaiCustomToolType {
				key.name = strings.TrimSpace(tool.Get("name").String())
				if key.name == "" {
					continue
				}
			}
			keys[key] = struct{}{}
		}
	}
	collect(gjson.GetBytes(body, "tools"))
	input := gjson.GetBytes(body, "input")
	if input.IsArray() {
		for _, item := range input.Array() {
			if item.Get("type").String() == "additional_tools" {
				collect(item.Get("tools"))
			}
		}
	}
	return keys
}

func xaiToolChoiceMatchesAvailable(choice gjson.Result, available map[xaiToolChoiceKey]struct{}) bool {
	toolType := strings.TrimSpace(choice.Get("type").String())
	if toolType == "" {
		return false
	}
	key := xaiToolChoiceKey{toolType: toolType}
	if toolType == xaiFunctionToolType || toolType == xaiCustomToolType {
		key.name = strings.TrimSpace(choice.Get("name").String())
		if key.name == "" {
			return false
		}
	}
	_, ok := available[key]
	return ok
}

func normalizeXAITools(body []byte) []byte {
	return normalizeXAIToolsWithConfig(body, config.XAIConfig{})
}

func normalizeXAIToolsWithConfig(body []byte, cfg config.XAIConfig) []byte {
	if !gjson.ValidBytes(body) {
		return body
	}
	original := body
	groups := make([]xaiNormalizedToolGroup, 0, 2)
	collectGroup := func(path string) bool {
		tools := gjson.GetBytes(body, path)
		if !tools.Exists() || !tools.IsArray() {
			return true
		}
		normalized, changed, ok := normalizeXAIToolArray(tools)
		if !ok {
			return false
		}
		groupIndex := len(groups)
		for index := range normalized {
			normalized[index].group = groupIndex
		}
		groups = append(groups, xaiNormalizedToolGroup{path: path, tools: normalized, changed: changed})
		return true
	}

	if !collectGroup("tools") {
		return original
	}
	input := gjson.GetBytes(body, "input")
	if input.Exists() && input.IsArray() {
		for index, item := range input.Array() {
			if item.Get("type").String() != "additional_tools" {
				continue
			}
			if !collectGroup(fmt.Sprintf("input.%d.tools", index)) {
				return original
			}
		}
	}

	allTools := make([]xaiNormalizedTool, 0)
	for _, group := range groups {
		allTools = append(allTools, group.tools...)
	}
	retained, capped := capXAINormalizedTools(body, allTools, cfg)
	if capped {
		retainedByGroup := make([][]xaiNormalizedTool, len(groups))
		for _, tool := range retained {
			retainedByGroup[tool.group] = append(retainedByGroup[tool.group], tool)
		}
		for index := range groups {
			groups[index].tools = retainedByGroup[index]
			groups[index].changed = true
		}
	}

	for _, group := range groups {
		if !group.changed {
			continue
		}
		filtered := make([][]byte, 0, len(group.tools))
		for _, tool := range group.tools {
			filtered = append(filtered, tool.raw)
		}
		updated, errSet := sjson.SetRawBytes(body, group.path, helps.JoinRawJSONArray(filtered))
		if errSet != nil {
			return original
		}
		body = updated
	}
	return body
}

// promoteXAIAdditionalTools moves Responses Lite tool declarations to the
// top-level tools array because xAI does not accept additional_tools input items.
func promoteXAIAdditionalTools(body []byte) []byte {
	if !gjson.ValidBytes(body) {
		return body
	}
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() {
		return body
	}

	inputItems := input.Array()
	remainingInput := make([]json.RawMessage, 0, len(inputItems))
	promotedTools := make([]json.RawMessage, 0)
	for _, item := range inputItems {
		if item.Get("type").String() != "additional_tools" {
			remainingInput = append(remainingInput, json.RawMessage(item.Raw))
			continue
		}
		for _, tool := range item.Get("tools").Array() {
			promotedTools = append(promotedTools, json.RawMessage(tool.Raw))
		}
	}
	if len(remainingInput) == len(inputItems) {
		return body
	}

	rawInput, errMarshalInput := json.Marshal(remainingInput)
	if errMarshalInput != nil {
		return body
	}
	updated, errSetInput := sjson.SetRawBytes(body, "input", rawInput)
	if errSetInput != nil {
		return body
	}
	if len(promotedTools) == 0 {
		return updated
	}

	topLevelTools := gjson.GetBytes(updated, "tools")
	tools := make([]json.RawMessage, 0, len(topLevelTools.Array())+len(promotedTools))
	if topLevelTools.IsArray() {
		for _, tool := range topLevelTools.Array() {
			tools = append(tools, json.RawMessage(tool.Raw))
		}
	}
	tools = append(tools, promotedTools...)
	rawTools, errMarshalTools := json.Marshal(tools)
	if errMarshalTools != nil {
		return body
	}
	updated, errSetTools := sjson.SetRawBytes(updated, "tools", rawTools)
	if errSetTools != nil {
		return body
	}
	return updated
}

func normalizeXAIToolArray(tools gjson.Result) ([]xaiNormalizedTool, bool, bool) {
	toolItems := tools.Array()
	changed := false
	normalized := make([]xaiNormalizedTool, 0, len(toolItems))
	for _, tool := range toolItems {
		toolType := tool.Get("type").String()
		if toolType == xaiNamespaceToolType {
			changed = true
			namespaceName := tool.Get("name").String()
			if namespaceTools := tool.Get("tools"); namespaceTools.IsArray() {
				for _, nestedTool := range namespaceTools.Array() {
					nestedRaw, nestedChanged, ok := normalizeXAITool(nestedTool, namespaceName)
					if !ok {
						return nil, false, false
					}
					changed = changed || nestedChanged
					if len(nestedRaw) == 0 {
						continue
					}
					normalized = append(normalized, xaiNormalizedTool{raw: nestedRaw, name: nestedTool.Get("name").String(), namespace: namespaceName})
				}
			}
			continue
		}
		raw, toolChanged, ok := normalizeXAITool(tool, "")
		if !ok {
			return nil, false, false
		}
		changed = changed || toolChanged
		if len(raw) == 0 {
			continue
		}
		normalized = append(normalized, xaiNormalizedTool{raw: raw, name: tool.Get("name").String()})
	}
	return normalized, changed, true
}

func capXAINormalizedTools(body []byte, tools []xaiNormalizedTool, cfg config.XAIConfig) ([]xaiNormalizedTool, bool) {
	model := strings.ToLower(gjson.GetBytes(body, "model").String())
	if !strings.HasPrefix(model, "grok-") {
		return tools, false
	}
	maxTools := xaiMaxToolsPerRequest
	if cfg.MaxTools > 0 && cfg.MaxTools < maxTools {
		maxTools = cfg.MaxTools
	}
	reservedCost := 0
	if cfg.InjectXSearch && !xaiNormalizedToolsContainType(tools, xaiXSearchToolType) {
		reservedCost = 1
	}
	upstreamCost := xaiToolsUpstreamCost(tools)
	projectedCost := upstreamCost + reservedCost
	if projectedCost <= maxTools {
		return tools, false
	}
	forcedChoice := xaiForcedToolSelectionKey(body)
	ordered := prioritizeXAITools(tools, cfg.PreferredToolNamespaces, forcedChoice)
	retained := make([]xaiNormalizedTool, 0, len(ordered))
	omittedNames := make([]string, 0, len(ordered))
	retainedCost := reservedCost
	for _, tool := range ordered {
		toolCost := xaiToolUpstreamCost(tool)
		if retainedCost+toolCost <= maxTools {
			retained = append(retained, tool)
			retainedCost += toolCost
			continue
		}
		name := tool.name
		if name == "" {
			name = gjson.GetBytes(tool.raw, "type").String()
		}
		if tool.namespace != "" {
			name = tool.namespace + "." + name
		}
		omittedNames = append(omittedNames, name)
	}
	log.Warnf("xai: capped tools for model %s from %d entries (%d effective upstream tools) to %d entries (%d effective upstream tools)", model, len(tools), projectedCost, len(retained), retainedCost)
	log.Debugf("xai: omitted tools for model %s: %s", model, strings.Join(omittedNames, ","))
	return retained, true
}

func xaiNormalizedToolsContainType(tools []xaiNormalizedTool, toolType string) bool {
	for _, tool := range tools {
		if gjson.GetBytes(tool.raw, "type").String() == toolType {
			return true
		}
	}
	return false
}

func xaiForcedToolSelectionKey(body []byte) xaiToolSelectionKey {
	choice := gjson.GetBytes(body, "tool_choice")
	if !choice.IsObject() {
		return xaiToolSelectionKey{}
	}
	toolType := strings.TrimSpace(choice.Get("type").String())
	name := strings.TrimSpace(choice.Get("name").String())
	if name == "" {
		name = strings.TrimSpace(choice.Get("function.name").String())
	}
	namespace := strings.TrimSpace(choice.Get("namespace").String())
	if namespace == "" {
		namespace = strings.TrimSpace(choice.Get("function.namespace").String())
	}
	if toolType == "" && name != "" {
		toolType = xaiFunctionToolType
	}
	toolType = xaiEffectiveDeclaredToolType(toolType)
	if toolType == "" || toolType == "allowed_tools" {
		return xaiToolSelectionKey{}
	}
	key := xaiToolSelectionKey{toolType: toolType}
	if toolType == xaiFunctionToolType {
		key.qualifiedName = qualifyXAINamespaceToolName(namespace, name)
		if key.qualifiedName == "" {
			return xaiToolSelectionKey{}
		}
	}
	return key
}

func xaiNormalizedToolSelectionKey(tool xaiNormalizedTool) xaiToolSelectionKey {
	toolType := xaiEffectiveDeclaredToolType(gjson.GetBytes(tool.raw, "type").String())
	key := xaiToolSelectionKey{toolType: toolType}
	if toolType == xaiFunctionToolType {
		key.qualifiedName = strings.TrimSpace(gjson.GetBytes(tool.raw, "name").String())
	}
	return key
}

func prioritizeXAITools(tools []xaiNormalizedTool, preferredNamespaces []string, forcedChoice xaiToolSelectionKey) []xaiNormalizedTool {
	ordered := make([]xaiNormalizedTool, 0, len(tools))
	selected := make([]bool, len(tools))
	appendSelection := func(key xaiToolSelectionKey) {
		if key.toolType == "" {
			return
		}
		for i, tool := range tools {
			if !selected[i] && xaiNormalizedToolSelectionKey(tool) == key {
				ordered = append(ordered, tool)
				selected[i] = true
				break
			}
		}
	}
	appendMatching := func(namespace string) {
		for i, tool := range tools {
			if !selected[i] && tool.namespace == namespace {
				ordered = append(ordered, tool)
				selected[i] = true
			}
		}
	}
	appendSelection(xaiToolSelectionKey{toolType: xaiXSearchToolType})
	appendSelection(forcedChoice)
	appendMatching("")
	for _, namespace := range preferredNamespaces {
		namespace = strings.TrimSpace(namespace)
		if namespace != "" {
			appendMatching(namespace)
		}
	}
	for i, tool := range tools {
		if !selected[i] {
			ordered = append(ordered, tool)
		}
	}
	return ordered
}

// normalizeXAIToolChoiceForTools drops tool_choice and parallel_tool_calls
// when tools are absent or empty (including after normalizeXAITools filtering).
// xAI rejects payloads that include tool_choice without any tools defined.
// Existence checks avoid unnecessary sjson parse/copy passes.
func normalizeXAIToolChoiceForTools(body []byte) []byte {
	tools := gjson.GetBytes(body, "tools")
	hasTools := tools.Exists() && tools.IsArray() && len(tools.Array()) > 0
	if !hasTools {
		input := gjson.GetBytes(body, "input")
		if input.Exists() && input.IsArray() {
			for _, item := range input.Array() {
				additionalTools := item.Get("tools")
				if item.Get("type").String() == "additional_tools" && additionalTools.IsArray() && len(additionalTools.Array()) > 0 {
					hasTools = true
					break
				}
			}
		}
	}
	if hasTools {
		return body
	}
	if tools.Exists() {
		body, _ = sjson.DeleteBytes(body, "tools")
	}
	if gjson.GetBytes(body, "tool_choice").Exists() {
		body, _ = sjson.DeleteBytes(body, "tool_choice")
	}
	if gjson.GetBytes(body, "parallel_tool_calls").Exists() {
		body, _ = sjson.DeleteBytes(body, "parallel_tool_calls")
	}
	return body
}

// normalizeXAINamespaceToolChoice qualifies namespaced function choices using
// the same names sent in the flattened tools list. xAI does not accept the
// Responses namespace field on tool choices.
func normalizeXAINamespaceToolChoice(body []byte) []byte {
	if !gjson.ValidBytes(body) {
		return body
	}
	original := body
	normalizeAtPath := func(path string) bool {
		toolChoice := gjson.GetBytes(body, path)
		if !toolChoice.IsObject() || toolChoice.Get("type").String() != xaiFunctionToolType {
			return true
		}
		namespaceName := strings.TrimSpace(toolChoice.Get("namespace").String())
		toolName := strings.TrimSpace(toolChoice.Get("name").String())
		qualifiedName := qualifyXAINamespaceToolName(namespaceName, toolName)
		if namespaceName == "" || qualifiedName == "" {
			return true
		}
		updated, errSet := sjson.SetBytes(body, path+".name", qualifiedName)
		if errSet != nil {
			return false
		}
		updated, errDelete := sjson.DeleteBytes(updated, path+".namespace")
		if errDelete != nil {
			return false
		}
		body = updated
		return true
	}

	if !normalizeAtPath("tool_choice") {
		return original
	}
	tools := gjson.GetBytes(body, "tool_choice.tools")
	if tools.IsArray() {
		for index := range tools.Array() {
			if !normalizeAtPath(fmt.Sprintf("tool_choice.tools.%d", index)) {
				return original
			}
		}
	}
	return body
}

func normalizeXAITool(tool gjson.Result, namespaceName string) ([]byte, bool, bool) {
	toolType := tool.Get("type").String()
	changed := false
	if toolType == xaiToolSearchType || toolType == xaiImageGenerationToolType {
		return nil, true, true
	}
	if toolType == xaiCustomToolType && tool.Get("name").String() == "apply_patch" {
		return nil, true, true
	}

	raw := []byte(tool.Raw)
	schemaTool := tool
	if toolType == xaiFunctionToolType || toolType == xaiCustomToolType {
		updatedTool, schemaChanged, ok := normalizeXAIObjectRootUnionBranchTypes(raw)
		if !ok {
			return nil, false, false
		}
		raw = updatedTool
		if schemaChanged {
			schemaTool = gjson.ParseBytes(raw)
			changed = true
			log.Debugf("xai: added object types to root union branches for tool %s.%s", namespaceName, tool.Get("name").String())
		}
	}
	if toolType == xaiCustomToolType {
		updatedTool, errSet := sjson.SetBytes(raw, "type", xaiFunctionToolType)
		if errSet != nil {
			return nil, false, false
		}
		raw = updatedTool
		toolType = xaiFunctionToolType
		changed = true
	}
	if toolType == xaiWebSearchToolType && tool.Get("external_web_access").Exists() {
		updatedTool, errDel := sjson.DeleteBytes(raw, "external_web_access")
		if errDel != nil {
			return nil, false, false
		}
		raw = updatedTool
		changed = true
	}
	if toolType == xaiFunctionToolType && !schemaTool.Get("parameters").Exists() {
		updatedTool, errSet := sjson.SetRawBytes(raw, "parameters", []byte(`{"type":"object","properties":{}}`))
		if errSet != nil {
			return nil, false, false
		}
		raw = updatedTool
		changed = true
	}
	// Simplify the Codex Desktop automation schema and root unions that xAI
	// rejects because function parameters must resolve exclusively to objects.
	if toolType == xaiFunctionToolType && xaiFunctionParametersNeedSimplification(schemaTool, namespaceName) {
		updatedTool, errSet := sjson.SetRawBytes(raw, "parameters", []byte(xaiSafeFunctionParameters))
		if errSet != nil {
			return nil, false, false
		}
		raw = updatedTool
		if strict := tool.Get("strict"); strict.Exists() && strict.Bool() {
			updatedTool, errSet = sjson.SetBytes(raw, "strict", false)
			if errSet != nil {
				return nil, false, false
			}
			raw = updatedTool
		}
		changed = true
		log.Debugf("xai: simplified parameters for tool %s.%s to avoid upstream schema rejection or hang", namespaceName, tool.Get("name").String())
	}
	if toolType == xaiFunctionToolType && strings.TrimSpace(namespaceName) != "" {
		qualifiedName := qualifyXAINamespaceToolName(namespaceName, tool.Get("name").String())
		if qualifiedName == "" {
			return nil, false, false
		}
		updatedTool, errSet := sjson.SetBytes(raw, "name", qualifiedName)
		if errSet != nil {
			return nil, false, false
		}
		raw = updatedTool
		changed = true
	}
	return raw, changed, true
}

func qualifyXAINamespaceToolName(namespaceName, toolName string) string {
	namespaceName = strings.TrimSpace(namespaceName)
	toolName = strings.TrimSpace(toolName)
	if namespaceName == "" || toolName == "" || strings.HasPrefix(toolName, "mcp__") {
		return toolName
	}
	prefix := namespaceName
	if !strings.HasSuffix(prefix, "__") {
		prefix += "__"
	}
	if strings.HasPrefix(toolName, prefix) {
		return toolName
	}
	return prefix + toolName
}

func collectXAINamespaceToolRefs(body []byte) map[string]xaiNamespaceToolRef {
	refs := make(map[string]xaiNamespaceToolRef)
	collect := func(tools gjson.Result) {
		if !tools.Exists() || !tools.IsArray() {
			return
		}
		for _, tool := range tools.Array() {
			if tool.Get("type").String() != xaiNamespaceToolType {
				continue
			}
			namespaceName := strings.TrimSpace(tool.Get("name").String())
			if namespaceName == "" {
				continue
			}
			for _, nestedTool := range tool.Get("tools").Array() {
				toolName := strings.TrimSpace(nestedTool.Get("name").String())
				qualifiedName := qualifyXAINamespaceToolName(namespaceName, toolName)
				if qualifiedName == "" {
					continue
				}
				refs[qualifiedName] = xaiNamespaceToolRef{namespace: namespaceName, name: toolName}
			}
		}
	}
	collect(gjson.GetBytes(body, "tools"))
	input := gjson.GetBytes(body, "input")
	if input.Exists() && input.IsArray() {
		for _, item := range input.Array() {
			if item.Get("type").String() == "additional_tools" {
				collect(item.Get("tools"))
			}
		}
	}
	return refs
}

func normalizeXAIInputCustomToolCalls(body []byte) []byte {
	input := gjson.GetBytes(body, "input")
	if !input.Exists() || !input.IsArray() {
		return body
	}

	changed := false
	inputArray := input.Array()
	items := make([]json.RawMessage, 0, len(inputArray))
	for _, item := range inputArray {
		var normalized []byte
		switch item.Get("type").String() {
		case "custom_tool_call":
			callID := strings.TrimSpace(item.Get("call_id").String())
			name := strings.TrimSpace(item.Get("name").String())
			if callID == "" || name == "" {
				changed = true
				continue
			}
			normalized = []byte(`{"type":"function_call"}`)
			normalized, _ = sjson.SetBytes(normalized, "call_id", callID)
			normalized, _ = sjson.SetBytes(normalized, "name", name)
			normalized, _ = sjson.SetBytes(normalized, "arguments", xaiCustomToolCallArguments(item.Get("input")))
		case "custom_tool_call_output":
			callID := strings.TrimSpace(item.Get("call_id").String())
			if callID == "" {
				changed = true
				continue
			}
			normalized = []byte(`{"type":"function_call_output"}`)
			normalized, _ = sjson.SetBytes(normalized, "call_id", callID)
			normalized, _ = sjson.SetBytes(normalized, "output", xaiCustomToolCallOutput(item.Get("output")))
		default:
			items = append(items, json.RawMessage(item.Raw))
			continue
		}
		items = append(items, json.RawMessage(normalized))
		changed = true
	}
	if !changed {
		return body
	}

	rawInput, errMarshal := json.Marshal(items)
	if errMarshal != nil {
		return body
	}
	updated, errSet := sjson.SetRawBytes(body, "input", rawInput)
	if errSet != nil {
		return body
	}
	return updated
}

func xaiCustomToolCallArguments(input gjson.Result) string {
	if !input.Exists() {
		return "{}"
	}
	if input.Type == gjson.String {
		text := input.String()
		trimmed := strings.TrimSpace(text)
		if gjson.Valid(trimmed) {
			parsed := gjson.Parse(trimmed)
			if parsed.IsObject() {
				return parsed.Raw
			}
		}
		encoded, errMarshal := json.Marshal(text)
		if errMarshal != nil {
			return "{}"
		}
		return `{"input":` + string(encoded) + `}`
	}
	if input.IsObject() {
		return input.Raw
	}
	if input.Raw != "" {
		return `{"input":` + input.Raw + `}`
	}
	return "{}"
}

func xaiCustomToolCallOutput(output gjson.Result) string {
	if !output.Exists() {
		return ""
	}
	if output.Type == gjson.String {
		return output.String()
	}
	return output.Raw
}
