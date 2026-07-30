package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func discardStreamChunks(ch <-chan cliproxyexecutor.StreamChunk) {
	if ch == nil {
		return
	}
	go func() {
		for range ch {
		}
	}()
}

type streamBootstrapError struct {
	cause   error
	headers http.Header
}

func cloneHTTPHeader(headers http.Header) http.Header {
	if headers == nil {
		return nil
	}
	return headers.Clone()
}

func newStreamBootstrapError(err error, headers http.Header) error {
	if err == nil {
		return nil
	}
	return &streamBootstrapError{
		cause:   err,
		headers: cloneHTTPHeader(headers),
	}
}

func (e *streamBootstrapError) Error() string {
	if e == nil || e.cause == nil {
		return ""
	}
	return e.cause.Error()
}

func (e *streamBootstrapError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *streamBootstrapError) Headers() http.Header {
	if e == nil {
		return nil
	}
	return cloneHTTPHeader(e.headers)
}

func streamErrorResult(headers http.Header, err error) *cliproxyexecutor.StreamResult {
	ch := make(chan cliproxyexecutor.StreamChunk, 1)
	ch <- cliproxyexecutor.StreamChunk{Err: err}
	close(ch)
	return &cliproxyexecutor.StreamResult{
		Headers: cloneHTTPHeader(headers),
		Chunks:  ch,
	}
}

func readStreamBootstrap(ctx context.Context, ch <-chan cliproxyexecutor.StreamChunk) ([]cliproxyexecutor.StreamChunk, bool, error) {
	if ch == nil {
		return nil, true, nil
	}
	buffered := make([]cliproxyexecutor.StreamChunk, 0, 1)
	for {
		var (
			chunk cliproxyexecutor.StreamChunk
			ok    bool
		)
		if ctx != nil {
			select {
			case <-ctx.Done():
				return nil, false, ctx.Err()
			case chunk, ok = <-ch:
			}
		} else {
			chunk, ok = <-ch
		}
		if !ok {
			return buffered, true, nil
		}
		if chunk.Err != nil {
			return nil, false, chunk.Err
		}
		buffered = append(buffered, chunk)
		if len(chunk.Payload) > 0 {
			return buffered, false, nil
		}
	}
}

func (m *Manager) wrapStreamResult(ctx context.Context, auth *Auth, provider, resultModel string, headers http.Header, buffered []cliproxyexecutor.StreamChunk, remaining <-chan cliproxyexecutor.StreamChunk, aliasResult OAuthModelAliasResult, ephemeralResult bool, lease *authLease, dispatchRelease func()) *cliproxyexecutor.StreamResult {
	out := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		defer close(out)
		defer lease.Release()
		if dispatchRelease != nil {
			defer dispatchRelease()
		}
		var failed bool
		forward := true
		var rewriter *StreamRewriter
		if aliasResult.ForceMapping && strings.TrimSpace(aliasResult.OriginalAlias) != "" {
			rewriter = NewStreamRewriter(StreamRewriteOptions{RewriteModel: aliasResult.OriginalAlias})
		}
		emit := func(chunk cliproxyexecutor.StreamChunk) bool {
			if chunk.Err != nil && !failed {
				failed = true
				rerr := resultErrorFromError(chunk.Err)
				m.recordExecutionResult(ctx, Result{AuthID: auth.ID, Provider: provider, Model: resultModel, Success: false, Error: rerr}, auth, ephemeralResult)
			}
			if !forward {
				return false
			}
			if chunk.Err != nil {
				if ctx == nil {
					out <- chunk
					return true
				}
				select {
				case <-ctx.Done():
					forward = false
					return false
				case out <- chunk:
					return true
				}
			}
			if len(chunk.Payload) == 0 {
				return true
			}
			payload := rewriteForceMappedStreamChunk(rewriter, chunk.Payload)
			if len(payload) == 0 {
				return true
			}
			chunk.Payload = payload
			if ctx == nil {
				out <- chunk
				return true
			}
			select {
			case <-ctx.Done():
				forward = false
				return false
			case out <- chunk:
				return true
			}
		}
		for _, chunk := range buffered {
			if ok := emit(chunk); !ok {
				discardStreamChunks(remaining)
				return
			}
		}
		for {
			var (
				chunk cliproxyexecutor.StreamChunk
				ok    bool
			)
			if ctx == nil {
				chunk, ok = <-remaining
			} else {
				select {
				case <-ctx.Done():
					discardStreamChunks(remaining)
					return
				case chunk, ok = <-remaining:
				}
			}
			if !ok {
				break
			}
			if emitted := emit(chunk); !emitted {
				discardStreamChunks(remaining)
				return
			}
		}
		if tail := finishForceMappedStreamChunks(rewriter); len(tail) > 0 {
			tailChunk := cliproxyexecutor.StreamChunk{Payload: tail}
			if !emit(tailChunk) {
				return
			}
		}
		if !failed {
			m.recordExecutionResult(ctx, Result{AuthID: auth.ID, Provider: provider, Model: resultModel, Success: true}, auth, ephemeralResult)
		}
	}()
	return &cliproxyexecutor.StreamResult{Headers: headers, Chunks: out}
}

func (m *Manager) executeStreamWithModelPool(ctx context.Context, executor ProviderExecutor, auth *Auth, provider string, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, routeModel, executionModel string, execModels []string, pooled bool, aliasResult OAuthModelAliasResult, routing *apiKeyModelRoutingSnapshot, allowRetry bool, ephemeralResult bool, lease *authLease) (*cliproxyexecutor.StreamResult, error) {
	ownedLease := lease
	defer func() {
		ownedLease.Release()
	}()
	if executor == nil {
		return nil, &Error{Code: "executor_not_found", Message: "executor not registered"}
	}
	ctx = contextWithRequestedModelAlias(ctx, opts, routeModel)
	var lastErr error
	didRefreshOnUnauthorized := false
	for idx, execModel := range execModels {
		var dispatchRelease func()
		resultModel := m.stateModelForExecution(auth, routeModel, execModel, pooled)
		execReq := req
		execReq.Model = execModel
		if executionModel != "" {
			execReq.Model = executionModel
		}
		execOpts := opts
		var errIntercept error
		execReq, execOpts, errIntercept = applyRequestAfterAuthInterceptor(ctx, executor, provider, execReq, execOpts, requestedModelAliasFromOptions(execOpts, routeModel))
		if errIntercept != nil {
			return nil, errIntercept
		}
		if executionModel == "" {
			execReq = attachResolvedAPIKeyModelInfo(routing, execReq, auth, routeModel, execModel)
		}
		if errCtx := ctx.Err(); errCtx != nil {
			return nil, errCtx
		}
		streamResult, dispatchRelease, errStream := m.executeStreamWithDispatchAdmission(ctx, executor, auth, execReq, execOpts)
		if errors.Is(errStream, ErrDispatchAdmissionRejected) {
			return nil, errStream
		}
		if errStream != nil {
			if errCtx := ctx.Err(); errCtx != nil {
				return nil, errCtx
			}
			if allowRetry {
				refreshed, okRefresh, errRefresh := m.tryRefreshAfterUnauthorized(ctx, auth, errStream, didRefreshOnUnauthorized)
				if errRefresh != nil {
					return nil, errRefresh
				}
				if okRefresh {
					auth = refreshed
					didRefreshOnUnauthorized = true
					streamResult, dispatchRelease, errStream = m.executeStreamWithDispatchAdmission(ctx, executor, auth, execReq, execOpts)
					if errors.Is(errStream, ErrDispatchAdmissionRejected) {
						return nil, errStream
					}
					if errStream != nil {
						if errCtx := ctx.Err(); errCtx != nil {
							return nil, errCtx
						}
					}
				}
			}
		}
		if errStream == nil && (streamResult == nil || streamResult.Chunks == nil) {
			if dispatchRelease != nil {
				dispatchRelease()
				dispatchRelease = nil
			}
			errStream = &Error{Code: "empty_stream", Message: "upstream stream has no source", Retryable: true}
		}
		if errStream != nil {
			rerr := resultErrorFromError(errStream)
			result := Result{AuthID: auth.ID, Provider: provider, Model: resultModel, Success: false, Error: rerr}
			result.RetryAfter = retryAfterFromError(errStream)
			m.recordExecutionResult(ctx, result, auth, ephemeralResult)
			if isRequestInvalidError(errStream) {
				return nil, errStream
			}
			lastErr = errStream
			continue
		}

		buffered, closed, bootstrapErr := readStreamBootstrap(ctx, streamResult.Chunks)
		if bootstrapErr != nil {
			if errCtx := ctx.Err(); errCtx != nil {
				discardStreamChunks(streamResult.Chunks)
				if dispatchRelease != nil {
					dispatchRelease()
				}
				return nil, errCtx
			}
			if allowRetry {
				refreshed, okRefresh, errRefresh := m.tryRefreshAfterUnauthorized(ctx, auth, bootstrapErr, didRefreshOnUnauthorized)
				if errRefresh != nil {
					discardStreamChunks(streamResult.Chunks)
					if dispatchRelease != nil {
						dispatchRelease()
						dispatchRelease = nil
					}
					return nil, errRefresh
				}
				if okRefresh {
					discardStreamChunks(streamResult.Chunks)
					if dispatchRelease != nil {
						dispatchRelease()
						dispatchRelease = nil
					}
					auth = refreshed
					didRefreshOnUnauthorized = true
					retryStream, retryRelease, retryErr := m.executeStreamWithDispatchAdmission(ctx, executor, auth, execReq, execOpts)
					if errors.Is(retryErr, ErrDispatchAdmissionRejected) {
						return nil, retryErr
					}
					if retryErr != nil {
						if errCtx := ctx.Err(); errCtx != nil {
							return nil, errCtx
						}
						bootstrapErr = retryErr
						streamResult = &cliproxyexecutor.StreamResult{}
					} else {
						streamResult = retryStream
						dispatchRelease = retryRelease
						buffered, closed, bootstrapErr = readStreamBootstrap(ctx, streamResult.Chunks)
					}
				}
			}
		}
		if bootstrapErr != nil {
			if dispatchRelease != nil {
				dispatchRelease()
				dispatchRelease = nil
			}
			if isRequestInvalidError(bootstrapErr) {
				rerr := resultErrorFromError(bootstrapErr)
				result := Result{AuthID: auth.ID, Provider: provider, Model: resultModel, Success: false, Error: rerr}
				result.RetryAfter = retryAfterFromError(bootstrapErr)
				m.recordExecutionResult(ctx, result, auth, ephemeralResult)
				discardStreamChunks(streamResult.Chunks)
				return nil, bootstrapErr
			}
			if idx < len(execModels)-1 {
				rerr := resultErrorFromError(bootstrapErr)
				result := Result{AuthID: auth.ID, Provider: provider, Model: resultModel, Success: false, Error: rerr}
				result.RetryAfter = retryAfterFromError(bootstrapErr)
				m.recordExecutionResult(ctx, result, auth, ephemeralResult)
				discardStreamChunks(streamResult.Chunks)
				lastErr = bootstrapErr
				continue
			}
			rerr := resultErrorFromError(bootstrapErr)
			result := Result{AuthID: auth.ID, Provider: provider, Model: resultModel, Success: false, Error: rerr}
			result.RetryAfter = retryAfterFromError(bootstrapErr)
			m.recordExecutionResult(ctx, result, auth, ephemeralResult)
			discardStreamChunks(streamResult.Chunks)
			return nil, newStreamBootstrapError(bootstrapErr, streamResult.Headers)
		}

		if closed && len(buffered) == 0 {
			if dispatchRelease != nil {
				dispatchRelease()
				dispatchRelease = nil
			}
			emptyErr := &Error{Code: "empty_stream", Message: "upstream stream closed before first payload", Retryable: true}
			result := Result{AuthID: auth.ID, Provider: provider, Model: resultModel, Success: false, Error: emptyErr}
			m.recordExecutionResult(ctx, result, auth, ephemeralResult)
			if idx < len(execModels)-1 {
				lastErr = emptyErr
				continue
			}
			return nil, newStreamBootstrapError(emptyErr, streamResult.Headers)
		}

		remaining := streamResult.Chunks
		if closed {
			closedCh := make(chan cliproxyexecutor.StreamChunk)
			close(closedCh)
			remaining = closedCh
		}
		attemptAliasResult := resolveAttemptAliasResult(routing, auth, routeModel, execModel, aliasResult)
		wrapped := m.wrapStreamResult(ctx, auth.Clone(), provider, resultModel, streamResult.Headers, buffered, remaining, attemptAliasResult, ephemeralResult, ownedLease, dispatchRelease)
		ownedLease = nil
		return wrapped, nil
	}
	if lastErr == nil {
		lastErr = &Error{Code: "auth_not_found", Message: "no upstream model available"}
	}
	return nil, lastErr
}
