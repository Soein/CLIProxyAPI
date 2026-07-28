package executor

import "context"

type downstreamWebsocketContextKey struct{}
type requireUpstreamWebsocketContextKey struct{}

type dispatchAdmissionContextKey struct{}

// DispatchAdmissionFunc admits executor-owned work for one auth. The returned
// release function must be held until that work can no longer start upstream
// network activity.
type DispatchAdmissionFunc func(authID string) (release func(), ok bool)

// WithDownstreamWebsocket marks the current request as coming from a downstream websocket connection.
func WithDownstreamWebsocket(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, downstreamWebsocketContextKey{}, true)
}

// DownstreamWebsocket reports whether the current request originates from a downstream websocket connection.
func DownstreamWebsocket(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	raw := ctx.Value(downstreamWebsocketContextKey{})
	enabled, ok := raw.(bool)
	return ok && enabled
}

// WithRequiredUpstreamWebsocket marks a request whose incremental context is valid only on the current upstream websocket.
func WithRequiredUpstreamWebsocket(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requireUpstreamWebsocketContextKey{}, true)
}

// RequiredUpstreamWebsocket reports whether falling back to an HTTP upstream would lose request context.
func RequiredUpstreamWebsocket(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	raw := ctx.Value(requireUpstreamWebsocketContextKey{})
	enabled, ok := raw.(bool)
	return ok && enabled
}

// WithDispatchAdmission lets an executor independently admit detached work
// that may outlive the request which scheduled it.
func WithDispatchAdmission(ctx context.Context, admit DispatchAdmissionFunc) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if admit == nil {
		return ctx
	}
	return context.WithValue(ctx, dispatchAdmissionContextKey{}, admit)
}

// AdmitDispatch admits executor-owned work for authID. Contexts without a
// callback retain standalone executor behavior and are admitted by default.
func AdmitDispatch(ctx context.Context, authID string) (release func(), ok bool) {
	if ctx == nil {
		return func() {}, true
	}
	admit, _ := ctx.Value(dispatchAdmissionContextKey{}).(DispatchAdmissionFunc)
	if admit == nil {
		return func() {}, true
	}
	release, ok = admit(authID)
	if !ok {
		return nil, false
	}
	if release == nil {
		release = func() {}
	}
	return release, true
}
