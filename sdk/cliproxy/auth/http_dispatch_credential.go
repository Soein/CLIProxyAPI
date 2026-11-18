package auth

import (
	"context"
	"net/http"
	"strings"
)

type httpDispatchCredentialKey struct{}

type httpDispatchCredential struct {
	auth        *Auth
	accessToken string
}

// WithHTTPDispatchCredentialCapture enables a fresh observation for one HTTP attempt.
// The returned reader provides the credential snapshot after the executor prepares
// the request, or nil if the executor did not record it. Read it after dispatch returns.
func WithHTTPDispatchCredentialCapture(ctx context.Context) (context.Context, func() *Auth) {
	if ctx == nil {
		ctx = context.Background()
	}
	capture := &httpDispatchCredential{}
	return context.WithValue(ctx, httpDispatchCredentialKey{}, capture), func() *Auth { return capture.auth.Clone() }
}

// CaptureHTTPDispatchCredential records the credential and final Authorization
// header used for the HTTP attempt. It is a no-op when capture is not enabled.
func CaptureHTTPDispatchCredential(ctx context.Context, credential *Auth, headers http.Header) {
	if ctx == nil || credential == nil {
		return
	}
	capture, _ := ctx.Value(httpDispatchCredentialKey{}).(*httpDispatchCredential)
	if capture == nil {
		return
	}
	var token string
	if scheme, value, ok := strings.Cut(strings.TrimSpace(headers.Get("Authorization")), " "); ok && strings.EqualFold(scheme, "Bearer") {
		token = strings.TrimSpace(value)
	}
	capture.auth = credential.Clone()
	capture.accessToken = token
}

func httpDispatchCredentialFromContext(ctx context.Context, id string) *httpDispatchCredential {
	if ctx == nil {
		return nil
	}
	capture, _ := ctx.Value(httpDispatchCredentialKey{}).(*httpDispatchCredential)
	if capture != nil && capture.auth != nil && capture.auth.ID != id {
		return nil
	}
	return capture
}
