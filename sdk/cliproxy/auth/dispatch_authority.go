package auth

import (
	"context"
	"net/http"
	"reflect"
	"sort"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

// ErrDispatchAdmissionRejected indicates that the current dispatch authority
// does not permit a new upstream attempt for the selected auth.
var ErrDispatchAdmissionRejected = &Error{
	Code:       "auth_unavailable",
	Message:    "dispatch authority rejected admission",
	Retryable:  true,
	HTTPStatus: http.StatusServiceUnavailable,
}

// DispatchAuthority gates the start of a request that uses one auth. A
// successful admission remains valid for that request until release is
// called, even if the authority later loses the lease; loss only prevents new
// requests from starting.
type DispatchAuthority interface {
	Admit(authID string) (release func(), ok bool)
	Wake()
	Ready() bool
	WaitReady(ctx context.Context) error
	CloseAdmissions()
}

type dispatchAuthorityHolder struct {
	authority DispatchAuthority
}

// SetDispatchAuthority atomically installs the authority used by new upstream
// attempts. A nil or typed-nil authority clears the current authority.
func (m *Manager) SetDispatchAuthority(authority DispatchAuthority) {
	if m == nil {
		return
	}
	if isNilDispatchAuthority(authority) {
		m.dispatchAuthority.Store(nil)
		return
	}
	m.dispatchAuthority.Store(&dispatchAuthorityHolder{authority: authority})
	authority.Wake()
}

func isNilDispatchAuthority(authority DispatchAuthority) bool {
	if authority == nil {
		return true
	}
	value := reflect.ValueOf(authority)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (m *Manager) admitDispatch(authID string) (func(), error) {
	if m == nil {
		return nil, nil
	}
	holder := m.dispatchAuthority.Load()
	if holder == nil || holder.authority == nil {
		return nil, nil
	}
	release, ok := holder.authority.Admit(authID)
	if !ok {
		return nil, ErrDispatchAdmissionRejected
	}
	if release == nil {
		return func() {}, nil
	}
	return release, nil
}

// AdmitDispatch gates an externally managed upstream attempt for authID.
// Callers must invoke the returned release function after the admitted attempt
// finishes. A nil release means no dispatch authority is configured.
func (m *Manager) AdmitDispatch(authID string) (release func(), err error) {
	return m.admitDispatch(authID)
}

func (m *Manager) executorContextWithDispatchAdmission(ctx context.Context) context.Context {
	return cliproxyexecutor.WithDispatchAdmission(ctx, func(authID string) (func(), bool) {
		release, errAdmit := m.admitDispatch(authID)
		return release, errAdmit == nil
	})
}

func (m *Manager) executeWithDispatchAdmission(ctx context.Context, executor ProviderExecutor, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	release, errAdmit := m.admitDispatch(auth.ID)
	if errAdmit != nil {
		return cliproxyexecutor.Response{}, errAdmit
	}
	if release != nil {
		defer release()
	}
	return executor.Execute(m.executorContextWithDispatchAdmission(ctx), auth, req, opts)
}

func (m *Manager) countTokensWithDispatchAdmission(ctx context.Context, executor ProviderExecutor, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	release, errAdmit := m.admitDispatch(auth.ID)
	if errAdmit != nil {
		return cliproxyexecutor.Response{}, errAdmit
	}
	if release != nil {
		defer release()
	}
	return executor.CountTokens(m.executorContextWithDispatchAdmission(ctx), auth, req, opts)
}

func (m *Manager) prepareRequestAuthWithDispatchAdmission(ctx context.Context, preparer RequestAuthPreparer, auth *Auth) (*Auth, error) {
	release, errAdmit := m.admitDispatch(auth.ID)
	if errAdmit != nil {
		return nil, errAdmit
	}
	if release != nil {
		defer release()
	}
	return preparer.PrepareRequestAuth(m.executorContextWithDispatchAdmission(ctx), auth)
}

func (m *Manager) refreshWithDispatchAdmission(ctx context.Context, executor ProviderExecutor, auth *Auth) (*Auth, error) {
	release, errAdmit := m.admitDispatch(auth.ID)
	if errAdmit != nil {
		return nil, errAdmit
	}
	if release != nil {
		defer release()
	}
	return executor.Refresh(m.executorContextWithDispatchAdmission(ctx), auth)
}

// executeStreamWithDispatchAdmission transfers ownership of release to the
// caller when the executor successfully starts a stream. Failed starts release
// admission before returning.
func (m *Manager) executeStreamWithDispatchAdmission(ctx context.Context, executor ProviderExecutor, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (result *cliproxyexecutor.StreamResult, release func(), err error) {
	release, err = m.admitDispatch(auth.ID)
	if err != nil {
		return nil, nil, err
	}
	result, err = executor.ExecuteStream(m.executorContextWithDispatchAdmission(ctx), auth, req, opts)
	if err != nil {
		if release != nil {
			release()
		}
		return nil, nil, err
	}
	return result, release, nil
}

func (m *Manager) httpRequestWithDispatchAdmission(ctx context.Context, executor ProviderExecutor, auth *Auth, req *http.Request) (resp *http.Response, err error) {
	release, errAdmit := m.admitDispatch(auth.ID)
	if errAdmit != nil {
		return nil, errAdmit
	}
	if release != nil {
		defer release()
	}
	return executor.HttpRequest(m.executorContextWithDispatchAdmission(ctx), auth, req)
}

func (m *Manager) wakeDispatchAuthority() {
	if m == nil {
		return
	}
	holder := m.dispatchAuthority.Load()
	if holder != nil && holder.authority != nil {
		holder.authority.Wake()
	}
}

// ListDispatchAuthIDs returns a stable, sorted snapshot of every auth ID that
// may be used for dispatch, including session-scoped runtime auths.
func (m *Manager) ListDispatchAuthIDs() []string {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	ids := make(map[string]struct{}, len(m.auths))
	for id := range m.auths {
		if id != "" {
			ids[id] = struct{}{}
		}
	}
	for _, sessionAuths := range m.homeRuntimeAuths {
		for id := range sessionAuths {
			if id != "" {
				ids[id] = struct{}{}
			}
		}
	}
	m.mu.RUnlock()

	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
