package auth

import (
	"context"
	"fmt"
	"time"

	internalkiro "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/kiro"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

// KiroAuthenticator implements the SDK Authenticator interface for the
// Kiro (Amazon Q / CodeWhisperer) provider. M1 wires Provider() and
// RefreshLead(); the full PKCE / Builder ID Login() flow is integrated in M2.
type KiroAuthenticator struct{}

// NewKiroAuthenticator constructs a new authenticator instance.
func NewKiroAuthenticator() Authenticator { return &KiroAuthenticator{} }

// Provider returns the provider key for kiro.
func (KiroAuthenticator) Provider() string { return "kiro" }

// RefreshLead instructs the manager to refresh one minute before expiry.
// The internal package's KiroAuth.RefreshLeeway uses the same value;
// they MUST stay in sync.
func (KiroAuthenticator) RefreshLead() *time.Duration {
	d := time.Minute
	return &d
}

// Login is wired in M2 once the executor + management API land. Returning
// ErrRefreshNotSupported here would be incorrect (it's not refresh); we
// instead return a clear "not yet implemented" error so calls fail fast.
func (KiroAuthenticator) Login(ctx context.Context, cfg *config.Config, opts *LoginOptions) (*coreauth.Auth, error) {
	_ = ctx
	_ = cfg
	_ = opts
	_ = internalkiro.MachineIDFallback // anchor the import so package compiles even if other refs change
	return nil, fmt.Errorf("kiro: Login not yet implemented (M2)")
}
