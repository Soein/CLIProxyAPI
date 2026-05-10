// Package kiro provides authentication functionality for the AWS Kiro IDE
// (Amazon Q Developer / CodeWhisperer) backend. It supports three credential
// acquisition methods: import (load existing oauth_creds.json), social PKCE
// (Google/GitHub via Kiro auth service), and AWS Builder ID device code.
package kiro

// Auth method identifiers persisted in the credential JSON.
const (
	AuthMethodSocial    = "social"
	AuthMethodBuilderID = "builder_id"
	AuthMethodImport    = "import"
)

// Default region used when the credential omits it.
const DefaultRegion = "us-east-1"

// Endpoint templates. {region} is substituted at call time.
const (
	SocialAuthEndpoint     = "https://prod.{region}.auth.desktop.kiro.dev"
	SocialRefreshEndpoint  = "https://prod.{region}.auth.desktop.kiro.dev/refreshToken"
	BuilderIDOIDCEndpoint  = "https://oidc.{region}.amazonaws.com"
	BuilderIDStartURL      = "https://view.awsapps.com/start"
)

// PKCE callback ports tried in order when launching the local server.
var CallbackPortRange = []int{19876, 19877, 19878, 19879, 19880}

// CodeWhisperer scopes granted to Kiro at Builder ID registration.
var BuilderIDScopes = []string{
	"codewhisperer:completions",
	"codewhisperer:analysis",
	"codewhisperer:conversations",
}

// Fake version constants matching Kiro IDE 0.11.63 — appears in user-agent.
const (
	KiroVersion     = "0.11.63"
	AwsSdkJsVersion = "1.0.34"
	AwsSdkUaVersion = "2.1"
	NodeFakeVersion = "20.11.1"
)

// Polling intervals for Builder ID device code flow.
const (
	DeviceCodePollingIntervalSec = 5
	DeviceCodeTimeoutSec         = 300
)

// MachineID fallback constant when no credential field is set.
const MachineIDFallback = "KIRO_DEFAULT_MACHINE"
