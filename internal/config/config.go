// Package config provides configuration management for the CLI Proxy API server.
// It handles loading and parsing YAML configuration files, and provides structured
// access to application settings including server port, authentication directory,
// debug settings, proxy configuration, and API keys.
package config

// Config represents the application's configuration, loaded from a YAML file.
type Config struct {
	SDKConfig `yaml:",inline"`
	// Host is the network host/interface on which the API server will bind.
	// Default is empty ("") to bind all interfaces (IPv4 + IPv6). Use "127.0.0.1" or "localhost" for local-only access.
	Host string `yaml:"host" json:"-"`
	// Port is the network port on which the API server will listen.
	Port int `yaml:"port" json:"-"`

	// TLS config controls HTTPS server settings.
	TLS TLSConfig `yaml:"tls" json:"tls"`

	// Home config is runtime-only and is populated from -home-jwt.
	Home HomeConfig `yaml:"-" json:"-"`

	// CredentialConcurrency contains Home-authoritative credential lifecycle settings.
	CredentialConcurrency CredentialConcurrencyConfig `yaml:"credential-concurrency" json:"credential-concurrency"`

	// CredentialInFlight configures credential observation snapshots.
	CredentialInFlight CredentialInFlightConfig `yaml:"credential-in-flight" json:"credential-in-flight"`

	// RemoteManagement nests management-related options under 'remote-management'.
	RemoteManagement RemoteManagement `yaml:"remote-management" json:"-"`

	// Plugins configures dynamic plugin discovery and per-plugin settings.
	Plugins PluginsConfig `yaml:"plugins" json:"plugins"`

	// AuthDir is the directory where authentication token files are stored.
	AuthDir string `yaml:"auth-dir" json:"-"`

	// Debug enables or disables debug-level logging and other debug features.
	Debug bool `yaml:"debug" json:"debug"`

	// Pprof config controls the optional pprof HTTP debug server.
	Pprof PprofConfig `yaml:"pprof" json:"pprof"`

	// CommercialMode disables high-overhead request logging and HTTP middleware features to minimize per-request memory usage.
	CommercialMode bool `yaml:"commercial-mode" json:"commercial-mode"`

	// LoggingToFile controls whether application logs are written to rotating files or stdout.
	LoggingToFile bool `yaml:"logging-to-file" json:"logging-to-file"`

	// LogsMaxTotalSizeMB limits the total size (in MB) of log files under the logs directory.
	// When exceeded, the oldest log files are deleted until within the limit. Set to 0 to disable.
	LogsMaxTotalSizeMB int `yaml:"logs-max-total-size-mb" json:"logs-max-total-size-mb"`

	// ErrorLogsMaxFiles limits the number of error log files retained when request logging is disabled.
	// When exceeded, the oldest error log files are deleted. Default is 10. Set to 0 to disable cleanup.
	ErrorLogsMaxFiles int `yaml:"error-logs-max-files" json:"error-logs-max-files"`

	// UsageStatisticsEnabled toggles in-memory usage aggregation; when false, usage data is discarded.
	UsageStatisticsEnabled bool `yaml:"usage-statistics-enabled" json:"usage-statistics-enabled"`

	// RedisUsageQueueRetentionSeconds controls how long usage queue items are retained
	// in memory for Management API consumers.
	// Default: 60. Max: 3600.
	RedisUsageQueueRetentionSeconds int `yaml:"redis-usage-queue-retention-seconds" json:"redis-usage-queue-retention-seconds"`

	// DisableCooling disables auth/model cooldown scheduling when true unless a credential or provider overrides it.
	DisableCooling bool `yaml:"disable-cooling" json:"disable-cooling"`

	// SaveCooldownStatus persists runtime cooldown status next to auth files when true.
	SaveCooldownStatus bool `yaml:"save-cooldown-status" json:"save-cooldown-status"`

	// TransientErrorCooldownSeconds controls cooldowns for transient upstream errors.
	// 0 keeps the legacy default cooldown. Negative values disable these cooldowns.
	TransientErrorCooldownSeconds int `yaml:"transient-error-cooldown-seconds" json:"transient-error-cooldown-seconds"`

	// AuthAutoRefreshWorkers overrides the size of the core auth auto-refresh worker pool.
	// When <= 0, the default worker count is used.
	AuthAutoRefreshWorkers int `yaml:"auth-auto-refresh-workers" json:"auth-auto-refresh-workers"`

	// RequestRetry defines the number of additional credential retry rounds after
	// the first round has exhausted its eligible credentials.
	RequestRetry int `yaml:"request-retry" json:"request-retry"`
	// MaxRetryCredentials defines the maximum number of different credentials to
	// try in each credential retry round.
	// Set to 0 or a negative value to keep trying all available credentials (legacy behavior).
	MaxRetryCredentials int `yaml:"max-retry-credentials" json:"max-retry-credentials"`
	// MaxRetryInterval defines the maximum positive cooldown wait, in seconds,
	// allowed before starting another credential retry round. A non-positive value
	// forbids positive cooldown waits; it does not disable same-round credential
	// failover or immediate additional rounds allowed by RequestRetry.
	MaxRetryInterval int `yaml:"max-retry-interval" json:"max-retry-interval"`

	// QuotaExceeded defines the behavior when a quota is exceeded.
	QuotaExceeded QuotaExceeded `yaml:"quota-exceeded" json:"quota-exceeded"`

	// Routing controls credential selection behavior.
	Routing RoutingConfig `yaml:"routing" json:"routing"`

	// WebsocketAuth enables or disables authentication for the WebSocket API.
	WebsocketAuth bool `yaml:"ws-auth" json:"ws-auth"`

	// AntigravitySignatureCacheEnabled controls whether signature cache validation is enabled for thinking blocks.
	// When true (default), cached signatures are preferred and validated.
	// When false, client signatures are used directly after normalization (bypass mode).
	AntigravitySignatureCacheEnabled *bool `yaml:"antigravity-signature-cache-enabled,omitempty" json:"antigravity-signature-cache-enabled,omitempty"`

	AntigravitySignatureBypassStrict *bool `yaml:"antigravity-signature-bypass-strict,omitempty" json:"antigravity-signature-bypass-strict,omitempty"`

	// Antigravity configures provider-wide Antigravity request behavior.
	Antigravity AntigravityConfig `yaml:"antigravity" json:"antigravity"`

	// GeminiKey defines Gemini API key configurations with optional routing overrides.
	GeminiKey []GeminiKey `yaml:"gemini-api-key" json:"gemini-api-key"`

	// InteractionsKey defines native Google Interactions API key configurations.
	InteractionsKey []GeminiKey `yaml:"interactions-api-key" json:"interactions-api-key"`

	// Codex defines a list of Codex API key configurations as specified in the YAML configuration file.
	CodexKey []CodexKey `yaml:"codex-api-key" json:"codex-api-key"`

	// XAIKey defines xAI API key configurations using the same structure as Codex API keys.
	XAIKey []XAIKey `yaml:"xai-api-key" json:"xai-api-key"`

	// XAI configures provider-wide xAI request behavior.
	XAI XAIConfig `yaml:"xai" json:"xai"`

	// Codex configures provider-wide Codex request behavior.
	Codex CodexConfig `yaml:"codex" json:"codex"`

	// CodexHeaderDefaults configures fallback headers for Codex OAuth model requests.
	// These are used only when the client does not send its own headers.
	CodexHeaderDefaults CodexHeaderDefaults `yaml:"codex-header-defaults" json:"codex-header-defaults"`

	// ClaudeKey defines a list of Claude API key configurations as specified in the YAML configuration file.
	ClaudeKey []ClaudeKey `yaml:"claude-api-key" json:"claude-api-key"`

	// ClaudeHeaderDefaults configures default header values for Claude API requests.
	// These are used as fallbacks when the client does not send its own headers.
	ClaudeHeaderDefaults ClaudeHeaderDefaults `yaml:"claude-header-defaults" json:"claude-header-defaults"`

	// DisableClaudeCloakMode globally disables Claude request cloaking when true.
	// Cloaking disguises requests as the official Claude Code CLI and replaces the
	// system prompt. When true, every Claude credential defaults to no cloaking
	// ("never"); a specific credential can still re-enable or override it via its own
	// cloak settings (the per claude-api-key "cloak" block, or a "cloak_mode" value in
	// the auth/OAuth token file). Default false preserves the per-client "auto" behavior.
	DisableClaudeCloakMode bool `yaml:"disable-claude-cloak-mode" json:"disable-claude-cloak-mode"`

	// OpenAICompatibility defines OpenAI API compatibility configurations for external providers.
	OpenAICompatibility []OpenAICompatibility `yaml:"openai-compatibility" json:"openai-compatibility"`

	// VertexCompatAPIKey defines Vertex AI-compatible API key configurations for third-party providers.
	// Used for services that use Vertex AI-style paths but with simple API key authentication.
	VertexCompatAPIKey []VertexCompatKey `yaml:"vertex-api-key" json:"vertex-api-key"`

	// OAuthExcludedModels defines per-provider global model exclusions applied to OAuth/file-backed auth entries.
	OAuthExcludedModels map[string][]string `yaml:"oauth-excluded-models,omitempty" json:"oauth-excluded-models,omitempty"`

	// OAuthModelAlias defines global model name aliases for OAuth/file-backed auth channels.
	// These aliases affect both model listing and model routing for supported channels:
	// vertex, aistudio, antigravity, claude, codex, kimi, xai.
	//
	// NOTE: This does not apply to existing per-credential model alias features under:
	// gemini-api-key, interactions-api-key, codex-api-key, xai-api-key, claude-api-key, openai-compatibility, and vertex-api-key.
	OAuthModelAlias map[string][]OAuthModelAlias `yaml:"oauth-model-alias,omitempty" json:"oauth-model-alias,omitempty"`

	// OAuthRequestScopedErrors defines per-provider request-scoped error rules applied to OAuth/file-backed auth entries.
	// Supported channels include: vertex, aistudio, antigravity, claude, codex, kimi, xai, and OAuth plugin provider keys.
	//
	// NOTE: This applies only to OAuth credentials and does not affect per-credential request-scoped-errors under *-api-key.
	OAuthRequestScopedErrors map[string][]RequestScopedErrorRule `yaml:"oauth-request-scoped-errors,omitempty" json:"oauth-request-scoped-errors,omitempty"`

	// Payload defines default and override rules for provider payload parameters.
	Payload PayloadConfig `yaml:"payload" json:"payload"`

	// Cluster configures multi-node HA coordination. Fields are all opt-in:
	// with Enabled=false (default) the service runs in single-instance mode
	// exactly as before. When Enabled=true the host must also configure a
	// Postgres-backed token store.
	Cluster ClusterConfig `yaml:"cluster,omitempty" json:"cluster,omitempty"`

	// Usage configures usage-statistics persistence: in-memory only (default),
	// dual-write to PG (rollout safety), or PG-authoritative (cluster aggregated).
	Usage UsageConfig `yaml:"usage,omitempty" json:"usage,omitempty"`
}

// ClusterConfig selects and parameterizes multi-node HA coordination. Meaningful
// only when the token store is Postgres-backed; otherwise the fields are ignored.
type ClusterConfig struct {
	// Enabled turns on cluster-mode hooks such as leader election and
	// cross-replica change propagation.
	Enabled bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	// NodeID identifies this replica; it defaults to the machine hostname.
	NodeID string `yaml:"node-id,omitempty" json:"node-id,omitempty"`
	// Region is a free-form operator-visible label.
	Region string `yaml:"region,omitempty" json:"region,omitempty"`
	// ProbeInterval is how often the leader elector re-checks its advisory
	// lock. Auth sharding also uses it as the node-lease and serving-path
	// watchdog cadence, capped to one fifth of RingStalenessThreshold.
	// Parsed as a Go duration string (e.g. "5s"). Empty/invalid means 5s.
	ProbeInterval string `yaml:"probe-interval,omitempty" json:"probe-interval,omitempty"`
	// Endpoint advertises this replica to an external front-door router.
	Endpoint string `yaml:"endpoint,omitempty" json:"endpoint,omitempty"`
	// Weight is the relative share assigned by the front-door router.
	Weight int `yaml:"weight,omitempty" json:"weight,omitempty"`
	// RegistrarInterval is how often InstanceRegistrar refreshes the row in
	// cluster_nodes. Parsed as a Go duration; empty/invalid means 10s.
	// Auth-sharding startup rejects combinations that cannot detect a failed
	// heartbeat strictly before RingStalenessThreshold.
	RegistrarInterval string `yaml:"registrar-interval,omitempty" json:"registrar-interval,omitempty"`
	// AuthSharding, when true, routes each auth (OAuth account) to exactly
	// one replica using weighted rendezvous hashing on cluster_nodes
	// membership. Each replica only dispatches / refreshes auths that hash
	// to its NodeID, ensuring cross-instance rate-limit, usage and
	// cooldown tracking stay consistent.
	//
	// Default false preserves Phase 1-3 behavior (all replicas use all
	// auths). This flag is read during cluster bootstrap; changing it requires
	// every replica to restart so the cluster cannot mix ownership modes. It
	// cannot be combined with Home mode or Spillover.
	AuthSharding bool `yaml:"auth-sharding,omitempty" json:"auth-sharding,omitempty"`
	// Spillover preserves legacy non-strict routing behavior outside auth
	// sharding. Strict auth sharding rejects this option at startup because a
	// non-owner may never dispatch an account.
	Spillover bool `yaml:"spillover,omitempty" json:"spillover,omitempty"`
	// RingStalenessThreshold is how old a cluster_nodes row may be before
	// the RingWatcher treats it as dead and excludes it from the ring.
	// Parsed as a Go duration; empty/invalid means 30s. Auth-sharding validates
	// this against both RegistrarInterval and the effective watchdog cadence;
	// unsafe custom timing fails startup. Dispatch ownership leases use half
	// this duration, capped at 15s, and must retain more than a 500ms guard.
	// The defaults are safe.
	RingStalenessThreshold string `yaml:"ring-staleness,omitempty" json:"ring-staleness,omitempty"`
	// RingPollInterval is how often the RingWatcher re-queries
	// cluster_nodes as a safety net in case LISTEN/NOTIFY drops a message.
	// Parsed as a Go duration; empty/invalid means 30s. Auth sharding caps the
	// effective poll interval to its node-lease watchdog cadence.
	RingPollInterval string `yaml:"ring-poll-interval,omitempty" json:"ring-poll-interval,omitempty"`
}

// UsageConfig governs usage-statistics persistence.
// Backend is memory (default), dual, or pg.
type UsageConfig struct {
	Backend             string `yaml:"backend,omitempty" json:"backend,omitempty"`
	FlushInterval       string `yaml:"flush-interval,omitempty" json:"flush-interval,omitempty"`
	FlushBatchSize      int    `yaml:"flush-batch-size,omitempty" json:"flush-batch-size,omitempty"`
	EventRetentionDays  int    `yaml:"event-retention-days,omitempty" json:"event-retention-days,omitempty"`
	RollupRetentionDays int    `yaml:"rollup-retention-days,omitempty" json:"rollup-retention-days,omitempty"`
	QueryCacheTTL       string `yaml:"query-cache-ttl,omitempty" json:"query-cache-ttl,omitempty"`
}

func (u UsageConfig) WithDefaults() UsageConfig {
	if u.Backend == "" {
		u.Backend = "memory"
	}
	if u.FlushInterval == "" {
		u.FlushInterval = "10s"
	}
	if u.FlushBatchSize == 0 {
		u.FlushBatchSize = 1000
	}
	if u.EventRetentionDays == 0 {
		u.EventRetentionDays = 7
	}
	if u.RollupRetentionDays == 0 {
		u.RollupRetentionDays = 90
	}
	if u.QueryCacheTTL == "" {
		u.QueryCacheTTL = "5s"
	}
	return u
}
