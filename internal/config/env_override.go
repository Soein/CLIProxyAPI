package config

import (
	"os"
	"strconv"

	log "github.com/sirupsen/logrus"
)

// ApplyClusterEnvOverrides merges CLUSTER_* environment variables into
// cfg.Cluster, taking precedence over any value loaded from config.yaml.
//
// Motivation: when the PG-backed config store is enabled (PGSTORE_DSN set),
// config.yaml is materialized from the shared PG config_store table — which
// means every replica would receive the SAME cluster block, clobbering the
// per-replica fields (node-id, endpoint, weight). Moving the per-replica
// knobs to environment variables lets each container carry its own
// identity without relying on the shared YAML.
//
// Environment variables (all optional — omit to fall back to cfg value):
//
//	CLUSTER_ENABLED             bool  ("true"/"false")
//	CLUSTER_NODE_ID             string
//	CLUSTER_REGION              string
//	CLUSTER_ENDPOINT            string  (http://<ip>:<port>)
//	CLUSTER_WEIGHT              int
//	CLUSTER_PROBE_INTERVAL      duration string (e.g. "5s")
//	CLUSTER_REGISTRAR_INTERVAL  duration string
//	CLUSTER_AUTH_SHARDING       bool
//	CLUSTER_SPILLOVER           bool
//	CLUSTER_RING_STALENESS      duration string
//	CLUSTER_RING_POLL           duration string
//
// Bools: anything strconv.ParseBool accepts (true, false, 1, 0, t, f,
// True, False, ...). Invalid values log a warning and keep the existing
// cfg value rather than silently defaulting.
func ApplyClusterEnvOverrides(cfg *Config) {
	if cfg == nil {
		return
	}
	applyBool("CLUSTER_ENABLED", &cfg.Cluster.Enabled)
	applyString("CLUSTER_NODE_ID", &cfg.Cluster.NodeID)
	applyString("CLUSTER_REGION", &cfg.Cluster.Region)
	applyString("CLUSTER_ENDPOINT", &cfg.Cluster.Endpoint)
	applyInt("CLUSTER_WEIGHT", &cfg.Cluster.Weight)
	applyString("CLUSTER_PROBE_INTERVAL", &cfg.Cluster.ProbeInterval)
	applyString("CLUSTER_REGISTRAR_INTERVAL", &cfg.Cluster.RegistrarInterval)
	applyBool("CLUSTER_AUTH_SHARDING", &cfg.Cluster.AuthSharding)
	applyBool("CLUSTER_SPILLOVER", &cfg.Cluster.Spillover)
	applyString("CLUSTER_RING_STALENESS", &cfg.Cluster.RingStalenessThreshold)
	applyString("CLUSTER_RING_POLL", &cfg.Cluster.RingPollInterval)
}

// ApplyUsageEnvOverrides merges USAGE_* environment variables into
// cfg.Usage, taking precedence over any value loaded from config.yaml.
// Same motivation as ApplyClusterEnvOverrides: when PG-backed config
// store ships a single shared YAML, per-replica usage knobs must come
// from env to allow staged rollout (one node on dual, three on memory,
// etc.).
//
// Environment variables (all optional):
//
//	USAGE_BACKEND        string  ("memory" | "dual" | "pg")
//	USAGE_FLUSH_INTERVAL duration string (e.g. "10s")
func ApplyUsageEnvOverrides(cfg *Config) {
	if cfg == nil {
		return
	}
	applyString("USAGE_BACKEND", &cfg.Usage.Backend)
	applyString("USAGE_FLUSH_INTERVAL", &cfg.Usage.FlushInterval)
}

func applyString(env string, dst *string) {
	if v, ok := os.LookupEnv(env); ok {
		*dst = v
	}
}

func applyBool(env string, dst *bool) {
	v, ok := os.LookupEnv(env)
	if !ok {
		return
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		log.Warnf("config: %s=%q is not a valid bool; keeping previous value %v", env, v, *dst)
		return
	}
	*dst = b
}

func applyInt(env string, dst *int) {
	v, ok := os.LookupEnv(env)
	if !ok {
		return
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Warnf("config: %s=%q is not a valid int; keeping previous value %d", env, v, *dst)
		return
	}
	*dst = n
}
