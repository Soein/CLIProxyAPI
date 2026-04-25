package config

import (
	"testing"
)

func TestApplyClusterEnvOverrides_NilCfgSafe(t *testing.T) {
	ApplyClusterEnvOverrides(nil) // must not panic
}

func TestApplyClusterEnvOverrides_AllFields(t *testing.T) {
	t.Setenv("CLUSTER_ENABLED", "true")
	t.Setenv("CLUSTER_NODE_ID", "fra-01")
	t.Setenv("CLUSTER_REGION", "fra")
	t.Setenv("CLUSTER_ENDPOINT", "http://100.101.208.10:8317")
	t.Setenv("CLUSTER_WEIGHT", "100")
	t.Setenv("CLUSTER_PROBE_INTERVAL", "7s")
	t.Setenv("CLUSTER_REGISTRAR_INTERVAL", "12s")
	t.Setenv("CLUSTER_AUTH_SHARDING", "true")
	t.Setenv("CLUSTER_SPILLOVER", "false")
	t.Setenv("CLUSTER_RING_STALENESS", "45s")
	t.Setenv("CLUSTER_RING_POLL", "20s")

	cfg := &Config{}
	ApplyClusterEnvOverrides(cfg)

	if !cfg.Cluster.Enabled {
		t.Error("Enabled should be true")
	}
	if cfg.Cluster.NodeID != "fra-01" {
		t.Errorf("NodeID=%q", cfg.Cluster.NodeID)
	}
	if cfg.Cluster.Region != "fra" {
		t.Errorf("Region=%q", cfg.Cluster.Region)
	}
	if cfg.Cluster.Endpoint != "http://100.101.208.10:8317" {
		t.Errorf("Endpoint=%q", cfg.Cluster.Endpoint)
	}
	if cfg.Cluster.Weight != 100 {
		t.Errorf("Weight=%d", cfg.Cluster.Weight)
	}
	if cfg.Cluster.ProbeInterval != "7s" {
		t.Errorf("ProbeInterval=%q", cfg.Cluster.ProbeInterval)
	}
	if cfg.Cluster.RegistrarInterval != "12s" {
		t.Errorf("RegistrarInterval=%q", cfg.Cluster.RegistrarInterval)
	}
	if !cfg.Cluster.AuthSharding {
		t.Error("AuthSharding should be true")
	}
	if cfg.Cluster.Spillover {
		t.Error("Spillover should be false")
	}
	if cfg.Cluster.RingStalenessThreshold != "45s" {
		t.Errorf("RingStalenessThreshold=%q", cfg.Cluster.RingStalenessThreshold)
	}
	if cfg.Cluster.RingPollInterval != "20s" {
		t.Errorf("RingPollInterval=%q", cfg.Cluster.RingPollInterval)
	}
}

func TestApplyClusterEnvOverrides_PreservesWhenEnvAbsent(t *testing.T) {
	// Deliberately do NOT set env vars. Pre-fill cfg and verify nothing
	// overwrites it.
	cfg := &Config{}
	cfg.Cluster.NodeID = "sj-01"
	cfg.Cluster.Weight = 50
	cfg.Cluster.Enabled = true
	ApplyClusterEnvOverrides(cfg)
	if cfg.Cluster.NodeID != "sj-01" {
		t.Errorf("NodeID was clobbered: %q", cfg.Cluster.NodeID)
	}
	if cfg.Cluster.Weight != 50 {
		t.Errorf("Weight was clobbered: %d", cfg.Cluster.Weight)
	}
	if !cfg.Cluster.Enabled {
		t.Error("Enabled was clobbered")
	}
}

func TestApplyClusterEnvOverrides_InvalidBoolKept(t *testing.T) {
	t.Setenv("CLUSTER_ENABLED", "notabool")
	cfg := &Config{}
	cfg.Cluster.Enabled = true
	ApplyClusterEnvOverrides(cfg)
	if !cfg.Cluster.Enabled {
		t.Error("invalid bool should leave value unchanged")
	}
}

func TestApplyClusterEnvOverrides_InvalidIntKept(t *testing.T) {
	t.Setenv("CLUSTER_WEIGHT", "abc")
	cfg := &Config{}
	cfg.Cluster.Weight = 99
	ApplyClusterEnvOverrides(cfg)
	if cfg.Cluster.Weight != 99 {
		t.Errorf("invalid int should leave value unchanged, got %d", cfg.Cluster.Weight)
	}
}

// Empty string IS a deliberate value (operator wrote CLUSTER_NODE_ID=""
// to clear). Preserve that behavior.
func TestApplyClusterEnvOverrides_EmptyStringApplied(t *testing.T) {
	t.Setenv("CLUSTER_NODE_ID", "")
	cfg := &Config{}
	cfg.Cluster.NodeID = "sj-01"
	ApplyClusterEnvOverrides(cfg)
	if cfg.Cluster.NodeID != "" {
		t.Errorf("empty env should be applied, got %q", cfg.Cluster.NodeID)
	}
}

func TestApplyUsageEnvOverrides_AllFields(t *testing.T) {
	t.Setenv("USAGE_BACKEND", "dual")
	t.Setenv("USAGE_FLUSH_INTERVAL", "5s")

	cfg := &Config{}
	ApplyUsageEnvOverrides(cfg)

	if cfg.Usage.Backend != "dual" {
		t.Errorf("Backend=%q want dual", cfg.Usage.Backend)
	}
	if cfg.Usage.FlushInterval != "5s" {
		t.Errorf("FlushInterval=%q want 5s", cfg.Usage.FlushInterval)
	}
}

func TestApplyUsageEnvOverrides_NilCfgSafe(t *testing.T) {
	ApplyUsageEnvOverrides(nil) // must not panic
}

func TestApplyUsageEnvOverrides_PreservesWhenAbsent(t *testing.T) {
	cfg := &Config{}
	cfg.Usage.Backend = "pg"
	cfg.Usage.FlushInterval = "30s"
	ApplyUsageEnvOverrides(cfg)
	if cfg.Usage.Backend != "pg" {
		t.Errorf("Backend was clobbered: %q", cfg.Usage.Backend)
	}
	if cfg.Usage.FlushInterval != "30s" {
		t.Errorf("FlushInterval was clobbered: %q", cfg.Usage.FlushInterval)
	}
}
