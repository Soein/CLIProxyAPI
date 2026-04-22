-- 001_ha_readiness.sql: HA 改造所需 schema (Phase 1)
-- 1) version 列用于乐观锁 UPSERT,避免跨实例并发覆盖
-- 2) cluster_nodes 表用于 leader election 心跳可视化

ALTER TABLE auth_store
    ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_writer TEXT;

ALTER TABLE config_store
    ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_writer TEXT;

CREATE TABLE IF NOT EXISTS cluster_nodes (
    node_id         TEXT PRIMARY KEY,
    role            TEXT NOT NULL,
    region          TEXT,
    last_heartbeat  TIMESTAMPTZ NOT NULL,
    metadata        JSONB
);

CREATE INDEX IF NOT EXISTS idx_cluster_nodes_heartbeat
    ON cluster_nodes(last_heartbeat DESC);

-- 用于 refreshAuth 的 per-auth advisory lock key 空间约定(fnv1a hash)
-- 用于 leader election 的 advisory lock: lock_key = 0xC11F0001
