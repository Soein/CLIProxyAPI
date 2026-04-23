-- 001_ha_readiness.sql: HA 改造所需 schema 参考
--
-- 注意: 该文件仅作为文档/备用手动迁移使用。runtime 的等价 schema 已经
-- 嵌入在 internal/store/postgresstore.go 的 EnsureSchema() 中,所有列都
-- 以 ADD COLUMN IF NOT EXISTS 方式添加,幂等。新部署直接启动 cli-proxy-api
-- 即可;只有在想提前准备 schema 或者走外部迁移工具时才手动执行本文件。
--
-- 列含义:
--   version      - 每次 UPSERT 递增的审计计数。注意: 本实现为
--                  "fire-and-forget" UPSERT,并不做 WHERE version = $expected
--                  的乐观锁检查。跨实例互斥由 auth.AuthRefreshLocker
--                  (sdk/cliproxy/cluster.PgAuthRefreshLocker) 在 refreshAuth
--                  外层保证。
--   last_writer  - 写入该行的节点标识,默认 hostname。
--
-- cluster_nodes  - LeaderElector 心跳表,便于用 SQL 查看集群当前 leader。
--                  不参与任何运行时决策,只是监控/运维可见性。

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

-- Advisory lock 键空间约定(与代码保持一致,使用 pg_try_advisory_lock(class, id) 2-arg 版):
--   leader election:    pg_try_advisory_lock(1, 1)
--   auth refresh:       pg_try_advisory_lock(2, int32(fnv1a64("cliproxy:auth:"+authID)))
