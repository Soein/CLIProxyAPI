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
-- cluster_nodes  - 多职责表:
--                  (1) LeaderElector 心跳 (role 列,参见 leader.go)
--                  (2) InstanceRegistrar 路由元数据 (endpoint/weight/status,
--                      参见 registrar.go),供 new-api 一致性哈希消费
--                  两者写同一行不同列,通过 ON CONFLICT DO UPDATE SET 只更新
--                  自己负责的列避免互相覆盖。

ALTER TABLE auth_store
    ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_writer TEXT;

ALTER TABLE config_store
    ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_writer TEXT;

CREATE TABLE IF NOT EXISTS cluster_nodes (
    node_id         TEXT PRIMARY KEY,
    role            TEXT,                    -- NULL 允许 Registrar 先于 LeaderElector 插入
    region          TEXT,
    last_heartbeat  TIMESTAMPTZ NOT NULL,
    metadata        JSONB,
    endpoint        TEXT,                    -- Tailscale/公网 URL, new-api 用来转发
    weight          INT  NOT NULL DEFAULT 100,
    status          TEXT NOT NULL DEFAULT 'active'  -- active | draining | down
);

-- 升级路径: 兼容 Phase 3 时期创建的旧版 cluster_nodes
ALTER TABLE cluster_nodes
    ALTER COLUMN role DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS endpoint TEXT,
    ADD COLUMN IF NOT EXISTS weight   INT  NOT NULL DEFAULT 100,
    ADD COLUMN IF NOT EXISTS status   TEXT NOT NULL DEFAULT 'active';

CREATE INDEX IF NOT EXISTS idx_cluster_nodes_heartbeat
    ON cluster_nodes(last_heartbeat DESC);

CREATE INDEX IF NOT EXISTS idx_cluster_nodes_status
    ON cluster_nodes(status, last_heartbeat DESC);

-- Phase 4: new-api HashRing watcher 订阅的通道。Payload = node_id。
-- 函数内抑制 no-op UPDATE 通知: InstanceRegistrar 每 10s 会写入相同的
-- endpoint/weight/status 做心跳,若不过滤则 N 节点集群会产生 N×6/min 次
-- 无意义 NOTIFY,导致所有 replica 做全量 refresh 查询。INSERT/DELETE 仍
-- 无条件通知。
CREATE OR REPLACE FUNCTION notify_cpa_instance_changed()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND OLD.endpoint IS NOT DISTINCT FROM NEW.endpoint
       AND OLD.weight   IS NOT DISTINCT FROM NEW.weight
       AND OLD.status   IS NOT DISTINCT FROM NEW.status THEN
        RETURN NULL;
    END IF;
    PERFORM pg_notify('cpa_instance_changed', COALESCE(NEW.node_id, OLD.node_id));
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_cpa_instance_changed ON cluster_nodes;
CREATE TRIGGER trg_cpa_instance_changed
    AFTER INSERT OR UPDATE OF endpoint, weight, status OR DELETE
    ON cluster_nodes
    FOR EACH ROW EXECUTE FUNCTION notify_cpa_instance_changed();

-- Advisory lock 键空间约定(与代码保持一致,使用 pg_try_advisory_lock(class, id) 2-arg 版):
--   leader election:    pg_try_advisory_lock(1, 1)
--   auth refresh:       pg_try_advisory_lock(2, int32(fnv1a64("cliproxy:auth:"+authID)))

-- LISTEN 通道约定:
--   cliproxy_auth_changed        auth 行变更(payload=auth_id)
--   cliproxy_config_changed      config 行变更(payload 空)
--   cpa_instance_changed         集群成员路由信息变更(payload=node_id)
