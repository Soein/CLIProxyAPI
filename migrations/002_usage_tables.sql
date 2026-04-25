-- 002_usage_tables.sql: PG-backed cluster usage statistics
--
-- 注意: 该文件仅作为文档/备用手动迁移使用。runtime 的等价 schema 已经
-- 嵌入在 internal/store/postgresstore.go 的 EnsureSchema() 中,启动 cli-proxy-api
-- 时会自动幂等创建 (CREATE TABLE IF NOT EXISTS)。
--
-- 设计要点:
--   * usage_events  : 单请求级原始行,服务详情面板与 /usage/export
--                     使用。受 EventRetentionDays (默认 7d) TTL 清理。
--   * usage_minute_rollup : 每 (分钟, 节点, api_key, model) 预聚合,服务
--                     UI 总计/趋势/sparkline/7×24 健康网格的查询。PK
--                     包含 node_id 让多节点同时 UPSERT 各自行不冲突,
--                     集群聚合在读时通过 SUM() GROUP BY 完成。受
--                     RollupRetentionDays (默认 90d) TTL 清理。
--   * dedup_hash    : SHA-256 over (apiName|modelName|timestamp|source|
--                     authIndex|failed|tokens...) — 与 internal/usage.DedupHash
--                     字节一致,用于幂等批量插入。
--
-- 与 001 一致,无独立版本表;每张表用 BIGINT 列做 audit (rollup 用
-- updated_at,events 用 inserted_at)。

-- usage_events: 单请求级原始行
CREATE TABLE IF NOT EXISTS usage_events (
    id               BIGSERIAL PRIMARY KEY,
    occurred_at      TIMESTAMPTZ NOT NULL,
    node_id          TEXT NOT NULL,
    api_key          TEXT NOT NULL,
    provider         TEXT NOT NULL DEFAULT '',
    model            TEXT NOT NULL DEFAULT 'unknown',
    source           TEXT NOT NULL DEFAULT '',
    auth_id          TEXT NOT NULL DEFAULT '',
    auth_index       TEXT NOT NULL DEFAULT '',
    auth_type        TEXT NOT NULL DEFAULT '',
    failed           BOOLEAN NOT NULL,
    latency_ms       BIGINT NOT NULL DEFAULT 0,
    input_tokens     BIGINT NOT NULL DEFAULT 0,
    output_tokens    BIGINT NOT NULL DEFAULT 0,
    reasoning_tokens BIGINT NOT NULL DEFAULT 0,
    cached_tokens    BIGINT NOT NULL DEFAULT 0,
    total_tokens     BIGINT NOT NULL DEFAULT 0,
    dedup_hash       BYTEA NOT NULL,
    inserted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_usage_events_occurred_at ON usage_events(occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_usage_events_api_model   ON usage_events(api_key, model, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_usage_events_node        ON usage_events(node_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_usage_events_source      ON usage_events(source, occurred_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_usage_events_dedup ON usage_events(dedup_hash);

-- usage_minute_rollup: 每分钟节点级聚合
CREATE TABLE IF NOT EXISTS usage_minute_rollup (
    bucket_start     TIMESTAMPTZ NOT NULL,
    node_id          TEXT NOT NULL,
    api_key          TEXT NOT NULL,
    model            TEXT NOT NULL,
    request_count    BIGINT NOT NULL DEFAULT 0,
    success_count    BIGINT NOT NULL DEFAULT 0,
    failure_count    BIGINT NOT NULL DEFAULT 0,
    input_tokens     BIGINT NOT NULL DEFAULT 0,
    output_tokens    BIGINT NOT NULL DEFAULT 0,
    reasoning_tokens BIGINT NOT NULL DEFAULT 0,
    cached_tokens    BIGINT NOT NULL DEFAULT 0,
    total_tokens     BIGINT NOT NULL DEFAULT 0,
    latency_ms_sum   BIGINT NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (bucket_start, node_id, api_key, model)
);

CREATE INDEX IF NOT EXISTS idx_usage_rollup_bucket ON usage_minute_rollup(bucket_start DESC);
CREATE INDEX IF NOT EXISTS idx_usage_rollup_api    ON usage_minute_rollup(api_key, bucket_start DESC);
CREATE INDEX IF NOT EXISTS idx_usage_rollup_model  ON usage_minute_rollup(model, bucket_start DESC);
