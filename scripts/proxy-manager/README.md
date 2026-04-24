# proxy-manager

按需给 CPA 账号分配独立 SOCKS5 代理端口（对应独立 IPv6 出口）。

零 CPA 代码改动；通过 PG `auth_store.content->>'proxy_url'` JSONB 字段动态控制。

## 安装

在 SJ（或任一能连 PG 的 CPA 节点）上：

```bash
apt install -y python3-psycopg2
mkdir -p /opt/proxy-manager
cp proxy_manager.py /opt/proxy-manager/
chmod +x /opt/proxy-manager/proxy_manager.py
ln -s /opt/proxy-manager/proxy_manager.py /usr/local/bin/pm
```

## 使用

```bash
# 需要 PGSTORE_DSN 环境变量 (或在 /etc/default/proxy-manager 里配)
export PGSTORE_DSN='postgres://postgres:PASSWORD@127.0.0.1:5000/cliproxy?sslmode=require'

# 看全量状态
pm list

# 看单个账号
pm status codex-54d5e498-xxx@wsaic.com-team

# 给账号分配独立 v6（自动算 ring 归属 + 挑空闲端口）
pm assign codex-54d5e498-xxx@wsaic.com-team

# 强制指定端口
pm assign codex-xxx --port 1095

# 试跑不落库
pm assign codex-xxx --dry-run

# 回到共享默认
pm unassign codex-xxx

# 批量给所有还没分配的账号分配
pm assign-all --dry-run              # 预览
pm assign-all                        # 执行
pm assign-all --filter codex         # 只给 codex

# JSON 输出方便脚本处理
pm list --json | jq '.[] | select(.owner=="fra-01")'
```

## 工作原理

1. 查 `cluster_nodes` 拿到活跃节点（last_heartbeat < 60s）
2. 用 CPA 相同的加权 Rendezvous Hashing (SHA-256) 算 `auth_id` 归属哪个 node
3. 查该 node 上已被占用的端口（扫所有归属该 node 的 auth_store 记录）
4. 从 1080-1109 挑第一个空闲端口
5. `UPDATE auth_store SET content = jsonb_set(content, '{proxy_url}', ...)`
6. CPA 的 PG watcher 自动热加载（`ReloadByID`）

## 什么时候跑？

**不是每次上传都要跑**。只在需要给账号独立 v6 时跑：
- 重要账号（Team plan / 付费订阅）
- 被怀疑共享 IP 触发风控的账号
- 刚换号的敏感 provider 账号

不跑时账号就走全局默认 `socks5://127.0.0.1:1081`（和其他未分配账号共享一个 v6）。

## 与扩容脚本的关系

当某个 node 的端口池用满时（默认 1080-1109 共 30 个），`pm assign` 会报错
"no free port in range"。此时用 `expand-pool.sh <node> <new-size>` 扩到 45/60 个。

## 限制

- 算法依赖 `cluster_nodes` 的 weight，和 CPA 的 ring 完全一致——**只要 CPA 升级改算法，这个脚本也要同步**。
- 只识别 `socks5://127.0.0.1:PORT` 格式的本地端口，其他协议/外部 IP 会被忽略（不纳入占用统计）。
- 不是分布式锁：两人同时跑 assign 有竞争（极小概率撞端口），建议单人操作或用 `flock` 锁文件。
