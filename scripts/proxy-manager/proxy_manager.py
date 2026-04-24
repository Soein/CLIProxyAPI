#!/usr/bin/env python3
"""
proxy_manager.py - Assign / unassign per-auth SOCKS5 proxy URLs in CPA's PG store.

Mirrors CPA's Weighted Rendezvous Hashing ring (SHA-256) to compute the owner
node for each auth. Picks the next free port within that node's microsocks
pool (1080-1109) and writes it into the auth's JSONB content.proxy_url.

Usage:
    proxy_manager.py list [--json]
    proxy_manager.py status <auth_id>
    proxy_manager.py assign <auth_id> [--port PORT] [--dry-run]
    proxy_manager.py unassign <auth_id> [--dry-run]
    proxy_manager.py assign-all [--filter codex|gemini|...] [--dry-run]

Environment:
    PGSTORE_DSN            libpq connection string (required)
    CPA_PROXY_PORT_MIN     default 1080
    CPA_PROXY_PORT_MAX     default 1109
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from typing import Iterable

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 required. Install with: apt install python3-psycopg2", file=sys.stderr)
    sys.exit(2)


# ---------- Ring (mirror of sdk/cliproxy/cluster/ring.go) ----------

MAX_UINT64 = (1 << 64) - 1


def weighted_score(node_id: str, weight: int, auth_id: str) -> float:
    """Replicates Go's weightedScore: score = weight / -ln(U)."""
    h = hashlib.sha256()
    h.update(node_id.encode("utf-8"))
    h.update(b"|")
    h.update(auth_id.encode("utf-8"))
    hash_val = int.from_bytes(h.digest()[:8], "big")
    u = (hash_val + 1.0) / (MAX_UINT64 + 2.0)
    return weight / -math.log(u)


def pick_owner(auth_id: str, members: list[tuple[str, int]]) -> str | None:
    """Picks the winning node_id. members = [(node_id, weight), ...]."""
    if not members:
        return None
    best_id: str | None = None
    best_score = -1.0
    for node_id, weight in sorted(members, key=lambda m: m[0]):
        w = weight if weight > 0 else 100
        score = weighted_score(node_id, w, auth_id)
        if score > best_score or (score == best_score and (best_id is None or node_id < best_id)):
            best_id, best_score = node_id, score
    return best_id


# ---------- PG helpers ----------


def pg_connect():
    dsn = os.environ.get("PGSTORE_DSN")
    if not dsn:
        print("ERROR: PGSTORE_DSN env var not set", file=sys.stderr)
        sys.exit(2)
    return psycopg2.connect(dsn)


def fetch_nodes(cur) -> list[tuple[str, int]]:
    """Active cluster_nodes (mirrors how CPA RingWatcher filters)."""
    cur.execute(
        "SELECT node_id, COALESCE(weight, 100) "
        "FROM cluster_nodes "
        "WHERE status = 'active' "
        "  AND last_heartbeat > NOW() - INTERVAL '60 seconds'"
    )
    return [(row[0], row[1]) for row in cur.fetchall()]


def fetch_all_auths(cur) -> list[tuple[str, dict]]:
    cur.execute("SELECT id, content FROM auth_store ORDER BY id")
    rows = []
    for auth_id, content in cur.fetchall():
        obj = content if isinstance(content, dict) else json.loads(content)
        rows.append((auth_id, obj))
    return rows


def fetch_auth(cur, auth_id: str) -> dict | None:
    cur.execute("SELECT content FROM auth_store WHERE id = %s", (auth_id,))
    row = cur.fetchone()
    if not row:
        return None
    return row[0] if isinstance(row[0], dict) else json.loads(row[0])


# ---------- Port allocation ----------


def port_range() -> range:
    lo = int(os.environ.get("CPA_PROXY_PORT_MIN", "1080"))
    hi = int(os.environ.get("CPA_PROXY_PORT_MAX", "1109"))
    return range(lo, hi + 1)


def extract_port(proxy_url: str) -> int | None:
    """From 'socks5://127.0.0.1:1088' returns 1088. Returns None if unparseable."""
    if not proxy_url or "://" not in proxy_url:
        return None
    try:
        after_scheme = proxy_url.split("://", 1)[1]
        host_port = after_scheme.split("/", 1)[0].split("@")[-1]
        port_str = host_port.rsplit(":", 1)[1]
        return int(port_str)
    except (IndexError, ValueError):
        return None


def used_ports_on_node(auths: list[tuple[str, dict]], nodes: list[tuple[str, int]], target_node: str) -> set[int]:
    """Collect ports already assigned to auths owned by target_node."""
    used: set[int] = set()
    for auth_id, content in auths:
        owner = pick_owner(auth_id, nodes)
        if owner != target_node:
            continue
        proxy_url = (content.get("proxy_url") or "").strip()
        port = extract_port(proxy_url)
        # Only count local-loopback ports (avoid counting arbitrary external proxies).
        if port and ("127.0.0.1" in proxy_url or "localhost" in proxy_url):
            used.add(port)
    return used


def pick_free_port(used: set[int]) -> int:
    for p in port_range():
        if p not in used:
            return p
    raise RuntimeError(
        f"no free port in range {port_range()}: all {len(used)} ports used. "
        "Expand pool (e.g. 1080-1124) via expand-pool.sh."
    )


# ---------- Commands ----------


def cmd_list(args) -> int:
    with pg_connect() as conn, conn.cursor() as cur:
        nodes = fetch_nodes(cur)
        auths = fetch_all_auths(cur)
    if not nodes:
        print("WARN: no active cluster_nodes (is CPA running?). Listing without owner.", file=sys.stderr)

    rows = []
    for auth_id, content in auths:
        owner = pick_owner(auth_id, nodes) if nodes else "?"
        proxy_url = (content.get("proxy_url") or "").strip()
        provider = content.get("type", "?")
        rows.append({
            "id": auth_id,
            "owner": owner,
            "provider": provider,
            "proxy_url": proxy_url or "(shared default)",
        })

    if getattr(args, "json", False):
        print(json.dumps(rows, indent=2, ensure_ascii=False))
        return 0

    # Text table grouped by owner
    by_owner: dict[str, list[dict]] = {}
    for r in rows:
        by_owner.setdefault(r["owner"], []).append(r)
    for owner in sorted(by_owner):
        group = by_owner[owner]
        print(f"\n[{owner}]  ({len(group)} auths)")
        for r in sorted(group, key=lambda x: (x["provider"], x["id"])):
            print(f"  {r['provider']:<12} {r['id']:<50} {r['proxy_url']}")
    print()
    return 0


def cmd_status(args) -> int:
    with pg_connect() as conn, conn.cursor() as cur:
        nodes = fetch_nodes(cur)
        content = fetch_auth(cur, args.auth_id)
    if content is None:
        print(f"auth not found: {args.auth_id}", file=sys.stderr)
        return 1
    owner = pick_owner(args.auth_id, nodes) if nodes else "?"
    proxy_url = (content.get("proxy_url") or "").strip() or "(shared default)"
    print(f"id        : {args.auth_id}")
    print(f"owner     : {owner}")
    print(f"provider  : {content.get('type', '?')}")
    print(f"proxy_url : {proxy_url}")
    return 0


def cmd_assign(args) -> int:
    return _assign_one(args.auth_id, explicit_port=args.port, dry_run=args.dry_run)


def _assign_one(auth_id: str, explicit_port: int | None = None, dry_run: bool = False) -> int:
    with pg_connect() as conn:
        with conn.cursor() as cur:
            nodes = fetch_nodes(cur)
            if not nodes:
                print("ERROR: no active cluster_nodes; refusing to assign (owner unknown).", file=sys.stderr)
                return 2
            content = fetch_auth(cur, auth_id)
            if content is None:
                print(f"auth not found: {auth_id}", file=sys.stderr)
                return 1
            owner = pick_owner(auth_id, nodes)
            all_auths = fetch_all_auths(cur)
            used = used_ports_on_node(all_auths, nodes, owner)

            # Don't re-count the auth's own port if re-assigning.
            current_proxy = (content.get("proxy_url") or "").strip()
            current_port = extract_port(current_proxy)
            if current_port:
                used.discard(current_port)

            if explicit_port is not None:
                if explicit_port in used:
                    print(f"ERROR: port {explicit_port} already used on {owner}", file=sys.stderr)
                    return 1
                if explicit_port not in port_range():
                    print(f"ERROR: port {explicit_port} outside pool range {port_range()}", file=sys.stderr)
                    return 1
                port = explicit_port
            else:
                try:
                    port = pick_free_port(used)
                except RuntimeError as e:
                    print(f"ERROR: {e}", file=sys.stderr)
                    return 1

            new_proxy = f"socks5://127.0.0.1:{port}"
            print(f"auth_id    : {auth_id}")
            print(f"ring owner : {owner}")
            print(f"old proxy  : {current_proxy or '(shared default)'}")
            print(f"new proxy  : {new_proxy}")

            if dry_run:
                print("(dry-run; no changes)")
                return 0

            cur.execute(
                "UPDATE auth_store "
                "SET content = jsonb_set(content, '{proxy_url}', to_jsonb(%s::text), true), "
                "    version = version + 1, "
                "    updated_at = NOW() "
                "WHERE id = %s",
                (new_proxy, auth_id),
            )
            if cur.rowcount != 1:
                print(f"ERROR: UPDATE affected {cur.rowcount} rows (expected 1). Rolled back.", file=sys.stderr)
                conn.rollback()
                return 1
        conn.commit()
    print("OK — CPA watcher will pick up the change via PG NOTIFY.")
    return 0


def cmd_unassign(args) -> int:
    with pg_connect() as conn:
        with conn.cursor() as cur:
            content = fetch_auth(cur, args.auth_id)
            if content is None:
                print(f"auth not found: {args.auth_id}", file=sys.stderr)
                return 1
            current_proxy = (content.get("proxy_url") or "").strip()
            print(f"auth_id   : {args.auth_id}")
            print(f"old proxy : {current_proxy or '(already shared default)'}")
            print(f"new proxy : (shared default)")
            if args.dry_run:
                print("(dry-run; no changes)")
                return 0
            if not current_proxy:
                print("already unassigned, nothing to do.")
                return 0
            cur.execute(
                "UPDATE auth_store "
                "SET content = content - 'proxy_url', "
                "    version = version + 1, "
                "    updated_at = NOW() "
                "WHERE id = %s",
                (args.auth_id,),
            )
            if cur.rowcount != 1:
                conn.rollback()
                return 1
        conn.commit()
    print("OK — CPA watcher will pick up the change via PG NOTIFY.")
    return 0


def cmd_assign_all(args) -> int:
    """Batch assign: pre-compute whole plan so dry-run shows distinct ports,
    and real writes are serialized under a single transaction for atomicity."""
    with pg_connect() as conn, conn.cursor() as cur:
        nodes = fetch_nodes(cur)
        auths = fetch_all_auths(cur)
    if not nodes:
        print("ERROR: no active cluster_nodes.", file=sys.stderr)
        return 2

    # Seed per-node "used" from ALL existing proxy_url (both assigned & about to skip).
    per_node_used: dict[str, set[int]] = {}
    for auth_id, content in auths:
        owner = pick_owner(auth_id, nodes)
        if not owner:
            continue
        port = extract_port((content.get("proxy_url") or "").strip())
        if port and "127.0.0.1" in (content.get("proxy_url") or ""):
            per_node_used.setdefault(owner, set()).add(port)

    # Build plan for all unassigned targets matching filter.
    plan: list[tuple[str, str, int]] = []  # (auth_id, owner, port)
    for auth_id, content in auths:
        if args.filter and content.get("type", "") != args.filter:
            continue
        if (content.get("proxy_url") or "").strip():
            continue
        owner = pick_owner(auth_id, nodes)
        if not owner:
            print(f"  SKIP {auth_id}: no ring owner", file=sys.stderr)
            continue
        used = per_node_used.setdefault(owner, set())
        try:
            port = pick_free_port(used)
        except RuntimeError as e:
            print(f"  SKIP {auth_id} on {owner}: {e}", file=sys.stderr)
            continue
        used.add(port)  # reserve within plan
        plan.append((auth_id, owner, port))

    print(f"plan: {len(plan)} assignments (filter={args.filter or 'none'})")
    for auth_id, owner, port in plan:
        print(f"  {owner:<8} {auth_id:<60} → socks5://127.0.0.1:{port}")

    if args.dry_run:
        print("\n(dry-run; no changes)")
        return 0

    # Execute the whole plan in one transaction.
    with pg_connect() as conn:
        with conn.cursor() as cur:
            for auth_id, owner, port in plan:
                new_proxy = f"socks5://127.0.0.1:{port}"
                cur.execute(
                    "UPDATE auth_store "
                    "SET content = jsonb_set(content, '{proxy_url}', to_jsonb(%s::text), true), "
                    "    version = version + 1, "
                    "    updated_at = NOW() "
                    "WHERE id = %s",
                    (new_proxy, auth_id),
                )
                if cur.rowcount != 1:
                    print(f"ERROR: {auth_id} UPDATE affected {cur.rowcount} rows. Rolled back.", file=sys.stderr)
                    conn.rollback()
                    return 1
        conn.commit()
    print(f"OK — {len(plan)} auths assigned. CPA watchers will reload via PG NOTIFY.")
    return 0


# ---------- CLI ----------


def main(argv: Iterable[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    p_list = sub.add_parser("list", help="list all auths grouped by ring owner")
    p_list.add_argument("--json", action="store_true")
    p_list.set_defaults(func=cmd_list)

    p_status = sub.add_parser("status", help="show single auth's owner + proxy_url")
    p_status.add_argument("auth_id")
    p_status.set_defaults(func=cmd_status)

    p_assign = sub.add_parser("assign", help="assign free proxy port to auth")
    p_assign.add_argument("auth_id")
    p_assign.add_argument("--port", type=int, help="force specific port")
    p_assign.add_argument("--dry-run", action="store_true")
    p_assign.set_defaults(func=cmd_assign)

    p_unassign = sub.add_parser("unassign", help="remove per-auth proxy_url (revert to shared)")
    p_unassign.add_argument("auth_id")
    p_unassign.add_argument("--dry-run", action="store_true")
    p_unassign.set_defaults(func=cmd_unassign)

    p_all = sub.add_parser("assign-all", help="assign to all unassigned auths (bulk)")
    p_all.add_argument("--filter", help="only provider==FILTER (e.g. codex)")
    p_all.add_argument("--dry-run", action="store_true")
    p_all.set_defaults(func=cmd_assign_all)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
