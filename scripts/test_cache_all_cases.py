"""
test_cache_all_cases.py
-----------------------
Đọc test cases từ data/cache_test_data.jsonl và chạy kiểm tra cache trên Redis.
Luôn cần Docker Redis đang chạy (make docker-up).

Chạy lệnh:
    # Unit Redis tests (tự SET rồi GET)
    python scripts/test_cache_all_cases.py

    # Unit + Integration (chạy run_chaos trước rồi test GET thật)
    python scripts/test_cache_all_cases.py --with-chaos

    # Dùng Redis đang có sẵn, bỏ qua bước chạy chaos
    python scripts/test_cache_all_cases.py --with-chaos --no-run-chaos
"""
from __future__ import annotations

import sys
import json
import argparse
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

from reliability_lab.cache import ResponseCache, SharedRedisCache

# ─────────────────────────────────────────────
# Màu sắc
# ─────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(msg: str)   -> str: return f"{GREEN}[PASS]{RESET} {msg}"
def fail(msg: str) -> str: return f"{RED}[FAIL]{RESET} {msg}"
def info(msg: str) -> str: return f"{CYAN}[INFO]{RESET} {msg}"
def warn(msg: str) -> str: return f"{YELLOW}[SKIP]{RESET} {msg}"
def header(title: str) -> None:
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{BOLD}{'='*60}{RESET}\n")

# ─────────────────────────────────────────────
# Load test cases
# ─────────────────────────────────────────────
DEFAULT_DATA = ROOT / "data" / "cache_test_data.jsonl"

def load_test_cases(path: Path) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            cases.append(json.loads(line))
    return cases

# ─────────────────────────────────────────────
# Chạy run_chaos để populate Redis
# ─────────────────────────────────────────────
def run_chaos_populate(config_path: str = "configs/default.yaml") -> None:
    from reliability_lab.chaos import load_queries, run_simulation
    from reliability_lab.config import load_config

    print(info("Đang chạy run_chaos để populate Redis cache..."))
    config = load_config(config_path)
    if not config.cache.enabled or config.cache.backend != "redis":
        print(f"{YELLOW}[WARN]{RESET} Cache backend không phải redis. Chuyển sang redis tạm thời.")
        config.cache.enabled = True
        config.cache.backend = "redis"
    queries = load_queries()
    metrics = run_simulation(config, queries)
    print(info(
        f"run_chaos xong: {metrics.total_requests} requests, "
        f"cache_hit_rate={metrics.cache_hit_rate:.1%}, "
        f"circuit_open_count={metrics.circuit_open_count}"
    ))
    print()

# ─────────────────────────────────────────────
# Unit Redis test (tự SET rồi GET trên Redis)
# ─────────────────────────────────────────────
def run_unit_redis_case(tc: dict[str, Any], cache: SharedRedisCache) -> bool:
    if tc.get("ttl_override") is not None:
        print(f"  {BOLD}{tc['id']}{RESET}  {warn('TTL test bo qua voi Redis')}\n")
        return True
    # flush() chỉ gọi 1 lần trước khi chạy tất cả unit tests (xem main())
    cache.set(tc["stored_query"], f"mock::{tc['stored_query']}", tc["metadata"])
    cached_val, score = cache.get(tc["lookup_query"], tc["metadata"])
    hit = cached_val is not None
    passed = hit == tc["expect_hit"]

    status = ok(f"hit={hit}") if passed else fail(f"hit={hit}, expected={tc['expect_hit']}")
    print(f"  {BOLD}{tc['id']}{RESET}  {tc['name']}")
    print(f"         stored : {tc['stored_query']!r}")
    print(f"         lookup : {tc['lookup_query']!r}")
    print(f"         score  : {score:.4f}")
    print(f"         {status}")
    if tc.get("note"):
        print(f"         {info(tc['note'])}")
    print()
    return passed

# ─────────────────────────────────────────────
# Integration test (chỉ GET — data do chaos set)
# ─────────────────────────────────────────────
def run_integration_case(tc: dict[str, Any], cache: SharedRedisCache) -> bool:
    cached_val, score = cache.get(tc["lookup_query"], tc["metadata"])
    hit = cached_val is not None
    passed = hit == tc["expect_hit"]

    status = ok(f"hit={hit}") if passed else fail(f"hit={hit}, expected={tc['expect_hit']}")
    print(f"  {BOLD}{tc['id']}{RESET}  {tc['name']}")
    print(f"         lookup : {tc['lookup_query']!r}")
    print(f"         score  : {score:.4f}")
    if cached_val:
        preview = cached_val[:80] + "..." if len(cached_val) > 80 else cached_val
        print(f"         result : {preview!r}")
    print(f"         {status}")
    if tc.get("note"):
        print(f"         {info(tc['note'])}")
    print()
    return passed

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Cache test runner tu JSONL (luon dung Redis)")
    parser.add_argument("--data",         default=str(DEFAULT_DATA))
    parser.add_argument("--with-chaos",   action="store_true",  help="Chay run_chaos roi test GET that tu Redis")
    parser.add_argument("--no-run-chaos", action="store_true",  help="Bo qua buoc chay chaos, dung Redis dang co san")
    parser.add_argument("--threshold",    type=float, default=0.85)
    parser.add_argument("--config",       default="configs/default.yaml")
    args = parser.parse_args()

    data_path = Path(args.data)
    if not data_path.exists():
        print(f"{RED}[LOI] Khong tim thay: {data_path}{RESET}")
        sys.exit(1)

    all_cases         = load_test_cases(data_path)
    unit_cases        = [tc for tc in all_cases if not tc.get("integration_only")]
    integration_cases = [tc for tc in all_cases if tc.get("integration_only")]
    threshold: float  = args.threshold

    print(f"\n{info(f'Doc {len(all_cases)} test cases tu: {data_path.name}')}")
    print(info(f"  Unit cases: {len(unit_cases)} | Integration cases: {len(integration_cases)}"))

    # Kết nối Redis — bắt buộc
    redis_cache = SharedRedisCache("redis://localhost:6379/0", 300, threshold)
    if not redis_cache.ping():
        print(f"\n{RED}[LOI] Khong ket noi duoc Redis. Chay 'make docker-up' truoc.{RESET}\n")
        sys.exit(1)
    print(info("Ket noi Redis thanh cong.\n"))

    # ── 1. Unit Redis (tự SET rồi GET) ──────────────────────────────────
    header(f"UNIT - REDIS CACHE  (threshold={threshold})")
    redis_cache.flush()
    print(info("Da flush Redis, bat dau chay unit tests..."))
    print()
    unit_results = [run_unit_redis_case(tc, redis_cache) for tc in unit_cases]
    _print_summary("Redis unit", unit_results, unit_cases)

    # Hiển thị keys đang có trong Redis sau unit tests
    try:
        keys = list(redis_cache._redis.scan_iter("rl:cache:*"))
        print(info(f"Redis sau unit tests: {len(keys)} key(s) dang ton tai:"))
        for k in keys:
            q = redis_cache._redis.hget(k, "query") or "(unknown)"
            print(f"  {k}  ->  {q!r}")
        print()
    except Exception:
        pass

    # ── 2. Integration (--with-chaos) ───────────────────────────────────
    if args.with_chaos:
        if not args.no_run_chaos:
            run_chaos_populate(args.config)
        else:
            print(info("Bo qua buoc run_chaos - dung data Redis dang co san.\n"))

        header("INTEGRATION - GET tu Redis data that (do run_chaos populate)")
        try:
            keys = list(redis_cache._redis.scan_iter("rl:cache:*"))
            print(info(f"Redis hien co {len(keys)} cache key(s): {keys[:5]}{'...' if len(keys) > 5 else ''}"))
            print()
        except Exception:
            pass

        integ_results = [run_integration_case(tc, redis_cache) for tc in integration_cases]
        _print_summary("Integration chaos", integ_results, integration_cases)

    print()


def _print_summary(label: str, results: list[bool], cases: list[dict[str, Any]]) -> None:
    passed = sum(results)
    total  = len(results)
    color  = GREEN if passed == total else RED
    print(f"{color}{BOLD}{label}: {passed}/{total} passed{RESET}")
    if passed < total:
        ids = [cases[i]["id"] for i, r in enumerate(results) if not r]
        print(f"{RED}  Failed: {', '.join(ids)}{RESET}")
    print()


if __name__ == "__main__":
    main()
