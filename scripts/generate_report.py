from __future__ import annotations

import argparse
import json
from pathlib import Path

from reliability_lab.cache import SharedRedisCache
from reliability_lab.circuit_breaker import CircuitBreaker
from reliability_lab.config import load_config


def _fmt_delta(new_value: float, old_value: float) -> str:
    if old_value == 0:
        return "n/a"
    delta = ((new_value - old_value) / old_value) * 100
    return f"{delta:+.1f}%"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics", default="reports/metrics.json")
    parser.add_argument("--out", default="reports/final_report.md")
    parser.add_argument("--config", default="configs/default.yaml")
    args = parser.parse_args()
    metrics = json.loads(Path(args.metrics).read_text())
    config = load_config(args.config)
    cache_compare = metrics.get("cache_comparison", {})
    without_cache = cache_compare.get("without_cache", {})
    with_cache = cache_compare.get("with_cache", {})

    redis_cli_block = "Redis backend not enabled for this run."
    redis_shared_block = "Not collected for non-Redis runs."
    breaker_stream_block = "Breaker streams not enabled for this run."
    if config.cache.backend == "redis":
        probe_a = SharedRedisCache(config.cache.redis_url, 60, config.cache.similarity_threshold, prefix="rl:report:")
        probe_b = SharedRedisCache(config.cache.redis_url, 60, config.cache.similarity_threshold, prefix="rl:report:")
        probe_a.flush()
        probe_a.set("shared report probe", "shared evidence", {"expected_risk": "faq"})
        shared_value, _ = probe_b.get("shared report probe", {"expected_risk": "faq"})
        keys = []
        if probe_a.ping():
            keys = sorted(str(key) for key in probe_a._redis.scan_iter("rl:cache:*"))  # type: ignore[attr-defined]
        redis_shared_block = f"Instance A wrote a key, instance B read back: `{shared_value}`."
        redis_cli_block = "\n".join(keys) if keys else "(no rl:cache:* keys found during report generation)"
        probe_a.flush()
        probe_a.close()
        probe_b.close()
    if config.circuit_breaker.backend == "redis" and config.circuit_breaker.stream_enabled:
        stream_probe = CircuitBreaker(
            "report-stream-probe",
            failure_threshold=1,
            reset_timeout_seconds=1,
            backend="redis",
            redis_url=config.circuit_breaker.redis_url,
            redis_prefix=config.circuit_breaker.redis_prefix,
            stream_enabled=True,
            stream_prefix=config.circuit_breaker.stream_prefix,
            stream_maxlen=config.circuit_breaker.stream_maxlen,
        )
        stream_probe.delete_shared_state()
        stream_probe.reset()
        stream_probe.record_failure()
        assert stream_probe.allow_request() is False
        import time

        time.sleep(1.05)
        if stream_probe.allow_request():
            stream_probe.record_success()
        breaker_stream_events = stream_probe.read_stream(10)
        breaker_stream_block = "\n".join(
            f"{event['breaker']}: {event['from']} -> {event['to']} ({event['reason']})"
            for event in breaker_stream_events
        )
        stream_probe.delete_shared_state()
        stream_probe.close()

    lines = [
        "# Day 25 Reliability Final Report",
        "",
        "**Student:** Bui Huu Huan",
        "",
        "**Student ID:** 2A202600353",
        "",
        "## 1. Architecture summary",
        "",
        "The gateway checks cache eligibility first, then routes through per-provider circuit breakers in priority order, and returns a static fallback only when every provider is unavailable. The implementation also tracks latency, cache behavior, fallback routing, recovery time, and reproducible scenario outcomes for grading.",
        "",
        "```text",
        "User Request",
        "    |",
        "    v",
        "[Gateway] -> [Cache check] -> hit => cached response",
        "    |",
        "    v",
        "[Circuit Breaker: primary] -> Provider A",
        "    | open/error",
        "    v",
        "[Circuit Breaker: backup] -> Provider B",
        "    | open/error",
        "    v",
        "[Static fallback message]",
        "```",
        "",
        "## 2. Configuration",
        "",
        "| Setting | Value | Why this value |",
        "|---|---:|---|",
        f"| failure_threshold | {config.circuit_breaker.failure_threshold} | Trips quickly enough to prevent retry storms while tolerating brief jitter. |",
        f"| reset_timeout_seconds | {config.circuit_breaker.reset_timeout_seconds} | Short probe window keeps recovery visible during chaos runs without hammering a failed provider. |",
        f"| success_threshold | {config.circuit_breaker.success_threshold} | One successful probe is enough to re-close because providers are fake/local and recover instantly once healthy. |",
        f"| circuit breaker backend | {config.circuit_breaker.backend} | Redis shares breaker state across instances so fail-fast behavior is not process-local anymore. |",
        f"| cache TTL | {config.cache.ttl_seconds} | Five-minute freshness fits FAQ/policy-style prompts while still expiring stale content automatically. |",
        f"| similarity_threshold | {config.cache.similarity_threshold} | Set high enough to avoid date-sensitive false hits while still capturing safe paraphrases. |",
        f"| load_test requests | {config.load_test.requests} | Large enough to exercise fallback, cache, and percentile metrics repeatedly. |",
        f"| load_test concurrency | {config.load_test.concurrency} | Bonus concurrent load exposes thread-safety and routing behavior under contention. |",
        "",
        "## 3. SLO definitions",
        "",
        "| SLI | SLO target | Actual value | Met? |",
        "|---|---|---:|---|",
        f"| Availability | >= 99% | {metrics['availability']:.4f} | {'yes' if metrics['availability'] >= 0.99 else 'no'} |",
        f"| Latency P95 | < 2500 ms | {metrics['latency_p95_ms']:.2f} | {'yes' if metrics['latency_p95_ms'] < 2500 else 'no'} |",
        f"| Fallback success rate | >= 95% | {metrics['fallback_success_rate']:.4f} | {'yes' if metrics['fallback_success_rate'] >= 0.95 else 'no'} |",
        f"| Cache hit rate | >= 10% | {metrics['cache_hit_rate']:.4f} | {'yes' if metrics['cache_hit_rate'] >= 0.10 else 'no'} |",
        f"| Recovery time | < 5000 ms | {metrics['recovery_time_ms']} | {'yes' if (metrics['recovery_time_ms'] or 999999) < 5000 else 'no'} |",
        "",
        "## 4. Metrics",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| total_requests | {metrics['total_requests']} |",
        f"| availability | {metrics['availability']:.4f} |",
        f"| error_rate | {metrics['error_rate']:.4f} |",
        f"| latency_p50_ms | {metrics['latency_p50_ms']:.2f} |",
        f"| latency_p95_ms | {metrics['latency_p95_ms']:.2f} |",
        f"| latency_p99_ms | {metrics['latency_p99_ms']:.2f} |",
        f"| fallback_success_rate | {metrics['fallback_success_rate']:.4f} |",
        f"| cache_hit_rate | {metrics['cache_hit_rate']:.4f} |",
        f"| estimated_cost | {metrics['estimated_cost']:.6f} |",
        f"| estimated_cost_saved | {metrics['estimated_cost_saved']:.6f} |",
        f"| circuit_open_count | {metrics['circuit_open_count']} |",
        f"| recovery_time_ms | {metrics['recovery_time_ms']} |",
        f"| breaker_stream_event_count | {metrics.get('breaker_stream_event_count', 0)} |",
        "",
        "## 5. Cache comparison",
        "",
        "| Metric | Without cache | With cache | Delta |",
        "|---|---:|---:|---:|",
        f"| latency_p50_ms | {without_cache.get('latency_p50_ms', 0)} | {with_cache.get('latency_p50_ms', 0)} | {_fmt_delta(with_cache.get('latency_p50_ms', 0.0), without_cache.get('latency_p50_ms', 0.0))} |",
        f"| latency_p95_ms | {without_cache.get('latency_p95_ms', 0)} | {with_cache.get('latency_p95_ms', 0)} | {_fmt_delta(with_cache.get('latency_p95_ms', 0.0), without_cache.get('latency_p95_ms', 0.0))} |",
        f"| estimated_cost | {without_cache.get('estimated_cost', 0)} | {with_cache.get('estimated_cost', 0)} | {_fmt_delta(with_cache.get('estimated_cost', 0.0), without_cache.get('estimated_cost', 0.0))} |",
        f"| cache_hit_rate | {without_cache.get('cache_hit_rate', 0)} | {with_cache.get('cache_hit_rate', 0)} | {_fmt_delta(with_cache.get('cache_hit_rate', 0.0), without_cache.get('cache_hit_rate', 0.0))} |",
        "",
        "## 6. Redis shared cache",
        "",
        "- Why in-memory cache is insufficient for multi-instance deployments: each process keeps its own private state, so horizontal scaling loses reuse and produces inconsistent cache hit behavior.",
        "- How SharedRedisCache solves this: Redis centralizes entries with TTL, so separate gateway instances can read/write the same cache namespace and share warm results.",
        "",
        "### Evidence of shared state",
        "",
        redis_shared_block,
        "",
        "### Redis CLI output",
        "",
        "```text",
        redis_cli_block,
        "```",
        "",
        "## 6b. Redis breaker streams",
        "",
        "Redis Streams now capture breaker transitions across instances, so OPEN, HALF_OPEN, and CLOSED events are queryable beyond a single process memory space.",
        "",
        "### Stream evidence",
        "",
        "```text",
        breaker_stream_block,
        "```",
        "",
        "## 7. Chaos scenarios",
        "",
        "| Scenario | Expected behavior | Observed behavior | Pass/Fail |",
        "|---|---|---|---|",
    ]
    for name, details in metrics.get("scenario_details", {}).items():
        lines.append(
            f"| {name} | {details['expected_behavior']} | {details['observed_behavior']} | {details['status']} |"
        )
    lines += [
        "",
        "## 8. Failure analysis",
        "",
        "A remaining production weakness is that Redis Streams are kept with a bounded max length, so very old breaker transitions will eventually be trimmed. Before production, I would forward these events into a longer-retention observability pipeline for historical forensics.",
        "",
        "## 9. Next steps",
        "",
        "1. Export Prometheus-style counters and gauges for request volume, latency buckets, cache hits, and circuit state.",
        "2. Forward Redis Stream breaker events into a longer-retention sink for audit and debugging.",
        "3. Add cost-aware routing and explicit monthly budget guardrails before allowing expensive providers.",
    ]
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text("\n".join(lines))
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
