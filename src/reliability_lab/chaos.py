from __future__ import annotations

import copy
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Lock

from reliability_lab.cache import ResponseCache, SharedRedisCache
from reliability_lab.circuit_breaker import CircuitBreaker
from reliability_lab.config import LabConfig, ScenarioConfig
from reliability_lab.gateway import ReliabilityGateway
from reliability_lab.metrics import RunMetrics
from reliability_lab.providers import FakeLLMProvider


@dataclass(frozen=True, slots=True)
class QuerySample:
    id: str
    query: str
    expected_risk: str


def load_queries(path: str | Path = "data/sample_queries.jsonl") -> list[QuerySample]:
    queries: list[QuerySample] = []
    for line in Path(path).read_text().splitlines():
        if not line.strip():
            continue
        raw = json.loads(line)
        queries.append(
            QuerySample(
                id=str(raw["id"]),
                query=str(raw["query"]),
                expected_risk=str(raw.get("expected_risk", "unknown")),
            )
        )
    return queries


def build_gateway(config: LabConfig, provider_overrides: dict[str, float] | None = None) -> ReliabilityGateway:
    providers = []
    for p in config.providers:
        fail_rate = provider_overrides.get(p.name, p.fail_rate) if provider_overrides else p.fail_rate
        providers.append(FakeLLMProvider(p.name, fail_rate, p.base_latency_ms, p.cost_per_1k_tokens))
    breakers = {
        p.name: CircuitBreaker(
            name=p.name,
            failure_threshold=config.circuit_breaker.failure_threshold,
            reset_timeout_seconds=config.circuit_breaker.reset_timeout_seconds,
            success_threshold=config.circuit_breaker.success_threshold,
            backend=config.circuit_breaker.backend,
            redis_url=config.circuit_breaker.redis_url,
            redis_prefix=config.circuit_breaker.redis_prefix,
            stream_enabled=config.circuit_breaker.stream_enabled,
            stream_prefix=config.circuit_breaker.stream_prefix,
            stream_maxlen=config.circuit_breaker.stream_maxlen,
        )
        for p in config.providers
    }
    cache: ResponseCache | SharedRedisCache | None = None
    if config.cache.enabled:
        if config.cache.backend == "redis":
            redis_cache = SharedRedisCache(
                config.cache.redis_url,
                config.cache.ttl_seconds,
                config.cache.similarity_threshold,
            )
            cache = redis_cache if redis_cache.ping() else ResponseCache(
                config.cache.ttl_seconds, config.cache.similarity_threshold
            )
        else:
            cache = ResponseCache(config.cache.ttl_seconds, config.cache.similarity_threshold)
    return ReliabilityGateway(providers, breakers, cache)


def calculate_recovery_time_ms(gateway: ReliabilityGateway) -> float | None:
    """Derive recovery time from circuit breaker transition logs.

    Recovery time = time between circuit opening and next successful close.
    Returns the average recovery time across all breakers, or None if no recovery occurred.
    """
    recovery_times: list[float] = []
    for breaker in gateway.breakers.values():
        open_ts: float | None = None
        for entry in breaker.transition_log:
            if entry["to"] == "open" and open_ts is None:
                open_ts = entry["ts"]
            elif entry["to"] == "closed" and open_ts is not None:
                recovery_times.append((float(entry["ts"]) - open_ts) * 1000)
                open_ts = None
    if not recovery_times:
        return None
    return sum(recovery_times) / len(recovery_times)


def _estimate_saved_cost(prompt: str, providers: list[FakeLLMProvider]) -> float:
    cheapest = min((provider.cost_per_1k_tokens for provider in providers), default=0.0)
    estimated_tokens = max(1, len(prompt.split())) + 48
    return (estimated_tokens / 1000.0) * cheapest


def _scenario_expectation(scenario: ScenarioConfig) -> str:
    expectations = {
        "primary_timeout_100": "Primary opens quickly and backup handles nearly all successful traffic.",
        "primary_flaky_50": "Primary intermittently fails, circuit opens at least once, fallback absorbs spillover.",
        "all_healthy": "Requests stay on primary with no circuit openings or static fallbacks.",
        "cache_stale_candidate": "Cache should serve safe repeats while blocking false hits on date-sensitive prompts.",
    }
    return expectations.get(scenario.name, scenario.description or "Scenario should meet its configured goal.")


def _scenario_observed(metrics: RunMetrics) -> str:
    return (
        f"availability={metrics.availability:.2%}, fallback_success_rate={metrics.fallback_success_rate:.2%}, "
        f"cache_hit_rate={metrics.cache_hit_rate:.2%}, circuit_open_count={metrics.circuit_open_count}, "
        f"false_hits_blocked={metrics.false_hit_count}"
    )


def _scenario_passed(metrics: RunMetrics, scenario: ScenarioConfig) -> bool:
    if scenario.name == "primary_timeout_100":
        return metrics.fallback_successes > 0 and metrics.static_fallbacks == 0 and metrics.circuit_open_count >= 1
    if scenario.name == "primary_flaky_50":
        return (
            metrics.circuit_open_count >= 1
            and metrics.fallback_successes > 0
            and metrics.availability >= 0.95
            and metrics.fallback_success_rate >= 0.9
        )
    if scenario.name == "all_healthy":
        return metrics.static_fallbacks == 0 and metrics.circuit_open_count == 0
    if scenario.name == "cache_stale_candidate":
        return metrics.cache_hit_rate > 0 and metrics.false_hit_count >= 1
    return metrics.availability > 0.0


def run_scenario(config: LabConfig, queries: list[QuerySample], scenario: ScenarioConfig) -> RunMetrics:
    """Run a single named chaos scenario."""
    gateway = build_gateway(config, scenario.provider_overrides or None)
    metrics = RunMetrics(cache_backend=config.cache.backend, concurrency=config.load_test.concurrency)
    metrics_lock = Lock()
    for breaker in gateway.breakers.values():
        if breaker.backend == "redis":
            breaker.delete_shared_state()
            breaker.reset()
    if isinstance(gateway.cache, SharedRedisCache):
        gateway.cache.flush()
    if scenario.name == "cache_stale_candidate" and gateway.cache is not None:
        gateway.cache.set(
            "Summarize refund policy for 2024 deadline",
            "Cached 2024 refund policy summary",
            {"expected_risk": "policy"},
        )
        gateway.cache.get(
            "Summarize refund policy for 2026 deadline",
            {"expected_risk": "policy"},
        )
    request_count = config.load_test.requests
    rng = random.Random(config.load_test.random_seed + sum(ord(char) for char in scenario.name))
    if scenario.name == "cache_stale_candidate":
        canned = [
            QuerySample(id="refund-2024", query="Summarize refund policy for 2024 deadline", expected_risk="policy"),
            QuerySample(id="refund-2026", query="Summarize refund policy for 2026 deadline", expected_risk="policy"),
            QuerySample(id="faq-1", query="Summarize the admission FAQ in 5 bullets.", expected_risk="faq"),
            QuerySample(id="faq-2", query="Summarize the admission FAQ in 5 bullets.", expected_risk="faq"),
        ]
        selected_queries = [canned[index % len(canned)] for index in range(request_count)]
    else:
        selected_queries = [rng.choice(queries) for _ in range(request_count)]

    def execute(sample: QuerySample) -> None:
        result = gateway.complete(sample.query, {"expected_risk": sample.expected_risk, "query_id": sample.id})
        with metrics_lock:
            metrics.total_requests += 1
            metrics.estimated_cost += result.estimated_cost
            if result.cache_hit:
                metrics.cache_hits += 1
                metrics.estimated_cost_saved += _estimate_saved_cost(sample.query, gateway.providers)
            route_family = result.route.split(":", 1)[0]
            metrics.route_counts[result.route] = metrics.route_counts.get(result.route, 0) + 1
            if route_family == "fallback":
                metrics.fallback_successes += 1
                metrics.successful_requests += 1
            elif route_family == "static_fallback":
                metrics.static_fallbacks += 1
                metrics.failed_requests += 1
            else:
                metrics.successful_requests += 1
            metrics.latencies_ms.append(result.latency_ms)

    if config.load_test.concurrency > 1:
        with ThreadPoolExecutor(max_workers=config.load_test.concurrency) as executor:
            list(executor.map(execute, selected_queries))
    else:
        for sample in selected_queries:
            execute(sample)

    if any(breaker.state.value == "open" for breaker in gateway.breakers.values()):
        time.sleep(config.circuit_breaker.reset_timeout_seconds + 0.05)
        for provider in gateway.providers:
            provider.fail_rate = 0.0
        gateway.complete("recovery probe for metrics", {"expected_risk": "technical"})

    metrics.circuit_open_count = sum(
        1 for breaker in gateway.breakers.values() for t in breaker.transition_log if t["to"] == "open"
    )
    metrics.recovery_time_ms = calculate_recovery_time_ms(gateway)
    breaker_stream_events: list[str] = []
    for breaker in gateway.breakers.values():
        for event in breaker.read_stream():
            breaker_stream_events.append(
                f"{event['breaker']}:{event['from']}->{event['to']} ({event['reason']})"
            )
    metrics.breaker_stream_event_count = len(breaker_stream_events)
    metrics.breaker_stream_examples = breaker_stream_events[:5]
    if gateway.cache is not None:
        metrics.false_hit_count = len(getattr(gateway.cache, "false_hit_log", []))
        metrics.false_hit_examples = [
            f"{entry['query']} -> {entry['cached_query']} ({entry['score']})"
            for entry in getattr(gateway.cache, "false_hit_log", [])[:3]
        ]
    return metrics


def _merge_metrics(target: RunMetrics, result: RunMetrics) -> None:
    target.total_requests += result.total_requests
    target.successful_requests += result.successful_requests
    target.failed_requests += result.failed_requests
    target.fallback_successes += result.fallback_successes
    target.static_fallbacks += result.static_fallbacks
    target.cache_hits += result.cache_hits
    target.circuit_open_count += result.circuit_open_count
    target.estimated_cost += result.estimated_cost
    target.estimated_cost_saved += result.estimated_cost_saved
    target.latencies_ms.extend(result.latencies_ms)
    target.false_hit_count += result.false_hit_count
    target.false_hit_examples.extend(result.false_hit_examples)
    target.breaker_stream_event_count += result.breaker_stream_event_count
    target.breaker_stream_examples.extend(result.breaker_stream_examples)
    for route, count in result.route_counts.items():
        target.route_counts[route] = target.route_counts.get(route, 0) + count
    if result.recovery_time_ms is not None:
        if target.recovery_time_ms is None:
            target.recovery_time_ms = result.recovery_time_ms
        else:
            target.recovery_time_ms = (target.recovery_time_ms + result.recovery_time_ms) / 2


def _run_scenarios(config: LabConfig, queries: list[QuerySample]) -> RunMetrics:
    combined = RunMetrics(cache_backend=config.cache.backend, concurrency=config.load_test.concurrency)
    scenarios = config.scenarios or [ScenarioConfig(name="default", description="baseline run")]
    for scenario in scenarios:
        result = run_scenario(config, queries, scenario)
        passed = _scenario_passed(result, scenario)
        combined.scenarios[scenario.name] = "pass" if passed else "fail"
        combined.scenario_details[scenario.name] = {
            "description": scenario.description,
            "expected_behavior": _scenario_expectation(scenario),
            "observed_behavior": _scenario_observed(result),
            "status": combined.scenarios[scenario.name],
        }
        _merge_metrics(combined, result)
    return combined


def _build_cache_comparison(config: LabConfig, queries: list[QuerySample], existing: RunMetrics) -> dict[str, dict[str, float]]:
    without_cache = copy.deepcopy(config)
    without_cache.cache.enabled = False
    without_cache.cache.backend = "memory"
    no_cache_metrics = _run_scenarios(without_cache, queries)
    with_cache_metrics = existing
    return {
        "without_cache": {
            "latency_p50_ms": round(no_cache_metrics.percentile(50), 2),
            "latency_p95_ms": round(no_cache_metrics.percentile(95), 2),
            "estimated_cost": round(no_cache_metrics.estimated_cost, 6),
            "cache_hit_rate": round(no_cache_metrics.cache_hit_rate, 4),
        },
        "with_cache": {
            "latency_p50_ms": round(with_cache_metrics.percentile(50), 2),
            "latency_p95_ms": round(with_cache_metrics.percentile(95), 2),
            "estimated_cost": round(with_cache_metrics.estimated_cost, 6),
            "cache_hit_rate": round(with_cache_metrics.cache_hit_rate, 4),
        },
    }


def run_simulation(config: LabConfig, queries: list[QuerySample]) -> RunMetrics:
    """Run all named scenarios from config, or a default run if none defined.

    TODO(student): Add a cache vs no-cache comparison scenario.
    Extend with your own custom scenarios (e.g., cost cap near limit).
    """
    combined = _run_scenarios(config, queries)
    if config.cache.enabled:
        combined.cache_comparison = _build_cache_comparison(config, queries, combined)
    return combined
