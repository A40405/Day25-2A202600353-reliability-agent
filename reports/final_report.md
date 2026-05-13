# Day 25 Reliability Final Report

**Student:** Bui Huu Huan

**Student ID:** 2A202600353

## 1. Architecture summary

The gateway checks cache eligibility first, then routes through per-provider circuit breakers in priority order, and returns a static fallback only when every provider is unavailable. The implementation also tracks latency, cache behavior, fallback routing, recovery time, and reproducible scenario outcomes for grading.

```text
User Request
    |
    v
[Gateway] -> [Cache check] -> hit => cached response
    |
    v
[Circuit Breaker: primary] -> Provider A
    | open/error
    v
[Circuit Breaker: backup] -> Provider B
    | open/error
    v
[Static fallback message]
```

## 2. Configuration

| Setting | Value | Why this value |
|---|---:|---|
| failure_threshold | 3 | Trips quickly enough to prevent retry storms while tolerating brief jitter. |
| reset_timeout_seconds | 2.0 | Short probe window keeps recovery visible during chaos runs without hammering a failed provider. |
| success_threshold | 1 | One successful probe is enough to re-close because providers are fake/local and recover instantly once healthy. |
| circuit breaker backend | redis | Redis shares breaker state across instances so fail-fast behavior is not process-local anymore. |
| cache TTL | 300 | Five-minute freshness fits FAQ/policy-style prompts while still expiring stale content automatically. |
| similarity_threshold | 0.92 | Set high enough to avoid date-sensitive false hits while still capturing safe paraphrases. |
| load_test requests | 200 | Large enough to exercise fallback, cache, and percentile metrics repeatedly. |
| load_test concurrency | 10 | Bonus concurrent load exposes thread-safety and routing behavior under contention. |

## 3. SLO definitions

| SLI | SLO target | Actual value | Met? |
|---|---|---:|---|
| Availability | >= 99% | 0.9950 | yes |
| Latency P95 | < 2500 ms | 311.00 | yes |
| Fallback success rate | >= 95% | 0.9600 | yes |
| Cache hit rate | >= 10% | 0.7800 | yes |
| Recovery time | < 5000 ms | 3853.077292442322 | yes |

## 4. Metrics

| Metric | Value |
|---|---:|
| total_requests | 800 |
| availability | 0.9950 |
| error_rate | 0.0050 |
| latency_p50_ms | 0.78 |
| latency_p95_ms | 311.00 |
| latency_p99_ms | 522.55 |
| fallback_success_rate | 0.9600 |
| cache_hit_rate | 0.7800 |
| estimated_cost | 0.077464 |
| estimated_cost_saved | 0.209328 |
| circuit_open_count | 2 |
| recovery_time_ms | 3853.077292442322 |
| breaker_stream_event_count | 6 |

## 5. Cache comparison

| Metric | Without cache | With cache | Delta |
|---|---:|---:|---:|
| latency_p50_ms | 274.77 | 0.78 | -99.7% |
| latency_p95_ms | 494.73 | 311.0 | -37.1% |
| estimated_cost | 0.34414 | 0.077464 | -77.5% |
| cache_hit_rate | 0.0 | 0.78 | n/a |

## 6. Redis shared cache

- Why in-memory cache is insufficient for multi-instance deployments: each process keeps its own private state, so horizontal scaling loses reuse and produces inconsistent cache hit behavior.
- How SharedRedisCache solves this: Redis centralizes entries with TTL, so separate gateway instances can read/write the same cache namespace and share warm results.

### Evidence of shared state

Instance A wrote a key, instance B read back: `shared evidence`.

### Redis CLI output

```text
rl:cache:095946136fea
rl:cache:8baa2cfa11fa
rl:cache:9e413fd814eb
rl:cache:b2a52f7dc795
```

## 6b. Redis breaker streams

Redis Streams now capture breaker transitions across instances, so OPEN, HALF_OPEN, and CLOSED events are queryable beyond a single process memory space.

### Stream evidence

```text
report-stream-probe: closed -> open (failure_threshold_reached)
report-stream-probe: open -> half_open (reset_timeout_elapsed)
report-stream-probe: half_open -> closed (probe_success)
```

## 7. Chaos scenarios

| Scenario | Expected behavior | Observed behavior | Pass/Fail |
|---|---|---|---|
| primary_timeout_100 | Primary opens quickly and backup handles nearly all successful traffic. | availability=100.00%, fallback_success_rate=100.00%, cache_hit_rate=71.50%, circuit_open_count=1, false_hits_blocked=0 | pass |
| primary_flaky_50 | Primary intermittently fails, circuit opens at least once, fallback absorbs spillover. | availability=98.00%, fallback_success_rate=90.24%, cache_hit_rate=74.00%, circuit_open_count=1, false_hits_blocked=0 | pass |
| cache_stale_candidate | Cache should serve safe repeats while blocking false hits on date-sensitive prompts. | availability=100.00%, fallback_success_rate=100.00%, cache_hit_rate=94.00%, circuit_open_count=0, false_hits_blocked=7 | pass |
| all_healthy | Requests stay on primary with no circuit openings or static fallbacks. | availability=100.00%, fallback_success_rate=0.00%, cache_hit_rate=72.50%, circuit_open_count=0, false_hits_blocked=0 | pass |

## 8. Failure analysis

A remaining production weakness is that Redis Streams are kept with a bounded max length, so very old breaker transitions will eventually be trimmed. Before production, I would forward these events into a longer-retention observability pipeline for historical forensics.

## 9. Next steps

1. Export Prometheus-style counters and gauges for request volume, latency buckets, cache hits, and circuit state.
2. Forward Redis Stream breaker events into a longer-retention sink for audit and debugging.
3. Add cost-aware routing and explicit monthly budget guardrails before allowing expensive providers.