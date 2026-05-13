import time

from reliability_lab.circuit_breaker import CircuitBreaker, CircuitState


def test_circuit_breaker_reopens_then_recovers_after_probe() -> None:
    breaker = CircuitBreaker("primary", failure_threshold=2, reset_timeout_seconds=0.01)

    breaker.record_failure()
    assert breaker.state == CircuitState.CLOSED

    breaker.record_failure()
    assert breaker.state == CircuitState.OPEN
    assert breaker.allow_request() is False

    time.sleep(0.02)
    assert breaker.allow_request() is True
    assert breaker.state == CircuitState.HALF_OPEN

    breaker.record_success()
    assert breaker.state == CircuitState.CLOSED
    assert [entry["to"] for entry in breaker.transition_log] == ["open", "half_open", "closed"]


def test_shared_redis_circuit_breaker_state() -> None:
    try:
        import redis as redis_lib

        probe = redis_lib.Redis.from_url("redis://localhost:6379/0")
        probe.ping()
        probe.close()
    except Exception:
        return

    b1 = CircuitBreaker(
        "shared-primary",
        failure_threshold=2,
        reset_timeout_seconds=0.01,
        backend="redis",
        redis_url="redis://localhost:6379/0",
        redis_prefix="rl:test:cb:",
        stream_enabled=True,
        stream_prefix="rl:test:cb:stream:",
    )
    b2 = CircuitBreaker(
        "shared-primary",
        failure_threshold=2,
        reset_timeout_seconds=0.01,
        backend="redis",
        redis_url="redis://localhost:6379/0",
        redis_prefix="rl:test:cb:",
        stream_enabled=True,
        stream_prefix="rl:test:cb:stream:",
    )
    b1.delete_shared_state()
    b1.reset()

    b1.record_failure()
    b1.record_failure()
    b2.refresh()
    assert b2.state == CircuitState.OPEN

    time.sleep(0.02)
    assert b2.allow_request() is True
    b1.refresh()
    assert b1.state == CircuitState.HALF_OPEN

    b1.record_success()
    b2.refresh()
    assert b2.state == CircuitState.CLOSED

    b1.delete_shared_state()
    b1.close()
    b2.close()


def test_redis_streams_capture_breaker_transitions() -> None:
    try:
        import redis as redis_lib

        probe = redis_lib.Redis.from_url("redis://localhost:6379/0")
        probe.ping()
        probe.close()
    except Exception:
        return

    breaker = CircuitBreaker(
        "stream-primary",
        failure_threshold=1,
        reset_timeout_seconds=0.01,
        backend="redis",
        redis_url="redis://localhost:6379/0",
        redis_prefix="rl:test:cb:",
        stream_enabled=True,
        stream_prefix="rl:test:cb:stream:",
    )
    breaker.delete_shared_state()
    breaker.reset()
    breaker.record_failure()
    time.sleep(0.02)
    assert breaker.allow_request() is True
    breaker.record_success()

    events = breaker.read_stream()
    assert len(events) >= 3
    assert events[0]["to"] == "open"
    assert events[-1]["to"] == "closed"

    breaker.delete_shared_state()
    breaker.close()
