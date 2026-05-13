from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock
from typing import Any, Callable, TypeVar

T = TypeVar("T")


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(RuntimeError):
    """Raised when a circuit is open and calls should fail fast."""


@dataclass(slots=True)
class CircuitBreaker:
    """Circuit breaker with optional Redis-backed shared state."""

    name: str
    failure_threshold: int
    reset_timeout_seconds: float
    success_threshold: int = 1
    backend: str = "memory"
    redis_url: str | None = None
    redis_prefix: str = "rl:cb:"
    stream_enabled: bool = False
    stream_prefix: str = "rl:cb:stream:"
    stream_maxlen: int = 500
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    opened_at: float | None = None
    transition_log: list[dict[str, str | float]] = field(default_factory=list)
    half_open_in_flight: bool = False
    _lock: Lock = field(default_factory=Lock, repr=False)
    _redis: Any = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.backend != "redis" or not self.redis_url:
            self.backend = "memory"
            return
        try:
            import redis as redis_lib

            self._redis = redis_lib.Redis.from_url(self.redis_url, decode_responses=True)
            self._redis.ping()
            self._write_snapshot(self._snapshot())
        except Exception:
            self.backend = "memory"
            self._redis = None

    @property
    def key(self) -> str:
        return f"{self.redis_prefix}{self.name}"

    @property
    def stream_key(self) -> str:
        return f"{self.stream_prefix}{self.name}"

    def refresh(self) -> None:
        if self.backend == "redis":
            self._apply_snapshot(self._read_snapshot())

    def allow_request(self) -> bool:
        """Return whether a request should be attempted."""
        with self._lock:
            if self.backend == "redis":
                return self._allow_request_redis()
            return self._allow_request_local()

    def call(self, fn: Callable[..., T], *args: object, **kwargs: object) -> T:
        if not self.allow_request():
            raise CircuitOpenError(f"circuit {self.name} is open")
        try:
            result = fn(*args, **kwargs)
        except Exception:
            self.record_failure()
            raise
        self.record_success()
        return result

    def record_success(self) -> None:
        with self._lock:
            if self.backend == "redis":
                self._record_success_redis()
            else:
                self._record_success_local()

    def record_failure(self) -> None:
        with self._lock:
            if self.backend == "redis":
                self._record_failure_redis()
            else:
                self._record_failure_local()

    def reset(self) -> None:
        with self._lock:
            snapshot = self._default_snapshot()
            self._apply_snapshot(snapshot)
            if self.backend == "redis":
                self._write_snapshot(snapshot)

    def close(self) -> None:
        if self._redis is not None:
            self._redis.close()

    def delete_shared_state(self) -> None:
        if self.backend == "redis" and self._redis is not None:
            self._redis.delete(self.key)
            if self.stream_enabled:
                self._redis.delete(self.stream_key)

    def read_stream(self, count: int = 100) -> list[dict[str, str]]:
        if self._redis is None or not self.stream_enabled:
            return []
        entries = self._redis.xrange(self.stream_key, count=count)
        normalized: list[dict[str, str]] = []
        for entry_id, payload in entries:
            normalized.append({"id": str(entry_id), **{str(key): str(value) for key, value in payload.items()}})
        return normalized

    def _allow_request_local(self) -> bool:
        if self.state == CircuitState.OPEN:
            if self.opened_at is not None and time.time() - self.opened_at >= self.reset_timeout_seconds:
                self.failure_count = 0
                self.success_count = 0
                self.half_open_in_flight = False
                self._transition(CircuitState.HALF_OPEN, "reset_timeout_elapsed")
            else:
                return False
        if self.state == CircuitState.HALF_OPEN:
            if self.half_open_in_flight:
                return False
            self.half_open_in_flight = True
        return True

    def _record_success_local(self) -> None:
        self.failure_count = 0
        if self.state == CircuitState.HALF_OPEN:
            self.half_open_in_flight = False
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                self.success_count = 0
                self.opened_at = None
                self._transition(CircuitState.CLOSED, "probe_success")
        else:
            self.success_count = 0

    def _record_failure_local(self) -> None:
        self.success_count = 0
        self.half_open_in_flight = False
        if self.state == CircuitState.HALF_OPEN:
            self.failure_count = self.failure_threshold
            self.opened_at = time.time()
            self._transition(CircuitState.OPEN, "half_open_probe_failed")
            return
        self.failure_count += 1
        if self.failure_count >= self.failure_threshold:
            self.opened_at = time.time()
            self._transition(CircuitState.OPEN, "failure_threshold_reached")

    def _allow_request_redis(self) -> bool:
        snapshot = self._read_snapshot()
        now = time.time()
        if snapshot["state"] == CircuitState.OPEN.value:
            opened_at = float(snapshot["opened_at"]) if snapshot["opened_at"] is not None else None
            if opened_at is not None and now - opened_at >= self.reset_timeout_seconds:
                snapshot["failure_count"] = 0
                snapshot["success_count"] = 0
                snapshot["half_open_in_flight"] = False
                self._transition_shared(snapshot, CircuitState.HALF_OPEN, "reset_timeout_elapsed")
            else:
                self._apply_snapshot(snapshot)
                return False
        if snapshot["state"] == CircuitState.HALF_OPEN.value:
            if self._as_bool(snapshot["half_open_in_flight"]):
                self._apply_snapshot(snapshot)
                return False
            snapshot["half_open_in_flight"] = True
            self._write_snapshot(snapshot)
        self._apply_snapshot(snapshot)
        return True

    def _record_success_redis(self) -> None:
        snapshot = self._read_snapshot()
        snapshot["failure_count"] = 0
        if snapshot["state"] == CircuitState.HALF_OPEN.value:
            snapshot["half_open_in_flight"] = False
            snapshot["success_count"] = int(snapshot["success_count"]) + 1
            if int(snapshot["success_count"]) >= self.success_threshold:
                snapshot["success_count"] = 0
                snapshot["opened_at"] = None
                self._transition_shared(snapshot, CircuitState.CLOSED, "probe_success")
                return
        else:
            snapshot["success_count"] = 0
        self._write_snapshot(snapshot)
        self._apply_snapshot(snapshot)

    def _record_failure_redis(self) -> None:
        snapshot = self._read_snapshot()
        snapshot["success_count"] = 0
        snapshot["half_open_in_flight"] = False
        if snapshot["state"] == CircuitState.HALF_OPEN.value:
            snapshot["failure_count"] = self.failure_threshold
            snapshot["opened_at"] = time.time()
            self._transition_shared(snapshot, CircuitState.OPEN, "half_open_probe_failed")
            return
        snapshot["failure_count"] = int(snapshot["failure_count"]) + 1
        if int(snapshot["failure_count"]) >= self.failure_threshold:
            snapshot["opened_at"] = time.time()
            self._transition_shared(snapshot, CircuitState.OPEN, "failure_threshold_reached")
            return
        self._write_snapshot(snapshot)
        self._apply_snapshot(snapshot)

    def _transition_shared(
        self, snapshot: dict[str, str | int | float | bool | None], new_state: CircuitState, reason: str
    ) -> None:
        current_state = str(snapshot["state"])
        if current_state == new_state.value:
            self._write_snapshot(snapshot)
            self._apply_snapshot(snapshot)
            return
        self._append_transition(current_state, new_state.value, reason)
        snapshot["state"] = new_state.value
        self._write_snapshot(snapshot)
        self._apply_snapshot(snapshot)

    def _transition(self, new_state: CircuitState, reason: str) -> None:
        if self.state == new_state:
            return
        self._append_transition(self.state.value, new_state.value, reason)
        self.state = new_state

    def _append_transition(self, from_state: str, to_state: str, reason: str) -> None:
        ts = time.time()
        self.transition_log.append({"from": from_state, "to": to_state, "reason": reason, "ts": ts})
        self._write_stream_event(from_state, to_state, reason, ts)

    def _default_snapshot(self) -> dict[str, str | int | float | bool | None]:
        return {
            "state": CircuitState.CLOSED.value,
            "failure_count": 0,
            "success_count": 0,
            "opened_at": None,
            "half_open_in_flight": False,
        }

    def _snapshot(self) -> dict[str, str | int | float | bool | None]:
        return {
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "opened_at": self.opened_at,
            "half_open_in_flight": self.half_open_in_flight,
        }

    def _apply_snapshot(self, snapshot: dict[str, str | int | float | bool | None]) -> None:
        self.state = CircuitState(str(snapshot["state"]))
        self.failure_count = int(snapshot["failure_count"])
        self.success_count = int(snapshot["success_count"])
        opened_at = snapshot["opened_at"]
        self.opened_at = None if opened_at in {None, "", "None"} else float(opened_at)
        self.half_open_in_flight = self._as_bool(snapshot["half_open_in_flight"])

    def _read_snapshot(self) -> dict[str, str | int | float | bool | None]:
        if self._redis is None:
            return self._snapshot()
        raw = self._redis.hgetall(self.key)
        if not raw:
            snapshot = self._default_snapshot()
            self._write_snapshot(snapshot)
            return snapshot
        return {
            "state": raw.get("state", CircuitState.CLOSED.value),
            "failure_count": int(raw.get("failure_count", 0)),
            "success_count": int(raw.get("success_count", 0)),
            "opened_at": None if raw.get("opened_at") in {None, "", "None"} else float(raw["opened_at"]),
            "half_open_in_flight": raw.get("half_open_in_flight", "0"),
        }

    def _write_snapshot(self, snapshot: dict[str, str | int | float | bool | None]) -> None:
        if self._redis is None:
            return
        mapping = {
            "state": str(snapshot["state"]),
            "failure_count": int(snapshot["failure_count"]),
            "success_count": int(snapshot["success_count"]),
            "opened_at": "" if snapshot["opened_at"] is None else float(snapshot["opened_at"]),
            "half_open_in_flight": "1" if self._as_bool(snapshot["half_open_in_flight"]) else "0",
        }
        self._redis.hset(self.key, mapping=mapping)

    def _write_stream_event(self, from_state: str, to_state: str, reason: str, ts: float) -> None:
        if self._redis is None or not self.stream_enabled:
            return
        self._redis.xadd(
            self.stream_key,
            {
                "breaker": self.name,
                "from": from_state,
                "to": to_state,
                "reason": reason,
                "ts": f"{ts:.6f}",
            },
            maxlen=self.stream_maxlen,
            approximate=True,
        )

    @staticmethod
    def _as_bool(value: object) -> bool:
        return str(value).lower() in {"1", "true", "yes"}
