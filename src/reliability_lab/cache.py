from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any

# ---------------------------------------------------------------------------
# Shared utilities - use these in both ResponseCache and SharedRedisCache
# ---------------------------------------------------------------------------

PRIVACY_PATTERNS = re.compile(
    r"\b(balance|password|credit.card|ssn|social.security|user.\d+|account.\d+)\b",
    re.IGNORECASE,
)


def _is_uncacheable(query: str) -> bool:
    """Return True if query contains privacy-sensitive keywords."""
    return bool(PRIVACY_PATTERNS.search(query))


def _looks_like_false_hit(query: str, cached_key: str) -> bool:
    """Return True if query and cached key contain different 4-digit numbers (years, IDs)."""
    nums_q = set(re.findall(r"\b\d{4}\b", query))
    nums_c = set(re.findall(r"\b\d{4}\b", cached_key))
    return bool(nums_q and nums_c and nums_q != nums_c)


# ---------------------------------------------------------------------------
# In-memory cache (existing)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class CacheEntry:
    key: str
    value: str
    created_at: float
    metadata: dict[str, str]


class ResponseCache:
    """In-memory response cache with deterministic similarity and guardrails."""

    def __init__(self, ttl_seconds: int, similarity_threshold: float):
        self.ttl_seconds = ttl_seconds
        self.similarity_threshold = similarity_threshold
        self._entries: list[CacheEntry] = []
        self.false_hit_log: list[dict[str, object]] = []
        self._lock = Lock()

    def get(self, query: str, metadata: dict[str, str] | None = None) -> tuple[str | None, float]:
        if self._should_bypass(query, metadata):
            return None, 0.0
        with self._lock:
            self._purge_expired()
            best_entry: CacheEntry | None = None
            best_score = 0.0
            normalized_query = self._normalize(query)
            for entry in self._entries:
                if normalized_query == self._normalize(entry.key):
                    return entry.value, 1.0
                score = self.similarity(query, entry.key)
                if _looks_like_false_hit(query, entry.key) and score >= 0.6:
                    self.false_hit_log.append(
                        {"query": query, "cached_query": entry.key, "score": round(score, 4)}
                    )
                if score > best_score:
                    best_score = score
                    best_entry = entry
            if best_entry is None or best_score < self.similarity_threshold:
                return None, best_score
            if _looks_like_false_hit(query, best_entry.key):
                self.false_hit_log.append(
                    {"query": query, "cached_query": best_entry.key, "score": round(best_score, 4)}
                )
                return None, best_score
            return best_entry.value, best_score

    def set(self, query: str, value: str, metadata: dict[str, str] | None = None) -> None:
        if self._should_bypass(query, metadata):
            return
        payload = metadata or {}
        with self._lock:
            self._purge_expired()
            normalized = self._normalize(query)
            self._entries = [entry for entry in self._entries if self._normalize(entry.key) != normalized]
            self._entries.append(CacheEntry(query, value, time.time(), payload))

    @staticmethod
    def similarity(a: str, b: str) -> float:
        """Deterministic token + character n-gram similarity."""
        if ResponseCache._normalize(a) == ResponseCache._normalize(b):
            return 1.0
        left = set(ResponseCache._tokenize(a))
        right = set(ResponseCache._tokenize(b))
        if not left or not right:
            return 0.0
        token_overlap = len(left & right) / len(left | right)
        char_left = ResponseCache._character_ngrams(a)
        char_right = ResponseCache._character_ngrams(b)
        if not char_left or not char_right:
            return token_overlap
        char_overlap = (2 * len(char_left & char_right)) / (len(char_left) + len(char_right))
        return round((0.7 * token_overlap) + (0.3 * char_overlap), 4)

    def _purge_expired(self) -> None:
        now = time.time()
        self._entries = [entry for entry in self._entries if now - entry.created_at <= self.ttl_seconds]

    @staticmethod
    def _normalize(value: str) -> str:
        return " ".join(ResponseCache._tokenize(value))

    @staticmethod
    def _tokenize(value: str) -> list[str]:
        tokens = re.findall(r"[a-z0-9]+", value.lower())
        stop_words = {"the", "a", "an", "is", "are", "what", "how", "why", "when", "where", "who", "do", "does", "did", "to", "of", "in", "on", "at", "by", "with", "for"}
        filtered = [t for t in tokens if t not in stop_words]
        return filtered if filtered else tokens

    @staticmethod
    def _character_ngrams(value: str, n: int = 3) -> set[str]:
        normalized = re.sub(r"\s+", " ", value.lower().strip())
        if len(normalized) < n:
            return {normalized} if normalized else set()
        return {normalized[i : i + n] for i in range(len(normalized) - n + 1)}

    @staticmethod
    def _should_bypass(query: str, metadata: dict[str, str] | None = None) -> bool:
        if _is_uncacheable(query):
            return True
        risk = (metadata or {}).get("expected_risk", "").lower()
        return risk in {"privacy", "financial", "pii", "sensitive"}


# ---------------------------------------------------------------------------
# Redis shared cache (new)
# ---------------------------------------------------------------------------


class SharedRedisCache:
    """Redis-backed shared cache for multi-instance deployments."""

    def __init__(
        self,
        redis_url: str,
        ttl_seconds: int,
        similarity_threshold: float,
        prefix: str = "rl:cache:",
    ):
        import redis as redis_lib

        self.ttl_seconds = ttl_seconds
        self.similarity_threshold = similarity_threshold
        self.prefix = prefix
        self.false_hit_log: list[dict[str, object]] = []
        self._redis: Any = redis_lib.Redis.from_url(redis_url, decode_responses=True)

    def ping(self) -> bool:
        try:
            return bool(self._redis.ping())
        except Exception:
            return False

    def get(self, query: str, metadata: dict[str, str] | None = None) -> tuple[str | None, float]:
        if ResponseCache._should_bypass(query, metadata):
            return None, 0.0
        try:
            key = f"{self.prefix}{self._query_hash(query)}"
            exact_entry = self._redis.hgetall(key)
            if exact_entry.get("response"):
                return str(exact_entry["response"]), 1.0

            best_response: str | None = None
            best_cached_query: str | None = None
            best_score = 0.0
            for cached_key in self._redis.scan_iter(f"{self.prefix}*"):
                cached_query = self._redis.hget(cached_key, "query")
                cached_response = self._redis.hget(cached_key, "response")
                if not cached_query or not cached_response:
                    continue
                score = ResponseCache.similarity(query, str(cached_query))
                if _looks_like_false_hit(query, str(cached_query)) and score >= 0.6:
                    self.false_hit_log.append(
                        {"query": query, "cached_query": str(cached_query), "score": round(score, 4)}
                    )
                if score > best_score:
                    best_score = score
                    best_response = str(cached_response)
                    best_cached_query = str(cached_query)
            if best_response is None or best_cached_query is None or best_score < self.similarity_threshold:
                return None, best_score
            if _looks_like_false_hit(query, best_cached_query):
                self.false_hit_log.append(
                    {"query": query, "cached_query": best_cached_query, "score": round(best_score, 4)}
                )
                return None, best_score
            return best_response, best_score
        except Exception:
            return None, 0.0

    def set(self, query: str, value: str, metadata: dict[str, str] | None = None) -> None:
        if ResponseCache._should_bypass(query, metadata):
            return
        try:
            key = f"{self.prefix}{self._query_hash(query)}"
            mapping: dict[str, str] = {"query": query, "response": value}
            for map_key, map_value in (metadata or {}).items():
                mapping[f"meta:{map_key}"] = map_value
            self._redis.hset(key, mapping=mapping)
            self._redis.expire(key, self.ttl_seconds)
        except Exception:
            return

    def flush(self) -> None:
        try:
            for key in self._redis.scan_iter(f"{self.prefix}*"):
                self._redis.delete(key)
        except Exception:
            return

    def close(self) -> None:
        if self._redis is not None:
            self._redis.close()

    @staticmethod
    def _query_hash(query: str) -> str:
        return hashlib.md5(query.lower().strip().encode()).hexdigest()[:12]
