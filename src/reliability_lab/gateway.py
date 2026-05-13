from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter

from reliability_lab.cache import ResponseCache, SharedRedisCache
from reliability_lab.circuit_breaker import CircuitBreaker, CircuitOpenError
from reliability_lab.providers import FakeLLMProvider, ProviderError, ProviderResponse


@dataclass(slots=True)
class GatewayResponse:
    text: str
    route: str
    provider: str | None
    cache_hit: bool
    latency_ms: float
    estimated_cost: float
    error: str | None = None


class ReliabilityGateway:
    """Routes requests through cache, circuit breakers, and fallback providers."""

    def __init__(
        self,
        providers: list[FakeLLMProvider],
        breakers: dict[str, CircuitBreaker],
        cache: ResponseCache | SharedRedisCache | None = None,
    ):
        self.providers = providers
        self.breakers = breakers
        self.cache = cache

    def complete(self, prompt: str, request_metadata: dict[str, str] | None = None) -> GatewayResponse:
        """Return a reliable response or a static fallback.

        TODO(student): Improve route reasons, cache safety checks, and error handling.
        TODO(student): Add cost budget check — if cumulative cost exceeds a threshold,
        skip expensive providers and route to cache or cheaper fallback.
        """
        started_at = perf_counter()
        if self.cache is not None:
            cached, score = self.cache.get(prompt, request_metadata)
            if cached is not None:
                latency_ms = (perf_counter() - started_at) * 1000
                return GatewayResponse(cached, f"cache_hit:{score:.2f}", None, True, latency_ms, 0.0)

        last_error: str | None = None
        route_failures: list[str] = []
        for index, provider in enumerate(self.providers):
            breaker = self.breakers[provider.name]
            try:
                response: ProviderResponse = breaker.call(provider.complete, prompt)
                if self.cache is not None:
                    cache_metadata = {"provider": provider.name}
                    if request_metadata:
                        cache_metadata.update(request_metadata)
                    self.cache.set(prompt, response.text, cache_metadata)
                route_prefix = "primary" if index == 0 else "fallback"
                route_suffix = provider.name if index == 0 else f"{provider.name}:after_{'+'.join(route_failures)}"
                total_latency_ms = (perf_counter() - started_at) * 1000
                return GatewayResponse(
                    text=response.text,
                    route=f"{route_prefix}:{route_suffix}",
                    provider=provider.name,
                    cache_hit=False,
                    latency_ms=total_latency_ms,
                    estimated_cost=response.estimated_cost,
                )
            except CircuitOpenError as exc:
                last_error = str(exc)
                route_failures.append(f"{provider.name}_open")
                continue
            except ProviderError as exc:
                last_error = str(exc)
                route_failures.append(f"{provider.name}_error")
                continue
            except Exception as exc:
                last_error = str(exc)
                route_failures.append(f"{provider.name}_unexpected")
                continue

        return GatewayResponse(
            text="The service is temporarily degraded. Please try again soon.",
            route="static_fallback:all_providers_failed",
            provider=None,
            cache_hit=False,
            latency_ms=(perf_counter() - started_at) * 1000,
            estimated_cost=0.0,
            error=last_error,
        )
