"""
Async circuit breaker.

Prevents cascading failures when an external API is degraded.
Integrates transparently as an async context manager.

States:
  CLOSED   — normal operation; failures are counted.
  OPEN     — all calls rejected immediately; entered after ``failure_threshold``
              consecutive failures.  After ``recovery_timeout`` seconds the
              breaker moves to HALF_OPEN.
  HALF_OPEN — a single probe request is allowed through.  On success the
              breaker returns to CLOSED; on failure it goes back to OPEN.

Usage::

    breaker = CircuitBreaker("spotify", failure_threshold=5, recovery_timeout=120)

    try:
        async with breaker:
            result = await spotify_client._get("/tracks/...")
    except CircuitBreakerOpen:
        # Fast-fail: skip Spotify, fall back to Deezer
        result = await deezer_fallback()
"""
from __future__ import annotations

import asyncio
import time
from enum import Enum
from typing import Optional

import structlog

logger = structlog.get_logger(__name__)


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerOpen(Exception):
    """Raised when a circuit breaker is in the OPEN state."""


class CircuitBreaker:
    """
    Async circuit breaker.

    Thread safety: all state mutations are protected by an asyncio.Lock so
    concurrent coroutines cannot corrupt the state machine.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 120.0,
        max_recovery_timeout: float = 3600.0,
    ) -> None:
        """
        Args:
            name: Human-readable name used in log lines.
            failure_threshold: Consecutive failures before opening the circuit.
            recovery_timeout: Default seconds to wait in OPEN state before probing.
            max_recovery_timeout: Upper cap when caller supplies a Retry-After hint.
        """
        self.name = name
        self._threshold = failure_threshold
        self._timeout = recovery_timeout
        self._max_timeout = max_recovery_timeout
        self._active_timeout = recovery_timeout  # updated per-open based on Retry-After
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._opened_at: float = 0.0
        self._lock = asyncio.Lock()

    # ── Public API ────────────────────────────────────────────────────────────

    @property
    def state(self) -> CircuitState:
        return self._state

    async def check(self) -> None:
        """
        Raise ``CircuitBreakerOpen`` if the circuit is currently OPEN.

        Call this at the start of a guarded operation instead of using
        the context manager when you need fine-grained control over which
        exceptions count as failures (e.g., 404 should not open the circuit).
        """
        async with self._lock:
            await self._maybe_transition()
            if self._state == CircuitState.OPEN:
                raise CircuitBreakerOpen(
                    f"Circuit breaker '{self.name}' is OPEN — "
                    f"retry in {self._seconds_until_recovery():.0f}s"
                )

    async def record_success(self) -> None:
        """Record a successful call — decrements failure count, closes HALF_OPEN."""
        await self._on_success()

    async def record_failure(self, retry_after: Optional[float] = None) -> None:
        """Record a failed call — may open the circuit.

        Args:
            retry_after: Hint from the upstream API (e.g. Spotify ``Retry-After``
                header value in seconds).  When provided, the circuit stays OPEN
                for ``min(retry_after, max_recovery_timeout)`` seconds instead of
                the default ``recovery_timeout``.  This prevents probe requests
                from being sent while a known long-lived ban is in effect.
        """
        await self._on_failure(retry_after=retry_after)

    async def __aenter__(self) -> "CircuitBreaker":
        await self.check()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: object,
    ) -> bool:
        if exc_type is None:
            await self._on_success()
        else:
            await self._on_failure()
        return False  # never suppress exceptions

    # ── State machine ─────────────────────────────────────────────────────────

    async def _maybe_transition(self) -> None:
        """Check if OPEN → HALF_OPEN transition is due (call inside lock)."""
        if self._state == CircuitState.OPEN:
            if time.monotonic() - self._opened_at >= self._active_timeout:
                self._state = CircuitState.HALF_OPEN
                logger.info(
                    "circuit_breaker_half_open",
                    name=self.name,
                    failures=self._failures,
                )

    async def _on_success(self) -> None:
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                # Probe succeeded — fully recover
                self._state = CircuitState.CLOSED
                self._failures = 0
                logger.info("circuit_breaker_closed", name=self.name)
            elif self._state == CircuitState.CLOSED:
                # Gradually forgive old failures
                self._failures = max(0, self._failures - 1)

    async def _on_failure(self, retry_after: Optional[float] = None) -> None:
        async with self._lock:
            self._failures += 1
            if self._state in (CircuitState.CLOSED, CircuitState.HALF_OPEN):
                if self._failures >= self._threshold or self._state == CircuitState.HALF_OPEN:
                    self._state = CircuitState.OPEN
                    self._opened_at = time.monotonic()
                    if retry_after is not None:
                        self._active_timeout = min(retry_after, self._max_timeout)
                    else:
                        self._active_timeout = self._timeout
                    logger.warning(
                        "circuit_breaker_opened",
                        name=self.name,
                        failures=self._failures,
                        recovery_in_seconds=self._active_timeout,
                    )

    def _seconds_until_recovery(self) -> float:
        elapsed = time.monotonic() - self._opened_at
        return max(0.0, self._active_timeout - elapsed)
