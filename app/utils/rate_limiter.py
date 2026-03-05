"""
Async token-bucket rate limiter.

Used to enforce per-second request limits for Spotify and Genius APIs
without blocking the event loop. The bucket refills at ``rate`` tokens
per second up to ``capacity`` tokens.

Example:
    limiter = RateLimiter(rate=10.0, capacity=10.0)
    await limiter.acquire()  # wait if necessary, then proceed
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional


class RateLimiter:
    """
    Token-bucket rate limiter for async code.

    Thread safety: not needed — asyncio is single-threaded.
    The ``_lock`` prevents concurrent ``acquire()`` calls from racing.
    """

    def __init__(self, rate: float, capacity: Optional[float] = None) -> None:
        """
        Args:
            rate: Maximum requests per second (token refill rate).
            capacity: Bucket size in tokens. Defaults to ``rate`` (1-second burst).
        """
        self._rate = rate
        self._capacity = capacity if capacity is not None else rate
        self._tokens: float = self._capacity
        self._last_refill: float = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        """Add tokens proportional to elapsed time since last call."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._last_refill = now
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)

    async def acquire(self, tokens: float = 1.0) -> None:
        """
        Wait until ``tokens`` are available and consume them.

        Args:
            tokens: Number of tokens to consume (default 1 per request).
        """
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return

                # Reserve the deficit: drain bucket and sleep until replenished.
                # Release the lock during sleep so other waiters can also compute
                # their wait time and queue up correctly.
                deficit = tokens - self._tokens
                wait_time = deficit / self._rate
                self._tokens = 0.0

            # Sleep outside the lock. After waking, re-acquire the lock and
            # re-check: another waiter may have drained the bucket again.
            await asyncio.sleep(wait_time)

    @property
    def available_tokens(self) -> float:
        """Current token count (approximate, not thread-safe for inspection)."""
        self._refill()
        return self._tokens
