"""
POSIX signal handling for graceful shutdown.

Sets a shared ``shutdown_event`` when SIGTERM or SIGINT is received.
Workers poll this event in their main loop to exit cleanly after the
current batch completes — no work is lost mid-batch.
"""
from __future__ import annotations

import asyncio
import signal
import structlog

logger = structlog.get_logger(__name__)

# Module-level shutdown flag — workers import and poll this
shutdown_event = asyncio.Event()


def install_signal_handlers() -> None:
    """
    Register SIGTERM and SIGINT handlers to set ``shutdown_event``.

    Must be called from the async main loop (after the loop is running),
    because ``add_signal_handler`` requires an active event loop.
    """
    loop = asyncio.get_running_loop()

    def _handle(sig_name: str) -> None:
        logger.info("shutdown_signal_received", signal=sig_name)
        shutdown_event.set()

    loop.add_signal_handler(signal.SIGTERM, lambda: _handle("SIGTERM"))
    loop.add_signal_handler(signal.SIGINT,  lambda: _handle("SIGINT"))
