"""
Structured JSON logging configuration using structlog.

All workers share this setup — logs are emitted as JSON for easy
ingestion by log aggregators (ELK, Loki, CloudWatch, etc.).
"""
from __future__ import annotations

import logging
import sys

import structlog


def configure_logging(worker_name: str = "worker", level: str = "INFO") -> None:
    """
    Configure structlog with JSON rendering and standard processors.

    Args:
        worker_name: Label injected into every log line as ``worker``.
        level: Logging level string, e.g. "DEBUG", "INFO", "WARNING".
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    # stdlib root logger — catches libraries that use stdlib logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    structlog.configure(
        processors=[
            # Add log level as a string
            structlog.stdlib.add_log_level,
            # Add timestamp in ISO-8601 format
            structlog.processors.TimeStamper(fmt="iso"),
            # Render exception info
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            # Inject worker name into every event
            structlog.contextvars.merge_contextvars,
            # Final renderer: JSON
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Bind worker name to context so every log line includes it
    structlog.contextvars.bind_contextvars(worker=worker_name)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a bound structlog logger."""
    return structlog.get_logger(name)
