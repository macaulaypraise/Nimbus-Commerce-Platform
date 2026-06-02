"""Structured logging configuration.

We use ``structlog`` everywhere in the application and bridge the stdlib
``logging`` module (used by uvicorn, SQLAlchemy, aiokafka) into the same
pipeline. Result: a single log line, in one format, with the same set of
context keys, regardless of which library produced it.

The format decision is driven by :class:`Settings.use_json_logs`:

  * JSON in production / staging (or when ``LOG_JSON=true``)
  * Pretty colored console in development
  * Plain key=value in test (or when ``LOG_JSON=false``)
"""

from __future__ import annotations

import logging
import sys
import uuid
from typing import Any

import structlog
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response


def configure_logging(settings: Any) -> None:
    """Configure structlog + stdlib logging. Idempotent.

    The ``settings`` parameter is typed ``Any`` to avoid a circular
    import (this module is imported by main.py, which is also where
    Settings lives). We only read public attributes:
    ``log_level`` and ``use_json_logs``.
    """
    level = getattr(logging, settings.log_level, logging.INFO)

    # Processors applied to BOTH structlog events and foreign stdlib records.
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True, key="timestamp"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.CallsiteParameterAdder(
            parameters=[
                structlog.processors.CallsiteParameter.MODULE,
                structlog.processors.CallsiteParameter.FUNC_NAME,
                structlog.processors.CallsiteParameter.LINENO,
            ],
        ),
    ]

    if settings.use_json_logs:
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer(sort_keys=True)
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stdout.isatty())

    # structlog -> stdlib LogRecord -> formatter -> stream
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    # Tame chatty third-party loggers.
    for noisy in (
        "sqlalchemy.engine.Engine",
        "aiokafka",
        "asyncio",
        "uvicorn.access",
    ):
        logging.getLogger(noisy).setLevel(max(level, logging.WARNING))


def bind_context(**kwargs: Any) -> None:
    """Bind key/value pairs to the structlog contextvars for this async task.

    Bindings are inherited by all child coroutines and cleared on the next
    request via :class:`RequestContextMiddleware`.
    """
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_context() -> None:
    """Clear all contextvars. Called at the start of each request."""
    structlog.contextvars.clear_contextvars()


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Binds a request_id to contextvars and emits start/end log lines.

    The request_id is taken from the incoming ``X-Request-ID`` header if
    present, otherwise generated with stdlib ``uuid.uuid4``. It is
    echoed back on the response so clients can correlate logs end-to-end.
    """

    HEADER = "X-Request-ID"
    _log = structlog.get_logger("nimbus.request")

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = request.headers.get(self.HEADER) or str(uuid.uuid4())
        clear_context()
        bind_context(
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            client=request.client.host if request.client else None,
        )
        self._log.info("request.start")
        try:
            response = await call_next(request)
        except Exception:
            self._log.exception("request.error")
            raise
        else:
            self._log.info("request.end", status_code=response.status_code)
        response.headers[self.HEADER] = request_id
        return response
