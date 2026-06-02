"""Domain exception hierarchy and FastAPI exception handlers.

Every error returned to a client has the same shape::

    {
      "error": {
        "code": "nimbus.<machine_readable_code>",
        "message": "<safe human-readable message>",
        "details": { ... optional structured context ... }
      }
    }

Stack traces are NEVER returned to clients; they are logged at the
appropriate severity. The ``safe_message`` on each exception is what
the client sees — never the raw exception args, which may contain
sensitive data.
"""

from __future__ import annotations

from typing import Any

import structlog
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

log = structlog.get_logger("nimbus.errors")


# ---------------------------------------------------------------------------
# Domain exception hierarchy
# ---------------------------------------------------------------------------


class NimbusError(Exception):
    """Base class for all Nimbus errors. All subclasses MUST set
    ``status_code`` and ``code``; ``safe_message`` should be a generic
    message that does NOT leak internal details.
    """

    status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR
    code: str = "nimbus.internal_error"
    safe_message: str = "An internal error occurred."

    def __init__(
        self,
        message: str | None = None,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message or self.safe_message)
        self.details: dict[str, Any] = details or {}


# ---- Domain (business rule) errors ----


class DomainError(NimbusError):
    """A business invariant was violated (e.g., ordering a sold-out SKU)."""

    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    code = "nimbus.domain_error"
    safe_message = "A business rule was violated."


class ResourceNotFoundError(NimbusError):
    status_code = status.HTTP_404_NOT_FOUND
    code = "nimbus.not_found"
    safe_message = "The requested resource was not found."


class ConflictError(NimbusError):
    status_code = status.HTTP_409_CONFLICT
    code = "nimbus.conflict"
    safe_message = "The request conflicts with the current state."


class IdempotencyConflictError(ConflictError):
    code = "nimbus.idempotency_conflict"
    safe_message = "A request with this idempotency key is already in progress."


class ValidationFailedError(NimbusError):
    status_code = status.HTTP_400_BAD_REQUEST
    code = "nimbus.validation_failed"
    safe_message = "Input validation failed."


# ---- Auth errors ----


class AuthenticationError(NimbusError):
    status_code = status.HTTP_401_UNAUTHORIZED
    code = "nimbus.unauthenticated"
    safe_message = "Authentication required."


class PermissionDeniedError(NimbusError):
    status_code = status.HTTP_403_FORBIDDEN
    code = "nimbus.forbidden"
    safe_message = "You do not have permission to perform this action."


class RateLimitedError(NimbusError):
    status_code = status.HTTP_429_TOO_MANY_REQUESTS
    code = "nimbus.rate_limited"
    safe_message = "Too many requests. Please retry later."


# ---- Infrastructure errors ----


class InfrastructureError(NimbusError):
    """Database, cache, message broker, or downstream service failures."""

    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    code = "nimbus.infrastructure_error"
    safe_message = "A downstream dependency is unavailable."


class DatabaseLockError(InfrastructureError):
    code = "nimbus.db_lock"
    safe_message = "The database is busy. Please retry."


class DatabaseConnectionError(InfrastructureError):
    code = "nimbus.db_connection"
    safe_message = "Cannot reach the database."


class CacheUnavailableError(InfrastructureError):
    code = "nimbus.cache_unavailable"
    safe_message = "The cache layer is unavailable."


class BrokerUnavailableError(InfrastructureError):
    code = "nimbus.broker_unavailable"
    safe_message = "The message broker is unavailable."


class TimeoutError(NimbusError):
    status_code = status.HTTP_504_GATEWAY_TIMEOUT
    code = "nimbus.timeout"
    safe_message = "The operation timed out."


# ---------------------------------------------------------------------------
# Response shape
# ---------------------------------------------------------------------------


def _error_payload(
    code: str,
    message: str,
    details: dict[str, Any] | None,
) -> dict[str, Any]:
    return {"error": {"code": code, "message": message, "details": details or {}}}


# ---------------------------------------------------------------------------
# FastAPI handler installation
# ---------------------------------------------------------------------------


def _install_handlers(app: FastAPI) -> None:
    """Register exception handlers on the given FastAPI app."""

    @app.exception_handler(NimbusError)
    async def _nimbus_error_handler(request: Request, exc: NimbusError) -> JSONResponse:
        # 5xx is genuinely unexpected; everything else is a client-facing
        # situation that we log at warning so it shows up in dashboards
        # without paging on-call.
        log_method = log.exception if exc.status_code >= 500 else log.warning
        log_method(
            "nimbus_error",
            code=exc.code,
            status_code=exc.status_code,
            path=request.url.path,
            error_type=type(exc).__name__,
            details=exc.details,
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=_error_payload(exc.code, exc.safe_message, exc.details),
            headers={"X-Error-Code": exc.code},
        )

    @app.exception_handler(RequestValidationError)
    async def _validation_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        log.info(
            "validation_error",
            path=request.url.path,
            error_count=len(exc.errors()),
        )
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=_error_payload(
                "nimbus.invalid_request",
                "The request payload failed validation.",
                {"errors": exc.errors()},
            ),
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http_exception_handler(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        message = exc.detail if isinstance(exc.detail, str) else "HTTP error."
        return JSONResponse(
            status_code=exc.status_code,
            content=_error_payload(f"http.{exc.status_code}", message, None),
        )

    @app.exception_handler(Exception)
    async def _unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        # Last-resort handler. Always log with full traceback, never leak
        # the exception text to the client.
        log.exception(
            "unhandled_exception",
            path=request.url.path,
            method=request.method,
            error_type=type(exc).__name__,
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=_error_payload(
                "nimbus.internal_error",
                "An internal error occurred.",
                None,
            ),
        )
