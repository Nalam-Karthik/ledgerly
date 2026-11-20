from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.observability import request_id_ctx


class AppError(Exception):
    status_code = 400
    code = "bad_request"

    def __init__(self, detail: str, *, headers: dict[str, str] | None = None):
        super().__init__(detail)
        self.detail = detail
        self.headers = headers


class Unauthorized(AppError):
    status_code = 401
    code = "unauthorized"


class Forbidden(AppError):
    status_code = 403
    code = "forbidden"


class NotFound(AppError):
    status_code = 404
    code = "not_found"


class Conflict(AppError):
    status_code = 409
    code = "conflict"


class InsufficientFunds(AppError):
    status_code = 422
    code = "insufficient_funds"


class IdempotencyMismatch(AppError):
    status_code = 422
    code = "idempotency_key_reused"


class RateLimited(AppError):
    status_code = 429
    code = "rate_limited"


def error_body(code: str, detail: str) -> dict:
    return {"error": {"code": code, "detail": detail, "request_id": request_id_ctx.get()}}


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def _handle(_: Request, exc: AppError) -> JSONResponse:
        return JSONResponse(error_body(exc.code, exc.detail), status_code=exc.status_code, headers=exc.headers)
