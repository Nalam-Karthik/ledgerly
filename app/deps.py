import uuid
from dataclasses import dataclass

from fastapi import Depends, Request, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from redis.asyncio import Redis

from app.errors import Forbidden, RateLimited, Unauthorized
from app.models import Role
from app.observability import RATE_LIMITED
from app.ratelimit import TokenBucketLimiter
from app.security import decode_token

_bearer = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class Principal:
    user_id: uuid.UUID
    role: Role


def get_redis(request: Request) -> Redis:
    return request.app.state.redis


def get_limiter(request: Request) -> TokenBucketLimiter:
    return request.app.state.limiter


async def current_principal(creds: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> Principal:
    # Stateless check: the access token is short-lived, so no DB hit per request.
    if creds is None:
        raise Unauthorized("missing bearer token", headers={"WWW-Authenticate": "Bearer"})
    claims = decode_token(creds.credentials, "access")
    return Principal(user_id=uuid.UUID(claims["sub"]), role=Role(claims["role"]))


async def require_admin(principal: Principal = Depends(current_principal)) -> Principal:
    if principal.role is not Role.admin:
        raise Forbidden("admin role required")
    return principal


async def rate_limit(
    request: Request,
    response: Response,
    creds: HTTPAuthorizationCredentials | None = Depends(_bearer),
    limiter: TokenBucketLimiter = Depends(get_limiter),
) -> None:
    # Key by user when authenticated, else by client IP.
    key = request.client.host if request.client else "anon"
    if creds is not None:
        try:
            key = f"user:{decode_token(creds.credentials, 'access')['sub']}"
        except Unauthorized:
            pass
    allowed, remaining, retry_after = await limiter.hit(key)
    headers = {"X-RateLimit-Limit": str(limiter.capacity), "X-RateLimit-Remaining": str(remaining)}
    if not allowed:
        RATE_LIMITED.inc()
        headers["Retry-After"] = str(max(1, round(retry_after)))
        raise RateLimited("too many requests", headers=headers)
    response.headers.update(headers)
