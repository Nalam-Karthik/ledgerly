from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis

from app.config import get_settings
from app.db import engine
from app.errors import register_error_handlers
from app.observability import RequestContextMiddleware, configure_logging
from app.ratelimit import TokenBucketLimiter
from app.routers import accounts, admin, auth, health, payments, webhooks

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    app.state.redis = Redis.from_url(settings.redis_url, decode_responses=True)
    app.state.limiter = TokenBucketLimiter(app.state.redis, settings.rate_limit_burst, settings.rate_limit_per_s)
    yield
    await app.state.redis.aclose()
    await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(
        title="Ledgerly",
        version="1.0.0",
        description="Double-entry wallet & payments API with idempotent money movement, "
        "transactional outbox to Kafka and signed webhooks.",
        lifespan=lifespan,
    )
    app.add_middleware(RequestContextMiddleware)
    register_error_handlers(app)
    for r in (health.router, auth.router, accounts.router, payments.router, webhooks.router, admin.router):
        app.include_router(r)
    return app


app = create_app()
