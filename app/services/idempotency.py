"""Exactly-once semantics for money-moving endpoints (Stripe-style Idempotency-Key).

Within one database transaction we:
  1. take a transaction-scoped advisory lock on (user, key), so concurrent
     requests carrying the same key are serialized,
  2. look the key up and replay the stored response if present,
  3. otherwise run the handler and insert the key row alongside the ledger
     writes, then commit (which also releases the lock).

Because the key row commits atomically with the money movement, a crash can
never leave "money moved but key not recorded" or vice versa. A request with
a reused key but a different body is rejected with 422. Only successful
outcomes are stored, so a request that failed (e.g. insufficient funds) can
be retried with the same key once the cause is fixed.
"""

import hashlib
import json
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.errors import IdempotencyMismatch
from app.models import IdempotencyKey
from app.observability import IDEMPOTENT_REPLAYS

settings = get_settings()


def fingerprint(route: str, payload: dict) -> str:
    canonical = json.dumps({"route": route, "body": payload}, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


async def run_idempotent(
    session: AsyncSession,
    *,
    user_id: uuid.UUID,
    key: str,
    request_hash: str,
    handler: Callable[[], Awaitable[dict]],
    status_code: int = 201,
) -> tuple[int, dict, bool]:
    """Returns (status_code, body, replayed)."""
    try:
        await session.execute(select(func.pg_advisory_xact_lock(func.hashtextextended(f"{user_id}:{key}", 0))))

        row = await session.scalar(
            select(IdempotencyKey).where(IdempotencyKey.user_id == user_id, IdempotencyKey.key == key)
        )
        if row is not None:
            expired = row.created_at < datetime.now(UTC) - timedelta(seconds=settings.idempotency_ttl_s)
            if not expired:
                if row.request_hash != request_hash:
                    raise IdempotencyMismatch("Idempotency-Key was already used with a different request")
                IDEMPOTENT_REPLAYS.inc()
                result = (row.status_code, row.response_body, True)
                await session.commit()
                return result
            await session.execute(
                delete(IdempotencyKey).where(IdempotencyKey.user_id == user_id, IdempotencyKey.key == key)
            )

        body = await handler()
        session.add(
            IdempotencyKey(
                user_id=user_id, key=key, request_hash=request_hash, status_code=status_code, response_body=body
            )
        )
        await session.commit()
        return status_code, body, False
    except BaseException:
        await session.rollback()
        raise
