from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.deps import require_admin
from app.models import OutboxEvent, WebhookDelivery
from app.schemas import ReconciliationReport
from app.services import ledger

router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(require_admin)])


@router.get("/reconciliation", response_model=ReconciliationReport)
async def reconciliation(session: AsyncSession = Depends(get_session)):
    """Re-derives every balance from the journal and checks all double-entry invariants."""
    return await ledger.reconcile(session)


@router.get("/outbox")
async def outbox_status(session: AsyncSession = Depends(get_session)):
    pending = await session.scalar(select(func.count()).where(OutboxEvent.published_at.is_(None)))
    oldest = await session.scalar(select(func.min(OutboxEvent.created_at)).where(OutboxEvent.published_at.is_(None)))
    deliveries = dict(
        (s.value, n)
        for s, n in await session.execute(
            select(WebhookDelivery.status, func.count()).group_by(WebhookDelivery.status)
        )
    )
    return {"unpublished": pending, "oldest_unpublished_at": oldest, "webhook_deliveries": deliveries}
