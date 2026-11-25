import secrets
import uuid

from fastapi import APIRouter, Depends, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.deps import Principal, current_principal, rate_limit
from app.errors import NotFound
from app.models import WebhookDelivery, WebhookEndpoint
from app.schemas import DeliveryOut, WebhookCreate, WebhookCreated, WebhookOut

router = APIRouter(prefix="/webhooks", tags=["webhooks"], dependencies=[Depends(rate_limit)])


@router.post("", response_model=WebhookCreated, status_code=status.HTTP_201_CREATED)
async def create_webhook(
    body: WebhookCreate,
    principal: Principal = Depends(current_principal),
    session: AsyncSession = Depends(get_session),
):
    endpoint = WebhookEndpoint(user_id=principal.user_id, url=str(body.url), secret=secrets.token_hex(32))
    session.add(endpoint)
    await session.commit()
    await session.refresh(endpoint)
    return endpoint


@router.get("", response_model=list[WebhookOut])
async def list_webhooks(principal: Principal = Depends(current_principal), session: AsyncSession = Depends(get_session)):
    return list(await session.scalars(select(WebhookEndpoint).where(WebhookEndpoint.user_id == principal.user_id)))


@router.delete("/{webhook_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_webhook(
    webhook_id: uuid.UUID,
    principal: Principal = Depends(current_principal),
    session: AsyncSession = Depends(get_session),
):
    endpoint = await session.get(WebhookEndpoint, webhook_id)
    if endpoint is None or endpoint.user_id != principal.user_id:
        raise NotFound("webhook not found")
    await session.delete(endpoint)
    await session.commit()


@router.get("/{webhook_id}/deliveries", response_model=list[DeliveryOut])
async def list_deliveries(
    webhook_id: uuid.UUID,
    principal: Principal = Depends(current_principal),
    session: AsyncSession = Depends(get_session),
):
    endpoint = await session.get(WebhookEndpoint, webhook_id)
    if endpoint is None or endpoint.user_id != principal.user_id:
        raise NotFound("webhook not found")
    rows = await session.scalars(
        select(WebhookDelivery)
        .where(WebhookDelivery.endpoint_id == webhook_id)
        .order_by(WebhookDelivery.created_at.desc())
        .limit(100)
    )
    return list(rows)
