"""Kafka consumer that fans ledger events out to users' webhook endpoints.

* Offsets are committed manually, only after every delivery for the message
  has reached a terminal state, so a crash causes redelivery, not loss.
* The unique (event_id, endpoint_id) row makes redelivery harmless: an
  endpoint already marked `delivered` is skipped.
* Failed deliveries retry with exponential backoff + jitter; after
  `webhook_max_attempts` the event is parked on the DLQ topic.
* Each endpoint only receives the ledger entries for its owner's accounts.
"""

import asyncio
import json
import logging
import random
import signal
import uuid

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import start_http_server
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.config import get_settings
from app.db import SessionLocal
from app.models import DeliveryStatus, WebhookDelivery, WebhookEndpoint
from app.observability import WEBHOOK_DELIVERIES, configure_logging
from app.services.signing import sign

log = logging.getLogger("webhook_dispatcher")
settings = get_settings()


async def backoff(attempt: int) -> None:
    """Exponential backoff with full-range jitter, capped at 30s."""
    await asyncio.sleep(min(30.0, 0.5 * 2 ** (attempt - 1)) * random.uniform(0.5, 1.5))


def payload_for(event: dict, user_id: uuid.UUID) -> dict:
    data = dict(event["data"])
    data["entries"] = [
        {k: v for k, v in e.items() if k != "owner_id"}
        for e in data["entries"]
        if e.get("owner_id") == str(user_id)
    ]
    return {"id": event["id"], "type": event["type"], "created_at": event["created_at"], "data": data}


async def _claim(event_id: uuid.UUID, endpoint_id: uuid.UUID) -> WebhookDelivery | None:
    """Get-or-create the delivery row; None if it was already delivered (duplicate message)."""
    async with SessionLocal() as s:
        await s.execute(
            pg_insert(WebhookDelivery)
            .values(id=uuid.uuid4(), event_id=event_id, endpoint_id=endpoint_id)
            .on_conflict_do_nothing(constraint="uq_delivery_event_endpoint")
        )
        await s.commit()
        row = await s.scalar(
            select(WebhookDelivery).where(
                WebhookDelivery.event_id == event_id, WebhookDelivery.endpoint_id == endpoint_id
            )
        )
        return None if row.status is DeliveryStatus.delivered else row


async def _record(delivery_id: uuid.UUID, status: DeliveryStatus, attempts: int, error: str | None) -> None:
    async with SessionLocal() as s:
        row = await s.get(WebhookDelivery, delivery_id)
        row.status, row.attempts, row.last_error = status, attempts, error
        await s.commit()


async def deliver(client: httpx.AsyncClient, dlq: AIOKafkaProducer, event: dict, endpoint: WebhookEndpoint) -> None:
    delivery = await _claim(uuid.UUID(event["id"]), endpoint.id)
    if delivery is None:
        return
    body = json.dumps(payload_for(event, endpoint.user_id), separators=(",", ":")).encode()
    error = None
    for attempt in range(delivery.attempts + 1, settings.webhook_max_attempts + 1):
        try:
            resp = await client.post(
                endpoint.url,
                content=body,
                headers={
                    "Content-Type": "application/json",
                    "Ledgerly-Event-Id": event["id"],
                    "Ledgerly-Signature": sign(endpoint.secret, body),
                },
            )
            if 200 <= resp.status_code < 300:
                WEBHOOK_DELIVERIES.labels("delivered").inc()
                await _record(delivery.id, DeliveryStatus.delivered, attempt, None)
                return
            error = f"HTTP {resp.status_code}"
        except httpx.HTTPError as exc:
            error = f"{exc.__class__.__name__}: {exc}"
        WEBHOOK_DELIVERIES.labels("retry").inc()
        await _record(delivery.id, DeliveryStatus.pending, attempt, error)
        if attempt < settings.webhook_max_attempts:
            await backoff(attempt)

    WEBHOOK_DELIVERIES.labels("dead_lettered").inc()
    await _record(delivery.id, DeliveryStatus.failed, settings.webhook_max_attempts, error)
    await dlq.send_and_wait(
        settings.kafka_dlq_topic,
        json.dumps({"event": event, "endpoint_id": str(endpoint.id), "error": error}).encode(),
    )
    log.warning("webhook dead-lettered", extra={"extra_fields": {"event_id": event["id"], "endpoint": str(endpoint.id)}})


async def handle(client, dlq, event: dict) -> None:
    owners = {uuid.UUID(e["owner_id"]) for e in event["data"]["entries"] if e.get("owner_id")}
    if not owners:
        return
    async with SessionLocal() as s:
        endpoints = list(
            await s.scalars(
                select(WebhookEndpoint).where(WebhookEndpoint.user_id.in_(owners), WebhookEndpoint.active)
            )
        )
    await asyncio.gather(*(deliver(client, dlq, event, ep) for ep in endpoints))


async def main() -> None:
    configure_logging()
    start_http_server(9102)
    consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id="webhook-dispatcher",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    dlq = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap, acks="all")
    await consumer.start()
    await dlq.start()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    log.info("webhook dispatcher started")
    try:
        async with httpx.AsyncClient(timeout=settings.webhook_timeout_s) as client:
            while not stop.is_set():
                batch = await consumer.getmany(timeout_ms=1000, max_records=50)
                for _tp, messages in batch.items():
                    for msg in messages:
                        await handle(client, dlq, json.loads(msg.value))
                if batch:
                    await consumer.commit()
    finally:
        await consumer.stop()
        await dlq.stop()


if __name__ == "__main__":
    asyncio.run(main())
