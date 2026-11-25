"""Outbox relay: Postgres outbox_events -> Kafka.

Delivery is at-least-once: if the process dies after Kafka acks but before the
UPDATE commits, the batch is re-sent, so consumers dedupe on event id.
FOR UPDATE SKIP LOCKED lets several relay replicas run without double-claiming rows.
"""

import asyncio
import json
import logging
import signal

from aiokafka import AIOKafkaProducer
from prometheus_client import start_http_server
from sqlalchemy import func, select, update

from app.config import get_settings
from app.db import SessionLocal
from app.models import OutboxEvent
from app.observability import OUTBOX_LAG, OUTBOX_PUBLISHED, configure_logging

log = logging.getLogger("outbox_relay")
settings = get_settings()


def serialize(event: OutboxEvent) -> bytes:
    return json.dumps(
        {
            "id": str(event.id),
            "type": event.event_type,
            "aggregate_id": str(event.aggregate_id),
            "created_at": event.created_at.isoformat(),
            "data": event.payload,
        }
    ).encode()


async def relay_once(producer) -> int:
    async with SessionLocal() as session:
        events = list(
            await session.scalars(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.created_at)
                .limit(settings.outbox_batch_size)
                .with_for_update(skip_locked=True)
            )
        )
        if events:
            # Fire the whole batch, then wait for every ack before marking published.
            futures = [
                await producer.send(settings.kafka_topic, value=serialize(e), key=str(e.aggregate_id).encode())
                for e in events
            ]
            await asyncio.gather(*futures)
            await session.execute(
                update(OutboxEvent)
                .where(OutboxEvent.id.in_([e.id for e in events]))
                .values(published_at=func.now())
            )
        await session.commit()
        OUTBOX_PUBLISHED.inc(len(events))
        OUTBOX_LAG.set(await session.scalar(select(func.count()).where(OutboxEvent.published_at.is_(None))))
        await session.commit()
        return len(events)


async def main() -> None:
    configure_logging()
    start_http_server(9101)
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap, acks="all", enable_idempotence=True)
    await producer.start()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    log.info("outbox relay started")
    try:
        while not stop.is_set():
            try:
                published = await relay_once(producer)
            except Exception:
                log.exception("relay iteration failed; backing off")
                published = 0
                await asyncio.sleep(2)
            if published:
                log.info("published batch", extra={"extra_fields": {"count": published}})
            else:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=settings.outbox_poll_interval_s)
                except TimeoutError:
                    pass
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
