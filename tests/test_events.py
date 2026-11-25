"""Outbox relay + webhook dispatcher, with Kafka and HTTP replaced by in-memory fakes."""

import asyncio
import json
import uuid

import httpx
from sqlalchemy import func, select

from app.db import SessionLocal
from app.models import DeliveryStatus, OutboxEvent, WebhookDelivery, WebhookEndpoint
from app.services.signing import sign, verify
from tests.conftest import deposit, idem, open_account, signup
from workers import outbox_relay, webhook_dispatcher


class FakeProducer:
    def __init__(self):
        self.sent: list[tuple[str, bytes]] = []

    async def send(self, topic, value, key=None):
        self.sent.append((topic, value))
        fut = asyncio.get_running_loop().create_future()
        fut.set_result(None)
        return fut

    async def send_and_wait(self, topic, value, key=None):
        self.sent.append((topic, value))


async def test_every_posting_writes_exactly_one_outbox_event(client):
    alice = await signup(client)
    a = await open_account(client, alice)
    await deposit(client, alice, a, 10)
    await client.post("/withdrawals", json={"account_id": a, "amount": 999}, headers={**alice["headers"], **idem()})
    async with SessionLocal() as s:
        assert await s.scalar(select(func.count()).select_from(OutboxEvent)) == 1  # failed withdrawal: none


async def test_relay_publishes_once_and_marks_published(client):
    alice = await signup(client)
    a = await open_account(client, alice)
    for _ in range(3):
        await deposit(client, alice, a, 10)

    producer = FakeProducer()
    assert await outbox_relay.relay_once(producer) == 3
    assert await outbox_relay.relay_once(producer) == 0
    assert len(producer.sent) == 3
    event = json.loads(producer.sent[0][1])
    assert event["type"] == "transaction.posted"
    assert event["data"]["amount"] == 10


def test_payload_only_contains_recipients_entries():
    alice, bob = uuid.uuid4(), uuid.uuid4()
    event = {
        "id": "e1", "type": "transaction.posted", "created_at": "now",
        "data": {"amount": 5, "entries": [
            {"account_id": "A", "owner_id": str(alice), "amount": -5, "balance_after": 0},
            {"account_id": "B", "owner_id": str(bob), "amount": 5, "balance_after": 5},
        ]},
    }
    payload = webhook_dispatcher.payload_for(event, bob)
    assert payload["data"]["entries"] == [{"account_id": "B", "amount": 5, "balance_after": 5}]


async def _published_event(client) -> tuple[dict, dict, WebhookEndpoint]:
    alice = await signup(client)
    r = await client.post("/webhooks", json={"url": "https://hooks.example/x"}, headers=alice["headers"])
    assert r.status_code == 201
    a = await open_account(client, alice)
    await deposit(client, alice, a, 42)
    producer = FakeProducer()
    await outbox_relay.relay_once(producer)
    async with SessionLocal() as s:
        endpoint = await s.get(WebhookEndpoint, uuid.UUID(r.json()["id"]))
    return alice, json.loads(producer.sent[0][1]), endpoint


async def test_dispatcher_retries_signs_and_dedupes(client, monkeypatch):
    async def no_sleep(_attempt):
        return None

    monkeypatch.setattr(webhook_dispatcher, "backoff", no_sleep)
    _, event, endpoint = await _published_event(client)

    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(503 if len(calls) == 1 else 200)

    dlq = FakeProducer()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        await webhook_dispatcher.handle(http, dlq, event)
        await webhook_dispatcher.handle(http, dlq, event)  # Kafka redelivery

    assert len(calls) == 2  # one failure + one success; the redelivery was skipped
    assert verify(endpoint.secret, calls[-1].content, calls[-1].headers["ledgerly-signature"])
    assert json.loads(calls[-1].content)["data"]["entries"][0]["amount"] == 42
    async with SessionLocal() as s:
        d = await s.scalar(select(WebhookDelivery))
    assert d.status is DeliveryStatus.delivered and d.attempts == 2
    assert dlq.sent == []


async def test_dispatcher_dead_letters_after_max_attempts(client, monkeypatch):
    async def no_sleep(_attempt):
        return None

    monkeypatch.setattr(webhook_dispatcher, "backoff", no_sleep)
    _, event, _ = await _published_event(client)

    dlq = FakeProducer()
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(500))) as http:
        await webhook_dispatcher.handle(http, dlq, event)

    async with SessionLocal() as s:
        d = await s.scalar(select(WebhookDelivery))
    assert d.status is DeliveryStatus.failed
    assert d.attempts == webhook_dispatcher.settings.webhook_max_attempts
    assert len(dlq.sent) == 1 and dlq.sent[0][0] == webhook_dispatcher.settings.kafka_dlq_topic


def test_signature_verification():
    body = b'{"x":1}'
    header = sign("s3cret", body, timestamp=1_000)
    assert verify("s3cret", body, header, now=1_010)
    assert not verify("s3cret", b'{"x":2}', header, now=1_010)  # tampered body
    assert not verify("wrong", body, header, now=1_010)  # wrong secret
    assert not verify("s3cret", body, header, now=5_000)  # replay outside tolerance
    assert not verify("s3cret", body, "garbage", now=1_010)
