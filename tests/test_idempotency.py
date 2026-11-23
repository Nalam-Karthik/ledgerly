import asyncio
import uuid

from tests.conftest import balance, deposit, open_account, signup


async def test_retry_with_same_key_replays_without_moving_money_twice(client):
    alice = await signup(client)
    a = await open_account(client, alice)
    headers = {**alice["headers"], "Idempotency-Key": str(uuid.uuid4())}
    body = {"account_id": a, "amount": 700}

    r1 = await client.post("/deposits", json=body, headers=headers)
    r2 = await client.post("/deposits", json=body, headers=headers)
    assert r1.status_code == r2.status_code == 201
    assert r1.json() == r2.json()
    assert r1.headers["idempotent-replayed"] == "false"
    assert r2.headers["idempotent-replayed"] == "true"
    assert await balance(client, alice, a) == 700


async def test_same_key_different_body_rejected(client):
    alice = await signup(client)
    a = await open_account(client, alice)
    headers = {**alice["headers"], "Idempotency-Key": str(uuid.uuid4())}
    assert (await client.post("/deposits", json={"account_id": a, "amount": 1}, headers=headers)).status_code == 201
    r = await client.post("/deposits", json={"account_id": a, "amount": 2}, headers=headers)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "idempotency_key_reused"


async def test_keys_are_scoped_per_user(client):
    alice, bob = await signup(client), await signup(client)
    a, b = await open_account(client, alice), await open_account(client, bob)
    key = {"Idempotency-Key": "shared-key-123"}
    r = await client.post("/deposits", json={"account_id": a, "amount": 5}, headers={**alice["headers"], **key})
    assert r.status_code == 201
    r = await client.post("/deposits", json={"account_id": b, "amount": 9}, headers={**bob["headers"], **key})
    assert r.status_code == 201
    assert await balance(client, bob, b) == 9


async def test_missing_key_rejected(client):
    alice = await signup(client)
    a = await open_account(client, alice)
    r = await client.post("/deposits", json={"account_id": a, "amount": 5}, headers=alice["headers"])
    assert r.status_code == 422


async def test_concurrent_duplicates_post_exactly_once(client):
    """20 identical requests in flight at once (e.g. a client retry storm) -> one posting."""
    alice, bob = await signup(client), await signup(client)
    a, b = await open_account(client, alice), await open_account(client, bob)
    await deposit(client, alice, a, 1_000)

    headers = {**alice["headers"], "Idempotency-Key": str(uuid.uuid4())}
    body = {"source_account_id": a, "destination_account_id": b, "amount": 600}
    responses = await asyncio.gather(*(client.post("/transfers", json=body, headers=headers) for _ in range(20)))

    assert all(r.status_code == 201 for r in responses), [r.text for r in responses if r.status_code != 201]
    assert len({r.json()["id"] for r in responses}) == 1
    assert sum(r.headers["idempotent-replayed"] == "false" for r in responses) == 1
    assert await balance(client, alice, a) == 400
    assert await balance(client, bob, b) == 600


async def test_failed_request_can_be_retried_with_same_key(client):
    alice, bob = await signup(client), await signup(client)
    a, b = await open_account(client, alice), await open_account(client, bob)
    headers = {**alice["headers"], "Idempotency-Key": str(uuid.uuid4())}
    body = {"source_account_id": a, "destination_account_id": b, "amount": 50}

    assert (await client.post("/transfers", json=body, headers=headers)).status_code == 422
    await deposit(client, alice, a, 50)
    assert (await client.post("/transfers", json=body, headers=headers)).status_code == 201
