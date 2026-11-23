"""Race-condition tests. These are what a naive read-check-write implementation fails."""

import asyncio

from tests.conftest import balance, deposit, idem, open_account, signup


async def test_no_double_spend_under_concurrent_transfers(client):
    alice, bob = await signup(client), await signup(client)
    a, b = await open_account(client, alice), await open_account(client, bob)
    await deposit(client, alice, a, 2_000)

    async def send():
        return await client.post(
            "/transfers",
            json={"source_account_id": a, "destination_account_id": b, "amount": 100},
            headers={**alice["headers"], **idem()},
        )

    responses = await asyncio.gather(*(send() for _ in range(50)))
    codes = [r.status_code for r in responses]
    assert codes.count(201) == 20
    assert codes.count(422) == 30
    assert await balance(client, alice, a) == 0
    assert await balance(client, bob, b) == 2_000


async def test_opposing_transfers_do_not_deadlock(client):
    """A->B and B->A concurrently: without a global lock order this deadlocks."""
    admin = await signup(client, "admin@test.dev")
    alice, bob = await signup(client), await signup(client)
    a, b = await open_account(client, alice), await open_account(client, bob)
    await deposit(client, alice, a, 10_000)
    await deposit(client, bob, b, 10_000)

    def move(user, src, dst):
        return client.post(
            "/transfers",
            json={"source_account_id": src, "destination_account_id": dst, "amount": 7},
            headers={**user["headers"], **idem()},
        )

    calls = [move(alice, a, b) if i % 2 else move(bob, b, a) for i in range(60)]
    responses = await asyncio.wait_for(asyncio.gather(*calls), timeout=30)
    assert all(r.status_code == 201 for r in responses), {r.status_code for r in responses}
    assert await balance(client, alice, a) + await balance(client, bob, b) == 20_000

    report = (await client.get("/admin/reconciliation", headers=admin["headers"])).json()
    assert report["ok"], report
