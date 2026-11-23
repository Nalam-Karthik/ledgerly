from tests.conftest import balance, deposit, idem, open_account, signup


async def test_deposit_withdraw_transfer(client):
    alice, bob = await signup(client), await signup(client)
    a, b = await open_account(client, alice), await open_account(client, bob)

    await deposit(client, alice, a, 10_000)
    r = await client.post(
        "/transfers",
        json={"source_account_id": a, "destination_account_id": b, "amount": 2_500},
        headers={**alice["headers"], **idem()},
    )
    assert r.status_code == 201, r.text
    assert r.json()["kind"] == "transfer"

    r = await client.post("/withdrawals", json={"account_id": b, "amount": 500}, headers={**bob["headers"], **idem()})
    assert r.status_code == 201

    assert await balance(client, alice, a) == 7_500
    assert await balance(client, bob, b) == 2_000


async def test_insufficient_funds_writes_nothing(client):
    alice, bob = await signup(client), await signup(client)
    a, b = await open_account(client, alice), await open_account(client, bob)
    await deposit(client, alice, a, 100)

    r = await client.post(
        "/transfers",
        json={"source_account_id": a, "destination_account_id": b, "amount": 101},
        headers={**alice["headers"], **idem()},
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "insufficient_funds"
    assert await balance(client, alice, a) == 100
    entries = (await client.get(f"/accounts/{a}/entries", headers=alice["headers"])).json()["items"]
    assert len(entries) == 1  # just the deposit


async def test_cannot_move_money_from_someone_elses_account(client):
    alice, mallory = await signup(client), await signup(client)
    a, m = await open_account(client, alice), await open_account(client, mallory)
    await deposit(client, alice, a, 1_000)

    r = await client.post(
        "/transfers",
        json={"source_account_id": a, "destination_account_id": m, "amount": 1_000},
        headers={**mallory["headers"], **idem()},
    )
    assert r.status_code == 404
    assert (await client.get(f"/accounts/{a}", headers=mallory["headers"])).status_code == 404
    assert await balance(client, alice, a) == 1_000


async def test_currency_mismatch_rejected(client):
    alice = await signup(client)
    usd, eur = await open_account(client, alice, "USD"), await open_account(client, alice, "EUR")
    await deposit(client, alice, usd, 1_000)
    r = await client.post(
        "/transfers",
        json={"source_account_id": usd, "destination_account_id": eur, "amount": 10},
        headers={**alice["headers"], **idem()},
    )
    assert r.status_code == 409


async def test_validation(client):
    alice = await signup(client)
    a = await open_account(client, alice)
    bad = [
        {"account_id": a, "amount": 0},
        {"account_id": a, "amount": -5},
        {"account_id": "not-a-uuid", "amount": 5},
    ]
    for body in bad:
        r = await client.post("/deposits", json=body, headers={**alice["headers"], **idem()})
        assert r.status_code == 422, body
    r = await client.post(
        "/transfers",
        json={"source_account_id": a, "destination_account_id": a, "amount": 1},
        headers={**alice["headers"], **idem()},
    )
    assert r.status_code == 422


async def test_entries_keyset_pagination(client):
    alice = await signup(client)
    a = await open_account(client, alice)
    for i in range(1, 8):
        await deposit(client, alice, a, i)

    seen, cursor = [], None
    while True:
        params = {"limit": 3, **({"cursor": cursor} if cursor else {})}
        page = (await client.get(f"/accounts/{a}/entries", params=params, headers=alice["headers"])).json()
        seen += [e["amount"] for e in page["items"]]
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert seen == [7, 6, 5, 4, 3, 2, 1]
    # balance_after on the newest entry equals the running total.
    first = (await client.get(f"/accounts/{a}/entries", params={"limit": 1}, headers=alice["headers"])).json()
    assert first["items"][0]["balance_after"] == sum(range(1, 8))


async def test_reconciliation_holds_after_activity(client):
    admin = await signup(client, "admin@test.dev")
    alice, bob = await signup(client), await signup(client)
    a, b = await open_account(client, alice), await open_account(client, bob)
    await deposit(client, alice, a, 5_000)
    await client.post(
        "/transfers",
        json={"source_account_id": a, "destination_account_id": b, "amount": 1_234},
        headers={**alice["headers"], **idem()},
    )
    report = (await client.get("/admin/reconciliation", headers=admin["headers"])).json()
    assert report["ok"] is True
    assert report["currency_totals"] == {"USD": 0}
    assert report["balance_mismatches"] == [] and report["unbalanced_transactions"] == []
