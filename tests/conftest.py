import os
import uuid

# Must be set before the app (and its settings/engine) is imported.
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://ledgerly:ledgerly@localhost:5432/ledgerly_test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/15")
os.environ.setdefault("RATE_LIMIT_BURST", "100000")
os.environ.setdefault("ADMIN_EMAILS", '["admin@test.dev"]')

import asyncpg  # noqa: E402
import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy import text  # noqa: E402

from app.db import engine  # noqa: E402
from app.main import app  # noqa: E402
from app.models import Base  # noqa: E402

PASSWORD = "correct-horse-battery"


def pytest_collection_modifyitems(items):
    # One event loop for the whole run so the shared asyncpg pool stays valid.
    for item in items:
        if pytest_asyncio.is_async_test(item):
            item.add_marker(pytest.mark.asyncio(loop_scope="session"), append=False)


@pytest_asyncio.fixture(scope="session", autouse=True)
async def _database():
    url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
    base, dbname = url.rsplit("/", 1)
    conn = await asyncpg.connect(f"{base}/postgres")
    if not await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", dbname):
        await conn.execute(f'CREATE DATABASE "{dbname}"')
    await conn.close()

    async with engine.begin() as c:
        await c.run_sync(Base.metadata.drop_all)
        await c.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()


@pytest_asyncio.fixture(scope="session")
async def _app():
    async with app.router.lifespan_context(app):
        yield app


@pytest_asyncio.fixture(autouse=True)
async def _clean(_app):
    tables = ", ".join(t.name for t in Base.metadata.sorted_tables)
    async with engine.begin() as c:
        await c.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))
    await _app.state.redis.flushdb()
    yield


@pytest_asyncio.fixture
async def client(_app):
    async with AsyncClient(transport=ASGITransport(app=_app), base_url="http://test") as c:
        yield c


async def signup(client: AsyncClient, email: str | None = None) -> dict:
    email = email or f"u-{uuid.uuid4().hex[:8]}@test.dev"
    r = await client.post("/auth/register", json={"email": email, "password": PASSWORD})
    assert r.status_code == 201, r.text
    r = await client.post("/auth/login", json={"email": email, "password": PASSWORD})
    assert r.status_code == 200, r.text
    tokens = r.json()
    return {"headers": {"Authorization": f"Bearer {tokens['access_token']}"}, **tokens}


async def open_account(client: AsyncClient, user: dict, currency: str = "USD") -> str:
    r = await client.post("/accounts", json={"currency": currency}, headers=user["headers"])
    assert r.status_code == 201, r.text
    return r.json()["id"]


def idem() -> dict:
    return {"Idempotency-Key": str(uuid.uuid4())}


async def deposit(client: AsyncClient, user: dict, account_id: str, amount: int):
    r = await client.post(
        "/deposits", json={"account_id": account_id, "amount": amount}, headers={**user["headers"], **idem()}
    )
    assert r.status_code == 201, r.text
    return r.json()


async def balance(client: AsyncClient, user: dict, account_id: str) -> int:
    r = await client.get(f"/accounts/{account_id}", headers=user["headers"])
    assert r.status_code == 200, r.text
    return r.json()["balance"]


@pytest.fixture
def user_factory(client):
    return lambda email=None: signup(client, email)
