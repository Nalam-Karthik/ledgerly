from app.ratelimit import TokenBucketLimiter
from tests.conftest import signup


async def test_token_bucket_returns_429_with_retry_after(client, _app):
    user = await signup(client)
    original = _app.state.limiter
    _app.state.limiter = TokenBucketLimiter(_app.state.redis, capacity=3, refill_per_s=0.5)
    try:
        codes = [(await client.get("/accounts", headers=user["headers"])) for _ in range(4)]
        assert [r.status_code for r in codes] == [200, 200, 200, 429]
        assert codes[0].headers["x-ratelimit-remaining"] == "2"
        assert int(codes[3].headers["retry-after"]) >= 1
        assert codes[3].json()["error"]["code"] == "rate_limited"
    finally:
        _app.state.limiter = original


async def test_buckets_are_per_user(client, _app):
    alice, bob = await signup(client), await signup(client)
    original = _app.state.limiter
    _app.state.limiter = TokenBucketLimiter(_app.state.redis, capacity=1, refill_per_s=0.01)
    try:
        assert (await client.get("/accounts", headers=alice["headers"])).status_code == 200
        assert (await client.get("/accounts", headers=alice["headers"])).status_code == 429
        assert (await client.get("/accounts", headers=bob["headers"])).status_code == 200
    finally:
        _app.state.limiter = original
