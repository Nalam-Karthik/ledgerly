import time

from redis.asyncio import Redis

# Atomic token bucket. State lives in one hash per key; refill is computed lazily
# from elapsed time, so there is no background refill job and no race between
# concurrent requests (the whole script runs atomically inside Redis).
_TOKEN_BUCKET_LUA = """
local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])

local state  = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1]) or capacity
local ts     = tonumber(state[2]) or now

tokens = math.min(capacity, tokens + (now - ts) * rate)
local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, math.ceil(capacity / rate * 1000) + 1000)

local retry_after = 0
if allowed == 0 then retry_after = (1 - tokens) / rate end
return {allowed, tostring(tokens), tostring(retry_after)}
"""


class TokenBucketLimiter:
    def __init__(self, redis: Redis, capacity: int, refill_per_s: float):
        self._redis = redis
        self._capacity = capacity
        self._rate = refill_per_s
        self._script = redis.register_script(_TOKEN_BUCKET_LUA)

    async def hit(self, key: str) -> tuple[bool, int, float]:
        """Returns (allowed, remaining_tokens, retry_after_seconds)."""
        allowed, remaining, retry_after = await self._script(
            keys=[f"rl:{key}"], args=[self._capacity, self._rate, time.time()]
        )
        return bool(allowed), int(float(remaining)), float(retry_after)

    @property
    def capacity(self) -> int:
        return self._capacity
