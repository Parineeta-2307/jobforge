"""Per-tenant rate limiting, modelled on Jira Cloud's public rate-limit design.

Two independent limits (see docs/DESIGN.md for the reasoning):
  1. Burst: a token bucket per (tenant, endpoint). Protects against a spike in one second.
  2. Quota: an hourly points budget per tenant. Heavier job types cost more points. Protects
     against a steady drip that adds up to too much load over an hour.

State lives in Redis so every API process shares it. Each read-modify-write runs inside one Lua
script, which Redis executes without interleaving any other command, so two API processes cannot
both spend the last token.
"""

import logging
import math
import time
from datetime import UTC, datetime

import redis

from app import config

log = logging.getLogger("ratelimit")

# Points charged per request, by job type. These are design choices (not measurements): a
# "sleep" job holds a worker lease for its whole duration, a "record" job also writes to the
# database, "noop" and the test-only "flaky" are the cheapest.
JOB_TYPE_COST = {
    "noop": 1,
    "sleep": 2,
    "record": 3,
    "flaky": 1,
}

# Token bucket. Returns {allowed (1/0), whole tokens left, milliseconds until the next token}.
# Time comes from Redis (TIME), not from the API process, so API servers with different clocks
# cannot disagree. The key expires once the bucket would be full again.
BURST_LUA = """
local t = redis.call('TIME')
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
    tokens = capacity
    ts = now_ms
end
tokens = math.min(capacity, tokens + math.max(0, now_ms - ts) * rate / 1000)
local allowed = 0
local retry_ms = 0
if tokens >= cost then
    tokens = tokens - cost
    allowed = 1
else
    retry_ms = math.ceil((cost - tokens) * 1000 / rate)
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now_ms)
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / rate * 1000) + 1000)
return {allowed, math.floor(tokens), retry_ms}
"""

# Hourly quota. Adds `cost` only if the total stays within the limit, otherwise changes nothing.
# Returns {allowed (1/0), points left}. A cost of 0 just reads the points left.
QUOTA_LUA = """
local used = tonumber(redis.call('GET', KEYS[1]) or '0')
local limit = tonumber(ARGV[1])
local cost = tonumber(ARGV[2])
if used + cost > limit then
    return {0, limit - used}
end
if cost > 0 then
    used = redis.call('INCRBY', KEYS[1], cost)
end
if redis.call('TTL', KEYS[1]) < 0 then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
end
return {1, limit - used}
"""

_client: redis.Redis | None = None

# Circuit breaker. A dead Redis makes every connection attempt wait for a timeout (about 1 second
# on Windows), and the API would pay that on every request while it fails open. After one
# failure we skip Redis completely for REDIS_RETRY_SECONDS, then try again.
REDIS_RETRY_SECONDS = 5
_retry_redis_at = 0.0


def redis_available() -> bool:
    """False while the breaker is open, i.e. shortly after a Redis failure."""
    return time.monotonic() >= _retry_redis_at


def mark_redis_down() -> None:
    global _retry_redis_at
    _retry_redis_at = time.monotonic() + REDIS_RETRY_SECONDS


def get_redis() -> redis.Redis:
    """One shared client (it has its own connection pool). Short timeouts so a dead Redis fails
    fast instead of making every request wait."""
    global _client
    if _client is None:
        _client = redis.Redis.from_url(
            config.REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=0.5,
            socket_timeout=0.5,
        )
    return _client


def current_hour() -> str:
    """The UTC hour as part of the quota key, e.g. '2026101821'. Tests replace this function to
    simulate the next hour."""
    return datetime.now(UTC).strftime("%Y%m%d%H")


def seconds_until_next_hour() -> int:
    now = datetime.now(UTC)
    return 3600 - (now.minute * 60 + now.second)


def check_burst(
    tenant_id: str, endpoint: str, capacity: int, refill_per_second: float, cost: int = 1
) -> tuple[bool, int, int]:
    """Spend `cost` tokens from the tenant's bucket for this endpoint.

    Returns (allowed, tokens_left, retry_after_seconds). retry_after_seconds is 0 when allowed,
    otherwise the time until enough tokens exist, rounded up to a whole second.
    """
    script = get_redis().register_script(BURST_LUA)
    allowed, tokens_left, retry_ms = script(
        keys=[f"burst:{tenant_id}:{endpoint}"], args=[capacity, refill_per_second, cost]
    )
    return bool(allowed), tokens_left, math.ceil(retry_ms / 1000)


def charge_points(tenant_id: str, cost: int, quota_per_hour: int) -> tuple[bool, int, int]:
    """Charge points against the tenant's quota for the current UTC hour.

    Returns (allowed, points_left, seconds_until_reset). The counter is a fixed window: it resets
    at the top of the hour, so a tenant can use up to 2x the quota across an hour boundary.
    """
    script = get_redis().register_script(QUOTA_LUA)
    remaining_seconds = seconds_until_next_hour()
    allowed, points_left = script(
        keys=[f"quota:{tenant_id}:{current_hour()}"],
        args=[quota_per_hour, cost, remaining_seconds + 300],  # keep the key a little past the hour
    )
    return bool(allowed), points_left, remaining_seconds


def rate_limit_headers(limit: int, remaining: int, near_limit: bool) -> dict[str, str]:
    """Headers for every response from a rate-limited endpoint, allowed or not."""
    return {
        "X-RateLimit-Limit": str(limit),
        "X-RateLimit-Remaining": str(max(remaining, 0)),
        "X-RateLimit-NearLimit": "true" if near_limit else "false",
    }


def is_near_limit(limit: int, remaining: int) -> bool:
    """True when 20% or less of the hourly quota is left."""
    return remaining <= limit * 0.2
