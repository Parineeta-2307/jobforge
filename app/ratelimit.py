"""Per-tenant rate limiting, modelled on Jira Cloud's public rate-limit design.

Two independent limits (see docs/DESIGN.md for the reasoning):
  1. Burst: a token bucket per (tenant, endpoint), refilled every second.
  2. Quota: an hourly points budget per tenant. Heavier job types cost more points.

State lives in Redis so every API process shares it. Do the read-modify-write inside one
Lua script (EVAL) so two API processes cannot both spend the last token.
"""

# Points charged per request, by job type. Tune these and explain the numbers in the README.
JOB_TYPE_COST = {
    "noop": 1,
    "http_call": 2,
    "report": 5,
}


def check_burst(tenant_id: str, endpoint: str, rps: int) -> tuple[bool, int]:
    """Spend one token from the tenant's bucket for this endpoint.

    Returns (allowed, retry_after_seconds).
    TODO (Weekend 2): Redis Lua token bucket.
    """
    raise NotImplementedError


def charge_points(tenant_id: str, cost: int, quota_per_hour: int) -> tuple[bool, int, int]:
    """Charge points against the tenant's hourly quota.

    Returns (allowed, remaining, seconds_until_reset).
    TODO (Weekend 2): a Redis counter keyed by tenant and UTC hour, expiring at the hour's end.
    """
    raise NotImplementedError


def rate_limit_headers(limit: int, remaining: int, near_limit: bool) -> dict[str, str]:
    """Headers to attach to every response. 429s also need Retry-After and a reason.

    TODO (Weekend 2): X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-NearLimit.
    """
    raise NotImplementedError
