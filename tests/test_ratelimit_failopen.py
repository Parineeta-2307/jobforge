"""Limiter behaviour that needs no Redis: fail open, and the optional tenant limits."""

import os

import pytest
import redis
from fastapi.testclient import TestClient

from app import ratelimit
from app.main import app

pytestmark = pytest.mark.skipif(
    not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set (needs a Postgres with the schema)"
)

client = TestClient(app)

LIMITS = "SELECT points_quota_per_hour, burst_rps, max_concurrency FROM tenants WHERE id = %s"


def test_job_is_accepted_when_redis_is_down_and_the_breaker_skips_redis_afterwards(
    tenant, monkeypatch, caplog
):
    calls = []

    def redis_is_down():
        calls.append(1)
        raise redis.ConnectionError("simulated outage")

    monkeypatch.setattr(ratelimit, "get_redis", redis_is_down)
    monkeypatch.setattr(ratelimit, "_retry_redis_at", 0.0)  # restored when the test ends

    def post(name):
        body = {"tenant_id": str(tenant), "name": name, "job_type": "noop"}
        return client.post("/jobs", json=body)

    first = post("while-redis-is-down-1")
    assert first.status_code == 200  # fail open: the outage does not stop job submission
    assert "X-RateLimit-Limit" not in first.headers
    assert "rate limiter unavailable" in caplog.text  # and it is logged
    assert len(calls) == 1

    second = post("while-redis-is-down-2")
    assert second.status_code == 200
    assert len(calls) == 1  # the breaker is open, so Redis was not tried again


def test_tenant_limits_default_to_the_database_defaults(db):
    r = client.post("/tenants", json={"name": "test-defaults-check"})
    assert r.status_code == 200
    row = db.execute(LIMITS, (r.json()["id"],)).fetchone()
    db.execute("DELETE FROM tenants WHERE id = %s", (r.json()["id"],))
    assert row == (10000, 50, 5)


def test_tenant_limits_can_be_set_and_are_validated(db):
    limits = {"points_quota_per_hour": 7, "burst_rps": 2, "max_concurrency": 3}
    ok = client.post("/tenants", json={"name": "test-limits-check", **limits})
    assert ok.status_code == 200
    row = db.execute(LIMITS, (ok.json()["id"],)).fetchone()
    db.execute("DELETE FROM tenants WHERE id = %s", (ok.json()["id"],))
    assert row == (7, 2, 3)
    for bad in ({"burst_rps": 0}, {"points_quota_per_hour": -1}, {"max_concurrency": 5000}):
        assert client.post("/tenants", json={"name": "test-bad-limits", **bad}).status_code == 422
