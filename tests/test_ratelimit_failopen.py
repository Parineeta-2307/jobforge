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


def test_job_is_accepted_when_redis_is_down_and_no_limit_headers_are_sent(
    tenant, monkeypatch, caplog
):
    def redis_is_down():
        raise redis.ConnectionError("simulated outage")

    monkeypatch.setattr(ratelimit, "get_redis", redis_is_down)
    body = {"tenant_id": str(tenant), "name": "while-redis-is-down", "job_type": "noop"}
    r = client.post("/jobs", json=body)
    assert r.status_code == 200  # fail open: the outage does not stop job submission
    assert "X-RateLimit-Limit" not in r.headers
    assert "rate limiter unavailable" in caplog.text  # and it is logged


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
