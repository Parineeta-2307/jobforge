"""Tests for the Redis rate limiter (W2-5).

Needs REDIS_URL (a reachable Redis, version 6 or later) and DATABASE_URL. Skipped otherwise.
Every key contains a random tenant id, and the tests delete the keys they created.
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import get_args
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app import ratelimit
from app.main import JobIn, app


def _redis_reachable() -> bool:
    if not os.getenv("REDIS_URL"):
        return False
    try:
        return bool(ratelimit.get_redis().ping())
    except Exception:
        return False


pytestmark = [
    pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set"),
    pytest.mark.skipif(not _redis_reachable(), reason="REDIS_URL not set or Redis unreachable"),
]

client = TestClient(app)


@pytest.fixture
def clean_keys():
    """Collects tenant ids used by a test and deletes their Redis keys afterwards."""
    tenants = []
    yield tenants
    r = ratelimit.get_redis()
    for tenant_id in tenants:
        for key in r.scan_iter(f"*:{tenant_id}:*"):
            r.delete(key)


@pytest.fixture(autouse=True)
def delete_test_tenants(db, monkeypatch):
    monkeypatch.setattr(ratelimit, "_retry_redis_at", 0.0)  # a breaker left open must not leak in
    yield
    db.execute("DELETE FROM tenants WHERE name LIKE 'test-rl-%'")


def _new_tenant(clean_keys, **limits):
    r = client.post("/tenants", json={"name": f"test-rl-{uuid4()}", **limits})
    assert r.status_code == 200, r.text
    tenant_id = r.json()["id"]
    clean_keys.append(tenant_id)
    return tenant_id


def _post_job(tenant_id, job_type="noop"):
    body = {"tenant_id": tenant_id, "name": f"j-{uuid4()}", "job_type": job_type}
    return client.post("/jobs", json=body)


# ---- the Lua scripts, directly -------------------------------------------------------------


def test_burst_allows_the_capacity_then_rejects_then_refills(clean_keys):
    tenant_id = str(uuid4())
    clean_keys.append(tenant_id)

    def hit():
        return ratelimit.check_burst(tenant_id, "e", capacity=3, refill_per_second=1)

    results = [hit() for _ in range(4)]
    assert [r[0] for r in results] == [True, True, True, False]
    assert results[3][2] >= 1  # Retry-After is at least one second
    time.sleep(1.1)
    assert hit()[0] is True


def test_50_threads_on_a_bucket_of_10_let_exactly_10_through(clean_keys):
    tenant_id = str(uuid4())
    clean_keys.append(tenant_id)
    start = Barrier(50)

    def hit(_):
        start.wait()
        # Refill is tiny, so no token comes back during the test.
        return ratelimit.check_burst(tenant_id, "e", capacity=10, refill_per_second=0.001)[0]

    with ThreadPoolExecutor(max_workers=50) as pool:
        allowed = list(pool.map(hit, range(50)))
    assert sum(allowed) == 10  # atomic: nobody spends a token that is already gone


def test_quota_charges_until_the_limit_then_rejects_without_charging(clean_keys):
    tenant_id = str(uuid4())
    clean_keys.append(tenant_id)
    assert ratelimit.charge_points(tenant_id, 5, 10)[:2] == (True, 5)
    assert ratelimit.charge_points(tenant_id, 5, 10)[:2] == (True, 0)
    allowed, left, reset_in = ratelimit.charge_points(tenant_id, 5, 10)
    assert (allowed, left) == (False, 0)
    assert 1 <= reset_in <= 3600
    assert ratelimit.charge_points(tenant_id, 0, 10)[:2] == (True, 0)  # cost 0 only reads


def test_a_new_hour_starts_a_fresh_quota(clean_keys, monkeypatch):
    tenant_id = str(uuid4())
    clean_keys.append(tenant_id)
    monkeypatch.setattr(ratelimit, "current_hour", lambda: "2099010100")
    assert ratelimit.charge_points(tenant_id, 10, 10)[0] is True
    assert ratelimit.charge_points(tenant_id, 1, 10)[0] is False
    monkeypatch.setattr(ratelimit, "current_hour", lambda: "2099010101")
    assert ratelimit.charge_points(tenant_id, 1, 10)[0] is True


# ---- through POST /jobs --------------------------------------------------------------------


def test_headers_are_on_200_and_near_limit_flips_at_20_percent(clean_keys):
    tenant_id = _new_tenant(clean_keys, points_quota_per_hour=10, burst_rps=100)
    for used in range(1, 9):  # 8 points used, 2 left = exactly 20%
        r = _post_job(tenant_id)
        assert r.status_code == 200
        assert r.headers["X-RateLimit-Limit"] == "10"
        assert r.headers["X-RateLimit-Remaining"] == str(10 - used)
        assert r.headers["X-RateLimit-NearLimit"] == ("true" if used >= 8 else "false")


def test_quota_exhausted_gives_429_with_headers_and_creates_no_job(clean_keys, db):
    tenant_id = _new_tenant(clean_keys, points_quota_per_hour=3, burst_rps=100)
    for _ in range(3):
        assert _post_job(tenant_id).status_code == 200
    r = _post_job(tenant_id)
    assert r.status_code == 429
    assert r.headers["RateLimit-Reason"] == "quota"
    assert 1 <= int(r.headers["Retry-After"]) <= 3600
    assert r.headers["X-RateLimit-Remaining"] == "0"
    assert r.headers["X-RateLimit-NearLimit"] == "true"
    jobs = db.execute("SELECT count(*) FROM jobs WHERE tenant_id = %s", (tenant_id,)).fetchone()
    assert jobs == (3,)  # the rejected request inserted nothing


def test_burst_exhausted_gives_429_with_reason_burst(clean_keys):
    tenant_id = _new_tenant(clean_keys, points_quota_per_hour=1000, burst_rps=1)
    assert _post_job(tenant_id).status_code == 200
    r = _post_job(tenant_id)
    assert r.status_code == 429
    assert r.headers["RateLimit-Reason"] == "burst"
    assert int(r.headers["Retry-After"]) >= 1
    assert r.headers["X-RateLimit-Limit"] == "1000"  # headers are present on the 429 too


def test_one_tenant_running_out_does_not_affect_another(clean_keys):
    noisy = _new_tenant(clean_keys, points_quota_per_hour=1, burst_rps=100)
    quiet = _new_tenant(clean_keys, points_quota_per_hour=100, burst_rps=100)
    assert _post_job(noisy).status_code == 200
    assert _post_job(noisy).status_code == 429
    r = _post_job(quiet)
    assert r.status_code == 200
    assert r.headers["X-RateLimit-Remaining"] == "99"


def test_heavier_job_types_cost_more_points(clean_keys):
    tenant_id = _new_tenant(clean_keys, points_quota_per_hour=100, burst_rps=100)
    r = _post_job(tenant_id, job_type="record")
    assert r.headers["X-RateLimit-Remaining"] == str(100 - ratelimit.JOB_TYPE_COST["record"])


def test_every_job_type_has_a_cost():
    job_types = get_args(JobIn.model_fields["job_type"].annotation)
    assert set(job_types) == set(ratelimit.JOB_TYPE_COST)
