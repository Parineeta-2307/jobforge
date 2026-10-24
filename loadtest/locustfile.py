"""Load test for POST /jobs. Start the API first:  python -m uvicorn app.main:app

Scenario 1, baseline: one tenant with a huge quota, 20 users, noop jobs.
    locust -f loadtest/locustfile.py BaselineUser --headless -u 20 -r 20 -t 70s \
        --host http://127.0.0.1:8000 --csv docs/results/load_baseline_RUN
Scenario 2, over-quota (needs a reachable Redis, or nothing is ever rejected): a noisy tenant with a
small quota and burst limit, and a quiet tenant with normal limits, both submitting.
    locust -f loadtest/locustfile.py NoisyUser QuietUser --headless -u 10 -r 10 -t 70s ...

The first 10 seconds are a warm-up: statistics are reset at 10 s, so the report covers the
remaining 60 s. Tenants are created when the test starts and deleted when it stops. A 429 is an
expected answer, not a failure: it is counted per tenant instead. Set LOAD_SUMMARY_PATH to also
save a JSON summary (requests, 429 share, p50/p95/p99 per tenant, whether Redis was reachable).
"""

import json
import os
import time
from collections import Counter
from uuid import uuid4

import gevent
import psycopg
import requests
from locust import HttpUser, between, events, task

from app import config

WARMUP_SECONDS = 10

# label -> tenant limits. "baseline" has a quota and burst limit far above what 20 users can use.
TENANTS = {
    "baseline": {"points_quota_per_hour": 1_000_000, "burst_rps": 10_000},
    "noisy": {"points_quota_per_hour": 50, "burst_rps": 5},
    "quiet": {},  # the default limits
}
tenant_ids: dict[str, str] = {}
status_429 = Counter()  # label -> number of 429 responses
measure_started = [time.time()]


def create_tenant(host: str, label: str) -> None:
    """Create the tenant for a label through the API, so the rate limiter reads real rows."""
    body = {"name": f"load-{label}-{uuid4().hex[:8]}", **TENANTS[label]}
    r = requests.post(f"{host}/tenants", json=body, timeout=10)
    r.raise_for_status()
    tenant_ids[label] = r.json()["id"]


class SubmitUser(HttpUser):
    abstract = True
    label = ""
    wait_time = between(0.01, 0.1)

    def on_start(self):
        self.tenant_id = tenant_ids[self.label]  # created once in on_test_start, not per user

    @task
    def submit(self):
        body = {"tenant_id": self.tenant_id, "name": f"j-{uuid4().hex}", "job_type": "noop"}
        with self.client.post(
            "/jobs", json=body, name=f"POST /jobs [{self.label}]", catch_response=True
        ) as r:
            if r.status_code == 429:
                status_429[self.label] += 1
                r.success()  # the limiter doing its job is not a failure


class BaselineUser(SubmitUser):
    label = "baseline"


class NoisyUser(SubmitUser):
    label = "noisy"
    wait_time = between(0.01, 0.05)  # hammers the API


class QuietUser(SubmitUser):
    label = "quiet"
    wait_time = between(0.2, 0.5)  # a well-behaved client


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    # One tenant per label, created before any user starts. (Creating it inside on_start would
    # let every user create its own, because requests yield to other users while waiting.)
    for user_class in environment.user_classes:
        if user_class.label not in tenant_ids:
            create_tenant(environment.host, user_class.label)
    status_429.clear()
    measure_started[0] = time.time() + WARMUP_SECONDS

    def reset():
        environment.runner.stats.reset_all()
        status_429.clear()
        measure_started[0] = time.time()

    gevent.spawn_later(WARMUP_SECONDS, reset)


def redis_reachable() -> bool:
    try:
        import redis

        return bool(redis.Redis.from_url(config.REDIS_URL, socket_connect_timeout=1).ping())
    except Exception:
        return False


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    seconds = max(time.time() - measure_started[0], 1e-9)
    summary = {"measured_seconds": round(seconds, 1), "redis_reachable": redis_reachable(),
               "tenants": {}}
    for name, entry in environment.runner.stats.entries.items():
        request_name = name[0]  # the key is (name, method), e.g. ("POST /jobs [quiet]", "POST")
        label = request_name.split("[")[-1].rstrip("]") if "[" in request_name else request_name
        n = entry.num_requests
        summary["tenants"][label] = {
            "requests": n,
            "requests_per_second": round(n / seconds, 1),
            "429s": status_429[label],
            "429_percent": round(100 * status_429[label] / n, 1) if n else 0,
            "failures": entry.num_failures,
            "p50_ms": entry.get_response_time_percentile(0.50),
            "p95_ms": entry.get_response_time_percentile(0.95),
            "p99_ms": entry.get_response_time_percentile(0.99),
        }
    print(json.dumps(summary, indent=2))
    path = os.getenv("LOAD_SUMMARY_PATH")
    if path:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
    # Delete the tenants this test created (ON DELETE CASCADE removes their jobs).
    with psycopg.connect(config.DATABASE_URL, autocommit=True) as conn:
        for tenant_id in tenant_ids.values():
            conn.execute("DELETE FROM tenants WHERE id = %s", (tenant_id,))
    tenant_ids.clear()
