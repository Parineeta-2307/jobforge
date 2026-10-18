"""Tests for per-tenant concurrency caps and fair claiming (W2-4).

Needs DATABASE_URL and the schema applied, and an otherwise empty queue (see quiet_queue in
conftest.py). Six of these tests fail on the old claim_run, which had no cap and took the globally
oldest run (cap, cap under concurrency, starvation, fewest-leased, idle cap, capped tenant). The
other two (tie-break, disabled job) are regression guards that also passed before.
"""

import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import psycopg
import pytest

from app.worker import claim_run, complete

pytestmark = pytest.mark.skipif(
    not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set (needs a Postgres with the schema)"
)

OLD = "1970-01-01 00:00:00+00"
NEWER = "1970-01-02 00:00:00+00"


def _setup(make_tenant, make_job, make_run, runs, cap=5, available_at=OLD):
    """A tenant with the given cap and `runs` ready runs. Returns (tenant_id, [run ids])."""
    tenant_id = make_tenant(max_concurrency=cap)
    job_id = make_job(tenant_id=tenant_id)
    return tenant_id, [make_run(job_id, available_at) for _ in range(runs)]


def _claim_all(db, limit=100):
    claimed = []
    while len(claimed) < limit:
        run = claim_run(db, "w1")
        if run is None:
            break
        claimed.append(run)
    return claimed


def test_a_tenant_never_has_more_runs_leased_than_its_cap(db, make_tenant, make_job, make_run):
    _setup(make_tenant, make_job, make_run, runs=5, cap=2)
    claimed = _claim_all(db)
    assert len(claimed) == 2  # 5 runs are ready, but only 2 may run at once

    complete(db, claimed[0])  # one finishes: exactly one slot opens
    assert claim_run(db, "w1") is not None
    assert claim_run(db, "w1") is None


def test_the_cap_holds_when_many_workers_claim_at_the_same_instant(
    db, make_tenant, make_job, make_run
):
    tenant_id, _ = _setup(make_tenant, make_job, make_run, runs=10, cap=3)
    start = Barrier(20)

    def worker(n):
        with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as conn:
            start.wait()
            return claim_run(conn, f"w{n}")

    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(worker, range(20)))

    assert sum(r is not None for r in results) == 3
    leased = db.execute(
        "SELECT count(*) FROM job_runs WHERE tenant_id = %s AND state = 'leased'", (tenant_id,)
    ).fetchone()
    assert leased == (3,)


def test_a_small_tenant_is_not_starved_by_a_big_backlog(db, make_tenant, make_job, make_run):
    # Tenant A queued 500 runs first (older). Tenant B arrives later with 5.
    big, _ = _setup(make_tenant, make_job, make_run, runs=1, cap=20, available_at=OLD)
    big_job = db.execute("SELECT id FROM jobs WHERE tenant_id = %s", (big,)).fetchone()[0]
    db.execute(
        "INSERT INTO job_runs (job_id, tenant_id, scheduled_for, available_at) "
        "SELECT %s, %s, now() + g * interval '1 second', %s FROM generate_series(1, 499) g",
        (big_job, big, OLD),
    )
    small, _ = _setup(make_tenant, make_job, make_run, runs=5, cap=20, available_at=NEWER)

    first_12 = [claim_run(db, "w1")["tenant_id"] for _ in range(12)]
    assert first_12.count(small) >= 5  # the old query would give B nothing until A's 500 are gone


def test_the_tenant_with_fewest_leased_runs_goes_first(db, make_tenant, make_job, make_run):
    a, _ = _setup(make_tenant, make_job, make_run, runs=3, cap=5, available_at=OLD)
    b, _ = _setup(make_tenant, make_job, make_run, runs=3, cap=5, available_at=NEWER)
    order = [claim_run(db, "w1")["tenant_id"] for _ in range(4)]
    assert order == [a, b, a, b]  # A is older, so it starts; after that they alternate


def test_a_tie_goes_to_the_tenant_that_has_waited_longest(db, make_tenant, make_job, make_run):
    _setup(make_tenant, make_job, make_run, runs=1, available_at=NEWER)
    waiting_longer, _ = _setup(make_tenant, make_job, make_run, runs=1, available_at=OLD)
    assert claim_run(db, "w1")["tenant_id"] == waiting_longer


def test_the_cap_holds_even_when_workers_are_idle(db, make_tenant, make_job, make_run):
    _setup(make_tenant, make_job, make_run, runs=3, cap=1)
    assert claim_run(db, "w1") is not None
    assert claim_run(db, "w1") is None  # nothing else to do, and still the answer is "no"


def test_a_tenant_at_its_cap_does_not_block_another_tenant(db, make_tenant, make_job, make_run):
    a, _ = _setup(make_tenant, make_job, make_run, runs=3, cap=1, available_at=OLD)
    b, _ = _setup(make_tenant, make_job, make_run, runs=2, cap=5, available_at=NEWER)
    order = [claim_run(db, "w1")["tenant_id"] for _ in range(3)]
    assert order == [a, b, b]  # A is capped after its first lease, so B gets the rest
    assert claim_run(db, "w1") is None


def test_runs_of_a_disabled_job_are_not_claimed(db, make_tenant, make_job, make_run):
    tenant_id = make_tenant()
    make_run(make_job(tenant_id=tenant_id, enabled=False))
    assert claim_run(db, "w1") is None
