"""The test that makes the project credible: kill workers at random, lose and double nothing.

Starts real worker processes and kills them with Popen.kill() (a hard kill, no cleanup). Takes
about a minute. Not part of the normal run; start it with:  pytest -m chaos -s

It builds its own scratch schema inside DATABASE_URL's database (see scripts/chaos.py), so it
does not touch your real tables.
"""

import os

import pytest

from scripts.chaos import run_scenario

pytestmark = pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set")


@pytest.mark.chaos
def test_no_run_lost_or_doubled_when_workers_are_killed():
    stats = run_scenario(os.environ["DATABASE_URL"], runs=60, workers=3, kill_every=2.0)
    print(
        f"\nruns={stats['runs']} kills={stats['kills']} attempts={stats['total_attempts']} "
        f"lease_expired={stats['lease_expired_attempts']} seconds={stats['seconds']} "
        f"recovery(s) median={stats['recovery_median']} max={stats['recovery_max']} "
        f"(lease={stats['lease_seconds']}s)"
    )
    assert stats["finished"], "not every run reached a final state within the timeout"
    assert stats["kills"] >= 5, "too few kills for the test to mean anything"
    assert stats["problems"] == [], stats["problems"]


def test_the_checker_reports_a_run_that_did_not_finish():
    """Sanity check that check() can fail: a run left in state ready must be reported as LOST."""
    import psycopg

    from scripts.chaos import check, create_schema, drop_schema, insert_runs

    base = os.environ["DATABASE_URL"]
    schema, url = create_schema(base)
    try:
        run_ids = insert_runs(url, 2)  # never executed, so both stay 'ready'
        with psycopg.connect(url, autocommit=True) as conn:
            conn.execute("UPDATE job_runs SET state = 'succeeded' WHERE id = %s", (run_ids[0],))
        problems = check(url, run_ids, [])["problems"]
    finally:
        drop_schema(base, schema)
    assert any(p.startswith("LOST") for p in problems)
    assert any("STALE" in p or "DOUBLED or LOST" in p for p in problems)
