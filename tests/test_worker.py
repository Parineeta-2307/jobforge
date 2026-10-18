"""Tests for the worker functions: claim, lease, heartbeat, fencing, retry, dead letter.

Needs DATABASE_URL and the schema applied. See conftest.py for why test runs use an ancient
available_at.
"""

import os
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest

from app import config
from app.worker import (
    PermanentError,
    backoff_seconds,
    claim_run,
    complete,
    execute,
    fail,
    heartbeat,
    reap_expired_leases,
    run_one,
)
from tests.conftest import ANCIENT

pytestmark = pytest.mark.skipif(
    not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set (needs a Postgres with the schema)"
)


def _state(db, run_id):
    return db.execute("SELECT state FROM job_runs WHERE id = %s", (run_id,)).fetchone()[0]


def _expire_lease(db, run_id):
    db.execute(
        "UPDATE job_runs SET lease_expires_at = now() - interval '1 second' WHERE id = %s",
        (run_id,),
    )


def _outcomes(db, run_id):
    rows = db.execute(
        "SELECT outcome FROM run_attempts WHERE run_id = %s ORDER BY attempt", (run_id,)
    ).fetchall()
    return [r[0] for r in rows]


# ---- claim ---------------------------------------------------------------------------------


def test_claim_takes_a_lease(db, make_job, make_run):
    run_id = make_run(make_job())
    run = claim_run(db, "w1")
    assert run["id"] == run_id
    assert (run["attempt"], run["fencing_token"]) == (1, 1)
    row = db.execute(
        "SELECT state, lease_owner, lease_expires_at > now() FROM job_runs WHERE id = %s", (run_id,)
    ).fetchone()
    assert row == ("leased", "w1", True)
    assert _outcomes(db, run_id) == [None]  # one attempt row, still open


def test_run_that_is_not_due_yet_is_not_claimed(db, make_job, make_run):
    run_id = make_run(make_job(), available_at="2999-01-01 00:00:00+00")
    run = claim_run(db, "w1")
    assert run is None or run["id"] != run_id


def test_two_workers_never_get_the_same_run(db, make_job, make_run):
    job_id = make_job()
    mine = {make_run(job_id) for _ in range(30)}

    def worker(worker_id):
        claimed = []
        with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as conn:
            for _ in range(10):
                run = claim_run(conn, worker_id)
                if run is not None:
                    claimed.append(run["id"])
        return claimed

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(worker, ["w1", "w2"]))
    all_claimed = results[0] + results[1]
    assert len(all_claimed) == 20
    assert len(set(all_claimed)) == 20  # no run was handed out twice
    assert set(all_claimed) <= mine


# ---- lease, heartbeat, fencing -------------------------------------------------------------


def test_heartbeat_extends_the_lease(db, make_job, make_run):
    run_id = make_run(make_job())
    run = claim_run(db, "w1")
    assert run["id"] == run_id
    db.execute(
        "UPDATE job_runs SET lease_expires_at = now() + interval '1 second' WHERE id = %s",
        (run_id,),
    )
    assert heartbeat(db, run) is True
    extended = db.execute(
        "SELECT lease_expires_at > now() + interval '10 seconds' FROM job_runs WHERE id = %s",
        (run_id,),
    ).fetchone()[0]
    assert extended


def test_expired_lease_is_reaped_and_the_stale_worker_loses(db, make_job, make_run):
    run_id = make_run(make_job())

    old = claim_run(db, "old-worker")
    assert old["id"] == run_id
    _expire_lease(db, run_id)  # the old worker "died": no heartbeat, lease runs out

    assert reap_expired_leases(db) >= 1
    assert _state(db, run_id) == "retry_wait"

    new = claim_run(db, "new-worker")
    assert new["id"] == run_id
    assert (new["attempt"], new["fencing_token"]) == (2, 2)

    # The old worker wakes up. The fencing token stops all three things it might try.
    assert heartbeat(db, old) is False
    assert fail(db, old, "late failure", transient=True) == "stale"
    assert complete(db, old) is False
    assert _state(db, run_id) == "leased"  # still the new worker's

    assert complete(db, new) is True
    assert _state(db, run_id) == "succeeded"
    assert _outcomes(db, run_id) == ["lease_expired", "succeeded"]


def test_complete_marks_succeeded(db, make_job, make_run):
    run_id = make_run(make_job())
    run = claim_run(db, "w1")
    assert complete(db, run) is True
    row = db.execute(
        "SELECT state, finished_at IS NOT NULL, lease_owner FROM job_runs WHERE id = %s", (run_id,)
    ).fetchone()
    assert row == ("succeeded", True, None)
    assert _outcomes(db, run_id) == ["succeeded"]


# ---- retries and dead letters --------------------------------------------------------------


def test_transient_failure_goes_to_retry_wait_with_a_future_available_at(db, make_job, make_run):
    run_id = make_run(make_job(max_attempts=3))
    run = claim_run(db, "w1")
    assert fail(db, run, "boom", transient=True) == "retry"
    row = db.execute(
        "SELECT state, available_at > now(), last_error FROM job_runs WHERE id = %s", (run_id,)
    ).fetchone()
    assert row == ("retry_wait", True, "boom")
    assert _outcomes(db, run_id) == ["failed"]


def test_running_out_of_attempts_moves_the_run_to_dead_letters(db, make_job, make_run):
    run_id = make_run(make_job(max_attempts=2))

    first = claim_run(db, "w1")
    assert fail(db, first, "boom 1", transient=True) == "retry"
    db.execute("UPDATE job_runs SET available_at = %s WHERE id = %s", (ANCIENT, run_id))

    second = claim_run(db, "w1")
    assert second["attempt"] == 2
    assert fail(db, second, "boom 2", transient=True) == "dead"

    assert _state(db, run_id) == "dead"
    reason = db.execute("SELECT reason FROM dead_letters WHERE run_id = %s", (run_id,)).fetchone()
    assert reason == ("out of attempts: boom 2",)


def test_permanent_error_is_dead_on_the_first_attempt(db, make_job, make_run):
    run_id = make_run(make_job(max_attempts=5))
    run = claim_run(db, "w1")
    assert fail(db, run, "bad payload", transient=False) == "dead"
    assert _state(db, run_id) == "dead"
    reason = db.execute("SELECT reason FROM dead_letters WHERE run_id = %s", (run_id,)).fetchone()
    assert reason == ("bad payload",)


def test_expired_lease_on_the_last_attempt_is_dead(db, make_job, make_run):
    run_id = make_run(make_job(max_attempts=1))
    claim_run(db, "w1")
    _expire_lease(db, run_id)
    reap_expired_leases(db)
    assert _state(db, run_id) == "dead"
    count = db.execute("SELECT count(*) FROM dead_letters WHERE run_id = %s", (run_id,)).fetchone()
    assert count == (1,)


def test_backoff_grows_with_the_attempt_and_is_capped():
    for _ in range(50):  # the jitter is random, so look at many samples
        assert 2 <= backoff_seconds(1) <= 4  # base 2 * 2**1 = 4, equal jitter -> 2..4
        assert 4 <= backoff_seconds(2) <= 8
        assert config.BACKOFF_MAX_SECONDS / 2 <= backoff_seconds(20) <= config.BACKOFF_MAX_SECONDS


# ---- execute and the full path -------------------------------------------------------------


def test_execute_noop_and_sleep():
    execute({"job_type": "noop", "payload": {}})
    execute({"job_type": "sleep", "payload": {"seconds": 0}})


@pytest.mark.parametrize(
    "run",
    [
        {"job_type": "banana", "payload": {}},
        {"job_type": "sleep", "payload": {"seconds": "ten"}},
        {"job_type": "sleep", "payload": {"seconds": -1}},
    ],
)
def test_execute_rejects_bad_jobs_as_permanent_errors(run):
    with pytest.raises(PermanentError):
        execute(run)


def test_run_one_executes_and_completes(db, make_job, make_run):
    run_id = make_run(make_job(job_type="sleep", payload={"seconds": 0}))
    run = claim_run(db, "w1")
    run_one(run)
    assert _state(db, run_id) == "succeeded"


def test_run_one_sends_a_bad_job_straight_to_dead_letters(db, make_job, make_run):
    run_id = make_run(make_job(job_type="sleep", payload={"seconds": "ten"}))
    run = claim_run(db, "w1")
    run_one(run)
    assert _state(db, run_id) == "dead"
