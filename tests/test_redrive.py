"""Tests for POST /dead-letters/{run_id}/redrive. Needs DATABASE_URL and the schema applied."""

import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.worker import claim_run, complete, fail, reap_expired_leases
from tests.conftest import ANCIENT

pytestmark = pytest.mark.skipif(
    not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set (needs a Postgres with the schema)"
)

client = TestClient(app)


def _kill(db, reason="first reason"):
    """Claim the (only ancient) ready run and fail it permanently, so it ends up dead."""
    run = claim_run(db, "w1")
    assert fail(db, run, reason, transient=False) == "dead"
    return run


def _dead_run(db, make_job, make_run, **job_args):
    run_id = make_run(make_job(**job_args))
    _kill(db, "first reason")
    return run_id


def _redrive(run_id, **body):
    return client.post(f"/dead-letters/{run_id}/redrive", json=body or None)


def test_dead_run_is_redriven_and_the_response_is_the_run(db, make_job, make_run):
    run_id = _dead_run(db, make_job, make_run)

    r = _redrive(run_id, note="downstream is fixed")
    assert r.status_code == 200
    data = r.json()
    assert data["id"] == str(run_id)
    assert data["state"] == "ready"
    assert data["attempt_base"] == data["attempt"] == 1  # a fresh budget starts after attempt 1
    assert data["finished_at"] is None
    assert data["lease_owner"] is None and data["lease_expires_at"] is None
    assert len(data["attempts"]) == 1  # history is untouched

    redriven = db.execute(
        "SELECT redriven_at IS NOT NULL FROM dead_letters WHERE run_id = %s", (run_id,)
    ).fetchone()
    assert redriven == (True,)
    log = db.execute(
        "SELECT previous_reason, note FROM redrive_log WHERE run_id = %s", (run_id,)
    ).fetchall()
    assert log == [("first reason", "downstream is fixed")]


def test_redrive_works_without_a_body(db, make_job, make_run):
    run_id = _dead_run(db, make_job, make_run)
    assert client.post(f"/dead-letters/{run_id}/redrive").status_code == 200
    note = db.execute("SELECT note FROM redrive_log WHERE run_id = %s", (run_id,)).fetchone()
    assert note == (None,)


def test_redriven_run_gets_a_fresh_set_of_attempts_and_attempt_numbers_never_repeat(
    db, make_job, make_run
):
    # max_attempts=2 and a job that always fails: dead after attempts 1 and 2.
    run_id = make_run(make_job(job_type="flaky", payload={"fail_times": 99}, max_attempts=2))
    for _ in range(2):
        run = claim_run(db, "w1")
        fail(db, run, "boom", transient=True)
        db.execute("UPDATE job_runs SET available_at = %s WHERE id = %s", (ANCIENT, run_id))
    assert db.execute("SELECT state FROM job_runs WHERE id = %s", (run_id,)).fetchone() == ("dead",)

    assert _redrive(run_id).status_code == 200

    # Attempt 3 is the first of the new budget: failing it must retry, not die at once.
    third = claim_run(db, "w1")
    assert (third["attempt"], third["attempt_base"]) == (3, 2)
    assert fail(db, third, "boom", transient=True) == "retry"
    db.execute("UPDATE job_runs SET available_at = %s WHERE id = %s", (ANCIENT, run_id))
    # Attempt 4 is the second of the new budget (max_attempts=2), so now it dies.
    fourth = claim_run(db, "w1")
    assert fail(db, fourth, "boom", transient=True) == "dead"

    rows = db.execute(
        "SELECT attempt FROM run_attempts WHERE run_id = %s ORDER BY attempt", (run_id,)
    ).fetchall()
    assert rows == [(1,), (2,), (3,), (4,)]  # numbers keep counting up, none repeats


def test_second_death_replaces_the_reason_and_second_redrive_adds_a_log_row(
    db, make_job, make_run
):
    run_id = _dead_run(db, make_job, make_run)
    assert _redrive(run_id).status_code == 200

    run = claim_run(db, "w1")
    assert fail(db, run, "second reason", transient=False) == "dead"
    row = db.execute(
        "SELECT reason, redriven_at FROM dead_letters WHERE run_id = %s", (run_id,)
    ).fetchone()
    assert row == ("second reason", None)  # new reason, and redriven_at cleared

    assert _redrive(run_id, note="again").status_code == 200
    log = db.execute(
        "SELECT previous_reason FROM redrive_log WHERE run_id = %s ORDER BY id", (run_id,)
    ).fetchall()
    assert log == [("first reason",), ("second reason",)]


def test_a_run_that_dies_from_an_expired_lease_after_redrive_gets_a_new_reason(
    db, make_job, make_run
):
    run_id = _dead_run(db, make_job, make_run, max_attempts=1)
    assert _redrive(run_id).status_code == 200

    claim_run(db, "w1")
    db.execute(
        "UPDATE job_runs SET lease_expires_at = now() - interval '1 second' WHERE id = %s",
        (run_id,),
    )
    reap_expired_leases(db)
    reason = db.execute("SELECT reason FROM dead_letters WHERE run_id = %s", (run_id,)).fetchone()
    assert reason == ("out of attempts: lease expired",)


def test_succeeded_run_cannot_be_redriven(db, make_job, make_run):
    run_id = make_run(make_job())
    complete(db, claim_run(db, "w1"))
    r = _redrive(run_id)
    assert r.status_code == 409
    assert r.json()["detail"] == "run is not dead"


def test_unknown_run_is_404():
    assert _redrive(uuid4()).status_code == 404


def test_malformed_run_id_is_422():
    assert client.post("/dead-letters/not-a-uuid/redrive").status_code == 422


def test_two_concurrent_redrives_of_one_run_give_exactly_one_200(db, make_job, make_run):
    run_id = _dead_run(db, make_job, make_run)
    start = Barrier(2)

    def attempt(_):
        start.wait()
        return _redrive(run_id).status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        statuses = sorted(pool.map(attempt, range(2)))
    assert statuses == [200, 409]
    count = db.execute("SELECT count(*) FROM redrive_log WHERE run_id = %s", (run_id,)).fetchone()
    assert count == (1,)
