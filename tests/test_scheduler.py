"""Tests for create_due_runs. Needs DATABASE_URL and the schema applied."""

import os
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest

from app.scheduler import create_due_runs

pytestmark = pytest.mark.skipif(
    not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set (needs a Postgres with the schema)"
)


def _count_runs(db, job_id):
    return db.execute("SELECT count(*) FROM job_runs WHERE job_id = %s", (job_id,)).fetchone()[0]


def test_one_off_job_gets_exactly_one_run(db, make_job):
    job_id = make_job()
    create_due_runs(db)
    assert _count_runs(db, job_id) == 1
    create_due_runs(db)  # a second pass must not add another
    assert _count_runs(db, job_id) == 1


def test_disabled_job_gets_no_run(db, make_job):
    job_id = make_job(enabled=False)
    create_due_runs(db)
    assert _count_runs(db, job_id) == 0


def test_recurring_job_after_downtime_skips_missed_occurrences(db, make_job):
    # Created an hour ago with a 60 second interval: 60 occurrences were "missed".
    job_id = make_job(interval_seconds=60, age_seconds=3600)
    create_due_runs(db)
    assert _count_runs(db, job_id) == 1
    age = db.execute(
        "SELECT extract(epoch FROM now() - scheduled_for) FROM job_runs WHERE job_id = %s",
        (job_id,),
    ).fetchone()[0]
    assert 0 <= age < 60  # it is the latest occurrence, not one from an hour ago
    create_due_runs(db)
    assert _count_runs(db, job_id) == 1


def test_two_schedulers_at_once_create_one_run(make_job, db):
    job_id = make_job(interval_seconds=60, age_seconds=3600)

    def scheduler_pass(_):
        with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as conn:
            create_due_runs(conn)

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(scheduler_pass, range(2)))
    assert _count_runs(db, job_id) == 1
