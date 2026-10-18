"""Shared fixtures for the tests that need Postgres.

Every fixture here needs DATABASE_URL, so the test modules that use them skip themselves when it
is not set (same pytestmark as test_schema.py). The database keeps its data between runs, so
every tenant gets a random name and is deleted at the end (ON DELETE CASCADE removes the rest).
"""

import os
from uuid import uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

# Test runs are made "older than everything else", so claim_run (oldest available_at first)
# picks them before any leftover runs that a developer may have in the same database.
ANCIENT = "1970-01-01 00:00:00+00"


@pytest.fixture
def db():
    """A connection in autocommit mode: each statement commits at once, like the app's own."""
    with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as c:
        yield c


@pytest.fixture
def make_tenant(db):
    """Returns a function that creates a tenant (optionally with a concurrency cap).

    Every tenant it made is deleted at the end of the test.
    """
    made = []

    def _make(max_concurrency=None):
        tenant_id = db.execute(
            "INSERT INTO tenants (name, max_concurrency) "
            "VALUES (%s, COALESCE(%s, 5)) RETURNING id",
            (f"test-{uuid4()}", max_concurrency),
        ).fetchone()[0]
        made.append(tenant_id)
        return tenant_id

    yield _make
    for tenant_id in made:
        db.execute("DELETE FROM tenants WHERE id = %s", (tenant_id,))


@pytest.fixture
def tenant(make_tenant):
    return make_tenant()


@pytest.fixture
def make_job(db, tenant):
    """Returns a function that inserts a job and returns its id (default tenant: `tenant`)."""

    def _make(
        tenant_id=None,
        job_type="noop",
        payload=None,
        interval_seconds=None,
        max_attempts=5,
        enabled=True,
        age_seconds=0,
    ):
        return db.execute(
            "INSERT INTO jobs (tenant_id, name, job_type, payload, interval_seconds, "
            "max_attempts, enabled, created_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, now() - make_interval(secs => %s)) RETURNING id",
            (
                tenant_id or tenant,
                f"job-{uuid4()}",
                job_type,
                Jsonb(payload or {}),
                interval_seconds,
                max_attempts,
                enabled,
                age_seconds,
            ),
        ).fetchone()[0]

    return _make


@pytest.fixture
def quiet_queue(db):
    """Fail early if other runs are waiting in the database.

    claim_run picks across ALL tenants (fewest leased first), so a leftover run from a
    development session would be claimed instead of the test's own run and the test would fail
    in a confusing way. Stop the scheduler and workers, or clear the leftovers, then re-run.
    """
    waiting = db.execute(
        "SELECT count(*) FROM job_runs WHERE state IN ('ready', 'retry_wait') "
        "AND available_at <= now()"
    ).fetchone()[0]
    if waiting:
        pytest.fail(
            f"{waiting} claimable run(s) already in the database. Stop the scheduler and "
            "workers and delete leftover tenants/jobs before running the claim tests."
        )


@pytest.fixture
def make_run(db, quiet_queue):
    """Returns a function that inserts a ready run for a job and returns its id."""

    def _make(job_id, available_at=ANCIENT):
        return db.execute(
            "INSERT INTO job_runs (job_id, tenant_id, scheduled_for, available_at) "
            "SELECT id, tenant_id, now(), %s FROM jobs WHERE id = %s RETURNING id",
            (available_at, job_id),
        ).fetchone()[0]

    return _make
