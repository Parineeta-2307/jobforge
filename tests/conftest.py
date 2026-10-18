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
def tenant(db):
    tenant_id = db.execute(
        "INSERT INTO tenants (name) VALUES (%s) RETURNING id", (f"test-{uuid4()}",)
    ).fetchone()[0]
    yield tenant_id
    db.execute("DELETE FROM tenants WHERE id = %s", (tenant_id,))


@pytest.fixture
def make_job(db, tenant):
    """Returns a function that inserts a job for the test tenant and returns its id."""

    def _make(
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
                tenant,
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
def make_run(db, tenant):
    """Returns a function that inserts a ready run for a job and returns its id."""

    def _make(job_id, available_at=ANCIENT):
        return db.execute(
            "INSERT INTO job_runs (job_id, tenant_id, scheduled_for, available_at) "
            "VALUES (%s, %s, now(), %s) RETURNING id",
            (job_id, tenant, available_at),
        ).fetchone()[0]

    return _make
