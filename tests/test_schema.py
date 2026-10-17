"""Checks the schema guarantees the design relies on. Needs DATABASE_URL and the schema applied."""

import os

import psycopg
import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set (needs a Postgres with the schema)"
)


@pytest.fixture
def conn():
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        yield c
        c.rollback()  # every test leaves the database untouched


def _make_job(conn):
    tenant_id = conn.execute(
        "INSERT INTO tenants (name) VALUES (gen_random_uuid()::text) RETURNING id"
    ).fetchone()[0]
    job_id = conn.execute(
        "INSERT INTO jobs (tenant_id, name, job_type) VALUES (%s, 'j', 'noop') RETURNING id",
        (tenant_id,),
    ).fetchone()[0]
    return tenant_id, job_id


def test_same_occurrence_cannot_be_created_twice(conn):
    tenant_id, job_id = _make_job(conn)
    sql = (
        "INSERT INTO job_runs (job_id, tenant_id, scheduled_for) "
        "VALUES (%s, %s, '2026-01-01 00:00:00+00')"
    )
    conn.execute(sql, (job_id, tenant_id))
    with pytest.raises(psycopg.errors.UniqueViolation):
        conn.execute(sql, (job_id, tenant_id))


def test_on_conflict_do_nothing_makes_scheduler_inserts_safe(conn):
    tenant_id, job_id = _make_job(conn)
    sql = (
        "INSERT INTO job_runs (job_id, tenant_id, scheduled_for) "
        "VALUES (%s, %s, '2026-01-01 00:00:00+00') "
        "ON CONFLICT (job_id, scheduled_for) DO NOTHING"
    )
    first = conn.execute(sql, (job_id, tenant_id)).rowcount
    second = conn.execute(sql, (job_id, tenant_id)).rowcount
    assert (first, second) == (1, 0)


def test_invalid_state_is_rejected(conn):
    tenant_id, job_id = _make_job(conn)
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            "INSERT INTO job_runs (job_id, tenant_id, scheduled_for, state) "
            "VALUES (%s, %s, now(), 'banana')",
            (job_id, tenant_id),
        )
