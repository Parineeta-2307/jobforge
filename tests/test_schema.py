"""Checks the schema guarantees the design relies on. Needs DATABASE_URL and the schema applied."""

import os
from pathlib import Path

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


# ---- migration 002 (Weekend 2) ---------------------------------------------------------------


def _make_run(conn, tenant_id, job_id):
    return conn.execute(
        "INSERT INTO job_runs (job_id, tenant_id, scheduled_for) VALUES (%s, %s, now()) "
        "RETURNING id",
        (job_id, tenant_id),
    ).fetchone()[0]


def test_blank_tenant_name_is_rejected_by_the_database(conn):
    for blank in ("", "   "):
        with pytest.raises(psycopg.errors.CheckViolation):
            with conn.transaction():  # a savepoint, so the failed insert does not abort the test
                conn.execute("INSERT INTO tenants (name) VALUES (%s)", (blank,))


def test_second_effect_for_the_same_run_is_rejected(conn):
    tenant_id, job_id = _make_job(conn)
    run_id = _make_run(conn, tenant_id, job_id)
    sql = "INSERT INTO job_effects (run_id, tenant_id) VALUES (%s, %s)"
    conn.execute(sql, (run_id, tenant_id))
    with pytest.raises(psycopg.errors.UniqueViolation):
        conn.execute(sql, (run_id, tenant_id))


def test_on_conflict_do_nothing_makes_the_effect_insert_safe_to_repeat(conn):
    tenant_id, job_id = _make_job(conn)
    run_id = _make_run(conn, tenant_id, job_id)
    sql = (
        "INSERT INTO job_effects (run_id, tenant_id) VALUES (%s, %s) "
        "ON CONFLICT (run_id) DO NOTHING"
    )
    first = conn.execute(sql, (run_id, tenant_id)).rowcount
    second = conn.execute(sql, (run_id, tenant_id)).rowcount
    assert (first, second) == (1, 0)


def test_new_runs_start_with_attempt_base_zero(conn):
    tenant_id, job_id = _make_job(conn)
    run_id = _make_run(conn, tenant_id, job_id)
    base = conn.execute("SELECT attempt_base FROM job_runs WHERE id = %s", (run_id,)).fetchone()
    assert base == (0,)


def test_redrive_log_rows_are_removed_with_their_run(conn):
    tenant_id, job_id = _make_job(conn)
    run_id = _make_run(conn, tenant_id, job_id)
    conn.execute(
        "INSERT INTO redrive_log (run_id, previous_reason) VALUES (%s, 'boom')", (run_id,)
    )
    conn.execute("DELETE FROM job_runs WHERE id = %s", (run_id,))
    count = conn.execute("SELECT count(*) FROM redrive_log WHERE run_id = %s", (run_id,)).fetchone()
    assert count == (0,)


def test_migration_002_can_be_applied_again(conn):
    sql = (Path(__file__).parent.parent / "migrations" / "002_weekend2.sql").read_text()
    conn.execute(sql)  # every statement must be a no-op the second time
