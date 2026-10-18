"""HTTP API: health, tenants, jobs, run status and dead-letter redrive."""

from typing import Literal
from uuid import UUID

import psycopg
from fastapi import FastAPI, HTTPException
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field, field_validator

from app.db import get_pool

app = FastAPI(title="jobforge")


class TenantIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)

    @field_validator("name", mode="before")
    @classmethod
    def strip_name(cls, v):
        # Trim spaces first, so "   " becomes "" and then fails the min_length check.
        return v.strip() if isinstance(v, str) else v


class JobIn(BaseModel):
    # retention_days, version and enabled are not here on purpose: the DB defaults decide them.
    tenant_id: UUID
    name: str = Field(min_length=1, max_length=100)
    job_type: Literal["noop", "sleep", "record", "flaky"]
    payload: dict = Field(default_factory=dict)
    interval_seconds: int | None = Field(default=None, ge=1)  # None means a one-off job
    max_attempts: int = Field(default=5, ge=1, le=10)

    @field_validator("name", mode="before")
    @classmethod
    def strip_name(cls, v):
        # Same trick as TenantIn: trim first, so "   " fails the min_length check.
        return v.strip() if isinstance(v, str) else v


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/tenants")
def create_tenant(body: TenantIn) -> dict:
    with get_pool().connection() as conn:
        try:
            row = conn.execute(
                "INSERT INTO tenants (name) VALUES (%s) RETURNING id, name",
                (body.name,),
            ).fetchone()
        except psycopg.errors.UniqueViolation:
            raise HTTPException(status_code=409, detail="tenant name already exists") from None
    return {"id": str(row[0]), "name": row[1]}


@app.post("/jobs")
def create_job(body: JobIn) -> dict:
    # TODO (Weekend 2): put the rate limiter in front of this route (see app/ratelimit.py)
    # and return X-RateLimit-* headers and 429 + Retry-After.
    with get_pool().connection() as conn:
        try:
            row = conn.execute(
                "INSERT INTO jobs (tenant_id, name, job_type, payload, interval_seconds, "
                "max_attempts) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id, tenant_id, name",
                (
                    body.tenant_id,
                    body.name,
                    body.job_type,
                    Jsonb(body.payload),
                    body.interval_seconds,
                    body.max_attempts,
                ),
            ).fetchone()
        except psycopg.errors.UniqueViolation:
            raise HTTPException(status_code=409, detail="job name already exists") from None
        except psycopg.errors.ForeignKeyViolation:
            raise HTTPException(status_code=404, detail="tenant not found") from None
    return {"id": str(row[0]), "tenant_id": str(row[1]), "name": row[2]}


def load_run(cur, run_id: UUID) -> dict | None:
    """The job_runs row plus its attempts, or None. Used by GET /runs/{id} and by redrive."""
    run = cur.execute("SELECT * FROM job_runs WHERE id = %s", (run_id,)).fetchone()
    if run is None:
        return None
    run["attempts"] = cur.execute(
        "SELECT attempt, worker_id, started_at, finished_at, outcome, error "
        "FROM run_attempts WHERE run_id = %s ORDER BY attempt",
        (run_id,),
    ).fetchall()
    return run


@app.get("/runs/{run_id}")
def get_run(run_id: UUID) -> dict:
    # run_id is typed UUID, so a malformed id is a 422 before any SQL runs.
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            run = load_run(cur, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    # FastAPI turns the UUIDs and datetimes in this dict into JSON strings.
    return run


class RedriveIn(BaseModel):
    note: str | None = Field(default=None, max_length=500)


@app.post("/dead-letters/{run_id}/redrive")
def redrive(run_id: UUID, body: RedriveIn | None = None) -> dict:
    """Put a dead run back in the queue with a fresh set of attempts, and keep an audit row."""
    note = body.note if body else None
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            # FOR UPDATE locks the row until we commit. A second redrive of the same run waits
            # here, then sees state 'ready' instead of 'dead' and gets a 409, so a double click
            # or two clients cannot redrive the same death twice.
            run = cur.execute(
                "SELECT state FROM job_runs WHERE id = %s FOR UPDATE", (run_id,)
            ).fetchone()
            if run is None:
                raise HTTPException(status_code=404, detail="run not found")
            if run["state"] != "dead":
                raise HTTPException(status_code=409, detail="run is not dead")
            dead = cur.execute(
                "SELECT reason FROM dead_letters WHERE run_id = %s", (run_id,)
            ).fetchone()
            previous_reason = dead["reason"] if dead else "(no dead_letters row)"
            cur.execute(
                "INSERT INTO redrive_log (run_id, previous_reason, note) VALUES (%s, %s, %s)",
                (run_id, previous_reason, note),
            )
            # attempt and fencing_token are left alone and run_attempts is not touched, so the
            # history stays intact. attempt_base = attempt starts a fresh budget of attempts.
            cur.execute(
                "UPDATE job_runs SET state = 'ready', available_at = now(), "
                "attempt_base = attempt, finished_at = NULL, "
                "lease_owner = NULL, lease_expires_at = NULL WHERE id = %s",
                (run_id,),
            )
            cur.execute(
                "UPDATE dead_letters SET redriven_at = now() WHERE run_id = %s", (run_id,)
            )
            result = load_run(cur, run_id)
    return result
