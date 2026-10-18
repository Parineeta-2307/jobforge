"""HTTP API. Endpoints are stubs: implement them in the order given in docs/BUILD_PLAN.md."""

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
    job_type: Literal["noop", "sleep"]
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


@app.get("/runs/{run_id}")
def get_run(run_id: UUID) -> dict:
    # run_id is typed UUID, so a malformed id is a 422 before any SQL runs.
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            run = cur.execute("SELECT * FROM job_runs WHERE id = %s", (run_id,)).fetchone()
            if run is None:
                raise HTTPException(status_code=404, detail="run not found")
            attempts = cur.execute(
                "SELECT attempt, worker_id, started_at, finished_at, outcome, error "
                "FROM run_attempts WHERE run_id = %s ORDER BY attempt",
                (run_id,),
            ).fetchall()
    # FastAPI turns the UUIDs and datetimes in these dicts into JSON strings.
    run["attempts"] = attempts
    return run


@app.post("/dead-letters/{run_id}/redrive")
def redrive(run_id: str) -> dict:
    # TODO (Weekend 2): audited redrive of a dead-lettered run.
    raise HTTPException(status_code=501, detail="not implemented")
