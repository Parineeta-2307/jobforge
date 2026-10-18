"""HTTP API: health, tenants, jobs, run status and dead-letter redrive."""

import logging
from typing import Literal
from uuid import UUID

import psycopg
import redis
from fastapi import FastAPI, HTTPException, Response
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field, field_validator

from app import ratelimit
from app.db import get_pool

app = FastAPI(title="jobforge")
log = logging.getLogger("api")


class TenantIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    # Optional limits. When left out, the database defaults apply (see create_tenant).
    points_quota_per_hour: int | None = Field(default=None, ge=1, le=1_000_000)
    burst_rps: int | None = Field(default=None, ge=1, le=10_000)
    max_concurrency: int | None = Field(default=None, ge=1, le=1_000)

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
                # COALESCE falls back to the column defaults of tenants (migrations/001_init.sql).
                "INSERT INTO tenants (name, points_quota_per_hour, burst_rps, max_concurrency) "
                "VALUES (%s, COALESCE(%s, 10000), COALESCE(%s, 50), COALESCE(%s, 5)) "
                "RETURNING id, name",
                (body.name, body.points_quota_per_hour, body.burst_rps, body.max_concurrency),
            ).fetchone()
        except psycopg.errors.UniqueViolation:
            raise HTTPException(status_code=409, detail="tenant name already exists") from None
    return {"id": str(row[0]), "name": row[1]}


def apply_rate_limit(body: JobIn) -> dict[str, str]:
    """Check the tenant's burst limit, then its hourly quota. Return the headers to send.

    Raises 404 for an unknown tenant (before touching Redis, so made-up tenant ids never create
    keys) and 429 with Retry-After when a limit is hit. If Redis is down the request is allowed
    and a warning is logged (fail open): the limiter protects capacity, and must not be the
    reason that job submission stops working. The cost is that limits are not enforced meanwhile.
    """
    with get_pool().connection() as conn:
        tenant = conn.execute(
            "SELECT burst_rps, points_quota_per_hour FROM tenants WHERE id = %s",
            (body.tenant_id,),
        ).fetchone()
    if tenant is None:
        raise HTTPException(status_code=404, detail="tenant not found")
    burst_rps, quota = tenant
    tenant_id = str(body.tenant_id)
    cost = ratelimit.JOB_TYPE_COST[body.job_type]

    try:
        # Bucket: holds burst_rps tokens and refills burst_rps per second.
        allowed, _, retry_after = ratelimit.check_burst(
            tenant_id, "POST /jobs", burst_rps, burst_rps
        )
        if not allowed:
            # cost 0 only reads how many points are left, so the headers on a 429 are accurate.
            _, left, _ = ratelimit.charge_points(tenant_id, 0, quota)
            reject(quota, left, retry_after, "burst", "burst limit exceeded")
        allowed, left, reset_in = ratelimit.charge_points(tenant_id, cost, quota)
        if not allowed:
            # No refund of the burst token spent above: simpler, and the request did arrive.
            reject(quota, left, reset_in, "quota", "hourly quota exceeded")
    except (redis.RedisError, OSError):
        log.warning("rate limiter unavailable, allowing the request", exc_info=True)
        return {}
    return ratelimit.rate_limit_headers(quota, left, ratelimit.is_near_limit(quota, left))


def reject(quota: int, left: int, retry_after: int, reason: str, detail: str) -> None:
    """Raise the 429 response, with the rate-limit headers, Retry-After and the reason."""
    headers = ratelimit.rate_limit_headers(quota, left, ratelimit.is_near_limit(quota, left))
    headers["Retry-After"] = str(max(retry_after, 1))
    headers["RateLimit-Reason"] = reason
    raise HTTPException(status_code=429, detail=detail, headers=headers)


@app.post("/jobs")
def create_job(body: JobIn, response: Response) -> dict:
    # Order: body is validated first (422), then tenant lookup (404), burst, quota (429).
    # A rejected request returns before the INSERT, so it never creates a job.
    response.headers.update(apply_rate_limit(body))
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
