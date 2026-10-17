"""HTTP API. Endpoints are stubs: implement them in the order given in docs/BUILD_PLAN.md."""

import psycopg
from fastapi import FastAPI, HTTPException
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
def create_job() -> dict:
    # TODO (Weekend 1): insert into jobs. Weekend 2: put the rate limiter in front of this route
    # (see app/ratelimit.py) and return X-RateLimit-* headers and 429 + Retry-After.
    raise HTTPException(status_code=501, detail="not implemented")


@app.get("/runs/{run_id}")
def get_run(run_id: str) -> dict:
    # TODO (Weekend 1): return the run and its attempts.
    raise HTTPException(status_code=501, detail="not implemented")


@app.post("/dead-letters/{run_id}/redrive")
def redrive(run_id: str) -> dict:
    # TODO (Weekend 2): audited redrive of a dead-lettered run.
    raise HTTPException(status_code=501, detail="not implemented")