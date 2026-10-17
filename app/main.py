"""HTTP API. Endpoints are stubs: implement them in the order given in docs/BUILD_PLAN.md."""

from fastapi import FastAPI, HTTPException

app = FastAPI(title="jobforge")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/tenants")
def create_tenant() -> dict:
    # TODO (Weekend 1): insert into tenants, return the row.
    raise HTTPException(status_code=501, detail="not implemented")


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
