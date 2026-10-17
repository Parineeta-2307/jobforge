# jobforge

A multi-tenant job scheduler with per-tenant rate limiting, built at small scale to practice the
design problems that large platforms solve: durable scheduling, leases, retries, idempotency,
fairness between tenants, and quota-based rate limiting.

> Status: scaffold. Replace this paragraph with one honest sentence about what works.

## What it does

TODO: 3 to 4 sentences. A tenant defines a job (one-off or recurring). The scheduler creates runs.
Workers claim runs, execute them and record the outcome. The API limits how much each tenant can submit.

## Architecture

TODO: add a diagram (draw.io or Excalidraw export, saved in docs/) showing API, scheduler,
workers, Postgres and Redis, and what each one owns.

| Component | Responsibility |
|---|---|
| API (FastAPI) | Tenants, jobs, run status. Rate limiting in front of job submission. |
| Scheduler | Turns job definitions into runs when they come due. SLA verifier. |
| Workers (N copies) | Claim runs with a lease, execute, retry with backoff, dead-letter. |
| Postgres | Source of truth for jobs, runs, attempts, dead letters. |
| Redis | Rate limiter state only (token buckets, hourly quota counters). |

## Run it

```bash
docker compose up --build              # api on :8000
docker compose up --build --scale worker=3
make test                              # needs: docker compose up -d postgres redis
make load                              # Locust UI on :8089
```

## Design decisions

Short version here, full reasoning in [docs/DESIGN.md](docs/DESIGN.md).

TODO: 5 bullets, each one decision and the trade-off you accepted.

## Results

Only fill this in with numbers you measured yourself, and say what machine they came from.

| Measurement | Result | How measured |
|---|---|---|
| API throughput | TODO | Locust, N users |
| p95 latency | TODO | Locust |
| Runs lost or doubled after random worker kills | TODO | tests/test_worker_kill.py |
| Recovery time after a worker kill | TODO | same test |

## Limitations

TODO: what this does not do (single Postgres, single region, no auth, and so on). Honest limits
read better in an interview than a claim of scale.
