# jobforge

[![ci](https://github.com/Parineeta-2307/jobforge/actions/workflows/ci.yml/badge.svg)](https://github.com/Parineeta-2307/jobforge/actions/workflows/ci.yml)

A multi-tenant job scheduler with per-tenant rate limiting, built to practise the design problems of
large platforms: durable scheduling, leases, retries, idempotency, fairness between tenants and
quota-based rate limiting.

> **Status:** the scheduler, workers, retries, dead letters, redrive, fair claiming and the Redis rate
> limiter work and are tested (83 tests pass against real PostgreSQL and Redis, plus a chaos test that
> kills worker processes). It runs on one machine. It is not deployed and has no authentication.

## What it does

A tenant defines jobs through an HTTP API; a job is one-off or recurring. The scheduler turns a job
into runs (one run is one scheduled occurrence). Workers claim runs under a time-limited lease,
execute them and record the outcome. If a worker dies, its lease expires and another worker finishes
the run; if a run keeps failing it moves to a dead-letter table and can be redriven explicitly. The API
limits how much each tenant can submit, and claiming is fair, so one noisy tenant cannot starve the others.

## Architecture

```mermaid
flowchart LR
    Client -->|POST /jobs| API
    API -->|burst + quota, Lua| Redis[(Redis)]
    API -->|jobs, runs| PG[(PostgreSQL)]
    Scheduler -->|create_due_runs| PG
    Workers["Workers (N copies)"] -->|claim with SKIP LOCKED, heartbeat, complete / fail| PG
```

| Component | Responsibility |
|---|---|
| API (FastAPI, `app/main.py`) | Tenants, jobs, run status, redrive. Rate limiting in front of `POST /jobs`. |
| Scheduler (`app/scheduler.py`) | Creates a run when a job comes due. Safe to run twice. |
| Workers (`app/worker.py`, N copies) | Claim a run with a lease, execute it, heartbeat, then complete, retry or dead-letter it. |
| PostgreSQL | Source of truth for jobs, runs, attempts, effects and dead letters. It is also the queue. |
| Redis | Rate limiter state only (token buckets, hourly counters). Nothing durable. |

## Run it (Windows, PowerShell, no Docker)

```powershell
git clone https://github.com/Parineeta-2307/jobforge.git
cd jobforge
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
```

Create the database once (needs the PostgreSQL superuser), then apply every migration in order:

```powershell
psql -U postgres -c "CREATE USER jobforge WITH PASSWORD 'jobforge';"
psql -U postgres -c "CREATE DATABASE jobforge OWNER jobforge;"
Get-ChildItem migrations\*.sql | Sort-Object Name | ForEach-Object { psql -U jobforge -d jobforge -f $_.FullName }
```

Start each of these in its own terminal (run the worker three times). Redis is optional: without it
the rate limiter fails open (requests are accepted and a warning is logged).

```powershell
$env:DATABASE_URL = "postgresql://jobforge:jobforge@localhost:5432/jobforge"
$env:REDIS_URL = "redis://127.0.0.1:6379/0"      # optional
python -m uvicorn app.main:app
python -m app.scheduler
python -m app.worker
```

```powershell
python -m scripts.demo_100_jobs                  # 100 jobs, 3 workers, checks none ran twice
pytest                                           # the normal suite (Redis tests skip without Redis)
pytest -m chaos -s                               # kills workers at random, about a minute
```

Open http://127.0.0.1:8000/ for a small read-only status page (run counts, tenants, newest runs, dead letters;
plain HTML, CSS and JS in `app/static`, no framework). The screenshot below uses demo data. It has no authentication, so it shows every tenant.

![status page with demo data](docs/status-page.jpg)

`docker-compose.yml` and the `Dockerfile` exist but were never tested.

## Design decisions (full reasoning in [decisions.md](decisions.md))

1. **PostgreSQL is the queue** (`SELECT ... FOR UPDATE SKIP LOCKED`). One system holds the durable state
   and the queue, so there is no gap between "run created" and "run enqueued", and many workers claim
   different rows without blocking each other. *Trade-off:* it will not match Kafka or SQS throughput; polling adds latency.
2. **Leases plus fencing tokens, not "mark as running".** A claim sets a lease; a heartbeat extends it; a dead
   worker's lease expires. Every claim bumps `fencing_token` and every write is `WHERE fencing_token = mine`, so a
   stalled worker that wakes up late cannot overwrite the newer worker's result. *Trade-off:* a worker that finishes just after its lease
   expires has its result discarded and the run repeats.
3. **At-least-once delivery with an idempotent effect.** Exactly-once is not possible across a crash, so a run
   may execute twice. The effect is a row in `job_effects` keyed by the run id (`ON CONFLICT DO NOTHING`), so it is visible once.
   *Trade-off:* every job's side effect has to be made idempotent by the person who writes the job.
4. **Fair claiming with a per-tenant cap.** `claim_run` picks the tenant with the fewest leased runs (ties: the one
   that has waited longest) and never exceeds `max_concurrency`, under a tenant row lock with a re-count so two workers cannot take the last
   slot. *Trade-off:* the pick query costs one probe per tenant; a partial index (migration 003) made it about 50x faster on test data, but it still grows with the tenant count.
5. **Rate limiting in Redis Lua, failing open.** A token bucket (burst) and an hourly points quota, modelled on Jira Cloud's
   published design, each in one Lua script so the read-modify-write is atomic. If Redis is down, requests are accepted
   and a circuit breaker stops waiting on it. *Trade-off:* while Redis is down limits are not enforced; the quota is a fixed window, so a
   tenant can use up to twice the quota across an hour boundary.

## Results

All numbers are from one machine: **HP Pavilion x360 14-dh1xxx, Intel Core i3-10110U (2 cores/4 threads),
7.8 GB RAM, Windows 11 Home build 26300, PostgreSQL 16.15.** The API, Locust, PostgreSQL and Redis shared that laptop, so these are
"on this machine" numbers, not capacity claims. Each row links to the saved raw output in [docs/results](docs/results).

| Measurement | Result | Source |
|---|---|---|
| `POST /jobs` throughput, 20 users, Redis active | 232.1 to 235.9 requests/s (3 runs, 60 s each) | [load_baseline_redis](docs/results/) runs 1 to 3 |
| `POST /jobs` latency, same runs | p50 24 to 25 ms, p95 56 to 57 ms, p99 69 to 71 ms, 0 failures | same files |
| Same test, Redis not running (limiter failing open) | 151.8 to 155.9 requests/s, p99 1.1 to 1.2 s | [load_baseline](docs/results/) runs 1 to 3 |
| Over-quota tenant (50 points/hour, 5/s burst) | 100% of requests answered 429 after a 10 s warm-up (its quota was spent during the warm-up) | [load_overquota](docs/results/) runs 1 to 3 |
| Quiet tenant sharing the API with it | 0% 429, p95 41 to 61 ms | same files |
| Runs lost after worker kills | **0** (5 scenarios, 300 runs, 134 hard kills) | [chaos_2026-10-18.json](docs/results/chaos_2026-10-18.json) |
| Runs doubled after worker kills | **0** (one effect row and one successful attempt per run; no stale overwrite) | same file |
| Recovery after a kill (to the next attempt) | min 2.14 s, median 2.9 s, max 3.81 s, with a 3 s lease and 1 s heartbeat | same file |
| 100 jobs, 3 workers | 100 succeeded, none twice, 37.9 s (one run) | [demo_100_jobs](docs/results/demo_100_jobs_2026-10-18.txt) |
| Fairness: quiet tenant behind a 300-run backlog | its 10 jobs finished in 11.4 s in 3 of 3 runs, while 27 to 31 of the 300 backlog runs had finished | [fairness runs](docs/results/) |
| Tenant-picking query, 7,900 waiting runs | 43 to 52 ms without the index, 0.5 to 0.9 ms with it (the planner ignored the index in 1 of 6 runs) | [explain_claim](docs/results/) files |

Notes: the chaos recovery time is roughly the lease length plus polling, so it scales with `LEASE_SECONDS`.
The fairness row has no "before" number, because I did not run the old claiming. Details and caveats for each row are in `decisions.md`.

## Limitations

- No authentication; `GET /runs/{id}` and redrive are not scoped to a tenant.
- One PostgreSQL instance, one region, not deployed. Only tested on Windows, on one laptop.
- After scheduler downtime missed occurrences are **skipped** (only the latest due one is created). That bounds load but loses runs; the SLA
  verifier that would make the loss visible is not built.
- Rate limiting fails open when Redis is down. The hourly quota is a fixed window (up to 2x the quota across an hour boundary).
- The claim query probes every tenant; run history is never deleted (`retention_days` is stored but not enforced).
- Verified on Redis 5.0 (local) and 7 (CI). The Redis tests do not run without a reachable Redis.
- The chaos test is evidence, not a proof: kills hit random moments, and the specific races are covered by unit tests.

## What would change at 10x and 100x

The first bottleneck is the claim path: every claim probes each tenant and every state change writes to the one `job_runs` table. At 10x:
more workers, a connection pooler in front of PostgreSQL, and a counter table instead of counting leased runs. At 100x: one queue per tenant (or per
shard) so a worker picks a tenant first, partition `job_runs` by time, move the queue out of PostgreSQL, and run several scheduler shards.
The scheduler also scans every recurring job each tick; a `next_run_at` column with an index would fix that.

## Repository

`app/` (API, scheduler, worker, rate limiter), `migrations/` (001 to 003), `tests/`, `scripts/` (demo, chaos, EXPLAIN, fairness),
`loadtest/` (Locust), `docs/` (SPEC, DESIGN, BUILD_PLAN, saved results), `decisions.md` (the reasoning log).
