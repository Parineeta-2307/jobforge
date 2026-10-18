# jobforge: specification for the rest of the project (Weekend 2 and 3)

This file is the single source of truth for what to build next. It was written after reviewing the
repository as it stood on 2026-10-18 (Weekend 1 finished, nothing committed yet). Read CLAUDE.md first
for the purpose, architecture, schema and rules. Read decisions.md for the reasoning behind what
already exists. This file says what remains, in order, with acceptance criteria.

## 0. How to work from this file

1. Do one task at a time, in the order of section 4. Do not start a task before the previous one
   passes its acceptance criteria.
2. For every task, finish with this checklist, and show the output of each item:
   - `ruff check .` passes.
   - `pytest` (plain, not `python -m pytest`) passes with `DATABASE_URL` set. Tasks that touch Redis
     also need `REDIS_URL`.
   - New behaviour has tests that would fail without it.
   - `decisions.md` has a new entry: what was chosen, why, the trade-off accepted, alternatives
     rejected, and two likely interview questions.
   - The task's box in `docs/BUILD_PLAN.md` is ticked and the "Current status" section in CLAUDE.md
     is updated.
3. Understanding gate. Before a task counts as done, ask Pari three questions about the code you
   wrote for it (one about how it works, one about what happens when it fails, one about the
   trade-off). Add her answers, in her own words, under "In my words" in that decisions.md entry.
   If she cannot answer, explain it and ask again. The goal of this project is that she can explain
   every line in an interview.
4. Ask before: adding a dependency, editing `.github/`, editing `migrations/001_init.sql`. A schema
   change is always a new numbered migration file, shown to Pari before it is applied.
5. Pari makes the commits. Suggest one conventional commit message per task.
6. Never invent a measurement. A number may appear in the README, a commit message, or a docs file
   only if a script or test run produced it, the raw output is saved, and the machine is named.

## 1. Purpose and what a reviewer should conclude

jobforge is a small multi-tenant job scheduler built to practice the design problems of large
platforms, and to be explained in SDE interviews. A reviewer reading the README should be able to
conclude, from evidence in the repo: runs are claimed safely by many workers, a worker that dies does
not lose or double a result, a noisy tenant cannot starve a quiet one, and the API limits each
tenant. Every one of those four claims must end up backed by a named test or a saved result file.

Target companies and what each task mirrors, so the work stays interview-relevant:
- Atlassian (Jira Cloud): burst-plus-hourly-points rate limiting with X-RateLimit headers (W2-5).
- Amazon, Google, Databricks: leases, retries, idempotency, fairness between tenants (W2-2 to W2-4).
- Rubrik: SLA verification of missed runs (W2-7).
- Adobe, Fyers: durable state, failure handling, load behaviour (W3).

## 2. Current state (verified from the code)

Done and tested:
- Schema `migrations/001_init.sql`: tenants, jobs, job_runs, run_attempts, dead_letters.
- API: `GET /health`, `POST /tenants`, `POST /jobs`, `GET /runs/{id}`. Names are trimmed and must be
  1 to 100 characters. `POST /dead-letters/{run_id}/redrive` is a 501 stub.
- Scheduler: `create_due_runs`. One-off jobs get one run. Recurring jobs get the latest due
  occurrence on a fixed grid (`created_at + k * interval_seconds`). Missed occurrences are skipped
  after downtime. `UNIQUE (job_id, scheduled_for)` plus `ON CONFLICT DO NOTHING` makes two
  schedulers safe.
- Worker: `claim_run` (oldest first, `FOR UPDATE OF r SKIP LOCKED`), lease, `heartbeat` thread,
  `complete`, `fail` (exponential backoff, equal jitter, dead letters), `reap_expired_leases`. All
  of `heartbeat`, `complete` and `fail` are guarded by `fencing_token`.
- Job types: `noop` and `sleep` (payload `seconds`). Neither has a side effect.
- Tests: schema, jobs API, runs API, scheduler, worker, health. `scripts/demo_100_jobs.py` runs the
  100-job check. Pari has not yet re-run it herself, so no number from it may be quoted yet.
- CI (`.github/workflows/ci.yml`): installs, `ruff check .`, applies `migrations/001_init.sql` only,
  runs `pytest`. Postgres and Redis service containers exist.

Not started: per-tenant concurrency and fair claiming, idempotent side effect, Redis rate limiter,
redrive, SLA verifier, chaos test, load test, README content, DESIGN.md answers.

## 3. Problems found in review (fix as part of the tasks below)

1. CI applies only `001_init.sql`. As soon as `002` exists, CI must apply every file in
   `migrations/` in name order, or the tests will fail there. (Task W2-1.)
2. Redrive cannot simply reset `attempt` to 0. `run_attempts` rows are matched by
   `(run_id, attempt)` in `complete`, `fail` and `reap_expired_leases`, so a reset would make two
   rows match the same UPDATE. Instead add `job_runs.attempt_base` and compare
   `attempt - attempt_base` with `max_attempts`. (Task W2-3.)
3. `fail` and the reaper insert into `dead_letters` with `ON CONFLICT DO NOTHING`. After a redrive
   the row already exists, so a second death would keep the stale reason and `redriven_at`. The
   second death must overwrite them. (Task W2-3.)
4. `POST /tenants` accepts no quota fields, so a load test cannot create a tenant with a small
   quota. Add optional `points_quota_per_hour`, `burst_rps` and `max_concurrency`. (Task W2-5.)
5. `JOB_TYPE_COST` in `app/ratelimit.py` lists `noop`, `http_call`, `report`. The real job types are
   `noop`, `sleep` and (after W2-2) `record`. (Task W2-5.)
6. The module docstring of `app/main.py` still says endpoints are stubs. `docs/BUILD_PLAN.md`
   checkboxes are not ticked. `README.md` is still the scaffold. (Task W3-5.)
7. The `tenants.name` rule (not blank) exists only in the API. Add a database CHECK. (Task W2-1.)
8. The SLA verifier cannot look for missing rows, because the skip policy never creates them for
   missed occurrences. It must compute the expected occurrences and compare. (Task W2-7.)
9. Known limitations to state honestly in the README, not fix: no authentication, `GET /runs/{id}`
   is not scoped to a tenant, one Postgres instance, fixed-window hourly quota (allows up to 2x the
   quota across an hour boundary), the claim query counts leased runs per tenant on every claim.

## 4. Tasks, in order

Time estimates are for Pari working with Claude Code.

### W2-1. Migration 002 and CI applies all migrations (about 1 h)

Create `migrations/002_weekend2.sql` (idempotent: `IF NOT EXISTS`, safe to run twice):
- `ALTER TABLE job_runs ADD COLUMN IF NOT EXISTS attempt_base integer NOT NULL DEFAULT 0`.
- Table `job_effects`: `run_id uuid PRIMARY KEY REFERENCES job_runs(id) ON DELETE CASCADE`,
  `tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE`, `created_at timestamptz NOT
  NULL DEFAULT now()`. The primary key on `run_id` is the idempotency guarantee.
- Table `redrive_log`: `id bigserial PRIMARY KEY`, `run_id uuid NOT NULL REFERENCES job_runs(id) ON
  DELETE CASCADE`, `previous_reason text NOT NULL`, `note text`, `redriven_at timestamptz NOT NULL
  DEFAULT now()`.
- `ALTER TABLE tenants ADD CONSTRAINT tenants_name_not_blank CHECK (btrim(name) <> '')`, written so
  that running the file twice does not fail (check `pg_constraint` first, or use a `DO` block).

Show the file to Pari before applying it to her local database. Then change the "Apply schema" step
of `.github/workflows/ci.yml` to a loop over `migrations/*.sql` in sorted order (Pari edits this
file herself, give her the exact YAML). `docker-compose.yml` already mounts the whole folder.

Acceptance: applying the files twice in a row to a fresh database succeeds. `tests/test_schema.py`
gains tests for the blank-name CHECK and for `job_effects` rejecting a second row for one run id.
CI is green on a pushed branch.

### W2-2. Idempotent side effect (about 1.5 h)

Goal: prove the answer to DESIGN.md Q4. Delivery is at-least-once, so a run may execute twice, but
its visible effect must happen once.

Spec:
- Add job type `record` (extend the `Literal` in `JobIn`). Payload may contain `seconds` (default 0):
  sleep that long to hold the lease, then write the effect.
- Writing the effect: `INSERT INTO job_effects (run_id, tenant_id) VALUES (%s, %s) ON CONFLICT
  (run_id) DO NOTHING`, using the run id as the key. A second execution of the same run inserts
  nothing.
- Optional job type `flaky` for tests and the load test: payload `fail_times` (int). It raises a
  transient error while `run["attempt"] <= fail_times`, then succeeds. Not shown in the README as a
  feature, used only to drive retry and dead-letter paths end to end.
- `execute` currently receives the run dict. It needs its own connection from the pool for the
  effect insert (it runs outside the claim transaction).

Acceptance and tests:
- Executing the same `record` run twice leaves exactly one `job_effects` row.
- A `record` run whose lease expires mid-execution and is finished by a second worker still leaves
  exactly one row.
- A `flaky` job with `fail_times = 2` and `max_attempts = 5` ends `succeeded` on attempt 3 with 3
  `run_attempts` rows (2 `failed`, 1 `succeeded`). With `fail_times = 9` it ends `dead` with a row
  in `dead_letters`.

Write in decisions.md why the effect and the "mark succeeded" step cannot be one atomic action for
an external effect, and why the unique key is therefore the right tool.

### W2-3. Redrive (about 1.5 h)

Endpoint `POST /dead-letters/{run_id}/redrive`. Change the path parameter type to `UUID`. Optional
JSON body `{"note": string | null}`.

One transaction:
1. `SELECT ... FROM job_runs WHERE id = %s FOR UPDATE`. 404 if it does not exist. 409 if its state is
   not `dead` (this also makes a double click safe).
2. Insert into `redrive_log` (run id, the old `dead_letters.reason`, the note).
3. Update the run: `state = 'ready'`, `available_at = now()`, `attempt_base = attempt`, `finished_at
   = NULL`, lease columns NULL. Do not touch `attempt`, `fencing_token`, or `run_attempts`; the
   history stays intact.
4. Update `dead_letters.redriven_at = now()`.
5. Return the run (same shape as `GET /runs/{id}`).

Related changes in `app/worker.py`:
- `claim_run` selects `attempt_base` and returns it in the run dict.
- `fail`: dead when `attempt - attempt_base >= max_attempts`.
- `reap_expired_leases`: same comparison.
- Both dead-letter inserts become `ON CONFLICT (run_id) DO UPDATE SET reason = EXCLUDED.reason,
  moved_at = now(), redriven_at = NULL`.

Tests: dead run is redriven, becomes claimable, gets a fresh set of `max_attempts` attempts, and
`run_attempts` keeps counting up (attempt numbers never repeat). A run that dies again after a
redrive shows the new reason and two `redrive_log` rows after a second redrive. Succeeded run: 409.
Unknown id: 404. Malformed id: 422. Two concurrent redrives of one run: exactly one 200.

### W2-4. Per-tenant concurrency and fair claiming (about 2 h)

Goal: DESIGN.md Q8. One tenant with 10,000 ready runs must not keep other tenants from getting
workers, and no tenant may have more than `max_concurrency` runs leased at once.

Invariants to enforce in `claim_run`:
1. A tenant whose number of `leased` runs is at or above its `max_concurrency` is not eligible.
2. Among eligible tenants that have a claimable run, choose the one with the fewest leased runs now;
   break ties by the oldest `available_at`. Then take that tenant's oldest claimable run.
3. Two workers claiming at the same instant must not push a tenant past its cap. A plain count
   followed by an update has a race. Pari chooses and records one of: lock the tenant row (`SELECT
   ... FROM tenants WHERE id = %s FOR UPDATE`) and re-count after the lock, or take a per-tenant
   advisory lock (`pg_advisory_xact_lock`). Recommended: the tenant row lock, because it is simple
   and visible in the schema. The cost: claims for one tenant are serialised for the length of the
   claim transaction, which is short.
4. Keep `FOR UPDATE OF r SKIP LOCKED` on the run row, the `enabled` check, and the `available_at <=
   now()` check.

Do not use `SKIP LOCKED` on the tenant row: with one busy tenant, all but one worker would see "no
work" and idle for `POLL_SECONDS`.

Tests (these are the proof, write them so they fail on the old `claim_run`):
- Cap: tenant with `max_concurrency = 2` and 5 ready runs. Claim until `None`. Exactly 2 claims.
  Completing one frees a slot for exactly one more.
- Cap under concurrency: 20 threads, each with its own connection, claim at once for a tenant with
  cap 3. Never more than 3 leased at any moment.
- Fairness: tenant A has 500 ready runs, tenant B has 5, same cap. In the first 12 claims B gets
  at least 5. With the old query B would wait behind A.
- Idle tenant: when A is at its cap and B has nothing, `claim_run` returns `None` (the cap holds
  even if workers are idle).

Write the query plan reasoning in decisions.md: run `EXPLAIN (ANALYZE)` on the new claim query with
a few thousand rows and keep the output. State the cost honestly: it counts leased runs per
candidate tenant, so cost grows with tenants. At 100x the fix is a counter table or per-tenant
queues; say so.

### W2-5. Redis rate limiter on `POST /jobs` (about 3 h)

Goal: DESIGN.md Q9, modelled on Jira Cloud's published design (burst limit plus hourly points
quota). Call it "modelled on", never "same as".

Setup: Redis is a hosted free tier (no Docker). `REDIS_URL` comes from the environment, usually
`rediss://default:PASSWORD@host:port`. The password lives only in the shell environment or a local
untracked `.env`; check `.gitignore` ignores `.env`. CI already has a Redis service. Tests that need
Redis skip when `REDIS_URL` is unset or unreachable. `redis==5.2.1` is already pinned.

Behaviour:
- Burst: token bucket per (tenant, endpoint), capacity `tenants.burst_rps`, refilled at
  `burst_rps` tokens per second, cost 1 per request. State in Redis, updated by one Lua script
  (`EVAL`/`register_script`) so the read, refill, check and write are atomic. The script uses
  Redis `TIME` for the clock, returns `allowed`, tokens left, and milliseconds until the next token,
  and sets a key expiry so idle buckets disappear.
- Quota: points per tenant per UTC hour, limit `tenants.points_quota_per_hour`. One Lua script adds
  the request cost only if the total stays within the limit, otherwise changes nothing. Key includes
  the UTC hour, expiry set to the end of that hour plus a few minutes. This is a fixed window;
  record the boundary-burst trade-off in decisions.md.
- Cost per job type in `JOB_TYPE_COST`: `noop`, `sleep`, `record` (and `flaky` if added). Pari picks
  the numbers and explains why in the README. They are design choices, not measurements.
- Order: validate the body, look up the tenant (404 if unknown, so fake tenant ids never create
  Redis keys), check burst, then quota. Record whether a burst token is refunded when the quota
  check fails (recommended: no refund, simpler, and say so).
- Responses from `POST /jobs`, allowed or not: `X-RateLimit-Limit` (hourly quota),
  `X-RateLimit-Remaining` (points left), `X-RateLimit-NearLimit: true` when remaining is at or below
  20% of the quota. A rejection is HTTP 429 with `Retry-After` (whole seconds, rounded up: time to
  the next token for burst, time to the next hour for quota), and a `RateLimit-Reason` header with
  `burst` or `quota`.
- A rejected request must not insert a job.
- Redis unavailable: Pari decides fail-open (accept, log loudly) or fail-closed (503). Recommended
  fail-open with a warning log, because the limiter protects capacity and must not take submissions
  down. Record the risk. Test the chosen behaviour.
- Extend `TenantIn` with optional `points_quota_per_hour`, `burst_rps`, `max_concurrency` (integers,
  at least 1, sensible upper bounds) so tests and the load test can create tenants with small
  limits. The database defaults still apply when omitted.
- Update `JOB_TYPE_COST` and delete the stale placeholder types.

Tests:
- Burst: capacity 3, refill 1 per second: 3 requests pass, the 4th gets 429 with `Retry-After >= 1`
  and reason `burst`; after about 1.1 s one more passes.
- Concurrency (the important one): 50 threads hit one tenant's bucket of capacity 10 at once. Exactly
  10 are allowed. This proves the Lua script is atomic.
- Optional demonstration for the README: a naive GET-then-SET limiter under the same 50 threads
  allows more than 10. Keep it in a test or script named clearly as the "what goes wrong without
  Lua" example, not used by the app.
- Quota: with quota 10 and cost 5, two requests pass, the third is 429 with reason `quota` and a
  `Retry-After` up to 3600. Hour reset: inject the hour key function (a small, replaceable function)
  and show a new hour allows requests again.
- Headers: present on both 200 and 429. `NearLimit` flips to true at the threshold.
- Isolation: one tenant exhausting its limits does not change another tenant's headers or results.
- A 429 inserts no job row.

### W2-6. Housekeeping before moving on (about 0.5 h)

Update the docstring of `app/main.py`, the `BUILD_PLAN.md` boxes, and CLAUDE.md "Current status".
Run the whole 100-job demo again with the new claim query. Pari runs `python -m
scripts.demo_100_jobs` herself and keeps the raw output.

### W2-7 (extra, cut first). SLA verifier (about 2 h)

`verify_sla(conn)` in `app/scheduler.py`, called from the scheduler loop every
`SLA_CHECK_SECONDS` (config, default 30), logging a warning per finding and returning the list.
Findings, each a dict with `kind`, `job_id`, `run_id` (if any), `since`:
- `stuck_lease`: a run `leased` with `lease_expires_at` older than `SLA_GRACE_SECONDS` (no worker is
  alive to reap it).
- `late_run`: a run `ready` or `retry_wait` with `available_at` older than `SLA_LATE_SECONDS`
  (workers are not keeping up, or the tenant is at its cap for too long).
- `missed_occurrence`: for each enabled recurring job, grid times in the last
  `SLA_LOOKBACK_SECONDS` that have no `job_runs` row (the scheduler was down). Compute the expected
  grid times with `generate_series` rather than looking for missing rows. Only grid times after the
  job's `created_at` count.
Tests: one per kind, plus "healthy system reports nothing". In decisions.md explain how this ties to
the skip-missed-occurrences policy: skipping loses the run, the verifier is what makes the loss
visible.

### W3-1. Chaos test (about 3 h)

File: `tests/test_worker_kill.py`, marker `chaos` (register it in `pyproject.toml` and exclude it
from the default run with `addopts = "-q -m 'not chaos'"`). It spawns real worker processes, so it
runs against a separate clean database (`jobforge_test`), selected by `DATABASE_URL`. Otherwise the
workers also claim leftover rows from development.

Scenario:
- Add a `WORKER_ID` environment override in `app/worker.py` so the test knows each child's id.
- Child environment: `LEASE_SECONDS=3`, `HEARTBEAT_SECONDS=1`, `POLL_SECONDS` small (0.2 or 1).
  `POLL_SECONDS` is read as an integer today; either make it a float or keep it 1 and say so.
- 3 workers via `subprocess.Popen([sys.executable, "-m", "app.worker"])`. Insert 60 `record` runs
  directly (not through the scheduler) with `payload.seconds` of 1 to 2 and `max_attempts = 10`,
  so kills cannot dead-letter them.
- Every 2 seconds, kill one random worker (`Popen.kill()`, which is a hard kill, no cleanup) and
  start a replacement, recording the kill time and the id.
- Wait until all runs are terminal or 120 s pass.

Assertions (the three claims):
- None lost: every run is `succeeded`, none `leased`, `ready` or `retry_wait` at the end.
- None doubled: exactly one `job_effects` row per run, and exactly one `run_attempts` row with
  outcome `succeeded` per run.
- No stale overwrite: for each run, the `succeeded` attempt is its highest attempt number, and the
  fencing token stored on the run equals the number of claims (attempt count).
- Print: runs, kills, total attempts, attempts with outcome `lease_expired`, and recovery time per
  kill (time from kill to the next `run_attempts.started_at` for the run that worker held).

Also write `scripts/chaos_report.py` that runs the same scenario 5 times and writes
`docs/results/chaos_<date>.json` with every run's numbers and the machine description. Report all 5
in the README (min, median, max for recovery time), not the best one. State `LEASE_SECONDS` next to
the recovery number, because recovery time is roughly the lease length plus polling.

Expected, not promised: lost 0, doubled 0. If the test finds a bug, that is a result: fix it, record
it in decisions.md, and keep the failing test.

### W3-2. Scheduler race check (about 1 h, optional)

Two scheduler processes running together for 30 s against a recurring job with a 2 s interval, then
one killed and restarted. Assert: no duplicate `(job_id, scheduled_for)` rows (the unique key
guarantees it, the test proves the code path has no error), the number of runs is close to
`30 / 2`, and after a 20 s stop the restart creates one run, not ten (the skip policy).

### W3-3. Load test with Locust (about 2 h)

Update `loadtest/locustfile.py` (Locust is already pinned). Create tenants in `on_start` through the
API (needs W2-5's optional quota fields); delete them when the test quits.

Scenarios, each with a stated duration, user count and spawn rate, the first 10 s excluded as warm-up:
1. Baseline: one tenant with a very large quota, 20 users, 60 s, `POST /jobs` with `noop`. Record
   requests per second, p50, p95, p99 latency, failure percentage.
2. Over-quota: a noisy tenant with a small quota and burst limit, and a quiet tenant with normal
   limits, both submitting. Record for each tenant: requests, 429 count and 429 percentage. The
   noisy tenant's 429 share should be high and the quiet tenant's should be 0. Record the quiet
   tenant's p95 next to the baseline p95.
3. Fairness end to end (script `scripts/fairness_demo.py`, scheduler and 3 workers running): the noisy
   tenant has 300 `sleep` runs queued, then the quiet tenant submits 10. Record how long until all
   10 of the quiet tenant's runs succeed. Without the fairness change in W2-4 they would wait behind
   the backlog; with it, they should not. Run it once on the old claim query (git stash or a flag)
   only if Pari wants a before/after number.

Measurement rules: run each scenario 3 times, keep Locust's CSV files in `docs/results/`, and
report the median and range. Locust and the API share one laptop, so say so; the numbers are
"on this machine" numbers, not capacity claims. Record machine model, CPU, RAM, Windows version,
Postgres version, number of uvicorn processes, and the worker count.

### W3-4. DESIGN.md (about 1.5 h)

Pari writes an answer under each of the 10 questions in her own words, 4 to 8 sentences each, using
the code and the saved results. Claude Code may ask questions and point out mistakes, not write the
answers. Q7 must match the skip policy, Q10 must name the first bottleneck, using the `EXPLAIN` and
load test results.

### W3-5. README and cleanup (about 2 h)

README sections: one honest status sentence; what it does (3 to 4 sentences); an architecture
diagram saved as `docs/architecture.png` (draw.io or Excalidraw export) with the component table;
how to run it on Windows without Docker (PowerShell steps from CLAUDE.md, including starting the API,
scheduler and three workers); five design decisions each with its trade-off; the results table filled
only from files in `docs/results/` (link each row to its file); limitations (list from section 3,
item 9, plus Redis fail-open and the skip policy); what would change at 10x and 100x. Remove the
unused Docker sections only if they were never tested, or mark them untested. Fix the `main.py`
docstring. Tick every box in `docs/BUILD_PLAN.md`. Confirm CI is green and add the badge.

### W3-6. Resume facts sheet (about 0.5 h)

Create `docs/RESUME_FACTS.md` with only things that are true and checkable. For each fact: the
claim in one plain sentence, the test name or results file that supports it, and the exact command
that reproduces it. Include only measured numbers that appear in `docs/results/`. Pari hands this
file, plus the repo link, to whoever writes her resume bullets. Do not write resume bullets
yourself and do not round up.

## 5. Out of scope (do not build)

Authentication, a web UI, Kubernetes, Celery or any other queue library, an ORM, an async rewrite,
catch-up scheduling (the skip policy stays unless Pari changes it), multi-region anything, and any
job type beyond `noop`, `sleep`, `record` and the test-only `flaky`.

## 6. Reference: commands (Windows PowerShell)

```
cd E:\Coding\jobforge
.venv\Scripts\Activate.ps1
$env:DATABASE_URL = "postgresql://jobforge:jobforge@localhost:5432/jobforge"
$env:Path += ";C:\Program Files\PostgreSQL\16\bin"; $env:PGPASSWORD = "jobforge"
python -m uvicorn app.main:app          # API
python -m app.scheduler                 # scheduler (own terminal)
python -m app.worker                    # worker (run three, own terminals)
python -m scripts.demo_100_jobs         # the 100-job check
ruff check .
pytest
```

Chaos database, one time:
```
psql -U postgres -c "CREATE DATABASE jobforge_test OWNER jobforge;"
Get-ChildItem migrations\*.sql | Sort-Object Name | ForEach-Object { psql -U jobforge -d jobforge_test -f $_.FullName }
$env:DATABASE_URL = "postgresql://jobforge:jobforge@localhost:5432/jobforge_test"
pytest -m chaos
```
