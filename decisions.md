# Decisions log

Every decision made while building jobforge, with the reasoning and the trade-off, written so each
entry can be turned into an interview answer. Newest section at the bottom. Format per entry:
**Decision**, **Why**, **Alternatives rejected**, **Trade-off / what to say if pushed**.

Who decided: **[spec]** came from the project brief, **[claude]** was my judgement call while
implementing, **[inherited]** was already in the repo when this section started.

---

## Part A: Inherited decisions (already in the repo before POST /jobs)

### A1. Plain SQL with psycopg 3, no ORM [spec]
- **Why:** The project exists to practise scheduling problems (`FOR UPDATE SKIP LOCKED`, leases,
  `ON CONFLICT DO NOTHING`). An ORM hides exactly the SQL I need to be able to explain line by line.
- **Alternatives rejected:** SQLAlchemy, Django ORM.
- **Trade-off:** More typing, and no automatic mapping from rows to objects (I index `row[0]`).
  Fine at this size; at 50 tables I would want a query layer.

### A2. Connection pool (`psycopg_pool.ConnectionPool`), created lazily in `app/db.py` [inherited]
- **Why:** Opening a Postgres connection costs a TCP + auth round trip per request. A pool reuses
  connections (min 1, max 10). It is created on first use, so `import app.main` never needs a
  database, which is why `test_health.py` passes with no Postgres.
- **Trade-off:** A global variable (`_pool`) is simple but is hidden shared state. FastAPI
  dependency injection would be cleaner but is "clever abstraction" I chose to avoid.

### A3. Postgres is the source of truth and the queue [spec, see docs/DESIGN.md Q1]
- **Why:** One system holds jobs, runs and the queue, so there is no gap between "run created" and
  "run enqueued" and no dual-write problem.
- **Trade-off:** Postgres as a queue tops out well below Kafka/SQS; polling adds latency.

### A4. Schema guarantees in `migrations/001_init.sql` [inherited, file is off-limits]
- `UNIQUE (tenant_id, name)` on `jobs`: job names are unique **per tenant**, not globally. Two
  tenants can both have a job called "nightly-report". This is what produces the 409 below.
- `tenant_id REFERENCES tenants(id) ON DELETE CASCADE`: a job cannot exist without a tenant, and
  deleting a tenant deletes its jobs (tests rely on this for cleanup).
- `id uuid DEFAULT gen_random_uuid()`: the database generates ids, never the client.
- `payload jsonb NOT NULL DEFAULT '{}'`, `max_attempts DEFAULT 5`, `retention_days DEFAULT 7`,
  `version DEFAULT 1`, `enabled DEFAULT true`.
- **Why enforce in the DB and not only in Python:** Python checks can race (two requests both
  check "name free" then both insert). A unique constraint is atomic. This is the same idea as
  `UNIQUE (job_id, scheduled_for)` for runs (DESIGN.md Q6).

### A5. `POST /tenants` pattern: INSERT first, catch the constraint error [inherited]
- **Decision:** Do not `SELECT` to check for a duplicate. Just `INSERT` and translate
  `UniqueViolation` into HTTP 409.
- **Why:** Check-then-insert has a race window. Insert-and-catch lets the database be the single
  referee, and costs one round trip instead of two.
- `raise HTTPException(...) from None` because ruff rule B904 requires an explicit `from` inside an
  `except`; `from None` hides the noisy chained psycopg traceback from the response/logs.

### A6. Name trimming with `field_validator(mode="before")` [inherited]
- **Why "before":** It runs before the length check. `"   "` becomes `""` and then fails
  `min_length=1`. With the default "after" mode, `"   "` (3 chars) would pass `min_length` first.
- `isinstance(v, str)` guard: if the client sends a number, leave it alone so Pydantic produces
  the normal type error instead of my validator crashing on `.strip()` (which would be a 500).

---

## Part B: POST /jobs (Task 1)

### B1. The working tree was half-edited, so I finished it rather than starting over [claude]
`app/main.py` had uncommitted changes that did not run:
- the `from app.db import get_pool` import had been deleted (every DB route would raise `NameError`),
- `job_type` and `payload` fields of `JobIn` had landed *inside* `health()` after its `return`
  (dead code, so `JobIn` had no `job_type` and `create_job` would crash on `body.job_type`),
- two `@app.post("/jobs")` routes existed (the new one and the old 501 stub).

I rewrote the file keeping your `create_job` body (it already matched the spec: `Jsonb`, `%s`
parameters, `from None`, 409 and 404 handling), moved the fields into `JobIn`, restored the import,
and deleted the 501 stub.
- **Why two routes on one path is a bug:** FastAPI matches routes in registration order and the
  first match wins. The first one registered (the real one) would win by luck of ordering. A
  duplicate is a trap for the next edit, so there must be exactly one.
- **Lesson for interviews:** run the app/tests after every edit; a half-saved file looks fine in
  the editor but fails at import or at first request.

### B2. `JobIn` request model fields
| Field | Type / constraint | Reason |
|---|---|---|
| `tenant_id` | `UUID` | Pydantic rejects non-UUID strings with 422 before touching the DB. Passing a non-UUID string to Postgres would give a 500 (`invalid input syntax for type uuid`). |
| `name` | `str`, 1..100, trimmed | Same rules as tenant name, for consistency. |
| `job_type` | `Literal["noop", "sleep"]` | Only two job types exist in the worker plan. `Literal` makes "banana" a 422 with no extra code, and the allowed list shows up in the auto-generated OpenAPI docs. |
| `payload` | `dict`, `Field(default_factory=dict)` | Defaults to `{}`. See B3. |
| `interval_seconds` | `int \| None`, default `None`, `ge=1` | `None` = one-off job (matches the nullable column and the schema comment). `0` or negative would mean "run continuously", which would make the scheduler spin, so it is rejected. |
| `max_attempts` | `int`, default 5, `1..10` | 5 mirrors the DB default so omitting it behaves the same either way. Lower bound 1 (at least one try). Upper bound 10 protects the system: with exponential backoff, a tenant setting 1,000,000 attempts could keep a poisoned job retrying for ever and hog workers. |

- **Not accepted in the request:** `retention_days`, `version`, `enabled`, `id`, `created_at`.
  Pydantic v2's default is to **ignore** unknown fields, so a client sending `"enabled": false`
  does not error, the value is silently dropped and the DB default applies.
  - **Why not allow them:** `retention_days` is an SLA *policy* the platform owns, `version` is
    for optimistic locking on later updates, `enabled` is an operational switch. Letting a tenant
    set them at creation would let it bypass policy. The id must come from the DB.
  - **Trade-off to be honest about:** silently ignoring unknown fields can hide client typos
    (`"max_attempt": 3` would be ignored and the default 5 used). `model_config =
    ConfigDict(extra="forbid")` would turn that into a 422. I did not add it because the brief did
    not ask for it and it would change the behaviour of the working `TenantIn` pattern; it is a
    reasonable next hardening step.

### B3. `Field(default_factory=dict)` instead of `payload: dict = {}`
- **Why:** A mutable default shared between instances is a classic Python bug. Pydantic actually
  copies mutable defaults, so `= {}` would be safe in Pydantic, but `default_factory=dict` makes
  the intent obvious and is safe in any library. Easy to explain in an interview.

### B4. One INSERT with `%s` placeholders, never string formatting
- **Why:** Parameterised queries stop SQL injection: psycopg sends the SQL text and the values
  separately, so a job named `x'); DROP TABLE jobs;--` is just a string. f-strings put user input
  into the SQL text itself, which is the injection vulnerability.
- Column list is explicit (`tenant_id, name, job_type, payload, interval_seconds, max_attempts`)
  and omits the columns with DB defaults (`id`, `retention_days`, `version`, `enabled`,
  `created_at`). Leaving them out is how the defaults apply.
- **Line length:** the SQL string was ~104 chars, over the ruff limit of 100, so I split it across
  two adjacent string literals (Python joins them at compile time, no runtime cost). I put the
  split after `interval_seconds, ` so the trailing space is inside the first piece, otherwise the
  words `max_attempts)` and `VALUES` would glue together.

### B5. `Jsonb(body.payload)` for the payload
- **Why:** psycopg 3 does not know whether a Python dict should become `json`, `jsonb`, or
  something else, so passing a bare dict raises `cannot adapt type 'dict'`. Wrapping with
  `psycopg.types.json.Jsonb` says "send this as jsonb". Reading back is automatic: a `jsonb`
  column comes back as a Python dict (this is what the round-trip test checks).
- **Alternative rejected:** `json.dumps(payload)` plus `%s::jsonb`. Works, but is manual and
  easy to forget the cast.

### B6. `RETURNING id, tenant_id, name`
- **Why:** The id is generated by Postgres, so I need it back. `RETURNING` gives it in the same
  statement and same round trip as the INSERT (the alternative is a second `SELECT`, which is
  slower and can race). I return only three fields because that is all the caller needs to refer
  to the job later.
- I convert UUIDs with `str(...)` because psycopg returns `uuid.UUID` objects. FastAPI could
  serialise them, but explicit `str` keeps the response shape obvious and matches `/tenants`.

### B7. Error mapping: constraint errors become HTTP codes
| DB error | HTTP | detail | Why that code |
|---|---|---|---|
| `UniqueViolation` | **409 Conflict** | `job name already exists` | The request is valid but conflicts with existing state (same tenant + name). |
| `ForeignKeyViolation` | **404 Not Found** | `tenant not found` | The `tenant_id` the client referenced does not exist. |
| Pydantic validation | **422** | automatic | Malformed input, rejected before any SQL runs. |

- **Why 404 and not 422 for a missing tenant:** The body is well-formed; the *referenced
  resource* is missing. 404 tells the client "that tenant isn't there". Some teams use 422 or 400
  here; I followed the brief. Know that it is a convention, not a rule.
- **Why catch the DB error rather than `SELECT` the tenant first:** same reasoning as A5, no race
  (a tenant could be deleted between the check and the insert), and one round trip instead of two.
- **Why catching only these two:** Anything else (database down, bad SQL) is a real server error
  and should be a 500 and show up in logs, not be disguised as a client error.
- **Transaction behaviour:** `with pool.connection() as conn:` commits on a clean exit and rolls
  back if an exception escapes. When the INSERT fails, the `HTTPException` propagates out of the
  `with`, so the transaction is rolled back and the connection goes back to the pool in a clean
  state. This is why I raise *inside* the `with` and not after it.

### B8. Rate limiting is deliberately NOT in this route yet
- I kept the existing TODO as a comment: the Weekend 2 plan puts a Redis limiter in front of
  `/jobs`, with `X-RateLimit-*` headers and 429 + Retry-After. Building it now would be
  out of scope and untestable without Redis.

### B9. Response status stays 200, not 201 [spec]
- REST convention says `201 Created` for a successful create. The brief asked for 200 and
  `/tenants` also returns 200, so `/jobs` matches it for consistency. If asked: "I'd move both to
  201 plus a `Location` header in a cleanup pass, doing it for one endpoint only would be
  inconsistent."

---

## Part C: Tests (Task 2)

### C1. Real database, real HTTP stack: `TestClient` against Postgres, no mocks
- **Why:** The behaviours that matter here (unique constraint, foreign key, jsonb round trip) live
  in Postgres. A mocked database would only prove my mock works. This is an integration test.
- **Cost:** needs a running Postgres, tests are slower, and tests can interfere via shared data
  (handled in C3).
- `httpx` was already in `requirements-dev.txt` (version 0.28.1; Starlette's `TestClient` is built
  on it), so I did not need to add it. CI installs from that file, so nothing changes there.

### C2. Skip the module when `DATABASE_URL` is unset
- Uses `pytestmark = pytest.mark.skipif(...)` identical to `tests/test_schema.py`, so both modules
  behave the same. I verified: with the variable unset, 12 tests skip and 2 pass (no failure).
- **Note for interviews:** CI runs a Postgres service and sets `DATABASE_URL`, so these tests run
  in CI too. A skip only protects people running tests without a database.

### C3. Cleanup with a fixture, and a random suffix on every tenant name
- The database keeps data between runs, and `tenants.name` is `UNIQUE`. Without a unique name the
  second run of the suite would hit a 409 on tenant creation. `uuid4()` in the name makes each run
  independent.
- A `tenant_id` fixture creates the tenant (through the real `POST /tenants`) and, after the test,
  deletes it with SQL. `yield` marks the split; code after `yield` runs even if the test fails.
  `ON DELETE CASCADE` removes the jobs, so no separate job cleanup is needed.
- **Why not roll back a transaction like `test_schema.py` does:** the API opens its *own* pooled
  connection and commits, so a rollback in the test's connection cannot undo it. Real commits need
  real deletes.
- The unknown-tenant test uses a random `uuid4()` that was never inserted, so it needs no cleanup.

### C4. What each test proves
| Test | Proves |
|---|---|
| `test_create_job` | Happy path: 200, id present, `tenant_id` and `name` echoed. |
| `test_same_job_twice_is_409` | The `UNIQUE (tenant_id, name)` constraint is mapped to 409 with the right detail text. |
| `test_unknown_tenant_is_404` | The foreign key violation is mapped to 404. |
| `test_bad_job_type_is_422` | `Literal` rejects `"banana"`. |
| `test_blank_name_is_422` | The "before" validator trims `"   "` so min_length fails. |
| `test_interval_zero_is_422` | `ge=1` on `interval_seconds`. |
| `test_max_attempts_11_is_422` | `le=10` on `max_attempts`. |
| `test_payload_round_trips` | The payload actually lands in the jsonb column intact (nested dict and list), read back with SQL, not via the API. |

- Helper `_job(tenant_id, **changes)` builds a valid body and lets each test change one field, so
  every failing test fails for exactly one reason.

### C5. `tests/test_health.py` had to change [claude]
- `test_unimplemented_routes_say_so` asserted `POST /jobs` with an empty body returns 501. Now
  that the route is real, the same request returns 422 (body missing), so the test would fail. Its
  own comment says "Delete this test as you implement the routes". I kept the idea by pointing it at
  `GET /runs/abc`, which is still a 501 stub, and shortened the comment to stay under 100 chars.
  Delete it when `GET /runs/{id}` is implemented.

---

## Part D: Environment and process decisions

- **Ran the real server for the manual check** with `python -m uvicorn app.main:app` (no
  `--reload`), as instructed. Reload spawns a watcher process and a child, which makes it harder to
  stop cleanly on Windows and is not what production does.
- **Manual check used `Invoke-RestMethod`** because PowerShell's `curl.exe` quoting of JSON is
  fiddly; `ConvertTo-Json` removes that class of mistake. The check created a tenant, created a job
  (200), repeated it (409), read the row in `psql` to confirm the DB defaults (`retention_days` 7,
  `version` 1, `enabled` true) were applied, then deleted the tenant to leave the DB clean.
- **Did not touch** `.github/`, the Makefile or `migrations/001_init.sql`, and **did not commit or
  push**: you do those.
- **Did not start the scheduler/worker.** `create_due_runs`, `claim_run`, leases, heartbeats,
  fencing tokens, retries and dead letters were not requested by name, so they were not written.
  (You said to ignore the "she writes it herself" rule, which is why this file explains decisions
  in full; the scope boundary is still the task list.)

---

## Likely interview questions on this part (with one-line answers)

1. **Why insert-and-catch instead of check-then-insert?** Atomic; no race between check and
   insert; one round trip.
2. **Why 409 vs 404 vs 422?** 422 = bad input shape; 404 = referenced thing missing; 409 = valid
   but conflicts with existing state.
3. **Why `Jsonb(...)`?** psycopg 3 can't guess how to adapt a dict; `Jsonb` says "send as jsonb".
4. **How do you prevent SQL injection?** Parameterised queries (`%s`), values sent separately
   from the SQL text.
5. **Why does the database generate the id?** One source of ids, no collisions, client can't
   choose or guess ids; uses `gen_random_uuid()` from pgcrypto.
6. **Why cap `max_attempts` at 10?** Unbounded retries from one tenant can pin workers on a
   permanently failing job; the cap bounds the damage.
7. **What happens to the transaction when you raise `HTTPException` inside `with conn`?** The
   context manager sees the exception, rolls back, returns the connection to the pool.
8. **What would you improve?** `extra="forbid"` on request models, 201 + Location, rate limiting
   (Weekend 2), and an authenticated tenant instead of trusting a `tenant_id` in the body (right
   now anyone can create jobs for any tenant: there is no auth, listed under Limitations).

---

# Weekend 1 core: scheduler, worker, GET /runs (you asked me to write the code from BUILD_PLAN.md)

Scope I chose: every Weekend 1 box in docs/BUILD_PLAN.md. Left alone on purpose: everything marked
Weekend 2 or 3 (per-tenant concurrency and fairness, Redis rate limiting, redrive endpoint,
`verify_sla`, idempotent side-effect table, chaos test). Their TODO comments are still in the code.

## Part E: Scheduler (`app/scheduler.py`, `create_due_runs`)

### E1. One-off vs recurring are two separate INSERT ... SELECT statements, in one transaction [claude]
- A one-off job (`interval_seconds IS NULL`) gets exactly one run, `scheduled_for = jobs.created_at`.
  It is due the moment it exists. A recurring job gets runs on a fixed grid.
- **Why two statements, not one `CASE`:** each is short enough to read in one go. A CASE inside the
  INSERT would work but mixes two ideas.
- **Why set-based SQL (INSERT ... SELECT) and not a Python loop over jobs:** one round trip for all
  jobs instead of one per job, and the database does the work next to the data.

### E2. Occurrence times are computed from a fixed grid, not from "the last run + interval" [claude]
- `scheduled_for = created_at + floor((now - created_at) / interval) * interval`.
- **Why:** it is a pure function of the job row and the clock. Two schedulers running at the same
  moment compute the *same* timestamp, so the `UNIQUE (job_id, scheduled_for)` constraint plus
  `ON CONFLICT DO NOTHING` stops duplicates. If I used "last run's time + interval", two schedulers
  could read different "last runs" and drift apart, and creating the next one would need a read
  then a write (a race).
- **Why the whole-second arithmetic matters:** `interval_seconds` is an integer, so
  `make_interval(secs => whole_number)` is exact. No floating-point drift between schedulers.
- `created_at <= now()` guard: avoids a negative grid index if a job row is committed with a
  timestamp slightly after the scheduler's transaction started.

### E3. Downtime policy: SKIP missed occurrences (DESIGN.md Q7) [claude, needs your sign-off]
- After an outage we create only the latest due occurrence, not one per missed interval.
- **Why skip:** this is a scheduler for periodic work like "refresh the report". Running 60 stale
  copies at once after an hour of downtime floods the workers (a thundering herd), and for most
  periodic jobs only the latest run has value.
- **What it costs:** work that must happen for *every* interval (e.g. "bill for each hour") would
  silently lose runs. For that you would choose catch-up, or catch-up with a cap.
- **Alternatives:** catch up (create all missed, risks a flood), expire (create them but mark them
  expired after a deadline; needs a new state, and the CHECK constraint on `state` would need a
  migration, which I may not touch).
- **Interview line:** "I chose skip because it bounds the load after an outage; the trade-off is
  that missed occurrences are lost, and I know the schema change needed to support catch-up."
- This is your call. If you want catch-up instead, say so and I will change E2.

### E4. `NOT EXISTS` for one-off jobs [claude]
- Without it, every poll tries to re-insert a conflicting run for every one-off job that ever
  existed (works, but scans and conflicts forever as the table grows). `NOT EXISTS (run for this
  job)` skips finished one-offs cheaply. `ON CONFLICT` is still there for the race between two
  schedulers; `NOT EXISTS` is only an optimisation.
- **Known limit:** recurring jobs are still scanned every poll. A `next_run_at` column and index
  would remove that, but it needs a migration (001 is off-limits). Fine at small scale; it is the
  first scheduler bottleneck I would name for DESIGN.md Q10.
- **Known limit 2:** if history is deleted by retention (not built), a finished one-off job would
  get a fresh run. The retention job would have to keep or archive that fact.

### E5. Scheduler main loop [claude]
- Polls every `POLL_SECONDS` (1s; the stub had 5x that, which would add up to 5s of latency before
  a new job even got a run). Catches `Exception`, logs with traceback and carries on, so a Postgres
  restart does not kill the process. It does not catch `KeyboardInterrupt`, so Ctrl+C still stops it.

## Part F: Worker (`app/worker.py`)

### F1. `claim_run`: SELECT ... FOR UPDATE OF r SKIP LOCKED, then UPDATE, then INSERT, one transaction
- `ORDER BY available_at LIMIT 1`: oldest-waiting first. Matches the partial index
  `idx_runs_claimable`.
- `SKIP LOCKED`: if another worker already locked the top row, skip to the next instead of
  waiting. Result: N workers claim N different rows in parallel with no blocking. Plain `FOR UPDATE`
  would make every worker queue behind the first.
- `FOR UPDATE OF r`: lock only the run row, not the joined `jobs` row (otherwise two runs of the
  same job would block each other).
- Joined `jobs` to get `job_type`, `payload` and `max_attempts` in the same query, and to skip
  disabled jobs (`j.enabled`).
- `attempt + 1` and `fencing_token + 1` are done in SQL, not Python, so they are atomic.
- The attempt row is inserted in the same transaction as the claim: either both exist or neither.
- Returns a plain dict (not a class) so it can be passed to the heartbeat thread and printed.
- `worker_id` is an optional parameter defaulting to `WORKER_ID`; tests pass names like "w1".
- **Not done (Weekend 2):** `max_concurrency` and fair claiming across tenants. One noisy tenant can
  currently take every worker. The TODO is kept in the docstring.

### F2. Lease length and heartbeat
- Lease 30s, heartbeat every 10s (existing config). Three heartbeats fit in one lease, so one
  missed or slow heartbeat does not lose the run.
- **Short lease:** fast recovery after a crash, but false expiries when a worker is merely slow.
  **Long lease:** few false expiries, but a dead worker's run waits longer. 30s is a middle value.
- `heartbeat` is `UPDATE ... WHERE id AND fencing_token AND state='leased'`; `rowcount == 1` means
  "still mine", 0 means "someone else has it". No separate read, so no race.
- The heartbeat runs in a **daemon thread with its own pooled connection**. Reason: the main thread
  is blocked inside `execute()` (for example `time.sleep`), so it cannot heartbeat itself. A
  connection is not shared across threads, to avoid mixing two threads' transactions.
- If the heartbeat finds the lease lost it sets a `threading.Event` and logs. It does not kill the
  work (Python cannot safely kill a thread). Correctness does not depend on it: `complete` and
  `fail` are fenced anyway. The cost is some wasted work; with idempotent side effects (Weekend 2)
  it is harmless.
- A heartbeat that raises (DB blip) is logged and retried next tick instead of ending the thread.

### F3. Fencing token on `complete`, `fail` and `heartbeat` (DESIGN.md Q3)
- Every claim bumps `fencing_token`. Each write is `WHERE id = %s AND fencing_token = %s AND
  state = 'leased'` and the code checks `rowcount`.
- Scenario: worker A stalls, its lease expires, worker B claims (token 1 -> 2), A wakes up and calls
  `complete` with token 1: matches nothing, returns False, B's result stands. The test
  `test_expired_lease_is_reaped_and_the_stale_worker_loses` runs exactly this story.
- I also require `state = 'leased'`. Without it, a run that was reaped to `retry_wait` (token
  unchanged) could still be completed by the old worker. With it, only a current lease holder can
  write. The cost is that a worker that finished a hair after its lease expired has its result
  discarded and the run is repeated; that is the safe side for at-least-once.
- `complete` returns a bool and `fail` returns "retry" / "dead" / "stale". The stub said `None`; I
  changed it so callers and tests can see what happened. Cheap, and avoids guessing from the DB.

### F4. `fail`: retry with backoff or dead-letter (jitter style is my pick)
- Dead when the error is **permanent** or `attempt >= max_attempts`. Otherwise `retry_wait` with
  `available_at = now() + backoff`.
- `backoff = min(BACKOFF_MAX, BACKOFF_BASE * 2**attempt)`, then **equal jitter**: half fixed plus a
  random half (`cap/2 + uniform(0, cap/2)`).
  - **Why jitter at all:** if 1,000 runs fail together (a downstream outage), identical backoff
    makes them all retry together, which re-creates the overload: a **retry storm**.
  - **Why equal and not full jitter (`uniform(0, cap)`):** full jitter can pick ~0 seconds and retry
    instantly. Equal jitter keeps a floor of cap/2 while still spreading retries. Full jitter spreads
    slightly better; I preferred the guaranteed minimum wait. Be ready to say both exist.
- Backoff is computed in Python and passed to SQL as a number of seconds (`make_interval`).
- The `run_attempts` row is closed as `failed` and the `dead_letters` row is inserted in the same
  transaction as the state change, so they cannot disagree.
- `dead_letters` insert uses `ON CONFLICT (run_id) DO NOTHING` (run_id is its primary key), so a
  repeated call is harmless.

### F5. Permanent vs transient errors
- `PermanentError` is raised by `execute` for things retrying cannot fix: unknown `job_type`, bad
  `payload.seconds`. Everything else (any other exception) is treated as transient.
- **Why default to transient:** an unexpected exception is more likely a network or DB glitch than a
  bad job, and `max_attempts` bounds the retries anyway.
- A job with `{"seconds": "ten"}` goes dead on attempt 1 instead of burning 5 attempts.
- `isinstance(seconds, bool)` check: `True` is an `int` in Python, so `{"seconds": true}` would
  otherwise sleep 1 second.

### F6. `reap_expired_leases`
- Finds `leased` runs whose `lease_expires_at < now()`, closes the attempt as `lease_expired`, and
  moves the run to `retry_wait`, or to `dead` (plus a dead-letter row) if that was the last
  attempt.
- **A crash counts as an attempt.** Otherwise a job that crashes the worker every time (a
  "poison pill") would loop for ever.
- It leaves `available_at` untouched. It is already in the past, so the run is claimable at once.
  Side benefit: the run keeps its place in the oldest-first queue.
- Uses `FOR UPDATE OF r SKIP LOCKED` so several workers can reap simultaneously without double
  handling.
- Every worker reaps at the top of each loop instead of having one reaper process. Fewer moving
  parts; the cost is a cheap indexed query per loop (`idx_runs_lease_expiry`).

### F7. Worker main loop, one run at a time
- Order each loop: reap, claim, (if nothing: sleep POLL_SECONDS), else `run_one`.
- The claim connection is released *before* executing, so a long job does not hold a pooled
  connection for its whole duration. `complete`/`fail` take a fresh one.
- Same "catch `Exception`, log, sleep, continue" resilience as the scheduler.
- If `complete`/`fail` raises (DB down at that moment), nothing is written, the lease expires, and
  the reaper recovers the run. That is the at-least-once guarantee working as designed.

### F8. `execute` job types
- `noop`: does nothing. `sleep`: sleeps `payload.seconds` (default 1). No side effects yet, so
  nothing to de-duplicate; the run id as an idempotency key arrives in Weekend 2 as a unique
  constraint on an effect table.

## Part G: `GET /runs/{run_id}` (`app/main.py`)

- `run_id: UUID` in the signature: a malformed id returns 422 from FastAPI, never reaching SQL
  (a non-UUID string passed to a `uuid` column would be a Postgres error and a 500).
- Unknown id returns 404 "run not found".
- Returns the whole `job_runs` row plus an `attempts` list ordered by attempt. `SELECT *` here is a
  deliberate choice: the endpoint's job is "show me everything about this run" for debugging, and
  new columns appear automatically. I would list columns explicitly for a public API.
- `dict_row` row factory: columns come back as a dict by name, so I do not index a 12-column tuple
  by position. Still plain SQL, no ORM.
- `HTTPException` is raised inside the `with` blocks (before any write), same style as the other
  routes.
- `test_health.py::test_unimplemented_routes_say_so` now points at the redrive endpoint, the only
  stub left.

## Part H: Tests for Weekend 1

### H1. Shared fixtures in `tests/conftest.py`
- `db` (autocommit connection), `tenant` (random-named, deleted afterwards), `make_job`, `make_run`.
  Four test files need the same set-up, so one place is cleaner than four copies.
- **Autocommit** because the code under test uses `conn.transaction()` and the real app commits;
  tests must see committed data from other connections (the two-worker threads).

### H2. Test runs get `available_at = 1970-01-01`
- `claim_run` takes the oldest claimable run in the *whole* table, and the database keeps data
  between runs. An ancient `available_at` makes the test's own runs sort first, so a leftover run
  from a demo cannot be claimed by mistake. Checked by asserting `run["id"]` equals the test's id.
- Known side effect: `create_due_runs` and `reap_expired_leases` are global, so a test run can also
  create or reap rows for other tenants. Assertions only look at the test's own rows.

### H3. What is covered
| Test | Proves |
|---|---|
| claim takes a lease | state `leased`, owner, expiry in the future, attempt 1, token 1, attempt row open |
| not-due run not claimed | `available_at` in the future is respected |
| two workers never get the same run | 2 threads, own connections, 20 claims from 30 runs: no duplicates (SKIP LOCKED works) |
| heartbeat extends lease | lease pushed out by LEASE_SECONDS |
| expired lease reaped, stale worker loses | reaper -> `retry_wait`; new claim has attempt 2, token 2; old heartbeat/fail/complete all rejected; final state and attempt history correct |
| complete marks succeeded | state, finished_at, lease cleared, attempt row closed |
| transient failure | `retry_wait`, future `available_at`, error stored |
| out of attempts | second failure -> `dead` plus dead-letter reason |
| permanent error | dead on attempt 1 |
| expired lease on last attempt | dead |
| backoff growth and cap | 50 random samples inside the expected ranges |
| execute (noop, sleep, bad inputs) | permanent errors for bad jobs |
| run_one end to end | sleep job succeeds; a bad payload goes straight to dead |
| scheduler: one-off, disabled, downtime skip, two schedulers | exactly one run per occurrence, idempotent, no catch-up flood |
| GET /runs | body, attempts list, 404, 422 |

### H4. What is NOT tested yet (be honest about this in interviews)
- Real process kills (that is the Weekend 3 chaos test).
- The heartbeat thread's timing (covered by calling `heartbeat` directly; the thread loop itself is
  only exercised in the manual 100-job run).
- Behaviour under heavy load.

## Part I: The "done when" check, and what I measured

- `scripts/demo_100_jobs.py` submits 100 `sleep 1s` jobs, waits for the three workers, then asserts
  per run: exactly one `run_attempts` row, outcome `succeeded`, `attempt = 1`. It deletes its tenant
  afterwards.
- Result on this machine (Windows 11, local Postgres 16, 1 API process, 1 scheduler, 3 workers):
  **100 of 100 succeeded, 0 ran twice, split 34 / 33 / 33 across the workers, drained in 36.8s**
  (submission and execution overlapped). Ideal for 3 workers at 1s per job is about 34s, so the
  overhead is small. Quote the split and the 0 duplicates, not a throughput claim: this is one run
  on a laptop, not a benchmark.
- Mistake found and fixed while measuring: the first run took 71s just to *submit*. Cause was my
  script, not the API: `httpx.post()` creates a new client (and reloads TLS certificates) on every
  call, about 0.7s on Windows. Using one `httpx.Client` fixed it. The API itself answered in about
  65ms. Good story for "tell me about a time you debugged performance": I measured the server
  separately before blaming it.
- Run it as `python -m scripts.demo_100_jobs` from the repo root so `import app` works.

## More likely interview questions (Weekend 1)

1. **Why SKIP LOCKED?** Concurrent workers take different rows without blocking each other.
2. **What if a worker dies mid-job?** Heartbeats stop, the lease expires, the reaper puts the run
   back, another worker claims it with a higher fencing token.
3. **What stops a zombie worker from overwriting a newer result?** Its `UPDATE ... WHERE
   fencing_token = old` matches 0 rows.
4. **Is it exactly-once?** No, at-least-once: a worker can finish the side effect and die before
   `complete`, so the run is repeated. Safe only if the side effect is idempotent, keyed by run id.
5. **Why can two schedulers run safely?** The occurrence time is deterministic and the
   `UNIQUE (job_id, scheduled_for)` constraint makes the second insert a no-op.
6. **What happens after downtime?** Skip: only the latest occurrence is created. Cost: missed
   occurrences are lost.
7. **Why jitter?** Avoids retry storms where many failed runs retry at the same instant.
8. **What breaks first at 10x?** The claim query and `job_runs` table churn on one Postgres, and
   the scheduler scanning all recurring jobs each tick.

---

# Weekend 2

## Part J: W2-1, migration 002 and CI applies every migration

### J1. New numbered file (`002_weekend2.sql`), never an edit to `001_init.sql`
- **Why:** 001 has already been applied to real databases (your laptop, CI). Editing it would not
  change them: `CREATE TABLE IF NOT EXISTS` skips a table that exists, so a new column added to the
  001 text would never appear there. Two machines would end up with different schemas under the same
  file name. A new file is an append-only history: "what changed, in order".
- **Alternatives rejected:** editing 001 and dropping/recreating the database (loses data, and
  does not work for anyone who already has one).

### J2. Every statement is safe to run twice (idempotent), with no migration-tracking table
- Tools used: `ADD COLUMN IF NOT EXISTS`, `CREATE TABLE IF NOT EXISTS`, and a `DO $$ ... $$` block
  for the constraint, because `ALTER TABLE ... ADD CONSTRAINT` has no `IF NOT EXISTS`. The block
  looks in `pg_constraint` first and only adds the constraint if it is missing.
- **Why:** CI, a fresh laptop and docker-compose all just run every file in order. With idempotent
  files the runner can be a three-line shell loop and re-running after a half-failed attempt is harmless.
- **Alternative rejected:** a `schema_migrations` table plus a runner (or Alembic/Flyway). That is
  what real teams use. It is more machinery than this project needs, and it is one more thing to
  explain.
- **Trade-off I accept:** idempotent SQL works for "add" changes. It cannot safely express a data
  rewrite ("multiply all values by 2" would run twice), and it cannot detect that a database has
  drifted. At team scale I would switch to a real migration tool.

### J3. `job_runs.attempt_base integer NOT NULL DEFAULT 0` (used by redrive in W2-3)
- **Problem it solves:** a redrive must give a dead run a fresh set of `max_attempts` attempts. The
  obvious fix, `UPDATE ... SET attempt = 0`, breaks the code: `complete`, `fail` and the reaper
  find their `run_attempts` row by `(run_id, attempt)`. After a reset, attempt 1 would exist
  twice and one UPDATE would hit two rows.
- **Decision:** never reset `attempt`; record where the current "budget" started. Attempts used
  since the last redrive = `attempt - attempt_base`. History keeps counting up, attempt numbers never repeat.
- `NOT NULL DEFAULT 0` means all existing rows get 0 and behave exactly as before.

### J4. `job_effects`: the primary key on `run_id` is the idempotency guarantee (used in W2-2)
- Delivery is at-least-once, so a run can execute twice. The effect is "insert a row keyed by run
  id". The second insert hits the primary key; with `ON CONFLICT (run_id) DO NOTHING` it becomes a
  no-op (rowcount 0). The database, not application code, enforces "once", so it holds even when
  two workers run the same run at the same moment.
- `tenant_id` is stored on the row (same denormalising idea as `job_runs.tenant_id`) so effects can
  be counted per tenant without a join. Both foreign keys use `ON DELETE CASCADE`, so deleting a run
  or tenant leaves no orphans (tests rely on this for cleanup).

### J5. `redrive_log` is a separate append-only table, not extra columns on `dead_letters`
- `dead_letters` has one row per run (`run_id` is the primary key). Section 3 item 3 of SPEC.md says
  a second death must overwrite `reason`, `moved_at` and `redriven_at`. So that row cannot hold
  history. `redrive_log` keeps one row per redrive (`bigserial` id) with `previous_reason`
  and an optional `note`, so "this run was redriven twice, and died of X the first time" stays answerable.
- Interview framing: current state in one table, audit history in another. That is why redrive
  stays "explicit and auditable".

### J6. CHECK `tenants_name_not_blank` as `btrim(name) <> ''`
- **Why in the database as well as the API:** the API trims and validates, but anything that writes
  SQL directly (a script, a test, a future service) would bypass it. A constraint is the last line
  of defence. Two layers: the API gives a friendly 422, the database guarantees the invariant.
- **Spaces only:** `btrim(name)` with no second argument strips spaces, not tabs or newlines, so
  `"\t"` passes the database check while the API's `.strip()` rejects it. I chose the simple form
  from SPEC.md; the API stays the stricter layer. Know this limitation if asked.
- **If a blank-named row already existed,** `ADD CONSTRAINT` would fail and the migration would
  stop (nothing like that exists here because the API never allowed it). `NOT VALID` would add
  the constraint without checking old rows; I did not need it.

### J7. CI applies `migrations/*.sql` in a loop, and psql gets `-v ON_ERROR_STOP=1`
```yaml
for f in $(ls migrations/*.sql | sort); do
  PGPASSWORD=jobforge psql -v ON_ERROR_STOP=1 -h localhost -U jobforge -d jobforge -f "$f"
done
```
- `sort` makes the order explicit (001 before 002). It works because the numbers are zero-padded;
  `10_x.sql` would sort before `2_x.sql`. Keep the 3-digit prefix.
- **`ON_ERROR_STOP` is the important part.** By default psql prints an error and carries on, then
  exits with status 0, so a broken migration would still give a green CI step and the failure would
  surface later as confusing test errors (or not at all). With it, the first error stops psql with
  a non-zero status and the CI step fails at the real cause. The old one-line step was missing this.
- **Process note:** SPEC.md said Pari edits `ci.yml` herself. She told me in chat to make the edit
  and she would review, which is the "ask before editing .github/" rule satisfied. Logged here so
  the history is honest about who typed it.

### J8. What I verified, and what I did not
- Applied 002 to the local database twice: both exit 0; the second run only prints "already exists,
  skipping" notices. `\d job_effects`, the constraint definition and `attempt_base DEFAULT 0` look
  as intended.
- I could not create a second database (user `jobforge` has no CREATEDB). Instead I created an
  empty schema, set `search_path` to it, and ran the exact CI loop (001 then 002) twice. All four
  runs exit 0 and all 7 tables appeared. Difference from CI: the extension and the `public`
  schema already existed here.
- `ruff check .` passes. Plain `pytest` with `DATABASE_URL` set: **43 passed, 1 skipped** (the
  Weekend 3 placeholder). New tests in `tests/test_schema.py`: blank names (`""` and `"   "`)
  rejected, second `job_effects` row rejected, `ON CONFLICT DO NOTHING` gives rowcounts 1 then 0,
  `attempt_base` defaults to 0, `redrive_log` rows cascade away with their run, and 002 can be
  re-applied.
- I did not run the new tests before applying the migration, so "they fail without it" is by
  construction (they use tables and a column that did not exist), not something I observed.
- **Not yet done, needs you:** push a branch and check CI is green. Acceptance for W2-1 says so;
  I cannot do it from here.

### J9. Two likely interview questions
1. **"How do you change a database schema safely across environments?"** Numbered, append-only
   migration files; each idempotent here; applied in order by CI; a failing statement must fail the
   pipeline (`ON_ERROR_STOP`). For a bigger team: a migration tool with a tracking table, and
   expand-then-contract changes so old and new code can run together.
2. **"Where do you enforce uniqueness and invariants: code or database?"** In the database
   (primary key, unique, check, foreign key), because application checks race and can be bypassed;
   the application layer adds friendly error messages on top (`job_effects` and `tenants.name`).

### In my words (understanding gate, W2-1)
SKIPPED on 2026-10-18 at Pari's request, to ship faster. SPEC.md says a task is not done until she
can answer these in her own words, so W2-1 is "done, gate still owed". Come back to it before an
interview; the three questions were:
- *How it works:* what does the database do on a second insert into `job_effects` for the same
  run id, and why is that safer than "check, then insert"?
- *What happens when it fails:* a typo in the 3rd line of `002_weekend2.sql` in CI, with and
  without `-v ON_ERROR_STOP=1`?
- *The trade-off:* what breaks if redrive resets `attempt` to 0, and what does `attempt_base` cost?

Answers (fill in later): (pending)

---

## Part K: W2-2, idempotent side effect (job types `record` and `flaky`)

### K1. The effect is "insert a row into `job_effects` keyed by the run id", with `ON CONFLICT (run_id) DO NOTHING`
- The `record` job sleeps `payload.seconds` (default 0, to hold the lease open in tests), then runs
  `INSERT INTO job_effects (run_id, tenant_id) VALUES (%s, %s) ON CONFLICT (run_id) DO NOTHING`.
- A second execution of the same run hits the primary key and inserts nothing (rowcount 0). So the
  run may *execute* twice, but its visible effect exists once. That is the answer to DESIGN.md Q4:
  delivery is at-least-once, the *outcome* is exactly-once because the key is the run id.
- **Why the run id is the right key, not a random token made in the worker:** the run id is created
  once, when the scheduler inserts the run, and every retry and every worker sees the same one. A
  token generated inside `execute` would differ on each execution and could never match.
- **Alternative rejected: check, then insert** (`SELECT` for a row, insert if absent). Two workers
  can both see "absent" and both insert (a race). The primary key makes the database the referee.

### K2. Why the effect and "mark succeeded" cannot be one atomic action, so the unique key is the right tool
- A real effect (send an email, call a payment API, write to another system) happens *outside* our
  Postgres. We cannot put "send the email" and "UPDATE job_runs SET state='succeeded'" in one
  transaction, because the email server does not take part in our commit.
- So there are always two steps, with a gap between them where the worker can die:
  - Effect first, then mark succeeded: a crash in the gap means the effect happened but the run
    still says leased. The lease expires, another worker re-runs it. **At-least-once** (the effect
    could happen twice).
  - Mark succeeded first, then effect: a crash in the gap means the run says done but the effect
    never happened. **At-most-once** (the effect is lost).
- We cannot have "exactly once" in the middle. We choose the first order (never lose work) and make
  the repeat harmless: the effect is idempotent, keyed by run id. The code does exactly this:
  `execute` writes the effect, then `complete` marks succeeded.
- In this project `job_effects` happens to live in the same database, so I *could* put it in the
  same transaction as `complete`. I did not, on purpose. It would hide the real problem. The point
  is to build what you must do when the effect is external, and `job_effects` stands in for that
  external system. Be ready to say this out loud.
- **Alternatives rejected:** two-phase commit across systems (slow, needs every system to support
  it, hard to run); a transactional outbox (a good pattern, but it needs a separate publisher
  process, out of scope); "at-most-once" (loses work, wrong for a scheduler).

### K3. `execute` uses its own pooled connection for the effect insert
- `execute` runs after the claim transaction has committed and the connection was released
  (`main` releases it before running the job). So it takes a fresh connection from the pool, and
  `with get_pool().connection()` commits when the block ends.
- The effect is therefore committed *before* `complete` runs. That is the order in K2.

### K4. The effect is written even by a worker that has lost its lease
- A stalled worker that wakes up after its lease expired still runs `execute`, so it still inserts
  its effect. That is fine and intended: it is the same key, so if the new worker already wrote it
  the insert is a no-op, and if the old one is first the new worker's insert is the no-op. Either
  way, one row. What the stale worker cannot do is `complete`: the fencing token rejects it.
- Test `test_record_run_finished_by_two_workers_leaves_one_effect` runs exactly this story.
- **Division of labour, a good interview line:** fencing tokens protect *our* state (the run row),
  idempotency keys protect the *outside world*. You need both.

### K5. `flaky` is test-only
- Payload `fail_times` (a number >= 0). It raises a plain `RuntimeError` (so it is a transient
  error, retried with backoff) while `run["attempt"] <= fail_times`, then succeeds.
- It lets me drive retry and dead-letter paths end to end: `fail_times=2, max_attempts=5` ends
  `succeeded` on attempt 3 with attempt outcomes `failed, failed, succeeded`; `fail_times=9` ends
  `dead` with a `dead_letters` row after 5 failed attempts.
- Not documented as a product feature. It is accepted by the API (the `Literal` includes it) because
  the load test needs to create it through the API. The scope list in SPEC.md allows exactly `noop`,
  `sleep`, `record` and `flaky`.
- **Known limit:** after a redrive (W2-3) `attempt` keeps counting up (that is the point of
  `attempt_base`), so `flaky` compares against the absolute `attempt`, as SPEC.md says. A redriven
  flaky job will therefore succeed immediately. Fine for tests; do not read more into it.

### K6. One small shared helper, `_number(payload, key, default)`
- `sleep` already validated `seconds`; `record` and `flaky` need the same rule (a number, not a
  bool, not negative). Rather than copy the check three times, one helper raises `PermanentError`
  with the key name in the message. It is the only new abstraction. Bad payloads still go straight
  to dead letters on the first attempt (retrying cannot fix them).
- This changed the error text for `sleep` from "sleep needs payload.seconds >= 0" to
  "payload.seconds must be a number >= 0". No test depended on the old text.

### K7. Verification
- `ruff check .` passes. Plain `pytest` with `DATABASE_URL` set: **53 passed, 1 skipped** (the
  Weekend 3 placeholder).
- **I checked the tests really catch the bug.** I temporarily removed `ON CONFLICT ... DO NOTHING`
  from `record_effect`: the two idempotency tests failed with a unique violation (2 failed, 6 passed
  in the record/flaky group). I put it back and they pass again.
- New tests: same `record` run twice gives one effect; two workers finishing one run after a lease
  expiry gives one effect and only the current worker's `complete` succeeds; the effect carries the
  run's tenant; `flaky` 2 failures then success (3 attempts); `flaky` 9 failures then dead with a
  dead letter; bad payloads are permanent errors; the API accepts `record` and `flaky`.

### K8. Two likely interview questions
1. **"Your queue is at-least-once. How do you stop a job doing its work twice?"** Idempotency key:
   the run id, with a unique constraint at the place the effect is recorded; the second attempt
   inserts nothing. At-least-once delivery plus an idempotent effect behaves like exactly-once.
2. **"Why not just do the effect and mark success in one transaction?"** Only possible if the effect
   is in the same database. For an external system there is a gap where the worker can die. You can
   close it only by choosing at-least-once (effect first) and making repeats safe.

### In my words (understanding gate, W2-2)
Pending. Questions:
- *How it works:* what happens, step by step, when two workers both finish the same run?
- *What happens when it fails:* the worker dies after the effect row is written but before
  `complete`. What state is the run in, and what happens next?
- *The trade-off:* why did I write the effect before `complete`, and what would be lost if I
  reversed the order?

Answers: (pending)

---

## Part L: W2-3, audited redrive of dead-lettered runs

### L1. Endpoint shape
- `POST /dead-letters/{run_id}/redrive`, `run_id` typed `UUID` (malformed id gives 422 before any SQL),
  optional JSON body `{"note": string | null}` (max 500 characters). No body at all also works.
- Returns the run in the same shape as `GET /runs/{id}` (a shared function `load_run` builds both, so
  the two cannot drift apart).
- 404 if the run does not exist, 409 if it is not `dead`.
- Redrive is an **explicit, audited action** (CLAUDE.md rule 5): nothing brings a dead run back
  automatically, and every redrive leaves a row in `redrive_log`.

### L2. One transaction, starting with `SELECT ... FOR UPDATE` on the run row
- Steps, all in one transaction: lock the run row, check its state, copy the old reason to
  `redrive_log`, reset the run, stamp `dead_letters.redriven_at`, read the run back.
  Either all of it happens or none of it (any error rolls back and the run stays dead).
- **Why `FOR UPDATE`:** the first redrive holds the row lock until it commits. A second, simultaneous
  redrive waits at the `SELECT`, then (after the first commits) sees `state = 'ready'`, not `dead`,
  and returns 409. So a double click or two operators cannot redrive one death twice, and the
  state check is not a check-then-act race.
- Test `test_two_concurrent_redrives_of_one_run_give_exactly_one_200`: two threads released together
  by a barrier. Statuses are exactly `[200, 409]` and `redrive_log` has one row. Without the lock
  (I removed it as a check) the test fails.
- **Alternatives rejected:** an `UPDATE ... WHERE state = 'dead'` and check the rowcount (also
  correct and shorter, but then the `redrive_log` insert has to come first or after separately, and
  the old reason must be read before the update; the row lock makes the steps simple to read).
  An optimistic `version` column would also work but needs more code.

### L3. What the reset changes, and what it deliberately does not
- Changes: `state = 'ready'`, `available_at = now()` (claimable immediately, no backoff),
  `attempt_base = attempt`, `finished_at = NULL`, lease columns NULL.
- **Does not touch** `attempt`, `fencing_token`, `last_error` or any `run_attempts` row. History
  stays intact. `fencing_token` keeps rising, which is what the fencing rule needs: a stale worker
  from before the death can never match the new token.
- `last_error` keeps the last failure message, which is useful context if someone looks at the run
  before it is claimed again.

### L4. Fresh attempts via `attempt_base`, not by resetting `attempt` (SPEC section 3, item 2)
- `fail` and `reap_expired_leases` now compare `attempt - attempt_base` with `max_attempts`.
  `claim_run` returns `attempt_base` in the run dict.
- Example, `max_attempts = 2`: dies after attempts 1 and 2; redrive sets `attempt_base = 2`;
  attempt 3 is "1 used", it can retry; attempt 4 is "2 used", dead again.
- **Why not reset `attempt = 0`:** `complete`, `fail` and the reaper look up their `run_attempts`
  row by `(run_id, attempt)`. After a reset, attempt 1 would exist twice and one UPDATE would hit
  two rows. With `attempt_base`, attempt numbers only go up. Test
  `test_redriven_run_gets_a_fresh_set_of_attempts_and_attempt_numbers_never_repeat` checks
  attempts `[1, 2, 3, 4]` and fails if the comparison ignores `attempt_base`.
- Cost: one more subtraction to remember in two places.

### L5. Dead letters are upserted, so the second death replaces the first
- Old code: `INSERT INTO dead_letters ... ON CONFLICT DO NOTHING`. After a redrive the row already
  exists, so a second death would keep the **old reason** and the old `redriven_at`, which would
  misreport the run.
- New code (one shared constant, `DEAD_LETTER_UPSERT`, used by `fail` and the reaper):
  `ON CONFLICT (run_id) DO UPDATE SET reason = EXCLUDED.reason, moved_at = now(),
  redriven_at = NULL`.
- `dead_letters` therefore always describes the **latest** death, and `redrive_log` is the history of
  previous ones. Test: first reason "first reason", second death "second reason"; `dead_letters`
  shows the second one with `redriven_at` NULL, and after a second redrive `redrive_log` has both
  previous reasons in order. The mirror case (the second death is an expired lease) has its own test.

### L6. Smaller choices
- A dead run with no `dead_letters` row would be an inconsistent state that should not happen. I
  log the previous reason as "(no dead_letters row)" instead of failing, because `redrive_log
  .previous_reason` is NOT NULL and the operator's job is to get the run moving.
- `main.py` docstring updated (it still said endpoints were stubs). The stub test in
  `test_health.py` was deleted, as its own comment said to do once the routes exist; redrive was
  the last stub.
- Still no authentication: anyone who can reach the API can redrive any run. Listed as a known
  limitation in SPEC.md.

### L7. Verification
- `ruff check .` passes. Plain `pytest` with `DATABASE_URL` set: **61 passed, 1 skipped** (the
  Weekend 3 placeholder).
- Mutation checks (change the code on purpose, expect a failing test, then restore): ignoring
  `attempt_base` in `fail` fails the fresh-attempts test; going back to `DO NOTHING` fails two
  tests; removing `FOR UPDATE` fails the concurrent test. All restored and green.
- The concurrent test depends on thread timing. It failed reliably when I removed the lock in my
  one run, but a timing-based test can in principle pass by luck without the lock. The lock is the
  reason it is correct; the test is the evidence, not the proof.

### L8. Two likely interview questions
1. **"How do you make an operator action like redrive safe against double clicks?"** Lock the row
   (`SELECT ... FOR UPDATE`), check the state under the lock, and make the state change in the
   same transaction. The second request sees the new state and gets a 409.
2. **"Why not just reset the attempt counter?"** Because attempt numbers are part of the identity of
   the attempt history rows. Resetting would produce duplicates. A separate base keeps the history
   append-only while giving a fresh retry budget.

### In my words (understanding gate, W2-3)
Pending. Questions:
- *How it works:* two operators click redrive on the same dead run at the same instant. Walk through
  what the database does for each request.
- *What happens when it fails:* the run dies a second time after a redrive. What does `dead_letters`
  show, what does `redrive_log` show, and which old code line would have gotten this wrong?
- *The trade-off:* why `attempt_base` instead of setting `attempt` back to 0, and what does it cost?

Answers: (pending)

---

## Part M: W2-4, per-tenant concurrency and fair claiming (DESIGN.md Q8)

### M1. What `claim_run` does now (two steps, each try in its own transaction)
1. **Pick a tenant** (`PICK_TENANT_SQL`, one query, no locks). For every tenant it computes how many runs
   are `leased` now and the `available_at` of its oldest claimable run (a tenant with no
   claimable run drops out). Tenants at or above `max_concurrency` are excluded. The rest are
   ordered by fewest leased runs first, then oldest waiting run. First row wins.
2. **Claim for that tenant** (`_claim_for_tenant`): lock the tenant row, count its leased runs again,
   and if still under the cap take the tenant's oldest claimable run
   (`FOR UPDATE OF r SKIP LOCKED`), then lease it, bump attempt and fencing token, insert the
   attempt row. Same `enabled` and `available_at <= now()` checks as before.
- If step 2 finds the tenant already full or empty (another worker got there first), `claim_run`
  picks again, up to `CLAIM_TRIES = 3` times, otherwise returns `None`.
- Returning `None` when everyone with work is at their cap is deliberate: the cap is a limit, not
  a preference, so workers sit idle rather than exceed it (test: `..._cap_holds_even_when_workers_are_idle`).

### M2. Fairness rule: fewest leased first, then oldest
- Rule: among eligible tenants pick the one with the fewest leased runs now. Ties: whoever has waited longest.
- This is "least-loaded first", a simple relative of fair queuing. A tenant with a backlog of 500
  gets one lease, then the quiet tenant (0 leased) goes next, and so on, so they alternate until a
  cap or the quiet tenant's work runs out. It balances *concurrent slots*, not total throughput over time.
- **What it does not do:** it is not weighted (every tenant counts the same, `max_concurrency` is a cap
  not a share) and it has no memory (a tenant that just finished 1,000 runs is not penalised).
  Real systems use weights or deficit round robin. Say this if asked, it shows you know the limit.
- **Alternative rejected: plain round robin by tenant id.** Needs a cursor stored somewhere (a table
  or a Redis key) and still has to skip tenants without work. Least-loaded needs no extra state, the
  state is the `leased` rows we already have.

### M3. The race, and how it is closed (the "tenant row lock" option from SPEC.md)
- Problem: two workers pick tenant T at the same instant, both count 2 leased with cap 3, both
  take a run: 4 leased. A count followed by an update has a check-then-act race.
- Fix: step 2 starts with `SELECT max_concurrency FROM tenants WHERE id = %s FOR NO KEY UPDATE`.
  Only one worker at a time holds T's row, the others wait. After the lock the count is repeated,
  and since the earlier worker has committed by then the new count includes its lease.
- SPEC.md said Pari chooses between the tenant row lock and an advisory lock and recommended the row
  lock. You had not chosen yet, so I used the recommended one. **You can still change it; it is one
  function.**
- **`FOR NO KEY UPDATE`, not `FOR UPDATE`:** inserting a `job_runs` row (what the scheduler does)
  takes a weak `FOR KEY SHARE` lock on the tenant row for the foreign key. `FOR UPDATE` conflicts
  with that, so claims would briefly block the scheduler. `FOR NO KEY UPDATE` still conflicts
  with other `FOR NO KEY UPDATE` locks (so claimers take turns) but not with `FOR KEY SHARE`.
  Same safety, less blocking. It is only valid because we never change the tenant's key.
- **No `SKIP LOCKED` on the tenant row** (SPEC.md rule): with one busy tenant, all but one worker
  would be told "no work" and idle for `POLL_SECONDS`. Waiting a few milliseconds for the lock is better.
- **Cost:** claims for one tenant are serialised for the length of the claim transaction (a few
  statements). Claims for different tenants do not wait for each other.
- Each retry is its own transaction. If the whole claim were one transaction, a worker that
  locks tenant A, finds it full, then locks tenant B, would hold two tenant locks; another worker
  doing B then A would deadlock. One lock per transaction removes that.

### M4. Tests (tests/test_claim_fairness.py) and what they prove
| Test | Proves |
|---|---|
| cap | cap 2 with 5 ready runs: exactly 2 claims; finishing one frees exactly one slot |
| cap under concurrency | 20 threads on 20 connections, released together by a barrier, cap 3: exactly 3 get a run, 3 rows leased |
| starvation | tenant A has 500 older runs, B has 5: B gets at least 5 of the first 12 claims |
| fewest leased first | A (older) and B alternate A, B, A, B |
| tie-break | equal leased counts: the tenant that has waited longest goes first |
| idle cap | at cap with nothing else to do: `None` |
| capped tenant does not block others | A capped at 1, B takes the rest |
| disabled job | its runs are never claimed |
- **I ran them against the old `claim_run`** (the version from the previous commit): 6 of 8 fail
  (cap, concurrency, starvation, fewest-leased, idle cap, capped tenant). The other 2 (tie-break,
  disabled job) are regression guards that passed before too.
- **Mutation checks:** removing the tenant row lock makes the concurrency test fail 5 times out
  of 5; with the lock it passes 5 out of 5. Removing the re-count after the lock also fails it.
  So the lock *and* the re-count are both needed, and the test sees both.
- Plain `pytest` with `DATABASE_URL` set: **69 passed, 1 skipped**. `ruff check .` passes.

### M5. Test-suite side effects you should know about
- `claim_run` now looks across all tenants and prefers the least-loaded one, so the old trick
  (give test runs an ancient `available_at`) no longer guarantees a test claims its own run if a
  *different* tenant already has claimable runs in the database. I added a `quiet_queue` fixture
  (used by `make_run`): if any claimable run already exists it **fails the test with a message**
  telling you to stop the scheduler/workers and clean up. A loud failure beats a mystery failure.
- Default tenants have `max_concurrency = 5`, so one old test that leases 20 runs for one tenant
  now raises the cap first (`UPDATE tenants SET max_concurrency = 100`); it is about duplicates, not caps.
- `make_run` now derives `tenant_id` from the job, and `make_job` takes an optional `tenant_id`,
  so tests with several tenants are straightforward. A `make_tenant` fixture cleans up what it creates.

### M6. EXPLAIN ANALYZE: what it showed (saved: `docs/results/explain_claim_2026-10-18.txt`)
Reproduce with `python -m scripts.explain_claim --machine "..." --out <file>` (it inserts synthetic
data inside a transaction and rolls it back). Data: 50 tenants, 7,900 ready runs (one tenant has a
3,000-run backlog, 20 runs leased). Machine: HP Pavilion x360 14-dh1xxx, Intel Core i3-10110U
(2 cores/4 threads), 7.8 GB RAM, Windows 11 Home build 26300, PostgreSQL 16.15, one connection.
Single `EXPLAIN (ANALYZE)` executions, so treat as rough:

| Query | Execution time |
|---|---|
| OLD claim query (global oldest run) | 0.055 ms and 0.078 ms (two runs) |
| NEW step 1, pick the tenant | **49.6 ms and 44.5 ms** (two runs) |
| NEW step 2, oldest run of the busiest tenant | 0.074 ms and 0.067 ms |
| NEW step 1 **with the proposed index** | **0.83 ms** (one run) |

**Honest reading:**
- The fairness query is roughly 1000 times slower than the old one on this data. Both are correct;
  the new one does much more work per claim.
- **Why:** for each of the 50 tenants it looks for "the oldest claimable run of this tenant" using
  `idx_runs_claimable`, which is ordered by `available_at` only. The index cannot jump to one
  tenant, so each lookup walks the global list and throws away other tenants' rows
  (`Rows Removed by Filter: 5374`, about 5,260 buffer hits per lookup). The counting of leased runs
  is cheap (an index-only scan of `idx_runs_tenant_state`).
- **Cost grows with the number of tenants** (one lookup per tenant), as SPEC.md predicted, and
  with how far down the global order a tenant's oldest run is.
- **Proposed fix, not applied:** an index on `(tenant_id, available_at) WHERE state IN ('ready',
  'retry_wait')` makes each lookup a single index probe. With it step 1 drops to 0.83 ms in the
  same test (about 50 times faster, one run). It needs a new migration (003), which you approved and
  I applied: see M9. Until then every claim is about 50 ms at 8,000 waiting runs, which is fine for the 100-job
  demo (a few runs waiting at a time) and not fine for a big backlog.
- **At 100x** (thousands of tenants) even one probe per tenant is too much. Then the options are a
  per-tenant counter table (leased count kept up to date instead of counted), or per-tenant queues
  where a worker picks a tenant first and then pops from its queue. That is the answer for DESIGN.md Q8/Q10.
- Index-only scans report `Heap Fetches: 20` here because the synthetic rows were not vacuumed; ignore.

### M7. Smaller notes
- The 100-job demo still passes with the new claim query (100 of 100 succeeded, 0 duplicated, 3
  workers of 34/33/33, no errors in any process log). I ran it as a smoke test only; per SPEC.md
  you re-run `python -m scripts.demo_100_jobs` yourself before quoting any number.
- `max_concurrency` limits *leased* runs. A run in `retry_wait` or `ready` does not count.
- The default cap is 5 per tenant (database default), so with 3 workers one tenant can use at most 5 slots.
- The tenant-picking query scans all tenants on every claim. Fine now; see M6 for the scaling answer.

### M8. Two likely interview questions
1. **"One tenant submits 10,000 jobs. How do the others still get served?"** Claiming is
   least-loaded-first: the tenant with the fewest running jobs goes next, so a big backlog gets
   one slot, then the others get theirs. A per-tenant cap bounds its total share, enforced under
   a tenant row lock so two workers cannot both take the last slot.
2. **"What does that cost, and what breaks at 100x?"** Each claim now counts per tenant, which I
   measured at about 50 ms for 8,000 waiting runs with the current indexes, about 0.8 ms with a
   `(tenant_id, available_at)` partial index. At 100x tenants, replace counting with a counter
   table or per-tenant queues.

### In my words (understanding gate, W2-4)
Pending. Questions:
- *How it works:* walk through two workers claiming at the same instant for a tenant with cap 3 that
  already has 2 leased runs. What does each worker do, and what does the second one see?
- *What happens when it fails:* the picked tenant turns out to be full by the time the worker has
  its lock. What does `claim_run` do, and why is each try its own transaction?
- *The trade-off:* what did the fairness query cost, why is it slow on this data, and what would fix it?

Answers: (pending)

### M9. Follow-up: migration 003 adds the index, and what the numbers showed (added after you approved it)

- `migrations/003_claim_index.sql`:
  `CREATE INDEX IF NOT EXISTS idx_runs_tenant_claimable ON job_runs (tenant_id, available_at)
  WHERE state IN ('ready', 'retry_wait')`. Idempotent. Applied to the local database twice (the second
  run only prints "already exists, skipping"), and the CI loop (001, 002, 003 in order, twice) ran
  clean in an empty scratch schema.
- **Why a partial, two-column index:** `tenant_id` first so one tenant's rows are together,
  `available_at` second so "oldest first" is already sorted, so `ORDER BY available_at LIMIT 1` for a tenant is a
  single probe. The `WHERE state IN (...)` clause means a run drops out of the index the moment it
  is leased or finished, so the index stays as small as the queue, not as big as the table.
- **What it costs:** every insert into `job_runs` and every state change into or out of
  ready/retry_wait now updates one more index. Writes get slightly slower, reads on the hot path get much faster.
  Plain `CREATE INDEX` blocks writes while building; on a large live table you would use
  `CREATE INDEX CONCURRENTLY`.
- **Alternative rejected: replace `idx_runs_claimable`.** It still serves the old global ordering
  and the reaper does not use it, but dropping it is a separate decision I did not need to make.
  (Candidate for cleanup; I have not measured whether anything still uses it.)

**Re-measured** with `scripts/explain_claim.py` (now measures the pick query with and without the
index in one transaction that is rolled back; raw files `docs/results/explain_claim_with_003_run1.txt`,
`..._run2.txt`, `..._run3.txt`). Same machine as M6 (HP Pavilion x360 14-dh1xxx, Intel Core i3-10110U,
7.8 GB RAM, Windows 11 Home build 26300, PostgreSQL 16.15), same synthetic 7,900 runs:

| Pick query (step 1) | run 1 | run 2 | run 3 |
|---|---|---|---|
| WITHOUT the index | 42.7 ms | 47.6 ms | 48.0 ms |
| WITH the index | **49.1 ms (index not used)** | 0.87 ms | 0.55 ms |

- Two more runs I did while checking run 1 (not saved as files): WITH index 0.56 ms and 0.53 ms, WITHOUT 51.5 and 52.1 ms.
- **Honest reading:** with the index the pick query is about 50-100 times faster (about 0.5-0.9 ms versus about
  43-52 ms) in 5 of 6 runs that had the index. **In run 1 the planner did not choose the new index** and
  used `idx_runs_claimable` as before, so that run was as slow as without it. I checked the obvious
  suspect (statistics on a freshly created index) by dropping and re-creating the index, with and without `ANALYZE`:
  both were fast, so that explanation is **not supported**. I do not know the cause. It is a reminder
  that an index is only a candidate: the planner picks the plan from its cost estimates, and
  estimates can be wrong. Worth saying in an interview. In production you would watch the
  claim query's real latency, not assume the index is used.
- `explain_claim_2026-10-18.txt` is the earlier file (before the index existed, produced by the first
  version of the script) and still shows the 49.6 ms / 44.5 ms numbers from M6.
- Per-claim cost now: about 1 ms for tenant selection plus the lock and the updates, which no longer
  grows with a tenant's backlog (the "walk past 5,000 other rows" problem is gone). It still grows with the
  number of tenants, because the query makes one probe per tenant. That is the 100x problem from M6.

### In my words (understanding gate, W2-4, continued)
Add to the question list: *Why is a partial index `WHERE state IN ('ready', 'retry_wait')` better than a full index on `(tenant_id, available_at)`, and what did the first measurement teach us about trusting indexes?*

---

## Part N: W2-5, Redis rate limiter on POST /jobs (short version)

**What it does.** `POST /jobs` checks, in order: body valid (422), tenant exists (404, before Redis is
touched so made-up tenant ids create no keys), burst bucket, hourly quota. A rejection is a 429 with
`Retry-After` (whole seconds, rounded up), `RateLimit-Reason: burst|quota` and the three
`X-RateLimit-*` headers; it returns before the INSERT, so no job is created. Every response
(200 and 429) carries `X-RateLimit-Limit` (hourly quota), `-Remaining` (points left) and
`-NearLimit` (true at 20% or less left). Modelled on Jira Cloud's published design, not identical to it.

**Decisions and trade-offs**
- **Two limits.** Burst (token bucket, capacity = refill per second = `tenants.burst_rps`) stops a
  one-second spike. Hourly points quota stops a steady drip that adds up. One limit cannot do both.
- **Lua scripts, not GET then SET.** Reading the counter and writing it back from Python lets two API
  processes both see "1 token left" and both spend it. A Lua script runs as one uninterrupted unit
  in Redis. The test with 50 threads on a bucket of 10 asserts exactly 10 pass.
- **Time comes from Redis (`TIME`), not the API server**, so API servers with different clocks agree.
  Bucket keys expire when the bucket would be full again, so idle tenants leave no keys.
- **Quota is a fixed window per UTC hour.** Cheap and simple. Known flaw: a tenant can use the full
  quota at 10:59 and again at 11:00, up to 2x across the boundary. A sliding window would fix it at
  the cost of more state.
- **Cost per job type** (`JOB_TYPE_COST`): noop 1, sleep 2, record 3, flaky 1. These are my
  provisional design choices, not measurements: `sleep` holds a worker lease, `record` also writes
  to the database. **Change them if you disagree, and be ready to justify the final numbers.**
- **No refund of the burst token** when the quota check fails. Simpler, and the request did arrive.
  A 429 for burst reads the quota with a cost-0 charge so its headers are accurate.
- **Fail open when Redis is down:** the request is allowed and a warning is logged. The limiter
  protects capacity and must not be the reason submissions stop. Risk: while Redis is down, limits
  are not enforced. Each request also waits up to the 0.5 s Redis timeout; a circuit breaker would
  remove that, not built. Test: Redis made to fail, job still accepted, no limit headers.
- A request that is allowed but then fails the INSERT (duplicate name) has already spent its points.
- `TenantIn` accepts optional `points_quota_per_hour`, `burst_rps`, `max_concurrency` (1 up to
  1,000,000 / 10,000 / 1,000). The SQL uses `COALESCE(%s, <default>)`; a test checks the defaults
  still equal the table's (10000, 50, 5).

**Verification.** Locally ruff passes and pytest gives 72 passed, 11 skipped: this machine has no
Redis (no Docker, no WSL), so the 10 Redis tests were skipped here. They ran in CI, which has a
Redis 7 service, and CI is green (you reported it; I have not seen the log). Running them against
your hosted Redis later is still worth doing. I did not run mutation checks on this task to save time.

**Likely interview questions.** (1) Why Lua, what breaks without it? (a race between read and write;
the 50-thread test). (2) Why two limits? (burst vs sustained). (3) What if Redis dies? (fail open,
and the risk).

**Understanding gate (W2-5):** not asked, to save time. Questions if you want them: why does a 429
for burst still read the quota? What exactly does the 50-thread test prove? Why fixed window and what does it cost?

---

## Part O: W3-1, chaos test (short version)

- **What it does.** `scripts/chaos.py` creates a scratch schema, inserts 60 `record` runs (1 to 2 s each,
  `max_attempts` 10), starts 3 real `python -m app.worker` processes (lease 3 s, heartbeat 1 s, poll 0.2 s)
  and every 2 s hard-kills a random worker with `Popen.kill()` (no cleanup runs) and starts a replacement.
  When every run is final it checks: none lost (all `succeeded`, one effect row each), none doubled (exactly
  one `succeeded` attempt per run), no stale overwrite (the success is the run's last attempt, and
  `fencing_token` equals the number of claims). It also measures recovery time per kill.
- **Deviation from SPEC.md:** SPEC.md says to use a separate `jobforge_test` database that you create
  with a superuser. I used a scratch schema inside the existing database (`search_path` passed to the
  workers in the connection URL, dropped at the end). Same isolation from leftover data, but no manual
  step, since the `jobforge` user cannot create databases.
- **Run it:** `pytest -m chaos -s` (about a minute; excluded from the normal run by `addopts`).
  `python -m scripts.chaos_report --machine "..."` runs it 5 times and saves `docs/results/chaos_<date>.json`.
- **Small changes made for it:** `WORKER_ID` can come from the environment, and `POLL_SECONDS` is now a float.
- **Checker sanity test:** a normal-suite test makes `check()` report a run that never finished, so a
  checker that cannot fail would be noticed.
- **One run so far (mine, on your laptop, for validation only, not for the README):** 60 runs, 24 kills,
  no problems found. Recovery time was close to the 3 s lease (recovery is about lease length plus polling).
  **You must run `chaos_report` yourself for numbers you can quote.**
- Honest limit: kills hit a worker at random moments, so it cannot prove every interleaving is safe;
  it is evidence, not a proof. The unit tests cover the specific races (fencing, double effect).
