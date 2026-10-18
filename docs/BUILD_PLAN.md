# Build plan (about 20 hours, weekends only)

Do not take hours from DSA. Finish the core first. Weekend 2 items marked (extra) are the first to cut.
Commit after every checked box so the history shows steady progress.

## Before you start (30 min)
- [ ] Create the GitHub repo, push this scaffold, confirm the CI run is green.
- [ ] `docker compose up --build`, open http://localhost:8000/health, then `make psql` and run `\dt`.

## Weekend 1: core (about 8 h)
- [x] POST /tenants and POST /jobs write to Postgres (psycopg, plain SQL).
- [x] Scheduler: `create_due_runs` with `ON CONFLICT (job_id, scheduled_for) DO NOTHING`.
- [x] Worker: `claim_run` with `FOR UPDATE SKIP LOCKED`, lease, fencing token bump, attempt row.
- [x] Worker: `execute` for two job types, `noop` and `sleep`.
- [x] Worker: `complete` guarded by the fencing token (UPDATE ... WHERE fencing_token = %s, check rowcount).
- [x] Worker: heartbeat thread and `reap_expired_leases`.
- [x] Worker: `fail` with exponential backoff plus jitter, dead_letters after max_attempts.
- [x] GET /runs/{id}.
- [x] Tests for claim (two workers never get the same run), lease expiry, and the fencing guard.

Done when: submit 100 jobs with 3 workers running, all reach `succeeded`, none twice.

## Weekend 2: idempotency, fairness, rate limiting (about 7 h)
- [x] Idempotent side effect: the run id as a key, with a unique constraint on the effect table.
- [ ] Per-tenant `max_concurrency` and fair claiming across tenants.
- [ ] Redis token bucket in a Lua script (burst), hourly points quota (quota), rate-limit headers, 429 with Retry-After.
- [x] Redrive endpoint for dead letters.
- [ ] (extra) `verify_sla` flagging missed runs.
- [ ] Tests: limiter allows exactly N then rejects, quota resets on the hour, one noisy tenant does not starve another.

## Weekend 3: proof (about 5 h)
- [ ] Chaos test: kill workers at random mid-run, assert none lost, none doubled, no stale overwrite.
- [ ] Locust load test, record throughput, p95 and the 429 share for an over-quota tenant.
- [ ] README: architecture diagram, decisions, a results table with your own measured numbers, limitations.
- [ ] Fill in docs/DESIGN.md. Practise explaining it out loud in 5 minutes.

## Then tell me
Send me the repo link and the measured numbers and I will write the resume bullets from them.
