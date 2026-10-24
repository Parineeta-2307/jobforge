# Design notes

Write your own answer under each question as you build. An interviewer will ask exactly these.
The one-line hints say where the trade-off is, not what to conclude.

## 1. Why Postgres as the queue (SKIP LOCKED) and not Redis, SQS or Kafka?
Hint: one system holds the durable state and the queue, so there is no gap between "run created"
and "run enqueued". What do you give up at higher throughput?

Where to look in this repo: app/worker.py `claim_run`; tests/test_worker.py::test_two_workers_never_get_the_same_run; decisions.md Parts F1, M. Numbers: docs/results/explain_claim_with_003_run*.txt.

Your answer (4 to 8 sentences, your own words):


## 2. Leases and heartbeats
- Why a lease and not "mark as running"? What happens when a worker dies?
- Short lease versus long lease: which failure does each one cause?
- Why does a worker heartbeat, and what does it do when the heartbeat says the lease is gone?

Where to look in this repo: app/worker.py `heartbeat`, `reap_expired_leases`, `_heartbeat_loop`; tests/test_worker.py (heartbeat, expired lease); decisions.md F2, F6.

Your answer (4 to 8 sentences, your own words):


## 3. Fencing tokens
A worker stalls (long GC pause, slow network), its lease expires, another worker takes over,
then the first worker wakes up and tries to finish. How does `fencing_token` stop it from
overwriting the newer result?

Where to look in this repo: app/worker.py `complete`/`fail`/`heartbeat` (WHERE fencing_token); tests/test_worker.py::test_expired_lease_is_reaped_and_the_stale_worker_loses; chaos check in scripts/chaos.py; decisions.md F3.

Your answer (4 to 8 sentences, your own words):


## 4. Delivery guarantee
Delivery is at-least-once. Why can it not be exactly-once? What does "one visible outcome per run"
require from the code that does the actual work, and how does the run id help?

Where to look in this repo: app/worker.py `record_effect`; migrations/002 `job_effects`; tests/test_worker.py (record run twice, two workers finishing one run); chaos result docs/results/chaos_2026-10-18.json; decisions.md Part K.

Your answer (4 to 8 sentences, your own words):


## 5. Retries
- Which errors are transient and which are permanent?
- Why exponential backoff with jitter? What is a retry storm?
- What decides when a run goes to dead_letters, and how does redrive stay auditable?

Where to look in this repo: app/worker.py `fail`, `backoff_seconds`, `PermanentError`, `DEAD_LETTER_UPSERT`; app/main.py redrive; tests/test_redrive.py; decisions.md F4, F5, Part L.

Your answer (4 to 8 sentences, your own words):


## 6. Duplicate occurrences
Why is `UNIQUE (job_id, scheduled_for)` safer than "only one scheduler runs"?

Where to look in this repo: app/scheduler.py `create_due_runs`; tests/test_scheduler.py::test_two_schedulers_at_once_create_one_run; decisions.md E2.

Your answer (4 to 8 sentences, your own words):


## 7. After downtime
If the scheduler is down for an hour, what happens to the occurrences it missed: skip, catch up,
or expire? Pick one, say why, and say what it costs.

Where to look in this repo: Chosen: skip. app/scheduler.py docstring; tests/test_scheduler.py::test_recurring_job_after_downtime_skips_missed_occurrences; decisions.md E3 (cost: missed runs are lost; the SLA verifier that would make that visible is not built).

Your answer (4 to 8 sentences, your own words):


## 8. Fairness between tenants
One tenant submits 10,000 runs. How do the other tenants still get workers? Describe the claim
query change (concurrency cap, round robin across tenants) and its cost.

Where to look in this repo: app/worker.py `PICK_TENANT_SQL`, `_claim_for_tenant`; tests/test_claim_fairness.py; docs/results/fairness_2026-10-24_run*.txt; docs/results/explain_claim_*.txt; decisions.md Part M.

Your answer (4 to 8 sentences, your own words):


## 9. Rate limiting (modelled on Jira Cloud's published design)
- Burst: token bucket per tenant per endpoint. Quota: hourly points, heavier operations cost more.
- Why two limits and not one? What does each protect against?
- Why a Lua script in Redis? What breaks without one?
- Headers: X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-NearLimit, Retry-After on 429.
- What should a well-behaved client do on a 429?

Where to look in this repo: app/ratelimit.py (Lua scripts), app/main.py `apply_rate_limit`; tests/test_ratelimit.py (50 threads, 10 allowed); docs/results/load_overquota_*; decisions.md Part N and the Part Q addendum.

Your answer (4 to 8 sentences, your own words):


## 10. What breaks first at 10x, and at 100x?
Name the bottleneck and the next step (partition the runs table, move the queue out of Postgres,
shard by tenant). This is the question that turns a project into a system design answer.

Where to look in this repo: README.md 'What would change at 10x and 100x'; decisions.md M6, M9; the first bottleneck is the per-tenant probes in the claim query (docs/results/explain_claim_*.txt).

Your answer (4 to 8 sentences, your own words):


