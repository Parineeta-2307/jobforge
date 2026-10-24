# Resume facts

Only things that are true and checkable. Repo: https://github.com/Parineeta-2307/jobforge

**Machine for every number:** HP Pavilion x360 14-dh1xxx, Intel Core i3-10110U (2 cores/4 threads), 7.8 GB RAM, Windows 11 Home
build 26300, PostgreSQL 16.15. The API, Locust, PostgreSQL and Redis all ran on this one laptop. These are "on this machine" numbers, not capacity claims.

**Provenance:** the runs were executed by Claude Code in Pari's session, on her laptop, at her request. The raw output of every run is saved in
`docs/results/`. Re-run the commands below yourself before putting a number on a resume.

## What works (yes or no)

| Item | Works? | Evidence |
|---|---|---|
| Leases with heartbeat | **Yes** | `tests/test_worker.py::test_heartbeat_extends_the_lease`, `::test_expired_lease_is_reaped_and_the_stale_worker_loses`; the chaos test |
| Fencing-token guard | **Yes** | `tests/test_worker.py::test_expired_lease_is_reaped_and_the_stale_worker_loses` (old worker's heartbeat, fail and complete are all rejected); chaos check "no stale overwrite" |
| Retries with backoff and dead letters | **Yes** | `tests/test_worker.py::test_transient_failure_goes_to_retry_wait_with_a_future_available_at`, `::test_running_out_of_attempts_moves_the_run_to_dead_letters`, `::test_backoff_grows_with_the_attempt_and_is_capped`; redrive: `tests/test_redrive.py` |
| Per-tenant concurrency limit and fair claiming | **Yes** | `tests/test_claim_fairness.py` (cap, 20 threads on a cap of 3, no starvation behind a 500-run backlog); `docs/results/fairness_2026-10-24_run*.txt` |
| Redis rate limiter with 429 and `Retry-After` | **Yes** | `tests/test_ratelimit.py` (50 threads on a bucket of 10 let exactly 10 through; 429 with `Retry-After` and `RateLimit-Reason`; headers on 200 and 429); `docs/results/load_overquota_*` |
| Chaos test with workers killed | **Yes** | `tests/test_worker_kill.py::test_no_run_lost_or_doubled_when_workers_are_killed` (`pytest -m chaos -s`); `docs/results/chaos_2026-10-18.json` |

Not built, so do not claim: authentication, a web UI, deployment, multi-region, the SLA verifier, catch-up scheduling.

## Measured numbers

**Requests per second and p95 latency** (`POST /jobs`, `noop`, 20 users, 60 s measured after a 10 s warm-up, Redis active, 3 runs):
232.1, 235.9 and 235.8 requests/s; p95 57, 56 and 56 ms; p99 69 to 71 ms; 0 failures.
Files: `docs/results/load_baseline_redis_2026-10-24_run{1,2,3}.*`.
Command: `locust -f loadtest/locustfile.py BaselineUser --headless -u 20 -r 20 -t 70s --host http://127.0.0.1:8000` (API and Redis running).

**Runs lost or doubled after worker kills:** 0 lost and 0 doubled, across 5 scenarios of 60 runs each (300 runs), with 134 hard kills of worker
processes (3 workers, one kill every 2 s, lease 3 s). "Doubled" means more than one effect row or more than one successful attempt for a run; none
occurred, and no stale worker overwrote a newer result. 121 attempts were recovered after their lease expired.
File: `docs/results/chaos_2026-10-18.json`. Command: `python -m scripts.chaos_report --machine "..."`.

**Recovery time after a kill** (kill to the start of the next attempt of the run the dead worker held): min 2.14 s, median 2.9 s, max 3.81 s over
121 recoveries, with `LEASE_SECONDS=3`. Always quote the lease next to it, because recovery is roughly the lease plus polling.

**Share of 429s for an over-quota tenant:** the noisy tenant (quota 50 points/hour, 5 requests/s burst) was answered **100% 429** in all 3 runs
(5,132, 5,456 and 5,507 requests), while the quiet tenant sharing the API got **0% 429** (760, 787 and 786 requests) with p95 41 to 61 ms.
Say this honestly: the noisy tenant's quota was spent during the 10 s warm-up, which the statistics exclude, so the 100% is after its quota ran out.
Files: `docs/results/load_overquota_2026-10-24_run{1,2,3}.*`.
Command: `locust -f loadtest/locustfile.py NoisyUser QuietUser --headless -u 10 -r 10 -t 70s --host http://127.0.0.1:8000`.

## Other checkable facts

- 100 jobs on 3 workers: 100 succeeded, none ran twice (34/33/33 per worker), 37.9 s, one run. `python -m scripts.demo_100_jobs`.
- Fair claiming: a quiet tenant's 10 jobs finished in 11.4 s (3 of 3 runs) while a noisy tenant had a 300-run backlog, of which only 27 to 31 had finished.
  No "before" number exists. `python -m scripts.fairness_demo`.
- The tenant-picking query went from 43 to 52 ms to 0.5 to 0.9 ms with a partial index on 7,900 synthetic runs (the planner skipped the index in 1 of 6 runs).
  `python -m scripts.explain_claim`.
- 83 automated tests pass against real PostgreSQL and Redis, plus 1 chaos test. CI runs PostgreSQL and Redis service containers.
- 3 idempotent numbered migrations applied by a CI loop with `ON_ERROR_STOP`.
- Found and fixed while measuring: with Redis down, every request waited about 1 s before failing open (100 submissions took 102.9 s); a circuit
  breaker brought it to 1.8 s. Raw output of both runs is in `docs/results/`.
