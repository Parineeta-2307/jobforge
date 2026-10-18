"""Worker process. Claims one run at a time, executes it, records the outcome.

Run several copies: docker compose up --scale worker=3
(or, without Docker, start `python -m app.worker` in several terminals).
"""

import logging
import os
import random
import socket
import threading
import time
import uuid

from app import config
from app.db import get_pool

log = logging.getLogger("worker")
# The WORKER_ID environment variable lets the chaos test know the id of each worker it starts.
WORKER_ID = os.getenv("WORKER_ID") or f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}"


# A run can die more than once (die, get redriven, die again). The second death must replace the
# old reason and clear redriven_at, so dead_letters always describes the latest death. The
# earlier history is kept in redrive_log.
DEAD_LETTER_UPSERT = (
    "INSERT INTO dead_letters (run_id, reason) VALUES (%s, %s) "
    "ON CONFLICT (run_id) DO UPDATE SET reason = EXCLUDED.reason, moved_at = now(), "
    "redriven_at = NULL"
)


class PermanentError(Exception):
    """The job can never succeed (bad payload, unknown job type). Retrying would be pointless."""


# Step 1 of a claim: which tenant should get the next worker? (kept as a constant so that
# scripts/explain_claim.py can run EXPLAIN on the exact same text.)
#   - leased counts how many runs the tenant has leased right now;
#   - the second LATERAL finds the tenant's oldest claimable run (no row = nothing to do);
#   - a tenant at or above max_concurrency is skipped (invariant 1);
#   - among the rest, fewest leased runs first, then the oldest waiting run (invariant 2).
PICK_TENANT_SQL = """
SELECT t.id
FROM tenants t
CROSS JOIN LATERAL (
    SELECT count(*) AS n FROM job_runs WHERE tenant_id = t.id AND state = 'leased'
) leased
CROSS JOIN LATERAL (
    SELECT r.available_at
    FROM job_runs r JOIN jobs j ON j.id = r.job_id
    WHERE r.tenant_id = t.id AND r.state IN ('ready', 'retry_wait')
      AND r.available_at <= now() AND j.enabled
    ORDER BY r.available_at
    LIMIT 1
) oldest
WHERE leased.n < t.max_concurrency
ORDER BY leased.n, oldest.available_at
LIMIT 1
"""

# How many times claim_run re-picks a tenant if another worker got there first.
CLAIM_TRIES = 3


def claim_run(conn, worker_id: str = WORKER_ID) -> dict | None:
    """Claim one run for one tenant and take a lease on it. Return it, or None.

    Fair claiming: the tenant with the fewest leased runs goes first (ties: the oldest waiting
    run), and a tenant never has more than tenants.max_concurrency runs leased. If every tenant
    with work is at its cap, this returns None even though workers are idle: the cap is a limit.

    Each try is one transaction (see _claim_for_tenant). If another worker took the tenant's last
    free slot or last run between our pick and our lock, we pick again, up to CLAIM_TRIES times.
    Every try is its own transaction so a worker never holds two tenant locks at once, which could
    deadlock with another worker holding them in the opposite order.
    """
    for _ in range(CLAIM_TRIES):
        with conn.transaction():
            picked = conn.execute(PICK_TENANT_SQL).fetchone()
            if picked is None:
                return None
            run = _claim_for_tenant(conn, picked[0], worker_id)
        if run is not None:
            return run
    return None


def _claim_for_tenant(conn, tenant_id, worker_id: str) -> dict | None:
    """Inside the caller's transaction: lock the tenant, re-check its cap, lease its oldest run."""
    # Two workers can pick the same tenant at the same instant, and a plain "count, then update"
    # would let both pass the cap check. Locking the tenant row makes claims for one tenant
    # take turns. FOR NO KEY UPDATE (not FOR UPDATE) because inserting a job_runs row takes a
    # weaker lock on this row for the foreign key, and we do not want to block the scheduler.
    # No SKIP LOCKED here: skipping would make all but one worker see "no work" for a busy tenant.
    max_concurrency = conn.execute(
        "SELECT max_concurrency FROM tenants WHERE id = %s FOR NO KEY UPDATE", (tenant_id,)
    ).fetchone()[0]
    leased = conn.execute(
        "SELECT count(*) FROM job_runs WHERE tenant_id = %s AND state = 'leased'", (tenant_id,)
    ).fetchone()[0]
    if leased >= max_concurrency:
        return None  # someone filled the last slot while we waited for the lock
    row = conn.execute(
        "SELECT r.id, r.job_id, j.job_type, j.payload, j.max_attempts, r.attempt_base "
        "FROM job_runs r JOIN jobs j ON j.id = r.job_id "
        "WHERE r.tenant_id = %s AND r.state IN ('ready', 'retry_wait') "
        "AND r.available_at <= now() AND j.enabled "
        "ORDER BY r.available_at LIMIT 1 "
        "FOR UPDATE OF r SKIP LOCKED",
        (tenant_id,),
    ).fetchone()
    if row is None:
        return None
    run_id, job_id, job_type, payload, max_attempts, attempt_base = row
    attempt, fencing_token = conn.execute(
        "UPDATE job_runs SET state = 'leased', lease_owner = %s, "
        "lease_expires_at = now() + make_interval(secs => %s), "
        "attempt = attempt + 1, fencing_token = fencing_token + 1 "
        "WHERE id = %s RETURNING attempt, fencing_token",
        (worker_id, config.LEASE_SECONDS, run_id),
    ).fetchone()
    conn.execute(
        "INSERT INTO run_attempts (run_id, attempt, worker_id) VALUES (%s, %s, %s)",
        (run_id, attempt, worker_id),
    )
    return {
        "id": run_id,
        "job_id": job_id,
        "tenant_id": tenant_id,
        "job_type": job_type,
        "payload": payload,
        "max_attempts": max_attempts,
        "attempt": attempt,
        "attempt_base": attempt_base,
        "fencing_token": fencing_token,
        "worker_id": worker_id,
    }


def _number(payload: dict, key: str, default):
    """Read a non-negative number from the payload, or raise a permanent error."""
    value = payload.get(key, default)
    # bool is a subclass of int in Python, so rule it out explicitly.
    if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
        raise PermanentError(f"payload.{key} must be a number >= 0, got {value!r}")
    return value


def record_effect(run: dict) -> None:
    """The visible side effect of a 'record' job: one row in job_effects, keyed by the run id.

    ON CONFLICT DO NOTHING is what makes it idempotent. If this run executes a second time (a
    retry, or a worker that came back after losing its lease), the primary key on run_id stops a
    second row and the insert quietly does nothing.
    It uses its own pooled connection because execute() runs outside the claim transaction.
    """
    with get_pool().connection() as conn:
        conn.execute(
            "INSERT INTO job_effects (run_id, tenant_id) VALUES (%s, %s) "
            "ON CONFLICT (run_id) DO NOTHING",
            (run["id"], run["tenant_id"]),
        )


def execute(run: dict) -> None:
    """Do the job's work. The run id is the idempotency key for any side effect.

    noop:   does nothing.
    sleep:  sleeps payload.seconds (default 1).
    record: sleeps payload.seconds (default 0) to hold the lease, then writes the effect row.
    flaky:  test-only. Fails (transient) while attempt <= payload.fail_times, then succeeds.
    """
    job_type = run["job_type"]
    payload = run["payload"]
    if job_type == "noop":
        return
    if job_type == "sleep":
        time.sleep(_number(payload, "seconds", 1))
        return
    if job_type == "record":
        time.sleep(_number(payload, "seconds", 0))
        record_effect(run)
        return
    if job_type == "flaky":
        fail_times = _number(payload, "fail_times", 0)
        if run["attempt"] <= fail_times:
            raise RuntimeError(f"flaky: attempt {run['attempt']} of {fail_times} planned failures")
        return
    raise PermanentError(f"unknown job_type {job_type!r}")


def heartbeat(conn, run: dict) -> bool:
    """Extend the lease. Return False if we no longer hold it (fencing token changed).

    The WHERE clause is the whole point: it only matches while we are still the current owner.
    If the lease expired and another worker claimed the run, fencing_token is higher, nothing
    matches, rowcount is 0 and we learn we lost the run.
    """
    with conn.transaction():
        updated = conn.execute(
            "UPDATE job_runs SET lease_expires_at = now() + make_interval(secs => %s) "
            "WHERE id = %s AND fencing_token = %s AND state = 'leased'",
            (config.LEASE_SECONDS, run["id"], run["fencing_token"]),
        ).rowcount
    return updated == 1


def complete(conn, run: dict) -> bool:
    """Mark succeeded, but only if our fencing_token still matches. A stale worker must not win.

    Returns True if we recorded the result, False if we were stale and the result was discarded.
    """
    with conn.transaction():
        updated = conn.execute(
            "UPDATE job_runs SET state = 'succeeded', finished_at = now(), "
            "lease_owner = NULL, lease_expires_at = NULL "
            "WHERE id = %s AND fencing_token = %s AND state = 'leased'",
            (run["id"], run["fencing_token"]),
        ).rowcount
        if updated == 1:
            conn.execute(
                "UPDATE run_attempts SET finished_at = now(), outcome = 'succeeded' "
                "WHERE run_id = %s AND attempt = %s",
                (run["id"], run["attempt"]),
            )
    return updated == 1


def backoff_seconds(attempt: int) -> float:
    """Exponential backoff with "equal jitter": half fixed, half random.

    attempt 1 -> cap 4s, attempt 2 -> 8s, attempt 3 -> 16s ... never more than BACKOFF_MAX.
    The random half spreads retries out so many failed runs do not all retry at the same instant.
    """
    cap = min(config.BACKOFF_MAX_SECONDS, config.BACKOFF_BASE_SECONDS * 2**attempt)
    return cap / 2 + random.uniform(0, cap / 2)


def fail(conn, run: dict, error: str, transient: bool) -> str:
    """Retry with exponential backoff and jitter, or move to dead_letters.

    Returns "retry", "dead" or "stale" (our fencing token no longer matched, nothing changed).
    A permanent error, or running out of attempts, means dead. Attempts used = attempt minus
    attempt_base, because a redrive moves attempt_base up to give the run a fresh budget.
    """
    attempts_used = run["attempt"] - run["attempt_base"]
    is_dead = (not transient) or attempts_used >= run["max_attempts"]
    with conn.transaction():
        if is_dead:
            updated = conn.execute(
                "UPDATE job_runs SET state = 'dead', finished_at = now(), last_error = %s, "
                "lease_owner = NULL, lease_expires_at = NULL "
                "WHERE id = %s AND fencing_token = %s AND state = 'leased'",
                (error, run["id"], run["fencing_token"]),
            ).rowcount
        else:
            updated = conn.execute(
                "UPDATE job_runs SET state = 'retry_wait', last_error = %s, "
                "available_at = now() + make_interval(secs => %s), "
                "lease_owner = NULL, lease_expires_at = NULL "
                "WHERE id = %s AND fencing_token = %s AND state = 'leased'",
                (error, backoff_seconds(run["attempt"]), run["id"], run["fencing_token"]),
            ).rowcount
        if updated == 0:
            return "stale"
        conn.execute(
            "UPDATE run_attempts SET finished_at = now(), outcome = 'failed', error = %s "
            "WHERE run_id = %s AND attempt = %s",
            (error, run["id"], run["attempt"]),
        )
        if is_dead:
            reason = error if not transient else f"out of attempts: {error}"
            conn.execute(DEAD_LETTER_UPSERT, (run["id"], reason))
    return "dead" if is_dead else "retry"


def reap_expired_leases(conn) -> int:
    """Make runs with expired leases claimable again. Return how many.

    A worker that died (or stalled) stops heartbeating, so lease_expires_at passes. We close its
    attempt as 'lease_expired' and put the run back to retry_wait, or to dead if that was the last
    attempt. available_at is left alone: it is already in the past, so the run is claimable now.
    SKIP LOCKED lets several workers reap at once without waiting for or double-handling a row.
    """
    reaped = 0
    with conn.transaction():
        expired = conn.execute(
            "SELECT r.id, r.attempt, r.attempt_base, j.max_attempts "
            "FROM job_runs r JOIN jobs j ON j.id = r.job_id "
            "WHERE r.state = 'leased' AND r.lease_expires_at < now() "
            "FOR UPDATE OF r SKIP LOCKED"
        ).fetchall()
        for run_id, attempt, attempt_base, max_attempts in expired:
            error = "lease expired"
            conn.execute(
                "UPDATE run_attempts SET finished_at = now(), outcome = 'lease_expired', "
                "error = %s WHERE run_id = %s AND attempt = %s AND outcome IS NULL",
                (error, run_id, attempt),
            )
            if attempt - attempt_base >= max_attempts:
                conn.execute(
                    "UPDATE job_runs SET state = 'dead', finished_at = now(), last_error = %s, "
                    "lease_owner = NULL, lease_expires_at = NULL WHERE id = %s",
                    (error, run_id),
                )
                conn.execute(DEAD_LETTER_UPSERT, (run_id, "out of attempts: lease expired"))
            else:
                conn.execute(
                    "UPDATE job_runs SET state = 'retry_wait', last_error = %s, "
                    "lease_owner = NULL, lease_expires_at = NULL WHERE id = %s",
                    (error, run_id),
                )
            reaped += 1
    return reaped


def _heartbeat_loop(run: dict, stop: threading.Event, lease_lost: threading.Event) -> None:
    """Background thread: extend the lease every HEARTBEAT_SECONDS until told to stop."""
    while not stop.wait(config.HEARTBEAT_SECONDS):
        try:
            with get_pool().connection() as conn:
                still_ours = heartbeat(conn, run)
        except Exception:
            log.exception("heartbeat failed for run %s, will try again", run["id"])
            continue
        if not still_ours:
            log.warning("lost the lease on run %s", run["id"])
            lease_lost.set()
            return


def run_one(run: dict) -> None:
    """Execute one claimed run with a heartbeat thread, then record the outcome."""
    stop = threading.Event()
    lease_lost = threading.Event()
    beat = threading.Thread(target=_heartbeat_loop, args=(run, stop, lease_lost), daemon=True)
    beat.start()
    error, transient = None, True
    try:
        execute(run)
    except PermanentError as e:
        error, transient = str(e), False
    except Exception as e:
        error = f"{type(e).__name__}: {e}"
    finally:
        stop.set()
        beat.join()
    # complete() and fail() are guarded by the fencing token, so if we lost the lease while
    # working, they change nothing and we only log it.
    with get_pool().connection() as conn:
        if error is None:
            if complete(conn, run):
                log.info("run %s succeeded (attempt %d)", run["id"], run["attempt"])
            else:
                log.warning("run %s: result discarded, lease was lost", run["id"])
        else:
            outcome = fail(conn, run, error, transient)
            log.info("run %s failed (%s): %s", run["id"], outcome, error)
    if lease_lost.is_set():
        log.warning("run %s was taken over by another worker", run["id"])


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    log.info("worker %s starting", WORKER_ID)
    while True:
        try:
            with get_pool().connection() as conn:
                reap_expired_leases(conn)
                run = claim_run(conn)
            if run is None:
                time.sleep(config.POLL_SECONDS)
                continue
            log.info("claimed run %s (attempt %d)", run["id"], run["attempt"])
            run_one(run)
        except Exception:
            # A database hiccup must not kill the worker. Log it, wait, try again.
            log.exception("worker loop error")
            time.sleep(config.POLL_SECONDS)


if __name__ == "__main__":
    main()
