"""Worker process. Claims one run at a time, executes it, records the outcome.

Run several copies: docker compose up --scale worker=3
(or, without Docker, start `python -m app.worker` in several terminals).
"""

import logging
import random
import socket
import threading
import time
import uuid

from app import config
from app.db import get_pool

log = logging.getLogger("worker")
WORKER_ID = f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}"


class PermanentError(Exception):
    """The job can never succeed (bad payload, unknown job type). Retrying would be pointless."""


def claim_run(conn, worker_id: str = WORKER_ID) -> dict | None:
    """Atomically claim one claimable run and take a lease on it. Return it, or None.

    All in one transaction:
      1. pick one run in state ready/retry_wait with available_at <= now(), oldest first, with
         FOR UPDATE SKIP LOCKED so that a row another worker has locked is skipped, not waited for;
      2. mark it leased, set the lease expiry, attempt + 1, fencing_token + 1;
      3. write a run_attempts row.
    TODO (Weekend 2): skip tenants already at max_concurrency, and pick fairly across tenants.
    """
    with conn.transaction():
        row = conn.execute(
            "SELECT r.id, r.job_id, r.tenant_id, j.job_type, j.payload, j.max_attempts "
            "FROM job_runs r JOIN jobs j ON j.id = r.job_id "
            "WHERE r.state IN ('ready', 'retry_wait') AND r.available_at <= now() AND j.enabled "
            "ORDER BY r.available_at LIMIT 1 "
            "FOR UPDATE OF r SKIP LOCKED"
        ).fetchone()
        if row is None:
            return None
        run_id, job_id, tenant_id, job_type, payload, max_attempts = row
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
        "fencing_token": fencing_token,
        "worker_id": worker_id,
    }


def execute(run: dict) -> None:
    """Do the job's work. The run id is the idempotency key for any side effect.

    Neither job type has a side effect yet, so there is nothing to de-duplicate (Weekend 2 adds
    an effect table with a unique constraint on run id).
    """
    job_type = run["job_type"]
    if job_type == "noop":
        return
    if job_type == "sleep":
        seconds = run["payload"].get("seconds", 1)
        # bool is a subclass of int in Python, so rule it out explicitly.
        if isinstance(seconds, bool) or not isinstance(seconds, int | float) or seconds < 0:
            raise PermanentError(f"sleep needs payload.seconds >= 0, got {seconds!r}")
        time.sleep(seconds)
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
    A permanent error, or running out of attempts, means dead.
    """
    is_dead = (not transient) or run["attempt"] >= run["max_attempts"]
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
            conn.execute(
                "INSERT INTO dead_letters (run_id, reason) VALUES (%s, %s) "
                "ON CONFLICT (run_id) DO NOTHING",
                (run["id"], reason),
            )
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
            "SELECT r.id, r.attempt, j.max_attempts "
            "FROM job_runs r JOIN jobs j ON j.id = r.job_id "
            "WHERE r.state = 'leased' AND r.lease_expires_at < now() "
            "FOR UPDATE OF r SKIP LOCKED"
        ).fetchall()
        for run_id, attempt, max_attempts in expired:
            error = "lease expired"
            conn.execute(
                "UPDATE run_attempts SET finished_at = now(), outcome = 'lease_expired', "
                "error = %s WHERE run_id = %s AND attempt = %s AND outcome IS NULL",
                (error, run_id, attempt),
            )
            if attempt >= max_attempts:
                conn.execute(
                    "UPDATE job_runs SET state = 'dead', finished_at = now(), last_error = %s, "
                    "lease_owner = NULL, lease_expires_at = NULL WHERE id = %s",
                    (error, run_id),
                )
                conn.execute(
                    "INSERT INTO dead_letters (run_id, reason) VALUES (%s, %s) "
                    "ON CONFLICT (run_id) DO NOTHING",
                    (run_id, "out of attempts: lease expired"),
                )
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
