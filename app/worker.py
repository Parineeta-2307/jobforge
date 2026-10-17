"""Worker process. Claims one run at a time, executes it, records the outcome.

Run several copies: docker compose up --scale worker=3

The loop below is safe to start before anything is implemented: it idles until claim_run() exists.
"""

import logging
import socket
import time
import uuid

from app import config

log = logging.getLogger("worker")
WORKER_ID = f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}"


def claim_run(conn) -> dict | None:
    """Atomically claim one claimable run and take a lease on it. Return it, or None.

    TODO (Weekend 1): in one transaction,
      - pick a run in state ready/retry_wait with available_at <= now(), using
        FOR UPDATE SKIP LOCKED so workers never block each other,
      - set state='leased', lease_owner, lease_expires_at = now() + LEASE_SECONDS,
        attempt = attempt + 1, fencing_token = fencing_token + 1,
      - insert a run_attempts row.
    TODO (Weekend 2): skip tenants already at max_concurrency, and pick fairly across tenants.
    """
    raise NotImplementedError


def execute(run: dict) -> None:
    """Do the job's work. The run id is the idempotency key for any side effect.

    TODO (Weekend 1): dispatch on job_type. Start with a 'noop' and a 'sleep' type.
    """
    raise NotImplementedError


def heartbeat(conn, run: dict) -> bool:
    """Extend the lease. Return False if we no longer hold it (fencing token changed).

    TODO (Weekend 1).
    """
    raise NotImplementedError


def complete(conn, run: dict) -> None:
    """Mark succeeded, but only if our fencing_token still matches. A stale worker must not win.

    TODO (Weekend 1).
    """
    raise NotImplementedError


def fail(conn, run: dict, error: str, transient: bool) -> None:
    """Retry with exponential backoff and jitter, or move to dead_letters.

    TODO (Weekend 1): backoff = min(BACKOFF_MAX, BACKOFF_BASE * 2**attempt) with random jitter,
    written to available_at. Permanent errors or attempt >= max_attempts go to dead_letters.
    """
    raise NotImplementedError


def reap_expired_leases(conn) -> int:
    """Make runs with expired leases claimable again. Return how many.

    TODO (Weekend 1).
    """
    raise NotImplementedError


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    log.info("worker %s starting", WORKER_ID)
    while True:
        try:
            # TODO (Weekend 1): open a connection from app.db.get_pool(), reap, claim, execute.
            raise NotImplementedError
        except NotImplementedError:
            log.info("worker idle: claim loop not implemented yet")
        time.sleep(config.POLL_SECONDS * 5)


if __name__ == "__main__":
    main()
