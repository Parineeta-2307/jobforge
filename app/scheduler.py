"""Scheduler process. Turns job definitions into job_runs rows when they come due.

Also hosts the SLA verifier (Weekend 2): flags runs that should have happened but did not.
"""

import logging
import time

from app import config

log = logging.getLogger("scheduler")


def create_due_runs(conn) -> int:
    """Insert a job_runs row for every occurrence that is now due. Return how many were created.

    TODO (Weekend 1): for each enabled job whose next occurrence is <= now(), insert a run with
    scheduled_for = that occurrence, using INSERT ... ON CONFLICT (job_id, scheduled_for)
    DO NOTHING. That unique key is what makes two schedulers safe.
    Decide and document what happens after downtime: skip missed occurrences, catch up, or expire.
    """
    raise NotImplementedError


def verify_sla(conn) -> list[dict]:
    """Find occurrences that should have run but did not, and runs stuck past their lease.

    TODO (Weekend 2).
    """
    raise NotImplementedError


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    log.info("scheduler starting")
    while True:
        try:
            # TODO (Weekend 1): get a connection, create_due_runs(). Weekend 2: verify_sla().
            raise NotImplementedError
        except NotImplementedError:
            log.info("scheduler idle: not implemented yet")
        time.sleep(config.POLL_SECONDS * 5)


if __name__ == "__main__":
    main()
