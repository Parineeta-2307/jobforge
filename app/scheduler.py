"""Scheduler process. Turns job definitions into job_runs rows when they come due.

Also hosts the SLA verifier (Weekend 2): flags runs that should have happened but did not.
"""

import logging
import time

from app import config
from app.db import get_pool

log = logging.getLogger("scheduler")


def create_due_runs(conn) -> int:
    """Insert a job_runs row for every occurrence that is now due. Return how many were created.

    One-off job (interval_seconds is NULL): a single run, scheduled_for = the job's created_at.
    Recurring job: occurrences sit on a fixed grid, created_at + k * interval_seconds. We create
    only the latest occurrence that is due (the biggest k with that time <= now()).

    After downtime this means: SKIP. If the scheduler was down for an hour and the interval is
    60 seconds, we do not create 60 catch-up runs, only the current one. The missed ones are lost
    on purpose (see decisions.md, D-section on downtime).

    Because the occurrence time is computed from the grid, two schedulers working at the same
    moment compute the same scheduled_for. The UNIQUE (job_id, scheduled_for) key plus
    ON CONFLICT DO NOTHING means only one of them inserts it.
    """
    with conn.transaction():
        one_off = conn.execute(
            "INSERT INTO job_runs (job_id, tenant_id, scheduled_for) "
            "SELECT j.id, j.tenant_id, j.created_at FROM jobs j "
            "WHERE j.enabled AND j.interval_seconds IS NULL AND j.created_at <= now() "
            "AND NOT EXISTS (SELECT 1 FROM job_runs r WHERE r.job_id = j.id) "
            "ON CONFLICT (job_id, scheduled_for) DO NOTHING"
        ).rowcount
        recurring = conn.execute(
            "INSERT INTO job_runs (job_id, tenant_id, scheduled_for) "
            "SELECT j.id, j.tenant_id, j.created_at + make_interval(secs => "
            "(floor(extract(epoch FROM now() - j.created_at) / j.interval_seconds) "
            "* j.interval_seconds)::double precision) "
            "FROM jobs j "
            "WHERE j.enabled AND j.interval_seconds IS NOT NULL AND j.created_at <= now() "
            "ON CONFLICT (job_id, scheduled_for) DO NOTHING"
        ).rowcount
    return one_off + recurring


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
            with get_pool().connection() as conn:
                created = create_due_runs(conn)
            if created:
                log.info("created %d run(s)", created)
        except Exception:
            # A database hiccup must not kill the scheduler. Log it and try again next tick.
            log.exception("create_due_runs failed")
        time.sleep(config.POLL_SECONDS)


if __name__ == "__main__":
    main()
