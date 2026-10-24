"""A small read-only status page: GET / serves the page, GET /status serves the numbers it shows.

Nothing here writes to the database. There is no authentication (see the README limitations), so
it shows every tenant's data: fine for a demo, not for a shared deployment.
"""

from datetime import UTC, datetime
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse
from psycopg.rows import dict_row

from app.db import get_pool

STATIC_DIR = Path(__file__).parent / "static"
router = APIRouter()

TENANTS_SQL = """
SELECT t.name, t.max_concurrency, t.burst_rps, t.points_quota_per_hour,
       count(r.id) FILTER (WHERE r.state = 'leased') AS leased,
       count(r.id) FILTER (WHERE r.state IN ('ready', 'retry_wait')) AS waiting,
       count(r.id) FILTER (WHERE r.state = 'succeeded') AS succeeded,
       count(r.id) FILTER (WHERE r.state = 'dead') AS dead
FROM tenants t LEFT JOIN job_runs r ON r.tenant_id = t.id
GROUP BY t.id
ORDER BY waiting DESC, leased DESC, t.name
LIMIT 20
"""

RECENT_RUNS_SQL = """
SELECT r.id, t.name AS tenant, j.name AS job, j.job_type, r.state, r.attempt, r.lease_owner,
       r.created_at
FROM job_runs r JOIN jobs j ON j.id = r.job_id JOIN tenants t ON t.id = r.tenant_id
ORDER BY r.created_at DESC
LIMIT 12
"""

DEAD_LETTERS_SQL = """
SELECT d.run_id, t.name AS tenant, j.name AS job, d.reason, d.moved_at, d.redriven_at
FROM dead_letters d JOIN job_runs r ON r.id = d.run_id JOIN jobs j ON j.id = r.job_id
JOIN tenants t ON t.id = r.tenant_id
ORDER BY d.moved_at DESC
LIMIT 10
"""


@router.get("/", include_in_schema=False)
def status_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/status")
def status() -> dict:
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            counts = {
                row["state"]: row["n"]
                for row in cur.execute(
                    "SELECT state, count(*) AS n FROM job_runs GROUP BY state"
                ).fetchall()
            }
            return {
                "as_of": datetime.now(UTC),
                "run_counts": counts,
                "tenants": cur.execute(TENANTS_SQL).fetchall(),
                "recent_runs": cur.execute(RECENT_RUNS_SQL).fetchall(),
                "dead_letters": cur.execute(DEAD_LETTERS_SQL).fetchall(),
            }
