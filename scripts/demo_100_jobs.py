"""Weekend 1 "done when" check: submit 100 jobs, let the workers run them, verify none ran twice.

Start these first, each in its own terminal (venv active, DATABASE_URL set if not the default):
    python -m uvicorn app.main:app
    python -m app.scheduler
    python -m app.worker        (three of these)
Then run, from the repo root:  python -m scripts.demo_100_jobs
"""

import sys
import time
from uuid import uuid4

import httpx
import psycopg

from app import config

API = "http://127.0.0.1:8000"
JOBS = 100
TIMEOUT_SECONDS = 180


def main() -> int:
    # One shared client: httpx.post() builds a new client (and loads TLS certificates) per call,
    # which cost ~0.7s per request on Windows.
    client = httpx.Client(base_url=API)
    tenant = client.post("/tenants", json={"name": f"demo-{uuid4()}"}).json()
    tenant_id = tenant["id"]
    print(f"tenant {tenant_id}")
    try:
        start = time.time()
        for i in range(JOBS):
            body = {
                "tenant_id": tenant_id,
                "name": f"job-{i}",
                "job_type": "sleep",
                "payload": {"seconds": 1},
            }
            r = client.post("/jobs", json=body)
            assert r.status_code == 200, r.text
        print(f"submitted {JOBS} jobs in {time.time() - start:.1f}s")

        with psycopg.connect(config.DATABASE_URL, autocommit=True) as conn:
            while time.time() - start < TIMEOUT_SECONDS:
                rows = conn.execute(
                    "SELECT state, count(*) FROM job_runs WHERE tenant_id = %s GROUP BY state",
                    (tenant_id,),
                ).fetchall()
                counts = dict(rows)
                print(f"{time.time() - start:5.1f}s  {counts}")
                if counts.get("succeeded", 0) == JOBS:
                    break
                time.sleep(2)
            else:
                print("TIMEOUT: not all runs succeeded")
                return 1
            total_seconds = time.time() - start

            # None twice: every run has exactly one attempt, and that attempt succeeded.
            bad = conn.execute(
                "SELECT r.id FROM job_runs r WHERE r.tenant_id = %s AND ("
                "(SELECT count(*) FROM run_attempts a WHERE a.run_id = r.id) <> 1 "
                "OR (SELECT count(*) FROM run_attempts a "
                "    WHERE a.run_id = r.id AND a.outcome = 'succeeded') <> 1 "
                "OR r.attempt <> 1)",
                (tenant_id,),
            ).fetchall()
            workers = conn.execute(
                "SELECT a.worker_id, count(*) FROM run_attempts a "
                "JOIN job_runs r ON r.id = a.run_id WHERE r.tenant_id = %s "
                "GROUP BY a.worker_id ORDER BY a.worker_id",
                (tenant_id,),
            ).fetchall()
        print(f"all {JOBS} succeeded after {total_seconds:.1f}s")
        print("runs per worker:", workers)
        print("runs that ran more than once or did not succeed exactly once:", len(bad))
        return 1 if bad else 0
    finally:
        with psycopg.connect(config.DATABASE_URL, autocommit=True) as conn:
            conn.execute("DELETE FROM tenants WHERE id = %s", (tenant_id,))


if __name__ == "__main__":
    sys.exit(main())
