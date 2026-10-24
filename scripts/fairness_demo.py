"""Fairness end to end: a noisy tenant queues a big backlog, then a quiet tenant submits a few jobs.

Measures how long the quiet tenant's jobs take to finish while the backlog is still being worked.
With fair claiming they are served early; with oldest-first claiming they would wait behind the
whole backlog. Start the API, the scheduler and three workers first (see docs/SPEC.md section 6):
    python -m scripts.fairness_demo --machine "describe it" --out docs/results/fairness_RUN.txt
"""

import argparse
import time
from uuid import uuid4

import httpx
import psycopg

from app import config

API = "http://127.0.0.1:8000"


def submit_jobs(client, tenant_id, prefix, count):
    for i in range(count):
        body = {
            "tenant_id": tenant_id,
            "name": f"{prefix}-{i}",
            "job_type": "sleep",
            "payload": {"seconds": 1},
        }
        r = client.post("/jobs", json=body)
        assert r.status_code == 200, r.text


def succeeded(conn, tenant_id) -> int:
    return conn.execute(
        "SELECT count(*) FROM job_runs WHERE tenant_id = %s AND state = 'succeeded'", (tenant_id,)
    ).fetchone()[0]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backlog", type=int, default=300)
    parser.add_argument("--quiet-jobs", type=int, default=10)
    parser.add_argument("--machine", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    client = httpx.Client(base_url=API, timeout=30)
    tag = uuid4().hex[:8]
    # The noisy tenant's cap is raised so the test shows fair claiming, not the cap.
    noisy = client.post(
        "/tenants",
        json={"name": f"fair-noisy-{tag}", "points_quota_per_hour": 1_000_000,
              "burst_rps": 10_000, "max_concurrency": 100},
    ).json()["id"]
    quiet = client.post("/tenants", json={"name": f"fair-quiet-{tag}"}).json()["id"]
    try:
        with psycopg.connect(config.DATABASE_URL, autocommit=True) as conn:
            submit_jobs(client, noisy, "noisy", args.backlog)
            # Wait until the scheduler has made a run for every noisy job: the backlog is real.
            deadline = time.time() + 60
            while time.time() < deadline:
                queued = conn.execute(
                    "SELECT count(*) FROM job_runs WHERE tenant_id = %s", (noisy,)
                ).fetchone()[0]
                if queued == args.backlog:
                    break
                time.sleep(0.5)

            start = time.time()
            submit_jobs(client, quiet, "quiet", args.quiet_jobs)
            quiet_done = None
            while time.time() - start < 180:
                if succeeded(conn, quiet) == args.quiet_jobs:
                    quiet_done = time.time() - start
                    break
                time.sleep(0.25)
            noisy_done = succeeded(conn, noisy)
    finally:
        with psycopg.connect(config.DATABASE_URL, autocommit=True) as conn:
            for tenant_id in (noisy, quiet):
                conn.execute("DELETE FROM tenants WHERE id = %s", (tenant_id,))

    lines = [
        f"machine: {args.machine}",
        f"noisy backlog: {args.backlog} sleep(1s) runs; quiet tenant: {args.quiet_jobs} jobs",
        "setup: 1 API process, 1 scheduler, 3 workers, fair claiming (migrations 001-003)",
        f"quiet tenant: all {args.quiet_jobs} jobs succeeded "
        + (f"{quiet_done:.1f} s after the first was submitted"
           if quiet_done is not None else "NOT within 180 s"),
        f"noisy tenant runs succeeded by then: {noisy_done} of {args.backlog}",
    ]
    print("\n".join(lines))
    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
