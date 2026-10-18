"""The chaos scenario: kill workers at random while they run, then check nothing was lost/doubled.

Used by tests/test_worker_kill.py (pytest -m chaos) and by scripts/chaos_report.py.

Isolation: the scenario creates its own schema (chaos_xxxxxxxx) in the database from DATABASE_URL,
applies every file in migrations/ into it, and points the worker processes at it with
search_path. So it never touches your real tables, and leftover development data cannot be claimed
by the test workers. The schema is dropped at the end.
"""

import os
import random
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

import psycopg

ROOT = Path(__file__).parent.parent
LEASE_SECONDS = 3
HEARTBEAT_SECONDS = 1
POLL_SECONDS = 0.2


def scoped_url(base_url: str, schema: str) -> str:
    """The same database URL, but every connection starts with search_path set to the schema."""
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}options={quote(f'-c search_path={schema}')}"


def create_schema(base_url: str) -> tuple[str, str]:
    """Create a scratch schema with all migrations applied. Returns (schema, scoped_url)."""
    schema = f"chaos_{uuid4().hex[:8]}"
    with psycopg.connect(base_url, autocommit=True) as conn:
        conn.execute(f"CREATE SCHEMA {schema}")  # the name is generated above, not user input
        conn.execute(f"SET search_path TO {schema}")
        for path in sorted((ROOT / "migrations").glob("*.sql")):
            conn.execute(path.read_text(encoding="utf-8"))
    return schema, scoped_url(base_url, schema)


def drop_schema(base_url: str, schema: str) -> None:
    with psycopg.connect(base_url, autocommit=True) as conn:
        conn.execute(f"DROP SCHEMA {schema} CASCADE")


def start_worker(url: str, worker_id: str) -> subprocess.Popen:
    env = os.environ.copy()
    env.update(
        DATABASE_URL=url,
        WORKER_ID=worker_id,
        LEASE_SECONDS=str(LEASE_SECONDS),
        HEARTBEAT_SECONDS=str(HEARTBEAT_SECONDS),
        POLL_SECONDS=str(POLL_SECONDS),
    )
    return subprocess.Popen(
        [sys.executable, "-m", "app.worker"],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def insert_runs(url: str, count: int) -> list:
    """One tenant, one job, `count` ready `record` runs that each take 1 to 2 seconds."""
    with psycopg.connect(url, autocommit=True) as conn:
        tenant_id = conn.execute(
            "INSERT INTO tenants (name, max_concurrency) VALUES ('chaos', 100) RETURNING id"
        ).fetchone()[0]
        run_ids = []
        for i in range(count):
            # max_attempts=10 so a few unlucky kills cannot dead-letter a run.
            job_id = conn.execute(
                "INSERT INTO jobs (tenant_id, name, job_type, payload, max_attempts) "
                "VALUES (%s, %s, 'record', %s::jsonb, 10) RETURNING id",
                (tenant_id, f"chaos-{i}", f'{{"seconds": {random.choice([1, 1.5, 2])}}}'),
            ).fetchone()[0]
            run_ids.append(
                conn.execute(
                    "INSERT INTO job_runs (job_id, tenant_id, scheduled_for) "
                    "VALUES (%s, %s, now()) RETURNING id",
                    (job_id, tenant_id),
                ).fetchone()[0]
            )
    return run_ids


def run_scenario(
    base_url: str, runs: int = 60, workers: int = 3, kill_every: float = 2.0, timeout: float = 120
) -> dict:
    """Run the scenario once. Returns the numbers; does not assert anything (see check)."""
    schema, url = create_schema(base_url)
    procs: dict[str, subprocess.Popen] = {}
    kills: list[tuple[str, datetime]] = []
    try:
        run_ids = insert_runs(url, runs)
        started = time.time()
        for _ in range(workers):
            worker_id = f"chaos-{uuid4().hex[:6]}"
            procs[worker_id] = start_worker(url, worker_id)
        next_kill = started + kill_every
        finished = False
        with psycopg.connect(url, autocommit=True) as conn:
            while time.time() - started < timeout:
                time.sleep(0.25)
                open_runs = conn.execute(
                    "SELECT count(*) FROM job_runs WHERE state NOT IN ('succeeded', 'dead')"
                ).fetchone()[0]
                if open_runs == 0:
                    finished = True
                    break
                if time.time() >= next_kill:
                    victim = random.choice(list(procs))
                    killed_at = datetime.now(UTC)
                    procs[victim].kill()  # a hard kill: no cleanup code runs in the worker
                    procs[victim].wait()
                    kills.append((victim, killed_at))
                    del procs[victim]
                    replacement = f"chaos-{uuid4().hex[:6]}"
                    procs[replacement] = start_worker(url, replacement)
                    next_kill = time.time() + kill_every
        elapsed = time.time() - started
        stats = check(url, run_ids, kills)
        stats.update(finished=finished, seconds=round(elapsed, 1), kills=len(kills))
        return stats
    finally:
        for proc in procs.values():
            proc.kill()
            proc.wait()
        drop_schema(base_url, schema)


def check(url: str, run_ids: list, kills: list) -> dict:
    """Look at the final state. Returns the numbers and a list of problems (empty if all well)."""
    problems = []
    with psycopg.connect(url, autocommit=True) as conn:
        rows = conn.execute("SELECT state, count(*) FROM job_runs GROUP BY state").fetchall()
        states = dict(rows)
        not_succeeded = sum(n for state, n in states.items() if state != "succeeded")
        if not_succeeded:
            problems.append(f"LOST: {not_succeeded} run(s) did not end succeeded: {states}")

        effects = dict(
            conn.execute("SELECT run_id, count(*) FROM job_effects GROUP BY run_id").fetchall()
        )
        missing = [r for r in run_ids if effects.get(r, 0) == 0]
        doubled = [r for r, n in effects.items() if n > 1]  # impossible by the primary key
        if missing:
            problems.append(f"LOST: {len(missing)} run(s) have no effect row")
        if doubled:
            problems.append(f"DOUBLED: {len(doubled)} run(s) have more than one effect row")

        succeeded_attempts = dict(
            conn.execute(
                "SELECT run_id, count(*) FROM run_attempts WHERE outcome = 'succeeded' "
                "GROUP BY run_id"
            ).fetchall()
        )
        bad = [r for r in run_ids if succeeded_attempts.get(r, 0) != 1]
        if bad:
            problems.append(f"DOUBLED or LOST: {len(bad)} run(s) without exactly one success")

        # No stale overwrite: the success is the run's last attempt, and the fencing token
        # equals the number of claims (every claim adds one attempt row and bumps the token).
        stale = conn.execute(
            "SELECT count(*) FROM job_runs r WHERE r.state = 'succeeded' AND ("
            " (SELECT max(attempt) FROM run_attempts a WHERE a.run_id = r.id) <> r.attempt"
            " OR (SELECT attempt FROM run_attempts a WHERE a.run_id = r.id"
            "     AND a.outcome = 'succeeded') <> r.attempt"
            " OR (SELECT count(*) FROM run_attempts a WHERE a.run_id = r.id) <> r.fencing_token)"
        ).fetchone()[0]
        if stale:
            problems.append(f"STALE OVERWRITE: {stale} run(s) with an inconsistent final attempt")

        total_attempts, expired = conn.execute(
            "SELECT count(*), count(*) FILTER (WHERE outcome = 'lease_expired') FROM run_attempts"
        ).fetchone()

        # Recovery time: from the kill to the start of the next attempt of the run it was holding.
        recoveries = []
        for worker_id, killed_at in kills:
            rows = conn.execute(
                "SELECT n.started_at FROM run_attempts a JOIN run_attempts n "
                "ON n.run_id = a.run_id AND n.attempt = a.attempt + 1 "
                "WHERE a.worker_id = %s AND a.outcome = 'lease_expired'",
                (worker_id,),
            ).fetchall()
            recoveries += [round((r[0] - killed_at).total_seconds(), 2) for r in rows]

    return {
        "runs": len(run_ids),
        "total_attempts": total_attempts,
        "lease_expired_attempts": expired,
        "recovery_seconds": recoveries,
        "recovery_median": round(statistics.median(recoveries), 2) if recoveries else None,
        "recovery_max": max(recoveries) if recoveries else None,
        "lease_seconds": LEASE_SECONDS,
        "problems": problems,
    }
