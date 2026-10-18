"""EXPLAIN (ANALYZE, BUFFERS) for the claim query on synthetic data. Saves the raw output.

Everything happens inside one transaction that is rolled back at the end, so nothing is left in the
database. Run from the repo root:
    python -m scripts.explain_claim --machine "describe it" --out docs/results/explain_claim.txt
It measures the pick query with and without the index from migrations/003.
Data: 50 tenants. Tenant 0 has a backlog of 3000 ready runs, the other 49 have 100 each, and the
first 10 tenants each have 2 leased runs. About 7,900 runs in total.
"""

import argparse
import platform
from pathlib import Path
from uuid import uuid4

import psycopg

from app import config
from app.worker import PICK_TENANT_SQL

# The claim query before W2-4 (git history: app/worker.py at commit de7f61f), for comparison.
OLD_CLAIM_SQL = """
SELECT r.id, r.job_id, r.tenant_id, j.job_type, j.payload, j.max_attempts, r.attempt_base
FROM job_runs r JOIN jobs j ON j.id = r.job_id
WHERE r.state IN ('ready', 'retry_wait') AND r.available_at <= now() AND j.enabled
ORDER BY r.available_at LIMIT 1
FOR UPDATE OF r SKIP LOCKED
"""

# Step 2 of the new claim (the per-tenant pick), run for the busiest tenant.
OLDEST_RUN_FOR_TENANT_SQL = """
SELECT r.id FROM job_runs r JOIN jobs j ON j.id = r.job_id
WHERE r.tenant_id = %s AND r.state IN ('ready', 'retry_wait') AND r.available_at <= now()
  AND j.enabled
ORDER BY r.available_at LIMIT 1
FOR UPDATE OF r SKIP LOCKED
"""


# Migration 003 adds this index. The script measures the pick query with and without it; the
# DROP / CREATE happen inside the transaction that is rolled back, so your database is unchanged.
INDEX_NAME = "idx_runs_tenant_claimable"
INDEX_MIGRATION = Path(__file__).parent.parent / "migrations" / "003_claim_index.sql"


def load_data(conn) -> str:
    tag = uuid4().hex[:8]
    conn.execute(
        "INSERT INTO tenants (name) SELECT 'explain-' || %s || '-' || g "
        "FROM generate_series(0, 49) g",
        (tag,),
    )
    conn.execute(
        "INSERT INTO jobs (tenant_id, name, job_type) "
        "SELECT id, 'j', 'noop' FROM tenants WHERE name LIKE %s",
        (f"explain-{tag}-%",),
    )
    # 3000 ready runs for tenant 0, 100 for every other tenant.
    conn.execute(
        "INSERT INTO job_runs (job_id, tenant_id, scheduled_for, available_at) "
        "SELECT j.id, j.tenant_id, now() + g * interval '1 second', now() - interval '1 hour' "
        "FROM jobs j JOIN tenants t ON t.id = j.tenant_id "
        "CROSS JOIN LATERAL generate_series(1, CASE WHEN t.name = %s THEN 3000 ELSE 100 END) g "
        "WHERE t.name LIKE %s",
        (f"explain-{tag}-0", f"explain-{tag}-%"),
    )
    # Two leased runs for each of the first 10 tenants.
    conn.execute(
        "UPDATE job_runs SET state = 'leased', lease_owner = 'x', "
        "lease_expires_at = now() + interval '1 hour' WHERE id IN ("
        "  SELECT id FROM (SELECT r.id, row_number() OVER (PARTITION BY r.tenant_id "
        "  ORDER BY r.scheduled_for) rn, t.name FROM job_runs r JOIN tenants t "
        "  ON t.id = r.tenant_id WHERE t.name LIKE %s) x "
        "  WHERE rn <= 2 AND substring(name from '[0-9]+$')::int < 10)",
        (f"explain-{tag}-%",),
    )
    conn.execute("ANALYZE tenants, jobs, job_runs")
    return tag


def explain(conn, title, sql, params=None) -> str:
    rows = conn.execute("EXPLAIN (ANALYZE, BUFFERS) " + sql, params).fetchall()
    return f"===== {title} =====\n" + "\n".join(r[0] for r in rows) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--machine", required=True, help="machine description, saved in the file")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    with psycopg.connect(config.DATABASE_URL) as conn:
        version = conn.execute("SHOW server_version").fetchone()[0]
        before = conn.execute("SELECT count(*) FROM job_runs").fetchone()[0]
        tag = load_data(conn)
        counts = conn.execute(
            "SELECT count(*), count(*) FILTER (WHERE state = 'leased') FROM job_runs r "
            "JOIN tenants t ON t.id = r.tenant_id WHERE t.name LIKE %s",
            (f"explain-{tag}-%",),
        ).fetchone()
        busiest = conn.execute(
            "SELECT id FROM tenants WHERE name = %s", (f"explain-{tag}-0",)
        ).fetchone()[0]
        out = [
            "claim query EXPLAIN (ANALYZE, BUFFERS), synthetic data, rolled back afterwards\n",
            f"machine: {args.machine}\n",
            f"python: {platform.python_version()}, postgres: {version}\n",
            f"job_runs rows in the database before the script: {before}\n",
            f"synthetic rows: {counts[0]} runs ({counts[1]} leased), 50 tenants\n\n",
        ]
        # Part 1: WITH the index from migration 003 (create it from the file if not applied yet).
        conn.execute(INDEX_MIGRATION.read_text(encoding="utf-8"))
        conn.execute("ANALYZE job_runs")
        out += [
            "#" * 70 + "\n# WITH index " + INDEX_NAME + " (migrations/003_claim_index.sql)\n",
            explain(conn, "NEW step 1: pick the tenant (PICK_TENANT_SQL)", PICK_TENANT_SQL),
            "\n",
            explain(
                conn,
                "NEW step 2: oldest run of the busiest tenant",
                OLDEST_RUN_FOR_TENANT_SQL,
                (busiest,),
            ),
            "\n",
        ]
        # Part 2: WITHOUT the index (dropped inside the transaction, rolled back below).
        conn.execute(f"DROP INDEX {INDEX_NAME}")
        conn.execute("ANALYZE job_runs")
        out += [
            "#" * 70 + "\n# WITHOUT index " + INDEX_NAME + "\n",
            explain(conn, "NEW step 1: pick the tenant (PICK_TENANT_SQL)", PICK_TENANT_SQL),
            "\n",
            explain(
                conn,
                "NEW step 2: oldest run of the busiest tenant",
                OLDEST_RUN_FOR_TENANT_SQL,
                (busiest,),
            ),
            "\n",
            "#" * 70 + "\n# For comparison\n",
            explain(conn, "OLD claim query (before W2-4)", OLD_CLAIM_SQL),
        ]
        conn.rollback()
    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        f.writelines(out)
    print("".join(out))


if __name__ == "__main__":
    main()
