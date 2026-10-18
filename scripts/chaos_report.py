"""Run the chaos scenario several times and save every run's numbers to docs/results/.

Run from the repo root (takes about a minute per run):
    python -m scripts.chaos_report --machine "describe your machine" [--times 5]
Report all runs in the README (min, median, max), not only the best one.
"""

import argparse
import json
import os
import platform
import statistics
from datetime import date
from pathlib import Path

import psycopg

from scripts.chaos import ROOT, run_scenario


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--machine", required=True, help="machine description, saved in the file")
    parser.add_argument("--times", type=int, default=5)
    args = parser.parse_args()
    base_url = os.environ["DATABASE_URL"]

    with psycopg.connect(base_url) as conn:
        postgres = conn.execute("SHOW server_version").fetchone()[0]

    results = []
    for n in range(1, args.times + 1):
        stats = run_scenario(base_url)
        results.append(stats)
        print(
            f"run {n}: runs={stats['runs']} kills={stats['kills']} "
            f"attempts={stats['total_attempts']} lease_expired={stats['lease_expired_attempts']} "
            f"recovery median={stats['recovery_median']}s max={stats['recovery_max']}s "
            f"problems={stats['problems']}"
        )

    recoveries = [s for r in results for s in r["recovery_seconds"]]
    summary = {
        "runs": len(results),
        "all_runs_clean": all(not r["problems"] and r["finished"] for r in results),
        "recovery_seconds_min": min(recoveries) if recoveries else None,
        "recovery_seconds_median": round(statistics.median(recoveries), 2) if recoveries else None,
        "recovery_seconds_max": max(recoveries) if recoveries else None,
    }
    out = {
        "date": date.today().isoformat(),
        "machine": args.machine,
        "python": platform.python_version(),
        "postgres": postgres,
        "scenario": "60 record runs, 3 workers, one hard kill every 2 s, lease 3 s, heartbeat 1 s",
        "summary": summary,
        "results": results,
    }
    path = Path(ROOT) / "docs" / "results" / f"chaos_{date.today().isoformat()}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"saved {path}")


if __name__ == "__main__":
    main()
