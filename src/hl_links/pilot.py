"""Resume the fixed October 23 pilot with bounded, independent UTC-day receipts.

The existing T02 acquisition policy remains the source of truth. A completed
day is immutable; an interrupted attempt is never treated as a receipt.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from .acquire import Vike, Reservoir, Window, acquire_snapshots, _AGENT
from .resources import private_runs_dir
from .study import PILOT_CUTOFF, _utc

DAY = timedelta(days=1)
ARCHIVE_START = _utc("2026-05-01T00:00:00Z")
LEDGER_START = PILOT_CUTOFF - 60 * DAY
FORWARD_END = PILOT_CUTOFF + 30 * DAY


def acquire_day(root, start):
    day = start.date().isoformat()
    end = start + DAY
    if start < ARCHIVE_START or end > FORWARD_END or start != _utc(day + "T00:00:00Z"):
        raise ValueError("Outside declared pilot day range")
    destination = root / day
    if destination.exists():
        if not all((destination / name).is_file() for name in ("fills.parquet", "ledger.parquet", "metadata.json")):
            raise ValueError("Existing day lacks completed artifacts")
        return json.loads((destination / "metadata.json").read_text())
    with TemporaryDirectory(prefix=f".{day}-", dir=root) as stage:
        output = Path(stage) / "snapshot"
        con = duckdb.connect(":memory:", config={"custom_user_agent": _AGENT})
        try:
            con.execute("SET extension_directory = ?", [str(private_runs_dir() / "duckdb_extensions")])
            con.execute("LOAD httpfs")
            vike = Vike(os.getenv("VIKE_API_KEY"))
            reservoir = Reservoir(con, "reservoir")
            meta = acquire_snapshots(output, [Window(start, end)],
                                     [Window(start, end)] if LEDGER_START <= start < PILOT_CUTOFF else [],
                                     connection=con, vike_archive=vike.archive,
                                     reservoir_archive=reservoir.archive, ledger_page=vike.ledger,
                                     page_limit=5000, max_pages=200)
        finally:
            con.close()
        output.rename(destination)
        return meta


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--start", type=_utc, required=True)
    p.add_argument("--end", type=_utc, required=True)
    p.add_argument("--workers", type=int, choices=(1, 2), default=1)
    args = p.parse_args(argv)
    root = args.root.resolve()
    private = private_runs_dir()
    if root == private or not root.is_relative_to(private):
        p.error("Root must be inside runs/")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    days = []
    start = args.start
    while start < args.end:
        days.append(start)
        start += DAY
    if not days or start != args.end:
        p.error("Request must consist of whole UTC days")

    def run(start):
        try:
            meta = acquire_day(root, start)
            fills = meta["fills"]["acquisition"]["slices"][0]
            ledger = meta["ledger"]["acquisition"]["slices"]
            selected = next((a for a in fills["attempts"] if a["provider"] == fills.get("selected_provider")), {})
            print(start.date(), "fills", fills.get("selected_provider"), selected.get("rows"),
                  fills["coverage_complete"], "ledger", [(s["retained_rows"], s["complete"]) for s in ledger],
                  flush=True)
        except Exception as exc:
            # Provider/SQL errors may contain signed URLs; emit only safe type.
            print(start.date(), "FAILED", type(exc).__name__, flush=True)
            return False
        return True

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        return 0 if all(list(pool.map(run, days))) else 1


if __name__ == "__main__":
    raise SystemExit(main())
