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
from .resources import SnapshotBudgetExceeded, private_runs_dir
from .study import PILOT_CUTOFF, _utc

DAY = timedelta(days=1)
ARCHIVE_START = _utc("2026-05-01T00:00:00Z")
LEDGER_START = PILOT_CUTOFF - 60 * DAY
FORWARD_END = PILOT_CUTOFF + 30 * DAY


def day_candidates(root, day, *, extra=None):
    paths = [extra, root / day] if extra is not None else [root / day]
    paths.extend(sorted((root / ".replacements" / day).glob("*/" + day)))
    return [p for p in paths if p.is_dir()]


def _day_metadata(directory):
    if not all((directory / name).is_file()
               for name in ("fills.parquet", "ledger.parquet", "metadata.json")):
        raise ValueError("Existing day lacks completed artifacts")
    return json.loads((directory / "metadata.json").read_text())


def complete_day(declaration, start, name):
    intervals = [tuple(map(_utc, pair)) for pair in declaration["intervals"]]
    slices = declaration["acquisition"]["slices"]
    return (intervals == [(start, start + DAY)] and len(slices) == 1
            and bool(slices[0].get("coverage_complete" if name == "fills" else "complete",
                                   slices[0].get("coverage_complete"))))


def select_day(root, start, name, *, extra=None):
    """Prefer the first complete immutable receipt; never union attempts."""
    day = start.date().isoformat()
    first = None
    for path in day_candidates(root, day, extra=extra):
        declaration = _day_metadata(path)[name]
        candidate = (path, declaration)
        if first is None:
            first = candidate
        if complete_day(declaration, start, name):
            return candidate
    return first


def acquire_day(root, start, *, max_attempts=2, ledger_extra=None, analysis=None):
    day = start.date().isoformat()
    if not ARCHIVE_START <= start < FORWARD_END or start != _utc(day + "T00:00:00Z"):
        raise ValueError("Outside declared pilot day range")
    if start >= PILOT_CUTOFF:
        if analysis is None or not (Path(analysis) / "frozen" / "receipt.json").is_file():
            raise ValueError("Forward acquisition requires a verified historical freeze")
        from .pilot_run import _load_checkpoint, _validate_frozen
        analysis = Path(analysis)
        _validate_frozen(analysis, _load_checkpoint(analysis / "frozen"))
    if not 1 <= max_attempts <= 2:
        raise ValueError("Invalid day attempt budget")
    needs_ledger = LEDGER_START <= start < PILOT_CUTOFF

    def selected():
        return {name: select_day(root, start, name, extra=ledger_extra if name == "ledger" else None)
                for name in ("fills", "ledger")}

    def complete(items):
        return (items["fills"] is not None and complete_day(items["fills"][1], start, "fills")
                and (not needs_ledger or (items["ledger"] is not None
                     and complete_day(items["ledger"][1], start, "ledger"))))

    for _ in range(max_attempts):
        items = selected()
        if complete(items):
            break
        destination = root
        if (root / day).exists():
            attempts = root / ".replacements" / day
            attempts.mkdir(parents=True, exist_ok=True, mode=0o700)
            # Stable attempt order: later ledger retries must not change a
            # previously selected complete fill receipt (and vice versa).
            sequence = max((int(p.name.removeprefix("attempt-"))
                            for p in attempts.glob("attempt-*")), default=0) + 1
            destination = attempts / f"attempt-{sequence:08d}"
            destination.mkdir(mode=0o700)
        fill_needed = items["fills"] is None or not complete_day(items["fills"][1], start, "fills")
        ledger_needed = needs_ledger and (items["ledger"] is None
                         or not complete_day(items["ledger"][1], start, "ledger"))
        try:
            _capture_day(destination, start, fill_needed=fill_needed, ledger_needed=ledger_needed)
        except SnapshotBudgetExceeded:
            raise
        except Exception as exc:
            print("day acquisition failed", day, type(exc).__name__, flush=True)
    items = selected()
    if any(item is None for item in items.values()):
        raise ValueError("No completed acquisition receipt for day")
    return {name: item[1] for name, item in items.items()}


def _capture_day(root, start, *, fill_needed, ledger_needed):
    day = start.date().isoformat()
    end = start + DAY
    if start < ARCHIVE_START or end > FORWARD_END or start != _utc(day + "T00:00:00Z"):
        raise ValueError("Outside declared pilot day range")
    destination = root / day
    if destination.exists():
        raise FileExistsError(destination)
    with TemporaryDirectory(prefix=f".{day}-", dir=root) as stage:
        output = Path(stage) / "snapshot"
        con = duckdb.connect(":memory:", config={"custom_user_agent": _AGENT})
        try:
            con.execute("SET extension_directory = ?", [str(private_runs_dir() / "duckdb_extensions")])
            con.execute("LOAD httpfs")
            vike = Vike(os.getenv("VIKE_API_KEY"))
            reservoir = Reservoir(con, "reservoir")
            meta = acquire_snapshots(output, [Window(start, end)] if fill_needed else [],
                                     [Window(start, end)] if ledger_needed else [],
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
    if args.end > PILOT_CUTOFF:
        p.error("Forward acquisition requires the staged pilot runner and a verified historical freeze")
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
        return (complete_day(meta["fills"], start, "fills")
                and (not LEDGER_START <= start < PILOT_CUTOFF
                     or complete_day(meta["ledger"], start, "ledger")))

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        return 0 if all(list(pool.map(run, days))) else 1


if __name__ == "__main__":
    raise SystemExit(main())
