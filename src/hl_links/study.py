"""Invocable, retrospective funding-link study over local Parquet snapshots."""

import argparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import statistics

import duckdb
import polars as pl

from .cohorts import WalletRecord, make_cohorts
from .fills import FillArchive
from .ledger import verify_transfers

PILOT_CUTOFF = datetime(2026, 10, 23, tzinfo=timezone.utc)
DAY = timedelta(days=1)
_TRADE_ISSUES = ("invalid_keys", "conflicting_keys", "cross_window_conflicting_keys", "invalid_notional")
_PNL_ISSUES = ("unknown_fees", "invalid_pnl")


def _has_issues(row: dict, keys: tuple[str, ...]) -> bool:
    return any(row.get(key, 0) for key in keys)


def _utc(value: str | datetime) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("All timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _json_default(value):
    if isinstance(value, datetime):
        return _utc(value).isoformat().replace("+00:00", "Z")
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Not JSON serializable: {type(value).__name__}")


def _sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _code_digest() -> str:
    files = {path.name: _sha256(path) for path in sorted(Path(__file__).parent.glob("*.py"))}
    return hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()


def _input_receipt(path: Path | None, metadata: dict, key: str) -> dict:
    declaration = metadata.get(key, {})
    intervals = []
    for pair in declaration.get("intervals", []):
        start, end = map(_utc, pair)
        if start >= end:
            raise ValueError(f"{key} coverage must use nonempty half-open intervals")
        intervals.append((start, end))
    present = path is not None and path.is_file()
    return {
        "present": present,
        "sha256": _sha256(path) if present else None,
        "source": declaration.get("source"),
        "retrieval_id": declaration.get("retrieval_id"),
        "declared_intervals": sorted(set(intervals)),
        **({"acquisition": declaration["acquisition"]} if "acquisition" in declaration else {}),
    }


def _gaps(receipt: dict, start: datetime, end: datetime) -> list:
    """Coverage declarations are not inferred from sparse event counts."""
    intervals = receipt["declared_intervals"] if receipt["present"] else []
    cursor = start
    gaps = []
    for left, right in intervals:
        if right <= cursor or left >= end:
            continue
        if left > cursor:
            gaps.append((cursor, min(left, end)))
        cursor = max(cursor, right)
        if cursor >= end:
            break
    if cursor < end:
        gaps.append((cursor, end))
    return gaps


def _wallet_records(rows: list[dict]) -> list[WalletRecord]:
    return [
        WalletRecord(
            row["wallet"], row["first_trade_at"], row["last_trade_at"],
            None if _has_issues(row, _PNL_ISSUES) else row["net_pnl"],
            row["known_at"],
        )
        for row in rows
        if not _has_issues(row, _TRADE_ISSUES)
    ]


def _cohort_scores(wallets: list[str], forward: dict[str, dict], *, coverage_complete: bool) -> dict:
    results = {}
    scores = []
    forward_trading = zero_volume = missing_data = invalid = unknown_fee_wallets = 0
    for wallet in wallets:
        row = forward.get(wallet)
        if row:
            forward_trading += 1
            unknown_fee_wallets += bool(row["unknown_fees"])
        if row and _has_issues(row, _TRADE_ISSUES + _PNL_ISSUES):
            invalid += 1
            results[wallet] = {"status": "invalid_economics", "score": None}
        elif row is None or row["notional"] == 0:
            if coverage_complete:
                zero_volume += 1
                results[wallet] = {"status": "zero_volume", "score": None}
            else:
                missing_data += 1
                results[wallet] = {"status": "missing_data", "score": None}
        else:
            score = float(100 * row["net_pnl"] / row["notional"])
            scores.append(score)
            results[wallet] = {
                "status": "valid", "score": score,
                "net_pnl_usd": row["net_pnl"], "notional_usd": row["notional"],
            }
    return {
        "eligible": len(wallets),
        "forward_trading": forward_trading,
        "valid": len(scores),
        "zero_volume": zero_volume,
        "missing_data": missing_data,
        "coverage_complete": coverage_complete,
        "invalid": invalid,
        "unknown_fee_wallets": unknown_fee_wallets,
        "mean": statistics.fmean(scores) if scores else None,
        "median": statistics.median(scores) if scores else None,
        "sample_sd": statistics.stdev(scores) if len(scores) >= 2 else None,
        "wallet_scores": results,
    }


def _compare(linked: dict, control: dict) -> dict:
    difference = None
    interval = None
    if linked["valid"] and control["valid"]:
        difference = linked["mean"] - control["mean"]
        if linked["valid"] >= 2 and control["valid"] >= 2:
            se = math.hypot(
                linked["sample_sd"] / math.sqrt(linked["valid"]),
                control["sample_sd"] / math.sqrt(control["valid"]),
            )
            interval = [difference - 1.96 * se, difference + 1.96 * se]
    return {
        "linked": linked,
        "no_observed_link": control,
        "mean_difference": difference,
        "descriptive_95pct_interval": interval,
    }


def run_study(
    fills_path: Path | None, ledger_path: Path | None, metadata: dict,
    *, cutoff: datetime = PILOT_CUTOFF,
) -> dict:
    """Freeze cohorts using pre-cutoff facts, then evaluate eligible wallets.

    Unknown availability is explicit retrospective evidence, never a fabricated
    historical observation. Source coverage is caller-declared, not certified.
    """
    cutoff = _utc(cutoff)
    history_start, forward_end = cutoff - 60 * DAY, cutoff + 30 * DAY
    fills_path = Path(fills_path) if fills_path is not None else None
    ledger_path = Path(ledger_path) if ledger_path is not None else None
    fills_receipt = _input_receipt(fills_path, metadata, "fills")
    ledger_receipt = _input_receipt(ledger_path, metadata, "ledger")
    archive_start = min(
        (start for start, _ in fills_receipt["declared_intervals"]), default=None
    )
    fills_receipt["archive_start"] = archive_start
    fills_receipt["historical_gaps"] = _gaps(
        fills_receipt, min(history_start, archive_start or history_start), cutoff
    )
    fills_receipt["forward_gaps"] = _gaps(fills_receipt, cutoff, forward_end)
    ledger_receipt["historical_gaps"] = _gaps(ledger_receipt, history_start, cutoff)

    archive = FillArchive(fills_path) if fills_receipt["present"] else None
    try:
        history, history_counts = archive.summarize(
            start=None, end=cutoff, score_start=history_start
        ) if archive else ([], {})
        transfers, evidence, ledger_counts = verify_transfers(ledger_path, cutoff)
        frozen = make_cohorts(_wallet_records(history), transfers, cutoff=cutoff)
        cohorts = {
            "proven": sorted(frozen.proven),
            "linked": sorted(frozen.linked),
            "no_observed_link": sorted(frozen.unlinked),
            "links": [evidence[edge.transaction_id] for edge in frozen.links],
        }
        # No forward records enter the historical queries or ledger matcher.
        # The only membership input to the next query is the frozen cohort.
        eligible = sorted(frozen.linked | frozen.unlinked)
        fill_input_counts = archive.input_diagnostics(
            forward_start=cutoff, forward_end=forward_end
        ) if archive else {"invalid_event_time_rows": 0, "forward_invalid_wallet_rows": 0}
        forward, forward_counts = archive.summarize(
            start=cutoff, end=forward_end, score_start=cutoff, wallets=eligible
        ) if archive else ([], {})
    finally:
        if archive:
            archive.close()

    forward_by_wallet = {row["wallet"]: row for row in forward}
    coverage_complete = not (
        fills_receipt["forward_gaps"] or any(fill_input_counts.values())
    )
    comparison = _compare(
        _cohort_scores(cohorts["linked"], forward_by_wallet, coverage_complete=coverage_complete),
        _cohort_scores(cohorts["no_observed_link"], forward_by_wallet, coverage_complete=coverage_complete),
    )
    limitations = [
        "Retrospectively reconstructed; ingestion is not historical availability.",
        "First observed main-perpetual trade is left-censored by archive coverage, not true wallet inception.",
        "No observed link is not proof of independence, common ownership, or complete transfer coverage.",
        "Fee-adjusted realized fill PnL per $100 traded excludes funding and unrealized PnL; not total account or follower return.",
        "Equal-wallet weighting; descriptive normal interval, when available, is not cluster-aware or causal inference.",
        "Coverage intervals and retrieval identities are declared by the snapshot producer, not independently certified.",
    ]
    if not coverage_complete:
        limitations.append("Forward coverage incomplete: zero observed volume is missing data, and positive-volume scores describe observed fills only.")
    partial = False
    for name, receipt in (("fills", fills_receipt), ("ledger", ledger_receipt)):
        if not receipt["present"]:
            limitations.append(f"{name} snapshot unavailable.")
            partial = True
        if not receipt["source"] or not receipt["retrieval_id"]:
            limitations.append(f"{name} source or retrieval identity missing.")
            partial = True
        if receipt["historical_gaps"] or receipt.get("forward_gaps"):
            limitations.append(f"{name} declared coverage has required-window gaps.")
            partial = True
    for name, counts in (("historical fills", history_counts), ("forward fills", forward_counts)):
        if counts.get("unknown_availability"):
            limitations.append(f"{name}: unknown historical availability is retained only as retrospective evidence.")
        if counts.get("late_known_rows"):
            if name == "historical fills":
                limitations.append("Historical fills: explicitly later-known rows excluded.")
            else:
                limitations.append("Forward fills: later-known rows included by event time in retrospective outcomes.")
        if _has_issues(counts, _TRADE_ISSUES + _PNL_ISSUES):
            limitations.append(f"{name}: unresolved fee units, economics, traded notional or fill identities; affected scores or eligibility excluded.")
            partial = True
    for name, counts in (("Fill input", fill_input_counts), ("Ledger input", ledger_counts)):
        if counts["invalid_event_time_rows"]:
            limitations.append(f"{name}: missing event timestamps; undated records cannot be assigned to either study window.")
            partial = True
    if fill_input_counts["forward_invalid_wallet_rows"]:
        limitations.append("Forward fills: missing wallet identities; unattributable fills make coverage incomplete.")
        partial = True
    if ledger_counts["unknown_availability_rows"]:
        limitations.append("Ledger availability is unknown for some historical evidence.")
    if ledger_counts["incomplete_window_rows"]:
        limitations.append("Unexhausted ledger windows cannot establish verified transfers.")
        partial = True
    if any(ledger_counts[key] for key in ("one_sided", "ambiguous", "incompatible", "invalid_rows", "late_known_rows")):
        limitations.append("Some ledger events are missing a side, incompatible, ambiguous, invalid or later-known; no qualifying edge inferred.")
    if comparison["mean_difference"] is None:
        status = "inconclusive"
        limitations.append("At least one cohort has no valid forward denominator; no comparison estimated.")
    else:
        status = "partial" if partial else "comparison_available"

    report = {
        "status": status,
        "cohorts": cohorts,
        "verified_transfers": [evidence[tx] for tx in sorted(evidence)],
        "comparison": comparison,
        "limitations": limitations,
        "manifest": {
            "cutoff": cutoff,
            "mode": "retrospectively_reconstructed",
            "windows": {
                "historical": [history_start, cutoff],
                "forward": [cutoff, forward_end],
                "activity": [cutoff - 7 * DAY, cutoff],
            },
            "parameters": {
                "market": "Hyperliquid main perpetuals",
                "proven_min_observed_age_days": 60,
                "proven_min_net_realized_usd": 20000,
                "young_min_age_days": 7,
                "young_max_age_days_exclusive": 60,
                "transfer_min_usd": 100,
                "forward_days": 30,
                "historical_availability_policy": "exclude explicitly later-known records",
                "forward_outcome_policy": "event time regardless of later availability",
                "historical_notional_policy": "positive notional required for trade evidence",
                "fee_policy": "signed USD only; no implicit non-USD conversion",
                "fill_money_decimal_places": 12,
                "weighting": "equal wallet",
            },
            "inputs": {"fills": fills_receipt, "ledger": ledger_receipt},
            "diagnostics": {
                "fill_input": fill_input_counts,
                "historical_fills": history_counts,
                "forward_fills": forward_counts,
                "ledger": ledger_counts,
            },
            "counts": {
                "proven": len(frozen.proven),
                "eligible_young": len(eligible),
                "linked": len(frozen.linked),
                "no_observed_link": len(frozen.unlinked),
                "qualifying_links": len(frozen.links),
                "valid_linked": comparison["linked"]["valid"],
                "valid_no_observed_link": comparison["no_observed_link"]["valid"],
            },
            "code_sha256": _code_digest(),
            "libraries": {"duckdb": duckdb.__version__, "polars": pl.__version__},
        },
    }
    return json.loads(json.dumps(report, default=_json_default, allow_nan=False, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fills", type=Path, help="Normalized local fills Parquet")
    parser.add_argument("--ledger", type=Path, help="Normalized local ledger Parquet")
    parser.add_argument("--metadata", type=Path, required=True, help="Source coverage JSON")
    parser.add_argument("--cutoff", type=_utc, default=PILOT_CUTOFF)
    parser.add_argument("--output", type=Path, required=True, help="New private run JSON under runs/")
    args = parser.parse_args(argv)
    # Keep raw reports inside the checkout's ignored private directory. Resolve
    # symlinks before checking the boundary; never replace an existing inode.
    private_root = Path(__file__).resolve().parents[2] / "runs"
    output = args.output.resolve()
    if output == private_root or not output.is_relative_to(private_root):
        parser.error("--output must be inside the project's runs/ directory")
    inputs = [path.resolve() for path in (args.fills, args.ledger, args.metadata)
              if path is not None]
    if output in inputs or output.exists():
        parser.error("--output must be a new file, not an existing input or artifact")
    metadata = json.loads(args.metadata.read_text(encoding="utf-8"))
    report = run_study(args.fills, args.ledger, metadata, cutoff=args.cutoff)
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if output.parent.resolve() != output.parent:
        parser.error("--output parent changed during the run")
    try:
        fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        parser.error("--output already exists")
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        stream.write(json.dumps(report, sort_keys=True, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
