"""Resumable, private execution of the approved single-cutoff pilot."""
import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from .cohorts import make_cohorts
from .fills import FillArchive
from .pilot import ARCHIVE_START, LEDGER_START, FORWARD_END, acquire_day
from .pilot_analysis import (
    _write_json, partition_fills, summarize_partitions, verify_partitioned_ledger,
    combine_summaries, wallet_digest,
)
from .resources import private_runs_dir
from .study import (
    DAY, PILOT_CUTOFF as T, _code_digest, _gaps, _json_default, _sha256, _utc,
    _wallet_records, assemble_report,
)


def _canonical(value):
    return json.loads(json.dumps(value, default=_json_default, sort_keys=True, allow_nan=False))


def _digest(value):
    return hashlib.sha256(json.dumps(_canonical(value), sort_keys=True).encode()).hexdigest()


def _days(start, end):
    while start < end:
        yield start
        start += DAY


def _roots(path):
    return sorted(p for p in path.iterdir() if not p.name.startswith(".") and p.is_dir()) if path.exists() else []


def daily_receipt(root, name, start, end, *, replacements=None):
    """Bind actual snapshots, metadata and coverage to the declared UTC days."""
    records, intervals, missing = [], [], []
    replacements = replacements or {}
    for day in _days(start, end):
        label = day.date().isoformat()
        directory = replacements.get(label, root / label)
        path, meta = directory / f"{name}.parquet", directory / "metadata.json"
        if not path.is_file() or not meta.is_file():
            missing.append(label)
            continue
        declaration = json.loads(meta.read_text())[name]
        declared = [tuple(map(_utc, pair)) for pair in declaration["intervals"]]
        if any(not day <= left < right <= day + DAY for left, right in declared):
            raise ValueError("Daily coverage outside requested UTC day")
        intervals.extend(declared)
        records.append({
            "day": label, "path": str(path), "sha256": _sha256(path),
            "metadata_sha256": _sha256(meta), "declaration": declaration,
        })
    return {
        "present": bool(records), "source": "daily normalized provider snapshots" if records else None,
        "retrieval_id": _digest(records) if records else None, "sha256": _digest(records) if records else None,
        "declared_intervals": sorted(set(intervals)), "days": records, "missing_days": missing,
    }


def _paths(receipt):
    return [Path(item["path"]) for item in receipt["days"]]


def partition_days(receipt, root, *, wallets=None):
    """Resume disjoint completed batches, refusing changed or duplicated input."""
    expected = {Path(item["path"]).resolve(): item["sha256"] for item in receipt["days"]}
    seen = set()
    for directory in _roots(root):
        record = json.loads((directory / "receipt.json").read_text())
        if record["buckets"] != 64 or record.get("wallets_sha256") != wallet_digest(wallets):
            raise ValueError("Unexpected fill partition configuration")
        for item in record["inputs"]:
            path = Path(item["path"]).resolve()
            if path in seen or expected.get(path) != item["sha256"]:
                raise ValueError("Partition checkpoint input mismatch")
            seen.add(path)
    missing = [p for p in _paths(receipt) if p.resolve() not in seen]
    for offset in range(0, len(missing), 4):
        batch = missing[offset:offset + 4]
        output = root / f"{batch[0].parent.name}_{batch[-1].parent.name}"
        partition_fills(batch, output, wallets=wallets)
        print("partition checkpoint", output.name, flush=True)
    return _roots(root)


def freeze_cohorts(history, ledger_paths, ledger_parts):
    records = _wallet_records(history)
    preliminary = make_cohorts(records, [], cutoff=T)
    edges, evidence, counts = verify_partitioned_ledger(
        ledger_paths, ledger_parts, cutoff=T,
        proven=preliminary.proven, young=preliminary.unlinked,
    ) if ledger_paths else _empty_ledger()
    frozen = make_cohorts(records, edges, cutoff=T)
    cohorts = {
        "proven": sorted(frozen.proven), "linked": sorted(frozen.linked),
        "no_observed_link": sorted(frozen.unlinked),
        "links": [evidence[edge.transaction_id] for edge in frozen.links],
    }
    return cohorts, counts


def _empty_ledger():
    from .ledger import verify_transfers
    return verify_transfers(None, T)


def _audit_edges(paths, cohorts):
    """Check sampled qualifying links directly against normalized ledger sides."""
    links = cohorts["links"]
    sample = sorted(links, key=lambda row: row["transaction_id"])[:24]
    if not sample:
        return {"sampled": 0, "matched": 0}
    with duckdb.connect() as con:
        con.execute("SET TimeZone = 'UTC'")
        rows = con.execute("""
            SELECT transaction_id, wallet, counterparty, direction, transfer_type,
                   event_at, amount_usd, token, signed_amount_usd, intermediary,
                   window_complete, known_at
            FROM read_parquet(?) WHERE transaction_id IN (SELECT unnest(?::VARCHAR[]))
        """, [[str(p) for p in paths], [row["transaction_id"] for row in sample]]).fetchall()
    matched = 0
    for edge in sample:
        sides = [row for row in rows if row[0] == edge["transaction_id"]]
        for wallet, other, direction, sign in (
            (edge["sender"], edge["recipient"], "out", -1),
            (edge["recipient"], edge["sender"], "in", 1),
        ):
            if not any(
                row[1] == wallet and row[2] in (None, other)
                and row[3] == direction and row[4] == "send"
                and row[5] == _utc(edge["event_at"])
                and Decimal(row[6]) == Decimal(edge["amount_usd"])
                and row[7].lower() == edge["token"]
                and Decimal(row[8]) == sign * Decimal(edge["amount_usd"])
                and row[9] is False and row[10] is True
                and (row[11] is None or row[11] <= T)
                for row in sides
            ):
                raise ValueError("Representative funding edge reconciliation failed")
        matched += 1
    return {"sampled": len(sample), "matched": matched}


def _audit_fills(paths):
    """Independent arithmetic and interval checks on actual normalized rows."""
    sampled = failures = 0
    with duckdb.connect() as con:
        con.execute("SET TimeZone = 'UTC'")
        for path in paths:
            day = _utc(path.parent.name + "T00:00:00Z")
            rows = con.execute("""
                SELECT event_at, wallet, trade_id, quantity, price, notional_usd,
                       fee_amount, fee_unit, closed_pnl_usd, side
                FROM read_parquet(?)
                WHERE upper(trim(dex_name))='HL' AND NOT unclassified_market
                  AND NOT regexp_matches(symbol, '[@#/:]') AND trim(symbol)<>''
                LIMIT 500
            """, [str(path)]).fetchall()
            sampled += len(rows)
            for at, wallet, tid, q, p, n, fee, unit, pnl, side in rows:
                valid = (at is not None and day <= at < day + DAY and wallet and tid
                         and all(x is not None and math.isfinite(x) for x in (q, p, n, fee, pnl))
                         and q > 0 and p > 0 and n > 0
                         and math.isclose(n, q * p, rel_tol=1e-9, abs_tol=1e-8)
                         and unit in ("USD", "USDC") and side.lower() in ("buy", "sell"))
                failures += not bool(valid)
    return {"sampled": sampled, "failed": failures, "days": [p.parent.name for p in paths]}


def _checkpoint(directory, value):
    directory.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with TemporaryDirectory(prefix=".checkpoint-", dir=directory.parent) as stage:
        target = Path(stage) / "complete"
        target.mkdir(mode=0o700)
        _write_json(target / "manifest.json", value)
        _write_json(target / "receipt.json", {"sha256": _sha256(target / "manifest.json")})
        target.rename(directory)


def _load_checkpoint(directory):
    path = directory / "manifest.json"
    receipt = json.loads((directory / "receipt.json").read_text())
    if _sha256(path) != receipt["sha256"]:
        raise ValueError("Checkpoint checksum mismatch")
    return json.loads(path.read_text())


def freeze(root, analysis, retry):
    directory = analysis / "frozen"
    if directory.exists():
        frozen = _load_checkpoint(directory)
        if frozen["code_sha256"] != _code_digest():
            raise ValueError("Frozen pilot code changed; do not silently refreeze")
        return frozen
    history_receipt = daily_receipt(root, "fills", ARCHIVE_START, T)
    ledger_receipt = daily_receipt(root, "ledger", LEDGER_START, T,
                                   replacements={"2026-09-08": retry})
    history_receipt["archive_start"] = ARCHIVE_START
    history_receipt["historical_gaps"] = _gaps(history_receipt, ARCHIVE_START, T)
    ledger_receipt["historical_gaps"] = _gaps(ledger_receipt, LEDGER_START, T)
    history_roots = partition_days(history_receipt, analysis / "history-parts")
    print("historical partitions validated", len(history_roots), flush=True)
    history, counts = summarize_partitions(
        history_roots, analysis / "history-summary", start=None, end=T, score_start=LEDGER_START)
    cohorts, ledger_counts = freeze_cohorts(history, _paths(ledger_receipt), analysis / "ledger-parts")
    with FillArchive(_paths(history_receipt)) as archive:
        input_counts = archive.input_diagnostics(forward_start=T, forward_end=FORWARD_END)
    sample = [p for p in _paths(history_receipt) if p.parent.name in
              {"2026-05-01", "2026-08-24", "2026-10-22"}]
    frozen = {
        "cutoff": T, "frozen_at": datetime.now(timezone.utc), "code_sha256": _code_digest(),
        "history_receipt": history_receipt, "ledger_receipt": ledger_receipt,
        "ledger_replacement": {"day": "2026-09-08",
                               "original_metadata_sha256": _sha256(root / "2026-09-08" / "metadata.json"),
                               "original_ledger_sha256": _sha256(root / "2026-09-08" / "ledger.parquet")},
        "history_partitions": [{"path": str(p), "receipt_sha256": _sha256(p / "receipt.json")}
                               for p in history_roots],
        "history_counts": counts, "ledger_counts": ledger_counts, "fill_input_counts": input_counts,
        "historical_wallets": len(history), "historical_wallets_with_valid_trade_evidence": len(_wallet_records(history)),
        "cohorts": cohorts, "edge_audit": _audit_edges(_paths(ledger_receipt), cohorts),
        "historical_fill_audit": _audit_fills(sample),
    }
    _checkpoint(directory, frozen)
    print("COHORTS FROZEN", {key: len(value) for key, value in cohorts.items()}, flush=True)
    return _load_checkpoint(directory)


def _report_from_summary(frozen, forward_receipt, forward, counts, input_counts):
    history_receipt = frozen["history_receipt"]
    fills = {
        "present": history_receipt["present"] or forward_receipt["present"],
        "source": "Vike primary, Reservoir fallback",
        "retrieval_id": _digest([history_receipt["retrieval_id"], forward_receipt["retrieval_id"]]),
        "sha256": _digest([history_receipt["sha256"], forward_receipt["sha256"]]),
        "declared_intervals": history_receipt["declared_intervals"] + forward_receipt["declared_intervals"],
        "archive_start": history_receipt["archive_start"],
        "historical_gaps": history_receipt["historical_gaps"],
        "forward_gaps": forward_receipt["forward_gaps"],
        "history_days": history_receipt["days"], "forward_days": forward_receipt["days"],
    }
    cohorts = frozen["cohorts"]
    return assemble_report(
        cohorts=cohorts, evidence={edge["transaction_id"]: edge for edge in cohorts["links"]},
        forward=forward, fills_receipt=fills, ledger_receipt=frozen["ledger_receipt"],
        history_counts=frozen["history_counts"], forward_counts=counts,
        fill_input_counts=input_counts, ledger_counts=frozen["ledger_counts"], cutoff=T,
    )


def evaluate(root, analysis, frozen):
    directory = analysis / "final"
    # A durable, checksum-verified historical freeze is mandatory before any
    # forward provider request. Never select cohorts from forward records.
    if frozen != _load_checkpoint(analysis / "frozen") or frozen["code_sha256"] != _code_digest():
        raise ValueError("Frozen historical checkpoint changed")
    if directory.exists():
        report = _load_checkpoint(directory)
        if report["manifest"]["frozen_checkpoint_sha256"] != _sha256(analysis / "frozen" / "manifest.json"):
            raise ValueError("Final report does not bind frozen cohort")
        return report
    for day in _days(T, FORWARD_END):
        for attempt in range(2):
            try:
                meta = acquire_day(root, day)
                item = meta["fills"]["acquisition"]["slices"][0]
                print("forward receipt", day.date(), item.get("selected_provider"),
                      item["coverage_complete"], flush=True)
                break
            except Exception as exc:
                print("forward acquisition failed", day.date(), type(exc).__name__, flush=True)
    receipt = daily_receipt(root, "fills", T, FORWARD_END)
    receipt["forward_gaps"] = _gaps(receipt, T, FORWARD_END)
    receipt = _canonical(receipt)
    history_roots = []
    for item in frozen["history_partitions"]:
        path = Path(item["path"])
        if _sha256(path / "receipt.json") != item["receipt_sha256"]:
            raise ValueError("Historical partition receipt changed after freeze")
        history_roots.append(path)
    eligible = sorted(frozen["cohorts"]["linked"] + frozen["cohorts"]["no_observed_link"])
    forward_roots = partition_days(receipt, analysis / "forward-parts", wallets=eligible)
    forward, counts = summarize_partitions(history_roots + forward_roots, analysis / "forward-summary",
                                            start=T, end=FORWARD_END, score_start=T, wallets=eligible)
    input_counts = dict(frozen["fill_input_counts"])
    if _paths(receipt):
        with FillArchive(_paths(receipt)) as archive:
            for key, value in archive.input_diagnostics(forward_start=T, forward_end=FORWARD_END).items():
                input_counts[key] += value
    report = _report_from_summary(frozen, receipt, forward, counts, input_counts)
    report["manifest"]["frozen_checkpoint_sha256"] = _sha256(analysis / "frozen" / "manifest.json")
    report["manifest"]["frozen_at"] = frozen["frozen_at"]
    report["manifest"]["verified_transfer_retention"] = "qualifying edges only; diagnostics cover all ledger transactions"
    report["manifest"]["historical_wallets"] = frozen["historical_wallets"]
    report["manifest"]["historical_wallets_with_valid_trade_evidence"] = frozen["historical_wallets_with_valid_trade_evidence"]
    report["manifest"]["ledger_replacement"] = frozen["ledger_replacement"]
    report["manifest"]["edge_audit"] = frozen["edge_audit"]
    report["manifest"]["historical_fill_audit"] = frozen["historical_fill_audit"]
    sample = [p for p in _paths(receipt) if p.parent.name in {"2026-10-23", "2026-11-07", "2026-11-21"}]
    report["manifest"]["forward_fill_audit"] = _audit_fills(sample)
    if report["manifest"]["historical_fill_audit"]["failed"] or report["manifest"]["forward_fill_audit"]["failed"]:
        report["limitations"].append("Representative normalized fill audit found invalid economics.")
        if report["status"] == "comparison_available":
            report["status"] = "partial"
    _checkpoint(directory, report)
    return _load_checkpoint(directory)


def verify_replay(root, analysis, frozen, report):
    """Reconstruct membership and outcomes from verified private checkpoints."""
    directory = analysis / "verified"
    report_hash = _sha256(analysis / "final" / "manifest.json")
    if directory.exists():
        verification = _load_checkpoint(directory)
        if verification["report_sha256"] != report_hash:
            raise ValueError("Verification does not bind final report")
        return verification
    history, history_counts = combine_summaries(_roots(analysis / "history-summary"))
    if _canonical(history_counts) != frozen["history_counts"]:
        raise ValueError("Historical diagnostics replay mismatch")
    cohorts, ledger_counts = freeze_cohorts(
        history, _paths(frozen["ledger_receipt"]), analysis / "ledger-parts")
    if _canonical(cohorts) != frozen["cohorts"] or ledger_counts != frozen["ledger_counts"]:
        raise ValueError("Cohort or ledger replay mismatch")
    forward, counts = combine_summaries(_roots(analysis / "forward-summary"))
    receipt = daily_receipt(root, "fills", T, FORWARD_END)
    receipt["forward_gaps"] = _gaps(receipt, T, FORWARD_END)
    replay = _report_from_summary(
        frozen, _canonical(receipt), forward, counts, report["manifest"]["diagnostics"]["fill_input"])
    for key in ("comparison", "cohorts", "verified_transfers"):
        if replay[key] != report[key]:
            raise ValueError("Outcome replay mismatch")
    for key in ("counts", "diagnostics", "inputs"):
        if replay["manifest"][key] != report["manifest"][key]:
            raise ValueError("Manifest replay mismatch")
    verification = {
        "report_sha256": report_hash,
        "frozen_sha256": _sha256(analysis / "frozen" / "manifest.json"),
        "verified_at": datetime.now(timezone.utc),
        "method": "checksum-verified shard summaries, repeated ledger verification and cohort and outcome reconstruction",
        "historical_summary_shards": len(_roots(analysis / "history-summary")),
        "forward_summary_shards": len(_roots(analysis / "forward-summary")),
        "cohorts_equal": True, "outcomes_equal": True, "diagnostics_equal": True,
    }
    _checkpoint(directory, verification)
    return _load_checkpoint(directory)


def sanitized(report):
    manifest = report["manifest"]
    result = {
        "status": report["status"], "counts": manifest["counts"],
        "comparison": {key: ({k: v for k, v in value.items() if k != "wallet_scores"}
                              if isinstance(value, dict) else value)
                       for key, value in report["comparison"].items()},
        "limitations": report["limitations"],
        "diagnostics": manifest["diagnostics"],
    }
    for key in ("cutoff", "windows", "parameters", "code_sha256", "frozen_at",
                "frozen_checkpoint_sha256", "historical_wallets", "historical_wallets_with_valid_trade_evidence",
                "edge_audit", "historical_fill_audit", "forward_fill_audit"):
        result[key] = manifest[key]
    acquisition = {}
    for label, requested, days in (("history", 175, manifest["inputs"]["fills"]["history_days"]),
                                   ("forward", 30, manifest["inputs"]["fills"]["forward_days"])):
        slices = [item["declaration"]["acquisition"]["slices"][0] for item in days]
        acquisition[label] = {
            "requested_days": requested, "receipts": len(days),
            "complete_days": sum(bool(item["coverage_complete"]) for item in slices),
            "selected_providers": dict(Counter(str(item.get("selected_provider")) for item in slices)),
            "reservoir_attempted_days": sum(
                any(attempt.get("provider") == "reservoir" for attempt in item.get("attempts", []))
                for item in slices),
        }
    ledger_slices = [item["declaration"]["acquisition"]["slices"][0]
                     for item in manifest["inputs"]["ledger"]["days"]]
    acquisition["ledger"] = {
        "requested_days": 60, "receipts": len(ledger_slices),
        "complete_days": sum(bool(item.get("complete", item.get("coverage_complete"))) for item in ledger_slices),
        "pagination_exhausted_days": sum(bool(item.get("pagination_exhausted")) for item in ledger_slices),
        **{key: sum(item.get(key, 0) for item in ledger_slices)
           for key in ("pages", "raw_rows", "retained_rows", "duplicate_rows")},
    }
    result["acquisition"] = acquisition
    result["coverage"] = {key: {k: v for k, v in value.items() if k.endswith("_gaps")}
                          for key, value in manifest["inputs"].items()}
    result["ledger_replacement_day"] = manifest["ledger_replacement"]["day"]
    for group in ("linked", "no_observed_link"):
        stats = result["comparison"][group]
        if stats["valid"] >= 2:
            half = 1.96 * stats["sample_sd"] / math.sqrt(stats["valid"])
            stats["descriptive_95pct_mean_interval"] = [stats["mean"] - half, stats["mean"] + half]
        else:
            stats["descriptive_95pct_mean_interval"] = None
    linked, control = (result["comparison"][group] for group in ("linked", "no_observed_link"))
    result["comparison"]["median_difference"] = (
        linked["median"] - control["median"]
        if linked["median"] is not None and control["median"] is not None else None)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("runs/t03-pilot"))
    parser.add_argument("--analysis", type=Path, default=Path("runs/t03-analysis"))
    parser.add_argument("--ledger-retry", type=Path, default=Path("runs/t03-ledger-retry-0908"))
    parser.add_argument("--freeze-only", action="store_true")
    args = parser.parse_args(argv)
    for path in (args.root, args.analysis, args.ledger_retry):
        if not path.resolve().is_relative_to(private_runs_dir()) or path.resolve() == private_runs_dir():
            parser.error("All paths must be inside the private runs directory")
    frozen = freeze(args.root, args.analysis, args.ledger_retry)
    if args.freeze_only:
        return 0
    report = evaluate(args.root, args.analysis, frozen)
    verification = verify_replay(args.root, args.analysis, frozen, report)
    result = sanitized(report)
    result["verification"] = verification
    public = args.analysis / "sanitized"
    if public.exists():
        if _load_checkpoint(public) != _canonical(result):
            raise ValueError("Sanitized report replay mismatch")
    else:
        _checkpoint(public, result)
    print(json.dumps(result, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
