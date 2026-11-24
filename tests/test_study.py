"""Behavioral tests at the local Parquet study boundary; all addresses are synthetic."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import hashlib
import json
import subprocess
import sys
from tempfile import TemporaryDirectory

import polars as pl
import pytest

from hl_links.study import run_study

T = datetime(2026, 10, 23, tzinfo=timezone.utc)
DAY = timedelta(days=1)
STAMP = pl.Datetime("us", "UTC")
FILL_SCHEMA = {
    "wallet": pl.String, "trade_id": pl.String, "event_at": STAMP,
    "known_at": STAMP, "ingested_at": STAMP, "dex_name": pl.String,
    "symbol": pl.String, "closed_pnl_usd": pl.Float64,
    "fee_amount": pl.Float64, "fee_unit": pl.String, "notional_usd": pl.Float64,
}
LEDGER_SCHEMA = {
    "transaction_id": pl.String, "wallet": pl.String, "counterparty": pl.String,
    "direction": pl.String, "transfer_type": pl.String, "intermediary": pl.Boolean,
    "event_at": STAMP, "known_at": STAMP, "ingested_at": STAMP, "amount_usd": pl.Float64,
    "source": pl.String,
}


def fill(wallet, trade_id, day, pnl=0, fee=0, notional=100, **changes):
    at = T + day * DAY
    return {
        "wallet": wallet, "trade_id": str(trade_id), "event_at": at, "known_at": at,
        "ingested_at": T + 31 * DAY, "dex_name": "HL", "symbol": "BTC",
        "closed_pnl_usd": pnl, "fee_amount": fee, "fee_unit": "USD",
        "notional_usd": notional, **changes,
    }


def transfer(tx, sender, recipient, day=-1, amount=100, **changes):
    at = T + day * DAY
    common = {
        "transaction_id": tx, "event_at": at, "known_at": at,
        "ingested_at": T + 31 * DAY, "amount_usd": amount,
        "transfer_type": "send", "intermediary": False, "source": "fixture",
        **changes,
    }
    return [
        dict(common, wallet=sender, counterparty=recipient, direction="out"),
        dict(common, wallet=recipient, counterparty=sender, direction="in"),
    ]


def metadata():
    return {
        "fills": {"source": "fixture", "retrieval_id": "fills-v1",
                  "intervals": [[(T - 90 * DAY).isoformat(), (T + 30 * DAY).isoformat()]]},
        "ledger": {"source": "fixture", "retrieval_id": "ledger-v1",
                   "intervals": [[(T - 60 * DAY).isoformat(), T.isoformat()]]},
    }


def base_fills():
    return [
        fill("PRO", 1, -70), fill("pro", 2, -60, pnl=20001, fee=1),
        fill("y", 3, -20), fill("y", 4, -7), fill("c", 5, -7),
        fill("y", 6, 0, pnl=12, fee=2), fill("c", 7, 29, pnl=-4, fee=1),
    ]


def snapshot(path: Path, fills, ledger, *, provenance=None):
    path.mkdir(parents=True, exist_ok=True)
    fill_path, ledger_path = path / "fills.parquet", path / "ledger.parquet"
    pl.DataFrame(fills, schema=FILL_SCHEMA).write_parquet(fill_path)
    if ledger is not None:
        pl.DataFrame(ledger, schema=LEDGER_SCHEMA).write_parquet(ledger_path)
    return run_study(fill_path, ledger_path if ledger is not None else None,
                     metadata() if provenance is None else provenance, cutoff=T)


def test_local_runner_compares_cutoff_frozen_wallets(tmp_path):
    report = snapshot(tmp_path, base_fills(), transfer("tx", "pro", "y"))
    assert report["cohorts"]["proven"] == ["pro"]
    assert report["cohorts"]["linked"] == ["y"]
    assert report["cohorts"]["no_observed_link"] == ["c"]
    assert report["comparison"]["linked"]["mean"] == 10
    assert report["comparison"]["no_observed_link"]["mean"] == -5
    assert report["comparison"]["mean_difference"] == 15
    assert report["status"] == "comparison_available"
    assert report["manifest"]["windows"]["historical"] == [
        "2026-08-24T00:00:00Z", "2026-10-23T00:00:00Z"]
    assert report["manifest"]["windows"]["forward"] == [
        "2026-10-23T00:00:00Z", "2026-11-22T00:00:00Z"]


def test_forward_outcome_uses_event_time_even_when_known_later(tmp_path):
    original = snapshot(tmp_path / "original", base_fills(), transfer("tx", "pro", "y"))
    rows = base_fills() + [
        fill("y", 90, 1, pnl=-100, known_at=T+31*DAY),
        fill("y", 91, 30, pnl=1_000_000, known_at=T+31*DAY),
        fill("late_winner", 92, -70, known_at=T+DAY),
        fill("late_winner", 93, -1, pnl=20_000, known_at=T+DAY),
    ]
    changed = snapshot(tmp_path / "changed", rows, transfer("tx", "pro", "y"))
    assert changed["cohorts"] == original["cohorts"]
    assert changed["comparison"]["linked"]["mean"] == -45
    assert changed["comparison"]["mean_difference"] == -40
    assert changed["manifest"]["diagnostics"]["forward_fills"]["late_known_rows"] == 1
    assert changed["manifest"]["diagnostics"]["historical_fills"]["late_known_rows"] == 2
    assert changed["status"] == "comparison_available"
    assert any("included" in text and "later-known" in text for text in changed["limitations"])


def test_forward_reused_historical_fill_identity_cannot_count_twice(tmp_path):
    original = snapshot(tmp_path / "original", base_fills(), transfer("tx", "pro", "y"))
    rows = base_fills() + [fill("y", 3, 1, pnl=10000)]
    changed = snapshot(tmp_path / "changed", rows, transfer("tx", "pro", "y"))
    assert changed["cohorts"] == original["cohorts"]
    assert changed["comparison"]["linked"]["valid"] == 0
    assert changed["comparison"]["linked"]["invalid"] == 1
    assert changed["manifest"]["diagnostics"]["forward_fills"]["cross_window_conflicting_keys"] == 1
    assert changed["status"] == "inconclusive"


def test_market_history_and_forward_boundaries(tmp_path):
    rows = base_fills() + [
        fill("p2", 10, -60, pnl=20_000, fee=0.01),
        fill("p2", 11, -61, pnl=1_000_000),
        fill("p2", 12, 0, pnl=1_000_000),
        fill("old", 13, -60), fill("old", 14, -1),
        fill("inactive", 15, -8), fill("inactive", 16, 0),
        fill("too_new", 17, -6), fill("too_new", 18, 0),
        fill("future", 19, 0),
        fill("c", 20, 30, pnl=1_000_000),
        fill("c", 21, -80, symbol="@1"),
        fill("c", 22, -80, symbol="#1"),
        fill("c", 23, -80, symbol="PURR/USDC"),
        fill("c", 24, -80, symbol="xyz:BTC"),
        fill("c", 25, -80, dex_name="HIP-3"),
        fill("c", 26, 1, pnl=1_000_000, symbol="@1"),
        fill("late", 27, -9, known_at=T+DAY),
        fill("late", 28, -1, known_at=T+DAY),
    ]
    report = snapshot(tmp_path, rows, transfer("tx", "pro", "y"))
    assert report["cohorts"]["proven"] == ["pro"]
    assert report["cohorts"]["linked"] == ["y"]
    assert report["cohorts"]["no_observed_link"] == ["c"]
    assert report["comparison"]["no_observed_link"]["mean"] == -5
    assert report["manifest"]["diagnostics"]["historical_fills"]["excluded_market_rows"] == 5
    assert report["manifest"]["diagnostics"]["historical_fills"]["late_known_rows"] == 2


@pytest.mark.parametrize("bad_notional", [-100, 0, None])
def test_invalid_historical_notional_cannot_establish_wallet_eligibility(tmp_path, bad_notional):
    rows = base_fills() + [
        fill("fake", 90, -7, notional=bad_notional),
        fill("fake", 91, 1, pnl=50),
        # Even an older invalid fill makes the observed-age evidence uncertain.
        fill("old_bad", 92, -70, notional=bad_notional),
        fill("old_bad", 93, -7, pnl=20_000),
        fill("recent_bad", 94, -20),
        fill("recent_bad", 95, -1, notional=bad_notional),
    ]
    report = snapshot(tmp_path, rows, transfer("tx", "pro", "y"))
    assert report["cohorts"]["proven"] == ["pro"]
    assert report["cohorts"]["no_observed_link"] == ["c"]
    assert report["comparison"]["no_observed_link"]["mean"] == -5
    assert report["manifest"]["diagnostics"]["historical_fills"]["invalid_notional"] == 3
    assert report["status"] == "partial"


def test_transfer_verification_rejects_nonqualifying_evidence(tmp_path):
    candidates = ["reverse", "one", "amb", "self", "internal", "middle",
                  "mismatch", "small", "old", "end", "late", "missing_peer"]
    rows = base_fills() + [fill(w, i+100, -7) for i, w in enumerate(candidates)]
    good = transfer("good", "reverse", "pro", day=-60)
    no_peer = transfer("no_peer", "pro", "missing_peer")
    for side in no_peer:
        side["counterparty"] = None
    mismatch = transfer("mismatch", "pro", "mismatch")
    mismatch[1]["amount_usd"] = 101
    ledger = (
        transfer("tx", "pro", "y") + good + good + no_peer
        + transfer("one", "pro", "one")[:1]
        + transfer("amb", "pro", "amb") + transfer("amb", "pro", "c")
        + transfer("self", "self", "self")
        + transfer("internal", "pro", "internal", transfer_type="subaccount")
        + transfer("middle", "pro", "middle", intermediary=True)
        + mismatch + transfer("small", "pro", "small", amount=99.99)
        + transfer("old", "pro", "old", day=-61)
        + transfer("end", "pro", "end", day=0)
        + transfer("late", "pro", "late", known_at=T+DAY)
    )
    report = snapshot(tmp_path, rows, ledger)
    assert report["cohorts"]["linked"] == ["missing_peer", "reverse", "y"]
    assert report["cohorts"]["no_observed_link"] == sorted({"c", *candidates} - {"missing_peer", "reverse"})
    links = report["cohorts"]["links"]
    assert len(links) == 3
    reverse = next(link for link in links if link["transaction_id"] == "good")
    assert (reverse["sender"], reverse["recipient"], reverse["amount_usd"]) == ("reverse", "pro", "100.0")
    assert reverse["sender_evidence"][0]["source"] == "fixture"
    counts = report["manifest"]["diagnostics"]["ledger"]
    assert counts["duplicate_rows"] == 2
    assert counts["one_sided"] == counts["ambiguous"] == counts["self"] == 1
    assert counts["non_direct"] == counts["intermediary"] == counts["incompatible"] == 1
    assert counts["below_minimum"] == 1
    assert counts["late_known_rows"] == 2


def test_equal_wallet_weighting_rebates_unknown_fees_and_zero_volume(tmp_path):
    rows = base_fills() + [
        fill("y2", 10, -7), fill("y2", 11, 1, pnl=0, fee=-2, notional=10),
        fill("zero", 12, -7), fill("zero", 13, 0, notional=0),
        fill("missing", 14, -7),
        fill("unknown", 15, -7), fill("unknown", 16, 1, pnl=100, fee=1, fee_unit="HYPE"),
        fill("c2", 17, -7), fill("c2", 18, 1, pnl=0, fee=0),
        fill("y", 6, 0, pnl=12, fee=2),
    ]
    ledger = transfer("tx", "pro", "y") + transfer("tx2", "pro", "y2")
    report = snapshot(tmp_path, rows, ledger)
    linked = report["comparison"]["linked"]
    control = report["comparison"]["no_observed_link"]
    assert linked["valid"] == 2
    assert linked["mean"] == linked["median"] == 15
    assert linked["wallet_scores"]["y2"]["score"] == 20
    assert control["eligible"] == 5
    assert control["valid"] == 2
    assert control["zero_volume"] == 2
    assert control["invalid"] == control["unknown_fee_wallets"] == 1
    assert control["mean"] == control["median"] == -2.5
    assert report["comparison"]["mean_difference"] == 17.5
    assert report["comparison"]["descriptive_95pct_interval"] is not None
    assert report["manifest"]["diagnostics"]["forward_fills"]["duplicate_rows"] == 1
    assert report["status"] == "partial"


def test_missing_and_partial_snapshots_are_explicit(tmp_path):
    absent = snapshot(tmp_path / "absent", base_fills(), None)
    assert absent["status"] == "inconclusive"
    assert absent["comparison"]["mean_difference"] is None
    assert absent["cohorts"]["no_observed_link"] == ["c", "y"]
    assert not absent["manifest"]["inputs"]["ledger"]["present"]
    empty = snapshot(tmp_path / "empty", [], [])
    assert empty["status"] == "inconclusive"
    assert empty["comparison"]["mean_difference"] is None
    partial_meta = metadata()
    partial_meta["fills"]["intervals"] = [
        [(T-90*DAY).isoformat(), T.isoformat()],
        [(T+DAY).isoformat(), (T+30*DAY).isoformat()],
    ]
    partial = snapshot(tmp_path / "partial", base_fills(), transfer("tx", "pro", "y"),
                       provenance=partial_meta)
    assert partial["status"] == "partial"
    assert partial["manifest"]["inputs"]["fills"]["forward_gaps"] == [
        ["2026-10-23T00:00:00Z", "2026-10-24T00:00:00Z"]]


def test_missing_forward_coverage_is_not_inactivity(tmp_path):
    rows = [row for row in base_fills() if row["trade_id"] != "6"] + [
        fill("zero", 90, -7), fill("zero", 91, 2, notional=0),
    ]
    incomplete = metadata()
    incomplete["fills"]["intervals"] = [
        [(T-90*DAY).isoformat(), T.isoformat()],
        [(T+DAY).isoformat(), (T+30*DAY).isoformat()],
    ]
    partial = snapshot(tmp_path / "partial", rows, transfer("tx", "pro", "y"),
                       provenance=incomplete)
    complete = snapshot(tmp_path / "complete", rows, transfer("tx", "pro", "y"))
    for group, wallet in (("linked", "y"), ("no_observed_link", "zero")):
        uncertain = partial["comparison"][group]
        assert uncertain["wallet_scores"][wallet]["status"] == "missing_data"
        assert uncertain["missing_data"] == 1
        assert uncertain["zero_volume"] == 0
        known = complete["comparison"][group]
        assert known["wallet_scores"][wallet]["status"] == "zero_volume"
        assert known["zero_volume"] == 1
        assert known["missing_data"] == 0
    assert partial["status"] == "inconclusive"
    assert partial["comparison"]["mean_difference"] is None
    assert partial["comparison"]["no_observed_link"]["valid"] == 1


@pytest.mark.parametrize("bad_source", ["fills", "ledger"])
def test_undated_records_are_disclosed_and_cannot_enter_a_window(tmp_path, bad_source):
    rows = base_fills() + [fill("idle", 90, -7)]
    ledger = transfer("tx", "pro", "y")
    if bad_source == "fills":
        rows.append(fill("y", 91, 1, pnl=-100, event_at=None))
    else:
        ledger += transfer("undated", "pro", "c", event_at=None)
    report = snapshot(tmp_path, rows, ledger)
    assert report["cohorts"]["linked"] == ["y"]
    assert report["cohorts"]["no_observed_link"] == ["c", "idle"]
    assert report["comparison"]["linked"]["mean"] == 10
    diagnostics = report["manifest"]["diagnostics"]
    assert diagnostics["fill_input"]["invalid_event_time_rows"] == (1 if bad_source == "fills" else 0)
    assert diagnostics["ledger"]["invalid_event_time_rows"] == (2 if bad_source == "ledger" else 0)
    assert report["comparison"]["no_observed_link"]["wallet_scores"]["idle"]["status"] == (
        "missing_data" if bad_source == "fills" else "zero_volume"
    )
    assert report["status"] == "partial"
    assert any("event timestamps" in text for text in report["limitations"])


@pytest.fixture
def private_run_dir():
    root = Path(__file__).resolve().parents[1] / "runs"
    root.mkdir(exist_ok=True)
    with TemporaryDirectory(prefix="t01-cli-", dir=root) as path:
        yield Path(path)


@pytest.mark.parametrize("kind", ["hardlink", "existing", "outside", "symlink_parent"])
def test_cli_refuses_unsafe_output(tmp_path, private_run_dir, kind):
    snapshot(private_run_dir, base_fills(), transfer("tx", "pro", "y"))
    fills_path = private_run_dir / "fills.parquet"
    ledger_path = private_run_dir / "ledger.parquet"
    meta_path = private_run_dir / "metadata.json"
    meta_path.write_text(json.dumps(metadata()))
    output = private_run_dir / "report.json"
    if kind == "hardlink":
        output.hardlink_to(fills_path)
    elif kind == "existing":
        output.write_text("existing private artifact")
    elif kind == "outside":
        output = tmp_path / "report.json"
    else:
        escape = private_run_dir / "escape"
        escape.symlink_to(tmp_path, target_is_directory=True)
        output = escape / "report.json"
    before = output.read_bytes() if output.exists() else None
    source_bytes = fills_path.read_bytes()
    result = subprocess.run([sys.executable, "-m", "hl_links.study", "--fills", str(fills_path),
                             "--ledger", str(ledger_path), "--metadata", str(meta_path),
                             "--output", str(output)], capture_output=True)
    assert result.returncode != 0
    assert fills_path.read_bytes() == source_bytes
    assert (output.read_bytes() if output.exists() else None) == before


def test_runner_repeatable_and_cli_matches_api(tmp_path, private_run_dir):
    rows = base_fills() + [fill("y", 40, 2, pnl=1, known_at=None)]
    ledger = transfer("tx", "pro", "y", known_at=None)
    first = snapshot(tmp_path, rows, ledger)
    fills_path = tmp_path / "fills.parquet"
    ledger_path = tmp_path / "ledger.parquet"
    assert run_study(fills_path, ledger_path, metadata()) == first
    permuted = snapshot(tmp_path / "permuted", list(reversed(rows)), list(reversed(ledger)))
    assert permuted["cohorts"] == first["cohorts"]
    assert permuted["comparison"] == first["comparison"]
    assert first["manifest"]["inputs"]["fills"]["sha256"] == hashlib.sha256(fills_path.read_bytes()).hexdigest()
    assert first["manifest"]["diagnostics"]["forward_fills"]["unknown_availability"] == 1
    assert first["manifest"]["diagnostics"]["ledger"]["unknown_availability_rows"] == 2
    meta_path = tmp_path / "metadata.json"
    meta_path.write_text(json.dumps(metadata()))
    output = private_run_dir / "report.json"
    subprocess.run([sys.executable, "-m", "hl_links.study", "--fills", str(fills_path),
                    "--ledger", str(ledger_path), "--metadata", str(meta_path),
                    "--output", str(output)], check=True)
    assert json.loads(output.read_text()) == first
    assert output.stat().st_mode & 0o777 == 0o600
    before = fills_path.read_bytes()
    refused = subprocess.run([sys.executable, "-m", "hl_links.study",
                              "--fills", str(fills_path), "--metadata", str(meta_path),
                              "--output", str(fills_path)], capture_output=True)
    assert refused.returncode != 0
    assert fills_path.read_bytes() == before


def test_forward_mutation_preserves_cohorts_and_conflicting_fills_are_invalid(tmp_path):
    ledger = transfer("tx", "pro", "y")
    initial = snapshot(tmp_path / "first", base_fills(), ledger)
    rows = base_fills() + [
        fill("new_future_wallet", 50, 1, pnl=1_000_000),
        fill("y", 6, 0, pnl=10_000),
        fill("c", 51, 1, pnl=123),
    ]
    changed = snapshot(tmp_path / "changed", rows, ledger + transfer("future", "pro", "c", day=1))
    assert changed["cohorts"] == initial["cohorts"]
    assert changed["comparison"]["linked"]["invalid"] == 1
    assert changed["manifest"]["diagnostics"]["forward_fills"]["conflicting_keys"] == 1
    assert changed["status"] == "inconclusive"
