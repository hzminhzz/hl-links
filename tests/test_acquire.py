from datetime import datetime, timezone

import duckdb
import polars as pl

from hl_links.acquire import SourceUnavailable, Window, acquire_snapshots
from hl_links.study import run_study


def utc(value):
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def vike_file(path, rows):
    schema = {
        "wallet": pl.String, "trade_id": pl.UInt64, "fill_ts": pl.Datetime("ms", "UTC"),
        "asset": pl.String, "dex_name": pl.String, "side": pl.String,
        "size_token": pl.Float64, "price": pl.Float64, "size_usd": pl.Float64,
        "closed_pnl": pl.Float64, "fee": pl.Float64, "fee_token": pl.String,
    }
    pl.DataFrame(rows, schema=schema).write_parquet(path)


def fill(wallet="0x" + "a"*40, trade_id=1, **changes):
    return dict(wallet=wallet, trade_id=trade_id, fill_ts=utc("2026-10-07T00:00:01+00:00"),
                asset="BTC", dex_name="HL", side="buy", size_token=2.0,
                price=100.0, size_usd=200.0, closed_pnl=7.0, fee=-0.25,
                fee_token="USD") | changes


def test_vike_slice_reuses_runner_contract_and_keeps_signed_fees(tmp_path):
    day = tmp_path / "day.parquet"
    vike_file(day, [
        fill(wallet=" 0x" + "A"*40 + " "), fill(),
        fill(trade_id=2, asset="@123"), fill(trade_id=3, asset="#ABC"),
        fill(trade_id=4, asset="A/B"), fill(trade_id=5, asset="xyz:ABC", dex_name="XYZ"),
        fill(trade_id=6, fee_token="HYPE"),
        fill(trade_id=7, fill_ts=utc("2026-10-07T00:00:05+00:00")),
    ])
    window = Window(utc("2026-10-07T00:00:00+00:00"), utc("2026-10-07T00:00:05+00:00"))
    out = tmp_path / "snapshot"
    metadata = acquire_snapshots(
        out, [window], [], connection=duckdb.connect(),
        vike_archive=lambda day_: (str(day), {"date": day_}),
        reservoir_archive=lambda _: (_ for _ in ()).throw(AssertionError("unexpected fallback")),
        ledger_page=lambda **_: {"items": [], "nextCursor": None},
    )
    data = pl.read_parquet(out / "fills.parquet").sort("trade_id")
    assert data.height == 2
    assert data["wallet"].to_list() == ["0x" + "a"*40] * 2
    assert data["fee_amount"].to_list() == [-0.25, -0.25]
    assert data["fee_unit"].to_list() == ["USD", "HYPE"]
    assert data["notional_usd"].to_list() == [200.0, 200.0]
    assert data["known_at"].null_count() == 2
    assert data["ingested_at"].null_count() == 0
    assert data["event_at"].to_list() == [utc("2026-10-07T00:00:01+00:00")] * 2
    assert metadata["fills"]["intervals"] == [[window.start.isoformat(), window.end.isoformat()]]
    assert metadata["fills"]["acquisition"]["slices"][0]["attempts"][0]["duplicate_rows"] == 1
    report = run_study(out / "fills.parquet", out / "ledger.parquet", metadata)
    assert report["status"] == "inconclusive"
    assert report["manifest"]["diagnostics"]["historical_fills"]["unknown_fees"] == 1
    assert report["manifest"]["inputs"]["fills"]["acquisition"]["fee_units"] == {"HYPE": 1, "USD": 1}


def ledger_item(tx, wallet, amount, **changes):
    return dict(hash="0x" + tx * 64, wallet=wallet, amount_usd=amount,
                ts="2026-10-07T00:00:01Z", event_type="send", token="USDC",
                counterparty=None) | changes


def test_global_ledger_exhausts_short_pages_deduplicates_and_verifies_two_sides(tmp_path):
    a, b = "0x" + "a"*40, "0x" + "b"*40
    outgoing = ledger_item("1", a.upper().replace("0X", "0x"), -125)
    incoming = ledger_item("1", b, 125)
    orphan = ledger_item("2", a, -180)
    pages = {
        None: {"items": [outgoing], "nextCursor": "p1"},
        "p1": {"items": [outgoing, incoming, orphan], "nextCursor": "p2"},
        "p2": {"items": [], "nextCursor": None},
    }
    calls = []

    def fetch(**params):
        calls.append(params)
        return pages[params.get("cursor")]

    window = Window(utc("2026-10-07T00:00:00+00:00"), utc("2026-10-07T00:00:05+00:00"))
    out = tmp_path / "ledger"
    metadata = acquire_snapshots(
        out, [], [window], connection=duckdb.connect(),
        vike_archive=lambda _: None, reservoir_archive=lambda _: None,
        ledger_page=fetch, page_limit=10,
    )
    assert [call.get("cursor") for call in calls] == [None, "p1", "p2"]
    assert all({k: v for k, v in call.items() if k != "cursor"} == calls[0] for call in calls)
    assert metadata["ledger"]["intervals"] == [window.pair()]
    receipt = metadata["ledger"]["acquisition"]["slices"][0]
    assert (receipt["pages"], receipt["raw_rows"], receipt["duplicate_rows"], receipt["retained_rows"]) == (3, 4, 1, 3)
    normalized = pl.read_parquet(out / "ledger.parquet")
    assert normalized["known_at"].null_count() == 3
    assert normalized["ingested_at"].null_count() == 0
    assert set(normalized["direction"]) == {"in", "out"}
    report = run_study(out / "fills.parquet", out / "ledger.parquet", metadata)
    counts = report["manifest"]["diagnostics"]["ledger"]
    assert counts["verified"] == 1
    assert counts["one_sided"] == 1
    assert len(report["verified_transfers"]) == 1
    assert report["verified_transfers"][0]["token"] == "usdc"
    assert report["manifest"]["inputs"]["ledger"]["acquisition"]["slices"][0]["complete"] is True


def reservoir_file(path, rows):
    schema = {
        "address": pl.String, "trade_id": pl.UInt64, "timestamp": pl.Datetime("ms", "UTC"),
        "coin": pl.String, "dex": pl.String, "side": pl.String,
        "size": pl.Float64, "price": pl.Float64,
        "realized_pnl": pl.Float64, "fee": pl.Float64, "fee_token": pl.String,
    }
    pl.DataFrame(rows, schema=schema).write_parquet(path)


def reservoir_fill(trade_id=1, **changes):
    return dict(address="0x" + "a"*40, trade_id=trade_id,
                timestamp=utc("2026-10-07T00:00:01+00:00"), coin="BTC",
                dex="hyperliquid", side="buy", size=2.0, price=100.0,
                realized_pnl=7.0, fee=-0.25, fee_token="USD") | changes


def unavailable(_):
    raise SourceUnavailable("missing_archive")


def test_fallback_and_missing_day_coverage(tmp_path):
    reservoir = tmp_path / "reservoir.parquet"
    reservoir_file(reservoir, [reservoir_fill(), reservoir_fill(), reservoir_fill(trade_id=2, coin="@8")])
    start = utc("2026-10-07T00:00:00Z")
    end = utc("2026-10-08T00:00:00Z")
    out = tmp_path / "fallback"
    metadata = acquire_snapshots(
        out, [Window(start, utc("2026-10-09T00:00:00Z"))], [],
        connection=duckdb.connect(), vike_archive=unavailable,
        reservoir_archive=lambda day: (str(reservoir), {"date": day}) if day == "2026-10-07" else unavailable(day),
        ledger_page=lambda **_: {"items": []},
    )
    assert metadata["fills"]["intervals"] == [Window(start, end).pair()]
    receipts = metadata["fills"]["acquisition"]["slices"]
    assert receipts[0]["selected_provider"] == "reservoir"
    assert "selected_provider" not in receipts[1]
    assert receipts[1]["attempts"] == [{"provider": "vike", "error": "missing_archive"}, {"provider": "reservoir", "error": "missing_archive"}]
    data = pl.read_parquet(out / "fills.parquet")
    assert data.height == 1
    assert data["source"].to_list() == ["reservoir"]
    assert data["fee_amount"].to_list() == [-0.25]
    assert data["notional_usd"].to_list() == [200.0]


def test_reconciliation_reports_discrepancies_without_union(tmp_path):
    vike = tmp_path / "vike.parquet"
    reservoir = tmp_path / "reservoir.parquet"
    vike_file(vike, [fill(), fill(trade_id=2), fill(trade_id=3)])
    reservoir_file(reservoir, [reservoir_fill(), reservoir_fill(trade_id=2, fee=-0.5), reservoir_fill(trade_id=4)])
    window = Window(utc("2026-10-07T00:00:00Z"), utc("2026-10-07T00:00:05Z"))
    out = tmp_path / "reconcile"
    metadata = acquire_snapshots(
        out, [window], [], connection=duckdb.connect(),
        vike_archive=lambda day: (str(vike), {"date": day}),
        reservoir_archive=lambda day: (str(reservoir), {"date": day}),
        ledger_page=lambda **_: {"items": []}, reconcile=True,
    )
    check = metadata["fills"]["acquisition"]["slices"][0]["reconciliation"]
    assert {k: check[k] for k in ("matched", "vike_only", "reservoir_only", "economic_discrepancies")} == {
        "matched": 2, "vike_only": 1, "reservoir_only": 1, "economic_discrepancies": 1,
    }
    assert metadata["fills"]["intervals"] == []
    data = pl.read_parquet(out / "fills.parquet")
    assert set(data["trade_id"]) == {"1", "2", "3"}
    assert set(data["source"]) == {"vike"}


def test_cursor_cycle_never_claims_complete_coverage(tmp_path):
    out = tmp_path / "incomplete"
    window = Window(utc("2026-10-07T00:00:00Z"), utc("2026-10-07T00:00:05Z"))
    item = ledger_item("1", "0x" + "a"*40, -100)
    metadata = acquire_snapshots(
        out, [], [window], connection=duckdb.connect(),
        vike_archive=unavailable, reservoir_archive=unavailable,
        ledger_page=lambda **_: {"items": [item], "nextCursor": "repeat"},
    )
    receipt = metadata["ledger"]["acquisition"]["slices"][0]
    assert receipt["error"] == "missing_or_repeated_cursor"
    assert receipt["duplicate_rows"] == 1
    assert metadata["ledger"]["intervals"] == []
    assert pl.read_parquet(out / "ledger.parquet").height == 1


def test_incomplete_ledger_window_cannot_verify_transfers(tmp_path):
    window = Window(utc("2026-10-07T00:00:00Z"), utc("2026-10-07T00:00:05Z"))
    a, b = "0x" + "a"*40, "0x" + "b"*40
    out = tmp_path / "incomplete_pair"
    metadata = acquire_snapshots(
        out, [], [window], connection=duckdb.connect(),
        vike_archive=unavailable, reservoir_archive=unavailable,
        ledger_page=lambda **_: {
            "items": [ledger_item("1", a, -125), ledger_item("1", b, 125)],
            "nextCursor": "unexhausted",
        },
        max_pages=1,
    )
    report = run_study(out / "fills.parquet", out / "ledger.parquet", metadata)
    assert report["manifest"]["diagnostics"]["ledger"]["verified"] == 0
    assert report["manifest"]["diagnostics"]["ledger"]["incomplete_window_rows"] == 2
    assert metadata["ledger"]["intervals"] == []


def test_same_hash_different_token_is_not_a_direct_transfer(tmp_path):
    window = Window(utc("2026-10-07T00:00:00Z"), utc("2026-10-07T00:00:05Z"))
    a, b = "0x" + "a"*40, "0x" + "b"*40
    out = tmp_path / "token_conflict"
    metadata = acquire_snapshots(
        out, [], [window], connection=duckdb.connect(),
        vike_archive=unavailable, reservoir_archive=unavailable,
        ledger_page=lambda **p: {"items": [], "nextCursor": None} if p.get("cursor") else {
            "items": [ledger_item("1", a, -125), ledger_item("1", b, 125, token="HYPE")],
            "nextCursor": "next",
        },
    )
    report = run_study(out / "fills.parquet", out / "ledger.parquet", metadata)
    counts = report["manifest"]["diagnostics"]["ledger"]
    assert counts["verified"] == 0
    assert counts["incompatible"] == 1
