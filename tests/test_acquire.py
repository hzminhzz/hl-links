from datetime import datetime, timezone

import duckdb
import polars as pl
import pytest

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


@pytest.mark.parametrize("primary_has_rows", [False, True])
def test_missing_primary_slice_recovers_clean_reservoir_superset(tmp_path, primary_has_rows):
    vike, reservoir = tmp_path / "vike.parquet", tmp_path / "reservoir.parquet"
    vike_file(vike, [fill()] if primary_has_rows else [])
    reservoir_file(reservoir, [reservoir_fill(), reservoir_fill(trade_id=2)])
    window = Window(utc("2026-10-07T00:00:00Z"), utc("2026-10-07T00:00:05Z"))
    out = tmp_path / "recover"
    with duckdb.connect() as connection:
        metadata = acquire_snapshots(
            out, [window], [], connection=connection,
            vike_archive=lambda day: (str(vike), {"date": day}),
            reservoir_archive=lambda day: (str(reservoir), {"date": day}),
            ledger_page=lambda **_: {"items": []}, reconcile=primary_has_rows,
        )
    receipt = metadata["fills"]["acquisition"]["slices"][0]
    assert receipt["selected_provider"] == "reservoir"
    assert receipt["reconciliation"]["reservoir_only"] == (1 if primary_has_rows else 2)
    assert metadata["fills"]["intervals"] == [window.pair()]
    rows = pl.read_parquet(out / "fills.parquet")
    assert sorted(rows["trade_id"]) == ["1", "2"]
    assert set(rows["source"]) == {"reservoir"}


def test_empty_primary_without_confirmed_fallback_is_a_coverage_gap(tmp_path):
    vike = tmp_path / "vike.parquet"
    vike_file(vike, [])
    window = Window(utc("2026-10-07T00:00:00Z"), utc("2026-10-07T00:00:05Z"))
    out = tmp_path / "gap"
    with duckdb.connect() as connection:
        metadata = acquire_snapshots(
            out, [window], [], connection=connection,
            vike_archive=lambda day: (str(vike), {"date": day}),
            reservoir_archive=unavailable, ledger_page=lambda **_: {"items": []},
        )
    assert metadata["fills"]["intervals"] == []
    assert metadata["fills"]["acquisition"]["slices"][0]["reconciliation"]["error"] == "missing_archive"
    assert pl.read_parquet(out / "fills.parquet").height == 0


def test_archive_row_count_mismatch_uses_reservoir(tmp_path):
    vike, reservoir = tmp_path / "vike.parquet", tmp_path / "reservoir.parquet"
    vike_file(vike, [fill()])
    reservoir_file(reservoir, [reservoir_fill(), reservoir_fill(trade_id=2)])
    window = Window(utc("2026-10-07T00:00:00Z"), utc("2026-10-07T00:00:05Z"))
    out = tmp_path / "truncated"
    with duckdb.connect() as connection:
        metadata = acquire_snapshots(
            out, [window], [], connection=connection,
            vike_archive=lambda day: (str(vike), {"date": day, "rows": 2}),
            reservoir_archive=lambda day: (str(reservoir), {"date": day}),
            ledger_page=lambda **_: {"items": []},
        )
    receipt = metadata["fills"]["acquisition"]["slices"][0]
    assert receipt["attempts"][0]["error"] == "archive_manifest_row_count_mismatch"
    assert receipt["selected_provider"] == "reservoir"
    assert metadata["fills"]["intervals"] == [window.pair()]
    assert pl.read_parquet(out / "fills.parquet").height == 2


def test_execution_conflicts_are_not_exact_duplicates(tmp_path):
    vike = tmp_path / "vike.parquet"
    vike_file(vike, [fill(), fill(side="sell", size_token=1.0, price=200.0)])
    window = Window(utc("2026-10-07T00:00:00Z"), utc("2026-10-07T00:00:05Z"))
    out = tmp_path / "execution-conflict"
    with duckdb.connect() as connection:
        metadata = acquire_snapshots(
            out, [window], [], connection=connection,
            vike_archive=lambda day: (str(vike), {"date": day}),
            reservoir_archive=unavailable, ledger_page=lambda **_: {"items": []},
        )
    report = run_study(out / "fills.parquet", out / "ledger.parquet", metadata)
    counts = report["manifest"]["diagnostics"]["historical_fills"]
    assert counts["conflicting_keys"] == 1
    assert counts["duplicate_rows"] == 0


def test_inconsistent_reported_notional_is_not_usable(tmp_path):
    vike = tmp_path / "vike.parquet"
    vike_file(vike, [
        fill(trade_id=1, size_usd=20_000.0),
        fill(trade_id=2, wallet="0x" + "b"*40, size_usd=200.00000001),
        fill(trade_id=3, wallet="0x" + "c"*40, size_usd=float("inf")),
    ])
    window = Window(utc("2026-10-07T00:00:00Z"), utc("2026-10-07T00:00:05Z"))
    out = tmp_path / "notional"
    with duckdb.connect() as connection:
        metadata = acquire_snapshots(
            out, [window], [], connection=connection,
            vike_archive=lambda day: (str(vike), {"date": day}),
            reservoir_archive=unavailable, ledger_page=lambda **_: {"items": []},
        )
    data = pl.read_parquet(out / "fills.parquet").sort("trade_id")
    assert data["notional_usd"].to_list() == [None, 200.00000001, None]
    assert metadata["fills"]["intervals"] == []
    report = run_study(out / "fills.parquet", out / "ledger.parquet", metadata)
    assert report["manifest"]["diagnostics"]["historical_fills"]["invalid_notional"] == 2


def test_acquisition_has_private_spill_and_explicit_resource_budget(tmp_path):
    day = tmp_path / "day.parquet"
    vike_file(day, [fill()])
    window = Window(utc("2026-10-07T00:00:00Z"), utc("2026-10-07T00:00:05Z"))
    out = tmp_path / "bounded"
    with duckdb.connect() as connection:
        metadata = acquire_snapshots(
            out, [window], [], connection=connection,
            vike_archive=lambda date: (str(day), {"date": date}),
            reservoir_archive=unavailable, ledger_page=lambda **_: {"items": []},
        )
        spill, memory, disk = connection.execute(
            "SELECT current_setting('temp_directory'), current_setting('memory_limit'), "
            "current_setting('max_temp_directory_size')"
        ).fetchone()
        from pathlib import Path
        assert Path(spill).resolve().is_relative_to(out.resolve())
        assert memory == "4.0 GiB"
        assert disk == "12.0 GiB"
    budget = metadata["fills"]["acquisition"]["resource_budget"]
    assert budget["memory_limit"] == "4GiB"
    assert budget["spill_limit"] == "12GiB"
    assert budget["max_snapshot_bytes"] > (out / "fills.parquet").stat().st_size
    assert not list(out.glob(".staging-*"))


def test_snapshot_byte_budget_fails_without_success_manifest(tmp_path):
    day = tmp_path / "day.parquet"
    vike_file(day, [fill()])
    window = Window(utc("2026-10-07T00:00:00Z"), utc("2026-10-07T00:00:05Z"))
    out = tmp_path / "too-small"
    with duckdb.connect() as connection, pytest.raises(RuntimeError, match="snapshot byte budget"):
        acquire_snapshots(
            out, [window], [], connection=connection,
            vike_archive=lambda date: (str(day), {"date": date}),
            reservoir_archive=unavailable, ledger_page=lambda **_: {"items": []},
            max_snapshot_bytes=1,
        )
    assert not (out / "metadata.json").exists()
    assert not list(out.glob(".staging-*"))


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


@pytest.mark.parametrize("system_address", [
    "0x" + "0"*40,
    "0x" + "0"*36 + "dead",
    "0x" + "22"*20,
    "0x" + "fe"*20,
    "0x" + "20" + "00"*18 + "c8",
])
def test_system_addresses_cannot_establish_direct_funding_links(tmp_path, system_address):
    window = Window(utc("2026-10-07T00:00:00Z"), utc("2026-10-07T00:00:05Z"))
    wallet = "0x" + "b"*40
    out = tmp_path / "system-address"
    with duckdb.connect() as connection:
        metadata = acquire_snapshots(
            out, [], [window], connection=connection,
            vike_archive=unavailable, reservoir_archive=unavailable,
            ledger_page=lambda **p: {"items": [], "nextCursor": None} if p.get("cursor") else {
                "items": [ledger_item("1", system_address, -125), ledger_item("1", wallet, 125)],
                "nextCursor": "next",
            },
        )
    report = run_study(out / "fills.parquet", out / "ledger.parquet", metadata)
    assert report["manifest"]["diagnostics"]["ledger"]["verified"] == 0
    assert report["manifest"]["diagnostics"]["ledger"]["intermediary"] == 1


def test_undated_ledger_row_invalidates_window_even_after_cursor_exhaustion(tmp_path):
    window = Window(utc("2026-10-07T00:00:00Z"), utc("2026-10-07T00:00:05Z"))
    a, b = "0x" + "a"*40, "0x" + "b"*40
    out = tmp_path / "undated"
    with duckdb.connect() as connection:
        metadata = acquire_snapshots(
            out, [], [window], connection=connection,
            vike_archive=unavailable, reservoir_archive=unavailable,
            ledger_page=lambda **p: {"items": [], "nextCursor": None} if p.get("cursor") else {
                "items": [ledger_item("1", a, -125), ledger_item("1", b, 125),
                          ledger_item("1", "0x" + "c"*40, 125, ts=None)],
                "nextCursor": "next",
            },
        )
    receipt = metadata["ledger"]["acquisition"]["slices"][0]
    assert receipt["pagination_exhausted"] is True
    assert receipt["undated_rows"] == 1
    assert receipt["complete"] is False
    assert metadata["ledger"]["intervals"] == []
    report = run_study(out / "fills.parquet", out / "ledger.parquet", metadata)
    assert report["manifest"]["diagnostics"]["ledger"]["verified"] == 0
    assert report["manifest"]["diagnostics"]["ledger"]["invalid_event_time_rows"] == 1


def test_daily_ledger_staging_preserves_boundary_and_deduplicates_pages(tmp_path):
    start = utc("2026-10-07T23:59:59Z")
    boundary = utc("2026-10-08T00:00:00Z")
    end = utc("2026-10-08T00:00:01Z")
    a, b = "0x" + "a"*40, "0x" + "b"*40
    earlier = [ledger_item("1", a, -125, ts=start.isoformat()),
               ledger_item("1", b, 125, ts=start.isoformat())]
    later = [ledger_item("2", a, -125, ts=boundary.isoformat()),
             ledger_item("2", b, 125, ts=boundary.isoformat())]
    def page(**params):
        if params.get("cursor"):
            return {"items": [], "nextCursor": None}
        return {"items": earlier + later if params["start"] == start.isoformat() else later,
                "nextCursor": "done"}
    out = tmp_path / "ledger-days"
    with duckdb.connect() as connection:
        metadata = acquire_snapshots(
            out, [], [Window(start, end), Window(boundary, end)], connection=connection,
            vike_archive=unavailable, reservoir_archive=unavailable, ledger_page=page,
        )
    assert metadata["ledger"]["intervals"] == [Window(start, boundary).pair(), Window(boundary, end).pair()]
    assert pl.read_parquet(out / "ledger.parquet").height == 4
    assert metadata["ledger"]["acquisition"]["slices"][0]["out_of_window_rows"] == 2
    report = run_study(out / "fills.parquet", out / "ledger.parquet", metadata)
    assert report["manifest"]["diagnostics"]["ledger"]["verified"] == 2


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
