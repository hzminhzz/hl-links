"""Exact partitioning must preserve the T01 economic and PIT semantics."""
import json

import polars as pl
import pytest

from hl_links.fills import FillArchive
from hl_links.ledger import verify_transfers
from hl_links.pilot_analysis import partition_fills, summarize_partitions
from test_study import T, DAY, FILL_SCHEMA, LEDGER_SCHEMA, base_fills, fill, transfer


def test_identity_shards_preserve_cross_day_conflicts_and_cutoff(tmp_path):
    # Duplicate identity across a day boundary and a future reuse of a key.
    rows = base_fills() + [
        fill("pro", 2, -59, pnl=20001, fee=1),
        fill("y", 3, 2, pnl=10000),
        fill("other", 99, -2, known_at=T + DAY),
        fill("Y", 100, -7),
        fill("y", 100, -7),
    ]
    all_path = tmp_path / "all.parquet"
    pl.DataFrame(rows, schema=FILL_SCHEMA).write_parquet(all_path)
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    pl.DataFrame(rows[::2], schema=FILL_SCHEMA).write_parquet(a)
    pl.DataFrame(rows[1::2], schema=FILL_SCHEMA).write_parquet(b)
    shards = tmp_path / "shards"
    partition_fills([a, b], shards, buckets=4)
    with FillArchive(all_path) as archive:
        expected_history = archive.summarize(start=None, end=T, score_start=T-60*DAY)
        expected_forward = archive.summarize(start=T, end=T+30*DAY, score_start=T,
                                             wallets=["y", "c"])
    assert summarize_partitions([shards], tmp_path / "history", start=None,
                                end=T, score_start=T-60*DAY) == expected_history
    assert summarize_partitions([shards], tmp_path / "forward", start=T,
                                end=T+30*DAY, score_start=T,
                                wallets=["y", "c"]) == expected_forward
    historical_path = tmp_path / "past.parquet"
    forward_path = tmp_path / "future.parquet"
    pl.DataFrame([r for r in rows if r["event_at"] < T], schema=FILL_SCHEMA).write_parquet(historical_path)
    pl.DataFrame([r for r in rows if r["event_at"] >= T], schema=FILL_SCHEMA).write_parquet(forward_path)
    history_shards, forward_shards = tmp_path / "past-parts", tmp_path / "future-parts"
    partition_fills([historical_path], history_shards, buckets=4)
    partition_fills([forward_path], forward_shards, buckets=4, wallets=["y", "c"])
    assert summarize_partitions([history_shards, forward_shards], tmp_path / "selected-forward",
                                start=T, end=T+30*DAY, score_start=T,
                                wallets=["y", "c"]) == expected_forward
    # Reusing a completed checkpoint must not rewrite it or accept a different query.
    before = sorted((p, p.stat().st_mtime_ns) for p in (tmp_path / "history").rglob("*.parquet"))
    assert summarize_partitions([shards], tmp_path / "history", start=None,
                                end=T, score_start=T-60*DAY) == expected_history
    assert sorted((p, p.stat().st_mtime_ns) for p in (tmp_path / "history").rglob("*.parquet")) == before
    with pytest.raises(ValueError, match="checkpoint"):
        summarize_partitions([shards], tmp_path / "history", start=None,
                             end=T, score_start=T-59*DAY)


def test_ledger_shards_match_complete_transaction_evidence(tmp_path):
    from hl_links.pilot_analysis import verify_partitioned_ledger

    rows = transfer("tx1", "pro", "y") + transfer("tx2", "other", "c")
    rows += transfer("tx3", "pro", "c") + [transfer(" TX3 ", "wrong", "c")[0]]
    rows += [dict(rows[0], transaction_id=" TX1 "), transfer("single", "pro", "y")[0]]
    all_path = tmp_path / "ledger.parquet"
    pl.DataFrame(rows, schema=LEDGER_SCHEMA).write_parquet(all_path)
    a, b = tmp_path / "a-ledger.parquet", tmp_path / "b-ledger.parquet"
    pl.DataFrame(rows[::2], schema=LEDGER_SCHEMA).write_parquet(a)
    pl.DataFrame(rows[1::2], schema=LEDGER_SCHEMA).write_parquet(b)
    expected, evidence, counts = verify_transfers(all_path, T)
    edges, retained, actual = verify_partitioned_ledger(
        [a, b], tmp_path / "ledger-parts", cutoff=T, proven={"pro"}, young={"y", "c"}, buckets=4)
    assert actual == counts
    assert edges == [edge for edge in expected if edge.transaction_id == "tx1"]
    assert retained == {"tx1": evidence["tx1"]}


def test_staged_pilot_freezes_before_provider_and_matches_fixture(tmp_path, monkeypatch):
    from hl_links import pilot_run as runner

    root = tmp_path / "days"
    analysis = tmp_path / "analysis"
    retry = tmp_path / "retry"
    fill_schema = dict(FILL_SCHEMA, quantity=pl.Float64, price=pl.Float64,
                       side=pl.String, unclassified_market=pl.Boolean,
                       source_window_start=FILL_SCHEMA["event_at"],
                       source_window_end=FILL_SCHEMA["event_at"])
    ledger_schema = dict(LEDGER_SCHEMA, token=pl.String, signed_amount_usd=pl.String,
                         window_complete=pl.Boolean)

    def day_snapshot(directory, day, fills, ledger):
        directory.mkdir(parents=True)
        fills = [dict(row, quantity=1.0, price=row["notional_usd"], side="buy",
                      unclassified_market=False, source_window_start=day,
                      source_window_end=day + DAY) for row in fills]
        ledger = [dict(row, token="USDC", signed_amount_usd=str(
            -row["amount_usd"] if row["direction"] == "out" else row["amount_usd"]),
            window_complete=True) for row in ledger]
        pl.DataFrame(fills, schema=fill_schema).write_parquet(directory / "fills.parquet")
        pl.DataFrame(ledger, schema=ledger_schema).write_parquet(directory / "ledger.parquet")
        metadata = {key: {"intervals": [[day.isoformat(), (day + DAY).isoformat()]],
                          "acquisition": {"slices": [{"selected_provider": "fixture",
                                                       "coverage_complete": True}]}}
                    for key in ("fills", "ledger")}
        (directory / "metadata.json").write_text(json.dumps(metadata))
        return metadata

    rows = base_fills()
    ledger = transfer("tx", "pro", "y")
    # The original September 8 receipt is preserved, but never joined with the replacement.
    original = T - 45 * DAY
    day_snapshot(root / original.date().isoformat(), original, [], transfer("wrong", "pro", "c", day=-45))
    day_snapshot(retry, original, [], [])
    for day in sorted({row["event_at"] for row in rows + ledger if row["event_at"] < T}):
        day_snapshot(root / day.date().isoformat(), day,
                     [r for r in rows if r["event_at"] == day],
                     [r for r in ledger if r["event_at"] == day])

    calls = []

    def provider(path, day):
        assert (analysis / "frozen" / "receipt.json").is_file()
        calls.append(day)
        return day_snapshot(path / day.date().isoformat(), day,
                            [r for r in rows if r["event_at"] == day], [])

    monkeypatch.setattr(runner, "acquire_day", provider)
    frozen = runner.freeze(root, analysis, retry)
    assert not calls
    assert frozen["cohorts"]["linked"] == ["y"]
    assert frozen["cohorts"]["no_observed_link"] == ["c"]
    assert runner.freeze(root, analysis, retry) == frozen
    report = runner.evaluate(root, analysis, frozen)
    assert len(calls) == 30
    assert report["comparison"]["linked"]["mean"] == 10
    assert report["comparison"]["no_observed_link"]["mean"] == -5
    assert report["comparison"]["mean_difference"] == 15
    assert report["status"] == "partial"  # deliberately sparse historical fixture
    assert report["manifest"]["edge_audit"] == {"sampled": 1, "matched": 1}
    assert runner.evaluate(root, analysis, frozen) == report
    assert len(calls) == 30
    verification = runner.verify_replay(root, analysis, frozen, report)
    assert verification["cohorts_equal"] and verification["outcomes_equal"]
    assert runner.verify_replay(root, analysis, frozen, report) == verification
    clean = runner.sanitized(report)
    assert "wallet_scores" not in clean["comparison"]["linked"]
    assert "cohorts" not in clean
