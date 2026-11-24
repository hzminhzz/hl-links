"""Exact partitioning must preserve the T01 economic and PIT semantics."""
import json
from pathlib import Path

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


def test_summary_and_combine_require_disk_headroom(tmp_path, monkeypatch):
    from hl_links import resources
    from hl_links.pilot_analysis import combine_summaries

    source = tmp_path / "fills.parquet"
    pl.DataFrame(base_fills(), schema=FILL_SCHEMA).write_parquet(source)
    parts = tmp_path / "parts"
    partition_fills([source], parts, buckets=1)
    output = tmp_path / "summary"
    usage = resources.shutil.disk_usage
    monkeypatch.setattr(resources.shutil, "disk_usage",
                        lambda path: usage(path)._replace(free=0))
    with pytest.raises(resources.SnapshotBudgetExceeded):
        summarize_partitions([parts], output, start=None, end=T, score_start=T-60*DAY)
    assert not (output / "bucket-000").exists()
    monkeypatch.setattr(resources.shutil, "disk_usage", usage)
    summarize_partitions([parts], output, start=None, end=T, score_start=T-60*DAY)
    monkeypatch.setattr(resources.shutil, "disk_usage",
                        lambda path: usage(path)._replace(free=0))
    with pytest.raises(resources.SnapshotBudgetExceeded):
        combine_summaries([output / "bucket-000"])


def test_partition_contract_rejects_changed_hash_semantics(tmp_path):
    source = tmp_path / "fills.parquet"
    pl.DataFrame(base_fills(), schema=FILL_SCHEMA).write_parquet(source)
    parts = tmp_path / "parts"
    partition_fills([source], parts, buckets=2)
    receipt = parts / "receipt.json"
    record = json.loads(receipt.read_text())
    assert record["partition_contract"]["duckdb"]
    record["partition_contract"]["duckdb"] = "incompatible"
    receipt.write_text(json.dumps(record))
    with pytest.raises(ValueError, match="Partition contract"):
        summarize_partitions([parts], tmp_path / "summary", start=None, end=T,
                             score_start=T-60*DAY)


def test_legacy_partition_is_validated_without_rewriting(tmp_path):
    source = tmp_path / "fills.parquet"
    pl.DataFrame(base_fills(), schema=FILL_SCHEMA).write_parquet(source)
    parts = tmp_path / "parts"
    partition_fills([source], parts, buckets=4)
    receipt = parts / "receipt.json"
    record = json.loads(receipt.read_text())
    del record["partition_contract"]
    receipt.write_text(json.dumps(record))
    before = receipt.read_bytes()
    summarize_partitions([parts], tmp_path / "summary", start=None, end=T,
                         score_start=T-60*DAY)
    assert receipt.read_bytes() == before
    # Misfile a real shard under a different bucket and update its path.
    first = record["shards"][0]
    old = parts / first["path"]
    wrong_bucket = (int(old.parent.name.split("=")[1]) + 1) % record["buckets"]
    new = parts / f"bucket={wrong_bucket}" / "wrong.parquet"
    new.parent.mkdir(exist_ok=True)
    old.rename(new)
    first["path"] = str(new.relative_to(parts))
    receipt.write_text(json.dumps(record))
    with pytest.raises(ValueError, match="Legacy partition contract"):
        summarize_partitions([parts], tmp_path / "other-summary", start=None, end=T,
                             score_start=T-60*DAY)


def test_standalone_acquisition_cannot_cross_cutoff(tmp_path, monkeypatch):
    from hl_links import pilot

    monkeypatch.setattr(pilot, "private_runs_dir", lambda: tmp_path)
    monkeypatch.setattr(pilot, "acquire_day",
                        lambda *args: pytest.fail("forward provider accessed"))
    with pytest.raises(SystemExit) as exc:
        pilot.main(["--root", str(tmp_path / "days"),
                    "--start", T.isoformat(), "--end", (T + DAY).isoformat()])
    assert exc.value.code == 2


def test_forward_day_api_requires_freeze(tmp_path, monkeypatch):
    from hl_links import pilot

    monkeypatch.setattr(pilot, "_capture_day",
                        lambda *args: pytest.fail("forward provider accessed"))
    with pytest.raises(ValueError, match="historical freeze"):
        pilot.acquire_day(tmp_path, T)


def test_incomplete_day_recovers_without_overwriting_receipts(tmp_path, monkeypatch):
    from hl_links import pilot

    calls = []
    day = T - DAY
    def capture(root, start, **kwargs):
        calls.append(root)
        destination = root / start.date().isoformat()
        destination.mkdir(parents=True)
        meta = {"fills": {"intervals": [[start.isoformat(), T.isoformat()]] if len(calls) >= 3 else [],
                          "acquisition": {"slices": [{"coverage_complete": len(calls) >= 3}]}},
                "ledger": {"intervals": [[start.isoformat(), T.isoformat()]],
                           "acquisition": {"slices": [{"complete": True}]}}}
        for name in ("fills.parquet", "ledger.parquet"):
            (destination / name).touch()
        (destination / "metadata.json").write_text(json.dumps(meta))
        return meta

    monkeypatch.setattr(pilot, "_capture_day", capture, raising=False)
    first = pilot.acquire_day(tmp_path, day, max_attempts=2)
    assert not first["fills"]["acquisition"]["slices"][0]["coverage_complete"]
    assert len(calls) == 2
    original = tmp_path / day.date().isoformat() / "metadata.json"
    before = original.read_bytes()
    second = pilot.acquire_day(tmp_path, day, max_attempts=2)
    assert second["fills"]["acquisition"]["slices"][0]["coverage_complete"]
    assert len(calls) == 3
    assert original.read_bytes() == before
    assert pilot.acquire_day(tmp_path, day) == second
    assert len(calls) == 3
    from hl_links.pilot_run import daily_receipt
    receipt = daily_receipt(tmp_path, "fills", day, T)
    assert receipt["days"][0]["path"] != str(original.with_name("fills.parquet"))
    assert receipt["days"][0]["declaration"] == second["fills"]


def test_ledger_recovery_keeps_first_complete_fill_receipt(tmp_path, monkeypatch):
    from hl_links import pilot
    from hl_links.pilot_run import daily_receipt

    day = T - DAY
    original = tmp_path / day.date().isoformat()
    calls = []
    def capture(root, start, **kwargs):
        calls.append(kwargs)
        directory = root / start.date().isoformat()
        directory.mkdir(parents=True)
        meta = {"fills": {"intervals": [[start.isoformat(), T.isoformat()]],
                          "acquisition": {"slices": [{"coverage_complete": True}]}},
                "ledger": {"intervals": [[start.isoformat(), T.isoformat()]] if len(calls) >= 3 else [],
                           "acquisition": {"slices": [{"complete": len(calls) >= 3}]}}}
        for name in ("fills.parquet", "ledger.parquet"):
            (directory / name).write_bytes(str(len(calls)).encode())
        (directory / "metadata.json").write_text(json.dumps(meta))
        return meta

    monkeypatch.setattr(pilot, "_capture_day", capture)
    pilot.acquire_day(tmp_path, day)
    pilot.acquire_day(tmp_path, day)
    fills = daily_receipt(tmp_path, "fills", day, T)
    ledger = daily_receipt(tmp_path, "ledger", day, T)
    assert Path(fills["days"][0]["path"]) == original / "fills.parquet"
    assert Path(ledger["days"][0]["path"]).read_bytes() == b"3"
    assert (original / "ledger.parquet").read_bytes() == b"1"
    assert calls == [{"fill_needed": True, "ledger_needed": True},
                     {"fill_needed": False, "ledger_needed": True},
                     {"fill_needed": False, "ledger_needed": True}]


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

    def provider(path, day, *, analysis):
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
    historical = next(iter(frozen["history_receipt"]["days"]))
    source = Path(historical["path"])
    original_bytes = source.read_bytes()
    source.write_bytes(original_bytes + b"changed")
    with pytest.raises(ValueError, match="Historical.*(input|receipt)|checksum"):
        runner.verify_replay(root, analysis, frozen, report)
    source.write_bytes(original_bytes)
    history_part = Path(frozen["history_partitions"][0]["path"])
    partition_receipt = history_part / "receipt.json"
    before = partition_receipt.read_bytes()
    partition_receipt.write_bytes(before + b" ")
    with pytest.raises(ValueError, match="Historical partition receipt"):
        runner.verify_replay(root, analysis, frozen, report)
    partition_receipt.write_bytes(before)
    summary = next((analysis / "forward-summary").glob("bucket-*/summary.parquet"))
    before = summary.read_bytes()
    summary.write_bytes(before + b"changed")
    with pytest.raises(ValueError, match="Summary checkpoint checksum"):
        runner.verify_replay(root, analysis, frozen, report)
    summary.write_bytes(before)
    clean = runner.sanitized(report)
    assert "wallet_scores" not in clean["comparison"]["linked"]
    assert "cohorts" not in clean
