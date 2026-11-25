"""Bounded identity-sharded execution of the existing fill-study SQL.

Sharding by normalized (wallet, trade_id) keeps every conflicting variant in
one shard, including across UTC dates and the cutoff. Wallet summaries are
then combined with exact decimal sums and conservative availability bounds.
"""
from collections import Counter
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from .fills import FillArchive, SUMMARY_COUNTERS, _WALLET, summary_details
from .resources import SPILL_LIMIT_BYTES, bounded_duckdb, configure_duckdb, private_runs_dir, require_disk_headroom
from .ledger import verify_transfers
from .study import _sha256, _json_default


def _write_json(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, default=_json_default, sort_keys=True, indent=2, allow_nan=False)
        stream.write("\n")
    path.chmod(0o600)


def archive_checkpoint(directory):
    """Retain a superseded, unpublished checkpoint outside the active set."""
    directory = Path(directory)
    digest = _sha256(directory / "receipt.json")
    archive = directory.parent / ".superseded" / f"{directory.name}-{digest}"
    archive.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if archive.exists():
        raise FileExistsError(archive)
    directory.rename(archive)
    return archive


def wallet_digest(wallets):
    return None if wallets is None else hashlib.sha256(
        json.dumps(sorted(set(wallets))).encode()).hexdigest()


FILL_KEY = f"{_WALLET}, trim(trade_id::VARCHAR)"
# Python strip() whitespace, including Unicode separators.
_LEDGER_WHITESPACE = " || ".join(f"chr({ord(c)})" for c in "\t\n\v\f\r \x1c\x1d\x1e\x1f\x85\xa0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000")
LEDGER_KEY = f"lower(trim(transaction_id, {_LEDGER_WHITESPACE}))"


def partition_contract(key):
    return {"duckdb": duckdb.__version__, "identity_sql": key,
            "algorithm": "duckdb_hash_modulo_v1"}


def validate_partition_contract(root, receipt, key):
    expected = partition_contract(key)
    if "partition_contract" in receipt:
        if receipt["partition_contract"] != expected:
            raise ValueError("Partition contract mismatch")
        return
    # Pre-contract receipts remain immutable. Prove compatibility from actual
    # bucket assignment under the current engine, rather than guessing its version.
    files = [str(root / item["path"]) for item in receipt["shards"]]
    if not files:
        if receipt["rows"] != 0:
            raise ValueError("Legacy partition contract row mismatch")
        return
    with bounded_duckdb() as con:
        rows, wrong = con.execute(f"""
            SELECT count(*), count(*) FILTER (WHERE hash({key}) % ? != bucket)
            FROM read_parquet(?, hive_partitioning=true)
        """, [receipt["buckets"], files]).fetchone()
    if wrong or rows != receipt["rows"]:
        raise ValueError("Legacy partition contract mismatch")


def partition_fills(inputs, output, *, buckets=64, wallets=None):
    """Project normalized snapshots into identity shards, optionally for a frozen cohort."""
    where = "" if wallets is None else f"WHERE {_WALLET} IN (SELECT unnest(?::VARCHAR[]))"
    return _partition(inputs, output, buckets=buckets,
                      key=FILL_KEY,
                      where=where, parameters=[] if wallets is None else [sorted(set(wallets))],
                      selection=wallet_digest(wallets))


def _partition(inputs, output, *, buckets, key, where="", parameters=(), selection=None):
    output = Path(output)
    if output.exists():
        raise FileExistsError(output)
    if buckets < 1:
        raise ValueError("buckets must be positive")
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with TemporaryDirectory(prefix=".partition-", dir=output.parent) as stage:
        stage = Path(stage)
        target = stage / "parts"
        require_disk_headroom(output.parent, sum(p.stat().st_size for p in inputs))
        with duckdb.connect() as con:
            configure_duckdb(con, stage / "spill")
            con.execute("SET enable_progress_bar = false")
            con.execute("SET preserve_insertion_order = false")
            con.execute("SET partitioned_write_max_open_files = 128")
            paths = [str(p) for p in inputs]
            raw_rows = con.execute("SELECT sum(num_rows) FROM parquet_file_metadata(?)", [paths]).fetchone()[0]
            source_rows = (con.execute(
                f"SELECT count(*) FROM read_parquet(?, hive_partitioning=false) {where}",
                [paths, *parameters]).fetchone()[0] if where else raw_rows)
            con.execute(f"""COPY (
                SELECT *, hash({key}) % {buckets} AS bucket
                FROM read_parquet(?, hive_partitioning=false) {where}
            ) TO ? (FORMAT PARQUET, PARTITION_BY (bucket), COMPRESSION ZSTD)""",
                        [paths, *parameters, str(target)])
            files = sorted(target.glob("bucket=*/*.parquet"))
            shard_rows = con.execute("SELECT sum(num_rows) FROM parquet_file_metadata(?)",
                                     [[str(p) for p in files]]).fetchone()[0] if files else 0
            target.mkdir(exist_ok=True)
            if shard_rows != source_rows:
                raise ValueError("Partition row conservation failure")
        _write_json(target / "receipt.json", {
            "buckets": buckets, "rows": source_rows, "raw_rows": raw_rows,
            "partition_contract": partition_contract(key),
            "wallets_sha256": selection,
            "inputs": [{"path": str(p), "sha256": _sha256(p)} for p in inputs],
            "shards": [{"path": str(p.relative_to(target)), "sha256": _sha256(p)} for p in files],
        })
        target.rename(output)


def _partition_inputs(roots, *, hashes=None, key=FILL_KEY):
    by_bucket, seen, sources = {}, set(), set()
    bucket_count = None
    for root in sorted(map(Path, roots)):
        receipt = json.loads((root / "receipt.json").read_text())
        if bucket_count is not None and receipt["buckets"] != bucket_count:
            raise ValueError("Partition bucket counts differ")
        bucket_count = receipt["buckets"]
        for item in receipt["inputs"]:
            source = Path(item["path"]).resolve()
            if source in sources:
                raise ValueError("Duplicate partition source")
            sources.add(source)
        for item in receipt["shards"]:
            path = root / item["path"]
            bucket = int(path.parent.name.split("=")[1])
            if not path.resolve().is_relative_to(root.resolve()) or not 0 <= bucket < bucket_count:
                raise ValueError("Invalid partition shard path")
            if path.resolve() in seen:
                raise ValueError("Duplicate partition shard")
            seen.add(path.resolve())
            by_bucket.setdefault(bucket, []).append(path)
            if hashes is not None:
                hashes[path] = item["sha256"]
        validate_partition_contract(root, receipt, key)
    return by_bucket


def summarize_shard(paths, output, *, start, end, score_start, wallets=None,
                    expected_hashes=None, resume_updates=False):
    """Materialize the exact T01 result and a safe diagnostic receipt."""
    output = Path(output)
    request = json.loads(json.dumps({
        "inputs": [{"path": str(p), "sha256": _sha256(p)} for p in sorted(paths)],
        "start": start, "end": end, "score_start": score_start,
        "wallets_sha256": wallet_digest(wallets),
        "fill_code_sha256": _sha256(Path(__file__).with_name("fills.py")),
        "duckdb": duckdb.__version__,
    }, default=_json_default, sort_keys=True))
    if expected_hashes is not None and any(
        item["sha256"] != expected_hashes[Path(item["path"])] for item in request["inputs"]
    ):
        raise ValueError("Partition shard checksum mismatch")
    if output.exists():
        receipt = json.loads((output / "receipt.json").read_text())
        if receipt.get("summary_sha256") != _sha256(output / "summary.parquet"):
            raise ValueError("Summary checkpoint checksum mismatch")
        if receipt.get("request") == request:
            return
        old_query = {k: v for k, v in receipt["request"].items() if k != "inputs"}
        new_query = {k: v for k, v in request.items() if k != "inputs"}
        if not resume_updates or old_query != new_query:
            raise ValueError("Summary checkpoint inputs or query changed")
        archive_checkpoint(output)
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    require_disk_headroom(private_runs_dir(), SPILL_LIMIT_BYTES)
    require_disk_headroom(output.parent, SPILL_LIMIT_BYTES + sum(p.stat().st_size for p in paths))
    with TemporaryDirectory(prefix=".summary-", dir=output.parent) as stage:
        stage = Path(stage)
        with FillArchive(paths) as archive:
            diagnostics = archive.write_summary(stage / "summary.parquet", start=start,
                                                 end=end, score_start=score_start, wallets=wallets)
        _write_json(stage / "receipt.json", {"request": request, "diagnostics": list(diagnostics),
                    "summary_sha256": _sha256(stage / "summary.parquet")})
        # An immutable completed directory is the checkpoint.
        stage.rename(output)


def combine_summaries(outputs):
    """Sum independent identity-shard contributions, not wallet returns."""
    files, diagnostics = [], [0, 0]
    for output in outputs:
        output = Path(output)
        receipt = json.loads((output / "receipt.json").read_text())
        path = output / "summary.parquet"
        if _sha256(path) != receipt["summary_sha256"]:
            raise ValueError("Summary checksum mismatch")
        files.append(str(path))
        diagnostics = [a+b for a,b in zip(diagnostics, receipt["diagnostics"], strict=True)]
    counters = ", ".join(f"sum({key})::BIGINT AS {key}" for key in SUMMARY_COUNTERS)
    spill_root = private_runs_dir()
    require_disk_headroom(spill_root, SPILL_LIMIT_BYTES)
    with TemporaryDirectory(prefix=".combine-", dir=spill_root) as spill:
        with duckdb.connect() as con:
            configure_duckdb(con, spill)
            result = con.execute(f"""
                SELECT wallet, min(first_trade_at) AS first_trade_at,
                    max(last_trade_at) AS last_trade_at,
                    CASE WHEN count(*) FILTER (WHERE known_at IS NULL) > 0 THEN NULL
                        ELSE max(known_at) END AS known_at,
                    min(first_ingested_at) AS first_ingested_at,
                    max(last_ingested_at) AS last_ingested_at,
                    {counters}, sum(net_pnl) AS net_pnl, sum(notional) AS notional
                FROM read_parquet(?) GROUP BY wallet ORDER BY wallet
            """, [files])
            columns = [item[0] for item in result.description]
            rows = [dict(zip(columns, row, strict=True)) for row in result.fetchall()]
    return rows, summary_details(rows, diagnostics)


def summarize_partitions(roots, output, *, start, end, score_start, wallets=None,
                         verify_only=False, resume_updates=False):
    hashes = {}
    shards = _partition_inputs(roots, hashes=hashes)
    outputs = []
    for bucket, paths in sorted(shards.items()):
        part = Path(output) / f"bucket-{bucket:03d}"
        if verify_only and not part.is_dir():
            raise ValueError("Missing summary checkpoint")
        summarize_shard(paths, part, start=start, end=end, score_start=score_start,
                        wallets=wallets, expected_hashes=hashes,
                        resume_updates=resume_updates and not verify_only)
        outputs.append(part)
        print("summary checkpoint", part.name, flush=True)
    obsolete = set(Path(output).glob("bucket-*")) - set(outputs)
    if obsolete and verify_only:
        raise ValueError("Unexpected summary checkpoint")
    if resume_updates and not verify_only:
        for part in sorted(obsolete):
            receipt = json.loads((part / "receipt.json").read_text())
            if receipt["summary_sha256"] != _sha256(part / "summary.parquet"):
                raise ValueError("Summary checkpoint checksum mismatch")
            archive_checkpoint(part)
    return combine_summaries(outputs) if outputs else ([], summary_details([], (0, 0)))


def verify_partitioned_ledger(inputs, output, *, cutoff, proven, young, buckets=32,
                              verify_only=False):
    """Verify each transaction once across all source days; retain only study edges."""
    output = Path(output)
    if not output.exists():
        if verify_only:
            raise ValueError("Missing ledger partition checkpoint")
        _partition(inputs, output, buckets=buckets, key=LEDGER_KEY)
    else:
        receipt = json.loads((output / "receipt.json").read_text())
        expected = [{"path": str(p), "sha256": _sha256(p)} for p in inputs]
        if receipt["inputs"] != expected or receipt["buckets"] != buckets:
            raise ValueError("Ledger checkpoint inputs changed")
        for item in receipt["shards"]:
            if _sha256(output / item["path"]) != item["sha256"]:
                raise ValueError("Ledger checkpoint checksum mismatch")
    edges, evidence = [], {}
    _, _, empty_counts = verify_transfers(None, cutoff)
    counts = Counter(empty_counts)
    for _, paths in sorted(_partition_inputs([output], key=LEDGER_KEY).items()):
        shard_edges, shard_evidence, shard_counts = verify_transfers(paths, cutoff)
        counts.update(shard_counts)
        for edge in shard_edges:
            if ((edge.sender in proven and edge.recipient in young)
                    or (edge.recipient in proven and edge.sender in young)):
                edges.append(edge)
                evidence[edge.transaction_id] = shard_evidence[edge.transaction_id]
    return sorted(edges, key=lambda edge: edge.transaction_id), evidence, dict(counts)
