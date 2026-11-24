"""Acquire bounded read-only vendor slices for the existing Parquet study runner.

The providers are intentionally not a general SDK. Archive URLs and credentials
are ephemeral; receipts contain only safe source identifiers and actual capture
times. The runner remains the owner of cohort and economic policy.
"""

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import subprocess
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import duckdb
import polars as pl

from .study import _utc

_AGENT = "OpenAI File Downloader, XaiImageApiFetch/1.0"
_BASE = "https://data.vike.io/v1/hyperliquid/"
_BUCKET = "hydromancer-reservoir"
_REGION = "ap-northeast-1"
_DAY = timedelta(days=1)


class SourceUnavailable(RuntimeError):
    """Provider could not establish coverage; message contains no credentials."""


@dataclass(frozen=True, order=True)
class Window:
    start: datetime
    end: datetime

    def __post_init__(self):
        object.__setattr__(self, "start", _utc(self.start))
        object.__setattr__(self, "end", _utc(self.end))
        if self.start >= self.end:
            raise ValueError("Window must be nonempty and half-open")

    def pair(self):
        return [self.start.isoformat(), self.end.isoformat()]


def _now():
    return datetime.now(timezone.utc)


def _windows(windows, *, daily):
    """Coalesce requested intervals before splitting; no overlapping acquisitions."""
    merged = []
    for window in sorted(windows):
        if merged and window.start <= merged[-1].end:
            merged[-1] = Window(merged[-1].start, max(merged[-1].end, window.end))
        else:
            merged.append(window)
    for window in merged:
        start = window.start
        while start < window.end:
            boundary = (start.replace(hour=0, minute=0, second=0, microsecond=0) + _DAY
                        if daily else start + 31 * _DAY)
            end = min(window.end, boundary)
            yield Window(start, end)
            start = end


def _archive_query(provider):
    if provider == "vike":
        fields = ("wallet", "fill_ts", "asset", "dex_name", "size_token", "closed_pnl", "size_usd")
        dex = "'HL'"
    elif provider == "reservoir":
        fields = ("address", "timestamp", "coin", "dex", "size", "realized_pnl", "size::DOUBLE * price::DOUBLE")
        dex = "'HYPERLIQUID'"
    else:
        raise ValueError("Unknown archive provider")
    wallet, ts, symbol, market, qty, pnl, notional = fields
    # Source schemas identify UTC; DuckDB's session is explicitly UTC below.
    # Null event timestamps remain diagnostics instead of disappearing in a filter.
    return f"""
        SELECT
            CASE WHEN regexp_full_match(lower(trim({wallet}, ' ' || chr(9) || chr(10) || chr(11) || chr(12) || chr(13))), '0x[0-9a-f]{{40}}')
                 THEN lower(trim({wallet}, ' ' || chr(9) || chr(10) || chr(11) || chr(12) || chr(13))) END AS wallet,
            trade_id::VARCHAR AS trade_id, {ts}::TIMESTAMPTZ AS event_at,
            NULL::TIMESTAMPTZ AS known_at, ?::TIMESTAMPTZ AS ingested_at,
            'HL'::VARCHAR AS dex_name, upper(trim({symbol})) AS symbol,
            try_cast({pnl} AS DOUBLE) AS closed_pnl_usd,
            try_cast(fee AS DOUBLE) AS fee_amount, upper(trim(fee_token)) AS fee_unit,
            CASE WHEN isfinite({qty}::DOUBLE) AND {qty} > 0
                       AND isfinite(price::DOUBLE) AND price > 0
                       AND lower(trim(side)) IN ('buy','sell')
                 THEN try_cast({notional} AS DOUBLE) END AS notional_usd,
            lower(trim(side)) AS side, {qty}::DOUBLE AS quantity,
            price::DOUBLE AS price, '{provider}' AS source,
            count(*)::BIGINT AS source_rows
        FROM read_parquet(?)
        WHERE ({ts} IS NULL OR ({ts} >= ? AND {ts} < ?))
          AND upper(trim({market})) = {dex}
          AND trim({symbol}) <> '' AND NOT regexp_matches({symbol}, '[@#/:]')
        GROUP BY ALL
    """


_FILL_SCHEMA = """
    wallet VARCHAR, trade_id VARCHAR, event_at TIMESTAMPTZ,
    known_at TIMESTAMPTZ, ingested_at TIMESTAMPTZ, dex_name VARCHAR,
    symbol VARCHAR, closed_pnl_usd DOUBLE, fee_amount DOUBLE, fee_unit VARCHAR,
    notional_usd DOUBLE, side VARCHAR, quantity DOUBLE, price DOUBLE, source VARCHAR
"""
_LEDGER_SCHEMA = {
    "transaction_id": pl.String, "wallet": pl.String, "counterparty": pl.String,
    "direction": pl.String, "transfer_type": pl.String, "intermediary": pl.Boolean,
    "event_at": pl.Datetime("us", "UTC"), "known_at": pl.Datetime("us", "UTC"),
    "ingested_at": pl.Datetime("us", "UTC"), "amount_usd": pl.String,
    "source": pl.String, "token": pl.String, "signed_amount_usd": pl.String,
    "window_complete": pl.Boolean,
}


def _read_slice(con, table, provider, archive, window):
    """A slice becomes visible only after a successful projected read."""
    uri, identity = archive(window.start.date().isoformat())
    captured = _now()
    con.execute(f"CREATE OR REPLACE TEMP TABLE {table} AS " + _archive_query(provider),
                [captured, uri, window.start, window.end])
    # Capture completion, not a fabricated historical receipt.
    captured = _now()
    con.execute(f"UPDATE {table} SET ingested_at = ?", [captured])
    count, invalid, duplicates = con.execute(
        f"SELECT count(*), count(*) FILTER (WHERE event_at IS NULL), "
        f"coalesce(sum(source_rows - 1), 0) FROM {table}"
    ).fetchone()
    con.execute(f"ALTER TABLE {table} DROP COLUMN source_rows")
    return {"provider": provider, "window": window.pair(),
            "source_date": window.start.date().isoformat(), "identity": identity,
            "retrieved_at": captured.isoformat(), "rows": count,
            "undated_rows": invalid, "duplicate_rows": duplicates}


def _error(exc):
    # A DuckDB/HTTP exception may include a signed URL. Never persist its text.
    return str(exc) if isinstance(exc, SourceUnavailable) else type(exc).__name__


def _reconcile(con):
    """Reconcile keys and economics without unioning the two providers."""
    keys = "wallet, trade_id"
    economics = "event_at, symbol, side, quantity, price, notional_usd, closed_pnl_usd, fee_amount, fee_unit"
    differs = " OR ".join(
        [f"a.{field} IS DISTINCT FROM b.{field}" for field in ("event_at", "symbol", "side", "fee_unit")]
        + [f"(a.{field} IS NULL) <> (b.{field} IS NULL) OR "
           f"abs(a.{field} - b.{field}) > greatest(1e-8, 1e-9 * greatest(abs(a.{field}), abs(b.{field})))"
           for field in ("quantity", "price", "notional_usd", "closed_pnl_usd", "fee_amount")]
    )
    counts = con.execute(f"""
        WITH a AS (SELECT *, row_number() OVER (PARTITION BY {keys} ORDER BY {economics}) AS variant
                   FROM primary_slice),
             b AS (SELECT *, row_number() OVER (PARTITION BY {keys} ORDER BY {economics}) AS variant
                   FROM check_slice)
        SELECT count(*) FILTER (WHERE a.variant IS NOT NULL AND b.variant IS NOT NULL),
               count(*) FILTER (WHERE a.variant IS NOT NULL AND b.variant IS NULL),
               count(*) FILTER (WHERE b.variant IS NOT NULL AND a.variant IS NULL),
               count(*) FILTER (WHERE a.variant IS NOT NULL AND b.variant IS NOT NULL AND ({differs}))
        FROM a FULL OUTER JOIN b USING (wallet, trade_id, variant)
    """).fetchone()
    return dict(zip(("matched", "vike_only", "reservoir_only", "economic_discrepancies"), counts, strict=True))


def _hex_identifier(value, digits):
    if isinstance(value, str):
        value = value.strip().lower()
        if len(value) == digits + 2 and value.startswith("0x") and all(c in "0123456789abcdef" for c in value[2:]):
            return value
    return None


def _address(value):
    return _hex_identifier(value, 40)


def _ledger_row(item, captured):
    try:
        amount = Decimal(str(item.get("amount_usd")))
        if not amount.is_finite() or not amount:
            amount = None
    except InvalidOperation:
        amount = None
    try:
        event_at = _utc(item["ts"])
    except (KeyError, ValueError, TypeError, AttributeError):
        event_at = None
    event_type = item.get("event_type")
    token = item.get("token")
    tx = item.get("hash")
    counterparty = item.get("counterparty")
    return {
        "transaction_id": _hex_identifier(tx, 64),
        "wallet": _address(item.get("wallet")),
        "counterparty": _address(counterparty),
        "direction": ("out" if amount < 0 else "in") if amount is not None else None,
        "transfer_type": event_type if isinstance(event_type, str) else None,
        # Only ordinary sends with no contradictory counterparty field enter
        # direct matching. A two-sided match still cannot identify exchange
        # intermediaries or establish common ownership.
        "intermediary": event_type != "send" or (counterparty is not None and _address(counterparty) is None),
        "event_at": event_at, "known_at": None, "ingested_at": captured,
        "amount_usd": str(abs(amount).normalize()) if amount is not None else None,
        "source": "vike:global-ledger", "token": token.strip().upper() if isinstance(token, str) else None,
        "signed_amount_usd": str(amount.normalize()) if amount is not None else None,
    }


def _fetch_ledger(window, page, *, limit, max_pages, seen, rows):
    cursor = None
    cursors = set()
    first_row = len(rows)
    receipt = {"window": window.pair(), "provider": "vike", "pages": 0,
               "raw_rows": 0, "duplicate_rows": 0, "out_of_window_rows": 0,
               "retained_rows": 0, "complete": False}
    for _ in range(max_pages):
        try:
            params = {"start": window.start.isoformat(), "end": window.end.isoformat(),
                      "min_usd": 100, "order": "asc", "limit": limit}
            if cursor is not None:
                params["cursor"] = cursor
            data = page(**params)
            captured = _now()
            items = data.get("items") if isinstance(data, dict) else None
            if not isinstance(items, list) or any(not isinstance(row, dict) for row in items):
                raise SourceUnavailable("invalid_ledger_page")
        except (SourceUnavailable, OSError, TimeoutError, ValueError) as exc:
            receipt["error"] = _error(exc)
            break
        receipt["pages"] += 1
        receipt["raw_rows"] += len(items)
        receipt["retrieved_at"] = captured.isoformat()
        if not items:
            receipt["complete"] = True
            break
        for item in items:
            row = _ledger_row(item, captured)
            ts = row["event_at"]
            if ts is not None and not window.start <= ts < window.end:
                receipt["out_of_window_rows"] += 1
                continue
            key = tuple(v for k, v in row.items() if k != "ingested_at")
            if key in seen:
                receipt["duplicate_rows"] += 1
                continue
            seen.add(key)
            rows.append(row)
            receipt["retained_rows"] += 1
        cursor = data.get("nextCursor")
        if not isinstance(cursor, str) or not cursor or cursor in cursors:
            receipt["error"] = "missing_or_repeated_cursor"
            break
        cursors.add(cursor)
    if not receipt["complete"]:
        receipt.setdefault("error", "page_budget_exhausted")
    for row in rows[first_row:]:
        row["window_complete"] = receipt["complete"]
    return receipt


def _receipt(source, slices, intervals, **details):
    acquisition = {"slices": slices, **details}
    digest = hashlib.sha256(json.dumps(acquisition, sort_keys=True).encode()).hexdigest()
    return {"source": source, "retrieval_id": digest, "intervals": intervals,
            "acquisition": acquisition}


def acquire_snapshots(output_dir, fill_windows, ledger_windows, *, connection,
                      vike_archive, reservoir_archive, ledger_page,
                      reconcile=False, page_limit=5000, max_pages=1000):
    """Public acquisition seam. External callbacks resolve source files/pages.

    Only successfully scanned intervals or exhausted ledger windows declare
    coverage. A failed reconciliation does not overwrite primary data.
    """
    if not 1 <= page_limit <= 5000 or max_pages < 1:
        raise ValueError("Invalid pagination budget")
    output_dir = Path(output_dir)
    output_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    con = connection
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"CREATE OR REPLACE TEMP TABLE acquired_fills ({_FILL_SCHEMA})")
    fill_receipts, fill_intervals = [], []
    for window in _windows(fill_windows, daily=True):
        receipt = {"window": window.pair(), "attempts": []}
        for provider, archive in (("vike", vike_archive), ("reservoir", reservoir_archive)):
            try:
                result = _read_slice(con, "primary_slice", provider, archive, window)
            except (SourceUnavailable, OSError, TimeoutError, duckdb.Error) as exc:
                receipt["attempts"].append({"provider": provider, "error": _error(exc)})
                continue
            receipt["attempts"].append(result)
            receipt["selected_provider"] = provider
            con.execute("INSERT INTO acquired_fills SELECT * FROM primary_slice")
            covered = not result["undated_rows"]
            if reconcile and provider == "vike":
                try:
                    check = _read_slice(con, "check_slice", "reservoir", reservoir_archive, window)
                    differences = _reconcile(con)
                    receipt["reconciliation"] = {"source": check, **differences}
                    if check["undated_rows"] or any(differences[k] for k in (
                        "vike_only", "reservoir_only", "economic_discrepancies",
                    )):
                        covered = False
                except (SourceUnavailable, OSError, TimeoutError, duckdb.Error) as exc:
                    receipt["reconciliation"] = {"error": _error(exc)}
            if covered:
                fill_intervals.append(window.pair())
            break
        fill_receipts.append(receipt)
    fills_path = output_dir / "fills.parquet"
    con.execute("COPY acquired_fills TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(fills_path)])
    fee_units = {str(unit): count for unit, count in con.execute(
        "SELECT fee_unit, count(*) FROM acquired_fills GROUP BY fee_unit ORDER BY fee_unit"
    ).fetchall()}
    ledger_receipts, ledger_intervals, rows, seen = [], [], [], set()
    for window in _windows(ledger_windows, daily=False):
        receipt = _fetch_ledger(window, ledger_page, limit=page_limit, max_pages=max_pages,
                                seen=seen, rows=rows)
        ledger_receipts.append(receipt)
        if receipt["complete"]:
            ledger_intervals.append(window.pair())
    ledger_path = output_dir / "ledger.parquet"
    pl.DataFrame(rows, schema=_LEDGER_SCHEMA).write_parquet(ledger_path)
    metadata = {
        "fills": _receipt("vike_primary_reservoir_fallback", fill_receipts, fill_intervals,
                          fee_units=fee_units, fee_policy="original signed fee and token retained; no conversion during acquisition"),
        "ledger": _receipt("vike_global_ledger", ledger_receipts, ledger_intervals,
                           min_usd=100, limit=page_limit, max_pages_per_window=max_pages,
                           intermediary_attribution="not available; two-sided ordinary sends only"),
    }
    metadata_path = output_dir / "metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    for path in (fills_path, ledger_path, metadata_path):
        path.chmod(0o600)
    return metadata


class Vike:
    def __init__(self, key):
        self.key = key

    def get(self, route, **params):
        if not self.key:
            raise SourceUnavailable("vike_key_unavailable")
        request = Request(_BASE + route + "?" + urlencode(params), headers={
            "X-API-KEY": self.key, "User-Agent": _AGENT, "Accept": "application/json",
        })
        try:
            with urlopen(request, timeout=30) as response:
                return json.load(response)
        except HTTPError as exc:
            raise SourceUnavailable(f"vike_http_{exc.code}") from None
        except (URLError, TimeoutError, ValueError):
            raise SourceUnavailable("vike_request_failed") from None

    def archive(self, day):
        data = self.get("archive/url", dataset="fills", date=day)
        if not isinstance(data, dict) or not isinstance(data.get("url"), str) or data.get("date") != day:
            raise SourceUnavailable("vike_archive_unavailable")
        return data["url"], {k: data[k] for k in ("date", "dataset", "bytes", "rows", "expires_at") if k in data}

    def ledger(self, **params):
        data = self.get("ledger", **params)
        if not isinstance(data, dict):
            raise SourceUnavailable("invalid_ledger_response")
        return data


class Reservoir:
    def __init__(self, connection, profile):
        self.connection = connection
        self.profile = profile
        self.ready = False

    def _aws(self, *args):
        try:
            return subprocess.run(["aws", *args, "--profile", self.profile],
                                  capture_output=True, text=True, check=True, timeout=45).stdout
        except (OSError, subprocess.SubprocessError):
            raise SourceUnavailable("reservoir_aws_request_failed") from None

    def archive(self, day):
        key = f"by_dex/hyperliquid/fills/perp/all/date={day}/fills.parquet"
        try:
            head = json.loads(self._aws("s3api", "head-object", "--bucket", _BUCKET, "--key", key,
                                        "--request-payer", "requester", "--region", _REGION, "--output", "json"))
        except ValueError:
            raise SourceUnavailable("reservoir_invalid_head_response") from None
        if not isinstance(head, dict) or not isinstance(head.get("ContentLength"), int):
            raise SourceUnavailable("reservoir_invalid_head_response")
        if not self.ready:
            creds = dict(line.split("=", 1) for line in self._aws(
                "configure", "export-credentials", "--format", "env-no-export").splitlines() if "=" in line)
            if not all(creds.get(k) for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")):
                raise SourceUnavailable("reservoir_credentials_unavailable")
            self.connection.execute(f"SET s3_region = '{_REGION}'; SET s3_requester_pays = true")
            self.connection.execute(
                "CREATE SECRET reservoir (TYPE S3, KEY_ID ?, SECRET ?, SESSION_TOKEN ?, SCOPE ?)",
                [creds["AWS_ACCESS_KEY_ID"], creds["AWS_SECRET_ACCESS_KEY"],
                 creds.get("AWS_SESSION_TOKEN", ""), f"s3://{_BUCKET}/"],
            )
            self.ready = True
        return f"s3://{_BUCKET}/{key}", {
            "key": key, "bytes": head.get("ContentLength"), "etag": head.get("ETag"),
            "last_modified": head.get("LastModified"),
        }


def _parse_window(value):
    start, end = value.split("/", 1)
    return Window(_utc(start), _utc(end))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True, help="New private directory under runs/")
    parser.add_argument("--fill-window", type=_parse_window, action="append", required=True, metavar="START/END")
    parser.add_argument("--ledger-window", type=_parse_window, action="append", required=True, metavar="START/END")
    parser.add_argument("--reservoir-profile", default="reservoir")
    parser.add_argument("--reconcile", action="store_true")
    parser.add_argument("--page-limit", type=int, default=5000)
    parser.add_argument("--max-pages", type=int, default=1000)
    args = parser.parse_args(argv)
    private_root = Path(__file__).resolve().parents[2] / "runs"
    output = args.output_dir.resolve()
    if output == private_root or not output.is_relative_to(private_root) or output.exists():
        parser.error("--output-dir must be a new directory inside runs/")
    private_root.mkdir(mode=0o700, exist_ok=True)
    con = duckdb.connect(":memory:", config={"custom_user_agent": _AGENT})
    try:
        con.execute("SET extension_directory = ?", [str(private_root / "duckdb_extensions")])
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        con.execute("SET threads = 4")
        vike = Vike(os.getenv("VIKE_API_KEY"))
        reservoir = Reservoir(con, args.reservoir_profile)
        acquire_snapshots(output, args.fill_window, args.ledger_window, connection=con,
                          vike_archive=vike.archive, reservoir_archive=reservoir.archive,
                          ledger_page=vike.ledger, reconcile=args.reconcile,
                          page_limit=args.page_limit, max_pages=args.max_pages)
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
