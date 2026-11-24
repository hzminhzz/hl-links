"""Read normalized Parquet fills with bounded, deterministic DuckDB aggregation."""

from datetime import datetime
from pathlib import Path

import duckdb

_REQUIRED = {
    "wallet", "trade_id", "event_at", "known_at", "ingested_at", "dex_name",
    "symbol", "closed_pnl_usd", "fee_amount", "fee_unit", "notional_usd",
}
# Hyperliquid main-perp PnL settles in USDC at the contract's nominal dollar
# value. The venue does not apply a USDC/USDT FX conversion. This is not an
# assertion that USDC always trades at one fiat USD.
ACCOUNTING_UNIT = "nominal Hyperliquid contract dollars (USDC settlement)"
FEE_POLICY_EVIDENCE = "https://hyperliquid.gitbook.io/hyperliquid-docs/trading/contract-specifications"
FEE_POLICY = "signed USD or native main-perp USDC fees; no external FX or implicit conversion of other tokens"

_MAIN_PERP = """
    upper(trim(dex_name)) = 'HL' AND trim(symbol) <> ''
    AND NOT regexp_matches(symbol, '[@#/:]')
"""

# Normalize the same ASCII whitespace for identity checks and cohort joins.
_WALLET = "lower(trim(wallet, ' ' || chr(9) || chr(10) || chr(11) || chr(12) || chr(13)))"

# Deduplicate by wallet/trade ID plus the actual economic event, never by
# source receipt timestamps. Conflicting variants invalidate the affected wallet.
_SUMMARY = f"""
WITH params AS (
    SELECT ?::TIMESTAMPTZ AS start_at, ?::TIMESTAMPTZ AS end_at,
           ?::TIMESTAMPTZ AS score_start, ?::VARCHAR[] AS wallets
), visible AS (
    SELECT {_WALLET} AS wallet, trim(trade_id::VARCHAR) AS trade_id,
           event_at, known_at, ingested_at, upper(trim(symbol)) AS symbol,
           try_cast(closed_pnl_usd AS DECIMAL(38,12)) AS pnl,
           try_cast(fee_amount AS DECIMAL(38,12)) AS fee,
           upper(trim(fee_unit)) AS fee_unit,
           try_cast(notional_usd AS DECIMAL(38,12)) AS notional
    FROM fill_input, params
    WHERE (start_at IS NULL OR event_at >= start_at) AND event_at < end_at
      -- Historical eligibility is availability-bounded. Retrospective outcomes
      -- use event time, including fills captured after the forward window.
      AND (start_at IS NOT NULL OR known_at IS NULL OR known_at <= end_at)
      AND ({_MAIN_PERP})
      AND (wallets IS NULL OR list_contains(wallets, {_WALLET}))
), variants AS (
    SELECT wallet, trade_id, event_at, symbol, pnl, fee, fee_unit, notional,
           min(known_at) AS known_at, min(ingested_at) AS first_ingested_at,
           max(ingested_at) AS last_ingested_at, count(*) AS raw_count
    FROM visible
    GROUP BY wallet, trade_id, event_at, symbol, pnl, fee, fee_unit, notional
), earlier_keys AS (
    SELECT DISTINCT {_WALLET} AS wallet, trim(trade_id::VARCHAR) AS trade_id
    FROM fill_input, params
    WHERE start_at IS NOT NULL AND event_at < start_at
      AND ({_MAIN_PERP})
      AND (wallets IS NULL OR list_contains(wallets, {_WALLET}))
), checked AS (
    SELECT variants.*,
           count(*) OVER (PARTITION BY wallet, trade_id) AS variants,
           earlier_keys.trade_id IS NOT NULL AS cross_window_conflict,
           trade_id IS NULL OR trade_id = '' OR wallet IS NULL OR wallet = '' AS bad_key,
           fee_unit IS NULL OR fee_unit NOT IN ('USD', 'USDC') OR fee IS NULL AS bad_fee,
           pnl IS NULL AS bad_pnl,
           -- An observed historical trade must have positive notional.
           -- A zero-volume forward row cannot carry realized PnL or fees.
           notional IS NULL OR notional < 0
               OR (notional = 0 AND (start_at IS NULL OR pnl <> 0 OR fee <> 0))
               AS bad_notional,
           event_at >= score_start AS in_score
    FROM variants LEFT JOIN earlier_keys USING (wallet, trade_id), params
)
SELECT wallet, min(event_at) AS first_trade_at, max(event_at) AS last_trade_at,
       CASE WHEN count(*) FILTER (WHERE known_at IS NULL) > 0 THEN NULL
            ELSE max(known_at) END AS known_at,
       min(first_ingested_at) AS first_ingested_at, max(last_ingested_at) AS last_ingested_at,
       count(*) AS observed_variants,
       sum(raw_count - 1)::BIGINT AS duplicate_rows,
       count(DISTINCT trade_id) FILTER (WHERE variants > 1) AS conflicting_keys,
       count(DISTINCT trade_id) FILTER (WHERE cross_window_conflict) AS cross_window_conflicting_keys,
       count(*) FILTER (WHERE bad_key) AS invalid_keys,
       count(*) FILTER (WHERE known_at IS NULL) AS unknown_availability,
       count(*) FILTER (WHERE in_score) AS score_fills,
       count(*) FILTER (WHERE in_score AND bad_fee) AS unknown_fees,
       count(*) FILTER (WHERE in_score AND fee_unit = 'USDC' AND NOT bad_fee) AS native_usdc_fee_fills,
       count(*) FILTER (WHERE in_score AND bad_pnl) AS invalid_pnl,
       count(*) FILTER (WHERE bad_notional) AS invalid_notional,
       coalesce(sum(pnl - fee) FILTER (
           WHERE in_score AND NOT bad_key AND variants = 1
             AND NOT bad_notional AND NOT bad_fee AND NOT bad_pnl
       ), 0) AS net_pnl,
       coalesce(sum(notional) FILTER (
           WHERE in_score AND NOT bad_key AND variants = 1 AND NOT bad_notional
       ), 0) AS notional
FROM checked
GROUP BY wallet
ORDER BY wallet
"""


class FillArchive:
    """One read-only snapshot; historical and forward queries have separate bounds."""

    def __init__(self, path: Path):
        self.connection = duckdb.connect(":memory:")
        try:
            self.connection.execute("SET TimeZone = 'UTC'")
            self.connection.read_parquet(str(path)).create_view("fill_input")
            schema = dict(
                (name, dtype)
                for name, dtype, *_ in self.connection.execute("DESCRIBE fill_input").fetchall()
            )
            missing = _REQUIRED - schema.keys()
            if missing:
                raise ValueError(f"Missing fill columns: {sorted(missing)}")
            for field in ("event_at", "known_at", "ingested_at"):
                if schema[field] != "TIMESTAMP WITH TIME ZONE":
                    raise ValueError(f"Fill {field} must be timezone-aware")
        except BaseException:
            self.close()
            raise

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def input_diagnostics(self, *, forward_start: datetime, forward_end: datetime) -> dict:
        # Count unattributable records before the eligible-wallet filter. Undated
        # records cannot be attributed to either window; count them once.
        undated, invalid_wallets = self.connection.execute(f"""
            SELECT count(*) FILTER (WHERE event_at IS NULL),
                   count(*) FILTER (
                       WHERE event_at >= ? AND event_at < ? AND ({_MAIN_PERP})
                         AND ({_WALLET} IS NULL OR {_WALLET} = '')
                   )
            FROM fill_input
        """, [forward_start, forward_end]).fetchone()
        return {
            "invalid_event_time_rows": undated,
            "forward_invalid_wallet_rows": invalid_wallets,
        }

    def summarize(
        self, *, start: datetime | None, end: datetime, score_start: datetime,
        wallets: list[str] | None = None,
    ) -> tuple[list[dict], dict]:
        result = self.connection.execute(_SUMMARY, [start, end, score_start, wallets])
        columns = [column[0] for column in result.description]
        rows = [dict(zip(columns, row, strict=True)) for row in result.fetchall()]
        # Coverage is declared independently of sparse observed events. These
        # counts are diagnostics, not proof that every source day was complete.
        diagnostics = self.connection.execute(f"""
            SELECT count(*) FILTER (WHERE NOT coalesce(({_MAIN_PERP}), false)),
                   count(*) FILTER (WHERE ({_MAIN_PERP}) AND known_at > ?)
            FROM fill_input
            WHERE (?::TIMESTAMPTZ IS NULL OR event_at >= ?) AND event_at < ?
              AND (?::VARCHAR[] IS NULL OR list_contains(?::VARCHAR[], {_WALLET}))
        """, [end, start, start, end, wallets, wallets]).fetchone()
        counters = (
            "observed_variants", "duplicate_rows", "conflicting_keys", "cross_window_conflicting_keys", "invalid_keys",
            "unknown_availability", "score_fills", "unknown_fees", "native_usdc_fee_fills", "invalid_pnl",
            "invalid_notional",
        )
        details = {key: sum(row[key] for row in rows) for key in counters}
        details["excluded_market_rows"], details["late_known_rows"] = diagnostics
        details["first_observed_at"] = min(
            (row["first_trade_at"] for row in rows), default=None)
        details["last_observed_at"] = max(
            (row["last_trade_at"] for row in rows), default=None)
        details["first_ingested_at"] = min(
            (row["first_ingested_at"] for row in rows if row["first_ingested_at"] is not None),
            default=None)
        details["last_ingested_at"] = max(
            (row["last_ingested_at"] for row in rows if row["last_ingested_at"] is not None),
            default=None)
        return rows, details
