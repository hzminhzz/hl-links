# hl-links

Independent, read-only Hyperliquid wallet-relationship research.

## Objective

Test whether newly active Hyperliquid wallets with a **verified pre-cutoff funding link** to a historically profitable trader outperform comparable unlinked wallets over a subsequent 30-day window. Treat this as a falsifiable hypothesis, not a trading signal.

## Scope and boundaries

- Standalone research repository under `/home/quant/dev/hl-links`.
- MVP sources: **Vike first** for historical fills and transfer ledger, **Reservoir** only for missing/older history and cross-checks, **HyperX** for optional enrichment. See [source audit](docs/source-audit-2026-11-23.md).
- Read-only access to existing historical data. No writes to `trading-ops`, `copytrade-pm`, `prop-production` or live trading services.
- No credentials in source control; no exchange order placement or trading authorization.
- Keep first-known timestamps and data availability separate from blockchain event times.
- Reconstruct the wallet universe *as of each cutoff*, including inactive and unsuccessful accounts. Never seed historical studies from today's leaderboard.
- MVP: test **direct, two-sided funding transfers**; flag obvious exchange/intermediary cases. Shared-funder, multi-hop and subaccount inference can be later studies.
- MVP report: linked vs **no observed link** cohorts, sample sizes, a clearly defined forward PnL metric and basic missing-data caveats. Matched controls, cluster uncertainty and follower execution are later extensions.

## Approved MVP study (architecture grill Q1-Q10)

- **Pilot cutoff (Q8):** `2026-10-23T00:00:00Z` only. The 30-day forward window is `[2026-10-23T00:00:00Z, 2026-11-22T00:00:00Z)`. Test another cutoff only after inspecting this pilot.
- **Approved proven-trader rule (Q5):** first observed trade at least 60 days before cutoff and **at least $20,000 realized trading profit during the 60 days before cutoff**. Profit is measured from historical fills, not current HyperX leaderboards.
- **Young wallet:** first observed trade 7 to fewer than 60 days before cutoff, and traded during the preceding seven days.
- **Approved money-link rule (Q6):** at least one verified, direct, two-sided transfer of **$100 or more in the 60 days before cutoff** between a young wallet and a proven trader. Preserve transfer direction, amount and evidence.
- **Approved primary outcome (Q7):** for each wallet, `100 × (sum(closed_pnl) - sum(trading_fees_usd)) / sum(fill_notional_usd)` over the 30-day forward window, then an **equal-wallet-weighted** cohort mean. Do not call it full account PnL: funding and unrealized PnL are excluded and must be disclosed. The same fee-adjusted realized measure is used for the $20K proven-trader screen.
- **Market scope (Q9):** Hyperliquid main perpetuals only; exclude spot and HIP-3. Explicitly filter Vike's spot identifiers (`@`, `#`, `/`), even when `dex_name=HL`. Reservoir's matching `fills/perp` partition is already perp-scoped.
- **Zero-volume rule (Q10):** exclude wallets with zero forward traded notional from the per-$100 cohort mean, but report the excluded count and denominator. Do not assign zero returns to undefined ratios.
- **MVP data path:** Vike archive fills + Vike global transfers → DuckDB/Polars cutoff cohorts → 30-day comparison and simple report. Reservoir is a read-only fallback and verification source; HyperX is optional enrichment, never a historical selection authority.
- Later outcome: follower-executable net PnL with execution delay, spread, fees, slippage and funding; not an MVP prerequisite.
- Declare cutoff and cohort screens before viewing forward results, mark historical cutoffs as **retrospectively reconstructed**, and do not infer common ownership, causation or tradeable alpha from a funding edge.

## Development

Uses Python 3.14 and `uv`. For a clean environment:

```bash
cd /home/quant/dev/hl-links
uv sync --group dev
uv run pytest
```

## Local study runner (T01)

Run from the project root with normalized, immutable Parquet snapshots in `data/` and write the private report under `runs/`:

```bash
uv run --locked python -m hl_links.study \
  --fills data/fills.parquet --ledger data/ledger.parquet \
  --metadata data/coverage.json --output runs/pilot.json
```

The default cutoff is the approved October 23 UTC pilot. `--cutoff` accepts an explicit timezone-aware ISO-8601 value for reproducible fixture experiments. The CLI only creates new report files inside the checkout's ignored `runs/` directory; existing files, input aliases and paths resolving outside that directory are rejected. Use a new filename for each run.

**Normalized inputs:** timestamps must be timezone-aware; `known_at` and `ingested_at` may be null rather than fabricated. Numeric money columns must be in the stated units.

| Snapshot | Required columns |
| --- | --- |
| Fills | `wallet`, `trade_id`, `event_at`, `known_at`, `ingested_at`, `dex_name` (HL for main venue), `symbol`, `closed_pnl_usd`, `fee_amount` (signed), `fee_unit`, `notional_usd` (absolute traded notional) |
| Ledger | `transaction_id`, `wallet`, `counterparty` (nullable), `direction` (out/in), `transfer_type` (send for confirmed direct external transfers), `intermediary` (boolean), `event_at`, `known_at`, `ingested_at`, `amount_usd`, `source` |

The runner accepts USD-normalized fees only; an adapter may normalize fees only with supported conversion evidence. Unknown units invalidate affected profit scores, not fabricate USD conversion. Fills are deduplicated by wallet/trade identity and economics; conflicting identities invalidate affected eligibility or forward scores. Historical eligibility requires positive observed trade notional throughout the available pre-cutoff history; invalid notional invalidates that wallet's eligibility rather than establishing age or activity. Explicitly later-known historical fills are excluded, while retrospective forward outcomes use fill event time even if the records became known after the evaluation window. Zero-notional forward records must have zero realized PnL and zero fees; otherwise the affected score is invalid. Ledger transactions require exactly two compatible distinct opposite sides; a missing counterparty can qualify only when the other evidence uniquely identifies both sides. Providers must classify internal transfers and intermediary events before normalization. Undated evidence known by cutoff invalidates a matching historical transaction identity; undated later-known or unanchored evidence is disclosed but cannot change frozen membership.

Example coverage metadata (illustrative, not verified live coverage):

```json
{
  "fills": {
    "source": "fixture",
    "retrieval_id": "fixture-fills-v1",
    "intervals": [["2026-07-25T00:00:00Z", "2026-11-22T00:00:00Z"]]
  },
  "ledger": {
    "source": "fixture",
    "retrieval_id": "fixture-ledger-v1",
    "intervals": [["2026-08-24T00:00:00Z", "2026-10-23T00:00:00Z"]]
  }
}
```

Intervals describe actual producer-verified coverage, not minimum/maximum timestamps of sparse rows. Use opaque retrieval identities without credentials. Input SHA-256s and code digest, declared coverage and gaps, cohort membership, transfer evidence, per-wallet outcomes, counts and limitations are recorded in the JSON report. Missing input paths, unresolved economics, undated records and coverage gaps are disclosed; an absent valid denominator produces an inconclusive result. Unattributable main-perpetual forward fills (null or blank wallet identity) make coverage incomplete and are counted before filtering for eligible wallets. No observed forward volume under incomplete coverage is classified as missing data, not inactivity. Positive-volume scores under incomplete coverage describe observed fills only. A comparison with declared coverage gaps, undated records or invalid economics is partial. The nominal descriptive interval is not cluster-aware evidence of skill.

## Bounded vendor acquisition (T02)

The acquisition CLI produces the same normalized Parquet inputs and coverage JSON consumed by the T01 runner. It uses an existing `VIKE_API_KEY` environment variable and the AWS CLI's read-only Reservoir profile (default: `reservoir`). Load the existing credentials in the shell without copying them into this repository. DuckDB's HTTP extension is installed in ignored `runs/duckdb_extensions/`; no vendor SDK, archive mirror or new service is needed.

Example bounded integration slice (UTC half-open windows):

```bash
uv run --locked python -m hl_links.acquire \
  --output-dir runs/t02-slice \
  --fill-window 2025-09-01T00:00:00Z/2025-09-01T00:00:05Z \
  --fill-window 2026-10-07T00:00:00Z/2026-10-07T00:00:05Z \
  --fill-window 2026-10-22T00:00:00Z/2026-10-22T00:00:05Z \
  --fill-window 2026-10-23T00:00:00Z/2026-10-23T00:00:05Z \
  --ledger-window 2026-10-07T00:00:00Z/2026-10-07T00:01:00Z \
  --reconcile --page-limit 10 --max-pages 100

uv run --locked python -m hl_links.study \
  --fills runs/t02-slice/fills.parquet \
  --ledger runs/t02-slice/ledger.parquet \
  --metadata runs/t02-slice/metadata.json \
  --output runs/t02-slice/report.json
```

Acquisition coalesces overlapping requests, splits fill windows at UTC day boundaries and ledger windows at 31 days, and reads only projected fill fields. Vike is primary; missing or unreadable archive slices fall back to Reservoir. Optional reconciliation compares normalized wallet/trade identities and economics without appending overlapping vendor rows. Discrepancies remove the affected interval from declared coverage. Ledger pages are exhausted by cursor until an empty page; repeated or missing cursors and page-budget exhaustion leave the window incomplete and its events cannot establish verified transfers. Ledger retrieval uses the study's $100 minimum per side; unmatched or inconsistent events do not establish independence. Token, direction, hash, compatible amount, event time and counterparty evidence are checked for direct sends. Exchange/intermediary attribution beyond the supplied event fields is not established.

Both providers preserve original signed fee amounts and currencies; USDC is **not** silently converted to USD. Historical availability is unknown, not the actual ingestion timestamp. The report retains all verified two-sided evidence privately and records source receipts, gaps, fallbacks and discrepancies.

**Bounded run evidence (November 24, 2026):** 358 Reservoir fills from September 1, 2025 after Vike rejected the date, plus 2,626 Vike fills across three October slices. Reservoir reconciled all 2,626 overlapping fills with zero missing keys or economic discrepancies in these slices. One 60-second ledger window exhausted six pages (45 rows), yielding ten verified transfers, 17 one-sided transactions and four non-direct transactions. All 2,984 sampled fees were USDC; the approved USD-only policy yielded zero proven or linked wallets and 49 young wallets with no observed link. Of those, 34 had sampled forward activity but invalid fee economics, and 15 had missing forward data. The result is inconclusive. This is sparse integration evidence, not a full archive coverage guarantee or a scientific comparison. Raw snapshots and reports remain ignored under `runs/t02-bounded-v1/`; the hardened runner reproduced the original cohorts and outcomes and retained all ten transfer evidence records.

**State:** T01 provides the deterministic study runner; T02 connects historical vendors and demonstrates a bounded real-data run. The full historical pilot belongs to T03. All findings remain exploratory; no trading authorization or production integration.
