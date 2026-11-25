# hl-links

Independent, read-only Hyperliquid wallet-relationship research.

**Operator documentation:** [T03 how-to and recovery guide](docs/t03-operator-guide.md) · [Changelog](CHANGELOG.md) · [MVP specification](docs/spec-mvp.md). For an existing pilot, follow the operator guide before updating the checkout or running any acquisition command.

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

The runner accepts signed USD-normalized fees and native USDC fees for main perpetuals, using nominal Hyperliquid contract dollars. The [official contract specifications](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/contract-specifications) establish USDC settlement without a USDC/USDT FX conversion. This is not fiat-USD valuation and does not model stablecoin depegs. Unknown fee units invalidate affected profit scores unless supported conversion evidence is supplied. Fills are deduplicated by wallet/trade identity and economics; conflicting identities invalidate affected eligibility or forward scores. Historical eligibility requires positive observed trade notional throughout the available pre-cutoff history; invalid notional invalidates that wallet's eligibility rather than establishing age or activity. Explicitly later-known historical fills are excluded, while retrospective forward outcomes use fill event time even if the records became known after the evaluation window. Zero-notional forward records must have zero realized PnL and zero fees; otherwise the affected score is invalid. Ledger transactions require exactly two compatible distinct opposite sides; a missing counterparty can qualify only when the other evidence uniquely identifies both sides. Providers must classify internal transfers and intermediary events before normalization. Undated evidence known by cutoff invalidates a matching historical transaction identity; undated later-known or unanchored evidence is disclosed but cannot change frozen membership.

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

Acquisition coalesces overlapping requests, splits fills and ledger windows at UTC day boundaries, and reads only projected fill fields. Vike is primary; unavailable archives, manifest/footer row-count mismatches, empty slices and structurally invalid slices trigger bounded Reservoir checks. Potentially main-perpetual rows with missing/blank market identifiers are counted before market filtering. Reservoir can restore coverage only if its clean slice includes every classified primary identity and resolves every unclassified identity with compatible economics. Unresolved rows remain explicit coverage gaps and are retained in the private fill snapshot with a market-quarantine flag and requested source-window bounds. They cannot establish trades, age, activity or scores. An unresolved identity matching an otherwise classified fill invalidates the affected wallet's economics in the relevant study window, including conflicts across daily batches. Undated quarantine evidence is considered for rejection only when its request bounds are wholly within that window; later or cutoff-straddling undated evidence cannot modify historical eligibility. A compatible clean Reservoir replacement clears the resolved quarantine. A clean Reservoir superset replaces a missing Vike slice; contradictory vendor records are never unioned or silently resolved. Unresolved discrepancies or missing slices remove the affected interval from declared coverage. A successful slice is not proof of venue-wide completeness. Execution identity retains side, quantity and price; notional must be finite and positive and match quantity × price within max(1e-8, 1e-9 × the larger absolute value). Invalid or conflicting observations cannot establish valid trading economics.

Ledger pages are exhausted by cursor until an empty terminal page without a continuation or with an exact echo of the requested cursor (observed live Vike behavior). An empty intermediate page with a valid new cursor does not terminate retrieval. Missing cursors on nonempty pages, nonempty-page cursor echoes, older-cursor cycles, invalid cursors, the per-day page budget, undated records and unexpected out-of-window timestamps invalidate coverage and prevent verification of that window's transfers. Every acquired ledger row records its requested source-window bounds separately from event, availability and ingestion times. An undated contradictory hash from a wholly pre-cutoff request can invalidate matching historical transfer evidence, but cannot establish an edge. Undated evidence with unknown availability from later or cutoff-straddling requests cannot alter historical links. The provider's inclusive upper endpoint is safely excluded. Ledger retrieval uses the study's $100 minimum per side; unmatched or inconsistent events do not establish independence. Token, direction, hash, compatible amount, event time and counterparty evidence are checked for direct sends. Zero/burn addresses and documented [HyperCore system addresses](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/hyperevm/hypercore-less-than-greater-than-hyperevm-transfers) and [assistance-fund address](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees) cannot form direct funding links. Other exchange/intermediary attribution remains unavailable.

Acquisition stages projected Parquet batches per UTC window, releases each temporary fill table, and consolidates the staged batches into the existing two-snapshot interface. Both acquisition and study DuckDB connections use private spill directories, four threads, a 4 GiB memory limit and a 12 GiB spill limit. Acquisition checks a 16 GiB free-disk reserve, including additional headroom before spill-capable reads and consolidation, and a 32 GiB default combined compressed snapshot budget (`--max-snapshot-gib` to change explicitly). Byte limits are checked at stage/publication boundaries, not an OS-level RSS or filesystem quota. The default ledger budget is 100 pages per UTC day (up to 5000 rows per page); incomplete windows are reported, never silently accepted. An exhausted snapshot budget prevents publication of a success manifest. Ordinary synthetic acquisition tests mock available disk capacity; separate resource-exhaustion cases verify that production safeguards still fail closed. A full-pilot capacity run is not part of T02.

Both providers preserve original signed fee amounts and currencies; the study runner applies the documented nominal main-perpetual USDC accounting convention, with the evidence URL and fee policy recorded in its manifest. No external FX conversion is performed. Historical availability is unknown, not the actual ingestion timestamp. The report retains all verified two-sided evidence privately and records source receipts, gaps, fallbacks and discrepancies.

**Bounded run evidence (November 24, 2026):** 358 Reservoir fills from September 1, 2025 after Vike rejected the date, plus 2,626 Vike fills across three October slices. Reservoir reconciled all 2,626 overlapping fills with zero missing keys or economic discrepancies in these slices. One 60-second ledger window exhausted six pages (45 rows), yielding ten verified transfers, 17 one-sided transactions and four non-direct transactions. All 2,984 sampled fees were USDC. The initial USD-only implementation rejected native USDC fees. Replaying the identical input hashes with the documented native accounting correction resolved all 1,878 historical score-window and 234 eligible forward fill fee units. Cohort counts remained zero proven, zero linked and 49 no-observed-link young wallets; 34 now have valid observed-fill forward scores and 15 have missing forward data. The result remains inconclusive because the time coverage is sparse and the linked cohort is empty. This is integration evidence, not a full archive coverage guarantee or a scientific comparison. Raw snapshots and reports remain ignored under `runs/t02-bounded-v1/`; `report-native-usdc.json` retains the corrected results and all ten verified transfer evidence records. A fresh hardened acquisition under `runs/t02-reviewed-v1/` reproduced the comparison exactly: all three Vike daily footer counts matched their manifests, 2,626/2,626 reconciled fills, zero invalid notionals or conflicting keys, six exhausted ledger pages, ten verified transfers and 34 valid forward wallet scores. Published fill and ledger Parquet files totaled 107,592 bytes; the run manifest records the resource limits. This verifies the bounded path, not full-pilot capacity. A second fresh hardened run under `runs/t02-reviewed-v3/` confirmed zero unclassified market records, all 2,626 shared fills reconciled, the six-page ledger terminating with Vike's exact cursor echo, ten verified transfers retaining source-window provenance, and identical cohort counts and comparison. The regression suite passed all 70 cases, including the empty-page continuation and cutoff-straddling cases. The final quarantine regression run under `runs/t02-reviewed-v4/` reproduced the same cohorts and comparison from a fresh acquisition with zero unclassified rows and zero conflicting keys; all 2,626 shared fills reconciled and all ten verified transfers remained intact. All 80 regression cases passed, including historical and forward identity conflicts in the same window, across separate windows, and with undated source-window evidence, plus compatible Reservoir recovery.

## Fixed historical pilot (T03)

The October 23 pilot uses the existing acquisition policy and the same wallet economics as the local runner. Capture historical UTC days separately with the existing read-only credentials:

```bash
uv run --locked python -m hl_links.pilot \
  --root runs/t03-pilot \
  --start 2026-05-01T00:00:00Z \
  --end 2026-10-23T00:00:00Z
```

The September 8 ledger retry is an independent replacement receipt at `runs/t03-ledger-retry-0908/`, not a union with the incomplete original. Resume the analysis with:

```bash
uv run --locked python -m hl_links.pilot_run \
  --root runs/t03-pilot \
  --analysis runs/t03-analysis \
  --ledger-retry runs/t03-ledger-retry-0908
```

The staged runner verifies immutable input receipts, partitions by normalized fill identity, and combines exact decimal wallet totals across shards. It verifies transaction identity across ledger days, retaining qualifying edges privately. It writes a checksum-verified historical cohort freeze before requesting any forward day. `--freeze-only` stops at that boundary. The standalone acquisition CLI is historical-only. A direct forward-day request also requires a valid staged freeze. Never launch two writers into the same analysis directory.

A day acquisition makes at most two capture attempts per invocation. Completed coverage is reused; incomplete coverage remains eligible for a later bounded attempt. Replacements are immutable, numbered receipts under the private day's `.replacements/` tree. Fill and ledger coverage select the first complete receipt independently, rather than unioning duplicate days. The original September 8 ledger replacement remains authoritative if complete. An already-published historical freeze or final report is immutable; a recovery that would change its inputs is a separate analysis revision, not a silent refreeze. Incompatible partition or summary checkpoints fail closed. New partition receipts bind DuckDB version, identity SQL and the hash algorithm; legacy receipts are checked against actual bucket assignments without rewriting the original receipt. Summary and combination operations check the disk reserve and spill budget before expensive work. Inspect disk and memory before running on a shared VPS.

The private analysis directory contains historical and forward partition receipts, wallet-summary checkpoints, `frozen/manifest.json`, `final/manifest.json`, `verified-v2/manifest.json` and `sanitized-v2/manifest.json`. Older verification/report receipts, if any, are preserved. Every verification invocation rechecks source and metadata hashes, partition contracts and checksums, summary requests and checksums, frozen membership, ledger evidence, sampled fill economics and the forward comparison; the first successful immutable verification receipt is reused only after those checks succeed again. This is checkpoint replay, not independent vendor certification or scientific qualification. Do not commit private run artifacts or signed URLs. Only aggregate sanitized research findings belong in Git.

If a pilot is already executing at the reviewed T03 commit `44482f07ec587c7b7a89951b23ecb52450067a93`, let its single writer reach a durable checkpoint or exit before updating the checkout. Resume from the same project directory and private paths. The reviewed freeze can be replayed by the hardened runner only while the acquisition and economic modules retain the pinned content digests; the historical freeze is not regenerated.

**State:** T01 provides the deterministic study runner; T02 connects historical vendors and demonstrates a bounded real-data run. T03 requires actual pilot results, not just passing fixture tests. All findings remain exploratory; no trading authorization or production integration.
