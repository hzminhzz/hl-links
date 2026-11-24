# 2026-11-23 — Source audit: Vike vs Reservoir (MVP)

**Status:** Verified observations and reversible MVP recommendation. No ingest/backtest feature was implemented. All external calls were read-only.

## Question

Which existing data source should the `hl-links` MVP use for Hyperliquid wallet funding relationships and historical trading outcomes?

## Live calls, comparable scope

- **Vike:** `hl_archive(view=datasets)`, `hl_archive(view=url, dataset=fills, date=...)`, remote DuckDB Parquet queries via time-limited archive URLs; `hl_wallet_activity(view=fills)`; `hl_ledger`.
- **Reservoir:** existing `reservoir` AWS profile, requester-pays S3 `HEAD` and DuckDB read-only Parquet queries against `s3://hydromancer-reservoir/by_dex/hyperliquid/fills/perp/all/date=YYYY-MM-DD/fills.parquet`.
- Tests compared Hyperliquid **main perpetual fills only**. Vike `dex_name=HL` also includes spot-market records with identifiers containing `@`, `#`, or `/`; omitting them materially inflated its apparent perp coverage. Reservoir's checked prefix was already scoped to `fills/perp`.
- All compared timestamps were normalized to UTC; local DuckDB session timezone display is not an event-time authority.

| UTC date | Vike main-perp fills | Reservoir main-perp fills | Difference |
| --- | ---: | ---: | ---: |
| 2026-10-07 | 8,049,202 | 8,049,202 | 0 |
| 2026-10-23 | 5,624,690 | 5,624,690 | 0 |

On October 7, Vike `dex_name=HL` contained **8,708,012** fills before the spot filter: `@...` or `.../...` accounted for **611,119** and `#...` for **47,691** additional non-perp records. Raw row counts without market-scope normalization are not comparable.

Three five-second UTC windows on October 7, beginning at 00:00, 12:00 and 23:00, yielded **1,588 main-perp fills from each provider**. Joining by `(lower(wallet), trade_id)` matched **1,588/1,588**, with **0** missing keys and **0** symbol/side/price/size/fee/realized-PnL discrepancies under floating-point tolerance. Per-wallet Vike API spot checks also matched several Reservoir fill records, including nonzero realized PnL. These samples do **not** prove every record across all dates is identical.

## Available fields and history

| Dimension | Vike | Reservoir |
| --- | --- | --- |
| Main-perp fills on checked dates | Same counts and sampled values | Same counts and sampled values |
| Fill attributes | Timestamp, symbol, side, size, price, fee, maker/crossed, closed PnL, order/trade IDs; start position in recent history | Timestamp, symbol, side, size, price, fee, realized PnL, start position plus liquidation, transaction, TWAP, builder and routing attributes |
| Daily historical fills | Listed from 2026-05-01 through 2026-11-22 at audit time | Confirmed 2025-09-01 main-perp object exists; broader earlier coverage needs date-by-date checks |
| Cross-wallet funding transfers | Direct global `hl_ledger` API | No transfer/ledger dataset identified in checked `by_dex/hyperliquid` or `global` S3 listings |
| Important limitations | Some `send` rows have no counterparty; reconstruct two-sided edges by transaction hash when possible; market classification required | S3 requester-pays, larger raw schema and potential egress, file timestamps can be later than event timestamps |
| Historical snapshot quality | Some October position hours were marked reconstructed; snapshot time can lag state time | Existing research has caveats on retrospective snapshot anchoring |

A one-minute Vike ledger probe on 2026-10-07 with minimum USD 100 returned two-sided hash-matching transfer events and some null counterparty fields. A transfer edge is a relationship hypothesis, **not** wallet common-ownership proof.

## Reversible source recommendation for MVP

1. **Vike first** for dated fills (main-perp filter) and transfers. This requires only one primary vendor path for the hypothesis and does not require a new data service.
2. **Reservoir on demand** for pre-May-2026 history, missing/ambiguous fills, liquidations or detailed reconciliation. Do not mirror the whole reservoir archive.
3. **HyperX optional** for present-day wallet discovery and diagnostics only. Its current leaderboard must not define a historical as-of population.
4. Use DuckDB/Polars directly to form the historical universe and construct linked vs **no-observed-link** cohorts. Store only compact study results and reproducibility metadata under the isolated project.
5. Do not start with a graph database, dedicated event bus, streaming daemon, portfolio integration, or follower execution engine.

## Lean MVP evidence policy

- Study frozen historical **event-time** cutoffs and label runs **retrospectively reconstructed**, never falsely claim that `hl-links` had captured them prospectively.
- A two-sided direct transfer matched by transaction hash is strong enough for the initial linked cohort. A missing link is simply **no observed link**, not proof of independence.
- Report cohort size, data-span limits, missingness and PnL definition honestly. Start with one cutoff; expand only after the pipeline produces interpretable results.
- Extra coverage gates, network-depth exploration, matched causal controls, funding-cluster bootstrap, and follower net-PnL simulations are **deferred**, not required for the first MVP run.

## Finalized MVP choices (subsequent architecture grill)

- **Pilot:** 2026-10-23 UTC cutoff, 30-day forward window ending 2026-11-22 UTC.
- **Market:** Hyperliquid main perpetuals only, excluding spot and HIP-3.
- **Proven trader:** first observed trade at least 60 days before cutoff; $20,000 or more in fee-adjusted realized trading profit in the preceding 60 days.
- **Young wallet:** at least 7 and fewer than 60 days old at cutoff; traded during the preceding 7 days.
- **Link:** two-sided verified direct transfer of $100 or more within 60 days before cutoff.
- **Outcome:** 30-day fee-adjusted realized PnL per $100 traded, equal weight by wallet; report and exclude zero-forward-volume wallets from the ratio. Funding and mark-to-market effects are omitted, explicitly.
- **Evidence:** label all historical cohorts retrospectively reconstructed. Compare linked to **no observed link**, not proven-independent wallets. Track gaps without blocking the MVP over every uncertainty.

## Remaining factual unknowns (not additional architecture gates)

- Actual number of qualifying proven and linked wallets at the October 23 cutoff.
- Completeness of two-sided ledger events at the cutoff, exchange/intermediary attribution and historical first-trade censoring.
- Sensitivity of results to missing fees, spot/DEX classification and open positions.

No conclusion about predictive skill or tradeable alpha can be drawn from this data-source audit.
