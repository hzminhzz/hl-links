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

**State:** The first study has not been implemented or executed. The initial `src/hl_links/cohorts.py` still contains pre-grill rules (`$5K` profit threshold and transfers without a `$100` minimum) and must be aligned with these approved decisions during implementation. All findings remain exploratory; no trading authorization or production integration.
