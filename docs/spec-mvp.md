# Specification: hl-links MVP — Hyperliquid funding-link study

**Status:** Draft for publication; research rules approved in the architecture grill (Q1–Q10). Proposed public testing seam awaits confirmation.

## Problem Statement

The researcher wants to test whether recently active Hyperliquid wallets with a direct monetary relationship to historically profitable wallets have better subsequent trading performance than young wallets with no observed such relationship. Existing trading repositories serve operational and portfolio-research purposes; using them for an unvalidated venue-wide hypothesis would introduce unnecessary coupling. The independent research project exists, but its initial eligibility code implements older thresholds, and no historical funding-link study has yet been run.

## Solution

Deliver one reproducible, read-only historical research run for the October 23, 2026 UTC cutoff. Discover eligible wallets using historical main-perpetual fills, identify direct pre-cutoff funding links, freeze cohort membership before evaluating the forward period, and compare 30-day fee-adjusted realized trading PnL per $100 traded. Prefer existing Vike data, fall back to Reservoir when needed, and use HyperX only for optional enrichment. Produce a concise report with cohort sizes, actual sample coverage, outcome distributions and limitations. Positive, negative and inconclusive findings are all acceptable. This is exploratory retrospective evidence, not a validated causal signal, follower simulation or trading authorization.

## User Stories

1. As a researcher, I want to reconstruct the eligible Hyperliquid population from historical venue records, so that present-day rankings do not select the historical sample.
2. As a researcher, I want the approved age, activity and $20,000 profit screen applied at a fixed UTC cutoff, so that proven and young wallets have reproducible definitions.
3. As a researcher, I want qualifying direct transfers of at least $100 linked to historical wallet identities, so that I can separate observed monetary relationships from speculative associations.
4. As a researcher, I want comparison wallets labeled no observed link rather than proven independent, so that missing transfer evidence does not become a false ownership claim.
5. As a researcher, I want cohort membership determined before reading forward outcomes, so that later fills, profits or transfers cannot change historical eligibility.
6. As a researcher, I want per-wallet 30-day realized trading PnL after fees normalized by traded notional, so that wallets of different sizes can be compared.
7. As a researcher, I want zero-volume, missing-data and ambiguous-edge counts reported separately, so that undefined outcomes are not manufactured or silently omitted.
8. As a researcher, I want the pipeline to reuse existing providers and fall back on Reservoir for missing fill history, so that infrastructure and duplicate acquisition remain minimal.
9. As a researcher, I want compact reproducible artifacts and a clear report even when the hypothesis fails or source coverage is incomplete, so that I can decide whether further research is worthwhile.
10. As an operator, I want all acquisition and analysis isolated from production repositories, stores and trading services, so that research cannot mutate operational state or place trades.

## Implementation Decisions

### Approved study rules

- Fixed pilot cutoff T = 2026-10-23T00:00:00Z.
- Historical scoring and transfer lookback: [2026-08-24T00:00:00Z, T). Forward evaluation: [T, 2026-11-22T00:00:00Z). Use UTC half-open intervals throughout.
- Universe: Hyperliquid main perpetuals only. Start from venue-wide pre-cutoff historical fills, including unprofitable wallets. Do not seed the historical population from current HyperX rankings, balances or the tracked-wallet registry. Vike's HL designation alone does not exclude spot; the audited spot identifiers include @, # and /. Exclude HIP-3.
- Proven wallet: first observed main-perp trade at least 60 days before T and sum of realized fill PnL less trading fees of at least $20,000 in the preceding 60 days.
- Young wallet: observed main-perp age at least 7 and fewer than 60 days at T and at least one qualifying fill in [T - 7 days, T). Disclose the archive start and left censoring; first observed trade is not proof of actual lifetime inception.
- Linked young wallet: at least one verified direct, two-sided monetary transfer of at least $100 during [T - 60 days, T) between the young wallet and a proven wallet. Either direction qualifies; preserve direction, amount, event identity and source evidence. Match opposite-side ledger records by transaction identity, distinct wallets and compatible transfer amounts/types; do not double count repeated pages. Reject ambiguous one-sided events and obvious intermediaries, self-transfers, and account-class/subaccount bookkeeping as direct external links. Do not infer common ownership.
- Remaining eligible young wallets form the no-observed-link group. Report unresolved or incomplete transfer evidence. Do not claim the absence of an edge establishes independence.
- Freeze the eligible population, historical labels and links using pre-cutoff records before the forward aggregation. The pilot is retrospectively reconstructed; keep event time separate from local capture time, flag unknown historical availability, and do not manufacture historical receipt timestamps.

### Data and processing

- Vike archive fills and the Vike global ledger are the primary inputs; fetch bounded ledger windows, exhaust pagination, and deduplicate. Vike's archive was verified to span May 1–November 22, 2026 at audit time. Main-perp fill counts matched Reservoir on October 7 (8,049,202) and October 23 (5,624,690); 1,588 sampled shared fill records matched on tested economic fields. This does not certify all dates or ledger completeness.
- Reuse Reservoir's existing read-only access and perp-scoped archive for missing/older fill history and targeted validation. Record the actual source and gaps; don't union duplicate records blindly. HyperX is optional current-day enrichment/diagnostics only, never historical selection authority.
- Python 3.14, uv, DuckDB, Polars and Parquet. Prefer projected column reads, compact daily/wallet aggregates, and one pipeline over a full data mirror or new generalized provider framework. Use stable fill identities and source provenance; archive history before the 60-day lookback is needed to distinguish observed age.
- For each eligible young wallet with positive forward traded notional, compute:
  `100 * (sum(closed_pnl_usd) - sum(trading_fees_usd)) / sum(fill_notional_usd)`.
  Count each wallet's own fills once, including both opening and closing notional. Preserve signed fees/rebates. The same fee-adjusted realized measure defines the historical profit screen. Do not assume non-USD fees are USD: convert only with supported evidence, otherwise disclose and flag affected observations. Funding and changes in unrealized PnL are outside this estimator; this is not total account return.
- Equal weight per wallet within each cohort. Report total eligible/linked/control counts, forward-trading counts, excluded zero-volume wallets, valid denominator counts, group means and their difference, medians or other simple distribution diagnostics. Include basic descriptive uncertainty when the sample supports it; do not imply independent observations where shared funding or repeated identity may matter. No minimum effect or significance threshold is required for MVP completion.
- Preserve a reproducible run manifest: cutoff and windows, approved parameters, source dates/versions or retrieval identity, coverage, relevant code revision or content identifier, cohort counts and data limitations. Keep private raw wallet-level artifacts and credentials out of Git. Keep reports and artifacts within this isolated project.
- Missing source days, unavailable ledgers, zero qualifying cohorts or unresolved fee data are explicit limitations. Attempt bounded fallback for relevant fills. Report partial or inconclusive results rather than stopping over incidental gaps; do not publish a successful full comparison when required evidence or a valid cohort denominator is absent.

### Minimum public interface (proposed)

A single invocable study runner accepts the cutoff plus historical input/source configuration and emits the cohort comparison and run manifest. The same runner can consume small deterministic local fixtures for testing and actual vendor-backed snapshots for the pilot. No additional user-facing service or dashboard is required. Exact command syntax is an implementation detail.

## Testing Decisions

**Proposed seam awaiting confirmation:** the public study-runner boundary, exercised end to end with deterministic local fill and ledger inputs. Existing pure cohort eligibility tests remain targeted regression coverage; do not duplicate the entire pipeline in unit-test mocks.

- Update the initial tests that encode the superseded $5,000 rule and unrestricted transfer amount. Retain age/activity, direction, normalization, ambiguous-intermediary and future-known-event regression cases.
- Verify exact cutoff and interval boundaries, $20,000 net realized profit (inclusive), $100 transfer (inclusive), 7-/60-day age boundaries, seven-day activity, correct main-perp exclusions and duplicate-event handling.
- Verify two-sided versus one-sided transfers, direction, rejected ambiguous and self/internal edges, no-observed-link labels, known future evidence, and historical membership invariance when arbitrary forward records are changed.
- Exercise positive, negative and zero PnL, maker rebates, valid versus unknown fee units, equal wallet weighting (not volume weighting), zero forward volume, absent or empty cohorts, unavailable archive/ledger slices and explicit fallback or inconclusive reporting.
- Verify deterministic cohort membership and outputs for identical fixture inputs and correct provenance/coverage reporting. Tests assert observable outputs, not particular SQL plans or private helper layout.
- Live acceptance outside deterministic tests: execute the October 23 pilot against actual historical Vike data with bounded Reservoir fallback; verify input coverage, market scope, denominators, representative transfer matches and result reproducibility. Compare sampled records to the already audited sources when useful. Report actual tests, actual pilot results, limitations and any blocker. Do not require a positive edge.

## Out of Scope

- Multi-cutoff or weekly automated prospective cohorts, multi-hop/shared-funder graph discovery, ownership attribution, HIP-3 and spot performance.
- Full equity accounting, funding/unrealized PnL, follower execution/backtests, sizing, live deployment or trading authorization.
- Matching/causal identification, cluster-aware inference, formal scientific qualification, graph databases, streaming services, schedulers, dashboards, general-purpose provider SDKs, new shared data stores or modifications to sibling repositories.
- Optimizing the hypothesis thresholds after looking at the forward results. Any sensitivity run must be separately labeled and not replace the declared pilot.

## Further Notes

- The existing project is a local Git repository with no commits or configured remote at inspection. No issue tracker was found. No GitHub repository/issue creation or push is authorized by the architecture decisions alone.
- The initial cohort module has public eligibility and grouping functions, but its inputs and thresholds are not aligned with the approved historical study; there is no existing full-study runner.
- The source audit identifies historical ledger coverage, observed first-trade censoring and the actual linked-cohort sample size as factual unknowns. These are measured and reported during the pilot, not new architecture gates.
- Basic descriptive uncertainty is consistent with repository research guidance; advanced inference is deferred. Historical archive retrieval does not establish actual past observation by this project.
- Keep this as the one MVP work unit. The specification defines engineering acceptance and an executed research report, not scientific validation or trading readiness.
