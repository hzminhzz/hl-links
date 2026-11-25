# T03 — October 23, 2026 funding-link pilot

**Acceptance:** engineering and executed-study acceptance passed on 2026-11-25. **Research conclusion:** inconclusive (runner status: `partial`). No scientific qualification, follower-executable alpha or trading authorization.

## Study contract

The single retrospective cutoff is 2026-10-23T00:00:00Z. Historical archive coverage starts on 2026-05-01; scoring and transfers use [2026-08-24, 2026-10-23), recent activity uses the final seven historical days, and forward evaluation uses [2026-10-23, 2026-11-22), all UTC. Proven wallets require observed age at least 60 days and at least $20,000 trailing 60-day realized trading PnL after signed fees. Young wallets require observed age at least seven but less than 60 days and qualifying recent activity. Links require a verified direct two-sided transfer of at least $100, in either direction. Main perpetuals only.

Membership was frozen at 2026-11-24T20:04:34.814655Z, before the first observed forward ingestion at 2026-11-24T20:04:50.866291Z. These are retrospective execution timestamps, not evidence of observation at the historical cutoff. The thresholds were not tuned on forward outcomes.

## Population and outcomes

593,842 historical wallets were observed; 593,839 had valid trade evidence. There were 2,556 proven wallets, 39,406 eligible young wallets and 452 qualifying transfer events. The comparison is equal-weighted by wallet and uses fee-adjusted realized trading PnL per $100 traded. Funding, unrealized PnL and external FX/depeg effects are excluded. USDC settlement is treated as nominal Hyperliquid contract dollars.

| Metric | Linked | No observed link |
| --- | ---: | ---: |
| Eligible young wallets | 97 | 39,309 |
| Forward-trading wallets | 87 | 30,848 |
| Valid scores | 87 | 30,848 |
| Zero-volume exclusions | 10 | 8,461 |
| Missing data | 0 | 0 |
| Invalid scores | 0 | 0 |
| Unknown fee wallets | 0 | 0 |
| Mean PnL per $100 traded | -1.767807 | -1.955545 |
| Median PnL per $100 traded | -0.412525 | -0.318959 |
| Sample standard deviation | 6.618539 | 7.956324 |
| Descriptive 95% mean interval | [-3.158587, -0.377026] | [-2.044333, -1.866757] |

Linked minus control mean: **+0.187738** per $100 traded; descriptive 95% difference interval [-1.205873, +1.581350]. Median difference: **-0.093566**. Both groups lost money on average. The modest positive mean difference is uncertain and the median difference is negative. The linked sample is small, group sizes are highly imbalanced, and the descriptive intervals are not cluster-aware. This does not establish a predictive edge or causation. No new threshold or cutoff was tested.

## Coverage and limitations

- Vike supplied all 175 historical fill days and all 30 forward fill days; no Reservoir fallback was attempted in this pilot. All 60 ledger days declared complete coverage and exhausted pagination (1,095 pages, 5,018,573 raw rows, 47 duplicate rows removed). The September 8 replacement is preserved separately. There are no declared day gaps; producer declarations do not independently certify venue-wide completeness.
- Historical fill diagnostics: 972,365,962 observed variants, 427,464,486 score fills, four conflicting keys, zero unknown fee units. Forward diagnostics for eligible wallets: 18,719,239 score fills, zero conflicting keys, unknown fees, invalid PnL or invalid notional. No undated input rows or invalid forward wallet identities. Historical conflicts caused the runner's partial status; affected eligibility or economics are excluded.
- Ledger diagnostics include 939,118 verified transactions, 415,018 one-sided, 4,068 ambiguous, 2 invalid rows, 303,508 intermediary and 1,051,910 non-direct diagnostics. No qualifying edge is inferred from unresolved evidence. These diagnostics should not be treated as mutually exclusive independent wallet samples.
- Historical availability was unknown for all 972,365,962 historical fill variants and all 5,018,526 retained ledger rows. Historical event time is not independently established first-known time. Observed age is left-censored by archive start and is not actual wallet inception. No observed link is not evidence of independence, and a link does not establish shared ownership.

## Verification and reproducibility

The reviewed execution at commit `44482f07ec587c7b7a89951b23ecb52450067a93` produced immutable frozen and final manifests and an initial replay. Hardening commit `10b343bfbd473c44cf89de473caaccc344cd3cc6` passed 90/90 regression tests (75.09 seconds). The documentation-only successor `de5ed36d6a0e946f9db8fd9438f7545ebecc40f6` ran the fresh hardened replay, which finished with exit code 0 on 2026-11-25T02:43:57.624301Z. Cohort membership, outcomes and diagnostics matched. The original and v2 sanitized results match exactly excluding verification metadata.

Representative audits: 24/24 qualifying funding edges reconciled against ledger sides; 1,500 historical and 1,500 forward normalized fill samples checked with zero failures. All 64 historical and 64 forward summary shards were revalidated. This is checkpoint replay with source hashes and legacy bucket validation, not an independent vendor rescan or a complete re-execution of every original fill aggregation.

| Artifact | SHA-256 |
| --- | --- |
| Frozen manifest | `9299a160b8063d83373fc1b756e53848c2e68b631b57c157f839c257e995349f` |
| Final private report | `a65143d6622ee149e6230dcd1058c7b85ca56804fa2f4d063f7015bff4788bb0` |
| Original execution code | `ed3565187e24756b6d9834b4ef66c3c9153f2e2f9fbb4518944385ee16a7d5c8` |
| Hardened verification code | `748b68e7150ae6c17208006c50a413b9a7771627f1a18140c0eaa664d994d4fa` |

Private artifacts are preserved at `runs/t03-pilot/`, `runs/t03-ledger-retry-0908/` and `runs/t03-analysis/`. The hardened verification and aggregate machine-readable report are in `verified-v2/manifest.json` and `sanitized-v2/manifest.json` below the analysis directory. Raw wallet-level evidence, source receipts, credentials and signed URLs are excluded from Git. See the [operator guide](t03-operator-guide.md) for safe reproduction.

**Disposition:** T03's declared pilot is complete, with a partial/inconclusive scientific comparison. No production changes or branch merge are part of this acceptance.
