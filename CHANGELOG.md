# Changelog

Changes to the independent Hyperliquid funding-link research project. Entries describe engineering milestones, not releases to production or claims of scientific qualification. The project remains on the MVP feature branch.

## Unreleased — T03 final review fixes (2026-11-25)

Implementation: `bdd609b726943c41ff79c33a15e362da68bf8f93`.

- Resume from complete replacements of previously partitioned incomplete days, retaining superseded unpublished artifacts and reusing unaffected batches and summaries.
- Produce an inconclusive report for missing historical evidence; the September 8 replacement provenance is conditional.
- Bind forward acquisition to its own frozen run and historical lineage through a shared checkpoint boundary, removing the acquisition-to-orchestration import.
- Apply private DuckDB resource configuration and disk headroom to legacy partition validation and representative audits.
- Preserve earlier verification receipts when compatible code changes; replay published input selections rather than silently selecting later recoveries.
- Regression suite: 93 passed in 21.57 seconds; original freeze accepted by the new code. The original study thresholds and completed report remain unchanged.
- Final real-data replay under this revision was stopped at the operator's request. On November 25, 2026 at 04:10:44 UTC, no pilot verification process remained and no new code-versioned verification receipt existed. The earlier successful v2 receipt remains the accepted real-data evidence; this revision has not completed a live replay. No branch merge or new trading authorization.

## T03 checkpoint hardening (2026-11-25)

Implementation: `10b343bfbd473c44cf89de473caaccc344cd3cc6`.

### Fixed

- Enforced disk headroom before materializing wallet summaries and combining shard totals, accounting for the DuckDB spill budget.
- Replaced permanent caching of incomplete UTC days with bounded retry and immutable, numbered replacement receipts. Complete fill and ledger coverage are selected independently; only missing streams are retried.
- Prevented standalone pilot acquisition of forward days. Direct forward-day requests require a verified historical freeze.
- Bound new partition receipts to the DuckDB version, normalized identity SQL and hash algorithm. Legacy receipts are checked against actual bucket assignment and row conservation without rewriting them.
- Made each reproducibility verification revalidate source and metadata hashes, partition lineage, summary requests and checksums, frozen membership, funding edges, sampled fill economics and reconstructed outcomes.
- Preserved the reviewed predecessor's frozen checkpoint through an explicit code-identity compatibility check. Acquisition and economic modules remain unchanged.

### Documentation

- Added the [T03 operator guide](docs/t03-operator-guide.md), covering pipeline stages, safe resume, recovery, monitoring, privacy and acceptance.
- Documented the hardened `verified-v2/` and `sanitized-v2/` artifacts, immutable lineage and limitations on changing inputs after publication.

### Verification

- 90/90 regression tests passed at the hardening commit; `git diff --check` passed.
- The real-data pilot and hardened replay completed successfully on 2026-11-25. Cohort membership, outcomes and diagnostics matched; the research conclusion remains partial/inconclusive. See [T03 pilot results](docs/t03-pilot-results.md).
- No production changes, branch merge, trading authorization or scientific qualification.

## T03 — Resumable pilot runner (2026-11-24)

Baseline: `44482f07ec587c7b7a89951b23ecb52450067a93`.

- Added UTC-day acquisition checkpoints and a staged historical freeze before forward evaluation.
- Added normalized identity partitioning, exact decimal aggregation, independent ledger transaction verification, representative audits and private/sanitized reporting.
- Preserved the independently recovered September 8 ledger receipt rather than unioning it with the incomplete original.

## T02 — Historical vendor integration (2026-11-24)

Verified baseline: `728159733ded7365df844c911460bfc7e32f8bae`.

- Integrated Vike archive fills and global ledger with bounded Reservoir fill fallback and reconciliation.
- Hardened market identity quarantine, pagination, source-window evidence, resource budgets and native USDC fee accounting.
- Verified bounded real-source acquisition and cross-provider reconciliation. See the [README](README.md#bounded-vendor-acquisition-t02) for evidence and limitations.

## T01 — Deterministic study runner (2026-11-24)

Verified baseline: `8a59adca3ae5e63ae5ae5c6802f92e9caa6ccbee`.

- Implemented fixed-cutoff historical eligibility, verified transfer evidence, per-wallet economics, equal-weight cohort comparison and reproducibility manifests.
- Added deterministic fixture coverage for cutoff boundaries, malformed evidence, fees, denominators and safe private output paths.
