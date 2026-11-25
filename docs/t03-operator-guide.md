# T03 — historical funding-link pilot: operator guide

This runbook covers the fixed October 23, 2026 pilot and its hardened checkpoint workflow (T03, GitHub issue #3). It is not a production deployment procedure or a general multi-cutoff research framework. For approved research rules, see [the MVP specification](spec-mvp.md); for vendor coverage evidence, see [the source audit](source-audit-2026-11-23.md).

## 1. Research contract

| Item | Fixed value |
| --- | --- |
| Historical archive start | 2026-05-01 00:00 UTC |
| Score and transfer lookback | [2026-08-24, 2026-10-23) UTC |
| Cohort cutoff | 2026-10-23 00:00 UTC |
| Forward evaluation | [2026-10-23, 2026-11-22) UTC |
| Proven wallet | Observed age >= 60 days; trailing 60-day net realized trading PnL >= $20,000 |
| Young wallet | Observed age >= 7 and < 60 days; traded in the preceding seven days |
| Link | Verified direct two-sided pre-cutoff transfer >= $100, either direction |
| Market | Hyperliquid main perpetuals only |
| Outcome | Equal-wallet-weighted fee-adjusted realized trading PnL per $100 traded |
| Providers | Vike primary; Reservoir bounded fallback for fills |

The outcome for a wallet is `100 * (closed_pnl - signed_trading_fees) / traded_notional`, using nominal contract dollars. Opening and closing notional count. Funding and unrealized PnL are excluded. Zero-volume ratios are undefined; missing coverage and invalid economics are reported separately.

Cohorts are reconstructed retrospectively from historical records, not today's leaderboard. A funding link is not ownership evidence. No observed link is not proof of independence. A result is exploratory source-wallet research, not causal evidence, follower-executable alpha, persistent skill or trading authorization.

## 2. Pipeline and checkpoint ownership

```text
historical daily fills + ledger receipts
                 |
                 v
    identity-sharded historical fills
                 |
                 v
     per-shard historical summaries
                 |
                 v
      wallet eligibility + ledger verification
                 |
                 v
        immutable historical freeze
                 |
                 v
     forward daily fills (frozen wallets)
                 |
                 v
     forward partitions + summaries
                 |
                 v
       immutable final private report
                 |
                 v
       fresh checkpoint-chain replay
                 |
                 v
       sanitized aggregate report
```

| Component | Responsibility |
| --- | --- |
| `hl_links.acquire` | Normalize source snapshots, coverage declarations, bounded fallback and ledger pagination. |
| `hl_links.pilot` | Acquire or recover independent UTC days. Standalone CLI only accepts historical days. |
| `hl_links.pilot_analysis` | Partition by normalized wallet/trade identity or ledger transaction identity; summarize and combine exact wallet totals. |
| `hl_links.pilot_run` | Freeze historical membership, acquire forward data, assemble and verify the report. |
| `hl_links.fills`, `ledger`, `cohorts`, `study` | Existing economic, eligibility, evidence and reporting rules. |

Each completed receipt is immutable. Temporary staging directories are not completed checkpoints. A single writer owns the acquisition and analysis trees; do not run a second pilot or change the checkout beneath an active process.

## 3. Resume the existing pilot safely

Run from the original project checkout, not from a detached development worktree. Some existing receipts reference paths relative to that checkout. Do not move, rename, delete or regenerate the existing run directories.

```bash
cd /home/quant/dev/hl-links
git status --short --branch
git rev-parse HEAD
git worktree list
ps -eo pid,etime,stat,comm | grep -E 'python|duckdb'
df -h .
free -h
```

Inspect the relevant process locally to confirm whether it owns the pilot directories. Do not print credentials, signed URLs or full environment variables. If a pilot is still running, leave it alone: **do not pull, switch branches, run another writer, kill the process or restart completed work.**

Only when the existing pilot writer has exited, and the checkout is clean and on `feat/hl-links-mvp`, update it:

```bash
git fetch origin feat/hl-links-mvp
git merge --ff-only origin/feat/hl-links-mvp
uv sync --locked --group dev
```

The hardened implementation explicitly supports the reviewed T03 freeze produced by commit `44482f07ec587c7b7a89951b23ecb52450067a93`, provided the pinned research/acquisition module digests still match. It does not refreeze an existing cohort. Other incompatible code identities fail closed.

Use the existing read-only Vike key in the environment and Reservoir's existing `reservoir` AWS profile. Do not copy keys into the repository, logs or documentation. The original pilot already has the private DuckDB extension and data directories. Check disk headroom before resuming; the code checks a 16 GiB reserve, and spill-capable summary/combination work reserves an additional 12 GiB (plus input-size allowance for shard summaries). These are preflight checks, not a filesystem quota or protection from other processes.

### Historical acquisition (only if necessary, before a freeze)

```bash
uv run --locked python -m hl_links.pilot \
  --root runs/t03-pilot \
  --start 2026-05-01T00:00:00Z \
  --end 2026-10-23T00:00:00Z \
  --workers 1
```

The existing pilot already has all 175 historical fill days and the ledger acquisition with a separate September 8 replacement. Do not rerun the historical command merely to restart analysis. For a specific incomplete day, use a one-day half-open UTC interval. This CLI exits unsuccessfully if a requested day remains incomplete; it does not promise a successful download. The standalone CLI does not accept dates after the cutoff.

**September 8 exception:** the original ledger receipt and the independently captured replacement at `runs/t03-ledger-retry-0908/` are part of the existing pilot's lineage. Preserve both. The staged runner uses that replacement when complete; do not reacquire or union it with the original. The following commands are for this existing pilot, not a new study with different paths or thresholds.

### Freeze only, if desired

```bash
uv run --locked python -m hl_links.pilot_run \
  --root runs/t03-pilot \
  --analysis runs/t03-analysis \
  --ledger-retry runs/t03-ledger-retry-0908 \
  --freeze-only
```

This completes or validates the historical freeze and exits without requesting forward data. Do not start another freeze once a valid immutable checkpoint exists.

### Complete or resume all stages

```bash
uv run --locked python -m hl_links.pilot_run \
  --root runs/t03-pilot \
  --analysis runs/t03-analysis \
  --ledger-retry runs/t03-ledger-retry-0908
```

The runner resumes the completed stages, acquires the 30-day forward period only after a valid freeze, evaluates the frozen cohorts, and performs reproducibility verification. If a final report already exists, the same command checks it and runs fresh verification without reacquiring completed forward days. No separate verification-only CLI is required for an existing final report.

## 4. Monitor without exposing private data

From the original checkout:

```bash
find runs/t03-analysis/history-parts -name receipt.json | wc -l
find runs/t03-analysis/history-summary -name receipt.json | wc -l
find runs/t03-analysis/forward-parts -name receipt.json | wc -l
find runs/t03-analysis/forward-summary -name receipt.json | wc -l
df -h .
```

The historical and forward fill pipelines use 64 identity buckets, but a receipt count for partition batches is not a day count. A bucket with no rows may have no summary. Check stage markers rather than inferring acceptance from these counts:

```bash
for stage in frozen final verified-v2 sanitized-v2; do
  if test -f "runs/t03-analysis/$stage/manifest.json"; then
    printf '%s: manifest present\n' "$stage"
  else
    printf '%s: pending\n' "$stage"
  fi
done
```

A present manifest is not proof that the underlying data still matches it. Run the staged command after the writer exits to verify the checkpoint chain.

## 5. Recovery and integrity behavior

| Situation | Action |
| --- | --- |
| Interrupted before a completed day receipt | Rerun the appropriate historical or staged command. Unpublished staging data is not a successful acquisition. |
| Incomplete day | The acquisition wrapper makes at most two capture attempts per invocation. It can retry on a later invocation; the original remains immutable. |
| Complete fills, incomplete ledger (or reverse) | Retry only the missing stream. Select the first complete receipt for each stream independently. Do not combine overlapping replacements. |
| Low disk headroom | Stop and inspect capacity. Resolve capacity safely; never delete existing pilot receipts or private source data to force progress. |
| Legacy partition receipt | Validate its actual row count and bucket assignments under the current DuckDB engine. No in-place receipt rewriting. |
| Changed partition contract, source, receipt or summary | Fail closed. Inspect the discrepancy; do not edit checksums to bypass validation. |
| Existing freeze with newly recovered historical data | Preserve the freeze. Any materially changed historical population requires a separately declared analysis revision, never a silent refreeze. |
| Existing final report with newly recovered forward data | Preserve the published report and its input lineage. A changed input set requires a separate analysis revision; there is no automatic final-report overwrite command. |

Before publishing a historical freeze or final report, the runner can recover a known incomplete input using its complete immutable replacement. It retains superseded partition batches and summaries under `.superseded/`, rebuilds only affected batches and identity buckets, and reuses unaffected checkpoints. Unexpected source changes, duplicate input identities, checksum mismatches and incompatible queries still fail closed. Missing historical coverage is reported explicitly; a missing September 8 original receipt is not a mandatory prerequisite. Once a freeze or final report exists, its selected inputs are immutable.

New partition receipts bind DuckDB version, identity SQL and the hash algorithm. For old receipts the validation reads the real shard assignments; this can cost I/O but avoids unnecessary repartitioning. Fresh verification also checks the source/metadata hashes, partition receipt lineage, summary requests and hashes, historical cohort membership, representative transfer and fill audits, and reconstructed outcome/diagnostics. Legacy validation and representative audits use the standard private DuckDB spill and disk budget. A prior verification receipt is reused only after the fresh checks pass. Later compatible code revisions produce separate `verified-v2-<code-sha256>/` and `sanitized-v2-<code-sha256>/` receipts. The original frozen population and final report are not rewritten. Replay uses the published forward input selection, not newly available replacements.

## 6. Artifacts and interpretation

All runtime artifacts stay under ignored `runs/`:

| Path (relative to the original checkout) | Contents |
| --- | --- |
| `runs/t03-pilot/YYYY-MM-DD/` | Original private normalized daily snapshots and metadata. |
| `runs/t03-pilot/.replacements/YYYY-MM-DD/attempt-NNNNNNNN/YYYY-MM-DD/` | Immutable replacement snapshots and metadata. |
| `runs/t03-ledger-retry-0908/` | Original independent September 8 ledger replacement. |
| `runs/t03-analysis/history-parts/`, `history-summary/`, `ledger-parts/` | Historical and ledger evidence checkpoints. |
| `runs/t03-analysis/frozen/manifest.json` | Private cutoff membership, funding edges, input and code digests. |
| `runs/t03-analysis/forward-parts/`, `forward-summary/` | Forward partitions and wallet economics. |
| `runs/t03-analysis/final/manifest.json` | Private full report and reproducibility provenance. |
| `runs/t03-analysis/verified-v2/manifest.json` | Hardened verification receipt. |
| `runs/t03-analysis/sanitized-v2/manifest.json` | Aggregate report with group statistics, coverage, limitations and verification. |

Older `verified/` and `sanitized/` directories may exist if the reviewed predecessor completed the pilot. Preserve them; they are not substitutes for the hardened v2 verification. Preserve versioned v2 receipts as well; select the receipt matching the current verifier code digest when inspecting the newest replay.

After successful verification, inspect only the aggregate artifact for sharing:

```bash
python -m json.tool runs/t03-analysis/sanitized-v2/manifest.json
```

Check the result for sensitive information before publishing it. Do not commit private wallet identities, scores, transfer evidence, raw Parquet data, credentials or signed source URLs. An observed positive difference can be economically interesting but still inconclusive, especially with incomplete coverage, unknown availability, left censoring or shared funding.

## 7. Engineering tests and research acceptance

Run deterministic regression tests from a clean checkout of the intended code:

```bash
uv run --locked pytest -q
git diff --check
```

Use pytest's normal temporary directories. Setting `--basetemp` inside `runs/` invalidates tests designed to exercise paths outside the private runs directory.

The hardening commit `10b343bfbd473c44cf89de473caaccc344cd3cc6` passed 90/90 regression tests. Fixture success is not real-data acceptance.

Before marking T03 complete, independently confirm all of the following:

- [ ] Historical source coverage and actual frozen proven/young/linked/control membership.
- [ ] The 30-day forward acquisition coverage, participation, denominator and per-wallet economics.
- [ ] Representative two-sided funding edges and sampled fill arithmetic.
- [ ] Fresh replay agrees on cohort membership, outcome and diagnostics; the v2 verification binds the final report and frozen checkpoint.
- [ ] A sanitized report discloses means, medians, differences, uncertainty where supported, missingness, unresolved evidence, fee limitations and left censoring.
- [ ] The report explicitly distinguishes observed source-wallet outcomes from follower execution, scientific qualification and trading authorization.

An honest inconclusive result can satisfy the research specification if supported by actual evidence. Passing code tests alone cannot close T03.
