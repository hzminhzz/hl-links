# Agent operating rules

This repository is independent research, not a trading service.

1. Work only inside `/home/quant/dev/hl-links` for all writes, experiments and output. Do not modify sibling repositories, systemd services, order routing, or production databases.
2. Other repositories and data mounts may be inspected **read-only** only if explicitly relevant and permitted. Never mutate source data or use credentials for execution.
3. Use point-in-time timestamps: record source event time, the first time the data was observable, ingestion time and cutoff. Exclude data that was not observable at cutoff.
4. Construct historical wallet populations from historical records; do not prefilter with today's wallet registry, balances or leaderboard.
5. Distinguish proven direct funding transfers from shared-funder inference, internal/subaccount movements, bridge/CEX edges, and ambiguous transfers. Never equate an on-chain link with common ownership.
6. Keep proposed study rules, exclusions and cost assumptions fixed before reading forward labels. Report confidence intervals, group sizes, failure modes, missing data and negative findings.
7. Execution backtests must be follower-executable, account for latency, slippage, spread, fees, funding and risk constraints. Research evidence cannot authorize live trading.
8. Use Python 3.14, uv, Polars, DuckDB, Parquet. Add dependencies only as needed. Keep tests deterministic and record experiment provenance.
9. Preserve dirty/untracked work. Review status and diff before committing. Do not deploy, trade or push without authorization.
