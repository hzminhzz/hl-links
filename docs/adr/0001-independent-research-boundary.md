# ADR 0001 — Independent research boundary

- **Status:** Accepted
- **Date:** 2026-11-23

## Context

The funding-link hypothesis needs historical joins and exploratory analysis. The existing `trading-ops`, `copytrade-pm` and `prop-production` projects serve different ongoing purposes. Integrating an unvalidated hypothesis into any of them would create release coupling and risk inappropriate promotion of research outputs into trading decisions.

## Decision

Maintain `hl-links` as a separate, research-only project. It may consume independently permitted, read-only external evidence and verified historical data from existing stores, but must not write to or alter existing trading systems or authorize trading. Findings require an explicit downstream review before any operational integration.

## Alternatives rejected

- **Module inside `trading-ops`:** fewer repositories but couples exploratory work to established portfolio workflows.
- **Module inside `copytrade-pm`:** easy access to wallet data but conflates research with source authority and operational state.
- **Standalone networked service or graph database:** no demonstrated query or scale need; premature operational cost.

## Consequences

- Research artifacts and failures remain isolated and auditable.
- Some interfaces to shared data need clear read-only contracts.
- No obligation to build a service, dashboard, duplicate data warehouse or deployment pipeline for the initial experiment.

Further choices about specific data sources, graph definition and PnL estimator are **not** settled by this ADR.
