# hl-links domain glossary

This is the canonical vocabulary for independent Hyperliquid funding-link research. Study thresholds and implementation details belong in study specifications, not this glossary.

- **Wallet**: A blockchain trading address/account observed on Hyperliquid. One wallet is not necessarily one human or independent trading decision-maker.
- **Trader identity**: The unknown real-world decision-maker controlling one or more wallets. A funding link alone does not establish trader identity or common ownership.
- **Funding event**: An observed movement of monetary value with a source, destination, event time, amount and evidence provenance. A trade with a matched counterparty is not a funding event.
- **Funding edge**: A directed relationship between two wallets supported by qualifying funding-event evidence. Whether a particular event qualifies is a study decision.
- **Candidate relationship**: A possible economic association inferred from transfers or shared funders. It is weaker than established common control.
- **Proven wallet**: A wallet that satisfies a declared historical-performance and age screen at a research cutoff. The label does not establish persistent skill or future profitability.
- **Young wallet**: A wallet satisfying declared trading-age and activity criteria at a research cutoff. Age since first *observed* trading activity may be left-censored.
- **Research cutoff**: The instant at which the eligible universe, historical labels, edges and all explanatory features are frozen for forward evaluation.
- **Event time**: When an economic event occurred on-chain or at the exchange.
- **Availability time**: The earliest time the relevant information was independently observable from a source. It differs from local receipt/ingestion time.
- **Ingestion time**: When this project captured the source record. Later ingestion alone does not prove historical availability.
- **Point-in-time reconstruction**: A historical as-of analysis based on information demonstrably available by the cutoff, distinguishable from a cohort actually captured and frozen on that day.
- **Linked cohort**: Young wallets with sufficient pre-cutoff evidence for the study's defined funding relationship.
- **No observed link**: A young wallet with no qualifying funding edge in the data queried for a study. This is a practical MVP comparison label, not proof of independence or complete transfer coverage.
- **Unlinked control**: A stronger claim that a predeclared transfer-coverage standard was met without finding a qualifying relationship. It is not interchangeable with **no observed link**.
- **Unknown relationship**: Eligibility or linkage cannot be established because evidence is missing, ambiguous or unreliable. Reporting this is useful even if the MVP does not exclude every such wallet.
- **Source-wallet return**: Return or PnL measured for the observed wallet itself, without assuming its trades can be copied.
- **Follower-executable return**: Modeled or observed follower PnL under an explicit latency, execution, cost and risk policy. It must not be conflated with source-wallet return.
- **Cohort dependency**: Repeated observations of one wallet, or wallets connected to a common funder, are not statistically independent samples.

