"""Verify direct two-sided ledger evidence without inferring ownership."""

from collections import Counter, defaultdict
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

import polars as pl

from .cohorts import MoneyTransfer

_REQUIRED = {
    "transaction_id", "wallet", "counterparty", "direction", "transfer_type",
    "intermediary", "event_at", "known_at", "ingested_at", "amount_usd", "source",
}


def _normalized(value):
    return value.strip().lower() if isinstance(value, str) and value.strip() else None


def _amount(value):
    try:
        amount = Decimal(str(value))
        return amount if amount.is_finite() and amount > 0 else None
    except InvalidOperation:
        return None


def verify_transfers(path: Path | None, cutoff: datetime):
    """Return canonical edges, their source evidence, and rejection diagnostics."""
    counts = Counter({
        key: 0 for key in (
            "rows", "duplicate_rows", "late_known_rows", "unknown_availability_rows",
            "invalid_rows", "invalid_event_time_rows", "one_sided", "ambiguous", "non_direct",
            "intermediary", "self", "incompatible", "below_minimum", "verified",
        )
    })
    if path is None or not path.is_file():
        return [], {}, dict(counts)
    scan = pl.scan_parquet(path)
    schema = scan.collect_schema()
    if missing := _REQUIRED - set(schema.names()):
        raise ValueError(f"Missing ledger columns: {sorted(missing)}")
    for field in ("event_at", "known_at", "ingested_at"):
        if not isinstance(schema[field], pl.Datetime) or schema[field].time_zone is None:
            raise ValueError(f"Ledger {field} must be timezone-aware")
    counts["invalid_event_time_rows"] = scan.select(
        pl.col("event_at").is_null().sum()
    ).collect().item()
    data = scan.select(sorted(_REQUIRED)).filter(
        (pl.col("event_at") >= cutoff - timedelta(days=60))
        & (pl.col("event_at") < cutoff)
    ).collect()
    counts["rows"] = data.height
    groups = defaultdict(lambda: defaultdict(set))
    invalid_transactions = set()
    for row in data.iter_rows(named=True):
        if row["known_at"] is not None and row["known_at"] > cutoff:
            counts["late_known_rows"] += 1
            continue
        counts["unknown_availability_rows"] += row["known_at"] is None
        tx = _normalized(row["transaction_id"])
        wallet = _normalized(row["wallet"])
        source = row["source"]
        amount = _amount(row["amount_usd"])
        if not tx or not wallet or amount is None or not isinstance(source, str) or not source.strip():
            counts["invalid_rows"] += 1
            if tx:
                invalid_transactions.add(tx)
            continue
        key = (
            wallet, _normalized(row["counterparty"]), _normalized(row["direction"]),
            _normalized(row["transfer_type"]), row["intermediary"], row["event_at"], amount,
        )
        evidence = (source, row["known_at"], row["ingested_at"])
        if key in groups[tx]:
            counts["duplicate_rows"] += 1
        groups[tx][key].add(evidence)

    edges = []
    evidence_by_tx = {}
    for tx in sorted(set(groups) | invalid_transactions):
        sides = groups[tx]
        if tx in invalid_transactions or len(sides) > 2:
            counts["ambiguous"] += 1
            continue
        if len(sides) != 2:
            counts["one_sided"] += 1
            continue
        if any(side[3] != "send" for side in sides):
            counts["non_direct"] += 1
            continue
        if any(side[4] is not False for side in sides):
            counts["intermediary"] += 1
            continue
        by_direction = {side[2]: side for side in sides}
        if set(by_direction) != {"out", "in"}:
            counts["ambiguous"] += 1
            continue
        outgoing, incoming = by_direction["out"], by_direction["in"]
        sender, recipient = outgoing[0], incoming[0]
        if sender == recipient:
            counts["self"] += 1
            continue
        if (
            outgoing[1] not in (None, recipient)
            or incoming[1] not in (None, sender)
            or outgoing[5:] != incoming[5:]
        ):
            counts["incompatible"] += 1
            continue
        if outgoing[6] < 100:
            counts["below_minimum"] += 1
            continue

        def observations(side):
            return [
                {"source": source, "known_at": known, "ingested_at": ingested}
                for source, known, ingested in sorted(
                    sides[side], key=lambda item: tuple(str(value) for value in item)
                )
            ]

        outgoing_evidence, incoming_evidence = observations(outgoing), observations(incoming)
        earliest = [
            min((item["known_at"] for item in entries if item["known_at"] is not None),
                default=None)
            for entries in (outgoing_evidence, incoming_evidence)
        ]
        known = max(earliest) if all(value is not None for value in earliest) else None
        edges.append(MoneyTransfer(
            sender, recipient, outgoing[5], known, outgoing[6], transaction_id=tx
        ))
        evidence_by_tx[tx] = {
            "transaction_id": tx, "sender": sender, "recipient": recipient,
            "event_at": outgoing[5], "amount_usd": str(outgoing[6]),
            "known_at": known, "transfer_type": "send",
            "sender_evidence": outgoing_evidence, "recipient_evidence": incoming_evidence,
            "sender_reported_counterparty": outgoing[1],
            "recipient_reported_counterparty": incoming[1],
        }
        counts["verified"] += 1
    return edges, evidence_by_tx, dict(counts)
