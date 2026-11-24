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


# Known sentinel and HyperCore system addresses, not an ownership or CEX map.
# https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/hyperevm/hypercore-less-than-greater-than-hyperevm-transfers
# https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees
_SYSTEM_ADDRESSES = {"0x" + "0"*40, "0x" + "0"*36 + "dead",
                     "0x" + "22"*20, "0x" + "fe"*20}


def is_system_address(value):
    address = _normalized(value)
    if address is None:
        return False
    # 0x20 followed by zero padding and the big-endian token index (up to 64 bits).
    return address in _SYSTEM_ADDRESSES or (
        len(address) == 42 and address.startswith("0x20" + "00"*11)
        and all(c in "0123456789abcdef" for c in address[2:])
    )


def _amount(value):
    try:
        amount = Decimal(str(value))
        return amount if amount.is_finite() and amount > 0 else None
    except InvalidOperation:
        return None


def verify_transfers(path: Path | list[Path] | None, cutoff: datetime):
    """Return canonical edges, their source evidence, and rejection diagnostics."""
    counts = Counter({
        key: 0 for key in (
            "rows", "duplicate_rows", "late_known_rows", "unknown_availability_rows",
            "invalid_rows", "invalid_event_time_rows", "undated_conflicting_transactions",
            "one_sided", "ambiguous", "non_direct",
            "intermediary", "self", "incompatible", "below_minimum", "verified",
            "incomplete_window_rows",
        )
    })
    if path is None or (isinstance(path, Path) and not path.is_file()):
        return [], {}, dict(counts)
    if isinstance(path, list) and not path:
        return [], {}, dict(counts)
    scan = pl.scan_parquet(path)
    schema = scan.collect_schema()
    if missing := _REQUIRED - set(schema.names()):
        raise ValueError(f"Missing ledger columns: {sorted(missing)}")
    window_fields = {"source_window_start", "source_window_end"}
    present_window_fields = window_fields & set(schema.names())
    if present_window_fields and present_window_fields != window_fields:
        raise ValueError("Ledger source-window bounds must be supplied together")
    for field in ("event_at", "known_at", "ingested_at", *sorted(present_window_fields)):
        if not isinstance(schema[field], pl.Datetime) or schema[field].time_zone is None:
            raise ValueError(f"Ledger {field} must be timezone-aware")
    undated = scan.filter(pl.col("event_at").is_null()).select(
        "transaction_id", "known_at", *sorted(present_window_fields)
    ).collect()
    counts["invalid_event_time_rows"] = undated.height
    # A wholly pre-cutoff historical request can anchor contradictory
    # undated evidence for rejection only, never establish its event time
    # or historical availability. Later/straddling/unanchored requests
    # cannot change cutoff-frozen transfer identities.
    historical_conflict = pl.col("known_at") <= cutoff
    if present_window_fields:
        historical_conflict = historical_conflict.fill_null(False) | (
            pl.col("known_at").is_null()
            & (pl.col("source_window_start") < pl.col("source_window_end"))
            & (pl.col("source_window_end") <= cutoff)
        )
    undated_known = {
        tx for raw in undated.filter(historical_conflict)["transaction_id"]
        if (tx := _normalized(raw))
    }
    optional = ({"token", "window_complete"} & set(schema.names())) | present_window_fields
    data = scan.select(sorted(_REQUIRED | optional)).filter(
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
        if row.get("window_complete", True) is not True:
            counts["incomplete_window_rows"] += 1
            if tx:
                invalid_transactions.add(tx)
            continue
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
            _normalized(row.get("token")),
        )
        evidence = (source, row["known_at"], row["ingested_at"],
                    row.get("source_window_start"), row.get("source_window_end"))
        if key in groups[tx]:
            counts["duplicate_rows"] += 1
        groups[tx][key].add(evidence)

    undated_conflicts = undated_known.intersection(groups)
    counts["undated_conflicting_transactions"] = len(undated_conflicts)
    invalid_transactions.update(undated_conflicts)

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
        if any(side[4] is not False or is_system_address(side[0]) or
               is_system_address(side[1]) for side in sides):
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
            or ("token" in optional and outgoing[7] is None)
        ):
            counts["incompatible"] += 1
            continue
        if outgoing[6] < 100:
            counts["below_minimum"] += 1
            continue

        def observations(side):
            return [
                {"source": source, "known_at": known, "ingested_at": ingested,
                 **({"source_window_start": window_start, "source_window_end": window_end}
                    if present_window_fields else {})}
                for source, known, ingested, window_start, window_end in sorted(
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
            **({"token": outgoing[7]} if "token" in optional else {}),
        }
        counts["verified"] += 1
    return edges, evidence_by_tx, dict(counts)
