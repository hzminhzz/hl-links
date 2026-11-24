"""Cutoff-safe screening for a wallet funding-link hypothesis.

This module intentionally does not query live exchanges or calculate returns.
Consumers must supply archival records with verifiable availability timestamps.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable


@dataclass(frozen=True, slots=True)
class WalletRecord:
    address: str
    first_trade_at: datetime
    last_trade_at: datetime
    net_realized_pnl_usd: float
    known_at: datetime


@dataclass(frozen=True, slots=True)
class MoneyTransfer:
    sender: str
    recipient: str
    event_at: datetime
    known_at: datetime
    amount_usd: float
    direct: bool = True
    intermediary: bool = False


@dataclass(frozen=True, slots=True)
class Cohorts:
    proven: frozenset[str]
    linked: frozenset[str]
    unlinked: frozenset[str]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("All timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _address(value: str) -> str:
    normalized = value.strip().lower()
    if not normalized:
        raise ValueError("Wallet address cannot be empty")
    return normalized


def eligible_proven(record: WalletRecord, cutoff: datetime) -> bool:
    """Proven trader: 60+ day trading history and >= $5K pre-cutoff net PnL."""
    t = _utc(cutoff)
    first = _utc(record.first_trade_at)
    last = _utc(record.last_trade_at)
    known = _utc(record.known_at)
    return (
        known <= t
        and first <= last <= t
        and t - first >= timedelta(days=60)
        and record.net_realized_pnl_usd >= 5_000
    )


def eligible_young(record: WalletRecord, cutoff: datetime) -> bool:
    """Young trader: 7 <= age < 60 days and traded during prior seven days."""
    t = _utc(cutoff)
    first = _utc(record.first_trade_at)
    last = _utc(record.last_trade_at)
    known = _utc(record.known_at)
    age = t - first
    return (
        known <= t
        and first <= last <= t
        and timedelta(days=7) <= age < timedelta(days=60)
        and t - last <= timedelta(days=7)
    )


def make_cohorts(
    wallets: Iterable[WalletRecord],
    transfers: Iterable[MoneyTransfer],
    *,
    cutoff: datetime,
) -> Cohorts:
    """Group wallets by a direct, known-at-cutoff transfer to a proven wallet.

    Transfer direction is retained by data producers but either direction qualifies
    for this first study. Ambiguous or intermediary edges are excluded.
    """
    t = _utc(cutoff)
    records = list(wallets)
    proven = {_address(r.address) for r in records if eligible_proven(r, t)}
    young = {_address(r.address) for r in records if eligible_young(r, t)}
    linked: set[str] = set()

    for transfer in transfers:
        if transfer.intermediary or not transfer.direct or transfer.amount_usd <= 0:
            continue
        if _utc(transfer.event_at) > t or _utc(transfer.known_at) > t:
            continue
        sender = _address(transfer.sender)
        recipient = _address(transfer.recipient)
        if sender in proven and recipient in young:
            linked.add(recipient)
        if recipient in proven and sender in young:
            linked.add(sender)

    return Cohorts(frozenset(proven), frozenset(linked), frozenset(young - linked))
