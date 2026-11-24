from datetime import datetime, timedelta, timezone

import pytest

from hl_links.cohorts import MoneyTransfer, WalletRecord, eligible_proven, eligible_young, make_cohorts


T = datetime(2026, 10, 23, tzinfo=timezone.utc)


def wallet(address: str, age_days: int, pnl: float = 0, recent_days: int = 1, known_days: int = 0) -> WalletRecord:
    return WalletRecord(
        address=address,
        first_trade_at=T - timedelta(days=age_days),
        last_trade_at=T - timedelta(days=recent_days),
        net_realized_pnl_usd=pnl,
        known_at=T + timedelta(days=known_days),
    )


def transfer(sender: str, recipient: str, days_ago: int = 1, known_days: int = -1, amount: float = 300, **kwargs) -> MoneyTransfer:
    return MoneyTransfer(
        sender=sender,
        recipient=recipient,
        amount_usd=amount,
        event_at=T - timedelta(days=days_ago),
        known_at=T + timedelta(days=known_days),
        **kwargs,
    )


def test_age_and_activity_boundaries():
    assert eligible_proven(wallet("p", 60, pnl=20_000), T)
    assert not eligible_proven(wallet("p", 59, pnl=20_000), T)
    assert not eligible_proven(wallet("p", 90, pnl=19_999.99), T)
    assert eligible_young(wallet("y", 7, recent_days=7), T)
    assert eligible_young(wallet("y", 59), T)
    assert not eligible_young(wallet("y", 60), T)
    assert not eligible_young(wallet("y", 20, recent_days=8), T)
    assert not eligible_young(wallet("y", 20, recent_days=0), T)
    assert not eligible_proven(wallet("p", 90, pnl=20_000, recent_days=0), T)


def test_funding_links_both_directions_and_case_insensitive_addresses():
    group = make_cohorts(
        [wallet("PRO", 90, 20_000), wallet("Young1", 20), wallet("young2", 40), wallet("control", 25)],
        [transfer("PRO", "young1"), transfer("YOUNG2", "pro")],
        cutoff=T,
    )
    assert group.proven == frozenset({"pro"})
    assert group.linked == frozenset({"young1", "young2"})
    assert group.unlinked == frozenset({"control"})


def test_future_known_wallet_and_transfer_do_not_leak():
    group = make_cohorts(
        [wallet("proven", 70, 20_000), wallet("young", 14), wallet("future_winner", 70, 25_000, known_days=1)],
        [
            transfer("proven", "young", known_days=1),
            transfer("future_winner", "young"),
            transfer("proven", "young", days_ago=-1, known_days=-1),
        ],
        cutoff=T,
    )
    assert group.proven == frozenset({"proven"})
    assert group.linked == frozenset()
    assert group.unlinked == frozenset({"young"})


def test_ambiguous_intermediary_edge_is_not_evidence():
    group = make_cohorts(
        [wallet("pro", 90, 20_000), wallet("young", 20)],
        [transfer("pro", "young", intermediary=True), transfer("pro", "young", direct=False)],
        cutoff=T,
    )
    assert group.linked == frozenset()
    assert group.unlinked == frozenset({"young"})


def test_transfer_amount_and_window_boundaries():
    group = make_cohorts(
        [wallet("p", 90, 20_000), *(wallet(name, 20) for name in ("start", "small", "old", "end"))],
        [
            transfer("p", "start", days_ago=60, amount=100),
            transfer("p", "small", amount=99.99),
            transfer("p", "old", days_ago=61),
            transfer("p", "end", days_ago=0),
        ],
        cutoff=T,
    )
    assert group.linked == frozenset({"start"})
    assert group.unlinked == frozenset({"small", "old", "end"})


def test_naive_timestamps_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        eligible_proven(wallet("p", 70, 10_000), datetime(2026, 10, 23))
