from dataclasses import replace
from decimal import Decimal

import pytest

from python_swarm.domain_models import (
    AccountMode,
    AccountSnapshot,
    BrokerPosition,
    BrokerSnapshot,
    ContributorAttribution,
    Direction,
    EntryLevels,
    FixedClock,
    LifecycleStage,
    QuoteSnapshot,
    ReservationState,
    RiskReservation,
    SymbolSnapshot,
    TradeIntent,
)
from python_swarm.trading_policies import (
    EntryPolicy,
    EntryPolicyConfig,
    RiskPolicy,
    RiskPolicyConfig,
)


NOW = 2_000_000


def make_intent(requested_risk="1", *, direction=Direction.BUY):
    levels = (
        EntryLevels("100", "99", "101", "102", "103")
        if direction == Direction.BUY
        else EntryLevels("100", "101", "99", "98", "97")
    )
    return TradeIntent(
        intent_id="intent-1",
        trade_id="trade-1",
        fusion_id="fusion-1",
        symbol="XAUUSD",
        magic=202412,
        direction=direction,
        levels=levels,
        reference_price="100",
        max_slippage_points=10,
        requested_risk_pct=requested_risk,
        contributors=(),
        regime="TRENDING",
        session="LONDON",
        created_at_ms=NOW - 100,
        expires_at_ms=NOW + 10_000,
        config_hash="config-v1",
    )


def make_symbol(step="0.001", minimum="0.001"):
    return SymbolSnapshot(
        symbol="XAUUSD",
        digits=2,
        tick_size="0.01",
        tick_value_per_lot="1",
        contract_size="100",
        volume_min=minimum,
        volume_step=step,
        volume_max="5",
        stops_level_points=10,
        margin_per_lot="100",
    )


def make_broker(*, equity="1000", balance="1000", baseline="1000", **changes):
    account = AccountSnapshot(
        account_id="acct-1",
        currency="USD",
        mode=AccountMode.HEDGING,
        balance=balance,
        equity=equity,
        free_margin="1000",
        risk_day="2026-07-15",
        day_start_equity=baseline,
        baseline_verified=changes.pop("baseline_verified", True),
        daily_halted=changes.pop("daily_halted", False),
        captured_at_ms=NOW,
    )
    return BrokerSnapshot(
        snapshot_id="broker-snapshot-1",
        connection_id="connection-1",
        account=account,
        symbol=make_symbol(),
        quote=QuoteSnapshot("XAUUSD", "99.99", "100", NOW),
        positions=changes.pop("positions", ()),
        captured_at_ms=NOW,
        reconciled=changes.pop("reconciled", True),
        foreign_symbol_exposure=changes.pop("foreign_symbol_exposure", False),
        **changes,
    )


def risk_policy(**config):
    return RiskPolicy(RiskPolicyConfig(**config), clock=FixedClock(NOW))


def test_over_cap_requested_risk_is_rejected_not_silently_clamped():
    decision = risk_policy().assess(
        make_intent("5"), make_broker(), "reservation-1", modifiers=("1.5", "2")
    )
    assert not decision.approved
    assert decision.reason == "REQUESTED_RISK_EXCEEDS_HARD_CAP"


def test_growth_modifiers_are_clamped_and_cannot_increase_valid_risk():
    decision = risk_policy().assess(
        make_intent("1"), make_broker(), "reservation-1", modifiers=("1.5", "2")
    )
    assert decision.approved
    assert decision.risk_pct <= Decimal("1")
    assert decision.risk_amount <= Decimal("10")


def test_reducing_modifier_reduces_volume_and_risk():
    full = risk_policy().assess(make_intent(), make_broker(), "reservation-full")
    half = risk_policy().assess(
        make_intent(), make_broker(), "reservation-half", modifiers=(Decimal("0.5"),)
    )
    assert full.approved and half.approved
    assert half.risk_amount == full.risk_amount / 2
    assert half.volume == full.volume / 2


def test_volume_is_rounded_down_to_broker_step():
    # $10 budget / $100.5 risk per lot produces 0.0995..., then floors to 0.001.
    broker = replace(
        make_broker(),
        quote=QuoteSnapshot("XAUUSD", "99.99", "100.005", NOW),
    )
    intent = replace(make_intent(), reference_price=Decimal("100.005"))
    decision = risk_policy().assess(intent, broker, "reservation-step")
    assert decision.approved
    assert decision.volume == Decimal("0.099")
    assert decision.risk_amount <= Decimal("10")


def test_pending_unknown_reservation_consumes_the_position_slot_even_if_expired():
    pending = RiskReservation(
        reservation_id="old-reservation",
        trade_id="old-trade",
        account_id="acct-1",
        direction=Direction.BUY,
        risk_day="2026-07-15",
        risk_amount="5",
        volume="0.05",
        state=ReservationState.UNKNOWN,
        created_at_ms=1,
        expires_at_ms=2,
    )
    decision = risk_policy().assess(
        make_intent(), make_broker(), "new-reservation", reservations=(pending,)
    )
    assert not decision.approved
    assert decision.reason == "MAX_POSITIONS_OR_RESERVATIONS"


def test_unverified_bootstrap_reconciliation_and_daily_halt_fail_closed():
    cases = (
        (make_broker(baseline_verified=False), "RISK_BASELINE_UNVERIFIED"),
        (make_broker(reconciled=False), "BROKER_NOT_RECONCILED"),
        (make_broker(daily_halted=True), "DAILY_HALT_ACTIVE"),
        (make_broker(equity="969", baseline="1000"), "DAILY_DRAWDOWN_LIMIT"),
    )
    for broker, expected in cases:
        decision = risk_policy().assess(make_intent(), broker, f"res-{expected}")
        assert not decision.approved
        assert decision.reason == expected


def test_stale_account_snapshot_fails_closed():
    stale = make_broker()
    stale = replace(stale, account=replace(stale.account, captured_at_ms=NOW - 6_000))
    decision = risk_policy().assess(make_intent(), stale, "reservation-stale")
    assert not decision.approved
    assert decision.reason == "STALE_OR_FUTURE_BROKER_SNAPSHOT"


def test_minimum_volume_is_rejected_when_it_would_exceed_budget():
    broker = replace(make_broker(), symbol=make_symbol(step="0.1", minimum="0.1"))
    decision = risk_policy().assess(
        make_intent(), broker, "reservation-too-small", modifiers=("0.5",)
    )
    assert not decision.approved
    assert decision.reason == "MIN_VOLUME_EXCEEDS_RISK_BUDGET"


def test_foreign_symbol_exposure_blocks_a_new_position():
    decision = risk_policy().assess(
        make_intent(),
        make_broker(foreign_symbol_exposure=True),
        "reservation-foreign",
    )
    assert not decision.approved
    assert decision.reason == "FOREIGN_SYMBOL_EXPOSURE"


def test_entry_policy_enforces_spread_freeze_distance_and_reward_to_risk():
    broker = make_broker()

    spread = EntryPolicy(
        EntryPolicyConfig(max_spread_points=Decimal("0.5")),
        clock=FixedClock(NOW),
    ).validate_for_broker(make_intent(), broker)
    assert not spread.valid
    assert spread.reason == "MAX_SPREAD_EXCEEDED"

    frozen = replace(
        broker,
        symbol=replace(broker.symbol, freeze_level_points=200),
    )
    freeze = EntryPolicy(clock=FixedClock(NOW)).validate_for_broker(
        make_intent(), frozen
    )
    assert not freeze.valid
    assert freeze.reason == "BROKER_STOPS_LEVEL_VIOLATION"

    poor_levels = EntryLevels("100", "99", "100.4", "101.1", "101.6")
    poor_reward = EntryPolicy(clock=FixedClock(NOW)).validate_for_broker(
        replace(make_intent(), levels=poor_levels), broker
    )
    assert not poor_reward.valid
    assert poor_reward.reason == "MIN_REWARD_TO_RISK_VIOLATION"


def test_risk_configuration_cannot_exceed_system_portfolio_ceilings():
    with pytest.raises(ValueError, match="drawdown"):
        RiskPolicyConfig(max_daily_drawdown_pct=Decimal("3.01"))
    with pytest.raises(ValueError, match="portfolio"):
        RiskPolicyConfig(max_total_risk_pct=Decimal("3.01"))
    with pytest.raises(ValueError, match="1-3"):
        RiskPolicyConfig(max_open_positions=4)
