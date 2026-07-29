from dataclasses import replace
from decimal import Decimal

from python_swarm.domain_models import (
    BrokerEvent,
    BrokerEventType,
    BrokerPosition,
    Direction,
    LifecycleActionKind,
    LifecycleStage,
    Milestone,
    QuoteSnapshot,
    SymbolSnapshot,
)
from python_swarm.trading_policies import LifecyclePolicy


NOW = 3_000_000


def symbol(minimum="0.01", step="0.01"):
    return SymbolSnapshot(
        symbol="XAUUSD",
        digits=2,
        tick_size="0.01",
        tick_value_per_lot="1",
        contract_size="100",
        volume_min=minimum,
        volume_step=step,
        volume_max="5",
    )


def position(direction=Direction.BUY, volume="0.04", stage=LifecycleStage.PRE_TP1):
    buy = direction == Direction.BUY
    return BrokerPosition(
        trade_id="trade-1",
        broker_position_id="90071992547409931234",
        position_ticket="12345",
        symbol="XAUUSD",
        magic=202412,
        direction=direction,
        initial_volume=volume,
        remaining_volume=volume,
        entry_price="100",
        original_sl="90" if buy else "110",
        current_sl="90" if buy else "110",
        tp1="110" if buy else "90",
        tp2="120" if buy else "80",
        tp3="130" if buy else "70",
        initial_risk_amount="40",
        lifecycle_stage=stage,
        position_version=1,
        opened_at_ms=NOW - 1000,
    )


def quote(bid, ask):
    return QuoteSnapshot("XAUUSD", bid, ask, NOW)


def event(event_type, command_id, version, **kwargs):
    return BrokerEvent(
        event_id=f"event-{version}",
        event_type=event_type,
        broker_time_ms=NOW,
        received_at_ms=NOW,
        command_id=command_id,
        trade_id="trade-1",
        broker_position_id="90071992547409931234",
        position_version=version,
        **kwargs,
    )


def test_tp1_is_half_initial_volume_and_state_waits_for_broker_confirmation():
    policy = LifecyclePolicy()
    original = position()
    decision = policy.plan(original, quote("110", "110.02"), symbol(), "cmd-tp1")
    assert decision.action.kind == LifecycleActionKind.PARTIAL_CLOSE
    assert decision.action.close_volume == Decimal("0.02")
    assert decision.position.remaining_volume == original.remaining_volume
    assert decision.position.lifecycle_stage == LifecycleStage.PRE_TP1
    assert decision.position.pending_command_id == "cmd-tp1"
    assert decision.position.pending_close_volume == Decimal("0.02")
    assert decision.position.pending_filled_volume == 0

    confirmed = policy.apply_broker_event(
        decision.position,
        event(
            BrokerEventType.PARTIAL_FILLED,
            "cmd-tp1",
            2,
            fill_volume="0.02",
            remaining_volume="0.02",
            profit="20",
            commission="-1",
        ),
    )
    assert confirmed.applied
    assert confirmed.position.lifecycle_stage == LifecycleStage.TP1_CONFIRMED
    assert confirmed.position.remaining_volume == Decimal("0.02")
    assert confirmed.position.realised_pnl == Decimal("19")
    assert confirmed.position.pending_close_volume == 0


def test_break_even_is_default_only_after_tp1_partial_confirmation():
    policy = LifecyclePolicy()
    before_tp1 = policy.plan(position(), quote("108", "108.02"), symbol(), "cmd-early")
    assert before_tp1.action is None

    after_tp1 = replace(
        position(volume="0.04", stage=LifecycleStage.TP1_CONFIRMED),
        remaining_volume=Decimal("0.02"),
    )
    decision = policy.plan(after_tp1, quote("111", "111.02"), symbol(), "cmd-be")
    assert decision.action.kind == LifecycleActionKind.MODIFY_SL
    assert decision.action.milestone == Milestone.BREAK_EVEN
    assert decision.action.new_sl == Decimal("100.03")

    # A retrace below the proposed BE stop must not generate an invalid modify.
    retraced = policy.plan(after_tp1, quote("99", "99.02"), symbol(), "cmd-retrace")
    assert retraced.action is None


def test_tp2_is_twenty_five_percent_of_initial_after_be_is_confirmed():
    policy = LifecyclePolicy()
    after_tp1 = replace(
        position(stage=LifecycleStage.TP1_CONFIRMED),
        remaining_volume=Decimal("0.02"),
        current_sl=Decimal("100.03"),
    )
    decision = policy.plan(after_tp1, quote("120", "120.02"), symbol(), "cmd-tp2")
    assert decision.action.kind == LifecycleActionKind.PARTIAL_CLOSE
    assert decision.action.milestone == Milestone.TP2
    assert decision.action.close_volume == Decimal("0.01")


def test_gap_through_tp3_has_priority_and_closes_all_remaining_once():
    policy = LifecyclePolicy()
    pos = position()
    decision = policy.plan(pos, quote("131", "131.02"), symbol(), "cmd-final")
    assert decision.action.kind == LifecycleActionKind.CLOSE
    assert decision.action.milestone == Milestone.TP3
    assert decision.action.close_volume == pos.remaining_volume


def test_one_in_flight_action_blocks_duplicate_planning():
    policy = LifecyclePolicy()
    pending = policy.plan(
        position(), quote("110", "110.02"), symbol(), "cmd-one"
    ).position
    duplicate = policy.plan(pending, quote("115", "115.02"), symbol(), "cmd-two")
    assert duplicate.action is None
    assert duplicate.reason == "ACTION_ALREADY_IN_FLIGHT"


def test_buy_uses_bid_and_sell_uses_ask_for_target_detection():
    policy = LifecyclePolicy()
    buy = policy.plan(position(), quote("109.99", "110.01"), symbol(), "cmd-buy")
    assert buy.action is None

    sell = policy.plan(
        position(Direction.SELL), quote("89.99", "90.01"), symbol(), "cmd-sell"
    )
    assert sell.action is None


def test_small_position_uses_virtual_milestone_without_claiming_a_fill():
    policy = LifecyclePolicy()
    pos = position(volume="0.01")
    decision = policy.plan(pos, quote("110", "110.02"), symbol(), "cmd-dust")
    assert decision.action.kind == LifecycleActionKind.VIRTUAL_MILESTONE
    assert decision.action.command_id is None
    assert decision.position.remaining_volume == Decimal("0.01")
    assert decision.position.lifecycle_stage == LifecycleStage.TP1_CONFIRMED
    assert decision.position.pending_action is None


def test_rejected_partial_clears_pending_without_changing_volume_or_stage():
    policy = LifecyclePolicy()
    pending = policy.plan(
        position(), quote("110", "110.02"), symbol(), "cmd-reject"
    ).position
    transitioned = policy.apply_broker_event(
        pending,
        event(BrokerEventType.COMMAND_REJECTED, "cmd-reject", 1, reason="BROKER_REJECT"),
    )
    assert transitioned.applied
    assert transitioned.position.remaining_volume == Decimal("0.04")
    assert transitioned.position.lifecycle_stage == LifecycleStage.PRE_TP1
    assert transitioned.position.pending_action is None
    assert transitioned.position.position_version == 1

    version_gap = policy.apply_broker_event(
        pending,
        event(BrokerEventType.COMMAND_REJECTED, "cmd-reject", 2),
    )
    assert not version_gap.applied
    assert version_gap.reason == "REJECTION_POSITION_VERSION_MISMATCH"
    assert version_gap.position.pending_command_id == "cmd-reject"


def test_close_state_changes_only_on_broker_close_event():
    policy = LifecyclePolicy()
    pending = policy.plan(
        position(), quote("131", "131.02"), symbol(), "cmd-close"
    ).position
    assert pending.is_open
    closed = policy.apply_broker_event(
        pending,
        event(
            BrokerEventType.POSITION_CLOSED,
            "cmd-close",
            2,
            remaining_volume="0",
            profit="30",
            commission="-1",
        ),
    )
    assert closed.applied
    assert not closed.position.is_open
    assert closed.position.lifecycle_stage == LifecycleStage.CLOSED
    assert closed.position.realised_pnl == Decimal("29")
    assert closed.position.realised_r == Decimal("0.725")


def test_broker_events_require_exact_trade_and_position_identity():
    policy = LifecyclePolicy()
    pending = policy.plan(
        position(), quote("131", "131.02"), symbol(), "cmd-close"
    ).position
    valid = event(
        BrokerEventType.POSITION_CLOSED,
        "cmd-close",
        2,
        remaining_volume="0",
    )

    wrong_trade = policy.apply_broker_event(
        pending, replace(valid, trade_id="other-trade")
    )
    missing_position = policy.apply_broker_event(
        pending, replace(valid, broker_position_id=None)
    )

    assert not wrong_trade.applied
    assert wrong_trade.reason == "TRADE_ID_MISMATCH"
    assert not missing_position.applied
    assert missing_position.reason == "POSITION_ID_MISMATCH"
    assert pending.is_open


def test_partial_command_accumulates_multiple_broker_deals_before_advancing():
    policy = LifecyclePolicy()
    pending = policy.plan(
        position(), quote("110", "110.02"), symbol(), "cmd-multi"
    ).position

    first = policy.apply_broker_event(
        pending,
        event(
            BrokerEventType.PARTIAL_FILLED,
            "cmd-multi",
            2,
            fill_volume="0.01",
            remaining_volume="0.03",
            profit="5",
            commission="-0.5",
        ),
    )
    assert first.applied
    assert first.reason == "PARTIAL_FILL_PROGRESS_CONFIRMED"
    assert first.position.lifecycle_stage == LifecycleStage.PRE_TP1
    assert first.position.pending_command_id == "cmd-multi"
    assert first.position.pending_close_volume == Decimal("0.02")
    assert first.position.pending_filled_volume == Decimal("0.01")
    assert first.position.remaining_volume == Decimal("0.03")

    second = policy.apply_broker_event(
        first.position,
        event(
            BrokerEventType.PARTIAL_FILLED,
            "cmd-multi",
            3,
            fill_volume="0.01",
            remaining_volume="0.02",
            profit="6",
            commission="-0.5",
        ),
    )
    assert second.applied
    assert second.reason == "PARTIAL_FILL_CONFIRMED"
    assert second.position.lifecycle_stage == LifecycleStage.TP1_CONFIRMED
    assert second.position.pending_action is None
    assert second.position.pending_filled_volume == 0
    assert second.position.realised_pnl == Decimal("10")


def test_partial_overfill_applies_broker_truth_and_quarantines_position():
    policy = LifecyclePolicy()
    pending = policy.plan(
        position(), quote("110", "110.02"), symbol(), "cmd-overfill"
    ).position
    transitioned = policy.apply_broker_event(
        pending,
        event(
            BrokerEventType.PARTIAL_FILLED,
            "cmd-overfill",
            2,
            fill_volume="0.03",
            remaining_volume="0.01",
        ),
    )
    assert transitioned.applied
    assert transitioned.reason == "PARTIAL_OVERFILL_QUARANTINED"
    assert transitioned.position.remaining_volume == Decimal("0.01")
    assert transitioned.position.lifecycle_stage == LifecycleStage.QUARANTINED
    assert transitioned.position.pending_action is None


def test_hard_48_h1_bar_timeout_closes_all_remaining_volume():
    policy = LifecyclePolicy()
    timed = policy.plan(
        position(),
        quote("105", "105.02"),
        symbol(),
        "cmd-timeout",
        h1_bars_elapsed=48,
    )
    assert timed.action.kind == LifecycleActionKind.CLOSE
    assert timed.action.milestone == Milestone.TIMEOUT
    assert timed.action.close_volume == Decimal("0.04")
    assert timed.position.pending_close_volume == Decimal("0.04")


def test_session_close_only_exits_an_unadvanced_trade():
    policy = LifecyclePolicy()
    unadvanced = policy.plan(
        position(),
        quote("105", "105.02"),
        symbol(),
        "cmd-session",
        session_close_due=True,
    )
    assert unadvanced.action.kind == LifecycleActionKind.CLOSE
    assert unadvanced.action.milestone == Milestone.SESSION_CLOSE

    advanced = replace(
        position(stage=LifecycleStage.TP1_CONFIRMED),
        remaining_volume=Decimal("0.02"),
        current_sl=Decimal("100.03"),
    )
    held = policy.plan(
        advanced,
        quote("105", "105.02"),
        symbol(),
        "cmd-session-advanced",
        session_close_due=True,
    )
    assert held.action is None


def test_modify_sl_uses_stricter_freeze_level_distance():
    policy = LifecyclePolicy()
    frozen_symbol = replace(symbol(), freeze_level_points=200)
    advanced = replace(
        position(stage=LifecycleStage.TP1_CONFIRMED),
        remaining_volume=Decimal("0.02"),
    )
    # Proposed 100.03 is only 0.97 below bid and therefore inside a 2.00 freeze.
    decision = policy.plan(
        advanced, quote("101", "101.02"), frozen_symbol, "cmd-frozen"
    )
    assert decision.action is None


def test_trailing_requires_an_explicit_closed_bar_structural_candidate():
    policy = LifecyclePolicy()
    advanced = replace(
        position(stage=LifecycleStage.TP2_CONFIRMED),
        remaining_volume=Decimal("0.01"),
        current_sl=Decimal("100.03"),
    )

    absent = policy.plan(
        advanced, quote("125", "125.02"), symbol(), "cmd-no-structure"
    )
    assert absent.action is None

    planned = policy.plan(
        advanced,
        quote("125", "125.02"),
        symbol(),
        "cmd-structural",
        structural_trailing_sl="114.90",
    )
    assert planned.action.kind == LifecycleActionKind.MODIFY_SL
    assert planned.action.milestone == Milestone.TRAILING
    assert planned.action.reason == "STRUCTURAL_TRAILING_IMPROVEMENT"
    assert planned.action.new_sl == Decimal("114.90")


def test_sell_structural_trail_must_improve_and_remain_above_the_ask():
    policy = LifecyclePolicy()
    advanced = replace(
        position(Direction.SELL, stage=LifecycleStage.TP2_CONFIRMED),
        remaining_volume=Decimal("0.01"),
        current_sl=Decimal("99.97"),
    )
    planned = policy.plan(
        advanced,
        quote("75", "75.02"),
        symbol(),
        "cmd-sell-structural",
        structural_trailing_sl="86.10",
    )
    assert planned.action.kind == LifecycleActionKind.MODIFY_SL
    assert planned.action.new_sl == Decimal("86.10")

    inside_market = policy.plan(
        advanced,
        quote("75", "75.02"),
        symbol(),
        "cmd-sell-invalid",
        structural_trailing_sl="75.01",
    )
    assert inside_market.action is None
