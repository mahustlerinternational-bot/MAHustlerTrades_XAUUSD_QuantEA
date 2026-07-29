from decimal import Decimal

from python_swarm.domain_models import (
    Direction,
    EntryLevels,
    FixedClock,
    SignalContribution,
)
from python_swarm.trading_policies import (
    EntryPolicy,
    FusionPolicy,
    FusionPolicyConfig,
)


NOW = 1_000_000


def signal(
    agent_id,
    direction,
    conviction="80",
    *,
    snapshot="snap-1",
    regime="ranging_wide",
    session="LONDON",
    levels=None,
):
    if levels is None:
        levels = (
            EntryLevels("100", "90", "110", "120", "130")
            if direction == Direction.BUY
            else EntryLevels("100", "110", "90", "80", "70")
        )
    return SignalContribution(
        signal_id=f"signal-{agent_id}-{direction.value}",
        snapshot_id=snapshot,
        agent_id=agent_id,
        direction=direction,
        conviction=conviction,
        levels=levels,
        observed_at_ms=NOW - 100,
        regime=regime,
        session=session,
        strategy_tags=(f"A{agent_id}",),
    )


def policy(**kwargs):
    return FusionPolicy(
        FusionPolicyConfig(**kwargs),
        clock=FixedClock(NOW),
    )


def test_three_distinct_positive_buy_signals_pass():
    decision = policy().evaluate(
        [signal(4, Direction.BUY), signal(5, Direction.BUY), signal(6, Direction.BUY)],
        "fusion-1",
    )
    assert decision.accepted
    assert decision.result.direction == Direction.BUY
    assert decision.result.confluence_pct == Decimal("80")
    assert [item.agent_id for item in decision.result.contributors] == [4, 5, 6]


def test_three_positive_sell_convictions_pass_symmetrically():
    decision = policy().evaluate(
        [signal(4, Direction.SELL), signal(5, Direction.SELL), signal(6, Direction.SELL)],
        "fusion-sell",
    )
    assert decision.accepted
    assert decision.result.direction == Direction.SELL
    assert decision.result.confluence_pct == Decimal("80")


def test_two_agents_and_duplicate_agent_cannot_satisfy_quorum():
    two = policy().evaluate(
        [signal(4, Direction.BUY), signal(5, Direction.BUY)], "fusion-two"
    )
    assert not two.accepted
    assert two.reason == "INSUFFICIENT_ELIGIBLE_SIGNALS"

    duplicate = policy().evaluate(
        [
            signal(4, Direction.BUY),
            signal(4, Direction.BUY),
            signal(5, Direction.BUY),
        ],
        "fusion-duplicate",
    )
    assert not duplicate.accepted
    assert duplicate.reason == "DUPLICATE_AGENT_SIGNAL"


def test_zero_conviction_is_an_abstention_not_a_third_vote():
    decision = policy().evaluate(
        [
            signal(4, Direction.BUY, "90"),
            signal(5, Direction.BUY, "90"),
            signal(6, Direction.BUY, "0"),
        ],
        "fusion-zero-abstention",
    )
    assert not decision.accepted
    assert decision.reason == "INSUFFICIENT_ELIGIBLE_SIGNALS"


def test_same_snapshot_id_cannot_hide_conflicting_regime_or_session_context():
    mixed_regime = policy().evaluate(
        [
            signal(4, Direction.BUY),
            signal(5, Direction.BUY),
            signal(6, Direction.BUY, regime="trending_bull"),
        ],
        "fusion-mixed-regime",
    )
    assert not mixed_regime.accepted
    assert mixed_regime.reason == "MIXED_SIGNAL_CONTEXT"

    mixed_session = policy().evaluate(
        [
            signal(4, Direction.BUY),
            signal(5, Direction.BUY),
            signal(6, Direction.BUY, session="NEW_YORK"),
        ],
        "fusion-mixed-session",
    )
    assert not mixed_session.accepted
    assert mixed_session.reason == "MIXED_SIGNAL_CONTEXT"


def test_tie_and_weak_directional_dominance_are_rejected():
    tied = [signal(i, Direction.BUY) for i in (4, 5, 6)] + [
        signal(i, Direction.SELL) for i in (7, 8, 9)
    ]
    tie_decision = policy().evaluate(tied, "fusion-tie")
    assert not tie_decision.accepted
    assert tie_decision.reason == "DIRECTION_TIE"

    weak = [signal(i, Direction.BUY, "80") for i in (4, 5, 6)] + [
        signal(i, Direction.SELL, "70") for i in (7, 8, 9)
    ]
    weak_decision = policy().evaluate(weak, "fusion-weak")
    assert not weak_decision.accepted
    assert weak_decision.reason == "DIRECTIONAL_DOMINANCE_TOO_LOW"


def test_only_winning_direction_is_attributed():
    signals = [signal(i, Direction.BUY, "90") for i in (4, 5, 6)] + [
        signal(7, Direction.SELL, "30")
    ]
    decision = policy(min_directional_dominance=Decimal("0.60")).evaluate(
        signals, "fusion-attribution"
    )
    assert decision.accepted
    assert {item.agent_id for item in decision.result.contributors} == {4, 5, 6}


def test_mixed_snapshots_and_invalid_individual_geometry_fail_closed():
    mixed = [
        signal(4, Direction.BUY),
        signal(5, Direction.BUY, snapshot="snap-2"),
        signal(6, Direction.BUY),
    ]
    assert policy().evaluate(mixed, "fusion-mixed").reason == "MIXED_SNAPSHOTS"

    bad = EntryLevels("100", "101", "110", "120", "130")
    invalid = [
        signal(4, Direction.BUY, levels=bad),
        signal(5, Direction.BUY),
        signal(6, Direction.BUY),
    ]
    decision = policy().evaluate(invalid, "fusion-invalid")
    assert not decision.accepted
    assert decision.reason == "INVALID_SIGNAL:INVALID_BUY_GEOMETRY"


def test_entry_policy_enforces_complete_directional_geometry():
    entry = EntryPolicy(clock=FixedClock(NOW))
    assert entry.validate_levels(
        Direction.BUY, EntryLevels("100", "90", "110", "120", "130")
    ).valid
    assert entry.validate_levels(
        Direction.SELL, EntryLevels("100", "110", "90", "80", "70")
    ).valid
    assert not entry.validate_levels(
        Direction.BUY, EntryLevels("100", "90", "120", "110", "130")
    ).valid
    assert not entry.validate_levels(
        Direction.SELL, EntryLevels("100", "90", "110", "120", "130")
    ).valid


def test_policy_instances_do_not_share_threshold_mutations():
    signals = [signal(i, Direction.BUY, "80") for i in (4, 5, 6)]
    permissive = policy(min_confluence_pct=Decimal("75"))
    strict = policy(min_confluence_pct=Decimal("85"))
    assert permissive.evaluate(signals, "fusion-permissive").accepted
    assert not strict.evaluate(signals, "fusion-strict").accepted
