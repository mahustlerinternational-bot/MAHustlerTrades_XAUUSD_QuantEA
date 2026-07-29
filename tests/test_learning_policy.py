import ast
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal
from pathlib import Path

import pytest

import python_swarm.learning_policy as learning_module
from python_swarm.agent_roster import AGENT_BY_ID
from python_swarm.domain_models import Direction, EntryLevels, FixedClock, SignalContribution
from python_swarm.learning_policy import (
    AuthoritativeClosedOutcome,
    CanonicalLearningPolicy,
    LearningActivationError,
    LearningConflictError,
    LearningDuplicateError,
    LearningInputError,
    LearningPolicyConfig,
    OutcomeSource,
    StrategyOutcomeAttribution,
)
from python_swarm.strategy_pipeline import CanonicalRegime, MarketSession
from python_swarm.trading_policies import FusionPolicy


BASE_MS = 1_800_000_000_000
PARENT_HASH = "a" * 64


def attributions(index: int, agent_ids=(4, 5, 6)):
    return tuple(
        StrategyOutcomeAttribution(
            agent_id=agent_id,
            signal_id=f"signal-{index}-{agent_id}",
            strategy_slug=AGENT_BY_ID[agent_id].slug,
            tags=(f"AGENT_{agent_id:02d}", "AUTHORITATIVE_ENTRY"),
        )
        for agent_id in reversed(agent_ids)
    )


def outcome(
    index: int,
    actual_r="1",
    *,
    agent_ids=(4, 5, 6),
    regime=CanonicalRegime.TRENDING_BULL,
    session=MarketSession.LONDON,
    config_hash="source-config-v1",
    account_id="account-demo-1",
    symbol="XAUUSD",
    magic=202412,
    source=OutcomeSource.BROKER_LEDGER,
    authoritative=True,
    broker_confirmed=True,
    deal_derived_actual_r=True,
):
    return AuthoritativeClosedOutcome(
        trade_id=f"trade-{index:04d}",
        close_event_id=f"close-event-{index:04d}",
        account_id=account_id,
        symbol=symbol,
        magic=magic,
        actual_r=actual_r,
        attributions=attributions(index, agent_ids),
        regime=regime,
        session=session,
        broker_opened_at_ms=BASE_MS + index * 10_000,
        broker_closed_at_ms=BASE_MS + index * 10_000 + 5_000,
        config_hash=config_hash,
        source=source,
        authoritative=authoritative,
        broker_confirmed=broker_confirmed,
        deal_derived_actual_r=deal_derived_actual_r,
    )


def proposal(outcomes, *, version=1, parent=None):
    return CanonicalLearningPolicy().propose(
        outcomes,
        proposal_version=version,
        target_config_hash="target-config-v2",
        parent_proposal_hash=parent,
    )


def context_stat(result, agent_id=4, regime=CanonicalRegime.TRENDING_BULL, session=MarketSession.LONDON):
    return next(
        item
        for item in result.context_stats
        if item.context.agent_id == agent_id
        and item.context.regime is regime
        and item.context.session is session
    )


def test_learning_module_has_no_io_random_or_wall_clock_imports():
    tree = ast.parse(Path(learning_module.__file__).read_text(encoding="utf-8"))
    imported_roots = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported_roots.update(
        node.module.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    )
    assert imported_roots.isdisjoint(
        {"asyncio", "os", "pathlib", "random", "sqlite3", "socket", "time"}
    )


def test_outcome_is_immutable_and_canonicalizes_attribution_order():
    item = outcome(1)

    assert tuple(attr.agent_id for attr in item.attributions) == (4, 5, 6)
    assert item.actual_r == Decimal("1")
    with pytest.raises(FrozenInstanceError):
        item.actual_r = Decimal("2")


def test_input_model_rejects_nonfinite_r_bad_time_and_incomplete_attribution():
    with pytest.raises(LearningInputError, match="finite"):
        outcome(1, "NaN")
    with pytest.raises(LearningInputError, match="close timestamp"):
        replace(outcome(1), broker_closed_at_ms=outcome(1).broker_opened_at_ms)
    with pytest.raises(LearningInputError, match="at least three"):
        outcome(1, agent_ids=(4, 5))
    with pytest.raises(LearningInputError, match="does not match"):
        StrategyOutcomeAttribution(4, "signal", "wrong_slug")


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"source": OutcomeSource.SYNTHETIC}, "not broker-ledger"),
        ({"source": OutcomeSource.RECOVERED}, "not broker-ledger"),
        ({"source": OutcomeSource.UNCONFIRMED}, "not broker-ledger"),
        ({"authoritative": False}, "not authoritative"),
        ({"broker_confirmed": False}, "unconfirmed"),
        ({"deal_derived_actual_r": False}, "not derived from broker deals"),
    ],
)
def test_non_authoritative_sources_are_rejected(changes, reason):
    with pytest.raises(LearningInputError, match=reason):
        proposal([outcome(1, **changes)])


def test_duplicate_and_conflicting_trade_or_event_identities_are_rejected():
    first = outcome(1)
    with pytest.raises(LearningDuplicateError, match="duplicate trade"):
        proposal([first, first])

    conflicting_trade = replace(first, close_event_id="different-close-event", actual_r=Decimal("-1"))
    with pytest.raises(LearningConflictError, match="conflicting outcome for trade"):
        proposal([first, conflicting_trade])

    reused_event = replace(outcome(2), close_event_id=first.close_event_id)
    with pytest.raises(LearningConflictError, match="close event"):
        proposal([first, reused_event])


def test_mixed_source_configuration_lineage_is_rejected():
    with pytest.raises(LearningConflictError, match="multiple source config"):
        proposal([outcome(1), outcome(2, config_hash="other-source-config")])


def test_mixed_runtime_account_symbol_magic_scope_is_rejected():
    with pytest.raises(LearningConflictError, match="account/symbol/magic"):
        proposal([outcome(1), outcome(2, account_id="different-account")])


def test_no_weight_activates_below_five_authoritative_outcomes():
    result = proposal([outcome(index, "2") for index in range(1, 5)])
    stat = context_stat(result)

    assert result.minimum_samples == 5
    assert stat.sample_count == 4
    assert stat.used_sample_count == 4
    assert stat.activated is False
    assert stat.proposed_weight == Decimal("1.0000")
    activation = result.activate(
        CanonicalRegime.TRENDING_BULL,
        MarketSession.LONDON,
        approved_proposal_hash=result.proposal_hash,
    )
    assert activation.fusion_weights[4] == Decimal("1.0")
    assert next(item for item in activation.weights if item.agent_id == 4).learned is False


def test_fifth_authoritative_outcome_activates_exact_context_only():
    result = proposal([outcome(index, "1") for index in range(1, 6)])
    stat = context_stat(result)

    assert stat.used_sample_count == 5
    assert stat.activated is True
    assert Decimal("1") < stat.proposed_weight <= result.maximum_weight

    london = result.activate(
        CanonicalRegime.TRENDING_BULL,
        MarketSession.LONDON,
        approved_proposal_hash=result.proposal_hash,
    )
    other_context = result.activate(
        CanonicalRegime.TRENDING_BEAR,
        MarketSession.LONDON,
        approved_proposal_hash=result.proposal_hash,
    )
    assert london.fusion_weights[4] == stat.proposed_weight
    assert other_context.fusion_weights[4] == Decimal("1.0")


@pytest.mark.parametrize(
    "values,weight_relation,expected_balance",
    [
        (["2"] * 5, "above", Decimal("1.0000")),
        (["-2"] * 5, "below", Decimal("-1.0000")),
        (["0"] * 5, "equal", Decimal("0.0000")),
    ],
)
def test_winning_only_losing_only_and_zero_denominator_are_safe(
    values, weight_relation, expected_balance
):
    result = proposal([outcome(index, value) for index, value in enumerate(values, start=1)])
    stat = context_stat(result)

    assert stat.payoff_balance == expected_balance
    assert stat.proposed_weight.is_finite()
    assert result.minimum_weight <= stat.proposed_weight <= result.maximum_weight
    if weight_relation == "above":
        assert stat.proposed_weight > Decimal("1")
    elif weight_relation == "below":
        assert stat.proposed_weight < Decimal("1")
    else:
        assert stat.proposed_weight == Decimal("1.0000")


def test_extreme_finite_actual_r_is_clipped_and_cannot_escape_bounds():
    values = ["1E+999", "1E+999", "-1E+999", "1E+999", "-1E+999"]
    result = proposal([outcome(index, value) for index, value in enumerate(values, start=1)])
    stat = context_stat(result)

    assert stat.clipped_r_sum == Decimal("3.0000")
    assert stat.proposed_weight.is_finite()
    assert result.minimum_weight <= stat.proposed_weight <= result.maximum_weight


def test_proposal_order_hash_and_lineage_are_deterministic():
    inputs = [outcome(index, value) for index, value in enumerate(("1", "-1", "0", "2", "-2"), 1)]
    first = proposal(inputs)
    second = proposal(list(reversed(inputs)))

    assert first == second
    assert first.proposal_hash == second.proposal_hash
    assert len(first.proposal_hash) == 64
    assert first.proposal_id == f"learn-{first.proposal_hash[:32]}"
    assert first.source_trade_ids == tuple(sorted(item.trade_id for item in inputs))
    assert first.source_event_ids == tuple(sorted(item.close_event_id for item in inputs))
    assert first.source_config_hash == "source-config-v1"
    assert (first.account_id, first.symbol, first.magic) == (
        "account-demo-1",
        "XAUUSD",
        202412,
    )
    assert first.target_config_hash == "target-config-v2"
    assert first.as_of_broker_time_ms == max(item.broker_closed_at_ms for item in inputs)


def test_semantically_equal_decimal_encodings_produce_same_hash():
    first = proposal([outcome(index, "1.0") for index in range(1, 6)])
    second = proposal([outcome(index, "1.00") for index in range(1, 6)])
    assert first.proposal_hash == second.proposal_hash


def test_version_lineage_is_mandatory_and_hashed():
    with pytest.raises(LearningInputError, match="first proposal cannot"):
        proposal([outcome(1)], parent=PARENT_HASH)
    with pytest.raises(LearningInputError, match="require parent"):
        proposal([outcome(1)], version=2)

    second = proposal([outcome(1)], version=2, parent=PARENT_HASH)
    assert second.parent_proposal_hash == PARENT_HASH
    assert second.proposal_version == 2


def test_policy_configuration_cannot_weaken_five_sample_or_positive_bounds():
    with pytest.raises(LearningInputError, match="integer >= 5"):
        LearningPolicyConfig(minimum_samples=4)
    with pytest.raises(LearningInputError, match="positive and ordered"):
        LearningPolicyConfig(minimum_weight=Decimal("0"))
    with pytest.raises(LearningInputError, match="conservative"):
        LearningPolicyConfig(maximum_adjustment=Decimal("0.30"))
    with pytest.raises(LearningInputError, match="exactly 1.0"):
        LearningPolicyConfig(neutral_weight=Decimal("0.90"))
    with pytest.raises(LearningInputError, match="documented"):
        LearningPolicyConfig(maximum_weight=Decimal("1.50"))
    with pytest.raises(LearningInputError, match="power-of-ten"):
        LearningPolicyConfig(weight_quantum=Decimal("0.03"))


def test_context_window_is_bounded_but_total_sample_lineage_is_retained():
    inputs = [outcome(index, "1") for index in range(1, 106)]
    result = proposal(inputs)
    stat = context_stat(result)

    assert stat.sample_count == 105
    assert stat.used_sample_count == 100
    assert result.agent_summaries[0].total_sample_count == 105


def test_activation_requires_exact_approval_and_is_read_only():
    result = proposal([outcome(index, "1") for index in range(1, 6)])
    with pytest.raises(LearningActivationError, match="does not match"):
        result.activate(
            CanonicalRegime.TRENDING_BULL,
            MarketSession.LONDON,
            approved_proposal_hash="b" * 64,
        )

    activation = result.activate(
        CanonicalRegime.TRENDING_BULL,
        MarketSession.LONDON,
        approved_proposal_hash=result.proposal_hash,
    )
    assert tuple(activation.fusion_weights) == tuple(range(4, 14))
    assert (activation.account_id, activation.symbol, activation.magic) == (
        result.account_id,
        result.symbol,
        result.magic,
    )
    with pytest.raises(TypeError):
        activation.fusion_weights[4] = Decimal("10")
    with pytest.raises(FrozenInstanceError):
        activation.target_config_hash = "changed"
    assert not hasattr(activation, "enabled_agent_ids")
    assert not hasattr(activation, "risk_pct")
    assert not hasattr(activation, "min_agents_agreeing")


def signal(agent_id, *, observed_at_ms=BASE_MS):
    return SignalContribution(
        signal_id=f"fusion-signal-{agent_id}",
        snapshot_id="learning-fusion-snapshot",
        agent_id=agent_id,
        direction=Direction.BUY,
        conviction="80",
        levels=EntryLevels("2000", "1990", "2010", "2020", "2030"),
        observed_at_ms=observed_at_ms,
        regime=CanonicalRegime.TRENDING_BULL.value,
        session=MarketSession.LONDON.value,
        strategy_tags=(AGENT_BY_ID[agent_id].slug,),
    )


def test_activation_can_only_weight_existing_signals_in_canonical_fusion():
    result = proposal([outcome(index, "1") for index in range(1, 6)])
    activation = result.activate(
        CanonicalRegime.TRENDING_BULL,
        MarketSession.LONDON,
        approved_proposal_hash=result.proposal_hash,
    )
    fusion = FusionPolicy(
        clock=FixedClock(BASE_MS),
        agent_weights=activation.fusion_weights,
    ).evaluate((signal(4), signal(5), signal(6)), "learning-fusion")

    assert fusion.accepted is True
    assert fusion.result is not None
    assert tuple(item.agent_id for item in fusion.result.contributors) == (4, 5, 6)
    assert 10 not in tuple(item.agent_id for item in fusion.result.contributors)


def test_empty_batch_and_non_outcome_objects_are_rejected():
    with pytest.raises(LearningInputError, match="at least one"):
        proposal([])
    with pytest.raises(LearningInputError, match="AuthoritativeClosedOutcome"):
        proposal([object()])
