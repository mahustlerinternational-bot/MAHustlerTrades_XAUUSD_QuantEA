import ast
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal
from pathlib import Path

import pytest

import python_swarm.decision_service as decision_module
from python_swarm.decision_service import (
    CanonicalDecisionService,
    DecisionPolicyConfig,
    DecisionReason,
    DecisionRequest,
    DecisionRuntimeIdentity,
    MacroDecisionPolicy,
    normalize_fused_levels,
)
from python_swarm.domain_models import (
    AccountMode,
    AccountSnapshot,
    BrokerSnapshot,
    Direction,
    EntryLevels,
    FixedClock,
    QuoteSnapshot,
    ReservationState,
    RiskReservation,
    SymbolSnapshot,
)
from python_swarm.learning_policy import ActivatedAgentWeight, LearningActivation
from python_swarm.strategy_pipeline import (
    AgentEvaluation,
    Bar,
    CanonicalMarketSnapshot,
    CanonicalRegime,
    CanonicalStrategyPipeline,
    DataMode,
    EvidenceSource,
    IntermarketContext,
    IntermarketObservation,
    MacroContext,
    MarketSession,
    MarketSource,
    ProviderHealth,
    SentimentObservation,
    StrategyCandidate,
    StrategyPipelineResult,
)
from python_swarm.trading_policies import RiskPolicyConfig


DAY_MS = 86_400_000
NOW = (1_800_000_000_000 // DAY_MS) * DAY_MS + 8 * 3_600_000
MAGIC = 20260715


def make_bars(
    count: int,
    interval_ms: int,
    *,
    end: Decimal = Decimal("2000.0"),
    step: Decimal = Decimal("0.5"),
) -> tuple[Bar, ...]:
    first_close = end - step * (count - 1)
    first_open_ms = NOW - interval_ms * count
    return tuple(
        Bar(
            open_time_ms=first_open_ms + interval_ms * index,
            close_time_ms=first_open_ms + interval_ms * (index + 1),
            open=(close := first_close + step * index) - step / 2,
            high=close + Decimal("1"),
            low=close - Decimal("1"),
            close=close,
            volume=Decimal("100"),
        )
        for index in range(count)
    )


def healthy_macro(*, freeze: bool = False) -> MacroContext:
    return MacroContext(
        source="calendar-primary",
        source_kind=EvidenceSource.HTTP,
        status=ProviderHealth.HEALTHY,
        source_at_ms=NOW - 1_000,
        received_at_ms=NOW - 500,
        sequence=8,
        freshness_ms=60_000,
        coverage_start_ms=NOW - 3_600_000,
        coverage_end_ms=NOW + 90_000_000,
        minimum_forward_coverage_ms=86_400_000,
        freeze_active=freeze,
        sentiment=SentimentObservation(
            source="sentiment-primary",
            source_kind=EvidenceSource.HTTP,
            observed_at_ms=NOW - 1_000,
            sequence=9,
            score=Decimal("0.8"),
        ),
    )


def healthy_intermarket() -> IntermarketContext:
    return IntermarketContext(
        source="intermarket-primary",
        source_kind=EvidenceSource.HTTP,
        status=ProviderHealth.HEALTHY,
        received_at_ms=NOW - 100,
        sequence=12,
        freshness_ms=15_000,
        previous=IntermarketObservation(
            observed_at_ms=NOW - 2_000,
            dxy="100",
            us10y="4.00",
            us30="40000",
            gold="1999.8",
        ),
        current=IntermarketObservation(
            observed_at_ms=NOW - 1_000,
            dxy="100.1",
            us10y="4.01",
            us30="40010",
            gold="2000.0",
        ),
    )


def make_market(**changes) -> CanonicalMarketSnapshot:
    values = {
        "snapshot_id": "snapshot-0001",
        "symbol": "XAUUSD",
        "captured_at_ms": NOW,
        "mode": DataMode.LIVE,
        "market_source": MarketSource.BROKER,
        "bid": Decimal("2000.0"),
        "ask": Decimal("2000.1"),
        "tick_size": Decimal("0.1"),
        "h1": make_bars(60, 3_600_000),
        "h4": make_bars(25, 4 * 3_600_000),
        "d1": make_bars(25, DAY_MS),
        "m15": make_bars(40, 15 * 60_000),
        "m5": make_bars(50, 5 * 60_000),
        "m1": make_bars(60, 60_000),
        "macro": healthy_macro(),
        "intermarket": healthy_intermarket(),
    }
    values.update(changes)
    return CanonicalMarketSnapshot(**values)


def make_broker(market: CanonicalMarketSnapshot, **changes) -> BrokerSnapshot:
    account = changes.pop(
        "account",
        AccountSnapshot(
            account_id="account-1",
            currency="USD",
            mode=AccountMode.HEDGING,
            balance="10000",
            equity="10000",
            free_margin="10000",
            risk_day="2026-07-15",
            day_start_equity="10000",
            baseline_verified=True,
            daily_halted=False,
            captured_at_ms=market.captured_at_ms,
        ),
    )
    symbol = changes.pop(
        "symbol",
        SymbolSnapshot(
            symbol=market.symbol,
            digits=1,
            tick_size=market.tick_size,
            tick_value_per_lot="1",
            contract_size="100",
            volume_min="0.01",
            volume_step="0.01",
            volume_max="5",
            stops_level_points=5,
            freeze_level_points=5,
            margin_per_lot="100",
        ),
    )
    quote = changes.pop(
        "quote",
        QuoteSnapshot(
            market.symbol,
            market.bid,
            market.ask,
            market.captured_at_ms,
        ),
    )
    return BrokerSnapshot(
        snapshot_id=changes.pop("snapshot_id", market.snapshot_id),
        connection_id=changes.pop("connection_id", "connection-1"),
        account=account,
        symbol=symbol,
        quote=quote,
        positions=changes.pop("positions", ()),
        captured_at_ms=changes.pop("captured_at_ms", market.captured_at_ms),
        reconciled=changes.pop("reconciled", True),
        foreign_symbol_exposure=changes.pop("foreign_symbol_exposure", False),
        **changes,
    )


def make_identity(**changes) -> DecisionRuntimeIdentity:
    values = {
        "runtime_id": "runtime-1",
        "account_id": "account-1",
        "symbol": "XAUUSD",
        "magic": MAGIC,
        "authenticated_magic": MAGIC,
        "config_hash": "config-v1",
        "connection_id": "connection-1",
        "runtime_ready": True,
        "ownership_verified": True,
        "execution_authorized": True,
    }
    values.update(changes)
    return DecisionRuntimeIdentity(**values)


def make_pipeline(
    direction: Direction = Direction.BUY,
    *,
    signal_agents: tuple[int, ...] = (4, 5, 6),
) -> CanonicalStrategyPipeline:
    def analyzer(_market, _regime):
        return StrategyCandidate(
            direction=direction,
            conviction=Decimal("80"),
            risk_distance=Decimal("10"),
            tags=("TEST_SETUP",),
        )

    return CanonicalStrategyPipeline(
        analyzers={
            agent_id: analyzer if agent_id in signal_agents else (lambda *_: None)
            for agent_id in range(4, 14)
        }
    )


class StaticPipeline:
    def __init__(self, result):
        self.result = result
        self.calls = 0

    def evaluate(self, _market):
        self.calls += 1
        return self.result


class ExplodingPipeline:
    def __init__(self):
        self.calls = 0

    def evaluate(self, _market):
        self.calls += 1
        raise AssertionError("pipeline must not run")


def make_request(
    *,
    market: CanonicalMarketSnapshot | None = None,
    broker: BrokerSnapshot | None = None,
    identity: DecisionRuntimeIdentity | None = None,
    risk: str = "1",
    slippage: int = 10,
    macro_policy: MacroDecisionPolicy = MacroDecisionPolicy.FAIL_CLOSED,
    reservations: tuple[RiskReservation, ...] = (),
    learning: LearningActivation | None = None,
    nonce: str = "nonce-1",
) -> DecisionRequest:
    selected_market = market or make_market()
    return DecisionRequest(
        request_nonce=nonce,
        market=selected_market,
        broker=broker or make_broker(selected_market),
        identity=identity or make_identity(),
        requested_risk_pct=Decimal(risk),
        max_slippage_points=slippage,
        macro_policy=macro_policy,
        reservations=reservations,
        learning_activation=learning,
    )


def service(
    direction: Direction = Direction.BUY,
    *,
    pipeline=None,
    config: DecisionPolicyConfig | None = None,
    risk_config: RiskPolicyConfig | None = None,
) -> CanonicalDecisionService:
    return CanonicalDecisionService(
        clock=FixedClock(NOW),
        pipeline=pipeline or make_pipeline(direction),
        config=config,
        risk_config=risk_config,
    )


def with_signal_levels(
    result: StrategyPipelineResult,
    levels: EntryLevels,
) -> StrategyPipelineResult:
    evaluations = tuple(
        replace(item, signal=replace(item.signal, levels=levels))
        if item.signal is not None
        else item
        for item in result.evaluations
    )
    return replace(result, evaluations=evaluations)


def make_learning(
    pipeline_result: StrategyPipelineResult,
    **changes,
) -> LearningActivation:
    assert pipeline_result.regime.assessment is not None
    values = {
        "proposal_hash": "a" * 64,
        "proposal_version": 1,
        "account_id": "account-1",
        "symbol": "XAUUSD",
        "magic": MAGIC,
        "target_config_hash": "config-v1",
        "regime": pipeline_result.regime.assessment.regime,
        "session": pipeline_result.session,
        "weights": tuple(
            ActivatedAgentWeight(agent_id, Decimal("1"), 50, True)
            for agent_id in range(4, 14)
        ),
    }
    values.update(changes)
    return LearningActivation(**values)


@pytest.mark.parametrize(
    ("direction", "expected_entry"),
    ((Direction.BUY, Decimal("2000.1")), (Direction.SELL, Decimal("2000.0"))),
)
def test_buy_and_sell_use_executable_side_and_return_only_actual_agreement(
    direction,
    expected_entry,
):
    coordinator = service(direction)
    request = make_request()

    first = coordinator.decide(request)
    second = coordinator.decide(request)

    assert first == second
    assert first.approved
    assert first.reason is DecisionReason.APPROVED
    assert first.intent is not None and first.risk_decision is not None
    assert first.intent.direction is direction
    assert first.intent.reference_price == expected_entry
    assert first.intent.levels.entry == expected_entry
    assert tuple(item.agent_id for item in first.intent.contributors) == (4, 5, 6)
    assert first.intent.decision_id == first.decision_id
    assert first.intent.snapshot_id == request.market.snapshot_id
    assert all(
        item.snapshot_id == request.market.snapshot_id
        and item.observed_at_ms == request.market.captured_at_ms
        for item in first.intent.contributors
    )
    assert first.risk_decision.risk_pct <= Decimal("1")
    if direction is Direction.BUY:
        assert (
            first.intent.levels.sl
            < expected_entry
            < first.intent.levels.tp1
            < first.intent.levels.tp2
            < first.intent.levels.tp3
        )
    else:
        assert (
            first.intent.levels.tp3
            < first.intent.levels.tp2
            < first.intent.levels.tp1
            < expected_entry
            < first.intent.levels.sl
        )


def test_nonce_changes_all_proposed_identity_without_nondeterminism():
    coordinator = service()
    one = coordinator.decide(make_request(nonce="one"))
    two = coordinator.decide(make_request(nonce="two"))

    assert one.approved and two.approved
    assert one.decision_id != two.decision_id
    assert one.fusion_result.fusion_id != two.fusion_result.fusion_id
    assert one.intent.trade_id != two.intent.trade_id
    assert one.intent.intent_id != two.intent.intent_id
    assert (
        one.risk_decision.reservation.reservation_id
        != two.risk_decision.reservation.reservation_id
    )


def test_same_nonce_with_changed_provider_evidence_changes_bound_ids():
    coordinator = service()
    first_market = make_market()
    second_market = replace(
        first_market,
        macro=replace(first_market.macro, sequence=first_market.macro.sequence + 1),
    )

    first = coordinator.decide(make_request(market=first_market, nonce="same"))
    second = coordinator.decide(make_request(market=second_market, nonce="same"))

    assert first.approved and second.approved
    assert first.decision_id != second.decision_id
    assert first.fusion_result.fusion_id != second.fusion_result.fusion_id
    assert first.intent.trade_id != second.intent.trade_id


def test_same_admitted_evidence_keeps_retry_identities_across_clock_advance():
    class MutableDecisionClock:
        def __init__(self, value: int) -> None:
            self.value = value

        def now_ms(self) -> int:
            return self.value

    clock = MutableDecisionClock(NOW)
    coordinator = CanonicalDecisionService(clock=clock, pipeline=make_pipeline())
    request = make_request(nonce="idempotent-retry")

    first = coordinator.decide(request)
    clock.value += 250
    second = coordinator.decide(request)

    assert first.approved and second.approved
    assert first.decision_id == second.decision_id
    assert first.fusion_result.fusion_id == second.fusion_result.fusion_id
    assert first.intent.trade_id == second.intent.trade_id
    assert first.intent.intent_id == second.intent.intent_id
    assert (
        first.risk_decision.reservation.reservation_id
        == second.risk_decision.reservation.reservation_id
    )
    assert first.intent.created_at_ms != second.intent.created_at_ms


@pytest.mark.parametrize(
    ("risk", "runtime_cap", "hard_cap"),
    (("1.01", "1", "1"), ("0.6", "0.5", "1"), ("0.6", "1", "0.5")),
)
def test_over_budget_request_is_rejected_before_pipeline_or_risk_sizing(
    risk,
    runtime_cap,
    hard_cap,
):
    pipeline = ExplodingPipeline()
    coordinator = service(
        pipeline=pipeline,
        config=DecisionPolicyConfig(runtime_max_risk_pct=runtime_cap),
        risk_config=RiskPolicyConfig(hard_max_risk_pct=hard_cap),
    )

    result = coordinator.decide(make_request(risk=risk))

    assert not result.approved
    assert result.reason is DecisionReason.REQUESTED_RISK_EXCEEDS_RUNTIME_CAP
    assert result.intent is None and result.risk_decision is None
    assert pipeline.calls == 0


@pytest.mark.parametrize("macro", (None, replace(healthy_macro(), source_at_ms=NOW - 60_001)))
def test_missing_or_stale_macro_evidence_globally_blocks_fail_closed(macro):
    market = make_market(macro=macro)
    result = service().decide(make_request(market=market))

    assert not result.approved
    assert result.reason is DecisionReason.MACRO_PROTECTION_UNAVAILABLE
    assert result.provider_gate is not None
    assert not result.provider_gate.macro_healthy
    assert 10 in result.disabled_agents and 14 in result.disabled_agents


def test_macro_freeze_always_blocks_even_stale_unsafe_mode():
    macro = replace(
        healthy_macro(freeze=True),
        status=ProviderHealth.ERROR,
        source_at_ms=NOW - 1_000_000,
    )
    market = make_market(macro=macro)
    result = service().decide(
        make_request(
            market=market,
            macro_policy=MacroDecisionPolicy.UNSAFE_TECHNICAL_ONLY,
        )
    )

    assert not result.approved
    assert result.reason is DecisionReason.MACRO_FREEZE_ACTIVE
    assert result.provider_gate.unsafe_technical_only
    assert {10, 14}.issubset(result.disabled_agents)


def test_explicit_unsafe_mode_is_visible_and_cannot_reenable_macro_agents():
    evidence_market = make_market()
    malicious = make_pipeline(signal_agents=(4, 5, 6, 10)).evaluate(evidence_market)
    market = replace(evidence_market, macro=None)
    result = service(pipeline=StaticPipeline(malicious)).decide(
        make_request(
            market=market,
            macro_policy=MacroDecisionPolicy.UNSAFE_TECHNICAL_ONLY,
        )
    )

    assert result.approved
    assert result.degraded
    assert result.provider_gate.reason == "UNSAFE_TECHNICAL_ONLY"
    assert result.disabled_agents == (10, 14)
    assert tuple(item.agent_id for item in result.intent.contributors) == (4, 5, 6)


def test_calendar_health_without_genuine_sentiment_disables_agent10_only():
    evidence_market = make_market()
    malicious = make_pipeline(signal_agents=(4, 5, 6, 10)).evaluate(evidence_market)
    calendar_only = replace(
        evidence_market,
        macro=replace(evidence_market.macro, sentiment=None),
    )
    result = service(pipeline=StaticPipeline(malicious)).decide(
        make_request(market=calendar_only)
    )

    assert result.approved
    assert result.degraded
    assert result.provider_gate.macro_healthy
    assert not result.provider_gate.sentiment_healthy
    assert result.disabled_agents == (10,)
    assert tuple(item.agent_id for item in result.intent.contributors) == (4, 5, 6)


def test_intermarket_failure_only_disables_12_and_15_and_still_allows_entry():
    evidence_market = make_market()
    malicious = make_pipeline(signal_agents=(4, 5, 6, 12)).evaluate(evidence_market)
    market = replace(evidence_market, intermarket=None)
    result = service(pipeline=StaticPipeline(malicious)).decide(
        make_request(market=market)
    )

    assert result.approved
    assert result.degraded
    assert result.provider_gate.reason == "INTERMARKET_UNAVAILABLE"
    assert result.disabled_agents == (12, 15)
    assert tuple(item.agent_id for item in result.intent.contributors) == (4, 5, 6)


def test_future_or_mixed_provider_timestamps_are_derived_as_unhealthy():
    future_macro = replace(
        healthy_macro(),
        source_at_ms=NOW + 1,
        received_at_ms=NOW,
    )
    macro_result = service().decide(
        make_request(market=make_market(macro=future_macro))
    )
    assert not macro_result.approved
    assert macro_result.reason is DecisionReason.MACRO_PROTECTION_UNAVAILABLE

    future_intermarket = replace(healthy_intermarket(), received_at_ms=NOW + 1)
    intermarket_result = service().decide(
        make_request(market=make_market(intermarket=future_intermarket))
    )
    assert intermarket_result.approved
    assert intermarket_result.provider_gate.intermarket_healthy is False
    assert intermarket_result.disabled_agents == (12, 15)


def test_learning_activation_requires_exact_scope_config_regime_and_session():
    market = make_market()
    selected_pipeline = make_pipeline()
    pipeline_result = selected_pipeline.evaluate(market)
    activation = make_learning(pipeline_result)
    approved = service(pipeline=selected_pipeline).decide(
        make_request(market=market, learning=activation)
    )
    assert approved.approved

    actual_regime = pipeline_result.regime.assessment.regime
    other_regime = next(item for item in CanonicalRegime if item is not actual_regime)
    cases = (
        (
            replace(activation, account_id="account-2"),
            DecisionReason.LEARNING_ACCOUNT_MISMATCH,
        ),
        (replace(activation, symbol="XAUEUR"), DecisionReason.LEARNING_SYMBOL_MISMATCH),
        (replace(activation, magic=MAGIC + 1), DecisionReason.LEARNING_MAGIC_MISMATCH),
        (
            replace(activation, target_config_hash="config-v2"),
            DecisionReason.LEARNING_CONFIG_MISMATCH,
        ),
        (replace(activation, regime=other_regime), DecisionReason.LEARNING_REGIME_MISMATCH),
        (
            replace(activation, session=MarketSession.ASIA),
            DecisionReason.LEARNING_SESSION_MISMATCH,
        ),
    )
    for invalid, reason in cases:
        rejected = service(pipeline=selected_pipeline).decide(
            make_request(market=market, learning=invalid)
        )
        assert not rejected.approved
        assert rejected.reason is reason


def test_pipeline_output_must_match_admitted_snapshot_and_context():
    market = make_market()
    baseline = make_pipeline().evaluate(market)
    no_signals = tuple(AgentEvaluation(agent_id, "NO_SETUP") for agent_id in range(4, 14))
    mismatched = replace(
        baseline,
        snapshot_id="different-snapshot",
        evaluations=no_signals,
    )
    result = service(pipeline=StaticPipeline(mismatched)).decide(
        make_request(market=market)
    )

    assert not result.approved
    assert result.reason is DecisionReason.STRATEGY_RESULT_IDENTITY_MISMATCH
    assert result.detail_code == "PIPELINE_SNAPSHOT_ID_MISMATCH"


def test_fused_levels_are_revalidated_against_current_buy_ask():
    market = make_market()
    baseline = make_pipeline().evaluate(market)
    # Valid around the signal's 1990 entry, but TP1 is already below ask=2000.1.
    malicious = with_signal_levels(
        baseline,
        EntryLevels("1990", "1980", "1995", "2005", "2010"),
    )
    result = service(pipeline=StaticPipeline(malicious)).decide(
        make_request(market=market)
    )

    assert not result.approved
    assert result.reason is DecisionReason.FUSED_LEVELS_INVALID_AT_EXECUTABLE_QUOTE
    assert result.detail_code == "INVALID_BUY_GEOMETRY"


def test_tick_normalization_never_invents_ordering_when_targets_collapse():
    market = make_market()
    baseline = make_pipeline().evaluate(market)
    collapsing = with_signal_levels(
        baseline,
        EntryLevels("2000.1", "1999.91", "2000.11", "2000.12", "2000.13"),
    )
    result = service(pipeline=StaticPipeline(collapsing)).decide(
        make_request(market=market)
    )

    assert not result.approved
    assert result.reason is DecisionReason.NORMALIZED_LEVELS_INVALID
    assert result.detail_code == "INVALID_BUY_GEOMETRY"


@pytest.mark.parametrize(
    ("direction", "levels"),
    (
        (
            Direction.BUY,
            EntryLevels("2000.1", "1999.1", "2001.1", "2002.1", "2003.1"),
        ),
        (
            Direction.SELL,
            EntryLevels("2000.0", "2001.0", "1999.0", "1998.0", "1997.0"),
        ),
    ),
)
def test_tick_rounding_exact_boundaries_are_identity_operations(direction, levels):
    market = make_market()
    baseline = make_pipeline(direction).evaluate(market)
    result = service(
        direction,
        pipeline=StaticPipeline(with_signal_levels(baseline, levels)),
    ).decide(make_request(market=market))

    assert result.approved
    assert result.intent.levels == levels


@pytest.mark.parametrize(
    ("direction", "levels", "expected"),
    (
        (
            Direction.BUY,
            EntryLevels("2000.08", "1999.94", "2000.11", "2000.22", "2000.33"),
            EntryLevels("2000.1", "1999.9", "2000.2", "2000.3", "2000.4"),
        ),
        (
            Direction.SELL,
            EntryLevels("2000.04", "2000.16", "1999.99", "1999.88", "1999.77"),
            EntryLevels("2000.0", "2000.2", "1999.9", "1999.8", "1999.7"),
        ),
    ),
)
def test_outward_tick_rounding_is_directional(direction, levels, expected):
    market = make_market()
    baseline = make_pipeline(direction).evaluate(market)
    fused = with_signal_levels(baseline, levels)
    fusion = service(direction, pipeline=StaticPipeline(fused)).decide(
        make_request(market=market)
    ).fusion_result
    # The full decision may reject broker R/stops, but fusion evidence is available.
    assert fusion is not None
    assert normalize_fused_levels(fusion, make_broker(market)) == expected


@pytest.mark.parametrize(
    ("levels", "detail"),
    (
        (
            EntryLevels("2000.1", "1999.9", "2000.3", "2000.5", "2000.7"),
            "BROKER_STOPS_LEVEL_VIOLATION",
        ),
        (
            EntryLevels("2000.1", "1999.1", "2000.6", "2001.0", "2001.5"),
            "MIN_REWARD_TO_RISK_VIOLATION",
        ),
    ),
)
def test_final_broker_stops_freeze_and_reward_checks_are_end_to_end(levels, detail):
    market = make_market()
    baseline = make_pipeline().evaluate(market)
    result = service(pipeline=StaticPipeline(with_signal_levels(baseline, levels))).decide(
        make_request(market=market)
    )

    assert not result.approved
    assert result.reason is DecisionReason.FINAL_ENTRY_REJECTED
    assert result.detail_code == detail


def test_fused_reference_is_preserved_so_quote_slippage_cannot_be_erased():
    market = make_market()
    baseline = make_pipeline().evaluate(market)
    drifted = with_signal_levels(
        baseline,
        EntryLevels("1999.0", "1988.0", "2012.0", "2023.0", "2034.0"),
    )
    result = service(pipeline=StaticPipeline(drifted)).decide(
        make_request(market=market, slippage=10)
    )

    assert not result.approved
    assert result.reason is DecisionReason.FINAL_ENTRY_REJECTED
    assert result.detail_code == "MAX_SLIPPAGE_EXCEEDED"
    assert result.intent.reference_price == Decimal("1999.0")
    assert result.intent.levels.entry == market.ask
    assert result.risk_decision is None


@pytest.mark.parametrize(
    ("request_update", "expected"),
    (
        ({"identity": make_identity(runtime_ready=False)}, DecisionReason.RUNTIME_NOT_READY),
        (
            {"identity": make_identity(ownership_verified=False)},
            DecisionReason.OWNERSHIP_NOT_VERIFIED,
        ),
        (
            {"identity": make_identity(execution_authorized=False)},
            DecisionReason.EXECUTION_NOT_AUTHORIZED,
        ),
        (
            {"identity": make_identity(authenticated_magic=MAGIC + 1)},
            DecisionReason.AUTHENTICATED_MAGIC_MISMATCH,
        ),
    ),
)
def test_runtime_ownership_boundary_fails_closed(request_update, expected):
    result = service().decide(make_request(**request_update))
    assert not result.approved
    assert result.reason is expected


def test_snapshot_reconciliation_quote_and_tick_identity_are_pinned():
    market = make_market()
    cases = (
        (make_broker(market, snapshot_id="other"), DecisionReason.MARKET_BROKER_SNAPSHOT_MISMATCH),
        (make_broker(market, connection_id="other"), DecisionReason.CONNECTION_IDENTITY_MISMATCH),
        (make_broker(market, reconciled=False), DecisionReason.BROKER_NOT_RECONCILED),
        (
            make_broker(
                market,
                quote=QuoteSnapshot("XAUUSD", "1999.9", "2000.1", NOW),
            ),
            DecisionReason.QUOTE_MISMATCH,
        ),
        (
            make_broker(
                market,
                symbol=replace(make_broker(market).symbol, tick_size=Decimal("0.01")),
            ),
            DecisionReason.TICK_SIZE_MISMATCH,
        ),
    )
    for broker, expected in cases:
        result = service().decide(make_request(market=market, broker=broker))
        assert not result.approved
        assert result.reason is expected


def test_entry_policy_rejects_quote_that_is_stale_at_decision_time():
    market = make_market()
    coordinator = CanonicalDecisionService(
        clock=FixedClock(NOW + 6_000),
        config=DecisionPolicyConfig(maximum_snapshot_age_ms=10_000),
        pipeline=make_pipeline(),
    )
    result = coordinator.decide(make_request(market=market))

    assert not result.approved
    assert result.reason is DecisionReason.FINAL_ENTRY_REJECTED
    assert result.detail_code == "STALE_OR_FUTURE_QUOTE"
    assert result.risk_decision is None


def test_empty_eligible_coalition_cannot_manufacture_three_agent_agreement():
    market = make_market()
    baseline = make_pipeline().evaluate(market)
    ineligible = replace(
        baseline,
        evaluations=tuple(
            replace(
                item,
                signal=replace(item.signal, context_enabled=False),
            )
            if item.signal is not None
            else item
            for item in baseline.evaluations
        ),
    )
    result = service(pipeline=StaticPipeline(ineligible)).decide(
        make_request(market=market)
    )

    assert not result.approved
    assert result.reason is DecisionReason.FUSION_REJECTED
    assert result.detail_code == "INSUFFICIENT_ELIGIBLE_SIGNALS"
    assert result.intent is None and result.risk_decision is None


def test_rejected_risk_decision_never_exposes_a_reservation():
    market = make_market()
    halted_account = replace(make_broker(market).account, daily_halted=True)
    broker = make_broker(market, account=halted_account)
    result = service().decide(make_request(market=market, broker=broker))

    assert not result.approved
    assert result.reason is DecisionReason.RISK_REJECTED
    assert result.risk_decision is not None
    assert result.risk_decision.reservation is None


def test_reservation_scope_and_duplicate_identity_fail_closed():
    market = make_market()
    wrong_scope = RiskReservation(
        reservation_id="existing-1",
        trade_id="trade-existing-1",
        account_id="account-2",
        direction=Direction.BUY,
        risk_day="2026-07-15",
        risk_amount="10",
        volume="0.1",
        state=ReservationState.RELEASED,
        created_at_ms=NOW - 2_000,
        expires_at_ms=NOW - 1_000,
    )
    scope_result = service().decide(
        make_request(market=market, reservations=(wrong_scope,))
    )
    assert scope_result.reason is DecisionReason.RESERVATION_SCOPE_MISMATCH

    correct = replace(wrong_scope, account_id="account-1")
    duplicate_result = service().decide(
        make_request(market=market, reservations=(correct, correct))
    )
    assert duplicate_result.reason is DecisionReason.RESERVATION_IDENTITY_CONFLICT


def test_decision_service_is_command_free_side_effect_free_and_results_are_immutable():
    source_path = Path(decision_module.__file__)
    source = source_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
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
        {"asyncio", "os", "pathlib", "random", "sqlite3", "socket", "subprocess", "time"}
    )
    assert not any(
        isinstance(node, ast.Name) and node.id == "BrokerCommand"
        for node in ast.walk(tree)
    )

    result = service().decide(make_request())
    assert result.approved
    with pytest.raises(FrozenInstanceError):
        result.approved = False
    with pytest.raises(FrozenInstanceError):
        result.intent.trade_id = "mutated"
