import ast
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal
from pathlib import Path

import pytest

import python_swarm.strategy_pipeline as pipeline_module
from python_swarm.domain_models import Direction, FixedClock
from python_swarm.provider_adapters import ProviderSnapshot, ProviderStatus
from python_swarm.strategy_pipeline import (
    Bar,
    CANONICAL_STRATEGY_IDS,
    CanonicalMarketSnapshot,
    CanonicalRegime,
    CanonicalRegimeDetector,
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
    intermarket_context_from_provider_snapshots,
    macro_context_from_provider_snapshot,
)
from python_swarm.trading_policies import EntryPolicy, FusionPolicy


DAY_MS = 86_400_000
BASE_MS = (1_800_000_000_000 // DAY_MS) * DAY_MS
CAPTURED_MS = BASE_MS + 8 * 3_600_000


def make_bars(
    count: int,
    interval_ms: int,
    *,
    end: Decimal = Decimal("2000.0"),
    step: Decimal = Decimal("0.5"),
    volume: Decimal = Decimal("100"),
) -> tuple[Bar, ...]:
    first_close = end - step * (count - 1)
    first_time = CAPTURED_MS - interval_ms * count
    result = []
    for index in range(count):
        close = first_close + step * index
        open_price = close - step / 2
        result.append(
            Bar(
                open_time_ms=first_time + interval_ms * index,
                close_time_ms=first_time + interval_ms * (index + 1),
                open=open_price,
                high=max(open_price, close) + Decimal("1.0"),
                low=min(open_price, close) - Decimal("1.0"),
                close=close,
                volume=volume,
            )
        )
    return tuple(result)


def healthy_macro(*, sentiment: bool = True, freeze: bool = False) -> MacroContext:
    observation = (
        SentimentObservation(
            source="sentiment-primary",
            source_kind=EvidenceSource.HTTP,
            observed_at_ms=CAPTURED_MS - 1_000,
            sequence=9,
            score=Decimal("0.8"),
        )
        if sentiment
        else None
    )
    return MacroContext(
        source="calendar-primary",
        source_kind=EvidenceSource.HTTP,
        status=ProviderHealth.HEALTHY,
        source_at_ms=CAPTURED_MS - 1_000,
        received_at_ms=CAPTURED_MS - 500,
        sequence=4,
        freshness_ms=60_000,
        coverage_start_ms=CAPTURED_MS - 3_600_000,
        coverage_end_ms=CAPTURED_MS + 90_000_000,
        minimum_forward_coverage_ms=86_400_000,
        freeze_active=freeze,
        sentiment=observation,
    )


def healthy_intermarket() -> IntermarketContext:
    return IntermarketContext(
        source="intermarket-primary",
        source_kind=EvidenceSource.HTTP,
        status=ProviderHealth.HEALTHY,
        received_at_ms=CAPTURED_MS - 100,
        sequence=11,
        freshness_ms=15_000,
        previous=IntermarketObservation(
            CAPTURED_MS - 2_000,
            dxy="100",
            us10y="4.00",
            us30="40000",
            gold="1999.8",
        ),
        current=IntermarketObservation(
            CAPTURED_MS - 1_000,
            dxy="100.10",
            us10y="4.01",
            us30="40005",
            gold="2000.0",
        ),
    )


def snapshot(**changes) -> CanonicalMarketSnapshot:
    values = {
        "snapshot_id": "broker-snapshot-0001",
        "symbol": "XAUUSD",
        "captured_at_ms": CAPTURED_MS,
        "mode": DataMode.DEMO,
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
        "macro": None,
        "intermarket": None,
    }
    values.update(changes)
    return CanonicalMarketSnapshot(**values)


def evaluation(result, agent_id):
    return next(item for item in result.evaluations if item.agent_id == agent_id)


def test_pipeline_module_has_no_io_random_or_wall_clock_imports():
    source_path = Path(pipeline_module.__file__)
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
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


def test_live_data_mode_is_not_exposed_by_this_release():
    with pytest.raises(ValueError):
        DataMode("live")
    assert {mode.value for mode in DataMode}.isdisjoint({"live"})


def test_snapshot_is_defensively_immutable_and_uses_broker_time_for_session():
    bars = list(make_bars(60, 3_600_000))
    market = snapshot(h1=bars)
    bars.clear()

    assert len(market.h1) == 60
    assert market.session is MarketSession.LONDON
    with pytest.raises(FrozenInstanceError):
        market.bid = Decimal("1")


@pytest.mark.parametrize("mode", [DataMode.SHADOW, DataMode.DEMO])
@pytest.mark.parametrize("source", [MarketSource.REPLAY, MarketSource.SUPPLIED_TEST])
def test_broker_modes_reject_non_broker_market_sources(mode, source):
    with pytest.raises(ValueError, match="broker-derived"):
        snapshot(mode=mode, market_source=source)


def test_snapshot_rejects_nonfinite_malformed_and_future_data():
    with pytest.raises(ValueError, match="finite"):
        Bar(CAPTURED_MS - 2, CAPTURED_MS - 1, "NaN", "2", "1", "1.5", "1")
    with pytest.raises(ValueError, match="inconsistent"):
        Bar(CAPTURED_MS - 2, CAPTURED_MS - 1, "2", "1.5", "1", "2", "1")
    future = Bar(CAPTURED_MS - 1, CAPTURED_MS + 1, "2", "3", "1", "2", "1")
    with pytest.raises(ValueError, match="incomplete or future"):
        snapshot(h1=(future,))
    with pytest.raises(ValueError, match="integer"):
        Bar(
            float(CAPTURED_MS - 2),
            CAPTURED_MS - 1,
            "1",
            "2",
            "1",
            "1.5",
            "1",
        )


def test_insufficient_data_has_no_fabricated_regime_or_signal():
    market = snapshot(h1=make_bars(20, 3_600_000), h4=make_bars(10, 4 * 3_600_000))
    decision = CanonicalRegimeDetector().detect(market)
    result = CanonicalStrategyPipeline().evaluate(market)

    assert decision.available is False
    assert decision.assessment is None
    assert decision.reason == "INSUFFICIENT_H1_BARS"
    assert result.signals == ()
    assert len(result.evaluations) == 10
    assert {item.reason for item in result.evaluations} == {"REGIME_UNAVAILABLE"}


def test_regime_contract_contains_exactly_the_documented_eight_states():
    assert {item.value for item in CanonicalRegime} == {
        "trending_bull",
        "trending_bear",
        "ranging_tight",
        "ranging_wide",
        "breakout_imminent",
        "high_impact_news",
        "institutional_hunt",
        "regime_transition",
    }


def test_all_ten_agents_are_represented_once_and_providers_fail_closed():
    result = CanonicalStrategyPipeline().evaluate(snapshot())

    assert tuple(item.agent_id for item in result.evaluations) == CANONICAL_STRATEGY_IDS
    assert evaluation(result, 10).reason == "PROVIDER_MACRO_UNAVAILABLE"
    assert evaluation(result, 12).reason == "PROVIDER_INTERMARKET_UNAVAILABLE"
    assert result.disabled_agents == (10, 12)


def test_agent10_requires_current_coverage_and_genuine_fresh_sentiment():
    pipeline = CanonicalStrategyPipeline()
    result = pipeline.evaluate(snapshot(macro=healthy_macro()))
    signal = evaluation(result, 10).signal

    assert signal is not None
    assert signal.direction is Direction.BUY
    assert "NEWS_SENTIMENT" in signal.strategy_tags
    assert signal.context_enabled is True

    stale = replace(healthy_macro(), source_at_ms=CAPTURED_MS - 60_001)
    stale_result = pipeline.evaluate(snapshot(macro=stale))
    assert evaluation(stale_result, 10).reason == "PROVIDER_MACRO_UNAVAILABLE"

    test_source = replace(healthy_macro(), source_kind=EvidenceSource.SUPPLIED_TEST)
    synthetic_result = pipeline.evaluate(snapshot(macro=test_source))
    assert evaluation(synthetic_result, 10).reason == "PROVIDER_MACRO_UNAVAILABLE"

    no_sentiment = pipeline.evaluate(snapshot(macro=healthy_macro(sentiment=False)))
    assert evaluation(no_sentiment, 10).reason == "NO_SETUP"


def test_healthy_calendar_freeze_creates_news_regime_and_zero_signals():
    result = CanonicalStrategyPipeline().evaluate(snapshot(macro=healthy_macro(freeze=True)))

    assert result.regime.assessment is not None
    assert result.regime.assessment.regime is CanonicalRegime.HIGH_IMPACT_NEWS
    assert result.signals == ()
    assert {item.reason for item in result.evaluations} == {"REGIME_ENTRY_FREEZE"}


def test_agent12_requires_fresh_ordered_intermarket_history():
    pipeline = CanonicalStrategyPipeline()
    result = pipeline.evaluate(snapshot(intermarket=healthy_intermarket()))
    signal = evaluation(result, 12).signal

    assert signal is not None
    assert signal.direction is Direction.BUY
    assert "CORRELATION_DEFLECTION" in signal.strategy_tags

    stale = replace(
        healthy_intermarket(),
        previous=replace(
            healthy_intermarket().previous,
            observed_at_ms=CAPTURED_MS - 16_000,
        ),
        current=replace(
            healthy_intermarket().current,
            observed_at_ms=CAPTURED_MS - 15_001,
        ),
    )
    stale_result = pipeline.evaluate(snapshot(intermarket=stale))
    assert evaluation(stale_result, 12).reason == "PROVIDER_INTERMARKET_UNAVAILABLE"

    old_baseline = replace(
        healthy_intermarket(),
        previous=replace(
            healthy_intermarket().previous,
            observed_at_ms=CAPTURED_MS - 30_000,
        ),
    )
    baseline_result = pipeline.evaluate(snapshot(intermarket=old_baseline))
    assert evaluation(baseline_result, 12).reason == "PROVIDER_INTERMARKET_UNAVAILABLE"


def test_signal_ids_are_deterministic_unique_and_geometry_is_directional():
    market = snapshot(macro=healthy_macro(), intermarket=healthy_intermarket())
    pipeline = CanonicalStrategyPipeline()
    first = pipeline.evaluate(market)
    second = pipeline.evaluate(market)

    assert first == second
    assert first.signals
    assert {item.agent_id for item in first.signals} == {4, 6, 9, 10, 12}
    assert len({item.signal_id for item in first.signals}) == len(first.signals)
    for signal in first.signals:
        assert signal.snapshot_id == market.snapshot_id
        assert signal.observed_at_ms == market.captured_at_ms
        assert EntryPolicy.validate_levels(signal.direction, signal.levels).valid
        assert f"AGENT_{signal.agent_id:02d}" in signal.strategy_tags
        assert any(tag.startswith("REGIME:") for tag in signal.strategy_tags)
        assert any(tag.startswith("SESSION:") for tag in signal.strategy_tags)


def test_remaining_default_analyzers_each_have_a_reachable_bounded_setup():
    pipeline = CanonicalStrategyPipeline()

    descending = list(make_bars(60, 3_600_000, end=Decimal("2000"), step=Decimal("-0.2")))
    descending[-1] = replace(
        descending[-1],
        open=Decimal("2000.5"),
        high=Decimal("2003"),
        low=Decimal("1999.5"),
        close=Decimal("2002"),
    )
    agent05 = pipeline.evaluate(
        snapshot(h1=tuple(descending), bid="2002.0", ask="2002.1")
    )
    assert evaluation(agent05, 5).signal is not None
    assert "TRENDLINE_BREAKOUT" in evaluation(agent05, 5).signal.strategy_tags

    fvg = list(make_bars(40, 15 * 60_000))
    fvg[-3] = replace(
        fvg[-3], open=Decimal("1997"), high=Decimal("1998"), low=Decimal("1996"), close=Decimal("1997.5")
    )
    fvg[-1] = replace(
        fvg[-1], open=Decimal("1999"), high=Decimal("2000"), low=Decimal("1999"), close=Decimal("1999.5")
    )
    agent07 = pipeline.evaluate(snapshot(m15=tuple(fvg)))
    assert evaluation(agent07, 7).signal is not None
    assert "ICT_SMC_CORE" in evaluation(agent07, 7).signal.strategy_tags

    profile = list(make_bars(60, 3_600_000))
    profile[-1] = replace(profile[-1], volume=Decimal("10000"))
    agent08 = pipeline.evaluate(snapshot(h1=tuple(profile)))
    assert evaluation(agent08, 8).signal is not None
    assert "VPVR_POC" in evaluation(agent08, 8).signal.strategy_tags

    extreme = list(make_bars(60, 3_600_000, end=Decimal("1990"), step=Decimal("0")))
    extreme[-1] = replace(
        extreme[-1],
        open=Decimal("1990"),
        high=Decimal("2001"),
        low=Decimal("1989"),
        close=Decimal("2000"),
    )
    agent11 = pipeline.evaluate(snapshot(h1=tuple(extreme)))
    assert evaluation(agent11, 11).signal is not None
    assert "MEAN_REVERSION_VOL_BANDS" in evaluation(agent11, 11).signal.strategy_tags

    schematic = list(make_bars(50, 5 * 60_000))
    schematic[-1] = replace(schematic[-1], volume=Decimal("1000"))
    agent13 = pipeline.evaluate(snapshot(m5=tuple(schematic)))
    assert evaluation(agent13, 13).signal is not None
    assert "WYCKOFF_SCHEMATIC" in evaluation(agent13, 13).signal.strategy_tags


def test_invalid_analyzer_geometry_is_rejected_before_signal_output():
    analyzers = dict(pipeline_module.DEFAULT_ANALYZERS)

    def invalid_geometry(_snapshot, _regime):
        return StrategyCandidate(Direction.BUY, "80", "-1", ("BAD",))

    analyzers[4] = invalid_geometry
    result = CanonicalStrategyPipeline(analyzers=analyzers).evaluate(snapshot())

    assert evaluation(result, 4).reason == "INVALID_GEOMETRY"
    assert evaluation(result, 4).signal is None


def test_analyzer_exception_fails_only_that_agent_closed():
    analyzers = dict(pipeline_module.DEFAULT_ANALYZERS)

    def broken(_snapshot, _regime):
        raise RuntimeError("must not escape strategy boundary")

    analyzers[4] = broken
    result = CanonicalStrategyPipeline(analyzers=analyzers).evaluate(snapshot())

    assert evaluation(result, 4).reason == "ANALYZER_REJECTED_INPUT"
    assert evaluation(result, 4).signal is None
    assert len(result.evaluations) == 10


def test_pipeline_output_is_directly_consumable_by_canonical_fusion():
    def buying_agent(_snapshot, _regime):
        return StrategyCandidate(Direction.BUY, "80", "2.0", ("TEST_SETUP",))

    analyzers = {agent_id: buying_agent for agent_id in CANONICAL_STRATEGY_IDS}
    market = snapshot(macro=healthy_macro(), intermarket=healthy_intermarket())
    result = CanonicalStrategyPipeline(analyzers=analyzers).evaluate(market)
    decision = FusionPolicy(clock=FixedClock(CAPTURED_MS)).evaluate(
        result.signals,
        fusion_id="fusion-pipeline-contract",
    )

    assert len(result.signals) == 10
    assert decision.accepted is True
    assert decision.result is not None
    assert tuple(item.agent_id for item in decision.result.contributors) == CANONICAL_STRATEGY_IDS


def test_macro_provider_adapter_never_invents_sentiment():
    now_s = CAPTURED_MS / 1000
    provider = ProviderSnapshot(
        provider="macro",
        source="calendar-primary",
        status=ProviderStatus.HEALTHY,
        source_timestamp=now_s - 1,
        received_timestamp=now_s - 0.5,
        sequence=2,
        freshness_s=60,
        payload={
            "coverage_start_utc": now_s - 3_600,
            "coverage_end_utc": now_s + 90_000,
            "minimum_forward_coverage_s": 86_400,
            "events": [],
        },
    )

    context = macro_context_from_provider_snapshot(
        provider,
        source_kind=EvidenceSource.HTTP,
        at_ms=CAPTURED_MS,
    )

    assert context.healthy_covered_at(CAPTURED_MS, DataMode.DEMO)
    assert context.sentiment is None
    result = CanonicalStrategyPipeline().evaluate(snapshot(macro=context))
    assert evaluation(result, 10).signal is None


def test_intermarket_provider_adapter_requires_two_monotonic_snapshots():
    now_s = CAPTURED_MS / 1000

    def provider(sequence, source_time, dxy):
        return ProviderSnapshot(
            provider="intermarket",
            source="intermarket-primary",
            status=ProviderStatus.HEALTHY,
            source_timestamp=source_time,
            received_timestamp=source_time + 0.1,
            sequence=sequence,
            freshness_s=15,
            payload={"dxy": dxy, "us10y": 4, "us30": 40_000, "gold": 2_000},
        )

    previous = provider(1, now_s - 2, 100)
    current = provider(2, now_s - 1, 100.1)
    context = intermarket_context_from_provider_snapshots(
        previous,
        current,
        source_kind=EvidenceSource.HTTP,
    )
    assert context.healthy_fresh_at(CAPTURED_MS, DataMode.DEMO)

    with pytest.raises(ValueError, match="sequences must increase"):
        intermarket_context_from_provider_snapshots(
            current,
            previous,
            source_kind=EvidenceSource.HTTP,
        )
