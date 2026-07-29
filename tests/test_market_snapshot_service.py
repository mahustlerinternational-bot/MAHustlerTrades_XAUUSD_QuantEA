import asyncio
from decimal import Decimal

import pytest

from python_swarm.broker_gateway import BrokerTick, CompletedBar
from python_swarm.domain_models import SymbolSnapshot
from python_swarm.market_snapshot_service import (
    MAXIMUM_LAST_BAR_AGE_MS,
    MINIMUM_BARS,
    TIMEFRAME_MS,
    MarketSnapshotAssembler,
    MarketSnapshotAssemblerConfig,
    ProviderContexts,
    ProviderCoordinator,
    SnapshotAssemblyError,
)
from python_swarm.provider_adapters import (
    ExecutionProtection,
    ProviderSnapshot,
    ProviderStatus,
)
from python_swarm.reconciliation import AuthoritativeBrokerSnapshot, BrokerRiskSnapshot
from python_swarm.runtime_state import BrokerIdentity, BrokerTradeMode
from python_swarm.strategy_pipeline import (
    DataMode,
    EvidenceSource,
    macro_context_from_provider_snapshot,
)
from python_swarm.trade_ledger import TradeLedger


NOW = 1_800_000_000_000


def provider_snapshot(provider: str, sequence: int, timestamp: float, payload):
    return ProviderSnapshot(
        provider=provider,
        source=f"{provider}-test-http",
        status=ProviderStatus.HEALTHY,
        source_timestamp=timestamp,
        received_timestamp=timestamp + 0.1,
        sequence=sequence,
        freshness_s=900 if provider == "macro" else 15,
        payload=payload,
        dependent_agents=(10, 14) if provider == "macro" else (12, 15),
        unavailable_protection=(
            ExecutionProtection.BLOCK_NEW_ENTRIES
            if provider == "macro"
            else ExecutionProtection.DISABLE_DEPENDENT_AGENTS
        ),
    )


def disabled(provider: str) -> ProviderSnapshot:
    return ProviderSnapshot(
        provider=provider,
        source="unconfigured",
        status=ProviderStatus.DISABLED,
        source_timestamp=None,
        received_timestamp=NOW / 1000,
        sequence=1,
        freshness_s=60,
        payload={},
    )


def test_macro_freeze_is_recomputed_at_each_decision_tick_boundary():
    scheduled_ms = NOW + 15 * 60_000 + 1
    snapshot = provider_snapshot(
        "macro",
        1,
        (NOW - 100) / 1000,
        {
            "coverage_start_utc": (NOW - 60_000) / 1000,
            "coverage_end_utc": (NOW + 3_600_000) / 1000,
            "events": [
                {
                    "impact": "HIGH",
                    "currency": "USD",
                    "scheduled_utc": scheduled_ms / 1000,
                }
            ],
        },
    )
    polled = macro_context_from_provider_snapshot(
        snapshot, source_kind=EvidenceSource.HTTP, at_ms=NOW
    )
    assert polled.freeze_active is False

    contexts = ProviderContexts(
        polled,
        None,
        snapshot,
        disabled("intermarket"),
        snapshot,
    )
    refreshed = contexts.refreshed(at_ms=NOW + 2)
    assert refreshed.macro is not None
    assert refreshed.macro.freeze_active is True


class SequenceProvider:
    def __init__(self, values):
        self.values = list(values)

    async def poll_once(self):
        return self.values.pop(0)


def bars():
    result = []
    for timeframe, interval in TIMEFRAME_MS.items():
        count = MINIMUM_BARS[timeframe]
        last_close = NOW - min(interval, MAXIMUM_LAST_BAR_AGE_MS[timeframe])
        first_open = last_close - count * interval
        for index in range(count):
            open_time = first_open + index * interval
            result.append(
                CompletedBar(
                    symbol="XAUUSD",
                    timeframe=timeframe,
                    open_time_ms=open_time,
                    close_time_ms=open_time + interval,
                    captured_at_ms=NOW,
                    open=Decimal("2500"),
                    high=Decimal("2502"),
                    low=Decimal("2498"),
                    close=Decimal("2501"),
                    volume=100,
                )
            )
    return tuple(result)


def assembly_inputs():
    identity = BrokerIdentity(
        account_id="demo-1",
        broker_server="Broker-Demo",
        trade_mode=BrokerTradeMode.DEMO,
        symbol="XAUUSD",
        magic=20260715,
        connection_epoch="epoch-1",
        trade_allowed=True,
        expert_allowed=True,
    )
    authoritative = AuthoritativeBrokerSnapshot(
        snapshot_id="broker-snapshot-1",
        identity=identity,
        risk=BrokerRiskSnapshot(
            risk_day="2027-01-15",
            balance="10000",
            equity="10000",
            day_start_equity="10000",
            baseline_verified=True,
            daily_halted=False,
        ),
        positions=(),
        pending_orders=(),
        captured_at_ms=NOW - 500,
        payload={"account": {"account_mode": "HEDGING", "currency": "USD"}},
    )
    account = {
        "received_at_ms": NOW - 100,
        "account_id": "demo-1",
        "symbol": "XAUUSD",
        "balance": Decimal("10000"),
        "equity": Decimal("9999"),
        "free_margin": Decimal("9900"),
        "risk_day": "2027-01-15",
        "baseline_verified": True,
        "daily_halted": False,
        "owned_positions": 0,
        "owned_pending_orders": 0,
    }
    symbol = SymbolSnapshot(
        symbol="XAUUSD",
        digits=2,
        tick_size="0.01",
        tick_value_per_lot="1",
        contract_size="100",
        volume_min="0.01",
        volume_step="0.01",
        volume_max="100",
    )
    tick = BrokerTick(
        msg_id="tick-1",
        connection_epoch="epoch-1",
        symbol="XAUUSD",
        received_at_ms=NOW,
        message_ts_ms=NOW,
        broker_time_ms=NOW - 20,
        bid="2500.00",
        ask="2500.10",
        spread_points="10",
        volume=3,
    )
    providers = ProviderContexts(None, None, disabled("macro"), disabled("intermarket"))
    return authoritative, account, symbol, tick, providers


def test_same_tick_assembles_market_and_broker_with_exact_identity():
    authoritative, account, symbol, tick, providers = assembly_inputs()
    bundle = MarketSnapshotAssembler(
        MarketSnapshotAssemblerConfig(DataMode.DEMO)
    ).assemble(
        now_ms=NOW,
        connection_epoch="epoch-1",
        account_health=account,
        symbol=symbol,
        tick=tick,
        completed_bars=bars(),
        authoritative=authoritative,
        reconciliation_succeeded=True,
        active_quarantine=False,
        providers=providers,
    )

    assert bundle.market.snapshot_id == bundle.broker.snapshot_id
    assert bundle.market.captured_at_ms == bundle.broker.captured_at_ms == NOW
    assert bundle.broker.quote.captured_at_ms == NOW
    assert bundle.broker.account.captured_at_ms == NOW - 100
    assert len(bundle.market.h1) == MINIMUM_BARS["H1"]


@pytest.mark.parametrize("failure", ["epoch", "count", "stale", "quarantine", "bars"])
def test_assembly_fails_closed_on_cross_run_or_incomplete_evidence(failure):
    authoritative, account, symbol, tick, providers = assembly_inputs()
    kwargs = {
        "now_ms": NOW,
        "connection_epoch": "epoch-1",
        "account_health": account,
        "symbol": symbol,
        "tick": tick,
        "completed_bars": bars(),
        "authoritative": authoritative,
        "reconciliation_succeeded": True,
        "active_quarantine": False,
        "providers": providers,
    }
    if failure == "epoch":
        kwargs["connection_epoch"] = "epoch-other"
    elif failure == "count":
        kwargs["account_health"] = {**account, "owned_positions": 1}
    elif failure == "stale":
        kwargs["now_ms"] = NOW + 5_001
    elif failure == "quarantine":
        kwargs["active_quarantine"] = True
    else:
        kwargs["completed_bars"] = tuple(
            item for item in bars() if item.timeframe != "H1"
        )

    with pytest.raises(SnapshotAssemblyError):
        MarketSnapshotAssembler(
            MarketSnapshotAssemblerConfig(DataMode.DEMO)
        ).assemble(**kwargs)


def test_provider_coordinator_requires_two_ordered_intermarket_observations(tmp_path):
    async def scenario():
        now_s = NOW / 1000
        macro_payload = {
            "events": [],
            "coverage_start_utc": now_s - 3600,
            "coverage_end_utc": now_s + 90_000,
            "minimum_forward_coverage_s": 86_400,
        }
        macro_values = [
            provider_snapshot("macro", 1, now_s - 1, macro_payload),
            provider_snapshot("macro", 2, now_s - 0.5, macro_payload),
        ]
        intermarket_values = [
            provider_snapshot(
                "intermarket", 1, now_s - 2, {"dxy": 100, "us10y": 4, "us30": 40000, "gold": 2500}
            ),
            provider_snapshot(
                "intermarket", 2, now_s - 1, {"dxy": 99.9, "us10y": 3.9, "us30": 40100, "gold": 2501}
            ),
        ]
        with TradeLedger(tmp_path / "providers.db") as ledger:
            coordinator = ProviderCoordinator(
                macro=SequenceProvider(macro_values),
                intermarket=SequenceProvider(intermarket_values),
                ledger=ledger,
            )
            first = await coordinator.poll_once(at_ms=NOW)
            assert first.macro is not None and first.intermarket is None
            second = await coordinator.poll_once(at_ms=NOW + 200)
            assert second.intermarket is not None
            assert second.intermarket.previous.observed_at_ms < second.intermarket.current.observed_at_ms
            assert ledger.latest_provider_health("macro")["state"] == "HEALTHY"
            assert ledger.latest_provider_health("intermarket")["state"] == "HEALTHY"

    asyncio.run(scenario())


def test_provider_coordinator_resets_intermarket_continuity_after_outage(
    tmp_path,
):
    async def scenario():
        now_s = NOW / 1000
        macro_payload = {
            "events": [],
            "coverage_start_utc": now_s - 3600,
            "coverage_end_utc": now_s + 90_000,
            "minimum_forward_coverage_s": 86_400,
        }
        macro_values = [
            provider_snapshot(
                "macro",
                sequence,
                now_s - 0.1 * sequence,
                macro_payload,
            )
            for sequence in range(1, 5)
        ]

        first = provider_snapshot(
            "intermarket",
            1,
            now_s - 4,
            {
                "dxy": 100,
                "us10y": 4,
                "us30": 40000,
                "gold": 2500,
            },
        )

        outage = ProviderSnapshot(
            provider="intermarket",
            source="intermarket-test-http",
            status=ProviderStatus.ERROR,
            source_timestamp=None,
            received_timestamp=now_s - 2,
            sequence=2,
            freshness_s=15,
            payload={},
            disabled_agents=(12, 15),
            execution_protection=(
                ExecutionProtection.DISABLE_DEPENDENT_AGENTS
            ),
            error="provider unavailable",
            dependent_agents=(12, 15),
            unavailable_protection=(
                ExecutionProtection.DISABLE_DEPENDENT_AGENTS
            ),
        )

        second = provider_snapshot(
            "intermarket",
            3,
            now_s - 1,
            {
                "dxy": 99.9,
                "us10y": 3.9,
                "us30": 40100,
                "gold": 2501,
            },
        )

        third = provider_snapshot(
            "intermarket",
            4,
            now_s,
            {
                "dxy": 99.8,
                "us10y": 3.8,
                "us30": 40200,
                "gold": 2502,
            },
        )

        with TradeLedger(tmp_path / "provider-outage.db") as ledger:
            coordinator = ProviderCoordinator(
                macro=SequenceProvider(macro_values),
                intermarket=SequenceProvider(
                    [first, outage, second, third]
                ),
                ledger=ledger,
            )

            first_context = await coordinator.poll_once(at_ms=NOW)
            assert first_context.intermarket is None

            outage_context = await coordinator.poll_once(
                at_ms=NOW + 100
            )
            assert outage_context.intermarket is None

            first_recovery = await coordinator.poll_once(
                at_ms=NOW + 200
            )
            assert first_recovery.intermarket is None

            recovered = await coordinator.poll_once(
                at_ms=NOW + 300
            )
            assert recovered.intermarket is not None
            assert (
                recovered.intermarket.previous.observed_at_ms
                < recovered.intermarket.current.observed_at_ms
            )

    asyncio.run(scenario())
