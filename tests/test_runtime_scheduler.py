import asyncio
from decimal import Decimal
from types import SimpleNamespace

from python_swarm.broker_gateway import BrokerTick
from python_swarm.decision_service import DecisionReason, DecisionResult
from python_swarm.domain_models import (
    AccountMode,
    AccountSnapshot,
    BrokerSnapshot,
    ContributorAttribution,
    Direction,
    EntryLevels,
    FixedClock,
    LifecycleActionKind,
    Milestone,
    QuoteSnapshot,
    ReservationState,
    RiskReservation,
    SymbolSnapshot,
    TradeIntent,
)
from python_swarm.execution_service import SubmissionState
from python_swarm.lifecycle_service import (
    LifecycleDisposition,
    LifecycleResult,
)
from python_swarm.market_snapshot_service import (
    MarketSnapshotBundle,
    ProviderContexts,
    SnapshotAssemblyError,
)
from python_swarm.provider_adapters import (
    ExecutionProtection,
    ProviderSnapshot,
    ProviderStatus,
)
from python_swarm.runtime_config import RuntimeConfig, RuntimeMode
from python_swarm.runtime_scheduler import (
    CanonicalTickScheduler,
    LearningRuntimeStatus,
)
from python_swarm.runtime_state import ControlPlaneState
from python_swarm.strategy_pipeline import (
    CanonicalMarketSnapshot,
    DataMode,
    MarketSource,
)
from python_swarm.trade_ledger import TradeLedger
from python_swarm.trading_policies import RiskDecision


NOW = 1_784_116_800_000
ACCOUNT = "demo-10001"
SYMBOL = "XAUUSD"
MAGIC = 202412
EPOCH = "epoch-1"
CONFIG_HASH = "a" * 64
SECRET_ENV = {"QSX_IPC_SECRET": "s" * 40}


def run(awaitable):
    return asyncio.run(awaitable)


def make_config(tmp_path, mode: RuntimeMode) -> RuntimeConfig:
    raw = {
        "schema_version": 2,
        "mode": mode.value,
        "system": {
            "symbol": SYMBOL,
            "magic_number": MAGIC,
            "instance_id": f"scheduler-{mode.value}",
            "expected_account_id": ACCOUNT,
            "expected_broker_server": "Broker-Demo",
            "expected_account_trade_mode": "DEMO",
            "permit_real_account": False,
        },
        "storage": {"data_dir": str(tmp_path)},
        "ipc": {"auth_secret_env": "QSX_IPC_SECRET"},
        "providers": {
            "macro": {"enabled": False, "source": "disabled"},
            "intermarket": {"enabled": False, "source": "disabled"},
            "unsafe_allow_technical_without_macro": mode is RuntimeMode.DEMO,
        },
    }
    return RuntimeConfig.from_mapping(raw, environ=SECRET_ENV)


def symbol_snapshot() -> SymbolSnapshot:
    return SymbolSnapshot(
        symbol=SYMBOL,
        digits=1,
        tick_size="0.1",
        tick_value_per_lot="1",
        contract_size="100",
        volume_min="0.01",
        volume_step="0.01",
        volume_max="5",
        stops_level_points=5,
        freeze_level_points=5,
        margin_per_lot="100",
    )


def tick(*, msg_id: str = "tick-1", received_at_ms: int = NOW) -> BrokerTick:
    return BrokerTick(
        msg_id=msg_id,
        connection_epoch=EPOCH,
        symbol=SYMBOL,
        received_at_ms=received_at_ms,
        message_ts_ms=received_at_ms - 1,
        broker_time_ms=received_at_ms - 2,
        bid="2000.0",
        ask="2000.1",
        spread_points="1",
        volume=10,
    )


def reconciliation():
    identity = SimpleNamespace(
        connection_epoch=EPOCH,
        account_id=ACCOUNT,
        symbol=SYMBOL,
        magic=MAGIC,
    )
    return SimpleNamespace(
        connection_epoch=EPOCH,
        succeeded=True,
        snapshot=SimpleNamespace(identity=identity),
    )


def provider_contexts() -> ProviderContexts:
    macro = ProviderSnapshot(
        provider="macro",
        source="disabled",
        status=ProviderStatus.DISABLED,
        source_timestamp=None,
        received_timestamp=NOW / 1_000,
        sequence=1,
        freshness_s=300,
        disabled_agents=(10, 14),
        execution_protection=ExecutionProtection.TECHNICAL_ONLY_UNPROTECTED,
        unavailable_protection=ExecutionProtection.TECHNICAL_ONLY_UNPROTECTED,
    )
    intermarket = ProviderSnapshot(
        provider="intermarket",
        source="disabled",
        status=ProviderStatus.DISABLED,
        source_timestamp=None,
        received_timestamp=NOW / 1_000,
        sequence=1,
        freshness_s=15,
        disabled_agents=(12, 15),
        execution_protection=ExecutionProtection.DISABLE_DEPENDENT_AGENTS,
        unavailable_protection=ExecutionProtection.DISABLE_DEPENDENT_AGENTS,
    )
    return ProviderContexts(None, None, macro, intermarket)


def bundle(mode: RuntimeMode, *, snapshot_id: str = "exact-bundle-snapshot"):
    market = CanonicalMarketSnapshot(
        snapshot_id=snapshot_id,
        symbol=SYMBOL,
        captured_at_ms=NOW,
        mode=DataMode(mode.value),
        market_source=MarketSource.BROKER,
        bid="2000.0",
        ask="2000.1",
        tick_size="0.1",
    )
    account = AccountSnapshot(
        account_id=ACCOUNT,
        currency="USD",
        mode=AccountMode.HEDGING,
        balance="10000",
        equity="10000",
        free_margin="10000",
        risk_day="2026-07-15",
        day_start_equity="10000",
        baseline_verified=True,
        daily_halted=False,
        captured_at_ms=NOW,
    )
    broker = BrokerSnapshot(
        snapshot_id=snapshot_id,
        connection_id=EPOCH,
        account=account,
        symbol=symbol_snapshot(),
        quote=QuoteSnapshot(SYMBOL, "2000.0", "2000.1", NOW),
        positions=(),
        captured_at_ms=NOW,
        reconciled=True,
    )
    return MarketSnapshotBundle(market, broker)


def approved_decision(*, snapshot_id: str = "exact-bundle-snapshot") -> DecisionResult:
    levels = EntryLevels("2000.1", "1995.0", "2005.0", "2010.0", "2015.0")
    contributors = tuple(
        ContributorAttribution(
            signal_id=f"signal-{agent_id}",
            agent_id=agent_id,
            direction=Direction.BUY,
            conviction="80",
            weight="1",
            weighted_score="80",
            strategy_tags=("TEST",),
            snapshot_id=snapshot_id,
            observed_at_ms=NOW,
        )
        for agent_id in (4, 5, 6)
    )
    intent = TradeIntent(
        intent_id="intent-1",
        trade_id="trade-1",
        fusion_id="fusion-1",
        symbol=SYMBOL,
        magic=MAGIC,
        direction=Direction.BUY,
        levels=levels,
        reference_price="2000.1",
        max_slippage_points=100,
        requested_risk_pct="1",
        contributors=contributors,
        regime="TRENDING",
        session="LONDON",
        created_at_ms=NOW,
        expires_at_ms=NOW + 10_000,
        config_hash=CONFIG_HASH,
        decision_id="decision-approved",
        snapshot_id=snapshot_id,
    )
    reservation = RiskReservation(
        reservation_id="reservation-1",
        trade_id="trade-1",
        account_id=ACCOUNT,
        direction=Direction.BUY,
        risk_day="2026-07-15",
        risk_amount="100",
        volume="0.2",
        state=ReservationState.RESERVED,
        created_at_ms=NOW,
        expires_at_ms=NOW + 30_000,
    )
    return DecisionResult(
        approved=True,
        reason=DecisionReason.APPROVED,
        detail_code="APPROVED",
        decision_id="decision-approved",
        provider_gate=object(),
        pipeline_result=object(),
        fusion_result=object(),
        intent=intent,
        risk_decision=RiskDecision(
            approved=True,
            reason="APPROVED",
            volume=reservation.volume,
            risk_amount=reservation.risk_amount,
            risk_pct=Decimal("1"),
            reservation=reservation,
        ),
    )


class StubLedger(TradeLedger):
    def __init__(self, tmp_path, positions=()):
        super().__init__(tmp_path / "scheduler-unused.sqlite")
        self.positions = list(positions)
        self.pending_orders = []
        self.reservations = []
        self.events = []
        self.allocations = []

    def list_owned_positions(self, account_id, symbol, magic):
        assert (account_id, symbol, magic) == (ACCOUNT, SYMBOL, MAGIC)
        return list(self.positions)

    def list_pending_orders(self, account_id, symbol, magic):
        assert (account_id, symbol, magic) == (ACCOUNT, SYMBOL, MAGIC)
        return list(self.pending_orders)

    def has_active_quarantined_exposure(self, account_id, symbol, magic):
        assert (account_id, symbol, magic) == (ACCOUNT, SYMBOL, MAGIC)
        return False

    def list_active_risk_reservations(self, account_id, risk_day):
        assert (account_id, risk_day) == (ACCOUNT, "2026-07-15")
        return list(self.reservations)

    def allocate_command_sequence(
        self, connection_epoch, allocation_key, *, allocated_at_ms
    ):
        self.allocations.append(
            (connection_epoch, allocation_key, allocated_at_ms)
        )
        return 7

    def append_event_with_outbox(self, event, outbox):
        self.events.append((event, tuple(outbox)))
        return True


class RuntimeStub:
    state = ControlPlaneState.READY
    evidence = SimpleNamespace(
        ownership_key=(ACCOUNT, "Broker-Demo", SYMBOL, MAGIC),
        identity_verified=True,
        tuple_lock_acquired=True,
    )
    is_ready = True

    def __init__(self):
        self.halts = []

    def halt(self, reason):
        self.halts.append(reason)


class GatewayStub:
    connection_epoch = EPOCH
    latest_symbol_snapshot = symbol_snapshot()
    completed_bars = ()

    def __init__(self):
        self.latest_account_health = {
            "account_id": ACCOUNT,
            "symbol": SYMBOL,
        }
        self.send_calls = []

    async def next_tick(self, timeout=None):
        del timeout
        return tick()

    async def send(self, command):
        self.send_calls.append(command)
        raise AssertionError("scheduler must never bypass ExecutionService")


class ExecutionSpy:
    def __init__(self, *, reject_submit=False):
        self.plans = []
        self.reject_submit = reject_submit

    async def submit_open(self, plan):
        if self.reject_submit:
            raise AssertionError("SHADOW/lifecycle-priority paths cannot submit")
        self.plans.append(plan)
        return SubmissionState.SENT_AWAITING_ACK

    def expire_sent_commands(self):
        return ()


class LifecycleSpy:
    def __init__(self, calls, result_factory):
        self.calls = calls
        self.result_factory = result_factory

    async def evaluate(self, position, **kwargs):
        self.calls.append(f"lifecycle:{position['broker_position_id']}")
        assert kwargs["account_id"] == ACCOUNT
        assert kwargs["connection_epoch"] == EPOCH
        assert kwargs["quote"].captured_at_ms == NOW
        return self.result_factory(position)


class AssemblerSpy:
    def __init__(self, calls, result):
        self.calls = calls
        self.result = result
        self.kwargs = None

    def assemble(self, **kwargs):
        self.calls.append("assemble")
        self.kwargs = kwargs
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class DecisionSpy:
    def __init__(self, result):
        self.result = result
        self.requests = []

    def decide(self, request):
        self.requests.append(request)
        return self.result


def scheduler(
    tmp_path,
    *,
    mode,
    ledger,
    lifecycle,
    assembler,
    decision,
    execution,
    learning=None,
    clock=None,
):
    return CanonicalTickScheduler(
        config=make_config(tmp_path, mode),
        config_hash=CONFIG_HASH,
        ledger=ledger,
        gateway=GatewayStub(),
        runtime=RuntimeStub(),
        execution=execution,
        lifecycle=lifecycle,
        clock=clock or FixedClock(NOW),
        assembler=assembler,
        decision=decision,
        learning=learning,
    )


def no_action(position):
    return LifecycleResult(
        disposition=LifecycleDisposition.NO_ACTION,
        trade_id=position["trade_id"],
        reason="NO_ACTION",
    )


def test_lifecycle_runs_for_every_position_before_entry_snapshot_failure(tmp_path):
    calls = []
    ledger = StubLedger(
        tmp_path,
        positions=(
            {"broker_position_id": "position-2", "trade_id": "trade-2"},
            {"broker_position_id": "position-1", "trade_id": "trade-1"},
        ),
    )
    assembler = AssemblerSpy(calls, SnapshotAssemblyError("missing M1 history"))
    execution = ExecutionSpy(reject_submit=True)
    service = scheduler(
        tmp_path,
        mode=RuntimeMode.DEMO,
        ledger=ledger,
        lifecycle=LifecycleSpy(calls, no_action),
        assembler=assembler,
        decision=DecisionSpy(None),
        execution=execution,
    )

    result = run(
        service.process_tick(
            tick(), providers=provider_contexts(), reconciliation=reconciliation()
        )
    )

    assert calls == ["lifecycle:position-1", "lifecycle:position-2", "assemble"]
    assert len(result.lifecycle) == 2
    assert result.decision is None
    assert result.entry_blocker == "SnapshotAssemblyError:missing M1 history"
    assert [item[0].event_type for item in ledger.events] == [
        "ENTRY_SNAPSHOT_BLOCKED"
    ]
    assert execution.plans == []


def test_any_lifecycle_action_suppresses_same_tick_entry(tmp_path):
    calls = []
    ledger = StubLedger(
        tmp_path,
        positions=({"broker_position_id": "position-1", "trade_id": "trade-1"},),
    )

    def protective_action(position):
        return LifecycleResult(
            disposition=LifecycleDisposition.SENT_AWAITING_ACK,
            trade_id=position["trade_id"],
            reason="BREAK_EVEN_DUE",
            action=LifecycleActionKind.MODIFY_SL,
            milestone=Milestone.BREAK_EVEN,
            command_id="lifecycle-command-1",
        )

    service = scheduler(
        tmp_path,
        mode=RuntimeMode.DEMO,
        ledger=ledger,
        lifecycle=LifecycleSpy(calls, protective_action),
        assembler=AssemblerSpy(calls, AssertionError("entry must be suppressed")),
        decision=DecisionSpy(None),
        execution=ExecutionSpy(reject_submit=True),
    )

    result = run(
        service.process_tick(
            tick(), providers=provider_contexts(), reconciliation=reconciliation()
        )
    )

    assert calls == ["lifecycle:position-1"]
    assert result.entry_blocker == "LIFECYCLE_PRIORITY"
    assert result.lifecycle[0].command_id == "lifecycle-command-1"
    assert [item[0].event_type for item in ledger.events] == [
        "ENTRY_SKIPPED_LIFECYCLE_PRIORITY"
    ]


def test_shadow_is_pure_observation_and_retains_contributor_lineage(tmp_path):
    calls = []
    ledger = StubLedger(tmp_path)
    decision = DecisionSpy(approved_decision())
    execution = ExecutionSpy(reject_submit=True)
    gateway = GatewayStub()
    service = CanonicalTickScheduler(
        config=make_config(tmp_path, RuntimeMode.SHADOW),
        config_hash=CONFIG_HASH,
        ledger=ledger,
        gateway=gateway,
        runtime=RuntimeStub(),
        execution=execution,
        lifecycle=LifecycleSpy(calls, no_action),
        clock=FixedClock(NOW),
        assembler=AssemblerSpy(calls, bundle(RuntimeMode.SHADOW)),
        decision=decision,
    )

    result = run(
        service.process_tick(
            tick(), providers=provider_contexts(), reconciliation=reconciliation()
        )
    )

    request = decision.requests[0]
    assert request.observation_only is True
    assert request.identity.execution_authorized is False
    assert result.entry_blocker == "SHADOW_OBSERVATION_ONLY"
    assert result.submission is None and result.command_id is None
    assert execution.plans == [] and gateway.send_calls == []
    event = ledger.events[0][0]
    assert event.event_type == "ENTRY_SIGNAL_APPROVED_OBSERVATION"
    assert event.payload["send_capability"] is False
    assert tuple(item["agent_id"] for item in event.payload["contributors"]) == (
        4,
        5,
        6,
    )
    assert all(
        item["snapshot_id"] == "exact-bundle-snapshot"
        and item["observed_at_ms"] == NOW
        for item in event.payload["contributors"]
    )


def test_rejected_signal_uses_exact_assembled_snapshot_and_decision_time(tmp_path):
    calls = []
    ledger = StubLedger(tmp_path)
    rejected = DecisionResult(
        approved=False,
        reason=DecisionReason.FUSION_REJECTED,
        detail_code="INSUFFICIENT_AGREEMENT",
        decision_id="decision-rejected",
    )
    decision = DecisionSpy(rejected)
    assembler = AssemblerSpy(
        calls,
        bundle(RuntimeMode.SHADOW, snapshot_id="assembler-owned-snapshot-id"),
    )
    decision_time = NOW + 2_000
    service = scheduler(
        tmp_path,
        mode=RuntimeMode.SHADOW,
        ledger=ledger,
        lifecycle=LifecycleSpy(calls, no_action),
        assembler=assembler,
        decision=decision,
        execution=ExecutionSpy(reject_submit=True),
        clock=FixedClock(decision_time),
    )

    run(
        service.process_tick(
            tick(), providers=provider_contexts(), reconciliation=reconciliation()
        )
    )

    assert assembler.kwargs["now_ms"] == decision_time
    assert ledger.events[0][0].payload["snapshot_id"] == (
        "assembler-owned-snapshot-id"
    )


def test_demo_approved_entry_builds_one_deterministic_execution_plan(tmp_path):
    calls = []
    ledger = StubLedger(tmp_path)
    execution = ExecutionSpy()
    decision = DecisionSpy(approved_decision())
    gateway = GatewayStub()
    service = CanonicalTickScheduler(
        config=make_config(tmp_path, RuntimeMode.DEMO),
        config_hash=CONFIG_HASH,
        ledger=ledger,
        gateway=gateway,
        runtime=RuntimeStub(),
        execution=execution,
        lifecycle=LifecycleSpy(calls, no_action),
        clock=FixedClock(NOW),
        assembler=AssemblerSpy(calls, bundle(RuntimeMode.DEMO)),
        decision=decision,
    )

    result = run(
        service.process_tick(
            tick(), providers=provider_contexts(), reconciliation=reconciliation()
        )
    )

    assert result.submission is SubmissionState.SENT_AWAITING_ACK
    assert result.command_id is not None
    assert result.command_id.startswith("open-command-")
    assert gateway.send_calls == []
    assert len(execution.plans) == 1
    plan = execution.plans[0]
    assert plan.command.command_id == result.command_id
    assert plan.sequence == 7
    assert plan.command.payload["direction"] == "BUY"
    assert plan.command.payload["reservation_id"] == "reservation-1"
    assert plan.expected_effect["event_type"] == "POSITION_OPENED"
    assert ledger.allocations == [(EPOCH, result.command_id, NOW)]


def test_agent20_failure_degrades_learning_without_interrupting_lifecycle(tmp_path):
    calls = []
    ledger = StubLedger(
        tmp_path,
        positions=({"broker_position_id": "position-1", "trade_id": "trade-1"},),
    )

    class FailingLearning:
        def process_due(self, *args, **kwargs):
            raise RuntimeError("analytics unavailable")

    service = scheduler(
        tmp_path,
        mode=RuntimeMode.DEMO,
        ledger=ledger,
        lifecycle=LifecycleSpy(calls, no_action),
        assembler=AssemblerSpy(calls, SnapshotAssemblyError("entry evidence absent")),
        decision=DecisionSpy(None),
        execution=ExecutionSpy(reject_submit=True),
        learning=FailingLearning(),
    )

    assert run(service.process_learning_due()) == ()
    assert service.learning_status is LearningRuntimeStatus.DEGRADED
    assert service.learning_detail == "RuntimeError"

    result = run(
        service.process_tick(
            tick(), providers=provider_contexts(), reconciliation=reconciliation()
        )
    )
    assert calls[:2] == ["lifecycle:position-1", "assemble"]
    assert result.lifecycle[0].trade_id == "trade-1"
    assert result.learning_status is LearningRuntimeStatus.DEGRADED

