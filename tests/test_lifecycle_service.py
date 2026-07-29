import asyncio
from dataclasses import replace
from decimal import Decimal

import pytest

from python_swarm.broker_gateway import CompletedBar
from python_swarm.domain_models import (
    BrokerCommand,
    BrokerEvent,
    BrokerEventType,
    ContributorAttribution,
    CommandAction,
    CommandStatus,
    Direction,
    EntryLevels,
    LifecycleActionKind,
    LifecycleStage,
    Milestone,
    QuoteSnapshot,
    ReservationState,
    RiskReservation,
    SymbolSnapshot,
    TradeIntent,
)
from python_swarm.execution_service import (
    ExecutionService,
    OpenExecutionPlan,
    canonical_open_expected_effect,
    canonical_open_payload,
)
from python_swarm.lifecycle_service import (
    LifecycleDisposition,
    LifecycleEvidenceError,
    LifecycleService,
    SessionCloseEvidence,
)
from python_swarm.runtime_paths import RuntimePaths
from python_swarm.runtime_state import (
    BrokerIdentity,
    BrokerTradeMode,
    ExpectedBrokerIdentity,
    RuntimeStateMachine,
)
from python_swarm.trade_ledger import (
    BrokerDealRecord,
    DailyRiskStateRecord,
    OwnedPositionRecord,
    TradeEventRecord,
    TradeLedger,
)
from python_swarm.trading_policies import LifecyclePolicy


NOW = 300_000_000
HOUR_MS = 3_600_000
ACCOUNT_ID = "demo-10001"
SYMBOL = "XAUUSD"
MAGIC = 202412
EPOCH = "epoch-1"


class MutableClock:
    def __init__(self, value: int):
        self.value = value

    def now_ms(self) -> int:
        return self.value


class RecordingBroker:
    def __init__(self):
        self.sent = []

    async def send(self, command):
        self.sent.append(command)


class SimulatedCrash(RuntimeError):
    pass


class CrashAfterLifecycleReserve:
    """Models process loss after CREATED/action-lock commit but before SENT."""

    def __init__(self, execution: ExecutionService):
        self.execution = execution

    async def submit_lifecycle(self, plan):
        self.execution.reserve_lifecycle(plan)
        raise SimulatedCrash("process stopped after lifecycle reservation")


@pytest.fixture
def ledger_and_clock(tmp_path):
    clock = MutableClock(NOW)
    paths = RuntimePaths.resolve("test", data_dir=tmp_path).ensure_layout()
    ledger = TradeLedger(paths.trade_ledger_path, clock=lambda: clock.value / 1_000).open()
    ledger.initialize_daily_risk_state(
        DailyRiskStateRecord(
            ACCOUNT_ID,
            "2026-07-15",
            "10000",
            "10000",
            True,
            False,
            NOW,
            current_balance="10000",
        )
    )
    try:
        yield ledger, clock
    finally:
        if ledger.is_open:
            ledger.close()


def _ready_runtime(clock: MutableClock) -> RuntimeStateMachine:
    runtime = RuntimeStateMachine(clock)
    runtime.start_bootstrap()
    runtime.wait_for_ea(ledger_healthy=True, provider_gate_safe=True)
    runtime.accept_connection(
        ExpectedBrokerIdentity(ACCOUNT_ID, "Broker-Demo", SYMBOL, MAGIC),
        BrokerIdentity(
            ACCOUNT_ID,
            "Broker-Demo",
            BrokerTradeMode.DEMO,
            SYMBOL,
            MAGIC,
            EPOCH,
            True,
            True,
        ),
        transport_loopback=True,
        tuple_lock_acquired=True,
    )
    runtime.mark_synchronized(
        snapshot_id="snapshot-0",
        reconciliation_run_id="reconciliation-0",
        deal_cursor_initialized=True,
        daily_baseline_verified=True,
        daily_risk_halted=False,
    )
    return runtime


def _levels(direction: Direction) -> EntryLevels:
    if direction is Direction.BUY:
        return EntryLevels("2500", "2495", "2505", "2510", "2515")
    return EntryLevels("2500", "2505", "2495", "2490", "2485")


def _open_plan(
    direction: Direction,
    *,
    initial_volume: Decimal,
) -> OpenExecutionPlan:
    levels = _levels(direction)
    contributors = tuple(
        ContributorAttribution(
            f"signal-{agent_id}",
            agent_id,
            direction,
            "80",
            "1",
            "80",
        )
        for agent_id in (4, 5, 6)
    )
    intent = TradeIntent(
        "intent-1",
        "trade-1",
        "fusion-1",
        SYMBOL,
        MAGIC,
        direction,
        levels,
        "2500",
        10,
        "0.5",
        contributors,
        "trending_bull" if direction is Direction.BUY else "trending_bear",
        "LONDON",
        NOW - 1_000,
        NOW + 60_000,
        "config-v1",
    )
    reservation = RiskReservation(
        "reservation-1",
        "trade-1",
        ACCOUNT_ID,
        direction,
        "2026-07-15",
        "50",
        initial_volume,
        ReservationState.RESERVED,
        NOW - 900,
        NOW + 60_000,
    )
    payload = canonical_open_payload(
        intent,
        reservation,
        account_id=ACCOUNT_ID,
        currency="USD",
        max_total_risk=Decimal("300"),
        max_open_positions=3,
    )
    effect = canonical_open_expected_effect(
        intent,
        reservation,
        account_id=ACCOUNT_ID,
    )
    command = BrokerCommand(
        "open-command-1",
        "trade-1",
        CommandAction.OPEN,
        CommandStatus.CREATED,
        NOW - 800,
        NOW + 60_000,
        payload=payload,
    )
    return OpenExecutionPlan(
        intent,
        reservation,
        command,
        ACCOUNT_ID,
        "USD",
        EPOCH,
        1,
        effect,
        Decimal("300"),
        3,
    )


def _seed_owned_position(
    ledger: TradeLedger,
    clock: MutableClock,
    direction: Direction,
    *,
    initial_volume: Decimal = Decimal("0.40"),
    lifecycle_stage: LifecycleStage = LifecycleStage.PRE_TP1,
    current_sl: Decimal | None = None,
    opened_at_ms: int = NOW - HOUR_MS // 2,
):
    runtime = _ready_runtime(clock)
    entry_execution = ExecutionService(
        ledger=ledger,
        runtime=runtime,
        broker=RecordingBroker(),
        clock=clock,
    )
    asyncio.run(
        entry_execution.submit_open(
            _open_plan(direction, initial_volume=initial_volume)
        )
    )
    levels = _levels(direction)
    owned = OwnedPositionRecord(
        ACCOUNT_ID,
        "trade-1",
        "position-1",
        "ticket-1",
        "entry-order-1",
        SYMBOL,
        MAGIC,
        direction.value,
        initial_volume,
        initial_volume,
        levels.entry,
        levels.sl,
        levels.sl if current_sl is None else current_sl,
        levels.tp1,
        levels.tp2,
        levels.tp3,
        "50",
        lifecycle_stage.value,
        1,
        opened_at_ms,
        NOW,
    )
    ledger.acknowledge_open_command_atomic(
        "open-command-1",
        event=TradeEventRecord(
            "position-opened-1",
            "POSITION_OPENED",
            "broker",
            NOW / 1_000,
            {},
            trade_id="trade-1",
            correlation_id="open-command-1",
        ),
        position=owned,
    )
    record = ledger.get_owned_position(ACCOUNT_ID, "position-1")
    assert record is not None
    return runtime, record


def _symbol(*, volume_min: str = "0.01", volume_step: str = "0.01"):
    return SymbolSnapshot(
        SYMBOL,
        2,
        "0.01",
        "1",
        "100",
        volume_min,
        volume_step,
        "100",
        stops_level_points=0,
        freeze_level_points=0,
    )


def _quote(direction: Direction, target: Milestone, *, captured_at_ms: int = NOW):
    if target is Milestone.TP1:
        if direction is Direction.BUY:
            return QuoteSnapshot(SYMBOL, "2505", "2505.10", captured_at_ms)
        return QuoteSnapshot(SYMBOL, "2494.90", "2495", captured_at_ms)
    if target is Milestone.TP2:
        if direction is Direction.BUY:
            return QuoteSnapshot(SYMBOL, "2510", "2510.10", captured_at_ms)
        return QuoteSnapshot(SYMBOL, "2489.90", "2490", captured_at_ms)
    raise AssertionError(f"unsupported quote target {target}")


def _neutral_quote(direction: Direction, *, captured_at_ms: int = NOW):
    if direction is Direction.BUY:
        return QuoteSnapshot(SYMBOL, "2501", "2501.10", captured_at_ms)
    return QuoteSnapshot(SYMBOL, "2498.90", "2499", captured_at_ms)


def _closed_h1_bars(count: int, *, end_at_ms: int = NOW):
    start = end_at_ms - count * HOUR_MS
    return tuple(
        CompletedBar(
            SYMBOL,
            "H1",
            start + index * HOUR_MS,
            start + (index + 1) * HOUR_MS,
            start + (index + 1) * HOUR_MS,
            "2500",
            "2502",
            "2498",
            "2501",
            100,
        )
        for index in range(count)
    )


def _lifecycle_service(
    ledger: TradeLedger,
    runtime: RuntimeStateMachine,
    clock: MutableClock,
    broker: RecordingBroker,
):
    execution = ExecutionService(
        ledger=ledger,
        runtime=runtime,
        broker=broker,
        clock=clock,
    )
    return LifecycleService(
        ledger=ledger,
        execution=execution,
        policy=LifecyclePolicy(),
        clock=clock,
    ), execution


@pytest.mark.parametrize("direction", [Direction.BUY, Direction.SELL])
def test_tp1_planning_is_directional_and_pins_exact_partial_effect(
    ledger_and_clock, direction
):
    ledger, clock = ledger_and_clock
    runtime, position = _seed_owned_position(ledger, clock, direction)
    broker = RecordingBroker()
    service, _ = _lifecycle_service(ledger, runtime, clock, broker)

    result = asyncio.run(
        service.evaluate(
            position,
            account_id=ACCOUNT_ID,
            quote=_quote(direction, Milestone.TP1),
            symbol=_symbol(),
            connection_epoch=EPOCH,
            completed_h1_bars=_closed_h1_bars(1),
        )
    )

    assert result.disposition is LifecycleDisposition.SENT_AWAITING_ACK
    assert result.action is LifecycleActionKind.PARTIAL_CLOSE
    assert result.milestone is Milestone.TP1
    assert len(broker.sent) == 1
    outbound = broker.sent[0]
    assert outbound.action is CommandAction.PARTIAL_CLOSE
    assert outbound.position_id == "position-1"
    assert outbound.expected_position_version == 1
    assert outbound.expected_effect == {
        "action": "PARTIAL_CLOSE",
        "broker_position_id": "position-1",
        "expected_position_version": 1,
        "previous_remaining_volume": "0.4",
        "target_remaining_volume": "0.20",
        "target_close_volume": "0.20",
    }


@pytest.mark.parametrize(
    ("direction", "expected_sl"),
    [(Direction.BUY, Decimal("2500.03")), (Direction.SELL, Decimal("2499.97"))],
)
def test_break_even_planning_moves_sl_in_the_safe_direction(
    ledger_and_clock, direction, expected_sl
):
    ledger, clock = ledger_and_clock
    runtime, position = _seed_owned_position(
        ledger,
        clock,
        direction,
        lifecycle_stage=LifecycleStage.TP1_CONFIRMED,
    )
    broker = RecordingBroker()
    service, _ = _lifecycle_service(ledger, runtime, clock, broker)
    quote = (
        QuoteSnapshot(SYMBOL, "2506", "2506.10", NOW)
        if direction is Direction.BUY
        else QuoteSnapshot(SYMBOL, "2493.90", "2494", NOW)
    )

    result = asyncio.run(
        service.evaluate(
            position,
            account_id=ACCOUNT_ID,
            quote=quote,
            symbol=_symbol(),
            connection_epoch=EPOCH,
            completed_h1_bars=_closed_h1_bars(1),
        )
    )

    assert result.action is LifecycleActionKind.MODIFY_SL
    assert result.milestone is Milestone.BREAK_EVEN
    assert Decimal(broker.sent[0].expected_effect["target_sl"]) == expected_sl


def test_timeout_requires_48_actual_closed_h1_bars_when_history_starts_after_open(
    ledger_and_clock,
):
    ledger, clock = ledger_and_clock
    runtime, position = _seed_owned_position(
        ledger,
        clock,
        Direction.BUY,
        opened_at_ms=NOW - 49 * HOUR_MS,
    )
    broker = RecordingBroker()
    service, _ = _lifecycle_service(ledger, runtime, clock, broker)

    with pytest.raises(LifecycleEvidenceError, match="neither reaches position open"):
        asyncio.run(
            service.evaluate(
                position,
                account_id=ACCOUNT_ID,
                quote=_neutral_quote(Direction.BUY),
                symbol=_symbol(),
                connection_epoch=EPOCH,
                completed_h1_bars=_closed_h1_bars(47),
            )
        )
    assert broker.sent == []

    result = asyncio.run(
        service.evaluate(
            position,
            account_id=ACCOUNT_ID,
            quote=_neutral_quote(Direction.BUY),
            symbol=_symbol(),
            connection_epoch=EPOCH,
            completed_h1_bars=_closed_h1_bars(48),
        )
    )
    assert result.action is LifecycleActionKind.CLOSE
    assert result.milestone is Milestone.TIMEOUT
    assert result.h1_bars_elapsed == 48
    assert broker.sent[0].expected_effect["target_remaining_volume"] == "0"


def test_incomplete_h1_bar_is_never_accepted_as_timeout_evidence(ledger_and_clock):
    ledger, clock = ledger_and_clock
    runtime, position = _seed_owned_position(ledger, clock, Direction.BUY)
    broker = RecordingBroker()
    service, _ = _lifecycle_service(ledger, runtime, clock, broker)
    open_bar = CompletedBar(
        SYMBOL,
        "H1",
        NOW - HOUR_MS // 2,
        NOW + HOUR_MS // 2,
        NOW,
        "2500",
        "2502",
        "2498",
        "2501",
        100,
    )

    with pytest.raises(LifecycleEvidenceError, match="open or time-invalid"):
        asyncio.run(
            service.evaluate(
                position,
                account_id=ACCOUNT_ID,
                quote=_neutral_quote(Direction.BUY),
                symbol=_symbol(),
                connection_epoch=EPOCH,
                completed_h1_bars=(open_bar,),
            )
        )
    assert broker.sent == []


@pytest.mark.parametrize("captured_at_ms", [NOW - 2_501, NOW + 251])
def test_stale_or_future_quote_is_rejected_before_planning(
    ledger_and_clock, captured_at_ms
):
    ledger, clock = ledger_and_clock
    runtime, position = _seed_owned_position(ledger, clock, Direction.BUY)
    broker = RecordingBroker()
    service, _ = _lifecycle_service(ledger, runtime, clock, broker)

    with pytest.raises(LifecycleEvidenceError, match="stale or future-dated"):
        asyncio.run(
            service.evaluate(
                position,
                account_id=ACCOUNT_ID,
                quote=_neutral_quote(Direction.BUY, captured_at_ms=captured_at_ms),
                symbol=_symbol(),
                connection_epoch=EPOCH,
            )
        )
    assert broker.sent == []


def test_session_close_evidence_must_be_bound_to_the_exact_quote(ledger_and_clock):
    ledger, clock = ledger_and_clock
    runtime, position = _seed_owned_position(ledger, clock, Direction.BUY)
    broker = RecordingBroker()
    service, _ = _lifecycle_service(ledger, runtime, clock, broker)
    evidence = SessionCloseEvidence(
        "london-v1",
        "LONDON",
        "UTC",
        NOW - 1,
        True,
    )

    with pytest.raises(LifecycleEvidenceError, match="not bound"):
        asyncio.run(
            service.evaluate(
                position,
                account_id=ACCOUNT_ID,
                quote=_neutral_quote(Direction.BUY),
                symbol=_symbol(),
                connection_epoch=EPOCH,
                completed_h1_bars=_closed_h1_bars(1),
                session_close_evidence=evidence,
            )
        )
    assert broker.sent == []


def test_exact_session_close_evidence_can_plan_the_unadvanced_close(ledger_and_clock):
    ledger, clock = ledger_and_clock
    runtime, position = _seed_owned_position(ledger, clock, Direction.BUY)
    broker = RecordingBroker()
    service, _ = _lifecycle_service(ledger, runtime, clock, broker)
    evidence = SessionCloseEvidence("london-v1", "LONDON", "UTC", NOW, True)

    result = asyncio.run(
        service.evaluate(
            position,
            account_id=ACCOUNT_ID,
            quote=_neutral_quote(Direction.BUY),
            symbol=_symbol(),
            connection_epoch=EPOCH,
            completed_h1_bars=_closed_h1_bars(1),
            session_close_evidence=evidence,
        )
    )

    assert result.action is LifecycleActionKind.CLOSE
    assert result.milestone is Milestone.SESSION_CLOSE
    assert broker.sent[0].expected_effect["target_remaining_volume"] == "0"


@pytest.mark.parametrize(
    ("from_stage", "milestone", "current_sl", "expected_stage"),
    [
        (
            LifecycleStage.PRE_TP1,
            Milestone.TP1,
            Decimal("2495"),
            LifecycleStage.TP1_CONFIRMED,
        ),
        (
            LifecycleStage.TP1_CONFIRMED,
            Milestone.TP2,
            Decimal("2500.03"),
            LifecycleStage.TP2_CONFIRMED,
        ),
    ],
)
def test_virtual_milestone_is_durable_and_stale_exact_retry_is_idempotent(
    ledger_and_clock, from_stage, milestone, current_sl, expected_stage
):
    ledger, clock = ledger_and_clock
    runtime, stale_position = _seed_owned_position(
        ledger,
        clock,
        Direction.BUY,
        initial_volume=Decimal("0.02"),
        lifecycle_stage=from_stage,
        current_sl=current_sl,
    )
    broker = RecordingBroker()
    service, _ = _lifecycle_service(ledger, runtime, clock, broker)
    quote = _quote(Direction.BUY, milestone)

    first = asyncio.run(
        service.evaluate(
            stale_position,
            account_id=ACCOUNT_ID,
            quote=quote,
            symbol=_symbol(volume_min="0.02"),
            connection_epoch=EPOCH,
            completed_h1_bars=_closed_h1_bars(1),
        )
    )
    assert first.disposition is LifecycleDisposition.VIRTUAL_MILESTONE_APPLIED
    durable = ledger.get_owned_position(ACCOUNT_ID, "position-1")
    assert durable is not None
    assert durable["lifecycle_stage"] == expected_stage.value
    assert durable["remaining_volume"] == "0.02"
    assert durable["position_version"] == 1
    assert durable["realised_pnl"] == "0"
    assert broker.sent == []

    # A restart may hold the immutable pre-commit record.  Advancing the wall
    # clock must not alter the retry identity because quote evidence is pinned.
    clock.value += 100
    retry = asyncio.run(
        service.evaluate(
            stale_position,
            account_id=ACCOUNT_ID,
            quote=quote,
            symbol=_symbol(volume_min="0.02"),
            connection_epoch=EPOCH,
            completed_h1_bars=_closed_h1_bars(1),
        )
    )
    assert retry.disposition is LifecycleDisposition.VIRTUAL_MILESTONE_EXACT_RETRY
    assert ledger.get_owned_position(ACCOUNT_ID, "position-1") == durable
    assert broker.sent == []


def test_created_lifecycle_command_is_recovered_and_sent_exactly_once(
    ledger_and_clock,
):
    ledger, clock = ledger_and_clock
    runtime, position = _seed_owned_position(ledger, clock, Direction.BUY)
    reserve_execution = ExecutionService(
        ledger=ledger,
        runtime=runtime,
        broker=RecordingBroker(),
        clock=clock,
    )
    interrupted = LifecycleService(
        ledger=ledger,
        execution=CrashAfterLifecycleReserve(reserve_execution),
        policy=LifecyclePolicy(),
        clock=clock,
    )
    quote = _quote(Direction.BUY, Milestone.TP1)

    with pytest.raises(SimulatedCrash):
        asyncio.run(
            interrupted.evaluate(
                position,
                account_id=ACCOUNT_ID,
                quote=quote,
                symbol=_symbol(),
                connection_epoch=EPOCH,
                completed_h1_bars=_closed_h1_bars(1),
            )
        )
    pending = ledger.get_owned_position(ACCOUNT_ID, "position-1")
    assert pending is not None
    command_id = pending["pending_command_id"]
    assert command_id is not None
    assert ledger.get_command(command_id)["status"] == "CREATED"

    broker = RecordingBroker()
    recovered, _ = _lifecycle_service(ledger, runtime, clock, broker)
    first_retry = asyncio.run(
        recovered.evaluate(
            pending,
            account_id=ACCOUNT_ID,
            quote=quote,
            symbol=_symbol(),
            connection_epoch=EPOCH,
        )
    )
    assert first_retry.disposition is LifecycleDisposition.EXACT_RETRY_AWAITING_ACK
    assert ledger.get_command(command_id)["status"] == "SENT"
    assert len(broker.sent) == 1

    still_pending = ledger.get_owned_position(ACCOUNT_ID, "position-1")
    second_retry = asyncio.run(
        recovered.evaluate(
            still_pending,
            account_id=ACCOUNT_ID,
            quote=quote,
            symbol=_symbol(),
            connection_epoch=EPOCH,
        )
    )
    assert second_retry.disposition is LifecycleDisposition.EXACT_RETRY_AWAITING_ACK
    assert len(broker.sent) == 1


def _plan_tp1_partial(ledger, clock):
    runtime, position = _seed_owned_position(ledger, clock, Direction.BUY)
    broker = RecordingBroker()
    service, execution = _lifecycle_service(ledger, runtime, clock, broker)
    result = asyncio.run(
        service.evaluate(
            position,
            account_id=ACCOUNT_ID,
            quote=_quote(Direction.BUY, Milestone.TP1),
            symbol=_symbol(),
            connection_epoch=EPOCH,
            completed_h1_bars=_closed_h1_bars(1),
        )
    )
    assert result.command_id is not None
    return service, execution, result.command_id


def test_fill_event_cannot_arrive_before_its_authoritative_deal(ledger_and_clock):
    ledger, clock = ledger_and_clock
    service, _, command_id = _plan_tp1_partial(ledger, clock)
    event = BrokerEvent(
        "partial-event-1",
        BrokerEventType.PARTIAL_FILLED,
        NOW + 10,
        NOW + 11,
        command_id=command_id,
        trade_id="trade-1",
        broker_position_id="position-1",
        position_ticket="ticket-1",
        order_id="partial-order-1",
        deal_id="missing-deal-1",
        fill_price="2506",
        fill_volume="0.20",
        remaining_volume="0.20",
        profit="7",
        position_version=2,
    )

    with pytest.raises(LifecycleEvidenceError, match="before its authoritative"):
        service.ingest_broker_event(
            event,
            account_id=ACCOUNT_ID,
            symbol=SYMBOL,
            magic=MAGIC,
        )
    stored = ledger.get_owned_position(ACCOUNT_ID, "position-1")
    assert stored["remaining_volume"] == "0.4"
    assert stored["pending_command_id"] == command_id


def test_fill_event_economics_must_exactly_match_its_authoritative_deal(
    ledger_and_clock,
):
    ledger, clock = ledger_and_clock
    service, execution, command_id = _plan_tp1_partial(ledger, clock)
    execution.record_broker_deal(
        BrokerDealRecord(
            ACCOUNT_ID,
            "partial-deal-1",
            "position-1",
            "partial-order-1",
            "OUT",
            # Deliberately differs from the event's 0.20 fill volume.
            "0.10",
            "2506",
            "7",
            "0",
            "0",
            "0",
            (NOW + 10) / 1_000,
            trade_id="trade-1",
            command_id=command_id,
        )
    )
    event = BrokerEvent(
        "partial-event-1",
        BrokerEventType.PARTIAL_FILLED,
        NOW + 10,
        NOW + 11,
        command_id=command_id,
        trade_id="trade-1",
        broker_position_id="position-1",
        position_ticket="ticket-1",
        order_id="partial-order-1",
        deal_id="partial-deal-1",
        fill_price="2506",
        fill_volume="0.20",
        remaining_volume="0.20",
        profit="7",
        position_version=2,
    )

    with pytest.raises(
        LifecycleEvidenceError,
        match="economics or broker time differ from its deal",
    ):
        service.ingest_broker_event(
            event,
            account_id=ACCOUNT_ID,
            symbol=SYMBOL,
            magic=MAGIC,
        )
    stored = ledger.get_owned_position(ACCOUNT_ID, "position-1")
    assert stored["remaining_volume"] == "0.4"
    assert stored["pending_command_id"] == command_id


def test_live_partial_fill_commits_deal_event_and_position_atomically(
    ledger_and_clock,
):
    ledger, clock = ledger_and_clock
    service, _, command_id = _plan_tp1_partial(ledger, clock)
    deal = BrokerDealRecord(
        ACCOUNT_ID,
        "atomic-partial-deal-1",
        "position-1",
        "atomic-partial-order-1",
        "OUT",
        "0.20",
        "2506",
        "7",
        "-1",
        "0",
        "0",
        (NOW + 10) / 1_000,
        trade_id="trade-1",
        command_id=command_id,
    )
    event = BrokerEvent(
        "atomic-partial-event-1",
        BrokerEventType.PARTIAL_FILLED,
        NOW + 10,
        NOW + 11,
        command_id=command_id,
        trade_id="trade-1",
        broker_position_id="position-1",
        position_ticket="ticket-1",
        order_id="atomic-partial-order-1",
        deal_id="atomic-partial-deal-1",
        fill_price="2506",
        fill_volume="0.20",
        remaining_volume="0.20",
        profit="7",
        commission="-1",
        position_version=2,
    )

    result = service.ingest_broker_event(
        event,
        account_id=ACCOUNT_ID,
        symbol=SYMBOL,
        magic=MAGIC,
        deal=deal,
    )

    assert result.disposition is LifecycleDisposition.BROKER_EVENT_APPLIED
    assert [item["deal_id"] for item in ledger.get_trade_deals("trade-1")] == [
        "atomic-partial-deal-1"
    ]
    stored = ledger.get_owned_position(ACCOUNT_ID, "position-1")
    assert stored["remaining_volume"] == "0.2"
    assert stored["realised_pnl"] == "6"
    assert stored["lifecycle_stage"] == "TP1_CONFIRMED"
    assert ledger.get_command(command_id)["status"] == "APPLIED"


def test_broker_rejection_clears_lifecycle_lock_without_fabricating_version(
    ledger_and_clock,
):
    ledger, clock = ledger_and_clock
    service, _, command_id = _plan_tp1_partial(ledger, clock)
    event = BrokerEvent(
        "partial-rejected-1",
        BrokerEventType.COMMAND_REJECTED,
        NOW + 10,
        NOW + 11,
        command_id=command_id,
        trade_id="trade-1",
        broker_position_id="position-1",
        position_ticket="ticket-1",
        reason="broker volume rule rejected request",
        position_version=1,
    )

    result = service.ingest_broker_event(
        event,
        account_id=ACCOUNT_ID,
        symbol=SYMBOL,
        magic=MAGIC,
    )

    assert result.disposition is LifecycleDisposition.BROKER_EVENT_APPLIED
    assert result.action is LifecycleActionKind.PARTIAL_CLOSE
    assert result.milestone is Milestone.TP1
    assert ledger.get_command(command_id)["status"] == "REJECTED"
    stored = ledger.get_owned_position(ACCOUNT_ID, "position-1")
    assert stored["position_version"] == 1
    assert stored["pending_action"] is None
    assert stored["pending_milestone"] is None
    assert stored["pending_command_id"] is None
    assert stored["remaining_volume"] == "0.4"
    assert ledger.get_reservation("reservation-1")["state"] == "CONSUMED"
    assert ledger.get_trade("trade-1")["status"] == "OPEN"


def test_service_derives_trailing_stop_only_from_a_confirmed_strict_h1_fractal(
    ledger_and_clock,
):
    ledger, clock = ledger_and_clock
    runtime, position = _seed_owned_position(
        ledger,
        clock,
        Direction.BUY,
        lifecycle_stage=LifecycleStage.TP2_CONFIRMED,
        current_sl=Decimal("2500.03"),
    )
    lows = ("2504", "2503", "2502", "2503.5", "2503")
    bars = tuple(
        CompletedBar(
            SYMBOL,
            "H1",
            NOW - (5 - index) * HOUR_MS,
            NOW - (4 - index) * HOUR_MS,
            NOW - (4 - index) * HOUR_MS,
            "2504",
            "2506",
            low,
            "2504.5",
            100,
        )
        for index, low in enumerate(lows)
    )
    broker = RecordingBroker()
    service, _ = _lifecycle_service(ledger, runtime, clock, broker)

    result = asyncio.run(
        service.evaluate(
            position,
            account_id=ACCOUNT_ID,
            quote=QuoteSnapshot(SYMBOL, "2512", "2512.10", NOW),
            symbol=_symbol(),
            connection_epoch=EPOCH,
            completed_h1_bars=bars,
        )
    )

    assert result.action is LifecycleActionKind.MODIFY_SL
    assert result.milestone is Milestone.TRAILING
    assert broker.sent[0].expected_effect["target_sl"] == "2501.90"


def test_partial_overfill_persists_broker_truth_and_quarantines_learning(
    ledger_and_clock,
):
    ledger, clock = ledger_and_clock
    service, execution, command_id = _plan_tp1_partial(ledger, clock)
    execution.record_broker_deal(
        BrokerDealRecord(
            ACCOUNT_ID,
            "partial-overfill-deal-1",
            "position-1",
            "partial-overfill-order-1",
            "OUT",
            "0.30",
            "2506",
            "9",
            "-1",
            "0",
            "0",
            (NOW + 10) / 1_000,
            trade_id="trade-1",
            command_id=command_id,
        )
    )
    event = BrokerEvent(
        "partial-overfill-event-1",
        BrokerEventType.PARTIAL_FILLED,
        NOW + 10,
        NOW + 11,
        command_id=command_id,
        trade_id="trade-1",
        broker_position_id="position-1",
        position_ticket="ticket-1",
        order_id="partial-overfill-order-1",
        deal_id="partial-overfill-deal-1",
        fill_price="2506",
        fill_volume="0.30",
        remaining_volume="0.10",
        profit="9",
        commission="-1",
        position_version=2,
    )

    result = service.ingest_broker_event(
        event,
        account_id=ACCOUNT_ID,
        symbol=SYMBOL,
        magic=MAGIC,
    )

    assert result.reason == "PARTIAL_OVERFILL_QUARANTINED"
    stored = ledger.get_owned_position(ACCOUNT_ID, "position-1")
    assert stored["remaining_volume"] == "0.1"
    assert stored["lifecycle_stage"] == "QUARANTINED"
    assert ledger.get_command(command_id)["status"] == "APPLIED"
    assert ledger.get_command(command_id)["last_error"] == "PARTIAL_CLOSE_OVERFILL"
    trade = ledger.get_trade("trade-1")
    assert trade["evidence_class"] == "QUARANTINED"
    assert trade["learning_eligible"] == 0
    assert f"LIFECYCLE_OVERFILL:trade-1" in execution._runtime.evidence.blocking_diffs


def test_partial_overfill_to_zero_closes_from_deals_and_remains_quarantined(
    ledger_and_clock,
):
    ledger, clock = ledger_and_clock
    service, execution, command_id = _plan_tp1_partial(ledger, clock)
    execution.record_broker_deal(
        BrokerDealRecord(
            ACCOUNT_ID,
            "partial-full-overfill-deal-1",
            "position-1",
            "partial-full-overfill-order-1",
            "OUT",
            "0.40",
            "2506",
            "11",
            "-1",
            "0",
            "0",
            (NOW + 10) / 1_000,
            trade_id="trade-1",
            command_id=command_id,
        )
    )
    event = BrokerEvent(
        "partial-full-overfill-event-1",
        BrokerEventType.PARTIAL_FILLED,
        NOW + 10,
        NOW + 11,
        command_id=command_id,
        trade_id="trade-1",
        broker_position_id="position-1",
        position_ticket="ticket-1",
        order_id="partial-full-overfill-order-1",
        deal_id="partial-full-overfill-deal-1",
        fill_price="2506",
        fill_volume="0.40",
        remaining_volume="0",
        profit="11",
        commission="-1",
        position_version=2,
    )

    result = service.ingest_broker_event(
        event,
        account_id=ACCOUNT_ID,
        symbol=SYMBOL,
        magic=MAGIC,
    )

    assert result.reason == "PARTIAL_OVERFILL_QUARANTINED"
    assert ledger.get_owned_position(ACCOUNT_ID, "position-1") is None
    command = ledger.get_command(command_id)
    assert command["status"] == "APPLIED"
    assert command["last_error"] == "PARTIAL_CLOSE_OVERFILLED_POSITION_CLOSED"
    trade = ledger.get_trade("trade-1")
    assert trade["status"] == "CLOSED"
    assert trade["actual_r_decimal"] == "0.2"
    assert trade["evidence_class"] == "QUARANTINED"
    assert trade["learning_eligible"] == 0
