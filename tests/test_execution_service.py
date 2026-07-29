import asyncio
from dataclasses import replace
from decimal import Decimal

import pytest

from python_swarm.domain_models import (
    BrokerCommand,
    BrokerEvent,
    BrokerEventType,
    BrokerPosition,
    ContributorAttribution,
    CommandAction,
    CommandStatus,
    Direction,
    EntryLevels,
    FixedClock,
    LifecycleActionKind,
    LifecycleStage,
    Milestone,
    ReservationState,
    RiskReservation,
    TradeIntent,
)
from python_swarm.execution_service import (
    BrokerSendUncertainError,
    ExecutionLedgerError,
    ExecutionService,
    ExecutionValidationError,
    LifecycleExecutionPlan,
    OpenExecutionPlan,
    SubmissionState,
    canonical_open_expected_effect,
    canonical_open_payload,
    canonical_lifecycle_payload,
)
from python_swarm.learning_service import GovernedLearningService
from python_swarm.runtime_paths import RuntimePaths
from python_swarm.runtime_state import (
    BrokerIdentity,
    BrokerTradeMode,
    ControlPlaneState,
    ExpectedBrokerIdentity,
    RuntimeNotReadyError,
    RuntimeStateMachine,
)
from python_swarm.trade_ledger import (
    BrokerDealRecord,
    DailyRiskStateRecord,
    OwnedPositionRecord,
    TradeEventRecord,
    TradeLedger,
)


NOW = 2_000_000


class InspectingBroker:
    def __init__(self, ledger, fail=False):
        self.ledger = ledger
        self.fail = fail
        self.sent = []

    async def send(self, command):
        assert self.ledger.get_command(command.command_id)["status"] == "SENT"
        assert self.ledger.get_reservation(command.reservation_id)["state"] == "SUBMITTED"
        self.sent.append(command)
        if self.fail:
            raise ConnectionError("ambiguous send")


class MutableClock:
    def __init__(self, value):
        self.value = value

    def now_ms(self):
        return self.value


class CancelledBroker(InspectingBroker):
    async def send(self, command):
        await super().send(command)
        raise asyncio.CancelledError


class LifecycleInspectingBroker:
    def __init__(self, ledger):
        self.ledger = ledger
        self.sent = []

    async def send(self, command):
        assert self.ledger.get_command(command.command_id)["status"] == "SENT"
        assert self.ledger.get_reservation(command.reservation_id)["state"] == "CONSUMED"
        position = self.ledger.list_owned_positions(
            "demo-10001", "XAUUSD", 202412
        )[0]
        assert position["pending_command_id"] == command.command_id
        self.sent.append(command)


def ready_runtime(clock):
    runtime = RuntimeStateMachine(clock)
    runtime.start_bootstrap()
    runtime.wait_for_ea(ledger_healthy=True, provider_gate_safe=True)
    runtime.accept_connection(
        ExpectedBrokerIdentity("demo-10001", "Broker-Demo", "XAUUSD", 202412),
        BrokerIdentity(
            "demo-10001",
            "Broker-Demo",
            BrokerTradeMode.DEMO,
            "XAUUSD",
            202412,
            "epoch-1",
            True,
            True,
        ),
        transport_loopback=True,
        tuple_lock_acquired=True,
    )
    runtime.mark_synchronized(
        snapshot_id="snapshot-0",
        reconciliation_run_id="recon-0",
        deal_cursor_initialized=True,
        daily_baseline_verified=True,
        daily_risk_halted=False,
    )
    return runtime


@pytest.fixture
def setup(tmp_path):
    clock = FixedClock(NOW)
    paths = RuntimePaths.resolve("test", data_dir=tmp_path).ensure_layout()
    ledger = TradeLedger(paths.trade_ledger_path, clock=lambda: NOW / 1_000).open()
    ledger.initialize_daily_risk_state(
        DailyRiskStateRecord(
            "demo-10001",
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


def make_plan():
    contributors = tuple(
        ContributorAttribution(
            f"signal-{agent}", agent, Direction.BUY, "80", "1", "80"
        )
        for agent in (4, 5, 6)
    )
    intent = TradeIntent(
        "intent-1",
        "trade-1",
        "fusion-1",
        "XAUUSD",
        202412,
        Direction.BUY,
        EntryLevels("2500", "2495", "2505", "2510", "2515"),
        "2500",
        10,
        "0.5",
        contributors,
        "trending_bull",
        "LONDON",
        NOW - 100,
        NOW + 10_000,
        "config-v1",
    )
    reservation = RiskReservation(
        "reservation-1",
        "trade-1",
        "demo-10001",
        Direction.BUY,
        "2026-07-15",
        "50",
        "0.1",
        ReservationState.RESERVED,
        NOW - 50,
        NOW + 10_000,
    )
    payload = canonical_open_payload(
        intent,
        reservation,
        account_id="demo-10001",
        currency="USD",
        max_total_risk=Decimal("300"),
        max_open_positions=3,
    )
    effect = canonical_open_expected_effect(
        intent, reservation, account_id="demo-10001"
    )
    command = BrokerCommand(
        "command-1",
        "trade-1",
        CommandAction.OPEN,
        CommandStatus.CREATED,
        NOW,
        NOW + 5_000,
        payload=payload,
    )
    return OpenExecutionPlan(
        intent,
        reservation,
        command,
        "demo-10001",
        "USD",
        "epoch-1",
        1,
        effect,
        Decimal("300"),
        3,
    )


def rekey_plan(plan, prefix, *, risk_day, max_open_positions, sequence=1):
    intent = replace(
        plan.intent,
        intent_id=f"{prefix}-intent",
        trade_id=f"{prefix}-trade",
        fusion_id=f"{prefix}-fusion",
    )
    reservation = replace(
        plan.reservation,
        reservation_id=f"{prefix}-reservation",
        trade_id=intent.trade_id,
        risk_day=risk_day,
    )
    payload = canonical_open_payload(
        intent,
        reservation,
        account_id=plan.account_id,
        currency=plan.currency,
        max_total_risk=plan.max_total_risk,
        max_open_positions=max_open_positions,
    )
    effect = canonical_open_expected_effect(
        intent,
        reservation,
        account_id=plan.account_id,
    )
    command = replace(
        plan.command,
        command_id=f"{prefix}-command",
        trade_id=intent.trade_id,
        payload=payload,
    )
    return replace(
        plan,
        intent=intent,
        reservation=reservation,
        command=command,
        expected_effect=effect,
        max_open_positions=max_open_positions,
        sequence=sequence,
    )


def seed_lifecycle_plan(ledger, clock):
    runtime = ready_runtime(clock)
    open_service = ExecutionService(
        ledger=ledger,
        runtime=runtime,
        broker=InspectingBroker(ledger),
        clock=clock,
    )
    asyncio.run(open_service.submit_open(make_plan()))
    owned = OwnedPositionRecord(
        "demo-10001",
        "trade-1",
        "position-1",
        "ticket-1",
        "order-1",
        "XAUUSD",
        202412,
        "BUY",
        "0.1",
        "0.1",
        "2500",
        "2495",
        "2495",
        "2505",
        "2510",
        "2515",
        "50",
        "PRE_TP1",
        1,
        NOW - 2,
        NOW,
    )
    ledger.acknowledge_open_command_atomic(
        "command-1",
        event=TradeEventRecord(
            "lifecycle-open",
            "POSITION_OPENED",
            "broker",
            NOW / 1_000,
            {},
            trade_id="trade-1",
            correlation_id="command-1",
        ),
        position=owned,
    )
    effect = {
        "action": "MODIFY_SL",
        "broker_position_id": "position-1",
        "expected_position_version": 1,
        "previous_sl": "2495",
        "target_sl": "2497",
    }
    bare_command = BrokerCommand(
        "lifecycle-command",
        "trade-1",
        CommandAction.MODIFY_SL,
        CommandStatus.CREATED,
        NOW,
        NOW + 5_000,
        position_id="position-1",
        expected_position_version=1,
    )
    command = replace(
        bare_command,
        payload=canonical_lifecycle_payload(
            bare_command,
            reservation_id="reservation-1",
            expected_effect=effect,
        ),
    )
    pending = BrokerPosition(
        "trade-1",
        "position-1",
        "ticket-1",
        "XAUUSD",
        202412,
        Direction.BUY,
        "0.1",
        "0.1",
        "2500",
        "2495",
        "2495",
        "2505",
        "2510",
        "2515",
        "50",
        LifecycleStage.PRE_TP1,
        1,
        NOW - 2,
        pending_action=LifecycleActionKind.MODIFY_SL,
        pending_milestone=Milestone.BREAK_EVEN,
        pending_command_id="lifecycle-command",
    )
    return runtime, LifecycleExecutionPlan(
        command,
        pending,
        "demo-10001",
        "reservation-1",
        "epoch-1",
        2,
        effect,
        broker_order_id="order-1",
    )


def test_sent_is_durable_before_transport_and_retry_never_resends(setup):
    ledger, clock = setup
    runtime = ready_runtime(clock)
    broker = InspectingBroker(ledger)
    service = ExecutionService(ledger=ledger, runtime=runtime, broker=broker, clock=clock)
    assert asyncio.run(service.submit_open(make_plan())) is SubmissionState.SENT_AWAITING_ACK
    assert len(broker.sent) == 1
    assert (
        asyncio.run(service.submit_open(make_plan()))
        is SubmissionState.EXACT_RETRY_AWAITING_ACK
    )
    assert len(broker.sent) == 1


def test_ambiguous_send_is_unknown_and_risk_is_not_released(setup):
    ledger, clock = setup
    runtime = ready_runtime(clock)
    broker = InspectingBroker(ledger, fail=True)
    service = ExecutionService(ledger=ledger, runtime=runtime, broker=broker, clock=clock)
    with pytest.raises(BrokerSendUncertainError):
        asyncio.run(service.submit_open(make_plan()))
    assert ledger.get_command("command-1")["status"] == "UNKNOWN"
    assert ledger.get_reservation("reservation-1")["state"] == "UNKNOWN"
    assert runtime.state is ControlPlaneState.DEGRADED


def test_open_ack_is_idempotent_and_registers_position_identity(setup):
    ledger, clock = setup
    runtime = ready_runtime(clock)
    service = ExecutionService(
        ledger=ledger,
        runtime=runtime,
        broker=InspectingBroker(ledger),
        clock=clock,
    )
    asyncio.run(service.submit_open(make_plan()))
    event = TradeEventRecord(
        "broker-open-1",
        "POSITION_OPENED",
        "broker",
        (NOW + 10) / 1_000,
        {},
        trade_id="trade-1",
        correlation_id="command-1",
    )
    position = OwnedPositionRecord(
        "demo-10001",
        "trade-1",
        "position-1",
        "ticket-1",
        "order-1",
        "XAUUSD",
        202412,
        "BUY",
        "0.1",
        "0.1",
        "2500",
        "2495",
        "2495",
        "2505",
        "2510",
        "2515",
        "50",
        "PRE_TP1",
        1,
        NOW + 9,
        NOW + 10,
    )
    assert ledger.acknowledge_open_command_atomic(
        "command-1", event=event, position=position
    )
    assert not ledger.acknowledge_open_command_atomic(
        "command-1", event=event, position=position
    )
    assert ledger.get_command("command-1")["status"] == "APPLIED"
    assert ledger.get_reservation("reservation-1")["state"] == "CONSUMED"
    stored = ledger.get_trade("trade-1")
    assert stored["broker_position_id"] == "position-1"
    assert stored["filled_volume"] == pytest.approx(0.1)
    assert stored["initial_risk_currency"] == pytest.approx(50)
    # Conservative exposure remains the full original reservation until close.
    assert Decimal(ledger.get_reservation("reservation-1")["risk_amount"]) == Decimal("50")

    sl_only = replace(
        position, current_sl="2497", position_version=2, updated_at_ms=NOW + 11
    )
    ledger.apply_position_update_atomic(
        event=TradeEventRecord(
            "broker-sl-1",
            "POSITION_UPDATED",
            "broker",
            (NOW + 11) / 1_000,
            {},
            trade_id="trade-1",
        ),
        position=sl_only,
    )
    assert ledger.get_trade("trade-1")["status"] == "OPEN"
    partial = replace(
        sl_only, remaining_volume="0.06", position_version=3, updated_at_ms=NOW + 12
    )
    ledger.apply_position_update_atomic(
        event=TradeEventRecord(
            "broker-partial-1",
            "PARTIAL_FILLED",
            "broker",
            (NOW + 12) / 1_000,
            {},
            trade_id="trade-1",
        ),
        position=partial,
    )
    assert ledger.get_trade("trade-1")["status"] == "PARTIALLY_CLOSED"
    for deal_id, volume, profit, at_ms in (
        ("deal-partial", 0.04, 10, NOW + 12),
        ("deal-final", 0.06, 40, NOW + 13),
    ):
        ledger.record_deal(
            BrokerDealRecord(
                "demo-10001",
                deal_id,
                "position-1",
                f"order-{deal_id}",
                "ENTRY_OUT",
                volume,
                2510,
                profit,
                0,
                0,
                0,
                at_ms / 1_000,
                trade_id="trade-1",
            )
        )
    result = ledger.close_trade_from_recorded_deals_atomic(
        "trade-1",
        event=TradeEventRecord(
            "broker-final-close",
            "POSITION_CLOSED",
            "broker",
            (NOW + 13) / 1_000,
            {},
            trade_id="trade-1",
        ),
        position_id="position-1",
        exit_price=2510,
        closed_at=(NOW + 13) / 1_000,
    )
    assert result.actual_r == pytest.approx(1.0)
    assert ledger.list_owned_positions("demo-10001", "XAUUSD", 202412) == []


def test_open_transaction_rebuilds_immutable_context_and_commits_deal_atomically(setup):
    ledger, clock = setup
    runtime = ready_runtime(clock)
    service = ExecutionService(
        ledger=ledger,
        runtime=runtime,
        broker=InspectingBroker(ledger),
        clock=clock,
    )
    asyncio.run(service.submit_open(make_plan()))
    deal = BrokerDealRecord(
        "demo-10001",
        "deal-open-atomic",
        "position-open-atomic",
        "order-open-atomic",
        "IN",
        "0.1",
        "2500",
        "0",
        "0",
        "0",
        "0",
        (NOW + 10) / 1_000,
        trade_id="trade-1",
        command_id="command-1",
    )
    event = BrokerEvent(
        "event-open-atomic",
        BrokerEventType.POSITION_OPENED,
        NOW + 10,
        NOW + 11,
        command_id="command-1",
        trade_id="trade-1",
        broker_position_id="position-open-atomic",
        position_ticket="ticket-open-atomic",
        order_id="order-open-atomic",
        deal_id="deal-open-atomic",
        fill_price="2500",
        fill_volume="0.1",
        remaining_volume="0.1",
        sl="2495",
        tp="2515",
        position_version=1,
    )

    assert service.ingest_open_transaction(
        event,
        account_id="demo-10001",
        symbol="XAUUSD",
        magic=202412,
        deal=deal,
    )
    stored = ledger.get_owned_position("demo-10001", "position-open-atomic")
    assert stored["initial_volume"] == "0.1"
    assert stored["initial_risk_amount"] == "50"
    assert stored["original_sl"] == "2495"
    assert stored["tp1"] == "2505"
    assert stored["tp2"] == "2510"
    assert stored["tp3"] == "2515"
    assert ledger.get_command("command-1")["status"] == "APPLIED"
    assert ledger.get_reservation("reservation-1")["state"] == "CONSUMED"
    assert [item["deal_id"] for item in ledger.get_trade_deals("trade-1")] == [
        "deal-open-atomic"
    ]



def test_progressive_open_fill_stays_received_until_reserved_volume_is_complete(setup):
    ledger, clock = setup
    service = ExecutionService(
        ledger=ledger,
        runtime=ready_runtime(clock),
        broker=InspectingBroker(ledger),
        clock=clock,
    )
    asyncio.run(service.submit_open(make_plan()))
    first_deal = BrokerDealRecord(
        "demo-10001",
        "deal-open-partial-1",
        "position-open-partial",
        "order-open-partial",
        "IN",
        "0.05",
        "2500",
        "0",
        "0",
        "0",
        "0",
        (NOW + 10) / 1_000,
        trade_id="trade-1",
        command_id="command-1",
    )
    first_event = BrokerEvent(
        "event-open-partial-1",
        BrokerEventType.POSITION_OPENED,
        NOW + 10,
        NOW + 11,
        command_id="command-1",
        trade_id="trade-1",
        broker_position_id="position-open-partial",
        position_ticket="ticket-open-partial",
        order_id="order-open-partial",
        deal_id="deal-open-partial-1",
        fill_price="2500",
        fill_volume="0.05",
        remaining_volume="0.05",
        sl="2495",
        tp="2515",
        position_version=1,
    )

    assert service.ingest_open_transaction(
        first_event,
        account_id="demo-10001",
        symbol="XAUUSD",
        magic=202412,
        deal=first_deal,
    )
    assert ledger.get_command("command-1")["status"] == "RECEIVED"
    assert ledger.get_reservation("reservation-1")["state"] == "SUBMITTED"
    assert not service.ingest_open_transaction(
        first_event,
        account_id="demo-10001",
        symbol="XAUUSD",
        magic=202412,
        deal=first_deal,
    )

    second_deal = replace(
        first_deal,
        deal_id="deal-open-partial-2",
        volume="0.05",
        occurred_at=(NOW + 12) / 1_000,
    )
    second_event = replace(
        first_event,
        event_id="event-open-partial-2",
        broker_time_ms=NOW + 12,
        received_at_ms=NOW + 13,
        deal_id="deal-open-partial-2",
        fill_volume=Decimal("0.05"),
        remaining_volume=Decimal("0.1"),
        position_version=2,
    )
    assert service.ingest_open_transaction(
        second_event,
        account_id="demo-10001",
        symbol="XAUUSD",
        magic=202412,
        deal=second_deal,
    )
    stored = ledger.get_owned_position("demo-10001", "position-open-partial")
    assert stored["initial_volume"] == "0.1"
    assert stored["initial_risk_amount"] == "50"
    assert ledger.get_command("command-1")["status"] == "APPLIED"
    assert ledger.get_reservation("reservation-1")["state"] == "CONSUMED"

def test_open_transaction_rejects_wire_protection_that_differs_from_durable_intent(setup):
    ledger, clock = setup
    service = ExecutionService(
        ledger=ledger,
        runtime=ready_runtime(clock),
        broker=InspectingBroker(ledger),
        clock=clock,
    )
    asyncio.run(service.submit_open(make_plan()))
    event = BrokerEvent(
        "event-open-bad-sl",
        BrokerEventType.POSITION_OPENED,
        NOW + 10,
        NOW + 11,
        command_id="command-1",
        trade_id="trade-1",
        broker_position_id="position-bad-sl",
        position_ticket="ticket-bad-sl",
        order_id="order-bad-sl",
        deal_id="deal-bad-sl",
        fill_price="2500",
        fill_volume="0.1",
        remaining_volume="0.1",
        sl="2494",
        tp="2515",
        position_version=1,
    )
    deal = BrokerDealRecord(
        "demo-10001",
        "deal-bad-sl",
        "position-bad-sl",
        "order-bad-sl",
        "IN",
        "0.1",
        "2500",
        "0",
        "0",
        "0",
        "0",
        (NOW + 10) / 1_000,
        trade_id="trade-1",
        command_id="command-1",
    )

    with pytest.raises(ExecutionValidationError, match="reserved protection"):
        service.ingest_open_transaction(
            event,
            account_id="demo-10001",
            symbol="XAUUSD",
            magic=202412,
            deal=deal,
        )
    assert ledger.get_trade_deals("trade-1") == []
    assert ledger.get_owned_position("demo-10001", "position-bad-sl") is None


def test_ledger_failure_halts_before_transport(setup):
    ledger, clock = setup
    runtime = ready_runtime(clock)
    broker = InspectingBroker(ledger)
    service = ExecutionService(ledger=ledger, runtime=runtime, broker=broker, clock=clock)
    ledger.close()
    with pytest.raises(ExecutionLedgerError):
        asyncio.run(service.submit_open(make_plan()))
    assert broker.sent == []
    assert runtime.state is ControlPlaneState.HALTED


def test_only_ready_can_reserve(setup):
    ledger, clock = setup
    service = ExecutionService(
        ledger=ledger,
        runtime=RuntimeStateMachine(clock),
        broker=InspectingBroker(ledger),
        clock=clock,
    )
    with pytest.raises(RuntimeNotReadyError):
        service.reserve_open(make_plan())


def test_created_crash_recovery_sends_once_and_later_time_retry_is_noop(setup):
    ledger, _ = setup
    clock = MutableClock(NOW)
    runtime = ready_runtime(clock)
    broker = InspectingBroker(ledger)
    service = ExecutionService(ledger=ledger, runtime=runtime, broker=broker, clock=clock)
    assert service.reserve_open(make_plan())
    slugs = {
        row[0]
        for row in ledger._connection.execute(
            "SELECT strategy_slug FROM trade_attributions"
        )
    }
    assert slugs == {"htf_liquidity_trap", "trendline_breakout", "crt_engine"}
    assert broker.sent == []
    clock.value += 100
    assert (
        asyncio.run(service.submit_open(make_plan()))
        is SubmissionState.EXACT_RETRY_AWAITING_ACK
    )
    assert len(broker.sent) == 1
    original_sent_at = ledger.get_command("command-1")["sent_at_ms"]
    clock.value += 100
    assert (
        asyncio.run(service.submit_open(make_plan()))
        is SubmissionState.EXACT_RETRY_AWAITING_ACK
    )
    assert len(broker.sent) == 1
    assert ledger.get_command("command-1")["sent_at_ms"] == original_sent_at


def test_cancellation_propagates_after_durable_unknown(setup):
    ledger, clock = setup
    runtime = ready_runtime(clock)
    service = ExecutionService(
        ledger=ledger,
        runtime=runtime,
        broker=CancelledBroker(ledger),
        clock=clock,
    )
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(service.submit_open(make_plan()))
    assert ledger.get_command("command-1")["status"] == "UNKNOWN"
    assert ledger.get_reservation("reservation-1")["state"] == "UNKNOWN"


@pytest.mark.parametrize("field", ["volume", "direction", "sl", "magic", "risk_amount"])
def test_noncanonical_open_payload_is_rejected_before_persistence(setup, field):
    ledger, clock = setup
    runtime = ready_runtime(clock)
    broker = InspectingBroker(ledger)
    service = ExecutionService(ledger=ledger, runtime=runtime, broker=broker, clock=clock)
    original = make_plan()
    payload = dict(original.command.payload)
    payload[field] = "999"
    altered = replace(
        original,
        command=replace(original.command, payload=payload),
    )
    with pytest.raises(ExecutionValidationError, match="payload is not canonical"):
        service.reserve_open(altered)
    assert ledger.get_trade("trade-1") is None
    assert broker.sent == []


def test_lifecycle_sent_boundary_ack_before_event_and_event_before_late_ack(setup):
    ledger, clock = setup
    runtime, plan = seed_lifecycle_plan(ledger, clock)
    broker = LifecycleInspectingBroker(ledger)
    service = ExecutionService(
        ledger=ledger, runtime=runtime, broker=broker, clock=clock
    )
    assert (
        asyncio.run(service.submit_lifecycle(plan))
        is SubmissionState.SENT_AWAITING_ACK
    )
    assert len(broker.sent) == 1
    sent_at = ledger.get_command("lifecycle-command")["sent_at_ms"]
    assert (
        asyncio.run(service.submit_lifecycle(plan))
        is SubmissionState.EXACT_RETRY_AWAITING_ACK
    )
    assert len(broker.sent) == 1
    assert ledger.get_command("lifecycle-command")["sent_at_ms"] == sent_at

    assert service.ingest_command_received(
        "lifecycle-command", received_at_ms=NOW + 20
    )
    completed = replace(
        plan.position,
        current_sl=Decimal("2497"),
            position_version=2,
            pending_action=None,
            pending_milestone=None,
            pending_command_id=None,
    )
    event = BrokerEvent(
        "lifecycle-effect",
        BrokerEventType.POSITION_UPDATED,
        NOW + 21,
        NOW + 22,
        command_id="lifecycle-command",
        trade_id="trade-1",
        broker_position_id="position-1",
        position_ticket="ticket-1",
        order_id="order-1",
        position_version=2,
    )
    assert service.ingest_broker_event(
        event,
        account_id="demo-10001",
        symbol="XAUUSD",
        magic=202412,
        position=completed,
    )
    assert ledger.get_command("lifecycle-command")["status"] == "APPLIED"
    assert not service.ingest_command_received(
        "lifecycle-command", received_at_ms=NOW + 23
    )
    stored = ledger.list_owned_positions("demo-10001", "XAUUSD", 202412)[0]
    assert stored["current_sl"] == "2497"
    assert stored["pending_command_id"] is None


def test_lifecycle_created_restart_resumes_once_without_releasing_risk(setup):
    ledger, clock = setup
    runtime, plan = seed_lifecycle_plan(ledger, clock)
    broker = LifecycleInspectingBroker(ledger)
    service = ExecutionService(
        ledger=ledger, runtime=runtime, broker=broker, clock=clock
    )
    assert service.reserve_lifecycle(plan)
    assert broker.sent == []
    assert ledger.get_command("lifecycle-command")["status"] == "CREATED"
    assert ledger.get_reservation("reservation-1")["state"] == "CONSUMED"
    assert (
        asyncio.run(service.submit_lifecycle(plan))
        is SubmissionState.EXACT_RETRY_AWAITING_ACK
    )
    assert len(broker.sent) == 1
    assert ledger.get_command("lifecycle-command")["status"] == "SENT"
    assert (
        asyncio.run(service.submit_lifecycle(plan))
        is SubmissionState.EXACT_RETRY_AWAITING_ACK
    )
    assert len(broker.sent) == 1


def test_ledger_independently_caps_risk_against_minimum_balance_or_equity(setup):
    ledger, clock = setup
    ledger.update_daily_risk_equity(
        "demo-10001",
        "2026-07-15",
        current_equity="10000",
        current_balance="4000",
        realised_pnl="0",
        consecutive_losses=0,
        updated_at_ms=NOW,
    )
    runtime = ready_runtime(clock)
    broker = InspectingBroker(ledger)
    service = ExecutionService(
        ledger=ledger, runtime=runtime, broker=broker, clock=clock
    )
    with pytest.raises(ExecutionLedgerError, match="1% per-trade"):
        asyncio.run(service.submit_open(make_plan()))
    assert broker.sent == []
    assert ledger.get_trade("trade-1") is None
    assert runtime.state is ControlPlaneState.HALTED


def test_overnight_consumed_reservation_still_uses_position_capacity(setup):
    ledger, clock = setup
    ledger.initialize_daily_risk_state(
        DailyRiskStateRecord(
            "demo-10001",
            "2026-07-14",
            "10000",
            "10000",
            True,
            False,
            NOW - 1_000,
            current_balance="10000",
        )
    )
    runtime = ready_runtime(clock)
    service = ExecutionService(
        ledger=ledger,
        runtime=runtime,
        broker=InspectingBroker(ledger),
        clock=clock,
    )
    overnight = rekey_plan(
        make_plan(),
        "overnight",
        risk_day="2026-07-14",
        max_open_positions=1,
    )
    assert service.reserve_open(overnight)
    ledger.mark_command_sent(overnight.command.command_id, sent_at_ms=NOW)
    ledger.acknowledge_open_command_atomic(
        overnight.command.command_id,
        event=TradeEventRecord(
            "overnight-opened",
            "POSITION_OPENED",
            "broker",
            (NOW + 1) / 1_000,
            {},
            trade_id=overnight.intent.trade_id,
            correlation_id=overnight.command.command_id,
            broker_timestamp=(NOW + 1) / 1_000,
        ),
        position=OwnedPositionRecord(
            "demo-10001",
            overnight.intent.trade_id,
            "overnight-position",
            "overnight-ticket",
            "overnight-order",
            "XAUUSD",
            202412,
            "BUY",
            "0.1",
            "0.1",
            "2500",
            "2495",
            "2495",
            "2505",
            "2510",
            "2515",
            "50",
            "PRE_TP1",
            1,
            NOW + 1,
            NOW + 1,
        ),
    )

    today = rekey_plan(
        make_plan(),
        "today",
        risk_day="2026-07-15",
        max_open_positions=1,
        sequence=2,
    )
    with pytest.raises(ExecutionLedgerError, match="position limit"):
        service.reserve_open(today)
    assert ledger.get_trade(today.intent.trade_id) is None
    exposure = ledger.get_active_risk_exposure("demo-10001", "2026-07-15")
    assert exposure["trade_ids"] == (overnight.intent.trade_id,)
    assert exposure["position_count"] == 1
    assert exposure["risk_amount"] == "50"


def test_ledger_refuses_caller_total_risk_limit_above_three_percent(setup):
    ledger, clock = setup
    original = make_plan()
    payload = dict(original.command.payload)
    payload["max_total_risk"] = Decimal("301")
    altered = replace(
        original,
        command=replace(original.command, payload=payload),
        max_total_risk=Decimal("301"),
    )
    runtime = ready_runtime(clock)
    broker = InspectingBroker(ledger)
    service = ExecutionService(
        ledger=ledger, runtime=runtime, broker=broker, clock=clock
    )
    with pytest.raises(ExecutionLedgerError, match="3% portfolio"):
        asyncio.run(service.submit_open(altered))
    assert broker.sent == []
    assert ledger.get_trade("trade-1") is None


def test_execution_plan_refuses_position_capacity_above_three():
    with pytest.raises(ValueError, match="position limit must be 1-3"):
        replace(make_plan(), max_open_positions=4)


def test_execution_refuses_requested_risk_above_one_percent(setup):
    ledger, clock = setup
    original = make_plan()
    service = ExecutionService(
        ledger=ledger,
        runtime=ready_runtime(clock),
        broker=InspectingBroker(ledger),
        clock=clock,
    )
    with pytest.raises(ExecutionValidationError, match="1% hard cap"):
        service.reserve_open(
            replace(
                original,
                intent=replace(
                    original.intent, requested_risk_pct=Decimal("1.01")
                ),
            )
        )
    assert ledger.get_trade("trade-1") is None


def test_noncanonical_effect_agent_range_and_zero_sequence_are_rejected(setup):
    ledger, clock = setup
    runtime = ready_runtime(clock)
    service = ExecutionService(
        ledger=ledger,
        runtime=runtime,
        broker=InspectingBroker(ledger),
        clock=clock,
    )
    original = make_plan()
    with pytest.raises(ExecutionValidationError, match="expected_effect"):
        service.reserve_open(replace(original, expected_effect={"event_type": "WRONG"}))
    bad_contributor = replace(original.intent.contributors[0], agent_id=14)
    bad_intent = replace(
        original.intent,
        contributors=(bad_contributor, *original.intent.contributors[1:]),
    )
    with pytest.raises(ExecutionValidationError, match="04 through 13"):
        service.reserve_open(replace(original, intent=bad_intent))
    with pytest.raises(ValueError, match="at least 1"):
        replace(original, sequence=0)


def test_close_ingest_returns_false_for_duplicate_and_raises_for_missing_trade(setup):
    ledger, clock = setup
    runtime = ready_runtime(clock)
    service = ExecutionService(
        ledger=ledger,
        runtime=runtime,
        broker=InspectingBroker(ledger),
        clock=clock,
    )
    asyncio.run(service.submit_open(make_plan()))
    ledger.acknowledge_trade_open(
        "trade-1",
        event_id="legacy-open",
        account_id="demo-10001",
        broker_order_id="order-1",
        broker_position_id="position-1",
        fill_price=2500,
        filled_volume=0.1,
        opened_at=(NOW + 1) / 1_000,
    )
    ledger.record_deal(
        BrokerDealRecord(
            "demo-10001",
            "deal-close",
            "position-1",
            "order-close",
            "ENTRY_OUT",
            0.1,
            2510,
            51,
            -1,
            0,
            0,
            (NOW + 10) / 1_000,
            trade_id="trade-1",
        )
    )
    close = BrokerEvent(
        "broker-close",
        BrokerEventType.POSITION_CLOSED,
        NOW + 10,
        NOW + 11,
        trade_id="trade-1",
        broker_position_id="position-1",
        fill_price="2510",
    )
    assert service.ingest_broker_event(
        close, account_id="demo-10001", symbol="XAUUSD", magic=202412
    )
    assert not service.ingest_broker_event(
        close, account_id="demo-10001", symbol="XAUUSD", magic=202412
    )
    missing = replace(
        close,
        event_id="missing-close",
        trade_id="missing-trade",
        broker_position_id="missing-position",
    )
    with pytest.raises(ExecutionLedgerError):
        service.ingest_broker_event(
            missing, account_id="demo-10001", symbol="XAUUSD", magic=202412
        )


def test_production_close_becomes_authoritative_agent20_learning_source(setup):
    """Exercise the production event name through close, PMA and proposal."""

    ledger, clock = setup
    runtime = ready_runtime(clock)
    service = ExecutionService(
        ledger=ledger,
        runtime=runtime,
        broker=InspectingBroker(ledger),
        clock=clock,
    )
    asyncio.run(service.submit_open(make_plan()))
    open_deal = BrokerDealRecord(
        "demo-10001",
        "deal-agent20-open",
        "position-agent20",
        "order-agent20-open",
        "IN",
        "0.05",
        "2500",
        "0",
        "-2",
        "0",
        "0",
        (NOW + 10) / 1_000,
        trade_id="trade-1",
        command_id="command-1",
    )
    open_event = BrokerEvent(
        "event-agent20-open",
        BrokerEventType.POSITION_OPENED,
        NOW + 10,
        NOW + 11,
        command_id="command-1",
        trade_id="trade-1",
        broker_position_id="position-agent20",
        position_ticket="ticket-agent20",
        order_id="order-agent20-open",
        deal_id="deal-agent20-open",
        fill_price="2500",
        fill_volume="0.05",
        remaining_volume="0.05",
        sl="2495",
        tp="2515",
        commission="-2",
        position_version=1,
    )
    assert service.ingest_open_transaction(
        open_event,
        account_id="demo-10001",
        symbol="XAUUSD",
        magic=202412,
        deal=open_deal,
    )

    close_deal = BrokerDealRecord(
        "demo-10001",
        "deal-agent20-close",
        "position-agent20",
        "order-agent20-close",
        "OUT",
        "0.05",
        "2510",
        "26",
        "-1",
        "0",
        "0",
        (NOW + 20) / 1_000,
        trade_id="trade-1",
    )
    close_event = BrokerEvent(
        "event-agent20-close",
        BrokerEventType.POSITION_CLOSED,
        NOW + 20,
        NOW + 21,
        trade_id="trade-1",
        broker_position_id="position-agent20",
        position_ticket="ticket-agent20",
        order_id="order-agent20-close",
        deal_id="deal-agent20-close",
        fill_price="2510",
        fill_volume="0.05",
        remaining_volume="0",
        profit="26",
        commission="-1",
        position_version=2,
    )
    assert service.ingest_broker_event(
        close_event,
        account_id="demo-10001",
        symbol="XAUUSD",
        magic=202412,
        deal=close_deal,
    )

    job = ledger.get_pma_job_for_trade("trade-1")
    result = GovernedLearningService(ledger).process_due(
        "runtime-agent20",
        now_ms=job["due_at_ms"],
        target_config_hash="config-v1",
    )
    assert len(result) == 1
    assert result[0].status == "COMPLETED"
    source = ledger.list_learning_sources(
        account_id="demo-10001",
        symbol="XAUUSD",
        magic=202412,
        source_config_hash="config-v1",
        as_of_broker_time_ms=NOW + 20,
    )
    assert len(source) == 1
    assert source[0]["close_event_id"] == "event-agent20-close"
    assert source[0]["actual_r_decimal"] == "1"
