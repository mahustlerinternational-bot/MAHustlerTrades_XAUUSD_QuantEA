import asyncio
from dataclasses import replace
from decimal import Decimal

import pytest

from python_swarm.domain_models import FixedClock
from python_swarm.reconciliation import (
    AuthoritativeBrokerSnapshot,
    BrokerDealPage,
    BrokerRiskSnapshot,
    ReconciliationLedgerError,
    ReconciliationService,
)
from python_swarm.runtime_paths import RuntimePaths
from python_swarm.runtime_state import (
    BrokerIdentity,
    BrokerTradeMode,
    ControlPlaneState,
    ExpectedBrokerIdentity,
    RuntimeStateMachine,
)
from python_swarm.trade_ledger import (
    BrokerCommandRecord,
    BrokerDealRecord,
    DailyRiskStateRecord,
    LedgerConflictError,
    LedgerInvariantError,
    OutboxRecord,
    OwnedPositionRecord,
    PendingOrderRecord,
    RiskReservationRecord,
    TradeLedger,
    TradeEventRecord,
    TradeRecord,
)


NOW = 1_784_116_800_000  # 2026-07-15 12:00:00 UTC
EXPECTED = ExpectedBrokerIdentity(
    "demo-10001", "Broker-Demo", "XAUUSD", 202412
)
IDENTITY = BrokerIdentity(
    "demo-10001",
    "Broker-Demo",
    BrokerTradeMode.DEMO,
    "XAUUSD",
    202412,
    "epoch-2",
    True,
    True,
)


class SnapshotBroker:
    def __init__(self, snapshot, pages):
        self.snapshot = snapshot
        self.pages = list(pages)

    async def fetch_snapshot(self, expected):
        return self.snapshot

    async def fetch_deal_page(self, account_id, cursor):
        page = self.pages.pop(0)
        assert page.previous_cursor == cursor
        return page


def syncing_runtime(clock):
    runtime = RuntimeStateMachine(clock)
    runtime.start_bootstrap()
    runtime.wait_for_ea(ledger_healthy=True, provider_gate_safe=True)
    runtime.accept_connection(
        EXPECTED, IDENTITY, transport_loopback=True, tuple_lock_acquired=True
    )
    return runtime


def snapshot(*, positions=(), pending_orders=(), risk=None):
    return AuthoritativeBrokerSnapshot(
        snapshot_id="snapshot-2",
        identity=IDENTITY,
        risk=risk
        or BrokerRiskSnapshot(
            "2026-07-15", Decimal("10000"), Decimal("10000"), Decimal("10000"), True, False
        ),
        positions=positions,
        pending_orders=pending_orders,
        captured_at_ms=NOW,
    )


def position(*, magic=202412, trade_id=None):
    return OwnedPositionRecord(
        account_id="demo-10001",
        trade_id=trade_id,
        broker_position_id="position-1",
        position_ticket="ticket-1",
        broker_order_id="order-1",
        symbol="XAUUSD",
        magic=magic,
        direction="BUY",
        initial_volume="0.1",
        remaining_volume="0.1",
        entry_price="2500",
        original_sl="2495",
        current_sl="2495",
        tp1="2505",
        tp2="2510",
        tp3="2515",
        initial_risk_amount="50",
        lifecycle_stage="PRE_TP1",
        position_version=1,
        opened_at_ms=NOW - 100,
        updated_at_ms=NOW,
    )


def seed_open_position(ledger):
    ledger.initialize_daily_risk_state(
        DailyRiskStateRecord(
            "demo-10001",
            "2026-07-15",
            "10000",
            "10000",
            True,
            False,
            NOW - 200,
            current_balance="10000",
        )
    )
    trade = TradeRecord(
        "trade-1",
        "intent-1",
        "demo-10001",
        "XAUUSD",
        202412,
        "BUY",
        (NOW - 200) / 1_000,
        2500,
        2495,
        2505,
        2510,
        2515,
        0.1,
        50,
    )
    reservation = RiskReservationRecord(
        "reservation-1",
        "trade-1",
        "demo-10001",
        "2026-07-15",
        "BUY",
        "50",
        "0.1",
        "RESERVED",
        NOW - 190,
        NOW + 10_000,
    )
    command = BrokerCommandRecord(
        "open-command",
        "trade-1",
        "reservation-1",
        "OPEN",
        "CREATED",
        {"position": "OPEN"},
        "epoch-2",
        1,
        NOW - 180,
        NOW + 9_000,
    )
    ledger.reserve_open_command_atomic(
        trade, (), reservation, command, max_total_risk="300", max_open_positions=3
    )
    ledger.mark_command_sent("open-command", sent_at_ms=NOW - 170)
    opened = position(trade_id="trade-1")
    ledger.acknowledge_open_command_atomic(
        "open-command",
        event=TradeEventRecord(
            "open-event",
            "POSITION_OPENED",
            "broker",
            (NOW - 160) / 1_000,
            {},
            trade_id="trade-1",
            correlation_id="open-command",
        ),
        position=opened,
    )
    return opened


def seed_lifecycle_command(
    ledger,
    base,
    *,
    action,
    expected_effect,
    pending_action,
    pending_close_volume="0",
    expires_at_ms=NOW - 10,
):
    command = BrokerCommandRecord(
        "lifecycle-command",
        "trade-1",
        "reservation-1",
        action,
        "CREATED",
        expected_effect,
        "epoch-2",
        2,
        NOW - 100,
        expires_at_ms,
        position_id="position-1",
        expected_position_version=base.position_version,
    )
    pending = replace(
        base,
        pending_action=pending_action,
        pending_milestone={
            "MODIFY_SL": "BREAK_EVEN",
            "PARTIAL_CLOSE": "TP1",
            "CLOSE": "TIMEOUT",
        }[pending_action],
        pending_command_id="lifecycle-command",
        pending_close_volume=Decimal(pending_close_volume),
        pending_filled_volume=Decimal("0"),
        updated_at_ms=NOW - 100,
    )
    ledger.create_lifecycle_command_atomic(command, position=pending)
    ledger.mark_command_sent("lifecycle-command", sent_at_ms=NOW - 90)
    ledger.mark_command_unknown(
        "lifecycle-command", unknown_at_ms=NOW - 80, reason="ack timeout"
    )
    return pending


def recovery_outbox(event):
    return (
        OutboxRecord(
            message_id=f"outbox:{event.event_id}",
            idempotency_key=f"projection:{event.event_id}",
            event_id=event.event_id,
            destination="website",
            topic="reconciliation.position_closed",
            payload={
                "event_id": event.event_id,
                "trade_id": event.trade_id,
                "event_type": event.event_type,
            },
            created_at=event.occurred_at,
        ),
    )


def seed_created_open_command(
    ledger,
    *,
    connection_epoch="epoch-2",
    expires_at_ms=NOW + 1_000,
):
    ledger.initialize_daily_risk_state(
        DailyRiskStateRecord(
            "demo-10001",
            "2026-07-15",
            "10000",
            "10000",
            True,
            False,
            NOW - 300,
            current_balance="10000",
        )
    )
    trade = TradeRecord(
        "trade-1",
        "intent-1",
        "demo-10001",
        "XAUUSD",
        202412,
        "BUY",
        (NOW - 250) / 1_000,
        2500,
        2495,
        2505,
        2510,
        2515,
        0.1,
        50,
    )
    reservation = RiskReservationRecord(
        "reservation-1",
        "trade-1",
        "demo-10001",
        "2026-07-15",
        "BUY",
        "50",
        "0.1",
        "RESERVED",
        NOW - 240,
        NOW + 10_000,
    )
    command = BrokerCommandRecord(
        "created-open",
        "trade-1",
        "reservation-1",
        "OPEN",
        "CREATED",
        {"position": "OPEN"},
        connection_epoch,
        1,
        NOW - 230,
        expires_at_ms,
    )
    ledger.reserve_open_command_atomic(
        trade, (), reservation, command, max_total_risk="300", max_open_positions=3
    )
    return command


def seed_unknown_open_recovery_case(ledger, fault):
    ledger.initialize_daily_risk_state(
        DailyRiskStateRecord(
            "demo-10001",
            "2026-07-15",
            "10000",
            "10000",
            True,
            False,
            NOW - 300,
            current_balance="10000",
        )
    )
    trade = TradeRecord(
        "trade-1",
        "intent-1",
        "demo-10001",
        "XAUUSD",
        202412,
        "BUY",
        (NOW - 250) / 1_000,
        "2500",
        "2495",
        "2505",
        "2510",
        "2515",
        "0.1",
        "50",
    )
    reservation = RiskReservationRecord(
        "reservation-1",
        "trade-1",
        "demo-10001",
        "2026-07-15",
        "BUY",
        "50",
        "0.1",
        "RESERVED",
        NOW - 240,
        NOW + 10_000,
    )
    expected_effect = {
        "event_type": "POSITION_OPENED",
        "trade_id": "trade-1",
        "reservation_id": "reservation-1",
        "account_id": "demo-10001",
        "symbol": "XAUUSD",
        "magic": 202412,
        "direction": "BUY",
        "initial_volume": "0.1",
        "original_sl": "2495",
        "tp1": "2505",
        "tp2": "2510",
        "tp3": "2515",
        "initial_risk_amount": "50",
    }
    payload = {
        "action": "OPEN",
        "trade_id": "trade-1",
        "reservation_id": "reservation-1",
        "account_id": "demo-10001",
        "symbol": "XAUUSD",
        "magic": 202412,
        "direction": "BUY",
        "volume": "0.1",
        "risk_amount": "50",
        "entry": "2501" if fault == "command_entry" else "2500",
        "sl": "2495",
        "tp1": "2505",
        "tp2": "2510",
        "tp3": "2515",
    }
    command = BrokerCommandRecord(
        "command-1",
        "trade-1",
        "reservation-1",
        "OPEN",
        "CREATED",
        expected_effect,
        "epoch-2",
        1,
        NOW - 230,
        NOW + 9_000,
        payload=payload,
    )
    ledger.reserve_open_command_atomic(
        trade, (), reservation, command, max_total_risk="300", max_open_positions=3
    )
    ledger.mark_command_sent("command-1", sent_at_ms=NOW - 200)
    ledger.mark_command_unknown(
        "command-1", unknown_at_ms=NOW - 180, reason="process crash"
    )
    raw = replace(
        position(),
        broker_order_id=None,
        original_sl=Decimal("0"),
        current_sl=Decimal("2496") if fault == "broker_sl" else Decimal("2495"),
        tp1=Decimal("0"),
        tp2=Decimal("0"),
        initial_risk_amount=Decimal("0"),
        lifecycle_stage="QUARANTINED",
    )
    payload_wire_command = "other-command" if fault == "wire_command" else "command-1"
    deal = BrokerDealRecord(
        "demo-10001",
        "open-deal",
        "position-1",
        "order-1",
        "OUT" if fault == "deal_type" else "IN",
        "0.09" if fault == "deal_volume" else "0.1",
        "2500",
        "0",
        "-1",
        "0",
        "0",
        (NOW - (99 if fault == "deal_time" else 100)) / 1_000,
        trade_id="trade-1",
        command_id="command-1",
        payload={
            "attribution_status": "VERIFIED",
            "wire_trade_id": "trade-1",
            "wire_command_id": payload_wire_command,
            "direction": "BUY",
            "entry_type": "IN",
        },
    )
    deals = (deal,)
    if fault == "duplicate_deal":
        deals += (replace(deal, deal_id="open-deal-2"),)
    return raw, deals


def seed_closed_loss(ledger, ordinal):
    trade_id = f"loss-trade-{ordinal}"
    position_id = f"loss-position-{ordinal}"
    opened_at = (NOW - 60_000 + ordinal * 1_000) / 1_000
    closed_at = (NOW - 30_000 + ordinal * 1_000) / 1_000
    ledger.create_trade(
        TradeRecord(
            trade_id=trade_id,
            request_id=f"loss-intent-{ordinal}",
            account_id="demo-10001",
            symbol="XAUUSD",
            magic_number=202412,
            direction="BUY",
            decision_at=opened_at - 1,
            intended_entry="2500",
            intended_sl="2495",
            tp1="2505",
            tp2="2510",
            tp3="2515",
            requested_volume="0.1",
            initial_risk_currency="50",
        )
    )
    ledger.mark_trade_submitted(
        trade_id,
        event_id=f"loss-submitted-{ordinal}",
        occurred_at=opened_at - 0.5,
        broker_order_id=f"loss-open-order-{ordinal}",
    )
    ledger.acknowledge_trade_open(
        trade_id,
        event_id=f"loss-opened-{ordinal}",
        account_id="demo-10001",
        broker_order_id=f"loss-open-order-{ordinal}",
        broker_position_id=position_id,
        fill_price=2500,
        filled_volume=0.1,
        opened_at=opened_at,
        broker_timestamp=opened_at,
    )
    deal = BrokerDealRecord(
        account_id="demo-10001",
        deal_id=f"loss-close-deal-{ordinal}",
        trade_id=trade_id,
        position_id=position_id,
        order_id=f"loss-close-order-{ordinal}",
        deal_type="OUT",
        volume="0.1",
        price="2499",
        profit="-10",
        commission="0",
        swap="0",
        fee="0",
        occurred_at=closed_at,
    )
    ledger.record_deal(deal)
    ledger.close_trade(
        trade_id,
        event_id=f"loss-closed-{ordinal}",
        exit_price=2499,
        net_realized_pnl=0,
        closed_at=closed_at,
        broker_timestamp=closed_at,
    )
    return deal


@pytest.fixture
def ledger(tmp_path):
    paths = RuntimePaths.resolve("test", data_dir=tmp_path).ensure_layout()
    value = TradeLedger(paths.trade_ledger_path, clock=lambda: NOW / 1_000).open()
    try:
        yield value
    finally:
        value.close()


def test_clean_startup_initializes_cursor_baseline_and_grants_ready(ledger):
    clock = FixedClock(NOW)
    runtime = syncing_runtime(clock)
    service = ReconciliationService(
        ledger=ledger,
        runtime=runtime,
        broker=SnapshotBroker(
            snapshot(), [BrokerDealPage(None, "cursor-0", (), True)]
        ),
        clock=clock,
    )
    report = asyncio.run(service.reconcile(EXPECTED))
    assert report.succeeded
    assert report.deal_cursor == "cursor-0"
    assert runtime.state is ControlPlaneState.READY
    assert ledger.get_daily_risk_state("demo-10001", "2026-07-15")[
        "baseline_verified"
    ]
    assert report.snapshot.snapshot_id == "snapshot-2"
    assert report.snapshot.identity is IDENTITY


def test_consecutive_loss_halt_is_derived_from_day_deals_not_snapshot_defaults(ledger):
    deals = tuple(seed_closed_loss(ledger, ordinal) for ordinal in range(1, 4))
    runtime = syncing_runtime(FixedClock(NOW))
    report = asyncio.run(
        ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=SnapshotBroker(
                snapshot(),
                [BrokerDealPage(None, "cursor-losses", deals, True)],
            ),
            clock=FixedClock(NOW),
            consecutive_loss_limit=3,
        ).reconcile(EXPECTED)
    )

    assert not report.succeeded
    assert runtime.state is ControlPlaneState.HALTED
    issue = next(
        item for item in report.blocking_issues
        if item.discrepancy_type == "DAILY_RISK_HALTED"
    )
    assert "CONSECUTIVE_LOSS_LIMIT" in issue.detail["reasons"]
    assert issue.detail["realised_pnl"] == "-30"
    assert issue.detail["consecutive_losses"] == 3
    state = ledger.get_daily_risk_state("demo-10001", "2026-07-15")
    assert state["realised_pnl"] == "-30"
    assert state["consecutive_losses"] == 3
    assert state["halted"] == 1


@pytest.mark.parametrize(
    ("connection_epoch", "expires_at_ms", "reason"),
    (
        ("epoch-1", NOW + 1_000, "OLD_CONNECTION_EPOCH"),
        ("epoch-2", NOW - 1, "CREATED_ENVELOPE_EXPIRED"),
    ),
)
def test_obsolete_created_open_without_any_broker_effect_releases_risk(
    ledger, connection_epoch, expires_at_ms, reason
):
    seed_created_open_command(
        ledger,
        connection_epoch=connection_epoch,
        expires_at_ms=expires_at_ms,
    )
    runtime = syncing_runtime(FixedClock(NOW))
    report = asyncio.run(
        ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=SnapshotBroker(
                snapshot(), [BrokerDealPage(None, "cursor-created-open", (), True)]
            ),
            clock=FixedClock(NOW),
        ).reconcile(EXPECTED)
    )
    assert report.succeeded
    assert ledger.get_command("created-open")["status"] == "RECONCILED_NOT_APPLIED"
    assert ledger.get_command("created-open")["last_error"] == reason
    assert ledger.get_reservation("reservation-1")["state"] == "RELEASED"
    assert ledger.get_trade("trade-1")["status"] == "CANCELLED"
    assert {
        item.discrepancy_type for item in report.issues
    } >= {"UNSENT_OPEN_COMMAND_RECONCILED_NOT_APPLIED"}
    assert runtime.state is ControlPlaneState.READY


def test_live_created_open_remains_frozen_for_exact_resume(ledger):
    seed_created_open_command(ledger)
    runtime = syncing_runtime(FixedClock(NOW))
    report = asyncio.run(
        ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=SnapshotBroker(
                snapshot(), [BrokerDealPage(None, "cursor-live-open", (), True)]
            ),
            clock=FixedClock(NOW),
        ).reconcile(EXPECTED)
    )
    assert report.succeeded
    assert ledger.get_command("created-open")["status"] == "CREATED"
    assert ledger.get_reservation("reservation-1")["state"] == "RESERVED"
    assert ledger.get_trade("trade-1")["status"] == "RISK_APPROVED"
    assert "UNSENT_COMMAND_RETAINED" in {
        item.discrepancy_type for item in report.issues
    }


def test_obsolete_created_open_with_pending_order_never_releases_risk(ledger):
    seed_created_open_command(
        ledger, connection_epoch="epoch-1", expires_at_ms=NOW + 1_000
    )
    pending = PendingOrderRecord(
        account_id="demo-10001",
        broker_order_id="pending-created-open",
        symbol="XAUUSD",
        magic=202412,
        order_type="BUY_LIMIT",
        volume="0.1",
        price="2500",
        sl="2495",
        tp="2515",
        state="PLACED",
        created_at_ms=NOW - 100,
        updated_at_ms=NOW,
        trade_id="trade-1",
        command_id="created-open",
    )
    runtime = syncing_runtime(FixedClock(NOW))
    report = asyncio.run(
        ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=SnapshotBroker(
                snapshot(pending_orders=(pending,)),
                [BrokerDealPage(None, "cursor-created-effect", (), True)],
            ),
            clock=FixedClock(NOW),
        ).reconcile(EXPECTED)
    )
    assert not report.succeeded
    assert ledger.get_command("created-open")["status"] == "CREATED"
    assert ledger.get_reservation("reservation-1")["state"] == "RESERVED"
    assert "UNSENT_OPEN_COMMAND_EFFECT_AMBIGUOUS" in {
        item.discrepancy_type for item in report.blocking_issues
    }


def test_obsolete_created_open_with_attributed_deal_never_releases_risk(ledger):
    seed_created_open_command(
        ledger, connection_epoch="epoch-1", expires_at_ms=NOW + 1_000
    )
    contradictory_deal = BrokerDealRecord(
        "demo-10001",
        "created-open-deal",
        "unexpected-position",
        "unexpected-order",
        "IN",
        "0.1",
        "2500",
        "0",
        "0",
        "0",
        "0",
        (NOW - 50) / 1_000,
        trade_id="trade-1",
        command_id="created-open",
        payload={"attribution_status": "VERIFIED", "reason": "EXPERT"},
    )
    runtime = syncing_runtime(FixedClock(NOW))
    report = asyncio.run(
        ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=SnapshotBroker(
                snapshot(),
                [BrokerDealPage(None, "cursor-created-deal", (contradictory_deal,), True)],
            ),
            clock=FixedClock(NOW),
        ).reconcile(EXPECTED)
    )
    assert not report.succeeded
    assert ledger.get_command("created-open")["status"] == "CREATED"
    assert ledger.get_reservation("reservation-1")["state"] == "RESERVED"
    issue = next(
        item
        for item in report.blocking_issues
        if item.discrepancy_type == "UNSENT_OPEN_COMMAND_EFFECT_AMBIGUOUS"
    )
    assert issue.detail["attributed_deal_ids"] == ["created-open-deal"]


def test_attributed_position_without_exact_in_deal_cannot_recover_unknown_open(ledger):
    ledger.initialize_daily_risk_state(
        DailyRiskStateRecord(
            "demo-10001",
            "2026-07-15",
            "10000",
            "10000",
            True,
            False,
            NOW - 10,
            current_balance="10000",
        )
    )
    trade = TradeRecord(
        "trade-1",
        "intent-1",
        "demo-10001",
        "XAUUSD",
        202412,
        "BUY",
        (NOW - 100) / 1_000,
        2500,
        2495,
        2505,
        2510,
        2515,
        0.1,
        50,
    )
    reservation = RiskReservationRecord(
        "reservation-1",
        "trade-1",
        "demo-10001",
        "2026-07-15",
        "BUY",
        "50",
        "0.1",
        "RESERVED",
        NOW - 100,
        NOW + 1000,
    )
    command = BrokerCommandRecord(
        "command-1",
        "trade-1",
        "reservation-1",
        "OPEN",
        "CREATED",
        {"position": "OPEN"},
        "epoch-2",
        1,
        NOW - 90,
        NOW + 900,
    )
    ledger.reserve_open_command_atomic(
        trade, (), reservation, command, max_total_risk="300", max_open_positions=3
    )
    ledger.mark_command_sent("command-1", sent_at_ms=NOW - 80)
    ledger.mark_command_unknown(
        "command-1", unknown_at_ms=NOW - 70, reason="ack timeout"
    )

    clock = FixedClock(NOW)
    runtime = syncing_runtime(clock)
    runtime.update_evidence(blocking_diffs=("UNKNOWN_COMMAND:command-1",))
    runtime.transition(ControlPlaneState.DEGRADED, "unknown broker command")
    service = ReconciliationService(
        ledger=ledger,
        runtime=runtime,
        broker=SnapshotBroker(
            snapshot(positions=(position(trade_id="trade-1"),)),
            [BrokerDealPage(None, "cursor-1", (), True)],
        ),
        clock=clock,
    )
    report = asyncio.run(service.reconcile(EXPECTED))
    assert not report.succeeded
    assert "BROKER_ONLY_POSITION" in {
        item.discrepancy_type for item in report.blocking_issues
    }
    assert ledger.get_command("command-1")["status"] == "UNKNOWN"
    assert ledger.get_reservation("reservation-1")["state"] == "UNKNOWN"
    assert ledger.list_owned_positions("demo-10001", "XAUUSD", 202412) == []
    assert ledger.has_active_quarantined_exposure(
        "demo-10001", "XAUUSD", 202412
    )
    assert runtime.state is ControlPlaneState.HALTED


@pytest.mark.parametrize(
    "fault",
    (
        "deal_type",
        "deal_volume",
        "deal_time",
        "wire_command",
        "broker_sl",
        "command_entry",
        "duplicate_deal",
    ),
)
def test_crash_lost_open_recovery_rejects_every_incomplete_or_conflicting_proof(
    ledger, fault
):
    raw, deals = seed_unknown_open_recovery_case(ledger, fault)
    runtime = syncing_runtime(FixedClock(NOW))
    runtime.update_evidence(blocking_diffs=("UNKNOWN_COMMAND:command-1",))
    runtime.transition(ControlPlaneState.DEGRADED, "unknown broker command")

    report = asyncio.run(
        ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=SnapshotBroker(
                snapshot(positions=(raw,)),
                [BrokerDealPage(None, f"cursor-open-{fault}", deals, True)],
            ),
            clock=FixedClock(NOW),
        ).reconcile(EXPECTED)
    )

    assert not report.succeeded
    assert "UNREGISTERED_OPEN_RECOVERED" not in {
        item.discrepancy_type for item in report.issues
    }
    assert "BROKER_ONLY_POSITION" in {
        item.discrepancy_type for item in report.blocking_issues
    }
    assert ledger.get_command("command-1")["status"] == "UNKNOWN"
    assert ledger.get_reservation("reservation-1")["state"] == "UNKNOWN"
    assert ledger.list_owned_positions("demo-10001", "XAUUSD", 202412) == []
    assert ledger.has_active_quarantined_exposure(
        "demo-10001", "XAUUSD", 202412
    )
    assert runtime.state is ControlPlaneState.HALTED


def test_foreign_position_is_reported_and_halts_without_being_owned(ledger):
    clock = FixedClock(NOW)
    runtime = syncing_runtime(clock)
    service = ReconciliationService(
        ledger=ledger,
        runtime=runtime,
        broker=SnapshotBroker(
            snapshot(positions=(position(magic=999),)),
            [BrokerDealPage(None, "cursor-2", (), True)],
        ),
        clock=clock,
    )
    report = asyncio.run(service.reconcile(EXPECTED))
    assert not report.succeeded
    assert {item.discrepancy_type for item in report.blocking_issues} == {
        "FOREIGN_ACCOUNT_POSITION"
    }
    assert runtime.state is ControlPlaneState.HALTED
    assert ledger.list_owned_positions("demo-10001", "XAUUSD", 202412) == []


@pytest.mark.parametrize(
    ("bad_snapshot", "issue_type"),
    (
        (replace(snapshot(), captured_at_ms=NOW - 5_001), "STALE_OR_FUTURE_BROKER_SNAPSHOT"),
        (
            replace(snapshot(), risk=replace(snapshot().risk, risk_day="2026-07-14")),
            "BROKER_RISK_DAY_MISMATCH",
        ),
    ),
)
def test_stale_snapshot_or_wrong_utc_risk_day_never_initializes_baseline(
    ledger, bad_snapshot, issue_type
):
    clock = FixedClock(NOW)
    runtime = syncing_runtime(clock)
    service = ReconciliationService(
        ledger=ledger,
        runtime=runtime,
        broker=SnapshotBroker(bad_snapshot, []),
        clock=clock,
    )
    report = asyncio.run(service.reconcile(EXPECTED))
    assert issue_type in {item.discrepancy_type for item in report.blocking_issues}
    assert runtime.state is ControlPlaneState.HALTED
    assert ledger.get_daily_risk_state("demo-10001", "2026-07-15") is None


@pytest.mark.parametrize(
    ("applied", "observed_sl", "observed_version", "terminal"),
    (
        (True, "2497", 2, "RECONCILED_APPLIED"),
        (False, "2495", 1, "RECONCILED_NOT_APPLIED"),
    ),
)
def test_modify_sl_reconnect_requires_exact_effect_or_exact_no_effect(
    ledger, applied, observed_sl, observed_version, terminal
):
    base = seed_open_position(ledger)
    effect = {
        "action": "MODIFY_SL",
        "broker_position_id": "position-1",
        "expected_position_version": 1,
        "previous_sl": "2495",
        "target_sl": "2497",
    }
    pending = seed_lifecycle_command(
        ledger,
        base,
        action="MODIFY_SL",
        expected_effect=effect,
        pending_action="MODIFY_SL",
    )
    observed = replace(
        pending,
        current_sl=Decimal(observed_sl),
        position_version=observed_version,
        updated_at_ms=NOW,
    )
    runtime = syncing_runtime(FixedClock(NOW))
    report = asyncio.run(
        ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=SnapshotBroker(
                snapshot(positions=(observed,)),
                [BrokerDealPage(None, "cursor-modify", (), True)],
            ),
            clock=FixedClock(NOW),
        ).reconcile(EXPECTED)
    )
    assert report.succeeded
    assert ledger.get_command("lifecycle-command")["status"] == terminal
    stored = ledger.list_owned_positions("demo-10001", "XAUUSD", 202412)[0]
    assert stored["pending_command_id"] is None
    assert stored["pending_close_volume"] == "0"


def test_unexpired_lifecycle_absence_is_not_terminal_no_effect_proof(ledger):
    base = seed_open_position(ledger)
    effect = {
        "action": "MODIFY_SL",
        "broker_position_id": "position-1",
        "expected_position_version": 1,
        "previous_sl": "2495",
        "target_sl": "2497",
    }
    pending = seed_lifecycle_command(
        ledger,
        base,
        action="MODIFY_SL",
        expected_effect=effect,
        pending_action="MODIFY_SL",
        expires_at_ms=NOW + 1_000,
    )
    runtime = syncing_runtime(FixedClock(NOW))
    report = asyncio.run(
        ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=SnapshotBroker(
                snapshot(positions=(replace(pending, updated_at_ms=NOW),)),
                [BrokerDealPage(None, "cursor-unexpired", (), True)],
            ),
            clock=FixedClock(NOW),
        ).reconcile(EXPECTED)
    )
    assert not report.succeeded
    assert ledger.get_command("lifecycle-command")["status"] == "UNKNOWN"
    assert runtime.state is ControlPlaneState.HALTED


def test_created_lifecycle_command_survives_reconciliation_for_exact_resume(ledger):
    base = seed_open_position(ledger)
    effect = {
        "action": "MODIFY_SL",
        "broker_position_id": "position-1",
        "expected_position_version": 1,
        "previous_sl": "2495",
        "target_sl": "2497",
    }
    command = BrokerCommandRecord(
        "created-lifecycle",
        "trade-1",
        "reservation-1",
        "MODIFY_SL",
        "CREATED",
        effect,
        "epoch-2",
        2,
        NOW - 100,
        NOW + 1_000,
        position_id="position-1",
        expected_position_version=1,
    )
    pending = replace(
        base,
        pending_action="MODIFY_SL",
        pending_milestone="BREAK_EVEN",
        pending_command_id="created-lifecycle",
        updated_at_ms=NOW - 100,
    )
    ledger.create_lifecycle_command_atomic(command, position=pending)
    runtime = syncing_runtime(FixedClock(NOW))
    report = asyncio.run(
        ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=SnapshotBroker(
                snapshot(positions=(replace(base, updated_at_ms=NOW),)),
                [BrokerDealPage(None, "cursor-created", (), True)],
            ),
            clock=FixedClock(NOW),
        ).reconcile(EXPECTED)
    )
    assert report.succeeded
    assert ledger.get_command("created-lifecycle")["status"] == "CREATED"
    assert runtime.state is ControlPlaneState.READY


def test_partial_close_reconnect_accepts_exact_multi_deal_cumulative_fill(ledger):
    base = seed_open_position(ledger)
    effect = {
        "action": "PARTIAL_CLOSE",
        "broker_position_id": "position-1",
        "expected_position_version": 1,
        "previous_remaining_volume": "0.1",
        "target_remaining_volume": "0.08",
        "target_close_volume": "0.02",
    }
    pending = seed_lifecycle_command(
        ledger,
        base,
        action="PARTIAL_CLOSE",
        expected_effect=effect,
        pending_action="PARTIAL_CLOSE",
        pending_close_volume="0.02",
    )
    observed = replace(
        pending,
        remaining_volume=Decimal("0.08"),
        position_version=3,
        pending_filled_volume=Decimal("0.02"),
        updated_at_ms=NOW,
    )
    deals = tuple(
        BrokerDealRecord(
            "demo-10001",
            f"partial-{index}",
            "position-1",
            f"partial-order-{index}",
            "PARTIAL_CLOSE",
            0.01,
            2505,
            5,
            0,
            0,
            0,
            (NOW - 70 + index) / 1_000,
            trade_id="trade-1",
            command_id="lifecycle-command",
        )
        for index in (1, 2)
    )
    runtime = syncing_runtime(FixedClock(NOW))
    report = asyncio.run(
        ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=SnapshotBroker(
                snapshot(positions=(observed,)),
                [BrokerDealPage(None, "cursor-partial", deals, True)],
            ),
            clock=FixedClock(NOW),
        ).reconcile(EXPECTED)
    )
    assert report.succeeded
    assert ledger.get_command("lifecycle-command")["status"] == "RECONCILED_APPLIED"
    assert ledger.get_trade("trade-1")["status"] == "PARTIALLY_CLOSED"
    stored = ledger.list_owned_positions("demo-10001", "XAUUSD", 202412)[0]
    assert stored["pending_filled_volume"] == "0"


@pytest.mark.parametrize("applied", (True, False))
def test_close_reconnect_requires_absence_plus_full_closing_deal_or_exact_no_effect(
    ledger, applied
):
    base = seed_open_position(ledger)
    effect = {
        "action": "CLOSE",
        "broker_position_id": "position-1",
        "expected_position_version": 1,
        "previous_remaining_volume": "0.1",
        "target_remaining_volume": "0",
    }
    pending = seed_lifecycle_command(
        ledger,
        base,
        action="CLOSE",
        expected_effect=effect,
        pending_action="CLOSE",
        pending_close_volume="0.1",
    )
    if applied:
        positions = ()
        deals = (
            BrokerDealRecord(
                "demo-10001",
                "final-close",
                "position-1",
                "final-order",
                "ENTRY_OUT",
                0.1,
                2510,
                50,
                0,
                0,
                0,
                (NOW - 70) / 1_000,
                trade_id="trade-1",
                command_id="lifecycle-command",
            ),
        )
    else:
        positions = (replace(pending, updated_at_ms=NOW),)
        deals = ()
    runtime = syncing_runtime(FixedClock(NOW))
    report = asyncio.run(
        ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=SnapshotBroker(
                snapshot(positions=positions),
                [BrokerDealPage(None, "cursor-close", deals, True)],
            ),
            clock=FixedClock(NOW),
            outbox_projector=recovery_outbox,
        ).reconcile(EXPECTED)
    )
    assert report.succeeded
    expected_terminal = (
        "RECONCILED_APPLIED" if applied else "RECONCILED_NOT_APPLIED"
    )
    assert ledger.get_command("lifecycle-command")["status"] == expected_terminal
    assert ledger.get_trade("trade-1")["status"] == ("CLOSED" if applied else "OPEN")
    assert bool(ledger.list_owned_positions("demo-10001", "XAUUSD", 202412)) is not applied
    projected = ledger.claim_outbox("recovery-test", now=NOW / 1_000)
    assert bool(projected) is applied
    if applied:
        assert projected[0]["event_id"] == "lifecycle-command:reconciled-close"
        assert projected[0]["topic"] == "reconciliation.position_closed"


@pytest.mark.parametrize("reason", ("SL", "TP"))
def test_broker_triggered_sl_tp_close_is_applied_from_exact_recorded_deal(
    ledger, reason
):
    seed_open_position(ledger)
    deal = BrokerDealRecord(
        "demo-10001",
        f"broker-{reason.lower()}-close",
        "position-1",
        f"broker-{reason.lower()}-order",
        "OUT",
        "0.1",
        "2510",
        "50",
        "0",
        "0",
        "0",
        (NOW - 50) / 1_000,
        trade_id="trade-1",
        command_id=None,
        payload={
            "reason": reason,
            "attribution_status": "VERIFIED",
            # History replay cannot query a remaining position after an SL/TP
            # close. Snapshot absence plus exact cumulative OUT volume is the
            # authoritative zero-remaining proof.
            "remaining_volume_known": False,
            "remaining_volume": None,
        },
    )
    runtime = syncing_runtime(FixedClock(NOW))
    report = asyncio.run(
        ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=SnapshotBroker(
                snapshot(),
                [BrokerDealPage(None, f"cursor-{reason.lower()}", (deal,), True)],
            ),
            clock=FixedClock(NOW),
            outbox_projector=recovery_outbox,
        ).reconcile(EXPECTED)
    )
    assert report.succeeded
    trade = ledger.get_trade("trade-1")
    assert trade["status"] == "CLOSED"
    assert trade["actual_r_decimal"] == "1"
    assert trade["net_realized_pnl_decimal"] == "50"
    assert ledger.get_reservation("reservation-1")["state"] == "RELEASED"
    assert ledger.list_owned_positions("demo-10001", "XAUUSD", 202412) == []
    issue = next(
        item
        for item in report.issues
        if item.discrepancy_type == "BROKER_TRIGGERED_POSITION_CLOSED"
    )
    assert issue.detail["broker_reason"] == reason
    assert runtime.state is ControlPlaneState.READY
    projected = ledger.claim_outbox("recovery-test", now=NOW / 1_000)
    assert len(projected) == 1
    assert projected[0]["event_id"] == (
        f"broker-triggered-close:trade-1:broker-{reason.lower()}-close"
    )
    assert projected[0]["topic"] == "reconciliation.position_closed"


@pytest.mark.parametrize(
    ("deal_volume", "payload"),
    (
        (
            "0.1",
            {
                "reason": "EXPERT",
                "attribution_status": "VERIFIED",
            },
        ),
        (
            "0.1",
            {
                "reason": "TP",
                "attribution_status": "UNKNOWN",
            },
        ),
        (
            "0.09",
            {
                "reason": "SL",
                "attribution_status": "VERIFIED",
            },
        ),
    ),
)
def test_absent_position_without_exact_broker_triggered_close_proof_blocks(
    ledger, deal_volume, payload
):
    seed_open_position(ledger)
    deal = BrokerDealRecord(
        "demo-10001",
        "ambiguous-broker-close",
        "position-1",
        "ambiguous-broker-order",
        "OUT",
        deal_volume,
        "2510",
        "50",
        "0",
        "0",
        "0",
        (NOW - 50) / 1_000,
        trade_id="trade-1",
        command_id=None,
        payload=payload,
    )
    runtime = syncing_runtime(FixedClock(NOW))
    report = asyncio.run(
        ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=SnapshotBroker(
                snapshot(), [BrokerDealPage(None, "cursor-ambiguous-close", (deal,), True)]
            ),
            clock=FixedClock(NOW),
        ).reconcile(EXPECTED)
    )
    assert not report.succeeded
    assert ledger.get_trade("trade-1")["status"] == "RECONCILIATION_REQUIRED"
    assert "LEDGER_ONLY_POSITION" in {
        item.discrepancy_type for item in report.blocking_issues
    }
    assert "BROKER_TRIGGERED_POSITION_CLOSED" not in {
        item.discrepancy_type for item in report.issues
    }
    assert runtime.state is ControlPlaneState.HALTED


def test_recovery_close_and_outbox_projection_fail_as_one_transaction(ledger):
    seed_open_position(ledger)
    deal = BrokerDealRecord(
        "demo-10001",
        "broker-sl-close",
        "position-1",
        "broker-sl-order",
        "OUT",
        "0.1",
        "2495",
        "-50",
        "0",
        "0",
        "0",
        (NOW - 50) / 1_000,
        trade_id="trade-1",
        command_id=None,
        payload={
            "reason": "SL",
            "attribution_status": "VERIFIED",
            "remaining_volume_known": False,
            "remaining_volume": None,
        },
    )
    runtime = syncing_runtime(FixedClock(NOW))

    def wrong_event_projection(event):
        return (replace(recovery_outbox(event)[0], event_id="another-event"),)

    with pytest.raises(ReconciliationLedgerError):
        asyncio.run(
            ReconciliationService(
                ledger=ledger,
                runtime=runtime,
                broker=SnapshotBroker(
                    snapshot(),
                    [BrokerDealPage(None, "cursor-sl", (deal,), True)],
                ),
                clock=FixedClock(NOW),
                outbox_projector=wrong_event_projection,
            ).reconcile(EXPECTED)
        )

    assert ledger.get_trade("trade-1")["status"] == "OPEN"
    assert len(ledger.list_owned_positions("demo-10001", "XAUUSD", 202412)) == 1
    assert ledger.claim_outbox("recovery-test", now=NOW / 1_000) == []
    assert runtime.state is ControlPlaneState.HALTED


@pytest.mark.parametrize("deal_volume", (None, "0.03"))
def test_partial_close_overfill_or_missing_deal_proof_remains_blocking(
    ledger, deal_volume
):
    base = seed_open_position(ledger)
    effect = {
        "action": "PARTIAL_CLOSE",
        "broker_position_id": "position-1",
        "expected_position_version": 1,
        "previous_remaining_volume": "0.1",
        "target_remaining_volume": "0.08",
        "target_close_volume": "0.02",
    }
    pending = seed_lifecycle_command(
        ledger,
        base,
        action="PARTIAL_CLOSE",
        expected_effect=effect,
        pending_action="PARTIAL_CLOSE",
        pending_close_volume="0.02",
    )
    observed = replace(
        pending,
        remaining_volume=Decimal("0.08"),
        position_version=2,
        updated_at_ms=NOW,
    )
    runtime = syncing_runtime(FixedClock(NOW))
    deals = (
        ()
        if deal_volume is None
        else (
            BrokerDealRecord(
                "demo-10001",
                "overfill",
                "position-1",
                "overfill-order",
                "PARTIAL_CLOSE",
                deal_volume,
                "2505",
                "5",
                "0",
                "0",
                "0",
                (NOW - 70) / 1_000,
                trade_id="trade-1",
                command_id="lifecycle-command",
            ),
        )
    )
    report = asyncio.run(
        ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=SnapshotBroker(
                snapshot(positions=(observed,)),
                [BrokerDealPage(None, "cursor-ambiguous", deals, True)],
            ),
            clock=FixedClock(NOW),
        ).reconcile(EXPECTED)
    )
    assert not report.succeeded
    assert ledger.get_command("lifecycle-command")["status"] == "UNKNOWN"
    assert ledger.get_reservation("reservation-1")["state"] == "CONSUMED"
    assert runtime.state is ControlPlaneState.HALTED


@pytest.mark.parametrize("limit", (Decimal("3.01"), Decimal("100")))
def test_reconciliation_refuses_drawdown_limit_above_canonical_ceiling(
    ledger, limit
):
    with pytest.raises(ValueError, match=r"\(0,3\]"):
        ReconciliationService(
            ledger=ledger,
            runtime=syncing_runtime(FixedClock(NOW)),
            broker=SnapshotBroker(snapshot(), []),
            clock=FixedClock(NOW),
            daily_drawdown_pct_limit=limit,
        )


def test_reconciliation_enforces_the_configured_total_risk_below_hard_ceiling(
    ledger,
):
    opened = seed_open_position(ledger)
    runtime = syncing_runtime(FixedClock(NOW))
    report = asyncio.run(
        ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=SnapshotBroker(
                snapshot(positions=(opened,)),
                [BrokerDealPage(None, "cursor-configured-risk", (), True)],
            ),
            clock=FixedClock(NOW),
            max_total_risk_pct=Decimal("0.40"),
            max_open_positions=1,
        ).reconcile(EXPECTED)
    )

    issue = next(
        item for item in report.blocking_issues if item.discrepancy_type == "DAILY_RISK_HALTED"
    )
    assert "TOTAL_RISK_CONFIGURED_LIMIT" in issue.detail["reasons"]
    assert issue.detail["active_risk"] == "50"
    assert issue.detail["active_risk_limit"] == "40.00"
    assert issue.detail["position_count_limit"] == 1
    assert runtime.state is ControlPlaneState.HALTED


@pytest.mark.parametrize(
    ("kwargs", "message"),
    (
        ({"max_total_risk_pct": Decimal("3.01")}, "total risk limit"),
        ({"max_open_positions": 0}, "maximum open positions"),
        ({"max_open_positions": 4}, "maximum open positions"),
    ),
)
def test_reconciliation_refuses_risk_limits_outside_canonical_bounds(
    ledger, kwargs, message
):
    with pytest.raises(ValueError, match=message):
        ReconciliationService(
            ledger=ledger,
            runtime=syncing_runtime(FixedClock(NOW)),
            broker=SnapshotBroker(snapshot(), []),
            clock=FixedClock(NOW),
            **kwargs,
        )


@pytest.mark.parametrize(
    ("action", "effect", "pending_action", "pending_close", "message"),
    (
        (
            "MODIFY_SL",
            {
                "action": "MODIFY_SL",
                "broker_position_id": "position-1",
                "expected_position_version": 1,
                "previous_sl": "2494",
                "target_sl": "2497",
            },
            "MODIFY_SL",
            "0",
            "pre-state differs",
        ),
        (
            "MODIFY_SL",
            {
                "action": "MODIFY_SL",
                "broker_position_id": "position-1",
                "expected_position_version": 1,
                "previous_sl": "2495",
                "target_sl": "2494",
            },
            "MODIFY_SL",
            "0",
            "directionally improve",
        ),
        (
            "PARTIAL_CLOSE",
            {
                "action": "PARTIAL_CLOSE",
                "broker_position_id": "position-1",
                "expected_position_version": 1,
                "previous_remaining_volume": "0.09",
                "target_remaining_volume": "0.07",
                "target_close_volume": "0.02",
            },
            "PARTIAL_CLOSE",
            "0.02",
            "pre-state differs",
        ),
        (
            "PARTIAL_CLOSE",
            {
                "action": "PARTIAL_CLOSE",
                "broker_position_id": "position-1",
                "expected_position_version": 1,
                "previous_remaining_volume": "0.1",
                "target_remaining_volume": "0.07",
                "target_close_volume": "0.03",
            },
            "PARTIAL_CLOSE",
            "0.02",
            "persisted action plan",
        ),
        (
            "CLOSE",
            {
                "action": "CLOSE",
                "broker_position_id": "position-1",
                "expected_position_version": 1,
                "previous_remaining_volume": "0.1",
                "target_remaining_volume": "0",
            },
            "CLOSE",
            "0.05",
            "entire persisted remaining volume",
        ),
    ),
)
def test_lifecycle_command_effect_must_bind_to_persisted_position_state(
    ledger, action, effect, pending_action, pending_close, message
):
    base = seed_open_position(ledger)
    command = BrokerCommandRecord(
        "mutation-command",
        "trade-1",
        "reservation-1",
        action,
        "CREATED",
        effect,
        "epoch-2",
        2,
        NOW - 100,
        NOW + 1_000,
        position_id="position-1",
        expected_position_version=1,
    )
    pending = replace(
        base,
        pending_action=pending_action,
        pending_milestone={
            "MODIFY_SL": "BREAK_EVEN",
            "PARTIAL_CLOSE": "TP1",
            "CLOSE": "TIMEOUT",
        }[pending_action],
        pending_command_id="mutation-command",
        pending_close_volume=Decimal(pending_close),
        updated_at_ms=NOW - 100,
    )
    with pytest.raises(LedgerInvariantError, match=message):
        ledger.create_lifecycle_command_atomic(command, position=pending)
    assert ledger.get_command("mutation-command") is None
    assert ledger.list_owned_positions("demo-10001", "XAUUSD", 202412)[0][
        "pending_command_id"
    ] is None


@pytest.mark.parametrize("attribution", (None, "unrelated-command"))
def test_unattributed_or_other_command_deal_cannot_prove_partial_close(
    ledger, attribution
):
    base = seed_open_position(ledger)
    effect = {
        "action": "PARTIAL_CLOSE",
        "broker_position_id": "position-1",
        "expected_position_version": 1,
        "previous_remaining_volume": "0.1",
        "target_remaining_volume": "0.08",
        "target_close_volume": "0.02",
    }
    pending = seed_lifecycle_command(
        ledger,
        base,
        action="PARTIAL_CLOSE",
        expected_effect=effect,
        pending_action="PARTIAL_CLOSE",
        pending_close_volume="0.02",
    )
    observed = replace(
        pending,
        remaining_volume=Decimal("0.08"),
        position_version=2,
        pending_filled_volume=Decimal("0.02"),
        updated_at_ms=NOW,
    )
    deal = BrokerDealRecord(
        "demo-10001",
        f"manual-{attribution or 'none'}",
        "position-1",
        "manual-order",
        "PARTIAL_CLOSE",
        "0.02",
        "2505",
        "5",
        "0",
        "0",
        "0",
        (NOW - 70) / 1_000,
        trade_id="trade-1",
        command_id=attribution,
    )
    runtime = syncing_runtime(FixedClock(NOW))
    report = asyncio.run(
        ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=SnapshotBroker(
                snapshot(positions=(observed,)),
                [BrokerDealPage(None, "cursor-unattributed", (deal,), True)],
            ),
            clock=FixedClock(NOW),
        ).reconcile(EXPECTED)
    )
    assert not report.succeeded
    assert ledger.get_command("lifecycle-command")["status"] == "UNKNOWN"
    assert runtime.state is ControlPlaneState.HALTED


def test_multi_deal_partial_fill_is_received_until_exact_cumulative_completion(
    ledger,
):
    base = seed_open_position(ledger)
    effect = {
        "action": "PARTIAL_CLOSE",
        "broker_position_id": "position-1",
        "expected_position_version": 1,
        "previous_remaining_volume": "0.1",
        "target_remaining_volume": "0.08",
        "target_close_volume": "0.02",
    }
    pending = seed_lifecycle_command(
        ledger,
        base,
        action="PARTIAL_CLOSE",
        expected_effect=effect,
        pending_action="PARTIAL_CLOSE",
        pending_close_volume="0.02",
    )
    first = replace(
        pending,
        remaining_volume=Decimal("0.09"),
        position_version=2,
        pending_filled_volume=Decimal("0.01"),
        updated_at_ms=NOW + 1,
    )
    assert ledger.apply_position_update_atomic(
        event=TradeEventRecord(
            "partial-progress-1",
            "PARTIAL_FILLED",
            "broker",
            (NOW + 1) / 1_000,
            {},
            trade_id="trade-1",
            correlation_id="lifecycle-command",
        ),
        position=first,
        command_id="lifecycle-command",
    )
    assert ledger.get_command("lifecycle-command")["status"] == "RECEIVED"
    assert not ledger.mark_command_received(
        "lifecycle-command", received_at_ms=NOW + 2
    )
    complete = replace(
        first,
        remaining_volume=Decimal("0.08"),
        position_version=3,
        pending_action=None,
        pending_milestone=None,
        pending_command_id=None,
        pending_close_volume=Decimal("0"),
        pending_filled_volume=Decimal("0"),
        updated_at_ms=NOW + 3,
    )
    assert ledger.apply_position_update_atomic(
        event=TradeEventRecord(
            "partial-progress-2",
            "PARTIAL_FILLED",
            "broker",
            (NOW + 3) / 1_000,
            {},
            trade_id="trade-1",
            correlation_id="lifecycle-command",
        ),
        position=complete,
        command_id="lifecycle-command",
    )
    assert ledger.get_command("lifecycle-command")["status"] == "APPLIED"
    assert not ledger.mark_command_received(
        "lifecycle-command", received_at_ms=NOW + 4
    )



def test_verified_broker_baseline_promotes_matching_durable_unverified_baseline(
    ledger,
):
    ledger.initialize_daily_risk_state(
        DailyRiskStateRecord(
            account_id=EXPECTED.account_id,
            risk_day="2026-07-15",
            day_start_equity="10000",
            current_equity="10000",
            baseline_verified=False,
            halted=False,
            updated_at_ms=NOW - 1_000,
            current_balance="10000",
        )
    )

    clock = FixedClock(NOW)
    runtime = syncing_runtime(clock)

    report = asyncio.run(
        ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=SnapshotBroker(
                snapshot(
                    risk=BrokerRiskSnapshot(
                        "2026-07-15",
                        Decimal("10000"),
                        Decimal("10000"),
                        Decimal("10000"),
                        True,
                        False,
                    )
                ),
                [BrokerDealPage(None, "cursor-approved", (), True)],
            ),
            clock=clock,
        ).reconcile(EXPECTED)
    )

    state = ledger.get_daily_risk_state(
        EXPECTED.account_id,
        "2026-07-15",
    )

    assert report.succeeded
    assert runtime.state is ControlPlaneState.READY
    assert state is not None
    assert state["day_start_equity"] == "10000"
    assert state["baseline_verified"] == 1
    assert state["baseline_source"] == "BROKER_OPERATOR_APPROVED"


def test_verified_broker_baseline_cannot_replace_mismatched_durable_baseline(
    ledger,
):
    ledger.initialize_daily_risk_state(
        DailyRiskStateRecord(
            account_id=EXPECTED.account_id,
            risk_day="2026-07-15",
            day_start_equity="9999",
            current_equity="10000",
            baseline_verified=False,
            halted=False,
            updated_at_ms=NOW - 1_000,
            current_balance="10000",
        )
    )

    clock = FixedClock(NOW)
    runtime = syncing_runtime(clock)

    report = asyncio.run(
        ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=SnapshotBroker(
                snapshot(
                    risk=BrokerRiskSnapshot(
                        "2026-07-15",
                        Decimal("10000"),
                        Decimal("10000"),
                        Decimal("10000"),
                        True,
                        False,
                    )
                ),
                [BrokerDealPage(None, "cursor-mismatch", (), True)],
            ),
            clock=clock,
        ).reconcile(EXPECTED)
    )

    state = ledger.get_daily_risk_state(
        EXPECTED.account_id,
        "2026-07-15",
    )

    issue_types = {
        item.discrepancy_type
        for item in report.blocking_issues
    }

    assert not report.succeeded
    assert runtime.state is ControlPlaneState.HALTED
    assert "DAILY_BASELINE_MISMATCH" in issue_types
    assert state is not None
    assert state["day_start_equity"] == "9999"
    assert state["baseline_verified"] == 0
