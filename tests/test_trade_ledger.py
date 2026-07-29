import dataclasses
import sqlite3
import stat
from decimal import Decimal

import pytest

import python_swarm.trade_ledger as ledger_module

from python_swarm.runtime_paths import RuntimePaths
from python_swarm.trade_ledger import (
    LEDGER_SCHEMA_VERSION,
    BrokerDealRecord,
    BrokerSnapshotRecord,
    LedgerConflictError,
    LedgerInvariantError,
    LedgerUnavailableError,
    OutboxRecord,
    OwnedPositionRecord,
    ProviderHealthRecord,
    ReconciliationItemRecord,
    ReconciliationRunRecord,
    RiskDecisionRecord,
    SchemaVersionError,
    TradeAttributionRecord,
    TradeEventRecord,
    TradeLedger,
    TradeRecord,
)


@pytest.fixture
def ledger(tmp_path):
    paths = RuntimePaths.resolve("test", data_dir=tmp_path).ensure_layout()
    value = TradeLedger(paths.trade_ledger_path, clock=lambda: 1_700_000_000.0).open()
    try:
        yield value
    finally:
        value.close()


def trade(**changes):
    values = {
        "trade_id": "trade-1",
        "request_id": "request-1",
        "account_id": "demo-10001",
        "symbol": "XAUUSD",
        "magic_number": 202412,
        "direction": "BUY",
        "decision_at": 1_700_000_001.0,
        "intended_entry": 2_500.0,
        "intended_sl": 2_495.0,
        "tp1": 2_505.0,
        "tp2": 2_510.0,
        "tp3": 2_515.0,
        "requested_volume": 0.1,
        "initial_risk_currency": 50.0,
        "regime_at_entry": "trending_bull",
        "session_at_entry": "LONDON",
        "metadata": {"config_version": "cfg-1"},
    }
    values.update(changes)
    return TradeRecord(**values)


def test_schema_is_versioned_wal_and_import_did_not_create_parent(tmp_path):
    db = tmp_path / "missing" / "ledger.db"
    with pytest.raises(LedgerUnavailableError, match="directory does not exist"):
        TradeLedger(db).open()
    assert not db.parent.exists()

    db.parent.mkdir()
    with TradeLedger(db) as value:
        assert value.schema_version == LEDGER_SCHEMA_VERSION
        value.require_healthy()
        assert value._connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        for path in (db, db.with_name(db.name + "-wal"), db.with_name(db.name + "-shm")):
            assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_trade_and_attribution_records_are_exactly_idempotent(ledger):
    record = trade()
    assert ledger.create_trade(record)
    assert not ledger.create_trade(record)
    with pytest.raises(LedgerConflictError, match="idempotency conflict"):
        ledger.create_trade(trade(direction="SELL"))

    attribution = TradeAttributionRecord(
        attribution_id="attr-1",
        trade_id="trade-1",
        agent_id=7,
        strategy_slug="ict_smc_core",
        direction="BUY",
        conviction=82.5,
        learned_weight=1.1,
        effective_weight=1.2,
        signal_at=1_700_000_000.5,
        tags=("FVG", "BOS"),
    )
    assert ledger.record_attribution(attribution)
    assert not ledger.record_attribution(attribution)
    with pytest.raises(LedgerConflictError):
        ledger.record_attribution(dataclasses.replace(attribution, conviction=10.0))


@pytest.mark.parametrize(
    "changes",
    (
        {"regime_at_entry": "TRENDING_BULL"},
        {"session_at_entry": "UNKNOWN_SESSION"},
    ),
)
def test_learning_eligible_trade_requires_canonical_context(ledger, changes):
    with pytest.raises(LedgerInvariantError, match="canonical regime and session"):
        ledger.create_trade(
            trade(
                evidence_class="CANONICAL_RUNTIME",
                learning_eligible=True,
                config_hash="config-v1",
                **changes,
            )
        )


def test_submit_ack_and_close_preserve_broker_identity_and_actual_r(ledger):
    ledger.create_trade(trade())
    assert ledger.mark_trade_submitted(
        "trade-1",
        event_id="event-submit",
        occurred_at=1_700_000_002.0,
        broker_order_id="order-9",
    )
    assert not ledger.mark_trade_submitted(
        "trade-1",
        event_id="event-submit",
        occurred_at=1_700_000_002.0,
        broker_order_id="order-9",
    )
    with pytest.raises(LedgerConflictError, match="different event ID"):
        ledger.mark_trade_submitted(
            "trade-1",
            event_id="event-submit-duplicate",
            occurred_at=1_700_000_002.0,
            broker_order_id="order-9",
        )
    assert ledger.acknowledge_trade_open(
        "trade-1",
        event_id="event-open",
        account_id="demo-10001",
        broker_order_id="order-9",
        broker_position_id="position-3",
        fill_price=2_500.2,
        filled_volume=0.1,
        opened_at=1_700_000_003.0,
    )
    with pytest.raises(LedgerConflictError, match="different event ID"):
        ledger.acknowledge_trade_open(
            "trade-1",
            event_id="event-open-duplicate",
            account_id="demo-10001",
            broker_order_id="order-9",
            broker_position_id="position-3",
            fill_price=2_500.2,
            filled_volume=0.1,
            opened_at=1_700_000_003.0,
        )
    ledger.record_deal(
        BrokerDealRecord(
            account_id="demo-10001",
            deal_id="deal-close-1",
            trade_id="trade-1",
            position_id="position-3",
            order_id="order-close-1",
            deal_type="ENTRY_OUT",
            volume=0.1,
            price=2_510.0,
            profit=76.0,
            commission=-1.0,
            swap=0.0,
            fee=0.0,
            occurred_at=1_700_000_099.0,
        )
    )
    actual_r = ledger.close_trade(
        "trade-1",
        event_id="event-close",
        exit_price=2_510.0,
        net_realized_pnl=999_999.0,
        closed_at=1_700_000_100.0,
        payload={"commission_included": True},
    )
    assert actual_r == pytest.approx(1.5)
    assert ledger.close_trade(
        "trade-1",
        event_id="event-close",
        exit_price=2_510.0,
        net_realized_pnl=-999_999.0,
        closed_at=1_700_000_100.0,
        payload={"commission_included": True},
    ) == pytest.approx(1.5)
    with pytest.raises(LedgerConflictError, match="different event ID"):
        ledger.close_trade(
            "trade-1",
            event_id="event-close-duplicate",
            exit_price=2_510.0,
            net_realized_pnl=75.0,
            closed_at=1_700_000_100.0,
            payload={"commission_included": True},
        )

    stored = ledger.get_trade("trade-1")
    assert stored["broker_order_id"] == "order-9"
    assert stored["broker_position_id"] == "position-3"
    assert stored["status"] == "CLOSED"
    assert stored["actual_r"] == pytest.approx(1.5)
    assert stored["filled_volume"] == pytest.approx(0.1)
    assert stored["remaining_volume"] == pytest.approx(0.0)
    assert [event["event_type"] for event in ledger.get_trade_events("trade-1")] == [
        "ORDER_SUBMITTED",
        "POSITION_OPENED",
        "TRADE_CLOSED",
    ]


def test_close_refuses_to_invent_r_without_initial_risk(ledger):
    ledger.create_trade(trade(initial_risk_currency=0.0))
    ledger.mark_trade_submitted(
        "trade-1", event_id="submit", occurred_at=2.0
    )
    ledger.acknowledge_trade_open(
        "trade-1",
        event_id="open",
        account_id="demo-10001",
        broker_order_id="order-1",
        broker_position_id="position-1",
        fill_price=2_500.0,
        filled_volume=0.1,
        opened_at=3.0,
    )
    ledger.record_deal(
        BrokerDealRecord(
            account_id="demo-10001",
            deal_id="close-deal",
            trade_id="trade-1",
            position_id="position-1",
            order_id="close-order",
            deal_type="ENTRY_OUT",
            volume=0.1,
            price=2_510.0,
            profit=20.0,
            commission=0.0,
            swap=0.0,
            fee=0.0,
            occurred_at=3.5,
        )
    )
    with pytest.raises(LedgerInvariantError, match="positive entry-time initial risk"):
        ledger.close_trade(
            "trade-1",
            event_id="close",
            exit_price=2_510.0,
            net_realized_pnl=20.0,
            closed_at=4.0,
        )
    assert [event["event_id"] for event in ledger.get_trade_events("trade-1")] == [
        "submit",
        "open",
    ]


def test_deal_risk_provider_outbox_and_reconciliation_records(ledger):
    ledger.create_trade(trade())
    deal = BrokerDealRecord(
        account_id="demo-10001",
        deal_id="deal-1",
        trade_id="trade-1",
        position_id="position-3",
        order_id="order-9",
        deal_type="ENTRY_IN",
        volume=0.1,
        price=2_500.2,
        profit=0.0,
        commission=-0.5,
        swap=0.0,
        fee=0.0,
        occurred_at=10.0,
    )
    assert ledger.record_deal(deal)
    assert not ledger.record_deal(deal)

    risk = RiskDecisionRecord(
        decision_id="risk-1",
        request_id="request-1",
        trade_id="trade-1",
        approved=True,
        reason_code="APPROVED",
        decided_at=9.0,
        equity=10_000.0,
        balance=10_000.0,
        risk_pct=0.5,
        risk_currency=50.0,
        requested_volume=0.1,
        approved_volume=0.1,
    )
    assert ledger.record_risk_decision(risk)
    assert not ledger.record_risk_decision(risk)

    health = ProviderHealthRecord(
        observation_id="provider-1",
        provider_name="macro",
        state="HEALTHY",
        observed_at=11.0,
        source_timestamp=10.0,
        detail={"source": "broker"},
    )
    assert ledger.record_provider_health(health)
    assert ledger.latest_provider_health("macro")["state"] == "HEALTHY"

    event = TradeEventRecord(
        event_id="notify-event",
        event_type="TRADE_INTENT_RECORDED",
        source="python",
        occurred_at=12.0,
        trade_id="trade-1",
        payload={},
    )
    ledger.append_event(event)
    message = OutboxRecord(
        message_id="message-1",
        idempotency_key="trade-1:intent:telegram",
        event_id="notify-event",
        destination="telegram",
        topic="trade.intent",
        payload={"trade_id": "trade-1"},
        created_at=12.0,
    )
    assert ledger.enqueue_outbox(message)
    assert not ledger.enqueue_outbox(message)
    claimed = ledger.claim_outbox("worker-a", now=13.0)
    assert [item["message_id"] for item in claimed] == ["message-1"]
    assert ledger.claim_outbox("worker-b", now=13.0) == []
    assert ledger.mark_outbox_delivered("message-1", "worker-a", delivered_at=14.0)
    assert not ledger.mark_outbox_delivered("message-1", "worker-a", delivered_at=14.0)

    run = ReconciliationRunRecord(
        run_id="recon-1",
        connection_epoch="epoch-2",
        account_id="demo-10001",
        started_at=15.0,
        broker_position_count=1,
        ledger_position_count=0,
        snapshot_hash="sha256:abc",
    )
    assert ledger.start_reconciliation(run)
    assert not ledger.start_reconciliation(run)
    item = ReconciliationItemRecord(
        run_id="recon-1",
        item_id="item-1",
        discrepancy_type="BROKER_ONLY_POSITION",
        broker_position_id="position-unknown",
        action="HALT_AND_IMPORT",
    )
    assert ledger.record_reconciliation_item(item)
    assert ledger.complete_reconciliation(
        "recon-1", succeeded=False, mismatch_count=1, finished_at=16.0
    )
    assert not ledger.complete_reconciliation(
        "recon-1", succeeded=False, mismatch_count=1, finished_at=16.0
    )


def test_unattributed_broker_exposure_is_visible_blocking_and_never_canonical(ledger):
    run = ReconciliationRunRecord(
        run_id="recon-quarantine-1",
        connection_epoch="epoch-q1",
        account_id="demo-10001",
        started_at=1_700_000_020.0,
        broker_position_count=1,
        ledger_position_count=0,
        snapshot_hash="hash-q1",
    )
    snapshot = BrokerSnapshotRecord(
        snapshot_id="snapshot-q1",
        connection_epoch="epoch-q1",
        account_id="demo-10001",
        broker_server="Broker-Demo",
        trade_mode="DEMO",
        symbol="XAUUSD",
        magic=202412,
        captured_at_ms=1_700_000_020_000,
        snapshot_hash="hash-q1",
    )
    issue = ReconciliationItemRecord(
        run_id=run.run_id,
        item_id="broker-only-q1",
        discrepancy_type="BROKER_ONLY_POSITION",
        action="HALT_AND_QUARANTINE",
        broker_position_id="position-q1",
        resolved=False,
    )
    raw = OwnedPositionRecord(
        account_id="demo-10001",
        trade_id=None,
        broker_position_id="position-q1",
        position_ticket="ticket-q1",
        broker_order_id=None,
        symbol="XAUUSD",
        magic=202412,
        direction="BUY",
        initial_volume="0.1",
        remaining_volume="0.1",
        entry_price="2500",
        original_sl="0",
        current_sl="2495",
        tp1="0",
        tp2="0",
        tp3="2510",
        initial_risk_amount="0",
        lifecycle_stage="QUARANTINED",
        position_version=7,
        opened_at_ms=1_700_000_000_000,
        updated_at_ms=1_700_000_020_000,
    )

    ledger.persist_reconciliation_atomic(
        run=run,
        snapshot=snapshot,
        items=(issue,),
        owned_positions=(raw,),
        pending_orders=(),
        finished_at=1_700_000_021.0,
        succeeded=False,
        replace_scope=True,
    )

    assert ledger.list_owned_positions("demo-10001", "XAUUSD", 202412) == []
    assert ledger.has_active_quarantined_exposure(
        "demo-10001", "XAUUSD", 202412
    )
    quarantined = ledger.list_quarantined_exposures(
        "demo-10001", "XAUUSD", 202412
    )
    assert len(quarantined) == 1
    assert quarantined[0]["broker_sl_decimal"] == "2495"
    assert quarantined[0]["broker_tp_decimal"] == "2510"
    assert quarantined[0]["reason"] == "BROKER_ONLY_POSITION"

    cleared_run = dataclasses.replace(
        run,
        run_id="recon-quarantine-2",
        connection_epoch="epoch-q2",
        started_at=1_700_000_030.0,
        broker_position_count=0,
    )
    cleared_snapshot = dataclasses.replace(
        snapshot,
        snapshot_id="snapshot-q2",
        connection_epoch="epoch-q2",
        captured_at_ms=1_700_000_030_000,
        snapshot_hash="hash-q2",
    )
    ledger.persist_reconciliation_atomic(
        run=cleared_run,
        snapshot=cleared_snapshot,
        items=(),
        owned_positions=(),
        pending_orders=(),
        finished_at=1_700_000_031.0,
        succeeded=True,
        replace_scope=True,
    )
    assert not ledger.has_active_quarantined_exposure(
        "demo-10001", "XAUUSD", 202412
    )


def test_unknown_unversioned_database_is_refused(tmp_path):
    db = tmp_path / "unknown.db"
    connection = sqlite3.connect(db)
    connection.execute("CREATE TABLE legacy_unknown(id INTEGER)")
    connection.commit()
    connection.close()
    with pytest.raises(SchemaVersionError, match="unversioned non-empty"):
        TradeLedger(db).open()


def test_schema_v1_is_migrated_additively_to_v2(tmp_path):
    db = tmp_path / "v1.db"
    connection = sqlite3.connect(db)
    for statement in ledger_module._SCHEMA_V1:
        connection.execute(statement)
    connection.execute(
        "INSERT INTO schema_meta(key,value) VALUES('schema_version','1')"
    )
    connection.execute("PRAGMA user_version=1")
    connection.commit()
    connection.close()

    with TradeLedger(db) as migrated:
        assert migrated.schema_version == LEDGER_SCHEMA_VERSION
        tables = {
            row[0]
            for row in migrated._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert {"broker_commands", "owned_positions", "deal_cursors"} <= tables
        columns = {
            row[1] for row in migrated._connection.execute("PRAGMA table_info(trades)")
        }
        assert "remaining_volume" in columns
        position_columns = {
            row[1]
            for row in migrated._connection.execute("PRAGMA table_info(owned_positions)")
        }
        assert {"pending_close_volume", "pending_filled_volume"} <= position_columns
        deal_columns = {
            row[1]
            for row in migrated._connection.execute("PRAGMA table_info(broker_deals)")
        }
        assert {
            "command_id",
            "volume_decimal",
            "price_decimal",
            "profit_decimal",
            "commission_decimal",
            "swap_decimal",
            "fee_decimal",
        } <= deal_columns
        trade_columns = {
            row[1] for row in migrated._connection.execute("PRAGMA table_info(trades)")
        }
        assert {
            "initial_risk_currency_decimal",
            "filled_volume_decimal",
            "remaining_volume_decimal",
            "actual_r_decimal",
        } <= trade_columns
        risk_columns = {
            row[1]
            for row in migrated._connection.execute(
                "PRAGMA table_info(daily_risk_state)"
            )
        }
        assert "current_balance" in risk_columns


def test_deal_page_and_cursor_roll_back_together_on_conflict(ledger):
    first = BrokerDealRecord(
        account_id="demo-10001",
        deal_id="deal-conflict",
        trade_id=None,
        position_id="position-x",
        order_id="order-x",
        deal_type="ENTRY_IN",
        volume=0.1,
        price=2500.0,
        profit=0.0,
        commission=0.0,
        swap=0.0,
        fee=0.0,
        occurred_at=20.0,
    )
    conflicting = dataclasses.replace(first, price=2501.0)
    with pytest.raises(LedgerConflictError):
        ledger.record_deal_page_and_advance_cursor(
            account_id="demo-10001",
            stream="history",
            expected_cursor=None,
            next_cursor="cursor-1",
            connection_epoch="epoch-1",
            deals=(first, conflicting),
            updated_at_ms=20_000,
        )
    assert ledger.get_deal_cursor("demo-10001") is None
    assert ledger.record_deal(first)


def test_closed_ledger_fails_closed(ledger):
    ledger.close()
    with pytest.raises(LedgerUnavailableError, match="not open"):
        ledger.get_trade("trade-1")


def test_authoritative_financial_values_use_exact_decimal_text(ledger):
    exact_risk = Decimal("50.0000000000000000001")
    ledger.create_trade(trade(initial_risk_currency=exact_risk))
    exact_volume = Decimal("0.1000000000000000001")
    deal = BrokerDealRecord(
        account_id="demo-10001",
        deal_id="decimal-deal",
        trade_id="trade-1",
        position_id="position-decimal",
        order_id="order-decimal",
        deal_type="ENTRY_IN",
        volume=exact_volume,
        price=Decimal("2500.1234567890123456789"),
        profit=Decimal("0.0000000000000000001"),
        commission=Decimal("-0.1234567890123456789"),
        swap=Decimal("0"),
        fee=Decimal("0"),
        occurred_at=20.0,
        command_id="open-decimal-command",
    )
    assert ledger.record_deal(deal)
    stored_trade = ledger.get_trade("trade-1")
    assert stored_trade["initial_risk_currency_decimal"] == str(exact_risk)
    stored_deal = ledger.get_trade_deals("trade-1")[0]
    assert stored_deal["volume"] == str(exact_volume)
    assert stored_deal["price"] == "2500.1234567890123456789"
    assert stored_deal["commission"] == "-0.1234567890123456789"
    assert stored_deal["command_id"] == "open-decimal-command"

    with pytest.raises(LedgerConflictError):
        ledger.record_deal(
            dataclasses.replace(
                deal, volume=Decimal("0.1000000000000000002")
            )
        )
