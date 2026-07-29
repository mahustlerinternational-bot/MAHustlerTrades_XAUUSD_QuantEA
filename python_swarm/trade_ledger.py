"""Authoritative, versioned SQLite trade ledger.

The ledger is a fail-closed infrastructure boundary.  It stores canonical trade
aggregates plus append-only attribution, event, broker-deal, risk, provider,
outbox, and reconciliation records.  Every externally supplied record has an
idempotency key; replaying the same content is a no-op, while reusing a key with
different content raises ``LedgerConflictError``.

Importing this module performs no I/O.  Call :meth:`TradeLedger.open` explicitly
after a mode-isolated runtime directory has been created.
"""

from __future__ import annotations

import json
import hashlib
import math
import os
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time as datetime_time, timedelta
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence


LEDGER_SCHEMA_VERSION = 7
PMA_DEFAULT_DELAY_MS = 15 * 60 * 1_000
PMA_DEFAULT_MAX_ATTEMPTS = 5

TRADE_STATUSES = frozenset(
    {
        "INTENT",
        "RISK_APPROVED",
        "RISK_REJECTED",
        "SUBMITTED",
        "OPEN",
        "PARTIALLY_CLOSED",
        "CLOSE_PENDING",
        "CLOSED",
        "REJECTED",
        "CANCELLED",
        "RECONCILIATION_REQUIRED",
    }
)
PROVIDER_STATES = frozenset({"STARTING", "HEALTHY", "STALE", "ERROR", "DISABLED"})
OUTBOX_STATES = frozenset({"PENDING", "IN_FLIGHT", "DELIVERED", "FAILED"})
RECONCILIATION_STATES = frozenset({"STARTED", "SUCCEEDED", "FAILED"})
RESERVATION_STATES = frozenset(
    {"RESERVED", "SUBMITTED", "UNKNOWN", "CONSUMED", "RELEASED"}
)
COMMAND_STATES = frozenset(
    {
        "CREATED",
        "SENT",
        "RECEIVED",
        "APPLIED",
        "REJECTED",
        "UNKNOWN",
        "RECONCILED_APPLIED",
        "RECONCILED_NOT_APPLIED",
    }
)
CANONICAL_LEARNING_REGIMES = frozenset(
    {
        "trending_bull",
        "trending_bear",
        "ranging_tight",
        "ranging_wide",
        "breakout_imminent",
        "high_impact_news",
        "institutional_hunt",
        "regime_transition",
    }
)
CANONICAL_MARKET_SESSIONS = frozenset({"ASIA", "LONDON", "NEW_YORK", "OFF"})


class LedgerError(RuntimeError):
    """Base class for authoritative-ledger failures."""


class LedgerUnavailableError(LedgerError):
    """The ledger cannot durably complete an operation; trading must fail closed."""


class LedgerConflictError(LedgerError):
    """An idempotency key or broker identity was reused with different content."""


class LedgerInvariantError(LedgerError):
    """A requested record violates an authoritative trade invariant."""


class SchemaVersionError(LedgerError):
    """The database schema is unknown, incomplete, or newer than this runtime."""


_SQL_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")

_EXACT_INSERT_TABLES = frozenset(
    {
        "broker_commands",
        "broker_deals",
        "broker_snapshots",
        "daily_risk_state",
        "learning_activations",
        "learning_approvals",
        "learning_proposal_sources",
        "learning_proposals",
        "learning_snapshots",
        "learning_sources",
        "outbox",
        "pma_jobs",
        "provider_health",
        "reconciliation_items",
        "reconciliation_runs",
        "risk_decisions",
        "risk_reservations",
        "trade_attributions",
        "trade_events",
        "trades",
    }
)


def _sql_identifier(value: str, label: str = "SQL identifier") -> str:
    """Validate and quote one internally selected SQLite identifier."""

    if (
        not isinstance(value, str)
        or _SQL_IDENTIFIER_PATTERN.fullmatch(value) is None
    ):
        raise LedgerInvariantError(f"{label} is invalid")

    return f'"{value}"'


def _exact_insert_table(value: str) -> str:
    """Return a quoted table name from the private insert allowlist."""

    if value not in _EXACT_INSERT_TABLES:
        raise LedgerInvariantError(
            f"table is not approved for exact insertion: {value!r}"
        )

    return _sql_identifier(value, "table")


@dataclass(frozen=True, slots=True)
class TradeRecord:
    trade_id: str
    request_id: str
    account_id: str
    symbol: str
    magic_number: int
    direction: str
    decision_at: float
    intended_entry: Decimal | str | int | float
    intended_sl: Decimal | str | int | float
    tp1: Decimal | str | int | float
    tp2: Decimal | str | int | float
    tp3: Decimal | str | int | float
    requested_volume: Decimal | str | int | float
    initial_risk_currency: Decimal | str | int | float
    currency: str = "USD"
    regime_at_entry: str | None = None
    session_at_entry: str | None = None
    status: str = "INTENT"
    metadata: Mapping[str, Any] = field(default_factory=dict)
    config_hash: str | None = None
    evidence_class: str = "UNCLASSIFIED"
    learning_eligible: bool = False


@dataclass(frozen=True, slots=True)
class TradeAttributionRecord:
    attribution_id: str
    trade_id: str
    agent_id: int
    strategy_slug: str
    direction: str
    conviction: Decimal | str | int | float
    learned_weight: Decimal | str | int | float
    effective_weight: Decimal | str | int | float
    signal_at: float
    tags: Sequence[str] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TradeEventRecord:
    event_id: str
    event_type: str
    source: str
    occurred_at: float
    payload: Mapping[str, Any]
    trade_id: str | None = None
    correlation_id: str | None = None
    broker_timestamp: float | None = None


@dataclass(frozen=True, slots=True)
class BrokerDealRecord:
    account_id: str
    deal_id: str
    position_id: str
    order_id: str | None
    deal_type: str
    volume: Decimal | str | int | float
    price: Decimal | str | int | float
    profit: Decimal | str | int | float
    commission: Decimal | str | int | float
    swap: Decimal | str | int | float
    fee: Decimal | str | int | float
    occurred_at: float
    trade_id: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)
    command_id: str | None = None


@dataclass(frozen=True, slots=True)
class RiskDecisionRecord:
    decision_id: str
    request_id: str
    approved: bool
    reason_code: str
    decided_at: float
    equity: float
    balance: float
    risk_pct: float
    risk_currency: float
    requested_volume: float
    approved_volume: float
    trade_id: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ProviderHealthRecord:
    observation_id: str
    provider_name: str
    state: str
    observed_at: float
    source_timestamp: float | None = None
    detail: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    message_id: str
    idempotency_key: str
    destination: str
    topic: str
    payload: Mapping[str, Any]
    created_at: float
    available_at: float | None = None
    event_id: str | None = None


@dataclass(frozen=True, slots=True)
class ReconciliationRunRecord:
    run_id: str
    connection_epoch: str
    account_id: str
    started_at: float
    broker_position_count: int
    ledger_position_count: int
    snapshot_hash: str
    detail: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ReconciliationItemRecord:
    run_id: str
    item_id: str
    discrepancy_type: str
    action: str
    broker_position_id: str | None = None
    trade_id: str | None = None
    resolved: bool = False
    detail: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DailyRiskStateRecord:
    account_id: str
    risk_day: str
    day_start_equity: Decimal | str | int | float
    current_equity: Decimal | str | int | float
    baseline_verified: bool
    halted: bool
    updated_at_ms: int
    halt_reason: str | None = None
    baseline_source: str = "BROKER"
    realised_pnl: Decimal | str | int | float = Decimal("0")
    consecutive_losses: int = 0
    current_balance: Decimal | str | int | float | None = None


@dataclass(frozen=True, slots=True)
class RiskReservationRecord:
    reservation_id: str
    trade_id: str
    account_id: str
    risk_day: str
    direction: str
    risk_amount: Decimal | str | int | float
    volume: Decimal | str | int | float
    state: str
    created_at_ms: int
    expires_at_ms: int


@dataclass(frozen=True, slots=True)
class BrokerCommandRecord:
    command_id: str
    trade_id: str
    reservation_id: str
    action: str
    status: str
    expected_effect: Mapping[str, Any]
    connection_epoch: str
    sequence: int
    created_at_ms: int
    expires_at_ms: int
    payload: Mapping[str, Any] = field(default_factory=dict)
    position_id: str | None = None
    expected_position_version: int | None = None


@dataclass(frozen=True, slots=True)
class OwnedPositionRecord:
    account_id: str
    trade_id: str | None
    broker_position_id: str
    position_ticket: str
    broker_order_id: str | None
    symbol: str
    magic: int
    direction: str
    initial_volume: Decimal | str | int | float
    remaining_volume: Decimal | str | int | float
    entry_price: Decimal | str | int | float
    original_sl: Decimal | str | int | float
    current_sl: Decimal | str | int | float
    tp1: Decimal | str | int | float
    tp2: Decimal | str | int | float
    tp3: Decimal | str | int | float
    initial_risk_amount: Decimal | str | int | float
    lifecycle_stage: str
    position_version: int
    opened_at_ms: int
    updated_at_ms: int
    realised_pnl: Decimal | str | int | float = Decimal("0")
    pending_action: str | None = None
    pending_milestone: str | None = None
    pending_command_id: str | None = None
    pending_close_volume: Decimal | str | int | float = Decimal("0")
    pending_filled_volume: Decimal | str | int | float = Decimal("0")


@dataclass(frozen=True, slots=True)
class PendingOrderRecord:
    account_id: str
    broker_order_id: str
    symbol: str
    magic: int
    order_type: str
    volume: Decimal | str | int | float
    price: Decimal | str | int | float
    sl: Decimal | str | int | float | None
    tp: Decimal | str | int | float | None
    state: str
    created_at_ms: int
    updated_at_ms: int
    trade_id: str | None = None
    command_id: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class BrokerSnapshotRecord:
    snapshot_id: str
    connection_epoch: str
    account_id: str
    broker_server: str
    trade_mode: str
    symbol: str
    magic: int
    captured_at_ms: int
    snapshot_hash: str
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CloseTradeResult:
    actual_r: Decimal
    event_inserted: bool


@dataclass(frozen=True, slots=True)
class LearningSourceRecord:
    source_id: str
    trade_id: str
    close_event_id: str
    account_id: str
    symbol: str
    magic: int
    source_config_hash: str
    regime: str
    session: str
    actual_r: Decimal | str | int | float
    broker_opened_at_ms: int
    broker_closed_at_ms: int
    attribution: Sequence[Mapping[str, Any]]
    broker_deals: Sequence[Mapping[str, Any]]
    content_hash: str
    created_at_ms: int


@dataclass(frozen=True, slots=True)
class LearningProposalRecord:
    proposal_id: str
    proposal_hash: str
    proposal_version: int
    parent_proposal_hash: str | None
    policy_config_hash: str
    account_id: str
    symbol: str
    magic: int
    source_config_hash: str
    target_config_hash: str
    as_of_broker_time_ms: int
    proposal: Mapping[str, Any]
    created_at_ms: int


@dataclass(frozen=True, slots=True)
class LearningSnapshotRecord:
    snapshot_id: str
    proposal_id: str
    proposal_hash: str
    source_set_hash: str
    context_stats: Sequence[Mapping[str, Any]]
    agent_summaries: Sequence[Mapping[str, Any]]
    bounds: Mapping[str, Any]
    content_hash: str
    created_at_ms: int


@dataclass(frozen=True, slots=True)
class LearningApprovalRecord:
    approval_id: str
    proposal_id: str
    proposal_hash: str
    approved_by: str
    approved_at_ms: int
    rationale: str
    content_hash: str


@dataclass(frozen=True, slots=True)
class LearningActivationRecord:
    activation_id: str
    approval_id: str
    proposal_id: str
    proposal_hash: str
    account_id: str
    symbol: str
    magic: int
    target_config_hash: str
    regime: str
    session: str
    weights: Sequence[Mapping[str, Any]]
    activated_by: str
    activated_at_ms: int
    content_hash: str


_SCHEMA_V1: tuple[str, ...] = (
    """
    CREATE TABLE schema_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE trades (
        trade_id TEXT PRIMARY KEY,
        request_id TEXT NOT NULL UNIQUE,
        account_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        magic_number INTEGER NOT NULL,
        direction TEXT NOT NULL CHECK(direction IN ('BUY','SELL')),
        status TEXT NOT NULL,
        decision_at REAL NOT NULL,
        regime_at_entry TEXT,
        session_at_entry TEXT,
        intended_entry REAL NOT NULL,
        intended_sl REAL NOT NULL,
        tp1 REAL NOT NULL,
        tp2 REAL NOT NULL,
        tp3 REAL NOT NULL,
        requested_volume REAL NOT NULL CHECK(requested_volume > 0),
        filled_volume REAL,
        initial_risk_currency REAL NOT NULL CHECK(initial_risk_currency >= 0),
        currency TEXT NOT NULL,
        broker_order_id TEXT,
        broker_position_id TEXT,
        filled_entry REAL,
        closed_exit REAL,
        submitted_at REAL,
        opened_at REAL,
        closed_at REAL,
        net_realized_pnl REAL,
        actual_r REAL,
        metadata_json TEXT NOT NULL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        UNIQUE(account_id, broker_order_id),
        UNIQUE(account_id, broker_position_id)
    )
    """,
    "CREATE INDEX trades_status_idx ON trades(status, updated_at)",
    """
    CREATE TABLE trade_attributions (
        attribution_id TEXT PRIMARY KEY,
        trade_id TEXT NOT NULL REFERENCES trades(trade_id) ON DELETE RESTRICT,
        agent_id INTEGER NOT NULL CHECK(agent_id BETWEEN 4 AND 13),
        strategy_slug TEXT NOT NULL,
        direction TEXT NOT NULL CHECK(direction IN ('BUY','SELL')),
        conviction REAL NOT NULL,
        learned_weight REAL NOT NULL,
        effective_weight REAL NOT NULL,
        signal_at REAL NOT NULL,
        tags_json TEXT NOT NULL,
        metadata_json TEXT NOT NULL,
        UNIQUE(trade_id, agent_id)
    )
    """,
    """
    CREATE TABLE trade_events (
        event_id TEXT PRIMARY KEY,
        trade_id TEXT REFERENCES trades(trade_id) ON DELETE RESTRICT,
        event_type TEXT NOT NULL,
        source TEXT NOT NULL,
        correlation_id TEXT,
        occurred_at REAL NOT NULL,
        broker_timestamp REAL,
        payload_json TEXT NOT NULL
    )
    """,
    "CREATE INDEX trade_events_trade_idx ON trade_events(trade_id, occurred_at, event_id)",
    """
    CREATE TABLE broker_deals (
        account_id TEXT NOT NULL,
        deal_id TEXT NOT NULL,
        trade_id TEXT REFERENCES trades(trade_id) ON DELETE RESTRICT,
        position_id TEXT NOT NULL,
        order_id TEXT,
        deal_type TEXT NOT NULL,
        volume REAL NOT NULL CHECK(volume > 0),
        price REAL NOT NULL CHECK(price > 0),
        profit REAL NOT NULL,
        commission REAL NOT NULL,
        swap REAL NOT NULL,
        fee REAL NOT NULL,
        occurred_at REAL NOT NULL,
        payload_json TEXT NOT NULL,
        PRIMARY KEY(account_id, deal_id)
    ) WITHOUT ROWID
    """,
    "CREATE INDEX broker_deals_trade_idx ON broker_deals(trade_id, occurred_at)",
    "CREATE INDEX broker_deals_position_idx ON broker_deals(account_id, position_id, occurred_at)",
    """
    CREATE TABLE risk_decisions (
        decision_id TEXT PRIMARY KEY,
        request_id TEXT NOT NULL,
        trade_id TEXT REFERENCES trades(trade_id) ON DELETE RESTRICT,
        approved INTEGER NOT NULL CHECK(approved IN (0,1)),
        reason_code TEXT NOT NULL,
        decided_at REAL NOT NULL,
        equity REAL NOT NULL,
        balance REAL NOT NULL,
        risk_pct REAL NOT NULL,
        risk_currency REAL NOT NULL,
        requested_volume REAL NOT NULL,
        approved_volume REAL NOT NULL,
        payload_json TEXT NOT NULL
    )
    """,
    "CREATE INDEX risk_decisions_request_idx ON risk_decisions(request_id, decided_at)",
    """
    CREATE TABLE provider_health (
        observation_id TEXT PRIMARY KEY,
        provider_name TEXT NOT NULL,
        state TEXT NOT NULL,
        observed_at REAL NOT NULL,
        source_timestamp REAL,
        detail_json TEXT NOT NULL
    )
    """,
    "CREATE INDEX provider_health_latest_idx ON provider_health(provider_name, observed_at DESC)",
    """
    CREATE TABLE outbox (
        message_id TEXT PRIMARY KEY,
        idempotency_key TEXT NOT NULL,
        event_id TEXT REFERENCES trade_events(event_id) ON DELETE RESTRICT,
        destination TEXT NOT NULL,
        topic TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        status TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        available_at REAL NOT NULL,
        locked_by TEXT,
        locked_until REAL,
        last_error TEXT,
        created_at REAL NOT NULL,
        delivered_at REAL,
        updated_at REAL NOT NULL,
        UNIQUE(destination, idempotency_key)
    )
    """,
    "CREATE INDEX outbox_dispatch_idx ON outbox(status, available_at, locked_until)",
    """
    CREATE TABLE reconciliation_runs (
        run_id TEXT PRIMARY KEY,
        connection_epoch TEXT NOT NULL,
        account_id TEXT NOT NULL,
        status TEXT NOT NULL,
        started_at REAL NOT NULL,
        finished_at REAL,
        broker_position_count INTEGER NOT NULL,
        ledger_position_count INTEGER NOT NULL,
        mismatch_count INTEGER,
        snapshot_hash TEXT NOT NULL,
        detail_json TEXT NOT NULL
    )
    """,
    "CREATE INDEX reconciliation_epoch_idx ON reconciliation_runs(account_id, connection_epoch)",
    """
    CREATE TABLE reconciliation_items (
        run_id TEXT NOT NULL REFERENCES reconciliation_runs(run_id) ON DELETE RESTRICT,
        item_id TEXT NOT NULL,
        discrepancy_type TEXT NOT NULL,
        broker_position_id TEXT,
        trade_id TEXT REFERENCES trades(trade_id) ON DELETE RESTRICT,
        action TEXT NOT NULL,
        resolved INTEGER NOT NULL CHECK(resolved IN (0,1)),
        detail_json TEXT NOT NULL,
        PRIMARY KEY(run_id, item_id)
    ) WITHOUT ROWID
    """,
)


_SCHEMA_V2: tuple[str, ...] = (
    "ALTER TABLE trades ADD COLUMN remaining_volume REAL",
    """
    CREATE TABLE daily_risk_state (
        account_id TEXT NOT NULL,
        risk_day TEXT NOT NULL,
        day_start_equity TEXT NOT NULL,
        current_equity TEXT NOT NULL,
        max_equity TEXT NOT NULL,
        baseline_verified INTEGER NOT NULL CHECK(baseline_verified IN (0,1)),
        baseline_source TEXT NOT NULL,
        halted INTEGER NOT NULL CHECK(halted IN (0,1)),
        halt_reason TEXT,
        realised_pnl TEXT NOT NULL,
        consecutive_losses INTEGER NOT NULL CHECK(consecutive_losses >= 0),
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY(account_id, risk_day)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE risk_reservations (
        reservation_id TEXT PRIMARY KEY,
        trade_id TEXT NOT NULL UNIQUE REFERENCES trades(trade_id) ON DELETE RESTRICT,
        account_id TEXT NOT NULL,
        risk_day TEXT NOT NULL,
        direction TEXT NOT NULL CHECK(direction IN ('BUY','SELL')),
        risk_amount TEXT NOT NULL,
        volume TEXT NOT NULL,
        state TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL,
        expires_at_ms INTEGER NOT NULL,
        submitted_at_ms INTEGER,
        consumed_at_ms INTEGER,
        released_at_ms INTEGER,
        release_reason TEXT,
        updated_at_ms INTEGER NOT NULL,
        FOREIGN KEY(account_id, risk_day)
            REFERENCES daily_risk_state(account_id, risk_day) ON DELETE RESTRICT
    )
    """,
    "CREATE INDEX risk_reservations_exposure_idx ON risk_reservations(account_id,risk_day,state)",
    """
    CREATE TABLE broker_commands (
        command_id TEXT PRIMARY KEY,
        trade_id TEXT NOT NULL REFERENCES trades(trade_id) ON DELETE RESTRICT,
        reservation_id TEXT NOT NULL REFERENCES risk_reservations(reservation_id) ON DELETE RESTRICT,
        action TEXT NOT NULL,
        status TEXT NOT NULL,
        expected_effect_json TEXT NOT NULL,
        connection_epoch TEXT NOT NULL,
        sequence INTEGER NOT NULL CHECK(sequence >= 0),
        position_id TEXT,
        expected_position_version INTEGER,
        payload_json TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL,
        expires_at_ms INTEGER NOT NULL,
        sent_at_ms INTEGER,
        acknowledged_at_ms INTEGER,
        last_error TEXT,
        updated_at_ms INTEGER NOT NULL,
        UNIQUE(connection_epoch, sequence)
    )
    """,
    "CREATE INDEX broker_commands_unresolved_idx ON broker_commands(status,expires_at_ms)",
    """
    CREATE TABLE owned_positions (
        account_id TEXT NOT NULL,
        broker_position_id TEXT NOT NULL,
        trade_id TEXT,
        position_ticket TEXT NOT NULL,
        broker_order_id TEXT,
        symbol TEXT NOT NULL,
        magic INTEGER NOT NULL,
        direction TEXT NOT NULL CHECK(direction IN ('BUY','SELL')),
        initial_volume TEXT NOT NULL,
        remaining_volume TEXT NOT NULL,
        entry_price TEXT NOT NULL,
        original_sl TEXT NOT NULL,
        current_sl TEXT NOT NULL,
        tp1 TEXT NOT NULL,
        tp2 TEXT NOT NULL,
        tp3 TEXT NOT NULL,
        initial_risk_amount TEXT NOT NULL,
        realised_pnl TEXT NOT NULL,
        lifecycle_stage TEXT NOT NULL,
        position_version INTEGER NOT NULL CHECK(position_version >= 0),
        opened_at_ms INTEGER NOT NULL,
        pending_action TEXT,
        pending_milestone TEXT,
        pending_command_id TEXT,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY(account_id, broker_position_id),
        UNIQUE(account_id, position_ticket)
    ) WITHOUT ROWID
    """,
    "CREATE INDEX owned_positions_scope_idx ON owned_positions(account_id,symbol,magic)",
    "CREATE INDEX owned_positions_trade_idx ON owned_positions(trade_id)",
    """
    CREATE TABLE pending_orders (
        account_id TEXT NOT NULL,
        broker_order_id TEXT NOT NULL,
        trade_id TEXT,
        command_id TEXT,
        symbol TEXT NOT NULL,
        magic INTEGER NOT NULL,
        order_type TEXT NOT NULL,
        volume TEXT NOT NULL,
        price TEXT NOT NULL,
        sl TEXT,
        tp TEXT,
        state TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        payload_json TEXT NOT NULL,
        PRIMARY KEY(account_id, broker_order_id)
    ) WITHOUT ROWID
    """,
    "CREATE INDEX pending_orders_scope_idx ON pending_orders(account_id,symbol,magic)",
    """
    CREATE TABLE deal_cursors (
        account_id TEXT NOT NULL,
        stream TEXT NOT NULL,
        cursor TEXT NOT NULL,
        connection_epoch TEXT NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY(account_id, stream)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE broker_snapshots (
        snapshot_id TEXT PRIMARY KEY,
        connection_epoch TEXT NOT NULL,
        account_id TEXT NOT NULL,
        broker_server TEXT NOT NULL,
        trade_mode TEXT NOT NULL,
        symbol TEXT NOT NULL,
        magic INTEGER NOT NULL,
        captured_at_ms INTEGER NOT NULL,
        snapshot_hash TEXT NOT NULL,
        payload_json TEXT NOT NULL
    )
    """,
    "CREATE INDEX broker_snapshots_epoch_idx ON broker_snapshots(account_id,connection_epoch,captured_at_ms)",
)


_SCHEMA_V3: tuple[str, ...] = (
    "ALTER TABLE owned_positions ADD COLUMN pending_close_volume TEXT NOT NULL DEFAULT '0'",
    "ALTER TABLE owned_positions ADD COLUMN pending_filled_volume TEXT NOT NULL DEFAULT '0'",
)

_SCHEMA_V4: tuple[str, ...] = (
    "ALTER TABLE daily_risk_state ADD COLUMN current_balance TEXT",
    "UPDATE daily_risk_state SET current_balance=current_equity",
    "ALTER TABLE broker_deals ADD COLUMN command_id TEXT",
    "ALTER TABLE broker_deals ADD COLUMN volume_decimal TEXT",
    "ALTER TABLE broker_deals ADD COLUMN price_decimal TEXT",
    "ALTER TABLE broker_deals ADD COLUMN profit_decimal TEXT",
    "ALTER TABLE broker_deals ADD COLUMN commission_decimal TEXT",
    "ALTER TABLE broker_deals ADD COLUMN swap_decimal TEXT",
    "ALTER TABLE broker_deals ADD COLUMN fee_decimal TEXT",
    "UPDATE broker_deals SET volume_decimal=CAST(volume AS TEXT),price_decimal=CAST(price AS TEXT),profit_decimal=CAST(profit AS TEXT),commission_decimal=CAST(commission AS TEXT),swap_decimal=CAST(swap AS TEXT),fee_decimal=CAST(fee AS TEXT)",
    "CREATE INDEX broker_deals_command_idx ON broker_deals(command_id,position_id,occurred_at)",
    "ALTER TABLE trades ADD COLUMN intended_entry_decimal TEXT",
    "ALTER TABLE trades ADD COLUMN intended_sl_decimal TEXT",
    "ALTER TABLE trades ADD COLUMN tp1_decimal TEXT",
    "ALTER TABLE trades ADD COLUMN tp2_decimal TEXT",
    "ALTER TABLE trades ADD COLUMN tp3_decimal TEXT",
    "ALTER TABLE trades ADD COLUMN requested_volume_decimal TEXT",
    "ALTER TABLE trades ADD COLUMN filled_volume_decimal TEXT",
    "ALTER TABLE trades ADD COLUMN remaining_volume_decimal TEXT",
    "ALTER TABLE trades ADD COLUMN initial_risk_currency_decimal TEXT",
    "ALTER TABLE trades ADD COLUMN filled_entry_decimal TEXT",
    "ALTER TABLE trades ADD COLUMN closed_exit_decimal TEXT",
    "ALTER TABLE trades ADD COLUMN net_realized_pnl_decimal TEXT",
    "ALTER TABLE trades ADD COLUMN actual_r_decimal TEXT",
    "UPDATE trades SET intended_entry_decimal=CAST(intended_entry AS TEXT),intended_sl_decimal=CAST(intended_sl AS TEXT),tp1_decimal=CAST(tp1 AS TEXT),tp2_decimal=CAST(tp2 AS TEXT),tp3_decimal=CAST(tp3 AS TEXT),requested_volume_decimal=CAST(requested_volume AS TEXT),filled_volume_decimal=CASE WHEN filled_volume IS NULL THEN NULL ELSE CAST(filled_volume AS TEXT) END,remaining_volume_decimal=CASE WHEN remaining_volume IS NULL THEN NULL ELSE CAST(remaining_volume AS TEXT) END,initial_risk_currency_decimal=CAST(initial_risk_currency AS TEXT),filled_entry_decimal=CASE WHEN filled_entry IS NULL THEN NULL ELSE CAST(filled_entry AS TEXT) END,closed_exit_decimal=CASE WHEN closed_exit IS NULL THEN NULL ELSE CAST(closed_exit AS TEXT) END,net_realized_pnl_decimal=CASE WHEN net_realized_pnl IS NULL THEN NULL ELSE CAST(net_realized_pnl AS TEXT) END,actual_r_decimal=CASE WHEN actual_r IS NULL THEN NULL ELSE CAST(actual_r AS TEXT) END",
)

_SCHEMA_V5: tuple[str, ...] = (
    "ALTER TABLE trade_attributions ADD COLUMN conviction_decimal TEXT",
    "ALTER TABLE trade_attributions ADD COLUMN learned_weight_decimal TEXT",
    "ALTER TABLE trade_attributions ADD COLUMN effective_weight_decimal TEXT",
    "UPDATE trade_attributions SET conviction_decimal=CAST(conviction AS TEXT),learned_weight_decimal=CAST(learned_weight AS TEXT),effective_weight_decimal=CAST(effective_weight AS TEXT)",
    "ALTER TABLE trades ADD COLUMN config_hash TEXT",
    "ALTER TABLE trades ADD COLUMN evidence_class TEXT NOT NULL DEFAULT 'UNCLASSIFIED'",
    "ALTER TABLE trades ADD COLUMN learning_eligible INTEGER NOT NULL DEFAULT 0 CHECK(learning_eligible IN (0,1))",
    """
    CREATE TABLE learning_sources (
        source_id TEXT PRIMARY KEY,
        trade_id TEXT NOT NULL UNIQUE REFERENCES trades(trade_id) ON DELETE RESTRICT,
        close_event_id TEXT NOT NULL UNIQUE REFERENCES trade_events(event_id) ON DELETE RESTRICT,
        account_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        magic INTEGER NOT NULL,
        source_config_hash TEXT NOT NULL,
        regime TEXT NOT NULL,
        session TEXT NOT NULL,
        actual_r_decimal TEXT NOT NULL,
        broker_opened_at_ms INTEGER NOT NULL,
        broker_closed_at_ms INTEGER NOT NULL,
        attribution_json TEXT NOT NULL,
        broker_deals_json TEXT NOT NULL,
        content_hash TEXT NOT NULL UNIQUE,
        created_at_ms INTEGER NOT NULL
    )
    """,
    "CREATE INDEX learning_sources_scope_idx ON learning_sources(account_id,symbol,magic,source_config_hash,regime,session,broker_closed_at_ms)",
    """
    CREATE TABLE learning_proposals (
        proposal_id TEXT PRIMARY KEY,
        proposal_hash TEXT NOT NULL UNIQUE,
        proposal_version INTEGER NOT NULL CHECK(proposal_version >= 1),
        parent_proposal_hash TEXT REFERENCES learning_proposals(proposal_hash) ON DELETE RESTRICT,
        policy_config_hash TEXT NOT NULL,
        account_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        magic INTEGER NOT NULL,
        source_config_hash TEXT NOT NULL,
        target_config_hash TEXT NOT NULL,
        as_of_broker_time_ms INTEGER NOT NULL,
        proposal_json TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL,
        UNIQUE(account_id,symbol,magic,source_config_hash,target_config_hash,proposal_version)
    )
    """,
    "CREATE INDEX learning_proposals_scope_idx ON learning_proposals(account_id,symbol,magic,target_config_hash,proposal_version DESC)",
    """
    CREATE TABLE learning_proposal_sources (
        proposal_id TEXT NOT NULL REFERENCES learning_proposals(proposal_id) ON DELETE RESTRICT,
        source_id TEXT NOT NULL REFERENCES learning_sources(source_id) ON DELETE RESTRICT,
        trade_id TEXT NOT NULL,
        close_event_id TEXT NOT NULL,
        source_content_hash TEXT NOT NULL,
        PRIMARY KEY(proposal_id,source_id),
        UNIQUE(proposal_id,trade_id),
        UNIQUE(proposal_id,close_event_id)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE learning_snapshots (
        snapshot_id TEXT PRIMARY KEY,
        proposal_id TEXT NOT NULL UNIQUE REFERENCES learning_proposals(proposal_id) ON DELETE RESTRICT,
        proposal_hash TEXT NOT NULL,
        source_set_hash TEXT NOT NULL,
        context_stats_json TEXT NOT NULL,
        agent_summaries_json TEXT NOT NULL,
        bounds_json TEXT NOT NULL,
        content_hash TEXT NOT NULL UNIQUE,
        created_at_ms INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE learning_approvals (
        approval_id TEXT PRIMARY KEY,
        proposal_id TEXT NOT NULL UNIQUE REFERENCES learning_proposals(proposal_id) ON DELETE RESTRICT,
        proposal_hash TEXT NOT NULL,
        approved_by TEXT NOT NULL,
        approved_at_ms INTEGER NOT NULL,
        rationale TEXT NOT NULL,
        content_hash TEXT NOT NULL UNIQUE
    )
    """,
    """
    CREATE TABLE learning_activations (
        activation_id TEXT PRIMARY KEY,
        approval_id TEXT NOT NULL REFERENCES learning_approvals(approval_id) ON DELETE RESTRICT,
        proposal_id TEXT NOT NULL REFERENCES learning_proposals(proposal_id) ON DELETE RESTRICT,
        proposal_hash TEXT NOT NULL,
        account_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        magic INTEGER NOT NULL,
        target_config_hash TEXT NOT NULL,
        regime TEXT NOT NULL,
        session TEXT NOT NULL,
        weights_json TEXT NOT NULL,
        activated_by TEXT NOT NULL,
        activated_at_ms INTEGER NOT NULL,
        content_hash TEXT NOT NULL UNIQUE,
        UNIQUE(proposal_id,regime,session)
    )
    """,
    "CREATE INDEX learning_activations_scope_idx ON learning_activations(account_id,symbol,magic,target_config_hash,regime,session,activated_at_ms DESC)",
    """
    CREATE TABLE learning_active_scopes (
        account_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        magic INTEGER NOT NULL,
        target_config_hash TEXT NOT NULL,
        regime TEXT NOT NULL,
        session TEXT NOT NULL,
        activation_id TEXT NOT NULL REFERENCES learning_activations(activation_id) ON DELETE RESTRICT,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY(account_id,symbol,magic,target_config_hash,regime,session)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE pma_jobs (
        job_id TEXT PRIMARY KEY,
        trade_id TEXT NOT NULL UNIQUE REFERENCES trades(trade_id) ON DELETE RESTRICT,
        close_event_id TEXT NOT NULL UNIQUE REFERENCES trade_events(event_id) ON DELETE RESTRICT,
        account_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        magic INTEGER NOT NULL,
        source_config_hash TEXT,
        regime TEXT,
        session TEXT,
        target_config_hash TEXT,
        status TEXT NOT NULL CHECK(status IN ('PENDING','IN_FLIGHT','COMPLETED','INELIGIBLE','SUPERSEDED','DEAD_LETTER')),
        due_at_ms INTEGER NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
        max_attempts INTEGER NOT NULL CHECK(max_attempts >= 1),
        locked_by TEXT,
        locked_until_ms INTEGER,
        last_error TEXT,
        eligibility_reason TEXT,
        result_proposal_id TEXT REFERENCES learning_proposals(proposal_id) ON DELETE RESTRICT,
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        completed_at_ms INTEGER,
        dead_lettered_at_ms INTEGER
    )
    """,
    "CREATE INDEX pma_jobs_claim_idx ON pma_jobs(status,due_at_ms,locked_until_ms,job_id)",
)

_SCHEMA_V6: tuple[str, ...] = (
    """
    CREATE TABLE quarantined_broker_exposures (
        account_id TEXT NOT NULL,
        broker_position_id TEXT NOT NULL,
        position_ticket TEXT NOT NULL,
        broker_order_id TEXT,
        reported_trade_id TEXT,
        symbol TEXT NOT NULL,
        magic INTEGER NOT NULL,
        direction TEXT NOT NULL CHECK(direction IN ('BUY','SELL')),
        initial_volume_decimal TEXT NOT NULL,
        remaining_volume_decimal TEXT NOT NULL,
        entry_price_decimal TEXT NOT NULL,
        broker_sl_decimal TEXT,
        broker_tp_decimal TEXT,
        position_version INTEGER NOT NULL,
        opened_at_ms INTEGER NOT NULL,
        first_seen_run_id TEXT NOT NULL REFERENCES reconciliation_runs(run_id) ON DELETE RESTRICT,
        last_seen_run_id TEXT NOT NULL REFERENCES reconciliation_runs(run_id) ON DELETE RESTRICT,
        snapshot_id TEXT NOT NULL REFERENCES broker_snapshots(snapshot_id) ON DELETE RESTRICT,
        reason TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        first_seen_at_ms INTEGER NOT NULL,
        last_seen_at_ms INTEGER NOT NULL,
        active INTEGER NOT NULL CHECK(active IN (0,1)),
        PRIMARY KEY(account_id,broker_position_id)
    ) WITHOUT ROWID
    """,
    "CREATE INDEX quarantined_exposure_scope_idx ON quarantined_broker_exposures(account_id,symbol,magic,active,broker_position_id)",
)

_SCHEMA_V7: tuple[str, ...] = (
    """
    CREATE TABLE command_sequence_allocations (
        connection_epoch TEXT NOT NULL,
        allocation_key TEXT NOT NULL,
        sequence INTEGER NOT NULL CHECK(sequence >= 1),
        allocated_at_ms INTEGER NOT NULL,
        PRIMARY KEY(connection_epoch,allocation_key),
        UNIQUE(connection_epoch,sequence)
    ) WITHOUT ROWID
    """,
)

_IMMUTABLE_LEARNING_TABLES = (
    "learning_sources",
    "learning_proposals",
    "learning_proposal_sources",
    "learning_snapshots",
    "learning_approvals",
    "learning_activations",
)

_REQUIRED_TABLES = frozenset(
    {
        "schema_meta",
        "trades",
        "trade_attributions",
        "trade_events",
        "broker_deals",
        "risk_decisions",
        "provider_health",
        "outbox",
        "reconciliation_runs",
        "reconciliation_items",
        "daily_risk_state",
        "risk_reservations",
        "broker_commands",
        "owned_positions",
        "pending_orders",
        "deal_cursors",
        "broker_snapshots",
        "pma_jobs",
        "learning_sources",
        "learning_proposals",
        "learning_proposal_sources",
        "learning_snapshots",
        "learning_approvals",
        "learning_activations",
        "learning_active_scopes",
        "quarantined_broker_exposures",
        "command_sequence_allocations",
    }
)


class TradeLedger:
    """Single-writer authoritative ledger with explicit lifecycle and health checks."""

    def __init__(
        self,
        database_path: str | Path,
        *,
        busy_timeout_ms: int = 5_000,
        clock: Callable[[], float] = time.time,
    ):
        self.database_path = Path(database_path)
        if not 100 <= busy_timeout_ms <= 120_000:
            raise ValueError("busy_timeout_ms must be between 100 and 120000")
        self.busy_timeout_ms = busy_timeout_ms
        self._clock = clock
        self._connection: sqlite3.Connection | None = None
        self._lock = threading.RLock()

    @property
    def is_open(self) -> bool:
        return self._connection is not None

    def open(self) -> "TradeLedger":
        """Open, configure, migrate, and verify the ledger or raise fail closed."""

        with self._lock:
            if self._connection is not None:
                return self
            parent = self.database_path.parent
            if not parent.is_dir():
                raise LedgerUnavailableError(
                    f"ledger directory does not exist; initialize RuntimePaths first: {parent}"
                )
            try:
                connection = sqlite3.connect(
                    str(self.database_path),
                    timeout=self.busy_timeout_ms / 1_000,
                    isolation_level=None,
                    check_same_thread=False,
                )
                connection.row_factory = sqlite3.Row
                self._connection = connection
                connection.execute("PRAGMA foreign_keys=ON")
                connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
                mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                if str(mode).casefold() != "wal":
                    raise LedgerUnavailableError(
                        f"authoritative ledger requires WAL mode, received {mode!r}"
                    )
                connection.execute("PRAGMA synchronous=FULL")
                connection.execute("PRAGMA trusted_schema=OFF")
                self._initialize_schema()
                self._harden_database_permissions()
                self.require_healthy()
                return self
            except LedgerError:
                if self._connection is not None:
                    self._connection.close()
                    self._connection = None
                raise
            except sqlite3.Error as exc:
                if self._connection is not None:
                    self._connection.close()
                    self._connection = None
                raise LedgerUnavailableError(f"cannot open ledger: {exc}") from exc

    def close(self) -> None:
        with self._lock:
            if self._connection is None:
                return
            connection, self._connection = self._connection, None
            failure: sqlite3.Error | None = None
            try:
                connection.execute("PRAGMA wal_checkpoint(PASSIVE)")
            except sqlite3.Error as exc:
                failure = exc
            finally:
                try:
                    connection.close()
                except sqlite3.Error as exc:
                    failure = failure or exc
            if failure is not None:
                raise LedgerUnavailableError(
                    f"cannot close ledger cleanly: {failure}"
                ) from failure

    def __enter__(self) -> "TradeLedger":
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    @property
    def schema_version(self) -> int:
        with self._read_connection() as connection:
            return int(connection.execute("PRAGMA user_version").fetchone()[0])

    def require_healthy(self) -> None:
        """Raise unless the schema and SQLite integrity check are healthy."""

        with self._read_connection() as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version != LEDGER_SCHEMA_VERSION:
                raise SchemaVersionError(
                    f"ledger schema is {version}; runtime requires {LEDGER_SCHEMA_VERSION}"
                )
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            missing = _REQUIRED_TABLES - tables
            if missing:
                raise SchemaVersionError(f"ledger schema is missing tables: {sorted(missing)}")
            check = connection.execute("PRAGMA quick_check").fetchone()[0]
            if check != "ok":
                raise LedgerUnavailableError(f"ledger integrity check failed: {check}")

    def create_trade(self, record: TradeRecord) -> bool:
        """Insert a canonical trade intent; exact retries return ``False``."""

        _validate_trade(record)
        now = _finite(self._clock(), "clock")
        values = _trade_values(record, now)
        semantic = tuple(key for key in values if key not in {"created_at", "updated_at"})
        with self._transaction() as connection:
            return self._insert_exact(
                connection,
                "trades",
                ("trade_id",),
                values,
                compare_columns=semantic,
            )

    def record_attribution(self, record: TradeAttributionRecord) -> bool:
        _validate_attribution(record)
        values = {
            "attribution_id": _text(record.attribution_id, "attribution_id"),
            "trade_id": _text(record.trade_id, "trade_id"),
            "agent_id": record.agent_id,
            "strategy_slug": _text(record.strategy_slug, "strategy_slug"),
            "direction": record.direction,
            "conviction": record.conviction,
            "learned_weight": record.learned_weight,
            "effective_weight": record.effective_weight,
            "signal_at": record.signal_at,
            "tags_json": _json(list(record.tags), "tags"),
            "metadata_json": _json(record.metadata, "metadata"),
        }
        with self._transaction() as connection:
            return self._insert_exact(connection, "trade_attributions", ("attribution_id",), values)

    def append_event(self, record: TradeEventRecord) -> bool:
        values = _event_values(record)
        with self._transaction() as connection:
            return self._insert_exact(connection, "trade_events", ("event_id",), values)

    def record_deal(self, record: BrokerDealRecord) -> bool:
        with self._transaction() as connection:
            return self._insert_broker_deal_tx(connection, record)

    def record_risk_decision(self, record: RiskDecisionRecord) -> bool:
        _validate_risk(record)
        values = {
            "decision_id": _text(record.decision_id, "decision_id"),
            "request_id": _text(record.request_id, "request_id"),
            "trade_id": _optional_text(record.trade_id, "trade_id"),
            "approved": int(record.approved),
            "reason_code": _text(record.reason_code, "reason_code"),
            "decided_at": record.decided_at,
            "equity": record.equity,
            "balance": record.balance,
            "risk_pct": record.risk_pct,
            "risk_currency": record.risk_currency,
            "requested_volume": record.requested_volume,
            "approved_volume": record.approved_volume,
            "payload_json": _json(record.payload, "payload"),
        }
        with self._transaction() as connection:
            return self._insert_exact(connection, "risk_decisions", ("decision_id",), values)

    def record_provider_health(self, record: ProviderHealthRecord) -> bool:
        state = _text(record.state, "state").upper()
        # Schema-v1 used the ambiguous UNAVAILABLE label.  Canonical v2 stores
        # provider lifecycle using STARTING/HEALTHY/STALE/ERROR/DISABLED.
        if state == "UNAVAILABLE":
            state = "ERROR"
        if state not in PROVIDER_STATES:
            raise LedgerInvariantError(f"unknown provider health state: {state}")
        values = {
            "observation_id": _text(record.observation_id, "observation_id"),
            "provider_name": _text(record.provider_name, "provider_name"),
            "state": state,
            "observed_at": _finite(record.observed_at, "observed_at"),
            "source_timestamp": _optional_finite(
                record.source_timestamp, "source_timestamp"
            ),
            "detail_json": _json(record.detail, "detail"),
        }
        with self._transaction() as connection:
            return self._insert_exact(connection, "provider_health", ("observation_id",), values)

    def enqueue_outbox(self, record: OutboxRecord) -> bool:
        created = _finite(record.created_at, "created_at")
        available = created if record.available_at is None else _finite(
            record.available_at, "available_at"
        )
        values = {
            "message_id": _text(record.message_id, "message_id"),
            "idempotency_key": _text(record.idempotency_key, "idempotency_key"),
            "event_id": _optional_text(record.event_id, "event_id"),
            "destination": _text(record.destination, "destination"),
            "topic": _text(record.topic, "topic"),
            "payload_json": _json(record.payload, "payload"),
            "status": "PENDING",
            "attempts": 0,
            "available_at": available,
            "locked_by": None,
            "locked_until": None,
            "last_error": None,
            "created_at": created,
            "delivered_at": None,
            "updated_at": created,
        }
        with self._transaction() as connection:
            return self._insert_exact(connection, "outbox", ("message_id",), values)

    def claim_outbox(
        self,
        worker_id: str,
        *,
        limit: int = 25,
        lease_seconds: float = 30.0,
        now: float | None = None,
    ) -> list[dict[str, Any]]:
        worker = _text(worker_id, "worker_id")
        if not 1 <= limit <= 500:
            raise LedgerInvariantError("outbox claim limit must be between 1 and 500")
        if not 1.0 <= lease_seconds <= 3_600.0:
            raise LedgerInvariantError("outbox lease must be between 1 and 3600 seconds")
        timestamp = _finite(self._clock() if now is None else now, "now")
        with self._transaction() as connection:
            ids = [
                row[0]
                for row in connection.execute(
                    """
                    SELECT message_id FROM outbox
                    WHERE available_at <= ?
                      AND (status='PENDING' OR (status='IN_FLIGHT' AND locked_until < ?))
                    ORDER BY available_at, created_at, message_id
                    LIMIT ?
                    """,
                    (timestamp, timestamp, limit),
                ).fetchall()
            ]
            if not ids:
                return []
            placeholders = ",".join("?" for _ in ids)
            connection.execute(
                f"""
                UPDATE outbox
                   SET status='IN_FLIGHT', attempts=attempts+1,
                       locked_by=?, locked_until=?, updated_at=?
                 WHERE message_id IN ({placeholders})
                """,
                (worker, timestamp + lease_seconds, timestamp, *ids),
            )
            rows = connection.execute(
                f"SELECT * FROM outbox WHERE message_id IN ({placeholders}) ORDER BY created_at, message_id",
                ids,
            ).fetchall()
            return [_decode_row(row) for row in rows]

    def mark_outbox_delivered(
        self, message_id: str, worker_id: str, *, delivered_at: float | None = None
    ) -> bool:
        delivered = _finite(
            self._clock() if delivered_at is None else delivered_at, "delivered_at"
        )
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT status, locked_by, delivered_at FROM outbox WHERE message_id=?",
                (_text(message_id, "message_id"),),
            ).fetchone()
            if row is None:
                raise LedgerInvariantError(f"unknown outbox message {message_id!r}")
            if row["status"] == "DELIVERED":
                return False
            if row["status"] != "IN_FLIGHT" or row["locked_by"] != worker_id:
                raise LedgerConflictError("outbox message is not leased by this worker")
            connection.execute(
                """
                UPDATE outbox SET status='DELIVERED', delivered_at=?, locked_by=NULL,
                    locked_until=NULL, last_error=NULL, updated_at=? WHERE message_id=?
                """,
                (delivered, delivered, message_id),
            )
            return True

    def mark_outbox_failed(
        self,
        message_id: str,
        worker_id: str,
        error: str,
        *,
        retry_at: float | None,
        now: float | None = None,
    ) -> bool:
        timestamp = _finite(self._clock() if now is None else now, "now")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT status, locked_by FROM outbox WHERE message_id=?", (message_id,)
            ).fetchone()
            if row is None:
                raise LedgerInvariantError(f"unknown outbox message {message_id!r}")
            if row["status"] == "DELIVERED":
                raise LedgerConflictError("delivered outbox messages cannot be failed")
            if row["status"] == "FAILED":
                return False
            if row["status"] != "IN_FLIGHT" or row["locked_by"] != worker_id:
                raise LedgerConflictError("outbox message is not leased by this worker")
            status = "FAILED" if retry_at is None else "PENDING"
            available = timestamp if retry_at is None else _finite(retry_at, "retry_at")
            connection.execute(
                """
                UPDATE outbox SET status=?, available_at=?, locked_by=NULL,
                    locked_until=NULL, last_error=?, updated_at=? WHERE message_id=?
                """,
                (status, available, _text(error, "error"), timestamp, message_id),
            )
            return True

    def start_reconciliation(self, record: ReconciliationRunRecord) -> bool:
        if record.broker_position_count < 0 or record.ledger_position_count < 0:
            raise LedgerInvariantError("reconciliation counts cannot be negative")
        values = {
            "run_id": _text(record.run_id, "run_id"),
            "connection_epoch": _text(record.connection_epoch, "connection_epoch"),
            "account_id": _text(record.account_id, "account_id"),
            "status": "STARTED",
            "started_at": _finite(record.started_at, "started_at"),
            "finished_at": None,
            "broker_position_count": record.broker_position_count,
            "ledger_position_count": record.ledger_position_count,
            "mismatch_count": None,
            "snapshot_hash": _text(record.snapshot_hash, "snapshot_hash"),
            "detail_json": _json(record.detail, "detail"),
        }
        with self._transaction() as connection:
            return self._insert_exact(connection, "reconciliation_runs", ("run_id",), values)

    def record_reconciliation_item(self, record: ReconciliationItemRecord) -> bool:
        values = {
            "run_id": _text(record.run_id, "run_id"),
            "item_id": _text(record.item_id, "item_id"),
            "discrepancy_type": _text(record.discrepancy_type, "discrepancy_type"),
            "broker_position_id": _optional_text(
                record.broker_position_id, "broker_position_id"
            ),
            "trade_id": _optional_text(record.trade_id, "trade_id"),
            "action": _text(record.action, "action"),
            "resolved": int(record.resolved),
            "detail_json": _json(record.detail, "detail"),
        }
        with self._transaction() as connection:
            return self._insert_exact(
                connection, "reconciliation_items", ("run_id", "item_id"), values
            )

    def complete_reconciliation(
        self,
        run_id: str,
        *,
        succeeded: bool,
        mismatch_count: int,
        finished_at: float,
        detail: Mapping[str, Any] | None = None,
    ) -> bool:
        if mismatch_count < 0:
            raise LedgerInvariantError("reconciliation mismatch_count cannot be negative")
        status = "SUCCEEDED" if succeeded else "FAILED"
        finished = _finite(finished_at, "finished_at")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT status, finished_at, mismatch_count, detail_json FROM reconciliation_runs WHERE run_id=?",
                (_text(run_id, "run_id"),),
            ).fetchone()
            if row is None:
                raise LedgerInvariantError(f"unknown reconciliation run {run_id!r}")
            detail_json = _json(detail or {}, "detail")
            if row["status"] != "STARTED":
                if (
                    row["status"] == status
                    and row["finished_at"] == finished
                    and row["mismatch_count"] == mismatch_count
                    and row["detail_json"] == detail_json
                ):
                    return False
                raise LedgerConflictError("reconciliation run was already completed differently")
            connection.execute(
                """
                UPDATE reconciliation_runs
                   SET status=?, finished_at=?, mismatch_count=?, detail_json=?
                 WHERE run_id=?
                """,
                (status, finished, mismatch_count, detail_json, run_id),
            )
            return True

    def initialize_daily_risk_state(self, record: DailyRiskStateRecord) -> bool:
        """Durably pin one UTC risk-day baseline; it cannot be silently replaced."""

        values = _daily_risk_values(record)
        with self._transaction() as connection:
            return self._insert_exact(
                connection,
                "daily_risk_state",
                ("account_id", "risk_day"),
                values,
                compare_columns=tuple(
                    key
                    for key in values
                    if key
                    not in {
                        "current_equity",
                        "current_balance",
                        "max_equity",
                        "updated_at_ms",
                    }
                ),
            )

    def update_daily_risk_equity(
        self,
        account_id: str,
        risk_day: str,
        *,
        current_equity: Decimal | str | int | float,
        realised_pnl: Decimal | str | int | float,
        consecutive_losses: int,
        updated_at_ms: int,
        current_balance: Decimal | str | int | float,
    ) -> None:
        equity = _decimal_text(current_equity, "current_equity", nonnegative=True)
        balance = _decimal_text(
            current_balance, "current_balance", nonnegative=True
        )
        realised = _decimal_text(realised_pnl, "realised_pnl")
        losses = _nonnegative_int(consecutive_losses, "consecutive_losses")
        at_ms = _timestamp_ms(updated_at_ms, "updated_at_ms")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM daily_risk_state WHERE account_id=? AND risk_day=?",
                (_text(account_id, "account_id"), _text(risk_day, "risk_day")),
            ).fetchone()
            if row is None:
                raise LedgerInvariantError("daily risk baseline is not initialized")
            max_equity = max(Decimal(row["max_equity"]), Decimal(equity))
            connection.execute(
                """
                UPDATE daily_risk_state SET current_equity=?,
                    current_balance=?,max_equity=?,realised_pnl=?,
                    consecutive_losses=?, updated_at_ms=? WHERE account_id=? AND risk_day=?
                """,
                (
                    equity,
                    balance,
                    str(max_equity),
                    realised,
                    losses,
                    at_ms,
                    account_id,
                    risk_day,
                ),
            )

    def verify_daily_risk_baseline(
        self,
        account_id: str,
        risk_day: str,
        *,
        expected_baseline: Decimal | str | int | float,
        verified_at_ms: int,
        source: str = "BROKER_OPERATOR_APPROVED",
    ) -> bool:
        """Promote one exact durable baseline without replacing its value."""

        account = _text(account_id, "account_id")
        day = _risk_day(risk_day)
        baseline = _decimal_text(
            expected_baseline,
            "expected_baseline",
            positive=True,
        )
        at_ms = _timestamp_ms(verified_at_ms, "verified_at_ms")
        source_text = _text(source, "source")

        with self._transaction() as connection:
            row = connection.execute(
                """
                SELECT day_start_equity,baseline_verified,baseline_source
                FROM daily_risk_state
                WHERE account_id=? AND risk_day=?
                """,
                (account, day),
            ).fetchone()

            if row is None:
                raise LedgerInvariantError(
                    "daily risk baseline is not initialized"
                )

            if Decimal(row["day_start_equity"]) != Decimal(baseline):
                raise LedgerConflictError(
                    "broker-approved baseline conflicts with durable baseline"
                )

            if row["baseline_verified"]:
                return False

            connection.execute(
                """
                UPDATE daily_risk_state
                SET baseline_verified=1,
                    baseline_source=?,
                    updated_at_ms=?
                WHERE account_id=? AND risk_day=?
                """,
                (
                    source_text,
                    at_ms,
                    account,
                    day,
                ),
            )

            return True

    def halt_daily_risk(
        self,
        account_id: str,
        risk_day: str,
        *,
        reason: str,
        halted_at_ms: int,
    ) -> bool:
        """Set a monotonic risk-day halt.  Only a new UTC day creates a clean state."""

        at_ms = _timestamp_ms(halted_at_ms, "halted_at_ms")
        halt_reason = _text(reason, "reason")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT halted,halt_reason FROM daily_risk_state WHERE account_id=? AND risk_day=?",
                (_text(account_id, "account_id"), _text(risk_day, "risk_day")),
            ).fetchone()
            if row is None:
                raise LedgerInvariantError("daily risk baseline is not initialized")
            if row["halted"]:
                if row["halt_reason"] == halt_reason:
                    return False
                raise LedgerConflictError("risk day is already halted for a different reason")
            connection.execute(
                """
                UPDATE daily_risk_state SET halted=1,halt_reason=?,updated_at_ms=?
                WHERE account_id=? AND risk_day=?
                """,
                (halt_reason, at_ms, account_id, risk_day),
            )
            return True

    def get_daily_risk_state(
        self, account_id: str, risk_day: str
    ) -> dict[str, Any] | None:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM daily_risk_state WHERE account_id=? AND risk_day=?",
                (_text(account_id, "account_id"), _text(risk_day, "risk_day")),
            ).fetchone()
            return _decode_row(row) if row else None

    def get_daily_realized_performance(
        self,
        account_id: str,
        risk_day: str,
        *,
        as_of_ms: int,
    ) -> dict[str, Any]:
        """Derive UTC-day realised PnL and the completed-trade loss streak.

        Account snapshots do not carry these values.  They are therefore
        reconstructed from durable broker OUT deals and broker-confirmed
        closed trades after deal-history synchronization, never accepted as
        transport defaults.
        """

        account = _text(account_id, "account_id")
        day_text = _text(risk_day, "risk_day")
        at_ms = _timestamp_ms(as_of_ms, "as_of_ms")
        try:
            parsed_day = date.fromisoformat(day_text)
        except ValueError as exc:
            raise LedgerInvariantError("risk_day must be an ISO UTC date") from exc
        if parsed_day.isoformat() != day_text:
            raise LedgerInvariantError("risk_day must be a canonical ISO UTC date")
        day_start = datetime.combine(parsed_day, datetime_time.min, tzinfo=UTC)
        next_day = day_start + timedelta(days=1)
        start_seconds = day_start.timestamp()
        end_seconds = min(next_day.timestamp(), at_ms / 1_000)
        if end_seconds < start_seconds:
            raise LedgerInvariantError("as_of_ms precedes the requested UTC risk day")

        with self._read_connection() as connection:
            deal_rows = connection.execute(
                """
                SELECT deal_type,profit_decimal,commission_decimal,
                    swap_decimal,fee_decimal
                FROM broker_deals
                WHERE account_id=? AND occurred_at>=? AND occurred_at<=?
                ORDER BY occurred_at,deal_id
                """,
                (account, start_seconds, end_seconds),
            ).fetchall()
            closed_rows = connection.execute(
                """
                SELECT trade_id,closed_at,net_realized_pnl_decimal
                FROM trades
                WHERE account_id=? AND status='CLOSED'
                  AND closed_at>=? AND closed_at<=?
                  AND net_realized_pnl_decimal IS NOT NULL
                ORDER BY closed_at DESC,trade_id DESC
                """,
                (account, start_seconds, end_seconds),
            ).fetchall()

        closing_rows = [
            row for row in deal_rows if _is_closing_deal_type(row["deal_type"])
        ]
        if any(
            row[f"{name}_decimal"] is None
            for row in closing_rows
            for name in ("profit", "commission", "swap", "fee")
        ):
            raise LedgerInvariantError(
                "daily realised performance lacks canonical deal economics"
            )
        realised_pnl = sum(
            (
                Decimal(row["profit_decimal"])
                + Decimal(row["commission_decimal"])
                + Decimal(row["swap_decimal"])
                + Decimal(row["fee_decimal"])
                for row in closing_rows
            ),
            Decimal("0"),
        )
        consecutive_losses = 0
        for row in closed_rows:
            pnl = Decimal(row["net_realized_pnl_decimal"])
            if pnl >= 0:
                break
            consecutive_losses += 1
        return {
            "realised_pnl": str(realised_pnl),
            "consecutive_losses": consecutive_losses,
            "closing_deal_count": len(closing_rows),
            "closed_trade_count": len(closed_rows),
            "as_of_ms": at_ms,
        }

    def get_active_risk_exposure(
        self, account_id: str, risk_day: str
    ) -> dict[str, Any]:
        """Return cross-day reserved/open/unknown account exposure.

        ``risk_day`` pins the current daily-control scope but deliberately does
        not filter portfolio exposure: overnight positions and unresolved
        commands continue consuming total-risk and position capacity.
        """

        account = _text(account_id, "account_id")
        _risk_day(risk_day)
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT trade_id,risk_amount FROM risk_reservations
                WHERE account_id=?
                  AND state IN ('RESERVED','SUBMITTED','UNKNOWN','CONSUMED')
                ORDER BY trade_id
                """,
                (account,),
            ).fetchall()
            total = sum(
                (Decimal(row["risk_amount"]) for row in rows), Decimal("0")
            )
            return {
                "risk_amount": _decimal_text(total, "risk_amount", nonnegative=True),
                "position_count": len({row["trade_id"] for row in rows}),
                "trade_ids": tuple(row["trade_id"] for row in rows),
            }

    def list_active_risk_reservations(
        self, account_id: str, risk_day: str
    ) -> list[dict[str, Any]]:
        """Return cross-day durable exposure inputs for the pure risk policy.

        ``CONSUMED`` reservations are represented by ``owned_positions`` and
        are deliberately excluded here so the policy cannot double count an
        open trade as both a position and a pending reservation. Overnight
        pending/unknown reservations remain capacity-consuming.
        """

        account = _text(account_id, "account_id")
        _risk_day(risk_day)
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM risk_reservations
                WHERE account_id=?
                  AND state IN ('RESERVED','SUBMITTED','UNKNOWN')
                ORDER BY reservation_id
                """,
                (account,),
            ).fetchall()
            return [_decode_row(row) for row in rows]

    def reserve_open_command_atomic(
        self,
        trade: TradeRecord,
        attributions: Sequence[TradeAttributionRecord],
        reservation: RiskReservationRecord,
        command: BrokerCommandRecord,
        *,
        max_total_risk: Decimal | str | int | float,
        max_open_positions: int,
        outbox: Sequence[OutboxRecord] = (),
    ) -> bool:
        """Create trade, attributions, risk reservation and command in one commit.

        This is the only safe pre-send boundary.  The method also evaluates the
        durable risk-day halt and exposure while holding ``BEGIN IMMEDIATE``.
        """

        _validate_trade(trade)
        _validate_reservation(reservation)
        _validate_command(command)
        risk_limit = Decimal(
            _decimal_text(max_total_risk, "max_total_risk", positive=True)
        )
        position_limit = _positive_int(max_open_positions, "max_open_positions")
        if position_limit > 3:
            raise LedgerInvariantError("position limit cannot exceed the hard ceiling of 3")
        if trade.status != "INTENT":
            raise LedgerInvariantError("atomic execution plans must begin as INTENT")
        if reservation.state != "RESERVED" or command.status != "CREATED":
            raise LedgerInvariantError("new reservation/command must be RESERVED/CREATED")
        if command.action != "OPEN":
            raise LedgerInvariantError("reserve_open_command_atomic accepts OPEN only")
        if not (
            trade.trade_id == reservation.trade_id == command.trade_id
            and reservation.reservation_id == command.reservation_id
            and trade.account_id == reservation.account_id
            and trade.direction == reservation.direction
        ):
            raise LedgerInvariantError("trade, reservation and command identities disagree")
        if Decimal(_decimal_text(trade.initial_risk_currency, "initial_risk_currency")) != Decimal(
            _decimal_text(reservation.risk_amount, "risk_amount")
        ):
            raise LedgerInvariantError("trade initial risk must equal the durable reservation")

        now = _finite(self._clock(), "clock")
        trade_values = _trade_values(trade, now)
        reservation_values = _reservation_values(reservation)
        command_values = _command_values(command)
        with self._transaction() as connection:
            day = connection.execute(
                """
                SELECT baseline_verified,halted,current_equity,current_balance
                FROM daily_risk_state WHERE account_id=? AND risk_day=?
                """,
                (reservation.account_id, reservation.risk_day),
            ).fetchone()
            if day is None or not day["baseline_verified"]:
                raise LedgerInvariantError("verified UTC daily equity baseline is required")
            if day["halted"]:
                raise LedgerInvariantError("daily risk state is halted")
            if day["current_balance"] is None:
                raise LedgerInvariantError("broker balance is not initialized")
            risk_base = min(
                Decimal(day["current_equity"]), Decimal(day["current_balance"])
            )
            hard_trade_risk = risk_base * Decimal("0.01")
            hard_total_risk = risk_base * Decimal("0.03")
            if Decimal(reservation_values["risk_amount"]) > hard_trade_risk:
                raise LedgerInvariantError(
                    "risk reservation exceeds the 1% per-trade hard ceiling"
                )
            if risk_limit > hard_total_risk:
                raise LedgerInvariantError(
                    "total risk limit exceeds the 3% portfolio hard ceiling"
                )

            existing_command = connection.execute(
                "SELECT * FROM broker_commands WHERE command_id=?",
                (command.command_id,),
            ).fetchone()
            if existing_command is not None:
                existing_trade = connection.execute(
                    "SELECT * FROM trades WHERE trade_id=?", (trade.trade_id,)
                ).fetchone()
                existing_reservation = connection.execute(
                    "SELECT * FROM risk_reservations WHERE reservation_id=?",
                    (reservation.reservation_id,),
                ).fetchone()
                if existing_trade is None or existing_reservation is None:
                    raise LedgerConflictError("execution plan exists only partially")
                _assert_columns_equal(
                    existing_command,
                    command_values,
                    (
                        "command_id",
                        "trade_id",
                        "reservation_id",
                        "action",
                        "expected_effect_json",
                        "connection_epoch",
                        "sequence",
                        "position_id",
                        "expected_position_version",
                        "payload_json",
                        "created_at_ms",
                        "expires_at_ms",
                    ),
                    "broker command retry",
                )
                _assert_columns_equal(
                    existing_reservation,
                    reservation_values,
                    (
                        "reservation_id",
                        "trade_id",
                        "account_id",
                        "risk_day",
                        "direction",
                        "risk_amount",
                        "volume",
                        "created_at_ms",
                        "expires_at_ms",
                    ),
                    "risk reservation retry",
                )
                _assert_columns_equal(
                    existing_trade,
                    trade_values,
                    (
                        "trade_id",
                        "request_id",
                        "account_id",
                        "symbol",
                        "magic_number",
                        "direction",
                        "decision_at",
                        "regime_at_entry",
                        "session_at_entry",
                        "intended_entry",
                        "intended_sl",
                        "tp1",
                        "tp2",
                        "tp3",
                        "requested_volume",
                        "initial_risk_currency",
                        "intended_entry_decimal",
                        "intended_sl_decimal",
                        "tp1_decimal",
                        "tp2_decimal",
                        "tp3_decimal",
                        "requested_volume_decimal",
                        "initial_risk_currency_decimal",
                        "currency",
                        "metadata_json",
                        "config_hash",
                        "evidence_class",
                        "learning_eligible",
                    ),
                    "trade retry",
                )
                for attribution in attributions:
                    _validate_attribution(attribution)
                    values = _attribution_values(attribution)
                    row = connection.execute(
                        "SELECT * FROM trade_attributions WHERE attribution_id=?",
                        (attribution.attribution_id,),
                    ).fetchone()
                    if row is None:
                        raise LedgerConflictError("execution plan attribution is missing")
                    _assert_columns_equal(
                        row, values, tuple(values), "trade attribution retry"
                    )
                return False

            trade_inserted = self._insert_exact(
                connection,
                "trades",
                ("trade_id",),
                trade_values,
                compare_columns=tuple(
                    key for key in trade_values if key not in {"created_at", "updated_at"}
                ),
            )
            for attribution in attributions:
                _validate_attribution(attribution)
                if attribution.trade_id != trade.trade_id:
                    raise LedgerInvariantError("attribution belongs to another trade")
                self._insert_exact(
                    connection,
                    "trade_attributions",
                    ("attribution_id",),
                    _attribution_values(attribution),
                )
            reservation_inserted = self._insert_exact(
                connection,
                "risk_reservations",
                ("reservation_id",),
                reservation_values,
            )
            command_inserted = self._insert_exact(
                connection, "broker_commands", ("command_id",), command_values
            )
            if trade_inserted != reservation_inserted or command_inserted != reservation_inserted:
                raise LedgerConflictError("execution plan exists only partially")

            exposure_rows = connection.execute(
                """
                SELECT risk_amount,trade_id FROM risk_reservations
                WHERE account_id=?
                  AND state IN ('RESERVED','SUBMITTED','UNKNOWN','CONSUMED')
                """,
                (reservation.account_id,),
            ).fetchall()
            total_risk = sum((Decimal(row["risk_amount"]) for row in exposure_rows), Decimal("0"))
            if total_risk > risk_limit:
                raise LedgerInvariantError(
                    f"risk reservation would exceed limit ({total_risk}>{risk_limit})"
                )
            if len({row["trade_id"] for row in exposure_rows}) > position_limit:
                raise LedgerInvariantError("risk reservation would exceed position limit")

            event = TradeEventRecord(
                event_id=f"{command.command_id}:created",
                trade_id=trade.trade_id,
                event_type="OPEN_COMMAND_RESERVED",
                source="execution_service",
                correlation_id=command.command_id,
                occurred_at=command.created_at_ms / 1_000,
                payload={
                    "command_id": command.command_id,
                    "reservation_id": reservation.reservation_id,
                    "connection_epoch": command.connection_epoch,
                    "sequence": command.sequence,
                    "symbol": trade.symbol,
                    "direction": trade.direction,
                    "entry": trade.intended_entry,
                    "sl": trade.intended_sl,
                    "tp1": trade.tp1,
                    "tp2": trade.tp2,
                    "tp3": trade.tp3,
                    "volume": reservation.volume,
                    "risk_amount": reservation.risk_amount,
                    "regime": trade.regime_at_entry,
                    "session": trade.session_at_entry,
                    "contributing_agents": tuple(
                        sorted(item.agent_id for item in attributions)
                    ),
                    "decision_id": trade.metadata.get("decision_id"),
                    "snapshot_id": trade.metadata.get("snapshot_id"),
                },
            )
            self._insert_exact(
                connection, "trade_events", ("event_id",), _event_values(event)
            )
            self._insert_outbox_many_tx(connection, event.event_id, outbox)
            connection.execute(
                "UPDATE trades SET status='RISK_APPROVED',updated_at=? WHERE trade_id=?",
                (now, trade.trade_id),
            )
            return command_inserted

    def mark_command_sent(
        self,
        command_id: str,
        *,
        sent_at_ms: int,
        outbox: Sequence[OutboxRecord] = (),
    ) -> bool:
        """Commit SENT/SUBMITTED before the broker port is invoked."""

        command_key = _text(command_id, "command_id")
        at_ms = _timestamp_ms(sent_at_ms, "sent_at_ms")
        with self._transaction() as connection:
            row = self._command_for_update(connection, command_key)
            if row["status"] == "SENT":
                # The first durable SENT timestamp is authoritative.  A later
                # exact API retry is a no-op and must not resend or rewrite it.
                return False
            if row["status"] != "CREATED":
                raise LedgerConflictError(f"cannot send command from {row['status']}")
            if at_ms < row["created_at_ms"]:
                raise LedgerInvariantError("command cannot be sent before it was created")
            if at_ms >= row["expires_at_ms"]:
                raise LedgerConflictError("expired command cannot be sent")
            connection.execute(
                """
                UPDATE broker_commands SET status='SENT',sent_at_ms=?,updated_at_ms=?
                WHERE command_id=?
                """,
                (at_ms, at_ms, command_key),
            )
            if row["action"] == "OPEN":
                connection.execute(
                    """
                    UPDATE risk_reservations SET state='SUBMITTED',submitted_at_ms=?,updated_at_ms=?
                    WHERE reservation_id=? AND state='RESERVED'
                    """,
                    (at_ms, at_ms, row["reservation_id"]),
                )
                connection.execute(
                    """
                    UPDATE trades SET status='SUBMITTED',submitted_at=COALESCE(submitted_at,?),
                        updated_at=? WHERE trade_id=?
                    """,
                    (at_ms / 1_000, at_ms / 1_000, row["trade_id"]),
                )
            elif row["action"] in {"CLOSE", "CLOSE_ALL"}:
                connection.execute(
                    "UPDATE trades SET status='CLOSE_PENDING',updated_at=? WHERE trade_id=?",
                    (at_ms / 1_000, row["trade_id"]),
                )
            event = TradeEventRecord(
                event_id=f"{command_key}:sent",
                trade_id=row["trade_id"],
                event_type="BROKER_COMMAND_SENT",
                source="execution_service",
                correlation_id=command_key,
                occurred_at=at_ms / 1_000,
                payload={
                    "command_id": command_key,
                    "connection_epoch": row["connection_epoch"],
                    "sequence": row["sequence"],
                },
            )
            self._insert_exact(
                connection, "trade_events", ("event_id",), _event_values(event)
            )
            self._insert_outbox_many_tx(connection, event.event_id, outbox)
            return True

    def create_lifecycle_command_atomic(
        self,
        command: BrokerCommandRecord,
        *,
        position: OwnedPositionRecord,
        outbox: Sequence[OutboxRecord] = (),
    ) -> bool:
        """Persist a lifecycle action and its crash-recovery state together."""

        _validate_command(command)
        _validate_owned_position(position)
        if command.action == "OPEN":
            raise LedgerInvariantError("OPEN must use reserve_open_command_atomic")
        if command.status != "CREATED":
            raise LedgerInvariantError("new lifecycle command must be CREATED")
        if position.trade_id != command.trade_id:
            raise LedgerInvariantError("lifecycle position/command trade IDs disagree")
        if position.pending_command_id != command.command_id:
            raise LedgerInvariantError("position must pin the lifecycle command ID")
        if command.position_id != position.broker_position_id:
            raise LedgerInvariantError("lifecycle command must pin broker position ID")
        if command.expected_position_version != position.position_version:
            raise LedgerInvariantError("lifecycle command must pin current position version")
        _validate_lifecycle_expected_effect(command)
        expected_action = {
            "MODIFY_SL": "MODIFY_SL",
            "PARTIAL_CLOSE": "PARTIAL_CLOSE",
            "CLOSE": "CLOSE",
        }.get(command.action)
        if expected_action is None or position.pending_action != expected_action:
            raise LedgerInvariantError("position pending action differs from command")
        values = _command_values(command)
        position_values = _owned_position_values(position)
        with self._transaction() as connection:
            trade = self._trade_for_update(connection, command.trade_id)
            if trade["status"] not in {"OPEN", "PARTIALLY_CLOSED"}:
                raise LedgerConflictError(
                    f"cannot create lifecycle command from {trade['status']}"
                )
            reservation = connection.execute(
                "SELECT * FROM risk_reservations WHERE reservation_id=?",
                (command.reservation_id,),
            ).fetchone()
            if (
                reservation is None
                or reservation["trade_id"] != command.trade_id
                or reservation["state"] != "CONSUMED"
            ):
                raise LedgerConflictError(
                    "lifecycle command requires the trade's consumed reservation"
                )
            current = connection.execute(
                """
                SELECT * FROM owned_positions
                WHERE account_id=? AND broker_position_id=?
                """,
                (position.account_id, position.broker_position_id),
            ).fetchone()
            if current is None:
                raise LedgerConflictError("lifecycle position is not registered")
            if current["position_version"] != position.position_version:
                raise LedgerConflictError("lifecycle decision used a stale position version")
            if current["pending_command_id"] is not None:
                existing = connection.execute(
                    "SELECT * FROM broker_commands WHERE command_id=?",
                    (command.command_id,),
                ).fetchone()
                if existing is None:
                    raise LedgerConflictError("position has an orphan pending command")
                _assert_columns_equal(
                    existing,
                    values,
                    (
                        "command_id",
                        "trade_id",
                        "reservation_id",
                        "action",
                        "expected_effect_json",
                        "connection_epoch",
                        "sequence",
                        "position_id",
                        "expected_position_version",
                        "payload_json",
                        "created_at_ms",
                        "expires_at_ms",
                    ),
                    "lifecycle command retry",
                )
                _assert_columns_equal(
                    current,
                    position_values,
                    tuple(name for name in position_values if name != "updated_at_ms"),
                    "lifecycle position retry",
                )
                return False
            effect = command.expected_effect
            if command.action == "MODIFY_SL":
                previous_sl = Decimal(
                    _decimal_text(effect["previous_sl"], "previous_sl", positive=True)
                )
                target_sl = Decimal(
                    _decimal_text(effect["target_sl"], "target_sl", positive=True)
                )
                current_sl = Decimal(current["current_sl"])
                if previous_sl != current_sl:
                    raise LedgerInvariantError(
                        "MODIFY_SL expected pre-state differs from persisted current SL"
                    )
                improves = (
                    target_sl > current_sl
                    if current["direction"] == "BUY"
                    else target_sl < current_sl
                )
                if not improves:
                    raise LedgerInvariantError(
                        "MODIFY_SL target must directionally improve the persisted SL"
                    )
            else:
                previous_remaining = Decimal(
                    _decimal_text(
                        effect["previous_remaining_volume"],
                        "previous_remaining_volume",
                        positive=True,
                    )
                )
                target_remaining = Decimal(
                    _decimal_text(
                        effect["target_remaining_volume"],
                        "target_remaining_volume",
                        nonnegative=True,
                    )
                )
                current_remaining = Decimal(current["remaining_volume"])
                planned_close = Decimal(position_values["pending_close_volume"])
                if previous_remaining != current_remaining:
                    raise LedgerInvariantError(
                        "close expected pre-state differs from persisted remaining volume"
                    )
                if command.action == "PARTIAL_CLOSE":
                    target_close = Decimal(
                        _decimal_text(
                            effect["target_close_volume"],
                            "target_close_volume",
                            positive=True,
                        )
                    )
                    if (
                        target_close != planned_close
                        or target_remaining != current_remaining - planned_close
                    ):
                        raise LedgerInvariantError(
                            "PARTIAL_CLOSE effect differs from the persisted action plan"
                        )
                elif planned_close != current_remaining or target_remaining != 0:
                    raise LedgerInvariantError(
                        "CLOSE must plan the entire persisted remaining volume"
                    )
            inserted = self._insert_exact(
                connection, "broker_commands", ("command_id",), values
            )
            pending_columns = {
                "pending_action",
                "pending_milestone",
                "pending_command_id",
                "pending_close_volume",
                "pending_filled_volume",
                "updated_at_ms",
            }
            _assert_columns_equal(
                current,
                position_values,
                tuple(key for key in position_values if key not in pending_columns),
                "lifecycle position decision",
            )
            connection.execute(
                """
                UPDATE owned_positions SET pending_action=?,pending_milestone=?,
                    pending_command_id=?,pending_close_volume=?,pending_filled_volume=?,
                    updated_at_ms=? WHERE account_id=? AND broker_position_id=?
                """,
                (
                    position_values["pending_action"],
                    position_values["pending_milestone"],
                    position_values["pending_command_id"],
                    position_values["pending_close_volume"],
                    position_values["pending_filled_volume"],
                    position_values["updated_at_ms"],
                    position.account_id,
                    position.broker_position_id,
                ),
            )
            event = TradeEventRecord(
                event_id=f"{command.command_id}:created",
                trade_id=command.trade_id,
                event_type="LIFECYCLE_COMMAND_CREATED",
                source="execution_service",
                correlation_id=command.command_id,
                occurred_at=command.created_at_ms / 1_000,
                payload={
                    "action": command.action,
                    "position_id": command.position_id,
                    "milestone": position.pending_milestone,
                    "expected_effect": command.expected_effect,
                },
            )
            self._insert_exact(
                connection, "trade_events", ("event_id",), _event_values(event)
            )
            self._insert_outbox_many_tx(connection, event.event_id, outbox)
            return inserted

    def advance_virtual_lifecycle_milestone_atomic(
        self,
        *,
        event_id: str,
        account_id: str,
        trade_id: str,
        broker_position_id: str,
        expected_position_version: int,
        from_stage: str,
        to_stage: str,
        milestone: str,
        occurred_at_ms: int,
        reason: str,
        expected_initial_volume: Decimal | str | int | float,
        expected_remaining_volume: Decimal | str | int | float,
        volume_min: Decimal | str | int | float,
        volume_step: Decimal | str | int | float,
        partial_fraction: Decimal | str | int | float,
        requested_close_volume: Decimal | str | int | float,
        normalized_close_volume: Decimal | str | int | float,
        outbox: Sequence[OutboxRecord] = (),
    ) -> bool:
        """Durably advance a policy-only dust milestone without inventing a fill.

        Only TP1/TP2 partial-close milestones may be virtual.  Broker volume, SL,
        realised PnL and position version remain untouched; the event and stage
        update commit together and exact retries are no-ops.
        """

        allowed = {
            ("TP1", "PRE_TP1", "TP1_CONFIRMED"),
            ("TP2", "TP1_CONFIRMED", "TP2_CONFIRMED"),
        }
        transition = (
            _text(milestone, "milestone"),
            _text(from_stage, "from_stage"),
            _text(to_stage, "to_stage"),
        )
        if transition not in allowed:
            raise LedgerInvariantError("unsupported virtual lifecycle transition")
        event_key = _text(event_id, "event_id")
        account = _text(account_id, "account_id")
        trade_key = _text(trade_id, "trade_id")
        position_key = _text(broker_position_id, "broker_position_id")
        version = _nonnegative_int(
            expected_position_version, "expected_position_version"
        )
        at_ms = _timestamp_ms(occurred_at_ms, "occurred_at_ms")
        reason_text = _text(reason, "reason")
        expected_initial = Decimal(
            _decimal_text(expected_initial_volume, "expected_initial_volume", positive=True)
        )
        expected_remaining = Decimal(
            _decimal_text(
                expected_remaining_volume, "expected_remaining_volume", positive=True
            )
        )
        minimum = Decimal(_decimal_text(volume_min, "volume_min", positive=True))
        step = Decimal(_decimal_text(volume_step, "volume_step", positive=True))
        fraction = Decimal(
            _decimal_text(partial_fraction, "partial_fraction", positive=True)
        )
        requested = Decimal(
            _decimal_text(requested_close_volume, "requested_close_volume", positive=True)
        )
        normalized = Decimal(
            _decimal_text(
                normalized_close_volume,
                "normalized_close_volume",
                nonnegative=True,
            )
        )
        calculated_requested = expected_initial * fraction
        calculated_normalized = (
            (calculated_requested / step).to_integral_value(rounding=ROUND_FLOOR) * step
        )
        if requested != calculated_requested or normalized != calculated_normalized:
            raise LedgerInvariantError("virtual milestone lot normalization proof differs")
        if not (
            normalized <= 0
            or normalized < minimum
            or expected_remaining - normalized < minimum
        ):
            raise LedgerInvariantError("virtual milestone is broker-executable, not dust")
        event = TradeEventRecord(
            event_id=event_key,
            trade_id=trade_key,
            event_type="VIRTUAL_MILESTONE_CONFIRMED",
            source="lifecycle_service",
            correlation_id=event_key,
            occurred_at=at_ms / 1_000,
            payload={
                "broker_position_id": position_key,
                "position_version": version,
                "milestone": transition[0],
                "from_stage": transition[1],
                "to_stage": transition[2],
                "reason": reason_text,
                "broker_effect": False,
                "dust_proof": {
                    "initial_volume": expected_initial,
                    "remaining_volume": expected_remaining,
                    "volume_min": minimum,
                    "volume_step": step,
                    "partial_fraction": fraction,
                    "requested_close_volume": requested,
                    "normalized_close_volume": normalized,
                },
            },
        )
        with self._transaction() as connection:
            position = connection.execute(
                """
                SELECT * FROM owned_positions
                WHERE account_id=? AND broker_position_id=?
                """,
                (account, position_key),
            ).fetchone()
            if position is None:
                raise LedgerConflictError("virtual milestone position is not registered")
            if position["trade_id"] != trade_key:
                raise LedgerConflictError("virtual milestone belongs to another trade")
            if position["position_version"] != version:
                raise LedgerConflictError("virtual milestone used a stale position version")
            if position["pending_command_id"] is not None:
                raise LedgerConflictError("virtual milestone cannot bypass an in-flight action")
            if Decimal(position["remaining_volume"]) <= 0:
                raise LedgerConflictError("virtual milestone position is not open")
            if (
                Decimal(position["initial_volume"]) != expected_initial
                or Decimal(position["remaining_volume"]) != expected_remaining
            ):
                raise LedgerConflictError(
                    "virtual milestone volume proof differs from durable position"
                )

            inserted = self._insert_exact(
                connection, "trade_events", ("event_id",), _event_values(event)
            )
            if position["lifecycle_stage"] == transition[2]:
                if inserted:
                    raise LedgerInvariantError(
                        "virtual lifecycle stage existed without its authoritative event"
                    )
                self._insert_outbox_many_tx(connection, event.event_id, outbox)
                return False
            if position["lifecycle_stage"] != transition[1]:
                raise LedgerConflictError(
                    "virtual milestone used an unexpected lifecycle stage"
                )
            if not inserted:
                raise LedgerInvariantError(
                    "virtual lifecycle event existed before its stage transition"
                )
            cursor = connection.execute(
                """
                UPDATE owned_positions SET lifecycle_stage=?,updated_at_ms=?
                WHERE account_id=? AND broker_position_id=?
                  AND trade_id=? AND position_version=? AND lifecycle_stage=?
                  AND pending_command_id IS NULL
                """,
                (
                    transition[2],
                    at_ms,
                    account,
                    position_key,
                    trade_key,
                    version,
                    transition[1],
                ),
            )
            if cursor.rowcount != 1:
                raise LedgerConflictError("virtual milestone position changed concurrently")
            self._insert_outbox_many_tx(connection, event.event_id, outbox)
            return True

    def mark_command_unknown(
        self, command_id: str, *, unknown_at_ms: int, reason: str
    ) -> bool:
        command_key = _text(command_id, "command_id")
        at_ms = _timestamp_ms(unknown_at_ms, "unknown_at_ms")
        error = _text(reason, "reason")
        with self._transaction() as connection:
            return self._mark_command_unknown_tx(connection, command_key, at_ms, error)

    def resolve_created_command_not_sent(
        self, command_id: str, *, resolved_at_ms: int, reason: str
    ) -> bool:
        """Terminally cancel a CREATED envelope proven unable to reach the EA."""

        command_key = _text(command_id, "command_id")
        at_ms = _timestamp_ms(resolved_at_ms, "resolved_at_ms")
        reason_text = _text(reason, "reason")
        with self._transaction() as connection:
            command = self._command_for_update(connection, command_key)
            if command["status"] == "RECONCILED_NOT_APPLIED":
                return False
            if command["status"] != "CREATED" or command["sent_at_ms"] is not None:
                raise LedgerConflictError(
                    "only a never-sent CREATED command can use unsent resolution"
                )
            connection.execute(
                """
                UPDATE broker_commands SET status='RECONCILED_NOT_APPLIED',
                    acknowledged_at_ms=?,last_error=?,updated_at_ms=?
                WHERE command_id=?
                """,
                (at_ms, reason_text, at_ms, command_key),
            )
            connection.execute(
                """
                UPDATE owned_positions SET pending_action=NULL,pending_milestone=NULL,
                    pending_command_id=NULL,pending_close_volume='0',
                    pending_filled_volume='0',updated_at_ms=?
                WHERE pending_command_id=?
                """,
                (at_ms, command_key),
            )
            if command["action"] == "OPEN":
                connection.execute(
                    """
                    UPDATE risk_reservations SET state='RELEASED',released_at_ms=?,
                        release_reason=?,updated_at_ms=?
                    WHERE reservation_id=? AND state='RESERVED'
                    """,
                    (at_ms, reason_text, at_ms, command["reservation_id"]),
                )
                connection.execute(
                    "UPDATE trades SET status='CANCELLED',updated_at=? WHERE trade_id=?",
                    (at_ms / 1_000, command["trade_id"]),
                )
            event = TradeEventRecord(
                event_id=f"{command_key}:created-not-sent",
                trade_id=command["trade_id"],
                event_type="RECONCILED_NOT_APPLIED",
                source="reconciliation",
                correlation_id=command_key,
                occurred_at=at_ms / 1_000,
                payload={"command_id": command_key, "proof": reason_text},
            )
            self._insert_exact(
                connection, "trade_events", ("event_id",), _event_values(event)
            )
            return True

    def mark_command_received(
        self,
        command_id: str,
        *,
        received_at_ms: int,
        evidence: Mapping[str, Any] | None = None,
        outbox: Sequence[OutboxRecord] = (),
    ) -> bool:
        """Persist transport ACK without conflating it with broker effect ACK."""

        command_key = _text(command_id, "command_id")
        at_ms = _timestamp_ms(received_at_ms, "received_at_ms")
        if evidence is not None and not isinstance(evidence, Mapping):
            raise LedgerInvariantError("command receipt evidence must be a mapping")
        stable_evidence = dict(evidence or {})
        with self._transaction() as connection:
            command = self._command_for_update(connection, command_key)
            if command["status"] == "CREATED":
                raise LedgerConflictError(
                    f"cannot mark command RECEIVED from {command['status']}"
                )
            if command["sent_at_ms"] is None or at_ms < command["sent_at_ms"]:
                raise LedgerInvariantError("command receipt precedes durable send")
            event = TradeEventRecord(
                event_id=f"{command_key}:received",
                trade_id=command["trade_id"],
                event_type="BROKER_COMMAND_RECEIVED",
                source="broker",
                correlation_id=command_key,
                occurred_at=at_ms / 1_000,
                payload={
                    "command_id": command_key,
                    "ack_evidence": stable_evidence,
                },
            )
            self._insert_exact(
                connection,
                "trade_events",
                ("event_id",),
                _event_values(event),
                compare_columns=(
                    "event_id",
                    "trade_id",
                    "event_type",
                    "source",
                    "correlation_id",
                    "payload_json",
                ),
            )
            self._insert_outbox_many_tx(connection, event.event_id, outbox)
            if command["status"] in {
                "RECEIVED",
                "APPLIED",
                "REJECTED",
                "RECONCILED_APPLIED",
                "RECONCILED_NOT_APPLIED",
            }:
                # An effect/rejection event is stronger proof of receipt and
                # may legitimately race ahead of the transport ACK.  The ACK
                # evidence is still retained for terminal-partial diagnosis.
                return False
            if command["status"] not in {"SENT", "UNKNOWN"}:
                raise LedgerConflictError(
                    f"cannot mark command RECEIVED from {command['status']}"
                )
            connection.execute(
                """
                UPDATE broker_commands SET status='RECEIVED',acknowledged_at_ms=?,
                    last_error=NULL,updated_at_ms=? WHERE command_id=?
                """,
                (at_ms, at_ms, command_key),
            )
            return True

    def expire_sent_commands(self, *, now_ms: int) -> tuple[str, ...]:
        """Move timed-out SENT/RECEIVED commands to UNKNOWN without releasing risk."""

        at_ms = _timestamp_ms(now_ms, "now_ms")
        with self._transaction() as connection:
            command_ids = tuple(
                row[0]
                for row in connection.execute(
                    """
                    SELECT command_id FROM broker_commands
                    WHERE status IN ('SENT','RECEIVED') AND expires_at_ms<=?
                    ORDER BY expires_at_ms,command_id
                    """,
                    (at_ms,),
                ).fetchall()
            )
            for command_id in command_ids:
                self._mark_command_unknown_tx(
                    connection, command_id, at_ms, "broker acknowledgement timeout"
                )
            return command_ids

    def acknowledge_open_command_atomic(
        self,
        command_id: str,
        *,
        event: TradeEventRecord,
        position: OwnedPositionRecord,
        deal: BrokerDealRecord | None = None,
        outbox: Sequence[OutboxRecord] = (),
    ) -> bool:
        """Apply one OPEN fill without confusing progress with completion.

        Production callers supply the exact IN deal.  The transaction then
        derives cumulative volume, VWAP and stop risk from every immutable
        deal attributed to this command.  A partial fill registers the live,
        broker-protected position but leaves the command ``RECEIVED`` and the
        full reservation ``SUBMITTED``.  Only exact requested-volume evidence
        consumes the reservation and marks the command ``APPLIED``.

        ``deal=None`` remains available for deterministic internal fixtures,
        but it is accepted only for an exact full-volume position; it cannot
        manufacture terminal status from a partial snapshot.
        """

        _validate_owned_position(position)
        if position.trade_id is None:
            raise LedgerInvariantError("an acknowledged owned position requires trade_id")
        command_key = _text(command_id, "command_id")
        if event.correlation_id != command_key:
            raise LedgerInvariantError("broker event correlation must equal command_id")
        if event.trade_id != position.trade_id:
            raise LedgerInvariantError("event and position trade IDs disagree")
        event_values = _event_values(event)
        position_values = _owned_position_values(position)
        with self._transaction() as connection:
            deal_inserted: bool | None = None
            if deal is not None:
                _validate_atomic_deal_binding(
                    deal,
                    event,
                    account_id=position.account_id,
                    position_id=position.broker_position_id,
                    command_id=command_key,
                    allowed_deal_types={"IN"},
                )
                deal_inserted = self._insert_broker_deal_tx(connection, deal)
            command = self._command_for_update(connection, command_key)
            if command["action"] != "OPEN":
                raise LedgerConflictError("open acknowledgement targets a non-OPEN command")
            if command["trade_id"] != position.trade_id:
                raise LedgerConflictError("command and position trade IDs disagree")
            if command["status"] not in {"SENT", "RECEIVED", "UNKNOWN", "APPLIED"}:
                raise LedgerConflictError(
                    f"cannot acknowledge command from {command['status']}"
                )
            if (
                command["sent_at_ms"] is None
                or position.updated_at_ms < command["sent_at_ms"]
            ):
                raise LedgerInvariantError("open acknowledgement precedes durable send")
            inserted = self._insert_exact(
                connection, "trade_events", ("event_id",), event_values
            )
            if (
                deal_inserted is not None
                and event.source == "broker"
                and deal_inserted != inserted
            ):
                raise LedgerConflictError(
                    "broker OPEN event and deal idempotency identities diverged"
                )
            trade = self._trade_for_update(connection, position.trade_id)
            if trade["account_id"] != position.account_id:
                raise LedgerConflictError("ACK account does not match canonical trade")
            if trade["symbol"] != position.symbol or trade["magic_number"] != position.magic:
                raise LedgerConflictError("ACK symbol/magic does not match canonical trade")
            if trade["direction"] != position.direction:
                raise LedgerConflictError("ACK direction does not match canonical trade")
            reservation = connection.execute(
                "SELECT * FROM risk_reservations WHERE reservation_id=?",
                (command["reservation_id"],),
            ).fetchone()
            if reservation is None:
                raise LedgerInvariantError("open command has no durable reservation")
            if (
                reservation["trade_id"] != position.trade_id
                or reservation["account_id"] != position.account_id
                or reservation["direction"] != position.direction
            ):
                raise LedgerConflictError(
                    "open fill reservation identity differs from its position"
                )
            reserved_volume = Decimal(reservation["volume"])
            reserved_risk = Decimal(reservation["risk_amount"])

            if deal is None:
                actual_volume = Decimal(position_values["initial_volume"])
                actual_entry = Decimal(position_values["entry_price"])
                actual_risk = Decimal(position_values["initial_risk_amount"])
                opening_ms = position.opened_at_ms
                order_id = position_values["broker_order_id"]
                if actual_volume != reserved_volume:
                    raise LedgerInvariantError(
                        "deal-free open acknowledgement requires exact reserved volume"
                    )
            else:
                deal_rows = connection.execute(
                    """
                    SELECT * FROM broker_deals WHERE command_id=?
                    ORDER BY occurred_at,deal_id
                    """,
                    (command_key,),
                ).fetchall()
                if not deal_rows:
                    raise LedgerInvariantError("OPEN command has no durable IN deal")
                order_ids: set[str] = set()
                actual_volume = Decimal("0")
                weighted_notional = Decimal("0")
                actual_risk = Decimal("0")
                opening_ms: int | None = None
                intended_entry = Decimal(trade["intended_entry_decimal"])
                intended_sl = Decimal(trade["intended_sl_decimal"])
                tp1 = Decimal(trade["tp1_decimal"])
                tp2 = Decimal(trade["tp2_decimal"])
                tp3 = Decimal(trade["tp3_decimal"])
                planned_distance = abs(intended_entry - intended_sl)
                if planned_distance <= 0:
                    raise LedgerInvariantError("OPEN plan has zero risk distance")
                for row in deal_rows:
                    if (
                        row["trade_id"] != position.trade_id
                        or row["account_id"] != position.account_id
                        or row["position_id"] != position.broker_position_id
                        or str(row["deal_type"]).upper() != "IN"
                        or not isinstance(row["order_id"], str)
                        or not row["order_id"]
                    ):
                        raise LedgerConflictError(
                            "OPEN command deals do not share one stable identity"
                        )
                    volume = Decimal(row["volume_decimal"])
                    price = Decimal(row["price_decimal"])
                    geometry = (
                        intended_sl < price < tp1 < tp2 < tp3
                        if position.direction == "BUY"
                        else intended_sl > price > tp1 > tp2 > tp3
                    )
                    if not geometry:
                        raise LedgerConflictError(
                            "OPEN deal violates canonical directional protection"
                        )
                    actual_distance = abs(price - intended_sl)
                    actual_volume += volume
                    weighted_notional += volume * price
                    actual_risk += (
                        reserved_risk
                        * volume
                        / reserved_volume
                        * actual_distance
                        / planned_distance
                    )
                    order_ids.add(row["order_id"])
                    deal_ms = int(round(float(row["occurred_at"]) * 1_000))
                    if deal_ms < int(command["sent_at_ms"]):
                        raise LedgerInvariantError(
                            "OPEN deal precedes the durable command send"
                        )
                    opening_ms = (
                        deal_ms if opening_ms is None else min(opening_ms, deal_ms)
                    )
                if len(order_ids) != 1 or opening_ms is None:
                    raise LedgerConflictError(
                        "progressive OPEN fills changed broker order identity"
                    )
                order_id = next(iter(order_ids))
                actual_entry = weighted_notional / actual_volume
                observed_remaining = Decimal(position_values["remaining_volume"])
                if observed_remaining != actual_volume:
                    raise LedgerConflictError(
                        "OPEN position volume differs from cumulative IN deals"
                    )

            if actual_volume <= 0 or actual_volume > reserved_volume:
                raise LedgerConflictError("broker fill exceeds reserved volume")
            if actual_risk <= 0 or actual_risk > reserved_risk:
                raise LedgerConflictError("broker fill risk is outside reserved risk")
            if order_id is None:
                raise LedgerInvariantError("OPEN fill lacks broker order identity")
            terminal = actual_volume == reserved_volume

            aggregate_values = dict(position_values)
            aggregate_values.update(
                {
                    "broker_order_id": order_id,
                    "initial_volume": _decimal_text(
                        actual_volume, "actual_volume", positive=True
                    ),
                    "remaining_volume": _decimal_text(
                        actual_volume, "actual_volume", positive=True
                    ),
                    "entry_price": _decimal_text(
                        actual_entry, "actual_entry", positive=True
                    ),
                    "initial_risk_amount": _decimal_text(
                        actual_risk, "actual_risk", positive=True
                    ),
                    "opened_at_ms": opening_ms,
                }
            )
            existing_position = connection.execute(
                """
                SELECT * FROM owned_positions
                WHERE account_id=? AND broker_position_id=?
                """,
                (position.account_id, position.broker_position_id),
            ).fetchone()
            if existing_position is None:
                columns = tuple(aggregate_values)
                connection.execute(
                    f"""
                    INSERT INTO owned_positions ({','.join(columns)})
                    VALUES ({','.join('?' for _ in columns)})
                    """,
                    tuple(aggregate_values[name] for name in columns),
                )
            else:
                stable = (
                    "account_id",
                    "broker_position_id",
                    "trade_id",
                    "position_ticket",
                    "broker_order_id",
                    "symbol",
                    "magic",
                    "direction",
                    "original_sl",
                    "current_sl",
                    "tp1",
                    "tp2",
                    "tp3",
                    "opened_at_ms",
                    "lifecycle_stage",
                )
                changed = [
                    name
                    for name in stable
                    if existing_position[name] != aggregate_values[name]
                ]
                if changed:
                    raise LedgerConflictError(
                        f"progressive OPEN fill changed stable position fields: {changed}"
                    )
                if inserted and (
                    aggregate_values["position_version"]
                    <= existing_position["position_version"]
                ):
                    raise LedgerConflictError(
                        "new OPEN fill did not advance broker position version"
                    )
                if not inserted:
                    differing = [
                        name
                        for name, value in aggregate_values.items()
                        if existing_position[name] != value
                    ]
                    if differing:
                        raise LedgerConflictError(
                            f"duplicate OPEN event changed position content: {differing}"
                        )
                else:
                    mutable = tuple(
                        name
                        for name in aggregate_values
                        if name not in {"account_id", "broker_position_id"}
                    )
                    connection.execute(
                        f"""
                        UPDATE owned_positions
                        SET {','.join(f'{name}=?' for name in mutable)}
                        WHERE account_id=? AND broker_position_id=?
                        """,
                        (
                            *(aggregate_values[name] for name in mutable),
                            position.account_id,
                            position.broker_position_id,
                        ),
                    )

            if command["status"] == "APPLIED":
                if inserted or not terminal:
                    raise LedgerConflictError(
                        "command was already applied under a different broker event"
                    )
            else:
                connection.execute(
                    """
                    UPDATE broker_commands SET status=?,acknowledged_at_ms=COALESCE(acknowledged_at_ms,?),
                        position_id=?,last_error=NULL,updated_at_ms=? WHERE command_id=?
                    """,
                    (
                        "APPLIED" if terminal else "RECEIVED",
                        position.updated_at_ms,
                        position.broker_position_id,
                        position.updated_at_ms,
                        command_key,
                    ),
                )
                connection.execute(
                    """
                    UPDATE risk_reservations SET state=?,
                        consumed_at_ms=CASE WHEN ? THEN COALESCE(consumed_at_ms,?) ELSE consumed_at_ms END,
                        updated_at_ms=? WHERE reservation_id=?
                          AND state IN ('SUBMITTED','UNKNOWN')
                    """,
                    (
                        "CONSUMED" if terminal else "SUBMITTED",
                        int(terminal),
                        position.updated_at_ms,
                        position.updated_at_ms,
                        command["reservation_id"],
                    ),
                )
            try:
                connection.execute(
                    """
                    UPDATE trades SET status='OPEN',broker_order_id=?,
                        broker_position_id=?,filled_entry=?,filled_volume=?,remaining_volume=?,
                        initial_risk_currency=?,filled_entry_decimal=?,
                        filled_volume_decimal=?,remaining_volume_decimal=?,
                        initial_risk_currency_decimal=?,opened_at=?,updated_at=?
                    WHERE trade_id=?
                    """,
                    (
                        order_id,
                        position.broker_position_id,
                        float(actual_entry),
                        float(actual_volume),
                        float(actual_volume),
                        float(actual_risk),
                        aggregate_values["entry_price"],
                        aggregate_values["initial_volume"],
                        aggregate_values["remaining_volume"],
                        aggregate_values["initial_risk_amount"],
                        opening_ms / 1_000,
                        position.updated_at_ms / 1_000,
                        position.trade_id,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise LedgerConflictError(
                    "broker position/order identity belongs to another trade"
                ) from exc
            self._insert_outbox_many_tx(connection, event.event_id, outbox)
            return inserted

    def reject_command_atomic(
        self,
        command_id: str,
        *,
        event: TradeEventRecord,
        rejected_at_ms: int,
        reason: str,
        outbox: Sequence[OutboxRecord] = (),
    ) -> bool:
        """Apply an explicit broker rejection; only this may release unfilled risk."""

        command_key = _text(command_id, "command_id")
        at_ms = _timestamp_ms(rejected_at_ms, "rejected_at_ms")
        error = _text(reason, "reason")
        if event.correlation_id != command_key:
            raise LedgerInvariantError("broker event correlation must equal command_id")
        with self._transaction() as connection:
            command = self._command_for_update(connection, command_key)
            if event.trade_id != command["trade_id"]:
                raise LedgerInvariantError("rejection event belongs to another trade")
            if command["status"] in {"APPLIED", "RECONCILED_APPLIED"}:
                raise LedgerConflictError("an applied command cannot later be rejected")
            if command["status"] == "CREATED":
                raise LedgerConflictError("broker cannot reject a command never marked SENT")
            if command["status"] == "RECONCILED_NOT_APPLIED":
                raise LedgerConflictError("a reconciled command cannot later be rejected")
            if command["sent_at_ms"] is None or at_ms < command["sent_at_ms"]:
                raise LedgerInvariantError("command rejection precedes durable send")
            inserted = self._insert_exact(
                connection, "trade_events", ("event_id",), _event_values(event)
            )
            if command["status"] == "REJECTED":
                if inserted:
                    raise LedgerConflictError(
                        "command was already rejected under a different broker event"
                    )
            else:
                connection.execute(
                    """
                    UPDATE broker_commands SET status='REJECTED',acknowledged_at_ms=?,
                        last_error=?,updated_at_ms=? WHERE command_id=?
                    """,
                    (at_ms, error, at_ms, command_key),
                )
                connection.execute(
                    """
                    UPDATE risk_reservations SET state='RELEASED',released_at_ms=?,
                        release_reason=?,updated_at_ms=? WHERE reservation_id=?
                          AND state IN ('SUBMITTED','UNKNOWN')
                    """,
                    (at_ms, error, at_ms, command["reservation_id"]),
                )
                if command["action"] == "OPEN":
                    connection.execute(
                        "UPDATE trades SET status='REJECTED',updated_at=? WHERE trade_id=?",
                        (at_ms / 1_000, command["trade_id"]),
                    )
                else:
                    position = connection.execute(
                        """
                        SELECT remaining_volume FROM owned_positions
                        WHERE pending_command_id=?
                        """,
                        (command_key,),
                    ).fetchone()
                    connection.execute(
                        """
                        UPDATE owned_positions SET pending_action=NULL,
                            pending_milestone=NULL,pending_command_id=NULL,
                            pending_close_volume='0',pending_filled_volume='0',
                            updated_at_ms=? WHERE pending_command_id=?
                        """,
                        (at_ms, command_key),
                    )
                    if command["action"] in {"CLOSE", "CLOSE_ALL"} and position:
                        trade = self._trade_for_update(connection, command["trade_id"])
                        status_after_reject = (
                            "OPEN"
                            if trade["filled_volume_decimal"] is not None
                            and Decimal(trade["filled_volume_decimal"])
                            == Decimal(position["remaining_volume"])
                            else "PARTIALLY_CLOSED"
                        )
                        connection.execute(
                            "UPDATE trades SET status=?,updated_at=? WHERE trade_id=?",
                            (status_after_reject, at_ms / 1_000, command["trade_id"]),
                        )
                connection.execute(
                    "DELETE FROM pending_orders WHERE command_id=?", (command_key,)
                )
            self._insert_outbox_many_tx(connection, event.event_id, outbox)
            return inserted

    def apply_position_update_atomic(
        self,
        *,
        event: TradeEventRecord,
        position: OwnedPositionRecord,
        command_id: str | None = None,
        deal: BrokerDealRecord | None = None,
        outbox: Sequence[OutboxRecord] = (),
    ) -> bool:
        """Apply an authoritative partial-close/SL/lifecycle position snapshot."""

        _validate_owned_position(position)
        if position.trade_id is None:
            raise LedgerInvariantError("position updates require a canonical trade ID")
        with self._transaction() as connection:
            if deal is not None:
                _validate_atomic_deal_binding(
                    deal,
                    event,
                    account_id=position.account_id,
                    position_id=position.broker_position_id,
                    command_id=command_id,
                    allowed_deal_types={"OUT", "OUT_BY"},
                )
                self._insert_broker_deal_tx(connection, deal)
            inserted = self._insert_exact(
                connection, "trade_events", ("event_id",), _event_values(event)
            )
            existing_position = connection.execute(
                """
                SELECT * FROM owned_positions
                WHERE account_id=? AND broker_position_id=?
                """,
                (position.account_id, position.broker_position_id),
            ).fetchone()
            if existing_position is None:
                raise LedgerConflictError("position update has no registered open position")
            old_remaining = Decimal(existing_position["remaining_volume"])
            new_remaining = Decimal(str(position.remaining_volume))
            if new_remaining > old_remaining:
                raise LedgerConflictError("position remaining volume cannot increase")
            command: sqlite3.Row | None = None
            effect_disposition = "PROGRESS"
            if command_id is not None:
                if event.correlation_id != command_id:
                    raise LedgerInvariantError(
                        "lifecycle event correlation must equal command_id"
                    )
                command = self._command_for_update(connection, command_id)
                if command["trade_id"] != position.trade_id:
                    raise LedgerConflictError(
                        "lifecycle command belongs to another trade"
                    )
                if command["status"] not in {
                    "SENT",
                    "UNKNOWN",
                    "RECEIVED",
                    "APPLIED",
                }:
                    raise LedgerConflictError(
                        f"cannot apply lifecycle command from {command['status']}"
                    )
                effect_disposition = _validate_position_command_effect(
                    command,
                    existing_position=existing_position,
                    incoming=position,
                )
            self._upsert_owned_position_tx(connection, _owned_position_values(position))
            trade = self._trade_for_update(connection, position.trade_id)
            if new_remaining == 0:
                raise LedgerInvariantError(
                    "final close must be derived from recorded broker deals"
                )
            if trade["status"] not in {"OPEN", "PARTIALLY_CLOSED", "CLOSE_PENDING"}:
                raise LedgerConflictError(
                    f"cannot update position from trade status {trade['status']}"
                )
            status = (
                "PARTIALLY_CLOSED"
                if new_remaining < old_remaining
                else trade["status"]
            )
            connection.execute(
                """
                UPDATE trades SET status=?,remaining_volume=?,remaining_volume_decimal=?,
                    updated_at=? WHERE trade_id=?
                """,
                (
                    status,
                    float(Decimal(str(position.remaining_volume))),
                    _decimal_text(
                        position.remaining_volume,
                        "remaining_volume",
                        nonnegative=True,
                    ),
                    position.updated_at_ms / 1_000,
                    position.trade_id,
                ),
            )
            if effect_disposition == "OVERFILL":
                connection.execute(
                    """
                    UPDATE trades SET evidence_class='QUARANTINED',learning_eligible=0,
                        updated_at=? WHERE trade_id=?
                    """,
                    (position.updated_at_ms / 1_000, position.trade_id),
                )
            if command_id is not None:
                if command is None:
                    raise LedgerInvariantError(
                        "position references an unavailable broker command"
                    )
                if (
                    command["sent_at_ms"] is None
                    or position.updated_at_ms < command["sent_at_ms"]
                ):
                    raise LedgerInvariantError(
                        "lifecycle effect acknowledgement precedes durable send"
                    )
                effect_complete = effect_disposition in {"COMPLETE", "OVERFILL"}
                target_status = "APPLIED" if effect_complete else "RECEIVED"
                if command["status"] == "APPLIED" and not effect_complete:
                    raise LedgerConflictError(
                        "applied lifecycle command cannot regress to in-progress"
                    )
                connection.execute(
                    """
                    UPDATE broker_commands SET status=?,
                        acknowledged_at_ms=COALESCE(acknowledged_at_ms,?),
                        position_id=?,last_error=?,updated_at_ms=? WHERE command_id=?
                    """,
                    (
                        target_status,
                        position.updated_at_ms,
                        position.broker_position_id,
                        "PARTIAL_CLOSE_OVERFILL"
                        if effect_disposition == "OVERFILL"
                        else None,
                        position.updated_at_ms,
                        command_id,
                    ),
                )
            self._insert_outbox_many_tx(connection, event.event_id, outbox)
            return inserted

    def append_event_with_outbox(
        self, event: TradeEventRecord, outbox: Sequence[OutboxRecord]
    ) -> bool:
        """Commit a structured trade event and every notification intent together."""

        with self._transaction() as connection:
            inserted = self._insert_exact(
                connection, "trade_events", ("event_id",), _event_values(event)
            )
            self._insert_outbox_many_tx(connection, event.event_id, outbox)
            return inserted

    def record_deal_page_and_advance_cursor(
        self,
        *,
        account_id: str,
        stream: str,
        expected_cursor: str | None,
        next_cursor: str,
        connection_epoch: str,
        deals: Sequence[BrokerDealRecord],
        updated_at_ms: int,
    ) -> bool:
        """Store a complete ordered deal page and its cursor in one transaction."""

        account = _text(account_id, "account_id")
        stream_key = _text(stream, "stream")
        next_key = _text(next_cursor, "next_cursor")
        epoch = _text(connection_epoch, "connection_epoch")
        at_ms = _timestamp_ms(updated_at_ms, "updated_at_ms")
        expected = _optional_text(expected_cursor, "expected_cursor")
        for deal in deals:
            _validate_deal(deal)
            if deal.account_id != account:
                raise LedgerInvariantError("deal page contains another account")
        with self._transaction() as connection:
            current = connection.execute(
                "SELECT * FROM deal_cursors WHERE account_id=? AND stream=?",
                (account, stream_key),
            ).fetchone()
            if current is not None and current["cursor"] == next_key:
                for deal in deals:
                    self._insert_exact(
                        connection,
                        "broker_deals",
                        ("account_id", "deal_id"),
                        _deal_values(deal),
                    )
                return False
            current_cursor = current["cursor"] if current else None
            if current_cursor != expected:
                raise LedgerConflictError(
                    f"deal cursor changed: expected {expected!r}, found {current_cursor!r}"
                )
            for deal in deals:
                self._insert_exact(
                    connection,
                    "broker_deals",
                    ("account_id", "deal_id"),
                    _deal_values(deal),
                )
            if current is None:
                connection.execute(
                    """
                    INSERT INTO deal_cursors(account_id,stream,cursor,connection_epoch,updated_at_ms)
                    VALUES(?,?,?,?,?)
                    """,
                    (account, stream_key, next_key, epoch, at_ms),
                )
            else:
                connection.execute(
                    """
                    UPDATE deal_cursors SET cursor=?,connection_epoch=?,updated_at_ms=?
                    WHERE account_id=? AND stream=?
                    """,
                    (next_key, epoch, at_ms, account, stream_key),
                )
            return True

    def get_deal_cursor(self, account_id: str, stream: str = "history") -> str | None:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT cursor FROM deal_cursors WHERE account_id=? AND stream=?",
                (_text(account_id, "account_id"), _text(stream, "stream")),
            ).fetchone()
            return row[0] if row else None

    def persist_reconciliation_atomic(
        self,
        *,
        run: ReconciliationRunRecord,
        snapshot: BrokerSnapshotRecord,
        items: Sequence[ReconciliationItemRecord],
        owned_positions: Sequence[OwnedPositionRecord],
        pending_orders: Sequence[PendingOrderRecord],
        finished_at: float,
        succeeded: bool,
        replace_scope: bool,
    ) -> bool:
        """Commit the exact report and one broker-authoritative scoped snapshot."""

        if run.account_id != snapshot.account_id:
            raise LedgerInvariantError("reconciliation run and snapshot account disagree")
        for item in items:
            if item.run_id != run.run_id:
                raise LedgerInvariantError("reconciliation item belongs to another run")
        blocked_position_ids = {
            item.broker_position_id
            for item in items
            if not item.resolved
            and item.broker_position_id is not None
            and item.discrepancy_type
            in {"BROKER_ONLY_POSITION", "POSITION_IDENTITY_CONFLICT"}
        }
        canonical_positions: list[OwnedPositionRecord] = []
        quarantined_positions: list[OwnedPositionRecord] = []
        for position in owned_positions:
            if (
                position.account_id != snapshot.account_id
                or position.symbol != snapshot.symbol
                or position.magic != snapshot.magic
            ):
                raise LedgerInvariantError("owned position is outside snapshot scope")
            if (
                position.trade_id is None
                or position.broker_position_id in blocked_position_ids
            ):
                _validate_quarantined_position(position)
                quarantined_positions.append(position)
            else:
                _validate_owned_position(position)
                canonical_positions.append(position)
        for order in pending_orders:
            _validate_pending_order(order)
            if (
                order.account_id != snapshot.account_id
                or order.symbol != snapshot.symbol
                or order.magic != snapshot.magic
            ):
                raise LedgerInvariantError("pending order is outside snapshot scope")

        run_values = _reconciliation_run_values(run)
        snapshot_values = _snapshot_values(snapshot)
        finished = _finite(finished_at, "finished_at")
        with self._transaction() as connection:
            run_inserted = self._insert_exact(
                connection,
                "reconciliation_runs",
                ("run_id",),
                run_values,
                compare_columns=tuple(
                    key
                    for key in run_values
                    if key not in {"status", "finished_at", "mismatch_count", "detail_json"}
                ),
            )
            self._insert_exact(
                connection, "broker_snapshots", ("snapshot_id",), snapshot_values
            )
            for item in items:
                self._insert_exact(
                    connection,
                    "reconciliation_items",
                    ("run_id", "item_id"),
                    _reconciliation_item_values(item),
                )
            if replace_scope:
                connection.execute(
                    "DELETE FROM owned_positions WHERE account_id=? AND symbol=? AND magic=?",
                    (snapshot.account_id, snapshot.symbol, snapshot.magic),
                )
                connection.execute(
                    """
                    UPDATE quarantined_broker_exposures SET active=0,last_seen_at_ms=?
                    WHERE account_id=? AND symbol=? AND magic=? AND active=1
                    """,
                    (
                        snapshot.captured_at_ms,
                        snapshot.account_id,
                        snapshot.symbol,
                        snapshot.magic,
                    ),
                )
                issue_reasons: dict[str, list[str]] = {}
                for item in items:
                    if item.broker_position_id in {
                        position.broker_position_id
                        for position in quarantined_positions
                    }:
                        issue_reasons.setdefault(item.broker_position_id, []).append(
                            item.discrepancy_type
                        )
                for position in quarantined_positions:
                    reason = ",".join(
                        sorted(set(issue_reasons.get(position.broker_position_id, ())))
                    ) or "UNATTRIBUTED_BROKER_POSITION"
                    values = _quarantined_position_values(
                        position,
                        run_id=run.run_id,
                        snapshot_id=snapshot.snapshot_id,
                        reason=reason,
                        seen_at_ms=snapshot.captured_at_ms,
                    )
                    connection.execute(
                        """
                        INSERT INTO quarantined_broker_exposures (
                            account_id,broker_position_id,position_ticket,
                            broker_order_id,reported_trade_id,symbol,magic,direction,
                            initial_volume_decimal,remaining_volume_decimal,
                            entry_price_decimal,broker_sl_decimal,broker_tp_decimal,
                            position_version,opened_at_ms,first_seen_run_id,
                            last_seen_run_id,snapshot_id,reason,payload_json,
                            first_seen_at_ms,last_seen_at_ms,active
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(account_id,broker_position_id) DO UPDATE SET
                            position_ticket=excluded.position_ticket,
                            broker_order_id=excluded.broker_order_id,
                            reported_trade_id=excluded.reported_trade_id,
                            symbol=excluded.symbol,magic=excluded.magic,
                            direction=excluded.direction,
                            initial_volume_decimal=excluded.initial_volume_decimal,
                            remaining_volume_decimal=excluded.remaining_volume_decimal,
                            entry_price_decimal=excluded.entry_price_decimal,
                            broker_sl_decimal=excluded.broker_sl_decimal,
                            broker_tp_decimal=excluded.broker_tp_decimal,
                            position_version=excluded.position_version,
                            opened_at_ms=excluded.opened_at_ms,
                            last_seen_run_id=excluded.last_seen_run_id,
                            snapshot_id=excluded.snapshot_id,reason=excluded.reason,
                            payload_json=excluded.payload_json,
                            last_seen_at_ms=excluded.last_seen_at_ms,active=1
                        """,
                        tuple(values.values()),
                    )
                for position in canonical_positions:
                    connection.execute(
                        f"""
                        INSERT INTO owned_positions ({','.join(_owned_position_values(position))})
                        VALUES ({','.join('?' for _ in _owned_position_values(position))})
                        """,
                        tuple(_owned_position_values(position).values()),
                    )
                    if position.trade_id is not None:
                        trade = connection.execute(
                            "SELECT * FROM trades WHERE trade_id=?",
                            (position.trade_id,),
                        ).fetchone()
                        if trade is not None:
                            if position.broker_position_id in blocked_position_ids:
                                continue
                            identity_matches = (
                                trade["account_id"] == position.account_id
                                and trade["symbol"] == position.symbol
                                and trade["magic_number"] == position.magic
                                and trade["direction"] == position.direction
                            )
                            if not identity_matches:
                                # Keep the raw broker snapshot/report but never
                                # bind a conflicting position to the trade or
                                # consume its reservation.
                                continue
                            remaining_decimal = _decimal_text(
                                position.remaining_volume,
                                "remaining_volume",
                                nonnegative=True,
                            )
                            initial_volume_decimal = _decimal_text(
                                position.initial_volume,
                                "initial_volume",
                                positive=True,
                            )
                            filled_authority = (
                                trade["filled_volume_decimal"]
                                or initial_volume_decimal
                            )
                            if Decimal(remaining_decimal) == 0:
                                reconciled_trade_status = "RECONCILIATION_REQUIRED"
                            elif Decimal(remaining_decimal) < Decimal(filled_authority):
                                reconciled_trade_status = "PARTIALLY_CLOSED"
                            else:
                                reconciled_trade_status = "OPEN"
                            connection.execute(
                                """
                                UPDATE trades SET broker_position_id=?,
                                    broker_order_id=COALESCE(broker_order_id,?),
                                    filled_entry=COALESCE(filled_entry,?),
                                    filled_volume=COALESCE(filled_volume,?),remaining_volume=?,
                                    filled_entry_decimal=COALESCE(filled_entry_decimal,?),
                                    filled_volume_decimal=COALESCE(filled_volume_decimal,?),
                                    remaining_volume_decimal=?,
                                    opened_at=COALESCE(opened_at,?),status=?,updated_at=?
                                WHERE trade_id=?
                                """,
                                (
                                    position.broker_position_id,
                                    position.broker_order_id,
                                    float(Decimal(str(position.entry_price))),
                                    float(Decimal(str(position.initial_volume))),
                                    float(Decimal(str(position.remaining_volume))),
                                    _decimal_text(
                                        position.entry_price,
                                        "entry_price",
                                        positive=True,
                                    ),
                                    initial_volume_decimal,
                                    remaining_decimal,
                                    position.opened_at_ms / 1_000,
                                    reconciled_trade_status,
                                    position.updated_at_ms / 1_000,
                                    position.trade_id,
                                ),
                            )
                            unresolved = connection.execute(
                                """
                                SELECT * FROM broker_commands
                                WHERE trade_id=? AND action='OPEN'
                                  AND status IN ('SENT','UNKNOWN','RECEIVED')
                                ORDER BY created_at_ms,command_id
                                """,
                                (position.trade_id,),
                            ).fetchall()
                            if len(unresolved) > 1:
                                raise LedgerConflictError(
                                    "multiple unresolved OPEN commands target one broker position"
                                )
                            if unresolved:
                                command = unresolved[0]
                                connection.execute(
                                    """
                                    UPDATE broker_commands SET status='APPLIED',
                                        acknowledged_at_ms=?,position_id=?,last_error=NULL,
                                        updated_at_ms=? WHERE command_id=?
                                    """,
                                    (
                                        position.updated_at_ms,
                                        position.broker_position_id,
                                        position.updated_at_ms,
                                        command["command_id"],
                                    ),
                                )
                                connection.execute(
                                    """
                                    UPDATE risk_reservations SET state='CONSUMED',
                                        consumed_at_ms=COALESCE(consumed_at_ms,?),updated_at_ms=?
                                    WHERE reservation_id=?
                                      AND state IN ('SUBMITTED','UNKNOWN')
                                    """,
                                    (
                                        position.updated_at_ms,
                                        position.updated_at_ms,
                                        command["reservation_id"],
                                    ),
                                )
                connection.execute(
                    "DELETE FROM pending_orders WHERE account_id=? AND symbol=? AND magic=?",
                    (snapshot.account_id, snapshot.symbol, snapshot.magic),
                )
                for order in pending_orders:
                    values = _pending_order_values(order)
                    connection.execute(
                        f"""
                        INSERT INTO pending_orders ({','.join(values)})
                        VALUES ({','.join('?' for _ in values)})
                        """,
                        tuple(values.values()),
                    )
                    if order.command_id is not None:
                        command = connection.execute(
                            "SELECT * FROM broker_commands WHERE command_id=?",
                            (order.command_id,),
                        ).fetchone()
                        if command is not None:
                            if (
                                command["trade_id"] == order.trade_id
                                and command["action"] == "OPEN"
                                and command["status"] in {"SENT", "UNKNOWN"}
                            ):
                                connection.execute(
                                    """
                                    UPDATE broker_commands SET status='RECEIVED',last_error=NULL,
                                        acknowledged_at_ms=?,updated_at_ms=? WHERE command_id=?
                                    """,
                                    (
                                        order.updated_at_ms,
                                        order.updated_at_ms,
                                        order.command_id,
                                    ),
                                )

                for item in items:
                    if item.trade_id is not None and item.discrepancy_type in {
                        "LEDGER_ONLY_POSITION",
                        "POSITION_IDENTITY_CONFLICT",
                    }:
                        connection.execute(
                            """
                            UPDATE trades SET status='RECONCILIATION_REQUIRED',
                                evidence_class='QUARANTINED',learning_eligible=0,updated_at=?
                            WHERE trade_id=? AND status!='CLOSED'
                            """,
                            (finished, item.trade_id),
                        )

            status = "SUCCEEDED" if succeeded else "FAILED"
            connection.execute(
                """
                UPDATE reconciliation_runs SET status=?,finished_at=?,mismatch_count=?,detail_json=?
                WHERE run_id=?
                """,
                (
                    status,
                    finished,
                    len(items),
                    _json(
                        {
                            **dict(run.detail),
                            "snapshot_id": snapshot.snapshot_id,
                            "replace_scope": replace_scope,
                            "succeeded": succeeded,
                        },
                        "reconciliation detail",
                    ),
                    run.run_id,
                ),
            )
            return run_inserted

    def list_owned_positions(
        self, account_id: str, symbol: str, magic: int
    ) -> list[dict[str, Any]]:
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM owned_positions WHERE account_id=? AND symbol=? AND magic=?
                ORDER BY broker_position_id
                """,
                (
                    _text(account_id, "account_id"),
                    _text(symbol, "symbol"),
                    _positive_int(magic, "magic"),
                ),
            ).fetchall()
            return [_decode_row(row) for row in rows]

    def get_owned_position(
        self, account_id: str, broker_position_id: str
    ) -> dict[str, Any] | None:
        with self._read_connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM owned_positions
                WHERE account_id=? AND broker_position_id=?
                """,
                (
                    _text(account_id, "account_id"),
                    _text(broker_position_id, "broker_position_id"),
                ),
            ).fetchone()
            return _decode_row(row) if row else None

    def list_quarantined_exposures(
        self,
        account_id: str,
        symbol: str,
        magic: int,
        *,
        active_only: bool = True,
    ) -> list[dict[str, Any]]:
        predicate = " AND active=1" if active_only else ""
        with self._read_connection() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM quarantined_broker_exposures
                WHERE account_id=? AND symbol=? AND magic=?{predicate}
                ORDER BY broker_position_id
                """,
                (
                    _text(account_id, "account_id"),
                    _text(symbol, "symbol"),
                    _positive_int(magic, "magic"),
                ),
            ).fetchall()
            return [_decode_row(row) for row in rows]

    def has_active_quarantined_exposure(
        self, account_id: str, symbol: str, magic: int
    ) -> bool:
        with self._read_connection() as connection:
            row = connection.execute(
                """
                SELECT 1 FROM quarantined_broker_exposures
                WHERE account_id=? AND symbol=? AND magic=? AND active=1 LIMIT 1
                """,
                (
                    _text(account_id, "account_id"),
                    _text(symbol, "symbol"),
                    _positive_int(magic, "magic"),
                ),
            ).fetchone()
            return row is not None

    def list_pending_orders(
        self, account_id: str, symbol: str, magic: int
    ) -> list[dict[str, Any]]:
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM pending_orders WHERE account_id=? AND symbol=? AND magic=?
                ORDER BY broker_order_id
                """,
                (
                    _text(account_id, "account_id"),
                    _text(symbol, "symbol"),
                    _positive_int(magic, "magic"),
                ),
            ).fetchall()
            return [_decode_row(row) for row in rows]

    def list_unresolved_commands(self) -> list[dict[str, Any]]:
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM broker_commands
                WHERE status IN ('CREATED','SENT','RECEIVED','UNKNOWN')
                ORDER BY created_at_ms,command_id
                """
            ).fetchall()
            return [_decode_row(row) for row in rows]

    def get_command(self, command_id: str) -> dict[str, Any] | None:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM broker_commands WHERE command_id=?",
                (_text(command_id, "command_id"),),
            ).fetchone()
            return _decode_row(row) if row else None

    def get_open_command_for_trade(self, trade_id: str) -> dict[str, Any] | None:
        """Return the unique durable OPEN envelope for a canonical trade."""

        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM broker_commands
                WHERE trade_id=? AND action='OPEN'
                ORDER BY created_at_ms,command_id
                """,
                (_text(trade_id, "trade_id"),),
            ).fetchall()
            if len(rows) > 1:
                raise LedgerConflictError(
                    "canonical trade has multiple OPEN command identities"
                )
            return _decode_row(rows[0]) if rows else None

    def get_reservation(self, reservation_id: str) -> dict[str, Any] | None:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM risk_reservations WHERE reservation_id=?",
                (_text(reservation_id, "reservation_id"),),
            ).fetchone()
            return _decode_row(row) if row else None

    def get_reservation_for_trade(self, trade_id: str) -> dict[str, Any] | None:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM risk_reservations WHERE trade_id=?",
                (_text(trade_id, "trade_id"),),
            ).fetchone()
            return _decode_row(row) if row else None

    def next_lifecycle_attempt(
        self,
        trade_id: str,
        broker_position_id: str,
        expected_position_version: int,
    ) -> int:
        """Return the deterministic next attempt ordinal for one position version.

        Runtime tuple ownership guarantees a single allocator.  A terminal
        rejection/reconciled-not-applied command therefore advances the ordinal,
        while a crash after command creation leaves a pending action that must be
        recovered instead of planned again.
        """

        with self._read_connection() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS attempts FROM broker_commands
                WHERE trade_id=? AND position_id=? AND expected_position_version=?
                  AND action!='OPEN'
                """,
                (
                    _text(trade_id, "trade_id"),
                    _text(broker_position_id, "broker_position_id"),
                    _nonnegative_int(
                        expected_position_version, "expected_position_version"
                    ),
                ),
            ).fetchone()
            if row is None:
                raise LedgerInvariantError(
                    "outbox attempt counter disappeared after update"
                )
            return int(row["attempts"]) + 1

    def allocate_command_sequence(
        self,
        connection_epoch: str,
        allocation_key: str,
        *,
        allocated_at_ms: int,
    ) -> int:
        """Atomically lease one monotonic per-epoch application sequence.

        The allocation key is normally the immutable command ID.  Exact retries
        recover the same sequence, while separate open/lifecycle tasks cannot
        collide even if they plan concurrently.  Unused allocations are safe
        monotonic gaps and are never reassigned.
        """

        epoch = _text(connection_epoch, "connection_epoch")
        key = _text(allocation_key, "allocation_key")
        at_ms = _timestamp_ms(allocated_at_ms, "allocated_at_ms")
        with self._transaction() as connection:
            existing = connection.execute(
                """
                SELECT sequence FROM command_sequence_allocations
                WHERE connection_epoch=? AND allocation_key=?
                """,
                (epoch, key),
            ).fetchone()
            if existing is not None:
                return int(existing["sequence"])
            row = connection.execute(
                """
                SELECT MAX(sequence) AS maximum FROM (
                    SELECT sequence FROM command_sequence_allocations
                    WHERE connection_epoch=?
                    UNION ALL
                    SELECT sequence FROM broker_commands WHERE connection_epoch=?
                )
                """,
                (epoch, epoch),
            ).fetchone()
            sequence = 1 if row is None or row["maximum"] is None else int(row["maximum"]) + 1
            if sequence > (1 << 63) - 1:
                raise LedgerInvariantError("command sequence space is exhausted")
            connection.execute(
                """
                INSERT INTO command_sequence_allocations(
                    connection_epoch,allocation_key,sequence,allocated_at_ms
                ) VALUES(?,?,?,?)
                """,
                (epoch, key, sequence, at_ms),
            )
            return sequence

    def get_trade_deals(self, trade_id: str) -> list[dict[str, Any]]:
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM broker_deals WHERE trade_id=?
                ORDER BY occurred_at,deal_id
                """,
                (_text(trade_id, "trade_id"),),
            ).fetchall()
            return [_decode_deal_row(row) for row in rows]

    def resolve_command_reconciliation(
        self,
        command_id: str,
        *,
        applied: bool,
        resolved_at_ms: int,
        proof: Mapping[str, Any],
        position_id: str | None = None,
        position: OwnedPositionRecord | None = None,
    ) -> bool:
        """Terminally resolve a lifecycle command from exact broker proof.

        Snapshot-proven not-applied is deliberately distinct from a broker
        rejection.  It releases only the lifecycle action lock; it never
        releases the trade's consumed risk reservation.
        """

        command_key = _text(command_id, "command_id")
        at_ms = _timestamp_ms(resolved_at_ms, "resolved_at_ms")
        if not isinstance(proof, Mapping) or not proof:
            raise LedgerInvariantError("reconciliation command proof must be non-empty")
        if position is not None:
            _validate_owned_position(position)
        target = "RECONCILED_APPLIED" if applied else "RECONCILED_NOT_APPLIED"
        with self._transaction() as connection:
            command = self._command_for_update(connection, command_key)
            if command["action"] == "OPEN" and not applied:
                raise LedgerInvariantError(
                    "OPEN not-applied cannot release risk without explicit broker rejection"
                )
            if command["status"] == target:
                return False
            if command["status"] not in {"SENT", "UNKNOWN", "RECEIVED"}:
                raise LedgerConflictError(
                    f"cannot reconcile command from {command['status']}"
                )
            effective_position_id = position_id or command["position_id"]
            if position is not None:
                if (
                    position.trade_id != command["trade_id"]
                    or position.broker_position_id != effective_position_id
                    or position.pending_action is not None
                    or position.pending_command_id is not None
                    or position.pending_milestone is not None
                ):
                    raise LedgerInvariantError(
                        "reconciled position does not exactly clear the command lock"
                    )
                existing_position = connection.execute(
                    """
                    SELECT * FROM owned_positions
                    WHERE account_id=? AND broker_position_id=?
                    """,
                    (position.account_id, position.broker_position_id),
                ).fetchone()
                if existing_position is None:
                    raise LedgerConflictError(
                        "reconciled lifecycle position is not registered"
                    )
                if existing_position["pending_command_id"] != command_key:
                    raise LedgerConflictError(
                        "reconciled lifecycle command does not own the position lock"
                    )
                if position.position_version < existing_position["position_version"]:
                    raise LedgerConflictError("reconciled lifecycle position regressed")
                values = _owned_position_values(position)
                immutable = (
                    "account_id",
                    "broker_position_id",
                    "trade_id",
                    "position_ticket",
                    "broker_order_id",
                    "symbol",
                    "magic",
                    "direction",
                    "initial_volume",
                    "entry_price",
                    "original_sl",
                    "tp1",
                    "tp2",
                    "tp3",
                    "initial_risk_amount",
                    "opened_at_ms",
                )
                changed = [
                    name for name in immutable if existing_position[name] != values[name]
                ]
                if changed:
                    raise LedgerConflictError(
                        f"reconciled lifecycle identity changed: {changed}"
                    )
                mutable = tuple(
                    name
                    for name in values
                    if name not in {"account_id", "broker_position_id"}
                )
                connection.execute(
                    f"""
                    UPDATE owned_positions
                    SET {','.join(f'{name}=?' for name in mutable)}
                    WHERE account_id=? AND broker_position_id=?
                    """,
                    (
                        *(values[name] for name in mutable),
                        position.account_id,
                        position.broker_position_id,
                    ),
                )
                remaining = Decimal(values["remaining_volume"])
                trade = self._trade_for_update(connection, command["trade_id"])
                trade_status = (
                    "OPEN"
                    if trade["filled_volume_decimal"] is not None
                    and remaining == Decimal(trade["filled_volume_decimal"])
                    else "PARTIALLY_CLOSED"
                )
                connection.execute(
                    """
                    UPDATE trades SET status=?,remaining_volume=?,
                        remaining_volume_decimal=?,updated_at=? WHERE trade_id=?
                    """,
                    (
                        trade_status,
                        float(remaining),
                        values["remaining_volume"],
                        at_ms / 1_000,
                        command["trade_id"],
                    ),
                )
            connection.execute(
                """
                UPDATE broker_commands SET status=?,acknowledged_at_ms=?,position_id=?,
                    last_error=NULL,updated_at_ms=? WHERE command_id=?
                """,
                (target, at_ms, effective_position_id, at_ms, command_key),
            )
            if position is None:
                connection.execute(
                    """
                    UPDATE owned_positions SET pending_action=NULL,pending_milestone=NULL,
                        pending_command_id=NULL,pending_close_volume='0',
                        pending_filled_volume='0',updated_at_ms=?
                    WHERE pending_command_id=?
                    """,
                    (at_ms, command_key),
                )
            if not applied and command["action"] == "CLOSE":
                position = connection.execute(
                    """
                    SELECT remaining_volume FROM owned_positions
                    WHERE trade_id=? AND broker_position_id=?
                    """,
                    (command["trade_id"], effective_position_id),
                ).fetchone()
                if position is not None:
                    prior_status = (
                        "OPEN"
                        if Decimal(position["remaining_volume"])
                        == Decimal(
                            connection.execute(
                                "SELECT filled_volume_decimal FROM trades WHERE trade_id=?",
                                (command["trade_id"],),
                            ).fetchone()[0]
                        )
                        else "PARTIALLY_CLOSED"
                    )
                    connection.execute(
                        "UPDATE trades SET status=?,updated_at=? WHERE trade_id=?",
                        (prior_status, at_ms / 1_000, command["trade_id"]),
                    )
            event = TradeEventRecord(
                event_id=f"{command_key}:{target.lower()}",
                trade_id=command["trade_id"],
                event_type=target,
                source="reconciliation",
                correlation_id=command_key,
                occurred_at=at_ms / 1_000,
                payload={"command_id": command_key, "proof": proof},
            )
            self._insert_exact(
                connection, "trade_events", ("event_id",), _event_values(event)
            )
            return True

    def mark_trade_submitted(
        self,
        trade_id: str,
        *,
        event_id: str,
        occurred_at: float,
        broker_order_id: str | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> bool:
        event = TradeEventRecord(
            event_id=event_id,
            trade_id=trade_id,
            event_type="ORDER_SUBMITTED",
            source="python",
            occurred_at=occurred_at,
            payload=payload or {},
            correlation_id=event_id,
        )
        with self._transaction() as connection:
            inserted = self._insert_exact(
                connection, "trade_events", ("event_id",), _event_values(event)
            )
            trade = self._trade_for_update(connection, trade_id)
            existing_order = trade["broker_order_id"]
            if existing_order and broker_order_id and existing_order != broker_order_id:
                raise LedgerConflictError("trade already has a different broker order ID")
            if trade["status"] not in {"INTENT", "RISK_APPROVED", "SUBMITTED"}:
                raise LedgerConflictError(
                    f"cannot submit trade from status {trade['status']!r}"
                )
            if trade["status"] == "SUBMITTED" and inserted:
                raise LedgerConflictError(
                    "trade submission already exists under a different event ID"
                )
            connection.execute(
                """
                UPDATE trades SET status='SUBMITTED', submitted_at=COALESCE(submitted_at, ?),
                    broker_order_id=COALESCE(broker_order_id, ?), updated_at=? WHERE trade_id=?
                """,
                (occurred_at, broker_order_id, self._clock(), trade_id),
            )
            return inserted

    def acknowledge_trade_open(
        self,
        trade_id: str,
        *,
        event_id: str,
        account_id: str,
        broker_order_id: str,
        broker_position_id: str,
        fill_price: float,
        filled_volume: float,
        opened_at: float,
        payload: Mapping[str, Any] | None = None,
        broker_timestamp: float | None = None,
    ) -> bool:
        fill = _positive(fill_price, "fill_price")
        volume = _positive(filled_volume, "filled_volume")
        opened = _finite(opened_at, "opened_at")
        event = TradeEventRecord(
            event_id=event_id,
            trade_id=trade_id,
            event_type="POSITION_OPENED",
            source="broker",
            occurred_at=opened,
            broker_timestamp=broker_timestamp,
            correlation_id=event_id,
            payload=payload or {},
        )
        with self._transaction() as connection:
            inserted = self._insert_exact(
                connection, "trade_events", ("event_id",), _event_values(event)
            )
            trade = self._trade_for_update(connection, trade_id)
            if trade["account_id"] != account_id:
                raise LedgerConflictError("broker ACK account does not match trade account")
            for column, value in (
                ("broker_order_id", broker_order_id),
                ("broker_position_id", broker_position_id),
                ("filled_entry", fill),
                ("filled_volume", volume),
            ):
                existing = trade[column]
                if existing is not None and existing != value:
                    raise LedgerConflictError(f"trade already has a different {column}")
            if trade["status"] not in {"SUBMITTED", "OPEN"}:
                raise LedgerConflictError(
                    f"cannot acknowledge open trade from status {trade['status']!r}"
                )
            if trade["status"] == "OPEN" and inserted:
                raise LedgerConflictError(
                    "position-open acknowledgement already exists under a different event ID"
                )
            try:
                connection.execute(
                    """
                    UPDATE trades SET status='OPEN', broker_order_id=?, broker_position_id=?,
                        filled_entry=?, filled_volume=?, opened_at=COALESCE(opened_at, ?),
                        filled_entry_decimal=?,filled_volume_decimal=?,
                        remaining_volume=?,remaining_volume_decimal=?,
                        updated_at=? WHERE trade_id=?
                    """,
                    (
                        broker_order_id,
                        broker_position_id,
                        fill,
                        volume,
                        opened,
                        _decimal_text(fill, "fill_price", positive=True),
                        _decimal_text(volume, "filled_volume", positive=True),
                        volume,
                        _decimal_text(volume, "remaining_volume", positive=True),
                        self._clock(),
                        trade_id,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise LedgerConflictError(
                    "broker order or position identity is already assigned to another trade"
                ) from exc
            return inserted

    def close_trade_from_recorded_deals_atomic(
        self,
        trade_id: str,
        *,
        event: TradeEventRecord,
        position_id: str,
        exit_price: Decimal | str | int | float,
        closed_at: float,
        command_id: str | None = None,
        command_terminal_status: str = "APPLIED",
        deal: BrokerDealRecord | None = None,
        outbox: Sequence[OutboxRecord] = (),
    ) -> CloseTradeResult:
        """Close using ledger-recorded broker economics, never caller aggregates."""

        trade_key = _text(trade_id, "trade_id")
        position_key = _text(position_id, "position_id")
        exit_decimal = _decimal_text(exit_price, "exit_price", positive=True)
        exit_value = float(Decimal(exit_decimal))
        closed = _finite(closed_at, "closed_at")
        if event.trade_id != trade_key:
            raise LedgerInvariantError("close event belongs to another trade")
        if command_terminal_status not in {"APPLIED", "RECONCILED_APPLIED"}:
            raise LedgerInvariantError("invalid close command terminal status")
        with self._transaction() as connection:
            trade = self._trade_for_update(connection, trade_key)
            if deal is not None:
                _validate_atomic_deal_binding(
                    deal,
                    event,
                    account_id=trade["account_id"],
                    position_id=position_key,
                    command_id=command_id,
                    allowed_deal_types={"OUT", "OUT_BY"},
                )
                self._insert_broker_deal_tx(connection, deal)
            inserted = self._insert_exact(
                connection, "trade_events", ("event_id",), _event_values(event)
            )
            if trade["broker_position_id"] not in {None, position_key}:
                raise LedgerConflictError("close position ID differs from canonical trade")
            initial_risk_text = trade["initial_risk_currency_decimal"]
            if initial_risk_text is None:
                raise LedgerInvariantError(
                    "trade lacks canonical entry-time initial risk"
                )
            initial_risk = Decimal(initial_risk_text)
            if initial_risk <= 0:
                raise LedgerInvariantError(
                    "actual R cannot be calculated without positive entry-time initial risk"
                )
            deal_rows = connection.execute(
                """
                SELECT deal_type,volume_decimal,profit_decimal,commission_decimal,
                    swap_decimal,fee_decimal FROM broker_deals
                WHERE trade_id=? AND position_id=? ORDER BY occurred_at,deal_id
                """,
                (trade_key, position_key),
            ).fetchall()
            if not deal_rows:
                raise LedgerInvariantError("trade close requires recorded broker deals")
            closing_rows = [
                row for row in deal_rows if _is_closing_deal_type(row["deal_type"])
            ]
            if not closing_rows:
                raise LedgerInvariantError("no recorded closing deal exists for this position")
            if any(
                row[f"{name}_decimal"] is None
                for row in deal_rows
                for name in ("volume", "profit", "commission", "swap", "fee")
            ):
                raise LedgerInvariantError(
                    "broker deal economics lack canonical decimal authority"
                )
            closing_volume = sum(
                (Decimal(row["volume_decimal"]) for row in closing_rows), Decimal("0")
            )
            filled_volume_text = trade["filled_volume_decimal"]
            if filled_volume_text is None or closing_volume != Decimal(filled_volume_text):
                raise LedgerInvariantError(
                    "recorded closing deal volume does not exactly close the initial fill"
                )
            pnl = sum(
                (
                    Decimal(row["profit_decimal"])
                    + Decimal(row["commission_decimal"])
                    + Decimal(row["swap_decimal"])
                    + Decimal(row["fee_decimal"])
                    for row in closing_rows
                ),
                Decimal("0"),
            )
            actual_r = pnl / initial_risk
            pnl_float = float(pnl)
            r_float = float(actual_r)
            if trade["status"] == "CLOSED":
                if inserted:
                    raise LedgerConflictError(
                        "trade close already exists under a different event ID"
                    )
                if (
                    trade["closed_exit_decimal"] == exit_decimal
                    and trade["net_realized_pnl_decimal"]
                    == _decimal_text(pnl, "net_realized_pnl")
                    and trade["closed_at"] == closed
                    and trade["actual_r_decimal"]
                    == _decimal_text(actual_r, "actual_r")
                ):
                    self._enqueue_pma_for_close_tx(
                        connection,
                        trade=trade,
                        event=event,
                        broker_closed_at_ms=int(closed * 1_000),
                    )
                    return CloseTradeResult(actual_r, False)
                raise LedgerConflictError("trade was already closed with different economics")
            if trade["status"] not in {
                "OPEN",
                "PARTIALLY_CLOSED",
                "CLOSE_PENDING",
                "RECONCILIATION_REQUIRED",
            }:
                raise LedgerConflictError(
                    f"cannot close trade from status {trade['status']!r}"
                )
            connection.execute(
                """
                UPDATE trades SET status='CLOSED',closed_exit=?,closed_at=?,
                    net_realized_pnl=?,actual_r=?,remaining_volume=0,
                    closed_exit_decimal=?,net_realized_pnl_decimal=?,
                    actual_r_decimal=?,remaining_volume_decimal='0',updated_at=?
                WHERE trade_id=?
                """,
                (
                    exit_value,
                    closed,
                    pnl_float,
                    r_float,
                    exit_decimal,
                    _decimal_text(pnl, "net_realized_pnl"),
                    _decimal_text(actual_r, "actual_r"),
                    self._clock(),
                    trade_key,
                ),
            )
            # Keep filled_volume as the authoritative initial fill.  Remaining
            # volume is the field that reaches zero.
            connection.execute(
                "DELETE FROM owned_positions WHERE account_id=? AND broker_position_id=?",
                (trade["account_id"], position_key),
            )
            reservation = connection.execute(
                "SELECT reservation_id FROM risk_reservations WHERE trade_id=?",
                (trade_key,),
            ).fetchone()
            if reservation is not None:
                connection.execute(
                    """
                    UPDATE risk_reservations SET state='RELEASED',released_at_ms=?,
                        release_reason='POSITION_CLOSED',updated_at_ms=?
                    WHERE reservation_id=? AND state='CONSUMED'
                    """,
                    (
                        int(closed * 1_000),
                        int(closed * 1_000),
                        reservation["reservation_id"],
                    ),
                )
            if command_id is not None:
                command = self._command_for_update(connection, command_id)
                if command["trade_id"] != trade_key:
                    raise LedgerConflictError("close command belongs to another trade")
                overfilled_partial = command["action"] == "PARTIAL_CLOSE"
                if command["action"] not in {"CLOSE", "CLOSE_ALL", "PARTIAL_CLOSE"}:
                    raise LedgerConflictError("position close targets a non-close command")
                if command["status"] not in {"SENT", "UNKNOWN", "RECEIVED"}:
                    raise LedgerConflictError(
                        f"cannot apply close command from {command['status']}"
                    )
                if (
                    command["sent_at_ms"] is None
                    or int(closed * 1_000) < command["sent_at_ms"]
                ):
                    raise LedgerInvariantError("close acknowledgement precedes durable send")
                if overfilled_partial:
                    try:
                        effect = json.loads(command["expected_effect_json"])
                    except (TypeError, json.JSONDecodeError) as exc:
                        raise LedgerInvariantError(
                            "partial-close command expected effect is corrupt"
                        ) from exc
                    target_remaining = Decimal(
                        _decimal_text(
                            effect.get("target_remaining_volume"),
                            "target_remaining_volume",
                            positive=True,
                        )
                    )
                    if target_remaining <= 0:
                        raise LedgerInvariantError(
                            "closed PARTIAL_CLOSE does not prove an overfill"
                        )
                    connection.execute(
                        """
                        UPDATE trades SET evidence_class='QUARANTINED',learning_eligible=0,
                            updated_at=? WHERE trade_id=?
                        """,
                        (closed, trade_key),
                    )
                connection.execute(
                    """
                    UPDATE broker_commands SET status=?,acknowledged_at_ms=?,
                        position_id=?,last_error=?,updated_at_ms=? WHERE command_id=?
                    """,
                    (
                        command_terminal_status,
                        int(closed * 1_000),
                        position_key,
                        "PARTIAL_CLOSE_OVERFILLED_POSITION_CLOSED"
                        if overfilled_partial
                        else None,
                        int(closed * 1_000),
                        command_id,
                    ),
                )
            self._enqueue_pma_for_close_tx(
                connection,
                trade=trade,
                event=event,
                broker_closed_at_ms=int(closed * 1_000),
            )
            self._insert_outbox_many_tx(connection, event.event_id, outbox)
            return CloseTradeResult(actual_r, inserted)

    def close_trade(
        self,
        trade_id: str,
        *,
        event_id: str,
        exit_price: float,
        net_realized_pnl: float,
        closed_at: float,
        payload: Mapping[str, Any] | None = None,
        broker_timestamp: float | None = None,
    ) -> float:
        """Compatibility wrapper; supplied PnL is deliberately ignored.

        New code should call :meth:`close_trade_from_recorded_deals_atomic`.
        The wrapper remains for legacy call sites while enforcing v2 economics.
        """

        exit_value = _positive(exit_price, "exit_price")
        # Validate only for API compatibility.  Authoritative economics below
        # are calculated from broker_deals and cannot be supplied by a caller.
        _finite(net_realized_pnl, "net_realized_pnl")
        closed = _finite(closed_at, "closed_at")
        event = TradeEventRecord(
            event_id=event_id,
            trade_id=trade_id,
            event_type="TRADE_CLOSED",
            source="broker",
            occurred_at=closed,
            broker_timestamp=broker_timestamp,
            correlation_id=event_id,
            payload=payload or {},
        )
        with self._read_connection() as connection:
            trade = self._trade_for_update(connection, trade_id)
            position_id = trade["broker_position_id"]
        if not position_id:
            raise LedgerInvariantError("trade has no authoritative broker position ID")
        return float(self.close_trade_from_recorded_deals_atomic(
            trade_id,
            event=event,
            position_id=position_id,
            exit_price=exit_value,
            closed_at=closed,
        ).actual_r)

    def get_trade(self, trade_id: str) -> dict[str, Any] | None:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM trades WHERE trade_id=?", (_text(trade_id, "trade_id"),)
            ).fetchone()
            return _decode_row(row) if row else None

    def list_learning_candidate_refs(
        self,
        *,
        account_id: str,
        symbol: str,
        magic: int,
        source_config_hash: str,
        as_of_broker_time_ms: int,
        limit: int = 10_000,
    ) -> list[tuple[str, str]]:
        """Return only canonical closed trades with broker-time close evidence."""

        account = _text(account_id, "account_id")
        instrument = _text(symbol, "symbol")
        magic_value = _positive_int(magic, "magic")
        config = _text(source_config_hash, "source_config_hash")
        as_of_ms = _timestamp_ms(as_of_broker_time_ms, "as_of_broker_time_ms")
        count = _positive_int(limit, "limit")
        if count > 10_000:
            raise LedgerInvariantError("learning candidate limit exceeds safety bound")
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT t.trade_id,e.event_id
                FROM trades t
                JOIN trade_events e ON e.trade_id=t.trade_id
                WHERE t.account_id=? AND t.symbol=? AND t.magic_number=?
                  AND t.config_hash=? AND t.status='CLOSED'
                  AND t.evidence_class='CANONICAL_RUNTIME'
                  AND t.learning_eligible=1 AND t.actual_r_decimal IS NOT NULL
                  AND e.event_type IN (
                    'POSITION_CLOSED','TRADE_CLOSED','POSITION_CLOSED_RECONCILED'
                  )
                  AND e.source IN ('broker','reconciliation')
                  AND e.broker_timestamp IS NOT NULL
                  AND CAST(ROUND(e.broker_timestamp*1000) AS INTEGER)<=?
                ORDER BY e.broker_timestamp,t.trade_id,e.event_id LIMIT ?
                """,
                (account, instrument, magic_value, config, as_of_ms, count),
            ).fetchall()
            return [(row["trade_id"], row["event_id"]) for row in rows]

    def build_authoritative_learning_source(
        self,
        trade_id: str,
        close_event_id: str,
        *,
        created_at_ms: int,
    ) -> LearningSourceRecord:
        """Re-derive immutable Agent-20 source evidence from canonical tables."""

        trade_key = _text(trade_id, "trade_id")
        event_key = _text(close_event_id, "close_event_id")
        created = _timestamp_ms(created_at_ms, "created_at_ms")
        with self._read_connection() as connection:
            trade = connection.execute(
                "SELECT * FROM trades WHERE trade_id=?", (trade_key,)
            ).fetchone()
            event = connection.execute(
                "SELECT * FROM trade_events WHERE event_id=?", (event_key,)
            ).fetchone()
            if trade is None or event is None or event["trade_id"] != trade_key:
                raise LedgerInvariantError("learning source trade/close identity is unknown")
            if (
                trade["status"] != "CLOSED"
                or trade["evidence_class"] != "CANONICAL_RUNTIME"
                or trade["learning_eligible"] != 1
                or trade["config_hash"] is None
                or trade["actual_r_decimal"] is None
                or trade["opened_at"] is None
                or trade["closed_at"] is None
            ):
                raise LedgerInvariantError(
                    "trade is not authoritative canonical learning evidence"
                )
            if (
                event["event_type"]
                not in {
                    "POSITION_CLOSED",
                    "TRADE_CLOSED",
                    "POSITION_CLOSED_RECONCILED",
                }
                or event["source"] not in {"broker", "reconciliation"}
                or event["broker_timestamp"] is None
            ):
                raise LedgerInvariantError("close event is not broker-confirmed evidence")
            attribution_rows = connection.execute(
                """
                SELECT * FROM trade_attributions WHERE trade_id=?
                ORDER BY agent_id,attribution_id
                """,
                (trade_key,),
            ).fetchall()
            deal_rows = connection.execute(
                """
                SELECT * FROM broker_deals WHERE trade_id=?
                ORDER BY occurred_at,deal_id
                """,
                (trade_key,),
            ).fetchall()

        if len(attribution_rows) < 3 or not deal_rows:
            raise LedgerInvariantError(
                "learning evidence requires three contributors and broker deals"
            )
        attribution: list[dict[str, Any]] = []
        for row in attribution_rows:
            decoded = _decode_row(row)
            metadata = decoded.get("metadata", {})
            signal_id = metadata.get("signal_id") if isinstance(metadata, Mapping) else None
            attribution.append(
                {
                    "agent_id": row["agent_id"],
                    "signal_id": _text(signal_id, "attribution signal_id"),
                    "strategy_slug": row["strategy_slug"],
                    "tags": decoded.get("tags", []),
                    "conviction_decimal": row["conviction_decimal"],
                    "learned_weight_decimal": row["learned_weight_decimal"],
                    "effective_weight_decimal": row["effective_weight_decimal"],
                }
            )
        deals = [_decode_deal_row(row) for row in deal_rows]
        if any(item["account_id"] != trade["account_id"] for item in deals):
            raise LedgerInvariantError("learning broker deals cross account scope")
        initial_risk = Decimal(trade["initial_risk_currency_decimal"])
        if initial_risk <= 0:
            raise LedgerInvariantError("learning source has no positive initial risk")
        closing_deals = [
            item for item in deals if _is_closing_deal_type(item["deal_type"])
        ]
        if not closing_deals:
            raise LedgerInvariantError("learning source has no closing broker deals")
        calculated_pnl = sum(
            (
                Decimal(item["profit"])
                + Decimal(item["commission"])
                + Decimal(item["swap"])
                + Decimal(item["fee"])
                for item in closing_deals
            ),
            Decimal("0"),
        )
        calculated_r = calculated_pnl / initial_risk
        if calculated_r != Decimal(trade["actual_r_decimal"]):
            raise LedgerInvariantError("stored actual R differs from broker-deal economics")
        closed_at_ms = _timestamp_ms(
            int(round(event["broker_timestamp"] * 1_000)),
            "broker_closed_at_ms",
        )
        opened_at_ms = _timestamp_ms(
            int(round(trade["opened_at"] * 1_000)), "broker_opened_at_ms"
        )
        if closed_at_ms <= opened_at_ms:
            raise LedgerInvariantError("learning broker close does not follow open")
        broker_deals = [
            {
                "deal_id": item["deal_id"],
                "position_id": item["position_id"],
                "order_id": item["order_id"],
                "command_id": item.get("command_id"),
                "deal_type": item["deal_type"],
                "volume_decimal": item["volume"],
                "price_decimal": item["price"],
                "profit_decimal": item["profit"],
                "commission_decimal": item["commission"],
                "swap_decimal": item["swap"],
                "fee_decimal": item["fee"],
                "occurred_at_ms": int(round(item["occurred_at"] * 1_000)),
            }
            for item in deals
        ]
        content = {
            "trade_id": trade_key,
            "close_event_id": event_key,
            "account_id": trade["account_id"],
            "symbol": trade["symbol"],
            "magic": trade["magic_number"],
            "source_config_hash": trade["config_hash"],
            "regime": trade["regime_at_entry"],
            "session": trade["session_at_entry"],
            "actual_r_decimal": trade["actual_r_decimal"],
            "broker_opened_at_ms": opened_at_ms,
            "broker_closed_at_ms": closed_at_ms,
            "attribution": attribution,
            "broker_deals": broker_deals,
        }
        content_hash = hashlib.sha256(
            _json(content, "learning source content").encode("utf-8")
        ).hexdigest()
        return LearningSourceRecord(
            source_id=f"source-{content_hash[:32]}",
            trade_id=trade_key,
            close_event_id=event_key,
            account_id=trade["account_id"],
            symbol=trade["symbol"],
            magic=trade["magic_number"],
            source_config_hash=trade["config_hash"],
            regime=trade["regime_at_entry"],
            session=trade["session_at_entry"],
            actual_r=trade["actual_r_decimal"],
            broker_opened_at_ms=opened_at_ms,
            broker_closed_at_ms=closed_at_ms,
            attribution=attribution,
            broker_deals=broker_deals,
            content_hash=content_hash,
            created_at_ms=created,
        )

    def record_learning_source(self, record: LearningSourceRecord) -> bool:
        values = _learning_source_values(record)
        with self._transaction() as connection:
            trade = self._trade_for_update(connection, record.trade_id)
            event = connection.execute(
                "SELECT trade_id FROM trade_events WHERE event_id=?",
                (record.close_event_id,),
            ).fetchone()
            if event is None or event["trade_id"] != record.trade_id:
                raise LedgerInvariantError("learning source close event identity differs")
            if (
                trade["account_id"] != record.account_id
                or trade["symbol"] != record.symbol
                or trade["magic_number"] != record.magic
                or trade["config_hash"] != record.source_config_hash
                or trade["actual_r_decimal"] != values["actual_r_decimal"]
            ):
                raise LedgerConflictError("learning source differs from canonical trade")
            return self._insert_exact(
                connection, "learning_sources", ("source_id",), values
            )

    def list_learning_sources(
        self,
        *,
        account_id: str,
        symbol: str,
        magic: int,
        source_config_hash: str,
        as_of_broker_time_ms: int,
    ) -> list[dict[str, Any]]:
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM learning_sources
                WHERE account_id=? AND symbol=? AND magic=?
                  AND source_config_hash=? AND broker_closed_at_ms<=?
                ORDER BY broker_closed_at_ms,trade_id,close_event_id
                """,
                (
                    _text(account_id, "account_id"),
                    _text(symbol, "symbol"),
                    _positive_int(magic, "magic"),
                    _text(source_config_hash, "source_config_hash"),
                    _timestamp_ms(as_of_broker_time_ms, "as_of_broker_time_ms"),
                ),
            ).fetchall()
            return [_decode_row(row) for row in rows]

    def next_learning_proposal_lineage(
        self,
        *,
        account_id: str,
        symbol: str,
        magic: int,
        source_config_hash: str,
        target_config_hash: str,
    ) -> tuple[int, str | None]:
        with self._read_connection() as connection:
            row = connection.execute(
                """
                SELECT proposal_version,proposal_hash FROM learning_proposals
                WHERE account_id=? AND symbol=? AND magic=?
                  AND source_config_hash=? AND target_config_hash=?
                ORDER BY proposal_version DESC LIMIT 1
                """,
                (
                    _text(account_id, "account_id"),
                    _text(symbol, "symbol"),
                    _positive_int(magic, "magic"),
                    _text(source_config_hash, "source_config_hash"),
                    _text(target_config_hash, "target_config_hash"),
                ),
            ).fetchone()
            return (1, None) if row is None else (
                row["proposal_version"] + 1,
                row["proposal_hash"],
            )

    def record_learning_proposal_bundle(
        self,
        proposal: LearningProposalRecord,
        snapshot: LearningSnapshotRecord,
        *,
        source_ids: Sequence[str],
    ) -> bool:
        proposal_values = _learning_proposal_values(proposal)
        snapshot_values = _learning_snapshot_values(snapshot)
        frozen_source_ids = tuple(_text(item, "source_id") for item in source_ids)
        if not frozen_source_ids or len(frozen_source_ids) != len(set(frozen_source_ids)):
            raise LedgerInvariantError("learning proposal source IDs must be unique and non-empty")
        if (
            snapshot.proposal_id != proposal.proposal_id
            or snapshot.proposal_hash != proposal.proposal_hash
        ):
            raise LedgerInvariantError("learning proposal snapshot identity differs")
        with self._transaction() as connection:
            sources = connection.execute(
                f"""
                SELECT * FROM learning_sources
                WHERE source_id IN ({','.join('?' for _ in frozen_source_ids)})
                ORDER BY source_id
                """,
                frozen_source_ids,
            ).fetchall()
            if len(sources) != len(frozen_source_ids):
                raise LedgerInvariantError("learning proposal references unknown sources")
            for source in sources:
                if (
                    source["account_id"] != proposal.account_id
                    or source["symbol"] != proposal.symbol
                    or source["magic"] != proposal.magic
                    or source["source_config_hash"] != proposal.source_config_hash
                    or source["broker_closed_at_ms"] > proposal.as_of_broker_time_ms
                ):
                    raise LedgerConflictError("learning proposal source scope differs")
            source_trade_ids = tuple(sorted(source["trade_id"] for source in sources))
            source_event_ids = tuple(
                sorted(source["close_event_id"] for source in sources)
            )
            proposal_payload = proposal.proposal
            if (
                not isinstance(proposal_payload, Mapping)
                or tuple(proposal_payload.get("source_trade_ids", ()))
                != source_trade_ids
                or tuple(proposal_payload.get("source_event_ids", ()))
                != source_event_ids
            ):
                raise LedgerConflictError(
                    "learning proposal lineage differs from linked source set"
                )
            source_set = [
                {
                    "source_id": source["source_id"],
                    "trade_id": source["trade_id"],
                    "close_event_id": source["close_event_id"],
                    "content_hash": source["content_hash"],
                }
                for source in sources
            ]
            expected_source_set_hash = hashlib.sha256(
                _json(source_set, "learning proposal source set").encode("utf-8")
            ).hexdigest()
            if snapshot.source_set_hash != expected_source_set_hash:
                raise LedgerConflictError("learning snapshot source-set hash differs")
            inserted = self._insert_exact(
                connection,
                "learning_proposals",
                ("proposal_id",),
                proposal_values,
            )
            for source in sources:
                self._insert_exact(
                    connection,
                    "learning_proposal_sources",
                    ("proposal_id", "source_id"),
                    {
                        "proposal_id": proposal.proposal_id,
                        "source_id": source["source_id"],
                        "trade_id": source["trade_id"],
                        "close_event_id": source["close_event_id"],
                        "source_content_hash": source["content_hash"],
                    },
                )
            self._insert_exact(
                connection,
                "learning_snapshots",
                ("snapshot_id",),
                snapshot_values,
            )
            return inserted

    def get_learning_proposal(self, proposal_id: str) -> dict[str, Any] | None:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM learning_proposals WHERE proposal_id=?",
                (_text(proposal_id, "proposal_id"),),
            ).fetchone()
            return _decode_row(row) if row else None

    def get_learning_proposal_for_source(
        self, close_event_id: str, *, target_config_hash: str
    ) -> dict[str, Any] | None:
        with self._read_connection() as connection:
            row = connection.execute(
                """
                SELECT p.* FROM learning_proposal_sources s
                JOIN learning_proposals p ON p.proposal_id=s.proposal_id
                WHERE s.close_event_id=? AND p.target_config_hash=?
                ORDER BY p.proposal_version,p.proposal_id LIMIT 1
                """,
                (
                    _text(close_event_id, "close_event_id"),
                    _text(target_config_hash, "target_config_hash"),
                ),
            ).fetchone()
            return _decode_row(row) if row else None

    def record_learning_approval(self, record: LearningApprovalRecord) -> bool:
        values = _learning_approval_values(record)
        with self._transaction() as connection:
            proposal = connection.execute(
                "SELECT proposal_hash FROM learning_proposals WHERE proposal_id=?",
                (record.proposal_id,),
            ).fetchone()
            if proposal is None or proposal["proposal_hash"] != record.proposal_hash:
                raise LedgerConflictError("approval does not bind the stored proposal hash")
            return self._insert_exact(
                connection, "learning_approvals", ("approval_id",), values
            )

    def get_learning_approval(self, approval_id: str) -> dict[str, Any] | None:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM learning_approvals WHERE approval_id=?",
                (_text(approval_id, "approval_id"),),
            ).fetchone()
            return _decode_row(row) if row else None

    def record_learning_activation(self, record: LearningActivationRecord) -> bool:
        values = _learning_activation_values(record)
        with self._transaction() as connection:
            proposal = connection.execute(
                "SELECT * FROM learning_proposals WHERE proposal_id=?",
                (record.proposal_id,),
            ).fetchone()
            approval = connection.execute(
                "SELECT * FROM learning_approvals WHERE approval_id=?",
                (record.approval_id,),
            ).fetchone()
            if proposal is None or approval is None:
                raise LedgerInvariantError("activation requires stored proposal and approval")
            if (
                proposal["proposal_hash"] != record.proposal_hash
                or approval["proposal_id"] != record.proposal_id
                or approval["proposal_hash"] != record.proposal_hash
                or proposal["account_id"] != record.account_id
                or proposal["symbol"] != record.symbol
                or proposal["magic"] != record.magic
                or proposal["target_config_hash"] != record.target_config_hash
            ):
                raise LedgerConflictError("activation scope/hash differs from approval")
            inserted = self._insert_exact(
                connection, "learning_activations", ("activation_id",), values
            )
            current = connection.execute(
                """
                SELECT a.activated_at_ms,s.activation_id
                FROM learning_active_scopes s
                JOIN learning_activations a ON a.activation_id=s.activation_id
                WHERE s.account_id=? AND s.symbol=? AND s.magic=?
                  AND s.target_config_hash=? AND s.regime=? AND s.session=?
                """,
                (
                    record.account_id,
                    record.symbol,
                    record.magic,
                    record.target_config_hash,
                    record.regime,
                    record.session,
                ),
            ).fetchone()
            if current is not None and current["activated_at_ms"] > record.activated_at_ms:
                raise LedgerConflictError("cannot replace a newer learning activation")
            connection.execute(
                """
                INSERT INTO learning_active_scopes (
                    account_id,symbol,magic,target_config_hash,regime,session,
                    activation_id,updated_at_ms
                ) VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(account_id,symbol,magic,target_config_hash,regime,session)
                DO UPDATE SET activation_id=excluded.activation_id,
                    updated_at_ms=excluded.updated_at_ms
                """,
                (
                    record.account_id,
                    record.symbol,
                    record.magic,
                    record.target_config_hash,
                    record.regime,
                    record.session,
                    record.activation_id,
                    record.activated_at_ms,
                ),
            )
            return inserted

    def get_active_learning_activation(
        self,
        *,
        account_id: str,
        symbol: str,
        magic: int,
        target_config_hash: str,
        regime: str,
        session: str,
    ) -> dict[str, Any] | None:
        with self._read_connection() as connection:
            row = connection.execute(
                """
                SELECT a.*,p.proposal_version
                FROM learning_active_scopes s
                JOIN learning_activations a ON a.activation_id=s.activation_id
                JOIN learning_proposals p ON p.proposal_id=a.proposal_id
                WHERE s.account_id=? AND s.symbol=? AND s.magic=?
                  AND s.target_config_hash=? AND s.regime=? AND s.session=?
                """,
                (
                    _text(account_id, "account_id"),
                    _text(symbol, "symbol"),
                    _positive_int(magic, "magic"),
                    _text(target_config_hash, "target_config_hash"),
                    _text(regime, "regime"),
                    _text(session, "session"),
                ),
            ).fetchone()
            return _decode_row(row) if row else None

    def get_pma_job(self, job_id: str) -> dict[str, Any] | None:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM pma_jobs WHERE job_id=?",
                (_text(job_id, "job_id"),),
            ).fetchone()
            return _decode_row(row) if row else None

    def get_pma_job_for_trade(self, trade_id: str) -> dict[str, Any] | None:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM pma_jobs WHERE trade_id=?",
                (_text(trade_id, "trade_id"),),
            ).fetchone()
            return _decode_row(row) if row else None

    def claim_pma_jobs(
        self,
        worker_id: str,
        *,
        now_ms: int,
        lease_ms: int = 60_000,
        limit: int = 1,
        account_id: str | None = None,
        symbol: str | None = None,
        magic: int | None = None,
    ) -> list[dict[str, Any]]:
        worker = _text(worker_id, "worker_id")
        at_ms = _timestamp_ms(now_ms, "now_ms")
        lease = _positive_int(lease_ms, "lease_ms")
        count = _positive_int(limit, "limit")
        if lease > 3_600_000 or count > 100:
            raise LedgerInvariantError("PMA lease/claim batch exceeds safety limit")
        scope_values = (account_id, symbol, magic)
        if any(item is not None for item in scope_values) and not all(
            item is not None for item in scope_values
        ):
            raise LedgerInvariantError(
                "PMA claim scope requires account_id, symbol and magic together"
            )
        scope_sql = ""
        scope_params: tuple[Any, ...] = ()
        if account_id is not None and symbol is not None and magic is not None:
            scope_sql = " AND account_id=? AND symbol=? AND magic=?"
            scope_params = (
                _text(account_id, "account_id"),
                _text(symbol, "symbol"),
                _positive_int(magic, "magic"),
            )
        with self._transaction() as connection:
            connection.execute(
                """
                UPDATE pma_jobs SET status='DEAD_LETTER',locked_by=NULL,
                    locked_until_ms=NULL,last_error=COALESCE(last_error,'lease expired'),
                    dead_lettered_at_ms=?,updated_at_ms=?
                WHERE status='IN_FLIGHT' AND locked_until_ms<=?
                  AND attempts>=max_attempts
                """,
                (at_ms, at_ms, at_ms),
            )
            rows = connection.execute(
                f"""
                SELECT job_id FROM pma_jobs
                WHERE due_at_ms<=? AND attempts<max_attempts
                  AND (
                    status='PENDING'
                    OR (status='IN_FLIGHT' AND locked_until_ms<=?)
                  ){scope_sql}
                ORDER BY due_at_ms,job_id LIMIT ?
                """,
                (at_ms, at_ms, *scope_params, count),
            ).fetchall()
            job_ids = tuple(row["job_id"] for row in rows)
            for job_id in job_ids:
                connection.execute(
                    """
                    UPDATE pma_jobs SET status='IN_FLIGHT',attempts=attempts+1,
                        locked_by=?,locked_until_ms=?,last_error=NULL,updated_at_ms=?
                    WHERE job_id=?
                    """,
                    (worker, at_ms + lease, at_ms, job_id),
                )
            return [
                _decode_row(
                    connection.execute(
                        "SELECT * FROM pma_jobs WHERE job_id=?", (job_id,)
                    ).fetchone()
                )
                for job_id in job_ids
            ]

    def pin_pma_target_config(
        self,
        job_id: str,
        worker_id: str,
        *,
        target_config_hash: str,
        updated_at_ms: int,
    ) -> bool:
        job_key = _text(job_id, "job_id")
        worker = _text(worker_id, "worker_id")
        target = _text(target_config_hash, "target_config_hash")
        at_ms = _timestamp_ms(updated_at_ms, "updated_at_ms")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM pma_jobs WHERE job_id=?", (job_key,)
            ).fetchone()
            if row is None:
                raise LedgerInvariantError("unknown PMA job")
            _require_pma_lease(row, worker, at_ms)
            if row["target_config_hash"] is not None:
                if row["target_config_hash"] == target:
                    return False
                raise LedgerConflictError("PMA target config is already pinned differently")
            connection.execute(
                "UPDATE pma_jobs SET target_config_hash=?,updated_at_ms=? WHERE job_id=?",
                (target, at_ms, job_key),
            )
            return True

    def fail_pma_job(
        self,
        job_id: str,
        worker_id: str,
        *,
        error: str,
        failed_at_ms: int,
        retry_at_ms: int | None,
    ) -> str:
        job_key = _text(job_id, "job_id")
        worker = _text(worker_id, "worker_id")
        reason = _text(error, "error")
        at_ms = _timestamp_ms(failed_at_ms, "failed_at_ms")
        retry = None if retry_at_ms is None else _timestamp_ms(retry_at_ms, "retry_at_ms")
        if retry is not None and retry < at_ms:
            raise LedgerInvariantError("PMA retry cannot precede failure")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM pma_jobs WHERE job_id=?", (job_key,)
            ).fetchone()
            if row is None:
                raise LedgerInvariantError("unknown PMA job")
            _require_pma_lease(row, worker, at_ms)
            dead = retry is None or row["attempts"] >= row["max_attempts"]
            status = "DEAD_LETTER" if dead else "PENDING"
            connection.execute(
                """
                UPDATE pma_jobs SET status=?,due_at_ms=COALESCE(?,due_at_ms),
                    locked_by=NULL,locked_until_ms=NULL,last_error=?,updated_at_ms=?,
                    dead_lettered_at_ms=? WHERE job_id=?
                """,
                (
                    status,
                    retry,
                    reason,
                    at_ms,
                    at_ms if dead else None,
                    job_key,
                ),
            )
            return status

    def terminalize_pma_job(
        self,
        job_id: str,
        worker_id: str,
        *,
        status: str,
        reason: str,
        completed_at_ms: int,
        result_proposal_id: str | None = None,
    ) -> bool:
        if status not in {"INELIGIBLE", "SUPERSEDED", "COMPLETED"}:
            raise LedgerInvariantError("invalid PMA terminal status")
        job_key = _text(job_id, "job_id")
        worker = _text(worker_id, "worker_id")
        detail = _text(reason, "reason")
        proposal_id = _optional_text(result_proposal_id, "result_proposal_id")
        at_ms = _timestamp_ms(completed_at_ms, "completed_at_ms")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM pma_jobs WHERE job_id=?", (job_key,)
            ).fetchone()
            if row is None:
                raise LedgerInvariantError("unknown PMA job")
            if row["status"] == status:
                if (
                    row["eligibility_reason"] == detail
                    and row["result_proposal_id"] == proposal_id
                ):
                    return False
                raise LedgerConflictError("PMA job already terminalized differently")
            _require_pma_lease(row, worker, at_ms)
            connection.execute(
                """
                UPDATE pma_jobs SET status=?,locked_by=NULL,locked_until_ms=NULL,
                    eligibility_reason=?,result_proposal_id=?,completed_at_ms=?,
                    updated_at_ms=? WHERE job_id=?
                """,
                (status, detail, proposal_id, at_ms, at_ms, job_key),
            )
            return True

    def get_trade_events(self, trade_id: str) -> list[dict[str, Any]]:
        with self._read_connection() as connection:
            rows = connection.execute(
                "SELECT * FROM trade_events WHERE trade_id=? ORDER BY occurred_at, event_id",
                (_text(trade_id, "trade_id"),),
            ).fetchall()
            return [_decode_row(row) for row in rows]

    def latest_provider_health(self, provider_name: str) -> dict[str, Any] | None:
        with self._read_connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM provider_health WHERE provider_name=?
                ORDER BY observed_at DESC, observation_id DESC LIMIT 1
                """,
                (_text(provider_name, "provider_name"),),
            ).fetchone()
            return _decode_row(row) if row else None

    def _initialize_schema(self) -> None:
        connection = self._require_connection()
        try:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version > LEDGER_SCHEMA_VERSION:
                raise SchemaVersionError(
                    f"ledger schema {version} is newer than supported {LEDGER_SCHEMA_VERSION}"
                )
            if version == 0:
                existing = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                    ).fetchall()
                }
                if existing:
                    raise SchemaVersionError(
                        "refusing to initialize an unversioned non-empty SQLite database"
                    )
                with self._transaction() as tx:
                    for statement in _SCHEMA_V1:
                        tx.execute(statement)
                    tx.execute(
                        "INSERT INTO schema_meta(key,value) VALUES('schema_version',?)",
                        ("1",),
                    )
                    tx.execute("PRAGMA user_version=1")
                version = 1
            if version == 1:
                with self._transaction() as tx:
                    for statement in _SCHEMA_V2:
                        tx.execute(statement)
                    tx.execute(
                        "UPDATE provider_health SET state='ERROR' WHERE UPPER(state)='UNAVAILABLE'"
                    )
                    tx.execute(
                        "UPDATE schema_meta SET value=? WHERE key='schema_version'",
                        ("2",),
                    )
                    tx.execute("PRAGMA user_version=2")
                version = 2
            if version == 2:
                with self._transaction() as tx:
                    for statement in _SCHEMA_V3:
                        tx.execute(statement)
                    tx.execute(
                        "UPDATE schema_meta SET value=? WHERE key='schema_version'",
                        ("3",),
                    )
                    tx.execute("PRAGMA user_version=3")
                version = 3
            if version == 3:
                with self._transaction() as tx:
                    for statement in _SCHEMA_V4:
                        tx.execute(statement)
                    _canonicalize_v4_decimal_backfill(tx)
                    tx.execute(
                        "UPDATE schema_meta SET value=? WHERE key='schema_version'",
                        ("4",),
                    )
                    tx.execute("PRAGMA user_version=4")
                version = 4
            if version == 4:
                with self._transaction() as tx:
                    for statement in _SCHEMA_V5:
                        tx.execute(statement)
                    _canonicalize_v5_attribution_backfill(tx)
                    for table in _IMMUTABLE_LEARNING_TABLES:
                        tx.execute(
                            f"""
                            CREATE TRIGGER {table}_immutable_update
                            BEFORE UPDATE ON {table}
                            BEGIN SELECT RAISE(ABORT,'immutable learning record'); END
                            """
                        )
                        tx.execute(
                            f"""
                            CREATE TRIGGER {table}_immutable_delete
                            BEFORE DELETE ON {table}
                            BEGIN SELECT RAISE(ABORT,'immutable learning record'); END
                            """
                        )
                    tx.execute(
                        "UPDATE schema_meta SET value=? WHERE key='schema_version'",
                        ("5",),
                    )
                    tx.execute("PRAGMA user_version=5")
                version = 5
            if version == 5:
                with self._transaction() as tx:
                    for statement in _SCHEMA_V6:
                        tx.execute(statement)
                    tx.execute(
                        "UPDATE schema_meta SET value=? WHERE key='schema_version'",
                        ("6",),
                    )
                    tx.execute("PRAGMA user_version=6")
                version = 6
            if version == 6:
                with self._transaction() as tx:
                    for statement in _SCHEMA_V7:
                        tx.execute(statement)
                    tx.execute(
                        "UPDATE schema_meta SET value=? WHERE key='schema_version'",
                        (str(LEDGER_SCHEMA_VERSION),),
                    )
                    tx.execute(f"PRAGMA user_version={LEDGER_SCHEMA_VERSION}")
                version = LEDGER_SCHEMA_VERSION
            if version != LEDGER_SCHEMA_VERSION:
                raise SchemaVersionError(
                    f"no migration path from ledger schema {version}"
                )
        except LedgerError:
            raise
        except sqlite3.Error as exc:
            raise LedgerUnavailableError(f"cannot initialize ledger schema: {exc}") from exc

    def _trade_for_update(self, connection: sqlite3.Connection, trade_id: str) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM trades WHERE trade_id=?", (_text(trade_id, "trade_id"),)
        ).fetchone()
        if row is None:
            raise LedgerInvariantError(f"unknown trade {trade_id!r}")
        return row

    def _command_for_update(
        self, connection: sqlite3.Connection, command_id: str
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM broker_commands WHERE command_id=?",
            (_text(command_id, "command_id"),),
        ).fetchone()
        if row is None:
            raise LedgerInvariantError(f"unknown broker command {command_id!r}")
        return row

    def _mark_command_unknown_tx(
        self,
        connection: sqlite3.Connection,
        command_id: str,
        unknown_at_ms: int,
        reason: str,
    ) -> bool:
        command = self._command_for_update(connection, command_id)
        if command["status"] == "UNKNOWN":
            if command["last_error"] == reason:
                return False
            raise LedgerConflictError("command is already UNKNOWN for a different reason")
        if command["status"] not in {"SENT", "RECEIVED"}:
            raise LedgerConflictError(
                "only a SENT/RECEIVED command can become UNKNOWN, "
                f"found {command['status']}"
            )
        if command["sent_at_ms"] is None or unknown_at_ms < command["sent_at_ms"]:
            raise LedgerInvariantError("UNKNOWN timestamp precedes durable send")
        connection.execute(
            """
            UPDATE broker_commands SET status='UNKNOWN',last_error=?,updated_at_ms=?
            WHERE command_id=?
            """,
            (reason, unknown_at_ms, command_id),
        )
        if command["action"] == "OPEN":
            connection.execute(
                """
                UPDATE risk_reservations SET state='UNKNOWN',updated_at_ms=?
                WHERE reservation_id=? AND state='SUBMITTED'
                """,
                (unknown_at_ms, command["reservation_id"]),
            )
            connection.execute(
                """
                UPDATE trades SET status='RECONCILIATION_REQUIRED',updated_at=?
                WHERE trade_id=?
                """,
                (unknown_at_ms / 1_000, command["trade_id"]),
            )
        event = TradeEventRecord(
            event_id=f"{command_id}:unknown",
            trade_id=command["trade_id"],
            event_type="BROKER_COMMAND_UNKNOWN",
            source="execution_service",
            correlation_id=command_id,
            occurred_at=unknown_at_ms / 1_000,
            payload={"command_id": command_id, "reason": reason},
        )
        self._insert_exact(
            connection, "trade_events", ("event_id",), _event_values(event)
        )
        return True

    def _enqueue_pma_for_close_tx(
        self,
        connection: sqlite3.Connection,
        *,
        trade: sqlite3.Row,
        event: TradeEventRecord,
        broker_closed_at_ms: int,
    ) -> bool:
        """Create exactly one restart-safe T+15m PMA job inside close commit."""

        if event.trade_id != trade["trade_id"]:
            raise LedgerInvariantError("PMA close event belongs to another trade")
        closed_ms = _timestamp_ms(broker_closed_at_ms, "broker_closed_at_ms")
        material = f"{trade['account_id']}\x1f{trade['trade_id']}\x1f{event.event_id}"
        job_id = "pma-" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]
        created_at_ms = _timestamp_ms(
            int(event.occurred_at * 1_000), "pma_created_at_ms"
        )
        values = {
            "job_id": job_id,
            "trade_id": trade["trade_id"],
            "close_event_id": event.event_id,
            "account_id": trade["account_id"],
            "symbol": trade["symbol"],
            "magic": trade["magic_number"],
            "source_config_hash": trade["config_hash"],
            "regime": trade["regime_at_entry"],
            "session": trade["session_at_entry"],
            "target_config_hash": None,
            "status": "PENDING",
            "due_at_ms": closed_ms + PMA_DEFAULT_DELAY_MS,
            "attempts": 0,
            "max_attempts": PMA_DEFAULT_MAX_ATTEMPTS,
            "locked_by": None,
            "locked_until_ms": None,
            "last_error": None,
            "eligibility_reason": None,
            "result_proposal_id": None,
            "created_at_ms": created_at_ms,
            "updated_at_ms": created_at_ms,
            "completed_at_ms": None,
            "dead_lettered_at_ms": None,
        }
        return self._insert_exact(
            connection,
            "pma_jobs",
            ("job_id",),
            values,
        )

    def _insert_outbox_many_tx(
        self,
        connection: sqlite3.Connection,
        event_id: str,
        records: Sequence[OutboxRecord],
    ) -> None:
        for record in records:
            if record.event_id != event_id:
                raise LedgerInvariantError(
                    "atomic outbox record must reference its committed trade event"
                )
            self._insert_exact(
                connection,
                "outbox",
                ("message_id",),
                _outbox_values(record),
            )

    def _insert_broker_deal_tx(
        self,
        connection: sqlite3.Connection,
        record: BrokerDealRecord,
    ) -> bool:
        _validate_deal(record)
        return self._insert_exact(
            connection,
            "broker_deals",
            ("account_id", "deal_id"),
            _deal_values(record),
        )

    def _upsert_owned_position_tx(
        self, connection: sqlite3.Connection, values: Mapping[str, Any]
    ) -> bool:
        key = (values["account_id"], values["broker_position_id"])
        existing = connection.execute(
            "SELECT * FROM owned_positions WHERE account_id=? AND broker_position_id=?",
            key,
        ).fetchone()
        if existing is None:
            columns = tuple(values)
            connection.execute(
                f"""
                INSERT INTO owned_positions ({','.join(columns)})
                VALUES ({','.join('?' for _ in columns)})
                """,
                tuple(values[column] for column in columns),
            )
            return True
        incoming_version = values["position_version"]
        if incoming_version < existing["position_version"]:
            raise LedgerConflictError("stale broker position version")
        if incoming_version == existing["position_version"]:
            differing = [key for key, value in values.items() if existing[key] != value]
            if differing:
                raise LedgerConflictError(
                    f"same position version has different content: {differing}"
                )
            return False
        immutable = (
            "account_id",
            "broker_position_id",
            "trade_id",
            "position_ticket",
            "broker_order_id",
            "symbol",
            "magic",
            "direction",
            "initial_volume",
            "entry_price",
            "original_sl",
            "tp1",
            "tp2",
            "tp3",
            "initial_risk_amount",
            "opened_at_ms",
        )
        changed = [name for name in immutable if existing[name] != values[name]]
        if changed:
            raise LedgerConflictError(
                f"broker position immutable identity changed: {changed}"
            )
        mutable = tuple(name for name in values if name not in {"account_id", "broker_position_id"})
        connection.execute(
            f"""
            UPDATE owned_positions SET {','.join(f'{name}=?' for name in mutable)}
            WHERE account_id=? AND broker_position_id=?
            """,
            (*(values[name] for name in mutable), *key),
        )
        return True

    def _insert_exact(
        self,
        connection: sqlite3.Connection,
        table: str,
        key_columns: tuple[str, ...],
        values: Mapping[str, Any],
        *,
        compare_columns: Sequence[str] | None = None,
    ) -> bool:
        table_sql = _exact_insert_table(table)

        if not key_columns:
            raise LedgerInvariantError("exact insertion requires key columns")

        missing_keys = [
            column for column in key_columns if column not in values
        ]
        if missing_keys:
            raise LedgerInvariantError(
                f"exact insertion is missing key columns: {missing_keys}"
            )

        quoted_keys = tuple(
            _sql_identifier(column, "key column")
            for column in key_columns
        )
        where = " AND ".join(
            f"{column}=?" for column in quoted_keys
        )
        key_values = tuple(values[column] for column in key_columns)

        # The table and key identifiers are strictly validated and quoted;
        # all record data remains bound through SQLite parameters.
        select_sql = (
            f"SELECT * FROM {table_sql} WHERE {where}"
        )  # nosec B608
        row = connection.execute(
            select_sql,
            key_values,
        ).fetchone()

        compared = tuple(compare_columns or values.keys())
        unknown_compared = [
            column for column in compared if column not in values
        ]
        if unknown_compared:
            raise LedgerInvariantError(
                "exact insertion comparison columns are unknown: "
                f"{unknown_compared}"
            )

        if row is not None:
            differing = [
                column
                for column in compared
                if row[column] != values[column]
            ]
            if differing:
                raise LedgerConflictError(
                    f"idempotency conflict in {table} "
                    f"for {key_values!r}: {differing}"
                )
            return False

        columns = tuple(values)
        if not columns:
            raise LedgerInvariantError(
                "exact insertion requires at least one column"
            )

        quoted_columns = ",".join(
            _sql_identifier(column, "insert column")
            for column in columns
        )
        placeholders = ",".join("?" for _ in columns)

        # Identifiers are allowlisted/validated above and values are bound.
        insert_sql = (
            f"INSERT INTO {table_sql} ({quoted_columns}) "
            f"VALUES ({placeholders})"
        )  # nosec B608

        try:
            connection.execute(
                insert_sql,
                tuple(values[column] for column in columns),
            )
        except sqlite3.IntegrityError as exc:
            raise LedgerConflictError(
                f"constraint conflict inserting {table} "
                f"for {key_values!r}: {exc}"
            ) from exc

        return True

    def _require_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise LedgerUnavailableError("trade ledger is not open")
        return self._connection

    def _harden_database_permissions(self) -> None:
        """Keep the database and SQLite sidecars private to the runtime user."""

        candidates = (
            self.database_path,
            Path(f"{self.database_path}-wal"),
            Path(f"{self.database_path}-shm"),
            Path(f"{self.database_path}-journal"),
        )
        try:
            for candidate in candidates:
                if candidate.exists():
                    os.chmod(candidate, 0o600)
        except OSError as exc:
            raise LedgerUnavailableError(
                f"cannot secure authoritative ledger files: {exc}"
            ) from exc

    @contextmanager
    def _read_connection(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            connection = self._require_connection()
            try:
                yield connection
            except LedgerError:
                raise
            except sqlite3.Error as exc:
                raise LedgerUnavailableError(f"ledger read failed: {exc}") from exc

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            connection = self._require_connection()
            try:
                connection.execute("BEGIN IMMEDIATE")
                yield connection
                connection.execute("COMMIT")
                self._harden_database_permissions()
            except LedgerError:
                _rollback(connection)
                raise
            except sqlite3.Error as exc:
                _rollback(connection)
                raise LedgerUnavailableError(f"ledger transaction failed: {exc}") from exc
            except Exception:
                _rollback(connection)
                raise


def _canonicalize_v4_decimal_backfill(connection: sqlite3.Connection) -> None:
    """Canonicalize additive TEXT shadows created from legacy REAL values."""

    deal_fields = ("volume", "price", "profit", "commission", "swap", "fee")
    for row in connection.execute(
        "SELECT account_id,deal_id,volume,price,profit,commission,swap,fee FROM broker_deals"
    ):
        values = {
            name: _decimal_text(
                row[name],
                name,
                positive=name in {"volume", "price"},
            )
            for name in deal_fields
        }
        connection.execute(
            """
            UPDATE broker_deals SET volume_decimal=?,price_decimal=?,profit_decimal=?,
                commission_decimal=?,swap_decimal=?,fee_decimal=?
            WHERE account_id=? AND deal_id=?
            """,
            (*(values[name] for name in deal_fields), row["account_id"], row["deal_id"]),
        )

    trade_fields = (
        "intended_entry",
        "intended_sl",
        "tp1",
        "tp2",
        "tp3",
        "requested_volume",
        "filled_volume",
        "remaining_volume",
        "initial_risk_currency",
        "filled_entry",
        "closed_exit",
        "net_realized_pnl",
        "actual_r",
    )
    for row in connection.execute(
        f"SELECT trade_id,{','.join(trade_fields)} FROM trades"
    ):
        values: dict[str, str | None] = {}
        for name in trade_fields:
            value = row[name]
            values[name] = (
                None
                if value is None
                else _decimal_text(
                    value,
                    name,
                    positive=name
                    in {
                        "intended_entry",
                        "intended_sl",
                        "tp1",
                        "tp2",
                        "tp3",
                        "requested_volume",
                        "filled_volume",
                        "filled_entry",
                        "closed_exit",
                    },
                    nonnegative=name
                    in {"remaining_volume", "initial_risk_currency"},
                )
            )
        connection.execute(
            f"""
            UPDATE trades SET {','.join(f'{name}_decimal=?' for name in trade_fields)}
            WHERE trade_id=?
            """,
            (*(values[name] for name in trade_fields), row["trade_id"]),
        )


def _canonicalize_v5_attribution_backfill(connection: sqlite3.Connection) -> None:
    fields = ("conviction", "learned_weight", "effective_weight")
    for row in connection.execute(
        """
        SELECT attribution_id,conviction,learned_weight,effective_weight
        FROM trade_attributions
        """
    ):
        values = {
            name: _decimal_text(
                row[name],
                name,
                nonnegative=True,
            )
            for name in fields
        }
        connection.execute(
            """
            UPDATE trade_attributions SET conviction_decimal=?,
                learned_weight_decimal=?,effective_weight_decimal=?
            WHERE attribution_id=?
            """,
            (*(values[name] for name in fields), row["attribution_id"]),
        )


def _trade_values(record: TradeRecord, now: float) -> dict[str, Any]:
    intended_entry = _decimal_text(record.intended_entry, "intended_entry", positive=True)
    intended_sl = _decimal_text(record.intended_sl, "intended_sl", positive=True)
    tp1 = _decimal_text(record.tp1, "tp1", positive=True)
    tp2 = _decimal_text(record.tp2, "tp2", positive=True)
    tp3 = _decimal_text(record.tp3, "tp3", positive=True)
    requested_volume = _decimal_text(
        record.requested_volume, "requested_volume", positive=True
    )
    initial_risk = _decimal_text(
        record.initial_risk_currency, "initial_risk_currency", nonnegative=True
    )
    return {
        "trade_id": _text(record.trade_id, "trade_id"),
        "request_id": _text(record.request_id, "request_id"),
        "account_id": _text(record.account_id, "account_id"),
        "symbol": _text(record.symbol, "symbol"),
        "magic_number": record.magic_number,
        "direction": record.direction,
        "status": record.status,
        "decision_at": record.decision_at,
        "regime_at_entry": record.regime_at_entry,
        "session_at_entry": record.session_at_entry,
        "intended_entry": float(Decimal(intended_entry)),
        "intended_sl": float(Decimal(intended_sl)),
        "tp1": float(Decimal(tp1)),
        "tp2": float(Decimal(tp2)),
        "tp3": float(Decimal(tp3)),
        "requested_volume": float(Decimal(requested_volume)),
        "filled_volume": None,
        "initial_risk_currency": float(Decimal(initial_risk)),
        "currency": _text(record.currency, "currency"),
        "broker_order_id": None,
        "broker_position_id": None,
        "filled_entry": None,
        "closed_exit": None,
        "submitted_at": None,
        "opened_at": None,
        "closed_at": None,
        "net_realized_pnl": None,
        "actual_r": None,
        "metadata_json": _json(record.metadata, "metadata"),
        "created_at": now,
        "updated_at": now,
        "remaining_volume": None,
        "intended_entry_decimal": intended_entry,
        "intended_sl_decimal": intended_sl,
        "tp1_decimal": tp1,
        "tp2_decimal": tp2,
        "tp3_decimal": tp3,
        "requested_volume_decimal": requested_volume,
        "filled_volume_decimal": None,
        "remaining_volume_decimal": None,
        "initial_risk_currency_decimal": initial_risk,
        "filled_entry_decimal": None,
        "closed_exit_decimal": None,
        "net_realized_pnl_decimal": None,
        "actual_r_decimal": None,
        "config_hash": _optional_text(record.config_hash, "config_hash"),
        "evidence_class": _text(record.evidence_class, "evidence_class").upper(),
        "learning_eligible": int(record.learning_eligible),
    }


def _attribution_values(record: TradeAttributionRecord) -> dict[str, Any]:
    conviction = _decimal_text(
        record.conviction, "conviction", nonnegative=True
    )
    learned_weight = _decimal_text(
        record.learned_weight, "learned_weight", nonnegative=True
    )
    effective_weight = _decimal_text(
        record.effective_weight, "effective_weight", nonnegative=True
    )
    return {
        "attribution_id": _text(record.attribution_id, "attribution_id"),
        "trade_id": _text(record.trade_id, "trade_id"),
        "agent_id": record.agent_id,
        "strategy_slug": _text(record.strategy_slug, "strategy_slug"),
        "direction": record.direction,
        "conviction": float(Decimal(conviction)),
        "learned_weight": float(Decimal(learned_weight)),
        "effective_weight": float(Decimal(effective_weight)),
        "signal_at": record.signal_at,
        "tags_json": _json(list(record.tags), "tags"),
        "metadata_json": _json(record.metadata, "metadata"),
        "conviction_decimal": conviction,
        "learned_weight_decimal": learned_weight,
        "effective_weight_decimal": effective_weight,
    }


def _sha256_text(value: Any, label: str) -> str:
    text = _text(value, label).lower()
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise LedgerInvariantError(f"{label} must be a 64-character SHA-256 hash")
    return text


def _learning_source_values(record: LearningSourceRecord) -> dict[str, Any]:
    attribution = tuple(record.attribution)
    broker_deals = tuple(record.broker_deals)
    if len(attribution) < 3 or not broker_deals:
        raise LedgerInvariantError(
            "learning source requires three attributions and broker deals"
        )
    if len({item.get("agent_id") for item in attribution}) != len(attribution):
        raise LedgerInvariantError("learning source attribution agents must be unique")
    if any(
        isinstance(item.get("agent_id"), bool)
        or not isinstance(item.get("agent_id"), int)
        or not 4 <= item["agent_id"] <= 13
        for item in attribution
    ):
        raise LedgerInvariantError("learning source attribution agent is outside 04-13")
    actual_r = _decimal_text(record.actual_r, "actual_r")
    opened = _timestamp_ms(record.broker_opened_at_ms, "broker_opened_at_ms")
    closed = _timestamp_ms(record.broker_closed_at_ms, "broker_closed_at_ms")
    if closed <= opened:
        raise LedgerInvariantError("learning source close must follow open")
    return {
        "source_id": _text(record.source_id, "source_id"),
        "trade_id": _text(record.trade_id, "trade_id"),
        "close_event_id": _text(record.close_event_id, "close_event_id"),
        "account_id": _text(record.account_id, "account_id"),
        "symbol": _text(record.symbol, "symbol"),
        "magic": _positive_int(record.magic, "magic"),
        "source_config_hash": _text(
            record.source_config_hash, "source_config_hash"
        ),
        "regime": _text(record.regime, "regime"),
        "session": _text(record.session, "session"),
        "actual_r_decimal": actual_r,
        "broker_opened_at_ms": opened,
        "broker_closed_at_ms": closed,
        "attribution_json": _json(attribution, "learning source attribution"),
        "broker_deals_json": _json(broker_deals, "learning source broker deals"),
        "content_hash": _sha256_text(record.content_hash, "content_hash"),
        "created_at_ms": _timestamp_ms(record.created_at_ms, "created_at_ms"),
    }


def _learning_proposal_values(record: LearningProposalRecord) -> dict[str, Any]:
    version = _positive_int(record.proposal_version, "proposal_version")
    parent = (
        None
        if record.parent_proposal_hash is None
        else _sha256_text(record.parent_proposal_hash, "parent_proposal_hash")
    )
    if (version == 1) != (parent is None):
        raise LedgerInvariantError("learning proposal version/parent lineage is invalid")
    return {
        "proposal_id": _text(record.proposal_id, "proposal_id"),
        "proposal_hash": _sha256_text(record.proposal_hash, "proposal_hash"),
        "proposal_version": version,
        "parent_proposal_hash": parent,
        "policy_config_hash": _sha256_text(
            record.policy_config_hash, "policy_config_hash"
        ),
        "account_id": _text(record.account_id, "account_id"),
        "symbol": _text(record.symbol, "symbol"),
        "magic": _positive_int(record.magic, "magic"),
        "source_config_hash": _text(
            record.source_config_hash, "source_config_hash"
        ),
        "target_config_hash": _text(
            record.target_config_hash, "target_config_hash"
        ),
        "as_of_broker_time_ms": _timestamp_ms(
            record.as_of_broker_time_ms, "as_of_broker_time_ms"
        ),
        "proposal_json": _json(record.proposal, "learning proposal"),
        "created_at_ms": _timestamp_ms(record.created_at_ms, "created_at_ms"),
    }


def _learning_snapshot_values(record: LearningSnapshotRecord) -> dict[str, Any]:
    return {
        "snapshot_id": _text(record.snapshot_id, "snapshot_id"),
        "proposal_id": _text(record.proposal_id, "proposal_id"),
        "proposal_hash": _sha256_text(record.proposal_hash, "proposal_hash"),
        "source_set_hash": _sha256_text(record.source_set_hash, "source_set_hash"),
        "context_stats_json": _json(record.context_stats, "context_stats"),
        "agent_summaries_json": _json(record.agent_summaries, "agent_summaries"),
        "bounds_json": _json(record.bounds, "learning bounds"),
        "content_hash": _sha256_text(record.content_hash, "content_hash"),
        "created_at_ms": _timestamp_ms(record.created_at_ms, "created_at_ms"),
    }


def _learning_approval_values(record: LearningApprovalRecord) -> dict[str, Any]:
    return {
        "approval_id": _text(record.approval_id, "approval_id"),
        "proposal_id": _text(record.proposal_id, "proposal_id"),
        "proposal_hash": _sha256_text(record.proposal_hash, "proposal_hash"),
        "approved_by": _text(record.approved_by, "approved_by"),
        "approved_at_ms": _timestamp_ms(record.approved_at_ms, "approved_at_ms"),
        "rationale": _text(record.rationale, "rationale"),
        "content_hash": _sha256_text(record.content_hash, "content_hash"),
    }


def _learning_activation_values(record: LearningActivationRecord) -> dict[str, Any]:
    weights = tuple(record.weights)
    agent_ids = tuple(item.get("agent_id") for item in weights)
    if agent_ids != tuple(range(4, 14)):
        raise LedgerInvariantError(
            "learning activation weights must contain Agents 04-13 in order"
        )
    for item in weights:
        weight = Decimal(_decimal_text(item.get("weight"), "activation weight", positive=True))
        if not Decimal("0.40") <= weight <= Decimal("1.40"):
            raise LedgerInvariantError("learning activation weight exceeds hard bounds")
    return {
        "activation_id": _text(record.activation_id, "activation_id"),
        "approval_id": _text(record.approval_id, "approval_id"),
        "proposal_id": _text(record.proposal_id, "proposal_id"),
        "proposal_hash": _sha256_text(record.proposal_hash, "proposal_hash"),
        "account_id": _text(record.account_id, "account_id"),
        "symbol": _text(record.symbol, "symbol"),
        "magic": _positive_int(record.magic, "magic"),
        "target_config_hash": _text(
            record.target_config_hash, "target_config_hash"
        ),
        "regime": _text(record.regime, "regime"),
        "session": _text(record.session, "session"),
        "weights_json": _json(weights, "activation weights"),
        "activated_by": _text(record.activated_by, "activated_by"),
        "activated_at_ms": _timestamp_ms(record.activated_at_ms, "activated_at_ms"),
        "content_hash": _sha256_text(record.content_hash, "content_hash"),
    }


def _deal_values(record: BrokerDealRecord) -> dict[str, Any]:
    volume = _decimal_text(record.volume, "volume", positive=True)
    price = _decimal_text(record.price, "price", positive=True)
    profit = _decimal_text(record.profit, "profit")
    commission = _decimal_text(record.commission, "commission")
    swap = _decimal_text(record.swap, "swap")
    fee = _decimal_text(record.fee, "fee")
    return {
        "account_id": _text(record.account_id, "account_id"),
        "deal_id": _text(record.deal_id, "deal_id"),
        "trade_id": _optional_text(record.trade_id, "trade_id"),
        "position_id": _text(record.position_id, "position_id"),
        "order_id": _optional_text(record.order_id, "order_id"),
        "deal_type": _text(record.deal_type, "deal_type"),
        # REAL columns remain only for additive compatibility with historical
        # databases.  Every authoritative comparison/economic calculation uses
        # the canonical decimal TEXT columns below.
        "volume": float(Decimal(volume)),
        "price": float(Decimal(price)),
        "profit": float(Decimal(profit)),
        "commission": float(Decimal(commission)),
        "swap": float(Decimal(swap)),
        "fee": float(Decimal(fee)),
        "occurred_at": _finite(record.occurred_at, "occurred_at"),
        "payload_json": _json(record.payload, "payload"),
        "command_id": _optional_text(record.command_id, "command_id"),
        "volume_decimal": volume,
        "price_decimal": price,
        "profit_decimal": profit,
        "commission_decimal": commission,
        "swap_decimal": swap,
        "fee_decimal": fee,
    }


def _daily_risk_values(record: DailyRiskStateRecord) -> dict[str, Any]:
    if not isinstance(record.baseline_verified, bool) or not isinstance(record.halted, bool):
        raise LedgerInvariantError("daily risk flags must be boolean")
    start = _decimal_text(record.day_start_equity, "day_start_equity", positive=True)
    current = _decimal_text(record.current_equity, "current_equity", nonnegative=True)
    if record.current_balance is None:
        raise LedgerInvariantError(
            "daily risk initialization requires authoritative broker balance"
        )
    balance = _decimal_text(
        record.current_balance, "current_balance", nonnegative=True
    )
    return {
        "account_id": _text(record.account_id, "account_id"),
        "risk_day": _risk_day(record.risk_day),
        "day_start_equity": start,
        "current_equity": current,
        "max_equity": _decimal_text(max(Decimal(start), Decimal(current)), "max_equity"),
        "baseline_verified": int(record.baseline_verified),
        "baseline_source": _text(record.baseline_source, "baseline_source"),
        "halted": int(record.halted),
        "halt_reason": _optional_text(record.halt_reason, "halt_reason"),
        "realised_pnl": _decimal_text(record.realised_pnl, "realised_pnl"),
        "consecutive_losses": _nonnegative_int(
            record.consecutive_losses, "consecutive_losses"
        ),
        "created_at_ms": _timestamp_ms(record.updated_at_ms, "updated_at_ms"),
        "updated_at_ms": _timestamp_ms(record.updated_at_ms, "updated_at_ms"),
        "current_balance": balance,
    }


def _reservation_values(record: RiskReservationRecord) -> dict[str, Any]:
    return {
        "reservation_id": _text(record.reservation_id, "reservation_id"),
        "trade_id": _text(record.trade_id, "trade_id"),
        "account_id": _text(record.account_id, "account_id"),
        "risk_day": _risk_day(record.risk_day),
        "direction": record.direction,
        "risk_amount": _decimal_text(record.risk_amount, "risk_amount", positive=True),
        "volume": _decimal_text(record.volume, "volume", positive=True),
        "state": record.state,
        "created_at_ms": record.created_at_ms,
        "expires_at_ms": record.expires_at_ms,
        "submitted_at_ms": None,
        "consumed_at_ms": None,
        "released_at_ms": None,
        "release_reason": None,
        "updated_at_ms": record.created_at_ms,
    }


def _command_values(record: BrokerCommandRecord) -> dict[str, Any]:
    return {
        "command_id": _text(record.command_id, "command_id"),
        "trade_id": _text(record.trade_id, "trade_id"),
        "reservation_id": _text(record.reservation_id, "reservation_id"),
        "action": _text(record.action, "action").upper(),
        "status": _text(record.status, "status").upper(),
        "expected_effect_json": _json(record.expected_effect, "expected_effect"),
        "connection_epoch": _text(record.connection_epoch, "connection_epoch"),
        "sequence": record.sequence,
        "position_id": _optional_text(record.position_id, "position_id"),
        "expected_position_version": record.expected_position_version,
        "payload_json": _json(record.payload, "payload"),
        "created_at_ms": record.created_at_ms,
        "expires_at_ms": record.expires_at_ms,
        "sent_at_ms": None,
        "acknowledged_at_ms": None,
        "last_error": None,
        "updated_at_ms": record.created_at_ms,
    }


def _owned_position_values(record: OwnedPositionRecord) -> dict[str, Any]:
    return {
        "account_id": _text(record.account_id, "account_id"),
        "broker_position_id": _text(
            record.broker_position_id, "broker_position_id"
        ),
        "trade_id": _optional_text(record.trade_id, "trade_id"),
        "position_ticket": _text(record.position_ticket, "position_ticket"),
        "broker_order_id": _optional_text(record.broker_order_id, "broker_order_id"),
        "symbol": _text(record.symbol, "symbol"),
        "magic": _positive_int(record.magic, "magic"),
        "direction": record.direction,
        "initial_volume": _decimal_text(
            record.initial_volume, "initial_volume", positive=True
        ),
        "remaining_volume": _decimal_text(
            record.remaining_volume, "remaining_volume", nonnegative=True
        ),
        "entry_price": _decimal_text(record.entry_price, "entry_price", positive=True),
        "original_sl": _decimal_text(record.original_sl, "original_sl", positive=True),
        "current_sl": _decimal_text(record.current_sl, "current_sl", positive=True),
        "tp1": _decimal_text(record.tp1, "tp1", positive=True),
        "tp2": _decimal_text(record.tp2, "tp2", positive=True),
        "tp3": _decimal_text(record.tp3, "tp3", positive=True),
        "initial_risk_amount": _decimal_text(
            record.initial_risk_amount, "initial_risk_amount", nonnegative=True
        ),
        "realised_pnl": _decimal_text(record.realised_pnl, "realised_pnl"),
        "lifecycle_stage": _text(record.lifecycle_stage, "lifecycle_stage"),
        "position_version": _nonnegative_int(
            record.position_version, "position_version"
        ),
        "opened_at_ms": _timestamp_ms(record.opened_at_ms, "opened_at_ms"),
        "pending_action": _optional_text(record.pending_action, "pending_action"),
        "pending_milestone": _optional_text(
            record.pending_milestone, "pending_milestone"
        ),
        "pending_command_id": _optional_text(
            record.pending_command_id, "pending_command_id"
        ),
        "pending_close_volume": _decimal_text(
            record.pending_close_volume, "pending_close_volume", nonnegative=True
        ),
        "pending_filled_volume": _decimal_text(
            record.pending_filled_volume, "pending_filled_volume", nonnegative=True
        ),
        "updated_at_ms": _timestamp_ms(record.updated_at_ms, "updated_at_ms"),
    }


def _quarantined_position_values(
    record: OwnedPositionRecord,
    *,
    run_id: str,
    snapshot_id: str,
    reason: str,
    seen_at_ms: int,
) -> dict[str, Any]:
    def optional_positive(value: Any, label: str) -> str | None:
        text = _decimal_text(value, label, nonnegative=True)
        return None if Decimal(text) == 0 else text

    return {
        "account_id": _text(record.account_id, "account_id"),
        "broker_position_id": _text(
            record.broker_position_id, "broker_position_id"
        ),
        "position_ticket": _text(record.position_ticket, "position_ticket"),
        "broker_order_id": _optional_text(record.broker_order_id, "broker_order_id"),
        "reported_trade_id": _optional_text(record.trade_id, "reported_trade_id"),
        "symbol": _text(record.symbol, "symbol"),
        "magic": _positive_int(record.magic, "magic"),
        "direction": record.direction,
        "initial_volume_decimal": _decimal_text(
            record.initial_volume, "initial_volume", positive=True
        ),
        "remaining_volume_decimal": _decimal_text(
            record.remaining_volume, "remaining_volume", nonnegative=True
        ),
        "entry_price_decimal": _decimal_text(
            record.entry_price, "entry_price", positive=True
        ),
        "broker_sl_decimal": optional_positive(record.current_sl, "current_sl"),
        "broker_tp_decimal": optional_positive(record.tp3, "broker_tp"),
        "position_version": _nonnegative_int(
            record.position_version, "position_version"
        ),
        "opened_at_ms": _timestamp_ms(record.opened_at_ms, "opened_at_ms"),
        "first_seen_run_id": _text(run_id, "run_id"),
        "last_seen_run_id": _text(run_id, "run_id"),
        "snapshot_id": _text(snapshot_id, "snapshot_id"),
        "reason": _text(reason, "quarantine reason"),
        "payload_json": _json(
            {
                "original_sl": record.original_sl,
                "tp1": record.tp1,
                "tp2": record.tp2,
                "tp3": record.tp3,
                "initial_risk_amount": record.initial_risk_amount,
                "lifecycle_stage": record.lifecycle_stage,
                "updated_at_ms": record.updated_at_ms,
            },
            "quarantined broker exposure",
        ),
        "first_seen_at_ms": _timestamp_ms(seen_at_ms, "seen_at_ms"),
        "last_seen_at_ms": _timestamp_ms(seen_at_ms, "seen_at_ms"),
        "active": 1,
    }


def _pending_order_values(record: PendingOrderRecord) -> dict[str, Any]:
    return {
        "account_id": _text(record.account_id, "account_id"),
        "broker_order_id": _text(record.broker_order_id, "broker_order_id"),
        "trade_id": _optional_text(record.trade_id, "trade_id"),
        "command_id": _optional_text(record.command_id, "command_id"),
        "symbol": _text(record.symbol, "symbol"),
        "magic": _positive_int(record.magic, "magic"),
        "order_type": _text(record.order_type, "order_type"),
        "volume": _decimal_text(record.volume, "volume", positive=True),
        "price": _decimal_text(record.price, "price", positive=True),
        "sl": None
        if record.sl is None
        else _decimal_text(record.sl, "sl", positive=True),
        "tp": None
        if record.tp is None
        else _decimal_text(record.tp, "tp", positive=True),
        "state": _text(record.state, "state"),
        "created_at_ms": _timestamp_ms(record.created_at_ms, "created_at_ms"),
        "updated_at_ms": _timestamp_ms(record.updated_at_ms, "updated_at_ms"),
        "payload_json": _json(record.payload, "payload"),
    }


def _snapshot_values(record: BrokerSnapshotRecord) -> dict[str, Any]:
    if record.trade_mode not in {"DEMO", "REAL", "CONTEST"}:
        raise LedgerInvariantError("snapshot trade_mode is invalid")
    return {
        "snapshot_id": _text(record.snapshot_id, "snapshot_id"),
        "connection_epoch": _text(record.connection_epoch, "connection_epoch"),
        "account_id": _text(record.account_id, "account_id"),
        "broker_server": _text(record.broker_server, "broker_server"),
        "trade_mode": record.trade_mode,
        "symbol": _text(record.symbol, "symbol"),
        "magic": _positive_int(record.magic, "magic"),
        "captured_at_ms": _timestamp_ms(record.captured_at_ms, "captured_at_ms"),
        "snapshot_hash": _text(record.snapshot_hash, "snapshot_hash"),
        "payload_json": _json(record.payload, "payload"),
    }


def _reconciliation_run_values(record: ReconciliationRunRecord) -> dict[str, Any]:
    if record.broker_position_count < 0 or record.ledger_position_count < 0:
        raise LedgerInvariantError("reconciliation counts cannot be negative")
    return {
        "run_id": _text(record.run_id, "run_id"),
        "connection_epoch": _text(record.connection_epoch, "connection_epoch"),
        "account_id": _text(record.account_id, "account_id"),
        "status": "STARTED",
        "started_at": _finite(record.started_at, "started_at"),
        "finished_at": None,
        "broker_position_count": record.broker_position_count,
        "ledger_position_count": record.ledger_position_count,
        "mismatch_count": None,
        "snapshot_hash": _text(record.snapshot_hash, "snapshot_hash"),
        "detail_json": _json(record.detail, "detail"),
    }


def _reconciliation_item_values(record: ReconciliationItemRecord) -> dict[str, Any]:
    return {
        "run_id": _text(record.run_id, "run_id"),
        "item_id": _text(record.item_id, "item_id"),
        "discrepancy_type": _text(record.discrepancy_type, "discrepancy_type"),
        "broker_position_id": _optional_text(
            record.broker_position_id, "broker_position_id"
        ),
        "trade_id": _optional_text(record.trade_id, "trade_id"),
        "action": _text(record.action, "action"),
        "resolved": int(record.resolved),
        "detail_json": _json(record.detail, "detail"),
    }


def _outbox_values(record: OutboxRecord) -> dict[str, Any]:
    created = _finite(record.created_at, "created_at")
    available = created if record.available_at is None else _finite(
        record.available_at, "available_at"
    )
    return {
        "message_id": _text(record.message_id, "message_id"),
        "idempotency_key": _text(record.idempotency_key, "idempotency_key"),
        "event_id": _optional_text(record.event_id, "event_id"),
        "destination": _text(record.destination, "destination"),
        "topic": _text(record.topic, "topic"),
        "payload_json": _json(record.payload, "payload"),
        "status": "PENDING",
        "attempts": 0,
        "available_at": available,
        "locked_by": None,
        "locked_until": None,
        "last_error": None,
        "created_at": created,
        "delivered_at": None,
        "updated_at": created,
    }


def _event_values(record: TradeEventRecord) -> dict[str, Any]:
    return {
        "event_id": _text(record.event_id, "event_id"),
        "trade_id": _optional_text(record.trade_id, "trade_id"),
        "event_type": _text(record.event_type, "event_type"),
        "source": _text(record.source, "source"),
        "correlation_id": _optional_text(record.correlation_id, "correlation_id"),
        "occurred_at": _finite(record.occurred_at, "occurred_at"),
        "broker_timestamp": _optional_finite(
            record.broker_timestamp, "broker_timestamp"
        ),
        "payload_json": _json(record.payload, "payload"),
    }


def _validate_trade(record: TradeRecord) -> None:
    if record.direction not in {"BUY", "SELL"}:
        raise LedgerInvariantError("trade direction must be BUY or SELL")
    if record.status not in TRADE_STATUSES:
        raise LedgerInvariantError(f"unknown trade status: {record.status!r}")
    if not isinstance(record.magic_number, int) or isinstance(record.magic_number, bool):
        raise LedgerInvariantError("magic_number must be an integer")
    if record.magic_number <= 0:
        raise LedgerInvariantError("magic_number must be positive")
    _finite(record.decision_at, "decision_at")
    for label, value in (
        ("intended_entry", record.intended_entry),
        ("intended_sl", record.intended_sl),
        ("tp1", record.tp1),
        ("tp2", record.tp2),
        ("tp3", record.tp3),
        ("requested_volume", record.requested_volume),
    ):
        _decimal_text(value, label, positive=True)
    _decimal_text(
        record.initial_risk_currency, "initial_risk_currency", nonnegative=True
    )
    if not isinstance(record.learning_eligible, bool):
        raise LedgerInvariantError("learning_eligible must be boolean")
    evidence = _text(record.evidence_class, "evidence_class").upper()
    if evidence not in {
        "CANONICAL_RUNTIME",
        "UNCLASSIFIED",
        "QUARANTINED",
        "RECOVERED",
        "SYNTHETIC",
    }:
        raise LedgerInvariantError("unsupported trade evidence_class")
    if record.learning_eligible and (
        evidence != "CANONICAL_RUNTIME" or record.config_hash is None
    ):
        raise LedgerInvariantError(
            "learning-eligible trade requires canonical runtime provenance and config"
        )
    if record.learning_eligible and (
        record.regime_at_entry not in CANONICAL_LEARNING_REGIMES
        or record.session_at_entry not in CANONICAL_MARKET_SESSIONS
    ):
        raise LedgerInvariantError(
            "learning-eligible trade requires canonical regime and session context"
        )
    _optional_text(record.config_hash, "config_hash")


def _validate_attribution(record: TradeAttributionRecord) -> None:
    if not 4 <= record.agent_id <= 13:
        raise LedgerInvariantError("trade attribution agent_id must be 04 through 13")
    if record.direction not in {"BUY", "SELL"}:
        raise LedgerInvariantError("attribution direction must be BUY or SELL")
    conviction = Decimal(
        _decimal_text(record.conviction, "conviction", nonnegative=True)
    )
    if conviction > 100:
        raise LedgerInvariantError("conviction must be between 0 and 100")
    _decimal_text(record.learned_weight, "learned_weight", nonnegative=True)
    _decimal_text(record.effective_weight, "effective_weight", nonnegative=True)
    _finite(record.signal_at, "signal_at")
    if not all(isinstance(tag, str) and tag for tag in record.tags):
        raise LedgerInvariantError("attribution tags must be non-empty strings")


def _validate_atomic_deal_binding(
    record: BrokerDealRecord,
    event: TradeEventRecord,
    *,
    account_id: str,
    position_id: str,
    command_id: str | None,
    allowed_deal_types: set[str],
) -> None:
    """Prove that one deal and one event describe the same broker transaction."""

    _validate_deal(record)
    payload = event.payload
    if not isinstance(payload, Mapping):
        raise LedgerInvariantError("broker transaction event payload must be a mapping")
    if (
        record.account_id != account_id
        or record.trade_id != event.trade_id
        or record.position_id != position_id
        or record.command_id != command_id
        or _text(record.deal_type, "deal_type").upper() not in allowed_deal_types
        or payload.get("broker_position_id") != position_id
        or payload.get("deal_id") != record.deal_id
        or payload.get("order_id") != record.order_id
    ):
        raise LedgerInvariantError("atomic broker deal/event identity differs")
    if event.correlation_id != command_id:
        raise LedgerInvariantError("atomic broker deal/event command correlation differs")
    expected = {
        "fill_volume": record.volume,
        "fill_price": record.price,
        "profit": record.profit,
        "commission": record.commission,
        "swap": record.swap,
        "fee": record.fee,
    }
    try:
        if any(
            Decimal(str(payload.get(name))) != Decimal(str(value))
            for name, value in expected.items()
        ):
            raise LedgerInvariantError("atomic broker deal/event economics differ")
        broker_timestamp = event.broker_timestamp
        if broker_timestamp is None or Decimal(str(broker_timestamp)) != Decimal(
            str(record.occurred_at)
        ):
            raise LedgerInvariantError("atomic broker deal/event time differs")
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise LedgerInvariantError(
            "atomic broker deal/event economics are not canonical decimals"
        ) from exc


def _validate_deal(record: BrokerDealRecord) -> None:
    _decimal_text(record.volume, "volume", positive=True)
    _decimal_text(record.price, "price", positive=True)
    for label, value in (
        ("profit", record.profit),
        ("commission", record.commission),
        ("swap", record.swap),
        ("fee", record.fee),
    ):
        _decimal_text(value, label)
    _finite(record.occurred_at, "occurred_at")
    _optional_text(record.command_id, "command_id")


def _is_closing_deal_type(value: object) -> bool:
    """Return whether a broker deal contributes to canonical realised R."""

    deal_type = str(value).strip().upper()
    return deal_type in {
        "OUT",
        "OUT_BY",
        "CLOSE",
        "EXIT",
        "POSITION_CLOSED",
        "PARTIAL_CLOSE",
        "ENTRY_OUT",
        "ENTRY_OUT_BY",
    } or deal_type.endswith("_OUT")


def _validate_risk(record: RiskDecisionRecord) -> None:
    if not isinstance(record.approved, bool):
        raise LedgerInvariantError("approved must be boolean")
    _finite(record.decided_at, "decided_at")
    _nonnegative(record.equity, "equity")
    _nonnegative(record.balance, "balance")
    _nonnegative(record.risk_pct, "risk_pct")
    _nonnegative(record.risk_currency, "risk_currency")
    _nonnegative(record.requested_volume, "requested_volume")
    _nonnegative(record.approved_volume, "approved_volume")
    if record.approved and record.approved_volume <= 0:
        raise LedgerInvariantError("approved risk decision requires positive approved_volume")
    if not record.approved and record.approved_volume != 0:
        raise LedgerInvariantError("rejected risk decision must have zero approved_volume")


def _validate_reservation(record: RiskReservationRecord) -> None:
    for name in ("reservation_id", "trade_id", "account_id"):
        _text(getattr(record, name), name)
    _risk_day(record.risk_day)
    if record.direction not in {"BUY", "SELL"}:
        raise LedgerInvariantError("reservation direction must be BUY or SELL")
    if record.state not in RESERVATION_STATES:
        raise LedgerInvariantError(f"unknown reservation state: {record.state!r}")
    _decimal_text(record.risk_amount, "risk_amount", positive=True)
    _decimal_text(record.volume, "volume", positive=True)
    created = _timestamp_ms(record.created_at_ms, "created_at_ms")
    expires = _timestamp_ms(record.expires_at_ms, "expires_at_ms")
    if expires <= created:
        raise LedgerInvariantError("reservation expiry must follow creation")


def _validate_command(record: BrokerCommandRecord) -> None:
    for name in ("command_id", "trade_id", "reservation_id", "connection_epoch"):
        _text(getattr(record, name), name)
    if record.status not in COMMAND_STATES:
        raise LedgerInvariantError(f"unknown broker command status: {record.status!r}")
    if record.action not in {
        "OPEN",
        "MODIFY_SL",
        "PARTIAL_CLOSE",
        "CLOSE",
        "CLOSE_ALL",
        "REQUEST_SNAPSHOT",
    }:
        raise LedgerInvariantError(f"unknown broker command action: {record.action!r}")
    if not isinstance(record.expected_effect, Mapping) or not record.expected_effect:
        raise LedgerInvariantError("broker command expected_effect must be non-empty")
    _json(record.expected_effect, "expected_effect")
    _json(record.payload, "payload")
    _nonnegative_int(record.sequence, "sequence")
    created = _timestamp_ms(record.created_at_ms, "created_at_ms")
    expires = _timestamp_ms(record.expires_at_ms, "expires_at_ms")
    if expires <= created:
        raise LedgerInvariantError("command expiry must follow creation")
    if record.expected_position_version is not None:
        _nonnegative_int(
            record.expected_position_version, "expected_position_version"
        )


def _validate_lifecycle_expected_effect(record: BrokerCommandRecord) -> None:
    effect = record.expected_effect
    required = {
        "MODIFY_SL": {
            "action",
            "broker_position_id",
            "expected_position_version",
            "previous_sl",
            "target_sl",
        },
        "PARTIAL_CLOSE": {
            "action",
            "broker_position_id",
            "expected_position_version",
            "previous_remaining_volume",
            "target_remaining_volume",
            "target_close_volume",
        },
        "CLOSE": {
            "action",
            "broker_position_id",
            "expected_position_version",
            "previous_remaining_volume",
            "target_remaining_volume",
        },
    }.get(record.action)
    if required is None or set(effect) != required:
        raise LedgerInvariantError("lifecycle expected_effect schema is not exact")
    if effect["action"] != record.action:
        raise LedgerInvariantError("lifecycle expected_effect action mismatch")
    if effect["broker_position_id"] != record.position_id:
        raise LedgerInvariantError("lifecycle expected_effect position mismatch")
    if effect["expected_position_version"] != record.expected_position_version:
        raise LedgerInvariantError("lifecycle expected_effect version mismatch")
    if record.action == "MODIFY_SL":
        previous = Decimal(
            _decimal_text(effect["previous_sl"], "previous_sl", positive=True)
        )
        target = Decimal(_decimal_text(effect["target_sl"], "target_sl", positive=True))
        if previous == target:
            raise LedgerInvariantError("MODIFY_SL target must differ from previous SL")
    else:
        previous = Decimal(
            _decimal_text(
                effect["previous_remaining_volume"],
                "previous_remaining_volume",
                positive=True,
            )
        )
        target = Decimal(
            _decimal_text(
                effect["target_remaining_volume"],
                "target_remaining_volume",
                nonnegative=True,
            )
        )
        if record.action == "CLOSE" and target != 0:
            raise LedgerInvariantError("CLOSE expected remaining volume must be zero")
        if record.action == "PARTIAL_CLOSE":
            close = Decimal(
                _decimal_text(
                    effect["target_close_volume"],
                    "target_close_volume",
                    positive=True,
                )
            )
            if not 0 < target < previous or close != previous - target:
                raise LedgerInvariantError("PARTIAL_CLOSE expected volumes are inconsistent")


def _validate_position_command_effect(
    command: sqlite3.Row,
    *,
    existing_position: sqlite3.Row,
    incoming: OwnedPositionRecord,
) -> str:
    """Validate one lifecycle event against the pinned durable effect.

    Returns ``COMPLETE``, ``PROGRESS`` or ``OVERFILL``.  An overfill is broker
    truth outside the requested partial effect: it is persisted but forces the
    trade into a non-learning quarantine.
    """

    try:
        effect = json.loads(command["expected_effect_json"])
    except (TypeError, json.JSONDecodeError) as exc:
        raise LedgerInvariantError("lifecycle command expected effect is corrupt") from exc
    expected_version = command["expected_position_version"]
    if (
        not isinstance(effect, Mapping)
        or expected_version is None
        or incoming.broker_position_id != command["position_id"]
        or effect.get("broker_position_id") != command["position_id"]
        or effect.get("expected_position_version") != expected_version
    ):
        raise LedgerInvariantError("lifecycle command effect identity is inconsistent")
    if command["status"] != "APPLIED" and existing_position["pending_command_id"] != command[
        "command_id"
    ]:
        raise LedgerConflictError("lifecycle event has no matching persisted action lock")
    if incoming.pending_command_id not in {None, command["command_id"]}:
        raise LedgerConflictError("lifecycle event carries another pending command")

    incoming_version = _nonnegative_int(
        incoming.position_version, "position_version"
    )
    action = command["action"]
    if action == "MODIFY_SL":
        previous = Decimal(
            _decimal_text(effect.get("previous_sl"), "previous_sl", positive=True)
        )
        target = Decimal(
            _decimal_text(effect.get("target_sl"), "target_sl", positive=True)
        )
        observed = Decimal(
            _decimal_text(incoming.current_sl, "current_sl", positive=True)
        )
        if incoming.pending_command_id is not None:
            if incoming.pending_action != "MODIFY_SL" or observed != previous:
                raise LedgerInvariantError(
                    "in-progress MODIFY_SL event differs from the pinned pre-state"
                )
            return "PROGRESS"
        if observed != target or incoming_version <= expected_version:
            raise LedgerInvariantError(
                "MODIFY_SL completion does not prove the exact pinned effect"
            )
        return "COMPLETE"

    if action not in {"PARTIAL_CLOSE", "CLOSE"}:
        raise LedgerInvariantError("position update targets an unsupported command action")
    previous = Decimal(
        _decimal_text(
            effect.get("previous_remaining_volume"),
            "previous_remaining_volume",
            positive=True,
        )
    )
    target = Decimal(
        _decimal_text(
            effect.get("target_remaining_volume"),
            "target_remaining_volume",
            nonnegative=True,
        )
    )
    observed = Decimal(
        _decimal_text(
            incoming.remaining_volume, "remaining_volume", nonnegative=True
        )
    )
    if not 0 <= observed <= previous:
        raise LedgerInvariantError("close fill lies outside the pinned volume interval")
    if observed < previous and incoming_version <= expected_version:
        raise LedgerInvariantError("close fill requires a newer broker position version")
    filled = previous - observed
    if action == "PARTIAL_CLOSE" and observed < target:
        if (
            incoming.pending_command_id is not None
            or incoming.lifecycle_stage != "QUARANTINED"
            or incoming_version <= expected_version
        ):
            raise LedgerInvariantError(
                "partial-close overfill must clear its lock and quarantine the position"
            )
        return "OVERFILL"
    if incoming.pending_command_id is not None:
        if incoming.pending_action != action:
            raise LedgerInvariantError("close progress action differs from command")
        planned = Decimal(
            _decimal_text(
                incoming.pending_close_volume,
                "pending_close_volume",
                positive=True,
            )
        )
        accumulated = Decimal(
            _decimal_text(
                incoming.pending_filled_volume,
                "pending_filled_volume",
                nonnegative=True,
            )
        )
        expected_close = previous - target
        if planned != expected_close or accumulated != filled or observed == target:
            raise LedgerInvariantError(
                "close progress does not match the exact cumulative fill state"
            )
        return "PROGRESS"
    if action == "CLOSE":
        raise LedgerInvariantError(
            "final CLOSE must be applied by the recorded-deal close transaction"
        )
    target_close = Decimal(
        _decimal_text(
            effect.get("target_close_volume"),
            "target_close_volume",
            positive=True,
        )
    )
    if (
        observed != target
        or target_close != previous - target
        or incoming_version <= expected_version
    ):
        raise LedgerInvariantError(
            "PARTIAL_CLOSE completion does not prove the exact pinned effect"
        )
    return "COMPLETE"


def _validate_owned_position(record: OwnedPositionRecord) -> None:
    for name in ("account_id", "broker_position_id", "position_ticket", "symbol"):
        _text(getattr(record, name), name)
    if record.direction not in {"BUY", "SELL"}:
        raise LedgerInvariantError("position direction must be BUY or SELL")
    initial = Decimal(
        _decimal_text(record.initial_volume, "initial_volume", positive=True)
    )
    remaining = Decimal(
        _decimal_text(record.remaining_volume, "remaining_volume", nonnegative=True)
    )
    if remaining > initial:
        raise LedgerInvariantError("remaining volume cannot exceed initial volume")
    for name in ("entry_price", "original_sl", "current_sl", "tp1", "tp2", "tp3"):
        _decimal_text(getattr(record, name), name, positive=True)
    _decimal_text(
        record.initial_risk_amount, "initial_risk_amount", nonnegative=True
    )
    _decimal_text(record.realised_pnl, "realised_pnl")
    _positive_int(record.magic, "magic")
    _nonnegative_int(record.position_version, "position_version")
    _timestamp_ms(record.opened_at_ms, "opened_at_ms")
    _timestamp_ms(record.updated_at_ms, "updated_at_ms")
    if (record.lifecycle_stage == "CLOSED") != (remaining == 0):
        raise LedgerInvariantError("closed lifecycle and remaining volume must agree")
    has_pending = record.pending_action is not None
    if has_pending != (record.pending_command_id is not None):
        raise LedgerInvariantError("pending position action requires a command ID")
    if has_pending != (record.pending_milestone is not None):
        raise LedgerInvariantError(
            "pending lifecycle action requires exactly one milestone"
        )
    if record.pending_action == "VIRTUAL_MILESTONE":
        raise LedgerInvariantError("virtual milestones cannot be broker-pending actions")
    pending_close = Decimal(
        _decimal_text(
            record.pending_close_volume, "pending_close_volume", nonnegative=True
        )
    )
    pending_filled = Decimal(
        _decimal_text(
            record.pending_filled_volume, "pending_filled_volume", nonnegative=True
        )
    )
    if pending_filled > pending_close:
        raise LedgerInvariantError(
            "pending filled volume cannot exceed pending close volume"
        )
    volume_action = record.pending_action in {"PARTIAL_CLOSE", "CLOSE"}
    if volume_action != (pending_close > 0):
        raise LedgerInvariantError(
            "pending close actions require explicit pending_close_volume"
        )
    if not volume_action and pending_filled != 0:
        raise LedgerInvariantError(
            "only close actions may accumulate pending_filled_volume"
        )


def _validate_quarantined_position(record: OwnedPositionRecord) -> None:
    """Validate raw broker exposure without inventing canonical entry geometry."""

    for name in ("account_id", "broker_position_id", "position_ticket", "symbol"):
        _text(getattr(record, name), name)
    if record.direction not in {"BUY", "SELL"}:
        raise LedgerInvariantError("quarantined position direction must be BUY or SELL")
    initial = Decimal(
        _decimal_text(record.initial_volume, "initial_volume", positive=True)
    )
    remaining = Decimal(
        _decimal_text(record.remaining_volume, "remaining_volume", nonnegative=True)
    )
    if remaining > initial:
        raise LedgerInvariantError("quarantined remaining volume exceeds initial volume")
    _decimal_text(record.entry_price, "entry_price", positive=True)
    for name in ("original_sl", "current_sl", "tp1", "tp2", "tp3"):
        _decimal_text(getattr(record, name), name, nonnegative=True)
    _decimal_text(
        record.initial_risk_amount, "initial_risk_amount", nonnegative=True
    )
    _positive_int(record.magic, "magic")
    _nonnegative_int(record.position_version, "position_version")
    _timestamp_ms(record.opened_at_ms, "opened_at_ms")
    _timestamp_ms(record.updated_at_ms, "updated_at_ms")
    if record.pending_action is not None or record.pending_command_id is not None:
        raise LedgerInvariantError("quarantined exposure cannot carry lifecycle ownership")


def _validate_pending_order(record: PendingOrderRecord) -> None:
    for name in ("account_id", "broker_order_id", "symbol", "order_type", "state"):
        _text(getattr(record, name), name)
    _positive_int(record.magic, "magic")
    _decimal_text(record.volume, "volume", positive=True)
    _decimal_text(record.price, "price", positive=True)
    if record.sl is not None:
        _decimal_text(record.sl, "sl", positive=True)
    if record.tp is not None:
        _decimal_text(record.tp, "tp", positive=True)
    _timestamp_ms(record.created_at_ms, "created_at_ms")
    _timestamp_ms(record.updated_at_ms, "updated_at_ms")
    _json(record.payload, "payload")


def _require_pma_lease(row: sqlite3.Row, worker_id: str, at_ms: int) -> None:
    if (
        row["status"] != "IN_FLIGHT"
        or row["locked_by"] != worker_id
        or row["locked_until_ms"] is None
        or at_ms > row["locked_until_ms"]
    ):
        raise LedgerConflictError("PMA job is not held by this live worker lease")


def _decode_row(row: sqlite3.Row) -> dict[str, Any]:
    decoded = dict(row)
    for key, value in tuple(decoded.items()):
        if key.endswith("_json") and isinstance(value, str):
            decoded[key[:-5]] = json.loads(value)
            del decoded[key]
    return decoded


def _decode_deal_row(row: sqlite3.Row) -> dict[str, Any]:
    """Expose canonical decimal deal economics under the public field names."""

    decoded = _decode_row(row)
    for name in ("volume", "price", "profit", "commission", "swap", "fee"):
        canonical = decoded.get(f"{name}_decimal")
        if canonical is None:
            raise LedgerInvariantError(
                f"broker deal {decoded.get('deal_id')!r} lacks canonical {name}"
            )
        decoded[f"{name}_legacy_real"] = decoded[name]
        decoded[name] = canonical
    return decoded


def _assert_columns_equal(
    row: sqlite3.Row,
    expected: Mapping[str, Any],
    columns: Sequence[str],
    label: str,
) -> None:
    differing = [name for name in columns if row[name] != expected[name]]
    if differing:
        raise LedgerConflictError(f"{label} differs in {differing}")


def _rollback(connection: sqlite3.Connection) -> None:
    try:
        connection.execute("ROLLBACK")
    except sqlite3.Error:
        pass


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LedgerInvariantError(f"{label} must be a non-empty string")
    return value.strip()


def _optional_text(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _text(value, label)


def _finite(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise LedgerInvariantError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise LedgerInvariantError(f"{label} must be finite")
    return result


def _optional_finite(value: Any, label: str) -> float | None:
    if value is None:
        return None
    return _finite(value, label)


def _positive(value: Any, label: str) -> float:
    result = _finite(value, label)
    if result <= 0:
        raise LedgerInvariantError(f"{label} must be positive")
    return result


def _nonnegative(value: Any, label: str) -> float:
    result = _finite(value, label)
    if result < 0:
        raise LedgerInvariantError(f"{label} cannot be negative")
    return result


def _timestamp_ms(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise LedgerInvariantError(f"{label} must be a non-negative integer timestamp")
    return value


def _positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise LedgerInvariantError(f"{label} must be a positive integer")
    return value


def _nonnegative_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise LedgerInvariantError(f"{label} must be a non-negative integer")
    return value


def _decimal_text(
    value: Any,
    label: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> str:
    if isinstance(value, bool):
        raise LedgerInvariantError(f"{label} must be numeric")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise LedgerInvariantError(f"{label} must be decimal numeric") from exc
    if not result.is_finite():
        raise LedgerInvariantError(f"{label} must be finite")
    if positive and result <= 0:
        raise LedgerInvariantError(f"{label} must be positive")
    if nonnegative and result < 0:
        raise LedgerInvariantError(f"{label} cannot be negative")
    if result == 0:
        return "0"
    return format(result.normalize(), "f")


def _risk_day(value: Any) -> str:
    day = _text(value, "risk_day")
    try:
        # ISO lexical shape plus round-trip catches invalid calendar dates.
        from datetime import date

        if date.fromisoformat(day).isoformat() != day:
            raise ValueError
    except ValueError as exc:
        raise LedgerInvariantError("risk_day must be an ISO UTC date (YYYY-MM-DD)") from exc
    return day


def _json(value: Any, label: str) -> str:
    try:
        return json.dumps(
            _json_ready(value),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise LedgerInvariantError(f"{label} must be JSON serializable: {exc}") from exc


def _json_ready(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_ready(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_json_ready(item) for item in value), key=repr)
    if isinstance(value, Decimal):
        return _decimal_text(value, "decimal")
    return value


__all__ = [
    "LEDGER_SCHEMA_VERSION",
    "PMA_DEFAULT_DELAY_MS",
    "BrokerCommandRecord",
    "BrokerDealRecord",
    "BrokerSnapshotRecord",
    "CloseTradeResult",
    "DailyRiskStateRecord",
    "LedgerConflictError",
    "LedgerError",
    "LedgerInvariantError",
    "LedgerUnavailableError",
    "LearningActivationRecord",
    "LearningApprovalRecord",
    "LearningProposalRecord",
    "LearningSnapshotRecord",
    "LearningSourceRecord",
    "OutboxRecord",
    "OwnedPositionRecord",
    "PendingOrderRecord",
    "ProviderHealthRecord",
    "ReconciliationItemRecord",
    "ReconciliationRunRecord",
    "RiskDecisionRecord",
    "RiskReservationRecord",
    "SchemaVersionError",
    "TradeAttributionRecord",
    "TradeEventRecord",
    "TradeLedger",
    "TradeRecord",
]
