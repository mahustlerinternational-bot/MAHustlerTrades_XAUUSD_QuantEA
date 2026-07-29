"""Broker-authoritative startup and reconnect reconciliation.

Wire parsing is intentionally outside this module.  A protocol adapter supplies
strict immutable snapshots and deal pages through ``ReconciliationBrokerPort``;
this service compares them to the durable ledger, persists exact reports, and
only grants READY when every blocking discrepancy is gone.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, Callable, Mapping, Protocol, Sequence, runtime_checkable

from .domain_models import Clock
from .runtime_state import (
    BrokerIdentity,
    ControlPlaneState,
    ExpectedBrokerIdentity,
    RuntimeStateMachine,
    identity_mismatches,
)
from .trade_ledger import (
    BrokerDealRecord,
    BrokerSnapshotRecord,
    DailyRiskStateRecord,
    LedgerError,
    LedgerInvariantError,
    OutboxRecord,
    OwnedPositionRecord,
    PendingOrderRecord,
    ReconciliationItemRecord,
    ReconciliationRunRecord,
    TradeEventRecord,
    TradeLedger,
)


class ReconciliationError(RuntimeError):
    """Base startup/reconnect reconciliation failure."""


class BrokerSnapshotError(ReconciliationError):
    """The broker snapshot/deal stream was unavailable or internally invalid."""


class ReconciliationLedgerError(ReconciliationError):
    """The durable reconciliation transaction failed and runtime was halted."""


class IssueSeverity(StrEnum):
    INFO = "INFO"
    BLOCKING = "BLOCKING"


@dataclass(frozen=True, slots=True)
class BrokerRiskSnapshot:
    risk_day: str
    balance: Decimal
    equity: Decimal
    day_start_equity: Decimal
    baseline_verified: bool
    daily_halted: bool

    def __post_init__(self) -> None:
        for name in ("balance", "equity", "day_start_equity"):
            object.__setattr__(self, name, Decimal(str(getattr(self, name))))
        if self.balance < 0 or self.equity < 0 or self.day_start_equity <= 0:
            raise ValueError("broker risk equity/balance values are invalid")


@dataclass(frozen=True, slots=True)
class AuthoritativeBrokerSnapshot:
    snapshot_id: str
    identity: BrokerIdentity
    risk: BrokerRiskSnapshot
    positions: tuple[OwnedPositionRecord, ...]
    pending_orders: tuple[PendingOrderRecord, ...]
    captured_at_ms: int
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.snapshot_id.strip():
            raise ValueError("snapshot_id must be non-empty")
        object.__setattr__(self, "positions", tuple(self.positions))
        object.__setattr__(self, "pending_orders", tuple(self.pending_orders))
        if self.captured_at_ms < 0:
            raise ValueError("captured_at_ms cannot be negative")
        for position in self.positions:
            if position.account_id != self.identity.account_id:
                raise ValueError("snapshot position belongs to another account")
        for order in self.pending_orders:
            if order.account_id != self.identity.account_id:
                raise ValueError("snapshot pending order belongs to another account")


@dataclass(frozen=True, slots=True)
class BrokerDealPage:
    previous_cursor: str | None
    next_cursor: str
    deals: tuple[BrokerDealRecord, ...]
    complete: bool

    def __post_init__(self) -> None:
        if not isinstance(self.next_cursor, str) or not self.next_cursor.strip():
            raise ValueError("deal page next_cursor must be non-empty")
        object.__setattr__(self, "deals", tuple(self.deals))


@runtime_checkable
class ReconciliationBrokerPort(Protocol):
    async def fetch_snapshot(
        self, expected: ExpectedBrokerIdentity
    ) -> AuthoritativeBrokerSnapshot:
        """Return a complete account snapshot for the current connection epoch."""

    async def fetch_deal_page(
        self, account_id: str, cursor: str | None
    ) -> BrokerDealPage:
        """Return the next ordered page after cursor, including an empty first page."""


@dataclass(frozen=True, slots=True)
class ReconciliationIssue:
    issue_id: str
    discrepancy_type: str
    severity: IssueSeverity
    action: str
    resolved: bool
    broker_position_id: str | None = None
    trade_id: str | None = None
    detail: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ReconciliationReport:
    run_id: str
    snapshot_id: str
    snapshot_hash: str
    connection_epoch: str
    deal_cursor: str | None
    issues: tuple[ReconciliationIssue, ...]
    succeeded: bool
    snapshot: AuthoritativeBrokerSnapshot

    @property
    def blocking_issues(self) -> tuple[ReconciliationIssue, ...]:
        return tuple(item for item in self.issues if item.severity is IssueSeverity.BLOCKING)


class ReconciliationService:
    def __init__(
        self,
        *,
        ledger: TradeLedger,
        runtime: RuntimeStateMachine,
        broker: ReconciliationBrokerPort,
        clock: Clock,
        daily_drawdown_pct_limit: Decimal = Decimal("3"),
        consecutive_loss_limit: int = 3,
        max_total_risk_pct: Decimal = Decimal("3"),
        max_open_positions: int = 3,
        max_deal_pages: int = 10_000,
        max_snapshot_age_ms: int = 5_000,
        max_future_skew_ms: int = 1_000,
        outbox_projector: Callable[
            [TradeEventRecord], Sequence[OutboxRecord]
        ]
        | None = None,
    ):
        if not isinstance(broker, ReconciliationBrokerPort):
            raise TypeError("broker must implement reconciliation snapshot/deal ports")
        self._ledger = ledger
        self._runtime = runtime
        self._broker = broker
        self._clock = clock
        self._drawdown_limit = Decimal(str(daily_drawdown_pct_limit))
        self._loss_limit = consecutive_loss_limit
        self._max_total_risk_pct = Decimal(str(max_total_risk_pct))
        self._max_open_positions = max_open_positions
        self._max_deal_pages = max_deal_pages
        self._max_snapshot_age_ms = max_snapshot_age_ms
        self._max_future_skew_ms = max_future_skew_ms
        self._outbox_projector = outbox_projector
        if not Decimal("0") < self._drawdown_limit <= Decimal("3"):
            raise ValueError("daily drawdown limit must be in (0,3]")
        if not Decimal("0") < self._max_total_risk_pct <= Decimal("3"):
            raise ValueError("total risk limit must be in (0,3]")
        if (
            isinstance(self._max_open_positions, bool)
            or not isinstance(self._max_open_positions, int)
            or not 1 <= self._max_open_positions <= 3
        ):
            raise ValueError("maximum open positions must be between 1 and 3")
        if (
            self._loss_limit <= 0
            or not 1 <= max_deal_pages <= 100_000
            or not 100 <= max_snapshot_age_ms <= 300_000
            or not 0 <= max_future_skew_ms <= 60_000
        ):
            raise ValueError("reconciliation safety limits are invalid")

    def _outbox_for(self, event: TradeEventRecord) -> tuple[OutboxRecord, ...]:
        """Project recovery events before their authoritative transaction.

        Reconciliation remains independent of any transport implementation;
        the runtime supplies a deterministic structured-event projector.  A
        malformed projection is a ledger invariant failure, never a reason to
        commit the close without its expected external-event records.
        """

        if self._outbox_projector is None:
            return ()
        records = tuple(self._outbox_projector(event))
        for record in records:
            if not isinstance(record, OutboxRecord):
                raise LedgerInvariantError(
                    "reconciliation outbox projector returned an invalid record"
                )
            if record.event_id != event.event_id:
                raise LedgerInvariantError(
                    "reconciliation outbox projection references another event"
                )
        return records

    async def reconcile(
        self, expected: ExpectedBrokerIdentity
    ) -> ReconciliationReport:
        if self._runtime.state not in {
            ControlPlaneState.SYNCHRONIZING,
            ControlPlaneState.DEGRADED,
        }:
            raise ReconciliationError(
                f"reconciliation requires SYNCHRONIZING/DEGRADED, current={self._runtime.state}"
            )
        try:
            snapshot = await self._broker.fetch_snapshot(expected)
        except Exception as exc:
            self._runtime.disconnect(f"broker snapshot failed: {type(exc).__name__}")
            raise BrokerSnapshotError("cannot obtain authoritative broker snapshot") from exc

        snapshot_hash = _snapshot_hash(snapshot)
        run_started_at_ms = self._clock.now_ms()
        run_id = (
            f"recon:{snapshot.identity.connection_epoch}:"
            f"{snapshot.snapshot_id}:{run_started_at_ms}"
        )
        issues: list[ReconciliationIssue] = []
        mismatches = identity_mismatches(expected, snapshot.identity)
        for mismatch in mismatches:
            issues.append(
                self._issue(
                    run_id,
                    "BROKER_IDENTITY_MISMATCH",
                    IssueSeverity.BLOCKING,
                    "HALT_WITHOUT_APPLYING_SCOPE",
                    False,
                    detail={"mismatch": mismatch},
                )
            )

        now_ms = self._clock.now_ms()
        snapshot_age_ms = now_ms - snapshot.captured_at_ms
        snapshot_fresh = (
            -self._max_future_skew_ms
            <= snapshot_age_ms
            <= self._max_snapshot_age_ms
        )
        if not snapshot_fresh:
            issues.append(
                self._issue(
                    run_id,
                    "STALE_OR_FUTURE_BROKER_SNAPSHOT",
                    IssueSeverity.BLOCKING,
                    "HALT_WITHOUT_APPLYING_SCOPE",
                    False,
                    detail={"snapshot_age_ms": snapshot_age_ms},
                )
            )
        current_risk_day = datetime.fromtimestamp(now_ms / 1_000, UTC).date().isoformat()
        risk_day_current = snapshot.risk.risk_day == current_risk_day
        if not risk_day_current:
            issues.append(
                self._issue(
                    run_id,
                    "BROKER_RISK_DAY_MISMATCH",
                    IssueSeverity.BLOCKING,
                    "HALT_WITHOUT_INITIALIZING_BASELINE",
                    False,
                    detail={
                        "expected_risk_day": current_risk_day,
                        "broker_risk_day": snapshot.risk.risk_day,
                    },
                )
            )

        snapshot_usable = not mismatches and snapshot_fresh and risk_day_current
        deal_cursor: str | None = None
        try:
            self._ledger.require_healthy()
            ledger_positions = self._ledger.list_owned_positions(
                expected.account_id, expected.symbol, expected.magic
            )
            ledger_orders = self._ledger.list_pending_orders(
                expected.account_id, expected.symbol, expected.magic
            )
            unresolved_commands = self._ledger.list_unresolved_commands()

            if snapshot_usable:
                deal_cursor = await self._synchronize_deals(snapshot)
                owned_positions, foreign_positions = self._partition_positions(
                    snapshot, expected
                )
                owned_orders, foreign_orders = self._partition_orders(snapshot, expected)
                owned_positions = self._recover_unregistered_opens(
                    run_id,
                    snapshot,
                    unresolved_commands,
                    owned_positions,
                    issues,
                )
                unresolved_commands = self._ledger.list_unresolved_commands()
                self._reconcile_created_open_commands(
                    run_id,
                    snapshot,
                    unresolved_commands,
                    owned_positions,
                    owned_orders,
                    issues,
                )
                unresolved_commands = self._ledger.list_unresolved_commands()
                owned_positions = self._reconcile_lifecycle_commands(
                    run_id, unresolved_commands, owned_positions, issues
                )
                ledger_positions = self._ledger.list_owned_positions(
                    expected.account_id, expected.symbol, expected.magic
                )
                unresolved_commands = self._ledger.list_unresolved_commands()
                self._reconcile_broker_triggered_closes(
                    run_id,
                    snapshot,
                    ledger_positions,
                    owned_positions,
                    issues,
                )
                ledger_positions = self._ledger.list_owned_positions(
                    expected.account_id, expected.symbol, expected.magic
                )
                unresolved_commands = self._ledger.list_unresolved_commands()
                # Exposure must be computed after exact no-effect/open-close
                # recovery.  Otherwise stale reservations or positions can
                # manufacture a drawdown/position-count halt during restart.
                risk_state = self._synchronize_risk(snapshot, run_id, issues)
                self._compare_positions(
                    run_id,
                    ledger_positions,
                    owned_positions,
                    unresolved_commands,
                    issues,
                )
                self._compare_orders(
                    run_id,
                    ledger_orders,
                    owned_orders,
                    owned_positions,
                    unresolved_commands,
                    issues,
                )
                self._report_foreign(run_id, foreign_positions, foreign_orders, issues)
                self._compare_commands(
                    run_id,
                    unresolved_commands,
                    owned_positions,
                    owned_orders,
                    issues,
                )
            else:
                risk_state = None
                owned_positions = ()
                owned_orders = ()

            blocking = tuple(
                item for item in issues if item.severity is IssueSeverity.BLOCKING
            )
            run = ReconciliationRunRecord(
                run_id=run_id,
                connection_epoch=snapshot.identity.connection_epoch,
                account_id=snapshot.identity.account_id,
                started_at=run_started_at_ms / 1_000,
                broker_position_count=len(snapshot.positions),
                ledger_position_count=len(ledger_positions),
                snapshot_hash=snapshot_hash,
                detail={
                    "snapshot_id": snapshot.snapshot_id,
                    "pending_order_count": len(snapshot.pending_orders),
                    "deal_cursor": deal_cursor,
                },
            )
            snapshot_record = BrokerSnapshotRecord(
                snapshot_id=snapshot.snapshot_id,
                connection_epoch=snapshot.identity.connection_epoch,
                account_id=snapshot.identity.account_id,
                broker_server=snapshot.identity.broker_server,
                trade_mode=snapshot.identity.trade_mode.value,
                symbol=snapshot.identity.symbol,
                magic=snapshot.identity.magic,
                captured_at_ms=snapshot.captured_at_ms,
                snapshot_hash=snapshot_hash,
                payload=_jsonable(snapshot.payload),
            )
            item_records_list: list[ReconciliationItemRecord] = []
            for item in issues:
                persisted_trade_id = item.trade_id
                item_detail = {**dict(item.detail), "severity": item.severity.value}
                if (
                    persisted_trade_id is not None
                    and self._ledger.get_trade(persisted_trade_id) is None
                ):
                    item_detail["broker_reported_trade_id"] = persisted_trade_id
                    persisted_trade_id = None
                item_records_list.append(
                    ReconciliationItemRecord(
                        run_id=run_id,
                        item_id=item.issue_id,
                        discrepancy_type=item.discrepancy_type,
                        action=item.action,
                        broker_position_id=item.broker_position_id,
                        trade_id=persisted_trade_id,
                        resolved=item.resolved,
                        detail=item_detail,
                    )
                )
            item_records = tuple(item_records_list)
            succeeded = not blocking
            self._ledger.persist_reconciliation_atomic(
                run=run,
                snapshot=snapshot_record,
                items=item_records,
                owned_positions=owned_positions,
                pending_orders=owned_orders,
                finished_at=self._clock.now_ms() / 1_000,
                succeeded=succeeded,
                replace_scope=snapshot_usable,
            )
        except LedgerError as exc:
            self._runtime.halt(
                f"authoritative ledger failure during reconciliation: {type(exc).__name__}: {exc}"
            )
            raise ReconciliationLedgerError(str(exc)) from exc
        except BrokerSnapshotError as exc:
            self._runtime.disconnect(f"broker history invalid: {exc}")
            raise
        except Exception as exc:
            self._runtime.disconnect(f"broker deal synchronization failed: {type(exc).__name__}")
            raise BrokerSnapshotError("cannot synchronize authoritative broker history") from exc

        report = ReconciliationReport(
            run_id=run_id,
            snapshot_id=snapshot.snapshot_id,
            snapshot_hash=snapshot_hash,
            connection_epoch=snapshot.identity.connection_epoch,
            deal_cursor=deal_cursor,
            issues=tuple(issues),
            succeeded=succeeded,
            snapshot=snapshot,
        )
        self._runtime.mark_synchronized(
            snapshot_id=snapshot.snapshot_id,
            reconciliation_run_id=run_id,
            deal_cursor_initialized=deal_cursor is not None,
            daily_baseline_verified=bool(
                risk_state and risk_state["baseline_verified"]
            ),
            daily_risk_halted=bool(
                True if risk_state is None else risk_state["halted"]
            ),
            blocking_diffs=tuple(
                f"{item.discrepancy_type}:{item.issue_id}"
                for item in report.blocking_issues
            ),
        )
        return report

    async def _synchronize_deals(
        self, snapshot: AuthoritativeBrokerSnapshot
    ) -> str:
        account_id = snapshot.identity.account_id
        cursor = self._ledger.get_deal_cursor(account_id)
        seen: set[tuple[str | None, str]] = set()
        for _ in range(self._max_deal_pages):
            try:
                page = await self._broker.fetch_deal_page(account_id, cursor)
            except Exception as exc:
                raise BrokerSnapshotError("deal-page request failed") from exc
            if page.previous_cursor != cursor:
                raise BrokerSnapshotError(
                    f"deal page gap: requested {cursor!r}, page starts {page.previous_cursor!r}"
                )
            edge = (cursor, page.next_cursor)
            if edge in seen or page.next_cursor == cursor:
                if not page.complete:
                    raise BrokerSnapshotError("deal cursor did not advance")
            seen.add(edge)
            self._ledger.record_deal_page_and_advance_cursor(
                account_id=account_id,
                stream="history",
                expected_cursor=cursor,
                next_cursor=page.next_cursor,
                connection_epoch=snapshot.identity.connection_epoch,
                deals=page.deals,
                updated_at_ms=self._clock.now_ms(),
            )
            cursor = page.next_cursor
            if page.complete:
                return cursor
        raise BrokerSnapshotError("broker deal history exceeded page safety limit")

    def _recover_unregistered_opens(
        self,
        run_id: str,
        snapshot: AuthoritativeBrokerSnapshot,
        commands: list[dict[str, Any]],
        positions: tuple[OwnedPositionRecord, ...],
        issues: list[ReconciliationIssue],
    ) -> tuple[OwnedPositionRecord, ...]:
        """Register a crash-lost OPEN only from one complete immutable proof.

        The gateway intentionally strips ownership from a live position when
        no durable ``OwnedPosition`` exists.  Reconciliation may restore that
        ownership only when a single SENT/RECEIVED/UNKNOWN OPEN envelope, its
        canonical trade and reservation, one attributed broker IN deal, and
        the raw authenticated position all agree exactly.  The final deal,
        event, position, command, reservation, trade and outbox mutation is
        committed by ``acknowledge_open_command_atomic``.
        """

        open_commands = tuple(
            command
            for command in commands
            if command.get("action") == "OPEN"
            and command.get("status") in {"SENT", "RECEIVED", "UNKNOWN"}
        )
        normalized = list(positions)
        for index, observed in enumerate(positions):
            if not _is_raw_quarantined_position(observed):
                continue
            candidates: list[
                tuple[dict[str, Any], OwnedPositionRecord, BrokerDealRecord]
            ] = []
            for command in open_commands:
                proof = self._exact_open_recovery_candidate(
                    snapshot=snapshot,
                    observed=observed,
                    command=command,
                    all_open_commands=open_commands,
                )
                if proof is not None:
                    recovered, deal = proof
                    candidates.append((command, recovered, deal))

            if len(candidates) > 1:
                issues.append(
                    self._issue(
                        run_id,
                        "UNREGISTERED_OPEN_RECOVERY_AMBIGUOUS",
                        IssueSeverity.BLOCKING,
                        "KEEP_RAW_EXPOSURE_QUARANTINED",
                        False,
                        broker_position_id=observed.broker_position_id,
                        detail={
                            "candidate_command_ids": [
                                item[0]["command_id"] for item in candidates
                            ]
                        },
                    )
                )
                continue
            if not candidates:
                continue

            command, recovered, deal = candidates[0]
            event = TradeEventRecord(
                event_id=(
                    f"reconciled-open:{command['command_id']}:{deal.deal_id}"
                ),
                trade_id=command["trade_id"],
                event_type="POSITION_OPENED_RECONCILED",
                source="reconciliation",
                correlation_id=command["command_id"],
                occurred_at=snapshot.captured_at_ms / 1_000,
                broker_timestamp=deal.occurred_at,
                payload={
                    "snapshot_id": snapshot.snapshot_id,
                    "broker_position_id": recovered.broker_position_id,
                    "position_ticket": recovered.position_ticket,
                    "order_id": deal.order_id,
                    "deal_id": deal.deal_id,
                    "position_version": recovered.position_version,
                    "fill_price": str(deal.price),
                    "fill_volume": str(deal.volume),
                    "remaining_volume": str(recovered.remaining_volume),
                    "sl": str(recovered.current_sl),
                    "tp": str(recovered.tp3),
                    "profit": str(deal.profit),
                    "commission": str(deal.commission),
                    "swap": str(deal.swap),
                    "fee": str(deal.fee),
                    "recovery_proof": "ONE_EXACT_ATTRIBUTED_IN_DEAL",
                },
            )
            self._ledger.acknowledge_open_command_atomic(
                command["command_id"],
                event=event,
                position=recovered,
                deal=deal,
                outbox=self._outbox_for(event),
            )
            normalized[index] = recovered
            issues.append(
                self._issue(
                    run_id,
                    "UNREGISTERED_OPEN_RECOVERED",
                    IssueSeverity.INFO,
                    "REGISTER_FROM_EXACT_IN_DEAL_AND_CONSUME_RESERVATION",
                    True,
                    broker_position_id=recovered.broker_position_id,
                    trade_id=recovered.trade_id,
                    detail={
                        "command_id": command["command_id"],
                        "deal_id": deal.deal_id,
                        "broker_order_id": deal.order_id,
                        "actual_initial_risk": str(
                            recovered.initial_risk_amount
                        ),
                    },
                )
            )
        return tuple(normalized)

    def _exact_open_recovery_candidate(
        self,
        *,
        snapshot: AuthoritativeBrokerSnapshot,
        observed: OwnedPositionRecord,
        command: Mapping[str, Any],
        all_open_commands: Sequence[Mapping[str, Any]],
    ) -> tuple[OwnedPositionRecord, BrokerDealRecord] | None:
        trade_id = command.get("trade_id")
        command_id = command.get("command_id")
        reservation_id = command.get("reservation_id")
        if not all(
            isinstance(value, str) and value
            for value in (trade_id, command_id, reservation_id)
        ):
            return None
        if (
            command.get("sent_at_ms") is None
            or command.get("position_id") is not None
            or sum(
                item.get("trade_id") == trade_id for item in all_open_commands
            )
            != 1
        ):
            return None

        trade = self._ledger.get_trade(trade_id)
        reservation = self._ledger.get_reservation(reservation_id)
        if trade is None or reservation is None:
            return None
        if (
            trade.get("account_id") != snapshot.identity.account_id
            or trade.get("symbol") != observed.symbol
            or trade.get("magic_number") != observed.magic
            or trade.get("direction") != observed.direction
            or trade.get("broker_position_id") is not None
            or reservation.get("trade_id") != trade_id
            or reservation.get("account_id") != observed.account_id
            or reservation.get("direction") != observed.direction
            or reservation.get("state") not in {"SUBMITTED", "UNKNOWN"}
        ):
            return None

        try:
            intended_entry = _canonical_trade_decimal(trade, "intended_entry")
            intended_sl = _canonical_trade_decimal(trade, "intended_sl")
            tp1 = _canonical_trade_decimal(trade, "tp1")
            tp2 = _canonical_trade_decimal(trade, "tp2")
            tp3 = _canonical_trade_decimal(trade, "tp3")
            requested_volume = _canonical_trade_decimal(trade, "requested_volume")
            planned_risk = _canonical_trade_decimal(
                trade, "initial_risk_currency"
            )
            reserved_volume = _mapping_decimal(reservation, "volume")
            reserved_risk = _mapping_decimal(reservation, "risk_amount")
        except LedgerError:
            return None
        effect = command.get("expected_effect")
        payload = command.get("payload")
        expected_effect = {
            "event_type": "POSITION_OPENED",
            "trade_id": trade_id,
            "reservation_id": reservation_id,
            "account_id": observed.account_id,
            "symbol": observed.symbol,
            "magic": observed.magic,
            "direction": observed.direction,
            "initial_volume": reserved_volume,
            "original_sl": intended_sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "initial_risk_amount": reserved_risk,
        }
        expected_payload_economics = {
            "action": "OPEN",
            "trade_id": trade_id,
            "reservation_id": reservation_id,
            "account_id": observed.account_id,
            "symbol": observed.symbol,
            "magic": observed.magic,
            "direction": observed.direction,
            "volume": reserved_volume,
            "risk_amount": reserved_risk,
            "entry": intended_entry,
            "sl": intended_sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
        }
        if not _exact_open_effect(effect, expected_effect) or not _open_payload_matches(
            payload, expected_payload_economics
        ):
            return None
        if (
            requested_volume != reserved_volume
            or planned_risk != reserved_risk
            or observed.current_sl != intended_sl
            or observed.tp3 != tp3
        ):
            return None

        deals = self._ledger.get_trade_deals(trade_id)
        if len(deals) != 1:
            return None
        deal = deals[0]
        payload = deal.get("payload")
        if not isinstance(payload, Mapping):
            return None
        if (
            deal.get("account_id") != observed.account_id
            or deal.get("position_id") != observed.broker_position_id
            or deal.get("trade_id") != trade_id
            or deal.get("command_id") != command_id
            or str(deal.get("deal_type", "")).upper() != "IN"
            or not isinstance(deal.get("order_id"), str)
            or not deal["order_id"]
            or payload.get("attribution_status") != "VERIFIED"
            or payload.get("wire_trade_id") != trade_id
            or payload.get("wire_command_id") != command_id
            or payload.get("direction") != observed.direction
            or payload.get("entry_type") != "IN"
        ):
            return None
        try:
            deal_volume = _mapping_decimal(deal, "volume")
            deal_price = _mapping_decimal(deal, "price")
            deal_time_ms = _exact_millisecond(deal.get("occurred_at"))
            sent_at_ms = int(command["sent_at_ms"])
        except (LedgerError, TypeError, ValueError):
            return None
        if (
            deal_volume != observed.remaining_volume
            or deal_volume != observed.initial_volume
            or deal_volume > reserved_volume
            or deal_price != observed.entry_price
            or deal_time_ms != observed.opened_at_ms
            or not sent_at_ms <= deal_time_ms <= snapshot.captured_at_ms
        ):
            return None

        geometry = (
            intended_sl < deal_price < tp1 < tp2 < tp3
            if observed.direction == "BUY"
            else intended_sl > deal_price > tp1 > tp2 > tp3
        )
        planned_distance = abs(intended_entry - intended_sl)
        actual_distance = abs(deal_price - intended_sl)
        if not geometry or planned_distance <= 0 or actual_distance <= 0:
            return None
        actual_risk = (
            reserved_risk
            * deal_volume
            / reserved_volume
            * actual_distance
            / planned_distance
        )
        if actual_risk <= 0 or actual_risk > reserved_risk:
            return None

        recovered = OwnedPositionRecord(
            account_id=observed.account_id,
            trade_id=trade_id,
            broker_position_id=observed.broker_position_id,
            position_ticket=observed.position_ticket,
            broker_order_id=deal["order_id"],
            symbol=observed.symbol,
            magic=observed.magic,
            direction=observed.direction,
            initial_volume=deal_volume,
            remaining_volume=observed.remaining_volume,
            entry_price=deal_price,
            original_sl=intended_sl,
            current_sl=observed.current_sl,
            tp1=tp1,
            tp2=tp2,
            tp3=tp3,
            initial_risk_amount=actual_risk,
            lifecycle_stage="PRE_TP1",
            position_version=observed.position_version,
            opened_at_ms=observed.opened_at_ms,
            updated_at_ms=snapshot.captured_at_ms,
        )
        deal_record = BrokerDealRecord(
            account_id=deal["account_id"],
            deal_id=deal["deal_id"],
            position_id=deal["position_id"],
            order_id=deal["order_id"],
            deal_type=deal["deal_type"],
            volume=deal["volume"],
            price=deal["price"],
            profit=deal["profit"],
            commission=deal["commission"],
            swap=deal["swap"],
            fee=deal["fee"],
            occurred_at=deal["occurred_at"],
            trade_id=deal["trade_id"],
            command_id=deal["command_id"],
            payload=payload,
        )
        return recovered, deal_record

    def _synchronize_risk(
        self,
        snapshot: AuthoritativeBrokerSnapshot,
        run_id: str,
        issues: list[ReconciliationIssue],
    ) -> dict[str, Any]:
        risk = snapshot.risk
        performance = self._ledger.get_daily_realized_performance(
            snapshot.identity.account_id,
            risk.risk_day,
            as_of_ms=snapshot.captured_at_ms,
        )
        realised_pnl = Decimal(str(performance["realised_pnl"]))
        consecutive_losses = int(performance["consecutive_losses"])
        existing = self._ledger.get_daily_risk_state(
            snapshot.identity.account_id, risk.risk_day
        )
        if existing is None:
            self._ledger.initialize_daily_risk_state(
                DailyRiskStateRecord(
                    account_id=snapshot.identity.account_id,
                    risk_day=risk.risk_day,
                    day_start_equity=risk.day_start_equity,
                    current_equity=risk.equity,
                    baseline_verified=risk.baseline_verified,
                    halted=risk.daily_halted,
                    halt_reason="BROKER_DAILY_HALT" if risk.daily_halted else None,
                    realised_pnl=realised_pnl,
                    consecutive_losses=consecutive_losses,
                    current_balance=risk.balance,
                    updated_at_ms=snapshot.captured_at_ms,
                )
            )
        else:
            durable_baseline = Decimal(existing["day_start_equity"])
            baseline_matches = durable_baseline == risk.day_start_equity

            if not baseline_matches:
                issues.append(
                    self._issue(
                        run_id,
                        "DAILY_BASELINE_MISMATCH",
                        IssueSeverity.BLOCKING,
                        "KEEP_DURABLE_BASELINE_AND_HALT",
                        False,
                        detail={
                            "durable": existing["day_start_equity"],
                            "broker": str(risk.day_start_equity),
                        },
                    )
                )
            elif (
                risk.baseline_verified
                and not bool(existing["baseline_verified"])
            ):
                self._ledger.verify_daily_risk_baseline(
                    snapshot.identity.account_id,
                    risk.risk_day,
                    expected_baseline=risk.day_start_equity,
                    verified_at_ms=snapshot.captured_at_ms,
                    source="BROKER_OPERATOR_APPROVED",
                )

            self._ledger.update_daily_risk_equity(
                snapshot.identity.account_id,
                risk.risk_day,
                current_equity=risk.equity,
                realised_pnl=realised_pnl,
                consecutive_losses=consecutive_losses,
                updated_at_ms=snapshot.captured_at_ms,
                current_balance=risk.balance,
            )

        current_state = self._ledger.get_daily_risk_state(
            snapshot.identity.account_id, risk.risk_day
        )
        if current_state is None:
            raise RuntimeError(
                "daily risk state disappeared during reconciliation"
            )
        authoritative_baseline = Decimal(current_state["day_start_equity"])
        drawdown_pct = max(
            Decimal("0"),
            (authoritative_baseline - risk.equity)
            / authoritative_baseline
            * Decimal("100"),
        )
        exposure = self._ledger.get_active_risk_exposure(
            snapshot.identity.account_id, risk.risk_day
        )
        risk_base = min(risk.balance, risk.equity)
        active_risk = Decimal(exposure["risk_amount"])
        halt_reasons: list[str] = []
        if current_state["halted"]:
            halt_reasons.append("DURABLE_DAILY_HALT")
        if risk.daily_halted:
            halt_reasons.append("BROKER_DAILY_HALT")
        if drawdown_pct >= self._drawdown_limit:
            halt_reasons.append("DAILY_DRAWDOWN_LIMIT")
        if consecutive_losses >= self._loss_limit:
            halt_reasons.append("CONSECUTIVE_LOSS_LIMIT")
        configured_risk_limit = (
            risk_base * self._max_total_risk_pct / Decimal("100")
        )
        if active_risk > configured_risk_limit:
            halt_reasons.append("TOTAL_RISK_CONFIGURED_LIMIT")
        if exposure["position_count"] > self._max_open_positions:
            halt_reasons.append("POSITION_COUNT_CONFIGURED_LIMIT")
        if halt_reasons:
            if not current_state["halted"]:
                self._ledger.halt_daily_risk(
                    snapshot.identity.account_id,
                    risk.risk_day,
                    reason="+".join(halt_reasons),
                    halted_at_ms=snapshot.captured_at_ms,
                )
            issues.append(
                self._issue(
                    run_id,
                    "DAILY_RISK_HALTED",
                    IssueSeverity.BLOCKING,
                    "HALT_UNTIL_NEXT_UTC_RISK_DAY",
                    False,
                    detail={
                        "reasons": halt_reasons,
                        "drawdown_pct": str(drawdown_pct),
                        "active_risk": str(active_risk),
                        "active_risk_limit": str(configured_risk_limit),
                        "risk_base": str(risk_base),
                        "position_count": exposure["position_count"],
                        "realised_pnl": str(realised_pnl),
                        "consecutive_losses": consecutive_losses,
                        "closing_deal_count": performance["closing_deal_count"],
                        "closed_trade_count": performance["closed_trade_count"],
                        "position_count_limit": self._max_open_positions,
                    },
                )
            )
        if not risk.baseline_verified:
            issues.append(
                self._issue(
                    run_id,
                    "DAILY_BASELINE_UNVERIFIED",
                    IssueSeverity.BLOCKING,
                    "HALT_UNTIL_BROKER_BASELINE_VERIFIED",
                    False,
                )
            )
        state = self._ledger.get_daily_risk_state(
            snapshot.identity.account_id, risk.risk_day
        )
        if state is None:
            raise RuntimeError(
                "daily risk state was not persisted during reconciliation"
            )
        return state

    def _reconcile_created_open_commands(
        self,
        run_id: str,
        snapshot: AuthoritativeBrokerSnapshot,
        commands: list[dict[str, Any]],
        positions: tuple[OwnedPositionRecord, ...],
        orders: tuple[PendingOrderRecord, ...],
        issues: list[ReconciliationIssue],
    ) -> None:
        """Release only obsolete OPEN envelopes proven never sent or applied.

        CREATED is the durable pre-send boundary.  An envelope from the active
        authenticated epoch remains resumable until expiry.  Once it is stale,
        cancellation is safe only when the complete snapshot, pending-order
        snapshot, and synchronized attributed deal history all show no effect.
        Unattributed owned exposure is deliberately treated as ambiguous.
        """

        now_ms = self._clock.now_ms()
        current_epoch = snapshot.identity.connection_epoch
        for command in commands:
            if command["action"] != "OPEN" or command["status"] != "CREATED":
                continue
            trade = self._ledger.get_trade(command["trade_id"])
            if trade is None:
                raise LedgerError("CREATED OPEN command has no canonical trade")
            if (
                trade["account_id"] != snapshot.identity.account_id
                or trade["symbol"] != snapshot.identity.symbol
                or trade["magic_number"] != snapshot.identity.magic
            ):
                # This reconciler owns exactly one authenticated tuple.  Never
                # mutate a command outside it.
                continue

            matching_positions = tuple(
                item
                for item in positions
                if item.trade_id == command["trade_id"]
            )
            ambiguous_positions = tuple(
                item for item in positions if item.trade_id is None
            )
            matching_orders = tuple(
                item
                for item in orders
                if item.trade_id == command["trade_id"]
                or item.command_id == command["command_id"]
            )
            ambiguous_orders = tuple(
                item
                for item in orders
                if item.trade_id is None and item.command_id is None
            )
            deals = tuple(self._ledger.get_trade_deals(command["trade_id"]))
            effect_or_ambiguity = bool(
                matching_positions
                or ambiguous_positions
                or matching_orders
                or ambiguous_orders
                or deals
            )
            if effect_or_ambiguity:
                issues.append(
                    self._issue(
                        run_id,
                        "UNSENT_OPEN_COMMAND_EFFECT_AMBIGUOUS",
                        IssueSeverity.BLOCKING,
                        "KEEP_RESERVATION_AND_HALT_FOR_EXACT_EFFECT_PROOF",
                        False,
                        trade_id=command["trade_id"],
                        detail={
                            "command_id": command["command_id"],
                            "matching_position_ids": [
                                item.broker_position_id for item in matching_positions
                            ],
                            "ambiguous_position_ids": [
                                item.broker_position_id for item in ambiguous_positions
                            ],
                            "matching_order_ids": [
                                item.broker_order_id for item in matching_orders
                            ],
                            "ambiguous_order_ids": [
                                item.broker_order_id for item in ambiguous_orders
                            ],
                            "attributed_deal_ids": [item["deal_id"] for item in deals],
                        },
                    )
                )
                continue

            active_envelope = (
                command["connection_epoch"] == current_epoch
                and command["expires_at_ms"] > now_ms
            )
            if active_envelope:
                continue
            reason = (
                "OLD_CONNECTION_EPOCH"
                if command["connection_epoch"] != current_epoch
                else "CREATED_ENVELOPE_EXPIRED"
            )
            self._ledger.resolve_created_command_not_sent(
                command["command_id"], resolved_at_ms=now_ms, reason=reason
            )
            issues.append(
                self._issue(
                    run_id,
                    "UNSENT_OPEN_COMMAND_RECONCILED_NOT_APPLIED",
                    IssueSeverity.INFO,
                    "RELEASE_NEVER_SENT_RESERVATION",
                    True,
                    trade_id=command["trade_id"],
                    detail={
                        "command_id": command["command_id"],
                        "proof": {
                            "reason": reason,
                            "snapshot_id": snapshot.snapshot_id,
                            "broker_position_absent": True,
                            "broker_pending_order_absent": True,
                            "attributed_deal_count": 0,
                        },
                    },
                )
            )

    def _reconcile_broker_triggered_closes(
        self,
        run_id: str,
        snapshot: AuthoritativeBrokerSnapshot,
        ledger_positions: list[dict[str, Any]],
        broker_positions: tuple[OwnedPositionRecord, ...],
        issues: list[ReconciliationIssue],
    ) -> None:
        """Apply an absent broker position only from an exact SL/TP deal proof."""

        broker_ids = {item.broker_position_id for item in broker_positions}
        for position in ledger_positions:
            position_id = position["broker_position_id"]
            if position_id in broker_ids:
                continue
            trade = self._ledger.get_trade(position["trade_id"])
            if trade is None:
                raise LedgerError("owned position has no canonical trade")
            proof = _broker_triggered_close_proof(
                trade=trade,
                position=position,
                deals=self._ledger.get_trade_deals(position["trade_id"]),
            )
            if proof is None:
                continue
            closing_deals, final_deal, reason = proof
            event = TradeEventRecord(
                event_id=(
                    f"broker-triggered-close:{position['trade_id']}:"
                    f"{final_deal['deal_id']}"
                ),
                trade_id=position["trade_id"],
                event_type=f"POSITION_CLOSED_{reason}_RECONCILED",
                source="reconciliation",
                correlation_id=final_deal["deal_id"],
                occurred_at=final_deal["occurred_at"],
                broker_timestamp=final_deal["occurred_at"],
                payload={
                    "snapshot_id": snapshot.snapshot_id,
                    "broker_position_id": position_id,
                    "broker_reason": reason,
                    "closing_deal_ids": [item["deal_id"] for item in closing_deals],
                },
            )
            result = self._ledger.close_trade_from_recorded_deals_atomic(
                position["trade_id"],
                event=event,
                position_id=position_id,
                exit_price=final_deal["price"],
                closed_at=final_deal["occurred_at"],
                command_id=None,
                outbox=self._outbox_for(event),
            )
            issues.append(
                self._issue(
                    run_id,
                    "BROKER_TRIGGERED_POSITION_CLOSED",
                    IssueSeverity.INFO,
                    "CLOSE_FROM_EXACT_RECORDED_SL_TP_DEALS",
                    True,
                    broker_position_id=position_id,
                    trade_id=position["trade_id"],
                    detail={
                        "broker_reason": reason,
                        "final_deal_id": final_deal["deal_id"],
                        "closing_deal_ids": [
                            item["deal_id"] for item in closing_deals
                        ],
                        "actual_r": str(result.actual_r),
                    },
                )
            )

    def _reconcile_lifecycle_commands(
        self,
        run_id: str,
        commands: list[dict[str, Any]],
        positions: tuple[OwnedPositionRecord, ...],
        issues: list[ReconciliationIssue],
    ) -> tuple[OwnedPositionRecord, ...]:
        """Resolve lifecycle commands only when snapshot/deal proof is exact."""

        by_id = {item.broker_position_id: item for item in positions}
        normalized = dict(by_id)
        for command in commands:
            action = command["action"]
            if action not in {"MODIFY_SL", "PARTIAL_CLOSE", "CLOSE"}:
                continue
            if command["status"] == "CREATED":
                current_epoch = self._runtime.evidence.connection_epoch
                resumable = (
                    command["connection_epoch"] == current_epoch
                    and command["expires_at_ms"] > self._clock.now_ms()
                )
                if resumable:
                    # Same authenticated epoch and a live envelope may resume
                    # exactly once after READY.
                    continue
                reason = (
                    "OLD_CONNECTION_EPOCH"
                    if command["connection_epoch"] != current_epoch
                    else "CREATED_ENVELOPE_EXPIRED"
                )
                self._ledger.resolve_created_command_not_sent(
                    command["command_id"],
                    resolved_at_ms=self._clock.now_ms(),
                    reason=reason,
                )
                position_id = command.get("position_id")
                position = by_id.get(position_id)
                if position is not None and position.pending_command_id == command["command_id"]:
                    normalized[position_id] = replace(
                        position,
                        pending_action=None,
                        pending_milestone=None,
                        pending_command_id=None,
                        pending_close_volume=Decimal("0"),
                        pending_filled_volume=Decimal("0"),
                    )
                issues.append(
                    self._issue(
                        run_id,
                        "UNSENT_COMMAND_RECONCILED_NOT_APPLIED",
                        IssueSeverity.INFO,
                        "CLEAR_NEVER_SENT_ACTION_LOCK",
                        True,
                        broker_position_id=position_id,
                        trade_id=command["trade_id"],
                        detail={"command_id": command["command_id"], "proof": reason},
                    )
                )
                continue
            effect = command.get("expected_effect")
            if not isinstance(effect, Mapping) or not _valid_lifecycle_effect(
                action, effect
            ):
                continue
            position_id = effect.get("broker_position_id") or command.get("position_id")
            if not isinstance(position_id, str) or not position_id:
                continue
            position = by_id.get(position_id)
            expected_version = effect.get("expected_position_version")
            if (
                isinstance(expected_version, bool)
                or not isinstance(expected_version, int)
                or expected_version < 0
                or command.get("expected_position_version") != expected_version
            ):
                continue

            proof: dict[str, Any] | None = None
            applied: bool | None = None
            if action == "MODIFY_SL" and position is not None:
                previous = _effect_decimal(effect, "previous_sl")
                target = _effect_decimal(effect, "target_sl")
                if previous is None or target is None:
                    continue
                observed_sl = Decimal(str(position.current_sl))
                if observed_sl == target and position.position_version > expected_version:
                    applied = True
                elif (
                    observed_sl == previous
                    and position.position_version == expected_version
                ):
                    applied = False
                proof = {
                    "snapshot_id": self._runtime.evidence.snapshot_id,
                    "broker_position_id": position_id,
                    "observed_sl": str(observed_sl),
                    "observed_version": position.position_version,
                    "expected_version": expected_version,
                }
            elif action == "PARTIAL_CLOSE" and position is not None:
                previous = _effect_decimal(effect, "previous_remaining_volume")
                target = _effect_decimal(effect, "target_remaining_volume")
                target_close = _effect_decimal(effect, "target_close_volume")
                if (
                    previous is None
                    or target is None
                    or target_close is None
                    or not Decimal("0") < target < previous
                    or target_close != previous - target
                ):
                    continue
                deals = _closing_deals_since(
                    self._ledger.get_trade_deals(command["trade_id"]),
                    position_id,
                    command["command_id"],
                )
                closed_volume = sum(
                    (Decimal(str(item["volume"])) for item in deals), Decimal("0")
                )
                observed_remaining = Decimal(str(position.remaining_volume))
                if (
                    observed_remaining == target
                    and position.position_version > expected_version
                    and closed_volume == target_close
                ):
                    applied = True
                elif (
                    observed_remaining == previous
                    and position.position_version == expected_version
                    and not deals
                ):
                    applied = False
                proof = {
                    "broker_position_id": position_id,
                    "observed_remaining_volume": str(observed_remaining),
                    "observed_version": position.position_version,
                    "closing_deal_ids": [item["deal_id"] for item in deals],
                    "closed_volume": str(closed_volume),
                }
            elif action == "CLOSE":
                previous = _effect_decimal(effect, "previous_remaining_volume")
                target = _effect_decimal(effect, "target_remaining_volume")
                if previous is None or previous <= 0 or target != 0:
                    continue
                deals = _closing_deals_since(
                    self._ledger.get_trade_deals(command["trade_id"]),
                    position_id,
                    command["command_id"],
                )
                if position is None and deals:
                    closed_volume = sum(
                        (Decimal(str(item["volume"])) for item in deals), Decimal("0")
                    )
                    if closed_volume == previous:
                        last = deals[-1]
                        close_event = TradeEventRecord(
                            event_id=f"{command['command_id']}:reconciled-close",
                            trade_id=command["trade_id"],
                            event_type="POSITION_CLOSED_RECONCILED",
                            source="reconciliation",
                            correlation_id=command["command_id"],
                            occurred_at=last["occurred_at"],
                            broker_timestamp=last["occurred_at"],
                            payload={
                                "closing_deal_ids": [item["deal_id"] for item in deals]
                            },
                        )
                        self._ledger.close_trade_from_recorded_deals_atomic(
                            command["trade_id"],
                            event=close_event,
                            position_id=position_id,
                            exit_price=last["price"],
                            closed_at=last["occurred_at"],
                            command_id=command["command_id"],
                            command_terminal_status="RECONCILED_APPLIED",
                            outbox=self._outbox_for(close_event),
                        )
                        applied = True
                        proof = {
                            "broker_position_absent": True,
                            "closing_deal_ids": [item["deal_id"] for item in deals],
                            "closed_volume": str(closed_volume),
                        }
                elif (
                    position is not None
                    and Decimal(str(position.remaining_volume)) == previous
                    and position.position_version == expected_version
                    and not deals
                ):
                    applied = False
                    proof = {
                        "broker_position_id": position_id,
                        "observed_remaining_volume": str(
                            Decimal(str(position.remaining_volume))
                        ),
                        "observed_version": position.position_version,
                        "closing_deal_ids": [],
                    }

            if applied is None or proof is None:
                continue
            if not applied and self._clock.now_ms() < command["expires_at_ms"]:
                # An unexpired SENT/UNKNOWN command can still be processed by
                # the EA after this snapshot.  Absence is not terminal proof
                # until the pinned envelope has expired.
                continue
            normalized_position: OwnedPositionRecord | None = None
            if position is not None:
                stage = position.lifecycle_stage
                realised_pnl = Decimal(str(position.realised_pnl))
                if applied and action == "PARTIAL_CLOSE":
                    if position.pending_milestone == "TP1":
                        stage = "TP1_CONFIRMED"
                    elif position.pending_milestone == "TP2":
                        stage = "TP2_CONFIRMED"
                    else:
                        # Without the exact milestone, clearing the action could
                        # repeat a scale-out after restart.
                        continue
                    realised_pnl += sum(
                        (
                            Decimal(str(item["profit"]))
                            + Decimal(str(item["commission"]))
                            + Decimal(str(item["swap"]))
                            + Decimal(str(item["fee"]))
                            for item in deals
                        ),
                        Decimal("0"),
                    )
                elif (
                    applied
                    and action == "MODIFY_SL"
                    and position.pending_milestone == "TRAILING"
                ):
                    stage = "TRAILING"
                normalized_position = replace(
                    position,
                    realised_pnl=realised_pnl,
                    lifecycle_stage=stage,
                    pending_action=None,
                    pending_milestone=None,
                    pending_command_id=None,
                    pending_close_volume=Decimal("0"),
                    pending_filled_volume=Decimal("0"),
                )
            if action != "CLOSE" or not applied:
                self._ledger.resolve_command_reconciliation(
                    command["command_id"],
                    applied=applied,
                    resolved_at_ms=self._clock.now_ms(),
                    proof=proof,
                    position_id=position_id,
                    position=normalized_position,
                )
            if normalized_position is not None:
                normalized[position_id] = normalized_position
            issues.append(
                self._issue(
                    run_id,
                    "LIFECYCLE_COMMAND_RECONCILED_APPLIED"
                    if applied
                    else "LIFECYCLE_COMMAND_RECONCILED_NOT_APPLIED",
                    IssueSeverity.INFO,
                    "CLEAR_ACTION_LOCK_FROM_EXACT_BROKER_PROOF",
                    True,
                    broker_position_id=position_id,
                    trade_id=command["trade_id"],
                    detail={"command_id": command["command_id"], "proof": proof},
                )
            )
        return tuple(normalized.values())

    @staticmethod
    def _partition_positions(
        snapshot: AuthoritativeBrokerSnapshot,
        expected: ExpectedBrokerIdentity,
    ) -> tuple[tuple[OwnedPositionRecord, ...], tuple[OwnedPositionRecord, ...]]:
        owned = tuple(
            item
            for item in snapshot.positions
            if item.symbol == expected.symbol and item.magic == expected.magic
        )
        foreign = tuple(item for item in snapshot.positions if item not in owned)
        return owned, foreign

    @staticmethod
    def _partition_orders(
        snapshot: AuthoritativeBrokerSnapshot,
        expected: ExpectedBrokerIdentity,
    ) -> tuple[tuple[PendingOrderRecord, ...], tuple[PendingOrderRecord, ...]]:
        owned = tuple(
            item
            for item in snapshot.pending_orders
            if item.symbol == expected.symbol and item.magic == expected.magic
        )
        foreign = tuple(item for item in snapshot.pending_orders if item not in owned)
        return owned, foreign

    def _compare_positions(
        self,
        run_id: str,
        ledger_rows: list[dict[str, Any]],
        broker_positions: tuple[OwnedPositionRecord, ...],
        commands: list[dict[str, Any]],
        issues: list[ReconciliationIssue],
    ) -> None:
        ledger_by_id = {row["broker_position_id"]: row for row in ledger_rows}
        broker_by_id = {item.broker_position_id: item for item in broker_positions}
        if len(broker_by_id) != len(broker_positions):
            raise BrokerSnapshotError("duplicate broker position identity")
        for position_id, position in broker_by_id.items():
            row = ledger_by_id.get(position_id)
            if row is None:
                issues.append(
                    self._issue(
                        run_id,
                        "BROKER_ONLY_POSITION",
                        IssueSeverity.BLOCKING,
                        "KEEP_RAW_POSITION_QUARANTINED_FOR_EXACT_DEAL_PROOF",
                        False,
                        broker_position_id=position_id,
                        trade_id=position.trade_id,
                    )
                )
                continue
            immutable_drift = _position_immutable_drift(row, position)
            if immutable_drift:
                issues.append(
                    self._issue(
                        run_id,
                        "POSITION_IDENTITY_CONFLICT",
                        IssueSeverity.BLOCKING,
                        "APPLY_BROKER_STATE_AND_HALT",
                        False,
                        broker_position_id=position_id,
                        trade_id=position.trade_id,
                        detail={"fields": immutable_drift},
                    )
                )
            else:
                mutable_drift = _position_mutable_drift(row, position)
                if mutable_drift:
                    issues.append(
                        self._issue(
                            run_id,
                            "POSITION_STATE_REPAIRED",
                            IssueSeverity.INFO,
                            "APPLY_BROKER_AUTHORITY",
                            True,
                            broker_position_id=position_id,
                            trade_id=position.trade_id,
                            detail={"fields": mutable_drift},
                        )
                    )
        for position_id, row in ledger_by_id.items():
            if position_id not in broker_by_id:
                issues.append(
                    self._issue(
                        run_id,
                        "LEDGER_ONLY_POSITION",
                        IssueSeverity.BLOCKING,
                        "REMOVE_OWNED_SNAPSHOT_AND_HALT_FOR_CLOSE_PROOF",
                        False,
                        broker_position_id=position_id,
                        trade_id=row["trade_id"],
                    )
                )

    def _compare_orders(
        self,
        run_id: str,
        ledger_rows: list[dict[str, Any]],
        broker_orders: tuple[PendingOrderRecord, ...],
        broker_positions: tuple[OwnedPositionRecord, ...],
        commands: list[dict[str, Any]],
        issues: list[ReconciliationIssue],
    ) -> None:
        ledger_by_id = {row["broker_order_id"]: row for row in ledger_rows}
        broker_by_id = {item.broker_order_id: item for item in broker_orders}
        if len(broker_by_id) != len(broker_orders):
            raise BrokerSnapshotError("duplicate broker pending-order identity")
        command_by_id = {row["command_id"]: row for row in commands}
        command_trade_ids = {row["trade_id"] for row in commands}
        open_trade_ids = {item.trade_id for item in broker_positions}
        for order_id, order in broker_by_id.items():
            if order_id not in ledger_by_id:
                command = command_by_id.get(order.command_id)
                known = (
                    command is not None
                    and command["action"] == "OPEN"
                    and command["trade_id"] == order.trade_id
                ) or (
                    order.trade_id is not None and order.trade_id in command_trade_ids
                )
                issues.append(
                    self._issue(
                        run_id,
                        "PENDING_ORDER_DISCOVERED"
                        if known
                        else "UNKNOWN_BROKER_PENDING_ORDER",
                        IssueSeverity.INFO if known else IssueSeverity.BLOCKING,
                        "IMPORT_BROKER_PENDING_ORDER"
                        if known
                        else "IMPORT_PENDING_ORDER_AND_HALT",
                        known,
                        trade_id=order.trade_id,
                        detail={"broker_order_id": order_id},
                    )
                )
        for order_id, row in ledger_by_id.items():
            if order_id not in broker_by_id:
                filled = row["trade_id"] in open_trade_ids
                issues.append(
                    self._issue(
                        run_id,
                        "PENDING_ORDER_FILLED_REPAIRED"
                        if filled
                        else "LEDGER_ONLY_PENDING_ORDER",
                        IssueSeverity.INFO if filled else IssueSeverity.BLOCKING,
                        "REPLACE_WITH_BROKER_POSITION"
                        if filled
                        else "REMOVE_SNAPSHOT_AND_HALT_FOR_ORDER_HISTORY",
                        filled,
                        trade_id=row["trade_id"],
                        detail={"broker_order_id": order_id},
                    )
                )

    def _compare_commands(
        self,
        run_id: str,
        commands: list[dict[str, Any]],
        positions: tuple[OwnedPositionRecord, ...],
        orders: tuple[PendingOrderRecord, ...],
        issues: list[ReconciliationIssue],
    ) -> None:
        effect_trade_ids = {item.trade_id for item in positions}
        effect_command_ids = {item.command_id for item in orders}
        effect_trade_ids.update(item.trade_id for item in orders)
        already_reported = {
            item.trade_id
            for item in issues
            if item.discrepancy_type in {
                "UNKNOWN_COMMAND_RESOLVED_OPEN",
                "PENDING_ORDER_DISCOVERED",
            }
        }
        already_reported_command_ids = {
            item.detail.get("command_id")
            for item in issues
            if isinstance(item.detail.get("command_id"), str)
        }
        for command in commands:
            if command["status"] == "CREATED":
                if command["command_id"] in already_reported_command_ids:
                    continue
                issues.append(
                    self._issue(
                        run_id,
                        "UNSENT_COMMAND_RETAINED",
                        IssueSeverity.INFO,
                        "RESUME_EXACT_COMMAND_ONLY_AFTER_READY",
                        True,
                        trade_id=command["trade_id"],
                        detail={"command_id": command["command_id"]},
                    )
                )
                continue
            if command["action"] != "OPEN":
                issues.append(
                    self._issue(
                        run_id,
                        "UNRESOLVED_LIFECYCLE_COMMAND",
                        IssueSeverity.BLOCKING,
                        "KEEP_COMMAND_UNKNOWN_AND_HALT_FOR_EXACT_EFFECT_PROOF",
                        False,
                        trade_id=command["trade_id"],
                        detail={
                            "command_id": command["command_id"],
                            "action": command["action"],
                            "status": command["status"],
                        },
                    )
                )
                continue
            has_effect = (
                command["trade_id"] in effect_trade_ids
                or command["command_id"] in effect_command_ids
            )
            if has_effect:
                if command["trade_id"] not in already_reported:
                    issues.append(
                        self._issue(
                            run_id,
                            "UNRESOLVED_COMMAND_EFFECT_CONFIRMED",
                            IssueSeverity.INFO,
                            "APPLY_BROKER_AUTHORITY",
                            True,
                            trade_id=command["trade_id"],
                            detail={"command_id": command["command_id"]},
                        )
                    )
            else:
                issues.append(
                    self._issue(
                        run_id,
                        "UNRESOLVED_COMMAND_WITHOUT_EFFECT",
                        IssueSeverity.BLOCKING,
                        "KEEP_RESERVATION_UNKNOWN_AND_HALT",
                        False,
                        trade_id=command["trade_id"],
                        detail={
                            "command_id": command["command_id"],
                            "status": command["status"],
                        },
                    )
                )

    def _report_foreign(
        self,
        run_id: str,
        positions: tuple[OwnedPositionRecord, ...],
        orders: tuple[PendingOrderRecord, ...],
        issues: list[ReconciliationIssue],
    ) -> None:
        for position in positions:
            issues.append(
                self._issue(
                    run_id,
                    "FOREIGN_ACCOUNT_POSITION",
                    IssueSeverity.BLOCKING,
                    "LEAVE_UNTOUCHED_AND_HALT_FOR_PORTFOLIO_RISK",
                    False,
                    broker_position_id=position.broker_position_id,
                    trade_id=position.trade_id,
                    detail={"symbol": position.symbol, "magic": position.magic},
                )
            )
        for order in orders:
            issues.append(
                self._issue(
                    run_id,
                    "FOREIGN_ACCOUNT_PENDING_ORDER",
                    IssueSeverity.BLOCKING,
                    "LEAVE_UNTOUCHED_AND_HALT_FOR_PORTFOLIO_RISK",
                    False,
                    trade_id=order.trade_id,
                    detail={
                        "broker_order_id": order.broker_order_id,
                        "symbol": order.symbol,
                        "magic": order.magic,
                    },
                )
            )

    @staticmethod
    def _issue(
        run_id: str,
        discrepancy_type: str,
        severity: IssueSeverity,
        action: str,
        resolved: bool,
        *,
        broker_position_id: str | None = None,
        trade_id: str | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> ReconciliationIssue:
        material = json.dumps(
            {
                "run_id": run_id,
                "type": discrepancy_type,
                "broker_position_id": broker_position_id,
                "trade_id": trade_id,
                "detail": _jsonable(detail or {}),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        issue_id = hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]
        return ReconciliationIssue(
            issue_id=issue_id,
            discrepancy_type=discrepancy_type,
            severity=severity,
            action=action,
            resolved=resolved,
            broker_position_id=broker_position_id,
            trade_id=trade_id,
            detail=detail or {},
        )


def _snapshot_hash(snapshot: AuthoritativeBrokerSnapshot) -> str:
    payload = {
        "snapshot_id": snapshot.snapshot_id,
        "identity": asdict(snapshot.identity),
        "risk": asdict(snapshot.risk),
        "positions": [asdict(item) for item in snapshot.positions],
        "pending_orders": [asdict(item) for item in snapshot.pending_orders],
        "captured_at_ms": snapshot.captured_at_ms,
        "payload": snapshot.payload,
    }
    encoded = json.dumps(
        _jsonable(payload), sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _position_immutable_drift(
    row: Mapping[str, Any], position: OwnedPositionRecord
) -> list[str]:
    expected = {
        "trade_id": position.trade_id,
        "position_ticket": position.position_ticket,
        "symbol": position.symbol,
        "magic": position.magic,
        "direction": position.direction,
        "initial_volume": _decimal(position.initial_volume),
        "entry_price": _decimal(position.entry_price),
        "original_sl": _decimal(position.original_sl),
        "initial_risk_amount": _decimal(position.initial_risk_amount),
        "opened_at_ms": position.opened_at_ms,
    }
    return [name for name, value in expected.items() if row[name] != value]


def _position_mutable_drift(
    row: Mapping[str, Any], position: OwnedPositionRecord
) -> list[str]:
    expected = {
        "remaining_volume": _decimal(position.remaining_volume),
        "current_sl": _decimal(position.current_sl),
        "tp1": _decimal(position.tp1),
        "tp2": _decimal(position.tp2),
        "tp3": _decimal(position.tp3),
        "realised_pnl": _decimal(position.realised_pnl),
        "lifecycle_stage": position.lifecycle_stage,
        "position_version": position.position_version,
        "pending_action": position.pending_action,
        "pending_milestone": position.pending_milestone,
        "pending_command_id": position.pending_command_id,
        "pending_close_volume": _decimal(position.pending_close_volume),
        "pending_filled_volume": _decimal(position.pending_filled_volume),
    }
    return [name for name, value in expected.items() if row[name] != value]


def _decimal(value: Any) -> str:
    result = Decimal(str(value))
    if result == 0:
        return "0"
    return format(result.normalize(), "f")


def _canonical_trade_decimal(row: Mapping[str, Any], name: str) -> Decimal:
    value = row.get(f"{name}_decimal")
    if value is None:
        raise LedgerError(f"trade lacks canonical decimal field {name!r}")
    try:
        result = Decimal(value)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise LedgerError(f"trade canonical decimal field {name!r} is invalid") from exc
    if not result.is_finite():
        raise LedgerError(f"trade canonical decimal field {name!r} is not finite")
    return result


def _mapping_decimal(row: Mapping[str, Any], name: str) -> Decimal:
    try:
        result = Decimal(str(row.get(name)))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise LedgerError(f"record decimal field {name!r} is invalid") from exc
    if not result.is_finite():
        raise LedgerError(f"record decimal field {name!r} is not finite")
    return result


def _exact_millisecond(value: Any) -> int:
    try:
        milliseconds = Decimal(str(value)) * Decimal("1000")
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise LedgerError("broker deal time is invalid") from exc
    integral = milliseconds.to_integral_value()
    if not milliseconds.is_finite() or milliseconds != integral or integral < 0:
        raise LedgerError("broker deal time is not an exact millisecond")
    return int(integral)


def _exact_open_effect(
    effect: Any, expected: Mapping[str, Any]
) -> bool:
    if not isinstance(effect, Mapping) or set(effect) != set(expected):
        return False
    decimal_names = {
        "initial_volume",
        "original_sl",
        "tp1",
        "tp2",
        "tp3",
        "initial_risk_amount",
    }
    for name, value in expected.items():
        if name in decimal_names:
            try:
                if Decimal(str(effect.get(name))) != Decimal(str(value)):
                    return False
            except (InvalidOperation, ValueError, TypeError):
                return False
        elif effect.get(name) != value:
            return False
    return True


def _open_payload_matches(
    payload: Any, expected: Mapping[str, Any]
) -> bool:
    """Check the broker-bound OPEN economics without inventing optional fields."""

    if not isinstance(payload, Mapping) or not set(expected) <= set(payload):
        return False
    decimal_names = {
        "volume",
        "risk_amount",
        "entry",
        "sl",
        "tp1",
        "tp2",
        "tp3",
    }
    for name, value in expected.items():
        if name in decimal_names:
            try:
                if Decimal(str(payload.get(name))) != Decimal(str(value)):
                    return False
            except (InvalidOperation, ValueError, TypeError):
                return False
        elif payload.get(name) != value:
            return False
    return True


def _is_raw_quarantined_position(position: OwnedPositionRecord) -> bool:
    """Recognize only the gateway's non-inferred raw exposure shape."""

    return (
        position.trade_id is None
        and position.broker_order_id is None
        and position.lifecycle_stage == "QUARANTINED"
        and Decimal(str(position.initial_volume))
        == Decimal(str(position.remaining_volume))
        and Decimal(str(position.remaining_volume)) > 0
        and Decimal(str(position.original_sl)) == 0
        and Decimal(str(position.tp1)) == 0
        and Decimal(str(position.tp2)) == 0
        and Decimal(str(position.initial_risk_amount)) == 0
        and Decimal(str(position.current_sl)) > 0
        and Decimal(str(position.tp3)) > 0
        and position.pending_action is None
        and position.pending_milestone is None
        and position.pending_command_id is None
        and Decimal(str(position.pending_close_volume)) == 0
        and Decimal(str(position.pending_filled_volume)) == 0
    )


def _effect_decimal(effect: Mapping[str, Any], key: str) -> Decimal | None:
    try:
        value = Decimal(str(effect[key]))
    except (InvalidOperation, KeyError, ValueError, TypeError):
        return None
    return value if value.is_finite() else None


def _valid_lifecycle_effect(action: str, effect: Mapping[str, Any]) -> bool:
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
    }.get(action)
    return (
        required is not None
        and set(effect) == required
        and effect.get("action") == action
    )


def _closing_deals_since(
    deals: list[dict[str, Any]], position_id: str, command_id: str
) -> list[dict[str, Any]]:
    return [
        item
        for item in deals
        if item["position_id"] == position_id
        and item.get("command_id") == command_id
        and _is_closing_deal(item)
    ]


def _broker_triggered_close_proof(
    *,
    trade: Mapping[str, Any],
    position: Mapping[str, Any],
    deals: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any], str] | None:
    """Return an exact broker SL/TP close proof, otherwise fail closed."""

    if trade.get("status") not in {
        "OPEN",
        "PARTIALLY_CLOSED",
        "CLOSE_PENDING",
        "RECONCILIATION_REQUIRED",
    }:
        return None
    if (
        trade.get("trade_id") != position.get("trade_id")
        or trade.get("broker_position_id") != position.get("broker_position_id")
        or trade.get("account_id") != position.get("account_id")
    ):
        return None
    filled_volume_text = trade.get("filled_volume_decimal")
    opened_at = trade.get("opened_at")
    if filled_volume_text is None or opened_at is None:
        return None
    filled_volume = Decimal(str(filled_volume_text))
    if (
        filled_volume <= 0
        or filled_volume != Decimal(str(position.get("initial_volume")))
    ):
        return None

    position_id = position["broker_position_id"]
    position_deals = [
        item
        for item in deals
        if item.get("position_id") == position_id
        and item.get("trade_id") == trade["trade_id"]
        and item.get("account_id") == trade["account_id"]
    ]
    closing_deals = [item for item in position_deals if _is_closing_deal(item)]
    if not closing_deals:
        return None
    if any(float(item["occurred_at"]) < float(opened_at) for item in closing_deals):
        return None
    closed_volume = sum(
        (Decimal(str(item["volume"])) for item in closing_deals), Decimal("0")
    )
    if closed_volume != filled_volume:
        return None

    final_deal = closing_deals[-1]
    payload = final_deal.get("payload")
    if not isinstance(payload, Mapping):
        return None
    reason = str(payload.get("reason", "")).strip().upper()
    if reason not in {"SL", "TP"}:
        return None
    if final_deal.get("command_id") is not None:
        return None
    if payload.get("attribution_status") != "VERIFIED":
        return None
    if not isinstance(final_deal.get("order_id"), str) or not final_deal["order_id"]:
        return None
    return closing_deals, final_deal, reason


def _is_closing_deal(deal: Mapping[str, Any]) -> bool:
    closing_types = {
        "OUT",
        "OUT_BY",
        "CLOSE",
        "EXIT",
        "POSITION_CLOSED",
        "PARTIAL_CLOSE",
        "ENTRY_OUT",
        "ENTRY_OUT_BY",
    }
    deal_type = str(deal.get("deal_type", "")).upper()
    return deal_type in closing_types or deal_type.endswith("_OUT")


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return _decimal(value)
    if isinstance(value, StrEnum):
        return value.value
    return value


__all__ = [
    "AuthoritativeBrokerSnapshot",
    "BrokerDealPage",
    "BrokerRiskSnapshot",
    "BrokerSnapshotError",
    "IssueSeverity",
    "ReconciliationBrokerPort",
    "ReconciliationError",
    "ReconciliationIssue",
    "ReconciliationLedgerError",
    "ReconciliationReport",
    "ReconciliationService",
]
