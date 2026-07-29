"""Durable execution service for the canonical QUANT-SWARM control plane.

No command reaches ``BrokerPort`` until its trade, attribution, risk
reservation and stable command envelope have committed.  The SENT transition
also commits before the send call.  Consequently a crash or ambiguous send can
only produce UNKNOWN -- never a silently released risk reservation.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any, Callable, Mapping, Protocol, Sequence, runtime_checkable

from .agent_roster import AGENT_BY_ID
from .domain_models import (
    BrokerCommand,
    BrokerEvent,
    BrokerEventType,
    BrokerPosition,
    Clock,
    CommandAction,
    CommandStatus,
    Direction,
    RiskReservation,
    TradeIntent,
)
from .runtime_state import RuntimeStateMachine
from .trade_ledger import (
    BrokerCommandRecord,
    BrokerDealRecord,
    LedgerConflictError,
    LedgerError,
    OutboxRecord,
    OwnedPositionRecord,
    RiskReservationRecord,
    TradeAttributionRecord,
    TradeEventRecord,
    TradeLedger,
    TradeRecord,
)


class ExecutionError(RuntimeError):
    """Base execution-boundary error."""


class ExecutionValidationError(ExecutionError):
    """An execution plan is internally inconsistent and was not persisted."""


class ExecutionLedgerError(ExecutionError):
    """The authoritative ledger failed and the runtime was halted."""


class ExecutionConflictError(ExecutionError):
    """A lifecycle CAS lost to newer durable state; reload without halting."""


class BrokerSendUncertainError(ExecutionError):
    """The broker send outcome is unknown and reconciliation is mandatory."""


class SubmissionState(StrEnum):
    SENT_AWAITING_ACK = "SENT_AWAITING_ACK"
    EXACT_RETRY_AWAITING_ACK = "EXACT_RETRY_AWAITING_ACK"


@dataclass(frozen=True, slots=True)
class OutboundCommand:
    command_id: str
    trade_id: str
    reservation_id: str
    action: CommandAction
    connection_epoch: str
    sequence: int
    created_at_ms: int
    expires_at_ms: int
    position_id: str | None
    expected_position_version: int | None
    expected_effect: Mapping[str, Any]
    payload: Mapping[str, Any]


@runtime_checkable
class BrokerPort(Protocol):
    async def send(self, command: OutboundCommand) -> None:
        """Attempt one send; absence of an exception is not an execution ACK."""


@dataclass(frozen=True, slots=True)
class OpenExecutionPlan:
    intent: TradeIntent
    reservation: RiskReservation
    command: BrokerCommand
    account_id: str
    currency: str
    connection_epoch: str
    sequence: int
    expected_effect: Mapping[str, Any]
    max_total_risk: Decimal
    max_open_positions: int
    outbox: tuple[OutboxRecord, ...] = ()

    def __post_init__(self) -> None:
        for name in ("account_id", "currency", "connection_epoch"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be non-empty")
        if self.sequence < 1:
            raise ValueError("sequence must be at least 1")
        if not self.expected_effect:
            raise ValueError("expected_effect must be explicit")
        object.__setattr__(self, "max_total_risk", Decimal(str(self.max_total_risk)))
        if self.max_total_risk <= 0 or not 1 <= self.max_open_positions <= 3:
            raise ValueError("risk must be positive and position limit must be 1-3")
        object.__setattr__(self, "outbox", tuple(self.outbox))
        if any(not isinstance(item, OutboxRecord) for item in self.outbox):
            raise ValueError("open outbox records must be canonical OutboxRecord items")


@dataclass(frozen=True, slots=True)
class LifecycleExecutionPlan:
    command: BrokerCommand
    position: BrokerPosition
    account_id: str
    reservation_id: str
    connection_epoch: str
    sequence: int
    expected_effect: Mapping[str, Any]
    broker_order_id: str | None = None
    outbox: tuple[OutboxRecord, ...] = ()

    def __post_init__(self) -> None:
        for name in ("account_id", "reservation_id", "connection_epoch"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValueError(f"{name} must be non-empty")
        if self.sequence < 1:
            raise ValueError("sequence must be at least 1")
        if not isinstance(self.expected_effect, Mapping) or not self.expected_effect:
            raise ValueError("expected_effect must be an explicit mapping")
        object.__setattr__(self, "outbox", tuple(self.outbox))
        if any(not isinstance(item, OutboxRecord) for item in self.outbox):
            raise ValueError("lifecycle outbox records must be canonical OutboxRecord items")


class ExecutionService:
    """Orchestrates durable command state around a pure injected broker port."""

    def __init__(
        self,
        *,
        ledger: TradeLedger,
        runtime: RuntimeStateMachine,
        broker: BrokerPort,
        clock: Clock,
        outbox_projector: Callable[
            [TradeEventRecord], Sequence[OutboxRecord]
        ] | None = None,
    ):
        if not isinstance(broker, BrokerPort):
            raise TypeError("broker must implement async send()")
        self._ledger = ledger
        self._runtime = runtime
        self._broker = broker
        self._clock = clock
        self._outbox_projector = outbox_projector

    def reserve_open(self, plan: OpenExecutionPlan) -> bool:
        """Commit a complete CREATED/RESERVED plan; only READY may call it."""

        self._runtime.require_ready("risk reservation")
        self._validate_open_plan(plan)
        trade, attributions, reservation, command = self._ledger_records(plan)
        created_event = self._open_created_event(plan)
        try:
            return self._ledger.reserve_open_command_atomic(
                trade,
                attributions,
                reservation,
                command,
                max_total_risk=plan.max_total_risk,
                max_open_positions=plan.max_open_positions,
                outbox=self._outbox_for(created_event, plan.outbox),
            )
        except LedgerConflictError as exc:
            raise ExecutionConflictError(str(exc)) from exc
        except LedgerError as exc:
            self._halt_for_ledger(exc)
            raise ExecutionLedgerError(str(exc)) from exc

    async def submit_open(self, plan: OpenExecutionPlan) -> SubmissionState:
        """Reserve, mark SENT durably, then invoke the broker transport once."""

        inserted = self.reserve_open(plan)
        outbound = self._outbound(plan)
        try:
            sent_at_ms = self._clock.now_ms()
            sent = self._ledger.mark_command_sent(
                plan.command.command_id,
                sent_at_ms=sent_at_ms,
                outbox=self._outbox_for(
                    self._sent_event(
                        command_id=plan.command.command_id,
                        trade_id=plan.command.trade_id,
                        connection_epoch=plan.connection_epoch,
                        sequence=plan.sequence,
                        occurred_at_ms=sent_at_ms,
                    )
                ),
            )
        except LedgerError as exc:
            self._halt_for_ledger(exc)
            raise ExecutionLedgerError(str(exc)) from exc
        if not sent:
            # An exact retry after SENT must never send a duplicate command.
            return SubmissionState.EXACT_RETRY_AWAITING_ACK
        try:
            await self._broker.send(outbound)
        except BaseException as exc:
            # Cancellation can arrive after bytes reached the socket too.  Mark
            # UNKNOWN before propagating so risk remains reserved on every path.
            try:
                self._ledger.mark_command_unknown(
                    plan.command.command_id,
                    unknown_at_ms=self._clock.now_ms(),
                    reason=f"broker send outcome uncertain: {type(exc).__name__}",
                )
            except LedgerError as ledger_exc:
                self._halt_for_ledger(ledger_exc)
                raise ExecutionLedgerError(str(ledger_exc)) from ledger_exc
            self._add_runtime_blocker(f"UNKNOWN_COMMAND:{plan.command.command_id}")
            if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                raise
            raise BrokerSendUncertainError(
                f"command {plan.command.command_id} requires reconciliation"
            ) from exc
        return (
            SubmissionState.SENT_AWAITING_ACK
            if inserted
            else SubmissionState.EXACT_RETRY_AWAITING_ACK
        )

    def reserve_lifecycle(self, plan: LifecycleExecutionPlan) -> bool:
        """Commit CREATED plus the position action lock without touching transport."""

        self._runtime.require_protection_ready("lifecycle reservation")
        command = plan.command
        if command.action is CommandAction.OPEN or command.status is not CommandStatus.CREATED:
            raise ExecutionValidationError("lifecycle command must be non-OPEN/CREATED")
        if command.trade_id != plan.position.trade_id:
            raise ExecutionValidationError("lifecycle command/position trade IDs disagree")
        if command.position_id != plan.position.broker_position_id:
            raise ExecutionValidationError("lifecycle command must pin broker position ID")
        if command.expected_position_version != plan.position.position_version:
            raise ExecutionValidationError("lifecycle command must pin position version")
        canonical_payload = canonical_lifecycle_payload(
            command,
            reservation_id=plan.reservation_id,
            expected_effect=plan.expected_effect,
        )
        differences = _contract_difference(command.payload, canonical_payload)
        if differences:
            raise ExecutionValidationError(
                "lifecycle command payload is not canonical: " + ", ".join(differences)
            )
        record = BrokerCommandRecord(
            command_id=command.command_id,
            trade_id=command.trade_id,
            reservation_id=plan.reservation_id,
            action=command.action.value,
            status=command.status.value,
            expected_effect=_thaw(plan.expected_effect),
            connection_epoch=plan.connection_epoch,
            sequence=plan.sequence,
            created_at_ms=command.created_at_ms,
            expires_at_ms=command.expires_at_ms,
            payload=_thaw(command.payload),
            position_id=command.position_id,
            expected_position_version=command.expected_position_version,
        )
        stored_position = self._owned_position(
            plan.position,
            account_id=plan.account_id,
            symbol=plan.position.symbol,
            magic=plan.position.magic,
            broker_order_id=plan.broker_order_id,
            updated_at_ms=command.created_at_ms,
        )
        created_event = self._lifecycle_created_event(plan)
        try:
            return self._ledger.create_lifecycle_command_atomic(
                record,
                position=stored_position,
                outbox=self._outbox_for(created_event, plan.outbox),
            )
        except LedgerConflictError as exc:
            raise ExecutionConflictError(str(exc)) from exc
        except LedgerError as exc:
            self._halt_for_ledger(exc)
            raise ExecutionLedgerError(str(exc)) from exc

    async def submit_lifecycle(
        self, plan: LifecycleExecutionPlan
    ) -> SubmissionState:
        """Persist lifecycle action state, commit SENT, then touch transport."""

        inserted = self.reserve_lifecycle(plan)
        command = plan.command
        try:
            sent_at_ms = self._clock.now_ms()
            sent = self._ledger.mark_command_sent(
                command.command_id,
                sent_at_ms=sent_at_ms,
                outbox=self._outbox_for(
                    self._sent_event(
                        command_id=command.command_id,
                        trade_id=command.trade_id,
                        connection_epoch=plan.connection_epoch,
                        sequence=plan.sequence,
                        occurred_at_ms=sent_at_ms,
                    )
                ),
            )
        except LedgerError as exc:
            self._halt_for_ledger(exc)
            raise ExecutionLedgerError(str(exc)) from exc
        if not sent:
            return SubmissionState.EXACT_RETRY_AWAITING_ACK
        outbound = OutboundCommand(
            command_id=command.command_id,
            trade_id=command.trade_id,
            reservation_id=plan.reservation_id,
            action=command.action,
            connection_epoch=plan.connection_epoch,
            sequence=plan.sequence,
            created_at_ms=command.created_at_ms,
            expires_at_ms=command.expires_at_ms,
            position_id=command.position_id,
            expected_position_version=command.expected_position_version,
            expected_effect=_thaw(plan.expected_effect),
            payload=_thaw(command.payload),
        )
        try:
            await self._broker.send(outbound)
        except BaseException as exc:
            try:
                self._ledger.mark_command_unknown(
                    command.command_id,
                    unknown_at_ms=self._clock.now_ms(),
                    reason=f"broker send outcome uncertain: {type(exc).__name__}",
                )
            except LedgerError as ledger_exc:
                self._halt_for_ledger(ledger_exc)
                raise ExecutionLedgerError(str(ledger_exc)) from ledger_exc
            self._add_runtime_blocker(f"UNKNOWN_COMMAND:{command.command_id}")
            if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                raise
            raise BrokerSendUncertainError(
                f"command {command.command_id} requires reconciliation"
            ) from exc
        return (
            SubmissionState.SENT_AWAITING_ACK
            if inserted
            else SubmissionState.EXACT_RETRY_AWAITING_ACK
        )

    def ingest_command_received(
        self,
        command_id: str,
        *,
        received_at_ms: int,
        evidence: Mapping[str, Any] | None = None,
    ) -> bool:
        try:
            command = self._ledger.get_command(command_id)
            if command is None:
                raise ExecutionValidationError("ACK references an unknown durable command")
            stable_evidence: dict[str, Any] = {}
            if evidence is not None:
                if not isinstance(evidence, Mapping):
                    raise ExecutionValidationError("ACK evidence must be a mapping")
                identity = {
                    "command_id": command_id,
                    "trade_id": command["trade_id"],
                    "action": command["action"],
                }
                if any(evidence.get(name) != value for name, value in identity.items()):
                    raise ExecutionValidationError("ACK evidence identity differs")
                if evidence.get("status") not in {
                    "APPLIED",
                    "PARTIALLY_APPLIED",
                    "ACCEPTED",
                }:
                    raise ExecutionValidationError("ACK evidence status is not canonical")
                for name in (
                    "status",
                    "retcode",
                    "retcode_desc",
                    "order_id",
                    "deal_id",
                    "position_id",
                    "position_ticket",
                    "actual_price",
                    "actual_volume",
                ):
                    if name not in evidence:
                        raise ExecutionValidationError(
                            f"ACK evidence lacks {name}"
                        )
                    value = evidence[name]
                    stable_evidence[name] = (
                        str(value)
                        if name in {"actual_price", "actual_volume"}
                        else value
                    )
            event = TradeEventRecord(
                event_id=f"{command_id}:received",
                trade_id=str(command["trade_id"]),
                event_type="BROKER_COMMAND_RECEIVED",
                source="broker",
                correlation_id=command_id,
                occurred_at=received_at_ms / 1_000,
                payload={
                    "command_id": command_id,
                    "ack_evidence": stable_evidence,
                },
            )
            return self._ledger.mark_command_received(
                command_id,
                received_at_ms=received_at_ms,
                evidence=stable_evidence,
                outbox=self._outbox_for(event),
            )
        except LedgerError as exc:
            self._halt_for_ledger(exc)
            raise ExecutionLedgerError(str(exc)) from exc

    def expire_sent_commands(self) -> tuple[str, ...]:
        try:
            expired = self._ledger.expire_sent_commands(now_ms=self._clock.now_ms())
        except LedgerError as exc:
            self._halt_for_ledger(exc)
            raise ExecutionLedgerError(str(exc)) from exc
        for command_id in expired:
            self._add_runtime_blocker(f"UNKNOWN_COMMAND:{command_id}")
        return expired

    def record_broker_deal(self, deal: BrokerDealRecord) -> bool:
        try:
            return self._ledger.record_deal(deal)
        except LedgerError as exc:
            self._halt_for_ledger(exc)
            raise ExecutionLedgerError(str(exc)) from exc

    def register_broker_anomaly(self, blocker: str) -> None:
        """Fence new entries after persisted broker truth violates a planned effect."""

        if not isinstance(blocker, str) or not blocker.strip():
            raise ExecutionValidationError("broker anomaly blocker must be non-empty")
        self._add_runtime_blocker(blocker.strip())

    def ingest_open_transaction(
        self,
        event: BrokerEvent,
        *,
        account_id: str,
        symbol: str,
        magic: int,
        deal: BrokerDealRecord,
        outbox: Sequence[OutboxRecord] = (),
    ) -> bool:
        """Register a fill from durable intent/risk plus exact broker evidence."""

        position = self.position_from_open_event(
            event,
            account_id=account_id,
            symbol=symbol,
            magic=magic,
        )
        return self.ingest_broker_event(
            event,
            account_id=account_id,
            symbol=symbol,
            magic=magic,
            position=position,
            deal=deal,
            outbox=outbox,
        )

    def position_from_open_event(
        self,
        event: BrokerEvent,
        *,
        account_id: str,
        symbol: str,
        magic: int,
    ) -> BrokerPosition:
        """Rebuild immutable position context without trusting wire-owned targets."""

        if event.event_type is not BrokerEventType.POSITION_OPENED:
            raise ExecutionValidationError("open position builder requires POSITION_OPENED")
        required_text = {
            "command_id": event.command_id,
            "trade_id": event.trade_id,
            "broker_position_id": event.broker_position_id,
            "position_ticket": event.position_ticket,
            "order_id": event.order_id,
            "deal_id": event.deal_id,
        }
        if any(
            not isinstance(value, str) or value.strip() in {"", "0"}
            for value in required_text.values()
        ):
            raise ExecutionValidationError("open fill lacks complete broker/trade identity")
        if (
            event.fill_price is None
            or event.fill_volume is None
            or event.remaining_volume is None
            or event.remaining_volume < event.fill_volume
            or event.sl is None
            or event.tp is None
            or event.position_version is None
            or event.position_version < 1
        ):
            raise ExecutionValidationError("open fill lacks exact position economics/version")

        if event.trade_id is None or event.command_id is None:
            raise ExecutionValidationError(
                "open fill is missing its trade or command identifier"
            )
        trade = self._ledger.get_trade(event.trade_id)
        command = self._ledger.get_command(event.command_id)
        reservation = self._ledger.get_reservation_for_trade(event.trade_id)
        if trade is None or command is None or reservation is None:
            raise ExecutionValidationError("open fill lacks durable trade/command/reservation")
        if (
            trade.get("account_id") != account_id
            or trade.get("symbol") != symbol
            or trade.get("magic_number") != magic
            or command.get("trade_id") != event.trade_id
            or command.get("action") != "OPEN"
            or command.get("reservation_id") != reservation.get("reservation_id")
            or reservation.get("account_id") != account_id
            or reservation.get("trade_id") != event.trade_id
            or command.get("status") not in {"SENT", "RECEIVED", "UNKNOWN", "APPLIED"}
            or reservation.get("state")
            not in {"SUBMITTED", "UNKNOWN", "CONSUMED"}
        ):
            raise ExecutionValidationError("open fill durable ownership/state is inconsistent")

        effect = command.get("expected_effect")
        if not isinstance(effect, Mapping):
            raise ExecutionValidationError("open command expected effect is corrupt")
        expected_effect_identity = {
            "event_type": "POSITION_OPENED",
            "trade_id": event.trade_id,
            "reservation_id": reservation["reservation_id"],
            "account_id": account_id,
            "symbol": symbol,
            "magic": magic,
            "direction": trade["direction"],
        }
        if any(effect.get(name) != value for name, value in expected_effect_identity.items()):
            raise ExecutionValidationError("open command expected-effect identity differs")

        def decimal_field(record: Mapping[str, Any], name: str) -> Decimal:
            value = record.get(name)
            try:
                number = Decimal(str(value))
            except (ValueError, TypeError) as exc:
                raise ExecutionValidationError(f"durable {name} is not decimal") from exc
            if not number.is_finite():
                raise ExecutionValidationError(f"durable {name} is not finite")
            return number

        intended_entry = decimal_field(trade, "intended_entry_decimal")
        intended_sl = decimal_field(trade, "intended_sl_decimal")
        tp1 = decimal_field(trade, "tp1_decimal")
        tp2 = decimal_field(trade, "tp2_decimal")
        tp3 = decimal_field(trade, "tp3_decimal")
        reserved_volume = decimal_field(reservation, "volume")
        reserved_risk = decimal_field(reservation, "risk_amount")
        expected_effect_decimals = {
            "initial_volume": reserved_volume,
            "original_sl": intended_sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "initial_risk_amount": reserved_risk,
        }
        try:
            effect_differs = any(
                Decimal(str(effect.get(name))) != value
                for name, value in expected_effect_decimals.items()
            )
        except (ValueError, TypeError) as exc:
            raise ExecutionValidationError(
                "open command expected-effect economics are corrupt"
            ) from exc
        if effect_differs:
            raise ExecutionValidationError("open command expected-effect economics differ")
        if (
            event.fill_volume > reserved_volume
            or event.remaining_volume > reserved_volume
            or event.sl != intended_sl
            or event.tp != tp3
        ):
            raise ExecutionValidationError("broker open fill differs from reserved protection/volume")
        try:
            direction = Direction(str(trade["direction"]))
        except ValueError as exc:
            raise ExecutionValidationError("durable trade direction is not canonical") from exc
        geometry = (
            intended_sl < event.fill_price < tp1 < tp2 < tp3
            if direction is Direction.BUY
            else intended_sl > event.fill_price > tp1 > tp2 > tp3
        )
        planned_distance = abs(intended_entry - intended_sl)
        actual_distance = abs(event.fill_price - intended_sl)
        if not geometry or planned_distance <= 0 or actual_distance <= 0:
            raise ExecutionValidationError("broker open fill has invalid directional geometry")
        actual_risk = (
            reserved_risk
            * event.fill_volume
            / reserved_volume
            * actual_distance
            / planned_distance
        )
        if actual_risk <= 0 or actual_risk > reserved_risk:
            raise ExecutionValidationError("broker fill exceeds its reserved entry-time risk")

        from .domain_models import LifecycleStage

        # This object is authenticated event evidence, not the final aggregate.
        # The ledger recomputes cumulative volume, VWAP and risk from every
        # command-attributed IN deal inside the same transaction that persists
        # the event.  In particular, no pre-read here is authoritative across
        # a crash or an exact duplicate.
        return BrokerPosition(
            trade_id=event.trade_id,
            broker_position_id=str(event.broker_position_id),
            position_ticket=str(event.position_ticket),
            symbol=symbol,
            magic=magic,
            direction=direction,
            initial_volume=event.remaining_volume,
            remaining_volume=event.remaining_volume,
            entry_price=event.fill_price,
            original_sl=event.sl,
            current_sl=event.sl,
            tp1=tp1,
            tp2=tp2,
            tp3=tp3,
            initial_risk_amount=actual_risk,
            lifecycle_stage=LifecycleStage.PRE_TP1,
            position_version=event.position_version,
            opened_at_ms=event.broker_time_ms,
        )

    def ingest_broker_event(
        self,
        event: BrokerEvent,
        *,
        account_id: str,
        symbol: str,
        magic: int,
        position: BrokerPosition | None = None,
        deal: BrokerDealRecord | None = None,
        outbox: Sequence[OutboxRecord] = (),
    ) -> bool:
        """Idempotently apply one broker event and its structured notifications."""

        trade_event = self._trade_event(event)
        projected_outbox = self._outbox_for(trade_event, outbox)
        try:
            if event.event_type is BrokerEventType.POSITION_OPENED:
                if event.command_id is None or position is None:
                    raise ExecutionValidationError(
                        "POSITION_OPENED requires command_id and full position snapshot"
                    )
                return self._ledger.acknowledge_open_command_atomic(
                    event.command_id,
                    event=trade_event,
                    position=self._owned_position(
                        position,
                        account_id=account_id,
                        symbol=symbol,
                        magic=magic,
                        broker_order_id=event.order_id,
                        updated_at_ms=event.received_at_ms,
                    ),
                    deal=deal,
                    outbox=projected_outbox,
                )
            if event.event_type is BrokerEventType.COMMAND_REJECTED:
                if event.command_id is None:
                    raise ExecutionValidationError(
                        "COMMAND_REJECTED requires command_id"
                    )
                return self._ledger.reject_command_atomic(
                    event.command_id,
                    event=trade_event,
                    rejected_at_ms=event.received_at_ms,
                    reason=event.reason or "broker rejected command",
                    outbox=projected_outbox,
                )
            if (
                event.event_type is BrokerEventType.PARTIAL_FILLED
                and event.remaining_volume == 0
            ):
                if event.trade_id is None or event.broker_position_id is None:
                    raise ExecutionValidationError(
                        "zero-remaining partial fill requires trade and position identity"
                    )
                if event.fill_price is None:
                    raise ExecutionValidationError(
                        "zero-remaining partial fill requires exit fill price"
                    )
                return self._ledger.close_trade_from_recorded_deals_atomic(
                    event.trade_id,
                    event=trade_event,
                    position_id=event.broker_position_id,
                    exit_price=event.fill_price,
                    closed_at=event.broker_time_ms / 1_000,
                    command_id=event.command_id,
                    deal=deal,
                    outbox=projected_outbox,
                ).event_inserted
            if event.event_type in {
                BrokerEventType.POSITION_UPDATED,
                BrokerEventType.PARTIAL_FILLED,
            }:
                if position is None:
                    raise ExecutionValidationError(
                        "position update requires full authoritative snapshot"
                    )
                current = self._ledger.get_owned_position(
                    account_id, position.broker_position_id
                )
                if current is None:
                    raise ExecutionValidationError(
                        "position update has no registered identity to preserve"
                    )
                return self._ledger.apply_position_update_atomic(
                    event=trade_event,
                    position=self._owned_position(
                        position,
                        account_id=account_id,
                        symbol=symbol,
                        magic=magic,
                        broker_order_id=current["broker_order_id"],
                        updated_at_ms=event.received_at_ms,
                    ),
                    command_id=event.command_id,
                    deal=deal,
                    outbox=projected_outbox,
                )
            if event.event_type is BrokerEventType.POSITION_CLOSED:
                if event.trade_id is None or event.broker_position_id is None:
                    raise ExecutionValidationError(
                        "POSITION_CLOSED requires trade and broker position identity"
                    )
                if event.fill_price is None:
                    raise ExecutionValidationError("POSITION_CLOSED requires exit fill price")
                return self._ledger.close_trade_from_recorded_deals_atomic(
                        event.trade_id,
                        event=trade_event,
                        position_id=event.broker_position_id,
                        exit_price=event.fill_price,
                        closed_at=event.broker_time_ms / 1_000,
                        command_id=event.command_id,
                        deal=deal,
                        outbox=projected_outbox,
                    ).event_inserted
            return self._ledger.append_event_with_outbox(
                trade_event, projected_outbox
            )
        except ExecutionValidationError:
            raise
        except LedgerError as exc:
            self._halt_for_ledger(exc)
            raise ExecutionLedgerError(str(exc)) from exc

    @staticmethod
    def _open_created_event(plan: OpenExecutionPlan) -> TradeEventRecord:
        return TradeEventRecord(
            event_id=f"{plan.command.command_id}:created",
            trade_id=plan.intent.trade_id,
            event_type="OPEN_COMMAND_RESERVED",
            source="execution_service",
            correlation_id=plan.command.command_id,
            occurred_at=plan.command.created_at_ms / 1_000,
            payload={
                "command_id": plan.command.command_id,
                "reservation_id": plan.reservation.reservation_id,
                "connection_epoch": plan.connection_epoch,
                "sequence": plan.sequence,
                "symbol": plan.intent.symbol,
                "direction": plan.intent.direction.value,
                "entry": plan.intent.levels.entry,
                "sl": plan.intent.levels.sl,
                "tp1": plan.intent.levels.tp1,
                "tp2": plan.intent.levels.tp2,
                "tp3": plan.intent.levels.tp3,
                "volume": plan.reservation.volume,
                "risk_amount": plan.reservation.risk_amount,
                "regime": plan.intent.regime,
                "session": plan.intent.session,
                "contributing_agents": tuple(
                    sorted(item.agent_id for item in plan.intent.contributors)
                ),
                "decision_id": plan.intent.decision_id or None,
                "snapshot_id": plan.intent.snapshot_id or None,
            },
        )

    @staticmethod
    def _lifecycle_created_event(plan: LifecycleExecutionPlan) -> TradeEventRecord:
        milestone = plan.position.pending_milestone
        return TradeEventRecord(
            event_id=f"{plan.command.command_id}:created",
            trade_id=plan.command.trade_id,
            event_type="LIFECYCLE_COMMAND_CREATED",
            source="execution_service",
            correlation_id=plan.command.command_id,
            occurred_at=plan.command.created_at_ms / 1_000,
            payload={
                "action": plan.command.action.value,
                "position_id": plan.command.position_id,
                "milestone": None if milestone is None else milestone.value,
                "expected_effect": _thaw(plan.expected_effect),
            },
        )

    @staticmethod
    def _sent_event(
        *,
        command_id: str,
        trade_id: str,
        connection_epoch: str,
        sequence: int,
        occurred_at_ms: int,
    ) -> TradeEventRecord:
        return TradeEventRecord(
            event_id=f"{command_id}:sent",
            trade_id=trade_id,
            event_type="BROKER_COMMAND_SENT",
            source="execution_service",
            correlation_id=command_id,
            occurred_at=occurred_at_ms / 1_000,
            payload={
                "command_id": command_id,
                "connection_epoch": connection_epoch,
                "sequence": sequence,
            },
        )

    def _outbox_for(
        self,
        event: TradeEventRecord,
        explicit: Sequence[OutboxRecord] = (),
    ) -> tuple[OutboxRecord, ...]:
        projected = (
            ()
            if self._outbox_projector is None
            else tuple(self._outbox_projector(event))
        )
        combined = (*tuple(explicit), *projected)
        by_message: dict[str, OutboxRecord] = {}
        for record in combined:
            if not isinstance(record, OutboxRecord):
                raise ExecutionValidationError(
                    "execution outbox projector returned a non-canonical record"
                )
            if record.event_id != event.event_id:
                raise ExecutionValidationError(
                    "execution outbox projection references another event"
                )
            existing = by_message.get(record.message_id)
            if existing is not None and existing != record:
                raise ExecutionValidationError(
                    "execution outbox reused a message ID with different content"
                )
            by_message[record.message_id] = record
        return tuple(by_message[key] for key in sorted(by_message))

    def _ledger_records(
        self, plan: OpenExecutionPlan
    ) -> tuple[
        TradeRecord,
        tuple[TradeAttributionRecord, ...],
        RiskReservationRecord,
        BrokerCommandRecord,
    ]:
        intent = plan.intent
        levels = intent.levels
        trade = TradeRecord(
            trade_id=intent.trade_id,
            request_id=intent.intent_id,
            account_id=plan.account_id,
            symbol=intent.symbol,
            magic_number=intent.magic,
            direction=intent.direction.value,
            decision_at=intent.created_at_ms / 1_000,
            intended_entry=levels.entry,
            intended_sl=levels.sl,
            tp1=levels.tp1,
            tp2=levels.tp2,
            tp3=levels.tp3,
            requested_volume=plan.reservation.volume,
            initial_risk_currency=plan.reservation.risk_amount,
            currency=plan.currency,
            regime_at_entry=intent.regime,
            session_at_entry=intent.session,
            metadata={
                "fusion_id": intent.fusion_id,
                "decision_id": intent.decision_id or None,
                "snapshot_id": intent.snapshot_id or None,
                "config_hash": intent.config_hash,
                "created_at_ms": intent.created_at_ms,
                "expires_at_ms": intent.expires_at_ms,
            },
            config_hash=intent.config_hash,
            evidence_class="CANONICAL_RUNTIME",
            learning_eligible=True,
        )
        attributions = tuple(
            TradeAttributionRecord(
                attribution_id=f"{intent.trade_id}:agent:{item.agent_id:02d}",
                trade_id=intent.trade_id,
                agent_id=item.agent_id,
                strategy_slug=AGENT_BY_ID[item.agent_id].slug,
                direction=item.direction.value,
                conviction=item.conviction,
                learned_weight=item.weight,
                effective_weight=item.weight,
                signal_at=(
                    item.observed_at_ms / 1_000
                    if item.observed_at_ms > 0
                    else intent.created_at_ms / 1_000
                ),
                tags=item.strategy_tags,
                metadata={
                    "signal_id": item.signal_id,
                    "snapshot_id": item.snapshot_id or intent.snapshot_id or None,
                    "observed_at_ms": item.observed_at_ms or intent.created_at_ms,
                    "decision_id": intent.decision_id or None,
                    "weighted_score": str(item.weighted_score),
                },
            )
            for item in intent.contributors
        )
        reservation = RiskReservationRecord(
            reservation_id=plan.reservation.reservation_id,
            trade_id=plan.reservation.trade_id,
            account_id=plan.reservation.account_id,
            risk_day=plan.reservation.risk_day,
            direction=plan.reservation.direction.value,
            risk_amount=plan.reservation.risk_amount,
            volume=plan.reservation.volume,
            state=plan.reservation.state.value,
            created_at_ms=plan.reservation.created_at_ms,
            expires_at_ms=plan.reservation.expires_at_ms,
        )
        command = BrokerCommandRecord(
            command_id=plan.command.command_id,
            trade_id=plan.command.trade_id,
            reservation_id=plan.reservation.reservation_id,
            action=plan.command.action.value,
            status=plan.command.status.value,
            expected_effect=_thaw(plan.expected_effect),
            connection_epoch=plan.connection_epoch,
            sequence=plan.sequence,
            created_at_ms=plan.command.created_at_ms,
            expires_at_ms=plan.command.expires_at_ms,
            payload=_thaw(plan.command.payload),
            position_id=plan.command.position_id,
            expected_position_version=plan.command.expected_position_version,
        )
        return trade, attributions, reservation, command

    def _outbound(self, plan: OpenExecutionPlan) -> OutboundCommand:
        command = plan.command
        return OutboundCommand(
            command_id=command.command_id,
            trade_id=command.trade_id,
            reservation_id=plan.reservation.reservation_id,
            action=command.action,
            connection_epoch=plan.connection_epoch,
            sequence=plan.sequence,
            created_at_ms=command.created_at_ms,
            expires_at_ms=command.expires_at_ms,
            position_id=command.position_id,
            expected_position_version=command.expected_position_version,
            expected_effect=_thaw(plan.expected_effect),
            payload=_thaw(command.payload),
        )

    @staticmethod
    def _validate_open_plan(plan: OpenExecutionPlan) -> None:
        intent = plan.intent
        reservation = plan.reservation
        command = plan.command
        if not (
            intent.trade_id == reservation.trade_id == command.trade_id
            and reservation.account_id == plan.account_id
        ):
            raise ExecutionValidationError("trade/reservation/command identities disagree")
        if reservation.direction is not intent.direction:
            raise ExecutionValidationError("reservation direction differs from intent")
        if intent.requested_risk_pct > Decimal("1"):
            raise ExecutionValidationError("requested risk cannot exceed the 1% hard cap")
        if command.action is not CommandAction.OPEN or command.status is not CommandStatus.CREATED:
            raise ExecutionValidationError("new execution command must be OPEN/CREATED")
        if plan.connection_epoch.strip() == "":
            raise ExecutionValidationError("connection epoch must be pinned")
        contributors = intent.contributors
        distinct_agents = {item.agent_id for item in contributors}
        if any(not 4 <= agent_id <= 13 for agent_id in distinct_agents):
            raise ExecutionValidationError("strategy contributors must be Agents 04 through 13")
        if len(distinct_agents) < 3:
            raise ExecutionValidationError("at least three distinct strategy agents must agree")
        if len(distinct_agents) != len(contributors):
            raise ExecutionValidationError("one strategy agent cannot vote more than once")
        if any(item.direction is not intent.direction for item in contributors):
            raise ExecutionValidationError("all recorded contributors must support the trade side")
        levels = intent.levels
        if intent.direction is Direction.BUY:
            valid = levels.sl < levels.entry < levels.tp1 < levels.tp2 < levels.tp3
        else:
            valid = levels.sl > levels.entry > levels.tp1 > levels.tp2 > levels.tp3
        if not valid:
            raise ExecutionValidationError("directional SL/TP ordering is invalid")
        if command.expires_at_ms > intent.expires_at_ms:
            raise ExecutionValidationError("command cannot outlive its trade intent")
        if reservation.expires_at_ms < command.expires_at_ms:
            raise ExecutionValidationError("risk reservation must cover command expiry")
        expected_payload = canonical_open_payload(
            intent,
            reservation,
            account_id=plan.account_id,
            currency=plan.currency,
            max_total_risk=plan.max_total_risk,
            max_open_positions=plan.max_open_positions,
        )
        expected_effect = canonical_open_expected_effect(
            intent, reservation, account_id=plan.account_id
        )
        payload_difference = _contract_difference(command.payload, expected_payload)
        if payload_difference:
            raise ExecutionValidationError(
                "OPEN command payload is not canonical: " + ", ".join(payload_difference)
            )
        effect_difference = _contract_difference(plan.expected_effect, expected_effect)
        if effect_difference:
            raise ExecutionValidationError(
                "OPEN expected_effect is not canonical: " + ", ".join(effect_difference)
            )

    @staticmethod
    def _trade_event(event: BrokerEvent) -> TradeEventRecord:
        return TradeEventRecord(
            event_id=event.event_id,
            trade_id=event.trade_id,
            event_type=event.event_type.value,
            source="broker",
            occurred_at=event.received_at_ms / 1_000,
            broker_timestamp=event.broker_time_ms / 1_000,
            correlation_id=event.command_id,
            payload={
                "broker_position_id": event.broker_position_id,
                "position_ticket": event.position_ticket,
                "order_id": event.order_id,
                "deal_id": event.deal_id,
                "reason": event.reason,
                "position_version": event.position_version,
                "fill_price": None
                if event.fill_price is None
                else str(event.fill_price),
                "fill_volume": None
                if event.fill_volume is None
                else str(event.fill_volume),
                "remaining_volume": None
                if event.remaining_volume is None
                else str(event.remaining_volume),
                "sl": None if event.sl is None else str(event.sl),
                "tp": None if event.tp is None else str(event.tp),
                "profit": str(event.profit),
                "commission": str(event.commission),
                "swap": str(event.swap),
                "fee": str(event.fee),
                "net_pnl": str(event.net_pnl),
            },
        )

    @staticmethod
    def _owned_position(
        position: BrokerPosition,
        *,
        account_id: str,
        symbol: str,
        magic: int,
        broker_order_id: str | None,
        updated_at_ms: int,
    ) -> OwnedPositionRecord:
        if position.symbol != symbol or position.magic != magic:
            raise ExecutionValidationError("broker position is outside configured scope")
        return OwnedPositionRecord(
            account_id=account_id,
            trade_id=position.trade_id,
            broker_position_id=position.broker_position_id,
            position_ticket=position.position_ticket,
            broker_order_id=broker_order_id,
            symbol=position.symbol,
            magic=position.magic,
            direction=position.direction.value,
            initial_volume=position.initial_volume,
            remaining_volume=position.remaining_volume,
            entry_price=position.entry_price,
            original_sl=position.original_sl,
            current_sl=position.current_sl,
            tp1=position.tp1,
            tp2=position.tp2,
            tp3=position.tp3,
            initial_risk_amount=position.initial_risk_amount,
            lifecycle_stage=position.lifecycle_stage.value,
            position_version=position.position_version,
            opened_at_ms=position.opened_at_ms,
            updated_at_ms=updated_at_ms,
            realised_pnl=position.realised_pnl,
            pending_action=None
            if position.pending_action is None
            else position.pending_action.value,
            pending_milestone=None
            if position.pending_milestone is None
            else position.pending_milestone.value,
            pending_command_id=position.pending_command_id,
            pending_close_volume=position.pending_close_volume,
            pending_filled_volume=position.pending_filled_volume,
        )

    def _add_runtime_blocker(self, blocker: str) -> None:
        blockers = tuple(dict.fromkeys((*self._runtime.evidence.blocking_diffs, blocker)))
        self._runtime.update_evidence(blocking_diffs=blockers)

    def _halt_for_ledger(self, error: LedgerError) -> None:
        self._runtime.halt(f"authoritative ledger failure: {type(error).__name__}: {error}")


def canonical_open_payload(
    intent: TradeIntent,
    reservation: RiskReservation,
    *,
    account_id: str,
    currency: str,
    max_total_risk: Decimal,
    max_open_positions: int,
) -> dict[str, Any]:
    """Build the one allowed OPEN transport payload.

    Financial values remain ``Decimal`` until the outbound adapter serializes
    them, preventing binary-float equality from weakening the risk boundary.
    """

    return {
        "action": "OPEN",
        "intent_id": intent.intent_id,
        "trade_id": intent.trade_id,
        "reservation_id": reservation.reservation_id,
        "account_id": account_id,
        "currency": currency,
        "symbol": intent.symbol,
        "magic": intent.magic,
        "direction": intent.direction.value,
        "volume": reservation.volume,
        "risk_amount": reservation.risk_amount,
        "requested_risk_pct": intent.requested_risk_pct,
        "entry": intent.levels.entry,
        "sl": intent.levels.sl,
        "tp1": intent.levels.tp1,
        "tp2": intent.levels.tp2,
        "tp3": intent.levels.tp3,
        "max_slippage_points": intent.max_slippage_points,
        "max_total_risk": Decimal(str(max_total_risk)),
        "max_open_positions": max_open_positions,
        "config_hash": intent.config_hash,
    }


def canonical_open_expected_effect(
    intent: TradeIntent,
    reservation: RiskReservation,
    *,
    account_id: str,
) -> dict[str, Any]:
    return {
        "event_type": "POSITION_OPENED",
        "trade_id": intent.trade_id,
        "reservation_id": reservation.reservation_id,
        "account_id": account_id,
        "symbol": intent.symbol,
        "magic": intent.magic,
        "direction": intent.direction.value,
        "initial_volume": reservation.volume,
        "original_sl": intent.levels.sl,
        "tp1": intent.levels.tp1,
        "tp2": intent.levels.tp2,
        "tp3": intent.levels.tp3,
        "initial_risk_amount": reservation.risk_amount,
    }


def canonical_lifecycle_payload(
    command: BrokerCommand,
    *,
    reservation_id: str,
    expected_effect: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "action": command.action.value,
        "command_id": command.command_id,
        "trade_id": command.trade_id,
        "reservation_id": reservation_id,
        "broker_position_id": command.position_id,
        "expected_position_version": command.expected_position_version,
        "expected_effect": expected_effect,
    }


def _contract_difference(
    actual: Any, expected: Any, path: str = "$"
) -> tuple[str, ...]:
    if isinstance(expected, Mapping):
        if not isinstance(actual, Mapping):
            return (f"{path}:expected mapping",)
        differences: list[str] = []
        actual_keys = set(actual)
        expected_keys = set(expected)
        for key in sorted(expected_keys - actual_keys):
            differences.append(f"{path}.{key}:missing")
        for key in sorted(actual_keys - expected_keys):
            differences.append(f"{path}.{key}:unexpected")
        for key in sorted(actual_keys & expected_keys):
            differences.extend(
                _contract_difference(actual[key], expected[key], f"{path}.{key}")
            )
        return tuple(differences)
    if isinstance(expected, Decimal):
        try:
            candidate = Decimal(str(actual))
        except Exception:
            return (f"{path}:not decimal",)
        return () if candidate == expected else (f"{path}:value mismatch",)
    if isinstance(expected, int):
        return (
            ()
            if isinstance(actual, int) and not isinstance(actual, bool) and actual == expected
            else (f"{path}:integer mismatch",)
        )
    return () if actual == expected else (f"{path}:value mismatch",)


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    if isinstance(value, frozenset):
        return sorted((_thaw(item) for item in value), key=repr)
    if isinstance(value, Decimal):
        return str(value)
    return value


__all__ = [
    "BrokerPort",
    "BrokerSendUncertainError",
    "ExecutionError",
    "ExecutionConflictError",
    "ExecutionLedgerError",
    "ExecutionService",
    "ExecutionValidationError",
    "LifecycleExecutionPlan",
    "OpenExecutionPlan",
    "OutboundCommand",
    "SubmissionState",
    "canonical_open_expected_effect",
    "canonical_open_payload",
    "canonical_lifecycle_payload",
]
