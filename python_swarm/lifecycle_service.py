"""End-to-end lifecycle orchestration for canonical owned positions.

Planning remains pure in :mod:`trading_policies`.  This application service
adds authenticated evidence checks, deterministic command identities, durable
reservations and broker-confirmed transition ingestion.  Importing the module
performs no I/O.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from enum import StrEnum
from typing import Any, Callable, Mapping, Sequence

from .broker_gateway import CompletedBar
from .domain_models import (
    BrokerCommand,
    BrokerEvent,
    BrokerEventType,
    BrokerPosition,
    Clock,
    CommandAction,
    CommandStatus,
    Direction,
    LifecycleActionKind,
    LifecycleStage,
    Milestone,
    QuoteSnapshot,
    SymbolSnapshot,
)
from .execution_service import (
    ExecutionService,
    LifecycleExecutionPlan,
    SubmissionState,
    canonical_lifecycle_payload,
)
from .trade_ledger import (
    BrokerDealRecord,
    OutboxRecord,
    TradeEventRecord,
    TradeLedger,
)
from .trading_policies import LifecycleAction, LifecycleDecision, LifecyclePolicy


class LifecycleServiceError(RuntimeError):
    """Base error at the lifecycle application boundary."""


class LifecycleEvidenceError(LifecycleServiceError):
    """Authenticated evidence is absent, stale or internally inconsistent."""


class LifecycleStateError(LifecycleServiceError):
    """Durable lifecycle state cannot support the requested transition."""


class LifecycleDisposition(StrEnum):
    NO_ACTION = "NO_ACTION"
    VIRTUAL_MILESTONE_APPLIED = "VIRTUAL_MILESTONE_APPLIED"
    VIRTUAL_MILESTONE_EXACT_RETRY = "VIRTUAL_MILESTONE_EXACT_RETRY"
    SENT_AWAITING_ACK = "SENT_AWAITING_ACK"
    EXACT_RETRY_AWAITING_ACK = "EXACT_RETRY_AWAITING_ACK"
    BROKER_EVENT_APPLIED = "BROKER_EVENT_APPLIED"
    BROKER_EVENT_DUPLICATE = "BROKER_EVENT_DUPLICATE"


@dataclass(frozen=True, slots=True)
class LifecycleServiceConfig:
    command_ttl_ms: int = 15_000
    max_quote_age_ms: int = 2_500
    max_future_skew_ms: int = 250
    max_sequence: int = (1 << 63) - 1

    def __post_init__(self) -> None:
        for name in (
            "command_ttl_ms",
            "max_quote_age_ms",
            "max_future_skew_ms",
            "max_sequence",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.command_ttl_ms <= self.max_future_skew_ms:
            raise ValueError("command TTL must exceed allowed future skew")


@dataclass(frozen=True, slots=True)
class SessionCloseEvidence:
    schedule_id: str
    session: str
    timezone: str
    evaluated_at_ms: int
    due: bool

    def __post_init__(self) -> None:
        for name in ("schedule_id", "session", "timezone"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be non-empty")
        if (
            isinstance(self.evaluated_at_ms, bool)
            or not isinstance(self.evaluated_at_ms, int)
            or self.evaluated_at_ms < 0
            or not isinstance(self.due, bool)
        ):
            raise ValueError("session-close evidence time/due flag is invalid")


@dataclass(frozen=True, slots=True)
class LifecycleResult:
    disposition: LifecycleDisposition
    trade_id: str
    reason: str
    action: LifecycleActionKind | None = None
    milestone: Milestone | None = None
    command_id: str | None = None
    h1_bars_elapsed: int | None = None


class LifecycleService:
    """Connect lifecycle policy decisions to durable execution and broker truth."""

    def __init__(
        self,
        *,
        ledger: TradeLedger,
        execution: ExecutionService,
        policy: LifecyclePolicy,
        clock: Clock,
        config: LifecycleServiceConfig | None = None,
        outbox_projector: Callable[
            [TradeEventRecord], Sequence[OutboxRecord]
        ] | None = None,
    ) -> None:
        self._ledger = ledger
        self._execution = execution
        self._policy = policy
        self._clock = clock
        self.config = config or LifecycleServiceConfig()
        self._outbox_projector = outbox_projector

    async def evaluate(
        self,
        position_record: Mapping[str, Any],
        *,
        account_id: str,
        quote: QuoteSnapshot,
        symbol: SymbolSnapshot,
        connection_epoch: str,
        completed_h1_bars: Sequence[CompletedBar] = (),
        session_close_evidence: SessionCloseEvidence | None = None,
        outbox: Sequence[OutboxRecord] = (),
    ) -> LifecycleResult:
        """Plan at most one action for one exact durable position version."""

        epoch = _required_text(connection_epoch, "connection_epoch")
        account = _required_text(account_id, "account_id")
        position = broker_position_from_ledger(position_record)
        if _required_text(position_record.get("account_id"), "position account_id") != account:
            raise LifecycleEvidenceError("position belongs to another account")
        if position.symbol != quote.symbol or position.symbol != symbol.symbol:
            raise LifecycleEvidenceError("position/quote/symbol scope differs")

        open_command = self._ledger.get_open_command_for_trade(position.trade_id)
        if open_command is None or open_command.get("status") not in {
            "APPLIED",
            "RECONCILED_APPLIED",
        }:
            # The initial broker SL/TP remains live, but scale-outs, break-even
            # and trailing must not race a still-progressing OPEN request whose
            # volume/version can increase under us.
            return LifecycleResult(
                LifecycleDisposition.NO_ACTION,
                position.trade_id,
                "OPEN_FILL_UNRESOLVED",
            )

        now_ms = self._clock.now_ms()
        quote_age = now_ms - quote.captured_at_ms
        if (
            quote_age < -self.config.max_future_skew_ms
            or quote_age > self.config.max_quote_age_ms
        ):
            raise LifecycleEvidenceError("lifecycle quote is stale or future-dated")
        session_close_due = False
        if session_close_evidence is not None:
            if session_close_evidence.evaluated_at_ms != quote.captured_at_ms:
                raise LifecycleEvidenceError(
                    "session-close evidence is not bound to the lifecycle quote"
                )
            session_close_due = session_close_evidence.due
        if position.pending_action is not None:
            return await self._resume_pending(
                position,
                position_record=position_record,
                account_id=account,
                connection_epoch=epoch,
                now_ms=now_ms,
            )
        h1_elapsed = completed_h1_bars_elapsed(
            position,
            completed_h1_bars,
            as_of_ms=quote.captured_at_ms,
        )
        _require_sufficient_h1_history(
            position,
            completed_h1_bars,
            required_closes=self._policy.config.max_h1_bars,
        )
        structural_sl = derive_structural_trailing_sl(
            position,
            completed_h1_bars,
            symbol=symbol,
            left_bars=self._policy.config.structural_swing_left_bars,
            right_bars=self._policy.config.structural_swing_right_bars,
            buffer_points=self._policy.config.structural_buffer_points,
        )

        probe = self._policy.plan(
            position,
            quote,
            symbol,
            "LIFECYCLE-PROBE",
            h1_bars_elapsed=h1_elapsed,
            session_close_due=session_close_due,
            structural_trailing_sl=structural_sl,
        )
        if probe.action is None:
            return LifecycleResult(
                LifecycleDisposition.NO_ACTION,
                position.trade_id,
                probe.reason,
                h1_bars_elapsed=h1_elapsed,
            )
        action = probe.action
        if action.kind is LifecycleActionKind.VIRTUAL_MILESTONE:
            result = self._apply_virtual(
                original=position,
                decision=probe,
                symbol=symbol,
                account_id=account,
                occurred_at_ms=quote.captured_at_ms,
                outbox=outbox,
            )
            return replace(result, h1_bars_elapsed=h1_elapsed)

        effect = _expected_effect(position, action)
        attempt = self._ledger.next_lifecycle_attempt(
            position.trade_id,
            position.broker_position_id,
            position.position_version,
        )
        command_id = _stable_id(
            "lifecycle-command",
            {
                "connection_epoch": epoch,
                "trade_id": position.trade_id,
                "broker_position_id": position.broker_position_id,
                "position_version": position.position_version,
                "attempt": attempt,
                "trigger_quote_at_ms": quote.captured_at_ms,
                "lifecycle_stage": position.lifecycle_stage.value,
                "milestone": action.milestone.value,
                "reason": action.reason,
                "expected_effect": effect,
            },
        )
        # Re-run the pure policy with the final command ID.  This catches any
        # accidental command-ID sensitivity before durable state is touched.
        final = self._policy.plan(
            position,
            quote,
            symbol,
            command_id,
            h1_bars_elapsed=h1_elapsed,
            session_close_due=session_close_due,
            structural_trailing_sl=structural_sl,
        )
        _assert_same_decision(probe, final)
        if final.action is None:
            raise LifecycleStateError(
                "lifecycle re-plan produced no final action"
            )
        final_effect = _expected_effect(position, final.action)
        if _canonical_json(final_effect) != _canonical_json(effect):
            raise LifecycleStateError("lifecycle re-plan changed its expected effect")

        reservation = self._ledger.get_reservation_for_trade(position.trade_id)
        if reservation is None or reservation.get("state") != "CONSUMED":
            raise LifecycleStateError(
                "lifecycle command requires the trade's consumed risk reservation"
            )
        reservation_id = _required_text(
            reservation.get("reservation_id"), "reservation_id"
        )
        sequence = self._ledger.allocate_command_sequence(
            epoch,
            command_id,
            allocated_at_ms=quote.captured_at_ms,
        )
        if (
            isinstance(sequence, bool)
            or not isinstance(sequence, int)
            or not 1 <= sequence <= self.config.max_sequence
        ):
            raise LifecycleStateError("sequence allocator returned an invalid value")
        command = BrokerCommand(
            command_id=command_id,
            trade_id=position.trade_id,
            action=CommandAction(final.action.kind.value),
            status=CommandStatus.CREATED,
            created_at_ms=quote.captured_at_ms,
            expires_at_ms=quote.captured_at_ms + self.config.command_ttl_ms,
            position_id=position.broker_position_id,
            expected_position_version=position.position_version,
        )
        command = replace(
            command,
            payload=canonical_lifecycle_payload(
                command,
                reservation_id=reservation_id,
                expected_effect=final_effect,
            ),
        )
        plan = LifecycleExecutionPlan(
            command=command,
            position=final.position,
            account_id=account,
            reservation_id=reservation_id,
            connection_epoch=epoch,
            sequence=sequence,
            expected_effect=final_effect,
            broker_order_id=_optional_text(position_record.get("broker_order_id")),
            outbox=tuple(outbox),
        )
        submission = await self._execution.submit_lifecycle(plan)
        disposition = (
            LifecycleDisposition.SENT_AWAITING_ACK
            if submission is SubmissionState.SENT_AWAITING_ACK
            else LifecycleDisposition.EXACT_RETRY_AWAITING_ACK
        )
        return LifecycleResult(
            disposition,
            position.trade_id,
            final.reason,
            action=final.action.kind,
            milestone=final.action.milestone,
            command_id=command_id,
            h1_bars_elapsed=h1_elapsed,
        )

    def ingest_broker_event(
        self,
        event: BrokerEvent,
        *,
        account_id: str,
        symbol: str,
        magic: int,
        deal: BrokerDealRecord | None = None,
        outbox: Sequence[OutboxRecord] = (),
    ) -> LifecycleResult:
        """Apply one lifecycle event after its referenced deal is durable.

        POSITION_OPENED belongs to the entry service.  Partial/final fills must
        reference a previously persisted broker deal, ensuring actual-R close
        accounting can never race ahead of authoritative economics.
        """

        if event.event_type is BrokerEventType.POSITION_OPENED:
            raise LifecycleEvidenceError("POSITION_OPENED belongs to entry execution")
        account = _required_text(account_id, "account_id")
        position_id = _required_text(event.broker_position_id, "broker_position_id")
        record = self._ledger.get_owned_position(account, position_id)
        if record is None:
            trade = self._ledger.get_trade(_required_text(event.trade_id, "trade_id"))
            if (
                event.event_type is BrokerEventType.POSITION_CLOSED
                and trade is not None
                and trade.get("status") == "CLOSED"
            ):
                return LifecycleResult(
                    LifecycleDisposition.BROKER_EVENT_DUPLICATE,
                    str(event.trade_id),
                    "POSITION_ALREADY_CLOSED",
                    command_id=event.command_id,
                )
            raise LifecycleStateError("broker event position is not registered")
        position = broker_position_from_ledger(record)
        if position.symbol != symbol or position.magic != magic:
            raise LifecycleEvidenceError("broker event position is outside runtime scope")
        if event.trade_id != position.trade_id:
            raise LifecycleEvidenceError("broker event trade attribution differs")

        if event.event_type in {
            BrokerEventType.PARTIAL_FILLED,
            BrokerEventType.POSITION_CLOSED,
        }:
            deal_id = _required_text(event.deal_id, "fill deal_id")
            deal_evidence: Mapping[str, Any]
            if deal is None:
                deals = self._ledger.get_trade_deals(position.trade_id)
                durable = next(
                    (item for item in deals if item.get("deal_id") == deal_id), None
                )
                if durable is None:
                    raise LifecycleEvidenceError(
                        "fill event arrived before its authoritative broker deal"
                    )
                deal_evidence = durable
            else:
                deal_evidence = _deal_evidence(deal)
            _require_exact_fill_deal(
                event,
                deal_evidence,
                account_id=account,
                trade_id=position.trade_id,
                broker_position_id=position.broker_position_id,
            )

        transition = self._policy.apply_broker_event(position, event)
        if not transition.applied:
            if transition.reason == "DUPLICATE_OR_OLD_EVENT":
                return LifecycleResult(
                    LifecycleDisposition.BROKER_EVENT_DUPLICATE,
                    position.trade_id,
                    transition.reason,
                    command_id=event.command_id,
                )
            raise LifecycleEvidenceError(
                f"broker event cannot advance lifecycle: {transition.reason}"
            )
        updated = (
            transition.position
            if event.event_type
            in {BrokerEventType.POSITION_UPDATED, BrokerEventType.PARTIAL_FILLED}
            and transition.position.remaining_volume > 0
            else None
        )
        inserted = self._execution.ingest_broker_event(
            event,
            account_id=account,
            symbol=symbol,
            magic=magic,
            position=updated,
            deal=deal,
            outbox=outbox,
        )
        if transition.reason == "PARTIAL_OVERFILL_QUARANTINED":
            self._execution.register_broker_anomaly(
                f"LIFECYCLE_OVERFILL:{position.trade_id}"
            )
        return LifecycleResult(
            LifecycleDisposition.BROKER_EVENT_APPLIED
            if inserted
            else LifecycleDisposition.BROKER_EVENT_DUPLICATE,
            position.trade_id,
            transition.reason,
            action=position.pending_action,
            milestone=position.pending_milestone,
            command_id=event.command_id,
        )

    async def _resume_pending(
        self,
        position: BrokerPosition,
        *,
        position_record: Mapping[str, Any],
        account_id: str,
        connection_epoch: str,
        now_ms: int,
    ) -> LifecycleResult:
        command_id = _required_text(position.pending_command_id, "pending_command_id")
        command_row = self._ledger.get_command(command_id)
        if command_row is None:
            raise LifecycleStateError("pending lifecycle action has no durable command")
        if command_row.get("trade_id") != position.trade_id:
            raise LifecycleStateError("pending lifecycle command belongs to another trade")
        if command_row.get("connection_epoch") != connection_epoch:
            raise LifecycleEvidenceError(
                "old-epoch lifecycle command requires fenced reconciliation"
            )
        status = _required_text(command_row.get("status"), "command status")
        if status not in {"CREATED", "SENT", "RECEIVED", "UNKNOWN"}:
            raise LifecycleStateError(
                f"terminal command {status} still owns the lifecycle action lock"
            )
        expires_at_ms = _strict_int(
            command_row.get("expires_at_ms"), "expires_at_ms", minimum=0
        )
        if expires_at_ms <= now_ms:
            raise LifecycleEvidenceError(
                "expired lifecycle command requires fenced reconciliation"
            )
        expected_effect = command_row.get("expected_effect")
        payload = command_row.get("payload")
        if not isinstance(expected_effect, Mapping) or not isinstance(payload, Mapping):
            raise LifecycleStateError("pending lifecycle command envelope is corrupt")
        try:
            action = CommandAction(_required_text(command_row.get("action"), "action"))
        except ValueError as exc:
            raise LifecycleStateError("pending lifecycle action is not canonical") from exc
        command = BrokerCommand(
            command_id=command_id,
            trade_id=position.trade_id,
            action=action,
            # CREATED is the immutable creation envelope; current durable status
            # is recovered independently by ExecutionService/TradeLedger.
            status=CommandStatus.CREATED,
            created_at_ms=_strict_int(
                command_row.get("created_at_ms"), "created_at_ms", minimum=0
            ),
            expires_at_ms=expires_at_ms,
            position_id=_required_text(command_row.get("position_id"), "position_id"),
            expected_position_version=_strict_int(
                command_row.get("expected_position_version"),
                "expected_position_version",
                minimum=0,
            ),
            payload=payload,
        )
        reservation_id = _required_text(
            command_row.get("reservation_id"), "reservation_id"
        )
        plan = LifecycleExecutionPlan(
            command=command,
            position=position,
            account_id=account_id,
            reservation_id=reservation_id,
            connection_epoch=connection_epoch,
            sequence=_strict_int(command_row.get("sequence"), "sequence", minimum=1),
            expected_effect=expected_effect,
            broker_order_id=_optional_text(position_record.get("broker_order_id")),
        )
        submission = await self._execution.submit_lifecycle(plan)
        return LifecycleResult(
            LifecycleDisposition.SENT_AWAITING_ACK
            if submission is SubmissionState.SENT_AWAITING_ACK
            else LifecycleDisposition.EXACT_RETRY_AWAITING_ACK,
            position.trade_id,
            "PENDING_COMMAND_RECOVERED",
            action=position.pending_action,
            milestone=position.pending_milestone,
            command_id=command_id,
        )

    def _apply_virtual(
        self,
        *,
        original: BrokerPosition,
        decision: LifecycleDecision,
        symbol: SymbolSnapshot,
        account_id: str,
        occurred_at_ms: int,
        outbox: Sequence[OutboxRecord],
    ) -> LifecycleResult:
        action = decision.action
        if action is None:
            raise LifecycleStateError(
                "lifecycle result cannot be built without an action"
            )
        if (
            action.kind is not LifecycleActionKind.VIRTUAL_MILESTONE
            or action.command_id is not None
            or decision.position.position_version != original.position_version
            or decision.position.remaining_volume != original.remaining_volume
            or decision.position.current_sl != original.current_sl
            or decision.position.realised_pnl != original.realised_pnl
        ):
            raise LifecycleStateError("virtual milestone attempted to invent broker state")
        event_id = _stable_id(
            "virtual-milestone",
            {
                "trade_id": original.trade_id,
                "broker_position_id": original.broker_position_id,
                "position_version": original.position_version,
                "from_stage": original.lifecycle_stage.value,
                "to_stage": decision.position.lifecycle_stage.value,
                "milestone": action.milestone.value,
                "reason": action.reason,
                "trigger_at_ms": occurred_at_ms,
                "volume_min": symbol.volume_min,
                "volume_step": symbol.volume_step,
            },
        )
        fraction = (
            self._policy.config.tp1_initial_fraction
            if action.milestone is Milestone.TP1
            else self._policy.config.tp2_initial_fraction
        )
        requested = original.initial_volume * fraction
        normalized = (requested // symbol.volume_step) * symbol.volume_step
        event = TradeEventRecord(
            event_id=event_id,
            trade_id=original.trade_id,
            event_type="VIRTUAL_MILESTONE_CONFIRMED",
            source="lifecycle_service",
            correlation_id=event_id,
            occurred_at=occurred_at_ms / 1_000,
            payload={
                "broker_position_id": original.broker_position_id,
                "position_version": original.position_version,
                "milestone": action.milestone.value,
                "from_stage": original.lifecycle_stage.value,
                "to_stage": decision.position.lifecycle_stage.value,
                "reason": action.reason,
                "broker_effect": False,
                "dust_proof": {
                    "initial_volume": original.initial_volume,
                    "remaining_volume": original.remaining_volume,
                    "volume_min": symbol.volume_min,
                    "volume_step": symbol.volume_step,
                    "partial_fraction": fraction,
                    "requested_close_volume": requested,
                    "normalized_close_volume": normalized,
                },
            },
        )
        projected = (
            ()
            if self._outbox_projector is None
            else tuple(self._outbox_projector(event))
        )
        combined_outbox = (*tuple(outbox), *projected)
        if any(
            not isinstance(item, OutboxRecord) or item.event_id != event_id
            for item in combined_outbox
        ):
            raise LifecycleStateError(
                "virtual milestone outbox projection is not bound to its event"
            )
        unique_outbox = {
            item.message_id: item for item in combined_outbox
        }
        if len(unique_outbox) != len(combined_outbox):
            raise LifecycleStateError("virtual milestone outbox IDs are not unique")
        inserted = self._ledger.advance_virtual_lifecycle_milestone_atomic(
            event_id=event_id,
            account_id=account_id,
            trade_id=original.trade_id,
            broker_position_id=original.broker_position_id,
            expected_position_version=original.position_version,
            from_stage=original.lifecycle_stage.value,
            to_stage=decision.position.lifecycle_stage.value,
            milestone=action.milestone.value,
            occurred_at_ms=occurred_at_ms,
            reason=action.reason,
            expected_initial_volume=original.initial_volume,
            expected_remaining_volume=original.remaining_volume,
            volume_min=symbol.volume_min,
            volume_step=symbol.volume_step,
            partial_fraction=fraction,
            requested_close_volume=requested,
            normalized_close_volume=normalized,
            outbox=tuple(unique_outbox[key] for key in sorted(unique_outbox)),
        )
        return LifecycleResult(
            LifecycleDisposition.VIRTUAL_MILESTONE_APPLIED
            if inserted
            else LifecycleDisposition.VIRTUAL_MILESTONE_EXACT_RETRY,
            original.trade_id,
            decision.reason,
            action=action.kind,
            milestone=action.milestone,
        )


def completed_h1_bars_elapsed(
    position: BrokerPosition,
    bars: Sequence[CompletedBar],
    *,
    as_of_ms: int,
) -> int | None:
    """Count actual authenticated H1 closes after the broker open timestamp."""

    if isinstance(as_of_ms, bool) or not isinstance(as_of_ms, int) or as_of_ms < 0:
        raise LifecycleEvidenceError("as_of_ms must be a non-negative integer")
    if not bars:
        return None
    if any(not isinstance(item, CompletedBar) for item in bars):
        raise LifecycleEvidenceError("H1 evidence contains a non-bar item")
    seen: set[int] = set()
    elapsed = 0
    prior_open = -1
    for bar in sorted(bars, key=lambda item: item.open_time_ms):
        if bar.symbol != position.symbol or bar.timeframe != "H1":
            raise LifecycleEvidenceError("H1 evidence is outside position scope")
        _validate_bar_prices(bar)
        if (
            bar.open_time_ms in seen
            or bar.open_time_ms <= prior_open
            or bar.close_time_ms - bar.open_time_ms != 3_600_000
            or bar.close_time_ms > as_of_ms
            or bar.captured_at_ms < bar.close_time_ms
            or bar.captured_at_ms > as_of_ms
        ):
            raise LifecycleEvidenceError("H1 evidence is duplicate, open or time-invalid")
        seen.add(bar.open_time_ms)
        prior_open = bar.open_time_ms
        if bar.close_time_ms > position.opened_at_ms:
            elapsed += 1
    return elapsed


def derive_structural_trailing_sl(
    position: BrokerPosition,
    bars: Sequence[CompletedBar],
    *,
    symbol: SymbolSnapshot,
    left_bars: int,
    right_bars: int,
    buffer_points: int,
) -> Decimal | None:
    """Return the latest immutable H1 fractal stop, or no candidate.

    A pivot is strict: a BUY swing low must be below every low in the configured
    bars on both sides; a SELL swing high must be above every corresponding
    high.  Right-side bars are therefore already closed before the pivot can be
    used.  The stop is buffered beyond structure and rounded away from it.
    """

    for name, value, minimum in (
        ("left_bars", left_bars, 1),
        ("right_bars", right_bars, 1),
        ("buffer_points", buffer_points, 0),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise LifecycleEvidenceError(f"{name} must be an integer >= {minimum}")
    if position.symbol != symbol.symbol:
        raise LifecycleEvidenceError("structural-trail symbol scope differs")
    if position.lifecycle_stage not in {
        LifecycleStage.TP2_CONFIRMED,
        LifecycleStage.TRAILING,
    }:
        return None
    window = left_bars + 1 + right_bars
    if len(bars) < window:
        return None
    ordered = sorted(bars, key=lambda item: item.open_time_ms)
    for bar in ordered:
        if bar.symbol != position.symbol or bar.timeframe != "H1":
            raise LifecycleEvidenceError("structural H1 evidence is outside position scope")
        _validate_bar_prices(bar)

    buffer = Decimal(buffer_points) * symbol.point
    for pivot_index in range(len(ordered) - right_bars - 1, left_bars - 1, -1):
        pivot = ordered[pivot_index]
        neighbours = (
            ordered[pivot_index - left_bars : pivot_index]
            + ordered[pivot_index + 1 : pivot_index + right_bars + 1]
        )
        if position.direction is Direction.BUY:
            if not all(pivot.low < item.low for item in neighbours):
                continue
            candidate = _round_to_step(
                pivot.low - buffer, symbol.tick_size, rounding=ROUND_FLOOR
            )
        else:
            if not all(pivot.high > item.high for item in neighbours):
                continue
            candidate = _round_to_step(
                pivot.high + buffer, symbol.tick_size, rounding=ROUND_CEILING
            )
        return candidate if candidate > 0 else None
    return None


def _validate_bar_prices(bar: CompletedBar) -> None:
    if (
        bar.open <= 0
        or bar.high <= 0
        or bar.low <= 0
        or bar.close <= 0
        or bar.low > min(bar.open, bar.close)
        or bar.high < max(bar.open, bar.close)
        or bar.low > bar.high
        or isinstance(bar.volume, bool)
        or not isinstance(bar.volume, int)
        or bar.volume < 0
    ):
        raise LifecycleEvidenceError("H1 evidence has invalid OHLCV geometry")


def _round_to_step(value: Decimal, step: Decimal, *, rounding: str) -> Decimal:
    if step <= 0:
        raise LifecycleEvidenceError("symbol tick size must be positive")
    return (value / step).to_integral_value(rounding=rounding) * step


def _require_exact_fill_deal(
    event: BrokerEvent,
    deal: Mapping[str, Any],
    *,
    account_id: str,
    trade_id: str,
    broker_position_id: str,
) -> None:
    """Bind a fill event to the one durable broker deal carrying its economics."""

    if event.fill_price is None or event.fill_volume is None:
        raise LifecycleEvidenceError("fill event lacks authoritative price or volume")
    if not isinstance(event.order_id, str) or not event.order_id.strip():
        raise LifecycleEvidenceError("fill event lacks authoritative broker order ID")
    if (
        deal.get("account_id") != account_id
        or deal.get("trade_id") != trade_id
        or deal.get("position_id") != broker_position_id
        or deal.get("order_id") != event.order_id
        or deal.get("command_id") != event.command_id
        or str(deal.get("deal_type", "")).upper() not in {"OUT", "OUT_BY"}
    ):
        raise LifecycleEvidenceError("fill event identity differs from its broker deal")
    expected_decimals = {
        "volume": event.fill_volume,
        "price": event.fill_price,
        "profit": event.profit,
        "commission": event.commission,
        "swap": event.swap,
        "fee": event.fee,
    }
    try:
        differs = any(
            Decimal(str(deal.get(name))) != expected
            for name, expected in expected_decimals.items()
        )
        deal_time_ms = Decimal(str(deal.get("occurred_at"))) * Decimal("1000")
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise LifecycleEvidenceError("broker deal economics are not canonical decimals") from exc
    if differs or deal_time_ms != Decimal(event.broker_time_ms):
        raise LifecycleEvidenceError("fill event economics or broker time differ from its deal")


def _deal_evidence(record: BrokerDealRecord) -> Mapping[str, Any]:
    return {
        "account_id": record.account_id,
        "deal_id": record.deal_id,
        "trade_id": record.trade_id,
        "position_id": record.position_id,
        "order_id": record.order_id,
        "command_id": record.command_id,
        "deal_type": record.deal_type,
        "volume": record.volume,
        "price": record.price,
        "profit": record.profit,
        "commission": record.commission,
        "swap": record.swap,
        "fee": record.fee,
        "occurred_at": record.occurred_at,
    }


def _require_sufficient_h1_history(
    position: BrokerPosition,
    bars: Sequence[CompletedBar],
    *,
    required_closes: int,
) -> None:
    if not bars:
        raise LifecycleEvidenceError(
            "closed H1 evidence is required for the hard holding timeout"
        )
    ordered = sorted(bars, key=lambda item: item.open_time_ms)
    # If history reaches the broker open, elapsed counting is exact.  For an
    # older position, at least the full timeout number of authenticated closes
    # is sufficient proof that the timeout is due.
    reaches_open = ordered[0].open_time_ms <= position.opened_at_ms
    if not reaches_open and len(ordered) < required_closes:
        raise LifecycleEvidenceError(
            "H1 history neither reaches position open nor proves the hard timeout"
        )


def broker_position_from_ledger(record: Mapping[str, Any]) -> BrokerPosition:
    """Strictly reconstruct a domain position from the canonical ledger row."""

    if not isinstance(record, Mapping):
        raise LifecycleStateError("position record must be a mapping")
    trade_id = _required_text(record.get("trade_id"), "trade_id")
    try:
        return BrokerPosition(
            trade_id=trade_id,
            broker_position_id=_required_text(
                record.get("broker_position_id"), "broker_position_id"
            ),
            position_ticket=_required_text(
                record.get("position_ticket"), "position_ticket"
            ),
            symbol=_required_text(record.get("symbol"), "symbol"),
            magic=_strict_int(record.get("magic"), "magic", minimum=1),
            direction=Direction(_required_text(record.get("direction"), "direction")),
            initial_volume=_decimal(record.get("initial_volume"), "initial_volume"),
            remaining_volume=_decimal(
                record.get("remaining_volume"), "remaining_volume"
            ),
            entry_price=_decimal(record.get("entry_price"), "entry_price"),
            original_sl=_decimal(record.get("original_sl"), "original_sl"),
            current_sl=_decimal(record.get("current_sl"), "current_sl"),
            tp1=_decimal(record.get("tp1"), "tp1"),
            tp2=_decimal(record.get("tp2"), "tp2"),
            tp3=_decimal(record.get("tp3"), "tp3"),
            initial_risk_amount=_decimal(
                record.get("initial_risk_amount"), "initial_risk_amount"
            ),
            lifecycle_stage=LifecycleStage(
                _required_text(record.get("lifecycle_stage"), "lifecycle_stage")
            ),
            position_version=_strict_int(
                record.get("position_version"), "position_version", minimum=0
            ),
            opened_at_ms=_strict_int(
                record.get("opened_at_ms"), "opened_at_ms", minimum=0
            ),
            realised_pnl=_decimal(record.get("realised_pnl", "0"), "realised_pnl"),
            pending_action=_optional_enum(
                LifecycleActionKind, record.get("pending_action"), "pending_action"
            ),
            pending_milestone=_optional_enum(
                Milestone, record.get("pending_milestone"), "pending_milestone"
            ),
            pending_command_id=_optional_text(record.get("pending_command_id")),
            pending_close_volume=_decimal(
                record.get("pending_close_volume", "0"), "pending_close_volume"
            ),
            pending_filled_volume=_decimal(
                record.get("pending_filled_volume", "0"), "pending_filled_volume"
            ),
        )
    except (ValueError, TypeError) as exc:
        raise LifecycleStateError(f"invalid canonical position record: {exc}") from exc


def _expected_effect(
    position: BrokerPosition, action: LifecycleAction
) -> dict[str, Any]:
    common = {
        "action": action.kind.value,
        "broker_position_id": position.broker_position_id,
        "expected_position_version": position.position_version,
    }
    if action.kind is LifecycleActionKind.MODIFY_SL:
        if action.new_sl is None:
            raise LifecycleStateError("MODIFY_SL lacks an exact target")
        return {
            **common,
            "previous_sl": position.current_sl,
            "target_sl": action.new_sl,
        }
    if action.kind is LifecycleActionKind.PARTIAL_CLOSE:
        if action.close_volume <= 0 or action.close_volume >= position.remaining_volume:
            raise LifecycleStateError("PARTIAL_CLOSE volume is not executable")
        return {
            **common,
            "previous_remaining_volume": position.remaining_volume,
            "target_remaining_volume": position.remaining_volume - action.close_volume,
            "target_close_volume": action.close_volume,
        }
    if action.kind is LifecycleActionKind.CLOSE:
        if action.close_volume != position.remaining_volume:
            raise LifecycleStateError("CLOSE must target all remaining volume")
        return {
            **common,
            "previous_remaining_volume": position.remaining_volume,
            "target_remaining_volume": Decimal("0"),
        }
    raise LifecycleStateError("virtual milestone has no broker expected effect")


def _assert_same_decision(first: LifecycleDecision, second: LifecycleDecision) -> None:
    if first.action is None or second.action is None:
        raise LifecycleStateError("lifecycle re-plan lost its action")
    left = first.action
    right = second.action
    if (
        left.kind is not right.kind
        or left.milestone is not right.milestone
        or left.reason != right.reason
        or left.close_volume != right.close_volume
        or left.new_sl != right.new_sl
        or first.reason != second.reason
    ):
        raise LifecycleStateError("lifecycle policy was not deterministic")
    if second.position.pending_command_id != right.command_id:
        raise LifecycleStateError("lifecycle re-plan did not pin its final command")


def _stable_id(prefix: str, payload: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    return f"{prefix}-{digest[:40]}"


def _canonical_json(value: Any) -> str:
    def ready(item: Any) -> Any:
        if isinstance(item, Mapping):
            return {str(key): ready(item[key]) for key in sorted(item)}
        if isinstance(item, (tuple, list)):
            return [ready(part) for part in item]
        if isinstance(item, Decimal):
            return "0" if item == 0 else format(item.normalize(), "f")
        if isinstance(item, StrEnum):
            return item.value
        return item

    return json.dumps(ready(value), sort_keys=True, separators=(",", ":"), allow_nan=False)


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LifecycleStateError(f"{label} must be a non-empty string")
    return value.strip()


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return _required_text(value, "optional text")


def _strict_int(value: Any, label: str, *, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise LifecycleStateError(f"{label} must be an integer >= {minimum}")
    return value


def _decimal(value: Any, label: str) -> Decimal:
    if isinstance(value, bool):
        raise LifecycleStateError(f"{label} must be decimal numeric")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise LifecycleStateError(f"{label} must be decimal numeric") from exc
    if not result.is_finite():
        raise LifecycleStateError(f"{label} must be finite")
    return result


def _optional_enum(enum_type: type[StrEnum], value: Any, label: str) -> Any:
    if value is None:
        return None
    try:
        return enum_type(_required_text(value, label))
    except ValueError as exc:
        raise LifecycleStateError(f"{label} is not canonical") from exc


__all__ = [
    "LifecycleDisposition",
    "LifecycleEvidenceError",
    "LifecycleResult",
    "LifecycleService",
    "LifecycleServiceConfig",
    "LifecycleServiceError",
    "LifecycleStateError",
    "SessionCloseEvidence",
    "broker_position_from_ledger",
    "completed_h1_bars_elapsed",
    "derive_structural_trailing_sl",
]
