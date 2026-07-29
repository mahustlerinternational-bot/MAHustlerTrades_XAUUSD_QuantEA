"""Lifecycle-first production tick scheduler for the canonical runtime.

The scheduler owns no socket and has no alternative execution path.  It binds
one authenticated broker tick to the latest reconciled snapshot, evaluates at
most one lifecycle action for every durable owned position, and only then may
ask the pure decision service for a new entry.  Approved entries are converted
to a deterministic durable execution plan and submitted exclusively through
``ExecutionService``.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass, replace
from decimal import Decimal
from enum import StrEnum
from typing import Any, Callable, Mapping, Protocol, Sequence

from .broker_gateway import BrokerTick, CompletedBar
from .decision_service import (
    CanonicalDecisionService,
    DecisionPolicyConfig,
    DecisionRequest,
    DecisionResult,
    DecisionRuntimeIdentity,
    MacroDecisionPolicy,
)
from .domain_models import (
    BrokerCommand,
    Clock,
    CommandAction,
    CommandStatus,
    Direction,
    ReservationState,
    RiskReservation,
    SymbolSnapshot,
)
from .execution_service import (
    ExecutionService,
    OpenExecutionPlan,
    SubmissionState,
    canonical_open_expected_effect,
    canonical_open_payload,
)
from .learning_service import GovernedLearningService, PMAProcessResult
from .lifecycle_service import LifecycleResult, LifecycleService
from .market_snapshot_service import (
    MarketSnapshotAssembler,
    MarketSnapshotAssemblerConfig,
    ProviderContexts,
    SnapshotAssemblyError,
)
from .reconciliation import ReconciliationReport
from .runtime_config import RuntimeConfig, RuntimeMode
from .runtime_state import ControlPlaneState, RuntimeStateMachine
from .strategy_pipeline import CanonicalStrategyPipeline, DataMode
from .trade_ledger import LedgerError, OutboxRecord, TradeEventRecord, TradeLedger
from .trading_policies import EntryPolicyConfig, FusionPolicyConfig, RiskPolicyConfig


class TickSchedulerError(RuntimeError):
    """Authenticated tick evidence could not be processed safely."""


class LearningRuntimeStatus(StrEnum):
    DISABLED_BY_CONFIG = "DISABLED_BY_CONFIG"
    NEUTRAL_NO_ACTIVATION = "NEUTRAL_NO_ACTIVATION"
    ACTIVE_APPROVED_SCOPE = "ACTIVE_APPROVED_SCOPE"
    DEGRADED = "DEGRADED"


class TickGatewayPort(Protocol):
    @property
    def connection_epoch(self) -> str | None: ...

    @property
    def latest_account_health(self) -> Mapping[str, Any] | None: ...

    @property
    def latest_symbol_snapshot(self) -> SymbolSnapshot | None: ...

    @property
    def completed_bars(self) -> tuple[CompletedBar, ...]: ...

    async def next_tick(self, timeout: float | None = None) -> BrokerTick: ...


@dataclass(frozen=True, slots=True)
class TickCycleResult:
    tick_id: str
    snapshot_id: str
    lifecycle: tuple[LifecycleResult, ...]
    decision: DecisionResult | None
    submission: SubmissionState | None
    command_id: str | None
    learning_status: LearningRuntimeStatus
    entry_blocker: str = ""


class CanonicalTickScheduler:
    """Compose exact market evidence, protection, decision and durable entry."""

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        config_hash: str,
        ledger: TradeLedger,
        gateway: TickGatewayPort,
        runtime: RuntimeStateMachine,
        execution: ExecutionService,
        lifecycle: LifecycleService,
        clock: Clock,
        decision: CanonicalDecisionService | None = None,
        assembler: MarketSnapshotAssembler | None = None,
        learning: GovernedLearningService | None = None,
        outbox_projector: Callable[
            [TradeEventRecord], Sequence[OutboxRecord]
        ] | None = None,
    ) -> None:
        if config.mode not in {RuntimeMode.SHADOW, RuntimeMode.DEMO}:
            raise ValueError("tick scheduler is only valid in shadow/demo modes")
        if not isinstance(ledger, TradeLedger):
            raise TypeError("tick scheduler requires the canonical TradeLedger")
        self.config = config
        self.config_hash = _sha256(config_hash)
        self.ledger = ledger
        self.gateway = gateway
        self.runtime = runtime
        self.execution = execution
        self.lifecycle = lifecycle
        self.clock = clock
        self._outbox_projector = outbox_projector
        self.learning = learning
        self.learning_status = (
            LearningRuntimeStatus.NEUTRAL_NO_ACTIVATION
            if learning is not None
            else LearningRuntimeStatus.DISABLED_BY_CONFIG
        )
        self.learning_detail = ""
        self._learning_worker_id = _stable_id(
            "pma-worker",
            {
                "runtime_id": config.system.instance_id,
                "account_id": config.system.expected_account_id,
                "symbol": config.system.symbol,
                "magic": config.system.magic_number,
            },
        )

        entry = config.entry
        strategy_ids = tuple(
            sorted(set(range(4, 14)) & set(config.enabled_agent_ids))
        )
        self.decision = decision or CanonicalDecisionService(
            clock=clock,
            config=DecisionPolicyConfig(
                runtime_max_risk_pct=Decimal(str(config.risk.max_risk_pct)),
                maximum_slippage_points=entry.maximum_slippage_points,
                intent_ttl_ms=entry.intent_ttl_ms,
                maximum_snapshot_age_ms=entry.maximum_snapshot_age_ms,
            ),
            pipeline=CanonicalStrategyPipeline(enabled_agent_ids=strategy_ids),
            fusion_config=FusionPolicyConfig(
                min_agents_agreeing=config.confluence.min_agents_agreeing,
                min_confluence_pct=Decimal(
                    str(config.confluence.base_threshold_pct)
                ),
            ),
            entry_config=EntryPolicyConfig(
                max_quote_age_ms=entry.maximum_snapshot_age_ms,
                max_spread_points=Decimal(str(entry.max_spread_points)),
            ),
            risk_config=RiskPolicyConfig(
                hard_max_risk_pct=Decimal(str(config.risk.max_risk_pct)),
                max_daily_drawdown_pct=Decimal(
                    str(config.risk.daily_max_drawdown_pct)
                ),
                max_total_risk_pct=Decimal(
                    str(config.risk.max_total_risk_pct)
                ),
                max_open_positions=config.risk.max_open_positions,
                reservation_ttl_ms=entry.reservation_ttl_ms,
                max_snapshot_age_ms=entry.maximum_snapshot_age_ms,
            ),
        )
        self.assembler = assembler or MarketSnapshotAssembler(
            MarketSnapshotAssemblerConfig(
                mode=DataMode(config.mode.value),
                maximum_tick_age_ms=entry.maximum_snapshot_age_ms,
                maximum_account_age_ms=entry.maximum_snapshot_age_ms,
            )
        )

    @property
    def execution_ready(self) -> bool:
        return self.config.mode is RuntimeMode.DEMO

    async def next_tick(self, timeout: float | None = None) -> BrokerTick:
        return await self.gateway.next_tick(timeout)

    async def process_tick(
        self,
        tick: BrokerTick,
        *,
        providers: ProviderContexts,
        reconciliation: ReconciliationReport,
    ) -> TickCycleResult:
        """Process one exact tick; lifecycle always precedes entry evaluation."""

        if not isinstance(tick, BrokerTick):
            raise TickSchedulerError("tick scheduler requires a canonical BrokerTick")
        account_id = self.config.system.expected_account_id
        if not account_id:
            raise TickSchedulerError("tick scheduler lacks a pinned account")
        epoch = self.gateway.connection_epoch
        account = self.gateway.latest_account_health
        symbol = self.gateway.latest_symbol_snapshot
        if not epoch or account is None or symbol is None:
            raise TickSchedulerError(
                "gateway lacks exact epoch/account/symbol evidence for this tick"
            )
        if (
            tick.connection_epoch != epoch
            or reconciliation.connection_epoch != epoch
            or reconciliation.snapshot.identity.connection_epoch != epoch
            or not reconciliation.succeeded
        ):
            raise TickSchedulerError(
                "tick is not bound to the latest successful reconciliation epoch"
            )
        identity = reconciliation.snapshot.identity
        if (
            tick.symbol != self.config.system.symbol
            or symbol.symbol != self.config.system.symbol
            or identity.account_id != account_id
            or identity.symbol != self.config.system.symbol
            or identity.magic != self.config.system.magic_number
            or account.get("account_id") != account_id
            or account.get("symbol") != self.config.system.symbol
        ):
            raise TickSchedulerError(
                "lifecycle tick/account/symbol ownership evidence differs"
            )

        positions = self.ledger.list_owned_positions(
            account_id, self.config.system.symbol, self.config.system.magic_number
        )
        completed_bars = self.gateway.completed_bars
        snapshot_id = f"market:{epoch}:{tick.msg_id}"
        lifecycle_results: list[LifecycleResult] = []
        if self.config.mode is RuntimeMode.DEMO:
            h1 = tuple(item for item in completed_bars if item.timeframe == "H1")
            for position in sorted(
                positions, key=lambda item: str(item["broker_position_id"])
            ):
                # Exactly one call per durable position per tick.  The lifecycle
                # service itself enforces a single pending action lock.
                lifecycle_results.append(
                    await self.lifecycle.evaluate(
                        position,
                        account_id=account_id,
                        quote=tick.quote_snapshot(),
                        symbol=symbol,
                        connection_epoch=epoch,
                        completed_h1_bars=h1,
                    )
                )
        if any(item.action is not None for item in lifecycle_results):
            self._persist_entry_boundary(
                "ENTRY_SKIPPED_LIFECYCLE_PRIORITY",
                tick=tick,
                snapshot_id=snapshot_id,
                detail={
                    "position_count": len(positions),
                    "lifecycle_commands": tuple(
                        item.command_id
                        for item in lifecycle_results
                        if item.command_id is not None
                    ),
                },
            )
            return TickCycleResult(
                tick.msg_id,
                snapshot_id,
                tuple(lifecycle_results),
                None,
                None,
                None,
                self.learning_status,
                "LIFECYCLE_PRIORITY",
            )

        if self.runtime.state is not ControlPlaneState.READY:
            return TickCycleResult(
                tick.msg_id,
                snapshot_id,
                tuple(lifecycle_results),
                None,
                None,
                None,
                self.learning_status,
                "RUNTIME_NOT_READY",
            )

        # Entry-only evidence is assembled after every owned position has had
        # its protection opportunity.  Missing M1/D1 history, stale providers,
        # or any other strategy admission failure cannot pre-empt lifecycle.
        pending_orders = self.ledger.list_pending_orders(
            account_id, self.config.system.symbol, self.config.system.magic_number
        )
        active_quarantine = self.ledger.has_active_quarantined_exposure(
            account_id, self.config.system.symbol, self.config.system.magic_number
        )
        # Provider freshness/freeze evidence is evaluated at the actual
        # decision instant, not when a potentially queued tick was received.
        # The assembler independently rejects a stale tick, so neither side of
        # the time-of-check/time-of-use boundary can inherit old authority.
        decision_at_ms = self.clock.now_ms()
        tick_providers = providers.refreshed(at_ms=decision_at_ms)
        try:
            bundle = self.assembler.assemble(
                now_ms=decision_at_ms,
                connection_epoch=epoch,
                account_health=account,
                symbol=symbol,
                tick=tick,
                completed_bars=completed_bars,
                authoritative=reconciliation.snapshot,
                reconciliation_succeeded=reconciliation.succeeded,
                active_quarantine=active_quarantine,
                providers=tick_providers,
                owned_positions=positions,
                pending_order_count=len(pending_orders),
            )
        except SnapshotAssemblyError as exc:
            blocker = type(exc).__name__ + ":" + str(exc)
            self._persist_entry_boundary(
                "ENTRY_SNAPSHOT_BLOCKED",
                tick=tick,
                snapshot_id=snapshot_id,
                detail={"blocker": blocker},
            )
            return TickCycleResult(
                tick.msg_id,
                snapshot_id,
                tuple(lifecycle_results),
                None,
                None,
                None,
                self.learning_status,
                blocker,
            )

        reservations = tuple(
            _reservation_from_row(item)
            for item in self.ledger.list_active_risk_reservations(
                account_id, bundle.broker.account.risk_day
            )
        )
        activation = self._active_learning(bundle.market)
        ownership = self.runtime.evidence.ownership_key
        authenticated_magic = -1 if ownership is None else ownership[3]
        request = DecisionRequest(
            request_nonce=_stable_id(
                "tick-decision",
                {
                    "tick_id": tick.msg_id,
                    "snapshot_id": bundle.market.snapshot_id,
                    "epoch": epoch,
                    "config_hash": self.config_hash,
                },
            ),
            market=bundle.market,
            broker=bundle.broker,
            identity=DecisionRuntimeIdentity(
                runtime_id=self.config.system.instance_id,
                account_id=account_id,
                symbol=self.config.system.symbol,
                magic=self.config.system.magic_number,
                authenticated_magic=authenticated_magic,
                config_hash=self.config_hash,
                connection_id=epoch,
                runtime_ready=self.runtime.is_ready,
                ownership_verified=bool(
                    self.runtime.evidence.identity_verified
                    and self.runtime.evidence.tuple_lock_acquired
                ),
                execution_authorized=self.config.mode is RuntimeMode.DEMO,
            ),
            requested_risk_pct=Decimal(str(self.config.risk.max_risk_pct)),
            max_slippage_points=self.config.entry.maximum_slippage_points,
            macro_policy=(
                MacroDecisionPolicy.UNSAFE_TECHNICAL_ONLY
                if self.config.providers.unsafe_allow_technical_without_macro
                else MacroDecisionPolicy.FAIL_CLOSED
            ),
            reservations=reservations,
            learning_activation=activation,
            observation_only=self.config.mode is RuntimeMode.SHADOW,
        )
        decision = self.decision.decide(request)
        if not decision.approved:
            self._persist_rejected_signal(
                decision,
                tick=tick,
                snapshot_id=bundle.market.snapshot_id,
            )
            return TickCycleResult(
                tick.msg_id,
                bundle.market.snapshot_id,
                tuple(lifecycle_results),
                decision,
                None,
                None,
                self.learning_status,
            )

        if self.config.mode is RuntimeMode.SHADOW:
            self._persist_approved_observation(decision, tick=tick)
            return TickCycleResult(
                tick.msg_id,
                bundle.market.snapshot_id,
                tuple(lifecycle_results),
                decision,
                None,
                None,
                self.learning_status,
                "SHADOW_OBSERVATION_ONLY",
            )

        intent = decision.intent
        risk_decision = decision.risk_decision
        if intent is None or risk_decision is None:
            raise RuntimeError(
                "approved execution decision lacks intent or risk decision"
            )
        reservation = risk_decision.reservation
        if reservation is None:
            raise RuntimeError(
                "approved execution decision lacks a risk reservation"
            )
        command_id = _stable_id(
            "open-command",
            {
                "decision_id": decision.decision_id,
                "trade_id": intent.trade_id,
                "reservation_id": reservation.reservation_id,
                "connection_epoch": epoch,
                "snapshot_id": bundle.market.snapshot_id,
            },
        )
        sequence = self.ledger.allocate_command_sequence(
            epoch, command_id, allocated_at_ms=intent.created_at_ms
        )
        max_total_risk = (
            min(bundle.broker.account.balance, bundle.broker.account.equity)
            * Decimal(str(self.config.risk.max_total_risk_pct))
            / Decimal("100")
        )
        expected_effect = canonical_open_expected_effect(
            intent, reservation, account_id=account_id
        )
        command = BrokerCommand(
            command_id=command_id,
            trade_id=intent.trade_id,
            action=CommandAction.OPEN,
            status=CommandStatus.CREATED,
            created_at_ms=intent.created_at_ms,
            expires_at_ms=min(intent.expires_at_ms, reservation.expires_at_ms),
        )
        command = replace(
            command,
            payload=canonical_open_payload(
                intent,
                reservation,
                account_id=account_id,
                currency=bundle.broker.account.currency,
                max_total_risk=max_total_risk,
                max_open_positions=self.config.risk.max_open_positions,
            ),
        )
        plan = OpenExecutionPlan(
            intent=intent,
            reservation=reservation,
            command=command,
            account_id=account_id,
            currency=bundle.broker.account.currency,
            connection_epoch=epoch,
            sequence=sequence,
            expected_effect=expected_effect,
            max_total_risk=max_total_risk,
            max_open_positions=self.config.risk.max_open_positions,
        )
        submission = await self.execution.submit_open(plan)
        return TickCycleResult(
            tick.msg_id,
            bundle.market.snapshot_id,
            tuple(lifecycle_results),
            decision,
            submission,
            command_id,
            self.learning_status,
        )

    def expire_sent_commands(self) -> tuple[str, ...]:
        return self.execution.expire_sent_commands()

    async def process_learning_due(self) -> tuple[PMAProcessResult, ...]:
        """Run Agent 20 as contained analytics, never as an execution gate."""

        if self.learning is None:
            self.learning_status = LearningRuntimeStatus.DISABLED_BY_CONFIG
            return ()
        account_id = self.config.system.expected_account_id
        if account_id is None:
            raise RuntimeError(
                "learning runtime requires an expected account identifier"
            )
        try:
            results = await asyncio.to_thread(
                self.learning.process_due,
                self._learning_worker_id,
                now_ms=self.clock.now_ms(),
                target_config_hash=self.config_hash,
                account_id=account_id,
                symbol=self.config.system.symbol,
                magic=self.config.system.magic_number,
            )
            if self.learning_status is LearningRuntimeStatus.DEGRADED:
                self.learning_status = LearningRuntimeStatus.NEUTRAL_NO_ACTIVATION
            self.learning_detail = ""
            return results
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.learning_status = LearningRuntimeStatus.DEGRADED
            self.learning_detail = type(exc).__name__
            return ()

    def _active_learning(self, market: Any) -> Any:
        if self.learning is None:
            self.learning_status = LearningRuntimeStatus.DISABLED_BY_CONFIG
            return None
        try:
            preview = self.decision.pipeline.evaluate(market)
            if not preview.regime.available or preview.regime.assessment is None:
                self.learning_status = LearningRuntimeStatus.NEUTRAL_NO_ACTIVATION
                return None
            activation = self.learning.active(
                account_id=str(self.config.system.expected_account_id),
                symbol=self.config.system.symbol,
                magic=self.config.system.magic_number,
                target_config_hash=self.config_hash,
                regime=preview.regime.assessment.regime,
                session=preview.session,
            )
            self.learning_status = (
                LearningRuntimeStatus.ACTIVE_APPROVED_SCOPE
                if activation is not None
                else LearningRuntimeStatus.NEUTRAL_NO_ACTIVATION
            )
            self.learning_detail = ""
            return activation
        except Exception as exc:
            # Approved-weight lookup is analytics.  Neutral canonical weights
            # remain safe and lifecycle protection must not be interrupted.
            self.learning_status = LearningRuntimeStatus.DEGRADED
            self.learning_detail = type(exc).__name__
            return None

    def _persist_rejected_signal(
        self,
        decision: DecisionResult,
        *,
        tick: BrokerTick,
        snapshot_id: str,
    ) -> None:
        event = TradeEventRecord(
            event_id=f"{decision.decision_id}:signal",
            event_type="ENTRY_SIGNAL_REJECTED",
            source="decision_service",
            occurred_at=tick.received_at_ms / 1_000,
            correlation_id=decision.decision_id,
            payload={
                "snapshot_id": snapshot_id,
                "reason": decision.reason.value,
                "detail_code": decision.detail_code,
                "disabled_agents": decision.disabled_agents,
                "degraded": decision.degraded,
                "learning_status": self.learning_status.value,
                "mode": self.config.mode.value,
            },
        )
        self._commit_event(event)

    def _persist_approved_observation(
        self, decision: DecisionResult, *, tick: BrokerTick
    ) -> None:
        intent = decision.intent
        risk = decision.risk_decision
        if (
            intent is None
            or risk is None
            or risk.reservation is None
        ):
            raise RuntimeError(
                "shadow observation lacks approved intent or reservation"
            )
        event = TradeEventRecord(
            event_id=f"{decision.decision_id}:shadow-signal",
            event_type="ENTRY_SIGNAL_APPROVED_OBSERVATION",
            source="decision_service",
            occurred_at=tick.received_at_ms / 1_000,
            correlation_id=decision.decision_id,
            payload={
                "snapshot_id": intent.snapshot_id,
                "direction": intent.direction.value,
                "entry": str(intent.levels.entry),
                "sl": str(intent.levels.sl),
                "tp1": str(intent.levels.tp1),
                "tp2": str(intent.levels.tp2),
                "tp3": str(intent.levels.tp3),
                "risk_amount": str(risk.reservation.risk_amount),
                "volume": str(risk.reservation.volume),
                "contributors": tuple(
                    {
                        "agent_id": item.agent_id,
                        "signal_id": item.signal_id,
                        "snapshot_id": item.snapshot_id,
                        "observed_at_ms": item.observed_at_ms,
                    }
                    for item in intent.contributors
                ),
                "mode": self.config.mode.value,
                "send_capability": False,
                "learning_status": self.learning_status.value,
            },
        )
        self._commit_event(event)

    def _persist_entry_boundary(
        self,
        event_type: str,
        *,
        tick: BrokerTick,
        snapshot_id: str,
        detail: Mapping[str, Any],
    ) -> None:
        event_id = _stable_id(
            "entry-boundary",
            {
                "event_type": event_type,
                "tick_id": tick.msg_id,
                "snapshot_id": snapshot_id,
                "detail": detail,
            },
        )
        self._commit_event(
            TradeEventRecord(
                event_id=event_id,
                event_type=event_type,
                source="runtime_scheduler",
                occurred_at=tick.received_at_ms / 1_000,
                correlation_id=tick.msg_id,
                payload={
                    "snapshot_id": snapshot_id,
                    "mode": self.config.mode.value,
                    **dict(detail),
                },
            )
        )

    def _commit_event(self, event: TradeEventRecord) -> None:
        try:
            self.ledger.append_event_with_outbox(
                event, self._outbox_for(event)
            )
        except LedgerError as exc:
            self.runtime.halt(
                f"authoritative decision ledger failure: {type(exc).__name__}"
            )
            raise TickSchedulerError(
                "runtime signal could not be committed atomically"
            ) from exc

    def _outbox_for(self, event: TradeEventRecord) -> tuple[OutboxRecord, ...]:
        if self._outbox_projector is None:
            return ()
        records = tuple(self._outbox_projector(event))
        if any(
            not isinstance(item, OutboxRecord) or item.event_id != event.event_id
            for item in records
        ):
            raise TickSchedulerError("decision outbox projection is not event-bound")
        if len({item.message_id for item in records}) != len(records):
            raise TickSchedulerError("decision outbox projection IDs are not unique")
        return records


def _reservation_from_row(row: Mapping[str, Any]) -> RiskReservation:
    try:
        return RiskReservation(
            reservation_id=str(row["reservation_id"]),
            trade_id=str(row["trade_id"]),
            account_id=str(row["account_id"]),
            direction=Direction(str(row["direction"])),
            risk_day=str(row["risk_day"]),
            risk_amount=Decimal(str(row["risk_amount"])),
            volume=Decimal(str(row["volume"])),
            state=ReservationState(str(row["state"])),
            created_at_ms=int(row["created_at_ms"]),
            expires_at_ms=int(row["expires_at_ms"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise TickSchedulerError("durable risk reservation is corrupt") from exc


def _stable_id(namespace: str, payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
        default=str,
    ).encode("utf-8")
    return f"{namespace}-{hashlib.sha256(encoded).hexdigest()[:40]}"


def _sha256(value: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError("config_hash must be a lowercase SHA-256 digest")
    return value


__all__ = [
    "CanonicalTickScheduler",
    "LearningRuntimeStatus",
    "TickCycleResult",
    "TickGatewayPort",
    "TickSchedulerError",
]
