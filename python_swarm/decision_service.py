"""Pure canonical entry-decision coordinator.

The coordinator joins immutable strategy, provider, learning, fusion, entry,
and risk policies.  It creates an in-memory ``TradeIntent`` and proposed
``RiskReservation`` only.  It never writes a ledger, reserves durable risk,
opens a socket, constructs a broker command, or mutates runtime state.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, fields, is_dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from enum import Enum, StrEnum
from typing import Mapping

try:
    from .domain_models import (
        BrokerSnapshot,
        Clock,
        EntryLevels,
        FusionResult,
        RiskReservation,
        TradeIntent,
        decimal_value,
    )
    from .learning_policy import LearningActivation
    from .strategy_pipeline import (
        CanonicalMarketSnapshot,
        CanonicalStrategyPipeline,
        StrategyPipelineResult,
    )
    from .trading_policies import (
        EntryPolicy,
        EntryPolicyConfig,
        FusionDecision,
        FusionPolicy,
        FusionPolicyConfig,
        RiskDecision,
        RiskPolicy,
        RiskPolicyConfig,
    )
except ImportError:  # pragma: no cover - legacy flat-import compatibility
    from domain_models import (  # type: ignore
        BrokerSnapshot,
        Clock,
        EntryLevels,
        FusionResult,
        RiskReservation,
        TradeIntent,
        decimal_value,
    )
    from learning_policy import LearningActivation  # type: ignore
    from strategy_pipeline import (  # type: ignore
        CanonicalMarketSnapshot,
        CanonicalStrategyPipeline,
        StrategyPipelineResult,
    )
    from trading_policies import (  # type: ignore
        EntryPolicy,
        EntryPolicyConfig,
        FusionDecision,
        FusionPolicy,
        FusionPolicyConfig,
        RiskDecision,
        RiskPolicy,
        RiskPolicyConfig,
    )


ZERO = Decimal("0")
ONE = Decimal("1")


def _text(value: str, field_name: str, *, maximum: int = 160) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    normalized = value.strip()
    if len(normalized) > maximum:
        raise ValueError(f"{field_name} exceeds {maximum} characters")
    return normalized


def _integer(value: object, field_name: str, *, minimum: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{field_name} must be an integer >= {minimum}")
    return value


def _decimal(value: object, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} cannot be boolean")
    try:
        result = decimal_value(value)  # type: ignore[arg-type]
    except (ValueError, TypeError, ArithmeticError) as exc:
        raise ValueError(f"{field_name} must be a finite decimal") from exc
    return result


def _decimal_text(value: Decimal) -> str:
    return "0" if value == ZERO else str(value.normalize())


class MacroDecisionPolicy(StrEnum):
    FAIL_CLOSED = "FAIL_CLOSED"
    UNSAFE_TECHNICAL_ONLY = "UNSAFE_TECHNICAL_ONLY"


class DecisionReason(StrEnum):
    APPROVED = "APPROVED"
    RUNTIME_NOT_READY = "RUNTIME_NOT_READY"
    OWNERSHIP_NOT_VERIFIED = "OWNERSHIP_NOT_VERIFIED"
    EXECUTION_NOT_AUTHORIZED = "EXECUTION_NOT_AUTHORIZED"
    AUTHENTICATED_MAGIC_MISMATCH = "AUTHENTICATED_MAGIC_MISMATCH"
    ACCOUNT_IDENTITY_MISMATCH = "ACCOUNT_IDENTITY_MISMATCH"
    SYMBOL_IDENTITY_MISMATCH = "SYMBOL_IDENTITY_MISMATCH"
    CONNECTION_IDENTITY_MISMATCH = "CONNECTION_IDENTITY_MISMATCH"
    MARKET_BROKER_SNAPSHOT_MISMATCH = "MARKET_BROKER_SNAPSHOT_MISMATCH"
    SNAPSHOT_TIMESTAMP_MISMATCH = "SNAPSHOT_TIMESTAMP_MISMATCH"
    SNAPSHOT_STALE_OR_FUTURE = "SNAPSHOT_STALE_OR_FUTURE"
    BROKER_NOT_RECONCILED = "BROKER_NOT_RECONCILED"
    QUOTE_MISMATCH = "QUOTE_MISMATCH"
    TICK_SIZE_MISMATCH = "TICK_SIZE_MISMATCH"
    QUOTE_NOT_TICK_ALIGNED = "QUOTE_NOT_TICK_ALIGNED"
    POSITION_SCOPE_MISMATCH = "POSITION_SCOPE_MISMATCH"
    POSITION_IDENTITY_CONFLICT = "POSITION_IDENTITY_CONFLICT"
    RESERVATION_SCOPE_MISMATCH = "RESERVATION_SCOPE_MISMATCH"
    RESERVATION_IDENTITY_CONFLICT = "RESERVATION_IDENTITY_CONFLICT"
    REQUESTED_RISK_NOT_POSITIVE = "REQUESTED_RISK_NOT_POSITIVE"
    REQUESTED_RISK_EXCEEDS_RUNTIME_CAP = "REQUESTED_RISK_EXCEEDS_RUNTIME_CAP"
    INVALID_SLIPPAGE_LIMIT = "INVALID_SLIPPAGE_LIMIT"
    STRATEGY_PIPELINE_ERROR = "STRATEGY_PIPELINE_ERROR"
    STRATEGY_RESULT_IDENTITY_MISMATCH = "STRATEGY_RESULT_IDENTITY_MISMATCH"
    MACRO_FREEZE_ACTIVE = "MACRO_FREEZE_ACTIVE"
    MACRO_PROTECTION_UNAVAILABLE = "MACRO_PROTECTION_UNAVAILABLE"
    LEARNING_ACTIVATION_INVALID = "LEARNING_ACTIVATION_INVALID"
    LEARNING_ACCOUNT_MISMATCH = "LEARNING_ACCOUNT_MISMATCH"
    LEARNING_SYMBOL_MISMATCH = "LEARNING_SYMBOL_MISMATCH"
    LEARNING_MAGIC_MISMATCH = "LEARNING_MAGIC_MISMATCH"
    LEARNING_CONFIG_MISMATCH = "LEARNING_CONFIG_MISMATCH"
    LEARNING_CONTEXT_UNAVAILABLE = "LEARNING_CONTEXT_UNAVAILABLE"
    LEARNING_REGIME_MISMATCH = "LEARNING_REGIME_MISMATCH"
    LEARNING_SESSION_MISMATCH = "LEARNING_SESSION_MISMATCH"
    FUSION_POLICY_ERROR = "FUSION_POLICY_ERROR"
    FUSION_REJECTED = "FUSION_REJECTED"
    FUSED_LEVELS_INVALID_AT_EXECUTABLE_QUOTE = (
        "FUSED_LEVELS_INVALID_AT_EXECUTABLE_QUOTE"
    )
    NORMALIZED_LEVELS_INVALID = "NORMALIZED_LEVELS_INVALID"
    FINAL_ENTRY_REJECTED = "FINAL_ENTRY_REJECTED"
    RISK_POLICY_ERROR = "RISK_POLICY_ERROR"
    RISK_REJECTED = "RISK_REJECTED"


@dataclass(frozen=True, slots=True)
class DecisionRuntimeIdentity:
    """Pinned runtime and authenticated broker ownership evidence."""

    runtime_id: str
    account_id: str
    symbol: str
    magic: int
    authenticated_magic: int
    config_hash: str
    connection_id: str
    runtime_ready: bool
    ownership_verified: bool
    execution_authorized: bool

    def __post_init__(self) -> None:
        for name in ("runtime_id", "account_id", "symbol", "config_hash", "connection_id"):
            object.__setattr__(self, name, _text(getattr(self, name), name, maximum=128))
        _integer(self.magic, "magic")
        _integer(self.authenticated_magic, "authenticated_magic")
        for name in ("runtime_ready", "ownership_verified", "execution_authorized"):
            if not isinstance(getattr(self, name), bool):
                raise ValueError(f"{name} must be boolean")


@dataclass(frozen=True, slots=True)
class DecisionPolicyConfig:
    runtime_max_risk_pct: Decimal = Decimal("1.0")
    maximum_slippage_points: int = 100
    intent_ttl_ms: int = 10_000
    maximum_snapshot_age_ms: int = 5_000

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "runtime_max_risk_pct",
            _decimal(self.runtime_max_risk_pct, "runtime_max_risk_pct"),
        )
        if not ZERO < self.runtime_max_risk_pct <= ONE:
            raise ValueError("runtime maximum requested risk cannot exceed 1%")
        _integer(self.maximum_slippage_points, "maximum_slippage_points", minimum=0)
        _integer(self.intent_ttl_ms, "intent_ttl_ms")
        _integer(self.maximum_snapshot_age_ms, "maximum_snapshot_age_ms")


@dataclass(frozen=True, slots=True)
class DecisionRequest:
    request_nonce: str
    market: CanonicalMarketSnapshot
    broker: BrokerSnapshot
    identity: DecisionRuntimeIdentity
    requested_risk_pct: Decimal
    max_slippage_points: int
    macro_policy: MacroDecisionPolicy = MacroDecisionPolicy.FAIL_CLOSED
    reservations: tuple[RiskReservation, ...] = ()
    learning_activation: LearningActivation | None = None
    observation_only: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "request_nonce",
            _text(self.request_nonce, "request_nonce", maximum=128),
        )
        if not isinstance(self.market, CanonicalMarketSnapshot):
            raise TypeError("market must be CanonicalMarketSnapshot")
        if not isinstance(self.broker, BrokerSnapshot):
            raise TypeError("broker must be BrokerSnapshot")
        if not isinstance(self.identity, DecisionRuntimeIdentity):
            raise TypeError("identity must be DecisionRuntimeIdentity")
        object.__setattr__(
            self,
            "requested_risk_pct",
            _decimal(self.requested_risk_pct, "requested_risk_pct"),
        )
        if isinstance(self.max_slippage_points, bool) or not isinstance(
            self.max_slippage_points, int
        ):
            raise ValueError("max_slippage_points must be an integer")
        object.__setattr__(self, "macro_policy", MacroDecisionPolicy(self.macro_policy))
        object.__setattr__(self, "reservations", tuple(self.reservations))
        if not isinstance(self.observation_only, bool):
            raise ValueError("observation_only must be boolean")


@dataclass(frozen=True, slots=True)
class ProviderGateDecision:
    allowed: bool
    reason: str
    macro_healthy: bool
    sentiment_healthy: bool
    intermarket_healthy: bool
    unsafe_technical_only: bool
    degraded: bool
    disabled_agents: tuple[int, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "disabled_agents", tuple(self.disabled_agents))
        if self.disabled_agents != tuple(sorted(set(self.disabled_agents))):
            raise ValueError("disabled provider agents must be unique and sorted")


@dataclass(frozen=True, slots=True)
class DecisionResult:
    approved: bool
    reason: DecisionReason
    detail_code: str
    decision_id: str
    disabled_agents: tuple[int, ...] = ()
    degraded: bool = False
    provider_gate: ProviderGateDecision | None = None
    pipeline_result: StrategyPipelineResult | None = None
    fusion_result: FusionResult | None = None
    intent: TradeIntent | None = None
    risk_decision: RiskDecision | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "reason", DecisionReason(self.reason))
        object.__setattr__(self, "detail_code", str(self.detail_code))
        object.__setattr__(self, "decision_id", _text(self.decision_id, "decision_id", maximum=80))
        object.__setattr__(self, "disabled_agents", tuple(self.disabled_agents))
        if self.disabled_agents != tuple(sorted(set(self.disabled_agents))):
            raise ValueError("decision disabled agents must be unique and sorted")
        if self.approved:
            if self.reason is not DecisionReason.APPROVED:
                raise ValueError("approved decision requires APPROVED reason")
            if (
                self.pipeline_result is None
                or self.provider_gate is None
                or self.fusion_result is None
                or self.intent is None
                or self.risk_decision is None
                or not self.risk_decision.approved
                or self.risk_decision.reservation is None
            ):
                raise ValueError("approved decision requires complete policy evidence")
        elif self.reason is DecisionReason.APPROVED:
            raise ValueError("rejected decision cannot use APPROVED reason")
        elif (
            self.risk_decision is not None
            and self.risk_decision.reservation is not None
        ):
            raise ValueError("rejected decision cannot expose a risk reservation")


def _ceil_tick(value: Decimal, tick: Decimal) -> Decimal:
    return (value / tick).to_integral_value(rounding=ROUND_CEILING) * tick


def _floor_tick(value: Decimal, tick: Decimal) -> Decimal:
    return (value / tick).to_integral_value(rounding=ROUND_FLOOR) * tick


def normalize_fused_levels(
    fusion: FusionResult,
    broker: BrokerSnapshot,
) -> EntryLevels:
    """Outward-normalize levels without inventing or reordering targets."""

    tick = broker.symbol.tick_size
    if fusion.direction.value == "BUY":
        return EntryLevels(
            entry=broker.quote.ask,
            sl=_floor_tick(fusion.levels.sl, tick),
            tp1=_ceil_tick(fusion.levels.tp1, tick),
            tp2=_ceil_tick(fusion.levels.tp2, tick),
            tp3=_ceil_tick(fusion.levels.tp3, tick),
        )
    return EntryLevels(
        entry=broker.quote.bid,
        sl=_ceil_tick(fusion.levels.sl, tick),
        tp1=_floor_tick(fusion.levels.tp1, tick),
        tp2=_floor_tick(fusion.levels.tp2, tick),
        tp3=_floor_tick(fusion.levels.tp3, tick),
    )


class CanonicalDecisionService:
    """Coordinate one deterministic, command-free new-entry decision."""

    def __init__(
        self,
        *,
        clock: Clock,
        config: DecisionPolicyConfig | None = None,
        pipeline: CanonicalStrategyPipeline | None = None,
        fusion_config: FusionPolicyConfig | None = None,
        entry_config: EntryPolicyConfig | None = None,
        risk_config: RiskPolicyConfig | None = None,
    ) -> None:
        if not isinstance(clock, Clock):
            raise TypeError("clock must satisfy the canonical Clock protocol")
        self.clock = clock
        self.config = config or DecisionPolicyConfig()
        self.pipeline = pipeline or CanonicalStrategyPipeline()
        self.fusion_config = fusion_config or FusionPolicyConfig()
        self.entry_config = entry_config or EntryPolicyConfig()
        self.risk_config = risk_config or RiskPolicyConfig()

    def decide(self, request: DecisionRequest) -> DecisionResult:
        if not isinstance(request, DecisionRequest):
            raise TypeError("decision service requires DecisionRequest")
        now = self.clock.now_ms()
        decision_id = self._decision_id(request)
        rejection = self._validate_request_boundary(request, now, decision_id)
        if rejection is not None:
            return rejection

        try:
            pipeline_result = self.pipeline.evaluate(request.market)
        except Exception as exc:
            return self._reject(
                decision_id,
                DecisionReason.STRATEGY_PIPELINE_ERROR,
                type(exc).__name__,
            )
        pipeline_rejection = self._validate_pipeline_result(
            request,
            pipeline_result,
            decision_id,
        )
        if pipeline_rejection is not None:
            return pipeline_rejection

        gate = self._provider_gate(request, now)
        if not gate.allowed:
            reason = (
                DecisionReason.MACRO_FREEZE_ACTIVE
                if gate.reason == "MACRO_FREEZE_ACTIVE"
                else DecisionReason.MACRO_PROTECTION_UNAVAILABLE
            )
            return self._reject(
                decision_id,
                reason,
                gate.reason,
                gate=gate,
                pipeline=pipeline_result,
            )

        learning_rejection = self._validate_learning(
            request,
            pipeline_result,
            decision_id,
            gate,
        )
        if learning_rejection is not None:
            return learning_rejection
        weights = (
            None
            if request.learning_activation is None
            else request.learning_activation.fusion_weights
        )
        disabled = frozenset(gate.disabled_agents)
        signals = tuple(
            signal for signal in pipeline_result.signals if signal.agent_id not in disabled
        )
        fusion_id = self._stable_id(
            "fusion",
            {
                "decision_id": decision_id,
                "signal_ids": sorted(signal.signal_id for signal in signals),
                "disabled_agents": list(gate.disabled_agents),
                "learning": (
                    None
                    if request.learning_activation is None
                    else request.learning_activation.proposal_hash
                ),
            },
        )
        entry_policy = EntryPolicy(self.entry_config, clock=self.clock)
        try:
            fusion_decision: FusionDecision = FusionPolicy(
                self.fusion_config,
                clock=self.clock,
                entry_policy=entry_policy,
                agent_weights=weights,
            ).evaluate(signals, fusion_id)
        except Exception as exc:
            return self._reject(
                decision_id,
                DecisionReason.FUSION_POLICY_ERROR,
                type(exc).__name__,
                gate=gate,
                pipeline=pipeline_result,
            )
        if not fusion_decision.accepted or fusion_decision.result is None:
            return self._reject(
                decision_id,
                DecisionReason.FUSION_REJECTED,
                fusion_decision.reason,
                gate=gate,
                pipeline=pipeline_result,
            )
        fusion = fusion_decision.result
        executable = (
            request.broker.quote.ask
            if fusion.direction.value == "BUY"
            else request.broker.quote.bid
        )
        raw_geometry = entry_policy.validate_levels(
            fusion.direction,
            fusion.levels,
            executable,
        )
        if not raw_geometry.valid:
            return self._reject(
                decision_id,
                DecisionReason.FUSED_LEVELS_INVALID_AT_EXECUTABLE_QUOTE,
                raw_geometry.reason,
                gate=gate,
                pipeline=pipeline_result,
                fusion=fusion,
            )
        levels = normalize_fused_levels(fusion, request.broker)
        normalized_geometry = entry_policy.validate_levels(
            fusion.direction,
            levels,
            executable,
        )
        if not normalized_geometry.valid:
            return self._reject(
                decision_id,
                DecisionReason.NORMALIZED_LEVELS_INVALID,
                normalized_geometry.reason,
                gate=gate,
                pipeline=pipeline_result,
                fusion=fusion,
            )

        identity_payload = {
            "decision_id": decision_id,
            "fusion_id": fusion.fusion_id,
            "direction": fusion.direction.value,
            "entry": _decimal_text(levels.entry),
            "sl": _decimal_text(levels.sl),
            "tp1": _decimal_text(levels.tp1),
            "tp2": _decimal_text(levels.tp2),
            "tp3": _decimal_text(levels.tp3),
        }
        trade_id = self._stable_id("trade", identity_payload)
        intent_id = self._stable_id("intent", identity_payload)
        reservation_id = self._stable_id("reservation", identity_payload)
        if any(
            reservation.reservation_id == reservation_id
            or reservation.trade_id == trade_id
            for reservation in request.reservations
        ) or any(position.trade_id == trade_id for position in request.broker.positions):
            return self._reject(
                decision_id,
                DecisionReason.RESERVATION_IDENTITY_CONFLICT,
                "PROPOSED_ID_ALREADY_EXISTS",
                gate=gate,
                pipeline=pipeline_result,
                fusion=fusion,
            )
        intent = TradeIntent(
            intent_id=intent_id,
            trade_id=trade_id,
            fusion_id=fusion.fusion_id,
            symbol=request.identity.symbol,
            magic=request.identity.magic,
            direction=fusion.direction,
            levels=levels,
            # Preserve the fused decision reference.  EntryPolicy compares it
            # with the side-specific executable quote after normalization, so
            # overwriting it with bid/ask cannot erase real quote slippage.
            reference_price=fusion.levels.entry,
            max_slippage_points=request.max_slippage_points,
            requested_risk_pct=request.requested_risk_pct,
            contributors=fusion.contributors,
            regime=fusion.regime,
            session=fusion.session,
            created_at_ms=now,
            expires_at_ms=now + self.config.intent_ttl_ms,
            config_hash=request.identity.config_hash,
            decision_id=decision_id,
            snapshot_id=request.market.snapshot_id,
        )
        final_entry = entry_policy.validate_for_broker(intent, request.broker)
        if not final_entry.valid:
            return self._reject(
                decision_id,
                DecisionReason.FINAL_ENTRY_REJECTED,
                final_entry.reason,
                gate=gate,
                pipeline=pipeline_result,
                fusion=fusion,
                intent=intent,
            )

        try:
            risk = RiskPolicy(
                self.risk_config,
                clock=self.clock,
                entry_policy=entry_policy,
            ).assess(
                intent,
                request.broker,
                reservation_id,
                reservations=request.reservations,
            )
        except Exception as exc:
            return self._reject(
                decision_id,
                DecisionReason.RISK_POLICY_ERROR,
                type(exc).__name__,
                gate=gate,
                pipeline=pipeline_result,
                fusion=fusion,
                intent=intent,
            )
        if not risk.approved or risk.reservation is None:
            return self._reject(
                decision_id,
                DecisionReason.RISK_REJECTED,
                risk.reason,
                gate=gate,
                pipeline=pipeline_result,
                fusion=fusion,
                intent=intent,
                risk=risk,
            )
        return DecisionResult(
            approved=True,
            reason=DecisionReason.APPROVED,
            detail_code="APPROVED",
            decision_id=decision_id,
            disabled_agents=gate.disabled_agents,
            degraded=gate.degraded,
            provider_gate=gate,
            pipeline_result=pipeline_result,
            fusion_result=fusion,
            intent=intent,
            risk_decision=risk,
        )

    def _validate_request_boundary(
        self,
        request: DecisionRequest,
        now: int,
        decision_id: str,
    ) -> DecisionResult | None:
        identity = request.identity
        broker = request.broker
        market = request.market
        if not identity.runtime_ready:
            return self._reject(decision_id, DecisionReason.RUNTIME_NOT_READY)
        if not identity.ownership_verified:
            return self._reject(decision_id, DecisionReason.OWNERSHIP_NOT_VERIFIED)
        if not identity.execution_authorized and not request.observation_only:
            return self._reject(decision_id, DecisionReason.EXECUTION_NOT_AUTHORIZED)
        if identity.authenticated_magic != identity.magic:
            return self._reject(decision_id, DecisionReason.AUTHENTICATED_MAGIC_MISMATCH)
        if identity.account_id != broker.account.account_id:
            return self._reject(decision_id, DecisionReason.ACCOUNT_IDENTITY_MISMATCH)
        if (
            identity.symbol != market.symbol
            or identity.symbol != broker.symbol.symbol
            or identity.symbol != broker.quote.symbol
        ):
            return self._reject(decision_id, DecisionReason.SYMBOL_IDENTITY_MISMATCH)
        if identity.connection_id != broker.connection_id:
            return self._reject(decision_id, DecisionReason.CONNECTION_IDENTITY_MISMATCH)
        if market.snapshot_id != broker.snapshot_id:
            return self._reject(decision_id, DecisionReason.MARKET_BROKER_SNAPSHOT_MISMATCH)
        if (
            market.captured_at_ms != broker.captured_at_ms
            or market.captured_at_ms != broker.quote.captured_at_ms
        ):
            return self._reject(decision_id, DecisionReason.SNAPSHOT_TIMESTAMP_MISMATCH)
        ages = (
            now - market.captured_at_ms,
            now - broker.captured_at_ms,
            now - broker.account.captured_at_ms,
            now - broker.quote.captured_at_ms,
        )
        if any(age < 0 or age > self.config.maximum_snapshot_age_ms for age in ages):
            return self._reject(decision_id, DecisionReason.SNAPSHOT_STALE_OR_FUTURE)
        if not broker.reconciled:
            return self._reject(decision_id, DecisionReason.BROKER_NOT_RECONCILED)
        if market.bid != broker.quote.bid or market.ask != broker.quote.ask:
            return self._reject(decision_id, DecisionReason.QUOTE_MISMATCH)
        if market.tick_size != broker.symbol.tick_size:
            return self._reject(decision_id, DecisionReason.TICK_SIZE_MISMATCH)
        if (
            broker.quote.bid % broker.symbol.tick_size
            or broker.quote.ask % broker.symbol.tick_size
        ):
            return self._reject(decision_id, DecisionReason.QUOTE_NOT_TICK_ALIGNED)
        if any(
            position.symbol != identity.symbol or position.magic != identity.magic
            for position in broker.positions
        ):
            return self._reject(decision_id, DecisionReason.POSITION_SCOPE_MISMATCH)
        position_identity_sets = (
            tuple(position.trade_id for position in broker.positions),
            tuple(position.broker_position_id for position in broker.positions),
            tuple(position.position_ticket for position in broker.positions),
        )
        if any(len(items) != len(set(items)) for items in position_identity_sets):
            return self._reject(
                decision_id,
                DecisionReason.POSITION_IDENTITY_CONFLICT,
                "BROKER_POSITION_IDENTITY_CONFLICT",
            )
        if any(
            not isinstance(reservation, RiskReservation)
            or reservation.account_id != identity.account_id
            for reservation in request.reservations
        ):
            return self._reject(decision_id, DecisionReason.RESERVATION_SCOPE_MISMATCH)
        reservation_ids = tuple(item.reservation_id for item in request.reservations)
        reservation_trades = tuple(item.trade_id for item in request.reservations)
        if len(reservation_ids) != len(set(reservation_ids)) or len(
            reservation_trades
        ) != len(set(reservation_trades)):
            return self._reject(decision_id, DecisionReason.RESERVATION_IDENTITY_CONFLICT)
        if request.requested_risk_pct <= ZERO:
            return self._reject(decision_id, DecisionReason.REQUESTED_RISK_NOT_POSITIVE)
        effective_risk_cap = min(
            ONE,
            self.config.runtime_max_risk_pct,
            self.risk_config.hard_max_risk_pct,
        )
        if request.requested_risk_pct > effective_risk_cap:
            return self._reject(
                decision_id,
                DecisionReason.REQUESTED_RISK_EXCEEDS_RUNTIME_CAP,
                f"MAX:{_decimal_text(effective_risk_cap)}",
            )
        if not 0 <= request.max_slippage_points <= self.config.maximum_slippage_points:
            return self._reject(decision_id, DecisionReason.INVALID_SLIPPAGE_LIMIT)
        return None

    @staticmethod
    def _validate_pipeline_result(
        request: DecisionRequest,
        pipeline: StrategyPipelineResult,
        decision_id: str,
    ) -> DecisionResult | None:
        """Pin every strategy output back to the admitted market snapshot."""

        if not isinstance(pipeline, StrategyPipelineResult):
            detail = "INVALID_PIPELINE_RESULT_TYPE"
        elif pipeline.snapshot_id != request.market.snapshot_id:
            detail = "PIPELINE_SNAPSHOT_ID_MISMATCH"
        elif pipeline.observed_at_ms != request.market.captured_at_ms:
            detail = "PIPELINE_TIMESTAMP_MISMATCH"
        elif pipeline.session != request.market.session:
            detail = "PIPELINE_SESSION_MISMATCH"
        elif (
            pipeline.regime.assessment is not None
            and (
                pipeline.regime.assessment.snapshot_id != request.market.snapshot_id
                or pipeline.regime.assessment.observed_at_ms
                != request.market.captured_at_ms
            )
        ):
            detail = "REGIME_IDENTITY_MISMATCH"
        elif any(
            signal.session != pipeline.session.value
            or (
                pipeline.regime.assessment is not None
                and signal.regime != pipeline.regime.assessment.regime.value
            )
            for signal in pipeline.signals
        ):
            detail = "SIGNAL_CONTEXT_MISMATCH"
        else:
            return None
        return CanonicalDecisionService._reject(
            decision_id,
            DecisionReason.STRATEGY_RESULT_IDENTITY_MISMATCH,
            detail,
        )

    @staticmethod
    def _provider_gate(request: DecisionRequest, now: int) -> ProviderGateDecision:
        market = request.market
        macro = market.macro
        intermarket = market.intermarket
        macro_healthy = bool(
            macro is not None and macro.healthy_covered_at(now, market.mode)
        )
        sentiment_healthy = bool(
            macro_healthy
            and macro is not None
            and macro.sentiment_healthy_at(now, market.mode)
        )
        intermarket_healthy = bool(
            intermarket is not None
            and intermarket.healthy_fresh_at(now, market.mode)
        )
        unsafe = request.macro_policy is MacroDecisionPolicy.UNSAFE_TECHNICAL_ONLY
        disabled: set[int] = set()
        if unsafe:
            disabled.update((10, 14))
        elif not sentiment_healthy:
            disabled.add(10)
        if not intermarket_healthy:
            disabled.update((12, 15))
        if macro is not None and macro.freeze_active:
            return ProviderGateDecision(
                allowed=False,
                reason="MACRO_FREEZE_ACTIVE",
                macro_healthy=macro_healthy,
                sentiment_healthy=sentiment_healthy,
                intermarket_healthy=intermarket_healthy,
                unsafe_technical_only=unsafe,
                degraded=True,
                disabled_agents=tuple(sorted(disabled | {10, 14})),
            )
        if not macro_healthy and not unsafe:
            return ProviderGateDecision(
                allowed=False,
                reason="MACRO_PROTECTION_UNAVAILABLE",
                macro_healthy=False,
                sentiment_healthy=False,
                intermarket_healthy=intermarket_healthy,
                unsafe_technical_only=False,
                degraded=True,
                disabled_agents=tuple(sorted(disabled | {10, 14})),
            )
        if not macro_healthy:
            disabled.update((10, 14))
        degraded = unsafe or not sentiment_healthy or not intermarket_healthy
        if unsafe and not intermarket_healthy:
            reason = "UNSAFE_TECHNICAL_ONLY_INTERMARKET_UNAVAILABLE"
        elif unsafe:
            reason = "UNSAFE_TECHNICAL_ONLY"
        elif not sentiment_healthy and not intermarket_healthy:
            reason = "PROVIDER_SIGNAL_AGENTS_UNAVAILABLE"
        elif not sentiment_healthy:
            reason = "MACRO_SENTIMENT_UNAVAILABLE"
        elif not intermarket_healthy:
            reason = "INTERMARKET_UNAVAILABLE"
        else:
            reason = "ALLOWED"
        return ProviderGateDecision(
            allowed=True,
            reason=reason,
            macro_healthy=macro_healthy,
            sentiment_healthy=sentiment_healthy,
            intermarket_healthy=intermarket_healthy,
            unsafe_technical_only=unsafe,
            degraded=degraded,
            disabled_agents=tuple(sorted(disabled)),
        )

    def _validate_learning(
        self,
        request: DecisionRequest,
        pipeline: StrategyPipelineResult,
        decision_id: str,
        gate: ProviderGateDecision,
    ) -> DecisionResult | None:
        activation = request.learning_activation
        if activation is None:
            return None
        if not isinstance(activation, LearningActivation):
            return self._reject(
                decision_id,
                DecisionReason.LEARNING_ACTIVATION_INVALID,
                gate=gate,
                pipeline=pipeline,
            )
        if activation.account_id != request.identity.account_id:
            reason = DecisionReason.LEARNING_ACCOUNT_MISMATCH
        elif activation.symbol != request.identity.symbol:
            reason = DecisionReason.LEARNING_SYMBOL_MISMATCH
        elif activation.magic != request.identity.magic:
            reason = DecisionReason.LEARNING_MAGIC_MISMATCH
        elif activation.target_config_hash != request.identity.config_hash:
            reason = DecisionReason.LEARNING_CONFIG_MISMATCH
        elif not pipeline.regime.available or pipeline.regime.assessment is None:
            reason = DecisionReason.LEARNING_CONTEXT_UNAVAILABLE
        elif activation.regime != pipeline.regime.assessment.regime:
            reason = DecisionReason.LEARNING_REGIME_MISMATCH
        elif activation.session != pipeline.session:
            reason = DecisionReason.LEARNING_SESSION_MISMATCH
        else:
            return None
        return self._reject(
            decision_id,
            reason,
            gate=gate,
            pipeline=pipeline,
        )

    def _decision_id(self, request: DecisionRequest) -> str:
        identity = request.identity
        return self._stable_id(
            "decision",
            {
                "runtime_id": identity.runtime_id,
                "account_id": identity.account_id,
                "symbol": identity.symbol,
                "magic": identity.magic,
                "config_hash": identity.config_hash,
                "connection_id": identity.connection_id,
                "request_nonce": request.request_nonce,
                "market_snapshot_id": request.market.snapshot_id,
                "broker_snapshot_id": request.broker.snapshot_id,
                "market_evidence_hash": self._evidence_hash(request.market),
                "broker_evidence_hash": self._evidence_hash(request.broker),
                "reservations_evidence_hash": self._evidence_hash(
                    request.reservations
                ),
                "learning_evidence_hash": self._evidence_hash(
                    request.learning_activation
                ),
                "decision_policy_hash": self._evidence_hash(self.config),
                "fusion_policy_hash": self._evidence_hash(self.fusion_config),
                "entry_policy_hash": self._evidence_hash(self.entry_config),
                "risk_policy_hash": self._evidence_hash(self.risk_config),
                "requested_risk_pct": _decimal_text(request.requested_risk_pct),
                "max_slippage_points": request.max_slippage_points,
                "macro_policy": request.macro_policy.value,
                "observation_only": request.observation_only,
            },
        )

    @classmethod
    def _evidence_hash(cls, value: object) -> str:
        encoded = json.dumps(
            cls._canonical_evidence(value),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @classmethod
    def _canonical_evidence(cls, value: object) -> object:
        """Convert admitted immutable evidence to a stable JSON value."""

        if value is None or isinstance(value, (bool, int, str)):
            return value
        if isinstance(value, Decimal):
            return _decimal_text(value)
        if isinstance(value, Enum):
            return cls._canonical_evidence(value.value)
        if is_dataclass(value) and not isinstance(value, type):
            return {
                item.name: cls._canonical_evidence(getattr(value, item.name))
                for item in fields(value)
            }
        if isinstance(value, Mapping):
            return {
                str(key): cls._canonical_evidence(item)
                for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            }
        if isinstance(value, (tuple, list)):
            return [cls._canonical_evidence(item) for item in value]
        raise TypeError(f"unsupported decision evidence type: {type(value).__name__}")

    @staticmethod
    def _stable_id(kind: str, payload: dict[str, object]) -> str:
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return f"{kind}-{digest[:32]}"

    @staticmethod
    def _reject(
        decision_id: str,
        reason: DecisionReason,
        detail: str = "",
        *,
        gate: ProviderGateDecision | None = None,
        pipeline: StrategyPipelineResult | None = None,
        fusion: FusionResult | None = None,
        intent: TradeIntent | None = None,
        risk: RiskDecision | None = None,
    ) -> DecisionResult:
        return DecisionResult(
            approved=False,
            reason=reason,
            detail_code=detail or reason.value,
            decision_id=decision_id,
            disabled_agents=() if gate is None else gate.disabled_agents,
            degraded=False if gate is None else gate.degraded,
            provider_gate=gate,
            pipeline_result=pipeline,
            fusion_result=fusion,
            intent=intent,
            risk_decision=risk,
        )


__all__ = [
    "CanonicalDecisionService",
    "DecisionPolicyConfig",
    "DecisionReason",
    "DecisionRequest",
    "DecisionResult",
    "DecisionRuntimeIdentity",
    "MacroDecisionPolicy",
    "ProviderGateDecision",
    "normalize_fused_levels",
]
