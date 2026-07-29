"""Pure trading policies built on the canonical immutable domain contracts.

No policy in this module performs I/O or mutates legacy runtime state.  Expected
rejections are returned as decisions, making the same rules usable by unit
tests, replay, the Python control plane and the MQL5 protocol adapter.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Mapping, Sequence

try:  # Package import in production/tests; direct import for legacy scripts.
    from .domain_models import (
        BrokerEvent,
        BrokerEventType,
        BrokerPosition,
        BrokerSnapshot,
        Clock,
        ContributorAttribution,
        Direction,
        EntryLevels,
        FusionResult,
        LifecycleActionKind,
        LifecycleStage,
        Milestone,
        QuoteSnapshot,
        ReservationState,
        RiskReservation,
        SignalContribution,
        SymbolSnapshot,
        SystemClock,
        TradeIntent,
        decimal_value,
    )
    try:
        from .agent_roster import STRATEGY_AGENT_IDS as _ROSTER_STRATEGY_AGENT_IDS
    except ImportError:  # Canonical roster may be deployed one migration earlier.
        _ROSTER_STRATEGY_AGENT_IDS = tuple(range(4, 14))
except ImportError:  # pragma: no cover - compatibility with direct script imports
    from domain_models import (  # type: ignore
        BrokerEvent,
        BrokerEventType,
        BrokerPosition,
        BrokerSnapshot,
        Clock,
        ContributorAttribution,
        Direction,
        EntryLevels,
        FusionResult,
        LifecycleActionKind,
        LifecycleStage,
        Milestone,
        QuoteSnapshot,
        ReservationState,
        RiskReservation,
        SignalContribution,
        SymbolSnapshot,
        SystemClock,
        TradeIntent,
        decimal_value,
    )
    try:
        from agent_roster import STRATEGY_AGENT_IDS as _ROSTER_STRATEGY_AGENT_IDS  # type: ignore
    except ImportError:
        _ROSTER_STRATEGY_AGENT_IDS = tuple(range(4, 14))


STRATEGY_AGENT_IDS = frozenset(_ROSTER_STRATEGY_AGENT_IDS)
ZERO = Decimal("0")
ONE = Decimal("1")
HUNDRED = Decimal("100")


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if value <= 0:
        return ZERO
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def _ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


@dataclass(frozen=True, slots=True)
class ValidationResult:
    valid: bool
    reason: str = "VALID"


@dataclass(frozen=True, slots=True)
class EntryPolicyConfig:
    max_quote_age_ms: int = 5_000
    require_tick_alignment: bool = True
    max_spread_points: Decimal = Decimal("300")
    min_tp1_reward_to_risk: Decimal = Decimal("0.50")
    min_tp2_reward_to_risk: Decimal = Decimal("1.00")
    min_tp3_reward_to_risk: Decimal = Decimal("1.50")

    def __post_init__(self) -> None:
        for name in (
            "max_spread_points",
            "min_tp1_reward_to_risk",
            "min_tp2_reward_to_risk",
            "min_tp3_reward_to_risk",
        ):
            object.__setattr__(self, name, decimal_value(getattr(self, name)))
        if self.max_quote_age_ms < 1 or self.max_spread_points <= 0:
            raise ValueError("entry quote age and spread limits must be positive")
        if not (
            ZERO < self.min_tp1_reward_to_risk
            <= self.min_tp2_reward_to_risk
            <= self.min_tp3_reward_to_risk
        ):
            raise ValueError("minimum TP reward-to-risk thresholds must be positive and ordered")


class EntryPolicy:
    """Directional geometry and broker-executability validation."""

    def __init__(
        self,
        config: EntryPolicyConfig | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.config = config or EntryPolicyConfig()
        self.clock = clock or SystemClock()

    @staticmethod
    def validate_levels(
        direction: Direction,
        levels: EntryLevels,
        executable_price: Decimal | None = None,
    ) -> ValidationResult:
        price = levels.entry if executable_price is None else decimal_value(executable_price)
        values = (levels.entry, levels.sl, levels.tp1, levels.tp2, levels.tp3, price)
        if any(value <= 0 or not value.is_finite() for value in values):
            return ValidationResult(False, "LEVEL_NOT_POSITIVE_FINITE")

        if direction == Direction.BUY:
            if not (levels.sl < price < levels.tp1 < levels.tp2 < levels.tp3):
                return ValidationResult(False, "INVALID_BUY_GEOMETRY")
        elif direction == Direction.SELL:
            if not (levels.tp3 < levels.tp2 < levels.tp1 < price < levels.sl):
                return ValidationResult(False, "INVALID_SELL_GEOMETRY")
        else:  # Defensive even though Direction is an enum.
            return ValidationResult(False, "INVALID_DIRECTION")
        return ValidationResult(True)

    @staticmethod
    def _tick_aligned(value: Decimal, tick_size: Decimal) -> bool:
        return value % tick_size == 0

    def validate_for_broker(
        self,
        intent: TradeIntent,
        broker: BrokerSnapshot,
    ) -> ValidationResult:
        now = self.clock.now_ms()
        if now >= intent.expires_at_ms:
            return ValidationResult(False, "INTENT_EXPIRED")
        if intent.symbol != broker.symbol.symbol or intent.symbol != broker.quote.symbol:
            return ValidationResult(False, "SYMBOL_MISMATCH")
        if not broker.symbol.trade_enabled:
            return ValidationResult(False, "SYMBOL_TRADING_DISABLED")
        quote_age = now - broker.quote.captured_at_ms
        if quote_age < 0 or quote_age > self.config.max_quote_age_ms:
            return ValidationResult(False, "STALE_OR_FUTURE_QUOTE")

        executable = (
            broker.quote.ask if intent.direction == Direction.BUY else broker.quote.bid
        )
        reference_geometry = self.validate_levels(intent.direction, intent.levels)
        if not reference_geometry.valid:
            return reference_geometry
        geometry = self.validate_levels(intent.direction, intent.levels, executable)
        if not geometry.valid:
            return geometry

        point = broker.symbol.point
        spread_points = (broker.quote.ask - broker.quote.bid) / point
        if spread_points > self.config.max_spread_points:
            return ValidationResult(False, "MAX_SPREAD_EXCEEDED")
        slippage_points = abs(executable - intent.reference_price) / point
        if slippage_points > intent.max_slippage_points:
            return ValidationResult(False, "MAX_SLIPPAGE_EXCEEDED")

        min_distance = max(
            broker.symbol.stops_level_points,
            broker.symbol.freeze_level_points,
        ) * point
        if intent.direction == Direction.BUY:
            sl_distance = executable - intent.levels.sl
            tp_distance = intent.levels.tp1 - executable
            rewards = (
                intent.levels.tp1 - executable,
                intent.levels.tp2 - executable,
                intent.levels.tp3 - executable,
            )
        else:
            sl_distance = intent.levels.sl - executable
            tp_distance = executable - intent.levels.tp1
            rewards = (
                executable - intent.levels.tp1,
                executable - intent.levels.tp2,
                executable - intent.levels.tp3,
            )
        if sl_distance < min_distance or tp_distance < min_distance:
            return ValidationResult(False, "BROKER_STOPS_LEVEL_VIOLATION")

        reward_to_risk = tuple(reward / sl_distance for reward in rewards)
        minimums = (
            self.config.min_tp1_reward_to_risk,
            self.config.min_tp2_reward_to_risk,
            self.config.min_tp3_reward_to_risk,
        )
        if any(actual < minimum for actual, minimum in zip(reward_to_risk, minimums)):
            return ValidationResult(False, "MIN_REWARD_TO_RISK_VIOLATION")

        if self.config.require_tick_alignment:
            for value in (
                intent.levels.sl,
                intent.levels.tp1,
                intent.levels.tp2,
                intent.levels.tp3,
            ):
                if not self._tick_aligned(value, broker.symbol.tick_size):
                    return ValidationResult(False, "PRICE_NOT_TICK_ALIGNED")
        return ValidationResult(True)


@dataclass(frozen=True, slots=True)
class FusionPolicyConfig:
    min_agents_agreeing: int = 3
    min_confluence_pct: Decimal = Decimal("75")
    min_directional_dominance: Decimal = Decimal("0.60")
    max_signal_age_ms: int = 30_000

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "min_confluence_pct", decimal_value(self.min_confluence_pct)
        )
        object.__setattr__(
            self,
            "min_directional_dominance",
            decimal_value(self.min_directional_dominance),
        )
        if self.min_agents_agreeing < 3:
            raise ValueError("canonical fusion quorum cannot be lower than three")
        if not ZERO <= self.min_confluence_pct <= HUNDRED:
            raise ValueError("confluence threshold must be between 0 and 100")
        if not Decimal("0.5") <= self.min_directional_dominance <= ONE:
            raise ValueError("directional dominance must be between 0.5 and 1")


@dataclass(frozen=True, slots=True)
class FusionDecision:
    accepted: bool
    reason: str
    result: FusionResult | None = None


class FusionPolicy:
    """Fuse one fresh, valid signal per strategy agent without global state."""

    def __init__(
        self,
        config: FusionPolicyConfig | None = None,
        clock: Clock | None = None,
        entry_policy: EntryPolicy | None = None,
        agent_weights: Mapping[int, Decimal] | None = None,
    ) -> None:
        self.config = config or FusionPolicyConfig()
        self.clock = clock or SystemClock()
        self.entry_policy = entry_policy or EntryPolicy(clock=self.clock)
        weights = agent_weights or {}
        self._weights = {
            agent_id: decimal_value(weights.get(agent_id, ONE))
            for agent_id in STRATEGY_AGENT_IDS
        }
        if any(weight <= 0 for weight in self._weights.values()):
            raise ValueError("agent weights must be positive")

    def evaluate(
        self,
        signals: Sequence[SignalContribution],
        fusion_id: str,
    ) -> FusionDecision:
        if not fusion_id.strip():
            raise ValueError("fusion_id must be non-empty")
        if not signals:
            return FusionDecision(False, "NO_SIGNALS")

        now = self.clock.now_ms()
        candidates: list[SignalContribution] = []
        seen_agents: set[int] = set()
        snapshot_ids: set[str] = set()
        regimes: set[str] = set()
        sessions: set[str] = set()

        for signal in signals:
            if signal.agent_id not in STRATEGY_AGENT_IDS:
                return FusionDecision(False, "NON_STRATEGY_AGENT")
            if signal.agent_id in seen_agents:
                return FusionDecision(False, "DUPLICATE_AGENT_SIGNAL")
            seen_agents.add(signal.agent_id)
            snapshot_ids.add(signal.snapshot_id)
            regimes.add(signal.regime)
            sessions.add(signal.session)

            age = now - signal.observed_at_ms
            if age < 0 or age > self.config.max_signal_age_ms:
                return FusionDecision(False, "STALE_OR_FUTURE_SIGNAL")
            if not (
                signal.htf_aligned
                and signal.session_valid
                and signal.volume_confirmed
                and signal.context_enabled
            ):
                continue
            # Zero conviction is an explicit abstention, not agreement.  It
            # must never be usable to manufacture the three-agent quorum.
            if signal.conviction <= ZERO:
                continue
            geometry = self.entry_policy.validate_levels(signal.direction, signal.levels)
            if not geometry.valid:
                return FusionDecision(False, f"INVALID_SIGNAL:{geometry.reason}")
            candidates.append(signal)

        if len(snapshot_ids) != 1:
            return FusionDecision(False, "MIXED_SNAPSHOTS")
        if len(regimes) != 1 or len(sessions) != 1:
            return FusionDecision(False, "MIXED_SIGNAL_CONTEXT")
        if len(candidates) < self.config.min_agents_agreeing:
            return FusionDecision(False, "INSUFFICIENT_ELIGIBLE_SIGNALS")

        directional_scores = {
            direction: sum(
                (self._weights[s.agent_id] * s.conviction for s in candidates
                 if s.direction == direction),
                ZERO,
            )
            for direction in Direction
        }
        buy_score = directional_scores[Direction.BUY]
        sell_score = directional_scores[Direction.SELL]
        if buy_score == sell_score:
            return FusionDecision(False, "DIRECTION_TIE")
        winner = Direction.BUY if buy_score > sell_score else Direction.SELL
        winner_score = directional_scores[winner]
        total_score = buy_score + sell_score
        dominance = winner_score / total_score if total_score > 0 else ZERO
        if dominance < self.config.min_directional_dominance:
            return FusionDecision(False, "DIRECTIONAL_DOMINANCE_TOO_LOW")

        agreeing = [s for s in candidates if s.direction == winner]
        if len(agreeing) < self.config.min_agents_agreeing:
            return FusionDecision(False, "INSUFFICIENT_DIRECTIONAL_QUORUM")
        max_possible = sum(
            (self._weights[s.agent_id] * HUNDRED for s in agreeing), ZERO
        )
        confluence = winner_score / max_possible * HUNDRED
        if confluence < self.config.min_confluence_pct:
            return FusionDecision(False, "CONFLUENCE_TOO_LOW")

        level_weight = sum(
            (self._weights[s.agent_id] * s.conviction for s in agreeing), ZERO
        )
        if level_weight <= 0:
            return FusionDecision(False, "ZERO_LEVEL_WEIGHT")

        def weighted_level(name: str) -> Decimal:
            return sum(
                (
                    self._weights[s.agent_id]
                    * s.conviction
                    * getattr(s.levels, name)
                    for s in agreeing
                ),
                ZERO,
            ) / level_weight

        levels = EntryLevels(
            entry=weighted_level("entry"),
            sl=weighted_level("sl"),
            tp1=weighted_level("tp1"),
            tp2=weighted_level("tp2"),
            tp3=weighted_level("tp3"),
        )
        geometry = self.entry_policy.validate_levels(winner, levels)
        if not geometry.valid:
            return FusionDecision(False, f"INVALID_FUSED_LEVELS:{geometry.reason}")

        attributions = tuple(
            ContributorAttribution(
                signal_id=s.signal_id,
                agent_id=s.agent_id,
                direction=s.direction,
                conviction=s.conviction,
                weight=self._weights[s.agent_id],
                weighted_score=self._weights[s.agent_id] * s.conviction,
                strategy_tags=s.strategy_tags,
                snapshot_id=s.snapshot_id,
                observed_at_ms=s.observed_at_ms,
            )
            for s in sorted(agreeing, key=lambda item: item.agent_id)
        )
        result = FusionResult(
            fusion_id=fusion_id,
            snapshot_id=next(iter(snapshot_ids)),
            direction=winner,
            confluence_pct=confluence,
            directional_dominance=dominance,
            levels=levels,
            regime=next(iter(regimes)),
            session=next(iter(sessions)),
            contributors=attributions,
            evaluated_at_ms=now,
        )
        return FusionDecision(True, "ACCEPTED", result)


@dataclass(frozen=True, slots=True)
class RiskPolicyConfig:
    hard_max_risk_pct: Decimal = Decimal("1.0")
    max_daily_drawdown_pct: Decimal = Decimal("3.0")
    max_total_risk_pct: Decimal = Decimal("1.0")
    max_open_positions: int = 1
    reservation_ttl_ms: int = 30_000
    max_snapshot_age_ms: int = 5_000

    def __post_init__(self) -> None:
        for name in (
            "hard_max_risk_pct",
            "max_daily_drawdown_pct",
            "max_total_risk_pct",
        ):
            object.__setattr__(self, name, decimal_value(getattr(self, name)))
        if not ZERO < self.hard_max_risk_pct <= ONE:
            raise ValueError("hard per-trade risk cap cannot exceed 1%")
        if self.max_daily_drawdown_pct <= 0 or self.max_total_risk_pct <= 0:
            raise ValueError("risk limits must be positive")
        if self.max_daily_drawdown_pct > Decimal("3"):
            raise ValueError("daily drawdown limit cannot exceed the 3% hard ceiling")
        if self.max_total_risk_pct > Decimal("3"):
            raise ValueError("total portfolio risk cannot exceed the 3% hard ceiling")
        if (
            not 1 <= self.max_open_positions <= 3
            or self.reservation_ttl_ms < 1
            or self.max_snapshot_age_ms < 1
        ):
            raise ValueError("positions must be 1-3 and reservation limits must be positive")


@dataclass(frozen=True, slots=True)
class RiskDecision:
    approved: bool
    reason: str
    volume: Decimal = ZERO
    risk_amount: Decimal = ZERO
    risk_pct: Decimal = ZERO
    reservation: RiskReservation | None = None


class RiskPolicy:
    """Broker-aware risk sizing with atomic-reservation inputs and hard caps."""

    ACTIVE_RESERVATION_STATES = frozenset(
        {
            ReservationState.RESERVED,
            ReservationState.SUBMITTED,
            ReservationState.UNKNOWN,
        }
    )

    def __init__(
        self,
        config: RiskPolicyConfig | None = None,
        clock: Clock | None = None,
        entry_policy: EntryPolicy | None = None,
    ) -> None:
        self.config = config or RiskPolicyConfig()
        self.clock = clock or SystemClock()
        self.entry_policy = entry_policy or EntryPolicy(clock=self.clock)

    def assess(
        self,
        intent: TradeIntent,
        broker: BrokerSnapshot,
        reservation_id: str,
        reservations: Sequence[RiskReservation] = (),
        modifiers: Sequence[Decimal] = (),
    ) -> RiskDecision:
        if not reservation_id.strip():
            raise ValueError("reservation_id must be non-empty")
        now = self.clock.now_ms()
        account = broker.account
        symbol = broker.symbol

        snapshot_ages = (
            now - broker.captured_at_ms,
            now - account.captured_at_ms,
        )
        if any(
            age < 0 or age > self.config.max_snapshot_age_ms
            for age in snapshot_ages
        ):
            return RiskDecision(False, "STALE_OR_FUTURE_BROKER_SNAPSHOT")
        if not broker.reconciled:
            return RiskDecision(False, "BROKER_NOT_RECONCILED")
        if not account.baseline_verified or account.day_start_equity <= 0:
            return RiskDecision(False, "RISK_BASELINE_UNVERIFIED")
        if account.daily_halted:
            return RiskDecision(False, "DAILY_HALT_ACTIVE")
        if broker.foreign_symbol_exposure:
            return RiskDecision(False, "FOREIGN_SYMBOL_EXPOSURE")

        entry_check = self.entry_policy.validate_for_broker(intent, broker)
        if not entry_check.valid:
            return RiskDecision(False, f"ENTRY_REJECTED:{entry_check.reason}")

        drawdown_pct = max(
            ZERO,
            (account.day_start_equity - account.equity)
            / account.day_start_equity
            * HUNDRED,
        )
        if drawdown_pct >= self.config.max_daily_drawdown_pct:
            return RiskDecision(False, "DAILY_DRAWDOWN_LIMIT")

        active_positions = [position for position in broker.positions if position.is_open]
        active_reservations = [
            reservation
            for reservation in reservations
            if reservation.account_id == account.account_id
            and reservation.state in self.ACTIVE_RESERVATION_STATES
        ]
        if len(active_positions) + len(active_reservations) >= self.config.max_open_positions:
            return RiskDecision(False, "MAX_POSITIONS_OR_RESERVATIONS")

        risk_base = min(account.balance, account.equity)
        if risk_base <= 0:
            return RiskDecision(False, "INVALID_RISK_BASE")

        modifier = ONE
        for raw_modifier in modifiers:
            item = decimal_value(raw_modifier)
            if item < 0:
                return RiskDecision(False, "NEGATIVE_RISK_MODIFIER")
            modifier *= min(item, ONE)  # Modifiers may reduce risk, never increase it.
        if modifier <= 0:
            return RiskDecision(False, "ZERO_RISK_BUDGET")

        if (
            intent.requested_risk_pct > self.config.hard_max_risk_pct
            or intent.requested_risk_pct > ONE
        ):
            return RiskDecision(False, "REQUESTED_RISK_EXCEEDS_HARD_CAP")
        requested_pct = intent.requested_risk_pct
        per_trade_budget = risk_base * requested_pct / HUNDRED * modifier

        total_cap = risk_base * self.config.max_total_risk_pct / HUNDRED
        committed = sum(
            (position.initial_risk_amount for position in active_positions), ZERO
        ) + sum((reservation.risk_amount for reservation in active_reservations), ZERO)
        available = max(ZERO, total_cap - committed)
        budget = min(per_trade_budget, available)
        if budget <= 0:
            return RiskDecision(False, "TOTAL_RISK_CAP")

        executable = (
            broker.quote.ask if intent.direction == Direction.BUY else broker.quote.bid
        )
        sl_distance = abs(executable - intent.levels.sl)
        risk_per_lot = sl_distance / symbol.tick_size * symbol.tick_value_per_lot
        if risk_per_lot <= 0:
            return RiskDecision(False, "INVALID_BROKER_RISK_VALUE")

        raw_volume = budget / risk_per_lot
        max_volume = symbol.volume_max
        if symbol.margin_per_lot is not None:
            margin_volume = account.free_margin / symbol.margin_per_lot
            max_volume = min(max_volume, margin_volume)
        volume = _floor_to_step(min(raw_volume, max_volume), symbol.volume_step)
        if volume < symbol.volume_min:
            return RiskDecision(False, "MIN_VOLUME_EXCEEDS_RISK_BUDGET")

        actual_risk = volume * risk_per_lot
        actual_pct = actual_risk / risk_base * HUNDRED
        if actual_pct > self.config.hard_max_risk_pct or actual_risk > budget:
            return RiskDecision(False, "NORMALISED_VOLUME_EXCEEDS_RISK_CAP")

        reservation = RiskReservation(
            reservation_id=reservation_id,
            trade_id=intent.trade_id,
            account_id=account.account_id,
            direction=intent.direction,
            risk_day=account.risk_day,
            risk_amount=actual_risk,
            volume=volume,
            state=ReservationState.RESERVED,
            created_at_ms=now,
            expires_at_ms=now + self.config.reservation_ttl_ms,
        )
        return RiskDecision(
            True,
            "APPROVED",
            volume=volume,
            risk_amount=actual_risk,
            risk_pct=actual_pct,
            reservation=reservation,
        )


@dataclass(frozen=True, slots=True)
class LifecyclePolicyConfig:
    tp1_initial_fraction: Decimal = Decimal("0.50")
    tp2_initial_fraction: Decimal = Decimal("0.25")
    # Canonical default: BE is planned only after the TP1 partial is confirmed.
    # Set this optional trigger to enable an explicitly configured earlier-R mode.
    early_break_even_trigger_r: Decimal | None = None
    break_even_buffer_points: int = 3
    # A trailing candidate is derived by LifecycleService from authenticated,
    # completed H1 bars.  Two bars on each side make the pivot immutable; the
    # buffer keeps the stop beyond that structure rather than on the exact low/high.
    structural_swing_left_bars: int = 2
    structural_swing_right_bars: int = 2
    structural_buffer_points: int = 10
    max_h1_bars: int = 48
    close_unadvanced_at_session_end: bool = True

    def __post_init__(self) -> None:
        for name in (
            "tp1_initial_fraction",
            "tp2_initial_fraction",
        ):
            object.__setattr__(self, name, decimal_value(getattr(self, name)))
        if self.early_break_even_trigger_r is not None:
            object.__setattr__(
                self,
                "early_break_even_trigger_r",
                decimal_value(self.early_break_even_trigger_r),
            )
        if self.tp1_initial_fraction <= 0 or self.tp2_initial_fraction <= 0:
            raise ValueError("partial fractions must be positive")
        if self.tp1_initial_fraction + self.tp2_initial_fraction >= ONE:
            raise ValueError("TP3 must retain a positive remainder")
        if (
            self.early_break_even_trigger_r is not None
            and self.early_break_even_trigger_r <= 0
        ):
            raise ValueError("lifecycle R thresholds must be positive")
        for name in (
            "break_even_buffer_points",
            "structural_swing_left_bars",
            "structural_swing_right_bars",
            "structural_buffer_points",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.structural_swing_left_bars < 1 or self.structural_swing_right_bars < 1:
            raise ValueError("structural swings require bars on both sides of the pivot")
        if not 1 <= self.max_h1_bars <= 10_000:
            raise ValueError("maximum H1 hold must be between 1 and 10000 bars")
        if not isinstance(self.close_unadvanced_at_session_end, bool):
            raise ValueError("session-close policy must be boolean")


@dataclass(frozen=True, slots=True)
class LifecycleAction:
    kind: LifecycleActionKind
    command_id: str | None
    milestone: Milestone
    reason: str
    close_volume: Decimal = ZERO
    new_sl: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "close_volume", decimal_value(self.close_volume))
        if self.new_sl is not None:
            object.__setattr__(self, "new_sl", decimal_value(self.new_sl))


@dataclass(frozen=True, slots=True)
class LifecycleDecision:
    action: LifecycleAction | None
    position: BrokerPosition
    reason: str


@dataclass(frozen=True, slots=True)
class LifecycleTransition:
    applied: bool
    reason: str
    position: BrokerPosition


class LifecyclePolicy:
    """Plan one action at a time and apply only broker-confirmed transitions."""

    def __init__(self, config: LifecyclePolicyConfig | None = None) -> None:
        self.config = config or LifecyclePolicyConfig()

    @staticmethod
    def _target_hit(direction: Direction, price: Decimal, target: Decimal) -> bool:
        return price >= target if direction == Direction.BUY else price <= target

    @staticmethod
    def _improves_sl(position: BrokerPosition, new_sl: Decimal) -> bool:
        return (
            new_sl > position.current_sl
            if position.direction == Direction.BUY
            else new_sl < position.current_sl
        )

    @staticmethod
    def _sl_is_broker_valid(
        position: BrokerPosition,
        new_sl: Decimal,
        exit_price: Decimal,
        symbol: SymbolSnapshot,
    ) -> bool:
        min_distance = max(
            symbol.stops_level_points,
            symbol.freeze_level_points,
        ) * symbol.point
        if position.direction == Direction.BUY:
            return new_sl <= exit_price - min_distance
        return new_sl >= exit_price + min_distance

    @staticmethod
    def _pending(
        position: BrokerPosition,
        action: LifecycleAction,
    ) -> BrokerPosition:
        close_volume = (
            action.close_volume
            if action.kind in {
                LifecycleActionKind.PARTIAL_CLOSE,
                LifecycleActionKind.CLOSE,
            }
            else ZERO
        )
        return replace(
            position,
            pending_action=action.kind,
            pending_milestone=action.milestone,
            pending_command_id=action.command_id,
            pending_close_volume=close_volume,
            pending_filled_volume=ZERO,
        )

    @staticmethod
    def _clear_pending(position: BrokerPosition, **changes: object) -> BrokerPosition:
        return replace(
            position,
            pending_action=None,
            pending_milestone=None,
            pending_command_id=None,
            pending_close_volume=ZERO,
            pending_filled_volume=ZERO,
            **changes,
        )

    def _partial_decision(
        self,
        position: BrokerPosition,
        symbol: SymbolSnapshot,
        command_id: str,
        milestone: Milestone,
        fraction: Decimal,
        confirmed_stage: LifecycleStage,
    ) -> LifecycleDecision:
        requested = _floor_to_step(
            position.initial_volume * fraction, symbol.volume_step
        )
        remaining_after = position.remaining_volume - requested
        if (
            requested < symbol.volume_min
            or remaining_after < symbol.volume_min
            or requested <= 0
        ):
            action = LifecycleAction(
                LifecycleActionKind.VIRTUAL_MILESTONE,
                None,
                milestone,
                "PARTIAL_NOT_BROKER_EXECUTABLE",
            )
            # This advances only a policy milestone; broker volume/SL remain unchanged.
            return LifecycleDecision(
                action,
                replace(position, lifecycle_stage=confirmed_stage),
                "VIRTUAL_MILESTONE_DUST_GUARD",
            )

        action = LifecycleAction(
            LifecycleActionKind.PARTIAL_CLOSE,
            command_id,
            milestone,
            f"{milestone.value}_PARTIAL",
            close_volume=requested,
        )
        return LifecycleDecision(action, self._pending(position, action), "ACTION_PLANNED")

    def plan(
        self,
        position: BrokerPosition,
        quote: QuoteSnapshot,
        symbol: SymbolSnapshot,
        command_id: str,
        *,
        h1_bars_elapsed: int | None = None,
        session_close_due: bool = False,
        structural_trailing_sl: Decimal | str | int | float | None = None,
    ) -> LifecycleDecision:
        if position.symbol != quote.symbol or position.symbol != symbol.symbol:
            return LifecycleDecision(None, position, "SYMBOL_MISMATCH")
        if not position.is_open:
            return LifecycleDecision(None, position, "POSITION_NOT_OPEN")
        if position.lifecycle_stage == LifecycleStage.QUARANTINED:
            return LifecycleDecision(None, position, "POSITION_QUARANTINED")
        if position.pending_action is not None:
            return LifecycleDecision(None, position, "ACTION_ALREADY_IN_FLIGHT")
        if not command_id.strip():
            raise ValueError("command_id must be non-empty")
        if h1_bars_elapsed is not None and (
            isinstance(h1_bars_elapsed, bool) or h1_bars_elapsed < 0
        ):
            raise ValueError("h1_bars_elapsed must be a non-negative integer")
        if not isinstance(session_close_due, bool):
            raise ValueError("session_close_due must be boolean")
        structural_candidate = None
        if structural_trailing_sl is not None:
            structural_candidate = decimal_value(structural_trailing_sl)
            if structural_candidate <= 0:
                raise ValueError("structural_trailing_sl must be positive when supplied")

        exit_price = quote.bid if position.direction == Direction.BUY else quote.ask

        # Terminal target has priority over queued milestones after a price gap.
        if self._target_hit(position.direction, exit_price, position.tp3):
            action = LifecycleAction(
                LifecycleActionKind.CLOSE,
                command_id,
                Milestone.TP3,
                "TP3_GAP_OR_TARGET",
                close_volume=position.remaining_volume,
            )
            return LifecycleDecision(action, self._pending(position, action), "ACTION_PLANNED")

        if (
            h1_bars_elapsed is not None
            and h1_bars_elapsed >= self.config.max_h1_bars
        ):
            action = LifecycleAction(
                LifecycleActionKind.CLOSE,
                command_id,
                Milestone.TIMEOUT,
                "MAX_H1_BARS_REACHED",
                close_volume=position.remaining_volume,
            )
            return LifecycleDecision(
                action, self._pending(position, action), "ACTION_PLANNED"
            )

        if (
            position.lifecycle_stage == LifecycleStage.PRE_TP1
            and self._target_hit(position.direction, exit_price, position.tp1)
        ):
            return self._partial_decision(
                position,
                symbol,
                command_id,
                Milestone.TP1,
                self.config.tp1_initial_fraction,
                LifecycleStage.TP1_CONFIRMED,
            )

        if (
            session_close_due
            and self.config.close_unadvanced_at_session_end
            and position.lifecycle_stage == LifecycleStage.PRE_TP1
        ):
            action = LifecycleAction(
                LifecycleActionKind.CLOSE,
                command_id,
                Milestone.SESSION_CLOSE,
                "UNADVANCED_SESSION_CLOSE",
                close_volume=position.remaining_volume,
            )
            return LifecycleDecision(
                action, self._pending(position, action), "ACTION_PLANNED"
            )

        initial_distance = abs(position.entry_price - position.original_sl)
        favourable_distance = (
            exit_price - position.entry_price
            if position.direction == Direction.BUY
            else position.entry_price - exit_price
        )
        current_r = favourable_distance / initial_distance if initial_distance > 0 else ZERO

        break_even_due = position.lifecycle_stage == LifecycleStage.TP1_CONFIRMED
        if (
            self.config.early_break_even_trigger_r is not None
            and position.lifecycle_stage == LifecycleStage.PRE_TP1
            and current_r >= self.config.early_break_even_trigger_r
        ):
            break_even_due = True

        if break_even_due:
            buffer = self.config.break_even_buffer_points * symbol.point
            raw_candidate = (
                position.entry_price + buffer
                if position.direction == Direction.BUY
                else position.entry_price - buffer
            )
            candidate = (
                _floor_to_step(raw_candidate, symbol.tick_size)
                if position.direction == Direction.BUY
                else _ceil_to_step(raw_candidate, symbol.tick_size)
            )
            if self._improves_sl(position, candidate) and self._sl_is_broker_valid(
                position, candidate, exit_price, symbol
            ):
                action = LifecycleAction(
                    LifecycleActionKind.MODIFY_SL,
                    command_id,
                    Milestone.BREAK_EVEN,
                    "BREAK_EVEN_TRIGGER",
                    new_sl=candidate,
                )
                return LifecycleDecision(
                    action, self._pending(position, action), "ACTION_PLANNED"
                )

        if (
            position.lifecycle_stage == LifecycleStage.TP1_CONFIRMED
            and self._target_hit(position.direction, exit_price, position.tp2)
        ):
            return self._partial_decision(
                position,
                symbol,
                command_id,
                Milestone.TP2,
                self.config.tp2_initial_fraction,
                LifecycleStage.TP2_CONFIRMED,
            )

        if (
            position.lifecycle_stage
            in (LifecycleStage.TP2_CONFIRMED, LifecycleStage.TRAILING)
            and structural_candidate is not None
        ):
            if self._improves_sl(position, structural_candidate) and self._sl_is_broker_valid(
                position, structural_candidate, exit_price, symbol
            ):
                action = LifecycleAction(
                    LifecycleActionKind.MODIFY_SL,
                    command_id,
                    Milestone.TRAILING,
                    "STRUCTURAL_TRAILING_IMPROVEMENT",
                    new_sl=structural_candidate,
                )
                return LifecycleDecision(
                    action, self._pending(position, action), "ACTION_PLANNED"
                )

        return LifecycleDecision(None, position, "NO_ACTION")

    def apply_broker_event(
        self,
        position: BrokerPosition,
        event: BrokerEvent,
    ) -> LifecycleTransition:
        if event.trade_id != position.trade_id:
            return LifecycleTransition(False, "TRADE_ID_MISMATCH", position)
        if event.broker_position_id != position.broker_position_id:
            return LifecycleTransition(False, "POSITION_ID_MISMATCH", position)

        command_matches = (
            position.pending_command_id is not None
            and event.command_id == position.pending_command_id
        )
        if (
            event.event_type
            in {
                BrokerEventType.POSITION_UPDATED,
                BrokerEventType.PARTIAL_FILLED,
                BrokerEventType.POSITION_CLOSED,
            }
            and event.position_version is None
        ):
            return LifecycleTransition(False, "BROKER_VERSION_REQUIRED", position)
        # A broker rejection proves that the requested position mutation did
        # not occur.  It clears the action lock but must not fabricate a new
        # broker position version.  A different version means another broker
        # mutation was missed and requires reconciliation instead.
        if event.event_type == BrokerEventType.COMMAND_REJECTED:
            if not command_matches:
                return LifecycleTransition(False, "UNMATCHED_COMMAND_REJECTION", position)
            if (
                event.position_version is not None
                and event.position_version != position.position_version
            ):
                return LifecycleTransition(
                    False, "REJECTION_POSITION_VERSION_MISMATCH", position
                )
            updated = self._clear_pending(position)
            return LifecycleTransition(True, "COMMAND_REJECTION_CONFIRMED", updated)

        if (
            event.position_version is not None
            and event.position_version <= position.position_version
        ):
            return LifecycleTransition(False, "DUPLICATE_OR_OLD_EVENT", position)
        next_version = (
            event.position_version
            if event.position_version is not None
            else position.position_version + 1
        )

        if event.event_type == BrokerEventType.PARTIAL_FILLED:
            if not command_matches or position.pending_action not in {
                LifecycleActionKind.PARTIAL_CLOSE,
                LifecycleActionKind.CLOSE,
            }:
                return LifecycleTransition(False, "UNMATCHED_PARTIAL_FILL", position)
            if event.remaining_volume is None:
                return LifecycleTransition(False, "PARTIAL_EVENT_MISSING_VOLUME", position)
            if not ZERO <= event.remaining_volume < position.remaining_volume:
                return LifecycleTransition(False, "INVALID_REMAINING_VOLUME", position)
            fill_delta = position.remaining_volume - event.remaining_volume
            if event.fill_volume is not None:
                if event.fill_volume != fill_delta:
                    return LifecycleTransition(False, "PARTIAL_VOLUME_MISMATCH", position)
            cumulative_fill = position.pending_filled_volume + fill_delta
            if cumulative_fill > position.pending_close_volume:
                if event.remaining_volume == ZERO:
                    stage = LifecycleStage.CLOSED
                else:
                    stage = LifecycleStage.QUARANTINED
                updated = self._clear_pending(
                    position,
                    remaining_volume=event.remaining_volume,
                    realised_pnl=position.realised_pnl + event.net_pnl,
                    lifecycle_stage=stage,
                    position_version=next_version,
                )
                return LifecycleTransition(
                    True, "PARTIAL_OVERFILL_QUARANTINED", updated
                )
            if cumulative_fill < position.pending_close_volume:
                updated = replace(
                    position,
                    remaining_volume=event.remaining_volume,
                    realised_pnl=position.realised_pnl + event.net_pnl,
                    pending_filled_volume=cumulative_fill,
                    position_version=next_version,
                )
                return LifecycleTransition(
                    True, "PARTIAL_FILL_PROGRESS_CONFIRMED", updated
                )
            stage = LifecycleStage.CLOSED if event.remaining_volume == ZERO else (
                LifecycleStage.TP1_CONFIRMED
                if position.pending_milestone == Milestone.TP1
                else LifecycleStage.TP2_CONFIRMED
            )
            updated = self._clear_pending(
                position,
                remaining_volume=event.remaining_volume,
                realised_pnl=position.realised_pnl + event.net_pnl,
                lifecycle_stage=stage,
                position_version=next_version,
            )
            return LifecycleTransition(True, "PARTIAL_FILL_CONFIRMED", updated)

        if event.event_type == BrokerEventType.POSITION_UPDATED:
            if not command_matches or position.pending_action != LifecycleActionKind.MODIFY_SL:
                return LifecycleTransition(False, "UNMATCHED_POSITION_UPDATE", position)
            if event.sl is None or not self._improves_sl(position, event.sl):
                return LifecycleTransition(False, "INVALID_CONFIRMED_SL", position)
            stage = (
                LifecycleStage.TRAILING
                if position.pending_milestone == Milestone.TRAILING
                else position.lifecycle_stage
            )
            updated = self._clear_pending(
                position,
                current_sl=event.sl,
                lifecycle_stage=stage,
                position_version=next_version,
            )
            return LifecycleTransition(True, "POSITION_UPDATE_CONFIRMED", updated)

        if event.event_type == BrokerEventType.POSITION_CLOSED:
            if event.remaining_volume not in (None, ZERO):
                return LifecycleTransition(False, "CLOSE_EVENT_HAS_REMAINING_VOLUME", position)
            updated = self._clear_pending(
                position,
                remaining_volume=ZERO,
                realised_pnl=position.realised_pnl + event.net_pnl,
                lifecycle_stage=LifecycleStage.CLOSED,
                position_version=next_version,
            )
            return LifecycleTransition(True, "POSITION_CLOSE_CONFIRMED", updated)

        return LifecycleTransition(False, "EVENT_NOT_APPLICABLE", position)


__all__ = [
    "EntryPolicy",
    "EntryPolicyConfig",
    "FusionDecision",
    "FusionPolicy",
    "FusionPolicyConfig",
    "LifecycleAction",
    "LifecycleDecision",
    "LifecyclePolicy",
    "LifecyclePolicyConfig",
    "LifecycleTransition",
    "RiskDecision",
    "RiskPolicy",
    "RiskPolicyConfig",
    "STRATEGY_AGENT_IDS",
    "ValidationResult",
]
