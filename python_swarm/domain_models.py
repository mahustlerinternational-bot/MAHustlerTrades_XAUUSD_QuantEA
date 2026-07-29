"""Canonical, immutable trading-domain contracts for QUANT-SWARM-XAU.

These models deliberately have no dependency on the legacy orchestrators.  They
form the fail-closed boundary shared by fusion, risk, execution and lifecycle
policies.  Financial values use :class:`~decimal.Decimal`; callers may pass
strings, ints or floats and values are normalised once at construction.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType
from typing import Mapping, Protocol, runtime_checkable


DecimalLike = Decimal | str | int | float


def decimal_value(value: DecimalLike) -> Decimal:
    """Convert a value to a finite ``Decimal`` without binary-float arithmetic."""
    result = value if isinstance(value, Decimal) else Decimal(str(value))
    if not result.is_finite():
        raise ValueError("financial values must be finite")
    return result


def _normalise_decimal(instance: object, *field_names: str) -> None:
    for name in field_names:
        value = getattr(instance, name)
        if value is not None:
            object.__setattr__(instance, name, decimal_value(value))


def _require_non_empty(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")


def _freeze_payload(value: object) -> object:
    """Recursively freeze command payload containers."""
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze_payload(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_payload(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze_payload(item) for item in value)
    return value


@runtime_checkable
class Clock(Protocol):
    """Injected time source used by all policies and deterministic tests."""

    def now_ms(self) -> int:
        """Return UTC Unix time in milliseconds."""


@dataclass(frozen=True, slots=True)
class SystemClock:
    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000


@dataclass(frozen=True, slots=True)
class FixedClock:
    utc_ms: int

    def now_ms(self) -> int:
        return self.utc_ms


class Direction(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class AccountMode(StrEnum):
    HEDGING = "HEDGING"
    NETTING = "NETTING"


class ReservationState(StrEnum):
    RESERVED = "RESERVED"
    SUBMITTED = "SUBMITTED"
    UNKNOWN = "UNKNOWN"
    CONSUMED = "CONSUMED"
    RELEASED = "RELEASED"


class CommandAction(StrEnum):
    OPEN = "OPEN"
    MODIFY_SL = "MODIFY_SL"
    PARTIAL_CLOSE = "PARTIAL_CLOSE"
    CLOSE = "CLOSE"
    CLOSE_ALL = "CLOSE_ALL"
    REQUEST_SNAPSHOT = "REQUEST_SNAPSHOT"


class CommandStatus(StrEnum):
    CREATED = "CREATED"
    SENT = "SENT"
    RECEIVED = "RECEIVED"
    APPLIED = "APPLIED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


class BrokerEventType(StrEnum):
    COMMAND_REJECTED = "COMMAND_REJECTED"
    POSITION_OPENED = "POSITION_OPENED"
    POSITION_UPDATED = "POSITION_UPDATED"
    PARTIAL_FILLED = "PARTIAL_FILLED"
    POSITION_CLOSED = "POSITION_CLOSED"


class LifecycleStage(StrEnum):
    PRE_TP1 = "PRE_TP1"
    TP1_CONFIRMED = "TP1_CONFIRMED"
    TP2_CONFIRMED = "TP2_CONFIRMED"
    TRAILING = "TRAILING"
    CLOSE_PENDING = "CLOSE_PENDING"
    CLOSED = "CLOSED"
    QUARANTINED = "QUARANTINED"


class LifecycleActionKind(StrEnum):
    PARTIAL_CLOSE = "PARTIAL_CLOSE"
    MODIFY_SL = "MODIFY_SL"
    CLOSE = "CLOSE"
    VIRTUAL_MILESTONE = "VIRTUAL_MILESTONE"


class Milestone(StrEnum):
    TP1 = "TP1"
    TP2 = "TP2"
    TP3 = "TP3"
    BREAK_EVEN = "BREAK_EVEN"
    TRAILING = "TRAILING"
    TIMEOUT = "TIMEOUT"
    SESSION_CLOSE = "SESSION_CLOSE"


@dataclass(frozen=True, slots=True)
class EntryLevels:
    entry: Decimal
    sl: Decimal
    tp1: Decimal
    tp2: Decimal
    tp3: Decimal

    def __post_init__(self) -> None:
        _normalise_decimal(self, "entry", "sl", "tp1", "tp2", "tp3")


@dataclass(frozen=True, slots=True)
class QuoteSnapshot:
    symbol: str
    bid: Decimal
    ask: Decimal
    captured_at_ms: int

    def __post_init__(self) -> None:
        _require_non_empty(self.symbol, "symbol")
        _normalise_decimal(self, "bid", "ask")
        if self.bid <= 0 or self.ask <= 0 or self.ask < self.bid:
            raise ValueError("quote must satisfy 0 < bid <= ask")


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    account_id: str
    currency: str
    mode: AccountMode
    balance: Decimal
    equity: Decimal
    free_margin: Decimal
    risk_day: str
    day_start_equity: Decimal
    baseline_verified: bool
    daily_halted: bool
    captured_at_ms: int

    def __post_init__(self) -> None:
        _require_non_empty(self.account_id, "account_id")
        _require_non_empty(self.currency, "currency")
        _require_non_empty(self.risk_day, "risk_day")
        _normalise_decimal(
            self, "balance", "equity", "free_margin", "day_start_equity"
        )
        if min(self.balance, self.equity, self.free_margin) < 0:
            raise ValueError("account monetary values cannot be negative")


@dataclass(frozen=True, slots=True)
class SymbolSnapshot:
    symbol: str
    digits: int
    tick_size: Decimal
    tick_value_per_lot: Decimal
    contract_size: Decimal
    volume_min: Decimal
    volume_step: Decimal
    volume_max: Decimal
    stops_level_points: int = 0
    freeze_level_points: int = 0
    margin_per_lot: Decimal | None = None
    trade_enabled: bool = True

    def __post_init__(self) -> None:
        _require_non_empty(self.symbol, "symbol")
        _normalise_decimal(
            self,
            "tick_size",
            "tick_value_per_lot",
            "contract_size",
            "volume_min",
            "volume_step",
            "volume_max",
            "margin_per_lot",
        )
        if self.digits < 0 or self.stops_level_points < 0 or self.freeze_level_points < 0:
            raise ValueError("symbol precision and broker levels cannot be negative")
        if min(
            self.tick_size,
            self.tick_value_per_lot,
            self.contract_size,
            self.volume_min,
            self.volume_step,
            self.volume_max,
        ) <= 0:
            raise ValueError("symbol sizing metadata must be positive")
        if self.volume_min > self.volume_max:
            raise ValueError("volume_min cannot exceed volume_max")
        if self.margin_per_lot is not None and self.margin_per_lot <= 0:
            raise ValueError("margin_per_lot must be positive when supplied")

    @property
    def point(self) -> Decimal:
        return Decimal(1).scaleb(-self.digits)


@dataclass(frozen=True, slots=True)
class SignalContribution:
    signal_id: str
    snapshot_id: str
    agent_id: int
    direction: Direction
    conviction: Decimal
    levels: EntryLevels
    observed_at_ms: int
    regime: str
    session: str
    strategy_tags: tuple[str, ...] = ()
    htf_aligned: bool = True
    session_valid: bool = True
    volume_confirmed: bool = True
    context_enabled: bool = True

    def __post_init__(self) -> None:
        _require_non_empty(self.signal_id, "signal_id")
        _require_non_empty(self.snapshot_id, "snapshot_id")
        _require_non_empty(self.regime, "regime")
        _require_non_empty(self.session, "session")
        _normalise_decimal(self, "conviction")
        object.__setattr__(self, "strategy_tags", tuple(self.strategy_tags))
        if not Decimal("0") <= self.conviction <= Decimal("100"):
            raise ValueError("conviction must be an unsigned value from 0 to 100")


@dataclass(frozen=True, slots=True)
class ContributorAttribution:
    signal_id: str
    agent_id: int
    direction: Direction
    conviction: Decimal
    weight: Decimal
    weighted_score: Decimal
    strategy_tags: tuple[str, ...] = ()
    snapshot_id: str = ""
    observed_at_ms: int = 0

    def __post_init__(self) -> None:
        _normalise_decimal(self, "conviction", "weight", "weighted_score")
        object.__setattr__(self, "strategy_tags", tuple(self.strategy_tags))
        if bool(self.snapshot_id) != bool(self.observed_at_ms):
            raise ValueError(
                "attribution snapshot_id and observed_at_ms must be supplied together"
            )
        if self.snapshot_id:
            _require_non_empty(self.snapshot_id, "snapshot_id")
            if self.observed_at_ms < 1:
                raise ValueError("attribution observed_at_ms must be positive")


@dataclass(frozen=True, slots=True)
class FusionResult:
    fusion_id: str
    snapshot_id: str
    direction: Direction
    confluence_pct: Decimal
    directional_dominance: Decimal
    levels: EntryLevels
    regime: str
    session: str
    contributors: tuple[ContributorAttribution, ...]
    evaluated_at_ms: int

    def __post_init__(self) -> None:
        _require_non_empty(self.regime, "regime")
        _require_non_empty(self.session, "session")
        _normalise_decimal(self, "confluence_pct", "directional_dominance")
        object.__setattr__(self, "contributors", tuple(self.contributors))


@dataclass(frozen=True, slots=True)
class TradeIntent:
    intent_id: str
    trade_id: str
    fusion_id: str
    symbol: str
    magic: int
    direction: Direction
    levels: EntryLevels
    reference_price: Decimal
    max_slippage_points: int
    requested_risk_pct: Decimal
    contributors: tuple[ContributorAttribution, ...]
    regime: str
    session: str
    created_at_ms: int
    expires_at_ms: int
    config_hash: str
    decision_id: str = ""
    snapshot_id: str = ""

    def __post_init__(self) -> None:
        for name in (
            "intent_id",
            "trade_id",
            "fusion_id",
            "symbol",
            "regime",
            "session",
            "config_hash",
        ):
            _require_non_empty(getattr(self, name), name)
        if bool(self.decision_id) != bool(self.snapshot_id):
            raise ValueError(
                "intent decision_id and snapshot_id must be supplied together"
            )
        if self.decision_id:
            _require_non_empty(self.decision_id, "decision_id")
            _require_non_empty(self.snapshot_id, "snapshot_id")
        _normalise_decimal(self, "reference_price", "requested_risk_pct")
        object.__setattr__(self, "contributors", tuple(self.contributors))
        if self.reference_price <= 0 or self.requested_risk_pct <= 0:
            raise ValueError("intent price and requested risk must be positive")
        if self.max_slippage_points < 0 or self.expires_at_ms <= self.created_at_ms:
            raise ValueError("intent expiry/slippage is invalid")


@dataclass(frozen=True, slots=True)
class RiskReservation:
    reservation_id: str
    trade_id: str
    account_id: str
    direction: Direction
    risk_day: str
    risk_amount: Decimal
    volume: Decimal
    state: ReservationState
    created_at_ms: int
    expires_at_ms: int

    def __post_init__(self) -> None:
        for name in ("reservation_id", "trade_id", "account_id", "risk_day"):
            _require_non_empty(getattr(self, name), name)
        _normalise_decimal(self, "risk_amount", "volume")
        if self.risk_amount <= 0 or self.volume <= 0:
            raise ValueError("reservation risk and volume must be positive")
        if self.expires_at_ms <= self.created_at_ms:
            raise ValueError("reservation expiry must be after creation")


@dataclass(frozen=True, slots=True)
class BrokerCommand:
    command_id: str
    trade_id: str
    action: CommandAction
    status: CommandStatus
    created_at_ms: int
    expires_at_ms: int
    position_id: str | None = None
    expected_position_version: int | None = None
    payload: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_non_empty(self.command_id, "command_id")
        _require_non_empty(self.trade_id, "trade_id")
        object.__setattr__(self, "payload", _freeze_payload(self.payload))
        if self.expires_at_ms <= self.created_at_ms:
            raise ValueError("command expiry must be after creation")


@dataclass(frozen=True, slots=True)
class BrokerEvent:
    event_id: str
    event_type: BrokerEventType
    broker_time_ms: int
    received_at_ms: int
    command_id: str | None = None
    trade_id: str | None = None
    broker_position_id: str | None = None
    position_ticket: str | None = None
    order_id: str | None = None
    deal_id: str | None = None
    fill_price: Decimal | None = None
    fill_volume: Decimal | None = None
    remaining_volume: Decimal | None = None
    sl: Decimal | None = None
    tp: Decimal | None = None
    profit: Decimal = Decimal("0")
    commission: Decimal = Decimal("0")
    swap: Decimal = Decimal("0")
    fee: Decimal = Decimal("0")
    reason: str = ""
    position_version: int | None = None

    def __post_init__(self) -> None:
        _require_non_empty(self.event_id, "event_id")
        _normalise_decimal(
            self,
            "fill_price",
            "fill_volume",
            "remaining_volume",
            "sl",
            "tp",
            "profit",
            "commission",
            "swap",
            "fee",
        )
        if self.fill_volume is not None and self.fill_volume <= 0:
            raise ValueError("fill_volume must be positive when supplied")
        if self.remaining_volume is not None and self.remaining_volume < 0:
            raise ValueError("remaining_volume cannot be negative")

    @property
    def net_pnl(self) -> Decimal:
        return self.profit + self.commission + self.swap + self.fee


@dataclass(frozen=True, slots=True)
class BrokerPosition:
    trade_id: str
    broker_position_id: str
    position_ticket: str
    symbol: str
    magic: int
    direction: Direction
    initial_volume: Decimal
    remaining_volume: Decimal
    entry_price: Decimal
    original_sl: Decimal
    current_sl: Decimal
    tp1: Decimal
    tp2: Decimal
    tp3: Decimal
    initial_risk_amount: Decimal
    lifecycle_stage: LifecycleStage
    position_version: int
    opened_at_ms: int
    realised_pnl: Decimal = Decimal("0")
    pending_action: LifecycleActionKind | None = None
    pending_milestone: Milestone | None = None
    pending_command_id: str | None = None
    pending_close_volume: Decimal = Decimal("0")
    pending_filled_volume: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        for name in ("trade_id", "broker_position_id", "position_ticket", "symbol"):
            _require_non_empty(getattr(self, name), name)
        _normalise_decimal(
            self,
            "initial_volume",
            "remaining_volume",
            "entry_price",
            "original_sl",
            "current_sl",
            "tp1",
            "tp2",
            "tp3",
            "initial_risk_amount",
            "realised_pnl",
            "pending_close_volume",
            "pending_filled_volume",
        )
        if self.initial_volume <= 0 or self.remaining_volume < 0:
            raise ValueError("position volumes are invalid")
        if self.remaining_volume > self.initial_volume:
            raise ValueError("remaining volume cannot exceed initial volume")
        if self.initial_risk_amount < 0:
            raise ValueError("initial risk cannot be negative")
        if self.position_version < 0:
            raise ValueError("position version cannot be negative")
        if (self.lifecycle_stage == LifecycleStage.CLOSED) != (self.remaining_volume == 0):
            raise ValueError("closed lifecycle state and remaining volume must agree")
        has_pending = self.pending_action is not None
        if has_pending != (self.pending_command_id is not None):
            raise ValueError("pending lifecycle action requires exactly one command id")
        if has_pending != (self.pending_milestone is not None):
            raise ValueError("pending lifecycle action requires exactly one milestone")
        if self.pending_action is LifecycleActionKind.VIRTUAL_MILESTONE:
            raise ValueError("virtual milestones cannot be broker-pending actions")
        if self.pending_close_volume < 0 or self.pending_filled_volume < 0:
            raise ValueError("pending lifecycle volumes cannot be negative")
        if self.pending_filled_volume > self.pending_close_volume:
            raise ValueError("pending filled volume cannot exceed requested close volume")
        volume_action = self.pending_action in {
            LifecycleActionKind.PARTIAL_CLOSE,
            LifecycleActionKind.CLOSE,
        }
        if volume_action != (self.pending_close_volume > 0):
            raise ValueError("pending close actions require an explicit target volume")
        if not volume_action and self.pending_filled_volume != 0:
            raise ValueError("only close actions may accumulate broker fills")

    @property
    def is_open(self) -> bool:
        return self.remaining_volume > 0 and self.lifecycle_stage != LifecycleStage.CLOSED

    @property
    def realised_r(self) -> Decimal:
        if self.initial_risk_amount <= 0:
            return Decimal("0")
        return self.realised_pnl / self.initial_risk_amount


@dataclass(frozen=True, slots=True)
class BrokerSnapshot:
    snapshot_id: str
    connection_id: str
    account: AccountSnapshot
    symbol: SymbolSnapshot
    quote: QuoteSnapshot
    positions: tuple[BrokerPosition, ...]
    captured_at_ms: int
    reconciled: bool
    foreign_symbol_exposure: bool = False

    def __post_init__(self) -> None:
        _require_non_empty(self.snapshot_id, "snapshot_id")
        _require_non_empty(self.connection_id, "connection_id")
        object.__setattr__(self, "positions", tuple(self.positions))
        if self.symbol.symbol != self.quote.symbol:
            raise ValueError("symbol and quote snapshots must refer to the same symbol")


__all__ = [
    "AccountMode",
    "AccountSnapshot",
    "BrokerCommand",
    "BrokerEvent",
    "BrokerEventType",
    "BrokerPosition",
    "BrokerSnapshot",
    "Clock",
    "CommandAction",
    "CommandStatus",
    "ContributorAttribution",
    "Direction",
    "EntryLevels",
    "FixedClock",
    "FusionResult",
    "LifecycleActionKind",
    "LifecycleStage",
    "Milestone",
    "QuoteSnapshot",
    "ReservationState",
    "RiskReservation",
    "SignalContribution",
    "SymbolSnapshot",
    "SystemClock",
    "TradeIntent",
    "decimal_value",
]
