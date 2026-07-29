"""Pure canonical strategy and regime pipeline for Agents 03 through 13.

The legacy strategy modules are intentionally not imported here: their
analysis path reads and writes journals, consults the wall clock, and uses
mutable process state.  This module accepts one immutable, broker-timestamped
snapshot and returns immutable decisions.  It performs no filesystem, network,
database, environment, random-number, or wall-clock access.

The strategy rules are deliberately bounded reference implementations.  They
make no profitability claim.  Promotion still requires replay, integration,
and the documented 30-to-60-day MT5 demo forward-test gate.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from enum import StrEnum
from math import isfinite
from types import MappingProxyType
from typing import Callable, Mapping, Sequence

try:
    from .agent_roster import AGENT_BY_ID, STRATEGY_AGENT_IDS
    from .domain_models import Direction, EntryLevels, SignalContribution, decimal_value
    from .trading_policies import EntryPolicy
except ImportError:  # pragma: no cover - legacy flat-import compatibility
    from agent_roster import AGENT_BY_ID, STRATEGY_AGENT_IDS  # type: ignore
    from domain_models import (  # type: ignore
        Direction,
        EntryLevels,
        SignalContribution,
        decimal_value,
    )
    from trading_policies import EntryPolicy  # type: ignore


ZERO = Decimal("0")
ONE = Decimal("1")
HUNDRED = Decimal("100")
BROKER_MODES = frozenset({"shadow", "demo", "live"})
CANONICAL_STRATEGY_IDS = tuple(range(4, 14))
if tuple(STRATEGY_AGENT_IDS) != CANONICAL_STRATEGY_IDS:  # pragma: no cover
    raise RuntimeError("canonical roster must expose strategy Agents 04 through 13")


def _decimal(value: object, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} cannot be boolean")
    try:
        result = decimal_value(value)  # type: ignore[arg-type]
    except (ValueError, TypeError, ArithmeticError) as exc:
        raise ValueError(f"{field_name} must be a finite decimal") from exc
    return result


def _require_text(value: str, field_name: str, *, max_length: int = 160) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    if len(value) > max_length:
        raise ValueError(f"{field_name} exceeds {max_length} characters")


def _require_integer(value: object, field_name: str, *, minimum: int = 1) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{field_name} must be an integer >= {minimum}")


def _status_value(value: object) -> str:
    raw = getattr(value, "value", value)
    return str(raw).strip().upper()


class DataMode(StrEnum):
    CHECK = "check"
    SHADOW = "shadow"
    DEMO = "demo"
    LIVE = "live"
    SIMULATION = "simulation"
    REPLAY = "replay"
    TEST = "test"


class MarketSource(StrEnum):
    BROKER = "broker"
    REPLAY = "replay"
    SUPPLIED_TEST = "supplied_test"


class EvidenceSource(StrEnum):
    BROKER = "broker"
    HTTP = "http"
    REPLAY = "replay"
    SUPPLIED_TEST = "supplied_test"


class ProviderHealth(StrEnum):
    STARTING = "STARTING"
    HEALTHY = "HEALTHY"
    STALE = "STALE"
    ERROR = "ERROR"
    DISABLED = "DISABLED"


class MarketSession(StrEnum):
    ASIA = "ASIA"
    LONDON = "LONDON"
    NEW_YORK = "NEW_YORK"
    OFF = "OFF"


class CanonicalRegime(StrEnum):
    TRENDING_BULL = "trending_bull"
    TRENDING_BEAR = "trending_bear"
    RANGING_TIGHT = "ranging_tight"
    RANGING_WIDE = "ranging_wide"
    BREAKOUT_IMMINENT = "breakout_imminent"
    HIGH_IMPACT_NEWS = "high_impact_news"
    INSTITUTIONAL_HUNT = "institutional_hunt"
    REGIME_TRANSITION = "regime_transition"


def session_at(timestamp_ms: int) -> MarketSession:
    """Derive the UTC session solely from the broker snapshot timestamp."""

    _require_integer(timestamp_ms, "timestamp_ms")
    minute = (timestamp_ms // 60_000) % (24 * 60)
    if minute < 7 * 60:
        return MarketSession.ASIA
    if minute < 12 * 60:
        return MarketSession.LONDON
    if 13 * 60 + 30 <= minute < 20 * 60:
        return MarketSession.NEW_YORK
    return MarketSession.OFF


@dataclass(frozen=True, slots=True)
class Bar:
    """One broker-derived OHLCV bar with a UTC open timestamp."""

    open_time_ms: int
    close_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    def __post_init__(self) -> None:
        _require_integer(self.open_time_ms, "bar.open_time_ms")
        _require_integer(self.close_time_ms, "bar.close_time_ms")
        if self.close_time_ms <= self.open_time_ms:
            raise ValueError("bar close time must follow its open time")
        for name in ("open", "high", "low", "close", "volume"):
            object.__setattr__(self, name, _decimal(getattr(self, name), f"bar.{name}"))
        if min(self.open, self.high, self.low, self.close) <= 0:
            raise ValueError("bar prices must be positive")
        if self.volume < 0:
            raise ValueError("bar volume cannot be negative")
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise ValueError("bar OHLC values are inconsistent")
        if self.low > self.high:
            raise ValueError("bar low cannot exceed high")


@dataclass(frozen=True, slots=True)
class SentimentObservation:
    """Optional genuine directional sentiment used only by Agent 10."""

    source: str
    source_kind: EvidenceSource
    observed_at_ms: int
    sequence: int
    score: Decimal

    def __post_init__(self) -> None:
        _require_text(self.source, "sentiment.source")
        object.__setattr__(self, "source_kind", EvidenceSource(self.source_kind))
        object.__setattr__(self, "score", _decimal(self.score, "sentiment.score"))
        _require_integer(self.observed_at_ms, "sentiment.observed_at_ms")
        _require_integer(self.sequence, "sentiment.sequence")
        if not -ONE <= self.score <= ONE:
            raise ValueError("sentiment score must be between -1 and 1")


@dataclass(frozen=True, slots=True)
class MacroContext:
    """Freshness and calendar-coverage proof supplied by Agent 14."""

    source: str
    source_kind: EvidenceSource
    status: ProviderHealth
    source_at_ms: int | None
    received_at_ms: int
    sequence: int
    freshness_ms: int
    coverage_start_ms: int | None
    coverage_end_ms: int | None
    minimum_forward_coverage_ms: int = 0
    freeze_active: bool = True
    sentiment: SentimentObservation | None = None

    def __post_init__(self) -> None:
        _require_text(self.source, "macro.source")
        object.__setattr__(self, "source_kind", EvidenceSource(self.source_kind))
        object.__setattr__(self, "status", ProviderHealth(self.status))
        _require_integer(self.received_at_ms, "macro.received_at_ms")
        _require_integer(self.sequence, "macro.sequence", minimum=0)
        _require_integer(self.freshness_ms, "macro.freshness_ms")
        _require_integer(
            self.minimum_forward_coverage_ms,
            "macro.minimum_forward_coverage_ms",
            minimum=0,
        )
        if self.source_at_ms is not None:
            _require_integer(self.source_at_ms, "macro.source_at_ms")
        if (self.coverage_start_ms is None) != (self.coverage_end_ms is None):
            raise ValueError("macro coverage start and end must be supplied together")
        if self.coverage_start_ms is not None and self.coverage_end_ms is not None:
            _require_integer(self.coverage_start_ms, "macro.coverage_start_ms")
            _require_integer(self.coverage_end_ms, "macro.coverage_end_ms")
        if (
            self.coverage_start_ms is not None
            and self.coverage_end_ms is not None
            and self.coverage_end_ms <= self.coverage_start_ms
        ):
            raise ValueError("macro coverage end must follow its start")

    def healthy_covered_at(self, at_ms: int, mode: DataMode) -> bool:
        if self.status is not ProviderHealth.HEALTHY or self.sequence <= 0:
            return False
        if mode.value in BROKER_MODES and self.source_kind not in {
            EvidenceSource.BROKER,
            EvidenceSource.HTTP,
        }:
            return False
        if self.source_at_ms is None or self.coverage_start_ms is None or self.coverage_end_ms is None:
            return False
        if not self.source_at_ms <= self.received_at_ms <= at_ms:
            return False
        if at_ms - self.source_at_ms > self.freshness_ms:
            return False
        if not self.coverage_start_ms <= at_ms <= self.coverage_end_ms:
            return False
        return self.coverage_end_ms - at_ms >= self.minimum_forward_coverage_ms

    def sentiment_healthy_at(self, at_ms: int, mode: DataMode) -> bool:
        if not self.healthy_covered_at(at_ms, mode) or self.sentiment is None:
            return False
        if mode.value in BROKER_MODES and self.sentiment.source_kind not in {
            EvidenceSource.BROKER,
            EvidenceSource.HTTP,
        }:
            return False
        age = at_ms - self.sentiment.observed_at_ms
        return 0 <= age <= self.freshness_ms and self.sentiment.sequence > 0


@dataclass(frozen=True, slots=True)
class IntermarketObservation:
    observed_at_ms: int
    dxy: Decimal
    us10y: Decimal
    us30: Decimal
    gold: Decimal

    def __post_init__(self) -> None:
        _require_integer(self.observed_at_ms, "intermarket.observed_at_ms")
        for name in ("dxy", "us10y", "us30", "gold"):
            object.__setattr__(self, name, _decimal(getattr(self, name), name))
            if getattr(self, name) <= 0:
                raise ValueError("intermarket prices must be positive")


@dataclass(frozen=True, slots=True)
class IntermarketContext:
    """Two genuine, ordered observations required for Agent 12 deflection."""

    source: str
    source_kind: EvidenceSource
    status: ProviderHealth
    received_at_ms: int
    sequence: int
    freshness_ms: int
    previous: IntermarketObservation
    current: IntermarketObservation

    def __post_init__(self) -> None:
        _require_text(self.source, "intermarket.source")
        object.__setattr__(self, "source_kind", EvidenceSource(self.source_kind))
        object.__setattr__(self, "status", ProviderHealth(self.status))
        _require_integer(self.received_at_ms, "intermarket.received_at_ms")
        _require_integer(self.sequence, "intermarket.sequence")
        _require_integer(self.freshness_ms, "intermarket.freshness_ms")
        if self.previous.observed_at_ms >= self.current.observed_at_ms:
            raise ValueError("intermarket observations must be strictly ordered")
        if self.current.observed_at_ms > self.received_at_ms:
            raise ValueError("intermarket source time cannot follow receive time")

    def healthy_fresh_at(self, at_ms: int, mode: DataMode) -> bool:
        if self.status is not ProviderHealth.HEALTHY:
            return False
        if mode.value in BROKER_MODES and self.source_kind not in {
            EvidenceSource.BROKER,
            EvidenceSource.HTTP,
        }:
            return False
        if not self.current.observed_at_ms <= self.received_at_ms <= at_ms:
            return False
        observation_gap = self.current.observed_at_ms - self.previous.observed_at_ms
        return (
            observation_gap <= self.freshness_ms
            and at_ms - self.current.observed_at_ms <= self.freshness_ms
        )


def _seconds_to_ms(value: object, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} cannot be boolean")
    result = float(value)
    if not isfinite(result) or result <= 0:
        raise ValueError(f"{field_name} must be a finite positive timestamp")
    return int(round(result * 1000))


def macro_context_from_provider_snapshot(
    provider_snapshot: object,
    *,
    source_kind: EvidenceSource,
    at_ms: int,
    pre_event_freeze_ms: int = 15 * 60_000,
    post_event_freeze_ms: int = 5 * 60_000,
) -> MacroContext:
    """Pure adapter for the normalized ``ProviderSnapshot`` calendar shape.

    A calendar feed is not a sentiment feed.  Consequently this adapter never
    creates a directional sentiment observation and Agent 10 remains silent.
    """

    if str(getattr(provider_snapshot, "provider", "")).lower() != "macro":
        raise ValueError("provider snapshot is not macro data")
    payload = getattr(provider_snapshot, "payload", None)
    if not isinstance(payload, Mapping):
        raise ValueError("macro provider payload must be a mapping")
    source_timestamp = getattr(provider_snapshot, "source_timestamp", None)
    received_timestamp = getattr(provider_snapshot, "received_timestamp", None)
    freshness_s = getattr(provider_snapshot, "freshness_s", None)
    coverage_start = payload.get("coverage_start_utc")
    coverage_end = payload.get("coverage_end_utc")
    events = payload.get("events", ())
    if not isinstance(events, (list, tuple)):
        raise ValueError("macro provider events must be a sequence")
    freeze = False
    for event in events:
        if not isinstance(event, Mapping):
            raise ValueError("macro event must be a mapping")
        if str(event.get("impact", "")).upper() != "HIGH":
            continue
        if str(event.get("currency", "USD")).upper() != "USD":
            continue
        scheduled_ms = _seconds_to_ms(event.get("scheduled_utc"), "scheduled_utc")
        if at_ms - post_event_freeze_ms <= scheduled_ms <= at_ms + pre_event_freeze_ms:
            freeze = True
    minimum_forward_s = payload.get("minimum_forward_coverage_s", 0)
    minimum_forward_ms = int(round(float(minimum_forward_s) * 1000))
    if minimum_forward_ms < 0:
        raise ValueError("minimum forward coverage cannot be negative")
    return MacroContext(
        source=str(getattr(provider_snapshot, "source", "unknown")),
        source_kind=source_kind,
        status=ProviderHealth(_status_value(getattr(provider_snapshot, "status", "ERROR"))),
        source_at_ms=(
            None
            if source_timestamp is None
            else _seconds_to_ms(source_timestamp, "source_timestamp")
        ),
        received_at_ms=_seconds_to_ms(received_timestamp, "received_timestamp"),
        sequence=int(getattr(provider_snapshot, "sequence", 0)),
        freshness_ms=max(1, int(round(float(freshness_s) * 1000))),
        coverage_start_ms=(
            None
            if coverage_start is None
            else _seconds_to_ms(coverage_start, "coverage_start_utc")
        ),
        coverage_end_ms=(
            None
            if coverage_end is None
            else _seconds_to_ms(coverage_end, "coverage_end_utc")
        ),
        minimum_forward_coverage_ms=minimum_forward_ms,
        freeze_active=freeze,
        sentiment=None,
    )


def intermarket_context_from_provider_snapshots(
    previous_snapshot: object,
    current_snapshot: object,
    *,
    source_kind: EvidenceSource,
) -> IntermarketContext:
    """Build ordered context from two normalized provider snapshots."""

    def observation(item: object) -> IntermarketObservation:
        if str(getattr(item, "provider", "")).lower() != "intermarket":
            raise ValueError("provider snapshot is not intermarket data")
        payload = getattr(item, "payload", None)
        if not isinstance(payload, Mapping):
            raise ValueError("intermarket payload must be a mapping")
        return IntermarketObservation(
            observed_at_ms=_seconds_to_ms(
                getattr(item, "source_timestamp", None), "source_timestamp"
            ),
            dxy=payload.get("dxy"),
            us10y=payload.get("us10y"),
            us30=payload.get("us30"),
            gold=payload.get("gold"),
        )

    previous = observation(previous_snapshot)
    current = observation(current_snapshot)
    previous_sequence = int(getattr(previous_snapshot, "sequence", 0))
    current_sequence = int(getattr(current_snapshot, "sequence", 0))
    if previous_sequence <= 0 or current_sequence <= previous_sequence:
        raise ValueError("intermarket provider sequences must increase")
    if _status_value(getattr(previous_snapshot, "status", "ERROR")) != "HEALTHY":
        raise ValueError("previous intermarket evidence was not healthy")
    return IntermarketContext(
        source=str(getattr(current_snapshot, "source", "unknown")),
        source_kind=source_kind,
        status=ProviderHealth(_status_value(getattr(current_snapshot, "status", "ERROR"))),
        received_at_ms=_seconds_to_ms(
            getattr(current_snapshot, "received_timestamp", None), "received_timestamp"
        ),
        sequence=current_sequence,
        freshness_ms=max(
            1,
            int(round(float(getattr(current_snapshot, "freshness_s", 0)) * 1000)),
        ),
        previous=previous,
        current=current,
    )


def _freeze_bars(
    bars: Sequence[Bar], timeframe: str, captured_at_ms: int
) -> tuple[Bar, ...]:
    result = tuple(bars)
    if len(result) > 5_000:
        raise ValueError(f"{timeframe} contains too many bars")
    previous_time = 0
    previous_close = 0
    for bar in result:
        if not isinstance(bar, Bar):
            raise TypeError(f"{timeframe} must contain Bar objects")
        if bar.open_time_ms <= previous_time:
            raise ValueError(f"{timeframe} bar timestamps must be strictly increasing")
        if bar.open_time_ms < previous_close:
            raise ValueError(f"{timeframe} contains overlapping bars")
        if bar.close_time_ms > captured_at_ms:
            raise ValueError(f"{timeframe} contains an incomplete or future bar")
        previous_time = bar.open_time_ms
        previous_close = bar.close_time_ms
    return result


@dataclass(frozen=True, slots=True)
class CanonicalMarketSnapshot:
    """The single immutable input shared by all ten strategy voters."""

    snapshot_id: str
    symbol: str
    captured_at_ms: int
    mode: DataMode
    market_source: MarketSource
    bid: Decimal
    ask: Decimal
    tick_size: Decimal
    h1: tuple[Bar, ...] = ()
    h4: tuple[Bar, ...] = ()
    d1: tuple[Bar, ...] = ()
    m15: tuple[Bar, ...] = ()
    m5: tuple[Bar, ...] = ()
    m1: tuple[Bar, ...] = ()
    macro: MacroContext | None = None
    intermarket: IntermarketContext | None = None

    def __post_init__(self) -> None:
        _require_text(self.snapshot_id, "snapshot_id", max_length=128)
        _require_text(self.symbol, "symbol", max_length=32)
        _require_integer(self.captured_at_ms, "snapshot.captured_at_ms")
        object.__setattr__(self, "mode", DataMode(self.mode))
        object.__setattr__(self, "market_source", MarketSource(self.market_source))
        for name in ("bid", "ask", "tick_size"):
            object.__setattr__(self, name, _decimal(getattr(self, name), name))
        if self.bid <= 0 or self.ask < self.bid or self.tick_size <= 0:
            raise ValueError("snapshot must satisfy 0 < bid <= ask and tick_size > 0")
        if self.bid % self.tick_size or self.ask % self.tick_size:
            raise ValueError("snapshot quotes must be aligned to broker tick_size")
        if self.mode.value in BROKER_MODES and self.market_source is not MarketSource.BROKER:
            raise ValueError("shadow/demo/live snapshots must be broker-derived")
        if self.mode is DataMode.REPLAY and self.market_source is not MarketSource.REPLAY:
            raise ValueError("replay mode requires replay provenance")
        for timeframe in ("h1", "h4", "d1", "m15", "m5", "m1"):
            object.__setattr__(
                self,
                timeframe,
                _freeze_bars(getattr(self, timeframe), timeframe, self.captured_at_ms),
            )

    @property
    def session(self) -> MarketSession:
        return session_at(self.captured_at_ms)


@dataclass(frozen=True, slots=True)
class RegimeAssessment:
    snapshot_id: str
    regime: CanonicalRegime
    confidence: Decimal
    observed_at_ms: int
    atr_ratio: Decimal
    trend_strength: Decimal
    compression_ratio: Decimal
    sweep_detected: bool
    news_active: bool

    def __post_init__(self) -> None:
        for name in ("confidence", "atr_ratio", "trend_strength", "compression_ratio"):
            object.__setattr__(self, name, _decimal(getattr(self, name), name))
        if not ZERO <= self.confidence <= ONE:
            raise ValueError("regime confidence must be between zero and one")


@dataclass(frozen=True, slots=True)
class RegimeDecision:
    available: bool
    reason: str
    assessment: RegimeAssessment | None = None

    def __post_init__(self) -> None:
        if self.available != (self.assessment is not None):
            raise ValueError("regime availability and assessment must agree")


def _true_ranges(bars: Sequence[Bar]) -> tuple[Decimal, ...]:
    if len(bars) < 2:
        return ()
    return tuple(
        max(
            bar.high - bar.low,
            abs(bar.high - bars[index - 1].close),
            abs(bar.low - bars[index - 1].close),
        )
        for index, bar in enumerate(bars[1:], start=1)
    )


def _mean(values: Sequence[Decimal]) -> Decimal:
    return sum(values, ZERO) / Decimal(len(values)) if values else ZERO


def _atr(bars: Sequence[Bar], period: int = 14) -> Decimal:
    ranges = _true_ranges(bars)
    return _mean(ranges[-period:])


def _trend(bars: Sequence[Bar], period: int) -> tuple[int, Decimal]:
    closes = tuple(bar.close for bar in bars[-period:])
    if len(closes) < period:
        return 0, ZERO
    net = closes[-1] - closes[0]
    path = sum((abs(closes[i] - closes[i - 1]) for i in range(1, len(closes))), ZERO)
    if path <= 0:
        return 0, ZERO
    direction = 1 if net > 0 else -1 if net < 0 else 0
    return direction, min(ONE, abs(net) / path)


def _sweep(bars: Sequence[Bar], lookback: int = 10) -> tuple[bool, Direction | None]:
    if len(bars) < lookback + 1:
        return False, None
    previous = bars[-lookback - 1 : -1]
    current = bars[-1]
    prior_high = max(bar.high for bar in previous)
    prior_low = min(bar.low for bar in previous)
    swept_high = current.high > prior_high and current.close < prior_high
    swept_low = current.low < prior_low and current.close > prior_low
    if swept_high == swept_low:
        return False, None
    return True, Direction.SELL if swept_high else Direction.BUY


class CanonicalRegimeDetector:
    """Side-effect-free eight-regime classifier with no fabricated fallback."""

    minimum_h1_bars = 55
    minimum_h4_bars = 20

    def detect(self, snapshot: CanonicalMarketSnapshot) -> RegimeDecision:
        if len(snapshot.h1) < self.minimum_h1_bars:
            return RegimeDecision(False, "INSUFFICIENT_H1_BARS")
        if len(snapshot.h4) < self.minimum_h4_bars:
            return RegimeDecision(False, "INSUFFICIENT_H4_BARS")
        ranges = _true_ranges(snapshot.h1)
        recent_atr = _mean(ranges[-14:])
        baseline_atr = _mean(ranges[-50:])
        if recent_atr <= 0 or baseline_atr <= 0:
            return RegimeDecision(False, "INVALID_ATR_EVIDENCE")
        atr_ratio = recent_atr / baseline_atr
        h1_direction, trend_strength = _trend(snapshot.h1, 20)
        h4_direction, _ = _trend(snapshot.h4, 20)
        range_10 = max(bar.high for bar in snapshot.h1[-10:]) - min(
            bar.low for bar in snapshot.h1[-10:]
        )
        range_50 = max(bar.high for bar in snapshot.h1[-50:]) - min(
            bar.low for bar in snapshot.h1[-50:]
        )
        if range_50 <= 0:
            return RegimeDecision(False, "INVALID_RANGE_EVIDENCE")
        compression = range_10 / range_50
        swept, _ = _sweep(snapshot.h1)
        macro_healthy = (
            snapshot.macro is not None
            and snapshot.macro.healthy_covered_at(snapshot.captured_at_ms, snapshot.mode)
        )
        news_active = bool(macro_healthy and snapshot.macro and snapshot.macro.freeze_active)

        if news_active:
            regime = CanonicalRegime.HIGH_IMPACT_NEWS
            confidence = ONE
        elif swept and trend_strength >= Decimal("0.30"):
            regime = CanonicalRegime.INSTITUTIONAL_HUNT
            confidence = min(Decimal("0.97"), Decimal("0.80") + trend_strength / 10)
        elif trend_strength >= Decimal("0.65") and h1_direction > 0:
            regime = CanonicalRegime.TRENDING_BULL
            confidence = min(Decimal("0.97"), Decimal("0.65") + trend_strength * Decimal("0.30"))
        elif trend_strength >= Decimal("0.65") and h1_direction < 0:
            regime = CanonicalRegime.TRENDING_BEAR
            confidence = min(Decimal("0.97"), Decimal("0.65") + trend_strength * Decimal("0.30"))
        elif compression <= Decimal("0.35") and atr_ratio <= Decimal("0.80"):
            regime = CanonicalRegime.BREAKOUT_IMMINENT
            confidence = Decimal("0.75")
        elif h1_direction and h4_direction and h1_direction != h4_direction:
            regime = CanonicalRegime.REGIME_TRANSITION
            confidence = Decimal("0.65")
        elif atr_ratio >= Decimal("1.45") and trend_strength < Decimal("0.35"):
            regime = CanonicalRegime.RANGING_WIDE
            confidence = Decimal("0.65")
        else:
            regime = CanonicalRegime.RANGING_TIGHT
            confidence = Decimal("0.60")

        return RegimeDecision(
            True,
            "CLASSIFIED",
            RegimeAssessment(
                snapshot_id=snapshot.snapshot_id,
                regime=regime,
                confidence=confidence,
                observed_at_ms=snapshot.captured_at_ms,
                atr_ratio=atr_ratio,
                trend_strength=trend_strength,
                compression_ratio=compression,
                sweep_detected=swept,
                news_active=news_active,
            ),
        )


@dataclass(frozen=True, slots=True)
class StrategyCandidate:
    direction: Direction
    conviction: Decimal
    risk_distance: Decimal
    tags: tuple[str, ...]
    htf_aligned: bool = True
    volume_confirmed: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "direction", Direction(self.direction))
        object.__setattr__(self, "conviction", _decimal(self.conviction, "conviction"))
        object.__setattr__(self, "risk_distance", _decimal(self.risk_distance, "risk_distance"))
        object.__setattr__(self, "tags", tuple(str(tag).strip() for tag in self.tags))


@dataclass(frozen=True, slots=True)
class AgentEvaluation:
    agent_id: int
    reason: str
    signal: SignalContribution | None = None

    def __post_init__(self) -> None:
        if self.agent_id not in CANONICAL_STRATEGY_IDS:
            raise ValueError("agent evaluation must belong to Agent 04 through 13")
        if self.signal is not None and self.signal.agent_id != self.agent_id:
            raise ValueError("agent evaluation signal identity mismatch")


@dataclass(frozen=True, slots=True)
class StrategyPipelineResult:
    snapshot_id: str
    observed_at_ms: int
    session: MarketSession
    regime: RegimeDecision
    evaluations: tuple[AgentEvaluation, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "evaluations", tuple(self.evaluations))
        if tuple(item.agent_id for item in self.evaluations) != CANONICAL_STRATEGY_IDS:
            raise ValueError("pipeline result must represent Agents 04 through 13 exactly once")
        signals = tuple(item.signal for item in self.evaluations if item.signal is not None)
        ids = tuple(signal.signal_id for signal in signals)
        if len(ids) != len(set(ids)):
            raise ValueError("pipeline signal IDs must be unique")
        if any(
            signal.snapshot_id != self.snapshot_id
            or signal.observed_at_ms != self.observed_at_ms
            for signal in signals
        ):
            raise ValueError("all signals must refer to the evaluated snapshot")

    @property
    def signals(self) -> tuple[SignalContribution, ...]:
        return tuple(item.signal for item in self.evaluations if item.signal is not None)

    @property
    def disabled_agents(self) -> tuple[int, ...]:
        return tuple(
            item.agent_id
            for item in self.evaluations
            if item.reason.startswith("PROVIDER_")
        )


Analyzer = Callable[[CanonicalMarketSnapshot, RegimeAssessment], StrategyCandidate | None]


def _valid_volume(bars: Sequence[Bar], minimum: int = 5) -> bool:
    return len(bars) >= minimum and all(bar.volume > 0 for bar in bars[-minimum:])


def _htf_direction(snapshot: CanonicalMarketSnapshot) -> int:
    direction, strength = _trend(snapshot.d1, 10)
    return direction if strength >= Decimal("0.55") else 0


def _candidate(
    direction: Direction,
    conviction: str,
    risk_distance: Decimal,
    *tags: str,
    htf_aligned: bool = True,
    volume_confirmed: bool = True,
) -> StrategyCandidate:
    return StrategyCandidate(
        direction=direction,
        conviction=Decimal(conviction),
        risk_distance=risk_distance,
        tags=tuple(tags),
        htf_aligned=htf_aligned,
        volume_confirmed=volume_confirmed,
    )


def _agent04(snapshot: CanonicalMarketSnapshot, regime: RegimeAssessment) -> StrategyCandidate | None:
    del regime
    if len(snapshot.h4) < 11 or len(snapshot.d1) < 20 or not _valid_volume(snapshot.h4):
        return None
    swept, direction = _sweep(snapshot.h4)
    if not swept or direction is None:
        return None
    atr = _atr(snapshot.h1)
    return _candidate(
        direction,
        "76",
        atr * Decimal("0.60"),
        "HTF_LIQUIDITY_TRAP",
        "H4_SWEEP_HIGH" if direction is Direction.SELL else "H4_SWEEP_LOW",
        htf_aligned=True,
    )


def _linear_projection(values: Sequence[Decimal]) -> tuple[Decimal, Decimal]:
    count = len(values)
    if count < 3:
        return ZERO, ZERO
    n = Decimal(count)
    mean_x = Decimal(count - 1) / 2
    mean_y = _mean(values)
    numerator = sum(
        ((Decimal(index) - mean_x) * (value - mean_y) for index, value in enumerate(values)),
        ZERO,
    )
    denominator = sum(((Decimal(index) - mean_x) ** 2 for index in range(count)), ZERO)
    slope = numerator / denominator if denominator else ZERO
    projected = mean_y + slope * (Decimal(count) - mean_x)
    return slope, projected


def _agent05(snapshot: CanonicalMarketSnapshot, regime: RegimeAssessment) -> StrategyCandidate | None:
    del regime
    if len(snapshot.h1) < 31 or len(snapshot.d1) < 10 or not _valid_volume(snapshot.h1):
        return None
    previous = snapshot.h1[-21:-1]
    last = snapshot.h1[-1]
    high_slope, upper = _linear_projection(tuple(bar.high for bar in previous))
    low_slope, lower = _linear_projection(tuple(bar.low for bar in previous))
    atr = _atr(snapshot.h1)
    htf = _htf_direction(snapshot)
    if high_slope < 0 and last.close > upper and last.open <= upper:
        return _candidate(
            Direction.BUY,
            "72",
            atr * Decimal("0.70"),
            "TRENDLINE_BREAKOUT",
            "DESCENDING_RESISTANCE_BREAK",
            htf_aligned=htf in (0, 1),
        )
    if low_slope > 0 and last.close < lower and last.open >= lower:
        return _candidate(
            Direction.SELL,
            "72",
            atr * Decimal("0.70"),
            "TRENDLINE_BREAKOUT",
            "ASCENDING_SUPPORT_BREAK",
            htf_aligned=htf in (0, -1),
        )
    return None


def _agent06(snapshot: CanonicalMarketSnapshot, regime: RegimeAssessment) -> StrategyCandidate | None:
    del regime
    if len(snapshot.h1) < 16 or not _valid_volume(snapshot.h1):
        return None
    reference = snapshot.h1[-2]
    current = snapshot.h1[-1]
    atr = _atr(snapshot.h1)
    if current.low < reference.low and current.close > reference.low:
        return _candidate(Direction.BUY, "78", atr * Decimal("0.55"), "CRT_ENGINE", "RANGE_LOW_SWEEP")
    if current.high > reference.high and current.close < reference.high:
        return _candidate(Direction.SELL, "78", atr * Decimal("0.55"), "CRT_ENGINE", "RANGE_HIGH_SWEEP")
    if current.open <= reference.high < current.close:
        return _candidate(Direction.BUY, "68", atr * Decimal("0.65"), "CRT_ENGINE", "RANGE_EXPANSION_UP")
    if current.open >= reference.low > current.close:
        return _candidate(Direction.SELL, "68", atr * Decimal("0.65"), "CRT_ENGINE", "RANGE_EXPANSION_DOWN")
    return None


def _agent07(snapshot: CanonicalMarketSnapshot, regime: RegimeAssessment) -> StrategyCandidate | None:
    del regime
    bars = snapshot.m15
    if len(bars) < 10 or len(snapshot.d1) < 10 or not _valid_volume(bars):
        return None
    first, _, third = bars[-3:]
    atr = _atr(snapshot.h1)
    htf = _htf_direction(snapshot)
    minimum_gap = max(snapshot.tick_size * 3, atr * Decimal("0.03"))
    if third.low - first.high >= minimum_gap:
        return _candidate(
            Direction.BUY,
            "82",
            atr * Decimal("0.60"),
            "ICT_SMC_CORE",
            "BULLISH_FAIR_VALUE_GAP",
            htf_aligned=htf in (0, 1),
        )
    if first.low - third.high >= minimum_gap:
        return _candidate(
            Direction.SELL,
            "82",
            atr * Decimal("0.60"),
            "ICT_SMC_CORE",
            "BEARISH_FAIR_VALUE_GAP",
            htf_aligned=htf in (0, -1),
        )
    return None


def _volume_profile_poc(bars: Sequence[Bar], bucket_size: Decimal) -> Decimal | None:
    if bucket_size <= 0 or not _valid_volume(bars, min(20, len(bars))):
        return None
    buckets: dict[Decimal, Decimal] = {}
    for bar in bars:
        typical = (bar.high + bar.low + bar.close) / 3
        bucket = (typical / bucket_size).to_integral_value(rounding=ROUND_FLOOR) * bucket_size
        buckets[bucket] = buckets.get(bucket, ZERO) + bar.volume
    if not buckets:
        return None
    max_volume = max(buckets.values())
    return min(price for price, volume in buckets.items() if volume == max_volume)


def _agent08(snapshot: CanonicalMarketSnapshot, regime: RegimeAssessment) -> StrategyCandidate | None:
    del regime
    if len(snapshot.h1) < 30 or not _valid_volume(snapshot.h1, 20):
        return None
    atr = _atr(snapshot.h1)
    bucket = max(snapshot.tick_size, atr / 20)
    poc = _volume_profile_poc(snapshot.h1[-50:], bucket)
    if poc is None or abs(snapshot.bid - poc) > atr * Decimal("0.35"):
        return None
    direction, strength = _trend(snapshot.h1, 5)
    if not direction or strength < Decimal("0.55"):
        return None
    side = Direction.BUY if direction > 0 else Direction.SELL
    return _candidate(side, "70", atr * Decimal("0.65"), "VPVR_POC", "BROKER_VOLUME_NODE")


def _asia_range(snapshot: CanonicalMarketSnapshot) -> tuple[Decimal, Decimal] | None:
    day_start = snapshot.captured_at_ms - snapshot.captured_at_ms % 86_400_000
    bars = tuple(
        bar
        for bar in snapshot.h1
        if day_start <= bar.open_time_ms < day_start + 7 * 3_600_000
    )
    if len(bars) < 4:
        return None
    return min(bar.low for bar in bars), max(bar.high for bar in bars)


def _agent09(snapshot: CanonicalMarketSnapshot, regime: RegimeAssessment) -> StrategyCandidate | None:
    del regime
    if snapshot.session is not MarketSession.LONDON or len(snapshot.m5) < 2:
        return None
    bounds = _asia_range(snapshot)
    if bounds is None or not _valid_volume(snapshot.m5):
        return None
    low, high = bounds
    current = snapshot.m5[-1]
    atr = _atr(snapshot.h1)
    if current.high > high and current.close < high:
        return _candidate(Direction.SELL, "80", atr * Decimal("0.50"), "ASIA_SESSION_SWEEP", "ASIA_HIGH_REJECTION")
    if current.low < low and current.close > low:
        return _candidate(Direction.BUY, "80", atr * Decimal("0.50"), "ASIA_SESSION_SWEEP", "ASIA_LOW_REJECTION")
    return None


def _agent10(snapshot: CanonicalMarketSnapshot, regime: RegimeAssessment) -> StrategyCandidate | None:
    del regime
    context = snapshot.macro
    if (
        context is None
        or context.freeze_active
        or not context.sentiment_healthy_at(snapshot.captured_at_ms, snapshot.mode)
        or context.sentiment is None
    ):
        return None
    score = context.sentiment.score
    if abs(score) < Decimal("0.35"):
        return None
    direction = Direction.BUY if score > 0 else Direction.SELL
    conviction = min(Decimal("85"), Decimal("55") + abs(score) * Decimal("30"))
    return StrategyCandidate(
        direction=direction,
        conviction=conviction,
        risk_distance=_atr(snapshot.h1) * Decimal("0.70"),
        tags=("NEWS_SENTIMENT", "VERIFIED_MACRO_CONTEXT"),
    )


def _standard_deviation(values: Sequence[Decimal]) -> Decimal:
    if not values:
        return ZERO
    mean = _mean(values)
    variance = _mean(tuple((value - mean) ** 2 for value in values))
    return variance.sqrt() if variance >= 0 else ZERO


def _agent11(snapshot: CanonicalMarketSnapshot, regime: RegimeAssessment) -> StrategyCandidate | None:
    del regime
    if len(snapshot.h1) < 25 or not _valid_volume(snapshot.h1):
        return None
    closes = tuple(bar.close for bar in snapshot.h1[-20:])
    mean = _mean(closes)
    deviation = _standard_deviation(closes)
    if deviation <= 0:
        return None
    atr = _atr(snapshot.h1)
    if snapshot.bid >= mean + deviation * 2:
        return _candidate(Direction.SELL, "68", atr * Decimal("0.65"), "MEAN_REVERSION_VOL_BANDS", "UPPER_2SIGMA")
    if snapshot.ask <= mean - deviation * 2:
        return _candidate(Direction.BUY, "68", atr * Decimal("0.65"), "MEAN_REVERSION_VOL_BANDS", "LOWER_2SIGMA")
    return None


def _return(current: Decimal, previous: Decimal) -> Decimal:
    return (current - previous) / previous


def _agent12(snapshot: CanonicalMarketSnapshot, regime: RegimeAssessment) -> StrategyCandidate | None:
    del regime
    context = snapshot.intermarket
    if context is None:
        return None
    previous = context.previous
    current = context.current
    if abs(current.gold - snapshot.bid) / snapshot.bid > Decimal("0.01"):
        return None
    dxy_return = _return(current.dxy, previous.dxy)
    yield_return = _return(current.us10y, previous.us10y)
    gold_return = _return(current.gold, previous.gold)
    threshold = Decimal("0.0005")
    atr = _atr(snapshot.h1)
    if dxy_return >= threshold and yield_return >= ZERO and gold_return >= -Decimal("0.0001"):
        return _candidate(Direction.BUY, "76", atr * Decimal("0.70"), "CORRELATION_DEFLECTION", "DXY_GOLD_BULL_DEFLECTION")
    if dxy_return <= -threshold and yield_return <= ZERO and gold_return <= Decimal("0.0001"):
        return _candidate(Direction.SELL, "76", atr * Decimal("0.70"), "CORRELATION_DEFLECTION", "DXY_GOLD_BEAR_DEFLECTION")
    return None


def _agent13(snapshot: CanonicalMarketSnapshot, regime: RegimeAssessment) -> StrategyCandidate | None:
    del regime
    if len(snapshot.m5) < 21 or not _valid_volume(snapshot.m5, 11):
        return None
    previous = snapshot.m5[-21:-1]
    current = snapshot.m5[-1]
    prior_low = min(bar.low for bar in previous)
    prior_high = max(bar.high for bar in previous)
    average_volume = _mean(tuple(bar.volume for bar in previous[-10:]))
    if average_volume <= 0 or current.volume < average_volume * Decimal("1.5"):
        return None
    atr = _atr(snapshot.h1)
    if current.low < prior_low and current.close > prior_low:
        return _candidate(Direction.BUY, "80", atr * Decimal("0.55"), "WYCKOFF_SCHEMATIC", "SPRING", "VOLUME_SURGE")
    if current.high > prior_high and current.close < prior_high:
        return _candidate(Direction.SELL, "80", atr * Decimal("0.55"), "WYCKOFF_SCHEMATIC", "UPTHRUST", "VOLUME_SURGE")
    return None


DEFAULT_ANALYZERS: Mapping[int, Analyzer] = MappingProxyType(
    {
        4: _agent04,
        5: _agent05,
        6: _agent06,
        7: _agent07,
        8: _agent08,
        9: _agent09,
        10: _agent10,
        11: _agent11,
        12: _agent12,
        13: _agent13,
    }
)


def _ceil_tick(value: Decimal, tick: Decimal) -> Decimal:
    return (value / tick).to_integral_value(rounding=ROUND_CEILING) * tick


def _floor_tick(value: Decimal, tick: Decimal) -> Decimal:
    return (value / tick).to_integral_value(rounding=ROUND_FLOOR) * tick


class CanonicalStrategyPipeline:
    """Evaluate Agents 04-13 once against one immutable snapshot."""

    def __init__(
        self,
        *,
        regime_detector: CanonicalRegimeDetector | None = None,
        analyzers: Mapping[int, Analyzer] | None = None,
        enabled_agent_ids: Sequence[int] | None = None,
    ) -> None:
        supplied = dict(DEFAULT_ANALYZERS if analyzers is None else analyzers)
        if tuple(sorted(supplied)) != CANONICAL_STRATEGY_IDS:
            raise ValueError("analyzers must represent Agents 04 through 13 exactly once")
        self._analyzers = MappingProxyType(supplied)
        self._regime_detector = regime_detector or CanonicalRegimeDetector()
        enabled = (
            frozenset(CANONICAL_STRATEGY_IDS)
            if enabled_agent_ids is None
            else frozenset(enabled_agent_ids)
        )
        if any(
            isinstance(agent_id, bool) or not isinstance(agent_id, int)
            for agent_id in enabled
        ):
            raise ValueError("enabled strategy agent IDs must be integers")
        unknown = enabled - frozenset(CANONICAL_STRATEGY_IDS)
        if unknown:
            raise ValueError(f"enabled strategy agents are outside 04-13: {sorted(unknown)}")
        self._enabled_agent_ids = enabled

    def evaluate(self, snapshot: CanonicalMarketSnapshot) -> StrategyPipelineResult:
        if not isinstance(snapshot, CanonicalMarketSnapshot):
            raise TypeError("strategy pipeline requires CanonicalMarketSnapshot")
        regime = self._regime_detector.detect(snapshot)
        if not regime.available:
            return self._result(
                snapshot,
                regime,
                tuple(AgentEvaluation(agent_id, "REGIME_UNAVAILABLE") for agent_id in CANONICAL_STRATEGY_IDS),
            )
        if regime.assessment is None:
            raise RuntimeError(
                "regime pipeline returned no assessment after validation"
            )
        if regime.assessment.regime is CanonicalRegime.HIGH_IMPACT_NEWS:
            return self._result(
                snapshot,
                regime,
                tuple(AgentEvaluation(agent_id, "REGIME_ENTRY_FREEZE") for agent_id in CANONICAL_STRATEGY_IDS),
            )

        macro_eligible = (
            snapshot.macro is not None
            and snapshot.macro.healthy_covered_at(snapshot.captured_at_ms, snapshot.mode)
        )
        intermarket_eligible = (
            snapshot.intermarket is not None
            and snapshot.intermarket.healthy_fresh_at(snapshot.captured_at_ms, snapshot.mode)
        )
        evaluations: list[AgentEvaluation] = []
        for agent_id in CANONICAL_STRATEGY_IDS:
            if agent_id not in self._enabled_agent_ids:
                evaluations.append(AgentEvaluation(agent_id, "CONFIG_DISABLED"))
                continue
            if agent_id == 10 and not macro_eligible:
                evaluations.append(AgentEvaluation(agent_id, "PROVIDER_MACRO_UNAVAILABLE"))
                continue
            if agent_id == 12 and not intermarket_eligible:
                evaluations.append(AgentEvaluation(agent_id, "PROVIDER_INTERMARKET_UNAVAILABLE"))
                continue
            try:
                candidate = self._analyzers[agent_id](snapshot, regime.assessment)
            except Exception:
                evaluations.append(AgentEvaluation(agent_id, "ANALYZER_REJECTED_INPUT"))
                continue
            if candidate is None:
                evaluations.append(AgentEvaluation(agent_id, "NO_SETUP"))
                continue
            evaluations.append(self._materialize(snapshot, regime.assessment, agent_id, candidate))
        return self._result(snapshot, regime, tuple(evaluations))

    @staticmethod
    def _result(
        snapshot: CanonicalMarketSnapshot,
        regime: RegimeDecision,
        evaluations: tuple[AgentEvaluation, ...],
    ) -> StrategyPipelineResult:
        return StrategyPipelineResult(
            snapshot_id=snapshot.snapshot_id,
            observed_at_ms=snapshot.captured_at_ms,
            session=snapshot.session,
            regime=regime,
            evaluations=evaluations,
        )

    @staticmethod
    def _materialize(
        snapshot: CanonicalMarketSnapshot,
        regime: RegimeAssessment,
        agent_id: int,
        candidate: StrategyCandidate,
    ) -> AgentEvaluation:
        if not ZERO < candidate.conviction <= HUNDRED:
            return AgentEvaluation(agent_id, "INVALID_CONVICTION")
        if candidate.risk_distance <= 0:
            return AgentEvaluation(agent_id, "INVALID_GEOMETRY")
        if not candidate.htf_aligned:
            return AgentEvaluation(agent_id, "HTF_MISALIGNED")
        if not candidate.volume_confirmed:
            return AgentEvaluation(agent_id, "VOLUME_UNCONFIRMED")
        atr = _atr(snapshot.h1)
        if atr <= 0:
            return AgentEvaluation(agent_id, "INSUFFICIENT_ATR")
        spread = snapshot.ask - snapshot.bid
        risk = _ceil_tick(
            max(candidate.risk_distance, atr * Decimal("0.40"), spread * 3, snapshot.tick_size * 5),
            snapshot.tick_size,
        )
        if candidate.direction is Direction.BUY:
            entry = snapshot.ask
            levels = EntryLevels(
                entry=entry,
                sl=_floor_tick(entry - risk, snapshot.tick_size),
                tp1=_ceil_tick(entry + risk, snapshot.tick_size),
                tp2=_ceil_tick(entry + risk * 2, snapshot.tick_size),
                tp3=_ceil_tick(entry + risk * 3, snapshot.tick_size),
            )
        else:
            entry = snapshot.bid
            levels = EntryLevels(
                entry=entry,
                sl=_ceil_tick(entry + risk, snapshot.tick_size),
                tp1=_floor_tick(entry - risk, snapshot.tick_size),
                tp2=_floor_tick(entry - risk * 2, snapshot.tick_size),
                tp3=_floor_tick(entry - risk * 3, snapshot.tick_size),
            )
        validation = EntryPolicy.validate_levels(candidate.direction, levels, entry)
        if not validation.valid:
            return AgentEvaluation(agent_id, f"INVALID_GEOMETRY:{validation.reason}")

        tags = (
            f"AGENT_{agent_id:02d}",
            AGENT_BY_ID[agent_id].slug.upper(),
            *candidate.tags,
            f"REGIME:{regime.regime.value}",
            f"SESSION:{snapshot.session.value}",
        )
        if any(not tag for tag in tags):
            return AgentEvaluation(agent_id, "INVALID_TAGS")
        digest_material = "|".join(
            (
                snapshot.snapshot_id,
                str(snapshot.captured_at_ms),
                str(agent_id),
                candidate.direction.value,
                str(candidate.conviction),
                str(levels.entry),
                str(levels.sl),
                str(levels.tp1),
                str(levels.tp2),
                str(levels.tp3),
                *tags,
            )
        )
        signal_id = "sig-" + hashlib.sha256(digest_material.encode("utf-8")).hexdigest()[:32]
        signal = SignalContribution(
            signal_id=signal_id,
            snapshot_id=snapshot.snapshot_id,
            agent_id=agent_id,
            direction=candidate.direction,
            conviction=candidate.conviction,
            levels=levels,
            observed_at_ms=snapshot.captured_at_ms,
            regime=regime.regime.value,
            session=snapshot.session.value,
            strategy_tags=tags,
            htf_aligned=True,
            session_valid=True,
            volume_confirmed=True,
            context_enabled=True,
        )
        return AgentEvaluation(agent_id, "SIGNAL", signal)


__all__ = [
    "AgentEvaluation",
    "Bar",
    "CANONICAL_STRATEGY_IDS",
    "CanonicalMarketSnapshot",
    "CanonicalRegime",
    "CanonicalRegimeDetector",
    "CanonicalStrategyPipeline",
    "DataMode",
    "EvidenceSource",
    "IntermarketContext",
    "IntermarketObservation",
    "MacroContext",
    "MarketSession",
    "MarketSource",
    "ProviderHealth",
    "RegimeAssessment",
    "RegimeDecision",
    "SentimentObservation",
    "StrategyCandidate",
    "StrategyPipelineResult",
    "intermarket_context_from_provider_snapshots",
    "macro_context_from_provider_snapshot",
    "session_at",
]
