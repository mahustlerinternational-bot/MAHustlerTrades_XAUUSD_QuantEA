"""Canonical authenticated loopback gateway for the QUANT-SWARM MT5 adapter.

The gateway is deliberately inert at import.  :meth:`BrokerGateway.start` is
the only operation that binds a socket.  The adapter implements both the
execution ``BrokerPort`` and reconciliation ``ReconciliationBrokerPort``
without letting wire parsing leak into either service.

Broker-observed mutable position facts and ledger-owned immutable trade
context are kept separate.  An attributed position is never converted into an
``OwnedPositionRecord`` unless a freshly-invoked durable context resolver
supplies the immutable risk/target and lifecycle fields.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import ipaddress
import json
from collections import deque
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Awaitable, Callable, Mapping, Protocol, runtime_checkable

from .broker_protocol import (
    BrokerSessionMode,
    DEFAULT_MAX_FRAME_BYTES,
    MessageType,
    ProtocolCodec,
    ProtocolError,
    ProtocolMessage,
)
from .domain_models import (
    Clock,
    CommandAction,
    Direction,
    LifecycleActionKind,
    LifecycleStage,
    Milestone,
    QuoteSnapshot,
    SymbolSnapshot,
    decimal_value,
)
from .execution_service import BrokerPort, OutboundCommand
from .reconciliation import (
    AuthoritativeBrokerSnapshot,
    BrokerDealPage,
    BrokerRiskSnapshot,
    ReconciliationBrokerPort,
)
from .runtime_state import (
    BrokerIdentity,
    BrokerTradeMode,
    ControlPlaneState,
    ExpectedBrokerIdentity,
    RuntimeStateMachine,
    identity_mismatches,
)
from .trade_ledger import BrokerDealRecord, OwnedPositionRecord, PendingOrderRecord
from .version import MQL5_EA_VERSION


_TIMEFRAME_MS: Mapping[str, int] = MappingProxyType(
    {
        "M1": 60_000,
        "M5": 300_000,
        "M15": 900_000,
        "H1": 3_600_000,
        "H4": 14_400_000,
        "D1": 86_400_000,
    }
)
_ACK_STATUSES = frozenset({"APPLIED", "PARTIALLY_APPLIED", "ACCEPTED"})
_CACHE_FAILURE_MARKERS = frozenset(
    {"CACHE_FAILURE", "COMMAND_CACHE_FAILURE", "IDEMPOTENCY_CACHE_FAILURE"}
)


class GatewayError(RuntimeError):
    """Base gateway failure."""


class GatewayDisconnectedError(GatewayError):
    """No authenticated, fresh EA session is available."""


class GatewayProtocolError(GatewayError):
    """The authenticated peer violated the v4 contract."""


class GatewaySnapshotError(GatewayError):
    """A snapshot/replay generation was incomplete or contradictory."""


class GatewayCacheError(GatewayError):
    """Durable context or bounded idempotency state was unavailable."""


class GatewayState(StrEnum):
    STOPPED = "STOPPED"
    LISTENING = "LISTENING"
    HANDSHAKING = "HANDSHAKING"
    AUTHENTICATED = "AUTHENTICATED"
    DISCONNECTED = "DISCONNECTED"
    CLOSED = "CLOSED"


class GatewaySignalType(StrEnum):
    CONNECTED = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"
    UNKNOWN_COMMAND = "UNKNOWN_COMMAND"
    COMMAND_ACK = "COMMAND_ACK"
    COMMAND_NACK = "COMMAND_NACK"
    BROKER_EVENT = "BROKER_EVENT"
    PROTOCOL_VIOLATION = "PROTOCOL_VIOLATION"
    CACHE_FAILURE = "CACHE_FAILURE"
    STALE_HEARTBEAT = "STALE_HEARTBEAT"
    SNAPSHOT_READY = "SNAPSHOT_READY"
    BAR_WITHHELD = "BAR_WITHHELD"


@dataclass(frozen=True, slots=True)
class GatewaySignal:
    signal_type: GatewaySignalType
    occurred_at_ms: int
    connection_epoch: str | None
    command_id: str | None = None
    trade_id: str | None = None
    reason: str = ""
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))


@dataclass(frozen=True, slots=True)
class BrokerObservedPosition:
    """Only facts present in one authenticated broker snapshot item."""

    broker_position_id: str
    position_ticket: str
    trade_id: str | None
    symbol: str
    magic: int
    direction: str
    remaining_volume: Decimal
    entry_price: Decimal
    current_sl: Decimal
    final_tp: Decimal
    position_version: int
    opened_at_ms: int
    comment: str

    def __post_init__(self) -> None:
        for name in (
            "remaining_volume",
            "entry_price",
            "current_sl",
            "final_tp",
        ):
            object.__setattr__(self, name, decimal_value(getattr(self, name)))
        for name in ("broker_position_id", "position_ticket", "symbol"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"{name} must be non-empty")
        if self.direction not in {item.value for item in Direction}:
            raise ValueError("broker position direction must be BUY or SELL")
        if (
            self.magic < 0
            or self.remaining_volume <= 0
            or self.entry_price <= 0
            or self.current_sl < 0
            or self.final_tp < 0
            or self.position_version < 1
            or self.opened_at_ms < 0
        ):
            raise ValueError("broker position facts are invalid")


@dataclass(frozen=True, slots=True)
class DurablePositionContext:
    """Fresh durable context resolved from the authoritative ledger/cache."""

    resolved_at_ms: int
    account_id: str
    trade_id: str
    broker_position_id: str
    symbol: str
    magic: int
    direction: str
    initial_volume: Decimal
    original_sl: Decimal
    tp1: Decimal
    tp2: Decimal
    tp3: Decimal
    initial_risk_amount: Decimal
    lifecycle_stage: str
    position_version: int
    broker_order_id: str | None = None
    realised_pnl: Decimal = Decimal("0")
    pending_action: str | None = None
    pending_milestone: str | None = None
    pending_command_id: str | None = None
    pending_close_volume: Decimal = Decimal("0")
    pending_filled_volume: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        for name in (
            "initial_volume",
            "original_sl",
            "tp1",
            "tp2",
            "tp3",
            "initial_risk_amount",
            "realised_pnl",
            "pending_close_volume",
            "pending_filled_volume",
        ):
            object.__setattr__(self, name, decimal_value(getattr(self, name)))
        for name in (
            "account_id",
            "trade_id",
            "broker_position_id",
            "symbol",
            "direction",
            "lifecycle_stage",
        ):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"{name} must be non-empty")
        if self.resolved_at_ms < 0 or self.magic <= 0 or self.position_version < 1:
            raise ValueError("durable position identity/version is invalid")
        if self.direction not in {item.value for item in Direction}:
            raise ValueError("durable position direction must be BUY or SELL")
        if self.lifecycle_stage not in {item.value for item in LifecycleStage}:
            raise ValueError("durable lifecycle stage is invalid")
        if (
            self.initial_volume <= 0
            or min(self.original_sl, self.tp1, self.tp2, self.tp3) <= 0
            or self.initial_risk_amount <= 0
            or self.pending_close_volume < 0
            or self.pending_filled_volume < 0
            or self.pending_filled_volume > self.pending_close_volume
        ):
            raise ValueError("durable position risk/targets/pending volumes are invalid")
        has_action = self.pending_action is not None
        if has_action != (self.pending_command_id is not None):
            raise ValueError("pending action and command identity must agree")
        if self.pending_action is not None and self.pending_action not in {
            item.value for item in LifecycleActionKind
        }:
            raise ValueError("durable pending action is invalid")
        if self.pending_milestone is not None and self.pending_milestone not in {
            item.value for item in Milestone
        }:
            raise ValueError("durable pending milestone is invalid")
        if has_action != (self.pending_milestone is not None):
            raise ValueError("durable pending action requires exactly one milestone")
        if self.pending_action == LifecycleActionKind.VIRTUAL_MILESTONE.value:
            raise ValueError("virtual milestones cannot be broker-pending actions")
        volume_action = self.pending_action in {
            LifecycleActionKind.PARTIAL_CLOSE.value,
            LifecycleActionKind.CLOSE.value,
        }
        if volume_action != (self.pending_close_volume > 0):
            raise ValueError("pending close action must carry explicit close volume")
        if not volume_action and self.pending_filled_volume != 0:
            raise ValueError("only pending close actions may have filled volume")


@runtime_checkable
class PositionContextResolver(Protocol):
    def __call__(
        self, observed: BrokerObservedPosition
    ) -> DurablePositionContext | None | Awaitable[DurablePositionContext | None]:
        """Resolve current durable context without inferring from broker comments."""


@dataclass(frozen=True, slots=True)
class BrokerObservedDeal:
    """Authenticated broker deal facts before durable trade attribution."""

    deal_id: str
    position_id: str
    order_id: str | None
    wire_trade_id: str | None
    wire_command_id: str | None
    symbol: str
    magic: int
    direction: str
    entry_type: str
    broker_time_ms: int

    def __post_init__(self) -> None:
        for name in ("deal_id", "position_id", "symbol", "direction", "entry_type"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"{name} must be non-empty")
        if self.magic <= 0 or self.broker_time_ms < 0:
            raise ValueError("broker deal scope/time is invalid")
        if self.direction not in {item.value for item in Direction}:
            raise ValueError("broker deal direction must be BUY or SELL")


@dataclass(frozen=True, slots=True)
class DurableDealAttribution:
    """Exact ledger/cache binding for one immutable broker deal identity."""

    resolved_at_ms: int
    account_id: str
    deal_id: str
    position_id: str
    trade_id: str
    command_id: str | None = None

    def __post_init__(self) -> None:
        for name in ("account_id", "deal_id", "position_id", "trade_id"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"{name} must be non-empty")
        if self.resolved_at_ms < 0:
            raise ValueError("deal attribution resolution time is invalid")


@runtime_checkable
class DealAttributionResolver(Protocol):
    def __call__(
        self, observed: BrokerObservedDeal
    ) -> DurableDealAttribution | None | Awaitable[DurableDealAttribution | None]:
        """Resolve a deal by immutable broker IDs, never comment/time proximity."""


@dataclass(frozen=True, slots=True)
class BrokerTick:
    """One authenticated quote; the gateway retains only the latest item."""

    msg_id: str
    connection_epoch: str
    symbol: str
    received_at_ms: int
    message_ts_ms: int
    broker_time_ms: int
    bid: Decimal
    ask: Decimal
    spread_points: Decimal
    volume: int

    def __post_init__(self) -> None:
        for name in ("bid", "ask", "spread_points"):
            object.__setattr__(self, name, decimal_value(getattr(self, name)))
        if not self.msg_id or not self.connection_epoch or not self.symbol:
            raise ValueError("broker tick identity must be non-empty")
        if (
            self.received_at_ms < 0
            or self.message_ts_ms < 0
            or self.broker_time_ms < 0
            or self.bid <= 0
            or self.ask < self.bid
            or self.spread_points < 0
            or self.volume < 0
        ):
            raise ValueError("broker tick values are invalid")

    def quote_snapshot(self) -> QuoteSnapshot:
        # Policy freshness is measured from authenticated local receipt.  The
        # broker timestamp remains retained separately as provenance.
        return QuoteSnapshot(self.symbol, self.bid, self.ask, self.received_at_ms)


@dataclass(frozen=True, slots=True)
class CompletedBar:
    symbol: str
    timeframe: str
    open_time_ms: int
    close_time_ms: int
    captured_at_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int

    def __post_init__(self) -> None:
        for name in ("open", "high", "low", "close"):
            object.__setattr__(self, name, decimal_value(getattr(self, name)))


@dataclass(frozen=True, slots=True)
class GatewayConfig:
    auth_token: str
    config_hash: str
    expected: ExpectedBrokerIdentity
    expected_trade_mode: BrokerTradeMode = BrokerTradeMode.DEMO
    expected_currency: str = "USD"
    server_id: str = "quant-swarm-gateway"
    host: str = "127.0.0.1"
    port: int = 9876
    max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES
    max_sequence: int = (1 << 63) - 1
    heartbeat_timeout_ms: int = 5_000
    handshake_timeout_ms: int = 5_000
    snapshot_timeout_ms: int = 5_000
    max_message_age_ms: int = 10_000
    max_future_skew_ms: int = 1_000
    max_context_age_ms: int = 5_000
    max_snapshot_items: int = 10_000
    dedupe_limit: int = 50_000
    max_completed_bars_per_timeframe: int = 5_000
    session_mode: BrokerSessionMode = BrokerSessionMode.SHADOW

    def __post_init__(self) -> None:
        if not isinstance(self.expected, ExpectedBrokerIdentity):
            raise TypeError("expected must be ExpectedBrokerIdentity")
        if not isinstance(self.expected_trade_mode, BrokerTradeMode):
            raise TypeError("expected_trade_mode must be BrokerTradeMode")
        if not isinstance(self.session_mode, BrokerSessionMode):
            raise TypeError("session_mode must be BrokerSessionMode")
        for name in ("auth_token", "config_hash", "expected_currency", "server_id"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValueError(f"{name} must be non-empty")
        if not _is_loopback_host(self.host):
            raise ValueError("broker gateway must bind only to loopback")
        if not 0 <= self.port <= 65_535:
            raise ValueError("port must be in [0,65535]")
        if self.expected_trade_mode is BrokerTradeMode.CONTEST:
            raise ValueError("contest accounts are not authorised")
        if self.expected_trade_mode is BrokerTradeMode.REAL:
            raise ValueError(
                "REAL accounts are not supported by this release"
            )
        if not 512 <= self.max_frame_bytes <= 65_536:
            raise ValueError("frame bound must be in [512,65536]")
        for name in (
            "max_sequence",
            "heartbeat_timeout_ms",
            "handshake_timeout_ms",
            "snapshot_timeout_ms",
            "max_message_age_ms",
            "max_context_age_ms",
            "max_snapshot_items",
            "dedupe_limit",
            "max_completed_bars_per_timeframe",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.max_completed_bars_per_timeframe > 5_000:
            raise ValueError("completed-bar timeframe cache cannot exceed 5000")
        if self.max_future_skew_ms < 0:
            raise ValueError("max_future_skew_ms cannot be negative")

    @property
    def execution_enabled(self) -> bool:
        return self.session_mode is BrokerSessionMode.DEMO


@dataclass(slots=True)
class _CommandTracker:
    command: OutboundCommand
    fingerprint: str
    ack_seen: bool = False
    effect_seen: bool = False
    effect_satisfied: bool = False
    rejected: bool = False
    expected_position_ids: frozenset[str] = frozenset()
    effected_position_ids: set[str] = field(default_factory=set)
    open_position_id: str | None = None
    open_position_ticket: str | None = None
    open_order_id: str | None = None
    open_deal_ids: set[str] = field(default_factory=set)
    open_filled_volume: Decimal = Decimal("0")

    @property
    def effect_complete(self) -> bool:
        if self.command.action is CommandAction.CLOSE_ALL:
            return self.expected_position_ids <= self.effected_position_ids
        return self.effect_satisfied

    def observe_effect(self, fields: Mapping[str, Any], position_id: str) -> None:
        self.effect_seen = True
        event_type = str(fields["event_type"])
        action = self.command.action
        if action is CommandAction.OPEN:
            if event_type != "POSITION_OPENED":
                return
            position_ticket = str(fields["position_ticket"])
            order_id = str(fields["order_id"])
            deal_id = str(fields["deal_id"])
            if any(value in {"", "0"} for value in (
                position_id,
                position_ticket,
                order_id,
                deal_id,
            )):
                raise GatewayProtocolError(
                    "OPEN fill lacks stable broker position/order/deal identity"
                )
            identities = (
                (self.open_position_id, position_id, "position"),
                (self.open_position_ticket, position_ticket, "position ticket"),
                (self.open_order_id, order_id, "order"),
            )
            for prior, observed, label in identities:
                if prior is not None and prior != observed:
                    raise GatewayProtocolError(
                        f"progressive OPEN fill changed its {label} identity"
                    )
            if deal_id in self.open_deal_ids:
                raise GatewayProtocolError(
                    "progressive OPEN fill repeated a deal under another event"
                )
            requested_source = self.command.expected_effect.get(
                "initial_volume", self.command.payload.get("volume")
            )
            if requested_source is None:
                raise GatewayProtocolError("OPEN command lacks requested volume proof")
            requested = decimal_value(requested_source)
            delta = abs(decimal_value(fields["volume_delta"]))
            remaining = decimal_value(fields["remaining_volume"])
            cumulative = self.open_filled_volume + delta
            if (
                requested <= 0
                or delta <= 0
                or cumulative > requested
                or remaining != cumulative
            ):
                raise GatewayProtocolError(
                    "progressive OPEN fill differs from exact cumulative broker volume"
                )
            self.open_position_id = position_id
            self.open_position_ticket = position_ticket
            self.open_order_id = order_id
            self.open_deal_ids.add(deal_id)
            self.open_filled_volume = cumulative
            # A transport ACK (including DONE_PARTIAL) is receipt evidence, not
            # proof that no remainder can still fill.  Live completion is safe
            # only after broker IN deals exactly reach the reserved request.
            self.effect_satisfied = cumulative == requested
        elif action is CommandAction.MODIFY_SL:
            target = decimal_value(self.command.expected_effect["target_sl"])
            self.effect_satisfied = (
                event_type == "POSITION_UPDATED"
                and decimal_value(fields["sl"]) == target
                and int(fields["position_version"])
                > int(self.command.expected_position_version or -1)
            )
        elif action is CommandAction.PARTIAL_CLOSE:
            target = decimal_value(
                self.command.expected_effect["target_remaining_volume"]
            )
            self.effect_satisfied = (
                event_type in {"PARTIAL_FILLED", "POSITION_CLOSED"}
                and decimal_value(fields["remaining_volume"]) <= target
            )
        elif action is CommandAction.CLOSE:
            self.effect_satisfied = (
                event_type == "POSITION_CLOSED"
                and decimal_value(fields["remaining_volume"]) == 0
            )
        elif action is CommandAction.CLOSE_ALL:
            if (
                event_type == "POSITION_CLOSED"
                and decimal_value(fields["remaining_volume"]) == 0
            ):
                self.effected_position_ids.add(position_id)


@dataclass(slots=True)
class _SnapshotAssembly:
    correlation_id: str
    created_at_ms: int
    deadline_ms: int
    requested_cursor: str
    account: ProtocolMessage | None = None
    symbol: ProtocolMessage | None = None
    position_snapshot_id: str | None = None
    position_count: int | None = None
    position_deal_cursor: str | None = None
    positions: dict[str, ProtocolMessage] = field(default_factory=dict)
    positions_ended: bool = False
    order_snapshot_id: str | None = None
    order_count: int | None = None
    orders: dict[str, ProtocolMessage] = field(default_factory=dict)
    orders_ended: bool = False
    replay_ended: bool = False


@dataclass(slots=True)
class _ReplayAssembly:
    correlation_id: str
    created_at_ms: int
    deadline_ms: int
    replay_id: str
    cursor_from: str
    expected_count: int
    items: dict[str, ProtocolMessage] = field(default_factory=dict)
    last_cursor: str | None = None


class BrokerGateway(BrokerPort, ReconciliationBrokerPort):
    """One-client v4 TCP adapter with strict epoch and reconciliation fencing."""

    def __init__(
        self,
        *,
        config: GatewayConfig,
        clock: Clock,
        committed_cursor_provider: Callable[[str], str | None] | None = None,
        position_context_resolver: PositionContextResolver | None = None,
        deal_attribution_resolver: DealAttributionResolver | None = None,
        runtime: RuntimeStateMachine | None = None,
        tuple_lock_acquirer: Callable[[BrokerIdentity], bool] | None = None,
    ) -> None:
        self.config = config
        self.clock = clock
        self._codec = ProtocolCodec(
            config.auth_token, max_frame_bytes=config.max_frame_bytes
        )
        self._cursor_provider = committed_cursor_provider or (lambda _account: None)
        self._context_resolver = position_context_resolver
        self._deal_attribution_resolver = deal_attribution_resolver
        self._runtime = runtime
        self._tuple_lock_acquirer = tuple_lock_acquirer

        self._state = GatewayState.STOPPED
        self._server: asyncio.AbstractServer | None = None
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._connection_token = 0
        self._reader_task: asyncio.Task[None] | None = None
        self._watchdog_task: asyncio.Task[None] | None = None
        self._write_lock = asyncio.Lock()
        self._request_lock = asyncio.Lock()

        self._identity: BrokerIdentity | None = None
        self._epoch: str | None = None
        self._last_epoch: str | None = None
        self._rx_sequence = 0
        self._tx_sequence = 0
        self._message_counter = 0
        self._handshake_deadline_ms: int | None = None
        self._last_heartbeat_received_ms: int | None = None
        self._heartbeat_seen = False
        self._last_error: str | None = None

        self._signals: asyncio.Queue[GatewaySignal] = asyncio.Queue()
        self._seen_message_ids: dict[str, str] = {}
        self._seen_event_ids: dict[str, str] = {}
        self._seen_ack_ids: dict[str, str] = {}
        self._seen_epochs: dict[str, str] = {}
        self._command_fingerprints: dict[str, str] = {}
        self._commands: dict[str, _CommandTracker] = {}
        self._terminal_commands: dict[str, OutboundCommand] = {}
        self._internal_commands: set[str] = set()

        self._snapshots: dict[str, _SnapshotAssembly] = {}
        self._completed_snapshots: deque[AuthoritativeBrokerSnapshot] = deque()
        self._snapshot_waiters: dict[
            str, asyncio.Future[AuthoritativeBrokerSnapshot]
        ] = {}
        self._replays: dict[str, _ReplayAssembly] = {}
        self._deal_pages: dict[str, tuple[str, tuple[BrokerDealRecord, ...]]] = {}
        self._deal_waiters: dict[str, asyncio.Future[tuple[str, tuple[BrokerDealRecord, ...]]]] = {}

        self._latest_account_health: Mapping[str, Any] | None = None
        self._latest_symbol_snapshot: SymbolSnapshot | None = None
        self._latest_tick: BrokerTick | None = None
        self._tick_queue: asyncio.Queue[BrokerTick] = asyncio.Queue(maxsize=1)
        self._closed_bars: dict[tuple[str, str, int], CompletedBar] = {}
        self._latest_observed_positions: dict[str, BrokerObservedPosition] = {}
        self._latest_position_contexts: dict[str, DurablePositionContext] = {}

    @property
    def state(self) -> GatewayState:
        return self._state

    @property
    def connection_epoch(self) -> str | None:
        return self._epoch

    @property
    def identity(self) -> BrokerIdentity | None:
        return self._identity

    @property
    def last_error(self) -> str | None:
        return self._last_error

    @property
    def listen_port(self) -> int | None:
        if self._server is None or not self._server.sockets:
            return None
        return int(self._server.sockets[0].getsockname()[1])

    @property
    def latest_account_health(self) -> Mapping[str, Any] | None:
        return self._latest_account_health

    @property
    def latest_symbol_snapshot(self) -> SymbolSnapshot | None:
        return self._latest_symbol_snapshot

    @property
    def latest_tick(self) -> BrokerTick | None:
        return self._latest_tick

    @property
    def completed_bars(self) -> tuple[CompletedBar, ...]:
        return tuple(
            sorted(
                self._closed_bars.values(),
                key=lambda item: (item.open_time_ms, item.symbol, item.timeframe),
            )
        )

    async def start(self) -> None:
        if self._state not in {GatewayState.STOPPED, GatewayState.CLOSED}:
            raise GatewayError("gateway is already started")
        self._server = await asyncio.start_server(
            self._accept_client,
            self.config.host,
            self.config.port,
            limit=self.config.max_frame_bytes + 1,
        )
        self._state = GatewayState.LISTENING
        self._watchdog_task = asyncio.create_task(
            self._watchdog(), name="quant-swarm-broker-gateway-watchdog"
        )

    async def close(self) -> None:
        if self._watchdog_task is not None:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
            self._watchdog_task = None
        if self._writer is not None:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except (ConnectionError, OSError):
                pass
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        if self._reader_task is not None and self._reader_task is not asyncio.current_task():
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
        self._reader_task = None
        self._writer = None
        self._reader = None
        self._fail_waiters(GatewayDisconnectedError("gateway closed"))
        self._state = GatewayState.CLOSED

    async def next_signal(self, timeout: float | None = None) -> GatewaySignal:
        if timeout is None:
            return await self._signals.get()
        return await asyncio.wait_for(self._signals.get(), timeout)

    async def next_tick(self, timeout: float | None = None) -> BrokerTick:
        if timeout is None:
            return await self._tick_queue.get()
        return await asyncio.wait_for(self._tick_queue.get(), timeout)

    async def check_health(self) -> None:
        now = self.clock.now_ms()
        if (
            self._state is GatewayState.HANDSHAKING
            and self._handshake_deadline_ms is not None
            and now > self._handshake_deadline_ms
        ):
            await self._disconnect("HELLO handshake timed out", protocol=True)
            return
        if self._state is GatewayState.AUTHENTICATED:
            last = self._last_heartbeat_received_ms
            if last is None or now - last > self.config.heartbeat_timeout_ms:
                self._emit(
                    GatewaySignalType.STALE_HEARTBEAT,
                    reason="authenticated EA heartbeat is stale",
                )
                await self._disconnect("stale heartbeat", unknown=True)
                return
        expired = [
            correlation
            for correlation, assembly in self._snapshots.items()
            if now > assembly.deadline_ms
        ]
        expired.extend(
            correlation
            for correlation, assembly in self._replays.items()
            if now > assembly.deadline_ms
        )
        if expired:
            await self._disconnect(
                "partial snapshot/replay generation timed out: "
                + ",".join(sorted(set(expired))),
                protocol=True,
                unknown=True,
            )

    async def send(self, command: OutboundCommand) -> None:
        if not self.config.execution_enabled:
            raise GatewayError("order transmission is disabled for this runtime mode")
        await self.check_health()
        self._require_authenticated()
        if self._epoch is None:
            raise GatewayDisconnectedError(
                "authenticated gateway has no active connection epoch"
            )
        if command.connection_epoch != self._epoch:
            raise GatewayDisconnectedError(
                f"command epoch {command.connection_epoch!r} is not active {self._epoch!r}"
            )
        fields = self._command_fields(command)
        fingerprint = _fingerprint(asdict(command))
        existing = self._command_fingerprints.get(command.command_id)
        if existing is not None:
            if existing != fingerprint:
                raise GatewayCacheError("command_id reused with conflicting immutable content")
            return
        self._remember(self._command_fingerprints, command.command_id, fingerprint)
        expected_positions = (
            frozenset(self._latest_observed_positions)
            if command.action is CommandAction.CLOSE_ALL
            else frozenset(
                {command.position_id}
                if command.position_id is not None
                and command.action is not CommandAction.OPEN
                else ()
            )
        )
        tracker = _CommandTracker(
            command,
            fingerprint,
            expected_position_ids=expected_positions,
        )
        self._commands[command.command_id] = tracker
        try:
            await self._send_protocol(
                MessageType.COMMAND, command.command_id, fields=fields
            )
        except BaseException:
            self._commands.pop(command.command_id, None)
            raise

    async def fetch_snapshot(
        self, expected: ExpectedBrokerIdentity
    ) -> AuthoritativeBrokerSnapshot:
        self._require_expected(expected)
        await self.check_health()
        self._require_authenticated()
        async with self._request_lock:
            while self._completed_snapshots:
                completed = self._completed_snapshots.popleft()
                if self._snapshot_is_fresh(completed):
                    return completed
            cursor = self._committed_cursor(expected.account_id)
            correlation = self._new_id("RECON")
            future = asyncio.get_running_loop().create_future()
            self._snapshot_waiters[correlation] = future
            try:
                await self._request_snapshot(cursor, correlation=correlation)
            except BaseException:
                self._snapshot_waiters.pop(correlation, None)
                raise
        try:
            return await asyncio.wait_for(
                future, self.config.snapshot_timeout_ms / 1_000
            )
        except TimeoutError as exc:
            await self._disconnect(
                "snapshot request timed out", protocol=True, unknown=True
            )
            raise GatewaySnapshotError("snapshot request timed out") from exc
        finally:
            self._snapshot_waiters.pop(correlation, None)

    async def fetch_deal_page(
        self, account_id: str, cursor: str | None
    ) -> BrokerDealPage:
        if account_id != self.config.expected.account_id:
            raise GatewaySnapshotError("deal-page account is outside pinned identity")
        await self.check_health()
        self._require_authenticated()
        normalised = _normalise_cursor(cursor)
        cached = self._deal_pages.pop(normalised, None)
        if cached is None:
            async with self._request_lock:
                cached = self._deal_pages.pop(normalised, None)
                if cached is None:
                    future = self._deal_waiters.get(normalised)
                    if future is None:
                        future = asyncio.get_running_loop().create_future()
                        self._deal_waiters[normalised] = future
                        await self._request_snapshot(normalised)
                else:
                    future = None
            if cached is None:
                if future is None:
                    raise GatewayError(
                        "snapshot request future was not created"
                    )
                try:
                    cached = await asyncio.wait_for(
                        future, self.config.snapshot_timeout_ms / 1_000
                    )
                except TimeoutError as exc:
                    await self._disconnect(
                        "deal replay request timed out", protocol=True, unknown=True
                    )
                    raise GatewaySnapshotError("deal replay request timed out") from exc
                finally:
                    self._deal_waiters.pop(normalised, None)
        next_cursor, deals = cached
        return BrokerDealPage(cursor, next_cursor, deals, True)

    async def _accept_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        if not _peer_is_loopback(peer) or self._writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            return
        self._connection_token += 1
        token = self._connection_token
        self._reader = reader
        self._writer = writer
        self._reader_task = asyncio.current_task()
        self._state = GatewayState.HANDSHAKING
        self._handshake_deadline_ms = (
            self.clock.now_ms() + self.config.handshake_timeout_ms
        )
        self._reset_session_state()
        try:
            while self._writer is writer:
                try:
                    frame = await reader.readline()
                except (ValueError, asyncio.LimitOverrunError) as exc:
                    raise GatewayProtocolError("inbound frame exceeded bound") from exc
                if not frame:
                    break
                if len(frame) > self.config.max_frame_bytes:
                    raise GatewayProtocolError("inbound frame exceeded bound")
                message = self._decode_inbound(frame)
                await self._dispatch(message)
        except (GatewayProtocolError, ProtocolError) as exc:
            await self._disconnect(str(exc), protocol=True, unknown=True, token=token)
        except GatewayCacheError as exc:
            await self._disconnect(str(exc), cache=True, unknown=True, token=token)
        except (ConnectionError, OSError, asyncio.IncompleteReadError) as exc:
            await self._disconnect(
                f"EA transport failed: {type(exc).__name__}", unknown=True, token=token
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._disconnect(
                f"gateway dispatch failed: {type(exc).__name__}: {exc}",
                protocol=True,
                unknown=True,
                token=token,
            )
        finally:
            if token == self._connection_token and self._writer is writer:
                await self._disconnect("EA disconnected", unknown=True, token=token)

    def _decode_inbound(self, frame: bytes) -> ProtocolMessage:
        expected_epoch = self._epoch if self._state is GatewayState.AUTHENTICATED else None
        message = self._codec.decode(frame, expected_epoch=expected_epoch)
        if message.seq > self.config.max_sequence:
            raise GatewayProtocolError("sequence exceeds configured bound")
        expected_sequence = self._rx_sequence + 1
        if message.seq != expected_sequence:
            raise GatewayProtocolError(
                f"non-contiguous inbound sequence {message.seq}, expected {expected_sequence}"
            )
        if self._state is GatewayState.HANDSHAKING:
            if message.message_type is not MessageType.HELLO or message.seq != 1:
                raise GatewayProtocolError("first frame must be HELLO sequence 1")
        elif message.message_type is MessageType.HELLO:
            raise GatewayProtocolError("HELLO is forbidden after authentication")
        self._rx_sequence = message.seq
        fingerprint = _fingerprint(message.as_flat_dict())
        previous = self._seen_message_ids.get(message.msg_id)
        if previous is not None:
            raise GatewayProtocolError("duplicate msg_id in one connection epoch")
        self._remember(self._seen_message_ids, message.msg_id, fingerprint)
        return message

    async def _dispatch(self, message: ProtocolMessage) -> None:
        if self._state is GatewayState.HANDSHAKING:
            await self._handle_hello(message)
            return
        if self._state is not GatewayState.AUTHENTICATED:
            raise GatewayProtocolError("application frame outside authenticated state")
        now = self.clock.now_ms()
        age = now - message.ts_ms
        if age < -self.config.max_future_skew_ms or age > self.config.max_message_age_ms:
            raise GatewayProtocolError(f"stale/future authenticated message age={age}ms")

        handlers = {
            MessageType.ACCOUNT_SNAPSHOT: self._handle_account_snapshot,
            MessageType.SYMBOL_SNAPSHOT: self._handle_symbol_snapshot,
            MessageType.POSITION_SNAPSHOT_BEGIN: self._handle_position_begin,
            MessageType.POSITION_SNAPSHOT_ITEM: self._handle_position_item,
            MessageType.POSITION_SNAPSHOT_END: self._handle_position_end,
            MessageType.PENDING_ORDER_SNAPSHOT_BEGIN: self._handle_order_begin,
            MessageType.PENDING_ORDER_SNAPSHOT_ITEM: self._handle_order_item,
            MessageType.PENDING_ORDER_SNAPSHOT_END: self._handle_order_end,
            MessageType.DEAL_REPLAY_BEGIN: self._handle_replay_begin,
            MessageType.DEAL_REPLAY_ITEM: self._handle_replay_item,
            MessageType.DEAL_REPLAY_END: self._handle_replay_end,
            MessageType.HEARTBEAT: self._handle_heartbeat,
            MessageType.COMMAND_ACK: self._handle_command_ack,
            MessageType.COMMAND_NACK: self._handle_command_nack,
            MessageType.BROKER_EVENT: self._handle_broker_event,
            MessageType.BAR: self._handle_bar,
            MessageType.TICK: self._handle_tick,
        }
        handler = handlers.get(message.message_type)
        if handler is None:
            raise GatewayProtocolError(
                f"message type {message.message_type.value} is invalid EA->gateway"
            )
        result = handler(message)
        if inspect.isawaitable(result):
            await result

    async def _handle_hello(self, message: ProtocolMessage) -> None:
        fields = message.fields
        age = self.clock.now_ms() - message.ts_ms
        if age < -self.config.max_future_skew_ms or age > self.config.max_message_age_ms:
            raise GatewayProtocolError(f"stale/future HELLO age={age}ms")
        try:
            trade_mode = BrokerTradeMode(str(fields["account_trade_mode"]))
        except ValueError as exc:
            raise GatewayProtocolError("unknown account trade mode") from exc
        try:
            session_mode = BrokerSessionMode(str(fields["session_mode"]))
        except ValueError as exc:
            raise GatewayProtocolError("unknown broker session mode") from exc
        actual = BrokerIdentity(
            account_id=str(fields["account_id"]),
            broker_server=str(fields["broker_server"]),
            trade_mode=trade_mode,
            symbol=str(fields["symbol"]),
            magic=int(fields["magic"]),
            connection_epoch=message.epoch,
            trade_allowed=bool(fields["trade_allowed"]),
            expert_allowed=bool(fields["expert_allowed"]),
        )
        mismatches = list(
            identity_mismatches(
                self.config.expected,
                actual,
                require_execution_permissions=self.config.execution_enabled,
            )
        )
        if trade_mode is not self.config.expected_trade_mode:
            mismatches.append(
                f"trade_mode: expected {self.config.expected_trade_mode.value}, got {trade_mode.value}"
            )
        if fields["role"] != "MT5_EA":
            mismatches.append(
                f"role: expected MT5_EA, got {fields['role']}"
            )
        if fields["ea_version"] != MQL5_EA_VERSION:
            mismatches.append(
                f"ea_version: expected {MQL5_EA_VERSION}, "
                f"got {fields['ea_version']}"
            )
        if session_mode is not self.config.session_mode:
            mismatches.append(
                f"session_mode: expected {self.config.session_mode.value}, "
                f"got {session_mode.value}"
            )
        if self.config.execution_enabled and bool(fields["emergency_stop"]):
            mismatches.append(
                "emergency_stop: DEMO requires the EA entry fence to be disabled"
            )
        if fields["config_hash"] != self.config.config_hash:
            mismatches.append("config_hash mismatch")
        if message.epoch in self._seen_epochs:
            mismatches.append("connection epoch was reused")
        elif len(self._seen_epochs) >= self.config.dedupe_limit:
            mismatches.append("bounded accepted-epoch cache is exhausted")
        cursor: str
        try:
            cursor = self._committed_cursor(actual.account_id)
        except Exception as exc:
            self._emit(
                GatewaySignalType.CACHE_FAILURE,
                reason=f"committed cursor provider failed: {type(exc).__name__}",
            )
            raise GatewayCacheError("committed cursor provider unavailable") from exc

        tuple_locked = False
        if not mismatches:
            try:
                if self._tuple_lock_acquirer is not None:
                    tuple_locked = bool(self._tuple_lock_acquirer(actual))
                else:
                    tuple_locked = self._runtime is None
            except Exception as exc:
                mismatches.append(
                    f"ownership tuple lock failed: {type(exc).__name__}"
                )
            if not tuple_locked and not mismatches:
                mismatches.append("ownership tuple lock was not acquired")
        if not mismatches and self._runtime is not None:
            try:
                self._runtime.accept_connection(
                    self.config.expected,
                    actual,
                    transport_loopback=True,
                    tuple_lock_acquired=tuple_locked,
                    require_execution_permissions=self.config.execution_enabled,
                )
            except Exception as exc:
                mismatches.append(f"runtime rejected connection: {type(exc).__name__}")
            if self._runtime.state is ControlPlaneState.HALTED:
                mismatches.append("runtime rejected broker identity")
        if not mismatches:
            self._remember(
                self._seen_epochs,
                message.epoch,
                _fingerprint(
                    {
                        "identity": asdict(actual),
                        "config_hash": self.config.config_hash,
                        "trade_mode": self.config.expected_trade_mode.value,
                        "role": fields["role"],
                        "ea_version": fields["ea_version"],
                        "emergency_stop": fields["emergency_stop"],
                        "session_mode": self.config.session_mode.value,
                    }
                ),
            )

        ack_fields = {
            "status": "REJECTED" if mismatches else "ACCEPTED",
            "server_id": self.config.server_id,
            "config_hash": self.config.config_hash,
            "expected_account_id": self.config.expected.account_id,
            "expected_broker_server": self.config.expected.broker_server,
            "expected_account_trade_mode": self.config.expected_trade_mode.value,
            "expected_ea_version": MQL5_EA_VERSION,
            "expected_session_mode": self.config.session_mode.value,
            "expected_symbol": self.config.expected.symbol,
            "expected_magic": self.config.expected.magic,
            "allow_real": False,
            "deal_cursor": cursor,
            "reason": "; ".join(mismatches),
        }
        self._epoch = message.epoch
        ack = await self._send_protocol(
            MessageType.HELLO_ACK, message.msg_id, fields=ack_fields
        )
        if mismatches:
            raise GatewayProtocolError("HELLO identity rejected: " + "; ".join(mismatches))
        self._identity = actual
        self._last_epoch = message.epoch
        self._state = GatewayState.AUTHENTICATED
        self._handshake_deadline_ms = None
        self._last_heartbeat_received_ms = self.clock.now_ms()
        self._heartbeat_seen = False
        self._ensure_snapshot_generation(ack.msg_id, requested_cursor=cursor)
        self._emit(GatewaySignalType.CONNECTED, payload={"identity": asdict(actual)})

    def _handle_account_snapshot(self, message: ProtocolMessage) -> None:
        assembly = self._generation(message.correlation_id)
        fields = message.fields
        self._validate_account_scope(fields)
        if str(fields["currency"]) != self.config.expected_currency:
            raise GatewayProtocolError("account currency is not the pinned sizing currency")
        account_mode = str(fields["account_mode"])
        if account_mode not in {"HEDGING", "NETTING"}:
            raise GatewayProtocolError("account margin mode is not canonical")
        if self.config.execution_enabled and account_mode != "HEDGING":
            raise GatewayProtocolError(
                "order-producing sessions require a HEDGING account"
            )
        self._set_once(assembly, "account", message)
        self._latest_account_health = MappingProxyType(
            {
                "received_at_ms": self.clock.now_ms(),
                "message_ts_ms": message.ts_ms,
                **dict(fields),
            }
        )

    def _handle_symbol_snapshot(self, message: ProtocolMessage) -> None:
        assembly = self._generation(message.correlation_id)
        fields = message.fields
        if fields["symbol"] != self.config.expected.symbol:
            raise GatewayProtocolError("symbol snapshot is outside pinned symbol")
        profit = decimal_value(fields["tick_value_profit"])
        loss = decimal_value(fields["tick_value_loss"])
        if profit <= 0 or loss <= 0:
            raise GatewayProtocolError("zero/unknown tick-value conversion")
        conservative = max(profit, loss)
        self._latest_symbol_snapshot = SymbolSnapshot(
            symbol=str(fields["symbol"]),
            digits=int(fields["digits"]),
            tick_size=fields["tick_size"],
            tick_value_per_lot=conservative,
            contract_size=fields["contract_size"],
            volume_min=fields["volume_min"],
            volume_step=fields["volume_step"],
            volume_max=fields["volume_max"],
            stops_level_points=int(fields["stops_level"]),
            freeze_level_points=int(fields["freeze_level"]),
            trade_enabled=str(fields["trade_mode"]) not in {"0", "DISABLED"},
        )
        self._set_once(assembly, "symbol", message)

    def _handle_position_begin(self, message: ProtocolMessage) -> None:
        assembly = self._generation(message.correlation_id)
        snapshot_id = str(message.fields["snapshot_id"])
        count = int(message.fields["count"])
        self._validate_item_count(count)
        if assembly.position_snapshot_id is not None:
            raise GatewayProtocolError("duplicate position snapshot begin")
        deal_cursor = _require_cursor(str(message.fields["deal_cursor"]))
        assembly.position_snapshot_id = snapshot_id
        assembly.position_count = count
        assembly.position_deal_cursor = deal_cursor

    def _handle_position_item(self, message: ProtocolMessage) -> None:
        assembly = self._generation(message.correlation_id)
        if assembly.position_snapshot_id is None or assembly.positions_ended:
            raise GatewayProtocolError("position item outside open generation")
        if message.fields["snapshot_id"] != assembly.position_snapshot_id:
            raise GatewayProtocolError("position item generation mismatch")
        position_id = str(message.fields["position_id"])
        self._insert_semantic(assembly.positions, position_id, message)
        self._validate_item_count(len(assembly.positions))

    async def _handle_position_end(self, message: ProtocolMessage) -> None:
        assembly = self._generation(message.correlation_id)
        if assembly.position_snapshot_id is None or assembly.positions_ended:
            raise GatewayProtocolError("position end without one open generation")
        if message.fields["snapshot_id"] != assembly.position_snapshot_id:
            raise GatewayProtocolError("position end generation mismatch")
        if int(message.fields["count"]) != assembly.position_count:
            raise GatewayProtocolError("position begin/end count conflict")
        if message.fields["deal_cursor"] != assembly.position_deal_cursor:
            raise GatewayProtocolError("position snapshot deal cursor changed mid-generation")
        if len(assembly.positions) != assembly.position_count:
            raise GatewayProtocolError("position snapshot item count incomplete")
        assembly.positions_ended = True
        await self._try_finish_snapshot(assembly)

    def _handle_order_begin(self, message: ProtocolMessage) -> None:
        assembly = self._generation(message.correlation_id)
        snapshot_id = str(message.fields["snapshot_id"])
        count = int(message.fields["count"])
        self._validate_item_count(count)
        if assembly.order_snapshot_id is not None:
            raise GatewayProtocolError("duplicate pending-order snapshot begin")
        assembly.order_snapshot_id = snapshot_id
        assembly.order_count = count

    def _handle_order_item(self, message: ProtocolMessage) -> None:
        assembly = self._generation(message.correlation_id)
        if assembly.order_snapshot_id is None or assembly.orders_ended:
            raise GatewayProtocolError("pending-order item outside open generation")
        if message.fields["snapshot_id"] != assembly.order_snapshot_id:
            raise GatewayProtocolError("pending-order generation mismatch")
        order_id = str(message.fields["order_id"])
        self._insert_semantic(assembly.orders, order_id, message)
        self._validate_item_count(len(assembly.orders))

    async def _handle_order_end(self, message: ProtocolMessage) -> None:
        assembly = self._generation(message.correlation_id)
        if assembly.order_snapshot_id is None or assembly.orders_ended:
            raise GatewayProtocolError("pending-order end without one open generation")
        if message.fields["snapshot_id"] != assembly.order_snapshot_id:
            raise GatewayProtocolError("pending-order end generation mismatch")
        if int(message.fields["count"]) != assembly.order_count:
            raise GatewayProtocolError("pending-order begin/end count conflict")
        if len(assembly.orders) != assembly.order_count:
            raise GatewayProtocolError("pending-order snapshot item count incomplete")
        assembly.orders_ended = True
        await self._try_finish_snapshot(assembly)

    def _handle_replay_begin(self, message: ProtocolMessage) -> None:
        correlation = message.correlation_id
        if correlation in self._replays:
            raise GatewayProtocolError("duplicate deal replay begin")
        cursor = _require_cursor(str(message.fields["cursor_from"]))
        snapshot = self._generation(correlation)
        if cursor != snapshot.requested_cursor:
            raise GatewayProtocolError("deal replay did not start at committed cursor")
        count = int(message.fields["count"])
        self._validate_item_count(count)
        self._replays[correlation] = _ReplayAssembly(
            correlation,
            self.clock.now_ms(),
            self.clock.now_ms() + self.config.snapshot_timeout_ms,
            str(message.fields["replay_id"]),
            cursor,
            count,
        )

    def _handle_replay_item(self, message: ProtocolMessage) -> None:
        assembly = self._replays.get(message.correlation_id)
        if assembly is None:
            raise GatewayProtocolError("deal replay item without begin")
        if message.fields["replay_id"] != assembly.replay_id:
            raise GatewayProtocolError("deal replay generation mismatch")
        cursor = _require_cursor(str(message.fields["cursor"]))
        deal_id = str(message.fields["deal_id"])
        cursor_time, cursor_deal = _cursor_parts(cursor)
        if cursor_deal != int(deal_id) or cursor_time != int(message.fields["broker_time_ms"]):
            raise GatewayProtocolError("deal cursor is not bound to deal identity/time")
        prior = assembly.last_cursor or assembly.cursor_from
        if _cursor_parts(cursor) <= _cursor_parts(prior):
            raise GatewayProtocolError("deal replay cursor is not strictly ordered")
        self._insert_semantic(assembly.items, deal_id, message)
        assembly.last_cursor = cursor
        self._validate_item_count(len(assembly.items))

    async def _handle_replay_end(self, message: ProtocolMessage) -> None:
        assembly = self._replays.pop(message.correlation_id, None)
        if assembly is None:
            raise GatewayProtocolError("deal replay end without begin")
        if message.fields["replay_id"] != assembly.replay_id:
            raise GatewayProtocolError("deal replay end generation mismatch")
        if int(message.fields["count"]) != assembly.expected_count:
            raise GatewayProtocolError("deal replay begin/end count conflict")
        if len(assembly.items) != assembly.expected_count:
            raise GatewayProtocolError("deal replay item count incomplete")
        next_cursor = _require_cursor(str(message.fields["cursor_next"]))
        expected_next = assembly.last_cursor or assembly.cursor_from
        if next_cursor != expected_next:
            raise GatewayProtocolError("deal replay end cursor conflicts with ordered items")
        snapshot = self._generation(message.correlation_id)
        if snapshot.position_deal_cursor != next_cursor:
            raise GatewayProtocolError(
                "deal replay cursor does not fence the position snapshot"
            )
        deals = tuple(
            [await self._deal_record(item) for item in assembly.items.values()]
        )
        existing = self._deal_pages.get(assembly.cursor_from)
        page = (next_cursor, deals)
        if existing is not None and _fingerprint(existing) != _fingerprint(page):
            raise GatewayProtocolError("conflicting replay page for one cursor")
        waiter = self._deal_waiters.get(assembly.cursor_from)
        if waiter is not None and not waiter.done():
            waiter.set_result(page)
        else:
            if existing is None and len(self._deal_pages) >= self.config.dedupe_limit:
                raise GatewayCacheError("bounded deal-page cache exhausted")
            self._deal_pages[assembly.cursor_from] = page
        snapshot.replay_ended = True
        await self._try_finish_snapshot(snapshot)

    def _handle_heartbeat(self, message: ProtocolMessage) -> None:
        fields = message.fields
        if (
            fields["account_id"] != self.config.expected.account_id
            or fields["symbol"] != self.config.expected.symbol
        ):
            raise GatewayProtocolError("heartbeat identity mismatch")
        now = self.clock.now_ms()
        age = now - message.ts_ms
        if age < -self.config.max_future_skew_ms or age > self.config.heartbeat_timeout_ms:
            raise GatewayProtocolError("stale/future heartbeat timestamp")
        self._last_heartbeat_received_ms = now
        self._heartbeat_seen = True
        self._latest_account_health = MappingProxyType(
            {"received_at_ms": now, "message_ts_ms": message.ts_ms, **dict(fields)}
        )

    def _handle_command_ack(self, message: ProtocolMessage) -> None:
        fields = message.fields
        command_id = str(fields["command_id"])
        if fields["status"] not in _ACK_STATUSES:
            raise GatewayProtocolError("unknown COMMAND_ACK status")
        if not self._remember_semantic(self._seen_ack_ids, command_id, fields):
            return
        if command_id in self._internal_commands:
            self._internal_commands.discard(command_id)
            return
        tracker = self._correlated_tracker(fields)
        tracker.ack_seen = True
        self._emit(
            GatewaySignalType.COMMAND_ACK,
            command_id=command_id,
            trade_id=tracker.command.trade_id,
            payload={
                **dict(fields),
                "broker_message_id": message.msg_id,
                "broker_message_ts_ms": message.ts_ms,
            },
        )
        self._retire_tracker_if_complete(command_id, tracker)

    async def _handle_command_nack(self, message: ProtocolMessage) -> None:
        fields = message.fields
        command_id = str(fields["command_id"])
        if fields["status"] != "REJECTED":
            raise GatewayProtocolError("COMMAND_NACK status must be REJECTED")
        if not self._remember_semantic(self._seen_ack_ids, command_id, fields):
            return
        if command_id in self._internal_commands:
            self._internal_commands.discard(command_id)
            reason = str(fields["reason"])
            if any(marker in reason.upper() for marker in _CACHE_FAILURE_MARKERS):
                raise GatewayCacheError("EA snapshot command-cache failure")
            raise GatewayProtocolError(f"EA rejected snapshot request: {reason}")
        tracker = self._correlated_tracker(fields)
        tracker.rejected = True
        self._retire_tracker_if_complete(command_id, tracker)
        reason = str(fields["reason"])
        self._emit(
            GatewaySignalType.COMMAND_NACK,
            command_id=command_id,
            trade_id=tracker.command.trade_id,
            reason=reason,
            payload={
                **dict(fields),
                "broker_message_id": message.msg_id,
                "broker_message_ts_ms": message.ts_ms,
            },
        )
        if any(marker in reason.upper() for marker in _CACHE_FAILURE_MARKERS):
            raise GatewayCacheError("EA command cache failure")

    async def _handle_broker_event(self, message: ProtocolMessage) -> None:
        fields = message.fields
        event_id = str(fields["event_id"])
        if not self._remember_semantic(self._seen_event_ids, event_id, fields):
            return
        event_type = str(fields["event_type"])
        if event_type in _CACHE_FAILURE_MARKERS:
            self._emit(
                GatewaySignalType.CACHE_FAILURE,
                reason=event_type,
                payload=dict(fields),
            )
            raise GatewayCacheError("EA reported command-cache failure")
        command_id = str(fields["command_id"])
        self._validate_event_scope(message)
        wire_trade_id = _optional_attribution(str(fields["trade_id"]))
        position_id = str(fields["position_id"])
        context = self._latest_position_contexts.get(position_id)
        trade_id: str | None = None
        domain_command_id = _optional_command(command_id)
        if command_id != "BROKER":
            tracker = self._commands.get(command_id)
            terminal = self._terminal_commands.get(command_id)
            command = tracker.command if tracker is not None else terminal
            if command is None:
                raise GatewayProtocolError("broker event references unknown command_id")
            compatible_events = {
                CommandAction.OPEN: {"POSITION_OPENED", "POSITION_UPDATED"},
                CommandAction.MODIFY_SL: {"POSITION_UPDATED"},
                CommandAction.PARTIAL_CLOSE: {
                    "PARTIAL_FILLED",
                    "POSITION_CLOSED",
                },
                CommandAction.CLOSE: {"PARTIAL_FILLED", "POSITION_CLOSED"},
                CommandAction.CLOSE_ALL: {"PARTIAL_FILLED", "POSITION_CLOSED"},
            }.get(command.action, set())
            if event_type not in compatible_events:
                raise GatewayProtocolError(
                    "broker event type is incompatible with correlated command action"
                )
            if command.action is CommandAction.CLOSE_ALL:
                if context is not None:
                    if wire_trade_id not in {None, command.trade_id, context.trade_id}:
                        raise GatewayCacheError(
                            "CLOSE_ALL event conflicts with durable position attribution"
                        )
                    trade_id = context.trade_id
            else:
                trade_id = command.trade_id
            if command.action is not CommandAction.CLOSE_ALL and wire_trade_id != trade_id:
                raise GatewayProtocolError("broker event command/trade attribution conflict")
            if tracker is not None:
                tracker.observe_effect(fields, position_id)
                self._retire_tracker_if_complete(command_id, tracker)
        elif context is not None:
            if wire_trade_id is not None and wire_trade_id != context.trade_id:
                raise GatewayCacheError("broker event/wire trade attribution conflict")
            trade_id = context.trade_id
        domain_event = self._domain_broker_event(
            fields,
            command_id=domain_command_id,
            trade_id=trade_id,
        )
        domain_deal = self._domain_broker_deal(
            fields,
            command_id=domain_command_id,
            trade_id=trade_id,
        )
        self._emit(
            GatewaySignalType.BROKER_EVENT,
            command_id=None if command_id == "BROKER" else command_id,
            trade_id=trade_id,
            payload={
                **dict(fields),
                "wire_trade_id": wire_trade_id,
                "attribution_status": "VERIFIED" if trade_id is not None else "UNKNOWN",
                "event": domain_event,
                "deal": domain_deal,
            },
        )

    def _handle_bar(self, message: ProtocolMessage) -> None:
        fields = message.fields
        symbol = str(fields["symbol"])
        timeframe = str(fields["tf"])
        if symbol != self.config.expected.symbol or timeframe not in _TIMEFRAME_MS:
            raise GatewayProtocolError("BAR scope/timeframe is not canonical")
        open_time = int(fields["bar_time_ms"])
        close_time = open_time + _TIMEFRAME_MS[timeframe]
        if close_time > message.ts_ms:
            self._emit(
                GatewaySignalType.BAR_WITHHELD,
                reason="bar close_time exceeds authenticated snapshot timestamp",
                payload={"symbol": symbol, "tf": timeframe, "open_time_ms": open_time},
            )
            return
        bar = CompletedBar(
            symbol,
            timeframe,
            open_time,
            close_time,
            message.ts_ms,
            fields["o"],
            fields["h"],
            fields["l"],
            fields["c"],
            int(fields["volume"]),
        )
        if (
            bar.low <= 0
            or bar.high < bar.low
            or not bar.low <= bar.open <= bar.high
            or not bar.low <= bar.close <= bar.high
            or bar.volume < 0
        ):
            raise GatewayProtocolError("BAR OHLC/volume geometry is invalid")
        key = (symbol, timeframe, open_time)
        existing = self._closed_bars.get(key)
        if existing is not None and existing != bar:
            raise GatewayProtocolError("conflicting immutable completed BAR")
        if existing is None:
            timeframe_keys = tuple(
                item_key
                for item_key in self._closed_bars
                if item_key[0] == symbol and item_key[1] == timeframe
            )
            if len(timeframe_keys) >= self.config.max_completed_bars_per_timeframe:
                oldest = min(timeframe_keys, key=lambda item_key: item_key[2])
                self._closed_bars.pop(oldest)
        self._closed_bars[key] = bar

    def _handle_tick(self, message: ProtocolMessage) -> None:
        fields = message.fields
        if fields["symbol"] != self.config.expected.symbol:
            raise GatewayProtocolError("TICK outside pinned symbol")
        symbol = self._latest_symbol_snapshot
        if symbol is None:
            raise GatewayProtocolError("TICK arrived before a fenced symbol snapshot")
        bid = decimal_value(fields["bid"])
        ask = decimal_value(fields["ask"])
        spread = decimal_value(fields["spread_points"])
        broker_time = int(fields["broker_time_ms"])
        broker_age = message.ts_ms - broker_time
        if (
            bid <= 0
            or ask < bid
            or spread < 0
            or int(fields["volume"]) < 0
            or broker_age < -self.config.max_future_skew_ms
            or broker_age > self.config.max_message_age_ms
        ):
            raise GatewayProtocolError("TICK price/spread/time values are invalid")
        if bid % symbol.tick_size or ask % symbol.tick_size:
            raise GatewayProtocolError("TICK quote is not aligned to broker tick size")
        calculated_spread = (ask - bid) / symbol.point
        if abs(calculated_spread - spread) > Decimal("0.01"):
            raise GatewayProtocolError("TICK spread conflicts with bid/ask")
        if self._epoch is None:
            raise GatewayProtocolError(
                "TICK received without an active connection epoch"
            )
        tick = BrokerTick(
            msg_id=message.msg_id,
            connection_epoch=self._epoch,
            symbol=str(fields["symbol"]),
            received_at_ms=self.clock.now_ms(),
            message_ts_ms=message.ts_ms,
            broker_time_ms=broker_time,
            bid=bid,
            ask=ask,
            spread_points=spread,
            volume=int(fields["volume"]),
        )
        self._latest_tick = tick
        if self._tick_queue.full():
            self._tick_queue.get_nowait()
        self._tick_queue.put_nowait(tick)

    async def _try_finish_snapshot(self, assembly: _SnapshotAssembly) -> None:
        if not (
            assembly.account is not None
            and assembly.symbol is not None
            and assembly.positions_ended
            and assembly.orders_ended
            and assembly.replay_ended
        ):
            return
        snapshot = await self._build_snapshot(assembly)
        self._snapshots.pop(assembly.correlation_id, None)
        waiter = self._snapshot_waiters.get(assembly.correlation_id)
        if waiter is not None and not waiter.done():
            waiter.set_result(snapshot)
        else:
            self._completed_snapshots.append(snapshot)
        self._emit(
            GatewaySignalType.SNAPSHOT_READY,
            payload={"snapshot_id": snapshot.snapshot_id},
        )

    async def _build_snapshot(
        self, assembly: _SnapshotAssembly
    ) -> AuthoritativeBrokerSnapshot:
        if self._identity is None:
            raise GatewayProtocolError(
                "snapshot received before broker identity was established"
            )
        if assembly.account is None or assembly.symbol is None:
            raise GatewayProtocolError(
                "snapshot assembly is missing account or symbol state"
            )
        account = assembly.account
        position_records: list[OwnedPositionRecord] = []
        unresolved: list[str] = []
        observed_positions: dict[str, BrokerObservedPosition] = {}
        resolved_contexts: dict[str, DurablePositionContext] = {}
        for item in assembly.positions.values():
            observed = self._observed_position(item)
            observed_positions[observed.broker_position_id] = observed
            owned_scope = (
                observed.symbol == self.config.expected.symbol
                and observed.magic == self.config.expected.magic
            )
            if not owned_scope:
                position_records.append(
                    self._quarantined_position(observed, account.ts_ms)
                )
                continue
            if observed.trade_id is None:
                unresolved.append(observed.broker_position_id)
                position_records.append(
                    self._quarantined_position(observed, account.ts_ms)
                )
                continue
            if self._context_resolver is None:
                raise GatewayCacheError(
                    f"durable context resolver required for {observed.broker_position_id}"
                )
            context = self._context_resolver(observed)
            if inspect.isawaitable(context):
                context = await context
            if context is None:
                unresolved.append(observed.broker_position_id)
                position_records.append(
                    self._quarantined_position(observed, account.ts_ms)
                )
                continue
            self._validate_context(observed, context)
            resolved_contexts[observed.broker_position_id] = context
            position_records.append(
                OwnedPositionRecord(
                    account_id=context.account_id,
                    trade_id=context.trade_id,
                    broker_position_id=observed.broker_position_id,
                    position_ticket=observed.position_ticket,
                    broker_order_id=context.broker_order_id,
                    symbol=observed.symbol,
                    magic=observed.magic,
                    direction=observed.direction,
                    initial_volume=context.initial_volume,
                    remaining_volume=observed.remaining_volume,
                    entry_price=observed.entry_price,
                    original_sl=context.original_sl,
                    current_sl=observed.current_sl,
                    tp1=context.tp1,
                    tp2=context.tp2,
                    tp3=context.tp3,
                    initial_risk_amount=context.initial_risk_amount,
                    lifecycle_stage=context.lifecycle_stage,
                    position_version=observed.position_version,
                    opened_at_ms=observed.opened_at_ms,
                    updated_at_ms=account.ts_ms,
                    realised_pnl=context.realised_pnl,
                    pending_action=context.pending_action,
                    pending_milestone=context.pending_milestone,
                    pending_command_id=context.pending_command_id,
                    pending_close_volume=context.pending_close_volume,
                    pending_filled_volume=context.pending_filled_volume,
                )
            )
        orders = tuple(
            self._pending_order(item, account.ts_ms)
            for item in assembly.orders.values()
        )
        fields = account.fields
        risk = BrokerRiskSnapshot(
            risk_day=str(fields["risk_day"]),
            balance=fields["balance"],
            equity=fields["equity"],
            day_start_equity=fields["day_start_equity"],
            baseline_verified=bool(fields["baseline_verified"]),
            daily_halted=bool(fields["daily_halted"]),
        )
        snapshot_id = (
            f"{self._epoch}:{assembly.correlation_id}:"
            f"{assembly.position_snapshot_id}:{assembly.order_snapshot_id}"
        )
        snapshot = AuthoritativeBrokerSnapshot(
            snapshot_id=snapshot_id,
            identity=self._identity,
            risk=risk,
            positions=tuple(position_records),
            pending_orders=orders,
            captured_at_ms=max(
                account.ts_ms,
                assembly.symbol.ts_ms,
                *(item.ts_ms for item in assembly.positions.values()),
                *(item.ts_ms for item in assembly.orders.values()),
            ),
            payload={
                "generation_id": assembly.correlation_id,
                "position_snapshot_id": assembly.position_snapshot_id,
                "pending_order_snapshot_id": assembly.order_snapshot_id,
                "unresolved_position_ids": tuple(unresolved),
                "account": dict(account.fields),
                "symbol": dict(assembly.symbol.fields),
                "tick_value_currency": self.config.expected_currency,
                "tick_value_policy": "MAX_PROFIT_LOSS_CONSERVATIVE",
            },
        )
        self._latest_observed_positions = observed_positions
        self._latest_position_contexts = resolved_contexts
        return snapshot

    def _observed_position(self, message: ProtocolMessage) -> BrokerObservedPosition:
        fields = message.fields
        trade_id = _optional_attribution(str(fields["trade_id"]))
        return BrokerObservedPosition(
            broker_position_id=str(fields["position_id"]),
            position_ticket=str(fields["position_ticket"]),
            trade_id=trade_id,
            symbol=str(fields["symbol"]),
            magic=int(fields["magic"]),
            direction=str(fields["direction"]),
            remaining_volume=fields["volume"],
            entry_price=fields["price_open"],
            current_sl=fields["sl"],
            final_tp=fields["tp"],
            position_version=int(fields["position_version"]),
            opened_at_ms=int(fields["open_time_ms"]),
            comment=str(fields["comment"]),
        )

    def _validate_context(
        self, observed: BrokerObservedPosition, context: DurablePositionContext
    ) -> None:
        now = self.clock.now_ms()
        if (
            now - context.resolved_at_ms > self.config.max_context_age_ms
            or context.resolved_at_ms - now > self.config.max_future_skew_ms
        ):
            raise GatewayCacheError("durable position context is stale/future")
        expected = {
            "account_id": self.config.expected.account_id,
            "trade_id": observed.trade_id,
            "broker_position_id": observed.broker_position_id,
            "symbol": observed.symbol,
            "magic": observed.magic,
            "direction": observed.direction,
        }
        for name, value in expected.items():
            if getattr(context, name) != value:
                raise GatewayCacheError(f"durable position context mismatch: {name}")
        if observed.position_version < context.position_version:
            raise GatewayCacheError("broker position version regressed behind durable context")
        if context.initial_volume < observed.remaining_volume:
            raise GatewayCacheError("initial volume is below broker remaining volume")
        if context.tp3 != observed.final_tp:
            raise GatewayCacheError("durable final TP conflicts with broker final TP")
        if observed.current_sl <= 0 or observed.final_tp <= 0:
            raise GatewayCacheError("attributed live position is missing broker SL/TP")
        if context.lifecycle_stage in {
            LifecycleStage.CLOSED.value,
            LifecycleStage.QUARANTINED.value,
        }:
            raise GatewayCacheError("attributed live position lifecycle stage is invalid")
        if observed.direction == Direction.BUY.value:
            geometry = (
                context.original_sl
                < observed.entry_price
                < context.tp1
                <= context.tp2
                <= context.tp3
            )
        else:
            geometry = (
                context.original_sl
                > observed.entry_price
                > context.tp1
                >= context.tp2
                >= context.tp3
            )
        if not geometry:
            raise GatewayCacheError("durable position risk/target geometry is invalid")

    def _quarantined_position(
        self, observed: BrokerObservedPosition, updated_at_ms: int
    ) -> OwnedPositionRecord:
        return OwnedPositionRecord(
            account_id=self.config.expected.account_id,
            trade_id=None,
            broker_position_id=observed.broker_position_id,
            position_ticket=observed.position_ticket,
            broker_order_id=None,
            symbol=observed.symbol,
            magic=observed.magic,
            direction=observed.direction,
            initial_volume=observed.remaining_volume,
            remaining_volume=observed.remaining_volume,
            entry_price=observed.entry_price,
            original_sl=Decimal("0"),
            current_sl=observed.current_sl,
            tp1=Decimal("0"),
            tp2=Decimal("0"),
            tp3=observed.final_tp,
            initial_risk_amount=Decimal("0"),
            lifecycle_stage="QUARANTINED",
            position_version=observed.position_version,
            opened_at_ms=observed.opened_at_ms,
            updated_at_ms=updated_at_ms,
        )

    def _pending_order(
        self, message: ProtocolMessage, updated_at_ms: int
    ) -> PendingOrderRecord:
        fields = message.fields
        sl = decimal_value(fields["sl"])
        tp = decimal_value(fields["tp"])
        return PendingOrderRecord(
            account_id=self.config.expected.account_id,
            broker_order_id=str(fields["order_id"]),
            symbol=str(fields["symbol"]),
            magic=int(fields["magic"]),
            order_type=str(fields["order_type"]),
            volume=fields["volume_current"],
            price=fields["price_open"],
            sl=None if sl == 0 else sl,
            tp=None if tp == 0 else tp,
            state=str(fields["order_state"]),
            created_at_ms=int(fields["order_time_ms"]),
            updated_at_ms=updated_at_ms,
            trade_id=_optional_attribution(str(fields["trade_id"])),
            command_id=_optional_command(str(fields["command_id"])),
            payload={
                "volume_initial": str(fields["volume_initial"]),
                "expiration_time_ms": int(fields["expiration_time_ms"]),
                "comment": str(fields["comment"]),
            },
        )

    async def _deal_record(self, message: ProtocolMessage) -> BrokerDealRecord:
        fields = message.fields
        if (
            fields["symbol"] != self.config.expected.symbol
            or int(fields["magic"]) != self.config.expected.magic
        ):
            raise GatewayProtocolError("deal replay item outside pinned scope")
        wire_trade_id = _optional_attribution(str(fields["trade_id"]))
        wire_command_id = _optional_command(str(fields["command_id"]))
        observed = BrokerObservedDeal(
            deal_id=str(fields["deal_id"]),
            position_id=str(fields["position_id"]),
            order_id=None if fields["order_id"] == "0" else str(fields["order_id"]),
            wire_trade_id=wire_trade_id,
            wire_command_id=wire_command_id,
            symbol=str(fields["symbol"]),
            magic=int(fields["magic"]),
            direction=str(fields["direction"]),
            entry_type=str(fields["entry_type"]),
            broker_time_ms=int(fields["broker_time_ms"]),
        )
        attribution: DurableDealAttribution | None = None
        position_context = self._latest_position_contexts.get(observed.position_id)
        if position_context is not None:
            attribution = DurableDealAttribution(
                resolved_at_ms=self.clock.now_ms(),
                account_id=position_context.account_id,
                deal_id=observed.deal_id,
                position_id=observed.position_id,
                trade_id=position_context.trade_id,
                command_id=wire_command_id,
            )
        elif self._deal_attribution_resolver is not None:
            try:
                resolved = self._deal_attribution_resolver(observed)
                attribution = await resolved if inspect.isawaitable(resolved) else resolved
            except Exception as exc:
                raise GatewayCacheError(
                    f"durable deal attribution resolver failed: {type(exc).__name__}"
                ) from exc
        if attribution is not None:
            self._validate_deal_attribution(observed, attribution)
        trade_id = None if attribution is None else attribution.trade_id
        command_id = wire_command_id
        if attribution is not None and attribution.command_id is not None:
            command_id = attribution.command_id
        remaining = (
            str(fields["remaining_volume"])
            if bool(fields["remaining_volume_known"])
            else None
        )
        return BrokerDealRecord(
            account_id=self.config.expected.account_id,
            deal_id=observed.deal_id,
            position_id=observed.position_id,
            order_id=observed.order_id,
            # Canonical accounting classifies by broker entry type.  Direction
            # remains separate provenance and must never alter OUT recognition.
            deal_type=str(fields["entry_type"]),
            volume=decimal_value(fields["volume"]),
            price=decimal_value(fields["price"]),
            profit=decimal_value(fields["profit"]),
            commission=decimal_value(fields["commission"]),
            swap=decimal_value(fields["swap"]),
            fee=decimal_value(fields["fee"]),
            occurred_at=int(fields["broker_time_ms"]) / 1_000,
            trade_id=trade_id,
            command_id=command_id,
            payload={
                "command_id": command_id,
                "wire_trade_id": wire_trade_id,
                "direction": str(fields["direction"]),
                "wire_command_id": wire_command_id,
                "attribution_status": "VERIFIED" if attribution is not None else "UNKNOWN",
                "cursor": str(fields["cursor"]),
                "entry_type": str(fields["entry_type"]),
                "remaining_volume": remaining,
                "remaining_volume_known": bool(fields["remaining_volume_known"]),
                "reason": str(fields["reason"]),
                "position_version": int(fields["position_version"]),
                "comment": str(fields["comment"]),
            },
        )

    def _validate_deal_attribution(
        self,
        observed: BrokerObservedDeal,
        attribution: DurableDealAttribution,
    ) -> None:
        now = self.clock.now_ms()
        if (
            now - attribution.resolved_at_ms > self.config.max_context_age_ms
            or attribution.resolved_at_ms - now > self.config.max_future_skew_ms
        ):
            raise GatewayCacheError("durable deal attribution is stale/future")
        expected = {
            "account_id": self.config.expected.account_id,
            "deal_id": observed.deal_id,
            "position_id": observed.position_id,
        }
        for name, value in expected.items():
            if getattr(attribution, name) != value:
                raise GatewayCacheError(f"durable deal attribution mismatch: {name}")
        if observed.wire_trade_id is not None and attribution.trade_id != observed.wire_trade_id:
            raise GatewayCacheError("durable deal/wire trade attribution conflict")
        if (
            observed.wire_command_id is not None
            and attribution.command_id != observed.wire_command_id
        ):
            raise GatewayCacheError("durable deal/wire command attribution conflict")

    def _validate_event_scope(self, message: ProtocolMessage) -> None:
        fields = message.fields
        if (
            fields["symbol"] != self.config.expected.symbol
            or int(fields["magic"]) != self.config.expected.magic
            or fields["direction"] not in {item.value for item in Direction}
        ):
            raise GatewayProtocolError("broker event is outside pinned scope/direction")
        event_type = str(fields["event_type"])
        if event_type == "POSITION_REVERSED":
            raise GatewayProtocolError(
                "broker position reversal is unsupported; reconciliation required"
            )
        from .domain_models import BrokerEventType

        try:
            BrokerEventType(event_type)
        except ValueError as exc:
            raise GatewayProtocolError(f"unknown broker event type {event_type}") from exc
        broker_age = message.ts_ms - int(fields["broker_time_ms"])
        if (
            broker_age < -self.config.max_future_skew_ms
            or broker_age > self.config.max_message_age_ms
            or decimal_value(fields["price"]) < 0
            or decimal_value(fields["remaining_volume"]) < 0
            or decimal_value(fields["sl"]) < 0
            or decimal_value(fields["tp"]) < 0
        ):
            raise GatewayProtocolError("broker event price/volume/time values are invalid")

    def _domain_broker_event(
        self,
        fields: Mapping[str, Any],
        *,
        command_id: str | None,
        trade_id: str | None,
    ) -> Any:
        from .domain_models import BrokerEvent, BrokerEventType

        event_type = BrokerEventType(str(fields["event_type"]))
        delta = abs(decimal_value(fields["volume_delta"]))
        return BrokerEvent(
            event_id=str(fields["event_id"]),
            event_type=event_type,
            broker_time_ms=int(fields["broker_time_ms"]),
            received_at_ms=self.clock.now_ms(),
            command_id=command_id,
            trade_id=trade_id,
            broker_position_id=str(fields["position_id"]),
            position_ticket=str(fields["position_ticket"]),
            order_id=str(fields["order_id"]),
            deal_id=str(fields["deal_id"]),
            fill_price=fields["price"],
            fill_volume=None if delta == 0 else delta,
            remaining_volume=fields["remaining_volume"],
            sl=fields["sl"],
            tp=fields["tp"],
            profit=fields["profit"],
            commission=fields["commission"],
            swap=fields["swap"],
            fee=fields["fee"],
            reason=str(fields["reason"]),
            position_version=int(fields["position_version"]),
        )

    def _domain_broker_deal(
        self,
        fields: Mapping[str, Any],
        *,
        command_id: str | None,
        trade_id: str | None,
    ) -> BrokerDealRecord | None:
        event_type = str(fields["event_type"])
        if event_type not in {"POSITION_OPENED", "PARTIAL_FILLED", "POSITION_CLOSED"}:
            return None
        deal_id = str(fields["deal_id"])
        order_id = str(fields["order_id"])
        volume = abs(decimal_value(fields["volume_delta"]))
        if deal_id in {"", "0"}:
            raise GatewayProtocolError("deal-bearing broker event lacks deal identity")
        if order_id in {"", "0"} or volume <= 0:
            raise GatewayProtocolError(
                "deal-bearing broker event lacks order identity or fill volume"
            )
        return BrokerDealRecord(
            account_id=self.config.expected.account_id,
            deal_id=deal_id,
            position_id=str(fields["position_id"]),
            order_id=order_id,
            deal_type="IN" if event_type == "POSITION_OPENED" else "OUT",
            volume=volume,
            price=decimal_value(fields["price"]),
            profit=decimal_value(fields["profit"]),
            commission=decimal_value(fields["commission"]),
            swap=decimal_value(fields["swap"]),
            fee=decimal_value(fields["fee"]),
            occurred_at=int(fields["broker_time_ms"]) / 1_000,
            trade_id=trade_id,
            command_id=command_id,
            payload={
                "direction": str(fields["direction"]),
                "entry_type": "IN" if event_type == "POSITION_OPENED" else "OUT",
                "reason": str(fields["reason"]),
                "position_version": int(fields["position_version"]),
                "source": "BROKER_EVENT",
            },
        )

    async def _request_snapshot(
        self, cursor: str, *, correlation: str | None = None
    ) -> str:
        correlation = correlation or self._new_id("RECON")
        self._internal_commands.add(correlation)
        self._ensure_snapshot_generation(correlation, requested_cursor=cursor)
        await self._send_protocol(
            MessageType.COMMAND,
            correlation,
            fields={
                "command_id": correlation,
                "trade_id": "RECONCILIATION",
                "action": "REQUEST_SNAPSHOT",
                "expires_at_ms": self.clock.now_ms() + self.config.snapshot_timeout_ms,
                "deal_cursor": cursor,
            },
            msg_id=correlation,
        )
        return correlation

    async def _send_protocol(
        self,
        message_type: MessageType,
        correlation_id: str,
        *,
        fields: Mapping[str, Any],
        msg_id: str | None = None,
    ) -> ProtocolMessage:
        writer = self._writer
        if writer is None:
            raise GatewayDisconnectedError("EA socket is not connected")
        async with self._write_lock:
            if self._tx_sequence >= self.config.max_sequence:
                raise GatewayProtocolError("outbound sequence exhausted")
            self._tx_sequence += 1
            message = self._codec.build(
                message_type,
                msg_id=msg_id or self._new_id(message_type.value),
                epoch=self._epoch or "HANDSHAKE",
                seq=self._tx_sequence,
                correlation_id=correlation_id,
                ts_ms=self.clock.now_ms(),
                **dict(fields),
            )
            frame = self._codec.encode(message)
            writer.write(frame)
            await writer.drain()
            return message

    def _command_fields(self, command: OutboundCommand) -> dict[str, Any]:
        self._validate_outbound_command(command)
        payload = dict(command.payload)
        action = command.action.value
        fields: dict[str, Any] = {
            "command_id": command.command_id,
            "trade_id": command.trade_id,
            "action": action,
            "expires_at_ms": command.expires_at_ms,
        }
        if command.action is CommandAction.OPEN:
            fields.update(
                {
                    "reservation_id": command.reservation_id,
                    "symbol": _required(payload, "symbol"),
                    "magic": _required(payload, "magic"),
                    "direction": _required(payload, "direction"),
                    "volume": _required(payload, "volume"),
                    "reference_price": _required_alias(payload, "reference_price", "entry"),
                    "max_slippage_points": _required(payload, "max_slippage_points"),
                    "sl": _required(payload, "sl"),
                    "tp1": _required(payload, "tp1"),
                    "tp2": _required(payload, "tp2"),
                    "tp3": _required(payload, "tp3"),
                    "risk_limit": _required_alias(payload, "risk_limit", "risk_amount"),
                    "risk_pct_limit": _required_alias(
                        payload, "risk_pct_limit", "requested_risk_pct"
                    ),
                    "comment": f"QSX|{command.trade_id}"[:31],
                }
            )
        elif command.action is CommandAction.MODIFY_SL:
            observed, context = self._lifecycle_wire_context(command)
            fields.update(
                {
                    "position_id": observed.broker_position_id,
                    "position_ticket": observed.position_ticket,
                    "expected_position_version": command.expected_position_version,
                    "new_sl": decimal_value(
                        _required_alias(command.expected_effect, "target_sl", "new_sl")
                    ),
                    "final_tp": context.tp3,
                }
            )
        elif command.action is CommandAction.PARTIAL_CLOSE:
            observed, _context = self._lifecycle_wire_context(command)
            fields.update(
                {
                    "position_id": observed.broker_position_id,
                    "position_ticket": observed.position_ticket,
                    "expected_position_version": command.expected_position_version,
                    "close_volume": decimal_value(
                        _required_alias(
                            command.expected_effect,
                            "target_close_volume",
                            "close_volume",
                        )
                    ),
                }
            )
        elif command.action is CommandAction.CLOSE:
            observed, _context = self._lifecycle_wire_context(command)
            fields.update(
                {
                    "position_id": observed.broker_position_id,
                    "position_ticket": observed.position_ticket,
                    "expected_position_version": command.expected_position_version,
                }
            )
        elif command.action is CommandAction.CLOSE_ALL:
            fields.update(
                {
                    "symbol": self.config.expected.symbol,
                    "magic": self.config.expected.magic,
                }
            )
        elif command.action is CommandAction.REQUEST_SNAPSHOT:
            fields["deal_cursor"] = _require_cursor(
                str(_required(payload, "deal_cursor"))
            )
        else:  # pragma: no cover - enum exhaustiveness
            raise GatewayError(f"unsupported action {action}")
        return fields

    def _validate_outbound_command(self, command: OutboundCommand) -> None:
        if not isinstance(command, OutboundCommand):
            raise TypeError("command must be OutboundCommand")
        if not isinstance(command.action, CommandAction):
            raise GatewayError("outbound command action is not canonical")
        for name in ("command_id", "trade_id", "reservation_id", "connection_epoch"):
            if not isinstance(getattr(command, name), str) or not getattr(command, name):
                raise GatewayError(f"outbound {name} must be non-empty")
        now = self.clock.now_ms()
        if (
            command.sequence < 1
            or command.created_at_ms < 0
            or command.expires_at_ms <= command.created_at_ms
            or command.created_at_ms - now > self.config.max_future_skew_ms
            or command.expires_at_ms <= now
        ):
            raise GatewayError("outbound command sequence/time envelope is invalid or expired")
        payload = command.payload
        if command.action is CommandAction.OPEN:
            exact = {
                "action": command.action.value,
                "trade_id": command.trade_id,
                "reservation_id": command.reservation_id,
                "account_id": self.config.expected.account_id,
                "currency": self.config.expected_currency,
                "symbol": self.config.expected.symbol,
                "magic": self.config.expected.magic,
                "config_hash": self.config.config_hash,
            }
            for name, expected in exact.items():
                if _required(payload, name) != expected:
                    raise GatewayError(f"OPEN payload conflicts with pinned {name}")
            direction = str(_required(payload, "direction"))
            entry = decimal_value(_required(payload, "entry"))
            sl = decimal_value(_required(payload, "sl"))
            tp1 = decimal_value(_required(payload, "tp1"))
            tp2 = decimal_value(_required(payload, "tp2"))
            tp3 = decimal_value(_required(payload, "tp3"))
            volume = decimal_value(_required(payload, "volume"))
            risk = decimal_value(_required(payload, "risk_amount"))
            risk_pct = decimal_value(_required(payload, "requested_risk_pct"))
            max_total_risk = decimal_value(_required(payload, "max_total_risk"))
            max_positions = _required(payload, "max_open_positions")
            slippage = _required(payload, "max_slippage_points")
            geometry = (
                sl < entry < tp1 <= tp2 <= tp3
                if direction == Direction.BUY.value
                else sl > entry > tp1 >= tp2 >= tp3
                if direction == Direction.SELL.value
                else False
            )
            if (
                not geometry
                or volume <= 0
                or risk <= 0
                or not Decimal("0") < risk_pct <= Decimal("1")
                or max_total_risk < risk
                or isinstance(max_positions, bool)
                or not isinstance(max_positions, int)
                or not 1 <= max_positions <= 3
                or isinstance(slippage, bool)
                or not isinstance(slippage, int)
                or slippage < 0
            ):
                raise GatewayError("OPEN payload risk/geometry controls are invalid")
        elif command.action in {
            CommandAction.MODIFY_SL,
            CommandAction.PARTIAL_CLOSE,
            CommandAction.CLOSE,
        }:
            if (
                command.position_id is None
                or command.expected_position_version is None
                or command.expected_position_version < 1
            ):
                raise GatewayError("lifecycle command must pin position identity/version")

    def _lifecycle_wire_context(
        self, command: OutboundCommand
    ) -> tuple[BrokerObservedPosition, DurablePositionContext]:
        if command.position_id is None:
            raise GatewayError(
                "lifecycle command is missing its position identifier"
            )
        observed = self._latest_observed_positions.get(command.position_id)
        context = self._latest_position_contexts.get(command.position_id)
        if observed is None or context is None:
            raise GatewaySnapshotError(
                "lifecycle command lacks a fenced attributed broker position"
            )
        if (
            observed.trade_id != command.trade_id
            or context.trade_id != command.trade_id
            or observed.position_version != command.expected_position_version
            or context.position_version != command.expected_position_version
        ):
            raise GatewaySnapshotError(
                "lifecycle command conflicts with fenced position attribution/version"
            )
        return observed, context

    def _correlated_tracker(self, fields: Mapping[str, Any]) -> _CommandTracker:
        command_id = str(fields["command_id"])
        tracker = self._commands.get(command_id)
        if tracker is None:
            raise GatewayProtocolError("ACK/NACK references unknown command_id")
        if (
            fields["trade_id"] != tracker.command.trade_id
            or fields["action"] != tracker.command.action.value
        ):
            raise GatewayProtocolError("ACK/NACK command attribution conflict")
        return tracker

    def _retire_tracker_if_complete(
        self, command_id: str, tracker: _CommandTracker
    ) -> None:
        if tracker.rejected or (tracker.ack_seen and tracker.effect_complete):
            self._commands.pop(command_id, None)
            if (
                command_id not in self._terminal_commands
                and len(self._terminal_commands) >= self.config.dedupe_limit
            ):
                self._terminal_commands.pop(next(iter(self._terminal_commands)))
            self._terminal_commands[command_id] = tracker.command

    def _generation(self, correlation_id: str) -> _SnapshotAssembly:
        assembly = self._snapshots.get(correlation_id)
        if assembly is None:
            raise GatewayProtocolError("snapshot correlation/generation is unknown")
        return assembly

    def _ensure_snapshot_generation(
        self, correlation_id: str, *, requested_cursor: str
    ) -> _SnapshotAssembly:
        if not correlation_id:
            raise GatewayProtocolError("snapshot generation correlation is empty")
        assembly = self._snapshots.get(correlation_id)
        if assembly is not None:
            if assembly.requested_cursor != requested_cursor:
                raise GatewayProtocolError("snapshot generation cursor conflict")
            return assembly
        now = self.clock.now_ms()
        assembly = _SnapshotAssembly(
            correlation_id,
            now,
            now + self.config.snapshot_timeout_ms,
            _require_cursor(requested_cursor),
        )
        self._snapshots[correlation_id] = assembly
        return assembly

    def _validate_account_scope(self, fields: Mapping[str, Any]) -> None:
        expected = self.config.expected
        checks = {
            "account_id": expected.account_id,
            "broker_server": expected.broker_server,
            "account_trade_mode": self.config.expected_trade_mode.value,
        }
        for name, value in checks.items():
            if fields[name] != value:
                raise GatewayProtocolError(f"account snapshot identity mismatch: {name}")
        if self.config.execution_enabled and (
            not bool(fields["trade_allowed"])
            or not bool(fields["expert_allowed"])
        ):
            raise GatewayProtocolError("account snapshot reports trading disabled")

    def _require_expected(self, expected: ExpectedBrokerIdentity) -> None:
        if expected != self.config.expected:
            raise GatewaySnapshotError("reconciliation requested an unpinned identity")

    def _require_authenticated(self) -> None:
        if self._state is not GatewayState.AUTHENTICATED or self._writer is None:
            raise GatewayDisconnectedError("no authenticated EA session")

    def _committed_cursor(self, account_id: str) -> str:
        value = self._cursor_provider(account_id)
        return _normalise_cursor(value)

    def _reset_session_state(self) -> None:
        self._identity = None
        self._epoch = None
        self._rx_sequence = 0
        self._tx_sequence = 0
        self._message_counter = 0
        self._seen_message_ids.clear()
        self._seen_event_ids.clear()
        self._seen_ack_ids.clear()
        self._terminal_commands.clear()
        self._internal_commands.clear()
        self._snapshots.clear()
        self._completed_snapshots.clear()
        self._replays.clear()
        self._deal_pages.clear()
        self._last_heartbeat_received_ms = None
        self._heartbeat_seen = False
        self._latest_account_health = None
        self._latest_symbol_snapshot = None
        self._latest_tick = None
        while not self._tick_queue.empty():
            self._tick_queue.get_nowait()
        self._closed_bars.clear()
        self._latest_observed_positions.clear()
        self._latest_position_contexts.clear()

    def _snapshot_is_fresh(self, snapshot: AuthoritativeBrokerSnapshot) -> bool:
        age = self.clock.now_ms() - snapshot.captured_at_ms
        return -self.config.max_future_skew_ms <= age <= self.config.max_message_age_ms

    async def _disconnect(
        self,
        reason: str,
        *,
        protocol: bool = False,
        cache: bool = False,
        unknown: bool = False,
        token: int | None = None,
    ) -> None:
        if token is not None and token != self._connection_token:
            return
        writer = self._writer
        if writer is None and self._state in {
            GatewayState.DISCONNECTED,
            GatewayState.CLOSED,
        }:
            return
        self._last_error = reason
        # Invalidate execution capability before emitting signals or awaiting
        # socket shutdown, closing the READY-after-transport-loss race.
        if self._runtime is not None:
            self._runtime.disconnect(reason)
        if protocol:
            self._emit(GatewaySignalType.PROTOCOL_VIOLATION, reason=reason)
        if cache:
            self._emit(GatewaySignalType.CACHE_FAILURE, reason=reason)
        if unknown:
            for command_id, tracker in tuple(self._commands.items()):
                if tracker.rejected:
                    continue
                self._emit(
                    GatewaySignalType.UNKNOWN_COMMAND,
                    command_id=command_id,
                    trade_id=tracker.command.trade_id,
                    reason=reason,
                    payload={
                        "action": tracker.command.action.value,
                        "ack_seen": tracker.ack_seen,
                        "effect_seen": tracker.effect_seen,
                    },
                )
            self._commands.clear()
        self._emit(GatewaySignalType.DISCONNECTED, reason=reason)
        self._fail_waiters(GatewayDisconnectedError(reason))
        self._snapshots.clear()
        self._completed_snapshots.clear()
        self._replays.clear()
        self._deal_pages.clear()
        self._internal_commands.clear()
        self._writer = None
        self._reader = None
        self._identity = None
        self._epoch = None
        self._latest_account_health = None
        self._latest_symbol_snapshot = None
        self._latest_tick = None
        while not self._tick_queue.empty():
            self._tick_queue.get_nowait()
        self._closed_bars.clear()
        self._latest_observed_positions.clear()
        self._latest_position_contexts.clear()
        self._state = GatewayState.DISCONNECTED
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    def _fail_waiters(self, error: Exception) -> None:
        for future in (*self._snapshot_waiters.values(), *self._deal_waiters.values()):
            if not future.done():
                future.set_exception(error)
        self._snapshot_waiters.clear()
        self._deal_waiters.clear()

    async def _watchdog(self) -> None:
        period = max(
            0.05,
            min(
                self.config.heartbeat_timeout_ms,
                self.config.handshake_timeout_ms,
                self.config.snapshot_timeout_ms,
            )
            / 4_000,
        )
        while True:
            await asyncio.sleep(period)
            await self.check_health()

    def _new_id(self, prefix: str) -> str:
        self._message_counter += 1
        return f"{prefix}-{self._epoch or 'preauth'}-{self._message_counter}"

    def _emit(
        self,
        signal_type: GatewaySignalType,
        *,
        command_id: str | None = None,
        trade_id: str | None = None,
        reason: str = "",
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        self._signals.put_nowait(
            GatewaySignal(
                signal_type,
                self.clock.now_ms(),
                self._epoch,
                command_id,
                trade_id,
                reason,
                payload or {},
            )
        )

    def _remember(self, cache: dict[str, str], key: str, fingerprint: str) -> None:
        if key not in cache and len(cache) >= self.config.dedupe_limit:
            raise GatewayCacheError("bounded gateway idempotency cache exhausted")
        cache[key] = fingerprint

    def _remember_semantic(
        self, cache: dict[str, str], key: str, value: Mapping[str, Any]
    ) -> bool:
        fingerprint = _fingerprint(value)
        existing = cache.get(key)
        if existing is not None:
            if existing != fingerprint:
                raise GatewayProtocolError(f"conflicting duplicate semantic identity {key}")
            return False
        self._remember(cache, key, fingerprint)
        return True

    @staticmethod
    def _set_once(
        assembly: _SnapshotAssembly, name: str, message: ProtocolMessage
    ) -> None:
        existing = getattr(assembly, name)
        if existing is not None:
            if _fingerprint(existing.fields) != _fingerprint(message.fields):
                raise GatewayProtocolError(f"conflicting duplicate snapshot {name}")
            raise GatewayProtocolError(f"duplicate snapshot {name}")
        setattr(assembly, name, message)

    @staticmethod
    def _insert_semantic(
        cache: dict[str, ProtocolMessage], key: str, message: ProtocolMessage
    ) -> None:
        existing = cache.get(key)
        if existing is not None:
            if _fingerprint(existing.fields) != _fingerprint(message.fields):
                raise GatewayProtocolError(f"conflicting duplicate snapshot item {key}")
            return
        cache[key] = message

    def _validate_item_count(self, count: int) -> None:
        if not 0 <= count <= self.config.max_snapshot_items:
            raise GatewayProtocolError("snapshot/replay item count exceeds bound")


def _is_loopback_host(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _peer_is_loopback(peer: Any) -> bool:
    if not isinstance(peer, tuple) or not peer:
        return False
    try:
        return ipaddress.ip_address(peer[0]).is_loopback
    except ValueError:
        return False


def _fingerprint(value: Any) -> str:
    encoded = json.dumps(
        _jsonable(value), sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, deque)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, StrEnum):
        return value.value
    return value


def _required(mapping: Mapping[str, Any], name: str) -> Any:
    if name not in mapping:
        raise GatewayError(f"outbound command payload missing {name}")
    return mapping[name]


def _required_alias(mapping: Mapping[str, Any], first: str, second: str) -> Any:
    if first in mapping:
        return mapping[first]
    return _required(mapping, second)


def _optional_attribution(value: str) -> str | None:
    if value in {"", "0", "UNATTRIBUTED"} or value.startswith("UNATTRIBUTED-"):
        return None
    return value


def _optional_command(value: str) -> str | None:
    return None if value in {"", "0", "BROKER"} else value


def _cursor_parts(cursor: str) -> tuple[int, int]:
    left, separator, right = cursor.partition(":")
    if separator != ":" or not left.isdecimal() or not right.isdecimal():
        raise GatewaySnapshotError("deal cursor must be '<time_ms>:<deal_id>'")
    return int(left), int(right)


def _require_cursor(cursor: str) -> str:
    _cursor_parts(cursor)
    return cursor


def _normalise_cursor(cursor: str | None) -> str:
    return "0:0" if cursor is None else _require_cursor(cursor)


__all__ = [
    "BrokerGateway",
    "BrokerObservedPosition",
    "CompletedBar",
    "DurablePositionContext",
    "GatewayCacheError",
    "GatewayConfig",
    "GatewayDisconnectedError",
    "GatewayError",
    "GatewayProtocolError",
    "GatewaySignal",
    "GatewaySignalType",
    "GatewaySnapshotError",
    "GatewayState",
    "PositionContextResolver",
]
