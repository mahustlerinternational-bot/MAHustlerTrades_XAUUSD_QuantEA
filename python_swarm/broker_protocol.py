"""QUANT-SWARM broker protocol v4: authenticated, flat, compact NDJSON.

The wire format is intentionally flat.  MQL5 can parse a bounded object of
scalar fields safely without embedding a general-purpose JSON implementation.
Every broker identifier is transported as a string to avoid precision loss in
JavaScript and downstream event consumers.
"""

from __future__ import annotations

import hmac
import json
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType
from typing import Mapping

try:
    from .domain_models import Clock, SystemClock
except ImportError:  # pragma: no cover - direct legacy import compatibility
    from domain_models import Clock, SystemClock  # type: ignore


try:
    from .version import BROKER_PROTOCOL_VERSION
except ImportError:  # pragma: no cover - direct legacy import compatibility
    from version import BROKER_PROTOCOL_VERSION  # type: ignore


PROTOCOL_VERSION = BROKER_PROTOCOL_VERSION
DEFAULT_MAX_FRAME_BYTES = 16_384


class MessageType(StrEnum):
    HELLO = "HELLO"
    HELLO_ACK = "HELLO_ACK"
    ACCOUNT_SNAPSHOT = "ACCOUNT_SNAPSHOT"
    SYMBOL_SNAPSHOT = "SYMBOL_SNAPSHOT"
    POSITION_SNAPSHOT_BEGIN = "POSITION_SNAPSHOT_BEGIN"
    POSITION_SNAPSHOT_ITEM = "POSITION_SNAPSHOT_ITEM"
    POSITION_SNAPSHOT_END = "POSITION_SNAPSHOT_END"
    PENDING_ORDER_SNAPSHOT_BEGIN = "PENDING_ORDER_SNAPSHOT_BEGIN"
    PENDING_ORDER_SNAPSHOT_ITEM = "PENDING_ORDER_SNAPSHOT_ITEM"
    PENDING_ORDER_SNAPSHOT_END = "PENDING_ORDER_SNAPSHOT_END"
    DEAL_REPLAY_BEGIN = "DEAL_REPLAY_BEGIN"
    DEAL_REPLAY_ITEM = "DEAL_REPLAY_ITEM"
    DEAL_REPLAY_END = "DEAL_REPLAY_END"
    HEARTBEAT = "HEARTBEAT"
    TICK = "TICK"
    BAR = "BAR"
    COMMAND = "COMMAND"
    COMMAND_ACK = "COMMAND_ACK"
    COMMAND_NACK = "COMMAND_NACK"
    BROKER_EVENT = "BROKER_EVENT"


class CommandAction(StrEnum):
    OPEN = "OPEN"
    MODIFY_SL = "MODIFY_SL"
    PARTIAL_CLOSE = "PARTIAL_CLOSE"
    CLOSE = "CLOSE"
    CLOSE_ALL = "CLOSE_ALL"
    REQUEST_SNAPSHOT = "REQUEST_SNAPSHOT"


class ProtocolErrorCode(StrEnum):
    FRAME_TOO_LARGE = "FRAME_TOO_LARGE"
    INVALID_NDJSON = "INVALID_NDJSON"
    INVALID_JSON = "INVALID_JSON"
    DUPLICATE_FIELD = "DUPLICATE_FIELD"
    NESTED_VALUE = "NESTED_VALUE"
    UNSUPPORTED_VERSION = "UNSUPPORTED_VERSION"
    AUTH_FAILED = "AUTH_FAILED"
    EPOCH_MISMATCH = "EPOCH_MISMATCH"
    INVALID_SEQUENCE = "INVALID_SEQUENCE"
    MISSING_FIELD = "MISSING_FIELD"
    UNKNOWN_FIELD = "UNKNOWN_FIELD"
    INVALID_FIELD_TYPE = "INVALID_FIELD_TYPE"
    UNKNOWN_MESSAGE_TYPE = "UNKNOWN_MESSAGE_TYPE"
    INVALID_COMMAND = "INVALID_COMMAND"
    SESSION_NOT_AUTHENTICATED = "SESSION_NOT_AUTHENTICATED"


class ProtocolError(ValueError):
    def __init__(self, code: ProtocolErrorCode, detail: str) -> None:
        super().__init__(f"{code.value}: {detail}")
        self.code = code
        self.detail = detail


@dataclass(frozen=True, slots=True)
class _Schema:
    required: frozenset[str]
    optional: frozenset[str] = frozenset()
    string_fields: frozenset[str] = frozenset()


COMMON_FIELDS = frozenset(
    {"v", "type", "msg_id", "auth", "epoch", "seq", "correlation_id", "ts_ms"}
)
COMMON_STRING_FIELDS = frozenset(
    {"v", "type", "msg_id", "auth", "epoch", "correlation_id"}
)


BOOLEAN_FIELDS: Mapping[MessageType, frozenset[str]]
INTEGER_FIELDS: Mapping[MessageType, frozenset[str]]


def _ids(*names: str) -> frozenset[str]:
    return frozenset(names)


SCHEMAS: Mapping[MessageType, _Schema] = MappingProxyType(
    {
        MessageType.HELLO: _Schema(
            frozenset(
                {
                    "role", "ea_version", "account_id", "broker_server",
                    "account_trade_mode", "trade_allowed", "expert_allowed",
                    "symbol", "magic", "config_hash",
                }
            ),
            frozenset({"terminal_build"}),
            _ids(
                "role", "ea_version", "account_id", "broker_server",
                "account_trade_mode", "symbol", "config_hash",
            ),
        ),
        MessageType.HELLO_ACK: _Schema(
            frozenset(
                {
                    "status", "server_id", "config_hash", "expected_account_id",
                    "expected_broker_server", "expected_account_trade_mode",
                    "expected_symbol", "expected_magic", "allow_real", "deal_cursor",
                }
            ),
            frozenset({"reason"}),
            _ids(
                "status", "server_id", "config_hash", "expected_account_id",
                "expected_broker_server", "expected_account_trade_mode",
                "expected_symbol", "deal_cursor", "reason",
            ),
        ),
        MessageType.ACCOUNT_SNAPSHOT: _Schema(
            frozenset(
                {
                    "account_id", "broker_server", "account_trade_mode", "currency",
                    "account_mode", "trade_allowed", "expert_allowed", "balance",
                    "equity", "free_margin", "margin", "day_start_equity",
                    "risk_day", "baseline_verified", "daily_halted", "server_time_ms",
                }
            ),
            string_fields=_ids(
                "account_id", "broker_server", "account_trade_mode", "currency",
                "account_mode", "risk_day",
            ),
        ),
        MessageType.SYMBOL_SNAPSHOT: _Schema(
            frozenset(
                {
                    "symbol", "digits", "tick_size", "tick_value_profit",
                    "tick_value_loss", "contract_size", "volume_min", "volume_step",
                    "volume_max", "stops_level", "freeze_level", "trade_mode",
                    "filling_mode",
                }
            ),
            string_fields=_ids("symbol", "trade_mode", "filling_mode"),
        ),
        MessageType.POSITION_SNAPSHOT_BEGIN: _Schema(
            frozenset({"snapshot_id", "count", "deal_cursor"}),
            string_fields=_ids("snapshot_id", "deal_cursor"),
        ),
        MessageType.POSITION_SNAPSHOT_ITEM: _Schema(
            frozenset(
                {
                    "snapshot_id", "position_id", "position_ticket", "trade_id",
                    "symbol", "magic", "direction", "volume", "price_open", "sl",
                    "tp", "position_version", "open_time_ms", "comment",
                }
            ),
            string_fields=_ids(
                "snapshot_id", "position_id", "position_ticket", "trade_id", "symbol",
                "direction", "comment",
            ),
        ),
        MessageType.POSITION_SNAPSHOT_END: _Schema(
            frozenset({"snapshot_id", "count", "deal_cursor"}),
            string_fields=_ids("snapshot_id", "deal_cursor"),
        ),
        MessageType.PENDING_ORDER_SNAPSHOT_BEGIN: _Schema(
            frozenset({"snapshot_id", "count"}),
            string_fields=_ids("snapshot_id"),
        ),
        MessageType.PENDING_ORDER_SNAPSHOT_ITEM: _Schema(
            frozenset(
                {
                    "snapshot_id", "order_id", "trade_id", "command_id", "symbol",
                    "magic", "order_type", "order_state", "volume_initial",
                    "volume_current", "price_open", "sl", "tp", "order_time_ms",
                    "expiration_time_ms", "comment",
                }
            ),
            string_fields=_ids(
                "snapshot_id", "order_id", "trade_id", "command_id", "symbol",
                "order_type", "order_state", "comment",
            ),
        ),
        MessageType.PENDING_ORDER_SNAPSHOT_END: _Schema(
            frozenset({"snapshot_id", "count"}),
            string_fields=_ids("snapshot_id"),
        ),
        MessageType.DEAL_REPLAY_BEGIN: _Schema(
            frozenset({"replay_id", "cursor_from", "count"}),
            string_fields=_ids("replay_id", "cursor_from"),
        ),
        MessageType.DEAL_REPLAY_ITEM: _Schema(
            frozenset(
                {
                    "replay_id", "cursor", "deal_id", "order_id", "position_id",
                    "position_ticket", "trade_id", "command_id", "symbol", "magic",
                    "direction", "entry_type", "price", "volume", "remaining_volume",
                    "remaining_volume_known",
                    "profit", "commission", "swap", "fee", "reason", "broker_time_ms",
                    "position_version", "comment",
                }
            ),
            string_fields=_ids(
                "replay_id", "cursor", "deal_id", "order_id", "position_id",
                "position_ticket", "trade_id", "command_id", "symbol", "direction",
                "entry_type", "reason", "comment",
            ),
        ),
        MessageType.DEAL_REPLAY_END: _Schema(
            frozenset({"replay_id", "cursor_next", "count"}),
            string_fields=_ids("replay_id", "cursor_next"),
        ),
        MessageType.HEARTBEAT: _Schema(
            frozenset(
                {
                    "account_id", "symbol", "balance", "equity", "free_margin",
                    "daily_pnl", "risk_day", "baseline_verified", "daily_halted",
                    "owned_positions", "last_deal_cursor",
                    "owned_pending_orders", "server_time_ms",
                }
            ),
            string_fields=_ids("account_id", "symbol", "risk_day", "last_deal_cursor"),
        ),
        MessageType.TICK: _Schema(
            frozenset({"symbol", "bid", "ask", "spread_points", "volume", "broker_time_ms"}),
            string_fields=_ids("symbol"),
        ),
        MessageType.BAR: _Schema(
            frozenset({"symbol", "tf", "o", "h", "l", "c", "volume", "bar_time_ms"}),
            string_fields=_ids("symbol", "tf"),
        ),
        MessageType.COMMAND: _Schema(
            frozenset({"command_id", "trade_id", "action", "expires_at_ms"}),
            frozenset(
                {
                    "reservation_id", "symbol", "magic", "direction", "volume",
                    "reference_price", "max_slippage_points", "sl", "tp1", "tp2", "tp3",
                    "risk_limit", "risk_pct_limit", "comment", "position_id",
                    "position_ticket", "expected_position_version", "new_sl", "final_tp",
                    "close_volume", "deal_cursor",
                }
            ),
            _ids(
                "command_id", "trade_id", "action", "reservation_id", "symbol",
                "direction", "comment", "position_id", "position_ticket", "deal_cursor",
            ),
        ),
        MessageType.COMMAND_ACK: _Schema(
            frozenset(
                {
                    "command_id", "trade_id", "action", "status", "retcode",
                    "retcode_desc", "order_id", "deal_id", "position_id",
                    "position_ticket", "actual_price", "actual_volume",
                }
            ),
            string_fields=_ids(
                "command_id", "trade_id", "action", "status", "retcode_desc", "order_id",
                "deal_id", "position_id", "position_ticket",
            ),
        ),
        MessageType.COMMAND_NACK: _Schema(
            frozenset(
                {"command_id", "trade_id", "action", "status", "reason", "retcode", "retcode_desc"}
            ),
            string_fields=_ids(
                "command_id", "trade_id", "action", "status", "reason", "retcode_desc"
            ),
        ),
        MessageType.BROKER_EVENT: _Schema(
            frozenset(
                {
                    "event_id", "event_type", "command_id", "trade_id", "order_id",
                    "deal_id", "position_id", "position_ticket", "symbol", "magic",
                    "direction", "price", "volume_delta", "remaining_volume", "sl", "tp",
                    "profit", "commission", "swap", "fee", "reason", "broker_time_ms",
                    "position_version",
                }
            ),
            string_fields=_ids(
                "event_id", "event_type", "command_id", "trade_id", "order_id", "deal_id",
                "position_id", "position_ticket", "symbol", "direction", "reason",
            ),
        ),
    }
)


BOOLEAN_FIELDS = MappingProxyType(
    {
        MessageType.HELLO: frozenset({"trade_allowed", "expert_allowed"}),
        MessageType.HELLO_ACK: frozenset({"allow_real"}),
        MessageType.ACCOUNT_SNAPSHOT: frozenset(
            {"trade_allowed", "expert_allowed", "baseline_verified", "daily_halted"}
        ),
        MessageType.DEAL_REPLAY_ITEM: frozenset({"remaining_volume_known"}),
        MessageType.HEARTBEAT: frozenset({"baseline_verified", "daily_halted"}),
    }
)


INTEGER_FIELDS = MappingProxyType(
    {
        MessageType.HELLO: frozenset({"magic", "terminal_build"}),
        MessageType.HELLO_ACK: frozenset({"expected_magic"}),
        MessageType.ACCOUNT_SNAPSHOT: frozenset({"server_time_ms"}),
        MessageType.SYMBOL_SNAPSHOT: frozenset(
            {"digits", "stops_level", "freeze_level"}
        ),
        MessageType.POSITION_SNAPSHOT_BEGIN: frozenset({"count"}),
        MessageType.POSITION_SNAPSHOT_ITEM: frozenset(
            {"magic", "position_version", "open_time_ms"}
        ),
        MessageType.POSITION_SNAPSHOT_END: frozenset({"count"}),
        MessageType.PENDING_ORDER_SNAPSHOT_BEGIN: frozenset({"count"}),
        MessageType.PENDING_ORDER_SNAPSHOT_ITEM: frozenset(
            {"magic", "order_time_ms", "expiration_time_ms"}
        ),
        MessageType.PENDING_ORDER_SNAPSHOT_END: frozenset({"count"}),
        MessageType.DEAL_REPLAY_BEGIN: frozenset({"count"}),
        MessageType.DEAL_REPLAY_ITEM: frozenset(
            {"magic", "broker_time_ms", "position_version"}
        ),
        MessageType.DEAL_REPLAY_END: frozenset({"count"}),
        MessageType.HEARTBEAT: frozenset(
            {"owned_positions", "owned_pending_orders", "server_time_ms"}
        ),
        MessageType.TICK: frozenset({"volume", "broker_time_ms"}),
        MessageType.BAR: frozenset({"volume", "bar_time_ms"}),
        MessageType.COMMAND: frozenset(
            {
                "expires_at_ms", "magic", "max_slippage_points",
                "expected_position_version",
            }
        ),
        MessageType.COMMAND_ACK: frozenset({"retcode"}),
        MessageType.COMMAND_NACK: frozenset({"retcode"}),
        MessageType.BROKER_EVENT: frozenset(
            {"magic", "broker_time_ms", "position_version"}
        ),
    }
)


COMMAND_REQUIRED: Mapping[CommandAction, frozenset[str]] = MappingProxyType(
    {
        CommandAction.OPEN: frozenset(
            {
                "reservation_id", "symbol", "magic", "direction", "volume",
                "reference_price", "max_slippage_points", "sl", "tp1", "tp2", "tp3",
                "risk_limit", "risk_pct_limit", "comment",
            }
        ),
        CommandAction.MODIFY_SL: frozenset(
            {"position_id", "position_ticket", "new_sl", "final_tp", "expected_position_version"}
        ),
        CommandAction.PARTIAL_CLOSE: frozenset(
            {"position_id", "position_ticket", "close_volume", "expected_position_version"}
        ),
        CommandAction.CLOSE: frozenset(
            {"position_id", "position_ticket", "expected_position_version"}
        ),
        CommandAction.CLOSE_ALL: frozenset({"symbol", "magic"}),
        CommandAction.REQUEST_SNAPSHOT: frozenset({"deal_cursor"}),
    }
)


def _freeze_fields(fields: Mapping[str, object]) -> Mapping[str, object]:
    return MappingProxyType(dict(fields))


def _is_scalar(value: object) -> bool:
    return isinstance(value, (str, int, float, bool, Decimal)) and not isinstance(
        value, (list, tuple, dict, set, frozenset)
    )


def _json_value(value: object) -> object:
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ProtocolError(ProtocolErrorCode.INVALID_FIELD_TYPE, "non-finite Decimal")
        return float(value)
    if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
        raise ProtocolError(ProtocolErrorCode.INVALID_FIELD_TYPE, "non-finite float")
    return value


@dataclass(frozen=True, slots=True)
class ProtocolMessage:
    message_type: MessageType
    msg_id: str
    auth: str
    epoch: str
    seq: int
    correlation_id: str
    ts_ms: int
    fields: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "fields", _freeze_fields(self.fields))

    def as_flat_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "v": PROTOCOL_VERSION,
            "type": self.message_type.value,
            "msg_id": self.msg_id,
            "auth": self.auth,
            "epoch": self.epoch,
            "seq": self.seq,
            "correlation_id": self.correlation_id,
            "ts_ms": self.ts_ms,
        }
        payload.update(self.fields)
        return payload


def _no_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ProtocolError(ProtocolErrorCode.DUPLICATE_FIELD, key)
        result[key] = value
    return result


class ProtocolCodec:
    def __init__(
        self,
        auth_token: str,
        *,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
    ) -> None:
        if not isinstance(auth_token, str) or not auth_token:
            raise ValueError("auth_token must be non-empty")
        if max_frame_bytes < 512:
            raise ValueError("max_frame_bytes is too small")
        self._auth_token = auth_token
        self.max_frame_bytes = max_frame_bytes

    def build(
        self,
        message_type: MessageType,
        *,
        msg_id: str,
        epoch: str,
        seq: int,
        correlation_id: str,
        ts_ms: int,
        **fields: object,
    ) -> ProtocolMessage:
        message = ProtocolMessage(
            message_type=message_type,
            msg_id=msg_id,
            auth=self._auth_token,
            epoch=epoch,
            seq=seq,
            correlation_id=correlation_id,
            ts_ms=ts_ms,
            fields=fields,
        )
        self.validate(message)
        return message

    def encode(self, message: ProtocolMessage) -> bytes:
        self.validate(message)
        payload = {key: _json_value(value) for key, value in message.as_flat_dict().items()}
        encoded = (
            json.dumps(payload, ensure_ascii=True, allow_nan=False, separators=(",", ":"))
            + "\n"
        ).encode("utf-8")
        if len(encoded) > self.max_frame_bytes:
            raise ProtocolError(
                ProtocolErrorCode.FRAME_TOO_LARGE,
                f"{len(encoded)} > {self.max_frame_bytes}",
            )
        return encoded

    def decode(
        self,
        frame: bytes | str,
        *,
        expected_epoch: str | None = None,
    ) -> ProtocolMessage:
        raw = frame.encode("utf-8") if isinstance(frame, str) else frame
        if len(raw) > self.max_frame_bytes:
            raise ProtocolError(ProtocolErrorCode.FRAME_TOO_LARGE, str(len(raw)))
        if not raw.endswith(b"\n") or raw.count(b"\n") != 1:
            raise ProtocolError(
                ProtocolErrorCode.INVALID_NDJSON,
                "one object terminated by exactly one newline is required",
            )
        try:
            data = json.loads(
                raw[:-1].decode("utf-8"),
                parse_float=Decimal,
                parse_constant=lambda value: (_ for _ in ()).throw(
                    ProtocolError(ProtocolErrorCode.INVALID_JSON, value)
                ),
                object_pairs_hook=_no_duplicate_pairs,
            )
        except ProtocolError:
            raise
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ProtocolError(ProtocolErrorCode.INVALID_JSON, str(exc)) from exc
        if not isinstance(data, dict):
            raise ProtocolError(ProtocolErrorCode.INVALID_JSON, "root must be an object")
        for key, value in data.items():
            if not isinstance(key, str) or not _is_scalar(value):
                raise ProtocolError(ProtocolErrorCode.NESTED_VALUE, str(key))
        missing_common = COMMON_FIELDS - frozenset(data)
        if missing_common:
            raise ProtocolError(
                ProtocolErrorCode.MISSING_FIELD,
                ",".join(sorted(missing_common)),
            )
        if data.get("v") != PROTOCOL_VERSION:
            raise ProtocolError(
                ProtocolErrorCode.UNSUPPORTED_VERSION, str(data.get("v"))
            )

        try:
            message_type = MessageType(data.get("type"))
        except ValueError as exc:
            raise ProtocolError(
                ProtocolErrorCode.UNKNOWN_MESSAGE_TYPE, str(data.get("type"))
            ) from exc
        message = ProtocolMessage(
            message_type=message_type,
            msg_id=data.get("msg_id"),
            auth=data.get("auth"),
            epoch=data.get("epoch"),
            seq=data.get("seq"),
            correlation_id=data.get("correlation_id"),
            ts_ms=data.get("ts_ms"),
            fields={key: value for key, value in data.items() if key not in COMMON_FIELDS},
        )
        self.validate(message, expected_epoch=expected_epoch)
        return message

    def validate(
        self,
        message: ProtocolMessage,
        *,
        expected_epoch: str | None = None,
    ) -> None:
        if not isinstance(message.message_type, MessageType):
            raise ProtocolError(ProtocolErrorCode.UNKNOWN_MESSAGE_TYPE, str(message.message_type))
        for name, value in (
            ("msg_id", message.msg_id),
            ("auth", message.auth),
            ("epoch", message.epoch),
            ("correlation_id", message.correlation_id),
        ):
            if not isinstance(value, str):
                raise ProtocolError(ProtocolErrorCode.INVALID_FIELD_TYPE, name)
        if not message.msg_id or not message.epoch:
            raise ProtocolError(ProtocolErrorCode.MISSING_FIELD, "msg_id/epoch")
        if not hmac.compare_digest(message.auth, self._auth_token):
            raise ProtocolError(ProtocolErrorCode.AUTH_FAILED, "token mismatch")
        if expected_epoch is not None and message.epoch != expected_epoch:
            raise ProtocolError(ProtocolErrorCode.EPOCH_MISMATCH, message.epoch)
        if isinstance(message.seq, bool) or not isinstance(message.seq, int) or message.seq < 1:
            raise ProtocolError(ProtocolErrorCode.INVALID_SEQUENCE, str(message.seq))
        if isinstance(message.ts_ms, bool) or not isinstance(message.ts_ms, int) or message.ts_ms < 0:
            raise ProtocolError(ProtocolErrorCode.INVALID_FIELD_TYPE, "ts_ms")

        schema = SCHEMAS[message.message_type]
        field_names = frozenset(message.fields)
        missing = schema.required - field_names
        if missing:
            raise ProtocolError(
                ProtocolErrorCode.MISSING_FIELD, ",".join(sorted(missing))
            )
        unknown = field_names - schema.required - schema.optional
        if unknown:
            raise ProtocolError(
                ProtocolErrorCode.UNKNOWN_FIELD, ",".join(sorted(unknown))
            )
        for name, value in message.fields.items():
            if not _is_scalar(value):
                raise ProtocolError(ProtocolErrorCode.NESTED_VALUE, name)
        boolean_fields = BOOLEAN_FIELDS.get(message.message_type, frozenset())
        integer_fields = INTEGER_FIELDS.get(message.message_type, frozenset())
        for name in schema.string_fields:
            if name in message.fields and not isinstance(message.fields[name], str):
                raise ProtocolError(ProtocolErrorCode.INVALID_FIELD_TYPE, name)
            must_be_nonempty = (
                name.endswith("_id")
                or name in {
                    "position_ticket", "symbol", "action", "status", "event_type",
                    "role", "ea_version", "config_hash", "account_trade_mode",
                    "account_mode", "currency", "risk_day", "tf", "direction",
                    "entry_type", "cursor", "cursor_from", "cursor_next",
                    "deal_cursor", "last_deal_cursor",
                }
            )
            if name in message.fields and must_be_nonempty and not message.fields[name]:
                raise ProtocolError(ProtocolErrorCode.MISSING_FIELD, name)
        for name in boolean_fields:
            if not isinstance(message.fields[name], bool):
                raise ProtocolError(ProtocolErrorCode.INVALID_FIELD_TYPE, name)
        for name in integer_fields:
            if name not in message.fields:
                continue
            value = message.fields[name]
            if isinstance(value, bool) or not isinstance(value, int):
                raise ProtocolError(ProtocolErrorCode.INVALID_FIELD_TYPE, name)
        numeric_fields = field_names - schema.string_fields - boolean_fields - integer_fields
        for name in numeric_fields:
            value = message.fields[name]
            if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
                raise ProtocolError(ProtocolErrorCode.INVALID_FIELD_TYPE, name)
            if isinstance(value, Decimal) and not value.is_finite():
                raise ProtocolError(ProtocolErrorCode.INVALID_FIELD_TYPE, name)
            if isinstance(value, float) and (
                value != value or value in (float("inf"), float("-inf"))
            ):
                raise ProtocolError(ProtocolErrorCode.INVALID_FIELD_TYPE, name)

        if message.message_type == MessageType.COMMAND:
            try:
                action = CommandAction(message.fields["action"])
            except (ValueError, TypeError) as exc:
                raise ProtocolError(
                    ProtocolErrorCode.INVALID_COMMAND, str(message.fields.get("action"))
                ) from exc
            missing_action_fields = COMMAND_REQUIRED[action] - field_names
            if missing_action_fields:
                raise ProtocolError(
                    ProtocolErrorCode.INVALID_COMMAND,
                    f"{action.value} missing {','.join(sorted(missing_action_fields))}",
                )
            if action in {
                CommandAction.MODIFY_SL,
                CommandAction.PARTIAL_CLOSE,
                CommandAction.CLOSE,
            } and message.fields["expected_position_version"] < 1:
                raise ProtocolError(
                    ProtocolErrorCode.INVALID_COMMAND,
                    f"{action.value} expected_position_version must be positive",
                )


class ProtocolSession:
    """One epoch-fenced, monotonically sequenced authenticated session."""

    def __init__(
        self,
        codec: ProtocolCodec,
        epoch: str,
        *,
        clock: Clock | None = None,
    ) -> None:
        if not epoch:
            raise ValueError("epoch must be non-empty")
        self.codec = codec
        self.epoch = epoch
        self.clock = clock or SystemClock()
        self._next_tx_seq = 1
        self._last_rx_seq = 0
        self._authenticated = False

    @property
    def authenticated(self) -> bool:
        return self._authenticated

    @property
    def last_rx_seq(self) -> int:
        return self._last_rx_seq

    def mark_authenticated(self) -> None:
        self._authenticated = True

    def build(
        self,
        message_type: MessageType,
        *,
        msg_id: str,
        correlation_id: str = "",
        **fields: object,
    ) -> ProtocolMessage:
        if not self._authenticated and message_type not in {
            MessageType.HELLO,
            MessageType.HELLO_ACK,
        }:
            raise ProtocolError(
                ProtocolErrorCode.SESSION_NOT_AUTHENTICATED, message_type.value
            )
        message = self.codec.build(
            message_type,
            msg_id=msg_id,
            epoch=self.epoch,
            seq=self._next_tx_seq,
            correlation_id=correlation_id,
            ts_ms=self.clock.now_ms(),
            **fields,
        )
        self._next_tx_seq += 1
        return message

    def receive(self, frame: bytes | str) -> ProtocolMessage:
        message = self.codec.decode(frame, expected_epoch=self.epoch)
        if message.seq <= self._last_rx_seq:
            raise ProtocolError(
                ProtocolErrorCode.INVALID_SEQUENCE,
                f"{message.seq} <= {self._last_rx_seq}",
            )
        if not self._authenticated and message.message_type not in {
            MessageType.HELLO,
            MessageType.HELLO_ACK,
        }:
            raise ProtocolError(
                ProtocolErrorCode.SESSION_NOT_AUTHENTICATED,
                message.message_type.value,
            )
        self._last_rx_seq = message.seq
        return message


__all__ = [
    "COMMAND_REQUIRED",
    "DEFAULT_MAX_FRAME_BYTES",
    "MessageType",
    "CommandAction",
    "PROTOCOL_VERSION",
    "ProtocolCodec",
    "ProtocolError",
    "ProtocolErrorCode",
    "ProtocolMessage",
    "ProtocolSession",
    "SCHEMAS",
]
