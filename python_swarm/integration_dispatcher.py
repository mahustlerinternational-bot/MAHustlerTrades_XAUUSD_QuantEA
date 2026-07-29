"""Durable-outbox integration ports isolated from the trading control path."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import html
import json
import logging
import math
import random
import re
import time
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
from types import MappingProxyType
from typing import Any, Callable, Mapping, Optional, Protocol, Sequence
from urllib.parse import urlsplit

try:
    from .trade_ledger import OutboxRecord
except ImportError:  # pragma: no cover - legacy flat-import compatibility
    from trade_ledger import OutboxRecord  # type: ignore


log = logging.getLogger("INTEGRATION-DISPATCHER")

EVENT_SCHEMA_VERSION = 1
MAX_EVENT_BYTES = 65_536
MAX_RESPONSE_BODY_BYTES = 65_536
_EVENT_ENVELOPE_FIELDS = frozenset(
    {
        "schema_version",
        "event_id",
        "event_type",
        "severity",
        "occurred_at",
        "trade_id",
        "request_id",
        "payload",
    }
)
_EVENT_TYPE = re.compile(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*")
_SEVERITIES = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})

_SECRET_KEY = re.compile(
    r"(?:authorization|api[_-]?key|token|secret|password|service[_-]?role|cookie)",
    re.IGNORECASE,
)
_TELEGRAM_TOKEN = re.compile(r"\b[0-9]{8,12}:[A-Za-z0-9_-]{25,}\b")
_JWT = re.compile(
    r"\beyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\b"
)
_URL = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
_TEXT_SECRET_ASSIGNMENT = re.compile(
    r"\b(?:authorization|api[_-]?key|token|secret|password|cookie)"
    r"\s*[:=]\s*[^\s,;]+",
    re.IGNORECASE,
)


def redact_text(value: str) -> str:
    value = _URL.sub("[REDACTED_URL]", value)
    value = _TEXT_SECRET_ASSIGNMENT.sub("[REDACTED]", value)
    value = _TELEGRAM_TOKEN.sub("[REDACTED]", value)
    return _JWT.sub("[REDACTED]", value)


def redact_payload(value: Any, key: str = "") -> Any:
    if key and _SECRET_KEY.search(key):
        return "[REDACTED]"
    if isinstance(value, Mapping):
        return {str(k): redact_payload(v, str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact_payload(item) for item in value]
    if isinstance(value, str):
        return redact_text(value)
    return value


@dataclass(frozen=True)
class DomainEvent:
    event_id: str
    event_type: str
    severity: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    occurred_at: float = field(default_factory=time.time)
    trade_id: Optional[str] = None
    request_id: Optional[str] = None
    schema_version: int = EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            isinstance(self.schema_version, bool)
            or not isinstance(self.schema_version, int)
            or self.schema_version != EVENT_SCHEMA_VERSION
        ):
            raise ValueError(
                f"unsupported event schema_version {self.schema_version!r}; "
                f"expected {EVENT_SCHEMA_VERSION}"
            )
        _strict_identifier(self.event_id, "event_id")
        if (
            not isinstance(self.event_type, str)
            or not _EVENT_TYPE.fullmatch(self.event_type)
        ):
            raise ValueError("event_type must use the canonical lowercase event syntax")
        if not isinstance(self.severity, str) or self.severity not in _SEVERITIES:
            raise ValueError("event severity is not canonical")
        if (
            isinstance(self.occurred_at, bool)
            or not isinstance(self.occurred_at, (int, float))
            or not math.isfinite(float(self.occurred_at))
            or float(self.occurred_at) <= 0
        ):
            raise ValueError("event occurred_at must be a positive finite timestamp")
        if self.trade_id is not None:
            _strict_identifier(self.trade_id, "trade_id")
        if self.request_id is not None:
            _strict_identifier(self.request_id, "request_id")
        _validate_event_payload(self.payload)
        object.__setattr__(self, "occurred_at", float(self.occurred_at))
        object.__setattr__(self, "payload", _freeze_json(self.payload))
        if len(_canonical_json_bytes(self.sanitized_dict())) > MAX_EVENT_BYTES:
            raise ValueError("structured event envelope exceeds the size limit")

    def sanitized_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "severity": self.severity,
            "occurred_at": self.occurred_at,
            "trade_id": self.trade_id,
            "request_id": self.request_id,
            "payload": redact_payload(self.payload),
        }


def _strict_identifier(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field_name} must be a canonical non-empty string")
    if len(value) > 128 or any(
        ord(char) < 0x20 or ord(char) == 0x7F for char in value
    ):
        raise ValueError(f"{field_name} contains invalid characters")
    if redact_text(value) != value:
        raise ValueError(f"{field_name} must not contain credential material")
    return value


def _validate_event_payload(value: Any) -> None:
    if not isinstance(value, Mapping):
        raise ValueError("event payload must be a JSON object")

    def validate(item: Any, *, depth: int) -> None:
        if depth > 24:
            raise ValueError("event payload nesting exceeds the safety limit")
        if item is None or isinstance(item, (str, bool, int)):
            return
        if isinstance(item, float):
            if not math.isfinite(item):
                raise ValueError("event payload contains a non-finite number")
            return
        if isinstance(item, Mapping):
            for key, nested in item.items():
                if not isinstance(key, str):
                    raise ValueError("event payload object keys must be strings")
                validate(nested, depth=depth + 1)
            return
        if isinstance(item, (list, tuple)):
            for nested in item:
                validate(nested, depth=depth + 1)
            return
        raise ValueError("event payload contains a non-JSON value")

    validate(value, depth=0)
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("event payload is not canonical JSON") from exc
    if len(encoded) > MAX_EVENT_BYTES:
        raise ValueError("event payload exceeds the structured-event size limit")


def _freeze_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {key: _freeze_json(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    return value


def _canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def outbox_records_for_event(
    event: DomainEvent,
    destinations: Sequence[str],
) -> tuple[OutboxRecord, ...]:
    """Build deterministic structured projection records for one event.

    These records are intended to be committed in the same ledger transaction
    as the corresponding authoritative trade event.  Delivery remains an
    asynchronous, non-trading side effect.
    """

    allowed = {"telegram", "discord", "website"}
    normalized = tuple(str(item).strip().casefold() for item in destinations)
    if any(not item or item not in allowed for item in normalized):
        raise ValueError("outbox destination is not a supported projection")
    if len(normalized) != len(set(normalized)):
        raise ValueError("outbox destinations must be unique")
    records: list[OutboxRecord] = []
    for destination in sorted(normalized):
        digest = hashlib.sha256(
            f"v1\0{destination}\0{event.event_id}".encode("utf-8")
        ).hexdigest()
        records.append(
            OutboxRecord(
                message_id=f"outbox:{digest}",
                idempotency_key=f"projection:v1:{digest}",
                event_id=event.event_id,
                destination=destination,
                topic=event.event_type,
                # Each destination owns an independent plain-JSON snapshot.
                # The authoritative event itself is already immutable.
                payload=event.sanitized_dict(),
                created_at=event.occurred_at,
            )
        )
    return tuple(records)


@dataclass(frozen=True)
class OutboxMessage:
    message_id: str
    destination: str
    event: DomainEvent
    attempts: int = 0
    claim_error: str = ""


class OutboxStore(Protocol):
    """Ledger-facing contract; claiming must be atomic in its implementation."""

    async def claim_due(self, *, now: float, limit: int) -> Sequence[OutboxMessage]: ...

    async def mark_delivered(
        self, message_id: str, *, delivered_at: float, detail: str
    ) -> None: ...

    async def mark_retry(
        self,
        message_id: str,
        *,
        attempts: int,
        next_attempt_at: float,
        error: str,
    ) -> None: ...

    async def mark_dead_letter(
        self, message_id: str, *, attempts: int, failed_at: float, error: str
    ) -> None: ...


class OutboxStoreError(RuntimeError):
    """Sanitized adapter failure surfaced through dispatcher health."""


class TradeLedgerOutboxPort(Protocol):
    """Narrow v1 ledger API consumed by :class:`LedgerOutboxStore`."""

    def claim_outbox(
        self,
        worker_id: str,
        *,
        limit: int,
        lease_seconds: float,
        now: float,
    ) -> Sequence[Mapping[str, Any]]: ...

    def mark_outbox_delivered(
        self, message_id: str, worker_id: str, *, delivered_at: float
    ) -> bool: ...

    def mark_outbox_failed(
        self,
        message_id: str,
        worker_id: str,
        error: str,
        *,
        retry_at: Optional[float],
        now: float,
    ) -> bool: ...


class LedgerOutboxStore:
    """Async, lease-preserving adapter over the authoritative ledger v1 API.

    Ledger calls run in worker threads and never share the trading transaction.
    The ledger owns attempt increments when it grants the lease; the dispatcher
    therefore receives the number of attempts completed before this claim.
    """

    def __init__(
        self,
        ledger: TradeLedgerOutboxPort,
        *,
        worker_id: str,
        lease_seconds: float = 30.0,
        clock: Callable[[], float] = time.time,
    ):
        worker = worker_id.strip()
        if not worker:
            raise ValueError("outbox worker_id is required")
        if not 1.0 <= float(lease_seconds) <= 3_600.0:
            raise ValueError("outbox lease_seconds must be between 1 and 3600")
        self.ledger = ledger
        self.worker_id = worker
        self.lease_seconds = float(lease_seconds)
        self.clock = clock

    async def claim_due(self, *, now: float, limit: int) -> Sequence[OutboxMessage]:
        try:
            rows = await asyncio.to_thread(
                self.ledger.claim_outbox,
                self.worker_id,
                limit=limit,
                lease_seconds=self.lease_seconds,
                now=now,
            )
            messages: list[OutboxMessage] = []
            for row in rows:
                try:
                    messages.append(self._decode_claimed_row(row))
                except Exception as exc:
                    messages.append(self._malformed_claim_message(row, exc))
            return tuple(messages)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise OutboxStoreError(
                f"ledger outbox claim failed ({type(exc).__name__})"
            ) from exc

    async def mark_delivered(
        self, message_id: str, *, delivered_at: float, detail: str
    ) -> None:
        del detail  # v1 deliberately stores delivery state, not remote response bodies.
        await self._ledger_call(
            "delivered",
            self.ledger.mark_outbox_delivered,
            message_id,
            self.worker_id,
            delivered_at=delivered_at,
        )

    async def mark_retry(
        self,
        message_id: str,
        *,
        attempts: int,
        next_attempt_at: float,
        error: str,
    ) -> None:
        if attempts < 1:
            raise OutboxStoreError("ledger retry attempt count is invalid")
        await self._ledger_call(
            "retry",
            self.ledger.mark_outbox_failed,
            message_id,
            self.worker_id,
            error,
            retry_at=next_attempt_at,
            now=self.clock(),
        )

    async def mark_dead_letter(
        self, message_id: str, *, attempts: int, failed_at: float, error: str
    ) -> None:
        if attempts < 1:
            raise OutboxStoreError("ledger dead-letter attempt count is invalid")
        await self._ledger_call(
            "dead_letter",
            self.ledger.mark_outbox_failed,
            message_id,
            self.worker_id,
            error,
            retry_at=None,
            now=failed_at,
        )

    async def _ledger_call(
        self,
        operation: str,
        function: Callable[..., Any],
        *args,
        **kwargs,
    ) -> None:
        try:
            result = await asyncio.to_thread(function, *args, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise OutboxStoreError(
                f"ledger outbox {operation} failed ({type(exc).__name__})"
            ) from exc
        if result is False:
            raise OutboxStoreError(
                f"ledger outbox {operation} returned a negative acknowledgement"
            )

    def _malformed_claim_message(
        self, row: Mapping[str, Any], decode_error: Exception
    ) -> OutboxMessage:
        if not isinstance(row, Mapping):
            raise ValueError("claimed outbox row is not a mapping")
        if row.get("status") != "IN_FLIGHT" or row.get("locked_by") != self.worker_id:
            raise ValueError("malformed row does not hold this worker lease")
        message_id = _required_text(row.get("message_id"), "message_id")
        claimed_attempts = _nonnegative_int(row.get("attempts"), "attempts")
        if claimed_attempts < 1:
            raise ValueError("claimed attempt count was not incremented")
        occurred_at = row.get("created_at")
        if (
            isinstance(occurred_at, bool)
            or not isinstance(occurred_at, (int, float))
            or not math.isfinite(float(occurred_at))
            or float(occurred_at) <= 0
        ):
            occurred_at = self.clock()
        error_type = type(decode_error).__name__
        safe_event_id = "malformed:" + hashlib.sha256(
            message_id.encode("utf-8")
        ).hexdigest()
        return OutboxMessage(
            message_id=message_id,
            destination="__malformed__",
            attempts=claimed_attempts - 1,
            claim_error=f"malformed claimed outbox payload ({error_type})",
            event=DomainEvent(
                event_id=safe_event_id,
                event_type="outbox.claim_decode_failed",
                severity="ERROR",
                occurred_at=float(occurred_at),
                payload={"error_type": error_type},
            ),
        )

    def _decode_claimed_row(self, row: Mapping[str, Any]) -> OutboxMessage:
        if not isinstance(row, Mapping):
            raise ValueError("claimed outbox row is not a mapping")
        if row.get("status") != "IN_FLIGHT" or row.get("locked_by") != self.worker_id:
            raise ValueError("claimed outbox row does not hold this worker lease")

        message_id = _required_text(row.get("message_id"), "message_id")
        destination = _required_text(row.get("destination"), "destination").casefold()
        topic = _required_text(row.get("topic"), "topic")
        claimed_attempts = _nonnegative_int(row.get("attempts"), "attempts")
        if claimed_attempts < 1:
            raise ValueError("claimed attempt count was not incremented")

        raw_payload = row.get("payload", {})
        if not isinstance(raw_payload, Mapping):
            raise ValueError("claimed outbox payload is not an object")
        structured = dict(raw_payload)
        if set(structured) != _EVENT_ENVELOPE_FIELDS:
            missing = sorted(_EVENT_ENVELOPE_FIELDS.difference(structured))
            extra = sorted(set(structured).difference(_EVENT_ENVELOPE_FIELDS))
            raise ValueError(
                f"structured event envelope mismatch (missing={missing}, extra={extra})"
            )
        schema_version = structured.get("schema_version")
        if (
            isinstance(schema_version, bool)
            or not isinstance(schema_version, int)
            or schema_version != EVENT_SCHEMA_VERSION
        ):
            raise ValueError("structured event schema version is unsupported")
        nested_payload = structured.get("payload")
        if not isinstance(nested_payload, Mapping):
            raise ValueError("structured event payload is not an object")
        event_payload = dict(nested_payload)

        occurred_at = structured.get("occurred_at", row.get("created_at"))
        if (
            isinstance(occurred_at, bool)
            or not isinstance(occurred_at, (int, float))
            or not math.isfinite(float(occurred_at))
            or float(occurred_at) <= 0
        ):
            raise ValueError("claimed event timestamp is invalid")

        row_event_id = _required_text(row.get("event_id"), "event_id")
        structured_event_id = structured.get("event_id")
        if structured_event_id != row_event_id:
            raise ValueError("structured event identity conflicts with ledger event")
        structured_type = structured.get("event_type")
        if structured_type != topic:
            raise ValueError("structured event type conflicts with ledger topic")
        trade_id = _optional_event_text(structured.get("trade_id"))
        request_id = _optional_event_text(structured.get("request_id"))
        return OutboxMessage(
            message_id=message_id,
            destination=destination,
            attempts=claimed_attempts - 1,
            event=DomainEvent(
                event_id=row_event_id,
                event_type=_required_text(structured_type, "event_type"),
                severity=_required_text(structured.get("severity"), "severity"),
                occurred_at=float(occurred_at),
                trade_id=trade_id,
                request_id=request_id,
                payload=event_payload,
                schema_version=EVENT_SCHEMA_VERSION,
            ),
        )


def _required_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _optional_event_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    return _required_text(value, "optional event identifier")


def _nonnegative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


@dataclass(frozen=True)
class HTTPPostResponse:
    status: int
    payload: Mapping[str, Any] = field(default_factory=dict)
    headers: Mapping[str, str] = field(default_factory=dict)
    body_truncated: bool = False

    def __post_init__(self) -> None:
        if (
            isinstance(self.status, bool)
            or not isinstance(self.status, int)
            or not 100 <= self.status <= 599
        ):
            raise ValueError("HTTP response status must be between 100 and 599")
        if not isinstance(self.payload, Mapping):
            raise ValueError("HTTP response payload must be an object")
        if not isinstance(self.headers, Mapping):
            raise ValueError("HTTP response headers must be an object")
        if not isinstance(self.body_truncated, bool):
            raise ValueError("HTTP body_truncated must be boolean")


class HTTPPostTransport(Protocol):
    async def post_json(
        self,
        url: str,
        payload: Mapping[str, Any],
        *,
        headers: Mapping[str, str],
        timeout_s: float,
    ) -> HTTPPostResponse: ...

    async def post_bytes(
        self,
        url: str,
        body: bytes,
        *,
        headers: Mapping[str, str],
        timeout_s: float,
    ) -> HTTPPostResponse: ...


class AiohttpPostTransport:
    async def post_json(
        self,
        url: str,
        payload: Mapping[str, Any],
        *,
        headers: Mapping[str, str],
        timeout_s: float,
    ) -> HTTPPostResponse:
        body = _canonical_json_bytes(payload)
        request_headers = {"Content-Type": "application/json; charset=utf-8"}
        request_headers.update(headers)
        return await self.post_bytes(
            url,
            body,
            headers=request_headers,
            timeout_s=timeout_s,
        )

    async def post_bytes(
        self,
        url: str,
        body: bytes,
        *,
        headers: Mapping[str, str],
        timeout_s: float,
    ) -> HTTPPostResponse:
        try:
            import aiohttp
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise RuntimeError("aiohttp is required for integration delivery") from exc

        if not isinstance(body, bytes):
            raise TypeError("HTTP byte transport requires an immutable bytes body")
        timeout = aiohttp.ClientTimeout(total=timeout_s)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                url,
                data=body,
                headers=dict(headers),
                allow_redirects=False,
            ) as response:
                response_headers: dict[str, str] = {}
                retry_after = response.headers.get("Retry-After")
                if retry_after is not None:
                    response_headers["retry-after"] = str(retry_after).replace(
                        "\r", ""
                    ).replace("\n", "")[:128]

                content_length = response.content_length
                if (
                    content_length is not None
                    and content_length > MAX_RESPONSE_BODY_BYTES
                ):
                    response.close()
                    return HTTPPostResponse(
                        response.status,
                        {},
                        response_headers,
                        body_truncated=True,
                    )
                try:
                    response_body = await response.content.readexactly(
                        MAX_RESPONSE_BODY_BYTES + 1
                    )
                    truncated = True
                except asyncio.IncompleteReadError as exc:
                    response_body = exc.partial
                    truncated = False
                if truncated:
                    response.close()
                    response_body = response_body[:MAX_RESPONSE_BODY_BYTES]

                decoded: Mapping[str, Any] = {}
                if response_body and not truncated:
                    try:
                        candidate = json.loads(response_body)
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        candidate = {}
                    if isinstance(candidate, Mapping):
                        decoded = candidate
                return HTTPPostResponse(
                    response.status,
                    decoded,
                    response_headers,
                    body_truncated=truncated,
                )


class DeliveryError(RuntimeError):
    """Sanitized remote failure carrying only bounded scheduling evidence."""

    def __init__(
        self,
        destination: str,
        status: int,
        *,
        retry_after_s: float | None = None,
    ):
        super().__init__(f"{destination} delivery failed with HTTP {status}")
        self.retry_after_s = retry_after_s


def _retry_after_seconds(
    response: HTTPPostResponse,
    *,
    now: float | None = None,
) -> float | None:
    raw = next(
        (
            str(value).strip()
            for key, value in response.headers.items()
            if str(key).casefold() == "retry-after"
        ),
        "",
    )
    if not raw:
        return None
    try:
        numeric = float(raw)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(raw)
            if parsed.tzinfo is None:
                return None
            numeric = parsed.timestamp() - (time.time() if now is None else now)
        except (TypeError, ValueError, OverflowError):
            return None
    if not math.isfinite(numeric) or numeric < 0:
        return None
    return numeric


def _raise_delivery_error(destination: str, response: HTTPPostResponse) -> None:
    raise DeliveryError(
        destination,
        response.status,
        retry_after_s=_retry_after_seconds(response),
    )


class Publisher(Protocol):
    name: str

    async def publish(self, event: DomainEvent) -> str: ...


def _require_https_url(value: str, destination: str) -> str:
    url = value.strip()
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise ValueError(f"{destination} must use an HTTPS URL without userinfo")
    return url


def _compact_payload(event: DomainEvent, max_chars: int) -> str:
    encoded = json.dumps(
        redact_payload(event.payload), sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False,
    )
    if len(encoded) > max_chars:
        return encoded[: max(0, max_chars - 1)] + "…"
    return encoded


def _html_escape_within(value: str, budget: int) -> str:
    """Escape at a character boundary while honoring an encoded-size budget."""
    if budget <= 0:
        return ""
    escaped = html.escape(value, quote=True)
    if len(escaped) <= budget:
        return escaped
    suffix = "…"
    low, high = 0, len(value)
    while low < high:
        middle = (low + high + 1) // 2
        candidate = html.escape(value[:middle], quote=True)
        if len(candidate) + len(suffix) <= budget:
            low = middle
        else:
            high = middle - 1
    return html.escape(value[:low], quote=True) + suffix


class TelegramPublisher:
    name = "telegram"

    def __init__(
        self,
        *,
        token: str,
        chat_id: str,
        transport: Optional[HTTPPostTransport] = None,
        timeout_s: float = 10.0,
    ):
        token = token.strip()
        chat_id = chat_id.strip()
        if not token or not chat_id:
            raise ValueError("Telegram token and chat ID are required")
        self._url = f"https://api.telegram.org/bot{token}/sendMessage"
        self._chat_id = chat_id
        self._transport = transport or AiohttpPostTransport()
        self._timeout_s = max(0.1, min(float(timeout_s), 120.0))

    async def publish(self, event: DomainEvent) -> str:
        title = html.escape(redact_text(event.event_type)[:160], quote=True)
        severity = html.escape(redact_text(event.severity)[:32], quote=True)
        event_id = html.escape(redact_text(event.event_id)[:128], quote=True)
        prefix = f"<b>{title}</b> [{severity}]\n<code>"
        suffix = f"</code>\nevent <code>{event_id}</code>"
        body = _html_escape_within(
            _compact_payload(event, 3200), 4096 - len(prefix) - len(suffix)
        )
        text = prefix + body + suffix
        response = await self._transport.post_json(
            self._url,
            {
                "chat_id": self._chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            headers={},
            timeout_s=self._timeout_s,
        )
        if not 200 <= response.status < 300 or response.payload.get("ok") is not True:
            _raise_delivery_error("Telegram", response)
        return f"HTTP {response.status}"


def _escape_discord(value: str) -> str:
    return re.sub(r"([\\`*_{}\[\]()#+\-.!|>])", r"\\\1", value)


class DiscordPublisher:
    name = "discord"

    def __init__(
        self,
        *,
        webhook_url: str,
        transport: Optional[HTTPPostTransport] = None,
        timeout_s: float = 10.0,
    ):
        if not webhook_url:
            raise ValueError("Discord webhook URL is required")
        self._url = _require_https_url(webhook_url, "Discord webhook")
        self._transport = transport or AiohttpPostTransport()
        self._timeout_s = max(0.1, min(float(timeout_s), 120.0))

    async def publish(self, event: DomainEvent) -> str:
        # A payload value containing a code fence must not escape the inert JSON
        # block and turn attacker-controlled text into Discord markdown.
        title = _escape_discord(redact_text(event.event_type)[:160])
        severity = _escape_discord(redact_text(event.severity)[:32])
        event_id = _escape_discord(redact_text(event.event_id)[:128])
        prefix = f"**{title}** [{severity}]\n```json\n"
        suffix = f"\n```\nevent `{event_id}`"
        body = _compact_payload(event, 1700).replace("```", "`\u200b``")
        body_budget = max(0, 2000 - len(prefix) - len(suffix))
        if len(body) > body_budget:
            body = body[: max(0, body_budget - 1)] + "…"
        content = prefix + body + suffix
        response = await self._transport.post_json(
            self._url,
            {"content": content, "allowed_mentions": {"parse": []}},
            headers={},
            timeout_s=self._timeout_s,
        )
        if not 200 <= response.status < 300:
            _raise_delivery_error("Discord", response)
        return f"HTTP {response.status}"


class SignedWebsiteWebhookPublisher:
    name = "website"

    def __init__(
        self,
        *,
        url: str,
        signing_secret: str,
        transport: Optional[HTTPPostTransport] = None,
        timeout_s: float = 10.0,
    ):
        if not url or not signing_secret:
            raise ValueError("Website URL and signing secret are required")
        self._url = _require_https_url(url, "Website webhook")
        self._secret = signing_secret.encode("utf-8")
        self._transport = transport or AiohttpPostTransport()
        self._timeout_s = max(0.1, min(float(timeout_s), 120.0))

    async def publish(self, event: DomainEvent) -> str:
        payload = event.sanitized_dict()
        canonical = _canonical_json_bytes(payload)
        signature = hmac.new(self._secret, canonical, hashlib.sha256).hexdigest()
        response = await self._transport.post_bytes(
            self._url,
            canonical,
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "X-Quant-Swarm-Event-ID": re.sub(
                    r"[\r\n]", "", event.event_id
                )[:128],
                "X-Quant-Swarm-Schema-Version": str(EVENT_SCHEMA_VERSION),
                "X-Quant-Swarm-Signature": f"sha256={signature}",
            },
            timeout_s=self._timeout_s,
        )
        if not 200 <= response.status < 300:
            _raise_delivery_error("Website", response)
        return f"HTTP {response.status}"


class IntegrationConfigurationError(ValueError):
    """An enabled projection is missing its external credential material."""


def build_publishers(
    settings: object,
    environ: Mapping[str, str],
    *,
    transport: Optional[HTTPPostTransport] = None,
) -> Mapping[str, Publisher]:
    """Construct only explicitly enabled publishers from external secrets.

    The returned mapping contains no trading capability.  Secrets are never
    copied into runtime configuration, events, the ledger, or health output.
    """

    publishers: dict[str, Publisher] = {}
    if bool(getattr(settings, "telegram", False)):
        token = environ.get("QSX_TG_TOKEN", "").strip()
        chat_id = environ.get("QSX_TG_CHAT", "").strip()
        if not _TELEGRAM_TOKEN.fullmatch(token) or not chat_id:
            raise IntegrationConfigurationError(
                "Telegram is enabled but its external credentials are invalid"
            )
        publishers["telegram"] = TelegramPublisher(
            token=token,
            chat_id=chat_id,
            transport=transport,
        )

    if bool(getattr(settings, "discord", False)):
        webhook = environ.get("QSX_DISCORD_WEBHOOK_URL", "").strip()
        parsed = urlsplit(webhook)
        if (
            parsed.scheme != "https"
            or parsed.hostname not in {"discord.com", "discordapp.com"}
            or parsed.username
            or parsed.password
        ):
            raise IntegrationConfigurationError(
                "Discord is enabled but its official HTTPS webhook is invalid"
            )
        publishers["discord"] = DiscordPublisher(
            webhook_url=webhook,
            transport=transport,
        )

    if bool(getattr(settings, "website", False)):
        url = environ.get("QSX_WEBSITE_EVENT_URL", "").strip()
        secret = environ.get("QSX_WEBSITE_EVENT_SECRET", "")
        if len(secret.encode("utf-8")) < 32:
            raise IntegrationConfigurationError(
                "Website events are enabled but the signing secret is too short"
            )
        try:
            publishers["website"] = SignedWebsiteWebhookPublisher(
                url=url,
                signing_secret=secret,
                transport=transport,
            )
        except ValueError as exc:
            raise IntegrationConfigurationError(
                "Website events are enabled but their HTTPS URL is invalid"
            ) from exc
    return MappingProxyType(publishers)


@dataclass(frozen=True)
class DispatcherHealth:
    status: str
    observed_at: float
    claimed: int = 0
    delivered: int = 0
    retrying: int = 0
    dead_lettered: int = 0
    errors: int = 0
    error_type: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "observed_at": self.observed_at,
            "claimed": self.claimed,
            "delivered": self.delivered,
            "retrying": self.retrying,
            "dead_lettered": self.dead_lettered,
            "errors": self.errors,
            "error_type": self.error_type,
        }


class IntegrationDispatcher:
    """Claims outbox work and contains every notification-side failure."""

    def __init__(
        self,
        store: OutboxStore,
        publishers: Mapping[str, Publisher],
        *,
        max_attempts: int = 5,
        batch_size: int = 50,
        max_concurrency: int = 4,
        base_delay_s: float = 2.0,
        max_delay_s: float = 300.0,
        jitter_ratio: float = 0.2,
        delivery_timeout_s: float = 15.0,
        clock: Callable[[], float] = time.time,
        random_fn: Callable[[], float] = random.random,
    ):
        self.store = store
        self.publishers = {
            str(name).casefold(): publisher
            for name, publisher in publishers.items()
        }
        if (
            isinstance(max_attempts, bool)
            or not isinstance(max_attempts, int)
            or not 1 <= max_attempts <= 100
        ):
            raise ValueError("max_attempts must be between 1 and 100")
        self.max_attempts = max_attempts
        self.batch_size = max(1, min(int(batch_size), 500))
        self.max_concurrency = max(1, min(int(max_concurrency), 32))
        base_delay = float(base_delay_s)
        max_delay = float(max_delay_s)
        jitter = float(jitter_ratio)
        if not math.isfinite(base_delay) or not 0.1 <= base_delay <= 3_600.0:
            raise ValueError("base_delay_s must be between 0.1 and 3600")
        if (
            not math.isfinite(max_delay)
            or not base_delay <= max_delay <= 3_600.0
        ):
            raise ValueError(
                "max_delay_s must be finite, at least base_delay_s, and at most 3600"
            )
        if not math.isfinite(jitter) or not 0.0 <= jitter <= 0.9:
            raise ValueError("jitter_ratio must be between 0 and 0.9")
        self.base_delay_s = base_delay
        self.max_delay_s = max_delay
        self.jitter_ratio = jitter
        self.delivery_timeout_s = max(
            0.1, min(float(delivery_timeout_s), 120.0)
        )
        store_lease = getattr(store, "lease_seconds", None)
        if (
            isinstance(store_lease, (int, float))
            and self.delivery_timeout_s + 1.0 >= float(store_lease)
        ):
            raise ValueError(
                "outbox lease_seconds must exceed delivery_timeout_s by more than one second"
            )
        self.clock = clock
        self.random_fn = random_fn
        self._latest_health = DispatcherHealth("STARTING", self.clock())

    @property
    def latest_health(self) -> DispatcherHealth:
        return self._latest_health

    def _retry_delay(
        self,
        attempt: int,
        *,
        remote_retry_after_s: float | None = None,
    ) -> float:
        exponent = max(0, min(int(attempt) - 1, 30))
        base = min(self.max_delay_s, self.base_delay_s * (2 ** exponent))
        random_value = float(self.random_fn())
        if not math.isfinite(random_value):
            random_value = 0.5
        random_value = max(0.0, min(1.0, random_value))
        factor = 1.0 + self.jitter_ratio * (2.0 * random_value - 1.0)
        local_delay = max(0.1, min(self.max_delay_s, base * factor))
        if (
            remote_retry_after_s is None
            or isinstance(remote_retry_after_s, bool)
            or not isinstance(remote_retry_after_s, (int, float))
            or not math.isfinite(float(remote_retry_after_s))
            or float(remote_retry_after_s) < 0
        ):
            return local_delay
        return min(
            self.max_delay_s,
            max(local_delay, float(remote_retry_after_s)),
        )

    async def dispatch_once(self) -> int:
        try:
            messages = list(await self.store.claim_due(
                now=self.clock(), limit=self.batch_size
            ))[: self.batch_size]
        except Exception as exc:
            log.error("Outbox claim failed: %s", type(exc).__name__)
            self._latest_health = DispatcherHealth(
                "ERROR", self.clock(), errors=1, error_type=type(exc).__name__
            )
            return 0

        semaphore = asyncio.Semaphore(self.max_concurrency)

        async def deliver(message: OutboxMessage) -> str:
            async with semaphore:
                try:
                    return await self._deliver_one(message)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.error(
                        "Outbox message isolation boundary caught: %s",
                        type(exc).__name__,
                    )
                    return f"ERROR:{type(exc).__name__}"

        outcomes = await asyncio.gather(*(deliver(message) for message in messages))
        delivered = outcomes.count("DELIVERED")
        retrying = outcomes.count("RETRY")
        dead_lettered = outcomes.count("DEAD_LETTER")
        failures = [item for item in outcomes if item.startswith("ERROR")]
        status = "ERROR" if failures else (
            "DEGRADED" if retrying or dead_lettered else "HEALTHY"
        )
        self._latest_health = DispatcherHealth(
            status=status,
            observed_at=self.clock(),
            claimed=len(messages),
            delivered=delivered,
            retrying=retrying,
            dead_lettered=dead_lettered,
            errors=len(failures),
            error_type=(failures[0].partition(":")[2] if failures else ""),
        )
        return len(messages)

    async def _deliver_one(self, message: OutboxMessage) -> str:
        attempt = message.attempts + 1
        if message.claim_error:
            stored = await self._safe_store_call(
                "dead_letter",
                self.store.mark_dead_letter(
                    message.message_id,
                    attempts=attempt,
                    failed_at=self.clock(),
                    error=message.claim_error[:500],
                ),
            )
            return "DEAD_LETTER" if stored else "ERROR:OutboxStoreError"
        publisher = self.publishers.get(message.destination)
        if publisher is None:
            unknown_destination = redact_text(str(message.destination))[:240]
            stored = await self._safe_store_call(
                "dead_letter",
                self.store.mark_dead_letter(
                    message.message_id,
                    attempts=attempt,
                    failed_at=self.clock(),
                    error=f"unknown destination: {unknown_destination}",
                ),
            )
            return "DEAD_LETTER" if stored else "ERROR:OutboxStoreError"

        try:
            detail = await asyncio.wait_for(
                publisher.publish(message.event), timeout=self.delivery_timeout_s
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            error = redact_text(f"{type(exc).__name__}: {exc}")[:500]
            retry_after_s = (
                exc.retry_after_s if isinstance(exc, DeliveryError) else None
            )
            if attempt >= self.max_attempts:
                stored = await self._safe_store_call(
                    "dead_letter",
                    self.store.mark_dead_letter(
                        message.message_id,
                        attempts=attempt,
                        failed_at=self.clock(),
                        error=error,
                    ),
                )
                outcome = "DEAD_LETTER"
            else:
                stored = await self._safe_store_call(
                    "retry",
                    self.store.mark_retry(
                        message.message_id,
                        attempts=attempt,
                        next_attempt_at=self.clock()
                        + self._retry_delay(
                            attempt,
                            remote_retry_after_s=retry_after_s,
                        ),
                        error=error,
                    ),
                )
                outcome = "RETRY"
            return outcome if stored else "ERROR:OutboxStoreError"

        stored = await self._safe_store_call(
            "delivered",
            self.store.mark_delivered(
                message.message_id,
                delivered_at=self.clock(),
                detail=redact_text(detail)[:240],
            ),
        )
        return "DELIVERED" if stored else "ERROR:OutboxStoreError"

    async def _safe_store_call(self, operation: str, awaitable: Any) -> bool:
        try:
            await awaitable
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("Outbox %s update failed: %s", operation, type(exc).__name__)
            return False

    async def run(self, *, poll_interval_s: float = 1.0) -> None:
        while True:
            try:
                await self.dispatch_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # final containment boundary
                log.error("Dispatcher iteration isolated: %s", type(exc).__name__)
            await asyncio.sleep(max(0.1, poll_interval_s))
