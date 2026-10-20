"""Explicit, fail-closed provider ports for macro and intermarket data."""

from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Mapping, Optional, Protocol, Sequence


class ProviderStatus(str, Enum):
    STARTING = "STARTING"
    HEALTHY = "HEALTHY"
    STALE = "STALE"
    ERROR = "ERROR"
    DISABLED = "DISABLED"


class ExecutionProtection(str, Enum):
    ALLOW = "ALLOW"
    BLOCK_NEW_ENTRIES = "BLOCK_NEW_ENTRIES"
    TECHNICAL_ONLY_UNPROTECTED = "TECHNICAL_ONLY_UNPROTECTED"
    DISABLE_DEPENDENT_AGENTS = "DISABLE_DEPENDENT_AGENTS"


class ProviderSafetyError(RuntimeError):
    """Raised when a test-only source is requested in a trading mode."""


def current_mode() -> str:
    return os.environ.get("QSX_MODE", "broker").strip().lower() or "broker"


def ensure_simulation_source_allowed(mode: Optional[str] = None) -> None:
    selected = (mode or current_mode()).strip().lower()
    if selected not in {"test", "simulation"}:
        raise ProviderSafetyError(
            f"Simulation provider is forbidden while QSX_MODE={selected!r}"
        )


@dataclass(frozen=True)
class ProviderSnapshot:
    provider: str
    source: str
    status: ProviderStatus
    source_timestamp: Optional[float]
    received_timestamp: float
    sequence: int
    freshness_s: float
    payload: Mapping[str, Any] = field(default_factory=dict)
    disabled_agents: tuple[int, ...] = ()
    execution_protection: ExecutionProtection = ExecutionProtection.ALLOW
    error: str = ""
    dependent_agents: tuple[int, ...] = ()
    unavailable_protection: ExecutionProtection = ExecutionProtection.ALLOW

    def age_s(self, now: Optional[float] = None) -> Optional[float]:
        if self.source_timestamp is None:
            return None
        return max(0.0, (time.time() if now is None else now) - self.source_timestamp)

    def is_fresh(self, now: Optional[float] = None) -> bool:
        age = self.age_s(now)
        return self.status is ProviderStatus.HEALTHY and age is not None and age <= self.freshness_s

    def effective_status(self, now: Optional[float] = None) -> ProviderStatus:
        if self.status is ProviderStatus.HEALTHY and not self.is_fresh(now):
            return ProviderStatus.STALE
        return self.status

    def effective_protection(
        self, now: Optional[float] = None
    ) -> ExecutionProtection:
        if self.effective_status(now) is ProviderStatus.HEALTHY:
            return ExecutionProtection.ALLOW
        if self.execution_protection is not ExecutionProtection.ALLOW:
            return self.execution_protection
        return self.unavailable_protection

    def effective_disabled_agents(self, now: Optional[float] = None) -> tuple[int, ...]:
        if self.effective_status(now) is ProviderStatus.HEALTHY:
            return ()
        return self.disabled_agents or self.dependent_agents

    def execution_eligible_at(self, now: Optional[float] = None) -> bool:
        if self.effective_status(now) is ProviderStatus.HEALTHY:
            return True
        return self.effective_protection(now) in {
            ExecutionProtection.TECHNICAL_ONLY_UNPROTECTED,
            ExecutionProtection.DISABLE_DEPENDENT_AGENTS,
        }

    @property
    def execution_eligible(self) -> bool:
        """Whether this provider state permits global entry evaluation.

        Intermarket outages disable only their dependent agents.  Macro outages
        block new entries unless an operator explicitly selected the visibly
        degraded, unprotected technical-only mode.
        """
        return self.execution_eligible_at()

    def degraded_at(self, now: Optional[float] = None) -> bool:
        return self.effective_status(now) is not ProviderStatus.HEALTHY

    @property
    def degraded(self) -> bool:
        return self.degraded_at()

    def as_dict(self, now: Optional[float] = None) -> dict[str, Any]:
        current = time.time() if now is None else now
        age_at_receive = (
            None
            if self.source_timestamp is None
            else max(0.0, self.received_timestamp - self.source_timestamp)
        )
        current_age = self.age_s(current)
        effective_status = self.effective_status(current)
        effective_protection = self.effective_protection(current)
        return {
            "provider": self.provider,
            "source": self.source,
            "provider_status": effective_status.value,
            "source_timestamp": self.source_timestamp,
            "received_timestamp": self.received_timestamp,
            "sequence": self.sequence,
            "freshness_s": self.freshness_s,
            "source_age_s": age_at_receive,
            "current_source_age_s": current_age,
            "fresh": self.is_fresh(current),
            "disabled_agents": list(self.effective_disabled_agents(current)),
            "execution_protection": effective_protection.value,
            "execution_eligible": self.execution_eligible_at(current),
            "provider_degraded": self.degraded_at(current),
            "provider_error": self.error,
        }


@dataclass(frozen=True)
class HTTPJSONResponse:
    status: int
    payload: Mapping[str, Any]


class HTTPJSONTransport(Protocol):
    async def get_json(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_s: float,
    ) -> HTTPJSONResponse: ...


class ProviderPort(Protocol):
    async def poll_once(self) -> ProviderSnapshot: ...


class AiohttpJSONTransport:
    """Small production transport; tests inject an in-memory implementation."""

    async def get_json(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_s: float,
    ) -> HTTPJSONResponse:
        try:
            import aiohttp
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise RuntimeError("aiohttp is required for HTTP providers") from exc

        timeout = aiohttp.ClientTimeout(total=timeout_s)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=dict(headers)) as response:
                payload = await response.json(content_type=None)
                if not isinstance(payload, Mapping):
                    raise ValueError("provider response must be a JSON object")
                return HTTPJSONResponse(status=response.status, payload=payload)


def _coerce_timestamp(value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError("boolean is not a timestamp")
    if isinstance(value, (int, float)):
        result = float(value)
    elif isinstance(value, str):
        text = value.strip()
        try:
            result = float(text)
        except ValueError:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            result = parsed.timestamp()
    else:
        raise ValueError("source_timestamp is required")
    if not math.isfinite(result) or result <= 0:
        raise ValueError("source_timestamp must be finite and positive")
    return result


class HTTPJSONProvider:
    """Base polling adapter with explicit health and freshness semantics."""

    def __init__(
        self,
        *,
        provider: str,
        url: Optional[str],
        source: str,
        freshness_s: float,
        disabled_agents: Sequence[int],
        unavailable_protection: ExecutionProtection,
        transport: Optional[HTTPJSONTransport] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout_s: float = 10.0,
        clock: Callable[[], float] = time.time,
    ):
        self.provider = provider
        self.url = (url or "").strip() or None
        self.source = _safe_source(source or "unconfigured")
        self.freshness_s = max(0.1, float(freshness_s))
        self.disabled_agents = tuple(sorted(set(int(a) for a in disabled_agents)))
        self.unavailable_protection = unavailable_protection
        self.transport = transport or AiohttpJSONTransport()
        self.headers = dict(headers or {})
        self.timeout_s = max(0.1, float(timeout_s))
        self.clock = clock
        self._sequence = 0
        self._last_source_timestamp: Optional[float] = None
        self._latest = self._make_snapshot(
            ProviderStatus.STARTING if self.url else ProviderStatus.DISABLED,
            source_timestamp=None,
        )

    @property
    def latest(self) -> ProviderSnapshot:
        return self._latest

    def runtime_health(self, now: Optional[float] = None) -> dict[str, Any]:
        """Return an age-aware view; callers must not gate on raw status alone."""
        current = self.clock() if now is None else now
        return self._latest.as_dict(current)

    def runtime_execution_eligible(self, now: Optional[float] = None) -> bool:
        current = self.clock() if now is None else now
        return self._latest.execution_eligible_at(current)

    def _make_snapshot(
        self,
        status: ProviderStatus,
        *,
        source_timestamp: Optional[float],
        payload: Optional[Mapping[str, Any]] = None,
        source: Optional[str] = None,
        error: str = "",
    ) -> ProviderSnapshot:
        unavailable = status is not ProviderStatus.HEALTHY
        return ProviderSnapshot(
            provider=self.provider,
            source=source or self.source,
            status=status,
            source_timestamp=source_timestamp,
            received_timestamp=self.clock(),
            sequence=self._sequence,
            freshness_s=self.freshness_s,
            payload=dict(payload or {}),
            disabled_agents=self.disabled_agents if unavailable else (),
            execution_protection=(
                self.unavailable_protection if unavailable else ExecutionProtection.ALLOW
            ),
            error=error[:240],
            dependent_agents=self.disabled_agents,
            unavailable_protection=self.unavailable_protection,
        )

    def normalize_payload(
        self,
        envelope: Mapping[str, Any],
        *,
        received_timestamp: Optional[float] = None,
    ) -> Mapping[str, Any]:
        raise NotImplementedError

    async def poll_once(self) -> ProviderSnapshot:
        self._sequence += 1
        if not self.url:
            self._latest = self._make_snapshot(
                ProviderStatus.DISABLED,
                source_timestamp=None,
                error="provider URL is not configured",
            )
            return self._latest

        try:
            response = await self.transport.get_json(
                self.url, headers=self.headers, timeout_s=self.timeout_s
            )
            received = self.clock()
            if not 200 <= response.status < 300:
                raise RuntimeError(f"HTTP {response.status}")
            envelope = response.payload
            source_timestamp = _coerce_timestamp(envelope.get("source_timestamp"))
            if source_timestamp > received + 30:
                raise ValueError("provider source timestamp is too far in the future")
            if (
                self._last_source_timestamp is not None
                and source_timestamp < self._last_source_timestamp
            ):
                raise ValueError("provider source timestamp moved backwards")
            normalized = self.normalize_payload(
                envelope, received_timestamp=received
            )
            status = (
                ProviderStatus.HEALTHY
                if received - source_timestamp <= self.freshness_s
                else ProviderStatus.STALE
            )
            self._latest = self._make_snapshot(
                status,
                source_timestamp=source_timestamp,
                payload=normalized,
                source=_safe_source(envelope.get("source") or self.source),
                error=(
                    "source data exceeded freshness limit"
                    if status is ProviderStatus.STALE else ""
                ),
            )
            self._last_source_timestamp = source_timestamp
        except Exception as exc:
            self._latest = self._make_snapshot(
                ProviderStatus.ERROR,
                source_timestamp=None,
                error=f"{type(exc).__name__}: provider poll failed",
            )
        return self._latest


def _safe_source(value: Any) -> str:
    source = str(value).strip()[:128]
    cleaned = "".join(
        character
        for character in source
        if character.isalnum() or character in "._:-"
    )
    return cleaned or "unknown"


def _optional_scalar(value: Any, field_name: str) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError(f"{field_name} must be a scalar value or null")
    return str(value)[:80]


class MacroHTTPProvider(HTTPJSONProvider):
    def __init__(
        self,
        url: Optional[str] = None,
        *,
        unsafe_technical_only: bool = False,
        source: str = "macro-http-json",
        freshness_s: float = 900.0,
        minimum_forward_coverage_s: float = 86_400.0,
        transport: Optional[HTTPJSONTransport] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout_s: float = 10.0,
        clock: Callable[[], float] = time.time,
    ):
        protection = (
            ExecutionProtection.TECHNICAL_ONLY_UNPROTECTED
            if unsafe_technical_only
            else ExecutionProtection.BLOCK_NEW_ENTRIES
        )
        coverage = float(minimum_forward_coverage_s)
        if not math.isfinite(coverage):
            raise ValueError("minimum_forward_coverage_s must be finite")
        self.minimum_forward_coverage_s = max(900.0, coverage)
        super().__init__(
            provider="macro",
            url=url,
            source=source,
            freshness_s=freshness_s,
            disabled_agents=(10, 14),
            unavailable_protection=protection,
            transport=transport,
            headers=headers,
            timeout_s=timeout_s,
            clock=clock,
        )

    def normalize_payload(
        self,
        envelope: Mapping[str, Any],
        *,
        received_timestamp: Optional[float] = None,
    ) -> Mapping[str, Any]:
        received = self.clock() if received_timestamp is None else received_timestamp
        coverage_start = _coerce_timestamp(envelope.get("coverage_start_utc"))
        coverage_end = _coerce_timestamp(envelope.get("coverage_end_utc"))
        if coverage_end <= coverage_start:
            raise ValueError("macro coverage end must follow coverage start")
        if not coverage_start <= received <= coverage_end:
            raise ValueError("macro coverage does not contain current time")
        if coverage_end - received < self.minimum_forward_coverage_s:
            raise ValueError("macro forward coverage is insufficient")
        events = envelope.get("events")
        if not isinstance(events, list):
            raise ValueError("macro provider must return an events list")
        normalized: list[dict[str, Any]] = []
        for item in events:
            if not isinstance(item, Mapping):
                raise ValueError("each macro event must be a JSON object")
            scheduled = _coerce_timestamp(item.get("scheduled_utc"))
            name = " ".join(str(item.get("name") or "").split())[:160]
            if not name:
                raise ValueError("macro event name is required")
            impact = str(item.get("impact") or "LOW").upper()
            if impact not in {"HIGH", "MEDIUM", "LOW"}:
                raise ValueError("macro impact must be HIGH, MEDIUM, or LOW")
            currency = str(item.get("currency") or "USD").upper()
            if not currency.isalpha() or not 3 <= len(currency) <= 5:
                raise ValueError("macro event currency is invalid")
            normalized.append({
                "name": name,
                "impact": impact,
                "scheduled_utc": scheduled,
                "currency": currency,
                "forecast": _optional_scalar(item.get("forecast"), "forecast"),
                "previous": _optional_scalar(item.get("previous"), "previous"),
                "actual": _optional_scalar(item.get("actual"), "actual"),
            })
        return {
            "events": normalized,
            "coverage_start_utc": coverage_start,
            "coverage_end_utc": coverage_end,
            "minimum_forward_coverage_s": self.minimum_forward_coverage_s,
        }


class IntermarketHTTPProvider(HTTPJSONProvider):
    REQUIRED_FIELDS = ("dxy", "us10y", "us30", "gold")

    def __init__(
        self,
        url: Optional[str] = None,
        *,
        source: str = "intermarket-http-json",
        freshness_s: float = 15.0,
        transport: Optional[HTTPJSONTransport] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout_s: float = 5.0,
        clock: Callable[[], float] = time.time,
    ):
        super().__init__(
            provider="intermarket",
            url=url,
            source=source,
            freshness_s=freshness_s,
            disabled_agents=(12, 15),
            unavailable_protection=ExecutionProtection.DISABLE_DEPENDENT_AGENTS,
            transport=transport,
            headers=headers,
            timeout_s=timeout_s,
            clock=clock,
        )

    def normalize_payload(
        self,
        envelope: Mapping[str, Any],
        *,
        received_timestamp: Optional[float] = None,
    ) -> Mapping[str, Any]:
        del received_timestamp
        values = envelope.get("data", envelope)
        if not isinstance(values, Mapping):
            raise ValueError("intermarket data must be a JSON object")
        normalized: dict[str, float] = {}
        for field_name in self.REQUIRED_FIELDS:
            raw = values.get(field_name)
            if isinstance(raw, bool):
                raise ValueError(f"{field_name} must be numeric")
            value = float(raw)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{field_name} must be finite and positive")
            normalized[field_name] = value
        return normalized


class SimulationProvider:
    """Explicit supplied-data port for tests/simulations; it never fabricates data."""

    def __init__(
        self,
        provider: str,
        payload: Mapping[str, Any],
        *,
        source: str = "simulation-supplied",
        freshness_s: float = 60.0,
        mode: Optional[str] = None,
        clock: Callable[[], float] = time.time,
    ):
        ensure_simulation_source_allowed(mode)
        self.provider = provider
        self.payload = dict(payload)
        self.source = source
        self.freshness_s = freshness_s
        self.clock = clock
        self._sequence = 0

    async def poll_once(self) -> ProviderSnapshot:
        self._sequence += 1
        now = self.clock()
        return ProviderSnapshot(
            provider=self.provider,
            source=self.source,
            status=ProviderStatus.HEALTHY,
            source_timestamp=now,
            received_timestamp=now,
            sequence=self._sequence,
            freshness_s=self.freshness_s,
            payload=dict(self.payload),
        )
