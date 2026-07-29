"""Provider polling and exact broker-derived decision snapshot assembly.

This module is the Agent-02/14/15 application seam.  It never fabricates a
quote, bar, account value, macro event, or intermarket observation.  Missing,
stale, conflicting, or incomplete evidence raises a blocking assembly error.
Importing it performs no I/O.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any, Mapping, Sequence

from .broker_gateway import BrokerTick, CompletedBar
from .domain_models import (
    AccountMode,
    AccountSnapshot,
    BrokerPosition,
    BrokerSnapshot,
    Direction,
    LifecycleActionKind,
    LifecycleStage,
    Milestone,
    QuoteSnapshot,
    SymbolSnapshot,
)
from .provider_adapters import ProviderPort, ProviderSnapshot, ProviderStatus
from .reconciliation import AuthoritativeBrokerSnapshot
from .strategy_pipeline import (
    Bar,
    CanonicalMarketSnapshot,
    DataMode,
    EvidenceSource,
    IntermarketContext,
    MacroContext,
    MarketSource,
    intermarket_context_from_provider_snapshots,
    macro_context_from_provider_snapshot,
)
from .trade_ledger import ProviderHealthRecord, TradeLedger


TIMEFRAME_MS: Mapping[str, int] = {
    "M1": 60_000,
    "M5": 5 * 60_000,
    "M15": 15 * 60_000,
    "H1": 60 * 60_000,
    "H4": 4 * 60 * 60_000,
    "D1": 24 * 60 * 60_000,
}
MINIMUM_BARS: Mapping[str, int] = {
    "M1": 2,
    "M5": 21,
    "M15": 16,
    "H1": 55,
    "H4": 20,
    "D1": 20,
}
MAXIMUM_LAST_BAR_AGE_MS: Mapping[str, int] = {
    "M1": 5 * 60_000,
    "M5": 15 * 60_000,
    "M15": 45 * 60_000,
    "H1": 3 * 60 * 60_000,
    "H4": 12 * 60 * 60_000,
    "D1": 4 * 24 * 60 * 60_000,
}


class SnapshotAssemblyError(RuntimeError):
    """Decision evidence is incomplete, stale, or scope-conflicting."""


@dataclass(frozen=True, slots=True)
class ProviderContexts:
    macro: MacroContext | None
    intermarket: IntermarketContext | None
    macro_snapshot: ProviderSnapshot
    intermarket_snapshot: ProviderSnapshot
    macro_evidence_snapshot: ProviderSnapshot | None = None

    def refreshed(self, *, at_ms: int) -> "ProviderContexts":
        """Re-evaluate time-sensitive macro freeze state at decision time.

        Provider polling establishes source lineage and freshness evidence, but
        a high-impact event can enter its pre-event freeze window between two
        polls.  Rebuilding from the retained immutable calendar schedule on
        every tick closes that time-of-check/time-of-use gap without inventing
        provider data.
        """

        evidence = self.macro_evidence_snapshot
        if evidence is None and self.macro_snapshot.payload:
            evidence = self.macro_snapshot
        macro = self.macro
        if evidence is not None:
            latest = self.macro_snapshot
            effective = evidence
            if latest is not evidence:
                effective = replace(
                    evidence,
                    status=latest.status,
                    received_timestamp=latest.received_timestamp,
                    sequence=latest.sequence,
                    error=latest.error,
                )
            try:
                macro = macro_context_from_provider_snapshot(
                    effective,
                    source_kind=(
                        self.macro.source_kind
                        if self.macro is not None
                        else EvidenceSource.HTTP
                    ),
                    at_ms=at_ms,
                )
            except (TypeError, ValueError):
                macro = None
        return ProviderContexts(
            macro,
            self.intermarket,
            self.macro_snapshot,
            self.intermarket_snapshot,
            evidence,
        )


class ProviderCoordinator:
    """Poll genuine configured provider ports and durably publish health."""

    def __init__(
        self,
        *,
        macro: ProviderPort,
        intermarket: ProviderPort,
        ledger: TradeLedger,
    ) -> None:
        if not isinstance(ledger, TradeLedger):
            raise TypeError("provider coordinator requires TradeLedger")
        self.macro = macro
        self.intermarket = intermarket
        self.ledger = ledger
        self._last_macro_evidence: ProviderSnapshot | None = None
        self._healthy_intermarket: list[ProviderSnapshot] = []
        self._latest_macro: ProviderSnapshot | None = None
        self._latest_intermarket: ProviderSnapshot | None = None

    async def poll_once(self, *, at_ms: int) -> ProviderContexts:
        macro, intermarket = await asyncio.gather(
            self.macro.poll_once(), self.intermarket.poll_once()
        )
        self._latest_macro = macro
        self._latest_intermarket = intermarket
        self._record_health(macro, at_ms)
        self._record_health(intermarket, at_ms)

        if macro.payload and macro.source_timestamp is not None:
            self._last_macro_evidence = macro

        if intermarket.effective_status(at_ms / 1_000) is not ProviderStatus.HEALTHY:
            # An outage, stale response, validation error, or disabled provider
            # breaks observation continuity. A recovered feed must provide two
            # fresh ordered observations before correlation context is rebuilt.
            self._healthy_intermarket.clear()
        elif intermarket.source_timestamp is not None and intermarket.payload:
            if not self._healthy_intermarket or (
                intermarket.sequence > self._healthy_intermarket[-1].sequence
                and intermarket.source_timestamp
                > (self._healthy_intermarket[-1].source_timestamp or 0)
            ):
                self._healthy_intermarket.append(intermarket)
                self._healthy_intermarket = self._healthy_intermarket[-2:]
        return self.contexts(at_ms=at_ms)

    def contexts(self, *, at_ms: int) -> ProviderContexts:
        if self._latest_macro is None or self._latest_intermarket is None:
            raise SnapshotAssemblyError("providers have not completed an initial poll")
        macro_context: MacroContext | None = None
        macro_evidence = self._last_macro_evidence
        if macro_evidence is not None:
            if self._latest_macro is not macro_evidence:
                macro_evidence = replace(
                    macro_evidence,
                    status=self._latest_macro.status,
                    received_timestamp=self._latest_macro.received_timestamp,
                    sequence=self._latest_macro.sequence,
                    error=self._latest_macro.error,
                )
            try:
                macro_context = macro_context_from_provider_snapshot(
                    macro_evidence,
                    source_kind=EvidenceSource.HTTP,
                    at_ms=at_ms,
                )
            except (TypeError, ValueError):
                macro_context = None

        intermarket_context: IntermarketContext | None = None
        if (
            self._latest_intermarket.status is ProviderStatus.HEALTHY
            and len(self._healthy_intermarket) == 2
            and self._healthy_intermarket[-1] is self._latest_intermarket
        ):
            try:
                intermarket_context = intermarket_context_from_provider_snapshots(
                    self._healthy_intermarket[0],
                    self._healthy_intermarket[1],
                    source_kind=EvidenceSource.HTTP,
                )
            except (TypeError, ValueError):
                intermarket_context = None
        return ProviderContexts(
            macro_context,
            intermarket_context,
            self._latest_macro,
            self._latest_intermarket,
            macro_evidence,
        )

    def _record_health(self, snapshot: ProviderSnapshot, at_ms: int) -> None:
        observed = at_ms / 1_000
        state = snapshot.effective_status(observed).value
        material = {
            "provider": snapshot.provider,
            "source": snapshot.source,
            "sequence": snapshot.sequence,
            "received_timestamp": snapshot.received_timestamp,
            "state": state,
        }
        observation_id = "provider-" + hashlib.sha256(
            json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:32]
        self.ledger.record_provider_health(
            ProviderHealthRecord(
                observation_id=observation_id,
                provider_name=snapshot.provider,
                state=state,
                observed_at=observed,
                source_timestamp=snapshot.source_timestamp,
                detail=snapshot.as_dict(observed),
            )
        )


@dataclass(frozen=True, slots=True)
class MarketSnapshotBundle:
    market: CanonicalMarketSnapshot
    broker: BrokerSnapshot


@dataclass(frozen=True, slots=True)
class MarketSnapshotAssemblerConfig:
    mode: DataMode
    maximum_tick_age_ms: int = 5_000
    maximum_account_age_ms: int = 5_000
    maximum_future_skew_ms: int = 1_000

    def __post_init__(self) -> None:
        object.__setattr__(self, "mode", DataMode(self.mode))
        if self.mode not in {DataMode.SHADOW, DataMode.DEMO, DataMode.LIVE}:
            raise ValueError("broker snapshot assembly is only for shadow/demo/live")
        for name in (
            "maximum_tick_age_ms",
            "maximum_account_age_ms",
            "maximum_future_skew_ms",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} cannot be negative")


class MarketSnapshotAssembler:
    """Build market and broker policy inputs from one admitted broker tick."""

    def __init__(self, config: MarketSnapshotAssemblerConfig) -> None:
        self.config = config

    def assemble(
        self,
        *,
        now_ms: int,
        connection_epoch: str,
        account_health: Mapping[str, Any],
        symbol: SymbolSnapshot,
        tick: BrokerTick,
        completed_bars: Sequence[CompletedBar],
        authoritative: AuthoritativeBrokerSnapshot,
        reconciliation_succeeded: bool,
        active_quarantine: bool,
        providers: ProviderContexts,
        owned_positions: Sequence[Any] | None = None,
        pending_order_count: int | None = None,
    ) -> MarketSnapshotBundle:
        if not reconciliation_succeeded or active_quarantine:
            raise SnapshotAssemblyError("broker reconciliation is not clean")
        if (
            not connection_epoch
            or authoritative.identity.connection_epoch != connection_epoch
            or tick.connection_epoch != connection_epoch
        ):
            raise SnapshotAssemblyError("connection epoch evidence differs")
        expected = authoritative.identity
        if (
            expected.symbol != symbol.symbol
            or expected.symbol != tick.symbol
            or account_health.get("account_id") != expected.account_id
            or account_health.get("symbol") != expected.symbol
        ):
            raise SnapshotAssemblyError("broker snapshot scope differs")
        tick_age = now_ms - tick.received_at_ms
        account_received = account_health.get("received_at_ms")
        if isinstance(account_received, bool) or not isinstance(account_received, int):
            raise SnapshotAssemblyError("account heartbeat lacks receive timestamp")
        account_age = now_ms - account_received
        if (
            tick_age < -self.config.maximum_future_skew_ms
            or tick_age > self.config.maximum_tick_age_ms
            or account_age < -self.config.maximum_future_skew_ms
            or account_age > self.config.maximum_account_age_ms
        ):
            raise SnapshotAssemblyError("tick or account heartbeat is stale/future")
        position_evidence = tuple(
            authoritative.positions if owned_positions is None else owned_positions
        )
        effective_pending_count = (
            len(authoritative.pending_orders)
            if pending_order_count is None
            else pending_order_count
        )
        if (
            str(account_health.get("risk_day")) != authoritative.risk.risk_day
            or int(account_health.get("owned_positions", -1))
            != len(position_evidence)
            or int(account_health.get("owned_pending_orders", -1))
            != effective_pending_count
        ):
            raise SnapshotAssemblyError("heartbeat counts/risk day require reconciliation")
        account_payload = authoritative.payload.get("account")
        if not isinstance(account_payload, Mapping):
            raise SnapshotAssemblyError("authoritative account metadata is unavailable")
        if not isinstance(account_health.get("baseline_verified"), bool) or not isinstance(
            account_health.get("daily_halted"), bool
        ):
            raise SnapshotAssemblyError("account risk flags are not boolean")
        try:
            account_mode = AccountMode(str(account_payload["account_mode"]))
            currency = str(account_payload["currency"])
            account = AccountSnapshot(
                account_id=expected.account_id,
                currency=currency,
                mode=account_mode,
                balance=account_health["balance"],
                equity=account_health["equity"],
                free_margin=account_health["free_margin"],
                risk_day=str(account_health["risk_day"]),
                day_start_equity=authoritative.risk.day_start_equity,
                baseline_verified=bool(account_health["baseline_verified"]),
                daily_halted=bool(account_health["daily_halted"]),
                captured_at_ms=account_received,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise SnapshotAssemblyError("account heartbeat values are invalid") from exc
        quote = QuoteSnapshot(
            symbol=expected.symbol,
            bid=tick.bid,
            ask=tick.ask,
            captured_at_ms=tick.received_at_ms,
        )
        positions = tuple(self._position(item) for item in position_evidence)
        bars = self._bars(
            completed_bars,
            symbol=expected.symbol,
            captured_at_ms=tick.received_at_ms,
        )
        snapshot_id = f"market:{connection_epoch}:{tick.msg_id}"
        broker = BrokerSnapshot(
            snapshot_id=snapshot_id,
            connection_id=connection_epoch,
            account=account,
            symbol=symbol,
            quote=quote,
            positions=positions,
            captured_at_ms=tick.received_at_ms,
            reconciled=True,
        )
        market = CanonicalMarketSnapshot(
            snapshot_id=snapshot_id,
            symbol=expected.symbol,
            captured_at_ms=tick.received_at_ms,
            mode=self.config.mode,
            market_source=MarketSource.BROKER,
            bid=tick.bid,
            ask=tick.ask,
            tick_size=symbol.tick_size,
            m1=bars["M1"],
            m5=bars["M5"],
            m15=bars["M15"],
            h1=bars["H1"],
            h4=bars["H4"],
            d1=bars["D1"],
            macro=providers.macro,
            intermarket=providers.intermarket,
        )
        return MarketSnapshotBundle(market, broker)

    @staticmethod
    def _position(item: Any) -> BrokerPosition:
        def value(name: str) -> Any:
            return item[name] if isinstance(item, Mapping) else getattr(item, name)

        if value("trade_id") is None or value("lifecycle_stage") == LifecycleStage.QUARANTINED.value:
            raise SnapshotAssemblyError("unattributed position cannot enter risk snapshot")
        try:
            return BrokerPosition(
                trade_id=value("trade_id"),
                broker_position_id=value("broker_position_id"),
                position_ticket=value("position_ticket"),
                symbol=value("symbol"),
                magic=value("magic"),
                direction=Direction(value("direction")),
                initial_volume=value("initial_volume"),
                remaining_volume=value("remaining_volume"),
                entry_price=value("entry_price"),
                original_sl=value("original_sl"),
                current_sl=value("current_sl"),
                tp1=value("tp1"),
                tp2=value("tp2"),
                tp3=value("tp3"),
                initial_risk_amount=value("initial_risk_amount"),
                lifecycle_stage=LifecycleStage(value("lifecycle_stage")),
                position_version=value("position_version"),
                opened_at_ms=value("opened_at_ms"),
                realised_pnl=value("realised_pnl"),
                pending_action=(
                    None
                    if value("pending_action") is None
                    else LifecycleActionKind(value("pending_action"))
                ),
                pending_milestone=(
                    None
                    if value("pending_milestone") is None
                    else Milestone(value("pending_milestone"))
                ),
                pending_command_id=value("pending_command_id"),
                pending_close_volume=value("pending_close_volume"),
                pending_filled_volume=value("pending_filled_volume"),
            )
        except (TypeError, ValueError) as exc:
            raise SnapshotAssemblyError("canonical broker position is invalid") from exc

    @staticmethod
    def _bars(
        values: Sequence[CompletedBar], *, symbol: str, captured_at_ms: int
    ) -> dict[str, tuple[Bar, ...]]:
        grouped: dict[str, list[CompletedBar]] = {key: [] for key in TIMEFRAME_MS}
        seen: set[tuple[str, int]] = set()
        for item in values:
            if item.symbol != symbol or item.timeframe not in grouped:
                raise SnapshotAssemblyError("completed bar is outside canonical scope")
            key = (item.timeframe, item.open_time_ms)
            if key in seen:
                raise SnapshotAssemblyError("duplicate completed bar identity")
            seen.add(key)
            grouped[item.timeframe].append(item)
        result: dict[str, tuple[Bar, ...]] = {}
        for timeframe, items in grouped.items():
            ordered = sorted(items, key=lambda item: item.open_time_ms)
            if len(ordered) < MINIMUM_BARS[timeframe]:
                raise SnapshotAssemblyError(f"insufficient {timeframe} completed bars")
            selected = ordered[-5_000:]
            prior_open = -1
            for item in selected:
                if (
                    item.open_time_ms <= prior_open
                    or item.close_time_ms - item.open_time_ms != TIMEFRAME_MS[timeframe]
                    or item.close_time_ms > captured_at_ms
                ):
                    raise SnapshotAssemblyError(
                        f"{timeframe} bar chronology/completeness is invalid"
                    )
                prior_open = item.open_time_ms
            last_age = captured_at_ms - selected[-1].close_time_ms
            if last_age < 0 or last_age > MAXIMUM_LAST_BAR_AGE_MS[timeframe]:
                raise SnapshotAssemblyError(f"latest {timeframe} bar is stale/future")
            result[timeframe] = tuple(
                Bar(
                    open_time_ms=item.open_time_ms,
                    close_time_ms=item.close_time_ms,
                    open=item.open,
                    high=item.high,
                    low=item.low,
                    close=item.close,
                    volume=Decimal(item.volume),
                )
                for item in selected
            )
        return result


__all__ = [
    "MarketSnapshotAssembler",
    "MarketSnapshotAssemblerConfig",
    "MarketSnapshotBundle",
    "ProviderContexts",
    "ProviderCoordinator",
    "SnapshotAssemblyError",
]
