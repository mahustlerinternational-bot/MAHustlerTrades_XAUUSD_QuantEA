"""Canonical, import-safe composition root for QUANT-SWARM-XAU.

Only ``check``, ``shadow`` and ``demo`` are launchable here.  Configuration,
filesystem ownership, broker identity, runtime readiness and reconciliation
are explicit boundaries; importing this module performs no I/O.  The
``RuntimeApp`` exposes small startup, one-signal and shutdown methods so tests
do not need to drive an opaque forever loop.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import signal
from dataclasses import asdict, dataclass, is_dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum, StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Awaitable, Callable, Mapping, Protocol, Sequence

from .broker_gateway import (
    BrokerGateway,
    BrokerObservedDeal,
    BrokerObservedPosition,
    BrokerTick,
    CompletedBar,
    DurableDealAttribution,
    DurablePositionContext,
    GatewayConfig,
    GatewayError,
    GatewaySignal,
    GatewaySignalType,
)
from .domain_models import (
    BrokerEvent,
    BrokerEventType,
    Clock,
    SymbolSnapshot,
    SystemClock,
)
from .execution_service import ExecutionService, OutboundCommand
from .integration_dispatcher import (
    DomainEvent,
    IntegrationDispatcher,
    LedgerOutboxStore,
    build_publishers,
    outbox_records_for_event,
)
from .learning_service import GovernedLearningService
from .lifecycle_service import LifecycleService
from .market_snapshot_service import ProviderContexts, ProviderCoordinator
from .provider_adapters import (
    IntermarketHTTPProvider,
    MacroHTTPProvider,
    ProviderSnapshot,
)
from .reconciliation import (
    ReconciliationReport,
    ReconciliationService,
    BrokerSnapshotError,
)
from .runtime_config import (
    AccountTradeMode,
    ProviderSettings,
    ProviderSource,
    RuntimeConfig,
    RuntimeMode,
    load_runtime_config,
)
from .runtime_paths import ProcessLock, RuntimePaths
from .runtime_state import (
    BrokerIdentity,
    BrokerTradeMode,
    ControlPlaneState,
    ExpectedBrokerIdentity,
    RuntimeStateMachine,
)
from .runtime_scheduler import (
    CanonicalTickScheduler,
    LearningRuntimeStatus,
    TickCycleResult,
    TickSchedulerError,
)
from .trade_ledger import BrokerDealRecord, TradeEventRecord, TradeLedger
from .trading_policies import LifecyclePolicy
from .version import PRODUCT_NAME, PRODUCT_VERSION


SUPPORTED_LAUNCH_MODES = frozenset(
    {RuntimeMode.CHECK, RuntimeMode.SHADOW, RuntimeMode.DEMO}
)
log = logging.getLogger("QUANT-SWARM-RUNTIME")
_DEAL_EVENT_TYPES = frozenset(
    {
        BrokerEventType.POSITION_OPENED,
        BrokerEventType.PARTIAL_FILLED,
        BrokerEventType.POSITION_CLOSED,
    }
)


class RuntimeAppError(RuntimeError):
    """Base composition/startup/runtime failure."""


class RuntimeModeError(RuntimeAppError):
    """The CLI mode is unsupported or differs from the file's exact mode."""


class RuntimeCapabilityError(RuntimeAppError):
    """An operation is outside the selected mode's capability envelope."""


class RuntimeIngressError(RuntimeAppError):
    """Broker transaction evidence is missing, conflicting, or unconsumed."""


class AppPhase(StrEnum):
    NEW = "NEW"
    STARTING = "STARTING"
    CHECKED = "CHECKED"
    WAITING_EA = "WAITING_EA"
    SYNCHRONIZED = "SYNCHRONIZED"
    DEGRADED = "DEGRADED"
    DISCONNECTED = "DISCONNECTED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"


class LedgerPort(Protocol):
    def open(self) -> Any: ...

    def require_healthy(self) -> None: ...

    def close(self) -> None: ...


class GatewayRuntimePort(Protocol):
    @property
    def connection_epoch(self) -> str | None: ...

    @property
    def latest_account_health(self) -> Mapping[str, Any] | None: ...

    @property
    def latest_symbol_snapshot(self) -> SymbolSnapshot | None: ...

    @property
    def completed_bars(self) -> tuple[CompletedBar, ...]: ...

    async def start(self) -> None: ...

    async def close(self) -> None: ...

    async def next_signal(self, timeout: float | None = None) -> GatewaySignal: ...

    async def next_tick(self, timeout: float | None = None) -> BrokerTick: ...

    async def check_health(self) -> None: ...

    async def send(self, command: OutboundCommand) -> None: ...


class ProviderRuntimePort(Protocol):
    async def poll_once(self, *, at_ms: int) -> ProviderContexts: ...


class ReconciliationRuntimePort(Protocol):
    async def reconcile(
        self, expected: ExpectedBrokerIdentity
    ) -> ReconciliationReport: ...


class BrokerTransactionConsumer(Protocol):
    """Consumes one exact event/deal pair; it must never infer economics."""

    @property
    def execution_ready(self) -> bool: ...

    async def ingest(
        self, event: BrokerEvent, deal: BrokerDealRecord | None
    ) -> Any: ...

    def ingest_command_received(self, signal: GatewaySignal) -> bool: ...

    def ingest_command_rejected(self, signal: GatewaySignal) -> bool: ...


class CommandProducerPort(Protocol):
    @property
    def execution_ready(self) -> bool: ...

    @property
    def learning_status(self) -> LearningRuntimeStatus: ...

    async def next_tick(self, timeout: float | None = None) -> BrokerTick: ...

    async def process_tick(
        self,
        tick: BrokerTick,
        *,
        providers: ProviderContexts,
        reconciliation: ReconciliationReport,
    ) -> TickCycleResult: ...

    def expire_sent_commands(self) -> tuple[str, ...]: ...

    async def process_learning_due(self) -> tuple[Any, ...]: ...


class DispatcherPort(Protocol):
    async def run(self, *, poll_interval_s: float = 1.0) -> None: ...


class RuntimeComponentFactory(Protocol):
    def make_clock(self) -> Clock: ...

    def assert_deployment_integrity(
        self, *, config_path: Path, config: RuntimeConfig
    ) -> None: ...

    def make_ledger(self, *, paths: RuntimePaths, config: RuntimeConfig) -> LedgerPort: ...

    def make_providers(
        self,
        *,
        config: RuntimeConfig,
        environ: Mapping[str, str],
        ledger: LedgerPort,
        clock: Clock,
    ) -> ProviderRuntimePort: ...

    def make_gateway(
        self,
        *,
        config: RuntimeConfig,
        config_hash: str,
        environ: Mapping[str, str],
        ledger: LedgerPort,
        runtime: RuntimeStateMachine,
        clock: Clock,
        tuple_lock_acquirer: Callable[[BrokerIdentity], bool],
    ) -> GatewayRuntimePort: ...

    def make_reconciler(
        self,
        *,
        config: RuntimeConfig,
        ledger: LedgerPort,
        runtime: RuntimeStateMachine,
        gateway: GatewayRuntimePort,
        clock: Clock,
    ) -> ReconciliationRuntimePort: ...

    def make_transaction_consumer(
        self,
        *,
        config: RuntimeConfig,
        ledger: LedgerPort,
        runtime: RuntimeStateMachine,
        gateway: GatewayRuntimePort,
        clock: Clock,
    ) -> BrokerTransactionConsumer: ...

    def make_command_producer(
        self,
        *,
        config: RuntimeConfig,
        config_hash: str,
        ledger: LedgerPort,
        runtime: RuntimeStateMachine,
        gateway: GatewayRuntimePort,
        transaction_consumer: BrokerTransactionConsumer,
        clock: Clock,
    ) -> CommandProducerPort: ...

    def make_dispatcher(
        self,
        *,
        config: RuntimeConfig,
        config_hash: str,
        environ: Mapping[str, str],
        ledger: LedgerPort,
        clock: Clock,
    ) -> DispatcherPort | None: ...


class CanonicalBrokerTransactionConsumer:
    """Route exact gateway transactions through authoritative services."""

    def __init__(
        self,
        *,
        execution: ExecutionService,
        lifecycle: LifecycleService,
        account_id: str,
        symbol: str,
        magic: int,
    ) -> None:
        self._execution = execution
        self._lifecycle = lifecycle
        self._account_id = account_id
        self._symbol = symbol
        self._magic = magic

    @property
    def execution_ready(self) -> bool:
        return True

    @property
    def execution(self) -> ExecutionService:
        return self._execution

    @property
    def lifecycle(self) -> LifecycleService:
        return self._lifecycle

    async def ingest(
        self, event: BrokerEvent, deal: BrokerDealRecord | None
    ) -> Any:
        if event.event_type is BrokerEventType.POSITION_OPENED:
            if deal is None:
                raise RuntimeIngressError("POSITION_OPENED requires its broker deal")
            return self._execution.ingest_open_transaction(
                event,
                account_id=self._account_id,
                symbol=self._symbol,
                magic=self._magic,
                deal=deal,
            )
        return self._lifecycle.ingest_broker_event(
            event,
            account_id=self._account_id,
            symbol=self._symbol,
            magic=self._magic,
            deal=deal,
        )

    def ingest_command_received(self, signal: GatewaySignal) -> bool:
        command_id, _trade_id, payload = self._validated_command_signal(
            signal, GatewaySignalType.COMMAND_ACK
        )
        return self._execution.ingest_command_received(
            command_id,
            received_at_ms=signal.occurred_at_ms,
            evidence=payload,
        )

    def ingest_command_rejected(self, signal: GatewaySignal) -> bool:
        command_id, trade_id, payload = self._validated_command_signal(
            signal, GatewaySignalType.COMMAND_NACK
        )
        if payload.get("status") != "REJECTED":
            raise RuntimeIngressError("COMMAND_NACK status is not authoritative REJECTED")
        message_id = payload.get("broker_message_id")
        message_ts = payload.get("broker_message_ts_ms")
        if (
            not isinstance(message_id, str)
            or not message_id
            or isinstance(message_ts, bool)
            or not isinstance(message_ts, int)
            or message_ts < 0
        ):
            raise RuntimeIngressError(
                "COMMAND_NACK lacks authenticated message identity/timestamp"
            )
        reason_evidence = {
            "action": payload.get("action"),
            "reason": payload.get("reason"),
            "retcode": payload.get("retcode"),
            "retcode_desc": payload.get("retcode_desc"),
            "status": payload.get("status"),
        }
        if any(value is None for value in reason_evidence.values()):
            raise RuntimeIngressError("COMMAND_NACK lacks exact rejection evidence")
        event = BrokerEvent(
            event_id=f"nack:{signal.connection_epoch}:{message_id}",
            event_type=BrokerEventType.COMMAND_REJECTED,
            broker_time_ms=message_ts,
            received_at_ms=signal.occurred_at_ms,
            command_id=command_id,
            trade_id=trade_id,
            reason=json.dumps(
                reason_evidence,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ),
        )
        return self._execution.ingest_broker_event(
            event,
            account_id=self._account_id,
            symbol=self._symbol,
            magic=self._magic,
        )

    @staticmethod
    def _validated_command_signal(
        signal: GatewaySignal, expected: GatewaySignalType
    ) -> tuple[str, str, Mapping[str, Any]]:
        if signal.signal_type is not expected:
            raise RuntimeIngressError("command acknowledgement type differs")
        payload = signal.payload
        command_id = signal.command_id
        trade_id = signal.trade_id
        if (
            not signal.connection_epoch
            or not isinstance(command_id, str)
            or not command_id
            or not isinstance(trade_id, str)
            or not trade_id
            or payload.get("command_id") != command_id
            or payload.get("trade_id") != trade_id
        ):
            raise RuntimeIngressError(
                "command acknowledgement identity is incomplete or conflicting"
            )
        return command_id, trade_id, payload


class DefaultRuntimeComponentFactory:
    """Build production components without performing work at import time."""

    def make_clock(self) -> Clock:
        return SystemClock()

    def assert_deployment_integrity(
        self, *, config_path: Path, config: RuntimeConfig
    ) -> None:
        # Imported lazily so CHECK remains usable while deployment tooling is
        # optional in developer workspaces.
        from .setup_check import assert_broker_deployment_integrity

        assert_broker_deployment_integrity(
            config_path=config_path,
            mode=config.mode,
        )

    def make_ledger(
        self, *, paths: RuntimePaths, config: RuntimeConfig
    ) -> TradeLedger:
        return TradeLedger(
            paths.trade_ledger_path,
            busy_timeout_ms=config.storage.sqlite_busy_timeout_ms,
        )

    def make_providers(
        self,
        *,
        config: RuntimeConfig,
        environ: Mapping[str, str],
        ledger: LedgerPort,
        clock: Clock,
    ) -> ProviderCoordinator:
        if not isinstance(ledger, TradeLedger):
            raise RuntimeAppError("default providers require the canonical TradeLedger")
        wall_clock = lambda: clock.now_ms() / 1_000
        macro = MacroHTTPProvider(
            url=_provider_url(config.providers.macro),
            unsafe_technical_only=(
                config.providers.unsafe_allow_technical_without_macro
            ),
            source=_provider_source_name(config.providers.macro, "macro"),
            freshness_s=config.providers.macro.max_age_s,
            headers=_provider_headers(config.providers.macro, environ),
            clock=wall_clock,
        )
        intermarket = IntermarketHTTPProvider(
            url=_provider_url(config.providers.intermarket),
            source=_provider_source_name(
                config.providers.intermarket, "intermarket"
            ),
            freshness_s=config.providers.intermarket.max_age_s,
            headers=_provider_headers(config.providers.intermarket, environ),
            clock=wall_clock,
        )
        return ProviderCoordinator(
            macro=macro,
            intermarket=intermarket,
            ledger=ledger,
        )

    def make_gateway(
        self,
        *,
        config: RuntimeConfig,
        config_hash: str,
        environ: Mapping[str, str],
        ledger: LedgerPort,
        runtime: RuntimeStateMachine,
        clock: Clock,
        tuple_lock_acquirer: Callable[[BrokerIdentity], bool],
    ) -> BrokerGateway:
        if not isinstance(ledger, TradeLedger):
            raise RuntimeAppError("default gateway requires the canonical TradeLedger")
        expected = _expected_identity(config)
        secret_name = config.ipc.auth_secret_env
        secret = environ.get(secret_name or "", "")
        gateway_config = GatewayConfig(
            auth_token=secret,
            config_hash=config_hash,
            expected=expected,
            expected_trade_mode=BrokerTradeMode(
                str(config.system.expected_account_trade_mode.value)
            ),
            host=config.ipc.host,
            port=config.ipc.port,
            max_frame_bytes=config.ipc.max_message_bytes,
            heartbeat_timeout_ms=int(config.ipc.heartbeat_timeout_s * 1_000),
            # The application composes the lifecycle-first producer before the
            # gateway starts listening.  SHADOW remains transport-fenced.
            execution_enabled=config.mode is RuntimeMode.DEMO,
        )
        return BrokerGateway(
            config=gateway_config,
            clock=clock,
            committed_cursor_provider=lambda account: ledger.get_deal_cursor(account),
            position_context_resolver=_position_context_resolver(
                ledger, expected, clock
            ),
            deal_attribution_resolver=_deal_attribution_resolver(
                ledger, expected, clock
            ),
            runtime=runtime,
            tuple_lock_acquirer=tuple_lock_acquirer,
        )

    def make_reconciler(
        self,
        *,
        config: RuntimeConfig,
        ledger: LedgerPort,
        runtime: RuntimeStateMachine,
        gateway: GatewayRuntimePort,
        clock: Clock,
    ) -> ReconciliationService:
        if not isinstance(ledger, TradeLedger):
            raise RuntimeAppError("default reconciler requires the canonical TradeLedger")
        return ReconciliationService(
            ledger=ledger,
            runtime=runtime,
            broker=gateway,
            clock=clock,
            daily_drawdown_pct_limit=config.risk.daily_max_drawdown_pct,
            consecutive_loss_limit=config.risk.consecutive_loss_halt,
            max_total_risk_pct=config.risk.max_total_risk_pct,
            max_open_positions=config.risk.max_open_positions,
            outbox_projector=_reconciliation_outbox_projector(config),
        )

    def make_transaction_consumer(
        self,
        *,
        config: RuntimeConfig,
        ledger: LedgerPort,
        runtime: RuntimeStateMachine,
        gateway: GatewayRuntimePort,
        clock: Clock,
    ) -> CanonicalBrokerTransactionConsumer:
        if not isinstance(ledger, TradeLedger):
            raise RuntimeAppError("default ingress requires the canonical TradeLedger")
        execution = ExecutionService(
            ledger=ledger,
            runtime=runtime,
            broker=gateway,
            clock=clock,
            outbox_projector=_trade_outbox_projector(config),
        )
        lifecycle = LifecycleService(
            ledger=ledger,
            execution=execution,
            policy=LifecyclePolicy(),
            clock=clock,
            outbox_projector=_trade_outbox_projector(config),
        )
        account_id = config.system.expected_account_id
        if account_id is None:
            raise RuntimeAppError("broker ingress requires a pinned account")
        return CanonicalBrokerTransactionConsumer(
            execution=execution,
            lifecycle=lifecycle,
            account_id=account_id,
            symbol=config.system.symbol,
            magic=config.system.magic_number,
        )

    def make_command_producer(
        self,
        *,
        config: RuntimeConfig,
        config_hash: str,
        ledger: LedgerPort,
        runtime: RuntimeStateMachine,
        gateway: GatewayRuntimePort,
        transaction_consumer: BrokerTransactionConsumer,
        clock: Clock,
    ) -> CanonicalTickScheduler:
        if not isinstance(ledger, TradeLedger):
            raise RuntimeAppError("default producer requires the canonical TradeLedger")
        if not isinstance(transaction_consumer, CanonicalBrokerTransactionConsumer):
            raise RuntimeAppError(
                "default producer requires canonical execution/lifecycle ingress"
            )
        learning = (
            GovernedLearningService(ledger)
            if config.is_agent_enabled(20)
            else None
        )
        return CanonicalTickScheduler(
            config=config,
            config_hash=config_hash,
            ledger=ledger,
            gateway=gateway,
            runtime=runtime,
            execution=transaction_consumer.execution,
            lifecycle=transaction_consumer.lifecycle,
            clock=clock,
            learning=learning,
            outbox_projector=_trade_outbox_projector(config),
        )

    def make_dispatcher(
        self,
        *,
        config: RuntimeConfig,
        config_hash: str,
        environ: Mapping[str, str],
        ledger: LedgerPort,
        clock: Clock,
    ) -> IntegrationDispatcher | None:
        enabled = _integration_destinations(config)
        if not enabled:
            return None
        if not isinstance(ledger, TradeLedger):
            raise RuntimeAppError(
                "default integration dispatcher requires the canonical TradeLedger"
            )
        publishers = build_publishers(config.integrations, environ)
        if set(publishers) != set(enabled):
            raise RuntimeAppError("enabled integration publisher composition is incomplete")
        worker_id = (
            f"outbox:{config.system.instance_id}:"
            f"{config_hash[:16]}"
        )
        store = LedgerOutboxStore(
            ledger,
            worker_id=worker_id,
            clock=lambda: clock.now_ms() / 1_000,
        )
        return IntegrationDispatcher(
            store,
            publishers,
            clock=lambda: clock.now_ms() / 1_000,
        )


@dataclass(frozen=True, slots=True)
class RuntimeAppStatus:
    mode: RuntimeMode
    phase: AppPhase
    control_plane: ControlPlaneState
    config_hash: str
    instance_root: str
    execution_enabled: bool
    transaction_ingress_ready: bool
    command_producer_ready: bool
    blockers: tuple[str, ...]
    snapshot_id: str | None
    reconciliation_run_id: str | None
    learning_status: str
    integrations_enabled: tuple[str, ...]
    dispatcher_running: bool
    dispatcher_error: str

    def as_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["mode"] = self.mode.value
        result["phase"] = self.phase.value
        result["control_plane"] = self.control_plane.value
        return result


class RuntimeApp:
    """Injectable application shell with explicit, fail-closed phases."""

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        config_path: str | Path,
        config_hash: str,
        paths: RuntimePaths,
        environ: Mapping[str, str],
        factory: RuntimeComponentFactory | None = None,
    ) -> None:
        if config.mode not in SUPPORTED_LAUNCH_MODES:
            raise RuntimeModeError(
                f"canonical launcher does not expose {config.mode.value!r} mode"
            )
        self.config = config
        self.config_path = Path(config_path).expanduser().resolve(strict=False)
        self.config_hash = _sha256_text(config_hash, "config_hash")
        self.paths = paths
        self.environ = MappingProxyType(dict(environ))
        self.factory = factory or DefaultRuntimeComponentFactory()
        self.clock = self.factory.make_clock()
        self.runtime = RuntimeStateMachine(self.clock)

        self.phase = AppPhase.NEW
        self.last_reconciliation: ReconciliationReport | None = None
        self._mode_lock: ProcessLock | None = None
        self._ownership_lock: ProcessLock | None = None
        self._ledger: LedgerPort | None = None
        self._providers: ProviderRuntimePort | None = None
        self._gateway: GatewayRuntimePort | None = None
        self._reconciler: ReconciliationRuntimePort | None = None
        self._transaction_consumer: BrokerTransactionConsumer | None = None
        self._command_producer: CommandProducerPort | None = None
        self._dispatcher: DispatcherPort | None = None
        self._dispatcher_task: asyncio.Task[None] | None = None
        self._dispatcher_error = ""
        self._provider_contexts: ProviderContexts | None = None
        self._command_producer_ready = False

    @property
    def transaction_ingress_ready(self) -> bool:
        consumer = self._transaction_consumer
        return bool(consumer is not None and consumer.execution_ready)

    @property
    def status(self) -> RuntimeAppStatus:
        evidence = self.runtime.evidence
        blockers = evidence.blockers()
        if self.config.mode is RuntimeMode.DEMO and not self._command_producer_ready:
            blockers = (*blockers, "command_producer_not_composed")
        return RuntimeAppStatus(
            mode=self.config.mode,
            phase=self.phase,
            control_plane=self.runtime.state,
            config_hash=self.config_hash,
            instance_root=str(self.paths.instance_root),
            execution_enabled=(
                self.config.mode is RuntimeMode.DEMO
                and self.transaction_ingress_ready
                and self._command_producer_ready
            ),
            transaction_ingress_ready=self.transaction_ingress_ready,
            command_producer_ready=self._command_producer_ready,
            blockers=blockers,
            snapshot_id=evidence.snapshot_id,
            reconciliation_run_id=evidence.reconciliation_run_id,
            learning_status=(
                LearningRuntimeStatus.DISABLED_BY_CONFIG.value
                if self._command_producer is None
                else self._command_producer.learning_status.value
            ),
            integrations_enabled=_integration_destinations(self.config),
            dispatcher_running=bool(
                self._dispatcher_task is not None
                and not self._dispatcher_task.done()
            ),
            dispatcher_error=self._dispatcher_error,
        )

    async def startup(self) -> RuntimeAppStatus:
        if self.phase is not AppPhase.NEW:
            raise RuntimeAppError(f"startup requires NEW phase, current={self.phase.value}")
        self.phase = AppPhase.STARTING
        self.runtime.start_bootstrap()
        try:
            if self.config.mode in {RuntimeMode.SHADOW, RuntimeMode.DEMO}:
                self.factory.assert_deployment_integrity(
                    config_path=self.config_path,
                    config=self.config,
                )

            self.paths.ensure_layout()
            self._mode_lock = self.paths.process_lock("runtime").acquire()
            if self.config.mode in {RuntimeMode.SHADOW, RuntimeMode.DEMO}:
                expected = _expected_identity(self.config)
                self._ownership_lock = self.paths.ownership_lock(
                    account_id=expected.account_id,
                    broker_server=expected.broker_server,
                    symbol=expected.symbol,
                    magic=expected.magic,
                ).acquire()

            self._ledger = self.factory.make_ledger(
                paths=self.paths, config=self.config
            )
            self._ledger.open()
            self._ledger.require_healthy()

            if self.config.mode is RuntimeMode.CHECK:
                self.runtime.stop("configuration and ledger check completed")
                self.phase = AppPhase.CHECKED
                return self.status

            self._providers = self.factory.make_providers(
                config=self.config,
                environ=self.environ,
                ledger=self._ledger,
                clock=self.clock,
            )
            self._provider_contexts = await self._providers.poll_once(
                at_ms=self.clock.now_ms()
            )
            self._gateway = self.factory.make_gateway(
                config=self.config,
                config_hash=self.config_hash,
                environ=self.environ,
                ledger=self._ledger,
                runtime=self.runtime,
                clock=self.clock,
                tuple_lock_acquirer=self._tuple_lock_acquirer,
            )
            self._reconciler = self.factory.make_reconciler(
                config=self.config,
                ledger=self._ledger,
                runtime=self.runtime,
                gateway=self._gateway,
                clock=self.clock,
            )
            self._transaction_consumer = self.factory.make_transaction_consumer(
                config=self.config,
                ledger=self._ledger,
                runtime=self.runtime,
                gateway=self._gateway,
                clock=self.clock,
            )
            producer_maker = getattr(self.factory, "make_command_producer", None)
            if callable(producer_maker):
                self._command_producer = producer_maker(
                    config=self.config,
                    config_hash=self.config_hash,
                    ledger=self._ledger,
                    runtime=self.runtime,
                    gateway=self._gateway,
                    transaction_consumer=self._transaction_consumer,
                    clock=self.clock,
                )
                self._command_producer_ready = bool(
                    self._command_producer is not None
                    and self._command_producer.execution_ready
                )
            dispatcher_maker = getattr(self.factory, "make_dispatcher", None)
            if callable(dispatcher_maker):
                self._dispatcher = dispatcher_maker(
                    config=self.config,
                    config_hash=self.config_hash,
                    environ=self.environ,
                    ledger=self._ledger,
                    clock=self.clock,
                )
            if self._command_producer is not None:
                # Agent 20 is an isolated analytics worker.  It never approves
                # or activates a proposal and cannot prevent broker protection.
                await self._command_producer.process_learning_due()
            provider_safe = self._entry_inputs_ready(self._provider_contexts)
            self.runtime.wait_for_ea(
                ledger_healthy=True,
                provider_gate_safe=provider_safe,
            )
            await self._gateway.start()
            if self._dispatcher is not None:
                self._dispatcher_error = ""
                self._dispatcher_task = asyncio.create_task(
                    self._dispatcher.run(),
                    name="quant-swarm-integration-dispatcher",
                )
            self.phase = AppPhase.WAITING_EA
            return self.status
        except BaseException as exc:
            if self.runtime.state not in {
                ControlPlaneState.HALTED,
                ControlPlaneState.STOPPED,
            }:
                self.runtime.halt(
                    f"runtime startup failed: {type(exc).__name__}"
                )
            self.phase = AppPhase.FAILED
            await self._release_components(suppress=True)
            raise

    async def handle_signal(self, gateway_signal: GatewaySignal) -> None:
        if self._gateway is None or self._reconciler is None:
            raise RuntimeAppError("broker signal received before broker startup")
        kind = gateway_signal.signal_type
        if kind is GatewaySignalType.CONNECTED:
            if self.runtime.state is not ControlPlaneState.SYNCHRONIZING:
                raise RuntimeAppError(
                    "CONNECTED signal lacks prior authenticated identity transition"
                )
            await self._reconcile()
            return
        if kind is GatewaySignalType.DISCONNECTED:
            self.phase = AppPhase.DISCONNECTED
            return
        if kind is GatewaySignalType.BROKER_EVENT:
            await self._ingest_broker_signal(gateway_signal)
            return
        if kind is GatewaySignalType.COMMAND_ACK:
            await self._ingest_command_ack(gateway_signal, rejected=False)
            return
        if kind is GatewaySignalType.COMMAND_NACK:
            await self._ingest_command_ack(gateway_signal, rejected=True)
            return
        if kind in {
            GatewaySignalType.UNKNOWN_COMMAND,
            GatewaySignalType.CACHE_FAILURE,
            GatewaySignalType.PROTOCOL_VIOLATION,
        }:
            await self._recover_authoritatively(
                f"gateway signal requires reconciliation:{kind.value}"
            )

    async def _ingest_command_ack(
        self, gateway_signal: GatewaySignal, *, rejected: bool
    ) -> None:
        consumer = self._transaction_consumer
        if consumer is None or not consumer.execution_ready:
            await self._recover_authoritatively(
                "command acknowledgement arrived without canonical ingress"
            )
            raise RuntimeIngressError("command acknowledgement ingress is unavailable")
        try:
            if rejected:
                consumer.ingest_command_rejected(gateway_signal)
            else:
                consumer.ingest_command_received(gateway_signal)
        except Exception as exc:
            await self._recover_authoritatively(
                f"command acknowledgement ingestion failed:{type(exc).__name__}"
            )
            raise RuntimeIngressError(
                "command acknowledgement was not committed authoritatively"
            ) from exc

    async def run_once(self, *, timeout: float | None = None) -> GatewaySignal:
        if self._gateway is None:
            raise RuntimeAppError("runtime gateway is not started")
        gateway_signal = await self._gateway.next_signal(timeout)
        await self.handle_signal(gateway_signal)
        return gateway_signal

    async def run_tick_once(
        self, *, timeout: float | None = None
    ) -> TickCycleResult | None:
        producer = self._command_producer
        if producer is None:
            raise RuntimeCapabilityError("canonical tick producer is not composed")
        tick = await producer.next_tick(timeout)
        contexts = self._provider_contexts
        reconciliation = self.last_reconciliation
        if contexts is None or reconciliation is None:
            # Pre-reconciliation ticks carry no execution authority and are
            # intentionally consumed without analysis or persistence.
            return None
        try:
            return await producer.process_tick(
                tick,
                providers=contexts,
                reconciliation=reconciliation,
            )
        except Exception as exc:
            await self._recover_authoritatively(
                f"tick scheduler failed:{type(exc).__name__}"
            )
            raise RuntimeIngressError(
                "authenticated tick was not processed safely"
            ) from exc

    async def run_cycle(self, *, timeout: float = 0.5) -> None:
        """Prioritize control signals, then process at most one queued tick."""

        if self._gateway is None:
            raise RuntimeAppError("runtime gateway is not started")
        tasks: dict[asyncio.Task[Any], str] = {
            asyncio.create_task(self._gateway.next_signal()): "signal"
        }
        producer = self._command_producer
        if producer is not None:
            tasks[asyncio.create_task(producer.next_tick())] = "tick"
        done, pending = await asyncio.wait(
            tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        completed = set(done)
        # If a queue getter completed in the cancellation race, its item has
        # already been removed and must be processed rather than silently lost.
        completed.update(
            task
            for task in pending
            if task.done() and not task.cancelled() and task.exception() is None
        )
        if not completed:
            await self._gateway.check_health()
        else:
            # A disconnect/NACK/event must fence state before a simultaneously
            # available tick is allowed to evaluate lifecycle or entry.
            ordered = sorted(
                completed, key=lambda item: 0 if tasks[item] == "signal" else 1
            )
            for task in ordered:
                result = task.result()
                if tasks[task] == "signal":
                    await self.handle_signal(result)
                elif self._provider_contexts is not None and self.last_reconciliation is not None:
                    if producer is None:
                        raise RuntimeError(
                            "runtime tick producer is unavailable"
                        )
                    try:
                        await producer.process_tick(
                            result,
                            providers=self._provider_contexts,
                            reconciliation=self.last_reconciliation,
                        )
                    except Exception as exc:
                        await self._recover_authoritatively(
                            f"tick scheduler failed:{type(exc).__name__}"
                        )
                        raise RuntimeIngressError(
                            "authenticated tick was not processed safely"
                        ) from exc

        if producer is not None:
            expired = producer.expire_sent_commands()
            if expired:
                await self._recover_authoritatively(
                    "command acknowledgement expired:" + ",".join(expired)
                )

    async def poll_providers(self) -> ProviderContexts:
        if self._providers is None:
            raise RuntimeAppError("providers are not composed in this mode")
        previous = self.runtime.evidence.provider_gate_safe
        contexts = await self._providers.poll_once(at_ms=self.clock.now_ms())
        self._provider_contexts = contexts
        current = self._entry_inputs_ready(contexts)
        self.runtime.update_evidence(provider_gate_safe=current)
        if current and not previous and self.runtime.state is ControlPlaneState.DEGRADED:
            await self._reconcile()
        if self._command_producer is not None:
            await self._command_producer.process_learning_due()
        return contexts

    async def send_command(self, command: OutboundCommand) -> None:
        """Transmit only from DEMO after the complete readiness gate."""

        if self.config.mode is not RuntimeMode.DEMO:
            raise RuntimeCapabilityError(
                f"{self.config.mode.value} mode has no order-send capability"
            )
        if not self._command_producer_ready:
            raise RuntimeCapabilityError(
                "entry/lifecycle command producer is not composed; DEMO execution remains blocked"
            )
        if not self.transaction_ingress_ready:
            raise RuntimeCapabilityError(
                "broker transaction ingress is not ready; execution remains blocked"
            )
        self.runtime.require_ready("broker command transmission")
        if self._gateway is None:
            raise RuntimeAppError("runtime gateway is not started")
        await self._gateway.send(command)

    async def serve(
        self,
        stop_event: asyncio.Event,
        *,
        signal_timeout_s: float = 0.5,
    ) -> None:
        """Run bounded iterations until an injected stop event is set."""

        if self.phase is AppPhase.NEW:
            await self.startup()
        if self.config.mode is RuntimeMode.CHECK:
            return
        interval = _provider_poll_interval_s(self.config)
        loop = asyncio.get_running_loop()
        next_provider_poll = loop.time() + interval
        while not stop_event.is_set():
            await self.run_cycle(timeout=signal_timeout_s)
            self._observe_dispatcher_task()
            if loop.time() >= next_provider_poll:
                await self.poll_providers()
                next_provider_poll = loop.time() + interval

    async def shutdown(self) -> None:
        if self.phase is AppPhase.STOPPED:
            return
        await self._release_components(suppress=False)
        if self.runtime.state is not ControlPlaneState.STOPPED:
            self.runtime.stop("runtime shutdown completed")
        self.phase = AppPhase.STOPPED

    async def _reconcile(self) -> ReconciliationReport | None:
        if self._reconciler is None:
            raise RuntimeAppError("reconciliation service is unavailable")
        try:
            report = await self._reconciler.reconcile(
                _expected_identity(self.config)
            )
        except BrokerSnapshotError:
            # TRANSIENT_BROKER_RECONCILIATION_CONTAINMENT:
            # Transport loss during snapshot/history assembly is recoverable.
            # The reconciliation service has already fenced the control plane;
            # clear stale authority and remain available for the next EA session.
            self.last_reconciliation = None
            self.phase = AppPhase.DISCONNECTED
            return None
        self.last_reconciliation = report
        self.phase = (
            AppPhase.SYNCHRONIZED
            if self.runtime.state is ControlPlaneState.READY
            else AppPhase.DEGRADED
        )
        return report

    async def _ingest_broker_signal(self, gateway_signal: GatewaySignal) -> None:
        event = gateway_signal.payload.get("event")
        deal = gateway_signal.payload.get("deal")
        if not isinstance(event, BrokerEvent):
            await self._recover_authoritatively(
                "broker event lacks a canonical BrokerEvent payload"
            )
            return
        if event.event_type in _DEAL_EVENT_TYPES:
            if not isinstance(deal, BrokerDealRecord):
                await self._recover_authoritatively(
                    f"{event.event_type.value} lacks its exact broker deal"
                )
                return
            differences = _transaction_differences(
                event,
                deal,
                account_id=_expected_identity(self.config).account_id,
            )
            if differences:
                await self._recover_authoritatively(
                    "broker event/deal conflict:" + ",".join(differences)
                )
                return
        elif deal is not None:
            await self._recover_authoritatively(
                f"non-fill {event.event_type.value} carried unexpected deal economics"
            )
            return
        consumer = self._transaction_consumer
        if consumer is None or not consumer.execution_ready:
            await self._recover_authoritatively(
                "broker transaction ingress is unavailable"
            )
            return
        try:
            await consumer.ingest(
                event,
                deal if isinstance(deal, BrokerDealRecord) else None,
            )
        except Exception as exc:
            await self._recover_authoritatively(
                f"broker transaction ingestion failed:{type(exc).__name__}"
            )
            raise RuntimeIngressError(
                "broker transaction was not accepted; reconciliation was required"
            ) from exc

    async def _recover_authoritatively(self, reason: str) -> None:
        state = self.runtime.state
        if state in {ControlPlaneState.READY, ControlPlaneState.DEGRADED}:
            existing = tuple(
                item
                for item in self.runtime.evidence.blocking_diffs
                if not item.startswith("RUNTIME_RECOVERY:")
            )
            self.runtime.update_evidence(
                reconciliation_succeeded=False,
                blocking_diffs=(*existing, f"RUNTIME_RECOVERY:{reason}"),
            )
            state = self.runtime.state
        if state in {ControlPlaneState.SYNCHRONIZING, ControlPlaneState.DEGRADED}:
            await self._reconcile()
        else:
            self.phase = AppPhase.DISCONNECTED

    def _entry_inputs_ready(self, contexts: ProviderContexts) -> bool:
        now = self.clock.now_ms() / 1_000
        # Macro is a global entry-protection dependency only when configured.
        # A deliberately disabled macro feed is valid for SHADOW observation,
        # and DEMO can omit it only through the explicit technical-only config
        # override validated by RuntimeConfig.  Intermarket loss is never a
        # global stop: the decision boundary disables Agents 12/15 instead.
        if self.config.providers.macro.enabled:
            providers_safe = bool(
                contexts.macro is not None
                and contexts.macro_snapshot.execution_eligible_at(now)
            )
        else:
            providers_safe = bool(
                self.config.mode is RuntimeMode.SHADOW
                or self.config.providers.unsafe_allow_technical_without_macro
            )
        if self.config.mode is RuntimeMode.DEMO:
            return (
                providers_safe
                and self.transaction_ingress_ready
                and self._command_producer_ready
            )
        return providers_safe

    def _observe_dispatcher_task(self) -> None:
        """Contain and expose an unexpected integration worker termination.

        Notifications are deliberately outside the trading control path, so a
        failed dispatcher must not halt lifecycle protection.  Consuming the
        task exception prevents a silent background failure while the explicit
        status/error and log make the deployment fault observable.
        """

        task = self._dispatcher_task
        if task is None or not task.done():
            return
        self._dispatcher_task = None
        if task.cancelled():
            detail = "CancelledError"
        else:
            exception = task.exception()
            detail = (
                "STOPPED_UNEXPECTEDLY"
                if exception is None
                else type(exception).__name__
            )
        self._dispatcher_error = detail
        log.error("Integration dispatcher stopped unexpectedly: %s", detail)

    def _tuple_lock_acquirer(self, actual: BrokerIdentity) -> bool:
        lock = self._ownership_lock
        if lock is None or not lock.acquired:
            return False
        expected = _expected_identity(self.config)
        return actual.ownership_key == (
            expected.account_id,
            expected.broker_server,
            expected.symbol,
            expected.magic,
        )

    async def _release_components(self, *, suppress: bool) -> None:
        failures: list[BaseException] = []
        if self._dispatcher_task is not None:
            self._dispatcher_task.cancel()
            try:
                await self._dispatcher_task
            except asyncio.CancelledError:
                pass
            except BaseException as exc:
                failures.append(exc)
            self._dispatcher_task = None
        if self._gateway is not None:
            try:
                await self._gateway.close()
            except BaseException as exc:  # cleanup must continue through every resource
                failures.append(exc)
            self._gateway = None
        if self._ledger is not None:
            try:
                self._ledger.close()
            except BaseException as exc:
                failures.append(exc)
            self._ledger = None
        for name in ("_ownership_lock", "_mode_lock"):
            lock = getattr(self, name)
            if lock is not None:
                try:
                    lock.release()
                except BaseException as exc:
                    failures.append(exc)
                setattr(self, name, None)
        self._providers = None
        self._reconciler = None
        self._transaction_consumer = None
        self._command_producer = None
        self._command_producer_ready = False
        self._dispatcher = None
        self._dispatcher_error = ""
        if failures and not suppress:
            raise RuntimeAppError(
                "runtime cleanup failed: "
                + ", ".join(type(item).__name__ for item in failures)
            ) from failures[0]


def effective_config_hash(config: RuntimeConfig) -> str:
    """Hash the fully expanded, strictly parsed semantic configuration."""

    if not isinstance(config, RuntimeConfig):
        raise TypeError("config must be RuntimeConfig")
    material = {
        "contract": "quant-swarm-effective-config-v1",
        "config": _canonical_value(config),
    }
    encoded = json.dumps(
        material,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_runtime_app(
    *,
    mode: RuntimeMode | str,
    config_path: str | Path,
    environ: Mapping[str, str] | None = None,
    factory: RuntimeComponentFactory | None = None,
) -> RuntimeApp:
    """Load schema-v2 configuration and require an exact CLI/file mode match."""

    requested = _launch_mode(mode)
    env = dict(os.environ if environ is None else environ)
    path = Path(config_path).expanduser().resolve(strict=False)
    config = load_runtime_config(path, environ=env)
    if config.mode is not requested:
        raise RuntimeModeError(
            f"CLI mode {requested.value!r} differs from config mode {config.mode.value!r}"
        )
    paths = RuntimePaths.resolve(
        requested,
        data_dir=config.storage.data_dir,
        instance_id=config.system.instance_id,
        environ=env,
    )
    return RuntimeApp(
        config=config,
        config_path=path,
        config_hash=effective_config_hash(config),
        paths=paths,
        environ=env,
        factory=factory,
    )


def _launch_mode(value: RuntimeMode | str) -> RuntimeMode:
    try:
        mode = value if isinstance(value, RuntimeMode) else RuntimeMode(value)
    except ValueError as exc:
        raise RuntimeModeError(f"unsupported canonical launch mode: {value!r}") from exc
    if mode not in SUPPORTED_LAUNCH_MODES:
        raise RuntimeModeError(
            f"canonical launcher exposes only check/shadow/demo, not {mode.value!r}"
        )
    return mode


def _expected_identity(config: RuntimeConfig) -> ExpectedBrokerIdentity:
    system = config.system
    if (
        system.expected_account_id is None
        or system.expected_broker_server is None
        or system.expected_account_trade_mode is None
    ):
        raise RuntimeAppError("broker mode requires a complete pinned identity")
    if system.expected_account_trade_mode is not AccountTradeMode.DEMO:
        raise RuntimeCapabilityError("canonical launcher accepts DEMO accounts only")
    return ExpectedBrokerIdentity(
        account_id=system.expected_account_id,
        broker_server=system.expected_broker_server,
        symbol=system.symbol,
        magic=system.magic_number,
        allow_real_account=False,
    )


def _provider_url(settings: ProviderSettings) -> str | None:
    if settings.source is ProviderSource.DISABLED:
        return None
    if settings.source is not ProviderSource.HTTP:
        raise RuntimeAppError(
            f"no canonical runtime adapter exists for provider source {settings.source.value!r}"
        )
    if not settings.url:
        raise RuntimeAppError("enabled HTTP provider lacks its validated URL")
    return settings.url


def _provider_source_name(settings: ProviderSettings, provider: str) -> str:
    return (
        "disabled"
        if settings.source is ProviderSource.DISABLED
        else f"{provider}-http-json"
    )


def _provider_headers(
    settings: ProviderSettings, environ: Mapping[str, str]
) -> Mapping[str, str]:
    if settings.credential_env is None:
        return {}
    credential = environ.get(settings.credential_env, "")
    if not credential:
        raise RuntimeAppError("configured provider credential is unavailable")
    return {"Authorization": f"Bearer {credential}"}


def _position_context_resolver(
    ledger: TradeLedger,
    expected: ExpectedBrokerIdentity,
    clock: Clock,
) -> Callable[[BrokerObservedPosition], DurablePositionContext | None]:
    def resolve(observed: BrokerObservedPosition) -> DurablePositionContext | None:
        row = ledger.get_owned_position(expected.account_id, observed.broker_position_id)
        if row is None or not isinstance(row.get("trade_id"), str):
            return None
        open_command = ledger.get_open_command_for_trade(row["trade_id"])
        if (
            open_command is not None
            and open_command.get("status")
            not in {"APPLIED", "RECONCILED_APPLIED"}
            and (
                observed.remaining_volume != Decimal(str(row["remaining_volume"]))
                or observed.position_version != int(row["position_version"])
            )
        ):
            # A crash may occur after one OPEN deal and before later deals are
            # ingested.  Do not force the newer broker snapshot through stale
            # immutable context; expose it raw so reconciliation can rebuild
            # the aggregate from synchronized command-attributed IN deals.
            return None
        return DurablePositionContext(
            resolved_at_ms=clock.now_ms(),
            account_id=expected.account_id,
            trade_id=row["trade_id"],
            broker_position_id=row["broker_position_id"],
            symbol=row["symbol"],
            magic=int(row["magic"]),
            direction=row["direction"],
            initial_volume=row["initial_volume"],
            original_sl=row["original_sl"],
            tp1=row["tp1"],
            tp2=row["tp2"],
            tp3=row["tp3"],
            initial_risk_amount=row["initial_risk_amount"],
            lifecycle_stage=row["lifecycle_stage"],
            position_version=int(row["position_version"]),
            broker_order_id=row.get("broker_order_id"),
            realised_pnl=row.get("realised_pnl", "0"),
            pending_action=row.get("pending_action"),
            pending_milestone=row.get("pending_milestone"),
            pending_command_id=row.get("pending_command_id"),
            pending_close_volume=row.get("pending_close_volume", "0"),
            pending_filled_volume=row.get("pending_filled_volume", "0"),
        )

    return resolve


def _deal_attribution_resolver(
    ledger: TradeLedger,
    expected: ExpectedBrokerIdentity,
    clock: Clock,
) -> Callable[[BrokerObservedDeal], DurableDealAttribution | None]:
    def resolve(observed: BrokerObservedDeal) -> DurableDealAttribution | None:
        position = ledger.get_owned_position(expected.account_id, observed.position_id)
        durable_trade_id = (
            position.get("trade_id") if position is not None else None
        )
        trade_id = observed.wire_trade_id or durable_trade_id
        if not isinstance(trade_id, str) or not trade_id:
            return None
        trade = ledger.get_trade(trade_id)
        if (
            trade is None
            or trade.get("account_id") != expected.account_id
            or trade.get("symbol") != expected.symbol
            or trade.get("magic_number") != expected.magic
            or trade.get("broker_position_id") not in {None, observed.position_id}
            or durable_trade_id not in {None, trade_id}
        ):
            return None
        command_id = observed.wire_command_id
        if command_id is not None:
            command = ledger.get_command(command_id)
            if (
                command is None
                or command.get("trade_id") != trade_id
                or (
                    command.get("action") != "OPEN"
                    and command.get("position_id") != observed.position_id
                )
            ):
                return None
        return DurableDealAttribution(
            resolved_at_ms=clock.now_ms(),
            account_id=expected.account_id,
            deal_id=observed.deal_id,
            position_id=observed.position_id,
            trade_id=trade_id,
            command_id=command_id,
        )

    return resolve


def _transaction_differences(
    event: BrokerEvent,
    deal: BrokerDealRecord,
    *,
    account_id: str,
) -> tuple[str, ...]:
    differences: list[str] = []
    expected = {
        "account_id": account_id,
        "deal_id": event.deal_id,
        "position_id": event.broker_position_id,
        "order_id": event.order_id,
        "trade_id": event.trade_id,
        "command_id": event.command_id,
    }
    for name, value in expected.items():
        if getattr(deal, name) != value:
            differences.append(name)
    decimals = {
        "volume": event.fill_volume,
        "price": event.fill_price,
        "profit": event.profit,
        "commission": event.commission,
        "swap": event.swap,
        "fee": event.fee,
    }
    for name, value in decimals.items():
        try:
            differs = value is None or Decimal(str(getattr(deal, name))) != value
        except (InvalidOperation, TypeError, ValueError):
            differs = True
        if differs:
            differences.append(name)
    try:
        deal_time_ms = Decimal(str(deal.occurred_at)) * Decimal("1000")
    except (InvalidOperation, TypeError, ValueError):
        deal_time_ms = Decimal("-1")
    if deal_time_ms != Decimal(event.broker_time_ms):
        differences.append("broker_time_ms")
    expected_type = (
        "IN" if event.event_type is BrokerEventType.POSITION_OPENED else "OUT"
    )
    if deal.deal_type.upper() not in (
        {"IN"} if expected_type == "IN" else {"OUT", "OUT_BY"}
    ):
        differences.append("deal_type")
    return tuple(differences)


def _provider_poll_interval_s(config: RuntimeConfig) -> float:
    ages = (
        config.providers.macro.max_age_s,
        config.providers.intermarket.max_age_s,
    )
    return max(1.0, min(60.0, min(ages) / 2))


def _integration_destinations(config: RuntimeConfig) -> tuple[str, ...]:
    return tuple(
        name
        for name in ("telegram", "discord", "website")
        if getattr(config.integrations, name)
    )


def _trade_outbox_projector(
    config: RuntimeConfig,
) -> Callable[[TradeEventRecord], tuple[Any, ...]]:
    destinations = _integration_destinations(config)
    topics = {
        "ENTRY_SIGNAL_REJECTED": "trade.signal_rejected",
        "ENTRY_SIGNAL_APPROVED_OBSERVATION": "trade.signal_observed",
        "ENTRY_SNAPSHOT_BLOCKED": "trade.signal_blocked",
        "ENTRY_SKIPPED_LIFECYCLE_PRIORITY": "trade.signal_deferred",
        "OPEN_COMMAND_RESERVED": "trade.signal_approved",
        "LIFECYCLE_COMMAND_CREATED": "trade.lifecycle_submitted",
        "BROKER_COMMAND_SENT": "trade.submitted",
        "BROKER_COMMAND_RECEIVED": "trade.acknowledged",
        "POSITION_OPENED": "trade.position_opened",
        "PARTIAL_FILLED": "trade.partial_filled",
        "POSITION_UPDATED": "trade.position_modified",
        "POSITION_CLOSED": "trade.position_closed",
        "COMMAND_REJECTED": "trade.command_rejected",
        "VIRTUAL_MILESTONE_CONFIRMED": "trade.lifecycle_virtual",
    }

    def project(event: TradeEventRecord) -> tuple[Any, ...]:
        if not destinations:
            return ()
        topic = topics.get(event.event_type, "trade.event")
        severity = (
            "WARNING"
            if event.event_type
            in {"COMMAND_REJECTED", "ENTRY_SNAPSHOT_BLOCKED"}
            else "INFO"
        )
        domain = DomainEvent(
            event_id=event.event_id,
            event_type=topic,
            severity=severity,
            occurred_at=event.occurred_at,
            trade_id=event.trade_id,
            request_id=event.correlation_id,
            payload={
                "ledger_event_type": event.event_type,
                "source": event.source,
                "broker_timestamp": event.broker_timestamp,
                "detail": _canonical_value(dict(event.payload)),
            },
        )
        return outbox_records_for_event(domain, destinations)

    return project


def _reconciliation_outbox_projector(
    config: RuntimeConfig,
) -> Callable[[TradeEventRecord], tuple[Any, ...]]:
    destinations = _integration_destinations(config)

    def project(event: TradeEventRecord) -> tuple[Any, ...]:
        domain = DomainEvent(
            event_id=event.event_id,
            event_type="reconciliation.position_closed",
            severity="INFO",
            occurred_at=event.occurred_at,
            trade_id=event.trade_id,
            request_id=event.correlation_id,
            payload={
                "ledger_event_type": event.event_type,
                "source": event.source,
                "broker_timestamp": event.broker_timestamp,
                "detail": _canonical_value(dict(event.payload)),
            },
        )
        return outbox_records_for_event(domain, destinations)

    return project


def _canonical_value(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return {
            item: _canonical_value(child)
            for item, child in asdict(value).items()
        }
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {
            str(key): _canonical_value(child)
            for key, child in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (set, frozenset)):
        return sorted((_canonical_value(item) for item in value), key=repr)
    if isinstance(value, (tuple, list)):
        return [_canonical_value(item) for item in value]
    return value


def _sha256_text(value: str, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} must be a lowercase SHA-256 hex digest")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m python_swarm.runtime_app")
    parser.add_argument(
        "--version",
        action="version",
        version=f"{PRODUCT_NAME} {PRODUCT_VERSION}",
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=tuple(item.value for item in sorted(SUPPORTED_LAUNCH_MODES, key=str)),
    )
    parser.add_argument(
        "--config",
        default=os.environ.get("QSX_CONFIG"),
        help="schema-v2 JSON path (or set QSX_CONFIG)",
    )
    return parser


async def _async_main(args: argparse.Namespace) -> int:
    if not args.config:
        raise RuntimeAppError("--config is required when QSX_CONFIG is unset")
    app = build_runtime_app(mode=args.mode, config_path=args.config)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop_event.set)
            installed.append(signum)
        except (NotImplementedError, RuntimeError):  # pragma: no cover - non-POSIX loop
            pass
    try:
        await app.startup()
        print(json.dumps(app.status.as_dict(), sort_keys=True, separators=(",", ":")))
        if app.config.mode is not RuntimeMode.CHECK:
            await app.serve(stop_event)
        return 0
    finally:
        await app.shutdown()
        for signum in installed:
            loop.remove_signal_handler(signum)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    return asyncio.run(_async_main(args))


if __name__ == "__main__":  # pragma: no cover - exercised by deployment
    raise SystemExit(main())


__all__ = [
    "AppPhase",
    "BrokerTransactionConsumer",
    "CanonicalBrokerTransactionConsumer",
    "DefaultRuntimeComponentFactory",
    "RuntimeApp",
    "RuntimeAppError",
    "RuntimeAppStatus",
    "RuntimeCapabilityError",
    "RuntimeComponentFactory",
    "RuntimeIngressError",
    "RuntimeModeError",
    "SUPPORTED_LAUNCH_MODES",
    "build_runtime_app",
    "effective_config_hash",
    "main",
]
