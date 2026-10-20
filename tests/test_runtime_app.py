import asyncio
import functools
import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from python_swarm.broker_gateway import (
    BrokerGateway,
    GatewayConfig,
    GatewayError,
    GatewaySignal,
    GatewaySignalType,
)
from python_swarm.broker_protocol import BrokerSessionMode
from python_swarm.domain_models import (
    BrokerEvent,
    BrokerEventType,
    CommandAction,
    FixedClock,
)
from python_swarm.execution_service import OutboundCommand
from python_swarm.market_snapshot_service import ProviderContexts
from python_swarm.provider_adapters import (
    ExecutionProtection,
    ProviderSnapshot,
    ProviderStatus,
)
from python_swarm.runtime_app import (
    AppPhase,
    CanonicalBrokerTransactionConsumer,
    DefaultRuntimeComponentFactory,
    RuntimeCapabilityError,
    RuntimeModeError,
    build_runtime_app,
    effective_config_hash,
)
from python_swarm.runtime_config import (
    IntegrationsSettings,
    ProviderSettings,
    ProvidersSettings,
    ProviderSource,
    RiskSettings,
    RuntimeConfig,
    RuntimeMode,
)
from python_swarm.runtime_paths import ProcessLockError, RuntimePaths
from python_swarm.runtime_state import (
    BrokerIdentity,
    BrokerTradeMode,
    ControlPlaneState,
    ExpectedBrokerIdentity,
    identity_mismatches,
)
from python_swarm.trade_ledger import BrokerDealRecord, TradeEventRecord, TradeLedger


NOW = 1_784_116_800_000
SECRET = "s" * 40


def async_test(function):
    @functools.wraps(function)
    def run(*args, **kwargs):
        return asyncio.run(function(*args, **kwargs))

    return run


def config_mapping(
    mode: str,
    data_dir: Path,
    *,
    instance_id: str = "runtime-a",
    account_id: str = "demo-10001",
) -> dict:
    raw = {
        "schema_version": 2,
        "mode": mode,
        "system": {
            "symbol": "XAUUSD",
            "magic_number": 202412,
            "instance_id": instance_id,
        },
        "storage": {
            "data_dir": str(data_dir),
            "sqlite_busy_timeout_ms": 5000,
        },
    }
    if mode in {"shadow", "demo"}:
        raw["system"].update(
            {
                "expected_account_id": account_id,
                "expected_broker_server": "Broker-Demo",
                "expected_account_trade_mode": "DEMO",
            }
        )
        raw["ipc"] = {"auth_secret_env": "QSX_IPC_SECRET"}
    if mode == "demo":
        raw["providers"] = {
            "macro": {"enabled": False, "source": "disabled"},
            "intermarket": {"enabled": False, "source": "disabled"},
            "unsafe_allow_technical_without_macro": True,
        }
    return raw


def write_config(tmp_path: Path, raw: dict, name: str = "runtime.json") -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(raw), encoding="utf-8")
    return path


def broker_env() -> dict[str, str]:
    return {"QSX_IPC_SECRET": SECRET}


class FakeLedger:
    def __init__(self, calls):
        self.calls = calls
        self.is_open = False

    def open(self):
        self.calls.append("ledger.open")
        self.is_open = True
        return self

    def require_healthy(self):
        self.calls.append("ledger.health")

    def close(self):
        self.calls.append("ledger.close")
        self.is_open = False


def provider_snapshot(provider: str, *, eligible: bool = True) -> ProviderSnapshot:
    return ProviderSnapshot(
        provider=provider,
        source="test",
        status=ProviderStatus.HEALTHY if eligible else ProviderStatus.ERROR,
        source_timestamp=NOW / 1000 if eligible else None,
        received_timestamp=NOW / 1000,
        sequence=1,
        freshness_s=300,
        payload={"evidence": True} if eligible else {},
        execution_protection=(
            ExecutionProtection.ALLOW
            if eligible
            else ExecutionProtection.BLOCK_NEW_ENTRIES
        ),
    )


class FakeProviders:
    def __init__(self, calls, *, eligible=True):
        self.calls = calls
        self.eligible = eligible

    async def poll_once(self, *, at_ms):
        self.calls.append("providers.poll")
        macro = provider_snapshot("macro", eligible=self.eligible)
        intermarket = provider_snapshot("intermarket", eligible=self.eligible)
        return ProviderContexts(None, None, macro, intermarket)


class FakeGateway:
    def __init__(self, calls, *, execution_enabled):
        self.calls = calls
        self.execution_enabled = execution_enabled
        self.signals = []
        self.sent = []

    async def start(self):
        self.calls.append("gateway.start")

    async def close(self):
        self.calls.append("gateway.close")

    async def next_signal(self, timeout=None):
        if not self.signals:
            raise TimeoutError
        return self.signals.pop(0)

    async def check_health(self):
        self.calls.append("gateway.health")

    async def send(self, command):
        if not self.execution_enabled:
            raise GatewayError("execution disabled")
        self.sent.append(command)


class FakeReconciler:
    def __init__(self, calls, runtime):
        self.calls = calls
        self.runtime = runtime
        self.count = 0

    async def reconcile(self, expected):
        self.calls.append("reconcile")
        self.count += 1
        self.runtime.mark_synchronized(
            snapshot_id=f"snapshot-{self.count}",
            reconciliation_run_id=f"run-{self.count}",
            deal_cursor_initialized=True,
            daily_baseline_verified=True,
            daily_risk_halted=False,
        )
        return SimpleNamespace(
            run_id=f"run-{self.count}",
            snapshot_id=f"snapshot-{self.count}",
            succeeded=True,
        )


class FakeIngress:
    def __init__(self, calls, *, ready=True):
        self.calls = calls
        self.execution_ready = ready
        self.transactions = []

    async def ingest(self, event, deal):
        self.calls.append("ingress")
        self.transactions.append((event, deal))


class FakeFactory:
    def __init__(self, *, eligible=True, ingress_ready=True, preflight_error=None):
        self.calls = []
        self.eligible = eligible
        self.ingress_ready = ingress_ready
        self.preflight_error = preflight_error
        self.clock = FixedClock(NOW)
        self.gateway = None
        self.reconciler = None
        self.ingress = None

    def make_clock(self):
        return self.clock

    def assert_deployment_integrity(self, *, config_path, config):
        self.calls.append("preflight")
        if self.preflight_error is not None:
            raise self.preflight_error

    def make_ledger(self, *, paths, config):
        self.calls.append("ledger.make")
        return FakeLedger(self.calls)

    def make_providers(self, *, config, environ, ledger, clock):
        self.calls.append("providers.make")
        return FakeProviders(self.calls, eligible=self.eligible)

    def make_gateway(
        self,
        *,
        config,
        config_hash,
        environ,
        ledger,
        runtime,
        clock,
        tuple_lock_acquirer,
    ):
        self.calls.append("gateway.make")
        self.gateway = FakeGateway(
            self.calls, execution_enabled=config.mode is RuntimeMode.DEMO
        )
        self.tuple_lock_acquirer = tuple_lock_acquirer
        return self.gateway

    def make_reconciler(self, *, config, ledger, runtime, gateway, clock):
        self.calls.append("reconciler.make")
        self.reconciler = FakeReconciler(self.calls, runtime)
        return self.reconciler

    def make_transaction_consumer(
        self, *, config, ledger, runtime, gateway, clock
    ):
        self.calls.append("ingress.make")
        self.ingress = FakeIngress(self.calls, ready=self.ingress_ready)
        return self.ingress


def expected_identity():
    return ExpectedBrokerIdentity(
        "demo-10001", "Broker-Demo", "XAUUSD", 202412
    )


def actual_identity(*, permissions=True, epoch="epoch-1"):
    return BrokerIdentity(
        "demo-10001",
        "Broker-Demo",
        BrokerTradeMode.DEMO,
        "XAUUSD",
        202412,
        epoch,
        permissions,
        permissions,
    )


def authenticate(app, factory, *, permissions=True):
    actual = actual_identity(permissions=permissions)
    app.runtime.accept_connection(
        expected_identity(),
        actual,
        transport_loopback=True,
        tuple_lock_acquired=factory.tuple_lock_acquirer(actual),
        require_execution_permissions=app.config.mode is RuntimeMode.DEMO,
    )


def connected_signal():
    return GatewaySignal(
        GatewaySignalType.CONNECTED,
        NOW,
        "epoch-1",
    )


def fill_transaction():
    event = BrokerEvent(
        event_id="event-1",
        event_type=BrokerEventType.PARTIAL_FILLED,
        broker_time_ms=NOW - 50,
        received_at_ms=NOW,
        command_id="command-1",
        trade_id="trade-1",
        broker_position_id="position-1",
        position_ticket="ticket-1",
        order_id="order-close-1",
        deal_id="deal-1",
        fill_price=Decimal("2505"),
        fill_volume=Decimal("0.03"),
        remaining_volume=Decimal("0.07"),
        sl=Decimal("2500"),
        tp=Decimal("2515"),
        profit=Decimal("15"),
        commission=Decimal("-0.2"),
        position_version=2,
    )
    deal = BrokerDealRecord(
        account_id="demo-10001",
        deal_id="deal-1",
        position_id="position-1",
        order_id="order-close-1",
        deal_type="OUT",
        volume="0.03",
        price="2505",
        profit="15",
        commission="-0.2",
        swap="0",
        fee="0",
        occurred_at=(NOW - 50) / 1000,
        trade_id="trade-1",
        command_id="command-1",
    )
    return event, deal


def outbound_command():
    return OutboundCommand(
        command_id="command-1",
        trade_id="trade-1",
        reservation_id="reservation-1",
        action=CommandAction.OPEN,
        connection_epoch="epoch-1",
        sequence=1,
        created_at_ms=NOW,
        expires_at_ms=NOW + 10_000,
        position_id=None,
        expected_position_version=None,
        expected_effect={"event_type": "POSITION_OPENED"},
        payload={"symbol": "XAUUSD"},
    )


def test_provider_gate_follows_configured_macro_policy_not_intermarket_availability(
    tmp_path,
):
    path = write_config(
        tmp_path, config_mapping("shadow", tmp_path / "state")
    )
    app = build_runtime_app(
        mode="shadow", config_path=path, environ=broker_env(), factory=FakeFactory()
    )
    unavailable_macro = provider_snapshot("macro", eligible=False)
    unavailable_intermarket = provider_snapshot("intermarket", eligible=False)

    # SHADOW explicitly disables provider-dependent voters in this validated
    # config, so unavailable external feeds cannot prevent observation.
    assert app._entry_inputs_ready(
        ProviderContexts(
            None,
            None,
            unavailable_macro,
            unavailable_intermarket,
        )
    )

    # When macro protection is configured it remains a global entry gate, but
    # an intermarket outage only disables Agents 12/15.
    app.config = replace(
        app.config,
        providers=ProvidersSettings(
            macro=ProviderSettings(
                enabled=True,
                source=ProviderSource.HTTP,
                url="https://macro.example.test/events",
            ),
            intermarket=ProviderSettings(),
        ),
    )
    healthy_macro = provider_snapshot("macro", eligible=True)
    contexts = ProviderContexts(
        SimpleNamespace(freeze_active=False),
        None,
        healthy_macro,
        unavailable_intermarket,
    )
    assert app._entry_inputs_ready(contexts)
    assert not app._entry_inputs_ready(
        replace(contexts, macro_snapshot=unavailable_macro)
    )


def test_demo_unsafe_macro_override_is_an_explicit_ready_policy(tmp_path):
    path = write_config(
        tmp_path, config_mapping("demo", tmp_path / "state")
    )
    app = build_runtime_app(
        mode="demo", config_path=path, environ=broker_env(), factory=FakeFactory()
    )
    app._transaction_consumer = FakeIngress([], ready=True)
    app._command_producer_ready = True
    unavailable = ProviderContexts(
        None,
        None,
        provider_snapshot("macro", eligible=False),
        provider_snapshot("intermarket", eligible=False),
    )

    assert app.config.providers.unsafe_allow_technical_without_macro is True
    assert app._entry_inputs_ready(unavailable)


@async_test
async def test_serve_contains_and_exposes_unexpected_dispatcher_termination(
    tmp_path, caplog
):
    stop_event = asyncio.Event()

    class BlockingGateway(FakeGateway):
        async def next_signal(self, timeout=None):
            del timeout
            await asyncio.Future()

    class FailingDispatcher:
        async def run(self):
            await asyncio.sleep(0)
            stop_event.set()
            raise RuntimeError("dispatcher crashed")

    class DispatcherFactory(FakeFactory):
        def make_gateway(
            self,
            *,
            config,
            config_hash,
            environ,
            ledger,
            runtime,
            clock,
            tuple_lock_acquirer,
        ):
            self.calls.append("gateway.make")
            self.gateway = BlockingGateway(
                self.calls, execution_enabled=config.mode is RuntimeMode.DEMO
            )
            self.tuple_lock_acquirer = tuple_lock_acquirer
            return self.gateway

        def make_dispatcher(
            self, *, config, config_hash, environ, ledger, clock
        ):
            self.calls.append("dispatcher.make")
            return FailingDispatcher()

    path = write_config(
        tmp_path, config_mapping("shadow", tmp_path / "state")
    )
    app = build_runtime_app(
        mode="shadow",
        config_path=path,
        environ=broker_env(),
        factory=DispatcherFactory(),
    )

    with caplog.at_level("ERROR", logger="QUANT-SWARM-RUNTIME"):
        await app.serve(stop_event, signal_timeout_s=0.01)
    try:
        assert app.status.dispatcher_running is False
        assert app.status.dispatcher_error == "RuntimeError"
        assert "Integration dispatcher stopped unexpectedly" in caplog.text
        assert app.runtime.state is ControlPlaneState.WAITING_EA
    finally:
        await app.shutdown()


def test_effective_hash_is_semantic_deterministic_and_secret_free(tmp_path):
    first = RuntimeConfig.from_mapping(config_mapping("check", tmp_path))
    same = RuntimeConfig.from_mapping(config_mapping("check", tmp_path))
    changed = RuntimeConfig.from_mapping(
        {**config_mapping("check", tmp_path), "risk": {"max_risk_pct": 0.5}}
    )

    assert effective_config_hash(first) == effective_config_hash(same)
    assert effective_config_hash(first) != effective_config_hash(changed)
    assert len(effective_config_hash(first)) == 64


def test_launcher_rejects_live_and_exact_mode_mismatch_before_startup(tmp_path):
    with pytest.raises(RuntimeModeError, match="unsupported canonical launch mode"):
        build_runtime_app(mode="live", config_path=tmp_path / "missing.json")

    path = write_config(tmp_path, config_mapping("check", tmp_path / "state"))
    with pytest.raises(RuntimeModeError, match="differs"):
        build_runtime_app(mode="shadow", config_path=path, environ=broker_env())


@async_test
async def test_check_bootstraps_real_private_ledger_without_broker_components(tmp_path):
    root = tmp_path / "state"
    path = write_config(tmp_path, config_mapping("check", root))
    app = build_runtime_app(mode="check", config_path=path, environ={})

    status = await app.startup()
    try:
        assert status.phase is AppPhase.CHECKED
        assert status.control_plane is ControlPlaneState.STOPPED
        assert app.paths.trade_ledger_path.is_file()
        assert app.paths.instance_root.stat().st_mode & 0o777 == 0o700
    finally:
        await app.shutdown()
    assert app.phase is AppPhase.STOPPED


@async_test
async def test_broker_preflight_precedes_every_mutating_startup_step(tmp_path):
    root = tmp_path / "state"
    path = write_config(tmp_path, config_mapping("shadow", root))
    factory = FakeFactory(preflight_error=RuntimeError("untrusted release"))
    app = build_runtime_app(
        mode="shadow", config_path=path, environ=broker_env(), factory=factory
    )

    with pytest.raises(RuntimeError, match="untrusted"):
        await app.startup()
    assert factory.calls == ["preflight"]
    assert not root.exists()
    assert app.phase is AppPhase.FAILED


@async_test
async def test_broker_startup_order_locks_tuple_and_shadow_never_requires_autotrading(
    tmp_path,
):
    root = tmp_path / "state"
    path = write_config(tmp_path, config_mapping("shadow", root))
    factory = FakeFactory()
    app = build_runtime_app(
        mode="shadow", config_path=path, environ=broker_env(), factory=factory
    )

    await app.startup()
    try:
        assert factory.calls == [
            "preflight",
            "ledger.make",
            "ledger.open",
            "ledger.health",
            "providers.make",
            "providers.poll",
            "gateway.make",
            "reconciler.make",
            "ingress.make",
            "gateway.start",
        ]
        assert app.phase is AppPhase.WAITING_EA
        assert factory.gateway.execution_enabled is False
        authenticate(app, factory, permissions=False)
        assert app.runtime.state is ControlPlaneState.SYNCHRONIZING
        await app.handle_signal(connected_signal())
        assert app.runtime.state is ControlPlaneState.READY

        with pytest.raises(RuntimeCapabilityError, match="no order-send"):
            await app.send_command(outbound_command())
        assert not factory.gateway.sent
    finally:
        await app.shutdown()
    assert factory.calls[-2:] == ["gateway.close", "ledger.close"]


@async_test
async def test_cross_mode_tuple_lock_blocks_second_runtime_before_ledger(tmp_path):
    root = tmp_path / "state"
    shadow_path = write_config(
        tmp_path,
        config_mapping("shadow", root, instance_id="observer"),
        "shadow.json",
    )
    demo_path = write_config(
        tmp_path,
        config_mapping("demo", root, instance_id="executor"),
        "demo.json",
    )
    first_factory = FakeFactory()
    second_factory = FakeFactory()
    shadow = build_runtime_app(
        mode="shadow",
        config_path=shadow_path,
        environ=broker_env(),
        factory=first_factory,
    )
    demo = build_runtime_app(
        mode="demo",
        config_path=demo_path,
        environ=broker_env(),
        factory=second_factory,
    )

    await shadow.startup()
    try:
        with pytest.raises(ProcessLockError, match="another runtime owns"):
            await demo.startup()
        assert second_factory.calls == ["preflight"]
    finally:
        await shadow.shutdown()


@async_test
async def test_demo_remains_blocked_without_production_command_producer(tmp_path):
    path = write_config(
        tmp_path, config_mapping("demo", tmp_path / "state")
    )
    factory = FakeFactory()
    app = build_runtime_app(
        mode="demo", config_path=path, environ=broker_env(), factory=factory
    )

    await app.startup()
    try:
        assert app.status.command_producer_ready is False
        assert app.status.execution_enabled is False
        assert "command_producer_not_composed" in app.status.blockers
        with pytest.raises(RuntimeCapabilityError, match="producer is not composed"):
            await app.send_command(outbound_command())
        authenticate(app, factory)
        await app.handle_signal(connected_signal())
        assert app.runtime.state is ControlPlaneState.DEGRADED
        with pytest.raises(RuntimeCapabilityError, match="producer is not composed"):
            await app.send_command(outbound_command())
        assert factory.gateway.sent == []
    finally:
        await app.shutdown()


@async_test
async def test_missing_fill_deal_forces_exact_reconciliation_without_ingestion(tmp_path):
    path = write_config(
        tmp_path, config_mapping("shadow", tmp_path / "state")
    )
    factory = FakeFactory()
    app = build_runtime_app(
        mode="shadow", config_path=path, environ=broker_env(), factory=factory
    )
    await app.startup()
    try:
        authenticate(app, factory, permissions=False)
        await app.handle_signal(connected_signal())
        assert factory.reconciler.count == 1
        event, _deal = fill_transaction()
        await app.handle_signal(
            GatewaySignal(
                GatewaySignalType.BROKER_EVENT,
                NOW,
                "epoch-1",
                command_id=event.command_id,
                trade_id=event.trade_id,
                payload={"event": event, "deal": None},
            )
        )
        assert factory.reconciler.count == 2
        assert factory.ingress.transactions == []
        assert app.runtime.state is ControlPlaneState.READY
    finally:
        await app.shutdown()


@async_test
async def test_exact_fill_event_and_deal_are_consumed_once(tmp_path):
    path = write_config(
        tmp_path, config_mapping("shadow", tmp_path / "state")
    )
    factory = FakeFactory()
    app = build_runtime_app(
        mode="shadow", config_path=path, environ=broker_env(), factory=factory
    )
    await app.startup()
    try:
        authenticate(app, factory, permissions=False)
        await app.handle_signal(connected_signal())
        event, deal = fill_transaction()
        await app.handle_signal(
            GatewaySignal(
                GatewaySignalType.BROKER_EVENT,
                NOW,
                "epoch-1",
                command_id=event.command_id,
                trade_id=event.trade_id,
                payload={"event": event, "deal": deal},
            )
        )
        assert factory.ingress.transactions == [(event, deal)]
        assert factory.reconciler.count == 1
    finally:
        await app.shutdown()


@async_test
async def test_conflicting_fill_economics_are_not_delivered_to_ingress(tmp_path):
    path = write_config(
        tmp_path, config_mapping("shadow", tmp_path / "state")
    )
    factory = FakeFactory()
    app = build_runtime_app(
        mode="shadow", config_path=path, environ=broker_env(), factory=factory
    )
    await app.startup()
    try:
        authenticate(app, factory, permissions=False)
        await app.handle_signal(connected_signal())
        event, deal = fill_transaction()
        conflict = replace(deal, profit=Decimal("99"))
        await app.handle_signal(
            GatewaySignal(
                GatewaySignalType.BROKER_EVENT,
                NOW,
                "epoch-1",
                payload={"event": event, "deal": conflict},
            )
        )
        assert factory.ingress.transactions == []
        assert factory.reconciler.count == 2
    finally:
        await app.shutdown()


@async_test
async def test_gateway_and_runtime_permission_fences_make_shadow_observe_only():
    expected = expected_identity()
    actual = actual_identity(permissions=False)
    assert identity_mismatches(
        expected, actual, require_execution_permissions=False
    ) == ()
    assert "trading disabled" in ";".join(identity_mismatches(expected, actual))

    gateway = BrokerGateway(
        config=GatewayConfig(
            auth_token="token",
            config_hash="cfg",
            expected=expected,
            session_mode=BrokerSessionMode.SHADOW,
        ),
        clock=FixedClock(NOW),
    )
    with pytest.raises(GatewayError, match="transmission is disabled"):
        await gateway.send(outbound_command())


@async_test
async def test_canonical_open_transaction_consumer_uses_exact_execution_ingress():
    calls = []

    class ExecutionSpy:
        def ingest_open_transaction(self, event, **kwargs):
            calls.append((event, kwargs))
            return True

    class LifecycleSpy:
        def ingest_broker_event(self, event, **kwargs):
            raise AssertionError("open events must not enter lifecycle ingestion")

    consumer = CanonicalBrokerTransactionConsumer(
        execution=ExecutionSpy(),
        lifecycle=LifecycleSpy(),
        account_id="demo-10001",
        symbol="XAUUSD",
        magic=202412,
    )
    event = BrokerEvent(
        event_id="open-event-1",
        event_type=BrokerEventType.POSITION_OPENED,
        broker_time_ms=NOW - 50,
        received_at_ms=NOW,
        command_id="open-command-1",
        trade_id="trade-1",
        broker_position_id="position-1",
        position_ticket="ticket-1",
        order_id="order-open-1",
        deal_id="deal-open-1",
        fill_price="2500",
        fill_volume="0.1",
        remaining_volume="0.1",
        sl="2495",
        tp="2515",
        position_version=1,
    )
    deal = BrokerDealRecord(
        account_id="demo-10001",
        deal_id="deal-open-1",
        position_id="position-1",
        order_id="order-open-1",
        deal_type="IN",
        volume="0.1",
        price="2500",
        profit="0",
        commission="-0.1",
        swap="0",
        fee="0",
        occurred_at=(NOW - 50) / 1000,
        trade_id="trade-1",
        command_id="open-command-1",
    )

    assert await consumer.ingest(event, deal) is True
    assert calls == [
        (
            event,
            {
                "account_id": "demo-10001",
                "symbol": "XAUUSD",
                "magic": 202412,
                "deal": deal,
            },
        )
    ]


@async_test
async def test_default_reconciler_wires_configured_risk_and_recovery_outbox(tmp_path):
    raw = config_mapping("demo", tmp_path / "state")
    raw["risk"] = {
        "max_risk_pct": 0.5,
        "daily_max_drawdown_pct": 2.0,
        "max_total_risk_pct": 0.75,
        "max_open_positions": 2,
        "consecutive_loss_halt": 4,
    }
    config = RuntimeConfig.from_mapping(raw, environ=broker_env())
    config = replace(
        config,
        integrations=IntegrationsSettings(
            telegram=True,
            discord=True,
            website=True,
        ),
    )
    paths = RuntimePaths.resolve(
        "demo",
        data_dir=tmp_path / "state",
        instance_id="factory-test",
    ).ensure_layout()
    ledger = TradeLedger(paths.trade_ledger_path).open()
    clock = FixedClock(NOW)

    class ReconciliationBroker:
        async def fetch_snapshot(self, expected):
            raise AssertionError("factory wiring test does not reconcile")

        async def fetch_deal_page(self, account_id, cursor):
            raise AssertionError("factory wiring test does not reconcile")

    from python_swarm.runtime_state import RuntimeStateMachine

    reconciler = DefaultRuntimeComponentFactory().make_reconciler(
        config=config,
        ledger=ledger,
        runtime=RuntimeStateMachine(clock),
        gateway=ReconciliationBroker(),
        clock=clock,
    )
    try:
        assert reconciler._drawdown_limit == Decimal("2")
        assert reconciler._loss_limit == 4
        assert reconciler._max_total_risk_pct == Decimal("0.75")
        assert reconciler._max_open_positions == 2
        event = TradeEventRecord(
            event_id="recovery-close-1",
            event_type="POSITION_CLOSED_SL_RECONCILED",
            source="reconciliation",
            occurred_at=NOW / 1000,
            payload={"broker_position_id": "position-1"},
            trade_id="trade-1",
            correlation_id="deal-1",
            broker_timestamp=NOW / 1000,
        )
        records = reconciler._outbox_for(event)
        assert {item.destination for item in records} == {
            "telegram",
            "discord",
            "website",
        }
        assert {item.topic for item in records} == {
            "reconciliation.position_closed"
        }
        assert all(item.event_id == event.event_id for item in records)
    finally:
        ledger.close()



@async_test
async def test_broker_snapshot_disconnect_during_connected_reconciliation_is_contained(
    tmp_path,
):
    from python_swarm.reconciliation import BrokerSnapshotError

    path = write_config(
        tmp_path,
        config_mapping("shadow", tmp_path / "state"),
    )
    factory = FakeFactory()
    app = build_runtime_app(
        mode="shadow",
        config_path=path,
        environ=broker_env(),
        factory=factory,
    )

    await app.startup()

    try:
        authenticate(app, factory, permissions=False)

        async def fail_reconciliation(_expected):
            app.runtime.disconnect(
                "broker snapshot failed: GatewayDisconnectedError"
            )
            raise BrokerSnapshotError(
                "cannot obtain authoritative broker snapshot"
            )

        factory.reconciler.reconcile = fail_reconciliation
        app.last_reconciliation = object()

        await app.handle_signal(connected_signal())

        assert app.phase.value == "DISCONNECTED"
        assert app.runtime.state.value == "DISCONNECTED"
        assert app.last_reconciliation is None
    finally:
        await app.shutdown()


@async_test
async def test_unexpected_connected_reconciliation_error_still_propagates(
    tmp_path,
):
    import pytest

    path = write_config(
        tmp_path,
        config_mapping("shadow", tmp_path / "state"),
    )
    factory = FakeFactory()
    app = build_runtime_app(
        mode="shadow",
        config_path=path,
        environ=broker_env(),
        factory=factory,
    )

    await app.startup()

    try:
        authenticate(app, factory, permissions=False)

        async def fail_reconciliation(_expected):
            raise RuntimeError("unexpected reconciliation defect")

        factory.reconciler.reconcile = fail_reconciliation

        with pytest.raises(
            RuntimeError,
            match="unexpected reconciliation defect",
        ):
            await app.handle_signal(connected_signal())
    finally:
        await app.shutdown()
