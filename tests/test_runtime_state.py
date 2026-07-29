import pytest

from python_swarm.domain_models import FixedClock
from python_swarm.runtime_state import (
    BrokerIdentity,
    BrokerTradeMode,
    ControlPlaneState,
    ExpectedBrokerIdentity,
    IllegalTransitionError,
    RuntimeNotReadyError,
    RuntimeStateMachine,
)


def expected(**changes):
    values = {
        "account_id": "demo-10001",
        "broker_server": "Broker-Demo",
        "symbol": "XAUUSD",
        "magic": 202412,
    }
    values.update(changes)
    return ExpectedBrokerIdentity(**values)


def actual(**changes):
    values = {
        "account_id": "demo-10001",
        "broker_server": "Broker-Demo",
        "trade_mode": BrokerTradeMode.DEMO,
        "symbol": "XAUUSD",
        "magic": 202412,
        "connection_epoch": "epoch-1",
        "trade_allowed": True,
        "expert_allowed": True,
    }
    values.update(changes)
    return BrokerIdentity(**values)


def synchronizing_runtime():
    runtime = RuntimeStateMachine(FixedClock(1_000))
    runtime.start_bootstrap()
    runtime.wait_for_ea(ledger_healthy=True, provider_gate_safe=True)
    runtime.accept_connection(
        expected(), actual(), transport_loopback=True, tuple_lock_acquired=True
    )
    return runtime


def test_canonical_states_and_legal_graph_are_fail_closed():
    assert {item.value for item in ControlPlaneState} == {
        "STOPPED",
        "BOOTSTRAPPING",
        "WAITING_EA",
        "SYNCHRONIZING",
        "READY",
        "DEGRADED",
        "HALTED",
        "DISCONNECTED",
    }
    runtime = RuntimeStateMachine(FixedClock(1))
    with pytest.raises(IllegalTransitionError):
        runtime.transition(ControlPlaneState.READY, "unsafe shortcut")
    with pytest.raises(RuntimeNotReadyError):
        runtime.require_ready("open")


def test_ready_requires_every_evidence_item_and_invalidates_to_degraded():
    runtime = synchronizing_runtime()
    runtime.mark_synchronized(
        snapshot_id="snapshot-1",
        reconciliation_run_id="run-1",
        deal_cursor_initialized=True,
        daily_baseline_verified=True,
        daily_risk_halted=False,
    )
    assert runtime.state is ControlPlaneState.READY
    runtime.require_ready("open")

    runtime.update_evidence(provider_gate_safe=False)
    assert runtime.state is ControlPlaneState.DEGRADED
    with pytest.raises(RuntimeNotReadyError, match="provider_gate_safe"):
        runtime.require_ready("open")


def test_real_account_without_explicit_authorization_halts():
    runtime = RuntimeStateMachine(FixedClock(1))
    runtime.start_bootstrap()
    runtime.wait_for_ea(ledger_healthy=True, provider_gate_safe=True)
    runtime.accept_connection(
        expected(),
        actual(trade_mode=BrokerTradeMode.REAL),
        transport_loopback=True,
        tuple_lock_acquired=True,
    )
    assert runtime.state is ControlPlaneState.HALTED
    assert "real account" in runtime.halt_reason


def test_disconnect_clears_connection_and_reconciliation_evidence():
    runtime = synchronizing_runtime()
    runtime.disconnect("heartbeat timeout")
    assert runtime.state is ControlPlaneState.DISCONNECTED
    assert not runtime.evidence.ea_connected
    assert runtime.evidence.connection_epoch is None
    assert not runtime.evidence.tuple_lock_acquired
