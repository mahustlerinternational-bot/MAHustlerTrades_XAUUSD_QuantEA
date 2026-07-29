from pathlib import Path

import pytest

from python_swarm.legacy_runtime_guard import require_non_broker_mode


ROOT = Path(__file__).resolve().parent.parent


@pytest.mark.parametrize("mode", ["live", "demo", "shadow", "LIVE"])
def test_legacy_components_fail_closed_in_broker_modes(monkeypatch, mode):
    monkeypatch.setenv("QSX_MODE", mode)

    with pytest.raises(RuntimeError, match="legacy pre-v4"):
        require_non_broker_mode("legacy-test")


def test_legacy_components_remain_available_for_simulation(monkeypatch):
    monkeypatch.setenv("QSX_MODE", "simulation")

    require_non_broker_mode("legacy-test")


def test_command_capable_legacy_entrypoints_use_the_guard():
    evolved = (ROOT / "python_swarm" / "evolved_orchestrator.py").read_text()
    bridge = (ROOT / "python_swarm" / "ipc_bridge.py").read_text()
    standalone = (ROOT / "python_swarm" / "run_ipc.py").read_text()

    assert 'require_non_broker_mode("EvolvedOrchestrator")' in evolved
    assert 'require_non_broker_mode("IPCBridge")' in bridge
    assert 'require_non_broker_mode("run_ipc")' in standalone
    assert 'asyncio.start_server(handle_client, HOST, PORT)' in standalone
    assert "unauthenticated v3 standalone IPC bridge is disabled" in standalone
