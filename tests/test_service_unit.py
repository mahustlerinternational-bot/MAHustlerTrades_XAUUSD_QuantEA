from pathlib import Path


def test_systemd_service_uses_virtualenv():
    service_path = Path(__file__).resolve().parent.parent / "deployment" / "quant_swarm_xau.service"
    text = service_path.read_text()

    assert "/opt/quant-swarm-xau/.venv/bin/python3" in text
    assert "-m python_swarm.runtime_app --mode shadow" in text
    assert "Environment=QSX_MODE=shadow" in text
    assert "Environment=QSX_CONFIG=/etc/quant-swarm-xau/runtime.json" in text
    assert "Environment=QSX_DATA_DIR=/var/lib/quant-swarm-xau" in text
    assert "EnvironmentFile=-/etc/quant-swarm-xau/quant-swarm.env" in text
    assert "StateDirectory=quant-swarm-xau" in text
    assert "WorkingDirectory=/opt/quant-swarm-xau\n" in text
    assert "--live" not in text
    assert "evolved_orchestrator" not in text
    assert "WatchdogSec" not in text
