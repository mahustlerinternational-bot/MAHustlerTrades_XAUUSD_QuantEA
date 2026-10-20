import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from python_swarm import setup_check
from python_swarm.runtime_config import RuntimeMode, load_runtime_config
from python_swarm.setup_check import DeploymentIntegrityError


ROOT = Path(__file__).resolve().parents[1]
START = ROOT / "deployment" / "start.sh"


def _start_env(tmp_path: Path, **changes: str) -> dict[str, str]:
    return {
        **os.environ,
        "QSX_ENV_FILE": str(tmp_path / "absent.env"),
        "QSX_PYTHON_BIN": "/bin/echo",
        "QSX_CONFIG": str(ROOT / "deployment" / "config.check.example.json"),
        "QSX_DATA_DIR": str(tmp_path / "data"),
        **changes,
    }


def test_start_wrapper_only_executes_canonical_runtime(tmp_path):
    completed = subprocess.run(
        [str(START), "--check"],
        cwd=tmp_path,
        env=_start_env(tmp_path),
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == (
        "-m python_swarm.runtime_app --mode check --config "
        f"{ROOT / 'deployment' / 'config.check.example.json'}"
    )


def test_start_wrapper_defaults_to_non_order_producing_shadow(tmp_path):
    completed = subprocess.run(
        [str(START)],
        cwd=tmp_path,
        env=_start_env(tmp_path),
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert "--mode shadow" in completed.stdout


@pytest.mark.parametrize("retired", ["--live", "--sim", "--dashboard"])
def test_start_wrapper_rejects_unavailable_modes(tmp_path, retired):
    completed = subprocess.run(
        [str(START), retired],
        cwd=tmp_path,
        env=_start_env(tmp_path),
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    assert "python_swarm.runtime_app" not in completed.stdout


def test_environment_file_is_parsed_as_data_not_executed(tmp_path):
    env_file = tmp_path / "runtime.env"
    sentinel = tmp_path / "must-not-exist"
    env_file.write_text(
        "QSX_CONFIG="
        + str(ROOT / "deployment" / "config.check.example.json")
        + "\n"
        "QSX_DATA_DIR=" + str(tmp_path / "data") + "\n"
        "UNTRUSTED=$(touch " + str(sentinel) + ")\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)
    env = _start_env(tmp_path, QSX_ENV_FILE=str(env_file))

    completed = subprocess.run(
        [str(START), "--check"],
        cwd=tmp_path,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert not sentinel.exists()


def test_setup_check_missing_config_has_nonzero_exit():
    completed = subprocess.run(
        [sys.executable, "-m", "python_swarm.setup_check", "--mode", "check"],
        cwd=ROOT,
        env={key: value for key, value in os.environ.items() if key != "QSX_CONFIG"},
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    assert "QSX_CONFIG is required" in completed.stderr


def test_server_setup_rejects_unknown_arguments_before_mutation():
    completed = subprocess.run(
        [str(ROOT / "server_setup.sh"), "--unknown"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    assert "Usage:" in completed.stderr


def test_setup_report_fails_when_a_required_module_cannot_import(tmp_path, monkeypatch):
    real_import = setup_check.importlib.import_module

    def broken_import(name):
        if name == "python_swarm.broker_gateway":
            raise ImportError("deliberate test failure")
        return real_import(name)

    monkeypatch.setattr(
        setup_check.importlib.util,
        "find_spec",
        lambda name: SimpleNamespace(origin="runtime_app.py"),
    )
    monkeypatch.setattr(setup_check.importlib, "import_module", broken_import)
    report = setup_check.run_setup_check(
        mode="check",
        config_path=ROOT / "deployment" / "config.check.example.json",
        environ={"QSX_DATA_DIR": str(tmp_path / "data")},
    )

    assert not report.ok
    failed = [result for result in report.results if not result.ok]
    assert any(result.name == "import python_swarm.broker_gateway" for result in failed)


def test_shadow_example_is_strictly_valid_with_demo_identity(tmp_path):
    config = load_runtime_config(
        ROOT / "deployment" / "config.shadow.example.json",
        environ={
            "QSX_EXPECTED_ACCOUNT_ID": "123456",
            "QSX_EXPECTED_BROKER_SERVER": "Broker-Demo",
            "QSX_IPC_AUTH_SECRET": "s" * 32,
            "QSX_DATA_DIR": str(tmp_path / "runtime"),
        },
    )

    assert config.mode is RuntimeMode.SHADOW
    assert not config.execution_enabled
    assert config.system.expected_account_trade_mode.value == "DEMO"


def test_integrity_check_accepts_only_nonwritable_trusted_paths(tmp_path, monkeypatch):
    release = tmp_path / "release"
    package = release / "python_swarm"
    package.mkdir(parents=True)
    config = tmp_path / "etc" / "runtime.json"
    config.parent.mkdir()
    config.write_text("{}", encoding="utf-8")

    real_access = setup_check.os.access

    def immutable_access(path, mode, *args, **kwargs):
        if mode == os.X_OK:
            return real_access(path, mode)
        if mode == os.W_OK:
            return False
        return real_access(path, mode)

    monkeypatch.setattr(setup_check.os, "geteuid", lambda: 1000)
    monkeypatch.setattr(setup_check.sys, "version_info", (3, 12, 0))
    monkeypatch.setattr(setup_check.os, "access", immutable_access)
    monkeypatch.setattr(setup_check, "_identity_has_write_bits", lambda metadata: False)

    checked = setup_check.assert_broker_deployment_integrity(
        mode="shadow",
        config_path=config,
        project_root=release,
        python_executable=sys.executable,
        module_search_paths=(release,),
    )

    assert release.absolute() in checked
    assert config.absolute() in checked


def test_integrity_check_rejects_service_writable_config(tmp_path, monkeypatch):
    release = tmp_path / "release"
    (release / "python_swarm").mkdir(parents=True)
    config = tmp_path / "runtime.json"
    config.write_text("{}", encoding="utf-8")
    config = config.absolute()
    real_access = setup_check.os.access

    def config_writable(path, mode, *args, **kwargs):
        if mode == os.X_OK:
            return real_access(path, mode)
        if mode == os.W_OK:
            return Path(path).absolute() == config
        return real_access(path, mode)

    monkeypatch.setattr(setup_check.os, "geteuid", lambda: 1000)
    monkeypatch.setattr(setup_check.sys, "version_info", (3, 12, 0))
    monkeypatch.setattr(setup_check.os, "access", config_writable)
    monkeypatch.setattr(setup_check, "_identity_has_write_bits", lambda metadata: False)

    with pytest.raises(DeploymentIntegrityError, match="runtime config path is writable"):
        setup_check.assert_broker_deployment_integrity(
            mode="demo",
            config_path=config,
            project_root=release,
            python_executable=sys.executable,
            module_search_paths=(release,),
        )


def test_integrity_check_rejects_individually_writable_module(tmp_path, monkeypatch):
    release = tmp_path / "release"
    package = release / "python_swarm"
    package.mkdir(parents=True)
    module = (package / "runtime_app.py").absolute()
    module.write_text("", encoding="utf-8")
    config = tmp_path / "runtime.json"
    config.write_text("{}", encoding="utf-8")
    real_access = setup_check.os.access

    def module_writable(path, mode, *args, **kwargs):
        if mode == os.X_OK:
            return real_access(path, mode)
        if mode == os.W_OK:
            return Path(path).absolute() == module
        return real_access(path, mode)

    monkeypatch.setattr(setup_check.os, "geteuid", lambda: 1000)
    monkeypatch.setattr(setup_check.sys, "version_info", (3, 12, 0))
    monkeypatch.setattr(setup_check.os, "access", module_writable)
    monkeypatch.setattr(setup_check, "_identity_has_write_bits", lambda metadata: False)

    with pytest.raises(DeploymentIntegrityError, match="python module path is writable"):
        setup_check.assert_broker_deployment_integrity(
            mode="shadow",
            config_path=config,
            project_root=release,
            python_executable=sys.executable,
            module_search_paths=(release,),
        )


def test_integrity_check_rejects_root_before_broker_start(tmp_path, monkeypatch):
    monkeypatch.setattr(setup_check.os, "geteuid", lambda: 0)

    with pytest.raises(DeploymentIntegrityError, match="must not run as root"):
        setup_check.assert_broker_deployment_integrity(
            mode="shadow",
            config_path=tmp_path / "missing.json",
        )


def test_mode_bit_check_is_not_masked_by_access_result(tmp_path):
    artifact = tmp_path / "module.py"
    artifact.write_text("", encoding="utf-8")
    artifact.chmod(0o600)
    assert setup_check._identity_has_write_bits(artifact.stat())

    artifact.chmod(0o400)
    assert not setup_check._identity_has_write_bits(artifact.stat())


def test_docker_default_is_nonroot_canonical_check():
    text = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "USER trader" in text
    assert "WORKDIR /opt/quant-swarm-xau\n" in text
    assert "QSX_CONFIG=/etc/quant-swarm-xau/runtime.json" in text
    assert "QSX_DATA_DIR=/var/lib/quant-swarm-xau" in text
    assert 'CMD ["./deployment/start.sh", "--check"]' in text
    assert "deployment/config.json" not in text
    assert "QSX_STATE_DIR" not in text
    assert "QSX_LOG_DIR" not in text


def test_production_dependencies_are_exactly_pinned_and_hash_locked():
    lock_path = ROOT / "python_swarm" / "requirements-runtime-py312.lock"
    lock = lock_path.read_text(encoding="utf-8")
    logical_requirements: list[str] = []
    current: list[str] = []
    for raw_line in lock.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        current.append(line)
        if not line.endswith("\\"):
            logical_requirements.append(" ".join(current))
            current = []

    assert current == []
    assert logical_requirements
    assert any(item.startswith("aiohttp==") for item in logical_requirements)
    assert any(item.startswith("numpy==") for item in logical_requirements)
    for requirement in logical_requirements:
        assert "==" in requirement
        assert "--hash=sha256:" in requirement

    docker = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    setup = (ROOT / "server_setup.sh").read_text(encoding="utf-8")
    assert "requirements-runtime-py312.lock" in docker
    assert "--require-hashes" in docker
    assert "requirements-runtime-py312.lock" in setup
    assert "--require-hashes" in setup
    assert "--no-index" in docker
    assert "--no-index" in setup
    assert "--only-binary=:all:" in docker
    assert "--only-binary=:all:" in setup


def test_operator_launch_files_contain_no_retired_runtime():
    files = (
        ROOT / "deployment" / "start.sh",
        ROOT / "deployment" / "quant_swarm_xau.service",
        ROOT / "Dockerfile",
        ROOT / "server_setup.sh",
        ROOT / "QUICK_START.sh",
    )
    prohibited = ("evolved_orchestrator", "orchestrator.py", "ipc_bridge.py", "--live")

    for path in files:
        text = path.read_text(encoding="utf-8")
        assert not any(value in text for value in prohibited), path


def test_demo_forward_runbook_has_complete_evidence_gate():
    text = (ROOT / "deployment" / "DEMO_FORWARD_TEST_RUNBOOK.md").read_text(
        encoding="utf-8"
    ).lower()

    for required in (
        "has **not** been\ncompleted",
        "30 trading days",
        "60 calendar days",
        "resets day zero",
        "broker-versus-ledger",
        "zero unexplained",
        "credential",
        "signed, read-only build manifest",
        "freshly produced ex5",
        "sha-256",
    ):
        assert required in text
