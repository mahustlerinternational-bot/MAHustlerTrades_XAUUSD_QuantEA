import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
ALLOWLIST = ROOT / "release" / "runtime-allowlist.txt"
BUILD = ROOT / "tools" / "build_release.py"
VALIDATE = ROOT / "tools" / "validate_release.py"
SCAN = ROOT / "tools" / "scan_secrets.py"

CANONICAL_PYTHON = {
    "python_swarm/__init__.py",
    "python_swarm/agent_roster.py",
    "python_swarm/broker_gateway.py",
    "python_swarm/broker_protocol.py",
    "python_swarm/decision_service.py",
    "python_swarm/domain_models.py",
    "python_swarm/execution_service.py",
    "python_swarm/integration_dispatcher.py",
    "python_swarm/learning_policy.py",
    "python_swarm/learning_service.py",
    "python_swarm/lifecycle_service.py",
    "python_swarm/market_snapshot_service.py",
    "python_swarm/provider_adapters.py",
    "python_swarm/reconciliation.py",
    "python_swarm/runtime_app.py",
    "python_swarm/runtime_config.py",
    "python_swarm/runtime_paths.py",
    "python_swarm/runtime_scheduler.py",
    "python_swarm/runtime_state.py",
    "python_swarm/setup_check.py",
    "python_swarm/strategy_pipeline.py",
    "python_swarm/trade_ledger.py",
    "python_swarm/trading_policies.py",
    "python_swarm/version.py",
}


def _run(*command: str | os.PathLike[str], **kwargs) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [os.fspath(part) for part in command],
        check=False,
        capture_output=True,
        text=True,
        **kwargs,
    )


def _allowlisted_paths() -> list[str]:
    return [
        line
        for raw in ALLOWLIST.read_text(encoding="utf-8").splitlines()
        if (line := raw.strip()) and not line.startswith("#")
    ]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _create_release_fixture(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    source = tmp_path / "source"
    source.mkdir()
    for relative in _allowlisted_paths():
        origin = ROOT / relative
        target = source / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(origin, target)

    _run("git", "init", "-b", "main", cwd=source)
    _run("git", "add", "--all", cwd=source)
    committed = _run(
        "git",
        "-c",
        "user.name=Release Contract",
        "-c",
        "user.email=release-contract@example.invalid",
        "commit",
        "-m",
        "fixture",
        cwd=source,
        env={**os.environ, "TZ": "UTC"},
    )
    assert committed.returncode == 0, committed.stderr

    ex5 = tmp_path / "QuantSwarmXAU.ex5"
    ex5.write_bytes(b"EX5-NATIVE-COMPILE-TEST-FIXTURE\0")
    mq5 = source / "mql5_ea" / "QuantSwarmXAU.mq5"
    compile_log = tmp_path / "MetaEditor.log"
    compile_log.write_text("0 errors, 0 warnings\n", encoding="utf-8")
    compile_evidence = tmp_path / "MQL5_COMPILE.json"
    compile_evidence.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "compiler": "MetaEditor64.exe",
                "compiler_version": "contract-fixture",
                "exit_code": 0,
                "errors": 0,
                "warnings": 0,
                "source_sha256": _sha256(mq5),
                "binary_sha256": _sha256(ex5),
                "compile_log_sha256": _sha256(compile_log),
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    signing_key = tmp_path / "release-signing"
    generated = _run(
        "ssh-keygen",
        "-q",
        "-t",
        "ed25519",
        "-N",
        "",
        "-C",
        "release-contract",
        "-f",
        signing_key,
    )
    assert generated.returncode == 0, generated.stderr
    public_fields = signing_key.with_suffix(".pub").read_text(encoding="utf-8").split()
    allowed_signers = tmp_path / "allowed_signers"
    allowed_signers.write_text(
        f"mahustlertrades-release {public_fields[0]} {public_fields[1]}\n",
        encoding="utf-8",
    )
    return source, ex5, compile_evidence, signing_key, allowed_signers


def test_runtime_allowlist_is_exact_sorted_and_has_no_release_contamination():
    paths = _allowlisted_paths()
    assert paths == sorted(paths)
    assert len(paths) == len(set(paths))
    assert {path for path in paths if path.startswith("python_swarm/") and path.endswith(".py")} == (
        CANONICAL_PYTHON
    )
    for forbidden in (
        "deployment/config.json",
        "deployment/config.small-account.json",
        "python_swarm/APPLY_FIX.py",
        "python_swarm/evolved_orchestrator.py",
        "python_swarm/orchestrator.py",
    ):
        assert forbidden not in paths
    for relative in paths:
        path = ROOT / relative
        assert path.is_file()
        assert not path.is_symlink()


def test_docker_and_launcher_require_exact_code_and_external_configuration():
    docker = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    start = (ROOT / "deployment" / "start.sh").read_text(encoding="utf-8")

    assert "FROM python:3.12.13-slim-bookworm@sha256:" in docker
    assert "python_swarm/*.py" not in docker
    assert "deployment/config.json" not in docker
    assert "pip install --upgrade" not in docker
    assert "--no-index" in docker
    assert "--only-binary=:all:" in docker
    for relative in CANONICAL_PYTHON:
        assert Path(relative).name in docker

    assert "deployment/config.json" not in start
    assert "QSX_CONFIG is required" in start
    assert "${QSX_CONFIG:-" not in start


def test_runtime_and_test_locks_are_platform_pinned_and_hash_locked():
    for relative in (
        "python_swarm/requirements-runtime-py312.lock",
        "requirements-test-py312.lock",
        "requirements-test-py314.lock",
    ):
        text = (ROOT / relative).read_text(encoding="utf-8")
        assert "generated by uv 0.11.16" in text
        assert "--hash=sha256:" in text
        assert " @ " not in text
        assert "git+" not in text
        logical = [
            line
            for line in text.splitlines()
            if line and not line.startswith(("#", " ", "\t", "-"))
        ]
        assert logical
        assert all("==" in line for line in logical)

    setup = (ROOT / "server_setup.sh").read_text(encoding="utf-8")
    assert "requirements-runtime-py312.lock" in setup
    assert "requirements-test-py312.lock" in setup
    assert "pip install --upgrade" not in setup
    assert "--only-binary=:all:" in setup


def test_secret_scanner_fails_on_large_binary_and_redacts_the_value(tmp_path):
    secret = "1234567890:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi"
    payload = tmp_path / "large.bin"
    payload.write_bytes(b"\0" * (6 * 1024 * 1024) + secret.encode() + b"\0")

    completed = _run(sys.executable, SCAN, tmp_path)

    assert completed.returncode == 1
    assert "telegram_bot_token" in completed.stdout
    assert secret not in completed.stdout
    assert secret not in completed.stderr


def test_secret_scanner_rejects_sensitive_filenames_even_when_empty(tmp_path):
    (tmp_path / "ipc-auth-token.txt").write_text("", encoding="utf-8")

    completed = _run(sys.executable, SCAN, tmp_path)

    assert completed.returncode == 1
    assert "sensitive_filename" in completed.stdout


@pytest.mark.skipif(shutil.which("ssh-keygen") is None, reason="OpenSSH is required")
def test_release_double_build_is_identical_signed_and_tamper_evident(tmp_path):
    source, ex5, evidence, signing_key, allowed_signers = _create_release_fixture(
        tmp_path
    )
    outputs: list[Path] = []
    for index in (1, 2):
        output_dir = tmp_path / f"out-{index}"
        completed = _run(
            sys.executable,
            BUILD,
            "--source-root",
            source,
            "--allowlist",
            ALLOWLIST,
            "--ex5",
            ex5,
            "--compile-evidence",
            evidence,
            "--signing-key",
            signing_key,
            "--signing-identity",
            "mahustlertrades-release",
            "--output-dir",
            output_dir,
        )
        assert completed.returncode == 0, completed.stderr
        outputs.append(Path(completed.stdout.strip()))

    assert outputs[0].read_bytes() == outputs[1].read_bytes()
    assert outputs[0].with_suffix(outputs[0].suffix + ".sha256").read_bytes() == (
        outputs[1].with_suffix(outputs[1].suffix + ".sha256").read_bytes()
    )

    valid = _run(
        sys.executable,
        VALIDATE,
        "--artifact",
        outputs[0],
        "--allowlist",
        ALLOWLIST,
        "--allowed-signers",
        allowed_signers,
        "--signing-identity",
        "mahustlertrades-release",
    )
    assert valid.returncode == 0, valid.stderr
    assert "VALID" in valid.stdout

    tampered = tmp_path / "tampered.tar.gz"
    body = bytearray(outputs[0].read_bytes())
    body[len(body) // 2] ^= 1
    tampered.write_bytes(body)
    tampered.with_suffix(tampered.suffix + ".sha256").write_text(
        outputs[0].with_suffix(outputs[0].suffix + ".sha256").read_text(
            encoding="utf-8"
        ),
        encoding="utf-8",
    )
    shutil.copy2(
        outputs[0].with_suffix(outputs[0].suffix + ".sha256.sig"),
        tampered.with_suffix(tampered.suffix + ".sha256.sig"),
    )

    rejected = _run(
        sys.executable,
        VALIDATE,
        "--artifact",
        tampered,
        "--allowlist",
        ALLOWLIST,
        "--allowed-signers",
        allowed_signers,
        "--signing-identity",
        "mahustlertrades-release",
    )
    assert rejected.returncode != 0
