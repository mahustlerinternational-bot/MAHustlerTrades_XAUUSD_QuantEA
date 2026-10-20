import base64
import gzip
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

from tools import build_mql5_native as native
from tools import run_test_evidence as test_evidence
from tools.dependency_common import LANES, parse_lock
from tools.release_common import (
    COMPILE_FIELDS,
    NATIVE_CONTRACT_PATH,
    ReleaseError,
    TEST_CONTRACT_PATH,
    TEST_EVIDENCE_LANES,
    checksum_manifest,
    clean_git_metadata,
    deterministic_archive_bytes,
    release_directories,
    secure_read_bytes,
    sha256_bytes,
    validate_compile_evidence,
    validate_native_compile_log,
)


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
FIXTURE_NODEIDS = (
    "tests/test_mql5_contract.py::test_release_real_capital_gate",
    "tests/test_runtime_config.py::test_release_config_gate",
)


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


def _valid_normalized_log(contract: dict[str, object], elapsed: int) -> bytes:
    includes = [
        str(value).replace("/", "\\")
        for value in contract["expected_includes"]  # type: ignore[union-attr]
    ]
    lines = [
        (
            r"C:\QSXBuild\QuantSwarmXAU.mq5 : information: compiling "
            r"C:\QSXBuild\QuantSwarmXAU.mq5"
        ),
        *[
            (
                r"C:\Program Files\MetaTrader 5\dummy.mqh"
                " : information: including C:\\Program Files\\MetaTrader 5\\"
                + include
            )
            for include in includes
        ],
        " : information: generating code",
        " : information: code generated",
        (
            f"Result: 0 errors, 0 warnings, {elapsed} ms elapsed, "
            "cpu='X64 Regular'"
        ),
        "",
    ]
    return "\n".join(lines).encode("utf-8")


def _native_evidence(
    *,
    contract_bytes: bytes,
    source_sha256: str,
    binary_sha256: str,
    compile_log_sha256: str,
    secondary_compile_log_sha256: str,
    source_commit: str,
) -> dict[str, object]:
    contract = json.loads(contract_bytes)
    compiler = contract["compiler"]
    return {
        "binary_reproducible": True,
        "binary_sha256": binary_sha256,
        "compile_log_sha256": compile_log_sha256,
        "compile_runs": 2,
        "compiler": compiler["path"],
        "compiler_dependency_manifest_sha256": sha256_bytes(
            native.canonical_json(contract["dependencies"])
        ),
        "compiler_sha256": compiler["sha256"],
        "compiler_version": compiler["version"],
        "contract_sha256": sha256_bytes(contract_bytes),
        "errors": 0,
        "experts_manifest_sha256": sha256_bytes(b"fixture-experts-manifest"),
        "gate_exit_code": 0,
        "host_tool_manifest_sha256": sha256_bytes(
            native.canonical_json(contract["host_tools"])
        ),
        "process_exit_code": contract["expected_process_exit_code"],
        "sandbox_profile_sha256": sha256_bytes(
            native.canonical_json(native.SANDBOX_PROFILE)
        ),
        "schema_version": 2,
        "secondary_compile_log_sha256": secondary_compile_log_sha256,
        "source_commit": source_commit,
        "source_sha256": source_sha256,
        "warnings": 0,
        "wine_version": contract["wine_version"],
    }


def _fixture_test_contract() -> tuple[bytes, bytes]:
    nodeids = "".join(f"{nodeid}\n" for nodeid in FIXTURE_NODEIDS).encode("utf-8")
    contract = {
        "allowed_skips": [],
        "expected_collected": len(FIXTURE_NODEIDS),
        "nodeids_sha256": sha256_bytes(nodeids),
        "pytest_arguments": [
            "-q",
            "-W",
            "error",
            "-p",
            "no:cacheprovider",
        ],
        "required_nodeids": sorted(FIXTURE_NODEIDS),
        "schema_version": 1,
    }
    return test_evidence.canonical_json(contract), nodeids


def _create_test_evidence_lane(
    root: Path,
    *,
    lane: str,
    source: Path,
    source_commit: str,
    contract_bytes: bytes,
    nodeids: bytes,
    signing_key: Path,
) -> Path:
    lane_contract = LANES[lane]
    output = root / lane
    output.mkdir(mode=0o700)
    log = f"{lane}: all tests passed\n[stderr]\n".encode("utf-8")
    result = {
        "collected": len(FIXTURE_NODEIDS),
        "deselected": 0,
        "errors": 0,
        "exit_status": 0,
        "failed": 0,
        "nodeids_sha256": sha256_bytes(nodeids),
        "passed": len(FIXTURE_NODEIDS),
        "schema_version": 1,
        "skipped": [],
        "warnings": 0,
        "xfailed": [],
        "xpassed": [],
    }
    lock_bytes = (source / lane_contract.lock_path).read_bytes()
    installed = [
        {"name": name, "version": record[0]}
        for name, record in sorted(parse_lock(lock_bytes).items())
    ]
    environment = {
        "LC_ALL": "C.UTF-8",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "PYTHONHASHSEED": "0",
        "PYTHONNOUSERSITE": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "TZ": "UTC",
    }
    cache_tag = f"cpython-{lane_contract.python_version_short.replace('.', '')}"
    interpreter = {
        "abi": lane_contract.abi,
        "base_executable_sha256": sha256_bytes(
            f"{lane}-base-executable".encode()
        ),
        "cache_tag": cache_tag,
        "implementation": lane_contract.implementation,
        "implementation_name": lane_contract.implementation_name,
        "machine": lane_contract.machine,
        "pip_version": lane_contract.pip_version,
        "platform": lane_contract.platform,
        "python_executable_sha256": sha256_bytes(
            f"{lane}-venv-executable".encode()
        ),
        "soabi": f"{cache_tag}-{lane_contract.machine}-linux-gnu",
        "version": lane_contract.python_version,
    }
    evidence = {
        "artifact_class": "canonical-test-evidence",
        "environment_sha256": sha256_bytes(
            test_evidence.canonical_json(environment)
        ),
        "gate_exit_code": 0,
        "installed_distributions": installed,
        "interpreter": interpreter,
        "lane": lane,
        "lock_sha256": sha256_bytes(lock_bytes),
        "pytest_arguments": [
            "-q",
            "-W",
            "error",
            "-p",
            "no:cacheprovider",
        ],
        "pytest_log_sha256": sha256_bytes(log),
        "pytest_nodeids_sha256": sha256_bytes(nodeids),
        "pytest_result": result,
        "sandbox_profile_sha256": sha256_bytes(
            test_evidence.canonical_json(test_evidence.SANDBOX_PROFILE)
        ),
        "schema_version": 1,
        "signing_identity": "mahustlertrades-release",
        "site_packages_sha256": sha256_bytes(
            f"{lane}-site-packages".encode()
        ),
        "source_commit": source_commit,
        "test_contract_sha256": sha256_bytes(contract_bytes),
        "wheelhouse_manifest_sha256": sha256_bytes(
            f"{lane}-wheelhouse".encode()
        ),
    }
    (output / "PYTEST.log").write_bytes(log)
    (output / "PYTEST_NODEIDS.txt").write_bytes(nodeids)
    evidence_path = output / "TEST_EVIDENCE.json"
    evidence_path.write_bytes(test_evidence.canonical_json(evidence))
    _sign(signing_key, evidence_path)
    for candidate in output.iterdir():
        candidate.chmod(0o400)
    output.chmod(0o500)
    return output


def _create_release_fixture(
    tmp_path: Path,
) -> tuple[Path, Path, Path, Path, Path, Path, Path, Path, Path]:
    source = tmp_path / "source"
    source.mkdir()
    for relative in _allowlisted_paths():
        origin = ROOT / relative
        target = source / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(origin, target)
    test_contract_bytes, test_nodeids = _fixture_test_contract()
    test_contract_path = source / TEST_CONTRACT_PATH
    test_contract_path.parent.mkdir(parents=True, exist_ok=True)
    test_contract_path.write_bytes(test_contract_bytes)
    canonical_allowlist = source / "release" / "runtime-allowlist.txt"
    canonical_allowlist.parent.mkdir(parents=True, exist_ok=True)
    fixture_allowlist = sorted(
        set(_allowlisted_paths()) | {TEST_CONTRACT_PATH}
    )
    canonical_allowlist.write_text(
        "".join(f"{relative}\n" for relative in fixture_allowlist),
        encoding="utf-8",
    )

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

    commit = _run("git", "rev-parse", "HEAD", cwd=source).stdout.strip()
    contract_bytes = (source / NATIVE_CONTRACT_PATH).read_bytes()
    contract = json.loads(contract_bytes)
    ex5 = tmp_path / "QuantSwarmXAU.ex5"
    ex5.write_bytes(
        b"EX5-NATIVE-COMPILE-TEST-FIXTURE\0"
        + b"\0" * int(contract["minimum_ex5_bytes"])
    )
    mq5 = source / "mql5_ea" / "QuantSwarmXAU.mq5"
    compile_log = tmp_path / "MetaEditor.log"
    compile_log.write_bytes(_valid_normalized_log(contract, 1234))
    secondary_compile_log = tmp_path / "MetaEditor-secondary.log"
    secondary_compile_log.write_bytes(_valid_normalized_log(contract, 1235))
    compile_evidence = tmp_path / "MQL5_COMPILE.json"
    compile_evidence.write_bytes(
        native.canonical_json(
            _native_evidence(
                contract_bytes=contract_bytes,
                source_sha256=_sha256(mq5),
                binary_sha256=_sha256(ex5),
                compile_log_sha256=_sha256(compile_log),
                secondary_compile_log_sha256=_sha256(secondary_compile_log),
                source_commit=commit,
            )
        )
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
    evidence_root = tmp_path / "test-evidence-inputs"
    evidence_root.mkdir()
    evidence_lanes = {
        lane: _create_test_evidence_lane(
            evidence_root,
            lane=lane,
            source=source,
            source_commit=commit,
            contract_bytes=test_contract_bytes,
            nodeids=test_nodeids,
            signing_key=signing_key,
        )
        for lane in TEST_EVIDENCE_LANES
    }
    return (
        source,
        ex5,
        compile_evidence,
        compile_log,
        secondary_compile_log,
        evidence_lanes["test-py312"],
        evidence_lanes["test-py314"],
        signing_key,
        allowed_signers,
    )


def _build_fixture_release(
    tmp_path: Path,
) -> tuple[Path, Path, Path, Path, Path]:
    (
        source,
        ex5,
        evidence,
        compile_log,
        secondary_compile_log,
        evidence_py312,
        evidence_py314,
        signing_key,
        allowed_signers,
    ) = _create_release_fixture(tmp_path)
    completed = _run(
        sys.executable,
        BUILD,
        "--source-root",
        source,
        "--allowlist",
        source / "release" / "runtime-allowlist.txt",
        "--ex5",
        ex5,
        "--compile-evidence",
        evidence,
        "--compile-log",
        compile_log,
        "--secondary-compile-log",
        secondary_compile_log,
        "--test-evidence-py312",
        evidence_py312,
        "--test-evidence-py314",
        evidence_py314,
        "--allowed-signers",
        allowed_signers,
        "--signing-key",
        signing_key,
        "--signing-identity",
        "mahustlertrades-release",
        "--output-dir",
        tmp_path / "release-output",
    )
    assert completed.returncode == 0, completed.stderr
    return (
        source,
        Path(completed.stdout.strip()),
        signing_key,
        allowed_signers,
        compile_log,
    )


def _sign(key: Path, target: Path) -> None:
    completed = _run(
        "ssh-keygen",
        "-Y",
        "sign",
        "-f",
        key,
        "-n",
        "file",
        target,
    )
    assert completed.returncode == 0, completed.stderr


def _write_outer_signature(artifact: Path, key: Path) -> None:
    checksum = artifact.with_suffix(artifact.suffix + ".sha256")
    checksum.write_text(
        f"{_sha256(artifact)}  {artifact.name}\n",
        encoding="utf-8",
    )
    _sign(key, checksum)


def _validate_command(
    artifact: Path,
    source: Path,
    allowed_signers: Path,
    *,
    installed_root: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    command: list[str | os.PathLike[str]] = [
        sys.executable,
        VALIDATE,
        "--artifact",
        artifact,
        "--allowlist",
        source / "release" / "runtime-allowlist.txt",
        "--allowed-signers",
        allowed_signers,
        "--signing-identity",
        "mahustlertrades-release",
    ]
    if installed_root is not None:
        command.extend(("--installed-root", installed_root))
    return _run(*command)


def _payload(artifact: Path) -> tuple[str, int, dict[str, bytes]]:
    files: dict[str, bytes] = {}
    with tarfile.open(artifact, mode="r:gz") as archive:
        members = archive.getmembers()
        root_name = members[0].name.rstrip("/").split("/", 1)[0]
        epoch = int(members[0].mtime)
        for member in members:
            if not member.isreg():
                continue
            relative = member.name.split("/", 1)[1]
            extracted = archive.extractfile(member)
            assert extracted is not None
            files[relative] = extracted.read()
    return root_name, epoch, files


def _resign_payload(files: dict[str, bytes], signing_key: Path, tmp_path: Path) -> None:
    checksummed = {
        relative: value
        for relative, value in files.items()
        if relative not in {"SHA256SUMS.txt", "SHA256SUMS.txt.sig"}
    }
    sums = checksum_manifest(checksummed.items())
    sums_path = tmp_path / "replacement-SHA256SUMS.txt"
    sums_path.write_bytes(sums)
    _sign(signing_key, sums_path)
    files["SHA256SUMS.txt"] = sums
    files["SHA256SUMS.txt.sig"] = sums_path.with_suffix(
        sums_path.suffix + ".sig"
    ).read_bytes()


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
    assert 'export QSX_CONFIG="${QSX_CONFIG:-' not in start


def test_runtime_and_test_locks_are_platform_pinned_and_hash_locked():
    for relative in (
        "python_swarm/requirements-runtime-py312.lock",
        "requirements-test-py312.lock",
        "requirements-test-py314.lock",
    ):
        text = (ROOT / relative).read_text(encoding="utf-8")
        assert "uv 0.11.16" in text
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


def test_secret_scanner_scans_and_redacts_secret_bearing_filenames(tmp_path):
    telegram = "1234567890:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi"
    github = "github_pat_11AA0abcdefghijklmnopqrstuvwxyz0123456789"
    (tmp_path / telegram).write_text("", encoding="utf-8")
    (tmp_path / github).write_text("", encoding="utf-8")

    completed = _run(sys.executable, SCAN, tmp_path)

    assert completed.returncode == 1
    assert "filename_telegram_bot_token" in completed.stdout
    assert "filename_github_token" in completed.stdout
    assert "[REDACTED]" in completed.stdout
    assert telegram not in completed.stdout + completed.stderr
    assert github not in completed.stdout + completed.stderr


def test_compile_evidence_rejects_boolean_zero_fields():
    digest = "0" * 64
    commit = "0" * 40
    contract_bytes = (ROOT / NATIVE_CONTRACT_PATH).read_bytes()
    evidence = _native_evidence(
        contract_bytes=contract_bytes,
        source_sha256=digest,
        binary_sha256=digest,
        compile_log_sha256=digest,
        secondary_compile_log_sha256=digest,
        source_commit=commit,
    )
    evidence["errors"] = False
    assert COMPILE_FIELDS == native.EVIDENCE_FIELDS

    with pytest.raises(ReleaseError, match="errors must be exactly 0"):
        validate_compile_evidence(
            json.dumps(evidence).encode(),
            contract_bytes=contract_bytes,
            source_sha256=digest,
            binary_sha256=digest,
            compile_log_sha256=digest,
            secondary_compile_log_sha256=digest,
            source_commit=commit,
            require_canonical=False,
        )


def test_native_compile_logs_and_verification_snapshots_are_fail_closed(tmp_path):
    contract = json.loads((ROOT / NATIVE_CONTRACT_PATH).read_bytes())
    expected_includes = contract["expected_includes"]
    valid = _valid_normalized_log(contract, 1234)
    validate_native_compile_log(
        valid,
        expected_includes=expected_includes,
        label="fixture log",
    )
    with pytest.raises(ReleaseError, match="compiler log contract"):
        validate_native_compile_log(
            b"Result: 0 errors, 0 warnings\n",
            expected_includes=expected_includes,
            label="forged log",
        )

    target = tmp_path / "verification-input"
    target.write_bytes(b"trusted snapshot")
    link = tmp_path / "verification-input-link"
    link.symlink_to(target)
    with pytest.raises(ReleaseError, match="non-symlink"):
        secure_read_bytes(link, "verification input")


def test_release_git_metadata_peels_head_and_scrubs_git_environment(
    tmp_path,
    monkeypatch,
):
    source = tmp_path / "source"
    source.mkdir()
    (source / "tracked").write_text("reviewed\n", encoding="utf-8")
    assert _run("git", "init", "-b", "main", cwd=source).returncode == 0
    assert _run("git", "add", "tracked", cwd=source).returncode == 0
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
    )
    assert committed.returncode == 0, committed.stderr
    tagged = _run(
        "git",
        "-c",
        "user.name=Release Contract",
        "-c",
        "user.email=release-contract@example.invalid",
        "tag",
        "-a",
        "release-candidate",
        "-m",
        "annotated",
        cwd=source,
    )
    assert tagged.returncode == 0, tagged.stderr
    expected_commit = _run(
        "git",
        "rev-parse",
        "HEAD^{commit}",
        cwd=source,
    ).stdout.strip()
    (source / ".git" / "HEAD").write_text(
        "ref: refs/tags/release-candidate\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("GIT_DIR", "/untrusted/git-dir")
    monkeypatch.setenv("GIT_WORK_TREE", "/untrusted/work-tree")
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.fsmonitor")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "true")

    observed_commit, epoch = clean_git_metadata(source)

    assert observed_commit == expected_commit
    assert epoch > 0


@pytest.mark.skipif(shutil.which("ssh-keygen") is None, reason="OpenSSH is required")
def test_release_double_build_is_identical_signed_and_tamper_evident(tmp_path):
    (
        source,
        ex5,
        evidence,
        compile_log,
        secondary_compile_log,
        evidence_py312,
        evidence_py314,
        signing_key,
        allowed_signers,
    ) = _create_release_fixture(tmp_path)
    outputs: list[Path] = []
    for index in (1, 2):
        output_dir = tmp_path / f"out-{index}"
        completed = _run(
            sys.executable,
            BUILD,
            "--source-root",
            source,
            "--allowlist",
            source / "release" / "runtime-allowlist.txt",
            "--ex5",
            ex5,
            "--compile-evidence",
            evidence,
            "--compile-log",
            compile_log,
            "--secondary-compile-log",
            secondary_compile_log,
            "--test-evidence-py312",
            evidence_py312,
            "--test-evidence-py314",
            evidence_py314,
            "--allowed-signers",
            allowed_signers,
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
    assert outputs[0].with_suffix(
        outputs[0].suffix + ".sha256.sig"
    ).read_bytes() == outputs[1].with_suffix(
        outputs[1].suffix + ".sha256.sig"
    ).read_bytes()

    valid = _run(
        sys.executable,
        VALIDATE,
        "--artifact",
        outputs[0],
        "--allowlist",
        source / "release" / "runtime-allowlist.txt",
        "--allowed-signers",
        allowed_signers,
        "--signing-identity",
        "mahustlertrades-release",
    )
    assert valid.returncode == 0, valid.stderr
    assert "VALID" in valid.stdout

    unrelated_installed_root = _validate_command(
        outputs[0],
        source,
        allowed_signers,
        installed_root=Path("/srv"),
    )
    assert unrelated_installed_root.returncode != 0

    unbound_installed_root = _run(
        sys.executable,
        VALIDATE,
        "--installed-root",
        "/srv",
    )
    assert unbound_installed_root.returncode != 0

    wrong_identity = _run(
        sys.executable,
        VALIDATE,
        "--artifact",
        outputs[0],
        "--allowlist",
        source / "release" / "runtime-allowlist.txt",
        "--allowed-signers",
        allowed_signers,
        "--signing-identity",
        "wrong-release-identity",
    )
    assert wrong_identity.returncode != 0

    provenance = source / "PROVENANCE.md"
    provenance.write_text(
        provenance.read_text(encoding="utf-8") + "\nuncommitted\n",
        encoding="utf-8",
    )
    dirty_build = _run(
        sys.executable,
        BUILD,
        "--source-root",
        source,
        "--allowlist",
        source / "release" / "runtime-allowlist.txt",
        "--ex5",
        ex5,
        "--compile-evidence",
        evidence,
        "--compile-log",
        compile_log,
        "--secondary-compile-log",
        secondary_compile_log,
        "--test-evidence-py312",
        evidence_py312,
        "--test-evidence-py314",
        evidence_py314,
        "--allowed-signers",
        allowed_signers,
        "--signing-key",
        signing_key,
        "--signing-identity",
        "mahustlertrades-release",
        "--output-dir",
        tmp_path / "dirty-output",
    )
    assert dirty_build.returncode != 0
    assert "dirty" in dirty_build.stderr

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
        source / "release" / "runtime-allowlist.txt",
        "--allowed-signers",
        allowed_signers,
        "--signing-identity",
        "mahustlertrades-release",
    )
    assert rejected.returncode != 0


@pytest.mark.skipif(shutil.which("ssh-keygen") is None, reason="OpenSSH is required")
def test_release_rejects_trailing_outer_and_inner_signature_bytes(tmp_path):
    source, artifact, signing_key, allowed_signers, _ = _build_fixture_release(
        tmp_path
    )
    outer_signature = artifact.with_suffix(artifact.suffix + ".sha256.sig")
    original_outer_signature = outer_signature.read_bytes()
    outer_signature.write_bytes(
        original_outer_signature + b"TRAILING-OUTER-SIGNATURE-DATA\n"
    )

    trailing_outer = _validate_command(artifact, source, allowed_signers)

    assert trailing_outer.returncode != 0
    assert "trailing" in trailing_outer.stderr.lower()

    encoded = b"".join(original_outer_signature.splitlines()[1:-1])
    decoded = base64.b64decode(encoded, validate=True) + b"TRAILING-BINARY"
    noncanonical_binary = base64.b64encode(decoded)
    wrapped = [
        noncanonical_binary[offset : offset + 70]
        for offset in range(0, len(noncanonical_binary), 70)
    ]
    outer_signature.write_bytes(
        b"\n".join(
            (
                b"-----BEGIN SSH SIGNATURE-----",
                *wrapped,
                b"-----END SSH SIGNATURE-----",
                b"",
            )
        )
    )

    trailing_binary = _validate_command(artifact, source, allowed_signers)

    assert trailing_binary.returncode != 0
    assert "trailing binary" in trailing_binary.stderr.lower()

    root_name, epoch, files = _payload(artifact)
    files["SHA256SUMS.txt.sig"] += b"TRAILING-INNER-SIGNATURE-DATA\n"
    inner_artifact = tmp_path / "trailing-inner-signature.tar.gz"
    inner_artifact.write_bytes(
        deterministic_archive_bytes(
            root_name=root_name,
            files=files,
            epoch=epoch,
        )
    )
    _write_outer_signature(inner_artifact, signing_key)

    trailing_inner = _validate_command(
        inner_artifact,
        source,
        allowed_signers,
    )

    assert trailing_inner.returncode != 0
    assert "trailing" in trailing_inner.stderr.lower()


@pytest.mark.skipif(shutil.which("ssh-keygen") is None, reason="OpenSSH is required")
def test_release_validator_redacts_secret_bearing_archive_names(tmp_path):
    source, artifact, signing_key, allowed_signers, _ = _build_fixture_release(
        tmp_path
    )
    root_name, epoch, files = _payload(artifact)
    secret = "1234567890:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi"
    files[f"unexpected/{secret}"] = b""
    secret_artifact = tmp_path / "secret-name.tar.gz"
    secret_artifact.write_bytes(
        deterministic_archive_bytes(
            root_name=root_name,
            files=files,
            epoch=epoch,
        )
    )
    _write_outer_signature(secret_artifact, signing_key)

    completed = _validate_command(secret_artifact, source, allowed_signers)
    output = completed.stdout + completed.stderr

    assert completed.returncode != 0
    assert "[REDACTED]" in output
    assert secret not in output


@pytest.mark.skipif(shutil.which("ssh-keygen") is None, reason="OpenSSH is required")
def test_release_rejects_noncanonical_gzip_and_tar_encodings(tmp_path):
    source, artifact, signing_key, allowed_signers, _ = _build_fixture_release(
        tmp_path
    )
    original = artifact.read_bytes()
    root_name, epoch, files = _payload(artifact)
    raw_tar = gzip.decompress(original)

    def gzip_bytes(value: bytes, *, filename: str = "") -> bytes:
        output = io.BytesIO()
        with gzip.GzipFile(
            filename=filename,
            mode="wb",
            fileobj=output,
            compresslevel=9,
            mtime=epoch,
        ) as compressed:
            compressed.write(value)
        return output.getvalue()

    pax_buffer = io.BytesIO()
    with tarfile.open(
        fileobj=pax_buffer,
        mode="w",
        format=tarfile.PAX_FORMAT,
    ) as archive:
        for index, (relative, value) in enumerate(
            sorted(files.items(), reverse=True)
        ):
            info = tarfile.TarInfo(f"{root_name}/{relative}")
            info.uid = 0
            info.gid = 0
            info.uname = "root"
            info.gname = "root"
            info.mtime = epoch
            info.mode = 0o555 if relative.endswith(".sh") else 0o444
            info.size = len(value)
            if index == 0:
                info.pax_headers = {"qsx-hidden": "noncanonical"}
            archive.addfile(info, io.BytesIO(value))

    reverse_ustar_buffer = io.BytesIO()
    with tarfile.open(
        fileobj=reverse_ustar_buffer,
        mode="w",
        format=tarfile.USTAR_FORMAT,
    ) as archive:
        for relative, value in sorted(files.items(), reverse=True):
            info = tarfile.TarInfo(f"{root_name}/{relative}")
            info.uid = 0
            info.gid = 0
            info.uname = "root"
            info.gname = "root"
            info.mtime = epoch
            info.mode = 0o555 if relative.endswith(".sh") else 0o444
            info.size = len(value)
            archive.addfile(info, io.BytesIO(value))

    gnu_buffer = io.BytesIO()
    with tarfile.open(
        fileobj=gnu_buffer,
        mode="w",
        format=tarfile.GNU_FORMAT,
    ) as archive:
        for directory in release_directories(root_name, files):
            info = tarfile.TarInfo(directory + "/")
            info.uid = 0
            info.gid = 0
            info.uname = "root"
            info.gname = "root"
            info.mtime = epoch
            info.mode = 0o555
            info.type = tarfile.DIRTYPE
            archive.addfile(info)
        for relative, value in sorted(files.items()):
            info = tarfile.TarInfo(f"{root_name}/{relative}")
            info.uid = 0
            info.gid = 0
            info.uname = "root"
            info.gname = "root"
            info.mtime = epoch
            info.mode = 0o555 if relative.endswith(".sh") else 0o444
            info.size = len(value)
            archive.addfile(info, io.BytesIO(value))

    variants = {
        "trailing": original + b"hidden-after-gzip",
        "concatenated": original + original,
        "gzip-filename": gzip_bytes(raw_tar, filename="hidden-metadata"),
        "pax": gzip_bytes(pax_buffer.getvalue()),
        "gnu": gzip_bytes(gnu_buffer.getvalue()),
        "reverse-without-directories": gzip_bytes(reverse_ustar_buffer.getvalue()),
    }
    for name, value in variants.items():
        candidate = tmp_path / f"{name}.tar.gz"
        candidate.write_bytes(value)
        _write_outer_signature(candidate, signing_key)

        completed = _validate_command(candidate, source, allowed_signers)

        assert completed.returncode != 0, name
        assert "VALID" not in completed.stdout


@pytest.mark.skipif(shutil.which("ssh-keygen") is None, reason="OpenSSH is required")
def test_release_binds_packaged_compile_log_and_source_manifest(tmp_path):
    source, artifact, signing_key, allowed_signers, _ = _build_fixture_release(
        tmp_path
    )
    root_name, epoch, original_files = _payload(artifact)

    log_files = dict(original_files)
    log_files["MQL5_COMPILE.log"] += b"tampered but re-signed\n"
    _resign_payload(log_files, signing_key, tmp_path)
    log_artifact = tmp_path / "wrong-log.tar.gz"
    log_artifact.write_bytes(
        deterministic_archive_bytes(
            root_name=root_name,
            files=log_files,
            epoch=epoch,
        )
    )
    _write_outer_signature(log_artifact, signing_key)

    wrong_log = _validate_command(log_artifact, source, allowed_signers)

    assert wrong_log.returncode != 0
    assert "compile_log_sha256" in wrong_log.stderr

    secondary_files = dict(original_files)
    secondary_files["MQL5_COMPILE.secondary.log"] += b"tampered but re-signed\n"
    secondary_signing = tmp_path / "secondary-signing"
    secondary_signing.mkdir()
    _resign_payload(secondary_files, signing_key, secondary_signing)
    secondary_artifact = tmp_path / "wrong-secondary-log.tar.gz"
    secondary_artifact.write_bytes(
        deterministic_archive_bytes(
            root_name=root_name,
            files=secondary_files,
            epoch=epoch,
        )
    )
    _write_outer_signature(secondary_artifact, signing_key)

    wrong_secondary = _validate_command(
        secondary_artifact,
        source,
        allowed_signers,
    )

    assert wrong_secondary.returncode != 0
    assert "compile_secondary_log_sha256" in wrong_secondary.stderr

    commit_files = dict(original_files)
    evidence = json.loads(commit_files["MQL5_COMPILE.json"])
    evidence["source_commit"] = "f" * 40
    commit_files["MQL5_COMPILE.json"] = native.canonical_json(evidence)
    commit_manifest = json.loads(commit_files["RELEASE.json"])
    commit_manifest["compile_evidence_sha256"] = sha256_bytes(
        commit_files["MQL5_COMPILE.json"]
    )
    commit_files["RELEASE.json"] = native.canonical_json(commit_manifest)
    commit_signing = tmp_path / "commit-signing"
    commit_signing.mkdir()
    _resign_payload(commit_files, signing_key, commit_signing)
    commit_artifact = tmp_path / "wrong-source-commit.tar.gz"
    commit_artifact.write_bytes(
        deterministic_archive_bytes(
            root_name=root_name,
            files=commit_files,
            epoch=epoch,
        )
    )
    _write_outer_signature(commit_artifact, signing_key)

    wrong_commit = _validate_command(commit_artifact, source, allowed_signers)

    assert wrong_commit.returncode != 0
    assert "source_commit" in wrong_commit.stderr

    manifest_files = dict(original_files)
    manifest = json.loads(manifest_files["RELEASE.json"])
    manifest["docker_base"] = (
        "python:3.12.13-slim-bookworm@sha256:" + "0" * 64
    )
    manifest_files["RELEASE.json"] = (
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    ).encode()
    replacement_dir = tmp_path / "manifest-signing"
    replacement_dir.mkdir()
    _resign_payload(manifest_files, signing_key, replacement_dir)
    manifest_artifact = tmp_path / "wrong-manifest.tar.gz"
    manifest_artifact.write_bytes(
        deterministic_archive_bytes(
            root_name=root_name,
            files=manifest_files,
            epoch=epoch,
        )
    )
    _write_outer_signature(manifest_artifact, signing_key)

    wrong_manifest = _validate_command(
        manifest_artifact,
        source,
        allowed_signers,
    )

    assert wrong_manifest.returncode != 0
    assert "docker_base" in wrong_manifest.stderr


@pytest.mark.skipif(shutil.which("ssh-keygen") is None, reason="OpenSSH is required")
def test_release_builder_rejects_git_replace_refs_and_nested_roots(tmp_path):
    (
        source,
        ex5,
        evidence,
        compile_log,
        secondary_compile_log,
        evidence_py312,
        evidence_py314,
        signing_key,
        allowed_signers,
    ) = _create_release_fixture(tmp_path)
    original = _run("git", "rev-parse", "HEAD", cwd=source).stdout.strip()
    provenance = source / "PROVENANCE.md"
    provenance.write_text(
        provenance.read_text(encoding="utf-8") + "\nreplacement tree\n",
        encoding="utf-8",
    )
    assert _run("git", "add", "PROVENANCE.md", cwd=source).returncode == 0
    tree = _run("git", "write-tree", cwd=source).stdout.strip()
    replacement = _run(
        "git",
        "-c",
        "user.name=Release Contract",
        "-c",
        "user.email=release-contract@example.invalid",
        "commit-tree",
        tree,
        cwd=source,
        input="replacement\n",
    )
    assert replacement.returncode == 0, replacement.stderr
    assert (
        _run(
            "git",
            "replace",
            original,
            replacement.stdout.strip(),
            cwd=source,
        ).returncode
        == 0
    )
    assert _run("git", "status", "--porcelain", cwd=source).stdout == ""

    replaced = _run(
        sys.executable,
        BUILD,
        "--source-root",
        source,
        "--allowlist",
        source / "release" / "runtime-allowlist.txt",
        "--ex5",
        ex5,
        "--compile-evidence",
        evidence,
        "--compile-log",
        compile_log,
        "--secondary-compile-log",
        secondary_compile_log,
        "--test-evidence-py312",
        evidence_py312,
        "--test-evidence-py314",
        evidence_py314,
        "--allowed-signers",
        allowed_signers,
        "--signing-key",
        signing_key,
        "--signing-identity",
        "mahustlertrades-release",
        "--output-dir",
        tmp_path / "replaced-output",
    )

    assert replaced.returncode != 0
    assert "replace refs" in replaced.stderr

    nested = _run(
        sys.executable,
        BUILD,
        "--source-root",
        source / "deployment",
        "--allowlist",
        source / "release" / "runtime-allowlist.txt",
        "--ex5",
        ex5,
        "--compile-evidence",
        evidence,
        "--compile-log",
        compile_log,
        "--secondary-compile-log",
        secondary_compile_log,
        "--test-evidence-py312",
        evidence_py312,
        "--test-evidence-py314",
        evidence_py314,
        "--allowed-signers",
        allowed_signers,
        "--signing-key",
        signing_key,
        "--signing-identity",
        "mahustlertrades-release",
        "--output-dir",
        tmp_path / "nested-output",
    )

    assert nested.returncode != 0
    assert "top-level" in nested.stderr
