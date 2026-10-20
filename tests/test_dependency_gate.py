import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
BUILD = ROOT / "tools" / "build_wheelhouse.py"
VALIDATE = ROOT / "tools" / "validate_wheelhouse.py"
COMMON_ROOT = ROOT / "tools"
sys.path.insert(0, str(COMMON_ROOT))

from dependency_common import canonical_json, wheel_record  # noqa: E402
import assert_installed_closure as installed_closure  # noqa: E402


@pytest.fixture(autouse=True)
def _restore_fixture_permissions(tmp_path):
    """Leave sealed fixture trees removable by pytest's warnings-strict cleanup."""

    yield
    for current, directories, files in os.walk(
        tmp_path,
        topdown=True,
        followlinks=False,
    ):
        current_path = Path(current)
        try:
            current_path.chmod(0o700)
        except FileNotFoundError:
            continue
        for name in directories:
            candidate = current_path / name
            if candidate.is_symlink():
                continue
            try:
                candidate.chmod(0o700)
            except FileNotFoundError:
                pass
        for name in files:
            candidate = current_path / name
            if candidate.is_symlink():
                continue
            try:
                candidate.chmod(0o600)
            except FileNotFoundError:
                pass


def _run(*command, **kwargs):
    return subprocess.run(
        [os.fspath(part) for part in command],
        check=False,
        capture_output=True,
        text=True,
        **kwargs,
    )


def _git(source: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return _run(
        "git",
        "-C",
        source,
        *arguments,
        env={
            **os.environ,
            "GIT_CONFIG_NOSYSTEM": "1",
            "LC_ALL": "C",
            "TZ": "UTC",
        },
    )


def _make_key(tmp_path: Path, name: str = "signing") -> tuple[Path, Path]:
    key = tmp_path / name
    generated = _run(
        "ssh-keygen",
        "-q",
        "-t",
        "ed25519",
        "-N",
        "",
        "-C",
        name,
        "-f",
        key,
    )
    assert generated.returncode == 0, generated.stderr
    key.chmod(0o600)
    fields = key.with_suffix(".pub").read_text(encoding="utf-8").split()
    allowed = tmp_path / f"{name}.allowed"
    allowed.write_text(
        f"dependency-test {fields[0]} {fields[1]}\n",
        encoding="utf-8",
    )
    allowed.chmod(0o400)
    return key, allowed


def _make_source(
    tmp_path: Path,
    *,
    wheel_name: str = "demo_pkg-1.0-py3-none-any.whl",
    lane: str = "runtime-py312",
) -> tuple[Path, Path]:
    source = tmp_path / "source"
    (source / "python_swarm").mkdir(parents=True)
    wheel = tmp_path / wheel_name
    wheel.write_bytes(b"signed-offline-wheel-fixture-v1")
    digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
    (source / "python_swarm" / "requirements.txt").write_text(
        "demo-pkg==1.0\n",
        encoding="utf-8",
    )
    if lane == "runtime-py312":
        lock_path = source / "python_swarm" / "requirements-runtime-py312.lock"
    else:
        lock_path = source / (
            "requirements-test-py312.lock"
            if lane == "test-py312"
            else "requirements-test-py314.lock"
        )
        (source / "requirements-test.in").write_text(
            "-r python_swarm/requirements.txt\n",
            encoding="utf-8",
        )
    lock_path.write_text(
        "demo-pkg==1.0 \\\n"
        f"    --hash=sha256:{digest}\n",
        encoding="utf-8",
    )
    initialized = _run("git", "init", "-q", "-b", "main", source)
    assert initialized.returncode == 0, initialized.stderr
    assert _git(source, "add", "--all").returncode == 0
    committed = _git(
        source,
        "-c",
        "user.name=Dependency Contract",
        "-c",
        "user.email=dependency-contract@example.invalid",
        "commit",
        "-q",
        "-m",
        "fixture",
    )
    assert committed.returncode == 0, committed.stderr
    return source, wheel


def _make_fake_python(tmp_path: Path, *, version: str = "3.12.13") -> Path:
    executable = tmp_path / f"fake-python-{version.replace('.', '')}"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, shutil, sys\n"
        "args = sys.argv[1:]\n"
        "if args and args[0] == '-c':\n"
        "    print(json.dumps({\n"
        "        'implementation': 'CPython',\n"
        "        'implementation_name': 'cpython',\n"
        "        'machine': 'x86_64',\n"
        f"        'version': {version!r},\n"
        "    }, sort_keys=True))\n"
        "elif args == ['-m', 'pip', '--version']:\n"
        f"    print('pip 25.1.1 from /fixture/pip (python {version[:4]})')\n"
        "elif args[:3] == ['-m', 'pip', 'download']:\n"
        "    destination = args[args.index('--dest') + 1]\n"
        "    shutil.copy2(os.environ['QSX_TEST_WHEEL'], destination)\n"
        "else:\n"
        "    raise SystemExit(91)\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    return executable


@pytest.fixture
def signed_wheelhouse(tmp_path):
    if shutil.which("ssh-keygen") is None:
        pytest.skip("OpenSSH is required")
    source, wheel = _make_source(tmp_path)
    executable = _make_fake_python(tmp_path)
    key, allowed = _make_key(tmp_path)
    output = tmp_path / "output"
    completed = _run(
        sys.executable,
        BUILD,
        "--source-root",
        source,
        "--lane",
        "runtime-py312",
        "--python",
        executable,
        "--signing-key",
        key,
        "--signing-identity",
        "dependency-test",
        "--output-root",
        output,
        env={**os.environ, "QSX_TEST_WHEEL": str(wheel)},
    )
    assert completed.returncode == 0, completed.stderr
    wheelhouse = Path(completed.stdout.strip())
    return source, wheelhouse, key, allowed


def _validate(
    source: Path,
    wheelhouse: Path,
    allowed: Path,
    *,
    identity: str = "dependency-test",
    lane: str = "runtime-py312",
    interpreter: Path | None = None,
):
    command = [
        sys.executable,
        VALIDATE,
        "--source-root",
        source,
        "--wheelhouse",
        wheelhouse,
        "--lane",
        lane,
        "--allowed-signers",
        allowed,
        "--signing-identity",
        identity,
        "--require-source-inputs",
    ]
    if interpreter is not None:
        command.extend(("--interpreter", interpreter))
    return _run(*command)


def _make_tree_writable(wheelhouse: Path) -> None:
    wheelhouse.chmod(0o700)
    (wheelhouse / "wheels").chmod(0o700)
    (wheelhouse / "WHEELHOUSE.json").chmod(0o600)


def _seal_tree(wheelhouse: Path) -> None:
    for candidate in (wheelhouse / "wheels").iterdir():
        candidate.chmod(0o400)
    (wheelhouse / "wheels").chmod(0o500)
    (wheelhouse / "WHEELHOUSE.json").chmod(0o400)
    (wheelhouse / "WHEELHOUSE.json.sig").chmod(0o400)
    wheelhouse.chmod(0o500)


def _resign(wheelhouse: Path, key: Path) -> None:
    signature = wheelhouse / "WHEELHOUSE.json.sig"
    if signature.exists() or signature.is_symlink():
        signature.unlink()
    completed = _run(
        "ssh-keygen",
        "-Y",
        "sign",
        "-f",
        key,
        "-n",
        "file",
        wheelhouse / "WHEELHOUSE.json",
        env={**os.environ, "LC_ALL": "C", "TZ": "UTC"},
    )
    assert completed.returncode == 0, completed.stderr


def test_signed_wheelhouse_validates_from_exact_canonical_inputs(signed_wheelhouse):
    source, wheelhouse, _key, allowed = signed_wheelhouse
    completed = _validate(
        source,
        wheelhouse,
        allowed,
        interpreter=source.parent / "fake-python-31213",
    )
    assert completed.returncode == 0, completed.stderr
    assert "WHEELHOUSE_VALID lane=runtime-py312" in completed.stdout

    manifest = json.loads(
        (wheelhouse / "WHEELHOUSE.json").read_text(encoding="utf-8")
    )
    assert manifest["source_commit"] == _git(source, "rev-parse", "HEAD").stdout.strip()
    assert manifest["lock"]["path"] == (
        "python_swarm/requirements-runtime-py312.lock"
    )
    assert {
        key: manifest["interpreter"][key]
        for key in (
            "abi",
            "implementation",
            "implementation_name",
            "machine",
            "pip_version",
            "platform",
            "version",
        )
    } == {
        "abi": "cp312",
        "implementation": "CPython",
        "implementation_name": "cpython",
        "machine": "x86_64",
        "pip_version": "25.1.1",
        "platform": "manylinux_2_28_x86_64",
        "version": "3.12.13",
    }


@pytest.mark.parametrize(
    ("lane", "version"),
    [
        ("runtime-py312", "3.12.13"),
        ("test-py312", "3.12.13"),
        ("test-py314", "3.14.4"),
    ],
)
def test_each_lane_builds_and_validates_as_a_separate_signed_closure(
    tmp_path,
    lane,
    version,
):
    if shutil.which("ssh-keygen") is None:
        pytest.skip("OpenSSH is required")
    source, wheel = _make_source(tmp_path, lane=lane)
    executable = _make_fake_python(tmp_path, version=version)
    key, allowed = _make_key(tmp_path)
    completed = _run(
        sys.executable,
        BUILD,
        "--source-root",
        source,
        "--lane",
        lane,
        "--python",
        executable,
        "--signing-key",
        key,
        "--signing-identity",
        "dependency-test",
        "--output-root",
        tmp_path / "output",
        env={**os.environ, "QSX_TEST_WHEEL": str(wheel)},
    )
    assert completed.returncode == 0, completed.stderr
    wheelhouse = Path(completed.stdout.strip())

    validated = _validate(
        source,
        wheelhouse,
        allowed,
        lane=lane,
    )
    assert validated.returncode == 0, validated.stderr
    assert f"lane={lane}" in validated.stdout


@pytest.mark.parametrize("mutation", ["missing", "extra", "sdist", "tampered"])
def test_validator_rejects_incomplete_extra_sdist_and_tampered_artifacts(
    signed_wheelhouse,
    mutation,
):
    source, wheelhouse, _key, allowed = signed_wheelhouse
    _make_tree_writable(wheelhouse)
    wheel = next((wheelhouse / "wheels").iterdir())
    wheel.chmod(0o600)
    if mutation == "missing":
        wheel.unlink()
    elif mutation == "extra":
        extra = wheelhouse / "wheels" / "extra_pkg-1.0-py3-none-any.whl"
        extra.write_bytes(b"extra")
    elif mutation == "sdist":
        extra = wheelhouse / "wheels" / "extra-pkg-1.0.tar.gz"
        extra.write_bytes(b"sdist")
    else:
        wheel.write_bytes(wheel.read_bytes() + b"-tampered")
    _seal_tree(wheelhouse)

    completed = _validate(source, wheelhouse, allowed)
    assert completed.returncode != 0
    assert "WHEELHOUSE_INVALID" in completed.stderr


def test_validator_rejects_symlinked_manifest_without_following_it(signed_wheelhouse):
    source, wheelhouse, _key, allowed = signed_wheelhouse
    _make_tree_writable(wheelhouse)
    manifest = wheelhouse / "WHEELHOUSE.json"
    backup = wheelhouse / "manifest-target"
    backup.write_bytes(manifest.read_bytes())
    manifest.unlink()
    manifest.symlink_to(backup)
    wheelhouse.chmod(0o500)

    completed = _validate(source, wheelhouse, allowed)
    assert completed.returncode != 0
    assert "WHEELHOUSE_INVALID" in completed.stderr


def test_validator_rejects_signed_manifest_trailing_bytes(signed_wheelhouse):
    source, wheelhouse, key, allowed = signed_wheelhouse
    _make_tree_writable(wheelhouse)
    manifest = wheelhouse / "WHEELHOUSE.json"
    manifest.write_bytes(manifest.read_bytes() + b" ")
    _resign(wheelhouse, key)
    _seal_tree(wheelhouse)

    completed = _validate(source, wheelhouse, allowed)
    assert completed.returncode != 0
    assert "manifest is not canonical JSON" in completed.stderr


def test_validator_rejects_signature_bytes_openssh_would_ignore(
    signed_wheelhouse,
):
    source, wheelhouse, _key, allowed = signed_wheelhouse
    _make_tree_writable(wheelhouse)
    signature = wheelhouse / "WHEELHOUSE.json.sig"
    signature.chmod(0o600)
    signature.write_bytes(
        signature.read_bytes() + b"TRAILING_UNPARSED_BYTES\n"
    )
    _seal_tree(wheelhouse)

    completed = _validate(source, wheelhouse, allowed)
    assert completed.returncode != 0
    assert "canonical OpenSSH armor" in completed.stderr


def test_validator_rejects_signed_wheel_path_escape(signed_wheelhouse):
    source, wheelhouse, key, allowed = signed_wheelhouse
    _make_tree_writable(wheelhouse)
    manifest_path = wheelhouse / "WHEELHOUSE.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"][0]["filename"] = "../escaped.whl"
    manifest_path.write_bytes(canonical_json(manifest))
    _resign(wheelhouse, key)
    _seal_tree(wheelhouse)

    completed = _validate(source, wheelhouse, allowed)
    assert completed.returncode != 0
    assert "wheel filename is invalid" in completed.stderr


@pytest.mark.parametrize(
    "replacement",
    [
        "demo_pkg-1.0-cp311-cp311-manylinux_2_28_x86_64.whl",
        "demo_pkg-1.0-cp312-cp312-win_amd64.whl",
    ],
)
def test_validator_rejects_correctly_signed_wrong_abi_and_platform(
    signed_wheelhouse,
    replacement,
):
    source, wheelhouse, key, allowed = signed_wheelhouse
    _make_tree_writable(wheelhouse)
    wheels = wheelhouse / "wheels"
    original = next(wheels.iterdir())
    value = original.read_bytes()
    original.chmod(0o600)
    original.unlink()
    replacement_path = wheels / replacement
    replacement_path.write_bytes(value)
    manifest_path = wheelhouse / "WHEELHOUSE.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"] = [wheel_record(replacement, value)]
    manifest_path.write_bytes(canonical_json(manifest))
    _resign(wheelhouse, key)
    _seal_tree(wheelhouse)

    completed = _validate(source, wheelhouse, allowed)
    assert completed.returncode != 0
    assert "incompatible" in completed.stderr


def test_validator_rejects_wrong_signer_and_wrong_identity(signed_wheelhouse, tmp_path):
    source, wheelhouse, _key, allowed = signed_wheelhouse
    _other_key, other_allowed = _make_key(tmp_path, "other-signing")

    wrong_key = _validate(source, wheelhouse, other_allowed)
    assert wrong_key.returncode != 0
    assert "signature verification failed" in wrong_key.stderr

    wrong_identity = _validate(
        source,
        wheelhouse,
        allowed,
        identity="another-identity",
    )
    assert wrong_identity.returncode != 0


def test_builder_rejects_wrong_interpreter_before_pip_download(tmp_path):
    source, wheel = _make_source(tmp_path)
    executable = _make_fake_python(tmp_path)
    body = executable.read_text(encoding="utf-8").replace(
        "'version': '3.12.13'",
        "'version': '3.12.12'",
    )
    executable.write_text(body, encoding="utf-8")
    executable.chmod(0o700)
    key, _allowed = _make_key(tmp_path)

    completed = _run(
        sys.executable,
        BUILD,
        "--source-root",
        source,
        "--lane",
        "runtime-py312",
        "--python",
        executable,
        "--signing-key",
        key,
        "--signing-identity",
        "dependency-test",
        "--output-root",
        tmp_path / "output",
        env={**os.environ, "QSX_TEST_WHEEL": str(wheel)},
    )
    assert completed.returncode != 0
    assert "wrong interpreter" in completed.stderr


def test_validator_rejects_changed_consumer_pip_even_when_resigned(
    signed_wheelhouse,
):
    source, wheelhouse, key, allowed = signed_wheelhouse
    executable = source.parent / "fake-python-31213"
    executable.write_text(
        executable.read_text(encoding="utf-8").replace(
            "pip 25.1.1 from",
            "pip 25.1.0 from",
        ),
        encoding="utf-8",
    )
    executable.chmod(0o700)
    _make_tree_writable(wheelhouse)
    manifest_path = wheelhouse / "WHEELHOUSE.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    executable_value = executable.read_bytes()
    manifest["interpreter"]["executable_sha256"] = hashlib.sha256(
        executable_value
    ).hexdigest()
    manifest["interpreter"]["executable_size"] = len(executable_value)
    manifest_path.write_bytes(canonical_json(manifest))
    _resign(wheelhouse, key)
    _seal_tree(wheelhouse)

    completed = _validate(
        source,
        wheelhouse,
        allowed,
        interpreter=executable,
    )
    assert completed.returncode != 0
    assert "executable identity does not match" in completed.stderr


def test_builder_rejects_dirty_and_untracked_source(tmp_path):
    source, wheel = _make_source(tmp_path)
    executable = _make_fake_python(tmp_path)
    key, _allowed = _make_key(tmp_path)
    (source / "untracked.txt").write_text("not canonical\n", encoding="utf-8")

    completed = _run(
        sys.executable,
        BUILD,
        "--source-root",
        source,
        "--lane",
        "runtime-py312",
        "--python",
        executable,
        "--signing-key",
        key,
        "--signing-identity",
        "dependency-test",
        "--output-root",
        tmp_path / "output",
        env={**os.environ, "QSX_TEST_WHEEL": str(wheel)},
    )
    assert completed.returncode != 0
    assert "dirty or contains untracked files" in completed.stderr


def test_builder_rejects_nested_git_source_root(tmp_path):
    source, wheel = _make_source(tmp_path)
    executable = _make_fake_python(tmp_path)
    key, _allowed = _make_key(tmp_path)

    completed = _run(
        sys.executable,
        BUILD,
        "--source-root",
        source / "python_swarm",
        "--lane",
        "runtime-py312",
        "--python",
        executable,
        "--signing-key",
        key,
        "--signing-identity",
        "dependency-test",
        "--output-root",
        tmp_path / "output",
        env={**os.environ, "QSX_TEST_WHEEL": str(wheel)},
    )
    assert completed.returncode != 0
    assert "exact Git worktree top level" in completed.stderr


def test_builder_scrubs_inherited_git_repository_redirection(tmp_path):
    source, wheel = _make_source(tmp_path)
    alternate = tmp_path / "alternate"
    alternate.mkdir()
    initialized = _run("git", "init", "-q", "-b", "main", alternate)
    assert initialized.returncode == 0, initialized.stderr
    executable = _make_fake_python(tmp_path)
    key, _allowed = _make_key(tmp_path)

    completed = _run(
        sys.executable,
        BUILD,
        "--source-root",
        source,
        "--lane",
        "runtime-py312",
        "--python",
        executable,
        "--signing-key",
        key,
        "--signing-identity",
        "dependency-test",
        "--output-root",
        tmp_path / "output",
        env={
            **os.environ,
            "GIT_DIR": str(alternate / ".git"),
            "GIT_WORK_TREE": str(alternate),
            "QSX_TEST_WHEEL": str(wheel),
        },
    )
    assert completed.returncode == 0, completed.stderr


def test_builder_rejects_path_injected_git_executable(tmp_path):
    source, wheel = _make_source(tmp_path)
    executable = _make_fake_python(tmp_path)
    key, _allowed = _make_key(tmp_path)
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    fake_git = fake_bin / "git"
    fake_git.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_git.chmod(0o700)

    completed = _run(
        sys.executable,
        BUILD,
        "--source-root",
        source,
        "--lane",
        "runtime-py312",
        "--python",
        executable,
        "--signing-key",
        key,
        "--signing-identity",
        "dependency-test",
        "--output-root",
        tmp_path / "output",
        env={
            **os.environ,
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "QSX_TEST_WHEEL": str(wheel),
        },
    )
    assert completed.returncode != 0
    assert "system executable must be root-owned and protected: git" in completed.stderr


def test_builder_ignores_git_replace_objects(tmp_path):
    source, wheel = _make_source(tmp_path)
    original = _git(source, "rev-parse", "HEAD").stdout.strip()
    lock = source / "python_swarm" / "requirements-runtime-py312.lock"
    lock.write_text(
        "demo-pkg==1.0 \\\n"
        f"    --hash=sha256:{'0' * 64}\n",
        encoding="utf-8",
    )
    assert _git(source, "add", "--all").returncode == 0
    replacement_commit = _git(
        source,
        "-c",
        "user.name=Dependency Contract",
        "-c",
        "user.email=dependency-contract@example.invalid",
        "commit",
        "-q",
        "-m",
        "replacement",
    )
    assert replacement_commit.returncode == 0, replacement_commit.stderr
    replacement = _git(source, "rev-parse", "HEAD").stdout.strip()
    reset = _git(source, "reset", "--hard", original)
    assert reset.returncode == 0, reset.stderr
    replaced = _git(source, "replace", original, replacement)
    assert replaced.returncode == 0, replaced.stderr

    executable = _make_fake_python(tmp_path)
    key, _allowed = _make_key(tmp_path)
    completed = _run(
        sys.executable,
        BUILD,
        "--source-root",
        source,
        "--lane",
        "runtime-py312",
        "--python",
        executable,
        "--signing-key",
        key,
        "--signing-identity",
        "dependency-test",
        "--output-root",
        tmp_path / "output",
        env={**os.environ, "QSX_TEST_WHEEL": str(wheel)},
    )
    assert completed.returncode == 0, completed.stderr
    manifest = json.loads(
        (Path(completed.stdout.strip()) / "WHEELHOUSE.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["source_commit"] == original


def test_installed_closure_requires_exact_bootstrap_version(
    tmp_path,
    monkeypatch,
    capsys,
):
    class Distribution:
        def __init__(self, name: str, version: str):
            self.metadata = {"Name": name}
            self.version = version

    lock = tmp_path / "closure.lock"
    lock.write_text("demo-pkg==1.0\n", encoding="utf-8")
    runtime_arguments = [
        "--lock",
        str(lock),
        "--implementation",
        sys.implementation.name,
        "--python-version",
        ".".join(str(part) for part in sys.version_info[:3]),
        "--machine",
        os.uname().machine,
        "--allow-bootstrap",
        "pip==25.1.1",
    ]
    monkeypatch.setattr(
        installed_closure.importlib.metadata,
        "distributions",
        lambda: [
            Distribution("demo-pkg", "1.0"),
            Distribution("pip", "25.1.1"),
        ],
    )
    assert installed_closure.main(runtime_arguments) == 0
    capsys.readouterr()

    monkeypatch.setattr(
        installed_closure.importlib.metadata,
        "distributions",
        lambda: [
            Distribution("demo-pkg", "1.0"),
            Distribution("pip", "25.1.0"),
        ],
    )
    assert installed_closure.main(runtime_arguments) == 1
    assert "DEPENDENCY_CLOSURE_INVALID" in capsys.readouterr().err


def test_dependency_consumers_use_separate_lanes_and_exact_runtime_guard():
    setup = (ROOT / "server_setup.sh").read_text(encoding="utf-8")
    docker = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    ignore = (ROOT / ".dockerignore").read_text(encoding="utf-8")

    assert "runtime-py312" in setup
    assert "test-py312" in setup
    assert "test-py314" not in setup
    assert "validate_wheelhouse.py" in setup
    assert "assert_installed_closure.py" in setup
    assert "sys.implementation.name" in setup
    assert "(3, 12, 13)" in setup
    assert "25.1.1" in setup
    assert "x86_64" in setup
    assert ".venv.stage." in setup

    assert "wheelhouse/runtime-py312/wheels/" in docker
    assert "wheelhouse/test-py312" not in docker
    assert "wheelhouse/test-py314" not in docker
    assert "assert_installed_closure.py" in docker
    assert "pip==25.1.1" in docker
    assert "!wheelhouse/runtime-py312/" in ignore
    assert "!wheelhouse/test-py312" not in ignore
    assert "!wheelhouse/test-py314" not in ignore
