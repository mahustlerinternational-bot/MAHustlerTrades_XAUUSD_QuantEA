#!/usr/bin/env python3
"""Build one exact, offline, signed QUANT-SWARM-XAU wheelhouse lane."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

from dependency_common import (
    DependencyGateError,
    MANIFEST_NAME,
    SAFE_IDENTITY,
    SIGNATURE_NAME,
    WHEELS_DIRECTORY,
    assert_canonical_ssh_signature,
    assert_not_writable,
    canonical_json,
    interpreter_pip_version,
    isolated_python_environment,
    lane_contract,
    open_directory_path,
    parse_lock,
    resolve_executable_path,
    sha256_bytes,
    snapshot_path,
    source_commit,
    source_file_records,
    trusted_system_executable,
    validate_records_against_lock,
    wheel_record,
)


INTERPRETER_PROBE = r"""
import json, platform, sys
print(json.dumps({
    "implementation": platform.python_implementation(),
    "implementation_name": sys.implementation.name,
    "machine": platform.machine(),
    "version": platform.python_version(),
}, sort_keys=True))
"""


def _interpreter_details(executable: Path) -> dict[str, str]:
    completed = subprocess.run(
        [os.fspath(executable), "-c", INTERPRETER_PROBE],
        check=False,
        capture_output=True,
        text=True,
        env=isolated_python_environment(),
    )
    if completed.returncode != 0:
        raise DependencyGateError("cannot inspect wheelhouse interpreter")
    try:
        parsed = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise DependencyGateError("interpreter probe did not return JSON") from exc
    if not isinstance(parsed, dict) or not all(
        isinstance(parsed.get(field), str)
        for field in ("implementation", "implementation_name", "machine", "version")
    ):
        raise DependencyGateError("interpreter probe returned invalid fields")
    return parsed


def _validate_key(path: Path) -> Path:
    value, metadata = snapshot_path(path, label="wheelhouse signing key", maximum_size=64 * 1024)
    del value
    if metadata.st_uid != os.getuid():
        raise DependencyGateError("wheelhouse signing key must be owned by this user")
    if stat.S_IMODE(metadata.st_mode) not in {0o400, 0o600}:
        raise DependencyGateError("wheelhouse signing key mode must be 0400 or 0600")
    trusted_system_executable("ssh-keygen")
    return path.expanduser().absolute()


def _download(
    *,
    executable: Path,
    lock_path: Path,
    destination: Path,
    lane_name: str,
) -> None:
    lane = lane_contract(lane_name)
    completed = subprocess.run(
        [
            os.fspath(executable),
            "-m",
            "pip",
            "download",
            "--disable-pip-version-check",
            "--no-cache-dir",
            "--no-deps",
            "--only-binary=:all:",
            "--require-hashes",
            "--implementation",
            "cp",
            "--python-version",
            lane.python_version_short.replace(".", ""),
            "--abi",
            lane.abi,
            "--platform",
            lane.platform,
            "--dest",
            os.fspath(destination),
            "-r",
            os.fspath(lock_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        env={
            **{
                key: value
                for key, value in isolated_python_environment().items()
                if not key.startswith("PIP_")
            },
            "PIP_CONFIG_FILE": os.devnull,
            "PIP_DISABLE_PIP_VERSION_CHECK": "1",
            "PIP_NO_CACHE_DIR": "1",
        },
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise DependencyGateError(f"pip download failed: {detail}")


def _sign(manifest: Path, key: Path) -> Path:
    signature = manifest.with_name(SIGNATURE_NAME)
    ssh_keygen = trusted_system_executable("ssh-keygen")
    completed = subprocess.run(
        [
            os.fspath(ssh_keygen),
            "-Y",
            "sign",
            "-f",
            os.fspath(key),
            "-n",
            "file",
            os.fspath(manifest),
        ],
        check=False,
        capture_output=True,
        text=True,
        env={**isolated_python_environment(), "LC_ALL": "C"},
    )
    if completed.returncode != 0 or not signature.is_file() or signature.is_symlink():
        raise DependencyGateError("OpenSSH wheelhouse signing failed")
    return signature


def build(arguments: argparse.Namespace) -> Path:
    lane = lane_contract(arguments.lane)
    source_root = arguments.source_root.expanduser().absolute()
    if source_root.is_symlink() or not source_root.is_dir():
        raise DependencyGateError("source root must be a non-symlink directory")
    source_root = source_root.resolve(strict=True)
    root_fd = open_directory_path(source_root)
    try:
        commit = source_commit(source_root)
        source_records, source_values = source_file_records(
            source_root,
            root_fd,
            commit,
            (lane.lock_path, *lane.input_paths),
        )
    finally:
        os.close(root_fd)
    lock_record, *input_records = source_records
    requirements = parse_lock(source_values[lane.lock_path])

    executable, executable_value, executable_metadata = resolve_executable_path(
        arguments.python
    )
    details = _interpreter_details(executable)
    expected = {
        "implementation": lane.implementation,
        "implementation_name": lane.implementation_name,
        "machine": lane.machine,
        "version": lane.python_version,
    }
    if details != expected:
        raise DependencyGateError(
            f"wrong interpreter for {lane.name}: expected={expected} actual={details}"
        )
    pip_version = interpreter_pip_version(executable)
    if pip_version != lane.pip_version:
        raise DependencyGateError(
            f"wrong pip for {lane.name}: expected={lane.pip_version} "
            f"actual={pip_version}"
        )
    key = _validate_key(arguments.signing_key)
    if SAFE_IDENTITY.fullmatch(arguments.signing_identity) is None:
        raise DependencyGateError("wheelhouse signing identity is invalid")

    output_root = arguments.output_root.expanduser().absolute()
    if output_root.is_symlink():
        raise DependencyGateError("wheelhouse output root must not be a symlink")
    output_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    if not output_root.is_dir():
        raise DependencyGateError("wheelhouse output root is not a directory")
    final = output_root / lane.name
    if final.exists() or final.is_symlink():
        raise DependencyGateError(f"refusing to replace wheelhouse lane: {final}")

    stage = Path(tempfile.mkdtemp(prefix=f".{lane.name}.", dir=output_root))
    try:
        wheels = stage / WHEELS_DIRECTORY
        wheels.mkdir(mode=0o700)
        lock_path = source_root / lane.lock_path
        _download(
            executable=executable,
            lock_path=lock_path,
            destination=wheels,
            lane_name=lane.name,
        )
        records: list[dict[str, object]] = []
        for candidate in sorted(wheels.iterdir(), key=lambda path: path.name):
            if candidate.is_symlink() or not candidate.is_file():
                raise DependencyGateError(
                    f"pip download emitted a non-regular artifact: {candidate.name}"
                )
            candidate.chmod(0o400)
            value, metadata = snapshot_path(
                candidate,
                label=f"downloaded wheel {candidate.name}",
                maximum_size=1024 * 1024 * 1024,
            )
            assert_not_writable(metadata, candidate.name)
            records.append(wheel_record(candidate.name, value))
        validate_records_against_lock(records, requirements, lane)
        manifest = {
            "artifact_class": "offline-python-wheelhouse",
            "files": records,
            "inputs": input_records,
            "interpreter": {
                **details,
                "abi": lane.abi,
                "executable_path": os.fspath(executable),
                "executable_sha256": sha256_bytes(executable_value),
                "executable_size": executable_metadata.st_size,
                "pip_version": pip_version,
                "platform": lane.platform,
            },
            "lane": lane.name,
            "lock": lock_record,
            "schema_version": 1,
            "signing_identity": arguments.signing_identity,
            "source_commit": commit,
        }
        manifest_path = stage / MANIFEST_NAME
        manifest_path.write_bytes(canonical_json(manifest))
        manifest_path.chmod(0o400)
        signature = _sign(manifest_path, key)
        signature_value, _ = snapshot_path(
            signature,
            label="generated wheelhouse signature",
            maximum_size=1024 * 1024,
        )
        assert_canonical_ssh_signature(
            signature_value, "generated wheelhouse signature"
        )
        signature.chmod(0o400)
        wheels.chmod(0o500)
        stage.rename(final)
        final.chmod(0o500)
        return final
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument(
        "--lane",
        required=True,
        choices=("runtime-py312", "test-py312", "test-py314"),
    )
    parser.add_argument("--python", required=True, type=Path)
    parser.add_argument("--signing-key", required=True, type=Path)
    parser.add_argument("--signing-identity", required=True)
    parser.add_argument("--output-root", required=True, type=Path)
    arguments = parser.parse_args(argv)
    try:
        result = build(arguments)
    except (OSError, DependencyGateError) as exc:
        print(f"WHEELHOUSE_BUILD_FAILED {exc}", file=sys.stderr)
        return 1
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
