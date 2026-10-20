#!/usr/bin/env python3
"""Validate one signed offline wheelhouse from stable no-follow snapshots."""

from __future__ import annotations

import argparse
import json
import os
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

from dependency_common import (
    DependencyGateError,
    FILE_FIELDS,
    GIT_COMMIT,
    HEX_256,
    INTERPRETER_FIELDS,
    MANIFEST_FIELDS,
    MANIFEST_NAME,
    SAFE_IDENTITY,
    SIGNATURE_NAME,
    SOURCE_FILE_FIELDS,
    WHEELS_DIRECTORY,
    assert_canonical_ssh_signature,
    assert_not_writable,
    canonical_json,
    git_blob,
    interpreter_pip_version,
    isolated_python_environment,
    lane_contract,
    open_directory_path,
    parse_lock,
    resolve_executable_path,
    sha256_bytes,
    snapshot_at,
    snapshot_path,
    snapshot_relative_file,
    source_commit,
    trusted_system_executable,
    validate_records_against_lock,
    validate_source_record,
    wheel_record,
)


def _verify_signature(
    *,
    manifest: bytes,
    signature: bytes,
    allowed_signers: bytes,
    identity: str,
) -> None:
    ssh_keygen = trusted_system_executable("ssh-keygen")
    with tempfile.TemporaryDirectory(prefix="qsx-wheelhouse-verify-") as raw:
        temporary = Path(raw)
        allowed = temporary / "allowed_signers"
        signature_path = temporary / "manifest.sig"
        allowed.write_bytes(allowed_signers)
        signature_path.write_bytes(signature)
        allowed.chmod(0o400)
        signature_path.chmod(0o400)
        completed = subprocess.run(
            [
                os.fspath(ssh_keygen),
                "-Y",
                "verify",
                "-f",
                os.fspath(allowed),
                "-I",
                identity,
                "-n",
                "file",
                "-s",
                os.fspath(signature_path),
            ],
            input=manifest,
            check=False,
            capture_output=True,
            env={**isolated_python_environment(), "LC_ALL": "C"},
        )
    if completed.returncode != 0:
        raise DependencyGateError("wheelhouse signature verification failed")


def _parse_manifest(value: bytes) -> dict[str, object]:
    try:
        parsed = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DependencyGateError("wheelhouse manifest is not valid UTF-8 JSON") from exc
    if not isinstance(parsed, dict) or frozenset(parsed) != MANIFEST_FIELDS:
        raise DependencyGateError("wheelhouse manifest has missing or unknown fields")
    if canonical_json(parsed) != value:
        raise DependencyGateError("wheelhouse manifest is not canonical JSON")
    return parsed


def _expected_commit(source_root: Path, explicit: str | None) -> tuple[str, bool]:
    if explicit is not None:
        if GIT_COMMIT.fullmatch(explicit) is None:
            raise DependencyGateError("expected source commit is invalid")
        return explicit, False
    return source_commit(source_root), True


def validate(arguments: argparse.Namespace) -> dict[str, object]:
    lane = lane_contract(arguments.lane)
    if SAFE_IDENTITY.fullmatch(arguments.signing_identity) is None:
        raise DependencyGateError("wheelhouse signing identity is invalid")
    source_root = arguments.source_root.expanduser().absolute()
    wheelhouse = arguments.wheelhouse.expanduser().absolute()

    allowed, allowed_metadata = snapshot_path(
        arguments.allowed_signers,
        label="wheelhouse allowed signers",
        maximum_size=1024 * 1024,
    )
    assert_not_writable(
        allowed_metadata,
        "wheelhouse allowed signers",
    )
    root_fd = open_directory_path(wheelhouse)
    try:
        root_metadata = os.fstat(root_fd)
        assert_not_writable(root_metadata, "wheelhouse root")
        root_entries = set(os.listdir(root_fd))
        expected_root = {MANIFEST_NAME, SIGNATURE_NAME, WHEELS_DIRECTORY}
        if root_entries != expected_root:
            raise DependencyGateError(
                f"wheelhouse root closure mismatch: {sorted(root_entries)}"
            )
        manifest_value, manifest_metadata = snapshot_at(
            root_fd,
            MANIFEST_NAME,
            label="wheelhouse manifest",
            maximum_size=4 * 1024 * 1024,
        )
        signature, signature_metadata = snapshot_at(
            root_fd,
            SIGNATURE_NAME,
            label="wheelhouse signature",
            maximum_size=1024 * 1024,
        )
        assert_not_writable(manifest_metadata, "wheelhouse manifest")
        assert_not_writable(signature_metadata, "wheelhouse signature")
        assert_canonical_ssh_signature(signature, "wheelhouse signature")
        _verify_signature(
            manifest=manifest_value,
            signature=signature,
            allowed_signers=allowed,
            identity=arguments.signing_identity,
        )
        manifest = _parse_manifest(manifest_value)
        if manifest.get("schema_version") != 1:
            raise DependencyGateError("wheelhouse manifest schema is unsupported")
        if manifest.get("artifact_class") != "offline-python-wheelhouse":
            raise DependencyGateError("wheelhouse artifact class is invalid")
        if manifest.get("lane") != lane.name:
            raise DependencyGateError("wheelhouse lane does not match requested lane")
        if manifest.get("signing_identity") != arguments.signing_identity:
            raise DependencyGateError("wheelhouse signing identity is not pinned")

        interpreter = manifest.get("interpreter")
        if not isinstance(interpreter, dict) or frozenset(interpreter) != INTERPRETER_FIELDS:
            raise DependencyGateError("wheelhouse interpreter record is invalid")
        expected_interpreter = {
            "abi": lane.abi,
            "implementation": lane.implementation,
            "implementation_name": lane.implementation_name,
            "machine": lane.machine,
            "pip_version": lane.pip_version,
            "platform": lane.platform,
            "version": lane.python_version,
        }
        for field, expected in expected_interpreter.items():
            if interpreter.get(field) != expected:
                raise DependencyGateError(
                    f"wheelhouse interpreter field is invalid: {field}"
                )
        executable_path = interpreter.get("executable_path")
        executable_digest = interpreter.get("executable_sha256")
        executable_size = interpreter.get("executable_size")
        if (
            not isinstance(executable_path, str)
            or not Path(executable_path).is_absolute()
            or ".." in Path(executable_path).parts
            or Path(executable_path) != Path(os.path.normpath(executable_path))
            or not isinstance(executable_digest, str)
            or HEX_256.fullmatch(executable_digest) is None
            or type(executable_size) is not int
            or executable_size < 1
        ):
            raise DependencyGateError("wheelhouse executable identity is invalid")
        if arguments.interpreter is not None:
            resolved, executable_value, executable_metadata = resolve_executable_path(
                arguments.interpreter
            )
            if (
                os.fspath(resolved) != executable_path
                or sha256_bytes(executable_value) != executable_digest
                or executable_metadata.st_size != executable_size
                or interpreter_pip_version(resolved) != lane.pip_version
            ):
                raise DependencyGateError(
                    "wheelhouse executable identity does not match"
                )
        expected_commit, verify_git = _expected_commit(
            source_root,
            arguments.expected_source_commit,
        )
        if manifest.get("source_commit") != expected_commit:
            raise DependencyGateError("wheelhouse source commit does not match")

        source_fd = open_directory_path(source_root)
        try:
            lock_record = validate_source_record(
                manifest.get("lock"),
                expected_path=lane.lock_path,
            )
            lock_value, lock_metadata = snapshot_relative_file(
                source_fd,
                lane.lock_path,
                label="canonical dependency lock",
                maximum_size=8 * 1024 * 1024,
            )
            if (
                lock_record["sha256"] != sha256_bytes(lock_value)
                or lock_record["size"] != lock_metadata.st_size
            ):
                raise DependencyGateError(
                    "wheelhouse lock binding does not match canonical lock"
                )
            if verify_git and lock_value != git_blob(
                source_root,
                expected_commit,
                lane.lock_path,
            ):
                raise DependencyGateError("canonical dependency lock differs from Git")

            inputs = manifest.get("inputs")
            if not isinstance(inputs, list) or len(inputs) != len(lane.input_paths):
                raise DependencyGateError("wheelhouse source input records are invalid")
            for record, relative in zip(inputs, lane.input_paths, strict=True):
                validated = validate_source_record(record, expected_path=relative)
                try:
                    input_value, input_metadata = snapshot_relative_file(
                        source_fd,
                        relative,
                        label=f"dependency input {relative}",
                        maximum_size=8 * 1024 * 1024,
                    )
                except DependencyGateError:
                    if verify_git or arguments.require_source_inputs:
                        raise
                    continue
                if (
                    validated["sha256"] != sha256_bytes(input_value)
                    or validated["size"] != input_metadata.st_size
                ):
                    raise DependencyGateError(
                        f"wheelhouse input binding does not match: {relative}"
                    )
                if verify_git and input_value != git_blob(
                    source_root,
                    expected_commit,
                    relative,
                ):
                    raise DependencyGateError(
                        f"canonical dependency input differs from Git: {relative}"
                    )
        finally:
            os.close(source_fd)

        requirements = parse_lock(lock_value)
        files = manifest.get("files")
        if not isinstance(files, list):
            raise DependencyGateError("wheelhouse file records are invalid")
        manifest_records: list[dict[str, object]] = []
        for item in files:
            if not isinstance(item, dict) or frozenset(item) != FILE_FIELDS:
                raise DependencyGateError(
                    "wheelhouse file record has missing or unknown fields"
                )
            manifest_records.append(item)
        validate_records_against_lock(manifest_records, requirements, lane)
        if [record.get("filename") for record in manifest_records] != sorted(
            record.get("filename") for record in manifest_records
        ):
            raise DependencyGateError("wheelhouse file records are not sorted")

        wheels_fd = os.open(
            WHEELS_DIRECTORY,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_DIRECTORY", 0),
            dir_fd=root_fd,
        )
        try:
            wheels_metadata = os.fstat(wheels_fd)
            assert_not_writable(wheels_metadata, "wheel directory")
            actual_names = set(os.listdir(wheels_fd))
            expected_names = {
                str(record["filename"]) for record in manifest_records
            }
            if actual_names != expected_names:
                raise DependencyGateError(
                    "wheelhouse contains missing or extra artifacts"
                )
            actual_records: list[dict[str, object]] = []
            for filename in sorted(actual_names):
                if not filename.endswith(".whl"):
                    raise DependencyGateError(
                        f"source distributions are forbidden: {filename}"
                    )
                wheel_value, wheel_metadata = snapshot_at(
                    wheels_fd,
                    filename,
                    label=f"wheel {filename}",
                    maximum_size=1024 * 1024 * 1024,
                )
                assert_not_writable(
                    wheel_metadata,
                    f"wheel {filename}",
                )
                actual_records.append(wheel_record(filename, wheel_value))
        finally:
            os.close(wheels_fd)
        if actual_records != manifest_records:
            raise DependencyGateError("wheelhouse file hash/size/tag binding failed")
    finally:
        os.close(root_fd)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--wheelhouse", required=True, type=Path)
    parser.add_argument(
        "--lane",
        required=True,
        choices=("runtime-py312", "test-py312", "test-py314"),
    )
    parser.add_argument("--allowed-signers", required=True, type=Path)
    parser.add_argument("--signing-identity", required=True)
    parser.add_argument("--expected-source-commit")
    parser.add_argument("--interpreter", type=Path)
    parser.add_argument("--require-source-inputs", action="store_true")
    arguments = parser.parse_args(argv)
    try:
        manifest = validate(arguments)
    except (OSError, DependencyGateError) as exc:
        print(f"WHEELHOUSE_INVALID {exc}", file=sys.stderr)
        return 1
    print(
        f"WHEELHOUSE_VALID lane={manifest['lane']} "
        f"source_commit={manifest['source_commit']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
