#!/usr/bin/env python3
"""Build a deterministic, signed QUANT-SWARM-XAU runtime artifact."""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

from release_common import (
    GENERATED_RELEASE_FILES,
    NATIVE_CONTRACT_PATH,
    ReleaseError,
    TEST_CONTRACT_PATH,
    TEST_EVIDENCE_LANES,
    TEST_EVIDENCE_LOCK_PATHS,
    checksum_manifest,
    clean_git_metadata,
    deterministic_archive_bytes,
    git_blob,
    read_allowlist_bytes,
    regular_source_file,
    secure_read_bytes,
    secure_snapshot_test_evidence_directory,
    sha256_bytes,
    source_version_contract,
    test_evidence_archive_path,
    trusted_ssh_keygen,
    validate_compile_evidence,
    validate_native_compile_log,
    validate_native_contract,
    validate_ssh_signature_bytes,
    validate_test_evidence_lane,
    write_exclusive_bytes,
)
from scan_secrets import redact_path, scan_named_bytes


BANNED_EX5_SHA256 = frozenset(
    {
        # Quarantined 4.0.0-rc1 binary compiled before the RC2 protocol fence.
        "e8d9f4804aa82248eb23ed1bfc28365a05441536219208dbb61ec2f64e943df6",
    }
)


def _reject_secrets(path: str, value: bytes) -> None:
    findings = scan_named_bytes(path, value)
    if findings:
        rules = ",".join(sorted({finding.rule for finding in findings}))
        raise ReleaseError(
            f"secret scan rejected {redact_path(path)} ({rules})"
        )


def _regular_external_file(path: Path, label: str) -> Path:
    path = path.expanduser().absolute()
    if path.is_symlink() or not path.is_file():
        raise ReleaseError(f"{label} must be a regular non-symlink file: {path}")
    return path


def _validate_signing_key(path: Path) -> Path:
    path = _regular_external_file(path, "release signing key")
    metadata = path.stat()
    if metadata.st_uid != os.getuid():
        raise ReleaseError("release signing key must be owned by the invoking identity")
    if stat.S_IMODE(metadata.st_mode) not in {0o400, 0o600}:
        raise ReleaseError("release signing key mode must be 0400 or 0600")
    trusted_ssh_keygen()
    return path


def _sign_file(path: Path, key: Path) -> Path:
    signature = path.with_suffix(path.suffix + ".sig")
    if signature.exists() or signature.is_symlink():
        raise ReleaseError(f"refusing to replace existing signature: {signature}")
    completed = subprocess.run(
        [
            os.fspath(trusted_ssh_keygen()),
            "-Y",
            "sign",
            "-f",
            os.fspath(key),
            "-n",
            "file",
            os.fspath(path),
        ],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "LC_ALL": "C", "TZ": "UTC"},
    )
    if (
        completed.returncode != 0
        or signature.is_symlink()
        or not signature.is_file()
    ):
        raise ReleaseError("OpenSSH release signing failed")
    signature.chmod(0o600, follow_symlinks=False)
    signature_bytes = secure_read_bytes(
        signature,
        "generated OpenSSH signature",
        max_bytes=1024 * 1024,
    )
    validate_ssh_signature_bytes(signature_bytes)
    return signature


def build(arguments: argparse.Namespace) -> Path:
    source_root = arguments.source_root.expanduser().absolute()
    if source_root.is_symlink() or not source_root.is_dir():
        raise ReleaseError("source root must be a regular directory")
    source_root = source_root.resolve(strict=True)
    commit, epoch = clean_git_metadata(source_root)
    allowlist_path = arguments.allowlist.expanduser().absolute()
    canonical_allowlist = source_root / "release" / "runtime-allowlist.txt"
    if allowlist_path.resolve(strict=True) != canonical_allowlist.resolve(strict=True):
        raise ReleaseError(
            "runtime allowlist must be the canonical file inside the source tree"
        )
    allowlist_bytes = secure_read_bytes(
        allowlist_path,
        "runtime allowlist",
        max_bytes=1024 * 1024,
    )
    if allowlist_bytes != git_blob(
        source_root, commit, "release/runtime-allowlist.txt"
    ):
        raise ReleaseError("runtime allowlist differs from Git HEAD")
    allowlist = read_allowlist_bytes(allowlist_bytes)
    for relative in allowlist:
        _reject_secrets(relative, b"")

    files: dict[str, bytes] = {}
    for relative in allowlist:
        working_value = secure_read_bytes(
            regular_source_file(source_root, relative),
            f"allowlisted source {relative}",
            max_bytes=256 * 1024 * 1024,
        )
        if working_value != git_blob(source_root, commit, relative):
            raise ReleaseError(f"allowlisted source differs from Git HEAD: {relative}")
        files[relative] = working_value

    try:
        native_contract_bytes = files[NATIVE_CONTRACT_PATH]
    except KeyError as exc:
        raise ReleaseError(
            "runtime allowlist omits the canonical native build contract"
        ) from exc
    native_contract = validate_native_contract(native_contract_bytes)

    ex5_bytes = secure_read_bytes(
        arguments.ex5,
        "freshly compiled EX5",
        max_bytes=256 * 1024 * 1024,
    )
    minimum_ex5_bytes = native_contract["minimum_ex5_bytes"]
    assert isinstance(minimum_ex5_bytes, int)
    if len(ex5_bytes) < minimum_ex5_bytes:
        raise ReleaseError("freshly compiled EX5 is below the native contract size floor")
    ex5_sha256 = sha256_bytes(ex5_bytes)
    if ex5_sha256 in BANNED_EX5_SHA256:
        raise ReleaseError("the quarantined pre-RC2 EX5 cannot enter an RC2 release")

    compile_log_bytes = secure_read_bytes(
        arguments.compile_log,
        "MetaEditor compile log",
        max_bytes=64 * 1024 * 1024,
    )
    secondary_compile_log_bytes = secure_read_bytes(
        arguments.secondary_compile_log,
        "secondary MetaEditor compile log",
        max_bytes=64 * 1024 * 1024,
    )
    expected_includes = native_contract["expected_includes"]
    assert isinstance(expected_includes, list)
    validate_native_compile_log(
        compile_log_bytes,
        expected_includes=expected_includes,
        label="primary MetaEditor compile log",
    )
    validate_native_compile_log(
        secondary_compile_log_bytes,
        expected_includes=expected_includes,
        label="secondary MetaEditor compile log",
    )
    compile_evidence_input = secure_read_bytes(
        arguments.compile_evidence,
        "native compile evidence",
        max_bytes=1024 * 1024,
    )
    mq5_sha256 = sha256_bytes(files["mql5_ea/QuantSwarmXAU.mq5"])
    compile_evidence, compile_evidence_bytes = validate_compile_evidence(
        compile_evidence_input,
        contract_bytes=native_contract_bytes,
        source_sha256=mq5_sha256,
        binary_sha256=ex5_sha256,
        compile_log_sha256=sha256_bytes(compile_log_bytes),
        secondary_compile_log_sha256=sha256_bytes(secondary_compile_log_bytes),
        source_commit=commit,
        require_canonical=False,
    )
    versions = source_version_contract(files)
    signing_key = _validate_signing_key(arguments.signing_key)
    signing_identity = arguments.signing_identity
    if re.fullmatch(r"[A-Za-z0-9_.@-]{3,128}", signing_identity) is None:
        raise ReleaseError("release signing identity is invalid")
    allowed_signers_bytes = secure_read_bytes(
        arguments.allowed_signers,
        "release and test-evidence allowed signers",
        max_bytes=1024 * 1024,
    )
    try:
        test_contract_bytes = files[TEST_CONTRACT_PATH]
    except KeyError as exc:
        raise ReleaseError(
            "runtime allowlist omits the canonical test evidence contract"
        ) from exc
    evidence_roots = {
        "test-py312": arguments.test_evidence_py312,
        "test-py314": arguments.test_evidence_py314,
    }
    test_evidence_records: dict[str, dict[str, str]] = {}
    for lane in TEST_EVIDENCE_LANES:
        lane_files = secure_snapshot_test_evidence_directory(
            evidence_roots[lane],
            lane=lane,
        )
        _, release_record = validate_test_evidence_lane(
            lane_files,
            lane=lane,
            source_commit=commit,
            signing_identity=signing_identity,
            allowed_signers=allowed_signers_bytes,
            contract_bytes=test_contract_bytes,
            lock_bytes=files[TEST_EVIDENCE_LOCK_PATHS[lane]],
        )
        test_evidence_records[lane] = release_record
        for filename, value in lane_files.items():
            files[test_evidence_archive_path(lane, filename)] = value

    files["mql5_ea/QuantSwarmXAU.ex5"] = ex5_bytes
    files["MQL5_COMPILE.json"] = compile_evidence_bytes
    files["MQL5_COMPILE.log"] = compile_log_bytes
    files["MQL5_COMPILE.secondary.log"] = secondary_compile_log_bytes
    final_allowlist = tuple(
        sorted(
            (
                *files,
                "RELEASE.json",
                "RUNTIME_ALLOWLIST.txt",
                "SHA256SUMS.txt",
                "SHA256SUMS.txt.sig",
            )
        )
    )
    if frozenset(final_allowlist) != frozenset(allowlist) | GENERATED_RELEASE_FILES:
        raise ReleaseError("generated release file contract is inconsistent")
    runtime_allowlist_bytes = "".join(f"{entry}\n" for entry in final_allowlist).encode(
        "utf-8"
    )
    files["RUNTIME_ALLOWLIST.txt"] = runtime_allowlist_bytes

    release_manifest = {
        "artifact_class": "shadow-demo-runtime",
        "artifact_schema_version": 1,
        "broker_protocol_version": versions["broker_protocol_version"],
        "compile_evidence_sha256": sha256_bytes(compile_evidence_bytes),
        "compile_log_sha256": compile_evidence["compile_log_sha256"],
        "compile_secondary_log_sha256": compile_evidence[
            "secondary_compile_log_sha256"
        ],
        "config_schema_version": versions["config_schema_version"],
        "docker_base": versions["docker_base"],
        "mql5_binary_sha256": ex5_sha256,
        "mql5_ea_version": versions["mql5_ea_version"],
        "mql5_source_sha256": mq5_sha256,
        "native_compile_contract_sha256": compile_evidence["contract_sha256"],
        "permitted_modes": ["CHECK", "SHADOW", "DEMO"],
        "product_name": versions["product_name"],
        "product_version": versions["product_version"],
        "python_runtime": "CPython 3.12",
        "real_capital_enabled": False,
        "runtime_allowlist_sha256": sha256_bytes(runtime_allowlist_bytes),
        "runtime_input_allowlist_sha256": sha256_bytes(allowlist_bytes),
        "runtime_lock_sha256": sha256_bytes(
            files["python_swarm/requirements-runtime-py312.lock"]
        ),
        "signature_namespace": "file",
        "signing_identity": signing_identity,
        "source_commit": commit,
        "source_date_epoch": epoch,
        "test_evidence": test_evidence_records,
    }
    files["RELEASE.json"] = (
        json.dumps(release_manifest, indent=2, sort_keys=True, ensure_ascii=True)
        + "\n"
    ).encode("utf-8")
    sums = checksum_manifest(sorted(files.items()))

    output_dir = arguments.output_dir.expanduser().absolute()
    if output_dir.is_symlink():
        raise ReleaseError("release output directory must not be a symlink")
    output_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    if not output_dir.is_dir():
        raise ReleaseError("release output path is not a directory")
    output_dir = output_dir.resolve(strict=True)
    try:
        output_dir.relative_to(source_root)
    except ValueError:
        pass
    else:
        raise ReleaseError("release output directory must be outside the source tree")
    output_metadata = output_dir.stat()
    if output_metadata.st_uid != os.getuid():
        raise ReleaseError("release output directory must be owned by the invoking identity")
    if stat.S_IMODE(output_metadata.st_mode) & 0o022:
        raise ReleaseError("release output directory must not be group/world writable")
    root_name = (
        f"quant-swarm-xau-{versions['product_version']}-{commit[:12]}"
    )
    archive_path = output_dir / f"{root_name}.tar.gz"
    for candidate in (
        archive_path,
        archive_path.with_suffix(archive_path.suffix + ".sha256"),
        archive_path.with_suffix(archive_path.suffix + ".sha256.sig"),
    ):
        if candidate.exists() or candidate.is_symlink():
            raise ReleaseError(f"refusing to replace existing output: {candidate}")

    with tempfile.TemporaryDirectory(prefix="qsx-release-sign-", dir=output_dir) as raw:
        signing_dir = Path(raw)
        sums_path = signing_dir / "SHA256SUMS.txt"
        sums_path.write_bytes(sums)
        internal_signature_path = _sign_file(sums_path, signing_key)
        files["SHA256SUMS.txt"] = sums
        files["SHA256SUMS.txt.sig"] = internal_signature_path.read_bytes()

    for relative, value in files.items():
        _reject_secrets(relative, value)

    archive_bytes = deterministic_archive_bytes(
        root_name=root_name,
        files=files,
        epoch=epoch,
    )
    write_exclusive_bytes(archive_path, archive_bytes)
    outer_checksum_path = archive_path.with_suffix(archive_path.suffix + ".sha256")
    outer_checksum_bytes = (
        f"{sha256_bytes(archive_bytes)}  {archive_path.name}\n"
    ).encode("utf-8")
    write_exclusive_bytes(
        outer_checksum_path,
        outer_checksum_bytes,
    )
    _sign_file(outer_checksum_path, signing_key)
    return archive_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--allowlist", required=True, type=Path)
    parser.add_argument("--ex5", required=True, type=Path)
    parser.add_argument("--compile-evidence", required=True, type=Path)
    parser.add_argument("--compile-log", required=True, type=Path)
    parser.add_argument("--secondary-compile-log", required=True, type=Path)
    parser.add_argument("--test-evidence-py312", required=True, type=Path)
    parser.add_argument("--test-evidence-py314", required=True, type=Path)
    parser.add_argument("--allowed-signers", required=True, type=Path)
    parser.add_argument("--signing-key", required=True, type=Path)
    parser.add_argument("--signing-identity", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    arguments = parser.parse_args(argv)
    try:
        archive = build(arguments)
    except (OSError, ReleaseError) as exc:
        print(f"RELEASE_BUILD_FAILED {exc}", file=sys.stderr)
        return 1
    print(archive)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
