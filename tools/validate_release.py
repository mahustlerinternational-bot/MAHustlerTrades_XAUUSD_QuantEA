#!/usr/bin/env python3
"""Validate a signed QUANT-SWARM-XAU artifact and optional installed tree."""

from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import re
import stat
import sys
import tarfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from release_common import (
    CONFIG_SCHEMA_VERSION,
    DOCKER_BASE,
    GENERATED_RELEASE_FILES,
    GIT_COMMIT,
    HEX_256,
    NATIVE_CONTRACT_PATH,
    PRODUCT_NAME,
    PRODUCT_VERSION,
    ReleaseError,
    TEST_CONTRACT_PATH,
    TEST_EVIDENCE_FILENAMES,
    TEST_EVIDENCE_LANES,
    TEST_EVIDENCE_LOCK_PATHS,
    checksum_manifest,
    deterministic_archive_bytes,
    parse_checksum_manifest,
    read_allowlist_bytes,
    release_directories,
    secure_read_bytes,
    sha256_bytes,
    source_version_contract,
    test_evidence_archive_path,
    validate_compile_evidence,
    validate_native_compile_log,
    validate_native_contract,
    validate_relative_path,
    validate_test_evidence_lane,
    verify_ssh_signature,
)
from scan_secrets import redact_path, scan_named_bytes


MAX_MEMBER_BYTES = 256 * 1024 * 1024
MAX_ARTIFACT_BYTES = 512 * 1024 * 1024
MAX_TOTAL_MEMBER_BYTES = 512 * 1024 * 1024
MAX_ARCHIVE_MEMBERS = 1024
SIGNING_IDENTITY = re.compile(r"^[A-Za-z0-9_.@-]{3,128}$")
RELEASE_FIELDS = frozenset(
    {
        "artifact_class",
        "artifact_schema_version",
        "broker_protocol_version",
        "compile_evidence_sha256",
        "compile_log_sha256",
        "compile_secondary_log_sha256",
        "config_schema_version",
        "docker_base",
        "mql5_binary_sha256",
        "mql5_ea_version",
        "mql5_source_sha256",
        "native_compile_contract_sha256",
        "permitted_modes",
        "product_name",
        "product_version",
        "python_runtime",
        "real_capital_enabled",
        "runtime_allowlist_sha256",
        "runtime_input_allowlist_sha256",
        "runtime_lock_sha256",
        "signature_namespace",
        "signing_identity",
        "source_commit",
        "source_date_epoch",
        "test_evidence",
    }
)


@dataclass(frozen=True, slots=True)
class ValidatedRelease:
    root_name: str
    files: dict[str, bytes]
    epoch: int


def _reject_secrets(path: str, value: bytes) -> None:
    findings = scan_named_bytes(path, value)
    if findings:
        rules = ",".join(sorted({finding.rule for finding in findings}))
        raise ReleaseError(
            f"secret scan rejected {redact_path(path)} ({rules})"
        )


def _verify_outer(
    artifact_name: str,
    artifact: bytes,
    checksum: bytes,
    signature: bytes,
    *,
    allowed_signers: bytes,
    identity: str,
) -> None:
    try:
        checksum_text = checksum.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ReleaseError("archive checksum is not UTF-8") from exc
    expected = f"{sha256_bytes(artifact)}  {artifact_name}\n"
    if checksum_text != expected:
        raise ReleaseError("archive checksum or bound filename does not match")
    verify_ssh_signature(
        checksum,
        signature,
        allowed_signers=allowed_signers,
        identity=identity,
    )


def _read_archive(artifact: bytes) -> tuple[str, dict[str, bytes], int]:
    if len(artifact) > MAX_ARTIFACT_BYTES:
        raise ReleaseError("release archive exceeds the size ceiling")
    header = artifact[:10]
    if len(header) != 10 or header[:2] != b"\x1f\x8b":
        raise ReleaseError("release artifact is not gzip")
    gzip_epoch = int.from_bytes(header[4:8], "little")

    files: dict[str, bytes] = {}
    seen: set[str] = set()
    casefolded: set[str] = set()
    root_names: set[str] = set()
    member_epochs: set[int] = set()
    total_member_bytes = 0
    try:
        with tarfile.open(fileobj=io.BytesIO(artifact), mode="r:gz") as archive:
            members = archive.getmembers()
            if not members:
                raise ReleaseError("release archive is empty")
            if len(members) > MAX_ARCHIVE_MEMBERS:
                raise ReleaseError("release archive contains too many members")
            for member in members:
                _reject_secrets(member.name, b"")
                raw_name = member.name.rstrip("/")
                validate_relative_path(raw_name)
                if member.name.startswith("/") or "//" in member.name:
                    raise ReleaseError("archive contains a noncanonical member name")
                if raw_name in seen or raw_name.casefold() in casefolded:
                    raise ReleaseError("archive contains a duplicate or case-fold collision")
                seen.add(raw_name)
                casefolded.add(raw_name.casefold())
                parts = PurePosixPath(raw_name).parts
                if not parts:
                    raise ReleaseError("archive member has no release root")
                root_names.add(parts[0])
                if member.uid != 0 or member.gid != 0:
                    raise ReleaseError("archive ownership metadata is not root:root")
                if member.uname != "root" or member.gname != "root":
                    raise ReleaseError("archive owner names are not normalized")
                if (
                    member.linkname
                    or member.pax_headers
                    or getattr(member, "sparse", None) is not None
                ):
                    raise ReleaseError("archive contains non-USTAR extension metadata")
                member_epochs.add(int(member.mtime))
                mode = stat.S_IMODE(member.mode)
                if member.isdir():
                    if mode != 0o555:
                        raise ReleaseError("archive directory mode is not 0555")
                    continue
                if not member.isreg():
                    raise ReleaseError("archive contains a link or special object")
                if member.size < 0 or member.size > MAX_MEMBER_BYTES:
                    raise ReleaseError("archive member exceeds the size ceiling")
                total_member_bytes += member.size
                if total_member_bytes > MAX_TOTAL_MEMBER_BYTES:
                    raise ReleaseError("release payload exceeds the size ceiling")
                if len(parts) < 2:
                    raise ReleaseError("archive root contains a regular file")
                relative = PurePosixPath(*parts[1:]).as_posix()
                expected_mode = 0o555 if relative.endswith(".sh") else 0o444
                if mode != expected_mode:
                    raise ReleaseError(f"unsafe or noncanonical mode for {relative}")
                extracted = archive.extractfile(member)
                if extracted is None:
                    raise ReleaseError(f"cannot read archive member {relative}")
                value = extracted.read()
                if len(value) != member.size:
                    raise ReleaseError(f"archive member size mismatch: {relative}")
                files[relative] = value
    except (gzip.BadGzipFile, tarfile.TarError, EOFError, OSError) as exc:
        raise ReleaseError("release archive is malformed or truncated") from exc
    if len(root_names) != 1:
        raise ReleaseError("archive must contain exactly one release root")
    if len(member_epochs) != 1:
        raise ReleaseError("archive member timestamps are not normalized")
    root_name = next(iter(root_names))
    member_epoch = next(iter(member_epochs))
    if gzip_epoch != member_epoch:
        raise ReleaseError("gzip and tar source epochs disagree")
    canonical = deterministic_archive_bytes(
        root_name=root_name,
        files=files,
        epoch=member_epoch,
    )
    if canonical != artifact:
        raise ReleaseError(
            "release archive is not the canonical single-gzip USTAR representation"
        )
    return root_name, files, member_epoch


def _json_object(value: bytes, label: str) -> dict[str, object]:
    try:
        parsed = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleaseError(f"{label} is not valid UTF-8 JSON") from exc
    if not isinstance(parsed, dict):
        raise ReleaseError(f"{label} must be a JSON object")
    return parsed


def _validate_payload(
    root_name: str,
    files: dict[str, bytes],
    epoch: int,
    *,
    allowlist: tuple[str, ...],
    allowlist_bytes: bytes,
    allowed_signers: bytes,
    identity: str,
) -> None:
    for relative in allowlist:
        _reject_secrets(relative, b"")
    expected_files = frozenset(allowlist) | GENERATED_RELEASE_FILES
    if frozenset(files) != expected_files:
        missing = sorted(expected_files - frozenset(files))
        extra = sorted(frozenset(files) - expected_files)
        raise ReleaseError(
            f"archive file set disagrees with allowlist; missing={missing} extra={extra}"
        )

    try:
        runtime_allowlist = tuple(
            line
            for line in files["RUNTIME_ALLOWLIST.txt"].decode("utf-8").splitlines()
            if line
        )
    except UnicodeDecodeError as exc:
        raise ReleaseError("embedded runtime allowlist is not UTF-8") from exc
    if runtime_allowlist != tuple(sorted(expected_files)):
        raise ReleaseError("embedded runtime allowlist disagrees with product contract")

    release = _json_object(files["RELEASE.json"], "RELEASE.json")
    if frozenset(release) != RELEASE_FIELDS:
        raise ReleaseError("RELEASE.json has missing or unknown fields")
    canonical_release = (
        json.dumps(release, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
    ).encode("utf-8")
    if files["RELEASE.json"] != canonical_release:
        raise ReleaseError("RELEASE.json is not canonical JSON")
    if (
        type(release.get("artifact_schema_version")) is not int
        or release["artifact_schema_version"] != 1
    ):
        raise ReleaseError("release manifest schema is unsupported")
    if release.get("artifact_class") != "shadow-demo-runtime":
        raise ReleaseError("artifact class is not the reviewed runtime class")
    if release.get("permitted_modes") != ["CHECK", "SHADOW", "DEMO"]:
        raise ReleaseError("release exposes an unreviewed runtime mode")
    if release.get("real_capital_enabled") is not False:
        raise ReleaseError("release claims real-capital authority")
    if release.get("python_runtime") != "CPython 3.12":
        raise ReleaseError("release runtime interpreter is not frozen to CPython 3.12")
    if release.get("signature_namespace") != "file":
        raise ReleaseError("release signature namespace is invalid")
    if release.get("signing_identity") != identity:
        raise ReleaseError("release signing identity is not the expected identity")
    commit = release.get("source_commit")
    if not isinstance(commit, str) or GIT_COMMIT.fullmatch(commit) is None:
        raise ReleaseError("release source commit is invalid")
    if (
        type(release.get("source_date_epoch")) is not int
        or release["source_date_epoch"] != epoch
    ):
        raise ReleaseError("release manifest and archive epochs disagree")
    if root_name != f"quant-swarm-xau-{PRODUCT_VERSION}-{commit[:12]}":
        raise ReleaseError("release root is not bound to product version and commit")

    source_contract = source_version_contract(files)
    native_contract = validate_native_contract(files[NATIVE_CONTRACT_PATH])
    minimum_ex5_bytes = native_contract["minimum_ex5_bytes"]
    assert isinstance(minimum_ex5_bytes, int)
    if len(files["mql5_ea/QuantSwarmXAU.ex5"]) < minimum_ex5_bytes:
        raise ReleaseError("packaged EX5 is below the native contract size floor")
    source_fields = {
        "product_name": source_contract["product_name"],
        "product_version": source_contract["product_version"],
        "broker_protocol_version": source_contract["broker_protocol_version"],
        "mql5_ea_version": source_contract["mql5_ea_version"],
        "config_schema_version": source_contract["config_schema_version"],
        "docker_base": source_contract["docker_base"],
    }
    for field, expected in source_fields.items():
        if release.get(field) != expected:
            raise ReleaseError(f"source and release manifest disagree: {field}")
    if (
        release.get("product_name") != PRODUCT_NAME
        or release.get("product_version") != PRODUCT_VERSION
        or release.get("config_schema_version") != CONFIG_SCHEMA_VERSION
        or release.get("docker_base") != DOCKER_BASE
    ):
        raise ReleaseError("release identity/schema/base contract is invalid")

    digest_contract = {
        "runtime_allowlist_sha256": sha256_bytes(files["RUNTIME_ALLOWLIST.txt"]),
        "runtime_input_allowlist_sha256": sha256_bytes(allowlist_bytes),
        "runtime_lock_sha256": sha256_bytes(
            files["python_swarm/requirements-runtime-py312.lock"]
        ),
        "mql5_source_sha256": sha256_bytes(files["mql5_ea/QuantSwarmXAU.mq5"]),
        "mql5_binary_sha256": sha256_bytes(files["mql5_ea/QuantSwarmXAU.ex5"]),
        "compile_evidence_sha256": sha256_bytes(files["MQL5_COMPILE.json"]),
        "compile_log_sha256": sha256_bytes(files["MQL5_COMPILE.log"]),
        "compile_secondary_log_sha256": sha256_bytes(
            files["MQL5_COMPILE.secondary.log"]
        ),
        "native_compile_contract_sha256": sha256_bytes(
            files[NATIVE_CONTRACT_PATH]
        ),
    }
    for field, expected in digest_contract.items():
        item = release.get(field)
        if (
            not isinstance(item, str)
            or HEX_256.fullmatch(item) is None
            or item != expected
        ):
            raise ReleaseError(f"release digest binding failed: {field}")

    expected_includes = native_contract["expected_includes"]
    assert isinstance(expected_includes, list)
    validate_native_compile_log(
        files["MQL5_COMPILE.log"],
        expected_includes=expected_includes,
        label="primary MetaEditor compile log",
    )
    validate_native_compile_log(
        files["MQL5_COMPILE.secondary.log"],
        expected_includes=expected_includes,
        label="secondary MetaEditor compile log",
    )
    evidence, _ = validate_compile_evidence(
        files["MQL5_COMPILE.json"],
        contract_bytes=files[NATIVE_CONTRACT_PATH],
        source_sha256=digest_contract["mql5_source_sha256"],
        binary_sha256=digest_contract["mql5_binary_sha256"],
        compile_log_sha256=digest_contract["compile_log_sha256"],
        secondary_compile_log_sha256=digest_contract[
            "compile_secondary_log_sha256"
        ],
        source_commit=commit,
        require_canonical=True,
    )
    if evidence["contract_sha256"] != digest_contract[
        "native_compile_contract_sha256"
    ]:
        raise ReleaseError("native evidence and release contract digest disagree")

    try:
        test_contract_bytes = files[TEST_CONTRACT_PATH]
        test_lock_bytes = {
            lane: files[TEST_EVIDENCE_LOCK_PATHS[lane]]
            for lane in TEST_EVIDENCE_LANES
        }
    except KeyError as exc:
        raise ReleaseError(
            "release omits a canonical test-evidence source input"
        ) from exc
    test_evidence_records: dict[str, dict[str, str]] = {}
    for lane in TEST_EVIDENCE_LANES:
        lane_files = {
            filename: files[test_evidence_archive_path(lane, filename)]
            for filename in TEST_EVIDENCE_FILENAMES
        }
        _, release_record = validate_test_evidence_lane(
            lane_files,
            lane=lane,
            source_commit=commit,
            signing_identity=identity,
            allowed_signers=allowed_signers,
            contract_bytes=test_contract_bytes,
            lock_bytes=test_lock_bytes[lane],
        )
        test_evidence_records[lane] = release_record
    if release.get("test_evidence") != test_evidence_records:
        raise ReleaseError(
            "release manifest test-evidence digest closure does not match"
        )

    checksummed_files = expected_files - {"SHA256SUMS.txt", "SHA256SUMS.txt.sig"}
    expected_sums = checksum_manifest(
        (relative, files[relative]) for relative in checksummed_files
    )
    if files["SHA256SUMS.txt"] != expected_sums:
        raise ReleaseError("checksum manifest is not canonical or does not match payload")
    checksum_records = parse_checksum_manifest(files["SHA256SUMS.txt"])
    if frozenset(checksum_records) != checksummed_files:
        raise ReleaseError("checksum manifest file set is incomplete or excessive")

    verify_ssh_signature(
        files["SHA256SUMS.txt"],
        files["SHA256SUMS.txt.sig"],
        allowed_signers=allowed_signers,
        identity=identity,
    )

    for relative, value in files.items():
        _reject_secrets(relative, value)


def validate_artifact(
    artifact: Path,
    *,
    allowlist_path: Path,
    allowed_signers_path: Path,
    identity: str,
) -> ValidatedRelease:
    if SIGNING_IDENTITY.fullmatch(identity) is None:
        raise ReleaseError("release signing identity is invalid")
    artifact_path = artifact.expanduser().absolute()
    checksum_path = artifact_path.with_suffix(artifact_path.suffix + ".sha256")
    signature_path = checksum_path.with_suffix(checksum_path.suffix + ".sig")

    # Snapshot every caller-controlled verification input before trusting any
    # relation between them.  No original path is reopened after this point.
    artifact_bytes = secure_read_bytes(
        artifact_path,
        "release artifact",
        max_bytes=MAX_ARTIFACT_BYTES,
    )
    checksum_bytes = secure_read_bytes(
        checksum_path,
        "archive checksum",
        max_bytes=4096,
    )
    signature_bytes = secure_read_bytes(
        signature_path,
        "archive checksum signature",
        max_bytes=1024 * 1024,
    )
    allowed_signers = secure_read_bytes(
        allowed_signers_path,
        "allowed signers",
        max_bytes=1024 * 1024,
    )
    allowlist_bytes = secure_read_bytes(
        allowlist_path,
        "runtime allowlist",
        max_bytes=1024 * 1024,
    )
    allowlist = read_allowlist_bytes(allowlist_bytes)

    _verify_outer(
        artifact_path.name,
        artifact_bytes,
        checksum_bytes,
        signature_bytes,
        allowed_signers=allowed_signers,
        identity=identity,
    )
    root_name, files, epoch = _read_archive(artifact_bytes)
    _validate_payload(
        root_name,
        files,
        epoch,
        allowlist=allowlist,
        allowlist_bytes=allowlist_bytes,
        allowed_signers=allowed_signers,
        identity=identity,
    )
    return ValidatedRelease(root_name=root_name, files=files, epoch=epoch)


def validate_installed_root(root: Path, release: ValidatedRelease) -> None:
    root = root.expanduser().absolute()
    try:
        root_metadata = root.lstat()
    except OSError as exc:
        raise ReleaseError("installed root is missing or unreadable") from exc
    if not stat.S_ISDIR(root_metadata.st_mode) or stat.S_ISLNK(root_metadata.st_mode):
        raise ReleaseError("installed root must be a non-symlink directory")
    if root_metadata.st_uid != 0 or root_metadata.st_gid != 0:
        raise ReleaseError("installed release root must be owned by root:root")
    if stat.S_IMODE(root_metadata.st_mode) != 0o555:
        raise ReleaseError("installed release root mode must be exactly 0555")

    expected_files = frozenset(release.files)
    expected_directories = frozenset(
        PurePosixPath(directory).relative_to(release.root_name).as_posix()
        for directory in release_directories(release.root_name, expected_files)
        if directory != release.root_name
    )
    actual_files: set[str] = set()
    actual_directories: set[str] = set()
    for candidate in sorted(root.rglob("*")):
        try:
            metadata = candidate.lstat()
        except OSError as exc:
            raise ReleaseError("cannot inspect installed release") from exc
        relative = candidate.relative_to(root).as_posix()
        _reject_secrets(relative, b"")
        validate_relative_path(relative)
        if stat.S_ISLNK(metadata.st_mode):
            raise ReleaseError("installed release contains a symlink")
        if metadata.st_uid != 0 or metadata.st_gid != 0:
            raise ReleaseError("installed release must be owned by root:root")
        if stat.S_ISDIR(metadata.st_mode):
            if stat.S_IMODE(metadata.st_mode) != 0o555:
                raise ReleaseError("installed release directory mode must be 0555")
            actual_directories.add(relative)
            continue
        if not stat.S_ISREG(metadata.st_mode):
            raise ReleaseError("installed release contains a special object")
        expected_mode = 0o555 if relative.endswith(".sh") else 0o444
        if stat.S_IMODE(metadata.st_mode) != expected_mode:
            raise ReleaseError(f"installed release mode is invalid: {relative}")
        actual_files.add(relative)
        expected = release.files.get(relative)
        if expected is None:
            continue
        observed = secure_read_bytes(
            candidate,
            f"installed release file {relative}",
            max_bytes=len(expected),
        )
        if observed != expected:
            raise ReleaseError(f"installed release content differs: {relative}")

    if frozenset(actual_files) != expected_files:
        missing = sorted(expected_files - frozenset(actual_files))
        extra = sorted(frozenset(actual_files) - expected_files)
        raise ReleaseError(
            f"installed release file set differs; missing={missing} extra={extra}"
        )
    if frozenset(actual_directories) != expected_directories:
        missing = sorted(expected_directories - frozenset(actual_directories))
        extra = sorted(frozenset(actual_directories) - expected_directories)
        raise ReleaseError(
            f"installed release directory set differs; missing={missing} extra={extra}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", required=True, type=Path)
    parser.add_argument("--installed-root", type=Path)
    parser.add_argument("--allowlist", required=True, type=Path)
    parser.add_argument("--allowed-signers", required=True, type=Path)
    parser.add_argument("--signing-identity", required=True)
    arguments = parser.parse_args(argv)
    try:
        release = validate_artifact(
            arguments.artifact,
            allowlist_path=arguments.allowlist,
            allowed_signers_path=arguments.allowed_signers,
            identity=arguments.signing_identity,
        )
        if arguments.installed_root is not None:
            validate_installed_root(arguments.installed_root, release)
    except (OSError, ReleaseError) as exc:
        print(f"RELEASE_INVALID {exc}", file=sys.stderr)
        return 1
    print("VALID")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
