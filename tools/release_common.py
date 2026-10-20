"""Shared fail-closed helpers for QUANT-SWARM-XAU release tooling."""

from __future__ import annotations

import base64
import binascii
import hashlib
import codecs
import gzip
import io
import json
import os
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
from pathlib import Path, PurePosixPath
from typing import Iterable

try:
    from build_mql5_native import (
        EVIDENCE_FIELDS as NATIVE_EVIDENCE_FIELDS,
        SANDBOX_PROFILE as NATIVE_SANDBOX_PROFILE,
        NativeBuildError,
        _validate_log as validate_metaeditor_log,
        canonical_json as native_canonical_json,
    )
except ModuleNotFoundError:  # Imported as tools.release_common by the test suite.
    from tools.build_mql5_native import (
        EVIDENCE_FIELDS as NATIVE_EVIDENCE_FIELDS,
        SANDBOX_PROFILE as NATIVE_SANDBOX_PROFILE,
        NativeBuildError,
        _validate_log as validate_metaeditor_log,
        canonical_json as native_canonical_json,
    )

try:
    from run_test_evidence import (
        CONTRACT_FIELDS as TEST_CONTRACT_FIELDS,
        EVIDENCE_FIELDS as TEST_EVIDENCE_FIELDS,
        INTERPRETER_FIELDS as TEST_INTERPRETER_FIELDS,
        RESULT_FIELDS as TEST_RESULT_FIELDS,
        SANDBOX_PROFILE as TEST_SANDBOX_PROFILE,
        canonical_json as test_canonical_json,
    )
    from dependency_common import (
        LANES as DEPENDENCY_LANES,
        DependencyGateError,
        parse_lock as parse_dependency_lock,
    )
except ModuleNotFoundError:  # Imported as tools.release_common by the test suite.
    from tools.run_test_evidence import (
        CONTRACT_FIELDS as TEST_CONTRACT_FIELDS,
        EVIDENCE_FIELDS as TEST_EVIDENCE_FIELDS,
        INTERPRETER_FIELDS as TEST_INTERPRETER_FIELDS,
        RESULT_FIELDS as TEST_RESULT_FIELDS,
        SANDBOX_PROFILE as TEST_SANDBOX_PROFILE,
        canonical_json as test_canonical_json,
    )
    from tools.dependency_common import (
        LANES as DEPENDENCY_LANES,
        DependencyGateError,
        parse_lock as parse_dependency_lock,
    )


TEST_EVIDENCE_LANES = ("test-py312", "test-py314")
TEST_EVIDENCE_LOCK_PATHS = {
    lane: DEPENDENCY_LANES[lane].lock_path for lane in TEST_EVIDENCE_LANES
}
TEST_EVIDENCE_FILENAMES = (
    "PYTEST.log",
    "PYTEST_NODEIDS.txt",
    "TEST_EVIDENCE.json",
    "TEST_EVIDENCE.json.sig",
)
TEST_CONTRACT_PATH = "release/test-evidence-contract.json"
TEST_EVIDENCE_RELEASE_FILES = frozenset(
    f"test-evidence/{lane}/{filename}"
    for lane in TEST_EVIDENCE_LANES
    for filename in TEST_EVIDENCE_FILENAMES
)

GENERATED_RELEASE_FILES = frozenset(
    {
        "MQL5_COMPILE.json",
        "MQL5_COMPILE.log",
        "MQL5_COMPILE.secondary.log",
        "RELEASE.json",
        "RUNTIME_ALLOWLIST.txt",
        "SHA256SUMS.txt",
        "SHA256SUMS.txt.sig",
        "mql5_ea/QuantSwarmXAU.ex5",
        *TEST_EVIDENCE_RELEASE_FILES,
    }
)
COMPILE_FIELDS = frozenset(NATIVE_EVIDENCE_FIELDS)
NATIVE_CONTRACT_FIELDS = frozenset(
    {
        "compiler",
        "dependencies",
        "expected_includes",
        "expected_process_exit_code",
        "host_tools",
        "minimum_ex5_bytes",
        "schema_version",
        "source_path",
        "wine_version",
    }
)
NATIVE_CONTRACT_PATH = "release/native-mql5-build-contract.json"
DOCKER_BASE = (
    "python:3.12.13-slim-bookworm@"
    "sha256:72d3d75f2639ab82b34b29390ad3d6e0827c775befee94edda8e9976818f488d"
)
HEX_256 = re.compile(r"^[0-9a-f]{64}$")
GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")
PRODUCT_NAME = "QUANT-SWARM-XAU"
PRODUCT_VERSION = "4.0.0-rc2"
CONFIG_SCHEMA_VERSION = 2


class ReleaseError(RuntimeError):
    """A release input or artifact violates the frozen contract."""


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def secure_read_bytes(
    path: Path,
    label: str,
    *,
    max_bytes: int | None = None,
) -> bytes:
    """Read one immutable regular-file snapshot without following its final link.

    All verification callers operate on the returned bytes.  They never reopen
    the caller-controlled path after a signature or checksum decision.
    """

    candidate = path.expanduser().absolute()
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise ReleaseError(f"{label} is not a readable regular non-symlink file") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ReleaseError(f"{label} must be a regular file")
        if max_bytes is not None and (before.st_size < 0 or before.st_size > max_bytes):
            raise ReleaseError(f"{label} exceeds its size ceiling")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise ReleaseError(f"{label} changed while it was being read")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise ReleaseError(f"{label} changed while it was being read")
        after = os.fstat(descriptor)
        stable_fields = (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_uid",
            "st_gid",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
            raise ReleaseError(f"{label} changed while it was being read")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def secure_snapshot_test_evidence_directory(
    path: Path,
    *,
    lane: str,
) -> dict[str, bytes]:
    """Snapshot one exact producer output through a pinned directory fd."""

    if lane not in TEST_EVIDENCE_LANES:
        raise ReleaseError("test evidence lane is unsupported")
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
        raise ReleaseError("platform lacks no-follow directory snapshot support")
    directory_path = path.expanduser().absolute()
    flags = (
        os.O_RDONLY
        | os.O_CLOEXEC
        | os.O_NOFOLLOW
        | os.O_DIRECTORY
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        directory_fd = os.open(directory_path, flags)
    except OSError as exc:
        raise ReleaseError(
            f"{lane} evidence root is not a readable non-symlink directory"
        ) from exc
    try:
        before = os.fstat(directory_fd)
        if (
            not stat.S_ISDIR(before.st_mode)
            or before.st_uid != os.getuid()
            or stat.S_IMODE(before.st_mode) != 0o500
        ):
            raise ReleaseError(
                f"{lane} evidence root ownership or mode is not canonical"
            )
        expected = set(TEST_EVIDENCE_FILENAMES)
        if set(os.listdir(directory_fd)) != expected:
            raise ReleaseError(f"{lane} evidence root closure is not exact")
        ceilings = {
            "PYTEST.log": 64 * 1024 * 1024,
            "PYTEST_NODEIDS.txt": 8 * 1024 * 1024,
            "TEST_EVIDENCE.json": 8 * 1024 * 1024,
            "TEST_EVIDENCE.json.sig": 1024 * 1024,
        }
        result: dict[str, bytes] = {}
        child_flags = (
            os.O_RDONLY
            | os.O_CLOEXEC
            | os.O_NOFOLLOW
            | getattr(os, "O_NONBLOCK", 0)
        )
        for name in TEST_EVIDENCE_FILENAMES:
            try:
                child_fd = os.open(
                    name,
                    child_flags,
                    dir_fd=directory_fd,
                )
            except OSError as exc:
                raise ReleaseError(
                    f"{lane} evidence file cannot be snapshotted"
                ) from exc
            try:
                child_before = os.fstat(child_fd)
                if (
                    not stat.S_ISREG(child_before.st_mode)
                    or child_before.st_uid != os.getuid()
                    or child_before.st_nlink != 1
                    or stat.S_IMODE(child_before.st_mode) != 0o400
                    or child_before.st_size < 0
                    or child_before.st_size > ceilings[name]
                ):
                    raise ReleaseError(
                        f"{lane} evidence file ownership, links, mode, or size "
                        "is not canonical"
                    )
                remaining = child_before.st_size
                chunks: list[bytes] = []
                while remaining:
                    chunk = os.read(child_fd, min(1024 * 1024, remaining))
                    if not chunk:
                        raise ReleaseError(
                            f"{lane} evidence changed while being read"
                        )
                    chunks.append(chunk)
                    remaining -= len(chunk)
                if os.read(child_fd, 1):
                    raise ReleaseError(f"{lane} evidence changed while being read")
                child_after = os.fstat(child_fd)
                stable_fields = (
                    "st_dev",
                    "st_ino",
                    "st_mode",
                    "st_uid",
                    "st_gid",
                    "st_nlink",
                    "st_size",
                    "st_mtime_ns",
                    "st_ctime_ns",
                )
                if any(
                    getattr(child_before, field) != getattr(child_after, field)
                    for field in stable_fields
                ):
                    raise ReleaseError(f"{lane} evidence changed while being read")
                result[name] = b"".join(chunks)
            finally:
                os.close(child_fd)
        after = os.fstat(directory_fd)
        stable_directory_fields = (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_uid",
            "st_gid",
            "st_nlink",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        if (
            set(os.listdir(directory_fd)) != expected
            or any(
                getattr(before, field) != getattr(after, field)
                for field in stable_directory_fields
            )
        ):
            raise ReleaseError(f"{lane} evidence root changed during snapshot")
        return result
    finally:
        os.close(directory_fd)


def read_allowlist_bytes(value: bytes) -> tuple[str, ...]:
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ReleaseError("runtime allowlist is not UTF-8") from exc
    entries = tuple(
        line
        for raw in text.splitlines()
        if (line := raw.strip()) and not line.startswith("#")
    )
    if not entries:
        raise ReleaseError("runtime allowlist is empty")
    if entries != tuple(sorted(entries)):
        raise ReleaseError("runtime allowlist must be bytewise sorted")
    if len(entries) != len(set(entries)):
        raise ReleaseError("runtime allowlist contains duplicate paths")
    casefolded: set[str] = set()
    for entry in entries:
        validate_relative_path(entry)
        folded = entry.casefold()
        if folded in casefolded:
            raise ReleaseError("runtime allowlist has a case-fold path collision")
        casefolded.add(folded)
    return entries


def read_allowlist(path: Path) -> tuple[str, ...]:
    return read_allowlist_bytes(
        secure_read_bytes(path, "runtime allowlist", max_bytes=1024 * 1024)
    )


def validate_relative_path(value: str) -> PurePosixPath:
    if not value or any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ReleaseError("release path is empty or contains a control character")
    if "\\" in value:
        raise ReleaseError("release path contains a backslash")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ReleaseError("release path is not canonical and relative")
    if path.as_posix() != value:
        raise ReleaseError("release path is not canonical")
    return path


def regular_source_file(root: Path, relative: str) -> Path:
    validate_relative_path(relative)
    root = root.resolve(strict=True)
    candidate = root / relative
    current = root
    for part in PurePosixPath(relative).parts:
        current = current / part
        try:
            metadata = current.lstat()
        except OSError as exc:
            raise ReleaseError("cannot inspect an allowlisted source") from exc
        if stat.S_ISLNK(metadata.st_mode):
            raise ReleaseError("an allowlisted source must not traverse a symlink")
    try:
        metadata = candidate.stat()
    except OSError as exc:
        raise ReleaseError("cannot inspect an allowlisted source") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise ReleaseError("an allowlisted source must be a regular file")
    try:
        candidate.resolve(strict=True).relative_to(root)
    except ValueError as exc:
        raise ReleaseError("an allowlisted source escapes the repository")
    return candidate


def _git_environment() -> dict[str, str]:
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("GIT_")
    }
    environment.update(
        {
            "LC_ALL": "C",
            "TZ": "UTC",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_OPTIONAL_LOCKS": "0",
        }
    )
    return environment


def _git_command(root: Path, *arguments: str) -> list[str]:
    return [
        "git",
        "--no-optional-locks",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.hooksPath=/dev/null",
        "-C",
        os.fspath(root),
        *arguments,
    ]


def git_output(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        _git_command(root, *arguments),
        check=False,
        capture_output=True,
        text=True,
        env=_git_environment(),
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or "git command failed"
        raise ReleaseError(detail)
    return completed.stdout.strip()


def git_blob(root: Path, commit: str, relative: str) -> bytes:
    validate_relative_path(relative)
    completed = subprocess.run(
        _git_command(root, "cat-file", "blob", f"{commit}:{relative}"),
        check=False,
        capture_output=True,
        env=_git_environment(),
    )
    if completed.returncode != 0:
        raise ReleaseError("an allowlisted input is not tracked at HEAD")
    return completed.stdout


def clean_git_metadata(root: Path) -> tuple[str, int]:
    root = root.resolve(strict=True)
    inside = git_output(root, "rev-parse", "--is-inside-work-tree")
    if inside != "true":
        raise ReleaseError("source root is not a Git worktree")
    top_level = Path(git_output(root, "rev-parse", "--show-toplevel")).resolve(
        strict=True
    )
    if top_level != root:
        raise ReleaseError("source root must be the Git worktree top-level")
    if git_output(root, "for-each-ref", "--format=%(refname)", "refs/replace"):
        raise ReleaseError("Git replace refs are forbidden for release builds")
    status = git_output(root, "status", "--porcelain=v1", "--untracked-files=all")
    if status:
        raise ReleaseError("source worktree is dirty or contains untracked files")
    commit = git_output(root, "rev-parse", "HEAD^{commit}")
    if not GIT_COMMIT.fullmatch(commit):
        raise ReleaseError("Git HEAD is not a full SHA-1 object name")
    epoch_text = git_output(root, "show", "-s", "--format=%ct", commit)
    try:
        epoch = int(epoch_text)
    except ValueError as exc:
        raise ReleaseError("Git commit timestamp is not an integer") from exc
    if epoch < 1:
        raise ReleaseError("Git commit timestamp is invalid")
    return commit, epoch


def validate_native_contract(value: bytes) -> dict[str, object]:
    try:
        parsed = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleaseError("native build contract is not valid UTF-8 JSON") from exc
    if not isinstance(parsed, dict) or frozenset(parsed) != NATIVE_CONTRACT_FIELDS:
        raise ReleaseError("native build contract has missing or unknown fields")
    if value != native_canonical_json(parsed):
        raise ReleaseError("native build contract is not canonical JSON")
    if type(parsed.get("schema_version")) is not int or parsed["schema_version"] != 1:
        raise ReleaseError("native build contract schema is unsupported")
    if (
        type(parsed.get("expected_process_exit_code")) is not int
        or parsed["expected_process_exit_code"] != 1
    ):
        raise ReleaseError("native build contract process status is invalid")
    if (
        type(parsed.get("minimum_ex5_bytes")) is not int
        or parsed["minimum_ex5_bytes"] < 65536
    ):
        raise ReleaseError("native build contract EX5 size floor is invalid")
    if parsed.get("source_path") != "mql5_ea/QuantSwarmXAU.mq5":
        raise ReleaseError("native build contract source path is invalid")

    compiler = parsed.get("compiler")
    if not isinstance(compiler, dict) or frozenset(compiler) != {
        "path",
        "sha256",
        "version",
    }:
        raise ReleaseError("native build contract compiler record is invalid")
    if compiler.get("path") != "MetaEditor64.exe":
        raise ReleaseError("native build contract compiler path is invalid")
    if (
        not isinstance(compiler.get("sha256"), str)
        or HEX_256.fullmatch(compiler["sha256"]) is None
    ):
        raise ReleaseError("native build contract compiler digest is invalid")
    if (
        not isinstance(compiler.get("version"), str)
        or not compiler["version"]
        or compiler["version"] != compiler["version"].strip()
    ):
        raise ReleaseError("native build contract compiler version is invalid")

    for collection_name, absolute in (("dependencies", False), ("host_tools", True)):
        records = parsed.get(collection_name)
        if not isinstance(records, list) or not records:
            raise ReleaseError(f"native build contract {collection_name} is empty")
        paths: list[str] = []
        for record in records:
            if not isinstance(record, dict) or frozenset(record) != {"path", "sha256"}:
                raise ReleaseError(
                    f"native build contract {collection_name} record is invalid"
                )
            path = record.get("path")
            digest = record.get("sha256")
            if not isinstance(path, str):
                raise ReleaseError(
                    f"native build contract {collection_name} path is invalid"
                )
            if absolute:
                if not Path(path).is_absolute():
                    raise ReleaseError("native host tool path must be absolute")
            else:
                validate_relative_path(path)
            if not isinstance(digest, str) or HEX_256.fullmatch(digest) is None:
                raise ReleaseError(
                    f"native build contract {collection_name} digest is invalid"
                )
            paths.append(path)
        if paths != sorted(paths) or len(paths) != len(set(paths)):
            raise ReleaseError(
                f"native build contract {collection_name} must be sorted and unique"
            )

    includes = parsed.get("expected_includes")
    if (
        not isinstance(includes, list)
        or not includes
        or not all(isinstance(item, str) for item in includes)
        or includes != sorted(includes)
        or len(includes) != len(set(includes))
    ):
        raise ReleaseError("native build contract include closure is invalid")
    for include in includes:
        validate_relative_path(include)
    dependency_paths = {
        record["path"]
        for record in parsed["dependencies"]  # type: ignore[union-attr]
        if isinstance(record, dict) and str(record["path"]).startswith("MQL5/Include/")
    }
    if set(includes) != dependency_paths:
        raise ReleaseError(
            "native build contract include closure and dependencies disagree"
        )
    wine_version = parsed.get("wine_version")
    if (
        not isinstance(wine_version, str)
        or not wine_version
        or wine_version != wine_version.strip()
        or len(wine_version) > 256
        or any(ord(character) < 32 or ord(character) == 127 for character in wine_version)
    ):
        raise ReleaseError("native build contract Wine version is invalid")
    return parsed


def validate_native_compile_log(
    value: bytes,
    *,
    expected_includes: list[str],
    label: str,
) -> None:
    """Require the native harness's canonical normalized MetaEditor log."""

    try:
        normalized = validate_metaeditor_log(
            codecs.BOM_UTF8 + value,
            expected_includes,
        )
    except (NativeBuildError, UnicodeDecodeError) as exc:
        raise ReleaseError(f"{label} violates the native compiler log contract") from exc
    if normalized != value:
        raise ReleaseError(f"{label} is not the canonical normalized compiler log")


def validate_compile_evidence(
    value: bytes,
    *,
    contract_bytes: bytes,
    source_sha256: str,
    binary_sha256: str,
    compile_log_sha256: str,
    secondary_compile_log_sha256: str,
    source_commit: str,
    require_canonical: bool,
) -> tuple[dict[str, object], bytes]:
    try:
        parsed = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleaseError("native compile evidence is not valid UTF-8 JSON") from exc
    if not isinstance(parsed, dict) or frozenset(parsed) != COMPILE_FIELDS:
        raise ReleaseError("native compile evidence has missing or unknown fields")
    if type(parsed.get("schema_version")) is not int or parsed["schema_version"] != 2:
        raise ReleaseError("native compile evidence schema is unsupported")
    contract = validate_native_contract(contract_bytes)
    compiler = contract["compiler"]
    assert isinstance(compiler, dict)

    integer_contract = {
        "compile_runs": 2,
        "errors": 0,
        "gate_exit_code": 0,
        "process_exit_code": contract["expected_process_exit_code"],
        "warnings": 0,
    }
    for field, expected_value in integer_contract.items():
        item = parsed.get(field)
        if type(item) is not int or item != expected_value:
            raise ReleaseError(
                f"native compile evidence {field} must be exactly {expected_value}"
            )
    if parsed.get("binary_reproducible") is not True:
        raise ReleaseError("native compile evidence must prove binary reproducibility")
    string_contract = {
        "compiler": compiler["path"],
        "compiler_version": compiler["version"],
        "source_commit": source_commit,
        "wine_version": contract["wine_version"],
    }
    for field, expected_value in string_contract.items():
        if parsed.get(field) != expected_value:
            raise ReleaseError(f"native compile evidence {field} does not match")
    if GIT_COMMIT.fullmatch(source_commit) is None:
        raise ReleaseError("native compile evidence expected source commit is invalid")

    expected = {
        "source_sha256": source_sha256,
        "binary_sha256": binary_sha256,
        "compile_log_sha256": compile_log_sha256,
        "secondary_compile_log_sha256": secondary_compile_log_sha256,
        "compiler_sha256": compiler["sha256"],
        "contract_sha256": sha256_bytes(contract_bytes),
        "compiler_dependency_manifest_sha256": sha256_bytes(
            native_canonical_json(contract["dependencies"])
        ),
        "host_tool_manifest_sha256": sha256_bytes(
            native_canonical_json(contract["host_tools"])
        ),
        "sandbox_profile_sha256": sha256_bytes(
            native_canonical_json(NATIVE_SANDBOX_PROFILE)
        ),
    }
    for field, digest in expected.items():
        item = parsed.get(field)
        if (
            not isinstance(item, str)
            or HEX_256.fullmatch(item) is None
            or item != digest
        ):
            raise ReleaseError(f"native compile evidence {field} does not match")
    experts_manifest = parsed.get("experts_manifest_sha256")
    if (
        not isinstance(experts_manifest, str)
        or HEX_256.fullmatch(experts_manifest) is None
    ):
        raise ReleaseError(
            "native compile evidence experts_manifest_sha256 is invalid"
        )
    normalized = (
        json.dumps(parsed, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
    ).encode("utf-8")
    if require_canonical and value != normalized:
        raise ReleaseError("native compile evidence JSON is not canonical")
    return parsed, normalized


def validate_test_evidence_contract(value: bytes) -> dict[str, object]:
    try:
        parsed = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleaseError("test evidence contract is not valid UTF-8 JSON") from exc
    if not isinstance(parsed, dict) or frozenset(parsed) != TEST_CONTRACT_FIELDS:
        raise ReleaseError("test evidence contract has missing or unknown fields")
    if test_canonical_json(parsed) != value:
        raise ReleaseError("test evidence contract is not canonical JSON")
    if type(parsed.get("schema_version")) is not int or parsed["schema_version"] != 1:
        raise ReleaseError("test evidence contract schema is unsupported")
    expected_collected = parsed.get("expected_collected")
    if type(expected_collected) is not int or expected_collected < 1:
        raise ReleaseError("test evidence collection count is invalid")
    nodeids_sha256 = parsed.get("nodeids_sha256")
    if (
        not isinstance(nodeids_sha256, str)
        or HEX_256.fullmatch(nodeids_sha256) is None
    ):
        raise ReleaseError("test evidence nodeid digest is invalid")
    for field in ("allowed_skips", "required_nodeids"):
        records = parsed.get(field)
        if (
            not isinstance(records, list)
            or not all(
                isinstance(record, str)
                and record
                and "\n" not in record
                and "\r" not in record
                for record in records
            )
            or records != sorted(records)
            or len(records) != len(set(records))
        ):
            raise ReleaseError(f"test evidence contract {field} is invalid")
    if parsed.get("pytest_arguments") != [
        "-q",
        "-W",
        "error",
        "-p",
        "no:cacheprovider",
    ]:
        raise ReleaseError("test evidence pytest arguments are not frozen")
    return parsed


def test_evidence_archive_path(lane: str, filename: str) -> str:
    if lane not in TEST_EVIDENCE_LANES or filename not in TEST_EVIDENCE_FILENAMES:
        raise ReleaseError("test evidence archive path request is invalid")
    return f"test-evidence/{lane}/{filename}"


def validate_test_evidence_lane(
    lane_files: dict[str, bytes],
    *,
    lane: str,
    source_commit: str,
    signing_identity: str,
    allowed_signers: bytes,
    contract_bytes: bytes,
    lock_bytes: bytes,
) -> tuple[dict[str, object], dict[str, str]]:
    if lane not in TEST_EVIDENCE_LANES:
        raise ReleaseError("test evidence lane is unsupported")
    if frozenset(lane_files) != frozenset(TEST_EVIDENCE_FILENAMES):
        raise ReleaseError(f"{lane} test evidence file closure is not exact")
    contract = validate_test_evidence_contract(contract_bytes)
    evidence_bytes = lane_files["TEST_EVIDENCE.json"]
    signature_bytes = lane_files["TEST_EVIDENCE.json.sig"]
    verify_ssh_signature(
        evidence_bytes,
        signature_bytes,
        allowed_signers=allowed_signers,
        identity=signing_identity,
    )
    try:
        evidence = json.loads(evidence_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleaseError(f"{lane} test evidence is not valid UTF-8 JSON") from exc
    if not isinstance(evidence, dict) or frozenset(evidence) != TEST_EVIDENCE_FIELDS:
        raise ReleaseError(f"{lane} test evidence has missing or unknown fields")
    if test_canonical_json(evidence) != evidence_bytes:
        raise ReleaseError(f"{lane} TEST_EVIDENCE.json is not canonical")
    if type(evidence.get("schema_version")) is not int or evidence[
        "schema_version"
    ] != 1:
        raise ReleaseError(f"{lane} test evidence schema is unsupported")
    if evidence.get("artifact_class") != "canonical-test-evidence":
        raise ReleaseError(f"{lane} test evidence artifact class is invalid")
    if evidence.get("lane") != lane:
        raise ReleaseError(f"{lane} test evidence lane is swapped or duplicated")
    if evidence.get("source_commit") != source_commit:
        raise ReleaseError(f"{lane} test evidence source_commit does not match")
    if evidence.get("signing_identity") != signing_identity:
        raise ReleaseError(f"{lane} test evidence signing identity does not match")
    if (
        type(evidence.get("gate_exit_code")) is not int
        or evidence["gate_exit_code"] != 0
    ):
        raise ReleaseError(f"{lane} test evidence gate did not pass")
    if evidence.get("pytest_arguments") != contract["pytest_arguments"]:
        raise ReleaseError(f"{lane} test evidence pytest arguments do not match")

    environment = {
        "LC_ALL": "C.UTF-8",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "PYTHONHASHSEED": "0",
        "PYTHONNOUSERSITE": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "TZ": "UTC",
    }
    digest_contract = {
        "environment_sha256": sha256_bytes(test_canonical_json(environment)),
        "lock_sha256": sha256_bytes(lock_bytes),
        "pytest_log_sha256": sha256_bytes(lane_files["PYTEST.log"]),
        "pytest_nodeids_sha256": sha256_bytes(
            lane_files["PYTEST_NODEIDS.txt"]
        ),
        "sandbox_profile_sha256": sha256_bytes(
            test_canonical_json(TEST_SANDBOX_PROFILE)
        ),
        "test_contract_sha256": sha256_bytes(contract_bytes),
    }
    for field, expected in digest_contract.items():
        observed = evidence.get(field)
        if (
            not isinstance(observed, str)
            or HEX_256.fullmatch(observed) is None
            or observed != expected
        ):
            raise ReleaseError(f"{lane} test evidence digest does not match: {field}")
    for field in ("site_packages_sha256", "wheelhouse_manifest_sha256"):
        observed = evidence.get(field)
        if (
            not isinstance(observed, str)
            or HEX_256.fullmatch(observed) is None
        ):
            raise ReleaseError(f"{lane} test evidence digest is invalid: {field}")

    try:
        expected_lock = parse_dependency_lock(lock_bytes)
    except DependencyGateError as exc:
        raise ReleaseError(f"{lane} canonical test lock is invalid") from exc
    expected_distributions = [
        {"name": name, "version": record[0]}
        for name, record in sorted(expected_lock.items())
    ]
    if evidence.get("installed_distributions") != expected_distributions:
        raise ReleaseError(
            f"{lane} installed distribution evidence differs from the exact lock"
        )

    interpreter = evidence.get("interpreter")
    if (
        not isinstance(interpreter, dict)
        or frozenset(interpreter) != TEST_INTERPRETER_FIELDS
    ):
        raise ReleaseError(f"{lane} interpreter evidence schema is invalid")
    lane_contract = DEPENDENCY_LANES[lane]
    expected_interpreter = {
        "abi": lane_contract.abi,
        "cache_tag": (
            f"cpython-{lane_contract.python_version_short.replace('.', '')}"
        ),
        "implementation": lane_contract.implementation,
        "implementation_name": lane_contract.implementation_name,
        "machine": lane_contract.machine,
        "pip_version": lane_contract.pip_version,
        "platform": lane_contract.platform,
        "version": lane_contract.python_version,
    }
    for field, expected in expected_interpreter.items():
        if interpreter.get(field) != expected:
            raise ReleaseError(f"{lane} interpreter evidence is invalid: {field}")
    for field in ("base_executable_sha256", "python_executable_sha256"):
        observed = interpreter.get(field)
        if (
            not isinstance(observed, str)
            or HEX_256.fullmatch(observed) is None
        ):
            raise ReleaseError(f"{lane} interpreter digest is invalid: {field}")
    soabi = interpreter.get("soabi")
    expected_soabi_prefix = (
        f"cpython-{lane_contract.python_version_short.replace('.', '')}-"
    )
    if not isinstance(soabi, str) or not soabi.startswith(expected_soabi_prefix):
        raise ReleaseError(f"{lane} interpreter SOABI is invalid")

    result = evidence.get("pytest_result")
    if not isinstance(result, dict) or frozenset(result) != TEST_RESULT_FIELDS:
        raise ReleaseError(f"{lane} pytest result schema is invalid")
    if type(result.get("schema_version")) is not int or result[
        "schema_version"
    ] != 1:
        raise ReleaseError(f"{lane} pytest result schema is unsupported")
    zero_fields = (
        "deselected",
        "errors",
        "exit_status",
        "failed",
        "warnings",
    )
    for field in zero_fields:
        if type(result.get(field)) is not int or result[field] != 0:
            raise ReleaseError(f"{lane} pytest result {field} must be zero")
    collected = result.get("collected")
    passed = result.get("passed")
    if (
        type(collected) is not int
        or collected != contract["expected_collected"]
        or type(passed) is not int
        or passed < 0
    ):
        raise ReleaseError(f"{lane} pytest collected/passed counts are invalid")
    allowed_skips = contract["allowed_skips"]
    if (
        result.get("skipped") != allowed_skips
        or result.get("xfailed") != []
        or result.get("xpassed") != []
        or passed + len(allowed_skips) != collected
    ):
        raise ReleaseError(f"{lane} pytest outcomes differ from the exact contract")
    nodeids_digest = result.get("nodeids_sha256")
    if (
        not isinstance(nodeids_digest, str)
        or nodeids_digest != contract["nodeids_sha256"]
        or nodeids_digest != digest_contract["pytest_nodeids_sha256"]
    ):
        raise ReleaseError(f"{lane} pytest nodeid digest differs from the contract")

    try:
        nodeids_text = lane_files["PYTEST_NODEIDS.txt"].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ReleaseError(f"{lane} pytest nodeids are not UTF-8") from exc
    nodeids = nodeids_text.splitlines()
    canonical_nodeids = "".join(f"{nodeid}\n" for nodeid in nodeids).encode("utf-8")
    if (
        not nodeids
        or any(not nodeid or "\r" in nodeid for nodeid in nodeids)
        or len(nodeids) != len(set(nodeids))
        or canonical_nodeids != lane_files["PYTEST_NODEIDS.txt"]
        or len(nodeids) != collected
        or not set(contract["required_nodeids"]).issubset(nodeids)  # type: ignore[arg-type]
    ):
        raise ReleaseError(f"{lane} pytest nodeid closure is invalid")
    pytest_log = lane_files["PYTEST.log"]
    if not pytest_log or b"\n[stderr]\n" not in pytest_log:
        raise ReleaseError(f"{lane} pytest log is not the canonical lane output")

    release_record = {
        "evidence_sha256": sha256_bytes(evidence_bytes),
        "evidence_signature_sha256": sha256_bytes(signature_bytes),
        "pytest_log_sha256": digest_contract["pytest_log_sha256"],
        "pytest_nodeids_sha256": digest_contract["pytest_nodeids_sha256"],
    }
    return evidence, release_record


def source_version_contract(files: dict[str, bytes]) -> dict[str, object]:
    try:
        version_source = files["python_swarm/version.py"].decode("utf-8")
        mq5_source = files["mql5_ea/QuantSwarmXAU.mq5"].decode("utf-8")
        docker_source = files["Dockerfile"].decode("utf-8")
    except (KeyError, UnicodeDecodeError) as exc:
        raise ReleaseError("version-bearing source is missing or invalid UTF-8") from exc
    contract: dict[str, object] = {
        "product_name": parse_literal(version_source, "PRODUCT_NAME"),
        "product_version": parse_literal(version_source, "PRODUCT_VERSION"),
        "broker_protocol_version": parse_literal(
            version_source, "BROKER_PROTOCOL_VERSION"
        ),
        "mql5_ea_version": parse_literal(version_source, "MQL5_EA_VERSION"),
    }
    schema_match = re.search(
        r"(?m)^CONFIG_SCHEMA_VERSION\s*=\s*([0-9]+)\s*$", version_source
    )
    if schema_match is None:
        raise ReleaseError("cannot locate CONFIG_SCHEMA_VERSION")
    contract["config_schema_version"] = int(schema_match.group(1))
    if contract["product_name"] != PRODUCT_NAME:
        raise ReleaseError("source product identity is not the reviewed product")
    if contract["product_version"] != PRODUCT_VERSION:
        raise ReleaseError("source product version is not the reviewed release")
    if contract["config_schema_version"] != CONFIG_SCHEMA_VERSION:
        raise ReleaseError("source configuration schema is not the reviewed schema")
    ea = re.escape(str(contract["mql5_ea_version"]))
    protocol = re.escape(str(contract["broker_protocol_version"]))
    if re.search(rf'(?m)^#define QSX_EA_VERSION "{ea}"\s*$', mq5_source) is None:
        raise ReleaseError("MQ5 EA version disagrees with Python version contract")
    if (
        re.search(
            rf'(?m)^#define QSX_PROTOCOL_VERSION "{protocol}"\s*$',
            mq5_source,
        )
        is None
    ):
        raise ReleaseError("MQ5 protocol version disagrees with Python contract")
    if re.search(rf'(?m)^#property version\s+"{ea}"\s*$', mq5_source) is None:
        raise ReleaseError("MQ5 property version disagrees with Python version contract")
    from_lines = [
        line.strip()
        for line in docker_source.splitlines()
        if re.match(r"(?i)^\s*FROM(?:\s|$)", line)
    ]
    if from_lines != [f"FROM {DOCKER_BASE}"]:
        raise ReleaseError("Dockerfile base is not the reviewed digest-pinned image")
    contract["docker_base"] = DOCKER_BASE
    return contract


def parse_literal(source: str, name: str) -> str:
    match = re.search(
        rf"(?m)^{re.escape(name)}\s*=\s*[\"']([^\"']+)[\"']\s*$",
        source,
    )
    if match is None:
        raise ReleaseError(f"cannot locate literal version field {name}")
    return match.group(1)


def parse_checksum_manifest(value: bytes) -> dict[str, str]:
    try:
        lines = value.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise ReleaseError("checksum manifest is not UTF-8") from exc
    records: dict[str, str] = {}
    for line in lines:
        if not line:
            raise ReleaseError("checksum manifest contains an empty record")
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
        if match is None:
            raise ReleaseError("checksum manifest record is malformed")
        digest, relative = match.groups()
        validate_relative_path(relative)
        if relative in records:
            raise ReleaseError("checksum manifest contains a duplicate path")
        records[relative] = digest
    if not records:
        raise ReleaseError("checksum manifest is empty")
    return records


def checksum_manifest(files: Iterable[tuple[str, bytes]]) -> bytes:
    records = sorted((relative, sha256_bytes(value)) for relative, value in files)
    return "".join(f"{digest}  {relative}\n" for relative, digest in records).encode(
        "utf-8"
    )


def release_directories(root_name: str, files: Iterable[str]) -> tuple[str, ...]:
    validate_relative_path(root_name)
    directories = {root_name}
    for relative in files:
        validate_relative_path(relative)
        parent = PurePosixPath(root_name, relative).parent
        while parent.as_posix() != ".":
            directories.add(parent.as_posix())
            if parent.as_posix() == root_name:
                break
            parent = parent.parent
    return tuple(sorted(directories))


def _tar_info(name: str, *, epoch: int, mode: int, size: int = 0) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name)
    info.uid = 0
    info.gid = 0
    info.uname = "root"
    info.gname = "root"
    info.mtime = epoch
    info.mode = mode
    info.size = size
    return info


class _BytesReader:
    def __init__(self, value: bytes) -> None:
        self._value = value
        self._offset = 0

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self._value) - self._offset
        result = self._value[self._offset : self._offset + size]
        self._offset += len(result)
        return result


def deterministic_archive_bytes(
    *,
    root_name: str,
    files: dict[str, bytes],
    epoch: int,
) -> bytes:
    """Serialize the one canonical gzip/USTAR representation."""

    if type(epoch) is not int or not 1 <= epoch <= 0xFFFFFFFF:
        raise ReleaseError("release source epoch is outside the gzip range")
    raw = io.BytesIO()
    with gzip.GzipFile(
        filename="",
        mode="wb",
        fileobj=raw,
        compresslevel=9,
        mtime=epoch,
    ) as compressed:
        with tarfile.open(
            fileobj=compressed,
            mode="w",
            format=tarfile.USTAR_FORMAT,
        ) as archive:
            for directory in release_directories(root_name, files):
                info = _tar_info(
                    directory + "/",
                    epoch=epoch,
                    mode=0o555,
                )
                info.type = tarfile.DIRTYPE
                archive.addfile(info)
            for relative, value in sorted(files.items()):
                mode = 0o555 if relative.endswith(".sh") else 0o444
                info = _tar_info(
                    f"{root_name}/{relative}",
                    epoch=epoch,
                    mode=mode,
                    size=len(value),
                )
                archive.addfile(info, fileobj=_BytesReader(value))
    return raw.getvalue()


def write_exclusive_bytes(path: Path, value: bytes, *, mode: int = 0o600) -> None:
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(path, flags, mode)
    try:
        view = memoryview(value)
        offset = 0
        while offset < len(view):
            written = os.write(descriptor, view[offset:])
            if written <= 0:
                raise ReleaseError(f"cannot complete exclusive write: {path}")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ssh_wire_string(
    value: bytes,
    offset: int,
    *,
    label: str,
) -> tuple[bytes, int]:
    if offset < 0 or offset + 4 > len(value):
        raise ReleaseError(f"OpenSSH signature has a truncated {label}")
    length = int.from_bytes(value[offset : offset + 4], "big")
    offset += 4
    end = offset + length
    if length < 0 or end > len(value):
        raise ReleaseError(f"OpenSSH signature has an invalid {label} length")
    return value[offset:end], end


def validate_ssh_signature_bytes(value: bytes) -> None:
    """Require one canonical, fully consumed Ed25519 SSHSIG armor block."""

    begin = b"-----BEGIN SSH SIGNATURE-----"
    end = b"-----END SSH SIGNATURE-----"
    if b"\r" in value:
        raise ReleaseError("OpenSSH signature armor is not canonical")
    lines = value.split(b"\n")
    if (
        len(lines) < 4
        or lines[0] != begin
        or lines[-2] != end
        or lines[-1] != b""
    ):
        raise ReleaseError("OpenSSH signature armor is malformed or has trailing data")
    body_lines = lines[1:-2]
    if (
        not body_lines
        or any(not line or len(line) > 70 for line in body_lines)
        or any(len(line) != 70 for line in body_lines[:-1])
    ):
        raise ReleaseError("OpenSSH signature armor wrapping is not canonical")
    encoded = b"".join(body_lines)
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ReleaseError("OpenSSH signature armor is not canonical base64") from exc
    if base64.b64encode(decoded) != encoded:
        raise ReleaseError("OpenSSH signature armor is not canonical base64")
    canonical_lines = [
        encoded[offset : offset + 70]
        for offset in range(0, len(encoded), 70)
    ]
    canonical = b"\n".join((begin, *canonical_lines, end, b""))
    if value != canonical:
        raise ReleaseError("OpenSSH signature armor wrapping is not canonical")

    if not decoded.startswith(b"SSHSIG"):
        raise ReleaseError("OpenSSH signature magic is invalid")
    cursor = len(b"SSHSIG")
    if cursor + 4 > len(decoded):
        raise ReleaseError("OpenSSH signature version is truncated")
    version = int.from_bytes(decoded[cursor : cursor + 4], "big")
    cursor += 4
    if version != 1:
        raise ReleaseError("OpenSSH signature version is unsupported")
    public_key, cursor = _ssh_wire_string(
        decoded,
        cursor,
        label="public key",
    )
    namespace, cursor = _ssh_wire_string(
        decoded,
        cursor,
        label="namespace",
    )
    reserved, cursor = _ssh_wire_string(
        decoded,
        cursor,
        label="reserved field",
    )
    hash_algorithm, cursor = _ssh_wire_string(
        decoded,
        cursor,
        label="hash algorithm",
    )
    signature_blob, cursor = _ssh_wire_string(
        decoded,
        cursor,
        label="signature blob",
    )
    if cursor != len(decoded):
        raise ReleaseError("OpenSSH signature contains trailing binary data")
    if namespace != b"file" or reserved or hash_algorithm != b"sha512":
        raise ReleaseError("OpenSSH signature domain or hash contract is invalid")

    key_algorithm, key_cursor = _ssh_wire_string(
        public_key,
        0,
        label="public key algorithm",
    )
    key_value, key_cursor = _ssh_wire_string(
        public_key,
        key_cursor,
        label="public key value",
    )
    if (
        key_cursor != len(public_key)
        or key_algorithm != b"ssh-ed25519"
        or len(key_value) != 32
    ):
        raise ReleaseError("OpenSSH signature public key is not canonical Ed25519")

    signature_algorithm, signature_cursor = _ssh_wire_string(
        signature_blob,
        0,
        label="signature algorithm",
    )
    signature_value, signature_cursor = _ssh_wire_string(
        signature_blob,
        signature_cursor,
        label="signature value",
    )
    if (
        signature_cursor != len(signature_blob)
        or signature_algorithm != b"ssh-ed25519"
        or len(signature_value) != 64
    ):
        raise ReleaseError("OpenSSH signature value is not canonical Ed25519")


def trusted_ssh_keygen() -> Path:
    located = shutil.which("ssh-keygen", path="/usr/bin:/bin")
    if located is None:
        raise ReleaseError("trusted OpenSSH ssh-keygen is required")
    resolved = Path(located).resolve(strict=True)
    metadata = resolved.stat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        raise ReleaseError("OpenSSH ssh-keygen is not root-controlled")
    return resolved


def verify_ssh_signature(
    value: bytes,
    signature: bytes,
    *,
    allowed_signers: bytes,
    identity: str,
) -> None:
    if re.fullmatch(r"[A-Za-z0-9_.@-]{3,128}", identity) is None:
        raise ReleaseError("OpenSSH signing identity is invalid")
    validate_ssh_signature_bytes(signature)
    executable = trusted_ssh_keygen()
    with tempfile.TemporaryDirectory(prefix="qsx-signature-verify-") as raw:
        private = Path(raw)
        signers_path = private / "allowed_signers"
        signature_path = private / "signature"
        write_exclusive_bytes(signers_path, allowed_signers)
        write_exclusive_bytes(signature_path, signature)
        completed = subprocess.run(
            [
                os.fspath(executable),
                "-Y",
                "verify",
                "-f",
                os.fspath(signers_path),
                "-I",
                identity,
                "-n",
                "file",
                "-s",
                os.fspath(signature_path),
            ],
            input=value,
            check=False,
            capture_output=True,
            env={
                "LC_ALL": "C",
                "PATH": "/usr/bin:/bin",
                "TZ": "UTC",
            },
        )
    if completed.returncode != 0:
        raise ReleaseError("OpenSSH signature verification failed")
