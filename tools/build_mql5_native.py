#!/usr/bin/env python3
"""Produce trusted, two-run native MetaEditor evidence in isolated Wine prefixes.

This harness never launches MetaEditor in the operator's Wine prefix. It reads
the MQ5 source from a clean Git commit, verifies every frozen compiler input,
creates two independent networkless bubblewrap sandboxes, and accepts output
only when both fresh EX5 files are byte-identical and both logs report exactly
zero errors and zero warnings.
"""

from __future__ import annotations

import argparse
import codecs
import errno
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable, Mapping


CONTRACT_FIELDS = frozenset(
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
EVIDENCE_FIELDS = frozenset(
    {
        "binary_reproducible",
        "binary_sha256",
        "compile_log_sha256",
        "compile_runs",
        "compiler",
        "compiler_dependency_manifest_sha256",
        "compiler_sha256",
        "compiler_version",
        "contract_sha256",
        "errors",
        "experts_manifest_sha256",
        "gate_exit_code",
        "host_tool_manifest_sha256",
        "process_exit_code",
        "sandbox_profile_sha256",
        "schema_version",
        "secondary_compile_log_sha256",
        "source_commit",
        "source_sha256",
        "warnings",
        "wine_version",
    }
)
BANNED_EX5_SHA256 = frozenset(
    {
        "e8d9f4804aa82248eb23ed1bfc28365a05441536219208dbb61ec2f64e943df6",
    }
)
HEX_256 = re.compile(r"^[0-9a-f]{64}$")
GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")
VERSION_TEXT = re.compile(r"^[0-9]+(?:\.[0-9]+){3}$")
RESULT_LINE = re.compile(r"^Result: 0 errors, 0 warnings, [^\r\n]+$")
COMPILE_LINE = (
    r"C:\QSXBuild\QuantSwarmXAU.mq5 : information: compiling "
    r"C:\QSXBuild\QuantSwarmXAU.mq5"
)
INCLUDE_LINE = re.compile(
    r"^[^\r\n]+ : information: including "
    r"(C:\\Program Files\\MetaTrader 5\\MQL5\\Include\\[^\r\n]+)$"
)
CANONICAL_CONTRACT_PATH = PurePosixPath(
    "release/native-mql5-build-contract.json"
)
CANONICAL_SOURCE_PATH = "mql5_ea/QuantSwarmXAU.mq5"
COMPILER_PATH = "MetaEditor64.exe"
REQUIRED_HOST_TOOL_PATHS = (
    "/usr/bin/Xvfb",
    "/usr/bin/bwrap",
    "/usr/bin/timeout",
    "/usr/bin/wine64",
    "/usr/bin/wineboot",
    "/usr/bin/xvfb-run",
    "/usr/lib/x86_64-linux-gnu/wine/wine",
    "/usr/lib/x86_64-linux-gnu/wine/wineserver",
)
SANDBOX_PREFIX = "/qsx-prefix"
MAX_CONTRACT_BYTES = 1024 * 1024
MAX_SOURCE_BYTES = 16 * 1024 * 1024
MAX_COMPILER_INPUT_BYTES = 256 * 1024 * 1024
MAX_HOST_TOOL_BYTES = 512 * 1024 * 1024
MAX_EX5_BYTES = 256 * 1024 * 1024
MAX_LOG_BYTES = 64 * 1024 * 1024
MAX_CONSOLE_BYTES = 16 * 1024 * 1024
OUTER_SANDBOX_TIMEOUT_SECONDS = 300
SANDBOX_PROFILE = {
    "bubblewrap": "unshare-all; nested-userns-disabled; all-capabilities-dropped",
    "filesystem": (
        "minimal-ro-runtime; fixed-/qsx-prefix-write; "
        "compiler-subtree-read-only; no-host-var-tmp"
    ),
    "network": "isolated-network-namespace",
    "prefix": "unique-physical-copy; sibling-prefixes-not-visible",
    "source_drive": "C-only; no-Z-drive",
    "version": 2,
}


class NativeBuildError(RuntimeError):
    """A native compiler input, sandbox, or output violated the frozen gate."""


@dataclass(frozen=True, slots=True)
class HostToolSnapshot:
    """One root-controlled executable path and its immutable verified target."""

    invoked: Path
    resolved: Path
    sha256: str


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_json(value: object) -> bytes:
    return (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
    ).encode("utf-8")


def _git_env() -> dict[str, str]:
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("GIT_")
    }
    environment.update(
        {
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "LC_ALL": "C",
            "TZ": "UTC",
        }
    )
    return environment


def _git(root: Path, *arguments: str, binary: bool = False) -> str | bytes:
    completed = subprocess.run(
        [
            "git",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.untrackedCache=false",
            "-C",
            os.fspath(root),
            *arguments,
        ],
        check=False,
        capture_output=True,
        text=not binary,
        env=_git_env(),
    )
    if completed.returncode != 0:
        detail = completed.stderr
        if isinstance(detail, bytes):
            detail = detail.decode("utf-8", errors="replace")
        raise NativeBuildError(detail.strip() or "Git command failed")
    return completed.stdout if binary else completed.stdout.strip()


def _clean_commit(root: Path) -> str:
    root = root.resolve(strict=True)
    top = Path(str(_git(root, "rev-parse", "--show-toplevel"))).resolve(strict=True)
    if top != root:
        raise NativeBuildError("source root must be the Git worktree top level")
    if _git(root, "status", "--porcelain=v1", "--untracked-files=all"):
        raise NativeBuildError("source worktree is dirty or contains untracked files")
    commit = str(_git(root, "rev-parse", "HEAD^{commit}"))
    if GIT_COMMIT.fullmatch(commit) is None:
        raise NativeBuildError("Git HEAD is not a full commit object name")
    return commit


def _relative_path(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        not value
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
        or "\\" in value
        or path.is_absolute()
        or path.as_posix() != value
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise NativeBuildError(f"contract path is not canonical: {value!r}")
    return path


def _read_descriptor(
    descriptor: int,
    label: str,
    *,
    max_bytes: int,
) -> tuple[bytes, os.stat_result]:
    """Read one stable regular-file snapshot from an already-safe descriptor."""

    before = os.fstat(descriptor)
    if not stat.S_ISREG(before.st_mode):
        raise NativeBuildError(f"{label} must be a regular file")
    if before.st_size < 0 or before.st_size > max_bytes:
        raise NativeBuildError(f"{label} exceeds its size ceiling")
    chunks: list[bytes] = []
    remaining = before.st_size
    while remaining:
        chunk = os.read(descriptor, min(1024 * 1024, remaining))
        if not chunk:
            raise NativeBuildError(f"{label} changed while it was being read")
        chunks.append(chunk)
        remaining -= len(chunk)
    if os.read(descriptor, 1):
        raise NativeBuildError(f"{label} changed while it was being read")
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
        raise NativeBuildError(f"{label} changed while it was being read")
    return b"".join(chunks), after


def _read_regular_nofollow(
    path: Path,
    label: str,
    *,
    max_bytes: int = MAX_COMPILER_INPUT_BYTES,
) -> bytes:
    """Snapshot a stable regular file without following its final component."""

    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise NativeBuildError(f"{label} cannot be opened safely: {path}") from exc
    try:
        value, _ = _read_descriptor(descriptor, label, max_bytes=max_bytes)
        return value
    finally:
        os.close(descriptor)


def _open_directory_nofollow(path: Path, label: str) -> int:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise NativeBuildError(f"{label} is not a safe directory: {path}") from exc
    metadata = os.fstat(descriptor)
    if not stat.S_ISDIR(metadata.st_mode):
        os.close(descriptor)
        raise NativeBuildError(f"{label} is not a directory: {path}")
    return descriptor


def _open_child_directory(
    parent_fd: int,
    name: str,
    label: str,
    *,
    create: bool = False,
    mode: int = 0o700,
    require_owned: bool = False,
) -> int:
    if (
        not name
        or name in {".", ".."}
        or "/" in name
        or "\x00" in name
    ):
        raise NativeBuildError(f"{label} contains an unsafe path component")
    if create:
        try:
            os.mkdir(name, mode, dir_fd=parent_fd)
        except FileExistsError:
            pass
        except OSError as exc:
            raise NativeBuildError(f"{label} cannot be created safely") from exc
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(name, flags, dir_fd=parent_fd)
    except OSError as exc:
        raise NativeBuildError(f"{label} is not a safe directory") from exc
    metadata = os.fstat(descriptor)
    if not stat.S_ISDIR(metadata.st_mode):
        os.close(descriptor)
        raise NativeBuildError(f"{label} is not a directory")
    if require_owned and (
        metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        os.close(descriptor)
        raise NativeBuildError(f"{label} is not privately controlled")
    return descriptor


def _walk_directory_fd(
    root_fd: int,
    parts: Iterable[str],
    label: str,
    *,
    create: bool = False,
    mode: int = 0o700,
    require_owned: bool = False,
) -> int:
    current = os.dup(root_fd)
    try:
        for part in parts:
            child = _open_child_directory(
                current,
                part,
                label,
                create=create,
                mode=mode,
                require_owned=require_owned,
            )
            os.close(current)
            current = child
        return current
    except BaseException:
        os.close(current)
        raise


def _read_regular_at(
    directory_fd: int,
    name: str,
    label: str,
    *,
    max_bytes: int,
) -> tuple[bytes, os.stat_result]:
    if not name or name in {".", ".."} or "/" in name or "\x00" in name:
        raise NativeBuildError(f"{label} has an unsafe filename")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(name, flags, dir_fd=directory_fd)
    except OSError as exc:
        raise NativeBuildError(f"{label} cannot be opened safely") from exc
    try:
        return _read_descriptor(descriptor, label, max_bytes=max_bytes)
    finally:
        os.close(descriptor)


def _read_beneath(
    root: Path,
    relative: PurePosixPath,
    label: str,
    *,
    max_bytes: int,
) -> bytes:
    root_fd = _open_directory_nofollow(root, f"{label} root")
    directory_fd = -1
    try:
        directory_fd = _walk_directory_fd(
            root_fd,
            relative.parts[:-1],
            label,
        )
        value, _ = _read_regular_at(
            directory_fd,
            relative.name,
            label,
            max_bytes=max_bytes,
        )
        return value
    finally:
        if directory_fd >= 0:
            os.close(directory_fd)
        os.close(root_fd)


def _write_new_regular_at(
    directory_fd: int,
    name: str,
    value: bytes,
    label: str,
    *,
    mode: int,
) -> os.stat_result:
    if not name or name in {".", ".."} or "/" in name or "\x00" in name:
        raise NativeBuildError(f"{label} has an unsafe filename")
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(name, flags, mode, dir_fd=directory_fd)
    except OSError as exc:
        raise NativeBuildError(f"{label} cannot be created safely") from exc
    try:
        view = memoryview(value)
        written = 0
        while written < len(view):
            count = os.write(descriptor, view[written:])
            if count <= 0:
                raise NativeBuildError(f"{label} could not be written completely")
            written += count
        os.fchmod(descriptor, mode)
        os.fsync(descriptor)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) != mode
            or metadata.st_size != len(value)
        ):
            raise NativeBuildError(f"{label} metadata is unsafe")
        return metadata
    finally:
        os.close(descriptor)


def _entry_exists_at(directory_fd: int, name: str) -> bool:
    try:
        os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    return True


def _parse_contract(raw: bytes) -> dict[str, object]:
    try:
        contract = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NativeBuildError("native build contract is not valid UTF-8 JSON") from exc
    if not isinstance(contract, dict) or frozenset(contract) != CONTRACT_FIELDS:
        raise NativeBuildError("native build contract has missing or unknown fields")
    if type(contract.get("schema_version")) is not int or contract["schema_version"] != 1:
        raise NativeBuildError("native build contract schema is unsupported")
    if raw != canonical_json(contract):
        raise NativeBuildError("native build contract is not canonical JSON")
    if (
        type(contract.get("expected_process_exit_code")) is not int
        or contract["expected_process_exit_code"] != 1
    ):
        raise NativeBuildError("pinned MetaEditor process status must be exactly one")
    if (
        type(contract.get("minimum_ex5_bytes")) is not int
        or contract["minimum_ex5_bytes"] < 65536
        or contract["minimum_ex5_bytes"] > MAX_EX5_BYTES
    ):
        raise NativeBuildError("native EX5 size floor is invalid")
    if contract.get("source_path") != CANONICAL_SOURCE_PATH:
        raise NativeBuildError("native build contract source path is invalid")

    compiler = contract.get("compiler")
    if not isinstance(compiler, dict) or frozenset(compiler) != {
        "path",
        "sha256",
        "version",
    }:
        raise NativeBuildError("native build contract compiler record is invalid")
    if compiler.get("path") != COMPILER_PATH:
        raise NativeBuildError("native build contract compiler path is invalid")
    compiler_digest = compiler.get("sha256")
    compiler_version = compiler.get("version")
    if (
        not isinstance(compiler_digest, str)
        or HEX_256.fullmatch(compiler_digest) is None
    ):
        raise NativeBuildError("native build contract compiler digest is invalid")
    if (
        not isinstance(compiler_version, str)
        or VERSION_TEXT.fullmatch(compiler_version) is None
    ):
        raise NativeBuildError("native build contract compiler version is invalid")

    for collection_name, absolute in (("dependencies", False), ("host_tools", True)):
        records = contract.get(collection_name)
        if not isinstance(records, list) or not records:
            raise NativeBuildError(f"native build contract {collection_name} is empty")
        paths: list[str] = []
        for record in records:
            if not isinstance(record, dict) or frozenset(record) != {"path", "sha256"}:
                raise NativeBuildError(
                    f"native build contract {collection_name} record is invalid"
                )
            name = record.get("path")
            digest = record.get("sha256")
            if not isinstance(name, str):
                raise NativeBuildError(
                    f"native build contract {collection_name} path is invalid"
                )
            if absolute:
                path = PurePosixPath(name)
                if (
                    not path.is_absolute()
                    or path.as_posix() != name
                    or any(part in {"", ".", ".."} for part in path.parts)
                    or any(
                        ord(character) < 32 or ord(character) == 127
                        for character in name
                    )
                ):
                    raise NativeBuildError("host tool path must be absolute")
            else:
                _relative_path(name)
            if not isinstance(digest, str) or HEX_256.fullmatch(digest) is None:
                raise NativeBuildError("native dependency digest is invalid")
            paths.append(name)
        if paths != sorted(paths) or len(paths) != len(set(paths)):
            raise NativeBuildError(
                f"native build contract {collection_name} must be sorted and unique"
            )
    expected = contract.get("expected_includes")
    if (
        not isinstance(expected, list)
        or not expected
        or not all(isinstance(name, str) for name in expected)
        or expected != sorted(expected)
        or len(expected) != len(set(expected))
    ):
        raise NativeBuildError("expected include closure must be sorted and unique")
    for name in expected:
        _relative_path(name)
        if not name.startswith("MQL5/Include/"):
            raise NativeBuildError("expected include is outside MQL5/Include")

    dependency_paths = [record["path"] for record in contract["dependencies"]]  # type: ignore[index]
    if dependency_paths != sorted(("Config/terminal.lic", *expected)):
        raise NativeBuildError(
            "native dependency set must be terminal.lic plus the exact include closure"
        )
    host_tool_paths = tuple(
        record["path"] for record in contract["host_tools"]  # type: ignore[index]
    )
    if host_tool_paths != REQUIRED_HOST_TOOL_PATHS:
        raise NativeBuildError("native host-tool set is not the exact reviewed set")
    wine_version = contract.get("wine_version")
    if (
        not isinstance(wine_version, str)
        or not wine_version
        or wine_version != wine_version.strip()
        or len(wine_version) > 256
        or any(ord(character) < 32 or ord(character) == 127 for character in wine_version)
    ):
        raise NativeBuildError("native build contract Wine version is invalid")
    return contract


def _load_contract(path: Path) -> tuple[dict[str, object], bytes]:
    path = path.expanduser().absolute()
    raw = _read_regular_nofollow(
        path,
        "native build contract",
        max_bytes=MAX_CONTRACT_BYTES,
    )
    contract = _parse_contract(raw)
    return contract, raw


def _load_committed_contract(
    source_root: Path, contract_path: Path, commit: str
) -> tuple[dict[str, object], bytes]:
    source_root = source_root.resolve(strict=True)
    canonical_contract = source_root / CANONICAL_CONTRACT_PATH.as_posix()
    provided_contract = contract_path.expanduser().absolute()
    if provided_contract != canonical_contract:
        raise NativeBuildError(
            "native build contract must be the canonical file in the source tree"
        )
    contract_raw = _read_beneath(
        source_root,
        CANONICAL_CONTRACT_PATH,
        "native build contract",
        max_bytes=MAX_CONTRACT_BYTES,
    )
    contract = _parse_contract(contract_raw)
    committed_contract = _git(
        source_root,
        "cat-file",
        "blob",
        f"{commit}:{CANONICAL_CONTRACT_PATH.as_posix()}",
        binary=True,
    )
    assert isinstance(committed_contract, bytes)
    if contract_raw != committed_contract:
        raise NativeBuildError("native build contract differs from Git HEAD")
    return contract, contract_raw


def _verified_inputs(
    contract: dict[str, object], mt5_install: Path
) -> tuple[dict[str, bytes], dict[str, HostToolSnapshot]]:
    mt5_install = mt5_install.expanduser().resolve(strict=True)
    mt5_fd = _open_directory_nofollow(
        mt5_install, "MT5 compiler input directory"
    )
    os.close(mt5_fd)
    inputs: dict[str, bytes] = {}
    compiler = contract["compiler"]
    assert isinstance(compiler, dict)
    records = [compiler, *contract["dependencies"]]  # type: ignore[list-item]
    for record in records:
        assert isinstance(record, dict)
        relative = str(record["path"])
        snapshot = _read_beneath(
            mt5_install,
            _relative_path(relative),
            f"compiler input {relative}",
            max_bytes=MAX_COMPILER_INPUT_BYTES,
        )
        if sha256_bytes(snapshot) != record["sha256"]:
            raise NativeBuildError(f"compiler input hash drifted: {relative}")
        inputs[relative] = snapshot

    tools: dict[str, HostToolSnapshot] = {}
    for record in contract["host_tools"]:  # type: ignore[union-attr]
        assert isinstance(record, dict)
        invoked = Path(str(record["path"]))
        try:
            invoked_metadata = invoked.lstat()
        except OSError as exc:
            raise NativeBuildError(f"host tool is missing: {invoked}") from exc
        if (
            invoked_metadata.st_uid != 0
            or stat.S_IMODE(invoked_metadata.st_mode) & 0o022
        ):
            raise NativeBuildError(f"host tool path is not root-controlled: {invoked}")
        resolved = invoked.resolve(strict=True)
        metadata = resolved.stat()
        if not stat.S_ISREG(metadata.st_mode):
            raise NativeBuildError(f"host tool is not regular: {invoked}")
        if metadata.st_uid != 0 or stat.S_IMODE(metadata.st_mode) & 0o022:
            raise NativeBuildError(f"host tool is not root-controlled: {invoked}")
        value = _read_regular_nofollow(
            resolved,
            f"host tool {invoked}",
            max_bytes=MAX_HOST_TOOL_BYTES,
        )
        if sha256_bytes(value) != record["sha256"]:
            raise NativeBuildError(f"host tool hash drifted: {invoked}")
        tools[os.fspath(invoked)] = HostToolSnapshot(
            invoked=invoked,
            resolved=resolved,
            sha256=str(record["sha256"]),
        )
    if tuple(sorted(tools)) != REQUIRED_HOST_TOOL_PATHS:
        raise NativeBuildError("verified host-tool set is incomplete or excessive")
    return inputs, tools


def _verify_host_tools_unchanged(
    contract: dict[str, object], tools: Mapping[str, HostToolSnapshot]
) -> None:
    for record in contract["host_tools"]:  # type: ignore[union-attr]
        assert isinstance(record, dict)
        invoked = str(record["path"])
        snapshot = tools.get(invoked)
        if snapshot is None:
            raise NativeBuildError(f"host tool audit is incomplete: {invoked}")
        try:
            invoked_metadata = snapshot.invoked.lstat()
            current_resolved = snapshot.invoked.resolve(strict=True)
            metadata = current_resolved.stat()
        except OSError as exc:
            raise NativeBuildError(
                f"host tool changed during native build: {invoked}"
            ) from exc
        if (
            current_resolved != snapshot.resolved
            or invoked_metadata.st_uid != 0
            or stat.S_IMODE(invoked_metadata.st_mode) & 0o022
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != 0
            or stat.S_IMODE(metadata.st_mode) & 0o022
            or sha256_bytes(
                _read_regular_nofollow(
                    current_resolved,
                    f"host tool {invoked}",
                    max_bytes=MAX_HOST_TOOL_BYTES,
                )
            )
            != snapshot.sha256
            or snapshot.sha256 != record["sha256"]
        ):
            raise NativeBuildError(f"host tool changed during native build: {invoked}")


def _verify_compiler_inputs_unchanged(
    contract: dict[str, object],
    mt5_install: Path,
    inputs: Mapping[str, bytes],
) -> None:
    compiler = contract["compiler"]
    assert isinstance(compiler, dict)
    records = [compiler, *contract["dependencies"]]  # type: ignore[list-item]
    if set(inputs) != {str(record["path"]) for record in records}:
        raise NativeBuildError("compiler input snapshot set is incomplete")
    for record in records:
        assert isinstance(record, dict)
        relative = str(record["path"])
        current = _read_beneath(
            mt5_install,
            _relative_path(relative),
            f"compiler input {relative}",
            max_bytes=MAX_COMPILER_INPUT_BYTES,
        )
        if current != inputs[relative] or sha256_bytes(current) != record["sha256"]:
            raise NativeBuildError(
                f"compiler input changed during native build: {relative}"
            )


def _required_tool(
    tools: Mapping[str, HostToolSnapshot],
    path: str,
) -> str:
    snapshot = tools.get(path)
    if snapshot is None or os.fspath(snapshot.invoked) != path:
        raise NativeBuildError(f"required host tool was not verified: {path}")
    return path


def _tree_manifest(root: Path) -> tuple[str, int]:
    records: list[tuple[str, str, int, str]] = []
    if root.exists() or root.is_symlink():
        for candidate in (root, *sorted(root.rglob("*"))):
            relative = "." if candidate == root else candidate.relative_to(root).as_posix()
            metadata = candidate.lstat()
            mode = stat.S_IMODE(metadata.st_mode)
            if stat.S_ISDIR(metadata.st_mode):
                records.append((relative, "dir", mode, ""))
            elif stat.S_ISREG(metadata.st_mode):
                records.append((relative, "file", mode, sha256_file(candidate)))
            elif stat.S_ISLNK(metadata.st_mode):
                records.append(
                    (
                        relative,
                        "symlink",
                        mode,
                        sha256_bytes(os.fsencode(os.readlink(candidate))),
                    )
                )
            else:
                records.append((relative, "special", mode, str(metadata.st_rdev)))
    encoded = json.dumps(records, separators=(",", ":"), ensure_ascii=True).encode()
    return sha256_bytes(encoded), len(records)


def _terminal_processes() -> list[dict[str, int]]:
    processes: list[dict[str, int]] = []
    for proc in sorted(Path("/proc").glob("[0-9]*"), key=lambda item: int(item.name)):
        try:
            command = (proc / "cmdline").read_bytes().replace(b"\0", b" ")
            if b"terminal64.exe" not in command.lower():
                continue
            process_stat = (proc / "stat").read_text(encoding="utf-8")
            close = process_stat.rfind(") ")
            if close < 0:
                continue
            # Fields after the command name begin with field 3 (state).
            fields_after_name = process_stat[close + 2 :].split()
            processes.append(
                {
                    "pid": int(proc.name),
                    "start_ticks": int(fields_after_name[19]),
                }
            )
        except (FileNotFoundError, PermissionError, IndexError, ValueError):
            continue
    return processes


def _sandbox(
    prefix: Path,
    command: Iterable[str],
    *,
    host_tools: Mapping[str, HostToolSnapshot],
    freeze_compiler_inputs: bool = False,
) -> list[str]:
    """Build one minimal namespace with only this run mounted at a fixed path."""

    prefix = prefix.absolute()
    command_values = [str(value) for value in command]
    if not command_values or any("\x00" in value for value in command_values):
        raise NativeBuildError("sandbox command is empty or malformed")
    arguments = [
        _required_tool(host_tools, "/usr/bin/bwrap"),
        "--die-with-parent",
        "--new-session",
        "--unshare-all",
        "--disable-userns",
        "--cap-drop",
        "ALL",
        "--hostname",
        "qsx-native-build",
        "--ro-bind",
        "/usr",
        "/usr",
        "--symlink",
        "usr/bin",
        "/bin",
        "--symlink",
        "usr/sbin",
        "/sbin",
        "--symlink",
        "usr/lib",
        "/lib",
        "--symlink",
        "usr/lib64",
        "/lib64",
        "--dir",
        "/etc",
        "--ro-bind",
        "/etc/alternatives",
        "/etc/alternatives",
        "--ro-bind",
        "/etc/fonts",
        "/etc/fonts",
        "--ro-bind",
        "/etc/passwd",
        "/etc/passwd",
        "--ro-bind",
        "/etc/group",
        "/etc/group",
        "--ro-bind",
        "/etc/nsswitch.conf",
        "/etc/nsswitch.conf",
        "--ro-bind",
        "/etc/ld.so.cache",
        "/etc/ld.so.cache",
        "--ro-bind-try",
        "/etc/ld.so.conf",
        "/etc/ld.so.conf",
        "--ro-bind-try",
        "/etc/ld.so.conf.d",
        "/etc/ld.so.conf.d",
        "--ro-bind-try",
        "/etc/X11",
        "/etc/X11",
        "--dir",
        "/var",
        "--dir",
        "/var/cache",
        "--ro-bind-try",
        "/var/cache/fontconfig",
        "/var/cache/fontconfig",
        "--dev",
        "/dev",
        "--proc",
        "/proc",
        "--tmpfs",
        "/tmp",
        "--tmpfs",
        "/run",
        "--dir",
        SANDBOX_PREFIX,
        "--bind",
        os.fspath(prefix),
        SANDBOX_PREFIX,
    ]
    if freeze_compiler_inputs:
        compiler_host = (
            prefix / "drive_c" / "Program Files" / "MetaTrader 5"
        )
        compiler_sandbox = (
            SANDBOX_PREFIX + "/drive_c/Program Files/MetaTrader 5"
        )
        arguments.extend(
            [
                "--ro-bind",
                os.fspath(compiler_host),
                compiler_sandbox,
            ]
        )
    arguments.extend(
        [
        "--clearenv",
        "--setenv",
        "HOME",
        SANDBOX_PREFIX + "/.home",
        "--setenv",
        "USER",
        "qsxbuilder",
        "--setenv",
        "LOGNAME",
        "qsxbuilder",
        "--setenv",
        "WINEPREFIX",
        SANDBOX_PREFIX,
        "--setenv",
        "XDG_RUNTIME_DIR",
        SANDBOX_PREFIX + "/.xdg",
        "--setenv",
        "WINEDEBUG",
        "-all",
        "--setenv",
        "WINEDLLOVERRIDES",
        "mscoree,mshtml=",
        "--setenv",
        "PATH",
        "/usr/bin:/bin",
        "--setenv",
        "LC_ALL",
        "C",
        "--setenv",
        "TZ",
        "UTC",
        "--chdir",
        SANDBOX_PREFIX,
        "--",
        *command_values,
        ]
    )
    return arguments


def _run_sandbox(
    prefix: Path,
    command: Iterable[str],
    *,
    host_tools: Mapping[str, HostToolSnapshot],
    freeze_compiler_inputs: bool = False,
    expected_status: int | None = None,
    timeout_seconds: int = OUTER_SANDBOX_TIMEOUT_SECONDS,
) -> subprocess.CompletedProcess[bytes]:
    try:
        completed = subprocess.run(
            _sandbox(
                prefix,
                command,
                host_tools=host_tools,
                freeze_compiler_inputs=freeze_compiler_inputs,
            ),
            check=False,
            capture_output=True,
            stdin=subprocess.DEVNULL,
            timeout=timeout_seconds,
            env={"PATH": "/usr/bin:/bin", "LC_ALL": "C", "TZ": "UTC"},
        )
    except subprocess.TimeoutExpired as exc:
        raise NativeBuildError("outer sandbox timeout expired") from exc
    if expected_status is not None and completed.returncode != expected_status:
        raise NativeBuildError(
            f"sandbox command returned {completed.returncode}, expected {expected_status}"
        )
    return completed


def _prepare_template(
    template: Path,
    *,
    inputs: dict[str, bytes],
    source: bytes,
    expected_wine_version: str,
    host_tools: Mapping[str, HostToolSnapshot],
) -> None:
    try:
        template.mkdir(mode=0o700)
    except OSError as exc:
        raise NativeBuildError("isolated Wine template cannot be created") from exc
    template_fd = _open_directory_nofollow(template, "isolated Wine template")
    try:
        template_metadata = os.fstat(template_fd)
        if (
            template_metadata.st_uid != os.getuid()
            or stat.S_IMODE(template_metadata.st_mode) != 0o700
        ):
            raise NativeBuildError("isolated Wine template is not private")
        for name in (".home", ".xdg"):
            child = _open_child_directory(
                template_fd,
                name,
                f"isolated Wine {name}",
                create=True,
                mode=0o700,
                require_owned=True,
            )
            os.close(child)
    finally:
        os.close(template_fd)

    boot = [
        _required_tool(host_tools, "/usr/bin/timeout"),
        "-k",
        "10s",
        "120s",
        _required_tool(host_tools, "/usr/bin/xvfb-run"),
        "-a",
        "-s",
        "-screen 0 1024x768x24",
        _required_tool(host_tools, "/usr/bin/wineboot"),
        "-u",
    ]
    _run_sandbox(
        template,
        boot,
        host_tools=host_tools,
        expected_status=0,
    )
    version = _run_sandbox(
        template,
        [_required_tool(host_tools, "/usr/bin/wine64"), "--version"],
        host_tools=host_tools,
    )
    if (
        version.returncode != 0
        or version.stderr
        or version.stdout.decode("utf-8", errors="strict").strip()
        != expected_wine_version
    ):
        raise NativeBuildError("Wine version differs from the native build contract")

    template_fd = _open_directory_nofollow(template, "isolated Wine template")
    drive_fd = -1
    install_fd = -1
    build_fd = -1
    try:
        for name in (".home", ".xdg"):
            child = _open_child_directory(
                template_fd,
                name,
                f"isolated Wine {name}",
                require_owned=True,
            )
            os.close(child)
        _sanitize_dosdevices(template_fd)
        drive_fd = _open_child_directory(
            template_fd,
            "drive_c",
            "isolated Wine drive_c",
            require_owned=True,
        )
        install_fd = _walk_directory_fd(
            drive_fd,
            ("Program Files", "MetaTrader 5"),
            "isolated MetaTrader installation",
            create=True,
            mode=0o755,
            require_owned=True,
        )
        for relative, snapshot in sorted(inputs.items()):
            path = _relative_path(relative)
            parent_fd = _walk_directory_fd(
                install_fd,
                path.parts[:-1],
                f"compiler input destination {relative}",
                create=True,
                mode=0o755,
                require_owned=True,
            )
            try:
                _write_new_regular_at(
                    parent_fd,
                    path.name,
                    snapshot,
                    f"compiler input destination {relative}",
                    mode=0o444,
                )
                copied, _ = _read_regular_at(
                    parent_fd,
                    path.name,
                    f"compiler input destination {relative}",
                    max_bytes=MAX_COMPILER_INPUT_BYTES,
                )
                if copied != snapshot:
                    raise NativeBuildError(
                        f"copied compiler input differs: {relative}"
                    )
            finally:
                os.close(parent_fd)
        build_fd = _open_child_directory(
            drive_fd,
            "QSXBuild",
            "isolated QSX build directory",
            create=True,
            mode=0o700,
            require_owned=True,
        )
        _write_new_regular_at(
            build_fd,
            "QuantSwarmXAU.mq5",
            source,
            "isolated MQ5 source",
            mode=0o600,
        )
    finally:
        if build_fd >= 0:
            os.close(build_fd)
        if install_fd >= 0:
            os.close(install_fd)
        if drive_fd >= 0:
            os.close(drive_fd)
        os.close(template_fd)
    _verify_prefix_inputs(template, inputs, sha256_bytes(source))


def _sanitize_dosdevices(prefix_fd: int) -> None:
    dosdevices_fd = _open_child_directory(
        prefix_fd,
        "dosdevices",
        "isolated Wine dosdevices",
        require_owned=True,
    )
    try:
        names = sorted(os.listdir(dosdevices_fd))
        if "c:" not in names:
            raise NativeBuildError("isolated Wine prefix lacks its C drive")
        for name in names:
            metadata = os.stat(name, dir_fd=dosdevices_fd, follow_symlinks=False)
            if not stat.S_ISLNK(metadata.st_mode):
                raise NativeBuildError("Wine drive mapping is not a symlink")
            target = os.readlink(name, dir_fd=dosdevices_fd)
            if name == "c:":
                if target != "../drive_c":
                    raise NativeBuildError(
                        "isolated Wine prefix has an unexpected C drive mapping"
                    )
            else:
                os.unlink(name, dir_fd=dosdevices_fd)
        if sorted(os.listdir(dosdevices_fd)) != ["c:"]:
            raise NativeBuildError("isolated Wine drive mapping cleanup was incomplete")
    finally:
        os.close(dosdevices_fd)


def _verify_dosdevices(prefix: Path) -> None:
    prefix_fd = _open_directory_nofollow(prefix, "isolated Wine prefix")
    dosdevices_fd = -1
    try:
        dosdevices_fd = _open_child_directory(
            prefix_fd,
            "dosdevices",
            "isolated Wine dosdevices",
            require_owned=True,
        )
        if sorted(os.listdir(dosdevices_fd)) != ["c:"]:
            raise NativeBuildError("isolated Wine prefix exposed a non-C drive")
        metadata = os.stat("c:", dir_fd=dosdevices_fd, follow_symlinks=False)
        if (
            not stat.S_ISLNK(metadata.st_mode)
            or os.readlink("c:", dir_fd=dosdevices_fd) != "../drive_c"
        ):
            raise NativeBuildError("isolated Wine C drive mapping changed")
    finally:
        if dosdevices_fd >= 0:
            os.close(dosdevices_fd)
        os.close(prefix_fd)


def _verify_prefix_inputs(
    prefix: Path,
    inputs: Mapping[str, bytes],
    source_sha256: str,
) -> None:
    for relative, expected in inputs.items():
        current = _read_beneath(
            prefix,
            PurePosixPath(
                "drive_c",
                "Program Files",
                "MetaTrader 5",
                *_relative_path(relative).parts,
            ),
            f"isolated compiler input {relative}",
            max_bytes=MAX_COMPILER_INPUT_BYTES,
        )
        if current != expected:
            raise NativeBuildError(f"isolated compiler input changed: {relative}")
    source = _read_beneath(
        prefix,
        PurePosixPath("drive_c/QSXBuild/QuantSwarmXAU.mq5"),
        "isolated MQ5 source",
        max_bytes=MAX_SOURCE_BYTES,
    )
    if sha256_bytes(source) != source_sha256:
        raise NativeBuildError("isolated MQ5 source changed")
    _verify_dosdevices(prefix)


def _regular_inode_manifest(root: Path) -> dict[str, tuple[int, int]]:
    result: dict[str, tuple[int, int]] = {}
    for directory, directory_names, file_names in os.walk(
        root,
        topdown=True,
        followlinks=False,
    ):
        base = Path(directory)
        retained_directories: list[str] = []
        for name in sorted(directory_names):
            candidate = base / name
            metadata = candidate.lstat()
            if stat.S_ISLNK(metadata.st_mode):
                continue
            if not stat.S_ISDIR(metadata.st_mode):
                raise NativeBuildError("Wine prefix contains a special directory entry")
            retained_directories.append(name)
        directory_names[:] = retained_directories
        for name in sorted(file_names):
            candidate = base / name
            metadata = candidate.lstat()
            if stat.S_ISREG(metadata.st_mode):
                relative = candidate.relative_to(root).as_posix()
                result[relative] = (metadata.st_dev, metadata.st_ino)
            elif not stat.S_ISLNK(metadata.st_mode):
                raise NativeBuildError("Wine prefix contains a special file entry")
    return result


def _clone_prefix(
    template: Path,
    destination: Path,
    *,
    inputs: Mapping[str, bytes],
    source_sha256: str,
) -> None:
    if destination.exists() or destination.is_symlink():
        raise NativeBuildError("Wine prefix clone destination already exists")
    source_inodes = _regular_inode_manifest(template)
    try:
        shutil.copytree(
            template,
            destination,
            symlinks=True,
            copy_function=shutil.copy2,
        )
    except (OSError, shutil.Error) as exc:
        raise NativeBuildError("cannot physically clone Wine prefix") from exc
    destination_inodes = _regular_inode_manifest(destination)
    if source_inodes.keys() != destination_inodes.keys():
        raise NativeBuildError("Wine prefix clone file set differs from template")
    if any(
        source_inodes[name] == destination_inodes[name]
        for name in source_inodes
    ):
        raise NativeBuildError("Wine prefix clone uses a shared regular-file inode")
    _verify_prefix_inputs(destination, inputs, source_sha256)


def _decode_log(raw: bytes) -> str:
    if raw.startswith(codecs.BOM_UTF8):
        text = raw.decode("utf-8-sig", errors="strict")
    elif raw.startswith(codecs.BOM_UTF16_LE):
        text = raw.decode("utf-16", errors="strict")
    else:
        raise NativeBuildError("MetaEditor log lacks a supported explicit BOM")
    if "\ufffd" in text or "\x00" in text:
        raise NativeBuildError("MetaEditor log contains invalid decoded characters")
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _validate_log(raw: bytes, expected_includes: list[str]) -> bytes:
    text = _decode_log(raw)
    lines = text.splitlines()
    nonempty = [line for line in lines if line]
    result_records = [line for line in lines if line.startswith("Result:")]
    if (
        len(result_records) != 1
        or RESULT_LINE.fullmatch(result_records[0]) is None
        or not nonempty
        or nonempty[-1] != result_records[0]
    ):
        raise NativeBuildError("MetaEditor log lacks one exact zero-error result")
    for line in lines:
        if line == result_records[0]:
            continue
        if re.search(r"(?i)\b(?:errors?|warnings?)\b", line):
            raise NativeBuildError("MetaEditor log contains an error or warning record")

    compile_records = [
        line for line in lines if "information: compiling" in line
    ]
    if compile_records != [COMPILE_LINE]:
        raise NativeBuildError("MetaEditor compiled an unexpected source path")

    include_records = [
        line for line in lines if "information: including" in line
    ]
    includes: list[str] = []
    for line in include_records:
        match = INCLUDE_LINE.fullmatch(line)
        if match is None:
            raise NativeBuildError(
                "MetaEditor include record has an unexpected path or format"
            )
        prefix = "C:\\Program Files\\MetaTrader 5\\"
        full_path = match.group(1)
        if not full_path.startswith(prefix):
            raise NativeBuildError("MetaEditor include escaped the frozen installation")
        includes.append(full_path[len(prefix) :].replace("\\", "/"))
    if sorted(includes) != expected_includes or len(includes) != len(expected_includes):
        raise NativeBuildError("MetaEditor include closure differs from the frozen contract")
    return text.encode("utf-8")


def _compile_once(
    prefix: Path,
    *,
    expected_status: int,
    expected_includes: list[str],
    source_sha256: str,
    minimum_ex5_bytes: int,
    inputs: Mapping[str, bytes],
    host_tools: Mapping[str, HostToolSnapshot],
    output_fd: int,
    console_name: str,
) -> tuple[bytes, bytes]:
    prefix_fd = _open_directory_nofollow(prefix, "isolated compile prefix")
    drive_fd = -1
    build_fd = -1
    try:
        drive_fd = _open_child_directory(
            prefix_fd,
            "drive_c",
            "isolated compile drive_c",
            require_owned=True,
        )
        build_fd = _open_child_directory(
            drive_fd,
            "QSXBuild",
            "isolated compile output directory",
            require_owned=True,
        )
        for generated in ("QuantSwarmXAU.ex5", "QuantSwarmXAU.log", ".compile-start"):
            if _entry_exists_at(build_fd, generated):
                raise NativeBuildError(
                    f"native output existed before compilation: {generated}"
                )
        marker = _write_new_regular_at(
            build_fd,
            ".compile-start",
            b"fresh\n",
            "native compile freshness marker",
            mode=0o400,
        )
        marker_ns = marker.st_mtime_ns
        frozen_source, _ = _read_regular_at(
            build_fd,
            "QuantSwarmXAU.mq5",
            "isolated MQ5 source",
            max_bytes=MAX_SOURCE_BYTES,
        )
        if sha256_bytes(frozen_source) != source_sha256:
            raise NativeBuildError("isolated MQ5 source changed before compilation")

        command = [
            _required_tool(host_tools, "/usr/bin/timeout"),
            "-k",
            "10s",
            "180s",
            _required_tool(host_tools, "/usr/bin/xvfb-run"),
            "-a",
            "-s",
            "-screen 0 1024x768x24",
            _required_tool(host_tools, "/usr/bin/wine64"),
            (
                SANDBOX_PREFIX
                + "/drive_c/Program Files/MetaTrader 5/MetaEditor64.exe"
            ),
            r"/compile:C:\QSXBuild\QuantSwarmXAU.mq5",
            "/log",
        ]
        completed = _run_sandbox(
            prefix,
            command,
            host_tools=host_tools,
            freeze_compiler_inputs=True,
        )
        console = completed.stdout + b"\n[stderr]\n" + completed.stderr
        if len(console) > MAX_CONSOLE_BYTES:
            raise NativeBuildError("MetaEditor console output exceeds its size ceiling")
        _write_new_regular_at(
            output_fd,
            console_name,
            console,
            f"MetaEditor console {console_name}",
            mode=0o400,
        )
        if completed.returncode != expected_status:
            raise NativeBuildError(
                f"MetaEditor process returned {completed.returncode}, "
                f"expected {expected_status}"
            )

        binary, binary_metadata = _read_regular_at(
            build_fd,
            "QuantSwarmXAU.ex5",
            "MetaEditor EX5 output",
            max_bytes=MAX_EX5_BYTES,
        )
        raw_log, log_metadata = _read_regular_at(
            build_fd,
            "QuantSwarmXAU.log",
            "MetaEditor log output",
            max_bytes=MAX_LOG_BYTES,
        )
        for name, metadata in (
            ("QuantSwarmXAU.ex5", binary_metadata),
            ("QuantSwarmXAU.log", log_metadata),
        ):
            if metadata.st_uid != os.getuid() or metadata.st_mtime_ns < marker_ns:
                raise NativeBuildError(f"MetaEditor output is not fresh or owned: {name}")
        source_after, _ = _read_regular_at(
            build_fd,
            "QuantSwarmXAU.mq5",
            "isolated MQ5 source",
            max_bytes=MAX_SOURCE_BYTES,
        )
        if sha256_bytes(source_after) != source_sha256:
            raise NativeBuildError("MetaEditor changed the frozen source")
        if len(binary) < minimum_ex5_bytes:
            raise NativeBuildError("MetaEditor EX5 is below the frozen size floor")
        log = _validate_log(raw_log, expected_includes)
    finally:
        if build_fd >= 0:
            os.close(build_fd)
        if drive_fd >= 0:
            os.close(drive_fd)
        os.close(prefix_fd)

    # These checks deliberately reopen only securely walked paths after the
    # compiler has exited. They detect drive recreation or input replacement;
    # output collection above remained pinned to the pre-opened build directory.
    _verify_prefix_inputs(prefix, inputs, source_sha256)
    return binary, log


def build(
    *,
    source_root: Path,
    contract_path: Path,
    mt5_install: Path,
    output_dir: Path,
) -> Path:
    if os.geteuid() == 0:
        raise NativeBuildError("native compiler harness must not run as root")
    source_root = source_root.expanduser().resolve(strict=True)
    commit = _clean_commit(source_root)
    contract, contract_raw = _load_committed_contract(
        source_root, contract_path, commit
    )

    source_path = str(contract["source_path"])
    source = _git(
        source_root,
        "cat-file",
        "blob",
        f"{commit}:{source_path}",
        binary=True,
    )
    assert isinstance(source, bytes)
    working_source = _regular(source_root / source_path, "working MQ5 source")
    if working_source.read_bytes() != source:
        raise NativeBuildError("working MQ5 source differs from Git HEAD")
    source_sha256 = sha256_bytes(source)
    inputs, host_tools = _verified_inputs(contract, mt5_install)

    output_dir = output_dir.expanduser().absolute()
    if output_dir.exists() or output_dir.is_symlink():
        raise NativeBuildError("native build output must not already exist")
    var_tmp = Path("/var/tmp").resolve(strict=True)
    try:
        output_dir.parent.resolve(strict=True).relative_to(var_tmp)
    except (OSError, ValueError) as exc:
        raise NativeBuildError("native build output must be below /var/tmp") from exc
    output_dir.mkdir(mode=0o700)

    real_experts = (
        mt5_install.expanduser().resolve(strict=True) / "MQL5" / "Experts"
    )
    experts_before, experts_count = _tree_manifest(real_experts)
    terminals_before = _terminal_processes()
    workspace = output_dir / ".isolated-prefixes"
    workspace.mkdir(mode=0o700)

    binary_a: bytes | None = None
    binary_b: bytes | None = None
    log_a: bytes | None = None
    log_b: bytes | None = None
    binary_sha256: str | None = None
    experts_after = experts_before
    experts_after_count = experts_count
    terminals_after = terminals_before
    primary_error: BaseException | None = None
    audit_errors: list[str] = []
    try:
        template = workspace / "template"
        _prepare_template(
            template,
            inputs=inputs,
            source=source,
            expected_wine_version=str(contract["wine_version"]),
        )
        prefix_a = workspace / "prefix-a"
        prefix_b = workspace / "prefix-b"
        _clone_prefix(template, prefix_a)
        _clone_prefix(template, prefix_b)
        expected_status = int(contract["expected_process_exit_code"])
        expected_includes = [str(value) for value in contract["expected_includes"]]
        minimum = int(contract["minimum_ex5_bytes"])
        binary_a, log_a = _compile_once(
            prefix_a,
            expected_status=expected_status,
            expected_includes=expected_includes,
            source_sha256=source_sha256,
            minimum_ex5_bytes=minimum,
            console_path=output_dir / "MetaEditor-A.console",
        )
        binary_b, log_b = _compile_once(
            prefix_b,
            expected_status=expected_status,
            expected_includes=expected_includes,
            source_sha256=source_sha256,
            minimum_ex5_bytes=minimum,
            console_path=output_dir / "MetaEditor-B.console",
        )
        if binary_a != binary_b:
            raise NativeBuildError("two isolated native compiles produced different EX5 bytes")
        binary_sha256 = sha256_bytes(binary_a)
        if binary_sha256 in BANNED_EX5_SHA256:
            raise NativeBuildError("native compiler reproduced a quarantined EX5")
    except BaseException as exc:
        primary_error = exc
    finally:
        try:
            shutil.rmtree(workspace, ignore_errors=False)
        except Exception as exc:
            audit_errors.append(
                f"isolated workspace cleanup failed: {type(exc).__name__}: {exc}"
            )
        try:
            experts_after, experts_after_count = _tree_manifest(real_experts)
            if (
                experts_before != experts_after
                or experts_count != experts_after_count
            ):
                audit_errors.append(
                    "the operator's real MQL5/Experts tree changed during build"
                )
        except Exception as exc:
            audit_errors.append(
                f"real Experts audit failed: {type(exc).__name__}: {exc}"
            )
        try:
            terminals_after = _terminal_processes()
            if terminals_after != terminals_before:
                audit_errors.append(
                    "the operator's active terminal process set changed during build"
                )
        except Exception as exc:
            audit_errors.append(
                f"active terminal audit failed: {type(exc).__name__}: {exc}"
            )
        try:
            _verify_host_tools_unchanged(contract, host_tools)
        except Exception as exc:
            audit_errors.append(
                f"host-tool audit failed: {type(exc).__name__}: {exc}"
            )

    if primary_error is not None:
        for detail in audit_errors:
            primary_error.add_note(detail)
        raise primary_error
    if audit_errors:
        raise NativeBuildError("; ".join(audit_errors))
    if (
        binary_a is None
        or binary_b is None
        or log_a is None
        or log_b is None
        or binary_sha256 is None
    ):
        raise NativeBuildError("native build completed without complete evidence")

    compiler = contract["compiler"]
    assert isinstance(compiler, dict)
    dependency_manifest = canonical_json(contract["dependencies"])
    host_tool_manifest = canonical_json(contract["host_tools"])
    evidence = {
        "binary_reproducible": True,
        "binary_sha256": binary_sha256,
        "compile_log_sha256": sha256_bytes(log_a),
        "compile_runs": 2,
        "compiler": "MetaEditor64.exe",
        "compiler_dependency_manifest_sha256": sha256_bytes(dependency_manifest),
        "compiler_sha256": compiler["sha256"],
        "compiler_version": compiler["version"],
        "contract_sha256": sha256_bytes(contract_raw),
        "errors": 0,
        "experts_manifest_sha256": experts_before,
        "gate_exit_code": 0,
        "host_tool_manifest_sha256": sha256_bytes(host_tool_manifest),
        "process_exit_code": contract["expected_process_exit_code"],
        "sandbox_profile_sha256": sha256_bytes(canonical_json(SANDBOX_PROFILE)),
        "schema_version": 2,
        "secondary_compile_log_sha256": sha256_bytes(log_b),
        "source_commit": commit,
        "source_sha256": source_sha256,
        "warnings": 0,
        "wine_version": contract["wine_version"],
    }
    if frozenset(evidence) != EVIDENCE_FIELDS:
        raise NativeBuildError("native evidence schema implementation is inconsistent")

    (output_dir / "QuantSwarmXAU.mq5").write_bytes(source)
    (output_dir / "QuantSwarmXAU.ex5").write_bytes(binary_a)
    (output_dir / "MQL5_COMPILE.log").write_bytes(log_a)
    (output_dir / "MQL5_COMPILE.secondary.log").write_bytes(log_b)
    evidence_path = output_dir / "MQL5_COMPILE.json"
    evidence_path.write_bytes(canonical_json(evidence))
    host_safety = {
        "active_terminal_processes_after": terminals_after,
        "active_terminal_processes_before": terminals_before,
        "experts_entries": experts_count,
        "experts_manifest_after_sha256": experts_after,
        "experts_manifest_before_sha256": experts_before,
        "schema_version": 1,
    }
    (output_dir / "MQL5_HOST_SAFETY.json").write_bytes(canonical_json(host_safety))
    for candidate in output_dir.iterdir():
        if candidate.is_file():
            candidate.chmod(0o400)
    return evidence_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--contract", required=True, type=Path)
    parser.add_argument("--mt5-install", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    arguments = parser.parse_args(argv)
    try:
        evidence = build(
            source_root=arguments.source_root,
            contract_path=arguments.contract,
            mt5_install=arguments.mt5_install,
            output_dir=arguments.output_dir,
        )
    except (NativeBuildError, OSError, subprocess.SubprocessError) as exc:
        print(f"NATIVE_BUILD_FAILED {exc}", file=sys.stderr)
        return 1
    print(evidence)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
