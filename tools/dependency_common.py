#!/usr/bin/env python3
"""Shared, fail-closed dependency artifact contracts.

The wheelhouse validator deliberately reads directories and files through
``openat``-style file descriptors with ``O_NOFOLLOW``.  Validation therefore
applies to the exact inodes that were opened, not to path names that can be
replaced between a check and a later read.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Mapping


MANIFEST_NAME = "WHEELHOUSE.json"
SIGNATURE_NAME = "WHEELHOUSE.json.sig"
WHEELS_DIRECTORY = "wheels"
HEX_256 = re.compile(r"^[0-9a-f]{64}$")
GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")
SAFE_WHEEL_NAME = re.compile(r"^[A-Za-z0-9_.+-]+\.whl$")
SAFE_IDENTITY = re.compile(r"^[A-Za-z0-9_.@-]{3,128}$")
PIP_VERSION_OUTPUT = re.compile(r"^pip ([A-Za-z0-9_.+-]+) ")


class DependencyGateError(RuntimeError):
    """An offline dependency artifact violates the frozen RC2 contract."""


@dataclass(frozen=True, slots=True)
class Lane:
    name: str
    lock_path: str
    input_paths: tuple[str, ...]
    implementation: str
    implementation_name: str
    python_version: str
    python_version_short: str
    abi: str
    platform: str
    machine: str
    pip_version: str


LANES: Mapping[str, Lane] = {
    "runtime-py312": Lane(
        name="runtime-py312",
        lock_path="python_swarm/requirements-runtime-py312.lock",
        input_paths=("python_swarm/requirements.txt",),
        implementation="CPython",
        implementation_name="cpython",
        python_version="3.12.13",
        python_version_short="3.12",
        abi="cp312",
        platform="manylinux_2_28_x86_64",
        machine="x86_64",
        pip_version="25.1.1",
    ),
    "test-py312": Lane(
        name="test-py312",
        lock_path="requirements-test-py312.lock",
        input_paths=("python_swarm/requirements.txt", "requirements-test.in"),
        implementation="CPython",
        implementation_name="cpython",
        python_version="3.12.13",
        python_version_short="3.12",
        abi="cp312",
        platform="manylinux_2_28_x86_64",
        machine="x86_64",
        pip_version="25.1.1",
    ),
    "test-py314": Lane(
        name="test-py314",
        lock_path="requirements-test-py314.lock",
        input_paths=("python_swarm/requirements.txt", "requirements-test.in"),
        implementation="CPython",
        implementation_name="cpython",
        python_version="3.14.4",
        python_version_short="3.14",
        abi="cp314",
        platform="manylinux_2_28_x86_64",
        machine="x86_64",
        pip_version="25.1.1",
    ),
}

MANIFEST_FIELDS = frozenset(
    {
        "artifact_class",
        "files",
        "inputs",
        "interpreter",
        "lane",
        "lock",
        "schema_version",
        "signing_identity",
        "source_commit",
    }
)
FILE_FIELDS = frozenset(
    {
        "abi_tags",
        "distribution",
        "filename",
        "platform_tags",
        "python_tags",
        "sha256",
        "size",
        "version",
    }
)
SOURCE_FILE_FIELDS = frozenset({"path", "sha256", "size"})
INTERPRETER_FIELDS = frozenset(
    {
        "abi",
        "executable_path",
        "executable_sha256",
        "executable_size",
        "implementation",
        "implementation_name",
        "machine",
        "pip_version",
        "platform",
        "version",
    }
)


def lane_contract(value: str) -> Lane:
    try:
        return LANES[value]
    except KeyError as exc:
        raise DependencyGateError(f"unknown dependency lane: {value!r}") from exc


def canonical_name(value: str) -> str:
    normalized = re.sub(r"[-_.]+", "-", value).casefold()
    if not normalized or re.fullmatch(r"[a-z0-9][a-z0-9-]*", normalized) is None:
        raise DependencyGateError(f"invalid distribution name: {value!r}")
    return normalized


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def canonical_json(value: object) -> bytes:
    return (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
    ).encode("utf-8")


def assert_canonical_ssh_signature(value: bytes, label: str) -> None:
    """Reject bytes OpenSSH ignores outside a canonical armored signature."""

    try:
        text = value.decode("ascii")
    except UnicodeDecodeError as exc:
        raise DependencyGateError(f"{label} is not ASCII armor") from exc
    lines = text.splitlines(keepends=True)
    if (
        len(lines) < 3
        or lines[0] != "-----BEGIN SSH SIGNATURE-----\n"
        or lines[-1] != "-----END SSH SIGNATURE-----\n"
        or any(not line.endswith("\n") or "\r" in line for line in lines)
    ):
        raise DependencyGateError(f"{label} is not canonical OpenSSH armor")
    body_lines = [line[:-1] for line in lines[1:-1]]
    if (
        not body_lines
        or any(not line or len(line) > 70 for line in body_lines)
        or any(len(line) != 70 for line in body_lines[:-1])
    ):
        raise DependencyGateError(f"{label} has noncanonical base64 wrapping")
    encoded = "".join(body_lines)
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise DependencyGateError(f"{label} contains invalid base64") from exc
    if base64.b64encode(decoded).decode("ascii") != encoded:
        raise DependencyGateError(f"{label} base64 is not canonical")
    wrapped = [encoded[index : index + 70] for index in range(0, len(encoded), 70)]
    canonical = (
        "-----BEGIN SSH SIGNATURE-----\n"
        + "".join(f"{line}\n" for line in wrapped)
        + "-----END SSH SIGNATURE-----\n"
    ).encode("ascii")
    if value != canonical:
        raise DependencyGateError(f"{label} contains unparsed or noncanonical bytes")


def isolated_python_environment() -> dict[str, str]:
    """Return a deterministic Python subprocess environment without injection."""

    inherited = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("PYTHON", "LD_", "DYLD_"))
        and key not in {"BASH_ENV", "ENV"}
    }
    return {
        **inherited,
        "LC_ALL": "C.UTF-8",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONHASHSEED": "0",
        "PYTHONNOUSERSITE": "1",
        "TZ": "UTC",
    }


def interpreter_pip_version(executable: Path) -> str:
    completed = subprocess.run(
        [os.fspath(executable), "-m", "pip", "--version"],
        check=False,
        capture_output=True,
        text=True,
        env=isolated_python_environment(),
    )
    match = PIP_VERSION_OUTPUT.match(completed.stdout)
    if completed.returncode != 0 or match is None:
        raise DependencyGateError("cannot determine the wheelhouse pip version")
    return match.group(1)


def validate_relative_path(value: str) -> PurePosixPath:
    if (
        not value
        or "\\" in value
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise DependencyGateError(f"dependency path is not canonical: {value!r}")
    parsed = PurePosixPath(value)
    if (
        parsed.is_absolute()
        or parsed.as_posix() != value
        or any(part in {"", ".", ".."} for part in parsed.parts)
    ):
        raise DependencyGateError(f"dependency path is not canonical: {value!r}")
    return parsed


def _open_flags(*, directory: bool = False) -> int:
    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    if directory:
        flags |= getattr(os, "O_DIRECTORY", 0)
    return flags


def open_directory_path(path: str | Path) -> int:
    """Open an absolute directory path without following any path component."""

    absolute = Path(path).expanduser().absolute()
    parts = absolute.parts
    if not parts or parts[0] != os.sep:
        raise DependencyGateError(f"directory path is not absolute: {absolute}")
    current = os.open(os.sep, _open_flags(directory=True))
    try:
        for part in parts[1:]:
            following = os.open(
                part,
                _open_flags(directory=True),
                dir_fd=current,
            )
            os.close(current)
            current = following
        metadata = os.fstat(current)
        if not stat.S_ISDIR(metadata.st_mode):
            raise DependencyGateError(f"path is not a directory: {absolute}")
        return current
    except Exception:
        os.close(current)
        raise


def open_relative_directory(root_fd: int, relative: str) -> int:
    parsed = validate_relative_path(relative)
    current = os.dup(root_fd)
    try:
        for part in parsed.parts:
            following = os.open(
                part,
                _open_flags(directory=True),
                dir_fd=current,
            )
            os.close(current)
            current = following
        return current
    except Exception:
        os.close(current)
        raise


def _metadata_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def snapshot_at(
    directory_fd: int,
    filename: str,
    *,
    label: str,
    maximum_size: int | None = None,
) -> tuple[bytes, os.stat_result]:
    """Snapshot one regular file from a trusted directory descriptor."""

    if (
        not filename
        or filename in {".", ".."}
        or "/" in filename
        or "\\" in filename
        or "\0" in filename
    ):
        raise DependencyGateError(f"{label} filename is invalid")
    try:
        descriptor = os.open(filename, _open_flags(), dir_fd=directory_fd)
    except OSError as exc:
        raise DependencyGateError(f"cannot open {label}: {exc}") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise DependencyGateError(f"{label} must be a regular file")
        if before.st_nlink != 1:
            raise DependencyGateError(f"{label} must have exactly one hard link")
        if maximum_size is not None and before.st_size > maximum_size:
            raise DependencyGateError(f"{label} exceeds its size limit")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise DependencyGateError(f"{label} changed while being read")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise DependencyGateError(f"{label} grew while being read")
        after = os.fstat(descriptor)
        if _metadata_identity(before) != _metadata_identity(after):
            raise DependencyGateError(f"{label} changed while being read")
        return b"".join(chunks), after
    finally:
        os.close(descriptor)


def snapshot_relative_file(
    root_fd: int,
    relative: str,
    *,
    label: str,
    maximum_size: int | None = None,
) -> tuple[bytes, os.stat_result]:
    parsed = validate_relative_path(relative)
    parent = parsed.parent.as_posix()
    directory_fd = (
        os.dup(root_fd)
        if parent == "."
        else open_relative_directory(root_fd, parent)
    )
    try:
        return snapshot_at(
            directory_fd,
            parsed.name,
            label=label,
            maximum_size=maximum_size,
        )
    finally:
        os.close(directory_fd)


def snapshot_path(
    path: str | Path,
    *,
    label: str,
    maximum_size: int | None = None,
) -> tuple[bytes, os.stat_result]:
    absolute = Path(path).expanduser().absolute()
    parent_fd = open_directory_path(absolute.parent)
    try:
        return snapshot_at(
            parent_fd,
            absolute.name,
            label=label,
            maximum_size=maximum_size,
        )
    finally:
        os.close(parent_fd)


def resolve_executable_path(path: str | Path) -> tuple[Path, bytes, os.stat_result]:
    """Resolve a trusted executable launcher and bind its final regular inode.

    A distribution-managed interpreter launcher may itself be a symlink.  Only
    a root-owned final symlink is accepted; the resolved target is then opened
    through the same no-follow snapshot path used for dependency artifacts.
    """

    requested = Path(path).expanduser().absolute()
    try:
        launcher_metadata = requested.lstat()
    except OSError as exc:
        raise DependencyGateError(f"cannot inspect interpreter launcher: {exc}") from exc
    if stat.S_ISLNK(launcher_metadata.st_mode):
        if launcher_metadata.st_uid != 0:
            raise DependencyGateError(
                "symlinked interpreter launcher must be owned by root"
            )
        try:
            resolved = requested.resolve(strict=True)
        except OSError as exc:
            raise DependencyGateError(
                f"cannot resolve interpreter launcher: {exc}"
            ) from exc
    else:
        resolved = requested
    value, metadata = snapshot_path(
        resolved,
        label="resolved interpreter executable",
        maximum_size=256 * 1024 * 1024,
    )
    if not stat.S_ISREG(metadata.st_mode) or not metadata.st_mode & 0o111:
        raise DependencyGateError("resolved interpreter is not an executable file")
    return resolved, value, metadata


def trusted_system_executable(name: str) -> Path:
    candidate = shutil.which(name)
    if candidate is None:
        raise DependencyGateError(f"required system executable is missing: {name}")
    resolved, _value, metadata = resolve_executable_path(candidate)
    if metadata.st_uid != 0 or stat.S_IMODE(metadata.st_mode) & 0o022:
        raise DependencyGateError(
            f"system executable must be root-owned and protected: {name}"
        )
    return resolved


def assert_not_writable(metadata: os.stat_result, label: str) -> None:
    if stat.S_IMODE(metadata.st_mode) & 0o222:
        raise DependencyGateError(f"{label} must be read-only")


def parse_lock(value: bytes) -> dict[str, tuple[str, frozenset[str]]]:
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise DependencyGateError("dependency lock is not UTF-8") from exc
    lines = text.splitlines()
    requirements: dict[str, tuple[str, frozenset[str]]] = {}
    index = 0
    while index < len(lines):
        raw = lines[index]
        if not raw or raw[0].isspace() or raw.startswith("#"):
            index += 1
            continue
        match = re.fullmatch(
            r"([A-Za-z0-9_.-]+)==([A-Za-z0-9_.+!-]+)\s*\\?",
            raw,
        )
        if match is None:
            raise DependencyGateError(f"unsupported lock requirement: {raw!r}")
        name = canonical_name(match.group(1))
        version = match.group(2)
        block = [raw]
        index += 1
        while index < len(lines):
            following = lines[index]
            if following and not following[0].isspace() and not following.startswith("#"):
                break
            block.append(following)
            index += 1
        hashes = frozenset(
            re.findall(r"--hash=sha256:([0-9a-f]{64})(?:\s|\\|$)", "\n".join(block))
        )
        if not hashes:
            raise DependencyGateError(f"lock requirement has no SHA-256 hashes: {name}")
        if name in requirements:
            raise DependencyGateError(f"duplicate lock requirement: {name}")
        requirements[name] = (version, hashes)
    if not requirements:
        raise DependencyGateError("dependency lock has no requirements")
    return requirements


def parse_wheel_filename(filename: str) -> dict[str, object]:
    if SAFE_WHEEL_NAME.fullmatch(filename) is None:
        raise DependencyGateError(f"wheel filename is invalid: {filename!r}")
    stem = filename[:-4]
    parts = stem.split("-")
    if len(parts) not in {5, 6}:
        raise DependencyGateError(f"wheel filename is not canonical: {filename!r}")
    if len(parts) == 6:
        distribution, version, build, python_tag, abi_tag, platform_tag = parts
        if re.fullmatch(r"[0-9][A-Za-z0-9_.]*", build) is None:
            raise DependencyGateError(f"wheel build tag is invalid: {filename!r}")
    else:
        distribution, version, python_tag, abi_tag, platform_tag = parts
    if not version:
        raise DependencyGateError(f"wheel version is empty: {filename!r}")
    return {
        "distribution": canonical_name(distribution),
        "version": version,
        "python_tags": sorted(set(python_tag.split("."))),
        "abi_tags": sorted(set(abi_tag.split("."))),
        "platform_tags": sorted(set(platform_tag.split("."))),
    }


def validate_wheel_for_lane(record: Mapping[str, object], lane: Lane) -> None:
    python_tags = record.get("python_tags")
    abi_tags = record.get("abi_tags")
    platform_tags = record.get("platform_tags")
    if not all(
        isinstance(value, list)
        and value
        and all(isinstance(item, str) and item for item in value)
        for value in (python_tags, abi_tags, platform_tags)
    ):
        raise DependencyGateError("wheel tag lists are invalid")
    python_values = set(python_tags)
    abi_values = set(abi_tags)
    platform_values = set(platform_tags)
    if platform_values == {"any"}:
        lane_pure_tag = f"py{lane.abi[2:]}"
        if abi_values != {"none"} or not any(
            tag in {"py3", lane_pure_tag}
            for tag in python_values
        ):
            raise DependencyGateError(
                f"pure wheel is incompatible with {lane.name}"
            )
        return
    if lane.platform not in platform_values:
        raise DependencyGateError(
            f"wheel platform is incompatible with {lane.platform}"
        )
    if lane.abi not in python_values or lane.abi not in abi_values:
        raise DependencyGateError(
            f"wheel ABI is incompatible with {lane.abi}"
        )


def wheel_record(filename: str, value: bytes) -> dict[str, object]:
    parsed = parse_wheel_filename(filename)
    return {
        "filename": filename,
        "sha256": sha256_bytes(value),
        "size": len(value),
        **parsed,
    }


def validate_records_against_lock(
    records: list[dict[str, object]],
    requirements: Mapping[str, tuple[str, frozenset[str]]],
    lane: Lane,
) -> None:
    by_name: dict[str, dict[str, object]] = {}
    filenames: set[str] = set()
    for record in records:
        if frozenset(record) != FILE_FIELDS:
            raise DependencyGateError("wheel record has missing or unknown fields")
        filename = record.get("filename")
        digest = record.get("sha256")
        size = record.get("size")
        name = record.get("distribution")
        version = record.get("version")
        if not isinstance(filename, str) or filename in filenames:
            raise DependencyGateError("wheel filenames must be unique strings")
        filenames.add(filename)
        parsed_filename = parse_wheel_filename(filename)
        if any(
            record.get(field) != parsed_filename[field]
            for field in parsed_filename
        ):
            raise DependencyGateError(
                f"wheel metadata disagrees with its filename: {filename}"
            )
        if not isinstance(name, str) or name != canonical_name(name) or name in by_name:
            raise DependencyGateError("wheel distributions must be unique and canonical")
        if not isinstance(version, str) or not version:
            raise DependencyGateError("wheel version is invalid")
        if not isinstance(digest, str) or HEX_256.fullmatch(digest) is None:
            raise DependencyGateError("wheel digest is invalid")
        if type(size) is not int or size < 1:
            raise DependencyGateError("wheel size is invalid")
        validate_wheel_for_lane(record, lane)
        by_name[name] = record
    if set(by_name) != set(requirements):
        missing = sorted(set(requirements) - set(by_name))
        extra = sorted(set(by_name) - set(requirements))
        raise DependencyGateError(
            f"wheel closure mismatch; missing={missing} extra={extra}"
        )
    for name, (expected_version, allowed_hashes) in requirements.items():
        record = by_name[name]
        if record["version"] != expected_version:
            raise DependencyGateError(f"wheel version disagrees with lock: {name}")
        if record["sha256"] not in allowed_hashes:
            raise DependencyGateError(f"wheel digest is not authorized by lock: {name}")


def _git_environment() -> dict[str, str]:
    inherited = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("GIT_", "LD_", "DYLD_"))
        and key not in {"BASH_ENV", "ENV"}
    }
    return {
        **inherited,
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_OPTIONAL_LOCKS": "0",
        "LC_ALL": "C",
        "TZ": "UTC",
    }


def git_output(root: Path, *arguments: str) -> str:
    git = trusted_system_executable("git")
    completed = subprocess.run(
        [
            os.fspath(git),
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.untrackedCache=false",
            "-c",
            f"core.worktree={root}",
            "-C",
            os.fspath(root),
            *arguments,
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_git_environment(),
    )
    if completed.returncode != 0:
        raise DependencyGateError(
            completed.stderr.strip() or "Git provenance command failed"
        )
    return completed.stdout.strip()


def source_commit(root: Path) -> str:
    normalized = root.resolve(strict=True)
    top_level = Path(
        git_output(normalized, "rev-parse", "--show-toplevel")
    ).resolve(strict=True)
    if top_level != normalized:
        raise DependencyGateError(
            "source root must be the exact Git worktree top level"
        )
    status = git_output(
        normalized,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    )
    if status:
        raise DependencyGateError(
            "dependency source worktree is dirty or contains untracked files"
        )
    commit = git_output(normalized, "rev-parse", "--verify", "HEAD^{commit}")
    if GIT_COMMIT.fullmatch(commit) is None:
        raise DependencyGateError("source commit is not a full SHA-1 identifier")
    return commit


def git_blob(root: Path, commit: str, relative: str) -> bytes:
    validate_relative_path(relative)
    git = trusted_system_executable("git")
    completed = subprocess.run(
        [
            os.fspath(git),
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.untrackedCache=false",
            "-c",
            f"core.worktree={root}",
            "-C",
            os.fspath(root),
            "cat-file",
            "blob",
            f"{commit}:{relative}",
        ],
        check=False,
        capture_output=True,
        env=_git_environment(),
    )
    if completed.returncode != 0:
        raise DependencyGateError(f"dependency input is not tracked: {relative}")
    return completed.stdout


def source_file_records(
    root: Path,
    root_fd: int,
    commit: str,
    paths: tuple[str, ...],
) -> tuple[list[dict[str, object]], dict[str, bytes]]:
    records: list[dict[str, object]] = []
    values: dict[str, bytes] = {}
    for relative in paths:
        value, metadata = snapshot_relative_file(
            root_fd,
            relative,
            label=f"dependency source {relative}",
            maximum_size=8 * 1024 * 1024,
        )
        if value != git_blob(root, commit, relative):
            raise DependencyGateError(
                f"dependency source differs from Git commit: {relative}"
            )
        values[relative] = value
        records.append(
            {
                "path": relative,
                "sha256": sha256_bytes(value),
                "size": metadata.st_size,
            }
        )
    return records, values


def validate_source_record(value: object, *, expected_path: str) -> dict[str, object]:
    if not isinstance(value, dict) or frozenset(value) != SOURCE_FILE_FIELDS:
        raise DependencyGateError("source file record has missing or unknown fields")
    if value.get("path") != expected_path:
        raise DependencyGateError("source file record path is invalid")
    digest = value.get("sha256")
    size = value.get("size")
    if not isinstance(digest, str) or HEX_256.fullmatch(digest) is None:
        raise DependencyGateError("source file record digest is invalid")
    if type(size) is not int or size < 1:
        raise DependencyGateError("source file record size is invalid")
    return value
