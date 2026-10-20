#!/usr/bin/env python3
"""Fail-closed streaming secret scanner with redacted findings.

The scanner intentionally has no size, binary, evidence, archive, or recovery
exceptions. Reports contain only a path and rule name—never the matched value,
source line, prefix, suffix, or fingerprint.
"""

from __future__ import annotations

import argparse
import os
import re
import stat
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable, Iterator


CHUNK_SIZE = 1024 * 1024
OVERLAP = 4096
CONTENT_RULES: tuple[tuple[str, re.Pattern[bytes]], ...] = (
    (
        "private_key",
        re.compile(rb"-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----"),
    ),
    (
        "telegram_bot_token",
        re.compile(rb"(?<![A-Za-z0-9_-])[0-9]{8,12}:[A-Za-z0-9_-]{25,}(?![A-Za-z0-9_-])"),
    ),
    (
        "jwt",
        re.compile(
            rb"(?<![A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{20,}\."
            rb"[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}(?![A-Za-z0-9_-])"
        ),
    ),
    (
        "aws_access_key",
        re.compile(rb"(?<![A-Z0-9])(?:AKIA|ASIA)[A-Z0-9]{16}(?![A-Z0-9])"),
    ),
    (
        "github_token",
        re.compile(
            rb"(?<![A-Za-z0-9_])"
            rb"(?:(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{30,}"
            rb"|github_pat_[A-Za-z0-9_]{22,})"
        ),
    ),
    (
        "discord_webhook",
        re.compile(
            rb"https://(?:canary\.|ptb\.)?discord(?:app)?\.com/api/webhooks/"
            rb"[0-9]{8,}/[A-Za-z0-9._-]{20,}"
        ),
    ),
    (
        "credential_in_url",
        re.compile(rb"[A-Za-z][A-Za-z0-9+.-]{1,15}://[^/\s:@]{1,128}:[^/\s@]{4,256}@"),
    ),
    (
        "assigned_env_secret",
        re.compile(
            rb"(?m)^[ \t]*(?:export[ \t]+)?[A-Z][A-Z0-9_]*"
            rb"(?:SECRET|TOKEN|PASSWORD|PASSWD|API[_-]?KEY|PRIVATE[_-]?KEY)"
            rb"[A-Z0-9_]*[ \t]*=[ \t]*[\"']?"
            rb"(?![\"']?[ \t]*(?:$|#|null\b|none\b|false\b|true\b|"
            rb"redacted\b|changeme\b|required\b|example\b|placeholder\b|"
            rb"QSX_[A-Z0-9_]+\b|\$\{|<))[A-Za-z0-9/+_.:@-]{16,}"
        ),
    ),
    (
        "assigned_json_secret",
        re.compile(
            rb"(?im)^[ \t]*[\"'][A-Za-z0-9_.-]*"
            rb"(?:secret|token|password|passwd|api[_-]?key|private[_-]?key)"
            rb"[A-Za-z0-9_.-]*[\"'][ \t]*:[ \t]*[\"']"
            rb"(?![ \t]*(?:redacted\b|changeme\b|required\b|example\b|"
            rb"placeholder\b|QSX_[A-Z0-9_]+\b|\$\{|<))"
            rb"[^\"'\r\n]{16,}[\"']"
        ),
    ),
)
SENSITIVE_NAME_PARTS = ("token", "secret", "credential", "password", "passwd")


@dataclass(frozen=True, order=True, slots=True)
class Finding:
    path: str
    rule: str


class ScanError(RuntimeError):
    """The scanner could not establish a complete result."""


def _safe_display(value: str) -> str:
    return "".join(
        character if 32 <= ord(character) < 127 else "?" for character in value
    )


def _filename_findings(path: str) -> tuple[Finding, ...]:
    findings: set[Finding] = set()
    if sensitive_filename(path):
        findings.add(Finding(path, "sensitive_filename"))
    encoded = os.fsencode(path)
    for rule, pattern in CONTENT_RULES:
        if pattern.search(encoded):
            findings.add(Finding(path, f"filename_{rule}"))
    return tuple(sorted(findings))


def redact_path(path: str) -> str:
    parts: list[str] = []
    for component in PurePosixPath(path).parts:
        encoded = os.fsencode(component)
        if sensitive_filename(component) or any(
            pattern.search(encoded) for _, pattern in CONTENT_RULES
        ):
            parts.append("[REDACTED]")
        else:
            parts.append(_safe_display(component))
    return "/".join(parts) or "[REDACTED]"


def sensitive_filename(path: str) -> bool:
    name = PurePosixPath(path).name.casefold()
    if name == ".env.example" or name.endswith(".pub"):
        return False
    if name in {"allowed_signers", "sha256sums.txt.sig"}:
        return False
    if name == ".env" or name.startswith(".env.") or name.endswith((".key", ".pem", ".p12", ".pfx")):
        return True
    return any(part in name for part in SENSITIVE_NAME_PARTS)


def scan_named_bytes(path: str, value: bytes) -> tuple[Finding, ...]:
    findings = set(_filename_findings(path))
    for rule, pattern in CONTENT_RULES:
        if pattern.search(value):
            findings.add(Finding(path, rule))
    return tuple(sorted(findings))


def _scan_regular_file(path: Path, display: str) -> Iterator[Finding]:
    found_rules: set[str] = set()
    tail = b""
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags)
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ScanError(
                f"path changed during scan: {redact_path(display)}"
            )
        handle = os.fdopen(descriptor, "rb", buffering=0, closefd=True)
        descriptor = None
        with handle:
            while True:
                chunk = handle.read(CHUNK_SIZE)
                if not chunk:
                    break
                window = tail + chunk
                for rule, pattern in CONTENT_RULES:
                    if rule not in found_rules and pattern.search(window):
                        found_rules.add(rule)
                        yield Finding(display, rule)
                tail = window[-OVERLAP:]
            after = os.fstat(handle.fileno())
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
            if any(
                getattr(before, field) != getattr(after, field)
                for field in stable_fields
            ):
                raise ScanError(
                    f"path changed during scan: {redact_path(display)}"
                )
    except OSError as exc:
        raise ScanError(
            f"cannot read {redact_path(display)}: {exc.strerror}"
        ) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def scan_path(root: Path) -> tuple[Finding, ...]:
    if not root.exists() and not root.is_symlink():
        raise ScanError(f"path does not exist: {redact_path(os.fspath(root))}")
    root = root.absolute()
    findings: set[Finding] = set()
    candidates: Iterable[Path]
    if root.is_dir() and not root.is_symlink():
        candidates = (root, *sorted(root.rglob("*")))
    else:
        candidates = (root,)
    for candidate in candidates:
        try:
            metadata = candidate.lstat()
        except OSError as exc:
            raise ScanError(
                f"cannot inspect {redact_path(os.fspath(candidate))}: {exc.strerror}"
            ) from exc
        try:
            display = candidate.relative_to(root).as_posix() if candidate != root else candidate.name
        except ValueError:
            display = candidate.name
        findings.update(_filename_findings(display))
        if stat.S_ISLNK(metadata.st_mode):
            findings.add(Finding(display, "symlink"))
        elif stat.S_ISREG(metadata.st_mode):
            findings.update(_scan_regular_file(candidate, display))
        elif not stat.S_ISDIR(metadata.st_mode):
            findings.add(Finding(display, "special_file"))
    return tuple(sorted(findings))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path)
    arguments = parser.parse_args(argv)
    findings: set[Finding] = set()
    try:
        for path in arguments.paths:
            findings.update(scan_path(path))
    except ScanError as exc:
        print(f"SECRET_SCAN_ERROR {exc}", file=sys.stderr)
        return 2
    for finding in sorted(findings):
        print(
            f"SECRET_SCAN_FAILED path={redact_path(finding.path)} "
            f"rule={finding.rule}"
        )
    if findings:
        return 1
    print("SECRET_SCAN_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
