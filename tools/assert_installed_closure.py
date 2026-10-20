#!/usr/bin/env python3
"""Assert the interpreter and installed application distribution closure."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import platform
import re
import sys
from pathlib import Path


def canonical_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).casefold()


def parse_lock(path: Path) -> dict[str, str]:
    expected: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw or raw[0].isspace() or raw.startswith("#"):
            continue
        match = re.fullmatch(
            r"([A-Za-z0-9_.-]+)==([A-Za-z0-9_.+!-]+)\s*\\?",
            raw,
        )
        if match is None:
            raise RuntimeError(f"unsupported lock requirement: {raw!r}")
        name = canonical_name(match.group(1))
        if name in expected:
            raise RuntimeError(f"duplicate lock requirement: {name}")
        expected[name] = match.group(2)
    if not expected:
        raise RuntimeError("dependency lock is empty")
    return expected


def parse_bootstrap(values: list[str]) -> dict[str, str]:
    expected: dict[str, str] = {}
    for value in values:
        match = re.fullmatch(
            r"([A-Za-z0-9_.-]+)==([A-Za-z0-9_.+!-]+)",
            value,
        )
        if match is None:
            raise RuntimeError(
                f"bootstrap distribution must be pinned as NAME==VERSION: {value!r}"
            )
        name = canonical_name(match.group(1))
        if name in expected:
            raise RuntimeError(f"duplicate bootstrap distribution: {name}")
        expected[name] = match.group(2)
    return expected


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", required=True, type=Path)
    parser.add_argument("--implementation", required=True)
    parser.add_argument("--python-version", required=True)
    parser.add_argument("--machine", required=True)
    parser.add_argument("--allow-bootstrap", action="append", default=[])
    arguments = parser.parse_args(argv)

    actual_runtime = {
        "implementation": sys.implementation.name,
        "machine": platform.machine(),
        "python_version": platform.python_version(),
    }
    expected_runtime = {
        "implementation": arguments.implementation,
        "machine": arguments.machine,
        "python_version": arguments.python_version,
    }
    if actual_runtime != expected_runtime:
        print(
            f"DEPENDENCY_CLOSURE_INVALID runtime={actual_runtime} "
            f"expected={expected_runtime}",
            file=sys.stderr,
        )
        return 1

    try:
        expected = parse_lock(arguments.lock)
        expected_bootstrap = parse_bootstrap(arguments.allow_bootstrap)
    except (OSError, RuntimeError, UnicodeDecodeError) as exc:
        print(f"DEPENDENCY_CLOSURE_INVALID {exc}", file=sys.stderr)
        return 1
    if set(expected) & set(expected_bootstrap):
        print(
            "DEPENDENCY_CLOSURE_INVALID bootstrap overlaps the application lock",
            file=sys.stderr,
        )
        return 1
    installed: dict[str, str] = {}
    duplicates: set[str] = set()
    try:
        for distribution in importlib.metadata.distributions():
            raw_name = distribution.metadata["Name"]
            raw_version = distribution.version
            if not isinstance(raw_name, str) or not raw_name:
                raise RuntimeError("installed distribution has no valid Name metadata")
            if not isinstance(raw_version, str) or not raw_version:
                raise RuntimeError(
                    f"installed distribution has no valid version: {raw_name}"
                )
            name = canonical_name(raw_name)
            if name in installed:
                duplicates.add(name)
            installed[name] = raw_version
    except (KeyError, RuntimeError) as exc:
        print(f"DEPENDENCY_CLOSURE_INVALID {exc}", file=sys.stderr)
        return 1
    application = {
        name: version
        for name, version in installed.items()
        if name not in expected_bootstrap
    }
    actual_bootstrap = {
        name: installed[name]
        for name in expected_bootstrap
        if name in installed
    }
    if (
        duplicates
        or application != expected
        or actual_bootstrap != expected_bootstrap
    ):
        detail = {
            "bootstrap": {
                "actual": actual_bootstrap,
                "expected": expected_bootstrap,
            },
            "duplicates": sorted(duplicates),
            "extra": sorted(set(application) - set(expected)),
            "missing": sorted(set(expected) - set(application)),
            "version_mismatch": {
                name: {"actual": application[name], "expected": expected[name]}
                for name in sorted(set(application) & set(expected))
                if application[name] != expected[name]
            },
        }
        print(
            "DEPENDENCY_CLOSURE_INVALID "
            + json.dumps(detail, sort_keys=True),
            file=sys.stderr,
        )
        return 1
    print(
        "DEPENDENCY_CLOSURE_VALID "
        f"application_distributions={len(application)} "
        f"bootstrap={','.join(sorted(expected_bootstrap))}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
