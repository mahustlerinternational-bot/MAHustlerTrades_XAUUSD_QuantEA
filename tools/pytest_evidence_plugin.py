"""Minimal pytest plugin that emits machine-readable release-gate results."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path


class _EvidencePlugin:
    def __init__(self) -> None:
        self.nodeids: list[str] = []

    def pytest_collection_finish(self, session) -> None:
        self.nodeids = [item.nodeid for item in session.items]

    def pytest_sessionfinish(self, session, exitstatus) -> None:
        reporter = session.config.pluginmanager.get_plugin("terminalreporter")
        if reporter is None:
            raise RuntimeError("pytest terminal reporter is required for evidence")
        stats = reporter.stats

        def reports(name: str):
            return list(stats.get(name, ()))

        call_passed = [
            report
            for report in reports("passed")
            if getattr(report, "when", None) == "call"
            and not hasattr(report, "wasxfail")
        ]
        call_failed = [
            report
            for report in reports("failed")
            if getattr(report, "when", None) == "call"
        ]
        errors = [
            report
            for report in reports("failed")
            if getattr(report, "when", None) != "call"
        ] + reports("error")
        skipped = {
            report.nodeid
            for report in reports("skipped")
            if not hasattr(report, "wasxfail")
        }
        xfailed = {
            report.nodeid
            for report in reports("xfailed") + reports("skipped")
            if hasattr(report, "wasxfail")
        }
        xpassed = {
            report.nodeid
            for report in reports("xpassed") + reports("passed")
            if hasattr(report, "wasxfail")
        }
        nodeid_bytes = "".join(f"{value}\n" for value in self.nodeids).encode("utf-8")
        result = {
            "collected": len(self.nodeids),
            "deselected": len(reports("deselected")),
            "errors": len(errors),
            "exit_status": int(exitstatus),
            "failed": len(call_failed),
            "nodeids_sha256": hashlib.sha256(nodeid_bytes).hexdigest(),
            "passed": len(call_passed),
            "schema_version": 1,
            "skipped": sorted(skipped),
            "warnings": len(reports("warnings")),
            "xfailed": sorted(xfailed),
            "xpassed": sorted(xpassed),
        }
        destination = os.environ.get("QSX_PYTEST_EVIDENCE_RESULT", "")
        if not destination:
            raise RuntimeError("QSX_PYTEST_EVIDENCE_RESULT is required")
        with Path(destination).open("xb") as handle:
            handle.write(
                (
                    json.dumps(
                        result,
                        indent=2,
                        sort_keys=True,
                        ensure_ascii=True,
                    )
                    + "\n"
                ).encode("utf-8")
            )
        nodeids_destination = os.environ.get("QSX_PYTEST_EVIDENCE_NODEIDS", "")
        if not nodeids_destination:
            raise RuntimeError("QSX_PYTEST_EVIDENCE_NODEIDS is required")
        with Path(nodeids_destination).open("xb") as handle:
            handle.write(nodeid_bytes)


def pytest_configure(config) -> None:
    config.pluginmanager.register(_EvidencePlugin(), "qsx-release-evidence")
