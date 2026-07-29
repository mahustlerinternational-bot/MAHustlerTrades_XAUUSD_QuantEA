from __future__ import annotations

import ast
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "python_swarm"


def test_release_python_modules_contain_no_runtime_assertions():
    findings = []

    for path in sorted(PACKAGE.glob("*.py")):
        tree = ast.parse(
            path.read_text(encoding="utf-8"),
            filename=str(path),
        )

        for node in ast.walk(tree):
            if isinstance(node, ast.Assert):
                findings.append(
                    f"{path.relative_to(ROOT)}:{node.lineno}"
                )

    assert findings == []


def test_canonical_launcher_works_under_python_optimization():
    completed = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "python_swarm.runtime_app",
            "--version",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0
    assert completed.stderr == ""
    assert completed.stdout.strip() == (
        "QUANT-SWARM-XAU 4.0.0-rc1"
    )
