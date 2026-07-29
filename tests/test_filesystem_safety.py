import json
import os
import re
import stat
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from safe_paths import create_ephemeral_workspace, guarded_rmtree, runtime_state_dir


ROOT = Path(__file__).resolve().parent.parent
def test_runtime_and_demo_modules_have_no_recursive_delete():
    allowed = {ROOT / "python_swarm" / "safe_paths.py"}
    for path in sorted((ROOT / "python_swarm").glob("*.py")):
        if path in allowed:
            continue
        source = path.read_text(encoding="utf-8")
        assert "rmtree(" not in source, path.name
        assert "rm -rf" not in source, path.name


def test_guard_refuses_unmarked_or_non_temporary_paths(tmp_path):
    sentinel = tmp_path / "keep.txt"
    sentinel.write_text("keep", encoding="utf-8")

    with pytest.raises(RuntimeError):
        guarded_rmtree(tmp_path, tmp_path)

    assert sentinel.read_text(encoding="utf-8") == "keep"


def test_guard_deletes_only_marked_ephemeral_workspace():
    workspace = create_ephemeral_workspace("guard-test")
    (workspace / "disposable.txt").write_text("temporary", encoding="utf-8")

    guarded_rmtree(workspace, workspace)

    assert not workspace.exists()


def test_runtime_state_never_falls_back_into_source(monkeypatch):
    monkeypatch.delenv("QSX_STATE_DIR", raising=False)

    with pytest.raises(RuntimeError, match="QSX_STATE_DIR is required"):
        runtime_state_dir()


def test_backtester_import_does_not_create_output_directory(tmp_path):
    output = tmp_path / "must-not-exist"
    env = dict(os.environ, QSX_BACKTEST_DIR=str(output), PYTHONPATH=str(ROOT))

    completed = subprocess.run(
        [sys.executable, "-c", "import python_swarm.backtester"],
        cwd=tmp_path,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert not output.exists()


def test_docker_build_is_allowlisted():
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "COPY . ." not in dockerfile


def test_operator_files_contain_no_live_looking_credentials():
    credential_pattern = re.compile(
        r"(?:[0-9]{8,12}:[A-Za-z0-9_-]{25,}"
        r"|eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,})"
    )
    files = [
        ROOT / "QUICK_START.sh",
        ROOT / "QUANT XAUUSD EABOT.txt",
        ROOT / "deployment" / "config.json",
        ROOT / "deployment" / ".env.example",
    ]
    for path in files:
        assert not credential_pattern.search(path.read_text(encoding="utf-8")), path

    config = json.loads((ROOT / "deployment" / "config.json").read_text())
    assert "telegram" not in config
    env_example = (ROOT / "deployment" / ".env.example").read_text(encoding="utf-8")
    assert "QSX_TG_TOKEN=\n" in env_example
    assert "QSX_TG_CHAT=\n" in env_example


def test_legacy_archive_is_sanitized_and_private():
    archive = ROOT / "QUANT_SWARM_XAU_v3.0_COMPLETE (10).zip"
    credential_pattern = re.compile(
        rb"(?:[0-9]{8,12}:[A-Za-z0-9_-]{25,}"
        rb"|eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,})"
    )

    assert stat.S_IMODE(archive.stat().st_mode) == 0o600
    with zipfile.ZipFile(archive) as bundle:
        for member in bundle.infolist():
            if member.is_dir():
                continue
            assert not credential_pattern.search(bundle.read(member)), member.filename


def test_recovery_evidence_permissions_are_private():
    recovery = ROOT / "RECOVERY_2026-07-15_OPEN_DB_HANDLES"

    assert stat.S_IMODE(recovery.stat().st_mode) == 0o700
    for path in recovery.rglob("*"):
        expected = 0o700 if path.is_dir() else 0o600
        assert stat.S_IMODE(path.stat().st_mode) == expected, path
