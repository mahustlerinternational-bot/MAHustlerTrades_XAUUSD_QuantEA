import os
import re
import subprocess
import sys
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
        ROOT / "deployment" / "config.check.example.json",
        ROOT / "deployment" / ".env.example",
    ]
    for path in files:
        assert not credential_pattern.search(path.read_text(encoding="utf-8")), path

    env_example = (ROOT / "deployment" / ".env.example").read_text(encoding="utf-8")
    assert "QSX_TG_TOKEN=\n" in env_example
    assert "QSX_TG_CHAT=\n" in env_example


def test_release_boundary_never_depends_on_legacy_archives_or_recovery_data():
    allowlist = (
        ROOT / "release" / "runtime-allowlist.txt"
    ).read_text(encoding="utf-8").splitlines()
    prohibited = (
        "RECOVERY_",
        "backups/",
        "build_evidence/",
        "release_output/",
    )
    for relative in allowlist:
        assert not relative.endswith((".zip", ".tar", ".tar.gz", ".tgz"))
        assert not relative.startswith(prohibited)

    builder = (ROOT / "tools" / "build_release.py").read_text(encoding="utf-8")
    assert "build_evidence/" not in builder
    assert "RECOVERY_" not in builder
