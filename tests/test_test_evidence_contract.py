import hashlib
import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import pytest_evidence_plugin
from tools import run_test_evidence as evidence


ROOT = Path(__file__).resolve().parents[1]


def test_pytest_evidence_plugin_emits_exact_counts_and_nodeid_digest(
    tmp_path,
    monkeypatch,
):
    result_path = tmp_path / "result.json"
    nodeids_path = tmp_path / "nodeids.txt"
    monkeypatch.setenv("QSX_PYTEST_EVIDENCE_RESULT", str(result_path))
    monkeypatch.setenv("QSX_PYTEST_EVIDENCE_NODEIDS", str(nodeids_path))
    plugin = pytest_evidence_plugin._EvidencePlugin()
    plugin.nodeids = ["tests/test_a.py::test_one", "tests/test_b.py::test_two"]
    reports = [
        SimpleNamespace(when="call", nodeid=nodeid)
        for nodeid in plugin.nodeids
    ]
    reporter = SimpleNamespace(stats={"passed": reports})
    manager = SimpleNamespace(
        get_plugin=lambda name: reporter if name == "terminalreporter" else None
    )
    session = SimpleNamespace(
        config=SimpleNamespace(pluginmanager=manager),
    )

    plugin.pytest_sessionfinish(session, 0)

    result = json.loads(result_path.read_text(encoding="utf-8"))
    nodeids = "".join(f"{value}\n" for value in plugin.nodeids).encode()
    assert result["collected"] == 2
    assert result["passed"] == 2
    assert result["failed"] == 0
    assert result["nodeids_sha256"] == hashlib.sha256(nodeids).hexdigest()
    assert nodeids_path.read_bytes() == nodeids


def test_test_contract_rejects_collection_or_argument_weakening():
    valid = {
        "allowed_skips": [],
        "expected_collected": 500,
        "nodeids_sha256": "a" * 64,
        "pytest_arguments": [
            "-q",
            "-W",
            "error",
            "-p",
            "no:cacheprovider",
        ],
        "required_nodeids": ["tests/test_mql5_contract.py::test_real_block"],
        "schema_version": 1,
    }
    evidence._validate_contract(valid)

    with pytest.raises(evidence.TestEvidenceError, match="pytest arguments"):
        evidence._validate_contract(
            {**valid, "pytest_arguments": ["-q", "-p", "no:cacheprovider"]}
        )
    with pytest.raises(evidence.TestEvidenceError, match="collection count"):
        evidence._validate_contract({**valid, "expected_collected": 0})


def test_test_evidence_runner_is_networkless_and_keeps_process_and_gate_separate():
    source = (ROOT / "tools" / "run_test_evidence.py").read_text(encoding="utf-8")

    assert '"--unshare-all"' in source
    assert '"--ro-bind"' in source
    assert "PYTEST_DISABLE_PLUGIN_AUTOLOAD" in source
    assert '"gate_exit_code": 0' in source
    assert "pytest_nodeids_sha256" in evidence.EVIDENCE_FIELDS
    assert "process_exit_code" not in evidence.EVIDENCE_FIELDS


def test_test_evidence_contract_cannot_be_injected_from_outside_source_tree(
    tmp_path: Path,
):
    source_root = tmp_path / "source"
    canonical = source_root / evidence.CANONICAL_TEST_CONTRACT.as_posix()
    canonical.parent.mkdir(parents=True)
    canonical.write_text("{}\n", encoding="utf-8")
    external = tmp_path / "test-contract.json"
    shutil.copyfile(canonical, external)

    with pytest.raises(evidence.TestEvidenceError, match="canonical source-tree"):
        evidence._load_committed_test_contract(source_root, external, "0" * 40)


def test_test_evidence_git_reads_disable_replace_refs_and_mutating_caches():
    environment = evidence._git_env()
    source = (ROOT / "tools" / "run_test_evidence.py").read_text(encoding="utf-8")

    assert environment["GIT_NO_REPLACE_OBJECTS"] == "1"
    assert environment["GIT_OPTIONAL_LOCKS"] == "0"
    assert "core.fsmonitor=false" in source
    assert "core.hooksPath=/dev/null" in source
    assert "core.untrackedCache=false" in source
