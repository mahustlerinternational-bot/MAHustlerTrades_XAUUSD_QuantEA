from pathlib import Path

import pytest

from python_swarm.runtime_paths import ProcessLockError, RuntimePathError, RuntimePaths


def test_resolution_is_mode_isolated_and_has_no_io(tmp_path, monkeypatch):
    root = tmp_path / "runtime-data"
    monkeypatch.setenv("QSX_DATA_DIR", str(root))
    demo = RuntimePaths.resolve("demo", instance_id="gold-main")
    test = RuntimePaths.resolve("test", instance_id="gold-main")

    assert demo.instance_root == root / "demo" / "gold-main"
    assert test.instance_root == root / "test" / "gold-main"
    assert demo.instance_root != test.instance_root
    assert not root.exists(), "path resolution/import must not create state"


def test_explicit_state_subdir_and_database_path_api(tmp_path):
    paths = RuntimePaths.resolve("test", data_dir=tmp_path, instance_id="worker-1")
    assert paths.state_subdir("legacy") == paths.state_dir / "legacy"
    assert paths.database_path("trade_ledger") == paths.database_dir / "trade_ledger.db"
    assert (
        paths.database_path("risk_ledger.db", namespace="legacy")
        == paths.state_dir / "legacy" / "risk_ledger.db"
    )
    assert not paths.instance_root.exists()

    paths.ensure_layout()
    created = paths.state_subdir("journals", create=True)
    db = paths.database_path("agent_04", namespace="journals", create_parent=True)
    assert created.is_dir()
    assert db.parent == created
    assert not db.exists()


def test_paths_reject_traversal_and_broker_cleanup(tmp_path):
    paths = RuntimePaths.resolve("demo", data_dir=tmp_path)
    with pytest.raises(RuntimePathError):
        paths.state_subdir("../journal_db_sim")
    with pytest.raises(RuntimePathError):
        paths.database_path("../../risk")
    with pytest.raises(RuntimePathError, match="outside managed"):
        paths.assert_managed_path(tmp_path / "elsewhere")
    with pytest.raises(RuntimePathError, match="cleanup is forbidden"):
        paths.assert_cleanup_allowed(paths.state_dir / "journals")


def test_process_lock_excludes_second_runtime_and_is_reusable(tmp_path):
    paths = RuntimePaths.resolve("test", data_dir=tmp_path).ensure_layout()
    first = paths.process_lock()
    second = paths.process_lock()

    first.acquire()
    assert first.acquired
    assert Path(first.path).read_text(encoding="utf-8")
    with pytest.raises(ProcessLockError, match="another runtime owns"):
        second.acquire()
    first.release()
    assert not first.acquired

    with paths.process_lock() as third:
        assert third.acquired


def test_ownership_lock_contends_across_modes_and_instance_ids(tmp_path):
    demo = RuntimePaths.resolve(
        "demo", data_dir=tmp_path / "runtime", instance_id="forward-a"
    )
    shadow = RuntimePaths.resolve(
        "shadow", data_dir=tmp_path / "runtime", instance_id="observer-b"
    )
    identity = {
        "account_id": "123456",
        "broker_server": "Broker-Demo",
        "symbol": "XAUUSD",
        "magic": 202412,
    }
    first = demo.ownership_lock(**identity)
    second = shadow.ownership_lock(**identity)

    assert first.path == second.path
    assert first.path.parent == tmp_path / "runtime" / "ownership"
    first.acquire()
    try:
        with pytest.raises(ProcessLockError, match="another runtime owns"):
            second.acquire()
    finally:
        first.release()

    different_magic = shadow.ownership_lock(**{**identity, "magic": 202413})
    assert different_magic.path != first.path


@pytest.mark.parametrize(
    "changes",
    [
        {"account_id": ""},
        {"broker_server": "bad\nserver"},
        {"symbol": ""},
        {"magic": 0},
        {"magic": True},
    ],
)
def test_ownership_lock_rejects_ambiguous_identity(tmp_path, changes):
    paths = RuntimePaths.resolve("demo", data_dir=tmp_path)
    identity = {
        "account_id": "123456",
        "broker_server": "Broker-Demo",
        "symbol": "XAUUSD",
        "magic": 202412,
    }
    with pytest.raises(RuntimePathError):
        paths.ownership_lock(**{**identity, **changes})
