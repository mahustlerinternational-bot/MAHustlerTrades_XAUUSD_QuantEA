import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from dashboard import (
    DashboardConfig,
    is_bearer_authorized,
    is_loopback_host,
    load_projection,
    projection_readiness,
)


def config(path: Path, **overrides):
    values = {
        "host": "127.0.0.1",
        "port": 8088,
        "projection_path": path,
        "bearer_token": "",
        "allowed_origin": "",
        "ready_max_age_s": 15.0,
    }
    values.update(overrides)
    return DashboardConfig(**values)


def test_default_configuration_is_loopback_and_uses_configured_state_root(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("QSX_STATE_DIR", str(tmp_path))
    for key in (
        "QSX_DASH_HOST",
        "QSX_DASH_PORT",
        "QSX_PROJECTION_PATH",
        "QSX_DASH_TOKEN",
        "QSX_DASH_ALLOWED_ORIGIN",
        "QSX_DASH_READY_MAX_AGE_S",
    ):
        monkeypatch.delenv(key, raising=False)

    selected = DashboardConfig.from_env()

    assert selected.host == "127.0.0.1"
    assert selected.exposed is False
    assert selected.projection_path == (tmp_path / "live_state.json").resolve()
    selected.validate()


@pytest.mark.parametrize("host", ["127.0.0.1", "::1", "localhost"])
def test_loopback_detection_is_explicit(host):
    assert is_loopback_host(host)


@pytest.mark.parametrize("host", ["0.0.0.0", "::", "dashboard.internal", ""])
def test_non_loopback_bind_requires_bearer_token(tmp_path, host):
    selected = config(tmp_path / "state.json", host=host)

    with pytest.raises(RuntimeError, match="QSX_DASH_TOKEN"):
        selected.validate()


def test_bearer_auth_uses_header_and_exposed_config_can_validate(tmp_path):
    selected = config(
        tmp_path / "state.json", host="0.0.0.0", bearer_token="unit-token"
    )
    selected.validate()

    assert is_bearer_authorized(selected, "Bearer unit-token") is True
    assert is_bearer_authorized(selected, "Bearer wrong") is False
    assert is_bearer_authorized(selected, "") is False
    assert is_bearer_authorized(
        config(tmp_path / "state.json"), ""
    ) is True


def test_cors_rejects_wildcard_and_non_origin_values(tmp_path):
    with pytest.raises(ValueError, match="wildcard"):
        config(tmp_path / "state.json", allowed_origin="*").validate()
    with pytest.raises(ValueError, match="explicit HTTP"):
        config(
            tmp_path / "state.json", allowed_origin="https://example.test/path"
        ).validate()

    config(
        tmp_path / "state.json", allowed_origin="https://example.test"
    ).validate()


def test_projection_loader_returns_generic_reasons(tmp_path):
    missing = tmp_path / "missing.json"
    assert load_projection(missing) == (None, "projection_missing")

    invalid = tmp_path / "invalid.json"
    invalid.write_text("not json", encoding="utf-8")
    assert load_projection(invalid) == (None, "projection_unreadable")

    array = tmp_path / "array.json"
    array.write_text("[]", encoding="utf-8")
    assert load_projection(array) == (None, "projection_invalid")

    non_finite = tmp_path / "non-finite.json"
    non_finite.write_text('{"equity":NaN}', encoding="utf-8")
    assert load_projection(non_finite) == (None, "projection_unreadable")


def test_projection_allowlist_never_serves_credentials(tmp_path):
    path = tmp_path / "state.json"
    path.write_text(json.dumps({
        "timestamp": 1_800_000_000,
        "equity": 1_000,
        "token": "must-not-leave-runtime",
        "broker_password": "must-not-leave-runtime",
    }), encoding="utf-8")

    projection, reason = load_projection(path)

    assert reason == "ok"
    assert projection == {"timestamp": 1_800_000_000, "equity": 1_000}


def test_readiness_requires_fresh_projection_and_runtime_ready(tmp_path):
    now = 1_800_000_000.0
    path = tmp_path / "state.json"
    selected = config(path, ready_max_age_s=15)

    path.write_text(json.dumps({
        "timestamp": datetime.fromtimestamp(now - 5, timezone.utc).isoformat(),
        "ea_connected": True,
    }), encoding="utf-8")
    ready, payload = projection_readiness(selected, now=now)
    assert ready is True
    assert payload == {"status": "ready", "age_s": 5.0}

    path.write_text(json.dumps({
        "timestamp": now - 16,
        "ea_connected": True,
    }), encoding="utf-8")
    ready, payload = projection_readiness(selected, now=now)
    assert ready is False
    assert payload["reason"] == "projection_stale"

    path.write_text(json.dumps({
        "timestamp": now,
        "ea_connected": False,
    }), encoding="utf-8")
    ready, payload = projection_readiness(selected, now=now)
    assert ready is False
    assert payload["reason"] == "runtime_not_ready"


def test_readiness_rejects_missing_or_far_future_timestamp(tmp_path):
    now = 1_800_000_000.0
    path = tmp_path / "state.json"
    selected = config(path)

    path.write_text('{"ready":true}', encoding="utf-8")
    assert projection_readiness(selected, now=now)[1]["reason"] == (
        "projection_timestamp_missing"
    )

    path.write_text(json.dumps({
        "timestamp": now + 31,
        "ready": True,
    }), encoding="utf-8")
    assert projection_readiness(selected, now=now)[1]["reason"] == (
        "projection_timestamp_future"
    )
