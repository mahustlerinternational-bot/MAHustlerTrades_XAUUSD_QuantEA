import json
from pathlib import Path

import pytest

from python_swarm.runtime_config import (
    AccountTradeMode,
    ConfigError,
    RuntimeConfig,
    RuntimeMode,
    load_runtime_config,
)


def minimal(mode="test"):
    return {"schema_version": 2, "mode": mode}


def broker_config(tmp_path, *, mode="demo", trade_mode="DEMO"):
    return {
        **minimal(mode),
        "system": {
            "expected_account_id": "123456",
            "expected_broker_server": "Broker-Demo",
            "expected_account_trade_mode": trade_mode,
        },
        "ipc": {"auth_secret_env": "QSX_IPC_SECRET"},
        "storage": {"data_dir": str(tmp_path)},
        "providers": {"unsafe_allow_technical_without_macro": True},
    }


BROKER_ENV = {"QSX_IPC_SECRET": "x" * 32}


def test_mode_and_schema_are_explicit_and_default_is_safe():
    with pytest.raises(ConfigError, match="schema_version is required"):
        RuntimeConfig.from_mapping({"mode": "test"})
    with pytest.raises(ConfigError, match="mode is required"):
        RuntimeConfig.from_mapping({"schema_version": 2})

    config = RuntimeConfig.from_mapping(minimal())
    assert config.mode is RuntimeMode.TEST
    assert not config.execution_enabled
    assert not config.providers.macro.enabled
    assert not config.providers.intermarket.enabled
    assert not config.is_agent_enabled(10)
    assert not config.is_agent_enabled(12)
    assert config.confluence.min_agents_agreeing == 3


def test_unknown_keys_and_wrong_types_are_rejected():
    with pytest.raises(ConfigError, match="unknown keys"):
        RuntimeConfig.from_mapping({**minimal(), "surprise": True})
    with pytest.raises(ConfigError, match="unknown keys"):
        RuntimeConfig.from_mapping(
            {**minimal(), "system": {"permit_real_account": "false"}}
        )
    with pytest.raises(ConfigError, match="unknown keys"):
        RuntimeConfig.from_mapping(
            {**minimal(), "ipc": {"allow_remote": False}}
        )
    with pytest.raises(ConfigError, match="unknown keys"):
        RuntimeConfig.from_mapping(
            {**minimal(), "risk": {"initial_balance": 10_000}}
        )


@pytest.mark.parametrize(
    ("risk", "message"),
    [
        ({"max_risk_pct": 1.01}, "1% hard cap"),
        ({"daily_max_drawdown_pct": 3.01}, "3% hard cap"),
        ({"max_total_risk_pct": 3.01}, "3% hard cap"),
        ({"max_open_positions": 4}, "must be 1-3"),
    ],
)
def test_hard_risk_limits_cannot_be_overridden(risk, message):
    with pytest.raises(ConfigError, match=message):
        RuntimeConfig.from_mapping({**minimal(), "risk": risk})


def test_confluence_floor_and_three_agent_quorum_are_hard_limits():
    with pytest.raises(ConfigError, match="68% hard floor"):
        RuntimeConfig.from_mapping(
            {**minimal(), "confluence": {"hard_floor_pct": 67.9}}
        )
    with pytest.raises(ConfigError, match="between 3 and 10"):
        RuntimeConfig.from_mapping(
            {**minimal(), "confluence": {"min_agents_agreeing": 2}}
        )


def test_provider_agents_fail_closed_without_real_providers(tmp_path):
    enabled = [1, 2, 3, 4, 5, 6, 10, 14, 16, 17, 18, 19, 20, 21]
    with pytest.raises(ConfigError, match="provider 'macro'.*enabled or disabled together"):
        RuntimeConfig.from_mapping(
            {
                **broker_config(tmp_path),
                "agents": {"enabled": enabled},
                "providers": {},
            },
            environ=BROKER_ENV,
        )

    synthetic = {
        "enabled": True,
        "source": "synthetic",
        "max_age_s": 60,
    }
    with pytest.raises(ConfigError, match="broker mode forbids"):
        RuntimeConfig.from_mapping(
            {
                **broker_config(tmp_path),
                "agents": {"enabled": enabled},
                "providers": {"macro": synthetic},
            },
            environ=BROKER_ENV,
        )


@pytest.mark.parametrize("provider_key", ["macro", "intermarket"])
@pytest.mark.parametrize("source", ["broker", "file"])
def test_unimplemented_provider_sources_are_rejected_before_runtime_construction(
    tmp_path,
    provider_key,
    source,
):
    provider = {"enabled": True, "source": source, "max_age_s": 30}
    enabled = [
        agent_id
        for agent_id in range(1, 22)
        if agent_id not in {10, 12, 14, 15}
    ]
    enabled.append(14 if provider_key == "macro" else 15)
    with pytest.raises(ConfigError, match="no canonical runtime adapter"):
        RuntimeConfig.from_mapping(
            {
                **broker_config(tmp_path),
                "agents": {"enabled": enabled},
                "providers": {provider_key: provider},
            },
            environ=BROKER_ENV,
        )


def test_broker_ipc_is_loopback_only_and_requires_strong_auth(tmp_path):
    raw = broker_config(tmp_path, mode="shadow")
    raw["ipc"] = {
        "host": "10.0.0.5",
        "auth_secret_env": "QSX_IPC_SECRET",
    }
    with pytest.raises(ConfigError, match="exactly 127.0.0.1"):
        RuntimeConfig.from_mapping(raw, environ=BROKER_ENV)

    raw["ipc"]["host"] = "127.0.0.1"
    with pytest.raises(ConfigError, match="at least 32"):
        RuntimeConfig.from_mapping(raw, environ={"QSX_IPC_SECRET": "too-short"})


def test_real_account_and_live_mode_are_not_available_in_this_release(tmp_path):
    with pytest.raises(ConfigError, match="config.mode"):
        RuntimeConfig.from_mapping(
            broker_config(tmp_path, mode="live", trade_mode="REAL"),
            environ=BROKER_ENV,
        )
    with pytest.raises(ConfigError, match="REAL accounts are unsupported"):
        RuntimeConfig.from_mapping(
            broker_config(tmp_path, mode="demo", trade_mode="REAL"),
            environ=BROKER_ENV,
        )


def test_demo_requires_macro_protection_or_visible_unsafe_override(tmp_path):
    raw = broker_config(tmp_path)
    raw["providers"] = {}
    with pytest.raises(ConfigError, match="macro protection"):
        RuntimeConfig.from_mapping(raw, environ=BROKER_ENV)

    raw["providers"] = {"unsafe_allow_technical_without_macro": True}
    config = RuntimeConfig.from_mapping(raw, environ=BROKER_ENV)
    assert config.providers.unsafe_allow_technical_without_macro


def test_enabled_integrations_require_external_credentials_without_storing_them():
    raw = {**minimal(), "integrations": {"telegram": True}}
    with pytest.raises(ConfigError, match="Telegram integration"):
        RuntimeConfig.from_mapping(raw, environ={})

    environment = {
        "QSX_TG_TOKEN": "123456789:" + "a" * 30,
        "QSX_TG_CHAT": "-100123456",
        "QSX_DISCORD_WEBHOOK_URL": "https://discord.com/api/webhooks/1/test",
        "QSX_WEBSITE_EVENT_URL": "https://events.example.invalid/quant-swarm",
        "QSX_WEBSITE_EVENT_SECRET": "s" * 32,
    }
    config = RuntimeConfig.from_mapping(
        {
            **minimal(),
            "integrations": {
                "telegram": True,
                "discord": True,
                "website": True,
            },
        },
        environ=environment,
    )
    assert config.integrations.telegram
    assert config.integrations.discord
    assert config.integrations.website
    assert not hasattr(config.integrations, "telegram_token")


def test_load_config_expands_only_declared_environment_references(tmp_path):
    path = tmp_path / "runtime.json"
    path.write_text(
        json.dumps(
            {
                **minimal(),
                "storage": {"data_dir": "${QSX_DATA_DIR}"},
            }
        ),
        encoding="utf-8",
    )
    config = load_runtime_config(path, environ={"QSX_DATA_DIR": str(tmp_path / "state")})
    assert config.storage.data_dir == str(tmp_path / "state")
    with pytest.raises(ConfigError, match="missing environment variable"):
        load_runtime_config(path, environ={})


def test_committed_configs_use_the_canonical_schema_and_never_execute_by_default(tmp_path):
    root = Path(__file__).resolve().parents[1]
    environment = {"QSX_DATA_DIR": str(tmp_path / "state")}
    for name in ("config.check.example.json", "config.small-account.json"):
        config = load_runtime_config(root / "deployment" / name, environ=environment)
        assert config.schema_version == 2
        assert config.mode is RuntimeMode.CHECK
        assert not config.execution_enabled


def test_demo_example_is_valid_only_with_explicit_broker_identity_and_auth(tmp_path):
    root = Path(__file__).resolve().parents[1]
    path = root / "deployment" / "config.demo.example.json"
    environment = {
        "QSX_DATA_DIR": str(tmp_path / "state"),
        "QSX_EXPECTED_ACCOUNT_ID": "123456",
        "QSX_EXPECTED_BROKER_SERVER": "Broker-Demo",
        "QSX_IPC_AUTH_SECRET": "x" * 32,
        "QSX_MACRO_PROVIDER_URL": "https://providers.invalid/macro",
        "QSX_INTERMARKET_PROVIDER_URL": "https://providers.invalid/intermarket",
        "QSX_MACRO_PROVIDER_CREDENTIAL": "test-macro-credential",
        "QSX_INTERMARKET_PROVIDER_CREDENTIAL": "test-intermarket-credential",
    }
    config = load_runtime_config(path, environ=environment)
    assert config.mode is RuntimeMode.DEMO
    assert config.execution_enabled
    assert config.system.expected_account_id == "123456"


def test_missing_runtime_config_fails_closed(tmp_path):
    missing = tmp_path / "missing-runtime.json"

    with pytest.raises(ConfigError, match="cannot read runtime config"):
        load_runtime_config(missing, environ={})
