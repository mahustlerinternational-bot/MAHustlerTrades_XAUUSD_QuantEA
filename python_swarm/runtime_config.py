"""Strict, side-effect-free runtime configuration for QUANT-SWARM-XAU.

Configuration parsing is intentionally fail closed: the execution mode and
schema version are mandatory, unknown keys are rejected, broker risk limits are
bounded, and provider-dependent agents cannot be enabled without a genuine data
source.  Reading a file is an explicit function call; importing this module does
not access the filesystem or environment.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

try:  # Support both package imports and the repository's legacy flat imports.
    from .agent_roster import (
        AGENT_BY_ID,
        CORE_RUNTIME_AGENT_IDS,
        STRATEGY_AGENT_IDS,
        enabled_dependency_errors,
    )
except ImportError:  # pragma: no cover - exercised by legacy launch paths
    from agent_roster import (  # type: ignore
        AGENT_BY_ID,
        CORE_RUNTIME_AGENT_IDS,
        STRATEGY_AGENT_IDS,
        enabled_dependency_errors,
    )


try:
    from .version import CONFIG_SCHEMA_VERSION as CANONICAL_CONFIG_SCHEMA_VERSION
except ImportError:  # pragma: no cover - direct legacy import compatibility
    from version import CONFIG_SCHEMA_VERSION as CANONICAL_CONFIG_SCHEMA_VERSION  # type: ignore


CONFIG_SCHEMA_VERSION = CANONICAL_CONFIG_SCHEMA_VERSION
_ENV_REFERENCE = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$")
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_INSTANCE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_SYMBOL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,31}$")
_TELEGRAM_TOKEN = re.compile(r"^[0-9]{8,12}:[A-Za-z0-9_-]{25,}$")


class ConfigError(ValueError):
    """Configuration is absent, malformed, contradictory, or unsafe."""


class RuntimeMode(str, Enum):
    """Explicit RC2 capability modes; real-capital mode is not compiled in."""

    CHECK = "check"
    SHADOW = "shadow"
    DEMO = "demo"
    SIMULATION = "simulation"
    REPLAY = "replay"
    TEST = "test"

    @property
    def may_connect_broker(self) -> bool:
        return self in {RuntimeMode.SHADOW, RuntimeMode.DEMO}

    @property
    def may_execute_orders(self) -> bool:
        return self is RuntimeMode.DEMO

    @property
    def may_use_synthetic_data(self) -> bool:
        return self in {RuntimeMode.SIMULATION, RuntimeMode.TEST}


class ProviderSource(str, Enum):
    DISABLED = "disabled"
    BROKER = "broker"
    HTTP = "http"
    FILE = "file"
    SYNTHETIC = "synthetic"


class AccountTradeMode(str, Enum):
    DEMO = "DEMO"
    REAL = "REAL"


@dataclass(frozen=True, slots=True)
class SystemSettings:
    symbol: str = "XAUUSD"
    magic_number: int = 202412
    instance_id: str = "default"
    log_level: str = "INFO"
    expected_account_id: str | None = None
    expected_broker_server: str | None = None
    expected_account_trade_mode: AccountTradeMode | None = None


@dataclass(frozen=True, slots=True)
class IPCSettings:
    host: str = "127.0.0.1"
    port: int = 9876
    heartbeat_timeout_s: float = 15.0
    max_message_bytes: int = 16_384
    auth_secret_env: str | None = None


@dataclass(frozen=True, slots=True)
class RiskSettings:
    max_risk_pct: float = 1.0
    daily_max_drawdown_pct: float = 3.0
    max_total_risk_pct: float = 1.0
    max_open_positions: int = 1
    consecutive_loss_halt: int = 3


@dataclass(frozen=True, slots=True)
class ConfluenceSettings:
    base_threshold_pct: float = 75.0
    min_agents_agreeing: int = 3
    hard_floor_pct: float = 68.0
    hard_ceiling_pct: float = 90.0


@dataclass(frozen=True, slots=True)
class EntrySettings:
    """Runtime-owned entry/execution envelope limits.

    These values were previously implicit policy defaults, which made an
    effective configuration hash insufficient to reproduce a broker decision.
    They are now part of the strict schema-v2 semantic configuration.
    """

    maximum_slippage_points: int = 100
    max_spread_points: float = 300.0
    intent_ttl_ms: int = 10_000
    reservation_ttl_ms: int = 30_000
    maximum_snapshot_age_ms: int = 5_000


@dataclass(frozen=True, slots=True)
class ProviderSettings:
    enabled: bool = False
    source: ProviderSource = ProviderSource.DISABLED
    max_age_s: float = 300.0
    credential_env: str | None = None
    url: str | None = None


@dataclass(frozen=True, slots=True)
class ProvidersSettings:
    macro: ProviderSettings = field(default_factory=ProviderSettings)
    intermarket: ProviderSettings = field(default_factory=ProviderSettings)
    unsafe_allow_technical_without_macro: bool = False

    def get(self, key: str) -> ProviderSettings:
        if key == "macro":
            return self.macro
        if key == "intermarket":
            return self.intermarket
        raise KeyError(f"unknown provider key: {key!r}")


@dataclass(frozen=True, slots=True)
class IntegrationsSettings:
    telegram: bool = False
    discord: bool = False
    website: bool = False


@dataclass(frozen=True, slots=True)
class StorageSettings:
    data_dir: str | None = None
    sqlite_busy_timeout_ms: int = 5_000


DEFAULT_ENABLED_AGENT_IDS = frozenset(
    {
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        11,
        13,
        16,
        17,
        18,
        19,
        20,
        21,
    }
)


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    schema_version: int
    mode: RuntimeMode
    system: SystemSettings = field(default_factory=SystemSettings)
    ipc: IPCSettings = field(default_factory=IPCSettings)
    risk: RiskSettings = field(default_factory=RiskSettings)
    confluence: ConfluenceSettings = field(default_factory=ConfluenceSettings)
    entry: EntrySettings = field(default_factory=EntrySettings)
    providers: ProvidersSettings = field(default_factory=ProvidersSettings)
    integrations: IntegrationsSettings = field(default_factory=IntegrationsSettings)
    storage: StorageSettings = field(default_factory=StorageSettings)
    enabled_agent_ids: frozenset[int] = DEFAULT_ENABLED_AGENT_IDS

    @property
    def execution_enabled(self) -> bool:
        return self.mode.may_execute_orders

    def is_agent_enabled(self, agent_id: int) -> bool:
        return agent_id in self.enabled_agent_ids

    @classmethod
    def from_mapping(
        cls,
        raw: Mapping[str, Any],
        *,
        environ: Mapping[str, str] | None = None,
    ) -> "RuntimeConfig":
        env = os.environ if environ is None else environ
        data = _expand_environment(_require_mapping(raw, "config"), env, "config")
        _reject_unknown(
            data,
            {
                "schema_version",
                "mode",
                "system",
                "ipc",
                "risk",
                "confluence",
                "entry",
                "providers",
                "integrations",
                "storage",
                "agents",
            },
            "config",
        )

        if "schema_version" not in data:
            raise ConfigError("config.schema_version is required")
        version = _integer(data["schema_version"], "config.schema_version")
        if version != CONFIG_SCHEMA_VERSION:
            raise ConfigError(
                f"unsupported config schema_version {version}; expected {CONFIG_SCHEMA_VERSION}"
            )

        if "mode" not in data:
            raise ConfigError(
                "config.mode is required; broker capability is never implicit"
            )
        mode = _enum(RuntimeMode, data["mode"], "config.mode")

        config = cls(
            schema_version=version,
            mode=mode,
            system=_parse_system(data.get("system", {})),
            ipc=_parse_ipc(data.get("ipc", {})),
            risk=_parse_risk(data.get("risk", {})),
            confluence=_parse_confluence(data.get("confluence", {})),
            entry=_parse_entry(data.get("entry", {})),
            providers=_parse_providers(data.get("providers", {})),
            integrations=_parse_integrations(data.get("integrations", {})),
            storage=_parse_storage(data.get("storage", {})),
            enabled_agent_ids=_parse_agents(data.get("agents", {})),
        )
        _validate_runtime(config, env)
        return config


def load_runtime_config(
    path: str | Path,
    *,
    environ: Mapping[str, str] | None = None,
) -> RuntimeConfig:
    """Read and strictly validate one JSON runtime configuration file."""

    config_path = Path(path).expanduser()
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigError(f"cannot read runtime config {config_path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(
            f"invalid JSON in runtime config {config_path}:{exc.lineno}:{exc.colno}: {exc.msg}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise ConfigError("runtime config root must be a JSON object")
    return RuntimeConfig.from_mapping(payload, environ=environ)


def _parse_system(raw: Any) -> SystemSettings:
    data = _require_mapping(raw, "config.system")
    _reject_unknown(
        data,
        {
            "symbol",
            "magic_number",
            "instance_id",
            "log_level",
            "expected_account_id",
            "expected_broker_server",
            "expected_account_trade_mode",
        },
        "config.system",
    )
    symbol = _string(data.get("symbol", "XAUUSD"), "config.system.symbol")
    if not _SYMBOL.fullmatch(symbol):
        raise ConfigError("config.system.symbol contains unsupported characters")
    magic = _integer(data.get("magic_number", 202412), "config.system.magic_number")
    if not 1 <= magic <= 2_147_483_647:
        raise ConfigError("config.system.magic_number must be between 1 and 2147483647")
    instance = _string(data.get("instance_id", "default"), "config.system.instance_id")
    if not _INSTANCE_ID.fullmatch(instance):
        raise ConfigError(
            "config.system.instance_id must be 1-64 safe filename characters"
        )
    level = _string(data.get("log_level", "INFO"), "config.system.log_level").upper()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ConfigError("config.system.log_level is invalid")
    account = _optional_string(
        data.get("expected_account_id"), "config.system.expected_account_id"
    )
    broker_server = _optional_string(
        data.get("expected_broker_server"), "config.system.expected_broker_server"
    )
    trade_mode_raw = data.get("expected_account_trade_mode")
    trade_mode = (
        None
        if trade_mode_raw is None
        else _enum(
            AccountTradeMode,
            trade_mode_raw,
            "config.system.expected_account_trade_mode",
        )
    )
    for value, path in (
        (account, "config.system.expected_account_id"),
        (broker_server, "config.system.expected_broker_server"),
    ):
        if value is not None and (len(value) > 128 or any(ord(char) < 32 for char in value)):
            raise ConfigError(f"{path} contains unsupported characters")
    return SystemSettings(
        symbol=symbol,
        magic_number=magic,
        instance_id=instance,
        log_level=level,
        expected_account_id=account,
        expected_broker_server=broker_server,
        expected_account_trade_mode=trade_mode,
    )


def _parse_ipc(raw: Any) -> IPCSettings:
    data = _require_mapping(raw, "config.ipc")
    _reject_unknown(
        data,
        {
            "host",
            "port",
            "heartbeat_timeout_s",
            "max_message_bytes",
            "auth_secret_env",
        },
        "config.ipc",
    )
    host = _string(data.get("host", "127.0.0.1"), "config.ipc.host")
    port = _integer(data.get("port", 9876), "config.ipc.port")
    heartbeat = _number(
        data.get("heartbeat_timeout_s", 15.0), "config.ipc.heartbeat_timeout_s"
    )
    max_bytes = _integer(
        data.get("max_message_bytes", 16_384), "config.ipc.max_message_bytes"
    )
    auth_env = _optional_string(data.get("auth_secret_env"), "config.ipc.auth_secret_env")
    if not 1 <= port <= 65_535:
        raise ConfigError("config.ipc.port must be between 1 and 65535")
    if not 3.0 <= heartbeat <= 300.0:
        raise ConfigError("config.ipc.heartbeat_timeout_s must be between 3 and 300")
    if not 1_024 <= max_bytes <= 16_384:
        raise ConfigError("config.ipc.max_message_bytes must be between 1024 and 16384")
    if auth_env is not None and not _ENV_NAME.fullmatch(auth_env):
        raise ConfigError("config.ipc.auth_secret_env must be an environment variable name")
    return IPCSettings(host, port, heartbeat, max_bytes, auth_env)


def _parse_risk(raw: Any) -> RiskSettings:
    data = _require_mapping(raw, "config.risk")
    _reject_unknown(
        data,
        {
            "max_risk_pct",
            "daily_max_drawdown_pct",
            "max_total_risk_pct",
            "max_open_positions",
            "consecutive_loss_halt",
        },
        "config.risk",
    )
    max_risk = _number(data.get("max_risk_pct", 1.0), "config.risk.max_risk_pct")
    max_dd = _number(
        data.get("daily_max_drawdown_pct", 3.0),
        "config.risk.daily_max_drawdown_pct",
    )
    max_total_risk = _number(
        data.get("max_total_risk_pct", 1.0),
        "config.risk.max_total_risk_pct",
    )
    max_positions = _integer(
        data.get("max_open_positions", 1), "config.risk.max_open_positions"
    )
    loss_halt = _integer(
        data.get("consecutive_loss_halt", 3), "config.risk.consecutive_loss_halt"
    )
    if not 0.0 < max_risk <= 1.0:
        raise ConfigError("config.risk.max_risk_pct must be > 0 and <= the 1% hard cap")
    if not 0.0 < max_dd <= 3.0:
        raise ConfigError(
            "config.risk.daily_max_drawdown_pct must be > 0 and <= the 3% hard cap"
        )
    if not 0.0 < max_total_risk <= 3.0:
        raise ConfigError(
            "config.risk.max_total_risk_pct must be > 0 and <= the 3% hard cap"
        )
    if max_total_risk < max_risk:
        raise ConfigError(
            "config.risk.max_total_risk_pct cannot be below max_risk_pct"
        )
    if not 1 <= max_positions <= 3:
        raise ConfigError(
            "config.risk.max_open_positions must be 1-3; production default is 1"
        )
    if not 1 <= loss_halt <= 10:
        raise ConfigError("config.risk.consecutive_loss_halt must be between 1 and 10")
    return RiskSettings(max_risk, max_dd, max_total_risk, max_positions, loss_halt)


def _parse_confluence(raw: Any) -> ConfluenceSettings:
    data = _require_mapping(raw, "config.confluence")
    _reject_unknown(
        data,
        {
            "base_threshold_pct",
            "min_agents_agreeing",
            "hard_floor_pct",
            "hard_ceiling_pct",
        },
        "config.confluence",
    )
    base = _number(
        data.get("base_threshold_pct", 75.0),
        "config.confluence.base_threshold_pct",
    )
    quorum = _integer(
        data.get("min_agents_agreeing", 3),
        "config.confluence.min_agents_agreeing",
    )
    floor = _number(
        data.get("hard_floor_pct", 68.0), "config.confluence.hard_floor_pct"
    )
    ceiling = _number(
        data.get("hard_ceiling_pct", 90.0), "config.confluence.hard_ceiling_pct"
    )
    if not 68.0 <= floor <= 90.0:
        raise ConfigError(
            "config.confluence.hard_floor_pct must be between the 68% hard floor and 90%"
        )
    if not floor <= base <= ceiling <= 100.0:
        raise ConfigError(
            "config.confluence thresholds must satisfy floor <= base <= ceiling <= 100"
        )
    if not 3 <= quorum <= len(STRATEGY_AGENT_IDS):
        raise ConfigError("config.confluence.min_agents_agreeing must be between 3 and 10")
    return ConfluenceSettings(base, quorum, floor, ceiling)


def _parse_entry(raw: Any) -> EntrySettings:
    data = _require_mapping(raw, "config.entry")
    _reject_unknown(
        data,
        {
            "maximum_slippage_points",
            "max_spread_points",
            "intent_ttl_ms",
            "reservation_ttl_ms",
            "maximum_snapshot_age_ms",
        },
        "config.entry",
    )
    slippage = _integer(
        data.get("maximum_slippage_points", 100),
        "config.entry.maximum_slippage_points",
    )
    spread = _number(
        data.get("max_spread_points", 300.0),
        "config.entry.max_spread_points",
    )
    intent_ttl = _integer(
        data.get("intent_ttl_ms", 10_000), "config.entry.intent_ttl_ms"
    )
    reservation_ttl = _integer(
        data.get("reservation_ttl_ms", 30_000),
        "config.entry.reservation_ttl_ms",
    )
    snapshot_age = _integer(
        data.get("maximum_snapshot_age_ms", 5_000),
        "config.entry.maximum_snapshot_age_ms",
    )
    if not 0 <= slippage <= 10_000:
        raise ConfigError(
            "config.entry.maximum_slippage_points must be between 0 and 10000"
        )
    if not 0.0 < spread <= 10_000.0:
        raise ConfigError("config.entry.max_spread_points must be > 0 and <= 10000")
    for name, value in (
        ("intent_ttl_ms", intent_ttl),
        ("reservation_ttl_ms", reservation_ttl),
        ("maximum_snapshot_age_ms", snapshot_age),
    ):
        if not 250 <= value <= 300_000:
            raise ConfigError(f"config.entry.{name} must be between 250 and 300000")
    if reservation_ttl < intent_ttl:
        raise ConfigError(
            "config.entry.reservation_ttl_ms cannot be shorter than intent_ttl_ms"
        )
    return EntrySettings(slippage, spread, intent_ttl, reservation_ttl, snapshot_age)


def _parse_provider(raw: Any, path: str) -> ProviderSettings:
    data = _require_mapping(raw, path)
    _reject_unknown(
        data,
        {"enabled", "source", "max_age_s", "credential_env", "url"},
        path,
    )
    enabled = _boolean(data.get("enabled", False), f"{path}.enabled")
    source = _enum(
        ProviderSource, data.get("source", "disabled"), f"{path}.source"
    )
    max_age = _number(data.get("max_age_s", 300.0), f"{path}.max_age_s")
    credential_env = _optional_string(data.get("credential_env"), f"{path}.credential_env")
    url = _optional_string(data.get("url"), f"{path}.url")
    if not 1.0 <= max_age <= 86_400.0:
        raise ConfigError(f"{path}.max_age_s must be between 1 and 86400")
    if enabled == (source is ProviderSource.DISABLED):
        raise ConfigError(
            f"{path}.enabled and source disagree; disabled providers must use source='disabled'"
        )
    if credential_env is not None and not _ENV_NAME.fullmatch(credential_env):
        raise ConfigError(f"{path}.credential_env must be an environment variable name")
    if source is ProviderSource.HTTP:
        parsed = urlparse(url or "")
        if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
            raise ConfigError(f"{path}.url must be an HTTPS URL without embedded credentials")
    elif url is not None:
        raise ConfigError(f"{path}.url is only valid for source='http'")
    return ProviderSettings(enabled, source, max_age, credential_env, url)


def _parse_providers(raw: Any) -> ProvidersSettings:
    data = _require_mapping(raw, "config.providers")
    _reject_unknown(
        data,
        {"macro", "intermarket", "unsafe_allow_technical_without_macro"},
        "config.providers",
    )
    return ProvidersSettings(
        macro=_parse_provider(data.get("macro", {}), "config.providers.macro"),
        intermarket=_parse_provider(
            data.get("intermarket", {}), "config.providers.intermarket"
        ),
        unsafe_allow_technical_without_macro=_boolean(
            data.get("unsafe_allow_technical_without_macro", False),
            "config.providers.unsafe_allow_technical_without_macro",
        ),
    )


def _parse_storage(raw: Any) -> StorageSettings:
    data = _require_mapping(raw, "config.storage")
    _reject_unknown(data, {"data_dir", "sqlite_busy_timeout_ms"}, "config.storage")
    data_dir = _optional_string(data.get("data_dir"), "config.storage.data_dir")
    timeout = _integer(
        data.get("sqlite_busy_timeout_ms", 5_000),
        "config.storage.sqlite_busy_timeout_ms",
    )
    if not 100 <= timeout <= 120_000:
        raise ConfigError("config.storage.sqlite_busy_timeout_ms must be 100-120000")
    return StorageSettings(data_dir, timeout)


def _parse_integrations(raw: Any) -> IntegrationsSettings:
    data = _require_mapping(raw, "config.integrations")
    _reject_unknown(data, {"telegram", "discord", "website"}, "config.integrations")
    return IntegrationsSettings(
        telegram=_boolean(data.get("telegram", False), "config.integrations.telegram"),
        discord=_boolean(data.get("discord", False), "config.integrations.discord"),
        website=_boolean(data.get("website", False), "config.integrations.website"),
    )


def _parse_agents(raw: Any) -> frozenset[int]:
    data = _require_mapping(raw, "config.agents")
    _reject_unknown(data, {"enabled"}, "config.agents")
    if "enabled" not in data:
        return DEFAULT_ENABLED_AGENT_IDS
    value = data["enabled"]
    if not isinstance(value, list):
        raise ConfigError("config.agents.enabled must be a JSON array of integer IDs")
    enabled: list[int] = []
    for index, agent_id in enumerate(value):
        enabled.append(_integer(agent_id, f"config.agents.enabled[{index}]"))
    if len(enabled) != len(set(enabled)):
        raise ConfigError("config.agents.enabled contains duplicate IDs")
    unknown = set(enabled) - set(AGENT_BY_ID)
    if unknown:
        raise ConfigError(f"config.agents.enabled contains unknown IDs: {sorted(unknown)}")
    return frozenset(enabled)


def _validate_runtime(config: RuntimeConfig, environ: Mapping[str, str]) -> None:
    dependency_errors = enabled_dependency_errors(config.enabled_agent_ids)
    if dependency_errors:
        raise ConfigError("; ".join(dependency_errors))

    enabled_voters = config.enabled_agent_ids & set(STRATEGY_AGENT_IDS)
    if config.mode not in {RuntimeMode.CHECK, RuntimeMode.TEST}:
        if len(enabled_voters) < config.confluence.min_agents_agreeing:
            raise ConfigError(
                "enabled strategy agents cannot satisfy the confluence quorum"
            )

    if config.mode.may_connect_broker:
        missing_core = CORE_RUNTIME_AGENT_IDS - config.enabled_agent_ids
        if missing_core:
            raise ConfigError(
                f"broker modes require core runtime agents: missing {sorted(missing_core)}"
            )
        if config.storage.data_dir is not None and not Path(
            config.storage.data_dir
        ).expanduser().is_absolute():
            raise ConfigError("broker modes require an absolute config.storage.data_dir")
        if config.storage.data_dir is None:
            raise ConfigError("broker modes require config.storage.data_dir")

        system = config.system
        missing_identity = [
            name
            for name, value in (
                ("expected_account_id", system.expected_account_id),
                ("expected_broker_server", system.expected_broker_server),
                ("expected_account_trade_mode", system.expected_account_trade_mode),
            )
            if value is None
        ]
        if missing_identity:
            raise ConfigError(
                "broker modes require pinned identity fields: "
                + ", ".join(missing_identity)
            )

        secret_name = config.ipc.auth_secret_env
        secret = environ.get(secret_name, "") if secret_name else ""
        if not secret_name or len(secret.encode("utf-8")) < 32:
            raise ConfigError(
                "broker modes require auth_secret_env with at least 32 UTF-8 bytes"
            )

        if system.expected_account_trade_mode is not AccountTradeMode.DEMO:
            raise ConfigError(
                f"{config.mode.value} mode requires a pinned DEMO account; "
                "REAL accounts are unsupported by this release"
            )

    if config.ipc.host != "127.0.0.1":
        raise ConfigError("config.ipc.host must be exactly 127.0.0.1")

    for provider_key, agent_id in (("macro", 14), ("intermarket", 15)):
        provider = config.providers.get(provider_key)
        agent_enabled = agent_id in config.enabled_agent_ids
        if provider.enabled != agent_enabled:
            raise ConfigError(
                f"provider {provider_key!r} and agent {agent_id:02d} must be enabled or disabled together"
            )
        if provider.enabled:
            if provider.source in {
                ProviderSource.BROKER,
                ProviderSource.FILE,
            }:
                raise ConfigError(
                    f"provider {provider_key!r} selects "
                    f"source={provider.source.value!r} but "
                    "no canonical runtime adapter exists"
                )
            if config.mode.may_connect_broker and provider.source not in {
                ProviderSource.HTTP,
            }:
                raise ConfigError(
                    f"broker mode forbids {provider.source.value!r} {provider_key} data"
                )
            if provider.source is ProviderSource.SYNTHETIC and not config.mode.may_use_synthetic_data:
                raise ConfigError(
                    f"synthetic {provider_key} data is only allowed in simulation/test mode"
                )
            if provider.credential_env and not environ.get(provider.credential_env):
                raise ConfigError(
                    f"provider {provider_key!r} credential environment variable is unset"
                )

    if 10 in config.enabled_agent_ids and not config.providers.macro.enabled:
        raise ConfigError("Agent 10 requires a healthy configured macro provider")
    if 12 in config.enabled_agent_ids and not config.providers.intermarket.enabled:
        raise ConfigError("Agent 12 requires a healthy configured intermarket provider")

    unsafe_macro = config.providers.unsafe_allow_technical_without_macro
    if unsafe_macro and config.providers.macro.enabled:
        raise ConfigError(
            "unsafe technical-only macro override requires the macro provider and Agents 10/14 to be disabled"
        )
    if unsafe_macro and ({10, 14} & config.enabled_agent_ids):
        raise ConfigError(
            "unsafe technical-only macro override requires Agents 10 and 14 to be disabled"
        )
    if config.mode.may_execute_orders and not config.providers.macro.enabled and not unsafe_macro:
        raise ConfigError(
            "order-executing modes require macro protection or the explicit unsafe technical-only override"
        )

    integrations = config.integrations
    if integrations.telegram:
        token = environ.get("QSX_TG_TOKEN", "")
        chat_id = environ.get("QSX_TG_CHAT", "")
        if not _TELEGRAM_TOKEN.fullmatch(token) or not chat_id.strip():
            raise ConfigError(
                "enabled Telegram integration requires valid QSX_TG_TOKEN and QSX_TG_CHAT"
            )
    if integrations.discord:
        discord_url = environ.get("QSX_DISCORD_WEBHOOK_URL", "")
        parsed = _parsed_https_url(discord_url)
        if parsed is None or parsed.hostname not in {"discord.com", "discordapp.com"}:
            raise ConfigError(
                "enabled Discord integration requires an official HTTPS webhook URL"
            )
    if integrations.website:
        website_url = environ.get("QSX_WEBSITE_EVENT_URL", "")
        signing_secret = environ.get("QSX_WEBSITE_EVENT_SECRET", "")
        if _parsed_https_url(website_url) is None or len(
            signing_secret.encode("utf-8")
        ) < 32:
            raise ConfigError(
                "enabled website integration requires an HTTPS URL and a 32-byte signing secret"
            )


def _parsed_https_url(value: str):
    if not isinstance(value, str) or not value.strip():
        return None
    parsed = urlparse(value.strip())
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        return None
    return parsed


def _expand_environment(
    value: Any, environ: Mapping[str, str], path: str
) -> Any:
    if isinstance(value, Mapping):
        return {
            key: _expand_environment(child, environ, f"{path}.{key}")
            for key, child in value.items()
        }
    if isinstance(value, list):
        return [
            _expand_environment(child, environ, f"{path}[{index}]")
            for index, child in enumerate(value)
        ]
    if isinstance(value, str):
        match = _ENV_REFERENCE.fullmatch(value)
        if match:
            name = match.group(1)
            if name not in environ or environ[name] == "":
                raise ConfigError(f"{path} references missing environment variable {name}")
            return environ[name]
    return value


def _require_mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfigError(f"{path} must be an object")
    if not all(isinstance(key, str) for key in value):
        raise ConfigError(f"{path} keys must be strings")
    return value


def _reject_unknown(data: Mapping[str, Any], allowed: set[str], path: str) -> None:
    unknown = set(data) - allowed
    if unknown:
        raise ConfigError(f"{path} contains unknown keys: {sorted(unknown)}")


def _string(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{path} must be a non-empty string")
    return value.strip()


def _optional_string(value: Any, path: str) -> str | None:
    if value is None:
        return None
    return _string(value, path)


def _boolean(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        raise ConfigError(f"{path} must be a boolean")
    return value


def _integer(value: Any, path: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{path} must be an integer")
    return value


def _number(value: Any, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{path} must be a number")
    result = float(value)
    if result != result or result in {float("inf"), float("-inf")}:
        raise ConfigError(f"{path} must be finite")
    return result


def _enum(enum_type: type[Enum], value: Any, path: str):
    if not isinstance(value, str):
        raise ConfigError(f"{path} must be a string")
    try:
        return enum_type(value)
    except ValueError as exc:
        allowed = [item.value for item in enum_type]
        raise ConfigError(f"{path} must be one of {allowed}") from exc


__all__ = [
    "AccountTradeMode",
    "CONFIG_SCHEMA_VERSION",
    "DEFAULT_ENABLED_AGENT_IDS",
    "ConfigError",
    "ConfluenceSettings",
    "EntrySettings",
    "IPCSettings",
    "IntegrationsSettings",
    "ProviderSettings",
    "ProviderSource",
    "ProvidersSettings",
    "RiskSettings",
    "RuntimeConfig",
    "RuntimeMode",
    "StorageSettings",
    "SystemSettings",
    "load_runtime_config",
]
