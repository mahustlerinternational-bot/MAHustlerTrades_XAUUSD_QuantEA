"""Canonical, machine-readable QUANT-SWARM-XAU 21-agent roster.

The roster represents logical system components, not operating-system
processes.  Infrastructure adapters such as MT5 transport, SQLite, Telegram,
Discord, and website publishing intentionally do not consume agent numbers.

This module performs no filesystem, database, network, or environment access at
import time.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Final, Iterable, Mapping


class AgentLayer(str, Enum):
    """Stable architectural layers used by documentation and health reports."""

    SYSTEM = "system"
    STRATEGY = "strategy"
    INTELLIGENCE = "intelligence"
    DECISION = "decision"
    CONTROL = "control"


@dataclass(frozen=True, slots=True)
class AgentSpec:
    """One logical agent and its stable integration contract metadata."""

    agent_id: int
    slug: str
    name: str
    layer: AgentLayer
    owner_module: str
    summary: str
    requires: tuple[int, ...] = ()
    consumes_from: tuple[int, ...] = ()
    provider_keys: tuple[str, ...] = ()
    votes_in_fusion: bool = False

    @property
    def display_id(self) -> str:
        return f"{self.agent_id:02d}"


_ROSTER: tuple[AgentSpec, ...] = (
    AgentSpec(
        1,
        "orchestrator",
        "Orchestrator",
        AgentLayer.SYSTEM,
        "python_swarm.runtime_app",
        "Supervises the single trading runtime and sequences the pipeline.",
    ),
    AgentSpec(
        2,
        "tick_streamer",
        "Tick Streamer",
        AgentLayer.SYSTEM,
        "python_swarm.broker_gateway",
        "Builds timestamped multi-timeframe market snapshots from broker data.",
    ),
    AgentSpec(
        3,
        "regime_detector",
        "Regime Detector",
        AgentLayer.SYSTEM,
        "python_swarm.strategy_pipeline",
        "Classifies the current market regime without overriding hard risk limits.",
        requires=(2,),
    ),
    AgentSpec(
        4,
        "htf_liquidity_trap",
        "HTF Liquidity Trap",
        AgentLayer.STRATEGY,
        "python_swarm.strategy_pipeline",
        "Detects higher-timeframe liquidity sweeps and rejection setups.",
        requires=(2,),
        consumes_from=(3,),
        votes_in_fusion=True,
    ),
    AgentSpec(
        5,
        "trendline_breakout",
        "Trendline Breakout",
        AgentLayer.STRATEGY,
        "python_swarm.strategy_pipeline",
        "Detects confirmed trendline breaks and retests.",
        requires=(2,),
        consumes_from=(3,),
        votes_in_fusion=True,
    ),
    AgentSpec(
        6,
        "crt_engine",
        "CRT Engine",
        AgentLayer.STRATEGY,
        "python_swarm.strategy_pipeline",
        "Evaluates Candle Range Theory sweep and expansion structures.",
        requires=(2,),
        consumes_from=(3,),
        votes_in_fusion=True,
    ),
    AgentSpec(
        7,
        "ict_smc_core",
        "ICT/SMC Core",
        AgentLayer.STRATEGY,
        "python_swarm.strategy_pipeline",
        "Detects SMC structure, order blocks, imbalances, and liquidity events.",
        requires=(2,),
        consumes_from=(3,),
        votes_in_fusion=True,
    ),
    AgentSpec(
        8,
        "vpvr_poc",
        "VPVR/POC",
        AgentLayer.STRATEGY,
        "python_swarm.strategy_pipeline",
        "Evaluates volume-profile and point-of-control setups.",
        requires=(2,),
        consumes_from=(3,),
        votes_in_fusion=True,
    ),
    AgentSpec(
        9,
        "asia_session_sweep",
        "Asia Session Sweep",
        AgentLayer.STRATEGY,
        "python_swarm.strategy_pipeline",
        "Detects London or New York sweeps of the Asia session range.",
        requires=(2,),
        consumes_from=(3,),
        votes_in_fusion=True,
    ),
    AgentSpec(
        10,
        "news_sentiment",
        "News/Sentiment",
        AgentLayer.STRATEGY,
        "python_swarm.strategy_pipeline",
        "Produces news-context signals only when authoritative macro data is healthy.",
        requires=(2, 14),
        consumes_from=(3,),
        provider_keys=("macro",),
        votes_in_fusion=True,
    ),
    AgentSpec(
        11,
        "mean_rev_vol_bands",
        "Mean Reversion / Vol Bands",
        AgentLayer.STRATEGY,
        "python_swarm.strategy_pipeline",
        "Detects bounded statistical mean-reversion opportunities.",
        requires=(2,),
        consumes_from=(3,),
        votes_in_fusion=True,
    ),
    AgentSpec(
        12,
        "correlation_deflection",
        "Correlation Deflection",
        AgentLayer.STRATEGY,
        "python_swarm.strategy_pipeline",
        "Produces correlation signals only from healthy intermarket observations.",
        requires=(2, 15),
        consumes_from=(3,),
        provider_keys=("intermarket",),
        votes_in_fusion=True,
    ),
    AgentSpec(
        13,
        "wyckoff_schematic",
        "Wyckoff Schematic",
        AgentLayer.STRATEGY,
        "python_swarm.strategy_pipeline",
        "Detects Wyckoff accumulation and distribution structures.",
        requires=(2,),
        consumes_from=(3,),
        votes_in_fusion=True,
    ),
    AgentSpec(
        14,
        "macro_catalyst_monitor",
        "Macro Catalyst Monitor",
        AgentLayer.INTELLIGENCE,
        "python_swarm.provider_adapters",
        "Ingests authoritative calendar data and publishes freshness-aware freezes.",
        provider_keys=("macro",),
    ),
    AgentSpec(
        15,
        "intermarket_watcher",
        "Intermarket Watcher",
        AgentLayer.INTELLIGENCE,
        "python_swarm.provider_adapters",
        "Normalizes authoritative DXY, yield, and equity-index observations.",
        provider_keys=("intermarket",),
    ),
    AgentSpec(
        16,
        "confluence_engine",
        "Confluence Engine",
        AgentLayer.DECISION,
        "python_swarm.trading_policies",
        "Fuses unique strategy contributions and constructs a coherent trade candidate.",
        requires=(2, 3),
        consumes_from=tuple(range(4, 14)) + (20,),
    ),
    AgentSpec(
        17,
        "entry_trigger",
        "Entry Trigger",
        AgentLayer.DECISION,
        "python_swarm.decision_service",
        "Applies the final readiness, timing, data, geometry, and exposure policies.",
        requires=(2, 3, 16),
        consumes_from=(14, 18, 21),
    ),
    AgentSpec(
        18,
        "risk_manager",
        "Risk Manager",
        AgentLayer.CONTROL,
        "python_swarm.trading_policies",
        "Reserves risk and enforces immutable portfolio and drawdown limits.",
        requires=(17,),
        consumes_from=(3, 21),
    ),
    AgentSpec(
        19,
        "lifecycle_manager",
        "Lifecycle Manager",
        AgentLayer.CONTROL,
        "python_swarm.trading_policies",
        "Issues lifecycle intents and advances state only on broker-confirmed events.",
        requires=(18,),
        consumes_from=(2, 21),
    ),
    AgentSpec(
        20,
        "continuous_learner",
        "Continuous Learner",
        AgentLayer.CONTROL,
        "python_swarm.learning_policy",
        "Builds bounded, versioned weight snapshots from authoritative outcomes.",
        consumes_from=tuple(range(3, 14)) + (16, 19),
    ),
    AgentSpec(
        21,
        "resilience_controller",
        "Resilience Controller",
        AgentLayer.CONTROL,
        "python_swarm.runtime_state",
        "Owns runtime readiness, disconnect handling, and broker reconciliation policy.",
        consumes_from=(1, 2, 18, 19),
    ),
)


def validate_roster(roster: Iterable[AgentSpec] = _ROSTER) -> tuple[AgentSpec, ...]:
    """Validate roster invariants and return an immutable normalized tuple."""

    normalized = tuple(roster)
    ids = [spec.agent_id for spec in normalized]
    if ids != list(range(1, 22)):
        raise ValueError("agent roster must contain ordered IDs 01 through 21 exactly once")

    slugs = [spec.slug for spec in normalized]
    if len(slugs) != len(set(slugs)):
        raise ValueError("agent roster slugs must be unique")

    names = [spec.name for spec in normalized]
    if len(names) != len(set(names)):
        raise ValueError("agent roster names must be unique")

    known_ids = set(ids)
    for spec in normalized:
        if not spec.slug or not spec.name or not spec.owner_module:
            raise ValueError(f"agent {spec.agent_id:02d} has incomplete metadata")
        references = spec.requires + spec.consumes_from
        if spec.agent_id in references:
            raise ValueError(f"agent {spec.agent_id:02d} cannot depend on itself")
        unknown = set(references) - known_ids
        if unknown:
            raise ValueError(
                f"agent {spec.agent_id:02d} references unknown agents {sorted(unknown)}"
            )

    voters = [spec.agent_id for spec in normalized if spec.votes_in_fusion]
    if voters != list(range(4, 14)):
        raise ValueError("only strategy agents 04 through 13 may vote in fusion")
    return normalized


AGENT_ROSTER: Final[tuple[AgentSpec, ...]] = validate_roster()
AGENT_BY_ID: Final[Mapping[int, AgentSpec]] = MappingProxyType(
    {spec.agent_id: spec for spec in AGENT_ROSTER}
)
AGENT_BY_SLUG: Final[Mapping[str, AgentSpec]] = MappingProxyType(
    {spec.slug: spec for spec in AGENT_ROSTER}
)
STRATEGY_AGENT_IDS: Final[tuple[int, ...]] = tuple(
    spec.agent_id for spec in AGENT_ROSTER if spec.votes_in_fusion
)
CORE_RUNTIME_AGENT_IDS: Final[frozenset[int]] = frozenset(
    {1, 2, 3, 16, 17, 18, 19, 20, 21}
)


def get_agent(agent_id: int) -> AgentSpec:
    """Return one canonical agent or raise a descriptive ``KeyError``."""

    try:
        return AGENT_BY_ID[agent_id]
    except KeyError as exc:
        raise KeyError(f"unknown QUANT-SWARM agent ID: {agent_id!r}") from exc


def enabled_dependency_errors(enabled_agent_ids: Iterable[int]) -> tuple[str, ...]:
    """Return hard-dependency violations for a proposed enabled-agent set."""

    enabled = frozenset(enabled_agent_ids)
    unknown = enabled - set(AGENT_BY_ID)
    errors: list[str] = []
    if unknown:
        errors.append(f"unknown agent IDs: {sorted(unknown)}")
    for agent_id in sorted(enabled & set(AGENT_BY_ID)):
        missing = set(AGENT_BY_ID[agent_id].requires) - enabled
        if missing:
            errors.append(f"agent {agent_id:02d} requires enabled agents {sorted(missing)}")
    return tuple(errors)


__all__ = [
    "AGENT_BY_ID",
    "AGENT_BY_SLUG",
    "AGENT_ROSTER",
    "CORE_RUNTIME_AGENT_IDS",
    "STRATEGY_AGENT_IDS",
    "AgentLayer",
    "AgentSpec",
    "enabled_dependency_errors",
    "get_agent",
    "validate_roster",
]
