import dataclasses

import pytest

from python_swarm.agent_roster import (
    AGENT_BY_ID,
    AGENT_ROSTER,
    CORE_RUNTIME_AGENT_IDS,
    STRATEGY_AGENT_IDS,
    AgentLayer,
    enabled_dependency_errors,
    get_agent,
)


def test_canonical_roster_is_exact_and_stable():
    expected_names = [
        "Orchestrator",
        "Tick Streamer",
        "Regime Detector",
        "HTF Liquidity Trap",
        "Trendline Breakout",
        "CRT Engine",
        "ICT/SMC Core",
        "VPVR/POC",
        "Asia Session Sweep",
        "News/Sentiment",
        "Mean Reversion / Vol Bands",
        "Correlation Deflection",
        "Wyckoff Schematic",
        "Macro Catalyst Monitor",
        "Intermarket Watcher",
        "Confluence Engine",
        "Entry Trigger",
        "Risk Manager",
        "Lifecycle Manager",
        "Continuous Learner",
        "Resilience Controller",
    ]
    assert [agent.agent_id for agent in AGENT_ROSTER] == list(range(1, 22))
    assert [agent.name for agent in AGENT_ROSTER] == expected_names
    assert len({agent.slug for agent in AGENT_ROSTER}) == 21
    assert AGENT_BY_ID[21].display_id == "21"


def test_only_ten_strategy_agents_vote_in_fusion():
    assert STRATEGY_AGENT_IDS == tuple(range(4, 14))
    assert all(AGENT_BY_ID[agent_id].layer is AgentLayer.STRATEGY for agent_id in range(4, 14))
    assert all(AGENT_BY_ID[agent_id].votes_in_fusion for agent_id in range(4, 14))
    assert not any(
        AGENT_BY_ID[agent_id].votes_in_fusion
        for agent_id in set(range(1, 22)) - set(range(4, 14))
    )


def test_provider_dependent_agents_have_explicit_dependencies():
    assert AGENT_BY_ID[10].requires == (2, 14)
    assert AGENT_BY_ID[10].provider_keys == ("macro",)
    assert AGENT_BY_ID[12].requires == (2, 15)
    assert AGENT_BY_ID[12].provider_keys == ("intermarket",)

    enabled = CORE_RUNTIME_AGENT_IDS | {4, 5, 6, 10}
    errors = enabled_dependency_errors(enabled)
    assert any("agent 10" in error and "14" in error for error in errors)


def test_roster_objects_and_indexes_are_immutable():
    with pytest.raises(dataclasses.FrozenInstanceError):
        AGENT_ROSTER[0].name = "Changed"
    with pytest.raises(TypeError):
        AGENT_BY_ID[1] = AGENT_ROSTER[0]
    with pytest.raises(KeyError, match="unknown QUANT-SWARM agent ID"):
        get_agent(99)


def test_canonical_owners_do_not_route_back_into_retired_command_paths():
    retired = {
        "python_swarm.evolved_orchestrator",
        "python_swarm.orchestrator",
        "python_swarm.ipc_bridge",
        "python_swarm.run_ipc",
        "python_swarm.risk_and_lifecycle",
        "python_swarm.continuous_learner",
    }
    assert not {agent.owner_module for agent in AGENT_ROSTER} & retired
    assert {
        AGENT_BY_ID[agent_id].owner_module for agent_id in STRATEGY_AGENT_IDS
    } == {"python_swarm.strategy_pipeline"}
