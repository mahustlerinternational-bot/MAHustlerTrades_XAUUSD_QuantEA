import time

import pytest

from python_swarm.confluence_engine import AgentSignal, ConfluenceFusionEngine


def _signal(agent_id: int, direction: str, conviction: float = 80.0) -> AgentSignal:
    if direction == "BUY":
        levels = dict(entry=3000.0, sl=2990.0, tp1=3010.0, tp2=3020.0, tp3=3040.0)
    else:
        levels = dict(entry=3000.0, sl=3010.0, tp1=2990.0, tp2=2980.0, tp3=2960.0)
    return AgentSignal(
        agent_id=agent_id,
        direction=direction,
        conviction=conviction,
        strategy_tags=(f"agent-{agent_id}",),
        timestamp=time.time(),
        **levels,
    )


@pytest.mark.parametrize("direction", ["BUY", "SELL"])
def test_legacy_adapter_is_unsigned_and_directionally_symmetric(direction):
    result = ConfluenceFusionEngine({}).fuse_signals(
        [_signal(4, direction), _signal(5, direction), _signal(6, direction)]
    )

    assert result is not None
    assert result["direction"] == direction
    assert result["agents_agreeing"] == 3
    assert result["confluence_pct"] == 80.0


def test_legacy_adapter_requires_three_distinct_directional_agents():
    engine = ConfluenceFusionEngine({})

    assert engine.fuse_signals([_signal(4, "BUY"), _signal(5, "BUY")]) is None
    assert engine.fuse_signals(
        [_signal(4, "BUY"), _signal(4, "BUY"), _signal(5, "BUY")]
    ) is None


def test_legacy_adapter_rejects_directionally_invalid_levels():
    invalid = _signal(4, "SELL")
    invalid.sl = 2990.0

    assert ConfluenceFusionEngine({}).fuse_signals(
        [invalid, _signal(5, "SELL"), _signal(6, "SELL")]
    ) is None


def test_legacy_signal_rejects_signed_conviction():
    with pytest.raises(ValueError, match="unsigned finite"):
        _signal(4, "SELL", -80.0)
