from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from python_swarm.domain_models import (
    BrokerCommand,
    CommandAction,
    CommandStatus,
    Direction,
    EntryLevels,
    FixedClock,
    SignalContribution,
)


def test_direction_is_a_stable_string_enum():
    assert Direction.BUY == "BUY"
    assert str(Direction.SELL) == "SELL"


def test_fixed_clock_is_deterministic_and_immutable():
    clock = FixedClock(123_456)
    assert clock.now_ms() == 123_456
    with pytest.raises(FrozenInstanceError):
        clock.utc_ms = 1


def test_signal_conviction_is_unsigned_and_immutable():
    levels = EntryLevels("100", "90", "110", "120", "130")
    signal = SignalContribution(
        signal_id="signal-1",
        snapshot_id="snapshot-1",
        agent_id=4,
        direction=Direction.BUY,
        conviction="82.5",
        levels=levels,
        observed_at_ms=1000,
        regime="trending_bull",
        session="LONDON",
    )
    assert signal.conviction == Decimal("82.5")
    with pytest.raises(FrozenInstanceError):
        signal.conviction = Decimal("90")

    with pytest.raises(ValueError, match="unsigned"):
        SignalContribution(
            signal_id="signal-2",
            snapshot_id="snapshot-1",
            agent_id=5,
            direction=Direction.SELL,
            conviction="-80",
            levels=EntryLevels("100", "110", "90", "80", "70"),
            observed_at_ms=1000,
            regime="trending_bear",
            session="NEW_YORK",
        )


def test_command_payload_is_defensively_immutable():
    source = {"volume": "0.01", "targets": ["1", {"price": "2"}]}
    command = BrokerCommand(
        command_id="command-1",
        trade_id="trade-1",
        action=CommandAction.OPEN,
        status=CommandStatus.CREATED,
        created_at_ms=100,
        expires_at_ms=200,
        payload=source,
    )
    source["volume"] = "5.00"
    assert command.payload["volume"] == "0.01"
    with pytest.raises(TypeError):
        command.payload["volume"] = "1.00"
    assert command.payload["targets"] == ("1", {"price": "2"})
    with pytest.raises(TypeError):
        command.payload["targets"][1]["price"] = "3"


def test_entry_levels_reject_non_finite_numbers():
    with pytest.raises(ValueError, match="finite"):
        EntryLevels("NaN", "90", "110", "120", "130")
