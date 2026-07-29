import json
from pathlib import Path

import pytest

from python_swarm.domain_models import FixedClock
from python_swarm.broker_protocol import (
    CommandAction,
    MessageType,
    ProtocolCodec,
    ProtocolError,
    ProtocolErrorCode,
    ProtocolSession,
)


FIXTURE = Path(__file__).parent / "fixtures" / "protocol_v4_golden.ndjson"
TOKEN = "test-token"
EPOCH = "epoch-1"


def fixture_lines():
    return FIXTURE.read_bytes().splitlines(keepends=True)


def fixture_line(message_type: MessageType):
    return next(
        line for line in fixture_lines()
        if json.loads(line)["type"] == message_type.value
    )


def test_golden_fixture_covers_and_validates_every_v4_message_family():
    codec = ProtocolCodec(TOKEN)
    messages = [codec.decode(line, expected_epoch=EPOCH) for line in fixture_lines()]
    assert {message.message_type for message in messages} == set(MessageType)
    assert [message.seq for message in messages] == list(range(1, 21))

    for message in messages:
        encoded = codec.encode(message)
        assert encoded.endswith(b"\n")
        assert b": " not in encoded and b", " not in encoded
        decoded = json.loads(encoded)
        assert all(not isinstance(value, (dict, list)) for value in decoded.values())


def test_broker_identifiers_are_strings_even_above_javascript_safe_integer():
    codec = ProtocolCodec(TOKEN)
    item = codec.decode(fixture_lines()[5])
    event = codec.decode(fixture_line(MessageType.BROKER_EVENT))
    assert item.fields["position_id"] == "90071992547409931"
    for name in ("order_id", "deal_id", "position_id", "position_ticket"):
        assert isinstance(event.fields[name], str)


def test_hello_exposes_identity_mode_and_permissions_for_fail_closed_authorization():
    hello = ProtocolCodec(TOKEN).decode(fixture_lines()[0])
    assert hello.fields["broker_server"] == "Broker-Demo"
    assert hello.fields["account_trade_mode"] == "DEMO"
    assert hello.fields["trade_allowed"] is True
    assert hello.fields["expert_allowed"] is True

    ack = ProtocolCodec(TOKEN).decode(fixture_lines()[1])
    assert ack.fields["expected_account_id"] == "123456"
    assert ack.fields["expected_broker_server"] == "Broker-Demo"
    assert ack.fields["expected_account_trade_mode"] == "DEMO"
    assert ack.fields["expected_symbol"] == "XAUUSD"
    assert ack.fields["expected_magic"] == 202412
    assert ack.fields["allow_real"] is False
    assert ack.fields["deal_cursor"] == "0:0"

    account = ProtocolCodec(TOKEN).decode(
        fixture_line(MessageType.ACCOUNT_SNAPSHOT)
    )
    assert account.fields["baseline_verified"] is True


def test_pending_order_snapshot_has_atomic_frames_and_string_broker_identity():
    codec = ProtocolCodec(TOKEN)
    begin = codec.decode(fixture_line(MessageType.PENDING_ORDER_SNAPSHOT_BEGIN))
    item = codec.decode(fixture_line(MessageType.PENDING_ORDER_SNAPSHOT_ITEM))
    end = codec.decode(fixture_line(MessageType.PENDING_ORDER_SNAPSHOT_END))
    assert begin.fields["snapshot_id"] == end.fields["snapshot_id"]
    assert begin.fields["count"] == end.fields["count"] == 1
    assert isinstance(item.fields["order_id"], str)
    assert item.fields["magic"] == 202412


def test_replayed_deal_does_not_claim_current_volume_is_historical_truth():
    replay = ProtocolCodec(TOKEN).decode(
        fixture_line(MessageType.DEAL_REPLAY_ITEM)
    )
    assert replay.fields["remaining_volume_known"] is False
    assert replay.fields["remaining_volume"] == 0


def test_wrong_version_auth_epoch_and_replayed_sequence_are_rejected():
    codec = ProtocolCodec(TOKEN)
    hello = json.loads(fixture_lines()[0])

    wrong_version = dict(hello, v="3.0")
    with pytest.raises(ProtocolError) as error:
        codec.decode(json.dumps(wrong_version, separators=(",", ":")) + "\n")
    assert error.value.code == ProtocolErrorCode.UNSUPPORTED_VERSION

    wrong_auth = dict(hello, auth="wrong")
    with pytest.raises(ProtocolError) as error:
        codec.decode(json.dumps(wrong_auth, separators=(",", ":")) + "\n")
    assert error.value.code == ProtocolErrorCode.AUTH_FAILED

    with pytest.raises(ProtocolError) as error:
        codec.decode(fixture_lines()[0], expected_epoch="another-epoch")
    assert error.value.code == ProtocolErrorCode.EPOCH_MISMATCH

    session = ProtocolSession(codec, EPOCH, clock=FixedClock(1000))
    session.receive(fixture_lines()[1])
    with pytest.raises(ProtocolError) as error:
        session.receive(fixture_lines()[1])
    assert error.value.code == ProtocolErrorCode.INVALID_SEQUENCE


def test_session_refuses_non_handshake_traffic_until_explicit_authentication():
    codec = ProtocolCodec(TOKEN)
    session = ProtocolSession(codec, EPOCH, clock=FixedClock(1000))
    with pytest.raises(ProtocolError) as error:
        session.receive(fixture_lines()[2])
    assert error.value.code == ProtocolErrorCode.SESSION_NOT_AUTHENTICATED

    ack = session.receive(fixture_lines()[1])
    assert ack.message_type == MessageType.HELLO_ACK
    session.mark_authenticated()
    heartbeat = session.receive(fixture_lines()[10])
    assert heartbeat.message_type == MessageType.HEARTBEAT


def test_only_one_newline_delimited_flat_object_is_accepted():
    codec = ProtocolCodec(TOKEN)
    line = fixture_lines()[0]
    for invalid in (line.rstrip(b"\n"), line + line):
        with pytest.raises(ProtocolError) as error:
            codec.decode(invalid)
        assert error.value.code == ProtocolErrorCode.INVALID_NDJSON

    nested = json.loads(line)
    nested["extra"] = {"unsafe": True}
    with pytest.raises(ProtocolError) as error:
        codec.decode(json.dumps(nested, separators=(",", ":")) + "\n")
    assert error.value.code == ProtocolErrorCode.NESTED_VALUE


def test_duplicate_keys_and_oversized_frames_are_rejected_before_dispatch():
    codec = ProtocolCodec(TOKEN)
    duplicate = fixture_lines()[0].decode().replace(
        '"role":"MT5_EA"', '"role":"MT5_EA","role":"DUPLICATE"'
    )
    with pytest.raises(ProtocolError) as error:
        codec.decode(duplicate)
    assert error.value.code == ProtocolErrorCode.DUPLICATE_FIELD

    small_codec = ProtocolCodec(TOKEN, max_frame_bytes=512)
    huge = fixture_lines()[0].decode().replace("Broker-Demo", "X" * 600)
    with pytest.raises(ProtocolError) as error:
        small_codec.decode(huge)
    assert error.value.code == ProtocolErrorCode.FRAME_TOO_LARGE


def test_numeric_position_identity_is_rejected():
    codec = ProtocolCodec(TOKEN)
    item = json.loads(fixture_lines()[5])
    item["position_id"] = 90071992547409931
    with pytest.raises(ProtocolError) as error:
        codec.decode(json.dumps(item, separators=(",", ":")) + "\n")
    assert error.value.code == ProtocolErrorCode.INVALID_FIELD_TYPE


@pytest.mark.parametrize(
    ("line_index", "field", "invalid"),
    [
        (0, "magic", True),
        (0, "terminal_build", 5000.5),
        (2, "equity", "1000.0"),
        (10, "daily_halted", 0),
        (14, "retcode", "10009"),
    ],
)
def test_scalar_fields_still_enforce_boolean_integer_and_numeric_types(
    line_index, field, invalid
):
    raw = json.loads(fixture_lines()[line_index])
    raw[field] = invalid
    with pytest.raises(ProtocolError) as error:
        ProtocolCodec(TOKEN).decode(json.dumps(raw, separators=(",", ":")) + "\n")
    assert error.value.code == ProtocolErrorCode.INVALID_FIELD_TYPE


@pytest.mark.parametrize(
    ("action", "fields"),
    [
        (
            CommandAction.OPEN,
            dict(
                reservation_id="reservation-1", symbol="XAUUSD", magic=202412,
                direction="BUY", volume=0.04, reference_price=100.0,
                max_slippage_points=30, sl=90.0, tp1=110.0, tp2=120.0,
                tp3=130.0, risk_limit=40.0, risk_pct_limit=1.0,
                comment="QSX|trade-1",
            ),
        ),
        (
            CommandAction.MODIFY_SL,
            dict(position_id="p-1", position_ticket="t-1", new_sl=100.03,
                 final_tp=130.0, expected_position_version=1),
        ),
        (
            CommandAction.PARTIAL_CLOSE,
            dict(position_id="p-1", position_ticket="t-1", close_volume=0.02,
                 expected_position_version=1),
        ),
        (
            CommandAction.CLOSE,
            dict(position_id="p-1", position_ticket="t-1", expected_position_version=1),
        ),
        (CommandAction.CLOSE_ALL, dict(symbol="XAUUSD", magic=202412)),
        (CommandAction.REQUEST_SNAPSHOT, dict(deal_cursor="0")),
    ],
)
def test_every_command_action_has_a_valid_flat_contract(action, fields):
    codec = ProtocolCodec(TOKEN)
    message = codec.build(
        MessageType.COMMAND,
        msg_id=f"msg-{action.value}",
        epoch=EPOCH,
        seq=1,
        correlation_id="intent-1",
        ts_ms=1000,
        command_id=f"command-{action.value}",
        trade_id="trade-1",
        action=action.value,
        expires_at_ms=2000,
        **fields,
    )
    decoded = codec.decode(codec.encode(message), expected_epoch=EPOCH)
    assert decoded.fields["action"] == action.value


def test_action_specific_missing_fields_fail_validation():
    codec = ProtocolCodec(TOKEN)
    with pytest.raises(ProtocolError) as error:
        codec.build(
            MessageType.COMMAND,
            msg_id="bad-open",
            epoch=EPOCH,
            seq=1,
            correlation_id="intent-1",
            ts_ms=1000,
            command_id="command-1",
            trade_id="trade-1",
            action="OPEN",
            expires_at_ms=2000,
        )
    assert error.value.code == ProtocolErrorCode.INVALID_COMMAND


def test_lifecycle_commands_require_a_positive_position_version_fence():
    codec = ProtocolCodec(TOKEN)
    with pytest.raises(ProtocolError) as error:
        codec.build(
            MessageType.COMMAND,
            msg_id="bad-close",
            epoch=EPOCH,
            seq=1,
            correlation_id="intent-1",
            ts_ms=1000,
            command_id="command-1",
            trade_id="trade-1",
            action="CLOSE",
            expires_at_ms=2000,
            position_id="p-1",
            position_ticket="t-1",
            expected_position_version=0,
        )
    assert error.value.code == ProtocolErrorCode.INVALID_COMMAND


def test_event_requires_explicit_identity_and_cumulative_remaining_volume():
    codec = ProtocolCodec(TOKEN)
    raw = json.loads(fixture_line(MessageType.BROKER_EVENT))
    raw.pop("remaining_volume")
    with pytest.raises(ProtocolError) as error:
        codec.decode(json.dumps(raw, separators=(",", ":")) + "\n")
    assert error.value.code == ProtocolErrorCode.MISSING_FIELD

    raw = json.loads(fixture_line(MessageType.BROKER_EVENT))
    raw["trade_id"] = ""
    with pytest.raises(ProtocolError) as error:
        codec.decode(json.dumps(raw, separators=(",", ":")) + "\n")
    assert error.value.code == ProtocolErrorCode.MISSING_FIELD
