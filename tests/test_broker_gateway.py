import asyncio
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import pytest

from python_swarm.broker_gateway import (
    BrokerGateway,
    BrokerTick,
    DurableDealAttribution,
    DurablePositionContext,
    GatewayConfig,
    GatewayDisconnectedError,
    GatewaySignalType,
    GatewayState,
)
from python_swarm.broker_protocol import (
    BrokerSessionMode,
    MessageType,
    ProtocolCodec,
)
from python_swarm.domain_models import CommandAction
from python_swarm.execution_service import BrokerPort, OutboundCommand
from python_swarm.reconciliation import ReconciliationBrokerPort, ReconciliationService
from python_swarm.runtime_paths import RuntimePaths
from python_swarm.runtime_state import (
    BrokerTradeMode,
    ControlPlaneState,
    ExpectedBrokerIdentity,
    RuntimeStateMachine,
)
from python_swarm.trade_ledger import (
    BrokerCommandRecord,
    BrokerDealRecord,
    DailyRiskStateRecord,
    RiskReservationRecord,
    TradeLedger,
    TradeRecord,
)


TOKEN = "test-token"
NOW = 1_800_000_000_000
EXPECTED = ExpectedBrokerIdentity(
    account_id="123456",
    broker_server="Broker-Demo",
    symbol="XAUUSD",
    magic=202412,
)
FIXTURE = Path(__file__).parent / "fixtures" / "protocol_v4_golden.ndjson"
_FIXTURE_CODEC = ProtocolCodec(TOKEN)
_FIELDS = {
    message.message_type: dict(message.fields)
    for message in (
        _FIXTURE_CODEC.decode(line + b"\n")
        for line in FIXTURE.read_bytes().splitlines()
    )
}


@dataclass
class MutableClock:
    value: int = NOW

    def now_ms(self) -> int:
        return self.value

    def advance(self, milliseconds: int) -> None:
        self.value += milliseconds


class Peer:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        clock: MutableClock,
        epoch: str,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.clock = clock
        self.epoch = epoch
        self.codec = ProtocolCodec(TOKEN)
        self.sequence = 0

    def frame(
        self,
        message_type: MessageType,
        correlation_id: str = "",
        **overrides: object,
    ) -> bytes:
        self.sequence += 1
        fields = dict(_FIELDS[message_type])
        fields.update(overrides)
        message = self.codec.build(
            message_type,
            msg_id=f"peer-{self.epoch}-{self.sequence}",
            epoch=self.epoch,
            seq=self.sequence,
            correlation_id=correlation_id,
            ts_ms=self.clock.now_ms(),
            **fields,
        )
        return self.codec.encode(message)

    async def send(self, *frames: bytes, fragmented: bool = False) -> None:
        data = b"".join(frames)
        if fragmented:
            cuts = (1, 3, 8, 17, 31)
            offset = 0
            for size in cuts:
                if offset >= len(data):
                    break
                self.writer.write(data[offset : offset + size])
                await self.writer.drain()
                offset += size
            self.writer.write(data[offset:])
        else:
            self.writer.write(data)
        await self.writer.drain()

    async def receive(self):
        frame = await asyncio.wait_for(self.reader.readline(), 1)
        assert frame
        return self.codec.decode(frame, expected_epoch=self.epoch)

    async def close(self) -> None:
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except (ConnectionError, OSError):
            pass


def gateway_config(**overrides: object) -> GatewayConfig:
    values = {
        "auth_token": TOKEN,
        "config_hash": "cfg-1",
        "expected": EXPECTED,
        "expected_trade_mode": BrokerTradeMode.DEMO,
        "expected_currency": "USD",
        "port": 0,
        "heartbeat_timeout_ms": 2_000,
        "snapshot_timeout_ms": 1_000,
        "max_message_age_ms": 1_000,
        "max_context_age_ms": 500,
        "session_mode": BrokerSessionMode.DEMO,
    }
    values.update(overrides)
    return GatewayConfig(**values)


def test_gateway_configuration_defaults_to_non_executing_shadow():
    config = GatewayConfig(
        auth_token=TOKEN,
        config_hash="cfg-1",
        expected=EXPECTED,
    )

    assert config.session_mode is BrokerSessionMode.SHADOW
    assert config.execution_enabled is False


def test_gateway_configuration_rejects_real_accounts_in_this_release():
    with pytest.raises(ValueError, match="REAL accounts are not supported"):
        GatewayConfig(
            auth_token=TOKEN,
            config_hash="cfg-1",
            expected=ExpectedBrokerIdentity(
                account_id="123456",
                broker_server="Broker-Real",
                symbol="XAUUSD",
                magic=202412,
                allow_real_account=True,
            ),
            expected_trade_mode=BrokerTradeMode.REAL,
            session_mode=BrokerSessionMode.DEMO,
        )


def context_for(
    observed,
    clock: MutableClock,
    *,
    resolved_at_ms: int | None = None,
    trade_id: str | None = None,
) -> DurablePositionContext:
    return DurablePositionContext(
        resolved_at_ms=clock.now_ms() if resolved_at_ms is None else resolved_at_ms,
        account_id=EXPECTED.account_id,
        trade_id=trade_id or observed.trade_id,
        broker_position_id=observed.broker_position_id,
        symbol=observed.symbol,
        magic=observed.magic,
        direction=observed.direction,
        initial_volume="0.04",
        original_sl="90",
        tp1="110",
        tp2="120",
        tp3="130",
        initial_risk_amount="40",
        lifecycle_stage="PRE_TP1",
        position_version=observed.position_version,
        broker_order_id="90071992547409934",
        pending_action="PARTIAL_CLOSE",
        pending_milestone="TP1",
        pending_command_id="pending-close-1",
        pending_close_volume="0.02",
        pending_filled_volume="0.01",
    )


async def connect_peer(
    gateway: BrokerGateway,
    clock: MutableClock,
    *,
    epoch: str = "epoch-1",
    fragmented_hello: bool = False,
    hello_overrides: dict[str, object] | None = None,
) -> tuple[Peer, object]:
    reader, writer = await asyncio.open_connection("127.0.0.1", gateway.listen_port)
    peer = Peer(reader, writer, clock, epoch)
    hello = peer.frame(MessageType.HELLO, **(hello_overrides or {}))
    await peer.send(hello, fragmented=fragmented_hello)
    ack = await peer.receive()
    return peer, ack


def full_snapshot_frames(
    peer: Peer,
    correlation_id: str,
    *,
    requested_cursor: str = "0:0",
    position_trade_id: str = "trade-1",
    position_volume: Decimal | str = "0.04",
    position_version: int | None = None,
    position_open_time_ms: int | None = None,
    account_mode: str = "HEDGING",
    deal_time_ms: int | None = None,
    deal_entry_type: str = "OUT",
    deal_price: Decimal | str = "110",
    deal_volume: Decimal | str = "0.02",
    deal_command_id: str = "command-1",
    pending: bool = True,
) -> tuple[bytes, ...]:
    version = position_version or peer.clock.now_ms() - 1_000
    deal_id = "90071992547409933"
    deal_time = peer.clock.now_ms() - 100 if deal_time_ms is None else deal_time_ms
    open_time = (
        peer.clock.now_ms() - 10_000
        if position_open_time_ms is None
        else position_open_time_ms
    )
    snapshot_cursor = f"{deal_time}:{deal_id}"
    position_id = "90071992547409931"
    frames = [
        peer.frame(
            MessageType.ACCOUNT_SNAPSHOT,
            correlation_id,
            server_time_ms=peer.clock.now_ms(),
            risk_day="2027-01-15",
            account_mode=account_mode,
        ),
        peer.frame(
            MessageType.SYMBOL_SNAPSHOT,
            correlation_id,
            tick_value_profit=Decimal("1.5"),
            tick_value_loss=Decimal("1.8"),
            contract_size=Decimal("100"),
        ),
        peer.frame(
            MessageType.POSITION_SNAPSHOT_BEGIN,
            correlation_id,
            snapshot_id=f"positions-{peer.epoch}",
            count=1,
            deal_cursor=snapshot_cursor,
        ),
        peer.frame(
            MessageType.POSITION_SNAPSHOT_ITEM,
            correlation_id,
            snapshot_id=f"positions-{peer.epoch}",
            position_id=position_id,
            position_ticket="90071992547409932",
            trade_id=position_trade_id,
            volume=Decimal(str(position_volume)),
            price_open=Decimal("100"),
            sl=Decimal("95"),
            tp=Decimal("130"),
            position_version=version,
            open_time_ms=open_time,
        ),
        peer.frame(
            MessageType.POSITION_SNAPSHOT_END,
            correlation_id,
            snapshot_id=f"positions-{peer.epoch}",
            count=1,
            deal_cursor=snapshot_cursor,
        ),
        peer.frame(
            MessageType.PENDING_ORDER_SNAPSHOT_BEGIN,
            correlation_id,
            snapshot_id=f"orders-{peer.epoch}",
            count=1 if pending else 0,
        ),
    ]
    if pending:
        frames.append(
            peer.frame(
                MessageType.PENDING_ORDER_SNAPSHOT_ITEM,
                correlation_id,
                snapshot_id=f"orders-{peer.epoch}",
                order_time_ms=peer.clock.now_ms() - 5_000,
                trade_id="trade-2",
                command_id="command-2",
            )
        )
    frames.extend(
        [
            peer.frame(
                MessageType.PENDING_ORDER_SNAPSHOT_END,
                correlation_id,
                snapshot_id=f"orders-{peer.epoch}",
                count=1 if pending else 0,
            ),
            peer.frame(
                MessageType.DEAL_REPLAY_BEGIN,
                correlation_id,
                replay_id=f"replay-{peer.epoch}",
                cursor_from=requested_cursor,
                count=1,
            ),
            peer.frame(
                MessageType.DEAL_REPLAY_ITEM,
                correlation_id,
                replay_id=f"replay-{peer.epoch}",
                cursor=snapshot_cursor,
                deal_id=deal_id,
                broker_time_ms=deal_time,
                position_id=position_id,
                trade_id=position_trade_id,
                command_id=deal_command_id,
                entry_type=deal_entry_type,
                price=Decimal(str(deal_price)),
                volume=Decimal(str(deal_volume)),
                remaining_volume=0,
                remaining_volume_known=False,
                position_version=version,
            ),
            peer.frame(
                MessageType.DEAL_REPLAY_END,
                correlation_id,
                replay_id=f"replay-{peer.epoch}",
                cursor_next=snapshot_cursor,
                count=1,
            ),
        ]
    )
    return tuple(frames)


async def signal_of(gateway: BrokerGateway, kind: GatewaySignalType):
    for _ in range(30):
        signal = await gateway.next_signal(1)
        if signal.signal_type is kind:
            return signal
    raise AssertionError(f"signal {kind} was not observed")


def make_open_command(epoch: str, command_id: str = "command-open") -> OutboundCommand:
    return OutboundCommand(
        command_id=command_id,
        trade_id="trade-open",
        reservation_id="reservation-open",
        action=CommandAction.OPEN,
        connection_epoch=epoch,
        sequence=1,
        created_at_ms=NOW,
        expires_at_ms=NOW + 10_000,
        position_id=None,
        expected_position_version=None,
        expected_effect={"remaining_volume": "0.04"},
        payload={
            "action": "OPEN",
            "intent_id": "intent-open",
            "trade_id": "trade-open",
            "reservation_id": "reservation-open",
            "account_id": "123456",
            "currency": "USD",
            "symbol": "XAUUSD",
            "magic": 202412,
            "direction": "BUY",
            "volume": Decimal("0.04"),
            "entry": Decimal("100"),
            "max_slippage_points": 30,
            "sl": Decimal("90"),
            "tp1": Decimal("110"),
            "tp2": Decimal("120"),
            "tp3": Decimal("130"),
            "risk_amount": Decimal("40"),
            "requested_risk_pct": Decimal("1"),
            "max_total_risk": Decimal("120"),
            "max_open_positions": 3,
            "config_hash": "cfg-1",
        },
    )


def make_lifecycle_command(
    epoch: str,
    action: CommandAction,
    position_version: int,
    *,
    command_id: str,
) -> OutboundCommand:
    expected_effect = (
        {
            "target_close_volume": "0.02",
            "target_remaining_volume": "0.02",
        }
        if action is CommandAction.PARTIAL_CLOSE
        else {"target_remaining_volume": "0"}
    )
    return OutboundCommand(
        command_id=command_id,
        trade_id="trade-1",
        reservation_id="reservation-1",
        action=action,
        connection_epoch=epoch,
        sequence=2,
        created_at_ms=NOW,
        expires_at_ms=NOW + 10_000,
        position_id="90071992547409931",
        expected_position_version=position_version,
        expected_effect=expected_effect,
        payload={"action": action.value, "trade_id": "trade-1"},
    )


def ack_frame(peer: Peer, command_id: str = "command-open") -> bytes:
    return peer.frame(
        MessageType.COMMAND_ACK,
        command_id,
        command_id=command_id,
        trade_id="trade-open",
        action="OPEN",
        status="APPLIED",
        actual_price=Decimal("100.01"),
        actual_volume=Decimal("0.04"),
    )


def lifecycle_ack_frame(
    peer: Peer,
    action: CommandAction,
    command_id: str,
) -> bytes:
    return peer.frame(
        MessageType.COMMAND_ACK,
        command_id,
        command_id=command_id,
        trade_id="trade-1",
        action=action.value,
        status="APPLIED",
        actual_price=Decimal("110"),
        actual_volume=Decimal("0.01"),
    )


def event_frame(
    peer: Peer,
    *,
    command_id: str = "command-open",
    event_id: str = "DEAL-open",
) -> bytes:
    return peer.frame(
        MessageType.BROKER_EVENT,
        command_id,
        event_id=event_id,
        event_type="POSITION_OPENED",
        command_id=command_id,
        trade_id="trade-open",
        direction="BUY",
        price=Decimal("100.01"),
        volume_delta=Decimal("0.04"),
        remaining_volume=Decimal("0.04"),
        sl=Decimal("90"),
        tp=Decimal("130"),
        profit=0,
        commission=0,
        swap=0,
        fee=0,
        broker_time_ms=peer.clock.now_ms(),
        position_version=peer.clock.now_ms(),
    )


def lifecycle_event_frame(
    peer: Peer,
    *,
    command_id: str,
    event_index: int,
    event_type: str,
    remaining_volume: Decimal | str,
    volume_delta: Decimal | str,
    position_version: int,
) -> bytes:
    deal_id = str(90071992547410000 + event_index)
    return peer.frame(
        MessageType.BROKER_EVENT,
        command_id,
        event_id=f"DEAL-{deal_id}",
        event_type=event_type,
        command_id=command_id,
        trade_id="trade-1",
        order_id="90071992547409934",
        deal_id=deal_id,
        position_id="90071992547409931",
        position_ticket="90071992547409932",
        direction="BUY",
        price=Decimal("110"),
        volume_delta=Decimal(str(volume_delta)),
        remaining_volume=Decimal(str(remaining_volume)),
        sl=Decimal("90"),
        tp=Decimal("130"),
        broker_time_ms=peer.clock.now_ms(),
        position_version=position_version,
    )


def test_import_and_construction_are_inert_and_ports_are_compatible():
    clock = MutableClock()
    gateway = BrokerGateway(config=gateway_config(), clock=clock)
    assert gateway.state is GatewayState.STOPPED
    assert gateway.listen_port is None
    assert isinstance(gateway, BrokerPort)
    assert isinstance(gateway, ReconciliationBrokerPort)


def test_fragmented_and_coalesced_stream_yields_fenced_snapshot_replay_and_bars():
    async def scenario() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(
            config=gateway_config(),
            clock=clock,
            committed_cursor_provider=lambda _account: "0:0",
            position_context_resolver=lambda observed: context_for(observed, clock),
        )
        await gateway.start()
        peer = None
        try:
            peer, ack = await connect_peer(
                gateway, clock, fragmented_hello=True
            )
            assert ack.fields["status"] == "ACCEPTED"
            frames = full_snapshot_frames(peer, ack.msg_id)
            await peer.send(*frames[:5])
            await peer.send(*frames[5:], fragmented=True)
            await signal_of(gateway, GatewaySignalType.SNAPSHOT_READY)

            snapshot = await gateway.fetch_snapshot(EXPECTED)
            assert snapshot.risk.baseline_verified is True
            assert len(snapshot.positions) == len(snapshot.pending_orders) == 1
            position = snapshot.positions[0]
            assert position.remaining_volume == Decimal("0.04")
            assert position.original_sl == Decimal("90")
            assert position.pending_action == "PARTIAL_CLOSE"
            assert position.pending_command_id == "pending-close-1"
            assert position.pending_filled_volume == Decimal("0.01")
            assert snapshot.pending_orders[0].command_id == "command-2"
            assert gateway.latest_symbol_snapshot.tick_value_per_lot == Decimal("1.8")
            assert snapshot.payload["tick_value_policy"] == "MAX_PROFIT_LOSS_CONSERVATIVE"

            page = await gateway.fetch_deal_page(EXPECTED.account_id, None)
            assert page.complete is True and len(page.deals) == 1
            assert page.deals[0].command_id == "command-1"
            assert page.deals[0].deal_type == "OUT"
            assert page.deals[0].payload["direction"] == "BUY"
            assert page.deals[0].payload["entry_type"] == "OUT"
            assert page.deals[0].payload["remaining_volume"] is None
            assert position.remaining_volume == Decimal("0.04")

            closed_bar = peer.frame(
                MessageType.BAR,
                tf="H1",
                bar_time_ms=clock.now_ms() - 3_600_000,
                o=100,
                h=110,
                l=99,
                c=105,
            )
            forming_bar = peer.frame(
                MessageType.BAR,
                tf="H1",
                bar_time_ms=clock.now_ms(),
                o=105,
                h=110,
                l=100,
                c=106,
            )
            await peer.send(closed_bar, forming_bar)
            await signal_of(gateway, GatewaySignalType.BAR_WITHHELD)
            assert len(gateway.completed_bars) == 1
            assert gateway.completed_bars[0].close_time_ms == clock.now_ms()
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())


def test_command_ack_event_correlation_is_order_independent_and_disconnect_is_unknown():
    async def scenario() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(config=gateway_config(), clock=clock)
        await gateway.start()
        peer = None
        try:
            peer, _ack = await connect_peer(gateway, clock)
            command = make_open_command(peer.epoch)
            await gateway.send(command)
            wire_command = await peer.receive()
            assert wire_command.message_type is MessageType.COMMAND
            assert wire_command.fields["reference_price"] == Decimal("100")
            assert wire_command.fields["risk_limit"] == Decimal("40")

            event = event_frame(peer)
            ack = ack_frame(peer)
            await peer.send(event, ack)
            broker_signal = await signal_of(gateway, GatewaySignalType.BROKER_EVENT)
            ack_signal = await signal_of(gateway, GatewaySignalType.COMMAND_ACK)
            assert broker_signal.command_id == ack_signal.command_id == command.command_id
            assert broker_signal.payload["event"].command_id == command.command_id
            deal = broker_signal.payload["deal"]
            assert isinstance(deal, BrokerDealRecord)
            assert deal.command_id == command.command_id
            assert deal.trade_id == "trade-open"
            assert deal.deal_type == "IN"
            assert deal.volume == Decimal("0.04")
            assert deal.position_id == broker_signal.payload["event"].broker_position_id
            assert deal.deal_id == broker_signal.payload["event"].deal_id

            await gateway.send(make_open_command(peer.epoch, "command-unknown"))
            await peer.receive()
            await peer.close()
            peer = None
            unknown = await signal_of(gateway, GatewaySignalType.UNKNOWN_COMMAND)
            assert unknown.command_id == "command-unknown"
            assert unknown.payload == {
                "action": "OPEN",
                "ack_seen": False,
                "effect_seen": False,
            }
            assert gateway.state is GatewayState.DISCONNECTED
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("disabled_permission", ["trade_allowed", "expert_allowed"])
def test_demo_handshake_rejects_each_disabled_trading_permission(
    disabled_permission,
):
    async def scenario() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(config=gateway_config(), clock=clock)
        await gateway.start()
        peer = None
        try:
            peer, ack = await connect_peer(
                gateway,
                clock,
                epoch=f"epoch-permission-{disabled_permission}",
                hello_overrides={disabled_permission: False},
            )
            assert ack.fields["status"] == "REJECTED"
            assert "trading disabled" in ack.fields["reason"]
            await signal_of(gateway, GatewaySignalType.PROTOCOL_VIOLATION)
            assert gateway.state is GatewayState.DISCONNECTED
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"role": "OTHER"}, "role"),
        ({"ea_version": "4.00"}, "ea_version"),
        ({"emergency_stop": True}, "emergency_stop"),
    ],
)
def test_demo_handshake_pins_peer_role_version_and_entry_readiness(
    overrides,
    reason,
):
    async def scenario() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(config=gateway_config(), clock=clock)
        await gateway.start()
        peer = None
        try:
            peer, ack = await connect_peer(
                gateway,
                clock,
                epoch=f"epoch-invalid-{reason}",
                hello_overrides=overrides,
            )
            assert ack.fields["status"] == "REJECTED"
            assert reason in ack.fields["reason"]
            await signal_of(gateway, GatewaySignalType.PROTOCOL_VIOLATION)
            assert gateway.state is GatewayState.DISCONNECTED
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "action",
    [CommandAction.PARTIAL_CLOSE, CommandAction.CLOSE],
)
def test_ack_and_intermediate_fill_remain_unknown_until_exact_effect(action):
    async def scenario() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(
            config=gateway_config(),
            clock=clock,
            position_context_resolver=lambda observed: context_for(observed, clock),
        )
        await gateway.start()
        peer = None
        try:
            peer, hello_ack = await connect_peer(
                gateway, clock, epoch=f"epoch-incomplete-{action.value.lower()}"
            )
            position_version = clock.now_ms() - 1_000
            await peer.send(
                *full_snapshot_frames(
                    peer,
                    hello_ack.msg_id,
                    position_version=position_version,
                )
            )
            await signal_of(gateway, GatewaySignalType.SNAPSHOT_READY)

            command_id = f"command-incomplete-{action.value.lower()}"
            command = make_lifecycle_command(
                peer.epoch,
                action,
                position_version,
                command_id=command_id,
            )
            await gateway.send(command)
            assert (await peer.receive()).fields["action"] == action.value
            await peer.send(
                lifecycle_event_frame(
                    peer,
                    command_id=command_id,
                    event_index=1,
                    event_type="PARTIAL_FILLED",
                    remaining_volume="0.03",
                    volume_delta="-0.01",
                    position_version=position_version + 1,
                ),
                lifecycle_ack_frame(peer, action, command_id),
            )
            await signal_of(gateway, GatewaySignalType.BROKER_EVENT)
            await signal_of(gateway, GatewaySignalType.COMMAND_ACK)

            await peer.close()
            peer = None
            unknown = await signal_of(gateway, GatewaySignalType.UNKNOWN_COMMAND)
            assert unknown.command_id == command_id
            assert unknown.payload == {
                "action": action.value,
                "ack_seen": True,
                "effect_seen": True,
            }
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("action", "second_event_type", "second_remaining"),
    [
        (CommandAction.PARTIAL_CLOSE, "PARTIAL_FILLED", "0.02"),
        (CommandAction.CLOSE, "POSITION_CLOSED", "0"),
    ],
)
def test_multifill_effect_completion_retains_terminal_command_correlation(
    action: CommandAction,
    second_event_type: str,
    second_remaining: str,
):
    async def scenario() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(
            config=gateway_config(),
            clock=clock,
            position_context_resolver=lambda observed: context_for(observed, clock),
        )
        await gateway.start()
        peer = None
        try:
            peer, hello_ack = await connect_peer(
                gateway, clock, epoch=f"epoch-multifill-{action.value.lower()}"
            )
            position_version = clock.now_ms() - 1_000
            await peer.send(
                *full_snapshot_frames(
                    peer,
                    hello_ack.msg_id,
                    position_version=position_version,
                )
            )
            await signal_of(gateway, GatewaySignalType.SNAPSHOT_READY)

            command_id = f"command-multifill-{action.value.lower()}"
            command = make_lifecycle_command(
                peer.epoch,
                action,
                position_version,
                command_id=command_id,
            )
            await gateway.send(command)
            await peer.receive()
            await peer.send(
                lifecycle_ack_frame(peer, action, command_id),
                lifecycle_event_frame(
                    peer,
                    command_id=command_id,
                    event_index=10,
                    event_type="PARTIAL_FILLED",
                    remaining_volume="0.03" if action is CommandAction.PARTIAL_CLOSE else "0.02",
                    volume_delta="-0.01" if action is CommandAction.PARTIAL_CLOSE else "-0.02",
                    position_version=position_version + 1,
                ),
                lifecycle_event_frame(
                    peer,
                    command_id=command_id,
                    event_index=11,
                    event_type=second_event_type,
                    remaining_volume=second_remaining,
                    volume_delta="-0.01" if action is CommandAction.PARTIAL_CLOSE else "-0.02",
                    position_version=position_version + 2,
                ),
            )
            await signal_of(gateway, GatewaySignalType.COMMAND_ACK)
            first = await signal_of(gateway, GatewaySignalType.BROKER_EVENT)
            second = await signal_of(gateway, GatewaySignalType.BROKER_EVENT)
            assert first.command_id == second.command_id == command_id

            # A later compatible transaction can arrive after ACK + exact effect
            # retired the live tracker. It must still correlate through the
            # bounded terminal-command cache.
            await peer.send(
                lifecycle_event_frame(
                    peer,
                    command_id=command_id,
                    event_index=12,
                    event_type="POSITION_CLOSED",
                    remaining_volume="0",
                    volume_delta="-0.02",
                    position_version=position_version + 3,
                )
            )
            terminal = await signal_of(gateway, GatewaySignalType.BROKER_EVENT)
            assert terminal.command_id == command_id
            assert terminal.trade_id == "trade-1"
            assert terminal.payload["attribution_status"] == "VERIFIED"
            assert gateway.state is GatewayState.AUTHENTICATED

            await peer.close()
            peer = None
            signals = []
            while True:
                signal = await gateway.next_signal(1)
                signals.append(signal)
                if signal.signal_type is GatewaySignalType.DISCONNECTED:
                    break
            assert not any(
                signal.signal_type is GatewaySignalType.UNKNOWN_COMMAND
                and signal.command_id == command_id
                for signal in signals
            )
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())


def test_exact_semantic_duplicates_are_idempotent_but_conflicts_disconnect():
    async def scenario() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(config=gateway_config(), clock=clock)
        await gateway.start()
        peer = None
        try:
            peer, _ = await connect_peer(gateway, clock)
            await gateway.send(make_open_command(peer.epoch))
            await peer.receive()
            first_event = event_frame(peer)
            first_ack = ack_frame(peer)
            duplicate_event = event_frame(peer)
            duplicate_ack = ack_frame(peer)
            await peer.send(first_event, first_ack, duplicate_event, duplicate_ack)
            await signal_of(gateway, GatewaySignalType.COMMAND_ACK)
            await asyncio.sleep(0)
            assert gateway.state is GatewayState.AUTHENTICATED

            conflicting = peer.frame(
                MessageType.COMMAND_ACK,
                "command-open",
                command_id="command-open",
                trade_id="trade-open",
                action="OPEN",
                status="APPLIED",
                actual_price=Decimal("101"),
                actual_volume=Decimal("0.04"),
            )
            await peer.send(conflicting)
            violation = await signal_of(gateway, GatewaySignalType.PROTOCOL_VIOLATION)
            assert "conflicting duplicate" in violation.reason
            assert gateway.state is GatewayState.DISCONNECTED
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())


def test_hello_is_exactly_pinned_and_tuple_lock_precedes_acceptance():
    async def scenario() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(
            config=gateway_config(),
            clock=clock,
            tuple_lock_acquirer=lambda _identity: False,
        )
        await gateway.start()
        peer = None
        try:
            peer, ack = await connect_peer(gateway, clock)
            assert ack.fields["status"] == "REJECTED"
            assert "tuple lock" in ack.fields["reason"]
            await signal_of(gateway, GatewaySignalType.PROTOCOL_VIOLATION)
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

        gateway = BrokerGateway(config=gateway_config(), clock=clock)
        await gateway.start()
        peer = None
        try:
            peer, ack = await connect_peer(
                gateway,
                clock,
                epoch="epoch-2",
                hello_overrides={"broker_server": "Wrong-Server"},
            )
            assert ack.fields["status"] == "REJECTED"
            assert "broker_server" in ack.fields["reason"]
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())


def test_missing_context_quarantines_then_fresh_reconnect_restores_pending_lifecycle():
    async def scenario() -> None:
        clock = MutableClock()
        mode = {"ready": False}

        def resolver(observed):
            return context_for(observed, clock) if mode["ready"] else None

        gateway = BrokerGateway(
            config=gateway_config(),
            clock=clock,
            position_context_resolver=resolver,
        )
        await gateway.start()
        peer = None
        try:
            peer, ack = await connect_peer(gateway, clock, epoch="epoch-context-1")
            await peer.send(*full_snapshot_frames(peer, ack.msg_id))
            await signal_of(gateway, GatewaySignalType.SNAPSHOT_READY)
            quarantined = await gateway.fetch_snapshot(EXPECTED)
            assert quarantined.payload["unresolved_position_ids"] == (
                "90071992547409931",
            )
            assert quarantined.positions[0].trade_id is None
            assert quarantined.positions[0].lifecycle_stage == "QUARANTINED"
            assert gateway.state is GatewayState.AUTHENTICATED
            await peer.close()
            await signal_of(gateway, GatewaySignalType.DISCONNECTED)
            peer = None

            mode["ready"] = True
            peer, ack = await connect_peer(gateway, clock, epoch="epoch-context-2")
            await peer.send(*full_snapshot_frames(peer, ack.msg_id))
            await signal_of(gateway, GatewaySignalType.SNAPSHOT_READY)
            snapshot = await gateway.fetch_snapshot(EXPECTED)
            assert snapshot.positions[0].pending_milestone == "TP1"
            assert snapshot.positions[0].pending_close_volume == Decimal("0.02")
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())


def test_netting_account_is_rejected_for_execution_but_observable_in_shadow():
    async def execution_rejected() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(config=gateway_config(), clock=clock)
        await gateway.start()
        peer = None
        try:
            peer, ack = await connect_peer(gateway, clock, epoch="epoch-netting-demo")
            assert ack.fields["status"] == "ACCEPTED"
            await peer.send(
                *full_snapshot_frames(
                    peer,
                    ack.msg_id,
                    account_mode="NETTING",
                    position_trade_id="UNATTRIBUTED-netting",
                    pending=False,
                )
            )
            violation = await signal_of(
                gateway, GatewaySignalType.PROTOCOL_VIOLATION
            )
            assert "HEDGING" in violation.reason
            assert gateway.state is GatewayState.DISCONNECTED
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    async def shadow_observes() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(
            config=gateway_config(session_mode=BrokerSessionMode.SHADOW), clock=clock
        )
        await gateway.start()
        peer = None
        try:
            peer, ack = await connect_peer(
                gateway,
                clock,
                epoch="epoch-netting-shadow",
                hello_overrides={
                    "session_mode": BrokerSessionMode.SHADOW.value,
                    "emergency_stop": True,
                },
            )
            await peer.send(
                *full_snapshot_frames(
                    peer,
                    ack.msg_id,
                    account_mode="NETTING",
                    position_trade_id="UNATTRIBUTED-netting",
                    pending=False,
                )
            )
            await signal_of(gateway, GatewaySignalType.SNAPSHOT_READY)
            observed = await gateway.fetch_snapshot(EXPECTED)
            assert observed.payload["account"]["account_mode"] == "NETTING"
            assert gateway.state is GatewayState.AUTHENTICATED
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(execution_rejected())
    asyncio.run(shadow_observes())


def test_shadow_handshake_accepts_disabled_mt5_trading_and_advertises_local_fence():
    async def scenario() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(
            config=gateway_config(session_mode=BrokerSessionMode.SHADOW), clock=clock
        )
        await gateway.start()
        peer = None
        try:
            peer, ack = await connect_peer(
                gateway,
                clock,
                epoch="epoch-observation-only",
                hello_overrides={
                    "trade_allowed": False,
                    "expert_allowed": False,
                    "emergency_stop": True,
                    "session_mode": BrokerSessionMode.SHADOW.value,
                },
            )
            assert ack.fields["status"] == "ACCEPTED"
            assert ack.fields["expected_ea_version"] == "4.01"
            assert (
                ack.fields["expected_session_mode"]
                == BrokerSessionMode.SHADOW.value
            )
            assert gateway.state is GatewayState.AUTHENTICATED
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("gateway_mode", "ea_mode"),
    [
        (BrokerSessionMode.SHADOW, BrokerSessionMode.DEMO),
        (BrokerSessionMode.DEMO, BrokerSessionMode.SHADOW),
    ],
)
def test_gateway_rejects_ea_session_mode_mismatch(gateway_mode, ea_mode):
    async def scenario() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(
            config=gateway_config(session_mode=gateway_mode),
            clock=clock,
        )
        await gateway.start()
        peer = None
        try:
            peer, ack = await connect_peer(
                gateway,
                clock,
                epoch=f"epoch-mode-{gateway_mode.value.lower()}",
                hello_overrides={"session_mode": ea_mode.value},
            )
            assert ack.fields["status"] == "REJECTED"
            assert "session_mode" in ack.fields["reason"]
            assert (
                ack.fields["expected_session_mode"] == gateway_mode.value
            )
            violation = await signal_of(
                gateway, GatewaySignalType.PROTOCOL_VIOLATION
            )
            assert "session_mode" in violation.reason
            assert gateway.state is GatewayState.DISCONNECTED
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())


def test_gateway_quarantine_recovers_crash_lost_open_only_from_exact_in_deal(
    tmp_path,
):
    async def scenario() -> None:
        clock = MutableClock()
        paths = RuntimePaths.resolve(
            "test", data_dir=tmp_path, instance_id="gateway-open-recovery"
        )
        ledger = TradeLedger(
            paths.ensure_layout().trade_ledger_path,
            clock=lambda: clock.now_ms() / 1_000,
        ).open()
        runtime = RuntimeStateMachine(clock)
        gateway = None
        peer = None
        try:
            ledger.initialize_daily_risk_state(
                DailyRiskStateRecord(
                    account_id=EXPECTED.account_id,
                    risk_day="2027-01-15",
                    day_start_equity="1000",
                    current_equity="1000",
                    baseline_verified=True,
                    halted=False,
                    updated_at_ms=NOW - 1_000,
                    current_balance="1000",
                )
            )
            trade = TradeRecord(
                trade_id="trade-1",
                request_id="intent-1",
                account_id=EXPECTED.account_id,
                symbol=EXPECTED.symbol,
                magic_number=EXPECTED.magic,
                direction="BUY",
                decision_at=(NOW - 500) / 1_000,
                intended_entry="100",
                intended_sl="95",
                tp1="110",
                tp2="120",
                tp3="130",
                requested_volume="0.04",
                initial_risk_currency="10",
            )
            reservation = RiskReservationRecord(
                reservation_id="reservation-1",
                trade_id="trade-1",
                account_id=EXPECTED.account_id,
                risk_day="2027-01-15",
                direction="BUY",
                risk_amount="10",
                volume="0.04",
                state="RESERVED",
                created_at_ms=NOW - 500,
                expires_at_ms=NOW + 5_000,
            )
            effect = {
                "event_type": "POSITION_OPENED",
                "trade_id": "trade-1",
                "reservation_id": "reservation-1",
                "account_id": EXPECTED.account_id,
                "symbol": EXPECTED.symbol,
                "magic": EXPECTED.magic,
                "direction": "BUY",
                "initial_volume": Decimal("0.04"),
                "original_sl": Decimal("95"),
                "tp1": Decimal("110"),
                "tp2": Decimal("120"),
                "tp3": Decimal("130"),
                "initial_risk_amount": Decimal("10"),
            }
            payload = {
                "action": "OPEN",
                "trade_id": "trade-1",
                "reservation_id": "reservation-1",
                "account_id": EXPECTED.account_id,
                "symbol": EXPECTED.symbol,
                "magic": EXPECTED.magic,
                "direction": "BUY",
                "volume": Decimal("0.04"),
                "risk_amount": Decimal("10"),
                "entry": Decimal("100"),
                "sl": Decimal("95"),
                "tp1": Decimal("110"),
                "tp2": Decimal("120"),
                "tp3": Decimal("130"),
            }
            command = BrokerCommandRecord(
                command_id="command-1",
                trade_id="trade-1",
                reservation_id="reservation-1",
                action="OPEN",
                status="CREATED",
                expected_effect=effect,
                connection_epoch="epoch-crash-open",
                sequence=1,
                created_at_ms=NOW - 450,
                expires_at_ms=NOW + 4_000,
                payload=payload,
            )
            ledger.reserve_open_command_atomic(
                trade,
                (),
                reservation,
                command,
                max_total_risk="30",
                max_open_positions=3,
            )
            ledger.mark_command_sent("command-1", sent_at_ms=NOW - 300)
            ledger.mark_command_unknown(
                "command-1", unknown_at_ms=NOW - 200, reason="process crash"
            )

            runtime.start_bootstrap()
            runtime.wait_for_ea(ledger_healthy=True, provider_gate_safe=True)

            def deal_resolver(observed):
                if (
                    observed.wire_trade_id != "trade-1"
                    or observed.wire_command_id != "command-1"
                ):
                    return None
                return DurableDealAttribution(
                    resolved_at_ms=clock.now_ms(),
                    account_id=EXPECTED.account_id,
                    deal_id=observed.deal_id,
                    position_id=observed.position_id,
                    trade_id="trade-1",
                    command_id="command-1",
                )

            gateway = BrokerGateway(
                config=gateway_config(),
                clock=clock,
                committed_cursor_provider=lambda account: ledger.get_deal_cursor(
                    account
                ),
                position_context_resolver=lambda _observed: None,
                deal_attribution_resolver=deal_resolver,
                runtime=runtime,
                tuple_lock_acquirer=lambda _identity: True,
            )
            await gateway.start()
            peer, ack = await connect_peer(
                gateway, clock, epoch="epoch-crash-open"
            )
            deal_time = NOW - 100
            await peer.send(
                *full_snapshot_frames(
                    peer,
                    ack.msg_id,
                    position_trade_id="trade-1",
                    position_volume="0.04",
                    position_version=deal_time,
                    position_open_time_ms=deal_time,
                    deal_time_ms=deal_time,
                    deal_entry_type="IN",
                    deal_price="100",
                    deal_volume="0.04",
                    deal_command_id="command-1",
                    pending=False,
                )
            )
            await signal_of(gateway, GatewaySignalType.SNAPSHOT_READY)

            report = await ReconciliationService(
                ledger=ledger,
                runtime=runtime,
                broker=gateway,
                clock=clock,
            ).reconcile(EXPECTED)

            raw = report.snapshot.positions[0]
            assert raw.trade_id is None
            assert raw.lifecycle_stage == "QUARANTINED"
            assert raw.original_sl == raw.tp1 == raw.tp2 == Decimal("0")
            assert report.succeeded
            assert "UNREGISTERED_OPEN_RECOVERED" in {
                item.discrepancy_type for item in report.issues
            }
            assert ledger.get_command("command-1")["status"] == "APPLIED"
            assert ledger.get_reservation("reservation-1")["state"] == "CONSUMED"
            stored = ledger.get_owned_position(
                EXPECTED.account_id, "90071992547409931"
            )
            assert stored is not None
            assert stored["trade_id"] == "trade-1"
            assert stored["broker_order_id"] == "90071992547409934"
            assert stored["entry_price"] == "100"
            assert stored["initial_risk_amount"] == "10"
            assert runtime.state is ControlPlaneState.READY
        finally:
            if peer is not None:
                await peer.close()
            if gateway is not None:
                await gateway.close()
            ledger.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["stale", "mismatch", "regression"])
def test_stale_or_mismatched_durable_context_fails_closed(failure: str):
    async def scenario() -> None:
        clock = MutableClock()

        def resolver(observed):
            kwargs = {}
            if failure == "stale":
                kwargs["resolved_at_ms"] = clock.now_ms() - 501
            elif failure == "mismatch":
                kwargs["trade_id"] = "different-trade"
            context = context_for(observed, clock, **kwargs)
            if failure == "regression":
                context = DurablePositionContext(
                    **{
                        name: getattr(context, name)
                        for name in context.__dataclass_fields__
                        if name != "position_version"
                    },
                    position_version=context.position_version + 1,
                )
            return context

        gateway = BrokerGateway(
            config=gateway_config(),
            clock=clock,
            position_context_resolver=resolver,
        )
        await gateway.start()
        peer = None
        try:
            peer, ack = await connect_peer(gateway, clock, epoch=f"epoch-{failure}")
            await peer.send(*full_snapshot_frames(peer, ack.msg_id))
            await signal_of(gateway, GatewaySignalType.CACHE_FAILURE)
            assert gateway.state is GatewayState.DISCONNECTED
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())


def test_broker_ahead_position_version_is_accepted_for_crash_recovery():
    async def scenario() -> None:
        clock = MutableClock()
        durable_version: list[int] = []

        def resolver(observed):
            context = context_for(observed, clock)
            durable_version.append(context.position_version - 1)
            return DurablePositionContext(
                **{
                    name: getattr(context, name)
                    for name in context.__dataclass_fields__
                    if name != "position_version"
                },
                position_version=durable_version[-1],
            )

        gateway = BrokerGateway(
            config=gateway_config(),
            clock=clock,
            position_context_resolver=resolver,
        )
        await gateway.start()
        peer = None
        try:
            peer, ack = await connect_peer(gateway, clock, epoch="epoch-broker-ahead")
            broker_version = clock.now_ms() - 1_000
            await peer.send(
                *full_snapshot_frames(
                    peer,
                    ack.msg_id,
                    position_version=broker_version,
                )
            )
            await signal_of(gateway, GatewaySignalType.SNAPSHOT_READY)
            position = (await gateway.fetch_snapshot(EXPECTED)).positions[0]
            assert durable_version == [broker_version - 1]
            assert position.position_version == broker_version
            assert gateway.state is GatewayState.AUTHENTICATED
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())


def test_unattributed_position_is_visible_quarantined_without_inferred_initial_targets():
    async def scenario() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(config=gateway_config(), clock=clock)
        await gateway.start()
        peer = None
        try:
            peer, ack = await connect_peer(gateway, clock)
            await peer.send(
                *full_snapshot_frames(
                    peer, ack.msg_id, position_trade_id="UNATTRIBUTED-123"
                )
            )
            await signal_of(gateway, GatewaySignalType.SNAPSHOT_READY)
            position = (await gateway.fetch_snapshot(EXPECTED)).positions[0]
            assert position.trade_id is None
            assert position.lifecycle_stage == "QUARANTINED"
            assert position.original_sl == position.tp1 == position.tp2 == Decimal("0")
            assert position.current_sl == Decimal("95")
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())


def test_complete_account_snapshot_keeps_foreign_exposure_visible_for_reconciliation():
    async def scenario() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(config=gateway_config(), clock=clock)
        await gateway.start()
        peer = None
        try:
            peer, ack = await connect_peer(
                gateway, clock, epoch="epoch-foreign-exposure"
            )
            correlation = ack.msg_id
            snapshot_id = "positions-foreign"
            order_snapshot_id = "orders-foreign"
            position_id = "771"
            order_id = "772"
            await peer.send(
                peer.frame(
                    MessageType.ACCOUNT_SNAPSHOT,
                    correlation,
                    server_time_ms=clock.now_ms(),
                    risk_day="2027-01-15",
                    account_mode="HEDGING",
                ),
                peer.frame(
                    MessageType.SYMBOL_SNAPSHOT,
                    correlation,
                    tick_value_profit=Decimal("1.5"),
                    tick_value_loss=Decimal("1.8"),
                    contract_size=Decimal("100"),
                ),
                peer.frame(
                    MessageType.POSITION_SNAPSHOT_BEGIN,
                    correlation,
                    snapshot_id=snapshot_id,
                    count=1,
                    deal_cursor="0:0",
                ),
                peer.frame(
                    MessageType.POSITION_SNAPSHOT_ITEM,
                    correlation,
                    snapshot_id=snapshot_id,
                    position_id=position_id,
                    position_ticket="771",
                    trade_id=f"UNATTRIBUTED-{position_id}",
                    symbol="EURUSD",
                    magic=0,
                    direction="SELL",
                    volume=Decimal("0.20"),
                    price_open=Decimal("1.10000"),
                    sl=Decimal("0"),
                    tp=Decimal("0"),
                    position_version=clock.now_ms() - 10,
                    open_time_ms=clock.now_ms() - 1_000,
                    comment="manual",
                ),
                peer.frame(
                    MessageType.POSITION_SNAPSHOT_END,
                    correlation,
                    snapshot_id=snapshot_id,
                    count=1,
                    deal_cursor="0:0",
                ),
                peer.frame(
                    MessageType.PENDING_ORDER_SNAPSHOT_BEGIN,
                    correlation,
                    snapshot_id=order_snapshot_id,
                    count=1,
                ),
                peer.frame(
                    MessageType.PENDING_ORDER_SNAPSHOT_ITEM,
                    correlation,
                    snapshot_id=order_snapshot_id,
                    order_id=order_id,
                    trade_id=f"UNATTRIBUTED-{order_id}",
                    command_id="BROKER",
                    symbol="EURUSD",
                    magic=0,
                    order_type="SELL_LIMIT",
                    order_state="PLACED",
                    volume_initial=Decimal("0.10"),
                    volume_current=Decimal("0.10"),
                    price_open=Decimal("1.11000"),
                    sl=Decimal("0"),
                    tp=Decimal("0"),
                    order_time_ms=clock.now_ms() - 500,
                    expiration_time_ms=0,
                    comment="manual",
                ),
                peer.frame(
                    MessageType.PENDING_ORDER_SNAPSHOT_END,
                    correlation,
                    snapshot_id=order_snapshot_id,
                    count=1,
                ),
                peer.frame(
                    MessageType.DEAL_REPLAY_BEGIN,
                    correlation,
                    replay_id="replay-foreign",
                    cursor_from="0:0",
                    count=0,
                ),
                peer.frame(
                    MessageType.DEAL_REPLAY_END,
                    correlation,
                    replay_id="replay-foreign",
                    cursor_next="0:0",
                    count=0,
                ),
            )
            await signal_of(gateway, GatewaySignalType.SNAPSHOT_READY)
            observed = await gateway.fetch_snapshot(EXPECTED)
            assert len(observed.positions) == len(observed.pending_orders) == 1
            assert observed.positions[0].symbol == "EURUSD"
            assert observed.positions[0].magic == 0
            assert observed.positions[0].trade_id is None
            assert observed.positions[0].lifecycle_stage == "QUARANTINED"
            assert observed.pending_orders[0].symbol == "EURUSD"
            assert observed.pending_orders[0].magic == 0
            assert observed.pending_orders[0].trade_id is None
            assert observed.pending_orders[0].command_id is None
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())


def test_partial_generation_sequence_gap_stale_heartbeat_and_cache_event_fail_closed():
    async def partial_timeout() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(
            config=gateway_config(snapshot_timeout_ms=100, heartbeat_timeout_ms=500),
            clock=clock,
        )
        await gateway.start()
        peer = None
        try:
            peer, _ = await connect_peer(gateway, clock, epoch="epoch-partial")
            clock.advance(101)
            await gateway.check_health()
            violation = await signal_of(gateway, GatewaySignalType.PROTOCOL_VIOLATION)
            assert "partial snapshot/replay" in violation.reason
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    async def sequence_gap() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(config=gateway_config(), clock=clock)
        await gateway.start()
        peer = None
        try:
            peer, _ = await connect_peer(gateway, clock, epoch="epoch-gap")
            peer.sequence += 1
            await peer.send(peer.frame(MessageType.HEARTBEAT))
            violation = await signal_of(gateway, GatewaySignalType.PROTOCOL_VIOLATION)
            assert "non-contiguous" in violation.reason
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    async def stale_heartbeat() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(
            config=gateway_config(
                heartbeat_timeout_ms=100, snapshot_timeout_ms=500
            ),
            clock=clock,
        )
        await gateway.start()
        peer = None
        try:
            peer, _ = await connect_peer(gateway, clock, epoch="epoch-stale")
            clock.advance(101)
            await gateway.check_health()
            await signal_of(gateway, GatewaySignalType.STALE_HEARTBEAT)
            assert gateway.state is GatewayState.DISCONNECTED
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    async def cache_event() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(config=gateway_config(), clock=clock)
        await gateway.start()
        peer = None
        try:
            peer, _ = await connect_peer(gateway, clock, epoch="epoch-cache")
            await peer.send(
                peer.frame(
                    MessageType.BROKER_EVENT,
                    event_id="cache-failure-1",
                    event_type="COMMAND_CACHE_FAILURE",
                    command_id="BROKER",
                    trade_id="UNATTRIBUTED-0",
                )
            )
            await signal_of(gateway, GatewaySignalType.CACHE_FAILURE)
            assert gateway.state is GatewayState.DISCONNECTED
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(partial_timeout())
    asyncio.run(sequence_gap())
    asyncio.run(stale_heartbeat())
    asyncio.run(cache_event())


def test_stale_cached_snapshot_is_not_reused_and_one_authenticated_peer_is_enforced():
    async def scenario() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(
            config=gateway_config(max_message_age_ms=100),
            clock=clock,
            position_context_resolver=lambda observed: context_for(observed, clock),
        )
        await gateway.start()
        peer = None
        second_writer = None
        try:
            peer, ack = await connect_peer(gateway, clock)
            second_reader, second_writer = await asyncio.open_connection(
                "127.0.0.1", gateway.listen_port
            )
            assert await asyncio.wait_for(second_reader.read(), 1) == b""

            await peer.send(*full_snapshot_frames(peer, ack.msg_id))
            await signal_of(gateway, GatewaySignalType.SNAPSHOT_READY)
            await gateway.fetch_deal_page(EXPECTED.account_id, None)
            clock.advance(101)

            fetch = asyncio.create_task(gateway.fetch_snapshot(EXPECTED))
            request = await peer.receive()
            assert request.message_type is MessageType.COMMAND
            assert request.fields["action"] == "REQUEST_SNAPSHOT"
            await peer.send(
                *full_snapshot_frames(
                    peer,
                    str(request.fields["command_id"]),
                    requested_cursor="0:0",
                )
            )
            refreshed = await asyncio.wait_for(fetch, 1)
            assert request.fields["command_id"] in refreshed.snapshot_id
        finally:
            if second_writer is not None:
                second_writer.close()
                await second_writer.wait_closed()
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())


def test_send_rejects_wrong_epoch_without_writing():
    async def scenario() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(config=gateway_config(), clock=clock)
        await gateway.start()
        peer = None
        try:
            peer, _ = await connect_peer(gateway, clock)
            with pytest.raises(GatewayDisconnectedError):
                await gateway.send(make_open_command("stale-epoch"))
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())


def test_transport_disconnect_invalidates_runtime_before_disconnect_signal():
    async def scenario() -> None:
        clock = MutableClock()
        runtime = RuntimeStateMachine(clock)
        runtime.start_bootstrap()
        runtime.wait_for_ea(ledger_healthy=True, provider_gate_safe=True)
        gateway = BrokerGateway(
            config=gateway_config(),
            clock=clock,
            runtime=runtime,
            tuple_lock_acquirer=lambda _identity: True,
        )
        await gateway.start()
        peer = None
        try:
            peer, _ = await connect_peer(gateway, clock, epoch="epoch-runtime-fence")
            assert runtime.state is ControlPlaneState.SYNCHRONIZING
            runtime.mark_synchronized(
                snapshot_id="snapshot-1",
                reconciliation_run_id="reconciliation-1",
                deal_cursor_initialized=True,
                daily_baseline_verified=True,
                daily_risk_halted=False,
            )
            assert runtime.state is ControlPlaneState.READY

            await peer.close()
            peer = None
            disconnected = await signal_of(gateway, GatewaySignalType.DISCONNECTED)
            assert disconnected.connection_epoch == "epoch-runtime-fence"
            assert runtime.state is ControlPlaneState.DISCONNECTED
            assert runtime.evidence.ea_connected is False
            assert runtime.evidence.connection_epoch is None
            assert runtime.evidence.identity_verified is False
            assert runtime.evidence.tuple_lock_acquired is False
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())


def test_completed_bar_cache_rolls_per_timeframe_and_tick_quote_uses_receive_time():
    async def scenario() -> None:
        clock = MutableClock()
        gateway = BrokerGateway(
            config=gateway_config(max_completed_bars_per_timeframe=1), clock=clock
        )
        await gateway.start()
        peer = None
        try:
            peer, _ = await connect_peer(gateway, clock, epoch="epoch-bars")
            for open_time in (clock.now_ms() - 120_000, clock.now_ms() - 60_000):
                await peer.send(
                    peer.frame(
                        MessageType.BAR,
                        symbol="XAUUSD",
                        tf="M1",
                        o=Decimal("100"),
                        h=Decimal("101"),
                        l=Decimal("99"),
                        c=Decimal("100.5"),
                        volume=10,
                        bar_time_ms=open_time,
                    )
                )
            for _ in range(20):
                await asyncio.sleep(0)
            assert len(gateway.completed_bars) == 1
            assert gateway.completed_bars[0].open_time_ms == clock.now_ms() - 60_000
        finally:
            if peer is not None:
                await peer.close()
            await gateway.close()

    asyncio.run(scenario())

    tick = BrokerTick(
        msg_id="tick-local-receipt",
        connection_epoch="epoch",
        symbol="XAUUSD",
        received_at_ms=NOW,
        message_ts_ms=NOW - 20,
        broker_time_ms=NOW - 40,
        bid="100",
        ask="100.1",
        spread_points="10",
        volume=1,
    )
    assert tick.quote_snapshot().captured_at_ms == NOW
