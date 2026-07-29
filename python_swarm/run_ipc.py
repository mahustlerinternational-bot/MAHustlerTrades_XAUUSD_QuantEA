#!/usr/bin/env python3
"""
QUANT-SWARM-XAU — Standalone IPC TCP Bridge
Runs a clean TCP server on 127.0.0.1:9876
MT5 (Wine) connects here. Forwards messages to/from the swarm.

Usage:
    python3 run_ipc.py
"""
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path

try:
    from .legacy_runtime_guard import require_non_broker_mode
except ImportError:
    from legacy_runtime_guard import require_non_broker_mode

HOST = "127.0.0.1"
PORT = 9876

log = logging.getLogger("IPC")

# ── Shared state writable by both sides ──────────────────────────────────────
class IPCState:
    writer = None          # active TCP connection to MT5
    connected = False
    tick_count = 0
    last_tick  = 0.0
    equity     = 0.0
    positions  = 0
    daily_pnl  = 0.0
    messages_in  = 0
    messages_out = 0

state = IPCState()

# ── Message handlers ─────────────────────────────────────────────────────────

def handle_message(raw: str):
    """Process a message received from MT5."""
    try:
        msg = json.loads(raw.strip())
        mtype = msg.get("type","")

        if mtype == "TICK":
            state.tick_count += 1
            state.last_tick   = time.time()

        elif mtype == "HEARTBEAT":
            state.equity    = msg.get("equity", 0)
            state.positions = msg.get("positions", 0)
            state.daily_pnl = msg.get("daily_pnl", 0)
            log.debug("HB equity=%.2f pos=%d", state.equity, state.positions)

        elif mtype == "EXEC_ACK":
            log.info("✅ ORDER FILLED: ticket=%s dir=%s entry=%.2f lot=%.2f",
                     msg.get("ticket"), msg.get("direction"),
                     msg.get("fill_price", 0), msg.get("lot_size", 0))

        elif mtype == "TRADE_CLOSED":
            log.info("🔴 TRADE CLOSED: ticket=%s reason=%s pnl=%.2f",
                     msg.get("ticket"), msg.get("reason"),
                     msg.get("pnl_usd", 0))

        elif mtype == "EMERGENCY_HALT":
            log.warning("🚨 EMERGENCY HALT: %s", msg.get("reason"))

        elif mtype == "REQUOTE":
            log.warning("⚠️  REQUOTE: ticket=%s", msg.get("ticket"))

        else:
            log.debug("MSG type=%s", mtype)

        state.messages_in += 1

    except json.JSONDecodeError:
        pass  # incomplete message, skip
    except Exception as e:
        log.debug("Handle error: %s", e)


async def send_to_mt5(cmd: dict):
    """Send a command to MT5."""
    if state.writer and not state.writer.is_closing():
        try:
            data = json.dumps(cmd) + "\n"
            state.writer.write(data.encode())
            await state.writer.drain()
            state.messages_out += 1
            return True
        except Exception as e:
            log.warning("Send failed: %s", e)
    return False


# ── TCP server ────────────────────────────────────────────────────────────────

async def handle_client(reader: asyncio.StreamReader,
                         writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    log.info("✅ MT5 CONNECTED from %s:%s", *addr)
    print(f"\n  ✅  MT5 EA connected from {addr[0]}:{addr[1]}")
    print(f"  ✅  IPC bridge is live — swarm receiving tick data\n")

    state.writer    = writer
    state.connected = True

    # Send handshake
    await send_to_mt5({"type": "HANDSHAKE", "version": "3.0",
                        "timestamp": int(time.time())})

    buf = ""
    try:
        while True:
            data = await reader.read(4096)
            if not data:
                break
            buf += data.decode(errors="replace")
            # Process complete lines
            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                if line.strip():
                    handle_message(line)
    except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError):
        pass
    except Exception as e:
        log.debug("Client handler: %s", e)
    finally:
        state.connected = False
        state.writer    = None
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        log.info("❌ MT5 disconnected from %s:%s", *addr)
        print(f"\n  ❌  MT5 disconnected — check Experts tab and re-attach EA\n")


# ── Status printer ────────────────────────────────────────────────────────────

async def status_loop():
    """Print connection status every 10 seconds."""
    await asyncio.sleep(3)
    while True:
        if state.connected:
            log.info("STATUS | equity=$%.2f | pos=%d | ticks=%d | msg_in=%d",
                     state.equity, state.positions,
                     state.tick_count, state.messages_in)
        else:
            log.info("STATUS | Waiting for MT5 connection on %s:%d ...", HOST, PORT)
        await asyncio.sleep(10)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    require_non_broker_mode("run_ipc")
    print()
    print("═"*60)
    print("  QUANT-SWARM-XAU v3.0 — IPC TCP Bridge")
    print(f"  Listening on {HOST}:{PORT}")
    print("═"*60)
    print()
    print("  ► In MT5 EA settings set:")
    print(f"    InpFallbackSocket = {HOST}:{PORT}")
    print()
    print("  ► Waiting for MT5 to connect...")
    print()

    server = await asyncio.start_server(handle_client, HOST, PORT)

    async with server:
        await asyncio.gather(
            server.serve_forever(),
            status_loop(),
        )


if __name__ == "__main__":
    raise SystemExit(
        "The unauthenticated v3 standalone IPC bridge is disabled. "
        "Use the single canonical v4 runtime."
    )
