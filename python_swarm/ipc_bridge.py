"""
QUANT-SWARM-XAU: IPC Bridge
Named Pipe server (Windows) / Unix socket (Linux/Mac) for MQL5 ↔ Python communication.
Handles reconnection, message framing, and async dispatch.
"""

import asyncio
import json
import logging
import platform
import time
from typing import Callable, Optional

try:
    from .legacy_runtime_guard import require_non_broker_mode
except ImportError:  # Direct legacy-script execution.
    from legacy_runtime_guard import require_non_broker_mode

log = logging.getLogger("IPC-BRIDGE")

IS_WINDOWS = platform.system() == "Windows"
PIPE_NAME  = r"\\.\pipe\QuantSwarmXAU" if IS_WINDOWS else "/tmp/quant_swarm_xau.sock"
HOST, PORT = "127.0.0.1", 9876  # TCP fallback


class IPCBridge:
    """
    Bidirectional async IPC bridge between MQL5 EA and Python swarm.
    Primary: Named Pipe (Windows) | Unix socket (Linux)
    Fallback: TCP localhost socket
    """

    def __init__(self, on_message: Callable, on_connect: Callable = None,
                 on_disconnect: Callable = None):
        self.on_message     = on_message
        self.on_connect     = on_connect or (lambda: None)
        self.on_disconnect  = on_disconnect or (lambda: None)
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected     = False
        self._send_queue: asyncio.Queue = asyncio.Queue()

    async def start_server(self):
        """Start IPC server — always TCP so Wine/MT5 can connect."""
        require_non_broker_mode("IPCBridge")
        # Always use TCP regardless of OS — MT5 under Wine needs TCP
        server = await asyncio.start_server(
            self._handle_client, HOST, PORT
        )
        log.info("IPC TCP server listening on %s:%d", HOST, PORT)
        print(f"[IPC] TCP server ready — waiting for MT5 on {HOST}:{PORT}")
        print(f"[IPC] MT5 EA setting: InpFallbackSocket = {HOST}:{PORT}")

        async with server:
            await server.serve_forever()

    async def _handle_client(self, reader: asyncio.StreamReader,
                              writer: asyncio.StreamWriter):
        self._writer = writer
        self._connected = True
        peer = writer.get_extra_info("peername", "EA")
        log.info("MQL5 EA connected from: %s", peer)
        self.on_connect()
        
        # Start parallel send worker
        send_task = asyncio.create_task(self._send_worker())
        
        try:
            while True:
                # Read line-delimited JSON
                data = await asyncio.wait_for(reader.readline(), timeout=65.0)
                if not data:
                    break
                
                raw = data.decode("utf-8", errors="replace").strip()
                if raw:
                    await self._dispatch(raw)
        
        except asyncio.TimeoutError:
            log.warning("EA heartbeat timeout — connection assumed dropped")
        except ConnectionResetError:
            log.warning("EA disconnected unexpectedly")
        finally:
            send_task.cancel()
            self._connected = False
            self._writer = None
            writer.close()
            await writer.wait_closed()
            self.on_disconnect()
            log.info("EA client disconnected. Waiting for reconnect...")

    async def _dispatch(self, raw: str):
        """Parse and dispatch incoming message from EA."""
        try:
            msg = json.loads(raw)
            msg_type = msg.get("type", "UNKNOWN")
            
            if msg_type == "HEARTBEAT":
                # Echo ACK immediately
                await self.send({"type": "HEARTBEAT_ACK", "ts": time.time()})
            else:
                await self.on_message(msg)
        except json.JSONDecodeError as e:
            log.error("JSON parse error: %s | Raw: %s", e, raw[:100])

    async def send(self, payload: dict):
        """Queue a message for sending to EA."""
        await self._send_queue.put(payload)

    async def _send_worker(self):
        """Async worker flushing send queue to EA."""
        while True:
            payload = await self._send_queue.get()
            if self._writer and not self._writer.is_closing():
                try:
                    line = json.dumps(payload) + "\n"
                    self._writer.write(line.encode())
                    await self._writer.drain()
                except Exception as e:
                    log.error("Send error: %s", e)
            self._send_queue.task_done()

    @property
    def connected(self) -> bool:
        return self._connected


# ─── Main Orchestrator ────────────────────────────────────────────────────────

class SwarmOrchestrator:
    """
    Top-level coordinator wiring all agents, IPC bridge, and Telegram.
    Entry point for production deployment.
    """

    def __init__(self):
        from confluence_engine import (
            AgentJournal, ConfluenceFusionEngine,
            SwarmMetaReviewer, InstitutionalRiskController, AGENT_REGISTRY
        )
        from telegram_liaison import TelegramLiaison

        self.journals  = {aid: AgentJournal(aid) for aid in AGENT_REGISTRY}
        self.fusion    = ConfluenceFusionEngine(self.journals)
        self.reviewer  = SwarmMetaReviewer(self.journals, self.fusion)
        self.risk      = InstitutionalRiskController(account_balance=10000.0)
        self.telegram  = TelegramLiaison()
        self.bridge    = IPCBridge(
            on_message    = self._handle_ea_message,
            on_connect    = self._on_ea_connect,
            on_disconnect = self._on_ea_disconnect
        )
        self._account_equity = 10000.0

    async def _on_ea_connect(self):
        log.info("EA connected. Swarm operational.")

    async def _on_ea_disconnect(self):
        await self.telegram.send_disconnect_alert("MQL5_EA")

    async def _handle_ea_message(self, msg: dict):
        """Route messages from EA to appropriate handlers."""
        msg_type = msg.get("type")
        
        if msg_type == "HANDSHAKE":
            log.info(
                "EA handshake: symbol=%s min_lot=%s step=%s max_lot=%s contract=%s",
                msg.get("symbol"),
                msg.get("volume_min"),
                msg.get("volume_step"),
                msg.get("volume_max"),
                msg.get("contract_size"),
            )

        elif msg_type == "HEARTBEAT":
            self._account_equity = msg.get("equity", self._account_equity)
        
        elif msg_type == "EXEC_ACK":
            log.info("Trade ACK: Ticket=%s Strategy=%s Price=%s",
                     msg.get("ticket"), msg.get("strategy"), msg.get("price"))
        
        elif msg_type == "EXEC_FAIL":
            log.error("Trade FAIL: %s", msg.get("reason"))
        
        elif msg_type == "EMERGENCY_HALT":
            await self.telegram.send_emergency_halt(
                reason   = msg.get("reason", "UNKNOWN"),
                equity   = self._account_equity,
                daily_dd = 0.0
            )
        
        elif msg_type == "TRADE_CLOSED":
            # Schedule PMA for originating agent
            from confluence_engine import TradeTelemetry
            t = TradeTelemetry(
                trade_id        = msg.get("trade_id", ""),
                agent_id        = msg.get("agent_id", 7),
                direction       = msg.get("direction", "BUY"),
                entry_price     = msg.get("entry_price", 0),
                exit_price      = msg.get("exit_price", 0),
                sl_price        = msg.get("sl_price", 0),
                outcome_r       = msg.get("outcome_r", 0),
                slippage_pts    = msg.get("slippage_pts", 0),
                session         = msg.get("session", "LONDON"),
                volume_at_entry = msg.get("volume_at_entry", 0),
                htf_aligned     = msg.get("htf_aligned", True),
                news_delta      = msg.get("news_delta", 0),
                time_of_day     = msg.get("time_of_day", "09:00"),
                duration_minutes= msg.get("duration_minutes", 0),
                timestamp_open  = msg.get("timestamp_open", time.time()),
                timestamp_close = time.time()
            )
            await self.reviewer.schedule_pma(t)

    async def submit_trade_signal(self, fusion_result: dict):
        """Send validated fusion signal to EA for execution."""
        lot_size = self.risk.calculate_lot_size(
            fusion_result["entry"],
            fusion_result["sl"],
            fusion_result["confluence_pct"]
        )
        if lot_size < self.risk.min_lot:
            log.info("Trade rejected: min lot exceeds risk budget")
            return
        
        cmd = {
            "cmd_type":      "OPEN",
            "direction":     fusion_result["direction"],
            "entry_price":   fusion_result["entry"],
            "sl_price":      fusion_result["sl"],
            "tp1_price":     fusion_result["tp1"],
            "tp2_price":     fusion_result["tp2"],
            "tp3_price":     fusion_result["tp3"],
            "lot_size":      lot_size,
            "strategy_tags": "+".join(fusion_result.get("strategy_tags", [])),
            "confidence":    fusion_result["confluence_pct"] / 100.0,
            "ticket":        0
        }
        
        await self.bridge.send(cmd)
        
        # Broadcast to Telegram
        await self.telegram.send_trade_signal(
            fusion_result, lot_size,
            risk_pct       = self.risk.max_risk_pct,
            account_equity = self._account_equity
        )

    async def run(self):
        """Main async entry point — runs all loops concurrently."""
        log.info("QUANT-SWARM-XAU starting up...")
        await asyncio.gather(
            self.bridge.start_server(),
            self.reviewer.pma_dispatch_loop(),
        )


if __name__ == "__main__":
    raise SystemExit(
        "The legacy unauthenticated IPC bridge is disabled. "
        "Use the single canonical v4 runtime."
    )
