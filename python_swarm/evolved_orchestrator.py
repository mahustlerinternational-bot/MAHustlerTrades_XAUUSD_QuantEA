"""
QUANT-SWARM-XAU v3.0: Evolved Orchestrator
Extends the base orchestrator with full regime-aware continuous learning.

What's new vs v2.0:
  - MarketRegimeDetector classifies current market every 60s
  - AdaptiveParameterRouter adjusts confluence gate, risk, TP per regime
  - RegimeMemory accumulates per-(regime × strategy × session) performance
  - ForwardOptimiser pre-calibrates each session 4h in advance
  - MarketCalendarIntelligence provides day/month seasonality priors
  - All agent conviction scores multiplied by learned regime multipliers
  - Regime transitions logged and trigger Telegram notifications
  - Weekly digest includes regime frequency breakdown and transition map
"""

import asyncio
import os
import pathlib
import json
import logging
import sys
import time
from pathlib import Path
from typing import Optional

# Base orchestrator
sys.path.insert(0, str(Path(__file__).parent))

from safe_paths import bootstrap_ephemeral_state
try:
    from .legacy_runtime_guard import require_non_broker_mode
except ImportError:  # Direct legacy-script execution.
    from legacy_runtime_guard import require_non_broker_mode

if __name__ == "__main__" and "--demo" in sys.argv:
    bootstrap_ephemeral_state("evolved-orchestrator-demo")

from regime_detector import (
    MarketRegimeDetector, AdaptiveParameterRouter, RegimeState
)
from continuous_learner import (
    ContinuousAdaptationEngine, RegimeMemory,
    MarketCalendarIntelligence, ForwardOptimiser
)
from confluence_engine import (
    AgentJournal, ConfluenceFusionEngine, SwarmMetaReviewer,
    AGENT_REGISTRY, DB_DIR, TradeTelemetry
)
from agent_strategy_cores import MarketSnapshot, build_strategy_agents
from remaining_agents import (
    TrendlineBreakoutAgent, VolumeProfileAgent, MeanReversionAgent,
    TimeInvalidationGuard, LiquidityPoolMapper, MTFTrendSynthesizer
)
from data_ingestion_agents import (
    TickStreamerAgent, MacroCatalystMonitor, InterMarketWatcher,
    LocalEventBus, CH_TRADE_CMD
)
from risk_and_lifecycle import (
    InstitutionalRiskController, TradeLifecycleManager,
    ManagedPosition, TradeState
)
from resilience import ResilienceController

log = logging.getLogger("EVOLVED-ORCHESTRATOR")


class EvolvedOrchestrator:
    """
    The continuously self-improving production runtime.
    Every subsystem is regime-aware and journal-backed.
    """

    ANALYSIS_INTERVAL_S  = 1.0
    REGIME_UPDATE_S      = 60.0
    WEIGHT_UPDATE_S      = 300.0

    def __init__(self, config: dict = None):
        require_non_broker_mode("EvolvedOrchestrator")
        self.cfg  = config or {}
        self.bus  = LocalEventBus()
        self._running      = False
        self._start_time   = time.time()
        self._trade_count  = 0
        self._wins = self._losses = 0
        self._daily_pnl    = 0.0
        self._latest_bid   = 0.0
        self._latest_ask   = 0.0
        self._ea_halted    = False
        self._ea_halt_reason = ""
        self._equity_hist: list[float] = []
        self._adapt_log:   list[dict]  = []

        # ── v2.0 subsystems ─────────────────────────────────────────────
        log.info("Initializing QUANT-SWARM-XAU v3.0 (Evolved)...")
        self.journals  = {aid: AgentJournal(aid) for aid in AGENT_REGISTRY}
        self.fusion    = ConfluenceFusionEngine(self.journals)
        self.reviewer  = SwarmMetaReviewer(self.journals, self.fusion)

        self.strategy_agents = build_strategy_agents(self.journals)
        self.strategy_agents[5]  = TrendlineBreakoutAgent(5,  self.journals[5])
        self.strategy_agents[8]  = VolumeProfileAgent(8,      self.journals[8])
        self.strategy_agents[11] = MeanReversionAgent(11,     self.journals[11])

        self.time_guard  = TimeInvalidationGuard()
        self.liq_mapper  = LiquidityPoolMapper()
        self.mtf_synth   = MTFTrendSynthesizer()

        async def _send_cmd(cmd): await self.bus.publish(CH_TRADE_CMD, cmd)
        balance = self.cfg.get("risk", {}).get("initial_balance", 10_000.0)
        self.risk_ctrl = InstitutionalRiskController(balance, _send_cmd)
        self.lifecycle = TradeLifecycleManager(_send_cmd, self.risk_ctrl)
        self.resilience = ResilienceController()
        self.tick_stream = TickStreamerAgent(self.bus)
        self.macro_mon   = MacroCatalystMonitor(self.bus)
        self.im_watch    = InterMarketWatcher(self.bus)

        # ── v3.0 continuous learning subsystems ─────────────────────────
        self.adapt_engine  = ContinuousAdaptationEngine()
        self.regime_router = self.adapt_engine.router

        log.info("v3.0 Evolved Orchestrator ready — regime-aware continuous learning active")

    # ── Regime-Aware Analysis Loop ────────────────────────────────────────────

    async def _analysis_loop(self):
        _loop_count = 0
        while self._running:
            _loop_count += 1
            snap = self.tick_stream.get_latest_snapshot()
            if snap is None:
                if _loop_count % 30 == 0:
                    log.debug("Analysis loop: no snapshot yet — waiting for ticks")
                await asyncio.sleep(self.ANALYSIS_INTERVAL_S)
                continue

            # ── Get live regime parameters ────────────────────────────────
            live_params = self.adapt_engine.get_live_params()
            regime_conf_gate  = live_params.get("confluence_threshold", 75.0)
            regime_risk_mult  = live_params.get("effective_risk_pct", 1.0)
            regime_tp_ext     = live_params.get("tp_extension", 1.0)
            preferred_strats  = live_params.get("preferred_strategies", [])
            muted_strats      = live_params.get("muted_strategies", [])
            current_regime    = live_params.get("regime", "unknown")

            # Gate checks — log every failure so we know what's blocking
            spread_ok = self.resilience.check_spread(snap.spread_pts)
            if not spread_ok:
                if _loop_count % 30 == 0:
                    log.debug("GATE BLOCKED: spread=%.2f pts above limit", snap.spread_pts)
                await asyncio.sleep(self.ANALYSIS_INTERVAL_S); continue

            macro = self.bus.latest("macro.events") or {}
            if macro.get("freeze"):
                if _loop_count % 30 == 0:
                    log.info("GATE BLOCKED: macro freeze active — %s", macro.get("next_event_name","?"))
                await asyncio.sleep(self.ANALYSIS_INTERVAL_S); continue

            if not self.time_guard.validate(snap):
                if _loop_count % 60 == 0:
                    log.debug("GATE BLOCKED: time guard — session=%s", snap.session)
                await asyncio.sleep(self.ANALYSIS_INTERVAL_S); continue

            if self.lifecycle.open_positions:
                await self._manage_positions(snap)
                await asyncio.sleep(self.ANALYSIS_INTERVAL_S); continue

            # ── Run strategy agents with regime multipliers ───────────────
            from confluence_engine import AgentSignal
            signals = []
            neutral_count = 0
            exception_agents = []
            stale_count = 0

            for aid, agent in self.strategy_agents.items():
                agent_name = AGENT_REGISTRY[aid]["name"]

                # Skip muted strategies for this regime
                if any(m.lower() in agent_name.lower() for m in muted_strats):
                    continue

                try:
                    sig = agent.analyze(snap)
                except Exception as _ae:
                    exception_agents.append(f"{agent_name}:{type(_ae).__name__}:{_ae}")
                    sig = None

                if sig is None:
                    continue

                if sig.direction == "NEUTRAL":
                    neutral_count += 1
                    continue

                if not self.resilience.validate_signal_freshness(sig.timestamp):
                    stale_count += 1
                    continue

                # Apply learned regime conviction multiplier
                session = snap.session
                mult = self.adapt_engine.get_agent_conviction_mult(
                    agent_name, session
                )
                # Preferred strategies get regime routing boost on top
                if any(p.lower() in agent_name.lower() for p in preferred_strats):
                    mult = min(mult * 1.15, 1.4)

                sig.conviction = min(sig.conviction * mult, 100.0)
                signals.append(sig)

            # Log agent signal summary every 60s for diagnostics
            if _loop_count % 60 == 0 or len(signals) >= 3:
                log.info(
                    "SWARM STATUS | regime=%s gate=%.0f%% | signals=%d neutral=%d stale=%d exceptions=%d | snap_age=%.1fs bid=%.3f",
                    current_regime, regime_conf_gate,
                    len(signals), neutral_count, stale_count, len(exception_agents),
                    time.time() - snap.timestamp, snap.bid
                )
            if exception_agents and _loop_count % 30 == 0:
                for err in exception_agents[:3]:
                    log.warning("Agent exception: %s", err)

            # ── Fuse with regime-adjusted gate ───────────────────────────
            if len(signals) >= 3:
                log.info("FUSION ATTEMPT | %d signals | gate=%.0f%% | regime=%s",
                         len(signals), regime_conf_gate, current_regime)
                # Temporarily override fusion threshold for this regime
                original_thresh = self.fusion.__class__.__dict__.get(
                    "MIN_CONFLUENCE_SCORE", 75.0
                )
                import confluence_engine as ce
                ce.MIN_CONFLUENCE_SCORE = regime_conf_gate

                result = self.fusion.fuse_signals(signals)

                ce.MIN_CONFLUENCE_SCORE = original_thresh  # restore

                if result:
                    log.info("FUSION PASSED | dir=%s conf=%.1f%% entry=%.3f sl=%.3f",
                             result.get("direction"), result.get("confluence_pct"),
                             result.get("entry", 0), result.get("sl", 0))
                    # Extend TP targets based on regime
                    for tp_key in ("tp1", "tp2", "tp3"):
                        if result.get(tp_key) and result.get("entry"):
                            dist = abs(result[tp_key] - result["entry"])
                            if result["direction"] == "BUY":
                                result[tp_key] = result["entry"] + dist * regime_tp_ext
                            else:
                                result[tp_key] = result["entry"] - dist * regime_tp_ext

                    await self._execute_signal(result, snap, regime_risk_mult)
                else:
                    log.debug("FUSION: below confluence gate (%.0f%%) — no trade", regime_conf_gate)
            else:
                if _loop_count % 60 == 0:
                    log.debug("FUSION SKIP: only %d/%d signals required", len(signals), 3)

            await asyncio.sleep(self.ANALYSIS_INTERVAL_S)

    async def _execute_signal(self, result: dict, snap: MarketSnapshot,
                               risk_mult: float):
        direction  = result["direction"]
        entry      = result["entry"]
        sl         = result["sl"]
        confluence = result["confluence_pct"]

        approved, reason = self.risk_ctrl.approve_trade(
            direction, entry, sl, confluence
        )
        if not approved:
            log.warning("TRADE BLOCKED by risk controller: %s | dir=%s conf=%.1f%%",
                        reason, direction, confluence)
            return

        tp1, tp2, tp3 = self.liq_mapper.optimize_tps(
            direction, entry,
            result["tp1"], result["tp2"], result["tp3"]
        )

        # Risk sizing with regime multiplier applied
        base_lot = self.risk_ctrl.calculate_lot_size(
            entry, sl, confluence, snap.atr_h1
        )
        lot_size = round(min(base_lot * risk_mult, 5.0), 2)

        cmd = {
            "cmd_type":      "OPEN",
            "type":          "OPEN",
            "direction":     direction,
            "entry_price":   entry,
            "sl_price":      sl,
            "tp1_price":     tp1,
            "tp2_price":     tp2,
            "tp3_price":     tp3,
            "lot_size":      lot_size,
            "strategy_tags": "+".join(result.get("strategy_tags", [])[:4]),
            "confidence":    confluence / 100.0,
            "ticket":        0
        }
        # Send to MT5 via TCP if connected, otherwise publish to bus
        sent = await self.send_to_mt5(cmd)
        if not sent:
            await self.bus.publish(CH_TRADE_CMD, cmd)
            log.warning("MT5 not connected — command queued on bus")
        self._trade_count += 1
        log.info("EVOLVED TRADE SUBMITTED: %s %.2f lots conf=%.1f%% regime=%s",
                 direction, lot_size, confluence,
                 self.adapt_engine._latest_params.get("regime","?"))

    async def _manage_positions(self, snap):
        for ticket in list(self.lifecycle.open_positions):
            await self.lifecycle.on_price_update(
                ticket, snap.bid, snap.h1_ohlc
            )

    def on_trade_closed_evolved(self, strategy: str, outcome_r: float,
                                  session: str, hour_utc: int):
        """Extended close handler — feeds regime memory in addition to PMA."""
        self.adapt_engine.on_trade_closed(strategy, outcome_r, session, hour_utc)
        log.info("Regime memory updated from closed trade: %s %.2fR in %s",
                 strategy, outcome_r, session)

    # ── TCP IPC Server (built-in — no separate process needed) ──────────────
    _tcp_writer = None   # active MT5 connection

    async def _tcp_server_loop(self, host: str = "127.0.0.1", port: int = 9876):
        """Embedded TCP server — receives ticks from MT5, sends orders back."""
        import asyncio as _aio

        async def _handle_mt5(reader, writer):
            addr = writer.get_extra_info("peername")
            log.info("MT5 CONNECTED from %s:%s", *addr)
            print(f"\n  ✅  MT5 connected from {addr[0]}:{addr[1]}")
            print(f"  ✅  Live tick feed active — swarm is trading\n")
            self._tcp_writer = writer
            buf = ""
            try:
                # Send handshake
                hs = json.dumps({"type": "HANDSHAKE", "version": "3.0",
                                  "timestamp": int(time.time())}) + "\n"
                writer.write(hs.encode())
                await writer.drain()

                while True:
                    data = await reader.read(4096)
                    if not data:
                        break
                    buf += data.decode(errors="replace")
                    while "\n" in buf:
                        line, buf = buf.split("\n", 1)
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            msg = json.loads(line)
                        except Exception:
                            continue
                        await self._handle_mt5_message(msg)
            except (asyncio.CancelledError, ConnectionResetError,
                    BrokenPipeError, OSError):
                pass
            finally:
                self._tcp_writer = None
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                log.info("MT5 disconnected from %s:%s", *addr)
                print("\n  ❌  MT5 disconnected — reconnecting automatically\n")

        server = await _aio.start_server(
            _handle_mt5,
            host,
            port,
            reuse_address=False,
            reuse_port=False,
        )
        print(f"\n  IPC TCP server listening on {host}:{port}")
        print(f"  Waiting for MT5 EA connection...\n")
        async with server:
            await server.serve_forever()

    async def _handle_mt5_message(self, msg: dict):
        """Route an incoming MT5 message to the correct subsystem."""
        mtype = msg.get("type", "")

        if mtype == "TICK":
            # Store bid/ask directly for dashboard
            _bid = msg.get("bid", 0)
            _ask = msg.get("ask", 0)
            if _bid > 0:
                self._latest_bid = _bid
                self._latest_ask = _ask
            # Feed real tick into the swarm's TickStreamerAgent
            await self.tick_stream.on_tick({
                "bid":       msg.get("bid", 0),
                "ask":       msg.get("ask", 0),
                "spread":    msg.get("spread", 3),
                "volume":    msg.get("volume", 100),
                "timestamp": msg.get("timestamp", time.time()),
            })
            # Keep the dashboard aligned with the latest tick feed
            self._write_state_file(
                self.risk_ctrl.equity,
                self.risk_ctrl.balance,
                len(self.lifecycle.open_positions),
                bid=msg.get("bid"),
                ask=msg.get("ask"),
                atr=msg.get("atr")
            )

        elif mtype == "HEARTBEAT":
            # Update risk controller with real account data
            eq  = msg.get("equity", 0)
            bal = msg.get("balance", 0)
            if eq > 0:
                self.risk_ctrl.equity  = eq
                self.risk_ctrl.balance = bal if bal > 0 else eq
                self.risk_ctrl.update_equity(eq)
            # Write shared state file for dashboard to read
            self._daily_pnl = msg.get("daily_pnl", 0)
            self._write_state_file(eq, bal, msg.get("positions", 0))

        elif mtype == "EXEC_ACK":
            # Register the opened position in lifecycle manager
            from risk_and_lifecycle import ManagedPosition, TradeState
            ticket    = msg.get("ticket", 0)
            direction = msg.get("direction", "BUY")
            entry     = msg.get("fill_price", 0)
            sl        = msg.get("sl", 0)
            tp        = msg.get("tp", 0)
            lot       = msg.get("lot_size", 0.01)
            if ticket and entry:
                pos = ManagedPosition(
                    ticket=ticket, direction=direction,
                    entry_price=entry, current_sl=sl,
                    tp1=tp, tp2=tp, tp3=tp,
                    lot_size=lot, open_time=time.time(),
                    state=TradeState.OPEN
                )
                self.lifecycle.register_open(pos)
                self._trade_count += 1
                log.info("Position registered: #%d %s @ %.2f", ticket, direction, entry)

        elif mtype == "TRADE_CLOSED":
            # Trigger PMA + regime memory
            ticket  = msg.get("ticket", 0)
            pnl     = msg.get("pnl_usd", 0)
            reason  = msg.get("reason", "CLOSED")
            if ticket:
                try:
                    self.lifecycle.unregister(ticket)
                except Exception:
                    pass
            # Feed into continuous learning
            session = "LONDON"
            import datetime  # noqa
            h = datetime.datetime.now(datetime.timezone.utc).hour
            if   7 <= h < 12: session = "LONDON"
            elif 13 <= h < 20: session = "NY"
            else:               session = "ASIA"
            try:
                self.on_trade_closed_evolved(
                    "LIVE_TRADE", pnl / max(abs(pnl), 0.01), session, h
                )
            except Exception as _ce:
                log.warning("TRADE_CLOSED callback non-fatal error (connection preserved): %s", _ce)
            # Always update counters regardless of callback errors
            if pnl > 0: self._wins += 1
            else:        self._losses += 1
            log.info("Trade closed #%d P&L=$%.2f [%s]", ticket, pnl, reason)

        elif mtype == "EMERGENCY_HALT":
            reason = msg.get("reason", "unknown")
            log.warning("EMERGENCY HALT from MT5: %s", reason)
            # Track halt state so analysis loop can report it
            self._ea_halted     = True
            self._ea_halt_reason = reason
            self.risk_ctrl._daily_halted = True

        elif mtype == "BAR":
            # Historical bar from EA backfill on connect.
            # The EA sends M1/M5/M15/H1/H4/D1 bars immediately after HANDSHAKE
            # via SendHistorySnapshot(). Without this handler they were silently
            # dropped, forcing a 2-3 hour cold-start warmup every restart.
            # load_bar() handles dedup + chronological sort automatically.
            tf = msg.get("tf", "H1").upper()
            bar = {
                "o": float(msg.get("o", 0) or 0),
                "h": float(msg.get("h", 0) or 0),
                "l": float(msg.get("l", 0) or 0),
                "c": float(msg.get("c", 0) or 0),
                "v": float(msg.get("v", 0) or 0),
                "t": float(msg.get("timestamp", 0) or 0),
            }
            if bar["c"] > 0 and bar["t"] > 0:
                self.tick_stream.cache.load_bar(tf, bar)
                # Log summary every 50 bars so we can see backfill progress
                h1_count = len(self.tick_stream.cache.get_bars("H1"))
                h4_count = len(self.tick_stream.cache.get_bars("H4"))
                if (h1_count + h4_count) % 50 == 0 and (h1_count + h4_count) > 0:
                    log.info("BAR BACKFILL | H1=%d H4=%d bars loaded from MT5",
                             h1_count, h4_count)

    async def send_to_mt5(self, cmd: dict):
        """Send a trade command back to MT5."""
        if self._tcp_writer and not self._tcp_writer.is_closing():
            try:
                # CRITICAL: use compact separators — MQL5's JsonGetDouble parser
                # cannot handle spaces after colons ("key": val). Default
                # json.dumps produces spaces which make every numeric field
                # return 0.0, causing lot=0/sl=0 → INVALID_ORDER_PARAMS rejection.
                data = json.dumps(cmd, separators=(',', ':')) + "\n"
                self._tcp_writer.write(data.encode())
                await self._tcp_writer.drain()
                return True
            except Exception as e:
                log.warning("send_to_mt5 failed: %s", e)
        return False

    def _write_state_file(
        self,
        equity: float,
        balance: float,
        positions: int,
        bid: Optional[float] = None,
        ask: Optional[float] = None,
        atr: Optional[float] = None,
    ):
        """Write live state to a shared JSON file for the dashboard."""
        import datetime
        now = datetime.datetime.now(datetime.timezone.utc)
        h = now.hour
        m = now.minute
        if   7 <= h < 12:                      session = "LONDON"
        elif h == 12 or (h == 13 and m < 30):  session = "LONDON"
        elif (h == 13 and m >= 30) or (14 <= h < 20): session = "NY"
        elif 0 <= h < 7:                       session = "ASIA"
        else:                                   session = "OFF"

        snap = self.tick_stream.get_latest_snapshot()
        if bid is None and snap is not None:
            bid = snap.bid
        if ask is None:
            if snap is not None and getattr(snap, "ask", None) is not None:
                ask = snap.ask
            elif bid is not None:
                ask = round(bid + 0.03, 2)
            else:
                ask = 0
        if atr is None and snap is not None:
            atr = snap.atr_h1

        regime_params = self.adapt_engine.get_live_params()
        state = {
            "ea_connected":   True,
            "equity":         equity,
            "balance":        balance,
            "positions":      positions,
            "daily_pnl":      self._daily_pnl,
            "session":        session,
            "regime":         regime_params.get("regime", "unknown"),
            "confluence_gate": regime_params.get("confluence_threshold", 75),
            "wins":           self._wins,
            "losses":         self._losses,
            "trade_count":    self._trade_count,
            "bid":            bid if bid is not None else 0,
            "ask":            ask if ask is not None else 0,
            "atr":            atr if atr is not None else 0,
            "timestamp":      time.time(),
        }
        state_path = pathlib.Path(__file__).parent / "journal_db_sim" / "live_state.json"
        try:
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(json.dumps(state))
        except Exception:
            pass

    async def run(self):
        self._running = True
        log.info("QUANT-SWARM-XAU v3.0 starting all coroutines...")
        host = os.environ.get("QSX_IPC_HOST", "127.0.0.1")
        port = int(os.environ.get("QSX_IPC_PORT", "9876"))

        async def _daily_reset_loop():
            """Reset Python-side daily halt and equity tracking at midnight UTC."""
            import datetime as _dt
            while self._running:
                now = _dt.datetime.now(_dt.timezone.utc)
                seconds_until_midnight = (
                    (23 - now.hour) * 3600 +
                    (59 - now.minute) * 60 +
                    (60 - now.second) + 60
                )
                await asyncio.sleep(seconds_until_midnight)
                self.risk_ctrl._daily_halted      = False
                self.risk_ctrl.day_start_equity   = self.risk_ctrl.equity
                self._wins         = 0
                self._losses       = 0
                self._trade_count  = 0
                self._daily_pnl    = 0.0
                log.info("Daily reset — halt cleared, equity baseline=$%.2f",
                         self.risk_ctrl.equity)

        await asyncio.gather(
            self._tcp_server_loop(host, port),
            self._analysis_loop(),
            self.reviewer.pma_dispatch_loop(),
            self.macro_mon.monitor_loop(),
            self.im_watch.monitor_loop(),
            self._weight_update_loop(),
            self._state_writer_loop(),
            self.adapt_engine.regime_loop(
                self.tick_stream.get_latest_snapshot
            ),
            self.adapt_engine.optimise_loop(),
            _daily_reset_loop(),
        )

    async def _weight_update_loop(self):
        while self._running:
            await asyncio.sleep(self.WEIGHT_UPDATE_S)
            self.fusion.recalculate_agent_performance()

    async def _state_writer_loop(self):
        """Write live_state.json every 5 seconds for dashboard to read."""
        import json as _json
        from pathlib import Path as _Path
        import datetime as _datetime
        state_path = _Path(__file__).parent / "journal_db_sim" / "live_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)
        while self._running:
            try:
                h = _datetime.datetime.now(_datetime.timezone.utc).hour
                m = _datetime.datetime.now(_datetime.timezone.utc).minute
                # Accurate session detection including 12:00-13:30 gap
                if   7 <= h < 12:                      session = "LONDON"
                elif h == 12 or (h == 13 and m < 30):  session = "LONDON"
                elif (h == 13 and m >= 30) or (14 <= h < 20): session = "NY"
                elif 0 <= h < 7:                       session = "ASIA"
                else:                                   session = "OFF"

                snap = self.tick_stream.get_latest_snapshot()
                regime_params = self.adapt_engine.get_live_params()

                state = {
                    "ea_connected":    self._tcp_writer is not None,
                    "equity":          self.risk_ctrl.equity,
                    "balance":         self.risk_ctrl.balance,
                    "positions":       len(self.lifecycle.open_positions),
                    "daily_pnl":       getattr(self, "_daily_pnl", 0),
                    "session":         session,
                    "wins":            self._wins,
                    "losses":          self._losses,
                    "trade_count":     self._trade_count,
                    "regime":          regime_params.get("regime", "unknown"),
                    "confluence_gate": regime_params.get("confluence_threshold", 75),
                    "bid":             self._latest_bid if self._latest_bid else (snap.bid if snap else 0),
                    "ask":             self._latest_ask if self._latest_ask else (snap.ask if snap else 0),
                    "atr":             snap.atr_h1 if snap else 0,
                    "timestamp":       _datetime.datetime.now(
                                         _datetime.timezone.utc).isoformat(),
                }
                state_path.write_text(_json.dumps(state))
            except Exception:
                pass
            await asyncio.sleep(1)

    def status(self) -> dict:
        regime_params = self.adapt_engine.get_live_params()
        return {
            "version":          "3.0-evolved",
            "uptime_s":         round(time.time() - self._start_time),
            "current_regime":   regime_params.get("regime", "unknown"),
            "regime_confidence":regime_params.get("regime_confidence", 0),
            "confluence_gate":  regime_params.get("confluence_threshold", 75),
            "effective_risk":   regime_params.get("effective_risk_pct", 1.0),
            "tp_extension":     regime_params.get("tp_extension", 1.0),
            "trades_today":     self._trade_count,
            "wins":             self._wins,
            "losses":           self._losses,
            "pma_queue":        len(self.reviewer._pending_pma),
            "open_positions":   len(self.lifecycle.open_positions),
            "risk":             self.risk_ctrl.get_status(),
        }


# ─── Demo ─────────────────────────────────────────────────────────────────────

async def demo_evolved():
    print("\n" + "═"*66)
    print("  QUANT-SWARM-XAU v3.0 EVOLVED — Full System Demo")
    print("═"*66)

    orch = EvolvedOrchestrator()

    # Simulate 100 ticks to build market snapshot
    print("\n  [1] Building market snapshot (100 synthetic ticks)...")
    await orch.tick_stream.simulate_ticks(2685.5, 100)
    snap = orch.tick_stream.get_latest_snapshot()
    print(f"       Bid={snap.bid:.2f}  ATR={snap.atr_h1:.2f}  Session={snap.session}")

    # Feed macro calendar
    print("\n  [2] Loading macro calendar into Agent 02...")
    orch.macro_mon.ingest_calendar([
        {"name": "NFP", "impact": "HIGH", "currency": "USD",
         "scheduled_utc": time.time() + 7200, "forecast": "180K", "previous": "175K"},
    ])
    await orch.macro_mon._evaluate_and_publish()
    macro = orch.bus.latest("macro.events") or {}
    print(f"       Next event: {macro.get('next_event_name')} | freeze={macro.get('freeze')}")

    # Simulate regime detection
    print("\n  [3] Regime detection on market data...")
    state = orch.adapt_engine.detector.detect(snap.h1_ohlc, snap.h4_ohlc)
    print(f"       Regime: {state.regime.value.upper()} ({state.confidence:.0%})")

    # Run forward optimiser
    print("\n  [4] Running ForwardOptimiser...")
    opt_params = orch.adapt_engine.optimiser.optimise(snap.h1_ohlc, snap.h4_ohlc)
    print(f"       Confluence gate:  {opt_params['confluence_threshold']:.0f}%")
    print(f"       Effective risk:   {opt_params['effective_risk_pct']:.2f}%")
    top = opt_params.get("top_strategies", [])[:3]
    print(f"       Top strategies:   {', '.join(s[0] for s in top)}")

    # Simulate a trade closing and feeding into regime memory
    print("\n  [5] Simulating trade close → regime memory update...")
    orch.on_trade_closed_evolved("ICT_SMC", +1.85, "LONDON", 9)
    orch.on_trade_closed_evolved("Mean_Rev", -0.95, "LONDON", 10)
    mult_ict = orch.adapt_engine.memory.get_multiplier(
        state.regime.value, "ICT_SMC", "LONDON"
    )
    print(f"       ICT_SMC multiplier for {state.regime.value}: {mult_ict:.4f}")

    # Full system status
    print("\n  [6] System Status v3.0:")
    st = orch.status()
    for k, v in st.items():
        if not isinstance(v, dict):
            print(f"       {k:<28} {v}")

    print(f"\n{'═'*66}")
    print("  QUANT-SWARM-XAU v3.0 EVOLVED: ALL SYSTEMS OPERATIONAL ✓")
    print()
    print("  Regime-aware adaptation layer:")
    print("    ✓ MarketRegimeDetector       — 8 regime classifiers")
    print("    ✓ AdaptiveParameterRouter    — live confluence/risk/TP adjustment")
    print("    ✓ RegimeMemory               — per-(regime×strategy×session) learning")
    print("    ✓ ForwardOptimiser           — 4-hour pre-session calibration")
    print("    ✓ MarketCalendarIntelligence — day/month seasonality priors")
    print("    ✓ ContinuousAdaptationEngine — ties all systems together")
    print(f"{'═'*66}\n")

if __name__ == "__main__":
    import os
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
        datefmt="%H:%M:%S"
    )
    log = logging.getLogger("MAIN")

    # ── Check if running in demo/test mode ───────────────────────────────
    if "--demo" in __import__("sys").argv:
        asyncio.run(demo_evolved())
    else:
        raise SystemExit(
            "Legacy evolved_orchestrator live execution is disabled. "
            "Use the canonical v4 runtime after its promotion gates pass; "
            "this module remains available only with --demo."
        )
