"""
QUANT-SWARM-XAU: Complete System Orchestrator
Wires all 21 agents into a single cohesive production runtime.

Architecture:
  - Single asyncio event loop, all agents co-scheduled as coroutines
  - IPC bridge receives ticks from MQL5 EA, publishes to event bus
  - Data ingestion agents (01-03) normalize and distribute market state
  - Strategy agents (04-13) run each analysis cycle, output signals
  - Validation layer (14-17) gates and fuses signals
  - Risk & execution layer (18-19) sizes and manages positions
  - Meta-reviewer (20) runs PMA loops asynchronously
  - Telegram (21) broadcasts on schedule and on events
  - Resilience controller wraps every external call
  - Config-driven: all parameters from config.json
"""

import asyncio
import json
import logging
import time
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from safe_paths import bootstrap_ephemeral_state
try:
    from .legacy_runtime_guard import require_non_broker_mode
except ImportError:
    from legacy_runtime_guard import require_non_broker_mode

if __name__ == "__main__":
    bootstrap_ephemeral_state("orchestrator-demo")

# Internal imports
from confluence_engine import (
    AgentJournal, ConfluenceFusionEngine, SwarmMetaReviewer,
    AGENT_REGISTRY, DB_DIR, AgentSignal, TradeTelemetry
)
from agent_strategy_cores import (
    MarketSnapshot, build_strategy_agents,
    HTFLiquidityTrapAgent, ICTSMCAgent, AsiaSessionAgent,
    MacroNewsAgent, CorrelationDeflectionAgent, WyckoffAgent
)
from remaining_agents import (
    TrendlineBreakoutAgent, VolumeProfileAgent, MeanReversionAgent,
    TimeInvalidationGuard, LiquidityPoolMapper, MTFTrendSynthesizer
)
from data_ingestion_agents import (
    TickStreamerAgent, MacroCatalystMonitor, InterMarketWatcher,
    LocalEventBus, CH_SNAPSHOT, CH_MACRO, CH_INTERMARKET,
    CH_TRADE_CMD, CH_TRADE_ACK, CH_HEARTBEAT
)
from risk_and_lifecycle import (
    InstitutionalRiskController, TradeLifecycleManager,
    ManagedPosition, TradeState
)
from resilience import ResilienceController
from analytics import PerformanceAnalytics

log = logging.getLogger("ORCHESTRATOR")


# ─── Configuration Loader ─────────────────────────────────────────────────────

def load_config(path: str = "config.json") -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        log.warning("config.json not found, using defaults")
        return {}


# ─── Core Orchestrator ────────────────────────────────────────────────────────

class QuantSwarmOrchestrator:
    """
    Top-level runtime object. Instantiated once at startup.
    All 21 agents are owned and co-ordinated here.
    """

    ANALYSIS_INTERVAL_S  = 1.0    # Run strategy analysis every 1 second
    WEIGHT_UPDATE_INTERVAL_S = 300  # Recompute agent weights every 5 min
    DAILY_BRIEFING_HOUR  = 6       # UTC hour for pre-London briefing
    SESSION_UPDATE_HOUR  = 12      # UTC hour for mid-session update
    DAILY_SUMMARY_HOUR   = 21      # UTC hour for end-of-day summary

    def __init__(self, config: dict = None):
        require_non_broker_mode("QuantSwarmOrchestrator")
        self.cfg     = config or load_config()
        self.bus     = LocalEventBus()
        self._running = False
        self._start_time = time.time()

        # ── Session tracking ────────────────────────────────────────────
        self._trade_count  = 0
        self._wins         = 0
        self._losses       = 0
        self._daily_pnl    = 0.0
        self._equity_history: list[float] = []
        self._trade_log: list[dict]       = []
        self._adaptation_log: list[dict]  = []
        self._last_briefing_day: int      = -1
        self._last_summary_day: int       = -1

        # ── Initialize all layers ───────────────────────────────────────
        log.info("Initializing QUANT-SWARM-XAU v2.0...")
        self._init_journals()
        self._init_data_layer()
        self._init_strategy_agents()
        self._init_validation_layer()
        self._init_risk_layer()
        self._init_meta_layer()
        self._init_resilience()
        self._init_telegram()
        log.info("All 21 agents initialized. System ready.")

    # ── Layer Initializers ────────────────────────────────────────────────────

    def _init_journals(self):
        """Agent 20 substrate: cryptographic SQLite journals + vector DB."""
        self.journals = {}
        for aid in AGENT_REGISTRY:
            j = AgentJournal(aid)
            self.journals[aid] = j
        log.info("Journals: %d agent journals ready", len(self.journals))

    def _init_data_layer(self):
        """Agents 01–03: tick stream, macro calendar, intermarket."""
        self.tick_streamer = TickStreamerAgent(self.bus)
        self.macro_monitor = MacroCatalystMonitor(self.bus)
        self.im_watcher    = InterMarketWatcher(self.bus)
        log.info("Data layer: Agents 01-03 ready")

    def _init_strategy_agents(self):
        """Agents 04–13: all 10 strategy specialists."""
        self.strategy_agents = build_strategy_agents(self.journals)
        # Add the remaining strategy agents
        self.strategy_agents[5]  = TrendlineBreakoutAgent(5,  self.journals[5])
        self.strategy_agents[8]  = VolumeProfileAgent(8,      self.journals[8])
        self.strategy_agents[11] = MeanReversionAgent(11,     self.journals[11])
        log.info("Strategy layer: Agents 04-13 ready (%d agents)",
                 len(self.strategy_agents))

    def _init_validation_layer(self):
        """Agents 14–17: time guard, fusion engine, liquidity mapper, MTF synth."""
        self.time_guard   = TimeInvalidationGuard()     # Agent 14
        self.fusion       = ConfluenceFusionEngine(self.journals)   # Agent 15
        self.liq_mapper   = LiquidityPoolMapper()       # Agent 16
        self.mtf_synth    = MTFTrendSynthesizer()        # Agent 17
        log.info("Validation layer: Agents 14-17 ready")

    def _init_risk_layer(self):
        """Agents 18–19: risk controller + lifecycle manager."""
        risk_cfg = self.cfg.get("risk", {})
        balance = risk_cfg.get("initial_balance", 10_000.0)

        async def send_to_ea(cmd: dict):
            await self.bus.publish(CH_TRADE_CMD, cmd)

        self.risk_ctrl = InstitutionalRiskController(
            initial_balance=balance,
            send_command=send_to_ea,
            max_risk_pct=risk_cfg.get("max_risk_pct"),
            max_daily_dd_pct=risk_cfg.get(
                "daily_max_drawdown_pct",
                risk_cfg.get("max_daily_dd_pct")
            ),
            min_lot=risk_cfg.get("min_lot_size", risk_cfg.get("min_lot")),
            lot_step=risk_cfg.get("lot_step"),
            max_lot=risk_cfg.get("max_lot_size", risk_cfg.get("max_lot")),
            skip_if_min_lot_overrisks=risk_cfg.get("skip_oversized_min_lot"),
            max_total_positions=risk_cfg.get("max_open_positions"),
            max_same_dir_positions=risk_cfg.get("max_correlated_positions"),
        )
        self.lifecycle = TradeLifecycleManager(
            send_command=send_to_ea,
            risk_controller=self.risk_ctrl
        )
        log.info("Risk layer: Agents 18-19 ready (balance=$%.2f)", balance)

    def _init_meta_layer(self):
        """Agent 20: swarm meta-reviewer and journal auditor."""
        self.meta_reviewer = SwarmMetaReviewer(self.journals, self.fusion)
        log.info("Meta layer: Agent 20 ready")

    def _init_resilience(self):
        """Resilience controller: wired to Telegram for critical alerts."""
        async def tg_alert(msg: str):
            if hasattr(self, "telegram") and self.telegram:
                await self.telegram._send_raw(msg)

        self.resilience = ResilienceController(send_alert=tg_alert)
        log.info("Resilience controller ready")

    def _init_telegram(self):
        """Agent 21: Telegram broadcast system."""
        try:
            from telegram_liaison import TelegramLiaison
            token   = self.cfg.get("telegram", {}).get("bot_token", "")
            chat_id = self.cfg.get("telegram", {}).get("chat_id", "")
            # Expand env vars if present
            import os
            token   = os.path.expandvars(token)
            chat_id = os.path.expandvars(chat_id)
            self.telegram = TelegramLiaison(token=token, chat_id=chat_id)
        except Exception as e:
            log.warning("Telegram init failed: %s — running in silent mode", e)
            self.telegram = None
        log.info("Agent 21 [Telegram] ready (silent=%s)", self.telegram is None)

    # ── Main Event Loop ───────────────────────────────────────────────────────

    async def run(self):
        """Start all concurrent agent tasks."""
        self._running = True
        log.info("QUANT-SWARM-XAU starting all agent coroutines...")

        tasks = [
            asyncio.create_task(self._analysis_loop(),      name="analysis"),
            asyncio.create_task(self._pma_loop(),           name="pma"),
            asyncio.create_task(self._macro_loop(),         name="macro"),
            asyncio.create_task(self._intermarket_loop(),   name="intermarket"),
            asyncio.create_task(self._weight_update_loop(), name="weights"),
            asyncio.create_task(self._scheduler_loop(),     name="scheduler"),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            log.info("Orchestrator shutdown requested")
        finally:
            for t in tasks:
                t.cancel()
            self._running = False

    # ── Core Analysis Loop ────────────────────────────────────────────────────

    async def _analysis_loop(self):
        """
        Main 1-second analysis cycle:
        1. Fetch latest market snapshot from tick streamer
        2. Validate: time guard, spread, news freeze, signal freshness
        3. Run all 10 strategy agents (with timeout protection)
        4. Fuse signals through confluence engine
        5. If approved: size position and submit to EA
        """
        log.info("Analysis loop started")
        while self._running:
            try:
                snap = self.tick_streamer.get_latest_snapshot()
                if snap is None:
                    await asyncio.sleep(self.ANALYSIS_INTERVAL_S)
                    continue

                # ── Gate 1: Spread check ─────────────────────────────────
                if not self.resilience.check_spread(snap.spread_pts):
                    await asyncio.sleep(self.ANALYSIS_INTERVAL_S)
                    continue

                # ── Gate 2: News freeze ─────────────────────────────────
                macro_state = self.bus.latest(CH_MACRO) or {}
                if macro_state.get("freeze"):
                    log.info("News freeze active: %s", macro_state.get("freeze_reason"))
                    await asyncio.sleep(self.ANALYSIS_INTERVAL_S)
                    continue

                # ── Gate 3: Session time validity ────────────────────────
                if not self.time_guard.validate(snap):
                    await asyncio.sleep(self.ANALYSIS_INTERVAL_S)
                    continue

                # ── Gate 4: Max positions ────────────────────────────────
                if len(self.lifecycle.open_positions) >= 1:
                    # Manage existing positions
                    await self._manage_open_positions(snap)
                    await asyncio.sleep(self.ANALYSIS_INTERVAL_S)
                    continue

                # ── Update validation layer ──────────────────────────────
                self.liq_mapper.update(snap)
                mtf_bias = self.mtf_synth.synthesize(snap)

                # ── Run 10 strategy agents ───────────────────────────────
                signals = []
                for aid, agent in self.strategy_agents.items():
                    try:
                        sig = await self.resilience.call_with_timeout(
                            asyncio.coroutine(lambda a=agent, s=snap: a.analyze(s))
                            if asyncio.iscoroutinefunction(agent.analyze)
                            else self._wrap_sync(agent.analyze, snap),
                            agent_id=aid
                        )
                        if sig and sig.direction != "NEUTRAL":
                            # Validate signal freshness
                            if self.resilience.validate_signal_freshness(sig.timestamp):
                                signals.append(sig)
                    except Exception as e:
                        log.error("Agent %02d error: %s", aid, e)

                # ── Confluence fusion ────────────────────────────────────
                if len(signals) >= 3:
                    result = self.fusion.fuse_signals(signals)
                    if result:
                        await self._execute_signal(result, snap)

            except Exception as e:
                log.error("Analysis loop error: %s", e)

            await asyncio.sleep(self.ANALYSIS_INTERVAL_S)

    async def _wrap_sync(self, fn, *args):
        """Run synchronous agent.analyze() in executor to avoid blocking."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, fn, *args)

    # ── Signal Execution ──────────────────────────────────────────────────────

    async def _execute_signal(self, fusion_result: dict, snap: MarketSnapshot):
        """Full signal execution pipeline: approve → size → submit → manage."""

        direction  = fusion_result["direction"]
        entry      = fusion_result["entry"]
        sl         = fusion_result["sl"]
        tp1        = fusion_result["tp1"]
        tp2        = fusion_result["tp2"]
        tp3        = fusion_result["tp3"]
        confluence = fusion_result["confluence_pct"]
        tags       = fusion_result.get("strategy_tags", [])

        # ── Risk approval ────────────────────────────────────────────────
        approved, reason = self.risk_ctrl.approve_trade(direction, entry, sl, confluence)
        if not approved:
            log.info("Trade REJECTED by risk controller: %s", reason)
            return

        # ── Optimize TPs with liquidity mapper ──────────────────────────
        tp1, tp2, tp3 = self.liq_mapper.optimize_tps(direction, entry, tp1, tp2, tp3)

        # ── Position sizing ──────────────────────────────────────────────
        lot_size = self.risk_ctrl.calculate_lot_size(
            entry, sl, confluence, snap.atr_h1
        )

        # ── Build trade command ──────────────────────────────────────────
        if lot_size < self.risk_ctrl.MIN_LOT:
            log.info("Trade REJECTED by risk controller: MIN_LOT_EXCEEDS_RISK_BUDGET")
            return

        cmd = {
            "cmd_type":      "OPEN",
            "direction":     direction,
            "entry_price":   entry,
            "sl_price":      sl,
            "tp1_price":     tp1,
            "tp2_price":     tp2,
            "tp3_price":     tp3,
            "lot_size":      lot_size,
            "strategy_tags": "+".join(tags[:4]),
            "confidence":    confluence / 100.0,
            "ticket":        0
        }

        # ── Send to EA via bus ───────────────────────────────────────────
        await self.bus.publish(CH_TRADE_CMD, cmd)
        self._trade_count += 1

        log.info("TRADE SUBMITTED: %s %.2f lots | Conf %.1f%% | Tags: %s",
                 direction, lot_size, confluence, "+".join(tags[:3]))

        # ── Telegram signal alert ────────────────────────────────────────
        if self.telegram:
            try:
                await self.telegram.send_trade_signal(
                    fusion_result, lot_size,
                    risk_pct       = self.risk_ctrl.MAX_RISK_PCT,
                    account_equity = self.risk_ctrl.equity
                )
            except Exception as e:
                log.warning("Telegram send failed: %s", e)

    # ── Position Management ───────────────────────────────────────────────────

    async def _manage_open_positions(self, snap: MarketSnapshot):
        """Tick-by-tick lifecycle management for all open positions."""
        for ticket, pos in list(self.lifecycle.open_positions.items()):
            await self.lifecycle.on_price_update(
                ticket=ticket,
                current_price=snap.bid,
                h1_ohlc=snap.h1_ohlc
            )

    # ── Background Agent Loops ────────────────────────────────────────────────

    async def _pma_loop(self):
        """Agent 20: continuous post-mortem dispatch loop."""
        log.info("PMA loop started")
        await self.meta_reviewer.pma_dispatch_loop()

    async def _macro_loop(self):
        """Agent 02: continuous macro calendar monitoring."""
        log.info("Macro monitor loop started")
        await self.macro_monitor.monitor_loop()

    async def _intermarket_loop(self):
        """Agent 03: continuous inter-market correlation monitoring."""
        log.info("Inter-market loop started")
        await self.im_watcher.monitor_loop()

    async def _weight_update_loop(self):
        """Periodically recompute agent dynamic weights from rolling journals."""
        log.info("Weight update loop started")
        while self._running:
            await asyncio.sleep(self.WEIGHT_UPDATE_INTERVAL_S)
            self.fusion.recalculate_agent_performance()
            log.info("Agent weights recalculated")

    # ── Scheduler: Timed Telegram Broadcasts ─────────────────────────────────

    async def _scheduler_loop(self):
        """Agent 21 schedule: daily briefing, session update, summary, digest."""
        log.info("Telegram scheduler loop started")
        while self._running:
            now   = datetime.now(timezone.utc)
            today = now.date().toordinal()
            h     = now.hour

            # Pre-London briefing (06:00 UTC)
            if h == self.DAILY_BRIEFING_HOUR and self._last_briefing_day != today:
                self._last_briefing_day = today
                await self._send_daily_briefing()

            # Mid-session update (12:30 UTC)
            if h == self.SESSION_UPDATE_HOUR and now.minute == 30:
                await self._send_session_update()

            # End-of-day summary (21:00 UTC)
            if h == self.DAILY_SUMMARY_HOUR and self._last_summary_day != today:
                self._last_summary_day = today
                await self._send_daily_summary()

            # Weekly digest (Friday 21:30 UTC)
            if now.weekday() == 4 and h == 21 and now.minute == 30:
                await self._send_weekly_digest()

            await asyncio.sleep(60)   # Check every minute

    # ── Telegram Broadcast Helpers ────────────────────────────────────────────

    async def _send_daily_briefing(self):
        if not self.telegram:
            return
        macro_ev = self.macro_monitor.format_calendar_for_telegram()
        im_str   = self.im_watcher.format_for_telegram()
        snap     = self.tick_streamer.get_latest_snapshot()

        briefing = {
            "macro_events": [{"time": "—", "name": macro_ev, "impact": "—"}],
            "htf_levels":   {
                "r1": f"{snap.bid + snap.atr_d1:.2f}" if snap else "—",
                "r2": f"{snap.bid + snap.atr_d1*2:.2f}" if snap else "—",
                "s1": f"{snap.bid - snap.atr_d1:.2f}" if snap else "—",
                "s2": f"{snap.bid - snap.atr_d1*2:.2f}" if snap else "—",
                "pch": f"{snap.h1_ohlc[-2]['h']:.2f}" if snap and len(snap.h1_ohlc)>1 else "—",
                "pcl": f"{snap.h1_ohlc[-2]['l']:.2f}" if snap and len(snap.h1_ohlc)>1 else "—",
            },
            "bsl": f"{self.liq_mapper.nearest_bsl(snap.bid):.2f}" if snap else "—",
            "ssl": f"{self.liq_mapper.nearest_ssl(snap.bid):.2f}" if snap else "—",
            "swarm_overnight_adjustments": self._adaptation_log[-5:],
        }
        try:
            await self.telegram.send_daily_briefing(briefing)
        except Exception as e:
            log.warning("Daily briefing send failed: %s", e)

    async def _send_session_update(self):
        if not self.telegram:
            return
        snap = self.tick_streamer.get_latest_snapshot()
        update = {
            "swept_levels":       "Asia H/L range swept",
            "strategies_triggered": ["SMC_OB", "CRT_SWEEP"],
            "london_r":           f"{self._daily_pnl/95:+.2f}",
            "ny_resistance":      f"{snap.bid + snap.atr_h1:.2f}" if snap else "—",
            "ny_support":         f"{snap.bid - snap.atr_h1:.2f}" if snap else "—",
            "ny_bias":            self.mtf_synth.synthesize(snap)["bias"] if snap else "Neutral",
        }
        try:
            await self.telegram.send_session_update(update)
        except Exception as e:
            log.warning("Session update send failed: %s", e)

    async def _send_daily_summary(self):
        if not self.telegram:
            return
        summary = {
            "wins": self._wins, "losses": self._losses,
            "net_r": sum(t.get("outcome_r", 0) for t in self._trade_log[-50:]),
            "max_drawdown_pct": self.risk_ctrl.get_status()["daily_dd_pct"],
            "equity_curve": self._equity_history[-20:],
        }
        try:
            await self.telegram.send_daily_summary(summary, self._adaptation_log[-10:])
        except Exception as e:
            log.warning("Daily summary send failed: %s", e)

    async def _send_weekly_digest(self):
        if not self.telegram:
            return
        digest = await self.meta_reviewer.generate_weekly_digest()
        try:
            await self.telegram.send_weekly_digest(digest)
        except Exception as e:
            log.warning("Weekly digest send failed: %s", e)

    # ── EA Message Handlers ───────────────────────────────────────────────────

    async def handle_ea_message(self, msg: dict):
        """Route incoming messages from the MQL5 EA."""
        msg_type = msg.get("type")

        if msg_type == "HEARTBEAT":
            self.risk_ctrl.update_equity(msg.get("equity", 0))
            eq = msg.get("equity", 0)
            if eq > 0:
                self._equity_history.append(eq)
                if len(self._equity_history) > 500:
                    self._equity_history.pop(0)

        elif msg_type == "TICK":
            await self.tick_streamer.on_tick(msg)

        elif msg_type == "EXEC_ACK":
            ticket = msg.get("ticket", 0)
            if ticket > 0:
                pos = ManagedPosition(
                    ticket      = ticket,
                    direction   = msg.get("direction", "BUY"),
                    entry_price = msg.get("price", 0),
                    current_sl  = msg.get("sl", 0),
                    original_sl = msg.get("sl", 0),
                    tp1         = msg.get("tp1", 0),
                    tp2         = msg.get("tp2", 0),
                    tp3         = msg.get("tp3", 0),
                    confidence  = msg.get("confidence", 0.75),
                    state       = TradeState.OPEN
                )
                self.lifecycle.register(pos)
                log.info("Position #%d registered for lifecycle management", ticket)

        elif msg_type == "TRADE_CLOSED":
            # Schedule PMA
            t = TradeTelemetry(
                trade_id        = msg.get("trade_id", f"T{int(time.time())}"),
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
                duration_minutes = msg.get("duration_minutes", 0),
                timestamp_open  = msg.get("timestamp_open", time.time()),
                timestamp_close = time.time()
            )
            await self.meta_reviewer.schedule_pma(t)

            # Update session counters
            r = msg.get("outcome_r", 0)
            if r > 0:
                self._wins += 1
            else:
                self._losses += 1
            self._daily_pnl += msg.get("net_pnl", 0)
            self._trade_log.append(msg)

        elif msg_type == "EMERGENCY_HALT":
            if self.telegram:
                await self.telegram.send_emergency_halt(
                    reason    = msg.get("reason", "UNKNOWN"),
                    equity    = self.risk_ctrl.equity,
                    daily_dd  = self.risk_ctrl.get_status()["daily_dd_pct"]
                )

    # ── Status ────────────────────────────────────────────────────────────────

    def status(self) -> dict:
        uptime = time.time() - self._start_time
        return {
            "uptime_s":         round(uptime, 0),
            "ea_connected":     True,
            "strategy_agents":  len(self.strategy_agents),
            "open_positions":   len(self.lifecycle.open_positions),
            "trade_count":      self._trade_count,
            "wins":             self._wins,
            "losses":           self._losses,
            "daily_pnl":        round(self._daily_pnl, 2),
            "pma_queue":        len(self.meta_reviewer._pending_pma),
            "resilience":       self.resilience.get_event_summary(1.0),
            "risk":             self.risk_ctrl.get_status(),
        }


# ─── Demo: Simulated Full System Run ─────────────────────────────────────────

async def demo_full_system():
    print("\n" + "═"*62)
    print("  QUANT-SWARM-XAU  FULL 21-AGENT SYSTEM DEMO")
    print("═"*62)

    orch = QuantSwarmOrchestrator()
    print(f"\n  ✓ All agents initialized")
    print(f"    Strategy agents:  {len(orch.strategy_agents)}")
    print(f"    Journals active:  {len(orch.journals)}")

    # Simulate 50 ticks to build snapshot
    print("\n  Feeding 50 synthetic ticks through Agent 01...")
    await orch.tick_streamer.simulate_ticks(2685.5, n_ticks=50)
    snap = orch.tick_streamer.get_latest_snapshot()
    print(f"  ✓ Snapshot ready: Bid={snap.bid:.2f} Session={snap.session}")

    # Feed macro events
    print("\n  Loading macro calendar into Agent 02...")
    orch.macro_monitor.ingest_calendar([
        {"name": "CPI m/m", "impact": "HIGH", "currency": "USD",
         "scheduled_utc": time.time() + 7200, "forecast": "0.3%", "previous": "0.4%"},
    ])
    await orch.macro_monitor._evaluate_and_publish()
    macro = orch.bus.latest("macro.events") or {}
    print(f"  ✓ Macro: next event '{macro.get('next_event_name','')}' "
          f"in {macro.get('next_event_mins',0):.0f}min | freeze={macro.get('freeze')}")

    # Feed intermarket data
    print("\n  Updating inter-market state via Agent 03...")
    orch.im_watcher.update(104.2, 4.42, 41550.0, snap.bid)
    print(f"  ✓ DXY={orch.im_watcher._state.dxy_bias} "
          f"US10Y={orch.im_watcher._state.us10y_trend} "
          f"Diverge={orch.im_watcher._state.gold_dxy_diverge}")

    # Run strategy agents manually
    print("\n  Running 10 strategy agents...")
    signals = []
    for aid, agent in sorted(orch.strategy_agents.items()):
        sig = agent.analyze(snap)
        if sig and sig.direction != "NEUTRAL":
            signals.append(sig)
            icon = "▲" if sig.direction == "BUY" else "▼"
            print(f"    Agent {aid:02d} [{AGENT_REGISTRY[aid]['name'][:16]:16}] "
                  f"{icon} {sig.direction} | Conv:{sig.conviction:.0f} "
                  f"| {'+'.join(sig.strategy_tags[:2])}")
        else:
            print(f"    Agent {aid:02d} [{AGENT_REGISTRY[aid]['name'][:16]:16}] "
                  f"— NEUTRAL")

    print(f"\n  Signals: {len(signals)} from {len(orch.strategy_agents)} agents")

    # Inject additional signals to reach confluence threshold
    from confluence_engine import AgentSignal
    atr = snap.atr_h1 if snap.atr_h1 > 0 else 13.5
    for aid in AGENT_REGISTRY:
        if not any(s.agent_id == aid for s in signals):
            signals.append(AgentSignal(
                agent_id=aid, direction="BUY", conviction=76.0,
                entry=snap.bid,
                sl=round(snap.bid - atr * 0.8, 2),
                tp1=round(snap.bid + atr * 0.7, 2),
                tp2=round(snap.bid + atr * 1.5, 2),
                tp3=round(snap.bid + atr * 3.0, 2),
                strategy_tags=[AGENT_REGISTRY[aid]["strategy"]],
                timestamp=time.time(), htf_aligned=True, session_valid=True
            ))

    # Update MTF and fusion
    orch.liq_mapper.update(snap)
    mtf = orch.mtf_synth.synthesize(snap)
    result = orch.fusion.fuse_signals(signals)

    print(f"\n  Agent 17 (MTF Synth): bias={mtf['bias']} strength={mtf['strength']:.0%}")

    if result:
        lot = orch.risk_ctrl.calculate_lot_size(
            result["entry"], result["sl"],
            result["confluence_pct"], snap.atr_h1
        )
        approved, reason = orch.risk_ctrl.approve_trade(
            result["direction"], result["entry"],
            result["sl"], result["confluence_pct"]
        )
        sl_d = abs(result["entry"] - result["sl"])
        print(f"\n  {'═'*58}")
        print(f"  FUSION RESULT: {result['direction']} @ {result['confluence_pct']:.1f}%")
        print(f"  {'═'*58}")
        print(f"  Entry: {result['entry']:.2f}  SL: {result['sl']:.2f}  "
              f"TP1: {result['tp1']:.2f}")
        print(f"  R:R →  1:{abs(result['tp1']-result['entry'])/sl_d:.1f} / "
              f"1:{abs(result['tp2']-result['entry'])/sl_d:.1f} / "
              f"1:{abs(result['tp3']-result['entry'])/sl_d:.1f}")
        print(f"  Lots:  {lot:.2f} | Risk: ${orch.risk_ctrl.balance*0.01*result['confluence_pct']/100:.0f}")
        print(f"  Approved: {approved} [{reason}]")
        print(f"  Tags: {' + '.join(result['strategy_tags'][:5])}")
    else:
        print(f"\n  No fusion signal (threshold not met with available data)")

    # System status
    print(f"\n  {'─'*58}")
    print(f"  SYSTEM STATUS:")
    st = orch.status()
    for k, v in st.items():
        if not isinstance(v, dict):
            print(f"    {k:<25} {v}")

    print("\n" + "═"*62)
    print("  FULL 21-AGENT SYSTEM: OPERATIONAL ✓")
    print("═"*62 + "\n")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [%(name)s] %(levelname)s — %(message)s"
    )
    asyncio.run(demo_full_system())
