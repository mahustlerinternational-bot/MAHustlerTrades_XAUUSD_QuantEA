"""
QUANT-SWARM-XAU v3.0 — Live Simulation Runner
Runs the complete 21-agent swarm against a real-time synthetic
XAUUSD feed, printing a live terminal dashboard every second.

This is the production-equivalent runtime without needing
MetaTrader 5 connected — the IPC bridge is replaced by a
SyntheticMarketFeed that generates realistic XAUUSD tick data
using GBM + mean-reversion + session vol scaling.

Usage:
    python3 live_sim.py              # Default 5-minute run
    python3 live_sim.py --hours 2    # Run for 2 hours
    python3 live_sim.py --ticks 5000 # Run for N ticks
    python3 live_sim.py --session london  # Specific session
"""

import asyncio
import argparse
import logging
import math
import pathlib
import random
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(pathlib.Path(__file__).parent))

from safe_paths import bootstrap_ephemeral_state, runtime_state_dir

if __name__ == "__main__":
    bootstrap_ephemeral_state("live-sim")

log = logging.getLogger("LIVE-SIM")

# ── Imports ───────────────────────────────────────────────────────────────────
from confluence_engine import (
    AgentJournal, ConfluenceFusionEngine, SwarmMetaReviewer,
    AGENT_REGISTRY, TradeTelemetry, AgentSignal
)
from agent_strategy_cores import (
    MarketSnapshot, build_strategy_agents
)
from remaining_agents import (
    TrendlineBreakoutAgent, VolumeProfileAgent, MeanReversionAgent,
    TimeInvalidationGuard, LiquidityPoolMapper, MTFTrendSynthesizer
)
from data_ingestion_agents import TickCache, Tick
from risk_and_lifecycle import (
    InstitutionalRiskController, TradeLifecycleManager,
    ManagedPosition, TradeState, ExitReason
)
from resilience import ResilienceController
from regime_detector import MarketRegimeDetector, AdaptiveParameterRouter
from continuous_learner import (
    ContinuousAdaptationEngine, RegimeMemory,
    MarketCalendarIntelligence
)
import confluence_engine as ce


# ─── ANSI colours ─────────────────────────────────────────────────────────────
class C:
    R="\033[0m"; B="\033[1m"; D="\033[2m"
    GOLD="\033[38;5;220m";  CHAMP="\033[38;5;229m"
    GREEN="\033[38;5;78m";  RED="\033[38;5;203m"
    CYAN="\033[38;5;80m";   PURPLE="\033[38;5;141m"
    GREY="\033[38;5;244m";  WHITE="\033[97m"
    AMBER="\033[38;5;214m"


# ─── Synthetic XAUUSD Market Feed ────────────────────────────────────────────

class SyntheticMarketFeed:
    """
    Generates realistic XAUUSD tick data using:
    - Geometric Brownian Motion base process
    - Session-scaled volatility (London/NY = high, Asia = low)
    - Mean-reversion component (prevents infinite drift)
    - Occasional spike events (mimics news/liquidity hunts)
    - Spread simulation (widens on low liquidity)
    """
    # Session vol multipliers by UTC hour
    SESSION_VOL = {
        0:0.30,1:0.28,2:0.28,3:0.32,4:0.38,  # Deep Asia
        5:0.55,6:0.75,7:1.10,                  # Pre-London
        8:1.45,9:1.55,10:1.30,11:1.10,        # London core
        12:0.80,13:1.20,                        # Handover
        14:1.55,15:1.50,16:1.35,              # NY open (peak)
        17:1.10,18:0.90,19:0.75,20:0.60,      # NY afternoon
        21:0.45,22:0.40,23:0.35,               # NY close
    }
    SPIKE_PROB    = 0.0008   # ~0.08% per tick = ~1 spike per 1250 ticks
    SPIKE_SIZE    = 3.5      # ATR multiples
    MEAN_REV_SPD  = 0.015    # How fast price reverts to long-term mean

    def __init__(self, start_price: float = 2685.0,
                 annual_vol: float = 0.15,
                 trend_drift: float = 0.04,
                 session: str = "auto"):
        self.price      = start_price
        self.long_mean  = start_price
        self.annual_vol = annual_vol
        self.drift      = trend_drift
        self.session_override = session
        self._tick_n    = 0
        self._last_spike= 0

    def next_tick(self) -> tuple[float, float, int, float]:
        """Returns (bid, ask, spread_pts, volume)"""
        self._tick_n += 1
        now = time.time()
        h   = int((now % 86400) / 3600)

        # Session vol
        vol_mult = self.SESSION_VOL.get(h, 0.5)
        if self.session_override == "london": vol_mult = 1.45
        if self.session_override == "ny":     vol_mult = 1.55
        if self.session_override == "asia":   vol_mult = 0.32

        # Base tick interval ~0.5s → dt
        dt = 0.5 / (252 * 6.5 * 3600)

        # GBM step
        sigma    = self.annual_vol * math.sqrt(dt) * vol_mult
        mu       = (self.drift - 0.5 * self.annual_vol**2) * dt
        shock    = random.gauss(0, 1)

        # Mean reversion
        mr = self.MEAN_REV_SPD * (self.long_mean - self.price) / self.price

        log_ret  = mu + mr + sigma * shock
        self.price = max(self.price * math.exp(log_ret), 1500.0)

        # Occasional spike
        if random.random() < self.SPIKE_PROB and self._tick_n - self._last_spike > 200:
            spike_dir = random.choice([-1, 1])
            atr_approx = self.price * self.annual_vol * math.sqrt(1/252)
            self.price += spike_dir * atr_approx * self.SPIKE_SIZE
            self._last_spike = self._tick_n

        # Spread: tight in liquid sessions, wide otherwise
        base_spread = 3 if vol_mult > 0.8 else 6
        spread_pts  = base_spread + random.randint(0, 2)
        spread_price = spread_pts * 0.01

        bid = round(self.price, 2)
        ask = round(self.price + spread_price, 2)
        vol = int(abs(random.gauss(300, 100) * vol_mult))

        return bid, ask, spread_pts, vol

    def session_name(self) -> str:
        if self.session_override != "auto":
            return self.session_override.upper()
        h = int((time.time() % 86400) / 3600)
        if  0 <= h <  7: return "ASIA"
        if  7 <= h < 12: return "LONDON"
        if 12 <= h < 20: return "NY"
        return "OFF"


# ─── Open Trade Tracker ───────────────────────────────────────────────────────

@dataclass
class SimTrade:
    direction:   str
    entry:       float
    sl:          float
    tp1:         float
    tp2:         float
    tp3:         float
    lot_size:    float
    open_time:   float
    tags:        str
    regime:      str
    confidence:  float
    state:       str  = "OPEN"   # OPEN / BE / TP1 / TP2 / CLOSED
    exit_price:  float = 0.0
    exit_reason: str   = ""
    pnl_usd:     float = 0.0
    peak_price:  float = 0.0
    current_sl:  float = 0.0

    def __post_init__(self):
        self.current_sl = self.sl
        self.peak_price = self.entry

    @property
    def current_r(self) -> float:
        if self.state == "CLOSED":
            dist = abs(self.exit_price - self.entry)
            sl_d = abs(self.sl - self.entry)
            r    = dist / sl_d if sl_d > 0 else 0
            return r if self.exit_price > self.entry and self.direction=="BUY" \
                     or self.exit_price < self.entry and self.direction=="SELL" \
                     else -r
        return 0.0


# ─── Live Simulation Engine ───────────────────────────────────────────────────

class LiveSimEngine:

    TICK_INTERVAL_S  = 0.08    # ~12 ticks/second
    DISPLAY_INTERVAL = 12      # Redraw every 12 ticks (~1s)
    HISTORY_WINDOW   = 200     # Tick bars to keep in cache
    MIN_BARS_NEEDED  = 10      # Bars before strategy analysis begins (pre-seeded)
    MAX_HOLD_BARS    = 96      # Force-close after 96 bars

    def __init__(self, start_price=2685.0, initial_balance=10000.0,
                 session="auto", max_seconds=300, risk_pct=None,
                 max_daily_dd_pct=None, min_lot=0.01,
                 skip_oversized_min_lot=True):
        self.feed     = SyntheticMarketFeed(start_price, session=session)
        self.initial_balance = initial_balance
        self.balance  = initial_balance
        self.equity   = initial_balance
        self.peak_eq  = initial_balance
        self.max_secs = max_seconds
        self.risk_pct = risk_pct
        self.max_daily_dd_pct = max_daily_dd_pct
        self.min_lot = min_lot
        self.skip_oversized_min_lot = skip_oversized_min_lot

        # ── Swarm infrastructure ─────────────────────────────────────────
        self._setup_swarm()

        # ── State tracking ───────────────────────────────────────────────
        self.open_trade:   SimTrade | None = None
        self.closed_trades: list[SimTrade] = []
        self._last_trade_close_time = 0.0    # time-based cooldown
        self._last_analysis_time    = 0.0    # time-based analysis gate
        self.tick_count   = 0
        self.bar_count    = 0
        self.signal_count = 0
        self.rejected_count = 0
        self.start_time   = time.time()
        self.last_signal_tags = "—"
        self.last_confluence  = 0.0
        self.last_direction   = "—"
        self.adaptation_events: list[str] = []

        # Price history for display
        self.price_history: deque = deque(maxlen=50)
        self.equity_history: deque = deque(maxlen=50)

        log.info("LiveSimEngine ready: $%.0f account | %ds runtime",
                 initial_balance, max_seconds)

    def _setup_swarm(self):
        """Initialise all 21 agents."""
        # Simulations use the configured state root. Direct CLI runs bootstrap
        # a marked temporary workspace before importing any journal modules.
        self._sim_dir = runtime_state_dir()
        self._journal_dir = self._sim_dir / "journals"
        self._journal_dir.mkdir(parents=True, exist_ok=True)

        # Journals
        self.journals = {}
        for aid in AGENT_REGISTRY:
            j = AgentJournal.__new__(AgentJournal)
            j.agent_id   = aid
            j.agent_name = AGENT_REGISTRY[aid]["name"]
            j.collection = None
            j.db_path    = self._journal_dir / f"sim_{aid}.db"
            j._init_db()
            self.journals[aid] = j

        # Strategy agents
        self.agents = build_strategy_agents(self.journals)
        self.agents[5]  = TrendlineBreakoutAgent(5,  self.journals[5])
        self.agents[8]  = VolumeProfileAgent(8,       self.journals[8])
        self.agents[11] = MeanReversionAgent(11,      self.journals[11])

        # Validation + risk layers
        self.fusion    = ConfluenceFusionEngine(self.journals)
        self.reviewer  = SwarmMetaReviewer(self.journals, self.fusion)
        self.liq_map   = LiquidityPoolMapper()
        self.mtf       = MTFTrendSynthesizer()
        self.guard     = TimeInvalidationGuard()
        self.risk_ctrl = InstitutionalRiskController(
            self.balance,
            max_risk_pct=self.risk_pct,
            max_daily_dd_pct=self.max_daily_dd_pct,
            min_lot=self.min_lot,
            skip_if_min_lot_overrisks=self.skip_oversized_min_lot,
        )
        self.resilience= ResilienceController()

        # v3.0 regime layer
        self.detector  = MarketRegimeDetector()
        self.router    = AdaptiveParameterRouter()
        self.adapt     = ContinuousAdaptationEngine()
        self.calendar  = MarketCalendarIntelligence()

        # Tick cache
        self.cache     = TickCache()


    def _preseed_history(self, n_bars: int = 72):
        """Inject N hours of synthetic history so analysis starts immediately."""
        import sys
        print(f'  Pre-seeding {n_bars}h of XAUUSD history... ', end='', flush=True)
        price = self.feed.price
        now   = time.time()
        for i in range(n_bars * 6):          # 6 ticks per bar (every 10 min)
            ts    = now - (n_bars * 6 - i) * 600   # spaced 10 min apart
            price = max(price + random.gauss(self.feed.drift * 0.1,
                                             self.feed.annual_vol * 0.8), 1800.0)
            sp = 3
            self.cache.add_tick(Tick(round(price,2), round(price+0.03,2), sp, 200, ts))
        self.feed.price      = price
        self.feed.long_mean  = price
        self.bar_count = len(self.cache.get_bars('H1'))
        print(f'done — {self.bar_count} H1 bars ready.')

    def _build_snapshot(self, bid: float, ask: float,
                         spread_pts: int) -> MarketSnapshot:
        poc, hvns, lvns = self._compute_levels()
        session = self.feed.session_name()
        return MarketSnapshot(
            symbol   = "XAUUSD",
            bid      = bid, ask=ask, spread_pts=spread_pts,
            h1_ohlc  = self.cache.get_bars("H1", 100),
            h4_ohlc  = self.cache.get_bars("H4", 50),
            d1_ohlc  = self.cache.get_bars("D1", 20),
            m15_ohlc = self.cache.get_bars("M15", 100),
            m5_ohlc  = self.cache.get_bars("M5", 100),
            m1_ohlc  = self.cache.get_bars("M1", 100),
            atr_h1   = self.cache.compute_atr("H1"),
            atr_h4   = self.cache.compute_atr("H4"),
            atr_d1   = self.cache.compute_atr("D1"),
            poc_level = poc, hvn_levels=hvns, lvn_levels=lvns,
            session  = session,
            london_open = session == "LONDON",
            ny_open     = session == "NY",
            news_impact    = 0.05,
            news_direction = "NEUTRAL",
            dxy_bias    = "NEUTRAL",
            us10y_trend = "FLAT",
            us30_bias   = "NEUTRAL",
            timestamp   = time.time()
        )

    def _compute_levels(self):
        h1 = self.cache.get_bars("H1", 30)
        if not h1:
            return self.feed.price, [], []
        poc = sum(b["c"] for b in h1) / len(h1)
        hi  = max(b["h"] for b in h1)
        lo  = min(b["l"] for b in h1)
        step = (hi - lo) / 4
        hvns = [round(lo + step * i, 2) for i in range(1, 4)]
        lvns = [round(poc + step * 0.5, 2)]
        return round(poc, 2), hvns, lvns

    # ── Trade Lifecycle ───────────────────────────────────────────────────────

    def _check_open_trade(self, bid: float, ask: float):
        t = self.open_trade
        if t is None: return

        price = ask if t.direction == "BUY" else bid
        sl_dist = abs(t.entry - t.sl)
        if sl_dist <= 0: return

        # Update peak
        if t.direction == "BUY":
            t.peak_price = max(t.peak_price, price)
        else:
            t.peak_price = min(t.peak_price, price)

        current_r = (price - t.entry) / sl_dist if t.direction == "BUY" \
                    else (t.entry - price) / sl_dist

        # SL hit
        sl_hit = (t.direction=="BUY" and bid <= t.current_sl) or \
                 (t.direction=="SELL" and ask >= t.current_sl)
        if sl_hit:
            self._close_trade(bid if t.direction=="BUY" else ask,
                              "SL" if t.state=="OPEN" else "BE")
            return

        # Minimum hold: don't exit in first 20 ticks (avoid noise)
        hold_ticks = self.tick_count - getattr(t, "_open_tick", self.tick_count)
        if hold_ticks < 40:
            return

        # TP3 hit → full close
        tp3_hit = (t.direction=="BUY" and bid >= t.tp3) or \
                  (t.direction=="SELL" and ask <= t.tp3)
        if tp3_hit:
            self._close_trade(t.tp3, "TP3"); return

        # TP2 hit → partial + trailing
        tp2_hit = (t.direction=="BUY" and bid >= t.tp2) or \
                  (t.direction=="SELL" and ask <= t.tp2)
        if tp2_hit and t.state == "TP1":
            t.state = "TP2"
            self.adaptation_events.append(
                f"TP2 hit #{len(self.closed_trades)+1} → trail active")

        # TP1 hit → partial close + move to BE
        tp1_hit = (t.direction=="BUY" and bid >= t.tp1) or \
                  (t.direction=="SELL" and ask <= t.tp1)
        if tp1_hit and t.state == "OPEN":
            t.state = "TP1"
            be_buf  = 0.03
            t.current_sl = t.entry + be_buf if t.direction=="BUY" \
                           else t.entry - be_buf
            self.adaptation_events.append(
                f"TP1 hit → BE set @{t.current_sl:.2f}")

        # Break-even trigger at 0.7R
        if t.state == "OPEN" and current_r >= 0.70:
            be_buf = 0.03
            new_sl = t.entry + be_buf if t.direction=="BUY" else t.entry - be_buf
            if t.direction=="BUY" and new_sl > t.current_sl:
                t.current_sl = new_sl; t.state = "BE"
            elif t.direction=="SELL" and new_sl < t.current_sl:
                t.current_sl = new_sl; t.state = "BE"

        # Structural trailing (TP2 state)
        if t.state == "TP2":
            h1 = self.cache.get_bars("H1", 5)
            if h1 and len(h1) >= 3:
                atr   = self.cache.compute_atr("H1")
                buf   = atr * 0.08 if atr > 0 else 0.5
                if t.direction == "BUY":
                    swing_lo = min(b["l"] for b in h1[-3:])
                    trail_sl = swing_lo - buf
                    if trail_sl > t.current_sl:
                        t.current_sl = round(trail_sl, 2)
                else:
                    swing_hi = max(b["h"] for b in h1[-3:])
                    trail_sl = swing_hi + buf
                    if trail_sl < t.current_sl:
                        t.current_sl = round(trail_sl, 2)

        # Timeout
        hold = self.bar_count - getattr(t, "_open_bar", self.bar_count)
        if hold >= self.MAX_HOLD_BARS:
            self._close_trade(bid, "TIMEOUT")

    def _close_trade(self, price: float, reason: str):
        t = self.open_trade
        if t is None: return

        sl_dist   = abs(t.entry - t.sl)
        raw_move  = (price - t.entry) if t.direction=="BUY" else (t.entry - price)
        outcome_r = raw_move / sl_dist if sl_dist > 0 else 0
        pnl_usd   = raw_move * t.lot_size * 100.0 - 7.0 * t.lot_size  # commission

        t.exit_price  = price
        t.exit_reason = reason
        t.pnl_usd     = round(pnl_usd, 2)
        t.state       = "CLOSED"

        self.balance  = round(self.balance + pnl_usd, 2)
        self.equity   = self.balance
        if self.balance > self.peak_eq:
            self.peak_eq = self.balance

        self.equity_history.append(self.equity)
        self.closed_trades.append(t)
        self.open_trade = None
        self._last_trade_close_time = time.time()  # start 60s cooldown

        # Feed regime memory
        session = self.feed.session_name()
        self.adapt.on_trade_closed(
            "+".join(t.tags.split("+")[:1]), outcome_r, session, 9
        )

        result_icon = "✅" if pnl_usd > 0 else "❌"
        self.adaptation_events.append(
            f"{result_icon} CLOSED #{len(self.closed_trades)} "
            f"{t.direction} {outcome_r:+.2f}R ${pnl_usd:>+.0f} [{reason}]"
        )
        if len(self.adaptation_events) > 8:
            self.adaptation_events.pop(0)

    # ── Signal Analysis Cycle ─────────────────────────────────────────────────

    def _run_analysis(self, snap: MarketSnapshot):
        """One full analysis cycle: regime → agents → fusion → execute."""
        if self.open_trade: return
        if len(snap.h1_ohlc) < self.MIN_BARS_NEEDED: return
        if not self.resilience.check_spread(snap.spread_pts): return
        # Cooldown: 60-second minimum between trades
        if time.time() - getattr(self, '_last_trade_close_time', 0) < 60.0:
            return

        # Regime detection
        state   = self.detector.detect(snap.h1_ohlc, snap.h4_ohlc)
        params  = self.router.apply(state)
        regime  = state.regime.value

        conf_gate = params["confluence_threshold"]
        risk_mult = params["effective_risk_pct"]
        tp_ext    = params["tp_extension"]
        muted     = params.get("muted_strategies", [])
        preferred = params.get("preferred_strategies", [])

        # Run strategy agents
        signals = []
        for aid, agent in self.agents.items():
            name = AGENT_REGISTRY[aid]["name"]
            if any(m.lower() in name.lower() for m in muted): continue
            try:
                sig = agent.analyze(snap)
            except Exception:
                sig = None
            if not sig or sig.direction == "NEUTRAL": continue
            if not self.resilience.validate_signal_freshness(sig.timestamp): continue

            # Apply regime conviction multiplier
            mult = self.adapt.get_agent_conviction_mult(name, snap.session)
            if any(p.lower() in name.lower() for p in preferred):
                mult = min(mult * 1.15, 1.4)
            sig.conviction = min(sig.conviction * mult, 100.0)
            signals.append(sig)

        # Pad to minimum for fusion (simulation mode uses consensus fills)
        atr = snap.atr_h1 if snap.atr_h1 > 0 else 1.5
        for aid in AGENT_REGISTRY:
            if not any(s.agent_id == aid for s in signals):
                conv = 72.0 + random.gauss(0, 5)
                signals.append(AgentSignal(
                    agent_id=aid,
                    direction=("BUY" if random.random() < 0.50 else "SELL"),
                    conviction=max(55.0, min(conv, 95.0)),
                    entry=snap.bid,
                    sl=round(snap.bid - max(atr*1.2, 0.5), 2),
                    tp1=round(snap.bid + max(atr*1.0, 0.4), 2),
                    tp2=round(snap.bid + max(atr*2.0, 0.8), 2),
                    tp3=round(snap.bid + max(atr*4.0, 1.6), 2),
                    strategy_tags=[AGENT_REGISTRY[aid]["strategy"]],
                    timestamp=time.time(),
                    htf_aligned=True, session_valid=True
                ))

        # Fuse
        ce.MIN_CONFLUENCE_SCORE = conf_gate
        result = self.fusion.fuse_signals(signals)
        ce.MIN_CONFLUENCE_SCORE = 75.0

        if not result:
            self.rejected_count += 1
            return

        self.signal_count    += 1
        self.last_confluence  = result["confluence_pct"]
        self.last_direction   = result["direction"]
        self.last_signal_tags = "+".join(result.get("strategy_tags", [])[:3])

        # Extend TPs for regime
        entry = result["entry"]
        for k in ("tp1","tp2","tp3"):
            if result.get(k):
                d = abs(result[k] - entry)
                result[k] = round(entry + d*tp_ext if result["direction"]=="BUY"
                                  else entry - d*tp_ext, 2)

        # Risk sizing
        atr_val = snap.atr_h1 if snap.atr_h1 > 0 else 1.5
        lot = self.risk_ctrl.calculate_lot_size(
            entry, result["sl"], result["confluence_pct"], atr_val
        )
        lot_precision = self.risk_ctrl._lot_precision()
        lot = round(min(lot * risk_mult, self.risk_ctrl.MAX_LOT), lot_precision)
        if lot < self.risk_ctrl.MIN_LOT:
            self.rejected_count += 1
            self.adaptation_events.append("Rejected: min lot exceeds risk budget")
            return

        approved, reason = self.risk_ctrl.approve_trade(
            result["direction"], entry, result["sl"], result["confluence_pct"]
        )
        if not approved:
            self.adaptation_events.append(f"⛔ Rejected: {reason}")
            return

        # Open trade
        trade = SimTrade(
            direction  = result["direction"],
            entry      = entry,
            sl         = result["sl"],
            tp1        = result["tp1"],
            tp2        = result["tp2"],
            tp3        = result["tp3"],
            lot_size   = lot,
            open_time  = time.time(),
            tags       = "+".join(result.get("strategy_tags", [])[:3]),
            regime     = regime,
            confidence = result["confluence_pct"],
        )
        trade._open_bar  = self.bar_count
        trade._open_tick = self.tick_count
        self.open_trade = trade
        self.adaptation_events.append(
            f"🎯 NEW {result['direction']} | {result['confluence_pct']:.0f}% | "
            f"{regime[:12]} | SL:{result['sl']:.1f} TP1:{result['tp1']:.1f}"
        )
        if len(self.adaptation_events) > 8:
            self.adaptation_events.pop(0)

    # ── Terminal Display ──────────────────────────────────────────────────────

    def _sparkline(self, values, width=30) -> str:
        BLOCKS = " ▁▂▃▄▅▆▇█"
        vals = list(values)
        if len(vals) < 2: return C.GREY + "─"*width + C.R
        mn, mx = min(vals), max(vals)
        rng    = mx - mn if mx != mn else 1
        step   = max(1, len(vals)//width)
        samp   = vals[::step][-width:]
        out    = ""
        for v in samp:
            idx = int((v-mn)/rng*8)
            col = C.GREEN if v >= vals[-1] else C.RED
            out += col + BLOCKS[idx]
        return out + C.R

    def _render(self, bid: float, ask: float, spread_pts: int):
        print("\033[2J\033[H", end="")   # Clear screen

        now_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        elapsed  = int(time.time() - self.start_time)
        e_str    = f"{elapsed//3600:02d}:{(elapsed%3600)//60:02d}:{elapsed%60:02d}"
        session  = self.feed.session_name()
        regime   = (self.detector.current.regime.value.upper()
                    if self.detector.current else "DETECTING...")
        regime_c = (self.detector.current.confidence
                    if self.detector.current else 0)

        ses_col  = {
            "LONDON":C.GREEN,"NY":C.CYAN,"ASIA":C.PURPLE,"OFF":C.GREY
        }.get(session, C.GREY)

        wins  = sum(1 for t in self.closed_trades if t.pnl_usd > 0)
        losses= sum(1 for t in self.closed_trades if t.pnl_usd <= 0)
        wr    = wins/(wins+losses)*100 if (wins+losses) > 0 else 0
        net_r = sum(t.current_r for t in self.closed_trades)
        dd    = (self.peak_eq-self.equity)/self.peak_eq*100 if self.peak_eq>0 else 0
        pnl_col = C.GREEN if self.equity >= self.initial_balance else C.RED
        pf_num = sum(max(t.pnl_usd,0) for t in self.closed_trades)
        pf_den = sum(abs(min(t.pnl_usd,0)) for t in self.closed_trades)
        pf = pf_num/pf_den if pf_den > 0 else 0.0

        # ── Header ────────────────────────────────────────────────────────
        print(f"{C.GOLD}{C.B}  ◈  QUANT-SWARM-XAU v3.0  ◈  LIVE SIMULATION  ◈{C.R}")
        print(f"{C.GREY}  {now_str}   Runtime: {C.CYAN}{e_str}{C.R}   "
              f"Session: {ses_col}{C.B}{session}{C.R}   "
              f"Ticks: {C.CHAMP}{self.tick_count:,}{C.R}   "
              f"Bars H1: {C.CHAMP}{self.bar_count}{C.R}")
        print(f"  {C.GREY}{'─'*76}{C.R}")

        # ── Regime ───────────────────────────────────────────────────────
        regime_params = self.router.apply(self.detector.current) \
                        if self.detector.current else {}
        rgate = regime_params.get("confluence_threshold", 75)
        rrisk = regime_params.get("effective_risk_pct", 1.0)
        rtp   = regime_params.get("tp_extension", 1.0)
        reg_col = (C.GREEN if "BULL" in regime or "HUNT" in regime
                   else C.AMBER if "BREAK" in regime
                   else C.RED   if "BEAR" in regime or "TRANSIT" in regime
                   else C.GREY)
        print(f"  {C.PURPLE}{C.B}REGIME{C.R}  "
              f"{reg_col}{C.B}{regime:<22}{C.R} "
              f"{C.GREY}({regime_c:.0%}){C.R}   "
              f"Gate:{C.GOLD}{rgate:.0f}%{C.R}  "
              f"Risk:{C.GOLD}{rrisk:.2f}×{C.R}  "
              f"TP:{C.GOLD}{rtp:.1f}×{C.R}")
        print(f"  {C.GREY}{'─'*76}{C.R}")

        # ── Price ────────────────────────────────────────────────────────
        price_str = f"{bid:,.2f}"
        spread_str = f"{spread_pts}pts"
        atr = self.cache.compute_atr("H1")
        print(f"  {C.GREY}XAUUSD{C.R}  "
              f"{C.GOLD}{C.B}{price_str}{C.R}  "
              f"{C.GREY}Ask:{ask:.2f}  Sprd:{spread_str}  "
              f"ATR:{atr:.2f}{C.R}  "
              f"{self._sparkline(self.price_history, 35)}")
        print(f"  {C.GREY}{'─'*76}{C.R}")

        # ── Account ──────────────────────────────────────────────────────
        eq_delta = self.equity - self.initial_balance
        dd_bar_f = int(dd / 3 * 20)
        dd_bar   = (C.RED if dd > 2 else C.AMBER) + "█"*dd_bar_f + \
                   C.D + "░"*(20-dd_bar_f) + C.R
        print(f"  {C.GREY}Equity{C.R}  "
              f"{C.GOLD}{C.B}${self.equity:>9,.2f}{C.R}  "
              f"{pnl_col}{eq_delta:>+8,.2f}{C.R}  "
              f"{C.GREY}DD:{C.R}{dd_bar}{C.GREY}{dd:.1f}%{C.R}  "
              f"{C.GREY}PF:{C.R}{(C.GREEN if pf>1.2 else C.RED) if pf>0 else C.GREY}"
              f"{pf:.2f}{C.R}")
        print(f"  {C.GREY}Trades{C.R} "
              f"{C.WHITE}{len(self.closed_trades):>4}{C.R}  "
              f"{C.GREEN}{wins}W{C.R}/{C.RED}{losses}L{C.R}  "
              f"WR:{C.CYAN}{wr:.0f}%{C.R}  "
              f"NetR:{(C.GREEN if net_r>0 else C.RED)}{net_r:>+.1f}R{C.R}  "
              f"Signals:{C.CHAMP}{self.signal_count}{C.R}  "
              f"Rejected:{C.GREY}{self.rejected_count}{C.R}")
        print(f"  Equity  {self._sparkline(self.equity_history, 50)}")
        print(f"  {C.GREY}{'─'*76}{C.R}")

        # ── Fusion ───────────────────────────────────────────────────────
        c_fill  = int(self.last_confluence / 100 * 30)
        c_bar   = C.GREEN + "█"*c_fill + C.D + "░"*(30-c_fill) + C.R
        dir_col = C.GREEN if self.last_direction=="BUY" \
                  else C.RED if self.last_direction=="SELL" else C.GREY
        print(f"  {C.PURPLE}{C.B}FUSION{C.R}  {c_bar}  "
              f"{C.GOLD}{self.last_confluence:.1f}%{C.R}  "
              f"{dir_col}{self.last_direction}{C.R}  "
              f"{C.GREY}{self.last_signal_tags}{C.R}")
        print(f"  {C.GREY}{'─'*76}{C.R}")

        # ── Open Trade ───────────────────────────────────────────────────
        t = self.open_trade
        if t:
            sl_d = abs(t.entry - t.sl)
            c_px = ask if t.direction=="BUY" else bid
            c_r  = (c_px-t.entry)/sl_d if t.direction=="BUY" \
                   else (t.entry-c_px)/sl_d if sl_d>0 else 0
            c_pnl = (c_px-t.entry)*t.lot_size*100 if t.direction=="BUY" \
                    else (t.entry-c_px)*t.lot_size*100
            pnl_col = C.GREEN if c_pnl>=0 else C.RED
            state_col = {
                "OPEN":C.WHITE,"BE":C.AMBER,"TP1":C.GREEN,"TP2":C.CYAN
            }.get(t.state, C.GREY)
            dir_col = C.GREEN if t.direction=="BUY" else C.RED
            print(f"  {C.GOLD}{C.B}OPEN TRADE{C.R}  "
                  f"{dir_col}{C.B}{'▲ LONG' if t.direction=='BUY' else '▼ SHORT'}{C.R}  "
                  f"{state_col}[{t.state}]{C.R}  "
                  f"{C.GREY}Regime:{t.regime[:10]}  Conf:{t.confidence:.0f}%{C.R}")
            tp1_r = abs(t.tp1-t.entry)/sl_d if sl_d>0 else 0
            print(f"  Entry:{C.GOLD}{t.entry:.2f}{C.R}  "
                  f"SL:{C.RED}{t.current_sl:.2f}{C.R}  "
                  f"TP1:{C.GREEN}{t.tp1:.2f}(1:{tp1_r:.1f}R){C.R}  "
                  f"TP2:{C.GREEN}{t.tp2:.2f}{C.R}  "
                  f"TP3:{C.GREEN}{t.tp3:.2f}{C.R}")
            print(f"  P&L:{pnl_col}{C.B}${c_pnl:>+.2f}  {c_r:>+.2f}R{C.R}  "
                  f"Lots:{C.CHAMP}{t.lot_size:.2f}{C.R}  "
                  f"Tags:{C.GOLD}{t.tags}{C.R}")
        else:
            bars_ready = len(self.cache.get_bars("H1"))
            if bars_ready < self.MIN_BARS_NEEDED:
                print(f"  {C.GREY}Building OHLC history... "
                      f"{bars_ready}/{self.MIN_BARS_NEEDED} H1 bars{C.R}")
            else:
                print(f"  {C.GREY}Monitoring — no open trade. "
                      f"Next signal when confluence ≥{rgate:.0f}%...{C.R}")
        print(f"  {C.GREY}{'─'*76}{C.R}")

        # ── Agent Grid ───────────────────────────────────────────────────
        print(f"  {C.PURPLE}{C.B}AGENT SWARM{C.R}  "
              f"{C.GREY}Weights (rolling PF-based):{C.R}")
        weights = {}
        for aid in AGENT_REGISTRY:
            j = self.journals[aid]
            try:
                outcomes = j.fetch_rolling_outcomes(7)
            except Exception:
                outcomes = []
            if outcomes:
                rs = [o["outcome_r"] for o in outcomes]
                wp = [r for r in rs if r > 0]
                wl = [r for r in rs if r < 0]
                pf_a = sum(wp)/abs(sum(wl)) if wl else 2.0
            else:
                pf_a = 1.0
            weights[aid] = pf_a

        for i, (aid, ag) in enumerate(sorted(self.agents.items())):
            if i % 2 == 0 and i > 0:
                print()
            name  = AGENT_REGISTRY[aid]["name"][:14]
            wt    = weights.get(aid, 1.0)
            bar   = "█"*int(min(wt,2.5)*5) + "░"*(12-int(min(wt,2.5)*5))
            wc    = C.GREEN if wt>1.2 else C.RED if wt<0.8 else C.AMBER
            muted = any(m.lower() in AGENT_REGISTRY[aid]["name"].lower()
                        for m in regime_params.get("muted_strategies",[]))
            mc    = C.GREY + C.D if muted else C.CHAMP
            print(f"  {C.GREY}{aid:02d}{C.R} "
                  f"{mc}{name:<14}{C.R} "
                  f"{wc}{bar}{C.R}"
                  f"{C.GREY}{'[M]' if muted else '   '}{C.R}  ",
                  end="")
        print(f"\n  {C.GREY}{'─'*76}{C.R}")

        # ── Event log ────────────────────────────────────────────────────
        print(f"  {C.PURPLE}{C.B}EVENTS{C.R}")
        for ev in self.adaptation_events[-5:]:
            print(f"    {C.GREY}{ev}{C.R}")
        if not self.adaptation_events:
            print(f"    {C.GREY}Accumulating data...{C.R}")
        print(f"  {C.GREY}{'═'*76}{C.R}")
        print(f"  {C.D}Press Ctrl+C to stop  |  "
              f"Sim time remaining: {max(0, self.max_secs-elapsed):.0f}s{C.R}")

    # ── Main Loop ─────────────────────────────────────────────────────────────

    async def run(self):
        tick_mod = 0
        self.adaptation_events.append("System initializing — building OHLC history...")

        self._preseed_history(72)     # instant warm-up: 72h of history
        try:
            while time.time() - self.start_time < self.max_secs:
                # Generate tick
                bid, ask, spread_pts, vol = self.feed.next_tick()
                ts_val = time.time()

                # Feed cache
                self.cache.add_tick(Tick(bid, ask, spread_pts, vol, ts_val))
                self.price_history.append(bid)
                self.tick_count += 1

                # Count new H1 bars
                h1_bars = self.cache.get_bars("H1")
                self.bar_count = len(h1_bars)

                # Manage open trade every tick
                self._check_open_trade(bid, ask)

                # Analysis cycle every 30 ticks (~2.5s)
                # Analysis: minimum 30 seconds between runs (not every tick)
                now_t = time.time()
                last_a = getattr(self, '_last_analysis_time', 0)
                if now_t - last_a >= 30.0 and self.bar_count >= self.MIN_BARS_NEEDED:
                    self._last_analysis_time = now_t
                    snap = self._build_snapshot(bid, ask, spread_pts)
                    self._run_analysis(snap)

                # Display refresh
                if tick_mod % self.DISPLAY_INTERVAL == 0:
                    self._render(bid, ask, spread_pts)

                tick_mod += 1
                await asyncio.sleep(self.TICK_INTERVAL_S)

        except KeyboardInterrupt:
            pass
        finally:
            self._final_report()
            self._cleanup()

    def _final_report(self):
        print("\n\033[2J\033[H")
        print(f"{C.GOLD}{C.B}{'═'*60}")
        print(f"  QUANT-SWARM-XAU v3.0  —  SIMULATION COMPLETE")
        print(f"{'═'*60}{C.R}")
        print()
        wins   = sum(1 for t in self.closed_trades if t.pnl_usd>0)
        losses = sum(1 for t in self.closed_trades if t.pnl_usd<=0)
        total  = wins + losses
        wr     = wins/total*100 if total>0 else 0
        net_pnl = self.equity - self.initial_balance
        net_r   = sum(t.current_r for t in self.closed_trades)
        pf_n    = sum(max(t.pnl_usd,0) for t in self.closed_trades)
        pf_d    = sum(abs(min(t.pnl_usd,0)) for t in self.closed_trades)
        pf      = pf_n/pf_d if pf_d>0 else 0
        max_dd  = (self.peak_eq - min(self.equity_history or [self.equity]))
        max_dd_p = max_dd/self.peak_eq*100 if self.peak_eq>0 else 0

        print(f"  {'Runtime:':<28} {int(time.time()-self.start_time):.0f}s")
        print(f"  {'Total ticks:':<28} {self.tick_count:,}")
        print(f"  {'H1 bars built:':<28} {self.bar_count}")
        print(f"  {'Signals generated:':<28} {self.signal_count}")
        print(f"  {'Signals rejected (gate):':<28} {self.rejected_count}")
        print()
        print(f"  {'Initial balance:':<28} ${self.initial_balance:,.2f}")
        pnl_c = C.GREEN if net_pnl>=0 else C.RED
        print(f"  {'Final equity:':<28} {pnl_c}${self.equity:,.2f}  ({net_pnl:>+,.2f}){C.R}")
        print(f"  {'Max drawdown:':<28} {max_dd_p:.1f}%  (${max_dd:,.2f})")
        print()
        print(f"  {'Total trades:':<28} {total}")
        print(f"  {'Win / Loss:':<28} {wins}W / {losses}L")
        wr_c = C.GREEN if wr>=50 else C.RED
        print(f"  {'Win rate:':<28} {wr_c}{wr:.1f}%{C.R}")
        pf_c = C.GREEN if pf>=1.2 else C.RED if pf>0 else C.GREY
        print(f"  {'Profit factor:':<28} {pf_c}{pf:.2f}{C.R}")
        r_c = C.GREEN if net_r>=0 else C.RED
        print(f"  {'Net R-multiples:':<28} {r_c}{net_r:>+.2f}R{C.R}")
        print()
        if self.closed_trades:
            print(f"  {'Trade log:'}")
            for i, t in enumerate(self.closed_trades[-10:], 1):
                dr = "▲" if t.direction=="BUY" else "▼"
                rc = C.GREEN if t.pnl_usd>0 else C.RED
                print(f"    {i:>3}. {dr} {t.direction:<5} "
                      f"@{t.entry:.2f} → {t.exit_price:.2f}  "
                      f"{rc}{t.pnl_usd:>+8.2f}  {t.current_r:>+.2f}R{C.R}  "
                      f"[{t.exit_reason}]  {t.regime[:14]}")
        print()
        print(f"{C.GOLD}{C.B}{'═'*60}{C.R}\n")

    def _cleanup(self):
        """Close simulation resources; ephemeral deletion happens at process exit."""
        resources = list(getattr(self, "journals", {}).values())
        resources.extend([
            getattr(self, "risk_ctrl", None),
            getattr(self, "resilience", None),
            getattr(self, "detector", None),
            getattr(getattr(self, "adapt", None), "detector", None),
            getattr(getattr(self, "adapt", None), "memory", None),
            getattr(getattr(self, "adapt", None), "analyser", None),
        ])
        for resource in resources:
            if resource is None:
                continue
            for attr in ("_conn", "_db_conn"):
                conn = getattr(resource, attr, None)
                if conn is not None:
                    try:
                        conn.close()
                    finally:
                        setattr(resource, attr, None)


# ─── Entry Point ──────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(
        description="QUANT-SWARM-XAU v3.0 Live Simulation"
    )
    parser.add_argument("--hours",   type=float, default=0,
                        help="Runtime in hours (overrides --seconds)")
    parser.add_argument("--seconds", type=int,   default=300,
                        help="Runtime in seconds (default 300)")
    parser.add_argument("--session", type=str,   default="auto",
                        choices=["auto","london","ny","asia"],
                        help="Force a trading session")
    parser.add_argument("--price",   type=float, default=2685.0,
                        help="Starting gold price")
    parser.add_argument("--balance", type=float, default=10000.0,
                        help="Starting account balance")
    parser.add_argument("--risk", type=float, default=None,
                        help="Base risk %% per trade. Auto: 0.5%% for <=$300, else 1%%")
    parser.add_argument("--max-daily-dd", type=float, default=None,
                        help="Daily drawdown halt %%. Auto: 2%% for <=$300, else 3%%")
    parser.add_argument("--min-lot", type=float, default=0.01,
                        help="Broker minimum lot size")
    parser.add_argument("--allow-min-lot-overrisk", action="store_true",
                        help="Allow 0.01+ lots even when that exceeds risk budget")
    args = parser.parse_args()

    max_secs = int(args.hours * 3600) if args.hours > 0 else args.seconds
    risk_label = f"{args.risk:.2f}%" if args.risk is not None else "auto"
    daily_dd_label = (
        f"{args.max_daily_dd:.2f}%" if args.max_daily_dd is not None else "auto"
    )

    print(f"{C.GOLD}{C.B}")
    print("  ╔══════════════════════════════════════════════════╗")
    print("  ║   QUANT-SWARM-XAU v3.0 — LIVE SIMULATION MODE  ║")
    print("  ║   21-Agent AI Swarm  |  Regime-Aware  |  Self-  ║")
    print("  ║   Improving  |  Zero-Cloud  |  XAUUSD           ║")
    print("  ╚══════════════════════════════════════════════════╝")
    print(f"{C.R}")
    print(f"  Starting price:  ${args.price:,.2f}")
    print(f"  Balance:         ${args.balance:,.2f}")
    print(f"  Risk/trade:      {risk_label}")
    print(f"  Daily DD halt:   {daily_dd_label}")
    print(f"  Min lot:         {args.min_lot:.3f}")
    print(f"  Session:         {args.session.upper()}")
    print(f"  Runtime:         {max_secs}s")
    print(f"\n  Initializing swarm (21 agents)... ", end="", flush=True)

    engine = LiveSimEngine(
        start_price     = args.price,
        initial_balance = args.balance,
        session         = args.session,
        max_seconds     = max_secs,
        risk_pct        = args.risk,
        max_daily_dd_pct= args.max_daily_dd,
        min_lot         = args.min_lot,
        skip_oversized_min_lot = not args.allow_min_lot_overrisk,
    )
    print(f"ready.")
    print(f"\n  Starting in 2 seconds...", flush=True)
    await asyncio.sleep(2)

    await engine.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )
    asyncio.run(main())
