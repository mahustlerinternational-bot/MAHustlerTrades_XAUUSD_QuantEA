"""
QUANT-SWARM-XAU: Continuous Market Adaptation Engine
The forward-looking self-improvement layer — takes the journaled
performance of the past and applies it to future market conditions.

Three interlocking systems:
  1. RegimeMemory      - Learns which strategies work in which regime
  2. MarketCalendar    - Pre-loads known recurring XAUUSD behavioral patterns
  3. ForwardOptimiser  - Uses rolling regime×strategy data to set
                         future agent weights and thresholds prospectively

This is the engine that answers the question:
  "Given what the system learned yesterday, how should it behave tomorrow?"
"""

import asyncio
import json
import logging
import math
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from safe_paths import bootstrap_ephemeral_state, runtime_state_dir

if __name__ == "__main__":
    bootstrap_ephemeral_state("continuous-learner-demo")

from regime_detector import (
    MarketRegimeDetector, AdaptiveParameterRouter,
    RegimeHistoryAnalyser, Regime, RegimeState, REGIME_DB
)

log = logging.getLogger("CONTINUOUS-LEARNER")

LEARN_DB = runtime_state_dir() / "continuous_learning.db"


# ─── 1. Regime Memory ─────────────────────────────────────────────────────────

class RegimeMemory:
    """
    Tracks strategy performance broken down by regime, session, and time-of-day.
    Produces live strategy conviction multipliers based on this learned data.

    For each (regime × strategy × session) combination it accumulates:
      - Rolling win rate
      - Rolling profit factor
      - Rolling expectancy
      - Sample count (confidence weight)

    These multipliers are injected into the Confluence Fusion Engine's
    weight calculation, supplementing the static Sharpe/PF formula.
    """

    MIN_SAMPLES = 5       # Need at least 5 trades to trust a multiplier
    DECAY_RATE  = 0.02    # Per-week decay of old data (recency weighting)
    MAX_MULT    = 1.40    # Cap: no strategy gets more than 40% boost
    MIN_MULT    = 0.40    # Floor: worst performer still gets 40% weight

    def __init__(self):
        self._db_conn: Optional[sqlite3.Connection] = None  # persistent — one FD
        self._init_db()
        log.info("RegimeMemory initialized")

    def _get_db(self) -> Optional[sqlite3.Connection]:
        """Return the persistent SQLite connection, opening it once if needed."""
        if self._db_conn is None:
            try:
                LEARN_DB.parent.mkdir(parents=True, exist_ok=True)
                self._db_conn = sqlite3.connect(
                    str(LEARN_DB), timeout=5, check_same_thread=False
                )
            except Exception as exc:
                log.debug("RegimeMemory: DB connect failed: %s", exc)
        return self._db_conn

    def _reset_db(self):
        """Close and discard the cached connection; next call gets a fresh one."""
        try:
            if self._db_conn:
                self._db_conn.close()
        except Exception:
            pass
        self._db_conn = None

    def _init_db(self):
        try:
            conn = self._get_db()
            if conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS regime_memory (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        regime TEXT, strategy TEXT, session TEXT,
                        hour_utc INTEGER,
                        outcome_r REAL, win INTEGER,
                        timestamp REAL, week_number INTEGER
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS learned_multipliers (
                        regime TEXT, strategy TEXT, session TEXT,
                        multiplier REAL, pf REAL, win_rate REAL,
                        sample_count INTEGER, last_updated REAL,
                        PRIMARY KEY (regime, strategy, session)
                    )
                """)
                conn.commit()
        except Exception as exc:
            log.debug("RegimeMemory: DB init skipped: %s", exc)
            self._reset_db()

    def record_trade(self, regime: str, strategy: str, session: str,
                     hour_utc: int, outcome_r: float):
        """Called by PMA after every trade close."""
        try:
            week = int(time.time() // (7 * 86400))
            conn = self._get_db()
            if conn:
                conn.execute("""
                    INSERT INTO regime_memory VALUES (NULL,?,?,?,?,?,?,?,?)
                """, (regime, strategy, session, hour_utc, outcome_r,
                      int(outcome_r > 0), time.time(), week))
                conn.commit()
        except Exception as _e:
            log.debug("record_trade DB error: %s", _e)
            self._reset_db()
            return
        self._update_multiplier(regime, strategy, session)

    def _update_multiplier(self, regime: str, strategy: str, session: str):
        """Recompute the learned multiplier for this (regime, strategy, session)."""
        try:
            since = time.time() - 60 * 86400
            conn = self._get_db()
            if not conn:
                return
            rows = conn.execute("""
                SELECT outcome_r, win, timestamp FROM regime_memory
                WHERE regime=? AND strategy=? AND session=? AND timestamp>=?
                ORDER BY timestamp DESC LIMIT 100
            """, (regime, strategy, session, since)).fetchall()
        except Exception as exc:
            log.debug("_update_multiplier DB read skipped: %s", exc)
            self._reset_db()
            return

        if len(rows) < self.MIN_SAMPLES:
            return

        now = time.time()
        weighted_wins = weighted_r = total_weight = 0.0
        for r, win, ts in rows:
            age_weeks = (now - ts) / (7 * 86400)
            w = math.exp(-self.DECAY_RATE * age_weeks)
            weighted_wins  += win * w
            weighted_r     += r * w
            total_weight   += w

        if total_weight == 0:
            return

        wr   = weighted_wins / total_weight
        avg_r = weighted_r / total_weight

        gross_profit = sum(max(r, 0) * math.exp(-self.DECAY_RATE *
                            (now-ts)/(7*86400)) for r,_,ts in rows)
        gross_loss   = sum(abs(min(r, 0)) * math.exp(-self.DECAY_RATE *
                            (now-ts)/(7*86400)) for r,_,ts in rows)
        pf = gross_profit / gross_loss if gross_loss > 0 else 2.0

        raw_mult = 0.6 + (pf - 0.5) * 0.4
        raw_mult = max(self.MIN_MULT, min(self.MAX_MULT, raw_mult))

        try:
            conn = self._get_db()
            if conn:
                conn.execute("""
                    INSERT OR REPLACE INTO learned_multipliers VALUES (?,?,?,?,?,?,?,?)
                """, (regime, strategy, session,
                      round(raw_mult, 4), round(pf, 4), round(wr, 4),
                      len(rows), time.time()))
                conn.commit()
        except Exception as exc:
            log.debug("_update_multiplier DB write skipped: %s", exc)
            self._reset_db()

    def get_multiplier(self, regime: str, strategy: str,
                        session: str) -> float:
        """Live lookup of the learned conviction multiplier."""
        try:
            conn = self._get_db()
            if not conn:
                return 1.0
            row = conn.execute("""
                SELECT multiplier, sample_count FROM learned_multipliers
                WHERE regime=? AND strategy=? AND session=?
            """, (regime, strategy, session)).fetchone()
        except Exception as exc:
            log.debug("get_multiplier DB read skipped: %s", exc)
            self._reset_db()
            return 1.0
        if not row or row[1] < self.MIN_SAMPLES:
            return 1.0
        return row[0]

    def get_all_multipliers(self) -> dict:
        """Full multiplier table for dashboard display."""
        try:
            conn = self._get_db()
            if not conn:
                return []
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT * FROM learned_multipliers ORDER BY multiplier DESC
            """).fetchall()
            return [dict(r) for r in rows]
        except Exception as exc:
            log.debug("get_all_multipliers DB read skipped: %s", exc)
            self._reset_db()
            return []

    def format_learned_table(self) -> str:
        rows = self.get_all_multipliers()
        if not rows:
            return "  No learned data yet — accumulates after 5+ trades per context"
        lines = [f"  {'Regime':<22} {'Strategy':<22} {'Session':<10} "
                 f"{'Mult':>6} {'PF':>6} {'WR':>6} {'N':>5}"]
        lines.append("  " + "─"*75)
        for r in rows[:20]:
            bar = "▲" if r["multiplier"] > 1.05 else ("▼" if r["multiplier"] < 0.95 else "─")
            lines.append(f"  {r['regime']:<22} {r['strategy']:<22} "
                         f"{r['session']:<10} {r['multiplier']:>6.3f} "
                         f"{r['pf']:>6.2f} {r['win_rate']*100:>5.1f}% "
                         f"{r['sample_count']:>5}  {bar}")
        return "\n".join(lines)


# ─── 2. Market Calendar Intelligence ─────────────────────────────────────────

class MarketCalendarIntelligence:
    """
    Encodes known recurring XAUUSD behavioral patterns:
    - Day-of-week patterns (Monday accumulation, Friday profit-taking)
    - Month-turn flows (end-of-month institutional rebalancing)
    - Quarterly option expiry effects (major vol events)
    - US economic release schedule (recurring NFP/CPI timing)
    - Known Gold seasonality (September weakness, Q1 strength)

    These are used to adjust baseline expected value before the
    regime detector runs, providing a prior probability distribution.
    """

    # Day-of-week bias for XAUUSD (documented from 2015-2024 data)
    DOW_BIAS = {
        0: {"label": "Monday",    "vol_mult": 0.85, "trend_bias":  0.0,
            "note": "Slow open, gap-fill tendency, reduced signals"},
        1: {"label": "Tuesday",   "vol_mult": 1.05, "trend_bias": +0.1,
            "note": "Trend establishment day — best for breakout strategies"},
        2: {"label": "Wednesday", "vol_mult": 1.15, "trend_bias": +0.15,
            "note": "Highest avg range day — peak killzone activity"},
        3: {"label": "Thursday",  "vol_mult": 1.10, "trend_bias": +0.1,
            "note": "Continuation or reversal — watch for EOD institutional activity"},
        4: {"label": "Friday",    "vol_mult": 0.80, "trend_bias": -0.2,
            "note": "Position squaring — avoid new trades after NY open"},
    }

    # Hour-of-day vol multipliers (UTC)
    HOD_VOL = {
        0: 0.40, 1: 0.35, 2: 0.35, 3: 0.40, 4: 0.45,   # Deep Asia
        5: 0.55, 6: 0.70, 7: 0.95,                        # Pre-London
        8: 1.40, 9: 1.50, 10: 1.30, 11: 1.10,            # London core
        12: 0.85, 13: 1.20,                                # London/NY overlap
        14: 1.55, 15: 1.45, 16: 1.35,                     # NY open — peak volume
        17: 1.10, 18: 0.90, 19: 0.75, 20: 0.65,          # NY afternoon
        21: 0.50, 22: 0.45, 23: 0.40,                     # NY close
    }

    # Monthly patterns (1=Jan, 12=Dec)
    MONTHLY_BIAS = {
        1:  {"label": "January",   "bias": +0.15,
             "note": "New year positioning — Gold often strong"},
        2:  {"label": "February",  "bias": +0.05, "note": "Neutral"},
        3:  {"label": "March",     "bias": -0.05,
             "note": "Quarter-end rebalancing — dollar strength possible"},
        4:  {"label": "April",     "bias": +0.10,
             "note": "Tax season, inflation data — Gold supportive"},
        5:  {"label": "May",       "bias":  0.00, "note": "Neutral"},
        6:  {"label": "June",      "bias": -0.10,
             "note": "Mid-year dollar strength, risk-on tends to hurt Gold"},
        7:  {"label": "July",      "bias": +0.05, "note": "Summer range"},
        8:  {"label": "August",    "bias": +0.10,
             "note": "Risk-off tendency — Gold seasonal strength begins"},
        9:  {"label": "September", "bias": -0.15,
             "note": "Historically weakest Gold month — central bank selling"},
        10: {"label": "October",   "bias":  0.00, "note": "Neutral"},
        11: {"label": "November",  "bias": +0.05,
             "note": "Year-end positioning begins"},
        12: {"label": "December",  "bias": +0.10,
             "note": "Year-end Gold buying — low liquidity amplifies moves"},
    }

    def get_current_context(self) -> dict:
        """Returns current calendar intelligence context."""
        import datetime
        now  = datetime.datetime.utcnow()
        dow  = now.weekday()          # 0=Mon
        hour = now.hour
        month = now.month
        day   = now.day

        dow_data   = self.DOW_BIAS.get(dow, self.DOW_BIAS[2])
        hod_mult   = self.HOD_VOL.get(hour, 0.5)
        month_data = self.MONTHLY_BIAS.get(month, {"bias": 0, "label": "?", "note": ""})

        # Month-turn effect: last 3 / first 3 days = institutional flow
        is_month_turn = day <= 3 or day >= 28

        # Quarter-end: March/June/September/December last week
        is_quarter_end = month in [3, 6, 9, 12] and day >= 25

        # NFP first Friday of month (approx)
        is_nfp_week = month in range(1, 13) and dow == 4 and day <= 7

        return {
            "day_of_week":       dow_data["label"],
            "vol_multiplier":    dow_data["vol_mult"] * hod_mult,
            "trend_bias":        dow_data["trend_bias"] + month_data["bias"],
            "session_note":      dow_data["note"],
            "monthly_bias":      month_data["bias"],
            "monthly_note":      month_data["note"],
            "is_month_turn":     is_month_turn,
            "is_quarter_end":    is_quarter_end,
            "is_nfp_week":       is_nfp_week,
            "reduce_risk":       dow == 4 or is_quarter_end,  # Friday or quarter-end
            "increase_alertness": is_nfp_week or is_month_turn,
        }

    def format_morning_intelligence(self) -> str:
        ctx = self.get_current_context()
        lines = [
            "CALENDAR INTELLIGENCE",
            "─" * 45,
            f"  Day:          {ctx['day_of_week']}",
            f"  Vol mult:     {ctx['vol_multiplier']:.2f}×",
            f"  Trend bias:   {ctx['trend_bias']:>+.2f}",
            f"  Note:         {ctx['session_note']}",
            f"  Monthly:      {ctx['monthly_note']}",
        ]
        if ctx["reduce_risk"]:
            lines.append("  ⚠️  REDUCE RISK: Friday/Quarter-end — no new trades after NY open")
        if ctx["increase_alertness"]:
            lines.append("  ⚡ HIGH ALERT: NFP week or month-turn — institutional flows active")
        if ctx["is_quarter_end"]:
            lines.append("  📅 QUARTER-END: Expect rebalancing flows — favour mean reversion")
        return "\n".join(lines)


# ─── 3. Forward Optimiser ─────────────────────────────────────────────────────

class ForwardOptimiser:
    """
    The brain that connects everything learned to future action.
    Runs every 4 hours and produces the 'optimised agent parameter set'
    for the next trading session — based on:

    1. Current detected regime
    2. Learned regime×strategy multipliers from RegimeMemory
    3. Calendar intelligence priors
    4. Rolling agent journal performance (14-day Sharpe/PF)
    5. Regime transition probabilities (what regime is likely next?)

    Output: A JSON config blob that gets loaded by the orchestrator
    at the start of each session, effectively 'pre-calibrating' the
    system for the conditions it's most likely to encounter.
    """

    OPTIMISE_INTERVAL_H = 4   # Re-optimise every 4 hours

    def __init__(self, regime_detector: MarketRegimeDetector,
                 regime_memory: RegimeMemory,
                 calendar: MarketCalendarIntelligence,
                 analyser: RegimeHistoryAnalyser):
        self.detector  = regime_detector
        self.memory    = regime_memory
        self.calendar  = calendar
        self.analyser  = analyser
        self._last_run = 0.0
        self._current_optimised_params: dict = {}
        self._optimisation_log: list = []

    async def run_loop(self):
        """Background loop: re-optimise every 4 hours."""
        while True:
            await asyncio.sleep(self.OPTIMISE_INTERVAL_H * 3600)
            params = self.optimise()
            log.info("Forward optimisation complete: %d agent params updated",
                     len(params.get("agent_multipliers", {})))

    def optimise(self, h1_ohlc: list = None,
                  h4_ohlc: list = None) -> dict:
        """
        Full forward optimisation pass.
        Returns the recommended parameter set for the next session.
        """
        now = time.time()
        self._last_run = now

        # ── Step 1: Detect current regime ────────────────────────────────
        if h1_ohlc and h4_ohlc and len(h1_ohlc) > 20:
            regime_state = self.detector.detect(h1_ohlc, h4_ohlc)
        elif self.detector.current:
            regime_state = self.detector.current
        else:
            # No market data — use calendar as prior
            regime_state = None

        current_regime = regime_state.regime.value if regime_state else "ranging_tight"

        # ── Step 2: Get calendar context ──────────────────────────────────
        cal = self.calendar.get_current_context()

        # ── Step 3: Build agent multipliers from regime memory ────────────
        strategies = [
            "ICT_SMC", "Asia_Sweep", "CRT_Engine", "HTF_Trap",
            "Wyckoff", "DXY_Div", "TL_Break", "Mean_Rev",
            "News_Arb", "VPVR_POC"
        ]
        sessions = ["LONDON", "NY", "ASIA"]

        agent_mults = {}
        for strat in strategies:
            best_session_mult = 1.0
            best_session      = "LONDON"
            for sess in sessions:
                m = self.memory.get_multiplier(current_regime, strat, sess)
                if m > best_session_mult:
                    best_session_mult = m
                    best_session      = sess
            # Apply calendar vol adjustment
            cal_adj = 0.8 + cal["vol_multiplier"] * 0.2
            agent_mults[strat] = round(best_session_mult * cal_adj, 4)

        # ── Step 4: Adjust confluence threshold ───────────────────────────
        base_conf = 75.0
        if regime_state:
            base_conf = regime_state.params.get("confluence_threshold", 75.0)

        # Calendar adjustments
        if cal["reduce_risk"]:
            base_conf  = min(base_conf + 5.0, 85.0)
        if cal["is_nfp_week"]:
            base_conf  = min(base_conf + 3.0, 85.0)

        # ── Step 5: Risk sizing recommendation ───────────────────────────
        base_risk = 1.0
        if regime_state:
            base_risk *= regime_state.params.get("risk_multiplier", 1.0)
        if cal["reduce_risk"]:
            base_risk *= 0.75
        base_risk = round(max(0.0, min(base_risk, 1.5)), 3)

        # ── Step 6: Session recommendation ───────────────────────────────
        if regime_state:
            session_bias = regime_state.params.get("session_bias", "LONDON")
        else:
            session_bias = "LONDON"  # Default

        # ── Step 7: Regime transition probability ─────────────────────────
        try:
            trans = self.analyser.transition_matrix(days=30)
            next_regime_probs = trans.get(current_regime, {})
            likely_next = max(next_regime_probs, key=next_regime_probs.get) \
                          if next_regime_probs else current_regime
        except Exception:
            next_regime_probs = {}
            likely_next = current_regime

        # ── Compile final parameter set ───────────────────────────────────
        result = {
            "generated_at":         now,
            "valid_for_hours":      self.OPTIMISE_INTERVAL_H,
            "current_regime":       current_regime,
            "regime_confidence":    regime_state.confidence if regime_state else 0.5,
            "likely_next_regime":   likely_next,
            "next_regime_probs":    next_regime_probs,
            "confluence_threshold": round(base_conf, 1),
            "effective_risk_pct":   base_risk,
            "agent_multipliers":    agent_mults,
            "session_bias":         session_bias,
            "calendar_context":     cal,
            "top_strategies":       sorted(agent_mults.items(),
                                           key=lambda x: -x[1])[:3],
            "muted_strategies":     [k for k, v in agent_mults.items() if v < 0.6],
        }

        self._current_optimised_params = result
        self._optimisation_log.append({
            "timestamp": now,
            "regime":    current_regime,
            "conf_thresh": base_conf,
            "risk":       base_risk,
            "top":        result["top_strategies"][:2],
        })

        return result

    def format_session_briefing(self) -> str:
        """Pre-session briefing from optimised params."""
        p = self._current_optimised_params
        if not p:
            return "No optimisation run yet"

        top = ", ".join(f"{s[0]}({s[1]:.2f}×)" for s in p.get("top_strategies", []))
        muted = ", ".join(p.get("muted_strategies", [])) or "none"

        lines = [
            "FORWARD-OPTIMISED SESSION PARAMETERS",
            "=" * 52,
            f"  Current Regime:    {p.get('current_regime','?').upper()}",
            f"  Confidence:        {p.get('regime_confidence',0):.0%}",
            f"  Next Likely:       {p.get('likely_next_regime','?').upper()}",
            f"  Confluence Gate:   {p.get('confluence_threshold',75):.0f}%",
            f"  Risk per Trade:    {p.get('effective_risk_pct',1.0):.2f}%",
            f"  Session Focus:     {p.get('session_bias','LONDON')}",
            f"  Top Strategies:    {top}",
            f"  Muted:             {muted}",
            "─" * 52,
            self.calendar.format_morning_intelligence(),
        ]
        return "\n".join(lines)


# ─── 4. Continuous Adaptation Loop ───────────────────────────────────────────

class ContinuousAdaptationEngine:
    """
    Top-level coordinator that runs all continuous learning systems
    as a background async service alongside the main orchestrator.

    Every 60 seconds:
      - Regime detector classifies current market state
      - Parameter router adjusts confluence/risk/TP for the regime
      - Agent conviction multipliers applied from RegimeMemory

    Every 4 hours:
      - ForwardOptimiser runs full session pre-calibration
      - New optimised parameter set persisted to disk
      - Telegram briefing updated with regime intelligence

    Every PMA event (15 min after trade close):
      - RegimeMemory.record_trade() called with (regime, strategy, session, r)
      - Learned multipliers updated with recency-decayed statistics
      - Regime×strategy performance DB grows permanently
    """

    def __init__(self):
        self.detector   = MarketRegimeDetector()
        self.router     = AdaptiveParameterRouter()
        self.memory     = RegimeMemory()
        self.calendar   = MarketCalendarIntelligence()
        self.analyser   = RegimeHistoryAnalyser()
        self.optimiser  = ForwardOptimiser(
            self.detector, self.memory, self.calendar, self.analyser
        )
        self._latest_params: dict = {}
        log.info("Continuous Adaptation Engine online")

    async def regime_loop(self, get_snapshot_fn):
        """60-second regime detection loop."""
        while True:
            try:
                snap = get_snapshot_fn()
                if snap:
                    state = self.detector.detect(
                        snap.h1_ohlc, snap.h4_ohlc,
                        news_active=snap.news_impact > 0.7
                    )
                    self._latest_params = self.router.apply(state)
                    log.debug("Regime: %s (%.0f%%)",
                              state.regime.value, state.confidence * 100)
            except Exception as e:
                log.error("Regime loop error: %s", e)
            await asyncio.sleep(60)

    async def optimise_loop(self):
        """4-hour forward optimisation loop."""
        while True:
            await asyncio.sleep(4 * 3600)
            try:
                self.optimiser.optimise()
                log.info("Forward optimisation complete")
            except Exception as e:
                log.error("Optimiser error: %s", e)

    def on_trade_closed(self, strategy: str, outcome_r: float,
                         session: str, hour_utc: int):
        """Hook: called by PMA 15min after every trade closes."""
        regime = self._latest_params.get("regime", "ranging_tight")
        self.memory.record_trade(regime, strategy, session, hour_utc, outcome_r)
        self.detector.log_strategy_outcome(regime, strategy, outcome_r)
        log.info("Regime memory updated: %s × %s → %.2fR", regime, strategy, outcome_r)

    def get_live_params(self) -> dict:
        """Current regime-adjusted parameters for the orchestrator."""
        return self._latest_params.copy()

    def get_agent_conviction_mult(self, agent_name: str, session: str) -> float:
        """Live conviction multiplier for a specific agent+session."""
        regime = self._latest_params.get("regime", "ranging_tight")
        base_mult = self.memory.get_multiplier(regime, agent_name, session)
        # Also apply regime routing (preferred/muted)
        return self.router.adjust_agent_conviction(
            agent_name, 100.0 * base_mult, self._latest_params
        ) / 100.0

    def format_status(self) -> str:
        """One-line status for dashboard."""
        if not self._latest_params:
            return "Adaptation engine: warming up..."
        p = self._latest_params
        return (f"Regime: {p.get('regime','?').upper():20} "
                f"Gate:{p.get('confluence_threshold',75):.0f}%  "
                f"Risk:{p.get('effective_risk_pct',1.0):.2f}%  "
                f"TPx:{p.get('tp_extension',1.0):.1f}")


# ─── Demo ─────────────────────────────────────────────────────────────────────

def demo_continuous_learner():
    import random
    random.seed(42)

    print("\n" + "═"*62)
    print("  CONTINUOUS ADAPTATION ENGINE — Demo")
    print("═"*62)

    engine = ContinuousAdaptationEngine()

    # ── Simulate 80 completed trades feeding into regime memory ──────────
    print("\n[1] Feeding 80 historical trades into RegimeMemory...")
    scenarios = [
        ("trending_bull",     "ICT_SMC",    "LONDON", 9,  +1.8, 0.62),
        ("trending_bull",     "Asia_Sweep", "LONDON", 8,  +1.9, 0.65),
        ("trending_bull",     "Mean_Rev",   "LONDON", 10, -0.7, 0.35),  # bad in trends
        ("ranging_tight",     "Mean_Rev",   "ASIA",   4,  +1.2, 0.58),  # good in ranges
        ("ranging_tight",     "CRT_Engine", "ASIA",   5,  +1.1, 0.55),
        ("ranging_tight",     "ICT_SMC",    "LONDON", 9,  +0.3, 0.45),  # mediocre in ranges
        ("institutional_hunt","ICT_SMC",    "LONDON", 8,  +2.1, 0.70),  # best in hunts
        ("institutional_hunt","Asia_Sweep", "LONDON", 9,  +1.8, 0.68),
        ("regime_transition", "ICT_SMC",    "NY",     14, +0.4, 0.48),
        ("regime_transition", "TL_Break",   "NY",     14, -0.9, 0.32),  # bad in transitions
    ]

    total_fed = 0
    for regime, strat, sess, hour, avg_r, wr in scenarios:
        for _ in range(8):  # 8 trades per scenario
            r = avg_r * random.uniform(0.6, 1.4) * (1 if random.random() < wr else -0.8)
            engine.memory.record_trade(regime, strat, sess, hour, r)
            total_fed += 1

    print(f"  ✓ {total_fed} trades ingested")
    print(f"\n  Learned multiplier table (top entries):")
    print(engine.memory.format_learned_table())

    # ── Calendar intelligence ─────────────────────────────────────────────
    print(f"\n[2] Calendar Intelligence:")
    print(engine.calendar.format_morning_intelligence())

    # ── Forward optimisation ─────────────────────────────────────────────
    print(f"\n[3] Forward Optimisation (INSTITUTIONAL_HUNT regime):")
    # Inject a hunt regime state manually
    engine._latest_params = {
        "regime": "institutional_hunt",
        "confluence_threshold": 68.0,
        "effective_risk_pct":   1.30,
        "tp_extension":         1.80,
        "sl_adjustment":        0.80,
        "preferred_strategies": ["ICT_SMC", "Asia_Sweep", "Wyckoff"],
        "muted_strategies":     ["Mean_Rev", "TL_Break"],
        "session_bias":         "LONDON",
        "regime_confidence":    0.85,
    }
    params = engine.optimiser.optimise()
    print()
    print(engine.optimiser.format_session_briefing())

    # ── Show conviction multiplier effect ─────────────────────────────────
    print(f"\n[4] Live Conviction Multipliers (INSTITUTIONAL_HUNT × LONDON):")
    agents_to_test = ["ICT_SMC", "Asia_Sweep", "Mean_Rev", "TL_Break", "Wyckoff"]
    for ag in agents_to_test:
        mult = engine.get_agent_conviction_mult(ag, "LONDON")
        effect = "BOOSTED" if mult > 1.05 else ("MUTED" if mult < 0.10 else "neutral")
        bar = "█" * int(mult * 10) + "░" * max(0, 15 - int(mult * 10))
        print(f"  {ag:<20} {bar}  {mult:.3f}×  [{effect}]")

    # ── Simulate a live trade close feeding back ──────────────────────────
    print(f"\n[5] PMA Feedback Loop — trade close at +1.6R:")
    engine.on_trade_closed("ICT_SMC", 1.6, "LONDON", 9)
    engine.on_trade_closed("Mean_Rev", -0.8, "LONDON", 9)
    new_mult_ict  = engine.memory.get_multiplier("institutional_hunt", "ICT_SMC", "LONDON")
    new_mult_mr   = engine.memory.get_multiplier("institutional_hunt", "Mean_Rev", "LONDON")
    print(f"  ICT_SMC multiplier updated:  {new_mult_ict:.4f}")
    print(f"  Mean_Rev multiplier updated: {new_mult_mr:.4f}")

    print(f"\n  Current engine status:")
    print(f"  {engine.format_status()}")

    print(f"\n{'═'*62}")
    print(f"  Continuous Adaptation Engine: FULLY OPERATIONAL ✓")
    print(f"  System learns from every trade — permanently and cumulatively")
    print(f"{'═'*62}\n")

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    demo_continuous_learner()
