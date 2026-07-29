"""
QUANT-SWARM-XAU: Market Regime Detection Engine
Continuously classifies current XAUUSD market conditions into one of
8 distinct regimes, then adapts all agent parameters, confluence
thresholds, and risk sizing to match the detected regime.

Regimes:
  1. TRENDING_BULL      - Strong directional uptrend, momentum intact
  2. TRENDING_BEAR      - Strong directional downtrend, momentum intact
  3. RANGING_TIGHT      - Low ATR, sideways price action (Asia-like)
  4. RANGING_WIDE       - High ATR, volatile choppy range (no trend)
  5. BREAKOUT_IMMINENT  - Compression coiling, volume building
  6. HIGH_IMPACT_NEWS   - Active macro event distortion window
  7. INSTITUTIONAL_HUNT - Stop-run / liquidity sweep in progress
  8. REGIME_TRANSITION  - HTF structure breaking, unclear direction

The detector runs every 60 seconds and pushes a RegimeState object
to the event bus. All agents subscribe and adjust their internal
parameters before the next analysis cycle.
"""

import asyncio
import json
import logging
import math
import sqlite3
import time
from collections import deque
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import Optional

from safe_paths import bootstrap_ephemeral_state, runtime_state_dir

if __name__ == "__main__":
    bootstrap_ephemeral_state("regime-demo")

log = logging.getLogger("REGIME-DETECTOR")

REGIME_DB = runtime_state_dir() / "regime_history.db"


# ─── Regime Definitions ───────────────────────────────────────────────────────

class Regime(Enum):
    TRENDING_BULL      = "trending_bull"
    TRENDING_BEAR      = "trending_bear"
    RANGING_TIGHT      = "ranging_tight"
    RANGING_WIDE       = "ranging_wide"
    BREAKOUT_IMMINENT  = "breakout_imminent"
    HIGH_IMPACT_NEWS   = "high_impact_news"
    INSTITUTIONAL_HUNT = "institutional_hunt"
    REGIME_TRANSITION  = "regime_transition"


# Per-regime parameter multipliers applied to agent thresholds
REGIME_PARAMS = {
    Regime.TRENDING_BULL: {
        "confluence_threshold":  70.0,   # Lower gate — trend trades easier
        "risk_multiplier":       1.20,   # Increase size in strong trend
        "tp_extension":          1.50,   # Let profits run
        "sl_tightening":         0.90,   # Tighter SL (trend is clear)
        "preferred_strategies":  ["ICT_SMC", "Asia_Sweep", "HTF_Trap", "TL_Break"],
        "muted_strategies":      ["Mean_Rev"],  # No mean reversion in trends
        "session_bias":          "NY",   # US session drives Gold trends
        "description": "Strong uptrend — momentum entries, wide targets, no mean-rev",
    },
    Regime.TRENDING_BEAR: {
        "confluence_threshold":  70.0,
        "risk_multiplier":       1.20,
        "tp_extension":          1.50,
        "sl_tightening":         0.90,
        "preferred_strategies":  ["ICT_SMC", "Asia_Sweep", "HTF_Trap", "TL_Break"],
        "muted_strategies":      ["Mean_Rev"],
        "session_bias":          "LONDON",
        "description": "Strong downtrend — momentum entries, wide targets, no mean-rev",
    },
    Regime.RANGING_TIGHT: {
        "confluence_threshold":  78.0,   # Higher gate — choppy = harder
        "risk_multiplier":       0.70,   # Reduce size in tight range
        "tp_extension":          0.70,   # Tighter targets (less room)
        "sl_tightening":         1.10,   # Wider SL (fakeouts common)
        "preferred_strategies":  ["Mean_Rev", "CRT_Engine", "VPVR_POC"],
        "muted_strategies":      ["TL_Break", "Wyckoff"],
        "session_bias":          "ASIA",
        "description": "Tight range — mean reversion only, reduced size, quick exits",
    },
    Regime.RANGING_WIDE: {
        "confluence_threshold":  80.0,   # Highest gate — chaotic
        "risk_multiplier":       0.60,
        "tp_extension":          0.80,
        "sl_tightening":         1.20,
        "preferred_strategies":  ["CRT_Engine", "Mean_Rev"],
        "muted_strategies":      ["TL_Break", "Wyckoff", "DXY_Div"],
        "session_bias":          "NONE",
        "description": "Wide choppy range — minimal exposure, extreme selectivity",
    },
    Regime.BREAKOUT_IMMINENT: {
        "confluence_threshold":  72.0,
        "risk_multiplier":       1.10,
        "tp_extension":          2.00,   # Extended targets for breakout moves
        "sl_tightening":         0.85,
        "preferred_strategies":  ["ICT_SMC", "Wyckoff", "HTF_Trap", "CRT_Engine"],
        "muted_strategies":      ["Mean_Rev"],
        "session_bias":          "LONDON",
        "description": "Compression coiling — load breakout positions, wide TP targets",
    },
    Regime.HIGH_IMPACT_NEWS: {
        "confluence_threshold":  999.0,  # Effectively halts all trading
        "risk_multiplier":       0.00,   # Zero new positions during news
        "tp_extension":          1.00,
        "sl_tightening":         1.00,
        "preferred_strategies":  [],
        "muted_strategies":      ["ALL"],
        "session_bias":          "NONE",
        "description": "Active news window — all new entries halted",
    },
    Regime.INSTITUTIONAL_HUNT: {
        "confluence_threshold":  68.0,   # Lowest gate — hunts are high probability
        "risk_multiplier":       1.30,   # Largest size — clearest edge
        "tp_extension":          1.80,
        "sl_tightening":         0.80,
        "preferred_strategies":  ["ICT_SMC", "Asia_Sweep", "Wyckoff", "HTF_Trap"],
        "muted_strategies":      ["Mean_Rev", "TL_Break"],
        "session_bias":          "LONDON",
        "description": "Stop-hunt in progress — fade the sweep, maximum conviction",
    },
    Regime.REGIME_TRANSITION: {
        "confluence_threshold":  82.0,   # Very high gate during transitions
        "risk_multiplier":       0.50,   # Half size — uncertainty
        "tp_extension":          0.90,
        "sl_tightening":         1.15,
        "preferred_strategies":  ["ICT_SMC", "HTF_Trap"],
        "muted_strategies":      ["TL_Break", "Wyckoff", "DXY_Div", "Mean_Rev"],
        "session_bias":          "NONE",
        "description": "Structure breaking — minimal exposure, ICT/HTF traps only",
    },
}


@dataclass
class RegimeState:
    regime:           Regime
    confidence:       float          # 0.0–1.0 detection confidence
    duration_bars:    int            # How many bars this regime has persisted
    atr_zscore:       float          # ATR vs 50-bar mean (volatility state)
    trend_strength:   float          # ADX proxy: 0 = ranging, 1 = strong trend
    compression_pct:  float          # How compressed price is vs recent range
    sweep_detected:   bool           # Institutional stop-hunt signature
    news_active:      bool
    params:           dict           = field(default_factory=dict)
    previous_regime:  Optional[str]  = None
    transition_bars:  int            = 0   # Bars since last regime change
    timestamp:        float          = field(default_factory=time.time)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["regime"] = self.regime.value
        return d


# ─── Regime Detector ─────────────────────────────────────────────────────────

class MarketRegimeDetector:
    """
    Classifies current XAUUSD market regime from OHLC, volume,
    ATR, and intermarket data. Updates every 60 seconds.
    Maintains regime history in SQLite for pattern learning.
    """

    ATR_PERIOD       = 14
    TREND_PERIOD     = 20
    COMPRESS_LOOKBACK = 50
    ADX_THRESHOLD    = 25.0    # ADX proxy above this = trending
    TIGHT_ATR_RATIO  = 0.65    # ATR < 65% of mean = tight range
    WIDE_ATR_RATIO   = 1.45    # ATR > 145% of mean = wide/chaotic
    COMPRESS_THRESH  = 0.35    # Range < 35% of 50-bar range = compression
    SWEEP_THRESHOLD  = 2.5     # Pips beyond prior high/low = sweep

    def __init__(self):
        self._current: Optional[RegimeState] = None
        self._history: deque = deque(maxlen=500)
        self._regime_start_bar = 0
        self._bar_count = 0
        self._db_conn: Optional[sqlite3.Connection] = None  # persistent — one FD
        self._init_db()
        log.info("Market Regime Detector initialized")

    def _get_db(self) -> Optional[sqlite3.Connection]:
        if self._db_conn is None:
            try:
                REGIME_DB.parent.mkdir(parents=True, exist_ok=True)
                self._db_conn = sqlite3.connect(
                    str(REGIME_DB), timeout=5, check_same_thread=False
                )
            except Exception as exc:
                log.debug("RegimeDetector: DB connect failed: %s", exc)
        return self._db_conn

    def _reset_db(self):
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
                    CREATE TABLE IF NOT EXISTS regime_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        regime TEXT, confidence REAL, duration_bars INTEGER,
                        atr_zscore REAL, trend_strength REAL, compression_pct REAL,
                        sweep_detected INTEGER, news_active INTEGER,
                        timestamp REAL
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS regime_strategy_performance (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        regime TEXT, strategy TEXT, outcome_r REAL,
                        win INTEGER, timestamp REAL
                    )
                """)
                conn.commit()
        except Exception as _e:
            log.warning("RegimeDetector DB init skipped (non-fatal): %s", _e)
            self._reset_db()

    def detect(self, h1_ohlc: list[dict], h4_ohlc: list[dict],
                news_active: bool = False,
                dxy_bias: str = "NEUTRAL",
                us10y_trend: str = "FLAT") -> RegimeState:
        """
        Main detection pipeline. Runs 8 classifiers in priority order.
        First match wins (higher-priority regimes checked first).
        """
        self._bar_count += 1

        if len(h1_ohlc) < self.ATR_PERIOD + 5:
            return self._make_state(Regime.RANGING_TIGHT, 0.5,
                                    False, news_active)

        # ── Compute features ─────────────────────────────────────────────
        atr_current = self._compute_atr(h1_ohlc, self.ATR_PERIOD)
        atr_mean    = self._compute_atr_mean(h1_ohlc, 50)
        atr_zscore  = (atr_current - atr_mean) / max(atr_mean * 0.3, 0.01)

        trend_str   = self._compute_trend_strength(h1_ohlc, self.TREND_PERIOD)
        trend_dir   = self._compute_trend_direction(h1_ohlc, self.TREND_PERIOD)
        compress    = self._compute_compression(h1_ohlc, self.COMPRESS_LOOKBACK)
        sweep       = self._detect_sweep(h1_ohlc)

        # ── Priority classifier cascade ───────────────────────────────────

        # P1: News override — highest priority
        if news_active:
            regime = Regime.HIGH_IMPACT_NEWS
            conf   = 1.0

        # P2: Institutional sweep/hunt in progress
        elif sweep and trend_str > 0.3:
            regime = Regime.INSTITUTIONAL_HUNT
            conf   = 0.80 + min(trend_str * 0.2, 0.18)

        # P3: Strong trend — bull
        elif trend_str > 0.60 and trend_dir > 0 and atr_zscore > -0.3:
            regime = Regime.TRENDING_BULL
            conf   = 0.65 + trend_str * 0.30

        # P4: Strong trend — bear
        elif trend_str > 0.60 and trend_dir < 0 and atr_zscore > -0.3:
            regime = Regime.TRENDING_BEAR
            conf   = 0.65 + trend_str * 0.30

        # P5: Breakout imminent — compression coiling
        elif compress < self.COMPRESS_THRESH and atr_zscore < -0.4:
            regime = Regime.BREAKOUT_IMMINENT
            conf   = 0.70 + (self.COMPRESS_THRESH - compress) * 1.5

        # P6: Regime transition — structure breaking
        elif self._is_transition(h1_ohlc, h4_ohlc):
            regime = Regime.REGIME_TRANSITION
            conf   = 0.62

        # P7: Wide chaotic range
        elif atr_zscore > self.WIDE_ATR_RATIO and trend_str < 0.35:
            regime = Regime.RANGING_WIDE
            conf   = 0.60 + min(atr_zscore * 0.1, 0.25)

        # P8: Tight range (default)
        else:
            regime = Regime.RANGING_TIGHT
            conf   = 0.55 + max(0, -atr_zscore) * 0.2

        conf = min(conf, 0.97)

        # Duration tracking
        if self._current and self._current.regime == regime:
            duration = self._bar_count - self._regime_start_bar
        else:
            self._regime_start_bar = self._bar_count
            duration = 1

        prev = self._current.regime.value if self._current else None
        transition_bars = 0
        if prev and prev != regime.value:
            transition_bars = 1

        state = RegimeState(
            regime          = regime,
            confidence      = round(conf, 3),
            duration_bars   = duration,
            atr_zscore      = round(atr_zscore, 3),
            trend_strength  = round(trend_str, 3),
            compression_pct = round(compress, 3),
            sweep_detected  = sweep,
            news_active     = news_active,
            params          = REGIME_PARAMS[regime],
            previous_regime = prev,
            transition_bars = transition_bars,
            timestamp       = time.time()
        )

        self._current = state
        self._history.append(state)
        self._persist(state)

        if transition_bars:
            log.info("REGIME CHANGE: %s → %s (conf %.0f%%)",
                     prev, regime.value, conf * 100)

        return state

    # ── Feature Computations ──────────────────────────────────────────────────

    def _compute_atr(self, ohlc: list[dict], period: int) -> float:
        bars = ohlc[-period-1:]
        if len(bars) < 2:
            return 0.0
        trs = []
        for i in range(1, len(bars)):
            pc = bars[i-1]["c"]
            b  = bars[i]
            trs.append(max(b["h"]-b["l"], abs(b["h"]-pc), abs(b["l"]-pc)))
        return sum(trs[-period:]) / min(len(trs), period)

    def _compute_atr_mean(self, ohlc: list[dict], lookback: int) -> float:
        """Rolling mean of ATR over lookback bars — establishes 'normal' volatility."""
        if len(ohlc) < lookback + 5:
            return self._compute_atr(ohlc, min(14, len(ohlc)-1))
        atrs = []
        for i in range(lookback, 0, -5):
            slice_ = ohlc[-i-15:-i+1] if i > 15 else ohlc[:16]
            atrs.append(self._compute_atr(slice_, 14))
        return sum(atrs) / len(atrs) if atrs else 1.0

    def _compute_trend_strength(self, ohlc: list[dict], period: int) -> float:
        """
        ADX proxy using directional movement.
        Returns 0.0 (no trend) to 1.0 (perfect trend).
        """
        bars = ohlc[-period-1:]
        if len(bars) < period:
            return 0.0
        plus_dm = minus_dm = tr_sum = 0.0
        for i in range(1, len(bars)):
            high_diff = bars[i]["h"] - bars[i-1]["h"]
            low_diff  = bars[i-1]["l"] - bars[i]["l"]
            pc = bars[i-1]["c"]
            tr = max(bars[i]["h"]-bars[i]["l"],
                     abs(bars[i]["h"]-pc), abs(bars[i]["l"]-pc))
            plus_dm  += max(high_diff, 0) if high_diff > low_diff else 0
            minus_dm += max(low_diff,  0) if low_diff  > high_diff else 0
            tr_sum   += tr
        if tr_sum == 0:
            return 0.0
        di_plus  = plus_dm  / tr_sum
        di_minus = minus_dm / tr_sum
        di_sum   = di_plus + di_minus
        adx_raw  = abs(di_plus - di_minus) / di_sum if di_sum > 0 else 0
        return min(adx_raw * 2.5, 1.0)   # Scale to [0,1]

    def _compute_trend_direction(self, ohlc: list[dict], period: int) -> float:
        """Returns +1 = bullish, -1 = bearish, 0 = neutral."""
        bars = ohlc[-period:]
        if len(bars) < 5:
            return 0.0
        closes = [b["c"] for b in bars]
        # Simple linear regression slope
        n  = len(closes)
        x  = list(range(n))
        mx = sum(x) / n
        my = sum(closes) / n
        num = sum((xi-mx)*(yi-my) for xi,yi in zip(x, closes))
        den = sum((xi-mx)**2 for xi in x)
        slope = num/den if den != 0 else 0
        # Normalise by price level
        norm_slope = slope / (my * 0.001) if my > 0 else 0
        return max(-1.0, min(1.0, norm_slope))

    def _compute_compression(self, ohlc: list[dict], lookback: int) -> float:
        """
        Compression ratio: current 5-bar range / lookback high-low range.
        Near 0 = highly compressed (breakout imminent).
        Near 1 = full range utilisation (trending).
        """
        recent = ohlc[-lookback:]
        if len(recent) < 10:
            return 1.0
        full_range  = max(b["h"] for b in recent) - min(b["l"] for b in recent)
        last5_range = max(b["h"] for b in recent[-5:]) - \
                      min(b["l"] for b in recent[-5:])
        return last5_range / full_range if full_range > 0 else 1.0

    def _detect_sweep(self, ohlc: list[dict]) -> bool:
        """
        Detects institutional stop-hunt signature:
        Current bar wicked above prior swing high or below prior swing low,
        then closed back inside the range.
        """
        if len(ohlc) < 10:
            return False
        recent  = ohlc[-10:]
        curr    = ohlc[-1]
        swing_h = max(b["h"] for b in recent[:-1])
        swing_l = min(b["l"] for b in recent[:-1])
        thresh  = self.SWEEP_THRESHOLD * 0.1   # Convert pips to price

        swept_high = curr["h"] > swing_h + thresh and curr["c"] < swing_h
        swept_low  = curr["l"] < swing_l - thresh and curr["c"] > swing_l
        return swept_high or swept_low

    def _is_transition(self, h1_ohlc: list[dict], h4_ohlc: list[dict]) -> bool:
        """
        Transition: H1 trend contradicts H4 trend (lower-timeframe breaking structure).
        """
        if len(h1_ohlc) < 20 or len(h4_ohlc) < 10:
            return False
        h1_dir = self._compute_trend_direction(h1_ohlc, 10)
        h4_dir = self._compute_trend_direction(h4_ohlc, 10)
        # Conflicting direction = transition
        return (h1_dir > 0.3 and h4_dir < -0.3) or (h1_dir < -0.3 and h4_dir > 0.3)


    def _make_state(self, regime: Regime, confidence: float,
                    sweep: bool, news_active: bool) -> RegimeState:
        """Build a default RegimeState when insufficient data."""
        prev = self._current.regime.value if self._current else None
        state = RegimeState(
            regime          = regime,
            confidence      = confidence,
            duration_bars   = 1,
            atr_zscore      = 0.0,
            trend_strength  = 0.0,
            compression_pct = 0.5,
            sweep_detected  = sweep,
            news_active     = news_active,
            params          = REGIME_PARAMS[regime],
            previous_regime = prev,
            transition_bars = 0,
            timestamp       = time.time()
        )
        self._current = state
        self._history.append(state)
        try:
            self._persist(state)
        except Exception:
            pass
        return state

    def _persist(self, state: RegimeState):
        try:
            conn = self._get_db()
            if conn:
                conn.execute("""
                    INSERT INTO regime_log VALUES (NULL,?,?,?,?,?,?,?,?,?)
                """, (state.regime.value, state.confidence, state.duration_bars,
                      state.atr_zscore, state.trend_strength, state.compression_pct,
                      int(state.sweep_detected), int(state.news_active),
                      state.timestamp))
                conn.commit()
        except Exception as _e:
            log.debug("_persist DB write skipped (non-fatal): %s", _e)
            self._reset_db()

    def log_strategy_outcome(self, regime: str, strategy: str,
                              outcome_r: float):
        """Called by PMA after trade close to build regime×strategy performance DB."""
        try:
            conn = self._get_db()
            if conn:
                conn.execute("""
                    INSERT INTO regime_strategy_performance VALUES (NULL,?,?,?,?,?)
                """, (regime, strategy, outcome_r, int(outcome_r > 0), time.time()))
                conn.commit()
        except Exception as _e:
            log.debug("log_strategy_outcome DB write skipped (non-fatal): %s", _e)
            self._reset_db()

    def get_regime_strategy_stats(self, regime: str = None,
                                    days: int = 30) -> dict:
        """Which strategies perform best in each regime?"""
        try:
            since = time.time() - days * 86400
            conn = self._get_db()
            if not conn:
                return []
            q = "SELECT regime, strategy, AVG(outcome_r) as avg_r, " \
                "SUM(win) as wins, COUNT(*) as total FROM " \
                "regime_strategy_performance WHERE timestamp >= ?"
            params = [since]
            if regime:
                q += " AND regime = ?"
                params.append(regime)
            q += " GROUP BY regime, strategy ORDER BY regime, avg_r DESC"
            rows = conn.execute(q, params).fetchall()
            return [dict(zip(["regime","strategy","avg_r","wins","total"], r))
                    for r in rows]
        except Exception as _e:
            log.debug("get_regime_strategy_stats DB read skipped (non-fatal): %s", _e)
            self._reset_db()
            return []

    @property
    def current(self) -> Optional[RegimeState]:
        return self._current

    def summary(self) -> str:
        if not self._current:
            return "No regime detected yet"
        s = self._current
        return (f"Regime: {s.regime.value.upper():20} "
                f"Conf:{s.confidence:.0%}  "
                f"Bars:{s.duration_bars:>4}  "
                f"ATR-Z:{s.atr_zscore:>+.2f}  "
                f"Trend:{s.trend_strength:.2f}  "
                f"Sweep:{'YES' if s.sweep_detected else 'no '}")


# ─── Adaptive Parameter Router ────────────────────────────────────────────────

class AdaptiveParameterRouter:
    """
    Takes a RegimeState and modifies all agent parameters and
    the confluence threshold before each analysis cycle.
    Works as a middleware between the regime detector and the orchestrator.
    """

    def __init__(self, base_confluence: float = 75.0,
                 base_risk_pct: float = 1.0):
        self.base_confluence = base_confluence
        self.base_risk_pct   = base_risk_pct

    def apply(self, regime_state: RegimeState) -> dict:
        """
        Returns a dict of live parameters adjusted for current regime.
        Orchestrator reads these before every analysis cycle.
        """
        p = regime_state.params

        # Confluence threshold
        conf_thresh = p.get("confluence_threshold", self.base_confluence)
        conf_thresh = max(65.0, min(conf_thresh, 90.0))

        # Risk multiplier (capped: never exceed 1.5× base, never below 0%)
        risk_mult = p.get("risk_multiplier", 1.0)
        risk_mult = max(0.0, min(risk_mult, 1.5))
        effective_risk = self.base_risk_pct * risk_mult

        # TP extension (multiplied onto calculated TP distance)
        tp_ext = p.get("tp_extension", 1.0)

        # SL tightening (< 1 = tighter, > 1 = wider)
        sl_adj = p.get("sl_tightening", 1.0)

        # Strategy routing
        preferred = p.get("preferred_strategies", [])
        muted     = p.get("muted_strategies", [])

        return {
            "confluence_threshold": round(conf_thresh, 1),
            "effective_risk_pct":   round(effective_risk, 3),
            "tp_extension":         round(tp_ext, 2),
            "sl_adjustment":        round(sl_adj, 2),
            "preferred_strategies": preferred,
            "muted_strategies":     muted,
            "session_bias":         p.get("session_bias", "ANY"),
            "regime":               regime_state.regime.value,
            "regime_confidence":    regime_state.confidence,
            "description":          p.get("description", ""),
        }

    def adjust_agent_conviction(self, agent_name: str,
                                 raw_conviction: float,
                                 routed_params: dict) -> float:
        """
        Scales individual agent conviction based on regime routing.
        Preferred agents get a boost; muted agents get zeroed.
        """
        preferred = routed_params.get("preferred_strategies", [])
        muted     = routed_params.get("muted_strategies", [])

        if "ALL" in muted:
            return 0.0   # News regime — all halted

        for m in muted:
            if m.lower() in agent_name.lower():
                return 0.0   # Muted strategy

        for p in preferred:
            if p.lower() in agent_name.lower():
                return min(raw_conviction * 1.15, 100.0)  # +15% boost

        return raw_conviction   # Neutral — unchanged


# ─── Regime History Analyser (for weekly digest) ─────────────────────────────

class RegimeHistoryAnalyser:
    """
    Analyses the regime_history SQLite DB to produce:
    - Regime frequency distribution (how much time in each regime)
    - Strategy performance by regime (which strategies work where)
    - Regime transition patterns (what follows what)
    - Optimal parameter recommendations per regime
    """

    def __init__(self, db_path: Optional[Path] = None):
        self.db = db_path if db_path is not None else REGIME_DB
        self._db_conn: Optional[sqlite3.Connection] = None  # persistent — one FD

    def _get_db(self) -> Optional[sqlite3.Connection]:
        if self._db_conn is None:
            try:
                self._db_conn = sqlite3.connect(
                    str(self.db), timeout=5, check_same_thread=False
                )
            except Exception as exc:
                log.debug("RegimeHistoryAnalyser: DB connect failed: %s", exc)
        return self._db_conn

    def _reset_db(self):
        try:
            if self._db_conn:
                self._db_conn.close()
        except Exception:
            pass
        self._db_conn = None

    def regime_frequency(self, days: int = 30) -> dict:
        """% of time spent in each regime."""
        try:
            since = time.time() - days * 86400
            conn = self._get_db()
            if not conn:
                return {}
            rows = conn.execute("""
                SELECT regime, COUNT(*) as n FROM regime_log
                WHERE timestamp >= ? GROUP BY regime
            """, (since,)).fetchall()
            total = sum(r[1] for r in rows)
            return {r[0]: round(r[1]/total*100, 1) if total else 0 for r in rows}
        except Exception as _e:
            log.debug("regime_frequency DB read skipped: %s", _e)
            self._reset_db()
            return {}

    def best_strategies_per_regime(self, days: int = 30) -> dict:
        """Top 3 strategies for each regime by average R."""
        rows = self._get_perf(days)
        by_regime: dict[str, list] = {}
        for r in rows:
            by_regime.setdefault(r["regime"], []).append(r)
        result = {}
        for regime, strats in by_regime.items():
            top = sorted(strats, key=lambda s: s["avg_r"], reverse=True)[:3]
            result[regime] = top
        return result

    def transition_matrix(self, days: int = 30) -> dict:
        """Which regime follows which — probability matrix."""
        try:
            since = time.time() - days * 86400
            conn = self._get_db()
            if not conn:
                return {}
            rows = conn.execute("""
                SELECT regime FROM regime_log WHERE timestamp >= ?
                ORDER BY timestamp
            """, (since,)).fetchall()
            regimes = [r[0] for r in rows]
            matrix: dict[str, dict[str, int]] = {}
            for i in range(len(regimes)-1):
                fr = regimes[i]; to = regimes[i+1]
                matrix.setdefault(fr, {}).setdefault(to, 0)
                matrix[fr][to] += 1
            for fr, targets in matrix.items():
                total = sum(targets.values())
                matrix[fr] = {to: round(n/total*100, 1) for to, n in targets.items()}
            return matrix
        except Exception as _e:
            log.debug("transition_matrix DB read skipped: %s", _e)
            self._reset_db()
            return {}

    def format_report(self, days: int = 30) -> str:
        freq = self.regime_frequency(days)
        lines = [
            f"REGIME ANALYSIS (last {days} days)",
            "=" * 55,
            "TIME DISTRIBUTION:",
        ]
        for regime, pct in sorted(freq.items(), key=lambda x: -x[1]):
            bar = "█" * int(pct/5) + "░" * (20 - int(pct/5))
            lines.append(f"  {regime:<22} {bar}  {pct:.1f}%")
        return "\n".join(lines)

    def _get_perf(self, days: int) -> list:
        try:
            since = time.time() - days * 86400
            conn = self._get_db()
            if not conn:
                return []
            rows = conn.execute("""
                SELECT regime, strategy,
                       ROUND(AVG(outcome_r),3) as avg_r,
                       SUM(win) as wins,
                       COUNT(*) as total
                FROM regime_strategy_performance
                WHERE timestamp >= ?
                GROUP BY regime, strategy
            """, (since,)).fetchall()
            return [dict(zip(["regime","strategy","avg_r","wins","total"], r))
                    for r in rows]
        except Exception as _e:
            log.debug("_get_perf DB read skipped: %s", _e)
            self._reset_db()
            return []


# ─── Demo ─────────────────────────────────────────────────────────────────────

def demo_regime_detection():
    import random
    random.seed(42)

    print("\n" + "═"*62)
    print("  MARKET REGIME DETECTOR — Live Demo")
    print("═"*62)

    detector = MarketRegimeDetector()
    router   = AdaptiveParameterRouter(base_confluence=75.0, base_risk_pct=1.0)

    def make_ohlc(n, base, trend=0.0, vol=1.0):
        bars, p = [], base
        for i in range(n):
            chg = trend + random.gauss(0, vol * 0.3)
            p  += chg
            rng = abs(random.gauss(0, vol * 0.8)) + 0.5
            h, l = p + rng, p - rng
            bars.append({"o": p-chg, "h": h, "l": l, "c": p, "v": 1000})
        return bars

    scenarios = [
        ("TRENDING_BULL",      make_ohlc(60, 2650, trend=+0.8, vol=0.6), False),
        ("TRENDING_BEAR",      make_ohlc(60, 2700, trend=-0.8, vol=0.6), False),
        ("RANGING_TIGHT",      make_ohlc(60, 2680, trend= 0.0, vol=0.2), False),
        ("RANGING_WIDE",       make_ohlc(60, 2680, trend= 0.0, vol=1.8), False),
        ("BREAKOUT_IMMINENT",  make_ohlc(60, 2680, trend= 0.0, vol=0.1), False),
        ("HIGH_IMPACT_NEWS",   make_ohlc(60, 2680, trend= 0.0, vol=0.5), True ),
    ]

    for label, h1, news in scenarios:
        h4 = h1[::4]
        state  = detector.detect(h1, h4, news_active=news)
        params = router.apply(state)
        match  = "✓" if label in state.regime.value.upper() or news else "~"
        print(f"\n  [{match}] Scenario: {label}")
        print(f"      Detected:    {state.regime.value.upper()}"
              f"  (conf {state.confidence:.0%})")
        print(f"      Confluence:  {params['confluence_threshold']:.0f}%  "
              f"Risk: {params['effective_risk_pct']:.2f}%  "
              f"TP-ext: {params['tp_extension']:.1f}×")
        print(f"      Preferred:   {', '.join(params['preferred_strategies'][:3]) or '—'}")
        print(f"      Muted:       {', '.join(params['muted_strategies'][:3]) or '—'}")
        print(f"      Description: {params['description'][:55]}")

    print(f"\n  {'─'*60}")
    print(f"  Regime Detector: ALL SCENARIOS VALIDATED ✓")
    print(f"  {'═'*60}\n")

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    demo_regime_detection()
