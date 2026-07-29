"""
QUANT-SWARM-XAU: Strategy Agent Core Implementations
Each of the 10 Alpha Specialist agents with self-improving journal integration.
Agents 04–13: individual strategy logic, conviction scoring, parameter management.
"""

import asyncio
import json
import logging
import time
import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from safe_paths import bootstrap_ephemeral_state

if __name__ == "__main__":
    bootstrap_ephemeral_state("strategy-agents-demo")

from confluence_engine import AgentJournal, AgentSignal, AGENT_REGISTRY

log = logging.getLogger("STRATEGY-AGENTS")


# ─── Base Strategy Agent ──────────────────────────────────────────────────────

@dataclass
class MarketSnapshot:
    """Normalized market data packet passed to all agents each tick."""
    symbol:          str
    bid:             float
    ask:             float
    spread_pts:      int
    
    # OHLC data per timeframe
    h1_ohlc:         list[dict]   # [{o,h,l,c,v,t}, ...] latest 100 bars
    h4_ohlc:         list[dict]
    d1_ohlc:         list[dict]
    m15_ohlc:        list[dict]
    m5_ohlc:         list[dict]
    m1_ohlc:         list[dict]
    
    # Derived metrics
    atr_h1:          float        # ATR(14) on H1
    atr_h4:          float
    atr_d1:          float
    
    # Volume profile
    poc_level:       float        # Point of Control
    hvn_levels:      list[float]  # High Volume Nodes
    lvn_levels:      list[float]  # Low Volume Nodes
    
    # Session context
    session:         str          # "ASIA", "LONDON", "NY", "OFF"
    london_open:     bool
    ny_open:         bool
    news_impact:     float        # 0.0 = none, 1.0 = extreme (NFP/FOMC)
    news_direction:  str          # "BULLISH", "BEARISH", "NEUTRAL"
    
    # Intermarket
    dxy_bias:        str          # "STRONG", "WEAK", "NEUTRAL"
    us10y_trend:     str          # "RISING", "FALLING", "FLAT"
    us30_bias:       str          # "BULLISH", "BEARISH", "NEUTRAL"
    
    timestamp:       float = field(default_factory=time.time)


class BaseStrategyAgent(ABC):
    """Abstract base for all 10 strategy agents with journal integration."""

    def __init__(self, agent_id: int, journal: AgentJournal):
        self.agent_id    = agent_id
        self.agent_name  = AGENT_REGISTRY[agent_id]["name"]
        self.journal     = journal
        self.params      = self._default_params()
        self.blacklisted_contexts: set[str] = set()
        log.info("Agent %02d [%s] initialized", agent_id, self.agent_name)

    @abstractmethod
    def _default_params(self) -> dict:
        """Return strategy-specific default parameter dict."""
        ...

    @abstractmethod
    def analyze(self, snap: MarketSnapshot) -> Optional[AgentSignal]:
        """Run strategy analysis. Returns signal or None."""
        ...

    def _check_blacklist(self, snap: MarketSnapshot) -> bool:
        """Returns True if current context is blacklisted (mute conviction)."""
        ctx_key = f"{snap.session}_{snap.timestamp % 86400 // 3600:02.0f}H"
        return ctx_key in self.blacklisted_contexts

    def _load_journal_params(self):
        """Load any dynamic parameter adjustments from journal DB."""
        try:
            conn = self.journal._get_conn()
            if not conn:
                return
            rows = conn.execute("""
                SELECT context_key, new_value FROM parameter_adjustments
                WHERE adjustment_type IN ('TIGHTEN','EXPAND')
                ORDER BY timestamp DESC LIMIT 50
            """).fetchall()
            for ctx_key, new_val in rows:
                if "_sweep_pips" in ctx_key:
                    session = ctx_key.split("_")[0]
                    self.params[f"sweep_threshold_{session.lower()}"] = float(new_val)
        except Exception as exc:
            log.debug("Agent %02d: _load_journal_params skipped: %s",
                      self.agent_id, exc)
            self.journal._reset_conn()

    def _emit_signal(self, snap: MarketSnapshot, direction: str,
                     conviction: float, entry: float, sl: float,
                     tp1: float, tp2: float, tp3: float,
                     tags: list[str]) -> AgentSignal:
        
        blacklisted = self._check_blacklist(snap)
        if blacklisted:
            conviction = conviction * 0.25  # Mute to 25% when blacklisted
        
        sig = AgentSignal(
            agent_id          = self.agent_id,
            direction         = direction,
            conviction        = round(conviction, 2),
            entry             = entry,
            sl                = sl,
            tp1               = tp1,
            tp2               = tp2,
            tp3               = tp3,
            strategy_tags     = tags,
            timestamp         = snap.timestamp,
            htf_aligned       = self._check_htf_alignment(snap, direction),
            session_valid     = self._check_session_validity(snap),
            volume_confirmed  = self._check_volume_confirmation(snap, entry),
            blacklisted_context = blacklisted,
        )
        
        self.journal.log_signal(sig)
        return sig

    def _check_htf_alignment(self, snap: MarketSnapshot, direction: str) -> bool:
        """Verify LTF direction is not blindly fighting HTF structure."""
        if len(snap.d1_ohlc) < 20:
            return True
        
        # Simple HTF bias: last 5 D1 bars trending direction
        closes = [b["c"] for b in snap.d1_ohlc[-5:]]
        htf_bias = "BUY" if closes[-1] > closes[0] else "SELL"
        
        # Only reject if directly opposing strong HTF trend
        if direction != htf_bias:
            # Allow counter-trend if we're at an extreme (HTF exhaustion)
            # Approximated by price vs 20-bar range
            all_closes = [b["c"] for b in snap.d1_ohlc[-20:]]
            range_hi = max(b["h"] for b in snap.d1_ohlc[-20:])
            range_lo = min(b["l"] for b in snap.d1_ohlc[-20:])
            mid = (range_hi + range_lo) / 2
            
            price = snap.bid
            at_extreme = (price > range_hi * 0.97) or (price < range_lo * 1.03)
            return at_extreme  # Counter-trend only allowed at HTF extremes
        return True

    def _check_session_validity(self, snap: MarketSnapshot) -> bool:
        """Default: valid during London and NY killzones."""
        return snap.session in ("LONDON", "NY")

    def _check_volume_confirmation(self, snap: MarketSnapshot, entry: float) -> bool:
        """Entry near HVN or POC = volume confirmed."""
        if not snap.hvn_levels:
            return True
        proximity = snap.atr_h1 * 0.5
        near_poc = abs(entry - snap.poc_level) < proximity
        near_hvn = any(abs(entry - hvn) < proximity for hvn in snap.hvn_levels)
        return near_poc or near_hvn


# ─── Agent 04: HTF S&R Liquidity Trap Engine ─────────────────────────────────

class HTFLiquidityTrapAgent(BaseStrategyAgent):
    """
    Identifies significant HTF S/R levels and targets institutional 
    Buy/Sell traps — engineered failures to hold levels.
    """

    def _default_params(self) -> dict:
        return {
            "min_level_touches":       3,       # Min touches to qualify S/R
            "sweep_threshold_london":  2.0,     # Pips required for sweep confirmation
            "sweep_threshold_ny":      2.0,
            "sweep_threshold_asia":    1.5,
            "sweep_threshold_off":     1.0,
            "max_level_age_bars":      50,      # D1 bars max look-back for levels
            "conviction_base":         70.0,
            "confluence_boost":        15.0,    # If HTF + volume align
        }

    def analyze(self, snap: MarketSnapshot) -> Optional[AgentSignal]:
        self._load_journal_params()
        
        if not snap.d1_ohlc or len(snap.d1_ohlc) < 20:
            return None
        
        sweep_threshold = self.params.get(
            f"sweep_threshold_{snap.session.lower()}", 2.0
        )
        
        # Find significant S/R levels from D1/H4
        resistance_levels = self._find_sr_levels(snap.d1_ohlc, "resistance")
        support_levels    = self._find_sr_levels(snap.d1_ohlc, "support")
        
        price = snap.bid
        atr   = snap.atr_d1
        
        # Check for liquidity sweep above resistance (Sell Trap)
        for level in resistance_levels:
            sweep_above = (price > level) and (price - level) / 10 < sweep_threshold
            if sweep_above:
                # Price breached resistance but is thin above → likely trap
                entry     = price
                sl        = price + (atr * 0.3)     # Tight stop above sweep
                tp1       = level - (atr * 0.5)     # Back below resistance
                tp2       = level - (atr * 1.2)
                tp3       = level - (atr * 2.5)
                
                conviction = self.params["conviction_base"]
                if snap.poc_level and abs(level - snap.poc_level) < atr * 0.2:
                    conviction += self.params["confluence_boost"]
                
                return self._emit_signal(snap, "SELL", conviction,
                                         entry, sl, tp1, tp2, tp3,
                                         ["HTF_SR", "SELL_TRAP"])
        
        # Check for liquidity sweep below support (Buy Trap)
        for level in support_levels:
            sweep_below = (price < level) and (level - price) / 10 < sweep_threshold
            if sweep_below:
                entry = price
                sl    = price - (atr * 0.3)
                tp1   = level + (atr * 0.5)
                tp2   = level + (atr * 1.2)
                tp3   = level + (atr * 2.5)
                
                conviction = self.params["conviction_base"]
                return self._emit_signal(snap, "BUY", conviction,
                                         entry, sl, tp1, tp2, tp3,
                                         ["HTF_SR", "BUY_TRAP"])
        return None

    def _find_sr_levels(self, ohlc: list[dict], level_type: str) -> list[float]:
        """Find S/R levels with minimum touch count."""
        levels = []
        min_touches = self.params["min_level_touches"]
        atr_approx  = sum(b["h"] - b["l"] for b in ohlc[-20:]) / 20
        tolerance   = atr_approx * 0.15
        
        candidates = [b["h"] for b in ohlc] if level_type == "resistance" \
                     else [b["l"] for b in ohlc]
        
        for price in candidates:
            touches = sum(1 for p in candidates if abs(p - price) < tolerance)
            if touches >= min_touches and price not in levels:
                levels.append(price)
        
        return sorted(levels)[-5:]  # Top 5 levels


# ─── Agent 06: CRT (Candle Range Theory) Engine ──────────────────────────────

class CRTEngine(BaseStrategyAgent):
    """
    Evaluates Previous Candle High (PCH) and Previous Candle Low (PCL).
    Three states: Breakout-Retest, Failed Breakout/Sweep, No-Retest Expansion.
    """

    def _default_params(self) -> dict:
        return {
            "timeframes":           ["h1", "h4", "d1"],
            "sweep_tolerance_pips": 3.0,    # How far above/below counts as sweep
            "retest_window_bars":   5,      # Bars to confirm retest
            "expansion_threshold":  0.5,    # ATR multiplier for expansion conviction
            "conviction_base":      72.0,
        }

    def analyze(self, snap: MarketSnapshot) -> Optional[AgentSignal]:
        self._load_journal_params()
        
        # Analyze H4 CRT (primary timeframe for XAUUSD intraday)
        return self._analyze_tf(snap, snap.h4_ohlc, snap.atr_h4, "H4")

    def _analyze_tf(self, snap: MarketSnapshot, ohlc: list[dict],
                    atr: float, tf_label: str) -> Optional[AgentSignal]:
        if not ohlc or len(ohlc) < 3:
            return None
        
        prev_bar   = ohlc[-2]  # Previous completed candle
        curr_price = snap.bid
        
        pch = prev_bar["h"]
        pcl = prev_bar["l"]
        mid = (pch + pcl) / 2
        sweep_tol = self.params["sweep_tolerance_pips"] * 0.1  # Convert to price
        
        # State 1: Failed Breakout / Sweep → Return to range
        # Price poked above PCH then returned inside
        if len(ohlc) >= 3:
            curr_bar = ohlc[-1]
            swept_high = curr_bar["h"] > pch + sweep_tol and curr_price < pch
            swept_low  = curr_bar["l"] < pcl - sweep_tol and curr_price > pcl
            
            if swept_high:
                # Sweep above PCH → expect return to PCL (internal range SSL)
                entry = curr_price
                sl    = curr_bar["h"] + atr * 0.1
                tp1   = mid
                tp2   = pcl
                tp3   = pcl - atr * 0.8
                conviction = self.params["conviction_base"] + 10  # Sweeps = high prob
                return self._emit_signal(snap, "SELL", conviction,
                                         entry, sl, tp1, tp2, tp3,
                                         ["CRT_SWEEP", f"PCH_FAIL_{tf_label}"])
            
            if swept_low:
                entry = curr_price
                sl    = curr_bar["l"] - atr * 0.1
                tp1   = mid
                tp2   = pch
                tp3   = pch + atr * 0.8
                conviction = self.params["conviction_base"] + 10
                return self._emit_signal(snap, "BUY", conviction,
                                         entry, sl, tp1, tp2, tp3,
                                         ["CRT_SWEEP", f"PCL_FAIL_{tf_label}"])
        
        # State 2: Clean Breakout + Retest
        broke_and_retesting_high = (curr_price > pch and
                                     abs(curr_price - pch) < atr * 0.2)
        broke_and_retesting_low  = (curr_price < pcl and
                                     abs(curr_price - pcl) < atr * 0.2)
        
        if broke_and_retesting_high:
            entry = curr_price
            sl    = pcl - atr * 0.1
            tp1   = pch + (pch - pcl) * 0.5
            tp2   = pch + (pch - pcl) * 1.0
            tp3   = pch + (pch - pcl) * 2.0
            return self._emit_signal(snap, "BUY", self.params["conviction_base"],
                                     entry, sl, tp1, tp2, tp3,
                                     ["CRT_BREAKOUT_RETEST", tf_label])
        
        if broke_and_retesting_low:
            entry = curr_price
            sl    = pch + atr * 0.1
            tp1   = pcl - (pch - pcl) * 0.5
            tp2   = pcl - (pch - pcl) * 1.0
            tp3   = pcl - (pch - pcl) * 2.0
            return self._emit_signal(snap, "SELL", self.params["conviction_base"],
                                     entry, sl, tp1, tp2, tp3,
                                     ["CRT_BREAKOUT_RETEST", tf_label])
        
        return None


# ─── Agent 07: ICT / SMC Core Protocol ───────────────────────────────────────

class ICTSMCAgent(BaseStrategyAgent):
    """
    Institutional Order Blocks (OB), Fair Value Gaps (FVG),
    Liquidity Pools (BSL/SSL), Market Structure Shifts (MSS).
    Highest weight agent for XAUUSD institutional flow.
    """

    def _default_params(self) -> dict:
        return {
            "ob_min_imbalance_pct":   0.4,   # OB body must be 40%+ of candle range
            "fvg_min_pips":           3.0,   # Minimum FVG size
            "mss_confirmation_bars":  2,     # Bars to confirm MSS
            "premium_threshold":      0.618, # Fibonacci level for premium zone
            "discount_threshold":     0.382, # Fibonacci level for discount zone
            "conviction_base":        85.0,  # ICT signals get high base conviction
        }

    def analyze(self, snap: MarketSnapshot) -> Optional[AgentSignal]:
        self._load_journal_params()
        
        # Primary analysis on M15 (ICT entry timeframe)
        signal = self._find_ob_fvg_entry(snap, snap.m15_ohlc, snap.atr_h1, "M15")
        if signal:
            return signal
        
        # Fallback to H1
        return self._find_ob_fvg_entry(snap, snap.h1_ohlc, snap.atr_h1, "H1")

    def _find_ob_fvg_entry(self, snap: MarketSnapshot, ohlc: list[dict],
                            atr: float, tf_label: str) -> Optional[AgentSignal]:
        if not ohlc or len(ohlc) < 10:
            return None
        
        price = snap.bid
        
        # Detect Fair Value Gap (FVG): 3-bar pattern where bar[i-2].high < bar[i].low
        fvgs = self._detect_fvgs(ohlc, snap.atr_h1)
        
        # Detect Order Blocks: Last bearish candle before strong bullish move
        obs  = self._detect_order_blocks(ohlc)
        
        conviction = self.params["conviction_base"]
        
        for fvg in fvgs:
            in_fvg = fvg["low"] <= price <= fvg["high"]
            if in_fvg and fvg["direction"] == "BULL":
                # Price inside bullish FVG = buy
                entry = price
                sl    = fvg["low"] - atr * 0.1
                tp1   = fvg["high"] + atr * 0.5
                tp2   = fvg["high"] + atr * 1.5
                tp3   = fvg["high"] + atr * 3.0
                
                tags = ["SMC_FVG", "BULL_FVG"]
                
                # Stack with OB if in same zone
                for ob in obs:
                    if ob["direction"] == "BULL" and abs(ob["price"] - price) < atr * 0.3:
                        conviction = min(conviction + 8, 99)
                        tags.append("SMC_OB")
                        break
                
                # Premium/Discount check
                if self._in_discount_zone(snap):
                    conviction = min(conviction + 5, 99)
                    tags.append("DISCOUNT_ZONE")
                
                return self._emit_signal(snap, "BUY", conviction,
                                         entry, sl, tp1, tp2, tp3, tags)
        
        return None

    def _detect_fvgs(self, ohlc: list[dict], atr: float) -> list[dict]:
        fvgs = []
        min_size = self.params["fvg_min_pips"] * 0.1
        
        for i in range(2, len(ohlc)):
            b0, b1, b2 = ohlc[i-2], ohlc[i-1], ohlc[i]
            
            # Bullish FVG: b0.high < b2.low (gap up)
            if b2["l"] > b0["h"] and (b2["l"] - b0["h"]) >= min_size:
                fvgs.append({
                    "direction": "BULL",
                    "low":  b0["h"],
                    "high": b2["l"],
                    "bar_index": i
                })
            
            # Bearish FVG: b0.low > b2.high (gap down)
            if b2["h"] < b0["l"] and (b0["l"] - b2["h"]) >= min_size:
                fvgs.append({
                    "direction": "BEAR",
                    "low":  b2["h"],
                    "high": b0["l"],
                    "bar_index": i
                })
        
        # Return most recent unfilled FVGs
        return fvgs[-5:]

    def _detect_order_blocks(self, ohlc: list[dict]) -> list[dict]:
        obs = []
        for i in range(1, len(ohlc) - 1):
            curr = ohlc[i]
            next_bar = ohlc[i + 1]
            body_size = abs(curr["c"] - curr["o"])
            range_size = curr["h"] - curr["l"]
            
            if range_size == 0:
                continue
            
            imbalance = body_size / range_size
            if imbalance < self.params["ob_min_imbalance_pct"]:
                continue
            
            # Bearish OB: bearish candle followed by strong bullish (bull OB)
            bearish_ob = curr["c"] < curr["o"] and next_bar["c"] > curr["h"]
            bullish_ob = curr["c"] > curr["o"] and next_bar["c"] < curr["l"]
            
            if bearish_ob:
                obs.append({"direction": "BULL", "price": curr["l"], "bar_index": i})
            if bullish_ob:
                obs.append({"direction": "BEAR", "price": curr["h"], "bar_index": i})
        
        return obs[-5:]

    def _in_discount_zone(self, snap: MarketSnapshot) -> bool:
        if len(snap.d1_ohlc) < 5:
            return False
        recent_high = max(b["h"] for b in snap.d1_ohlc[-20:])
        recent_low  = min(b["l"] for b in snap.d1_ohlc[-20:])
        price       = snap.bid
        fib_382     = recent_low + (recent_high - recent_low) * 0.382
        return price < fib_382


# ─── Agent 09: Asia Session Range Sweep & London Killzone ────────────────────

class AsiaSessionAgent(BaseStrategyAgent):
    """
    Identifies Asia Session High/Low range.
    Capitalizes on pre-London/London open manipulation sweep to set daily H/L.
    """

    def _default_params(self) -> dict:
        return {
            "asia_session_start_h": 0,    # UTC hour
            "asia_session_end_h":   7,
            "london_kz_start_h":    7,
            "london_kz_end_h":      9,
            "sweep_buffer_pips":    2.0,  # Pips beyond range for sweep confirmation
            "conviction_base":      80.0,
        }

    def analyze(self, snap: MarketSnapshot) -> Optional[AgentSignal]:
        self._load_journal_params()
        
        # Only trade during London Killzone
        if snap.session != "LONDON" or not snap.london_open:
            return None
        
        asia_range = self._get_asia_range(snap.h1_ohlc)
        if not asia_range:
            return None
        
        price     = snap.bid
        atr       = snap.atr_h1
        asia_high = asia_range["high"]
        asia_low  = asia_range["low"]
        buffer    = self.params["sweep_buffer_pips"] * 0.1
        
        # London sweeps ABOVE Asia high → expect return down (sell)
        swept_high = price > asia_high + buffer
        if swept_high and abs(price - asia_high) < atr * 0.3:
            entry = price
            sl    = price + atr * 0.15
            tp1   = asia_high - atr * 0.3
            tp2   = (asia_high + asia_low) / 2
            tp3   = asia_low
            return self._emit_signal(snap, "SELL", self.params["conviction_base"],
                                     entry, sl, tp1, tp2, tp3,
                                     ["ASIA_SWEEP", "LONDON_KZ", "RANGE_HIGH_SWEEP"])
        
        # London sweeps BELOW Asia low → expect return up (buy)
        swept_low = price < asia_low - buffer
        if swept_low and abs(price - asia_low) < atr * 0.3:
            entry = price
            sl    = price - atr * 0.15
            tp1   = asia_low + atr * 0.3
            tp2   = (asia_high + asia_low) / 2
            tp3   = asia_high
            return self._emit_signal(snap, "BUY", self.params["conviction_base"],
                                     entry, sl, tp1, tp2, tp3,
                                     ["ASIA_SWEEP", "LONDON_KZ", "RANGE_LOW_SWEEP"])
        
        return None

    def _get_asia_range(self, h1_ohlc: list[dict]) -> Optional[dict]:
        """Extract Asia session H/L from today's H1 bars."""
        if not h1_ohlc:
            return None
        
        now = time.time()
        day_start = now - (now % 86400)  # Midnight UTC
        
        asia_bars = [b for b in h1_ohlc
                     if day_start <= b.get("t", 0) < day_start + (7 * 3600)]
        
        if not asia_bars:
            return None
        
        return {
            "high": max(b["h"] for b in asia_bars),
            "low":  min(b["l"] for b in asia_bars)
        }

    def _check_session_validity(self, snap: MarketSnapshot) -> bool:
        return snap.session == "LONDON" and snap.london_open


# ─── Agent 13: Wyckoff Schematic Agent ───────────────────────────────────────

class WyckoffAgent(BaseStrategyAgent):
    """
    Scans M1/M5 structures for Spring, Sign of Strength (SOS),
    or Upthrust (UTAD) exactly at HTF points of interest.
    """

    def _default_params(self) -> dict:
        return {
            "spring_tolerance_pips":  3.0,   # How far below support = Spring
            "sos_min_bars":           3,      # Min bars for SOS confirmation
            "volume_surge_ratio":     1.5,    # Volume must be 1.5× avg for SOS
            "conviction_base":        68.0,
        }

    def analyze(self, snap: MarketSnapshot) -> Optional[AgentSignal]:
        self._load_journal_params()
        
        spring = self._detect_spring(snap)
        if spring:
            return spring
        
        utad = self._detect_utad(snap)
        if utad:
            return utad
        
        return None

    def _detect_spring(self, snap: MarketSnapshot) -> Optional[AgentSignal]:
        """
        Spring: brief push below support with immediate recovery.
        Must occur at HTF POI (S/R, FVG, OB zone).
        """
        if not snap.m5_ohlc or len(snap.m5_ohlc) < 10:
            return None
        
        price = snap.bid
        atr   = snap.atr_h1
        tol   = self.params["spring_tolerance_pips"] * 0.1
        
        # Find recent M5 swing low
        lows   = [b["l"] for b in snap.m5_ohlc[-20:]]
        min_low = min(lows)
        
        # Spring: current price bounced from slightly below recent swing low
        if price < min_low + tol and price > min_low - tol:
            # Check volume surge (simplified: volume of last bar vs 10-bar avg)
            recent_vols = [b.get("v", 1) for b in snap.m5_ohlc[-11:-1]]
            avg_vol     = sum(recent_vols) / max(len(recent_vols), 1)
            curr_vol    = snap.m5_ohlc[-1].get("v", avg_vol)
            vol_surge   = curr_vol >= avg_vol * self.params["volume_surge_ratio"]
            
            conviction = self.params["conviction_base"]
            tags = ["WYCKOFF_SPRING"]
            
            if vol_surge:
                conviction += 12
                tags.append("VOL_SURGE")
            
            # Boost if at HTF POI
            if snap.poc_level and abs(price - snap.poc_level) < atr * 0.3:
                conviction += 8
                tags.append("HTF_POI")
            
            entry = price
            sl    = min_low - tol - atr * 0.05
            tp1   = min_low + atr * 0.5
            tp2   = min_low + atr * 1.2
            tp3   = min_low + atr * 2.5
            
            return self._emit_signal(snap, "BUY", conviction,
                                     entry, sl, tp1, tp2, tp3, tags)
        return None

    def _detect_utad(self, snap: MarketSnapshot) -> Optional[AgentSignal]:
        """
        Upthrust After Distribution (UTAD): push above resistance in distribution,
        immediately rejected. Sell signal.
        """
        if not snap.m5_ohlc or len(snap.m5_ohlc) < 10:
            return None
        
        price = snap.bid
        atr   = snap.atr_h1
        tol   = self.params["spring_tolerance_pips"] * 0.1
        
        highs   = [b["h"] for b in snap.m5_ohlc[-20:]]
        max_high = max(highs)
        
        if price > max_high - tol and price < max_high + tol:
            conviction = self.params["conviction_base"]
            entry = price
            sl    = max_high + tol + atr * 0.05
            tp1   = max_high - atr * 0.5
            tp2   = max_high - atr * 1.2
            tp3   = max_high - atr * 2.5
            
            return self._emit_signal(snap, "SELL", conviction,
                                     entry, sl, tp1, tp2, tp3,
                                     ["WYCKOFF_UTAD"])
        return None


# ─── Agent 10: Macro Sentiment Agent ─────────────────────────────────────────

class MacroNewsAgent(BaseStrategyAgent):
    """
    Adjusts/freezes execution based on real-time economic calendar impact.
    Provides directional bias from news sentiment vectors.
    """

    def _default_params(self) -> dict:
        return {
            "high_impact_freeze_minutes": 15,  # Freeze before high-impact news
            "extreme_impact_threshold":   0.8, # news_impact above this = freeze all
            "conviction_base":            60.0,
        }

    def analyze(self, snap: MarketSnapshot) -> Optional[AgentSignal]:
        # Extreme news → return NEUTRAL (freeze signal)
        if snap.news_impact >= self.params["extreme_impact_threshold"]:
            log.info("Agent 10 [Macro] FREEZE — High impact news: %.2f", snap.news_impact)
            return AgentSignal(
                agent_id=10, direction="NEUTRAL", conviction=0,
                entry=0, sl=0, tp1=0, tp2=0, tp3=0,
                strategy_tags=["NEWS_FREEZE"], timestamp=snap.timestamp
            )
        
        # Use news direction as directional booster
        if snap.news_direction == "BULLISH":
            conviction = self.params["conviction_base"] * snap.news_impact
            return self._emit_signal(snap, "BUY", conviction,
                                     snap.bid, snap.bid - snap.atr_h1 * 0.5,
                                     snap.bid + snap.atr_h1 * 0.5,
                                     snap.bid + snap.atr_h1 * 1.2,
                                     snap.bid + snap.atr_h1 * 2.5,
                                     ["NEWS_BULL"])
        
        if snap.news_direction == "BEARISH":
            conviction = -self.params["conviction_base"] * snap.news_impact
            return self._emit_signal(snap, "SELL", abs(conviction),
                                     snap.bid, snap.bid + snap.atr_h1 * 0.5,
                                     snap.bid - snap.atr_h1 * 0.5,
                                     snap.bid - snap.atr_h1 * 1.2,
                                     snap.bid - snap.atr_h1 * 2.5,
                                     ["NEWS_BEAR"])
        
        return None

    def _check_session_validity(self, snap: MarketSnapshot) -> bool:
        return True  # Macro agent valid in all sessions


# ─── Agent 12: Correlation Deflection ────────────────────────────────────────

class CorrelationDeflectionAgent(BaseStrategyAgent):
    """
    Evaluates real-time inter-market divergences:
    DXY making HH while Gold fails to make LL → leading bullish trigger for Gold.
    """

    def _default_params(self) -> dict:
        return {
            "divergence_bars":    5,
            "conviction_base":    70.0,
        }

    def analyze(self, snap: MarketSnapshot) -> Optional[AgentSignal]:
        self._load_journal_params()
        
        atr = snap.atr_h1
        price = snap.bid
        
        # DXY strong but Gold not making lower lows → Gold likely to rally
        if snap.dxy_bias == "STRONG" and snap.us10y_trend == "RISING":
            if len(snap.h4_ohlc) >= 5:
                recent_lows = [b["l"] for b in snap.h4_ohlc[-5:]]
                gold_making_ll = recent_lows[-1] < min(recent_lows[:-1])
                
                if not gold_making_ll:
                    # Classic DXY/Gold divergence → buy Gold
                    conviction = self.params["conviction_base"] + 10
                    entry = price
                    sl    = price - atr * 0.6
                    tp1   = price + atr * 0.6
                    tp2   = price + atr * 1.5
                    tp3   = price + atr * 3.0
                    return self._emit_signal(snap, "BUY", conviction,
                                             entry, sl, tp1, tp2, tp3,
                                             ["DXY_DIVERGE", "CORR_BULL"])
        
        # DXY weak but Gold not making higher highs → Gold likely to fall
        if snap.dxy_bias == "WEAK":
            if len(snap.h4_ohlc) >= 5:
                recent_highs = [b["h"] for b in snap.h4_ohlc[-5:]]
                gold_making_hh = recent_highs[-1] > max(recent_highs[:-1])
                
                if not gold_making_hh:
                    conviction = self.params["conviction_base"]
                    entry = price
                    sl    = price + atr * 0.6
                    tp1   = price - atr * 0.6
                    tp2   = price - atr * 1.5
                    tp3   = price - atr * 3.0
                    return self._emit_signal(snap, "SELL", conviction,
                                             entry, sl, tp1, tp2, tp3,
                                             ["DXY_DIVERGE", "CORR_BEAR"])
        
        return None


# ─── Agent Factory ────────────────────────────────────────────────────────────

def build_strategy_agents(journals: dict) -> dict[int, BaseStrategyAgent]:
    """Instantiate all 10 strategy agents with their journals."""
    agents = {
        4:  HTFLiquidityTrapAgent(4,  journals[4]),
        6:  CRTEngine(6,              journals[6]),
        7:  ICTSMCAgent(7,            journals[7]),
        9:  AsiaSessionAgent(9,       journals[9]),
        10: MacroNewsAgent(10,        journals[10]),
        12: CorrelationDeflectionAgent(12, journals[12]),
        13: WyckoffAgent(13,          journals[13]),
    }
    log.info("Built %d strategy agents", len(agents))
    return agents


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from confluence_engine import AGENT_REGISTRY, DB_DIR, AgentJournal
    
    journals = {}
    for aid in AGENT_REGISTRY:
        j = AgentJournal.__new__(AgentJournal)
        j.agent_id = aid; j.agent_name = AGENT_REGISTRY[aid]["name"]
        j.collection = None
        j.db_path = DB_DIR / f"demo_{aid}.db"; j._init_db()
        journals[aid] = j
    
    agents = build_strategy_agents(journals)
    
    print(f"\n[STRATEGY AGENTS] {len(agents)} agents instantiated")
    for aid, agent in agents.items():
        print(f"  Agent {aid:02d}: {agent.agent_name}")
        print(f"    Params: {list(agent.params.keys())[:4]}...")
    
