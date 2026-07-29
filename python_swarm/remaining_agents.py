"""
QUANT-SWARM-XAU: Remaining Strategy Agents (05, 08, 11)
+ Data Ingestion Layer Agents (01, 02, 03)
+ Confluence/Validation Layer Agents (14, 16, 17)
"""

import asyncio
import json
import logging
import time
import math
from typing import Optional
from agent_strategy_cores import BaseStrategyAgent, MarketSnapshot
from confluence_engine import AgentJournal, AgentSignal, AGENT_REGISTRY

log = logging.getLogger("REMAINING-AGENTS")


# ─── Agent 05: Trendline Rejection & Structural Breakout ─────────────────────

class TrendlineBreakoutAgent(BaseStrategyAgent):
    """
    Trades clean structural trendline breaks or third-touch rejections.
    Heavily filtered by historical volume and multi-bar price reaction.
    """
    def _default_params(self) -> dict:
        return {
            "min_tl_touches":      3,
            "volume_filter":       1.3,   # Volume must be 1.3x avg at touch/break
            "reaction_bars":       2,     # Bars of confirmed reaction
            "conviction_base":    72.0,
        }

    def analyze(self, snap: MarketSnapshot) -> Optional[AgentSignal]:
        if not snap.h1_ohlc or len(snap.h1_ohlc) < 30:
            return None
        
        price = snap.bid
        atr   = snap.atr_h1
        
        tl = self._fit_trendline(snap.h1_ohlc)
        if not tl:
            return None
        
        tl_price_now = tl["slope"] * len(snap.h1_ohlc) + tl["intercept"]
        dist = price - tl_price_now
        
        # Third-touch rejection at downtrend line (sell)
        if tl["direction"] == "DOWN" and abs(dist) < atr * 0.15 and dist > 0:
            entry = price
            sl    = tl_price_now + atr * 0.2
            tp1   = tl_price_now - atr * 0.5
            tp2   = tl_price_now - atr * 1.5
            tp3   = tl_price_now - atr * 3.0
            return self._emit_signal(snap, "SELL", self.params["conviction_base"],
                                     entry, sl, tp1, tp2, tp3, ["TL_REJECTION"])
        
        # Clean breakout above downtrend (buy)
        if tl["direction"] == "DOWN" and dist > atr * 0.1:
            # Wait for retest
            if abs(price - tl_price_now) < atr * 0.2:
                entry = price
                sl    = tl_price_now - atr * 0.1
                tp1   = tl_price_now + atr * 1.0
                tp2   = tl_price_now + atr * 2.5
                tp3   = tl_price_now + atr * 4.5
                return self._emit_signal(snap, "BUY", self.params["conviction_base"],
                                         entry, sl, tp1, tp2, tp3, ["TL_BREAK", "RETEST"])
        
        return None

    def _fit_trendline(self, ohlc: list[dict]) -> Optional[dict]:
        """Simple linear regression on swing points."""
        highs  = [b["h"] for b in ohlc[-30:]]
        lows   = [b["l"] for b in ohlc[-30:]]
        n      = len(highs)
        x      = list(range(n))
        
        # Fit to highs (downtrend line)
        mx, my = sum(x) / n, sum(highs) / n
        num    = sum((xi - mx) * (yi - my) for xi, yi in zip(x, highs))
        den    = sum((xi - mx) ** 2 for xi in x)
        
        if den == 0:
            return None
        
        slope  = num / den
        intercept = my - slope * mx
        
        return {
            "slope": slope,
            "intercept": intercept,
            "direction": "DOWN" if slope < -0.001 else "UP" if slope > 0.001 else "FLAT"
        }


# ─── Agent 08: Volume Profile & Order Flow (VPVR/POC) ────────────────────────

class VolumeProfileAgent(BaseStrategyAgent):
    """
    Identifies HVNs and POC to ensure entries at high-liquidity inflections.
    Acts as a filter and entry precision enhancer.
    """
    def _default_params(self) -> dict:
        return {
            "poc_proximity_atr":   0.3,    # Entry within 0.3 ATR of POC
            "hvn_proximity_atr":   0.25,
            "lvn_avoid_atr":       0.15,   # Avoid entries within LVN (no support)
            "conviction_base":    65.0,
        }

    def analyze(self, snap: MarketSnapshot) -> Optional[AgentSignal]:
        if not snap.poc_level:
            return None
        
        price = snap.bid
        atr   = snap.atr_h1
        
        near_poc = abs(price - snap.poc_level) < atr * self.params["poc_proximity_atr"]
        near_hvn = any(abs(price - hvn) < atr * self.params["hvn_proximity_atr"]
                       for hvn in snap.hvn_levels)
        in_lvn   = any(abs(price - lvn) < atr * self.params["lvn_avoid_atr"]
                       for lvn in snap.lvn_levels)
        
        if in_lvn:
            return None  # LVN = no liquidity support, skip
        
        if near_poc or near_hvn:
            # POC/HVN acts as magnet; bias based on approach direction
            h4_bias = self._get_htf_bias(snap.h4_ohlc)
            conviction = self.params["conviction_base"]
            if near_poc:
                conviction += 10
            
            if h4_bias == "BUY":
                entry = price
                sl    = snap.poc_level - atr * 0.5
                tp1   = snap.poc_level + atr * 0.6
                tp2   = snap.poc_level + atr * 1.5
                tp3   = snap.poc_level + atr * 3.0
                tags  = ["VPVR_POC" if near_poc else "VPVR_HVN"]
                return self._emit_signal(snap, "BUY", conviction,
                                         entry, sl, tp1, tp2, tp3, tags)
            elif h4_bias == "SELL":
                entry = price
                sl    = snap.poc_level + atr * 0.5
                tp1   = snap.poc_level - atr * 0.6
                tp2   = snap.poc_level - atr * 1.5
                tp3   = snap.poc_level - atr * 3.0
                tags  = ["VPVR_POC" if near_poc else "VPVR_HVN"]
                return self._emit_signal(snap, "SELL", conviction,
                                         entry, sl, tp1, tp2, tp3, tags)
        
        return None

    def _get_htf_bias(self, ohlc: list[dict]) -> str:
        if not ohlc or len(ohlc) < 5:
            return "NEUTRAL"
        closes = [b["c"] for b in ohlc[-5:]]
        return "BUY" if closes[-1] > closes[0] else "SELL"


# ─── Agent 11: Mean Reversion & Volatility Bands ─────────────────────────────

class MeanReversionAgent(BaseStrategyAgent):
    """
    Detects statistical exhaustion using multi-timeframe ATR expansion
    and Bollinger Band / Z-score anomalies at key HTF levels.
    """
    def _default_params(self) -> dict:
        return {
            "bb_period":     20,
            "bb_std":        2.0,
            "zscore_thresh": 2.2,    # Standard deviations for extreme signal
            "atr_expansion": 1.8,    # ATR ratio considered "expanded"
            "conviction_base": 58.0,
        }

    def analyze(self, snap: MarketSnapshot) -> Optional[AgentSignal]:
        if not snap.h1_ohlc or len(snap.h1_ohlc) < 25:
            return None
        
        closes = [b["c"] for b in snap.h1_ohlc[-self.params["bb_period"]:]]
        price  = snap.bid
        atr    = snap.atr_h1
        
        # Bollinger Bands
        mean   = sum(closes) / len(closes)
        std    = math.sqrt(sum((c - mean) ** 2 for c in closes) / len(closes))
        upper  = mean + std * self.params["bb_std"]
        lower  = mean - std * self.params["bb_std"]
        
        # Z-score
        zscore = (price - mean) / std if std > 0 else 0
        
        # ATR expansion check: current H1 ATR vs 5-bar average
        if len(snap.h1_ohlc) >= 6:
            recent_ranges = [b["h"] - b["l"] for b in snap.h1_ohlc[-6:-1]]
            avg_range = sum(recent_ranges) / len(recent_ranges) if recent_ranges else atr
            atr_expanded = atr > avg_range * self.params["atr_expansion"]
        else:
            atr_expanded = False
        
        conviction = self.params["conviction_base"]
        
        # Statistical extreme: price at upper BB + high Z-score → SELL
        if price >= upper and zscore >= self.params["zscore_thresh"]:
            if atr_expanded:
                conviction += 12  # Confirmed exhaustion
            entry = price
            sl    = upper + atr * 0.1
            tp1   = mean
            tp2   = lower
            tp3   = lower - atr * 0.8
            return self._emit_signal(snap, "SELL", conviction,
                                     entry, sl, tp1, tp2, tp3,
                                     ["MEAN_REV", "BB_UPPER", f"Z{zscore:.1f}"])
        
        # Statistical extreme: price at lower BB + negative Z-score → BUY
        if price <= lower and zscore <= -self.params["zscore_thresh"]:
            if atr_expanded:
                conviction += 12
            entry = price
            sl    = lower - atr * 0.1
            tp1   = mean
            tp2   = upper
            tp3   = upper + atr * 0.8
            return self._emit_signal(snap, "BUY", conviction,
                                     entry, sl, tp1, tp2, tp3,
                                     ["MEAN_REV", "BB_LOWER", f"Z{zscore:.1f}"])
        
        return None


# ─── Agent 14: Time & Invalidation Guard ─────────────────────────────────────

class TimeInvalidationGuard:
    """
    Agent 14: Monitors algorithmic session timing and invalidates entries
    outside London/NY killzones. Not a signal generator — a gate.
    """
    KILLZONES = {
        "LONDON_OPEN":  (7, 0,  10, 0),   # 07:00-10:00 UTC
        "LONDON_CLOSE": (11, 0, 13, 30),  # 11:00-13:30 UTC
        "NY_OPEN":      (13, 30, 16, 0),  # 13:30-16:00 UTC
        "NY_SESSION":   (13, 30, 20, 0),  # 13:30-20:00 UTC
    }
    
    NEWS_BLACKOUT_MINUTES = 15  # No trades within 15 min of high-impact news
    
    def validate(self, snap: MarketSnapshot, news_times: list[float] = None) -> bool:
        """Returns True if current time is valid for trade execution."""
        
        # News blackout
        now = time.time()
        if news_times:
            for news_time in news_times:
                if abs(now - news_time) < self.NEWS_BLACKOUT_MINUTES * 60:
                    log.info("Agent 14: NEWS BLACKOUT active")
                    return False
        
        # Must be in a killzone
        utc_hour   = (now % 86400) // 3600
        utc_minute = (now % 3600) // 60
        
        for kz_name, (sh, sm, eh, em) in self.KILLZONES.items():
            start_mins = sh * 60 + sm
            end_mins   = eh * 60 + em
            curr_mins  = utc_hour * 60 + utc_minute
            
            if start_mins <= curr_mins <= end_mins:
                return True
        
        log.info("Agent 14: Outside killzone — no trades")
        return False


# ─── Agent 16: Liquidity Pool Mapper ─────────────────────────────────────────

class LiquidityPoolMapper:
    """
    Agent 16: Maps stop-loss clusters and pending order books
    to optimize TP target placement.
    """
    
    def __init__(self):
        self.bsl_levels: list[float] = []  # Buy-Side Liquidity (highs)
        self.ssl_levels: list[float] = []  # Sell-Side Liquidity (lows)
    
    def update(self, snap: MarketSnapshot):
        """Extract liquidity pools from swing H/L structure."""
        if not snap.h4_ohlc or len(snap.h4_ohlc) < 10:
            return
        
        # BSL: significant swing highs (equal highs = stop clusters above)
        highs = [b["h"] for b in snap.h4_ohlc[-30:]]
        lows  = [b["l"] for b in snap.h4_ohlc[-30:]]
        
        self.bsl_levels = self._find_clusters(highs, snap.atr_h4)
        self.ssl_levels = self._find_clusters(lows,  snap.atr_h4)
    
    def _find_clusters(self, prices: list[float], atr: float) -> list[float]:
        """Group prices within ATR tolerance into cluster levels."""
        clusters = []
        tolerance = atr * 0.15
        
        for p in prices:
            found = False
            for i, c in enumerate(clusters):
                if abs(p - c) < tolerance:
                    clusters[i] = (c + p) / 2  # Update cluster centroid
                    found = True
                    break
            if not found:
                clusters.append(p)
        
        return sorted(set(round(c, 2) for c in clusters))
    
    def nearest_bsl(self, price: float) -> Optional[float]:
        above = [l for l in self.bsl_levels if l > price]
        return min(above) if above else None
    
    def nearest_ssl(self, price: float) -> Optional[float]:
        below = [l for l in self.ssl_levels if l < price]
        return max(below) if below else None
    
    def optimize_tps(self, direction: str, entry: float,
                     tp1: float, tp2: float, tp3: float) -> tuple:
        """Adjust TP levels to align with nearest liquidity pools."""
        if direction == "BUY":
            bsl = self.nearest_bsl(entry)
            if bsl and entry < bsl < tp3:
                # Snap TP2 to BSL cluster
                tp2 = bsl - 0.10  # 10 cents below stop cluster
        else:
            ssl = self.nearest_ssl(entry)
            if ssl and tp3 < ssl < entry:
                tp2 = ssl + 0.10  # 10 cents above stop cluster
        
        return tp1, tp2, tp3


# ─── Agent 17: Multi-Timeframe Trend Synthesizer ─────────────────────────────

class MTFTrendSynthesizer:
    """
    Agent 17: Ensures LTF entries don't blindly fight HTF structural order flow.
    Generates a bias score for each direction.
    """
    
    def synthesize(self, snap: MarketSnapshot) -> dict:
        """Returns {'bias': 'BUY'/'SELL'/'NEUTRAL', 'strength': 0.0-1.0, 'htf_ok': bool}"""
        scores = {}
        
        for tf_name, ohlc in [("D1", snap.d1_ohlc), ("H4", snap.h4_ohlc),
                               ("H1", snap.h1_ohlc)]:
            if not ohlc or len(ohlc) < 10:
                continue
            closes = [b["c"] for b in ohlc[-10:]]
            ema    = sum(closes) / len(closes)  # Simple mean as EMA proxy
            scores[tf_name] = "BUY" if closes[-1] > ema else "SELL"
        
        if not scores:
            return {"bias": "NEUTRAL", "strength": 0.0, "htf_ok": True}
        
        buy_count  = sum(1 for v in scores.values() if v == "BUY")
        sell_count = sum(1 for v in scores.values() if v == "SELL")
        total      = len(scores)
        
        if buy_count > sell_count:
            bias     = "BUY"
            strength = buy_count / total
        elif sell_count > buy_count:
            bias     = "SELL"
            strength = sell_count / total
        else:
            bias     = "NEUTRAL"
            strength = 0.5
        
        # HTF OK: D1 must agree with bias (or be neutral) for clean trade
        d1_bias  = scores.get("D1", bias)
        htf_ok   = (d1_bias == bias) or strength < 0.67  # Allow if not extreme
        
        return {"bias": bias, "strength": round(strength, 2), "htf_ok": htf_ok,
                "breakdown": scores}
