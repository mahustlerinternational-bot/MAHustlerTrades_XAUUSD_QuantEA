"""
QUANT-SWARM-XAU: Strategy Backtesting Harness
Walk-forward backtester that replays historical OHLC data through the
full 10-agent swarm pipeline and produces institutional-grade performance reports.

Features:
  - Walk-forward validation with configurable IS/OOS windows
  - Per-strategy and aggregate equity curves
  - Realistic slippage simulation (spread + market impact)
  - Session-filtered execution (London/NY killzones only)
  - Monte Carlo permutation test for strategy significance
  - Full trade log with all telemetry fields
"""

import asyncio
import csv
import json
import logging
import math
import os
import random
import sqlite3
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger("BACKTESTER")

BACKTEST_DIR = Path(
    os.environ.get(
        "QSX_BACKTEST_DIR",
        str(Path(__file__).resolve().parent.parent / "backtest_results"),
    )
).expanduser().resolve(strict=False)


def _lot_precision(step: float) -> int:
    step_text = f"{step:.8f}".rstrip("0").rstrip(".")
    return len(step_text.split(".")[1]) if "." in step_text else 0


# ─── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class BacktestConfig:
    symbol:              str   = "XAUUSD"
    initial_balance:     float = 10_000.0
    risk_pct:            float = 1.0        # % risk per trade
    max_daily_dd_pct:    float = 3.0
    spread_pts:          int   = 3          # Fixed spread in points
    slippage_pts:        int   = 5          # Max random slippage
    commission_per_lot:  float = 7.0        # $ commission per round-turn lot
    min_confluence:      float = 75.0
    session_filter:      bool  = True       # Only trade London/NY
    min_lot:             float = 0.01
    lot_step:            float = 0.01
    max_lot:             float = 5.0
    skip_oversized_min_lot: bool = True      # Small-account guard
    precision_scalp:     bool  = False       # $100-$300 standard-account mode
    precision_tf_label:  str   = "M5"
    precision_tp_r:      float = 1.35
    precision_min_sl:    float = 0.18
    precision_max_sl:    float = 0.80
    precision_max_hold_bars: int = 8
    precision_be_trigger_r: float = 0.35
    precision_trail_start_r: float = 0.65
    precision_trail_gap_r: float = 0.30
    precision_min_lock_r: float = 0.18
    precision_max_cost_r: float = 0.35
    cooldown_bars:       int   = 0
    max_trades_per_day:  int   = 0
    is_window_days:      int   = 90         # In-sample window
    oos_window_days:     int   = 30         # Out-of-sample window
    n_monte_carlo:       int   = 1000       # Permutation test iterations

@dataclass
class BacktestTrade:
    trade_id:     str
    agent_id:     int
    strategy:     str
    direction:    str
    entry_time:   float
    exit_time:    float
    entry_price:  float
    exit_price:   float
    sl_price:     float
    tp1_price:    float
    lot_size:     float
    gross_pnl:    float     # Before commission
    commission:   float
    net_pnl:      float     # After commission
    outcome_r:    float
    slippage_pts: float
    session:      str
    tags:         list[str]
    confluence:   float

@dataclass
class StrategyStats:
    name:             str
    trades:           int   = 0
    wins:             int   = 0
    losses:           int   = 0
    gross_profit:     float = 0.0
    gross_loss:       float = 0.0
    max_drawdown:     float = 0.0
    max_drawdown_pct: float = 0.0
    peak_equity:      float = 0.0
    net_r:            float = 0.0
    equity_curve:     list  = field(default_factory=list)

    @property
    def win_rate(self) -> float:
        return self.wins / self.trades if self.trades > 0 else 0.0

    @property
    def profit_factor(self) -> float:
        return self.gross_profit / self.gross_loss if self.gross_loss > 0 else 0.0

    @property
    def avg_r(self) -> float:
        return self.net_r / self.trades if self.trades > 0 else 0.0

    @property
    def sharpe(self) -> float:
        if len(self.equity_curve) < 2:
            return 0.0
        returns = [self.equity_curve[i] - self.equity_curve[i-1]
                   for i in range(1, len(self.equity_curve))]
        if not returns:
            return 0.0
        mean_r = sum(returns) / len(returns)
        std_r  = math.sqrt(sum((r - mean_r)**2 for r in returns) / len(returns))
        return (mean_r / std_r * math.sqrt(252)) if std_r > 0 else 0.0

    @property
    def calmar(self) -> float:
        annual_return = (self.equity_curve[-1] / self.equity_curve[0] - 1) * 100 \
            if self.equity_curve and self.equity_curve[0] > 0 else 0.0
        return annual_return / self.max_drawdown_pct if self.max_drawdown_pct > 0 else 0.0


# ─── Historical Data Generator / Loader ──────────────────────────────────────

class HistoricalDataProvider:
    """
    Generates or loads OHLC data for backtesting.
    In production: loads from CSV/MT5 export / broker history API.
    Here: provides a realistic GBM + mean-reversion synthetic series.
    """

    @staticmethod
    def generate_synthetic(
        n_bars:    int   = 5000,
        tf_mins:   int   = 60,       # Timeframe in minutes
        start_price: float = 2600.0,
        annual_vol:  float = 0.15,   # 15% annualized volatility
        trend_drift: float = 0.05,   # 5% annual drift (Gold long-term uptrend)
        start_ts: Optional[float] = None,
    ) -> list[dict]:
        """
        Geometric Brownian Motion with mean-reversion component.
        Mimics XAUUSD intraday volatility structure.
        """
        bars = []
        price = start_price
        bars_per_year = (252 * 6.5 * 60) / tf_mins  # ~trading hours per year
        dt     = 1.0 / bars_per_year
        sigma  = annual_vol * math.sqrt(dt)
        mu     = (trend_drift - 0.5 * annual_vol**2) * dt

        # Mean reversion parameters
        mean_rev_speed  = 0.02
        long_term_price = start_price * 1.02

        # Session vol multipliers (XAUUSD is most volatile in London/NY)
        vol_multipliers = [0.4, 0.5, 0.6, 0.7, 0.8,  # Asia (0-5 UTC)
                           0.6, 0.9, 1.3, 1.4, 1.2,  # London open (6-10)
                           1.0, 0.9, 0.8, 1.5, 1.6,  # NY open (12-14)
                           1.4, 1.2, 1.0, 0.8, 0.7,  # NY afternoon
                           0.5, 0.5, 0.4, 0.4]        # Off-hours

        start_ts = (
            time.time() - n_bars * tf_mins * 60
            if start_ts is None else start_ts
        )

        for i in range(n_bars):
            bar_ts    = start_ts + i * tf_mins * 60
            bar_hour  = int((bar_ts % 86400) / 3600) % 24
            vol_mult  = vol_multipliers[bar_hour]

            # GBM step with mean reversion
            shock      = random.gauss(0, 1)
            mr_force   = mean_rev_speed * (long_term_price - price) / price
            log_return = mu + mr_force + sigma * vol_mult * shock
            price      = price * math.exp(log_return)

            # Intrabar range (ATR-proportional)
            intrabar_range = price * annual_vol * math.sqrt(dt) * vol_mult * 4
            bar_open  = price
            bar_high  = price + abs(random.gauss(0, intrabar_range * 0.6))
            bar_low   = price - abs(random.gauss(0, intrabar_range * 0.6))
            bar_close = price + random.gauss(0, intrabar_range * 0.3)

            bar_close = max(bar_low, min(bar_high, bar_close))
            volume    = int(abs(random.gauss(5000, 2000)) * vol_mult)

            bars.append({
                "o": round(bar_open, 2), "h": round(bar_high, 2),
                "l": round(bar_low, 2),  "c": round(bar_close, 2),
                "v": volume, "t": bar_ts
            })
            price = bar_close

        log.info("Generated %d synthetic H1 bars (XAUUSD GBM+MR, σ=%.0f%%)",
                 n_bars, annual_vol * 100)
        return bars

    @staticmethod
    def load_csv(filepath: str) -> list[dict]:
        """Load MT5-exported CSV: Date,Time,Open,High,Low,Close,Volume"""
        bars = []
        with open(filepath, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    dt_str = f"{row.get('Date','2024-01-01')} {row.get('Time','00:00')}"
                    ts = datetime.strptime(dt_str, "%Y.%m.%d %H:%M").timestamp()
                    bars.append({
                        "o": float(row["Open"]),  "h": float(row["High"]),
                        "l": float(row["Low"]),   "c": float(row["Close"]),
                        "v": int(float(row.get("Volume", 1000))),
                        "t": ts
                    })
                except (ValueError, KeyError):
                    continue
        log.info("Loaded %d bars from %s", len(bars), filepath)
        return bars


# ─── Core Backtester ─────────────────────────────────────────────────────────

class SwarmBacktester:
    """
    Walk-forward backtester for the full QUANT-SWARM-XAU pipeline.
    Simulates the complete confluence fusion → risk sizing → order execution cycle.
    """

    def __init__(self, config: BacktestConfig):
        self.cfg    = config
        self.trades: list[BacktestTrade] = []
        self.equity_curve: list[float]  = [config.initial_balance]
        self.balance  = config.initial_balance
        self.peak_eq  = config.initial_balance
        self.max_dd   = 0.0
        self.max_dd_pct = 0.0
        self.daily_pnl  = 0.0
        self.last_day   = 0
        self.signal_count = 0
        self.skipped_min_lot = 0
        self.daily_trade_count = 0
        self._last_entry_idx = -10_000

        # Per-strategy stats
        self.strat_stats: dict[str, StrategyStats] = {}

    # ── Signal Generation ────────────────────────────────────────────────────

    def _generate_signal(self, bars: list[dict], idx: int) -> Optional[dict]:
        """
        Simplified signal generator for backtesting.
        Runs rule-based versions of each strategy that don't require
        the full agent infrastructure (pure OHLC logic).
        Returns dict with direction, conviction, entry, sl, tp1, tags, agent_id.
        """
        if self.cfg.precision_scalp:
            return self._generate_precision_scalp_signal(bars, idx - 1)

        if idx < 50:
            return None

        window = bars[max(0, idx-50):idx+1]
        curr   = bars[idx]
        price  = curr["c"]

        signals = []

        # ── Strategy 1: HTF S&R Trap ────────────────────────────────────────
        highs_20 = [b["h"] for b in window[-20:]]
        lows_20  = [b["l"] for b in window[-20:]]
        resistance = sorted(set(round(h, 1) for h in highs_20))[-3:]
        support    = sorted(set(round(l, 1) for l in lows_20))[:3]
        atr_20     = sum(b["h"]-b["l"] for b in window[-20:]) / 20

        for r in resistance:
            if r * 0.9995 <= price <= r * 1.001:  # Price at resistance
                signals.append({"dir": "SELL", "conv": 78, "entry": price,
                                 "sl": r + atr_20*0.3, "tp1": r - atr_20*0.8,
                                 "tags": ["HTF_SR"], "agent": 4})
        for s in support:
            if s * 0.999 <= price <= s * 1.0005:
                signals.append({"dir": "BUY", "conv": 78, "entry": price,
                                 "sl": s - atr_20*0.3, "tp1": s + atr_20*0.8,
                                 "tags": ["HTF_SR"], "agent": 4})

        # ── Strategy 2: CRT Sweep ───────────────────────────────────────────
        if idx >= 2:
            prev = bars[idx-1]
            pch, pcl = prev["h"], prev["l"]
            swept_hi = curr["h"] > pch and price < pch
            swept_lo = curr["l"] < pcl and price > pcl
            if swept_hi:
                signals.append({"dir": "SELL", "conv": 82, "entry": price,
                                 "sl": curr["h"] + atr_20*0.1,
                                 "tp1": (pch+pcl)/2,
                                 "tags": ["CRT_SWEEP"], "agent": 6})
            if swept_lo:
                signals.append({"dir": "BUY", "conv": 82, "entry": price,
                                 "sl": curr["l"] - atr_20*0.1,
                                 "tp1": (pch+pcl)/2,
                                 "tags": ["CRT_SWEEP"], "agent": 6})

        # ── Strategy 3: ICT FVG ──────────────────────────────────────────────
        if idx >= 3:
            b0, b2 = bars[idx-2], bars[idx]
            if b2["l"] > b0["h"] and (b2["l"] - b0["h"]) > atr_20 * 0.15:
                mid_fvg = (b2["l"] + b0["h"]) / 2
                if abs(price - mid_fvg) < atr_20 * 0.2:
                    signals.append({"dir": "BUY", "conv": 88, "entry": price,
                                     "sl": b0["h"] - atr_20*0.1,
                                     "tp1": price + atr_20*1.5,
                                     "tags": ["SMC_FVG"], "agent": 7})
            if b2["h"] < b0["l"] and (b0["l"] - b2["h"]) > atr_20 * 0.15:
                mid_fvg = (b0["l"] + b2["h"]) / 2
                if abs(price - mid_fvg) < atr_20 * 0.2:
                    signals.append({"dir": "SELL", "conv": 88, "entry": price,
                                     "sl": b0["l"] + atr_20*0.1,
                                     "tp1": price - atr_20*1.5,
                                     "tags": ["SMC_FVG"], "agent": 7})

        # ── Strategy 4: Mean Reversion BB ───────────────────────────────────
        closes_20 = [b["c"] for b in window[-20:]]
        mean_c = sum(closes_20) / 20
        std_c  = math.sqrt(sum((c-mean_c)**2 for c in closes_20) / 20)
        upper  = mean_c + 2*std_c
        lower  = mean_c - 2*std_c
        zscore = (price - mean_c) / std_c if std_c > 0 else 0

        if price >= upper and zscore >= 2.0:
            signals.append({"dir": "SELL", "conv": 65, "entry": price,
                             "sl": upper + atr_20*0.1, "tp1": mean_c,
                             "tags": ["MEAN_REV", "BB_UPPER"], "agent": 11})
        if price <= lower and zscore <= -2.0:
            signals.append({"dir": "BUY", "conv": 65, "entry": price,
                             "sl": lower - atr_20*0.1, "tp1": mean_c,
                             "tags": ["MEAN_REV", "BB_LOWER"], "agent": 11})

        # ── Strategy 5: Trendline Break ──────────────────────────────────────
        if len(window) >= 20:
            highs_tl  = [b["h"] for b in window[-20:]]
            x_vals    = list(range(20))
            mx, my    = sum(x_vals)/20, sum(highs_tl)/20
            num = sum((xi-mx)*(yi-my) for xi,yi in zip(x_vals, highs_tl))
            den = sum((xi-mx)**2 for xi in x_vals)
            slope = num/den if den != 0 else 0
            intercept = my - slope*mx
            tl_now = slope*19 + intercept

            if slope < -0.05 and price > tl_now and abs(price-tl_now) < atr_20*0.3:
                signals.append({"dir": "BUY", "conv": 70, "entry": price,
                                 "sl": tl_now - atr_20*0.2,
                                 "tp1": price + atr_20*1.8,
                                 "tags": ["TL_BREAK"], "agent": 5})

        if not signals:
            return None

        # Simple confluence: majority direction wins
        buy_sig  = [s for s in signals if s["dir"] == "BUY"]
        sell_sig = [s for s in signals if s["dir"] == "SELL"]
        dominant = buy_sig if len(buy_sig) >= len(sell_sig) else sell_sig

        if not dominant:
            return None

        avg_conv = sum(s["conv"] for s in dominant) / len(dominant)
        if avg_conv < self.cfg.min_confluence * 0.9:
            return None

        best     = max(dominant, key=lambda s: s["conv"])
        all_tags = list(set(t for s in dominant for t in s["tags"]))
        atr_sl   = abs(best["entry"] - best["sl"])

        return {
            "direction":    best["dir"],
            "confluence":   avg_conv,
            "entry":        best["entry"],
            "sl":           best["sl"],
            "tp1":          best["tp1"],
            "tp2":          best["entry"] + (best["tp1"] - best["entry"]) * 2.0
                            if best["dir"] == "BUY"
                            else best["entry"] - (best["entry"] - best["tp1"]) * 2.0,
            "tp3":          best["entry"] + (best["tp1"] - best["entry"]) * 3.5
                            if best["dir"] == "BUY"
                            else best["entry"] - (best["entry"] - best["tp1"]) * 3.5,
            "tags":         all_tags,
            "agent_id":     best["agent"],
            "atr":          atr_20,
            "agents":       len(dominant),
        }

    def _generate_precision_scalp_signal(self, bars: list[dict],
                                          idx: int) -> Optional[dict]:
        """Small-account standard-lot profile: only tight, swept-liquidity setups."""
        if idx < 35:
            return None

        curr = bars[idx]
        recent = bars[max(0, idx-24):idx]
        if len(recent) < 20:
            return None

        price = curr["c"]
        bar_range = max(curr["h"] - curr["l"], 0.01)
        close_pos = (curr["c"] - curr["l"]) / bar_range
        atr_12 = sum(max(b["h"] - b["l"], 0.01) for b in recent[-12:]) / 12

        confidence = max(self.cfg.min_confluence, 92.0)
        confidence_scalar = min(confidence / 100.0, 1.0)
        risk_usd = self.balance * (self.cfg.risk_pct / 100.0) * confidence_scalar
        max_sl_dist = risk_usd / (self.cfg.min_lot * 100.0)
        if max_sl_dist < self.cfg.precision_min_sl:
            return None

        spread_cost = self.cfg.spread_pts * 0.01 + self.cfg.slippage_pts * 0.01
        sl_dist = max(
            self.cfg.precision_min_sl,
            min(atr_12 * 0.38, max_sl_dist * 0.92, self.cfg.precision_max_sl)
        )
        if sl_dist <= 0 or spread_cost / sl_dist > self.cfg.precision_max_cost_r:
            return None

        prev_high = max(b["h"] for b in recent[-8:])
        prev_low = min(b["l"] for b in recent[-8:])
        avg_body = sum(abs(b["c"] - b["o"]) for b in recent[-12:]) / 12
        body = abs(curr["c"] - curr["o"])
        body_ok = body >= max(avg_body * 0.55, 0.03)

        swept_low = curr["l"] < prev_low and curr["c"] > prev_low
        swept_high = curr["h"] > prev_high and curr["c"] < prev_high
        reclaim_long = swept_low and close_pos >= 0.62 and curr["c"] > curr["o"] and body_ok
        reclaim_short = swept_high and close_pos <= 0.38 and curr["c"] < curr["o"] and body_ok

        if not reclaim_long and not reclaim_short:
            return None

        if reclaim_long:
            direction = "BUY"
            sl = round(price - sl_dist, 2)
            tp1 = round(price + sl_dist * self.cfg.precision_tp_r, 2)
            tags = [
                "PRECISION_SCALP",
                f"{self.cfg.precision_tf_label}_SWEEP_RECLAIM",
                "TIGHT_SL"
            ]
            agent_id = 6
        else:
            direction = "SELL"
            sl = round(price + sl_dist, 2)
            tp1 = round(price - sl_dist * self.cfg.precision_tp_r, 2)
            tags = [
                "PRECISION_SCALP",
                f"{self.cfg.precision_tf_label}_SWEEP_RECLAIM",
                "TIGHT_SL"
            ]
            agent_id = 6

        return {
            "direction":    direction,
            "confluence":   confidence,
            "entry":        price,
            "sl":           sl,
            "tp1":          tp1,
            "tp2":          tp1,
            "tp3":          tp1,
            "tags":         tags,
            "agent_id":     agent_id,
            "atr":          atr_12,
            "agents":       3,
        }

    # ── Order Simulation ─────────────────────────────────────────────────────

    def _simulate_fill(self, signal: dict, bar: dict) -> tuple[float, float]:
        """
        Simulate realistic fill: spread + random slippage bounded by config.
        Returns (fill_price, slippage_pts).
        """
        spread = self.cfg.spread_pts * 0.01   # Convert pts to price
        slip   = random.uniform(0, self.cfg.slippage_pts) * 0.01

        if signal["direction"] == "BUY":
            fill = bar["o"] + spread + slip   # Pay spread + slip on entry
        else:
            fill = bar["o"] - spread - slip

        return round(fill, 2), round(slip * 100, 1)

    def _calculate_lot_size(self, entry: float, sl: float, confluence: float) -> float:
        """Risk-scaled lot sizing identical to live InstitutionalRiskController."""
        sl_dist = abs(entry - sl)
        if sl_dist <= 0:
            return 0.0

        confidence_scalar = min(confluence / 100.0, 1.0)
        risk_usd = self.balance * (self.cfg.risk_pct / 100.0) * confidence_scalar
        raw_lot = risk_usd / (sl_dist * 100.0)
        if raw_lot < self.cfg.min_lot and self.cfg.skip_oversized_min_lot:
            return 0.0

        lot_precision = _lot_precision(self.cfg.lot_step)
        lot_size = round(
            round(raw_lot / self.cfg.lot_step) * self.cfg.lot_step,
            lot_precision
        )
        lot_size = max(self.cfg.min_lot, min(lot_size, self.cfg.max_lot))
        return round(lot_size, lot_precision)

    def _simulate_exit(self, signal: dict, bars: list[dict],
                        entry_idx: int, fill_price: float,
                        lot_size: float) -> tuple[float, float, str]:
        """
        Walk forward bar-by-bar from entry, check SL/TP hits.
        Returns (exit_price, exit_idx, exit_reason).
        Max hold: 48 bars.
        """
        direction = signal["direction"]
        sl        = signal["sl"]
        tp1       = signal["tp1"]

        for j in range(1, min(49, len(bars) - entry_idx)):
            bar = bars[entry_idx + j]

            if direction == "BUY":
                if bar["l"] <= sl:
                    return sl, entry_idx+j, "SL"
                if bar["h"] >= tp1:
                    return tp1, entry_idx+j, "TP1"
            else:  # SELL
                if bar["h"] >= sl:
                    return sl, entry_idx+j, "SL"
                if bar["l"] <= tp1:
                    return tp1, entry_idx+j, "TP1"

        # Time-out exit at market
        exit_bar = bars[min(entry_idx + 48, len(bars)-1)]
        return exit_bar["c"], min(entry_idx+48, len(bars)-1), "TIMEOUT"

    # ── Main Backtest Runner ─────────────────────────────────────────────────

    def run(self, bars: list[dict]) -> dict:
        """
        Full walk-forward backtest.
        Returns comprehensive performance report dict.
        """
        log.info("Starting backtest: %d bars | Balance: $%.0f | Risk: %.1f%%",
                 len(bars), self.balance, self.cfg.risk_pct)

        open_trade: Optional[dict] = None
        daily_dd_halted = False

        for i, bar in enumerate(bars):
            # Daily reset
            day = int(bar["t"] // 86400)
            if day != self.last_day:
                self.daily_pnl  = 0.0
                self.daily_trade_count = 0
                daily_dd_halted = False
                self.last_day   = day

            if daily_dd_halted:
                continue

            # Check for session filter (only London 7-12 UTC, NY 12-20 UTC)
            if self.cfg.session_filter:
                bar_hour = int((bar["t"] % 86400) / 3600)
                if not (7 <= bar_hour < 20):
                    continue

            # Manage open trade
            if open_trade:
                direction = open_trade["direction"]
                sl  = open_trade.get("current_sl", open_trade["sl"])
                tp1 = open_trade["tp1"]
                if self.cfg.precision_scalp:
                    sl_dist = abs(open_trade["fill_price"] - open_trade["sl"])
                    if sl_dist > 0:
                        favorable = (
                            bar["h"] - open_trade["fill_price"]
                            if direction == "BUY"
                            else open_trade["fill_price"] - bar["l"]
                        )
                        favorable_r = favorable / sl_dist
                        stop_r = (
                            (sl - open_trade["fill_price"]) / sl_dist
                            if direction == "BUY"
                            else (open_trade["fill_price"] - sl) / sl_dist
                        )
                        if favorable_r >= self.cfg.precision_be_trigger_r:
                            stop_r = max(stop_r, 0.0)
                        if favorable_r >= self.cfg.precision_trail_start_r:
                            stop_r = max(
                                stop_r,
                                self.cfg.precision_min_lock_r,
                                favorable_r - self.cfg.precision_trail_gap_r,
                            )
                        if stop_r > 0:
                            if direction == "BUY":
                                sl = round(open_trade["fill_price"] + sl_dist * stop_r, 2)
                            else:
                                sl = round(open_trade["fill_price"] - sl_dist * stop_r, 2)
                            open_trade["current_sl"] = sl

                hit_sl  = (direction == "BUY" and bar["l"] <= sl) or \
                          (direction == "SELL" and bar["h"] >= sl)
                hit_tp1 = (direction == "BUY" and bar["h"] >= tp1) or \
                          (direction == "SELL" and bar["l"] <= tp1)
                max_hold = self.cfg.precision_max_hold_bars if self.cfg.precision_scalp else 48
                timeout = (i - open_trade["entry_idx"]) >= max_hold

                if hit_sl or hit_tp1 or timeout:
                    exit_price  = sl if hit_sl else tp1 if hit_tp1 else bar["c"]
                    exit_reason = "SL" if hit_sl else "TP1" if hit_tp1 else "TIMEOUT"

                    lot_size    = open_trade["lot_size"]
                    raw_pnl     = (exit_price - open_trade["fill_price"]) * \
                                   lot_size * 100.0
                    if direction == "SELL":
                        raw_pnl = -raw_pnl

                    commission  = self.cfg.commission_per_lot * lot_size
                    net_pnl     = raw_pnl - commission
                    if self.balance + net_pnl < 0:
                        net_pnl = -self.balance
                        raw_pnl = net_pnl + commission
                    sl_dist     = abs(open_trade["fill_price"] - open_trade["sl"])
                    outcome_r   = net_pnl / (sl_dist * lot_size * 100.0) \
                                  if sl_dist > 0 else 0.0

                    self.balance   += net_pnl
                    self.daily_pnl += net_pnl
                    self.equity_curve.append(round(self.balance, 2))

                    # Update peak / drawdown
                    if self.balance > self.peak_eq:
                        self.peak_eq = self.balance
                    dd = self.peak_eq - self.balance
                    if dd > self.max_dd:
                        self.max_dd     = dd
                        self.max_dd_pct = dd / self.peak_eq * 100

                    # Daily DD halt
                    daily_dd_pct = abs(self.daily_pnl) / \
                                   (self.balance - self.daily_pnl) * 100
                    if self.daily_pnl < 0 and daily_dd_pct >= self.cfg.max_daily_dd_pct:
                        daily_dd_halted = True

                    # Record trade
                    strat = "+".join(open_trade["tags"][:2])
                    trade = BacktestTrade(
                        trade_id     = f"BT_{i:05d}",
                        agent_id     = open_trade["agent_id"],
                        strategy     = strat,
                        direction    = direction,
                        entry_time   = open_trade["entry_bar"]["t"],
                        exit_time    = bar["t"],
                        entry_price  = open_trade["fill_price"],
                        exit_price   = exit_price,
                        sl_price     = sl,
                        tp1_price    = open_trade["tp1"],
                        lot_size     = lot_size,
                        gross_pnl    = raw_pnl,
                        commission   = commission,
                        net_pnl      = net_pnl,
                        outcome_r    = round(outcome_r, 3),
                        slippage_pts = open_trade["slippage_pts"],
                        session      = self._bar_session(bar),
                        tags         = open_trade["tags"],
                        confluence   = open_trade["confluence"],
                    )
                    self.trades.append(trade)

                    # Update per-strategy stats
                    if strat not in self.strat_stats:
                        self.strat_stats[strat] = StrategyStats(
                            name=strat, peak_equity=self.cfg.initial_balance,
                            equity_curve=[self.cfg.initial_balance]
                        )
                    ss = self.strat_stats[strat]
                    ss.trades  += 1
                    ss.net_r   += outcome_r
                    ss.equity_curve.append(ss.equity_curve[-1] + net_pnl)
                    if net_pnl > 0:
                        ss.wins       += 1
                        ss.gross_profit += abs(net_pnl)
                    else:
                        ss.losses     += 1
                        ss.gross_loss += abs(net_pnl)
                    if ss.equity_curve[-1] > ss.peak_equity:
                        ss.peak_equity = ss.equity_curve[-1]
                    ss_dd = ss.peak_equity - ss.equity_curve[-1]
                    if ss_dd > ss.max_drawdown:
                        ss.max_drawdown     = ss_dd
                        ss.max_drawdown_pct = ss_dd / ss.peak_equity * 100

                    open_trade = None
                    if self.balance <= 0:
                        break
                continue  # Don't look for new entries while managing trade

            # Look for new signal
            sig = self._generate_signal(bars, i)
            if not sig:
                continue

            self.signal_count += 1
            lot_size  = self._calculate_lot_size(
                sig["entry"], sig["sl"], sig["confluence"]
            )
            if lot_size < self.cfg.min_lot:
                self.skipped_min_lot += 1
                continue
            if self.cfg.max_trades_per_day and \
               self.daily_trade_count >= self.cfg.max_trades_per_day:
                continue
            if self.cfg.cooldown_bars and \
               (i - self._last_entry_idx) < self.cfg.cooldown_bars:
                continue
            spread    = self.cfg.spread_pts * 0.01
            slip_pts  = random.uniform(0, self.cfg.slippage_pts)
            slip      = slip_pts * 0.01
            fill      = (bar["o"] + spread + slip
                         if sig["direction"] == "BUY"
                         else bar["o"] - spread - slip)

            open_trade = {
                "direction":    sig["direction"],
                "fill_price":   round(fill, 2),
                "sl":           sig["sl"],
                "current_sl":    sig["sl"],
                "tp1":          sig["tp1"],
                "lot_size":     lot_size,
                "tags":         sig["tags"],
                "agent_id":     sig["agent_id"],
                "confluence":   sig["confluence"],
                "slippage_pts": round(slip_pts, 1),
                "entry_idx":    i,
                "entry_bar":    bar,
            }
            self.daily_trade_count += 1
            self._last_entry_idx = i

        return self._compile_report()

    def _bar_session(self, bar: dict) -> str:
        h = int((bar["t"] % 86400) / 3600)
        if  0 <= h <  7: return "ASIA"
        if  7 <= h < 12: return "LONDON"
        if 12 <= h < 20: return "NY"
        return "OFF"

    # ── Report Compilation ────────────────────────────────────────────────────

    def _compile_report(self) -> dict:
        if not self.trades:
            total_return = (
                (self.balance - self.cfg.initial_balance)
                / self.cfg.initial_balance * 100
            ) if self.cfg.initial_balance > 0 else 0.0
            return {
                "error": "No trades generated",
                "summary": {
                    "initial_balance":   self.cfg.initial_balance,
                    "final_balance":     round(self.balance, 2),
                    "net_profit_usd":    round(self.balance - self.cfg.initial_balance, 2),
                    "total_return_pct":  round(total_return, 2),
                    "total_trades":      0,
                    "win_rate_pct":      0.0,
                    "profit_factor":     0.0,
                    "sharpe_ratio":      0.0,
                    "max_drawdown_pct":  round(self.max_dd_pct, 2),
                    "max_drawdown_usd":  round(self.max_dd, 2),
                    "calmar_ratio":      0.0,
                    "avg_r_multiple":    0.0,
                    "avg_trade_hours":   0.0,
                    "max_consec_wins":   0,
                    "max_consec_losses": 0,
                    "total_commission":  0.0,
                },
                "risk_sizing": {
                    "signals_seen":       self.signal_count,
                    "skipped_min_lot":    self.skipped_min_lot,
                    "risk_pct":           self.cfg.risk_pct,
                    "max_daily_dd_pct":   self.cfg.max_daily_dd_pct,
                    "min_lot":            self.cfg.min_lot,
                    "lot_step":           self.cfg.lot_step,
                    "skip_oversized_min_lot": self.cfg.skip_oversized_min_lot,
                },
                "strategy_breakdown": {},
                "session_breakdown":  {},
                "equity_curve":       self.equity_curve,
                "n_bars_tested":      0,
            }

        r_vals    = [t.outcome_r for t in self.trades]
        wins      = [t for t in self.trades if t.net_pnl > 0]
        losses    = [t for t in self.trades if t.net_pnl <= 0]

        gross_profit = sum(t.gross_pnl for t in wins)
        gross_loss   = abs(sum(t.gross_pnl for t in losses))
        net_profit   = sum(t.net_pnl for t in self.trades)
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0.0

        mean_r = sum(r_vals) / len(r_vals)
        std_r  = math.sqrt(sum((r-mean_r)**2 for r in r_vals) / len(r_vals))
        sharpe = mean_r / std_r * math.sqrt(252) if std_r > 0 else 0.0

        # Consecutive stats
        max_consec_wins = max_consec_losses = 0
        curr_w = curr_l = 0
        for t in self.trades:
            if t.net_pnl > 0:
                curr_w += 1; curr_l = 0
            else:
                curr_l += 1; curr_w = 0
            max_consec_wins   = max(max_consec_wins,   curr_w)
            max_consec_losses = max(max_consec_losses, curr_l)

        # Average trade duration
        avg_duration_h = sum((t.exit_time - t.entry_time) / 3600
                              for t in self.trades) / len(self.trades)

        # Session breakdown
        session_stats = {}
        for s in ["LONDON", "NY", "ASIA"]:
            s_trades = [t for t in self.trades if t.session == s]
            if s_trades:
                s_wins = [t for t in s_trades if t.net_pnl > 0]
                session_stats[s] = {
                    "trades":   len(s_trades),
                    "win_rate": round(len(s_wins)/len(s_trades)*100, 1),
                    "net_pnl":  round(sum(t.net_pnl for t in s_trades), 2),
                }

        # Per-strategy summary
        strat_summary = {}
        for name, ss in self.strat_stats.items():
            strat_summary[name] = {
                "trades":        ss.trades,
                "win_rate_pct":  round(ss.win_rate * 100, 1),
                "profit_factor": round(ss.profit_factor, 2),
                "net_r":         round(ss.net_r, 2),
                "avg_r":         round(ss.avg_r, 3),
                "sharpe":        round(ss.sharpe, 2),
                "max_dd_pct":    round(ss.max_drawdown_pct, 2),
                "calmar":        round(ss.calmar, 2),
            }

        final_balance = self.equity_curve[-1]
        total_return  = (final_balance - self.cfg.initial_balance) / \
                         self.cfg.initial_balance * 100

        return {
            "summary": {
                "initial_balance":   self.cfg.initial_balance,
                "final_balance":     round(final_balance, 2),
                "net_profit_usd":    round(net_profit, 2),
                "total_return_pct":  round(total_return, 2),
                "total_trades":      len(self.trades),
                "win_rate_pct":      round(len(wins)/len(self.trades)*100, 1),
                "profit_factor":     round(profit_factor, 2),
                "sharpe_ratio":      round(sharpe, 2),
                "max_drawdown_pct":  round(self.max_dd_pct, 2),
                "max_drawdown_usd":  round(self.max_dd, 2),
                "calmar_ratio":      round(total_return / self.max_dd_pct, 2)
                                     if self.max_dd_pct > 0 else 0,
                "avg_r_multiple":    round(mean_r, 3),
                "avg_trade_hours":   round(avg_duration_h, 1),
                "max_consec_wins":   max_consec_wins,
                "max_consec_losses": max_consec_losses,
                "total_commission":  round(sum(t.commission for t in self.trades), 2),
            },
            "risk_sizing": {
                "signals_seen":       self.signal_count,
                "skipped_min_lot":    self.skipped_min_lot,
                "risk_pct":           self.cfg.risk_pct,
                "max_daily_dd_pct":   self.cfg.max_daily_dd_pct,
                "min_lot":            self.cfg.min_lot,
                "lot_step":           self.cfg.lot_step,
                "skip_oversized_min_lot": self.cfg.skip_oversized_min_lot,
            },
            "strategy_breakdown": strat_summary,
            "session_breakdown":  session_stats,
            "equity_curve":       self.equity_curve[::10],   # Downsample
            "n_bars_tested":      0,  # Filled by caller
        }

    # ── Monte Carlo Significance Test ────────────────────────────────────────

    def monte_carlo_test(self, n_sims: int = None) -> dict:
        """
        Permutation test: shuffle trade outcomes 1000× and compare
        actual Sharpe/PF to shuffled distribution.
        p-value < 0.05 → strategy is non-random.
        """
        n_sims = n_sims or self.cfg.n_monte_carlo
        if not self.trades:
            return {}

        r_vals = [t.outcome_r for t in self.trades]
        actual_mean = sum(r_vals) / len(r_vals)

        shuffled_means = []
        for _ in range(n_sims):
            shuffled = r_vals.copy()
            random.shuffle(shuffled)
            shuffled_means.append(sum(shuffled) / len(shuffled))

        p_value = sum(1 for m in shuffled_means if m >= actual_mean) / n_sims

        return {
            "actual_mean_r":   round(actual_mean, 4),
            "shuffled_mean_r": round(sum(shuffled_means)/len(shuffled_means), 4),
            "p_value":         round(p_value, 4),
            "significant":     p_value < 0.05,
            "n_simulations":   n_sims,
        }

    # ── Report Formatting ─────────────────────────────────────────────────────

    @staticmethod
    def format_report(report: dict) -> str:
        s = report["summary"]
        mc = report.get("monte_carlo", {})

        lines = [
            "═"*62,
            "  QUANT-SWARM-XAU BACKTEST REPORT",
            "═"*62,
            f"  Bars Tested:      {report.get('n_bars_tested', 0):,}",
            f"  Initial Balance:  ${s['initial_balance']:>10,.2f}",
            f"  Final Balance:    ${s['final_balance']:>10,.2f}",
            f"  Net Profit:       ${s['net_profit_usd']:>+10,.2f}  ({s['total_return_pct']:+.1f}%)",
            f"  Commissions:      ${s['total_commission']:>10,.2f}",
            "─"*62,
            f"  Total Trades:     {s['total_trades']:>6}",
            f"  Win Rate:         {s['win_rate_pct']:>6.1f}%",
            f"  Profit Factor:    {s['profit_factor']:>6.2f}",
            f"  Sharpe Ratio:     {s['sharpe_ratio']:>6.2f}",
            f"  Calmar Ratio:     {s['calmar_ratio']:>6.2f}",
            f"  Max Drawdown:     {s['max_drawdown_pct']:>6.2f}%  (${s['max_drawdown_usd']:,.2f})",
            f"  Avg R/Trade:      {s['avg_r_multiple']:>+6.3f}R",
            f"  Avg Hold Time:    {s['avg_trade_hours']:>5.1f}h",
            f"  Max Consec W/L:   {s['max_consec_wins']}W / {s['max_consec_losses']}L",
            "─"*62,
            "  STRATEGY BREAKDOWN:",
        ]

        strats = report.get("strategy_breakdown", {})
        sorted_strats = sorted(strats.items(),
                                key=lambda x: x[1]["profit_factor"], reverse=True)
        for name, ss in sorted_strats:
            icon = "🥇" if ss["profit_factor"] == max(v["profit_factor"] for v in strats.values()) \
                   else ("⚠️" if ss["profit_factor"] < 1.0 else "  ")
            lines.append(
                f"  {icon} {name[:20]:<20} "
                f"T:{ss['trades']:>3}  WR:{ss['win_rate_pct']:>5.1f}%  "
                f"PF:{ss['profit_factor']:>4.2f}  "
                f"R:{ss['net_r']:>+5.1f}  "
                f"Sharpe:{ss['sharpe']:>+5.2f}"
            )

        lines.append("─"*62)
        lines.append("  SESSION BREAKDOWN:")
        for sess, ss in report.get("session_breakdown", {}).items():
            lines.append(f"    {sess:<8}  Trades:{ss['trades']:>3}  "
                         f"WR:{ss['win_rate']:>5.1f}%  "
                         f"PnL:${ss['net_pnl']:>+8,.2f}")

        if mc:
            sig_str = "✓ STATISTICALLY SIGNIFICANT" if mc.get("significant") \
                      else "✗ NOT SIGNIFICANT (could be random)"
            lines += [
                "─"*62,
                f"  MONTE CARLO TEST ({mc.get('n_simulations',0):,} permutations):",
                f"    Actual Mean R:   {mc['actual_mean_r']:>+.4f}",
                f"    Shuffled Mean R: {mc['shuffled_mean_r']:>+.4f}",
                f"    p-value:         {mc['p_value']:.4f}  {sig_str}",
            ]

        lines.append("═"*62)
        return "\n".join(lines)

    def save_trade_log(self, filepath: str = None):
        """Save full trade log to CSV."""
        path = Path(filepath).expanduser() if filepath else BACKTEST_DIR / "trade_log.csv"
        if not self.trades:
            return
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fieldnames = list(asdict(self.trades[0]).keys())
        with path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for t in self.trades:
                row = asdict(t)
                row["tags"] = "+".join(row["tags"])
                w.writerow(row)
        log.info("Trade log saved: %s (%d trades)", path, len(self.trades))
        return str(path)


# ─── Walk-Forward Runner ──────────────────────────────────────────────────────

def run_walk_forward(bars: list[dict], cfg: BacktestConfig) -> list[dict]:
    """
    Walk-forward validation: slide IS/OOS window across full dataset.
    Returns per-window OOS results to detect overfitting.
    """
    is_bars  = cfg.is_window_days  * 24   # Convert days → H1 bars
    oos_bars = cfg.oos_window_days * 24
    step     = oos_bars

    results = []
    start   = 0

    while start + is_bars + oos_bars <= len(bars):
        is_slice  = bars[start : start + is_bars]
        oos_slice = bars[start + is_bars : start + is_bars + oos_bars]

        # IS: fit (just run to get stats; no parameter tuning in rule-based)
        bt_is  = SwarmBacktester(cfg)
        bt_is.run(is_slice)

        # OOS: evaluate
        bt_oos = SwarmBacktester(cfg)
        report = bt_oos.run(oos_slice)
        report["n_bars_tested"] = len(oos_slice)
        report["window_start"]  = datetime.fromtimestamp(
            oos_slice[0]["t"], tz=timezone.utc).strftime("%Y-%m-%d")
        report["window_end"]    = datetime.fromtimestamp(
            oos_slice[-1]["t"], tz=timezone.utc).strftime("%Y-%m-%d")
        report["is_pf"] = bt_is._compile_report().get("summary", {}).get("profit_factor", 0)
        results.append(report)

        start += step
        log.info("WF Window %d: OOS %s→%s | PF=%.2f | Sharpe=%.2f",
                 len(results),
                 report["window_start"], report["window_end"],
                 report["summary"]["profit_factor"],
                 report["summary"]["sharpe_ratio"])

    return results


# ─── Main Demo ────────────────────────────────────────────────────────────────

def run_backtest_demo():
    print("\n" + "═"*62)
    print("  QUANT-SWARM-XAU BACKTEST DEMO")
    print("═"*62)

    cfg  = BacktestConfig(
        initial_balance  = 10_000.0,
        risk_pct         = 1.0,
        spread_pts       = 3,
        slippage_pts     = 5,
        commission_per_lot = 7.0,
        session_filter   = True,
        n_monte_carlo    = 500,
    )

    print(f"\n  Generating {2000} synthetic H1 bars (GBM + mean-reversion)...")
    bars = HistoricalDataProvider.generate_synthetic(
        n_bars=2000, tf_mins=60, start_price=2600.0,
        annual_vol=0.15, trend_drift=0.05
    )
    print(f"  Price range: ${min(b['l'] for b in bars):.2f} → ${max(b['h'] for b in bars):.2f}")

    print(f"\n  Running backtest ({len(bars)} bars)...")
    bt = SwarmBacktester(cfg)
    report = bt.run(bars)
    report["n_bars_tested"] = len(bars)

    # Monte Carlo test
    print("  Running Monte Carlo significance test (500 permutations)...")
    mc = bt.monte_carlo_test(500)
    report["monte_carlo"] = mc

    print(SwarmBacktester.format_report(report))

    # Save CSV trade log
    log_path = bt.save_trade_log()
    if log_path:
        print(f"\n  Trade log saved → {log_path}")

    # Walk-forward summary
    print("\n  Running walk-forward validation (90-day IS / 30-day OOS)...")
    wf_results = run_walk_forward(bars, cfg)
    if wf_results:
        pfs    = [r["summary"]["profit_factor"] for r in wf_results]
        sharps = [r["summary"]["sharpe_ratio"]   for r in wf_results]
        print(f"\n  Walk-Forward Summary ({len(wf_results)} windows):")
        print(f"    OOS Profit Factor avg:  {sum(pfs)/len(pfs):.2f}  (range: {min(pfs):.2f}–{max(pfs):.2f})")
        print(f"    OOS Sharpe avg:         {sum(sharps)/len(sharps):.2f}  (range: {min(sharps):.2f}–{max(sharps):.2f})")
        positive_pf = sum(1 for pf in pfs if pf > 1.0)
        print(f"    Profitable windows:     {positive_pf}/{len(wf_results)}")

    # Results are evidence and must not be deleted by the backtester. Test and
    # simulation callers that need disposable output must inject a marked
    # temporary state directory and let the outer harness clean it safely.


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    run_backtest_demo()
