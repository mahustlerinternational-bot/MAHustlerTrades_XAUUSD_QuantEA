"""
QUANT-SWARM-XAU: Performance Analytics Module
Deep-dive statistical analysis of swarm trading performance.
Produces formatted reports for Telegram weekly digest and internal review.

Metrics:
  - Equity curve analytics (Sharpe, Sortino, Calmar, Max DD, Recovery factor)
  - Per-strategy alpha attribution
  - Session performance breakdown
  - Agent weight evolution tracking
  - Time-of-day edge analysis (heatmap)
  - R-multiple distribution statistics
  - Drawdown analysis (duration, depth, recovery time)
  - Monte Carlo confidence intervals
"""

import math
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# ─── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class EquityCurveStats:
    initial:            float
    final:              float
    net_pnl:            float
    total_return_pct:   float
    annualized_return:  float
    sharpe:             float
    sortino:            float
    calmar:             float
    max_dd_pct:         float
    max_dd_usd:         float
    max_dd_duration_h:  float
    recovery_factor:    float
    ulcer_index:        float
    profit_factor:      float
    win_rate:           float
    avg_win_r:          float
    avg_loss_r:         float
    expectancy_r:       float
    avg_hold_hours:     float
    trades_per_week:    float

@dataclass
class StrategyAlpha:
    name:           str
    trades:         int
    wins:           int
    losses:         int
    net_r:          float
    profit_factor:  float
    sharpe:         float
    avg_r:          float
    best_r:         float
    worst_r:        float
    max_dd_r:       float
    win_rate:       float
    avg_hold_h:     float
    top_session:    str


# ─── Core Analytics Engine ────────────────────────────────────────────────────

class PerformanceAnalytics:
    """
    Reads from agent SQLite journals and the risk ledger to produce
    comprehensive performance analytics.
    """

    def __init__(self, journal_dir: str = "./journal_db"):
        self.journal_dir = Path(journal_dir)

    # ── Equity Curve Analysis ─────────────────────────────────────────────────

    @staticmethod
    def equity_curve_stats(equity_curve: list[float],
                            trades: list[dict],
                            period_days: float = None) -> EquityCurveStats:
        """
        Full institutional equity curve analytics from list of equity values
        and trade records.
        trades: list of dicts with keys: net_pnl, outcome_r, session,
                entry_time, exit_time, direction, strategy
        """
        if not equity_curve or len(equity_curve) < 2:
            return None

        initial = equity_curve[0]
        final   = equity_curve[-1]
        net_pnl = final - initial
        total_r = net_pnl / initial * 100.0

        # Period in years (estimate from trade timestamps or assume 1 year)
        if trades and period_days is None:
            span_s      = trades[-1]["exit_time"] - trades[0]["entry_time"]
            period_days = span_s / 86400.0
        period_days = period_days or 365.0
        years = max(period_days / 365.0, 1/365.0)

        # Annualised return (CAGR)
        ann_return = ((final / initial) ** (1.0 / years) - 1.0) * 100.0

        # Trade-level R-multiples
        r_vals  = [t.get("outcome_r", 0) for t in trades]
        wins    = [r for r in r_vals if r > 0]
        losses  = [r for r in r_vals if r <= 0]

        # Sharpe (trade-level)
        mean_r = sum(r_vals) / len(r_vals) if r_vals else 0
        std_r  = _std(r_vals)
        sharpe = (mean_r / std_r * math.sqrt(252)) if std_r > 0 else 0.0

        # Sortino (downside deviation only)
        down_dev = _std([min(r, 0) for r in r_vals])
        sortino  = (mean_r / down_dev * math.sqrt(252)) if down_dev > 0 else 0.0

        # Drawdown analysis
        peak = initial
        max_dd_usd = 0.0
        max_dd_pct = 0.0
        max_dd_start = max_dd_end = 0
        drawdowns = []
        dd_start  = None

        for i, eq in enumerate(equity_curve):
            if eq > peak:
                if dd_start is not None:
                    drawdowns.append((dd_start, i, peak - min(equity_curve[dd_start:i])))
                    dd_start = None
                peak = eq
            else:
                if dd_start is None:
                    dd_start = i
                dd = peak - eq
                dd_pct = dd / peak * 100.0 if peak > 0 else 0
                if dd > max_dd_usd:
                    max_dd_usd   = dd
                    max_dd_pct   = dd_pct
                    max_dd_start = dd_start or i
                    max_dd_end   = i

        # Max drawdown duration
        max_dd_duration_h = 0.0
        if trades and max_dd_start < len(trades) and max_dd_end < len(trades):
            t_start = trades[min(max_dd_start, len(trades)-1)].get("entry_time", 0)
            t_end   = trades[min(max_dd_end,   len(trades)-1)].get("exit_time",  0)
            max_dd_duration_h = (t_end - t_start) / 3600.0

        # Recovery factor
        recovery_factor = abs(net_pnl / max_dd_usd) if max_dd_usd > 0 else 0.0

        # Ulcer Index: RMS of % drawdowns
        pct_dds = []
        pk = initial
        for eq in equity_curve:
            if eq > pk:
                pk = eq
            pct_dds.append(((pk - eq) / pk * 100.0) ** 2 if pk > 0 else 0)
        ulcer = math.sqrt(sum(pct_dds) / len(pct_dds)) if pct_dds else 0.0

        # Calmar
        calmar = ann_return / max_dd_pct if max_dd_pct > 0 else 0.0

        # Win/loss stats
        avg_win  = sum(wins) / len(wins)       if wins   else 0.0
        avg_loss = sum(losses) / len(losses)   if losses else 0.0
        pf       = abs(sum(wins) / sum(losses)) if losses and sum(losses) != 0 else 0.0
        wr       = len(wins) / len(r_vals)     if r_vals else 0.0
        expectancy = mean_r

        # Hold time
        avg_hold_h = sum(
            (t.get("exit_time", 0) - t.get("entry_time", 0)) / 3600.0
            for t in trades
        ) / len(trades) if trades else 0.0

        # Trades per week
        tpw = len(trades) / max(period_days / 7.0, 1.0)

        return EquityCurveStats(
            initial           = initial,
            final             = round(final, 2),
            net_pnl           = round(net_pnl, 2),
            total_return_pct  = round(total_r, 2),
            annualized_return = round(ann_return, 2),
            sharpe            = round(sharpe, 3),
            sortino           = round(sortino, 3),
            calmar            = round(calmar, 3),
            max_dd_pct        = round(max_dd_pct, 2),
            max_dd_usd        = round(max_dd_usd, 2),
            max_dd_duration_h = round(max_dd_duration_h, 1),
            recovery_factor   = round(recovery_factor, 2),
            ulcer_index       = round(ulcer, 3),
            profit_factor     = round(pf, 3),
            win_rate          = round(wr, 4),
            avg_win_r         = round(avg_win, 3),
            avg_loss_r        = round(avg_loss, 3),
            expectancy_r      = round(expectancy, 4),
            avg_hold_hours    = round(avg_hold_h, 2),
            trades_per_week   = round(tpw, 1),
        )

    # ── Strategy Alpha Attribution ────────────────────────────────────────────

    @staticmethod
    def strategy_alpha(trades: list[dict]) -> dict[str, StrategyAlpha]:
        """Decompose performance by strategy tag."""
        by_strat: dict[str, list[dict]] = defaultdict(list)
        for t in trades:
            strat = t.get("strategy", t.get("tags", "UNKNOWN"))
            if isinstance(strat, list):
                strat = "+".join(strat[:2])
            by_strat[strat].append(t)

        result = {}
        for name, strat_trades in by_strat.items():
            r_vals = [t.get("outcome_r", 0) for t in strat_trades]
            wins   = [r for r in r_vals if r > 0]
            losses = [r for r in r_vals if r < 0]
            pf     = abs(sum(wins)/sum(losses)) if losses and sum(losses) != 0 else 0.0

            # Per-strategy Sharpe
            mean_r = sum(r_vals)/len(r_vals) if r_vals else 0
            std_r  = _std(r_vals)
            sharpe = (mean_r / std_r * math.sqrt(252)) if std_r > 0 else 0.0

            # Session breakdown for this strategy
            sess_r: dict[str, float] = defaultdict(float)
            for t in strat_trades:
                sess_r[t.get("session", "?")] += t.get("outcome_r", 0)
            top_sess = max(sess_r, key=sess_r.get) if sess_r else "?"

            # Per-strategy max drawdown in R
            cum_r = 0.0; peak_r = 0.0; max_dd_r = 0.0
            for r in r_vals:
                cum_r += r
                peak_r = max(peak_r, cum_r)
                max_dd_r = max(max_dd_r, peak_r - cum_r)

            hold_times = [(t.get("exit_time",0) - t.get("entry_time",0))/3600.0
                          for t in strat_trades
                          if t.get("exit_time") and t.get("entry_time")]
            avg_hold = sum(hold_times)/len(hold_times) if hold_times else 0

            result[name] = StrategyAlpha(
                name          = name,
                trades        = len(r_vals),
                wins          = len(wins),
                losses        = len(losses),
                net_r         = round(sum(r_vals), 3),
                profit_factor = round(pf, 3),
                sharpe        = round(sharpe, 3),
                avg_r         = round(mean_r, 4),
                best_r        = round(max(r_vals), 3) if r_vals else 0,
                worst_r       = round(min(r_vals), 3) if r_vals else 0,
                max_dd_r      = round(max_dd_r, 3),
                win_rate      = round(len(wins)/len(r_vals), 4) if r_vals else 0,
                avg_hold_h    = round(avg_hold, 2),
                top_session   = top_sess,
            )

        return dict(sorted(result.items(),
                           key=lambda x: x[1].net_r, reverse=True))

    # ── Time-of-Day Edge Analysis ─────────────────────────────────────────────

    @staticmethod
    def time_of_day_heatmap(trades: list[dict]) -> dict[int, dict]:
        """
        Analyse performance by UTC hour to find peak alpha windows.
        Returns {hour: {trades, win_rate, avg_r, net_r}}.
        """
        by_hour: dict[int, list[float]] = defaultdict(list)
        for t in trades:
            entry_ts = t.get("entry_time", t.get("timestamp_open", 0))
            if entry_ts:
                hour = int((entry_ts % 86400) / 3600)
                by_hour[hour].append(t.get("outcome_r", 0))

        result = {}
        for hour in range(24):
            r_vals = by_hour.get(hour, [])
            if not r_vals:
                result[hour] = {"trades": 0, "win_rate": 0, "avg_r": 0, "net_r": 0}
                continue
            wins = [r for r in r_vals if r > 0]
            result[hour] = {
                "trades":   len(r_vals),
                "win_rate": round(len(wins)/len(r_vals)*100, 1),
                "avg_r":    round(sum(r_vals)/len(r_vals), 3),
                "net_r":    round(sum(r_vals), 3),
            }
        return result

    # ── R-Distribution Stats ──────────────────────────────────────────────────

    @staticmethod
    def r_distribution(trades: list[dict]) -> dict:
        """Full R-multiple distribution statistics."""
        r_vals = sorted(t.get("outcome_r", 0) for t in trades)
        if not r_vals:
            return {}

        n = len(r_vals)
        mean_r = sum(r_vals) / n
        std_r  = _std(r_vals)

        # Percentiles
        def pct(p: float) -> float:
            idx = max(0, min(n-1, int(p/100 * n)))
            return round(r_vals[idx], 3)

        # Skewness
        skew = sum((r - mean_r)**3 for r in r_vals) / (n * std_r**3) \
               if std_r > 0 else 0

        # Kurtosis
        kurt = sum((r - mean_r)**4 for r in r_vals) / (n * std_r**4) - 3 \
               if std_r > 0 else 0

        return {
            "count":     n,
            "mean":      round(mean_r, 4),
            "std":       round(std_r,  4),
            "skewness":  round(skew,   3),
            "kurtosis":  round(kurt,   3),
            "min":       round(r_vals[0], 3),
            "p5":        pct(5),
            "p25":       pct(25),
            "median":    pct(50),
            "p75":       pct(75),
            "p95":       pct(95),
            "max":       round(r_vals[-1], 3),
            "positive":  sum(1 for r in r_vals if r > 0),
            "negative":  sum(1 for r in r_vals if r <= 0),
        }

    # ── Formatted Reports ─────────────────────────────────────────────────────

    @staticmethod
    def format_weekly_digest(
        stats: EquityCurveStats,
        alpha: dict[str, StrategyAlpha],
        tod_map: dict[int, dict],
        r_dist: dict,
        agent_weights: dict[int, float] = None,
    ) -> str:
        """Institutional-grade weekly performance digest (plain text)."""
        top_strat   = max(alpha.values(), key=lambda s: s.net_r).name if alpha else "—"
        worst_strat = min(alpha.values(), key=lambda s: s.net_r).name if alpha else "—"

        # Best trading hours (top 3 by avg_r)
        active_hours = {h: v for h, v in tod_map.items() if v["trades"] >= 2}
        best_hours   = sorted(active_hours.items(),
                               key=lambda x: x[1]["avg_r"], reverse=True)[:3]

        lines = [
            "═"*62,
            "  QUANT-SWARM-XAU  WEEKLY PERFORMANCE DIGEST",
            f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
            "═"*62,
            "",
            "  ── EQUITY CURVE METRICS ────────────────────────────────",
            f"  Initial Balance:    ${stats.initial:>10,.2f}",
            f"  Final Balance:      ${stats.final:>10,.2f}",
            f"  Net P&L:            ${stats.net_pnl:>+10,.2f}  "
            f"({stats.total_return_pct:>+.1f}%  /  "
            f"{stats.annualized_return:>+.1f}% ann.)",
            "",
            f"  Sharpe Ratio:       {stats.sharpe:>8.3f}",
            f"  Sortino Ratio:      {stats.sortino:>8.3f}",
            f"  Calmar Ratio:       {stats.calmar:>8.3f}",
            f"  Ulcer Index:        {stats.ulcer_index:>8.3f}",
            f"  Recovery Factor:    {stats.recovery_factor:>8.2f}",
            "",
            f"  Max Drawdown:       {stats.max_dd_pct:>7.2f}%  "
            f"(${stats.max_dd_usd:,.2f})  "
            f"Duration: {stats.max_dd_duration_h:.0f}h",
            "",
            f"  Profit Factor:      {stats.profit_factor:>8.3f}",
            f"  Win Rate:           {stats.win_rate*100:>7.1f}%",
            f"  Avg Win R:          {stats.avg_win_r:>+8.3f}R",
            f"  Avg Loss R:         {stats.avg_loss_r:>+8.3f}R",
            f"  Expectancy:         {stats.expectancy_r:>+8.4f}R  per trade",
            f"  Avg Hold Time:      {stats.avg_hold_hours:>7.1f}h",
            f"  Trades / Week:      {stats.trades_per_week:>7.1f}",
            "",
            "  ── STRATEGY ALPHA ATTRIBUTION ──────────────────────────",
        ]

        for name, sa in list(alpha.items())[:8]:
            icon = "🥇" if name == top_strat else ("⚠️" if name == worst_strat else "  ")
            lines.append(
                f"  {icon} {name[:22]:<22}  "
                f"T:{sa.trades:>3}  WR:{sa.win_rate*100:>4.0f}%  "
                f"PF:{sa.profit_factor:>4.2f}  "
                f"R:{sa.net_r:>+5.1f}  "
                f"Sh:{sa.sharpe:>+5.2f}  "
                f"[{sa.top_session}]"
            )

        lines += [
            "",
            f"  Top Alpha Strategy:  {top_strat}",
            f"  Needs Improvement:   {worst_strat}",
            "",
            "  ── R-MULTIPLE DISTRIBUTION ─────────────────────────────",
            f"  Count: {r_dist.get('count',0)}  "
            f"Mean: {r_dist.get('mean',0):>+.3f}R  "
            f"Std: {r_dist.get('std',0):.3f}  "
            f"Skew: {r_dist.get('skewness',0):>+.2f}  "
            f"Kurt: {r_dist.get('kurtosis',0):>+.2f}",
            f"  P5: {r_dist.get('p5',0):>+.2f}R  "
            f"P25: {r_dist.get('p25',0):>+.2f}R  "
            f"Median: {r_dist.get('median',0):>+.2f}R  "
            f"P75: {r_dist.get('p75',0):>+.2f}R  "
            f"P95: {r_dist.get('p95',0):>+.2f}R",
            f"  Best: {r_dist.get('max',0):>+.2f}R  "
            f"Worst: {r_dist.get('min',0):>+.2f}R",
            "",
        ]

        # Time of day heatmap (compact text)
        if any(v["trades"] > 0 for v in tod_map.values()):
            lines.append("  ── TIME-OF-DAY EDGE (UTC) ─────────────────────────────")
            hmap_line = "  "
            for h in range(24):
                d = tod_map.get(h, {})
                avg_r = d.get("avg_r", 0)
                if avg_r > 0.1:    sym = "▮"
                elif avg_r > 0:    sym = "▬"
                elif avg_r < -0.1: sym = "▪"
                else:              sym = "·"
                hmap_line += sym
            lines.append(hmap_line + "  (▮=strong ▬=mild ▪=weak ·=none)")
            lines.append("  " + "".join(f"{h//10}" for h in range(24)))
            lines.append("  " + "".join(f"{h%10}" for h in range(24)) + "  UTC hour")

            if best_hours:
                lines.append(f"\n  Best hours: " +
                              ", ".join(f"{h:02d}:00 (avg {v['avg_r']:>+.2f}R)"
                                        for h, v in best_hours))

        # Agent weights table
        if agent_weights:
            lines += [
                "",
                "  ── DYNAMIC AGENT WEIGHTS ───────────────────────────────",
            ]
            for aid, w in sorted(agent_weights.items(), key=lambda x: x[1], reverse=True):
                name = tuple(range(1, 22)).get(aid, {}).get("name", f"Agent{aid}")[:18]
                bar  = "█" * int(w * 10) + "░" * (20 - int(w * 10))
                lines.append(f"  {aid:02d} {name:<18} {bar}  {w:.3f}")

        lines.append("\n" + "═"*62)
        return "\n".join(lines)

    @staticmethod
    def format_telegram_weekly(stats: EquityCurveStats,
                                 alpha: dict[str, StrategyAlpha]) -> str:
        """Compact Telegram-formatted version of weekly digest."""
        top_strat   = max(alpha.values(), key=lambda s: s.net_r).name if alpha else "—"
        worst_strat = min(alpha.values(), key=lambda s: s.net_r).name if alpha else "—"
        result_icon = "✅" if stats.net_pnl >= 0 else "❌"

        strat_lines = ""
        for name, sa in sorted(alpha.items(),
                                key=lambda x: x[1].net_r, reverse=True)[:6]:
            icon = "🥇" if name == top_strat else ("⚠️" if name == worst_strat else "  ")
            strat_lines += (f"\n{icon} <b>{name[:18]}</b>  "
                             f"T:{sa.trades} WR:{sa.win_rate*100:.0f}% "
                             f"PF:{sa.profit_factor:.2f} "
                             f"NetR:{sa.net_r:>+.1f}")

        return (
            f"{result_icon} <b>WEEKLY DIGEST — QUANT-SWARM-XAU</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Net P&L: <code>${stats.net_pnl:>+,.2f}</code>  "
            f"({stats.total_return_pct:>+.1f}%)\n"
            f"📊 Sharpe: <code>{stats.sharpe:.2f}</code>  "
            f"PF: <code>{stats.profit_factor:.2f}</code>  "
            f"WR: <code>{stats.win_rate*100:.0f}%</code>\n"
            f"📉 Max DD: <code>{stats.max_dd_pct:.1f}%</code>  "
            f"Calmar: <code>{stats.calmar:.2f}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📈 <b>STRATEGY ALPHA</b>{strat_lines}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🥇 Top: <b>{top_strat}</b>  ⚠️ Review: <b>{worst_strat}</b>"
        )


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((v - mean)**2 for v in values) / len(values))


# ─── Demo ─────────────────────────────────────────────────────────────────────

def demo_analytics():
    import random, math as m

    print("\n" + "═"*62)
    print("  QUANT-SWARM-XAU: Performance Analytics Demo")
    print("═"*62)

    # Synthetic trade log (150 trades, positive expectancy)
    random.seed(42)
    base_ts = time.time() - 86400 * 30
    trades  = []
    strat_pool = ["SMC_FVG", "CRT_SWEEP", "HTF_SR", "ASIA_SWEEP",
                   "MEAN_REV", "TL_BREAK", "WYCKOFF", "DXY_DIV"]
    sessions = ["LONDON", "NY", "LONDON", "NY", "ASIA"]

    equity = 10_000.0
    eq_curve = [equity]

    for i in range(150):
        # Biased coin: 58% win rate, avg win 1.4R, avg loss 1.0R
        if random.random() < 0.58:
            r = random.gauss(1.4, 0.6)
            r = max(r, 0.05)
        else:
            r = random.gauss(-1.0, 0.3)
            r = min(r, -0.05)

        # Slight time-of-day bias (London/NY better)
        session = random.choice(sessions)
        entry_ts = base_ts + i * (86400 * 30 / 150)
        hold_h   = random.uniform(0.5, 12.0)

        pnl = r * 95.0   # ~$95 per 1R at 1% risk on $9.5K avg balance
        equity += pnl
        eq_curve.append(round(equity, 2))

        trades.append({
            "outcome_r":   round(r, 3),
            "net_pnl":     round(pnl, 2),
            "strategy":    random.choice(strat_pool),
            "session":     session,
            "entry_time":  entry_ts,
            "exit_time":   entry_ts + hold_h * 3600,
            "direction":   random.choice(["BUY", "SELL"]),
        })

    # Compute all stats
    stats  = PerformanceAnalytics.equity_curve_stats(eq_curve, trades, 30.0)
    alpha  = PerformanceAnalytics.strategy_alpha(trades)
    tod    = PerformanceAnalytics.time_of_day_heatmap(trades)
    r_dist = PerformanceAnalytics.r_distribution(trades)

    # Simulated weights for the locked 21-agent product roster.
    # This analytics-only sample has no dependency on retired runtimes.
    synthetic_agent_ids = tuple(range(1, 22))
    agent_weights = {
        agent_id: round(random.uniform(0.7, 1.9), 3)
        for agent_id in synthetic_agent_ids
    }
    agent_weights[7] = 1.78   # ICT always highest
    agent_weights[9] = 1.55   # Asia always strong

    # Print full report
    report = PerformanceAnalytics.format_weekly_digest(
        stats, alpha, tod, r_dist, agent_weights
    )
    print(report)

    # Telegram version preview
    print("\n[TELEGRAM PREVIEW]")
    tg = PerformanceAnalytics.format_telegram_weekly(stats, alpha)
    # Strip HTML tags for terminal
    import re
    tg_clean = re.sub(r"<[^>]+>", "", tg)
    print(tg_clean)


if __name__ == "__main__":
    demo_analytics()
