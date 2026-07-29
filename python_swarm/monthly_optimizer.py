#!/usr/bin/env python3
"""Search monthly backtest parameters and expose win-rate tradeoffs."""

import argparse
import random
import sys
from datetime import datetime, timedelta, timezone

from backtester import BacktestConfig, HistoricalDataProvider, SwarmBacktester

for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")


class ProfileBacktester(SwarmBacktester):
    def __init__(
        self,
        config: BacktestConfig,
        *,
        rr_target: float,
        sl_mult: float,
        allowed_tags: tuple[str, ...],
        max_hold_bars: int,
    ):
        super().__init__(config)
        self.rr_target = rr_target
        self.sl_mult = sl_mult
        self.allowed_tags = allowed_tags
        self.max_hold_bars = max_hold_bars

    def _generate_signal(self, bars: list[dict], idx: int):
        sig = super()._generate_signal(bars, idx)
        if not sig:
            return None
        if self.allowed_tags and not any(t in sig["tags"] for t in self.allowed_tags):
            return None

        entry = sig["entry"]
        old_sl = sig["sl"]
        direction = sig["direction"]
        sl_dist = abs(entry - old_sl)
        if sl_dist <= 0:
            return None

        new_sl_dist = sl_dist * self.sl_mult
        if direction == "BUY":
            sig["sl"] = round(entry - new_sl_dist, 2)
            sig["tp1"] = round(entry + new_sl_dist * self.rr_target, 2)
        else:
            sig["sl"] = round(entry + new_sl_dist, 2)
            sig["tp1"] = round(entry - new_sl_dist * self.rr_target, 2)
        return sig

    def run(self, bars: list[dict]) -> dict:
        open_trade = None
        daily_dd_halted = False

        for i, bar in enumerate(bars):
            day = int(bar["t"] // 86400)
            if day != self.last_day:
                self.daily_pnl = 0.0
                daily_dd_halted = False
                self.last_day = day

            if daily_dd_halted:
                continue

            if self.cfg.session_filter:
                bar_hour = int((bar["t"] % 86400) / 3600)
                if not (7 <= bar_hour < 20):
                    continue

            if open_trade:
                direction = open_trade["direction"]
                sl = open_trade["sl"]
                tp1 = open_trade["tp1"]
                hit_sl = (direction == "BUY" and bar["l"] <= sl) or (
                    direction == "SELL" and bar["h"] >= sl
                )
                hit_tp1 = (direction == "BUY" and bar["h"] >= tp1) or (
                    direction == "SELL" and bar["l"] <= tp1
                )
                timeout = (i - open_trade["entry_idx"]) >= self.max_hold_bars

                if hit_sl or hit_tp1 or timeout:
                    exit_price = sl if hit_sl else tp1 if hit_tp1 else bar["c"]
                    lot_size = open_trade["lot_size"]
                    raw_pnl = (exit_price - open_trade["fill_price"]) * lot_size * 100.0
                    if direction == "SELL":
                        raw_pnl = -raw_pnl
                    commission = self.cfg.commission_per_lot * lot_size
                    net_pnl = raw_pnl - commission
                    sl_dist = abs(open_trade["fill_price"] - open_trade["sl"])
                    outcome_r = (
                        net_pnl / (sl_dist * lot_size * 100.0) if sl_dist > 0 else 0.0
                    )

                    self.balance += net_pnl
                    self.daily_pnl += net_pnl
                    self.equity_curve.append(round(self.balance, 2))
                    if self.balance > self.peak_eq:
                        self.peak_eq = self.balance
                    dd = self.peak_eq - self.balance
                    if dd > self.max_dd:
                        self.max_dd = dd
                        self.max_dd_pct = dd / self.peak_eq * 100

                    daily_dd_pct = abs(self.daily_pnl) / (
                        self.balance - self.daily_pnl
                    ) * 100
                    if self.daily_pnl < 0 and daily_dd_pct >= self.cfg.max_daily_dd_pct:
                        daily_dd_halted = True

                    from backtester import BacktestTrade, StrategyStats

                    strat = "+".join(open_trade["tags"][:2])
                    trade = BacktestTrade(
                        trade_id=f"BT_{i:05d}",
                        agent_id=open_trade["agent_id"],
                        strategy=strat,
                        direction=direction,
                        entry_time=open_trade["entry_bar"]["t"],
                        exit_time=bar["t"],
                        entry_price=open_trade["fill_price"],
                        exit_price=exit_price,
                        sl_price=open_trade["sl"],
                        tp1_price=open_trade["tp1"],
                        lot_size=lot_size,
                        gross_pnl=raw_pnl,
                        commission=commission,
                        net_pnl=net_pnl,
                        outcome_r=round(outcome_r, 3),
                        slippage_pts=open_trade["slippage_pts"],
                        session=self._bar_session(bar),
                        tags=open_trade["tags"],
                        confluence=open_trade["confluence"],
                    )
                    self.trades.append(trade)

                    if strat not in self.strat_stats:
                        self.strat_stats[strat] = StrategyStats(
                            name=strat,
                            peak_equity=self.cfg.initial_balance,
                            equity_curve=[self.cfg.initial_balance],
                        )
                    ss = self.strat_stats[strat]
                    ss.trades += 1
                    ss.net_r += outcome_r
                    ss.equity_curve.append(ss.equity_curve[-1] + net_pnl)
                    if net_pnl > 0:
                        ss.wins += 1
                        ss.gross_profit += abs(net_pnl)
                    else:
                        ss.losses += 1
                        ss.gross_loss += abs(net_pnl)
                    if ss.equity_curve[-1] > ss.peak_equity:
                        ss.peak_equity = ss.equity_curve[-1]
                    ss_dd = ss.peak_equity - ss.equity_curve[-1]
                    if ss_dd > ss.max_drawdown:
                        ss.max_drawdown = ss_dd
                        ss.max_drawdown_pct = ss_dd / ss.peak_equity * 100

                    open_trade = None
                continue

            sig = self._generate_signal(bars, i)
            if not sig:
                continue

            lot_size = self._calculate_lot_size(sig["entry"], sig["sl"], sig["confluence"])
            if lot_size < self.cfg.min_lot:
                continue
            spread = self.cfg.spread_pts * 0.01
            slip_pts = random.uniform(0, self.cfg.slippage_pts)
            slip = slip_pts * 0.01
            fill = (
                bar["o"] + spread + slip
                if sig["direction"] == "BUY"
                else bar["o"] - spread - slip
            )

            open_trade = {
                "direction": sig["direction"],
                "fill_price": round(fill, 2),
                "sl": sig["sl"],
                "tp1": sig["tp1"],
                "lot_size": lot_size,
                "tags": sig["tags"],
                "agent_id": sig["agent_id"],
                "confluence": sig["confluence"],
                "slippage_pts": round(slip_pts, 1),
                "entry_idx": i,
                "entry_bar": bar,
            }

        return self._compile_report()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2026-06-01")
    parser.add_argument("--end", default="2026-06-30")
    parser.add_argument("--seed", type=int, default=202606)
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    n_bars = int(((end + timedelta(days=1)) - start).total_seconds() // 3600)

    random.seed(args.seed)
    bars = HistoricalDataProvider.generate_synthetic(
        n_bars=n_bars,
        tf_mins=60,
        start_price=2685,
        annual_vol=0.15,
        trend_drift=0.05,
        start_ts=start.timestamp(),
    )

    tag_profiles = [
        (),
        ("CRT_SWEEP",),
        ("BB_UPPER", "BB_LOWER"),
        ("CRT_SWEEP", "BB_UPPER", "BB_LOWER"),
    ]
    rows = []
    for conf in (70, 75, 80, 85, 90, 95):
        for rr in (0.05, 0.08, 0.10, 0.15, 0.20, 0.30, 0.50, 0.75, 1.00):
            for sl_mult in (1.0, 1.5, 2.0, 3.0, 4.0):
                for max_hold in (6, 12, 24, 48):
                    for tags in tag_profiles:
                        random.seed(args.seed)
                        cfg = BacktestConfig(
                            initial_balance=10000,
                            risk_pct=1,
                            spread_pts=3,
                            slippage_pts=5,
                            min_confluence=conf,
                            session_filter=True,
                        )
                        bt = ProfileBacktester(
                            cfg,
                            rr_target=rr,
                            sl_mult=sl_mult,
                            allowed_tags=tags,
                            max_hold_bars=max_hold,
                        )
                        report = bt.run(bars)
                        if "summary" not in report:
                            continue
                        s = report["summary"]
                        rows.append(
                            {
                                "wr": s["win_rate_pct"],
                                "pf": s["profit_factor"],
                                "trades": s["total_trades"],
                                "return": s["total_return_pct"],
                                "dd": s["max_drawdown_pct"],
                                "net": s["net_profit_usd"],
                                "conf": conf,
                                "rr": rr,
                                "sl": sl_mult,
                                "hold": max_hold,
                                "tags": "+".join(tags) or "ALL",
                            }
                        )

    print("Top by win rate, minimum 20 trades:")
    for row in sorted(
        [r for r in rows if r["trades"] >= 20],
        key=lambda r: (r["wr"], r["pf"], r["return"]),
        reverse=True,
    )[:20]:
        print(row)

    print("\nTop by profit factor, minimum 20 trades:")
    for row in sorted(
        [r for r in rows if r["trades"] >= 20],
        key=lambda r: (r["pf"], r["return"], r["wr"]),
        reverse=True,
    )[:20]:
        print(row)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
