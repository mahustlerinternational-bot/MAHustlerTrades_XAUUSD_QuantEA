#!/usr/bin/env python3
"""Run a reproducible one-month synthetic backtest for QUANT-SWARM-XAU."""

import argparse
import json
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from backtester import (
    BACKTEST_DIR,
    BacktestConfig,
    HistoricalDataProvider,
    SwarmBacktester,
)

for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")


def _parse_day(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _money(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}${abs(value):,.2f}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run an accelerated synthetic month backtest."
    )
    parser.add_argument("--start", default="2026-06-01")
    parser.add_argument("--end", default="2026-06-30")
    parser.add_argument("--seed", type=int, default=202606)
    parser.add_argument("--price", type=float, default=2685.0)
    parser.add_argument("--balance", type=float, default=10000.0)
    parser.add_argument("--risk", type=float, default=1.0)
    parser.add_argument("--max-daily-dd", type=float, default=3.0)
    parser.add_argument("--min-confluence", type=float, default=75.0)
    parser.add_argument("--min-lot", type=float, default=0.01)
    parser.add_argument("--lot-step", type=float, default=0.01)
    parser.add_argument(
        "--allow-min-lot-overrisk",
        action="store_true",
        help="Allow rounding up to broker min lot even when it exceeds risk budget.",
    )
    args = parser.parse_args()

    start_dt = _parse_day(args.start)
    end_day = _parse_day(args.end)
    end_exclusive = end_day + timedelta(days=1)
    if end_exclusive <= start_dt:
        raise SystemExit("--end must be on or after --start")

    n_bars = int((end_exclusive - start_dt).total_seconds() // 3600)
    random.seed(args.seed)

    cfg = BacktestConfig(
        initial_balance=args.balance,
        risk_pct=args.risk,
        max_daily_dd_pct=args.max_daily_dd,
        spread_pts=3,
        slippage_pts=5,
        commission_per_lot=7.0,
        min_confluence=args.min_confluence,
        session_filter=True,
        min_lot=args.min_lot,
        lot_step=args.lot_step,
        skip_oversized_min_lot=not args.allow_min_lot_overrisk,
        n_monte_carlo=500,
    )

    bars = HistoricalDataProvider.generate_synthetic(
        n_bars=n_bars,
        tf_mins=60,
        start_price=args.price,
        annual_vol=0.15,
        trend_drift=0.05,
        start_ts=start_dt.timestamp(),
    )

    bt = SwarmBacktester(cfg)
    report = bt.run(bars)
    report["n_bars_tested"] = len(bars)
    report["date_start_utc"] = start_dt.strftime("%Y-%m-%d %H:%M")
    report["date_end_utc"] = (end_exclusive - timedelta(hours=1)).strftime(
        "%Y-%m-%d %H:%M"
    )
    report["data_source"] = "synthetic_gbm_mean_reversion"
    report["random_seed"] = args.seed
    if "error" not in report:
        report["monte_carlo"] = bt.monte_carlo_test(cfg.n_monte_carlo)

    BACKTEST_DIR.mkdir(exist_ok=True)
    stem = f"monthly_backtest_{args.start}_to_{args.end}_seed_{args.seed}"
    report_path = BACKTEST_DIR / f"{stem}.json"
    trade_path = BACKTEST_DIR / f"{stem}_trades.csv"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    saved_trade_path = bt.save_trade_log(str(trade_path))

    print("=" * 72)
    print("QUANT-SWARM-XAU MONTHLY SYNTHETIC BACKTEST")
    print("=" * 72)
    print(f"Period UTC:       {args.start} 00:00 -> {args.end} 23:00")
    print(f"Bars tested:      {n_bars} H1")
    print(f"Seed:             {args.seed}")
    print(f"Start price:      ${args.price:,.2f}")
    print(f"Initial balance:  {_money(args.balance)}")
    print(f"Risk/confluence:  {args.risk:.2f}% / {args.min_confluence:.0f}%")
    print(f"Session filter:   London/NY only")

    if "error" in report:
        print(f"Result:           {report['error']}")
        risk = report.get("risk_sizing", {})
        print(f"Signals seen:     {risk.get('signals_seen', 0)}")
        print(f"Skipped min lot:  {risk.get('skipped_min_lot', 0)}")
    else:
        s = report["summary"]
        print("-" * 72)
        print(f"Final balance:    {_money(s['final_balance'])}")
        print(f"Net PnL:          {_money(s['net_profit_usd'])}")
        print(f"Return:           {s['total_return_pct']:+.2f}%")
        print(f"Max drawdown:     {s['max_drawdown_pct']:.2f}% ({_money(s['max_drawdown_usd'])})")
        print(f"Trades:           {s['total_trades']}")
        print(f"Win rate:         {s['win_rate_pct']:.1f}%")
        print(f"Profit factor:    {s['profit_factor']:.2f}")
        print(f"Sharpe ratio:     {s['sharpe_ratio']:+.2f}")
        print(f"Avg R:            {s['avg_r_multiple']:+.3f}R")
        print(f"Net R:            {sum(t.outcome_r for t in bt.trades):+.2f}R")
        print(f"Max win/loss run: {s['max_consec_wins']}W / {s['max_consec_losses']}L")
        print(f"Commission:       {_money(s['total_commission'])}")
        risk = report.get("risk_sizing", {})
        print(f"Signals seen:     {risk.get('signals_seen', 0)}")
        print(f"Skipped min lot:  {risk.get('skipped_min_lot', 0)}")

        if report.get("session_breakdown"):
            print("-" * 72)
            print("Session breakdown:")
            for name, sess in report["session_breakdown"].items():
                print(
                    f"  {name:<8} trades={sess['trades']:>3} "
                    f"WR={sess['win_rate']:>5.1f}% "
                    f"PnL={_money(sess['net_pnl'])}"
                )

        if bt.trades:
            print("-" * 72)
            print("First/last trades:")
            for trade in (bt.trades[:3] + bt.trades[-3:]):
                entry = datetime.fromtimestamp(
                    trade.entry_time, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M")
                print(
                    f"  {entry} {trade.direction:<4} {trade.strategy:<24} "
                    f"{trade.outcome_r:+.2f}R {_money(trade.net_pnl)}"
                )

    print("-" * 72)
    print(f"Report JSON:      {report_path}")
    if saved_trade_path:
        print(f"Trade CSV:        {saved_trade_path}")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
