#!/usr/bin/env python3
"""Run a small-account risk matrix for $100-$300 XAUUSD accounts."""

import argparse
import json
import random
import sys
from datetime import datetime, timedelta, timezone

from backtester import (
    BACKTEST_DIR,
    BacktestConfig,
    HistoricalDataProvider,
    SwarmBacktester,
)

for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")


DEFAULT_PERIODS = [
    ("2025", "2025-01-01", "2025-12-31", 2025),
    ("2026YTD", "2026-01-01", "2026-07-06", 20260706),
]


def _parse_day(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _parse_float_list(value: str) -> list[float]:
    return [float(item.strip()) for item in value.split(",") if item.strip()]


def _period_bars(start: str, end: str, price: float, seed: int,
                 tf_mins: int) -> list[dict]:
    start_dt = _parse_day(start)
    end_exclusive = _parse_day(end) + timedelta(days=1)
    n_bars = int((end_exclusive - start_dt).total_seconds() // (tf_mins * 60))
    random.seed(seed)
    return HistoricalDataProvider.generate_synthetic(
        n_bars=n_bars,
        tf_mins=tf_mins,
        start_price=price,
        annual_vol=0.15,
        trend_drift=0.05,
        start_ts=start_dt.timestamp(),
    )


def _summary_row(period: str, balance: float, risk: float,
                 report: dict) -> dict:
    summary = report.get("summary", {})
    sizing = report.get("risk_sizing", {})
    return {
        "period": period,
        "balance": balance,
        "risk_pct": risk,
        "trades": summary.get("total_trades", 0),
        "signals": sizing.get("signals_seen", 0),
        "skipped_min_lot": sizing.get("skipped_min_lot", 0),
        "win_rate_pct": summary.get("win_rate_pct", 0.0),
        "profit_factor": summary.get("profit_factor", 0.0),
        "return_pct": summary.get("total_return_pct", 0.0),
        "max_dd_pct": summary.get("max_drawdown_pct", 0.0),
        "final_balance": summary.get("final_balance", balance),
        "error": report.get("error", ""),
    }


def _run_case(period: str, bars: list[dict], balance: float, risk: float,
              min_confluence: float, max_daily_dd: float, min_lot: float,
              lot_step: float, max_lot: float,
              allow_min_lot_overrisk: bool, precision_scalp: bool,
              precision_max_sl: float, cooldown_bars: int,
              max_trades_per_day: int, tf_mins: int, seed: int) -> dict:
    cfg = BacktestConfig(
        initial_balance=balance,
        risk_pct=risk,
        max_daily_dd_pct=max_daily_dd,
        spread_pts=3,
        slippage_pts=5,
        commission_per_lot=7.0,
        min_confluence=min_confluence,
        session_filter=True,
        min_lot=min_lot,
        lot_step=lot_step,
        max_lot=max_lot,
        skip_oversized_min_lot=not allow_min_lot_overrisk,
        precision_scalp=precision_scalp,
        precision_tf_label=f"M{tf_mins}",
        precision_max_sl=precision_max_sl,
        cooldown_bars=cooldown_bars,
        max_trades_per_day=max_trades_per_day,
        n_monte_carlo=0,
    )
    random.seed(seed)
    bt = SwarmBacktester(cfg)
    report = bt.run(bars)
    report["period"] = period
    report["n_bars_tested"] = len(bars)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run $100-$300 risk suitability matrix."
    )
    parser.add_argument("--balances", default="100,200,300")
    parser.add_argument("--risks", default="0.25,0.50,1.00")
    parser.add_argument("--price", type=float, default=2685.0)
    parser.add_argument("--max-daily-dd", type=float, default=2.0)
    parser.add_argument("--min-confluence", type=float, default=80.0)
    parser.add_argument("--min-lot", type=float, default=0.01)
    parser.add_argument("--lot-step", type=float, default=None)
    parser.add_argument("--max-lot", type=float, default=None)
    parser.add_argument("--tf-mins", type=int, default=60)
    parser.add_argument("--precision-scalp", action="store_true")
    parser.add_argument("--precision-max-sl", type=float, default=0.80)
    parser.add_argument("--cooldown-bars", type=int, default=0)
    parser.add_argument("--max-trades-per-day", type=int, default=0)
    parser.add_argument("--allow-min-lot-overrisk", action="store_true")
    parser.add_argument(
        "--include-june",
        action="store_true",
        help="Also test synthetic June 2026.",
    )
    args = parser.parse_args()

    balances = _parse_float_list(args.balances)
    risks = _parse_float_list(args.risks)
    lot_step = args.lot_step if args.lot_step is not None else args.min_lot
    max_lot = args.max_lot if args.max_lot is not None else (
        args.min_lot if args.precision_scalp else 5.0
    )
    periods = list(DEFAULT_PERIODS)
    if args.include_june:
        periods.append(("June2026", "2026-06-01", "2026-06-30", 202606))

    rows = []
    details = []
    for period, start, end, market_seed in periods:
        bars = _period_bars(start, end, args.price, market_seed, args.tf_mins)
        for balance in balances:
            for risk in risks:
                case_seed = market_seed + int(balance * 100) + int(risk * 1000)
                report = _run_case(
                    period=period,
                    bars=bars,
                    balance=balance,
                    risk=risk,
                    min_confluence=args.min_confluence,
                    max_daily_dd=args.max_daily_dd,
                    min_lot=args.min_lot,
                    lot_step=lot_step,
                    max_lot=max_lot,
                    allow_min_lot_overrisk=args.allow_min_lot_overrisk,
                    precision_scalp=args.precision_scalp,
                    precision_max_sl=args.precision_max_sl,
                    cooldown_bars=args.cooldown_bars,
                    max_trades_per_day=args.max_trades_per_day,
                    tf_mins=args.tf_mins,
                    seed=case_seed,
                )
                rows.append(_summary_row(period, balance, risk, report))
                details.append(report)

    output = {
        "settings": {
            "balances": balances,
            "risks": risks,
            "max_daily_dd": args.max_daily_dd,
            "min_confluence": args.min_confluence,
            "min_lot": args.min_lot,
            "lot_step": lot_step,
            "max_lot": max_lot,
            "tf_mins": args.tf_mins,
            "precision_scalp": args.precision_scalp,
            "precision_max_sl": args.precision_max_sl,
            "cooldown_bars": args.cooldown_bars,
            "max_trades_per_day": args.max_trades_per_day,
            "allow_min_lot_overrisk": args.allow_min_lot_overrisk,
        },
        "rows": rows,
        "details": details,
    }
    BACKTEST_DIR.mkdir(exist_ok=True)
    suffix = "allow_overrisk" if args.allow_min_lot_overrisk else "guarded"
    lot_label = f"minlot_{args.min_lot:g}_step_{lot_step:g}".replace(".", "p")
    conf_label = f"conf_{args.min_confluence:g}".replace(".", "p")
    mode_label = "precision" if args.precision_scalp else "standard"
    out_path = BACKTEST_DIR / f"small_account_matrix_{suffix}_{lot_label}_{conf_label}_{mode_label}_tf{args.tf_mins}.json"
    out_path.write_text(json.dumps(output, indent=2), encoding="utf-8")

    print("=" * 116)
    print("QUANT-SWARM-XAU SMALL-ACCOUNT RISK MATRIX")
    print("=" * 116)
    print(
        f"Risk guard: {'OFF' if args.allow_min_lot_overrisk else 'ON'} | "
        f"Min lot: {args.min_lot:.3f} | Lot step: {lot_step:.3f} | "
        f"Max lot: {max_lot:.3f} | "
        f"Daily DD: {args.max_daily_dd:.2f}% | "
        f"Confluence: {args.min_confluence:.0f}% | "
        f"Mode: {mode_label} | TF: M{args.tf_mins}"
    )
    print("-" * 116)
    print(
        f"{'Period':<9} {'Bal':>7} {'Risk':>6} {'Trades':>7} {'Signals':>8} "
        f"{'Skipped':>8} {'WR':>6} {'PF':>6} {'Ret':>8} {'DD':>7} {'Final':>10}"
    )
    print("-" * 116)
    for row in rows:
        print(
            f"{row['period']:<9} "
            f"${row['balance']:>6.0f} "
            f"{row['risk_pct']:>5.2f}% "
            f"{row['trades']:>7} "
            f"{row['signals']:>8} "
            f"{row['skipped_min_lot']:>8} "
            f"{row['win_rate_pct']:>5.1f}% "
            f"{row['profit_factor']:>6.2f} "
            f"{row['return_pct']:>+7.2f}% "
            f"{row['max_dd_pct']:>6.2f}% "
            f"${row['final_balance']:>9.2f}"
        )
    print("-" * 116)
    print(f"Saved JSON: {out_path}")
    print("=" * 116)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
