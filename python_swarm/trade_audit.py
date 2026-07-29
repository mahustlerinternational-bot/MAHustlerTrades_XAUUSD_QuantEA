#!/usr/bin/env python3
"""
QUANT-SWARM-XAU v3.0 — Trade Pipeline Audit
Checks every gate in the execution chain and reports what's passing or blocking.
Run: python3 trade_audit.py
"""

import json
import sqlite3
import time
import os
from pathlib import Path
from datetime import datetime, timezone

BASE   = Path(__file__).parent
STATE  = BASE / "journal_db_sim" / "live_state.json"
DBDIR  = BASE / "journal_db_sim"
LOG    = BASE / "logs" / "swarm.log"

# ── Colours ────────────────────────────────────────────────────────────────────
G  = "\033[92m"   # green
R  = "\033[91m"   # red
Y  = "\033[93m"   # yellow
B  = "\033[94m"   # blue
M  = "\033[95m"   # magenta
C  = "\033[96m"   # cyan
DIM= "\033[2m"
W  = "\033[0m"    # reset
BOLD="\033[1m"

def ok(msg):   print(f"  {G}✅ {msg}{W}")
def fail(msg): print(f"  {R}❌ {msg}{W}")
def warn(msg): print(f"  {Y}⚠  {msg}{W}")
def info(msg): print(f"  {B}ℹ  {msg}{W}")
def head(msg): print(f"\n{BOLD}{C}{'─'*58}{W}\n{BOLD}  {msg}{W}\n{C}{'─'*58}{W}")

# ── Load live state ─────────────────────────────────────────────────────────────
def load_state():
    try:
        return json.loads(STATE.read_text())
    except Exception as e:
        return {}

# ── Agent signal counts from DB ─────────────────────────────────────────────────
def agent_signal_stats(lookback_mins=60):
    since = time.time() - lookback_mins * 60
    results = []
    for db_file in sorted(DBDIR.glob("agent_*.db")):
        agent_name = db_file.stem
        try:
            conn = sqlite3.connect(str(db_file), timeout=3)
            row = conn.execute("""
                SELECT COUNT(*), MAX(timestamp)
                FROM signals
                WHERE timestamp >= ?
            """, (since,)).fetchone()
            total_row = conn.execute("SELECT COUNT(*) FROM signals").fetchone()
            conn.close()
            count     = row[0] if row else 0
            last_sig  = row[1] if row and row[1] else None
            total     = total_row[0] if total_row else 0
            results.append((agent_name, count, last_sig, total))
        except Exception as e:
            results.append((agent_name, -1, None, -1))
    return results

# ── Recent trade submissions from log ───────────────────────────────────────────
def recent_trades_from_log(n=20):
    trades = []
    gate_blocks = []
    try:
        lines = LOG.read_text(errors="replace").splitlines()
        for line in reversed(lines):
            if "EVOLVED TRADE SUBMITTED" in line:
                trades.append(line.strip())
            if "Below confluence" in line or "confluence_pct" in line.lower():
                gate_blocks.append(line.strip())
            if "approve_trade" in line or "Daily halt" in line or "Max positions" in line:
                gate_blocks.append(line.strip())
            if len(trades) >= n and len(gate_blocks) >= n:
                break
    except Exception:
        pass
    return trades[:n], gate_blocks[:n]

# ── Risk DB check ────────────────────────────────────────────────────────────────
def check_risk_db():
    db = DBDIR / "risk_ledger.db"
    try:
        conn = sqlite3.connect(str(db), timeout=3)
        rows = conn.execute("""
            SELECT event_type, direction, lot_size, risk_usd, timestamp
            FROM risk_ledger ORDER BY timestamp DESC LIMIT 10
        """).fetchall()
        conn.close()
        return rows
    except Exception:
        return []

# ── Confluence DB check ──────────────────────────────────────────────────────────
def check_agent_outcomes():
    results = {}
    for db_file in DBDIR.glob("agent_*.db"):
        try:
            conn = sqlite3.connect(str(db_file), timeout=3)
            row = conn.execute("""
                SELECT COUNT(*), AVG(outcome_r)
                FROM trade_outcomes
            """).fetchone()
            conn.close()
            if row and row[0] > 0:
                results[db_file.stem] = {"trades": row[0], "avg_r": round(row[1],3)}
        except Exception:
            pass
    return results

# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{BOLD}{M}{'═'*58}")
print(f"  QUANT-SWARM-XAU v3.0 — TRADE PIPELINE AUDIT")
print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"{'═'*58}{W}")

state = load_state()

# ─── GATE 0: Live State ───────────────────────────────────────────────────────
head("GATE 0 — Live State / MT5 Connection")
if not state:
    fail("live_state.json not found — swarm not running")
else:
    ea   = state.get("ea_connected", False)
    bid  = float(state.get("bid", 0))
    ask  = float(state.get("ask", 0))
    sess = state.get("session", "?")
    reg  = state.get("regime", "?")
    ts   = state.get("timestamp", 0)
    age  = time.time() - (ts if isinstance(ts, float) else time.time())

    if ea and bid > 0:
        ok(f"EA connected | bid={bid:.3f} ask={ask:.3f}")
    elif not ea:
        fail("EA NOT connected — no tick data flowing")
    else:
        warn(f"EA connected but bid=0 — no ticks received yet")

    if age < 5:
        ok(f"State fresh — {age:.1f}s old")
    else:
        warn(f"State stale — {age:.1f}s old (swarm may be stuck)")

    info(f"Session: {sess}  |  Regime: {reg}")

# ─── GATE 1: Session / Time Guard ─────────────────────────────────────────────
head("GATE 1 — Session / Time Guard")
sess = state.get("session", "OFF")
h_utc = datetime.now(timezone.utc).hour
if sess in ("LONDON", "NY"):
    ok(f"Active session: {sess} (hour={h_utc} UTC)")
elif sess == "ASIA":
    warn(f"ASIA session (hour={h_utc} UTC) — some agents may be inactive")
else:
    fail(f"Session is OFF (hour={h_utc} UTC) — time guard will block all trades")
    info("London opens 07:00-12:00 UTC | NY opens 13:00-20:00 UTC")

# ─── GATE 2: Spread Check ─────────────────────────────────────────────────────
head("GATE 2 — Spread Check")
spread = round(float(state.get("ask",0)) - float(state.get("bid",0)), 3)
# ResilienceController typically blocks > 50 pts spread on XAUUSD
SPREAD_LIMIT = 50.0
if spread <= 0:
    warn("Spread = 0 (no tick data yet)")
elif spread < SPREAD_LIMIT:
    ok(f"Spread {spread:.3f} pts — within limit (<{SPREAD_LIMIT})")
else:
    fail(f"Spread {spread:.3f} pts — EXCEEDS LIMIT ({SPREAD_LIMIT}) — blocked")

# ─── GATE 3: Macro Freeze ─────────────────────────────────────────────────────
head("GATE 3 — Macro / News Freeze")
macro_db = DBDIR / "macro_events.db"
macro_frozen = False
upcoming_high = []
try:
    conn = sqlite3.connect(str(macro_db), timeout=3)
    now = time.time()
    rows = conn.execute("""
        SELECT name, scheduled_utc, impact, impact_score
        FROM events
        WHERE scheduled_utc BETWEEN ? AND ?
        AND currency IN ('USD','XAU')
        ORDER BY scheduled_utc
    """, (now - 1800, now + 3600)).fetchall()
    conn.close()
    for name, sched, impact, score in rows:
        mins = (sched - now) / 60
        if score and float(score) > 7.0:
            upcoming_high.append((name, mins, impact))
except Exception:
    pass

if upcoming_high:
    warn("High-impact news nearby:")
    for name, mins, impact in upcoming_high:
        tag = f"+{abs(int(mins))}m" if mins > 0 else f"{int(mins)}m ago"
        warn(f"  {name} [{impact}] {tag}")
else:
    ok("No high-impact news in ±60 min window")

# ─── GATE 4: Open Positions ───────────────────────────────────────────────────
head("GATE 4 — Open Positions")
positions = int(state.get("positions", 0))
if positions == 0:
    ok("No open positions — signal evaluation will proceed")
else:
    warn(f"{positions} open position(s) — analysis loop is managing them, NOT seeking new entries")
    info("New signals blocked until all positions are closed")

# ─── GATE 5: Signal Generation ────────────────────────────────────────────────
head("GATE 5 — Agent Signal Generation (last 60 min)")
sig_stats = agent_signal_stats(lookback_mins=60)
active_agents   = [(n,c,l,t) for n,c,l,t in sig_stats if c > 0]
inactive_agents = [(n,c,l,t) for n,c,l,t in sig_stats if c == 0 and t >= 0]
error_agents    = [(n,c,l,t) for n,c,l,t in sig_stats if c < 0]

print(f"\n  {BOLD}Active agents ({len(active_agents)}/16):{W}")
for name, count, last_ts, total in active_agents:
    mins_ago = (time.time() - last_ts) / 60 if last_ts else 999
    agent_short = name.replace("agent_","").replace("_"," ")
    print(f"  {G}  ▸ {agent_short:<35}{W} {count:>3} signals | last {mins_ago:.0f}m ago | total={total}")

if inactive_agents:
    print(f"\n  {DIM}Inactive agents (0 signals in last 60m):{W}")
    for name, _, _, total in inactive_agents:
        agent_short = name.replace("agent_","").replace("_"," ")
        print(f"  {DIM}  · {agent_short:<35} total ever={total}{W}")

if len(active_agents) >= 3:
    ok(f"\n  {len(active_agents)} active agents ≥ 3 required — signal fusion will proceed")
elif len(active_agents) > 0:
    fail(f"\n  Only {len(active_agents)} active agents — need ≥3 for fusion gate to open")
else:
    fail("\n  0 active agents generating signals — nothing to fuse")

# ─── GATE 6: Confluence Gate ──────────────────────────────────────────────────
head("GATE 6 — Confluence Gate")
gate = float(state.get("confluence_gate", 75))
regime = state.get("regime","unknown")
info(f"Current regime: {regime.replace('_',' ').upper()}")
info(f"Regime-adjusted confluence gate: {gate}%")
info(f"Default gate (no regime): 75%")
if gate < 75:
    ok(f"Gate LOWERED to {gate}% by regime router — easier to trigger trades")
elif gate > 75:
    warn(f"Gate RAISED to {gate}% by regime router — harder to trigger trades")
else:
    info(f"Gate at default {gate}%")

# ─── GATE 7: Risk Controller ──────────────────────────────────────────────────
head("GATE 7 — Risk Controller")
equity  = float(state.get("equity", 0))
balance = float(state.get("balance", 0))
daily_pnl = float(state.get("daily_pnl", 0))
atr     = float(state.get("atr", 0))
wins    = int(state.get("wins", 0))
losses  = int(state.get("losses", 0))
trades  = int(state.get("trade_count", 0))

# InstitutionalRiskController limits (from risk_and_lifecycle.py defaults)
MAX_DAILY_LOSS_PCT = 3.0
MAX_CONSEC_LOSSES  = 3
daily_loss_pct = abs(min(daily_pnl, 0)) / max(balance, 1) * 100

if daily_loss_pct >= MAX_DAILY_LOSS_PCT:
    fail(f"Daily loss {daily_loss_pct:.2f}% ≥ {MAX_DAILY_LOSS_PCT}% limit — TRADING HALTED")
else:
    ok(f"Daily loss {daily_loss_pct:.2f}% < {MAX_DAILY_LOSS_PCT}% limit")

if daily_pnl >= 0:
    ok(f"Daily PnL +${daily_pnl:.2f} — positive session")
else:
    warn(f"Daily PnL -${abs(daily_pnl):.2f}")

if atr > 0:
    ok(f"ATR H1 = {atr:.2f} pts — volatility data available for lot sizing")
else:
    warn("ATR = 0 — lot sizing may use fallback values")

info(f"Equity: ${equity:.2f}  |  Balance: ${balance:.2f}")
info(f"Trades today: {trades}  |  W:{wins} L:{losses}  |  WR: {f'{wins/(wins+losses)*100:.0f}%' if (wins+losses) > 0 else 'N/A'}")

# ─── GATE 8: MT5 TCP Connection ───────────────────────────────────────────────
head("GATE 8 — MT5 TCP / Command Delivery")
if state.get("ea_connected"):
    ok("TCP connection active — OPEN commands will be delivered to MT5 EA")
else:
    fail("MT5 not connected — trades will be queued on bus (not executed)")

# ─── Recent Trade Activity ───────────────────────────────────────────────────
head("Recent Trade Log")
trades_log, gate_blocks = recent_trades_from_log(10)
if trades_log:
    ok(f"Found {len(trades_log)} trade submission(s) in log:")
    for t in trades_log[:5]:
        print(f"  {G}  {t[-120:]}{W}")
else:
    warn("No 'EVOLVED TRADE SUBMITTED' entries found in log")
    info("Either no trades fired yet, or log was rotated")

risk_rows = check_risk_db()
if risk_rows:
    ok(f"Risk ledger has {len(risk_rows)} event(s) — trades are being risk-validated:")
    for ev, direction, lot, risk_usd, ts in risk_rows[:3]:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S")
        print(f"  {G}  [{dt}] {ev} {direction} {lot}lot ${risk_usd:.2f} risk{W}")
else:
    info("Risk ledger empty — no trades approved yet this session")

# ─── Agent Outcomes ────────────────────────────────────────────────────────────
outcomes = check_agent_outcomes()
if outcomes:
    head("Agent Trade Outcomes (all time)")
    for agent, data in sorted(outcomes.items()):
        r_col = G if data["avg_r"] > 0 else R
        print(f"  {r_col}  {agent:<40} trades={data['trades']} avg_R={data['avg_r']:.3f}{W}")

# ─── Verdict ──────────────────────────────────────────────────────────────────
head("AUDIT VERDICT")

gates_open = []
gates_blocked = []

if state.get("ea_connected") and float(state.get("bid",0)) > 0:
    gates_open.append("MT5 connected")
else:
    gates_blocked.append("MT5 not connected")

if sess in ("LONDON", "NY", "ASIA"):
    gates_open.append(f"Session active ({sess})")
else:
    gates_blocked.append("Session OFF")

if positions == 0:
    gates_open.append("No open positions")
else:
    gates_blocked.append(f"{positions} position(s) open — blocking new entries")

if len(active_agents) >= 3:
    gates_open.append(f"{len(active_agents)} agents generating signals")
else:
    gates_blocked.append(f"Only {len(active_agents)}/3 required agents active")

if daily_loss_pct < MAX_DAILY_LOSS_PCT:
    gates_open.append("Risk limits clear")
else:
    gates_blocked.append("Daily loss limit hit — halted")

if not gates_blocked:
    print(f"\n  {G}{BOLD}✅ ALL GATES OPEN — swarm CAN place trades{W}")
    print(f"  {G}  Trades will fire when confluence score ≥ {gate}%{W}")
    if len(active_agents) < 5:
        warn(f"  Only {len(active_agents)} agents active — confluence may be low; wait for more agents")
    if atr == 0:
        warn("  ATR=0 — lot sizes will be minimal until H1 bar completes")
else:
    print(f"\n  {R}{BOLD}⛔ TRADE BLOCKED — gates not passing:{W}")
    for b in gates_blocked:
        fail(b)
    if gates_open:
        print(f"\n  {G}  Gates that ARE open:{W}")
        for g in gates_open:
            ok(g)

print(f"\n{DIM}  Run again in 60s to see if signal count changes{W}\n")
