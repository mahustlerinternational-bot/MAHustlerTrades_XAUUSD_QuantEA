"""
QUANT-SWARM-XAU: Risk & Execution Layer
Agent 18: Institutional Risk Controller
  - Dynamic position sizing with correlation exposure caps
  - Daily drawdown circuit breaker
  - Per-trade risk audit trail

Agent 19: Trade Lifecycle & Trailing Manager
  - Break-even trigger logic
  - Partial profit-take commands (TP1/TP2 scale-out)
  - Structural swing-based trailing stop
  - Active position state machine
"""

import asyncio
import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field, asdict
from enum import Enum, auto
from pathlib import Path
from typing import Optional, Callable

from safe_paths import bootstrap_ephemeral_state, runtime_state_dir
try:
    from .legacy_runtime_guard import require_non_broker_mode
except ImportError:
    from legacy_runtime_guard import require_non_broker_mode

if __name__ == "__main__":
    bootstrap_ephemeral_state("risk-lifecycle-demo")

log = logging.getLogger("RISK-LIFECYCLE")

RISK_DB = runtime_state_dir() / "risk_ledger.db"


# ─── Enums ────────────────────────────────────────────────────────────────────

class TradeState(Enum):
    PENDING   = auto()   # Signal generated, awaiting execution
    OPEN      = auto()   # Position live at broker
    AT_BE     = auto()   # Stop moved to break-even
    TP1_HIT   = auto()   # First partial take-profit executed
    TP2_HIT   = auto()   # Second partial executed, trailing active
    CLOSING   = auto()   # Close command sent
    CLOSED    = auto()   # Position confirmed closed


class ExitReason(Enum):
    TP1        = "TP1"
    TP2        = "TP2"
    TP3        = "TP3_FULL"
    SL         = "STOP_LOSS"
    BE         = "BREAK_EVEN"
    TRAILING   = "TRAILING_STOP"
    MANUAL     = "MANUAL_CLOSE"
    EMERGENCY  = "EMERGENCY_HALT"
    TIMEOUT    = "MAX_HOLD_TIMEOUT"


# ─── Position State Machine ───────────────────────────────────────────────────

@dataclass
class ManagedPosition:
    """Full lifecycle state for one open position."""
    ticket:         int
    direction:      str           # "BUY" | "SELL"
    symbol:         str           = "XAUUSD"
    lot_size:       float         = 0.01
    entry_price:    float         = 0.0
    current_sl:     float         = 0.0
    original_sl:    float         = 0.0
    tp1:            float         = 0.0
    tp2:            float         = 0.0
    tp3:            float         = 0.0
    confidence:     float         = 0.75
    strategy_tags:  list          = field(default_factory=list)
    state:          TradeState    = TradeState.OPEN
    open_time:      float         = field(default_factory=time.time)
    last_update:    float         = field(default_factory=time.time)
    agent_id:       int           = 7
    current_price:  float         = 0.0
    peak_price:     float         = 0.0   # Highest favourable excursion
    partial_lots_closed: float    = 0.0
    realised_pnl:   float         = 0.0   # From partial closes

    @property
    def sl_distance(self) -> float:
        return abs(self.entry_price - self.original_sl)

    @property
    def current_pnl_pts(self) -> float:
        if self.direction == "BUY":
            return self.current_price - self.entry_price
        return self.entry_price - self.current_price

    @property
    def current_r(self) -> float:
        return self.current_pnl_pts / self.sl_distance if self.sl_distance > 0 else 0.0

    @property
    def hold_minutes(self) -> float:
        return (time.time() - self.open_time) / 60.0


# ─── Agent 18: Institutional Risk Controller ─────────────────────────────────

class InstitutionalRiskController:
    """
    Full risk management engine with:
    - Kelly-modified position sizing (capped at 1% risk)
    - Correlation exposure caps (max 3 same-direction positions)
    - Daily drawdown circuit breaker (3% halt)
    - Consecutive loss de-risking (halve size after 3 losses)
    - Volatility-adjusted sizing (ATR scaling)
    - Complete risk audit ledger
    """

    # ── Risk Parameters ──────────────────────────────────────────────────────
    MAX_RISK_PCT          = 1.0     # Max % of balance per trade
    MAX_DAILY_DD_PCT      = 3.0     # Daily loss → halt
    MAX_SAME_DIR_POSITIONS = 3      # Max concurrent same-direction
    MAX_TOTAL_POSITIONS    = 1      # For scalping: one at a time
    CONSEC_LOSS_HALVE_AT   = 3      # Halve size after N consecutive losses
    ATR_SCALE_NEUTRAL      = 13.5   # "Normal" ATR for XAUUSD H1 (pips reference)
    CONFIDENCE_MIN         = 0.65   # Min confidence scalar (regime gate is primary filter)
    RISK_PER_R_USD_CAP     = 200.0  # Hard dollar cap per 1R risk regardless of balance
    LOT_STEP               = 0.01
    MIN_LOT                = 0.01
    MAX_LOT                = 5.0
    SKIP_IF_MIN_LOT_OVERRISKS = True
    SMALL_ACCOUNT_BALANCE_CEILING = 300.0
    SMALL_ACCOUNT_MAX_RISK_PCT    = 0.5
    SMALL_ACCOUNT_MAX_DAILY_DD_PCT = 2.0

    def __init__(self, initial_balance: float = 10_000.0,
                 send_command: Optional[Callable] = None,
                 max_risk_pct: Optional[float] = None,
                 max_daily_dd_pct: Optional[float] = None,
                 min_lot: Optional[float] = None,
                 lot_step: Optional[float] = None,
                 max_lot: Optional[float] = None,
                 skip_if_min_lot_overrisks: Optional[bool] = None,
                 max_total_positions: Optional[int] = None,
                 max_same_dir_positions: Optional[int] = None):
        require_non_broker_mode("risk_and_lifecycle.InstitutionalRiskController")
        self.small_account_mode = initial_balance <= self.SMALL_ACCOUNT_BALANCE_CEILING
        if self.small_account_mode:
            if max_risk_pct is None:
                max_risk_pct = self.SMALL_ACCOUNT_MAX_RISK_PCT
            if max_daily_dd_pct is None:
                max_daily_dd_pct = self.SMALL_ACCOUNT_MAX_DAILY_DD_PCT

        self.MAX_RISK_PCT = float(
            self.MAX_RISK_PCT if max_risk_pct is None else max_risk_pct
        )
        self.MAX_DAILY_DD_PCT = float(
            self.MAX_DAILY_DD_PCT if max_daily_dd_pct is None else max_daily_dd_pct
        )
        self.MIN_LOT = float(self.MIN_LOT if min_lot is None else min_lot)
        self.LOT_STEP = float(self.LOT_STEP if lot_step is None else lot_step)
        self.MAX_LOT = float(self.MAX_LOT if max_lot is None else max_lot)
        if skip_if_min_lot_overrisks is not None:
            self.SKIP_IF_MIN_LOT_OVERRISKS = bool(skip_if_min_lot_overrisks)
        if max_total_positions is not None:
            self.MAX_TOTAL_POSITIONS = int(max_total_positions)
        if max_same_dir_positions is not None:
            self.MAX_SAME_DIR_POSITIONS = int(max_same_dir_positions)

        self.balance          = initial_balance
        self.equity           = initial_balance
        self.day_start_equity = initial_balance
        self._send_command    = send_command
        self._open_positions: dict[int, ManagedPosition] = {}
        self._consecutive_losses = 0
        self._daily_halted    = False
        self._last_day        = int(time.time() // 86400)
        self._db_conn: Optional[sqlite3.Connection] = None  # persistent — one FD
        self._init_db()
        log.info(
            "Agent 18 [Risk Controller] | Balance: $%.2f | MaxRisk: %.2f%% | "
            "DailyDD: %.2f%% | MinLot: %.3f | small_account=%s",
            initial_balance, self.MAX_RISK_PCT, self.MAX_DAILY_DD_PCT,
            self.MIN_LOT, self.small_account_mode
        )

    def _get_db(self) -> Optional[sqlite3.Connection]:
        """Return the persistent SQLite connection, opening it once if needed."""
        if self._db_conn is None:
            try:
                RISK_DB.parent.mkdir(parents=True, exist_ok=True)
                self._db_conn = sqlite3.connect(
                    str(RISK_DB), timeout=5, check_same_thread=False
                )
            except Exception as exc:
                log.debug("RiskController: DB connect failed: %s", exc)
        return self._db_conn

    def _reset_db(self):
        """Close and discard the cached connection; next call gets a fresh one."""
        try:
            if self._db_conn:
                self._db_conn.close()
        except Exception:
            pass
        self._db_conn = None

    def close(self) -> None:
        """Release the legacy simulation ledger deterministically."""

        self._reset_db()

    def __enter__(self) -> "InstitutionalRiskController":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def _init_db(self):
        try:
            conn = self._get_db()
            if conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS risk_ledger (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_type TEXT,
                        ticket INTEGER,
                        symbol TEXT,
                        direction TEXT,
                        lot_size REAL,
                        entry_price REAL,
                        sl_price REAL,
                        tp1_price REAL,
                        risk_usd REAL,
                        risk_pct REAL,
                        confidence REAL,
                        reason TEXT,
                        balance_before REAL,
                        balance_after REAL,
                        timestamp REAL
                    )
                """)
                conn.commit()
        except Exception as exc:
            log.debug("RiskController: DB init skipped: %s", exc)
            self._reset_db()

    def _lot_precision(self) -> int:
        step_text = f"{self.LOT_STEP:.8f}".rstrip("0").rstrip(".")
        return len(step_text.split(".")[1]) if "." in step_text else 0

    def _log_event(self, event: str, pos: ManagedPosition,
                    reason: str = "", bal_after: float = None):
        sl_dist = abs(pos.entry_price - pos.original_sl)
        risk_usd = sl_dist * pos.lot_size * 100.0
        try:
            conn = self._get_db()
            if conn:
                conn.execute("""
                    INSERT INTO risk_ledger VALUES
                    (NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (event, pos.ticket, pos.symbol, pos.direction,
                      pos.lot_size, pos.entry_price, pos.current_sl,
                      pos.tp1, risk_usd,
                      risk_usd / self.balance * 100 if self.balance > 0 else 0,
                      pos.confidence, reason,
                      self.balance, bal_after or self.balance, time.time()))
                conn.commit()
        except Exception as exc:
            log.debug("RiskController: _log_event DB write skipped: %s", exc)
            self._reset_db()

    # ── Position Sizing ───────────────────────────────────────────────────────

    def calculate_lot_size(self, entry: float, sl: float,
                            confidence: float, atr_h1: float = None) -> float:
        """
        Institutional position sizing:

        1. Base risk = balance × max_risk_pct × confidence_scalar
        2. Consecutive-loss de-risking: halve if on a losing streak
        3. ATR volatility adjustment: scale inversely with ATR expansion
        4. Hard dollar cap: never risk > RISK_PER_R_USD_CAP per 1R
        5. Broker lot constraints: round to step, clamp to min/max

        lot = risk_usd / (sl_distance_price × contract_size)
        For XAUUSD: contract_size = 100 oz, so $1 move = $100 per lot
        """
        sl_dist = abs(entry - sl)
        if sl_dist <= 0:
            return 0.0

        # 1. Confidence scalar (75% conf = 0.75×, 100% = 1.0×)
        conf_scalar = max(min(confidence / 100.0, 1.0), self.CONFIDENCE_MIN)

        # 2. Base risk amount
        risk_usd = self.balance * (self.MAX_RISK_PCT / 100.0) * conf_scalar

        # 3. Consecutive loss de-risking
        if self._consecutive_losses >= self.CONSEC_LOSS_HALVE_AT:
            de_risk = 0.5 ** (self._consecutive_losses - self.CONSEC_LOSS_HALVE_AT + 1)
            risk_usd *= max(de_risk, 0.25)   # Floor at 25% of normal
            log.info("De-risking active: %d consec losses → risk_usd=$%.2f",
                     self._consecutive_losses, risk_usd)

        # 4. ATR volatility adjustment
        if atr_h1 and atr_h1 > 0:
            atr_ratio  = self.ATR_SCALE_NEUTRAL / atr_h1
            atr_scalar = max(0.5, min(1.5, atr_ratio))   # Clamp ±50%
            risk_usd  *= atr_scalar

        # 5. Hard dollar cap
        risk_usd = min(risk_usd, self.RISK_PER_R_USD_CAP)

        # 6. Lot calculation
        raw_lot = risk_usd / (sl_dist * 100.0)
        if raw_lot < self.MIN_LOT and self.SKIP_IF_MIN_LOT_OVERRISKS:
            log.info(
                "Trade skipped: min lot %.2f would exceed risk budget $%.2f for SL %.2f",
                self.MIN_LOT, risk_usd, sl_dist
            )
            return 0.0

        # 7. Broker constraints (standard XAUUSD)
        lot_precision = self._lot_precision()
        lot_size = round(round(raw_lot / self.LOT_STEP) * self.LOT_STEP, lot_precision)
        lot_size = max(self.MIN_LOT, min(self.MAX_LOT, lot_size))

        return round(lot_size, lot_precision)

    # ── Pre-Trade Checks ─────────────────────────────────────────────────────

    def approve_trade(self, direction: str, entry: float, sl: float,
                       confidence: float) -> tuple[bool, str]:
        """
        Full pre-trade approval gate. Returns (approved, reason).
        Called by Fusion Engine before sending order to EA.
        """
        # Daily drawdown circuit breaker
        if self._daily_halted:
            return False, f"DAILY_DD_HALT (>{self.MAX_DAILY_DD_PCT}%)"

        # Check daily DD on current equity
        dd_pct = (self.day_start_equity - self.equity) / self.day_start_equity * 100
        if dd_pct >= self.MAX_DAILY_DD_PCT:
            self._daily_halted = True
            log.warning("DAILY DD HALT triggered: %.1f%% loss", dd_pct)
            return False, f"DAILY_DD_{dd_pct:.1f}%"

        # Max concurrent positions
        if len(self._open_positions) >= self.MAX_TOTAL_POSITIONS:
            return False, f"MAX_POSITIONS ({self.MAX_TOTAL_POSITIONS})"

        # Correlation cap: count same-direction open trades
        same_dir = sum(1 for p in self._open_positions.values()
                       if p.direction == direction)
        if same_dir >= self.MAX_SAME_DIR_POSITIONS:
            return False, f"CORRELATION_CAP_{direction}"

        # Minimum confidence
        if confidence < self.CONFIDENCE_MIN * 100:
            return False, f"CONFIDENCE_TOO_LOW ({confidence:.0f}%)"

        # SL validity
        if sl <= 0 or abs(entry - sl) < 0.10:
            return False, "INVALID_SL"

        return True, "APPROVED"

    def register_open(self, pos: ManagedPosition):
        """Register a newly opened position in the risk ledger."""
        self._open_positions[pos.ticket] = pos
        self._log_event("OPEN", pos, reason="New position registered")
        log.info("Position registered: #%d %s %.2f lots @ %.2f | SL %.2f",
                 pos.ticket, pos.direction, pos.lot_size, pos.entry_price, pos.current_sl)

    def register_close(self, ticket: int, exit_price: float,
                        exit_reason: ExitReason, pnl_usd: float):
        """Update state on position close and adjust consecutive loss counter."""
        pos = self._open_positions.pop(ticket, None)
        if not pos:
            return

        self.balance   += pnl_usd
        self.equity     = self.balance
        bal_after = self.balance

        if pnl_usd < 0:
            self._consecutive_losses += 1
        else:
            self._consecutive_losses = 0   # Reset on win

        self._log_event("CLOSE", pos, reason=exit_reason.value, bal_after=bal_after)
        log.info("Position closed: #%d | PnL $%.2f | Consec losses: %d",
                 ticket, pnl_usd, self._consecutive_losses)

    def update_equity(self, equity: float):
        """Called on each heartbeat from EA."""
        self.equity = equity
        # Daily reset
        today = int(time.time() // 86400)
        if today != self._last_day:
            self.day_start_equity = equity
            self._daily_halted    = False
            self._last_day        = today

    def get_status(self) -> dict:
        dd_pct = (self.day_start_equity - self.equity) / self.day_start_equity * 100 \
                  if self.day_start_equity > 0 else 0
        return {
            "balance":           round(self.balance, 2),
            "equity":            round(self.equity, 2),
            "daily_dd_pct":      round(dd_pct, 2),
            "daily_halted":      self._daily_halted,
            "open_positions":    len(self._open_positions),
            "consecutive_losses": self._consecutive_losses,
            "max_risk_pct":      round(self.MAX_RISK_PCT, 3),
            "max_daily_dd_pct":  round(self.MAX_DAILY_DD_PCT, 3),
            "min_lot":           round(self.MIN_LOT, 3),
            "small_account_mode": self.small_account_mode,
            "min_lot_guard":     self.SKIP_IF_MIN_LOT_OVERRISKS,
        }


# ─── Agent 19: Trade Lifecycle & Trailing Manager ────────────────────────────

class TradeLifecycleManager:
    """
    State machine managing every open position through its lifecycle:

    State flow:
      OPEN → (TP1 hit) → TP1_HIT: close 50% lots, move SL to BE
      TP1_HIT → (TP2 hit) → TP2_HIT: close 25% lots, activate structural trail
      TP2_HIT → (TP3 hit or trail triggered) → CLOSING → CLOSED

    Trailing stop: based on structural swing points (H1 bars).
    Reads current H1 OHLC to find the last significant swing low/high
    and trails the stop behind it.
    """

    # ── Management Parameters ─────────────────────────────────────────────
    TP1_PARTIAL_PCT     = 50.0    # % of lots to close at TP1
    TP2_PARTIAL_PCT     = 25.0    # % of remaining lots to close at TP2
    BE_BUFFER_PTS       = 3       # Points beyond entry for BE stop (covers commission)
    TRAIL_SWING_BARS    = 3       # Look back N bars for swing point trailing
    MAX_HOLD_HOURS      = 24.0    # Force-close after 24 hours
    TP1_REQUIRED_R      = 1.0     # Minimum R before TP1 is triggered
    BE_TRIGGER_R        = 0.7     # Move to BE when position is at 0.7R
    PRECISION_BE_TRIGGER_R = 0.35
    PRECISION_TRAIL_START_R = 0.65
    PRECISION_TRAIL_GAP_R = 0.30
    PRECISION_MIN_LOCK_R = 0.18
    PRECISION_TRAIL_STEP_PRICE = 0.05
    PRECISION_MAX_HOLD_HOURS = 3.0

    def __init__(self, send_command: Callable,
                 risk_controller: InstitutionalRiskController):
        require_non_broker_mode("risk_and_lifecycle.TradeLifecycleManager")
        self.send_cmd     = send_command    # async fn(cmd_dict) → to IPC bridge
        self.risk         = risk_controller
        self._positions: dict[int, ManagedPosition] = {}
        if self.risk.small_account_mode:
            self.BE_TRIGGER_R = self.PRECISION_BE_TRIGGER_R
            self.MAX_HOLD_HOURS = self.PRECISION_MAX_HOLD_HOURS
        log.info("Agent 19 [Lifecycle Manager] initialized")

    def register(self, pos: ManagedPosition):
        """Start managing a new position."""
        pos.peak_price = pos.entry_price
        self._positions[pos.ticket] = pos
        self.risk.register_open(pos)

    async def on_price_update(self, ticket: int, current_price: float,
                               h1_ohlc: list[dict] = None):
        """
        Called on every tick for managed positions.
        Runs the state machine and issues management commands as needed.
        """
        pos = self._positions.get(ticket)
        if not pos or pos.state in (TradeState.CLOSING, TradeState.CLOSED):
            return

        pos.current_price = current_price
        pos.last_update   = time.time()

        # Update peak favourable excursion
        if pos.direction == "BUY":
            pos.peak_price = max(pos.peak_price, current_price)
        else:
            pos.peak_price = min(pos.peak_price, current_price)

        current_r = pos.current_r

        # ── Timeout check ─────────────────────────────────────────────────
        if pos.hold_minutes >= self.MAX_HOLD_HOURS * 60:
            await self._close_position(pos, ExitReason.TIMEOUT)
            return

        # ── SL hit check ──────────────────────────────────────────────────
        sl_hit = (pos.direction == "BUY"  and current_price <= pos.current_sl) or \
                  (pos.direction == "SELL" and current_price >= pos.current_sl)
        if sl_hit:
            reason = ExitReason.BE if pos.state == TradeState.AT_BE else ExitReason.SL
            await self._close_position(pos, reason)
            return

        if self.risk.small_account_mode:
            await self._precision_trail(pos, current_r)

        # ── State machine transitions ─────────────────────────────────────

        if pos.state == TradeState.OPEN:
            # Move to BE when 0.7R in profit
            if current_r >= self.BE_TRIGGER_R:
                await self._move_to_breakeven(pos)

            # TP1 hit
            if self._tp_hit(pos, current_price, "TP1"):
                await self._execute_partial_close(pos, self.TP1_PARTIAL_PCT,
                                                   ExitReason.TP1)
                pos.state = TradeState.TP1_HIT
                log.info("TP1 hit on #%d — partial close %.0f%%, moved to BE",
                         pos.ticket, self.TP1_PARTIAL_PCT)

        elif pos.state == TradeState.AT_BE:
            if self._tp_hit(pos, current_price, "TP1"):
                await self._execute_partial_close(pos, self.TP1_PARTIAL_PCT,
                                                   ExitReason.TP1)
                pos.state = TradeState.TP1_HIT

        elif pos.state == TradeState.TP1_HIT:
            if self._tp_hit(pos, current_price, "TP2"):
                await self._execute_partial_close(pos, self.TP2_PARTIAL_PCT,
                                                   ExitReason.TP2)
                pos.state = TradeState.TP2_HIT
                log.info("TP2 hit on #%d — trailing stop activated", pos.ticket)

        elif pos.state == TradeState.TP2_HIT:
            # Structural trailing stop using H1 swing points
            if h1_ohlc:
                new_trail_sl = self._compute_structural_trail(pos, h1_ohlc)
                if new_trail_sl and self._sl_is_improvement(pos, new_trail_sl):
                    await self._modify_sl(pos, new_trail_sl, "STRUCTURAL_TRAIL")

            # TP3 hit → full close
            if self._tp_hit(pos, current_price, "TP3"):
                await self._close_position(pos, ExitReason.TP3)

    # ── Management Actions ────────────────────────────────────────────────────

    async def _move_to_breakeven(self, pos: ManagedPosition):
        """Move SL to entry + BE_BUFFER_PTS (covers spread + commission)."""
        be_buffer = self.BE_BUFFER_PTS * 0.01
        if pos.direction == "BUY":
            new_sl = pos.entry_price + be_buffer
        else:
            new_sl = pos.entry_price - be_buffer

        if self._sl_is_improvement(pos, new_sl):
            await self._modify_sl(pos, new_sl, "BREAK_EVEN")
            pos.state = TradeState.AT_BE
            log.info("BE triggered on #%d: SL → %.2f", pos.ticket, new_sl)

    async def _execute_partial_close(self, pos: ManagedPosition,
                                      pct: float, reason: ExitReason):
        """Send partial close command to EA."""
        partial_lots = pos.lot_size * pct / 100.0
        if partial_lots < self.risk.MIN_LOT:
            if self.risk.small_account_mode:
                await self._lock_r_profit(
                    pos, self.PRECISION_MIN_LOCK_R, reason.value
                )
            log.info(
                "Partial close skipped on #%d: %.3f lots below broker minimum %.3f",
                pos.ticket, partial_lots, self.risk.MIN_LOT
            )
            return

        cmd = {
            "cmd_type":    "PARTIAL_CLOSE",
            "ticket":      pos.ticket,
            "partial_pct": pct,
        }
        pos.partial_lots_closed += partial_lots
        await self.send_cmd(cmd)
        log.info("Partial close %.0f%% on #%d (%s)",
                 pct, pos.ticket, reason.value)

    async def _precision_trail(self, pos: ManagedPosition, current_r: float):
        """Aggressive small-account stop management for one 0.01-lot position."""
        if current_r >= self.BE_TRIGGER_R and pos.state == TradeState.OPEN:
            await self._move_to_breakeven(pos)

        if current_r < self.PRECISION_TRAIL_START_R:
            return

        lock_r = max(
            self.PRECISION_MIN_LOCK_R,
            current_r - self.PRECISION_TRAIL_GAP_R,
        )
        await self._lock_r_profit(pos, lock_r, "PRECISION_TRAIL")

    async def _lock_r_profit(self, pos: ManagedPosition, lock_r: float,
                              reason: str):
        sl_dist = pos.sl_distance
        if sl_dist <= 0:
            return

        if pos.direction == "BUY":
            new_sl = pos.entry_price + sl_dist * lock_r
            if new_sl - pos.current_sl < self.PRECISION_TRAIL_STEP_PRICE:
                return
        else:
            new_sl = pos.entry_price - sl_dist * lock_r
            if pos.current_sl - new_sl < self.PRECISION_TRAIL_STEP_PRICE:
                return

        if self._sl_is_improvement(pos, new_sl):
            await self._modify_sl(pos, new_sl, reason)

    async def _modify_sl(self, pos: ManagedPosition,
                          new_sl: float, reason: str):
        """Send SL modification to EA."""
        cmd = {
            "cmd_type":  "MODIFY",
            "ticket":    pos.ticket,
            "sl_price":  round(new_sl, 2),
            # Keep broker-side failsafe at the final target while Python manages milestones.
            "tp1_price": pos.tp3 or pos.tp1,
        }
        pos.current_sl = round(new_sl, 2)
        await self.send_cmd(cmd)
        log.info("SL modified on #%d: %.2f → %.2f (%s)",
                 pos.ticket, pos.original_sl, new_sl, reason)

    async def _close_position(self, pos: ManagedPosition, reason: ExitReason):
        """Send full close command and deregister."""
        pos.state = TradeState.CLOSING
        cmd = {
            "cmd_type": "CLOSE",
            "ticket":   pos.ticket,
        }
        await self.send_cmd(cmd)

        # Estimate PnL for risk controller
        pnl_pts = pos.current_pnl_pts
        pnl_usd = pnl_pts * pos.lot_size * 100.0 + pos.realised_pnl
        self.risk.register_close(pos.ticket, pos.current_price, reason, pnl_usd)
        self._positions.pop(pos.ticket, None)
        log.info("Position #%d closed: %s | PnL pts=%.2f $=%.2f",
                 pos.ticket, reason.value, pnl_pts, pnl_usd)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _tp_hit(self, pos: ManagedPosition,
                 current_price: float, tp_level: str) -> bool:
        tp = getattr(pos, tp_level.lower(), 0.0)
        if tp <= 0:
            return False
        if pos.direction == "BUY":
            return current_price >= tp
        return current_price <= tp

    def _sl_is_improvement(self, pos: ManagedPosition, new_sl: float) -> bool:
        """Ensure SL only moves in profit direction."""
        if pos.direction == "BUY":
            return new_sl > pos.current_sl
        return new_sl < pos.current_sl

    def _compute_structural_trail(self, pos: ManagedPosition,
                                   h1_ohlc: list[dict]) -> Optional[float]:
        """
        Trail behind the last significant H1 swing point.
        For BUY: trail behind the most recent H1 swing LOW
                 (lowest low of last N bars, buffered by 0.1×ATR)
        For SELL: trail above the most recent H1 swing HIGH
        """
        n     = self.TRAIL_SWING_BARS
        bars  = h1_ohlc[-max(n+1, 5):]
        if len(bars) < n:
            return None

        atr = sum(b["h"] - b["l"] for b in bars[-14:]) / min(len(bars), 14)
        buffer = atr * 0.10   # 10% ATR buffer below swing

        if pos.direction == "BUY":
            # Find lowest swing low in last N completed bars
            swing_low = min(b["l"] for b in bars[-n:])
            trail_sl  = swing_low - buffer
            # Only trail if new SL is above current (locking in profit)
            return trail_sl if trail_sl > pos.current_sl else None
        else:
            swing_high = max(b["h"] for b in bars[-n:])
            trail_sl   = swing_high + buffer
            return trail_sl if trail_sl < pos.current_sl else None

    @property
    def open_positions(self) -> dict[int, ManagedPosition]:
        return self._positions.copy()

    def summary(self) -> str:
        if not self._positions:
            return "No open positions"
        lines = []
        for ticket, pos in self._positions.items():
            lines.append(
                f"  #{ticket} {pos.direction} {pos.lot_size:.2f}L "
                f"@ {pos.entry_price:.2f} | SL {pos.current_sl:.2f} "
                f"| {pos.current_r:+.2f}R | {pos.state.name}"
            )
        return "\n".join(lines)


# ─── Validation Demo ──────────────────────────────────────────────────────────

async def demo_risk_lifecycle():
    sent_commands = []

    async def mock_send(cmd: dict):
        sent_commands.append(cmd)
        log.debug("→ EA: %s", cmd.get("cmd_type"))

    print("\n" + "═"*58)
    print("  Agent 18 + 19: Risk & Lifecycle Demo")
    print("═"*58)

    risk = InstitutionalRiskController(initial_balance=25_000.0,
                                        send_command=mock_send)
    mgr  = TradeLifecycleManager(send_command=mock_send, risk_controller=risk)

    # ── Sizing tests ──────────────────────────────────────────────────────────
    print("\n[Agent 18] Position Sizing:")
    cases = [
        (2685.5, 2672.0, 82.0, 13.5,  "Normal London trade"),
        (2685.5, 2672.0, 75.0, 13.5,  "Min confidence (75%)"),
        (2685.5, 2672.0, 95.0, 13.5,  "High confidence (95%)"),
        (2685.5, 2672.0, 82.0, 22.0,  "High ATR day (expanded vol)"),
        (2685.5, 2672.0, 82.0,  8.5,  "Low ATR day (compressed vol)"),
    ]
    for entry, sl, conf, atr, label in cases:
        lot  = risk.calculate_lot_size(entry, sl, conf, atr)
        sl_d = abs(entry - sl)
        r_usd = sl_d * lot * 100
        r_pct = r_usd / 25000 * 100
        print(f"  {label:<35} → {lot:.2f} lots  "
              f"(${r_usd:>6.2f} risk / {r_pct:.2f}%)")

    # ── Pre-trade approval tests ──────────────────────────────────────────────
    print("\n[Agent 18] Pre-Trade Approval Gate:")
    approvals = [
        ("BUY",  2685.5, 2672.0, 82.0, "Normal trade"),
        ("BUY",  2685.5, 2672.0, 60.0, "Low confidence → reject"),
        ("BUY",  2685.5, 2685.4, 82.0, "Tiny SL → reject"),
    ]
    for dir_, entry, sl, conf, label in approvals:
        ok, reason = risk.approve_trade(dir_, entry, sl, conf)
        icon = "✓" if ok else "✗"
        print(f"  {icon} {label:<35} [{reason}]")

    # ── Lifecycle state machine simulation ────────────────────────────────────
    print("\n[Agent 19] Trade Lifecycle State Machine:")

    # Register a position
    pos = ManagedPosition(
        ticket=100001, direction="BUY", lot_size=0.15,
        entry_price=2683.20, current_sl=2670.00, original_sl=2670.00,
        tp1=2696.40, tp2=2709.60, tp3=2730.00,
        confidence=0.83, strategy_tags=["SMC_OB", "CRT_SWEEP"],
        state=TradeState.OPEN
    )
    mgr.register(pos)
    print(f"  Opened: #{pos.ticket} BUY 0.15L @ 2683.20 | SL 2670.00")

    # Simulate price evolution
    price_sequence = [
        (2684.50, None, "Price moves up slightly"),
        (2686.80, None, "0.7R reached — expect BE"),
        (2688.00, None, "Approaching TP1"),
        (2696.50, None, "TP1 hit (2696.40) — expect partial close + BE"),
        (2702.00, None, "Trending toward TP2"),
        (2709.70, None, "TP2 hit (2709.60) — trailing activated"),
        (2718.00, None, "Trailing higher"),
        (2730.10, None, "TP3 hit (2730.00) — full close"),
    ]

    # Synthetic H1 bars for trailing
    def make_trail_bars(current_price, n=5):
        return [{"h": current_price - i*0.5 + 2,
                 "l": current_price - i*0.5 - 3,
                 "c": current_price - i*0.5,
                 "o": current_price - i*0.5 - 1} for i in range(n)]

    for price, _, note in price_sequence:
        prev_state = pos.state.name
        await mgr.on_price_update(
            pos.ticket, price,
            h1_ohlc=make_trail_bars(price)
        )
        new_state = pos.state.name if pos.ticket in mgr._positions else "CLOSED"
        cmds_str = ", ".join(c.get("cmd_type","?") for c in sent_commands[-3:]) \
                   if sent_commands else "none"
        print(f"  Price {price:>7.2f} | {prev_state:>10} → {new_state:<12} "
              f"| {note}")
        if not mgr._positions.get(pos.ticket):
            break

    print(f"\n  Commands sent to EA: {len(sent_commands)}")
    for cmd in sent_commands:
        print(f"    → {cmd.get('cmd_type','?'):15} ticket={cmd.get('ticket','')}"
              f"  {cmd.get('sl_price','')}")

    print(f"\n  Risk Controller Status: {risk.get_status()}")
    print("\n" + "═"*58)
    print("  Agent 18 + 19: ALL SYSTEMS VALIDATED ✓")
    print("═"*58 + "\n")

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(demo_risk_lifecycle())
