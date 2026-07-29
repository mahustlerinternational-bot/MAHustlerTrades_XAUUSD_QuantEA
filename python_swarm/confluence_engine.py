"""
QUANT-SWARM-XAU: Legacy Confluence Compatibility Engine (Agent 16)
              + Swarm Meta-Reviewer & Journal Auditor (Agent 20)

Core mathematical engine for dynamic agent weighting,
signal fusion, and post-mortem self-correction loops.
"""

import asyncio
import json
import math
import sqlite3
import time
import hashlib
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import numpy as np

from safe_paths import bootstrap_ephemeral_state, runtime_state_dir
try:
    from .legacy_runtime_guard import require_non_broker_mode
except ImportError:
    from legacy_runtime_guard import require_non_broker_mode

if __name__ == "__main__":
    bootstrap_ephemeral_state("confluence-demo")

# Optional: ChromaDB for vector embeddings
try:
    import chromadb
    CHROMA_AVAILABLE = True
except ImportError:
    CHROMA_AVAILABLE = False
    logging.getLogger(__name__).debug(
        "ChromaDB not available - using SQLite-only journaling"
    )

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
        datefmt="%H:%M:%S",
    )
log = logging.getLogger("QUANT-SWARM")

# ─── Configuration ────────────────────────────────────────────────────────────

AGENT_REGISTRY = {
    4:  {"name": "HTF_Liquidity_Trap",    "strategy": "HTF_SR"},
    5:  {"name": "Trendline_Breakout",    "strategy": "TL_BREAK"},
    6:  {"name": "CRT_Engine",            "strategy": "CRT"},
    7:  {"name": "ICT_SMC_Core",          "strategy": "ICT_SMC"},
    8:  {"name": "VPVR_POC",              "strategy": "VOL_PROFILE"},
    9:  {"name": "Asia_Session_Sweep",    "strategy": "ASIA_SWEEP"},
    10: {"name": "News_Sentiment",        "strategy": "MACRO_NEWS"},
    11: {"name": "MeanRev_VolBands",      "strategy": "MEAN_REV"},
    12: {"name": "Corr_Deflection",       "strategy": "CORR_MATRIX"},
    13: {"name": "Wyckoff_Schematic",     "strategy": "WYCKOFF"},
}

MIN_CONFLUENCE_SCORE   = 75.0   # % threshold to pass fusion
ROLLING_WINDOW_DAYS    = 14     # Days for Sharpe/PF calculation
MIN_AGENTS_AGREEING = 3      # Canonical distinct-agent directional quorum
MIN_DIRECTIONAL_DOMINANCE = 0.60
MAX_SIGNAL_AGE_SECONDS = 30.0
PMA_DELAY_MINUTES      = 15     # Post-mortem analysis delay after close
MIN_PROFIT_FACTOR      = 0.4    # Below this → contextual blacklisting

DB_DIR = runtime_state_dir()


# ─── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class AgentSignal:
    agent_id: int
    direction: str              # "BUY", "SELL", "NEUTRAL"
    conviction: float           # unsigned 0.0 to 100.0
    entry: float
    sl: float
    tp1: float
    tp2: float
    tp3: float
    strategy_tags: list[str]
    timestamp: float = field(default_factory=time.time)
    htf_aligned: bool = True
    session_valid: bool = True
    volume_confirmed: bool = True
    blacklisted_context: bool = False

    def __post_init__(self):
        if self.agent_id not in AGENT_REGISTRY:
            raise ValueError(f"unknown strategy agent: {self.agent_id!r}")
        self.direction = self.direction.upper()
        if self.direction not in {"BUY", "SELL", "NEUTRAL"}:
            raise ValueError("direction must be BUY, SELL, or NEUTRAL")
        if not math.isfinite(self.conviction) or not 0.0 <= self.conviction <= 100.0:
            raise ValueError("conviction must be an unsigned finite value from 0 to 100")

@dataclass
class TradeTelemetry:
    trade_id: str
    agent_id: int
    direction: str
    entry_price: float
    exit_price: float
    sl_price: float
    outcome_r: float            # R-multiple result
    slippage_pts: float
    session: str                # "LONDON", "NY", "ASIA", "OFF"
    volume_at_entry: float
    htf_aligned: bool
    news_delta: float           # News impact score at time of trade
    time_of_day: str            # "HH:MM"
    duration_minutes: int
    timestamp_open: float
    timestamp_close: float

@dataclass
class AgentPerformanceMetrics:
    agent_id: int
    rolling_sharpe: float       = 0.0
    rolling_profit_factor: float = 1.0
    win_rate: float             = 0.5
    avg_r_multiple: float       = 0.0
    total_trades: int           = 0
    consecutive_losses: int     = 0
    dynamic_weight: float       = 1.0
    current_drawdown_state: bool = False
    last_recalibrated: float    = 0.0
    blacklisted_contexts: list  = field(default_factory=list)


# ─── Journal Database Layer ───────────────────────────────────────────────────

class AgentJournal:
    """Cryptographic SQLite journal + optional vector embedding store."""

    def __init__(self, agent_id: int, db_dir: Path | str | None = None):
        self.agent_id = agent_id
        self.agent_name = AGENT_REGISTRY[agent_id]["name"]
        journal_root = Path(db_dir).expanduser().resolve() if db_dir else DB_DIR
        self.db_path = journal_root / f"agent_{agent_id:02d}_{self.agent_name}.db"
        self._conn: Optional[sqlite3.Connection] = None  # persistent — one FD for lifetime
        self._init_db()
        self.perf: AgentPerformanceMetrics = AgentPerformanceMetrics(agent_id=agent_id)
        
        # ChromaDB vector store for signal embeddings
        if CHROMA_AVAILABLE:
            chroma_client = chromadb.PersistentClient(
                path=str(self.db_path.parent / "chroma")
            )
            self.collection = chroma_client.get_or_create_collection(
                name=f"agent_{agent_id:02d}_signals"
            )
        else:
            self.collection = None

    def _get_conn(self) -> Optional[sqlite3.Connection]:
        """Return the persistent SQLite connection, opening it once if needed."""
        if self._conn is None:
            try:
                self.db_path.parent.mkdir(parents=True, exist_ok=True)
                self._conn = sqlite3.connect(
                    str(self.db_path), timeout=5, check_same_thread=False
                )
            except Exception as exc:
                log.debug("AgentJournal[%02d]: DB connect failed: %s",
                          self.agent_id, exc)
        return self._conn

    def _reset_conn(self):
        """Close and discard the cached connection; next call gets a fresh one."""
        try:
            if self._conn:
                self._conn.close()
        except Exception:
            pass
        self._conn = None

    def _init_db(self):
        try:
            conn = self._get_conn()
            if not conn:
                return
            conn.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_hash TEXT UNIQUE,
                    agent_id INTEGER,
                    direction TEXT,
                    conviction REAL,
                    entry REAL, sl REAL, tp1 REAL, tp2 REAL, tp3 REAL,
                    strategy_tags TEXT,
                    htf_aligned INTEGER,
                    session_valid INTEGER,
                    volume_confirmed INTEGER,
                    blacklisted_ctx INTEGER,
                    timestamp REAL,
                    executed INTEGER DEFAULT 0
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trade_outcomes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id TEXT UNIQUE,
                    agent_id INTEGER,
                    direction TEXT,
                    entry_price REAL,
                    exit_price REAL,
                    sl_price REAL,
                    outcome_r REAL,
                    slippage_pts REAL,
                    session TEXT,
                    volume_at_entry REAL,
                    htf_aligned INTEGER,
                    news_delta REAL,
                    time_of_day TEXT,
                    duration_minutes INTEGER,
                    timestamp_open REAL,
                    timestamp_close REAL,
                    post_mortem_processed INTEGER DEFAULT 0
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS parameter_adjustments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    adjustment_type TEXT,
                    context_key TEXT,
                    old_value TEXT,
                    new_value TEXT,
                    reason TEXT,
                    timestamp REAL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS performance_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_date TEXT,
                    sharpe REAL,
                    profit_factor REAL,
                    win_rate REAL,
                    avg_r REAL,
                    total_trades INTEGER,
                    dynamic_weight REAL,
                    timestamp REAL
                )
            """)
            conn.commit()
        except Exception as exc:
            log.debug("AgentJournal[%02d]: DB init skipped: %s", self.agent_id, exc)
            self._reset_conn()

    def log_signal(self, sig: AgentSignal) -> str:
        """Log signal with cryptographic hash for dedup and audit."""
        payload = f"{sig.agent_id}:{sig.direction}:{sig.entry:.5f}:{sig.timestamp:.3f}"
        sig_hash = hashlib.sha256(payload.encode()).hexdigest()[:16]
        
        try:
            conn = self._get_conn()
            if conn:
                conn.execute("""
                    INSERT OR IGNORE INTO signals 
                    (signal_hash, agent_id, direction, conviction, entry, sl, tp1, tp2, tp3,
                     strategy_tags, htf_aligned, session_valid, volume_confirmed, blacklisted_ctx, timestamp)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (sig_hash, sig.agent_id, sig.direction, sig.conviction,
                      sig.entry, sig.sl, sig.tp1, sig.tp2, sig.tp3,
                      json.dumps(sig.strategy_tags),
                      int(sig.htf_aligned), int(sig.session_valid),
                      int(sig.volume_confirmed), int(sig.blacklisted_context),
                      sig.timestamp))
                conn.commit()
        except Exception as exc:
            log.debug("AgentJournal[%02d]: log_signal DB write skipped: %s",
                      self.agent_id, exc)
            self._reset_conn()
        
        # Embed signal as vector (simplified feature vector)
        if self.collection:
            features = [
                sig.conviction / 100.0,
                1.0 if sig.direction == "BUY" else -1.0,
                float(sig.htf_aligned),
                float(sig.session_valid),
                float(sig.volume_confirmed),
                (sig.tp1 - sig.entry) / max(sig.entry - sig.sl, 0.001),
                sig.timestamp % 86400 / 86400.0
            ]
            self.collection.add(
                embeddings=[features],
                documents=[json.dumps(asdict(sig))],
                ids=[sig_hash]
            )
        
        return sig_hash

    def log_outcome(self, telemetry: TradeTelemetry):
        try:
            conn = self._get_conn()
            if conn:
                conn.execute("""
                    INSERT OR REPLACE INTO trade_outcomes
                    (trade_id, agent_id, direction, entry_price, exit_price, sl_price,
                     outcome_r, slippage_pts, session, volume_at_entry, htf_aligned,
                     news_delta, time_of_day, duration_minutes, timestamp_open, timestamp_close)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (telemetry.trade_id, telemetry.agent_id, telemetry.direction,
                      telemetry.entry_price, telemetry.exit_price, telemetry.sl_price,
                      telemetry.outcome_r, telemetry.slippage_pts, telemetry.session,
                      telemetry.volume_at_entry, int(telemetry.htf_aligned),
                      telemetry.news_delta, telemetry.time_of_day, telemetry.duration_minutes,
                      telemetry.timestamp_open, telemetry.timestamp_close))
                conn.commit()
        except Exception as exc:
            log.debug("AgentJournal[%02d]: log_outcome DB write skipped: %s",
                      self.agent_id, exc)
            self._reset_conn()

    def fetch_rolling_outcomes(self, days: int = ROLLING_WINDOW_DAYS) -> list[dict]:
        try:
            since = time.time() - (days * 86400)
            conn = self._get_conn()
            if not conn:
                return []
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT * FROM trade_outcomes
                WHERE timestamp_close >= ? AND post_mortem_processed = 1
                ORDER BY timestamp_close DESC
            """, (since,)).fetchall()
            return [dict(r) for r in rows]
        except Exception as exc:
            log.debug("AgentJournal[%02d]: fetch_rolling_outcomes skipped: %s",
                      self.agent_id, exc)
            self._reset_conn()
            return []

    def log_parameter_adjustment(self, adj_type: str, ctx_key: str,
                                  old_val: str, new_val: str, reason: str):
        try:
            conn = self._get_conn()
            if conn:
                conn.execute("""
                    INSERT INTO parameter_adjustments
                    (adjustment_type, context_key, old_value, new_value, reason, timestamp)
                    VALUES (?,?,?,?,?,?)
                """, (adj_type, ctx_key, old_val, new_val, reason, time.time()))
                conn.commit()
        except Exception as exc:
            log.debug("AgentJournal[%02d]: log_parameter_adjustment skipped: %s",
                      self.agent_id, exc)
            self._reset_conn()


# ─── Agent 16: Confluence Fusion Engine ──────────────────────────────────────

class ConfluenceFusionEngine:
    """
    Aggregates signals from 10 strategy agents using dynamically weighted
    matrix math. Requires ≥75% cumulative confidence score to emit a trade.
    """

    def __init__(self, journals: dict[int, AgentJournal]):
        require_non_broker_mode("ConfluenceFusionEngine")
        self.journals = journals
        self.performance: dict[int, AgentPerformanceMetrics] = {
            aid: AgentPerformanceMetrics(agent_id=aid) for aid in AGENT_REGISTRY
        }
        log.info("Confluence Fusion Engine initialized for %d agents", len(journals))

    def compute_dynamic_weights(self) -> dict[int, float]:
        """
        Dynamic weight formula based on rolling Sharpe + Profit Factor.
        Agents in drawdown are throttled; top performers are amplified.
        
        Weight_i = (Sharpe_i × α + PF_i × β) × drawdown_multiplier_i
        where α=0.4, β=0.6 (PF weighted higher for XAUUSD manipulation resilience)
        """
        ALPHA = 0.4   # Sharpe weight
        BETA  = 0.6   # Profit Factor weight
        weights = {}
        
        for aid, perf in self.performance.items():
            # Normalize Sharpe (clip to [-3, 3] then scale to [0, 1])
            norm_sharpe = (min(max(perf.rolling_sharpe, -3.0), 3.0) + 3.0) / 6.0
            
            # Normalize Profit Factor (PF 0→1 bad, 1→2+ good)
            norm_pf = min(max((perf.rolling_profit_factor - 0.5) / 1.5, 0.0), 1.0)
            
            raw_weight = ALPHA * norm_sharpe + BETA * norm_pf
            
            # Drawdown throttle: 50% weight reduction if in active drawdown
            if perf.current_drawdown_state:
                drawdown_multiplier = 0.5
            else:
                drawdown_multiplier = 1.0
            
            # Minimum weight floor (never fully mute an agent)
            weights[aid] = max(raw_weight * drawdown_multiplier, 0.1)
        
        # Normalize so weights sum to number of agents (preserve scale)
        total = sum(weights.values())
        n_agents = len(weights)
        if total > 0:
            weights = {k: v * n_agents / total for k, v in weights.items()}
        
        log.debug("Dynamic weights computed: %s", 
                  {AGENT_REGISTRY[k]['name'][:8]: f"{v:.2f}" for k, v in weights.items()})
        return weights

    def fuse_signals(self, signals: list[AgentSignal]) -> Optional[dict]:
        """
        Core fusion algorithm:
        1. Filter blacklisted/invalid signals
        2. Resolve directional consensus
        3. Apply dynamic weights
        4. Compute cumulative confidence
        5. Gate on ≥75% threshold and ≥3 agents agreeing
        """
        if not signals:
            return None

        weights = self.compute_dynamic_weights()

        # Compatibility callers still use this legacy shape.  Apply the same
        # fail-closed invariants as the canonical FusionPolicy: one known agent
        # per evaluation, fresh observations, unsigned conviction, and valid
        # directional geometry.
        agent_ids = [signal.agent_id for signal in signals]
        if len(agent_ids) != len(set(agent_ids)):
            log.warning("Fusion REJECTED: duplicate strategy-agent contribution")
            return None
        now = time.time()

        def valid_geometry(signal: AgentSignal) -> bool:
            values = (signal.entry, signal.sl, signal.tp1, signal.tp2, signal.tp3)
            if any(not math.isfinite(value) or value <= 0 for value in values):
                return False
            if signal.direction == "BUY":
                return signal.sl < signal.entry < signal.tp1 < signal.tp2 < signal.tp3
            if signal.direction == "SELL":
                return signal.tp3 < signal.tp2 < signal.tp1 < signal.entry < signal.sl
            return False
        
        # Filter out disqualified signals
        valid_signals = [s for s in signals if (
            s.session_valid and
            s.htf_aligned and
            s.volume_confirmed and
            not s.blacklisted_context and
            s.direction != "NEUTRAL" and
            0.0 <= now - s.timestamp <= MAX_SIGNAL_AGE_SECONDS
        )]

        if any(not valid_geometry(signal) for signal in valid_signals):
            log.warning("Fusion REJECTED: invalid directional SL/TP geometry")
            return None
        
        if len(valid_signals) < MIN_AGENTS_AGREEING:
            log.info("Fusion: Insufficient valid signals (%d < %d)", 
                     len(valid_signals), MIN_AGENTS_AGREEING)
            return None
        
        # Directional vote with weighting
        buy_score  = sum(weights.get(s.agent_id, 1.0) * s.conviction
                         for s in valid_signals if s.direction == "BUY")
        sell_score = sum(weights.get(s.agent_id, 1.0) * s.conviction
                         for s in valid_signals if s.direction == "SELL")

        if buy_score == sell_score:
            log.info("Fusion REJECTED: exact directional tie")
            return None
        direction  = "BUY" if buy_score > sell_score else "SELL"
        raw_score  = buy_score if direction == "BUY" else sell_score

        total_directional_score = buy_score + sell_score
        dominance = (
            raw_score / total_directional_score
            if total_directional_score > 0
            else 0.0
        )
        if dominance < MIN_DIRECTIONAL_DOMINANCE:
            log.info(
                "Fusion REJECTED: Directional dominance %.1f%% < %.1f%%",
                dominance * 100.0,
                MIN_DIRECTIONAL_DOMINANCE * 100.0,
            )
            return None
        
        # Count agents agreeing with consensus direction
        agreeing_agents = [s for s in valid_signals if s.direction == direction]
        
        # Max possible = agreeing agents only (institutional standard: 
        # confluence measures conviction strength among aligned agents,
        # dissent is captured separately via agent count gate)
        max_possible = sum(weights.get(s.agent_id, 1.0) * 100.0 for s in agreeing_agents)
        
        # Confluence percentage
        confluence_pct = (raw_score / max_possible * 100.0) if max_possible > 0 else 0.0
        
        if confluence_pct < MIN_CONFLUENCE_SCORE:
            log.info("Fusion REJECTED: Confluence %.1f%% < threshold %.1f%%",
                     confluence_pct, MIN_CONFLUENCE_SCORE)
            return None
        
        if len(agreeing_agents) < MIN_AGENTS_AGREEING:
            log.info("Fusion REJECTED: Only %d agents agree (min %d)",
                     len(agreeing_agents), MIN_AGENTS_AGREEING)
            return None
        
        # Aggregate price levels (weighted average by conviction × weight)
        def weighted_avg_level(attr: str) -> float:
            total_w = sum(weights.get(s.agent_id, 1.0) * abs(s.conviction)
                          for s in agreeing_agents if getattr(s, attr, 0) > 0)
            if total_w == 0: return 0.0
            return sum(weights.get(s.agent_id, 1.0) * abs(s.conviction) * getattr(s, attr)
                       for s in agreeing_agents if getattr(s, attr, 0) > 0) / total_w
        
        # Collect all strategy tags
        all_tags = sorted(set(tag for s in agreeing_agents for tag in s.strategy_tags))

        fused_levels = {
            "entry": weighted_avg_level("entry"),
            "sl": weighted_avg_level("sl"),
            "tp1": weighted_avg_level("tp1"),
            "tp2": weighted_avg_level("tp2"),
            "tp3": weighted_avg_level("tp3"),
        }
        fused_probe = AgentSignal(
            agent_id=agreeing_agents[0].agent_id,
            direction=direction,
            conviction=confluence_pct,
            strategy_tags=all_tags,
            timestamp=now,
            **fused_levels,
        )
        if not valid_geometry(fused_probe):
            log.warning("Fusion REJECTED: fused directional geometry is invalid")
            return None
        
        result = {
            "direction":        direction,
            "confluence_pct":   round(confluence_pct, 2),
            "agents_agreeing":  len(agreeing_agents),
            "agents_total":     len(valid_signals),
            "directional_dominance": round(dominance, 4),
            "strategy_tags":    all_tags,
            **fused_levels,
            "agent_weights":    {AGENT_REGISTRY[aid]['name']: round(w, 3)
                                 for aid, w in weights.items()
                                 if aid in {s.agent_id for s in agreeing_agents}},
            "timestamp":        time.time(),
        }
        
        log.info("Fusion ACCEPTED: %s %.1f%% confluence | Tags: %s",
                 direction, confluence_pct, "+".join(all_tags))
        return result

    def recalculate_agent_performance(self):
        """Recompute Sharpe + PF from journal data, update weights."""
        for aid, journal in self.journals.items():
            outcomes = journal.fetch_rolling_outcomes()
            if len(outcomes) < 3:
                continue
            
            r_vals = np.array([o["outcome_r"] for o in outcomes])
            wins   = r_vals[r_vals > 0]
            losses = r_vals[r_vals < 0]
            
            # Rolling Sharpe (annualized approximation for trade-level R)
            mean_r = r_vals.mean()
            std_r  = r_vals.std() if r_vals.std() > 0 else 0.001
            sharpe = mean_r / std_r * math.sqrt(252)
            
            # Profit Factor
            gross_profit = wins.sum() if len(wins) > 0 else 0
            gross_loss   = abs(losses.sum()) if len(losses) > 0 else 0.001
            pf = gross_profit / gross_loss
            
            win_rate = len(wins) / len(r_vals)
            avg_r    = mean_r
            
            perf = self.performance[aid]
            perf.rolling_sharpe       = round(sharpe, 4)
            perf.rolling_profit_factor = round(pf, 4)
            perf.win_rate             = round(win_rate, 4)
            perf.avg_r_multiple       = round(avg_r, 4)
            perf.total_trades         = len(r_vals)
            perf.current_drawdown_state = (pf < 0.8 or sharpe < -0.5)
            
            # Consecutive losses
            perf.consecutive_losses = 0
            for r in reversed(r_vals):
                if r < 0: perf.consecutive_losses += 1
                else: break
            
            log.info("Agent %02d [%s] | Sharpe=%.2f | PF=%.2f | WR=%.1f%% | Trades=%d",
                     aid, AGENT_REGISTRY[aid]['name'][:12],
                     sharpe, pf, win_rate * 100, len(r_vals))


# ─── Agent 20: Swarm Meta-Reviewer & Journal Auditor ─────────────────────────

class SwarmMetaReviewer:
    """
    Orchestrates Post-Mortem Analysis (PMA) loops.
    Runs within 15 minutes of trade close, forces parameter recalibration.
    """
    
    def __init__(self, journals: dict[int, AgentJournal], 
                 fusion_engine: ConfluenceFusionEngine):
        self.journals = journals
        self.fusion = fusion_engine
        self._pending_pma: list[tuple[TradeTelemetry, float]] = []  # (telemetry, schedule_time)
        log.info("Swarm Meta-Reviewer initialized")

    async def schedule_pma(self, telemetry: TradeTelemetry):
        """Schedule PMA to run 15 minutes after trade close."""
        pma_time = time.time() + (PMA_DELAY_MINUTES * 60)
        self._pending_pma.append((telemetry, pma_time))
        log.info("PMA scheduled for trade %s (agent %d) at T+%dm",
                 telemetry.trade_id, telemetry.agent_id, PMA_DELAY_MINUTES)

    async def pma_dispatch_loop(self):
        """Continuous async loop that fires PMA when scheduled."""
        while True:
            now = time.time()
            due = [(t, s) for t, s in self._pending_pma if s <= now]
            for telemetry, _ in due:
                self._pending_pma.remove((telemetry, _))
                await self.run_post_mortem(telemetry)
            await asyncio.sleep(30)

    async def run_post_mortem(self, t: TradeTelemetry):
        """
        Post-Mortem Analysis Pipeline:
        1. Log outcome to journal
        2. Detect failure patterns
        3. Trigger parameter tightening or contextual blacklisting
        4. Recompute dynamic weights
        5. Emit adaptation log for Telegram
        """
        journal = self.journals.get(t.agent_id)
        if not journal:
            return
        
        # Log outcome
        journal.log_outcome(t)
        adaptation_events = []
        
        # ── Pattern Detection ─────────────────────────────────────────────
        
        # 1. Consecutive losses → parameter tightening
        outcomes = journal.fetch_rolling_outcomes(days=3)
        recent_r = [o["outcome_r"] for o in outcomes[-5:]]
        consecutive_losses = sum(1 for r in reversed(recent_r) if r < 0)
        
        if consecutive_losses >= 3:
            adj = self._tighten_invalidation(journal, t, consecutive_losses)
            if adj:
                adaptation_events.append(adj)
        
        # 2. Context-specific poor performance → blacklisting
        context_key = f"{t.session}_{t.time_of_day[:2]}H"
        ctx_outcomes = [o for o in journal.fetch_rolling_outcomes(days=ROLLING_WINDOW_DAYS)
                        if o["session"] == t.session and
                        o["time_of_day"][:2] == t.time_of_day[:2]]
        
        if len(ctx_outcomes) >= 5:
            ctx_r = [o["outcome_r"] for o in ctx_outcomes]
            ctx_wins   = [r for r in ctx_r if r > 0]
            ctx_losses = [abs(r) for r in ctx_r if r < 0]
            ctx_pf = sum(ctx_wins) / sum(ctx_losses) if ctx_losses else 999
            
            if ctx_pf < MIN_PROFIT_FACTOR:
                adj = self._blacklist_context(journal, context_key, ctx_pf, t)
                if adj:
                    adaptation_events.append(adj)
        
        # 3. High slippage pattern → widen entry tolerance
        if t.slippage_pts > 15 and t.outcome_r < 0:
            adj = self._widen_slippage_filter(journal, t)
            if adj:
                adaptation_events.append(adj)
        
        # Mark PMA as processed — reuse journal's persistent connection
        try:
            conn = journal._get_conn()
            if conn:
                conn.execute("""
                    UPDATE trade_outcomes SET post_mortem_processed = 1
                    WHERE trade_id = ?
                """, (t.trade_id,))
                conn.commit()
        except Exception as exc:
            log.debug("SwarmMetaReviewer: PMA mark processed skipped: %s", exc)
            journal._reset_conn()
        
        # Recompute performance metrics after PMA
        self.fusion.recalculate_agent_performance()
        
        if adaptation_events:
            log.info("Agent %02d PMA complete: %d adaptations applied",
                     t.agent_id, len(adaptation_events))
        
        return adaptation_events

    def _tighten_invalidation(self, journal: AgentJournal, t: TradeTelemetry,
                               n_losses: int) -> Optional[dict]:
        """
        Dynamically tighten structural invalidation tolerance.
        e.g., liquidity sweep threshold 2 pips → 5 pips in high-vol sessions.
        """
        TIGHTEN_MAP = {
            "LONDON": {"sweep_pips": (2.0, 5.0), "fvg_min_pips": (3.0, 6.0)},
            "NY":     {"sweep_pips": (2.0, 4.0), "fvg_min_pips": (2.0, 5.0)},
            "ASIA":   {"sweep_pips": (1.5, 3.5), "fvg_min_pips": (2.0, 4.0)},
            "OFF":    {"sweep_pips": (1.0, 2.5), "fvg_min_pips": (1.5, 3.0)},
        }
        
        session_params = TIGHTEN_MAP.get(t.session, TIGHTEN_MAP["OFF"])
        param_key = "sweep_pips"
        old_val, new_val = session_params[param_key]
        
        reason = (f"Consecutive losses: {n_losses} | Session: {t.session} | "
                  f"Tightening sweep threshold {old_val}→{new_val} pips")
        
        journal.log_parameter_adjustment(
            adj_type="TIGHTEN",
            ctx_key=f"{t.session}_{param_key}",
            old_val=str(old_val),
            new_val=str(new_val),
            reason=reason
        )
        
        return {
            "type": "PARAMETER_TIGHTEN",
            "agent_id": t.agent_id,
            "agent_name": AGENT_REGISTRY[t.agent_id]["name"],
            "param": param_key,
            "old_value": old_val,
            "new_value": new_val,
            "reason": reason,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    def _blacklist_context(self, journal: AgentJournal, context_key: str,
                            pf: float, t: TradeTelemetry) -> Optional[dict]:
        """
        Flag a micro-environment as low-expectancy.
        Agent will mute conviction during these specific states.
        """
        reason = (f"Context PF={pf:.2f} < {MIN_PROFIT_FACTOR} threshold | "
                  f"Context: {context_key} | Muting conviction")
        
        journal.log_parameter_adjustment(
            adj_type="BLACKLIST",
            ctx_key=context_key,
            old_val="ACTIVE",
            new_val="MUTED",
            reason=reason
        )
        
        # Add to in-memory blacklist
        perf = self.fusion.performance.get(t.agent_id)
        if perf and context_key not in perf.blacklisted_contexts:
            perf.blacklisted_contexts.append(context_key)
        
        return {
            "type": "CONTEXT_BLACKLIST",
            "agent_id": t.agent_id,
            "agent_name": AGENT_REGISTRY[t.agent_id]["name"],
            "context": context_key,
            "profit_factor": round(pf, 3),
            "reason": reason,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    def _widen_slippage_filter(self, journal: AgentJournal,
                                t: TradeTelemetry) -> Optional[dict]:
        reason = (f"Slippage {t.slippage_pts:.1f}pts caused losing trade. "
                  f"Widening slippage filter for {t.session} session.")
        
        journal.log_parameter_adjustment(
            adj_type="WIDEN_FILTER",
            ctx_key=f"{t.session}_slippage",
            old_val=f"{t.slippage_pts:.1f}",
            new_val=f"{t.slippage_pts * 1.3:.1f}",
            reason=reason
        )
        
        return {
            "type": "SLIPPAGE_FILTER_WIDEN",
            "agent_id": t.agent_id,
            "session": t.session,
            "slippage_pts": round(t.slippage_pts, 1),
            "reason": reason
        }

    async def generate_weekly_digest(self) -> dict:
        """Produce weekly strategy-by-strategy performance breakdown."""
        digest = {"strategies": {}, "top_strategy": None, "worst_strategy": None}
        
        best_pf, worst_pf = -999, 999
        best_name = worst_name = ""
        
        for aid, journal in self.journals.items():
            outcomes = journal.fetch_rolling_outcomes(days=7)
            if not outcomes:
                continue
            
            r_vals = [o["outcome_r"] for o in outcomes]
            wins   = [r for r in r_vals if r > 0]
            losses = [abs(r) for r in r_vals if r < 0]
            pf     = sum(wins) / sum(losses) if losses else 999
            
            name = AGENT_REGISTRY[aid]["name"]
            digest["strategies"][name] = {
                "trades":       len(r_vals),
                "win_rate":     round(len(wins) / len(r_vals) * 100, 1) if r_vals else 0,
                "profit_factor": round(pf, 2),
                "net_r":        round(sum(r_vals), 2),
                "avg_r":        round(sum(r_vals) / len(r_vals), 2) if r_vals else 0,
            }
            
            if pf > best_pf:
                best_pf = pf
                best_name = name
            if pf < worst_pf:
                worst_pf = pf
                worst_name = name
        
        digest["top_strategy"]   = best_name
        digest["worst_strategy"] = worst_name
        return digest


# ─── Risk Controller (Position Sizing) ───────────────────────────────────────

class InstitutionalRiskController:
    """Agent 18: Dynamic lot sizing with correlation exposure caps."""
    
    def __init__(self, account_balance: float, max_risk_pct: float = 1.0,
                 min_lot: float = 0.01, lot_step: float = 0.01,
                 max_lot: float = 5.0,
                 skip_oversized_min_lot: bool = True):
        require_non_broker_mode("confluence_engine.InstitutionalRiskController")
        self.account_balance = account_balance
        self.max_risk_pct = max_risk_pct
        self.min_lot = min_lot
        self.lot_step = lot_step
        self.max_lot = max_lot
        self.skip_oversized_min_lot = skip_oversized_min_lot
        self.open_positions: list[dict] = []

    def _lot_precision(self) -> int:
        step_text = f"{self.lot_step:.8f}".rstrip("0").rstrip(".")
        return len(step_text.split(".")[1]) if "." in step_text else 0

    def calculate_lot_size(self, entry: float, sl: float,
                            confidence_pct: float) -> float:
        """
        Risk-adjusted lot size:
        Lot = (Account × Risk% × Confidence_Scalar) / (SL_distance_in_price × ContractValue)
        For XAUUSD: 1 standard lot = 100 oz × price per oz
        """
        sl_distance = abs(entry - sl)
        if sl_distance <= 0:
            return 0.0
        
        # Scale risk by confluence confidence (75% conf = 0.75x, 95% = full risk)
        confidence_scalar = min(confidence_pct / 100.0, 1.0)
        risk_amount = self.account_balance * (self.max_risk_pct / 100.0) * confidence_scalar
        
        # XAUUSD: pip value = $1 per 0.01 lot at $1 pip
        # Simplified: lot = risk_amount / (sl_pips * pip_value_per_lot)
        # 1 lot XAUUSD = 100 oz; each $1 move = $100 per lot
        raw_lot = risk_amount / (sl_distance * 100.0)
        if raw_lot < self.min_lot and self.skip_oversized_min_lot:
            return 0.0
        
        # Clamp to broker limits
        lot_precision = self._lot_precision()
        lot_size = round(round(raw_lot / self.lot_step) * self.lot_step, lot_precision)
        lot_size = max(self.min_lot, min(lot_size, self.max_lot))
        return round(lot_size, lot_precision)

    def check_correlation_exposure(self, direction: str) -> bool:
        """Prevent over-exposure in same direction."""
        same_dir = sum(1 for p in self.open_positions if p.get("direction") == direction)
        return same_dir < 3  # Max 3 concurrent same-direction positions


# ─── Initialization & Demo ────────────────────────────────────────────────────

async def main():
    # Initialize journals for all 10 strategy agents
    journals = {aid: AgentJournal(aid) for aid in AGENT_REGISTRY}
    
    # Initialize engines
    fusion   = ConfluenceFusionEngine(journals)
    reviewer = SwarmMetaReviewer(journals, fusion)
    risk     = InstitutionalRiskController(account_balance=10000.0)
    
    # Simulate signals from all 10 agents
    demo_signals = [
        AgentSignal(4,  "BUY", 82.0, 2685.50, 2672.00, 2695.00, 2705.00, 2720.00, ["HTF_SR"]),
        AgentSignal(5,  "BUY", 74.0, 2686.00, 2671.50, 2696.00, 2707.00, 2721.00, ["TL_BREAK"]),
        AgentSignal(6,  "BUY", 68.0, 2685.80, 2672.50, 2694.50, 2704.00, 2718.00, ["CRT_SWEEP"]),
        AgentSignal(7,  "BUY", 91.0, 2685.20, 2672.00, 2695.50, 2706.00, 2722.00, ["SMC_OB", "FVG"]),
        AgentSignal(8,  "SELL", 45.0, 2685.50, 2698.00, 2674.00, 2664.00, 2650.00, ["VPVR_HVN"]),
        AgentSignal(9,  "BUY", 78.0, 2685.30, 2672.00, 2695.00, 2705.50, 2720.00, ["ASIA_SWEEP"]),
        AgentSignal(10, "NEUTRAL", 5.0, 0, 0, 0, 0, 0, ["NEWS_FLAT"]),
        AgentSignal(11, "BUY", 55.0, 2685.60, 2673.00, 2693.00, 2702.00, 2715.00, ["MEAN_REV"]),
        AgentSignal(12, "BUY", 70.0, 2685.40, 2672.50, 2694.00, 2704.50, 2719.00, ["DXY_DIVERGE"]),
        AgentSignal(13, "BUY", 63.0, 2685.70, 2672.00, 2695.20, 2705.80, 2721.50, ["WYCKOFF_SPRING"]),
    ]
    
    # Fuse signals
    result = fusion.fuse_signals(demo_signals)
    
    if result:
        lot = risk.calculate_lot_size(
            result["entry"], result["sl"], result["confluence_pct"]
        )
        result["lot_size"] = lot
        
        print("\n" + "═"*60)
        print("  QUANT-SWARM-XAU FUSION RESULT")
        print("═"*60)
        print(f"  Direction:    {result['direction']}")
        print(f"  Confluence:   {result['confluence_pct']:.1f}%")
        print(f"  Agents:       {result['agents_agreeing']}/{result['agents_total']} agree")
        print(f"  Tags:         {' + '.join(result['strategy_tags'])}")
        print(f"  Entry:        {result['entry']:.2f}")
        print(f"  SL:           {result['sl']:.2f}")
        print(f"  TP1:          {result['tp1']:.2f}")
        print(f"  TP2:          {result['tp2']:.2f}")
        print(f"  TP3:          {result['tp3']:.2f}")
        print(f"  Lot Size:     {lot:.2f}")
        print("═"*60 + "\n")
    else:
        print("\n[FUSION] No trade signal — confluence threshold not met\n")
    
    # Start PMA dispatch loop (runs continuously in production)
    # asyncio.create_task(reviewer.pma_dispatch_loop())

if __name__ == "__main__":
    asyncio.run(main())
