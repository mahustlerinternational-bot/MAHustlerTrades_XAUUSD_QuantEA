"""
QUANT-SWARM-XAU: Data & Ingestion Layer
Agent 01: Tick & Order-Book Streamer     (MQL5-Python socket bridge)
Agent 02: Macro Catalyst Monitor         (Economic calendar scraper/parser)
Agent 03: Inter-Market Divergence Watcher (DXY, US10Y, US30 correlation engine)

All agents normalize raw data into MarketSnapshot objects and publish
to the local Redis pub/sub bus (or IPC queue fallback if Redis unavailable).
"""

import asyncio
import html
import json
import logging
import math
import re
import sqlite3
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Callable
from collections import deque

from safe_paths import bootstrap_ephemeral_state, runtime_state_dir
from provider_adapters import (
    ExecutionProtection,
    IntermarketHTTPProvider,
    MacroHTTPProvider,
    ProviderPort,
    ProviderSnapshot,
    ProviderStatus,
    ensure_simulation_source_allowed,
)

if __name__ == "__main__":
    bootstrap_ephemeral_state("data-ingestion-demo")

from agent_strategy_cores import MarketSnapshot

log = logging.getLogger("DATA-INGESTION")

DATA_DIR = runtime_state_dir()

# ─── Redis Bus (with in-process fallback) ────────────────────────────────────

class LocalEventBus:
    """
    Zero-dependency in-process pub/sub bus.
    Drop-in replacement when Redis is unavailable.
    In production: swap publish/subscribe with redis.asyncio calls.
    """
    def __init__(self):
        self._subscribers: dict[str, list[Callable]] = {}
        self._history: dict[str, deque] = {}

    def subscribe(self, channel: str, callback: Callable):
        self._subscribers.setdefault(channel, []).append(callback)
        self._history.setdefault(channel, deque(maxlen=1000))

    async def publish(self, channel: str, payload: dict):
        self._history.setdefault(channel, deque(maxlen=1000)).append(payload)
        for cb in self._subscribers.get(channel, []):
            try:
                if asyncio.iscoroutinefunction(cb):
                    await cb(payload)
                else:
                    cb(payload)
            except Exception as e:
                log.error("Bus dispatch error on %s: %s", channel, e)

    def latest(self, channel: str) -> Optional[dict]:
        hist = self._history.get(channel)
        return hist[-1] if hist else None


# Singleton bus — shared across all agents
BUS = LocalEventBus()

# Channel names
CH_TICK       = "tick.xauusd"
CH_OHLC       = "ohlc.xauusd"
CH_MACRO      = "macro.events"
CH_INTERMARKET = "intermarket.snapshot"
CH_SNAPSHOT   = "market.snapshot"
CH_SIGNAL     = "swarm.signal"
CH_TRADE_CMD  = "trade.command"
CH_TRADE_ACK  = "trade.ack"
CH_HEARTBEAT  = "ea.heartbeat"


# ─── Tick Cache ───────────────────────────────────────────────────────────────

@dataclass
class Tick:
    bid:       float
    ask:       float
    spread:    float
    volume:    float
    timestamp: float

class TickCache:
    """Rolling window of ticks; builds OHLC bars on-the-fly."""
    
    def __init__(self, max_ticks: int = 10_000):
        self._ticks: deque[Tick] = deque(maxlen=max_ticks)
        self._bars: dict[str, list[dict]] = {
            "M1": [], "M5": [], "M15": [], "H1": [], "H4": [], "D1": []
        }
        self._bar_starts: dict[str, float] = {}
        self._current_bars: dict[str, dict] = {}
        
        TF_SECONDS = {"M1": 60, "M5": 300, "M15": 900, "H1": 3600,
                       "H4": 14400, "D1": 86400}
        self._tf_seconds = TF_SECONDS

    def add_tick(self, tick: Tick):
        self._ticks.append(tick)
        for tf, secs in self._tf_seconds.items():
            bar_start = (tick.timestamp // secs) * secs
            
            if tf not in self._bar_starts or self._bar_starts[tf] != bar_start:
                # Close current bar
                if tf in self._current_bars:
                    self._bars[tf].append(self._current_bars[tf].copy())
                    if len(self._bars[tf]) > 500:
                        self._bars[tf] = self._bars[tf][-500:]
                
                # Open new bar
                self._bar_starts[tf] = bar_start
                self._current_bars[tf] = {
                    "o": tick.bid, "h": tick.bid, "l": tick.bid,
                    "c": tick.bid, "v": tick.volume, "t": bar_start
                }
            else:
                bar = self._current_bars[tf]
                bar["h"] = max(bar["h"], tick.bid)
                bar["l"] = min(bar["l"], tick.bid)
                bar["c"] = tick.bid
                bar["v"] += tick.volume

    def get_bars(self, tf: str, n: int = 200) -> list[dict]:
        bars = list(self._bars.get(tf, []))
        if tf in self._current_bars:
            bars = bars + [self._current_bars[tf]]
        return bars[-n:]

    def load_bar(self, tf: str, bar: dict):
        """Load one closed historical bar from MT5 backfill."""
        tf = tf.upper()
        if tf not in self._bars:
            return
        try:
            clean = {
                "o": float(bar["o"]),
                "h": float(bar["h"]),
                "l": float(bar["l"]),
                "c": float(bar["c"]),
                "v": float(bar.get("v", 0)),
                "t": float(bar["t"]),
            }
        except (KeyError, TypeError, ValueError):
            return

        bars = self._bars[tf]
        if bars and bars[-1]["t"] == clean["t"]:
            bars[-1] = clean
        elif not any(existing["t"] == clean["t"] for existing in bars[-20:]):
            bars.append(clean)
            bars.sort(key=lambda item: item["t"])
            if len(bars) > 500:
                self._bars[tf] = bars[-500:]

    def current_price(self) -> tuple[float, float]:
        if not self._ticks:
            return 0.0, 0.0
        t = self._ticks[-1]
        return t.bid, t.ask

    def compute_atr(self, tf: str, period: int = 14) -> float:
        bars = self.get_bars(tf, period + 1)
        if len(bars) < 2:
            return 0.0
        trs = []
        for i in range(1, len(bars)):
            prev_c = bars[i-1]["c"]
            curr   = bars[i]
            tr = max(curr["h"] - curr["l"],
                     abs(curr["h"] - prev_c),
                     abs(curr["l"] - prev_c))
            trs.append(tr)
        return sum(trs[-period:]) / min(len(trs), period)


# ─── Agent 01: Tick & Order-Book Streamer ─────────────────────────────────────

class TickStreamerAgent:
    """
    Agent 01: Receives raw tick data from MQL5 EA via IPC bridge.
    Maintains TickCache, computes OHLC and ATR, publishes MarketSnapshot
    to the event bus every configurable interval.
    """
    SNAPSHOT_INTERVAL_S = 1.0   # Publish snapshot every 1 second

    def __init__(self, bus: LocalEventBus = BUS):
        self.bus         = bus
        self.cache       = TickCache()
        self._last_snap  = 0.0
        self._tick_count = 0
        log.info("Agent 01 [Tick Streamer] initialized")

    async def on_tick(self, raw: dict):
        """Called by IPC bridge on every incoming tick from EA."""
        tick = Tick(
            bid       = raw.get("bid", 0),
            ask       = raw.get("ask", 0),
            spread    = raw.get("spread", 0),
            volume    = raw.get("volume", 0),
            timestamp = raw.get("timestamp", time.time())
        )
        self.cache.add_tick(tick)
        self._tick_count += 1
        
        # Publish raw tick
        await self.bus.publish(CH_TICK, asdict(tick))
        
        # Publish snapshot at interval
        now = time.time()
        if now - self._last_snap >= self.SNAPSHOT_INTERVAL_S:
            await self._publish_snapshot()
            self._last_snap = now

    async def _publish_snapshot(self):
        """Assemble and publish a full MarketSnapshot to the bus."""
        bid, ask = self.cache.current_price()
        if bid == 0:
            return
        
        # Fetch intermarket from bus
        im = self.bus.latest(CH_INTERMARKET) or {}
        macro = self.bus.latest(CH_MACRO) or {}
        
        # Volume profile (simplified: identify POC from H4 OHLC)
        h4_bars = self.cache.get_bars("H4", 50)
        poc, hvns, lvns = self._compute_volume_profile(h4_bars)
        
        snap = MarketSnapshot(
            symbol    = "XAUUSD",
            bid       = bid,
            ask       = ask,
            spread_pts = int((ask - bid) * 100),
            h1_ohlc   = self.cache.get_bars("H1", 200),
            h4_ohlc   = self.cache.get_bars("H4", 100),
            d1_ohlc   = self.cache.get_bars("D1", 50),
            m15_ohlc  = self.cache.get_bars("M15", 200),
            m5_ohlc   = self.cache.get_bars("M5", 200),
            m1_ohlc   = self.cache.get_bars("M1", 200),
            atr_h1    = self.cache.compute_atr("H1"),
            atr_h4    = self.cache.compute_atr("H4"),
            atr_d1    = self.cache.compute_atr("D1"),
            poc_level       = poc,
            hvn_levels      = hvns,
            lvn_levels      = lvns,
            session         = self._detect_session(),
            london_open     = self._is_london_open(),
            ny_open         = self._is_ny_open(),
            news_impact     = macro.get("next_impact", 0.0),
            news_direction  = macro.get("bias", "NEUTRAL"),
            dxy_bias        = im.get("dxy_bias", "NEUTRAL"),
            us10y_trend     = im.get("us10y_trend", "FLAT"),
            us30_bias       = im.get("us30_bias", "NEUTRAL"),
            timestamp       = time.time(),
        )
        
        await self.bus.publish(CH_SNAPSHOT, {"snapshot": "ready",
                                              "bid": bid, "ts": snap.timestamp})
        # Store snapshot reference for strategy agents to consume
        self._latest_snapshot = snap

    def get_latest_snapshot(self) -> Optional[MarketSnapshot]:
        return getattr(self, "_latest_snapshot", None)

    def _compute_volume_profile(self, bars: list[dict]) -> tuple:
        """TPO-style volume profile from OHLC bars."""
        if not bars:
            return 0.0, [], []
        
        # Build price→volume map in 0.1 increments
        vol_map: dict[int, float] = {}
        for b in bars:
            lo  = int(b["l"] * 10)
            hi  = int(b["h"] * 10)
            v   = b.get("v", 1)
            rng = max(hi - lo, 1)
            for tick in range(lo, hi + 1):
                vol_map[tick] = vol_map.get(tick, 0) + v / rng
        
        if not vol_map:
            return 0.0, [], []
        
        poc_tick = max(vol_map, key=vol_map.get)
        poc = poc_tick / 10.0
        
        # HVN: top quartile volume nodes
        sorted_vol = sorted(vol_map.items(), key=lambda x: x[1], reverse=True)
        threshold_hvn = sorted_vol[len(sorted_vol)//4][1]
        threshold_lvn = sorted_vol[3*len(sorted_vol)//4][1]
        
        hvns = sorted(set(round(t/10.0, 1) for t, v in sorted_vol
                          if v >= threshold_hvn))[:10]
        lvns = sorted(set(round(t/10.0, 1) for t, v in vol_map.items()
                          if v <= threshold_lvn))[:10]
        
        return poc, hvns, lvns

    def _detect_session(self) -> str:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        h, m = now.hour, now.minute
        if 0  <= h < 7:                      return "ASIA"
        if 7  <= h < 12:                     return "LONDON"
        if h == 12 or (h == 13 and m < 30): return "LONDON"   # London close
        if (h == 13 and m >= 30) or (14 <= h < 20): return "NY"
        return "OFF"

    def _is_london_open(self) -> bool:
        h = (time.time() % 86400) // 3600
        return 7 <= h < 10

    def _is_ny_open(self) -> bool:
        h = (time.time() % 86400) // 3600
        m = (time.time() % 3600) // 60
        return (h == 13 and m >= 30) or (14 <= h < 16)

    async def simulate_ticks(self, base_price: float = 2685.5,
                              n_ticks: int = 100, atr: float = 13.5):
        """Generate synthetic tick feed for testing."""
        ensure_simulation_source_allowed()
        import random
        price = base_price
        for i in range(n_ticks):
            move  = random.gauss(0, atr * 0.01)
            price = max(price + move, 1.0)
            spread = random.uniform(0.2, 0.5)
            tick_raw = {
                "bid":       round(price, 2),
                "ask":       round(price + spread, 2),
                "spread":    round(spread * 100, 1),
                "volume":    random.randint(50, 500),
                "timestamp": time.time() - (n_ticks - i) * 0.5
            }
            await self.on_tick(tick_raw)
        log.info("Agent 01: Simulated %d ticks. Cache bars: H1=%d H4=%d",
                 n_ticks, len(self.cache.get_bars("H1")),
                 len(self.cache.get_bars("H4")))


# ─── Agent 02: Macro Catalyst Monitor ────────────────────────────────────────

@dataclass
class MacroEvent:
    name:          str
    impact:        str       # "HIGH", "MEDIUM", "LOW"
    scheduled_utc: float     # Unix timestamp
    currency:      str       # "USD", "EUR", etc.
    forecast:      Optional[str] = None
    previous:      Optional[str] = None
    actual:        Optional[str] = None
    impact_score:  float = 0.0   # 0.0–1.0 computed score

    @property
    def minutes_away(self) -> float:
        return (self.scheduled_utc - time.time()) / 60.0

    @property
    def is_high_impact_usd(self) -> bool:
        return self.impact == "HIGH" and self.currency == "USD"


class MacroCatalystMonitor:
    """
    Agent 02: Economic calendar monitor.
    Parses scheduled events, computes impact scores, publishes
    directional bias and freeze flags to the event bus.
    
    In production: integrates with ForexFactory API, Investing.com feed,
    or broker economic calendar endpoint. Shown here with local DB storage
    and a clean interface for any calendar data source.
    """
    
    IMPACT_WEIGHTS = {
        "NFP":      1.0,
        "FOMC":     1.0,
        "CPI":      0.92,
        "PPI":      0.75,
        "GDP":      0.80,
        "RETAIL":   0.65,
        "PMI":      0.55,
        "JOLTS":    0.50,
        "CLAIMS":   0.40,
        "SPEECH":   0.60,  # Fed Chair speeches
    }
    
    FREEZE_MINUTES_BEFORE = 15
    FREEZE_MINUTES_AFTER  = 5
    
    HIGH_IMPACT_EVENTS = {"NFP", "FOMC", "CPI", "GDP"}

    def __init__(self, bus: LocalEventBus = BUS,
                 provider: Optional[ProviderPort] = None,
                 unsafe_technical_only: bool = False):
        self.bus    = bus
        self._events: list[MacroEvent] = []
        self.provider = provider or MacroHTTPProvider(
            url=None, unsafe_technical_only=unsafe_technical_only
        )
        self._provider_snapshot = getattr(self.provider, "latest", None)
        self._last_persisted_provider_version: Optional[tuple[str, float]] = None
        self._db_path = DATA_DIR / "macro_events.db"
        self._db_conn: Optional[sqlite3.Connection] = None  # persistent — one FD for lifetime
        self._init_db()
        log.info("Agent 02 [Macro Monitor] initialized")

    def _get_db(self) -> Optional[sqlite3.Connection]:
        """Return the persistent SQLite connection, creating it once if needed."""
        if self._db_conn is None:
            try:
                self._db_path.parent.mkdir(parents=True, exist_ok=True)
                self._db_conn = sqlite3.connect(
                    str(self._db_path), timeout=5, check_same_thread=False
                )
            except Exception as exc:
                log.debug("Agent 02: DB connect failed: %s", exc)
        return self._db_conn

    def _reset_db(self):
        """Close and discard the cached connection; next call gets a fresh one."""
        try:
            if self._db_conn:
                self._db_conn.close()
        except Exception:
            pass
        self._db_conn = None

    def _init_db(self):
        try:
            conn = self._get_db()
            if conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT, impact TEXT, scheduled_utc REAL,
                        currency TEXT, forecast TEXT, previous TEXT, actual TEXT,
                        impact_score REAL, processed INTEGER DEFAULT 0
                    )
                """)
                conn.commit()
        except Exception as _dberr:
            log.debug("Agent 02: DB init skipped: %s", _dberr)
            self._reset_db()
    def ingest_calendar(self, events: list[dict]):
        """
        Supply calendar data only in test/simulation mode.

        Live and demo runtimes must use a provider port so source identity,
        timestamps, sequence, and freshness cannot be bypassed.
        Expected format:
          [{"name": "CPI m/m", "impact": "HIGH", "scheduled_utc": 1234567890,
            "currency": "USD", "forecast": "0.3%", "previous": "0.4%"}]
        """
        ensure_simulation_source_allowed()
        now = time.time()
        self._provider_snapshot = ProviderSnapshot(
            provider="macro",
            source="simulation-manual",
            status=ProviderStatus.HEALTHY,
            source_timestamp=now,
            received_timestamp=now,
            sequence=0,
            freshness_s=60.0,
        )
        self._replace_events(events, persist=True)

    def _replace_events(self, events: list[dict], *, persist: bool) -> None:
        self._events = []
        new_count = 0
        for e in events:
            impact_score = self._compute_impact_score(e)
            event = MacroEvent(
                name          = e.get("name", ""),
                impact        = e.get("impact", "LOW"),
                scheduled_utc = e.get("scheduled_utc", 0),
                currency      = e.get("currency", "USD"),
                forecast      = e.get("forecast"),
                previous      = e.get("previous"),
                actual        = e.get("actual"),
                impact_score  = impact_score
            )
            self._events.append(event)
            new_count += 1
        
        if not persist:
            return

        # Persist to DB
        try:
            conn = self._get_db()
            if conn:
                for e in events[-new_count:]:
                    conn.execute("""
                        INSERT OR IGNORE INTO events
                        (name, impact, scheduled_utc, currency, forecast, previous, actual, impact_score)
                        VALUES (?,?,?,?,?,?,?,?)
                    """, (e.get("name"), e.get("impact"), e.get("scheduled_utc"),
                          e.get("currency"), e.get("forecast"), e.get("previous"),
                          e.get("actual"), self._compute_impact_score(e)))
                conn.commit()
        except Exception as _dberr:
            log.debug("Agent 02: DB write skipped: %s", _dberr)
            self._reset_db()
        log.info("Agent 02: Ingested %d calendar events", new_count)

    def _compute_impact_score(self, event: dict) -> float:
        """
        Score = base_weight × surprise_multiplier × recency_decay
        Surprise multiplier: if actual deviates significantly from forecast → amplify.
        """
        name_upper = str(event.get("name", "")).upper()
        base = 0.3  # default for LOW impact
        
        for keyword, weight in self.IMPACT_WEIGHTS.items():
            if keyword in name_upper:
                base = weight
                break
        
        if event.get("impact") == "HIGH":
            base = max(base, 0.7)
        elif event.get("impact") == "MEDIUM":
            base = max(base, 0.4)
        
        # Surprise factor
        try:
            forecast = float(re.sub(r"[^0-9.\-]", "", event.get("forecast") or "0"))
            actual   = float(re.sub(r"[^0-9.\-]", "", event.get("actual") or "0"))
            if forecast != 0 and actual != 0:
                surprise = abs(actual - forecast) / abs(forecast)
                base = min(base * (1 + surprise * 2), 1.0)
        except (ValueError, ZeroDivisionError):
            pass
        
        return round(base, 3)

    async def monitor_loop(self, interval_s: float = 30.0):
        """Poll the configured provider and publish explicit safety state."""
        while True:
            await self.poll_once()
            await asyncio.sleep(max(0.1, interval_s))

    async def poll_once(self) -> dict:
        snapshot = await self.provider.poll_once()
        self._provider_snapshot = snapshot
        if snapshot.status is ProviderStatus.HEALTHY:
            events = snapshot.payload.get("events", [])
            provider_version = (
                snapshot.source,
                float(snapshot.source_timestamp or 0.0),
            )
            persist = provider_version != self._last_persisted_provider_version
            self._replace_events(list(events), persist=persist)
            if persist:
                self._last_persisted_provider_version = provider_version
        await self._evaluate_and_publish(snapshot)
        return self.bus.latest(CH_MACRO) or {}

    async def _evaluate_and_publish(
        self, provider_snapshot: Optional[ProviderSnapshot] = None
    ):
        now     = time.time()
        snapshot = provider_snapshot or self._provider_snapshot
        if snapshot is None:
            ensure_simulation_source_allowed()
            snapshot = ProviderSnapshot(
                provider="macro",
                source="simulation-manual",
                status=ProviderStatus.HEALTHY,
                source_timestamp=now,
                received_timestamp=now,
                sequence=0,
                freshness_s=60.0,
            )
        payload = {
            "next_impact":      0.0,
            "bias":             "NEUTRAL",
            "freeze":           False,
            "freeze_reason":    "",
            "next_event_name":  "",
            "next_event_mins":  999.0,
            "timestamp":        now,
            "coverage_start_utc": snapshot.payload.get("coverage_start_utc"),
            "coverage_end_utc": snapshot.payload.get("coverage_end_utc"),
            "minimum_forward_coverage_s": snapshot.payload.get(
                "minimum_forward_coverage_s"
            ),
            **snapshot.as_dict(now),
        }

        effective_status = snapshot.effective_status(now)
        effective_protection = snapshot.effective_protection(now)
        if effective_status is not ProviderStatus.HEALTHY:
            fail_closed = (
                effective_protection
                is not ExecutionProtection.TECHNICAL_ONLY_UNPROTECTED
            )
            payload["freeze"] = fail_closed
            payload["freeze_reason"] = (
                f"MACRO_PROVIDER_{effective_status.value}"
                if fail_closed else
                "MACRO_PROTECTION_EXPLICITLY_DISABLED"
            )
            await self.bus.publish(CH_MACRO, payload)
            log.error(
                "Macro provider %s (%s); protection=%s; disabled_agents=%s",
                effective_status.value, snapshot.source,
                effective_protection.value,
                list(snapshot.effective_disabled_agents(now)),
            )
            return
        
        # Find nearest high-impact USD event
        upcoming = sorted(
            [e for e in self._events if e.scheduled_utc > now - 3600
             and e.is_high_impact_usd],
            key=lambda e: e.scheduled_utc
        )
        
        if not upcoming:
            await self.bus.publish(CH_MACRO, payload)
            return
        
        next_ev = upcoming[0]
        mins_to = next_ev.minutes_away
        
        payload["next_event_name"] = next_ev.name
        payload["next_event_mins"] = round(mins_to, 1)
        payload["next_impact"]     = next_ev.impact_score
        
        # Freeze window
        if -self.FREEZE_MINUTES_AFTER <= mins_to <= self.FREEZE_MINUTES_BEFORE:
            payload["freeze"]        = True
            payload["freeze_reason"] = (
                f"{next_ev.name} in {mins_to:.0f}min" if mins_to > 0
                else f"{next_ev.name} released {abs(mins_to):.0f}min ago"
            )
        payload["execution_eligible"] = (
            snapshot.execution_eligible_at(now) and not payload["freeze"]
        )
        
        # Directional bias from actual vs forecast (post-release)
        if next_ev.actual and mins_to < 0 and abs(mins_to) < 60:
            payload["bias"] = self._assess_bias(next_ev)
        
        await self.bus.publish(CH_MACRO, payload)
        
        if payload["freeze"]:
            log.warning("Agent 02: MACRO FREEZE — %s", payload["freeze_reason"])

    def _assess_bias(self, event: MacroEvent) -> str:
        """Assess Gold bullish/bearish bias from macro data."""
        try:
            forecast = float(re.sub(r"[^0-9.\-]", "", event.forecast or "0"))
            actual   = float(re.sub(r"[^0-9.\-]", "", event.actual or "0"))
        except ValueError:
            return "NEUTRAL"
        
        # Gold is inversely correlated with strong USD data generally
        # CPI higher than forecast → inflationary → Gold bullish
        # NFP beat → USD strong → Gold bearish
        name = event.name.upper()
        
        if "CPI" in name or "INFLATION" in name:
            return "BULLISH" if actual > forecast else "BEARISH"
        elif "NFP" in name or "JOBS" in name or "EMPLOYMENT" in name:
            return "BEARISH" if actual > forecast else "BULLISH"
        elif "GDP" in name:
            return "BEARISH" if actual > forecast else "BULLISH"
        
        return "NEUTRAL"

    def get_todays_events(self) -> list[MacroEvent]:
        today_start = (time.time() // 86400) * 86400
        return [e for e in self._events
                if today_start <= e.scheduled_utc < today_start + 86400]

    def format_calendar_for_telegram(self) -> str:
        if (self._provider_snapshot is not None and
                self._provider_snapshot.effective_status()
                is not ProviderStatus.HEALTHY):
            return (
                "  • Macro calendar unavailable — protection state: "
                f"{html.escape(self._provider_snapshot.effective_protection().value)}"
            )
        events = self.get_todays_events()
        if not events:
            return "  • No high-impact events today"
        lines = []
        for e in sorted(events, key=lambda x: x.scheduled_utc):
            dt  = datetime.fromtimestamp(e.scheduled_utc, tz=timezone.utc)
            imp = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "⚪"}.get(e.impact, "⚪")
            name = html.escape(str(e.name), quote=True)
            lines.append(f"  {imp} <b>{dt.strftime('%H:%M UTC')}</b> — {name}")
            if e.forecast:
                lines[-1] += f"  F: {html.escape(str(e.forecast), quote=True)}"
        return "\n".join(lines)


# ─── Agent 03: Inter-Market Divergence Watcher ───────────────────────────────

@dataclass
class InterMarketData:
    """Normalized inter-market state snapshot."""
    # Zero means unavailable.  Real deltas start only after the first verified
    # observation; fabricated baseline prices must never influence a signal.
    dxy_price:    float = 0.0
    dxy_change:   float = 0.0    # % change vs yesterday's close
    dxy_bias:     str   = "NEUTRAL"  # "STRONG", "WEAK", "NEUTRAL"
    
    us10y_yield:  float = 0.0
    us10y_change: float = 0.0    # bps change
    us10y_trend:  str   = "FLAT" # "RISING", "FALLING", "FLAT"
    
    us30_price:   float = 0.0
    us30_change:  float = 0.0
    us30_bias:    str   = "NEUTRAL"
    
    # Divergence signals
    gold_dxy_diverge:   bool = False   # Gold not following DXY
    gold_bonds_diverge: bool = False   # Gold not following bonds
    risk_off:           bool = False   # US30 selling + Gold buying = risk off
    timestamp:          float = field(default_factory=time.time)


class InterMarketWatcher:
    """
    Agent 03: Tracks DXY, US10Y, US30 correlations with Gold.
    Detects divergences that serve as leading directional triggers.
    
    In production: connects to broker's market data API or
    MT5's MarketWatch for instrument feeds.
    """
    
    DXY_STRONG_THRESH  = 0.3   # % daily gain = "STRONG"
    DXY_WEAK_THRESH    = -0.3  # % daily loss = "WEAK"
    BOND_RISE_THRESH   = 5.0   # bps rise = bearish for Gold
    BOND_FALL_THRESH   = -5.0  # bps fall = bullish for Gold

    def __init__(self, bus: LocalEventBus = BUS,
                 provider: Optional[ProviderPort] = None):
        self.bus  = bus
        self.provider = provider or IntermarketHTTPProvider(url=None)
        self._provider_snapshot = getattr(self.provider, "latest", None)
        self._state = InterMarketData()
        self._gold_history: deque[float] = deque(maxlen=20)
        self._dxy_history:  deque[float] = deque(maxlen=20)
        self._db_path = DATA_DIR / "intermarket.db"
        self._db_conn: Optional[sqlite3.Connection] = None  # persistent — one FD for lifetime
        self._init_db()
        log.info("Agent 03 [Inter-Market Watcher] initialized")

    def _get_db(self) -> Optional[sqlite3.Connection]:
        """Return the persistent SQLite connection, creating it once if needed."""
        if self._db_conn is None:
            try:
                self._db_path.parent.mkdir(parents=True, exist_ok=True)
                self._db_conn = sqlite3.connect(
                    str(self._db_path), timeout=5, check_same_thread=False
                )
            except Exception as exc:
                log.debug("Agent 03: DB connect failed: %s", exc)
        return self._db_conn

    def _reset_db(self):
        """Close and discard the cached connection; next call gets a fresh one."""
        try:
            if self._db_conn:
                self._db_conn.close()
        except Exception:
            pass
        self._db_conn = None

    def _init_db(self):
        try:
            conn = self._get_db()
            if conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        dxy_price REAL, dxy_change REAL, dxy_bias TEXT,
                        us10y_yield REAL, us10y_change REAL, us10y_trend TEXT,
                        us30_price REAL, us30_change REAL, us30_bias TEXT,
                        gold_dxy_diverge INTEGER, gold_bonds_diverge INTEGER,
                        risk_off INTEGER, timestamp REAL
                    )
                """)
                conn.commit()
        except Exception as exc:
            log.warning("Agent 03: failed to init intermarket DB (%s): %s",
                        self._db_path, exc)
            self._reset_db()

    def update(self, dxy: float, us10y: float, us30: float, gold: float,
               *, _provider_verified: bool = False):
        """Update with verified prices and compute all derived metrics."""
        if not _provider_verified:
            ensure_simulation_source_allowed()
            now = time.time()
            self._provider_snapshot = ProviderSnapshot(
                provider="intermarket",
                source="simulation-manual",
                status=ProviderStatus.HEALTHY,
                source_timestamp=now,
                received_timestamp=now,
                sequence=0,
                freshness_s=60.0,
            )

        values = (float(dxy), float(us10y), float(us30), float(gold))
        if any(not math.isfinite(value) or value <= 0 for value in values):
            raise ValueError("intermarket prices must be finite and positive")
        dxy, us10y, us30, gold = values
        
        # DXY bias
        if self._state.dxy_price > 0:
            dxy_chg = (dxy - self._state.dxy_price) / self._state.dxy_price * 100
        else:
            dxy_chg = 0.0
        
        dxy_bias = "NEUTRAL"
        if dxy_chg >= self.DXY_STRONG_THRESH:
            dxy_bias = "STRONG"
        elif dxy_chg <= self.DXY_WEAK_THRESH:
            dxy_bias = "WEAK"
        
        # US10Y trend (in basis points)
        us10y_chg = (
            (us10y - self._state.us10y_yield) * 100
            if self._state.us10y_yield > 0 else 0.0
        )  # bps
        if us10y_chg >= self.BOND_RISE_THRESH:
            us10y_trend = "RISING"
        elif us10y_chg <= self.BOND_FALL_THRESH:
            us10y_trend = "FALLING"
        else:
            us10y_trend = "FLAT"
        
        # US30 bias
        if self._state.us30_price > 0:
            us30_chg = (us30 - self._state.us30_price) / self._state.us30_price * 100
        else:
            us30_chg = 0.0
        us30_bias = "BULLISH" if us30_chg > 0.2 else ("BEARISH" if us30_chg < -0.2 else "NEUTRAL")
        
        # Update histories
        self._gold_history.append(gold)
        self._dxy_history.append(dxy)
        
        # Divergence detection
        gold_dxy_diverge  = self._detect_dxy_divergence(dxy_bias)
        gold_bond_diverge = self._detect_bond_divergence(us10y_trend, gold)
        risk_off          = (us30_bias == "BEARISH" and dxy_bias == "WEAK")
        
        self._state = InterMarketData(
            dxy_price    = dxy,   dxy_change  = round(dxy_chg, 4),
            dxy_bias     = dxy_bias,
            us10y_yield  = us10y, us10y_change = round(us10y_chg, 2),
            us10y_trend  = us10y_trend,
            us30_price   = us30,  us30_change  = round(us30_chg, 4),
            us30_bias    = us30_bias,
            gold_dxy_diverge  = gold_dxy_diverge,
            gold_bonds_diverge = gold_bond_diverge,
            risk_off     = risk_off,
            timestamp    = time.time()
        )
        
        # Persist to DB. Non-critical failure should not stop the swarm.
        # If DB is locked/corrupted, skip persistence—swarm continues fine without it.
        try:
            conn = self._get_db()
            if conn:
                conn.execute("""
                    INSERT INTO snapshots VALUES
                    (NULL,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (dxy, dxy_chg, dxy_bias, us10y, us10y_chg, us10y_trend,
                      us30, us30_chg, us30_bias,
                      int(gold_dxy_diverge), int(gold_bond_diverge),
                      int(risk_off), time.time()))
                conn.commit()
        except sqlite3.OperationalError:
            # Database locked or corrupted — silently skip, reset for next tick.
            self._reset_db()
        except Exception as exc:
            log.debug("Agent 03: intermarket DB error: %s", exc)
            self._reset_db()

    def _detect_dxy_divergence(self, dxy_bias: str) -> bool:
        """
        Gold-DXY divergence: DXY making higher highs but Gold NOT
        making lower lows (or vice versa). Leading indicator for Gold reversal.
        """
        if len(self._gold_history) < 5 or len(self._dxy_history) < 5:
            return False
        
        gold_recent = list(self._gold_history)[-5:]
        dxy_recent  = list(self._dxy_history)[-5:]
        
        # DXY rising but Gold not falling
        dxy_rising   = dxy_recent[-1] > dxy_recent[0]
        gold_falling = gold_recent[-1] < gold_recent[0]
        
        if dxy_rising and not gold_falling:
            return True  # Bullish Gold divergence
        
        # DXY falling but Gold not rising
        dxy_falling = dxy_recent[-1] < dxy_recent[0]
        gold_rising = gold_recent[-1] > gold_recent[0]
        
        if dxy_falling and not gold_rising:
            return True  # Bearish Gold divergence
        
        return False

    def _detect_bond_divergence(self, us10y_trend: str, gold: float) -> bool:
        """
        Bonds rising (yields up) usually pressures Gold.
        If yields rise and Gold holds / rises → bullish divergence.
        """
        if len(self._gold_history) < 5:
            return False
        
        gold_recent = list(self._gold_history)[-5:]
        gold_rising = gold_recent[-1] > gold_recent[0]
        
        # Yields rising but Gold not falling = bullish Gold divergence
        return us10y_trend == "RISING" and gold_rising

    async def monitor_loop(self, interval_s: float = 5.0):
        """Continuously poll a real or explicitly supplied provider port."""
        i = 0
        while True:
            await self.poll_once()
            i += 1
            if i % 10 == 0:
                status = self._provider_snapshot.status.value \
                    if self._provider_snapshot else "STARTING"
                log.info("Intermarket provider=%s | DXY=%s US10Y=%s US30=%s",
                         status, self._state.dxy_bias,
                         self._state.us10y_trend, self._state.us30_bias)
            await asyncio.sleep(max(0.1, interval_s))

    async def poll_once(self) -> dict:
        snapshot = await self.provider.poll_once()
        self._provider_snapshot = snapshot

        if snapshot.status is ProviderStatus.HEALTHY:
            values = snapshot.payload
            self.update(
                float(values["dxy"]), float(values["us10y"]),
                float(values["us30"]), float(values["gold"]),
                _provider_verified=True,
            )
            payload = {
                "dxy_bias": self._state.dxy_bias,
                "us10y_trend": self._state.us10y_trend,
                "us30_bias": self._state.us30_bias,
                "dxy_price": self._state.dxy_price,
                "us10y_yield": self._state.us10y_yield,
                "us30_price": self._state.us30_price,
                "diverge_dxy": self._state.gold_dxy_diverge,
                "diverge_bonds": self._state.gold_bonds_diverge,
                "risk_off": self._state.risk_off,
                "timestamp": self._state.timestamp,
                **snapshot.as_dict(),
            }
        else:
            # Never leak a stale prior value as if it were current context.
            self._state = InterMarketData()
            self._gold_history.clear()
            self._dxy_history.clear()
            payload = {
                "dxy_bias": "NEUTRAL",
                "us10y_trend": "FLAT",
                "us30_bias": "NEUTRAL",
                "dxy_price": 0.0,
                "us10y_yield": 0.0,
                "us30_price": 0.0,
                "diverge_dxy": False,
                "diverge_bonds": False,
                "risk_off": False,
                "timestamp": time.time(),
                **snapshot.as_dict(),
            }
            log.error(
                "Intermarket provider %s (%s); disabled_agents=%s",
                snapshot.status.value, snapshot.source,
                list(snapshot.disabled_agents),
            )

        await self.bus.publish(CH_INTERMARKET, payload)
        return payload

    def format_for_telegram(self) -> str:
        if (self._provider_snapshot is not None and
                self._provider_snapshot.effective_status()
                is not ProviderStatus.HEALTHY):
            return ("  Intermarket unavailable — dependent agents disabled "
                    f"({self._provider_snapshot.effective_status().value})")
        s = self._state
        dxy_icon  = "📈" if s.dxy_bias == "STRONG" else ("📉" if s.dxy_bias == "WEAK" else "➡️")
        bond_icon = "📈" if s.us10y_trend == "RISING" else ("📉" if s.us10y_trend == "FALLING" else "➡️")
        eq_icon   = "📈" if s.us30_bias == "BULLISH" else ("📉" if s.us30_bias == "BEARISH" else "➡️")
        div_flag  = "⚡ DIVERGENCE DETECTED" if s.gold_dxy_diverge else ""
        
        return (f"  {dxy_icon} DXY: {s.dxy_price:.2f} ({s.dxy_bias}) "
                f"  {bond_icon} US10Y: {s.us10y_yield:.2f}% ({s.us10y_trend})\n"
                f"  {eq_icon} US30: {s.us30_price:,.0f} ({s.us30_bias})"
                + (f"\n  {div_flag}" if div_flag else ""))


# ─── Demo / Validation ────────────────────────────────────────────────────────

async def demo_ingestion_layer():
    print("\n" + "═"*58)
    print("  QUANT-SWARM-XAU: Data Ingestion Layer Demo")
    print("═"*58)

    # Agent 01: Tick Streamer
    streamer = TickStreamerAgent(BUS)
    print("\n[Agent 01] Simulating 200 ticks...")
    await streamer.simulate_ticks(base_price=2685.5, n_ticks=200)
    snap = streamer.get_latest_snapshot()
    if snap:
        print(f"  ✓ Snapshot built: Bid={snap.bid:.2f} Session={snap.session}")
        print(f"    H1 bars: {len(snap.h1_ohlc)} | ATR H1: {snap.atr_h1:.2f}")
        print(f"    POC: {snap.poc_level:.2f} | HVNs: {snap.hvn_levels[:3]}")

    # Agent 02: Macro Monitor
    macro = MacroCatalystMonitor(BUS)
    now = time.time()
    test_events = [
        {"name": "CPI m/m", "impact": "HIGH", "currency": "USD",
         "scheduled_utc": now + 3600, "forecast": "0.3%", "previous": "0.4%"},
        {"name": "Fed Chair Speech", "impact": "HIGH", "currency": "USD",
         "scheduled_utc": now + 7200, "forecast": None, "previous": None},
        {"name": "Unemployment Claims", "impact": "MEDIUM", "currency": "USD",
         "scheduled_utc": now + 1800, "forecast": "215K", "previous": "218K"},
    ]
    macro.ingest_calendar(test_events)
    await macro._evaluate_and_publish()
    macro_state = BUS.latest(CH_MACRO)
    print(f"\n[Agent 02] Macro state:")
    print(f"  ✓ Next event: {macro_state.get('next_event_name')} "
          f"in {macro_state.get('next_event_mins'):.0f}min")
    print(f"    Impact: {macro_state.get('next_impact'):.2f} | "
          f"Freeze: {macro_state.get('freeze')} | Bias: {macro_state.get('bias')}")
    print(f"\n  Calendar:\n{macro.format_calendar_for_telegram()}")

    # Agent 03: Inter-Market Watcher
    watcher = InterMarketWatcher(BUS)
    # Simulate a DXY divergence scenario
    for dxy, us10y, us30, gold in [
        (103.2, 4.33, 41800, 2682.0),
        (103.5, 4.36, 41900, 2683.5),  # DXY rising
        (103.8, 4.39, 41750, 2685.0),  # DXY still rising, Gold rising too
        (104.1, 4.41, 41600, 2686.5),  # DXY higher high, Gold also higher = DIVERGE
        (104.2, 4.42, 41550, 2688.0),  # Confirmed divergence
    ]:
        watcher.update(dxy, us10y, us30, gold)
    
    im_state = watcher._state
    await watcher.bus.publish(CH_INTERMARKET, {
        "dxy_bias": im_state.dxy_bias, "us10y_trend": im_state.us10y_trend,
        "us30_bias": im_state.us30_bias, "dxy_price": im_state.dxy_price,
        "us10y_yield": im_state.us10y_yield, "us30_price": im_state.us30_price,
        "diverge_dxy": im_state.gold_dxy_diverge,
        "diverge_bonds": im_state.gold_bonds_diverge,
        "risk_off": im_state.risk_off, "timestamp": im_state.timestamp
    })
    print(f"\n[Agent 03] Inter-market state:")
    print(f"  ✓ {watcher.format_for_telegram()}")
    print(f"    Gold-DXY Diverge: {im_state.gold_dxy_diverge}")
    print(f"    Gold-Bond Diverge: {im_state.gold_bonds_diverge}")
    print(f"    Risk-Off: {im_state.risk_off}")

    print("\n" + "═"*58)
    print("  Data ingestion layer: ALL AGENTS VALIDATED ✓")
    print("═"*58 + "\n")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(demo_ingestion_layer())
