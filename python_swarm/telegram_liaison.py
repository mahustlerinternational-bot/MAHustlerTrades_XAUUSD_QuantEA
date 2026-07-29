"""
QUANT-SWARM-XAU: Agent 21 — Telegram Communications Liaison
Institutional-grade signal formatting and broadcast automation.
"""

import asyncio
import html
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional
try:
    import aiohttp
    _AIOHTTP_OK = True
except ImportError:
    aiohttp = None          # type: ignore
    _AIOHTTP_OK = False

log = logging.getLogger("TELEGRAM-LIAISON")

TELEGRAM_BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"   # Replace with BotFather token
TELEGRAM_CHAT_ID   = "YOUR_CHAT_ID_HERE"     # Channel or group chat ID
API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# Rate limiter: Telegram allows 30 msg/sec to groups
SEND_DELAY_S = 0.1
_TOKEN_PATTERN = re.compile(r"\b[0-9]{8,12}:[A-Za-z0-9_-]{25,}\b")
_JWT_PATTERN = re.compile(
    r"\beyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\b"
)
_HTML_TAG_PATTERN = re.compile(r"</?[A-Za-z][^>]{0,200}>")


def _redact_message(text: str) -> str:
    return _JWT_PATTERN.sub("[REDACTED]", _TOKEN_PATTERN.sub("[REDACTED]", text))


def _html_value(value: object) -> str:
    return html.escape(_redact_message(str(value)), quote=True)


def _bounded_message(text: str, parse_mode: str, limit: int = 4096) -> str:
    """Bound Telegram text without cutting through HTML tags or entities."""
    redacted = _redact_message(text)
    if len(redacted) <= limit:
        return redacted
    if parse_mode.upper() != "HTML":
        return redacted[: limit - 1] + "…"
    plain = html.unescape(_HTML_TAG_PATTERN.sub("", redacted))
    low, high = 0, len(plain)
    while low < high:
        middle = (low + high + 1) // 2
        if len(html.escape(plain[:middle], quote=True)) + 1 <= limit:
            low = middle
        else:
            high = middle - 1
    return html.escape(plain[:low], quote=True) + "…"


class TelegramLiaison:
    """
    Agent 21: Formats and broadcasts all system communications.
    Uses raw async HTTP (no library dependency) for zero-cloud resilience.
    Implements exponential backoff for network failures.
    """

    def __init__(self, token: str = TELEGRAM_BOT_TOKEN,
                 chat_id: str = TELEGRAM_CHAT_ID,
                 queue_max_size: int = 500):
        self.token    = token
        self.chat_id  = chat_id
        self.api_base = f"https://api.telegram.org/bot{token}"
        self._session: Optional[object] = None
        self._msg_queue: asyncio.Queue = asyncio.Queue(
            maxsize=max(1, int(queue_max_size))
        )
        self._running = False

    async def _get_session(self):
        if not _AIOHTTP_OK:
            raise RuntimeError('aiohttp not installed. Run: pip install aiohttp')
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )
        return self._session

    async def _send_raw(self, text: str, parse_mode: str = "HTML",
                        max_retries: int = 5) -> bool:
        """Send with exponential backoff for network resilience."""
        if not _AIOHTTP_OK:
            log.warning("Telegram disabled: aiohttp is not installed")
            return False

        attempts = max(1, min(int(max_retries), 8))
        try:
            session = await self._get_session()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("Telegram session unavailable: %s", type(exc).__name__)
            return False
        url = f"{self.api_base}/sendMessage"
        payload = {
            "chat_id":    self.chat_id,
            "text":       _bounded_message(text, parse_mode),
            "parse_mode": parse_mode,
            "disable_web_page_preview": True
        }
        
        for attempt in range(attempts):
            try:
                async with session.post(url, json=payload) as resp:
                    data = await resp.json()
                    if resp.status == 200 and data.get("ok"):
                        return True
                    elif resp.status == 429:  # Rate limited
                        retry_after = max(
                            1, min(int(data.get("parameters", {}).get("retry_after", 5)), 60)
                        )
                        log.warning("Rate limited. Waiting %ds", retry_after)
                        await asyncio.sleep(retry_after)
                    else:
                        description = _redact_message(str(data.get("description", "")))[:160]
                        log.error("Telegram API error %d: %s", resp.status, description)
                        if 500 <= resp.status < 600:
                            await asyncio.sleep(min(2 ** attempt, 30))
                        else:
                            return False
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                backoff = min(2 ** attempt, 30)
                log.warning("Telegram transport %s. Retry %d/%d in %ds",
                            type(exc).__name__, attempt+1, attempts, backoff)
                await asyncio.sleep(backoff)
            except Exception as exc:
                log.error("Unexpected Telegram send error: %s", type(exc).__name__)
                await asyncio.sleep(min(2 ** attempt, 30))
        
        log.error("Telegram delivery failed after %d attempts", attempts)
        return False

    async def queue_send(self, text: str) -> bool:
        """Non-blocking enqueue for high-frequency environments."""
        try:
            self._msg_queue.put_nowait(_redact_message(text)[:8192])
            return True
        except asyncio.QueueFull:
            log.error("Telegram in-memory queue is full; durable outbox required")
            return False

    async def message_worker(self):
        """Async worker draining message queue with rate limiting."""
        self._running = True
        while self._running:
            try:
                text = await asyncio.wait_for(self._msg_queue.get(), timeout=1.0)
                try:
                    delivered = await self._send_raw(text)
                    if not delivered:
                        log.error("Telegram queue item was not delivered")
                    await asyncio.sleep(SEND_DELAY_S)
                finally:
                    self._msg_queue.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("Telegram worker isolated error: %s", type(exc).__name__)

    # ─── Message Formatters ───────────────────────────────────────────────────

    async def send_trade_signal(self, signal: dict, lot_size: float,
                                 risk_pct: float, account_equity: float):
        """Real-time trade signal with institutional formatting."""
        direction_icon = "🟢" if signal["direction"] == "BUY" else "🔴"
        dir_label      = "LONG ▲" if signal["direction"] == "BUY" else "SHORT ▼"
        
        entry  = signal.get("entry", 0)
        sl     = signal.get("sl", 0)
        tp1    = signal.get("tp1", 0)
        tp2    = signal.get("tp2", 0)
        tp3    = signal.get("tp3", 0)
        
        # Calculate R:R ratios
        sl_dist = abs(entry - sl)
        rr1 = round(abs(tp1 - entry) / sl_dist, 1) if sl_dist > 0 else 0
        rr2 = round(abs(tp2 - entry) / sl_dist, 1) if sl_dist > 0 else 0
        rr3 = round(abs(tp3 - entry) / sl_dist, 1) if sl_dist > 0 else 0
        
        tags_str = _html_value(" + ".join(signal.get("strategy_tags", [])))
        agents_n = signal.get("agents_agreeing", 0)
        conf_pct = signal.get("confluence_pct", 0)
        
        msg = f"""⚠️ <b>QUANT-SWARM TRADE ALERT</b> ⚠️
━━━━━━━━━━━━━━━━━━━━━━━━
{direction_icon} <b>XAUUSD — {dir_label}</b>
📊 <b>Strategy Confluence:</b> [{tags_str}]
🧠 <b>Swarm Consensus:</b> {agents_n} Agents | {conf_pct:.0f}% Confidence
━━━━━━━━━━━━━━━━━━━━━━━━
📍 <b>ENTRY ZONE:</b>  <code>{entry:.2f}</code>
🛑 <b>STOP LOSS:</b>   <code>{sl:.2f}</code>
━━━━━━━━━━━━━━━━━━━━━━━━
🎯 <b>TP1:</b> <code>{tp1:.2f}</code>  <i>(1:{rr1}R)</i>
🎯 <b>TP2:</b> <code>{tp2:.2f}</code>  <i>(1:{rr2}R)</i>
🎯 <b>TP3:</b> <code>{tp3:.2f}</code>  <i>(1:{rr3}R)</i>
━━━━━━━━━━━━━━━━━━━━━━━━
💼 <b>Position:</b> {lot_size:.2f} lots | Risk: {risk_pct:.1f}% (${account_equity * risk_pct / 100:.0f})
━━━━━━━━━━━━━━━━━━━━━━━━
⚙️ <b>Dynamic Management:</b>
  • Move SL to BE when TP1 hit
  • Trail behind 1H structural swing at TP2
  • Run TP3 to full target — do not manage
━━━━━━━━━━━━━━━━━━━━━━━━
🕐 <i>{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</i>"""
        
        await self._send_raw(msg)

    async def send_daily_briefing(self, briefing: dict):
        """Pre-London open institutional market briefing."""
        macro_events = "\n".join(
            f"  • <b>{_html_value(e['time'])}</b> — {_html_value(e['name'])} "
            f"(Impact: {_html_value(e['impact'])})"
            for e in briefing.get("macro_events", [])
        )
        htf_levels = briefing.get("htf_levels", {})
        swarm_state = briefing.get("swarm_overnight_adjustments", [])
        
        adj_lines = "\n".join(
            f"  ↳ Agent {_html_value(a.get('agent_name', '?'))}: "
            f"{_html_value(a.get('adjustment', ''))}"
            for a in swarm_state[:5]
        )
        
        msg = f"""🌅 <b>QUANT-SWARM DAILY BRIEFING</b>
<i>London Pre-Open Intelligence Package</i>
━━━━━━━━━━━━━━━━━━━━━━━━
📰 <b>MACRO CALENDAR TODAY</b>
{macro_events if macro_events else "  • No high-impact events scheduled"}
━━━━━━━━━━━━━━━━━━━━━━━━
📐 <b>HTF STRUCTURAL MAP (XAUUSD)</b>
  🔴 Key Resistance: <code>{_html_value(htf_levels.get('r1', 'N/A'))}</code> → <code>{_html_value(htf_levels.get('r2', 'N/A'))}</code>
  🟢 Key Support:    <code>{_html_value(htf_levels.get('s1', 'N/A'))}</code> → <code>{_html_value(htf_levels.get('s2', 'N/A'))}</code>
  📏 Daily PCH:      <code>{_html_value(htf_levels.get('pch', 'N/A'))}</code>
  📏 Daily PCL:      <code>{_html_value(htf_levels.get('pcl', 'N/A'))}</code>
━━━━━━━━━━━━━━━━━━━━━━━━
💧 <b>LIQUIDITY POOLS</b>
  🎯 Buy-Side SSL:  <code>{_html_value(briefing.get('bsl', 'N/A'))}</code>
  🎯 Sell-Side BSL: <code>{_html_value(briefing.get('ssl', 'N/A'))}</code>
━━━━━━━━━━━━━━━━━━━━━━━━
🤖 <b>SWARM OPTIMIZATION STATE</b>
{adj_lines if adj_lines else "  • All agents stable — no overnight adjustments"}
━━━━━━━━━━━━━━━━━━━━━━━━
⏰ London Open: <b>08:00 UTC</b> | NY Open: <b>13:30 UTC</b>
<i>Generated: {datetime.now(timezone.utc).strftime('%H:%M UTC')}</i>"""
        
        await self._send_raw(msg)

    async def send_session_update(self, update: dict):
        """Mid-day London/Pre-NY session recalibration."""
        swept = _html_value(update.get("swept_levels", "None"))
        triggered = _html_value(", ".join(update.get("strategies_triggered", ["None"])))
        
        msg = f"""🔄 <b>MID-SESSION UPDATE</b>
<i>London Review → NY Setup</i>
━━━━━━━━━━━━━━━━━━━━━━━━
📊 <b>LONDON MORNING REVIEW</b>
  Swept Levels:  {swept}
  Strategies Triggered: {triggered}
  London Result: {_html_value(update.get('london_r', '+0.0'))} R

📐 <b>NY SESSION SETUP</b>
  Key Level Above: <code>{_html_value(update.get('ny_resistance', 'N/A'))}</code>
  Key Level Below: <code>{_html_value(update.get('ny_support', 'N/A'))}</code>
  Bias:            {_html_value(update.get('ny_bias', 'Neutral'))}

⚙️ <b>Swarm Recalibration:</b> {_html_value(update.get('recal_note', 'Levels refreshed for NY open'))}
━━━━━━━━━━━━━━━━━━━━━━━━
<i>{datetime.now(timezone.utc).strftime('%H:%M UTC')}</i>"""
        
        await self._send_raw(msg)

    async def send_daily_summary(self, summary: dict, adaptations: list[dict]):
        """End-of-NY-session performance summary."""
        wins       = summary.get("wins", 0)
        losses     = summary.get("losses", 0)
        total      = wins + losses
        wr_pct     = round(wins / total * 100, 1) if total > 0 else 0
        net_r      = summary.get("net_r", 0.0)
        max_dd_pct = summary.get("max_drawdown_pct", 0.0)
        
        result_icon = "✅" if net_r > 0 else "❌" if net_r < 0 else "⚖️"
        
        adapt_lines = "\n".join(
            f"  ↳ {_html_value(a.get('agent_name','?'))}: "
            f"{_html_value(str(a.get('reason',''))[:80])}"
            for a in adaptations[:4]
        )
        
        # Text equity curve (sparkline)
        eq_curve = summary.get("equity_curve", [])
        spark = _equity_sparkline(eq_curve)
        
        msg = f"""{result_icon} <b>DAILY TRADE SUMMARY</b>
<i>NY Session Close | XAUUSD</i>
━━━━━━━━━━━━━━━━━━━━━━━━
📊 <b>SESSION METRICS</b>
  Trades:   {total} ({wins}W / {losses}L)
  Win Rate: {wr_pct}%
  Net R:    {net_r:+.2f}R
  Max DD:   {max_dd_pct:.1f}%

📈 <b>EQUITY CURVE</b>
  <code>{spark}</code>
━━━━━━━━━━━━━━━━━━━━━━━━
🤖 <b>SWARM ADAPTATION LOG</b>
{adapt_lines if adapt_lines else "  • No adaptations triggered today"}
━━━━━━━━━━━━━━━━━━━━━━━━
<i>{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</i>"""
        
        await self._send_raw(msg)

    async def send_weekly_digest(self, digest: dict):
        """Friday post-market weekly performance digest."""
        strategies  = digest.get("strategies", {})
        top_strat   = digest.get("top_strategy", "N/A")
        worst_strat = digest.get("worst_strategy", "N/A")
        
        strat_lines = ""
        for name, m in sorted(strategies.items(),
                               key=lambda x: x[1].get("profit_factor", 0), reverse=True):
            icon = "🥇" if name == top_strat else ("⚠️" if name == worst_strat else "  ")
            strat_lines += (f"\n{icon} <b>{_html_value(str(name)[:18])}</b>\n"
                            f"     PF: {m['profit_factor']:.2f} | "
                            f"WR: {m['win_rate']}% | "
                            f"Trades: {m['trades']} | "
                            f"Net R: {m['net_r']:+.2f}\n")
        
        week_r = sum(m.get("net_r", 0) for m in strategies.values())
        week_result = "✅ PROFITABLE" if week_r > 0 else "❌ DRAWDOWN"
        
        msg = f"""📅 <b>WEEKLY PERFORMANCE DIGEST</b>
<i>XAUUSD | QUANT-SWARM-XAU</i>
━━━━━━━━━━━━━━━━━━━━━━━━
🏆 <b>Week Result: {week_result}</b>
   Net R: {week_r:+.2f}R
━━━━━━━━━━━━━━━━━━━━━━━━
📊 <b>STRATEGY ALPHA BREAKDOWN</b>
{strat_lines}
━━━━━━━━━━━━━━━━━━━━━━━━
🥇 Best Alpha: <b>{_html_value(top_strat)}</b>
⚠️  Needs Work: <b>{_html_value(worst_strat)}</b>
━━━━━━━━━━━━━━━━━━━━━━━━
<i>Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</i>"""
        
        await self._send_raw(msg)

    async def send_emergency_halt(self, reason: str, equity: float, daily_dd: float):
        """Emergency halt notification with account state."""
        msg = f"""🚨🚨 <b>EMERGENCY HALT TRIGGERED</b> 🚨🚨
━━━━━━━━━━━━━━━━━━━━━━━━
⛔ <b>ALL POSITIONS CLOSED</b>
   Reason: <code>{_html_value(reason)}</code>
━━━━━━━━━━━━━━━━━━━━━━━━
💼 Account Equity: <code>${equity:,.2f}</code>
📉 Daily Drawdown: <code>{daily_dd:.2f}%</code>
━━━━━━━━━━━━━━━━━━━━━━━━
⚠️ EA paused. Manual review required before resuming.
<i>{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</i>"""
        
        await self._send_raw(msg)

    async def send_disconnect_alert(self, component: str):
        msg = f"""⚡ <b>CONNECTIVITY ALERT</b>
   Component: <code>{_html_value(component)}</code> disconnected.
   Attempting reconnect... monitor manually if unresolved within 60s.
<i>{datetime.now(timezone.utc).strftime('%H:%M UTC')}</i>"""
        await self._send_raw(msg)

    async def close(self):
        self._running = False
        if self._session and not self._session.closed:
            await self._session.close()


# ─── Utilities ────────────────────────────────────────────────────────────────

def _equity_sparkline(values: list[float], width: int = 20) -> str:
    """Generate ASCII sparkline from equity curve values."""
    BLOCKS = " ▁▂▃▄▅▆▇█"
    if not values or len(values) < 2:
        return "▄" * width
    
    mn = min(values)
    mx = max(values)
    rng = mx - mn if mx != mn else 1
    
    # Downsample to width
    step = max(1, len(values) // width)
    sampled = values[::step][:width]
    
    return "".join(BLOCKS[int((v - mn) / rng * 8)] for v in sampled)


# ─── Demo ─────────────────────────────────────────────────────────────────────

async def demo():
    bot = TelegramLiaison(
        token="TEST_TOKEN",
        chat_id="TEST_CHAT"
    )
    
    # Demo signal format
    signal = {
        "direction": "BUY",
        "entry": 2685.50,
        "sl": 2672.00,
        "tp1": 2698.50,
        "tp2": 2711.25,
        "tp3": 2730.00,
        "confluence_pct": 87.3,
        "agents_agreeing": 7,
        "strategy_tags": ["SMC_OB", "CRT_SWEEP", "ASIA_RANGE", "HTF_SR"]
    }
    
    # Format and print (won't send without real token)
    print("\n── Trade Signal Preview ──")
    # Inline the formatter for demo purposes
    print(f"[BUY] XAUUSD | 87.3% confluence | 7/10 agents | SL=2672 TP1=2698 TP2=2711 TP3=2730")
    
    # Demo weekly digest
    digest = {
        "strategies": {
            "ICT_SMC_Core":       {"trades": 12, "win_rate": 75.0, "profit_factor": 2.31, "net_r": 8.4},
            "HTF_Liquidity_Trap": {"trades": 8,  "win_rate": 62.5, "profit_factor": 1.87, "net_r": 5.1},
            "CRT_Engine":         {"trades": 15, "win_rate": 53.3, "profit_factor": 1.42, "net_r": 3.2},
            "Asia_Session_Sweep": {"trades": 10, "win_rate": 80.0, "profit_factor": 3.10, "net_r": 9.8},
            "Wyckoff_Schematic":  {"trades": 6,  "win_rate": 33.3, "profit_factor": 0.72, "net_r": -2.1},
        },
        "top_strategy": "Asia_Session_Sweep",
        "worst_strategy": "Wyckoff_Schematic"
    }
    
    print("\n── Weekly Digest Preview ──")
    for name, m in sorted(digest["strategies"].items(),
                           key=lambda x: x[1]["profit_factor"], reverse=True):
        icon = "🥇" if name == digest["top_strategy"] else ("⚠️" if name == digest["worst_strategy"] else "  ")
        print(f"{icon} {name:<25} PF:{m['profit_factor']:.2f} WR:{m['win_rate']}% NetR:{m['net_r']:+.1f}")

    await bot.close()


if __name__ == "__main__":
    asyncio.run(demo())
