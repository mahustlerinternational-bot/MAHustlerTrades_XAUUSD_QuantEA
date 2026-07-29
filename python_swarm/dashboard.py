#!/usr/bin/env python3
"""
QUANT-SWARM-XAU v4.0.0-rc2 — Read-only Dashboard Server
Dashboard startup is not exposed by the remediation deployment wrapper.
The default listener is http://127.0.0.1:8088 and is read-only.
"""

import hmac
import http.server
import ipaddress
import json
import math
import os
import socketserver
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional
from urllib.parse import urlsplit

try:
    from .safe_paths import runtime_state_dir
    from .version import PRODUCT_DISPLAY_VERSION
except ImportError:  # pragma: no cover - direct dashboard execution
    from safe_paths import runtime_state_dir  # type: ignore
    from version import PRODUCT_DISPLAY_VERSION  # type: ignore


MAX_PROJECTION_BYTES = 2 * 1024 * 1024
PUBLIC_PROJECTION_FIELDS = frozenset({
    "ask",
    "atr",
    "balance",
    "bid",
    "confluence_gate",
    "daily_pnl",
    "ea_connected",
    "equity",
    "halt_reason",
    "halted",
    "losses",
    "mode",
    "positions",
    "projected_at",
    "ready",
    "regime",
    "session",
    "status",
    "timestamp",
    "trade_count",
    "updated_at",
    "wins",
})


def is_loopback_host(host: str) -> bool:
    """Return true only for an unambiguous loopback bind target."""
    candidate = host.strip().lower()
    if candidate == "localhost":
        return True
    try:
        return ipaddress.ip_address(candidate).is_loopback
    except ValueError:
        return False


@dataclass(frozen=True)
class DashboardConfig:
    host: str = "127.0.0.1"
    port: int = 8088
    projection_path: Path = Path("live_state.json")
    bearer_token: str = ""
    allowed_origin: str = ""
    ready_max_age_s: float = 15.0

    @property
    def exposed(self) -> bool:
        return not is_loopback_host(self.host)

    def validate(self) -> None:
        if not 1 <= self.port <= 65535:
            raise ValueError("QSX_DASH_PORT must be between 1 and 65535")
        if self.ready_max_age_s <= 0:
            raise ValueError("QSX_DASH_READY_MAX_AGE_S must be positive")
        if self.exposed and not self.bearer_token:
            raise RuntimeError(
                "QSX_DASH_TOKEN is required when the dashboard is not loopback-only"
            )
        if self.allowed_origin == "*":
            raise ValueError("wildcard dashboard CORS is forbidden")
        if self.allowed_origin:
            origin = urlsplit(self.allowed_origin)
            if (
                origin.scheme not in {"http", "https"}
                or not origin.netloc
                or origin.path
                or origin.query
                or origin.fragment
            ):
                raise ValueError(
                    "QSX_DASH_ALLOWED_ORIGIN must be an explicit HTTP(S) origin"
                )

    @classmethod
    def from_env(cls) -> "DashboardConfig":
        projection = os.environ.get("QSX_PROJECTION_PATH", "").strip()
        projection_path = (
            Path(projection).expanduser().resolve()
            if projection else (runtime_state_dir() / "live_state.json").resolve()
        )
        return cls(
            host=os.environ.get("QSX_DASH_HOST", "127.0.0.1").strip() or "127.0.0.1",
            port=int(os.environ.get("QSX_DASH_PORT", "8088")),
            projection_path=projection_path,
            bearer_token=os.environ.get("QSX_DASH_TOKEN", "").strip(),
            allowed_origin=os.environ.get("QSX_DASH_ALLOWED_ORIGIN", "").strip(),
            ready_max_age_s=float(
                os.environ.get("QSX_DASH_READY_MAX_AGE_S", "15")
            ),
        )


def is_bearer_authorized(config: DashboardConfig, authorization: str) -> bool:
    """Use a constant-time comparison and never accept credentials in URLs."""
    if not config.bearer_token:
        return not config.exposed
    prefix = "Bearer "
    if not authorization.startswith(prefix):
        return False
    supplied = authorization[len(prefix):]
    return hmac.compare_digest(supplied, config.bearer_token)


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def load_projection(path: Path) -> tuple[Optional[dict[str, Any]], str]:
    """Read a bounded JSON object without exposing filesystem errors."""
    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = handle.read(MAX_PROJECTION_BYTES + 1)
        if len(raw.encode("utf-8")) > MAX_PROJECTION_BYTES:
            return None, "projection_too_large"
        decoded = json.loads(raw, parse_constant=_reject_json_constant)
    except FileNotFoundError:
        return None, "projection_missing"
    except (OSError, UnicodeError, ValueError, RecursionError):
        return None, "projection_unreadable"
    if not isinstance(decoded, Mapping):
        return None, "projection_invalid"
    if not _is_safe_projection_value(decoded):
        return None, "projection_invalid"
    # The dashboard gets a projection, never the runtime's unrestricted state.
    # This allow-list is a second containment boundary against accidentally
    # publishing a broker credential or service-role key.
    return {
        key: value for key, value in decoded.items()
        if key in PUBLIC_PROJECTION_FIELDS
    }, "ok"


def _is_safe_projection_value(value: Any, depth: int = 0) -> bool:
    if depth > 32:
        return False
    if value is None or isinstance(value, (str, bool, int)):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, list):
        return all(_is_safe_projection_value(item, depth + 1) for item in value)
    if isinstance(value, Mapping):
        return all(
            isinstance(key, str) and _is_safe_projection_value(item, depth + 1)
            for key, item in value.items()
        )
    return False


def _timestamp_seconds(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        result = float(value)
        return result if math.isfinite(result) and result > 0 else None
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        result = parsed.timestamp()
        return result if math.isfinite(result) and result > 0 else None
    except ValueError:
        return None


def projection_readiness(
    config: DashboardConfig, *, now: Optional[float] = None
) -> tuple[bool, dict[str, Any]]:
    projection, reason = load_projection(config.projection_path)
    if projection is None:
        return False, {"status": "not_ready", "reason": reason}

    timestamp = next(
        (
            parsed
            for key in ("timestamp", "projected_at", "updated_at")
            if (parsed := _timestamp_seconds(projection.get(key))) is not None
        ),
        None,
    )
    if timestamp is None:
        return False, {"status": "not_ready", "reason": "projection_timestamp_missing"}

    current = time.time() if now is None else now
    age = current - timestamp
    if age < -30:
        return False, {"status": "not_ready", "reason": "projection_timestamp_future"}
    if age > config.ready_max_age_s:
        return False, {
            "status": "not_ready",
            "reason": "projection_stale",
            "age_s": round(age, 3),
        }

    projected_ready = projection.get("ready")
    if projected_ready is None:
        projected_ready = projection.get("ea_connected", False)
    if projected_ready is not True:
        return False, {"status": "not_ready", "reason": "runtime_not_ready"}
    return True, {"status": "ready", "age_s": round(max(0.0, age), 3)}


CONFIG = DashboardConfig.from_env()
# Compatibility aliases for existing local tooling.  All request handling uses
# CONFIG so an operator-selected projection cannot silently fall back elsewhere.
PORT = CONFIG.port
STATE = CONFIG.projection_path

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>QUANT-SWARM-XAU __QSX_PRODUCT_VERSION__</title>
<style>
:root{
  --bg:#0d0d0f;--bg2:#141417;--bg3:#1c1c21;--bg4:#242429;
  --border:#2a2a32;--border2:#38383f;
  --text:#e8e8ec;--muted:#7a7a88;--dim:#4a4a58;
  --green:#22c55e;--red:#ef4444;--amber:#f59e0b;
  --blue:#3b82f6;--purple:#a855f7;--teal:#14b8a6;
  --mono:'JetBrains Mono','Fira Mono','Consolas',monospace;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:system-ui,sans-serif;
  font-size:13px;min-height:100vh;padding:16px}
header{display:flex;align-items:center;gap:10px;margin-bottom:20px;flex-wrap:wrap}
.logo{font-family:var(--mono);font-size:11px;font-weight:600;letter-spacing:2px;
  color:var(--amber);user-select:none}
.badge{font-size:10px;padding:2px 8px;border-radius:20px;font-weight:500;
  border:1px solid;letter-spacing:.5px}
.bl{background:#052814;color:var(--green);border-color:#0d4a22}
.bd{background:#200a0a;color:var(--red);border-color:#4a1010}
.bw{background:#1a1200;color:var(--amber);border-color:#4a3000}
.bb{background:#060f2a;color:var(--blue);border-color:#0d2060}
.bs{background:var(--bg3);color:var(--muted);border-color:var(--border)}
.dot{display:inline-block;width:6px;height:6px;border-radius:50%;
  margin-right:5px;vertical-align:middle}
.ts{margin-left:auto;font-family:var(--mono);font-size:10px;color:var(--dim)}

/* Price hero */
.hero{text-align:center;padding:20px 0;border-top:1px solid var(--border);
  border-bottom:1px solid var(--border);margin-bottom:16px}
.price-row{font-family:var(--mono);font-size:40px;font-weight:600;
  letter-spacing:3px;transition:color .12s}
.price-row.up{color:var(--green)}.price-row.dn{color:var(--red)}
.price-row.neu{color:var(--amber)}
.price-labels{display:flex;justify-content:center;gap:160px;
  font-size:10px;color:var(--dim);margin-top:4px;letter-spacing:1px}
.spread{font-size:11px;color:var(--muted);margin-top:6px}

/* Metric cards */
.metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-bottom:16px}
.m{background:var(--bg3);border:1px solid var(--border);border-radius:8px;
  padding:12px 14px}
.ml{font-size:10px;color:var(--muted);letter-spacing:.8px;
  text-transform:uppercase;margin-bottom:6px}
.mv{font-family:var(--mono);font-size:20px;font-weight:600}
.pos{color:var(--green)}.neg{color:var(--red)}.neu2{color:var(--text)}
.amb{color:var(--amber)}

/* Panels */
.panels{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:16px}
.panel{background:var(--bg3);border:1px solid var(--border);border-radius:8px;padding:14px}
.pt{font-size:10px;color:var(--dim);letter-spacing:1px;
  text-transform:uppercase;margin-bottom:10px}
.row{display:flex;justify-content:space-between;align-items:center;
  padding:6px 0;border-bottom:1px solid var(--border)}
.row:last-child{border-bottom:none;padding-bottom:0}
.rk{color:var(--muted)}
.rv{font-family:var(--mono);font-weight:500}

/* History log */
.log-wrap{background:var(--bg2);border:1px solid var(--border);border-radius:8px;
  padding:12px 14px}
.log-box{height:110px;overflow-y:auto;font-family:var(--mono);
  font-size:10px;color:var(--muted);line-height:1.8}
.log-box p{margin:0}.log-box .ts2{color:var(--dim)}
.log-box .ev{color:var(--text)}

footer{text-align:center;font-size:10px;color:var(--dim);margin-top:16px}
@media(max-width:600px){
  .metrics{grid-template-columns:1fr 1fr}
  .panels{grid-template-columns:1fr}
  .price-labels{gap:80px}
}
</style>
</head>
<body>

<header>
  <span class="logo">QUANT-SWARM-XAU&nbsp;__QSX_PRODUCT_VERSION__</span>
  <span class="badge bl" id="conn-badge">
    <span class="dot" id="conn-dot" style="background:var(--green)"></span>
    <span id="conn-txt">CONNECTED</span>
  </span>
  <span class="badge bw" id="sess-badge">—</span>
  <span class="badge bs" id="regime-badge">—</span>
  <span class="ts" id="ts">—</span>
</header>

<div class="hero">
  <div class="price-row neu" id="price-row">
    <span id="bid">—</span>&thinsp;/&thinsp;<span id="ask">—</span>
  </div>
  <div class="price-labels"><span>BID</span><span>ASK</span></div>
  <div class="spread">Spread:&nbsp;<span id="spread">—</span>&nbsp;pts</div>
</div>

<div class="metrics">
  <div class="m"><div class="ml">Equity</div><div class="mv neu2" id="equity">—</div></div>
  <div class="m"><div class="ml">Balance</div><div class="mv neu2" id="balance">—</div></div>
  <div class="m"><div class="ml">Daily PnL</div><div class="mv" id="pnl">—</div></div>
  <div class="m"><div class="ml">Positions</div><div class="mv neu2" id="positions">—</div></div>
</div>

<div class="panels">
  <div class="panel">
    <div class="pt">Swarm status</div>
    <div class="row"><span class="rk">Trades today</span><span class="rv" id="trades">—</span></div>
    <div class="row"><span class="rk">Confluence gate</span><span class="rv amb" id="gate">—</span></div>
    <div class="row"><span class="rk">ATR H1</span><span class="rv" id="atr">—</span></div>
    <div class="row"><span class="rk">Session</span><span class="rv" id="sess2">—</span></div>
    <div class="row"><span class="rk">Regime</span><span class="rv" id="regime2">—</span></div>
  </div>
  <div class="panel">
    <div class="pt">Performance</div>
    <div class="row"><span class="rk">Wins</span><span class="rv pos" id="wins">0</span></div>
    <div class="row"><span class="rk">Losses</span><span class="rv neg" id="losses">0</span></div>
    <div class="row"><span class="rk">Win rate</span><span class="rv" id="wr">—</span></div>
    <div class="row"><span class="rk">Uptime</span><span class="rv" id="uptime">—</span></div>
  </div>
</div>

<div class="log-wrap">
  <div class="pt">Event log</div>
  <div class="log-box" id="log-box">
    <p><span class="ts2">[init]</span>&nbsp;<span class="ev">dashboard connected — polling /api/state every 1s</span></p>
  </div>
</div>

<footer>QUANT-SWARM-XAU __QSX_PRODUCT_VERSION__ &nbsp;|&nbsp; 21 logical agents &nbsp;|&nbsp; XAUUSD</footer>

<script>
let prev = {};
let _firstRead = true;   // guard against first-load false "Trade fired"
const startT = Date.now();

function fmt(n,d=2){ return parseFloat(n||0).toFixed(d); }
function log(msg){
  const box = document.getElementById('log-box');
  const now = new Date().toLocaleTimeString();
  const p = document.createElement('p');
  p.innerHTML = `<span class="ts2">[${now}]</span>&nbsp;<span class="ev">${msg}</span>`;
  box.prepend(p);
  while(box.children.length > 60) box.lastChild.remove();
}

function uptime(){
  const s = Math.floor((Date.now()-startT)/1000);
  const h=Math.floor(s/3600), m=Math.floor((s%3600)/60), ss=s%60;
  return `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(ss).padStart(2,'0')}`;
}

function update(d){
  const bid = parseFloat(d.bid)||0;
  const ask = parseFloat(d.ask)||0;
  const spread = (ask-bid).toFixed(3);
  const pnl = parseFloat(d.daily_pnl)||0;
  const wins = parseInt(d.wins)||0;
  const losses = parseInt(d.losses)||0;
  const total = wins+losses;
  const wr = total>0 ? (wins/total*100).toFixed(1)+'%' : '—';
  const atr = parseFloat(d.atr)||0;
  const connected = d.ea_connected===true||d.ea_connected==='true';
  const session = (d.session||'—').toUpperCase();
  const regime = d.regime||'unknown';

  // Connection badge
  const cb = document.getElementById('conn-badge');
  cb.className = 'badge '+(connected?'bl':'bd');
  document.getElementById('conn-dot').style.background = connected?'var(--green)':'var(--red)';
  document.getElementById('conn-txt').textContent = connected?'CONNECTED':'OFFLINE';

  // Session badge
  const sb = document.getElementById('sess-badge');
  sb.textContent = session;
  sb.className = 'badge '+(session==='NY'?'bb':session==='LONDON'?'bw':session==='ASIA'?'bl':'bs');

  document.getElementById('regime-badge').textContent = regime.toUpperCase();

  // Price
  const pr = document.getElementById('price-row');
  const bidEl = document.getElementById('bid');
  const askEl = document.getElementById('ask');
  if(bid>0){
    const dir = prev.bid ? (bid>prev.bid?'up':bid<prev.bid?'dn':'neu') : 'neu';
    pr.className='price-row '+dir;
    if(dir!=='neu') setTimeout(()=>pr.className='price-row neu',600);
    if(prev.bid && bid!==prev.bid) log(`Tick &nbsp;bid=${bid.toFixed(3)} ask=${ask.toFixed(3)} spread=${spread}`);
  }
  bidEl.textContent = bid>0?fmt(bid,3):'—';
  askEl.textContent = ask>0?fmt(ask,3):'—';
  document.getElementById('spread').textContent = bid>0?spread:'—';

  document.getElementById('equity').textContent = '$'+fmt(d.equity||0,2);
  document.getElementById('balance').textContent = '$'+fmt(d.balance||0,2);

  const pnlEl=document.getElementById('pnl');
  pnlEl.textContent=(pnl>=0?'+$':'-$')+Math.abs(pnl).toFixed(2);
  pnlEl.className='mv '+(pnl>0?'pos':pnl<0?'neg':'neu2');

  document.getElementById('positions').textContent=d.positions||0;
  document.getElementById('trades').textContent=d.trade_count||0;
  document.getElementById('gate').textContent=(d.confluence_gate||0)+'%';
  document.getElementById('atr').textContent=atr>0?fmt(atr,2):'warming up…';
  document.getElementById('sess2').textContent=session;
  document.getElementById('regime2').textContent=regime;
  document.getElementById('wins').textContent=wins;
  document.getElementById('losses').textContent=losses;
  document.getElementById('wr').textContent=wr;
  document.getElementById('uptime').textContent=uptime();

  if(d.ea_connected!==prev.ea_connected)
    log(connected?'✅ MT5 EA connected':'❌ MT5 EA disconnected');

  // Only log a real trade when count genuinely increases on a stable connection.
  // Guards: skip first poll (prev is empty), skip reconnect cycles (ea_connected just flipped).
  const _wasConn = prev.ea_connected===true||prev.ea_connected==='true';
  const _justReconn = !_wasConn && connected;
  const _newTC = parseInt(d.trade_count||0);
  const _prevTC = parseInt(prev.trade_count||0);
  if (!_firstRead && !_justReconn && _newTC > _prevTC)
    log(`🔔 Trade #${_newTC} confirmed — W:${wins} L:${losses} Gate:${d.confluence_gate||0}%`);

  _firstRead = false;

  if(d.timestamp){
    const dt=new Date(typeof d.timestamp==='number'?d.timestamp*1000:d.timestamp);
    document.getElementById('ts').textContent=dt.toLocaleTimeString();
  }
  prev=Object.assign({},d);
}

async function poll(){
  try{
    const r=await fetch('/api/state');
    if(!r.ok) throw new Error(r.status);
    update(await r.json());
  } catch(e){
    document.getElementById('conn-badge').className='badge bd';
    document.getElementById('conn-txt').textContent='SERVER LOST';
    log('⚠ /api/state unreachable: '+e.message);
  }
}

poll();
setInterval(poll,1000);
setInterval(()=>document.getElementById('uptime').textContent=uptime(),1000);
</script>
</body>
</html>
""".replace("__QSX_PRODUCT_VERSION__", PRODUCT_DISPLAY_VERSION)


class Handler(http.server.BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass  # suppress per-request noise in terminal

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if not is_bearer_authorized(CONFIG, self.headers.get("Authorization", "")):
            self._serve_unauthorized()
        elif path == "/healthz":
            self._send_json(200, {"status": "ok"})
        elif path == "/readyz":
            ready, payload = projection_readiness(CONFIG)
            self._send_json(200 if ready else 503, payload)
        elif path == "/api/state":
            self._serve_state()
        elif path in ("/", "/index.html"):
            self._serve_html()
        else:
            self._send_json(404, {"error": "not_found"})

    def do_OPTIONS(self):
        origin = self.headers.get("Origin", "")
        if not CONFIG.allowed_origin or not hmac.compare_digest(
            origin, CONFIG.allowed_origin
        ):
            self._send_json(403, {"error": "origin_not_allowed"})
            return
        self.send_response(204)
        self._send_common_headers()
        self.send_header("Access-Control-Allow-Methods", "GET")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.send_header("Access-Control-Max-Age", "300")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _serve_unauthorized(self):
        self.send_response(401)
        self._send_common_headers()
        self.send_header("WWW-Authenticate", 'Bearer realm="quant-swarm-dashboard"')
        body = b'{"error":"unauthorized"}'
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_common_headers(self):
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        origin = self.headers.get("Origin", "")
        if CONFIG.allowed_origin and hmac.compare_digest(origin, CONFIG.allowed_origin):
            self.send_header("Access-Control-Allow-Origin", CONFIG.allowed_origin)
            self.send_header("Vary", "Origin")

    def _send_json(self, status: int, payload: Mapping[str, Any]):
        body = json.dumps(
            payload, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
        self.send_response(status)
        self._send_common_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_state(self):
        projection, reason = load_projection(CONFIG.projection_path)
        if projection is None:
            self._send_json(503, {"status": "unavailable", "reason": reason})
            return
        self._send_json(200, projection)

    def _serve_html(self):
        body = HTML.encode()
        self.send_response(200)
        self._send_common_headers()
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; style-src 'unsafe-inline'; "
            "script-src 'unsafe-inline'; connect-src 'self'; "
            "img-src 'self' data:; object-src 'none'; base-uri 'none'",
        )
        self.end_headers()
        self.wfile.write(body)


class ReusableTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    allow_reuse_port = False
    daemon_threads = True


def main():
    CONFIG.validate()
    with ReusableTCPServer((CONFIG.host, CONFIG.port), Handler) as srv:
        print(f"\n  ══════════════════════════════════════════════")
        print(f"  QUANT-SWARM-XAU {PRODUCT_DISPLAY_VERSION} — Read-only Dashboard")
        print(f"  Listening on  http://{CONFIG.host}:{CONFIG.port}")
        print(f"  State projection:  {CONFIG.projection_path}")
        print(f"  Authentication:  {'bearer required' if CONFIG.bearer_token else 'loopback only'}")
        print(f"  ══════════════════════════════════════════════\n")
        srv.serve_forever()


if __name__ == "__main__":
    main()
