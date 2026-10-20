# QUANT-SWARM-XAU documentation

Status: remediation build; not approved for real-capital trading  
Architecture version: 4.0.0-rc2
Last reviewed: 2026-07-29

The normative system design is [CANONICAL_ARCHITECTURE.md](CANONICAL_ARCHITECTURE.md). If another file, comment, configuration, binary, or historical runtime conflicts with that document, the canonical architecture wins.

## Current safety status

- Real-account execution is blocked. Do not enable MT5 AutoTrading on a real account.
- The previous v2/v3 orchestrators, standalone IPC launchers, and self-modifying learning paths are retired and are not deployment entry points.
- Historical databases and recovered journals are evidence only. They are not authoritative broker records and are not learning inputs.
- Telegram, Discord, website, and dashboard output are projections only; they cannot approve or mutate trades.
- Broker protocol 4.1 pins both ends to `SHADOW` or `DEMO`; SHADOW is the
  EA-local default and rejects broker mutation even if a peer sends a command.
- The account-scoped UTC risk-day high-water migration must be explicitly
  approved before the RC2 EA starts, verified, and then removed from the EA
  inputs. Risk state is broker/account scoped, and command state is bound to
  the full broker ownership tuple.
- A frozen build must pass the promotion gates and then complete at least 30 trading days, preferably 60 calendar days, on MT5 demo. That observation period has not yet been completed.

No win rate, signal count, profit factor, return, or “never loses” statement in an archived document is accepted as verified performance.

## Canonical 21-agent roster

| ID | Agent | ID | Agent | ID | Agent |
|---:|---|---:|---|---:|---|
| 01 | Orchestrator | 08 | VPVR/POC | 15 | Intermarket Watcher |
| 02 | Tick Streamer | 09 | Asia Session Sweep | 16 | Confluence Engine |
| 03 | Regime Detector | 10 | News/Sentiment | 17 | Entry Trigger |
| 04 | HTF Liquidity Trap | 11 | Mean Reversion / Vol Bands | 18 | Risk Manager |
| 05 | Trendline Breakout | 12 | Correlation Deflection | 19 | Lifecycle Manager |
| 06 | CRT Engine | 13 | Wyckoff Schematic | 20 | Continuous Learner |
| 07 | ICT/SMC Core | 14 | Macro Catalyst Monitor | 21 | Resilience Controller |

Only Agents 04–13 vote. A direction requires at least three distinct eligible strategy agents. Agent 10 requires healthy Agent 14 evidence; Agent 12 requires healthy Agent 15 evidence. Macro-calendar uncertainty blocks new execution by default.

## Source-of-truth boundaries

MetaTrader 5 and the broker are authoritative for orders, deals, fills, positions, broker prices, costs, and broker timestamps. The local durable ledger owns intents, attribution, risk reservations, lifecycle requests, reconciliation evidence, governed learning, and notification delivery.

The execution path is deliberately fail-closed:

```text
immutable market/provider snapshot
  -> Agents 03–15 analysis
  -> Agent 16 fusion (unique voters, direction and context)
  -> Agent 17 executable-quote validation
  -> Agent 18 durable risk reservation
  -> idempotent broker command
  -> broker transaction/deal evidence
  -> Agent 19 lifecycle and Agent 20 governed learning
```

No timeout is treated as a rejection. Ambiguous commands become `UNKNOWN` and block dependent action until exact broker evidence or reconciliation proves the outcome.

## Runtime modes

Configuration uses schema version 2.

The operational launcher and RC2 configuration schema expose only `CHECK`,
`SHADOW`, and `DEMO` broker capabilities. `LIVE` and its real-account
authorization fields are absent from this release.

- `CHECK`: validates configuration and local prerequisites without connecting or trading.
- `SHADOW`: accepts demo-account market evidence but cannot send broker commands.
- `DEMO`: may send commands only to the configured and authenticated demo account after synchronization.

A future real-capital mode would require a separately designed, reviewed, and
promoted release; it cannot be enabled by changing this release's configuration.

The supported transport is a single authenticated loopback TCP connection.
Account ID, broker server, account mode, symbol, magic number, protocol version,
and the complete atomic broker snapshot must match before readiness. DEMO
execution requires `HEDGING`; `SHADOW` may observe a NETTING account but has no
command send path. The snapshot exposes all account positions and pending
orders so foreign symbol/magic exposure can block readiness without ever being
claimed or mutated by this runtime.

Start from [deployment/config.shadow.example.json](../deployment/config.shadow.example.json) for observation and [deployment/config.demo.example.json](../deployment/config.demo.example.json) only after promotion gates pass. Secrets belong in environment variables, never JSON, source files, archives, screenshots, or logs. A developer-only local `.env`, if used for `CHECK`, must be owned by that user and mode `0600` or `0400`; the launcher parses it as assignments and never executes it as shell code. The exact installed layout and permission boundary are documented in [deployment/README.md](../deployment/README.md).

The canonical runtime command is `python -m python_swarm.runtime_app` with an
explicit mode and schema-v2 configuration. The wrapper accepts only
`--check`, `--shadow`, and `--demo`, passes `QSX_CONFIG` and `QSX_DATA_DIR`,
and defaults to non-order-producing `SHADOW`. Always spell the intended mode
explicitly in operator procedures.

`SHADOW` and `DEMO` must run as a non-root service identity from an installed,
root-owned release. The service identity must not be able to write or replace
the source/package tree, runtime JSON, interpreter, or Python import roots.
The runtime checks these paths and their ancestors before creating state or
opening a broker socket; it fails closed and never repairs a writable checkout
with `chmod`. Keep writable data below `QSX_DATA_DIR`, separate from the release.

## Risk and lifecycle invariants

- Maximum requested and approved risk is 1% per trade; runtime profiles may be lower.
- Total open, pending, and unknown risk may not exceed 3%.
- Broker balance/equity initializes risk state; configuration cannot supply substitute equity.
- The daily drawdown baseline and halt are durable and cannot be cleared by restart.
- Default total position capacity is one; the absolute supported ceiling is three.
- BUY geometry: `0 < SL < ASK < TP1 < TP2 < TP3`.
- SELL geometry: `0 < TP3 < TP2 < TP1 < BID < SL`.
- TP1 requests 50% of initial volume; after confirmed execution, breakeven may be requested.
- TP2 requests 25% of initial volume; after confirmed execution, structural trailing may begin.
- Structural trailing uses the latest strict completed-H1 fractal with two closed
  bars on each side, buffered 10 symbol points beyond the pivot; without that
  evidence, no trailing stop change is requested.
- TP3 closes the confirmed remaining volume.
- Lifecycle state changes only from broker events or an authoritative broker snapshot.
- A crash-lost OPEN registration is restored only from one exact durable OPEN
  command/reservation plus its matching authenticated broker `IN` deal and raw
  position. Snapshot attribution alone is insufficient; ambiguity remains
  quarantined and blocking.

Actual R is derived from all authoritative closing deals, including commission, swap, and fees, divided by the immutable entry-time risk in account currency.

## Development verification

Use a fresh CPython 3.12 virtual environment installed only from the verified
test wheelhouse and `requirements-test-py312.lock`. The signed lane build,
verification, transactional setup, and offline Docker procedure is specified
in [DEPENDENCY_REPRODUCIBILITY.md](DEPENDENCY_REPRODUCIBILITY.md). The
canonical evidence invocation is:

```bash
PYTHONHASHSEED=0 TZ=UTC LC_ALL=C.UTF-8 \
PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 \
.venv-py312/bin/python -m pytest -q -W error -p no:cacheprovider
```

A separately locked CPython 3.14 environment may run the same suite as
forward-compatibility evidence. It is not a broker-runtime interpreter, and
packages from one interpreter must never be injected into another with
`PYTHONPATH`.

Before any demo AutoTrading session, also require:

1. Native MetaEditor compilation with zero errors and zero warnings from the exact frozen source.
2. Golden protocol, malformed-frame, wrong-identity, stale-provider, duplicate-command, and second-client rejection tests.
3. Crash/restart tests at every command boundary and exact broker-ledger reconciliation.
4. A clean secret scan and byte-stable test run outside ephemeral test directories.
5. A signed build manifest containing source, configuration, migration, test, and EX5 hashes.

Passing tests is necessary but does not establish profitability or authorize live trading.

## Operator rule

Until a later reviewed release explicitly changes this status, use `CHECK` for inspection and a broker demo account for controlled testing only. Never copy instructions from the retired HTML manuals or invoke a legacy orchestrator directly.

Algorithmic trading can lose more than expected through gaps, slippage, broker behavior, software faults, and operational mistakes. Broker-native protection reduces risk but cannot eliminate it.
