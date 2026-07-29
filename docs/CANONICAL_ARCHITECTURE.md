# QUANT-SWARM-XAU Canonical Architecture

Status: normative implementation baseline  
Version: 4.0.0-rc2
Date: 2026-07-29

This document is the authority for the QUANT-SWARM-XAU implementation. Generated operator documentation may expand it. Customer-facing documentation is non-normative and must never contradict it.

## 1. Safety model

- MetaTrader 5 and the broker are authoritative for orders, deals, positions, fills, volume, stop levels, profit, commission, swap, fees, and broker timestamps.
- The local durable ledger is authoritative for trade intent, strategy attribution, risk reservations, lifecycle intent, reconciliation results, learning inputs, and notification delivery.
- Telegram, Discord, dashboards, websites, and Supabase are asynchronous projections. Their failure cannot change a trading decision or broker state.
- The system fails closed whenever execution identity, ledger durability, broker synchronization, required provider health, account identity, or risk state is uncertain.
- Command delivery is at least once. Commands are idempotent, and ambiguous outcomes are resolved through broker-event and snapshot reconciliation. The system does not claim exactly-once network execution.
- Broker-native stop loss and final take profit remain attached as protection during Python, network, or provider failure.

## 2. Canonical 21-agent roster

The agents are logical bounded components, not 21 operating-system processes.

| ID | Agent | Responsibility |
|---:|---|---|
| 01 | Orchestrator | Composition, supervision, readiness and scheduling |
| 02 | Tick Streamer | Broker ticks, bars and immutable market snapshots |
| 03 | Regime Detector | Eight-regime classification from market snapshots |
| 04 | HTF Liquidity Trap | Higher-timeframe liquidity-trap strategy |
| 05 | Trendline Breakout | Trendline break and retest strategy |
| 06 | CRT Engine | Candle-range and liquidity-sweep strategy |
| 07 | ICT/SMC Core | Structure, order-block and fair-value-gap strategy |
| 08 | VPVR/POC | Volume-profile and point-of-control strategy |
| 09 | Asia Session Sweep | Asia-range sweep strategy |
| 10 | News/Sentiment | Strategy using healthy Agent 14 context |
| 11 | Mean Reversion / Vol Bands | Volatility-band mean-reversion strategy |
| 12 | Correlation Deflection | Strategy using healthy Agent 15 context |
| 13 | Wyckoff Schematic | Wyckoff phase and schematic strategy |
| 14 | Macro Catalyst Monitor | Economic-calendar health, freeze and macro context |
| 15 | Intermarket Watcher | DXY, US10Y and US30 context and freshness |
| 16 | Confluence Engine | Unique-agent directional fusion and target planning |
| 17 | Entry Trigger | Time, MTF, quote, spread and final geometry validation |
| 18 | Risk Manager | Broker-aware risk budget, reservations and hard limits |
| 19 | Lifecycle Manager | Confirmed partials, breakeven, trailing and closing |
| 20 | Continuous Learner | Governed PMA, weights and versioned learning snapshots |
| 21 | Resilience Controller | Readiness, disconnect handling and reconciliation policy |

Time Guard and MTF synthesis are Agent 17 policies. Liquidity mapping is an Agent 16 target-planning policy, followed by Agent 17 revalidation. Meta review and continuous adaptation are Agent 20 components. Notifications, providers, storage, IPC and the dashboard are adapters, not numbered agents.

Only Agents 04-13 vote in strategy fusion. Agent 10 is disabled without fresh Agent 14 data. Agent 12 is disabled without fresh Agent 15 data.

## 3. Runtime ownership and state

Exactly one order-producing process may own a broker account, symbol and magic-number tuple. It holds an exclusive process lock and the only command-capable EA connection.

The runtime state machine is:

```text
STOPPED -> BOOTSTRAPPING -> WAITING_EA -> SYNCHRONIZING -> READY
                                      \-> DEGRADED / HALTED
READY -> DISCONNECTED -> SYNCHRONIZING -> READY
```

Only `READY` may create a risk reservation or an OPEN command. Startup and reconnection require:

- compatible authenticated protocol;
- expected account, server, symbol and magic number;
- account mode and trading-permission validation; an order-producing session
  requires a HEDGING account, while SHADOW may observe NETTING without a send path;
- account and broker-symbol metadata;
- atomic account-complete position and pending-order snapshot; foreign
  symbol/magic exposure remains visible, is never mutated, and blocks readiness;
- deals since the last durable cursor;
- durable ledger and risk-state health;
- required provider-policy satisfaction;
- zero unresolved blocking reconciliation differences.

Default invocation is non-trading. Demo or live command capability requires an explicit mode. Real-capital accounts remain disabled unless a separate explicit live-account permission is configured.

The broker protocol pins the local EA and Python gateway to the same
two-value session mode:

- `SHADOW` is the EA default. It permits authenticated observations and
  snapshots with terminal trading disabled. The EA rejects every mutating
  command locally before replay, journal persistence, or broker dispatch.
- `DEMO` is explicitly selected on both sides and retains terminal, account,
  and expert trading-permission requirements. The EA also advertises its
  emergency-stop state; DEMO authentication is rejected while that entry
  fence remains active.

There is no independently configurable execution boolean. A mode mismatch
rejects the handshake, and protocol 4.1 must be promoted atomically with the
matching freshly compiled EA. The gateway also pins `role=MT5_EA` and EA
version `4.01`. REAL accounts are rejected by this release's gateway even
when it is constructed directly.

## 4. Signal and fusion contract

- Direction is an enum (`BUY` or `SELL`). Conviction is an unsigned finite value from 0 through 100.
- A signal carries a unique signal ID, snapshot ID, one Agent 04-13 ID, direction, levels, observation time, regime, session and tags.
- At most one fresh eligible signal per agent and snapshot is considered.
- Three distinct strategy agents must support the selected direction.
- Exact ties are rejected. A configurable directional-dominance margin rejects conflicted coalitions.
- Unknown agents, duplicates, stale signals, mixed snapshots, non-finite values and malformed individual geometry are rejected.
- Fusion output records only actual contributors and their weights.
- Thresholds are immutable instance configuration. Runtime code never mutates module globals.

Directional geometry is validated per signal, after fusion, after regime adjustment, after liquidity mapping, immediately before execution, and against the executable quote:

```text
BUY:  0 < SL < executable ASK < TP1 < TP2 < TP3
SELL: 0 < TP3 < TP2 < TP1 < executable BID < SL
```

Tick normalization, broker stop/freeze distance, quote age, slippage, and minimum reward-to-risk constraints also apply.

## 5. Risk contract

- Broker account snapshots initialize balance, equity and daily baseline. Configuration never impersonates broker equity.
- The risk base is the stricter of balance and equity unless a documented broker-specific policy is configured.
- The absolute per-trade ceiling is 1%. Confidence, ATR, regime and learning adjustments may reduce risk but never increase it beyond the configured hard cap.
- Production defaults to one total position; the system ceiling is three only when an explicitly validated profile allows it.
- Open positions plus pending and unknown OPEN commands consume risk and position capacity across UTC-day rollover; only daily PnL and drawdown accounting reset to a new risk date.
- Risk is reserved durably before an OPEN command is transmitted and released exactly once after a proven rejection or terminal reconciliation.
- Volume is calculated with broker tick size, tick value, contract and currency metadata, then rounded down to the broker step. Minimum-lot over-risk is rejected.
- Daily baseline and halt state are durable by account and UTC risk date. Restart cannot clear a halt. An unverifiable baseline keeps the runtime halted.
- A daily drawdown halt blocks new risk only. It does not silently liquidate an
  already protected position; liquidation requires a separate durable,
  authenticated lifecycle command. The EA never invents a local close command.
- Daily realised PnL is re-derived from UTC-day broker closing deals, and the consecutive-loss streak from broker-confirmed completed trades after history synchronization. Account-snapshot defaults cannot reset either value.
- The EA independently enforces an equal or stricter safety ceiling and may reduce volume, never increase it.

## 6. Execution and identity contract

Every command has stable string identifiers for command, trade and risk reservation. Broker identifiers are stored and transported as strings to avoid integer precision loss in web consumers.

Command state:

```text
CREATED -> SENT -> RECEIVED -> APPLIED
                         \-> REJECTED
             \-> UNKNOWN -> RECONCILED_APPLIED | RECONCILED_NOT_APPLIED
```

A timeout means `UNKNOWN`, never "not executed." Duplicate commands replay their recorded result and cannot create a duplicate broker order.

Before any broker-mutating API call, the EA appends the immutable `PENDING`
command to its common-files journal, flushes it, reopens it, and verifies the
exact appended record. A failed write/readback disables mutation and reconnect.
An ACK/NACK is sent only after its terminal record is also durably verified;
failure after a broker call leaves the Python command unknown for reconciliation.
Journal compaction is verified before the compact file can replace the append
journal.

If the process crashes after an OPEN fill but before local lifecycle
registration, the gateway retains the broker position as unowned quarantined
exposure. Reconciliation may register it only when exactly one durable
SENT/RECEIVED/UNKNOWN OPEN command, its canonical trade and reservation, the
complete raw broker position, and one attributed `IN` deal agree on account,
trade/command/reservation, order/deal/position IDs, direction, price, volume,
broker millisecond time, SL/TP and computed actual risk. The deal, recovery
event, position, command acknowledgement, risk consumption and notification
outbox commit atomically. Missing, duplicated or contradictory evidence keeps
the exposure quarantined and blocks readiness.

The EA emits broker-authoritative events from `OnTradeTransaction`, including order ID, deal ID, position identifier, position ticket, actual price, volume delta, remaining volume, SL/TP, broker reason, costs and millisecond broker time.

`position_version` is a durable logical version, not the raw MT5 millisecond
timestamp. Its broker-time base and monotonic ordinal are stored with a
two-phase commit marker per position, so multiple fills or mutations in the
same millisecond remain strictly ordered across restart. A delayed
`MODIFY_SL` position transaction may reuse cached command attribution only
when exactly one durable fingerprint agrees on position ID, prior version,
target SL, preserved final TP and a strictly newer logical version; otherwise
the event is unattributed and reconciliation blocks.

UTC baseline, halt, deal-cursor and position-version terminal globals are
read back after writes. Multi-key cursor/version state uses an invalid marker
before component writes and a verified commit marker last; partial or legacy
uncommitted tuples fail startup closed.

## 7. Lifecycle contract

The primary position state and lifecycle milestone are separate. Only a broker event or authoritative snapshot may mutate actual fill, remaining volume, actual SL or closed state.

Canonical allocation is based on initial volume:

- TP1: close 50%; after confirmed partial, move SL to breakeven.
- TP2: close 25%; after confirmation, enable structural trailing.
- TP3: close all remaining volume.

Structural trailing uses completed H1 bars only. The most recent strict fractal
pivot is eligible after two completed bars exist on both sides: for BUY, its low
must be lower than all four neighbouring lows; for SELL, its high must be higher
than all four neighbouring highs. The candidate is placed 10 symbol points beyond
the pivot and rounded away from structure to the broker tick size. It is submitted
only when it directionally improves the confirmed stop and satisfies the stricter
of the broker stop/freeze distances. If no confirmed pivot exists, no trailing
command is invented.

One lifecycle action may be in flight per position. Partial volume is normalized down, illegal dust is never left, and too-small positions use a recorded virtual milestone without claiming a fill. A direct gap through TP3 produces one final-close action rather than three queued actions. BUY management uses bid; SELL management uses ask.

The hard timeout is 48 H1 bars. A configured session-close policy may close an unadvanced trade earlier. Disconnect behavior is halt-and-protect: stop entries, retain broker protection, reconcile on return, and close only under an explicit protection-loss policy.

Actual R is:

```text
sum(profit + commission + swap + fee for all closing deals)
----------------------------------------------------------------
entry-time initial risk in account currency
```

## 8. Persistence and learning

The canonical SQLite store uses migrations, foreign keys, WAL, busy timeout, durable synchronization and serialized writes. It contains trades, attributions, append-only events, broker deals, risk state/reservations, provider health, reconciliation reports, durable PMA jobs, learning snapshots and a notification outbox.

If intent, broker-event or risk-state persistence fails, new entries halt. Analytics and notification failures may degrade independently but must expose health.

Learning consumes only authoritative broker-confirmed outcomes with entry-time attribution. A versioned learning snapshot records source event, sample counts, weights, bounds and configuration hash. No multiplier activates below five authoritative outcomes. Learning may not bypass entry, provider or hard-risk rules.

Recovered and synthetic journals are evidence archives. They are not automatically promoted into the production learning set.

## 9. Provider policy

Provider state is one of `STARTING`, `HEALTHY`, `STALE`, `ERROR` or `DISABLED`. Every snapshot includes source time, receive time, sequence and freshness limit.

- Synthetic providers are permitted only in test and simulation.
- Intermarket unavailability disables Agents 12 and 15.
- Macro-calendar unavailability blocks new execution by default because news-blackout protection is unknown.
- An explicit unsafe override may allow technical-only operation, but it visibly disables Agents 10 and 14 and may never report a healthy neutral calendar.

This release implements only explicit HTTPS macro and intermarket provider
adapters. The `broker` provider enum is reserved for a future authenticated
EA relay and is rejected during configuration validation because no canonical
runtime adapter exists. Missing provider support leaves the provider disabled;
values are never fabricated.

## 9.1 UTC risk-day monotonicity

The EA stores a committed, account-scoped durable UTC-day high-water mark.
Account risk state is namespaced by broker server and account login; command,
cursor and position-version state is namespaced by the complete broker
server/account/symbol/magic ownership tuple. A UTC day earlier than the mark
or active in-memory day fails local state closed. A forward jump beyond the
exact next calendar day also fails closed. Only a continuously running
session whose prior baseline is verified may automatically verify a newly
created exact-next-day baseline. Existing unverified baselines are elevated
only by exact one-day approval inputs. Persisted authorization and halt
markers accept only exact `0` or `1`.

After a legitimate multi-day shutdown, a cold restart may advance the
committed day only when `InpApproveRiskDayForwardJumpUtcDay` exactly matches
the operator-verified current UTC day. This recovery never carries forward
verification: the current-day baseline remains unverified and requires a
separate exact equity/day approval after review. An active session never
accepts this forward-gap override.

Protocol 4.1 migration requires an operator to verify machine UTC and set
`InpApproveRiskDayHighWaterUtcDay` to the exact current `YYYY-MM-DD` once for
each account. The EA read-verifies the resulting high-water mark. It does not
blindly seed this authority from the host clock. Remove that approval input
after the successful migration and preserve the recorded state hashes.

## 10. Integrations and deployment

Structured events and notification outbox records are committed with the trading transaction. Telegram, Discord and website workers use bounded exponential retry, jitter and dead-letter state. They distinguish signal, submission, fill, partial, modification and close events.

The dashboard is read-only, loopback by default, authenticated when exposed, and never receives broker or service-role credentials.

Deployment runs one canonical module entry point (`python -m python_swarm.runtime_app`) with an explicit `CHECK`, `SHADOW`, or `DEMO` mode, private mode-isolated directories below `QSX_DATA_DIR`, protected environment secrets, graceful SIGTERM, preflight/migration checks and no competing IPC service. Real-account launch is not exposed by this release. Broker-connected modes run as a non-root identity from a root-owned installed release and fail before mutable startup if that identity can write or replace the source, runtime configuration, interpreter, module search roots, or any of their path ancestors. Runtime code never uses `chmod` to disguise a writable source checkout. TCP loopback is the supported transport until another transport is implemented and tested.

## 11. Promotion gates

Before MT5 demo AutoTrading:

- no live-looking secrets in source, archives, image context or logs;
- tests leave production and recovery trees byte-for-byte unchanged;
- no runtime destructive cleanup or import-time persistence side effects;
- v4 protocol, MQL5 compilation and golden contract tests pass;
- wrong account/magic/protocol, second client, malformed/oversized frame and stale provider are rejected;
- crash/restart tests at every execution boundary reconcile to broker truth;
- zero duplicate orders/events, invalid geometry, risk-cap violations or premature local lifecycle mutation;
- broker statement and ledger reconcile exactly within documented broker rounding.

The frozen corrected release then runs for at least 30 trading days, preferably 60 calendar days. A material strategy, risk, protocol, lifecycle or persistence change resets that period. Promotion to real capital is always a separate manual decision.
