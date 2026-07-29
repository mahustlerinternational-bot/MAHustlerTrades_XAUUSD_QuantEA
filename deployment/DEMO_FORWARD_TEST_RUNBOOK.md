# MT5 demo forward-test runbook

Status: required promotion evidence; the observation period has **not** been
completed for this remediation build.

This runbook authorizes evidence collection on an exactly pinned MT5 demo
account only. It does not establish profitability and cannot authorize a real
account.

## 1. Freeze prerequisites

Do not start the observation clock until all items are true:

- Every credential that may have appeared in source, archives, logs, images,
  or chat has been revoked/reissued, and a clean secret scan is retained.
- Unit, integration, replay, disconnect/restart and broker-reconciliation
  suites pass on the exact release candidate.
- MetaEditor compiles the exact frozen MQL5 source with zero errors and zero
  warnings. Install only the freshly produced EX5 and retain its SHA-256 hash,
  the source hash and the native compiler log; a stale checked-in EX5 is not
  acceptable evidence.
- A signed, read-only build manifest binds source, Python dependencies,
  schema/migrations, runtime JSON, test results, MQL5 source, compiler result
  and fresh EX5 hashes.
- The release/config/interpreter/import paths pass immutable-deployment
  preflight under the non-root service identity. `QSX_DATA_DIR` is private and
  separate from the release.
- `CHECK` passes; `SHADOW` authenticates the exact demo account, broker server,
  symbol, magic and protocol; the initial atomic snapshot reconciles to zero
  blocking differences while MT5 AutoTrading remains off.
- The pinned demo account reports `HEDGING`. The account-complete snapshot
  exposes every position and pending order, and no foreign symbol/magic
  exposure remains as a blocking reconciliation difference.
- Macro/intermarket agents are backed by genuine healthy providers, or are
  explicitly disabled exactly as the frozen configuration records.

Record the manifest hash, account/server identity, magic, start timestamp and
UTC risk date in the forward-test evidence package. The supplied systemd unit
is SHADOW-only; DEMO requires a separately reviewed, explicit demo service
invocation of `python -m python_swarm.runtime_app --mode demo` with the
protected `QSX_CONFIG` and `QSX_DATA_DIR`.

## 2. Minimum observation window

Collect at least **30 trading days** on the frozen build; **60 calendar days is
preferred**. A trading day counts only when the scheduled session ran with
complete logs, broker evidence and the daily reconciliation below. Weekends,
outages, incomplete evidence and halted days do not silently count as trading
days.

Any material change to strategy, voting, risk, provider policy, protocol,
execution, lifecycle, reconciliation, persistence/migrations, broker-facing
configuration, MQL5 source or EX5 resets day zero. Freeze a new signed manifest
and repeat all prerequisites. Documentation-only or notification-copy changes
may be classified non-material only through a recorded review.

## 3. Start-of-day checks

- Confirm the running manifest/config/EX5 hashes match the frozen record.
- Confirm account mode is `DEMO` and account, server, symbol and magic match.
- Confirm one canonical runtime and one authenticated EA connection own the
  tuple; no retired orchestrator or standalone command bridge is running.
- Reconcile broker positions, pending orders and deals since the durable cursor
  before readiness. Do not clear an `UNKNOWN` command manually.
- Record balance, equity, UTC daily baseline, drawdown state, risk reservations,
  open-position capacity, provider freshness and notification dead-letter
  health.

## 4. End-of-day reconciliation

Retain a dated, immutable report comparing the broker statement/snapshot with
the authoritative ledger:

- every owned order, deal and position identity;
- initial/final volume, partial fills, remaining volume, SL/TP changes and
  broker timestamps;
- profit, commission, swap, fee, net PnL and actual R;
- strategy attribution, regime, session and entry/close timestamps;
- risk reservations/releases, daily baseline, drawdown halts and position
  counts;
- lifecycle TP1/TP2/TP3, breakeven, structural trailing, timeout and close
  events;
- reconciliation issues, command `UNKNOWN` states, outbox dead letters,
  disconnects/restarts and recovery results.

The required daily result is zero unexplained broker-versus-ledger difference.
Document broker rounding explicitly; never edit the ledger to force a match.

## 5. Incident and zero-incident criteria

Immediately halt new entries, preserve broker protection and capture an atomic
snapshot if any of these occurs:

- duplicate/unattributed order or deal, wrong identity/protocol, or competing
  command-capable process;
- invalid directional SL/TP, missing broker protection, incorrect partial,
  breakeven, trailing or final-close behavior;
- per-trade/aggregate risk, drawdown halt or position-capacity violation;
- unresolved command ambiguity, recovery/reconciliation mismatch, cursor gap,
  ledger corruption or non-idempotent restart;
- execution using stale/fabricated required provider evidence;
- loss of complete audit timestamps, costs, attribution or actual-R evidence.

A qualifying observation has zero such safety incidents and zero unexplained
daily reconciliation differences. Ordinary losing trades are not software
incidents when they respect the frozen rules; they remain part of the
performance record. Diagnose from preserved evidence, correct the defect,
classify materiality, and reset the clock for every material correction.

## 6. Completion package

At the end, retain the signed manifest, native compiler record, fresh EX5/hash,
test/secret-scan results, daily reports, broker statement, ledger export,
availability/outage log, all incident classifications and a signed count of
qualifying trading/calendar days. Independent review must confirm the complete
evidence before any later promotion discussion.

Completion of this runbook still does not enable or approve real-capital
trading; this release has no real-account launcher.
