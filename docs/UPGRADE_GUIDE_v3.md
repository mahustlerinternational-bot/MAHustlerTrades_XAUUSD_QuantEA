# Migration from experimental v2/v3 runtimes

Status: mandatory breaking migration to the v4.0.0-rc1 safety model  
Date: 2026-07-15

This is not a drop-in upgrade. The old `orchestrator.py`, `evolved_orchestrator.py`, `ipc_bridge.py`, adaptive parameter mutation, per-agent simulation journals, and legacy start commands are not compatible with the canonical execution and evidence contracts.

Real-account execution remains blocked throughout this migration.

## What must not be carried forward

- Do not start an old runtime or a standalone command-capable IPC service.
- Do not import recovered or synthetic journals as broker truth or learning history.
- Do not use configuration balance as equity initialization.
- Do not activate learned weights directly from a mutable file or an unapproved calculation.
- Do not infer a fill from an acknowledgement, elapsed time, price proximity, or the mere existence of a position.
- Do not infer lifecycle completion from a target touch.
- Do not describe calendar-only data as directional news sentiment.
- Do not enable Agent 10 or Agent 12 when their required provider evidence is unavailable.
- Do not reuse an old EX5 unless its source hash and native compiler record match the frozen release.

## Required migration order

1. Rotate every credential that may have appeared in source, archives, logs, or images.
2. Preserve historical databases as read-only evidence and initialize a new migrated canonical ledger.
3. Configure the exact HEDGING demo account, broker server, symbol, magic
   number, data directory, and IPC HMAC secret. NETTING is observation-only in
   SHADOW and cannot be promoted to order-producing DEMO.
4. Install the protocol-v4 EA compiled from the same frozen source revision as the Python runtime.
5. Start in `CHECK`, then `SHADOW`; verify broker identity and full reconciliation without sending commands.
6. Exercise `DEMO` only after every promotion gate in [CANONICAL_ARCHITECTURE.md](CANONICAL_ARCHITECTURE.md) passes.
7. Freeze the corrected release and observe it for at least 30 trading days, preferably 60 calendar days. Any material strategy, risk, protocol, lifecycle, or persistence change restarts the clock.
8. Treat real-capital promotion as a separate manual risk decision. It is never an automatic result of the demo period.

All three available modes use `python -m python_swarm.runtime_app` (or the
restricted `deployment/start.sh` wrapper) with `QSX_CONFIG` and `QSX_DATA_DIR`.
Do not run `SHADOW` or `DEMO` from this writable migration checkout: install a
root-owned release and use a non-root service identity that cannot modify the
release, runtime JSON, interpreter, or module search roots. The preflight
rejects mutable paths instead of changing their permissions.

## Data migration rules

Canonical identifiers are strings. Decimal monetary and volume values use exact decimal representations at persistence boundaries. A migrated trade is eligible for analytics or learning only when its immutable entry context and all broker closing deals are authoritatively attributed.

Records that lack exact account/symbol/magic scope, trade identity, initial volume, original stop, initial risk, regime, session, strategy attribution, or broker timestamps remain quarantined. They may be inspected but cannot influence decisions.

## Learning migration rules

The v4 learner creates bounded proposals from authoritative outcomes, with at least five samples per exact context. Proposals are versioned, hashed, reviewed, approved, and activated for an exact account/symbol/magic/configuration/regime/session scope. Learning can reduce or redistribute strategy influence within policy; it cannot loosen provider, entry, lifecycle, or hard-risk rules.

Previous projected profit factors, win rates, signal counts, seasonal priors, and “self-improvement” claims are not migration inputs.

## Completion evidence

A migration is complete only when the release bundle contains:

- a canonical roster/configuration check;
- migration and SQLite integrity results;
- protocol and native MQL5 build results;
- unit, integration, replay, restart, and reconciliation results;
- a broker-statement-versus-ledger report;
- a secret-scan result;
- a demo observation report with the exact frozen build hashes.

Until then, the build remains a remediation or research build, not a production trading system.
