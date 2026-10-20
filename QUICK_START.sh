#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cat <<'EOF'
QUANT-SWARM-XAU safe quick start

1. Revoke any credentials that were previously stored in this project.
2. Follow docs/DEPENDENCY_REPRODUCIBILITY.md, verify the signed CPython 3.12
   wheelhouse, set QSX_WHEELHOUSE to its lane directory, then run:
     bash server_setup.sh
3. Read docs/CANONICAL_ARCHITECTURE.md and docs/README.md.
4. Keep MT5 AutoTrading disabled and run the non-trading check:
     QSX_CONFIG="$PWD/deployment/config.check.example.json" \
       bash deployment/start.sh --check
5. Package a reviewed release under a root-owned, service-non-writable tree.
   Derive the schema-v2 runtime JSON from config.shadow.example.json, put it in
   a root-owned configuration directory, and set QSX_CONFIG and QSX_DATA_DIR
   in the service environment. See deployment/README.md.
6. Start SHADOW from that installed release before considering DEMO:
     bash deployment/start.sh --shadow
7. Use --demo only after the complete demo promotion checklist passes.
   Real-account launch is not exposed and real-account execution is blocked.
   Then follow deployment/DEMO_FORWARD_TEST_RUNBOOK.md for at least 30 trading
   days (preferably 60 calendar days); this period has not yet been completed.

Never paste Telegram, Discord, Supabase, broker, or website credentials into
source files, shell scripts, documentation, chat, logs, or command arguments.

The only supported runtime module is:
  python -m python_swarm.runtime_app --mode {check,shadow,demo}
EOF

printf '\nProject root: %s\n' "$ROOT"
