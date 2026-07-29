#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${QSX_IPC_PORT:-9876}"

cat <<EOF
QUANT-SWARM-XAU read-only connection diagnostics
Project: $ROOT
Expected endpoint: 127.0.0.1:$PORT (authenticated protocol v4)
EOF

printf '\nListener ownership:\n'
if ss -H -ltnp "sport = :$PORT" 2>/dev/null; then
  :
else
  printf '  no listener found\n'
fi

printf '\nCanonical runtime processes:\n'
if pgrep -af '[p]ython.*python_swarm\.runtime_app' 2>/dev/null; then
  :
else
  printf '  no canonical runtime found\n'
fi

printf '\nRetired command-capable processes (must be absent):\n'
if pgrep -af '[p]ython.*(evolved_orchestrator|orchestrator\.py|run_ipc|ipc_bridge)' 2>/dev/null; then
  printf '  UNSAFE: stop the listed retired runtime before continuing.\n' >&2
  exit 1
else
  printf '  none found\n'
fi

cat <<'EOF'

This diagnostic intentionally does not open a TCP connection: the gateway
allows one authenticated EA client, and an unauthenticated probe must not race
or displace it. Broker/account synchronization is reported by the canonical
runtime health state, not by TCP reachability alone.
EOF
