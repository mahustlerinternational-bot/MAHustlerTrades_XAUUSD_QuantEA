#!/usr/bin/env bash
set -euo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
Usage: bash deployment/start.sh [--check|--shadow|--demo]

  --check    Validate configuration and prerequisites without a broker connection
  --shadow   Connect to the pinned demo account without sending orders (default)
  --demo     Permit demo-account commands after all runtime readiness gates pass

The wrapper launches only: python -m python_swarm.runtime_app
Real-account and retired-runtime launch modes are intentionally unavailable.
QSX_CONFIG selects the schema-v2 configuration and QSX_DATA_DIR selects the
mode-isolated writable data root.
EOF
}

mode="shadow"
case "${1:-}" in
  "") ;;
  --check) mode="check" ;;
  --shadow) mode="shadow" ;;
  --demo) mode="demo" ;;
  -h|--help)
    usage
    exit 0
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
if (( $# > 1 )); then
  usage >&2
  exit 2
fi

# Local CHECK runs may use a private .env file.  Parse assignments as data;
# never source the file as shell code.  Deployed services should use the
# service manager's root-owned EnvironmentFile instead.
ENV_FILE="${QSX_ENV_FILE:-$ROOT/.env}"
if [[ -f "$ENV_FILE" ]]; then
  env_mode="$(stat -c '%a' "$ENV_FILE")"
  env_uid="$(stat -c '%u' "$ENV_FILE")"
  if (( (8#$env_mode & 022) != 0 )); then
    printf '%s must not be writable by group or other (currently %s).\n' \
      "$ENV_FILE" "$env_mode" >&2
    exit 1
  fi
  if [[ "$env_uid" != "$(id -u)" && "$env_uid" != "0" ]]; then
    printf '%s must be owned by the service identity or root.\n' "$ENV_FILE" >&2
    exit 1
  fi
  if [[ "$mode" != "check" && -w "$ENV_FILE" ]]; then
    printf '%s is writable by the service identity; broker mode is blocked.\n' \
      "$ENV_FILE" >&2
    exit 1
  fi
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "$line" || "$line" == \#* ]] && continue
    if [[ ! "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]]; then
      printf '%s contains an invalid assignment; shell syntax is not accepted.\n' \
        "$ENV_FILE" >&2
      exit 1
    fi
    key="${line%%=*}"
    value="${line#*=}"
    export "$key=$value"
  done < "$ENV_FILE"
fi

PY="${QSX_PYTHON_BIN:-$ROOT/.venv/bin/python3}"
if [[ ! -x "$PY" ]]; then
  printf 'Python interpreter is missing or not executable: %s\n' "$PY" >&2
  printf 'For a developer CHECK run, execute: bash server_setup.sh\n' >&2
  exit 1
fi

export QSX_MODE="$mode"
if [[ -z "${QSX_CONFIG:-}" ]]; then
  printf 'QSX_CONFIG is required; select an explicit protected schema-v2 JSON file.\n' >&2
  exit 1
fi
export QSX_CONFIG
if [[ "$mode" == "check" ]]; then
  export QSX_DATA_DIR="${QSX_DATA_DIR:-$ROOT/var}"
elif [[ -z "${QSX_DATA_DIR:-}" ]]; then
  printf 'QSX_DATA_DIR is required for broker-connected modes.\n' >&2
  exit 1
fi

if [[ -L "$QSX_CONFIG" || ! -f "$QSX_CONFIG" || ! -r "$QSX_CONFIG" ]]; then
  printf 'Runtime configuration is missing or unreadable: %s\n' "$QSX_CONFIG" >&2
  exit 1
fi

cd "$ROOT"
exec "$PY" -m python_swarm.runtime_app \
  --mode "$mode" \
  --config "$QSX_CONFIG"
