#!/usr/bin/env bash
set -euo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$ROOT/.venv"
PYTHON_BIN="${PYTHON_BIN:-python3.12}"

if (( $# > 1 )) || [[ ${1:-} != "" && ${1:-} != "--dev" ]]; then
  printf 'Usage: bash server_setup.sh [--dev]\n' >&2
  exit 2
fi
if [[ "$(id -u)" == "0" ]]; then
  printf 'Developer setup must not run as root. Use a normal development user.\n' >&2
  exit 1
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  printf 'Required Python 3.12 interpreter not found: %s\n' "$PYTHON_BIN" >&2
  printf 'Install Python 3.12 or set PYTHON_BIN to a supported 3.11-3.12 interpreter.\n' >&2
  exit 1
fi

if ! "$PYTHON_BIN" -c 'import sys; raise SystemExit(0 if (3, 11) <= sys.version_info[:2] <= (3, 12) else 1)'; then
  printf 'Unsupported Python version. QUANT-SWARM-XAU requires Python 3.11-3.12; production uses 3.12.\n' >&2
  exit 1
fi

if [[ -L "$VENV" ]]; then
  printf 'Refusing symlinked virtual environment path: %s\n' "$VENV" >&2
  exit 1
fi

if [[ -e "$ROOT/.env" ]]; then
  if [[ -L "$ROOT/.env" || ! -f "$ROOT/.env" ]]; then
    printf 'Refusing non-regular environment file: %s\n' "$ROOT/.env" >&2
    exit 1
  fi
  env_mode="$(stat -c '%a' "$ROOT/.env")"
  env_uid="$(stat -c '%u' "$ROOT/.env")"
  if [[ "$env_uid" != "$(id -u)" || \
        ( "$env_mode" != "600" && "$env_mode" != "400" ) ]]; then
    printf '%s must already be owned by you with mode 0600 or 0400; refusing to modify it.\n' \
      "$ROOT/.env" >&2
    exit 1
  fi
fi

rebuild_venv="no"
if [[ ! -x "$VENV/bin/python3" ]]; then
  rebuild_venv="yes"
elif ! "$VENV/bin/python3" -c 'import sys; raise SystemExit(0 if (3, 11) <= sys.version_info[:2] <= (3, 12) else 1)'; then
  printf 'Existing virtual environment uses an unsupported Python; rebuilding it.\n'
  rebuild_venv="yes"
fi
if [[ "$rebuild_venv" == "yes" ]]; then
  printf 'Creating or repairing virtual environment at %s\n' "$VENV"
  "$PYTHON_BIN" -m venv --clear "$VENV"
fi

"$VENV/bin/python3" -m pip install --upgrade pip
"$VENV/bin/python3" -m pip install --require-hashes \
  -r "$ROOT/python_swarm/requirements.lock"
if [[ "${1:-}" == "--dev" ]]; then
  "$VENV/bin/python3" -m pip install -r "$ROOT/requirements-dev.txt"
fi

install -d -m 0700 "$ROOT/var"

if [[ ! -e "$ROOT/.env" ]]; then
  install -m 0600 "$ROOT/deployment/.env.example" "$ROOT/.env"
  printf 'Created %s. Fill secrets locally; never commit or archive it.\n' "$ROOT/.env"
fi

printf '\nDependencies installed. Running the canonical non-trading preflight.\n'
if ! "$ROOT/deployment/start.sh" --check; then
  printf '\nSetup is incomplete because the canonical CHECK preflight failed.\n' >&2
  exit 1
fi

printf '\nSetup complete. This writable checkout is for CHECK/development only.\n'
printf 'Broker-connected modes require a non-root service identity and a separate root-owned, non-writable release tree.\n'
