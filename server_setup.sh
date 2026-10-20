#!/usr/bin/env bash
set -euo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$ROOT/.venv"
REQUESTED_PYTHON_BIN="${PYTHON_BIN:-python3.12}"
for inherited_name in ${!PYTHON@} ${!PIP_@}; do
  unset "$inherited_name"
done
PYTHON_BIN="$REQUESTED_PYTHON_BIN"
unset REQUESTED_PYTHON_BIN inherited_name
export PYTHONDONTWRITEBYTECODE=1
export PYTHONHASHSEED=0
export PYTHONNOUSERSITE=1
export PIP_CONFIG_FILE=/dev/null
export PIP_DISABLE_PIP_VERSION_CHECK=1
export PIP_NO_CACHE_DIR=1

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
  printf 'Install CPython 3.12 or set PYTHON_BIN to that exact interpreter.\n' >&2
  exit 1
fi
PYTHON_BIN="$(command -v "$PYTHON_BIN")"

if ! "$PYTHON_BIN" -c '
import platform
import sys
from importlib import metadata

libc_name, libc_version = platform.libc_ver()
try:
    libc = tuple(int(part) for part in libc_version.split(".")[:2])
except ValueError:
    libc = ()
valid = (
    sys.implementation.name == "cpython"
    and sys.version_info[:3] == (3, 12, 13)
    and platform.machine() == "x86_64"
    and libc_name == "glibc"
    and libc >= (2, 28)
    and metadata.version("pip") == "25.1.1"
)
raise SystemExit(0 if valid else 1)
'; then
  printf '%s\n' \
    'Unsupported interpreter. RC2 requires CPython 3.12.13, pip 25.1.1, x86_64, and glibc 2.28+.' >&2
  exit 1
fi

if [[ "${1:-}" == "--dev" ]]; then
  LANE="test-py312"
  LOCK="$ROOT/requirements-test-py312.lock"
else
  LANE="runtime-py312"
  LOCK="$ROOT/python_swarm/requirements-runtime-py312.lock"
fi
WHEELHOUSE="${QSX_WHEELHOUSE:-$ROOT/wheelhouse/$LANE}"
ALLOWED_SIGNERS="${QSX_WHEELHOUSE_ALLOWED_SIGNERS:-}"
SIGNING_IDENTITY="${QSX_WHEELHOUSE_SIGNING_IDENTITY:-mahustlertrades-release}"

if [[ -L "$WHEELHOUSE" || ! -d "$WHEELHOUSE" ]]; then
  printf 'Signed %s wheelhouse is missing or unsafe: %s\n' \
    "$LANE" "$WHEELHOUSE" >&2
  exit 1
fi
if [[ -z "$ALLOWED_SIGNERS" ]]; then
  printf '%s\n' \
    'QSX_WHEELHOUSE_ALLOWED_SIGNERS must select the protected release signer list.' >&2
  exit 1
fi
if [[ ! -f "$ROOT/tools/validate_wheelhouse.py" || \
      ! -f "$ROOT/tools/assert_installed_closure.py" ]]; then
  printf '%s\n' 'Dependency verification tools are missing from this source tree.' >&2
  exit 1
fi

SOURCE_COMMIT="${QSX_SOURCE_COMMIT:-}"
VERIFY_SOURCE_ARGS=()
if [[ -e "$ROOT/.git" ]]; then
  # The validator performs the Git lookup with inherited GIT_* variables
  # scrubbed, replace refs disabled, and an exact clean-worktree check.
  VERIFY_SOURCE_ARGS=(--require-source-inputs)
elif [[ -z "$SOURCE_COMMIT" && -f "$ROOT/RELEASE.json" ]]; then
  SOURCE_COMMIT="$("$PYTHON_BIN" - "$ROOT/RELEASE.json" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    value = json.load(handle).get("source_commit", "")
print(value if isinstance(value, str) else "")
PY
)"
fi
if [[ ! -e "$ROOT/.git" ]]; then
  if [[ ! "$SOURCE_COMMIT" =~ ^[0-9a-f]{40}$ ]]; then
    printf '%s\n' \
      'A full source commit is required from RELEASE.json or QSX_SOURCE_COMMIT.' >&2
    exit 1
  fi
  VERIFY_SOURCE_ARGS=(
    --expected-source-commit "$SOURCE_COMMIT"
    --require-source-inputs
  )
fi

if ! "$PYTHON_BIN" "$ROOT/tools/validate_wheelhouse.py" \
     --source-root "$ROOT" \
     --wheelhouse "$WHEELHOUSE" \
     --lane "$LANE" \
     --allowed-signers "$ALLOWED_SIGNERS" \
     --signing-identity "$SIGNING_IDENTITY" \
     --interpreter "$PYTHON_BIN" \
     "${VERIFY_SOURCE_ARGS[@]}"; then
  printf '%s\n' 'Signed wheelhouse validation failed before environment mutation.' >&2
  exit 1
fi

if [[ -L "$VENV" || ( -e "$VENV" && ! -d "$VENV" ) ]]; then
  printf 'Refusing unsafe virtual environment path: %s\n' "$VENV" >&2
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

STAGE="$(mktemp -d "$ROOT/.venv.stage.XXXXXXXX")"
PREVIOUS=""
SWAPPED="no"
COMMITTED="no"

remove_private_tree() {
  local target="$1"
  case "$target" in
    "$ROOT"/.venv.stage.*|"$ROOT"/.venv.previous.*|"$VENV")
      rm -rf -- "$target"
      ;;
    *)
      printf 'Refusing unsafe cleanup target: %s\n' "$target" >&2
      return 1
      ;;
  esac
}

rollback_setup() {
  local status=$?
  if [[ "$COMMITTED" != "yes" ]]; then
    if [[ "$SWAPPED" == "yes" && -e "$VENV" ]]; then
      remove_private_tree "$VENV"
    fi
    if [[ -n "$PREVIOUS" && -d "$PREVIOUS" && ! -e "$VENV" ]]; then
        mv -- "$PREVIOUS" "$VENV"
        PREVIOUS=""
    fi
  fi
  if [[ -n "$STAGE" && -d "$STAGE" ]]; then
    remove_private_tree "$STAGE"
  fi
  return "$status"
}
trap rollback_setup EXIT

printf 'Building a fresh transactional %s environment.\n' "$LANE"
"$PYTHON_BIN" -m venv --clear "$STAGE"
"$STAGE/bin/python3" -m pip uninstall -y setuptools wheel >/dev/null
"$STAGE/bin/python3" -m pip install \
  --no-index \
  --find-links "$WHEELHOUSE/wheels" \
  --only-binary=:all: \
  --no-compile \
  --require-hashes \
  -r "$LOCK"
"$STAGE/bin/python3" -m pip check
"$STAGE/bin/python3" "$ROOT/tools/assert_installed_closure.py" \
  --lock "$LOCK" \
  --implementation cpython \
  --python-version 3.12.13 \
  --machine x86_64 \
  --allow-bootstrap pip==25.1.1

install -d -m 0700 "$ROOT/var"

if [[ -d "$VENV" ]]; then
  PREVIOUS="$ROOT/.venv.previous.$$"
  if [[ -e "$PREVIOUS" || -L "$PREVIOUS" ]]; then
    printf 'Refusing occupied transactional backup path: %s\n' "$PREVIOUS" >&2
    exit 1
  fi
  mv -- "$VENV" "$PREVIOUS"
fi
mv -- "$STAGE" "$VENV"
STAGE=""
SWAPPED="yes"

printf '\nDependencies installed. Running the canonical non-trading preflight.\n'
if ! QSX_CONFIG="$ROOT/deployment/config.check.example.json" \
     QSX_DATA_DIR="$ROOT/var" \
     QSX_PYTHON_BIN="$VENV/bin/python3" \
     "$ROOT/deployment/start.sh" --check; then
  printf '\nSetup is incomplete because the canonical CHECK preflight failed.\n' >&2
  exit 1
fi

COMMITTED="yes"
if [[ -n "$PREVIOUS" && -d "$PREVIOUS" ]]; then
  remove_private_tree "$PREVIOUS"
  PREVIOUS=""
fi
trap - EXIT

if [[ ! -e "$ROOT/.env" ]]; then
  install -m 0600 "$ROOT/deployment/.env.example" "$ROOT/.env"
  printf 'Created %s. Fill secrets locally; never commit or archive it.\n' "$ROOT/.env"
fi

printf '\nSetup complete. This writable checkout is for CHECK/development only.\n'
printf 'Broker-connected modes require a non-root service identity and a separate root-owned, non-writable release tree.\n'
