#!/usr/bin/env bash
set -euo pipefail
umask 077

if (( $# != 2 )); then
  printf 'Usage: %s PRIVATE_ED25519_KEY FILE_TO_SIGN\n' "$0" >&2
  exit 2
fi

key="$1"
target="$2"
signature="${target}.sig"

if [[ -L "$key" || ! -f "$key" ]]; then
  printf 'Signing key must be a regular non-symlink file.\n' >&2
  exit 1
fi
if [[ "$(stat -c '%u' "$key")" != "$(id -u)" ]]; then
  printf 'Signing key must be owned by the invoking identity.\n' >&2
  exit 1
fi
key_mode="$(stat -c '%a' "$key")"
if [[ "$key_mode" != "400" && "$key_mode" != "600" ]]; then
  printf 'Signing key mode must be 0400 or 0600.\n' >&2
  exit 1
fi
if [[ -L "$target" || ! -f "$target" ]]; then
  printf 'Signing target must be a regular non-symlink file.\n' >&2
  exit 1
fi
if [[ -e "$signature" || -L "$signature" ]]; then
  printf 'Refusing to replace existing signature: %s\n' "$signature" >&2
  exit 1
fi

exec ssh-keygen -Y sign -f "$key" -n file "$target"
