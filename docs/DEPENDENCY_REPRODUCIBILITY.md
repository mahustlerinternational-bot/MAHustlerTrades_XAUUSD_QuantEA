# Offline dependency reproducibility

Status: RC2 dependency gate  
Runtime target: CPython 3.12.13, pip 25.1.1, x86_64, glibc 2.28+

The project has three independent dependency lanes. They must never be merged
or substituted for one another:

| Lane | Purpose | Canonical lock | Interpreter |
|---|---|---|---|
| `runtime-py312` | Broker runtime and container | `python_swarm/requirements-runtime-py312.lock` | CPython 3.12.13 |
| `test-py312` | Canonical release tests | `requirements-test-py312.lock` | CPython 3.12.13 |
| `test-py314` | Forward-compatibility tests only | `requirements-test-py314.lock` | CPython 3.14.4 |

The 3.14 lane is never a broker-runtime input. A test wheelhouse is never a
container input. The repository intentionally contains no generated wheels.

## Trust model

Each lane is built from a clean, exact Git worktree and its canonical hashed
lock. The builder:

- scrubs inherited Git replacement/configuration variables;
- disables Git replace objects;
- requires the exact worktree top level and rejects tracked, staged, or
  untracked changes;
- snapshots the lock and input files and compares them with `HEAD`;
- invokes pinned pip with `download`, `--require-hashes`, `--no-deps`, and
  `--only-binary=:all:`;
- rejects source distributions, missing or extra packages, duplicate
  distributions, unauthorized hashes, and incompatible wheel tags;
- writes a canonical manifest binding the lane, source commit, lock, inputs,
  interpreter executable, ABI, platform, every filename, size, and hash; and
- signs that manifest with OpenSSH's `file` namespace.

The validator snapshots the trust anchor, manifest, signature, directories,
and wheels through no-follow file descriptors. It rejects symlinks,
hard-linked files, writable artifacts, signature failures, noncanonical
manifests, and any closure mismatch. Validation never repairs an artifact.

Keep the private signing key outside the repository, release archive, Docker
context, and runtime host. Protect it with mode `0600` or `0400`. The allowed
signers file is the deployment trust anchor; protect it from the service
identity and from group/world writes.

## Build the signed lanes

Run this only from a fully reviewed and clean commit. The builder refuses a
dirty worktree. The examples use the supported Area 6 Python base and place
generated artifacts outside the repository:

```bash
export SOURCE_ROOT="$PWD"
export OUTPUT_ROOT="/var/tmp/quant-swarm-wheelhouse"
export SIGNING_KEY="$HOME/.config/quant-swarm-xau/dependency-signing"
export SIGNING_IDENTITY="mahustlertrades-release"
export PY312="/usr/bin/python3.12"
export PY314="/usr/bin/python3.14"

"$PY312" tools/build_wheelhouse.py \
  --source-root "$SOURCE_ROOT" \
  --lane runtime-py312 \
  --python "$PY312" \
  --signing-key "$SIGNING_KEY" \
  --signing-identity "$SIGNING_IDENTITY" \
  --output-root "$OUTPUT_ROOT"

"$PY312" tools/build_wheelhouse.py \
  --source-root "$SOURCE_ROOT" \
  --lane test-py312 \
  --python "$PY312" \
  --signing-key "$SIGNING_KEY" \
  --signing-identity "$SIGNING_IDENTITY" \
  --output-root "$OUTPUT_ROOT"

"$PY314" tools/build_wheelhouse.py \
  --source-root "$SOURCE_ROOT" \
  --lane test-py314 \
  --python "$PY314" \
  --signing-key "$SIGNING_KEY" \
  --signing-identity "$SIGNING_IDENTITY" \
  --output-root "$OUTPUT_ROOT"
```

The commands require pip 25.1.1 in each builder interpreter. They fail rather
than upgrading pip or resolving a different dependency. Git and OpenSSH must
resolve to root-owned, protected system executables. Generate a new, reviewed
lane directory for a new source commit; never overwrite one.

## Validate before use

An allowed signers entry has the OpenSSH form:

```text
mahustlertrades-release ssh-ed25519 AAAA... release-key-comment
```

Validate each lane independently. Supplying `--interpreter` also requires the
consumer to match the signed resolved executable identity and pip version:

```bash
export ALLOWED_SIGNERS="/etc/quant-swarm-xau/dependency-allowed-signers"

"$PY312" tools/validate_wheelhouse.py \
  --source-root "$SOURCE_ROOT" \
  --wheelhouse "$OUTPUT_ROOT/runtime-py312" \
  --lane runtime-py312 \
  --allowed-signers "$ALLOWED_SIGNERS" \
  --signing-identity "$SIGNING_IDENTITY" \
  --interpreter "$PY312" \
  --require-source-inputs

"$PY312" tools/validate_wheelhouse.py \
  --source-root "$SOURCE_ROOT" \
  --wheelhouse "$OUTPUT_ROOT/test-py312" \
  --lane test-py312 \
  --allowed-signers "$ALLOWED_SIGNERS" \
  --signing-identity "$SIGNING_IDENTITY" \
  --interpreter "$PY312" \
  --require-source-inputs

"$PY314" tools/validate_wheelhouse.py \
  --source-root "$SOURCE_ROOT" \
  --wheelhouse "$OUTPUT_ROOT/test-py314" \
  --lane test-py314 \
  --allowed-signers "$ALLOWED_SIGNERS" \
  --signing-identity "$SIGNING_IDENTITY" \
  --interpreter "$PY314" \
  --require-source-inputs
```

In a release archive without Git metadata, pass the full reviewed commit using
`--expected-source-commit`. Keep `--require-source-inputs` so the packaged
requirements inputs remain bound to the signature.

## Transactional developer environment

`server_setup.sh` validates the signed lane before touching `.venv`. It builds
a staging environment, installs offline, runs `pip check`, proves the exact
installed distribution closure, and runs the CHECK preflight. Only then does
it replace `.venv`; a failure restores the previous environment.

```bash
PYTHON_BIN="$PY312" \
QSX_WHEELHOUSE="$OUTPUT_ROOT/test-py312" \
QSX_WHEELHOUSE_ALLOWED_SIGNERS="$ALLOWED_SIGNERS" \
QSX_WHEELHOUSE_SIGNING_IDENTITY="$SIGNING_IDENTITY" \
bash server_setup.sh --dev
```

Omit `--dev` and select `runtime-py312` only when constructing the runtime
environment. Setup is deliberately offline after the wheelhouse has been
built.

## Offline Docker build

The Docker context admits only the `runtime-py312` wheels. After validating
the signed lane on the host, stage that exact lane under
`wheelhouse/runtime-py312` without committing it, then disable networking:

```bash
install -d -m 0700 wheelhouse
cp -a "$OUTPUT_ROOT/runtime-py312" wheelhouse/runtime-py312
docker build --network=none --pull=false -t quant-swarm-xau:rc2 .
```

The image pins the CPython 3.12.13 amd64 base digest, installs only from the
runtime wheel directory, removes noncontract bootstrap packages, runs
`pip check`, and verifies the exact application plus `pip==25.1.1` closure.

Delete the staged `wheelhouse/` after the build. It is ignored by Git and must
not enter a source commit or release package. A successful dependency gate
establishes reproducible inputs; it does not establish trading profitability
or authorize real-capital operation.
