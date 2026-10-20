# CPython 3.12.13 amd64 manifest from the official Python image index.
FROM python:3.12.13-slim-bookworm@sha256:72d3d75f2639ab82b34b29390ad3d6e0827c775befee94edda8e9976818f488d

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONNOUSERSITE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    QSX_MODE=check \
    QSX_CONFIG=/etc/quant-swarm-xau/runtime.json \
    QSX_DATA_DIR=/var/lib/quant-swarm-xau \
    QSX_PYTHON_BIN=/usr/local/bin/python3

WORKDIR /opt/quant-swarm-xau

# The build context contains only the separately signed and host-validated
# runtime-py312 lane. Test lanes are never Docker build inputs. Build with
# network disabled after validating WHEELHOUSE.json and its OpenSSH signature.
COPY --chmod=0444 wheelhouse/runtime-py312/wheels/ /wheelhouse/
COPY --chmod=0444 \
    python_swarm/requirements-runtime-py312.lock \
    ./python_swarm/requirements-runtime-py312.lock
COPY --chmod=0444 tools/assert_installed_closure.py /tmp/assert_installed_closure.py
RUN python3 -c \
      'import importlib.metadata,platform,sys; raise SystemExit(0 if sys.implementation.name == "cpython" and sys.version_info[:3] == (3,12,13) and platform.machine() == "x86_64" and importlib.metadata.version("pip") == "25.1.1" else 1)' \
    && python3 -m pip uninstall -y setuptools wheel >/dev/null \
    && python3 -m pip install \
      --no-index \
      --find-links=/wheelhouse \
      --only-binary=:all: \
      --no-compile \
      --require-hashes \
      -r python_swarm/requirements-runtime-py312.lock \
    && python3 -m pip check \
    && python3 /tmp/assert_installed_closure.py \
      --lock python_swarm/requirements-runtime-py312.lock \
      --implementation cpython \
      --python-version 3.12.13 \
      --machine x86_64 \
      --allow-bootstrap pip==25.1.1 \
    && rm -rf /wheelhouse /tmp/assert_installed_closure.py

# This is the exact canonical runtime closure. Wildcards and repository-root
# copies are forbidden because the source tree also contains retired modules.
COPY --chmod=0444 \
    python_swarm/__init__.py \
    python_swarm/agent_roster.py \
    python_swarm/broker_gateway.py \
    python_swarm/broker_protocol.py \
    python_swarm/decision_service.py \
    python_swarm/domain_models.py \
    python_swarm/execution_service.py \
    python_swarm/integration_dispatcher.py \
    python_swarm/learning_policy.py \
    python_swarm/learning_service.py \
    python_swarm/lifecycle_service.py \
    python_swarm/market_snapshot_service.py \
    python_swarm/provider_adapters.py \
    python_swarm/reconciliation.py \
    python_swarm/runtime_app.py \
    python_swarm/runtime_config.py \
    python_swarm/runtime_paths.py \
    python_swarm/runtime_scheduler.py \
    python_swarm/runtime_state.py \
    python_swarm/setup_check.py \
    python_swarm/strategy_pipeline.py \
    python_swarm/trade_ledger.py \
    python_swarm/trading_policies.py \
    python_swarm/version.py \
    ./python_swarm/
COPY --chmod=0555 deployment/start.sh ./deployment/start.sh

RUN groupadd --system trader \
    && useradd --system --gid trader --home-dir /nonexistent --shell /usr/sbin/nologin trader \
    && install -d -o root -g trader -m 0750 /etc/quant-swarm-xau \
    && install -d -o trader -g trader -m 0700 /var/lib/quant-swarm-xau

USER trader
WORKDIR /opt/quant-swarm-xau

VOLUME ["/var/lib/quant-swarm-xau"]

# An external read-only QSX_CONFIG mount is mandatory, including in CHECK.
CMD ["./deployment/start.sh", "--check"]
