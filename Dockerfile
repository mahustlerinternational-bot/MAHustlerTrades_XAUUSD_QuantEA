# Allowlisted Python control-plane image. When MT5 runs under Wine on the host,
# use host networking and retain the loopback-only IPC listener.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONNOUSERSITE=1 \
    PIP_NO_CACHE_DIR=1 \
    QSX_MODE=check \
    QSX_CONFIG=/opt/quant-swarm-xau/deployment/config.json \
    QSX_DATA_DIR=/var/lib/quant-swarm-xau \
    QSX_PYTHON_BIN=/usr/local/bin/python3

WORKDIR /opt/quant-swarm-xau

COPY python_swarm/requirements.lock ./python_swarm/requirements.lock
RUN python3 -m pip install --upgrade pip \
    && python3 -m pip install --require-hashes -r python_swarm/requirements.lock

# Never copy the repository root: it may contain archives, local environments,
# recovery data, databases, logs, or operator credential notes.
COPY --chmod=0444 python_swarm/*.py ./python_swarm/
COPY --chmod=0444 deployment/config.json ./deployment/config.json
COPY --chmod=0755 deployment/start.sh ./deployment/start.sh

RUN groupadd --system trader \
    && useradd --system --gid trader --home-dir /nonexistent --shell /usr/sbin/nologin trader \
    && install -d -o trader -g trader -m 0700 /var/lib/quant-swarm-xau

USER trader
WORKDIR /opt/quant-swarm-xau

VOLUME ["/var/lib/quant-swarm-xau"]

CMD ["python3", "-m", "python_swarm.runtime_app", "--mode", "check"]
