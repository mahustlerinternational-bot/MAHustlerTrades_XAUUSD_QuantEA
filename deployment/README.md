# Deployment contract

This remediation build exposes only `CHECK`, `SHADOW`, and `DEMO`. Every
operational path runs `python -m python_swarm.runtime_app`; the shell wrapper
does not launch simulations, dashboards, standalone IPC bridges, retired
orchestrators, or a real-account mode.

## Developer preflight

Run `bash server_setup.sh`, then explicitly run
`bash deployment/start.sh --check`. The committed `config.json` is a
non-broker CHECK profile. A writable checkout is deliberately rejected by
broker-connected modes and must not be “fixed” by changing its permissions.

## Installed SHADOW layout

Create a separate release, rather than changing the developer workspace:

- `/opt/quant-swarm-xau`: root-owned and not writable by the `trader` service
  identity; the virtual environment and every Python module follow the same
  rule.
- `/etc/quant-swarm-xau/runtime.json`: a root-owned, service-readable copy of
  `config.shadow.example.json`, with an exact demo account/server identity.
- `/etc/quant-swarm-xau/quant-swarm.env`: root-owned service environment data;
  keep credentials out of the JSON and command line.
- `/var/lib/quant-swarm-xau`: mode `0700`, owned by `trader`, and selected by
  `QSX_DATA_DIR`.

Set `QSX_CONFIG` to the protected runtime JSON. The supplied systemd unit pins
`SHADOW`, uses the canonical module directly, and grants write access only to
the runtime data/runtime directories. The runtime validates the release,
configuration, interpreter and Python import paths before it creates state or
opens a broker socket.

## DEMO promotion

`--demo` is an explicit promotion. Use a protected configuration derived from
`config.demo.example.json` only after every canonical demo gate passes. Keep
the account pinned to `DEMO`; any material code, configuration, risk,
protocol, lifecycle or persistence change resets the forward-test clock.
Follow [DEMO_FORWARD_TEST_RUNBOOK.md](DEMO_FORWARD_TEST_RUNBOOK.md): it requires
a freshly compiled EX5 bound into the frozen manifest, daily broker-versus-
ledger reconciliation, at least 30 trading days (preferably 60 calendar days),
and zero unexplained safety incidents. That observation period has not yet
been completed.

This release has no real-account launcher.
