# Deployment contract

This remediation build exposes only `CHECK`, `SHADOW`, and `DEMO`. Every
operational path runs `python -m python_swarm.runtime_app`; the shell wrapper
does not launch simulations, dashboards, standalone IPC bridges, retired
orchestrators, or a real-account mode.

## Developer preflight

Verify the signed CPython 3.12 wheelhouse, set `QSX_WHEELHOUSE`, and run
`bash server_setup.sh`. The setup preflight explicitly selects
`config.check.example.json`; `deployment/start.sh` itself never guesses a
configuration. A writable checkout is deliberately rejected by
broker-connected modes and must not be “fixed” by changing its permissions.
Build and validate each isolated dependency lane using
[the offline dependency procedure](../docs/DEPENDENCY_REPRODUCIBILITY.md).

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

For the RC2 EA, set `InpSessionMode=QSX_SESSION_SHADOW` and keep
`InpEmergencyStop=true`. Verify UTC, perform the one-time
`InpApproveRiskDayHighWaterUtcDay` migration for the exact broker/account,
record successful read-back, clear the approval input, and restart. SHADOW
must authenticate with MT5 AutoTrading off and remain locally non-mutating.
After a legitimate multi-day shutdown, use
`InpApproveRiskDayForwardJumpUtcDay` only on a cold restart with the exact
independently verified current UTC day. The resulting baseline remains
unverified and needs the separate day/equity approval before DEMO readiness.
Clear every approval input after verified read-back.

## DEMO promotion

`--demo` is an explicit promotion. Use a protected configuration derived from
`config.demo.example.json` only after every canonical demo gate passes. Keep
the account pinned to `DEMO`; any material code, configuration, risk,
protocol, lifecycle or persistence change resets the forward-test clock.
DEMO requires `InpSessionMode=QSX_SESSION_DEMO` and
`InpEmergencyStop=false`; the gateway will reject DEMO while the emergency
entry fence or any required trading permission remains active.
Follow [DEMO_FORWARD_TEST_RUNBOOK.md](DEMO_FORWARD_TEST_RUNBOOK.md): it requires
a freshly compiled EX5 bound into the frozen manifest, daily broker-versus-
ledger reconciliation, at least 30 trading days (preferably 60 calendar days),
and zero unexplained safety incidents. That observation period has not yet
been completed.

This release has no real-account launcher.
