"""Fail-closed control-plane state for the canonical swarm runtime.

The legacy orchestrators infer readiness from a socket being present.  This
module deliberately makes readiness an evidence-backed state transition.  It
does no I/O at import (or at runtime); callers inject a clock and acquire the
account/symbol/magic ownership lock outside this boundary.
"""

from __future__ import annotations

from dataclasses import dataclass, fields, replace
from enum import StrEnum
from typing import Protocol, runtime_checkable


class ControlPlaneState(StrEnum):
    STOPPED = "STOPPED"
    BOOTSTRAPPING = "BOOTSTRAPPING"
    WAITING_EA = "WAITING_EA"
    SYNCHRONIZING = "SYNCHRONIZING"
    READY = "READY"
    DEGRADED = "DEGRADED"
    HALTED = "HALTED"
    DISCONNECTED = "DISCONNECTED"


class BrokerTradeMode(StrEnum):
    DEMO = "DEMO"
    REAL = "REAL"
    CONTEST = "CONTEST"


class RuntimeStateError(RuntimeError):
    """Base error for an illegal or unsafe control-plane operation."""


class IllegalTransitionError(RuntimeStateError):
    """The requested state transition is not part of the canonical graph."""


class RuntimeNotReadyError(RuntimeStateError):
    """Execution was attempted without complete readiness evidence."""


@runtime_checkable
class RuntimeClock(Protocol):
    def now_ms(self) -> int:
        """Return a UTC Unix timestamp in milliseconds."""


@dataclass(frozen=True, slots=True)
class ExpectedBrokerIdentity:
    """Pinned identity the runtime is authorised to control.

    ``allow_real_account`` has no permissive default.  A deployment that ever
    enables it must still pin the exact account, server, symbol, and magic.
    """

    account_id: str
    broker_server: str
    symbol: str
    magic: int
    allow_real_account: bool = False

    def __post_init__(self) -> None:
        for name in ("account_id", "broker_server", "symbol"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty pinned value")
        if not isinstance(self.magic, int) or isinstance(self.magic, bool) or self.magic <= 0:
            raise ValueError("magic must be a positive integer")


@dataclass(frozen=True, slots=True)
class BrokerIdentity:
    account_id: str
    broker_server: str
    trade_mode: BrokerTradeMode
    symbol: str
    magic: int
    connection_epoch: str
    trade_allowed: bool
    expert_allowed: bool

    def __post_init__(self) -> None:
        for name in ("account_id", "broker_server", "symbol", "connection_epoch"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be non-empty")
        if not isinstance(self.magic, int) or isinstance(self.magic, bool) or self.magic <= 0:
            raise ValueError("magic must be a positive integer")

    @property
    def ownership_key(self) -> tuple[str, str, str, int]:
        return (self.account_id, self.broker_server, self.symbol, self.magic)


def identity_mismatches(
    expected: ExpectedBrokerIdentity,
    actual: BrokerIdentity,
    *,
    require_execution_permissions: bool = True,
) -> tuple[str, ...]:
    """Return every blocking identity discrepancy, never only the first."""

    if not isinstance(require_execution_permissions, bool):
        raise TypeError("require_execution_permissions must be boolean")

    mismatches: list[str] = []
    for name in ("account_id", "broker_server", "symbol", "magic"):
        if getattr(expected, name) != getattr(actual, name):
            mismatches.append(
                f"{name}: expected {getattr(expected, name)!r}, got {getattr(actual, name)!r}"
            )
    if actual.trade_mode is BrokerTradeMode.REAL and not expected.allow_real_account:
        mismatches.append("real account is not explicitly authorised")
    if actual.trade_mode is BrokerTradeMode.CONTEST:
        mismatches.append("contest accounts are not authorised")
    if require_execution_permissions:
        if not actual.trade_allowed:
            mismatches.append("broker reports trading disabled")
        if not actual.expert_allowed:
            mismatches.append("broker reports expert trading disabled")
    return tuple(mismatches)


@dataclass(frozen=True, slots=True)
class ReadinessEvidence:
    ledger_healthy: bool = False
    ea_connected: bool = False
    transport_loopback: bool = False
    connection_epoch: str | None = None
    identity_verified: bool = False
    ownership_key: tuple[str, str, str, int] | None = None
    tuple_lock_acquired: bool = False
    daily_baseline_verified: bool = False
    daily_risk_halted: bool = True
    snapshot_id: str | None = None
    reconciliation_run_id: str | None = None
    reconciliation_succeeded: bool = False
    deal_cursor_initialized: bool = False
    blocking_diffs: tuple[str, ...] = ()
    provider_gate_safe: bool = False

    def blockers(self) -> tuple[str, ...]:
        result: list[str] = []
        boolean_requirements = (
            ("ledger_healthy", self.ledger_healthy),
            ("ea_connected", self.ea_connected),
            ("transport_loopback", self.transport_loopback),
            ("identity_verified", self.identity_verified),
            ("tuple_lock_acquired", self.tuple_lock_acquired),
            ("daily_baseline_verified", self.daily_baseline_verified),
            ("reconciliation_succeeded", self.reconciliation_succeeded),
            ("deal_cursor_initialized", self.deal_cursor_initialized),
            ("provider_gate_safe", self.provider_gate_safe),
        )
        result.extend(name for name, value in boolean_requirements if not value)
        if self.daily_risk_halted:
            result.append("daily_risk_halted")
        for name, value in (
            ("connection_epoch", self.connection_epoch),
            ("ownership_key", self.ownership_key),
            ("snapshot_id", self.snapshot_id),
            ("reconciliation_run_id", self.reconciliation_run_id),
        ):
            if value is None:
                result.append(name)
        result.extend(f"blocking_diff:{item}" for item in self.blocking_diffs)
        return tuple(result)

    def protection_blockers(self) -> tuple[str, ...]:
        """Blockers for managing existing exposure, independent of entry gates."""

        result: list[str] = []
        requirements = (
            ("ledger_healthy", self.ledger_healthy),
            ("ea_connected", self.ea_connected),
            ("transport_loopback", self.transport_loopback),
            ("identity_verified", self.identity_verified),
            ("tuple_lock_acquired", self.tuple_lock_acquired),
            ("reconciliation_succeeded", self.reconciliation_succeeded),
            ("deal_cursor_initialized", self.deal_cursor_initialized),
        )
        result.extend(name for name, value in requirements if not value)
        for name, value in (
            ("connection_epoch", self.connection_epoch),
            ("ownership_key", self.ownership_key),
            ("snapshot_id", self.snapshot_id),
            ("reconciliation_run_id", self.reconciliation_run_id),
        ):
            if value is None:
                result.append(name)
        result.extend(f"blocking_diff:{item}" for item in self.blocking_diffs)
        return tuple(result)


@dataclass(frozen=True, slots=True)
class StateTransition:
    from_state: ControlPlaneState
    to_state: ControlPlaneState
    occurred_at_ms: int
    reason: str


_LEGAL_TRANSITIONS: dict[ControlPlaneState, frozenset[ControlPlaneState]] = {
    ControlPlaneState.STOPPED: frozenset({ControlPlaneState.BOOTSTRAPPING}),
    ControlPlaneState.BOOTSTRAPPING: frozenset(
        {ControlPlaneState.WAITING_EA, ControlPlaneState.HALTED, ControlPlaneState.STOPPED}
    ),
    ControlPlaneState.WAITING_EA: frozenset(
        {
            ControlPlaneState.SYNCHRONIZING,
            ControlPlaneState.DISCONNECTED,
            ControlPlaneState.HALTED,
            ControlPlaneState.STOPPED,
        }
    ),
    ControlPlaneState.SYNCHRONIZING: frozenset(
        {
            ControlPlaneState.READY,
            ControlPlaneState.DEGRADED,
            ControlPlaneState.DISCONNECTED,
            ControlPlaneState.HALTED,
            ControlPlaneState.STOPPED,
        }
    ),
    ControlPlaneState.READY: frozenset(
        {
            ControlPlaneState.DEGRADED,
            ControlPlaneState.DISCONNECTED,
            ControlPlaneState.HALTED,
            ControlPlaneState.STOPPED,
        }
    ),
    ControlPlaneState.DEGRADED: frozenset(
        {
            ControlPlaneState.SYNCHRONIZING,
            ControlPlaneState.DISCONNECTED,
            ControlPlaneState.HALTED,
            ControlPlaneState.STOPPED,
        }
    ),
    ControlPlaneState.DISCONNECTED: frozenset(
        {
            ControlPlaneState.WAITING_EA,
            ControlPlaneState.SYNCHRONIZING,
            ControlPlaneState.HALTED,
            ControlPlaneState.STOPPED,
        }
    ),
    ControlPlaneState.HALTED: frozenset({ControlPlaneState.STOPPED}),
}


class RuntimeStateMachine:
    """Small synchronous state machine; external effects remain injected ports."""

    def __init__(self, clock: RuntimeClock):
        if not isinstance(clock, RuntimeClock):
            raise TypeError("clock must implement now_ms()")
        self._clock = clock
        self._state = ControlPlaneState.STOPPED
        self._evidence = ReadinessEvidence()
        self._transitions: list[StateTransition] = []
        self._halt_reason: str | None = None

    @property
    def state(self) -> ControlPlaneState:
        return self._state

    @property
    def evidence(self) -> ReadinessEvidence:
        return self._evidence

    @property
    def transitions(self) -> tuple[StateTransition, ...]:
        return tuple(self._transitions)

    @property
    def halt_reason(self) -> str | None:
        return self._halt_reason

    @property
    def is_ready(self) -> bool:
        return self._state is ControlPlaneState.READY and not self._evidence.blockers()

    def update_evidence(self, **changes: object) -> ReadinessEvidence:
        known = {item.name for item in fields(ReadinessEvidence)}
        unknown = set(changes) - known
        if unknown:
            raise ValueError(f"unknown readiness evidence: {sorted(unknown)}")
        if "blocking_diffs" in changes:
            changes["blocking_diffs"] = tuple(changes["blocking_diffs"])  # type: ignore[arg-type]
        updated = replace(self._evidence, **changes)
        if updated.tuple_lock_acquired and updated.ownership_key is None:
            raise ValueError("tuple lock evidence requires an ownership key")
        if updated.identity_verified and updated.ownership_key is None:
            raise ValueError("identity evidence requires an ownership key")
        self._evidence = updated
        if self._state is ControlPlaneState.READY and updated.blockers():
            self.transition(ControlPlaneState.DEGRADED, "readiness evidence was invalidated")
        return updated

    def transition(self, target: ControlPlaneState, reason: str) -> bool:
        if not isinstance(target, ControlPlaneState):
            raise TypeError("target must be a ControlPlaneState")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("transition reason must be non-empty")
        if target is self._state:
            return False
        if target not in _LEGAL_TRANSITIONS[self._state]:
            raise IllegalTransitionError(
                f"illegal control-plane transition {self._state.value}->{target.value}"
            )
        if target is ControlPlaneState.READY:
            blockers = self._evidence.blockers()
            if blockers:
                raise RuntimeNotReadyError(
                    "READY requires complete evidence: " + ", ".join(blockers)
                )
        previous = self._state
        self._state = target
        self._transitions.append(
            StateTransition(previous, target, self._clock.now_ms(), reason.strip())
        )
        return True

    def start_bootstrap(self) -> None:
        self._halt_reason = None
        self._evidence = ReadinessEvidence()
        self.transition(ControlPlaneState.BOOTSTRAPPING, "runtime bootstrap started")

    def wait_for_ea(self, *, ledger_healthy: bool, provider_gate_safe: bool) -> None:
        self.update_evidence(
            ledger_healthy=ledger_healthy, provider_gate_safe=provider_gate_safe
        )
        if not ledger_healthy:
            self.halt("authoritative ledger is unhealthy")
            return
        self.transition(ControlPlaneState.WAITING_EA, "bootstrap checks completed")

    def accept_connection(
        self,
        expected: ExpectedBrokerIdentity,
        actual: BrokerIdentity,
        *,
        transport_loopback: bool,
        tuple_lock_acquired: bool,
        require_execution_permissions: bool = True,
    ) -> None:
        if self._state not in {ControlPlaneState.WAITING_EA, ControlPlaneState.DISCONNECTED}:
            raise IllegalTransitionError("connections are accepted only while waiting/disconnected")
        mismatches = identity_mismatches(
            expected,
            actual,
            require_execution_permissions=require_execution_permissions,
        )
        ownership_key = actual.ownership_key
        self.update_evidence(
            ea_connected=True,
            transport_loopback=transport_loopback,
            connection_epoch=actual.connection_epoch,
            identity_verified=not mismatches,
            ownership_key=ownership_key,
            tuple_lock_acquired=tuple_lock_acquired,
            snapshot_id=None,
            reconciliation_run_id=None,
            reconciliation_succeeded=False,
            deal_cursor_initialized=False,
            blocking_diffs=mismatches,
        )
        if not transport_loopback:
            self.halt("non-loopback broker transport is forbidden")
        elif mismatches:
            self.halt("broker identity mismatch: " + "; ".join(mismatches))
        elif not tuple_lock_acquired:
            self.halt("account/server/symbol/magic ownership lock was not acquired")
        else:
            self.transition(ControlPlaneState.SYNCHRONIZING, "broker identity verified")

    def mark_synchronized(
        self,
        *,
        snapshot_id: str,
        reconciliation_run_id: str,
        deal_cursor_initialized: bool,
        daily_baseline_verified: bool,
        daily_risk_halted: bool,
        blocking_diffs: tuple[str, ...] = (),
    ) -> None:
        self.update_evidence(
            snapshot_id=_required_text(snapshot_id, "snapshot_id"),
            reconciliation_run_id=_required_text(
                reconciliation_run_id, "reconciliation_run_id"
            ),
            reconciliation_succeeded=not blocking_diffs,
            deal_cursor_initialized=deal_cursor_initialized,
            daily_baseline_verified=daily_baseline_verified,
            daily_risk_halted=daily_risk_halted,
            blocking_diffs=blocking_diffs,
        )
        blockers = self._evidence.blockers()
        if blocking_diffs:
            self.halt("blocking reconciliation differences: " + "; ".join(blocking_diffs))
        elif blockers:
            if self._state is ControlPlaneState.SYNCHRONIZING:
                self.transition(
                    ControlPlaneState.DEGRADED,
                    "synchronization evidence incomplete: " + ", ".join(blockers),
                )
        else:
            if self._state is ControlPlaneState.DEGRADED:
                self.transition(ControlPlaneState.SYNCHRONIZING, "resynchronization completed")
            self.transition(ControlPlaneState.READY, "all readiness evidence verified")

    def disconnect(self, reason: str) -> None:
        self.update_evidence(
            ea_connected=False,
            connection_epoch=None,
            identity_verified=False,
            tuple_lock_acquired=False,
            snapshot_id=None,
            reconciliation_run_id=None,
            reconciliation_succeeded=False,
            deal_cursor_initialized=False,
        )
        if self._state in {
            ControlPlaneState.WAITING_EA,
            ControlPlaneState.SYNCHRONIZING,
            ControlPlaneState.READY,
            ControlPlaneState.DEGRADED,
        }:
            self.transition(ControlPlaneState.DISCONNECTED, reason)

    def halt(self, reason: str) -> None:
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("halt reason must be non-empty")
        self._halt_reason = reason.strip()
        if self._state is ControlPlaneState.HALTED:
            return
        if self._state is ControlPlaneState.STOPPED:
            # A ledger failure can occur before bootstrap.  Record a proper
            # transition rather than silently pretending the runtime ran.
            self.transition(ControlPlaneState.BOOTSTRAPPING, "fail-closed bootstrap")
        self.transition(ControlPlaneState.HALTED, self._halt_reason)

    def stop(self, reason: str = "runtime stopped") -> None:
        if self._state is ControlPlaneState.STOPPED:
            return
        self.transition(ControlPlaneState.STOPPED, reason)
        self._evidence = ReadinessEvidence()

    def require_ready(self, operation: str = "execution") -> None:
        blockers = self._evidence.blockers()
        if self._state is not ControlPlaneState.READY or blockers:
            suffix = f"; blockers={','.join(blockers)}" if blockers else ""
            raise RuntimeNotReadyError(
                f"{operation} requires READY, current={self._state.value}{suffix}"
            )

    def require_protection_ready(self, operation: str = "position protection") -> None:
        """Allow existing-position protection through entry-only degradation.

        Provider outages, an unverified daily baseline and a drawdown halt block
        new risk, but must not disable partial exits, breakeven, trailing or a
        protective close.  Identity, transport, reconciliation and ledger proof
        remain mandatory.
        """

        blockers = self._evidence.protection_blockers()
        if self._state not in {ControlPlaneState.READY, ControlPlaneState.DEGRADED} or blockers:
            suffix = f"; blockers={','.join(blockers)}" if blockers else ""
            raise RuntimeNotReadyError(
                f"{operation} requires protective readiness, "
                f"current={self._state.value}{suffix}"
            )


def _required_text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty")
    return value.strip()


__all__ = [
    "BrokerIdentity",
    "BrokerTradeMode",
    "ControlPlaneState",
    "ExpectedBrokerIdentity",
    "IllegalTransitionError",
    "ReadinessEvidence",
    "RuntimeClock",
    "RuntimeNotReadyError",
    "RuntimeStateError",
    "RuntimeStateMachine",
    "StateTransition",
    "identity_mismatches",
]
