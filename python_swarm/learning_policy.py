"""Pure, governed learning policy for canonical Agent 20 proposals.

This module consumes only immutable, broker-confirmed closed-trade outcomes
whose actual R was already derived by the durable ledger from broker deals.  It
does not read the ledger itself and performs no filesystem, database, network,
environment, random-number, or wall-clock access.

Learning is intentionally narrow: a proposal can supply positive, bounded
weights to the canonical fusion policy.  It cannot enable agents or providers,
change quorum or confluence rules, alter entry geometry, reserve risk, issue a
broker command, or mutate runtime readiness.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_EVEN
from enum import StrEnum
from types import MappingProxyType
from typing import Mapping, Sequence

try:
    from .agent_roster import AGENT_BY_ID, STRATEGY_AGENT_IDS
    from .strategy_pipeline import CanonicalRegime, MarketSession
except ImportError:  # pragma: no cover - legacy flat-import compatibility
    from agent_roster import AGENT_BY_ID, STRATEGY_AGENT_IDS  # type: ignore
    from strategy_pipeline import CanonicalRegime, MarketSession  # type: ignore


LEARNING_SCHEMA_VERSION = 1
CANONICAL_AGENT_IDS = tuple(range(4, 14))
if tuple(STRATEGY_AGENT_IDS) != CANONICAL_AGENT_IDS:  # pragma: no cover
    raise RuntimeError("learning policy requires canonical strategy Agents 04-13")

ZERO = Decimal("0")
ONE = Decimal("1")
DOCUMENTED_MINIMUM_WEIGHT = Decimal("0.40")
DOCUMENTED_MAXIMUM_WEIGHT = Decimal("1.40")
MAXIMUM_CONSERVATIVE_ADJUSTMENT = Decimal("0.25")


class LearningPolicyError(ValueError):
    """Base class for rejected learning input or activation."""


class LearningInputError(LearningPolicyError):
    """An outcome batch is not eligible authoritative learning evidence."""


class LearningDuplicateError(LearningInputError):
    """An identical trade or close event appeared more than once."""


class LearningConflictError(LearningInputError):
    """A durable trade/event identity was reused with different content."""


class LearningActivationError(LearningPolicyError):
    """A proposal was not explicitly approved or its context is malformed."""


def _text(value: str, field_name: str, *, maximum: int = 160) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LearningInputError(f"{field_name} must be a non-empty string")
    normalized = value.strip()
    if len(normalized) > maximum:
        raise LearningInputError(f"{field_name} exceeds {maximum} characters")
    return normalized


def _integer(value: object, field_name: str, *, minimum: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise LearningInputError(f"{field_name} must be an integer >= {minimum}")
    return value


def _decimal(value: object, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise LearningInputError(f"{field_name} cannot be boolean")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (ValueError, TypeError, ArithmeticError) as exc:
        raise LearningInputError(f"{field_name} must be a finite decimal") from exc
    if not result.is_finite():
        raise LearningInputError(f"{field_name} must be a finite decimal")
    return result


def _decimal_text(value: Decimal) -> str:
    if value == ZERO:
        return "0"
    return str(value.normalize())


def _hash_text(value: str, field_name: str) -> str:
    normalized = _text(value, field_name, maximum=128).lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise LearningInputError(f"{field_name} must be a 64-character SHA-256 hash")
    return normalized


def _clamp(value: Decimal, low: Decimal, high: Decimal) -> Decimal:
    return max(low, min(high, value))


class OutcomeSource(StrEnum):
    BROKER_LEDGER = "BROKER_LEDGER"
    SYNTHETIC = "SYNTHETIC"
    RECOVERED = "RECOVERED"
    UNCONFIRMED = "UNCONFIRMED"


@dataclass(frozen=True, slots=True)
class StrategyOutcomeAttribution:
    """Entry-time attribution for one actual fusion contributor."""

    agent_id: int
    signal_id: str
    strategy_slug: str
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.agent_id not in CANONICAL_AGENT_IDS:
            raise LearningInputError("learning attribution agent must be 04 through 13")
        object.__setattr__(self, "signal_id", _text(self.signal_id, "signal_id", maximum=128))
        slug = _text(self.strategy_slug, "strategy_slug", maximum=80)
        if slug != AGENT_BY_ID[self.agent_id].slug:
            raise LearningInputError(
                f"strategy_slug does not match canonical Agent {self.agent_id:02d}"
            )
        object.__setattr__(self, "strategy_slug", slug)
        frozen_tags = tuple(_text(tag, "attribution tag", maximum=120) for tag in self.tags)
        if len(frozen_tags) != len(set(frozen_tags)):
            raise LearningInputError("attribution tags cannot contain duplicates")
        object.__setattr__(self, "tags", frozen_tags)


@dataclass(frozen=True, slots=True)
class AuthoritativeClosedOutcome:
    """A closed-trade learning input assembled from one durable ledger result."""

    trade_id: str
    close_event_id: str
    account_id: str
    symbol: str
    magic: int
    actual_r: Decimal
    attributions: tuple[StrategyOutcomeAttribution, ...]
    regime: CanonicalRegime
    session: MarketSession
    broker_opened_at_ms: int
    broker_closed_at_ms: int
    config_hash: str
    source: OutcomeSource = OutcomeSource.BROKER_LEDGER
    authoritative: bool = True
    broker_confirmed: bool = True
    deal_derived_actual_r: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "trade_id", _text(self.trade_id, "trade_id", maximum=128))
        object.__setattr__(
            self,
            "close_event_id",
            _text(self.close_event_id, "close_event_id", maximum=128),
        )
        object.__setattr__(self, "account_id", _text(self.account_id, "account_id", maximum=80))
        object.__setattr__(self, "symbol", _text(self.symbol, "symbol", maximum=32))
        _integer(self.magic, "magic")
        object.__setattr__(self, "actual_r", _decimal(self.actual_r, "actual_r"))
        object.__setattr__(self, "regime", CanonicalRegime(self.regime))
        object.__setattr__(self, "session", MarketSession(self.session))
        object.__setattr__(self, "source", OutcomeSource(self.source))
        _integer(self.broker_opened_at_ms, "broker_opened_at_ms")
        _integer(self.broker_closed_at_ms, "broker_closed_at_ms")
        if self.broker_closed_at_ms <= self.broker_opened_at_ms:
            raise LearningInputError("broker close timestamp must follow broker open timestamp")
        object.__setattr__(self, "config_hash", _text(self.config_hash, "config_hash", maximum=128))
        for name in ("authoritative", "broker_confirmed", "deal_derived_actual_r"):
            if not isinstance(getattr(self, name), bool):
                raise LearningInputError(f"{name} must be boolean")

        frozen = tuple(self.attributions)
        if len(frozen) < 3:
            raise LearningInputError("closed trade must retain at least three actual contributors")
        if any(not isinstance(item, StrategyOutcomeAttribution) for item in frozen):
            raise LearningInputError("attributions must be StrategyOutcomeAttribution objects")
        agent_ids = tuple(item.agent_id for item in frozen)
        signal_ids = tuple(item.signal_id for item in frozen)
        if len(agent_ids) != len(set(agent_ids)):
            raise LearningInputError("one trade cannot credit the same agent more than once")
        if len(signal_ids) != len(set(signal_ids)):
            raise LearningInputError("one trade cannot reuse a contributor signal ID")
        object.__setattr__(self, "attributions", tuple(sorted(frozen, key=lambda item: item.agent_id)))


@dataclass(frozen=True, slots=True)
class LearningPolicyConfig:
    """Immutable conservative bounds for one policy generation."""

    minimum_samples: int = 5
    maximum_samples_per_context: int = 100
    neutral_weight: Decimal = Decimal("1.0")
    minimum_weight: Decimal = Decimal("0.75")
    maximum_weight: Decimal = Decimal("1.25")
    maximum_adjustment: Decimal = Decimal("0.20")
    actual_r_clip: Decimal = Decimal("3.0")
    weight_quantum: Decimal = Decimal("0.0001")
    maximum_batch_outcomes: int = 10_000

    def __post_init__(self) -> None:
        _integer(self.minimum_samples, "minimum_samples", minimum=5)
        _integer(
            self.maximum_samples_per_context,
            "maximum_samples_per_context",
            minimum=self.minimum_samples,
        )
        _integer(
            self.maximum_batch_outcomes,
            "maximum_batch_outcomes",
            minimum=self.minimum_samples,
        )
        for name in (
            "neutral_weight",
            "minimum_weight",
            "maximum_weight",
            "maximum_adjustment",
            "actual_r_clip",
            "weight_quantum",
        ):
            object.__setattr__(self, name, _decimal(getattr(self, name), name))
        if not ZERO < self.minimum_weight <= self.neutral_weight <= self.maximum_weight:
            raise LearningInputError("learning weights must be positive and ordered")
        if self.neutral_weight != ONE:
            raise LearningInputError("inactive learning weight must remain exactly 1.0")
        if (
            self.minimum_weight < DOCUMENTED_MINIMUM_WEIGHT
            or self.maximum_weight > DOCUMENTED_MAXIMUM_WEIGHT
        ):
            raise LearningInputError("learning bounds exceed documented 0.40-1.40 limits")
        if self.maximum_adjustment <= 0 or self.actual_r_clip <= 0 or self.weight_quantum <= 0:
            raise LearningInputError("adjustment, R clip, and weight quantum must be positive")
        if self.maximum_adjustment > MAXIMUM_CONSERVATIVE_ADJUSTMENT:
            raise LearningInputError("maximum_adjustment exceeds conservative 0.25 limit")
        available_down = self.neutral_weight - self.minimum_weight
        available_up = self.maximum_weight - self.neutral_weight
        if self.maximum_adjustment > min(available_down, available_up):
            raise LearningInputError("maximum_adjustment exceeds configured weight bounds")
        quantum_tuple = self.weight_quantum.normalize().as_tuple()
        if (
            quantum_tuple.digits != (1,)
            or quantum_tuple.exponent > -2
            or any(
                value % self.weight_quantum
                for value in (self.minimum_weight, self.neutral_weight, self.maximum_weight)
            )
        ):
            raise LearningInputError(
                "weight_quantum must be a <=0.01 power-of-ten aligned with all bounds"
            )


@dataclass(frozen=True, slots=True)
class LearningContext:
    agent_id: int
    regime: CanonicalRegime
    session: MarketSession

    def __post_init__(self) -> None:
        if self.agent_id not in CANONICAL_AGENT_IDS:
            raise LearningInputError("learning context agent must be 04 through 13")
        object.__setattr__(self, "regime", CanonicalRegime(self.regime))
        object.__setattr__(self, "session", MarketSession(self.session))

    @property
    def sort_key(self) -> tuple[int, str, str]:
        return self.agent_id, self.regime.value, self.session.value


@dataclass(frozen=True, slots=True)
class ContextLearningStat:
    context: LearningContext
    sample_count: int
    used_sample_count: int
    win_count: int
    loss_count: int
    flat_count: int
    clipped_r_sum: Decimal
    clipped_r_mean: Decimal
    payoff_balance: Decimal
    outcome_balance: Decimal
    score: Decimal
    proposed_weight: Decimal
    activated: bool

    def __post_init__(self) -> None:
        for name in (
            "sample_count",
            "used_sample_count",
            "win_count",
            "loss_count",
            "flat_count",
        ):
            _integer(getattr(self, name), name, minimum=0)
        if self.used_sample_count > self.sample_count:
            raise LearningInputError("used sample count cannot exceed total samples")
        if self.win_count + self.loss_count + self.flat_count != self.used_sample_count:
            raise LearningInputError("context outcome counts do not add up")
        for name in (
            "clipped_r_sum",
            "clipped_r_mean",
            "payoff_balance",
            "outcome_balance",
            "score",
            "proposed_weight",
        ):
            object.__setattr__(self, name, _decimal(getattr(self, name), name))
        if self.proposed_weight <= 0:
            raise LearningInputError("proposed learning weight must remain positive")
        if not DOCUMENTED_MINIMUM_WEIGHT <= self.proposed_weight <= DOCUMENTED_MAXIMUM_WEIGHT:
            raise LearningInputError("proposed weight exceeds documented hard bounds")
        if not isinstance(self.activated, bool):
            raise LearningInputError("context activated flag must be boolean")
        if not -ONE <= self.payoff_balance <= ONE:
            raise LearningInputError("payoff balance must be bounded")
        if not -ONE <= self.outcome_balance <= ONE or not -ONE <= self.score <= ONE:
            raise LearningInputError("learning balances and score must be bounded")


@dataclass(frozen=True, slots=True)
class AgentSampleSummary:
    agent_id: int
    total_sample_count: int
    context_count: int
    activated_context_count: int

    def __post_init__(self) -> None:
        if self.agent_id not in CANONICAL_AGENT_IDS:
            raise LearningInputError("agent summary must belong to Agent 04 through 13")
        for name in ("total_sample_count", "context_count", "activated_context_count"):
            _integer(getattr(self, name), name, minimum=0)
        if self.activated_context_count > self.context_count:
            raise LearningInputError("activated context count cannot exceed context count")


@dataclass(frozen=True, slots=True)
class ActivatedAgentWeight:
    agent_id: int
    weight: Decimal
    sample_count: int
    learned: bool

    def __post_init__(self) -> None:
        if self.agent_id not in CANONICAL_AGENT_IDS:
            raise LearningActivationError("activated weight must belong to Agent 04 through 13")
        object.__setattr__(self, "weight", _decimal(self.weight, "activated weight"))
        _integer(self.sample_count, "sample_count", minimum=0)
        if not DOCUMENTED_MINIMUM_WEIGHT <= self.weight <= DOCUMENTED_MAXIMUM_WEIGHT:
            raise LearningActivationError("fusion weight exceeds documented hard bounds")
        if not isinstance(self.learned, bool):
            raise LearningActivationError("learned flag must be boolean")


@dataclass(frozen=True, slots=True)
class LearningActivation:
    """Approved, context-specific data that can only parameterize fusion weights."""

    proposal_hash: str
    proposal_version: int
    account_id: str
    symbol: str
    magic: int
    target_config_hash: str
    regime: CanonicalRegime
    session: MarketSession
    weights: tuple[ActivatedAgentWeight, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "proposal_hash", _hash_text(self.proposal_hash, "proposal_hash"))
        _integer(self.proposal_version, "proposal_version")
        object.__setattr__(self, "account_id", _text(self.account_id, "account_id", maximum=80))
        object.__setattr__(self, "symbol", _text(self.symbol, "symbol", maximum=32))
        _integer(self.magic, "magic")
        object.__setattr__(
            self,
            "target_config_hash",
            _text(self.target_config_hash, "target_config_hash", maximum=128),
        )
        object.__setattr__(self, "regime", CanonicalRegime(self.regime))
        object.__setattr__(self, "session", MarketSession(self.session))
        object.__setattr__(self, "weights", tuple(self.weights))
        if tuple(item.agent_id for item in self.weights) != CANONICAL_AGENT_IDS:
            raise LearningActivationError("activation must contain Agents 04 through 13 exactly once")

    @property
    def fusion_weights(self) -> Mapping[int, Decimal]:
        """Return the only runtime-facing payload: an immutable fusion weight map."""

        return MappingProxyType({item.agent_id: item.weight for item in self.weights})


@dataclass(frozen=True, slots=True)
class LearningProposal:
    schema_version: int
    proposal_version: int
    proposal_id: str
    proposal_hash: str
    parent_proposal_hash: str | None
    policy_config_hash: str
    account_id: str
    symbol: str
    magic: int
    source_config_hash: str
    target_config_hash: str
    source_trade_ids: tuple[str, ...]
    source_event_ids: tuple[str, ...]
    as_of_broker_time_ms: int
    context_stats: tuple[ContextLearningStat, ...]
    agent_summaries: tuple[AgentSampleSummary, ...]
    minimum_samples: int
    neutral_weight: Decimal
    minimum_weight: Decimal
    maximum_weight: Decimal

    def __post_init__(self) -> None:
        if self.schema_version != LEARNING_SCHEMA_VERSION:
            raise LearningInputError("unsupported learning proposal schema version")
        _integer(self.proposal_version, "proposal_version")
        object.__setattr__(self, "proposal_id", _text(self.proposal_id, "proposal_id", maximum=80))
        object.__setattr__(self, "proposal_hash", _hash_text(self.proposal_hash, "proposal_hash"))
        if self.parent_proposal_hash is not None:
            object.__setattr__(
                self,
                "parent_proposal_hash",
                _hash_text(self.parent_proposal_hash, "parent_proposal_hash"),
            )
        object.__setattr__(
            self,
            "policy_config_hash",
            _hash_text(self.policy_config_hash, "policy_config_hash"),
        )
        object.__setattr__(self, "account_id", _text(self.account_id, "account_id", maximum=80))
        object.__setattr__(self, "symbol", _text(self.symbol, "symbol", maximum=32))
        _integer(self.magic, "magic")
        for name in ("source_config_hash", "target_config_hash"):
            object.__setattr__(self, name, _text(getattr(self, name), name, maximum=128))
        object.__setattr__(self, "source_trade_ids", tuple(self.source_trade_ids))
        object.__setattr__(self, "source_event_ids", tuple(self.source_event_ids))
        if not self.source_trade_ids or not self.source_event_ids:
            raise LearningInputError("proposal lineage cannot be empty")
        if self.source_trade_ids != tuple(sorted(set(self.source_trade_ids))):
            raise LearningInputError("source trade lineage must be unique and sorted")
        if self.source_event_ids != tuple(sorted(set(self.source_event_ids))):
            raise LearningInputError("source event lineage must be unique and sorted")
        _integer(self.as_of_broker_time_ms, "as_of_broker_time_ms")
        object.__setattr__(self, "context_stats", tuple(self.context_stats))
        if tuple(item.context.sort_key for item in self.context_stats) != tuple(
            sorted(item.context.sort_key for item in self.context_stats)
        ):
            raise LearningInputError("context statistics must be deterministically sorted")
        object.__setattr__(self, "agent_summaries", tuple(self.agent_summaries))
        if tuple(item.agent_id for item in self.agent_summaries) != CANONICAL_AGENT_IDS:
            raise LearningInputError("proposal must summarize Agents 04 through 13")
        _integer(self.minimum_samples, "minimum_samples", minimum=5)
        for name in ("neutral_weight", "minimum_weight", "maximum_weight"):
            object.__setattr__(self, name, _decimal(getattr(self, name), name))
        if not ZERO < self.minimum_weight <= self.neutral_weight <= self.maximum_weight:
            raise LearningInputError("proposal learning bounds are invalid")
        if self.neutral_weight != ONE:
            raise LearningInputError("proposal neutral weight must remain exactly 1.0")
        if (
            self.minimum_weight < DOCUMENTED_MINIMUM_WEIGHT
            or self.maximum_weight > DOCUMENTED_MAXIMUM_WEIGHT
        ):
            raise LearningInputError("proposal exceeds documented learning bounds")
        for stat in self.context_stats:
            if not self.minimum_weight <= stat.proposed_weight <= self.maximum_weight:
                raise LearningInputError("context weight exceeds proposal bounds")
            if not stat.activated and stat.proposed_weight != self.neutral_weight:
                raise LearningInputError("inactive context must retain neutral weight")
            if stat.activated and stat.used_sample_count < self.minimum_samples:
                raise LearningInputError("context activated below the proposal sample threshold")

    def activate(
        self,
        regime: CanonicalRegime,
        session: MarketSession,
        *,
        approved_proposal_hash: str,
    ) -> LearningActivation:
        """Create an immutable exact-context fusion map after explicit approval."""

        approved = _hash_text(approved_proposal_hash, "approved_proposal_hash")
        if approved != self.proposal_hash:
            raise LearningActivationError("approved proposal hash does not match proposal")
        selected_regime = CanonicalRegime(regime)
        selected_session = MarketSession(session)
        by_context = {stat.context: stat for stat in self.context_stats}
        weights: list[ActivatedAgentWeight] = []
        for agent_id in CANONICAL_AGENT_IDS:
            stat = by_context.get(LearningContext(agent_id, selected_regime, selected_session))
            if stat is None or not stat.activated:
                weights.append(ActivatedAgentWeight(agent_id, self.neutral_weight, 0 if stat is None else stat.used_sample_count, False))
            else:
                weights.append(ActivatedAgentWeight(agent_id, stat.proposed_weight, stat.used_sample_count, True))
        return LearningActivation(
            proposal_hash=self.proposal_hash,
            proposal_version=self.proposal_version,
            account_id=self.account_id,
            symbol=self.symbol,
            magic=self.magic,
            target_config_hash=self.target_config_hash,
            regime=selected_regime,
            session=selected_session,
            weights=tuple(weights),
        )


class CanonicalLearningPolicy:
    """Deterministically turn authoritative outcomes into a governed proposal."""

    def __init__(self, config: LearningPolicyConfig | None = None) -> None:
        self.config = config or LearningPolicyConfig()

    def propose(
        self,
        outcomes: Sequence[AuthoritativeClosedOutcome],
        *,
        proposal_version: int,
        target_config_hash: str,
        parent_proposal_hash: str | None = None,
    ) -> LearningProposal:
        version = _integer(proposal_version, "proposal_version")
        target_hash = _text(target_config_hash, "target_config_hash", maximum=128)
        if version == 1 and parent_proposal_hash is not None:
            raise LearningInputError("first proposal cannot have a parent proposal hash")
        if version > 1 and parent_proposal_hash is None:
            raise LearningInputError("proposal versions after one require parent lineage")
        parent_hash = (
            None
            if parent_proposal_hash is None
            else _hash_text(parent_proposal_hash, "parent_proposal_hash")
        )

        frozen = tuple(outcomes)
        if not frozen:
            raise LearningInputError("learning proposal requires at least one outcome")
        if len(frozen) > self.config.maximum_batch_outcomes:
            raise LearningInputError("learning outcome batch exceeds configured maximum")
        if any(not isinstance(item, AuthoritativeClosedOutcome) for item in frozen):
            raise LearningInputError("all outcomes must be AuthoritativeClosedOutcome objects")
        self._validate_provenance(frozen)
        self._validate_identities(frozen)
        source_configs = {item.config_hash for item in frozen}
        if len(source_configs) != 1:
            raise LearningConflictError("learning batch spans multiple source config hashes")
        source_config_hash = next(iter(source_configs))
        scopes = {(item.account_id, item.symbol, item.magic) for item in frozen}
        if len(scopes) != 1:
            raise LearningConflictError("learning batch spans multiple account/symbol/magic scopes")
        account_id, symbol, magic = next(iter(scopes))

        ordered = tuple(
            sorted(
                frozen,
                key=lambda item: (
                    item.broker_closed_at_ms,
                    item.trade_id,
                    item.close_event_id,
                ),
            )
        )
        grouped: dict[LearningContext, list[AuthoritativeClosedOutcome]] = {}
        for outcome in ordered:
            for attribution in outcome.attributions:
                context = LearningContext(attribution.agent_id, outcome.regime, outcome.session)
                grouped.setdefault(context, []).append(outcome)

        context_stats = tuple(
            self._calculate_context(context, grouped[context])
            for context in sorted(grouped, key=lambda item: item.sort_key)
        )
        agent_summaries = tuple(
            AgentSampleSummary(
                agent_id=agent_id,
                total_sample_count=sum(
                    stat.sample_count for stat in context_stats if stat.context.agent_id == agent_id
                ),
                context_count=sum(1 for stat in context_stats if stat.context.agent_id == agent_id),
                activated_context_count=sum(
                    1
                    for stat in context_stats
                    if stat.context.agent_id == agent_id and stat.activated
                ),
            )
            for agent_id in CANONICAL_AGENT_IDS
        )
        policy_config_hash = self._policy_config_hash()
        source_trade_ids = tuple(sorted(item.trade_id for item in ordered))
        source_event_ids = tuple(sorted(item.close_event_id for item in ordered))
        payload = self._proposal_payload(
            ordered,
            context_stats,
            version=version,
            parent_hash=parent_hash,
            policy_config_hash=policy_config_hash,
            account_id=account_id,
            symbol=symbol,
            magic=magic,
            source_config_hash=source_config_hash,
            target_config_hash=target_hash,
        )
        proposal_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return LearningProposal(
            schema_version=LEARNING_SCHEMA_VERSION,
            proposal_version=version,
            proposal_id=f"learn-{proposal_hash[:32]}",
            proposal_hash=proposal_hash,
            parent_proposal_hash=parent_hash,
            policy_config_hash=policy_config_hash,
            account_id=account_id,
            symbol=symbol,
            magic=magic,
            source_config_hash=source_config_hash,
            target_config_hash=target_hash,
            source_trade_ids=source_trade_ids,
            source_event_ids=source_event_ids,
            as_of_broker_time_ms=max(item.broker_closed_at_ms for item in ordered),
            context_stats=context_stats,
            agent_summaries=agent_summaries,
            minimum_samples=self.config.minimum_samples,
            neutral_weight=self.config.neutral_weight,
            minimum_weight=self.config.minimum_weight,
            maximum_weight=self.config.maximum_weight,
        )

    @staticmethod
    def _validate_provenance(outcomes: Sequence[AuthoritativeClosedOutcome]) -> None:
        for outcome in outcomes:
            if outcome.source is not OutcomeSource.BROKER_LEDGER:
                raise LearningInputError(
                    f"outcome {outcome.trade_id!r} is not broker-ledger evidence"
                )
            if not outcome.authoritative:
                raise LearningInputError(f"outcome {outcome.trade_id!r} is not authoritative")
            if not outcome.broker_confirmed:
                raise LearningInputError(f"outcome {outcome.trade_id!r} is unconfirmed")
            if not outcome.deal_derived_actual_r:
                raise LearningInputError(
                    f"outcome {outcome.trade_id!r} actual R was not derived from broker deals"
                )

    @staticmethod
    def _validate_identities(outcomes: Sequence[AuthoritativeClosedOutcome]) -> None:
        trades: dict[str, AuthoritativeClosedOutcome] = {}
        events: dict[str, AuthoritativeClosedOutcome] = {}
        for outcome in outcomes:
            prior_trade = trades.get(outcome.trade_id)
            if prior_trade is not None:
                if prior_trade == outcome:
                    raise LearningDuplicateError(f"duplicate trade outcome {outcome.trade_id!r}")
                raise LearningConflictError(f"conflicting outcome for trade {outcome.trade_id!r}")
            prior_event = events.get(outcome.close_event_id)
            if prior_event is not None:
                if prior_event == outcome:
                    raise LearningDuplicateError(
                        f"duplicate close event {outcome.close_event_id!r}"
                    )
                raise LearningConflictError(
                    f"close event {outcome.close_event_id!r} belongs to conflicting outcomes"
                )
            trades[outcome.trade_id] = outcome
            events[outcome.close_event_id] = outcome

    def _calculate_context(
        self,
        context: LearningContext,
        outcomes: Sequence[AuthoritativeClosedOutcome],
    ) -> ContextLearningStat:
        ordered = sorted(
            outcomes,
            key=lambda item: (item.broker_closed_at_ms, item.trade_id, item.close_event_id),
        )
        used = ordered[-self.config.maximum_samples_per_context :]
        clipped = tuple(
            _clamp(item.actual_r, -self.config.actual_r_clip, self.config.actual_r_clip)
            for item in used
        )
        wins = sum(1 for value in clipped if value > ZERO)
        losses = sum(1 for value in clipped if value < ZERO)
        flats = len(clipped) - wins - losses
        total = sum(clipped, ZERO)
        mean = total / Decimal(len(clipped)) if clipped else ZERO
        gross_profit = sum((value for value in clipped if value > ZERO), ZERO)
        gross_loss = sum((-value for value in clipped if value < ZERO), ZERO)
        absolute_payoff = gross_profit + gross_loss
        payoff_balance = (
            (gross_profit - gross_loss) / absolute_payoff if absolute_payoff > ZERO else ZERO
        )
        directional_count = wins + losses
        outcome_balance = (
            Decimal(wins - losses) / Decimal(directional_count)
            if directional_count
            else ZERO
        )
        expectancy = _clamp(mean / self.config.actual_r_clip, -ONE, ONE)
        score = (
            expectancy * Decimal("0.50")
            + outcome_balance * Decimal("0.25")
            + payoff_balance * Decimal("0.25")
        )
        score = _clamp(score, -ONE, ONE)
        activated = len(used) >= self.config.minimum_samples
        weight = (
            self.config.neutral_weight + score * self.config.maximum_adjustment
            if activated
            else self.config.neutral_weight
        )
        weight = _clamp(weight, self.config.minimum_weight, self.config.maximum_weight)
        quantum = self.config.weight_quantum

        def quantize(value: Decimal) -> Decimal:
            return value.quantize(quantum, rounding=ROUND_HALF_EVEN)

        return ContextLearningStat(
            context=context,
            sample_count=len(ordered),
            used_sample_count=len(used),
            win_count=wins,
            loss_count=losses,
            flat_count=flats,
            clipped_r_sum=quantize(total),
            clipped_r_mean=quantize(mean),
            payoff_balance=quantize(payoff_balance),
            outcome_balance=quantize(outcome_balance),
            score=quantize(score),
            proposed_weight=quantize(weight),
            activated=activated,
        )

    def _policy_config_hash(self) -> str:
        payload = {
            "minimum_samples": self.config.minimum_samples,
            "maximum_samples_per_context": self.config.maximum_samples_per_context,
            "neutral_weight": _decimal_text(self.config.neutral_weight),
            "minimum_weight": _decimal_text(self.config.minimum_weight),
            "maximum_weight": _decimal_text(self.config.maximum_weight),
            "maximum_adjustment": _decimal_text(self.config.maximum_adjustment),
            "actual_r_clip": _decimal_text(self.config.actual_r_clip),
            "weight_quantum": _decimal_text(self.config.weight_quantum),
            "maximum_batch_outcomes": self.config.maximum_batch_outcomes,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _outcome_payload(outcome: AuthoritativeClosedOutcome) -> dict[str, object]:
        return {
            "trade_id": outcome.trade_id,
            "close_event_id": outcome.close_event_id,
            "account_id": outcome.account_id,
            "symbol": outcome.symbol,
            "magic": outcome.magic,
            "actual_r": _decimal_text(outcome.actual_r),
            "regime": outcome.regime.value,
            "session": outcome.session.value,
            "broker_opened_at_ms": outcome.broker_opened_at_ms,
            "broker_closed_at_ms": outcome.broker_closed_at_ms,
            "config_hash": outcome.config_hash,
            "source": outcome.source.value,
            "authoritative": outcome.authoritative,
            "broker_confirmed": outcome.broker_confirmed,
            "deal_derived_actual_r": outcome.deal_derived_actual_r,
            "attributions": [
                {
                    "agent_id": item.agent_id,
                    "signal_id": item.signal_id,
                    "strategy_slug": item.strategy_slug,
                    "tags": list(item.tags),
                }
                for item in outcome.attributions
            ],
        }

    @staticmethod
    def _stat_payload(stat: ContextLearningStat) -> dict[str, object]:
        return {
            "agent_id": stat.context.agent_id,
            "regime": stat.context.regime.value,
            "session": stat.context.session.value,
            "sample_count": stat.sample_count,
            "used_sample_count": stat.used_sample_count,
            "win_count": stat.win_count,
            "loss_count": stat.loss_count,
            "flat_count": stat.flat_count,
            "clipped_r_sum": _decimal_text(stat.clipped_r_sum),
            "clipped_r_mean": _decimal_text(stat.clipped_r_mean),
            "payoff_balance": _decimal_text(stat.payoff_balance),
            "outcome_balance": _decimal_text(stat.outcome_balance),
            "score": _decimal_text(stat.score),
            "proposed_weight": _decimal_text(stat.proposed_weight),
            "activated": stat.activated,
        }

    def _proposal_payload(
        self,
        outcomes: Sequence[AuthoritativeClosedOutcome],
        stats: Sequence[ContextLearningStat],
        *,
        version: int,
        parent_hash: str | None,
        policy_config_hash: str,
        account_id: str,
        symbol: str,
        magic: int,
        source_config_hash: str,
        target_config_hash: str,
    ) -> dict[str, object]:
        return {
            "schema_version": LEARNING_SCHEMA_VERSION,
            "proposal_version": version,
            "parent_proposal_hash": parent_hash,
            "policy_config_hash": policy_config_hash,
            "account_id": account_id,
            "symbol": symbol,
            "magic": magic,
            "source_config_hash": source_config_hash,
            "target_config_hash": target_config_hash,
            "outcomes": [self._outcome_payload(item) for item in outcomes],
            "context_stats": [self._stat_payload(item) for item in stats],
        }


__all__ = [
    "ActivatedAgentWeight",
    "AgentSampleSummary",
    "AuthoritativeClosedOutcome",
    "CanonicalLearningPolicy",
    "ContextLearningStat",
    "LEARNING_SCHEMA_VERSION",
    "LearningActivation",
    "LearningActivationError",
    "LearningConflictError",
    "LearningContext",
    "LearningDuplicateError",
    "LearningInputError",
    "LearningPolicyConfig",
    "LearningPolicyError",
    "LearningProposal",
    "OutcomeSource",
    "StrategyOutcomeAttribution",
]
