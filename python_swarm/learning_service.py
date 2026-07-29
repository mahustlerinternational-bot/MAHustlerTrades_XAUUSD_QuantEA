"""Restart-safe governed Agent-20 learning application service.

The pure policy proposes bounded fusion weights.  This service supplies only
authoritative ledger outcomes, persists immutable lineage, and requires an
explicit approval record before an exact regime/session activation can exist.
Importing this module performs no I/O.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping, Sequence

from .learning_policy import (
    ActivatedAgentWeight,
    AgentSampleSummary,
    AuthoritativeClosedOutcome,
    CanonicalLearningPolicy,
    CanonicalRegime,
    ContextLearningStat,
    LearningActivation,
    LearningContext,
    LearningPolicyError,
    LearningProposal,
    MarketSession,
    OutcomeSource,
    StrategyOutcomeAttribution,
)
from .trade_ledger import (
    LedgerConflictError,
    LedgerError,
    LedgerInvariantError,
    LearningActivationRecord,
    LearningApprovalRecord,
    LearningProposalRecord,
    LearningSnapshotRecord,
    TradeLedger,
)


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _hash(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


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
        "clipped_r_sum": str(stat.clipped_r_sum),
        "clipped_r_mean": str(stat.clipped_r_mean),
        "payoff_balance": str(stat.payoff_balance),
        "outcome_balance": str(stat.outcome_balance),
        "score": str(stat.score),
        "proposed_weight": str(stat.proposed_weight),
        "activated": stat.activated,
    }


def _summary_payload(summary: AgentSampleSummary) -> dict[str, object]:
    return {
        "agent_id": summary.agent_id,
        "total_sample_count": summary.total_sample_count,
        "context_count": summary.context_count,
        "activated_context_count": summary.activated_context_count,
    }


def proposal_payload(proposal: LearningProposal) -> dict[str, object]:
    return {
        "schema_version": proposal.schema_version,
        "proposal_version": proposal.proposal_version,
        "proposal_id": proposal.proposal_id,
        "proposal_hash": proposal.proposal_hash,
        "parent_proposal_hash": proposal.parent_proposal_hash,
        "policy_config_hash": proposal.policy_config_hash,
        "account_id": proposal.account_id,
        "symbol": proposal.symbol,
        "magic": proposal.magic,
        "source_config_hash": proposal.source_config_hash,
        "target_config_hash": proposal.target_config_hash,
        "source_trade_ids": list(proposal.source_trade_ids),
        "source_event_ids": list(proposal.source_event_ids),
        "as_of_broker_time_ms": proposal.as_of_broker_time_ms,
        "context_stats": [_stat_payload(item) for item in proposal.context_stats],
        "agent_summaries": [
            _summary_payload(item) for item in proposal.agent_summaries
        ],
        "minimum_samples": proposal.minimum_samples,
        "neutral_weight": str(proposal.neutral_weight),
        "minimum_weight": str(proposal.minimum_weight),
        "maximum_weight": str(proposal.maximum_weight),
    }


def proposal_from_payload(value: Mapping[str, Any]) -> LearningProposal:
    stats = tuple(
        ContextLearningStat(
            context=LearningContext(
                int(item["agent_id"]),
                CanonicalRegime(item["regime"]),
                MarketSession(item["session"]),
            ),
            sample_count=int(item["sample_count"]),
            used_sample_count=int(item["used_sample_count"]),
            win_count=int(item["win_count"]),
            loss_count=int(item["loss_count"]),
            flat_count=int(item["flat_count"]),
            clipped_r_sum=Decimal(str(item["clipped_r_sum"])),
            clipped_r_mean=Decimal(str(item["clipped_r_mean"])),
            payoff_balance=Decimal(str(item["payoff_balance"])),
            outcome_balance=Decimal(str(item["outcome_balance"])),
            score=Decimal(str(item["score"])),
            proposed_weight=Decimal(str(item["proposed_weight"])),
            activated=bool(item["activated"]),
        )
        for item in value["context_stats"]
    )
    summaries = tuple(
        AgentSampleSummary(
            agent_id=int(item["agent_id"]),
            total_sample_count=int(item["total_sample_count"]),
            context_count=int(item["context_count"]),
            activated_context_count=int(item["activated_context_count"]),
        )
        for item in value["agent_summaries"]
    )
    return LearningProposal(
        schema_version=int(value["schema_version"]),
        proposal_version=int(value["proposal_version"]),
        proposal_id=str(value["proposal_id"]),
        proposal_hash=str(value["proposal_hash"]),
        parent_proposal_hash=(
            None
            if value.get("parent_proposal_hash") is None
            else str(value["parent_proposal_hash"])
        ),
        policy_config_hash=str(value["policy_config_hash"]),
        account_id=str(value["account_id"]),
        symbol=str(value["symbol"]),
        magic=int(value["magic"]),
        source_config_hash=str(value["source_config_hash"]),
        target_config_hash=str(value["target_config_hash"]),
        source_trade_ids=tuple(str(item) for item in value["source_trade_ids"]),
        source_event_ids=tuple(str(item) for item in value["source_event_ids"]),
        as_of_broker_time_ms=int(value["as_of_broker_time_ms"]),
        context_stats=stats,
        agent_summaries=summaries,
        minimum_samples=int(value["minimum_samples"]),
        neutral_weight=Decimal(str(value["neutral_weight"])),
        minimum_weight=Decimal(str(value["minimum_weight"])),
        maximum_weight=Decimal(str(value["maximum_weight"])),
    )


def _outcome_from_source(row: Mapping[str, Any]) -> AuthoritativeClosedOutcome:
    attribution = row.get("attribution")
    if not isinstance(attribution, list):
        raise LedgerInvariantError("learning source attribution JSON is invalid")
    try:
        regime = CanonicalRegime(str(row["regime"]))
        session = MarketSession(str(row["session"]))
    except (KeyError, ValueError, TypeError) as exc:
        raise LedgerInvariantError(
            "learning source regime/session context is not canonical"
        ) from exc
    return AuthoritativeClosedOutcome(
        trade_id=str(row["trade_id"]),
        close_event_id=str(row["close_event_id"]),
        account_id=str(row["account_id"]),
        symbol=str(row["symbol"]),
        magic=int(row["magic"]),
        actual_r=Decimal(str(row["actual_r_decimal"])),
        attributions=tuple(
            StrategyOutcomeAttribution(
                agent_id=int(item["agent_id"]),
                signal_id=str(item["signal_id"]),
                strategy_slug=str(item["strategy_slug"]),
                tags=tuple(str(tag) for tag in item.get("tags", ())),
            )
            for item in attribution
        ),
        regime=regime,
        session=session,
        broker_opened_at_ms=int(row["broker_opened_at_ms"]),
        broker_closed_at_ms=int(row["broker_closed_at_ms"]),
        config_hash=str(row["source_config_hash"]),
        source=OutcomeSource.BROKER_LEDGER,
        authoritative=True,
        broker_confirmed=True,
        deal_derived_actual_r=True,
    )


@dataclass(frozen=True, slots=True)
class PMAProcessResult:
    job_id: str
    status: str
    proposal_id: str | None = None
    reason: str = ""


class GovernedLearningService:
    """Durable PMA worker plus explicit approval/activation application API."""

    def __init__(
        self,
        ledger: TradeLedger,
        *,
        policy: CanonicalLearningPolicy | None = None,
        retry_delay_ms: int = 60_000,
    ) -> None:
        if not isinstance(ledger, TradeLedger):
            raise TypeError("ledger must be TradeLedger")
        if retry_delay_ms < 1_000 or retry_delay_ms > 3_600_000:
            raise ValueError("learning retry delay must be between 1s and 1h")
        self.ledger = ledger
        self.policy = policy or CanonicalLearningPolicy()
        self.retry_delay_ms = retry_delay_ms

    def process_due(
        self,
        worker_id: str,
        *,
        now_ms: int,
        target_config_hash: str,
        limit: int = 10,
        lease_ms: int = 60_000,
        account_id: str | None = None,
        symbol: str | None = None,
        magic: int | None = None,
    ) -> tuple[PMAProcessResult, ...]:
        worker = _text(worker_id, "worker_id")
        target = _text(target_config_hash, "target_config_hash")
        jobs = self.ledger.claim_pma_jobs(
            worker,
            now_ms=now_ms,
            lease_ms=lease_ms,
            limit=limit,
            account_id=account_id,
            symbol=symbol,
            magic=magic,
        )
        return tuple(
            self._process_one(job, worker, now_ms=now_ms, target_config_hash=target)
            for job in jobs
        )

    def _process_one(
        self,
        job: Mapping[str, Any],
        worker: str,
        *,
        now_ms: int,
        target_config_hash: str,
    ) -> PMAProcessResult:
        job_id = str(job["job_id"])
        try:
            self.ledger.pin_pma_target_config(
                job_id,
                worker,
                target_config_hash=target_config_hash,
                updated_at_ms=now_ms,
            )
            if any(job.get(name) is None for name in ("source_config_hash", "regime", "session")):
                self.ledger.terminalize_pma_job(
                    job_id,
                    worker,
                    status="INELIGIBLE",
                    reason="MISSING_CANONICAL_ENTRY_CONTEXT",
                    completed_at_ms=now_ms,
                )
                return PMAProcessResult(job_id, "INELIGIBLE", reason="MISSING_CONTEXT")
            existing = self.ledger.get_learning_proposal_for_source(
                str(job["close_event_id"]), target_config_hash=target_config_hash
            )
            if existing is not None:
                self.ledger.terminalize_pma_job(
                    job_id,
                    worker,
                    status="COMPLETED",
                    reason="RECOVERED_EXISTING_IMMUTABLE_PROPOSAL",
                    completed_at_ms=now_ms,
                    result_proposal_id=str(existing["proposal_id"]),
                )
                return PMAProcessResult(
                    job_id, "COMPLETED", str(existing["proposal_id"]), "RECOVERED"
                )

            primary = self.ledger.build_authoritative_learning_source(
                str(job["trade_id"]),
                str(job["close_event_id"]),
                created_at_ms=now_ms,
            )
            if (
                primary.account_id != job["account_id"]
                or primary.symbol != job["symbol"]
                or primary.magic != job["magic"]
                or primary.source_config_hash != job["source_config_hash"]
                or primary.regime != job["regime"]
                or primary.session != job["session"]
            ):
                raise LedgerConflictError("PMA job scope differs from source trade")
            refs = self.ledger.list_learning_candidate_refs(
                account_id=primary.account_id,
                symbol=primary.symbol,
                magic=primary.magic,
                source_config_hash=primary.source_config_hash,
                as_of_broker_time_ms=primary.broker_closed_at_ms,
            )
            for trade_id, event_id in refs:
                source = self.ledger.build_authoritative_learning_source(
                    trade_id, event_id, created_at_ms=now_ms
                )
                self.ledger.record_learning_source(source)
            rows = self.ledger.list_learning_sources(
                account_id=primary.account_id,
                symbol=primary.symbol,
                magic=primary.magic,
                source_config_hash=primary.source_config_hash,
                as_of_broker_time_ms=primary.broker_closed_at_ms,
            )
            outcomes = tuple(_outcome_from_source(row) for row in rows)
            version, parent = self.ledger.next_learning_proposal_lineage(
                account_id=primary.account_id,
                symbol=primary.symbol,
                magic=primary.magic,
                source_config_hash=primary.source_config_hash,
                target_config_hash=target_config_hash,
            )
            proposal = self.policy.propose(
                outcomes,
                proposal_version=version,
                target_config_hash=target_config_hash,
                parent_proposal_hash=parent,
            )
            payload = proposal_payload(proposal)
            source_set = sorted(
                [
                {
                    "source_id": row["source_id"],
                    "trade_id": row["trade_id"],
                    "close_event_id": row["close_event_id"],
                    "content_hash": row["content_hash"],
                }
                for row in rows
                ],
                key=lambda item: str(item["source_id"]),
            )
            source_set_hash = _hash(source_set)
            stats = payload["context_stats"]
            summaries = payload["agent_summaries"]
            bounds = {
                "minimum_samples": proposal.minimum_samples,
                "neutral_weight": str(proposal.neutral_weight),
                "minimum_weight": str(proposal.minimum_weight),
                "maximum_weight": str(proposal.maximum_weight),
            }
            snapshot_content = {
                "proposal_hash": proposal.proposal_hash,
                "source_set_hash": source_set_hash,
                "context_stats": stats,
                "agent_summaries": summaries,
                "bounds": bounds,
            }
            snapshot_hash = _hash(snapshot_content)
            self.ledger.record_learning_proposal_bundle(
                LearningProposalRecord(
                    proposal_id=proposal.proposal_id,
                    proposal_hash=proposal.proposal_hash,
                    proposal_version=proposal.proposal_version,
                    parent_proposal_hash=proposal.parent_proposal_hash,
                    policy_config_hash=proposal.policy_config_hash,
                    account_id=proposal.account_id,
                    symbol=proposal.symbol,
                    magic=proposal.magic,
                    source_config_hash=proposal.source_config_hash,
                    target_config_hash=proposal.target_config_hash,
                    as_of_broker_time_ms=proposal.as_of_broker_time_ms,
                    proposal=payload,
                    created_at_ms=now_ms,
                ),
                LearningSnapshotRecord(
                    snapshot_id=f"snapshot-{snapshot_hash[:32]}",
                    proposal_id=proposal.proposal_id,
                    proposal_hash=proposal.proposal_hash,
                    source_set_hash=source_set_hash,
                    context_stats=stats,  # type: ignore[arg-type]
                    agent_summaries=summaries,  # type: ignore[arg-type]
                    bounds=bounds,
                    content_hash=snapshot_hash,
                    created_at_ms=now_ms,
                ),
                source_ids=tuple(str(row["source_id"]) for row in rows),
            )
            self.ledger.terminalize_pma_job(
                job_id,
                worker,
                status="COMPLETED",
                reason="IMMUTABLE_PROPOSAL_RECORDED_AWAITING_APPROVAL",
                completed_at_ms=now_ms,
                result_proposal_id=proposal.proposal_id,
            )
            return PMAProcessResult(job_id, "COMPLETED", proposal.proposal_id)
        except (LearningPolicyError, LedgerInvariantError) as exc:
            self.ledger.terminalize_pma_job(
                job_id,
                worker,
                status="INELIGIBLE",
                reason=f"{type(exc).__name__}:{str(exc)[:180]}",
                completed_at_ms=now_ms,
            )
            return PMAProcessResult(job_id, "INELIGIBLE", reason=type(exc).__name__)
        except LedgerConflictError as exc:
            self.ledger.fail_pma_job(
                job_id,
                worker,
                error=f"{type(exc).__name__}:{str(exc)[:180]}",
                failed_at_ms=now_ms,
                retry_at_ms=None,
            )
            return PMAProcessResult(job_id, "DEAD_LETTER", reason="LEDGER_CONFLICT")
        except LedgerError as exc:
            attempts = int(job["attempts"])
            status = self.ledger.fail_pma_job(
                job_id,
                worker,
                error=f"{type(exc).__name__}:{str(exc)[:180]}",
                failed_at_ms=now_ms,
                retry_at_ms=now_ms + self.retry_delay_ms * min(8, 2 ** (attempts - 1)),
            )
            return PMAProcessResult(job_id, status, reason=type(exc).__name__)

    def approve(
        self,
        proposal_id: str,
        *,
        approved_by: str,
        rationale: str,
        approved_at_ms: int,
    ) -> str:
        proposal = self.ledger.get_learning_proposal(proposal_id)
        if proposal is None:
            raise LedgerInvariantError("cannot approve an unknown proposal")
        material = {
            "proposal_id": proposal["proposal_id"],
            "proposal_hash": proposal["proposal_hash"],
            "approved_by": _text(approved_by, "approved_by"),
            "rationale": _text(rationale, "rationale"),
            "approved_at_ms": approved_at_ms,
        }
        content_hash = _hash(material)
        approval_id = f"approval-{content_hash[:32]}"
        self.ledger.record_learning_approval(
            LearningApprovalRecord(
                approval_id=approval_id,
                proposal_id=str(proposal["proposal_id"]),
                proposal_hash=str(proposal["proposal_hash"]),
                approved_by=str(material["approved_by"]),
                approved_at_ms=approved_at_ms,
                rationale=str(material["rationale"]),
                content_hash=content_hash,
            )
        )
        return approval_id

    def activate(
        self,
        proposal_id: str,
        approval_id: str,
        *,
        regime: CanonicalRegime,
        session: MarketSession,
        activated_by: str,
        activated_at_ms: int,
    ) -> LearningActivation:
        stored = self.ledger.get_learning_proposal(proposal_id)
        approval = self.ledger.get_learning_approval(approval_id)
        if stored is None or approval is None:
            raise LedgerInvariantError("activation requires proposal and approval")
        if (
            approval["proposal_id"] != proposal_id
            or approval["proposal_hash"] != stored["proposal_hash"]
        ):
            raise LedgerConflictError("approval does not bind requested proposal")
        proposal = proposal_from_payload(stored["proposal"])
        activation = proposal.activate(
            regime,
            session,
            approved_proposal_hash=str(approval["proposal_hash"]),
        )
        weights = [
            {
                "agent_id": item.agent_id,
                "weight": str(item.weight),
                "sample_count": item.sample_count,
                "learned": item.learned,
            }
            for item in activation.weights
        ]
        material = {
            "approval_id": approval_id,
            "proposal_id": proposal_id,
            "proposal_hash": activation.proposal_hash,
            "regime": activation.regime.value,
            "session": activation.session.value,
            "weights": weights,
            "activated_by": _text(activated_by, "activated_by"),
            "activated_at_ms": activated_at_ms,
        }
        content_hash = _hash(material)
        record = LearningActivationRecord(
            activation_id=f"activation-{content_hash[:32]}",
            approval_id=approval_id,
            proposal_id=proposal_id,
            proposal_hash=activation.proposal_hash,
            account_id=activation.account_id,
            symbol=activation.symbol,
            magic=activation.magic,
            target_config_hash=activation.target_config_hash,
            regime=activation.regime.value,
            session=activation.session.value,
            weights=weights,
            activated_by=str(material["activated_by"]),
            activated_at_ms=activated_at_ms,
            content_hash=content_hash,
        )
        self.ledger.record_learning_activation(record)
        return activation

    def active(
        self,
        *,
        account_id: str,
        symbol: str,
        magic: int,
        target_config_hash: str,
        regime: CanonicalRegime,
        session: MarketSession,
    ) -> LearningActivation | None:
        row = self.ledger.get_active_learning_activation(
            account_id=account_id,
            symbol=symbol,
            magic=magic,
            target_config_hash=target_config_hash,
            regime=regime.value,
            session=session.value,
        )
        if row is None:
            return None
        weights = row.get("weights")
        if not isinstance(weights, list):
            raise LedgerInvariantError("active learning weights are corrupt")
        return LearningActivation(
            proposal_hash=str(row["proposal_hash"]),
            proposal_version=int(row["proposal_version"]),
            account_id=str(row["account_id"]),
            symbol=str(row["symbol"]),
            magic=int(row["magic"]),
            target_config_hash=str(row["target_config_hash"]),
            regime=CanonicalRegime(str(row["regime"])),
            session=MarketSession(str(row["session"])),
            weights=tuple(
                ActivatedAgentWeight(
                    agent_id=int(item["agent_id"]),
                    weight=Decimal(str(item["weight"])),
                    sample_count=int(item["sample_count"]),
                    learned=bool(item["learned"]),
                )
                for item in weights
            ),
        )


__all__ = [
    "GovernedLearningService",
    "PMAProcessResult",
    "proposal_from_payload",
    "proposal_payload",
]
