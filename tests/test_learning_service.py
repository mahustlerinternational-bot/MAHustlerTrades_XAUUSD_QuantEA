import sqlite3

import pytest

from python_swarm.agent_roster import AGENT_BY_ID
from python_swarm.learning_service import GovernedLearningService
from python_swarm.strategy_pipeline import CanonicalRegime, MarketSession
from python_swarm.trade_ledger import (
    BrokerDealRecord,
    LedgerConflictError,
    LedgerInvariantError,
    TradeAttributionRecord,
    TradeLedger,
    TradeRecord,
)


ACCOUNT = "demo-learning-1"
SYMBOL = "XAUUSD"
MAGIC = 20260715
OPENED = 1_800_000_000.0
CLOSED = OPENED + 600


@pytest.fixture
def ledger(tmp_path):
    db = tmp_path / "learning.db"
    with TradeLedger(db, clock=lambda: CLOSED + 1) as value:
        yield value


def create_closed_canonical_trade(ledger: TradeLedger) -> None:
    ledger.create_trade(
        TradeRecord(
            trade_id="trade-learning-1",
            request_id="intent-learning-1",
            account_id=ACCOUNT,
            symbol=SYMBOL,
            magic_number=MAGIC,
            direction="BUY",
            decision_at=OPENED - 10,
            intended_entry="2500",
            intended_sl="2495",
            tp1="2505",
            tp2="2510",
            tp3="2515",
            requested_volume="0.1",
            initial_risk_currency="50",
            currency="USD",
            regime_at_entry=CanonicalRegime.TRENDING_BULL.value,
            session_at_entry=MarketSession.LONDON.value,
            config_hash="source-config-v1",
            evidence_class="CANONICAL_RUNTIME",
            learning_eligible=True,
        )
    )
    for agent_id in (4, 5, 6):
        ledger.record_attribution(
            TradeAttributionRecord(
                attribution_id=f"trade-learning-1:agent:{agent_id}",
                trade_id="trade-learning-1",
                agent_id=agent_id,
                strategy_slug=AGENT_BY_ID[agent_id].slug,
                direction="BUY",
                conviction="80",
                learned_weight="1",
                effective_weight="1",
                signal_at=OPENED - 10,
                tags=("canonical",),
                metadata={"signal_id": f"signal-{agent_id}"},
            )
        )
    ledger.mark_trade_submitted(
        "trade-learning-1", event_id="submitted-learning-1", occurred_at=OPENED - 5
    )
    ledger.acknowledge_trade_open(
        "trade-learning-1",
        event_id="opened-learning-1",
        account_id=ACCOUNT,
        broker_order_id="order-learning-1",
        broker_position_id="position-learning-1",
        fill_price=2500.0,
        filled_volume=0.1,
        opened_at=OPENED,
        broker_timestamp=OPENED,
    )
    ledger.record_deal(
        BrokerDealRecord(
            account_id=ACCOUNT,
            deal_id="deal-learning-close-1",
            trade_id="trade-learning-1",
            position_id="position-learning-1",
            order_id="order-learning-close-1",
            deal_type="ENTRY_OUT",
            volume="0.1",
            price="2510",
            profit="51",
            commission="-1",
            swap="0",
            fee="0",
            occurred_at=CLOSED,
        )
    )
    ledger.close_trade(
        "trade-learning-1",
        event_id="closed-learning-1",
        exit_price=2510.0,
        net_realized_pnl=0,
        closed_at=CLOSED,
        broker_timestamp=CLOSED,
    )


def test_pma_is_due_at_t_plus_15_and_persists_exact_source_and_proposal(ledger):
    create_closed_canonical_trade(ledger)
    service = GovernedLearningService(ledger)
    job = ledger.get_pma_job_for_trade("trade-learning-1")
    assert job["due_at_ms"] == int(CLOSED * 1000) + 15 * 60_000

    assert service.process_due(
        "worker-1",
        now_ms=job["due_at_ms"] - 1,
        target_config_hash="target-config-v2",
    ) == ()
    result = service.process_due(
        "worker-1",
        now_ms=job["due_at_ms"],
        target_config_hash="target-config-v2",
    )

    assert len(result) == 1 and result[0].status == "COMPLETED"
    stored_job = ledger.get_pma_job(job["job_id"])
    assert stored_job["result_proposal_id"] == result[0].proposal_id
    sources = ledger.list_learning_sources(
        account_id=ACCOUNT,
        symbol=SYMBOL,
        magic=MAGIC,
        source_config_hash="source-config-v1",
        as_of_broker_time_ms=int(CLOSED * 1000),
    )
    assert len(sources) == 1
    assert sources[0]["actual_r_decimal"] == "1"
    assert [item["agent_id"] for item in sources[0]["attribution"]] == [4, 5, 6]
    assert sources[0]["broker_deals"][0]["deal_id"] == "deal-learning-close-1"


def test_activation_requires_explicit_matching_approval_and_survives_restart(ledger):
    create_closed_canonical_trade(ledger)
    service = GovernedLearningService(ledger)
    job = ledger.get_pma_job_for_trade("trade-learning-1")
    result = service.process_due(
        "worker-1",
        now_ms=job["due_at_ms"],
        target_config_hash="target-config-v2",
    )[0]
    assert service.active(
        account_id=ACCOUNT,
        symbol=SYMBOL,
        magic=MAGIC,
        target_config_hash="target-config-v2",
        regime=CanonicalRegime.TRENDING_BULL,
        session=MarketSession.LONDON,
    ) is None

    approval_id = service.approve(
        result.proposal_id,
        approved_by="risk-committee",
        rationale="Reviewed bounded Agent-20 proposal",
        approved_at_ms=job["due_at_ms"] + 1,
    )
    with pytest.raises(LedgerInvariantError) as captured:
        service.activate(
            result.proposal_id,
            "approval-does-not-exist",
            regime=CanonicalRegime.TRENDING_BULL,
            session=MarketSession.LONDON,
            activated_by="risk-committee",
            activated_at_ms=job["due_at_ms"] + 2,
        )
    assert "approval" in str(captured.value).lower()

    activation = service.activate(
        result.proposal_id,
        approval_id,
        regime=CanonicalRegime.TRENDING_BULL,
        session=MarketSession.LONDON,
        activated_by="risk-committee",
        activated_at_ms=job["due_at_ms"] + 3,
    )
    assert tuple(item.agent_id for item in activation.weights) == tuple(range(4, 14))
    restarted = GovernedLearningService(ledger).active(
        account_id=ACCOUNT,
        symbol=SYMBOL,
        magic=MAGIC,
        target_config_hash="target-config-v2",
        regime=CanonicalRegime.TRENDING_BULL,
        session=MarketSession.LONDON,
    )
    assert restarted == activation

    with pytest.raises(sqlite3.IntegrityError, match="immutable learning record"):
        ledger._connection.execute(
            "UPDATE learning_proposals SET target_config_hash='tampered'"
        )


def test_noncanonical_or_missing_context_trade_is_never_a_learning_candidate(ledger):
    ledger.create_trade(
        TradeRecord(
            trade_id="recovered-1",
            request_id="recovered-request-1",
            account_id=ACCOUNT,
            symbol=SYMBOL,
            magic_number=MAGIC,
            direction="SELL",
            decision_at=OPENED,
            intended_entry="2500",
            intended_sl="2505",
            tp1="2495",
            tp2="2490",
            tp3="2485",
            requested_volume="0.1",
            initial_risk_currency="50",
            evidence_class="RECOVERED",
            learning_eligible=False,
        )
    )
    assert ledger.list_learning_candidate_refs(
        account_id=ACCOUNT,
        symbol=SYMBOL,
        magic=MAGIC,
        source_config_hash="source-config-v1",
        as_of_broker_time_ms=int(CLOSED * 1000),
    ) == []


def test_runtime_scoped_pma_worker_cannot_claim_or_pin_another_account(ledger):
    create_closed_canonical_trade(ledger)
    job = ledger.get_pma_job_for_trade("trade-learning-1")
    service = GovernedLearningService(ledger)

    assert service.process_due(
        "runtime-other",
        now_ms=job["due_at_ms"],
        target_config_hash="wrong-runtime-config",
        account_id="another-account",
        symbol=SYMBOL,
        magic=MAGIC,
    ) == ()
    untouched = ledger.get_pma_job(job["job_id"])
    assert untouched["status"] == "PENDING"
    assert untouched["attempts"] == 0
    assert untouched["target_config_hash"] is None

    result = service.process_due(
        "runtime-owner",
        now_ms=job["due_at_ms"],
        target_config_hash="owner-runtime-config",
        account_id=ACCOUNT,
        symbol=SYMBOL,
        magic=MAGIC,
    )
    assert len(result) == 1 and result[0].status == "COMPLETED"
    assert ledger.get_pma_job(job["job_id"])["target_config_hash"] == (
        "owner-runtime-config"
    )


def test_pma_lease_is_single_worker_and_expired_work_is_restart_safe(ledger):
    create_closed_canonical_trade(ledger)
    job = ledger.get_pma_job_for_trade("trade-learning-1")
    due = job["due_at_ms"]
    first = ledger.claim_pma_jobs(
        "worker-a", now_ms=due, lease_ms=1_000, limit=1
    )
    assert [item["job_id"] for item in first] == [job["job_id"]]
    assert ledger.claim_pma_jobs(
        "worker-b", now_ms=due + 999, lease_ms=1_000, limit=1
    ) == []
    reclaimed = ledger.claim_pma_jobs(
        "worker-b", now_ms=due + 1_001, lease_ms=1_000, limit=1
    )
    assert reclaimed[0]["attempts"] == 2
    ledger.fail_pma_job(
        job["job_id"],
        "worker-b",
        error="simulated restart",
        failed_at_ms=due + 1_001,
        retry_at_ms=due + 2_001,
    )

    service = GovernedLearningService(ledger)
    completed = service.process_due(
        "worker-c",
        now_ms=due + 2_001,
        target_config_hash="target-config-v2",
    )[0]
    assert completed.status == "COMPLETED"

    # Simulate a process crash after the immutable proposal commit but before
    # PMA terminalization.  The recovered worker must reuse, not version-bump.
    ledger._connection.execute(
        """
        UPDATE pma_jobs SET status='IN_FLIGHT',result_proposal_id=NULL,
            completed_at_ms=NULL,eligibility_reason=NULL,locked_by='dead-worker',
            locked_until_ms=?,updated_at_ms=? WHERE job_id=?
        """,
        (due + 3_000, due + 2_500, job["job_id"]),
    )
    recovered = service.process_due(
        "worker-d",
        now_ms=due + 3_001,
        target_config_hash="target-config-v2",
    )[0]
    assert recovered.status == "COMPLETED"
    assert recovered.proposal_id == completed.proposal_id
    assert recovered.reason == "RECOVERED"
    proposal_count = ledger._connection.execute(
        "SELECT COUNT(*) FROM learning_proposals"
    ).fetchone()[0]
    assert proposal_count == 1
