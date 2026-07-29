from python_swarm.risk_and_lifecycle import InstitutionalRiskController


def test_small_account_profile_auto_reduces_base_risk():
    with InstitutionalRiskController(initial_balance=300.0) as risk:
        assert risk.small_account_mode is True
        assert risk.MAX_RISK_PCT == 0.5
        assert risk.MAX_DAILY_DD_PCT == 2.0


def test_min_lot_guard_skips_trade_that_exceeds_budget():
    with InstitutionalRiskController(
        initial_balance=100.0,
        max_risk_pct=0.5,
        min_lot=0.01,
        skip_if_min_lot_overrisks=True,
    ) as risk:
        lot = risk.calculate_lot_size(entry=2685.0, sl=2675.0, confidence=80.0)
        assert lot == 0.0


def test_min_lot_overrisk_can_be_enabled_for_simulation_only():
    with InstitutionalRiskController(
        initial_balance=100.0,
        max_risk_pct=0.5,
        min_lot=0.01,
        skip_if_min_lot_overrisks=False,
    ) as risk:
        lot = risk.calculate_lot_size(entry=2685.0, sl=2675.0, confidence=80.0)
        assert lot == 0.01


def test_three_decimal_lot_step_is_preserved():
    with InstitutionalRiskController(
        initial_balance=300.0,
        max_risk_pct=1.0,
        min_lot=0.001,
        lot_step=0.001,
        skip_if_min_lot_overrisks=True,
    ) as risk:
        lot = risk.calculate_lot_size(entry=2685.0, sl=2675.0, confidence=80.0)
        assert lot == 0.002
