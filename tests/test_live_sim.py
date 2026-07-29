from python_swarm.live_sim import LiveSimEngine


def test_final_report_uses_configured_initial_balance(capsys):
    engine = LiveSimEngine(initial_balance=50000.0, max_seconds=0)
    try:
        engine._final_report()
        out = capsys.readouterr().out
    finally:
        engine._cleanup()

    assert "Initial balance:" in out
    assert "$50,000.00" in out
    assert "+0.00" in out
