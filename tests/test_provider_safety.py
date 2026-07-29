import asyncio

import pytest

from python_swarm.provider_adapters import (
    ExecutionProtection,
    HTTPJSONResponse,
    IntermarketHTTPProvider,
    MacroHTTPProvider,
    ProviderSafetyError,
    ProviderStatus,
    SimulationProvider,
)


class FakeGetTransport:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def get_json(self, url, *, headers, timeout_s):
        self.calls.append((url, dict(headers), timeout_s))
        if isinstance(self.response, BaseException):
            raise self.response
        return self.response


def run(coroutine):
    return asyncio.run(coroutine)


def covered_macro_envelope(now, *, events=None, **overrides):
    envelope = {
        "source_timestamp": now,
        "coverage_start_utc": now - 3_600,
        "coverage_end_utc": now + 90_000,
        "events": [] if events is None else events,
    }
    envelope.update(overrides)
    return envelope


def test_unconfigured_macro_provider_is_fail_closed_and_disables_dependents():
    provider = MacroHTTPProvider(url=None)

    snapshot = run(provider.poll_once())

    assert snapshot.status is ProviderStatus.DISABLED
    assert snapshot.disabled_agents == (10, 14)
    assert snapshot.execution_protection is ExecutionProtection.BLOCK_NEW_ENTRIES
    assert snapshot.execution_eligible is False
    assert snapshot.degraded is True
    assert snapshot.sequence == 1
    assert snapshot.source_timestamp is None
    assert snapshot.payload == {}


def test_explicit_technical_only_macro_mode_is_visible_and_unprotected():
    provider = MacroHTTPProvider(url=None, unsafe_technical_only=True)

    snapshot = run(provider.poll_once())

    assert snapshot.status is ProviderStatus.DISABLED
    assert (
        snapshot.execution_protection
        is ExecutionProtection.TECHNICAL_ONLY_UNPROTECTED
    )
    assert snapshot.execution_eligible is True
    assert snapshot.degraded is True
    assert snapshot.disabled_agents == (10, 14)


def test_unconfigured_intermarket_provider_disables_only_dependent_agents():
    provider = IntermarketHTTPProvider(url=None)

    snapshot = run(provider.poll_once())

    assert snapshot.status is ProviderStatus.DISABLED
    assert snapshot.disabled_agents == (12, 15)
    assert (
        snapshot.execution_protection
        is ExecutionProtection.DISABLE_DEPENDENT_AGENTS
    )
    assert snapshot.execution_eligible is True
    assert snapshot.degraded is True
    assert snapshot.payload == {}


def test_http_macro_provider_tracks_source_receive_sequence_and_freshness():
    now = 1_800_000_000.0
    transport = FakeGetTransport(
        HTTPJSONResponse(
            200,
            covered_macro_envelope(
                now,
                source="calendar-primary",
                source_timestamp=now - 5,
                events=[
                    {
                        "name": "CPI m/m",
                        "impact": "HIGH",
                        "scheduled_utc": now + 600,
                        "currency": "USD",
                        "forecast": "0.3%",
                    }
                ],
            ),
        )
    )
    provider = MacroHTTPProvider(
        url="https://provider.invalid/calendar",
        freshness_s=10,
        transport=transport,
        headers={"Authorization": "test-only"},
        clock=lambda: now,
    )

    first = run(provider.poll_once())
    second = run(provider.poll_once())

    assert first.status is ProviderStatus.HEALTHY
    assert first.source == "calendar-primary"
    assert first.source_timestamp == now - 5
    assert first.received_timestamp == now
    assert first.sequence == 1
    assert first.is_fresh(now)
    assert first.execution_eligible is True
    assert first.disabled_agents == ()
    assert first.as_dict(now)["source_age_s"] == 5

    expired = first.as_dict(now + 11)
    assert expired["provider_status"] == "STALE"
    assert expired["execution_eligible"] is False
    assert expired["disabled_agents"] == [10, 14]

    assert second.sequence == 2
    assert len(transport.calls) == 2
    assert transport.calls[0][1] == {"Authorization": "test-only"}


def test_stale_macro_source_blocks_entries_even_when_payload_is_valid():
    now = 1_800_000_000.0
    provider = MacroHTTPProvider(
        url="https://provider.invalid/calendar",
        freshness_s=60,
        transport=FakeGetTransport(
            HTTPJSONResponse(
                200,
                covered_macro_envelope(
                    now,
                    source_timestamp=now - 61,
                ),
            )
        ),
        clock=lambda: now,
    )

    snapshot = run(provider.poll_once())

    assert snapshot.status is ProviderStatus.STALE
    assert snapshot.execution_eligible is False
    assert snapshot.disabled_agents == (10, 14)


def test_provider_rejects_timestamp_regression_as_replayed_data():
    now = 1_800_000_000.0

    class SequencedTransport:
        def __init__(self):
            self.timestamps = iter((now - 2, now - 3))

        async def get_json(self, url, *, headers, timeout_s):
            return HTTPJSONResponse(
                200,
                covered_macro_envelope(
                    now,
                    source_timestamp=next(self.timestamps),
                ),
            )

    provider = MacroHTTPProvider(
        url="https://provider.invalid/calendar",
        transport=SequencedTransport(),
        clock=lambda: now,
    )

    assert run(provider.poll_once()).status is ProviderStatus.HEALTHY

    replay = run(provider.poll_once())

    assert replay.status is ProviderStatus.ERROR
    assert replay.execution_eligible is False
    assert replay.disabled_agents == (10, 14)


@pytest.mark.parametrize(
    "coverage_fields",
    [
        {},
        {
            "coverage_start_utc": 1_800_000_001.0,
            "coverage_end_utc": 1_800_090_000.0,
        },
        {
            "coverage_start_utc": 1_799_999_000.0,
            "coverage_end_utc": 1_800_000_100.0,
        },
    ],
)
def test_fresh_empty_macro_list_requires_sufficient_coverage(coverage_fields):
    now = 1_800_000_000.0
    envelope = {
        "source_timestamp": now,
        "events": [],
        **coverage_fields,
    }
    provider = MacroHTTPProvider(
        url="https://provider.invalid/calendar",
        transport=FakeGetTransport(HTTPJSONResponse(200, envelope)),
        clock=lambda: now,
    )

    snapshot = run(provider.poll_once())

    assert snapshot.status is ProviderStatus.ERROR
    assert snapshot.execution_eligible_at(now) is False
    assert snapshot.effective_disabled_agents(now) == (10, 14)


def test_empty_macro_calendar_is_healthy_with_verified_coverage():
    now = 1_800_000_000.0
    provider = MacroHTTPProvider(
        url="https://provider.invalid/calendar",
        minimum_forward_coverage_s=3_600,
        transport=FakeGetTransport(
            HTTPJSONResponse(
                200,
                covered_macro_envelope(
                    now,
                    coverage_end_utc=now + 3_600,
                ),
            )
        ),
        clock=lambda: now,
    )

    snapshot = run(provider.poll_once())

    assert snapshot.status is ProviderStatus.HEALTHY
    assert snapshot.payload == {
        "events": [],
        "coverage_start_utc": now - 3_600,
        "coverage_end_utc": now + 3_600,
        "minimum_forward_coverage_s": 3_600,
    }


def test_intermarket_validation_failure_is_error_not_neutral_health():
    now = 1_800_000_000.0
    provider = IntermarketHTTPProvider(
        url="https://provider.invalid/markets",
        transport=FakeGetTransport(
            HTTPJSONResponse(
                200,
                {
                    "source_timestamp": now,
                    "data": {
                        "dxy": 104.2,
                        "us10y": 4.42,
                        "us30": 41_550,
                        "gold": -1,
                    },
                },
            )
        ),
        clock=lambda: now,
    )

    snapshot = run(provider.poll_once())

    assert snapshot.status is ProviderStatus.ERROR
    assert snapshot.provider == "intermarket"
    assert snapshot.disabled_agents == (12, 15)
    assert snapshot.degraded is True
    assert snapshot.error == "ValueError: provider poll failed"


def test_simulation_sources_are_impossible_in_live_mode(monkeypatch):
    monkeypatch.setenv("QSX_MODE", "live")

    with pytest.raises(ProviderSafetyError):
        SimulationProvider("macro", {"events": []})


def test_supplied_simulation_provider_never_generates_data():
    provider = SimulationProvider(
        "macro",
        {"events": []},
        mode="test",
        clock=lambda: 1234.0,
    )

    first = run(provider.poll_once())
    second = run(provider.poll_once())

    assert first.payload == {"events": []}
    assert first.status is ProviderStatus.HEALTHY
    assert first.source_timestamp == 1234.0
    assert first.received_timestamp == 1234.0
    assert first.sequence == 1
    assert second.sequence == 2
