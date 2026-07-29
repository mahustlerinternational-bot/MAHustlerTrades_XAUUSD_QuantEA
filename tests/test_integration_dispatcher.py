import asyncio
import hashlib
import hmac
import json
import sys
from types import SimpleNamespace

import pytest

from integration_dispatcher import (
    AiohttpPostTransport,
    DiscordPublisher,
    DomainEvent,
    EVENT_SCHEMA_VERSION,
    HTTPPostResponse,
    IntegrationDispatcher,
    IntegrationConfigurationError,
    LedgerOutboxStore,
    OutboxMessage,
    OutboxStoreError,
    MAX_EVENT_BYTES,
    MAX_RESPONSE_BODY_BYTES,
    SignedWebsiteWebhookPublisher,
    TelegramPublisher,
    build_publishers,
    outbox_records_for_event,
)
from runtime_config import IntegrationsSettings
from trade_ledger import OutboxRecord, TradeEventRecord, TradeLedger


def run(coroutine):
    return asyncio.run(coroutine)


class MemoryOutbox:
    def __init__(self, messages=(), *, fail_claim=False, fail_updates=False):
        self.messages = list(messages)
        self.fail_claim = fail_claim
        self.fail_updates = fail_updates
        self.claims = []
        self.delivered = []
        self.retries = []
        self.dead_letters = []

    async def claim_due(self, *, now, limit):
        if self.fail_claim:
            raise RuntimeError("database unavailable")
        self.claims.append((now, limit))
        return self.messages[:limit]

    async def mark_delivered(self, message_id, *, delivered_at, detail):
        if self.fail_updates:
            raise RuntimeError("write failed")
        self.delivered.append((message_id, delivered_at, detail))

    async def mark_retry(self, message_id, *, attempts, next_attempt_at, error):
        if self.fail_updates:
            raise RuntimeError("write failed")
        self.retries.append((message_id, attempts, next_attempt_at, error))

    async def mark_dead_letter(self, message_id, *, attempts, failed_at, error):
        if self.fail_updates:
            raise RuntimeError("write failed")
        self.dead_letters.append((message_id, attempts, failed_at, error))


class FakePublisher:
    name = "fake"

    def __init__(self, error=None):
        self.error = error
        self.events = []

    async def publish(self, event):
        self.events.append(event)
        if self.error:
            raise self.error
        return "delivered without credential detail"


class FakePostTransport:
    def __init__(self, response=None):
        self.response = response or HTTPPostResponse(200, {"ok": True})
        self.calls = []

    async def post_json(self, url, payload, *, headers, timeout_s):
        self.calls.append({
            "kind": "json",
            "url": url,
            "payload": dict(payload),
            "headers": dict(headers),
            "timeout_s": timeout_s,
        })
        return self.response

    async def post_bytes(self, url, body, *, headers, timeout_s):
        self.calls.append({
            "kind": "bytes",
            "url": url,
            "body": body,
            "headers": dict(headers),
            "timeout_s": timeout_s,
        })
        return self.response


def event(**overrides):
    values = {
        "event_id": "evt-1",
        "event_type": "trade.opened",
        "severity": "INFO",
        "payload": {"trade_id": "trade-1"},
        "occurred_at": 99.0,
    }
    values.update(overrides)
    return DomainEvent(**values)


def test_dispatch_success_marks_durable_outbox_delivered():
    message = OutboxMessage("msg-1", "telegram", event())
    store = MemoryOutbox([message])
    publisher = FakePublisher()
    dispatcher = IntegrationDispatcher(
        store, {"telegram": publisher}, clock=lambda: 100.0
    )

    processed = run(dispatcher.dispatch_once())

    assert processed == 1
    assert publisher.events == [message.event]
    assert store.delivered == [
        ("msg-1", 100.0, "delivered without credential detail")
    ]
    assert store.retries == []
    assert store.dead_letters == []
    assert dispatcher.latest_health.status == "HEALTHY"
    assert dispatcher.latest_health.delivered == 1


def test_structured_projection_records_are_deterministic_redacted_and_unique():
    domain_event = event(
        event_type="trade.partial_filled",
        trade_id="trade-1",
        payload={"remaining_volume": "0.02", "token": "must-not-leak"},
    )
    first = outbox_records_for_event(
        domain_event, ("website", "telegram", "discord")
    )
    second = outbox_records_for_event(
        domain_event, ("discord", "website", "telegram")
    )

    assert first == second
    assert [record.destination for record in first] == [
        "discord",
        "telegram",
        "website",
    ]
    assert len({record.message_id for record in first}) == 3
    assert all(record.event_id == "evt-1" for record in first)
    assert all(
        record.payload["schema_version"] == EVENT_SCHEMA_VERSION for record in first
    )
    assert all(record.payload["payload"]["token"] == "[REDACTED]" for record in first)
    with pytest.raises(ValueError, match="unique"):
        outbox_records_for_event(domain_event, ("telegram", "telegram"))
    with pytest.raises(ValueError, match="supported"):
        outbox_records_for_event(domain_event, ("broker",))


def test_event_v1_contract_is_strict_immutable_and_recovery_projection_has_parity():
    mutable = {"closing_deal_ids": ["deal-1"]}
    recovered = event(
        event_id="reconciliation-evt-1",
        event_type="reconciliation.position_closed",
        trade_id="trade-1",
        payload=mutable,
    )
    records = outbox_records_for_event(
        recovered, ("telegram", "discord", "website")
    )

    mutable["closing_deal_ids"].append("deal-2")
    assert recovered.payload["closing_deal_ids"] == ("deal-1",)
    assert {record.destination for record in records} == {
        "telegram",
        "discord",
        "website",
    }
    assert all(
        record.topic == "reconciliation.position_closed" for record in records
    )
    assert all(record.payload == recovered.sanitized_dict() for record in records)
    records[0].payload["payload"]["closing_deal_ids"].append("local-mutation")
    assert records[1].payload["payload"]["closing_deal_ids"] == ["deal-1"]

    with pytest.raises(ValueError, match="schema_version"):
        event(schema_version=2)
    with pytest.raises(ValueError, match="canonical lowercase"):
        event(event_type="TRADE_OPENED")
    with pytest.raises(ValueError, match="severity"):
        event(severity="info")
    with pytest.raises(ValueError, match="non-finite"):
        event(payload={"risk": float("nan")})
    with pytest.raises(ValueError, match="size limit"):
        event(payload={"message": "x" * MAX_EVENT_BYTES})


def test_dispatch_failure_retries_with_bounded_deterministic_backoff():
    message = OutboxMessage("msg-1", "discord", event(), attempts=0)
    store = MemoryOutbox([message])
    dispatcher = IntegrationDispatcher(
        store,
        {"discord": FakePublisher(RuntimeError("remote rejected request"))},
        max_attempts=3,
        base_delay_s=2,
        jitter_ratio=0,
        clock=lambda: 100.0,
    )

    run(dispatcher.dispatch_once())

    assert len(store.retries) == 1
    message_id, attempts, next_attempt, error = store.retries[0]
    assert (message_id, attempts, next_attempt) == ("msg-1", 1, 102.0)
    assert error == "RuntimeError: remote rejected request"
    assert store.dead_letters == []
    assert dispatcher.latest_health.status == "DEGRADED"
    assert dispatcher.latest_health.retrying == 1


def test_retry_after_is_honored_but_bounded_and_exhaustion_is_dead_lettered():
    response = HTTPPostResponse(429, {}, {"Retry-After": "600"})
    transport = FakePostTransport(response)
    publisher = DiscordPublisher(
        webhook_url="https://discord.invalid/unit", transport=transport
    )
    retry = OutboxMessage("msg-retry", "discord", event())
    exhausted = OutboxMessage(
        "msg-exhausted", "discord", event(event_id="evt-exhausted"), attempts=2
    )
    store = MemoryOutbox([retry, exhausted])
    dispatcher = IntegrationDispatcher(
        store,
        {"discord": publisher},
        max_attempts=3,
        base_delay_s=2,
        max_delay_s=30,
        jitter_ratio=0,
        clock=lambda: 100.0,
    )

    assert run(dispatcher.dispatch_once()) == 2

    assert store.retries[0][:3] == ("msg-retry", 1, 130.0)
    assert store.dead_letters[0][:2] == ("msg-exhausted", 3)
    assert dispatcher.latest_health.status == "DEGRADED"
    assert dispatcher.latest_health.retrying == 1
    assert dispatcher.latest_health.dead_lettered == 1


def test_remote_errors_redact_urls_and_secret_assignments_before_persistence():
    secret_url = "https://discord.com/api/webhooks/123/super-secret?token=private"
    message = OutboxMessage("msg-1", "discord", event())
    store = MemoryOutbox([message])
    dispatcher = IntegrationDispatcher(
        store,
        {
            "discord": FakePublisher(
                RuntimeError(f"request to {secret_url} failed; api_key=also-private")
            )
        },
        max_attempts=2,
        jitter_ratio=0,
        clock=lambda: 100.0,
    )

    run(dispatcher.dispatch_once())

    persisted_error = store.retries[0][3]
    assert "super-secret" not in persisted_error
    assert "private" not in persisted_error
    assert "discord.com" not in persisted_error
    assert "[REDACTED_URL]" in persisted_error


def test_dispatch_exhaustion_and_unknown_destination_go_to_dead_letter():
    exhausted = OutboxMessage("msg-1", "discord", event(), attempts=2)
    unknown = OutboxMessage("msg-2", "pager", event(event_id="evt-2"))
    store = MemoryOutbox([exhausted, unknown])
    dispatcher = IntegrationDispatcher(
        store,
        {"discord": FakePublisher(RuntimeError("still unavailable"))},
        max_attempts=3,
        clock=lambda: 100.0,
    )

    processed = run(dispatcher.dispatch_once())

    assert processed == 2
    assert [(item[0], item[1]) for item in store.dead_letters] == [
        ("msg-1", 3),
        ("msg-2", 1),
    ]
    assert store.retries == []


def test_claim_and_store_update_failures_are_isolated_from_trading_path():
    claim_failure = MemoryOutbox(fail_claim=True)
    dispatcher = IntegrationDispatcher(claim_failure, {})
    assert run(dispatcher.dispatch_once()) == 0
    assert dispatcher.latest_health.status == "ERROR"
    assert dispatcher.latest_health.error_type == "RuntimeError"

    message = OutboxMessage("msg-1", "telegram", event())
    update_failure = MemoryOutbox([message], fail_updates=True)
    dispatcher = IntegrationDispatcher(
        update_failure, {"telegram": FakePublisher()}
    )
    assert run(dispatcher.dispatch_once()) == 1
    assert dispatcher.latest_health.status == "ERROR"
    assert dispatcher.latest_health.error_type == "OutboxStoreError"


def _seed_ledger_outbox(ledger, *, message_id="msg-ledger", payload=None):
    event_id = f"event-{message_id}"
    ledger.append_event(TradeEventRecord(
        event_id=event_id,
        event_type="TRADE_OPENED",
        source="unit-test",
        occurred_at=90.0,
        payload={},
    ))
    ledger.enqueue_outbox(OutboxRecord(
        message_id=message_id,
        idempotency_key=f"idempotency-{message_id}",
        event_id=event_id,
        destination="telegram",
        topic="trade.opened",
        payload=(
            DomainEvent(
                event_id=event_id,
                event_type="trade.opened",
                severity="INFO",
                occurred_at=90.0,
                trade_id="trade-unit",
                payload={"trade_id": "trade-unit"},
            ).sanitized_dict()
            if payload is None
            else payload
        ),
        created_at=90.0,
    ))


def test_ledger_outbox_adapter_delivers_with_lease_and_attempt_semantics(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.db", clock=lambda: 100.0).open()
    try:
        _seed_ledger_outbox(ledger)
        store = LedgerOutboxStore(
            ledger, worker_id="notification-worker", clock=lambda: 100.0
        )
        publisher = FakePublisher()
        dispatcher = IntegrationDispatcher(
            store, {"telegram": publisher}, clock=lambda: 100.0
        )

        assert run(dispatcher.dispatch_once()) == 1

        assert dispatcher.latest_health.status == "HEALTHY"
        assert publisher.events[0].event_type == "trade.opened"
        assert publisher.events[0].trade_id == "trade-unit"
        assert publisher.events[0].payload == {"trade_id": "trade-unit"}
        assert ledger.claim_outbox("other-worker", now=200.0) == []
    finally:
        ledger.close()


def test_ledger_outbox_adapter_preserves_retry_schedule(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.db", clock=lambda: 100.0).open()
    try:
        _seed_ledger_outbox(ledger)
        store = LedgerOutboxStore(
            ledger, worker_id="notification-worker", clock=lambda: 100.0
        )
        dispatcher = IntegrationDispatcher(
            store,
            {"telegram": FakePublisher(RuntimeError("remote unavailable"))},
            base_delay_s=2,
            jitter_ratio=0,
            clock=lambda: 100.0,
        )

        assert run(dispatcher.dispatch_once()) == 1
        assert dispatcher.latest_health.status == "DEGRADED"
        assert ledger.claim_outbox("other-worker", now=101.0) == []
        reclaimed = ledger.claim_outbox("other-worker", now=102.0)
        assert reclaimed[0]["attempts"] == 2
        ledger.mark_outbox_failed(
            "msg-ledger", "other-worker", "test cleanup", retry_at=None, now=102.0
        )
    finally:
        ledger.close()


def test_ledger_row_decode_failure_is_individually_dead_lettered(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.db", clock=lambda: 100.0).open()
    try:
        _seed_ledger_outbox(
            ledger,
            payload={"occurred_at": "invalid", "trade_id": "trade-unit"},
        )
        store = LedgerOutboxStore(
            ledger, worker_id="notification-worker", clock=lambda: 100.0
        )
        publisher = FakePublisher()
        dispatcher = IntegrationDispatcher(
            store, {"telegram": publisher}, clock=lambda: 100.0
        )

        assert run(dispatcher.dispatch_once()) == 1

        assert dispatcher.latest_health.status == "DEGRADED"
        assert dispatcher.latest_health.dead_lettered == 1
        assert publisher.events == []
        assert ledger.claim_outbox("other-worker", now=200.0) == []
    finally:
        ledger.close()


def test_ledger_decoder_refuses_unknown_structured_event_schema(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.db", clock=lambda: 100.0).open()
    try:
        unsupported = event(
            event_id="event-msg-ledger",
            event_type="trade.opened",
        ).sanitized_dict()
        unsupported["schema_version"] = EVENT_SCHEMA_VERSION + 1
        _seed_ledger_outbox(ledger, payload=unsupported)
        store = LedgerOutboxStore(
            ledger, worker_id="notification-worker", clock=lambda: 100.0
        )
        publisher = FakePublisher()
        dispatcher = IntegrationDispatcher(
            store, {"telegram": publisher}, clock=lambda: 100.0
        )

        assert run(dispatcher.dispatch_once()) == 1

        assert publisher.events == []
        assert dispatcher.latest_health.dead_lettered == 1
        assert dispatcher.latest_health.status == "DEGRADED"
    finally:
        ledger.close()


def test_malformed_ledger_payload_does_not_poison_valid_batch_peer(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.db", clock=lambda: 100.0).open()
    try:
        _seed_ledger_outbox(
            ledger,
            message_id="msg-malformed",
            payload={"occurred_at": "invalid"},
        )
        _seed_ledger_outbox(ledger, message_id="msg-valid")
        store = LedgerOutboxStore(
            ledger, worker_id="notification-worker", clock=lambda: 100.0
        )
        publisher = FakePublisher()
        dispatcher = IntegrationDispatcher(
            store, {"telegram": publisher}, clock=lambda: 100.0
        )

        assert run(dispatcher.dispatch_once()) == 2

        assert len(publisher.events) == 1
        assert publisher.events[0].event_id == "event-msg-valid"
        assert dispatcher.latest_health.status == "DEGRADED"
        assert dispatcher.latest_health.delivered == 1
        assert dispatcher.latest_health.dead_lettered == 1
        assert ledger.claim_outbox("other-worker", now=200.0) == []
    finally:
        ledger.close()


class FalseAcknowledgementLedger:
    def claim_outbox(self, worker_id, *, limit, lease_seconds, now):
        return [{
            "message_id": "msg-false",
            "destination": "telegram",
            "topic": "trade.opened",
            "payload": {"trade_id": "trade-unit"},
            "event_id": "event-false",
            "created_at": 90.0,
            "attempts": 1,
            "status": "IN_FLIGHT",
            "locked_by": worker_id,
        }]

    def mark_outbox_delivered(self, message_id, worker_id, *, delivered_at):
        return False

    def mark_outbox_failed(
        self, message_id, worker_id, error, *, retry_at, now
    ):
        return False


def test_ledger_false_mark_acknowledgements_are_errors():
    store = LedgerOutboxStore(
        FalseAcknowledgementLedger(),
        worker_id="notification-worker",
        clock=lambda: 100.0,
    )

    with pytest.raises(OutboxStoreError, match="negative acknowledgement"):
        run(store.mark_delivered("msg-false", delivered_at=100.0, detail="ok"))
    with pytest.raises(OutboxStoreError, match="negative acknowledgement"):
        run(store.mark_retry(
            "msg-false",
            attempts=1,
            next_attempt_at=102.0,
            error="retry",
        ))
    with pytest.raises(OutboxStoreError, match="negative acknowledgement"):
        run(store.mark_dead_letter(
            "msg-false", attempts=1, failed_at=100.0, error="failed"
        ))

    dispatcher = IntegrationDispatcher(
        store, {"telegram": FakePublisher()}, clock=lambda: 100.0
    )
    assert run(dispatcher.dispatch_once()) == 1
    assert dispatcher.latest_health.status == "ERROR"
    assert dispatcher.latest_health.error_type == "OutboxStoreError"


def test_closed_ledger_failure_is_surfaced_without_escaping_dispatcher(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.db").open()
    ledger.close()
    store = LedgerOutboxStore(ledger, worker_id="notification-worker")
    dispatcher = IntegrationDispatcher(store, {})

    assert run(dispatcher.dispatch_once()) == 0
    assert dispatcher.latest_health.status == "ERROR"
    assert dispatcher.latest_health.error_type == "OutboxStoreError"


def test_telegram_publisher_escapes_html_redacts_secrets_and_stays_bounded():
    transport = FakePostTransport()
    publisher = TelegramPublisher(
        token="unit-token", chat_id="unit-chat", transport=transport
    )
    malicious = event(
        event_type="security.alert",
        payload={"message": "<script>alert(1)</script>" * 500, "api_key": "private"},
    )

    run(publisher.publish(malicious))

    payload = transport.calls[0]["payload"]
    assert payload["parse_mode"] == "HTML"
    assert "<b>security.alert</b>" in payload["text"]
    assert "&lt;script&gt;" in payload["text"]
    assert "private" not in payload["text"]
    assert "[REDACTED]" in payload["text"]
    assert len(payload["text"]) <= 4096
    assert payload["text"].endswith("</code>")


def test_telegram_requires_an_explicit_positive_api_acknowledgement():
    transport = FakePostTransport(HTTPPostResponse(200, {}))
    publisher = TelegramPublisher(
        token="unit-token", chat_id="unit-chat", transport=transport
    )

    with pytest.raises(RuntimeError, match="delivery failed"):
        run(publisher.publish(event()))


def test_discord_publisher_disables_mentions_and_neutralizes_code_fences():
    transport = FakePostTransport(HTTPPostResponse(204, {}))
    publisher = DiscordPublisher(
        webhook_url="https://discord.invalid/unit", transport=transport
    )

    run(publisher.publish(event(payload={"text": "``` @everyone"})))

    payload = transport.calls[0]["payload"]
    assert payload["allowed_mentions"] == {"parse": []}
    assert "``` @everyone" not in payload["content"]
    assert len(payload["content"]) <= 2000


def test_aiohttp_transport_caps_response_body_before_json_decode(monkeypatch):
    class FakeContent:
        async def readexactly(self, count):
            assert count == MAX_RESPONSE_BODY_BYTES + 1
            return b"x" * count

    class FakeResponse:
        status = 502
        headers = {}
        content_length = None
        content = FakeContent()

        def __init__(self):
            self.closed = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def close(self):
            self.closed = True

    response = FakeResponse()

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def post(self, url, *, data, headers, allow_redirects):
            assert url == "https://events.example.invalid/unit"
            assert isinstance(data, bytes)
            assert allow_redirects is False
            return response

    fake_aiohttp = SimpleNamespace(
        ClientTimeout=lambda *, total: ("timeout", total),
        ClientSession=lambda *, timeout: FakeSession(),
    )
    monkeypatch.setitem(sys.modules, "aiohttp", fake_aiohttp)

    result = run(
        AiohttpPostTransport().post_json(
            "https://events.example.invalid/unit",
            {"value": 1},
            headers={},
            timeout_s=2.0,
        )
    )

    assert result.status == 502
    assert result.payload == {}
    assert result.body_truncated is True
    assert response.closed is True


def test_publisher_factory_wires_only_explicit_external_destinations():
    transport = FakePostTransport()
    settings = IntegrationsSettings(telegram=True, discord=True, website=True)
    environment = {
        "QSX_TG_TOKEN": "123456789:" + "a" * 30,
        "QSX_TG_CHAT": "-100123",
        "QSX_DISCORD_WEBHOOK_URL": "https://discord.com/api/webhooks/1/unit",
        "QSX_WEBSITE_EVENT_URL": "https://events.example.invalid/quant-swarm",
        "QSX_WEBSITE_EVENT_SECRET": "s" * 32,
    }

    publishers = build_publishers(settings, environment, transport=transport)

    assert tuple(publishers) == ("telegram", "discord", "website")
    with pytest.raises(TypeError):
        publishers["extra"] = FakePublisher()
    with pytest.raises(IntegrationConfigurationError, match="Telegram"):
        build_publishers(IntegrationsSettings(telegram=True), {})


def test_publisher_factory_rejects_discord_lookalike_and_weak_website_secret():
    with pytest.raises(IntegrationConfigurationError, match="Discord"):
        build_publishers(
            IntegrationsSettings(discord=True),
            {"QSX_DISCORD_WEBHOOK_URL": "https://discord.com.evil.invalid/hook"},
        )
    with pytest.raises(IntegrationConfigurationError, match="too short"):
        build_publishers(
            IntegrationsSettings(website=True),
            {
                "QSX_WEBSITE_EVENT_URL": "https://events.example.invalid/hook",
                "QSX_WEBSITE_EVENT_SECRET": "short",
            },
        )


def test_website_webhook_is_structured_redacted_and_hmac_signed():
    transport = FakePostTransport()
    secret = "unit-signing-key"
    publisher = SignedWebsiteWebhookPublisher(
        url="https://website.invalid/events",
        signing_secret=secret,
        transport=transport,
    )
    domain_event = event(payload={"token": "private", "nested": {"value": 7}})

    run(publisher.publish(domain_event))

    call = transport.calls[0]
    assert call["kind"] == "bytes"
    transmitted = json.loads(call["body"])
    assert transmitted["schema_version"] == EVENT_SCHEMA_VERSION
    assert transmitted["payload"]["token"] == "[REDACTED]"
    canonical = json.dumps(
        transmitted,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()
    assert call["body"] == canonical
    expected = hmac.new(secret.encode(), call["body"], hashlib.sha256).hexdigest()
    assert call["headers"]["X-Quant-Swarm-Signature"] == f"sha256={expected}"
    assert call["headers"]["X-Quant-Swarm-Event-ID"] == "evt-1"
    assert call["headers"]["X-Quant-Swarm-Schema-Version"] == "1"
