from pathlib import Path


SOURCE_PATH = Path(__file__).parents[1] / "mql5_ea" / "QuantSwarmXAU.mq5"
SOURCE = SOURCE_PATH.read_text(encoding="utf-8")


def _brace_balance(source: str) -> int:
    """Balance braces while ignoring strings and // or /* */ comments."""
    depth = 0
    index = 0
    in_string = False
    in_line_comment = False
    in_block_comment = False
    escaped = False
    while index < len(source):
        char = source[index]
        nxt = source[index + 1] if index + 1 < len(source) else ""
        if in_line_comment:
            if char == "\n":
                in_line_comment = False
            index += 1
            continue
        if in_block_comment:
            if char == "*" and nxt == "/":
                in_block_comment = False
                index += 2
            else:
                index += 1
            continue
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            index += 1
            continue
        if char == "/" and nxt == "/":
            in_line_comment = True
            index += 2
            continue
        if char == "/" and nxt == "*":
            in_block_comment = True
            index += 2
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            assert depth >= 0, "closing brace without matching opener"
        index += 1
    assert not in_string and not in_block_comment
    return depth


def test_source_has_balanced_structure_and_explicit_unverified_compile_gate():
    assert _brace_balance(SOURCE) == 0
    assert '#property version   "4.01"' in SOURCE
    assert "Native MetaEditor compilation remains a mandatory release gate" in SOURCE


def test_authenticated_epoch_fenced_flat_v4_session_is_mandatory():
    for token in (
        "QSX_PROTOCOL_VERSION \"4.1\"",
        "InpAuthToken",
        "g_ConnectionEpoch",
        "g_SendSequence",
        "g_LastRecvSequence",
        "ValidateIncomingEnvelope",
        "IsFlatJsonObject",
        'messageType != "HELLO_ACK"',
        "g_Authenticated = true",
    ):
        assert token in SOURCE
    assert 'InpAuthToken == ""' in SOURCE


def test_hello_ack_must_authorize_identity_mode_config_and_permissions():
    for token in (
        "broker_server",
        "account_trade_mode",
        "trade_allowed",
        "expert_allowed",
        "session_mode",
        "expected_account_id",
        "expected_broker_server",
        "expected_account_trade_mode",
        "expected_symbol",
        "expected_magic",
        "allow_real",
        "expected_session_mode",
        "peerDealCursor",
        "InpPermitRealAccount",
        "InpSessionMode",
        "configHash == InpConfigHash",
        "TerminalInfoInteger(TERMINAL_TRADE_ALLOWED)",
    ):
        assert token in SOURCE
    assert "if(!identityOk || !modeOk || !sessionModeOk || !permissionOk)" in SOURCE
    assert "bool permissionOk = IsObservationOnlySession() ||" in SOURCE


def test_socket_send_handles_partial_writes_and_inbound_frames_are_bounded():
    assert "while(offset < total)" in SOURCE
    assert "offset += sent" in SOURCE
    assert "SocketSend(g_Socket, chunk, remaining)" in SOURCE
    assert "InpMaxFrameBytes" in SOURCE
    assert "Oversized inbound frame; disconnecting" in SOURCE


def test_atomic_account_complete_snapshots_and_owned_deal_replay_are_present():
    for message_type in (
        "ACCOUNT_SNAPSHOT",
        "SYMBOL_SNAPSHOT",
        "POSITION_SNAPSHOT_BEGIN",
        "POSITION_SNAPSHOT_ITEM",
        "POSITION_SNAPSHOT_END",
        "PENDING_ORDER_SNAPSHOT_BEGIN",
        "PENDING_ORDER_SNAPSHOT_ITEM",
        "PENDING_ORDER_SNAPSHOT_END",
        "DEAL_REPLAY_BEGIN",
        "DEAL_REPLAY_ITEM",
        "DEAL_REPLAY_END",
    ):
        assert f'SendMessage("{message_type}"' in SOURCE
    assert "POSITION_MAGIC" in SOURCE
    assert "POSITION_SYMBOL" in SOURCE
    assert "DEAL_MAGIC" in SOURCE
    assert "DEAL_SYMBOL" in SOURCE
    assert "g_LastDealCursorMs" in SOURCE
    assert "g_LastDealCursorDeal" in SOURCE
    assert "ParseDealCursor" in SOURCE
    assert "replayDeals" in SOURCE and "replayTimes" in SOURCE
    assert "position_version" in SOURCE
    assert "int count = CountAccountPositions();" in SOURCE
    assert "int count = CountAccountPendingOrders();" in SOURCE
    assert "positionSymbol = PositionGetString(POSITION_SYMBOL)" in SOURCE
    assert "orderSymbol = OrderGetString(ORDER_SYMBOL)" in SOURCE
    assert '"UNATTRIBUTED-" + positionId' in SOURCE
    assert '"UNATTRIBUTED-" + orderId' in SOURCE


def test_pending_orders_are_reconciled_and_consume_open_capacity():
    assert "CountOwnedPendingOrders" in SOURCE
    assert "SelectOwnedPendingOrder" in SOURCE
    assert "ORDER_MAGIC" in SOURCE and "ORDER_SYMBOL" in SOURCE
    assert "CountOwnedPositions() + CountOwnedPendingOrders()" in SOURCE
    assert "MAX_OWNED_EXPOSURES" in SOURCE


def test_all_command_actions_have_distinct_handlers_and_idempotent_replay():
    for action, handler in (
        ("OPEN", "HandleOpen"),
        ("MODIFY_SL", "HandleModifySl"),
        ("PARTIAL_CLOSE", "HandlePartialClose"),
        ("CLOSE", "HandleClose"),
        ("CLOSE_ALL", "HandleCloseAll"),
    ):
        assert f'action == "{action}"' in SOURCE
        assert handler in SOURCE
    assert 'action == "REQUEST_SNAPSHOT"' in SOURCE
    assert "ReplayCommandIfKnown" in SOURCE
    assert "PersistCommand" in SOURCE
    assert "FILE_COMMON" in SOURCE
    assert 'record.status = "PENDING"' in SOURCE
    assert "COMMAND_ID_COLLISION" in SOURCE
    assert "PARTIALLY_APPLIED" in SOURCE


def test_open_is_broker_validated_and_never_rounds_volume_up():
    for token in (
        "ValidateOpenGeometry",
        "NormalizeVolumeDown",
        "MathFloor(requested / step",
        "OrderCalcProfit",
        "OrderCalcMargin",
        "ACTUAL_RISK_EXCEEDS_LIMIT",
        "MAX_SLIPPAGE_EXCEEDED",
        "BROKER_STOPS_LEVEL",
        "InpMaxOwnedPositions",
    ):
        assert token in SOURCE
    assert "MathMin(InpMaxRiskPct, commandPct)" in SOURCE
    assert 'AccountMarginModeText() != "HEDGING"' in SOURCE
    assert '"HEDGING_ACCOUNT_REQUIRED"' in SOURCE
    assert '"FOREIGN_ACCOUNT_EXPOSURE"' in SOURCE
    assert "CountAccountPositions() != CountOwnedPositions()" in SOURCE
    assert "CountAccountPendingOrders() != CountOwnedPendingOrders()" in SOURCE


def test_retries_are_limited_to_explicit_retriable_broker_codes():
    assert "IsRetriableRetcode" in SOURCE
    assert "TRADE_RETCODE_REQUOTE" in SOURCE
    assert "TRADE_RETCODE_PRICE_CHANGED" in SOURCE
    assert "TRADE_RETCODE_PRICE_OFF" in SOURCE
    assert "if(!IsRetriableRetcode(retcode)) break" in SOURCE
    assert "COMMAND_NACK" in SOURCE
    assert "RETRY_VALIDATION_FAILED" in SOURCE


def test_modify_partial_and_close_are_position_identity_and_version_fenced():
    for token in (
        "ResolveTicket",
        "POSITION_IDENTIFIER",
        "ValidatePositionVersion",
        "POSITION_VERSION_MISMATCH",
        "PARTIAL_DUST_OR_FULL_CLOSE",
        "PositionClosePartial",
        "PartialCloseNetting",
        "expected_position_version",
    ):
        assert token in SOURCE
    assert "brokerFinalTp" in SOURCE
    assert "FINAL_TP_MISMATCH" in SOURCE
    assert "g_Trade.PositionModify(ticket, newSl, brokerFinalTp)" in SOURCE


def test_trade_transactions_are_authoritative_and_include_costs_and_identity():
    assert "void OnTradeTransaction" in SOURCE
    for token in (
        "DEAL_POSITION_ID",
        "DEAL_PROFIT",
        "DEAL_COMMISSION",
        "DEAL_SWAP",
        "DEAL_FEE",
        "DEAL_REASON",
        "DEAL_TIME_MSC",
        "remaining_volume",
        "volume_delta",
        "position_version",
        "ResultPrice()",
        "ResultOrder()",
        "ResultDeal()",
    ):
        assert token in SOURCE


def test_historical_cache_uses_exact_ids_or_a_narrow_modify_fingerprint():
    start = SOURCE.index("void ContextForIdentity")
    end = SOURCE.index("\ndouble RemainingVolumeByIdentifier", start)
    body = SOURCE[start:end]
    cache_start = body.index("for(int i = ArraySize(g_CommandCache) - 1")
    cache_fallback = body[cache_start:]

    assert 'orderId != "0" && g_CommandCache[i].order_id == orderId' in cache_fallback
    assert 'dealId != "0" && g_CommandCache[i].deal_id == dealId' in cache_fallback
    assert "CachedModifyContextForPosition(positionId, record)" in cache_fallback
    assert 'record.command_id = "BROKER"' in cache_fallback
    assert 'record.trade_id = "UNATTRIBUTED-" + positionId' in cache_fallback

    fingerprint_start = SOURCE.index("bool CachedModifyContextForPosition")
    fingerprint_end = SOURCE.index("\nvoid ContextForIdentity", fingerprint_start)
    fingerprint = SOURCE[fingerprint_start:fingerprint_end]
    for proof in (
        'candidate.action != "MODIFY_SL"',
        "candidate.position_id != positionId",
        "candidate.expected_position_version < 1",
        "currentVersion <= candidate.expected_position_version",
        "MathAbs(currentSl - candidate.target_sl) > tolerance",
        "MathAbs(currentTp - candidate.final_tp) > tolerance",
        "if(match >= 0) return false",
    ):
        assert proof in fingerprint


def test_active_position_wildcard_is_limited_to_the_open_command():
    start = SOURCE.index("void ContextForIdentity")
    end = SOURCE.index("\ndouble RemainingVolumeByIdentifier", start)
    body = SOURCE[start:end]

    assert 'g_ActiveCommand.position_id == positionId' in body
    assert 'g_ActiveCommand.action == "OPEN"' in body
    assert "g_ActiveCommand.position_id == \"\" || g_ActiveCommand.position_id == \"0\"" in body


def test_legacy_position_count_close_polling_and_v3_messages_are_removed():
    assert "g_LastPosCount" not in SOURCE
    assert "TRADE_CLOSED" not in SOURCE
    assert "EXEC_ACK" not in SOURCE
    assert "EXEC_FAIL" not in SOURCE
    assert '"version":"3.0"' not in SOURCE


def test_daily_baseline_rolls_by_utc_date_and_persists_halt_state():
    for token in (
        "TimeGMT()",
        "UtcRiskDay",
        "BaselineGlobalKey",
        "HaltGlobalKey",
        "GlobalVariableSet",
        "UpdateUtcRiskDay",
        "BaselineVerifiedGlobalKey",
        "g_BaselineVerified",
        "BASELINE_UNVERIFIED",
    ):
        assert token in SOURCE


def test_daily_baseline_rollover_requires_verified_prior_state_and_durable_monotonic_day():
    start = SOURCE.index("bool UpdateUtcRiskDay")
    end = SOURCE.index("\nbool SetDailyHalt", start)
    body = SOURCE[start:end]

    assert "ReadUtcRiskDay(day, dayOrdinal)" in body
    assert "RiskDayHighWaterGlobalKey()" in body
    assert "InpApproveRiskDayHighWaterUtcDay" in body
    assert 'MarkLocalStateFailure("UTC_RISK_DAY_MOVED_BACKWARD")' in body
    assert (
        'MarkLocalStateFailure("UTC_ROLLOVER_PRIOR_BASELINE_UNVERIFIED")'
        in body
    )
    assert "dayOrdinal < durableHighWater" in body
    assert "dayOrdinal < g_RiskDayOrdinal" in body
    assert (
        "bool continuousRollover = g_RiskDayOrdinal > 0 && "
        "dayOrdinal > g_RiskDayOrdinal && "
        "g_BaselineVerified;"
    ) in body
    assert "verified = continuousRollover;" in body
    assert "SetVerifiedGlobal(highWaterKey, (double)dayOrdinal)" in body


def test_observation_only_is_default_and_locally_fences_every_mutating_command():
    assert "InpSessionMode          = QSX_SESSION_SHADOW" in SOURCE
    assert "InpEmergencyStop        = true" in SOURCE
    assert "bool IsObservationOnlySession()" in SOURCE
    assert 'return action == "REQUEST_SNAPSHOT";' in SOURCE

    start = SOURCE.index("void HandleCommand")
    end = SOURCE.index("\nvoid HandleHelloAck", start)
    body = SOURCE[start:end]
    fence = (
        "if(IsObservationOnlySession() && "
        "!IsObservationCommand(action))"
    )
    assert fence in body
    assert '"SHADOW_OBSERVATION_ONLY"' in body
    assert body.index(fence) < body.index("ReplayCommandIfKnown")
    assert body.index(fence) < body.index("RejectAndStore")
    for handler in (
        "HandleOpen(json",
        "HandleModifySl(json",
        "HandlePartialClose(json",
        "HandleClose(json",
        "HandleCloseAll(json",
    ):
        assert body.index(fence) < body.index(handler)


def test_command_journal_is_verified_before_every_broker_mutation():
    persist_start = SOURCE.index("bool PersistCommand")
    persist_end = SOURCE.index("\nbool CompactCommandCache", persist_start)
    persist = SOURCE[persist_start:persist_end]
    assert "FileWriteString" in persist
    assert "FileFlush(file)" in persist
    assert persist.count("FileOpen(CacheFileName()") == 2
    assert "return last == expected" in persist

    begin_start = SOURCE.index("bool BeginActiveCommand")
    begin_end = SOURCE.index("\nvoid EndActiveCommand", begin_start)
    begin = SOURCE[begin_start:begin_end]
    assert "if(!StoreCommand(record)) return false" in begin
    assert begin.index("StoreCommand(record)") < begin.index("g_HasActiveCommand = true")

    for mutation in (
        "g_Trade.Buy(",
        "g_Trade.Sell(",
        "g_Trade.PositionModify(",
        "g_Trade.PositionClosePartial(",
        "OrderSend(request, result)",
        "g_Trade.PositionClose(",
    ):
        assert mutation in SOURCE
    assert SOURCE.count("if(!BeginActiveCommand(record))") == 5
    assert "LOCAL_COMMAND_PERSISTENCE_FAILED" in SOURCE
    assert "g_LocalStateHealthy" in SOURCE


def test_durable_pending_replay_is_unknown_never_a_terminal_rejection():
    start = SOURCE.index("bool ReplayCommandIfKnown")
    end = SOURCE.index("\n//+------------------------------------------------------------------+\n//| Broker metadata", start)
    replay = SOURCE[start:end]
    pending = replay[replay.index('else if(record.status == "PENDING")') :]
    pending = pending[: pending.index("\n   else\n")]
    assert "DisconnectSocket()" in pending
    assert "SendNackValues" not in pending
    assert "terminal NACK would falsely prove non-execution" in pending
    assert "COMMAND_IN_PROGRESS" not in SOURCE
    assert 'action == "CLOSE_ALL" && known >= 0' not in SOURCE


def test_risk_cursor_and_position_versions_are_verified_and_commit_fenced():
    for token in (
        "SetVerifiedGlobal",
        "ReadVerifiedGlobal",
        "GlobalVariablesFlush",
        "CursorCommitGlobalKey",
        "CursorCommitValue",
        "LoadDealCursorState",
        "DEAL_CURSOR_TORN_OR_UNVERIFIED",
        "PositionVersionCommitGlobalKey",
        "PositionVersionCommitValue",
        "POSITION_VERSION_TORN_STATE",
        "DAILY_BASELINE_WRITE_OR_VERIFY_FAILED",
        "DAILY_HALT_WRITE_OR_VERIFY_FAILED",
    ):
        assert token in SOURCE


def test_position_version_is_persistent_monotonic_and_same_ms_safe():
    start = SOURCE.index("long PositionVersionForIdentity")
    end = SOURCE.index("\nlong CurrentPositionVersion", start)
    body = SOURCE[start:end]
    assert "version = brokerTimeMs * 1000" in body
    assert "if(version <= storedVersion) version = storedVersion + 1" in body
    assert "PersistPositionVersionState" in body
    assert "PositionVersionForIdentity(positionId, brokerTime, true)" in SOURCE
    assert "PositionVersionForIdentity(positionId, brokerUpdateTime, true)" in SOURCE
    assert "replayVersionOrdinal++" in SOURCE
    assert "dealTime * 1000 + replayVersionOrdinal" in SOURCE


def test_daily_halt_never_invents_an_untracked_close_command():
    assert "EA-DAILY-HALT-" not in SOURCE
    assert "InpCloseOnDailyHalt" not in SOURCE
    assert "new risk blocked, existing broker protection retained" in SOURCE
    assert "the EA must never invent an untracked command_id" in SOURCE


def test_cache_compacts_without_evicting_live_broker_identity():
    assert "CompactCommandCache" in SOURCE
    assert "CommandProtectsOpenPosition" in SOURCE
    assert "FILE_REWRITE" in SOURCE
    assert "g_CommandWritesSinceCompact" in SOURCE


def test_unsigned_identity_strict_shape_and_local_transport_are_fail_closed():
    assert "ParseUlongText" in SOURCE and "ULONG_MAX" in SOURCE
    assert "StringToInteger(ticketText)" not in SOURCE
    assert "RequireUniqueField" in SOURCE and "ValidateCommandShape" in SOURCE
    assert "IsLoopbackHost" in SOURCE
    assert "InpDailyMaxDrawPct > 3.0" in SOURCE
    assert "InpMaxOwnedPositions > 3" in SOURCE
    assert "DisconnectSocket();" in SOURCE


def test_replay_does_not_misstate_final_volume_as_historical_remaining():
    assert '"remaining_volume\\\":0.0"' in SOURCE
    assert '"remaining_volume_known\\\":false"' in SOURCE


def test_completed_bars_continue_after_connect_without_silent_catchup_loss():
    for token in (
        "SendCompletedBarUpdates();",
        "SendNewCompletedBarsTf",
        "SeedBarPublicationState",
        "iBarShift(_Symbol, timeframe, lastSent, true)",
        "startPosition = pending - count + 1",
        "if(rates[i].time <= lastSent) continue;",
        "CopyRates(_Symbol, timeframe, 1, requested, rates)",
    ):
        assert token in SOURCE
    assert "CopyRates(_Symbol, timeframe, startPosition, count, rates)" in SOURCE
    assert "Completed-bar cursor is no longer present" in SOURCE
