from __future__ import annotations

import ast
from pathlib import Path

import pytest

from python_swarm.trade_ledger import (
    LedgerInvariantError,
    _EXACT_INSERT_TABLES,
    _exact_insert_table,
    _sql_identifier,
)


ROOT = Path(__file__).resolve().parents[1]
LEDGER_SOURCE = ROOT / "python_swarm" / "trade_ledger.py"


def test_sql_identifier_quotes_valid_internal_names():
    assert _sql_identifier("account_id") == '"account_id"'
    assert _sql_identifier("trade_events") == '"trade_events"'


@pytest.mark.parametrize(
    "value",
    [
        "",
        "trades; DROP TABLE trades--",
        "account_id OR 1=1",
        "account-id",
        'trades"',
        "trades schema",
        "1invalid",
        "../trades",
    ],
)
def test_sql_identifier_rejects_injection_fragments(value):
    with pytest.raises(LedgerInvariantError):
        _sql_identifier(value)


def test_exact_insert_table_is_strictly_allowlisted():
    for table in _EXACT_INSERT_TABLES:
        assert _exact_insert_table(table) == f'"{table}"'

    with pytest.raises(LedgerInvariantError):
        _exact_insert_table("sqlite_master")

    with pytest.raises(LedgerInvariantError):
        _exact_insert_table("trades; DROP TABLE trades--")


def test_every_insert_exact_call_uses_a_literal_approved_table():
    tree = ast.parse(
        LEDGER_SOURCE.read_text(encoding="utf-8")
    )
    calls = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        if not isinstance(node.func, ast.Attribute):
            continue

        if node.func.attr != "_insert_exact":
            continue

        assert len(node.args) >= 2, (
            f"_insert_exact call at line {node.lineno} "
            "does not include a table argument"
        )

        table_argument = node.args[1]

        assert isinstance(table_argument, ast.Constant), (
            f"_insert_exact table at line {node.lineno} "
            "must be a string literal"
        )
        assert isinstance(table_argument.value, str)
        assert table_argument.value in _EXACT_INSERT_TABLES, (
            f"unapproved _insert_exact table "
            f"{table_argument.value!r} at line {node.lineno}"
        )

        calls.append(table_argument.value)

    assert calls
    assert set(calls) <= _EXACT_INSERT_TABLES


def test_dynamic_sql_contains_no_record_value_interpolation():
    source = LEDGER_SOURCE.read_text(encoding="utf-8")

    forbidden_fragments = (
        "f\"SELECT * FROM {record.",
        "f\"INSERT INTO {record.",
        "f\"UPDATE {record.",
        "f\"DELETE FROM {record.",
        "f'''SELECT * FROM {record.",
        'f"""SELECT * FROM {record.',
    )

    for fragment in forbidden_fragments:
        assert fragment not in source
