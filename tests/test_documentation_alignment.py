import re
from pathlib import Path

from python_swarm.agent_roster import AGENT_ROSTER


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def _canonical_table_rows() -> list[tuple[int, str]]:
    text = (DOCS / "CANONICAL_ARCHITECTURE.md").read_text(encoding="utf-8")
    return [
        (int(agent_id), name.strip())
        for agent_id, name in re.findall(r"^\|\s*(\d{2})\s*\|\s*([^|]+?)\s*\|", text, re.MULTILINE)
    ]


def test_machine_roster_exactly_matches_normative_document():
    documented = _canonical_table_rows()
    assert documented == [(agent.agent_id, agent.name) for agent in AGENT_ROSTER]


def test_retired_html_manuals_are_non_operational_not_marketing_material():
    files = [
        DOCS / "FULL_DOCUMENTATION.html",
        DOCS / "PRODUCTION_DOCS.html",
        DOCS / "QUANT_SWARM_XAU_CUSTOMERDOCS.html",
    ]
    prohibited = (
        "57%",
        "75,000",
        "production-ready",
        "can never lose",
        "no position is ever left unmanaged",
        "python3 evolved_orchestrator.py",
    )
    for path in files:
        text = path.read_text(encoding="utf-8").lower()
        assert "canonical_architecture.md" in text
        assert any(marker in text for marker in ("retired", "withdrawn"))
        assert not any(claim in text for claim in prohibited)


def test_current_docs_do_not_instruct_legacy_runtime_start():
    for name in ("README.md", "UPGRADE_GUIDE_v3.md"):
        text = (DOCS / name).read_text(encoding="utf-8")
        assert "python3 orchestrator.py" not in text
        assert "python3 evolved_orchestrator.py" not in text
        assert "--live" not in text
        assert "not approved for real-capital trading" in text.lower() or "real-account execution remains blocked" in text.lower()


def test_root_roster_notice_contains_every_canonical_name_in_order():
    text = (ROOT / "Agents Name and Description").read_text(encoding="utf-8")
    offsets = [text.index(f"{agent.agent_id:02d}. {agent.name}") for agent in AGENT_ROSTER]
    assert offsets == sorted(offsets)
    assert "docs/CANONICAL_ARCHITECTURE.md" in text
    assert "evolved_orchestrator.py" not in text
