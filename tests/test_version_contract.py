from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from python_swarm import __version__
from python_swarm.broker_protocol import PROTOCOL_VERSION
from python_swarm.runtime_app import SUPPORTED_LAUNCH_MODES
from python_swarm.runtime_config import CONFIG_SCHEMA_VERSION, RuntimeMode
from python_swarm.version import (
    BROKER_PROTOCOL_VERSION,
    CONFIG_SCHEMA_VERSION as CONTRACT_CONFIG_SCHEMA_VERSION,
    MQL5_EA_VERSION,
    PRODUCT_VERSION,
    VERSION_CONTRACT,
)


ROOT = Path(__file__).resolve().parent.parent
MQ5 = ROOT / "mql5_ea" / "QuantSwarmXAU.mq5"


def test_product_version_is_unified():
    assert PRODUCT_VERSION == "4.0.0-rc2"
    assert __version__ == PRODUCT_VERSION
    assert VERSION_CONTRACT["product"] == PRODUCT_VERSION


def test_protocol_and_config_schema_match_contract():
    assert PROTOCOL_VERSION == BROKER_PROTOCOL_VERSION == "4.1"
    assert CONFIG_SCHEMA_VERSION == CONTRACT_CONFIG_SCHEMA_VERSION == 2


def test_mql5_declares_compatible_versions():
    source = MQ5.read_text(encoding="utf-8")

    assert f'#property version   "{MQL5_EA_VERSION}"' in source
    assert f'#define QSX_EA_VERSION "{MQL5_EA_VERSION}"' in source
    assert f'#define QSX_PROTOCOL_VERSION "{BROKER_PROTOCOL_VERSION}"' in source
    assert '",\\"ea_version\\":" + J(QSX_EA_VERSION)' in source


def test_canonical_launcher_exposes_only_safe_modes():
    assert SUPPORTED_LAUNCH_MODES == frozenset(
        {
            RuntimeMode.CHECK,
            RuntimeMode.SHADOW,
            RuntimeMode.DEMO,
        }
    )


def test_canonical_launcher_reports_product_version():
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "python_swarm.runtime_app",
            "--version",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0
    assert completed.stderr == ""
    assert completed.stdout.strip() == "QUANT-SWARM-XAU 4.0.0-rc2"


def test_active_branding_contains_no_draft_version():
    active_files = [
        ROOT / "python_swarm" / "dashboard.py",
        ROOT / "docs" / "CANONICAL_ARCHITECTURE.md",
        ROOT / "docs" / "README.md",
        ROOT / "docs" / "UPGRADE_GUIDE_v3.md",
    ]

    for path in active_files:
        text = path.read_text(encoding="utf-8")
        assert "4.0-draft" not in text
        assert "v4-draft" not in text


def test_normative_docs_and_operator_runbooks_pin_rc2_contract():
    required = {
        ROOT / "docs" / "CANONICAL_ARCHITECTURE.md": (
            PRODUCT_VERSION,
            f"protocol {BROKER_PROTOCOL_VERSION}",
            "InpApproveRiskDayHighWaterUtcDay",
        ),
        ROOT / "docs" / "README.md": (
            PRODUCT_VERSION,
            f"protocol {BROKER_PROTOCOL_VERSION}",
        ),
        ROOT / "docs" / "UPGRADE_GUIDE_v3.md": (
            PRODUCT_VERSION,
            f"protocol {BROKER_PROTOCOL_VERSION}",
            MQL5_EA_VERSION,
        ),
        ROOT / "deployment" / "DEMO_FORWARD_TEST_RUNBOOK.md": (
            "InpSessionMode",
            "QSX_SESSION_SHADOW",
            "InpApproveRiskDayHighWaterUtcDay",
            "InpApproveRiskDayForwardJumpUtcDay",
            "InpEmergencyStop",
        ),
        ROOT / "deployment" / "README.md": (
            "InpSessionMode",
            "InpApproveRiskDayHighWaterUtcDay",
            "InpApproveRiskDayForwardJumpUtcDay",
            "InpEmergencyStop",
        ),
    }

    for path, tokens in required.items():
        text = path.read_text(encoding="utf-8")
        for token in tokens:
            assert token in text, f"{path.relative_to(ROOT)} missing {token}"
