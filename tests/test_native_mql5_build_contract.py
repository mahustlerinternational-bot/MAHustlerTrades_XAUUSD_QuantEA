import codecs
import json
import shutil
from pathlib import Path

import pytest

from tools import build_mql5_native as native


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "release" / "native-mql5-build-contract.json"


def _valid_log() -> bytes:
    include_paths = [
        value.replace("/", "\\")
        for value in json.loads(CONTRACT.read_text(encoding="utf-8"))[
            "expected_includes"
        ]
    ]
    lines = [
        r"C:\QSXBuild\QuantSwarmXAU.mq5 : information: compiling C:\QSXBuild\QuantSwarmXAU.mq5",
        *[
            (
                "C:\\Program Files\\MetaTrader 5\\dummy.mqh"
                " : information: including C:\\Program Files\\MetaTrader 5\\"
                + include
            )
            for include in include_paths
        ],
        " : information: generating code",
        " : information: code generated",
        "Result: 0 errors, 0 warnings, 1234 ms elapsed, cpu='X64 Regular'",
        "",
    ]
    return codecs.BOM_UTF8 + "\r\n".join(lines).encode("utf-8")


def test_native_build_contract_is_canonical_exact_and_no_real_prefix_is_defaulted():
    contract, raw = native._load_contract(CONTRACT)

    assert raw == native.canonical_json(contract)
    assert contract["expected_process_exit_code"] == 1
    assert contract["compiler"]["version"] == "5.0.0.6061"
    assert len(contract["expected_includes"]) == 7
    assert contract["minimum_ex5_bytes"] >= 65536

    source = (ROOT / "tools" / "build_mql5_native.py").read_text(encoding="utf-8")
    assert "--unshare-all" in source
    assert '"--tmpfs",\n        "/home"' in source
    assert 'f"{commit}:{source_path}"' in source
    assert "~/.wine" not in source
    assert "terminal64.exe" not in raw.decode("utf-8")


def test_native_log_parser_accepts_only_exact_include_and_zero_warning_contract():
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    normalized = native._validate_log(
        _valid_log(),
        contract["expected_includes"],
    )

    assert normalized.startswith(b"C:\\QSXBuild")
    assert b"\r" not in normalized
    assert normalized.count(b"Result: 0 errors, 0 warnings") == 1

    warning = _valid_log().replace(
        b" : information: code generated",
        b"C:\\QSXBuild\\QuantSwarmXAU.mq5 : warning: unsafe",
    )
    with pytest.raises(native.NativeBuildError, match="error or warning"):
        native._validate_log(warning, contract["expected_includes"])

    missing = _valid_log().replace(
        contract["expected_includes"][0].replace("/", "\\").encode(),
        b"MQL5\\Include\\Unexpected.mqh",
    )
    with pytest.raises(native.NativeBuildError, match="include closure"):
        native._validate_log(missing, contract["expected_includes"])

    contradictory = _valid_log().replace(
        b"Result: 0 errors, 0 warnings",
        b"Result: 1 errors, 0 warnings, failed\r\n"
        b"Result: 0 errors, 0 warnings",
    )
    with pytest.raises(native.NativeBuildError, match="zero-error result"):
        native._validate_log(contradictory, contract["expected_includes"])

    fatal_after_result = (
        _valid_log().rstrip(b"\r\n")
        + b"\r\nfatal error after compiler result\r\n"
    )
    with pytest.raises(native.NativeBuildError, match="zero-error result"):
        native._validate_log(fatal_after_result, contract["expected_includes"])

    expected_compile = native.COMPILE_LINE.encode()
    extra_compile = _valid_log().replace(
        expected_compile,
        expected_compile
        + b"\r\nC:\\evil.mq5 : information: compiling C:\\evil.mq5",
    )
    with pytest.raises(native.NativeBuildError, match="unexpected source"):
        native._validate_log(extra_compile, contract["expected_includes"])


def test_native_log_decoder_rejects_ambiguous_or_invalid_encoding():
    with pytest.raises(native.NativeBuildError, match="explicit BOM"):
        native._decode_log(b"Result: 0 errors, 0 warnings")
    with pytest.raises(UnicodeDecodeError):
        native._decode_log(codecs.BOM_UTF8 + b"\xff")


def test_native_evidence_schema_separates_process_status_from_gate_status():
    assert "process_exit_code" in native.EVIDENCE_FIELDS
    assert "gate_exit_code" in native.EVIDENCE_FIELDS
    assert "binary_reproducible" in native.EVIDENCE_FIELDS
    assert "exit_code" not in native.EVIDENCE_FIELDS


def test_native_contract_cannot_be_supplied_from_outside_the_clean_source_tree(
    tmp_path: Path,
):
    source_root = tmp_path / "source"
    canonical = source_root / native.CANONICAL_CONTRACT_PATH.as_posix()
    canonical.parent.mkdir(parents=True)
    shutil.copyfile(CONTRACT, canonical)
    external = tmp_path / "external-contract.json"
    shutil.copyfile(CONTRACT, external)

    with pytest.raises(native.NativeBuildError, match="canonical file"):
        native._load_committed_contract(source_root, external, "0" * 40)


def test_native_git_environment_disables_replace_refs_and_optional_mutations():
    environment = native._git_env()

    assert environment["GIT_NO_REPLACE_OBJECTS"] == "1"
    assert environment["GIT_OPTIONAL_LOCKS"] == "0"
    source = (ROOT / "tools" / "build_mql5_native.py").read_text(encoding="utf-8")
    assert "core.fsmonitor=false" in source
    assert "core.hooksPath=/dev/null" in source
    assert "core.untrackedCache=false" in source
