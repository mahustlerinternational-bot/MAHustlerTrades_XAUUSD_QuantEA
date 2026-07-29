"""Authoritative QUANT-SWARM-XAU release and compatibility contract.

All user-facing and runtime version reporting must derive from this module.
The MQL5 source remains independently compiled, but its declared EA and
protocol versions are tested against these constants.
"""

from __future__ import annotations

from types import MappingProxyType


PRODUCT_NAME = "QUANT-SWARM-XAU"
PRODUCT_VERSION = "4.0.0-rc1"
PRODUCT_DISPLAY_VERSION = f"v{PRODUCT_VERSION}"

BROKER_PROTOCOL_VERSION = "4.0"
MQL5_EA_VERSION = "4.00"
CONFIG_SCHEMA_VERSION = 2

VERSION_CONTRACT = MappingProxyType(
    {
        "product": PRODUCT_VERSION,
        "broker_protocol": BROKER_PROTOCOL_VERSION,
        "mql5_ea": MQL5_EA_VERSION,
        "config_schema": CONFIG_SCHEMA_VERSION,
    }
)


__all__ = [
    "BROKER_PROTOCOL_VERSION",
    "CONFIG_SCHEMA_VERSION",
    "MQL5_EA_VERSION",
    "PRODUCT_DISPLAY_VERSION",
    "PRODUCT_NAME",
    "PRODUCT_VERSION",
    "VERSION_CONTRACT",
]
