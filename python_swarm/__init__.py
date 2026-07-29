"""QUANT-SWARM-XAU Python control-plane package.

Importing this package is deliberately side-effect free. Runtime directories,
database connections, network listeners, and agent instances are all created
by explicit composition-root calls.
"""

from .version import PRODUCT_VERSION, VERSION_CONTRACT


__version__ = PRODUCT_VERSION

__all__ = [
    "PRODUCT_VERSION",
    "VERSION_CONTRACT",
    "__version__",
]
