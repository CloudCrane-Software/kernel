"""Smoke test: environment wiring (WO-00 acceptance)."""

import kernel
import reconciler


def test_version() -> None:
    assert kernel.__version__ == "0.1.0"
    assert reconciler.__version__ == "0.1.0"


def test_python_312() -> None:
    import sys

    assert sys.version_info[:2] == (3, 12), "the kernel targets Python 3.12"
