"""reconciler/external/syslist.py — work-order system-list parser (WO-101 G1).

G1 (adapter list closure) is machine-checkable: the work order carries a
BEGIN/END-delimited closed list; the harness parses it and asserts the
registered adapter count == N with >=1 drift-injection case per system.
"""

from __future__ import annotations

import re
from pathlib import Path

BEGIN_MARK = "WO101-SYSTEM-LIST BEGIN"
END_MARK = "WO101-SYSTEM-LIST END"

_SYSTEM_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


class SystemListError(ValueError):
    pass


def parse_system_list(text: str) -> list[str]:
    """Extract the closed system list between the markers. Fails closed on
    missing markers, empty list, bad identifiers or duplicates."""
    try:
        begin = text.index(BEGIN_MARK)
        end = text.index(END_MARK)
    except ValueError as exc:
        raise SystemListError(f"system-list markers missing: {exc}") from exc
    if end < begin:
        raise SystemListError("system-list END marker precedes BEGIN marker")
    # drop any same-line annotation after the BEGIN marker
    body_start = text.index("\n", begin + len(BEGIN_MARK))

    systems: list[str] = []
    for raw in text[body_start:end].splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if not _SYSTEM_RE.match(line):
            raise SystemListError(f"bad system identifier: {line!r}")
        if line in systems:
            raise SystemListError(f"duplicate system in list: {line}")
        systems.append(line)
    if not systems:
        raise SystemListError("system list is empty")
    return systems


def parse_system_list_file(path: Path) -> list[str]:
    return parse_system_list(path.read_text(encoding="utf-8"))
