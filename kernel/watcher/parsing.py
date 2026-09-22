"""watcher/parsing.py — work-order file parsing.

The mandates work-order format is Markdown with a `# Work order NNN — title`
heading and a metadata bullet list; the machine-readable bits the watcher
needs are:

  * the file name        workorders/0105-kernel-self-rewrite.md
                         -> workorder id "wo-0105-kernel-self-rewrite"
                         (the episode-id convention of the seeded episodes)
  * the **状态** bullet   first token decides dispatch eligibility (OPEN)
  * suite references     inline `suites/<name>.md` paths pointing at the
                         eval-gate repo

Parsing is deliberately tolerant: anything it cannot extract is returned as
None / empty and the poller decides policy. Only the file name is authoritative
— a name that does not match the convention is a hard error (the poller
records the file as skipped instead of guessing an episode id).
"""

from __future__ import annotations

import re

from kernel.watcher.models import ParsedWorkorder

# workorders/0105-kernel-self-rewrite.md -> ("0105", "kernel-self-rewrite")
WORKORDER_FILENAME = re.compile(r"^(?P<num>\d{4})-(?P<slug>[a-z0-9][a-z0-9-]*)\.md$")

# "- **状态**: OPEN（2026-09-20 ...)" -> "OPEN"; also tolerate a fullwidth colon
STATUS_LINE = re.compile(
    r"^\s*-\s*\*\*状态\*\*\s*[:：]\s*(?P<status>[A-Za-z][A-Za-z_-]*)", re.MULTILINE
)

# `suites/wo105-kernel-self-rewrite.md` -> "wo105-kernel-self-rewrite"
SUITE_REF = re.compile(r"suites/(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)\.md")

_HEADING = re.compile(r"^#\s+(?P<title>.+?)\s*$", re.MULTILINE)


class WorkorderFilenameError(ValueError):
    """Raised for a workorders/ file whose name carries no parseable id."""


def workorder_id_for(file_path: str) -> str:
    """workorders/0105-kernel-self-rewrite.md -> wo-0105-kernel-self-rewrite.

    The id convention mirrors the seeded episodes already on record
    (wo-0101..wo-0105): prefix "wo-" + file stem.
    """
    name = file_path.rsplit("/", 1)[-1]
    match = WORKORDER_FILENAME.fullmatch(name)
    if match is None:
        raise WorkorderFilenameError(
            f"{file_path!r} does not match NNNN-<slug>.md; cannot derive a workorder id"
        )
    return f"wo-{match.group('num')}-{match.group('slug')}"


def parse_workorder(file_path: str, text: str) -> ParsedWorkorder:
    """Extract id / title / status token / suite refs from one work order."""
    status_match = STATUS_LINE.search(text)
    seen: dict[str, None] = {}  # de-dup that preserves file order (py3.7+ dicts)
    for match in SUITE_REF.finditer(text):
        seen.setdefault(match.group("name"), None)
    title_match = _HEADING.search(text)
    return ParsedWorkorder(
        workorder_id=workorder_id_for(file_path),
        file_path=file_path,
        title=title_match.group("title") if title_match else None,
        status=status_match.group("status").upper() if status_match else None,
        suite_refs=tuple(seen),
    )


def is_dispatchable(parsed: ParsedWorkorder) -> bool:
    """Only work orders whose status bullet says OPEN enter the pipeline.

    Files marked COMPLETED / superseded / anything else are recorded as
    skipped — the watcher must never re-open history it finds already done.
    """
    return parsed.status == "OPEN"
