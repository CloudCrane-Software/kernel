"""WO-105: doc-vs-reality sync gates for the kernel self-rewrite.

The system rewrote its own docs (AGENTS.md / README.md / docs/) to match
post-M2 reality; these gates keep them honest — code that moves without the
docs (or the reverse) now fails here. Machine items mirror eval-gate suite
wo105-kernel-self-rewrite (G2) plus structural doc-reality checks.
"""

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
AGENTS_MD = ROOT / "AGENTS.md"
README_MD = ROOT / "README.md"
DOC_PATHS = [AGENTS_MD, README_MD, *sorted((ROOT / "docs").rglob("*.md"))]


def _agents_text() -> str:
    return AGENTS_MD.read_text(encoding="utf-8")


def _directory_map_paths() -> list[str]:
    """First token of every entry line inside the Directory map fence."""
    lines = _agents_text().splitlines()
    assert "## Directory map" in lines, "AGENTS.md lost its directory map"
    start = lines.index("## Directory map")
    fence = 0
    paths: list[str] = []
    for line in lines[start + 1 :]:
        if line.startswith("```"):
            fence += 1
            if fence == 2:
                break
            continue
        token = line.split()[0] if line.strip() else ""
        if not token:
            continue  # blank separator line inside the map fence
        paths.append(token)
    assert paths, "directory map is empty"
    return paths


def test_agents_md_documents_runsc_isolation_tier() -> None:
    text = _agents_text()
    assert "runsc" in text
    assert "sandbox_tier" in text
    assert (ROOT / "kernel/runner/runsc.py").is_file()
    assert (ROOT / "kernel/runner/scheduler.py").is_file()


@pytest.mark.parametrize("workorder", ["WO-101", "WO-102", "WO-103", "WO-104", "WO-105"])
def test_m2_workorder_numbers_present_in_docs(workorder: str) -> None:
    assert workorder in _agents_text(), f"{workorder} missing from AGENTS.md"


def test_directory_map_paths_exist() -> None:
    for token in _directory_map_paths():
        target = ROOT / token.rstrip("/")
        assert target.exists(), f"AGENTS.md maps {token!r} but it does not exist"


def test_episode_endpoints_documented_match_gateway() -> None:
    app_src = (ROOT / "gateway/app.py").read_text(encoding="utf-8")
    routes = [
        "/v1/workorders",
        "/v1/episodes/{episode_id}/transition",
        "/v1/episodes/{episode_id}/close",
    ]
    for route in routes:
        assert route in _agents_text(), f"{route} not documented in AGENTS.md"
        assert route in app_src, f"{route} documented but absent from gateway/app.py"


def test_readme_layout_matches_reality() -> None:
    text = README_MD.read_text(encoding="utf-8")
    for needle in ("kernel/audit_export/", "reconciler/external/", "runsc"):
        assert needle in text, f"README layout stale: {needle} missing"
    assert (ROOT / "kernel/audit_export").is_dir()
    assert (ROOT / "reconciler/external").is_dir()


def test_no_stale_host_reference_in_docs() -> None:
    for path in DOC_PATHS:
        text = path.read_text(encoding="utf-8")
        assert "srv-1" not in text, f"{path.name} still references the old host srv-1"
