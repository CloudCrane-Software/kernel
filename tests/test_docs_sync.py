"""WO-105: doc-vs-reality sync gates for the kernel self-rewrite.

The system rewrote its own docs (AGENTS.md / README.md / docs/) to match
post-M2 reality; these gates keep them honest — code that moves without the
docs (or the reverse) now fails here. Machine items mirror eval-gate suite
wo105-kernel-self-rewrite (G2) plus structural doc-reality checks.

WO-108 F4: existence checks are upgraded to BINDING assertions — the adapter
module set must equal the ADAPTER_SYSTEMS closed list, and the AGENTS.md
deployment-topology line must match the deploy/images.env version pins.
"""

import re
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


# ------------------------------------------------------------------ WO-108 F4
# Binding assertions: counts and versions, not just existence. A seventh
# adapter directory or a bumped image version now REQUIRES the docs to move
# in the same PR.


def test_adapter_module_count_matches_closed_list() -> None:
    adapters_dir = ROOT / "reconciler/external/adapters"
    modules = sorted(p.name for p in adapters_dir.glob("*_adapter.py"))
    init = (adapters_dir / "__init__.py").read_text(encoding="utf-8")
    match = re.search(
        r"ADAPTER_SYSTEMS: Final\[tuple\[str, \.\.\.\]\] = \(([^)]*)\)", init, re.DOTALL
    )
    assert match, "ADAPTER_SYSTEMS tuple not found in reconciler/external/adapters/__init__.py"
    systems = sorted(re.findall(r'"([a-z0-9_]+)"', match.group(1)))
    assert systems, "ADAPTER_SYSTEMS parsed empty — check the tuple format"
    expected = [f"{s}_adapter.py" for s in systems]
    assert modules == expected, (
        f"adapter modules {modules} != ADAPTER_SYSTEMS {systems}: "
        "an adapter was added/removed without the closed list (or vice versa)"
    )


def _image_pins() -> dict[str, str]:
    pins: dict[str, str] = {}
    for line in (ROOT / "deploy/images.env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            pins[key] = value
    return pins


def _topology_paragraph() -> str:
    lines = _agents_text().splitlines()
    start = next(
        (i for i, line in enumerate(lines) if line.startswith("Deployment topology:")), None
    )
    assert start is not None, "AGENTS.md lost its deployment topology line"
    paragraph: list[str] = []
    for line in lines[start:]:
        if not line.strip():
            break
        paragraph.append(line)
    return " ".join(paragraph)


def test_topology_line_matches_deploy_image_pins() -> None:
    pins = _image_pins()
    assert {"GATEWAY_IMAGE", "AUDIT_EXPORT_IMAGE", "KERNEL_TASK_IMAGE"} <= set(pins)
    topology = _topology_paragraph()
    gateway_tag = pins["GATEWAY_IMAGE"].rsplit(":", 1)[-1]
    audit_tag = pins["AUDIT_EXPORT_IMAGE"].rsplit(":", 1)[-1]
    assert f"gateway {gateway_tag}" in topology, (
        "topology line stale vs deploy/images.env (gateway)"
    )
    assert f"audit-export {audit_tag}" in topology, (
        "topology line stale vs deploy/images.env (audit-export)"
    )
    assert "kernel-task" in topology, "topology line omits the kernel-task runsc task image"
    assert "deploy/images.env" in topology, "topology line must cite its binding source"


def test_kernel_task_pin_matches_runsc_default_image() -> None:
    pins = _image_pins()
    runsc_src = (ROOT / "kernel/runner/runsc.py").read_text(encoding="utf-8")
    assert f'image: str = "{pins["KERNEL_TASK_IMAGE"]}"' in runsc_src, (
        "deploy/images.env KERNEL_TASK_IMAGE != RunscRunner default image"
    )


def test_adr_references_resolve_inside_the_repo() -> None:
    adr3 = ROOT / "docs/adr/0003-auto-merge-principle.md"
    adr5 = ROOT / "docs/adr/0005-evalgate-ci-platform.md"
    assert adr3.is_file(), "ADR-0003 mirror missing (references must resolve in-repo, WO-108 F2)"
    assert adr5.is_file(), "ADR-0005 mirror missing (references must resolve in-repo, WO-108 F2)"
    assert "docs/adr/0003" in _agents_text(), "AGENTS.md ADR-0003 reference not an in-repo path"
    gate = (ROOT / ".github/workflows/eval-gate.yml").read_text(encoding="utf-8")
    guard = (ROOT / ".github/workflows/guard.yml").read_text(encoding="utf-8")
    assert "docs/adr/0003" in gate, "eval-gate.yml ADR-0003 reference not an in-repo path"
    assert "docs/adr/0005" in guard, "guard.yml ADR-0005 reference not an in-repo path"
    # mirrors are marked as read-only snapshots of an authoritative home
    assert "READ-ONLY SNAPSHOT" in adr3.read_text(encoding="utf-8")
    assert "READ-ONLY" in adr5.read_text(encoding="utf-8")


def test_known_open_items_documented() -> None:
    text = _agents_text()
    assert "Known open items" in text, "status section lost its known-open-items line"
    assert "WO-106" in text, "WO-106 evidence binding must stay visible (negative items on record)"
