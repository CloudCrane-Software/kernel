"""WO-101 external adapters — per-system behavior over in-memory transports.

Each adapter must: expose case-scoped snapshot counters, create its
disposable resource under the test prefix with the documented backstop
(Redis SET ... EX 3600, LiteLLM duration=1h), yield findings whose counters
carry the resource handle, and clean up back to the exact baseline.
"""

from __future__ import annotations

from reconciler.external.adapters import (
    ClickHouseAdapter,
    LangfuseAdapter,
    LiteLLMAdapter,
    MinIOAdapter,
    RedisAdapter,
    ZotAdapter,
)
from reconciler.external.model import SystemSnapshot, diff_snapshots, utcnow
from reconciler.external.registry import fake_fleet_backends, fleet_from_backends


async def test_redis_cycle_and_ttl_backstop() -> None:
    redis = fake_fleet_backends()["redis"]
    adapter = RedisAdapter(redis)
    baseline = await adapter.snapshot()
    assert baseline == {}

    resource = await adapter.inject("c1")
    assert resource.handle == "wo101-drift:c1"
    set_cmds = [c for c in redis.commands if c[0] == "SET"]
    assert set_cmds and set_cmds[0][1] == "wo101-drift:c1"
    assert "EX" in set_cmds[0] and set_cmds[0][-1] == 3600  # TTL backstop

    snap = await adapter.snapshot()
    assert snap == {"key:wo101-drift:c1": 1}
    findings = diff_snapshots(
        SystemSnapshot("redis", utcnow(), baseline), SystemSnapshot("redis", utcnow(), snap)
    )
    assert [f.counter for f in findings] == ["key:wo101-drift:c1"]
    assert all(resource.handle in f.counter for f in findings)

    await adapter.cleanup(resource)
    assert await adapter.snapshot() == baseline


async def test_clickhouse_memory_table_rows() -> None:
    ch = fake_fleet_backends()["clickhouse"]
    adapter = ClickHouseAdapter(ch)
    baseline = await adapter.snapshot()
    assert baseline == {}

    resource = await adapter.inject("c2")
    assert resource.handle == "wo101_drift_c2"
    assert ch.tables["wo101_drift_c2"], "injection must insert rows"

    snap = await adapter.snapshot()
    assert snap == {"rows:wo101_drift_c2": len(ch.tables["wo101_drift_c2"])}
    await adapter.cleanup(resource)
    assert await adapter.snapshot() == baseline


async def test_minio_bucket_and_objects() -> None:
    store = fake_fleet_backends()["minio"]
    adapter = MinIOAdapter(store)
    assert await adapter.snapshot() == {}

    resource = await adapter.inject("c3")
    assert resource.handle == "wo101-drift-c3"
    snap = await adapter.snapshot()
    assert snap["bucket:wo101-drift-c3"] == 1
    assert snap["objects:wo101-drift-c3"] == 2
    await adapter.cleanup(resource)
    assert store.buckets == {}
    assert await adapter.snapshot() == {}


async def test_zot_repo_tag_and_orphan_free_cleanup() -> None:
    registry = fake_fleet_backends()["zot"]
    adapter = ZotAdapter(registry)
    assert await adapter.snapshot() == {}

    resource = await adapter.inject("c4")
    assert resource.handle == "wo101-drift/c4"
    assert await adapter.snapshot() == {"tags:wo101-drift/c4:drift": 1}

    await adapter.cleanup(resource)
    assert registry.repos == {}  # tag gone AND no orphaned blobs recorded
    assert await adapter.snapshot() == {}


async def test_litellm_virtual_key_with_expiry() -> None:
    litellm = fake_fleet_backends()["litellm"]
    adapter = LiteLLMAdapter(litellm)
    assert await adapter.snapshot() == {}

    resource = await adapter.inject("c5")
    assert resource.handle == "wo101-drift/c5"
    assert litellm.durations["wo101-drift/c5"] == "1h"  # expiry backstop
    assert await adapter.snapshot() == {"virtual_key:wo101-drift/c5": 1}

    await adapter.cleanup(resource)
    assert await adapter.snapshot() == {}


async def test_langfuse_trace_namespace() -> None:
    langfuse = fake_fleet_backends()["langfuse"]
    adapter = LangfuseAdapter(langfuse)
    assert await adapter.snapshot() == {}

    resource = await adapter.inject("c6")
    assert resource.handle.startswith("wo101-drift-c6-")
    assert langfuse.traces[resource.handle] == "wo101-drift-recon"
    assert await adapter.snapshot() == {f"trace:{resource.handle}": 1}

    await adapter.cleanup(resource)
    assert resource.handle in langfuse.deleted
    assert await adapter.snapshot() == {}


async def test_fleet_counters_are_case_scoped() -> None:
    """Every adapter's finding counters must carry the case id so G2's
    detection assertion is precise (no fuzzy system-level deltas)."""
    for system, adapter in fleet_from_backends(fake_fleet_backends()).items():
        resource = await adapter.inject("prec")
        snap = await adapter.snapshot()
        assert snap, f"{system} snapshot empty after injection"
        assert any(resource.handle in counter for counter in snap), (
            f"{system} counters not case-scoped: {sorted(snap)}"
        )
        await adapter.cleanup(resource)
