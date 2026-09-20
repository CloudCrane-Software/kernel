"""reconciler/external/registry.py — adapter construction: env config + fake fleet (WO-101).

`adapters_from_config` builds the six real-system adapters (T4 live eval);
`fake_fleet` builds the in-memory fleet used by CI unit tests and by the
runsc sandbox self-check (no external side effects inside the sandbox).
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from reconciler.external.adapters import (
    ADAPTER_SYSTEMS,
    ClickHouseAdapter,
    HttpxClickHouse,
    HttpxLangfuse,
    HttpxLiteLLM,
    HttpxRegistry,
    LangfuseAdapter,
    LiteLLMAdapter,
    MinIOAdapter,
    RedisAdapter,
    ZotAdapter,
)
from reconciler.external.adapters.minio_adapter import (
    ObjectStore,  # noqa: F401 (protocol re-export)
)
from reconciler.external.adapters.zot_adapter import PushedManifest
from reconciler.external.model import ExternalSystemAdapter
from reconciler.external.s3 import S3Client


class MissingSystemConfig(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SystemConfig:
    redis_addr: str
    redis_password: str
    redis_db: int
    clickhouse_url: str
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_db: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str
    zot_url: str
    litellm_url: str
    litellm_api_key: str
    langfuse_url: str
    langfuse_public_key: str
    langfuse_secret_key: str


_ENV_MAP: Mapping[str, str] = {
    "redis_addr": "WO101_REDIS_ADDR",
    "redis_password": "WO101_REDIS_PASSWORD",
    "redis_db": "WO101_REDIS_DB",
    "clickhouse_url": "WO101_CLICKHOUSE_URL",
    "clickhouse_user": "WO101_CLICKHOUSE_USER",
    "clickhouse_password": "WO101_CLICKHOUSE_PASSWORD",
    "clickhouse_db": "WO101_CLICKHOUSE_DB",
    "s3_endpoint": "WO101_S3_ENDPOINT",
    "s3_access_key": "WO101_S3_ACCESS_KEY",
    "s3_secret_key": "WO101_S3_SECRET_KEY",
    "s3_region": "WO101_S3_REGION",
    "zot_url": "WO101_ZOT_URL",
    "litellm_url": "WO101_LITELLM_URL",
    "litellm_api_key": "WO101_LITELLM_API_KEY",
    "langfuse_url": "WO101_LANGFUSE_URL",
    "langfuse_public_key": "WO101_LANGFUSE_PUBLIC_KEY",
    "langfuse_secret_key": "WO101_LANGFUSE_SECRET_KEY",
}

_OPTIONAL_DEFAULTS: Mapping[str, str] = {
    "redis_db": "15",
    "clickhouse_user": "default",
    "clickhouse_password": "",
    "clickhouse_db": "default",
    "s3_region": "us-east-1",
}


def config_from_env(env: Mapping[str, str]) -> SystemConfig:
    values: dict[str, str] = {}
    missing: list[str] = []
    for field, var in _ENV_MAP.items():
        raw = env.get(var)
        if raw is None or raw == "":
            default = _OPTIONAL_DEFAULTS.get(field)
            if default is None:
                missing.append(var)
                continue
            raw = default
        values[field] = raw
    if missing:
        raise MissingSystemConfig(f"missing WO101 config: {', '.join(sorted(missing))}")
    return SystemConfig(
        **{**values, "redis_db": int(values["redis_db"])}  # type: ignore[arg-type]
    )


def adapters_from_config(config: SystemConfig) -> dict[str, ExternalSystemAdapter]:
    redis_host, _, redis_port = config.redis_addr.rpartition(":")
    if not redis_host:
        raise MissingSystemConfig("WO101_REDIS_ADDR must be host:port")
    s3 = S3Client(config.s3_endpoint, config.s3_access_key, config.s3_secret_key, config.s3_region)
    return {
        "redis": RedisAdapter(
            _LiveRedis(
                host=redis_host,
                port=int(redis_port or 6379),
                password=config.redis_password,
                db=config.redis_db,
            )
        ),
        "clickhouse": ClickHouseAdapter(
            HttpxClickHouse(
                config.clickhouse_url,
                config.clickhouse_user,
                config.clickhouse_password,
            ),
            database=config.clickhouse_db,
        ),
        "minio": MinIOAdapter(s3),
        "zot": ZotAdapter(HttpxRegistry(config.zot_url)),
        "litellm": LiteLLMAdapter(HttpxLiteLLM(config.litellm_url, config.litellm_api_key)),
        "langfuse": LangfuseAdapter(
            HttpxLangfuse(
                config.langfuse_url, config.langfuse_public_key, config.langfuse_secret_key
            )
        ),
    }


class _LiveRedis:
    """Connects lazily and reuses one RESP connection per adapter."""

    def __init__(self, *, host: str, port: int, password: str, db: int) -> None:
        from reconciler.external.resp import RespConnection

        self._conn = RespConnection(host, port, password=password or None, db=db)

    async def execute(self, *args: object) -> Any:
        if not self._conn.connected:
            await self._conn.connect()
        return await self._conn.execute(*args)


# ---------------------------------------------------------------------------
# In-memory fake fleet (CI tests + runsc sandbox self-check)
# ---------------------------------------------------------------------------


class FakeRedis:
    def __init__(self) -> None:
        self.data: dict[str, str] = {}
        self.commands: list[tuple[object, ...]] = []

    async def execute(self, *args: object) -> Any:
        import fnmatch

        self.commands.append(args)
        cmd = str(args[0]).upper()
        if cmd == "SET":
            self.data[str(args[1])] = str(args[2])
            return "OK"
        if cmd == "SCAN":
            pattern = str(args[3])
            matches = sorted(k for k in self.data if fnmatch.fnmatchcase(k, pattern))
            return ["0", matches]
        if cmd == "DEL":
            removed = sum(1 for k in args[1:] if str(k) in self.data)
            for k in args[1:]:
                self.data.pop(str(k), None)
            return removed
        if cmd in ("AUTH", "SELECT"):
            return "OK"
        raise AssertionError(f"fake redis: unexpected command {args!r}")


class FakeClickHouse:
    def __init__(self) -> None:
        self.tables: dict[str, list[tuple[str, int]]] = {}

    async def query(self, sql: str, *, data: str | None = None) -> str:
        if sql.startswith("CREATE TABLE"):
            name = sql.split()[2].split(".")[-1]
            self.tables[name] = []
            return ""
        if sql.startswith("INSERT INTO"):
            name = sql.split()[2].split(".")[-1]
            for line in (data or "").splitlines():
                k, _, v = line.partition(",")
                self.tables[name].append((k, int(v)))
            return ""
        if sql.startswith("SELECT name, total_rows"):
            return "".join(
                f"{name}\t{len(rows)}\n" for name, rows in sorted(self.tables.items()) if rows
            )
        if sql.startswith("DROP TABLE"):
            name = sql.split()[-1].split(".")[-1]
            self.tables.pop(name, None)
            return ""
        raise AssertionError(f"fake clickhouse: unexpected query {sql!r}")


class FakeObjectStore:
    def __init__(self) -> None:
        self.buckets: dict[str, dict[str, bytes]] = {}

    async def list_buckets(self) -> list[str]:
        return sorted(self.buckets)

    async def create_bucket(self, bucket: str) -> None:
        self.buckets.setdefault(bucket, {})

    async def put_object(self, bucket: str, key: str, data: bytes) -> None:
        self.buckets[bucket][key] = data

    async def delete_object(self, bucket: str, key: str) -> None:
        self.buckets.get(bucket, {}).pop(key, None)

    async def delete_bucket(self, bucket: str) -> None:
        self.buckets.pop(bucket, None)

    async def count_objects(self, bucket: str, prefix: str = "") -> int:
        return sum(1 for k in self.buckets.get(bucket, {}) if k.startswith(prefix))


class FakeRegistry:
    def __init__(self) -> None:
        self.repos: dict[str, dict[str, PushedManifest]] = {}

    async def catalog(self) -> list[str]:
        return sorted(self.repos)

    async def tags(self, repo: str) -> list[str]:
        return sorted(self.repos.get(repo, {}))

    async def push_manifest(
        self, repo: str, tag: str, config: bytes, layer: bytes
    ) -> PushedManifest:
        import hashlib

        def dg(b: bytes) -> str:
            return f"sha256:{hashlib.sha256(b).hexdigest()}"

        pushed = PushedManifest(digest=dg(config + layer), blob_digests=(dg(config), dg(layer)))
        self.repos.setdefault(repo, {})[tag] = pushed
        return pushed

    async def delete_manifest(self, repo: str, digest: str) -> None:
        tags = self.repos.get(repo, {})
        for tag, pushed in list(tags.items()):
            if pushed.digest == digest:
                del tags[tag]
        if repo in self.repos and not self.repos[repo]:
            del self.repos[repo]  # empty repos vanish from the catalog

    async def delete_blob(self, repo: str, digest: str) -> None:
        return None


class FakeLiteLLM:
    def __init__(self) -> None:
        self.keys: dict[str, str] = {}
        self.durations: dict[str, str] = {}

    async def generate_key(self, alias: str, duration: str) -> str:
        token = f"sk-fake-{alias}-{int(time.time())}"
        self.keys[alias] = token
        self.durations[alias] = duration
        return token

    async def list_keys(self) -> list[tuple[str, str]]:
        return sorted(self.keys.items())

    async def delete_key(self, token: str) -> None:
        for alias, value in list(self.keys.items()):
            if value == token:
                del self.keys[alias]


class FakeLangfuse:
    def __init__(self) -> None:
        self.traces: dict[str, str] = {}
        self.deleted: list[str] = []

    async def ingest_trace(self, trace_id: str, name: str) -> None:
        self.traces[trace_id] = name

    async def list_trace_ids(self, name: str) -> list[str]:
        return sorted(tid for tid, n in self.traces.items() if n == name)

    async def delete_trace(self, trace_id: str) -> None:
        self.traces.pop(trace_id, None)
        self.deleted.append(trace_id)


def fake_fleet() -> dict[str, ExternalSystemAdapter]:
    """The full six-system fleet over in-memory transports. Satisfies the
    ExternalSystemAdapter protocol per system, so G1/G2/G3 gates run with
    zero external side effects (CI + runsc sandbox self-check)."""
    return {
        "redis": RedisAdapter(FakeRedis()),
        "clickhouse": ClickHouseAdapter(FakeClickHouse()),
        "minio": MinIOAdapter(FakeObjectStore()),
        "zot": ZotAdapter(FakeRegistry()),
        "litellm": LiteLLMAdapter(FakeLiteLLM()),
        "langfuse": LangfuseAdapter(FakeLangfuse()),
    }


def fake_fleet_backends() -> dict[str, Any]:
    """The raw fakes behind fake_fleet(), for tests that must observe
    command-level effects (e.g. the SET ... EX 3600 TTL backstop)."""
    return {
        "redis": FakeRedis(),
        "clickhouse": FakeClickHouse(),
        "minio": FakeObjectStore(),
        "zot": FakeRegistry(),
        "litellm": FakeLiteLLM(),
        "langfuse": FakeLangfuse(),
    }


def fleet_from_backends(backends: Mapping[str, Any]) -> dict[str, ExternalSystemAdapter]:
    return {
        "redis": RedisAdapter(backends["redis"]),
        "clickhouse": ClickHouseAdapter(backends["clickhouse"]),
        "minio": MinIOAdapter(backends["minio"]),
        "zot": ZotAdapter(backends["zot"]),
        "litellm": LiteLLMAdapter(backends["litellm"]),
        "langfuse": LangfuseAdapter(backends["langfuse"]),
    }


assert tuple(fake_fleet_backends()) == ADAPTER_SYSTEMS, "fleet must mirror the closed list"
