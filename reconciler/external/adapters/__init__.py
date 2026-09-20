"""reconciler/external/adapters/__init__.py — adapter registration (WO-101 G1 root).

The closed system list in the work order names exactly these six systems;
G1 closure is machine-checked against this registry: one adapter per
system, >=1 registered drift-injection case per system.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

from reconciler.external.adapters.clickhouse_adapter import (
    ClickHouseAdapter,
    ClickHouseQuery,
    HttpxClickHouse,
)
from reconciler.external.adapters.langfuse_adapter import (
    HttpxLangfuse,
    LangfuseAdapter,
    LangfuseClient,
)
from reconciler.external.adapters.litellm_adapter import (
    HttpxLiteLLM,
    LiteLLMAdapter,
    LiteLLMClient,
)
from reconciler.external.adapters.minio_adapter import MinIOAdapter, ObjectStore
from reconciler.external.adapters.redis_adapter import RedisAdapter
from reconciler.external.adapters.zot_adapter import (
    HttpxRegistry,
    RegistryClient,
    ZotAdapter,
)

__all__ = [
    "ADAPTER_SYSTEMS",
    "INJECTION_CASES",
    "ClickHouseAdapter",
    "ClickHouseQuery",
    "HttpxClickHouse",
    "HttpxLangfuse",
    "HttpxLiteLLM",
    "HttpxRegistry",
    "LangfuseAdapter",
    "LangfuseClient",
    "LiteLLMAdapter",
    "LiteLLMClient",
    "MinIOAdapter",
    "ObjectStore",
    "RedisAdapter",
    "RegistryClient",
    "ZotAdapter",
    "check_list_closure",
]

ADAPTER_SYSTEMS: Final[tuple[str, ...]] = (
    "redis",
    "clickhouse",
    "minio",
    "zot",
    "litellm",
    "langfuse",
)

INJECTION_CASES: Final[dict[str, tuple[str, ...]]] = {
    system: (f"wo101-{system}-drift-1",) for system in ADAPTER_SYSTEMS
}


def check_list_closure(
    system_list: Sequence[str], adapters: Sequence[str] = ADAPTER_SYSTEMS
) -> tuple[bool, str]:
    """G1 machine check: registered adapters == closed list, every system
    carries >=1 registered drift-injection case."""
    missing = sorted(set(system_list) - set(adapters))
    extra = sorted(set(adapters) - set(system_list))
    no_case = [s for s in system_list if not INJECTION_CASES.get(s)]
    ok = not missing and not extra and not no_case
    detail = (
        f"adapters={len(adapters)} list={len(system_list)}"
        + (f" missing={missing}" if missing else "")
        + (f" extra={extra}" if extra else "")
        + (f" no_case={no_case}" if no_case else "")
    )
    return ok, detail
