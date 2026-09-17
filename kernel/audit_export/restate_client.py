"""Restate admin API client for the audit exporter.

Grounded in a probe of the LIVE cluster (Restate 1.7.10, admin API on :9070,
no auth) on 2026-09-17:

- There is **no** ``GET /invocations`` listing endpoint. The admin OpenAPI
  (``GET /openapi``) exposes only per-invocation-id lifecycle operations
  (``DELETE /invocations/{id}``, ``PATCH /invocations/{id}/cancel|kill|...``).
  The ``/restate/invocations`` and ``/restate/journal`` paths assumed by the
  work order do **not** exist (404); all real endpoints live at the root,
  without the ``/restate`` prefix.
- Invocation data is readable via ``POST /query`` — a SQL (DataFusion)
  interface over the cluster state: request ``{"query": "SELECT ..."}``,
  response ``{"rows": [...]}`` **only when** ``Accept: application/json`` is
  set; without it the endpoint returns an Arrow stream instead.
- Relevant tables (from ``information_schema.tables``):
  ``sys_invocation`` (VIEW: one row per retained invocation, joined from
  ``sys_invocation_status`` + in-flight ``sys_invocation_state``, with a
  computed ``status`` column: pending/scheduled/completed/suspended/paused/
  running/backing-off/ready), ``sys_invocation_status`` (raw states incl.
  completed), and ``sys_journal`` (per-invocation journal entries with
  ``entry_json``). Timestamps are ``Timestamp(ms, "+00:00")``; the JSON
  serialization observed in the spec is RFC3339-ish strings, so timestamps
  are parsed defensively (string, epoch-ms int/float, or null).

The client is transport-injectable (``httpx.AsyncClient`` or a mock) for
offline tests.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx

#: Columns pulled from ``sys_invocation`` — the audit event payload shape.
#: Kept explicit (no ``SELECT *``) so the exported JSON stays stable across
#: Restate upgrades; the full row is preserved verbatim in ``raw``.
INVOCATION_COLUMNS: tuple[str, ...] = (
    "id",
    "target",
    "target_service_name",
    "target_service_key",
    "target_handler_name",
    "target_service_ty",
    "invoked_by",
    "invoked_by_service_name",
    "invoked_by_id",
    "idempotency_key",
    "trace_id",
    "journal_size",
    "retry_count",
    "status",
    "created_at",
    "modified_at",
    "running_at",
    "completed_at",
    "completion_result",
    "completion_failure",
    "last_failure",
    "last_failure_error_code",
)

_TS_COLUMNS = frozenset(
    {
        "created_at",
        "modified_at",
        "running_at",
        "completed_at",
        "inboxed_at",
        "scheduled_at",
        "scheduled_start_at",
        "last_start_at",
        "next_retry_at",
        "appended_at",
        "sleep_wakeup_at",
    }
)


def parse_ts(value: Any) -> datetime | None:
    """Normalize a Restate/DataFusion timestamp to an aware UTC datetime.

    Handles the shapes the query API can return: RFC3339 strings
    (``2026-09-17T08:00:00.123Z`` or ``+00:00`` offsets, possibly with a
    space separator) and epoch milliseconds as int/float. Returns ``None``
    for null/unknown values (Restate columns are nullable).
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        text = value.strip().replace(" ", "T")
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return datetime.fromtimestamp(value / 1000.0, tz=UTC)
    raise ValueError(f"unparseable timestamp: {value!r}")


def _sql_literal(moment: datetime) -> str:
    """Render a datetime as a single-quoted DataFusion timestamp literal."""
    return moment.astimezone(UTC).isoformat(sep=" ")


class RestateAdminError(RuntimeError):
    """The Restate admin API failed or returned an unexpected shape."""


class RestateAdminClient:
    """Async reader over the Restate admin ``/query`` interface."""

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:9070",
        *,
        client: httpx.AsyncClient | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._client = client or httpx.AsyncClient(timeout=timeout)
        self._owns_client = client is None

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    # ------------------------------------------------------------- transport
    async def query(self, sql: str) -> list[dict[str, Any]]:
        """Run one SQL statement; return the JSON rows.

        ``Accept: application/json`` is mandatory — without it the endpoint
        streams Arrow bytes instead of JSON.
        """
        response = await self._client.post(
            f"{self._base_url}/query",
            json={"query": sql},
            headers={"Accept": "application/json"},
        )
        if response.status_code != 200:
            raise RestateAdminError(
                f"query failed: HTTP {response.status_code}: {response.text[:200]}"
            )
        try:
            body = response.json()
        except ValueError as exc:
            raise RestateAdminError(f"non-JSON query response: {response.text[:200]}") from exc
        rows = body.get("rows")
        if not isinstance(rows, list):
            raise RestateAdminError(f"unexpected query response shape: {body!r:.200}")
        return rows

    async def ping(self) -> bool:
        """True when the admin API ``/health`` endpoint answers 200."""
        try:
            response = await self._client.get(f"{self._base_url}/health")
        except httpx.HTTPError:
            return False
        return response.status_code == 200

    # ------------------------------------------------------------- readers
    async def fetch_invocations(
        self,
        since: datetime | None = None,
        *,
        after_id: str = "",
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Fetch invocation rows strictly after the ``(since, after_id)`` cursor.

        Uses the ``sys_invocation`` view (covers in-flight AND retained
        completed invocations) ordered by ``(created_at, id)`` ascending so
        the exporter's cursor advances monotonically; the id tiebreaker keeps
        records sharing the same millisecond from being skipped.
        """
        columns = ", ".join(INVOCATION_COLUMNS)
        predicate = ""
        if since is not None:
            safe_id = after_id.replace("'", "''")
            predicate = (
                f"WHERE created_at > TIMESTAMP '{_sql_literal(since)}' "
                f"OR (created_at = TIMESTAMP '{_sql_literal(since)}' AND id > '{safe_id}')"
            )
        sql = (
            f"SELECT {columns} FROM sys_invocation {predicate} "
            f"ORDER BY created_at ASC, id ASC LIMIT {int(limit)}"
        )
        rows = await self.query(sql)
        return [_normalize_row(row) for row in rows]

    async def fetch_journal(
        self,
        invocation_id: str,
        *,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Fetch journal entries of one invocation (``sys_journal`` table).

        There is no REST journal endpoint; journal data is only reachable
        through the SQL interface. Retained for evidence enrichment; the
        core exporter ships invocation-level events.
        """
        safe_id = invocation_id.replace("'", "''")
        sql = (
            "SELECT id, index, entry_type, name, completed, invoked_id, "
            "invoked_target, appended_at, entry_json FROM sys_journal "
            f"WHERE id = '{safe_id}' ORDER BY index ASC LIMIT {int(limit)}"
        )
        rows = await self.query(sql)
        return [_normalize_row(row) for row in rows]


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    """Parse known timestamp columns; leave everything else untouched."""
    normalized = dict(row)
    for column in _TS_COLUMNS & row.keys():
        value = normalized[column]
        if value is not None and not isinstance(value, datetime):
            normalized[column] = parse_ts(value)
        elif isinstance(value, datetime) and value.tzinfo is None:
            normalized[column] = value.replace(tzinfo=UTC)
    return normalized
