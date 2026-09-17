"""kernel/audit_export — Restate journal/invocation audit export (WO-01, M1).

Periodically exports Restate invocation records to the Kafka topic
``audit-events`` and a replayable PG table ``audit_events``:

- :class:`RestateAdminClient` — reads invocation data from the Restate admin
  API (grounded in the live 1.7.10 cluster, see its docstring).
- :class:`AuditExporter` — poll loop; cursor-persisted, idempotent exports.
- :class:`AuditConsumer` — replays ``audit-events`` back into PG.
- ``python -m kernel.audit_export`` — runnable service entrypoint.
"""
