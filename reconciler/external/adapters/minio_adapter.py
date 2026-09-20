"""reconciler/external/adapters/minio_adapter.py — MinIO drift adapter (WO-101 #3).

Reconciliation surface: object counts of dedicated `wo101-drift-*` buckets
(ListObjectsV2 via SigV4 — reconciler/s3.py). Injection: create one
dedicated bucket + PutObject two objects; cleanup DeleteObject+DeleteBucket
and the post-cleanup snapshot must equal baseline.
"""

from __future__ import annotations

from typing import Protocol

from reconciler.external.model import InjectedResource, utcnow

DEFAULT_BUCKET_PREFIX = "wo101-drift-"


class ObjectStore(Protocol):
    async def list_buckets(self) -> list[str]: ...

    async def create_bucket(self, bucket: str) -> None: ...

    async def put_object(self, bucket: str, key: str, data: bytes) -> None: ...

    async def delete_object(self, bucket: str, key: str) -> None: ...

    async def delete_bucket(self, bucket: str) -> None: ...

    async def count_objects(self, bucket: str, prefix: str = "") -> int: ...


class MinIOAdapter:
    name = "minio"

    def __init__(self, store: ObjectStore, *, bucket_prefix: str = DEFAULT_BUCKET_PREFIX) -> None:
        self._store = store
        self._prefix = bucket_prefix

    async def snapshot(self) -> dict[str, int]:
        counters: dict[str, int] = {}
        buckets = [b for b in await self._store.list_buckets() if b.startswith(self._prefix)]
        for bucket in sorted(buckets):
            counters[f"bucket:{bucket}"] = 1
            counters[f"objects:{bucket}"] = await self._store.count_objects(bucket)
        return counters

    async def inject(self, case_id: str) -> InjectedResource:
        bucket = f"{self._prefix}{case_id}"
        await self._store.create_bucket(bucket)
        keys = self._object_keys(case_id)
        for key in keys:
            await self._store.put_object(bucket, key, f"wo101-drift:{case_id}:{key}".encode())
        return InjectedResource(
            system=self.name, case_id=case_id, handle=bucket, created_at=utcnow()
        )

    async def cleanup(self, resource: InjectedResource) -> None:
        bucket = resource.handle
        for key in self._object_keys(resource.case_id):
            await self._store.delete_object(bucket, key)
        await self._store.delete_bucket(bucket)

    @staticmethod
    def _object_keys(case_id: str) -> list[str]:
        return [f"{case_id}/drift-a.bin", f"{case_id}/drift-b.bin"]
