"""reconciler/external/adapters/zot_adapter.py — zot OCI registry drift adapter (WO-101 #4).

Reconciliation surface: tag counts of `wo101-drift/*` repositories
(/v2/_catalog + /v2/<repo>/tags/list — OCI Distribution API). Injection:
push one test repo (config+layer blobs, then a tagged OCI manifest) over
plain HTTP; cleanup DELETEs the manifest and both blobs (no orphans) and
the post-cleanup snapshot must equal baseline.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from reconciler.external.model import InjectedResource, utcnow

DEFAULT_REPO_PREFIX = "wo101-drift/"

_MANIFEST_MEDIA = "application/vnd.oci.image.manifest.v1+json"
_CONFIG_MEDIA = "application/vnd.oci.image.config.v1+json"
_LAYER_MEDIA = "application/vnd.oci.image.layer.v1.tar"


def _digest(data: bytes) -> str:
    return f"sha256:{hashlib.sha256(data).hexdigest()}"


@dataclass(frozen=True, slots=True)
class PushedManifest:
    digest: str
    blob_digests: tuple[str, ...]


class RegistryClient(Protocol):
    async def catalog(self) -> list[str]: ...

    async def tags(self, repo: str) -> list[str]: ...

    async def push_manifest(
        self, repo: str, tag: str, config: bytes, layer: bytes
    ) -> PushedManifest: ...

    async def delete_manifest(self, repo: str, digest: str) -> None: ...

    async def delete_blob(self, repo: str, digest: str) -> None: ...


class HttpxRegistry:
    """OCI Distribution API client against zot (DELETE enabled for the
    eval; the work order's disposable-injection table names DELETE as the
    cleanup path)."""

    def __init__(
        self,
        url: str,
        *,
        username: str = "",
        password: str = "",
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._url = url.rstrip("/")
        self._auth = (username, password) if username else None
        self._transport = transport
        self._timeout = timeout

    async def _request(
        self,
        method: str,
        path: str,
        *,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        async with httpx.AsyncClient(transport=self._transport, timeout=self._timeout) as client:
            resp = await client.request(
                method,
                f"{self._url}{path}",
                content=content,
                headers=headers,
                auth=self._auth,
            )
        if resp.status_code >= 300:
            raise RuntimeError(f"registry {method} {path} -> {resp.status_code}")
        return resp

    async def catalog(self) -> list[str]:
        resp = await self._request("GET", "/v2/_catalog")
        repos: list[str] = resp.json().get("repositories", [])
        return repos

    async def tags(self, repo: str) -> list[str]:
        resp = await self._request("GET", f"/v2/{repo}/tags/list")
        tags: list[str] = resp.json().get("tags") or []
        return tags

    async def push_manifest(
        self, repo: str, tag: str, config: bytes, layer: bytes
    ) -> PushedManifest:
        digests: list[str] = []
        for blob, media in (
            (config, _CONFIG_MEDIA),
            (layer, _LAYER_MEDIA),
        ):
            digest = _digest(blob)
            start = await self._request("POST", f"/v2/{repo}/blobs/uploads/")
            location = start.headers["Location"]
            sep = "&" if "?" in location else "?"
            await self._request(
                "PUT",
                f"{location}{sep}digest={digest}",
                content=blob,
                headers={"Content-Type": media},
            )
            digests.append(digest)

        manifest = {
            "schemaVersion": 2,
            "mediaType": _MANIFEST_MEDIA,
            "config": {
                "mediaType": _CONFIG_MEDIA,
                "digest": digests[0],
                "size": len(config),
            },
            "layers": [{"mediaType": _LAYER_MEDIA, "digest": digests[1], "size": len(layer)}],
        }
        body = json.dumps(manifest).encode("utf-8")
        put = await self._request(
            "PUT",
            f"/v2/{repo}/manifests/{tag}",
            content=body,
            headers={"Content-Type": _MANIFEST_MEDIA},
        )
        manifest_digest = put.headers.get("Docker-Content-Digest") or _digest(body)
        return PushedManifest(digest=manifest_digest, blob_digests=tuple(digests))

    async def delete_manifest(self, repo: str, digest: str) -> None:
        await self._request("DELETE", f"/v2/{repo}/manifests/{digest}")

    async def delete_blob(self, repo: str, digest: str) -> None:
        await self._request("DELETE", f"/v2/{repo}/blobs/{digest}")


class ZotAdapter:
    name = "zot"

    def __init__(self, client: RegistryClient, *, repo_prefix: str = DEFAULT_REPO_PREFIX) -> None:
        self._client = client
        self._prefix = repo_prefix

    async def snapshot(self) -> dict[str, int]:
        counters: dict[str, int] = {}
        repos = [r for r in await self._client.catalog() if r.startswith(self._prefix)]
        for repo in sorted(repos):
            for tag in await self._client.tags(repo):
                counters[f"tags:{repo}:{tag}"] = 1
        return counters

    async def inject(self, case_id: str) -> InjectedResource:
        repo = f"{self._prefix}{case_id}"
        tag = "drift"
        config = json.dumps({"architecture": "amd64", "os": "linux"}).encode("utf-8")
        layer = f"wo101-drift:{case_id}".encode()
        pushed = await self._client.push_manifest(repo, tag, config, layer)
        aux = json.dumps({"digest": pushed.digest, "blobs": list(pushed.blob_digests)})
        return InjectedResource(
            system=self.name, case_id=case_id, handle=repo, created_at=utcnow(), aux=aux
        )

    async def cleanup(self, resource: InjectedResource) -> None:
        detail: dict[str, Any] = json.loads(resource.aux) if resource.aux else {}
        if detail.get("digest"):
            await self._client.delete_manifest(resource.handle, str(detail["digest"]))
        for blob in detail.get("blobs", []):
            await self._client.delete_blob(resource.handle, str(blob))
