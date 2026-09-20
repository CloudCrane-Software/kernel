"""reconciler/external/s3.py — minimal path-style S3 client with SigV4 (WO-101).

Just enough S3 for the MinIO reconciler adapter: ListBuckets, ListObjectsV2
(paginated count), PutObject, DeleteObject, CreateBucket, DeleteBucket.
Single fixed credential, UNSIGNED-PAYLOAD (body bytes still sent; only the
signature treats them as unsigned — standard S3 SigV4 option). No boto3 in
the kernel supply chain.
"""

from __future__ import annotations

import hashlib
import hmac
import xml.etree.ElementTree as ET
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, urlsplit

import httpx


class S3Error(RuntimeError):
    pass


def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _localname(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _xml_findall(root: ET.Element[Any], name: str) -> list[ET.Element[Any]]:
    return [el for el in root.iter() if _localname(el.tag) == name]


def _uri_encode(raw: str) -> str:
    return quote(raw, safe="-_.~")


class S3Client:
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._endpoint = endpoint.rstrip("/")
        self._access_key = access_key
        self._secret_key = secret_key
        self._region = region
        self._transport = transport
        self._timeout = timeout

    # -- SigV4 core ------------------------------------------------------

    def _auth_headers(
        self, method: str, path: str, params: Sequence[tuple[str, str]], extra: Mapping[str, str]
    ) -> dict[str, str]:
        split = urlsplit(self._endpoint)
        host = split.netloc
        now = datetime.now(UTC)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        payload_hash = "UNSIGNED-PAYLOAD"

        canonical_uri = quote(path, safe="/-_.~") or "/"
        pairs = sorted((_uri_encode(k), _uri_encode(v)) for k, v in params)
        canonical_query = "&".join(f"{k}={v}" for k, v in pairs)

        headers = {
            "host": host,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
            **{k.lower(): v for k, v in extra.items()},
        }
        canonical_headers = "".join(f"{k}:{headers[k]}\n" for k in sorted(headers))
        signed_headers = ";".join(sorted(headers))
        canonical_request = "\n".join(
            [
                method,
                canonical_uri,
                canonical_query,
                canonical_headers,
                signed_headers,
                payload_hash,
            ]
        )
        scope = f"{date_stamp}/{self._region}/s3/aws4_request"
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ]
        )
        k_date = _sign(f"AWS4{self._secret_key}".encode(), date_stamp)
        k_region = _sign(k_date, self._region)
        k_service = _sign(k_region, "s3")
        k_signing = _sign(k_service, "aws4_request")
        signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        authorization = (
            f"AWS4-HMAC-SHA256 Credential={self._access_key}/{scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )
        return {**headers, "Authorization": authorization}

    async def _request(
        self,
        method: str,
        path: str,
        params: Sequence[tuple[str, str]] = (),
        *,
        body: bytes = b"",
        content_type: str | None = None,
    ) -> httpx.Response:
        extra: dict[str, str] = {}
        if content_type:
            extra["content-type"] = content_type
        headers = self._auth_headers(method, path, params, extra)
        query = "&".join(f"{k}={v}" for k, v in params) if params else ""
        url = f"{self._endpoint}{quote(path, safe='/')}" + (f"?{query}" if query else "")
        async with httpx.AsyncClient(transport=self._transport, timeout=self._timeout) as client:
            resp = await client.request(method, url, headers=headers, content=body)
        if resp.status_code >= 300:
            raise S3Error(f"{method} {path} -> {resp.status_code}: {resp.text[:200]}")
        return resp

    # -- operations ------------------------------------------------------

    async def list_buckets(self) -> list[str]:
        resp = await self._request("GET", "/")
        root = ET.fromstring(resp.text)
        return [el.text or "" for el in _xml_findall(root, "Name")]

    async def create_bucket(self, bucket: str) -> None:
        await self._request("PUT", f"/{bucket}")

    async def delete_bucket(self, bucket: str) -> None:
        await self._request("DELETE", f"/{bucket}")

    async def put_object(self, bucket: str, key: str, data: bytes) -> None:
        await self._request(
            "PUT", f"/{bucket}/{key}", body=data, content_type="application/octet-stream"
        )

    async def delete_object(self, bucket: str, key: str) -> None:
        await self._request("DELETE", f"/{bucket}/{key}")

    async def count_objects(self, bucket: str, prefix: str = "") -> int:
        total = 0
        token: str | None = None
        while True:
            params: list[tuple[str, str]] = [("list-type", "2")]
            if prefix:
                params.append(("prefix", prefix))
            if token:
                params.append(("continuation-token", token))
            resp = await self._request("GET", f"/{bucket}", params)
            root = ET.fromstring(resp.text)
            total += len(_xml_findall(root, "Contents"))
            truncated = next((el.text == "true" for el in _xml_findall(root, "IsTruncated")), False)
            if not truncated:
                return total
            token = next((el.text for el in _xml_findall(root, "NextContinuationToken")), None)
            if token is None:
                raise S3Error("truncated listing without continuation token")
