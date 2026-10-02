"""Presigned attachment uploads for Fluxer.

The multipart form that fluxer.py's Webhook.send / HTTPClient.send_message use pushes every file byte through Fluxer's
API servers inside a single request. Under load that shows up as 503s, a big upload can hit the client's fixed 5-minute
limit, and every library retry re-sends the whole payload. Fluxer also offers a direct route:

  1. POST /channels/{channel_id}/attachments          -> presigned storage URLs (<=10 files per call)
       file <= 10 MB : upload_mode "singlepart"  -> one PUT to upload_url
       file  > 10 MB : upload_mode "multipart"   -> PUT each part (part_size bytes) to its URL, then
  2. POST /channels/{channel_id}/attachments/complete  {"uploads": [{"upload_filename", "upload_id"}]}
  3. the message payload then just *references* each upload:
       {"id", "filename", "content_type", "upload_filename", "file_size"}

so the message POST is a few hundred bytes of JSON and a failed/slow upload can never have delivered anything.
(The OpenAPI spec lists the endpoints at https://api.fluxer.app/v1/openapi.json; the docs site names the first one
".../attachments/presigned", but the live API serves it at ".../attachments".)
"""
import asyncio
import logging
import mimetypes
import re
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

MAX_PER_PRESIGN = 10          # Fluxer limit per presign request
PUT_ATTEMPTS = 4              # per PUT: 1s, 2s, 4s backoff between tries
FILE_ATTEMPTS = 3             # whole-file tries (fresh presign) when a presigned URL was rejected / expired
MIN_UPLOAD_BPS = 40_000       # presigned URLs live ~300s, so a PUT is given 60s + size/40KB/s, capped below that
PUT_TIMEOUT_CAP = 280.0


class PresignUnavailable(Exception):
    """The instance doesn't offer (or refuses) presigned uploads: fall back to the multipart form."""


class FileTooLarge(Exception):
    """Fluxer refused a file for exceeding its per-file limit (FILE_SIZE_TOO_LARGE). `limit` is in bytes (0 = unknown)."""

    def __init__(self, limit: int):
        super().__init__(f"file exceeds Fluxer's per-file limit ({limit} bytes)")
        self.limit = limit


class PresignRejected(Exception):
    """The presign request was refused for this message only (bad value, missing permission, ...). Use the multipart
    form for this message; presigned uploads stay enabled."""


class UploadError(Exception):
    """A transient upload failure that survived the retries (outage, stall, 5xx). Nothing was posted."""


class _PutRejected(Exception):
    def __init__(self, status: int):
        super().__init__(f"storage rejected the upload (HTTP {status})")
        self.status = status


def guess_content_type(filename: str) -> str:
    return mimetypes.guess_type(filename)[0] or "application/octet-stream"


class PresignedUploader:
    def __init__(self, client: Any, session: Any, cancelled: Callable[[], bool] = lambda: False, sleep=None):
        self.client = client          # fluxer.HTTPClient (authenticated; handles 429/5xx retry for the JSON calls)
        self.session = session        # plain aiohttp session WITHOUT the bot Authorization header (storage URLs)
        self.cancelled = cancelled
        self._sleep = sleep or asyncio.sleep

    # ── JSON calls through the library client ──────────────────────────────
    async def _json(self, method: str, path: str, channel_id: str, body: dict) -> dict:
        route = self.client._route(method, path, channel_id=channel_id)
        try:
            return await self.client.request(route, json=body)
        except RuntimeError as e:                      # "Failed after N attempts" (429/5xx/connection, retries spent)
            raise UploadError(str(e)) from e
        except Exception as e:
            status = getattr(e, "status", None)
            if isinstance(status, int) and 400 <= status < 500 and status != 429:
                code, msg = getattr(e, "code", None), str(getattr(e, "message", e))
                if code == "FILE_SIZE_TOO_LARGE":
                    m = re.search(r"maximum[^0-9]*(\d+)", msg, re.IGNORECASE)   # "Maximum file size is 52428800."
                    raise FileTooLarge(int(m.group(1)) if m else 0) from e
                if status in (404, 405, 501):                    # the endpoint isn't there at all
                    raise PresignUnavailable(f"HTTP {status}: {msg}") from e
                raise PresignRejected(f"HTTP {status} {code}: {msg}") from e
            raise UploadError(str(e)) from e

    async def _put(self, url: str, data: bytes) -> None:
        import aiohttp
        timeout = aiohttp.ClientTimeout(total=min(PUT_TIMEOUT_CAP, 60 + len(data) / MIN_UPLOAD_BPS), sock_connect=30)
        last: Optional[Exception] = None
        for attempt in range(PUT_ATTEMPTS):
            if self.cancelled():
                raise UploadError("Cancelled during upload")
            try:
                async with self.session.put(url, data=data, headers={"Content-Length": str(len(data))},
                                            timeout=timeout) as r:
                    if r.status < 300:
                        return
                    if r.status == 429 or r.status >= 500:       # storage under load: back off and retry the same URL
                        last = RuntimeError(f"storage HTTP {r.status}")
                    else:
                        raise _PutRejected(r.status)             # 403 = expired/invalid signature: needs a fresh presign
            except _PutRejected:
                raise
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                last = e
            if attempt < PUT_ATTEMPTS - 1:
                await self._sleep(2 ** attempt)
        raise UploadError(f"upload failed after {PUT_ATTEMPTS} tries: {last}")

    # ── one file ───────────────────────────────────────────────────────────
    async def _upload_item(self, channel_id: str, item: dict, data: bytes) -> str:
        if item.get("upload_mode") == "multipart":
            part_size = int(item["part_size"])
            for p in item["parts"]:
                n = int(p["part_number"])
                await self._put(p["upload_url"], data[(n - 1) * part_size: n * part_size])
            res = await self._json("POST", "/channels/{channel_id}/attachments/complete", channel_id,
                                   {"uploads": [{"upload_filename": item["upload_filename"], "upload_id": item["upload_id"]}]})
            return res["uploads"][0]["upload_filename"]
        await self._put(item["upload_url"], data)
        return item["upload_filename"]

    async def upload(self, channel_id: str, files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Uploads every {"filename","data"} and returns message-ready attachment descriptors.
        Raises PresignUnavailable (use the multipart form instead) or UploadError (transient, nothing posted)."""
        out: List[Dict[str, Any]] = []
        for start in range(0, len(files), MAX_PER_PRESIGN):
            batch = files[start:start + MAX_PER_PRESIGN]
            metas = [{"id": i, "filename": f["filename"], "file_size": len(f["data"]),
                      "content_type": guess_content_type(f["filename"])} for i, f in enumerate(batch)]
            for attempt in range(FILE_ATTEMPTS):
                try:
                    plan = await self._json("POST", "/channels/{channel_id}/attachments", channel_id, {"attachments": metas})
                    items = {int(it["id"]): it for it in plan["attachments"]}
                    keys = {}
                    for i, f in enumerate(batch):
                        keys[i] = await self._upload_item(channel_id, items[i], f["data"])
                    break
                except (KeyError, TypeError, ValueError, IndexError) as e:   # response shape we don't understand
                    raise PresignUnavailable(f"unexpected presign response ({type(e).__name__}: {e})") from e
                except _PutRejected as e:                       # expired signature etc.: plan again from scratch
                    if attempt == FILE_ATTEMPTS - 1:
                        raise UploadError(str(e)) from e
                    logger.warning(f"Fluxer upload: {e}; requesting fresh upload URLs")
            for i, f in enumerate(batch):
                out.append({"id": start + i, "filename": f["filename"], "content_type": metas[i]["content_type"],
                            "upload_filename": keys[i], "file_size": len(f["data"])})
        return out
