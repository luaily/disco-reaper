"""Preserve Discord CDN media that was pasted as a *link* in message text.

Discord signs and expires attachment URLs, so an old pasted link like
    https://media.discordapp.net/attachments/<channel>/<attachment>/attachment.gif
is usually dead unless it is refreshed. A bot token can refresh it (POST /attachments/refresh-urls, up to 50 per
call, no extra permission), after which the file can be downloaded. This module

  * finds those links in a backup's message text,
  * refreshes + downloads each distinct attachment once,
  * hashes it (SHA-256) and stores it in the backup's content-addressed media pool, so a GIF pasted 3,000 times
    (under 3,000 different URLs) is stored once,
  * records the outcome per attachment in the `link_media` table so re-runs skip finished links, and
  * at migration time turns resolved links into real attachments (attach_link_media).

Fluxer's own attachment URLs are signed and expire too, so "upload once and paste that URL later" is not durable;
migrated messages each carry a real attachment instead (Fluxer de-duplicates by content hash on its side).
"""
import asyncio
import hashlib
import logging
import re
import shutil
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
from urllib.parse import unquote

logger = logging.getLogger(__name__)

REFRESH_URL = "https://discord.com/api/v10/attachments/refresh-urls"
REFRESH_BATCH = 50
UA = "DiscordBot (https://github.com/luaily/disco-reaper, 1.0)"

# cdn.discordapp.com / media.discordapp.net share the same path space; the query string is the (expiring) signature.
CDN_LINK_RE = re.compile(
    r"https?://(?:cdn\.discordapp\.com|media\.discordapp\.net)/(?:attachments|ephemeral-attachments)/"
    r"(\d+)/(\d+)/([^\s<>()\[\]\"'?]+)(?:\?[^\s<>()\[\]\"']*)?",
    re.IGNORECASE,
)


def link_key(channel_id: str, attachment_id: str, filename: str) -> str:
    """Stable identity of an attachment regardless of host or signature."""
    return f"{channel_id}/{attachment_id}/{filename}"


def extract_links(text: str) -> List[Tuple[str, str]]:
    """[(full_url_as_pasted, key)] for each distinct Discord CDN attachment link in `text`, in order."""
    seen, out = set(), []
    for m in CDN_LINK_RE.finditer(text or ""):
        key = link_key(m.group(1), m.group(2), m.group(3))
        if key not in seen:
            seen.add(key)
            out.append((m.group(0), key))
    return out


def base_url(url: str) -> str:
    """URL without its query string (what we send to the refresh endpoint)."""
    return url.split("?", 1)[0]


def filename_from_key(key: str) -> str:
    return unquote(key.rsplit("/", 1)[-1]) or "attachment"


def embed_refs_link(embed: Any, keys: set) -> bool:
    """True if a Discord auto-embed just mirrors one of the links we turned into an attachment."""
    d = embed.to_dict() if hasattr(embed, "to_dict") else embed
    if not isinstance(d, dict):
        return False
    urls = [d.get("url")]
    for part in ("image", "thumbnail", "video"):
        p = d.get(part)
        if isinstance(p, dict):
            urls += [p.get("url"), p.get("proxy_url")]
    for u in urls:
        if u:
            m = CDN_LINK_RE.match(str(u))
            if m and link_key(m.group(1), m.group(2), m.group(3)) in keys:
                return True
    return False


def attach_link_media(content: str, db: Any, backup_root: Path, max_files: int = 10,
                      max_bytes: int = 25 * 1024 * 1024) -> Tuple[str, List[Dict[str, Any]], set]:
    """Replace resolved CDN links in `content` with real attachments.

    Returns (new_content, [{"filename", "data"}...], replaced_keys). Links that are unresolved (dead, too large,
    not yet resolved, over max_files/max_bytes) are left in the text untouched."""
    files: List[Dict[str, Any]] = []
    replaced: set = set()
    if not content or max_files <= 0 or not hasattr(db, "get_link_media"):
        return content, files, replaced
    total = 0
    for url, key in extract_links(content):
        if len(files) >= max_files:
            break
        row = db.get_link_media(key)
        if not row or row.get("status") != "ok" or not row.get("hash"):
            continue
        pool = db.get_media_by_hash(row["hash"])
        if not pool:
            continue
        path = Path(backup_root) / pool["local_path"]
        try:
            size = path.stat().st_size
            if size > max_bytes or total + size > max_bytes:
                continue
            data = path.read_bytes()
        except OSError:
            continue
        total += size
        files.append({"filename": row.get("filename") or filename_from_key(key), "data": data})
        replaced.add(key)
    if not replaced:
        return content, files, replaced

    def _drop(m: "re.Match") -> str:
        return "" if link_key(m.group(1), m.group(2), m.group(3)) in replaced else m.group(0)

    new = CDN_LINK_RE.sub(_drop, content)
    new = re.sub(r"[ \t]{2,}", " ", new)
    new = re.sub(r"\n{3,}", "\n\n", new).strip()
    return new, files, replaced


class LinkGone(Exception):
    """The file no longer exists / is not accessible (permanent): the refresh endpoint signs any well-formed URL,
    so a deleted attachment only shows up as a 404/403/410 on download."""


class MediaLinkError(Exception):
    """Fatal problem for the whole run (e.g. the bot token was rejected)."""


class MediaLinkResolver:
    def __init__(self, db: Any, backup_root: Path, token: str, max_bytes: int = 100 * 1024 * 1024,
                 concurrency: int = 4, session: Any = None):
        self.db, self.root, self.token = db, Path(backup_root), token
        self.max_bytes, self.concurrency = max_bytes, max(1, concurrency)
        self._session = session          # injectable for tests
        self._own_session = session is None
        self._store_lock = threading.Lock()   # _store runs in worker threads; check-then-insert must be atomic
        (self.root / "attachments").mkdir(parents=True, exist_ok=True)

    # ── discovery ──────────────────────────────────────────────────────────
    def collect_links(self, limit: Optional[int] = None) -> Dict[str, str]:
        """{key: url} for every distinct CDN attachment link in stored message text (first URL seen wins)."""
        found: Dict[str, str] = {}
        for _mid, content in self.db.iter_link_candidates():
            for url, key in extract_links(content):
                found.setdefault(key, url)
            if limit and len(found) >= limit:
                break
        return found

    # ── network ────────────────────────────────────────────────────────────
    async def _get_session(self):
        if self._session is None:
            import aiohttp
            self._session = aiohttp.ClientSession(headers={"User-Agent": UA})
        return self._session

    async def close(self):
        if self._session is not None and hasattr(self._session, "close"):
            await self._session.close()
        self._session = None

    async def _refresh(self, urls: List[str]) -> Dict[str, str]:
        """{original: refreshed} for the URLs Discord could refresh (missing = gone/inaccessible)."""
        session = await self._get_session()
        for attempt in range(6):
            async with session.post(REFRESH_URL, json={"attachment_urls": urls},
                                    headers={"Authorization": f"Bot {self.token}", "User-Agent": UA}) as r:
                if r.status == 429:
                    retry = 1.0
                    try:
                        retry = float((await r.json()).get("retry_after", 1.0))
                    except Exception:
                        pass
                    logger.warning(f"media refresh rate limited; waiting {retry:.1f}s")
                    await asyncio.sleep(retry + 0.1)
                    continue
                if r.status in (401, 403):
                    raise MediaLinkError(f"Discord rejected the bot token for URL refresh (HTTP {r.status})")
                if r.status >= 400:
                    raise RuntimeError(f"refresh HTTP {r.status}")
                body = await r.json()
                return {x.get("original"): x.get("refreshed") for x in body.get("refreshed_urls", []) if x.get("refreshed")}
        raise RuntimeError("refresh kept being rate limited")

    async def _download(self, url: str) -> Tuple[Optional[Path], str, int, str]:
        """Stream to a temp file. -> (path|None, sha256, size, content_type). path None => too large."""
        session = await self._get_session()
        h, size = hashlib.sha256(), 0
        async with session.get(url) as r:
            if r.status in (403, 404, 410):
                raise LinkGone(f"download HTTP {r.status}")
            if r.status >= 400:
                raise RuntimeError(f"download HTTP {r.status}")
            ctype = r.headers.get("Content-Type", "") or ""
            clen = r.headers.get("Content-Length")
            if clen and clen.isdigit() and int(clen) > self.max_bytes:
                return None, "", int(clen), ctype
            tmp = tempfile.NamedTemporaryFile(delete=False, dir=self.root / "attachments", suffix=".part")
            try:
                async for chunk in r.content.iter_chunked(256 * 1024):
                    size += len(chunk)
                    if size > self.max_bytes:
                        tmp.close()
                        Path(tmp.name).unlink(missing_ok=True)
                        return None, "", size, ctype
                    h.update(chunk)
                    tmp.write(chunk)
                tmp.close()
            except BaseException:
                tmp.close()
                Path(tmp.name).unlink(missing_ok=True)
                raise
        return Path(tmp.name), h.hexdigest(), size, ctype

    def _store(self, tmp: Path, digest: str, size: int, ctype: str, filename: str, url: str) -> bool:
        """Move into the content-addressed pool unless that content is already there. -> True if it was new."""
        with self._store_lock:
            if self.db.get_media_by_hash(digest):
                tmp.unlink(missing_ok=True)
                return False
            ext = Path(filename).suffix
            target = self.root / "attachments" / f"{digest}{ext}"
            shutil.move(str(tmp), str(target))
            self.db.add_media_to_pool(digest, f"attachments/{digest}{ext}", size, ctype, base_url(url))
            return True

    # ── main entry ─────────────────────────────────────────────────────────
    async def resolve_backup(self, progress: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
                             retry_dead: bool = False, limit: Optional[int] = None,
                             dry_run: bool = False) -> Dict[str, Any]:
        links = self.collect_links(limit)
        stats = {"distinct": len(links), "already_ok": 0, "skipped_dead": 0, "pending": 0, "ok": 0, "dead": 0,
                 "error": 0, "too_large": 0, "new_files": 0, "dedup_hits": 0, "bytes_new": 0, "bytes_deduped": 0}
        todo: List[Tuple[str, str]] = []
        for key, url in links.items():
            row = self.db.get_link_media(key)
            st = row["status"] if row else None
            if st == "ok":
                stats["already_ok"] += 1
            elif st in ("dead", "too_large") and not retry_dead:
                stats["skipped_dead" if st == "dead" else "too_large"] += 1
            else:
                todo.append((key, url))
        stats["pending"] = len(todo)
        if dry_run or not todo:
            return stats

        sem = asyncio.Semaphore(self.concurrency)

        async def one(key: str, original: str, refreshed: Optional[str]):
            filename = filename_from_key(key)
            if not refreshed:
                self.db.set_link_media(key, "dead", filename=filename, error="refresh returned no URL")
                stats["dead"] += 1
                return
            async with sem:
                try:
                    tmp, digest, size, ctype = await self._download(refreshed)
                    if tmp is None:
                        self.db.set_link_media(key, "too_large", filename=filename, size=size, error="over size limit")
                        stats["too_large"] += 1
                        return
                    new = await asyncio.to_thread(self._store, tmp, digest, size, ctype, filename, refreshed)
                    self.db.set_link_media(key, "ok", hash=digest, filename=filename, size=size, content_type=ctype)
                    stats["ok"] += 1
                    if new:
                        stats["new_files"] += 1
                        stats["bytes_new"] += size
                    else:
                        stats["dedup_hits"] += 1
                        stats["bytes_deduped"] += size
                except MediaLinkError:
                    raise
                except LinkGone as e:
                    self.db.set_link_media(key, "dead", filename=filename, error=str(e))
                    stats["dead"] += 1
                except Exception as e:
                    logger.warning(f"media link {key}: {e}")
                    self.db.set_link_media(key, "error", filename=filename, error=str(e)[:200])
                    stats["error"] += 1

        try:
            for i in range(0, len(todo), REFRESH_BATCH):
                batch = todo[i:i + REFRESH_BATCH]
                sent = {key: base_url(url) for key, url in batch}
                try:
                    refreshed = await self._refresh(list(sent.values()))
                except MediaLinkError:
                    raise
                except Exception as e:
                    logger.warning(f"media refresh batch failed: {e}")
                    for key, _ in batch:
                        self.db.set_link_media(key, "error", filename=filename_from_key(key), error=f"refresh: {e}"[:200])
                        stats["error"] += 1
                    continue
                await asyncio.gather(*(one(k, sent[k], refreshed.get(sent[k])) for k, _ in batch))
                if progress:
                    await progress({**stats, "done": min(i + REFRESH_BATCH, len(todo)), "total": len(todo)})
        finally:
            if self._own_session:
                await self.close()
        return stats


def summarize(stats: Dict[str, Any]) -> str:
    mb = lambda b: f"{b / 1e6:.1f} MB"
    return (f"{stats['distinct']} distinct CDN links: {stats['already_ok']} already saved, {stats['ok']} saved now "
            f"({stats['new_files']} new files {mb(stats['bytes_new'])}, {stats['dedup_hits']} duplicates "
            f"{mb(stats['bytes_deduped'])} not stored again), {stats['dead']} dead, {stats['too_large']} too large, "
            f"{stats['error']} errors")
