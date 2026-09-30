import asyncio
import hashlib
from pathlib import Path

import pytest

from src.core.backup_database import BackupDatabase
from src.core import media_links as ml
from src.core.media_links import (MediaLinkError, MediaLinkResolver, attach_link_media, embed_refs_link,
                                  extract_links, link_key)

U1 = "https://media.discordapp.net/attachments/111/222/attachment.gif"
U2 = "https://cdn.discordapp.com/attachments/111/333/cat.png?ex=abc&is=def&hm=123"


# ── extraction ─────────────────────────────────────────────────────────────

def test_extract_links_normalizes_and_dedupes():
    text = f"look {U1} and <{U2}> again {U1}?ex=1&is=2&hm=3 (https://cdn.discordapp.com/attachments/111/444/a.webp)"
    got = extract_links(text)
    assert [k for _, k in got] == ["111/222/attachment.gif", "111/333/cat.png", "111/444/a.webp"]
    assert got[1][0] == U2                          # full URL as pasted (with signature) is kept
    assert extract_links("no links, https://example.com/attachments/1/2/x.png") == []
    assert extract_links("https://cdn.discordapp.com/emojis/123.png") == []       # emojis don't expire; not handled here


# ── helpers ────────────────────────────────────────────────────────────────

class _FakeResp:
    def __init__(self, status=200, body=None, data=b"", headers=None):
        self.status, self._body, self._data, self.headers = status, body, data, headers or {}
        self.content = self

    async def json(self):
        return self._body

    async def iter_chunked(self, n):
        for i in range(0, len(self._data), n):
            yield self._data[i:i + n]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _FakeSession:
    """post() = Discord refresh endpoint; get() = CDN download."""

    def __init__(self, blobs, dead=(), limit_first=False, status=200):
        self.blobs, self.dead, self.limit_first, self.status = blobs, set(dead), limit_first, status
        self.posts, self.gets = [], []

    def post(self, url, json=None, headers=None):
        self.posts.append(json["attachment_urls"])
        if self.status != 200:
            return _FakeResp(self.status)
        if self.limit_first and len(self.posts) == 1:
            return _FakeResp(429, {"retry_after": 0.01})
        refreshed = [{"original": u, "refreshed": u + "?ex=1&is=2&hm=3"} for u in json["attachment_urls"]
                     if not any(d in u for d in self.dead)]
        return _FakeResp(200, {"refreshed_urls": refreshed})

    def get(self, url):
        self.gets.append(url)
        base = url.split("?")[0]
        if base not in self.blobs:                           # refreshed URL that 404s (deleted attachment)
            return _FakeResp(404)
        data = self.blobs[base]
        return _FakeResp(200, data=data, headers={"Content-Type": "image/gif", "Content-Length": str(len(data))})

    async def close(self):
        pass


def _db(tmp_path, contents):
    db = BackupDatabase(tmp_path / "backup.db")
    db.save_messages_batch([
        {"id": 1000 + i, "channel_id": 1, "author_id": 1, "content": c, "timestamp": "2024-01-01T00:00:00", "type": 0,
         "message_reference": None, "is_pinned": 0, "extra_data": None, "custom_display_name": None,
         "custom_avatar_url": None, "attachments": [], "embeds": [], "reactions": [], "stickers": []}
        for i, c in enumerate(contents)])
    return db


GIF = b"GIF89a" + b"\x00" * 300


# ── resolver ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_same_bytes_under_different_urls_are_stored_once(tmp_path):
    a = "https://media.discordapp.net/attachments/1/10/x.gif"
    b = "https://cdn.discordapp.com/attachments/2/20/y.gif"
    db = _db(tmp_path, [f"one {a}", f"two {b}", f"again {a}"])
    sess = _FakeSession({a: GIF, b: GIF})                     # identical content, different attachments
    stats = await MediaLinkResolver(db, tmp_path, "tok", session=sess).resolve_backup()
    assert stats["distinct"] == 2 and stats["ok"] == 2 and stats["new_files"] == 1 and stats["dedup_hits"] == 1
    assert len(list((tmp_path / "attachments").glob("*.gif"))) == 1                       # one file on disk
    digest = hashlib.sha256(GIF).hexdigest()
    assert db.get_link_media("1/10/x.gif")["hash"] == db.get_link_media("2/20/y.gif")["hash"] == digest
    assert db.get_media_by_hash(digest)["local_path"] == f"attachments/{digest}.gif"
    assert not list((tmp_path / "attachments").glob("*.part"))                            # no temp leftovers


@pytest.mark.asyncio
async def test_dead_links_recorded_and_skipped_next_time(tmp_path):
    a = "https://media.discordapp.net/attachments/1/10/x.gif"
    b = "https://media.discordapp.net/attachments/1/11/gone.gif"
    db = _db(tmp_path, [a, b])
    sess = _FakeSession({a: GIF}, dead=["gone"])
    r1 = await MediaLinkResolver(db, tmp_path, "tok", session=sess).resolve_backup()
    assert (r1["ok"], r1["dead"]) == (1, 1) and db.get_link_media("1/11/gone.gif")["status"] == "dead"
    sess2 = _FakeSession({a: GIF}, dead=["gone"])
    r2 = await MediaLinkResolver(db, tmp_path, "tok", session=sess2).resolve_backup()
    assert r2["already_ok"] == 1 and r2["skipped_dead"] == 1 and r2["pending"] == 0 and sess2.posts == []
    r3 = await MediaLinkResolver(db, tmp_path, "tok", session=_FakeSession({a: GIF}, dead=["gone"])).resolve_backup(retry_dead=True)
    assert r3["pending"] == 1                                                            # dead link retried on request


@pytest.mark.asyncio
async def test_dry_run_changes_nothing_and_size_cap(tmp_path):
    a = "https://media.discordapp.net/attachments/1/10/x.gif"
    db = _db(tmp_path, [a])
    r = await MediaLinkResolver(db, tmp_path, "tok", session=_FakeSession({a: GIF})).resolve_backup(dry_run=True)
    assert r["pending"] == 1 and db.get_link_media("1/10/x.gif") is None
    big = await MediaLinkResolver(db, tmp_path, "tok", max_bytes=100, session=_FakeSession({a: GIF})).resolve_backup()
    assert big["too_large"] == 1 and db.get_link_media("1/10/x.gif")["status"] == "too_large"
    assert not list((tmp_path / "attachments").iterdir())


@pytest.mark.asyncio
async def test_refresh_rate_limit_is_waited_out_and_bad_token_is_fatal(tmp_path):
    a = "https://media.discordapp.net/attachments/1/10/x.gif"
    db = _db(tmp_path, [a])
    sess = _FakeSession({a: GIF}, limit_first=True)
    r = await MediaLinkResolver(db, tmp_path, "tok", session=sess).resolve_backup()
    assert r["ok"] == 1 and len(sess.posts) == 2                                         # 429 then success
    (tmp_path / "x").mkdir()
    db2 = _db(tmp_path / "x", [a])
    with pytest.raises(MediaLinkError):
        await MediaLinkResolver(db2, tmp_path / "x", "bad", session=_FakeSession({a: GIF}, status=401)).resolve_backup()


# ── migration-time conversion ──────────────────────────────────────────────

def _resolved(tmp_path, content):
    db = _db(tmp_path, [content])
    digest = hashlib.sha256(GIF).hexdigest()
    (tmp_path / "attachments").mkdir(exist_ok=True)
    (tmp_path / "attachments" / f"{digest}.gif").write_bytes(GIF)
    db.add_media_to_pool(digest, f"attachments/{digest}.gif", len(GIF), "image/gif", U1)
    db.set_link_media("111/222/attachment.gif", "ok", hash=digest, filename="attachment.gif", size=len(GIF))
    return db


def test_attach_link_media_turns_resolved_links_into_files(tmp_path):
    db = _resolved(tmp_path, "check this   " + U1 + "  out")
    content, files, keys = attach_link_media("check this   " + U1 + "  out", db, tmp_path)
    assert content == "check this out" and keys == {"111/222/attachment.gif"}
    assert files == [{"filename": "attachment.gif", "data": GIF}]


def test_unresolved_or_limited_links_stay_as_text(tmp_path):
    db = _resolved(tmp_path, U1)
    other = "see https://cdn.discordapp.com/attachments/9/9/unknown.png"
    assert attach_link_media(other, db, tmp_path) == (other, [], set())                 # never resolved
    assert attach_link_media(U1, db, tmp_path, max_files=0)[1:] == ([], set())          # no room for attachments
    assert attach_link_media(U1, db, tmp_path, max_bytes=10)[1:] == ([], set())         # over the upload cap
    db.set_link_media("111/222/attachment.gif", "dead")
    assert attach_link_media(U1, db, tmp_path)[1:] == ([], set())                       # dead stays a link


def test_embed_mirroring_a_replaced_link_is_dropped():
    keys = {"111/222/attachment.gif"}
    assert embed_refs_link({"url": U1, "type": "gifv"}, keys)
    assert embed_refs_link({"video": {"url": U1 + "?ex=1"}}, keys)
    assert not embed_refs_link({"url": "https://example.com/x"}, keys)
    assert not embed_refs_link({"url": "https://cdn.discordapp.com/attachments/1/2/other.gif"}, keys)


@pytest.mark.asyncio
async def test_signed_url_that_404s_is_dead_not_a_retryable_error(tmp_path):
    ghost = "https://media.discordapp.net/attachments/1/12/deleted.gif"
    db = _db(tmp_path, [ghost])
    r = await MediaLinkResolver(db, tmp_path, "tok", session=_FakeSession({})).resolve_backup()
    assert r["dead"] == 1 and r["error"] == 0 and db.get_link_media("1/12/deleted.gif")["status"] == "dead"
