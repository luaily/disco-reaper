import asyncio
import sys
import time
import types
from datetime import datetime, timezone

import pytest

try:
    import fluxer  # noqa: F401
except ImportError:
    stub = types.ModuleType("fluxer")
    for name in ("Bot", "Webhook", "Forbidden", "File"):
        setattr(stub, name, type(name, (), {}))
    sys.modules["fluxer"] = stub

import src.fluxer.migrate_message as mm
import src.fluxer.writer as wm
from src.core.configuration import AppConfig
from src.core.database import MigrationDatabase
from src.fluxer.writer import FluxerWriter, MessageSendError, SendTimeout

_real_sleep = asyncio.sleep


async def _fast(_):
    await _real_sleep(0)


# ── writer: timeout scales with payload; a timed-out send checks whether it actually landed ──────────

def test_upload_timeout_scales_with_payload():
    assert wm._upload_timeout(None) == wm._SEND_TIMEOUT
    assert wm._upload_timeout([{"data": b"x" * 10}]) == pytest.approx(wm._SEND_TIMEOUT, abs=0.01)
    assert wm._upload_timeout([{"data": b"x" * 10_000_000}]) == pytest.approx(wm._SEND_TIMEOUT + 100, abs=0.5)
    assert wm._upload_timeout([{"data": b"x" * 500_000_000}]) == 900.0                  # capped


class _SlowWebhook:
    id, token = 11, "tok"

    async def send(self, **kw):
        await _real_sleep(30)                    # never answers within the (patched) timeout


class _HTTP:
    def __init__(self, recent):
        self.recent, self.after = recent, None

    def _route(self, *a, **k):
        return None

    async def get_messages(self, channel_id, *, limit=50, after=None, before=None):
        self.after = after
        return self.recent


def _writer(recent):
    w = FluxerWriter(token="t", community_id="1")
    w.bot = types.SimpleNamespace(_http=_HTTP(recent))
    w._ready_event.set()
    w._webhooks["chan"] = _SlowWebhook()
    return w


@pytest.mark.asyncio
async def test_timed_out_send_that_actually_landed_is_not_retried(monkeypatch):
    monkeypatch.setattr(wm, "_SEND_TIMEOUT", 0.05)
    body = "-# <t:100:D>\nhello"
    w = _writer([{"id": "999", "content": body, "author": {"username": "Bob (discord)"}},
                 {"id": "998", "content": "other", "author": {"username": "Bob (discord)"}}])
    assert await w.send_message(channel_id="chan", author_name="Bob", content="hello", timestamp=100) == "999"
    assert int(w.client.after) > 0                                  # bounded by time via the snowflake


@pytest.mark.asyncio
async def test_timed_out_send_not_found_still_raises(monkeypatch):
    monkeypatch.setattr(wm, "_SEND_TIMEOUT", 0.05)
    w = _writer([{"id": "1", "content": "-# <t:100:D>\nhello", "author": {"username": "SomeoneElse (discord)"}}])
    with pytest.raises(SendTimeout):
        await w.send_message(channel_id="chan", author_name="Bob", content="hello", timestamp=100)


# ── migration database ───────────────────────────────────────────────────

def test_attempt_and_skip_records(tmp_path):
    db = MigrationDatabase(tmp_path / "m.db", "fluxer")
    assert db.get_message_attempts(5) == 0
    assert db.record_message_attempt(5, "boom") == 1 and db.record_message_attempt(5, "boom2") == 2
    db.record_skipped_message(5, 77, "Alice", "boom2", 2)
    assert db.get_message_attempts(5) == 0                          # cleared once skipped
    row = db.get_skipped_messages()[0]
    assert (row["source_id"], row["channel_id"], row["author"], row["attempts"]) == ("5", "77", "Alice", 2)
    db.record_message_attempt(6, "x")
    db.clear_all_migration_data()
    assert db.get_skipped_messages() == [] and db.get_message_attempts(6) == 0


def test_config_default_is_five():
    assert AppConfig().max_message_attempts == 5


# ── retry then skip ───────────────────────────────────────────────────────

class _State:
    def __init__(self, prior=None):
        self.attempts = dict(prior or {})
        self.skipped, self.mapped, self.progress, self.cursor = [], {}, [], None

    def record_message_attempt(self, mid, err=""):
        self.attempts[str(mid)] = self.attempts.get(str(mid), 0) + 1
        return self.attempts[str(mid)]

    def clear_message_attempts(self, mid):
        self.attempts.pop(str(mid), None)

    def record_skipped_message(self, mid, ch, author, reason, attempts):
        self.skipped.append((str(mid), author, attempts))
        self.attempts.pop(str(mid), None)

    def get_user_alias(self, uid):
        return "Alias"

    def set_message_mapping(self, ch, mid, tid):
        self.mapped[str(mid)] = tid

    def update_last_message_timestamp(self, ch, ts):
        pass

    def update_last_message_id(self, ch, mid):
        self.progress.append(str(mid))

    def get_all_last_message_ids(self):
        return {}

    def get_target_channel_id(self, _):
        return "T"

    def set_waterfall_cursor(self, mid):
        self.cursor = mid


class _Writer:
    def __init__(self, ok=True):
        self.markers, self.ok = [], ok

    async def send_marker(self, channel_id, content, **kw):
        self.markers.append(content)
        return "marker-1" if self.ok else None


def _msg(i=42):
    return types.SimpleNamespace(id=i, author=types.SimpleNamespace(id=1, display_name="Alice"),
                                 created_at=datetime(2024, 1, 1, tzinfo=timezone.utc), channel=types.SimpleNamespace(id=9),
                                 type=0, thread=None, jump_url=f"u{i}")


def _ctx(max_attempts=5, prior=None, marker_ok=True):
    ctx = types.SimpleNamespace(is_running=True, deadline=None, state=_State(prior), fluxer_writer=_Writer(marker_ok),
                                config=types.SimpleNamespace(max_message_attempts=max_attempts, anonymize_users=False),
                                on_notice=None)
    ctx.deadline_reached = lambda: ctx.deadline is not None and time.time() >= ctx.deadline
    ctx.notices = []
    ctx.on_notice = ctx.notices.append
    return ctx


def _failing(times):
    calls = {"n": 0}

    async def fake(context, msg, **kw):
        calls["n"] += 1
        if calls["n"] <= times:
            raise MessageSendError("Send timed out after 45s (delivery unknown)")
        return "sent-1"

    return fake, calls


@pytest.mark.asyncio
async def test_succeeds_after_retries_and_clears_attempts(monkeypatch):
    monkeypatch.setattr(mm.asyncio, "sleep", _fast)
    fake, calls = _failing(2)
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    ctx = _ctx(5)
    stats = {}
    assert await mm._process_with_retries(ctx, _msg(), "T", stats) == "sent-1"
    assert calls["n"] == 3 and ctx.state.attempts == {} and ctx.state.skipped == [] and "skipped" not in stats
    assert any("Attempt 2/5" in n for n in ctx.notices)


@pytest.mark.asyncio
async def test_skips_after_max_attempts_with_marker_and_progress(monkeypatch):
    monkeypatch.setattr(mm.asyncio, "sleep", _fast)
    fake, calls = _failing(99)
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    ctx = _ctx(3)
    stats = {}
    assert await mm._process_with_retries(ctx, _msg(42), "T", stats) == "marker-1"
    assert calls["n"] == 3
    assert ctx.state.skipped == [("42", "Alice", 3)] and stats["skipped"] == 1 and stats["skipped_ids"] == ["42"]
    assert ctx.state.mapped == {"42": "marker-1"} and ctx.state.progress == ["42"]       # handled: resume won't retry it
    text = ctx.fluxer_writer.markers[0]
    assert "`42`" in text and "Alice" in text and "after 3 attempts" in text and "skipping" in text


@pytest.mark.asyncio
async def test_skip_still_advances_progress_if_the_marker_cannot_be_posted(monkeypatch):
    monkeypatch.setattr(mm.asyncio, "sleep", _fast)
    fake, _ = _failing(99)
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    ctx = _ctx(2, marker_ok=False)
    stats = {}
    assert await mm._process_with_retries(ctx, _msg(7), "T", stats) is None
    assert ctx.state.skipped and ctx.state.progress == ["7"] and ctx.state.mapped == {}


@pytest.mark.asyncio
async def test_attempts_persist_across_runs(monkeypatch):
    monkeypatch.setattr(mm.asyncio, "sleep", _fast)
    fake, calls = _failing(99)
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    ctx = _ctx(5, prior={"42": 3})                                   # already failed 3 times in earlier runs
    await mm._process_with_retries(ctx, _msg(42), "T", {})
    assert calls["n"] == 2 and ctx.state.skipped                      # only 2 more tries before the 5th failure


@pytest.mark.asyncio
async def test_zero_disables_skipping_and_cancel_is_never_counted(monkeypatch):
    monkeypatch.setattr(mm.asyncio, "sleep", _fast)
    fake, _ = _failing(99)
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    ctx = _ctx(0)
    with pytest.raises(MessageSendError):
        await mm._process_with_retries(ctx, _msg(), "T", {})
    assert ctx.state.attempts == {}
    ctx2 = _ctx(5)
    ctx2.is_running = False                                          # user cancelled
    with pytest.raises(MessageSendError):
        await mm._process_with_retries(ctx2, _msg(), "T", {})
    assert ctx2.state.attempts == {} and ctx2.state.skipped == []


# ── the waterfall keeps going after a skip ────────────────────────────────

class _Reader:
    MESSAGE_TYPE_DEFAULT, MESSAGE_TYPE_REPLY, MESSAGE_TYPE_THREAD_STARTER, MESSAGE_TYPE_FORWARD = 0, 19, 21, 99
    MESSAGE_TYPE_CHAT_INPUT_COMMAND, MESSAGE_TYPE_CONTEXT_MENU_COMMAND = 20, 23
    MESSAGE_TYPE_POLL_RESULT, MESSAGE_TYPE_AUTO_MODERATION_ACTION = 46, 24

    async def fetch_global_message_history(self, after_id=None):
        for i in (1, 2, 3):
            yield _msg(i)


@pytest.mark.asyncio
async def test_waterfall_continues_past_a_skipped_message(monkeypatch):
    monkeypatch.setattr(mm.asyncio, "sleep", _fast)
    sent = []

    async def fake(context, msg, **kw):
        if msg.id == 2:
            raise MessageSendError("keeps failing")
        sent.append(msg.id)
        kw["stats"]["messages"] += 1
        return "ok"

    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    ctx = _ctx(2)
    ctx.discord_reader = _Reader()
    res = await mm.migrate_global_messages(ctx)
    assert sent == [1, 3] and res["messages"] == 2 and res["skipped"] == 1 and "error" not in res
    assert ctx.state.cursor == 3 and ctx.state.skipped[0][0] == "2"
