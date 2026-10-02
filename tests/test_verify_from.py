import asyncio
import sys
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
from src.core.database import MigrationDatabase
from src.fluxer.server_index import ServerIndex

_real_sleep = asyncio.sleep
BOT = "Migration Bot"


def ts(i):
    return int(datetime(2024, 1, 1, 0, 0, i, tzinfo=timezone.utc).timestamp())


def wh(sid, name, epoch, body="hi"):
    return {"id": str(sid), "content": f"-# <t:{epoch}:D>\n{body}", "author": {"username": f"{name} (discord)"}}


def marker(sid, source_id):
    return {"id": str(sid), "content": f"⚠️ There was an error migrating message `{source_id}` from **Al** (<t:1:f>) after 5 attempts, skipping...",
            "author": {"username": BOT}}


class _Client:
    """Fake channel history. `messages` is oldest-first like a real channel; get_messages pages newest-first."""

    def __init__(self, messages, fail=False):
        self.messages, self.fail, self.pages = sorted(messages, key=lambda m: int(m["id"])), fail, []

    async def get_messages(self, channel_id, limit=50, before=None, after=None):
        if self.fail:
            raise RuntimeError("503")
        self.pages.append(before)
        pool = [m for m in self.messages if before is None or int(m["id"]) < int(before)]
        return list(reversed(pool))[:limit]


# ── ServerIndex ────────────────────────────────────────────────────────────

def test_fingerprints():
    f = ServerIndex.fingerprint
    assert f(wh(1, "Al", 100), BOT) == ("Al (discord)", 100)
    legacy = {"id": "2", "content": "-# <t:100:D>\n-# · Al\nhello", "author": {"username": BOT}}   # reply sent by an older build via the bot
    assert f(legacy, BOT) == ("Al (discord)", 100)
    assert f({"id": "3", "content": "-# <t:100:D>\nx", "author": {"username": "RandomUser"}}, BOT) is None
    assert f({"id": "4", "content": "plain chat", "author": {"username": "Al (discord)"}}, BOT) is None


@pytest.mark.asyncio
async def test_each_server_copy_is_used_once_and_markers_are_found():
    w = types.SimpleNamespace(client=_Client([wh(10, "Al", ts(5)), wh(11, "Al", ts(5)), wh(12, "Bo", ts(6)), marker(13, 777)]))
    idx = ServerIndex(w, start_ts=None, bot_username=BOT)
    assert await idx.find("c", "Al (discord)", ts(5)) == "10"          # oldest copy first
    assert await idx.find("c", "Al (discord)", ts(5)) == "11"
    assert await idx.find("c", "Al (discord)", ts(5)) is None          # two source messages, two copies: no third
    assert await idx.find("c", "Bo (discord)", ts(6)) == "12"
    assert await idx.find("c", "Bo (discord)", ts(59)) is None
    assert idx.marker_for("c", 777) == "13" and idx.marker_for("c", 1) is None


@pytest.mark.asyncio
async def test_a_marker_only_counts_when_the_bot_wrote_it():
    forged = marker(5, 123)
    forged["author"]["username"] = "SomeoneElse"
    idx = ServerIndex(types.SimpleNamespace(client=_Client([forged])), None, bot_username=BOT)
    await idx.find("c", "x", 1)
    assert idx.marker_for("c", 123) is None


@pytest.mark.asyncio
async def test_scan_stops_once_it_is_well_past_the_start_but_not_for_a_stray_old_resend():
    old = [wh(i, "Al", ts(1)) for i in range(1, 300)]                  # 299 old migrated messages
    new = [wh(1000 + i, "Al", ts(30 + i)) for i in range(5)]
    client = _Client(old + new)
    idx = ServerIndex(types.SimpleNamespace(client=client), start_ts=ts(20), bot_username=BOT, stop_after_older=100)
    await idx.find("c", "Al (discord)", ts(31))
    assert idx.scanned["c"] < 300                                       # did not read the whole channel
    # a late re-send of an OLD message sits among the newest ones: the scan must keep going past it
    stray = wh(2000, "Al", ts(2))
    client2 = _Client(old[:50] + [wh(1500, "Bo", ts(25))] + [stray] + new)
    idx2 = ServerIndex(types.SimpleNamespace(client=client2), start_ts=ts(20), bot_username=BOT, stop_after_older=3)
    assert await idx2.find("c", "Bo (discord)", ts(25)) == "1500"


@pytest.mark.asyncio
async def test_pagination_walks_backwards_through_the_channel():
    msgs = [wh(i, "Al", ts(i % 50)) for i in range(1, 251)]
    client = _Client(msgs)
    idx = ServerIndex(types.SimpleNamespace(client=client), start_ts=None, bot_username=BOT)
    await idx.find("c", "none (discord)", 1)
    assert idx.scanned["c"] == 250 and client.pages[0] is None and client.pages[1:] == [151, 51]


# ── the Waterfall in verify mode ───────────────────────────────────────────

class _State:
    def __init__(self, progress=None):
        self.mapped, self.progress_map, self.cleared, self.cursor, self.last = {}, progress or {}, [], None, []

    def get_all_last_message_ids(self):
        return self.progress_map

    def get_target_channel_id(self, _):
        return "T"

    def get_user_alias(self, uid):
        return "Alias"

    def set_message_mapping(self, ch, sid, tid):
        self.mapped[str(sid)] = tid

    def update_last_message_timestamp(self, *a):
        pass

    def update_last_message_id(self, ch, mid):
        self.last.append(str(mid))

    def clear_skipped_message(self, sid):
        self.cleared.append(str(sid))

    def record_skipped_message(self, *a):
        pass

    def record_message_attempt(self, *a):
        return 99

    def clear_message_attempts(self, *a):
        pass

    def set_waterfall_cursor(self, mid):
        self.cursor = mid


class _Writer:
    def __init__(self, client):
        self.client, self.deleted, self.markers = client, [], []

    def bot_username(self):
        return BOT

    async def delete_message(self, ch, mid):
        self.deleted.append(mid)
        return True

    async def send_marker(self, **kw):
        self.markers.append(kw["content"])
        return "newmarker"

    async def check_health(self, ch):
        return True, "ok"

    async def verify_message(self, ch, mid):
        return True


def _msg(i):
    return types.SimpleNamespace(id=i, author=types.SimpleNamespace(id=1, display_name="Al"),
                                 created_at=datetime(2024, 1, 1, 0, 0, i, tzinfo=timezone.utc),
                                 channel=types.SimpleNamespace(id=9), type=0, thread=None, jump_url=f"u{i}")


class _Reader:
    MESSAGE_TYPE_DEFAULT, MESSAGE_TYPE_REPLY, MESSAGE_TYPE_THREAD_STARTER, MESSAGE_TYPE_FORWARD = 0, 19, 21, 99
    MESSAGE_TYPE_CHAT_INPUT_COMMAND, MESSAGE_TYPE_CONTEXT_MENU_COMMAND = 20, 23
    MESSAGE_TYPE_POLL_RESULT, MESSAGE_TYPE_AUTO_MODERATION_ACTION = 46, 24

    async def fetch_global_message_history(self, after_id=None):
        for i in (1, 2, 3, 4):
            if after_id is None or i > after_id:
                yield _msg(i)


def _ctx(server_msgs, progress=None, fail=False, attempts=5):
    client = _Client(server_msgs, fail=fail)
    ctx = types.SimpleNamespace(is_running=True, deadline=None, state=_State(progress), fluxer_writer=_Writer(client),
                                discord_reader=_Reader(), on_notice=None,
                                config=types.SimpleNamespace(max_message_attempts=attempts, anonymize_users=False, max_outage_minutes=0))
    ctx.deadline_reached = lambda: False
    return ctx


@pytest.fixture(autouse=True)
def _quick(monkeypatch):
    async def fast(_):
        await _real_sleep(0)
    monkeypatch.setattr(mm.asyncio, "sleep", fast)


def _sender(sent):
    async def fake(context, msg, **kw):
        sent.append(msg.id)
        kw["stats"]["messages"] += 1
        return f"new{msg.id}"
    return fake


@pytest.mark.asyncio
async def test_only_missing_messages_are_sent_and_the_rest_adopted(monkeypatch):
    sent = []
    monkeypatch.setattr(mm, "_process_and_send_message", _sender(sent))
    # 1 and 3 are on the server; 2 was skipped (marker only); 4 never got there
    ctx = _ctx([wh(101, "Al", ts(1)), marker(102, 2), wh(103, "Al", ts(3))])
    res = await mm.migrate_global_messages(ctx, after_message_id=0, verify_server=True, start_ts=ts(1))
    assert sent == [2, 4] and res["messages"] == 2 and res["already_on_server"] == 2 and "error" not in res
    assert ctx.state.mapped == {"1": "101", "3": "103"}                 # database repaired from the server
    assert ctx.fluxer_writer.deleted == ["102"] and "2" in ctx.state.cleared   # the stale marker for 2 is gone
    assert ctx.state.cursor == 4


@pytest.mark.asyncio
async def test_the_database_is_not_trusted_in_verify_mode(monkeypatch):
    sent = []
    monkeypatch.setattr(mm, "_process_and_send_message", _sender(sent))
    ctx = _ctx([], progress={"T": "999999999"})                          # DB claims everything up to a huge id is done
    res = await mm.migrate_global_messages(ctx, after_message_id=0, verify_server=True, start_ts=ts(1))
    assert sent == [1, 2, 3, 4] and res["messages"] == 4
    # ...whereas a normal run would have believed the database and skipped all four
    sent.clear()
    ctx2 = _ctx([], progress={"T": "999999999"})
    res2 = await mm.migrate_global_messages(ctx2, after_message_id=0)
    assert sent == [] and res2["messages"] == 0


@pytest.mark.asyncio
async def test_unreadable_server_halts_instead_of_sending_blind(monkeypatch):
    sent = []
    monkeypatch.setattr(mm, "_process_and_send_message", _sender(sent))
    ctx = _ctx([], fail=True)
    res = await mm.migrate_global_messages(ctx, after_message_id=0, verify_server=True, start_ts=ts(1))
    assert sent == [] and "error" in res and "check for existing messages" in res["error"]       # no duplicates risked


@pytest.mark.asyncio
async def test_marker_is_kept_if_the_message_gets_skipped_again(monkeypatch):
    async def fails(context, msg, **kw):
        if msg.id == 2:
            return await mm._skip_message(context, msg, "T", "still broken", 5, kw["stats"])
        kw["stats"]["messages"] += 1
        return "ok"
    monkeypatch.setattr(mm, "_process_and_send_message", fails)
    ctx = _ctx([marker(102, 2)])
    res = await mm.migrate_global_messages(ctx, after_message_id=1, verify_server=True, start_ts=ts(2))
    assert res["skipped"] == 1 and ctx.fluxer_writer.deleted == []        # not deleted: it is still not on the server


@pytest.mark.asyncio
async def test_find_start_message():
    ctx = _ctx([])
    assert (await mm.find_start_message(ctx, 3)).id == 3
    with pytest.raises(ValueError):
        await mm.find_start_message(ctx, 99)


# ── progress never goes backwards ──────────────────────────────────────────

def test_progress_markers_only_move_forward(tmp_path):
    db = MigrationDatabase(tmp_path / "m.db", "fluxer")
    db.update_channel_tracking("1000", last_msg_id="500", last_msg_ts="2024-01-02")
    db.update_channel_tracking("1000", last_msg_id="300", last_msg_ts="2024-01-01")        # re-sending an older message
    t = db.get_channel_tracking("1000")
    assert str(t["last_msg_id"]) == "500" and t["last_msg_ts"] == "2024-01-02"
    db.update_channel_tracking("1000", last_msg_id="900")
    assert str(db.get_channel_tracking("1000")["last_msg_id"]) == "900"
    db.update_thread_tracking("1000", "th", last_msg_id="70")
    db.update_thread_tracking("1000", "th", last_msg_id="60")
    assert str(db.get_thread_tracking("1000", "th")["last_msg_id"]) == "70"
    db.record_skipped_message(5, "9", "Al", "x", 5)
    db.clear_skipped_message(5)
    assert db.get_skipped_messages() == []


def test_waterfall_cursor_only_moves_forward(tmp_path):
    from src.core.state import MigrationState
    st = MigrationState()
    st.db = MigrationDatabase(tmp_path / "m.db", "fluxer")
    st.set_waterfall_cursor(500)
    st.set_waterfall_cursor(300)
    assert st.get_waterfall_cursor() == 500
    st.set_waterfall_cursor(900)
    assert st.get_waterfall_cursor() == 900
