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
from src.fluxer.writer import FluxerWriter, ServiceUnavailable

_real_sleep = asyncio.sleep


@pytest.fixture(autouse=True)
def _quick(monkeypatch):
    async def fast(_):
        await _real_sleep(0)
    monkeypatch.setattr(mm, "OUTAGE_BACKOFF", (0,))
    monkeypatch.setattr(mm.asyncio, "sleep", fast)


# ── helpers ────────────────────────────────────────────────────────────────

def test_defang_links_and_link_info():
    assert mm.defang_links("see https://dead.example/x?y=1 and https://tenor.com/v) ok") == \
        "see `https://dead.example/x?y=1` and `https://tenor.com/v` ) ok".replace("` )", "`)")
    assert mm.defang_links("no links here") == "no links here" and mm.defang_links("") == ""
    msg = types.SimpleNamespace(content="look https://dead-site.example/page",
                                embeds=[{"url": "https://www.youtube.com/watch?v=1", "thumbnail": {"url": "https://i.ytimg.com/x.jpg"}}],
                                attachments=[1, 2])
    info = mm.message_link_info(msg)
    assert "dead-site.example" in info and "www.youtube.com" in info and "i.ytimg.com" in info
    assert "embeds: 1" in info and "attachments: 2" in info


# ── the ladder ─────────────────────────────────────────────────────────────

class _State:
    def __init__(self):
        self.skipped, self.mapped, self.progress = [], {}, []

    def record_skipped_message(self, mid, *a):
        self.skipped.append(str(mid))

    def get_user_alias(self, uid):
        return "A"

    def set_message_mapping(self, *a):
        pass

    def update_last_message_timestamp(self, *a):
        pass

    def update_last_message_id(self, ch, mid):
        self.progress.append(str(mid))

    def record_message_attempt(self, *a):
        return 1

    def clear_message_attempts(self, *a):
        pass


class _Writer:
    def __init__(self, flapping=False, canary=True, with_canary=True):
        self.flapping, self.canary_result, self.canary_calls, self.markers = flapping, canary, 0, []
        self._probes = 0
        if not with_canary:
            self.canary = None

    async def check_health(self, ch):
        self._probes += 1
        if self.flapping and self._probes % 2 == 1:      # every outage: first probe unhealthy, the next one fine
            return False, "server error 503"
        return True, "ok"

    async def verify_message(self, ch, mid):
        return True

    async def send_marker(self, **kw):
        self.markers.append(kw["content"])
        return "marker"

    async def canary(self, ch):                     # replaced by None when with_canary=False
        self.canary_calls += 1
        return self.canary_result


def _msg(content="check https://dead-site.example/page", embeds=(1,)):
    return types.SimpleNamespace(id=9, content=content, embeds=list(embeds), attachments=[],
                                 author=types.SimpleNamespace(id=1, display_name="Al"),
                                 created_at=datetime(2024, 1, 1, tzinfo=timezone.utc), channel=types.SimpleNamespace(id=3))


def _ctx(writer):
    ctx = types.SimpleNamespace(is_running=True, deadline=None, state=_State(), fluxer_writer=writer, notices=[], dms=[],
                                config=types.SimpleNamespace(max_message_attempts=5, anonymize_users=False, max_outage_minutes=0))
    ctx.deadline_reached = lambda: False
    ctx.on_notice = ctx.notices.append
    ctx.notify = lambda text, **kw: (ctx.dms.append(text) or True)
    return ctx


def _fake(fail_while):
    """Fails with ServiceUnavailable while fail_while(degrade) is true; records the degrade level of every attempt."""
    seen = []

    async def fake(context, msg, **kw):
        seen.append(kw.get("degrade", 0))
        if fail_while(kw.get("degrade", 0), len(seen)):
            raise ServiceUnavailable("Send timed out after 45s (delivery unknown)")
        kw["stats"]["messages"] = kw["stats"].get("messages", 0) + 1
        return "ok"
    return fake, seen


@pytest.mark.asyncio
async def test_a_message_that_only_fails_with_previews_is_delivered_without_them(monkeypatch):
    fake, seen = _fake(lambda d, n: d == 0)
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    ctx = _ctx(_Writer())
    stats = {}
    assert await mm._process_with_retries(ctx, _msg(), "T", stats) == "ok"
    assert seen == [0, 0, 1] and stats["degraded"] == 1 and stats["degraded_ids"] == ["9"] and ctx.state.skipped == []
    assert any("without embeds" in n for n in ctx.notices) and any("keeps failing while Fluxer looks healthy" in d for d in ctx.dms)


@pytest.mark.asyncio
async def test_second_step_shows_links_as_plain_code(monkeypatch):
    fake, seen = _fake(lambda d, n: d < 2)
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    ctx = _ctx(_Writer())
    assert await mm._process_with_retries(ctx, _msg(), "T", {}) == "ok"
    assert seen == [0, 0, 1, 1, 2] and ctx.state.skipped == []


@pytest.mark.asyncio
async def test_still_failing_but_canary_passes_means_the_message_is_the_problem(monkeypatch):
    fake, seen = _fake(lambda d, n: True)
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    w = _Writer(canary=True)
    ctx = _ctx(w)
    stats = {}
    assert await mm._process_with_retries(ctx, _msg(), "T", stats) == "marker"          # skipped, run can continue
    assert w.canary_calls == 1 and ctx.state.skipped == ["9"] and stats["skipped"] == 1
    assert seen == [0, 0, 1, 1, 2, 2]
    assert "skipping" in w.markers[0]


@pytest.mark.asyncio
async def test_canary_failing_means_a_real_outage_so_the_run_keeps_holding(monkeypatch):
    fake, seen = _fake(lambda d, n: n < 12)                       # fails for a long time, then Fluxer recovers
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    w = _Writer(canary=False)
    ctx = _ctx(w)
    assert await mm._process_with_retries(ctx, _msg(), "T", {}) == "ok"
    assert ctx.state.skipped == [] and w.canary_calls >= 1       # never skipped while the canary also fails


@pytest.mark.asyncio
async def test_a_message_without_links_goes_straight_to_the_canary(monkeypatch):
    fake, seen = _fake(lambda d, n: True)
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    w = _Writer(canary=True)
    ctx = _ctx(w)
    await mm._process_with_retries(ctx, _msg(content="plain words", embeds=()), "T", {})
    assert seen == [0, 0] and max(seen) == 0 and ctx.state.skipped == ["9"]      # no pointless degraded attempts


@pytest.mark.asyncio
async def test_a_real_outage_never_triggers_the_ladder(monkeypatch):
    fake, seen = _fake(lambda d, n: n <= 6)
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    w = _Writer(flapping=True)                                      # every failure is followed by an unhealthy probe
    ctx = _ctx(w)
    assert await mm._process_with_retries(ctx, _msg(), "T", {}) == "ok"
    assert max(seen) == 0 and ctx.state.skipped == [] and w.canary_calls == 0


@pytest.mark.asyncio
async def test_without_a_canary_nothing_is_ever_skipped(monkeypatch):
    fake, seen = _fake(lambda d, n: n < 15)
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    ctx = _ctx(_Writer(with_canary=False))
    assert await mm._process_with_retries(ctx, _msg(), "T", {}) == "ok" and ctx.state.skipped == []


# ── what a degraded send actually sends ────────────────────────────────────

def _send_ctx():
    sent = []

    async def send_message(**kw):
        sent.append(kw)
        return "new"
    state = _State()
    state.emoji_map, state.channel_map = {}, {}
    state.get_fluxer_message_id = lambda *a: None
    state.increment_stats = lambda *a, **k: None
    ctx = types.SimpleNamespace(is_running=True, state=state, discord_reader=types.SimpleNamespace(guild=None, db=None, backup_path=None),
                                fluxer_writer=types.SimpleNamespace(send_message=send_message, community_id="1"),
                                config=types.SimpleNamespace(anonymize_users=False))
    return ctx, sent


def _real_msg():
    m = _msg(content="look https://dead-site.example/page", embeds=[{"type": "rich", "title": "t"}])
    m.mentions = m.role_mentions = m.channel_mentions = m.stickers = []
    m.flags, m.reference = types.SimpleNamespace(forwarded=False), None
    return m


@pytest.mark.asyncio
@pytest.mark.parametrize("degrade, embeds_sent, suppress, defanged", [(0, True, False, False), (1, False, True, False), (2, False, True, True)])
async def test_each_degrade_level_changes_the_payload(degrade, embeds_sent, suppress, defanged):
    ctx, sent = _send_ctx()
    ctx.state.update_last_message_timestamp = lambda *a: None
    ctx.state.set_message_mapping = lambda *a: None
    ctx.state.update_last_message_id = lambda *a: None
    await mm._process_and_send_message(ctx, _real_msg(), "T", {"messages": 0, "attachments": 0}, degrade=degrade)
    kw = sent[0]
    assert (kw["embeds"] is not None) == embeds_sent and bool(kw.get("suppress_embeds")) == suppress
    assert ("`https://dead-site.example/page`" in kw["content"]) == defanged


# ── writer: suppression and canary ─────────────────────────────────────────

class _Http:
    def __init__(self, fail=False):
        self.posts, self.deleted, self.fail = [], [], fail

    def _route(self, method, path, **kw):
        return (method, path.format(**kw))

    async def request(self, route, json=None, data=None, params=None, **kw):
        if self.fail:
            raise RuntimeError("Failed after 5 attempts")
        self.posts.append(json)
        return {"id": "555"}

    async def delete_message(self, ch, mid):
        self.deleted.append(mid)


def _writer(http):
    w = FluxerWriter(token="t", community_id="1")
    w.bot = types.SimpleNamespace(_http=http)
    w._ready_event.set()
    w._webhooks["chan"] = types.SimpleNamespace(id=5, token="tok", send=None)
    return w


@pytest.mark.asyncio
async def test_suppress_embeds_sends_flags_4_and_no_embeds():
    http = _Http()
    w = _writer(http)
    mid = await w.send_message(channel_id="chan", author_name="Al", content="see https://dead.example", timestamp=1,
                               embeds=[{"type": "rich", "title": "t"}], suppress_embeds=True)
    assert mid == "555" and http.posts[0]["flags"] == 4 and "embeds" not in http.posts[0]


@pytest.mark.asyncio
async def test_canary_posts_a_tiny_message_and_removes_it():
    http = _Http()
    w = _writer(http)
    assert await w.canary("chan") is True
    assert http.posts[0]["content"] == "·" and http.deleted == ["555"]
    assert await _writer(_Http(fail=True)).canary("chan") is False                 # failing route -> False, never raises
