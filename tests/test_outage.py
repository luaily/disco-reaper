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
from src.fluxer.writer import FluxerWriter, MessageSendError, SendTimeout, ServiceUnavailable

_real_sleep = asyncio.sleep


async def _fast(_):
    await _real_sleep(0)


# ── error classes ──────────────────────────────────────────────────────────

def test_timeouts_and_outages_are_service_errors_but_plain_failures_are_not():
    assert issubclass(SendTimeout, ServiceUnavailable) and issubclass(ServiceUnavailable, MessageSendError)
    assert not issubclass(MessageSendError, ServiceUnavailable)


@pytest.mark.asyncio
async def test_writer_classifies_failures(monkeypatch):
    import aiohttp
    monkeypatch.setattr(wm, "_MAX_RECOVERY_ROUNDS", 1)
    monkeypatch.setattr(wm.asyncio, "sleep", _fast)
    w = FluxerWriter(token="t", community_id="1")

    def failing(exc):
        async def attempt():
            raise exc
        return attempt

    class Http503(Exception):
        status = 503

    for exc in (RuntimeError("Failed after 5 attempts: POST x"), aiohttp.ClientConnectionError("reset"),
                asyncio.TimeoutError(), Http503("boom")):
        with pytest.raises(ServiceUnavailable):
            await w._send_with_recovery(failing(exc), "chan")
    with pytest.raises(MessageSendError) as ei:                       # a bug / odd error is about the message, not the service
        await w._send_with_recovery(failing(ValueError("odd")), "chan")
    assert not isinstance(ei.value, ServiceUnavailable)


# ── health probe + read-back ───────────────────────────────────────────────

class _Route:
    def __init__(self, method, url):
        self.method, self.url = method, url


class _Session:
    def __init__(self, statuses, raises=None):
        self.statuses, self.raises, self.calls = list(statuses), raises, []

    def request(self, method, url, timeout=None):
        self.calls.append(url)
        if self.raises:
            raise self.raises
        status = self.statuses.pop(0) if self.statuses else 200

        class Ctx:
            async def __aenter__(s):
                return types.SimpleNamespace(status=status)

            async def __aexit__(s, *a):
                return False
        return Ctx()


class _Client:
    def __init__(self, session):
        self.session = session

    def _route(self, method, path, **kw):
        return _Route(method, "https://api/" + path.format(**kw))

    async def _ensure_session(self):
        return self.session


def _health_writer(session, webhook=False):
    w = FluxerWriter(token="t", community_id="1")
    w.bot = types.SimpleNamespace(_http=_Client(session))
    if webhook:
        w._webhooks["chan"] = types.SimpleNamespace(id=5, token="tok")
    return w


@pytest.mark.asyncio
async def test_check_health_results():
    assert (await _health_writer(_Session([200])).check_health("chan"))[0] is True
    ok, why = await _health_writer(_Session([503])).check_health("chan")
    assert ok is False and "503" in why
    assert (await _health_writer(_Session([429])).check_health("chan"))[0] is False
    assert (await _health_writer(_Session([404])).check_health("chan"))[0] is True            # up; a different problem
    ok, why = await _health_writer(_Session([], raises=asyncio.TimeoutError())).check_health("chan")
    assert ok is False and "TimeoutError" in why
    s = _Session([200, 503])                                         # the channel answers but the webhook route is down
    assert (await _health_writer(s, webhook=True).check_health("chan"))[0] is False
    assert any("/webhooks/5/tok" in u for u in s.calls)


@pytest.mark.asyncio
async def test_verify_message():
    class C:
        def __init__(self, exc=None):
            self.exc = exc

        async def get_message(self, ch, mid):
            if self.exc:
                raise self.exc
            return {"id": mid}

    class NotFound(Exception):
        status = 404
    for exc, expected in ((None, True), (NotFound(), False), (RuntimeError("x"), None)):
        w = FluxerWriter(token="t", community_id="1")
        w.bot = types.SimpleNamespace(_http=C(exc))
        assert await w.verify_message("chan", "1") is expected


# ── the run holds during an outage instead of skipping ─────────────────────

class _State:
    def __init__(self):
        self.attempts, self.skipped, self.cursor, self.progress, self.mapped = {}, [], None, [], {}

    def record_message_attempt(self, mid, err=""):
        self.attempts[str(mid)] = self.attempts.get(str(mid), 0) + 1
        return self.attempts[str(mid)]

    def clear_message_attempts(self, mid):
        self.attempts.pop(str(mid), None)

    def record_skipped_message(self, mid, ch, author, reason, attempts):
        self.skipped.append(str(mid))

    def get_user_alias(self, uid):
        return "A"

    def set_message_mapping(self, *a):
        pass

    def update_last_message_timestamp(self, *a):
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
    def __init__(self, health=(), verify=(True,)):
        self.health, self.verify, self.probes, self.verifies, self.markers = list(health), list(verify), 0, 0, []

    async def check_health(self, channel):
        self.probes += 1
        return self.health.pop(0) if self.health else (True, "ok")

    async def verify_message(self, channel, mid):
        self.verifies += 1
        return self.verify.pop(0) if self.verify else True

    async def send_marker(self, channel_id, content, **kw):
        self.markers.append(content)
        return "marker"


def _msg(i):
    return types.SimpleNamespace(id=i, author=types.SimpleNamespace(id=1, display_name="Al"),
                                 created_at=datetime(2024, 1, 1, tzinfo=timezone.utc), channel=types.SimpleNamespace(id=9),
                                 type=0, thread=None, jump_url=f"u{i}")


def _ctx(writer, max_attempts=5, max_outage=0):
    ctx = types.SimpleNamespace(is_running=True, deadline=None, state=_State(), fluxer_writer=writer, notices=[],
                                config=types.SimpleNamespace(max_message_attempts=max_attempts, anonymize_users=False,
                                                             max_outage_minutes=max_outage))
    ctx.deadline_reached = lambda: ctx.deadline is not None and time.time() >= ctx.deadline
    ctx.on_notice = ctx.notices.append
    return ctx


def _fake_process(fail_times, exc=lambda: ServiceUnavailable("503 Service Unavailable"), ids=("ok",)):
    calls = {"n": 0}
    results = list(ids)

    async def fake(context, msg, **kw):
        calls["n"] += 1
        if calls["n"] <= fail_times:
            raise exc()
        kw["stats"]["messages"] = kw["stats"].get("messages", 0) + 1
        return results.pop(0) if len(results) > 1 else results[0]
    return fake, calls


@pytest.fixture(autouse=True)
def _quick(monkeypatch):
    monkeypatch.setattr(mm, "OUTAGE_BACKOFF", (0,))
    monkeypatch.setattr(mm.asyncio, "sleep", _fast)


@pytest.mark.asyncio
async def test_outage_holds_the_run_and_never_counts_or_skips(monkeypatch):
    fake, calls = _fake_process(fail_times=9)                         # way more than max_message_attempts (5)
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    w = _Writer(health=[(False, "server error 503"), (False, "server error 503")])
    ctx = _ctx(w)
    stats = {}
    assert await mm._process_with_retries(ctx, _msg(7), "T", stats) == "ok"
    assert calls["n"] == 10 and ctx.state.attempts == {} and ctx.state.skipped == [] and "skipped" not in stats
    assert w.probes >= 9                                              # probed between every retry, never hammered
    assert any("nothing is skipped" in n for n in ctx.notices) and any("answering again" in n for n in ctx.notices)


@pytest.mark.asyncio
async def test_first_message_after_an_outage_is_read_back_and_resent_if_missing(monkeypatch):
    fake, calls = _fake_process(fail_times=1, ids=("m1", "m2"))
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    w = _Writer(verify=[False, True])                                 # Fluxer returned m1 but it isn't in the channel
    ctx = _ctx(w)
    stats = {}
    assert await mm._process_with_retries(ctx, _msg(1), "T", stats) == "m2"
    assert w.verifies == 2 and calls["n"] == 3 and stats["messages"] == 1


@pytest.mark.asyncio
async def test_no_read_back_when_there_was_no_outage(monkeypatch):
    fake, _ = _fake_process(fail_times=0)
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    w = _Writer()
    assert await mm._process_with_retries(_ctx(w), _msg(1), "T", {}) == "ok" and w.verifies == 0


@pytest.mark.asyncio
async def test_max_outage_minutes_stops_the_run_without_skipping(monkeypatch):
    fake, _ = _fake_process(fail_times=99)
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    clock = {"t": 1000.0}

    def now():
        clock["t"] += 100.0
        return clock["t"]
    monkeypatch.setattr(mm.time, "time", now)
    ctx = _ctx(_Writer(health=[(False, "503")] * 50), max_outage=1)
    with pytest.raises(MessageSendError) as ei:
        await mm._process_with_retries(ctx, _msg(3), "T", {})
    assert "unavailable for over 1 minutes" in str(ei.value) and "nothing is skipped" in str(ei.value)
    assert ctx.state.skipped == [] and ctx.state.attempts == {}


@pytest.mark.asyncio
async def test_cancel_while_waiting_is_clean_and_uncounted(monkeypatch):
    fake, _ = _fake_process(fail_times=99)
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    monkeypatch.setattr(mm, "OUTAGE_BACKOFF", (3,))
    ctx = _ctx(_Writer(health=[(False, "503")] * 5))

    async def cancelling_sleep(_):
        ctx.is_running = False                                         # the user presses Cancel during the pause
    monkeypatch.setattr(mm.asyncio, "sleep", cancelling_sleep)
    with pytest.raises(MessageSendError) as ei:
        await mm._process_with_retries(ctx, _msg(3), "T", {})
    assert str(ei.value).startswith("Cancelled") and ctx.state.attempts == {} and ctx.state.skipped == []


@pytest.mark.asyncio
async def test_message_specific_failures_still_count_and_skip(monkeypatch):
    fake, calls = _fake_process(fail_times=99, exc=lambda: MessageSendError("Send failed: odd error"))
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    ctx = _ctx(_Writer(), max_attempts=3)
    stats = {}
    assert await mm._process_with_retries(ctx, _msg(4), "T", stats) == "marker"
    assert calls["n"] == 3 and ctx.state.skipped == ["4"] and stats["skipped"] == 1


# ── whole-loop behaviour ───────────────────────────────────────────────────

class _Reader:
    MESSAGE_TYPE_DEFAULT, MESSAGE_TYPE_REPLY, MESSAGE_TYPE_THREAD_STARTER, MESSAGE_TYPE_FORWARD = 0, 19, 21, 99
    MESSAGE_TYPE_CHAT_INPUT_COMMAND, MESSAGE_TYPE_CONTEXT_MENU_COMMAND = 20, 23
    MESSAGE_TYPE_POLL_RESULT, MESSAGE_TYPE_AUTO_MODERATION_ACTION = 46, 24

    async def fetch_global_message_history(self, after_id=None):
        for i in (1, 2, 3):
            yield _msg(i)


@pytest.mark.asyncio
async def test_waterfall_rides_out_an_outage_with_nothing_skipped(monkeypatch):
    sent, fails = [], {"n": 0}

    async def fake(context, msg, **kw):
        if msg.id == 2 and fails["n"] < 8:
            fails["n"] += 1
            raise ServiceUnavailable("Send timed out after 45s (delivery unknown)")
        sent.append(msg.id)
        kw["stats"]["messages"] += 1
        return f"id{msg.id}"
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    ctx = _ctx(_Writer(health=[(False, "timeout")] * 3))
    ctx.discord_reader = _Reader()
    res = await mm.migrate_global_messages(ctx)
    assert sent == [1, 2, 3] and res["messages"] == 3 and "skipped" not in res and "error" not in res
    assert ctx.state.cursor == 3 and ctx.state.skipped == []


@pytest.mark.asyncio
async def test_user_cancel_is_not_reported_as_an_error(monkeypatch):
    async def fake(context, msg, **kw):
        context.is_running = False
        raise MessageSendError("Cancelled while sending")
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    ctx = _ctx(_Writer())
    ctx.discord_reader = _Reader()
    res = await mm.migrate_global_messages(ctx)
    assert "error" not in res and ctx.state.cursor is None
