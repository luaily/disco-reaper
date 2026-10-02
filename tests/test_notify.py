import asyncio
import logging
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
from src.core.notify import FluxerNotifier, describe_result, redact
from src.fluxer.writer import MessageSendError, ServiceUnavailable

_real_sleep = asyncio.sleep


class _Client:
    def __init__(self, fail_dm=False):
        self.dms, self.messages, self.fail_dm = [], [], fail_dm

    async def create_dm(self, user_id):
        if self.fail_dm:
            raise RuntimeError("Cannot send messages to this user")
        self.dms.append(user_id)
        return {"id": "dm-1"}

    async def send_message(self, channel_id, content=None, **kw):
        self.messages.append((channel_id, content))
        return {"id": "m"}


def _notifier(client=None, **kw):
    client = client or _Client()
    return FluxerNotifier(types.SimpleNamespace(client=client), "42", min_gap=0, **kw), client


# ── the notifier ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_dm_goes_to_the_configured_user_through_the_bots_own_client():
    n, c = _notifier()
    assert n.notify("hello", kind="ok") is True
    await n.flush()
    assert c.dms == ["42"] and c.messages == [("dm-1", "✅ hello")]


@pytest.mark.asyncio
async def test_webhook_tokens_are_redacted_from_text_and_log_excerpts():
    n, c = _notifier()
    logging.getLogger("fluxer.http").warning("Server error 503 on https://api.fluxer.app/v1/webhooks/123/SeCrEtToKen, retrying")
    n.notify("failed at https://api.fluxer.app/v1/webhooks/123/SeCrEtToKen", kind="error", with_logs=True)
    await n.flush()
    body = c.messages[0][1]
    assert "SeCrEtToKen" not in body and "/webhooks/123/***" in body and "Server error 503" in body and "```" in body
    assert redact("x/webhooks/9/abc?wait=true") == "x/webhooks/9/***?wait=true"


@pytest.mark.asyncio
async def test_repeats_are_throttled_and_the_suppressed_count_is_reported():
    n, c = _notifier()
    assert n.notify("skip 1", key="skip", cooldown=60) is True
    assert n.notify("skip 2", key="skip", cooldown=60) is False
    assert n.notify("skip 3", key="skip", cooldown=60) is False
    assert n.notify("other thing", key="other", cooldown=60) is True              # different kind is independent
    n._last_by_key["skip"] -= 120                                                # cooldown over
    assert n.notify("skip 4", key="skip", cooldown=60) is True
    await n.flush()
    texts = [m for _, m in c.messages]
    assert len(texts) == 3 and "(+2 similar notifications suppressed)" in texts[-1] and "skip 4" in texts[-1]


@pytest.mark.asyncio
async def test_a_failing_dm_never_raises_and_does_not_hammer():
    n, c = _notifier(_Client(fail_dm=True))
    for i in range(5):
        n.notify(f"m{i}")
    await n.flush()                                                               # no exception
    assert c.messages == [] and n._disabled_until > time.monotonic()               # backed off instead of retrying forever


@pytest.mark.asyncio
async def test_long_text_is_cut_to_fit():
    n, c = _notifier()
    n.notify("x" * 5000)
    await n.flush()
    assert len(c.messages[0][1]) <= 1900


def test_no_event_loop_means_not_sent_but_no_crash():
    n = FluxerNotifier(types.SimpleNamespace(client=_Client()), "1")
    assert n.notify("hi") is False


def test_result_summaries():
    assert describe_result("W", {"messages": 5})[0] == "ok"
    kind, text = describe_result("W", {"messages": 5, "skipped": 2, "skipped_ids": ["1", "2"]})
    assert kind == "warn" and "2 skipped (1, 2)" in text
    kind, text = describe_result("W", {"messages": 5, "error": "Halted at message 7: boom"})
    assert kind == "error" and "boom" in text
    kind, text = describe_result("W", {"messages": 5, "stopped": "deadline"}, elapsed=3700)
    assert kind == "info" and "scheduled stop" in text and "1h01m" in text


# ── what the migration sends ───────────────────────────────────────────────

class _State:
    attempts = {}

    def __init__(self):
        self.cursor = None

    def record_message_attempt(self, *a):
        return 1

    def clear_message_attempts(self, *a):
        pass

    def record_skipped_message(self, *a):
        pass

    def get_user_alias(self, uid):
        return "A"

    def set_message_mapping(self, *a):
        pass

    def update_last_message_timestamp(self, *a):
        pass

    def update_last_message_id(self, *a):
        pass

    def get_all_last_message_ids(self):
        return {}

    def get_target_channel_id(self, _):
        return "T"

    def set_waterfall_cursor(self, mid):
        self.cursor = mid


class _Writer:
    def __init__(self, health=()):
        self.health = list(health)

    async def check_health(self, ch):
        return self.health.pop(0) if self.health else (True, "ok")

    async def verify_message(self, ch, mid):
        return True

    async def send_marker(self, **kw):
        return "marker"


def _msg(i):
    return types.SimpleNamespace(id=i, author=types.SimpleNamespace(id=1, display_name="Al"),
                                 created_at=datetime(2024, 1, 1, tzinfo=timezone.utc), channel=types.SimpleNamespace(id=9),
                                 type=0, thread=None, jump_url=f"u{i}")


def _ctx(**cfg):
    ctx = types.SimpleNamespace(is_running=True, deadline=None, state=_State(), fluxer_writer=_Writer(), dms=[],
                                config=types.SimpleNamespace(max_message_attempts=cfg.get("attempts", 5), anonymize_users=False,
                                                             max_outage_minutes=0), on_notice=None)
    ctx.deadline_reached = lambda: False
    ctx.notify = lambda text, **kw: (ctx.dms.append((kw.get("kind"), text)) or True)
    return ctx


@pytest.fixture(autouse=True)
def _quick(monkeypatch):
    async def fast(_):
        await _real_sleep(0)
    monkeypatch.setattr(mm, "OUTAGE_BACKOFF", (0,))
    monkeypatch.setattr(mm.asyncio, "sleep", fast)


@pytest.mark.asyncio
async def test_a_long_outage_sends_one_down_and_one_recovered_message_but_a_blip_sends_none(monkeypatch):
    clock = {"t": 1000.0}
    monkeypatch.setattr(mm.time, "time", lambda: clock.__setitem__("t", clock["t"] + 60.0) or clock["t"])
    calls = {"n": 0}

    async def fake(context, msg, **kw):
        calls["n"] += 1
        if calls["n"] <= 4:
            raise ServiceUnavailable("503")
        kw["stats"]["messages"] = 1
        return "ok"
    monkeypatch.setattr(mm, "_process_and_send_message", fake)
    ctx = _ctx()
    ctx.fluxer_writer = _Writer(health=[(False, "server error 503")] * 3)
    assert await mm._process_with_retries(ctx, _msg(7), "T", {}) == "ok"
    kinds = [k for k, _ in ctx.dms]
    assert "warn" in kinds and "ok" in kinds
    down = next(t for k, t in ctx.dms if k == "warn")
    assert "`7`" in down and "Nothing is skipped" in down
    # a blip: the outage is over before it passes two minutes -> nothing sent
    clock["t"], calls["n"] = 5000.0, 3
    monkeypatch.setattr(mm.time, "time", lambda: clock.__setitem__("t", clock["t"] + 1.0) or clock["t"])
    ctx2 = _ctx()
    assert await mm._process_with_retries(ctx2, _msg(8), "T", {}) == "ok" and ctx2.dms == []


@pytest.mark.asyncio
async def test_skipped_message_and_halted_run_are_reported(monkeypatch):
    async def bad(context, msg, **kw):
        raise MessageSendError("Send failed: odd error")
    monkeypatch.setattr(mm, "_process_and_send_message", bad)
    ctx = _ctx(attempts=1)
    stats = {}
    await mm._process_with_retries(ctx, _msg(5), "T", stats)
    assert any(k == "warn" and "Skipped message `5`" in t for k, t in ctx.dms)

    class Reader:
        MESSAGE_TYPE_DEFAULT, MESSAGE_TYPE_REPLY, MESSAGE_TYPE_THREAD_STARTER, MESSAGE_TYPE_FORWARD = 0, 19, 21, 99
        MESSAGE_TYPE_CHAT_INPUT_COMMAND, MESSAGE_TYPE_CONTEXT_MENU_COMMAND = 20, 23
        MESSAGE_TYPE_POLL_RESULT, MESSAGE_TYPE_AUTO_MODERATION_ACTION = 46, 24

        async def fetch_global_message_history(self, after_id=None):
            yield _msg(9)
    ctx2 = _ctx(attempts=0)                                  # 0 = never skip: the run halts
    ctx2.discord_reader = Reader()
    res = await mm.migrate_global_messages(ctx2)
    assert "error" in res and any(k == "error" and "halted at message `9`" in t for k, t in ctx2.dms)
