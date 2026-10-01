import asyncio
import importlib.util
import sys
import time
import types
from datetime import datetime
from pathlib import Path

import pytest

try:
    import fluxer  # noqa: F401
except ImportError:
    stub = types.ModuleType("fluxer")
    for name in ("Bot", "Webhook", "Forbidden", "File"):
        setattr(stub, name, type(name, (), {}))
    sys.modules["fluxer"] = stub

import src.fluxer.migrate_message as mm
from src.fluxer.writer import FluxerWriter, MessageSendError

_spec = importlib.util.spec_from_file_location(
    "timed_waterfall", Path(__file__).parent.parent / "scripts" / "timed_waterfall.py")
tw = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tw)


# ── CLI parsing ────────────────────────────────────────────────────────────

def test_parse_until_today_and_tomorrow():
    now = datetime(2026, 1, 1, 22, 0, 0)
    assert datetime.fromtimestamp(tw.parse_until("23:30", now)) == datetime(2026, 1, 1, 23, 30)
    assert datetime.fromtimestamp(tw.parse_until("07:00", now)) == datetime(2026, 1, 2, 7, 0)
    assert datetime.fromtimestamp(tw.parse_until("22:00", now)) == datetime(2026, 1, 2, 22, 0)  # now == past
    with pytest.raises(ValueError):
        tw.parse_until("25:00", now)


def test_parse_duration():
    assert tw.parse_duration("9h") == 9 * 3600
    assert tw.parse_duration("90m") == 5400
    assert tw.parse_duration("9h30m") == 9 * 3600 + 1800
    assert tw.parse_duration("45s") == 45
    for bad in ("", "9", "h", "abc"):
        with pytest.raises(ValueError):
            tw.parse_duration(bad)


# ── loop behaviour ─────────────────────────────────────────────────────────

class _Reader:
    MESSAGE_TYPE_DEFAULT = 0
    MESSAGE_TYPE_REPLY = 19
    MESSAGE_TYPE_THREAD_STARTER = 21
    MESSAGE_TYPE_FORWARD = 99
    MESSAGE_TYPE_CHAT_INPUT_COMMAND = 20
    MESSAGE_TYPE_CONTEXT_MENU_COMMAND = 23
    MESSAGE_TYPE_POLL_RESULT = 46
    MESSAGE_TYPE_AUTO_MODERATION_ACTION = 24

    def __init__(self, ids):
        self.ids = ids

    async def fetch_global_message_history(self, after_id=None):
        for i in self.ids:
            yield types.SimpleNamespace(id=i, type=0, channel=types.SimpleNamespace(id=1), thread=None,
                                        jump_url=f"u{i}")


class _State:
    def __init__(self):
        self.cursor = None

    def get_all_last_message_ids(self):
        return {}

    def get_target_channel_id(self, _):
        return "T"

    def set_waterfall_cursor(self, i):
        self.cursor = i


def _ctx(ids, deadline=None):
    ctx = types.SimpleNamespace(is_running=True, deadline=deadline, discord_reader=_Reader(ids), state=_State())
    ctx.deadline_reached = lambda: ctx.deadline is not None and time.time() >= ctx.deadline
    return ctx


@pytest.mark.asyncio
async def test_deadline_stops_cleanly_between_messages(monkeypatch):
    ctx = _ctx([1, 2, 3, 4, 5])
    sent = []

    async def fake_send(context, msg, **kw):
        sent.append(msg.id)
        if msg.id == 2:
            context.deadline = time.time() - 1   # deadline passes while message 2 is being handled
        kw["stats"]["messages"] += 1
        return "x"

    monkeypatch.setattr(mm, "_process_and_send_message", fake_send)
    res = await mm.migrate_global_messages(ctx)
    assert sent == [1, 2]                       # message 2 completed, 3 never started
    assert res["stopped"] == "deadline" and "error" not in res
    assert ctx.state.cursor == 2                # cursor covers exactly what was handled
    assert ctx.is_running is False


@pytest.mark.asyncio
async def test_send_error_at_deadline_is_a_clean_stop_and_unmarked(monkeypatch):
    ctx = _ctx([1, 2, 3])

    async def fake_send(context, msg, **kw):
        if msg.id == 2:
            context.deadline = time.time() - 1
            raise MessageSendError("Cancelled while waiting on rate limit")
        kw["stats"]["messages"] += 1
        return "x"

    monkeypatch.setattr(mm, "_process_and_send_message", fake_send)
    res = await mm.migrate_global_messages(ctx)
    assert res["stopped"] == "deadline" and "error" not in res
    assert ctx.state.cursor == 1                # message 2 NOT marked; a resume retries it


@pytest.mark.asyncio
async def test_send_error_before_deadline_is_still_an_error(monkeypatch):
    ctx = _ctx([1, 2], deadline=time.time() + 3600)
    ctx.config = types.SimpleNamespace(max_message_attempts=0)       # 0 = never skip: halt at the failing message

    async def fake_send(context, msg, **kw):
        raise MessageSendError("gave up")

    monkeypatch.setattr(mm, "_process_and_send_message", fake_send)
    res = await mm.migrate_global_messages(ctx)
    assert "error" in res and "stopped" not in res
    assert ctx.state.cursor is None


# ── pacing ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_pacing_enforces_min_interval():
    w = FluxerWriter(token="t", community_id="1")
    w.min_send_interval = 0.3
    t0 = time.monotonic()
    await w._pace()
    await w._pace()
    await w._pace()
    assert time.monotonic() - t0 >= 0.6 - 0.05


@pytest.mark.asyncio
async def test_pacing_aborts_on_cancel():
    w = FluxerWriter(token="t", community_id="1")
    w.min_send_interval = 30
    await w._pace()                # first send is immediate
    w.stop_check = lambda: True
    with pytest.raises(MessageSendError):
        await w._pace()


# ── shared stop-spec parsing (TUI dialog + CLI) ────────────────────────────

def test_parse_stop_spec():
    from src.core.utils import parse_stop_spec
    assert parse_stop_spec("") is None and parse_stop_spec("   ") is None
    assert parse_stop_spec("9h", now_ts=1000) == 1000 + 9 * 3600
    assert parse_stop_spec("45s", now_ts=1000) == 1045
    assert parse_stop_spec("07:00") > time.time()           # next occurrence is always in the future
    with pytest.raises(ValueError):
        parse_stop_spec("banana")
    with pytest.raises(ValueError):
        parse_stop_spec("25:99")


# ── TUI run-options dialog ─────────────────────────────────────────────────

async def _open_modal():
    from textual.app import App
    from src.ui.modals import RunOptionsModal
    results = []

    class Host(App):
        def on_mount(self):
            self.push_screen(RunOptionsModal(), results.append)

    return Host(), results


@pytest.mark.asyncio
async def test_run_options_modal_blank_means_no_limits():
    app, results = await _open_modal()
    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.click("#btn_run_opts_start")
        await pilot.pause()
    assert results == [{"deadline": None, "max_rate": 0.0}]


@pytest.mark.asyncio
async def test_run_options_modal_values_and_validation():
    app, results = await _open_modal()
    async with app.run_test() as pilot:
        await pilot.pause()
        app.screen.query_one("#input_stop_spec").value = "9h"
        app.screen.query_one("#input_max_rate").value = "abc"      # invalid -> stays open
        await pilot.click("#btn_run_opts_start")
        await pilot.pause()
        assert results == []
        app.screen.query_one("#input_max_rate").value = "40"
        app.screen.query_one("#input_max_rate").focus()
        await pilot.press("enter")            # Enter in a field submits, same as the Start button
        await pilot.pause()
    assert len(results) == 1
    assert results[0]["max_rate"] == 40.0
    assert abs(results[0]["deadline"] - (time.time() + 9 * 3600)) < 5


@pytest.mark.asyncio
async def test_run_options_modal_back_returns_none():
    app, results = await _open_modal()
    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.click("#btn_run_opts_back")
        await pilot.pause()
    assert results == [None]


# ── scripts/monitor_run.py ─────────────────────────────────────────────────

_mspec = importlib.util.spec_from_file_location("monitor_run", Path(__file__).parent.parent / "scripts" / "monitor_run.py")
mon = importlib.util.module_from_spec(_mspec)
_mspec.loader.exec_module(mon)


def test_monitor_parses_fluxer_log_lines():
    p = mon.parse_log_line
    assert p("12:00:01,1 fluxer.http WARNING Rate limited on https://api.fluxer.app/v1/webhooks/1/x, retry in 0.62s (attempt 1)") == ("rl", 0.62)
    assert p("Global rate limit hit, pausing for 3.00s") == ("global", 3.0)
    assert p("Server error 502 on https://x, retrying (attempt 1)") == ("5xx", None)
    assert p("Connection error: boom, retrying (attempt 2)") == ("conn", None)
    assert p("Fluxer: send failed after client retries (Failed after 5 attempts); pausing 5s") == ("giveup", None)
    assert p("ERROR src.fluxer.migrate_message Waterfall halted at message 5: x") == ("halt", None)
    assert p("INFO nothing interesting here") is None


def test_monitor_log_tail_handles_truncation(tmp_path):
    f = tmp_path / ".reaper.log"
    f.write_text("old line\n")
    t = mon.LogTail(f)                       # starts at end: old content ignored
    assert t.read_new() == []
    with open(f, "a") as fh:
        fh.write("a\nb\n")
    assert t.read_new() == ["a", "b"]
    f.write_text("fresh\n")                  # app restarted -> file truncated (mode='w')
    assert t.read_new() == ["fresh"]
