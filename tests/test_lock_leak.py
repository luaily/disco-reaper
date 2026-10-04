import asyncio
import sys
import types

import pytest

import src.fluxer.writer as wm
from src.fluxer.writer import FluxerWriter, MessageSendError, SendTimeout

_real_sleep = asyncio.sleep


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    monkeypatch.setattr(wm, "_SEND_TIMEOUT", 0.05)
    monkeypatch.setattr(wm, "_STALL_GRACE", 0.12)
    monkeypatch.setattr(wm, "_STALL_POLL", 0.04)


# ── repair_rate_limiter ────────────────────────────────────────────────────

class _Limiter:
    def __init__(self, n=2):
        self._locks = {f"bucket{i}": asyncio.Lock() for i in range(n)}


def _bare_writer(limiter):
    w = FluxerWriter(token="t", community_id="1")
    w.bot = types.SimpleNamespace(_http=types.SimpleNamespace(_rate_limiter=limiter))
    return w


@pytest.mark.asyncio
async def test_repair_releases_only_locks_that_are_stuck():
    lim = _Limiter(3)
    await lim._locks["bucket1"].acquire()
    await lim._locks["bucket2"].acquire()
    assert _bare_writer(lim).repair_rate_limiter() == 2
    assert not any(l.locked() for l in lim._locks.values())
    assert _bare_writer(lim).repair_rate_limiter() == 0                      # nothing left to do
    assert FluxerWriter(token="t", community_id="1").repair_rate_limiter() == 0   # no client yet: no crash


# ── a cancelled request must not wedge later sends ─────────────────────────

class _LeakyClient:
    """Mimics the fluxer.py HTTP client's lock handling: take the bucket lock, release it only when the request returns."""

    def __init__(self, first_hangs=True):
        self._rate_limiter = _Limiter(1)
        self.first_hangs, self.calls, self.recent = first_hangs, 0, []

    def _route(self, method, path, **kw):
        return (method, path.format(**kw))

    async def request(self, route, json=None, data=None, params=None, **kw):
        lock = self._rate_limiter._locks["bucket0"]
        await lock.acquire()                                            # a cancelled request never gets to release this
        self.calls += 1
        if self.first_hangs and self.calls == 1:
            await _real_sleep(30)                                       # the answer never arrives in time
        lock.release()
        return {"id": f"m{self.calls}"}

    async def get_messages(self, channel_id, limit=50, before=None, after=None):
        return self.recent


def _writer(client):
    w = FluxerWriter(token="t", community_id="1")
    w.bot = types.SimpleNamespace(_http=client)
    w._ready_event.set()
    w._webhooks["chan"] = types.SimpleNamespace(id=5, token="tok", send=None)
    return w


@pytest.mark.asyncio
async def test_after_a_timeout_the_next_send_goes_through_instead_of_hanging():
    client = _LeakyClient()
    w = _writer(client)
    # reply sends use the direct webhook call, which is the path through the library's lock
    with pytest.raises(SendTimeout):
        await w.send_message(channel_id="chan", author_name="Al", content="first", timestamp=1, reply_to_message_id="9")
    assert not client._rate_limiter._locks["bucket0"].locked()           # the leaked lock was released
    mid = await asyncio.wait_for(
        w.send_message(channel_id="chan", author_name="Al", content="second", timestamp=2, reply_to_message_id="9"), 5)
    assert mid == "m2"                                                    # would hang forever without the repair


@pytest.mark.asyncio
async def test_a_message_that_landed_is_found_during_the_grace_period_and_not_resent():
    client = _LeakyClient()
    w = _writer(client)

    async def recent_after_a_moment(channel_id, limit=50, before=None, after=None):
        await _real_sleep(0.05)
        return [{"id": "777", "content": "-# <t:1:D>\nfirst", "author": {"username": "Al (discord)"}}]
    client.get_messages = recent_after_a_moment
    mid = await w.send_message(channel_id="chan", author_name="Al", content="first", timestamp=1, reply_to_message_id="9")
    assert mid == "777" and client.calls == 1                             # not sent a second time
    assert not client._rate_limiter._locks["bucket0"].locked()


@pytest.mark.asyncio
async def test_a_slow_answer_is_accepted_without_cancelling_anything(monkeypatch):
    client = _LeakyClient(first_hangs=False)

    async def slow(route, json=None, **kw):
        lock = client._rate_limiter._locks["bucket0"]
        await lock.acquire()
        await _real_sleep(0.08)                                           # slower than the 0.05s timeout, faster than the grace
        lock.release()
        return {"id": "slow1"}
    client.request = slow
    w = _writer(client)
    assert await w.send_message(channel_id="chan", author_name="Al", content="x", timestamp=1, reply_to_message_id="9") == "slow1"


@pytest.mark.asyncio
async def test_cancelling_a_run_mid_send_also_frees_the_lock():
    client = _LeakyClient()
    w = _writer(client)
    state = {"stop": False}
    w.stop_check = lambda: state["stop"]

    async def stop_soon():
        await _real_sleep(0.02)
        state["stop"] = True
    asyncio.get_running_loop().create_task(stop_soon())
    with pytest.raises(MessageSendError):
        await w.send_message(channel_id="chan", author_name="Al", content="x", timestamp=1, reply_to_message_id="9")
    assert not client._rate_limiter._locks["bucket0"].locked()


@pytest.mark.asyncio
async def test_wait_for_service_frees_a_stuck_lock_when_the_api_looks_healthy():
    import src.fluxer.migrate_message as mm
    mm.OUTAGE_BACKOFF = (0,)
    calls = {"repair": 0}

    class FakeWriter:
        async def check_health(self, ch):
            return True, "ok"

        def repair_rate_limiter(self):
            calls["repair"] += 1
            return 1
    ctx = types.SimpleNamespace(is_running=True, deadline_reached=lambda: False, on_notice=None,
                                config=types.SimpleNamespace(max_outage_minutes=0), fluxer_writer=FakeWriter())
    import time
    await mm._wait_for_service(ctx, "T", types.SimpleNamespace(id=1), SendTimeout("x"), time.time(), 0)
    assert calls["repair"] == 1
