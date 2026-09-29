import asyncio
import sys
import types
import pytest

# The fluxer package may not be installed in the test env; stub only what writer.py imports.
if "fluxer" not in sys.modules:
    try:
        import fluxer  # noqa: F401
    except ImportError:
        stub = types.ModuleType("fluxer")
        for name in ("Bot", "Webhook", "Forbidden", "File"):
            setattr(stub, name, type(name, (), {}))
        sys.modules["fluxer"] = stub

import src.fluxer.writer as writer_mod
from src.fluxer.writer import FluxerWriter, MessageSendError


@pytest.fixture
def writer(monkeypatch):
    monkeypatch.setattr(writer_mod.asyncio, "sleep", _fast_sleep)
    return FluxerWriter(token="t", community_id="1")


_real_sleep = asyncio.sleep


async def _fast_sleep(_):
    await _real_sleep(0)


class _Rejected(Exception):
    status = 400


@pytest.mark.asyncio
async def test_retries_same_message_after_client_gives_up(writer):
    calls = {"n": 0}
    notices = []
    writer.on_rate_limit = notices.append

    async def attempt():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("Failed after 5 attempts: POST /x")
        return "msg-1"

    assert await writer._send_with_recovery(attempt, "c") == "msg-1"
    assert calls["n"] == 3 and len(notices) == 2


@pytest.mark.asyncio
async def test_gives_up_with_send_error_not_none(writer):
    async def attempt():
        raise RuntimeError("Failed after 5 attempts: POST /x")

    with pytest.raises(MessageSendError):
        await writer._send_with_recovery(attempt, "c")


@pytest.mark.asyncio
async def test_permanent_rejection_propagates_original(writer):
    async def attempt():
        raise _Rejected("bad embed")

    with pytest.raises(_Rejected):
        await writer._send_with_recovery(attempt, "c")


@pytest.mark.asyncio
async def test_cancel_stops_waiting(writer):
    writer.stop_check = lambda: True

    async def attempt():
        return "x"

    with pytest.raises(MessageSendError):
        await writer._send_with_recovery(attempt, "c")


def test_rate_limit_log_parsing(writer):
    import logging
    h = writer_mod._RateLimitLogHandler(writer)
    rec = logging.LogRecord("fluxer.http", logging.WARNING, "", 0,
                            "Rate limited on %s, retry in %.2fs (attempt %d)", ("https://x/y", 12.5, 1), None)
    h.emit(rec)
    assert 12 < writer._rate_limit_remaining() <= 12.5
    rec = logging.LogRecord("fluxer.http", logging.WARNING, "", 0,
                            "Global rate limit hit, pausing for %.2fs", (30.0,), None)
    h.emit(rec)
    assert writer._rate_limit_remaining() > 29
