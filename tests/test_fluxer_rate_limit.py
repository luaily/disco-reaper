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


def test_webhook_tokens_are_redacted():
    e = RuntimeError("Failed after 5 attempts: POST https://api.fluxer.app/v1/webhooks/123456/SeCrEt_tok-en.x?wait=true")
    out = writer_mod._redact(e)
    assert "SeCrEt" not in out and "/webhooks/123456/***" in out and "?wait=true" in out


class _FakeHTTP:
    """Mimics fluxer.HTTPClient.send_message, which indexes attachments as file["filename"] / file["data"]."""

    def __init__(self):
        self.calls = []

    async def send_message(self, channel_id, *, content=None, embeds=None, files=None, message_reference=None, **kw):
        for i, file in enumerate(files or []):
            file["filename"], file["data"]          # raises TypeError for fluxer.File objects
        self.calls.append({"channel_id": channel_id, "files": files, "ref": message_reference})
        return {"id": "555"}


def _bot_path_writer():
    import types as _t
    w = FluxerWriter(token="t", community_id="1")
    w.bot = _t.SimpleNamespace(_http=_FakeHTTP())
    w._ready_event.set()
    w._webhooks["chan"] = None          # no webhook -> bot path (also what replies use)
    return w


@pytest.mark.asyncio
async def test_reply_with_attachment_uses_plain_dicts_on_bot_path():
    w = _bot_path_writer()
    msg_id = await w.send_message(channel_id="chan", author_name="A", content="hi", timestamp=1,
                                  files=[{"filename": "a.png", "data": b"\x89PNG"}], reply_to_message_id="9")
    assert msg_id == "555"
    sent = w.client.calls[0]["files"]
    assert sent == [{"filename": "a.png", "data": b"\x89PNG"}]
    assert w.client.calls[0]["ref"]["message_id"] == "9"


@pytest.mark.asyncio
async def test_send_marker_with_files_uses_plain_dicts():
    w = _bot_path_writer()
    from fluxer import File
    import io as _io
    await w.send_marker("chan", "x", files=[File(_io.BytesIO(b"abc"), filename="m.txt"), {"filename": "b.bin", "data": b"z"}])
    sent = w.client.calls[0]["files"]
    assert {f["filename"] for f in sent} == {"m.txt", "b.bin"} and all(isinstance(f, dict) for f in sent)


# ── native replies through the webhook ─────────────────────────────────────

class _RefHTTP(_FakeHTTP):
    def __init__(self, reject_reference=False):
        super().__init__()
        self.requests = []
        self.reject_reference = reject_reference

    def _route(self, method, path, **kw):
        return (method, path.format(**kw))

    async def request(self, route, json=None, data=None, params=None, **kw):
        body = json
        if body is None and data is not None:          # multipart: payload_json is the first field
            import json as _j
            body = _j.loads(data._fields[0][2])        # (type_options, headers, value)
        self.requests.append({"route": route, "json": body, "multipart": data is not None, "params": params})
        if self.reject_reference and body.get("message_reference"):
            e = Exception("Unknown message")
            e.status = 400
            raise e
        return {"id": "777"}


class _FakeWebhook:
    id, token = 11, "tok"

    def __init__(self):
        self.sent = []

    async def send(self, **kw):
        self.sent.append(kw)
        return types.SimpleNamespace(id=888)


def _webhook_writer(reject_reference=False):
    w = FluxerWriter(token="t", community_id="1")
    http = _RefHTTP(reject_reference)
    w.bot = types.SimpleNamespace(_http=http)
    w._ready_event.set()
    w._webhooks["chan"] = _FakeWebhook()
    return w, http, w._webhooks["chan"]


@pytest.mark.asyncio
async def test_reply_goes_through_webhook_with_message_reference():
    w, http, wh = _webhook_writer()
    mid = await w.send_message(channel_id="chan", author_name="Bob", content="hi", timestamp=1,
                               author_avatar_url="http://a/x.png", reply_to_message_id="42")
    assert mid == "777" and wh.sent == []                       # raw webhook call, not Webhook.send / bot send
    req = http.requests[0]
    assert req["route"] == ("POST", "/webhooks/11/tok") and req["params"] == {"wait": "true"}
    assert req["json"]["message_reference"] == {"message_id": "42", "channel_id": "chan"}
    assert req["json"]["username"] == "Bob (discord)" and req["json"]["avatar_url"] == "http://a/x.png"
    assert http.calls == []                                      # bot path untouched


@pytest.mark.asyncio
async def test_reply_with_attachment_sends_multipart_with_reference():
    w, http, wh = _webhook_writer()
    await w.send_message(channel_id="chan", author_name="Bob", content="pic", timestamp=1,
                         files=[{"filename": "a.png", "data": b"\x89PNG"}], reply_to_message_id="42")
    req = http.requests[0]
    assert req["multipart"] and req["json"]["message_reference"]["message_id"] == "42"
    assert req["json"]["attachments"] == [{"id": 0, "filename": "a.png"}]


@pytest.mark.asyncio
async def test_plain_message_still_uses_webhook_send():
    w, http, wh = _webhook_writer()
    mid = await w.send_message(channel_id="chan", author_name="Bob", content="hi", timestamp=1)
    assert mid == "888" and http.requests == [] and len(wh.sent) == 1


@pytest.mark.asyncio
async def test_rejected_reference_retries_without_it_and_notes_it():
    w, http, wh = _webhook_writer(reject_reference=True)
    mid = await w.send_message(channel_id="chan", author_name="Bob", content="hi", timestamp=1, reply_to_message_id="42")
    assert mid == "888"                                          # delivered via plain webhook after the retry
    assert len(http.requests) == 1 and len(wh.sent) == 1
    assert "could not be linked" in wh.sent[0]["content"] and "hi" in wh.sent[0]["content"]


def test_reply_fallback_quote_format():
    from src.fluxer.migrate_message import format_reply_fallback as f
    out = f("line one\n\n  line   two @everyone", "Alice")
    assert out == "> line one line two @​everyone\n-# ↳ replying to `@Alice`\n"
    assert f("", "A", has_attachments=True).startswith("> [attachment]")
    assert f("", "A").startswith("> [message]")
    long = f("x" * 500, "A")
    assert long.splitlines()[0].endswith("…") and len(long.splitlines()[0]) <= 2 + 160
