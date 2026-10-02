import asyncio
import sys
import types

import pytest

try:
    import fluxer  # noqa: F401
except ImportError:
    stub = types.ModuleType("fluxer")
    for name in ("Bot", "Webhook", "Forbidden", "File"):
        setattr(stub, name, type(name, (), {}))
    sys.modules["fluxer"] = stub

import src.fluxer.writer as wm
from src.fluxer import uploads as up
from src.fluxer.uploads import PresignUnavailable, PresignedUploader, UploadError, guess_content_type
from src.fluxer.writer import FluxerWriter, MessageSendError

MB = 1024 * 1024


_real_sleep = asyncio.sleep


async def _nosleep(_):
    await _real_sleep(0)


class _HTTPError(Exception):
    def __init__(self, status):
        super().__init__(f"HTTP {status}")
        self.status = status


class FakeAPI:
    """Stands in for fluxer.HTTPClient: presign, complete, and webhook execution."""

    def __init__(self):
        self.presigns, self.completes, self.posts = [], [], []
        self.presign_error = None          # exception to raise from the presign call
        self.post_errors = []              # exceptions to raise (in order) from webhook POSTs

    def _route(self, method, path, **kw):
        return (method, path.format(**kw))

    async def request(self, route, json=None, data=None, params=None, **kw):
        method, path = route
        if path.endswith("/attachments/complete"):
            self.completes.append(json)
            return {"uploads": [{"upload_filename": f"done-{u['upload_filename']}"} for u in json["uploads"]]}
        if path.endswith("/attachments"):
            self.presigns.append(json)
            if self.presign_error:
                raise self.presign_error
            items = []
            for a in json["attachments"]:
                key = f"key{len(self.presigns)}-{a['id']}"
                if a["file_size"] <= 10 * MB:
                    items.append({**a, "upload_mode": "singlepart", "upload_filename": key, "upload_url": f"https://s3/{key}"})
                else:
                    n = -(-a["file_size"] // (10 * MB))
                    items.append({**a, "upload_mode": "multipart", "upload_filename": key, "upload_id": f"uid-{key}",
                                  "part_size": 10 * MB, "parts": [{"part_number": i + 1, "upload_url": f"https://s3/{key}/p{i + 1}"} for i in range(n)]})
            return {"attachments": items}
        if "/webhooks/" in path:
            if self.post_errors:
                raise self.post_errors.pop(0)
            self.posts.append({"json": json, "multipart": data is not None})
            return {"id": "777"}
        raise AssertionError(path)


class FakeStorage:
    closed = False

    def __init__(self, codes=()):
        self.puts, self.codes = [], list(codes)        # codes: statuses to return first, in order; then 200

    def put(self, url, data=None, headers=None, timeout=None):
        self.puts.append((url, len(data)))
        status = self.codes.pop(0) if self.codes else 200
        resp = types.SimpleNamespace(status=status)

        class Ctx:
            async def __aenter__(s):
                return resp

            async def __aexit__(s, *a):
                return False
        return Ctx()

    async def close(self):
        self.closed = True


def _uploader(api, storage, cancelled=lambda: False):
    return PresignedUploader(api, storage, cancelled=cancelled, sleep=_nosleep)


# ── uploader ───────────────────────────────────────────────────────────────

def test_content_type_guess():
    assert guess_content_type("a.png") == "image/png" and guess_content_type("x.gif") == "image/gif"
    assert guess_content_type("blob.unknownext") == "application/octet-stream"


@pytest.mark.asyncio
async def test_small_file_is_one_presign_one_put():
    api, st = FakeAPI(), FakeStorage()
    out = await _uploader(api, st).upload("chan", [{"filename": "a.png", "data": b"\x89PNG" * 10}])
    assert out == [{"id": 0, "filename": "a.png", "content_type": "image/png", "upload_filename": "key1-0", "file_size": 40}]
    assert api.presigns[0]["attachments"] == [{"id": 0, "filename": "a.png", "file_size": 40, "content_type": "image/png"}]
    assert st.puts == [("https://s3/key1-0", 40)] and api.completes == []


@pytest.mark.asyncio
async def test_large_file_uses_multipart_parts_then_complete():
    api, st = FakeAPI(), FakeStorage()
    out = await _uploader(api, st).upload("chan", [{"filename": "big.bin", "data": b"x" * (12 * MB)}])
    assert [n for _, n in st.puts] == [10 * MB, 2 * MB]
    assert api.completes == [{"uploads": [{"upload_filename": "key1-0", "upload_id": "uid-key1-0"}]}]
    assert out[0]["upload_filename"] == "done-key1-0" and out[0]["file_size"] == 12 * MB


@pytest.mark.asyncio
async def test_more_than_ten_files_are_batched_with_contiguous_ids():
    api, st = FakeAPI(), FakeStorage()
    out = await _uploader(api, st).upload("chan", [{"filename": f"f{i}.txt", "data": b"d"} for i in range(12)])
    assert [len(p["attachments"]) for p in api.presigns] == [10, 2]
    assert [a["id"] for a in out] == list(range(12))


@pytest.mark.asyncio
async def test_storage_503_is_retried_then_succeeds_or_gives_up():
    api, st = FakeAPI(), FakeStorage(codes=[503, 503])
    out = await _uploader(api, st).upload("chan", [{"filename": "a.txt", "data": b"d"}])
    assert len(st.puts) == 3 and out[0]["upload_filename"] == "key1-0"            # same URL retried, no re-presign
    assert len(api.presigns) == 1
    with pytest.raises(UploadError):
        await _uploader(FakeAPI(), FakeStorage(codes=[503] * 20)).upload("chan", [{"filename": "a.txt", "data": b"d"}])


@pytest.mark.asyncio
async def test_expired_url_triggers_a_fresh_presign():
    api, st = FakeAPI(), FakeStorage(codes=[403])
    out = await _uploader(api, st).upload("chan", [{"filename": "a.txt", "data": b"d"}])
    assert len(api.presigns) == 2 and out[0]["upload_filename"] == "key2-0"
    with pytest.raises(UploadError):
        await _uploader(FakeAPI(), FakeStorage(codes=[403] * 20)).upload("chan", [{"filename": "a.txt", "data": b"d"}])


@pytest.mark.asyncio
async def test_unavailable_vs_transient_presign_failures():
    api = FakeAPI()
    api.presign_error = _HTTPError(404)
    with pytest.raises(PresignUnavailable):
        await _uploader(api, FakeStorage()).upload("chan", [{"filename": "a.txt", "data": b"d"}])
    api.presign_error = RuntimeError("Failed after 5 attempts: POST x")
    with pytest.raises(UploadError):
        await _uploader(api, FakeStorage()).upload("chan", [{"filename": "a.txt", "data": b"d"}])


@pytest.mark.asyncio
async def test_cancel_stops_the_upload():
    with pytest.raises(UploadError):
        await _uploader(FakeAPI(), FakeStorage(), cancelled=lambda: True).upload("chan", [{"filename": "a.txt", "data": b"d"}])


# ── writer integration ─────────────────────────────────────────────────────

class _Webhook:
    id, token = 11, "tok"

    def __init__(self):
        self.sent = []

    async def send(self, **kw):
        self.sent.append(kw)
        return types.SimpleNamespace(id=888)


def _writer(api=None, storage=None):
    w = FluxerWriter(token="t", community_id="1")
    w.bot = types.SimpleNamespace(_http=api or FakeAPI())
    w._ready_event.set()
    w._webhooks["chan"] = _Webhook()
    w._storage_session = storage or FakeStorage()
    return w


FILES = [{"filename": "a.png", "data": b"\x89PNG" * 50}]


@pytest.mark.asyncio
async def test_message_with_files_posts_small_json_referencing_the_uploads(monkeypatch):
    monkeypatch.setattr(up.asyncio, "sleep", _nosleep)
    w = _writer()
    mid = await w.send_message(channel_id="chan", author_name="Bob", content="pic", timestamp=1, files=FILES)
    assert mid == "777" and w._webhooks["chan"].sent == []                    # not through Webhook.send / multipart
    post = w.client.posts[0]
    assert post["multipart"] is False
    assert post["json"]["attachments"] == [{"id": 0, "filename": "a.png", "content_type": "image/png",
                                            "upload_filename": "key1-0", "file_size": 200}]
    assert "message_reference" not in post["json"] and post["json"]["username"] == "Bob (discord)"


@pytest.mark.asyncio
async def test_reply_with_files_has_reference_and_uploaded_attachments():
    w = _writer()
    await w.send_message(channel_id="chan", author_name="Bob", content="re", timestamp=1, files=FILES, reply_to_message_id="42")
    j = w.client.posts[0]["json"]
    assert j["message_reference"] == {"message_id": "42", "channel_id": "chan"} and j["attachments"][0]["upload_filename"] == "key1-0"


@pytest.mark.asyncio
async def test_failed_upload_posts_nothing_and_raises_send_error(monkeypatch):
    monkeypatch.setattr(wm, "_UPLOAD_ROUNDS", 1)
    monkeypatch.setattr(wm.asyncio, "sleep", _nosleep)
    monkeypatch.setattr(up.asyncio, "sleep", _nosleep)
    w = _writer(storage=FakeStorage(codes=[503] * 50))
    with pytest.raises(MessageSendError):
        await w.send_message(channel_id="chan", author_name="Bob", content="pic", timestamp=1, files=FILES)
    assert w.client.posts == [] and w._webhooks["chan"].sent == []            # nothing was delivered -> safe to retry


@pytest.mark.asyncio
async def test_instance_without_presigned_uploads_falls_back_to_multipart_and_stops_asking():
    api = FakeAPI()
    api.presign_error = _HTTPError(404)
    w = _writer(api)
    await w.send_message(channel_id="chan", author_name="Bob", content="a", timestamp=1, files=FILES)
    assert w.presigned_uploads is False and len(w._webhooks["chan"].sent) == 1          # legacy Webhook.send with files
    n = len(api.presigns)
    await w.send_message(channel_id="chan", author_name="Bob", content="b", timestamp=2, files=FILES)
    assert len(api.presigns) == n                                                    # didn't try presigning again


@pytest.mark.asyncio
async def test_message_rejected_with_uploaded_attachments_retries_with_multipart():
    api = FakeAPI()
    api.post_errors = [_HTTPError(400)]
    w = _writer(api)
    mid = await w.send_message(channel_id="chan", author_name="Bob", content="pic", timestamp=1, files=FILES)
    assert mid == "888" and len(w._webhooks["chan"].sent) == 1 and api.posts == []     # delivered via the legacy form


@pytest.mark.asyncio
async def test_message_without_files_is_unchanged():
    w = _writer()
    mid = await w.send_message(channel_id="chan", author_name="Bob", content="hi", timestamp=1)
    assert mid == "888" and w.client.presigns == []


# ── oversized files, 413, and rejected messages ────────────────────────────

class _CodedError(Exception):
    def __init__(self, status, code, message):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


@pytest.mark.asyncio
async def test_presign_error_codes_are_told_apart():
    api = FakeAPI()
    api.presign_error = _CodedError(400, "FILE_SIZE_TOO_LARGE", "File size is too large. Maximum file size is 52428800.")
    with pytest.raises(up.FileTooLarge) as ei:
        await _uploader(api, FakeStorage()).upload("chan", [{"filename": "a.bin", "data": b"d"}])
    assert ei.value.limit == 52428800
    api.presign_error = _CodedError(400, "INVALID_FORM_BODY", "bad")
    with pytest.raises(up.PresignRejected):
        await _uploader(api, FakeStorage()).upload("chan", [{"filename": "a.bin", "data": b"d"}])
    api.presign_error = _CodedError(404, "NOT_FOUND", "Not found.")
    with pytest.raises(PresignUnavailable):
        await _uploader(api, FakeStorage()).upload("chan", [{"filename": "a.bin", "data": b"d"}])


@pytest.mark.asyncio
async def test_oversized_files_are_left_out_with_a_note_and_the_rest_is_sent():
    w = _writer()
    w.max_file_bytes = 100
    mid = await w.send_message(channel_id="chan", author_name="Bob", content="look", timestamp=1,
                               files=[{"filename": "ok.txt", "data": b"x" * 50}, {"filename": "huge.mp4", "data": b"x" * 5_000}])
    assert mid == "777"
    j = w.client.posts[0]["json"]
    assert [a["filename"] for a in j["attachments"]] == ["ok.txt"]                       # only the small file was uploaded
    assert "huge.mp4" in j["content"] and "not migrated" in j["content"] and "look" in j["content"]
    assert [m for p in w.client.presigns for m in p["attachments"]] == [
        {"id": 0, "filename": "ok.txt", "file_size": 50, "content_type": "text/plain"}]


@pytest.mark.asyncio
async def test_only_oversized_files_still_sends_the_text():
    w = _writer()
    w.max_file_bytes = 10
    mid = await w.send_message(channel_id="chan", author_name="Bob", content="", timestamp=1,
                               files=[{"filename": "huge.mp4", "data": b"x" * 500}])
    assert mid == "888" or mid == "777"                                                 # text-only message with the note
    assert w.client.presigns == []


@pytest.mark.asyncio
async def test_a_smaller_server_limit_is_learned_and_the_message_resent():
    api = FakeAPI()
    api.presign_error = _CodedError(400, "FILE_SIZE_TOO_LARGE", "File size is too large. Maximum file size is 1000.")
    w = _writer(api)
    mid = await w.send_message(channel_id="chan", author_name="Bob", content="x", timestamp=1,
                               files=[{"filename": "a.bin", "data": b"x" * 2000}])
    assert w.max_file_bytes == 1000 and mid is not None                                  # learned, then resent without it
    assert "a.bin" in (w._webhooks["chan"].sent[0]["content"] if w._webhooks["chan"].sent else api.posts[0]["json"]["content"])


@pytest.mark.asyncio
async def test_other_presign_refusals_only_affect_that_message():
    api = FakeAPI()
    api.presign_error = _CodedError(400, "INVALID_FORM_BODY", "bad filename")
    w = _writer(api)
    mid = await w.send_message(channel_id="chan", author_name="Bob", content="x", timestamp=1, files=FILES)
    assert w.presigned_uploads is True and mid == "888" and len(w._webhooks["chan"].sent) == 1


@pytest.mark.asyncio
async def test_413_on_the_multipart_form_resends_text_without_the_files():
    api = FakeAPI()
    api.presign_error = _HTTPError(404)                  # no presigned uploads here -> multipart path
    w = _writer(api)

    class Big(Exception):
        status = 413
    sent = []

    async def send(**kw):
        sent.append(kw)
        if kw.get("files"):
            raise Big("413 payload too large")
        return types.SimpleNamespace(id=999)
    w._webhooks["chan"].send = send
    mid = await w.send_message(channel_id="chan", author_name="Bob", content="hi", timestamp=1, files=FILES)
    assert mid == "999" and len(sent) == 2 and not sent[1].get("files")
    assert "a.png" in sent[1]["content"] and "too large" in sent[1]["content"]


@pytest.mark.asyncio
async def test_permanent_rejection_returns_none_and_records_why():
    w = _writer()

    class Bad(Exception):
        status = 400

    async def send(**kw):
        raise Bad("Invalid embed")
    w._webhooks["chan"].send = send
    assert await w.send_message(channel_id="chan", author_name="Bob", content="x", timestamp=1) is None
    assert "Invalid embed" in w.last_rejection
