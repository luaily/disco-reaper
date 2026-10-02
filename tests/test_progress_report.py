import asyncio
from datetime import datetime, timedelta

import pytest

import src.core.notify as nt
from src.core.notify import ProgressReporter, next_boundary

_real_sleep = asyncio.sleep


class Clock:
    def __init__(self, start):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += timedelta(seconds=seconds)


def _reporter(total=131_204, cap=0.0, monotonic=None, monkeypatch=None, **kw):
    sent = []
    r = ProgressReporter(lambda text, **k: sent.append((k.get("kind"), text)), total, lambda: cap, title="Waterfall", **kw)
    r.sent = sent
    return r


# ── wall-clock boundaries ──────────────────────────────────────────────────

def test_next_boundary_is_the_next_top_of_the_hour():
    assert next_boundary(datetime(2026, 10, 1, 14, 23, 41), 60) == datetime(2026, 10, 1, 15, 0)
    assert next_boundary(datetime(2026, 10, 1, 14, 0, 0), 60) == datetime(2026, 10, 1, 15, 0)       # strictly after
    assert next_boundary(datetime(2026, 10, 1, 23, 30), 60) == datetime(2026, 10, 2, 0, 0)          # across midnight
    assert next_boundary(datetime(2026, 10, 1, 14, 23), 15) == datetime(2026, 10, 1, 14, 30)
    assert next_boundary(datetime(2026, 10, 1, 14, 23, 5), 1) == datetime(2026, 10, 1, 14, 24)


# ── the start report ───────────────────────────────────────────────────────

def test_start_report_uncapped_uses_an_assumed_rate_and_says_so():
    text = _reporter(131_204).start_report()
    assert "Waterfall started" in text and "Messages to send: 131,204" in text
    assert "Set send speed: no cap (as fast as Fluxer allows)" in text
    assert "Estimated time remaining: ~39h 45m (about 1d 15h) — assuming ~55 msgs/min" in text


def test_start_report_with_a_cap_uses_the_cap():
    text = _reporter(4_000, cap=40).start_report()
    assert "Set send speed: 40 msgs/min (cap)" in text and "~1h 40m — at the 40 msgs/min cap" in text


def test_start_report_when_nothing_was_counted_or_nothing_is_left():
    assert "unknown (not counted)" in _reporter(None).start_report()
    assert "nothing to send" in _reporter(0).start_report()


# ── the hourly report ──────────────────────────────────────────────────────

def _timed(monkeypatch, r, seconds_since_start):
    t = {"v": 1000.0}
    monkeypatch.setattr(nt.time, "monotonic", lambda: t["v"])
    r._t0 = r._last_t = 1000.0

    def at(s):
        t["v"] = 1000.0 + s
    return at


def test_hourly_report_has_every_figure(monkeypatch):
    r = _reporter(131_204, cap=0, clock=lambda: datetime(2026, 10, 1, 15, 0))
    at = _timed(monkeypatch, r, 0)
    at(3600)
    r.update({"messages": 3240})                                       # 54 msgs/min for an hour
    text = r.hourly_report()
    assert "report at 15:00" in text
    assert "Sent so far: 3,240" in text and "Remaining: 127,964" in text
    assert "Set send speed: no cap" in text
    assert "Average real speed: 54.0 msgs/min (last interval: 54.0 msgs/min)" in text
    assert "Estimated time remaining: ~39h 29m (about 1d 15h) — at the average real speed" in text
    at(7200)                                                           # the next hour is slower
    r.update({"messages": 3240 + 3000})
    text2 = r.hourly_report()
    assert "Sent so far: 6,240" in text2 and "Average real speed: 52.0 msgs/min (last interval: 50.0 msgs/min)" in text2


def test_already_on_server_and_skipped_count_as_done(monkeypatch):
    r = _reporter(1_000)
    at = _timed(monkeypatch, r, 0)
    at(600)
    r.update({"messages": 100, "already_on_server": 300, "skipped": 5})
    text = r.hourly_report()
    assert "Sent so far: 100 (+300 already on the server, 5 skipped)" in text and "Remaining: 595" in text


def test_no_sends_yet_or_no_count_is_stated_honestly(monkeypatch):
    r = _reporter(5_000)
    at = _timed(monkeypatch, r, 0)
    at(3600)
    text = r.hourly_report()
    assert "nothing sent yet" in text and "unknown until messages are going out" in text and "Remaining: 5,000" in text
    r2 = _reporter(None)
    at2 = _timed(monkeypatch, r2, 0)
    at2(3600)
    r2.update({"messages": 100})
    t2 = r2.hourly_report()
    assert "Remaining: unknown (not counted)" in t2 and "Estimated time remaining: unknown" in t2
    r3 = _reporter(100)
    at3 = _timed(monkeypatch, r3, 0)
    at3(3600)
    r3.update({"messages": 100})
    assert "nothing left to send" in r3.hourly_report()


# ── the schedule ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_reports_start_immediately_then_at_each_top_of_the_hour():
    clock = Clock(datetime(2026, 10, 1, 14, 23, 0))
    delays = []

    async def fake_sleep(d):
        delays.append(d)
        clock.advance(d)
        await _real_sleep(0.01)

    r = _reporter(1_000, clock=clock, sleep=fake_sleep)
    r.start()
    assert [k for k, _ in r.sent] == ["info"] and "started" in r.sent[0][1]          # the start report goes out at once
    await _real_sleep(0.08)
    await r.stop()
    assert delays[0] == pytest.approx(37 * 60)                                       # 14:23 -> 15:00
    assert all(d == pytest.approx(3600) for d in delays[1:]) and len(delays) >= 3    # then every hour on the hour
    hourly = [t for _, t in r.sent[1:]]
    assert hourly and all("report at" in t for t in hourly)
    assert hourly[0].startswith("**Waterfall: report at 15:00**") and "report at 16:00" in hourly[1]
    n = len(r.sent)
    await _real_sleep(0.05)
    assert len(r.sent) == n                                                          # stop() really stops the schedule


@pytest.mark.asyncio
async def test_reports_still_go_out_when_nothing_is_being_sent():
    clock = Clock(datetime(2026, 10, 1, 2, 59, 0))

    async def fake_sleep(d):
        clock.advance(d)
        await _real_sleep(0.01)
    r = _reporter(500, clock=clock, sleep=fake_sleep)
    r.start()                                                                        # no update() ever called: a paused run
    await _real_sleep(0.04)
    await r.stop()
    assert any("nothing sent yet" in t for _, t in r.sent[1:])
