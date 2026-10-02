"""Direct-message notifications from the migration bot itself.

Set `notify_user_id` (a Fluxer user ID; the Configuration screen, `reaper_config.yaml`, or `--notify-user`) and the
migration bot DMs that user when something needs attention (Fluxer down for a while / back again, a skipped message,
a halted run, the final summary) instead of you re-opening the terminal. It uses the bot that is already running the
migration: no extra bot, webhook or token. The user needs to share the community with the bot.

Notifications never get in the way of a migration: they are sent in the background, throttled per kind so an outage
can't flood you, and any failure to deliver one (DMs closed, API down) is logged once and otherwise ignored.
"""
import asyncio
import collections
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

_WEBHOOK_TOKEN_RE = re.compile(r"(/webhooks/\d+/)[^\s/?'\")]+")
_ICONS = {"info": "ℹ️", "ok": "✅", "warn": "⚠️", "error": "🛑"}
MAX_LEN = 1900                      # stay under the 2000-character message limit


def redact(text: str) -> str:
    """Webhook URLs contain a secret token; never put one in a chat message."""
    return _WEBHOOK_TOKEN_RE.sub(r"\1***", str(text))


class RecentLogs(logging.Handler):
    """Keeps the last few WARNING+ log lines so a DM about a failure can include what led up to it."""

    def __init__(self, maxlen: int = 60):
        super().__init__(level=logging.WARNING)
        self.lines: collections.deque = collections.deque(maxlen=maxlen)

    def emit(self, record):
        try:
            self.lines.append(f"{time.strftime('%H:%M:%S', time.localtime(record.created))} {record.levelname[:4]} "
                              f"{record.name.rsplit('.', 1)[-1]}: {redact(record.getMessage())}")
        except Exception:
            pass

    def tail(self, n: int = 6) -> List[str]:
        return [l[:200] for l in list(self.lines)[-n:]]


class FluxerNotifier:
    def __init__(self, writer: Any, user_id: str, min_gap: float = 2.0, send_timeout: float = 20.0):
        self.writer, self.user_id = writer, str(user_id)
        self.min_gap, self.send_timeout = min_gap, send_timeout
        self._dm: Optional[str] = None
        self._tasks: set = set()
        self._lock = asyncio.Lock()
        self._last_sent = 0.0
        self._last_by_key: Dict[str, float] = {}
        self._suppressed: Dict[str, int] = {}
        self._disabled_until = 0.0
        self._failures = 0
        self.sent: List[str] = []                       # what was delivered (handy for tests / the run summary)
        self.logs = RecentLogs()
        logging.getLogger().addHandler(self.logs)

    # ── public ─────────────────────────────────────────────────────────────
    def notify(self, text: str, kind: str = "info", key: Optional[str] = None, cooldown: float = 0.0,
               with_logs: bool = False) -> bool:
        """Queues a DM (returns immediately). `key` + `cooldown` throttle repeats of the same kind of event; messages
        suppressed meanwhile are counted into the next one. False = not queued."""
        now = time.monotonic()
        if key and cooldown and now - self._last_by_key.get(key, -1e9) < cooldown:
            self._suppressed[key] = self._suppressed.get(key, 0) + 1
            return False
        if key:
            self._last_by_key[key] = now
            n = self._suppressed.pop(key, 0)
            if n:
                text += f"\n(+{n} similar notification{'s' if n != 1 else ''} suppressed)"
        body = f"{_ICONS.get(kind, '')} {redact(text)}"
        if with_logs:
            lines = self.logs.tail()
            if lines:
                body += "\n```\n" + "\n".join(lines) + "\n```"
        try:
            task = asyncio.get_running_loop().create_task(self._send(body[:MAX_LEN]))
        except RuntimeError:                              # no event loop running (e.g. called from a thread)
            logger.info(f"notification not sent (no event loop): {text[:100]}")
            return False
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return True

    async def flush(self, timeout: float = 30.0) -> None:
        """Waits for queued notifications (call before shutting the connection down)."""
        if self._tasks:
            await asyncio.wait(set(self._tasks), timeout=timeout)
        try:
            logging.getLogger().removeHandler(self.logs)
        except Exception:
            pass

    # ── internals ──────────────────────────────────────────────────────────
    async def _ensure_dm(self) -> Optional[str]:
        if self._dm is None:
            chan = await self.writer.client.create_dm(self.user_id)
            self._dm = str(chan["id"])
        return self._dm

    async def _send(self, body: str) -> None:
        async with self._lock:
            if time.monotonic() < self._disabled_until:
                return
            gap = self.min_gap - (time.monotonic() - self._last_sent)
            if gap > 0:
                await asyncio.sleep(gap)
            try:
                dm = await asyncio.wait_for(self._ensure_dm(), self.send_timeout)
                await asyncio.wait_for(self.writer.client.send_message(dm, content=body), self.send_timeout)
                self._last_sent = time.monotonic()
                self._failures = 0
                self.sent.append(body)
            except Exception as e:
                self._failures += 1
                self._dm = None if self._failures > 1 else self._dm
                if self._failures in (1, 5, 20):
                    logger.warning(f"Could not DM Fluxer user {self.user_id} ({type(e).__name__}: {redact(e)}). Does the "
                                   f"user share the community with the bot and allow DMs from it?")
                self._disabled_until = time.monotonic() + min(600, 30 * self._failures)    # back off, never block the run


def describe_result(title: str, result: Dict[str, Any], elapsed: Optional[float] = None) -> tuple:
    """(kind, text) summary of a migration result dict, for the end-of-run notification."""
    sent, skipped = result.get("messages", 0), result.get("skipped", 0)
    parts = [f"**{title}**: {sent} message{'s' if sent != 1 else ''} migrated"]
    if result.get("already_on_server"):
        parts.append(f"{result['already_on_server']} already on the server")
    if skipped:
        ids = ", ".join(str(i) for i in result.get("skipped_ids", [])[:8])
        parts.append(f"{skipped} skipped ({ids}{'…' if skipped > 8 else ''})")
    if elapsed:
        parts.append(f"took {int(elapsed // 3600)}h{int(elapsed % 3600 // 60):02d}m")
    if result.get("error"):
        return "error", " · ".join(parts) + f"\n{result['error']}"
    if result.get("stopped") == "deadline":
        return "info", " · ".join(parts) + "\nPaused at the scheduled stop time; run again to continue."
    return ("warn" if skipped else "ok"), " · ".join(parts)


# ── progress reports ───────────────────────────────────────────────────────

def next_boundary(now: datetime, minutes: int) -> datetime:
    """The next wall-clock multiple of `minutes` after `now` (60 -> the next top of the hour)."""
    minutes = max(1, int(minutes))
    base = now.replace(second=0, microsecond=0)
    since_midnight = base.hour * 60 + base.minute
    nxt = (since_midnight // minutes + 1) * minutes
    return base.replace(hour=0, minute=0) + timedelta(minutes=nxt)


class ProgressReporter:
    """Direct-message progress reports while a migration runs.

    One report at the start (messages to send, the set send speed, estimated time) and then one at every wall-clock
    boundary (default: the top of every hour) with: sent so far, remaining, the set send speed, the average real send
    speed and the estimated time remaining. The hourly report goes out even while the run is paused (for example
    during an outage), so silence never means "unknown".

    Feed it the migration's stats dict with update(); it reads `messages` (sent), `already_on_server` and `skipped`.
    """

    def __init__(self, notify: Callable[..., Any], total: Optional[int], speed_cap: Callable[[], float],
                 title: str = "Migration", interval_minutes: int = 60, assumed_rate: float = 55.0,
                 clock: Callable[[], datetime] = datetime.now, sleep: Optional[Callable] = None):
        self.notify, self.total, self.speed_cap, self.title = notify, total, speed_cap, title
        self.interval, self.assumed_rate, self.clock = interval_minutes, assumed_rate, clock
        self._sleep = sleep or asyncio.sleep
        self.stats: Dict[str, Any] = {}
        self._t0 = time.monotonic()
        self._last_t, self._last_sent = self._t0, 0
        self._task: Optional[asyncio.Task] = None

    # ── numbers ────────────────────────────────────────────────────────────
    def update(self, stats: Dict[str, Any]) -> None:
        self.stats = stats

    def _sent(self) -> int:
        return int(self.stats.get("messages", 0))

    def _done(self) -> int:
        return self._sent() + int(self.stats.get("already_on_server", 0)) + int(self.stats.get("skipped", 0))

    def remaining(self) -> Optional[int]:
        return None if self.total is None else max(0, self.total - self._done())

    def _speed_text(self) -> str:
        cap = self.speed_cap()
        return f"{cap:g} msgs/min (cap)" if cap else "no cap (as fast as Fluxer allows)"

    @staticmethod
    def _fmt_eta(seconds: float) -> str:
        s = int(seconds)
        if s < 60:
            return "under a minute"
        h, m = s // 3600, (s % 3600) // 60
        text = f"~{h}h {m:02d}m" if h else f"~{m}m"
        return text + (f" (about {h // 24}d {h % 24}h)" if h >= 24 else "")

    # ── reports ────────────────────────────────────────────────────────────
    def start_report(self) -> str:
        rem = self.remaining()
        cap = self.speed_cap()
        rate = cap if cap else self.assumed_rate
        lines = [f"**{self.title} started**"]
        lines.append(f"• Messages to send: {rem:,}" if rem is not None else "• Messages to send: unknown (not counted)")
        lines.append(f"• Set send speed: {self._speed_text()}")
        if rem is not None:
            basis = f"at the {cap:g} msgs/min cap" if cap else f"assuming ~{rate:g} msgs/min, Fluxer's typical webhook rate"
            lines.append(f"• Estimated time remaining: {self._fmt_eta(rem / rate * 60) if rem else 'nothing to send'} — {basis}")
        return "\n".join(lines)

    def hourly_report(self) -> str:
        now = time.monotonic()
        sent, rem = self._sent(), self.remaining()
        avg = sent / max(now - self._t0, 1e-9) * 60 if sent else 0.0
        window = (sent - self._last_sent) / max(now - self._last_t, 1e-9) * 60
        stamp = self.clock().strftime("%H:%M")
        extra = []
        if self.stats.get("already_on_server"):
            extra.append(f"{self.stats['already_on_server']:,} already on the server")
        if self.stats.get("skipped"):
            extra.append(f"{self.stats['skipped']:,} skipped")
        lines = [f"**{self.title}: report at {stamp}**",
                 f"• Sent so far: {sent:,}" + (f" (+{', '.join(extra)})" if extra else "")]
        lines.append(f"• Remaining: {rem:,}" if rem is not None else "• Remaining: unknown (not counted)")
        lines.append(f"• Set send speed: {self._speed_text()}")
        lines.append(f"• Average real speed: {avg:.1f} msgs/min (last interval: {window:.1f} msgs/min)" if sent
                     else "• Average real speed: nothing sent yet (the run may be paused or waiting)")
        if rem is None:
            lines.append("• Estimated time remaining: unknown")
        elif rem == 0:
            lines.append("• Estimated time remaining: nothing left to send")
        elif avg > 0:
            lines.append(f"• Estimated time remaining: {self._fmt_eta(rem / avg * 60)} — at the average real speed")
        else:
            lines.append("• Estimated time remaining: unknown until messages are going out")
        self._last_t, self._last_sent = now, sent
        return "\n".join(lines)

    # ── lifecycle ──────────────────────────────────────────────────────────
    def start(self) -> None:
        self._t0 = self._last_t = time.monotonic()
        self.notify(self.start_report(), kind="info")
        try:
            self._task = asyncio.get_running_loop().create_task(self._loop())
        except RuntimeError:
            self._task = None

    async def _loop(self) -> None:
        while True:
            delay = (next_boundary(self.clock(), self.interval) - self.clock()).total_seconds()
            await self._sleep(max(delay, 1.0))
            self.notify(self.hourly_report(), kind="info")

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
