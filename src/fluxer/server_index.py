"""What is actually on the Fluxer server, read from the server itself (never from the migration database).

Used by "start the Waterfall from message X": every message from X onward is checked against the channel on Fluxer and
only the ones that really aren't there are sent. That is how you repair messages that were skipped (or lost) because of
a bug, even when the local database claims otherwise.

How a migrated message is recognised: the migration posts it as the webhook "<name> (discord)" with a body that starts
`-# <t:EPOCH:D>` where EPOCH is the original message's creation time in seconds. So (webhook name, EPOCH) identifies a
source message; the same pair is counted, not just tested, so two messages from one person in the same second need two
copies on the server. Older builds sent replies through the bot as `-# <t:EPOCH:D>` + `-# · Name`; those are recognised
too. The bot's own "There was an error migrating message `ID`" markers are indexed as well: a skipped message is NOT
there, and once it has been sent properly its stale marker can be deleted.
"""
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

PREFIX_RE = re.compile(r"^-# <t:(\d+):D>\n")
BOT_NAME_RE = re.compile(r"^-# · (.+?)(?:\n|$)")
MARKER_RE = re.compile(r"There was an error migrating message `(\d+)`")
PAGE = 100


class ServerIndex:
    def __init__(self, writer: Any, start_ts: Optional[float], bot_username: Optional[str] = None,
                 stop_after_older: int = 200):
        self.writer, self.bot_username = writer, bot_username
        self.start_ts = start_ts
        self.stop_after_older = stop_after_older
        self._copies: Dict[str, Dict[Tuple[str, int], List[str]]] = {}      # channel -> (username, epoch) -> server ids
        self._markers: Dict[str, Dict[str, str]] = {}                       # channel -> source id -> marker message id
        self.scanned: Dict[str, int] = {}                                   # channel -> server messages read

    @staticmethod
    def fingerprint(message: dict, bot_username: Optional[str]) -> Optional[Tuple[str, int]]:
        """(webhook name, epoch) if the server message is a migrated copy of a source message, else None."""
        content = message.get("content") or ""
        m = PREFIX_RE.match(content)
        if not m:
            return None
        epoch = int(m.group(1))
        username = (message.get("author") or {}).get("username") or ""
        if username.endswith(" (discord)"):
            return username, epoch
        if bot_username and username == bot_username:                       # reply sent through the bot by an older build
            n = BOT_NAME_RE.match(content[m.end():])
            if n:
                return f"{n.group(1)} (discord)", epoch
        return None

    async def _scan(self, channel_id: str) -> None:
        """Reads the channel newest-first until it is well past the starting point."""
        copies: Dict[Tuple[str, int], List[str]] = {}
        markers: Dict[str, str] = {}
        before, older_run, total = None, 0, 0
        client = self.writer.client
        while True:
            kwargs = {"limit": PAGE}
            if before:
                kwargs["before"] = before
            page = await client.get_messages(channel_id, **kwargs)
            if not page:
                break
            for m in page:
                total += 1
                mk = MARKER_RE.search(m.get("content") or "")
                if mk and (not self.bot_username or (m.get("author") or {}).get("username") == self.bot_username):
                    markers[mk.group(1)] = str(m["id"])
                    continue
                fp = self.fingerprint(m, self.bot_username)
                if fp is None:
                    continue
                copies.setdefault(fp, []).append(str(m["id"]))
                if self.start_ts is not None and fp[1] < self.start_ts - 1:
                    older_run += 1
                else:
                    older_run = 0
            if len(page) < PAGE or (self.start_ts is not None and older_run >= self.stop_after_older):
                break
            before = min(int(x["id"]) for x in page)
        for ids in copies.values():
            ids.sort(key=int)                                               # oldest first: earliest source -> earliest copy
        self._copies[channel_id], self._markers[channel_id], self.scanned[channel_id] = copies, markers, total
        logger.info(f"Server index for channel {channel_id}: {sum(len(v) for v in copies.values())} migrated messages, "
                    f"{len(markers)} skip markers ({total} read)")

    async def find(self, channel_id: str, username: str, epoch: int) -> Optional[str]:
        """Server message ID of an existing copy of the source message (each copy is used once), or None."""
        channel_id = str(channel_id)
        if channel_id not in self._copies:
            await self._scan(channel_id)
        ids = self._copies[channel_id].get((username, int(epoch)))
        return ids.pop(0) if ids else None

    def marker_for(self, channel_id: str, source_id: Any) -> Optional[str]:
        return self._markers.get(str(channel_id), {}).get(str(source_id))
