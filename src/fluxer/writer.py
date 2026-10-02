import asyncio
import io
import json
import logging
import re
import time
from typing import Optional, List, Dict, Any, Callable
from fluxer import Bot, Webhook, Forbidden, File
from src.fluxer.uploads import FileTooLarge, PresignedUploader, PresignRejected, PresignUnavailable, UploadError

logger = logging.getLogger(__name__)

# Number of pause-and-retry rounds after the fluxer HTTP client itself gives up
# (it retries 429/5xx/connection errors only a few times before raising RuntimeError).
_MAX_RECOVERY_ROUNDS = 8
_SEND_TIMEOUT = 45.0
_RATE_LIMIT_RE = re.compile(r"(?:rate limit(?:ed)?.*?retry in|global rate limit.*?pausing for)\s+([\d.]+)s", re.IGNORECASE)


_WEBHOOK_URL_RE = re.compile(r"(/webhooks/\d+/)[^\s/?'\")]+")


def _redact(text) -> str:
    """Strips webhook tokens from error text (the fluxer client embeds the full webhook URL in its errors)."""
    return _WEBHOOK_URL_RE.sub(r"\1***", str(text))


_ASSUMED_UPLOAD_BPS = 100_000      # assume at least ~100 KB/s up so big attachments aren't timed out too early
_DELIVERY_LOOKBACK_S = 180         # how far back to look for a message whose send "timed out"
_UPLOAD_ROUNDS = 3                # pause-and-retry rounds for a transient upload failure (5s, 10s, 20s)


def _upload_timeout(files) -> float:
    """Send timeout that grows with the payload (a flat 45s can never succeed for a large attachment)."""
    total = sum(len(f["data"]) for f in files) if files else 0
    return min(900.0, _SEND_TIMEOUT + total / _ASSUMED_UPLOAD_BPS)


class MessageSendError(Exception):
    """A message could NOT be delivered for a transient reason (rate limit that never cleared,
    timeout, outage, cancellation). The caller must stop and must NOT record the message as
    migrated, so a resume retries it instead of skipping it."""


class ServiceUnavailable(MessageSendError):
    """The service is struggling (503/5xx, timeouts, connection errors, rate limits or uploads that never clear). This says
    nothing about the message itself, so callers must pause and retry the SAME message rather than count it as a failure
    or skip it."""


class SendTimeout(ServiceUnavailable):
    """The send timed out with no rate limit active: the message may or may not have been delivered."""


class _RateLimitLogHandler(logging.Handler):
    """Watches the fluxer HTTP client's own rate-limit warnings so we know when we're paused."""

    def __init__(self, writer: "FluxerWriter"):
        super().__init__(level=logging.WARNING)
        self.writer = writer

    def emit(self, record):
        try:
            m = _RATE_LIMIT_RE.search(record.getMessage())
            if m:
                self.writer._note_rate_limit(float(m.group(1)))
        except Exception:
            pass


class FluxerWriter:
    def __init__(self, token: str, community_id: str, api_url: str = "default"):
        # Rate-limit awareness (see _note_rate_limit / _send_with_recovery)
        self.rate_limited_until: float = 0.0
        self.on_rate_limit: Optional[Callable[[float], None]] = None  # UI hook: called with seconds to wait
        self.stop_check: Optional[Callable[[], bool]] = None          # returns True when the run was cancelled
        self.min_send_interval: float = 0.0   # optional self-imposed pacing (seconds between sends); 0 = off
        self._last_send_at: float = 0.0
        # Attachments go straight to Fluxer's object storage (presigned URLs) instead of through the API servers;
        # turned off automatically for an instance that doesn't offer it, then the old multipart form is used.
        self.presigned_uploads: bool = True
        self._storage_session = None
        # Fluxer refuses files over this size (52,428,800 B on the public instance; learned from the API if it differs).
        # Bigger files are left out of the message with a visible note instead of failing the whole message.
        self.max_file_bytes: int = 50 * 1024 * 1024
        self.last_rejection: Optional[str] = None     # why the last send_message returned None (permanent rejection)
        self._rl_handler: Optional[_RateLimitLogHandler] = None
        self.token = token
        self.community_id = str(community_id)
        self.api_url = api_url
        self.bot: Optional[Bot] = None
        self._bot_task: Optional[asyncio.Task] = None
        self._ready_event = asyncio.Event()
        self._webhooks: Dict[str, Webhook] = {} # channel_id -> Webhook
        self._channels_cache: List[Dict[str, Any]] | None = None

    @staticmethod
    async def fetch_guilds(token: str, api_url: str = "default") -> list[tuple[str, str]]:
        """Fetches the list of Fluxer communities the bot is in. Returns list of (label, id)."""
        from fluxer import HTTPClient, Guild
        
        http_kwargs = {}
        if api_url and api_url != "default":
            http_kwargs["api_url"] = api_url
            
        async with HTTPClient(token, **http_kwargs) as http:
            try:
                guilds_data = await http.get_current_user_guilds()
                guilds_list = []
                for g_data in guilds_data:
                    g = Guild.from_data(g_data)
                    label = f"{g.id}-{g.name}"
                    guilds_list.append((label, str(g.id)))
                return guilds_list
            except Exception as e:
                print(f"Failed to fetch Fluxer communities via HTTP: {e}")
                logger.error(f"Failed to fetch Fluxer communities via HTTP: {e}")
                raise

    async def _get_or_create_webhook(self, channel_id: str) -> Optional[Webhook]:
        """Gets an existing webhook for the channel or creates one."""
        if channel_id in self._webhooks:
            return self._webhooks[channel_id]
        
        assert self.client is not None
        try:
            # 1. Try to find existing webhook named "ReapersWebhook"
            webhooks_data = await self.client.get_channel_webhooks(channel_id)
            for w_data in webhooks_data:
                if w_data.get("name") == "ReapersWebhook":
                    w = Webhook.from_data(w_data, self.client)
                    self._webhooks[channel_id] = w
                    return w
            
            # 2. Create new one if not found
            w_data = await self.client.create_webhook(channel_id, name="ReapersWebhook")
            w = Webhook.from_data(w_data, self.client)
            self._webhooks[channel_id] = w
            return w
        except Exception as e:
            print(f"Failed to manage webhook for channel {channel_id}: {e}")
            logger.error(f"Failed to manage webhook for channel {channel_id}: {e}")
            return None

    async def start(self):
        # ... (lines 14-35)
        # (I will use multi_replace or just replace_file_content carefully)
        # Actually I'm using replace_file_content so I need to provide the whole block.
        if self.bot and self._bot_task and not self._bot_task.done():
            return

        bot_kwargs = {}
        if self.api_url and self.api_url != "default":
            bot_kwargs["api_url"] = self.api_url
            
        self.bot = Bot(**bot_kwargs)
        self._ready_event.clear()

        if self._rl_handler is None:
            self._rl_handler = _RateLimitLogHandler(self)
            logging.getLogger("fluxer.http").addHandler(self._rl_handler)

        # Define a simple on_ready listener to signal when we're connected
        @self.bot.event
        async def on_ready():
            self._ready_event.set()

        # Start the bot in the background
        self._bot_task = asyncio.create_task(self.bot.start(self.token))
        
        # Wait for the bot to be ready (timeout of 10s to be safe)
        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            pass

    @property
    def client(self):
        """Helper to access the underlying HTTP client."""
        return self.bot._http if self.bot else None

    async def validate(self) -> dict:
        """Validates the token, community ID, and permissions."""
        if not self.bot or not self._ready_event.is_set():
            await self.start()
        
        is_token_valid = False
        is_community_valid = False
        bot_name = None
        community_name = None
        error_reason = None
        permissions = {
            "administrator": False
        }

        try:
            # Check token by fetching me
            me_id = None
            try:
                if self.bot and self.bot.user:
                    is_token_valid = True
                    bot_name = self.bot.user.username
                    me_id = self.bot.user.id
                else:
                    me = await self.client.get_current_user()
                    if me:
                        is_token_valid = True
                        bot_name = me.get("username")
                        me_id = int(me["id"])
            except Exception as e:
                error_reason = f"Token Error: {str(e)}"
                return {
                    "token": False,
                    "community": False,
                    "bot_name": None,
                    "community_name": None,
                    "error_reason": error_reason,
                    "permissions": permissions
                }
            
            # Check community and permissions concurrently
            try:
                # 1. Fetch data concurrently
                guild_data, member_data, roles_data = await asyncio.gather(
                    self.client.get_guild(self.community_id),
                    self.client.get_guild_member(self.community_id, me_id),
                    self.client.get_guild_roles(self.community_id)
                )

                if guild_data:
                    is_community_valid = True
                    community_name = guild_data.get("name")
                    owner_id = int(guild_data.get("owner_id", 0))
                    
                    # 2. Compute effective permissions
                    member_role_ids = {int(r) for r in member_data.get("roles", [])}
                    computed_perms = 0
                    guild_id_int = int(self.community_id)
                    
                    for r_data in roles_data:
                        r_id = int(r_data["id"])
                        # Add permissions for @everyone (role ID == guild ID) or roles the bot has
                        if r_id == guild_id_int or r_id in member_role_ids:
                            computed_perms |= int(r_data.get("permissions", 0))
                    
                    # 3. Check for Administrator bypass (Guild Owner or Administrator bit 1<<3)
                    is_admin = (me_id == owner_id) or bool(computed_perms & (1 << 3))
                    
                    # 4. Map permissions dictionary
                    permissions["administrator"] = is_admin
                else:
                    error_reason = "Community not found"
            except Exception as e:
                error_reason = f"Community/Permission Error: {str(e)}"
        except Exception as e:
            error_reason = str(e)
            
        return {
            "token": is_token_valid,
            "community": is_community_valid,
            "bot_name": bot_name,
            "community_name": community_name,
            "error_reason": error_reason,
            "permissions": permissions
        }

    async def create_channel(self, name: str, topic: str = "", type: int = 0, parent_id: Optional[str] = None, nsfw: bool = False, slowmode_delay: int = 0, position: Optional[int] = None) -> str:
        """
        Creates a new channel in the target Fluxer community.
        Returns the new Fluxer channel ID.
        """
        assert self.client is not None
        
        logger.debug(f"Fluxer: Creating channel {name} (type {type}) with topic='{topic}', nsfw={nsfw}, slowmode={slowmode_delay}, position={position}")
        
        guild_channel = await self.client.create_guild_channel(
            guild_id=self.community_id,
            name=name,
            type=type,
            topic=topic or None,
            parent_id=parent_id,
            nsfw=nsfw,
            rate_limit_per_user=slowmode_delay,
            position=position
        )
        self._channels_cache = None
        return str(guild_channel["id"])

    async def modify_channel(self, channel_id: str, parent_id: Optional[str] = None, name: Optional[str] = None, topic: Optional[str] = None, nsfw: Optional[bool] = None, slowmode_delay: Optional[int] = None, position: Optional[int] = None) -> bool:
        """
        Updates channel properties.
        """
        assert self.client is not None
        
        logger.debug(f"Fluxer: Modifying channel {channel_id}: name={name}, topic='{topic}', parent_id={parent_id}, nsfw={nsfw}, slowmode={slowmode_delay}, position={position}")
        
        try:
            await self.client.modify_channel(
                channel_id=channel_id,
                name=name,
                topic=topic,
                parent_id=parent_id,
                nsfw=nsfw,
                rate_limit_per_user=slowmode_delay,
                position=position
            )
        except Forbidden as e:
            if getattr(e, 'code', None) == "NSFW_CONTENT_AGE_RESTRICTED":
                logger.warning(f"Fluxer: Could not update certain properties (likely NSFW) on channel {channel_id}: {e.message}")
                return False
            raise
        return True

    async def move_channel(self, channel_id: str, parent_id: Optional[str]) -> bool:
        """
        Backward compatibility for moving a channel to a category.
        """
        return await self.modify_channel(channel_id, parent_id=parent_id)

    async def get_channels(self) -> List[Dict[str, Any]]:
        """Returns all channels in the community."""
        if self._channels_cache is not None:
            return self._channels_cache
        assert self.client is not None
        self._channels_cache = await self.client.get_guild_channels(self.community_id)
        return self._channels_cache

    async def send_message(self, channel_id: str, author_name: str, content: str, timestamp: int, author_avatar_url: Optional[str] = None, files: Optional[List[Dict[str, Any]]] = None, reply_to_message_id: Optional[str] = None, is_forwarded: bool = False, embeds: Optional[List[Dict[str, Any]]] = None, _limit_retry: bool = False) -> Optional[str]:
        """
        Sends a message to the target channel.
        Uses a webhook to mimic the original author if possible.
        Returns the ID of the sent message if available.
        """
        assert self.client is not None
        self.last_rejection = None
        # Files over Fluxer's per-file limit can never be uploaded: leave them out and say so in the message.
        if files:
            too_big = [f for f in files if len(f["data"]) > self.max_file_bytes]
            if too_big:
                files = [f for f in files if len(f["data"]) <= self.max_file_bytes] or None
                content = ((content + "\n") if content else "") + "\n".join(
                    f"-# ⚠ not migrated, larger than Fluxer's {self.max_file_bytes / 1048576:.0f} MB file limit: "
                    f"`{f['filename']}` ({len(f['data']) / 1048576:.1f} MB)" for f in too_big)
        logger.debug(f"Fluxer: send_message called for channel {channel_id}, author='{author_name}', content_len={len(content) if content else 0}, files={len(files) if files else 0}, is_forwarded={is_forwarded}")
        
        # Ensure we are ready before sending (wait a bit if needed)
        if not self._ready_event.is_set():
            logger.debug(f"Fluxer: Bot not ready, waiting...")
            try:
                await asyncio.wait_for(self._ready_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning(f"Fluxer: Timeout waiting for bot readiness.")
                pass

        # Use webhook for avatar/username spoofing
        logger.debug(f"Fluxer: Resolving webhook for channel {channel_id}...")
        webhook = await self._get_or_create_webhook(channel_id)
        logger.debug(f"Fluxer: Webhook resolved: {webhook.id if webhook else 'None'}")
        
        # Prepare content with subtext timestamp
        # -# is Fluxer/Discord's subtext markdown: small, muted grey text
        prefix = f"-# <t:{timestamp}:D>\n"
        if is_forwarded:
            prefix += "-# ⮫*forwarded*\n"
            
        display_content = content
        if is_forwarded and content:
            display_content = f">>> {content}"
            
        final_content = prefix + display_content if display_content else prefix
        logger.debug(f"Fluxer: Prepared final_content (len {len(final_content)}): {final_content!r}")

        # Normalize embeds (ensure they are dicts, handling fluxer.Embed objects or dicts)
        normalized_embeds = None
        if embeds:
            normalized_embeds = []
            for e in embeds:
                d = e.to_dict() if hasattr(e, "to_dict") else e
                if not isinstance(d, dict):
                    continue
                
                # Heuristic: Skip redundant link previews to avoid "Invalid Embed" errors or duplication.
                # If an embed has a URL that is already in the message content, and no complex fields, skip it.
                if content and d.get("url") and str(d.get("url")) in content:
                    if not d.get("fields") and not d.get("description") and not d.get("title"):
                        logger.debug(f"Fluxer: Skipping redundant link preview embed for {d.get('url')}")
                        continue
                
                normalized_embeds.append(d)
        if not normalized_embeds: normalized_embeds = None

        def _build_files():
            # Rebuilt per attempt: BytesIO streams are consumed by a failed upload.
            if not files:
                return None
            return [File(io.BytesIO(f["data"]), filename=f["filename"]) for f in files]

        # Replies: Fluxer's execute-webhook endpoint accepts `message_reference` (native reply that keeps the
        # webhook's username/avatar), but fluxer.py's Webhook.send() doesn't expose it, so we call the route directly.
        # If Fluxer rejects the reference (e.g. the target no longer exists) we retry once without it.
        can_ref_via_webhook = bool(webhook and reply_to_message_id and hasattr(self.client, "_route"))
        use_reference = True

        # Files are stored first (presigned upload); the message itself is then a small JSON request that references
        # them. A failed upload can't have delivered anything, and a retried POST never re-sends the files.
        await self._pace()
        uploaded = None
        if files and webhook and self.presigned_uploads and hasattr(self.client, "_route"):
            try:
                uploaded = await self._upload_with_recovery(channel_id, files)
            except FileTooLarge as e:
                # Our idea of the limit was too generous for this instance/plan: learn it, drop the offenders, resend.
                if e.limit and e.limit < self.max_file_bytes and not _limit_retry:
                    logger.warning(f"Fluxer: per-file limit is {e.limit} bytes; leaving larger files out of messages")
                    self.max_file_bytes = e.limit
                    return await self.send_message(channel_id, author_name, content, timestamp, author_avatar_url, files,
                                                   reply_to_message_id, is_forwarded, embeds, _limit_retry=True)
                self.last_rejection = f"a file exceeds Fluxer's per-file limit ({e.limit or 'unknown'} bytes)"
                logger.error(f"Fluxer rejected message for channel {channel_id}: {self.last_rejection}")
                return None
            except PresignRejected as e:
                logger.warning(f"Fluxer: presigned upload refused for this message ({_redact(e)}); using the multipart form for it")
        use_uploaded = uploaded is not None

        async def _attempt() -> Optional[str]:
            fluxer_files = _build_files()
            if webhook and not (reply_to_message_id and not can_ref_via_webhook):
                username = f"{author_name} (discord)"
                ref = ({"message_id": str(reply_to_message_id), "channel_id": str(channel_id)}
                       if (reply_to_message_id and use_reference) else None)
                body = final_content
                if reply_to_message_id and not ref:      # reference was rejected: keep a visible trace of the reply
                    body = "-# ↳ *(in reply to a message that could not be linked)*\n" + final_content
                if (ref or use_uploaded) and hasattr(self.client, "_route"):
                    logger.debug(f"Fluxer: Sending {'reply' if ref else 'message'} via webhook {webhook.id} for user '{author_name}'")
                    return await self._webhook_execute(
                        webhook, content=body, username=username, avatar_url=author_avatar_url,
                        embeds=normalized_embeds, message_reference=ref,
                        attachments=uploaded if use_uploaded else None, files=None if use_uploaded else files)
                logger.debug(f"Fluxer: Sending message via webhook {webhook.id} for user '{author_name}'")
                msg = await webhook.send(
                    content=body,
                    username=username,
                    avatar_url=author_avatar_url,
                    files=fluxer_files,
                    embeds=normalized_embeds,
                    wait=True
                )
                return str(msg.id) if msg else None

            # No webhook available: use bot direct message (supports files and message_reference)
            # We add the author name to the prefix since bot name won't match
            bot_prefix = f"-# <t:{timestamp}:D>\n"
            if is_forwarded:
                bot_prefix += "-# ⮫*forwarded*\n"
            bot_prefix += f"-# · {author_name}\n"

            final_bot_content = bot_prefix + display_content if display_content else bot_prefix

            kwargs = {
                "channel_id": channel_id,
                "content": final_bot_content,
                "embeds": normalized_embeds
            }
            if files:
                # HTTPClient.send_message (unlike Webhook.send) wants plain {"filename", "data"} dicts and
                # indexes them as file["filename"] -- passing fluxer.File objects raises
                # "'File' object is not subscriptable".
                kwargs["files"] = [{"filename": f["filename"], "data": f["data"]} for f in files]
            if reply_to_message_id:
                kwargs["message_reference"] = {"message_id": str(reply_to_message_id), "channel_id": str(channel_id)}

            logger.debug(f"Fluxer: Sending message via bot for user '{author_name}'")
            msg_data = await self.client.send_message(**kwargs)
            return str(msg_data["id"]) if msg_data else None

        started = time.time()
        try:
            while True:
                # Uploaded files make the POST tiny, so it gets the normal timeout; otherwise it carries the payload.
                send_timeout = _SEND_TIMEOUT if use_uploaded else _upload_timeout(files)
                try:
                    return await self._send_with_recovery(_attempt, channel_id, send_timeout)
                except SendTimeout:
                    # The request may have gone through even though we never saw the answer. Look for it before
                    # anyone retries, otherwise a retry would post a duplicate.
                    if webhook:
                        found = await self._find_delivered(
                            channel_id, started, f"{author_name} (discord)",
                            {final_content, "-# ↳ *(in reply to a message that could not be linked)*\n" + final_content})
                        if found:
                            logger.warning(f"Fluxer: send to {channel_id} timed out but the message was delivered ({found}); not retrying")
                            return found
                    raise
                except MessageSendError:
                    raise
                except Exception as e:
                    status = getattr(e, "status", None)
                    rejected = isinstance(status, int) and 400 <= status < 500
                    if rejected and can_ref_via_webhook and use_reference:
                        logger.warning(f"Fluxer: reply reference rejected ({status}) for channel {channel_id}; "
                                       f"retrying without the reply link")
                        use_reference = False
                        continue
                    if status == 413 and files:
                        # Payload too large for the edge in front of the API (the multipart form carries every file
                        # byte): send the text without the files rather than losing the whole message.
                        logger.warning(f"Fluxer: 413 payload too large for channel {channel_id}; resending without "
                                       f"{len(files)} attachment(s)")
                        note = "-# ⚠ attachments not migrated (too large to upload): " + ", ".join(
                            f"`{f['filename']}`" for f in files)
                        display_content = ((display_content + "\n") if display_content else "") + note
                        final_content = prefix + display_content
                        files, uploaded, use_uploaded = None, None, False
                        continue
                    if rejected and use_uploaded:
                        logger.warning(f"Fluxer: message with presigned attachments rejected ({status}) for channel "
                                       f"{channel_id}; retrying with the multipart upload")
                        use_uploaded = False
                        continue
                    raise
        except MessageSendError:
            raise
        except Exception as e:
            # Permanent rejection (e.g. 400/403/413: bad embed, file too large). Retrying can't help,
            # so log and skip this one message. Returning None means "not sent, don't retry".
            err_msg = f"Fluxer rejected message for channel {channel_id}: {_redact(e)}"
            if hasattr(e, 'errors') and e.errors:
                err_msg += f" - Details: {e.errors}"
            logger.error(err_msg)
            self.last_rejection = _redact(e)
            return None

    # ── attachments: presigned upload phase ────────────────────────────────

    async def _upload_with_recovery(self, channel_id: str, files: List[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
        """Stores the files via presigned URLs. Returns attachment descriptors, or None if this instance doesn't offer
        presigned uploads (the caller then uses the multipart form). Transient failures are paused-and-retried;
        raises MessageSendError if they never clear (nothing has been posted at that point)."""
        import aiohttp
        if self._storage_session is None or self._storage_session.closed:
            self._storage_session = aiohttp.ClientSession()      # no Authorization header: storage URLs are presigned
        last = None
        for round_ in range(_UPLOAD_ROUNDS):
            if self._cancelled():
                raise MessageSendError("Cancelled during upload")
            try:
                uploader = PresignedUploader(self.client, self._storage_session, cancelled=self._cancelled)
                return await uploader.upload(channel_id, files)
            except PresignUnavailable as e:
                logger.warning(f"Fluxer: presigned uploads unavailable ({_redact(e)}); using the multipart upload instead")
                self.presigned_uploads = False
                return None
            except UploadError as e:
                last = e
                if self._cancelled():
                    raise MessageSendError("Cancelled during upload") from e
                delay = min(60.0, 5.0 * (2 ** round_))
                logger.warning(f"Fluxer: upload failed ({_redact(e)}); pausing {delay:.0f}s then retrying")
                if self.on_rate_limit:
                    try:
                        self.on_rate_limit(delay)
                    except Exception:
                        pass
                waited = 0.0
                while waited < delay:
                    if self._cancelled():
                        raise MessageSendError("Cancelled while waiting to retry the upload")
                    await asyncio.sleep(1.0)
                    waited += 1.0
        raise ServiceUnavailable(f"Upload to Fluxer storage failed after {_UPLOAD_ROUNDS} rounds: {_redact(last)}")

    def bot_username(self) -> Optional[str]:
        """The migration bot's own username (to recognise its messages), if known."""
        try:
            return self.bot.user.username
        except Exception:
            return None

    async def delete_message(self, channel_id: str, message_id: str) -> bool:
        """Deletes one message (used to remove a stale skip marker). False if it couldn't be."""
        try:
            await self.client.delete_message(channel_id, message_id)
            return True
        except Exception as e:
            logger.warning(f"Could not delete message {message_id} in {channel_id}: {_redact(e)}")
            return False

    async def check_health(self, channel_id: str) -> tuple:
        """Is the API answering normally right now? -> (healthy, detail). One light GET of the channel (and of the
        channel's webhook when we have one), no library retries and a 15s limit, so a hung API can't hang the check.
        An unexpected 4xx counts as healthy: the service is up, the problem is something else."""
        import aiohttp
        timeout = aiohttp.ClientTimeout(total=15)
        try:
            session = await self.client._ensure_session()
            routes = [self.client._route("GET", "/channels/{channel_id}", channel_id=channel_id)]
            hook = self._webhooks.get(str(channel_id))
            if hook is not None:
                routes.append(self.client._route("GET", "/webhooks/{webhook_id}/{token}", webhook_id=hook.id, token=hook.token))
            for route in routes:
                async with session.request(route.method, route.url, timeout=timeout) as r:
                    if r.status == 429:
                        return False, "rate limited (429)"
                    if r.status >= 500:
                        return False, f"server error {r.status}"
            return True, "ok"
        except (asyncio.TimeoutError, aiohttp.ClientError, OSError) as e:
            return False, f"{type(e).__name__}: {_redact(e)}"[:140]
        except Exception as e:                              # e.g. a client without the route helpers
            return True, f"health check skipped ({type(e).__name__})"

    async def verify_message(self, channel_id: str, message_id: str) -> Optional[bool]:
        """Reads a just-sent message back. True = it exists, False = Fluxer says it does not (404),
        None = couldn't tell (treat as fine: the API already returned its id)."""
        try:
            await self.client.get_message(channel_id, message_id)
            return True
        except Exception as e:
            return False if getattr(e, "status", None) == 404 else None

    async def _find_delivered(self, channel_id: str, since_ts: float, username: str, bodies: set) -> Optional[str]:
        """After a timed-out send: the ID of a recent message in the channel that is exactly what we tried to
        send (same webhook name and body), or None. Snowflakes embed their creation time, so `after` bounds it."""
        try:
            since = (int((since_ts - _DELIVERY_LOOKBACK_S) * 1000) - 1420070400000) << 22
            recent = await self.client.get_messages(channel_id, limit=50, after=str(max(since, 0)))
        except Exception as e:
            logger.debug(f"Fluxer: delivery check failed: {_redact(e)}")
            return None
        for m in recent or []:
            if (m.get("author") or {}).get("username") == username and m.get("content") in bodies:
                return str(m["id"])
        return None

    async def _webhook_execute(self, webhook, *, content, username, avatar_url, embeds, message_reference=None,
                               attachments=None, files=None) -> Optional[str]:
        """POST /webhooks/{id}/{token} directly (fluxer.py's Webhook.send can't carry a reply reference or presigned
        attachments). `attachments` = descriptors from PresignedUploader (plain JSON); `files` = legacy multipart."""
        import aiohttp
        route = self.client._route("POST", "/webhooks/{webhook_id}/{token}", webhook_id=webhook.id, token=webhook.token)
        payload: Dict[str, Any] = {"content": content, "username": username}
        if message_reference:
            payload["message_reference"] = message_reference
        if avatar_url:
            payload["avatar_url"] = avatar_url
        if embeds:
            payload["embeds"] = embeds
        params = {"wait": "true"}
        if attachments:
            payload["attachments"] = attachments
            res = await self.client.request(route, json=payload, params=params)
        elif files:
            form = aiohttp.FormData()
            payload["attachments"] = [{"id": i, "filename": f["filename"]} for i, f in enumerate(files)]
            form.add_field("payload_json", json.dumps(payload), content_type="application/json")
            for i, f in enumerate(files):
                form.add_field(f"files[{i}]", f["data"], filename=f["filename"])
            res = await self.client.request(route, data=form, params=params)
        else:
            res = await self.client.request(route, json=payload, params=params)
        return str(res["id"]) if res else None

    # ── rate-limit handling ────────────────────────────────────────────────

    async def _pace(self):
        """Optional self-imposed rate cap (min_send_interval seconds between sends), abortable on cancel/deadline."""
        if self.min_send_interval > 0:
            while True:
                wait = self._last_send_at + self.min_send_interval - time.monotonic()
                if wait <= 0:
                    break
                if self._cancelled():
                    raise MessageSendError("Cancelled while pacing")
                await asyncio.sleep(min(wait, 1.0))
        self._last_send_at = time.monotonic()

    def _note_rate_limit(self, seconds: float):
        """Called (via log handler) whenever the HTTP client reports a 429 / global limit."""
        self.rate_limited_until = max(self.rate_limited_until, time.monotonic() + seconds)
        logger.warning(f"Fluxer: rate limited, waiting {seconds:.1f}s")
        if self.on_rate_limit:
            try:
                self.on_rate_limit(seconds)
            except Exception:
                pass

    def _rate_limit_remaining(self) -> float:
        return max(0.0, self.rate_limited_until - time.monotonic())

    def _cancelled(self) -> bool:
        return bool(self.stop_check and self.stop_check())

    async def _await_with_ratelimit(self, coro, timeout: float = _SEND_TIMEOUT) -> Any:
        """Awaits a send, but only enforces the timeout while we are NOT waiting on a rate limit.
        (A plain wait_for would cancel a request that is merely sleeping through a 429 pause.)"""
        task = asyncio.ensure_future(coro)
        try:
            while True:
                done, _ = await asyncio.wait({task}, timeout=timeout)
                if done:
                    return task.result()
                if self._cancelled():
                    raise MessageSendError("Cancelled while sending")
                if self._rate_limit_remaining() > 0:
                    continue  # paused by the rate limiter, keep waiting
                raise SendTimeout(f"Send timed out after {timeout:.0f}s (delivery unknown)")
        finally:
            if not task.done():
                task.cancel()

    async def _send_with_recovery(self, attempt_fn, channel_id: str, timeout: float = _SEND_TIMEOUT) -> str:
        """Runs a send. Rate limits/outages the HTTP client can't ride out are waited out here and the
        SAME message is retried. Returns the new message id, or raises MessageSendError.
        Permanent API rejections propagate as their original exception."""
        for round_ in range(_MAX_RECOVERY_ROUNDS):
            if self._cancelled():
                raise MessageSendError("Cancelled")
            try:
                msg_id = await self._await_with_ratelimit(attempt_fn(), timeout)
                if msg_id:
                    return msg_id
                raise MessageSendError(f"Fluxer returned no message id for channel {channel_id}")
            except MessageSendError:
                raise
            except RuntimeError as e:
                # fluxer.http raises RuntimeError("Failed after N attempts") once its retries are spent
                if "Failed after" not in str(e):
                    raise
                delay = max(self._rate_limit_remaining(), min(60.0, 5.0 * (2 ** round_)))
                logger.warning(f"Fluxer: send failed after client retries ({_redact(e)}); pausing {delay:.0f}s then retrying same message")
                if self.on_rate_limit:
                    try:
                        self.on_rate_limit(delay)
                    except Exception:
                        pass
                waited = 0.0
                while waited < delay:
                    if self._cancelled():
                        raise MessageSendError("Cancelled while waiting on rate limit")
                    await asyncio.sleep(1.0)
                    waited += 1.0
            except Exception as e:
                status = getattr(e, "status", None)
                if isinstance(status, int) and 400 <= status < 500:
                    raise  # permanent rejection
                import aiohttp
                if isinstance(e, (aiohttp.ClientError, asyncio.TimeoutError, OSError)) or (isinstance(status, int) and status >= 500):
                    raise ServiceUnavailable(f"Send to channel {channel_id} failed ({type(e).__name__}): {_redact(e)}") from e
                raise MessageSendError(f"Send failed for channel {channel_id}: {_redact(e)}") from e
        raise ServiceUnavailable(f"Gave up sending to channel {channel_id} after {_MAX_RECOVERY_ROUNDS} rate-limit/outage retries")

    async def send_marker(self, channel_id: str, content: str, files: list[dict] | None = None, reply_to_message_id: Optional[str] = None) -> Optional[str]:
        """
        Sends a simple marker message (e.g., thread start/end) using the bot directly.
        """
        assert self.client is not None
        
        # HTTPClient.send_message takes plain {"filename", "data"} dicts, not fluxer.File objects
        raw_files = None
        if files:
            raw_files = [f.to_dict() if hasattr(f, "to_dict") else {"filename": f["filename"], "data": f["data"]} for f in files]
        
        message_reference = None
        if reply_to_message_id:
            message_reference = {"message_id": str(reply_to_message_id), "channel_id": str(channel_id)}

        try:
            kwargs = {
                "channel_id": channel_id,
                "content": content
            }
            if raw_files:
                kwargs["files"] = raw_files
            if message_reference:
                kwargs["message_reference"] = message_reference

            msg_data = await self.client.send_message(**kwargs)
            return str(msg_data["id"]) if msg_data else None
        except Exception as e:
            print(f"Failed to send marker: {e}")
            logger.error(f"Failed to send marker: {e}")
            return None

    async def create_role(self, name: str, color: int, hoist: bool, mentionable: bool, permissions: int, position: Optional[int] = None) -> str:
        """
        Creates a new role in the Fluxer community.
        Returns the new Fluxer role ID.
        """
        assert self.client is not None
        
        try:
            role = await self.client.create_guild_role(
                guild_id=self.community_id,
                name=name,
                color=color,
                hoist=hoist,
                mentionable=mentionable,
                permissions=permissions,
                position=position
            )
            return str(role["id"])
        except Exception as e:
            print(f"Failed to copy role {name}: {e}")
            logger.error(f"Failed to copy role {name}: {e}")
            return ""

    async def create_emoji(self, name: str, image_bytes: bytes) -> str:
        """
        Creates a custom emoji in the Fluxer community.
        """
        assert self.client is not None
        
        try:
            emoji = await self.client.create_guild_emoji(
                guild_id=self.community_id,
                name=name,
                image=image_bytes
            )
            return str(emoji["id"])
        except Exception as e:
            logger.error(f"Failed to copy emoji '{name}': {e}", exc_info=True)
            return ""

    async def create_sticker(self, name: str, image_bytes: bytes) -> str:
        """
        Creates a custom sticker in the Fluxer community.
        """
        assert self.client is not None
        
        try:
            sticker = await self.client.create_guild_sticker(
                guild_id=self.community_id,
                name=name,
                image=image_bytes
            )
            return str(sticker["id"])
        except Exception as e:
            logger.error(f"Failed to copy sticker '{name}': {e}", exc_info=True)
            return ""

    async def update_guild_metadata(self, name: Optional[str] = None, icon: Optional[bytes] = None, banner: Optional[bytes] = None) -> None:
        """
        Updates the Fluxer community name, icon, and banner.
        """
        assert self.client is not None
        
        kwargs = {}
        if banner:
            import base64
            image_data = base64.b64encode(banner).decode("ascii")
            if banner.startswith(b"\x89PNG"):
                content_type = "image/png"
            elif banner.startswith(b"\xff\xd8\xff"):
                content_type = "image/jpeg"
            elif banner.startswith(b"GIF89a") or banner.startswith(b"GIF87a"):
                content_type = "image/gif"
            else:
                content_type = "image/png"
            kwargs["banner"] = f"data:{content_type};base64,{image_data}"

        try:
            await self.client.modify_guild(
                guild_id=self.community_id,
                name=name,
                icon=icon,
                **kwargs
            )
        except Exception as e:
            print(f"Failed to update community metadata: {e}")
            logger.error(f"Failed to update community metadata: {e}")

    async def remove_community_logo_and_banner(self) -> dict:
        """
        Removes the community logo (icon) and banner.
        Fetches the current guild state first so it can report whether each
        field was actually set (REMOVED) or already empty (SKIP).

        Correct API calls per Fluxer contract:
            await http.modify_guild(guild_id, icon=None)
            await http.modify_guild(guild_id, banner=None)

        Returns:
            {"icon": "REMOVED"|"SKIP", "banner": "REMOVED"|"SKIP"}
        """
        assert self.client is not None

        # 1. Check current state
        guild = await self.client.get_guild(self.community_id)
        has_icon = bool(guild.get("icon"))
        has_banner = bool(guild.get("banner"))

        # 2. Remove icon if set
        if has_icon:
            try:
                await self.client.modify_guild(
                    guild_id=self.community_id,
                    icon=None
                )
            except Exception as e:
                print(f"Failed to remove community icon: {e}")
                logger.error(f"Failed to remove community icon: {e}")

        # 3. Remove banner if set
        if has_banner:
            try:
                await self.client.modify_guild(
                    guild_id=self.community_id,
                    banner=None
                )
            except Exception as e:
                print(f"Failed to remove community banner: {e}")
                logger.error(f"Failed to remove community banner: {e}")

        return {
            "icon": "REMOVED" if has_icon else "SKIP",
            "banner": "REMOVED" if has_banner else "SKIP",
        }

    async def delete_all_channels(self, progress_callback=None) -> int:
        """
        Deletes all channels and categories in the Fluxer community.
        Returns the count of deleted channels.
        """
        assert self.client is not None
        channels = await self.client.get_guild_channels(self.community_id)
        total = len(channels)
        deleted = 0
        # Delete non-category channels first, then categories
        sorted_channels = sorted(channels, key=lambda c: 0 if c.get("type") == 4 else -1)
        for ch in sorted_channels:
            name = str(ch.get("name", "")).lower()
            if name in ["reaper-logs", "reaper_logs"]:
                logger.info(f"Danger Zone: Skipping deletion of audit channel {name}")
                total -= 1
                continue

            try:
                await self.client.delete_channel(ch["id"])
                deleted += 1
                if progress_callback:
                    await progress_callback(ch.get("name", "Unknown"), deleted, total)
            except Exception as e:
                print(f"Failed to delete channel {ch.get('name')}: {e}")
                logger.error(f"Failed to delete channel {ch.get('name')}: {e}")
        return deleted

    async def reset_channel_permissions(self, progress_callback=None) -> int:
        """
        Resets all permission overwrites on every channel and category.
        Returns the count of channels processed.
        """
        assert self.client is not None
        channels = await self.client.get_guild_channels(self.community_id)
        total = len(channels)
        processed = 0
        for ch in channels:
            name = str(ch.get("name", "")).lower()
            if name in ["reaper-logs", "reaper_logs"]:
                logger.info(f"Danger Zone: Skipping permission reset for audit channel {name}")
                total -= 1
                continue

            try:
                # Fetch existing overwrites and delete each one
                overwrites = ch.get("permission_overwrites", [])
                for ow in overwrites:
                    try:
                        await self.client.request(
                            self.client._route(
                                "DELETE", 
                                "/channels/{channel_id}/permissions/{overwrite_id}",
                                channel_id=ch["id"],
                                overwrite_id=ow["id"]
                            )
                        )
                    except Exception as e:
                        print(f"Failed to delete overwrite {ow['id']} for channel {ch['id']}: {e}")
                        logger.error(f"Failed to delete overwrite {ow['id']} for channel {ch['id']}: {e}")
                processed += 1
                if progress_callback:
                    await progress_callback(ch.get("name", "Unknown"), processed, total)
            except Exception as e:
                print(f"Failed to reset permissions for channel {ch.get('name')}: {e}")
                logger.error(f"Failed to reset permissions for channel {ch.get('name')}: {e}")
        return processed

    async def set_channel_permission(self, channel_id: str, overwrite_id: str, allow: int, deny: int, is_role: bool = True):
        """Sets a permission overwrite for a channel or category."""
        assert self.client is not None
        try:
            target_channel_id = int(channel_id)
            target_overwrite_id = int(overwrite_id)
        except (ValueError, TypeError):
            logger.warning(f"Fluxer: Skipping permission set for non-numeric ID: channel={channel_id}, overwrite={overwrite_id}")
            return

        try:
            await self.client.edit_channel_permissions(
                channel_id=target_channel_id,
                overwrite_id=target_overwrite_id,
                allow=allow,
                deny=deny,
                type=0 if is_role else 1
            )
        except Exception as e:
            print(f"Failed to set permission on channel {channel_id} for overwrite {overwrite_id}: {e}")
            logger.error(f"Failed to set permission on channel {channel_id} for overwrite {overwrite_id}: {e}")


    async def delete_all_roles(self, progress_callback=None) -> int:
        """
        Deletes all non-managed, non-default roles in the Fluxer community,
        while safely skipping the bot's own managed role.
        Returns the count of deleted roles.
        """
        assert self.client is not None
        
        # Fetch the bot's user ID so we can skip its managed role
        bot_user_id = None
        try:
            if self.bot and self.bot.user:
                bot_user_id = str(self.bot.user.id)
            else:
                me = await self.client.get_current_user()
                if me:
                    bot_user_id = str(me.get("id"))
        except Exception:
            pass

        roles = await self.client.get_guild_roles(self.community_id)
        deletable = []
        for r in roles:
            # Skip @everyone (position 0) and managed roles (e.g. bot roles)
            if r.get("managed") or r.get("name") == "@everyone":
                continue
            deletable.append(r)

        total = len(deletable)
        deleted = 0
        for role in deletable:
            try:
                await self.client.delete_guild_role(self.community_id, role["id"])
                deleted += 1
                if progress_callback:
                    await progress_callback(role.get("name", "Unknown"), deleted, total)
            except Exception as e:
                print(f"Failed to delete role {role.get('name')}: {e}")
                logger.error(f"Failed to delete role {role.get('name')}: {e}")
        return deleted

    async def delete_all_emojis_and_stickers(self, progress_callback=None) -> dict:
        """
        Deletes all custom emojis and stickers in the Fluxer community.
        Returns {"emojis": int, "stickers": int} with independent counts.
        """
        assert self.client is not None
        emoji_deleted = 0
        sticker_deleted = 0

        # Delete emojis
        try:
            emojis = await self.client.get_guild_emojis(self.community_id)
            emoji_total = len(emojis)
            for emoji in emojis:
                try:
                    await self.client.delete_guild_emoji(self.community_id, emoji["id"])
                    emoji_deleted += 1
                    if progress_callback:
                        await progress_callback(emoji.get("name", "Unknown"), "Emoji", emoji_deleted, emoji_total)
                except Exception as e:
                    print(f"Failed to delete emoji {emoji.get('name')}: {e}")
                    logger.error(f"Failed to delete emoji {emoji.get('name')}: {e}")
        except Exception as e:
            print(f"Failed to fetch emojis: {e}")
            logger.error(f"Failed to fetch emojis: {e}")

        # Delete stickers
        try:
            stickers = await self.client.get_guild_stickers(self.community_id)
            sticker_total = len(stickers)
            for sticker in stickers:
                try:
                    await self.client.delete_guild_sticker(self.community_id, sticker["id"])
                    sticker_deleted += 1
                    if progress_callback:
                        await progress_callback(sticker.get("name", "Unknown"), "Sticker", sticker_deleted, sticker_total)
                except Exception as e:
                    print(f"Failed to delete sticker {sticker.get('name')}: {e}")
                    logger.error(f"Failed to delete sticker {sticker.get('name')}: {e}")
        except Exception as e:
            print(f"Failed to fetch stickers: {e}")
            logger.error(f"Failed to fetch stickers: {e}")

        return {"emojis": emoji_deleted, "stickers": sticker_deleted}


    async def close(self):
        """Cleanly close connection and stop bot task."""
        bot = self.bot
        self.bot = None # Atomic clear
        self._channels_cache = None
        if self._storage_session is not None:
            try:
                await self._storage_session.close()
            except Exception:
                pass
            self._storage_session = None
        self._webhooks.clear()

        if bot:
            try:
                await bot.close()
            except Exception as e:
                logger.debug(f"Error closing Fluxer bot: {e}")
                
        if self._bot_task:
            task = self._bot_task
            self._bot_task = None
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._ready_event.clear()


