import asyncio
import time
import logging
import re
import json
import io
from typing import Callable, Awaitable, Dict, Any, List
from pathlib import Path

try:
    from lottie.objects import Animation
    from lottie.exporters.gif import export_gif
    HAS_LOTTIE = True
except ImportError:
    HAS_LOTTIE = False

from src.core.base import MigrationContext
from src.fluxer.writer import MessageSendError, ServiceUnavailable
from src.core.media_links import attach_link_media, embed_refs_link
from src.core.utils import resolve_discord_links

logger = logging.getLogger(__name__)

def clean_mentions(content: str, guild, user_mentions=None, role_mentions=None, channel_mentions=None, emoji_map=None, channel_map=None, state=None, target_server_id=None, channel_names=None, anonymize_users=False) -> str:
    if content is None:
        return ""
    if not content or not guild:
        return content
        
    def replace_user(match):
        uid = int(match.group(1))
        if anonymize_users and state:
            alias = state.get_user_alias(str(uid))
            return f"`@{alias}`" if alias else "`@Anonymized User`"
        
        # 1. Try provided guild
        member = guild.get_member(uid)
        if member:
            return f"`@{member.display_name}`"
        
        # 2. Try provided user_mentions
        if user_mentions:
            m = next((u for u in user_mentions if u.id == uid), None)
            if m:
                return f"`@{m.display_name}`"
        
        # 3. Try global cache via guild.client
        if hasattr(guild, 'client'):
            user = guild.client.get_user(uid)
            if user:
                return f"`@{user.name}`"
        return "`@Unknown User`"
        
    def replace_role(match):
        rid = int(match.group(1))
        # 0. Try native mapping first
        if state:
            target_role_id = state.get_target_role_id(str(rid))
            if target_role_id:
                return f"<@&{target_role_id}>"

        # 1. Try provided guild cache/list
        role = guild.get_role(rid) or next((r for r in guild.roles if r.id == rid), None)
        # 2. Try message's role_mentions
        if not role and role_mentions:
            role = next((r for r in role_mentions if r.id == rid), None)
        
        # 3. Try all guilds the client is aware of (fallback for cache issues)
        if not role and hasattr(guild, 'client'):
            for g in guild.client.guilds:
                role = g.get_role(rid)
                if role: break
        
        if role and role.name:
            return f"`@{role.name}`"
            
        return f"`@Unknown Role`"
        
    def replace_channel(match):
        cid = int(match.group(1))
        
        # 1. Check if channel is mapped in state
        if channel_map and cid in channel_map:
            return f"<#{channel_map[cid]}>"
            
        # 2. Try to resolve channel name from pre-fetched names
        name = None
        if channel_names and str(cid) in channel_names:
            name = channel_names[str(cid)]
        
        # 3. Try live lookup (fallback)
        if not name:
            try:
                channel = guild.get_channel(cid) or guild.get_thread(cid)
            except Exception:
                channel = None
            if not channel and channel_mentions:
                channel = next((c for c in channel_mentions if c.id == cid), None)
            if channel:
                name = channel.name

        if name:
            return f"`#{name}`"
            
        return f"<#{cid}>"

    def replace_emoji(match):
        animated = match.group(1) == "a"
        name = match.group(2)
        eid = int(match.group(3))
        
        if emoji_map and eid in emoji_map:
            target_eid = emoji_map[eid]
            prefix = "a" if animated else ""
            return f"<{prefix}:{name}:{target_eid}>"
        
        return f":{name}:"

    content = re.sub(r'<@!?([0-9]+)>', replace_user, content)
    content = re.sub(r'<@&([0-9]+)>', replace_role, content)
    content = re.sub(r'<#([0-9]+)>', replace_channel, content)
    content = re.sub(r'<(a?):([^:]+):([0-9]+)>', replace_emoji, content)
    content = content.replace("@everyone", "`@everyone`").replace("@here", "`@here`")

    # Resolve Discord Links
    if state and target_server_id:
        content = resolve_discord_links(content, state, "fluxer", target_server_id)

    return content


def format_reply_fallback(ref_text: str, ref_name: str, has_attachments: bool = False, max_len: int = 160) -> str:
    """Quote block + 'replying to' header, used when the replied-to message can't be linked natively
    (it was never migrated, was skipped, or the reference was rejected)."""
    snippet = " ".join((ref_text or "").split())                # collapse newlines / runs of whitespace
    snippet = snippet.replace("@everyone", "@\u200beveryone").replace("@here", "@\u200bhere")   # never ping from a quote
    if not snippet:
        snippet = "[attachment]" if has_attachments else "[message]"
    if len(snippet) > max_len:
        snippet = snippet[:max_len - 1].rstrip() + "…"
    return f"> {snippet}\n-# ↳ replying to `@{ref_name}`\n"


async def get_channel_threads(reader: Any, channel_id: int) -> List[Any]:
    """Helper to fetch all threads (active and archived) for a channel from Live or Backup."""
    threads = []
    
    # 1. From Backup (BackupReader has 'db' attribute)
    if hasattr(reader, 'db') and hasattr(reader, 'threads'):
        for t in reader.threads:
            if t.parent_id == channel_id:
                threads.append(t)
        return threads

    # 2. From live Discord
    if hasattr(reader, 'guild') and reader.guild:
        try:
            # Guild-wide active threads
            if hasattr(reader.guild, 'active_threads'):
                for t in reader.guild.active_threads:
                    if t.parent_id == channel_id:
                        threads.append(t)
            
            # Archived threads for this specific channel
            channel = await reader.get_channel(channel_id)
            if hasattr(channel, 'archived_threads'):
                # discord.py method
                async for t in channel.archived_threads(limit=None):
                    threads.append(t)
        except Exception as e:
            logger.debug(f"Could not fetch live threads for {channel_id}: {e}")
            
    return threads



async def _process_and_send_message(
    context: MigrationContext,
    msg: Any,
    target_channel_id: str,
    stats: Dict[str, Any],
    thread_id: str | None = None,
    parent_target_id: str | None = None,
    thread_name: str | None = None,
    processed_threads: set | None = None,
    degrade: int = 0
) -> str | None:
    """
    Internal helper to process a single Discord message (mentions, attachments, stickers)
    and send it to the Fluxer platform.

    degrade (set by _process_with_retries for a message that keeps failing while Fluxer looks healthy):
    1 = no custom embeds and link unfurling suppressed; 2 = additionally every link is shown as plain code text.
    """
    # 1. Formatting
    content = msg.content or ""
    
    # Check for forwarded flag
    is_forwarded = False
    if hasattr(msg.flags, 'forwarded'):
        is_forwarded = msg.flags.forwarded
        
    # Always ensure alias is created/retrieved to populate user_alias table
    alias = context.state.get_user_alias(str(msg.author.id))
    anonymize_users = context.config.anonymize_users if hasattr(context, 'config') else False
    
    # Process Stickers
    files = []
    if hasattr(msg, 'stickers') and msg.stickers:
        for s in msg.stickers:
            try:
                sticker_data = await context.discord_reader.download_sticker(s)
                if not sticker_data: continue
                
                format_val = getattr(s, 'format', 'png')
                if hasattr(format_val, 'name'):
                    ext = format_val.name.lower()
                elif isinstance(format_val, int):
                    format_map = {1: 'png', 2: 'apng', 3: 'lottie', 4: 'gif'}
                    ext = format_map.get(format_val, 'png')
                else:
                    ext = str(format_val).lower()
                
                # Conversion logic (Simplified for unification)
                if ext == 'lottie' and HAS_LOTTIE:
                    try:
                        lottie_data = json.loads(sticker_data)
                        def _convert_lottie(data):
                            anim = Animation.load(data)
                            buf = io.BytesIO()
                            export_gif(anim, buf)
                            buf.seek(0)
                            return buf
                        gif_buf = await asyncio.to_thread(_convert_lottie, lottie_data)
                        from PIL import Image
                        def _convert_gif_to_webp(buf):
                            img = Image.open(buf)
                            w_buf = io.BytesIO()
                            if getattr(img, 'n_frames', 1) > 1:
                                img.save(w_buf, format='WEBP', save_all=True, loop=0, quality=80)
                            else:
                                img.save(w_buf, format='WEBP', quality=80)
                            return w_buf.getvalue()
                        sticker_data = await asyncio.to_thread(_convert_gif_to_webp, gif_buf)
                        ext = 'webp'
                    except Exception: ext = 'json'
                elif ext in ('apng', 'gif'):
                    try:
                        from PIL import Image
                        def _process_animated_sticker(data):
                            img = Image.open(io.BytesIO(data))
                            webp_buf = io.BytesIO()
                            if getattr(img, 'n_frames', 1) > 1:
                                img.save(webp_buf, format='WEBP', save_all=True, loop=0, quality=80)
                            else:
                                img.save(webp_buf, format='WEBP', quality=80)
                            return webp_buf.getvalue()
                        sticker_data = await asyncio.to_thread(_process_animated_sticker, sticker_data)
                        ext = 'webp'
                    except Exception: pass
                
                files.append({"filename": f"sticker_{s.name}_{s.id}.{ext}", "data": sticker_data})
                stats["attachments"] += 1
            except Exception as e:
                logger.error(f"Failed to download sticker {getattr(s, 'name', 'unknown')}: {e}")

    # Process Attachments
    attachments_to_process = list(msg.attachments)
    if is_forwarded and hasattr(msg, 'message_snapshots') and msg.message_snapshots:
        snapshot = msg.message_snapshots[0]
        if not content:
            content = snapshot.content
        attachments_to_process.extend(snapshot.attachments)

    for att in attachments_to_process:
        try:
            att_data = await context.discord_reader.download_attachment(att)
            files.append({"filename": att.filename, "data": att_data})
            stats["attachments"] += 1
        except Exception as e:
            logger.error(f"Failed to download attachment {att.filename}: {e}")

    # Clean Mentions
    content = clean_mentions(
        content=content,
        guild=context.discord_reader.guild,
        user_mentions=msg.mentions,
        role_mentions=msg.role_mentions,
        channel_mentions=msg.channel_mentions,
        emoji_map=context.state.emoji_map,
        channel_map=context.state.channel_map,
        state=context.state,
        target_server_id=context.fluxer_writer.community_id,
        channel_names=context.channel_names if hasattr(context, 'channel_names') else None,
        anonymize_users=anonymize_users
    )

    if not content and not files:
        return None

    # Reply Resolution
    reply_to_fluxer_id = None
    if msg.reference and msg.reference.message_id:
        reply_to_fluxer_id = context.state.get_fluxer_message_id(target_channel_id, str(msg.reference.message_id))
        
        # Fallback author tagging for replies if mapping not found
        if not reply_to_fluxer_id:
            try:
                source_ref_msg = await context.discord_reader.get_message(msg.channel.id, msg.reference.message_id)
                if source_ref_msg and source_ref_msg.author:
                    ref_name = context.state.get_user_alias(str(source_ref_msg.author.id)) if anonymize_users else source_ref_msg.author.display_name
                    try:
                        ref_text = clean_mentions(
                            source_ref_msg.content or "", context.discord_reader.guild,
                            source_ref_msg.mentions, source_ref_msg.role_mentions, source_ref_msg.channel_mentions,
                            context.state.emoji_map, context.state.channel_map, state=context.state,
                            target_server_id=context.fluxer_writer.community_id,
                            channel_names=context.channel_names if hasattr(context, 'channel_names') else None,
                            anonymize_users=anonymize_users)
                    except Exception:
                        ref_text = source_ref_msg.content or ""
                    content = format_reply_fallback(ref_text, ref_name, bool(source_ref_msg.attachments)) + content
                else:
                    tgt_reply = context.state.get_target_message_id(target_channel_id, msg.reference.message_id)
                    if tgt_reply: content = f"[Reply to {tgt_reply}]\n{content}"
            except Exception: pass

    # Thread logic
    if not reply_to_fluxer_id and parent_target_id and stats["messages"] == 0:
        reply_to_fluxer_id = parent_target_id
    if thread_name and stats["messages"] == 0:
        content = f"> <<< THREAD: **{thread_name}** >>>\n{content}"

    # Send Message
    if anonymize_users:
        author_name = alias or "Anonymized User"
        author_avatar_url = None
    else:
        author_name = msg.author.display_name
        author_avatar_url = msg.author.avatar.url if hasattr(msg.author, 'avatar') and msg.author.avatar else None

    # Discord CDN links pasted in the text (already refreshed + saved by the backup's media-link pass) become real
    # attachments; unresolved/dead links stay as text. Backup source only (needs the local media pool).
    send_content, send_files, send_embeds, link_files = content, list(files), msg.embeds, []
    r_db, r_root = getattr(context.discord_reader, "db", None), getattr(context.discord_reader, "backup_path", None)
    if r_db is not None and r_root is not None and hasattr(r_db, "get_link_media"):
        try:
            new_content, link_files, replaced = attach_link_media(content, r_db, r_root, max_files=max(0, 10 - len(files)))
            if link_files:
                send_content, send_files = new_content, files + link_files
                send_embeds = [e for e in msg.embeds if not embed_refs_link(e, replaced)]
        except Exception as e:
            logger.warning(f"Message {msg.id}: could not attach saved media links ({e}); leaving links as text")
            send_content, send_files, send_embeds, link_files = content, list(files), msg.embeds, []

    def _send(c, f, em):
        if degrade >= 2:
            c = defang_links(c)
        extra = {"suppress_embeds": True} if degrade >= 1 else {}
        return context.fluxer_writer.send_message(
            channel_id=target_channel_id,
            author_name=author_name,
            author_avatar_url=author_avatar_url,
            content=c,
            timestamp=int(msg.created_at.timestamp()),
            files=f if f else None,
            reply_to_message_id=reply_to_fluxer_id,
            is_forwarded=is_forwarded,
            embeds=None if degrade >= 1 else em,
            **extra
        )

    fluxer_msg_id = await _send(send_content, send_files, send_embeds)
    if not fluxer_msg_id and link_files:
        # Fluxer refused the message (e.g. an attachment over its size limit): retry with the original links as text
        logger.warning(f"Message {msg.id}: rejected with saved media attached; retrying with the links as text")
        link_files = []
        fluxer_msg_id = await _send(content, files, msg.embeds)
    if not fluxer_msg_id:
        # send_message returns None only for a permanent rejection (it already logged why). Don't let the message
        # vanish silently: post a marker, record it in skipped_messages, and move on.
        reason = getattr(context.fluxer_writer, "last_rejection", None) or "rejected by Fluxer"
        return await _skip_message(context, msg, target_channel_id, reason, 1, stats, thread_id, rejected=True)
    if fluxer_msg_id:
        files = files + link_files
        stats["attachments"] += len(link_files)
        if thread_id:
            context.state.set_thread_message_mapping(target_channel_id, thread_id, str(msg.id), fluxer_msg_id)
            context.state.update_thread_last_message_timestamp(target_channel_id, thread_id, str(msg.created_at))
            context.state.update_thread_last_message_id(target_channel_id, thread_id, str(msg.id))
            context.state.increment_thread_stats(target_channel_id, thread_id, messages=1, files=len(files) if files else 0)
        else:
            context.state.set_message_mapping(target_channel_id, str(msg.id), fluxer_msg_id)
            context.state.update_last_message_timestamp(target_channel_id, str(msg.created_at))
            context.state.update_last_message_id(target_channel_id, str(msg.id))
            context.state.increment_stats(target_channel_id, messages=1, files=len(files) if files else 0)

        stats["messages"] += 1
        stats["last_message_content"] = content
        stats["last_message_author"] = msg.author.display_name
        
    return fluxer_msg_id

async def _skip_message(context: MigrationContext, msg: Any, target_channel_id: str, reason: str, attempts: int,
                        stats: Dict[str, Any], thread_id: str | None = None, rejected: bool = False) -> str | None:
    """Gives up on a message that keeps failing: posts a visible marker, records the skip, advances progress."""
    anonymize = context.config.anonymize_users if hasattr(context, "config") else False
    try:
        author = (context.state.get_user_alias(str(msg.author.id)) if anonymize else msg.author.display_name) or "unknown"
    except Exception:
        author = "unknown"
    when = int(msg.created_at.timestamp())
    marker_id = None
    try:
        marker_id = await context.fluxer_writer.send_marker(
            channel_id=target_channel_id,
            content=f"⚠️ There was an error migrating message `{msg.id}` from **{author}** (<t:{when}:f>)"
                    f"{' (Fluxer rejected it)' if rejected else (f' after {attempts} attempts' if attempts > 1 else '')}"
                    f", skipping...")
    except Exception as e:
        logger.warning(f"Could not post the skip marker for message {msg.id}: {e}")
    context.state.record_skipped_message(msg.id, getattr(msg.channel, "id", ""), author, reason, attempts)
    # The message is handled (skipped): advance progress so a resume doesn't retry it, and map it to the marker
    # so replies / links to it still resolve to something.
    if thread_id:
        if marker_id:
            context.state.set_thread_message_mapping(target_channel_id, thread_id, str(msg.id), marker_id)
        context.state.update_thread_last_message_timestamp(target_channel_id, thread_id, str(msg.created_at))
        context.state.update_thread_last_message_id(target_channel_id, thread_id, str(msg.id))
    else:
        if marker_id:
            context.state.set_message_mapping(target_channel_id, str(msg.id), marker_id)
        context.state.update_last_message_timestamp(target_channel_id, str(msg.created_at))
        context.state.update_last_message_id(target_channel_id, str(msg.id))
    stats["skipped"] = stats.get("skipped", 0) + 1
    stats.setdefault("skipped_ids", []).append(str(msg.id))
    logger.error(f"Skipped message {msg.id} ({'rejected by Fluxer' if rejected else f'{attempts} failed attempts'}): {reason}")
    notice = getattr(context, "on_notice", None)
    if notice:
        notice(f"[bold yellow]Skipped message {msg.id} ({'rejected by Fluxer' if rejected else f'{attempts} failed attempts'}): {reason}[/bold yellow]")
    if hasattr(context, "notify"):
        context.notify(f"Skipped message `{msg.id}` from {author} ({'rejected by Fluxer' if rejected else f'{attempts} failed attempts'}): "
                       f"{str(reason)[:200]}", kind="warn", key="skip", cooldown=120)
    return marker_id


async def find_start_message(context: MigrationContext, message_id: int) -> Any:
    """Loads the backup message to start from (inclusive). Raises ValueError if it isn't in the backup."""
    wanted = int(message_id)
    async for m in context.discord_reader.fetch_global_message_history(after_id=wanted - 1):
        if m.id == wanted:
            return m
        break
    raise ValueError(f"Message {message_id} is not in the backup")


def _author_name_for(context: MigrationContext, msg: Any) -> str:
    """The name the message is posted under (before the ' (discord)' suffix), exactly as the send path does."""
    anonymize = context.config.anonymize_users if hasattr(context, "config") else False
    if anonymize:
        return context.state.get_user_alias(str(msg.author.id)) or "Anonymized User"
    return msg.author.display_name


async def _find_on_server(context: MigrationContext, index: Any, msg: Any, target_channel_id: str) -> str | None:
    """ID of a copy of `msg` already on the Fluxer channel (read from the server), or None."""
    try:
        return await index.find(target_channel_id, f"{_author_name_for(context, msg)} (discord)", int(msg.created_at.timestamp()))
    except Exception as e:
        raise ServiceUnavailable(f"Could not read channel {target_channel_id} from Fluxer to check for existing messages: {e}") from e


async def _drop_stale_marker(context: MigrationContext, index: Any, msg: Any, target_channel_id: str) -> None:
    """The message is now on the server for real: remove the bot's old 'error migrating ... skipping' marker for it."""
    marker = index.marker_for(target_channel_id, msg.id)
    if marker:
        if await context.fluxer_writer.delete_message(target_channel_id, marker):
            logger.info(f"Deleted stale skip marker {marker} for message {msg.id}")
    context.state.clear_skipped_message(msg.id)


async def _adopt_existing(context: MigrationContext, index: Any, msg: Any, target_channel_id: str, server_id: str,
                          stats: Dict[str, Any]) -> None:
    """The message is already on the server: make the database agree (so replies and links resolve to the real
    message), remove any stale skip marker, and move on without sending anything."""
    context.state.set_message_mapping(target_channel_id, str(msg.id), server_id)
    context.state.update_last_message_timestamp(target_channel_id, str(msg.created_at))
    context.state.update_last_message_id(target_channel_id, str(msg.id))
    await _drop_stale_marker(context, index, msg, target_channel_id)
    stats["already_on_server"] = stats.get("already_on_server", 0) + 1


URL_RE = re.compile(r"https?://[^\s<>()\[\]\"'`]+", re.IGNORECASE)
SUSPECT_LIMIT = 2          # consecutive failures with a healthy-looking API before a message is treated as suspect


def defang_links(text: str) -> str:
    """Shows every link as plain code text (`https://...`) so Fluxer never fetches or unfurls it."""
    return URL_RE.sub(lambda m: f"`{m.group(0)}`", text) if text else text


def message_link_info(msg: Any) -> str:
    """One line describing what is risky about a message that keeps failing: its link hosts and embeds."""
    from urllib.parse import urlparse
    content = getattr(msg, "content", None) or ""
    urls = list(URL_RE.findall(content))
    for e in (getattr(msg, "embeds", None) or []):
        d = e.to_dict() if hasattr(e, "to_dict") else (e if isinstance(e, dict) else {})
        urls += [d.get("url")] + [(d.get(k) or {}).get("url") for k in ("thumbnail", "image")]
    hosts = sorted({(urlparse(u).hostname or "") for u in urls if u} - {""})
    return (f"links: {', '.join(hosts[:6]) or 'none'}; embeds: {len(getattr(msg, 'embeds', None) or [])}; "
            f"text length: {len(content)}; attachments: {len(getattr(msg, 'attachments', None) or [])}")


OUTAGE_BACKOFF = (15, 30, 60, 120, 300)   # seconds between health checks while Fluxer is down; the last value repeats


def _fmt_secs(n: float) -> str:
    n = int(n)
    return f"{n // 3600}h{(n % 3600) // 60:02d}m" if n >= 3600 else (f"{n // 60}m{n % 60:02d}s" if n >= 60 else f"{n}s")


async def _sleep_checked(context: MigrationContext, seconds: float) -> bool:
    """Sleeps in 1s steps; False if the run was cancelled or reached its stop time meanwhile."""
    for _ in range(int(seconds)):
        if not context.is_running or context.deadline_reached():
            return False
        await asyncio.sleep(1)
    return context.is_running and not context.deadline_reached()


async def _wait_for_service(context: MigrationContext, target_channel_id: str, msg: Any, error: Exception,
                            outage_started: float, step: int) -> tuple:
    """Holds the run (nothing is retried, counted or skipped) until Fluxer answers again.

    Backs off 15s, 30s, 60s, 2m, 5m... and probes the API between waits. Returns (backoff step to continue from,
    number of probes that found the API unhealthy). 0 unhealthy probes means the API looked fine straight away, so the
    failure may be about this particular message rather than an outage.
    Raises MessageSendError if the run is cancelled / hits its stop time, or if config.max_outage_minutes (0 = keep
    waiting forever) is exceeded; in every case the message stays unmarked, so a resume retries it."""
    notice = getattr(context, "on_notice", None)
    max_minutes = int(getattr(getattr(context, "config", None), "max_outage_minutes", 0) or 0)
    detail = str(error)
    dm_sent = False
    unhealthy = 0
    while True:
        delay = OUTAGE_BACKOFF[min(step, len(OUTAGE_BACKOFF) - 1)]
        step += 1
        down_for = time.time() - outage_started
        logger.warning(f"Fluxer unavailable ({detail}); paused {_fmt_secs(down_for)} so far; message {msg.id} will be retried "
                       f"when it answers (next check in {delay}s)")
        if notice:
            notice(f"[yellow]Fluxer isn't accepting messages ({detail[:110]}). Paused for {_fmt_secs(down_for)}; "
                   f"checking again in {_fmt_secs(delay)}. Message {msg.id} is waiting, nothing is skipped.[/yellow]")
        if down_for >= 120 and hasattr(context, "notify"):      # a blip isn't worth a message; a real outage is
            dm_sent = context.notify(
                f"Fluxer isn't accepting messages ({detail[:160]}). The migration is paused on message `{msg.id}` "
                f"({_fmt_secs(down_for)} so far) and will resume by itself. Nothing is skipped.",
                kind="warn", key="outage", cooldown=1800, with_logs=True) or dm_sent
        if not await _sleep_checked(context, delay):
            raise MessageSendError("Cancelled while waiting for Fluxer to recover")
        if max_minutes and (time.time() - outage_started) > max_minutes * 60:
            raise MessageSendError(f"Fluxer has been unavailable for over {max_minutes} minutes ({detail}); stopping so "
                                   f"nothing is skipped. Message {msg.id} was not migrated.")
        healthy, detail = await context.fluxer_writer.check_health(target_channel_id)
        if not healthy:
            unhealthy += 1
        if healthy:
            repair = getattr(context.fluxer_writer, "repair_rate_limiter", None)
            if repair is not None:
                repair()          # the API answers: a send that still hangs is more likely a stuck local lock than an outage
            logger.info(f"Fluxer answers again after {_fmt_secs(time.time() - outage_started)}; retrying message {msg.id}")
            if notice:
                notice(f"[green]Fluxer is answering again. Retrying message {msg.id}...[/green]")
            if dm_sent and hasattr(context, "notify"):
                context.notify(f"Fluxer is answering again after {_fmt_secs(time.time() - outage_started)}; "
                               f"the migration is resuming at message `{msg.id}`.", kind="ok", key="recovered", cooldown=600)
            return step, unhealthy


async def _process_with_retries(context: MigrationContext, msg: Any, target_channel_id: str, stats: Dict[str, Any],
                                **kwargs) -> str | None:
    """_process_and_send_message with three answers to failure.

    * The SERVICE is struggling (503/5xx, timeouts, connection errors, uploads or rate limits that never clear):
      the run HOLDS, backing off and probing Fluxer, then retries the same message. Nothing is counted against the
      message and nothing is skipped. The first message sent after an outage is read back from the channel.
      `max_outage_minutes` (0 = wait as long as it takes) stops the run, still without skipping.
    * ONE MESSAGE seems to be the problem: it failed and the API looked healthy straight away, twice in a row. It is
      retried without custom embeds and with link unfurling suppressed, then with its links shown as plain code. If it
      still fails, a tiny canary message is sent the same way: if the canary goes through, Fluxer is fine and this
      message is the problem, so it is skipped with a marker (the run carries on); if the canary fails it is an outage
      after all and the run keeps holding.
    * Any other `MessageSendError` (about the message) is counted per message in the migration DB against
      `max_message_attempts` (default 5; 0 = never skip) and then skipped with a marker.
    A cancel or the scheduled stop time is never counted."""
    max_attempts = int(getattr(getattr(context, "config", None), "max_message_attempts", 5) or 0)
    notice = getattr(context, "on_notice", None)
    failed_here = False
    outage_started: float | None = None
    outage_step = 0
    degrade = 0
    suspect = 0
    risky = bool(URL_RE.search(getattr(msg, "content", None) or "")) or bool(getattr(msg, "embeds", None))
    while True:
        try:
            result = await _process_and_send_message(context=context, msg=msg, target_channel_id=target_channel_id,
                                                     stats=stats, degrade=degrade, **kwargs)
            if outage_started is not None and result:
                # first message after an outage: make sure it is really there before moving on
                if await context.fluxer_writer.verify_message(target_channel_id, result) is False:
                    logger.warning(f"Message {msg.id}: Fluxer returned {result} but it is not in the channel; resending")
                    stats["messages"] = max(0, stats.get("messages", 0) - 1)
                    continue
                outage_started, outage_step = None, 0
            if degrade:
                stats["degraded"] = stats.get("degraded", 0) + 1
                stats.setdefault("degraded_ids", []).append(str(msg.id))
            if failed_here:
                context.state.clear_message_attempts(msg.id)
            return result
        except ServiceUnavailable as e:
            if not context.is_running or context.deadline_reached():
                raise
            if outage_started is None:
                outage_started = time.time()
            info = message_link_info(msg)
            logger.warning(f"Message {msg.id} failed to send ({e}); {info}")
            outage_step, unhealthy = await _wait_for_service(context, target_channel_id, msg, e, outage_started, outage_step)
            suspect = 0 if unhealthy else suspect + 1          # an unhealthy probe means a real outage, not this message
            if suspect < SUSPECT_LIMIT:
                continue
            suspect = 0
            if risky and degrade < 2:
                degrade += 1
                what = "without embeds and with link previews suppressed" if degrade == 1 else "with its links shown as plain code"
                logger.warning(f"Message {msg.id} keeps failing while Fluxer looks healthy ({info}); retrying {what}")
                if notice:
                    notice(f"[yellow]Message {msg.id} keeps failing while Fluxer looks healthy ({info}). Retrying {what}...[/yellow]")
                if hasattr(context, "notify"):
                    context.notify(f"Message `{msg.id}` keeps failing while Fluxer looks healthy ({info}). Retrying {what}.",
                                   kind="warn", key="degrade", cooldown=600)
                continue
            canary = getattr(context.fluxer_writer, "canary", None)
            if canary is not None and await canary(target_channel_id):
                return await _skip_message(
                    context, msg, target_channel_id,
                    f"Fluxer keeps failing on this message ({e}) although other messages send fine; also tried "
                    f"{'without embeds/links' if risky else 'again'} ({info})", SUSPECT_LIMIT * (3 if risky else 1), stats,
                    kwargs.get("thread_id"))
            # the canary failed too (or can't be sent): it is an outage after all, keep holding
        except MessageSendError as e:
            if max_attempts <= 0 or not context.is_running or context.deadline_reached():
                raise
            failed_here = True
            attempts = context.state.record_message_attempt(msg.id, str(e))
            if attempts >= max_attempts:
                return await _skip_message(context, msg, target_channel_id, str(e), attempts, stats,
                                           kwargs.get("thread_id"))
            delay = min(60, 5 * attempts)
            logger.warning(f"Message {msg.id} failed ({e}); attempt {attempts}/{max_attempts}, retrying in {delay}s")
            if notice:
                notice(f"[yellow]Message {msg.id} failed ({e}). Attempt {attempts}/{max_attempts}; retrying in {delay}s...[/yellow]")
            await _sleep_checked(context, delay)


async def analyze_migration(context: MigrationContext, source_channel_id: int, after_message_id: int | None = None, inclusive: bool = False, progress_callback: Callable[[Dict[str, Any]], Awaitable[None]] | None = None, processed_threads: set | None = None) -> Dict[str, int]:

    """
    Scans channel history to count messages, threads, and attachments.
    """
    stats = {"messages": 0, "threads": 0, "attachments": 0}
    
    if processed_threads is None:
        processed_threads = set()

    async for msg in context.discord_reader.fetch_message_history(source_channel_id, after_id=after_message_id, inclusive=inclusive):
        if not context.is_running:
            break
        
        # Count thread messages and markers even if parent is skipped
        if hasattr(msg, 'thread') and msg.thread:
            thread = msg.thread
            if thread.id not in processed_threads:
                processed_threads.add(thread.id)
                stats["threads"] += 1
                
                # Fetch last migrated message ID for this thread
                target_channel_id = context.state.get_target_channel_id(str(source_channel_id))
                thread_after_id = None
                if target_channel_id:
                    thread_after_id = context.state.get_thread_last_message_id(target_channel_id, str(thread.id))
                
                # Recursively count thread content
                thread_stats = await analyze_migration(context, thread.id, after_message_id=int(thread_after_id) if thread_after_id else None, processed_threads=processed_threads)
                stats["messages"] += thread_stats["messages"]
                stats["attachments"] += thread_stats["attachments"]
                stats["threads"] += thread_stats["threads"] # Nested threads (rare in Discord but possible in forum channels)

        # Consistent filtering with migrate_messages
        if msg.type not in [
            context.discord_reader.MESSAGE_TYPE_DEFAULT,
            context.discord_reader.MESSAGE_TYPE_REPLY,
            context.discord_reader.MESSAGE_TYPE_THREAD_STARTER,
            context.discord_reader.MESSAGE_TYPE_FORWARD,
            context.discord_reader.MESSAGE_TYPE_CHAT_INPUT_COMMAND,
            context.discord_reader.MESSAGE_TYPE_CONTEXT_MENU_COMMAND,
            context.discord_reader.MESSAGE_TYPE_POLL_RESULT,
            context.discord_reader.MESSAGE_TYPE_AUTO_MODERATION_ACTION
        ]:
            logger.debug(f"Skipping message {msg.id} in analyze: type={msg.type} (not an allowed type)")
            continue

        stats["messages"] += 1
        stats["attachments"] += len(msg.attachments)
        logger.debug(f"Analyze msg {msg.id}: type={msg.type}, content={msg.content[:50]!r}...")

        if progress_callback and stats["messages"] % 10 == 0:
            await progress_callback(stats)

    # After scanning messages, explicitly check for any missed threads (e.g. archived or skipped in scan)
    # Only do this at the top level (not in recursive thread calls)
    if after_message_id is not None or inclusive: # Usually top level calls have some start point
        # Optimization: We check all threads for the channel
        all_threads = await get_channel_threads(context.discord_reader, source_channel_id)
        for t in all_threads:
            if t.id not in processed_threads:
                processed_threads.add(t.id)
                stats["threads"] += 1
                
                # Fetch last migrated message ID for this thread
                target_channel_id = context.state.get_target_channel_id(str(source_channel_id))
                thread_after_id = None
                if target_channel_id:
                    thread_after_id = context.state.get_thread_last_message_id(target_channel_id, str(t.id))

                thread_stats = await analyze_migration(context, t.id, after_message_id=int(thread_after_id) if thread_after_id else None, processed_threads=processed_threads)
                stats["messages"] += thread_stats["messages"]
                stats["attachments"] += thread_stats["attachments"]
                stats["threads"] += thread_stats["threads"]

    return stats


async def migrate_messages(
    context: MigrationContext, 
    source_channel_id: int, 
    target_channel_id: str, 
    after_message_id: int | None = None, 
    inclusive: bool = False,
    progress_callback: Callable[[Dict[str, Any]], Awaitable[None]] | None = None,
    thread_id: str | None = None,
    parent_target_id: str | None = None,
    thread_name: str | None = None,
    processed_threads: set | None = None
) -> Dict[str, Any]:
    """Migrate messages for a specific channel and returns detailed statistics."""
    stats = {
        "messages": 0, 
        "threads": 0, 
        "attachments": 0,
        "first_message_url": "",
        "last_message_url": "",
        "last_message_content": "",
        "last_message_author": ""
    }
    
    logger.info(f"Starting message migration: Discord #{source_channel_id} -> Fluxer #{target_channel_id}")
    if after_message_id:
        logger.info(f"Starting migration of {source_channel_id} (inclusive={inclusive})...")
    
    # Pre-fetch channel and thread names for better mention resolution
    if not hasattr(context, 'channel_names'):
        context.channel_names = {}
        try:
            logger.debug(f"Pre-fetching channel and thread names for guild {context.discord_reader.guild.id}...")
            # fetch_channels usually includes all non-thread channels
            all_channels = await context.discord_reader.fetch_channels()
            for c in all_channels:
                context.channel_names[str(c.id)] = c.name
            
            # get_active_threads helps find threads that might be mentioned
            threads = await context.discord_reader.get_active_threads()
            for t in threads:
                context.channel_names[str(t.id)] = t.name
            
            logger.debug(f"Pre-fetched {len(context.channel_names)} names.")
        except Exception as e:
            logger.debug(f"Failed to pre-fetch channel names: {e}")

    # Process missed threads first if resuming
    if processed_threads is None:
        processed_threads = set()

    async def _process_missed_threads():
        """Helper to scan for threads not yet processed in the current scan."""
        if not context.is_running:
            return
        logger.info(f"Checking for missed or pending threads in channel {source_channel_id}...")
        all_threads = await get_channel_threads(context.discord_reader, source_channel_id)
        for t in all_threads:
            if not context.is_running:
                break
            if t.id not in processed_threads:
                processed_threads.add(t.id)
                
                # Skip if thread was already fully migrated in a previous run
                if context.state.is_thread_completed(target_channel_id, str(t.id)):
                    logger.debug(f"Skipping already completed thread '{t.name}' (ID: {t.id})")
                    continue

                logger.info(f"Checking missed thread '{t.name}' (ID: {t.id})")
                
                # Fetch last migrated message ID for this thread
                thread_after_id = context.state.get_thread_last_message_id(target_channel_id, str(t.id))
                if thread_after_id:
                    logger.info(f"Resuming missed/pending thread '{t.name}' from after message ID: {thread_after_id}")

                stats["threads"] += 1
                thread_stats = await migrate_messages(
                    context=context,
                    source_channel_id=t.id,
                    target_channel_id=target_channel_id,
                    after_message_id=int(thread_after_id) if thread_after_id else None,
                    thread_id=str(t.id),
                    parent_target_id=None,
                    thread_name=t.name,
                    processed_threads=processed_threads
                )
                stats["messages"] += thread_stats["messages"]
                stats["attachments"] += thread_stats["attachments"]
                stats["threads"] += thread_stats["threads"]
                # A halt/deadline inside a thread must surface on the parent so the UI reports it (not "Interrupted")
                for _k in ("error", "stopped"):
                    if thread_stats.get(_k):
                        stats.setdefault(_k, thread_stats[_k])
                if thread_stats.get("skipped"):
                    stats["skipped"] = stats.get("skipped", 0) + thread_stats["skipped"]
                    stats.setdefault("skipped_ids", []).extend(thread_stats.get("skipped_ids", []))
                
                if context.is_running:
                    await context.fluxer_writer.send_marker(
                        channel_id=target_channel_id,
                        content=f"> <<< END OF THREAD >>>"
                    )

    try:
        # If resuming (after_message_id is set) and at top level, check for pending threads FIRST
        # to preserve chronological order (finish old unfinished business first)
        if not thread_id and after_message_id is not None:
            await _process_missed_threads()

        async for msg in context.discord_reader.fetch_message_history(source_channel_id, after_id=after_message_id, inclusive=inclusive):
            if not context.is_running:
                logger.warning("Migration interrupted by user (is_running=False)")
                break
            if context.deadline_reached():
                logger.info("Migration reached its scheduled stop time; stopping cleanly.")
                stats["stopped"] = "deadline"
                context.is_running = False
                break
                


            # Skip system messages like "pinned a message", etc.
            logger.debug(f"Analyzing message {msg.id}: type={msg.type}, content_len={len(msg.content) if msg.content else 0}, attachments={len(msg.attachments)}, embeds={len(msg.embeds)}")
            if msg.type not in [
                context.discord_reader.MESSAGE_TYPE_DEFAULT,
                context.discord_reader.MESSAGE_TYPE_REPLY,
                context.discord_reader.MESSAGE_TYPE_THREAD_STARTER,
                context.discord_reader.MESSAGE_TYPE_FORWARD,
                context.discord_reader.MESSAGE_TYPE_CHAT_INPUT_COMMAND,
                context.discord_reader.MESSAGE_TYPE_CONTEXT_MENU_COMMAND,
                context.discord_reader.MESSAGE_TYPE_POLL_RESULT,
                context.discord_reader.MESSAGE_TYPE_AUTO_MODERATION_ACTION
            ]:
                # If we are skipping the parent, we STILL need to check for a thread!
                if hasattr(msg, 'thread') and msg.thread:
                    thread = msg.thread
                    if thread.id not in processed_threads:
                        processed_threads.add(thread.id)
                        # Track thread entry
                        stats["threads"] += 1
                        
                        # Fetch last migrated message ID for this thread
                        thread_after_id = context.state.get_thread_last_message_id(target_channel_id, str(thread.id))
                        if thread_after_id:
                            logger.info(f"Resuming thread '{thread.name}' from after message ID: {thread_after_id}")

                        # Migrate thread messages recursively
                        thread_stats = await migrate_messages(
                            context=context,
                            source_channel_id=thread.id,
                            target_channel_id=target_channel_id,
                            after_message_id=int(thread_after_id) if thread_after_id else None,
                            thread_id=str(thread.id),
                            parent_target_id=None,
                            thread_name=thread.name,
                            processed_threads=processed_threads
                        )
                        stats["messages"] += thread_stats["messages"]
                        stats["attachments"] += thread_stats["attachments"]
                        stats["threads"] += thread_stats["threads"]
                        # A halt/deadline inside a thread must surface on the parent so the UI reports it (not "Interrupted")
                        for _k in ("error", "stopped"):
                            if thread_stats.get(_k):
                                stats.setdefault(_k, thread_stats[_k])
                        if thread_stats.get("skipped"):
                            stats["skipped"] = stats.get("skipped", 0) + thread_stats["skipped"]
                            stats.setdefault("skipped_ids", []).extend(thread_stats.get("skipped_ids", []))
    
                        # Send End Marker
                        if context.is_running:
                            await context.fluxer_writer.send_marker(
                                channel_id=target_channel_id,
                                content=f"> <<< END OF THREAD >>>"
                            )
                    
                if progress_callback:
                    await progress_callback(stats)
                continue
            else:
                # Use custom clean_mentions with msg mentions for accuracy
                content = clean_mentions(
                    msg.content, 
                    context.discord_reader.guild, 
                    msg.mentions, 
                    msg.role_mentions, 
                    msg.channel_mentions,
                    context.state.emoji_map,
                    context.state.channel_map,
                    state=context.state,
                    target_server_id=context.fluxer_writer.community_id,
                    channel_names=context.channel_names if hasattr(context, 'channel_names') else None,
                    anonymize_users=context.config.anonymize_users
                )
                logger.debug(f"Message {msg.id} cleaned content length: {len(content) if content else 0}")
                
            # Process attachments
            files = []
            attachments_to_process = list(msg.attachments)
            
            # Check if this message is forwarded
            # Discord flags: forwarded (is bit 28 / 0x10000000)
            is_forwarded = False
            if hasattr(msg.flags, 'forwarded'):
                is_forwarded = msg.flags.forwarded
            
            # If forwarded, the content and attachments might be in message_snapshots (discord.py 2.5+)
            # Note: If content was set by thread_starter_message, we don't overwrite it.
            if is_forwarded:
                logger.debug(f"Detected forwarded message: ID={msg.id}, Flags={msg.flags.value}")
                if hasattr(msg, 'message_snapshots') and msg.message_snapshots:
                    # For now we handle the first snapshot
                    snapshot = msg.message_snapshots[0]
                    if not content: # Only update content if it wasn't already set (e.g., by thread_starter_message)
                        content = snapshot.content
                        if context.discord_reader.guild:
                            content = clean_mentions(
                                content, 
                                context.discord_reader.guild, 
                                snapshot.mentions if hasattr(snapshot, 'mentions') else None,
                                snapshot.role_mentions if hasattr(snapshot, 'role_mentions') else None,
                                snapshot.channel_mentions if hasattr(snapshot, 'channel_mentions') else None, # Changed this line
                                context.state.emoji_map,
                                context.state.channel_map,
                                state=context.state,
                                target_server_id=context.fluxer_writer.community_id,
                                channel_names=context.channel_names if hasattr(context, 'channel_names') else None,
                                anonymize_users=context.config.anonymize_users
                            )
                    # Add snapshot attachments to the list to process
                    attachments_to_process.extend(snapshot.attachments)
                    logger.debug(f"Found forwarded snapshot content: {content[:50]}... and {len(snapshot.attachments)} attachments")

            for att in attachments_to_process:
                try:
                    att_data = await context.discord_reader.download_attachment(att)
                    files.append({"filename": att.filename, "data": att_data})
                    stats["attachments"] += 1
                except Exception as e:
                    logger.error(f"Failed to download attachment {att.filename}: {e}")
            
            # Process stickers as attachments
            if hasattr(msg, 'stickers') and msg.stickers:
                for s in msg.stickers:
                    try:
                        sticker_data = await context.discord_reader.download_sticker(s)
                        if sticker_data:
                            # Use format to determine extension
                            format_val = getattr(s, 'format', 'png')
                            logger.debug(f"Sticker {getattr(s, 'name', 'unknown')} format_val type: {type(format_val)}, value: {format_val}")
                            
                            if hasattr(format_val, 'name'): # discord.py StickerFormat enum
                                ext = format_val.name.lower()
                            elif isinstance(format_val, int):
                                format_map = {1: 'png', 2: 'apng', 3: 'lottie', 4: 'gif'}
                                ext = format_map.get(format_val, 'png')
                            else:
                                ext = str(format_val).lower()
                            
                            logger.debug(f"Determined sticker extension: {ext}")
                            
                            # Fluxer: Convert animated stickers to WebP
                            # Lottie (json) → GIF (via lottie lib) → WebP (via Pillow)
                            if ext == 'lottie':
                                if HAS_LOTTIE:
                                    try:
                                        logger.debug(f"Converting Lottie sticker {s.name} (ID: {s.id}) to WebP...")
                                        lottie_data = json.loads(sticker_data)
                                        
                                        def _convert_lottie(data):
                                            anim = Animation.load(data)
                                            buf = io.BytesIO()
                                            export_gif(anim, buf)
                                            buf.seek(0)
                                            return buf

                                        gif_buf = await asyncio.to_thread(_convert_lottie, lottie_data)
                                        
                                        # GIF → WebP via Pillow
                                        from PIL import Image
                                        
                                        def _convert_gif_to_webp(buf):
                                            img = Image.open(buf)
                                            w_buf = io.BytesIO()
                                            if getattr(img, 'n_frames', 1) > 1:
                                                img.save(w_buf, format='WEBP', save_all=True, loop=0, quality=80)
                                            else:
                                                img.save(w_buf, format='WEBP', quality=80)
                                            return w_buf.getvalue()

                                        sticker_data = await asyncio.to_thread(_convert_gif_to_webp, gif_buf)
                                        ext = 'webp'
                                        logger.debug(f"Successfully converted Lottie sticker {s.name} to WebP")
                                    except Exception as conv_err:
                                        logger.error(f"Failed to convert Lottie sticker {s.name} to WebP: {conv_err}")
                                        ext = 'json'
                                else:
                                    logger.warning(f"Lottie library not available, sending sticker {s.name} as raw JSON")
                                    ext = 'json'
                            
                            elif ext in ('apng', 'gif'):
                                try:
                                    logger.debug(f"Converting {ext.upper()} sticker {s.name} (ID: {s.id}) to WebP...")
                                    from PIL import Image
                                    
                                    def _process_animated_sticker(data):
                                        img = Image.open(io.BytesIO(data))
                                        webp_buf = io.BytesIO()
                                        if getattr(img, 'n_frames', 1) > 1:
                                            img.save(webp_buf, format='WEBP', save_all=True, loop=0, quality=80)
                                        else:
                                            img.save(webp_buf, format='WEBP', quality=80)
                                        return webp_buf.getvalue()

                                    sticker_data = await asyncio.to_thread(_process_animated_sticker, sticker_data)
                                    ext = 'webp'
                                    logger.debug(f"Successfully converted sticker {s.name} to WebP")
                                except Exception as conv_err:
                                    logger.error(f"Failed to convert {ext.upper()} sticker {s.name} to WebP: {conv_err}")
                                    # Keep original format as fallback
                            
                            filename = f"sticker_{s.name}_{s.id}.{ext}"
                            sticker_size = len(sticker_data)
                            files.append({"filename": filename, "data": sticker_data})
                            stats["attachments"] += 1
                            logger.debug(f"Added sticker {s.name} as attachment (extension: {ext}, size: {sticker_size} bytes)")
                    except Exception as e:
                        logger.error(f"Failed to download sticker {getattr(s, 'name', 'unknown')}: {e}")
                       # Check for existing mapping to avoid duplicates when resuming
            if context.state.get_target_message_id(target_channel_id, str(msg.id)):
                continue
                
            try:
                fluxer_msg_id = await _process_with_retries(
                    context=context,
                    msg=msg,
                    target_channel_id=target_channel_id,
                    stats=stats,
                    thread_id=thread_id,
                    parent_target_id=parent_target_id,
                    thread_name=thread_name,
                    processed_threads=processed_threads
                )

                # Check for associated thread (Individual mode recursion)
                if hasattr(msg, 'thread') and msg.thread:
                    thread = msg.thread
                    if thread.id not in processed_threads:
                        processed_threads.add(thread.id)
                        stats["threads"] += 1
                        
                        thread_after_id = context.state.get_thread_last_message_id(target_channel_id, str(thread.id))
                        thread_stats = await migrate_messages(
                            context=context,
                            source_channel_id=thread.id,
                            target_channel_id=target_channel_id,
                            after_message_id=int(thread_after_id) if thread_after_id else None,
                            thread_id=str(thread.id),
                            parent_target_id=fluxer_msg_id,
                            thread_name=thread.name,
                            processed_threads=processed_threads
                        )
                        stats["messages"] += thread_stats["messages"]
                        stats["attachments"] += thread_stats["attachments"]
                        stats["threads"] += thread_stats["threads"]
                        # A halt/deadline inside a thread must surface on the parent so the UI reports it (not "Interrupted")
                        for _k in ("error", "stopped"):
                            if thread_stats.get(_k):
                                stats.setdefault(_k, thread_stats[_k])
                        if thread_stats.get("skipped"):
                            stats["skipped"] = stats.get("skipped", 0) + thread_stats["skipped"]
                            stats.setdefault("skipped_ids", []).extend(thread_stats.get("skipped_ids", []))
                        
                        if context.is_running:
                            await context.fluxer_writer.send_marker(
                                channel_id=target_channel_id,
                                content=f"> <<< END OF THREAD >>>"
                            )
                
                # Update Link Tracking (Parent pointer updates)
                if not stats["first_message_url"]:
                    stats["first_message_url"] = msg.jump_url
                stats["last_message_url"] = msg.jump_url
                
                if progress_callback:
                    await progress_callback(stats)
            except MessageSendError as e:
                # Not delivered: stop so progress isn't advanced past this message.
                context.is_running = False
                if context.deadline_reached():
                    logger.info(f"Scheduled stop time reached while sending message {msg.id}; it was not marked migrated.")
                    stats["stopped"] = "deadline"
                elif str(e).startswith("Cancelled"):
                    logger.info(f"Cancelled by the user at message {msg.id}; it was not marked migrated.")
                else:
                    logger.error(f"Migration halted at message {msg.id}: {e}")
                    stats["error"] = f"Halted at message {msg.id}: {e}"
                    if hasattr(context, "notify"):
                        context.notify(f"Migration halted at message `{msg.id}`: {e}", kind="error", key="halt", with_logs=True)
                break
            except Exception as e:
                logger.error(f"Failed to process message {msg.id}: {e}")
                if hasattr(context, "notify"):
                    context.notify(f"Unexpected error on message `{msg.id}` (it was left unmigrated and the run continued): {e}",
                                   kind="warn", key="exc", cooldown=300, with_logs=True)
                import traceback
                logger.error(traceback.format_exc())
        
        # Mark thread as completed if we finished the loop without being interrupted
        if thread_id and context.is_running:
            context.state.update_thread_completed(target_channel_id, thread_id, completed=True)
            logger.info(f"Thread '{thread_name}' (ID: {thread_id}) marked as completed.")
        

    except (KeyboardInterrupt, asyncio.CancelledError):
        context.is_running = False
        pass
    
    return stats


async def analyze_global_migration(context: MigrationContext, after_message_id: int | None = None, inclusive: bool = False, progress_callback: Callable[[Dict[str, Any]], Awaitable[None]] | None = None, ignore_progress: bool = False) -> Dict[str, int]:
    """
    Scans the entire server history to count messages, threads, and attachments globally.
    """
    stats = {"messages": 0, "threads": 0, "attachments": 0}
    
    # In global mode, thread messages are returned natively in timestamp order by global fetch if they're in the DB
    # However we just count them if the fetcher yields them.
    # Fetch global progress map to skip migrated messages efficiently
    progress_map = context.state.get_all_last_message_ids()
    
    async for msg in context.discord_reader.fetch_global_message_history(after_id=after_message_id):
        if not context.is_running:
            break
            
        # Determine target channel to check for existing mapping
        if not msg.channel:
            continue
            
        target_channel_id = context.state.get_target_channel_id(str(msg.channel.id))
        if not target_channel_id:
            continue

        # Efficient skip: if message ID is <= last migrated ID for this channel/thread
        # This is the primary resume mechanism: wait until we pass the last migrated ID for this channel
        last_id = progress_map.get(str(target_channel_id))
        if last_id and msg.id <= int(last_id) and not ignore_progress:
            continue
            
        if msg.type not in [
            context.discord_reader.MESSAGE_TYPE_DEFAULT,
            context.discord_reader.MESSAGE_TYPE_REPLY,
            context.discord_reader.MESSAGE_TYPE_THREAD_STARTER,
            context.discord_reader.MESSAGE_TYPE_FORWARD,
            context.discord_reader.MESSAGE_TYPE_CHAT_INPUT_COMMAND,
            context.discord_reader.MESSAGE_TYPE_CONTEXT_MENU_COMMAND,
            context.discord_reader.MESSAGE_TYPE_POLL_RESULT,
            context.discord_reader.MESSAGE_TYPE_AUTO_MODERATION_ACTION
        ]:
            continue

        # Messages with nothing to send (no text, files, stickers or forwarded snapshot) are skipped by
        # _process_and_send_message, so don't count them (keeps totals / ETAs honest).
        if not (msg.content or msg.attachments or getattr(msg, 'stickers', None) or getattr(msg, 'message_snapshots', None)):
            continue
            
        stats["messages"] += 1
        stats["attachments"] += len(msg.attachments)
        if hasattr(msg, 'thread') and msg.thread:
            # We don't recursively traverse here, we just count the fact there is a thread
            # The actual thread messages are also fetched by the global fetcher because they have their own timestamp/id
            stats["threads"] += 1
            
        if progress_callback and stats["messages"] % 100 == 0:
            await progress_callback(stats)
            
    if progress_callback:
        await progress_callback(stats)
        
    return stats


async def migrate_global_messages(
    context: MigrationContext,
    after_message_id: int | None = None,
    inclusive: bool = False,
    progress_callback: Callable[[Dict[str, Any]], Awaitable[None]] | None = None,
    verify_server: bool = False,
    start_ts: float | None = None,
) -> Dict[str, Any]:
    """
    Migrates messages across all channels chronologically to Fluxer.

    verify_server=True ("start from message X"): the database's progress is ignored. Each message is looked up on the
    Fluxer channel itself (see server_index.py) and only sent if it really isn't there; messages found there are
    adopted (mapping repaired) and any stale skip marker for them is deleted. `start_ts` = creation time (epoch s)
    of the first message, which bounds how far back each channel is read.
    """
    stats = {
        "messages": 0,
        "threads": 0,
        "attachments": 0,
        "last_message_content": "",
        "last_message_author": "",
        "first_message_url": None,
        "last_message_url": None
    }
    
    processed_threads = set()
    logger.info("Starting Global Waterfall Migration for Fluxer...")
    
    # Fetch global progress map to skip migrated messages efficiently
    progress_map = context.state.get_all_last_message_ids()

    index = None
    if verify_server:
        from src.fluxer.server_index import ServerIndex
        index = ServerIndex(context.fluxer_writer, start_ts, bot_username=context.fluxer_writer.bot_username())
        stats["already_on_server"] = 0
        logger.info("Waterfall in verify mode: checking each message against the server before sending")

    try:
        async for msg in context.discord_reader.fetch_global_message_history(after_id=after_message_id):
            if not context.is_running:
                logger.warning("Global migration interrupted by user")
                break
            if context.deadline_reached():
                logger.info("Global migration reached its scheduled stop time; stopping cleanly.")
                stats["stopped"] = "deadline"
                context.is_running = False
                break
                
            if msg.type not in [
                context.discord_reader.MESSAGE_TYPE_DEFAULT,
                context.discord_reader.MESSAGE_TYPE_REPLY,
                context.discord_reader.MESSAGE_TYPE_THREAD_STARTER,
                context.discord_reader.MESSAGE_TYPE_FORWARD,
                context.discord_reader.MESSAGE_TYPE_CHAT_INPUT_COMMAND,
                context.discord_reader.MESSAGE_TYPE_CONTEXT_MENU_COMMAND,
                context.discord_reader.MESSAGE_TYPE_POLL_RESULT,
                context.discord_reader.MESSAGE_TYPE_AUTO_MODERATION_ACTION
            ]:
                continue
                
            # Determine target channel
            if not msg.channel:
                continue
                
            target_channel_id = context.state.get_target_channel_id(str(msg.channel.id))
            if not target_channel_id:
                continue

            # Efficient skip: if message ID is <= last migrated ID for this channel/thread
            # This ensures we only resume a channel once we reach its last known progress point
            last_id = progress_map.get(str(target_channel_id))
            if last_id and msg.id <= int(last_id) and not verify_server:
                continue
                
            # If it's a thread message, we need to handle it based on if it's the thread starter or a reply
            if hasattr(msg, 'thread') and msg.thread and msg.id == msg.thread.id:
                processed_threads.add(msg.thread.id)
                stats["threads"] += 1

            try:
                existing = await _find_on_server(context, index, msg, target_channel_id) if verify_server else None
                if existing:
                    await _adopt_existing(context, index, msg, target_channel_id, existing, stats)
                else:
                    skipped_before = stats.get("skipped", 0)
                    await _process_with_retries(
                        context=context,
                        msg=msg,
                        target_channel_id=target_channel_id,
                        stats=stats,
                        processed_threads=processed_threads
                    )
                    if verify_server and stats.get("skipped", 0) == skipped_before:
                        await _drop_stale_marker(context, index, msg, target_channel_id)

                if not stats["first_message_url"]:
                    stats["first_message_url"] = msg.jump_url
                stats["last_message_url"] = msg.jump_url
                
                if progress_callback:
                    await progress_callback(stats)
                    
            except MessageSendError as e:
                # Not delivered (rate limit never cleared / timeout / outage / deadline). Stop right here WITHOUT
                # advancing any progress marker so a resume retries this exact message.
                context.is_running = False
                if context.deadline_reached():
                    logger.info(f"Scheduled stop time reached while sending message {msg.id}; it was not marked migrated.")
                    stats["stopped"] = "deadline"
                elif str(e).startswith("Cancelled"):
                    logger.info(f"Cancelled by the user at message {msg.id}; it was not marked migrated.")
                else:
                    logger.error(f"Waterfall halted at message {msg.id}: {e}")
                    stats["error"] = f"Halted at message {msg.id}: {e}"
                    if hasattr(context, "notify"):
                        context.notify(f"Waterfall halted at message `{msg.id}`: {e}", kind="error", key="halt", with_logs=True)
                break
            except Exception as e:
                logger.error(f"Failed to process global message {msg.id}: {e}")
                if hasattr(context, "notify"):
                    context.notify(f"Unexpected error on message `{msg.id}` (it was left unmigrated and the run continued): {e}",
                                   kind="warn", key="exc", cooldown=300, with_logs=True)

            # Message is fully handled (sent, or deliberately skipped/rejected): everything up to and
            # including this ID is done. This is the resume point.
            context.state.set_waterfall_cursor(msg.id)
                
    except (KeyboardInterrupt, asyncio.CancelledError):
        context.is_running = False
        pass
        
    return stats
