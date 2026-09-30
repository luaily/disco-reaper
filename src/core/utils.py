from typing import Any, Optional
import re
import logging

def parse_snowflake(value: Any) -> Optional[int]:
    """Safely parses a Discord ID (Snowflake) from any input, handling 'None' strings."""
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() == "none" or s == "NULL":
        return None
    try:
        return int(s)
    except ValueError:
        return None

logger = logging.getLogger(__name__)

def resolve_discord_links(content: str, state, platform: str, target_server_id: str) -> str:
    """
    Finds Discord message/channel links and resolves them to the target platform 
    if they have been migrated.
    """
    from src.core.state import MigrationState
    if not isinstance(state, MigrationState):
        logger.warning(f"resolve_discord_links: state is not MigrationState (type: {type(state)})")
    if not content:
        return content

    # Regex for Discord links: https://discord.com/channels/{guild}/{channel}/{message}
    # Matches: https://discord.com/channels/123/456 or https://discord.com/channels/123/456/789
    discord_link_re = re.compile(r'https?://(?:ptb\.|canary\.)?discord\.com/channels/(\d+)/(\d+)(?:/(\d+))?')

    def replace_link(match):
        full_url = match.group(0)
        
        # Check if already part of a markdown link: [text](link) or [text](<link>)        
        # We look backwards for ]( or ](<
        start_idx = match.start()
        if start_idx > 2:
            prev_chars = content[max(0, start_idx-3):start_idx]
            if prev_chars.endswith("](") or prev_chars.endswith("](<"):
                logger.debug(f"resolve_discord_links: Skipping already-wrapped link: {full_url[:60]}")
                return full_url

        guild_id = match.group(1)
        channel_id = match.group(2)
        message_id = match.group(3)

        target_cid = state.get_target_channel_id(channel_id) or state.get_target_category_id(channel_id)
        logger.debug(f"resolve_discord_links: guild={guild_id} channel={channel_id} msg={message_id} target_cid={target_cid}")
        
        if message_id:
            # Message link resolution
            t_cid, t_mid = state.find_message_mapping(message_id)
            logger.debug(f"resolve_discord_links: find_message_mapping({message_id}) -> t_cid={t_cid}, t_mid={t_mid}")
            if t_mid:
                # Use found channel ID if available, otherwise fallback to channel_id mapping
                final_cid = t_cid or target_cid
                if final_cid:
                    if platform == "stoat":
                        return f"https://stoat.chat/server/{target_server_id}/channel/{final_cid}/{t_mid}"
                    else: # Fluxer
                        return f"https://fluxer.app/channels/{target_server_id}/{final_cid}/{t_mid}"
            
            # Fallback for unmigrated message
            return f"[`discord-message`](<{full_url}>)"
        else:
            # Channel link resolution
            if target_cid:
                if platform == "stoat":
                    return f"https://stoat.chat/server/{target_server_id}/channel/{target_cid}"
                else: # Fluxer
                    return f"https://fluxer.app/channels/{target_server_id}/{target_cid}"
            
            # Fallback for unmapped channel
            return f"[`discord-channel`](<{full_url}>)"


    logger.debug(f"resolve_discord_links: Processing content (len {len(content)}): {content[:100]!r}")
    result = discord_link_re.sub(replace_link, content)
    if result != content:
        logger.debug(f"resolve_discord_links: Content resolved to (len {len(result)}): {result[:100]!r}")
    return result

import subprocess

def get_app_version() -> str:
    """Gets the dynamic app version from baked file or git."""
    try:
        from src.core._baked_version import __version__
        return f"Reaper-{__version__}"
    except ImportError:
        pass
        
    try:
        version = subprocess.check_output(
            ["git", "describe", "--tags", "--always"], 
            stderr=subprocess.DEVNULL,
            universal_newlines=True
        ).strip()
        if not version:
            return "Reaper-Unknown"
        return f"Reaper-{version}"
    except Exception:
        return "Reaper-Unknown-git"


# ── run-window helpers (used by the TUI run-options dialog and scripts/timed_waterfall.py) ──

def parse_until(text: str, now=None) -> float:
    """'HH:MM' (24h, local time) -> epoch seconds of its next occurrence (tomorrow if already past)."""
    from datetime import datetime, timedelta
    m = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", text.strip())
    if not m:
        raise ValueError(f"Stop time must be HH:MM (24h), got {text!r}")
    now = now or datetime.now()
    t = now.replace(hour=int(m.group(1)), minute=int(m.group(2)), second=0, microsecond=0)
    if t <= now:
        t += timedelta(days=1)
    return t.timestamp()


def parse_duration(text: str) -> float:
    """'9h', '90m', '9h30m', '45s' -> seconds."""
    m = re.fullmatch(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?", text.strip().lower())
    if not m or not any(m.groups()):
        raise ValueError(f"Duration must look like 9h, 90m, 9h30m or 45s, got {text!r}")
    return int(m.group(1) or 0) * 3600 + int(m.group(2) or 0) * 60 + int(m.group(3) or 0)


def fmt_dur(seconds: float) -> str:
    seconds = int(max(0, seconds))
    if seconds < 3600:
        return f"{seconds // 60}m{seconds % 60:02d}s"
    return f"{seconds // 3600}h{(seconds % 3600) // 60:02d}m"


def parse_stop_spec(text: str, now_ts: float | None = None) -> float | None:
    """Stop-time field: blank -> None (no limit); 'HH:MM' -> next occurrence; '9h30m'/'90m'/'45s' -> now + duration.
    Returns an epoch-seconds deadline."""
    import time as _time
    text = (text or "").strip()
    if not text:
        return None
    if ":" in text:
        return parse_until(text)
    return (now_ts if now_ts is not None else _time.time()) + parse_duration(text)
