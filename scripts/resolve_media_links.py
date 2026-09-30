"""Save Discord media that was pasted as a *link* in an existing backup (updates backup.db + the media pool).

Discord expires attachment links; a bot token can refresh them. This finds every distinct
cdn.discordapp.com / media.discordapp.net attachment link in the backup's message text, refreshes and downloads
each once, hashes it (SHA-256) into the content-addressed media pool (so 3,000 pastes of one GIF = one file), and
records the result in the `link_media` table. Safe to re-run: finished links are skipped. New backups/syncs in the
TUI do this automatically at the end; use this script to update backups made before that.

  ./venv/bin/python scripts/resolve_media_links.py --profile MyServer --dry-run     # count only
  ./venv/bin/python scripts/resolve_media_links.py --profile MyServer                # do it
  ./venv/bin/python scripts/resolve_media_links.py --profile MyServer --retry-dead   # try links that failed before

Migration (Fluxer) then uploads resolved links as real attachments; unresolved/dead links stay as text.
"""
import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.core.backup_database import BackupDatabase  # noqa: E402
from src.core.configuration import load_config  # noqa: E402
from src.core.media_links import MediaLinkError, MediaLinkResolver, summarize  # noqa: E402


def locate(profile: str, backup_dir: str | None):
    base = Path(".") if profile == "." else Path(f"ReaperFiles-{profile}")
    cfg = load_config(base / "reaper_config.yaml", create_if_missing=False)
    if backup_dir:
        bdir = Path(backup_dir)
    else:
        bdir = base / f"DISCORD_BACKUP-{cfg.discord_server_id}"
    if not (bdir / "backup.db").exists():
        raise SystemExit(f"No backup.db in {bdir}")
    return cfg, bdir


async def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", required=True, help="profile name (ReaperFiles-NAME) or '.' for ./reaper_config.yaml")
    ap.add_argument("--backup-dir", help="explicit DISCORD_BACKUP-<id> folder (default: derived from the profile)")
    ap.add_argument("--dry-run", action="store_true", help="only count what would be fetched")
    ap.add_argument("--retry-dead", action="store_true", help="retry links previously marked dead / too large")
    ap.add_argument("--concurrency", type=int, default=4, help="parallel downloads (default 4)")
    ap.add_argument("--max-size-mb", type=int, default=100, help="skip files larger than this (default 100)")
    ap.add_argument("--limit", type=int, help="stop discovery after N distinct links (testing)")
    a = ap.parse_args()

    cfg, bdir = locate(a.profile, a.backup_dir)
    if not cfg.discord_bot_token and not a.dry_run:
        raise SystemExit("No discord_bot_token in the profile config; needed to refresh expired links.")
    db = BackupDatabase(bdir / "backup.db")
    resolver = MediaLinkResolver(db, bdir, cfg.discord_bot_token or "", max_bytes=a.max_size_mb * 1024 * 1024,
                                 concurrency=a.concurrency)

    async def progress(st):
        print(f"  {st['done']}/{st['total']}  saved {st['ok']} (new {st['new_files']}, dup {st['dedup_hits']})  "
              f"dead {st['dead']}  errors {st['error']}", flush=True)

    try:
        stats = await resolver.resolve_backup(progress=progress, retry_dead=a.retry_dead, limit=a.limit, dry_run=a.dry_run)
    except MediaLinkError as e:
        raise SystemExit(f"Stopped: {e}")
    print(("DRY RUN: " if a.dry_run else "") + summarize(stats))
    if a.dry_run:
        print(f"{stats['pending']} link(s) would be fetched.")
    else:
        print("link_media status counts:", db.link_media_counts())
    db.close()


if __name__ == "__main__":
    asyncio.run(main())
