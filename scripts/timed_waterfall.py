"""Timed / overnight Waterfall migration (Fluxer, from a local backup).

Runs the same Waterfall the TUI runs, headless, and stops cleanly at a set time. Because progress lives in the
migration DB (waterfall cursor), you can run it every night from cron/launchd until it finishes.

  python scripts/timed_waterfall.py --profile MyServer --until 07:00
  python scripts/timed_waterfall.py --profile MyServer --for 9h --max-rate 40      # 40 msgs/min cap
  python scripts/timed_waterfall.py --profile .                                    # reaper_config.yaml in CWD

Exit codes (for schedulers): 0 = everything migrated, 10 = stopped at the deadline / by signal with work left,
1 = error (the message that failed was NOT marked migrated; the next run retries it), 2 = bad usage/config.
Stopping is always between messages: an in-flight rate-limit wait is abandoned at the deadline and that message is
retried on the next run. Ctrl-C / SIGTERM also stop cleanly.
"""
import argparse
import asyncio
import logging
import re
import signal
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.core.base import MigrationContext  # noqa: E402
from src.core.configuration import load_config  # noqa: E402
from src.core.utils import parse_until, parse_duration, fmt_dur  # noqa: E402,F401

EXIT_DONE, EXIT_MORE, EXIT_ERROR, EXIT_USAGE = 0, 10, 1, 2


def profile_paths(profile: str) -> tuple[Path, str]:
    if profile == ".":
        return Path("reaper_config.yaml"), "."
    return Path(f"ReaperFiles-{profile}") / "reaper_config.yaml", f"ReaperFiles-{profile}"


def log(msg: str):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


async def run(args) -> int:
    from src.fluxer import clone_server, migrate_message as mm

    cfg_path, base_dir = profile_paths(args.profile)
    try:
        config = load_config(cfg_path, create_if_missing=False)
    except Exception as e:
        log(f"Cannot load {cfg_path}: {e}")
        return EXIT_USAGE
    if (config.target_platform or "fluxer") != "fluxer":
        log("Timed Waterfall currently supports Fluxer only (Stoat lacks the cursor / halt-on-failure semantics).")
        return EXIT_USAGE

    deadline = None
    if args.until:
        deadline = parse_until(args.until)
    elif args.duration:
        deadline = time.time() + parse_duration(args.duration)

    if args.max_attempts is not None:
        config.max_message_attempts = max(0, args.max_attempts)
    if args.max_outage is not None:
        config.max_outage_minutes = max(0, args.max_outage)
    if args.notify_user:
        config.notify_user_id = args.notify_user.strip()
    ctx = MigrationContext(config, "fluxer", "backup", base_dir)
    ctx.on_notice = lambda text: log(re.sub(r"\[/?[a-z ]+\]", "", text))
    ctx.deadline = deadline
    if args.max_rate:
        ctx.writer.min_send_interval = 60.0 / args.max_rate

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: (log("Stop requested; finishing the current message boundary..."),
                                                  setattr(ctx, "is_running", False)))
        except NotImplementedError:   # Windows
            pass

    started = time.time()
    stop_txt = "none" if deadline is None else datetime.fromtimestamp(deadline).strftime("%Y-%m-%d %H:%M")
    log(f"Timed Waterfall: profile={args.profile} stop={stop_txt} max_rate={args.max_rate or 'unlimited'}/min")
    code = EXIT_ERROR
    await ctx.start_connections()
    ctx.notify(f"Timed Waterfall started (profile `{args.profile}`, stop {stop_txt}).", kind="info")
    try:
        v = await ctx.writer.validate()
        if not (v.get("token") and v.get("community")):
            log(f"Fluxer validation failed: {v}")
            return EXIT_ERROR
        ctx.ensure_state_initialized(str(config.fluxer_server_id or ""), "Fluxer")
        if args.fresh:
            ctx.state.clear_all_migration_data()
            log("State cleared (--fresh).")

        ctx.is_running = True
        if not args.no_clone:
            await clone_server.sync_channel_state(ctx)
            await clone_server.migrate_channels(ctx)
        if not ctx.state.channel_map:
            log("No channels are mapped to Fluxer; refusing to run (every message would be skipped).")
            return EXIT_ERROR

        verify: dict = {}
        if args.from_message:
            try:
                start_msg = await mm.find_start_message(ctx, args.from_message)
            except ValueError as e:
                log(str(e))
                return EXIT_USAGE
            after_id = int(args.from_message) - 1
            verify = {"verify_server": True, "start_ts": start_msg.created_at.timestamp()}
            log(f"Starting at message {args.from_message}: every message from there on is checked against the Fluxer "
                f"server and only the ones that are missing get sent (the database's progress is ignored).")
        else:
            after_id = ctx.state.get_waterfall_cursor()
            log(f"Resuming after source message ID {after_id}" if after_id else "Starting from the beginning.")

        total = None
        if not args.no_count:
            log("Counting remaining messages...")
            total = (await mm.analyze_global_migration(ctx, after_message_id=after_id, ignore_progress=bool(verify)))["messages"]
            log(f"{total} messages remaining.")

        t0 = time.time()
        from src.core.notify import ProgressReporter
        reporter = ProgressReporter(
            ctx.notify, total, lambda: (60.0 / ctx.writer.min_send_interval) if ctx.writer.min_send_interval else 0.0,
            title="Timed Waterfall", interval_minutes=args.report_interval)

        async def progress(st):
            reporter.update(st)
            n = st["messages"]
            if n and n % args.report_every == 0:
                rate = n / max(1e-6, time.time() - t0)
                eta = f", ~{fmt_dur((total - n) / rate)} left at this rate" if total and rate > 0 else ""
                log(f"sent {n}{f'/{total}' if total else ''}  ({rate * 60:.0f}/min{eta})  "
                    f"cursor={ctx.state.get_waterfall_cursor()}")

        ctx.writer.on_rate_limit = lambda secs: log(f"rate limited: pausing {secs:.1f}s")
        reporter.start()
        try:
            res = await mm.migrate_global_messages(ctx, after_message_id=after_id, progress_callback=progress, **verify)
        finally:
            await reporter.stop()
        if res.get("already_on_server"):
            log(f"{res['already_on_server']} message(s) were already on the server and were not sent again.")

        sent = res["messages"]
        kind, text = __import__("src.core.notify", fromlist=["describe_result"]).describe_result(
            "Timed Waterfall", res, time.time() - started)
        ctx.notify(text, kind=kind, with_logs=(kind == "error"))
        if res.get("skipped"):
            log(f"{res['skipped']} message(s) were SKIPPED after repeated errors (marker posted in the channel): "
                f"{', '.join(res.get('skipped_ids', [])[:20])}. Full list: scripts/list_skipped.py --profile {args.profile}")
        rate = sent / max(1e-6, time.time() - t0)
        cursor = ctx.state.get_waterfall_cursor()
        if res.get("error"):
            log(f"ERROR after {sent} messages this session: {res['error']}")
            log("That message was NOT marked migrated; the next run retries it.")
            code = EXIT_ERROR
        elif res.get("stopped") == "deadline" or not ctx.is_running:
            reason = "scheduled stop time reached" if res.get("stopped") == "deadline" else "stopped by signal"
            left = f"~{max(0, total - sent)} messages left" if total is not None else "work remains"
            eta = f", ~{fmt_dur((total - sent) / rate)} more at this rate" if total and rate > 0 else ""
            log(f"Paused ({reason}): sent {sent} this session in {fmt_dur(time.time() - started)}; {left}{eta}. cursor={cursor}")
            code = EXIT_MORE
        else:
            log(f"DONE: sent {sent} this session in {fmt_dur(time.time() - started)}. cursor={cursor}")
            code = EXIT_DONE
    finally:
        ctx.is_running = False
        await ctx.close_connections()
    return code


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", required=True, help="profile name (ReaperFiles-<name>/) or '.' for ./reaper_config.yaml")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--until", help="stop at HH:MM local time (next occurrence)")
    g.add_argument("--for", dest="duration", help="stop after this long, e.g. 9h, 90m, 9h30m, 45s")
    ap.add_argument("--max-rate", type=float, default=0, help="cap at N messages/minute (0 = as fast as Fluxer allows)")
    ap.add_argument("--max-attempts", type=int, default=None,
                    help="send attempts per message before it is skipped with a marker (default: profile setting, 5; 0 = never skip)")
    ap.add_argument("--max-outage", type=int, default=None,
                    help="stop (exit 1, nothing skipped) if Fluxer stays unavailable this many minutes "
                         "(default: profile setting, 0 = wait as long as it takes)")
    ap.add_argument("--notify-user", default=None,
                    help="Fluxer user ID the migration bot DMs about problems and the final summary "
                         "(default: notify_user_id in the profile)")
    ap.add_argument("--from-message", type=int, default=None, metavar="ID",
                    help="start at this source message ID (inclusive) and check every message from there on against the "
                         "Fluxer server, sending only the ones that are missing; use it to repair skipped messages")
    ap.add_argument("--report-interval", type=int, default=60, metavar="MIN",
                    help="minutes between progress DMs, on the wall clock (60 = the top of every hour); needs a "
                         "notify user. The first report is sent at the start.")
    ap.add_argument("--fresh", action="store_true", help="clear migration state first (re-sends everything!)")
    ap.add_argument("--no-clone", action="store_true", help="skip the channel clone/sync step")
    ap.add_argument("--no-count", action="store_true", help="skip the up-front remaining-message count (faster start)")
    ap.add_argument("--report-every", type=int, default=100, help="progress line every N messages")
    args = ap.parse_args()
    if args.from_message and args.fresh:
        ap.error("--from-message and --fresh can't be combined (--fresh wipes the progress it relies on)")
    try:
        if args.until:
            parse_until(args.until)
        if args.duration:
            parse_duration(args.duration)
    except ValueError as e:
        ap.error(str(e))

    _, base_dir = profile_paths(args.profile)
    Path(base_dir).mkdir(exist_ok=True)
    logging.basicConfig(level=logging.INFO, filename=str(Path(base_dir) / "timed-waterfall.log"), filemode="a",
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    sys.exit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
