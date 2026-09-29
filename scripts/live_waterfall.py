"""Live Waterfall test harness against a throwaway Fluxer server.

  backup  back up the real (throwaway) Discord server to a local backup
  run     clone channels + run the Waterfall migration (headless, same code path as the TUI)
  verify  read messages back from Fluxer; report missing / duplicated / out-of-order source messages
"""
import argparse
import asyncio
import json
import logging
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.core.base import MigrationContext  # noqa: E402
from src.core.configuration import AppConfig  # noqa: E402


def load(path):
    with open(path, "rb") as f:
        return tomllib.load(f)


def make_ctx(cfg, mode):
    d, f, r = cfg["discord"], cfg["fluxer"], cfg.get("run", {})
    for sec, keys in (("discord", ("bot_token", "server_id")), ("fluxer", ("bot_token", "server_id"))):
        for k in keys:
            if not cfg[sec].get(k):
                sys.exit(f"Fill in [{sec}] {k} in livetest.toml first.")
    conf = AppConfig(discord_bot_token=d["bot_token"], discord_server_id=str(d["server_id"]),
                     tool_mode="backup_transfer", target_platform="fluxer",
                     fluxer_bot_token=f["bot_token"], fluxer_server_id=str(f["server_id"]),
                     fluxer_api_url=f.get("api_url", "default"),
                     anonymize_users=r.get("anonymize_users", False), log_level=r.get("log_level", "INFO"))
    return MigrationContext(conf, "fluxer", mode, str(ROOT / r.get("work_dir", "livetest-work")))


async def cmd_backup(cfg, args):
    """Real Discord -> local backup (same exporter calls as the TUI's full backup)."""
    from src.core.exporter import DiscordExporter
    ctx = make_ctx(cfg, "live")
    work = ROOT / cfg.get("run", {}).get("work_dir", "livetest-work")
    work.mkdir(exist_ok=True)
    reader = ctx.discord_reader
    await reader.start()
    try:
        exp = DiscordExporter(reader, base_dir=work)
        await exp.setup()
        await exp.export_metadata()
        await exp.download_server_assets()
        await exp.export_channels_structure()
        await exp.export_roles()
        await exp.export_assets()
        await exp.prefetch_members()
        chans = [c for c in await reader.get_channels()
                 if c.type in (reader.CHANNEL_TYPE_TEXT, reader.CHANNEL_TYPE_NEWS, reader.CHANNEL_TYPE_FORUM)]
        print(f"backing up {len(chans)} channels -> {exp.export_path}")
        acc = (0, 0, 0)
        for c in chans:
            async def cb(name, count, **kw):
                pass
            acc = await exp.export_channel_messages(c.id, progress_callback=cb, force=args.force,
                                                    accumulated_count=acc[0], accumulated_threads=acc[1],
                                                    accumulated_files=acc[2])
            print(f"  #{c.name}: running totals messages/threads/files = {acc}")
        await exp.export_metadata()
    finally:
        await reader.close()



# ── fault injection ────────────────────────────────────────────────────────
# Fakes real Fluxer 429 bodies ({"code":"RATE_LIMITED","message","retry_after","global"}) at the aiohttp layer,
# so the fluxer library's own retry/global-lock handling AND our recovery code are both exercised.
# Windows are over the Nth message-send HTTP call (library retries count as calls).
INJECT_PLAN = {
    "short":     [(3, 4, 2.0, False)],    # two 429s, 2s each: the library rides these out by itself
    "global":    [(8, 8, 3.0, True)],     # one GLOBAL 429 (3s)
    "sustained": [(12, 17, 0.5, False)],  # 6 in a row: outlasts the library's 4 retries -> RuntimeError -> our pause+retry
    "halt":      [(20, 10**9, 0.3, False)],  # never clears: our recovery must give up, halt, and NOT mark the message
}


def install_injection(names):
    import aiohttp
    windows = [w for n in names for w in INJECT_PLAN[n]]
    calls = {"n": 0}
    orig = aiohttp.ClientSession.request

    class _Resp:
        status = 429
        headers = {}

        def __init__(self, body):
            self._body = body

        async def json(self):
            return self._body

    class _Ctx:
        def __init__(self, body):
            self.body = body

        async def __aenter__(self):
            return _Resp(self.body)

        async def __aexit__(self, *a):
            return False

    def patched(self, method, url, **kw):
        u = str(url)
        if method.upper() == "POST" and ("/webhooks/" in u or u.endswith("/messages")):
            calls["n"] += 1
            for lo, hi, secs, glob in windows:
                if lo <= calls["n"] <= hi:
                    print(f"  [inject] send-call #{calls['n']}: 429 retry_after={secs}s global={glob}", flush=True)
                    return _Ctx({"code": "RATE_LIMITED", "message": "injected", "retry_after": secs, "global": glob})
        return orig(self, method, url, **kw)

    aiohttp.ClientSession.request = patched


async def cmd_run(cfg, args):
    from src.fluxer import clone_server, migrate_message as mm
    import src.fluxer.writer as wr
    if args.inject:
        names = args.inject.split(",")
        install_injection(names)
        if "halt" in names:
            wr._MAX_RECOVERY_ROUNDS = 2   # give up quickly instead of after ~5 minutes
        print(f"fault injection ON: {names}")
    ctx = make_ctx(cfg, "backup")
    await ctx.start_connections()
    try:
        v = await ctx.writer.validate()
        print("validate:", {k: v.get(k) for k in ("token", "community", "bot_name", "community_name")})
        ctx.ensure_state_initialized(str(cfg["fluxer"]["server_id"]), "Fluxer")
        if args.fresh:
            ctx.state.clear_all_migration_data()
            print("state cleared (fresh run)")
        ctx.is_running = True   # the clone/migrate loops bail out immediately when this is False
        await clone_server.sync_channel_state(ctx)
        cloned = await clone_server.migrate_channels(ctx)
        print("clone:", {k: v for k, v in (cloned or {}).items() if k != "structure"})
        n_map = len(ctx.state.channel_map)
        print(f"mapped channels: {n_map}")
        if not n_map:
            sys.exit("No channels are mapped to Fluxer; refusing to run (every message would be skipped).")

        after_id = ctx.state.get_waterfall_cursor() if args.resume else None
        print(f"waterfall start: after_id={after_id}")
        ctx.writer.on_rate_limit = lambda secs: print(f"  ** rate limited: pausing {secs:.1f}s **", flush=True)
        limit = args.stop_after

        async def progress(st):
            if st["messages"] % 10 == 0:
                print(f"  sent {st['messages']}  cursor={ctx.state.get_waterfall_cursor()}", flush=True)
            if limit and st["messages"] >= limit:
                ctx.is_running = False   # simulate the user pressing Cancel

        res = await mm.migrate_global_messages(ctx, after_message_id=after_id, progress_callback=progress)
        print(json.dumps({k: res[k] for k in ("messages", "threads", "attachments")} | {"error": res.get("error")}))
        print("final cursor:", ctx.state.get_waterfall_cursor())
    finally:
        ctx.is_running = False
        await ctx.close_connections()


async def cmd_verify(cfg, args):
    """Compare the backup (source of truth) with what is actually on Fluxer."""
    ctx = make_ctx(cfg, "backup")
    await ctx.start_connections()
    try:
        await ctx.writer.validate()
        ctx.ensure_state_initialized(str(cfg["fluxer"]["server_id"]), "Fluxer")
        rd, st = ctx.discord_reader, ctx.state
        ok_types = {rd.MESSAGE_TYPE_DEFAULT, rd.MESSAGE_TYPE_REPLY, rd.MESSAGE_TYPE_THREAD_STARTER, rd.MESSAGE_TYPE_FORWARD,
                    rd.MESSAGE_TYPE_CHAT_INPUT_COMMAND, rd.MESSAGE_TYPE_CONTEXT_MENU_COMMAND,
                    rd.MESSAGE_TYPE_POLL_RESULT, rd.MESSAGE_TYPE_AUTO_MODERATION_ACTION}
        expected, mapped_ids, order = [], {}, []   # mapped_ids: target channel -> {target msg ids}
        unmapped_channel = 0
        async for m in rd.fetch_global_message_history():
            if m.type not in ok_types or not m.channel or not (m.content or m.attachments or m.stickers):
                continue
            tgt = st.get_target_channel_id(str(m.channel.id))
            if not tgt:
                unmapped_channel += 1
                continue
            expected.append(m)
            tid = st.get_target_message_id(tgt, str(m.id))
            if tid:
                mapped_ids.setdefault(str(tgt), set()).add(str(tid))
                order.append((m.id, int(tid)))
        # Fetch what is really on Fluxer
        on_fluxer = {}
        for tgt in set(str(v) for v in st.channel_map.values()):
            ids, before = set(), None
            while True:   # page backwards from the newest message
                page = await ctx.writer.client.get_messages(tgt, limit=100, before=before) if before \
                    else await ctx.writer.client.get_messages(tgt, limit=100)
                if not page:
                    break
                ids |= {str(x["id"]) for x in page}
                if len(page) < 100:
                    break
                before = min(int(x["id"]) for x in page)
            on_fluxer[tgt] = ids
        n_mapped = sum(len(v) for v in mapped_ids.values())
        ghost = sum(len(ids - on_fluxer.get(t, set())) for t, ids in mapped_ids.items())  # marked sent, not on Fluxer
        extra = sum(len(on_fluxer[t] - mapped_ids.get(t, set())) for t in on_fluxer)       # on Fluxer, not in our map
        not_sent = len(expected) - n_mapped
        disorder = sum(1 for i in range(1, len(order)) if order[i][1] < order[i - 1][1])
        print(f"sendable source messages: {len(expected)} (+{unmapped_channel} in unmapped channels)")
        print(f"marked as migrated:       {n_mapped}    not migrated: {not_sent}")
        print(f"GHOSTS (marked sent, absent on Fluxer): {ghost}")
        print(f"extra on Fluxer (dupes/markers/other):  {extra}")
        print(f"out-of-order vs global chronology:      {disorder}")
        print(f"waterfall cursor: {st.get_waterfall_cursor()}")
        sys.exit(0 if not (ghost or disorder) else 1)
    finally:
        await ctx.close_connections()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(ROOT / "livetest.toml"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    bk = sub.add_parser("backup")
    bk.add_argument("--force", action="store_true", help="re-download channels already in the backup")
    r = sub.add_parser("run")
    r.add_argument("--fresh", action="store_true", help="clear migration state first")
    r.add_argument("--resume", action="store_true", help="continue from the waterfall cursor")
    r.add_argument("--inject", default="", help="comma list of: short,global,sustained,halt (fake 429s)")
    r.add_argument("--stop-after", type=int, default=0, help="cancel after N messages (to test resume)")
    sub.add_parser("verify")
    args = ap.parse_args()
    cfg = load(args.config)
    logging.basicConfig(level=getattr(logging, cfg.get("run", {}).get("log_level", "INFO")),
                        filename=str(ROOT / "livetest.log"), filemode="w")
    if args.cmd == "backup":
        asyncio.run(cmd_backup(cfg, args))
    elif args.cmd == "run":
        asyncio.run(cmd_run(cfg, args))
    else:
        asyncio.run(cmd_verify(cfg, args))


if __name__ == "__main__":
    main()
