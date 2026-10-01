"""List messages the migration skipped after repeated send errors (they also got a marker posted in the channel).

  ./venv/bin/python scripts/list_skipped.py --profile MyServer
  ./venv/bin/python scripts/list_skipped.py --profile . --state-db path/to/Name-<fluxer id>.db

Skipped messages are recorded in the migration database's `skipped_messages` table (source message ID, channel,
author, last error, attempts). The number of tries before skipping is the "Max send attempts per message" setting.
"""
import argparse
import glob
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.core.configuration import load_config  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", required=True, help="profile name (ReaperFiles-NAME) or '.' for ./reaper_config.yaml")
    ap.add_argument("--state-db", help="explicit migration database (default: <profile>/*-<fluxer server id>.db)")
    a = ap.parse_args()
    base = Path(".") if a.profile == "." else Path(f"ReaperFiles-{a.profile}")
    if a.state_db:
        db = Path(a.state_db)
    else:
        cfg = load_config(base / "reaper_config.yaml", create_if_missing=False)
        found = glob.glob(str(base / f"*-{cfg.fluxer_server_id}.db"))
        if not found:
            raise SystemExit(f"No migration database found in {base} for server {cfg.fluxer_server_id}")
        db = Path(found[0])
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute("SELECT * FROM skipped_messages ORDER BY source_id").fetchall()
    except sqlite3.OperationalError:
        rows = []
    if not rows:
        print("No skipped messages.")
        return
    print(f"{len(rows)} skipped message(s) in {db.name}:")
    for r in rows:
        print(f"  {r['source_id']}  channel {r['channel_id']}  by {r['author']}  after {r['attempts']} attempts  ({r['skipped_at'][:19]})")
        print(f"      {r['reason']}")


if __name__ == "__main__":
    main()
