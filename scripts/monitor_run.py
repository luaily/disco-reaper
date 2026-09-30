"""Read-only observer for a running DiscoReaper session (TUI or CLI): measures a full backup + migrate.

Start it in a second terminal, then run the backup and migration in the TUI as usual:

    ./venv/bin/python scripts/monitor_run.py --dir ReaperFiles-MyServer            # profile folder
    ./venv/bin/python scripts/monitor_run.py --dir . --interval 15 --out run1.csv  # ./reaper_config.yaml

It never touches the app. Every --interval seconds it samples:
  * progress   backup.db message count / media pool, migration DB mappings + waterfall cursor -> msgs/min per phase
  * rate limit the app's log (.reaper.log): Fluxer 429s, retry_after, global limits, 5xx, connection errors, give-ups
  * resources  the app process's RSS (incl. children) and system-wide network bytes down/up
  * reachability  a light GET to the Fluxer API from this same IP (every --probe-every s) to spot an IP-level block
Live lines + a CSV are written as it goes; Ctrl-C (or the app exiting) prints a summary and writes <out>.summary.txt.

Network counters are system-wide: run it on a dedicated host for clean bandwidth numbers.
"""
import argparse
import csv
import glob
import os
import re
import signal
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import psutil
import yaml

RL_RE = re.compile(r"Rate limited on \S+, retry in ([\d.]+)s", re.I)
GLOBAL_RE = re.compile(r"Global rate limit hit, pausing for ([\d.]+)s", re.I)
EVENTS = ("rl", "global", "5xx", "conn", "giveup", "halt", "deadline")


def parse_log_line(line: str):
    """-> (kind, seconds|None) or None. kinds: rl, global, 5xx, conn, giveup, halt, deadline."""
    m = GLOBAL_RE.search(line)
    if m:
        return "global", float(m.group(1))
    m = RL_RE.search(line)
    if m:
        return "rl", float(m.group(1))
    if "Server error" in line and "retrying" in line:
        return "5xx", None
    if "Connection error" in line:
        return "conn", None
    if "send failed after client retries" in line:
        return "giveup", None
    if "halted at message" in line:
        return "halt", None
    if "scheduled stop time" in line.lower():
        return "deadline", None
    return None


class LogTail:
    """Follows a log file, surviving truncation (TUI restarts with mode='w') and rotation."""

    def __init__(self, path: Path):
        self.path, self.pos, self.ino = Path(path), 0, None
        if self.path.exists():           # only count what happens from now on
            st = self.path.stat()
            self.pos, self.ino = st.st_size, st.st_ino

    def read_new(self) -> list[str]:
        try:
            st = self.path.stat()
        except FileNotFoundError:
            return []
        if self.ino is not None and (st.st_ino != self.ino or st.st_size < self.pos):
            self.pos = 0                 # rotated or truncated: start over
        self.ino = st.st_ino
        if st.st_size == self.pos:
            return []
        with open(self.path, "r", errors="replace") as f:
            f.seek(self.pos)
            data = f.read()
            self.pos = f.tell()
        return data.splitlines()


def load_cfg(d: Path) -> dict:
    p = d / "reaper_config.yaml"
    try:
        return yaml.safe_load(open(p)) or {}
    except Exception:
        return {}


def ro_query(path, sql):
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2)
        try:
            return con.execute(sql).fetchone()
        finally:
            con.close()
    except Exception:
        return None


def sample_db(d: Path, fluxer_id: str) -> dict:
    out = {"backup_msgs": None, "media_files": None, "media_bytes": None, "migrated": None, "cursor": None}
    for bdb in glob.glob(str(d / "DISCORD_BACKUP-*" / "backup.db")):
        r = ro_query(bdb, "SELECT COUNT(*) FROM messages")
        out["backup_msgs"] = (out["backup_msgs"] or 0) + (r[0] if r else 0)
        r = ro_query(bdb, "SELECT COUNT(*), COALESCE(SUM(size),0) FROM media_pool")
        if r:
            out["media_files"] = (out["media_files"] or 0) + r[0]
            out["media_bytes"] = (out["media_bytes"] or 0) + r[1]
    for sdb in glob.glob(str(d / f"*-{fluxer_id}.db")) if fluxer_id else []:
        r = ro_query(sdb, "SELECT COUNT(*) FROM message_mappings")
        r2 = ro_query(sdb, "SELECT COUNT(*) FROM thread_mappings")
        out["migrated"] = (r[0] if r else 0) + (r2[0] if r2 else 0)
        c = ro_query(sdb, "SELECT value FROM metadata WHERE key='waterfall_cursor'")
        out["cursor"] = c[0] if c else None
    return out


def probe(url: str, timeout: float = 10.0):
    """GET the API base from this IP. Any HTTP answer proves reachability; timeouts/resets/403/429 hint at a block."""
    t0 = time.time()
    req = urllib.request.Request(url, headers={"User-Agent": "DiscoReaper-monitor"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, int((time.time() - t0) * 1000)
    except urllib.error.HTTPError as e:
        return e.code, int((time.time() - t0) * 1000)
    except Exception as e:
        return f"ERR:{type(e).__name__}", int((time.time() - t0) * 1000)


def find_process(pid):
    if pid:
        return psutil.Process(pid)
    best = None
    for p in psutil.process_iter(["pid", "name", "cmdline", "memory_info"]):
        try:
            cmd = " ".join(p.info["cmdline"] or [])
            if "disco-reaper.py" in cmd or (p.info["name"] or "").lower().startswith("discoreaper") \
                    or "timed_waterfall.py" in cmd:
                if best is None or p.info["memory_info"].rss > best.info["memory_info"].rss:
                    best = p
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return best


def rss_mb(proc) -> float | None:
    try:
        total = proc.memory_info().rss
        for c in proc.children(recursive=True):
            try:
                total += c.memory_info().rss
            except psutil.Error:
                pass
        return total / 1e6
    except psutil.Error:
        return None


class Stats:
    def __init__(self):
        self.t0 = time.time()
        self.ev = {k: 0 for k in EVENTS}
        self.retry_after = []
        self.peak_rss = 0.0
        self.net_down = self.net_up = 0
        self.phase = {"backup": [0, 0.0], "migrate": [0, 0.0]}      # [messages, active seconds]
        self.clean = [0, 0.0]                                        # migrate windows with zero 429s
        self.probe_fail = []
        self.probe_n = 0
        self.first = {}
        self.last = {}

    def summary(self) -> str:
        el = time.time() - self.t0
        L = [f"=== Run summary ({el / 3600:.2f} h observed) ==="]
        for name, (n, secs) in self.phase.items():
            if n:
                L.append(f"{name:8s}: {n} messages in {secs / 60:.1f} active min -> {n / max(secs, 1e-9) * 60:.1f} msgs/min "
                         f"({n / max(secs, 1e-9):.2f}/s)")
        mig = self.phase["migrate"][0]
        L.append(f"Fluxer 429s: {self.ev['rl']} (+{self.ev['global']} global) "
                 f"= {(self.ev['rl'] + self.ev['global']) / max(mig, 1):.2f} per migrated message")
        if self.retry_after:
            ra = sorted(self.retry_after)
            L.append(f"retry_after: mean {sum(ra) / len(ra):.2f}s  median {ra[len(ra) // 2]:.2f}s  max {ra[-1]:.2f}s")
        L.append(f"5xx retries {self.ev['5xx']}, connection errors {self.ev['conn']}, client give-ups {self.ev['giveup']}, "
                 f"halts {self.ev['halt']}")
        if self.clean[1] > 0:
            L.append(f"429-free migrate windows: {self.clean[0]} msgs in {self.clean[1] / 60:.1f} min -> "
                     f"{self.clean[0] / self.clean[1] * 60:.1f} msgs/min  (candidate safe --max-rate)")
        L.append(f"peak app RSS: {self.peak_rss:.0f} MB" if self.peak_rss else "peak app RSS: n/a (app process not found)")
        L.append(f"network (system-wide): down {self.net_down / 1e9:.2f} GB, up {self.net_up / 1e9:.2f} GB "
                 f"-> avg {(self.net_down + self.net_up) * 8 / max(el, 1) / 1e6:.2f} Mbit/s")
        if self.probe_n:
            L.append(f"API reachability probes: {self.probe_n} sent, {len(self.probe_fail)} failed"
                     + (f" (first failure at +{(self.probe_fail[0][0] - self.t0) / 60:.0f} min: {self.probe_fail[0][1]})"
                        if self.probe_fail else ""))
        if self.last.get("media_files") is not None:
            L.append(f"backup media pool: {self.last['media_files']} unique files, {(self.last.get('media_bytes') or 0) / 1e9:.2f} GB")
        return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", default=".", help="profile folder (ReaperFiles-NAME) or '.' when reaper_config.yaml is in CWD")
    ap.add_argument("--log", default=".reaper.log", help="the app's log file (default ./.reaper.log)")
    ap.add_argument("--pid", type=int, help="app process ID (default: auto-detect disco-reaper.py / DiscoReaper)")
    ap.add_argument("--interval", type=float, default=30, help="seconds between samples")
    ap.add_argument("--probe-every", type=float, default=60, help="seconds between Fluxer reachability probes")
    ap.add_argument("--no-probe", action="store_true", help="don't send any requests to Fluxer")
    ap.add_argument("--out", default="monitor.csv", help="CSV output (summary goes to <out>.summary.txt)")
    a = ap.parse_args()

    d = Path(a.dir)
    cfg = load_cfg(d)
    fluxer_id = str(cfg.get("fluxer_server_id") or "")
    base = (cfg.get("fluxer_api_url") or "https://api.fluxer.app/v1").rstrip("/")
    if base == "default":
        base = "https://api.fluxer.app/v1"
    tail = LogTail(Path(a.log))
    proc = find_process(a.pid)
    print(f"watching: dir={d} log={a.log} process={'pid %d' % proc.pid if proc else 'NOT FOUND (RAM will be blank)'} "
          f"api={'off' if a.no_probe else base}", flush=True)

    st = Stats()
    stop = {"now": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(now=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(now=True))

    net0 = psutil.net_io_counters()
    prev_net = net0
    prev = sample_db(d, fluxer_id)
    st.first = dict(prev)
    last_t = time.time()
    last_probe = 0.0
    probe_txt, probe_status, probe_ms = "", "", ""

    cols = ["time", "backup_msgs", "migrated", "backup_per_min", "migrate_per_min", "rl", "global", "5xx", "conn", "giveup",
            "max_retry_after", "rss_mb", "down_mb_min", "up_mb_min", "probe_status", "probe_ms", "cursor"]
    f = open(a.out, "w", newline="")
    w = csv.writer(f)
    w.writerow(cols)

    while not stop["now"]:
        time.sleep(a.interval)
        now = time.time()
        dt = now - last_t
        last_t = now
        cur = sample_db(d, fluxer_id)

        win = {k: 0 for k in EVENTS}
        ras = []
        for line in tail.read_new():
            ev = parse_log_line(line)
            if ev:
                win[ev[0]] += 1
                if ev[1] is not None:
                    ras.append(ev[1])
        for k in EVENTS:
            st.ev[k] += win[k]
        st.retry_after += ras

        d_b = (cur["backup_msgs"] or 0) - (prev["backup_msgs"] or 0) if cur["backup_msgs"] is not None else 0
        d_m = (cur["migrated"] or 0) - (prev["migrated"] or 0) if cur["migrated"] is not None else 0
        d_b, d_m = max(d_b, 0), max(d_m, 0)
        if d_b:
            st.phase["backup"][0] += d_b
            st.phase["backup"][1] += dt
        if d_m:
            st.phase["migrate"][0] += d_m
            st.phase["migrate"][1] += dt
            if win["rl"] + win["global"] == 0:
                st.clean[0] += d_m
                st.clean[1] += dt
        prev = cur
        st.last = cur

        rss = rss_mb(proc) if proc else None
        if rss:
            st.peak_rss = max(st.peak_rss, rss)
        net = psutil.net_io_counters()
        down, up = net.bytes_recv - prev_net.bytes_recv, net.bytes_sent - prev_net.bytes_sent
        prev_net = net
        st.net_down += down
        st.net_up += up

        if not a.no_probe and now - last_probe >= a.probe_every:
            last_probe = now
            status, ms = probe(base)
            st.probe_n += 1
            probe_status, probe_ms = str(status), ms
            probe_txt = f"{status}/{ms}ms"
            if isinstance(status, str) or status in (403, 429):
                st.probe_fail.append((now, str(status)))
                probe_txt += "  <-- POSSIBLE IP BLOCK / LIMIT"
        mpm, bpm = d_m / dt * 60, d_b / dt * 60
        w.writerow([time.strftime("%H:%M:%S"), cur["backup_msgs"], cur["migrated"], f"{bpm:.1f}", f"{mpm:.1f}", win["rl"],
                    win["global"], win["5xx"], win["conn"], win["giveup"], f"{max(ras):.2f}" if ras else "",
                    f"{rss:.0f}" if rss else "", f"{down / dt * 60 / 1e6:.1f}", f"{up / dt * 60 / 1e6:.1f}",
                    probe_status, probe_ms, cur["cursor"]])
        f.flush()
        print(f"[{time.strftime('%H:%M:%S')}] backup {cur['backup_msgs']} (+{bpm:.0f}/min)  migrated {cur['migrated']} "
              f"(+{mpm:.0f}/min)  429s {win['rl']}+{win['global']}g  err {win['5xx'] + win['conn']}  "
              f"rss {rss and f'{rss:.0f}MB'}  net v{down / dt * 60 / 1e6:.0f}/^{up / dt * 60 / 1e6:.0f} MB/min  probe {probe_txt}",
              flush=True)
        if proc and not proc.is_running():
            print("app process exited; finishing.", flush=True)
            break
        if win["halt"] or win["deadline"]:
            print(f"note: run {'halted' if win['halt'] else 'reached its stop time'}", flush=True)

    f.close()
    text = st.summary()
    print("\n" + text)
    Path(a.out + ".summary.txt").write_text(text + "\n")
    print(f"\nCSV: {a.out}   summary: {a.out}.summary.txt")


if __name__ == "__main__":
    main()
