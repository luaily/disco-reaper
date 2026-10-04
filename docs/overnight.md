# Overnight / timed Waterfall (Fluxer)

Large servers take days at Fluxer's real webhook rate (~1 message/second measured, so ~4,000/hour — a 198k-message
server is roughly 50 hours). `scripts/timed_waterfall.py` runs the Waterfall headless and **stops cleanly at a set time**;
the next run resumes from the saved cursor. Run it nightly until it reports `DONE`.

Requires: a local backup of the Discord server (Backup mode) and a profile whose `reaper_config.yaml` has the Fluxer
token and community ID. Fluxer targets only.

## From the TUI
Migrate tab → **Waterfall Migration** (or **Migrate Messages** for a single channel) → choose *Start From Beginning* / *Continue* → the **Run Options** dialog asks for:

- **Stop at / run for:** `07:00` (24h, next occurrence) or a duration such as `9h`, `90m`, `9h30m`, `45s`. Blank = no limit.
- **Max speed:** messages per minute. Blank = as fast as Fluxer allows.

Press **Start** (or Enter). The log shows the active options, and the status line shows live `msgs/min · ~time left`.
At the stop time the run pauses ("Paused at the scheduled stop time") and *Continue* resumes exactly where it stopped.
*Back* in the dialog cancels without changing anything (for a single channel it returns to the channel picker). Fluxer only. The TUI must stay open for the whole run;
for unattended overnight runs use the command line below with cron/launchd.

## Command line

```bash
# stop at 07:00 local time (tomorrow if it is already past 07:00)
./venv/bin/python scripts/timed_waterfall.py --profile MyServer --until 07:00

# or a duration; optionally cap the speed to leave headroom (messages per minute)
./venv/bin/python scripts/timed_waterfall.py --profile MyServer --for 9h --max-rate 40

# profile "." = ./reaper_config.yaml in the current directory
./venv/bin/python scripts/timed_waterfall.py --profile . --for 45s
```

| Option | Meaning |
|---|---|
| `--profile NAME` | uses `ReaperFiles-NAME/reaper_config.yaml` and that folder's backup and state (`.` = current directory) |
| `--until HH:MM` / `--for 9h30m` | stop time (mutually exclusive; `--for` also accepts `90m`, `45s`). Omit for no limit |
| `--max-rate N` | cap at N messages/minute (0 = as fast as Fluxer allows) |
| `--fresh` | **clear migration state and re-send everything** |
| `--no-clone` | skip the channel clone/sync step |
| `--no-count` | skip the up-front "remaining messages" count (faster start; no ETA) |
| `--report-every N` | progress line every N messages (default 100) |

Progress lines look like `sent 300/38000  (54/min, ~11h40m left at this rate)  cursor=...`; every rate-limit pause is
logged. A log is also appended to `<profile folder>/timed-waterfall.log`.

## How it stops
- Stopping happens **between messages**. If a rate-limit wait is in progress at the stop time it is abandoned, and that
  message is **not** marked as migrated — the next run sends it. Nothing is skipped or duplicated.
- Ctrl-C and SIGTERM stop the same way.
- Progress is the *waterfall cursor* in the migration database. Move/copy the profile folder and the cursor goes with it.

## Exit codes (for schedulers)
| Code | Meaning | Scheduler action |
|---|---|---|
| `0` | everything migrated | stop scheduling |
| `10` | paused at the stop time / by signal; work remains | run again next night |
| `1` | error (message that failed was not marked; next run retries it) | alert someone |
| `2` | bad usage / config / unsupported platform | fix config |

## Scheduling examples

**cron** (macOS/Linux; 23:00 start, stops itself at 07:00):
```cron
0 23 * * *  cd /path/to/disco-reaper && ./venv/bin/python scripts/timed_waterfall.py --profile MyServer --until 07:00 >> overnight.log 2>&1
```

**launchd** (macOS) — `~/Library/LaunchAgents/com.discoreaper.overnight.plist`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>com.discoreaper.overnight</string>
  <key>WorkingDirectory</key><string>/path/to/disco-reaper</string>
  <key>ProgramArguments</key><array>
    <string>/path/to/disco-reaper/venv/bin/python</string>
    <string>scripts/timed_waterfall.py</string>
    <string>--profile</string><string>MyServer</string>
    <string>--until</string><string>07:00</string>
  </array>
  <key>StartCalendarInterval</key><dict><key>Hour</key><integer>23</integer><key>Minute</key><integer>0</integer></dict>
  <key>StandardOutPath</key><string>/path/to/disco-reaper/overnight.log</string>
  <key>StandardErrorPath</key><string>/path/to/disco-reaper/overnight.log</string>
</dict></plist>
```
Load with `launchctl load ~/Library/LaunchAgents/com.discoreaper.overnight.plist`. Make sure the machine is awake
(`caffeinate -i` or Energy settings) and the network stays up.

## Notes
- A send that times out with **no** rate limit active has unknown delivery; the run halts (exit 1) and the retry may
  post one duplicate.
- Don't run two instances against the same profile at once.
- Test first with a short window against a throwaway server, e.g. `--for 45s`, then `--profile . --for 10m` to finish.

## Measuring a full run (`scripts/monitor_run.py`)
A read-only observer for a running TUI or CLI session. Start it in a second terminal, then do the backup and migration as usual:

```bash
./venv/bin/python scripts/monitor_run.py --dir ReaperFiles-MyServer --interval 30 --out run1.csv
```

It samples every `--interval` seconds and never touches the app:

| Measures | From |
|---|---|
| backup and migrate **msgs/min** per phase | `backup.db` message count; migration DB mappings + waterfall cursor |
| Fluxer **429s**, `retry_after`, global limits, 5xx, connection errors, client give-ups | the app's `.reaper.log` (`--log` to change) |
| **429s per migrated message** and the msgs/min of 429-free windows (a candidate safe `--max-rate`) | derived |
| peak **RAM** of the app process (incl. children) | `psutil` (auto-detects `disco-reaper.py` / `DiscoReaper`, or `--pid`) |
| **bandwidth** down/up | system-wide network counters (use a dedicated host for clean numbers) |
| **reachability** of the Fluxer API from this IP, every `--probe-every` s (`--no-probe` to disable) | one light GET; a timeout/reset/403/429 is flagged `POSSIBLE IP BLOCK / LIMIT` |

Ctrl-C (or the app exiting) prints a summary and writes `<out>.summary.txt`; the per-sample CSV is written as it goes.

To find a rate that avoids 429s entirely, run the migration with a *Max speed* in the Run Options dialog (e.g. 45, then 55, then 65 msgs/min) and compare "429s per migrated message" — the highest cap that stays near zero is your safe ceiling. Fewer 429s means less hammering of the API.

## When Fluxer has a problem (503s, timeouts)
If Fluxer stops accepting messages the run **holds**: it says why, waits (15s, 30s, 1m, 2m, then every 5 min), checks that
the API answers again, and retries the *same* message. Nothing is counted against the message and nothing is skipped; the
first message after the outage is read back from the channel to confirm it arrived. By default it waits as long as it takes
(use the TUI's Cancel or Ctrl-C to stop). To make an unattended run give up instead, set "Stop if Fluxer is down for (min)"
on the Configuration screen, `max_outage_minutes` in `reaper_config.yaml`, or `--max-outage N` on `timed_waterfall.py`
(it then exits with code 1, still without skipping anything).

### One bad message can't stall the run
If a single message keeps failing while Fluxer otherwise looks healthy (suspected cause: some links or embeds), the run does
not wait on it forever: it retries it without embeds and with link previews suppressed, then with its links shown as plain
code, and finally sends a tiny test message to check that sending works at all. If that works the message is skipped with
the usual marker; if it does not, it is treated as an outage and the run keeps holding. The log and the DM say what was risky
about the message (link hosts, embeds, length).

## Messages that keep failing
A send that fails for a reason about *that message* is retried, and after **5 attempts** the message is
skipped: the bot posts a marker in the channel ("There was an error migrating message `id` … skipping...") and the run
carries on, so one bad message can't stall an overnight run. Change the limit on the Configuration screen ("Max send
attempts per message"), in `reaper_config.yaml` (`max_message_attempts`) or with `--max-attempts N`; `0` = never skip,
stop at the first failure. List what was skipped with:

```bash
./venv/bin/python scripts/list_skipped.py --profile MyServer
```
Upload timeouts grow with the file size, and a send that timed out is looked for in the channel before it is retried,
so large attachments no longer fail on a fixed 45s limit and a retry won't post a duplicate.

## Getting told about problems (no terminal needed)
Set your **Fluxer user ID** under "DM me problems (Fluxer user ID)" on the Configuration screen, in `reaper_config.yaml` as
`notify_user_id`, or pass `--notify-user ID` to `timed_waterfall.py`. The bot that is running the migration then DMs you when
Fluxer has been down for more than 2 minutes (and when it is back), when a message is skipped, when the run halts, and with a
summary at the end. It uses the same bot, so there is nothing else to set up; you only need to share the community with it
and allow DMs. (Find your ID by enabling Developer Mode and copying your user ID.)

### Progress reports
With a notify user set, the bot also DMs a **start report** (messages to send, the set send speed, estimated time remaining)
and then a report **at the top of every hour**: sent so far, remaining, the set send speed, the average real send speed and
the estimated time remaining. Reports keep arriving while the run is paused, so no news is never "unknown". Change the
cadence with `--report-interval MIN` on `timed_waterfall.py` (`60` = top of the hour, `30` = on the hour and half hour).

## Repairing messages that were skipped or lost
To go back and fix a range, start from a message and let the server decide what is missing:

```bash
./venv/bin/python scripts/timed_waterfall.py --profile MyServer --from-message 1349205131891183747
```
In the TUI use Waterfall → **Start from message ID**. Every message from that one onward (inclusive) is checked against the
Fluxer channel itself, not the database; the ones already there are left alone, stale "error migrating" markers for messages
that are there are deleted, and only the missing ones are sent (at the end of their channel, with the original date in the
prefix). It can be repeated safely. Use `scripts/list_skipped.py` to see what was skipped.
