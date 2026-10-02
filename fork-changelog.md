# Fork Changelog

Changes in this fork relative to upstream Disco Reaper (`V4-main` @ `7fd487a`, "add support for stoat custom instance").
This release focuses on **Fluxer rate limiting** and the **Waterfall resume** bugs it exposed. Stoat sending is unchanged.

Status: the rate-limit fixes were merged in PR #2. Everything under **Unreleased** is verified against a live Fluxer community (each entry lists what was run) and covered by unit tests: 132 pass, plus the 4 pre-existing `tests/test_database.py` failures noted under Testing.

---

## Unreleased — Timed / overnight Waterfall and robustness

At a glance (details below, newest first):
- **Progress DMs** at the start and at the top of every hour: sent / remaining / set speed / average real speed / ETA.
- **DM notifications** from the migration bot (outages, skips, halts, summary), no extra bot or webhook.
- **Start the Waterfall from any message**, checking the Fluxer *server* (not the database) so only missing messages are sent; stale skip markers are removed.
- **Outages hold the run** (pause, probe, retry the same message, verify) instead of skipping messages.
- **Big attachments:** 50 MiB limit handled (left out with a note), no more silent drops, no more 413s.
- **Presigned uploads** end the 503s / `(delivery unknown)` timeouts on media-heavy messages.
- **Retry then skip** for failures about a single message, with a marker, a record and `list_skipped.py`.
- **Saved media links** (refreshed + de-duplicated), **native replies**, **run options** (stop time / rate cap), the **timed runner**, **monitor**, and the `File` object fix.

Large servers take days at Fluxer's real rate (~1 msg/s measured; a 198k-message server ≈ 50h), so the Waterfall can now run in a nightly window and resume where it stopped. See [docs/overnight.md](docs/overnight.md).

### Added
- **TUI run options (Waterfall *and* per-channel Migrate Messages, Fluxer):** after you choose *Start From Beginning* / *Continue* (per-channel: also *Start from message ID*), a **Run Options** dialog asks for an optional **stop time** (`07:00` or a duration like `9h`, `90m`, `45s`) and a **max speed** (messages/minute). Leave both blank to run until finished. It appears *before* anything is cleared, so *Back* never wipes progress (per-channel: Back returns to the channel picker). While running, the log shows the active options and the item status shows live `msgs/min · ~ETA left`. At the stop time the run pauses cleanly (status **Stopped**, "Paused at the scheduled stop time…", audit-log entry) and *Continue* resumes exactly where it stopped.
- **`scripts/monitor_run.py`** — read-only observer for a running TUI/CLI session: per-phase msgs/min, Fluxer 429 counts and `retry_after`, 429s per migrated message, 429-free throughput (candidate safe `--max-rate`), peak RAM, bandwidth, and a Fluxer reachability probe that flags a possible IP block. CSV + summary. Tests for its log parsing and log-follow logic.
- **Shared parsing:** `parse_until` / `parse_duration` / `fmt_dur` / `parse_stop_spec` moved into `src/core/utils.py`, used by both the TUI dialog and `scripts/timed_waterfall.py`.
- **`scripts/timed_waterfall.py`** — headless Waterfall for real profiles: `--profile`, `--until HH:MM` / `--for 9h30m` (also `90m`, `45s`), `--max-rate N` (messages/minute), `--fresh`, `--no-clone`, `--no-count`, `--report-every`. Logs rate, ETA and every rate-limit pause; appends to `<profile>/timed-waterfall.log`. SIGINT/SIGTERM stop cleanly. Exit codes: `0` done, `10` paused with work left, `1` error, `2` usage/config — usable from cron/launchd.
- **Run deadline:** `MigrationContext.deadline` / `deadline_reached()`. The Waterfall and per-channel loops stop **between messages** at the deadline (`stats["stopped"] == "deadline"`). A rate-limit wait or pacing sleep in progress at the deadline is abandoned and that message is **left unmarked**, so the next run sends it — nothing skipped, nothing duplicated.
- **Rate cap:** `FluxerWriter.min_send_interval` (seconds between sends), enforced by `_pace()`; abortable on cancel/deadline. Exposed as `--max-rate`.
- **`docs/overnight.md`** — usage, exit codes, cron and launchd examples.
- **Tests:** `tests/test_timed_waterfall.py` (parsing, the TUI dialog, deadline stop between messages, send error at the deadline is a clean stop and unmarked, error before the deadline is still an error, pacing and its cancel).

### Added — progress reports by DM (at the start and every hour on the hour)
With a notify user set (see below), the bot sends a **start report** — messages to send, the set send speed (the cap, or "no cap (as fast as Fluxer allows)") and the estimated time remaining (from the cap; uncapped it assumes ~55 msgs/min and says so) — and then a report **at the top of every hour** (wall clock) with: messages sent so far (plus any already on the server / skipped), messages remaining, the set send speed, the **average real send speed** (and the last hour's), and the estimated time remaining at the average real speed. Reports keep coming while the run is paused (for example during an outage) and then say "nothing sent yet" / "unknown until messages are going out" instead of guessing. TUI (Waterfall and per-channel) and `timed_waterfall.py` (`--report-interval MIN`, default 60). Verified live with a 60 msgs/min cap and a 1-minute interval: start report, reports at 21:14 and 21:15 (44 sent/62 left, then 100 sent/6 left, ~59.8 then 57.6 msgs/min), and the final summary. 9 new tests.

### Added — DM notifications from the migration bot
Set **"DM me problems (Fluxer user ID)"** on the Configuration screen (`notify_user_id` in `reaper_config.yaml`, `--notify-user` on the timed runner) and the bot that is already running the migration sends that user a direct message when something needs attention — no new bot, webhook or token (the user just needs to share the community with the bot). Sent: Fluxer down for more than 2 minutes (once per 30 minutes) and back again, skipped messages (throttled), a halted run and unexpected per-message errors (with the last few warnings attached), and an end-of-run summary (the timed runner also sends a "started" message). Webhook tokens are always redacted. Notifications are sent in the background, throttled per kind, and a failure to deliver one (DMs closed, API down) is logged once and never affects the migration. Verified live: start + summary DMs from two runs arrived in the owner's DMs; 9 new tests.

### Added — start the Waterfall from a chosen message, checking the *server* (repairs skipped messages)
**TUI:** Waterfall → **Start from message ID**. **CLI:** `timed_waterfall.py --from-message ID`. Starting at that source message (inclusive), every message is looked up on the Fluxer channel itself — **not** in the database — and only the ones that really are missing are sent. How a migrated message is recognised: the webhook name `<name> (discord)` plus the epoch in its `-# <t:EPOCH:D>` prefix (copies are counted, so two messages from one person in the same second need two copies; replies sent through the bot by older builds are recognised too). Messages found are adopted (the mapping is repaired from the server so replies/links resolve), the bot's stale "error migrating message … skipping" marker for a message that is now there is **deleted** and its skip record cleared, and anything still missing is posted (at the end of its channel, with its original date in the prefix). Each channel is read newest-first only until well past the starting message, and an unreadable channel halts the run instead of sending blind, so it can't create duplicates. Re-running is idempotent.
- **Progress can no longer move backwards:** per-channel/thread `last_msg_id`/`last_msg_ts` and the Waterfall cursor only move forward, so replaying from an older message can never make a later normal resume send things twice.
- Verified live: starting at an old synthetic message found all 3 later messages already on the server (0 sent, cursor unchanged), deleted the stale marker and cleared the skip record; adding one never-sent message and starting just before it sent exactly that one; a repeat run sent nothing. 13 new tests.

### Changed — an outage now HOLDS the run instead of skipping messages
Seen live: `Server error 503 …` and `Message … failed (Send timed out after 45s (delivery unknown)); attempt 1/5 … 2/5 … 3/5`. Every failure counted against the message, so during an outage the run burned through its attempts, **skipped the message and moved on to the next one, which failed the same way** — errors stacked up and messages were skipped for no fault of their own (the API could even answer other requests while the send route was down).
- **Two kinds of failure.** The writer now raises `ServiceUnavailable` when the *service* is struggling (503/5xx, timeouts, connection errors, rate limits or uploads that never clear). That says nothing about the message, so it **never counts and never skips**. Everything else is a failure about the message and keeps the retry-then-skip behaviour.
- **The run holds until Fluxer answers.** On `ServiceUnavailable` the migration pauses, tells you why ("Fluxer isn't accepting messages (…). Paused for 2m10s; checking again in 1m. Message `id` is waiting, nothing is skipped."), backs off 15s → 30s → 60s → 2m → 5m (repeating), probes the API in between (`check_health`: a light GET of the channel and its webhook) and retries **the same message** when it answers.
- **Verified before moving on.** The first message sent after an outage is read back from the channel; if Fluxer says it isn't there (404) it is sent again.
- **Optional stop condition.** `max_outage_minutes` (Configuration screen, `reaper_config.yaml`, `--max-outage` on the timed runner; default 0 = wait as long as it takes) stops the run after that long down — still without skipping and with the message unmarked, so a resume retries it (timed runner exits 1).
- A deliberate **Cancel no longer shows up as an error**.
- Verified live with 56 injected 503s spanning more than ten failed attempts on one message: the run held on that message, paused/retried with notices, then carried on; **0 skipped, 0 errors, 105/105 migrated, 0 ghosts, 0 out of order**. 12 new tests. `skip after N` (default 5) now applies only to failures about the message itself.

### Fixed — `413` on big attachments, and messages disappearing silently
Seen on a long run: `Connection error: 413, message='Attempt to decode JSON with unexpected mimetype: text/html'` on `/webhooks/…`, then a run of failures and skips.
- **Cause.** Fluxer refuses any single file over **50 MiB** (the API says so: `FILE_SIZE_TOO_LARGE … Maximum file size is 52428800`). On the multipart form the oversized body is turned away by the edge in front of the API with an HTML **413** page (the library fails to parse it as JSON and retries the same huge upload several times). Our code then logged "rejected" and returned `None`, and the migrate step carried on **without recording anything**: the message was lost silently. With the presign endpoint, a refusal for one file also switched presigned uploads off for the whole session (every later message then used the fragile multipart form).
- **Oversized files are left out, not fatal:** files over the limit are never uploaded; the message is sent with its text and the other attachments plus `-# ⚠ not migrated, larger than Fluxer's 50 MB file limit: \`movie.mp4\` (60.0 MB)`. If the instance's limit differs, the writer learns it from the API's answer and resends. (The file is still in the local backup.) On the legacy multipart form a 413 resends the text without the files and a note.
- **Presigned uploads only switch off when the endpoint is truly absent** (404/405/501). A refusal for one message (`400 INVALID_FORM_BODY`, permissions) only changes that message to the multipart form.
- **No more silent drops:** when `send_message` returns `None` (permanent rejection) the message is now skipped like any other failure: the bot posts "⚠️ There was an error migrating message `id` from **author** (date) (Fluxer rejected it), skipping...", it is recorded in `skipped_messages` with the reason, and progress advances. `scripts/list_skipped.py` lists them.
- Verified live: a message with a small file plus a 60 MB video posted with only the small file attached and the note (1.6 s), and with the writer deliberately given the wrong limit it learned 52,428,800 from Fluxer's error and did the same. 9 new tests (+1 for the rejected-message skip).
- **Messages lost *before* this fix** (413-dropped ones were never recorded) can be found by comparing the backup with the mapping table; a reconcile/retry tool for that is the obvious next step.

### Fixed — `(delivery unknown)` timeouts and 503s on media-heavy messages: attachments now use presigned uploads
- **Cause.** fluxer.py sends files as one multipart form *through Fluxer's API servers*. Under load that returns 503s; every library retry re-sends the whole payload; its HTTP session has a fixed 5-minute total limit; and our own timer covered upload + posting together, so a slow upload ended as `Send timed out (delivery unknown)` even though nothing had been posted.
- **Fix: `src/fluxer/uploads.py`.** Fluxer has a direct route (found in its OpenAPI spec, `https://api.fluxer.app/v1/openapi.json`): `POST /channels/{id}/attachments` returns presigned object-storage URLs (≤10 files per call; ≤10 MB = one PUT, larger = multipart parts finished by `POST /channels/{id}/attachments/complete`), and the message then only *references* the uploads (`{id, filename, content_type, upload_filename, file_size}`). Files never touch the API servers, each PUT/part is retried on its own (503/429/timeouts back off 1/2/4s; an expired URL gets a fresh presign), and the message POST is a few hundred bytes of JSON. The writer uploads first, then posts, so a failed upload cannot have delivered anything and a retried POST never re-sends the files.
- **Safe fallbacks.** An instance without the endpoint (or an unexpected answer) switches to the old multipart form for the session; a message rejected (4xx) with presigned attachments is retried once with the multipart form. Replies, avatars/usernames, skip-after-N, the delivery check and rate-limit handling are unchanged.
- Verified live: one message carrying **10 files × 3 MB (30 MB)** posted in 20.6 s, and a **reply carrying a 12 MB file (multipart plan)** in 8.4 s, attachments intact, native reply as the webhook identity. 16 new tests (fake API + fake storage). Not reproduced live: the 503 itself (it needs heavy load, and flooding from the same IP could trip the abuse block).

### Added — retry then skip messages that keep failing (and stop timing out large uploads)
A message that kept failing (live: `Halted at message …: Send timed out after 45s (delivery unknown)`) used to halt the whole run every time.
- **Retry, then skip with a marker.** A failed send is retried (5s·n backoff, up to 60s) and after **5 attempts** (setting **"Max send attempts per message"** on the Configuration screen / `max_message_attempts` in `reaper_config.yaml`; `0` = never skip, halt as before; CLI: `--max-attempts`) the message is **skipped**: the bot posts "⚠️ There was an error migrating message `<id>` from **author** (date) after N attempts, skipping..." in the channel, the source message is mapped to that marker (so replies/links still resolve), progress advances and the migration continues. Attempts are counted per message in the migration database, so they survive restarts. Cancel / the scheduled stop time are never counted as failures.
- **Skipped messages are recorded** in a `skipped_messages` table (source ID, channel, author, last error, attempts), shown in the run summary / audit log, and listed by **`scripts/list_skipped.py`**. A fresh start clears them.
- **The timeout now scales with the upload** (45s + ~1s per 100 KB, max 900s). A flat 45s could never succeed for a large attachment, so retrying it forever would never have helped.
- **A timed-out send is checked before it is retried:** the writer looks for the message in the channel (same webhook name and exact text, within the last few minutes of snowflake time). If it actually landed it is treated as sent, so a retry can't create a duplicate.
- Verified live: with a message rigged to always fail and a limit of 3, the messages before and after arrived in order with the skip marker between them, the cursor advanced, `list_skipped` showed it, and the run reported "1 message(s) skipped". 11 new tests; `halt on first error` is still available with the setting at 0.

### Added — media pasted as links is now saved (and de-duplicated)
Discord signs and expires CDN attachment URLs, so an old pasted link (`https://media.discordapp.net/attachments/…/attachment.gif`) is dead unless refreshed — but a bot token *can* refresh it (`POST /attachments/refresh-urls`, up to 50 per call, no extra permission; verified live on a real expired link).
- **`src/core/media_links.py`**: finds Discord CDN attachment links in message text, refreshes them in batches (waits out 429s), streams each download while hashing it, and stores it in the backup's content-addressed media pool by SHA-256 — **a GIF pasted 3,000 times under 3,000 different URLs is stored once**. Outcomes are cached per attachment in a new `link_media` table (`ok` / `dead` / `too_large` / `error`), so re-runs skip finished links; `error` is retried, `dead` only with `--retry-dead`. A signed URL that 404s (deleted attachment; the refresh endpoint signs any well-formed URL) is recorded as `dead`.
- **Backups update automatically:** the TUI backup, message backup and sync now finish with a "Saving media pasted as links…" pass (best-effort; never fails the backup). **`scripts/resolve_media_links.py`** does the same for existing backups (`--dry-run` to count first).
- **Migration (Fluxer):** resolved links become real attachments (≤10 files per message) and the link text — plus the Discord auto-embed that mirrored it — is dropped; dead/unresolved/oversized links stay as text. If Fluxer rejects a message with the saved media attached, it is retried once with the links as text instead of being lost.
- Why each message carries its own attachment instead of "upload once and paste that URL": Fluxer attachment URLs are signed and expire too (`ex=`/`expires_at`), so a pasted Fluxer URL would die. Local storage is de-duplicated; Fluxer receives the bytes per message (it de-duplicates by `content_hash` on its side).
- Verified end to end with the real Discord API and a real Fluxer community: an expired link resolved and saved once (105,564 B), two messages using it (one with a signature query) each migrated with a real attachment and no duplicate embed, and a dead link stayed as text. 9 new tests.
- Not covered yet: emoji/sticker CDN links (they don't expire), links inside embeds, and the Stoat migration path (Fluxer only).

### Changed
- **Replies are now native replies that keep the user's identity.** Fluxer's execute-webhook endpoint supports `message_reference`, but fluxer.py's `Webhook.send()` doesn't expose it, so replies used to go through the *bot* (nickname prefix, no avatar). The writer now calls the webhook route directly with the reference: the reply shows as a real reply (type 19) with Fluxer's own quote header and jump link, posted as "Name (discord)" with their avatar, files included (multipart). If Fluxer rejects the reference (e.g. the target message no longer exists — it returns 404) the send retries once without it and adds `-# ↳ (in reply to a message that could not be linked)` instead of dropping the message. When the replied-to message was never migrated (skipped/empty), the migrate step adds a quote block + header — `> quoted text` / `-# ↳ replying to \`@name\``, mentions cleaned, `@everyone`/`@here` neutralized, truncated to 160 chars. The thread-starter reply to the parent message uses the same path. Verified live (reply + attachment + avatar → type 19 with reference, and the unknown-target fallback); 5 new tests.

### Fixed
- **`'File' object is not subscriptable` on replies with attachments (pre-existing bug).** Replies are sent through the fluxer HTTP client's bot-send call, which indexes attachments as plain `{"filename", "data"}` dicts (`file["filename"]`), but the writer passed `fluxer.File` objects (that call is used instead of the webhook because `execute_webhook` can't carry a reply reference). Before this fork's halt-on-failure change the error was swallowed and the message **silently dropped**; now it halted the run at the first such message (seen live: `Halted at message 1227429728089276447: Send failed for channel …: 'File' object is not subscriptable`). Fixed in `FluxerWriter.send_message` and `send_marker`; regression tests added and verified live (reply + attachment sent with its file and reply link). **If you migrated with an older build, replies that carried attachments were missing from the result.** Resume a halted run with *Continue* after updating; the failed message is retried.
- **Halt/deadline inside a thread was reported as "Interrupted".** A nested thread run set `error`/`stopped` on its own stats and the parent channel loop never saw it. It is now propagated to the parent (per-channel migrate), so the UI shows the real reason. (Not exercised live: the test backup has no threads.)
- **Webhook tokens leaked into logs/errors.** The fluxer client puts the full webhook URL (`/webhooks/<id>/<token>`) in its error text and the writer logged/raised it. Now redacted (`_redact`) everywhere a send error is logged or raised; covered by a test.
- **Waterfall pre-scan overcounted:** it counted messages with nothing to send (no text/files/stickers/forward snapshot), so "remaining" and ETAs were ~2× too high (209 vs 99 on the test server). It now counts only what will actually be sent.

### Live test (throwaway Discord backup → throwaway Fluxer community)
| Test | Result |
|---|---|
| **TUI** (driven headlessly): Waterfall → Continue → Run Options `45s` / `60` msgs/min | dialog appeared; ran ~45s at ~60/min; "Paused at the scheduled stop time. 45 messages migrated"; status *Stopped*; 19 → 64 mapped |
| **TUI per-channel** (`#general`, Start from First, `40s` / 60 msgs/min) | dialog appeared; "Paused at the scheduled stop time. 40 messages migrated"; status *Stopped*; 40 mapped |
| **TUI per-channel** Continue, blank options | "Success! 53 messages migrated"; 93 mappings, 93 distinct source IDs (no duplicates) |
| **TUI** resume with blank options | "Success! 35 messages migrated globally"; `verify`: 99/99, 0 ghosts, 0 out-of-order |
| `--for 35s --max-rate 60` (fresh) | paused cleanly at the deadline, exit 10, 32 sent (~60/min) |
| resume `--for 10m` | counted 67 remaining (exact), sent 67, exit 0; `verify`: 99/99, 0 ghosts, 0 out-of-order |
| deadline lands **mid rate-limit wait** (injected permanent 429 from send #20, `--for 25s`) | exit 10 (clean pause, not an error); 19 mapped; cursor on message 19; interrupted message unmarked |

Unit tests: 39 pass in the suite (the 4 failures are the pre-existing `test_database.py` ones).

### Not yet done
- Timed runs for Stoat (needs the cursor and halt-on-failure semantics ported).
- Built-in scheduler (use cron/launchd for now).

---

## Fixed

### 1. Messages marked as migrated when they were never delivered (Fluxer)
`FluxerWriter.send_message` swallowed every failure (including the HTTP client giving up after its 4 rate-limit retries) and returned `None`. The migrate loops treated `None` as "skip and continue", and the next message that *did* send advanced the channel's last-migrated pointer past the dropped one. On resume the dropped message was skipped forever.

- A message is now only recorded as migrated after Fluxer returns a message ID.
- Transient failures (rate limit that never clears, timeout, outage, cancel) raise `MessageSendError`. Both the Waterfall loop and the per-channel loop **stop at that message without advancing any progress marker**, so a resume retries it.
- Permanent API rejections (4xx: bad embed, file too large, ...) are still logged and skipped, so one unsendable message can't block a run.

### 2. Rate limits were not waited out
- Rate limits the `fluxer` client can't ride out (it raises `RuntimeError("Failed after N attempts")`) are now waited out by the writer (5s, 10s, 20s, 40s, then 60s, up to 8 rounds) and the **same message is retried**.
- The 45s send timeout used to cancel requests that were merely sleeping through a 429 pause. It is now only enforced while no rate limit is active.
- File streams are rebuilt for every attempt (a failed upload consumes the `BytesIO`).
- Rate-limit state is read from the client's own log warnings (`fluxer.http`: `Rate limited on …, retry in Ns` and `Global rate limit hit, pausing for Ns`), matching the documented 429 body (`code: RATE_LIMITED`, `retry_after`, `global`).
- Cancelling a run aborts any rate-limit wait (`MigrationContext` wires `writer.stop_check`).

### 3. Resume restarted from the top / duplicated channels
Two independent causes:

- **Stale-mapping wipe (root cause of the duplicate channels).** For Fluxer, saved channel/category/role/emoji/sticker IDs come back from `MigrationDatabase.get_server_mapping` as `int`, but the sync steps compared them to sets of `str` IDs from the API. Every saved mapping looked stale and was deleted on each run, so a resume re-cloned all categories and channels and orphaned messages already mapped to the old ones. Fixed by comparing as strings in `fluxer/clone_server.py`, `fluxer/roles_permissions.py` and `fluxer/emoji_stickers.py` (including the parent-category name match).
- **Resume point was the per-channel minimum.** An untouched channel counts as 0, so "Continue" resolved to ID 0. Waterfall now writes a **global cursor** (`waterfall_cursor` in the migration DB metadata) after each fully handled message (sent, or deliberately skipped) and resumes from it. It falls back to the old minimum only when no cursor exists; "Start From Beginning" resets it.

### 4. Waterfall pre-scan counted already-migrated messages
`analyze_global_migration` looked up progress by *source* channel ID while progress is keyed by *target* channel ID, so totals were overstated on resume. It now uses the target ID like the migrate loop.

---

## Added

- **UI:** the progress log shows `Rate limited by Fluxer — pausing Ns, then resuming the same message...` (Waterfall and per-channel migration). A halted run shows the failing message ID and states that it was **not** marked as migrated.
- **`MigrationState.get_waterfall_cursor()` / `set_waterfall_cursor()`**.
- **`FluxerWriter` hooks:** `on_rate_limit(seconds)` (UI callback) and `stop_check()` (cancellation).
- **`scripts/live_waterfall.py`** — headless live-test harness (`backup`, `run [--fresh|--resume|--stop-after N|--inject …]`, `verify`), configured by `livetest.toml`. `--inject short,global,sustained,halt` fakes real 429 bodies at the HTTP layer. `verify` reports ghosts (marked sent, absent on Fluxer), unmigrated, extras and out-of-order counts.
- **`tests/test_fluxer_rate_limit.py`** — 5 unit tests for the retry, give-up, permanent-rejection, cancel and log-parsing paths.
- **`SourceDirectory.md`** — function-level map of `src/`.
- `.gitignore`: `livetest.toml` (holds tokens) and `livetest-work/`.

---

## Testing

Live, against a throwaway Discord server (backup source: 215 messages, 99 sendable across 7 channels) and a throwaway Fluxer community:

| Test | Result |
|---|---|
| Stop after 60 → resume → verify | 99/99 migrated, 0 ghosts, 0 out-of-order, no re-clone |
| Injected 429s: short (2s ×2), global (3s), sustained (6 in a row, outlasts client retries) | all 99 sent; sustained case paused 5s and retried the same message |
| Injected permanent 429 from call #20 (halt) | stopped after 19; cursor stayed on message 19; failed message unmarked; resume sent the remaining 80; 0 ghosts |
| Heavy traffic: 4 back-to-back full passes | pass 1 unthrottled (38s); passes 2–4 hit ~70 real 429s each (~0.6s waits), ~92s each; all 99 sent every time |

Unit tests: 5/5 new tests pass; suite is 27 passed / 4 failed. The 4 failures are pre-existing in `tests/test_database.py` (`sqlite3.ProgrammingError` at `src/core/backup_database.py:478`) and unrelated to this work — not yet investigated.


---

## Known limitations

- **Send timeout with no rate limit active:** delivery is unknown. The writer now looks for the message in the channel before anyone retries (exact webhook name + body within the last few minutes), so a duplicate is unlikely but not impossible.
- **Repaired / re-sent messages land at the end of their channel** (with their original date in the prefix), not in their original position.
- **Stoat** has none of the Fluxer-only features above (presigned uploads, outage hold, skip-and-marker, DM notifications, start-from-message).
- **Webhook resolution failure** falls back to the bot-post path (author shown as a `-# · name` prefix rather than the webhook identity). It is not retried.
- **`send_marker` (thread start/end markers)** still returns `None` on failure and is not retried.
- **Stoat** sending is untouched: it still logs-and-skips failures and does not update the Waterfall cursor (resume falls back to the per-channel minimum).
- Category names are matched case-sensitively when syncing to an existing server (`Text Channels` vs `Text channels` creates a second category).
- Duplicate `OperationPane._fetch_clone_preview` definition in `ui/shuttle_ops.py` (the later one wins) — noted, not changed.
- 4 pre-existing failing tests (above).

## Files changed

| File | Change |
|---|---|
| `src/fluxer/writer.py` | `MessageSendError`, rate-limit log handler, retry/backoff (`_send_with_recovery`, `_await_with_ratelimit`), `send_message` rework, `on_rate_limit`/`stop_check` hooks |
| `src/fluxer/migrate_message.py` | halt on `MessageSendError` (Waterfall + per-channel), Waterfall cursor writes, pre-scan key fix |
| `src/core/state.py` | waterfall cursor get/set; cleared with migration data |
| `src/core/base.py` | wires `writer.stop_check` |
| `src/fluxer/clone_server.py`, `roles_permissions.py`, `emoji_stickers.py` | int/str ID comparison fixes |
| `src/ui/shuttle_ops.py` | cursor-first resume point, rate-limit notices, halt reporting |
| `src/fluxer/uploads.py` *(new)* | presigned attachment uploads (singlepart / multipart), per-file limit handling |
| `src/fluxer/server_index.py` *(new)* | reads what is on the Fluxer server (migrated copies + skip markers) for start-from-message |
| `src/core/notify.py` *(new)* | DM notifications and the hourly `ProgressReporter` |
| `src/core/media_links.py` *(new)* | saves + de-duplicates media pasted as links |
| `src/core/utils.py`, `configuration.py`, `database.py`, `exporter.py`, `backup_database.py`, `src/ui/modals.py`, `main_app.py` | run options dialog, settings (attempts, outage, notify user), skipped-message tables, forward-only progress, link table |
| `scripts/timed_waterfall.py`, `monitor_run.py`, `resolve_media_links.py`, `list_skipped.py`, `live_waterfall.py` *(new)* | overnight runner, observer, media-link updater, skipped-message list, live test harness |
| `tests/test_fluxer_rate_limit.py`, `test_timed_waterfall.py`, `test_media_links.py`, `test_skip_messages.py`, `test_uploads.py`, `test_outage.py`, `test_notify.py`, `test_verify_from.py`, `test_progress_report.py` | new / updated |
| `docs/overnight.md`, `SourceDirectory.md`, `fork-changelog.md`, `.gitignore` | docs |
