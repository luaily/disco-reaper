# Fork Changelog

Changes in this fork relative to upstream Disco Reaper (`V4-main` @ `7fd487a`, "add support for stoat custom instance").
This release focuses on **Fluxer rate limiting** and the **Waterfall resume** bugs it exposed. Stoat sending is unchanged.

Status: rate-limit fixes **merged and verified against a live Fluxer community** (see [Testing](#testing)). The timed / overnight version below is **unreleased** (verified live, not yet committed).

---

## Unreleased — Timed / overnight Waterfall

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

- **Send timeout with no rate limit active:** delivery is unknown; the run halts and a resume may post one duplicate.
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
| `tests/test_fluxer_rate_limit.py`, `scripts/live_waterfall.py`, `livetest.toml`, `SourceDirectory.md`, `.gitignore` | new / updated |
