# Fork Changelog

Changes in this fork relative to upstream Disco Reaper (`V4-main` @ `7fd487a`, "add support for stoat custom instance").
This release focuses on **Fluxer rate limiting** and the **Waterfall resume** bugs it exposed. Stoat sending is unchanged.

Status: **verified against a live Fluxer community** (see [Testing](#testing)). Not yet committed.

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
