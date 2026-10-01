# Source Directory

Reference map of everything in `./src` (plus the `disco-reaper.py` entry point). Line numbers are `file:line` of the `def`/`class` at the time of writing (branch `gitbutler/workspace`) and will drift as code changes. Updated for the Fluxer rate-limit / Waterfall-resume work (see [fork-changelog.md](fork-changelog.md)); sections for files that version did not touch keep their original numbers.

Legend: `async` = coroutine. Methods are listed under their class. "Src" = the Discord side (live API or local backup); "Target" = Fluxer or Stoat.

## Architecture in one paragraph

`disco-reaper.py` launches a Textual TUI (`src/ui`). The UI builds a `MigrationContext` (`core/base.py`) which wires together a **source reader** (`DiscordReader` for live Discord, or `BackupReader` for a local SQLite backup), a **target writer** (`FluxerWriter` or `StoatWriter`), and a `MigrationState` (mapping Discord IDs to target IDs, persisted in SQLite via `MigrationDatabase`). Backup mode uses `DiscordExporter` to write into `BackupDatabase`. Per-platform operations live in `src/fluxer/*` and `src/stoat/*`, which are near-mirror-images of each other (same function names and signatures), and the UI picks the module by `target_platform`.

---

### Flow
1. `_logic_waterfall_migration`: connect, validate writer, `_perform_auto_matching`, clone any missing channels/categories (or skip), compute the resume point — **the waterfall cursor if set, otherwise** the global MIN of `last_msg_id` across mapped target channels/threads (only ones with backed-up messages) — then ask Start-from-beginning / Continue.
2. `analyze_global_migration` counts messages to process (for the progress bar), skipping messages at/below each target channel's last-migrated ID.
3. `migrate_global_messages` iterates `fetch_global_message_history(after_id=...)`, resolves each message's target channel via `state.get_target_channel_id(str(msg.channel.id))`, skips already-migrated messages using a per-channel progress map (keyed by target ID), and sends via `_process_and_send_message`.
4. After each message is fully handled (sent, or deliberately skipped/rejected) the loop calls `state.set_waterfall_cursor(msg.id)`. On `MessageSendError` it stops **without** touching any progress marker and returns `stats["error"]`; the UI reports it and a resume retries that message.

### Send path and error semantics (Fluxer)
- `FluxerWriter.send_message` returns the new message ID; returns `None` only for a **permanent** API rejection (4xx) that is logged and skipped; raises `MessageSendError` for anything transient (rate limit that never clears, timeout, outage, cancel).
- `_send_with_recovery` waits out `RuntimeError("Failed after N attempts")` from the `fluxer` HTTP client (5s, 10s, 20s, 40s, then 60s, up to 8 rounds) and retries the same message; `_await_with_ratelimit` only applies the 45s timeout while no rate limit is active; `_RateLimitLogHandler` reads the client's `fluxer.http` warnings to learn `retry_after`.
- Hooks: `writer.on_rate_limit(seconds)` (UI notice) and `writer.stop_check()` (set by `MigrationContext`, aborts waits on cancel).

### Fixed issues (history — see [fork-changelog.md](fork-changelog.md))
- Failed sends were marked migrated / progress advanced past them → now halt without marking.
- Resume restarted from the top or re-cloned channels: Fluxer mapping IDs are `int` but were compared with `str` API IDs in the sync steps (`clone_server`, `roles_permissions`, `emoji_stickers`), wiping every mapping each run → compare as `str`; plus the per-channel MIN resume point (0 for untouched channels) → global cursor.
- Pre-scan looked up progress by source channel ID instead of target ID → now target ID.

### Still worth knowing when debugging Waterfall
- **Thread messages:** `msg.channel` for a thread message must resolve via `get_target_channel_id(str(thread_id))`; if a thread has no mapping its messages are silently `continue`d (no log).
- **Unmapped channels are silent:** messages in a channel with no target mapping are skipped without a log line (this is what made an early live run report "0 messages").
- **`is_running` must be `True`** before clone/migrate calls: the loops exit immediately when it is `False` (the default on a new `MigrationContext`).
- **Ordering:** relies entirely on `get_global_messages_paged` ordering (`ORDER BY id ASC`, snowflake order) — check there first if messages arrive out of order.
- **Non-rate-limit send timeout:** delivery is unknown; the run halts and a resume may post one duplicate.

---

## Entry point

### `disco-reaper.py`
- `setup_logging()` — configures rotating `.reaper.log` file logging at the level from config; quiets PIL.
- `relaunch_in_terminal()` — on Linux with no TTY, relaunches itself inside a terminal emulator.
- (main block) — calls `run_disco_reaper_tui()`.

---

## `src/core/` — engine, data, and source readers

### `core/audit.py`
- `async log_audit_event(context, title, description, files)` :10 — posts a summary (and optional files) to the target's `#reaper-logs` audit channel.

### `core/base.py`
- `class MigrationContext` :14 — holds config, target platform, source mode (live/backup), the reader, writer, and state; the object passed to every operation.
  - `__init__` :17 — builds reader/writer by platform and mode; sets `is_running=False` (loops exit immediately until a caller sets it `True`) adds the optional `deadline` (epoch seconds; `None` = no limit) and wires `writer.stop_check` so rate-limit waits and pacing abort on cancel **or** once the deadline passes.
  - `deadline_reached()` :70 — NEW; `True` once `deadline` has passed (checked between messages by the migrate loops).
  - `_find_backup_path(server_id, base_dir_str)` :75 — locates a `DISCORD_BACKUP-{id}` folder.
  - `async validate_all()` :99 — connection/permission validation status dict for source and target.
  - `ensure_state_initialized(community_id, community_name)` :142 — creates/opens the `MigrationState` DB in the correctly named folder.
  - `async start_connections()` :176 — starts reader and writer.
  - `async start_target_only()` :180 — starts only the writer (Danger Zone).
  - `async close_connections()` :184 — closes reader and writer.
  - `async close_target_only()` :194 — closes only the writer.
  - `stop()` :202 — sets `is_running=False` to cancel work.

### `core/configuration.py`
- `class AppConfig` :6 — dataclass of per-profile settings (tokens, server IDs, mode, platform, log level, `anonymize_users`, **`max_message_attempts`** = send tries per message before it is skipped, default 5, 0 = never skip; editable on the Configuration screen).
- `load_config(config_path, create_if_missing)` :21 — reads a profile's config file.
- `save_config(config, config_path)` :40 — writes it.
- `get_available_configs()` :46 — lists profile names.
- `create_new_config(name)` :60 — creates a profile folder with a default config.

### `core/database.py` — `MigrationDatabase` (SQLite mappings + progress)
- `class MigrationDatabase` :14
  - `__init__(db_path, platform)` :20; `_get_conn()` :26 — connection helper; `_init_db()` :32 — creates tables / handles migrations per platform.
  - Message maps: `set_message_mapping` :246, `get_target_message_id` :254, `get_all_message_mappings` :265 — Discord msg ID ↔ target msg ID per channel.
  - User aliases (anonymize mode): `_generate_alias` :277 — unique `{Adjective}{Name}` from `random_users.json`; `get_or_create_user_alias` :311.
  - Server entity maps (channels/roles/categories): `set_server_mapping` :346, `get_server_mapping` :354, `get_all_server_mappings` :365, `delete_server_mapping` :375, `clear_server_mappings` :383.
  - Asset maps (emoji/sticker): `set_asset_mapping` :393, `get_asset_mapping` :401, `get_all_asset_mappings` :412, `delete_asset_mapping` :422, `clear_asset_mappings` :430.
  - Metadata KV: `set_metadata` :440, `get_metadata` :445.
  - Channel progress: `update_channel_tracking` :450 (last msg id/ts + counters), `get_channel_tracking` :467, `get_global_min_last_message_id` :475 (min progress across channels — **Waterfall resume point**).
  - Thread maps/progress: `set_thread_message_mapping` :518, `get_target_thread_message_id` :526, `update_thread_tracking` :537, `get_thread_tracking` :555.
  - Progress maps: `get_all_channel_tracking_ids` :562, `get_all_thread_tracking_ids` :568 — channel/thread → last msg ID.
  - **NEW** failed sends: tables `message_attempts` + `skipped_messages`; `get_message_attempts` :596, `record_message_attempt` :600, `clear_message_attempts` :612, `record_skipped_message` :617, `get_skipped_messages` :627 (all cleared by `clear_all_migration_data`).
  - Cleanup: `clear_channel_data` :574, `clear_all_migration_data` :583, `close` :630.

### `core/state.py` — `MigrationState` (facade over `MigrationDatabase`)
- `class MigrationState` :11 — resumable state; most methods are thin wrappers, many with legacy alias names.
  - `__init__` :16, `_ensure_db` :20.
  - Channels: `set_channel_mapping` :27, `get_target_channel_id` :32, `remove_channel_mapping` :37, `remove_target_channel_mapping` :41, `set_target_channel_id` :45 (legacy alias).
  - Categories: `set_category_mapping` :53, `get_category_mapping` :58, `remove_category_mapping` :64, `set_target_category_id` :68 (alias).
  - Roles: `set_role_mapping` :77, `get_role_mapping` :82, `remove_role_mapping` :88, `set_target_role_id` :92 (alias).
  - Emoji: `set_emoji_mapping` :101, `get_emoji_mapping` :106, `remove_emoji_mapping` :111.
  - Stickers: `set_sticker_mapping` :120, `get_sticker_mapping` :125, `remove_sticker_mapping` :130.
  - Properties returning full maps: `channel_map` :140, `category_map` :144, `role_map` :148, `emoji_map` :152, `sticker_map` :156; `audit_log_channel` getter :160 / setter :164.
  - Messages: `set_target_message_mapping` :170, `get_target_message_id` :174, `set_message_mapping` :179, `get_fluxer_message_id` :182 (legacy), `find_message_mapping` :295 (look up by Discord ID across channels).
  - Stats/threads: `increment_stats` :185, `increment_thread_stats` :189, `set_thread_message_mapping` :193, `update_thread_last_message_timestamp` :197, `update_thread_last_message_id` :201, `update_thread_completed` :205, `is_thread_completed` :209, `get_thread_message_id` :215, `get_thread_last_message_id` :290.
  - Channel progress: `update_last_message_timestamp` :220, `update_last_message_id` :224, `get_last_message_id` :228.
  - **NEW** failed sends: `get_message_attempts` :249, `record_message_attempt` :252, `clear_message_attempts` :255, `record_skipped_message` :259, `get_skipped_messages` :263.
  - Waterfall/resume: **`get_waterfall_cursor` :266 / `set_waterfall_cursor` :278** (NEW — global "everything ≤ this source message ID is handled" cursor, stored as `waterfall_cursor` in the DB `metadata` table; preferred resume point), `get_global_min_last_message_id` :235 (fallback), `get_all_last_message_ids` :282, `clear_all_migration_data` :242 (also resets the cursor).
  - Clearing: `clear_channel_mappings` :309, `clear_role_mappings` :314, `clear_asset_mappings` :318, `clear_message_history` :323, `clear_channel_data` :332.
  - `set_folder(server_id, clean_name, platform, base_dir)` :336 — creates the per-server state folder and opens its DB.
  - `get_user_alias(user_id)` :375 — alias via the DB; `load` :382 / `save_state` :383 — no-op compatibility.
  - Note: for Fluxer, `get_*_mapping`/`get_target_*_id` return **`int`** (the DB column is INTEGER); compare with `str(...)` against API IDs.

### `core/utils.py`
- `parse_snowflake(value)` :5 — safe int parse of a Discord ID (handles `'None'`).
- `resolve_discord_links(content, state, platform, target_server_id)` :19 — rewrites Discord message/channel URLs to the target platform's equivalents (inner `replace_link` :34).
- NEW run-window helpers (shared by the TUI dialog and `scripts/timed_waterfall.py`): `parse_until(text, now)` :111 (`HH:MM` → next occurrence), `parse_duration(text)` :124 (`9h30m`, `45s`), `fmt_dur(seconds)` :132, `parse_stop_spec(text)` :139 (blank → no limit; `HH:MM` → next occurrence; duration → now + duration).
- `get_app_version()` :88 — version from baked file or git.

### `core/updater.py`
- `get_current_version()` :17 — current version string.
- `parse_version(version_str)` :27 — version → int tuple.
- `async check_for_updates()` :36 — queries GitHub releases.
- `async download_and_extract_update(asset_url, progress_callback)` :105 — downloads release zip.
- `apply_update_and_restart(new_exe_path)` :159 — swaps executable and restarts.

### `core/discord_reader.py` — live Discord source
- `class DiscordReader` :7
  - Static: `find_item` :28 (discord.utils.get equivalent), `create_permission_overwrite` :36, `async fetch_guilds(token)` :41 (guilds the bot is in), `get_sticker_extension` :332.
  - `__init__` :60, `_create_client` :71, `async start` :78, `async validate` :99 (token/intents/permissions), `async close` :407.
  - Metadata/structure: `get_server_metadata` :170, `download_asset` :181, `get_categories` :185, `get_roles` :191, `get_emojis` :198, `get_stickers` :204, `get_members` :210, `get_channels` :220, `get_active_threads` :232, `fetch_channels` :238, `get_channel` :244.
  - Messages: `get_message` :248, `get_first_message` :262, `fetch_message_history` :283 (paginated, `after_id`/`limit`/`inclusive`).
  - Media: `download_emoji` :327, `download_sticker` :349, `download_attachment` :403.

### `core/backup_reader.py` — local-backup source that mimics discord.py
Provides stand-in classes so migration code works unchanged on backups.
- Enums/consts: `ChannelType` :25, `StickerFormatType` :41, `MessageType` :47.
- `BackupColor` :92 (`from_hex` :112) ≈ discord.Color.
- `BackupPermissions` :121 (`view_channel`, `read_message_history`), `BackupPermissionOverwrite` :141 (`pair`, `__iter__`), `BackupOverwriteTarget` :204, `_parse_overwrites(raw_list)` :281.
- `BackupAsset` :224 (`read`, `is_animated`) — file-backed asset.
- `BackupRole` :247, `BackupCategory` :299, `BackupChannel` :321 (`mention`, `jump_url`, `permissions_for` stub), `BackupMember` :382 (`top_role`, `display_avatar`).
- Media: `BackupAttachment` :459 (`read`, `save`), `BackupEmoji` :503, `BackupSticker` :530, `BackupPartialEmoji` :579, `BackupReaction` :595, `BackupTag` :617 (forum tag), `BackupThread` :634.
- `BackupMessage` :671 — full message from a DB row (author, attachments, embeds, reactions, stickers, reference, thread, flags); `BackupMessageReference` :799, `BackupMessageFlags` :805.
- Embeds: `BackupEmbed` :812 (`to_dict`), `BackupEmbedThumbnail` :868, `BackupEmbedImage` :872, `BackupEmbedAuthor` :876, `BackupEmbedFooter` :883, `BackupEmbedField` :889.
- `BackupGuild` :898 — guild stand-in; `active_threads`, `roles`, `members`, `channels`, `categories`, `emojis`, `stickers`, `get_member`, `get_role`, `get_channel`, `get_thread`, `async fetch_channels`, `async fetch_active_threads`.
- `BackupForbidden` :993 — raised for missing resources.
- `class BackupReader` :1002 — same interface as `DiscordReader`, reading SQLite.
  - Static: `find_item` :1026, `create_permission_overwrite` :1034.
  - `__init__` :1038, `async start` :1065, `async validate` :1200 (DB integrity), `async close` :1478 (no-op).
  - Lazy loaders/props: `roles` :1081, `categories` :1090, `threads` :1095, `channels` :1100, `_ensure_structure_loaded` :1104, `emojis` :1140, `stickers` :1145, `_ensure_assets_loaded` :1149, `members` :1165, `_ensure_members_loaded` :1169, `_ensure_media_pool_loaded` :1311.
  - Getters: `get_server_metadata` :1249, `download_asset` :1259, `get_categories` :1264, `get_channels` :1267, `fetch_channels` :1273, `get_active_threads` :1277, `get_backed_up_channel_ids` :1281, `get_channel` :1286, `get_roles` :1297, `get_emojis` :1300, `get_stickers` :1303, `get_members` :1306.
  - Messages: `_resolve_author` :1315 (stub member if missing), `_hydrate_message` :1338, `get_message` :1374, `get_first_message` :1382, `fetch_message_history` :1390 (per channel), **`fetch_global_message_history` :1429** (all channels, chronological — Waterfall).
  - Media: `download_emoji` :1467, `download_sticker` :1470, `download_attachment` :1473.

### `core/media_links.py` — NEW: save media pasted as links
Discord signs/expires CDN attachment URLs; a bot token can refresh them (`POST /attachments/refresh-urls`, ≤50 per call, no extra permission). This module saves such links found in message text into the backup's content-addressed media pool.
- Helpers: `link_key` :44 (`<channel>/<attachment>/<filename>`, host/signature independent), `extract_links(text)` :49, `base_url` :60, `filename_from_key` :65, `embed_refs_link(embed, keys)` :69 (Discord auto-embed that just mirrors a replaced link).
- `attach_link_media(content, db, backup_root, max_files, max_bytes)` :87 — migration time: resolved links become `{"filename","data"}` attachments and the link text is dropped; unresolved/dead/oversized links stay as text.
- `class MediaLinkResolver` :139 — `collect_links` (scan stored messages), `_refresh` (batch refresh, waits out 429, `MediaLinkError` on a rejected token), `_download` (streams to a temp file while hashing, size cap), `_store` (atomic check-then-insert into `media_pool`, one file per SHA-256), `resolve_backup(progress, retry_dead, limit, dry_run)` :238 → stats. Statuses: `ok`, `dead` (refresh gave nothing, or download 403/404/410 — Discord signs any well-formed URL), `too_large`, `error` (retried next run).
- `LinkGone`, `MediaLinkError`, `summarize(stats)` :315.
- Why not "upload once, link later" on Fluxer: Fluxer attachment URLs are signed and expire too, so each migrated message carries a real attachment (Fluxer dedupes by `content_hash` server-side).

### `core/backup_database.py` — `BackupDatabase` (SQLite for backups)
- `class BackupDatabase` :13
  - `__init__` :16, `_migrate_db` :30 (legacy column renames), `_init_db` :148 (schema), `close` :1077.
  - Writes: `set_guild_profile` :378, `save_roles` :404, `save_channels` :424, `save_permissions` :432, `save_users` :441, `save_server_assets` :450, `save_threads` :470, `save_forum_tags` :479, `save_messages_batch` :488 (messages + attachments/embeds/reactions/stickers).
  - Media pool (dedupe): `get_media_by_hash` :580, `get_media_by_url` :585, `add_media_to_pool` :628, `get_all_media` :787.
  - Reads: `get_guild_profile` :392, `get_last_message_id` :575, `get_stats_by_channel` :635, `get_all_roles` :695, `get_all_channels` :700, `get_all_threads` :736, `get_forum_tags` :742, `get_threads_by_parent` :751, `get_thread` :757, `get_all_users` :763, `get_user` :768, `get_server_assets` :778, `get_backed_up_channel_ids` :1023, `get_message_with_relations` :1029.
  - **NEW** media links: table `link_media` (key, status, hash, filename, size, content_type, error, checked_at); `get_link_media` :590, `set_link_media` :595 (commits, also flushing a pending pool insert), `link_media_counts` :606, `iter_link_candidates` :611 (messages whose text mentions a CDN attachment URL).
  - Message paging: `get_messages_paged` :793 (one channel), **`get_global_messages_paged` :869** (all channels, ordered by timestamp/ID ascending — Waterfall).
  - Cleanup: `delete_channel_messages` :946, `purge_unused_media` :983.

### `core/exporter.py` — `DiscordExporter` (Discord → backup)
- `class DiscordExporter` :13
  - `__init__` :16, `async setup` :30, `_calculate_sha256` :58, `async prefetch_members` :66.
  - Export steps: `export_metadata` :80, `export_roles` :117, `download_server_assets` :136, `export_assets` :167, `export_channels_structure` :234, `export_channel_messages` :353 (incremental), `export_threads` :778 (active + archived; inner `_export_one_thread` :869).
  - **NEW** `resolve_content_links(token, progress, **kw)` :686 — saves media pasted as links via `MediaLinkResolver` (called at the end of a TUI backup/sync and by `scripts/resolve_media_links.py`).
  - Helpers: `_process_channel_batch` :301, `_format_channel` :314, `_format_user` :457, `_flush_pending_avatars` :520 (inner `_save_avatar` :525), `_format_message` :535, `_process_media` :693 (SHA-256 content-addressed dedupe).

---

## `src/fluxer/` — Fluxer target (mirrored by `src/stoat/`)

### `fluxer/writer.py` — `FluxerWriter` (REST/bot client)
- **NEW** `class MessageSendError` :37 — raised when a message could not be delivered for a transient reason; callers must halt and not mark the message migrated.
- **NEW** `class SendTimeout(MessageSendError)` :43 — a send timed out with no rate limit active (delivery unknown). The timeout now scales with the payload (`_upload_timeout`: 45s + ~1s per 100 KB, capped at 900s) and, after a timeout, `_find_delivered` :467 looks for the message in the channel (same webhook name + exact body, bounded by snowflake time) so a retry doesn't post a duplicate.
- **NEW** `_redact(text)` :22 — strips webhook tokens (`/webhooks/<id>/<token>`) from error text before it is logged or raised; the fluxer client embeds the full webhook URL in its errors.
- **NEW** `class _RateLimitLogHandler` :47 (`__init__` :50, `emit` :54) — parses the `fluxer.http` logger's `Rate limited on …, retry in Ns` / `Global rate limit hit, pausing for Ns` warnings into `writer._note_rate_limit`.
- `class FluxerWriter` :63 — `__init__` :50 (now also sets `rate_limited_until`, `on_rate_limit`, `stop_check`), static `fetch_guilds(token, api_url)` :82, `_get_or_create_webhook` :104, `start` :129 (inner `on_ready` :149; attaches the rate-limit log handler), `client` :162, `validate` :166, `close` :934.
- Channels: `create_channel` :251, `modify_channel` :273, `move_channel` :298, `get_channels` :304.
- Messages: `send_message` :312 (webhook impersonation: author name/avatar, files, reply, forward, embeds; inner `_build_files` :367 and `_attempt` :379; returns the message ID, `None` only for a permanent 4xx rejection, raises `MessageSendError` otherwise; webhook path passes `fluxer.File` objects, **the bot path (now only used when no webhook can be created) must pass plain `{"filename","data"}` dicts** because `HTTPClient.send_message` indexes them). **Replies** with a webhook available go through `_webhook_execute_with_reference` (native reply, keeps the migrated user's name/avatar); a 4xx on the reference (e.g. target gone) retries once via `Webhook.send` with an "in reply to a message that could not be linked" note, `send_marker` :589 (bot-posted thread start/end markers; still returns `None` on failure, not retried).
- **NEW** `_webhook_execute_with_reference` :481 — calls `POST /webhooks/{id}/{token}` directly with `message_reference` (Fluxer's execute-webhook supports it; fluxer.py's `Webhook.send` doesn't expose it); multipart when files are attached.
- **NEW** pacing: `min_send_interval` (seconds between sends, 0 = off) enforced by `_pace` :505 before every send, abortable on cancel/deadline (used by `--max-rate`).
- **NEW** rate-limit handling: `_note_rate_limit` :517 (records `rate_limited_until`, calls `on_rate_limit`), `_rate_limit_remaining` :527, `_cancelled` :530 (uses `stop_check`), `_await_with_ratelimit` :533 (45s timeout enforced only while not rate limited), `_send_with_recovery` :551 (waits out client give-ups with 5/10/20/40/60s backoff, up to 8 rounds, retrying the same message).
- Roles/assets: `create_role` :621, `create_emoji` :644, `create_sticker` :661, `update_guild_metadata` :678, `remove_community_logo_and_banner` :709.
- Danger zone: `delete_all_channels` :756, `reset_channel_permissions` :784, `set_channel_permission` :824, `delete_all_roles` :847, `delete_all_emojis_and_stickers` :888.

### `fluxer/clone_server.py`
- `async sync_channel_state(context)` :9 — match existing Fluxer channels to Discord names and record mappings; drops mappings whose target no longer exists (IDs now compared as `str` — the `int`/`str` mismatch used to wipe every mapping each run).
- `async migrate_channels(context, progress_callback, force)` :67 — clone categories and channels (loops exit immediately if `context.is_running` is `False`).

### `fluxer/roles_permissions.py`
- `sync_roles_state` :9 — name-match roles into state.
- `sync_permissions` :41 — sync channel/category role overwrites (inner `_sync_overwrites` :66).
- `migrate_roles` :132 — copy roles + baseline permissions.

### `fluxer/emoji_stickers.py`
- `sync_assets_state` :9 — name-match emojis/stickers into state.
- `migrate_emojis` :59 — copy emojis and stickers.

### `fluxer/server_metadata.py`
- `sync_server_metadata(context, progress_callback, components)` :8 — name / logo / banner.

### `fluxer/danger_zone.py`
- `danger_remove_logo_and_banner` :8, `danger_delete_all_channels` :12, `danger_reset_channel_permissions` :19, `danger_delete_all_roles` :23, `danger_delete_all_emojis_and_stickers` :29 — thin wrappers over writer methods.

### `fluxer/migrate_message.py` — message migration
- `clean_mentions(...)` :23 — rewrites user/role/channel/emoji mentions for the target (inner `replace_user` :29, `replace_role` :53, `replace_channel` :78, `replace_emoji` :106); supports anonymize mode.
- **NEW** `_skip_message(context, msg, target_channel_id, reason, attempts, stats, thread_id)` :390 — gives up on a message that keeps failing: posts a bot marker ("There was an error migrating message `id` from **author** … after N attempts, skipping..."), records it in `skipped_messages`, maps the source ID to the marker, advances progress, bumps `stats["skipped"]` / `skipped_ids`.
- **NEW** `_process_with_retries(context, msg, target_channel_id, stats, **kw)` :429 — wraps `_process_and_send_message`: on `MessageSendError` retries (5s·n backoff, max 60s) until `config.max_message_attempts` (default 5, counted per message in the DB so it survives restarts; 0 = never skip, halt as before) then skips. A cancel / scheduled stop is never counted. Used by both the Waterfall and per-channel loops.
- **NEW** `format_reply_fallback(ref_text, ref_name, has_attachments, max_len)` :131 — `> quote` + `-# ↳ replying to \`@name\`` header used when the replied-to message can't be linked natively (never migrated / skipped); collapses whitespace, truncates to 160 chars, neutralizes `@everyone`/`@here`.
- `get_channel_threads(reader, channel_id)` :143 — all (active + archived) threads for a channel.
- `_process_and_send_message(context, msg, target_channel_id, stats, thread_id, parent_target_id, thread_name, processed_threads)` :176 — per-message core: mentions, attachments, stickers, embeds, replies/forwards, link rewriting, send, record mapping and progress. Only records a mapping / advances progress when Fluxer returns a message ID; lets `MessageSendError` propagate.
- (in `_process_and_send_message`) **NEW** media links: for a backup source, resolved CDN links in the text are converted to real attachments via `attach_link_media` (max 10 files per message), the mirrored Discord auto-embeds are dropped, and if Fluxer rejects the message with the saved media attached it retries once with the links left as text.
- `analyze_migration(...)` :464 — count messages/threads/attachments for one channel.
- `migrate_messages(...)` :542 — per-channel migration incl. threads (inner `_process_missed_threads` :592); **halts on `MessageSendError`** (sets `is_running=False`, returns `stats["error"]`) instead of logging and continuing; **stops cleanly at `context.deadline`** (`stats["stopped"] = "deadline"`, no error); an `error`/`stopped` from a nested thread run is propagated to the parent's stats so the UI reports the real reason.
- **`analyze_global_migration(...)` :954 — Waterfall pre-scan** (progress lookup keyed by target channel ID; skips messages with nothing to send so totals/ETAs match what is actually sent).
- **`migrate_global_messages(...)` :1016 — Waterfall migration loop**: halts on `MessageSendError` without marking the message; writes `state.set_waterfall_cursor(msg.id)` after each fully handled message; checks `context.deadline_reached()` before each message and treats a `MessageSendError` that coincides with the deadline as a clean stop (message left unmarked, `stats["stopped"] = "deadline"`).

---

## `src/stoat/` — Stoat target
Same function set as `src/fluxer/`; differences only in the API used.

- `stoat/writer.py` — `_discover_stoat_config(api_url)` :8 (finds WS/CDN URLs); `class StoatWriter` :50 with `__init__` :51, `fetch_guilds` :62 (inner `on_ready` :96), `start` :146, `my_id` :197, `_get_server` :200, `validate` :212, `get_channels` :302, `create_channel` :344, `modify_channel` :382, `move_channel` :408, `send_message` :411 (masquerade impersonation), `send_marker` :543, `create_role` :568, `_map_permissions` :623 (Discord bitfield → Stoat), `update_default_role_permissions` :668, `create_emoji` :681, `create_sticker` :691 (unsupported stub), `update_guild_metadata` :694, `remove_community_logo_and_banner` :706, `delete_all_channels` :730, `reset_channel_permissions` :779, `set_channel_permission` :811, `delete_all_roles` :839, `delete_all_emojis_and_stickers` :867, `close` :881.
- `stoat/clone_server.py` — `sync_channel_state` :10, `migrate_channels` :67 (inner `get_cat_position` :270).
- `stoat/roles_permissions.py` — `sync_roles_state` :9, `sync_permissions` :42, `migrate_roles` :170.
- `stoat/emoji_stickers.py` — `sync_assets_state` :9, `migrate_emojis` :50 (emojis only).
- `stoat/server_metadata.py` — `sync_server_metadata` :8.
- `stoat/danger_zone.py` — five `danger_*` wrappers, :8–:29.
- `stoat/migrate_message.py` — `clean_mentions` :21 (inner `replace_*` :27/:51/:72/:100), `get_channel_threads` :126, `_process_and_send_message` :158, `analyze_migration` :342, `migrate_messages` :423 (inner `_process_missed_threads` :471), **`analyze_global_migration` :805, `migrate_global_messages` :858** (Waterfall).

---

## `src/ui/` — Textual TUI

### `ui/main_app.py`
- `FirstInfoModal` :27 (`compose`, `on_button_pressed`) — first-launch info (renders `src/first-info.md`).
- `NewConfigModal` :64 (`compose`, `_get_sanitized_name`, `on_button_pressed`, `on_key`) — name a new profile.
- `ConfigSelectionScreen` :111 — pick/create profile: `compose` :135, `on_mount` :153, `check_updates` :162, `on_screen_resume` :179, `refresh_configs` :182, `on_list_view_selected` :203, `action_new_config` :219, `on_button_pressed` :235.
- `ConfigScreen` :283 — tokens, tool mode, target platform: `__init__` :322, `compose` :328, `on_mount` :445, `_fetch_and_populate` :465, `_do_fetch_guilds` :504, `_do_fetch_target_servers` :515, `on_button_pressed` :561, `_get_selected_mode` :581, `_get_selected_platform` :587, `_toggle_target_section` :593, `on_radio_set_changed` :597, `_collect_and_save` :627, `_launch_mode` :671.
- `ReaperApp` :680 — app root: `on_mount` :705, `action_screenshot` :709, `deliver_screenshot` :713.
- `run_disco_reaper_tui()` :736 — starts the app.

### `ui/mode_screen.py`
- `ModeScreen` :21 — one screen for all tool modes; `__init__` :110, `compose` :117, `on_button_pressed` :145, `_toggle_pane` :166 (Backup ↔ Migrate).

### `ui/shuttle_ops.py` — the operations pane (largest file)
- `RateLimitHandler` :49 (`__init__`, `emit`) — log handler that surfaces rate-limit messages.
- `class OperationPane` :84 — Backup / Clone / Sync / Migrate / Waterfall / Danger Zone.
  - Setup: `__init__` :108, `compose` :125, `on_mount` :171, `on_show` :177, `reload_config` :185, `_base_dir` :191, `_rebuild_engine` :197, `_get_backup_info` :209, `_update_info_labels` :237 (enables/disables buttons), `run_validate` :413 (inner `check_discord` :495, `check_target` :516), `_check_and_update` :549, `on_button_pressed` :576.
  - Autotest: `run_autotest_sequence` :606, `_run_migration_autotest_logic` :631, `_run_backup_autotest_logic` :680, `_logic_autotest_migrate_all_channels` :1144.
  - Clone/Sync menus: `_open_clone_menu` :722, `_open_sync_menu` :737, `run_batch_clone` :754, `run_batch_sync` :888.
  - Clone/Sync logic: `_logic_clone_channels` :972, `_logic_clone_roles` :1000, `_logic_sync_permissions` :1014, `_logic_copy_assets` :1034, `_logic_sync_metadata` :1053, `_format_sync_report` :1080, `_format_clone_report` :1111.
  - Matching/preview: `_perform_auto_matching` :2148 (name-match roles/channels/emojis/stickers), `_fetch_dz_preview` :2076, `_fetch_clone_preview` :2145 **and again :2197 (duplicate definition; the later one wins)**.
  - Per-channel migrate: `run_migrate_messages` :1141, `_logic_migrate_messages` :1191 (after the Start/Continue choice and before anything is cleared it asks for **Run Options** — Back returns to the channel picker; then arms the stop time / rate cap, shows live `msgs/min · ~ETA left`, reports a halted run and a **"Paused at the scheduled stop time"** outcome, and resets the options in `finally`).
  - **NEW run-option helpers (shared by Waterfall and per-channel):** `_ask_run_options` :1632 (pushes `RunOptionsModal`; Fluxer only, other targets get no limits), `_apply_run_options` :1646 (sets `engine.deadline` + `writer.min_send_interval`, logs the options), `_reset_run_options` :1660.
  - **Waterfall: `_hook_rate_limit_notice` :1666 (NEW — shows "Rate limited … pausing Ns" in the progress log), `run_waterfall_migration` :1676, `_logic_waterfall_migration` :1679** (resume point = waterfall cursor, else per-channel minimum; **for Fluxer, after Start/Continue and before anything is cleared, pushes `RunOptionsModal`** — Back cancels the run — then sets `engine.deadline` and `writer.min_send_interval`, logs the run options, shows live `msgs/min · ~ETA left` in the item status, and resets both in `finally`; reports `result["error"]` when halted and a **"Paused at the scheduled stop time"** outcome (status *Stopped*, audit log entry) when `result["stopped"] == "deadline"`).
  - Danger Zone: `_open_danger_menu` :1966, `run_batch_danger` :1979, `_logic_dz_delete_channels` :2337, `_logic_dz_reset_perms` :2352, `_logic_dz_delete_roles` :2367, `_logic_dz_delete_assets` :2382.
  - **NEW** `_resolve_media_links_step(modal)` :2491 — best-effort pass called at the end of `_logic_full_backup`, `run_backup_messages` and `run_backup_sync` (needs the live Discord token; never fails the backup).
  - Backup: `run_backup_messages` :2401, `_logic_full_backup` :2512, `run_backup_sync` :2631.

### `ui/modals.py` — shared dialogs
- `UILogHandler` :18 — pipes logging into the UI RichLog.
- `ProgressScreen` :36 — progress dialog with stats/log and phased buttons: `compose` :81, `__init__` :119, `on_unmount` :143, `update_timer` :149, `on_button_pressed` :157, `write` :187, `write_live` :193, `set_status` :201, `set_progress` :207, `set_item_status` :217, `show_stats` :224, `update_stats` :233, `phase_wait_confirm` :241, `show_early_buttons` :282, `phase_progress` :336, `phase_report` :366, `show_info` :416, `allow_close` :425.
- **NEW** `RunOptionsModal` :1026 — asks for an optional stop time (`HH:MM` or a duration) and a messages-per-minute cap; dismisses with `{"deadline": epoch|None, "max_rate": float}` (0 = unlimited) or `None` on Back; Enter in a field submits.
- `SubMenuModal` :433, `OptionSelectModal` :466 — button list / radio-option pickers.
- `ChannelPickerScreen` :536 — dual (source/target) channel picker.
- `ChannelSelectScreen` :739 — checkbox channel selector.
- `MessageIDInputModal` :869, `ChannelNameInputModal` :976, `ChannelIDInputModal` :1094 — validated input modals.
- `UpdateModalScreen` :1191, `UpdateProgressScreen` :1229 — update confirm + download progress.

### `ui/backup_stats.py`
- `BackupStatsScreen` :20 — backup statistics tree: `__init__` :158, `compose` :165, `on_mount` :223, `on_button_pressed` :237, `on_node_selected` :242, `_on_modal_close` :256, `_format_size` :260, `_format_tree_row` :271, `load_data` :289.
- `ManageBackupModal` :436 — per-channel delete/purge: `__init__` :496, `compose` :504, `on_button_pressed` :522, `run_deletion` :549.

### `ui/widgets.py`
- `RamDisplay` :8 (`on_mount`, `_format_speed`, `update_stats`) — RAM/network readout; `Footnote` :65 — branding text.

---

## Non-code files in `src/`
- `src/first-info.md` — welcome/first-launch text shown by `FirstInfoModal`.
- `src/random_users.json` — `names` (and adjectives) for `MigrationDatabase._generate_alias` anonymized aliases.

## Outside `src/` (for reference)
- `tests/` — `conftest.py`, `test_database.py`, `test_migration.py`, `test_ui.py`, `test_utils.py`, **`test_fluxer_rate_limit.py`** (NEW — retry-after-client-gives-up, give-up raises `MessageSendError`, permanent rejection propagates, cancel stops waiting, rate-limit log parsing, webhook-token redaction), **`test_skip_messages.py`** (NEW — timeout scaling, a timed-out send that actually landed is not retried, retry-then-skip with marker/progress/persistence, skip still advances if the marker can't be posted, 0 disables skipping, cancel is never counted, the waterfall continues past a skipped message, DB records), **`test_media_links.py`** (NEW — link extraction/normalization, same bytes under different URLs stored once, dead/too-large/error handling and skipping on re-run, dry run, refresh 429 + bad token, signed-URL-404 = dead, attach_link_media limits, embed mirroring), **`test_timed_waterfall.py`** (NEW — `--until`/`--for` and stop-spec parsing, the TUI `RunOptionsModal` (blank = no limits, validation, Enter submits, Back → `None`), deadline stops the loop cleanly between messages, send error at the deadline is a clean stop and leaves the message unmarked, an error before the deadline is still an error, `--max-rate` pacing and its cancel). Run with `./runtests.sh` (uses `./venv`). Known: 4 pre-existing failures in `test_database.py`.
- **`scripts/live_waterfall.py`** (NEW) — headless live-test harness, configured by `livetest.toml` (gitignored; `[discord]`, `[fluxer]`, `[run]`):
  - `load` :22, `make_ctx(cfg, mode)` :27 — builds a `MigrationContext` (`"live"` or `"backup"` source).
  - `cmd_backup` :41 — real Discord → local backup via `DiscordExporter` (same calls as the TUI's full backup).
  - `install_injection(names)` :87 — fakes real Fluxer 429 bodies at the aiohttp layer; plans `short`, `global`, `sustained`, `halt`.
  - `cmd_run` :126 — clone channels, then run the Waterfall (`--fresh`, `--resume`, `--stop-after N`, `--inject …`); sets `is_running=True`, refuses to run if no channels are mapped.
  - `cmd_verify` :172 — compares the backup with what is on Fluxer: ghosts (marked sent, absent), not-migrated, extras, out-of-order.
  - Typical sequence: `backup` → `run --fresh --stop-after 60` → `run --resume` → `verify`.
- **`scripts/timed_waterfall.py`** (NEW) — timed / overnight headless Waterfall for real profiles (see [docs/overnight.md](docs/overnight.md)); the time parsing now lives in `core/utils.py`: `profile_paths` :35, `log` :41, `run(args)` :45 (validate → clone/sync → resume from cursor → count remaining → `migrate_global_messages` with `ctx.deadline`; SIGINT/SIGTERM stop cleanly; prints rate, ETA and a summary), `main` :144. Options `--profile`, `--until`/`--for`, `--max-rate`, `--fresh`, `--no-clone`, `--no-count`, `--report-every`. Exit codes: `0` done, `10` paused with work left, `1` error, `2` usage/config.
- **`scripts/list_skipped.py`** (NEW) — lists messages skipped after repeated send errors from the migration DB's `skipped_messages` table (`--profile`, `--state-db`).
- **`scripts/monitor_run.py`** (NEW) — read-only observer for a running TUI/CLI session; see [docs/overnight.md](docs/overnight.md#measuring-a-full-run-scriptsmonitor_runpy). `parse_log_line` (Fluxer 429 / global / 5xx / connection / give-up / halt lines), `LogTail` (follows `.reaper.log`, survives truncation and rotation), `sample_db` (read-only counts from `backup.db` and the migration DB), `probe` (Fluxer API reachability from this IP), `find_process` / `rss_mb`, `Stats.summary`, `main`. Writes a CSV plus `<out>.summary.txt`.
- **`scripts/resolve_media_links.py`** (NEW) — updates an existing backup: finds CDN links in message text, refreshes + downloads each once, dedupes by SHA-256 into the media pool, records results in `link_media`. `--profile`, `--dry-run`, `--retry-dead`, `--concurrency`, `--max-size-mb`, `--limit`, `--backup-dir`. Safe to re-run.
- `livetest-work/` (gitignored) — generated backup + migration-state DB; `livetest.log` — harness log.
- `docs/` — `backup-specs.md`, `faq.md`, `features.md`, `guide.md`, **`overnight.md`** (NEW — timed runs, exit codes, cron/launchd examples).
- `fork-changelog.md` — what this fork changed and why; `SourceDirectory.md` — this file.
