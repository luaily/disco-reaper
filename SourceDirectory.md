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
  - `__init__` :17 — builds reader/writer by platform and mode; sets `is_running=False` (loops exit immediately until a caller sets it `True`) and wires `writer.stop_check` so rate-limit waits abort on cancel.
  - `_find_backup_path(server_id, base_dir_str)` :65 — locates a `DISCORD_BACKUP-{id}` folder.
  - `async validate_all()` :89 — connection/permission validation status dict for source and target.
  - `ensure_state_initialized(community_id, community_name)` :132 — creates/opens the `MigrationState` DB in the correctly named folder.
  - `async start_connections()` :166 — starts reader and writer.
  - `async start_target_only()` :170 — starts only the writer (Danger Zone).
  - `async close_connections()` :174 — closes reader and writer.
  - `async close_target_only()` :184 — closes only the writer.
  - `stop()` :192 — sets `is_running=False` to cancel work.

### `core/configuration.py`
- `class AppConfig` :6 — dataclass of per-profile settings (tokens, server IDs, mode, platform, log level).
- `load_config(config_path, create_if_missing)` :20 — reads a profile's config file.
- `save_config(config, config_path)` :39 — writes it.
- `get_available_configs()` :45 — lists profile names.
- `create_new_config(name)` :59 — creates a profile folder with a default config.

### `core/database.py` — `MigrationDatabase` (SQLite mappings + progress)
- `class MigrationDatabase` :14
  - `__init__(db_path, platform)` :20; `_get_conn()` :26 — connection helper; `_init_db()` :32 — creates tables / handles migrations per platform.
  - Message maps: `set_message_mapping` :226, `get_target_message_id` :234, `get_all_message_mappings` :245 — Discord msg ID ↔ target msg ID per channel.
  - User aliases (anonymize mode): `_generate_alias` :257 — unique `{Adjective}{Name}` from `random_users.json`; `get_or_create_user_alias` :291.
  - Server entity maps (channels/roles/categories): `set_server_mapping` :326, `get_server_mapping` :334, `get_all_server_mappings` :345, `delete_server_mapping` :355, `clear_server_mappings` :363.
  - Asset maps (emoji/sticker): `set_asset_mapping` :373, `get_asset_mapping` :381, `get_all_asset_mappings` :392, `delete_asset_mapping` :402, `clear_asset_mappings` :410.
  - Metadata KV: `set_metadata` :420, `get_metadata` :425.
  - Channel progress: `update_channel_tracking` :430 (last msg id/ts + counters), `get_channel_tracking` :447, `get_global_min_last_message_id` :455 (min progress across channels — **Waterfall resume point**).
  - Thread maps/progress: `set_thread_message_mapping` :498, `get_target_thread_message_id` :506, `update_thread_tracking` :517, `get_thread_tracking` :535.
  - Progress maps: `get_all_channel_tracking_ids` :542, `get_all_thread_tracking_ids` :548 — channel/thread → last msg ID.
  - Cleanup: `clear_channel_data` :554, `clear_all_migration_data` :563, `close` :573.

### `core/state.py` — `MigrationState` (facade over `MigrationDatabase`)
- `class MigrationState` :11 — resumable state; most methods are thin wrappers, many with legacy alias names.
  - `__init__` :16, `_ensure_db` :20.
  - Channels: `set_channel_mapping` :27, `get_target_channel_id` :32, `remove_channel_mapping` :37, `remove_target_channel_mapping` :41, `set_target_channel_id` :45 (legacy alias).
  - Categories: `set_category_mapping` :53, `get_category_mapping` :58, `remove_category_mapping` :64, `set_target_category_id` :68 (alias).
  - Roles: `set_role_mapping` :77, `get_role_mapping` :82, `remove_role_mapping` :88, `set_target_role_id` :92 (alias).
  - Emoji: `set_emoji_mapping` :101, `get_emoji_mapping` :106, `remove_emoji_mapping` :111.
  - Stickers: `set_sticker_mapping` :120, `get_sticker_mapping` :125, `remove_sticker_mapping` :130.
  - Properties returning full maps: `channel_map` :140, `category_map` :144, `role_map` :148, `emoji_map` :152, `sticker_map` :156; `audit_log_channel` getter :160 / setter :164.
  - Messages: `set_target_message_mapping` :170, `get_target_message_id` :174, `set_message_mapping` :179, `get_fluxer_message_id` :182 (legacy), `find_message_mapping` :277 (look up by Discord ID across channels).
  - Stats/threads: `increment_stats` :185, `increment_thread_stats` :189, `set_thread_message_mapping` :193, `update_thread_last_message_timestamp` :197, `update_thread_last_message_id` :201, `update_thread_completed` :205, `is_thread_completed` :209, `get_thread_message_id` :215, `get_thread_last_message_id` :272.
  - Channel progress: `update_last_message_timestamp` :220, `update_last_message_id` :224, `get_last_message_id` :228.
  - Waterfall/resume: **`get_waterfall_cursor` :248 / `set_waterfall_cursor` :260** (NEW — global "everything ≤ this source message ID is handled" cursor, stored as `waterfall_cursor` in the DB `metadata` table; preferred resume point), `get_global_min_last_message_id` :235 (fallback), `get_all_last_message_ids` :264, `clear_all_migration_data` :242 (also resets the cursor).
  - Clearing: `clear_channel_mappings` :291, `clear_role_mappings` :296, `clear_asset_mappings` :300, `clear_message_history` :305, `clear_channel_data` :314.
  - `set_folder(server_id, clean_name, platform, base_dir)` :318 — creates the per-server state folder and opens its DB.
  - `get_user_alias(user_id)` :357 — alias via the DB; `load` :364 / `save_state` :365 — no-op compatibility.
  - Note: for Fluxer, `get_*_mapping`/`get_target_*_id` return **`int`** (the DB column is INTEGER); compare with `str(...)` against API IDs.

### `core/utils.py`
- `parse_snowflake(value)` :5 — safe int parse of a Discord ID (handles `'None'`).
- `resolve_discord_links(content, state, platform, target_server_id)` :19 — rewrites Discord message/channel URLs to the target platform's equivalents (inner `replace_link` :34).
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

### `core/backup_database.py` — `BackupDatabase` (SQLite for backups)
- `class BackupDatabase` :13
  - `__init__` :16, `_migrate_db` :30 (legacy column renames), `_init_db` :148 (schema), `close` :1024.
  - Writes: `set_guild_profile` :363, `save_roles` :389, `save_channels` :409, `save_permissions` :417, `save_users` :426, `save_server_assets` :435, `save_threads` :455, `save_forum_tags` :464, `save_messages_batch` :473 (messages + attachments/embeds/reactions/stickers).
  - Media pool (dedupe): `get_media_by_hash` :565, `get_media_by_url` :570, `add_media_to_pool` :575, `get_all_media` :734.
  - Reads: `get_guild_profile` :377, `get_last_message_id` :560, `get_stats_by_channel` :582, `get_all_roles` :642, `get_all_channels` :647, `get_all_threads` :683, `get_forum_tags` :689, `get_threads_by_parent` :698, `get_thread` :704, `get_all_users` :710, `get_user` :715, `get_server_assets` :725, `get_backed_up_channel_ids` :970, `get_message_with_relations` :976.
  - Message paging: `get_messages_paged` :740 (one channel), **`get_global_messages_paged` :816** (all channels, ordered by timestamp/ID ascending — Waterfall).
  - Cleanup: `delete_channel_messages` :893, `purge_unused_media` :930.

### `core/exporter.py` — `DiscordExporter` (Discord → backup)
- `class DiscordExporter` :13
  - `__init__` :16, `async setup` :30, `_calculate_sha256` :58, `async prefetch_members` :66.
  - Export steps: `export_metadata` :80, `export_roles` :117, `download_server_assets` :136, `export_assets` :167, `export_channels_structure` :234, `export_channel_messages` :353 (incremental), `export_threads` :771 (active + archived; inner `_export_one_thread` :862).
  - Helpers: `_process_channel_batch` :301, `_format_channel` :314, `_format_user` :457, `_flush_pending_avatars` :520 (inner `_save_avatar` :525), `_format_message` :535, `_process_media` :686 (SHA-256 content-addressed dedupe).

---

## `src/fluxer/` — Fluxer target (mirrored by `src/stoat/`)

### `fluxer/writer.py` — `FluxerWriter` (REST/bot client)
- **NEW** `class MessageSendError` :18 — raised when a message could not be delivered for a transient reason; callers must halt and not mark the message migrated.
- **NEW** `class _RateLimitLogHandler` :24 (`__init__` :27, `emit` :31) — parses the `fluxer.http` logger's `Rate limited on …, retry in Ns` / `Global rate limit hit, pausing for Ns` warnings into `writer._note_rate_limit`.
- `class FluxerWriter` :40 — `__init__` :41 (now also sets `rate_limited_until`, `on_rate_limit`, `stop_check`), static `fetch_guilds(token, api_url)` :57, `_get_or_create_webhook` :79, `start` :104 (inner `on_ready` :124; attaches the rate-limit log handler), `client` :137, `validate` :141, `close` :818.
- Channels: `create_channel` :226, `modify_channel` :248, `move_channel` :273, `get_channels` :279.
- Messages: `send_message` :287 (webhook impersonation: author name/avatar, files, reply, forward, embeds; inner `_build_files` :342 and `_attempt` :348; returns the message ID, `None` only for a permanent 4xx rejection, raises `MessageSendError` otherwise), `send_marker` :474 (bot-posted thread start/end markers; still returns `None` on failure, not retried).
- **NEW** rate-limit handling: `_note_rate_limit` :402 (records `rate_limited_until`, calls `on_rate_limit`), `_rate_limit_remaining` :412, `_cancelled` :415 (uses `stop_check`), `_await_with_ratelimit` :418 (45s timeout enforced only while not rate limited), `_send_with_recovery` :436 (waits out client give-ups with 5/10/20/40/60s backoff, up to 8 rounds, retrying the same message).
- Roles/assets: `create_role` :505, `create_emoji` :528, `create_sticker` :545, `update_guild_metadata` :562, `remove_community_logo_and_banner` :593.
- Danger zone: `delete_all_channels` :640, `reset_channel_permissions` :668, `set_channel_permission` :708, `delete_all_roles` :731, `delete_all_emojis_and_stickers` :772.

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
- `clean_mentions(...)` :22 — rewrites user/role/channel/emoji mentions for the target (inner `replace_user` :28, `replace_role` :52, `replace_channel` :77, `replace_emoji` :105); supports anonymize mode.
- `get_channel_threads(reader, channel_id)` :130 — all (active + archived) threads for a channel.
- `_process_and_send_message(context, msg, target_channel_id, stats, thread_id, parent_target_id, thread_name, processed_threads)` :163 — per-message core: mentions, attachments, stickers, embeds, replies/forwards, link rewriting, send, record mapping and progress. Only records a mapping / advances progress when Fluxer returns a message ID; lets `MessageSendError` propagate.
- `analyze_migration(...)` :344 — count messages/threads/attachments for one channel.
- `migrate_messages(...)` :422 — per-channel migration incl. threads (inner `_process_missed_threads` :472); **halts on `MessageSendError`** (sets `is_running=False`, returns `stats["error"]`) instead of logging and continuing.
- **`analyze_global_migration(...)` :804 — Waterfall pre-scan** (progress lookup now keyed by target channel ID).
- **`migrate_global_messages(...)` :861 — Waterfall migration loop**: halts on `MessageSendError` without marking the message; writes `state.set_waterfall_cursor(msg.id)` after each fully handled message.

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
- `ConfigScreen` :283 — tokens, tool mode, target platform: `__init__` :320, `compose` :326, `on_mount` :430, `_fetch_and_populate` :450, `_do_fetch_guilds` :489, `_do_fetch_target_servers` :500, `on_button_pressed` :546, `_get_selected_mode` :566, `_get_selected_platform` :572, `_toggle_target_section` :578, `on_radio_set_changed` :582, `_collect_and_save` :612, `_launch_mode` :652.
- `ReaperApp` :661 — app root: `on_mount` :686, `action_screenshot` :690, `deliver_screenshot` :694.
- `run_disco_reaper_tui()` :717 — starts the app.

### `ui/mode_screen.py`
- `ModeScreen` :21 — one screen for all tool modes; `__init__` :110, `compose` :117, `on_button_pressed` :145, `_toggle_pane` :166 (Backup ↔ Migrate).

### `ui/shuttle_ops.py` — the operations pane (largest file)
- `RateLimitHandler` :48 (`__init__`, `emit`) — log handler that surfaces rate-limit messages.
- `class OperationPane` :83 — Backup / Clone / Sync / Migrate / Waterfall / Danger Zone.
  - Setup: `__init__` :107, `compose` :124, `on_mount` :170, `on_show` :176, `reload_config` :184, `_base_dir` :190, `_rebuild_engine` :196, `_get_backup_info` :208, `_update_info_labels` :236 (enables/disables buttons), `run_validate` :412 (inner `check_discord` :494, `check_target` :515), `_check_and_update` :548, `on_button_pressed` :575.
  - Autotest: `run_autotest_sequence` :605, `_run_migration_autotest_logic` :630, `_run_backup_autotest_logic` :679, `_logic_autotest_migrate_all_channels` :1143.
  - Clone/Sync menus: `_open_clone_menu` :721, `_open_sync_menu` :736, `run_batch_clone` :753, `run_batch_sync` :887.
  - Clone/Sync logic: `_logic_clone_channels` :971, `_logic_clone_roles` :999, `_logic_sync_permissions` :1013, `_logic_copy_assets` :1033, `_logic_sync_metadata` :1052, `_format_sync_report` :1079, `_format_clone_report` :1110.
  - Matching/preview: `_perform_auto_matching` :2054 (name-match roles/channels/emojis/stickers), `_fetch_dz_preview` :1982, `_fetch_clone_preview` :2051 **and again :2197 (duplicate definition; the later one wins)**.
  - Per-channel migrate: `run_migrate_messages` :1140, `_logic_migrate_messages` :1190 (hooks the rate-limit notice at :1533 and reports a halted run).
  - **Waterfall: `_hook_rate_limit_notice` :1602 (NEW — shows "Rate limited … pausing Ns" in the progress log), `run_waterfall_migration` :1611, `_logic_waterfall_migration` :1614** (resume point = waterfall cursor, else per-channel minimum; reports `result["error"]` when halted).
  - Danger Zone: `_open_danger_menu` :1872, `run_batch_danger` :1885, `_logic_dz_delete_channels` :2243, `_logic_dz_reset_perms` :2258, `_logic_dz_delete_roles` :2273, `_logic_dz_delete_assets` :2288.
  - Backup: `run_backup_messages` :2307, `_logic_full_backup` :2397, `run_backup_sync` :2514.

### `ui/modals.py` — shared dialogs
- `UILogHandler` :18 — pipes logging into the UI RichLog.
- `ProgressScreen` :36 — progress dialog with stats/log and phased buttons: `compose` :81, `__init__` :119, `on_unmount` :143, `update_timer` :149, `on_button_pressed` :157, `write` :187, `write_live` :193, `set_status` :201, `set_progress` :207, `set_item_status` :217, `show_stats` :224, `update_stats` :233, `phase_wait_confirm` :241, `show_early_buttons` :282, `phase_progress` :336, `phase_report` :366, `show_info` :416, `allow_close` :425.
- `SubMenuModal` :433, `OptionSelectModal` :466 — button list / radio-option pickers.
- `ChannelPickerScreen` :536 — dual (source/target) channel picker.
- `ChannelSelectScreen` :739 — checkbox channel selector.
- `MessageIDInputModal` :869, `ChannelNameInputModal` :976, `ChannelIDInputModal` :1026 — validated input modals.
- `UpdateModalScreen` :1123, `UpdateProgressScreen` :1161 — update confirm + download progress.

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
- `tests/` — `conftest.py`, `test_database.py`, `test_migration.py`, `test_ui.py`, `test_utils.py`, **`test_fluxer_rate_limit.py`** (NEW — 5 tests: retry-after-client-gives-up, give-up raises `MessageSendError`, permanent rejection propagates, cancel stops waiting, rate-limit log parsing). Run with `./runtests.sh` (uses `./venv`). Known: 4 pre-existing failures in `test_database.py`.
- **`scripts/live_waterfall.py`** (NEW) — headless live-test harness, configured by `livetest.toml` (gitignored; `[discord]`, `[fluxer]`, `[run]`):
  - `load` :22, `make_ctx(cfg, mode)` :27 — builds a `MigrationContext` (`"live"` or `"backup"` source).
  - `cmd_backup` :41 — real Discord → local backup via `DiscordExporter` (same calls as the TUI's full backup).
  - `install_injection(names)` :87 — fakes real Fluxer 429 bodies at the aiohttp layer; plans `short`, `global`, `sustained`, `halt`.
  - `cmd_run` :126 — clone channels, then run the Waterfall (`--fresh`, `--resume`, `--stop-after N`, `--inject …`); sets `is_running=True`, refuses to run if no channels are mapped.
  - `cmd_verify` :172 — compares the backup with what is on Fluxer: ghosts (marked sent, absent), not-migrated, extras, out-of-order.
  - Typical sequence: `backup` → `run --fresh --stop-after 60` → `run --resume` → `verify`.
- `livetest-work/` (gitignored) — generated backup + migration-state DB; `livetest.log` — harness log.
- `docs/` — `backup-specs.md`, `faq.md`, `features.md`, `guide.md`.
- `fork-changelog.md` — what this fork changed and why; `SourceDirectory.md` — this file.
