# DiscoReaper (Luaily's Spinoff Version for Fluxer)
- [Ask DeepWiki](https://deepwiki.com/luaily/disco-reaper)
## EDITED IN THIS FORK VERSION:
Edited by [@luaily](https://github.com/luaily)  
This fork creates a heavily tested fixed and changed spinoff version based on [rambros3d/disco-reaper](https://github.com/rambros3d/disco-reaper) which adds QoL features, fixes bugs previously present in code, and creates a larger backup scope.

### TEST STATISTICS: 
| DATE | MESSAGES MIGRATED | TIME TO FINISH | SCOPE |
| :--- | :---: | :---: |  :---: |
| 2026 SEP 28 | `396` MESSAGES | `4min 31sec` (MR) | MESSAGES & IMAGES |
| 2026 OCT 01 | `89,258` MESSAGES | `26hr 52min` (SR) | ALL POSSIBLE MESSAGES |

_**MR** = Multi Run | **SR** = Single Run_ | Disclaimer: All fixes & tests apply only to Fluxer. 

### This repository's version comes with an AGPLv3 license.
  * The original code states "Take it, use it, modify it, feel free to do whatever you wish.", therefore, I will do whatever I wish and license it with the restriction that all iterations from this point—using this repository's versions past V4-Main original fork point commit:`e9450ed`—onwards MUST also contribute their modifications back to the public. 
  * If the original author has a problem with this I will remove said license.
  
* What that means for this repository:  

| ACTION | ALLOWED? | CONDITION | SCOPE FOR THIS REPOSITORY |
| :--- | :---: | :---: |  :---: |
| COPY/MODIFY/USE source code for personal use | ✅ ALLOWED | All code is provided free of charge for anyone to read or modify | ALL FILES |
| Modify the code in this repository for commercial use | ☑️ ALLOWED (!) | (!) All changes must also be published with this license. | ALL FILES AFTER COMMIT:`e9450ed` |
| Use the code in this repository to provide a service | ☑️ ALLOWED (!) | (!) A direct link to this repository (if using this code) must be provided. | ALL FILES AFTER COMMIT:`e9450ed` |
| Direct resale of this code | ☑️ ALLOWED (!) | (!) A direct link to this repository (if using this repo's source code) must be provided. | ALL FILES AFTER COMMIT:`e9450ed` | 
* All files after commit `e9450ed` means all files in this repository that have been modified after sourced from this repository, **if your source code is based on the original ([rambros3d/disco-reaper](https://github.com/rambros3d/disco-reaper)) repository this does NOT apply to you.**

### CHANGELOG:  
BASED ON [rambros3d/disco-reaper](https://github.com/rambros3d/disco-reaper):V4-main commit:[`2d33b16`](https://github.com/rambros3d/disco-reaper/commit/2d33b16d2c09098a3a1648dc15fc8abaee92b977), this repository will likely not update the source past this version.

#### IMPORTED FROM BRANCH: `ratelimit-fix` (COMMIT: `e9450ed`).  
1. FIXED: Messages marked as migrated when they were never delivered (Fluxer)
2. FIXED: Rate limits were not waited out
3. FIXED: Resume restarted from the top / duplicated channels
4. FIXED: Waterfall pre-scan counted already-migrated messages

#### IMPORTED FROM BRANCH: `QoL-Features-Fluxer` (COMMIT: `d0a5c8d`). 
5. ADDED TUI run options
6. ADDED a monitor (for extensive testing purposes)
7. ADDED deadline runs (runs until HH:MM or for x hours)
8. ADDED media.discord.com resolver
9. ADDED auto-backup for media.discord.com links
10. CHANGED the way replies are handled to keep the user’s identity
11. FIXED `'File' object is not subscriptable` bug that would halt all migration.
12. FIXED a bug where messages would be skipped or disco-reaper would lose track of if it has been migrated, (error: `(delivery unknown) The message was NOT marked as migrated`). 
13. FIXED a bug where large media attachments would not send if they took longer than 45s to upload (45s to upload + 1s for every 100KB of size to max 900s)
14. ADDED a feature, where, in the event of a error, it will re-attempt to send the message 5 times and log messages to migration and to logs when it cannot migrate a message.
15. FIXED `delivery unknown` timeouts and 503s on media-heavy messages
16. FIXED `HTTP ERROR 413` on large attachments and messages vanishing
17. CHANGED `HTTP ERROR 503` will now hold the migration instad of sending messages into the void and skipping them.
18. ADDED the option to start waterfall migration from a certain message, will auto-check any unsent messages after that discord message id and send them
19. ADDED DM notifs from the migration bot
20. ADDED DM progress reports at the start of every hour on the hour, to the already set fluxer id.
21. FIX leak-lock, when cancelled mid-upload, a message will hold the waterfall migration indefinitely. (FLUXER, BUG ON CHANGED CODE FOR PREVIOUS [COMMIT](499fed1)) 
22. TRANSPARANCY FIX on poisioned messages, random errors may cause issues which, tied with the previous bug, would lock the program in an indefinite loop of failing to send messages. (FLUXER)

SEE [CHANGELOG](./fork-changelog.md)

## HOW TO USE THIS FORK (BINARIES)
1. Navigate to this fork's [releases](https://github.com/luaily/disco-reaper/releases) tab 
2. Select the latest green release (or yellow if you're a risk taker)
3. Download the version for your OS
  * Select disco-reaper-linux.zip for **MacOS & Linux**
    * ON MACOS YOU MUST NAVIGATE TO THE EXTRACTED FOLDER AND RUN `xattr -rd com.apple.quarantine ./DiscoReaper`, CODE IS NOT SIGNED SO IT WILL NOT WORK UNLESS YOU LET IT THROUGH GATEKEEPER.
  * Select disco-reaper-windows.zip for **Windows**
    * Windows may throw a blue smartscreen error because the code is unsigned. 
4. Run the package
5. Leave a star on this repo (or else it wont work trust!!1!)

## HOW TO USE THIS FORK (CLONING, LATEST UNSTABLE)
1. **Clone**: Clone the repository to your local machine:
   ```bash
   git clone https://github.com/luaily/disco-reaper.git
   cd disco-reaper
   ```
2. **Launch**: Run the appropriate launcher script for your OS. It will automatically create a virtual environment and install dependencies:
   - **Linux**: `./launch-app.sh`
   - **MacOS**: `./launch-app-MAC.sh`
   - **Windows**: Double-click `launch-app-WIN.bat`


--------
# ORIGINAL README.md

**DiscoReaper** is a powerful tool designed to help you migrate your entire Discord server to Fluxer or Stoat. It clones channels, roles, emojis, permissions, and also your community's full message history.

>Join our [**Reaper Community**](https://fluxer.gg/9KxDP8WH) if you need help or have any questions.


### Video Guide - [Youtube](https://www.youtube.com/watch?v=SwIPQDxLzqA)


| Features | Fluxer | Stoat |
| :--- | :---: | :---: |
| **Server Template** |  |  |
| - Copy Categories & Channels | 🟩 | 🟩 |
| - Category Linking | 🟩 | 🟩 |
| - Channel Topic/Description | 🟩 | 🟩 |
| - NSFW Status | 🟩 | 🟩 |
| - Slowmode | 🟩 | ⚠️ |
| **Roles & Permissions** |  |  |
| - Roles Cloning | 🟩 | 🟩 |
| - Roles Permissions | 🟩 | 🟩 |
| - Category Permissions | 🟩 | ⚠️ |
| - Channel Permissions | 🟩 | ⏳ |
| **Emojis & Stickers** |  |  |
| - Copy Emojis | 🟩 | 🟩 |
| - Copy Stickers | 🟩 | ⚠️ |
| **Server Identity** |  |  |
| - Server Name | 🟩 | 🟩 |
| - Server Icon | 🟩 | 🟩 |
| - Server Banner | 🟩 | 🟩 |
| **Message Message history** |  |  |
| - Text Messages | 🟩 | 🟩 |
| - File Attachments | 🟩 | 🟩 *some file extensions not supported*|
| - Preserve Reply Links | 🟩 | 🟩 |
| - Webhook Embeds | 🟩 | 🟩 |
| - Threads | 🟩 | 🟩 |
| - Forums | ⚠️ | ⚠️ |

- ⚠️**Fluxer/Stoat**: Threads & Forums type channels are not yet natively available. As a workaround, threads are migrated in their parent channels as normal messages. 
- ⚠️**Stoat**: doesn't have features like Category Permissions or Slowmode settings for channels.
- ⏳**Stoat**: permission sync for channels was not implemented due to architectural differences.

---

### Core Operations

#### 📦 Local Backup (Backup & Migrate)
Create full, local backups of your Discord servers. 
- **Server Profile**: Export server identity, categories, channels, roles, and emojis.
- **Message Backup**: Save total message history, including attachments and threads, to your disk.
- **Migration Source**: Use your local backups as a source for migrations, allowing you to move to Fluxer even if you no longer have access to the original Discord server.

#### Shuttle Transfer (Direct Migration)
Migrate directly from Discord or your Local Backups to the target community.
- **Server Clone**: Transfer the entire server structure, roles, and assets in one click.
- **Server Sync**: Sync channel names, topics, and permissions between servers.
- **Message Migration**: Transfer years of history with support for incremental sync (picking up where you left off).

### Notable Features
*   **Sequential Batch Execution**: Select multiple channels, emojis, or roles and let the tool handle them one by one.
*   **Confirmation Previews**: See what's about to happen (name changes, logo updates, counts) before you continue.
*   **Incremental Migration**: Tracks already migrated messages to avoid duplicates and save time on large servers.
*   **Flexible Start Points**: Migrating history can start from the first message, a specific message ID (to be implemented), or continue from the last saved state.
*   **Audit Channel**: Live logging of migration progress directly in your target server.

### ⚠️ Danger Zone
*   **Full Wipe**: Clean out categories, channels, roles, or assets to reset a community for a fresh start.
*   **Permission Reset**: Batch-wipe all channel permission overwrites in the target server.
*   **Safety First**: All destructive actions require a dedicated confirmation step and data-fetching preview.

### Notes:
- The Reaper has **read-only access** to the source Discord server, so your original data is never touched.
- Currently, Fluxer experiences stability issues during high traffic. Use cautiously for large migrations. Check status at [fluxerstatus.com](https://fluxerstatus.com).

# Getting Started

#### Setup the bots as per this [guide](BOT-SETUP.md)

### Option 1: Using Pre-built Binaries (Easiest)
1. **Download**: Grab the latest version from the [Releases](https://github.com/rambros3d/disco-reaper/releases) page.
2. **Run**:
   - **Linux**: Run the `disco-reaper` binary (e.g., `./launch.sh` or double-click).
   - **Windows**: Run `disco-reaper.exe`.

### Option 2: Running from Source (To use latest unstable code)
1. **Clone**: Clone the repository to your local machine:
   ```bash
   git clone https://github.com/rambros3d/disco-reaper.git
   cd disco-reaper
   ```
2. **Launch**: Run the appropriate launcher script for your OS. It will automatically create a virtual environment and install dependencies:
   - **Linux**: `./launch-app.sh`
   - **MacOS**: `./launch-app-MAC.sh`
   - **Windows**: Double-click `launch-app-WIN.bat`

---
## What do the people say
![Comment](images/comments/comment-1.png)
![Comment](images/comments/comment-2.png)
![Comment](images/comments/comment-3.png)
![Comment](images/comments/comment-4.png)
![Comment](images/comments/comment-5.png)
### No regrets walking away from Discord

Dear Discord, The Reaper has a [message for you](https://c.tenor.com/dq8yuzNDkWkAAAAd/tenor.gif).

---
---
## Discord Age Verification Misconceptions

Discord’s latest operation just proves they never cared about you or your kids' safety.

### Age verification protects kids
- Online safety of your kids should be your responsibility.
- The big tech and especially the government should stay away from the kids. Period. 
- Do you think they care about your kids' safety, Really?


### Its the Law
>Discord is rolling out age verification in all countries; even when not required by law.

>**Big tech companies didn't comply with the laws when mishandling our data.**
Now they suddenly seem to care when complying with age verification laws (as they can grab even more data).

- Point being, in either case they never cared about the us. As we are their product anyway
- I dont expect these companies to stand up for our rights against these dystopian laws.


### Discord ID verification is "privacy-preserving"

>Discord initially **misled everyone** that it will be optional and the verification data wont leave the device.
But now their own website states that **Persona** will be used in some countries, its brought to you by the same guy involved with [**Palantir**](https://corbettreport.com/what-does-palantir-actually-do/).

### **Just say NO** to invasive companies & Move to platforms that respect you.

---
---

### Documentation
- [![Website](https://img.shields.io/badge/Website-rambros3d.com-blue?style=flat&logo=googlechrome&logoColor=white)](https://reaper.rambros3d.com/) - view bot setup guides, tool usage guides, and backup viewer
- [![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/rambros3d/disco-reaper) - incredibily good AI docs
- [![Ask DeepWiki (luaily's version)](https://deepwiki.com/badge.svg)](https://deepwiki.com/luaily/disco-reaper)


### Vibe Code Notice

- Code is provided as is; This tool was developed with AI.
- Take it, use it, modify it, feel free to do whatever you wish.
