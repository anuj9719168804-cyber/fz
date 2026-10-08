# 🚀 Flezen Downloader Bot

A Telegram bot (Pyrogram / Kurigram + aiohttp) that takes a **flezen.com share link** and gives you the file as a **Telegram upload, stream link or direct link**. Built on the Ak-vips bot; the resolver is the Flezen API used by the fbot repo.

## ✨ Features

- 🔎 Flezen link detection (`flezen.com/s|share|f|v|d/<id>` and `flezen.com/<id>`, with or without `https://` / `www.`) — also from **forwarded posts** (text, caption, hidden hyperlinks and inline buttons)
- 📥 Download straight to Telegram (large files are split automatically)
- 🔗 Stream link: races the download + stream URL, speed-tests them and opens the fastest (`STREAM_MODE=cdn`, default); `STREAM_MODE=proxy` relays through the bot's own seekable `/stream` proxy
- 📥 Direct link (freshly resolved on every tap, because Flezen CDN links are signed and expire)
- 🖼️ Thumbnail from the API, or a frame cut with FFmpeg
- 📊 Live progress, speed & ETA, cancel button · 📦 parallel ranged download with resume
- 🔁 Automatic retry with a freshly resolved link if a download fails or is too slow
- 👑 Admin tools (ban, premium, broadcast, stats, users), plans, referral, custom caption/thumbnail, file cache
- 💾 JSON storage with optional MongoDB, 🐳 Docker-ready

## 🔄 How a link is resolved (`flezen_api.py`)

1. **Public API:** `POST https://playflezen.com/api/extract` with `{"url": share_url}` → `{"success": true, "data": {"downloadUrl", "streamUrl", "fileName", "sizeBytes", "thumbnail"}}`. Retried on 429 / 5xx / network errors (`API_RETRIES`, default 3). No Flezen account or cookie needed.
2. **Page check:** if the API fails, `flezen.com/s/<id>` is checked, so a deleted link gives a clear *"does not exist or has been deleted by the uploader"* message.
3. If no size came back, the real size is probed (HEAD, then a 1-byte Range request).

## 🛠️ Setup

```bash
cp .env.example .env      # fill in API_ID, API_HASH, BOT_TOKEN, OWNER_ID
pip install -r requirements.txt
python bot.py
```

Docker:

```bash
docker build -t flezen-bot .
docker run --env-file .env -p 10000:10000 flezen-bot
```

### Main environment variables

| Variable | Meaning |
|---|---|
| `API_ID`, `API_HASH`, `BOT_TOKEN`, `OWNER_ID` | Telegram credentials |
| `ADMINS`, `LOG_CHANNEL`, `FORCE_SUB` | Admin ids, log channel, force-join channels |
| `MONGO_URI`, `MONGO_DB_NAME` | Optional MongoDB (built-in default URI kept; default DB name `flezenbot`, so data stays separate from the other bots) |
| `FLEZEN_API` | Extract API URL override |
| `API_RETRIES` / `API_TIMEOUT` | API retries (default 3) / seconds per attempt (default 30) |
| `LINK_TTL` | Seconds before a stored link is re-resolved (default 300) |
| `STREAM_MODE` | `cdn` (default) or `proxy` (via `/stream`) |
| `PUBLIC_URL` | Public URL of the bot (needed for the Stream button on VPS/Docker) |
| `PARALLEL_CONNECTIONS`, `MAX_FILE_SIZE_MB`, `MAX_SPLIT_MB`, `SPLIT_LARGE`, `DAILY_LIMIT` | Download tuning and limits |

Plans, referral, settings (`/set_caption`, `/set_thumb`, `/setchat` …), the file cache and resume work exactly as in the Ak-vips bot.

## 🧪 Tests

`flezen_api.py` was checked against a mock Flezen API (link detection, success, missing size, deleted file, API failure, 5xx retry). The Telegram part was not run end-to-end — deploy and send one Flezen link to check.