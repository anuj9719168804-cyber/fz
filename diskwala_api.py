"""Diskwala token-API resolver for Flezen share links (async port of the fbot-main/diskwala.py token path).

Used by flezen_api as the 2nd tier, after Flezen's public extract API:

  POST DOWNLOAD_ENDPOINT   {"link": <flezen share url>}              -> {"ok": true, ...}
  GET  STATUS_ENDPOINT?link=<share url>   (polled until "done")      -> {"ok": true, "status": "done", "file": {...}}
       the file is usually AES-GCM encrypted ({"_x","s","p","h"}) and is decrypted here.

Needs a bearer token, taken from (first match wins):
  * DISKWALA_TOKEN   - a ready bearer token (expires; mostly for testing)
  * SESSION          - a Telethon StringSession of a user account; opens the "sky577bot" mini app to get a fresh
                       token (cached 1h). Needs `pip install telethon`; it uses bot.py's API_ID / API_HASH.
Without either, this tier is skipped silently. No secret is stored in this repo - set them in the environment.
"""
import asyncio
import json
import logging
import os
import time
from typing import Optional
from urllib.parse import quote, unquote, urlparse

import aiohttp

log = logging.getLogger("diskwala_api")

DOWNLOAD_ENDPOINT = os.getenv("DISKWALA_FLEZEN_DOWNLOAD", "https://api2.diskwala.net/api/flezen/downloadw").strip()
STATUS_ENDPOINT = os.getenv("DISKWALA_FLEZEN_STATUS", "https://api2.diskwala.net/api/flezen/statusw").strip()
# Public constant of the Diskwala web client (not a secret of ours) used to decrypt the {"_x","s","p","h"} replies.
ENCRYPTION_KEY = "e7109544dab612bd5b80b8a427ac474ba5541b9efff7a4ca1c8ef85df2489c23"

DISKWALA_ENABLED = os.getenv("DISKWALA_FALLBACK", "1").strip().lower() not in ("0", "false", "no", "off")
DISKWALA_TIMEOUT = max(20, int(os.getenv("DISKWALA_TIMEOUT", "75")))  # whole tier, seconds
STATIC_TOKEN = os.getenv("DISKWALA_TOKEN", "").strip()
SESSION = os.getenv("SESSION", "").strip()  # Telethon StringSession of a user account
# API_ID / API_HASH live in ONE place only: bot.py. It hands them over with set_telegram_credentials() at start-up.
# (DISKWALA_API_ID / DISKWALA_API_HASH can still override them, e.g. when the session belongs to another Telegram app.)
TG_API_ID = os.getenv("DISKWALA_API_ID") or ""
TG_API_HASH = os.getenv("DISKWALA_API_HASH") or ""


def set_telegram_credentials(api_id, api_hash) -> None:
    """Called once by bot.py with its own API_ID / API_HASH (used only for the Telethon SESSION tier)."""
    global TG_API_ID, TG_API_HASH
    if not os.getenv("DISKWALA_API_ID"):
        TG_API_ID = str(api_id or "")
    if not os.getenv("DISKWALA_API_HASH"):
        TG_API_HASH = str(api_hash or "")

UA = "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36"
_URL_KEYS = ("downloadUrl", "download_url", "directUrl", "direct_url", "contentUrl", "content_url",
             "streamUrl", "stream_url", "videoUrl", "video_url", "url", "link")


class DiskwalaError(Exception):
    pass


class DiskwalaAuthError(DiskwalaError):
    """Bearer token rejected (HTTP 401/403): a fresh token, not a retry, is needed."""


def token_tier_available() -> bool:
    return bool(STATIC_TOKEN or (SESSION and TG_API_ID and TG_API_HASH))


# ------------------------------------------------------------------- token --
_auth = {"token": None, "expires": 0.0}
_tg_client = None
_tg_lock: Optional[asyncio.Lock] = None


def _invalidate_token():
    _auth["token"], _auth["expires"] = None, 0.0


async def get_token() -> str:
    global _tg_client, _tg_lock
    if STATIC_TOKEN and not (SESSION and TG_API_ID and TG_API_HASH):
        return STATIC_TOKEN
    if _auth["token"] and time.time() < _auth["expires"]:
        return _auth["token"]
    if not (SESSION and TG_API_ID and TG_API_HASH):
        raise DiskwalaError("no token source configured (set DISKWALA_TOKEN, or SESSION + API_ID/API_HASH)")
    if _tg_lock is None:
        _tg_lock = asyncio.Lock()
    async with _tg_lock:
        if _auth["token"] and time.time() < _auth["expires"]:
            return _auth["token"]
        try:
            from telethon import TelegramClient
            from telethon.sessions import StringSession
            from telethon.tl.functions.messages import RequestAppWebViewRequest
            from telethon.tl.types import DataJSON, InputBotAppShortName, InputPeerSelf
        except ImportError as e:
            raise DiskwalaError(f"telethon not installed: {e}")
        if _tg_client is None:
            _tg_client = TelegramClient(StringSession(SESSION), int(TG_API_ID), TG_API_HASH)
        if not _tg_client.is_connected():
            await _tg_client.connect()
        bot = await _tg_client.get_input_entity("sky577bot")
        r = await _tg_client(RequestAppWebViewRequest(
            peer=InputPeerSelf(), app=InputBotAppShortName(bot_id=bot, short_name="open"),
            platform="android", write_allowed=True, start_param="", theme_params=DataJSON("{}")))
        try:
            frag = urlparse(r.url).fragment
            token = unquote(frag.split("tgWebAppData=", 1)[1].split("&tgWebAppVersion=", 1)[0])
        except Exception:
            raise DiskwalaError("could not read token from mini app url")
        _auth["token"], _auth["expires"] = token, time.time() + 3600
        return token


def decrypt_file(fd: dict) -> dict:
    """AES-GCM reply decrypt. The API has used two byte orders (ct+tag, and p+h as one blob): try both."""
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError as e:
        raise DiskwalaError(f"cryptography not installed: {e}")
    aes = AESGCM(bytes.fromhex(ENCRYPTION_KEY))
    iv, p, h = bytes.fromhex(fd["s"]), bytes.fromhex(fd["p"]), bytes.fromhex(fd["h"])
    last: Exception = ValueError("decrypt failed")
    for blob in (p + h, h + p):
        try:
            return json.loads(aes.decrypt(iv, blob, None).decode("utf-8"))
        except Exception as e:
            last = e
    raise DiskwalaError(f"AES-GCM decryption failed: {last}")


# ---------------------------------------------------------------- helpers --
def _pick(d: dict, *keys):
    for k in keys:
        v = d.get(k)
        if v not in (None, ""):
            return v
    return None


def _to_int(v) -> int:
    try:
        if isinstance(v, str) and ":" in v:
            return sum(int(p) * 60 ** i for i, p in enumerate(reversed(v.split(":"))))
        return int(float(v))
    except Exception:
        return 0


def _usable(link) -> bool:
    """A real media/download link, not an echo of the flezen share page."""
    if not (isinstance(link, str) and link.startswith("http")):
        return False
    u = urlparse(link)
    host = (u.hostname or "").lower()
    return not (("flezen." in host) and u.path.lower().lstrip("/").split("/", 1)[0] in ("s", "share", "f", "v", "d"))


def _norm(file: dict, extra: Optional[dict] = None) -> dict:
    meta = {**file, **(extra or {})}
    link = next((file[k] for k in _URL_KEYS if _usable(file.get(k))), None)
    if not link:
        raise DiskwalaError(f"no download link in reply: {str(file)[:200]}")
    return {
        "link": link,
        "filename": _pick(file, "name", "fileName", "filename", "title"),
        "size": _to_int(_pick(file, "size", "sizeBytes", "fileSize", "length")),
        "thumbnail": next((meta[k] for k in ("thumb", "thumbnail", "thumbnailUrl", "poster", "image")
                           if isinstance(meta.get(k), str) and meta[k].startswith("http")), None),
        "duration": _to_int(_pick(meta, "duration", "duration_seconds", "durationSeconds", "video_duration")),
    }


# ------------------------------------------------------------ token API --
async def _token_resolve_with(share_url: str, session: aiohttp.ClientSession, token: str) -> dict:
    headers = {
        "Authorization": f"Bearer {token}", "X-Bot-Id": "diskwala", "Content-Type": "application/json",
        "Origin": "https://miniapp.diskwala.net", "Referer": "https://miniapp.diskwala.net/", "User-Agent": UA,
    }
    async with session.post(DOWNLOAD_ENDPOINT, headers=headers, json={"link": share_url},
                            timeout=aiohttp.ClientTimeout(total=30, connect=15)) as r:
        if r.status in (401, 403):
            raise DiskwalaAuthError(f"HTTP {r.status}")
        try:
            data = await r.json(content_type=None)
        except Exception:
            data = {"ok": False, "error": f"non-JSON reply (HTTP {r.status})"}
    if not (isinstance(data, dict) and data.get("ok")):
        raise DiskwalaError(str((data or {}).get("error") if isinstance(data, dict) else None) or f"API error: {str(data)[:150]}")

    status_url = f"{STATUS_ENDPOINT}?link={quote(share_url, safe='')}"
    interval = 0.5
    for _ in range(60):
        async with session.get(status_url, headers=headers, timeout=aiohttp.ClientTimeout(total=30, connect=15)) as r:
            if r.status in (401, 403):
                raise DiskwalaAuthError(f"HTTP {r.status} while polling")
            try:
                data = await r.json(content_type=None)
            except Exception:
                raise DiskwalaError(f"status: non-JSON reply (HTTP {r.status})")
        if not data.get("ok"):
            raise DiskwalaError(str(data.get("error") or f"status error: {str(data)[:150]}"))
        st = str(data.get("status", "")).lower()
        if st == "pending":
            await asyncio.sleep(interval)
            interval = min(interval * 1.5, 2.0)
            continue
        if st == "done":
            file = data.get("file")
            if not isinstance(file, dict) or not file:
                raise DiskwalaError(f"no file returned: {str(data)[:150]}")
            if file.get("_x"):
                file = decrypt_file(file)
            return _norm(file, {k: v for k, v in data.items() if k not in ("file", "ok", "status")})
        raise DiskwalaError(f"unexpected status {st!r}")
    raise DiskwalaError("timed out waiting for diskwala status")


async def _token_resolve(share_url: str, session: aiohttp.ClientSession) -> dict:
    token = await get_token()
    try:
        return await _token_resolve_with(share_url, session, token)
    except DiskwalaAuthError as e:
        if not (SESSION and TG_API_ID and TG_API_HASH):
            raise  # a static token cannot be refreshed
        log.warning("diskwala token rejected (%s), refreshing once", e)
        _invalidate_token()
        return await _token_resolve_with(share_url, session, await get_token())


# ------------------------------------------------------------------ entry --
async def resolve(share_url: str, session: aiohttp.ClientSession) -> dict:
    """-> {"link", "filename", "size", "thumbnail", "duration"}. Raises DiskwalaError (the real reason) on failure."""
    if not DISKWALA_ENABLED:
        raise DiskwalaError("diskwala fallback disabled")
    if not token_tier_available():
        raise DiskwalaError("no token configured")
    try:
        return await asyncio.wait_for(_token_resolve(share_url, session), timeout=DISKWALA_TIMEOUT)
    except asyncio.TimeoutError:
        raise DiskwalaError("timed out")
    except DiskwalaError:
        raise
    except aiohttp.ClientError as e:
        raise DiskwalaError(str(e) or e.__class__.__name__)
