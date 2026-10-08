"""Flezen Downloader Telegram bot (Pyrogram/kurigram + aiohttp).

Send a flezen.com share link -> bot resolves it through the Flezen API (playflezen.com/api/extract) -> pick Download or Stream.
"""
import asyncio
import contextlib
import glob
import hashlib
import html
import io
import json
import logging
import os
import re
import shutil
import tempfile
import time
import uuid
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

import aiohttp
from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ParseMode
from pyrogram.errors import FloodWait, InputUserDeactivated, MessageNotModified, PeerIdInvalid, UserIsBlocked, UserNotParticipant
from pyrogram.handlers import CallbackQueryHandler, MessageHandler
from pyrogram.types import (InlineKeyboardButton as Btn, InlineKeyboardMarkup as Markup, LinkPreviewOptions, BotCommand,
                            ReplyKeyboardMarkup, KeyboardButton)
import stream_proxy
import flezen_api as fz
from store import Store

load_dotenv()
# ---- blockquote everywhere ------------------------------------------------
# Every HTML message / caption the bot sends or edits is wrapped in <blockquote>.
# Text that already contains its own <blockquote> (file info card, upload caption) is left untouched,
# because Telegram does not allow nested blockquotes.
BLOCKQUOTE_ALL = os.getenv("BLOCKQUOTE_ALL", "1") != "0"  # set BLOCKQUOTE_ALL=0 to switch off


def _install_blockquote():
    try:
        from pyrogram.parser.html import HTML as _HTMLParser
    except Exception:  # pragma: no cover
        return
    _orig = _HTMLParser.parse

    async def parse(self, text):
        t = str(text if text is not None else "")
        if BLOCKQUOTE_ALL and t.strip() and "<blockquote" not in t.lower():
            t = f"<blockquote>{t.strip()}</blockquote>"
        return await _orig(self, t)

    _HTMLParser.parse = parse


_install_blockquote()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("flezen-bot")


def _req(name: str, default: str = "") -> str:
    v = os.getenv(name, default).strip()
    if not v:
        raise SystemExit(f"Missing required environment variable {name} (see .env.example)")
    return v


# ---------------------------------------------------------------- config ----
# Hardcoded defaults (same as fbot's config.py) -- a real env var still overrides them.
API_ID = int(os.getenv("API_ID", "33029767"))
API_HASH = os.getenv("API_HASH", "5d897bed11bc8b062a12f6c1c3c5360a")
SESSION = os.getenv("SESSION", "1AZWarzcBu05VzVtvhcIZvE8HBtYfT3K6JUeR9n1kvua24ufHs6A-blFqfztzBwgdpBjs7YThEepbfT_JgLZ44l_LnDwD-vSybauAfGu5ccJxnoVMqORpTNgx8j-M9ynKSvSO2wp9b1XBTVZiHjLDYwYe6b0qArzrUFr0X4o5sg_IZeM2rS6Gpla2CHmrfww2_6dmh7Ca9uc3K00Oh1au_AArOikG_drgACfOc4EG5FwWRlZoJIx8OXnFQ_AREuQoKSLAaRxNqWyuPVNURxhE6cq7dzdzmuAW2pHxkl9flUoYDZ7hBNrLDh_G638zTM1gy6C98W4XNnIN7T-LYmkqwnJTOH5_FuE=").strip()  # optional: Telethon StringSession, only the Diskwala token tier needs it
# diskwala_api.py reads these three from the environment, so export the resolved values (same single source: this file)
os.environ["API_ID"], os.environ["API_HASH"], os.environ["SESSION"] = str(API_ID), API_HASH, SESSION
BOT_TOKEN = _req("BOT_TOKEN", "7838427472:AAG_WkPpbKawNnoDPhwxYvC3ppdSZcKwSo4")
OWNER_ID = int(os.getenv("OWNER_ID", "8729304171") or 0)
ADMINS = {OWNER_ID, *(int(x) for x in re.findall(r"\d+", os.getenv("ADMINS", "8931907813")))} - {0}
LOG_CHANNEL = (os.getenv("LOG_CHANNEL", "-1004401290975") or "").strip()
FORCE_SUB = [x.strip() for x in os.getenv("FORCE_SUB", "-1004401290975").split(",") if x.strip()]
# fbot-style file cache: a private channel (bot must be admin) that keeps one copy of every uploaded file, so repeat requests are
# served instantly with copy_message() instead of downloading again. Leave empty to cache by plain file_id only.
_cc = os.getenv("CACHE_CHANNEL_ID", "-1004401290975").strip()
CACHE_CHANNEL_ID = int(_cc) if re.fullmatch(r"-?\d+", _cc) else None
FILE_CACHE = os.getenv("FILE_CACHE", "1").strip().lower() not in ("0", "false", "no", "off")  # FILE_CACHE=0 switches caching off
# extra channels/groups (comma separated ids) that ALSO get a copy of every video, on top of LOG_CHANNEL and /set_channel_id ones
BACKUP_CHANNELS = [int(x) for x in re.findall(r"-?\d+", os.getenv("BACKUP_CHANNELS", "-1004401290975"))]
# Resume interrupted downloads after a restart (partial file + progress are kept; needs DOWNLOAD_DIR to survive the restart)
RESUME_DOWNLOADS = os.getenv("RESUME_DOWNLOADS", "1").strip().lower() not in ("0", "false", "no", "off")
RESUME_MAX_AGE = int(os.getenv("RESUME_MAX_AGE_HOURS", "12") or 12) * 3600  # older interrupted jobs are dropped
RESUME_MAX_TRIES = int(os.getenv("RESUME_MAX_TRIES", "3") or 3)             # stops crash -> resume -> crash loops
SHUTTING_DOWN = [False]   # True while the bot is stopping: running downloads keep their partial files
ACTIVE_RESUME: set = set()  # resume keys of downloads running right now
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "downloads")
MAX_FILE_BYTES = int(os.getenv("MAX_FILE_SIZE_MB", "2000")) * 1024 * 1024  # Telegram bot limit is 2 GB (per part)
SPLIT_LARGE = os.getenv("SPLIT_LARGE", "1").strip().lower() not in ("0", "false", "no", "off")
# biggest file we will download to disk; anything above MAX_FILE_BYTES is split into parts
MAX_DL_BYTES = int(os.getenv("MAX_SPLIT_MB", "8192")) * 1024 * 1024 if SPLIT_LARGE else MAX_FILE_BYTES
MAX_CONCURRENT = max(0, int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "0") or 0))  # 0 = no limit: all downloads run at the same time
# Parallel downloads per user, by plan. Format "tier:limit,..." - tiers: free, plan length in days, lifetime. 0 = unlimited.
# A user gets the limit of the biggest tier their plan reaches. Admins are always unlimited.
PARALLEL_LIMITS_RAW = os.getenv("PARALLEL_LIMITS", "free:1,18:1,35:3,50:5,68:10,100:12,165:15,lifetime:0")
DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "3") or 0)  # free users: downloads per day (premium/admin unlimited); 0 = unlimited
EDIT_INTERVAL = max(3.0, float(os.getenv("PROGRESS_EDIT_INTERVAL", "5")))
BOT_NAME = os.getenv("BOT_NAME", "Flezen Downloader")
LINK_TTL = int(os.getenv("LINK_TTL", "300"))  # seconds a resolved download link is trusted before it is re-resolved
# /start welcome (same look as fbot). Empty START_PHOTO_URL = text only.
START_PHOTO_URL = os.getenv("START_PHOTO_URL", "https://t.me/log_ak_bot/207").strip()
POWERED_BY = os.getenv("POWERED_BY", "Anuj Kumar")
POWERED_BY_URL = os.getenv("POWERED_BY_URL", "https://t.me/anujedits76")

store = Store(os.getenv("DATA_FILE", "bot_data.json"), os.getenv("MONGO_URI", "mongodb+srv://Anujedit:Anujedit@cluster0.7cs2nhd.mongodb.net/?appName=Cluster0").strip(), os.getenv("MONGO_DB_NAME", "flezenbot"))
sem = asyncio.Semaphore(MAX_CONCURRENT) if MAX_CONCURRENT else contextlib.nullcontext()  # nullcontext = unlimited parallel downloads
http: aiohttp.ClientSession  # created in main()
NO_PREVIEW = LinkPreviewOptions(is_disabled=True)
PORT = int(os.environ.get("PORT", "10000"))  # Render sets PORT automatically
START_TIME = time.time()
_START_PHOTO_OK = True

# rid -> {"res": VBResult, "uid": int, "exp": float}   (callback buttons reference results by short id)
RESULTS: dict = {}
# job id -> {"cancel": bool, "uid": int}
JOBS: dict = {}
_invite_cache: dict = {}


# --------------------------------------------------------------- helpers ----
def human_size(n: float) -> str:
    n = float(n or 0)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or u == "TB":
            return f"{n:.0f} {u}" if u == "B" else f"{n:.2f} {u}"
        n /= 1024


def human_time(s: float) -> str:
    s = int(max(0, s))
    h, r = divmod(s, 3600)
    m, s = divmod(r, 60)
    return f"{h}h {m}m" if h else (f"{m}m {s}s" if m else f"{s}s")


def bar(done: float, total: float, w: int = 10) -> str:
    """fbot-style hexagon progress bar."""
    p = min(1.0, done / total) if total else 0
    f = int(p * w)
    return "⬢" * f + "⬡" * (w - f)


_SMALLCAPS_MAP = str.maketrans(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "ᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢ",
)
_TAG_OR_MENTION_RE = re.compile(r"(<[^>]+>|@[A-Za-z][A-Za-z0-9_]{3,31})")


def SC(text: str) -> str:
    """Small-caps the plain text of an HTML message; tags, <code> contents and @mentions stay untouched (like fbot)."""
    out, in_code = [], 0
    for part in _TAG_OR_MENTION_RE.split(text):
        if part.startswith("<") and part.endswith(">"):
            low = part.lower()
            if low.startswith("<code"):
                in_code += 1
            elif low.startswith("</code"):
                in_code = max(0, in_code - 1)
            out.append(part)
        elif part.startswith("@") or in_code:
            out.append(part)
        else:
            out.append(part.translate(_SMALLCAPS_MAP))
    return "".join(out)


try:  # coloured (blue / red) inline buttons, like fbot
    from pyrogram.enums import ButtonStyle
    BTN_PRIMARY, BTN_DANGER = ButtonStyle.PRIMARY, ButtonStyle.DANGER
    BTN_STYLE_IMPORT_OK = True
except Exception:
    BTN_PRIMARY = BTN_DANGER = None
    BTN_STYLE_IMPORT_OK = False
_BTN_STYLE_WARNED = [False]


def mbtn(text: str, callback_data: str = None, url: str = None, style=None) -> Btn:
    kw = {"text": SC(text)}
    if callback_data:
        kw["callback_data"] = callback_data
    if url:
        kw["url"] = url
    if style is not None:
        try:
            return Btn(**kw, style=style)
        except TypeError:  # installed kurigram does not accept the style kwarg -> plain button
            if not _BTN_STYLE_WARNED[0]:
                _BTN_STYLE_WARNED[0] = True
                log.warning("button colours OFF: installed pyrogram/kurigram does not accept InlineKeyboardButton(style=...). "
                            "Run: pip install -U kurigram")
    return Btn(**kw)


def progress_text(kind: str, name: str, done: float, total: float, t0: float, extra: str = "", base: float = 0.0) -> str:
    """fbot 'Fast Downloading via Main Engine' progress card. kind = 'download' | 'upload'."""
    el = max(time.time() - t0, 1e-3)
    speed = max(done - base, 0) / el  # base = bytes that were already on disk when a resumed download started
    eta = (total - done) / speed if total and speed else 0
    pct = min(100.0, done / total * 100) if total else 0
    dl = kind == "download"
    cv = kind == "convert"
    icon = "🎞" if cv else ("📥" if dl else "📤")
    title = "Converting to MP4" if cv else (f"Fast {'Downloading' if dl else 'Uploading'} via Main Engine")
    foot = "⚡ Packing video into MP4 — quality unchanged" if cv else f"⚡ Hyper {kind} connections active"
    unit = "Processed" if cv else ""
    return SC(
        f"{icon} <b>{title}</b>\n\n"
        "╭━━━━❰Progress❱━➣\n"
        f"┣⪼ 🎬 File: <code>{html.escape(name)}</code>\n"
        f"{extra}"
        f"┣⪼ [{bar(done, total)}]\n"
        f"┣⪼ ✅ {pct:.1f}%\n"
        f"┣⪼ 💾 {human_size(done)} / {human_size(total)}\n"
        f"┣⪼ ⚡ {human_size(speed)}/s\n"
        f"┣⪼ 🕐 Elapsed: {human_time(el)}\n"
        f"┣⪼ ⏳ ETA: {human_time(eta)}\n"
        "╰━━━━━━━━━━━━━━━➣\n\n"
        f"{foot}"
    )


def safe_name(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", name).strip(" .") or "file"
    return name[:150]


def gc_results():
    now = time.time()
    for k in [k for k, v in RESULTS.items() if v["exp"] < now]:
        RESULTS.pop(k, None)


def _parse_parallel(raw: str):
    free, life, tiers = 1, 0, []
    for part in (raw or "").split(","):
        if ":" not in part:
            continue
        k, v = (x.strip().lower() for x in part.split(":", 1))
        if not v.isdigit():
            continue
        if k == "free":
            free = int(v)
        elif k in ("lifetime", "life", "forever", "0"):
            life = int(v)
        elif k.isdigit():
            tiers.append((int(k), int(v)))
    return free, life, sorted(tiers)


_PAR_FREE, _PAR_LIFE, _PAR_TIERS = _parse_parallel(PARALLEL_LIMITS_RAW)


def parallel_for_days(days) -> int:
    """Parallel limit of a plan with this many days (None = lifetime). 0 = unlimited."""
    if days is None:
        return _PAR_LIFE
    lim = _PAR_TIERS[0][1] if _PAR_TIERS else _PAR_FREE
    for d, l in _PAR_TIERS:
        if days >= d:
            lim = l
    return lim


def parallel_limit(uid: int) -> int:
    """How many downloads this user may run at the same time. 0 = unlimited."""
    if is_admin(uid):
        return 0
    pi = store.premium_info(uid)
    if not pi["is_premium"]:
        return _PAR_FREE
    if pi["lifetime"]:
        return _PAR_LIFE
    pd = int((store.data["users"].get(str(uid)) or {}).get("plan_days") or 0) or (pi["days_left"] or 0)
    return parallel_for_days(pd)


def is_unlimited(uid: int) -> bool:
    return is_admin(uid) or store.is_premium(uid)


def is_admin(uid: int) -> bool:
    return uid in ADMINS


def clean_url(u, allow_m3u8: bool = False):
    """Make a link safe for a Telegram URL button (percent-encode spaces/unicode, reject junk). None = don't show a button."""
    if not u or not isinstance(u, str):
        return None
    u = u.strip()
    if not u.lower().startswith(("http://", "https://")) or (not allow_m3u8 and ".m3u8" in u.lower()):
        return None
    try:
        p = urlsplit(u)
        host = p.hostname or ""
        if not host or "." not in host or host in ("localhost", "127.0.0.1"):
            return None
        out = urlunsplit((p.scheme, p.netloc, quote(p.path, safe="/%:@!$&'()*+,;=~-._"),
                          quote(p.query, safe="=&%:/?@!$'()*+,;~-._"), ""))
    except Exception:
        return None
    return out if len(out.encode()) <= 1900 else None


def _strip_url_buttons(markup):
    rows = getattr(markup, "inline_keyboard", None)
    if not rows:
        return None
    kept = [[b for b in row if not getattr(b, "url", None)] for row in rows]
    kept = [r for r in kept if r]
    return Markup(kept) if kept else None


async def safe_edit(msg, text: str, markup=None, _retry: bool = True):
    try:
        if getattr(msg, "photo", None):  # thumbnail menu is a photo message -> edit its caption
            await msg.edit_caption(text[:1024], reply_markup=markup, parse_mode=ParseMode.HTML)
        else:
            await msg.edit_text(text, reply_markup=markup, parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)
    except MessageNotModified:
        pass
    except FloodWait as e:
        await asyncio.sleep(min(e.value, 10))
    except Exception as e:
        log.warning("edit failed: %s", e)
        if _retry and markup is not None and any(getattr(b, "url", None) for row in (getattr(markup, "inline_keyboard", None) or []) for b in row):
            # most likely Telegram rejected a URL button (BUTTON_URL_INVALID) -> show the menu without the link buttons
            await safe_edit(msg, text, _strip_url_buttons(markup), _retry=False)


def new_user_text(u) -> str:
    """fbot-style 'New User' notice for the log channel."""
    uname = f"@{u.username}" if getattr(u, "username", None) else "(no username)"
    return ("🆕 <b>New User</b>\n\n"
            f"👤 Name: {html.escape(u.first_name or 'User')}\n"
            f"🔗 Username: {html.escape(uname)}\n"
            f"🆔 ID: <code>{u.id}</code>")


async def log_to_channel(client: Client, text: str):
    if not LOG_CHANNEL:
        return
    try:
        chat = int(LOG_CHANNEL) if re.fullmatch(r"-?\d+", LOG_CHANNEL) else LOG_CHANNEL
        await client.send_message(chat, text, parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)
    except Exception as e:
        log.warning("log channel failed: %s", e)


def user_tag(u) -> str:
    n = html.escape(u.first_name or "user")
    return f'<a href="tg://user?id={u.id}">{n}</a> (<code>{u.id}</code>)'


async def backup_to_linked_channels(client: Client, chat_id: int, message_id: int):
    """fbot-style: copy every delivered video/file into LOG_CHANNEL + BACKUP_CHANNELS + channels linked with /set_channel_id.
    Best effort - failures are only logged and never break the user's download."""
    targets = []
    if LOG_CHANNEL:
        targets.append(int(LOG_CHANNEL) if re.fullmatch(r"-?\d+", LOG_CHANNEL) else LOG_CHANNEL)
    targets += BACKUP_CHANNELS + store.get_channels()
    seen = set()
    for t in targets:
        if t in seen:
            continue
        seen.add(t)
        try:
            await client.copy_message(chat_id=t, from_chat_id=chat_id, message_id=message_id)
        except FloodWait as e:
            await asyncio.sleep(min(e.value, 30))
            try:
                await client.copy_message(chat_id=t, from_chat_id=chat_id, message_id=message_id)
            except Exception as e2:
                log.warning("backup to %s failed: %s", t, e2)
        except Exception as e:
            log.warning("backup to %s failed: %s", t, e)


# ------------------------------------------------------------ referrals ----
REFERRAL_PHOTO_URL = os.getenv("REFERRAL_PHOTO_URL", "https://t.me/log_ak_bot/206")
# (referrals needed, premium days granted when first reached) - rewards stack
REFERRAL_REWARDS = [(5, 1), (10, 1)]
REFERRAL_GOAL = REFERRAL_REWARDS[-1][0]


def referral_link(bot_username: str, uid: int) -> str:
    return f"https://t.me/{bot_username}?start=ref_{uid}"


def referral_text(count: int, limit_hit: bool = False) -> str:
    head = ("⚡ Aapki daily download limit khatam ho gayi hai ya premium expire ho gaya hai.\n\n" if limit_hit else "")
    return smallcaps_html(
        "🎁 <b>Refer &amp; Earn Premium</b>\n\n" + head +
        "💎 Premium free mein chahiye? Dosto ko apna referral link bhejo.\n\n"
        "🏆 <b>Rewards</b>\n"
        "🎯 5 referrals → 💎 1 day premium\n"
        "🎯 10 referrals → 💎 2 days premium\n\n"
        f"📊 Your referrals: {count}/{REFERRAL_GOAL}\n\n"
        "🔗 Link share karo aur premium kamao!")


def referral_kb(bot_username: str, uid: int, count: int) -> Markup:
    link = referral_link(bot_username, uid)
    share = f"https://t.me/share/url?url={quote(link, safe='')}&text={quote('🎬 Flezen videos free download karo Telegram par — ye bot try karo!')}"
    return Markup([
        [mbtn("🔗 Get Referral Link", "ref_getlink", style=BTN_PRIMARY)],
        [mbtn("📤 Share Referral Link", url=share, style=BTN_PRIMARY)],
        [mbtn(f"👥 Referrals: {count}", "ref_count", style=BTN_PRIMARY)],
        [mbtn("💎 Premium Rewards", "ref_rewards", style=BTN_PRIMARY)],
        [mbtn("📞 Contact Admin", url=POWERED_BY_URL, style=BTN_PRIMARY)],
    ])


async def _bot_username(client: Client) -> str:
    if not _ME:
        me = await client.get_me()
        _ME.update(username=me.username or "", name=me.first_name or BOT_NAME)
    return _ME["username"]


async def send_referral_prompt(client: Client, chat_id: int, limit_hit: bool = False):
    uname = await _bot_username(client)
    count = store.referral_count(chat_id)
    kb = referral_kb(uname, chat_id, count)
    text = referral_text(count, limit_hit)
    try:
        m = re.match(r"https?://t\.me/(?:c/(\d+)|([A-Za-z0-9_]+))/(\d+)/?$", REFERRAL_PHOTO_URL.strip())
        if m:  # t.me post link -> copy that post (photo/video) with our caption + buttons
            src = int("-100" + m.group(1)) if m.group(1) else m.group(2)
            await client.copy_message(chat_id, src, int(m.group(3)), caption=text, reply_markup=kb, parse_mode=ParseMode.HTML)
        else:
            await client.send_photo(chat_id, REFERRAL_PHOTO_URL, caption=text, reply_markup=kb, parse_mode=ParseMode.HTML)
    except Exception as e:
        log.warning("referral photo failed, sending text: %s", e)
        await client.send_message(chat_id, text, reply_markup=kb, parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)


async def process_referral(client: Client, new_user, referrer_id: int):
    """Called once for a brand-new user who opened the bot with /start ref_<id>."""
    if referrer_id == new_user.id or not store.set_referrer(new_user.id, referrer_id):
        return
    count = store.add_referral(referrer_id)
    try:
        await client.send_message(referrer_id, SC(f"🎁 <b>Someone joined using your referral link!</b>\n👥 Total referrals: {count}/{REFERRAL_GOAL}"),
                                  parse_mode=ParseMode.HTML)
    except Exception:
        pass
    claimed = store.rewards_claimed(referrer_id)
    for threshold, days in REFERRAL_REWARDS:
        if count >= threshold and threshold not in claimed:
            store.add_premium(referrer_id, days)  # extends an active plan, no-op for lifetime
            store.mark_reward(referrer_id, threshold)
            try:
                await client.send_message(referrer_id, SC(f"🎉 <b>Referral reward!</b>\n\n👥 {threshold} referrals reached — 💎 {days} day(s) premium added!"),
                                          parse_mode=ParseMode.HTML)
            except Exception:
                pass
    await store.flush()
    await log_to_channel(client, f"🎁 <b>Referral</b>\n👤 New: <code>{new_user.id}</code>\n👥 By: <code>{referrer_id}</code> ({count})")


async def on_referral(client: Client, message):
    if store.is_banned(message.from_user.id) and not is_admin(message.from_user.id):
        return
    await send_referral_prompt(client, message.from_user.id)


async def on_ref_getlink(client: Client, cq):
    uname = await _bot_username(client)
    await cq.answer()
    await client.send_message(cq.from_user.id, SC(f"🔗 <b>Your referral link:</b>\n\n<code>{referral_link(uname, cq.from_user.id)}</code>\n\n"
                                                   "Dosto ko ye link do — jab wo bot start karenge to aapko credit milega."),
                              parse_mode=ParseMode.HTML)


async def on_ref_menu(client: Client, cq):
    await cq.answer()
    await send_referral_prompt(client, cq.from_user.id)


async def on_ref_count(client: Client, cq):
    await cq.answer(f"👥 Your referrals: {store.referral_count(cq.from_user.id)}/{REFERRAL_GOAL}", show_alert=True)


async def on_ref_rewards(client: Client, cq):
    await cq.answer("🎯 5 referrals → 1 day premium\n🎯 10 referrals → 2 days premium", show_alert=True)


# ------------------------------------------- admin: link / unlink channels ----
async def on_set_channel(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    args = message.command[1:]
    if not args:
        await message.reply_text(SC("⚠️ <b>Usage:</b> <code>/set_channel_id -100xxxxxxxxxx</code>\n\n"
                                    "Bot us channel/group mein admin hona chahiye. Har video ki copy us channel mein jayegi.\n"
                                    "Ek se zyada channel link kar sakte ho. /channel_id = list, /del_channel_id &lt;id&gt; = unlink."),
                                 parse_mode=ParseMode.HTML)
        return
    raw = args[0]
    if not re.fullmatch(r"-100\d+", raw):
        await message.reply_text(SC("⚠️ ID <code>-100</code> se start honi chahiye, e.g. <code>-1001234567890</code>."), parse_mode=ParseMode.HTML)
        return
    cid = int(raw)
    try:
        chat = await client.get_chat(cid)
        me = await client.get_chat_member(cid, "me")
        if me.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
            await message.reply_text("⚠️ Main us chat mein hoon par admin nahi. Pehle mujhe admin banao.")
            return
    except Exception as e:
        await message.reply_text(f"⚠️ Chat verify nahi hui — pehle bot ko wahan add karo.\n<code>{html.escape(str(e)[:300])}</code>", parse_mode=ParseMode.HTML)
        return
    new = store.add_channel(cid)
    await store.flush()
    await message.reply_text(f"✅ {'Linked' if new else 'Already linked'}: <b>{html.escape(getattr(chat, 'title', None) or str(cid))}</b> (<code>{cid}</code>)",
                             parse_mode=ParseMode.HTML)


async def on_channel_list(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    dyn = store.get_channels()
    static = [c for c in ([int(LOG_CHANNEL)] if re.fullmatch(r"-?\d+", LOG_CHANNEL or "") else []) + BACKUP_CHANNELS if c not in dyn]
    if not dyn and not static:
        await message.reply_text("❌ Koi channel linked nahi hai.\n\nLink karne ke liye: <code>/set_channel_id -100xxxxxxxxxx</code>", parse_mode=ParseMode.HTML)
        return
    lines = []
    for cid in dyn:
        try:
            title = (await client.get_chat(cid)).title or str(cid)
        except Exception:
            title = "(unreachable)"
        lines.append(f"• <b>{html.escape(title)}</b> — <code>{cid}</code>")
    for cid in dict.fromkeys(static):
        lines.append(f"• <code>{cid}</code> — config se (log/backup channel, /del_channel_id se remove nahi hota)")
    await message.reply_text("🔗 <b>Linked Channels</b>\n\n" + "\n".join(lines), parse_mode=ParseMode.HTML)


async def on_del_channel(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    args = message.command[1:]
    if not args:
        n = store.remove_all_channels()
        await store.flush()
        await message.reply_text(f"🗑️ Saare linked channels hata diye ({n}).")
        return
    if not re.fullmatch(r"-?\d+", args[0]):
        await message.reply_text("⚠️ Channel ID number honi chahiye, e.g. <code>-1001234567890</code>.", parse_mode=ParseMode.HTML)
        return
    ok = store.remove_channel(int(args[0]))
    await store.flush()
    await message.reply_text(f"🗑️ Unlinked <code>{args[0]}</code>." if ok else f"⚠️ <code>{args[0]}</code> linked nahi tha (ya config wala hai).", parse_mode=ParseMode.HTML)


# ---------------------------------------------------------- force subscribe ----
async def _invite_link(client: Client, ref: str):
    if ref in _invite_cache:
        return _invite_cache[ref]
    link = None
    try:
        if ref.startswith("http"):
            link = ref
        elif ref.startswith("@") or not re.fullmatch(r"-?\d+", ref):
            link = f"https://t.me/{ref.lstrip('@')}"
        else:
            chat = await client.get_chat(int(ref))
            link = chat.invite_link or (f"https://t.me/{chat.username}" if chat.username else None) \
                or await client.export_chat_invite_link(int(ref))
    except Exception as e:
        log.warning("invite link for %s failed: %s", ref, e)
    _invite_cache[ref] = link
    return link


def _chat_ref(ref: str):
    m = re.search(r"t\.me/(?:\+|joinchat/)", ref)
    if m:  # private invite link: cannot check membership by link, handled as "unknown"
        return None
    m = re.search(r"t\.me/([A-Za-z0-9_]+)", ref)
    if m:
        return m.group(1)
    return int(ref) if re.fullmatch(r"-?\d+", ref) else ref.lstrip("@")


async def missing_channels(client: Client, uid: int) -> list:
    missing = []
    for ref in FORCE_SUB:
        chat = _chat_ref(ref)
        if chat is None:
            continue
        try:
            m = await client.get_chat_member(chat, uid)
            if m.status in (ChatMemberStatus.BANNED, ChatMemberStatus.LEFT):
                missing.append(ref)
        except UserNotParticipant:
            missing.append(ref)
        except Exception as e:  # bot not admin / bad ref: never lock users out because of config errors
            log.warning("force-sub check for %s failed: %s", ref, e)
    return missing


async def gate(client: Client, message) -> bool:
    """False (and a reply is sent) if the user is banned or has not joined the force-sub channels."""
    uid = message.from_user.id
    if store.is_banned(uid) and not is_admin(uid):
        await message.reply_text("🚫 You are banned from using this bot.")
        return False
    if FORCE_SUB and not is_admin(uid):
        miss = await missing_channels(client, uid)
        if miss:
            rows = []
            for i, ref in enumerate(miss, 1):
                link = await _invite_link(client, ref)
                if link:
                    rows.append([mbtn(f"📢 Join Channel {i}", url=link, style=BTN_PRIMARY)])
            rows.append([mbtn("✅ Verify", "verify", style=BTN_PRIMARY)])
            await message.reply_text(
                "🔒 <b>Access denied</b>\n\nPlease join our channel(s) first, then tap <b>Verify</b>.",
                reply_markup=Markup(rows), parse_mode=ParseMode.HTML)
            return False
    return True


# ------------------------------------------------------------- handlers ----
_SMALLCAPS_MAP = str.maketrans(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "ᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢ",
)
_TAG_RE = re.compile(r"(<[^>]+>)")


def smallcaps(text: str) -> str:
    return text.translate(_SMALLCAPS_MAP)


def smallcaps_html(text: str) -> str:
    """Small-caps all plain text; HTML tags and <code>...</code> contents stay as typed."""
    out, in_code = [], 0
    for part in _TAG_RE.split(text):
        if part.startswith("<") and part.endswith(">"):
            low = part.lower()
            if low.startswith("<code"):
                in_code += 1
            elif low.startswith("</code"):
                in_code = max(0, in_code - 1)
            out.append(part)
        else:
            out.append(part if in_code else part.translate(_SMALLCAPS_MAP))
    return "".join(out)


_ME = {}  # cached bot identity (set on first /start)


def start_caption(first_name: str, bot_username: str, bot_name: str) -> str:
    name = html.escape(smallcaps(first_name or "there"))
    bot = html.escape(smallcaps(bot_name or BOT_NAME))
    head = (
        f"<b>👋 {smallcaps('Hello')} {name},</b>\n"
        f"<b>🤖 {smallcaps('I am')} <a href=\"https://t.me/{bot_username}\">{bot}</a></b>\n\n"
    )
    body = smallcaps_html(
        "⚡ I'm a very powerful Flezen downloader bot.\n\n"
        "📥 Simply send me any Flezen link, and I'll fetch the video and a stream link for you in seconds.\n\n"
        "🚀 Ultra-fast processing\n"
        "🎬 Instant video extraction\n"
        "⚡ Lightning-speed downloads\n"
        "🛡️ Reliable & stable service\n"
        "🔗 Just paste your Flezen link below and let the magic begin!\n\n"
        "✅ ʏᴇ ʟɪɴᴋ ꜱᴜᴘᴘᴏʀᴛᴇᴅ ʜᴀɪ:\n"
        "• <code>https://flezen.com/s/...</code>\n\n"
    )
    powered = f'<a href="{html.escape(POWERED_BY_URL, quote=True)}">{html.escape(smallcaps(POWERED_BY))}</a>'
    foot = (
        "━━━━━━━━━━━━━━━\n"
        f"👑 {smallcaps('Powered by')} {powered}\n"
        f"⚡ {smallcaps('Speed')} • {smallcaps('Performance')} • {smallcaps('Reliability')}\n"
        "━━━━━━━━━━━━━━━"
    )
    return head + body + foot


HELP_TEXT = smallcaps_html(
    "ℹ️ <b>How to use</b>\n\n"
    "🔹 <b>Just send the link:</b>\n"
    "Paste any Flezen share link directly in the chat.\n\n"
    "🔹 <b>Supported link format:</b>\n"
    "<code>flezen.com/s/&lt;id&gt;</code>\n\n"
    "📌 <b>Example:</b>\n"
    "<code>https://flezen.com/s/abc123</code>\n\n"
    "💡 <b>Tips:</b>\n"
    f"• Files up to {human_size(MAX_FILE_BYTES)} are uploaded directly — bigger ones (up to {human_size(MAX_DL_BYTES)}) are split into parts automatically\n"
    "• Use the Stream button if you just want a link\n"
    "• If a download fails, just send the link again\n"
    "• Use <code>/cancel</code> to stop an active download\n"
    "• <code>/settings</code> — custom caption, thumbnail &amp; dump chat\n"
    "• <code>/referral</code> — refer friends &amp; earn free premium\n"
    "• <code>/plans</code> — premium plans\n\n"
    "Having trouble? Make sure you're sending a valid Flezen link."
)



# ------------------------------------------------- fbot-style menus ----
def rbtn(text: str, style=None):
    """Bottom (reply) keyboard button, coloured when the pyrogram build supports it."""
    if style is not None:
        try:
            return KeyboardButton(text=text, style=style)
        except TypeError:
            pass
    return text


BTN_PLANS, BTN_MYSTATUS, BTN_HELP, BTN_SUPPORT = "💎 ᴘʟᴀɴs", "📊 ᴍʏ sᴛᴀᴛᴜs", "❓ ʜᴇʟᴘ", "☎️ sᴜᴘᴘᴏʀᴛ"
MENU_BUTTON_TEXTS = [BTN_PLANS, BTN_MYSTATUS, BTN_HELP, BTN_SUPPORT]

MAIN_MENU_KB = ReplyKeyboardMarkup(
    [[rbtn(BTN_PLANS, BTN_PRIMARY), rbtn(BTN_MYSTATUS, BTN_PRIMARY)],
     [rbtn(BTN_HELP, BTN_PRIMARY), rbtn(BTN_SUPPORT, BTN_PRIMARY)]],
    resize_keyboard=True,
)


def _tolerant(expected: str) -> str:
    """Regex that ignores the invisible U+FE0F selector some clients drop/add (☎️, ❓)."""
    base = expected.replace("\ufe0f", "")
    return "^" + r"\ufe0f?".join(re.escape(c) for c in base) + r"\ufe0f?$"


def menu_text_filter(expected: str):
    return filters.regex(_tolerant(expected))


NOT_MENU_BUTTON = ~filters.regex("|".join(f"(?:{_tolerant(t)})" for t in MENU_BUTTON_TEXTS))

FALLBACK_TEXT = "👇 Apna Flezen link bhejo boss!"

PLANS_RAW = os.getenv("PLANS", "19:12,29:21,45:35,99:99,999:lifetime")
PLANS_PHOTO_URL = os.getenv("PLANS_PHOTO_URL", "https://iili.io/nHyIqox.jpg").strip()  # URL or t.me post link
QR_CODE_URL = os.getenv("QR_CODE_URL", "https://iili.io/nHyIqox.jpg").strip()           # "Scan to Pay" link
UPI_ID = os.getenv("UPI_ID", "971916880@ybl")
PAID_CLAIM_COOLDOWN = int(os.getenv("PAID_CLAIM_COOLDOWN", "300") or 300)  # seconds between "I've Paid" claims
_PAID_TS: dict = {}
_TG_POST_RE = re.compile(r"^https?://t\.me/(?:(c)/(\d+)|([A-Za-z0-9_]+))/(\d+)/?(?:\?.*)?$")


PLAN_EXTRA: dict = {}  # price -> bonus days (already included in the granted total)


def _parse_plans(raw: str) -> list:
    out = []
    for part in (raw or "").split(","):
        if ":" not in part:
            continue
        price, days = (x.strip().lower() for x in part.split(":", 1))
        if not price.isdigit():
            continue
        if days in ("0", "life", "lifetime", "forever"):
            out.append((int(price), None))
            continue
        base, _, extra = days.partition("+")
        base, extra = base.strip(), extra.strip()
        if base.isdigit() and int(base) > 0 and (not extra or extra.isdigit()):
            if extra and int(extra) > 0:
                PLAN_EXTRA[int(price)] = int(extra)
            out.append((int(price), int(base) + (int(extra) if extra else 0)))
    return out


PLANS = _parse_plans(PLANS_RAW)  # [(price, total_days|None)]
PLAN_DAYS = dict(PLANS)


def _plan_label(days, price=None) -> str:
    if days is None:
        return "Lifetime Access ♾️"
    extra = PLAN_EXTRA.get(price, 0) if price is not None else 0
    if extra:
        return f"{days - extra} Days + {extra} Extra Days 🎁"
    return f"{days} Days"


async def send_photo_or_text(client: Client, chat_id: int, src: str, text: str, markup=None):
    """Photo (URL or t.me post link) with `text` as caption; plain message if it fails."""
    src = (src or "").strip()
    if src:
        try:
            tg = _TG_POST_RE.match(src)
            if tg:
                is_c, chan_id, username, msg_id = tg.groups()
                chat = int(f"-100{chan_id}") if is_c else username
                return await client.copy_message(chat_id, chat, int(msg_id), caption=text, reply_markup=markup, parse_mode=ParseMode.HTML)
            return await client.send_photo(chat_id, src, caption=text, reply_markup=markup, parse_mode=ParseMode.HTML)
        except Exception as e:
            log.warning("photo send failed (%s): %s", src[:40], e)
    return await client.send_message(chat_id, text, reply_markup=markup, parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)


def fallback_kb() -> Markup:
    """Inline [Download] [Status] buttons shown under the /start message."""
    return Markup([[mbtn("📥 Download", "fallback_download", style=BTN_PRIMARY),
                    mbtn("📊 Status", "fallback_status", style=BTN_PRIMARY)]])


def plans_text() -> str:
    lines = "\n".join(f"• ₹{p} → {_plan_label(d, p)} • ⚡ {parallel_for_days(d) or 'Unlimited'} parallel" for p, d in PLANS) or "• Admin se contact karo"
    return smallcaps_html(
        "💎 <b>Premium Membership Plans</b>\n"
        "✨ Unlock Unlimited Access & Advanced Features!\n\n"
        f"{lines}\n\n"
        "🔒 <b>Secure Payment:</b>\n"
        f"⚡️ UPI ID: <code>{html.escape(UPI_ID)}</code>\n"
        "💡 After Payment: Send Screenshot to Admin for Instant Activation.\n\n"
        "👇 Plan pe tap karo — shuru ho jao!")


def plans_kb() -> Markup:
    rows = [[mbtn(f"💎 ₹{p} - {_plan_label(d, p)}", f"plan_{p}", style=BTN_PRIMARY)] for p, d in PLANS]
    rows.append([mbtn("📸 Send Payment Proof", url=POWERED_BY_URL, style=BTN_PRIMARY)])
    rows.append([mbtn("⬅️ Back", "plans_back", style=BTN_DANGER)])
    return Markup(rows)


def payment_kb(price: int) -> Markup:
    return Markup([[mbtn("✅ I've Paid", f"paid_{price}", style=BTN_PRIMARY)],
                   [mbtn("📸 Send Payment Proof", url=POWERED_BY_URL, style=BTN_PRIMARY)],
                   [mbtn("⬅️ Back", "plans_back", style=BTN_DANGER)]])


def my_status_text(uid: int) -> str:
    u = store.data["users"].get(str(uid), {})
    pi = store.premium_info(uid)
    if is_admin(uid):
        plan = "👑 Admin (Unlimited)"
    elif pi["lifetime"]:
        plan = "💎 Premium (Lifetime ♾️)"
    elif pi["is_premium"]:
        plan = f"💎 Premium ({pi['days_left']} day{'s' if pi['days_left'] != 1 else ''} left)"
    else:
        plan = "Free"
    today = store.downloads_today(uid)
    limit = "Unlimited" if (DAILY_LIMIT == 0 or is_unlimited(uid)) else f"{today}/{DAILY_LIMIT} ({max(0, DAILY_LIMIT - today)} left)"
    return smallcaps_html(
        "<b>📊 Your Status</b>\n\n"
        f"User ID: <code>{uid}</code>\n"
        f"Plan: <code>{plan}</code>\n"
        f"Total Downloads: <code>{u.get('dl', 0)}</code>\n"
        f"Today's downloads: {limit}\n"
        f"Parallel downloads: {parallel_limit(uid) or 'Unlimited'}")


def status_kb() -> Markup:
    return Markup([[mbtn("💎 View Plans", "show_plans", style=BTN_PRIMARY)],
                   [mbtn("🎁 Refer & Earn", "ref_menu", style=BTN_PRIMARY)],
                   [mbtn("📞 Contact Admin", url=POWERED_BY_URL, style=BTN_PRIMARY)]])


async def send_plans(client: Client, chat_id: int):
    await send_photo_or_text(client, chat_id, PLANS_PHOTO_URL, plans_text(), plans_kb())


async def on_plans(client: Client, message):
    await send_plans(client, message.chat.id)


async def on_my_status(client: Client, message):
    await message.reply_text(my_status_text(message.from_user.id), reply_markup=status_kb(), parse_mode=ParseMode.HTML)


async def on_support(client: Client, message):
    await message.reply_text(
        smallcaps_html("📞 <b>Support</b>\n\nKoi problem? Idhar baat karo:\n\n"
                       f"👤 Admin: <a href=\"{html.escape(POWERED_BY_URL, quote=True)}\">{html.escape(POWERED_BY)}</a>\n\n"
                       "⏰ 24 ghante ke andar reply, pakka!"),
        parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)


async def on_fallback_download(client: Client, cq):
    await cq.answer(SC(FALLBACK_TEXT), show_alert=True)


async def on_fallback_status(client: Client, cq):
    await cq.message.reply_text(my_status_text(cq.from_user.id), reply_markup=status_kb(), parse_mode=ParseMode.HTML)
    await cq.answer()


async def on_show_plans(client: Client, cq):
    await send_plans(client, cq.message.chat.id)
    await cq.answer()


async def on_plans_back(client: Client, cq):
    try:
        await cq.message.delete()
    except Exception:
        pass
    await cq.answer()


async def on_plan_selected(client: Client, cq):
    price = int(cq.data.split("_", 1)[1])
    if price not in PLAN_DAYS:
        return await cq.answer(SC("Ye plan ab available nahi hai."), show_alert=True)
    how = (f"📱 Kisi bhi UPI app se pay karo (PhonePe / GPay / Paytm):\n<code>{html.escape(UPI_ID)}</code>\n\n"
           if UPI_ID else "📱 Payment details ke liye admin se contact karo.\n\n")
    qr = "🔗 §§§\n\n" if QR_CODE_URL else ""
    text = smallcaps_html(
        f"💳 <b>{_plan_label(PLAN_DAYS[price], price)}</b> ke liye Payment\n\n"
        f"Amount: ₹{price}\n\n" + how + qr +
        "Payment ke baad 'I've Paid' dabao aur screenshot admin ko bhejo.")
    if QR_CODE_URL:  # inserted after small-caps so "Scan to Pay" keeps normal letters
        text = text.replace("§§§", f'{smallcaps("QR Code")}: <a href="{html.escape(QR_CODE_URL, quote=True)}">Scan to Pay</a>')
    await cq.answer()
    await send_photo_or_text(client, cq.message.chat.id, PLANS_PHOTO_URL, text, payment_kb(price))


async def on_paid(client: Client, cq):
    price = int(cq.data.split("_", 1)[1])
    if price not in PLAN_DAYS:
        return await cq.answer(SC("Ye plan ab available nahi hai."), show_alert=True)
    days = PLAN_DAYS[price]
    user = cq.from_user
    now = time.time()
    if now - _PAID_TS.get(user.id, 0) < PAID_CLAIM_COOLDOWN:
        return await cq.answer(SC("⏳ Claim already bheja ja chuka hai. Thodi der baad dobara try karo."), show_alert=True)
    _PAID_TS[user.id] = now
    if len(_PAID_TS) > 2000:
        for k in [k for k, t in _PAID_TS.items() if now - t > PAID_CLAIM_COOLDOWN]:
            _PAID_TS.pop(k, None)
    claim = (f"🔔 <b>Payment Claim</b>\n\nUser: {user_tag(user)}\n"
             f"Plan: ₹{price} - {_plan_label(days, price)}\n\n"
             f"Screenshot verify karke chalao:\n<code>/addpremium {user.id} {days or 'lifetime'}</code>")
    for admin_id in ADMINS:
        try:
            await client.send_message(admin_id, smallcaps_html(claim), parse_mode=ParseMode.HTML)
        except Exception as e:
            log.warning("failed to notify admin %s: %s", admin_id, e)
    await log_to_channel(client, f"🔔 <b>Payment Claim</b>\n\n👤 {user_tag(user)}\nPlan: ₹{price} - {_plan_label(days, price)}")
    await cq.answer(SC("✅ Admin ko notify kar diya!"), show_alert=True)
    await cq.message.reply_text(
        smallcaps_html("✅ Aapka payment claim admin ko bhej diya gaya hai.\n"
                       f'Jaldi verification ke liye screenshot bhi bhej do: <a href="{html.escape(POWERED_BY_URL, quote=True)}">{html.escape(POWERED_BY)}</a>'),
        parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)


async def on_start(client: Client, message):
    u = message.from_user
    if store.add_user(u.id, u.first_name or ""):
        await log_to_channel(client, new_user_text(u))
        arg = message.command[1] if len(message.command) > 1 else ""
        if arg.startswith("ref_") and arg[4:].isdigit():
            await process_referral(client, u, int(arg[4:]))
    if not await gate(client, message):
        return
    if not _ME:
        me = await client.get_me()
        _ME.update(username=me.username or "", name=me.first_name or BOT_NAME)
    caption = start_caption(u.first_name, _ME["username"], _ME["name"])
    global _START_PHOTO_OK
    sent_photo = False
    if START_PHOTO_URL and _START_PHOTO_OK:
        try:
            m = re.fullmatch(r"https?://t\.me/([A-Za-z0-9_]+)/(\d+)", START_PHOTO_URL)
            if m:  # t.me post link is not an image url -> copy that post (photo) with our caption
                coro = client.copy_message(message.chat.id, m.group(1), int(m.group(2)),
                                           caption=caption, parse_mode=ParseMode.HTML, reply_markup=fallback_kb())
            else:
                coro = message.reply_photo(START_PHOTO_URL, caption=caption, parse_mode=ParseMode.HTML, reply_markup=fallback_kb())
            await asyncio.wait_for(coro, timeout=12)
            sent_photo = True
        except Exception as e:
            _START_PHOTO_OK = False  # don't make every /start wait on a broken photo
            log.warning("start photo failed (disabled until restart), sending text: %s", e)
    if sent_photo:
        await message.reply_text(SC(FALLBACK_TEXT), reply_markup=MAIN_MENU_KB)
        return
    await message.reply_text(caption, parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW, reply_markup=fallback_kb())
    await message.reply_text(SC(FALLBACK_TEXT), reply_markup=MAIN_MENU_KB)


async def on_help(client: Client, message):
    if await gate(client, message):
        await message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML)


# ------------------------------------------------------------------ about ----
HOSTING_TEXT = os.getenv("HOSTING_TEXT", "Dedicated High-Speed Server")


DEVELOPER_URL = "https://t.me/anujedits76"


def about_text(bot_username: str) -> str:
    bot_link = f"https://t.me/{bot_username}" if bot_username else POWERED_BY_URL
    dev = html.escape(smallcaps(POWERED_BY))
    return (
        f"💠 {smallcaps('About This Bot')} 💠\n\n"
        f"╭────[ ✨ {html.escape(smallcaps(POWERED_BY.split()[0]))} ]────⍟\n"
        f"├⍟ 🚀 {smallcaps('Bot Name')}  : <a href=\"{bot_link}\">{smallcaps('Flezen Downloader Bot')}</a>\n"
        f"├⍟ 👨‍💻 {smallcaps('Developer')}  : <a href=\"{DEVELOPER_URL}\">{dev}</a>\n"
        f"├⍟ 🔗 {smallcaps('Library')}  : <a href=\"https://docs.pyrogram.org/\">{smallcaps('Pyrogram Async')}</a>\n"
        f"├⍟ ⚡️ {smallcaps('Language')}  : <a href=\"https://www.python.org/\">{smallcaps('Python')} 3.12</a>\n"
        f"├⍟ ⚙️ {smallcaps('Database')}  : <a href=\"https://www.mongodb.com/\">{smallcaps('MongoDB')}</a>\n"
        f"├⍟ ⭐️ {smallcaps('Hosting')}  :  {smallcaps(HOSTING_TEXT)}\n"
        "╰───────────────⍟"
    )


async def on_about(client: Client, message):
    if not _ME:
        me = await client.get_me()
        _ME.update(username=me.username or "", name=me.first_name or BOT_NAME)
    await message.reply_text(about_text(_ME["username"]), parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW,
                             reply_markup=Markup([[mbtn("❌ Close", "about_close", style=BTN_DANGER)]]))


async def on_about_close(client: Client, cq):
    try:
        await cq.message.delete()
    except Exception:
        pass
    await cq.answer()


async def on_cancel(client: Client, message):
    n = 0
    for j in JOBS.values():
        if j["uid"] == message.from_user.id and not j["cancel"]:
            j["cancel"] = True
            n += 1
    await message.reply_text("🛑 Cancelling…" if n else "No active download.")


async def on_verify(client: Client, cq):
    miss = await missing_channels(client, cq.from_user.id)
    if miss:
        await cq.answer("❌ You have not joined all channels yet.", show_alert=True)
        return
    await cq.answer("✅ Verified!")
    try:
        await cq.message.edit_text("✅ <b>Verified!</b> Now send me a Flezen link.", parse_mode=ParseMode.HTML)
    except Exception:
        pass




def file_menu_kb(rid: str, idx: int, f: fz.FZFile) -> Markup:
    """'Choose an action' keyboard: Download / Stream Link / Cancel."""
    rows = [[mbtn("🔽 Download", f"dl:{rid}:{idx}", style=BTN_PRIMARY)],
            [mbtn("🔗 Stream Link", f"st:{rid}:{idx}", style=BTN_PRIMARY)]]
    rows.append([mbtn("❌ Cancel", f"cx:{rid}", style=BTN_DANGER)])
    return Markup(rows)


def stream_card(rid: str, idx: int, f: fz.FZFile, su: str = "", bu: str = ""):
    """'Stream Link Ready': 🔗 Open Stream = our /stream relay (plays inline like fbot's, even for Flezen's .bin links),
    plus ▶️ Direct (VLC / MX) = the raw Flezen CDN link. Without a relay link only the direct one is shown."""
    su = clean_url(su, allow_m3u8=True)
    bu = clean_url(bu)
    if su:
        if bu:
            rows = [[mbtn("🔗 Open Stream", url=bu, style=BTN_PRIMARY)],
                    [mbtn("▶️ Direct (VLC / MX Player)", url=su, style=BTN_PRIMARY)]]
        else:
            rows = [[mbtn("🔗 Open Stream", url=su, style=BTN_PRIMARY)]]
        return (SC(f"<b>Stream Link Ready</b>\n\nName: <code>{html.escape(f.name)}</code>\nSize: <code>{human_size(f.size)}</code>"),
                Markup(rows))
    msg = (
        "<b>❌ No stream link found for this video</b>\n\n"
        "This can happen if:\n"
        "• The Flezen link has expired or was removed\n"
        "• The stream service is temporarily unavailable\n"
        "• The bot has no public URL set (PUBLIC_URL)\n\n"
        "Try again in a few minutes."
    )
    return SC(msg), Markup([[mbtn("⬅️ Back", f"m:{rid}:{idx}", style=BTN_DANGER)]])


_CATEGORIES = (
    ("Video", {"mp4", "mkv", "mov", "avi", "webm", "m4v", "ts", "flv", "3gp", "wmv", "mpg", "mpeg", "m3u8"}),
    ("Audio", {"mp3", "m4a", "wav", "flac", "aac", "ogg", "opus", "wma"}),
    ("Image", {"jpg", "jpeg", "png", "gif", "webp", "bmp", "heic", "svg"}),
    ("Archive", {"zip", "rar", "7z", "tar", "gz", "bz2", "xz", "iso"}),
    ("Document", {"pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "epub", "csv", "json", "md"}),
    ("App", {"apk", "exe", "msi", "dmg", "deb"}),
)


def file_category(name: str) -> str:
    """'movie.MP4' -> 'Video', 'a.zip' -> 'Archive', unknown/no extension -> 'File'."""
    ext = (name or "").rsplit(".", 1)[-1].lower() if "." in (name or "") else ""
    return next((cat for cat, exts in _CATEGORIES if ext in exts), "File")


def meta_block(f: fz.FZFile, url: str = "") -> str:
    """fbot-style info block: title + blockquote with file details (only lines we actually know)."""
    lines = [f"📄 {smallcaps('File Name')}: {html.escape(smallcaps(f.name[:90]))}",
             f"📦 {smallcaps('Size')}: {human_size(f.size)}"]
    if f.duration:
        h, r = divmod(int(f.duration), 3600)
        m, sec = divmod(r, 60)
        lines.append(f"⏱️ {smallcaps('Duration')}: " + (f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"))
    lines.append(f"🏷️ {smallcaps('Category')}: {html.escape(smallcaps(file_category(f.name)))}")
    if f.ctime > 946684800:
        lines.append(f"📅 {smallcaps('Uploaded')}: {time.strftime('%Y-%m-%d', time.gmtime(f.ctime))}")
    if url.startswith("http"):
        lines.append(f"🔗 {smallcaps('Source')}: Flezen")
    icon = "🎬" if f.is_video else "📄"
    return f"{icon} <b>{html.escape(smallcaps(f.name[:90].rsplit('.', 1)[0]))}</b>\n\n<blockquote>{chr(10).join(lines)}</blockquote>"


def menu_text(url: str, f: fz.FZFile = None) -> str:
    head = f"<b>{smallcaps('Link received')}</b>\n<code>{html.escape(url)}</code>\n\n"
    if not f:
        return head + smallcaps("Choose an action:")
    return head + meta_block(f, url) + f"\n\n{smallcaps('Choose an action:')}"


def file_card(f: fz.FZFile) -> str:
    return meta_block(f) + f"\n\n{smallcaps('Choose an action:')}"


async def fetch_thumb(url: str):
    """Download a thumbnail ourselves (Telegram often cannot fetch CDN urls). Returns BytesIO or None."""
    if not url or not url.startswith("http"):
        return None
    try:
        async with http.get(url, headers=DL_HEADERS, timeout=aiohttp.ClientTimeout(total=6, connect=4)) as r:
            if r.status != 200 or "image" not in (r.headers.get("Content-Type") or ""):
                return None
            data = await r.content.read(5 * 1024 * 1024)
        bio = io.BytesIO(data)
        bio.name = "thumb.jpg"
        return bio if len(data) > 500 else None
    except BaseException as e:  # never let a thumbnail problem break the menu (but still honour task cancellation)
        if isinstance(e, asyncio.CancelledError):
            raise
        log.info("thumbnail skipped (%s): %s", url[:80], e.__class__.__name__)
        return None


async def _frame_from(src: str, hdr: str):
    """Duration via ffprobe, then one frame at 10% of the video - the same spot make_thumb() uses for the sent video."""
    net = ["-headers", hdr, "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "2"]
    dur = 0.0
    rc, out = await _run("ffprobe", "-v", "error", *net, "-show_entries", "format=duration", "-of", "csv=p=0", src, timeout=8)
    if rc == 0:
        try:
            dur = float(out.decode().strip().splitlines()[0])
        except Exception:
            dur = 0.0
    at = max(1, int(dur * 0.1)) if dur else 8
    vf = "scale='if(gt(iw,ih),960,-2)':'if(gt(iw,ih),-2,960)':flags=lanczos"
    rc, img = await _run("ffmpeg", "-nostdin", "-loglevel", "error", *net, "-ss", str(at), "-i", src, "-frames:v", "1",
                         "-vf", vf, "-q:v", "4", "-f", "image2pipe", "-vcodec", "mjpeg", "-", timeout=12)
    return img if rc == 0 and len(img) > 2000 else None


async def frame_thumb(f: fz.FZFile):
    """Sharp menu preview: a frame cut from the remote video with ffmpeg at the same spot (10%) as the thumbnail of the
    sent video, so both look alike. Tries the stream and download links. Returns BytesIO or None."""
    if not (shutil.which("ffmpeg") and shutil.which("ffprobe")) or not f.is_video:
        return None
    hdr = f"User-Agent: {DL_HEADERS['User-Agent']}\r\nReferer: {DL_HEADERS['Referer']}\r\n"
    srcs = list(dict.fromkeys(u for u in (f.download_url, f.stream_url) if u and u.startswith("http") and not is_hls_url(u)))

    async def run():
        for src in srcs:
            img = await _frame_from(src, hdr)
            if img:
                return img
        return None
    try:
        img = await asyncio.wait_for(run(), timeout=FRAME_TIMEOUT)
    except Exception:
        return None
    if not img:
        return None
    bio = io.BytesIO(img)
    bio.name = "thumb.jpg"
    return bio


FRAME_TIMEOUT = 20  # max seconds to wait for the sharp video frame before falling back to the API thumbnail


THUMB_HD = os.getenv("THUMB_HD", "1") == "1"  # 1 = sharper thumbnail (bigger size, re-compressed so it still loads fast); 0 = as the source gives it
THUMB_MAX_SIDE = int(os.getenv("THUMB_MAX_SIDE", "800"))      # px - plenty for a chat card, loads fast even on slow mobile data
THUMB_MAX_KB = int(os.getenv("THUMB_MAX_KB", "0"))        # 0 = never re-compress: full HD exactly as the source gives it (default). e.g. 150 = shrink bigger thumbnails to <=150 KB


async def shrink_thumb(bio):
    """Big thumbnails load slowly on weak mobile data (half-loaded card). Re-compress to <= THUMB_MAX_SIDE px / small JPEG."""
    try:
        data = bio.getvalue()
        if THUMB_MAX_KB <= 0 or len(data) <= THUMB_MAX_KB * 1024 or not shutil.which("ffmpeg"):
            return bio
        with tempfile.TemporaryDirectory() as d:
            src, dst = os.path.join(d, "in.img"), os.path.join(d, "out.jpg")
            with open(src, "wb") as fh:
                fh.write(data)
            vf = f"scale='if(gt(iw,ih),min(iw,{THUMB_MAX_SIDE}),-2)':'if(gt(iw,ih),-2,min(ih,{THUMB_MAX_SIDE}))':flags=lanczos"
            best = None
            for q in ("2", "3", "5", "8", "12", "18"):  # raise compression until it fits the size limit
                rc, _ = await _run("ffmpeg", "-nostdin", "-loglevel", "error", "-y", "-i", src, "-vf", vf, "-frames:v", "1", "-q:v", q, dst, timeout=15)
                if rc != 0 or not os.path.exists(dst):
                    break
                with open(dst, "rb") as fh:
                    best = fh.read()
                if len(best) <= THUMB_MAX_KB * 1024:
                    break
            if best and 2000 < len(best) < len(data):
                log.info("thumbnail shrunk %d KB -> %d KB", len(data) // 1024, len(best) // 1024)
                nb = io.BytesIO(best)
                nb.name = "thumb.jpg"
                return nb
        return bio
    except Exception as e:
        log.info("thumbnail shrink skipped: %s", e)
        return bio


async def fetch_thumb_hd(url: str):
    """Link thumbnail as BytesIO (or None). `src_url` lets send_photo_card hand Telegram the URL directly."""
    img = await fetch_thumb(url)
    if img is not None:
        img.seek(0)
        img.src_url = url
    return img


MENU_THUMB = os.getenv("MENU_THUMB", "link").lower()  # "link" = the link's own thumbnail (default), "frame" = frame cut from the video


MENU_PREVIEW = os.getenv("MENU_PREVIEW", "1") == "1"  # 1 (default) = thumbnail card, info is held back until the thumbnail has fully loaded; 0 = text-only card


async def get_menu_thumb(f: fz.FZFile):
    """Thumbnail for the file card. Default: the video's own thumbnail from the API;
    a frame cut from the video is only used when the link has no thumbnail (or MENU_THUMB=frame puts it first)."""
    if not MENU_PREVIEW:  # no preview on the info card - thumbnail is fetched only after the full download (see download flow)
        return None
    if MENU_THUMB == "frame":
        order = [("video frame", lambda: frame_thumb(f)), ("link thumbnail", lambda: (fetch_thumb_hd(f.thumb) if THUMB_HD else fetch_thumb(f.thumb)))]
    else:
        order = [("link thumbnail", lambda: (fetch_thumb_hd(f.thumb) if THUMB_HD else fetch_thumb(f.thumb))), ("video frame", lambda: frame_thumb(f))]
    for name, fn in order:
        try:
            img = await fn()
        except Exception:
            img = None
        if img:
            if THUMB_HD:
                img = await shrink_thumb(img)
            img.seek(0)
            log.info("menu thumbnail: %s", name)
            return img
    log.info("menu thumbnail: none")
    return None


THUMB_WAIT_KBPS = float(os.getenv("THUMB_WAIT_KBPS", "0"))  # assumed phone download speed (KB/s) used to time the info card
THUMB_URL_FIRST = os.getenv("THUMB_URL_FIRST", "1") == "1"  # 1 = give Telegram the thumbnail URL directly (fbot style), upload bytes only if that fails
THUMB_WAIT_MAX = float(os.getenv("THUMB_WAIT_MAX", "20"))    # never hold the info back longer than this (seconds)


async def send_photo_card(target, thumb, text: str, kb) -> bool:
    """Send the HD thumbnail FIRST (info held back), give the phone time to load it, then add the info + buttons
    to the same message. The bot cannot see when a phone finished loading, so the wait is estimated from the image
    size (THUMB_WAIT_KBPS) and capped (THUMB_WAIT_MAX); THUMB_WAIT_KBPS=0 sends info together with the photo."""
    try:
        kb_size = len(thumb.getbuffer()) / 1024
        wait = 0.0 if THUMB_WAIT_KBPS <= 0 else min(THUMB_WAIT_MAX, max(2.0, kb_size / THUMB_WAIT_KBPS))
        if wait <= 0:
            src = getattr(thumb, "src_url", "")
            if THUMB_URL_FIRST and src:  # same as fbot: Telegram's own servers fetch the image, so the card arrives complete + full HD
                try:
                    await asyncio.wait_for(target.reply_photo(src, caption=text[:1024], parse_mode=ParseMode.HTML, reply_markup=kb), timeout=30)
                    log.info("thumbnail card sent via url")
                    return True
                except Exception as e:
                    log.info("thumbnail url send failed (%s) - uploading the image instead", e.__class__.__name__)
                    thumb.seek(0)
            await asyncio.wait_for(target.reply_photo(thumb, caption=text[:1024], parse_mode=ParseMode.HTML, reply_markup=kb), timeout=30)
            return True
        m = await asyncio.wait_for(target.reply_photo(thumb, caption=SC("🖼 <b>Loading preview…</b>"), parse_mode=ParseMode.HTML), timeout=30)
    except Exception as e:
        log.warning("thumbnail card failed: %s", e)
        return False
    log.info("thumbnail sent (%d KB), info after %.1fs", kb_size, wait)
    await asyncio.sleep(wait)
    try:
        await m.edit_caption(text[:1024], parse_mode=ParseMode.HTML, reply_markup=kb)
    except Exception as e:
        log.warning("adding info to thumbnail card failed (%s), sending it as a new card", e)
        try:
            await m.delete()
        except Exception:
            pass
        try:
            thumb.seek(0)
            await target.reply_photo(thumb, caption=text[:1024], parse_mode=ParseMode.HTML, reply_markup=kb)
        except Exception as e2:
            log.warning("thumbnail card resend failed: %s", e2)
            await target.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
    return True


CACHE_INSTANT = os.getenv("CACHE_INSTANT", "0").strip().lower() not in ("0", "false", "no", "off")  # same as ak-vip


async def try_instant_from_cache(client: Client, message, url: str, user) -> bool:
    """A Flezen link that was uploaded before -> send the cached copy right away, without calling any resolver.
    False = not cached (normal flow). Same behaviour as the ak-vip instant cache."""
    if not (FILE_CACHE and CACHE_INSTANT):
        return False
    if DAILY_LIMIT and not is_unlimited(user.id) and store.downloads_today(user.id) >= DAILY_LIMIT:
        return False  # let the normal flow show the daily-limit message
    link = url.split("?")[0].strip()
    entries = await store.find_cached_by_link(link)
    if not entries:
        return False
    cached = entries[0]
    status = await message.reply_text(SC("⚡ <b>Found in cache, sending instantly…</b>"), parse_mode=ParseMode.HTML)
    cap = build_caption(name=cached.get("name") or "file", size=cached.get("size") or 0, user=user, source_url=url,
                        dl_s=0, ul_s=0, duration=cached.get("duration", 0), height=cached.get("height", 0))
    hit = await send_cached_file(client, message.chat.id, cached, cap)
    try:
        await status.delete()
    except Exception:
        pass
    if hit is None:
        log.warning("instant cache send failed - dropping stale entry for %s", cached.get("name", "")[:60])
        await store.delete_cached_file(link, cached.get("key", ""))
        return False
    asyncio.create_task(backup_to_linked_channels(client, message.chat.id, hit.id))
    asyncio.create_task(forward_to_dump_chat(client, message.chat.id, hit.id))
    store.record_download(user.id, cached.get("size") or 0)
    await store.flush()
    await log_to_channel(client, f"⚡ <b>Cache hit</b>\n{user_tag(user)}\n📄 {html.escape(cached.get('name', '')[:100])}")
    return True


def message_link_text(message) -> str:
    """All text a link can hide in: text OR caption (forwarded posts with a photo/video carry the link in the caption),
    plus hidden hyperlinks (text_link entities) and url entities."""
    body = message.text or message.caption or ""
    parts = [body]
    for ent in (message.entities or []) + (message.caption_entities or []):
        url = getattr(ent, "url", None)  # text_link: the visible text is not the url
        if url:
            parts.append(url)
    kb = getattr(getattr(message, "reply_markup", None), "inline_keyboard", None) or []
    for row in kb:  # links that sit behind inline buttons of a forwarded post
        for btn in row:
            if getattr(btn, "url", None):
                parts.append(btn.url)
    return "\n".join(p for p in parts if p)


async def on_link(client: Client, message):
    u = message.from_user
    if store.add_user(u.id, u.first_name or ""):
        await log_to_channel(client, new_user_text(u))
    url = fz.extract_flezen_url(message_link_text(message))
    if not url:
        if message.chat.type.name == "PRIVATE":
            await message.reply_text("❌ Please send a valid Flezen link.")
        return
    if not await gate(client, message):
        return
    if await try_instant_from_cache(client, message, url, u):  # already uploaded before -> instant, no API call
        return
    status = await message.reply_text("🔍 <b>Fetching video info…</b>", parse_mode=ParseMode.HTML)
    try:
        res = await asyncio.wait_for(fz.fetch_flezen(url, http), timeout=fz.RESOLVE_TIMEOUT)
    except Exception as e:
        reason = str(e) or e.__class__.__name__
        log.warning("fetch failed for %s: %s", url, reason)
        extra = f"\n\n<code>{html.escape(reason[:350])}</code>" if is_admin(u.id) else ""
        await safe_edit(status, "❌ <b>Could not fetch this link.</b>\nIt may be invalid or removed. Try again later." + extra)
        await log_to_channel(client, f"⚠️ <b>Fetch failed</b>\n{user_tag(u)}\n<code>{html.escape(url)}</code>\n<code>{html.escape(str(e)[:300])}</code>")
        return

    try:
        f = res.files[0]
        gc_results()
        rid = uuid.uuid4().hex[:8]
        RESULTS[rid] = {"res": res, "uid": u.id, "exp": time.time() + 3600, "url": url}
        text, kb = menu_text(url, f), file_menu_kb(rid, 0, f)
        thumb = await get_menu_thumb(f) if MENU_PREVIEW else None  # preview off by default: text-only card, thumbnail only after download
        sent = bool(thumb) and await send_photo_card(message, thumb, text, kb)
        if sent:
            try:
                await status.delete()
            except Exception:
                pass
        else:
            await safe_edit(status, text, kb)
    except Exception as e:
        log.exception("on_link failed after fetch for %s", url)
        extra = f"\n\n<code>{html.escape(f'{e.__class__.__name__}: {e}'[:300])}</code>" if is_admin(u.id) else ""
        await safe_edit(status, "❌ <b>Something went wrong while preparing this video.</b>\nPlease try again." + extra)
        await log_to_channel(client, f"⚠️ <b>on_link error</b>\n{user_tag(u)}\n<code>{html.escape(url)}</code>\n<code>{html.escape(f'{e.__class__.__name__}: {e}'[:300])}</code>")


def _get_file(rid: str, idx: int, uid: int):
    ent = RESULTS.get(rid)
    if not ent or ent["exp"] < time.time():
        return None
    if ent["uid"] != uid and not is_admin(uid):
        return None
    try:
        return ent["res"].files[idx]
    except IndexError:
        return None


async def fresh_file(rid: str, idx: int, f: fz.FZFile, force: bool = False) -> fz.FZFile:
    """Flezen download links can expire: re-resolve the share link when the stored one is older than LINK_TTL.
    Falls back to the stored file if re-resolving fails."""
    if not f.source or (not force and time.time() - f.resolved < LINK_TTL):
        return f
    try:
        nf = (await asyncio.wait_for(fz.fetch_flezen(f.source, http, probe=not f.size), timeout=fz.RESOLVE_TIMEOUT)).files[0]
    except Exception as e:
        log.warning("re-resolve failed for %s: %s", f.source[:80], e)
        return f
    if not nf.size:
        nf.size = f.size
    if not nf.thumb:
        nf.thumb = f.thumb
    ent = RESULTS.get(rid)
    if ent:
        try:
            ent["res"].files[idx] = nf
        except IndexError:
            pass
    return nf


STREAM_DEADLINE = 12.0  # max seconds to wait while racing the candidate links for the stream button
# "cdn"   = Stream button opens the fastest speed-tested DIRECT CDN link (nothing goes through our server -> as fast as the CDN)
# "proxy" = everything is relayed through our own /stream proxy (needs PUBLIC_URL; slower on free hosts, but works when the CDN needs Referer/UA)
STREAM_MODE = os.getenv("STREAM_MODE", "cdn").strip().lower()
# 1 (default) = the Stream button gives Flezen API's own streamUrl DIRECTLY (nothing goes through our Render /stream proxy);
# if that link is dead / unusable the old ranking + proxy logic below is used as a fallback. 0 = skip this step.
FLEZEN_DIRECT_STREAM = os.getenv("FLEZEN_DIRECT_STREAM", "1").strip().lower() in ("1", "true", "yes", "on")


# Worker / proxy hosts (dl-worker.teraboxdl.site etc.) serve the bytes themselves and are not meant to be opened raw.
WORKER_HOSTS = tuple(h.strip().lower() for h in os.getenv("STREAM_WORKER_HOSTS", "teraboxdl.site,workers.dev").split(",") if h.strip())
STREAM_RESOLVE_CDN = os.getenv("STREAM_RESOLVE_CDN", "1").strip().lower() in ("1", "true", "yes", "on")


def is_worker_url(u: str) -> bool:
    host = (urlsplit(u or "").hostname or "").lower()
    return any(host == h or host.endswith("." + h) for h in WORKER_HOSTS)


async def to_cdn(url: str) -> str:
    """Follow redirects by hand and return the final url (same url back when nothing redirects)."""
    if not STREAM_RESOLVE_CDN or not url or is_hls_url(url):
        return url
    cur = url
    try:
        for _ in range(6):
            async with http.get(cur, headers={**DL_HEADERS, "Range": "bytes=0-0"}, allow_redirects=False,
                                timeout=aiohttp.ClientTimeout(total=12, connect=6, sock_read=6)) as r:
                loc = r.headers.get("Location")
                if r.status in (301, 302, 303, 307, 308) and loc:
                    cur = urljoin(str(r.url), loc)
                    continue
            break
    except Exception as e:
        log.debug("to_cdn: %s", e)
    return cur


async def is_attachment(url: str) -> bool:
    """True when the link would be DOWNLOADED by a browser: Content-Disposition: attachment, or a non-media type
    (e.g. application/octet-stream). Such links must go through the /stream proxy, which serves them inline."""
    if not url or is_hls_url(url):
        return False
    try:
        async with http.get(url, headers={**DL_HEADERS, "Range": "bytes=0-0"}, allow_redirects=True,
                            timeout=aiohttp.ClientTimeout(total=10, connect=6, sock_read=6)) as r:
            cd = (r.headers.get("Content-Disposition") or "").strip().lower()
            ctype = (r.headers.get("Content-Type") or "").strip().lower()
            return cd.startswith("attachment") or not ctype.startswith(("video/", "audio/"))
    except Exception as e:
        log.debug("is_attachment: %s", e)
        return False


async def stream_link(f: fz.FZFile) -> str:
    """Playable link for the Stream button. Re-resolves the share link (Flezen CDN links are signed and expire), races
    every candidate (fresh download url, fresh stream url, stored link), speed-tests them and picks the fastest one."""
    api_link = fz.fallback_link(f.source) if f.source else ""  # always "" for Flezen (no GET-stream endpoint) - kept for the shared flow
    if api_link and await probe_speed(api_link) > 0:
        return clean_url(api_link, allow_m3u8=True) or api_link
    if f.source and FLEZEN_DIRECT_STREAM:
        # Flezen API stream link first: fresh resolve, take its streamUrl, hand it out as-is (no Render relay)
        try:
            fresh = await fz.resolve_candidates(f.source, http, timeout=STREAM_DEADLINE)
        except Exception as e:
            log.info("stream: flezen direct resolve failed: %s", e)
            fresh = []
        for c in fresh:
            su = clean_url(c.stream_url, allow_m3u8=True)
            if su and await probe_speed(su) > 0:
                log.info("stream: Flezen API streamUrl used directly (%s)", (urlsplit(su).hostname or "")[:60])
                return su
        log.info("stream: Flezen API streamUrl unusable -> falling back to ranked links / proxy")
    cand = await fresh_file("", 0, f, force=True)
    extra = []
    if f.source:  # race EVERY resolver tier (Flezen API: download + stream url) like the ak-vip stream race
        try:
            extra = [c.download_url for c in await fz.resolve_candidates(f.source, http, timeout=STREAM_DEADLINE)]
        except Exception as e:
            log.info("stream: resolver race failed: %s", e)
    links = list(dict.fromkeys(x for x in ([cand.download_url, f.download_url] + extra
                                           + ([fz.fallback_link(f.source)] if f.source else []))
                               if x and x.startswith("http")))
    if not links:
        return ""

    async def probe(u: str):
        return await probe_speed(u), u

    tasks = [asyncio.create_task(probe(u)) for u in links]
    done, pending = await asyncio.wait(tasks, timeout=STREAM_DEADLINE + 6)
    for t in pending:
        t.cancel()
    ranked = []
    for t in done:
        try:
            sp, u = t.result()
        except Exception:
            continue
        log.info("stream probe: %s -> %s/s", u[:60], human_size(sp) if sp else "unusable")
        if sp > 0:
            ranked.append((sp, u))
    ranked.sort(key=lambda r: -r[0])
    if not ranked:
        log.warning("stream: no working link for %s", f.name[:60])
        return ""
    link = ranked[0][1]
    log.info("stream ranking: %s", ", ".join(f"{human_size(sp)}/s" for sp, _ in ranked))
    return await _stream_out(link, f, cand.size or f.size, follow=True)


async def _stream_out(link: str, f: fz.FZFile, size: int, follow: bool = True) -> str:
    """Final stream link: worker links / download-type links go through /stream (inline, seekable), others raw."""
    if follow and is_worker_url(link):  # worker url -> real CDN when it redirects (same as ak-vip)
        new = await to_cdn(link)
        if new != link and clean_url(new, allow_m3u8=True) and await probe_speed(new) > 0:
            log.info("stream: worker link -> real CDN %s", (urlsplit(new).hostname or "")[:60])
            link = new

    # A worker link, or a link the browser would download (attachment / non-media type), is opened through our
    # /stream proxy (it serves inline and seeks). Needs a public URL; without one the raw link is used.
    needs_proxy = is_worker_url(link) or await is_attachment(link)
    if STREAM_MODE != "proxy" and not needs_proxy and clean_url(link, allow_m3u8=True):
        return link  # direct CDN link
    if needs_proxy and not stream_proxy.public_base_url():
        log.warning("stream: worker/attachment link but no PUBLIC_URL / public IP known -> raw link (may download). Set PUBLIC_URL.")
    try:
        return stream_proxy.register_stream(link, f.name, size or f.size) or link  # raw link if no public URL
    except Exception as e:
        log.warning("stream proxy registration failed: %s", e)
        return link



async def on_stream(client: Client, cq):
    _, rid, idx = cq.data.split(":")
    f = _get_file(rid, int(idx), cq.from_user.id)
    if not f:
        await cq.answer("⌛ Expired — send the link again.", show_alert=True)
        return
    await cq.answer()
    await safe_edit(cq.message, SC("🔍 <b>Finding the stream link…</b>"))
    try:
        su = await stream_link(f)
    except Exception as e:
        log.error("❌ Stream error: %s", e)
        await safe_edit(cq.message, SC(f"⚠️ <b>Stream Error</b>\n\n<code>{html.escape(str(e)[:200])}</code>\n\nTry again in a few minutes."),
                        Markup([[mbtn("⬅️ Back", f"m:{rid}:{idx}", style=BTN_DANGER)]]))
        return
    bu = ""
    try:
        # Same as fbot: serve through our /stream relay (inline, video/* type) so Flezen's .bin attachment links play in a browser
        if su and not stream_proxy.is_ours(su):
            bu = stream_proxy.register_stream(su, f.name, f.size)
    except Exception as e:
        log.info("stream: relay link failed: %s", e)
    text, kb = stream_card(rid, int(idx), f, su, bu)
    await safe_edit(cq.message, text, kb)


async def on_menu_back(client: Client, cq):
    """Back from the stream card on a single-file link -> 'Link received' menu."""
    _, rid, idx = cq.data.split(":")
    f = _get_file(rid, int(idx), cq.from_user.id)
    if not f:
        await cq.answer("⌛ Expired — send the link again.", show_alert=True)
        return
    await cq.answer()
    await safe_edit(cq.message, menu_text(RESULTS[rid].get("url", ""), f), file_menu_kb(rid, int(idx), f))


async def on_cancel_menu(client: Client, cq):
    RESULTS.pop(cq.data.split(":", 1)[1], None)
    await cq.answer("Cancelled")
    await safe_edit(cq.message, SC("❌ <b>Cancelled</b>"))


async def on_cancel_btn(client: Client, cq):
    job = JOBS.get(cq.data.split(":", 1)[1])
    if job and (job["uid"] == cq.from_user.id or is_admin(cq.from_user.id)):
        job["cancel"] = True
        await cq.answer("🛑 Cancelling…")
    else:
        await cq.answer("Job not found.", show_alert=True)


class Cancelled(Exception):
    pass


class SlowLink(Exception):
    """Link is too slow (or unusable) AND an alternative link can be tried."""


SLOW_BYTES_S = 150 * 1024
SLOW_GRACE_S = 12


def check_slow(job: dict, t0: float, done: int):
    if job.get("slow_check"):
        el = time.time() - t0
        if el >= SLOW_GRACE_S and done / max(el, 1e-3) < SLOW_BYTES_S:
            raise SlowLink(f"link too slow ({human_size(done / el)}/s)")


async def hls_download(url: str, dest: str, job: dict, total_hint: int, on_progress) -> int:
    """HLS (.m3u8) -> mp4 with ffmpeg stream copy. Progress = bytes written so far."""
    if not shutil.which("ffmpeg"):
        raise RuntimeError("this link is an HLS stream and ffmpeg is not installed on the server")
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-nostdin", "-loglevel", "error", "-y",
        *(["-user_agent", "Mozilla/5.0"] if url.lower().startswith("http") else []), "-i", url,
        "-c", "copy", "-bsf:a", "aac_adtstoasc", "-movflags", "+faststart", dest,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
    last_size, last_growth, last_edit = 0, time.time(), 0.0
    try:
        while proc.returncode is None:
            try:
                await asyncio.wait_for(proc.wait(), timeout=2)
            except asyncio.TimeoutError:
                pass
            if job["cancel"]:
                raise Cancelled()
            size = os.path.getsize(dest) if os.path.exists(dest) else 0
            if size > MAX_DL_BYTES:
                raise RuntimeError("TOO_BIG")
            now = time.time()
            if size > last_size:
                last_size, last_growth = size, now
            elif now - last_growth > 120:
                raise RuntimeError("stream stalled (no data for 120s)")
            if now - last_edit >= EDIT_INTERVAL:
                last_edit = now
                await on_progress(size, total_hint)
        if proc.returncode != 0:
            err = (await proc.stderr.read()).decode(errors="ignore").strip().splitlines()
            raise RuntimeError("ffmpeg failed: " + (err[-1][:150] if err else f"exit {proc.returncode}"))
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
    return os.path.getsize(dest)


DL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Referer": "https://flezen.com/",
}
DL_TIMEOUT = aiohttp.ClientTimeout(total=None, connect=30, sock_read=90)  # sock_read = stall detector


class RangeIgnored(Exception):
    """The server answered a Range request with a plain 200 (no partial content)."""


def _task_exc(t):
    return None if t.cancelled() else t.exception()
PARALLEL_CONNECTIONS = max(1, int(os.getenv("PARALLEL_CONNECTIONS", "8")))
PARALLEL_MIN_BYTES = 20 * 1024 * 1024  # smaller files are not worth splitting
CHUNK = 256 * 1024


async def _probe_ranges(url: str):
    """Returns total size if the server honours Range requests, else 0."""
    try:
        async with http.get(url, headers={**DL_HEADERS, "Range": "bytes=0-0"}, timeout=DL_TIMEOUT, allow_redirects=True) as r:
            if r.status != 206 or "text/html" in (r.headers.get("Content-Type") or "").lower():
                return 0
            m = re.fullmatch(r"bytes 0-0/(\d+)", (r.headers.get("Content-Range") or "").strip())
            return int(m.group(1)) if m else 0
    except Exception:
        return 0


def _rm(path: str):
    try:
        os.remove(path)
    except OSError:
        pass


async def parallel_download(url: str, dest: str, job: dict, total: int, on_progress) -> int:
    """Split `total` bytes over PARALLEL_CONNECTIONS ranged GETs written into one preallocated file.
    A broken range worker is retried (resuming where it stopped); any other failure bubbles up so the caller can fall back.
    Progress of every range is saved next to the file (dest + '.rs') so a download that was cut off by a restart continues from there."""
    side = dest + ".rs"
    ranges = pos = None
    if os.path.exists(side) and os.path.exists(dest) and os.path.getsize(dest) == total:
        try:
            with open(side, encoding="utf-8") as sf:
                d = json.load(sf)
            rs = [(int(a), int(b)) for a, b in d["ranges"]]
            ps = [int(x) for x in d["pos"]]
            if (d["total"] == total and rs and len(rs) == len(ps) and rs[-1][1] == total - 1
                    and all(a <= p <= b + 1 for (a, b), p in zip(rs, ps))):
                ranges, pos = rs, ps
                log.info("resuming parallel download %s from %s / %s", os.path.basename(dest)[:50],
                         human_size(sum(p - a for (a, _), p in zip(rs, ps))), human_size(total))
        except Exception as e:
            log.warning("resume info unreadable (%s) - starting over", e)
    if ranges is None:
        n = min(PARALLEL_CONNECTIONS, max(1, total // (4 * 1024 * 1024)))
        part = -(-total // n)
        ranges = [(i * part, min((i + 1) * part, total) - 1) for i in range(n) if i * part < total]
        pos = [a for a, _ in ranges]
        with open(side, "w", encoding="utf-8") as sf:  # marker first: a half-written (sparse) file is never taken for a finished one
            json.dump({"total": total, "ranges": ranges, "pos": pos}, sf)
        with open(dest, "wb") as fh:
            fh.truncate(total)
    base = sum(p - a for (a, _), p in zip(ranges, pos))
    state = {"done": base}
    job["resumed"] = base

    def save_rs(fh):
        """Flush written data first, then record how far each range got (no await in between -> consistent)."""
        try:
            fh.flush()
            tmp = side + ".tmp"
            with open(tmp, "w", encoding="utf-8") as sf:
                json.dump({"total": total, "ranges": ranges, "pos": list(pos)}, sf)
            os.replace(tmp, side)
        except OSError as e:
            log.debug("could not save resume info: %s", e)

    async def worker(fh, i: int, end: int):
        fails = 0
        while pos[i] <= end:
            before = pos[i]
            try:
                async with http.get(url, headers={**DL_HEADERS, "Range": f"bytes={pos[i]}-{end}"}, timeout=DL_TIMEOUT) as r:
                    if r.status == 200:
                        raise RangeIgnored()
                    if r.status != 206:
                        raise RuntimeError(f"range request answered HTTP {r.status}")
                    async for chunk in r.content.iter_chunked(CHUNK):
                        if job["cancel"]:
                            raise Cancelled()
                        chunk = chunk[: end - pos[i] + 1]
                        fh.seek(pos[i])  # no await between seek and write -> safe across coroutines
                        fh.write(chunk)
                        pos[i] += len(chunk)
                        state["done"] += len(chunk)
                        if pos[i] > end:
                            break
                if pos[i] == before:  # server answered but sent nothing -> count it, don't spin forever
                    raise RuntimeError("range request returned no data")
            except (Cancelled, RangeIgnored):
                raise
            except Exception:
                fails = 1 if pos[i] > before else fails + 1  # it moved before breaking -> fresh retry budget
                if fails > 3:
                    raise
                await asyncio.sleep(1.5 * fails)

    with open(dest, "r+b") as fh:
        tasks = [asyncio.create_task(worker(fh, i, b)) for i, (_, b) in enumerate(ranges)]
        try:
            last, last_save, t0 = 0.0, time.time(), time.time()
            while not all(t.done() for t in tasks):
                await asyncio.wait(tasks, timeout=1, return_when=asyncio.FIRST_EXCEPTION)
                for t in tasks:
                    if t.done() and _task_exc(t):
                        raise _task_exc(t)
                check_slow(job, t0, state["done"] - base)
                now = time.time()
                if now - last_save >= 2:
                    last_save = now
                    save_rs(fh)
                if now - last >= EDIT_INTERVAL:
                    last = now
                    await on_progress(state["done"], total)
            for t in tasks:
                if _task_exc(t):
                    raise _task_exc(t)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            save_rs(fh)  # also on cancel / shutdown: the next start continues from exactly here
    if state["done"] != total:
        raise RuntimeError(f"incomplete download ({state['done']}/{total} bytes)")
    _rm(side)
    return total


async def sequential_download(url: str, dest: str, job: dict, total_hint: int, on_progress) -> int:
    side = dest + ".rs"
    start = 0
    if os.path.exists(side):  # leftover of a parallel attempt: that file has holes, it cannot be continued with one connection
        _rm(dest)
        _rm(side)
    elif os.path.exists(dest):
        cur = os.path.getsize(dest)
        if total_hint and cur == total_hint:  # already complete (e.g. the bot stopped while uploading)
            job["resumed"] = cur
            return cur
        if cur > 0 and (not total_hint or cur < total_hint):
            start = cur  # continue where the last run stopped
        else:
            _rm(dest)
    hdr = {**DL_HEADERS, **({"Range": f"bytes={start}-"} if start else {})}
    async with http.get(url, headers=hdr, timeout=DL_TIMEOUT, allow_redirects=True) as r:
        if start and r.status == 416:  # nothing left to fetch -> the file on disk is complete
            job["resumed"] = start
            return start
        if r.status >= 400:
            raise RuntimeError(f"download server returned HTTP {r.status}")
        if "text/html" in (r.headers.get("Content-Type") or "").lower():
            raise RuntimeError("download link expired or blocked (got a web page)")
        if start and not (r.status == 206 and (r.headers.get("Content-Range") or "").startswith(f"bytes {start}-")):
            start = 0  # server ignored the Range header -> start from byte 0
        if start:
            m = re.search(r"/(\d+)\s*$", r.headers.get("Content-Range") or "")
            total = int(m.group(1)) if m else (start + int(r.headers.get("Content-Length") or 0)) or total_hint
        else:
            total = int(r.headers.get("Content-Length") or 0) or total_hint
        if total and total > MAX_DL_BYTES:
            raise RuntimeError("TOO_BIG")
        job["resumed"] = start
        clen = int(r.headers.get("Content-Length") or 0) if (r.headers.get("Content-Length") or "").isdigit() else 0
        done, last, t0 = start, 0.0, time.time()
        with open(dest, "ab" if start else "wb") as fh:
            async for chunk in r.content.iter_chunked(CHUNK):
                if job["cancel"]:
                    raise Cancelled()
                fh.write(chunk)
                done += len(chunk)
                check_slow(job, t0, done - start)
                if done > MAX_DL_BYTES:
                    raise RuntimeError("TOO_BIG")
                now = time.time()
                if now - last >= EDIT_INTERVAL:
                    last = now
                    await on_progress(done, total)
    if clen and done != start + clen:  # never hand a truncated (unplayable) file on
        raise RuntimeError(f"incomplete download ({done - start}/{clen} bytes)")
    return done


async def resumable_download(url: str, dest: str, job: dict, total: int, on_progress) -> int:
    """One connection, but it RESUMES (Range: bytes=N-) after a dropped / stalled connection and verifies the exact
    final size. Ported from the reference repo's downloader._resumable - the worker's plain GET can die half way."""
    done, stall, last, t0 = 0, 0, 0.0, time.time()
    _rm(dest + ".rs")
    open(dest, "wb").close()
    while done < total:
        prev = done
        headers = {**DL_HEADERS, "Range": f"bytes={done}-"} if done > 0 else dict(DL_HEADERS)
        try:
            async with http.get(url, headers=headers, timeout=DL_TIMEOUT, allow_redirects=True) as r:
                if r.status >= 400:
                    raise RuntimeError(f"download server returned HTTP {r.status}")
                if "text/html" in (r.headers.get("Content-Type") or "").lower():
                    raise RuntimeError("download link expired or blocked (got a web page)")
                if done > 0 and r.status == 200:  # server ignored Range -> start over
                    done = 0
                    open(dest, "wb").close()
                with open(dest, "ab" if done > 0 else "wb") as fh:
                    async for chunk in r.content.iter_chunked(CHUNK):
                        if job["cancel"]:
                            raise Cancelled()
                        chunk = chunk[: total - done]
                        fh.write(chunk)
                        done += len(chunk)
                        check_slow(job, t0, done)
                        now = time.time()
                        if now - last >= EDIT_INTERVAL:
                            last = now
                            await on_progress(done, total)
                        if done >= total:
                            break
        except (Cancelled, SlowLink, asyncio.CancelledError, RuntimeError):
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
            log.info("resumable download interrupted at %d/%d (%s)", done, total, e.__class__.__name__)
        if done >= total:
            break
        if done == prev:
            stall += 1
            if stall >= 6:
                raise RuntimeError(f"download stalled at {done}/{total} bytes")
            await asyncio.sleep(min(2 ** stall, 15))
        else:
            stall = 0
    if done != total:
        raise RuntimeError(f"size mismatch: {done}/{total} bytes")
    return done


async def download_file(url: str, dest: str, job: dict, total_hint: int, on_progress):
    if ".m3u8" in url.lower():
        return await hls_download(url, dest, job, total_hint, on_progress)
    if total_hint and os.path.exists(dest) and not os.path.exists(dest + ".rs") and os.path.getsize(dest) == total_hint:
        job["resumed"] = total_hint  # fully downloaded earlier (the bot stopped while uploading) -> straight to upload
        return total_hint
    total = await _probe_ranges(url)  # > 0 only when the server honours Range and tells the true size
    if total:
        if total > MAX_DL_BYTES:
            raise RuntimeError("TOO_BIG")
        if PARALLEL_CONNECTIONS > 1 and total >= PARALLEL_MIN_BYTES:
            try:
                return await parallel_download(url, dest, job, total, on_progress)
            except (Cancelled, SlowLink, asyncio.CancelledError):
                raise
            except Exception as e:  # server misbehaved mid-way: start over with the resumable single connection
                log.warning("parallel download failed (%s) - falling back to single resumable connection", e)
        return await resumable_download(url, dest, job, total, on_progress)
    return await sequential_download(url, dest, job, total_hint, on_progress)


async def precheck(client: Client, cq, need: int = 1) -> bool:
    """Common ban / force-sub / limit / per-user checks for download buttons. Answers the callback on failure."""
    uid = cq.from_user.id
    if store.is_banned(uid) and not is_admin(uid):
        await cq.answer("🚫 Banned.", show_alert=True)
        return False
    if FORCE_SUB and not is_admin(uid) and await missing_channels(client, uid):
        await cq.answer("🔒 Join the channel(s) first, then send /start.", show_alert=True)
        return False
    # downloads still running count too, otherwise parallel starts could slip past the daily limit
    running = sum(1 for j in JOBS.values() if j["uid"] == uid)
    if DAILY_LIMIT and not is_unlimited(uid) and store.downloads_today(uid) + running + need > DAILY_LIMIT:
        left = max(0, DAILY_LIMIT - store.downloads_today(uid) - running)
        await cq.answer(f"⛔ Daily limit {DAILY_LIMIT}/day — you have {left} left today.", show_alert=True)
        try:
            await send_referral_prompt(client, uid, limit_hit=True)
        except Exception as e:
            log.warning("referral prompt failed: %s", e)
        return False
    plim = parallel_limit(uid)
    if plim and running >= plim:
        await cq.answer(f"⏳ Your plan allows {plim} download{'s' if plim != 1 else ''} at a time — wait for one to finish or use /cancel. "
                        "Upgrade with /plans for more.", show_alert=True)
        return False
    return True


# ---------------------------------------------------------- file helpers ----
_MAGIC = [(b"\x1aE\xdf\xa3", ".mkv"), (b"PK\x03\x04", ".zip"), (b"%PDF-", ".pdf"), (b"\xff\xd8\xff", ".jpg"),
          (b"\x89PNG\r\n\x1a\n", ".png"), (b"Rar!\x1a\x07", ".rar"), (b"7z\xbc\xaf\x27\x1c", ".7z"), (b"GIF8", ".gif"),
          (b"ID3", ".mp3")]
_TEXTY = (".html", ".htm", ".txt", ".json", ".xml", ".csv", ".srt", ".vtt", ".md")


def read_head(path: str, n: int = 64) -> bytes:
    try:
        with open(path, "rb") as fh:
            return fh.read(n)
    except OSError:
        return b""


def sniff_ext(head: bytes) -> str:
    if head[4:8] == b"ftyp":
        return ".mp4"
    for sig, ext in _MAGIC:
        if head.startswith(sig):
            return ext
    return ""


def validate_download(path: str, name: str, size: int, is_video: bool):
    """Raise if the 'file' is really an error page or absurdly small for a video."""
    head = read_head(path).lstrip().lower()
    if not name.lower().endswith(_TEXTY) and (head.startswith((b"<!doctype", b"<html", b"<?xml")) or head.startswith(b'{"')):
        raise RuntimeError("download link returned a web page / error instead of the file")
    if is_video and size < 50 * 1024:
        raise RuntimeError(f"downloaded video is only {human_size(size)} — link is probably broken")


async def _run(*cmd, timeout: int = 60):
    try:
        p = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        out, _ = await asyncio.wait_for(p.communicate(), timeout=timeout)
        return p.returncode, out
    except Exception:
        return 1, b""


def mp4_probe(path: str):
    """Pure-python fallback (no ffprobe): (duration_s, width, height) from the mp4 'moov' box, zeros if not an mp4."""
    import struct
    try:
        with open(path, "rb") as fh:
            fh.seek(0, 2)
            end = fh.tell()
            pos = 0
            while pos + 8 <= end:
                fh.seek(pos)
                hdr = fh.read(16)
                size, typ = struct.unpack(">I4s", hdr[:8])
                hlen = 8
                if size == 1:
                    size, hlen = struct.unpack(">Q", hdr[8:16])[0], 16
                elif size == 0:
                    size = end - pos
                if size < hlen:
                    break
                if typ == b"moov" and size < 64 * 1024 * 1024:
                    fh.seek(pos + hlen)
                    moov = fh.read(size - hlen)
                    dur = w = h = 0
                    i = moov.find(b"mvhd")
                    if i >= 4:
                        ver = moov[i + 4]
                        if ver == 1:
                            ts, d = struct.unpack(">IQ", moov[i + 24:i + 36])
                        else:
                            ts, d = struct.unpack(">II", moov[i + 16:i + 24])
                        dur = int(d / ts) if ts else 0
                    j = 0
                    while True:
                        j = moov.find(b"tkhd", j)
                        if j < 0:
                            break
                        ver = moov[j + 4]
                        off = j + (92 if ver == 1 else 80)  # width/height (16.16 fixed) are the last 8 bytes of tkhd
                        if off + 8 <= len(moov):
                            tw, th = struct.unpack(">II", moov[off:off + 8])
                            if (tw >> 16) and (th >> 16):
                                w, h = tw >> 16, th >> 16
                                break
                        j += 4
                    return dur, w, h
                pos += size
    except Exception as e:
        log.info("mp4 header probe failed: %s", e)
    return 0, 0, 0


async def probe_video(path: str):
    """(duration_s, width, height) — ffprobe first, pure-python mp4 reader if ffprobe is missing/fails, zeros otherwise."""
    res = (0, 0, 0)
    if shutil.which("ffprobe"):
        rc, out = await _run("ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
                             "stream=width,height:format=duration", "-of", "json", path)
        try:
            d = json.loads(out)
            st = d["streams"][0]
            res = (int(float(d["format"]["duration"])), int(st.get("width") or 0), int(st.get("height") or 0))
        except Exception:
            res = (0, 0, 0)
    else:
        log.warning("ffprobe not found - install ffmpeg or videos can show up as plain files")
    if not all(res):
        alt = await asyncio.to_thread(mp4_probe, path)
        res = tuple(a or b for a, b in zip(res, alt))
    log.info("video probe: duration=%s width=%s height=%s", *res)
    return res


async def make_thumb(path: str, out: str, duration: int) -> bool:
    """Cut a sharp frame from the video: longest side 320 px (Telegram's max), JPEG kept under 200 KB."""
    if not shutil.which("ffmpeg"):
        return False
    at = max(1, int(duration * 0.1)) if duration else 1
    vf = "scale='if(gt(iw,ih),320,-2)':'if(gt(iw,ih),-2,320)':flags=lanczos"
    for q in ("2", "5", "10"):  # best quality first, then smaller until it fits Telegram's 200 KB limit
        rc, _ = await _run("ffmpeg", "-nostdin", "-loglevel", "error", "-y", "-ss", str(at), "-i", path, "-frames:v", "1",
                           "-vf", vf, "-q:v", q, out, timeout=60)
        if rc == 0 and os.path.exists(out) and 0 < os.path.getsize(out) <= 200 * 1024:
            return True
    return rc == 0 and os.path.exists(out) and 0 < os.path.getsize(out) <= 200 * 1024


async def _ffmpeg_segment(src: str, pattern: str, secs: int) -> list:
    p = await asyncio.create_subprocess_exec(
        "ffmpeg", "-nostdin", "-loglevel", "error", "-y", "-i", src, "-c", "copy", "-map", "0",
        "-f", "segment", "-segment_time", str(secs), "-reset_timestamps", "1", pattern,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    await p.wait()
    return sorted(glob.glob(pattern.replace("%03d", "*")))


async def split_video(src: str, max_part: int) -> list:
    """Stream-copy split (no re-encode) into playable mp4 parts under max_part. [] if it cannot be done."""
    if not (shutil.which("ffmpeg") and shutil.which("ffprobe")):
        return []
    size = os.path.getsize(src)
    dur, _, _ = await probe_video(src)
    if dur <= 0:
        return []
    secs = max(30, int(max_part * 0.85 / (size / dur)))
    stem = os.path.splitext(src)[0]
    parts = await _ffmpeg_segment(src, f"{stem}_part%03d.mp4", secs)
    if not parts:
        return []
    fixed = []
    for part in parts:  # one corrective pass for parts that landed over budget (high-bitrate stretches)
        psize = os.path.getsize(part)
        if psize <= max_part:
            fixed.append(part)
            continue
        pdur, _, _ = await probe_video(part)
        ok = False
        for factor in (0.85, 0.6, 0.4, 0.25):  # cuts land on keyframes, so shrink the target until every piece fits
            if not pdur:
                break
            subs = await _ffmpeg_segment(part, part[:-4] + "_s%03d.mp4", max(3, int(pdur * (max_part / psize) * factor)))
            if subs and all(os.path.getsize(x) <= max_part for x in subs):
                os.remove(part)
                fixed.extend(subs)
                ok = True
                break
            for x in subs:
                os.remove(x)
        if not ok:
            return []  # could not get under the limit -> caller falls back to raw split
    return fixed


def split_bytes(src: str, max_part: int) -> list:
    """Raw split into name.ext.001, .002 … (join with `cat`, 7-Zip or HJSplit)."""
    parts, i = [], 1
    with open(src, "rb") as fh:
        while True:
            out = f"{src}.{i:03d}"
            written = 0
            with open(out, "wb") as o:
                while written < max_part:
                    chunk = fh.read(min(8 * 1024 * 1024, max_part - written))
                    if not chunk:
                        break
                    o.write(chunk)
                    written += len(chunk)
            if written == 0:
                os.remove(out)
                break
            parts.append(out)
            i += 1
    return parts


# ------------------------------------------------------- link probing ----
PROBE_BYTES = 2 * 1024 * 1024   # speed test: first 2 MB (or 4 s) of each candidate link
PROBE_SECS = 4.0


def is_hls_url(u: str) -> bool:
    return "m3u8" in (u or "").lower()  # also catches API urls like .../get_m3u8_stream_fast?token=...


async def hls_ok(url: str) -> bool:
    """True only if the HLS playlist loads AND its first segment is real media (not an error page/JSON).
    Big files often get a dead playlist from the API -> the player showed a broken 0:00 screen."""
    try:
        t = aiohttp.ClientTimeout(total=12, connect=6)
        async with http.get(url, headers=DL_HEADERS, timeout=t, allow_redirects=True) as r:
            if r.status >= 400:
                return False
            body = (await r.content.read(256 * 1024)).decode("utf-8", "ignore")
            base = str(r.url)
        if not body.lstrip().startswith("#EXTM3U"):
            return False
        for _ in range(2):  # master playlist -> first variant playlist
            lines = [ln.strip() for ln in body.splitlines() if ln.strip()]
            if "#EXT-X-STREAM-INF" not in body:
                break
            nxt = next((ln for ln in lines if not ln.startswith("#")), "")
            if not nxt:
                return False
            base = urljoin(base, nxt)
            async with http.get(base, headers=DL_HEADERS, timeout=t, allow_redirects=True) as r2:
                if r2.status >= 400:
                    return False
                body = (await r2.content.read(256 * 1024)).decode("utf-8", "ignore")
                base = str(r2.url)
            if not body.lstrip().startswith("#EXTM3U"):
                return False
        seg = next((ln.strip() for ln in body.splitlines() if ln.strip() and not ln.startswith("#")), "")
        if not seg:
            return False
        async with http.get(urljoin(base, seg), headers={**DL_HEADERS, "Range": "bytes=0-4095"}, timeout=t, allow_redirects=True) as r3:
            ctype = (r3.headers.get("Content-Type") or "").lower()
            log.info("hls segment: status=%s type=%s", r3.status, ctype)
            if r3.status >= 400 or "text/html" in ctype or "json" in ctype:
                return False
            return len(await r3.content.read(4096)) >= 512
    except Exception as e:
        log.info("hls check failed (%s): %s", url[:70], e.__class__.__name__)
        return False


async def probe_speed(url: str) -> float:
    """Bytes/s measured on the first PROBE_BYTES of `url` (0 = unusable). HLS playlists get a token score so they rank last."""
    if is_hls_url(url):
        return 1.0 if await hls_ok(url) else 0.0  # a dead HLS playlist is unusable, not "slow but fine"
    t0, got = time.time(), 0
    try:
        async with http.get(url, headers={**DL_HEADERS, "Range": f"bytes=0-{PROBE_BYTES - 1}"}, allow_redirects=True,
                            timeout=aiohttp.ClientTimeout(total=PROBE_SECS + 8, connect=8, sock_read=5)) as r:
            ctype = (r.headers.get("Content-Type") or "").lower()
            if r.status >= 400 or "text" in ctype or "json" in ctype:
                return 0.0
            async for chunk in r.content.iter_chunked(65536):
                got += len(chunk)
                if got >= PROBE_BYTES or time.time() - t0 >= PROBE_SECS:
                    break
    except Exception:
        return 0.0
    return got / max(time.time() - t0, 0.05) if got >= 32 * 1024 else 0.0


async def alt_link(f: fz.FZFile):
    """A freshly resolved link for the same Flezen video (used once when a download fails or is too slow)."""
    if not f.source:
        return None
    try:
        nf = (await asyncio.wait_for(fz.fetch_flezen(f.source, http, probe=False), timeout=fz.RESOLVE_TIMEOUT)).files[0]
    except Exception as e:
        log.warning("alternate link failed: %s", e)
        return None
    if not nf.size:
        nf.size = f.size
    if not nf.thumb:
        nf.thumb = f.thumb
    return nf if nf.download_url else None


def build_caption(name: str, size: int, user, source_url: str = "", dl_s: float = 0, ul_s: float = None,
                  duration: int = 0, height: int = 0, part: str = "") -> str:
    """fbot-style upload caption (blockquote with file info, timings, downloaded-by, source, powered-by)."""
    def hms(sec):
        sec = max(0, int(round(sec)))
        h, r = divmod(sec, 3600)
        m, s_ = divmod(r, 60)
        return f"{h}:{m:02d}:{s_:02d}"

    custom = store.get_caption(user.id)
    if custom:  # fbot-style /set_caption template with placeholders
        vals = {
            "filename": html.escape(name), "size": human_size(size), "quality": f"{height}p" if height else "Auto (Best)",
            "source": html.escape(source_url or "Unknown"), "duration": hms(duration) if duration else "Unknown",
            "part": part or "1/1", "user": html.escape(user.first_name or "User"),
            "downloaded_in": f"{hms(dl_s)} sec", "uploaded_in": f"{hms(ul_s)} sec" if ul_s is not None else "",
        }
        for k, v in vals.items():
            custom = custom.replace("{" + k + "}", v)
        return custom
    by = f'<a href="tg://user?id={user.id}">{html.escape(smallcaps(user.first_name or "User"))}</a>'
    src_txt = html.escape(smallcaps("Flezen Link"))
    src = f'<a href="{html.escape(source_url, quote=True)}">{src_txt}</a>' if source_url.startswith("http") else src_txt
    powered = f'<a href="{html.escape(POWERED_BY_URL, quote=True)}">{html.escape(smallcaps(POWERED_BY))}</a>'
    lines = [f"📄 {smallcaps('File Name')}: {html.escape(smallcaps(name[:120]))}"]
    lines.append(f"📦 {smallcaps('Size')}: {human_size(size)}")
    if part:
        lines.append(f"🧩 {smallcaps('Part')}: {part}")
    if height:
        lines.append(f"🎞️ {smallcaps('Quality')}: {height}p")
    if duration:
        lines.append(f"⏱️ {smallcaps('Duration')}: {hms(duration)}")
    lines.append(f"⬇️ {smallcaps('Downloaded in')}: {hms(dl_s)} sec")
    if ul_s is not None:
        lines.append(f"⬆️ {smallcaps('Uploaded in')}: {hms(ul_s)} sec")
    lines.append(f"🙋 {smallcaps('Downloaded by')}: {by}")
    lines.append(f"🔗 {smallcaps('Source')}: {src}")
    return f"<blockquote>{chr(10).join(lines)}</blockquote>\n\n⚡ {smallcaps('Powered by')} {powered}"


async def finalize_custom_thumb(src: str, dst: str):
    """Telegram wants thumbnails <= 320px and < 200 KB JPEG; returns the usable path (or the original if ffmpeg is missing)."""
    if not shutil.which("ffmpeg"):
        return src
    vf = "scale='if(gt(iw,ih),min(iw,320),-2)':'if(gt(iw,ih),-2,min(ih,320))'"
    for q in ("3", "6", "10", "16"):
        rc, _ = await _run("ffmpeg", "-nostdin", "-loglevel", "error", "-y", "-i", src, "-vf", vf, "-frames:v", "1", "-q:v", q, dst, timeout=20)
        if rc == 0 and os.path.exists(dst) and os.path.getsize(dst) < 200 * 1024:
            return dst
    return dst if (os.path.exists(dst) and os.path.getsize(dst) > 0) else src


async def forward_to_dump_chat(client: Client, chat_id: int, message_id: int):
    """Best-effort copy of a delivered file into the user's own dump chat (/setchat), if they set one."""
    dump = store.get_dump_chat(chat_id)
    if not dump:
        return
    try:
        await client.copy_message(chat_id=dump, from_chat_id=chat_id, message_id=message_id)
    except FloodWait as e:
        await asyncio.sleep(min(e.value, 30))
        try:
            await client.copy_message(chat_id=dump, from_chat_id=chat_id, message_id=message_id)
        except Exception as e2:
            log.warning("dump-chat forward to %s failed: %s", dump, e2)
    except Exception as e:
        log.warning("dump-chat forward to %s failed: %s", dump, e)


# ------------------------------------------------ file cache (from fbot) ----
def cache_key_for(f: fz.FZFile, source_url: str = ""):
    """(link, key) identifying this exact file inside a share, or None if it can't be cached safely."""
    link = (source_url or f.source or "").split("?")[0].strip()
    if not FILE_CACHE or not link or not f.size:
        return None
    return link, f"{f.path or f.name}::{f.size}"


async def send_cached_file(client: Client, chat_id: int, cached: dict, caption: str):
    """Send a cached file. Prefers copy_message() from the cache channel (fresh file_reference for any user),
    falls back to the stored file_id. Returns the sent message, or None if both fail."""
    sent = None
    if CACHE_CHANNEL_ID and cached.get("cache_chat_id") and cached.get("cache_message_id"):
        try:
            sent = await client.copy_message(chat_id=chat_id, from_chat_id=cached["cache_chat_id"],
                                             message_id=cached["cache_message_id"], caption=caption, parse_mode=ParseMode.HTML)
        except Exception as e:
            log.warning("cache-channel copy failed, falling back to file_id: %s", e)
    if sent is None and cached.get("file_id"):
        try:
            if cached.get("kind") == "video":
                sent = await client.send_video(chat_id, cached["file_id"], caption=caption, parse_mode=ParseMode.HTML,
                                               supports_streaming=True, duration=cached.get("duration", 0),
                                               width=cached.get("width", 0), height=cached.get("height", 0))
            else:
                sent = await client.send_document(chat_id, cached["file_id"], caption=caption, parse_mode=ParseMode.HTML)
        except Exception as e:
            log.warning("cached file_id send failed: %s", e)
    return sent


async def store_in_cache(client: Client, ck, sent, name: str, size: int, duration: int, width: int, height: int):
    """After a fresh single-part upload: copy it into the cache channel and remember it. Never raises."""
    try:
        media = sent.video or sent.document
        if not media:
            return
        cache_chat_id = cache_message_id = None
        if CACHE_CHANNEL_ID:
            try:
                cp = await client.copy_message(chat_id=CACHE_CHANNEL_ID, from_chat_id=sent.chat.id, message_id=sent.id)
                cache_chat_id, cache_message_id = CACHE_CHANNEL_ID, cp.id
            except Exception as e:
                log.warning("cache-channel copy after upload failed (is the bot admin there?): %s", e)
        await store.set_cached_file(ck[0], ck[1], file_id=media.file_id, kind="video" if sent.video else "document",
                                    name=name, size=size, duration=duration, width=width, height=height,
                                    cache_chat_id=cache_chat_id, cache_message_id=cache_message_id)
    except Exception as e:
        log.warning("store_in_cache failed: %s", e)


# ------------------------------------------- custom caption / thumbnail / dump chat / settings (from fbot) ----
CAPTION_PLACEHOLDERS = ("{filename}", "{size}", "{quality}", "{source}", "{duration}", "{part}", "{user}", "{downloaded_in}", "{uploaded_in}")


async def on_set_caption(client: Client, message):
    if not await gate(client, message):
        return
    parts = (message.text or "").split(None, 1)
    if len(parts) < 2 or not parts[1].strip():
        return await message.reply_text(
            SC("⚠️ <b>Usage Error</b>\n\nPlease provide the caption text after the command.\n\n"
               "<b>Correct Format:</b>\n<code>/set_caption Your Caption Here</code>\n\n<b>Supported Placeholders:</b>\n"
               "• <code>{filename}</code> : File name\n• <code>{size}</code> : File size\n• <code>{quality}</code> : Quality label\n"
               "• <code>{source}</code> : Source Flezen link\n• <code>{duration}</code> : Video duration\n"
               "• <code>{part}</code> : Part number (split files)\n• <code>{user}</code> : Your name\n"
               "• <code>{downloaded_in}</code> / <code>{uploaded_in}</code> : Timings\n\n"
               "<i>HTML like &lt;b&gt;bold&lt;/b&gt; and &lt;i&gt;italic&lt;/i&gt; works.</i>\n"
               "<i>Example:</i> <code>/set_caption File: {filename} | Size: {size}</code>"),
            parse_mode=ParseMode.HTML)
    # keep the user's own formatting (html) as typed
    caption = message.text.html.split(None, 1)[1].strip() if hasattr(message.text, "html") else parts[1].strip()
    if len(caption) > 800:
        return await message.reply_text(SC("❌ <b>Caption too long</b>\n<i>Keep it under 800 characters.</i>"), parse_mode=ParseMode.HTML)
    sample = caption
    for k, v in {"filename": "Sample Video.mp4", "size": "350.00 MB", "quality": "720p", "source": "https://flezen.com/s/xxxx", "duration": "0:12:34",
                 "part": "1/1", "user": message.from_user.first_name or "User", "downloaded_in": "0:00:20 sec", "uploaded_in": "0:00:15 sec"}.items():
        sample = sample.replace("{" + k + "}", html.escape(v))
    try:  # also proves the HTML is valid before it is ever used on a real upload
        await message.reply_text(sample, parse_mode=ParseMode.HTML)
    except Exception as e:
        return await message.reply_text(SC(f"❌ <b>Invalid caption / HTML</b>\n<code>{html.escape(str(e)[:150])}</code>"), parse_mode=ParseMode.HTML)
    store.set_caption(message.from_user.id, caption)
    await message.reply_text(SC("✅ <b>Custom Caption Saved!</b>\n\n<i>The message above is your preview.\nIt will be applied to your future downloads.</i>"),
                             parse_mode=ParseMode.HTML)


async def on_see_caption(client: Client, message):
    if not await gate(client, message):
        return
    cap = store.get_caption(message.from_user.id)
    if cap:
        await message.reply_text(SC("📝 <b>Your Custom Caption</b>\n\n") + f"<code>{html.escape(cap)}</code>\n\n" + SC("<i>To remove this, use /del_caption</i>"),
                                 parse_mode=ParseMode.HTML)
    else:
        await message.reply_text(SC("❌ <b>No Caption Set</b>\n\nYou are currently using the default bot caption.\n<i>Use /set_caption to customize it.</i>"),
                                 parse_mode=ParseMode.HTML)


async def on_del_caption(client: Client, message):
    if not await gate(client, message):
        return
    if store.del_caption(message.from_user.id):
        await message.reply_text(SC("🗑 <b>Custom Caption Removed</b>\n\n<i>Your uploads will now use the default bot caption.</i>"), parse_mode=ParseMode.HTML)
    else:
        await message.reply_text(SC("⚠️ <b>No Caption Found</b>\n\nYou don't have a custom caption set."), parse_mode=ParseMode.HTML)


async def on_set_thumb(client: Client, message):
    if not await gate(client, message):
        return
    rep = message.reply_to_message
    if not rep or not rep.photo:
        return await message.reply_text(
            SC("🖼 <b>Set Custom Thumbnail</b>\n\n<i>Reply to any photo with /set_thumb to use it as your default thumbnail.</i>\n\n"
               "<b>Usage:</b> Reply to a photo → <code>/set_thumb</code>"), parse_mode=ParseMode.HTML)
    store.set_thumb(message.from_user.id, rep.photo.file_id)
    await message.reply_photo(rep.photo.file_id, caption=SC("✅ <b>Custom Thumbnail Set Successfully!</b>\n\n<i>This thumbnail will be used for all your future uploads.</i>\n"
                                                           "<i>Use /view_thumb to preview • /del_thumb to remove</i>"), parse_mode=ParseMode.HTML)


async def on_view_thumb(client: Client, message):
    if not await gate(client, message):
        return
    tid = store.get_thumb(message.from_user.id)
    if not tid:
        return await message.reply_text(SC("❌ <b>No Custom Thumbnail Found</b>\n\n<i>Reply to a photo with /set_thumb to add one.</i>"), parse_mode=ParseMode.HTML)
    try:
        await message.reply_photo(tid, caption=SC("🖼 <b>Your Current Custom Thumbnail</b>\n\n<i>This is applied to all uploads.</i>\n<i>To delete, use /del_thumb</i>"),
                                  parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.reply_text(SC(f"❌ Error loading thumbnail: {e}\nPlease set a new one."))


async def on_del_thumb(client: Client, message):
    if not await gate(client, message):
        return
    if store.del_thumb(message.from_user.id):
        await message.reply_text(SC("🗑 <b>Custom Thumbnail Deleted</b>\n\n<i>Your uploads will now use the default video thumbnail.</i>"), parse_mode=ParseMode.HTML)
    else:
        await message.reply_text(SC("ℹ️ You don't have a custom thumbnail set."))


async def on_thumb_mode(client: Client, message):
    if not await gate(client, message):
        return
    if store.get_thumb(message.from_user.id):
        status, extra = "🟢 Custom Thumbnail Active", "<i>Use /view_thumb to preview</i>"
    else:
        status, extra = "🔴 No Custom Thumbnail", "<i>Use /set_thumb (reply to photo) to enable</i>"
    await message.reply_text(SC(f"🖼 <b>Thumbnail Status</b>\n\n{status}\n{extra}"), parse_mode=ParseMode.HTML)


async def _check_chat(client: Client, chat_id: int):
    """Returns (title, None) or (None, error)."""
    try:
        chat = await client.get_chat(chat_id)
        return (chat.title or "Private Chat"), None
    except Exception as e:
        return None, str(e)


async def on_setchat(client: Client, message):
    if not await gate(client, message):
        return
    uid = message.from_user.id
    if len(message.command) < 2:
        return await message.reply_text(
            SC("🗑 <b>Set Dump Chat</b>\n\n<b>Usage:</b>\n<code>/setchat &lt;chat_id&gt;</code> → every file you download also gets copied here\n"
               "<code>/setchat clear</code> → remove it\n\n<i>Example: /setchat -1001234567890</i>\nℹ️ I must already be an admin in that channel/group."),
            parse_mode=ParseMode.HTML)
    arg = message.command[1].strip()
    if arg.lower() == "clear":
        store.set_dump_chat(uid, None)
        return await message.reply_text(SC("✅ <b>Dump Chat Cleared.</b>"), parse_mode=ParseMode.HTML)
    try:
        chat_id = int(arg)
    except ValueError:
        return await message.reply_text(SC("❌ <b>Invalid Chat ID</b>\n\n<i>Must be a number (e.g., -1001234567890)</i>"), parse_mode=ParseMode.HTML)
    title, err = await _check_chat(client, chat_id)
    if err:
        return await message.reply_text(SC("❌ <b>Unable to Access Chat</b>\n") + f"<i>{html.escape(err[:200])}</i>", parse_mode=ParseMode.HTML)
    store.set_dump_chat(uid, chat_id)
    await message.reply_text(SC("✅ <b>Dump Chat Set Successfully</b>\n\n<b>Forward To:</b> ") + f"<code>{chat_id}</code>\n" + SC("<b>Title:</b> ") + html.escape(title),
                             parse_mode=ParseMode.HTML)


async def on_admin_set_dump(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    if len(message.command) < 3:
        return await message.reply_text(SC("⚠️ <b>Usage:</b> <code>/set_dump &lt;user_id&gt; &lt;chat_id&gt;</code>\n<code>/set_dump &lt;user_id&gt; clear</code> → remove it"),
                                        parse_mode=ParseMode.HTML)
    try:
        target = int(message.command[1])
    except ValueError:
        return await message.reply_text(SC("⚠️ user_id must be a number."))
    if message.command[2].strip().lower() == "clear":
        store.set_dump_chat(target, None)
        return await message.reply_text(SC("✅ Dump chat cleared for ") + f"<code>{target}</code>.", parse_mode=ParseMode.HTML)
    try:
        chat_id = int(message.command[2].strip())
    except ValueError:
        return await message.reply_text(SC("⚠️ chat_id must be a number."))
    title, err = await _check_chat(client, chat_id)
    if err:
        return await message.reply_text(SC("❌ <b>Unable to Access Chat</b>\n") + f"<i>{html.escape(err[:200])}</i>", parse_mode=ParseMode.HTML)
    store.set_dump_chat(target, chat_id)
    await message.reply_text(SC("✅ Dump chat set for ") + f"<code>{target}</code> → <code>{chat_id}</code> ({html.escape(title)})", parse_mode=ParseMode.HTML)


def settings_kb() -> Markup:
    return Markup([
        [mbtn("📊 My Usage Stats", "settings_stats", style=BTN_PRIMARY)],
        [mbtn("🗑 Dump Chat", "settings_dump", style=BTN_PRIMARY)],
        [mbtn("🖼 Thumbnail", "settings_thumb", style=BTN_PRIMARY), mbtn("📝 Caption", "settings_caption", style=BTN_PRIMARY)],
        [mbtn("❌ Close", "settings_close", style=BTN_DANGER)],
    ])


def settings_back_kb() -> Markup:
    return Markup([[mbtn("⬅️ Back", "settings_back", style=BTN_PRIMARY), mbtn("❌ Close", "settings_close", style=BTN_DANGER)]])


def settings_text(uid: int) -> str:
    badge = "👑 Admin" if is_admin(uid) else ("💎 Premium Member" if store.is_premium(uid) else "👤 Free User")
    return SC("⚙️ <b>Settings Panel</b>\n━━━━━━━━━━━━━━━━━━\n"
              f"<b>Account:</b> {badge}\n<b>User ID:</b> <code>{uid}</code>\n\n<i>Select an option below to customize your experience.</i>")


async def on_settings(client: Client, message):
    if not await gate(client, message):
        return
    await message.reply_text(settings_text(message.from_user.id), reply_markup=settings_kb(), parse_mode=ParseMode.HTML)


async def on_settings_cb(client: Client, cq):
    uid, data = cq.from_user.id, cq.data
    try:
        if data == "settings_stats":
            await cq.message.edit_text(my_status_text(uid), reply_markup=settings_back_kb(), parse_mode=ParseMode.HTML)
        elif data == "settings_dump":
            cur = store.get_dump_chat(uid)
            if cur:
                title, err = await _check_chat(client, cur)
                text = (SC("🗑 <b>Current Dump Chat</b>\n\n<b>Chat ID:</b> ") + f"<code>{cur}</code>\n" + SC("<b>Title:</b> ") + html.escape(title or "Unknown (Inaccessible)")
                        + SC("\n\n<i>All your delivered files are copied here.</i>\n<i>Use /setchat to change or clear.</i>"))
            else:
                text = SC("🗑 <b>No Dump Chat Set</b>\n\n<i>Delivered files only appear in this chat.</i>\n<i>Use /setchat &lt;chat_id&gt; to enable forwarding.</i>")
            await cq.message.edit_text(text, reply_markup=settings_back_kb(), parse_mode=ParseMode.HTML)
        elif data == "settings_thumb":
            tid = store.get_thumb(uid)
            if tid:
                await cq.message.reply_photo(tid, caption=SC("🖼 <b>Your Current Custom Thumbnail</b>\n\n<i>Use /set_thumb (reply to photo) to update, /del_thumb to remove</i>"),
                                             parse_mode=ParseMode.HTML)
                return await cq.answer("Thumbnail preview sent below 👇")
            await cq.message.edit_text(SC("🖼 <b>No Custom Thumbnail Set</b>\n\n<i>Reply to a photo with /set_thumb to add one.</i>"),
                                       reply_markup=settings_back_kb(), parse_mode=ParseMode.HTML)
        elif data == "settings_caption":
            cap = store.get_caption(uid)
            if cap:
                text = (SC("📝 <b>Current Custom Caption</b>\n\n") + f"<code>{html.escape(cap)}</code>\n\n"
                        + SC("<i>Placeholders: {filename}, {size}, {quality}, {source}, {duration}, {part}, {user}</i>\n<i>/set_caption &lt;text&gt; to change • /del_caption to remove</i>"))
            else:
                text = SC("📝 <b>No Custom Caption Set</b>\n\n<i>Use /set_caption &lt;text&gt; to set one.</i>")
            await cq.message.edit_text(text, reply_markup=settings_back_kb(), parse_mode=ParseMode.HTML)
        elif data == "settings_back":
            await cq.message.edit_text(settings_text(uid), reply_markup=settings_kb(), parse_mode=ParseMode.HTML)
        elif data == "settings_close":
            try:
                await cq.message.delete()
            except Exception:
                pass
    except MessageNotModified:
        pass
    except Exception as e:
        log.warning("settings '%s' failed for %s: %s", data, uid, e)
        try:
            return await cq.answer("⚠️ Something went wrong, try again.", show_alert=True)
        except Exception:
            pass
    await cq.answer()



_KEEP_EXT = (".mp3", ".jpg", ".jpeg", ".pdf", ".zip", ".rar", ".7z", ".apk", ".txt", ".doc", ".xlsx")  # these stay as they are; EVERYTHING else is delivered as .mp4


def force_mp4_name(path: str) -> str:
    """Rename (NOT convert) any file except mp3/jpg/jpeg to .mp4. Bytes stay untouched, so it is instant."""
    low = path.lower()
    if low.endswith(_KEEP_EXT) or low.endswith(".mp4"):
        return path
    dst = os.path.splitext(path)[0] + ".mp4"
    if dst == path:
        dst = path + ".mp4"
    try:
        os.replace(path, dst)
        return dst
    except OSError as e:
        log.warning("rename to mp4 failed for %s: %s", path, e)
        return path


_NEEDS_MP4 = (".ts", ".mkv", ".avi", ".flv", ".3gp", ".webm", ".wmv", ".mpg", ".mpeg", ".mov", ".m4v")


async def remux_to_mp4(path: str, job: dict = None, on_progress=None) -> str:
    """Telegram only shows real playable videos for .mp4 — .ts/.mkv/etc. arrive as plain documents.
    Remux to .mp4 (stream copy, no quality loss; audio -> aac only if copy is not possible), with a live progress callback
    on_progress(done_bytes, total_bytes). Returns the new path, or the original on failure. Raises Cancelled if job is cancelled."""
    low = path.lower()
    if not low.endswith(_NEEDS_MP4) or not shutil.which("ffmpeg"):
        return path
    dst = os.path.splitext(path)[0] + ".mp4"
    insize = os.path.getsize(path)
    dur, _, _ = await probe_video(path)
    tries = (
        ["-map", "0:v:0", "-map", "0:a?", "-c", "copy", "-bsf:a", "aac_adtstoasc"],
        ["-map", "0:v:0", "-map", "0:a?", "-c:v", "copy", "-c:a", "aac", "-b:a", "128k"],
    )

    def _rm(fp):
        try:
            os.remove(fp)
        except OSError:
            pass

    for args in tries:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-nostdin", "-loglevel", "error", "-y", "-progress", "pipe:1", "-nostats", "-i", path, *args,
            "-movflags", "+faststart", dst, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        cur_sec, last_edit = 0.0, 0.0
        try:
            while True:
                try:
                    line = await asyncio.wait_for(proc.stdout.readline(), timeout=2)
                except asyncio.TimeoutError:
                    line = None
                if line == b"":  # ffmpeg finished
                    break
                if job and job.get("cancel"):
                    raise Cancelled()
                if line:
                    txt = line.decode(errors="ignore").strip()
                    if txt.startswith(("out_time_us=", "out_time_ms=")):  # both are microseconds in ffmpeg
                        try:
                            cur_sec = int(txt.split("=", 1)[1]) / 1e6
                        except ValueError:
                            pass
                now = time.time()
                if on_progress and now - last_edit >= EDIT_INTERVAL:
                    last_edit = now
                    if dur and cur_sec > 0:
                        frac = cur_sec / dur
                    else:  # no duration known: stream copy output ~ input size
                        frac = (os.path.getsize(dst) / insize) if insize and os.path.exists(dst) else 0
                    await on_progress(int(min(frac, 0.99) * insize), insize)
            await proc.wait()
        except BaseException:  # Cancelled / task cancelled -> stop ffmpeg and clean up
            if proc.returncode is None:
                proc.kill()
                await proc.wait()
            _rm(dst)
            raise
        if proc.returncode == 0 and os.path.exists(dst) and os.path.getsize(dst) > 50 * 1024:
            _rm(path)
            if on_progress:
                await on_progress(insize, insize)
            return dst
        _rm(dst)
    log.warning("remux to mp4 failed for %s - sending original", path)
    return path


RANK_FIRST = os.getenv("RANK_FIRST", "0") == "1"  # 1 = speed-test every resolver link BEFORE downloading (slower start)
RANK_DEADLINE = 14.0


async def rank_links(f: fz.FZFile) -> list:
    """All working download links for one Flezen video, fastest first (every resolver tier runs at once)."""
    cands = [f] if f.download_url else []
    if f.source:
        try:
            cands += await fz.resolve_candidates(f.source, http, timeout=RANK_DEADLINE)
        except Exception as e:
            log.info("rank: resolve failed: %s", e)

    async def probe_one(c):
        if not c.download_url:
            return None
        if f.size and c.size and abs(c.size - f.size) > 0.03 * f.size:  # not the same file
            return None
        sp = await probe_speed(c.download_url)
        return (sp, c) if sp > 0 else None

    seen, uniq = set(), []
    for c in cands:
        if c.download_url and c.download_url not in seen:
            seen.add(c.download_url)
            uniq.append(c)
    res = await asyncio.wait_for(asyncio.gather(*(probe_one(c) for c in uniq), return_exceptions=True), timeout=RANK_DEADLINE + 6) \
        if uniq else []
    found = [r for r in res if isinstance(r, tuple)]
    found.sort(key=lambda x: -x[0])
    log.info("link ranking: %s", ", ".join(f"{human_size(sp)}/s" for sp, _ in found) or "none usable")
    return [c for _, c in found]


async def transfer(client: Client, msg, user, f: fz.FZFile, job: dict, label: str = "", kb=None, back_kb=None, source_url: str = "") -> str:
    """Download `f` and upload it to msg.chat. Edits `msg` with progress.
    Returns "ok", "cancelled", "toobig" or "failed". Never raises."""
    uid = user.id
    cancel_kb = kb
    # same user + same link + same file name -> same folder, so a restarted bot finds the partial file again
    src_url = (source_url or f.source or "").strip()
    rkey = hashlib.sha1(f"{uid}|{src_url.split('?')[0]}|{f.name}".encode()).hexdigest()[:12]
    resumable = RESUME_DOWNLOADS and bool(src_url) and rkey not in ACTIVE_RESUME
    workdir = os.path.join(DOWNLOAD_DIR, f"{uid}_{rkey}" if resumable else f"{uid}_{uuid.uuid4().hex[:8]}")
    if resumable:
        ACTIVE_RESUME.add(rkey)
    keep = False  # True when the bot is stopping: keep the partial file + resume entry
    os.makedirs(workdir, exist_ok=True)
    path = os.path.join(workdir, safe_name(f.name))
    title = f"{label}<b>{html.escape(f.name[:80])}</b>"
    lbl_line = f"┣⪼ 📦 Item: {label.strip()}\n" if label else ""
    t0 = time.time()
    ck = cache_key_for(f, source_url)
    try:
        if ck:  # instant resend if this exact file was uploaded before (fbot-style file cache)
            cached = await store.get_cached_file(*ck)
            if cached:
                await safe_edit(msg, f"⚡ <b>Found in cache, sending instantly…</b>\n{title}", kb)
                cap = build_caption(name=cached.get("name") or f.name, size=cached.get("size") or f.size, user=user,
                                    source_url=source_url or f.source or "", dl_s=0, ul_s=0,
                                    duration=cached.get("duration", 0), height=cached.get("height", 0))
                hit = await send_cached_file(client, msg.chat.id, cached, cap)
                if hit is not None:
                    asyncio.create_task(backup_to_linked_channels(client, msg.chat.id, hit.id))
                    asyncio.create_task(forward_to_dump_chat(client, msg.chat.id, hit.id))
                    csize = cached.get("size") or f.size or 0
                    store.record_download(uid, csize)
                    try:
                        await msg.delete()
                    except Exception:
                        await safe_edit(msg, f"✅ <b>Done!</b>\n{title}\n⏱ {human_time(time.time() - t0)}")
                    await log_to_channel(client, f"⚡ <b>Cache hit</b>\n{user_tag(user)}\n📄 {html.escape(f.name[:100])}\n💾 {human_size(csize)}")
                    return "ok"
                log.warning("cache hit could not be sent — dropping stale entry for %s", f.name[:60])
                await store.delete_cached_file(*ck)
        if not f.download_url:
            raise RuntimeError("no download link for this file")
        if f.size and f.size > MAX_DL_BYTES:
            raise RuntimeError("TOO_BIG")
        if resumable:  # remember this download, so it continues after a restart / crash
            prev = (store.data.get("resume") or {}).get(rkey) or {}
            store.resume_put(rkey, {"uid": uid, "chat_id": msg.chat.id, "msg_id": msg.id, "url": src_url, "name": f.name,
                                    "size": f.size or 0, "label": label, "ts": prev.get("ts") or int(time.time()),
                                    "tries": prev.get("tries", 0)})
            await store.flush()
        admin_run = is_admin(uid)  # admin / owner never wait in the queue
        if MAX_CONCURRENT and not admin_run and sem.locked():  # "Queued" only when every slot is really taken
            await safe_edit(msg, f"⏳ <b>Queued…</b>\n{title}", cancel_kb)
        async with (contextlib.nullcontext() if admin_run else sem):
            if job["cancel"]:
                raise Cancelled()

            async def dl_prog(done, total):
                await safe_edit(msg, progress_text("download", f.name, done, total, t0, lbl_line, base=job.get("resumed", 0)), cancel_kb)

            cur, tried_alt, size = f, False, 0
            # progress bar starts straight away; a slow / expired link is swapped once for a freshly resolved one
            await safe_edit(msg, progress_text("download", f.name, 0, f.size or 0, t0, lbl_line), cancel_kb)
            job.pop("resumed", None)
            if f.source and RANK_FIRST:  # optional: speed-test all resolver links BEFORE starting (RANK_FIRST=1)
                await safe_edit(msg, f"⚡ <b>Finding the fastest server…</b>\n{title}", cancel_kb)
                ranked = await rank_links(f)
                if ranked:
                    cur = ranked[0]
            d0 = time.time()
            while True:
                job["slow_check"] = bool(cur.source and not cur.fallback and not tried_alt)  # only if a retry is still possible
                try:
                    size = await download_file(cur.download_url, path, job, cur.size, dl_prog)
                    validate_download(path, f.name, size, f.is_video)
                    break
                except (Cancelled, asyncio.CancelledError):
                    raise
                except Exception as e:
                    if str(e) == "TOO_BIG" or tried_alt or not cur.source:
                        raise
                    log.warning("download failed (%s) — fetching a fresh link", e)
                    await safe_edit(msg, f"🔁 <b>Link problem — fetching a fresh link…</b>\n{title}", cancel_kb)
                    tried_alt = True
                    alt = await alt_link(cur)
                    if not alt:
                        raise
                    cur = alt
            job["slow_check"] = False
            dl_secs = time.time() - d0

            # fix a missing extension from the file's magic bytes
            if "." not in os.path.basename(path):
                ext = sniff_ext(read_head(path))
                if ext:
                    os.replace(path, path + ext)
                    path += ext

            disp_name = f.name
            if path.lower().endswith(_NEEDS_MP4):  # mkv/ts/avi/webm... -> real mp4 container (stream copy, no quality loss)
                c0 = time.time()
                await safe_edit(msg, progress_text("convert", f.name, 0, size or 1, c0, lbl_line), cancel_kb)

                async def cv_prog(done, total, c0=c0):
                    await safe_edit(msg, progress_text("convert", f.name, done, total, c0, lbl_line), cancel_kb)

                rp = await remux_to_mp4(path, job, cv_prog)  # on failure returns the original path -> plain rename below
                if rp != path:
                    path = rp
                    size = os.path.getsize(path)
                    disp_name = os.path.splitext(f.name)[0] + ".mp4"
            has_ext = bool(os.path.splitext(f.name)[1])  # files with no extension in their name stay as they are
            new_path = force_mp4_name(path) if has_ext else path
            if new_path != path:
                path = new_path
                disp_name = os.path.splitext(f.name)[0] + ".mp4"

            thumb_path = None
            if f.thumb and f.thumb.startswith("http"):
                try:
                    async with http.get(f.thumb, timeout=aiohttp.ClientTimeout(total=15)) as tr:
                        if tr.status == 200 and "image" in (tr.headers.get("Content-Type") or ""):
                            thumb_path = os.path.join(workdir, "thumb.jpg")
                            with open(thumb_path, "wb") as th:
                                th.write(await tr.read())
                except Exception:
                    thumb_path = None

            custom_thumb = None  # user's /set_thumb photo overrides every other thumbnail
            cthumb_id = store.get_thumb(uid)
            if cthumb_id:
                try:
                    cp = await client.download_media(cthumb_id, file_name=os.path.join(workdir, "custom_thumb_raw.jpg"))
                    if cp and os.path.exists(cp):
                        custom_thumb = await finalize_custom_thumb(cp, os.path.join(workdir, "custom_thumb.jpg"))
                except Exception as e:
                    log.warning("custom thumb download failed: %s", e)

            # split files that exceed Telegram's per-file limit
            parts = [path]
            if size > MAX_FILE_BYTES:
                await safe_edit(msg, f"✂️ <b>Splitting into parts…</b>\n{title}", cancel_kb)
                parts = (await split_video(path, MAX_FILE_BYTES)) if f.is_video else []
                if parts:
                    os.remove(path)
                else:
                    parts = await asyncio.to_thread(split_bytes, path, MAX_FILE_BYTES)
                    os.remove(path)
                    split_note = "\n🧩 Raw split — join the parts (<code>cat name.* &gt; name</code> or 7-Zip) to get the file."
                    await client.send_message(msg.chat.id, f"ℹ️ <b>{html.escape(f.name[:80])}</b> was too large and is sent as {len(parts)} raw parts." + split_note, parse_mode=ParseMode.HTML)

            for pi, part in enumerate(parts, 1):
                if job["cancel"]:
                    raise Cancelled()
                is_vid = f.is_video and part.lower().endswith((".mp4", ".mkv", ".mov", ".webm", ".m4v", ".avi", ".ts", ".flv", ".3gp"))
                duration = width = height = 0
                pthumb = thumb_path if len(parts) == 1 else None
                if is_vid:
                    duration, width, height = await probe_video(part)
                    duration = duration or (f.duration if len(parts) == 1 else 0)  # API duration if the file itself could not be read
                    if not (width and height):  # Telegram shows a video with no size as a plain file -> give it a sane 16:9 default
                        width, height = 1280, 720
                    tp = os.path.join(workdir, f"frame_{pi}.jpg")
                    if not custom_thumb and await make_thumb(part, tp, duration):  # sharp frame beats the small API thumbnail
                        pthumb = tp
                if custom_thumb:
                    pthumb = custom_thumb
                psize = os.path.getsize(part)
                plabel = f" • part {pi}/{len(parts)}" if len(parts) > 1 else ""
                ptitle = title + plabel
                u0, ulast = time.time(), [0.0]

                async def up_prog(cur_b, total_b, part_name=part, u0=u0, ulast=ulast):
                    if job["cancel"]:
                        client.stop_transmission()
                    now = time.time()
                    if now - ulast[0] < EDIT_INTERVAL:
                        return
                    ulast[0] = now
                    await safe_edit(msg, progress_text("upload", f.name if len(parts) == 1 else os.path.basename(part_name), cur_b, total_b, u0, lbl_line), cancel_kb)

                shown = os.path.basename(part) if len(parts) > 1 else disp_name
                cap_kw = dict(name=shown, size=psize, user=user, source_url=source_url or f.source or "", dl_s=dl_secs,
                              duration=duration, height=height, part=f"{pi}/{len(parts)}" if len(parts) > 1 else "")
                caption = build_caption(**cap_kw)
                try:
                    u_start = time.time()
                    if is_vid:
                        sent = await client.send_video(msg.chat.id, part, caption=caption, parse_mode=ParseMode.HTML, supports_streaming=True,
                                                       duration=duration, width=width, height=height, thumb=pthumb, file_name=shown, progress=up_prog)
                    else:
                        sent = await client.send_document(msg.chat.id, part, caption=caption, parse_mode=ParseMode.HTML,
                                                          thumb=pthumb, progress=up_prog)
                    try:  # add the real upload time now that it is known
                        await sent.edit_caption(build_caption(**cap_kw, ul_s=time.time() - u_start), parse_mode=ParseMode.HTML)
                    except Exception:
                        pass
                    asyncio.create_task(backup_to_linked_channels(client, msg.chat.id, sent.id))
                    asyncio.create_task(forward_to_dump_chat(client, msg.chat.id, sent.id))
                    if ck and len(parts) == 1:  # split uploads have no single file_id, so only single-part files are cached
                        await store_in_cache(client, ck, sent, f.name, psize, duration, width, height)
                except Exception:
                    if job["cancel"]:
                        raise Cancelled()
                    raise

        store.record_download(uid, size)
        try:  # video/file is already in the chat -> remove the progress/menu message instead of leaving a "Done!" card
            await msg.delete()
        except Exception:
            await safe_edit(msg, f"✅ <b>Done!</b>\n{title}\n⏱ {human_time(time.time() - t0)}")
        await log_to_channel(client, f"✅ <b>Download</b>\n{user_tag(user)}\n📄 {html.escape(f.name[:100])}\n💾 {human_size(size)}")
        return "ok"
    except asyncio.CancelledError:
        keep = True  # the bot is stopping (not the user's Cancel button): keep the partial file, it continues after restart
        raise
    except Cancelled:
        await safe_edit(msg, f"🛑 <b>Cancelled</b>\n{title}", back_kb)
        return "cancelled"
    except Exception as e:
        if str(e) == "TOO_BIG":
            await safe_edit(msg, f"⚠️ <b>Too big</b> (limit {human_size(MAX_DL_BYTES)})\n{title}\nUse the Stream button.", back_kb)
            return "toobig"
        log.exception("download failed")
        await safe_edit(msg, f"❌ <b>Failed</b>\n{title}\n<code>{html.escape(str(e)[:200])}</code>", back_kb)
        await log_to_channel(client, f"❌ <b>Download failed</b>\n{user_tag(user)}\n<code>{html.escape(str(e)[:300])}</code>")
        return "failed"
    finally:
        keep = keep or SHUTTING_DOWN[0]
        if resumable:
            ACTIVE_RESUME.discard(rkey)
            if not keep:
                store.resume_del(rkey)
        if not keep:
            shutil.rmtree(workdir, ignore_errors=True)




async def on_download(client: Client, cq):
    _, rid, idx = cq.data.split(":")
    uid = cq.from_user.id
    f = _get_file(rid, int(idx), uid)
    if not f:
        await cq.answer("⌛ Expired — send the link again.", show_alert=True)
        return
    if not await precheck(client, cq):
        return
    src_url = RESULTS.get(rid, {}).get("url", "")
    ck = cache_key_for(f, src_url)
    already_cached = bool(ck and FILE_CACHE and await store.get_cached_file(*ck))
    if not already_cached:  # cached files are sent from the cache: no API call, no size check
        await safe_edit(cq.message, SC("🔄 <b>Calling API to get download link…</b>"))
        f = await fresh_file(rid, int(idx), f)
        if not f.download_url:
            log.warning("❌ No download link for %s", f.name[:50])
            await safe_edit(cq.message, SC("❌ <b>No download link available</b>"), file_menu_kb(rid, int(idx), f))
            await cq.answer("No download link available for this video.", show_alert=True)
            return
        if f.size and f.size > MAX_DL_BYTES:
            await safe_edit(cq.message, menu_text(src_url, f), file_menu_kb(rid, int(idx), f))
            await cq.answer(f"File is bigger than {human_size(MAX_DL_BYTES)} — use the Stream button.", show_alert=True)
            return
    await cq.answer("⬇️ Starting…")
    jid = uuid.uuid4().hex[:8]
    job = {"cancel": False, "uid": uid}
    JOBS[jid] = job
    try:
        await transfer(client, cq.message, cq.from_user, f, job,
                       source_url=src_url,
                       kb=Markup([[mbtn("❌ Cancel", f"x:{jid}", style=BTN_DANGER)]]),
                       back_kb=file_menu_kb(rid, int(idx), f))
    finally:
        JOBS.pop(jid, None)


# ---------------------------------------------------------------- admin ----
async def on_stats(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    d = store.data
    await message.reply_text(
        SC("📊 <b>Bot Stats</b>\n\n"
           f"👥 <b>Total Users:</b> {len(d['users'])}\n"
           f"💎 <b>Premium Users:</b> {len(store.premium_ids())}\n"
           f"🚫 <b>Banned Users:</b> {store.banned_count()}\n\n"
           f"📦 <b>Total Downloads:</b> {d['total_downloads']}\n"
           f"💾 <b>Data Sent:</b> {human_size(d['total_bytes'])}\n"
           f"🗂 <b>Files Cached:</b> {await store.cache_count()}\n"
           f"🔄 <b>Active Jobs:</b> {len(JOBS)}\n"
           f"⏱ <b>Uptime:</b> {human_time(time.time() - START_TIME)}"),
        parse_mode=ParseMode.HTML)


async def on_clearcache(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    n = await store.clear_cache()
    await message.reply_text(f"🗑 <b>File cache cleared</b> — {n} entries removed.", parse_mode=ParseMode.HTML)


_ID_IN_TEXT = re.compile(r"(?:user\s*id|id|user)\s*[:=\-]?\s*(\d{5,15})", re.I)


async def target_user_id(client: Client, message):
    """Who to ban/unban: /ban <id>, /ban @username, or a reply to the user's message / a forwarded message /
    a bot notice that contains 'ID: 123456789' (e.g. the New User log). Returns int or None."""
    if len(message.command) >= 2:
        arg = message.command[1].strip()
        if re.fullmatch(r"-?\d+", arg):
            return int(arg)
        if arg.startswith("@") or re.fullmatch(r"[A-Za-z]\w{4,}", arg):
            try:
                return (await client.get_users(arg.lstrip("@"))).id
            except Exception:
                return None
        return None
    r = message.reply_to_message
    if not r:
        return None
    fwd = getattr(r, "forward_from", None) or getattr(getattr(r, "forward_origin", None), "sender_user", None)
    if fwd and not getattr(fwd, "is_bot", False):
        return fwd.id
    fu = r.from_user
    if fu and not fu.is_bot and not is_admin(fu.id):
        return fu.id
    m = _ID_IN_TEXT.search(r.text or r.caption or "")
    return int(m.group(1)) if m else None


async def on_ban(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    on = message.command[0].lower() == "ban"
    cmd = "ban" if on else "unban"
    uid = await target_user_id(client, message)
    if uid is None:
        await message.reply_text(SC(f"⚠️ <b>Usage:</b> <code>/{cmd} &lt;user_id&gt;</code>\nYa kisi user ke message par reply karke <code>/{cmd}</code> likho."), parse_mode=ParseMode.HTML)
        return
    if on and is_admin(uid):
        await message.reply_text(SC("⚠️ Can't ban an admin."))
        return
    store.ban(uid, on)
    await store.flush()
    await message.reply_text(SC(f"{'🚫' if on else '✅'} <code>{uid}</code> has been {'banned' if on else 'unbanned'}."), parse_mode=ParseMode.HTML)
    try:
        await client.send_message(uid, SC("🚫 You've been banned from using this bot." if on
                                          else "✅ You've been unbanned — you can use the bot again."))
    except Exception as e:
        log.warning("Couldn't notify %s about %s: %s", uid, cmd, e)


async def on_addpremium(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    usage = ("Usage:\n<code>/addpremium &lt;user_id&gt; &lt;days&gt;</code>\n"
             "<code>/addpremium &lt;user_id&gt; lifetime</code>\n\nExample: <code>/addpremium 123456789 30</code>")
    if len(parts) < 3 or not parts[1].isdigit():
        await message.reply_text(usage, parse_mode=ParseMode.HTML)
        return
    uid, arg = int(parts[1]), parts[2].lower()
    if arg in ("lifetime", "life", "forever", "0"):
        days = 0
    elif arg.isdigit():
        days = int(arg)
    else:
        await message.reply_text(usage, parse_mode=ParseMode.HTML)
        return
    info = store.add_premium(uid, days)
    await store.flush()
    plan = "Lifetime ♾️" if info["lifetime"] else f"{info['days_left']} day(s) left"
    await message.reply_text(f"✅ <b>Premium added</b>\n\nUser: <code>{uid}</code>\nPlan: <code>{plan}</code>", parse_mode=ParseMode.HTML)
    try:
        await client.send_message(uid, SC(f"🎉 <b>Premium Activated!</b>\n\n💎 Plan: <code>{plan}</code>\n✨ Ab aapko unlimited downloads milenge. Enjoy!"),
                                  parse_mode=ParseMode.HTML)
    except Exception:
        await message.reply_text("ℹ️ User ko notify nahi kar paya (bot start nahi kiya ya block hai).")
    await log_to_channel(client, f"💎 <b>Premium added</b> by <code>{message.from_user.id}</code>\nUser: <code>{uid}</code> · {plan}")


async def on_removepremium(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.reply_text("Usage: <code>/removepremium &lt;user_id&gt;</code>", parse_mode=ParseMode.HTML)
        return
    uid = int(parts[1])
    had = store.remove_premium(uid)
    await store.flush()
    if not had:
        await message.reply_text(f"⚠️ <code>{uid}</code> ke paas premium nahi tha.", parse_mode=ParseMode.HTML)
        return
    await message.reply_text(f"🗑 <b>Premium removed</b> for <code>{uid}</code>", parse_mode=ParseMode.HTML)
    try:
        await client.send_message(uid, SC("⚠️ <b>Aapka Premium plan hata diya gaya hai.</b>\n\n💎 Dobara lene ke liye /plans dekho."), parse_mode=ParseMode.HTML)
    except Exception:
        pass
    await log_to_channel(client, f"🗑 <b>Premium removed</b> by <code>{message.from_user.id}</code>\nUser: <code>{uid}</code>")


async def on_premiumlist(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    ids = store.premium_ids()
    if not ids:
        await message.reply_text("No premium users yet.")
        return
    rows = []
    for i in ids[:60]:
        pi = store.premium_info(i)
        rows.append(f"• <code>{i}</code> — " + ("Lifetime ♾️" if pi["lifetime"] else f"{pi['days_left']}d left"))
    await message.reply_text(f"💎 <b>Premium users ({len(ids)})</b>\n\n" + "\n".join(rows), parse_mode=ParseMode.HTML)


async def _broadcast_one(client: Client, cid: int, text, src, from_chat_id: int) -> str:
    try:
        if text is not None:
            await client.send_message(cid, text)
        else:
            await client.copy_message(chat_id=cid, from_chat_id=from_chat_id, message_id=src.id)
        return "success"
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        return await _broadcast_one(client, cid, text, src, from_chat_id)
    except (InputUserDeactivated, UserIsBlocked, PeerIdInvalid):
        store.remove_user(cid)
        return "removed"
    except Exception as e:
        log.warning("broadcast failed for %s: %s", cid, e)
        return "failed"


async def on_broadcast(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    src = message.reply_to_message
    text = None
    if len(message.command) >= 2:
        text = message.text.split(None, 1)[1]
    elif not src:
        await message.reply_text(
            SC("⚠️ <b>Usage:</b> <code>/broadcast &lt;message&gt;</code>\n"
               "(or reply to a message with just <code>/broadcast</code> to forward that)"),
            parse_mode=ParseMode.HTML)
        return
    ids = store.user_ids()
    total = len(ids)
    status = await message.reply_text(SC(f"📣 Broadcasting to {total} users..."))
    done = success = removed = failed = 0
    for cid in ids:
        r = await _broadcast_one(client, cid, text, src, message.chat.id)
        if r == "success":
            success += 1
        elif r == "removed":
            removed += 1
        else:
            failed += 1
        done += 1
        if done % 20 == 0 or done == total:
            try:
                await status.edit_text(
                    SC("📣 <b>Broadcast in progress...</b>\n\n"
                       f"👥 Total: {total}\n💫 Done: {done}/{total}\n✅ Success: {success}\n"
                       f"🚫 Removed (blocked/deleted): {removed}\n❌ Failed: {failed}"),
                    parse_mode=ParseMode.HTML)
            except Exception:
                pass
        await asyncio.sleep(0.05)
    await store.flush()
    try:
        await status.edit_text(
            SC("📣 <b>Broadcast done.</b>\n\n"
               f"✅ Success: {success}\n🚫 Removed (blocked/deleted): {removed}\n❌ Failed: {failed}"),
            parse_mode=ParseMode.HTML)
    except Exception:
        pass


async def on_users(client: Client, message):
    """Export all users as a JSON file (same as fbot's /users)."""
    if not is_admin(message.from_user.id):
        return
    export = [{"id": int(k), "name": v.get("name", ""), "is_banned": int(k) in store.data["banned"],
               "is_premium": store.is_premium(int(k)), "downloads": v.get("dl", 0), "first_seen": v.get("joined")}
              for k, v in store.data["users"].items()]
    path = f"/tmp/flezen_users_{message.chat.id}.json"
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(export, f, indent=2, ensure_ascii=False)
        await message.reply_document(path, caption=SC(f"📄 {len(export)} users exported."))
    except Exception as e:
        await message.reply_text(SC(f"⚠️ Error exporting users: {e}"))
    finally:
        try:
            os.remove(path)
        except Exception:
            pass


def status_text(started: bool, username: str) -> str:
    """fbot-style start / stop notice for the log channel."""
    ist = time.strftime("%I:%M %p IST", time.gmtime(time.time() + 5.5 * 3600))
    head = "🚀 <b>Bot successfully started!</b>" if started else "🛑 <b>Bot stopped!</b>"
    return (f"{head}\n\n"
            f"⭐ Bot: @{username}\n"
            f"👥 Users: {len(store.data['users'])}\n"
            f"⏳ Time: {ist}\n\n"
            f"👑 Developed by {DEVELOPER_URL.replace('https://t.me/', '@')}")


BOT_COMMANDS_LIST = [
    BotCommand("start", "🚀 Start the bot"),
    BotCommand("help", "❓ How to use the bot"),
    BotCommand("about", "ℹ️ About this bot"),
    BotCommand("plans", "💎 Premium plans"),
    BotCommand("myplan", "📊 Your status"),
    BotCommand("premium", "💎 Premium plans (same as /plans)"),
    BotCommand("mystatus", "📊 Your status (same as /myplan)"),
    BotCommand("referral", "🎁 Refer friends & earn free premium"),
    BotCommand("cancel", "🚫 Cancel current active download"),
    BotCommand("settings", "⚙️ Settings panel (caption, thumbnail, dump chat)"),
    BotCommand("set_caption", "📝 Set custom caption (/set_caption <text>)"),
    BotCommand("see_caption", "📝 View your custom caption"),
    BotCommand("del_caption", "🗑 Delete your custom caption"),
    BotCommand("set_thumb", "🖼 Set custom thumbnail (reply to a photo)"),
    BotCommand("view_thumb", "🖼 View your custom thumbnail"),
    BotCommand("see_thumb", "🖼 View your custom thumbnail"),
    BotCommand("del_thumb", "🗑 Delete your custom thumbnail"),
    BotCommand("delete_thumb", "🗑 Delete your custom thumbnail"),
    BotCommand("thumb_mode", "🖼 Thumbnail status"),
    BotCommand("setchat", "💬 Set/clear your personal dump chat"),
    BotCommand("set_dump", "💬 [Admin] Set a user's dump chat (/set_dump <uid> <chat_id|clear>)"),
    BotCommand("set_channel_id", "📡 [Admin] Link a channel (/set_channel_id -100…)"),
    BotCommand("channel_id", "🔗 [Admin] List linked channels"),
    BotCommand("del_channel_id", "🗑 [Admin] Unlink channel (/del_channel_id <id> or all)"),
    BotCommand("stats", "📊 [Admin] Bot statistics"),
    BotCommand("broadcast", "📣 [Admin] Broadcast a message (reply to it)"),
    BotCommand("addpremium", "💎 [Admin] Add premium (/addpremium <id> <days|lifetime>)"),
    BotCommand("removepremium", "🗑 [Admin] Remove premium (/removepremium <id>)"),
    BotCommand("premiumlist", "📋 [Admin] List premium users"),
    BotCommand("users", "👥 [Admin] Export users list"),
    BotCommand("ban", "⛔ [Admin] Ban a user (/ban <id> or reply)"),
    BotCommand("unban", "✅ [Admin] Unban a user (/unban <id> or reply)"),
]


# ------------------------------------------------------- resume after restart ----
async def _resume_one(app: Client, key: str, e: dict, delay: float):
    """Re-fetch a fresh link for an interrupted download and continue it into the same partial file."""
    await asyncio.sleep(delay)
    uid, chat_id, url, name = e["uid"], e["chat_id"], e.get("url", ""), e.get("name", "")
    jid = uuid.uuid4().hex[:8]
    job = {"cancel": False, "uid": uid}
    JOBS[jid] = job
    kb = Markup([[mbtn("❌ Cancel", f"x:{jid}", style=BTN_DANGER)]])
    try:
        msg = None
        try:
            msg = await app.get_messages(chat_id, e["msg_id"])
            if getattr(msg, "empty", False):
                msg = None
        except Exception:
            msg = None
        note = f"♻️ <b>Bot restarted — continuing your download…</b>\n<code>{html.escape(name[:80])}</code>"
        if msg is None:
            msg = await app.send_message(chat_id, note, parse_mode=ParseMode.HTML, reply_markup=kb)
        else:
            await safe_edit(msg, note, kb)
        res = await asyncio.wait_for(fz.fetch_flezen(url, http), timeout=fz.RESOLVE_TIMEOUT)
        f = res.files[0]
        if not f.download_url:
            raise RuntimeError("file not found in this link anymore")
        user = await app.get_users(uid)
        log.info("resuming interrupted download for %s: %s", uid, name[:60])
        await transfer(app, msg, user, f, job, label=e.get("label", ""), kb=kb, source_url=url)
    except asyncio.CancelledError:
        raise
    except Exception as ex:
        log.warning("could not resume %s: %s", name[:60], ex)
        store.resume_del(key)
        shutil.rmtree(os.path.join(DOWNLOAD_DIR, f"{uid}_{key}"), ignore_errors=True)
        with contextlib.suppress(Exception):
            await app.send_message(chat_id, f"⚠️ <b>Your download was interrupted and could not be resumed.</b>\n<code>{html.escape(name[:80])}</code>\nPlease send the link again.", parse_mode=ParseMode.HTML)
    finally:
        JOBS.pop(jid, None)


async def resume_interrupted(app: Client):
    """Called once after startup: every download that was still running when the bot stopped is continued."""
    if not RESUME_DOWNLOADS:
        return
    pend = store.resume_all()
    if not pend:
        return
    now, todo = time.time(), []
    for key, e in pend.items():
        tries = int(e.get("tries", 0))
        if now - e.get("ts", 0) > RESUME_MAX_AGE or tries >= RESUME_MAX_TRIES:
            store.resume_del(key)
            shutil.rmtree(os.path.join(DOWNLOAD_DIR, f"{e.get('uid')}_{key}"), ignore_errors=True)
            with contextlib.suppress(Exception):
                await app.send_message(e["chat_id"], f"⚠️ <b>Your download was interrupted.</b>\n<code>{html.escape(e.get('name', '')[:80])}</code>\nPlease send the link again.", parse_mode=ParseMode.HTML)
            continue
        e["tries"] = tries + 1
        store.resume_put(key, e)
        todo.append((key, e))
    await store.flush()
    log.info("resuming %d interrupted download(s)", len(todo))
    for i, (key, e) in enumerate(todo):
        asyncio.create_task(_resume_one(app, key, e, delay=i * 3))


# ------------------------------------------------------------------ main ----
async def main():
    log.info("button colours: %s", "ON" if BTN_STYLE_IMPORT_OK else "OFF (ButtonStyle missing in installed kurigram)")
    global http
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    http = aiohttp.ClientSession(headers={"User-Agent": DL_HEADERS["User-Agent"]})
    web_runner = await stream_proxy.start_web_server(PORT, lambda: http, DL_HEADERS)
    pinger = asyncio.create_task(stream_proxy.keep_alive_loop(lambda: http))
    app = Client("flezen_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True, parse_mode=ParseMode.HTML)

    private = filters.private
    app.add_handler(MessageHandler(on_start, filters.command("start") & private))
    app.add_handler(MessageHandler(on_help, (filters.command("help") | menu_text_filter(BTN_HELP)) & private))
    app.add_handler(MessageHandler(on_plans, (filters.command(["plans", "premium"]) | menu_text_filter(BTN_PLANS)) & private))
    app.add_handler(MessageHandler(on_my_status, (filters.command(["myplan", "mystatus"]) | menu_text_filter(BTN_MYSTATUS)) & private))
    app.add_handler(MessageHandler(on_support, menu_text_filter(BTN_SUPPORT) & private))
    app.add_handler(CallbackQueryHandler(on_fallback_download, filters.regex(r"^fallback_download$")))
    app.add_handler(CallbackQueryHandler(on_fallback_status, filters.regex(r"^fallback_status$")))
    app.add_handler(CallbackQueryHandler(on_show_plans, filters.regex(r"^show_plans$")))
    app.add_handler(CallbackQueryHandler(on_plans_back, filters.regex(r"^plans_back$")))
    app.add_handler(CallbackQueryHandler(on_plan_selected, filters.regex(r"^plan_\d+$")))
    app.add_handler(CallbackQueryHandler(on_paid, filters.regex(r"^paid_\d+$")))
    app.add_handler(MessageHandler(on_about, filters.command("about") & private))
    app.add_handler(CallbackQueryHandler(on_about_close, filters.regex(r"^about_close$")))
    app.add_handler(MessageHandler(on_cancel, filters.command("cancel") & private))
    app.add_handler(MessageHandler(on_stats, filters.command("stats") & private))
    app.add_handler(MessageHandler(on_clearcache, filters.command("clearcache") & private))
    app.add_handler(MessageHandler(on_ban, filters.command(["ban", "unban"]) & (private | filters.group)))
    app.add_handler(MessageHandler(on_broadcast, filters.command("broadcast") & private))
    app.add_handler(MessageHandler(on_users, filters.command("users") & private))
    app.add_handler(MessageHandler(on_addpremium, filters.command("addpremium") & private))
    app.add_handler(MessageHandler(on_removepremium, filters.command(["removepremium", "rmpremium"]) & private))
    app.add_handler(MessageHandler(on_premiumlist, filters.command("premiumlist") & private))
    app.add_handler(MessageHandler(on_referral, filters.command("referral") & private))
    app.add_handler(MessageHandler(on_set_channel, filters.command("set_channel_id") & private))
    app.add_handler(MessageHandler(on_channel_list, filters.command("channel_id") & private))
    app.add_handler(MessageHandler(on_del_channel, filters.command("del_channel_id") & private))
    app.add_handler(CallbackQueryHandler(on_ref_getlink, filters.regex(r"^ref_getlink$")))
    app.add_handler(CallbackQueryHandler(on_ref_menu, filters.regex(r"^ref_menu$")))
    app.add_handler(CallbackQueryHandler(on_ref_count, filters.regex(r"^ref_count$")))
    app.add_handler(CallbackQueryHandler(on_ref_rewards, filters.regex(r"^ref_rewards$")))
    app.add_handler(MessageHandler(on_set_caption, filters.command("set_caption") & private))
    app.add_handler(MessageHandler(on_see_caption, filters.command("see_caption") & private))
    app.add_handler(MessageHandler(on_del_caption, filters.command("del_caption") & private))
    app.add_handler(MessageHandler(on_set_thumb, filters.command("set_thumb") & private))
    app.add_handler(MessageHandler(on_view_thumb, filters.command(["view_thumb", "see_thumb"]) & private))
    app.add_handler(MessageHandler(on_del_thumb, filters.command(["del_thumb", "delete_thumb"]) & private))
    app.add_handler(MessageHandler(on_thumb_mode, filters.command("thumb_mode") & private))
    app.add_handler(MessageHandler(on_setchat, filters.command("setchat") & private))
    app.add_handler(MessageHandler(on_admin_set_dump, filters.command("set_dump") & private))
    app.add_handler(MessageHandler(on_settings, filters.command("settings") & private))
    app.add_handler(CallbackQueryHandler(on_settings_cb, filters.regex(r"^settings_(stats|dump|thumb|caption|back|close)$")))
    app.add_handler(MessageHandler(on_link, (filters.text | filters.caption) & private & NOT_MENU_BUTTON & ~filters.command(["start", "help", "about", "cancel", "stats", "ban", "unban", "broadcast", "plans", "premium", "myplan", "mystatus", "addpremium", "removepremium", "rmpremium", "premiumlist", "users", "referral", "set_channel_id", "channel_id", "del_channel_id", "set_caption", "see_caption", "del_caption", "set_thumb", "view_thumb", "see_thumb", "del_thumb", "delete_thumb", "thumb_mode", "setchat", "set_dump", "settings", "clearcache"])))
    app.add_handler(CallbackQueryHandler(on_verify, filters.regex(r"^verify$")))
    app.add_handler(CallbackQueryHandler(on_download, filters.regex(r"^dl:")))
    app.add_handler(CallbackQueryHandler(on_stream, filters.regex(r"^st:")))
    app.add_handler(CallbackQueryHandler(on_menu_back, filters.regex(r"^m:")))
    app.add_handler(CallbackQueryHandler(on_cancel_menu, filters.regex(r"^cx:")))
    app.add_handler(CallbackQueryHandler(on_cancel_btn, filters.regex(r"^x:")))

    await app.start()
    try:  # "/" command menu (same idea as fbot's BOT_COMMANDS_LIST) - no need to set it in BotFather
        await app.set_bot_commands(BOT_COMMANDS_LIST)
    except Exception as e:
        log.warning("set_bot_commands failed: %s", e)
    me = await app.get_me()
    log.info("Started as @%s", me.username)
    saver = asyncio.create_task(store.autosave_loop())
    await log_to_channel(app, status_text(True, me.username))
    resumer = asyncio.create_task(resume_interrupted(app))
    try:
        await idle()
    finally:
        SHUTTING_DOWN[0] = True  # running downloads now keep their partial files (see transfer())
        resumer.cancel()
        saver.cancel()
        pinger.cancel()
        await store.flush()
        await log_to_channel(app, status_text(False, me.username))
        await app.stop()
        await http.close()
        await web_runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
