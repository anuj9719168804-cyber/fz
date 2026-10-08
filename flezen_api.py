"""Flezen resolver: link detection + API client (async/aiohttp), ported from the fbot repo (diskwala.py).

Flow for a `https://flezen.com/s/<id>` link:
  1. POST {"url": share_url} to Flezen's public extract API (https://playflezen.com/api/extract), retried on
     429 / 5xx / network errors  ->  {"success": true, "data": {"downloadUrl", "streamUrl", "fileName", "sizeBytes", "thumbnail"}}
  2. If the API fails, the flezen.com share page is checked so the user gets a clear "deleted / not found" message
     instead of a generic API error.
  3. If no size came back, the true file size is probed (HEAD, then a 1-byte Range request).

No Flezen account / cookie is needed for this path.
"""
import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import aiohttp

API_URL = os.getenv("FLEZEN_API", "https://playflezen.com/api/extract").strip()
API_RETRIES = max(1, int(os.getenv("API_RETRIES", "3")))
API_TIMEOUT = max(10, int(os.getenv("API_TIMEOUT", "30")))  # seconds per attempt
# upper bound for one whole resolve (all retries + page check + size probe); callers wrap fetch_flezen in wait_for(this)
RESOLVE_TIMEOUT = API_RETRIES * (API_TIMEOUT + 10) + 60

PAGE_BASE = os.getenv("FLEZEN_PAGE_BASE", "https://flezen.com").strip().rstrip("/")
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0 Safari/537.36")
API_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "*/*",
    "User-Agent": UA,
    "Origin": "https://playflezen.com",
    "Referer": "https://playflezen.com/",
}
DL_REFERER = "https://flezen.com/"  # the CDN checks it (same as fbot's stream proxy)

FLEZEN_HOSTS = ("flezen.com", "playflezen.com")
VIDEO_EXTS = {"mp4", "mkv", "mov", "avi", "webm", "m4v", "ts", "flv", "3gp", "wmv", "m2ts", "vob"}
_KNOWN_EXTS = VIDEO_EXTS | {
    "mp3", "flac", "aac", "ogg", "m4a", "wav", "opus", "wma", "alac", "aiff",
    "jpg", "jpeg", "png", "gif", "webp", "bmp", "tiff", "heic", "avif",
    "pdf", "doc", "docx", "ppt", "pptx", "xls", "xlsx", "txt", "zip", "rar", "7z", "tar", "gz",
}

# flezen links, with or without scheme / www.   e.g. flezen.com/s/<id>, flezen.com/share/<id>, flezen.com/<id>
_URL_RE = re.compile(r"(?:https?://)?(?:[a-z0-9-]+\.)*(?:play)?flezen\.[a-z]{2,}/[^\s<>\"']+", re.I)
_PREFIXES = {"s", "share", "f", "v", "d"}
_RESERVED = {"user", "login", "register", "signup", "files", "api", "terms", "privacy", "dmca", "contact", "about",
             "upload", "dashboard", "account", "settings", "logout", "help", "faq", "pricing", "premium"}
_ID_RE = re.compile(r"^[A-Za-z0-9_-]{3,64}$")

log = logging.getLogger("flezen_api")


class FlezenError(Exception):
    pass


class FlezenGone(FlezenError):
    """The share link does not exist / was deleted by the uploader (retrying is pointless)."""


@dataclass
class FZFile:
    name: str
    size: int
    download_url: Optional[str]
    stream_url: Optional[str] = None
    m3u8_url: Optional[str] = None
    thumb: Optional[str] = None
    is_dir: bool = False
    path: str = ""
    source: str = ""          # the flezen share url (lets the bot ask the API for a fresh link - signed links expire)
    fallback: bool = False    # kept for bot.py compatibility (Flezen has no GET-stream fallback)
    duration: int = 0         # seconds, when the API provides it
    ctime: int = 0
    resolved: float = field(default_factory=time.time)  # when the link was resolved (links expire after ~1h)

    @property
    def is_video(self) -> bool:
        return self.name.lower().rsplit(".", 1)[-1] in VIDEO_EXTS


@dataclass
class FZResult:
    title: str = ""
    files: List[FZFile] = field(default_factory=list)


# ------------------------------------------------------------ link detection --
def is_flezen_host(host: str) -> bool:
    host = (host or "").lower()
    return any(host == h or host.endswith("." + h) for h in FLEZEN_HOSTS) or bool(re.search(r"(^|\.)(play)?flezen\.[a-z]{2,}$", host))


def share_id(url: str) -> Optional[str]:
    """The share id of a flezen url: /s/<id>, /share/<id>, /f/<id>, /v/<id>, /d/<id> or /<id>. None if it is not a share link."""
    segs = [s for s in urlparse(url).path.split("/") if s]
    if not segs:
        return None
    if segs[0].lower() in _PREFIXES:
        cand = segs[1] if len(segs) > 1 else ""
    elif len(segs) == 1 and segs[0].lower() not in _RESERVED:
        cand = segs[0]
    else:
        return None
    return cand if _ID_RE.match(cand) else None


def extract_urls(text: Optional[str]) -> List[str]:
    """All de-duplicated flezen share links in `text`, in order of appearance (scheme-less links get https://)."""
    seen, out = set(), []
    for m in _URL_RE.finditer(text or ""):
        url = m.group(0).rstrip(").,;:!]>'\"")
        if not url.lower().startswith("http"):
            url = "https://" + url
        if not is_flezen_host(urlparse(url).hostname or ""):
            continue
        sid = share_id(url)
        if sid and sid not in seen:
            seen.add(sid)
            out.append(url)
    return out


def extract_flezen_url(text: Optional[str]) -> Optional[str]:
    urls = extract_urls(text)
    return urls[0] if urls else None


def fallback_link(share_url: str) -> str:
    """vidbunker's worker had a plain GET stream endpoint; Flezen has none -> empty (bot.py skips empty links)."""
    return ""


# ------------------------------------------------------------------ helpers --
def _clean_name(name: Optional[str], fallback: str) -> str:
    name = (name or "").strip().replace("/", "_").replace("\\", "_")
    name = re.sub(r'[<>:"|?*\x00-\x1f]', "", name).strip(". ")
    return name or fallback


def _with_ext(name: str) -> str:
    """Same rule as fbot: a name without a known extension gets .mp4 so Telegram treats it as a video."""
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    return name if ext in _KNOWN_EXTS else name + ".mp4"


def _to_int(v) -> int:
    try:
        return int(float(str(v).strip()))
    except Exception:
        return 0


# ----------------------------------------------------------- size probing --
async def probe_size(session: aiohttp.ClientSession, link: str, headers: Optional[dict] = None) -> Tuple[int, bool]:
    """-> (total_size, accepts_ranges). total_size is 0 when unknown."""
    headers = headers or {"User-Agent": UA, "Referer": DL_REFERER}
    total, accepts = 0, False
    try:
        async with session.head(link, headers=headers, allow_redirects=True,
                                timeout=aiohttp.ClientTimeout(total=30)) as r:
            ctype = (r.headers.get("Content-Type") or "").lower()
            cl = r.headers.get("Content-Length")
            if r.status < 400 and cl and cl.isdigit() and "text/html" not in ctype and "json" not in ctype:
                total = int(cl)
            accepts = (r.headers.get("Accept-Ranges") or "").lower() == "bytes"
    except Exception:
        pass
    if not total or not accepts:
        try:
            async with session.get(link, headers={**headers, "Range": "bytes=0-0"}, allow_redirects=True,
                                   timeout=aiohttp.ClientTimeout(total=30)) as r:
                ctype = (r.headers.get("Content-Type") or "").lower()
                if r.status == 206 and "text/html" not in ctype and "json" not in ctype:
                    accepts = True
                    tail = (r.headers.get("Content-Range") or "").rsplit("/", 1)[-1]
                    if tail.isdigit():
                        total = int(tail)
        except Exception:
            pass
    return total, accepts


# --------------------------------------------------------------- resolving --
_GONE_HINTS = ("not found", "does not exist", "deleted", "removed", "expired", "invalid link", "no such file")


async def _post_api(share_url: str, session: aiohttp.ClientSession) -> dict:
    """POST to Flezen's extract API with retries. Returns the `data` dict (always has a downloadUrl or streamUrl)."""
    last: Exception = FlezenError("unknown error")
    for attempt in range(API_RETRIES):
        try:
            async with session.post(API_URL, json={"url": share_url}, headers=API_HEADERS,
                                    timeout=aiohttp.ClientTimeout(total=API_TIMEOUT, connect=15)) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    payload = data.get("data") if isinstance(data, dict) else None
                    if isinstance(data, dict) and data.get("success") and isinstance(payload, dict):
                        if payload.get("downloadUrl") or payload.get("streamUrl"):
                            return payload
                        last = FlezenError("API did not return a download URL")
                    else:
                        msg = str((data or {}).get("error") or (data or {}).get("message") or "extraction failed") if isinstance(data, dict) else "invalid reply"
                        if any(h in msg.lower() for h in _GONE_HINTS):
                            raise FlezenGone(msg)
                        last = FlezenError(f"API said: {msg[:200]}")
                elif r.status in (429, 500, 502, 503, 504):
                    last = FlezenError(f"transient status {r.status}")
                else:
                    last = FlezenError(f"API status {r.status}: {(await r.text())[:200]}")
                    break  # not worth retrying (bad link, blocked, ...)
        except FlezenGone:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:  # ValueError = non-JSON body
            last = e if str(e) else FlezenError(e.__class__.__name__)
        if attempt < API_RETRIES - 1:
            await asyncio.sleep(min(2 ** attempt, 8))
    raise FlezenError(str(last) or last.__class__.__name__)


async def _page_check(share_url: str, session: aiohttp.ClientSession) -> None:
    """Raise FlezenGone when the flezen.com share page itself says the file is gone. Returns quietly otherwise."""
    sid = share_id(share_url)
    if not sid:
        return
    try:
        async with session.get(f"{PAGE_BASE}/s/{sid}", headers={"User-Agent": UA, "Referer": PAGE_BASE + "/"},
                               allow_redirects=True, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 404:
                raise FlezenGone("This Flezen link does not exist or has been deleted by the uploader.")
            if r.status != 200:
                return
            page = await r.text()
    except FlezenGone:
        raise
    except Exception:
        return
    low = page.lower()
    if "can't find this file" in low or "file not found" in low or (not re.search(r"<h1[^>]*>", page) and "data-bytes" not in page):
        raise FlezenGone("This Flezen link does not exist or has been deleted by the uploader.")


def _build_file(url: str, payload: dict, download_url: str, stream_url: Optional[str] = None) -> FZFile:
    sid = share_id(url) or "video"
    name = _with_ext(_clean_name(payload.get("fileName") or payload.get("filename") or payload.get("name"), f"flezen_{sid}.mp4"))
    thumb = payload.get("thumbnail") or payload.get("thumb")
    return FZFile(
        name=name,
        size=_to_int(payload.get("sizeBytes") or payload.get("size_bytes") or payload.get("size") or 0),
        download_url=download_url,
        stream_url=stream_url or download_url,
        thumb=thumb.strip() if isinstance(thumb, str) and thumb.strip().startswith("http") else None,
        source=url,
        duration=_to_int(payload.get("duration") or payload.get("durationSeconds") or payload.get("duration_seconds")),
    )


async def resolve_candidates(url: str, session: aiohttp.ClientSession, timeout: float = 15.0) -> List[FZFile]:
    """Every working link Flezen gives for this share (the download url and, when different, the stream url) as FZFiles.
    Used by the stream race and by RANK_FIRST. Never raises; a slow/failed API simply returns []."""
    try:
        payload = await asyncio.wait_for(_post_api(url, session), timeout=timeout)
    except Exception as e:
        log.info("flezen candidates failed for %s: %s", url, str(e) or e.__class__.__name__)
        return []
    dl = payload.get("downloadUrl") or payload.get("streamUrl")
    st = payload.get("streamUrl") or dl
    out = [_build_file(url, payload, dl, st)]
    if st and st != dl:
        out.append(_build_file(url, payload, st, st))
    return out


async def fetch_flezen(url: str, session: aiohttp.ClientSession, probe: bool = True) -> FZResult:
    """Resolve a flezen share link into a one-file FZResult. Raises FlezenError with the real reason."""
    if not is_flezen_host(urlparse(url).hostname or "") or not share_id(url):
        raise FlezenError(f"not a Flezen share link: {url}")
    try:
        payload = await _post_api(url, session)
    except FlezenGone:
        raise
    except FlezenError as e:
        await _page_check(url, session)  # raises FlezenGone when the page itself says the file was deleted
        raise FlezenError(f"could not resolve {url}: {e}")
    dl = payload.get("downloadUrl") or payload.get("streamUrl")
    st = payload.get("streamUrl") or dl
    f = _build_file(url, payload, dl, st)
    log.info("Flezen API resolved: %s (%s bytes) -> %s", f.name, f.size, dl[:100])
    if probe and not f.size:  # best effort - the download still works without a known size
        try:
            f.size, _ = await asyncio.wait_for(probe_size(session, dl), timeout=25)
        except Exception:
            pass
    return FZResult(title=f.name, files=[f])
