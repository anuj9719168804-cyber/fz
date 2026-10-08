"""Tiny persistence layer: JSON file by default, MongoDB when MONGO_URI is set (survives redeploys)."""
import asyncio
import copy
import json
import os
import time
from datetime import datetime, timezone


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class Store:
    def __init__(self, path: str = "bot_data.json", mongo_uri: str = "", mongo_db: str = "flezenbot"):
        self.path = path
        self.mongo = None
        self.cache_mongo = None           # file cache lives in its own collection (a single doc would hit Mongo's 16 MB cap)
        self.cache_path = os.path.splitext(path)[0] + "_filecache.json"
        self.cache = {}                   # JSON-mode file cache: "{link}::{key}" -> entry
        self._cache_dirty = False
        if mongo_uri:
            try:
                from pymongo import MongoClient  # imported lazily: only needed with Mongo
                cli = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
                cli.admin.command("ping")
                self.mongo = cli[mongo_db]["state"]
                self.cache_mongo = cli[mongo_db]["file_cache"]
            except Exception as e:  # unreachable / bad URI / pymongo missing -> keep running on the JSON files
                print(f"[store] MongoDB unavailable ({e.__class__.__name__}: {e}) - using JSON file {path}")
                self.mongo = self.cache_mongo = None
        self.data = {"users": {}, "banned": [], "total_downloads": 0, "total_bytes": 0, "channels": [], "resume": {}}
        self._dirty = False
        self._lock = asyncio.Lock()
        self._load()

    # ---- persistence -------------------------------------------------------
    def _load(self):
        try:
            if self.mongo is not None:
                doc = self.mongo.find_one({"_id": "main"})
                if doc:
                    doc.pop("_id")
                    self.data.update(doc)
            elif os.path.exists(self.path):
                with open(self.path, encoding="utf-8") as f:
                    self.data.update(json.load(f))
            if self.cache_mongo is None and os.path.exists(self.cache_path):
                with open(self.cache_path, encoding="utf-8") as f:
                    self.cache = json.load(f)
        except Exception as e:  # corrupt file must not stop the bot
            print(f"[store] load failed: {e}")

    def _write(self, data: dict):
        if self.mongo is not None:
            self.mongo.replace_one({"_id": "main"}, {"_id": "main", **data}, upsert=True)
        else:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f)
            os.replace(tmp, self.path)

    def _write_cache(self, cache: dict):
        tmp = self.cache_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cache, f)
        os.replace(tmp, self.cache_path)

    async def flush(self):
        async with self._lock:
            if self._cache_dirty and self.cache_mongo is None:
                self._cache_dirty = False
                try:
                    await asyncio.to_thread(self._write_cache, copy.deepcopy(self.cache))  # snapshot: the loop keeps mutating the live dict
                except Exception as e:
                    self._cache_dirty = True
                    print(f"[store] cache save failed: {e}")
            if not self._dirty:
                return
            self._dirty = False
            try:
                await asyncio.to_thread(self._write, copy.deepcopy(self.data))
            except Exception as e:
                self._dirty = True
                print(f"[store] save failed: {e}")

    async def autosave_loop(self, every: int = 20):
        while True:
            await asyncio.sleep(every)
            await self.flush()

    # ---- interrupted downloads (resumed automatically after a restart) -----
    def resume_put(self, key: str, entry: dict):
        self.data.setdefault("resume", {})[key] = entry
        self._dirty = True

    def resume_del(self, key: str):
        if (self.data.get("resume") or {}).pop(key, None) is not None:
            self._dirty = True

    def resume_all(self) -> dict:
        return dict(self.data.get("resume") or {})

    # ---- users -------------------------------------------------------------
    def add_user(self, uid: int, name: str) -> bool:
        """Returns True if the user is new."""
        k = str(uid)
        new = k not in self.data["users"]
        if new:
            self.data["users"][k] = {"name": name, "joined": int(time.time()), "dl": 0, "day": _today(), "today": 0}
            self._dirty = True
        return new

    def user_ids(self):
        return [int(k) for k in self.data["users"]]

    def is_banned(self, uid: int) -> bool:
        return uid in self.data["banned"]

    def ban(self, uid: int, on: bool = True):
        b = self.data["banned"]
        if on and uid not in b:
            b.append(uid)
        elif not on and uid in b:
            b.remove(uid)
        self._dirty = True

    # ---- daily limit / stats ----------------------------------------------
    def downloads_today(self, uid: int) -> int:
        u = self.data["users"].get(str(uid))
        if not u:
            return 0
        if u.get("day") != _today():
            u["day"], u["today"] = _today(), 0
        return u["today"]

    def record_download(self, uid: int, size: int):
        self.downloads_today(uid)  # rolls the day over
        u = self.data["users"].setdefault(str(uid), {"name": "", "joined": int(time.time()), "dl": 0, "day": _today(), "today": 0})
        u["dl"] += 1
        u["today"] += 1
        self.data["total_downloads"] += 1
        self.data["total_bytes"] += size
        self._dirty = True

    # ---- premium -------------------------------------------------------------
    # users[uid]["premium_until"]: 0/missing = free, -1 = lifetime, else unix expiry time
    def premium_info(self, uid: int) -> dict:
        u = self.data["users"].get(str(uid)) or {}
        until = int(u.get("premium_until") or 0)
        if until == -1:
            return {"is_premium": True, "lifetime": True, "expires_at": None, "days_left": None}
        if until > time.time():
            return {"is_premium": True, "lifetime": False, "expires_at": until,
                    "days_left": int((until - time.time()) // 86400) + 1}
        return {"is_premium": False, "lifetime": False, "expires_at": None, "days_left": 0}

    def is_premium(self, uid: int) -> bool:
        return self.premium_info(uid)["is_premium"]

    def add_premium(self, uid: int, days: int = 0, name: str = "") -> dict:
        """days=0 -> lifetime. Adding days to an active plan extends it. Returns the new premium_info."""
        u = self.data["users"].setdefault(str(uid), {"name": name, "joined": int(time.time()), "dl": 0, "day": _today(), "today": 0})
        cur = int(u.get("premium_until") or 0)
        if days <= 0 or cur == -1:
            u["premium_until"] = -1
            u["plan_days"] = 0  # 0 = lifetime tier
        else:
            active = cur > time.time()
            u["premium_until"] = max(cur, int(time.time())) + days * 86400
            # plan_days = biggest plan bought while premium is active (decides the parallel-download tier)
            u["plan_days"] = max(int(u.get("plan_days") or 0) if active else 0, days)
        self._dirty = True
        return self.premium_info(uid)

    def remove_premium(self, uid: int) -> bool:
        u = self.data["users"].get(str(uid))
        had = bool(u and u.get("premium_until"))
        if u is not None:
            u["premium_until"] = 0
            u.pop("plan_days", None)
            self._dirty = True
        return had

    def premium_ids(self):
        return [int(k) for k in self.data["users"] if self.premium_info(int(k))["is_premium"]]

    def remove_user(self, uid: int):
        """Drop a user who blocked the bot / deleted their account (used by /broadcast)."""
        if self.data["users"].pop(str(uid), None) is not None:
            self._dirty = True

    def banned_count(self) -> int:
        return len(self.data["banned"])


    # ---- linked backup channels (admin: /set_channel_id, /del_channel_id) -----
    def get_channels(self) -> list:
        return list(self.data.get("channels", []))

    def add_channel(self, cid: int) -> bool:
        """True if newly linked, False if it was already linked."""
        ch = self.data.setdefault("channels", [])
        if cid in ch:
            return False
        ch.append(cid)
        self._dirty = True
        return True

    def remove_channel(self, cid: int) -> bool:
        ch = self.data.setdefault("channels", [])
        if cid not in ch:
            return False
        ch.remove(cid)
        self._dirty = True
        return True

    def remove_all_channels(self) -> int:
        n = len(self.data.get("channels", []))
        self.data["channels"] = []
        self._dirty = True
        return n

    # ---- referrals ---------------------------------------------------------------
    # users[uid]: referred_by, referral_count, referral_rewards (list of claimed thresholds)
    def _user(self, uid: int) -> dict:
        return self.data["users"].setdefault(str(uid), {"name": "", "joined": int(time.time()), "dl": 0, "day": _today(), "today": 0})

    def set_referrer(self, uid: int, referrer: int) -> bool:
        """Only ever set once per user and never for self-referral. True if this call set it."""
        if uid == referrer:
            return False
        u = self._user(uid)
        if u.get("referred_by"):
            return False
        u["referred_by"] = referrer
        self._dirty = True
        return True

    def add_referral(self, referrer: int) -> int:
        u = self._user(referrer)
        u["referral_count"] = int(u.get("referral_count", 0)) + 1
        self._dirty = True
        return u["referral_count"]

    def referral_count(self, uid: int) -> int:
        return int((self.data["users"].get(str(uid)) or {}).get("referral_count", 0))

    def rewards_claimed(self, uid: int) -> list:
        return list((self.data["users"].get(str(uid)) or {}).get("referral_rewards", []))

    def mark_reward(self, uid: int, threshold: int):
        u = self._user(uid)
        r = u.setdefault("referral_rewards", [])
        if threshold not in r:
            r.append(threshold)
            self._dirty = True


    # ---- per-user custom caption / thumbnail / dump chat (fbot: /set_caption, /set_thumb, /setchat) ----
    def get_caption(self, uid: int):
        return (self.data["users"].get(str(uid)) or {}).get("caption") or None

    def set_caption(self, uid: int, text: str):
        self._user(uid)["caption"] = text
        self._dirty = True

    def del_caption(self, uid: int) -> bool:
        u = self.data["users"].get(str(uid))
        if u and u.pop("caption", None):
            self._dirty = True
            return True
        return False

    def get_thumb(self, uid: int):
        return (self.data["users"].get(str(uid)) or {}).get("thumbnail") or None

    def set_thumb(self, uid: int, file_id: str):
        self._user(uid)["thumbnail"] = file_id
        self._dirty = True

    def del_thumb(self, uid: int) -> bool:
        u = self.data["users"].get(str(uid))
        if u and u.pop("thumbnail", None):
            self._dirty = True
            return True
        return False

    def get_dump_chat(self, uid: int):
        return (self.data["users"].get(str(uid)) or {}).get("dump_chat")

    def set_dump_chat(self, uid: int, chat_id):
        """chat_id=None clears it."""
        if chat_id is None:
            u = self.data["users"].get(str(uid))
            if u:
                u.pop("dump_chat", None)
        else:
            self._user(uid)["dump_chat"] = chat_id
        self._dirty = True


    # ---- file cache (fbot: get_cached_file / set_cached_file / delete_cached_file) ----
    # One entry per (share link, file key). Holds the Telegram file_id plus, when CACHE_CHANNEL_ID is set,
    # the location of a copy inside that channel (used with copy_message for a fresh file_reference).
    # Every method swallows its own errors: a broken cache must never break a download.
    @staticmethod
    def _cache_id(link: str, key: str) -> str:
        return f"{link}::{key}"

    async def get_cached_file(self, link: str, key: str):
        cid = self._cache_id(link, key)
        try:
            if self.cache_mongo is not None:
                doc = await asyncio.to_thread(self.cache_mongo.find_one, {"_id": cid})
                if not doc:
                    return None
                doc.pop("_id", None)
                return doc
            ent = self.cache.get(cid)
            return dict(ent) if ent else None
        except Exception as e:
            print(f"[store] cache get failed: {e}")
            return None

    async def set_cached_file(self, link: str, key: str, **fields):
        cid = self._cache_id(link, key)
        doc = {"link": link, "key": key, "cached_at": int(time.time()), **fields}
        try:
            if self.cache_mongo is not None:
                await asyncio.to_thread(self.cache_mongo.replace_one, {"_id": cid}, {"_id": cid, **doc}, True)
            else:
                self.cache[cid] = doc
                self._cache_dirty = True
        except Exception as e:
            print(f"[store] cache set failed: {e}")

    async def delete_cached_file(self, link: str, key: str):
        cid = self._cache_id(link, key)
        try:
            if self.cache_mongo is not None:
                await asyncio.to_thread(self.cache_mongo.delete_one, {"_id": cid})
            elif self.cache.pop(cid, None) is not None:
                self._cache_dirty = True
        except Exception as e:
            print(f"[store] cache delete failed: {e}")

    async def find_cached_by_link(self, link: str) -> list:
        """All cache entries stored for one share link (used to serve a repeat link without calling the API)."""
        try:
            if self.cache_mongo is not None:
                docs = await asyncio.to_thread(lambda: list(self.cache_mongo.find({"link": link})))
                for d in docs:
                    d.pop("_id", None)
                return docs
            return [dict(v) for v in self.cache.values() if v.get("link") == link]
        except Exception as e:
            print(f"[store] cache link lookup failed: {e}")
            return []

    async def cache_count(self) -> int:
        try:
            if self.cache_mongo is not None:
                return await asyncio.to_thread(self.cache_mongo.count_documents, {})
            return len(self.cache)
        except Exception:
            return 0

    async def clear_cache(self) -> int:
        try:
            if self.cache_mongo is not None:
                r = await asyncio.to_thread(self.cache_mongo.delete_many, {})
                return r.deleted_count
            n = len(self.cache)
            self.cache = {}
            self._cache_dirty = True
            return n
        except Exception:
            return 0
