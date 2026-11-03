"""
24x7 Multi-User Telegram Deal-Reposting Worker & Automation Service.

Features:
  1. Aiogram 3 Bot with FSM Login (Phone -> OTP -> 2FA -> StringSession save).
  2. Dynamic Channel Selection UI (Paginated inline keyboard for Source & Target channels).
  3. Multi-Session Forwarding & Tracking Engine (Per-user Cuelinks sub_id conversion).
  4. aiohttp Health-Check Server on PORT (for Render / cloud deployments).
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import os
import re
import tempfile
import time
from dataclasses import dataclass, field
from typing import Optional, Any
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse

import aiohttp
from aiohttp import web
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from dotenv import load_dotenv
from telethon import TelegramClient, events, Button
from telethon.errors import (
    ApiIdInvalidError,
    FloodWaitError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberBannedError,
    PhoneNumberFloodError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession

load_dotenv()

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_ID = int(os.getenv("TELEGRAM_API_ID", "0") or 0)
API_HASH = os.getenv("TELEGRAM_API_HASH", "").strip()
SESSION_STRING = os.getenv("TELETHON_SESSION_STRING", "").strip()
BASE_WEB_URL = os.getenv("BASE_WEB_URL", "https://kamai-three.vercel.app").rstrip("/")
WORKER_API_SECRET = os.getenv("WORKER_API_SECRET", "kamaikaro-tg-worker-secret-2026").strip()
CUELINKS_PUB_ID = os.getenv("CUELINKS_PUB_ID", "282274").strip()
CUELINKS_CHNL_ID = os.getenv("CUELINKS_CHNL_ID", "332862").strip()
SYNC_INTERVAL = int(os.getenv("CONFIG_SYNC_INTERVAL", "60"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
PORT = int(os.getenv("PORT", "8080"))
AMAZON_ASSOCIATE_TAG = os.getenv("AMAZON_ASSOCIATE_TAG", "kamaikaro02-21").strip() or "kamaikaro02-21"
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID", "00000000-0000-0000-0000-000000000000").strip() or "00000000-0000-0000-0000-000000000000"
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", os.getenv("SUPABASE_KEY", "")).strip()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
log = logging.getLogger("worker")
logger = log

VERIFY_URL = f"{BASE_WEB_URL}/api/telegram/verify-sync"
CONFIG_URL = f"{BASE_WEB_URL}/api/telegram/user-config"
SAVE_SESSION_URL = f"{BASE_WEB_URL}/api/telegram/save-session"
CHANNELS_URL = f"{BASE_WEB_URL}/api/telegram/channels"

SHORTENER_DOMAINS = (
    "amazn.lt", "amzn.to", "amzn.in", "amzn.cl", "amzn.eu", "a.co",
    "fkrt.it", "fkrt.co", "fkrt.cc", "fkt.in", "dl.flipkart.com",
    "myntr.it", "ajio.page.link", "ajiio.in",
    "bit.ly", "bitly.com", "tinyurl.com", "cutt.ly", "t.co", "rb.gy",
    "shorturl.at", "shorturl.ac", "is.gd", "t.ly", "v.gd", "buff.ly",
    "ow.ly", "clk.im", "trib.al", "ift.tt", "goo.gl", "cleanuri.com",
    "snip.ly", "rebrand.ly", "bl.ink", "dub.co", "short.io",
)

STORE_DOMAINS = (
    "amazon.in", "amazon.com", "flipkart.com", "myntra.com", "ajio.com",
    "meesho.com", "tatacliq.com", "nykaa.com", "jiomart.com", "shopsy.in",
    "boat-lifestyle.com", "gonoise.com", "snapdeal.com", "croma.com",
    "reliancedigital.in", "firstcry.com", "pepperfry.com", "swiggy.com",
    "zomato.com", "zeptonow.com", "blinkit.com",
)

EXCLUDED_DOMAINS = (
    "t.me", "telegram.me", "telegram.dog",
    "kamaikaro.in", "kamaikaro.com",
    "linksredirect.com", "cuelinks.com",
)

SUPPORTED_DOMAINS = {
    "amazon": ("amazon.in", "amazon.com", "amazn.lt", "amzn.to", "amzn.in", "a.co", "amzn.cl", "amzn.eu"),
    "flipkart": ("flipkart.com", "fkrt.it", "fkrt.co", "fkrt.cc", "fkt.in", "dl.flipkart.com"),
    "myntra": ("myntra.com", "myntr.it"),
    "ajio": ("ajio.com", "ajiio.in", "ajio.page.link"),
    "stores": STORE_DOMAINS,
    "shorteners": SHORTENER_DOMAINS,
}
ALL_DOMAINS = tuple(set(d for ds in SUPPORTED_DOMAINS.values() for d in ds))
URL_RE = re.compile(r"https?://[^\s<>\"')\]]+", re.IGNORECASE)
TRAILING_PUNCT = ".,;:!?)]}>'\""

CAPTION_LIMIT = 1024
MESSAGE_LIMIT = 4096

# --------------------------------------------------------------------------- #
# Strict Deduplication Cache (2 Hours TTL)
# --------------------------------------------------------------------------- #
class TTLCache:
    """Thread/async-safe in-memory cache with TTL expiration (default 2 hours = 7200s) and maxsize."""
    def __init__(self, maxsize: int = 1000, ttl: float = 7200.0) -> None:
        self.maxsize = maxsize
        self.ttl = ttl
        self._cache: dict[str, float] = {}

    def _cleanup(self, now: float) -> None:
        expired = [k for k, exp in self._cache.items() if now > exp]
        for k in expired:
            self._cache.pop(k, None)
        if len(self._cache) > self.maxsize:
            excess = len(self._cache) - self.maxsize
            for k in list(self._cache.keys())[:excess]:
                self._cache.pop(k, None)

    def contains(self, key: str) -> bool:
        now = time.time()
        exp = self._cache.get(key)
        if exp is None:
            return False
        if now > exp:
            self._cache.pop(key, None)
            return False
        return True

    def add(self, key: str) -> None:
        now = time.time()
        self._cleanup(now)
        self._cache[key] = now + self.ttl

    def __contains__(self, key: str) -> bool:
        return self.contains(key)

    def __len__(self) -> int:
        return len(self._cache)


# 2-Hour TTL deal deduplication cache
recent_deal_cache = TTLCache(maxsize=1000, ttl=7200.0)
_seen_posts = recent_deal_cache._cache  # backwards-compatible mapping


def normalize_deal_target(url: str) -> str:
    """Extract canonical normalized deal identity from a destination URL:
    - Amazon: amazon:asin:B0XXXXXXXX
    - Flipkart: flipkart:pid:XXXX or flipkart:path
    - Other stores: host:path without query tracking parameters
    """
    clean = clean_url_formatting(url)
    asin = extract_amazon_asin(clean)
    if asin:
        return f"amazon:asin:{asin.upper()}"

    p = urlparse(clean)
    host = p.netloc.lower().split(":")[0]

    if any(h in host for h in ("flipkart.com", "fkrt.it", "fkrt.co")):
        q = dict(parse_qsl(p.query))
        if "pid" in q:
            return f"flipkart:pid:{q['pid']}"
        clean_path = re.sub(r"/p/[a-zA-Z0-9]+", "", p.path).strip("/")
        return f"flipkart:{clean_path}"

    clean_path = p.path.rstrip("/")
    return f"{host}:{clean_path}"


def get_deal_fingerprint(identifier: str) -> str:
    """Generate SHA256 hex digest fingerprint for a normalized identifier."""
    norm = identifier.strip().lower()
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()

# In-memory tracking for pending logins: keyed by telegram_user_id (int and str)
login_sessions: dict[Any, dict[str, Any]] = {}
_pending_logins = login_sessions  # backwards-compatible alias


def get_login_session(user_id: int | str) -> Optional[dict[str, Any]]:
    now = time.time()
    sess = (
        login_sessions.get(user_id)
        or login_sessions.get(int(user_id) if str(user_id).lstrip("-").isdigit() else None)
        or login_sessions.get(str(user_id))
    )
    if sess:
        # Do not expire for at least 10 minutes (600 seconds)
        if now - sess.get("timestamp", now) > 600:
            client = sess.get("client")
            if client:
                try:
                    asyncio.create_task(client.disconnect())
                except Exception:
                    pass
            pop_login_session(user_id)
            return None
        return sess
    return None


def set_login_session(user_id: int | str, data: dict[str, Any]) -> None:
    data["timestamp"] = time.time()
    login_sessions[user_id] = data
    if isinstance(user_id, int):
        login_sessions[str(user_id)] = data
    elif str(user_id).lstrip("-").isdigit():
        login_sessions[int(user_id)] = data


def pop_login_session(user_id: int | str) -> Optional[dict[str, Any]]:
    sess = login_sessions.pop(user_id, None)
    if isinstance(user_id, int):
        login_sessions.pop(str(user_id), None)
    elif str(user_id).lstrip("-").isdigit():
        login_sessions.pop(int(user_id), None)
    return sess

# In-memory drafts for channel selection: tg_user_id -> dict
_channel_drafts: dict[int, dict[str, set[str]]] = {}
_dialogs_cache: dict[int, tuple[float, list[dict[str, Any]]]] = {}

# In-memory mapping of telegram_user_id -> verified user_id & exact tracking_sub_id & amazon_tag
_telegram_to_user_id: dict[str, str] = {}
_telegram_to_sub_id: dict[str, str] = {}
_telegram_to_amazon_tag: dict[str, str] = {}


# --------------------------------------------------------------------------- #
# Shared State: Cached User Configs
# --------------------------------------------------------------------------- #
@dataclass
class UserConfig:
    user_id: str
    telegram_user_id: str = ""
    session_string: str = ""
    target_channel_id: Optional[int | str] = None
    target_channel_name: str = ""
    target_channels: list[Any] = field(default_factory=list)
    source_channels: list[str] = field(default_factory=list)
    source_channels_raw: list[Any] = field(default_factory=list)
    tracking_sub_id: str = ""
    amazon_tag: str = ""
    is_active: bool = True


class ConfigStore:
    def __init__(self) -> None:
        self.users: dict[str, UserConfig] = {}
        self.loaded = asyncio.Event()

    def replace(self, users: dict[str, UserConfig]) -> None:
        self.users = users
        self.loaded.set()

    def active_for_source(self, source_keys: set[str]) -> list[UserConfig]:
        out = []
        for u in self.users.values():
            if not (u.is_active and (u.target_channel_id or u.target_channels)):
                continue
            user_sources = set()
            for c in u.source_channels:
                norm = norm_channel(c)
                if norm:
                    user_sources.add(norm)
                    if re.fullmatch(r"-?\d+", norm):
                        raw_id = norm.lstrip("-")
                        clean_id = re.sub(r"^100", "", raw_id)
                        user_sources.update({norm, raw_id, clean_id, f"-100{clean_id}"})
            if user_sources & source_keys:
                out.append(u)
        return out

    def get_by_telegram_id(self, tg_id: int | str) -> Optional[UserConfig]:
        s_id = str(tg_id).strip()
        for u in self.users.values():
            if str(u.telegram_user_id).strip() == s_id:
                return u
        return None

    def remove_by_telegram_id(self, tg_id: int | str) -> Optional[UserConfig]:
        s_id = str(tg_id).strip()
        found_key = None
        for uid, u in list(self.users.items()):
            if str(u.telegram_user_id).strip() == s_id:
                found_key = uid
                break
        if found_key:
            return self.users.pop(found_key, None)
        return None

    def all_sources(self) -> set[str]:
        s: set[str] = set()
        for u in self.users.values():
            if u.is_active:
                for c in u.source_channels:
                    norm = norm_channel(c)
                    if norm:
                        s.add(norm)
                        if re.fullmatch(r"-?\d+", norm):
                            raw_id = norm.lstrip("-")
                            clean_id = re.sub(r"^100", "", raw_id)
                            s.update({norm, raw_id, clean_id, f"-100{clean_id}"})
        s.discard("")
        return s


store = ConfigStore()


def norm_channel(c: Any) -> str:
    """Normalise a channel reference (str, int, dict) to a lowercase key."""
    if isinstance(c, dict):
        c = c.get("username") or c.get("id") or ""
    c = str(c or "").strip()
    c = re.sub(r"^https?://t\.me/", "", c, flags=re.I)
    c = c.lstrip("@").split("/")[0].split("?")[0]
    return c.lower()


def parse_target(v: Any) -> Optional[int | str]:
    if v is None or v == "":
        return None
    s = str(v).strip()
    if re.fullmatch(r"-?\d+", s):
        return int(s)
    return "@" + s.lstrip("@") if not s.startswith("http") else s


# --------------------------------------------------------------------------- #
# HTTP API Helpers
# --------------------------------------------------------------------------- #
_session: Optional[aiohttp.ClientSession] = None


async def http() -> aiohttp.ClientSession:
    global _session
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None

    if _session is not None and (_session.closed or getattr(_session, "_loop", None) is not current_loop):
        _session = None

    if _session is None:
        headers = {
            "User-Agent": "kamai-telegram-worker/2.0",
            "x-worker-secret": WORKER_API_SECRET,
        }
        _session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT), headers=headers
        )
    return _session


async def api_verify_sync(user_id: str, telegram_user_id: int | str, telegram_username: Optional[str]) -> dict:
    """POST /api/telegram/verify-sync"""
    s = await http()
    payload = {
        "userId": user_id,
        "authPayload": f"AUTH_{user_id}",
        "telegramUserId": str(telegram_user_id),
        "telegramUsername": telegram_username or "",
    }
    try:
        async with s.post(VERIFY_URL, json=payload) as r:
            try:
                data = await r.json(content_type=None)
            except Exception:
                data = {"message": await r.text()}
            if not isinstance(data, dict):
                data = {"message": str(data)}
            data["_status"] = r.status
            return data
    except Exception as e:
        log.warning("api_verify_sync error: %s", e)
        return {"error": str(e), "_status": 500}


async def api_fetch_configs(telegram_id: Optional[int | str] = None, user_id: Optional[str] = None) -> Optional[list[dict]]:
    """GET /api/telegram/user-config. Returns list of configs, or None on failure."""
    s = await http()
    params: dict[str, str] = {}
    if telegram_id is not None:
        params["telegramId"] = str(telegram_id)
    elif user_id is not None:
        params["userId"] = str(user_id)
    else:
        params["activeOnly"] = "true"

    try:
        timeout = aiohttp.ClientTimeout(total=8.0, connect=3.0)
        async with s.get(CONFIG_URL, params=params, timeout=timeout) as r:
            r.raise_for_status()
            data = await r.json(content_type=None)
            configs = data.get("configs") if isinstance(data, dict) else None
            return configs if isinstance(configs, list) else []
    except Exception as e:
        log.warning("api_fetch_configs error: %s", e)
        return None


async def api_save_session(session_string: str, telegram_user_id: int | str, telegram_username: Optional[str], user_id: Optional[str] = None) -> dict:
    """POST /api/telegram/save-session"""
    s = await http()
    payload = {
        "sessionString": session_string,
        "telegramUserId": str(telegram_user_id),
        "telegramUsername": telegram_username or "",
        "isActive": True,
    }
    if user_id:
        payload["userId"] = user_id

    try:
        async with s.post(SAVE_SESSION_URL, json=payload) as r:
            try:
                data = await r.json(content_type=None)
            except Exception:
                data = {"message": await r.text()}
            if not isinstance(data, dict):
                data = {"message": str(data)}
            data["_status"] = r.status
            return data
    except Exception as e:
        log.warning("api_save_session error: %s", e)
        return {"error": str(e), "_status": 500}


async def api_save_channels(
    telegram_user_id: int | str,
    source_channels: list[dict],
    target_channels: list[dict],
    target_channel_id: Optional[str],
    target_channel_name: Optional[str],
    user_id: Optional[str] = None,
) -> dict:
    """POST /api/telegram/channels"""
    s = await http()
    payload: dict[str, Any] = {
        "telegramUserId": str(telegram_user_id),
        "sourceChannels": source_channels,
        "targetChannels": target_channels,
        "targetChannelId": target_channel_id,
        "targetChannelName": target_channel_name,
        "isActive": True,
    }
    if user_id:
        payload["userId"] = str(user_id)
    try:
        async with s.post(CHANNELS_URL, json=payload) as r:
            try:
                data = await r.json(content_type=None)
            except Exception:
                data = {"message": await r.text()}
            if not isinstance(data, dict):
                data = {"message": str(data)}
            data["_status"] = r.status
            return data
    except Exception as e:
        log.warning("api_save_channels error: %s", e)
        return {"error": str(e), "_status": 500}


async def api_save_user_config(payload: dict[str, Any]) -> dict:
    """POST /api/telegram/user-config"""
    s = await http()
    try:
        async with s.post(CONFIG_URL, json=payload) as r:
            try:
                data = await r.json(content_type=None)
            except Exception:
                data = {"_status": r.status}
            return data if isinstance(data, dict) else {"message": str(data)}
    except Exception as e:
        log.warning("api_save_user_config error: %s", e)
        return {"error": str(e), "_status": 500}


def extract_amazon_asin(url: str) -> Optional[str]:
    """Extract 10-character Amazon ASIN from path or query parameters."""
    if not url:
        return None
    # 1. Path pattern: /dp/ASIN, /gp/product/ASIN, /gp/aw/d/ASIN, /product/ASIN, /d/ASIN, /deal/ASIN, /ASIN/ASIN
    m = re.search(r"/(?:dp|product|gp/product|gp/aw/d|d|deal|ASIN)/([a-zA-Z0-9]{10})(?:[/?#&]|$)", url, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    # 2. Query param: pd_rd_i=ASIN, asin=ASIN, creativeASIN=ASIN
    m = re.search(r"[?&](?:pd_rd_i|asin|creativeasin)=([a-zA-Z0-9]{10})(?:[&#]|$)", url, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    # 3. Direct B0-prefixed ASIN segment in path
    m = re.search(r"/(B0[a-zA-Z0-9]{8})(?:[/?#&]|$)", url, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    return None


def clean_amazon_associate_tag(raw_tag: str) -> str:
    if not raw_tag:
        return ""
    tag = raw_tag.strip()
    if tag.lower().startswith("tag="):
        tag = tag[4:].strip()
    elif tag.lower().startswith("tag ="):
        tag = tag[5:].strip()
    tag = re.sub(r"\s+", "", tag)
    return tag


def parse_config(raw: dict) -> Optional[UserConfig]:
    try:
        raw_sources = raw.get("sourceChannels") or []
        if isinstance(raw_sources, str):
            raw_sources = [x for x in re.split(r"[,\s]+", raw_sources) if x]

        source_names: list[str] = []
        for x in raw_sources:
            if isinstance(x, dict):
                uname = x.get("username")
                cid = x.get("id")
                if uname:
                    source_names.append(str(uname))
                if cid:
                    source_names.append(str(cid))
            elif x:
                source_names.append(str(x))

        target_ch_list = raw.get("targetChannels") or []
        primary_target = parse_target(raw.get("targetChannelId"))
        primary_target_name = str(raw.get("targetChannelName") or "")

        if not primary_target and target_ch_list and isinstance(target_ch_list, list):
            first = target_ch_list[0]
            if isinstance(first, dict):
                primary_target = parse_target(first.get("id"))
                primary_target_name = first.get("title") or ""
            else:
                primary_target = parse_target(first)

        parsed_sub_id = str(
            raw.get("trackingSubId")
            or raw.get("subId")
            or (raw.get("user") or {}).get("trackingSubId")
            or (raw.get("user") or {}).get("subId")
            or ""
        )

        parsed_amazon_tag = clean_amazon_associate_tag(str(
            raw.get("customAmazonTag")
            or raw.get("amazonTag")
            or raw.get("amazon_tag")
            or (raw.get("user") or {}).get("customAmazonTag")
            or (raw.get("user") or {}).get("amazonTag")
            or (raw.get("user") or {}).get("amazon_tag")
            or ""
        ))

        return UserConfig(
            user_id=str(raw["userId"]),
            telegram_user_id=str(raw.get("telegramUserId") or ""),
            session_string=str(raw.get("sessionString") or ""),
            target_channel_id=primary_target,
            target_channel_name=primary_target_name,
            target_channels=list(target_ch_list) if isinstance(target_ch_list, list) else [],
            source_channels=source_names,
            source_channels_raw=list(raw_sources) if isinstance(raw_sources, list) else [],
            tracking_sub_id=parsed_sub_id,
            amazon_tag=parsed_amazon_tag,
            is_active=bool(raw.get("isActive", True)),
        )
    except Exception:
        log.exception("Bad config row skipped: %r", raw)
        return None


# --------------------------------------------------------------------------- #
# Link Extraction + Cuelinks Conversion Engine
# --------------------------------------------------------------------------- #
def _host_matches(host: str) -> bool:
    host = host.lower().split(":")[0]
    return any(host == d or host.endswith("." + d) for d in ALL_DOMAINS)


def is_shortened_url(url: str) -> bool:
    try:
        p = urlparse(url)
        host = p.netloc.lower().split(":")[0]
        if not host or any(host == d or host.endswith("." + d) for d in EXCLUDED_DOMAINS):
            return False
        # Known shortener domains (amazn.lt, amzn.to, fkrt.it, bit.ly, etc.)
        if any(host == d or host.endswith("." + d) for d in SHORTENER_DOMAINS):
            return True
        # Generic short link heuristic: short hostname (<=15 chars) with single path segment
        path = p.path.strip("/")
        if len(host) <= 15 and ("/" not in path) and (2 <= len(path) <= 20):
            return True
        return False
    except Exception:
        return False


def is_supported_product_host(host: str) -> bool:
    host = host.lower().split(":")[0]
    return any(host == d or host.endswith("." + d) for d in STORE_DOMAINS)


def is_deal_target_candidate(url: str) -> bool:
    try:
        p = urlparse(url)
        host = p.netloc.lower().split(":")[0]
        if not host or any(host == d or host.endswith("." + d) for d in EXCLUDED_DOMAINS):
            return False
        # Known shortener or store domain
        if any(host == d or host.endswith("." + d) for d in ALL_DOMAINS):
            return True
        # Generic HTTP/HTTPS URL
        return True
    except Exception:
        return False


def extract_deal_urls(text: str) -> list[str]:
    seen, out = set(), []
    for m in URL_RE.finditer(text or ""):
        url = m.group(0).rstrip(TRAILING_PUNCT)
        try:
            host = urlparse(url).netloc.lower().split(":")[0]
        except Exception:
            continue
        if not host or any(host == d or host.endswith("." + d) for d in EXCLUDED_DOMAINS):
            continue
        if is_deal_target_candidate(url) and url not in seen:
            seen.add(url)
            out.append(url)
    return out


def strip_affiliate_params(url: str) -> str:
    """Drop third-party tracking params so only the user's subid earns."""
    try:
        p = urlparse(url)
        drop = {
            "tag", "affid", "aff_id", "affextparam1", "affextparam2",
            "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
            "subid", "cid", "ref", "ref_", "linkcode", "linkid", "ascsubtag",
            "creative", "camp", "creativeasin", "tag_id", "source", "share_tag",
        }
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
             if k.lower() not in drop and not k.lower().startswith("utm_") and not k.lower().startswith("aff_")]
        clean_path = re.sub(r"/ref=[^/?#]+", "", p.path)
        return urlunparse(p._replace(path=clean_path, query=urlencode(q)))
    except Exception:
        return url


_RESOLVED_CACHE: dict[str, str] = {}


async def unshorten_url(url: str) -> str:
    url = clean_url_formatting(url)
    if not url:
        return ""
    if url in _RESOLVED_CACHE:
        return _RESOLVED_CACHE[url]
    if not is_shortened_url(url):
        return url

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    resolved = url

    # Method 1: aiohttp with allow_redirects=True (HEAD first, then GET)
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        timeout = aiohttp.ClientTimeout(total=4.0, connect=1.5, sock_read=2.5)
        async with aiohttp.ClientSession(headers=headers, connector=connector, timeout=timeout) as session:
            try:
                async with session.head(url, allow_redirects=True) as resp:
                    final_url = str(resp.url)
                    if final_url and final_url.startswith("http") and final_url != url:
                        resolved = final_url
            except Exception:
                pass

            if resolved == url:
                async with session.get(url, allow_redirects=True) as resp:
                    final_url = str(resp.url)
                    if final_url and final_url.startswith("http") and final_url != url:
                        resolved = final_url
                    elif "location" in resp.headers:
                        loc = resp.headers["location"]
                        if loc.startswith("http"):
                            resolved = loc
    except Exception as e:
        log.warning("aiohttp unshorten error for %s: %s", url, e)

    _RESOLVED_CACHE[url] = resolved
    if len(_RESOLVED_CACHE) > 5000:
        _RESOLVED_CACHE.clear()
    return resolved


def clean_url_formatting(url: str) -> str:
    """Ensure URL is completely contiguous without spaces, newlines, or splits."""
    if not url:
        return ""
    cleaned = url.strip()
    cleaned = re.sub(r"[\r\n\t\s]+", "", cleaned)
    cleaned = re.sub(r"(https?://)\s+", r"\1", cleaned)
    return cleaned


def fix_split_urls(text: str) -> str:
    """Fix any accidental splits around https:// or http:// in text."""
    if not text:
        return ""
    return re.sub(r"(https?://)[\s\r\n]+([a-zA-Z0-9])", r"\1\2", text)


def to_cuelinks(url: str, sub_id: str) -> str:
    clean = strip_affiliate_params(clean_url_formatting(url))
    encoded_target_url = quote(clean, safe="")
    sub_id_clean = clean_url_formatting(sub_id)
    raw_cuelink = f"https://linksredirect.com/?pub_id={CUELINKS_PUB_ID}&chnl_id={CUELINKS_CHNL_ID}&subid={sub_id_clean}&url={encoded_target_url}"
    return clean_url_formatting(raw_cuelink)


def is_amazon_url(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower().split(":")[0]
        amazon_hosts = (
            "amazon.in", "amazon.com", "amazn.lt", "amzn.to", "amzn.in",
            "a.co", "amzn.cl", "amzn.eu", "amazon.co.uk", "amazon.de"
        )
        return any(host == d or host.endswith("." + d) for d in amazon_hosts)
    except Exception:
        return False


def build_intelligent_amazon_url(
    url: str,
    custom_amazon_tag: Optional[str] = None,
    user_id: Optional[str] = None,
    fallback_url: Optional[str] = None,
) -> str:
    """Intelligent Amazon Link Builder:
    IF customAmazonTag exists and is non-empty:
      Format URL: ${cleanUrl}?tag=${customAmazonTag}
    ELSE:
      Format URL: ${cleanUrl}?tag=${AMAZON_ASSOCIATE_TAG || 'kamaikaro02-21'}&ascsubtag=${userId}
    """
    clean_tag = clean_amazon_associate_tag(custom_amazon_tag or "")
    asin = extract_amazon_asin(url) or (extract_amazon_asin(fallback_url) if fallback_url else None)

    if clean_tag:
        if asin:
            return clean_url_formatting(f"https://www.amazon.in/dp/{asin}?tag={clean_tag}")
        clean = strip_affiliate_params(clean_url_formatting(url))
        p = urlparse(clean)
        domain = "www.amazon.in" if "amazon" in p.netloc.lower() else (p.netloc or "www.amazon.in")
        clean_path = re.sub(r"/ref=[^/?#]+", "", p.path)
        return clean_url_formatting(f"https://{domain}{clean_path}?tag={clean_tag}")

    # Fallback to Master Tag with user tracking ascsubtag
    master_tag = clean_amazon_associate_tag(AMAZON_ASSOCIATE_TAG) or "kamaikaro02-21"
    sub_id = user_id or ADMIN_USER_ID
    if asin:
        return clean_url_formatting(f"https://www.amazon.in/dp/{asin}?tag={master_tag}&ascsubtag={sub_id}")

    clean = strip_affiliate_params(clean_url_formatting(url))
    p = urlparse(clean)
    domain = "www.amazon.in" if "amazon" in p.netloc.lower() else (p.netloc or "www.amazon.in")
    clean_path = re.sub(r"/ref=[^/?#]+", "", p.path)
    return clean_url_formatting(f"https://{domain}{clean_path}?tag={master_tag}&ascsubtag={sub_id}")


def to_amazon_affiliate_url(
    url: str,
    amazon_tag: Optional[str] = None,
    fallback_url: Optional[str] = None,
    user_id: Optional[str] = None,
) -> str:
    """Generate clean direct Amazon affiliate URL using the Intelligent Link Builder."""
    return build_intelligent_amazon_url(
        url=url,
        custom_amazon_tag=amazon_tag,
        user_id=user_id,
        fallback_url=fallback_url,
    )


async def api_shorten_link(
    original_url: str,
    user_id: Optional[str] = None,
    sub_id: Optional[str] = None,
) -> Optional[str]:
    """Call web shortener API: POST https://www.kamaikaro.in/api/links/shorten
    Request body: { "originalUrl": convertedUrl, "userId": user_id, "subId": sub_id }
    Uses 5.5-second timeout.
    Returns clean short link: https://kamaikaro.in/l/{shortCode} or None on failure/non-200.
    """
    clean_orig = clean_url_formatting(original_url)
    if not clean_orig:
        return None

    # Candidate endpoints: primary branded domain first, fallback to BASE_WEB_URL
    endpoints = [
        "https://www.kamaikaro.in/api/links/shorten",
        "https://kamaikaro.in/api/links/shorten",
    ]
    base_endpoint = f"{BASE_WEB_URL}/api/links/shorten"
    if base_endpoint not in endpoints:
        endpoints.append(base_endpoint)

    payload: dict[str, Any] = {"originalUrl": clean_orig}
    if user_id:
        payload["userId"] = str(user_id)
    if sub_id:
        payload["subId"] = str(sub_id)

    async def _request_shorten() -> Optional[str]:
        s = await http()
        timeout = aiohttp.ClientTimeout(total=5.0, connect=2.0, sock_read=3.0)

        for endpoint in endpoints:
            try:
                async with s.post(endpoint, json=payload, timeout=timeout) as resp:
                    if resp.status in (200, 201):
                        data = await resp.json(content_type=None)
                        if isinstance(data, dict):
                            # 1. Direct shortCode / code field
                            code = (
                                data.get("shortCode")
                                or data.get("code")
                                or (data.get("link") or {}).get("shortCode")
                                or (data.get("link") or {}).get("code")
                                or (data.get("data") or {}).get("shortCode")
                                or (data.get("data") or {}).get("code")
                            )
                            if code and isinstance(code, str):
                                clean_code = code.strip().lstrip("/")
                                return clean_url_formatting(f"https://kamaikaro.in/l/{clean_code}")

                            # 2. Direct shortUrl / shortenedUrl / url field
                            short_url = (
                                data.get("shortUrl")
                                or data.get("shortenedUrl")
                                or data.get("url")
                                or (data.get("link") or {}).get("shortUrl")
                                or (data.get("link") or {}).get("url")
                                or (data.get("data") or {}).get("shortUrl")
                                or (data.get("data") or {}).get("url")
                            )
                            if short_url and isinstance(short_url, str):
                                clean_short = clean_url_formatting(short_url)
                                m = re.match(r"^https?://(?:www\.)?kamaikaro\.in/(?:l/)?([a-zA-Z0-9_\-]+)$", clean_short)
                                if m:
                                    return f"https://kamaikaro.in/l/{m.group(1)}"
                                if clean_short.startswith("http"):
                                    return clean_short
                                elif clean_short.startswith("/l/"):
                                    return clean_url_formatting(f"https://kamaikaro.in{clean_short}")
                                elif clean_short.startswith("/"):
                                    return clean_url_formatting(f"https://kamaikaro.in/l{clean_short}")
                                else:
                                    return clean_url_formatting(f"https://kamaikaro.in/l/{clean_short}")

                            # 3. Slug field
                            slug = (
                                data.get("slug")
                                or (data.get("link") or {}).get("slug")
                                or (data.get("data") or {}).get("slug")
                            )
                            if slug and isinstance(slug, str):
                                clean_slug = slug.strip().lstrip("/")
                                return clean_url_formatting(f"https://kamaikaro.in/l/{clean_slug}")
                    else:
                        resp_text = await resp.text()
                        log.warning("api_shorten_link (%s) HTTP %d: %s", endpoint, resp.status, resp_text[:120])
            except Exception as ep_err:
                log.warning("api_shorten_link (%s) error: %s", endpoint, ep_err)

        return None

    try:
        return await asyncio.wait_for(_request_shorten(), timeout=5.5)
    except asyncio.TimeoutError:
        log.warning("api_shorten_link timed out (>5.5s) for %s; applying markdown hyperlink fallback", clean_orig[:60])
    except Exception as e:
        log.warning("api_shorten_link failed for %s (%s); applying markdown hyperlink fallback", clean_orig[:60], e)

    return None


def strip_deal_links_and_clean_caption(text: str) -> str:
    """Remove all deal links, markdown hyperlinks, and CTA phrases completely from caption text,
    leaving a 100% clean deal post (Title, Features, MRP, Deal Price, Discount) with ZERO text links.
    All deal links MUST only exist on the native inline button below the message.
    """
    if not text:
        return ""

    lines = [line.strip() for line in text.strip().splitlines()]
    cleaned_lines = []

    promo_patterns = [
        re.compile(r"^(?:join|follow|subscribe|more deals|more loots|updates on|channel)\b.*@\w+", re.I),
        re.compile(r"^join\s+(?:our\s+)?(?:telegram|channel|group)\b.*$", re.I),
        re.compile(r"^t\.me/[\w_]+(?:\s+for\s+more.*)?$", re.I),
        re.compile(r"^forwarded\s+from\b.*$", re.I),
        re.compile(r"^join\s+@\w+$", re.I),
    ]

    link_line_pattern = re.compile(
        r"^(?:👉|🛒|🛍️|🔗|⚡|🔥|📌|👉🏻|👉🏼|👉🏽|👉🏾|👉🏿|\*|\s)*"
        r"(?:buy\s*(?:on\s*\w+|now|here)?|grab\s*(?:deal|now|here|on\s*\w+)?|order\s*(?:now|here|on\s*\w+)?|"
        r"shop\s*(?:now|here|on\s*\w+)?|deal\s*link|link|view\s*deal|check\s*here|get\s*it\s*here|direct\s*link)"
        r"(?:\*|\s)*[:\-–—]?\s*"
        r"(?:https?://[^\s]+|\[[^\]]+\]\([^)]+\))?\s*$",
        re.IGNORECASE,
    )

    cta_words_re = re.compile(
        r"^(?:[👉🛒🛍️🔗⚡🔥📌👉🏻👉🏼👉🏽👉🏾👉🏿\*\s:\-–—\(\)\[\]/\\|.]|"
        r"buy|now|here|order|grab|deal|link|links|shop|view|on|at|get|it|amazon|flipkart|myntra|ajio|store)+$",
        re.IGNORECASE,
    )

    for line in lines:
        if not line:
            if cleaned_lines and cleaned_lines[-1] != "":
                cleaned_lines.append("")
            continue

        if any(p.match(line) for p in promo_patterns):
            continue

        if link_line_pattern.match(line):
            continue

        # If line is only a bare URL or markdown link, skip it
        if re.match(r"^https?://[^\s]+$", line) or re.match(r"^\[[^\]]+\]\([^)]+\)$", line):
            continue

        # Strip inline URLs and markdown links from the line
        line_clean = re.sub(
            r"\s*(?:👉|🛒|🛍️|🔗|⚡|🔥|📌)?\s*(?:buy\s*(?:on\s*\w+|now|here)?|grab\s*(?:deal|now|here|on\s*\w+)?|order\s*(?:now|here)?|shop\s*now|link|deal\s*link)?\s*[:\-–—]?\s*(?:https?://[^\s]+|\[[^\]]+\]\([^)]+\))\s*$",
            "",
            line,
            flags=re.IGNORECASE,
        ).strip()

        # Remove any remaining raw URLs
        line_clean = re.sub(r"https?://[^\s<>\"')\]]+", "", line_clean).strip()
        # Remove markdown hyperlinks [Buy Now](...) or [Text](...)
        line_clean = re.sub(r"\[(?:buy|now|deal|grab|shop|view|order|link|here)[^\]]*\]\([^)]+\)", "", line_clean, flags=re.I).strip()
        line_clean = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", line_clean).strip()

        # If the remaining line is only CTA words/emojis/punctuation, discard it
        if not line_clean or cta_words_re.match(line_clean):
            continue

        cleaned_lines.append(line_clean)

    while cleaned_lines and cleaned_lines[-1] == "":
        cleaned_lines.pop()

    result = "\n".join(cleaned_lines)
    result = re.sub(r"\n{3,}", "\n\n", result).strip()
    return result


def format_caption_with_links(clean_caption: str, converted_deals: list[dict[str, Any]]) -> str:
    """Append clean, prominent clickable link(s) to the caption for Telethon User Client posting,
    where Telegram API restricts inline buttons to official Bot tokens only.
    Handles single deals as well as multiple deals cleanly.
    """
    if not converted_deals:
        return clean_caption

    link_lines = []
    if len(converted_deals) == 1:
        d = converted_deals[0]
        url = d.get("converted_url", "")
        store = d.get("store", "Store")
        if d.get("is_amz") or "amazon" in store.lower():
            link_lines.append(f"🛒 Buy on Amazon: {url}")
        elif "flipkart" in store.lower():
            link_lines.append(f"🛒 Buy on Flipkart: {url}")
        elif "myntra" in store.lower():
            link_lines.append(f"🛍️ Buy on Myntra: {url}")
        elif "ajio" in store.lower():
            link_lines.append(f"🛍️ Buy on Ajio: {url}")
        else:
            link_lines.append(f"👉 🛒 Buy Now / Grab Deal: {url}")
    else:
        # Multiple links handling: list each one cleanly
        seen_urls = set()
        for d in converted_deals:
            url = d.get("converted_url", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            store = d.get("store", "Store")
            if d.get("is_amz") or "amazon" in store.lower():
                link_lines.append(f"🛒 Buy on Amazon: {url}")
            elif "flipkart" in store.lower():
                link_lines.append(f"🛒 Buy on Flipkart: {url}")
            elif "myntra" in store.lower():
                link_lines.append(f"🛍️ Buy on Myntra: {url}")
            elif "ajio" in store.lower():
                link_lines.append(f"🛍️ Buy on Ajio: {url}")
            else:
                link_lines.append(f"🛒 Buy on {store}: {url}")

    links_block = "\n".join(link_lines)
    if clean_caption.strip():
        return f"{clean_caption.strip()}\n\n{links_block}"
    return links_block


def get_deal_button_label(store: str, is_amz: bool = False) -> str:
    """Format standardized, channel-supported deal button labels:
    - Amazon: 🛒 BUY NOW ON AMAZON
    - Flipkart: 🛍️ BUY ON FLIPKART
    - Myntra: 🛍️ BUY ON MYNTRA
    - Ajio: 🛍️ BUY ON AJIO
    - Other stores: 🛍️ BUY ON {STORE}
    """
    store_clean = (store or "").strip()
    store_lower = store_clean.lower()
    if is_amz or "amazon" in store_lower:
        return "🛒 BUY NOW ON AMAZON"
    elif "flipkart" in store_lower:
        return "🛍️ BUY ON FLIPKART"
    elif "myntra" in store_lower:
        return "🛍️ BUY ON MYNTRA"
    elif "ajio" in store_lower:
        return "🛍️ BUY ON AJIO"
    elif store_clean and store_lower not in ("store", "deal"):
        return f"🛍️ BUY ON {store_clean.upper()}"
    else:
        return "🛒 BUY NOW / GRAB DEAL"


def make_aiogram_deal_button(btn_text: str, url: str) -> InlineKeyboardButton:
    """Create a channel-supported native URL inline button for Aiogram Bot."""
    return InlineKeyboardButton(text=btn_text, url=url)


def make_telethon_deal_button(btn_text: str, url: str) -> Any:
    """Create a channel-supported native URL inline button for Telethon."""
    return Button.url(btn_text, url)


async def resolve_and_convert_deals(
    text: str,
    sub_id: str,
    user_id: Optional[str] = None,
    amazon_tag: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Extract deal URLs, unshorten them, apply affiliate tags/Cuelinks,
    and return list of converted deals with store metadata for inline buttons.
    """
    if not text:
        return []

    urls = extract_deal_urls(text)
    if not urls:
        return []

    converted_deals: list[dict[str, Any]] = []

    for u in sorted(urls, key=len, reverse=True):
        try:
            target_url = u
            if is_shortened_url(u):
                try:
                    target_url = await asyncio.wait_for(unshorten_url(u), timeout=4.0)
                except Exception as unshort_err:
                    log.warning("Unshorten failed for %s (%s); using original link", u, unshort_err)
                    target_url = u

            clean_url = strip_affiliate_params(target_url)
            is_amz = is_amazon_url(target_url) or is_amazon_url(u)

            effective_user_id = user_id or sub_id or ADMIN_USER_ID

            if is_amz:
                converted_url = clean_url_formatting(
                    build_intelligent_amazon_url(
                        target_url,
                        custom_amazon_tag=amazon_tag,
                        user_id=effective_user_id,
                        fallback_url=u,
                    )
                )
                store = "Amazon"
            else:
                converted_url = clean_url_formatting(to_cuelinks(clean_url, effective_user_id))
                if any(h in clean_url for h in ("flipkart", "fkrt")):
                    store = "Flipkart"
                elif any(h in clean_url for h in ("myntra", "myntr")):
                    store = "Myntra"
                elif "ajio" in clean_url:
                    store = "Ajio"
                else:
                    store = "Deal"

            converted_deals.append({
                "original_url": u,
                "target_url": target_url,
                "converted_url": converted_url,
                "is_amz": is_amz,
                "store": store,
            })
            log.info("Resolved deal %s -> %s (store: %s, is_amz: %s)", u, converted_url, store, is_amz)
        except Exception as e:
            log.warning("Failed to convert deal URL %s: %s", u, e)

    return converted_deals


def clean_markdown_deal_template(text: str) -> str:
    """Clean markdown template for deal posts:
    - Retains deal emoji/title, discount %, price, and details
    - Removes source channel watermarks/join promos
    - Normalizes spacing (no triple blank lines, clean paragraphs)
    - Formats deal call to action cleanly with short link or markdown hyperlink
    """
    if not text:
        return ""

    lines = [line.strip() for line in text.strip().splitlines()]
    cleaned_lines = []

    # Patterns for external channel promotion / spam to remove
    promo_patterns = [
        re.compile(r"^(?:join|follow|subscribe|more deals|more loots|updates on|channel)\b.*@\w+", re.I),
        re.compile(r"^join\s+(?:our\s+)?(?:telegram|channel|group)\b.*$", re.I),
        re.compile(r"^t\.me/[\w_]+(?:\s+for\s+more.*)?$", re.I),
        re.compile(r"^forwarded\s+from\b.*$", re.I),
        re.compile(r"^join\s+@\w+$", re.I),
    ]

    for line in lines:
        if not line:
            if cleaned_lines and cleaned_lines[-1] != "":
                cleaned_lines.append("")
            continue

        if any(p.match(line) for p in promo_patterns):
            continue

        cleaned_lines.append(line)

    while cleaned_lines and cleaned_lines[-1] == "":
        cleaned_lines.pop()

    result = "\n".join(cleaned_lines)
    result = re.sub(r"\n{3,}", "\n\n", result).strip()

    # Clean up CTA line before links if present
    def _clean_link_line(match: re.Match) -> str:
        url = match.group(1)
        if url.startswith("[Buy Now"):
            return f"👉 {url}"
        return f"🛒 **Buy Now:** {url}"

    result = re.sub(
        r"(?i)(?:👉\s*|🛒\s*|🔗\s*|⚡\s*)?(?:buy\s*(?:now|here)?|grab\s*(?:deal|now|here)?|order\s*now|link|deal\s*link|shop\s*now)?\s*[:\-–—]\s*(https?://kamaikaro\.in/l/[^\s]+|\[Buy Now[^\]]+\]\([^)]+\))",
        _clean_link_line,
        result,
    )

    # Format standalone bare links on their own lines
    formatted_final_lines = []
    for line in result.splitlines():
        trimmed = line.strip()
        if re.match(r"^https?://kamaikaro\.in/l/[^\s]+$", trimmed):
            formatted_final_lines.append(f"🛒 **Buy Now:** {trimmed}")
        elif re.match(r"^\[Buy Now[^\]]+\]\([^)]+\)$", trimmed):
            formatted_final_lines.append(f"👉 {trimmed}")
        else:
            formatted_final_lines.append(line)

    result = "\n".join(formatted_final_lines)
    return result


async def convert_text(
    text: str,
    sub_id: str,
    user_id: Optional[str] = None,
    amazon_tag: Optional[str] = None,
) -> tuple[str, int]:
    if not text:
        return text, 0

    urls = extract_deal_urls(text)
    if not urls:
        return text, 0

    converted_count = 0
    # Process longest URLs first to avoid partial replacements
    for u in sorted(urls, key=len, reverse=True):
        try:
            # Unshorten redirect URLs with 4-second timeout protection
            target_url = u
            if is_shortened_url(u):
                try:
                    target_url = await asyncio.wait_for(unshorten_url(u), timeout=4.0)
                except Exception as unshort_err:
                    log.warning("Unshorten failed for %s (%s); using original link", u, unshort_err)
                    target_url = u

            clean_url = strip_affiliate_params(target_url)
            is_amz = is_amazon_url(target_url) or is_amazon_url(u)

            effective_user_id = user_id or sub_id or ADMIN_USER_ID

            # Determine the converted affiliate destination URL
            if is_amz:
                converted_url = clean_url_formatting(
                    build_intelligent_amazon_url(
                        target_url,
                        custom_amazon_tag=amazon_tag,
                        user_id=effective_user_id,
                        fallback_url=u,
                    )
                )
            else:
                converted_url = clean_url_formatting(to_cuelinks(clean_url, effective_user_id))

            # Mandatory short URL conversion via API route:
            # POST https://www.kamaikaro.in/api/links/shorten
            # with { originalUrl: convertedUrl, userId: user_id, subId: sub_id }
            short_url = None
            try:
                short_url = await api_shorten_link(converted_url, user_id=user_id, sub_id=sub_id)
            except Exception as short_err:
                log.warning("api_shorten_link error in convert_text: %s", short_err)

            if short_url:
                final_link = clean_url_formatting(short_url)
                log.info("Shortened affiliate URL: %s -> %s", u, final_link)
            else:
                # Fallback: If shorten API fails, format the text cleanly with Markdown hyperlink:
                # [Buy Now / View Deal](<convertedUrl>) so long parameters remain hidden behind clean text.
                final_link = f"[Buy Now / View Deal](<{converted_url}>)"
                log.info("api_shorten_link failed; formatted as clean markdown hyperlink: %s -> %s", u, final_link)

            text = text.replace(u, final_link)
            converted_count += 1
        except Exception as e:
            log.warning("Error in convert_text for URL %s: %s; applying fallback hyperlink", u, e)
            try:
                if (is_amazon_url(u) or (target_url and is_amazon_url(target_url))) and amazon_tag:
                    fb_url = clean_url_formatting(to_amazon_affiliate_url(target_url or u, amazon_tag, fallback_url=u))
                elif sub_id:
                    fb_url = clean_url_formatting(to_cuelinks(u, sub_id))
                else:
                    fb_url = clean_url_formatting(u)
                text = text.replace(u, f"[Buy Now / View Deal](<{fb_url}>)")
                converted_count += 1
            except Exception:
                pass
            continue

    # Safety pass: Never output raw https://linksredirect.com/?... in Telegram channel post!
    text = re.sub(r"(?<!\(<)(?<!\()https://linksredirect\.com/[^\s\)\]]+(?!\>)", r"[Buy Now / View Deal](<\g<0>>)", text)

    return text, converted_count


async def resolve_user_sub_id(user: UserConfig) -> str:
    """Strictly resolve user's SubID from memory/state or database. No dummy fallbacks."""
    sub_id = user.tracking_sub_id or _telegram_to_sub_id.get(str(user.telegram_user_id))

    if not sub_id and user.user_id:
        try:
            configs = await api_fetch_configs(user_id=user.user_id)
            if configs:
                sub_id = (
                    configs[0].get("trackingSubId")
                    or configs[0].get("subId")
                    or (configs[0].get("user") or {}).get("trackingSubId")
                    or (configs[0].get("user") or {}).get("subId")
                    or ""
                )
                if sub_id:
                    user.tracking_sub_id = sub_id
                    if user.telegram_user_id:
                        _telegram_to_sub_id[str(user.telegram_user_id)] = sub_id
        except Exception as e:
            log.warning("Dynamic sub_id fetch by userId %s failed: %s", user.user_id, e)

    if not sub_id and user.telegram_user_id:
        try:
            configs = await api_fetch_configs(telegram_id=user.telegram_user_id)
            if configs:
                sub_id = (
                    configs[0].get("trackingSubId")
                    or configs[0].get("subId")
                    or (configs[0].get("user") or {}).get("trackingSubId")
                    or (configs[0].get("user") or {}).get("subId")
                    or ""
                )
                if sub_id:
                    user.tracking_sub_id = sub_id
                    _telegram_to_sub_id[str(user.telegram_user_id)] = sub_id
        except Exception as e:
            log.warning("Dynamic sub_id fetch by telegramId %s failed: %s", user.telegram_user_id, e)

    return sub_id or ""


async def resolve_user_amazon_tag(user: UserConfig) -> str:
    """Strictly resolve user's Amazon Associate Tag from memory/state or database."""
    tag = clean_amazon_associate_tag(user.amazon_tag or _telegram_to_amazon_tag.get(str(user.telegram_user_id)) or "")

    if not tag and user.user_id:
        try:
            configs = await api_fetch_configs(user_id=user.user_id)
            if configs:
                raw_tag = str(
                    configs[0].get("customAmazonTag")
                    or configs[0].get("amazonTag")
                    or configs[0].get("amazon_tag")
                    or (configs[0].get("user") or {}).get("customAmazonTag")
                    or (configs[0].get("user") or {}).get("amazonTag")
                    or (configs[0].get("user") or {}).get("amazon_tag")
                    or ""
                ).strip()
                tag = clean_amazon_associate_tag(raw_tag)
                if tag:
                    user.amazon_tag = tag
                    if user.telegram_user_id:
                        _telegram_to_amazon_tag[str(user.telegram_user_id)] = tag
        except Exception as e:
            log.warning("Dynamic amazon_tag fetch by userId %s failed: %s", user.user_id, e)

    if not tag and user.telegram_user_id:
        try:
            configs = await api_fetch_configs(telegram_id=user.telegram_user_id)
            if configs:
                raw_tag = str(
                    configs[0].get("customAmazonTag")
                    or configs[0].get("amazonTag")
                    or configs[0].get("amazon_tag")
                    or (configs[0].get("user") or {}).get("customAmazonTag")
                    or (configs[0].get("user") or {}).get("amazonTag")
                    or (configs[0].get("user") or {}).get("amazon_tag")
                    or ""
                ).strip()
                tag = clean_amazon_associate_tag(raw_tag)
                if tag:
                    user.amazon_tag = tag
                    _telegram_to_amazon_tag[str(user.telegram_user_id)] = tag
        except Exception as e:
            log.warning("Dynamic amazon_tag fetch by telegramId %s failed: %s", user.telegram_user_id, e)

    return tag or ""


async def fetch_user_context(telegram_chat_id: int | str) -> dict[str, Any]:
    """Fetch user profile from Supabase/PostgreSQL using telegram_chat_id.
    Retrieves:
      - userId
      - customAmazonTag (if user saved their own Amazon tag in settings)
    Falls back gracefully to ADMIN_USER_ID for public channels or non-registered users.
    """
    s_id = str(telegram_chat_id).strip()
    if not s_id:
        return {
            "userId": ADMIN_USER_ID,
            "customAmazonTag": None,
            "isRegistered": False,
        }

    # 1. In-memory / cache lookup
    cached_user = store.get_by_telegram_id(s_id)
    cached_uid = _telegram_to_user_id.get(s_id) or (cached_user.user_id if cached_user else None)
    cached_tag = _telegram_to_amazon_tag.get(s_id) or (cached_user.amazon_tag if cached_user else None)

    if cached_uid:
        clean_tag = clean_amazon_associate_tag(cached_tag or "")
        return {
            "userId": cached_uid,
            "customAmazonTag": clean_tag or None,
            "isRegistered": True,
        }

    # 2. Supabase PostgREST direct lookup (if SUPABASE_URL & KEY configured)
    if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:
        try:
            s = await http()
            headers = {
                "apikey": SUPABASE_SERVICE_ROLE_KEY,
                "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                "Content-Type": "application/json",
            }
            url = f"{SUPABASE_URL}/rest/v1/TelegramChannelConfig?telegramUserId=eq.{s_id}&select=userId,amazonTag,isActive"
            timeout = aiohttp.ClientTimeout(total=4.0)
            async with s.get(url, headers=headers, timeout=timeout) as resp:
                if resp.status == 200:
                    rows = await resp.json()
                    if rows and isinstance(rows, list) and len(rows) > 0:
                        first = rows[0]
                        uid = first.get("userId")
                        tag = clean_amazon_associate_tag(first.get("amazonTag") or "")
                        if uid:
                            _telegram_to_user_id[s_id] = str(uid)
                            if tag:
                                _telegram_to_amazon_tag[s_id] = tag
                            return {
                                "userId": str(uid),
                                "customAmazonTag": tag or None,
                                "isRegistered": True,
                            }
        except Exception as sb_err:
            log.debug("Direct Supabase lookup for %s failed: %s", s_id, sb_err)

    # 3. Dynamic lookup via backend API (/api/telegram/user-config)
    try:
        configs = await api_fetch_configs(telegram_id=s_id)
        if configs and isinstance(configs, list) and len(configs) > 0:
            first = configs[0]
            uid = first.get("userId") or (first.get("user") or {}).get("id")
            tag = clean_amazon_associate_tag(
                first.get("customAmazonTag")
                or first.get("amazonTag")
                or (first.get("user") or {}).get("customAmazonTag")
                or (first.get("user") or {}).get("amazonTag")
                or ""
            )
            if uid:
                _telegram_to_user_id[s_id] = str(uid)
                if tag:
                    _telegram_to_amazon_tag[s_id] = tag
                return {
                    "userId": str(uid),
                    "customAmazonTag": tag or None,
                    "isRegistered": True,
                }
    except Exception as api_err:
        log.warning("User-config API lookup for %s failed: %s", s_id, api_err)

    # 4. Fallback for Public Channel / Non-Registered
    return {
        "userId": ADMIN_USER_ID,
        "customAmazonTag": None,
        "isRegistered": False,
    }


# --------------------------------------------------------------------------- #
# Multi-Session Telethon Manager
# --------------------------------------------------------------------------- #
class MultiSessionManager:
    def __init__(self) -> None:
        self.clients: dict[str, TelegramClient] = {}  # session_string -> TelegramClient
        self.user_sessions: dict[str, str] = {}       # telegram_user_id -> session_string
        self.bot_instance: Optional[Bot] = None
        self._lock = asyncio.Lock()

    def set_bot(self, bot: Bot) -> None:
        self.bot_instance = bot

    async def get_or_create_client(self, session_str: str, name: str = "client") -> Optional[TelegramClient]:
        if not session_str:
            return None
        async with self._lock:
            if session_str in self.clients:
                client = self.clients[session_str]
                if client.is_connected():
                    return client
                try:
                    await client.connect()
                    return client
                except Exception:
                    pass

            try:
                client = TelegramClient(
                    StringSession(session_str),
                    API_ID,
                    API_HASH,
                    auto_reconnect=True,
                    connection_retries=None,
                    retry_delay=2,
                    timeout=15,
                )
                await client.connect()
                if not await client.is_user_authorized():
                    log.warning("Session string for %s is not authorized/expired.", name)
                    await client.disconnect()
                    return None

                @client.on(events.NewMessage(incoming=True))
                async def _on_msg(event: Any) -> None:
                    try:
                        if not getattr(event, "message", None):
                            return
                        if not (event.is_channel or event.is_group):
                            return
                        if self.bot_instance:
                            asyncio.create_task(handle_new_post(event, self.bot_instance))
                    except Exception as err:
                        log.error("Unhandled error in _on_msg event listener: %s", err, exc_info=True)

                self.clients[session_str] = client
                log.info("Telethon client [%s] connected & listening.", name)
                return client
            except Exception as e:
                log.warning("Failed to initialize Telethon client for %s: %s", name, e)
                return None

    async def keep_alive_loop(self) -> None:
        """Periodic keepalive and auto-reconnect monitor for all registered Telethon clients."""
        log.info("Telethon multi-session keepalive monitor active.")
        while True:
            try:
                async with self._lock:
                    clients_snapshot = list(self.clients.items())

                for session_str, client in clients_snapshot:
                    try:
                        if not client.is_connected():
                            log.warning("Telethon client detected disconnected; attempting auto-reconnect...")
                            await client.connect()
                            if await client.is_user_authorized():
                                log.info("Telethon client auto-reconnected and authorized.")
                            else:
                                log.warning("Telethon client reconnected but session unauthorized.")
                    except Exception as conn_err:
                        log.warning("Client auto-reconnect attempt failed (%s); will retry next cycle", conn_err)
            except Exception as loop_err:
                log.warning("Error in session_mgr keep_alive_loop: %s", loop_err)
            await asyncio.sleep(15)

    async def sync_user_sessions(self, users: dict[str, UserConfig]) -> None:
        # Also ensure the default worker SESSION_STRING is active if provided
        if SESSION_STRING and SESSION_STRING not in self.clients:
            await self.get_or_create_client(SESSION_STRING, "global-default")

        for u in users.values():
            if u.is_active and u.session_string:
                self.user_sessions[str(u.telegram_user_id)] = u.session_string
                await self.get_or_create_client(u.session_string, f"user-{u.telegram_user_id}")

    async def register_user_client(self, telegram_user_id: str, client: TelegramClient) -> None:
        session_str = client.session.save()
        async with self._lock:
            self.clients[session_str] = client
            self.user_sessions[str(telegram_user_id)] = session_str

        @client.on(events.NewMessage(incoming=True))
        async def _on_msg(event: Any) -> None:
            try:
                if not getattr(event, "message", None):
                    return
                if not (event.is_channel or event.is_group):
                    return
                if self.bot_instance:
                    asyncio.create_task(handle_new_post(event, self.bot_instance))
            except Exception as err:
                log.error("Unhandled error in user _on_msg listener: %s", err, exc_info=True)

    async def get_client_for_telegram_id(self, telegram_user_id: int | str) -> Optional[TelegramClient]:
        s_id = str(telegram_user_id).strip()
        sess = self.user_sessions.get(s_id)
        if sess and sess in self.clients:
            c = self.clients[sess]
            if not c.is_connected():
                await c.connect()
            return c

        u = store.get_by_telegram_id(s_id)
        if u and u.session_string:
            return await self.get_or_create_client(u.session_string, f"user-{s_id}")

        if SESSION_STRING:
            return await self.get_or_create_client(SESSION_STRING, "global-default")
        return None

    async def disconnect_user(self, telegram_user_id: int | str) -> None:
        s_id = str(telegram_user_id).strip()
        async with self._lock:
            session_str = self.user_sessions.pop(s_id, None)
            if session_str and session_str != SESSION_STRING:
                if session_str not in self.user_sessions.values():
                    client = self.clients.pop(session_str, None)
                    if client:
                        try:
                            await client.disconnect()
                            log.info("Disconnected Telethon client for user %s", s_id)
                        except Exception as e:
                            log.warning("Error disconnecting client for %s: %s", s_id, e)

    async def disconnect_all(self) -> None:
        async with self._lock:
            for s, c in list(self.clients.items()):
                try:
                    await c.disconnect()
                except Exception:
                    pass
            self.clients.clear()


session_mgr = MultiSessionManager()


# --------------------------------------------------------------------------- #
# Telethon Deal Dispatcher & Poster
# --------------------------------------------------------------------------- #
def chat_keys(chat: Any) -> set[str]:
    keys = set()
    uname = getattr(chat, "username", None)
    if uname:
        keys.add(uname.lower().lstrip("@"))
    cid = getattr(chat, "id", None)
    if cid is not None:
        raw_id = str(cid)
        abs_id = str(abs(cid))
        clean_id = re.sub(r"^-?100", "", abs_id)
        keys.update({raw_id, abs_id, clean_id, f"-100{clean_id}", f"-{clean_id}"})
    return keys


async def bot_send_with_retry(
    bot: Bot,
    target: Any,
    caption: str,
    media_path: Optional[str],
    is_real_media: bool,
    reply_markup: Optional[InlineKeyboardMarkup],
    max_retries: int = 2,
) -> bool:
    """Send post to target channel via Aiogram Bot with retry logic and media fallback.
    Uses standard native URL buttons strictly supported by Telegram Channels.
    """
    for attempt in range(max_retries + 1):
        try:
            if is_real_media and media_path and os.path.exists(media_path):
                file_input = FSInputFile(media_path)
                if len(caption) <= CAPTION_LIMIT:
                    try:
                        await bot.send_photo(target, file_input, caption=caption, reply_markup=reply_markup)
                        return True
                    except Exception as photo_err:
                        try:
                            await bot.send_document(target, file_input, caption=caption, reply_markup=reply_markup)
                            return True
                        except Exception:
                            log.warning("Bot media upload failed (%s); falling back to text message with buttons", photo_err)
                else:
                    try:
                        sent = await bot.send_photo(target, file_input)
                        await bot.send_message(target, caption[:MESSAGE_LIMIT], reply_to_message_id=sent.message_id, reply_markup=reply_markup)
                        return True
                    except Exception:
                        try:
                            sent = await bot.send_document(target, file_input)
                            await bot.send_message(target, caption[:MESSAGE_LIMIT], reply_to_message_id=sent.message_id, reply_markup=reply_markup)
                            return True
                        except Exception:
                            log.warning("Bot media chunked send failed; falling back to text message with buttons")

            # Text-only send (or media fallback)
            await bot.send_message(target, caption[:MESSAGE_LIMIT], reply_markup=reply_markup)
            return True

        except Exception as e:
            err_str = str(e)
            retry_after = getattr(e, "retry_after", None)
            if retry_after is not None and retry_after <= 15 and attempt < max_retries:
                log.warning("Bot hit TelegramRetryAfter (%ss); sleeping and retrying for %s...", retry_after, target)
                await asyncio.sleep(retry_after + 0.5)
                continue
            if attempt < max_retries and any(t in err_str.lower() for t in ("timeout", "connection reset", "network", "retry")):
                await asyncio.sleep(1.0)
                continue
            log.warning("Bot Token posting to %s failed on attempt %d: %s", target, attempt + 1, e)
            return False

    return False


async def telethon_send_with_retry(
    u_client: TelegramClient,
    target: Any,
    caption: str,
    media: Any,
    media_path: Optional[str],
    is_real_media: bool,
    buttons: Any,
    max_retries: int = 2,
) -> bool:
    """Send post via Telethon client with retry logic and clean caption (zero text links)."""
    for attempt in range(max_retries + 1):
        try:
            file_to_send = media if media else (media_path if media_path and os.path.exists(media_path) else None)
            if is_real_media and file_to_send:
                if len(caption) <= CAPTION_LIMIT:
                    try:
                        await u_client.send_file(target, file=file_to_send, caption=caption, buttons=buttons)
                        return True
                    except Exception as btn_err:
                        log.debug("Telethon send_file with buttons failed (%s); trying without buttons", btn_err)
                        await u_client.send_file(target, file=file_to_send, caption=caption)
                        return True
                else:
                    sent_media = await u_client.send_file(target, file=file_to_send)
                    reply_id = getattr(sent_media, "id", None)
                    try:
                        await u_client.send_message(target, caption[:MESSAGE_LIMIT], reply_to=reply_id, buttons=buttons)
                    except Exception:
                        await u_client.send_message(target, caption[:MESSAGE_LIMIT], reply_to=reply_id)
                    return True
            else:
                try:
                    await u_client.send_message(target, caption[:MESSAGE_LIMIT], buttons=buttons)
                except Exception:
                    await u_client.send_message(target, caption[:MESSAGE_LIMIT])
                return True

        except FloodWaitError as fw:
            if fw.seconds <= 15 and attempt < max_retries:
                log.warning("Telethon FloodWait (%ds) on target %s; sleeping and retrying...", fw.seconds, target)
                await asyncio.sleep(fw.seconds + 0.5)
                continue
            log.error("Telethon FloodWait (%ds) exceeded limit on target %s", fw.seconds, target)
            return False
        except Exception as e:
            if attempt < max_retries:
                await asyncio.sleep(1.0)
                continue
            log.error("Telethon send failed to %s on attempt %d: %s", target, attempt + 1, e)
            return False

    return False


async def post_to_user(bot: Bot, user: UserConfig, text: str, msg: Any, media_path: Optional[str]) -> None:
    # 1. Strictly resolve active user's SubID and Amazon Associate Tag from memory/state or database
    sub_id = await resolve_user_sub_id(user)
    amazon_tag = await resolve_user_amazon_tag(user)
    uid = user.user_id or _telegram_to_user_id.get(str(user.telegram_user_id)) or ADMIN_USER_ID
    if not sub_id:
        sub_id = uid or ADMIN_USER_ID

    # 2. Extract and convert deal URLs (Amazon tag / Cuelinks)
    converted_deals = await resolve_and_convert_deals(text, sub_id=sub_id, user_id=uid, amazon_tag=amazon_tag)

    # 3. Clean up caption completely: remove all URLs, Markdown hyperlinks,
    # and CTA lines so caption is 100% clean (Title, Features, MRP, Deal Price, Discount).
    # ZERO text links in caption - all deal links MUST exist only on the native inline button below.
    clean_caption = strip_deal_links_and_clean_caption(text)
    if not clean_caption.strip():
        clean_caption = "🔥 Special Deal Offer"

    # 4. Build official channel-supported inline keyboard markup (Native URL buttons)
    aiogram_reply_markup = None
    telethon_buttons = []
    if converted_deals:
        keyboard_rows = []
        for d in converted_deals:
            url = d["converted_url"]
            store = d.get("store", "Store")
            is_amz = bool(d.get("is_amz"))
            btn_text = get_deal_button_label(store, is_amz)
            keyboard_rows.append([make_aiogram_deal_button(btn_text, url)])
            telethon_buttons.append([make_telethon_deal_button(btn_text, url)])
        aiogram_reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)

    # 5. Resolve user's target channels
    targets = []
    if user.target_channel_id:
        targets.append(user.target_channel_id)
    for t in user.target_channels:
        t_id = parse_target(t.get("id") if isinstance(t, dict) else t)
        if t_id and t_id not in targets:
            targets.append(t_id)

    if not targets:
        log.warning("User %s has no target channels configured; skipping delivery", user.user_id)
        return

    is_real_media = bool(
        getattr(msg, "photo", None)
        or getattr(msg, "video", None)
        or getattr(msg, "document", None)
        or (getattr(msg, "media", None) and not hasattr(msg.media, "webpage"))
    )

    u_client = await session_mgr.get_client_for_telegram_id(user.telegram_user_id)

    # 6. Post to each target channel using Dual-Mode Delivery with Retry Logic
    for target in targets:
        posted = False

        # ------------------------------------------------------------------- #
        # MODE 1: Official Bot Token Delivery (Primary)
        # Uses native Telegram URL inline buttons - supported by all channels
        # ------------------------------------------------------------------- #
        if bot and BOT_TOKEN:
            try:
                posted = await bot_send_with_retry(
                    bot=bot,
                    target=target,
                    caption=clean_caption,
                    media_path=media_path,
                    is_real_media=is_real_media,
                    reply_markup=aiogram_reply_markup,
                )
                if posted:
                    log.info("Posted deal to target %s via official Bot Token with native inline buttons for user %s", target, user.user_id)
                else:
                    log.info("Bot Token posting to %s failed; switching to Telethon User Client mode...", target)
            except Exception as bot_err:
                log.info("Bot Token posting to %s threw exception (%s); switching to Telethon User Client mode...", target, bot_err)

        # ------------------------------------------------------------------- #
        # MODE 2: Telethon User Client Delivery (Fallback)
        # ------------------------------------------------------------------- #
        if not posted and u_client and u_client.is_connected():
            try:
                posted = await telethon_send_with_retry(
                    u_client=u_client,
                    target=target,
                    caption=clean_caption,
                    media=getattr(msg, "media", None),
                    media_path=media_path,
                    is_real_media=is_real_media,
                    buttons=telethon_buttons if telethon_buttons else None,
                )
                if posted:
                    log.info("Posted deal to target %s via Telethon client for user %s", target, user.user_id)
                else:
                    log.error("Telethon posting failed to %s for user %s", target, user.user_id)
            except Exception as client_err:
                log.error("Telethon User Client posting exception for %s: %s", target, client_err)


async def safe_post(bot: Bot, user: UserConfig, text: str, msg: Any, media_path: Optional[str]) -> bool:
    try:
        await post_to_user(bot, user, text, msg, media_path)
        return True
    except Exception as e:
        log.error("safe_post failed for user %s: %s", user.user_id, e)
        return False


async def handle_new_post(event: Any, bot: Bot) -> None:
    try:
        msg = event.message
        if not msg:
            return

        # ------------------------------------------------------------------- #
        # Strict Duplicate Prevention (Deduplication) - 2 Hours TTL
        # 1. Source Message Level Deduplication
        # ------------------------------------------------------------------- #
        chat_id = getattr(event, "chat_id", None)
        msg_id = getattr(msg, "id", None)
        msg_fp = None
        if chat_id is not None and msg_id is not None:
            msg_fp = get_deal_fingerprint(f"msg:{chat_id}:{msg_id}")
            if msg_fp in recent_deal_cache:
                log.info("Duplicate post detected via source message ID %s:%s; skipping", chat_id, msg_id)
                return
            recent_deal_cache.add(msg_fp)

        chat = getattr(event, "chat", None)
        if chat is None:
            try:
                chat = await asyncio.wait_for(event.get_chat(), timeout=3.0)
            except Exception as e:
                log.debug("event.get_chat timed out/failed: %s", e)

        c_keys = set()
        if chat:
            c_keys.update(chat_keys(chat))
        if chat_id is not None:
            raw_id = str(chat_id)
            abs_id = str(abs(chat_id))
            clean_id = re.sub(r"^-?100", "", abs_id)
            c_keys.update({raw_id, abs_id, clean_id, f"-100{clean_id}", f"-{clean_id}"})

        active_sources = store.all_sources()

        # If active source channels are monitored, only proceed if chat matches
        if active_sources and not (c_keys & active_sources):
            return

        users = store.active_for_source(c_keys)
        if not users:
            return

        raw_text = msg.message or ""
        display_text = fix_split_urls(raw_text)

        # Incorporate hidden entity URLs if any
        for ent in sorted(msg.entities or [], key=lambda e: -e.offset):
            url = getattr(ent, "url", None)
            if url:
                clean_ent = clean_url_formatting(url)
                if is_deal_target_candidate(clean_ent):
                    display_text = display_text[:ent.offset] + clean_ent + display_text[ent.offset + ent.length:]

        is_real_media = bool(
            getattr(msg, "photo", None)
            or getattr(msg, "video", None)
            or getattr(msg, "document", None)
            or (getattr(msg, "media", None) and not hasattr(msg.media, "webpage"))
        )
        has_text = bool(display_text.strip())

        # If message has neither text nor media, skip
        if not has_text and not is_real_media:
            return

        # Unshorten any shortened deal links beforehand in parallel with timeout
        deal_urls = extract_deal_urls(display_text)
        shortened = [u for u in deal_urls if is_shortened_url(u)]
        if shortened:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*(unshorten_url(u) for u in shortened), return_exceptions=True),
                    timeout=5.0,
                )
            except Exception as unshort_batch_err:
                log.warning("Parallel unshorten batch timed out: %s", unshort_batch_err)

        # ------------------------------------------------------------------- #
        # 2. Destination Product URL Fingerprint Deduplication (2 Hours TTL)
        # ------------------------------------------------------------------- #
        deal_fps = []
        for u in deal_urls:
            target_url = u
            if is_shortened_url(u):
                try:
                    target_url = await asyncio.wait_for(unshorten_url(u), timeout=2.0)
                except Exception:
                    target_url = u
            norm_target = normalize_deal_target(target_url)
            deal_fp = get_deal_fingerprint(f"deal:{norm_target}")
            if deal_fp in recent_deal_cache:
                log.info("Duplicate deal detected via fingerprint %s (%s); completely skipping target channel post", deal_fp[:12], norm_target)
                return
            deal_fps.append(deal_fp)

        # Record unique fingerprints in cache (2-hour TTL)
        if msg_fp:
            recent_deal_cache.add(msg_fp)
        for dfp in deal_fps:
            recent_deal_cache.add(dfp)

        # Download media to disk for bot fallback if real media is present
        media_path = None
        if is_real_media:
            try:
                temp_dir = tempfile.gettempdir()
                media_path = await asyncio.wait_for(msg.download_media(file=temp_dir), timeout=15.0)
            except Exception as media_err:
                log.warning("Media download failed (%s); continuing with native Telethon media or text", media_err)

        try:
            results = await asyncio.gather(
                *(safe_post(bot, u, display_text, msg, media_path) for u in users),
                return_exceptions=True,
            )
            ok = sum(1 for r in results if r is True)
            chat_title = getattr(chat, "username", getattr(chat, "title", chat_id))
            log.info("Post from %s fanned out: %d/%d delivered", chat_title, ok, len(users))
        finally:
            if media_path and os.path.exists(media_path):
                try:
                    os.remove(media_path)
                except OSError:
                    pass
    except Exception as e:
        log.error("Unhandled error in handle_new_post listener: %s", e, exc_info=True)


# --------------------------------------------------------------------------- #
# Aiogram FSM & Dialog Management
# --------------------------------------------------------------------------- #
class LoginFSM(StatesGroup):
    waiting_for_phone = State()
    waiting_for_otp = State()
    waiting_for_2fa = State()
    waiting_for_custom_channel = State()


router = Router()

HELP_TEXT = (
    "<b>KamaiKaro Deal Automation Bot</b>\n\n"
    "Commands:\n"
    "• /start - Open main menu & dashboard\n"
    "• /login - Connect your personal Telegram account\n"
    "• /select_channels or /mychannels - Select Source & Target channels\n"
    "• /status - View your automation status & SubID\n"
    "• /disconnect - Disconnect your Telegram session\n"
    "• /cancel - Cancel any ongoing action\n\n"
    "Deals from your chosen source channels are automatically converted with your "
    "tracking SubID and posted straight to your channel!"
)


def get_main_menu_keyboard(is_logged_in: bool = False, is_linked: bool = False) -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton(
                text="📱 Login Telegram Account" if not is_logged_in else "🔄 Re-login Telegram Account",
                callback_data="menu:login",
            )
        ],
        [
            InlineKeyboardButton(text="📢 Select Channels", callback_data="menu:channels"),
            InlineKeyboardButton(text="⚙️ Automation Status", callback_data="menu:status"),
        ],
    ]
    if is_logged_in or is_linked:
        buttons.append([
            InlineKeyboardButton(text="🔌 Disconnect Session", callback_data="menu:disconnect")
        ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@router.message(CommandStart())
@router.message(Command("start"))
async def cmd_start(message: Message, command: CommandObject = None, state: FSMContext = None) -> None:
    tg_user = message.from_user
    if tg_user is None:
        await message.answer("❌ Couldn't identify your Telegram account.")
        return

    # 1. Extract start payload (support command.args or message.text)
    arg = ""
    if command and command.args:
        arg = command.args.strip()
    elif message.text and len(message.text.split(maxsplit=1)) > 1:
        arg = message.text.split(maxsplit=1)[1].strip()

    user_id = ""
    if arg:
        clean_arg = re.sub(r"^[?&]start=", "", arg).strip()
        if clean_arg.startswith("AUTH_"):
            user_id = clean_arg[len("AUTH_"):].strip()
        else:
            user_id = clean_arg.strip()
        user_id = user_id.split("?")[0].split("&")[0].strip()

    if user_id:
        # Save verified userId directly in memory and FSM state
        _telegram_to_user_id[str(tg_user.id)] = user_id
        if state:
            await state.update_data(user_id=user_id)

        res = await api_verify_sync(user_id, tg_user.id, tg_user.username)
        sub_id = (
            res.get("trackingSubId")
            or res.get("subId")
            or (res.get("user") or {}).get("trackingSubId")
            or (res.get("user") or {}).get("subId")
            or (res.get("data") or {}).get("trackingSubId")
            or (res.get("data") or {}).get("subId")
            or ""
        )
        if not sub_id:
            cfg_rows = await api_fetch_configs(user_id=user_id)
            if cfg_rows:
                sub_id = (
                    cfg_rows[0].get("trackingSubId")
                    or cfg_rows[0].get("subId")
                    or (cfg_rows[0].get("user") or {}).get("trackingSubId")
                    or (cfg_rows[0].get("user") or {}).get("subId")
                    or ""
                )

        if sub_id:
            _telegram_to_sub_id[str(tg_user.id)] = sub_id
            if state:
                await state.update_data(tracking_sub_id=sub_id, sub_id=sub_id)

        status_code = res.get("_status")
        is_success = (status_code == 200 or res.get("success") is True or "successfully" in str(res.get("message") or "").lower())

        if is_success:
            asyncio.create_task(refresh_configs())
            sub_id_display = sub_id if sub_id else "Configured in Dashboard"
            await message.answer(
                f"✅ <b>Account Connected Successfully!</b>\n\n"
                f"👤 <b>User ID:</b> <code>{user_id}</code>\n"
                f"🔗 <b>Your Tracking SubID:</b> <code>{sub_id_display}</code>\n\n"
                "👉 Tap <b>[📱 Login Telegram Account]</b> below to link your Telegram session so KamaiKaro can access your channels:",
                reply_markup=get_main_menu_keyboard(is_logged_in=False, is_linked=True),
                parse_mode="HTML",
            )
            return
        else:
            err = res.get("error") or res.get("message") or f"HTTP {status_code}"
            await message.answer(
                f"❌ <b>Account Linking Failed:</b> {err}\n\n"
                f"Please open your dashboard at <a href=\"{BASE_WEB_URL}/telegram-bot\">{BASE_WEB_URL}/telegram-bot</a> and click <b>Open in Telegram</b> to generate a valid connection link.",
                parse_mode="HTML",
            )
            return

    # 2. No start payload: check if user is already linked in memory, state, or database
    stored_user_id = _telegram_to_user_id.get(str(tg_user.id))
    if not stored_user_id and state:
        stored_user_id = (await state.get_data()).get("user_id")

    u_cfg = store.get_by_telegram_id(tg_user.id)
    if not stored_user_id and u_cfg and u_cfg.user_id:
        stored_user_id = u_cfg.user_id
        _telegram_to_user_id[str(tg_user.id)] = stored_user_id

    if not stored_user_id:
        db_rows = await api_fetch_configs(telegram_id=tg_user.id)
        if db_rows and db_rows[0].get("userId"):
            stored_user_id = db_rows[0]["userId"]
            _telegram_to_user_id[str(tg_user.id)] = stored_user_id
            if db_rows[0].get("trackingSubId") and not u_cfg:
                u_cfg = parse_config(db_rows[0])

    if stored_user_id:
        if state:
            await state.update_data(user_id=stored_user_id)
        is_logged_in = bool(u_cfg and u_cfg.session_string)
        sub_id = u_cfg.tracking_sub_id if u_cfg and u_cfg.tracking_sub_id else "Configured in Dashboard"
        await message.answer(
            f"👋 <b>Welcome back to KamaiKaro Deal Automation Bot!</b>\n\n"
            f"👤 <b>Linked User ID:</b> <code>{stored_user_id}</code>\n"
            f"🔗 <b>Your Tracking SubID:</b> <code>{sub_id}</code>\n\n"
            "Select an option below to manage your automation:",
            reply_markup=get_main_menu_keyboard(is_logged_in=is_logged_in, is_linked=True),
            parse_mode="HTML",
        )
        return

    # 3. No user ID found: prompt user to click "Open in Telegram" from web dashboard (NO dummy fallbacks)
    dashboard_url = f"{BASE_WEB_URL}/telegram-bot"
    unlinked_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Open Web Dashboard", url=dashboard_url)],
        [InlineKeyboardButton(text="🔄 Try Again", callback_data="menu:main")],
    ])
    await message.answer(
        "👋 <b>Welcome to KamaiKaro Deal Automation Bot!</b>\n\n"
        "⚠️ <b>Account Not Linked</b>\n\n"
        "Your Telegram account is not yet connected to a KamaiKaro user account.\n\n"
        "👉 <b>How to connect:</b>\n"
        f"1. Open your web dashboard: <a href=\"{dashboard_url}\">{dashboard_url}</a>\n"
        "2. Click the <b>Connect Telegram</b> (or <b>Open in Telegram</b>) button.\n"
        "3. Telegram will open and automatically link your account with your personal SubID!\n\n"
        "<i>All deals and commissions will be tracked directly to your own account.</i>",
        reply_markup=unlinked_kb,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


@router.callback_query(F.data == "menu:main")
async def cb_main_menu(query: CallbackQuery, state: FSMContext = None) -> None:
    tg_user_id = query.from_user.id
    stored_user_id = _telegram_to_user_id.get(str(tg_user_id))
    if not stored_user_id and state:
        stored_user_id = (await state.get_data()).get("user_id")

    u_cfg = store.get_by_telegram_id(tg_user_id)
    is_logged_in = bool(u_cfg and u_cfg.session_string)
    user_info = f"\n👤 <b>User ID:</b> <code>{stored_user_id}</code>\n" if stored_user_id else ""
    await query.message.edit_text(
        f"👋 <b>KamaiKaro Deal Automation Bot Menu</b>\n{user_info}\n"
        "Choose an action below:",
        reply_markup=get_main_menu_keyboard(is_logged_in=is_logged_in, is_linked=bool(stored_user_id)),
        parse_mode="HTML",
    )
    await query.answer()


@router.message(Command("cancel"))
@router.callback_query(F.data == "login:cancel")
async def cancel_handler(event: Message | CallbackQuery, state: FSMContext) -> None:
    tg_user_id = event.from_user.id
    # Preserve user_id and tracking_sub_id when clearing state
    user_id = (await state.get_data()).get("user_id") or _telegram_to_user_id.get(str(tg_user_id))
    sub_id = (await state.get_data()).get("tracking_sub_id") or (await state.get_data()).get("sub_id") or _telegram_to_sub_id.get(str(tg_user_id))
    pending = pop_login_session(tg_user_id)
    if pending and pending.get("client"):
        try:
            await pending["client"].disconnect()
        except Exception:
            pass
    await state.clear()
    state_updates: dict[str, Any] = {}
    if user_id:
        _telegram_to_user_id[str(tg_user_id)] = user_id
        state_updates["user_id"] = user_id
    if sub_id:
        _telegram_to_sub_id[str(tg_user_id)] = sub_id
        state_updates["tracking_sub_id"] = sub_id
        state_updates["sub_id"] = sub_id
    if state_updates:
        await state.update_data(**state_updates)
    msg = event.message if isinstance(event, CallbackQuery) else event
    await msg.answer("❌ Action cancelled. Tap /start to open the main menu.")
    if isinstance(event, CallbackQuery):
        await event.answer()


@router.message(Command("disconnect"))
@router.callback_query(F.data == "menu:disconnect")
async def disconnect_handler(event: Message | CallbackQuery, state: FSMContext = None) -> None:
    tg_user = event.from_user
    if tg_user is None:
        return
    tg_user_id = tg_user.id
    s_tg_id = str(tg_user_id)

    log.info("Processing disconnect for Telegram user: %s", s_tg_id)

    # 1. Retrieve user identity before clearing
    state_data = await state.get_data() if state else {}
    user_id = _telegram_to_user_id.get(s_tg_id) or state_data.get("user_id")
    u_cfg = store.get_by_telegram_id(tg_user_id)
    if not user_id and u_cfg and u_cfg.user_id:
        user_id = u_cfg.user_id

    # 2. Disconnect Telethon client and clear pending login sessions
    await session_mgr.disconnect_user(tg_user_id)
    pending = pop_login_session(tg_user_id)
    if pending and pending.get("client"):
        try:
            await pending["client"].disconnect()
        except Exception:
            pass

    # 3. Stop listening or auto-posting for this user's configured target channel immediately
    store.remove_by_telegram_id(tg_user_id)
    if user_id:
        store.users.pop(str(user_id), None)
    if u_cfg:
        u_cfg.is_active = False
        u_cfg.session_string = ""
        u_cfg.target_channel_id = None
        u_cfg.target_channels = []
        u_cfg.source_channels = []
        u_cfg.source_channels_raw = []

    # 4. Clear in-memory mappings and caches for this Telegram chat ID
    _telegram_to_user_id.pop(s_tg_id, None)
    _telegram_to_sub_id.pop(s_tg_id, None)
    _telegram_to_amazon_tag.pop(s_tg_id, None)
    _channel_drafts.pop(tg_user_id, None)
    _dialogs_cache.pop(tg_user_id, None)

    # 5. Clear FSM state completely
    if state:
        await state.clear()

    # 6. Clear session and subId link in database / web backend
    if user_id:
        try:
            await api_save_user_config({
                "userId": user_id,
                "telegramUserId": s_tg_id,
                "sessionString": "",
                "sourceChannels": [],
                "targetChannels": [],
                "targetChannelId": None,
                "targetChannelName": "",
                "isActive": False,
            })
        except Exception as e:
            log.warning("api_save_user_config error on disconnect for %s: %s", user_id, e)

        try:
            await api_save_session(
                session_string="",
                telegram_user_id=tg_user_id,
                telegram_username=tg_user.username or "",
                user_id=user_id,
            )
        except Exception as e:
            log.warning("api_save_session error on disconnect for %s: %s", user_id, e)

        try:
            s = await http()
            await s.post(
                f"{BASE_WEB_URL}/api/telegram/disconnect",
                json={"userId": user_id, "telegramUserId": s_tg_id},
                timeout=aiohttp.ClientTimeout(total=5.0),
            )
        except Exception:
            pass
    else:
        try:
            await api_save_session(
                session_string="",
                telegram_user_id=tg_user_id,
                telegram_username=tg_user.username or "",
            )
        except Exception as e:
            log.warning("api_save_session error on disconnect for tg_id %s: %s", tg_user_id, e)

    # Refresh store configs in background
    asyncio.create_task(refresh_configs())

    # 7. Send confirmation message and show initial unauthenticated state
    confirmation_text = (
        "🔌 <b>Account Disconnected</b>\n\n"
        "Your account has been disconnected successfully. Use /start or login from kamaikaro.in to connect again."
    )

    dashboard_url = f"{BASE_WEB_URL}/telegram-bot"
    unlinked_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Open Web Dashboard", url=dashboard_url)],
        [InlineKeyboardButton(text="🔄 Reconnect Account", callback_data="menu:main")],
    ])

    if isinstance(event, CallbackQuery):
        await event.message.edit_text(
            confirmation_text,
            reply_markup=unlinked_kb,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        await event.answer("Disconnected successfully.")
    else:
        await event.answer(
            confirmation_text,
            reply_markup=unlinked_kb,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )


# --------------------------------------------------------------------------- #
# Telegram Login Flow (FSM)
# --------------------------------------------------------------------------- #
@router.message(Command("login"))
@router.callback_query(F.data == "menu:login")
async def start_login(event: Message | CallbackQuery, state: FSMContext) -> None:
    tg_user_id = event.from_user.id

    # Retrieve verified user_id from state, memory, store, or database API
    curr_data = await state.get_data()
    user_id = curr_data.get("user_id") or _telegram_to_user_id.get(str(tg_user_id))
    sub_id = curr_data.get("tracking_sub_id") or curr_data.get("sub_id") or _telegram_to_sub_id.get(str(tg_user_id))
    if not user_id:
        u_cfg = store.get_by_telegram_id(tg_user_id)
        if u_cfg and u_cfg.user_id:
            user_id = u_cfg.user_id
            if u_cfg.tracking_sub_id and not sub_id:
                sub_id = u_cfg.tracking_sub_id
    if not user_id:
        rows = await api_fetch_configs(telegram_id=tg_user_id)
        if rows and rows[0].get("userId"):
            user_id = rows[0]["userId"]
            if rows[0].get("trackingSubId") and not sub_id:
                sub_id = rows[0]["trackingSubId"]

    if not user_id:
        dashboard_url = f"{BASE_WEB_URL}/telegram-bot"
        unlinked_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Open Web Dashboard", url=dashboard_url)],
            [InlineKeyboardButton(text="🔙 Main Menu", callback_data="menu:main")],
        ])
        text = (
            "⚠️ <b>Account Not Linked</b>\n\n"
            "We couldn't find your KamaiKaro user ID. You must connect your account before logging in to Telegram.\n\n"
            f"👉 Please open your web dashboard (<a href=\"{dashboard_url}\">{dashboard_url}</a>) "
            "and click <b>Open in Telegram</b> (or <b>Connect Telegram</b>)."
        )
        if isinstance(event, CallbackQuery):
            await event.message.edit_text(text, reply_markup=unlinked_kb, parse_mode="HTML")
            await event.answer()
        else:
            await event.answer(text, reply_markup=unlinked_kb, parse_mode="HTML")
        return

    _telegram_to_user_id[str(tg_user_id)] = user_id
    if sub_id:
        _telegram_to_sub_id[str(tg_user_id)] = sub_id
    pending = pop_login_session(tg_user_id)
    if pending and pending.get("client"):
        try:
            await pending["client"].disconnect()
        except Exception:
            pass

    # Preserve user_id and sub_id in state
    await state.set_state(LoginFSM.waiting_for_phone)
    state_updates: dict[str, Any] = {"user_id": user_id}
    if sub_id:
        state_updates["tracking_sub_id"] = sub_id
        state_updates["sub_id"] = sub_id
    await state.update_data(**state_updates)

    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Cancel", callback_data="login:cancel")]])
    text = (
        f"📱 <b>Telegram Account Authentication</b>\n\n"
        f"👤 <b>Account ID:</b> <code>{user_id}</code>\n\n"
        "Please enter your Telegram phone number with international country code:\n"
        "Example: <code>+919876543210</code>\n\n"
        "<i>Your session string will be linked directly to your KamaiKaro account.</i>"
    )
    if isinstance(event, CallbackQuery):
        await event.message.edit_text(text, reply_markup=cancel_kb, parse_mode="HTML")
        await event.answer()
    else:
        await event.answer(text, reply_markup=cancel_kb, parse_mode="HTML")


@router.message(LoginFSM.waiting_for_phone)
async def process_phone(message: Message, state: FSMContext) -> None:
    phone = (message.text or "").strip().replace(" ", "").replace("-", "")
    if not re.match(r"^\+\d{7,16}$", phone):
        await message.answer("❌ Invalid format. Please enter phone number starting with +, e.g. <code>+919876543210</code>:")
        return

    client = TelegramClient(
        StringSession(),
        API_ID,
        API_HASH,
        auto_reconnect=True,
        connection_retries=None,
        retry_delay=1,
        timeout=30,
    )
    try:
        await client.connect()
    except Exception as conn_err:
        logger.error(f"Failed to send code: {conn_err}")
        try:
            await client.disconnect()
        except Exception:
            pass
        await message.answer(f"❌ Failed to connect to Telegram servers: {conn_err}\n\nPlease try again with /login.")
        await state.clear()
        return

    try:
        req = await client.send_code_request(phone)
    except FloodWaitError as e:
        logger.error(f"Failed to send code: {e}")
        try:
            await client.disconnect()
        except Exception:
            pass
        wait_seconds = getattr(e, "seconds", 0)
        minutes = math.ceil(wait_seconds / 60) if wait_seconds else 0
        wait_msg = f"\n\n⏳ <b>Wait duration:</b> {wait_seconds} seconds (~{minutes} min)" if wait_seconds else ""
        await message.answer(
            f"⏳ <b>Telegram Rate Limit (FloodWaitError):</b>\n\n"
            f"<code>{e}</code>{wait_msg}\n\n"
            "Telegram has rate-limited login requests for this account. Please wait for the cooldown to expire before trying /login again."
        )
        await state.clear()
        return
    except PhoneNumberFloodError as e:
        logger.error(f"Failed to send code: {e}")
        try:
            await client.disconnect()
        except Exception:
            pass
        await message.answer(
            f"🚫 <b>Telegram Limit (PhoneNumberFloodError):</b>\n\n"
            f"<code>{e}</code>\n\n"
            "Too many requests have been made for this phone number. Telegram has temporarily rate-limited login attempts. Please wait some time before trying /login again."
        )
        await state.clear()
        return
    except ApiIdInvalidError as e:
        logger.error(f"Failed to send code: {e}")
        try:
            await client.disconnect()
        except Exception:
            pass
        await message.answer(
            f"❌ <b>API Configuration Error (ApiIdInvalidError):</b>\n\n"
            f"<code>{e}</code>\n\n"
            "The configured TELEGRAM_API_ID or TELEGRAM_API_HASH is invalid. Please check your credentials."
        )
        await state.clear()
        return
    except (PhoneNumberInvalidError, PhoneNumberBannedError) as e:
        logger.error(f"Failed to send code: {e}")
        try:
            await client.disconnect()
        except Exception:
            pass
        await message.answer(
            f"❌ <b>Phone Number Error:</b>\n\n"
            f"<code>{e}</code>\n\n"
            "Please verify your international country code format (e.g. <code>+919876543210</code>) and try again with /login."
        )
        await state.clear()
        return
    except Exception as e:
        logger.error(f"Failed to send code: {e}")
        try:
            await client.disconnect()
        except Exception:
            pass
        wait_text = ""
        wait_match = re.search(r"wait of (\d+) seconds", str(e), re.I) or re.search(r"(\d+) seconds", str(e), re.I)
        if wait_match:
            secs = int(wait_match.group(1))
            wait_text = f"\n\n⏳ <b>Wait duration:</b> {secs} seconds (~{math.ceil(secs / 60)} minutes)"

        await message.answer(
            f"❌ <b>Failed to send verification code:</b>\n\n"
            f"<code>{e}</code>{wait_text}\n\n"
            "Please try again with /login."
        )
        await state.clear()
        return

    tg_user_id = message.from_user.id
    state_data = await state.get_data()
    user_id = state_data.get("user_id") or _telegram_to_user_id.get(str(tg_user_id))
    sub_id = state_data.get("tracking_sub_id") or state_data.get("sub_id") or _telegram_to_sub_id.get(str(tg_user_id))

    sent_type_name = type(getattr(req, "type", None)).__name__
    raw_type = getattr(req, "type", None)
    next_type_name = type(getattr(req, "next_type", None)).__name__ if getattr(req, "next_type", None) else None
    timeout_sec = getattr(req, "timeout", None)

    logger.info(
        "send_code_request response: phone=%s, sent_code_type=%s, full_type=%r, phone_code_hash=%s, timeout=%s, next_type=%s, raw=%r",
        phone,
        sent_type_name,
        raw_type,
        getattr(req, "phone_code_hash", None),
        timeout_sec,
        next_type_name,
        req,
    )

    set_login_session(
        tg_user_id,
        {
            "client": client,
            "phone": phone,
            "phone_code_hash": req.phone_code_hash,
            "user_id": user_id,
            "sub_id": sub_id,
            "timestamp": time.time(),
        },
    )
    state_updates: dict[str, Any] = {
        "phone": phone,
        "phone_code_hash": req.phone_code_hash,
        "user_id": user_id,
    }
    if sub_id:
        state_updates["tracking_sub_id"] = sub_id
        state_updates["sub_id"] = sub_id
    await state.update_data(**state_updates)
    await state.set_state(LoginFSM.waiting_for_otp)

    if sent_type_name == "SentCodeTypeApp":
        delivery_info = (
            "📲 <b>Delivery: Official Telegram Mobile App</b>\n\n"
            f"Telegram sent the verification code to the official Telegram chat (<b>Telegram / 777000</b>) on your phone.\n\n"
            "⚠️ <b>Can't find the code in chat 777000?</b>\n"
            f"• Check your <b>mobile app</b> (iOS/Android) logged into <code>{phone}</code>.\n"
            "• If you have multiple accounts, switch to the profile with this exact phone number.\n"
            "• Telegram blocks code delivery to Web/Desktop clients for 3rd-party apps."
        )
    elif sent_type_name == "SentCodeTypeSms":
        delivery_info = (
            "📩 <b>Delivery: Cellular SMS Text Message</b>\n\n"
            f"Telegram dispatched your verification code via <b>SMS</b> to <code>{phone}</code>.\n\n"
            "⚠️ <b>Check your phone's SMS inbox (Messages app)</b>, NOT Telegram chat 777000.\n"
            "<i>(Carrier SMS delivery can take up to 60-120 seconds in India)</i>"
        )
    elif sent_type_name == "SentCodeTypeCall":
        delivery_info = (
            "📞 <b>Delivery: Voice Phone Call</b>\n\n"
            f"Telegram will call <code>{phone}</code> to dictate your login code."
        )
    elif sent_type_name == "SentCodeTypeFlashCall":
        delivery_info = (
            "📞 <b>Delivery: Flash Call</b>\n\n"
            f"Telegram will flash-call <code>{phone}</code>. Enter the required digits from the caller number."
        )
    else:
        delivery_info = f"📩 Telegram dispatched your verification code ({sent_type_name}) to <code>{phone}</code>."

    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Cancel", callback_data="login:cancel")]])
    await message.answer(
        f"🔐 <b>Verification Code Dispatched!</b>\n\n"
        f"{delivery_info}\n\n"
        "👉 <b>Enter the code here:</b>\n"
        "<i>(e.g. <code>12345</code> or <code>1 2 3 4 5</code>)</i>",
        reply_markup=cancel_kb,
        parse_mode="HTML",
    )


@router.message(LoginFSM.waiting_for_otp)
async def process_otp(message: Message, state: FSMContext) -> None:
    code = (message.text or "").replace(" ", "").replace("-", "").strip()
    tg_user_id = message.from_user.id
    pending = get_login_session(tg_user_id)
    if not pending or not pending.get("client"):
        await message.answer("⚠️ Session expired. Please start over with /login.")
        await state.clear()
        return

    client: TelegramClient = pending["client"]
    phone = pending["phone"]
    phone_code_hash = pending["phone_code_hash"]
    user_id = pending.get("user_id") or (await state.get_data()).get("user_id") or _telegram_to_user_id.get(str(tg_user_id))
    sub_id = pending.get("sub_id") or (await state.get_data()).get("tracking_sub_id") or (await state.get_data()).get("sub_id") or _telegram_to_sub_id.get(str(tg_user_id))

    if not client.is_connected():
        try:
            await client.connect()
        except Exception as ce:
            log.warning("Reconnect client before sign_in failed: %s", ce)

    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
    except SessionPasswordNeededError:
        pending["timestamp"] = time.time()
        await state.set_state(LoginFSM.waiting_for_2fa)
        cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Cancel", callback_data="login:cancel")]])
        await message.answer(
            "🔐 <b>Two-Step Verification (2FA) is Enabled!</b>\n\n"
            "Please enter your Telegram 2FA cloud password to complete login:",
            reply_markup=cancel_kb,
            parse_mode="HTML",
        )
        return
    except (PhoneCodeInvalidError, PhoneCodeExpiredError) as pe:
        await message.answer(f"❌ Verification failed: {pe}. Please re-enter the code or /cancel:")
        return
    except Exception as e:
        log.warning("sign_in error: %s", e)
        await message.answer(f"❌ Login failed: {e}\n\nPlease try again or /cancel:")
        return

    await finish_login(message, state, client, tg_user_id, user_id=user_id, sub_id=sub_id)


@router.message(LoginFSM.waiting_for_2fa)
async def process_2fa(message: Message, state: FSMContext) -> None:
    password = (message.text or "").strip()
    tg_user_id = message.from_user.id
    pending = get_login_session(tg_user_id)
    if not pending or not pending.get("client"):
        await message.answer("⚠️ Session expired. Please start over with /login.")
        await state.clear()
        return

    client: TelegramClient = pending["client"]
    user_id = pending.get("user_id") or (await state.get_data()).get("user_id") or _telegram_to_user_id.get(str(tg_user_id))
    sub_id = pending.get("sub_id") or (await state.get_data()).get("tracking_sub_id") or (await state.get_data()).get("sub_id") or _telegram_to_sub_id.get(str(tg_user_id))

    if not client.is_connected():
        try:
            await client.connect()
        except Exception as ce:
            log.warning("Reconnect client before 2fa sign_in failed: %s", ce)

    try:
        await client.sign_in(password=password)
    except PasswordHashInvalidError:
        await message.answer("❌ Incorrect 2FA password. Please re-enter your password or /cancel:")
        return
    except Exception as e:
        log.warning("2FA sign_in error: %s", e)
        await message.answer(f"❌ 2FA error: {e}\n\nPlease re-enter password or /cancel:")
        return

    await finish_login(message, state, client, tg_user_id, user_id=user_id, sub_id=sub_id)


async def finish_login(
    message: Message,
    state: FSMContext,
    client: TelegramClient,
    tg_user_id: int,
    user_id: Optional[str] = None,
    sub_id: Optional[str] = None,
) -> None:
    session_str = client.session.save()
    tg_user = message.from_user

    state_data = await state.get_data()
    if not user_id:
        user_id = state_data.get("user_id") or _telegram_to_user_id.get(str(tg_user_id))
    if not sub_id:
        sub_id = state_data.get("tracking_sub_id") or state_data.get("sub_id") or _telegram_to_sub_id.get(str(tg_user_id))

    log.info("Finishing login: telegram_user_id=%s, user_id=%s", tg_user_id, user_id)

    # Save to Next.js API WITH explicit user_id!
    res = await api_save_session(
        session_string=session_str,
        telegram_user_id=tg_user_id,
        telegram_username=tg_user.username or "",
        user_id=user_id,
    )

    # Capture exact tracking SubID returned from Next.js web API
    api_sub_id = (
        res.get("trackingSubId")
        or res.get("subId")
        or (res.get("user") or {}).get("trackingSubId")
        or (res.get("user") or {}).get("subId")
        or (res.get("data") or {}).get("trackingSubId")
        or (res.get("data") or {}).get("subId")
        or (res.get("config") or {}).get("trackingSubId")
        or (res.get("config") or {}).get("subId")
        or sub_id
        or ""
    )

    if not api_sub_id:
        # Fallback to query user config from Next.js API if not included directly in save-session response
        try:
            cfg_rows = await api_fetch_configs(telegram_id=tg_user_id, user_id=user_id)
            if cfg_rows:
                api_sub_id = (
                    cfg_rows[0].get("trackingSubId")
                    or cfg_rows[0].get("subId")
                    or (cfg_rows[0].get("user") or {}).get("trackingSubId")
                    or (cfg_rows[0].get("user") or {}).get("subId")
                    or ""
                )
        except Exception as e:
            log.warning("Fetch configs after save_session failed: %s", e)

    await state.clear()
    state_updates: dict[str, Any] = {}
    if user_id:
        _telegram_to_user_id[str(tg_user_id)] = user_id
        state_updates["user_id"] = user_id
    if api_sub_id:
        _telegram_to_sub_id[str(tg_user_id)] = api_sub_id
        state_updates["tracking_sub_id"] = api_sub_id
        state_updates["sub_id"] = api_sub_id
        if user_id and user_id in store.users:
            store.users[user_id].tracking_sub_id = api_sub_id
        cfg = store.get_by_telegram_id(tg_user_id)
        if cfg:
            cfg.tracking_sub_id = api_sub_id

    if state_updates:
        await state.update_data(**state_updates)

    pop_login_session(tg_user_id)

    # Register live client
    await session_mgr.register_user_client(str(tg_user_id), client)
    asyncio.create_task(refresh_configs())

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Select Channels", callback_data="menu:channels")],
        [InlineKeyboardButton(text="⚙️ Automation Status", callback_data="menu:status")],
    ])

    user_info = f" (User ID: <code>{user_id}</code>)" if user_id else ""
    sub_info = f"\n🔗 <b>Your Tracking SubID:</b> <code>{api_sub_id}</code>\n" if api_sub_id else ""
    await message.answer(
        f"✅ <b>Account Linked Successfully!</b>{user_info}\n\n"
        f"Your Telegram account is now connected and authorized.{sub_info}\n"
        "👉 Next step: tap <b>[📢 Select Channels]</b> to choose your source & target channels.",
        reply_markup=kb,
        parse_mode="HTML",
    )


# --------------------------------------------------------------------------- #
# Dynamic Channel Selection UI (Pagination & Checkmarks)
# --------------------------------------------------------------------------- #
async def get_user_dialogs(tg_user_id: int) -> list[dict[str, Any]]:
    now = time.time()
    if tg_user_id in _dialogs_cache:
        ts, cached = _dialogs_cache[tg_user_id]
        if (now - ts) < 180:  # cache for 3 mins
            return cached

    client = await session_mgr.get_client_for_telegram_id(tg_user_id)
    if not client:
        return []

    try:
        dialogs = await client.get_dialogs(limit=100)
    except Exception as e:
        log.warning("get_dialogs failed for %s: %s", tg_user_id, e)
        return []

    out = []
    for d in dialogs:
        if d.is_channel or d.is_group:
            entity = d.entity
            admin_rights = getattr(entity, "admin_rights", None)
            is_creator = getattr(entity, "creator", False)
            can_post = bool(
                is_creator
                or (admin_rights and (getattr(admin_rights, "post_messages", False) or getattr(admin_rights, "send_messages", False)))
                or getattr(entity, "megagroup", False)
            )
            out.append({
                "id": d.id,
                "title": d.title or "Untitled Channel",
                "username": getattr(entity, "username", None),
                "can_post": can_post,
            })

    _dialogs_cache[tg_user_id] = (now, out)
    return out


def build_channel_hub_kb(sources_count: int, targets_count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📥 Source Channels ({sources_count})", callback_data="ch:src:0")],
        [InlineKeyboardButton(text="➕ Add Custom Source Channel", callback_data="ch:add_custom")],
        [InlineKeyboardButton(text=f"📤 Target Channel(s) ({targets_count})", callback_data="ch:tgt:0")],
        [InlineKeyboardButton(text="💾 Save & Activate Channels", callback_data="ch:save")],
        [InlineKeyboardButton(text="🔙 Back to Main Menu", callback_data="menu:main")],
    ])


@router.message(Command("select_channels"))
@router.message(Command("mychannels"))
@router.callback_query(F.data == "menu:channels")
@router.callback_query(F.data == "ch:hub")
async def show_channel_hub(event: Message | CallbackQuery, state: FSMContext = None) -> None:
    tg_user_id = event.from_user.id
    u_cfg = store.get_by_telegram_id(tg_user_id)
    if not u_cfg:
        try:
            db_rows = await api_fetch_configs(telegram_id=tg_user_id)
            if db_rows:
                u_cfg = parse_config(db_rows[0])
                if u_cfg and u_cfg.user_id:
                    _telegram_to_user_id[str(tg_user_id)] = u_cfg.user_id
        except Exception:
            pass

    # Initialize draft selection with NO presets active by default!
    # Only load channels previously explicitly saved by the user.
    if tg_user_id not in _channel_drafts:
        initial_sources = set()
        initial_targets = set()
        if u_cfg and u_cfg.source_channels:
            for s in u_cfg.source_channels:
                initial_sources.add(norm_channel(s))
        if u_cfg:
            if u_cfg.target_channel_id:
                initial_targets.add(str(u_cfg.target_channel_id))
            for t in u_cfg.target_channels:
                t_id = str(t.get("id") if isinstance(t, dict) else t)
                if t_id:
                    initial_targets.add(t_id)
        _channel_drafts[tg_user_id] = {
            "sources": initial_sources,
            "targets": initial_targets,
        }

    draft = _channel_drafts[tg_user_id]

    text = (
        "📢 <b>Channel Management Hub</b>\n\n"
        f"📥 <b>Source Channels:</b> {len(draft['sources'])} selected (deals monitored)\n"
        f"📤 <b>Target Channels:</b> {len(draft['targets'])} selected (deals posted with your SubID)\n\n"
        "Tap below to choose dialogs or add custom channels, then click <b>Save & Activate</b>:"
    )

    kb = build_channel_hub_kb(len(draft["sources"]), len(draft["targets"]))
    if isinstance(event, CallbackQuery):
        await event.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        await event.answer()
    else:
        await event.answer(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "ch:add_custom")
async def prompt_custom_channel(query: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(LoginFSM.waiting_for_custom_channel)
    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Back to Channels Hub", callback_data="ch:hub")]])
    await query.message.edit_text(
        "➕ <b>Add Custom Source Channel</b>\n\n"
        "Send the Telegram channel username or link you want to monitor deals from.\n\n"
        "<b>Examples:</b>\n"
        "• <code>@deals_channel</code>\n"
        "• <code>https://t.me/deals_channel</code>\n"
        "• Channel ID like <code>-1001234567890</code>\n\n"
        "Type your channel username below or click Back:",
        reply_markup=cancel_kb,
        parse_mode="HTML",
    )
    await query.answer()


@router.message(LoginFSM.waiting_for_custom_channel)
async def process_custom_channel(message: Message, state: FSMContext) -> None:
    tg_user_id = message.from_user.id
    input_text = (message.text or "").strip()
    if not input_text:
        await message.answer("❌ Please send a valid channel username or link, or /cancel.")
        return

    clean_key = norm_channel(input_text)
    if not clean_key:
        await message.answer("❌ Could not parse channel. Please enter like <code>@channelname</code>:")
        return

    draft = _channel_drafts.setdefault(tg_user_id, {"sources": set(), "targets": set()})
    draft["sources"].add(clean_key)
    await state.set_state(None)

    kb = build_channel_hub_kb(len(draft["sources"]), len(draft["targets"]))
    await message.answer(
        f"✅ <b>Added source channel:</b> <code>{clean_key}</code>\n\n"
        f"You now have {len(draft['sources'])} source channel(s) selected.\n\n"
        "Tap <b>💾 Save & Activate Channels</b> to confirm and activate monitoring!",
        reply_markup=kb,
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("ch:src:"))
async def view_source_channels(query: CallbackQuery) -> None:
    page = int(query.data.split(":")[2])
    tg_user_id = query.from_user.id
    dialogs = await get_user_dialogs(tg_user_id)

    if not dialogs:
        await query.answer("⚠️ Could not load channels. Please /login first.", show_alert=True)
        return

    draft = _channel_drafts.setdefault(tg_user_id, {"sources": set(), "targets": set()})
    sources = draft["sources"]

    page_size = 5
    total_pages = max(1, math.ceil(len(dialogs) / page_size))
    page = max(0, min(page, total_pages - 1))
    items = dialogs[page * page_size : (page + 1) * page_size]

    keyboard_rows = []
    for d in items:
        k1 = norm_channel(d.get("username") or "")
        k2 = str(d["id"])
        k3 = norm_channel(k2)
        is_sel = bool(k1 and k1 in sources) or (k2 in sources) or (k3 in sources)
        prefix = "✅ " if is_sel else "⬜ "
        title = (d["title"] or "Channel")[:22]
        keyboard_rows.append([
            InlineKeyboardButton(text=f"{prefix}{title}", callback_data=f"ch:ts:{d['id']}:{page}")
        ])

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"ch:src:{page-1}"))
    nav_row.append(InlineKeyboardButton(text=f"Page {page+1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"ch:src:{page+1}"))
    keyboard_rows.append(nav_row)

    keyboard_rows.append([
        InlineKeyboardButton(text="🔙 Back to Channels Hub", callback_data="ch:hub")
    ])

    await query.message.edit_text(
        "📥 <b>Select Source Channels</b> (Where deals are monitored from):\n\n"
        "Tap a channel to toggle checkmark. When done, go back to the hub and tap Save.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_rows),
        parse_mode="HTML",
    )
    await query.answer()


@router.callback_query(F.data.startswith("ch:ts:"))
async def toggle_source_channel(query: CallbackQuery) -> None:
    parts = query.data.split(":")
    ch_id = parts[2]
    page = int(parts[3])
    tg_user_id = query.from_user.id
    draft = _channel_drafts.setdefault(tg_user_id, {"sources": set(), "targets": set()})

    # Find username if available
    dialogs = await get_user_dialogs(tg_user_id)
    key = norm_channel(ch_id)
    for d in dialogs:
        if str(d["id"]) == ch_id and d.get("username"):
            key = norm_channel(d["username"])
            break

    if key in draft["sources"] or ch_id in draft["sources"]:
        draft["sources"].discard(key)
        draft["sources"].discard(ch_id)
    else:
        draft["sources"].add(key)

    await view_source_channels(query)


@router.callback_query(F.data.startswith("ch:tgt:"))
async def view_target_channels(query: CallbackQuery) -> None:
    page = int(query.data.split(":")[2])
    tg_user_id = query.from_user.id
    dialogs = await get_user_dialogs(tg_user_id)

    if not dialogs:
        await query.answer("⚠️ Could not load channels. Please /login first.", show_alert=True)
        return

    # Sort channels where user has posting permissions first
    targets_pool = sorted(dialogs, key=lambda x: not x["can_post"])
    draft = _channel_drafts.setdefault(tg_user_id, {"sources": set(), "targets": set()})
    targets = draft["targets"]

    page_size = 5
    total_pages = max(1, math.ceil(len(targets_pool) / page_size))
    page = max(0, min(page, total_pages - 1))
    items = targets_pool[page * page_size : (page + 1) * page_size]

    keyboard_rows = []
    for d in items:
        ch_id_str = str(d["id"])
        is_sel = ch_id_str in targets or (d.get("username") and norm_channel(d["username"]) in targets)
        prefix = "🎯 " if is_sel else "⬜ "
        tag = " (Admin)" if d["can_post"] else ""
        title = (d["title"] or "Channel")[:20] + tag
        keyboard_rows.append([
            InlineKeyboardButton(text=f"{prefix}{title}", callback_data=f"ch:tt:{d['id']}:{page}")
        ])

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"ch:tgt:{page-1}"))
    nav_row.append(InlineKeyboardButton(text=f"Page {page+1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"ch:tgt:{page+1}"))
    keyboard_rows.append(nav_row)

    keyboard_rows.append([
        InlineKeyboardButton(text="🔙 Back to Channels Hub", callback_data="ch:hub")
    ])

    await query.message.edit_text(
        "📤 <b>Select Target Channels</b> (Where converted deals are posted):\n\n"
        "Tap a channel to toggle target selection. When done, go back to the hub and tap Save.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_rows),
        parse_mode="HTML",
    )
    await query.answer()


@router.callback_query(F.data.startswith("ch:tt:"))
async def toggle_target_channel(query: CallbackQuery) -> None:
    parts = query.data.split(":")
    ch_id = parts[2]
    page = int(parts[3])
    tg_user_id = query.from_user.id
    draft = _channel_drafts.setdefault(tg_user_id, {"sources": set(), "targets": set()})

    ch_id_str = str(ch_id)
    if ch_id_str in draft["targets"]:
        draft["targets"].discard(ch_id_str)
    else:
        draft["targets"].add(ch_id_str)

    await view_target_channels(query)


@router.callback_query(F.data == "ch:save")
async def save_channels_handler(query: CallbackQuery, state: FSMContext = None) -> None:
    tg_user_id = query.from_user.id
    draft = _channel_drafts.get(tg_user_id)
    if not draft:
        await query.answer("No changes to save.")
        return

    # Retrieve verified user_id
    user_id = _telegram_to_user_id.get(str(tg_user_id))
    if not user_id and state:
        user_id = (await state.get_data()).get("user_id")
    if not user_id:
        cfg = store.get_by_telegram_id(tg_user_id)
        if cfg and cfg.user_id:
            user_id = cfg.user_id
    if not user_id:
        try:
            db_rows = await api_fetch_configs(telegram_id=tg_user_id)
            if db_rows and db_rows[0].get("userId"):
                user_id = db_rows[0]["userId"]
                _telegram_to_user_id[str(tg_user_id)] = user_id
        except Exception:
            pass

    dialogs = await get_user_dialogs(tg_user_id)
    dialog_map = {str(d["id"]): d for d in dialogs}
    for d in dialogs:
        if d.get("username"):
            dialog_map[norm_channel(d["username"])] = d

    # Build source channel objects
    source_objs = []
    for s in draft["sources"]:
        d = dialog_map.get(s)
        if d:
            source_objs.append({"id": str(d["id"]), "title": d["title"], "username": d.get("username") or ""})
        else:
            source_objs.append({"id": s, "title": s, "username": s})

    # Build target channel objects
    target_objs = []
    primary_target_id = None
    primary_target_name = None
    for t in draft["targets"]:
        d = dialog_map.get(t)
        if d:
            target_objs.append({"id": str(d["id"]), "title": d["title"], "username": d.get("username") or ""})
            if not primary_target_id:
                primary_target_id = str(d["id"])
                primary_target_name = d["title"]
        else:
            target_objs.append({"id": t, "title": t, "username": t})
            if not primary_target_id:
                primary_target_id = t
                primary_target_name = t

    res = await api_save_channels(
        telegram_user_id=tg_user_id,
        source_channels=source_objs,
        target_channels=target_objs,
        target_channel_id=primary_target_id,
        target_channel_name=primary_target_name,
        user_id=user_id,
    )

    await api_save_user_config({
        "userId": user_id,
        "telegramUserId": str(tg_user_id),
        "sourceChannels": source_objs,
        "targetChannels": target_objs,
        "targetChannelId": primary_target_id,
        "targetChannelName": primary_target_name,
        "isActive": True,
    })

    asyncio.create_task(refresh_configs())

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ View Status", callback_data="menu:status")],
        [InlineKeyboardButton(text="🔙 Main Menu", callback_data="menu:main")],
    ])

    user_info = f"\n🆔 <b>User ID:</b> <code>{user_id}</code>" if user_id else ""
    await query.message.edit_text(
        "✅ <b>Channels Saved & Activated!</b>\n\n"
        f"📥 <b>Source Channels:</b> {len(source_objs)} active\n"
        f"📤 <b>Target Channels:</b> {len(target_objs)} active"
        f"{user_info}\n\n"
        "🚀 The worker is monitoring your sources and will automatically convert "
        "and post deals to your target channel!",
        reply_markup=kb,
        parse_mode="HTML",
    )
    await query.answer()


@router.callback_query(F.data == "noop")
async def noop_handler(query: CallbackQuery) -> None:
    await query.answer()


# --------------------------------------------------------------------------- #
# Status & Help
# --------------------------------------------------------------------------- #
@router.message(Command("status"))
@router.callback_query(F.data == "menu:status")
async def cmd_status(event: Message | CallbackQuery, state: FSMContext = None) -> None:
    tg_user = event.from_user
    if tg_user is None:
        return

    state_data = await state.get_data() if state else {}
    exact_sub_id = (
        state_data.get("tracking_sub_id")
        or state_data.get("sub_id")
        or _telegram_to_sub_id.get(str(tg_user.id))
    )

    # Check local store or fetch fresh
    c = store.get_by_telegram_id(tg_user.id)
    if not c or not (c.tracking_sub_id or exact_sub_id):
        uid = _telegram_to_user_id.get(str(tg_user.id))
        if not uid and state:
            uid = state_data.get("user_id")
        rows = await api_fetch_configs(telegram_id=tg_user.id, user_id=uid)
        parsed = [p for p in (parse_config(r) for r in rows) if p]
        if parsed:
            c = parsed[0]
            if c.user_id:
                _telegram_to_user_id[str(tg_user.id)] = c.user_id
            if c.tracking_sub_id:
                _telegram_to_sub_id[str(tg_user.id)] = c.tracking_sub_id

    final_sub_id = (
        exact_sub_id
        or (c.tracking_sub_id if c and c.tracking_sub_id else None)
        or _telegram_to_sub_id.get(str(tg_user.id))
    )

    if not c and not final_sub_id:
        text = (
            "ℹ️ <b>No Linked Account Found</b>\n\n"
            "Please open your website dashboard and click <b>Connect Telegram</b> to link your account, "
            "or connect your Telegram account with /login."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📱 Login Telegram Account", callback_data="menu:login")],
            [InlineKeyboardButton(text="🌐 Open Dashboard", url=f"{BASE_WEB_URL}/telegram-bot")],
            [InlineKeyboardButton(text="🔙 Main Menu", callback_data="menu:main")],
        ])
    else:
        target = (c.target_channel_name or c.target_channel_id) if c else "Not set"
        session_status = "🟢 Connected" if (c and c.session_string) else "🟡 Using Bot / Not Connected"
        sub_id_str = final_sub_id or "Not configured"
        amazon_tag_str = (c.amazon_tag if c and c.amazon_tag else None) or _telegram_to_amazon_tag.get(str(tg_user.id)) or "None (Cuelinks routed)"
        user_id_str = (c.user_id if c else None) or _telegram_to_user_id.get(str(tg_user.id)) or state_data.get("user_id") or "Connected"
        is_active = c.is_active if c else True
        source_channels = c.source_channels if c else []

        text = (
            "⚙️ <b>Automation Status</b>\n\n"
            f"• <b>User ID:</b> <code>{user_id_str}</code>\n"
            f"• <b>Status:</b> {'🟢 Active' if is_active else '🔴 Inactive'}\n"
            f"• <b>SubID:</b> <code>{sub_id_str}</code>\n"
            f"• <b>Amazon Tag:</b> <code>{amazon_tag_str}</code>\n"
            f"• <b>Telegram Account:</b> {session_status}\n"
            f"• <b>Target Channel:</b> <code>{target}</code>\n"
            f"• <b>Source Channels:</b> {len(source_channels)}\n"
            + (("\n".join(f"  ▫️ {x}" for x in source_channels[:8])) if source_channels else "  ▫️ None selected")
            + ("\n  <i>...and more</i>" if len(source_channels) > 8 else "")
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Select Channels", callback_data="menu:channels")],
            [InlineKeyboardButton(text="🔄 Re-login", callback_data="menu:login")],
            [InlineKeyboardButton(text="🔌 Disconnect Session", callback_data="menu:disconnect")],
            [InlineKeyboardButton(text="🔙 Main Menu", callback_data="menu:main")],
        ])

    if isinstance(event, CallbackQuery):
        await event.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        await event.answer()
    else:
        await event.answer(text, reply_markup=kb, parse_mode="HTML")


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    await message.answer(HELP_TEXT, parse_mode="HTML")


@router.message(F.text)
async def handle_user_incoming_url(message: Message, state: FSMContext = None) -> None:
    """When a user sends a URL via Telegram:
    1. User Context Lookup:
       - Fetch profile from Supabase/PostgreSQL using telegram_chat_id.
       - Retrieve userId & customAmazonTag (if saved in settings).
    2. Intelligent Amazon Link Builder:
       - IF URL is Amazon:
         - IF customAmazonTag exists and is non-empty:
           Format URL: ${cleanUrl}?tag=${customAmazonTag}
         - ELSE:
           Format URL: ${cleanUrl}?tag=${AMAZON_ASSOCIATE_TAG || 'kamaikaro02-21'}&ascsubtag=${userId}
       - ELSE (Other stores):
         - Format URL via Cuelinks API with subid=${userId}.
    3. Fallback for Public Channel / Non-Registered:
       - If message is from the broadcast channel or non-registered user, use ADMIN_USER_ID as default subid.
    """
    if state:
        current_state = await state.get_state()
        if current_state is not None:
            return

    raw_text = (message.text or "").strip()
    if not raw_text or raw_text.startswith("/"):
        return

    urls = extract_deal_urls(raw_text)
    if not urls:
        return

    # 1. User Context Lookup using telegram_chat_id
    telegram_chat_id = message.chat.id
    sender_id = message.from_user.id if message.from_user else telegram_chat_id
    user_ctx = await fetch_user_context(sender_id)
    user_id = user_ctx.get("userId") or ADMIN_USER_ID
    custom_amazon_tag = user_ctx.get("customAmazonTag")
    is_registered = user_ctx.get("isRegistered", False)

    # 2. Intelligent Link Builder
    converted_deals = []
    buttons = []
    links_text_lines = []

    for u in urls:
        target_url = u
        if is_shortened_url(u):
            try:
                target_url = await asyncio.wait_for(unshorten_url(u), timeout=4.0)
            except Exception:
                target_url = u

        clean_url = strip_affiliate_params(target_url)
        is_amz = is_amazon_url(target_url) or is_amazon_url(u)

        if is_amz:
            converted_url = clean_url_formatting(
                build_intelligent_amazon_url(
                    target_url,
                    custom_amazon_tag=custom_amazon_tag,
                    user_id=user_id,
                    fallback_url=u,
                )
            )
            store = "Amazon"
            btn_text = "🛒 BUY NOW ON AMAZON"
        else:
            converted_url = clean_url_formatting(to_cuelinks(clean_url, user_id))
            store = "Store"
            if any(h in clean_url for h in ("flipkart", "fkrt")):
                store = "Flipkart"
                btn_text = "🛍️ BUY ON FLIPKART"
            elif any(h in clean_url for h in ("myntra", "myntr")):
                store = "Myntra"
                btn_text = "🛍️ BUY ON MYNTRA"
            elif "ajio" in clean_url:
                store = "Ajio"
                btn_text = "🛍️ BUY ON AJIO"
            else:
                btn_text = "🛒 BUY NOW / GRAB DEAL"

        # Shorten link via internal API if possible
        short_url = None
        if not is_amz or not custom_amazon_tag:
            try:
                short_url = await api_shorten_link(converted_url, user_id=user_id, sub_id=user_id)
            except Exception:
                short_url = None

        final_link = short_url or converted_url
        converted_deals.append({
            "original": u,
            "converted": converted_url,
            "final": final_link,
            "store": store,
            "is_amz": is_amz,
        })
        links_text_lines.append(f"👉 <b>{store}:</b> {final_link}")
        buttons.append([InlineKeyboardButton(text=btn_text, url=final_link)])

    reply_markup = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None

    # 3. Response Formatting
    msg_lines = [
        "✅ <b>Your Monetized Deal Link is Ready!</b>",
        "",
        *links_text_lines,
    ]

    has_amz = any(d["is_amz"] for d in converted_deals)
    if has_amz and custom_amazon_tag:
        msg_lines.append(f"\n🏷️ <i>Applied your custom Amazon tag: <code>{custom_amazon_tag}</code></i>")
    elif has_amz:
        msg_lines.append(f"\n🏷️ <i>Applied Master Amazon Tag with subid: <code>{user_id}</code></i>")
    else:
        msg_lines.append(f"\n🏷️ <i>Tracking SubID: <code>{user_id}</code></i>")

    if not is_registered:
        msg_lines.append(
            "\n💡 <i>Tip: Your Telegram account is not yet connected to a KamaiKaro user account. "
            "Links are generated using default admin settings. Use /start to link your account and earn commissions!</i>"
        )

    await message.reply(
        "\n".join(msg_lines),
        reply_markup=reply_markup,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


# --------------------------------------------------------------------------- #
# Config Sync Worker Loop
# --------------------------------------------------------------------------- #
async def refresh_configs() -> None:
    try:
        rows = await api_fetch_configs()
    except Exception as e:
        log.warning("Config sync failed: %s", e)
        return

    if rows is None:
        log.warning("Config sync API error or timeout; retaining %d cached users & %d sources to avoid deal stoppage.", len(store.users), len(store.all_sources()))
        return

    if len(rows) == 0 and len(store.users) > 0:
        log.warning("Config sync returned 0 rows but store currently has %d active users; retaining cached configurations.", len(store.users))
        return

    users: dict[str, UserConfig] = {}
    for r in rows:
        cfg = parse_config(r)
        if cfg and cfg.is_active:
            users[cfg.user_id] = cfg
            if cfg.telegram_user_id:
                _telegram_to_user_id[str(cfg.telegram_user_id)] = cfg.user_id
                if cfg.tracking_sub_id:
                    _telegram_to_sub_id[str(cfg.telegram_user_id)] = cfg.tracking_sub_id
                if cfg.amazon_tag:
                    _telegram_to_amazon_tag[str(cfg.telegram_user_id)] = cfg.amazon_tag

    if users:
        store.replace(users)
        log.info("Config synced: %d active users, %d distinct sources: %s", len(users), len(store.all_sources()), list(store.all_sources())[:5])
        await session_mgr.sync_user_sessions(users)
    elif not store.users:
        store.replace({})


async def config_sync_loop() -> None:
    while True:
        await refresh_configs()
        await asyncio.sleep(SYNC_INTERVAL)


# --------------------------------------------------------------------------- #
# aiohttp Health-Check Server
# --------------------------------------------------------------------------- #
http_runner: Optional[web.AppRunner] = None


async def health_check_handler(request: web.Request) -> web.Response:
    sources = list(store.all_sources())
    return web.json_response({
        "status": "ok",
        "service": "kamaikaro-worker",
        "active_users": len(store.users),
        "active_sources": len(sources),
        "source_channels": sources[:25],
        "connected_sessions": sum(1 for c in session_mgr.clients.values() if c.is_connected()),
        "total_sessions": len(session_mgr.clients),
    })


async def run_http_server() -> None:
    global http_runner
    app = web.Application()
    app.router.add_get("/", health_check_handler)
    app.router.add_get("/health", health_check_handler)

    http_runner = web.AppRunner(app)
    await http_runner.setup()
    site = web.TCPSite(http_runner, host="0.0.0.0", port=PORT)
    await site.start()
    log.info("HTTP health-check server listening on port %d", PORT)
    while True:
        await asyncio.sleep(3600)


# --------------------------------------------------------------------------- #
# Bot Runner & Main Entrypoint
# --------------------------------------------------------------------------- #
async def run_bot(bot: Bot) -> None:
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    while True:
        try:
            await dp.start_polling(bot, handle_signals=False)
            return
        except Exception:
            log.exception("Bot polling crashed; restarting in 5s")
            await asyncio.sleep(5)


def validate_env() -> None:
    missing = [k for k, v in {
        "BOT_TOKEN": BOT_TOKEN,
        "TELEGRAM_API_ID": API_ID,
        "TELEGRAM_API_HASH": API_HASH,
    }.items() if not v]
    if missing:
        raise SystemExit("Missing required env vars: " + ", ".join(missing))


async def main() -> None:
    validate_env()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    me = await bot.get_me()
    log.info("Bot online as @%s (id=%d)", me.username, me.id)
    session_mgr.set_bot(bot)

    # Initial load of configs & clients
    await refresh_configs()

    try:
        await asyncio.gather(
            run_http_server(),
            run_bot(bot),
            config_sync_loop(),
            session_mgr.keep_alive_loop(),
        )
    finally:
        await bot.session.close()
        if _session and not _session.closed:
            await _session.close()
        await session_mgr.disconnect_all()
        if http_runner:
            await http_runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit) as e:
        if isinstance(e, SystemExit) and e.code:
            raise
