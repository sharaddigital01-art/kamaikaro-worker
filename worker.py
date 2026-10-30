"""
24x7 multi-user Telegram deal-reposting worker.

Components (all in one asyncio loop):
  1. aiogram 3 bot  - /start AUTH_{userId} linking, /status, /help
  2. config sync    - polls the web API every N seconds, caches user setups
  3. Telethon       - listens to source channels, converts links, reposts
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse

import aiohttp
from aiohttp import web
from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import Message
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession

load_dotenv()

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
API_ID = int(os.getenv("TELEGRAM_API_ID", "0") or 0)
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
SESSION_STRING = os.getenv("TELETHON_SESSION_STRING", "")
BASE_WEB_URL = os.getenv("BASE_WEB_URL", "https://kamai-three.vercel.app").rstrip("/")
WORKER_API_SECRET = os.getenv("WORKER_API_SECRET", "")
CUELINKS_PUB_ID = os.getenv("CUELINKS_PUB_ID", "282274")
CUELINKS_CHNL_ID = os.getenv("CUELINKS_CHNL_ID", "332862")
SYNC_INTERVAL = int(os.getenv("CONFIG_SYNC_INTERVAL", "60"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
PORT = int(os.getenv("PORT", 8080))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
log = logging.getLogger("worker")

VERIFY_URL = f"{BASE_WEB_URL}/api/telegram/verify-sync"
CONFIG_URL = f"{BASE_WEB_URL}/api/telegram/user-config"

SHORTENER_DOMAINS = (
    "amzn.to", "amzn.in", "a.co",
    "fkrt.it", "fkrt.cc", "dl.flipkart.com",
    "myntr.it", "ajio.page.link", "ajiio.in",
    "bit.ly", "tinyurl.com", "cutt.ly", "t.co", "rb.gy", "shorturl.at"
)

SUPPORTED_DOMAINS = {
    "amazon": ("amazon.in", "amazon.com", "amzn.to", "amzn.in", "a.co"),
    "flipkart": ("flipkart.com", "fkrt.it", "fkrt.cc", "dl.flipkart.com"),
    "myntra": ("myntra.com", "myntr.it"),
    "ajio": ("ajio.com", "ajiio.in", "ajio.page.link"),
    "shorteners": SHORTENER_DOMAINS,
}
ALL_DOMAINS = tuple(d for ds in SUPPORTED_DOMAINS.values() for d in ds)
URL_RE = re.compile(r"https?://[^\s<>\"')\]]+", re.IGNORECASE)
TRAILING_PUNCT = ".,;:!?)]}>'\""

CAPTION_LIMIT = 1024   # Telegram caption limit
MESSAGE_LIMIT = 4096


# --------------------------------------------------------------------------- #
# Shared state: cached user configs
# --------------------------------------------------------------------------- #
@dataclass
class UserConfig:
    user_id: str
    telegram_user_id: str = ""
    target_channel_id: Optional[int | str] = None
    target_channel_name: str = ""
    source_channels: list[str] = field(default_factory=list)
    tracking_sub_id: str = ""
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
            if not (u.is_active and u.target_channel_id and u.tracking_sub_id):
                continue
            if {norm_channel(c) for c in u.source_channels} & source_keys:
                out.append(u)
        return out

    def all_sources(self) -> set[str]:
        s: set[str] = set()
        for u in self.users.values():
            if u.is_active:
                s.update(norm_channel(c) for c in u.source_channels)
        s.discard("")
        return s


store = ConfigStore()


def norm_channel(c) -> str:
    """Normalise a channel reference to a lowercase username or numeric id string."""
    c = str(c or "").strip()
    c = re.sub(r"^https?://t\.me/", "", c, flags=re.I)
    c = c.lstrip("@").split("/")[0].split("?")[0]
    return c.lower()


def parse_target(v):
    if v is None or v == "":
        return None
    s = str(v).strip()
    if re.fullmatch(r"-?\d+", s):
        return int(s)
    return "@" + s.lstrip("@") if not s.startswith("http") else s


# --------------------------------------------------------------------------- #
# HTTP helpers
# --------------------------------------------------------------------------- #
_session: Optional[aiohttp.ClientSession] = None


async def http() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        headers = {"User-Agent": "kamai-telegram-worker/1.0"}
        if WORKER_API_SECRET:
            headers["x-worker-secret"] = WORKER_API_SECRET
        _session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT), headers=headers
        )
    return _session


async def api_verify_sync(user_id: str, telegram_user_id: int | str, telegram_username: Optional[str]) -> dict:
    """POST /api/telegram/verify-sync -> {"success": true, "trackingSubId": "..."}"""
    s = await http()
    payload = {
        "userId": user_id,
        "telegramUserId": str(telegram_user_id),
        "telegramUsername": telegram_username or "",
    }
    async with s.post(VERIFY_URL, json=payload) as r:
        try:
            data = await r.json(content_type=None)
        except Exception:
            data = {"error": await r.text()}
        if not isinstance(data, dict):
            data = {"error": str(data)}
        data["_status"] = r.status
        return data


async def api_fetch_configs(telegram_id: Optional[int | str] = None) -> list[dict]:
    """GET /api/telegram/user-config?activeOnly=true | ?telegramId=<id> -> {"configs": [...]}"""
    s = await http()
    params = {"telegramId": str(telegram_id)} if telegram_id is not None else {"activeOnly": "true"}
    async with s.get(CONFIG_URL, params=params) as r:
        r.raise_for_status()
        data = await r.json(content_type=None)
    configs = data.get("configs") if isinstance(data, dict) else None
    return configs if isinstance(configs, list) else []


def parse_config(raw: dict) -> Optional[UserConfig]:
    try:
        sources = raw.get("sourceChannels") or []
        if isinstance(sources, str):  # defensive: tolerate a comma-separated string
            sources = [x for x in re.split(r"[,\s]+", sources) if x]
        return UserConfig(
            user_id=str(raw["userId"]),
            telegram_user_id=str(raw.get("telegramUserId") or ""),
            target_channel_id=parse_target(raw.get("targetChannelId")),
            target_channel_name=str(raw.get("targetChannelName") or ""),
            source_channels=list(sources),
            tracking_sub_id=str(raw.get("trackingSubId") or ""),
            is_active=bool(raw.get("isActive", True)),
        )
    except Exception:
        log.exception("Bad config row skipped: %r", raw)
        return None


# --------------------------------------------------------------------------- #
# Link extraction + Cuelinks conversion
# --------------------------------------------------------------------------- #
def _host_matches(host: str) -> bool:
    host = host.lower().split(":")[0]
    return any(host == d or host.endswith("." + d) for d in ALL_DOMAINS)


def is_shortened_url(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower().split(":")[0]
        return any(host == d or host.endswith("." + d) for d in SHORTENER_DOMAINS)
    except Exception:
        return False


def is_supported_product_host(host: str) -> bool:
    host = host.lower().split(":")[0]
    product_domains = tuple(
        d for k, ds in SUPPORTED_DOMAINS.items() if k != "shorteners" for d in ds
    )
    return any(host == d or host.endswith("." + d) for d in product_domains)


def extract_deal_urls(text: str) -> list[str]:
    seen, out = set(), []
    for m in URL_RE.finditer(text or ""):
        url = m.group(0).rstrip(TRAILING_PUNCT)
        try:
            host = urlparse(url).netloc
        except ValueError:
            continue
        if _host_matches(host) and url not in seen:
            seen.add(url)
            out.append(url)
    return out


def strip_affiliate_params(url: str) -> str:
    """Drop other people's tracking params so only the user's subid earns."""
    try:
        p = urlparse(url)
        drop = {
            "tag", "affid", "aff_id", "affextparam1", "affextparam2",
            "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
            "subid", "cid", "ref", "ref_", "linkcode", "linkid", "ascsubtag",
            "creative", "camp", "creativeasin"
        }
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
             if k.lower() not in drop and not k.lower().startswith("utm_")]
        return urlunparse(p._replace(query=urlencode(q)))
    except Exception:
        return url


_RESOLVED_CACHE: dict[str, str] = {}


async def unshorten_url(url: str) -> str:
    """Follow redirects for shortened URLs (amzn.to, fkrt.it, bit.ly, etc.) to get destination product URL."""
    if url in _RESOLVED_CACHE:
        return _RESOLVED_CACHE[url]
    if not is_shortened_url(url):
        return url

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    resolved = url
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        timeout = aiohttp.ClientTimeout(total=8, connect=4)
        async with aiohttp.ClientSession(headers=headers, connector=connector, timeout=timeout) as session:
            async with session.get(url, allow_redirects=True) as resp:
                final_url = str(resp.url)
                if final_url and final_url.startswith("http"):
                    resolved = final_url
    except Exception as e:
        log.warning("Could not unshorten %s: %s", url, e)
        resolved = url

    _RESOLVED_CACHE[url] = resolved
    if len(_RESOLVED_CACHE) > 5000:
        _RESOLVED_CACHE.clear()
    return resolved


def to_cuelinks(url: str, sub_id: str) -> str:
    """
    Form the redirect URL correctly:
        https://linksredirect.com/?pub_id={CUELINKS_PUB_ID}&chnl_id={CUELINKS_CHNL_ID}&subid={sub_id}&url={encoded_target_url}
    """
    clean = strip_affiliate_params(url)
    encoded_target_url = quote(clean, safe="")
    return f"https://linksredirect.com/?pub_id={CUELINKS_PUB_ID}&chnl_id={CUELINKS_CHNL_ID}&subid={sub_id}&url={encoded_target_url}"


async def convert_text(text: str, sub_id: str) -> tuple[str, int]:
    urls = extract_deal_urls(text)
    converted_count = 0
    for u in sorted(urls, key=len, reverse=True):
        target_url = await unshorten_url(u) if is_shortened_url(u) else u
        clean_url = strip_affiliate_params(target_url)
        if is_supported_product_host(urlparse(clean_url).netloc) or not is_shortened_url(clean_url):
            cuelink = to_cuelinks(clean_url, sub_id)
            text = text.replace(u, cuelink)
            converted_count += 1
    return text, converted_count


# --------------------------------------------------------------------------- #
# aiogram bot
# --------------------------------------------------------------------------- #
router = Router()

HELP_TEXT = (
    "<b>Kamai Karo Bot</b>\n\n"
    "/start - link your account (use the button on the website)\n"
    "/status - check your connection &amp; config\n"
    "/help - show this message\n\n"
    "Once linked, deals from your chosen source channels are converted to your "
    "affiliate links and posted to your target channel automatically. "
    "Make sure this bot is an <b>admin</b> in your target channel."
)


@router.message(Command("start"))
async def cmd_start(message: Message, command: CommandObject) -> None:
    arg = (command.args or "").strip()
    if not arg.startswith("AUTH_"):
        await message.answer(
            "👋 Welcome! To connect your account, open the website dashboard and "
            f"tap <b>Connect Telegram</b>.\n\n{HELP_TEXT}"
        )
        return

    user_id = arg[len("AUTH_"):]
    if not re.fullmatch(r"[A-Za-z0-9_\-]{1,128}", user_id):
        await message.answer("❌ Invalid link. Please generate a new one from the website.")
        return

    tg_user = message.from_user
    if tg_user is None:
        await message.answer("❌ Couldn't identify your Telegram account.")
        return

    payload = {
        "userId": user_id,
        "authPayload": arg,
        "telegramUserId": str(tg_user.id),
        "telegramUsername": tg_user.username or "",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{BASE_WEB_URL}/api/telegram/verify-sync", json=payload) as resp:
                http_status = resp.status
                try:
                    data = await resp.json()
                except Exception:
                    data = {"message": await resp.text()}

                is_success = (
                    http_status == 200 or 
                    data.get("success") is True or 
                    "successfully" in str(data.get("message", "")).lower()
                )

                if is_success:
                    sub_id = data.get("trackingSubId") or (data.get("user") or {}).get("trackingSubId", "tg_deal")
                    await message.answer(
                        f"✅ <b>Account Connected Successfully!</b>\n\n"
                        f"🔗 <b>Your tracking SubID:</b> <code>{sub_id}</code>\n\n"
                        f"👉 <b>Next step:</b> Apne channel me bot ko Admin banayein aur dashboard me Target Channel save karein.",
                        parse_mode="HTML"
                    )
                    asyncio.create_task(refresh_configs())
                else:
                    err = data.get("error") or data.get("message") or f"HTTP {http_status}"
                    await message.answer(f"❌ Linking failed: {err}")
    except Exception:
        log.exception("verify-sync failed")
        await message.answer("⚠️ Couldn't reach the server. Please try again in a minute.")


@router.message(Command("status"))
async def cmd_status(message: Message) -> None:
    if message.from_user is None:
        return
    try:
        rows = await api_fetch_configs(telegram_id=message.from_user.id)
    except Exception:
        log.exception("status fetch failed")
        await message.answer("⚠️ Couldn't fetch your status right now.")
        return

    mine = [c for c in (parse_config(r) for r in rows) if c]
    if not mine:
        await message.answer("ℹ️ No linked account found. Use the Connect Telegram button on the website.")
        return

    c = mine[0]
    target = c.target_channel_name or c.target_channel_id or "not set"
    await message.answer(
        "📊 <b>Your status</b>\n\n"
        f"Active: {'🟢 Yes' if c.is_active else '🔴 No'}\n"
        f"SubID: <code>{c.tracking_sub_id or 'N/A'}</code>\n"
        f"Target channel: <code>{target}</code>\n"
        f"Source channels: {len(c.source_channels)}\n"
        + "\n".join(f"• {x}" for x in c.source_channels[:15])
    )


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    await message.answer(HELP_TEXT)


# --------------------------------------------------------------------------- #
# Config sync worker
# --------------------------------------------------------------------------- #
tele: Optional[TelegramClient] = None
_watched: set[str] = set()


async def refresh_configs() -> None:
    try:
        rows = await api_fetch_configs()
    except Exception as e:
        log.warning("Config sync failed (keeping previous cache): %s", e)
        return
    users: dict[str, UserConfig] = {}
    for r in rows:
        cfg = parse_config(r)
        if cfg:
            users[cfg.user_id] = cfg
    store.replace(users)
    log.info("Config synced: %d users, %d active sources", len(users), len(store.all_sources()))
    await resolve_sources()


async def config_sync_loop() -> None:
    while True:
        await refresh_configs()
        await asyncio.sleep(SYNC_INTERVAL)


async def resolve_sources() -> None:
    """Warm up Telethon's entity cache for new source channels so events fire."""
    global _watched
    if tele is None:
        return
    for key in store.all_sources() - _watched:
        try:
            await tele.get_input_entity(int(key) if re.fullmatch(r"-?\d+", key) else key)
            _watched.add(key)
            log.info("Watching source: %s", key)
        except Exception as e:
            log.warning("Cannot resolve source %s (is the session account joined?): %s", key, e)
        await asyncio.sleep(1.5)  # be gentle with Telegram rate limits


# --------------------------------------------------------------------------- #
# Telethon scraper & deal processor
# --------------------------------------------------------------------------- #
def chat_keys(chat) -> set[str]:
    keys = set()
    uname = getattr(chat, "username", None)
    if uname:
        keys.add(uname.lower())
    cid = getattr(chat, "id", None)
    if cid is not None:
        keys.update({str(cid), f"-100{cid}"})
    return keys


async def post_to_user(bot: Bot, user: UserConfig, text: str, media_path: Optional[str]) -> None:
    converted, n = await convert_text(text, user.tracking_sub_id)
    if n == 0:
        return
    target = user.target_channel_id
    if media_path:
        caption = converted[:CAPTION_LIMIT]
        from aiogram.types import FSInputFile
        await bot.send_photo(target, FSInputFile(media_path), caption=caption, parse_mode=None)
        if len(converted) > CAPTION_LIMIT:
            await bot.send_message(target, converted[CAPTION_LIMIT:CAPTION_LIMIT + MESSAGE_LIMIT],
                                   parse_mode=None, disable_web_page_preview=True)
    else:
        await bot.send_message(target, converted[:MESSAGE_LIMIT], parse_mode=None)


async def handle_new_post(event, bot: Bot) -> None:
    msg = event.message
    text = msg.message or ""
    # also pull URLs hidden behind text entities (hyperlinks)
    for ent in (msg.entities or []):
        url = getattr(ent, "url", None)
        if url:
            text += "\n" + url
    if not extract_deal_urls(text):
        return

    chat = await event.get_chat()
    users = store.active_for_source(chat_keys(chat))
    if not users:
        return

    # Replace hyperlinked entities' visible text with their URLs so conversion catches them
    display_text = msg.message or ""
    for ent in sorted(msg.entities or [], key=lambda e: -e.offset):
        url = getattr(ent, "url", None)
        if url and _host_matches(urlparse(url).netloc):
            display_text = display_text[:ent.offset] + url + display_text[ent.offset + ent.length:]
    if display_text.strip() == "":
        display_text = text

    # Pre-resolve any shortened URLs in parallel so all users resolve instantly from cache
    deal_urls = extract_deal_urls(display_text)
    if not deal_urls:
        return
    shortened = [u for u in deal_urls if is_shortened_url(u)]
    if shortened:
        await asyncio.gather(*(unshorten_url(u) for u in shortened), return_exceptions=True)

    media_path = None
    try:
        if msg.photo:
            media_path = await msg.download_media(file="/tmp/")
    except Exception:
        log.exception("Media download failed; posting text only")

    try:
        results = await asyncio.gather(
            *(safe_post(bot, u, display_text, media_path) for u in users),
            return_exceptions=True,
        )
        ok = sum(1 for r in results if r is True)
        log.info("Post from %s fanned out: %d/%d delivered", getattr(chat, "username", chat.id), ok, len(users))
    finally:
        if media_path and os.path.exists(media_path):
            try:
                os.remove(media_path)
            except OSError:
                pass


async def safe_post(bot: Bot, user: UserConfig, text: str, media_path: Optional[str]) -> bool:
    """One user's failure must never affect others."""
    try:
        await post_to_user(bot, user, text, media_path)
        return True
    except Exception as e:
        log.error("Post failed for user %s -> %s: %s", user.user_id, user.target_channel_id, e)
        return False


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #
def validate_env() -> None:
    missing = [k for k, v in {
        "BOT_TOKEN": BOT_TOKEN, "TELEGRAM_API_ID": API_ID,
        "TELEGRAM_API_HASH": API_HASH, "TELETHON_SESSION_STRING": SESSION_STRING,
    }.items() if not v]
    if missing:
        raise SystemExit(
            "Missing env vars: " + ", ".join(missing) +
            ("\nRun `python generate_session.py` to create TELETHON_SESSION_STRING." if "TELETHON_SESSION_STRING" in missing else "")
        )


async def run_telethon(bot: Bot) -> None:
    global tele
    while True:
        try:
            tele = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
            await tele.connect()
            if not await tele.is_user_authorized():
                raise SystemExit("TELETHON_SESSION_STRING is invalid/expired. Regenerate it.")

            @tele.on(events.NewMessage(incoming=True))
            async def _on_msg(event):
                if not (event.is_channel or event.is_group):
                    return
                try:
                    await handle_new_post(event, bot)
                except Exception:
                    log.exception("Unhandled error processing post")

            log.info("Telethon connected.")
            await store.loaded.wait()
            await resolve_sources()
            await tele.run_until_disconnected()
        except SystemExit:
            raise
        except Exception:
            log.exception("Telethon crashed; restarting in 10s")
        await asyncio.sleep(10)


async def run_bot(bot: Bot) -> None:
    dp = Dispatcher()
    dp.include_router(router)
    while True:
        try:
            await dp.start_polling(bot, handle_signals=False)
            return
        except Exception:
            log.exception("Bot polling crashed; restarting in 5s")
            await asyncio.sleep(5)


# --------------------------------------------------------------------------- #
# HTTP Health-check server (for Render Web Service)
# --------------------------------------------------------------------------- #
http_runner: Optional[web.AppRunner] = None


async def health_check_handler(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "service": "kamaikaro-worker"})


async def run_http_server() -> None:
    global http_runner
    port = int(os.getenv("PORT", 8080))
    app = web.Application()
    app.router.add_get("/", health_check_handler)
    app.router.add_get("/health", health_check_handler)

    http_runner = web.AppRunner(app)
    await http_runner.setup()
    site = web.TCPSite(http_runner, host="0.0.0.0", port=port)
    await site.start()
    log.info("HTTP health-check server running on port %d", port)
    while True:
        await asyncio.sleep(3600)


async def main() -> None:
    validate_env()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    me = await bot.get_me()
    log.info("Bot online as @%s", me.username)
    try:
        await asyncio.gather(
            run_http_server(),
            run_bot(bot),
            config_sync_loop(),
            run_telethon(bot),
        )
    finally:
        await bot.session.close()
        if _session and not _session.closed:
            await _session.close()
        if tele:
            await tele.disconnect()
        if http_runner:
            await http_runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit) as e:
        if isinstance(e, SystemExit) and e.code:
            raise
