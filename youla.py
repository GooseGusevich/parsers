from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
import re
import sqlite3
import time
from typing import Optional, List, Dict

# --- Telegram ---
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, Bot
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters,
)

# --- Selenium ---
from selenium import webdriver
from selenium.webdriver.common.by import By

# ======= Константы / ключи =======
CONFIG_PATH = "bot.youla.config.json"

KEY_EXPIRY_AT    = "expiry_at"
KEY_EXPIRED_LOCK = "expired_lock"
KEY_WATCH_URL    = "watch_url"
KEY_CHAT_ID      = "chat_id"
KEY_PROXY        = "proxy"

# ======= Контент (ссылки и контакты) =======
HELP_TELEGRAPH_URL    = "https://telegra.ph/your-guide"       # Инструкция
SUPPORT_TELEGRAPH_URL = "https://telegra.ph/your-support"     # Техподдержка

# ======= Валидация =======
def sanitize_token(token: str) -> str:
    token = (token or "").strip()
    if not re.fullmatch(r"\d{6,}:[A-Za-z0-9_-]{30,}", token):
        raise ValueError("Неверный формат токена Telegram.")
    return token

def sanitize_days(days: int) -> int:
    if 0 <= days <= 3650:
        return days
    raise ValueError("--days должен быть 0..3650")

def sanitize_url_youla(url: str) -> Optional[str]:
    if not url:
        return None
    url = url.strip()
    if len(url) > 2000:
        return None
    low = url.lower()
    if not (low.startswith("https://youla.ru") or low.startswith("https://www.youla.ru")):
        return None
    if re.search(r"[;'\\]", url):
        return None
    return url

def sanitize_proxy(proxy: Optional[str]) -> Optional[str]:
    if not proxy:
        return None
    s = proxy.strip()
    if len(s) > 300:
        return None
    if not re.fullmatch(r"(http|https|socks5)://[^\s@/:]+(?::[^\s@/:]+)?@?[A-Za-z0-9\.\-\[\]]+(?::\d{2,5})?", s):
        return None
    if re.search(r"[;'\\]", s):
        return None
    return s

def safe_url_display(url: str) -> str:
    if not url:
        return "— не задан —"
    return (url
        .replace("https://youla.ru", "https://you\u200bla.ru")
        .replace("https://www.youla.ru", "https://www.you\u200bla.ru")
    )

# ======= Конфиг =======
@dataclass
class Config:
    token: str
    db: str
    days: int
    proxy: Optional[str]
    interval: int = 5
    max_items: int = 5
    profile_path: str = os.path.expanduser("~/.config/google-chrome/Default")

def _cfg_load(path: str) -> Optional[Config]:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        d = json.load(f)
    return Config(
        token=d["token"], db=d["db"], days=int(d["days"]),
        proxy=d.get("proxy"),
        interval=int(d.get("interval", 5)),
        max_items=int(d.get("max_items", 5)),
        profile_path=d.get("profile_path", os.path.expanduser("~/.config/google-chrome/Default")),
    )

def _cfg_save(path: str, cfg: Config) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump({
            "token": cfg.token, "db": cfg.db, "days": cfg.days,
            "proxy": cfg.proxy, "interval": cfg.interval,
            "max_items": cfg.max_items, "profile_path": cfg.profile_path,
        }, f, ensure_ascii=False, indent=2)

def resolve_config() -> Config:
    p = argparse.ArgumentParser(description="Youla бот (меню + парсер)")
    p.add_argument("--token", help="Telegram Bot API token (единый)")
    p.add_argument("--db", help="Path to sqlite DB")
    p.add_argument("--days", type=int, help="Сколько дней действует услуга (после — блок навсегда)")
    p.add_argument("--proxy", help="Прокси парсера (http/https/socks5://user:pass@host:port)", default=None)
    p.add_argument("--interval", type=int, help="Интервал опроса (сек)", default=None)
    p.add_argument("--max-items", type=int, help="Сколько карточек смотреть", default=None)
    p.add_argument("--profile-path", help="Chrome user-data-dir", default=None)
    args = p.parse_args()

    file_cfg = _cfg_load(CONFIG_PATH)
    token = args.token or (file_cfg.token if file_cfg else None)
    db    = args.db    or (file_cfg.db    if file_cfg else None)
    days  = args.days if args.days is not None else (file_cfg.days if file_cfg else None)
    proxy = args.proxy if args.proxy is not None else (file_cfg.proxy if file_cfg else None)
    interval = args.interval if args.interval is not None else (file_cfg.interval if file_cfg else 5)
    max_items = args.max_items if args.max_items is not None else (file_cfg.max_items if file_cfg else 5)
    profile_path = args.profile_path if args.profile_path is not None else (file_cfg.profile_path if file_cfg else os.path.expanduser("~/.config/google-chrome/Default"))

    if not token or not db or days is None:
        raise SystemExit("Первый запуск: --token --db --days [--proxy --interval --max-items --profile-path]. Далее можно без аргументов (bot.youla.config.json).")

    token = sanitize_token(token)
    days = sanitize_days(int(days))
    proxy = sanitize_proxy(proxy)
    cfg = Config(token=token, db=db, days=days, proxy=proxy, interval=interval, max_items=max_items, profile_path=profile_path)
    if (not file_cfg) or (cfg != file_cfg):
        try: _cfg_save(CONFIG_PATH, cfg)
        except Exception as e: print(f"[WARN] Не удалось сохранить конфиг: {e}")
    return cfg

# ======= БД =======
CREATE_URLS = "CREATE TABLE IF NOT EXISTS urls (id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE NOT NULL, added_at TIMESTAMP NOT NULL);"
CREATE_SETTINGS = "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);"
CREATE_ADS = """
CREATE TABLE IF NOT EXISTS ads (
  ad_id TEXT PRIMARY KEY,
  title TEXT,
  price TEXT,
  href TEXT,
  city TEXT,
  created_at TIMESTAMP
);
"""

class Storage:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute(CREATE_URLS)
        self.conn.execute(CREATE_SETTINGS)
        self.conn.execute(CREATE_ADS)
        self.conn.commit()

    def set_kv(self, key: str, value: str):
        self.conn.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value)
        ); self.conn.commit()

    def get_kv(self, key: str) -> Optional[str]:
        cur = self.conn.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = cur.fetchone()
        return row[0] if row else None

    def del_kv(self, key: str):
        self.conn.execute("DELETE FROM settings WHERE key=?", (key,)); self.conn.commit()

    # expiry/lock
    def ensure_expiry_once(self, days: int):
        if self.get_kv(KEY_EXPIRY_AT):
            return
        tz = ZoneInfo("Europe/Moscow")
        expiry = (datetime.now(tz) + timedelta(days=days)).isoformat()
        self.set_kv(KEY_EXPIRY_AT, expiry)
        self.set_kv(KEY_EXPIRED_LOCK, "false")

    def get_expiry(self) -> Optional[datetime]:
        raw = self.get_kv(KEY_EXPIRY_AT)
        if not raw: return None
        try: return datetime.fromisoformat(raw)
        except Exception: return None

    def is_locked(self) -> bool:
        return self.get_kv(KEY_EXPIRED_LOCK) == "true"

    def lock_forever(self):
        self.set_kv(KEY_EXPIRED_LOCK, "true")

    # watch url
    def set_watch_url(self, url: str) -> tuple[bool, str]:
        s = sanitize_url_youla(url)
        if not s: return False, "Недопустимый URL (только https://youla.ru/ ... )"
        self.set_kv(KEY_WATCH_URL, s)
        try:
            self.conn.execute("INSERT OR IGNORE INTO urls(url,added_at) VALUES(?,?)", (s, datetime.now()))
            self.conn.commit()
        except Exception:
            pass
        return True, "URL установлен"

    def get_watch_url(self) -> Optional[str]:
        return self.get_kv(KEY_WATCH_URL)

    # ads
    def is_new_ad(self, ad_id: str) -> bool:
        cur = self.conn.execute("SELECT 1 FROM ads WHERE ad_id=?", (ad_id,))
        return cur.fetchone() is None

    def save_ad(self, ad: Dict):
        self.conn.execute(
            "INSERT OR IGNORE INTO ads(ad_id,title,price,href,city,created_at) VALUES(?,?,?,?,?,?)",
            (ad["id"], ad["title"], ad["price"], ad["href"], ad["city"], datetime.now())
        )
        self.conn.commit()

# ======= Кнопки / текст =======
BTN_TOGGLE  = "toggle"
BTN_SET_URL = "set_url"
# BTN_HELP и BTN_SUPPORT — url-кнопки

def build_menu_kb(running: bool) -> InlineKeyboardMarkup:
    toggle_text = "⏹ Стоп" if running else "▶️ Старт"
    help_btn = InlineKeyboardButton("📘 Инструкция", url=HELP_TELEGRAPH_URL)
    support_btn = InlineKeyboardButton("👨‍💻 Техподдержка", url=SUPPORT_TELEGRAPH_URL)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(toggle_text, callback_data=BTN_TOGGLE)],
        [InlineKeyboardButton("🔗 Назначить/поменять URL", callback_data=BTN_SET_URL)],
        [help_btn],
        [support_btn],
    ])

def format_status_text(store: Storage, running: bool) -> str:
    tz = ZoneInfo("Europe/Moscow")
    now = datetime.now(tz)
    raw_url = store.get_watch_url()
    if raw_url:
        url = f'<a href="{raw_url}">клик</a>'
    else:
        url = "— не задан —"
    expiry = store.get_expiry()
    locked = store.is_locked()

    if locked:
        countdown = "⛔️ истёк — заблокировано навсегда"
    elif expiry:
        if expiry.tzinfo is None: expiry = expiry.replace(tzinfo=tz)
        sec = int((expiry - now).total_seconds())
        if sec <= 0:
            countdown = "⛔️ истёк — блокировка включена"
        else:
            days = sec // 86_400
            hrs  = (sec % 86_400) // 3600
            mins = (sec % 3600) // 60
            s    = sec % 60
            countdown = f"{days} д {hrs:02d} ч {mins:02d} м {s:02d} с"
    else:
        countdown = "—"

    text = (
        "⚙️ <b>Меню управления</b> (Youla)\n\n"
        f"🔗 <b>URL</b>: {url}\n"
        f"⏳ <b>Осталось</b>: {countdown}\n"
        f"🤖 <b>Парсер</b>: {'работает' if running else 'остановлен'}"
    )
    return text

# ======= Парсер (внутри процесса) =======
def build_chrome_options(profile_path: str, proxy: Optional[str]) -> webdriver.ChromeOptions:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    if proxy:
        options.add_argument(f"--proxy-server={proxy}")
    if profile_path and os.path.exists(profile_path):
        options.add_argument(f"--user-data-dir={profile_path}")
    return options

def collect_ads(url: str, max_items: int, profile_path: str, proxy: Optional[str]) -> List[Dict]:
    """Сбор объявлений с youla.ru. Селекторы с запасными вариантами."""
    options = build_chrome_options(profile_path, proxy)
    driver = webdriver.Chrome(options=options)
    ads: List[Dict] = []
    try:
        driver.get(url)
        time.sleep(5)

        # Основной список карточек
        items = driver.find_elements(By.CSS_SELECTOR, "figure[data-test-component='ProductCard']")
        if not items:
            items = driver.find_elements(By.CSS_SELECTOR, "a[data-test-component='ProductCard'] figure")
        items = items[:max_items]

        for item in items:
            # href
            try:
                href = item.find_element(By.XPATH, "./ancestor::a").get_attribute("href") or ""
            except Exception:
                href = ""
            # id
            ad_id = item.get_attribute("data-test-id")
            if not ad_id:
                m = re.search(r"/product/(\w+)", href)
                ad_id = m.group(1) if m else str(abs(hash(href)))
            # title
            title = ""
            for sel in ("[data-test-block='ProductName']", "[itemprop='name']", ".aKcZzY"):
                try:
                    title = item.find_element(By.CSS_SELECTOR, sel).text.strip()
                    if title: break
                except Exception:
                    pass
            # price
            price = ""
            for sel in ("[data-test-block='ProductPrice']", "[itemprop='price']", ".dRy8nq"):
                try:
                    price = item.find_element(By.CSS_SELECTOR, sel).text.strip()
                    if price: break
                except Exception:
                    pass
            if not price: price = "цена не найдена"
            # city
            city = ""
            for sel in ("span.gRdgms", "[data-test-component='Geo'] span", "[data-test-component='Geo']"):
                try:
                    city = item.find_element(By.CSS_SELECTOR, sel).text.strip()
                    if city: break
                except Exception:
                    pass
            if not city: city = "город не найден"
            # image
            img = ""
            try:
                img = item.find_element(By.CSS_SELECTOR, "image").get_attribute("xlink:href") or ""
            except Exception:
                try:
                    img = item.find_element(By.CSS_SELECTOR, "img").get_attribute("src") or ""
                except Exception:
                    pass

            ads.append({"id": ad_id, "title": title or "без названия", "href": href,
                        "price": price, "city": city, "image": img})
    except Exception as e:
        print(f"[WARN] Ошибка парсинга: {e}")
    finally:
        driver.quit()
    return ads

class ParserService:
    def __init__(self, bot: Bot, store: Storage, proxy: Optional[str], interval: int, max_items: int, profile_path: str):
        self.bot = bot
        self.store = store
        self.proxy = proxy
        self.interval = max(2, int(interval))
        self.max_items = max(1, int(max_items))
        self.profile_path = profile_path
        self._task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def _send_ad(self, chat_id: str, ad: Dict, new=True):
        msk_time = datetime.now(ZoneInfo("Europe/Moscow")).strftime("%H:%M:%S")
        status = "🔥 НОВОЕ ОБЪЯВЛЕНИЕ!" if new else "ℹ️ Уже в базе"
        caption = (
            f"🕒 <b>{msk_time} MSK</b> | {status}\n\n"
            f"📌 <b>{ad['title']}</b>\n"
            f"💰 Цена: {ad['price']}\n"
            f"🏙 Город: {ad.get('city', 'не указан')}\n"
            f"🔗 <a href='{ad['href']}'>ссылка на объявление</a>\n"
        )
        if ad.get("image"):
            try:
                await self.bot.send_photo(chat_id=chat_id, photo=ad["image"], caption=caption, parse_mode="HTML")
                return
            except Exception as e:
                print(f"[WARN] send_photo failed: {e}; fallback to text")
        try:
            await self.bot.send_message(chat_id=chat_id, text=caption, parse_mode="HTML", disable_web_page_preview=True)
        except Exception as e:
            print(f"[WARN] send_message failed: {e}")

    async def _run(self):
        # уведомим чат при старте
        chat_id = self.store.get_kv(KEY_CHAT_ID)
        if chat_id:
            try:
                await self.bot.send_message(chat_id=chat_id, text="Парсер Youla запущен ✅")
            except Exception as e:
                print(f"[WARN] notify start failed: {e}")

        stop_event = self._stop_event
        assert stop_event is not None
        try:
            while not stop_event.is_set():
                url = self.store.get_kv(KEY_WATCH_URL)
                chat_id = self.store.get_kv(KEY_CHAT_ID)
                if not url or not chat_id:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=self.interval)
                    except asyncio.TimeoutError:
                        continue

                ads = collect_ads(url, self.max_items, self.profile_path, self.proxy)

                for ad in reversed(ads):
                    try:
                        if self.store.is_new_ad(ad["id"]):
                            await self._send_ad(chat_id, ad, new=True)
                            self.store.save_ad(ad)
                    except Exception as e:
                        print(f"[WARN] send/save failed: {e}")

                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self.interval)
                except asyncio.TimeoutError:
                    pass
        except Exception as e:
            print(f"[ERROR] parser loop: {e}")

    def start(self):
        if self.is_running():
            return
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        if not self.is_running():
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._task, timeout=self.interval + 5)
        except asyncio.TimeoutError:
            pass
        self._task = None
        self._stop_event = None

# ======= Handlers =======
async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    store: Storage = context.bot_data["store"]
    svc: ParserService = context.bot_data["parser"]
    # зафиксируем chat_id первого пользователя
    chat_id = str(update.effective_chat.id)
    if not store.get_kv(KEY_CHAT_ID):
        store.set_kv(KEY_CHAT_ID, chat_id)

    text = format_status_text(store, svc.is_running())
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=text,
        reply_markup=build_menu_kb(svc.is_running()),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_menu(update, context)

async def menu_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    store: Storage = context.bot_data["store"]
    cfg: Config = context.bot_data["cfg"]
    svc: ParserService = context.bot_data["parser"]
    q = update.callback_query; await q.answer()
    data = q.data

    tz = ZoneInfo("Europe/Moscow")
    now = datetime.now(tz)
    expiry = store.get_expiry()
    locked = store.is_locked()
    running = svc.is_running()

    async def warn_and_menu(msg: str):
        text = format_status_text(store, svc.is_running())
        full_text = f"{msg}\n\n{text}"
        await q.edit_message_text(
            full_text,
            reply_markup=build_menu_kb(svc.is_running()),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

    if data == BTN_TOGGLE:
        if locked or (expiry and now >= expiry):
            if running:
                await svc.stop()
            if not locked:
                store.lock_forever()
            await q.edit_message_text("⛔️ Срок услуги истёк. Перезапуск запрещён навсегда.",
                                      disable_web_page_preview=True)
            return

        # нужен URL
        if not store.get_watch_url():
            await warn_and_menu("⚠️ Сначала назначьте URL (кнопка «🔗 Назначить/поменять URL»).\nПример: https://youla.ru/kirovskaya_oblast_kirov/auto?attributes[sort_field]=date_published")
            return
        # нужен chat_id
        if not store.get_kv(KEY_CHAT_ID):
            await warn_and_menu("⚠️ Нажмите /start в этом чате, чтобы привязать chat_id.")
            return

        if running:
            await svc.stop()
            text = format_status_text(store, False)
            await q.edit_message_text("⏹ Парсер остановлен.\n\n" + text,
                                      reply_markup=build_menu_kb(False),
                                      parse_mode="HTML",
                                      disable_web_page_preview=True)
        else:
            if cfg.proxy: store.set_kv(KEY_PROXY, cfg.proxy)  # фиксируем прокси в БД (опционально)
            svc.start()
            text = format_status_text(store, True)
            await q.edit_message_text("▶️ Парсер запущен.\n\n" + text,
                                      reply_markup=build_menu_kb(True),
                                      parse_mode="HTML",
                                      disable_web_page_preview=True)
        return

    if data == BTN_SET_URL:
        if locked or (expiry and now >= expiry):
            await q.edit_message_text("⛔️ Срок услуги истёк. Управление недоступно.",
                                      disable_web_page_preview=True)
        else:
            context.user_data["mode"] = BTN_SET_URL
            await q.edit_message_text("Пришли один URL (только https://youla.ru/...):",
                                      disable_web_page_preview=True)
        return

async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    store: Storage = context.bot_data["store"]
    svc: ParserService = context.bot_data["parser"]
    mode = context.user_data.get("mode")
    text_in = (update.message.text or "").strip()

    tz = ZoneInfo("Europe/Moscow")
    now = datetime.now(tz)
    expiry = store.get_expiry()
    if store.is_locked() or (expiry and now >= expiry):
        await update.message.reply_text("⛔️ Срок услуги истёк. Управление недоступно.")
        return

    # фиксируем chat_id первого, кто пишет
    if not store.get_kv(KEY_CHAT_ID):
        store.set_kv(KEY_CHAT_ID, str(update.effective_chat.id))

    if mode == BTN_SET_URL:
        ok, msg = store.set_watch_url(text_in)
        text = format_status_text(store, svc.is_running())
        await update.message.reply_text(
            ("✅ " if ok else "⚠️ ") + msg + "\n\n" + text,
            reply_markup=build_menu_kb(svc.is_running()),
            parse_mode="HTML", disable_web_page_preview=True
        )
        context.user_data["mode"] = None
        return

    # по умолчанию — показать меню
    await show_menu(update, context)

# ======= Watchdog истечения (через JobQueue) =======
async def expiry_job(context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    store: Storage = app.bot_data["store"]
    svc: ParserService = app.bot_data["parser"]
    try:
        tz = ZoneInfo("Europe/Moscow")
        now = datetime.now(tz)
        expiry = store.get_expiry()
        locked = store.is_locked()
        if not locked and expiry and now >= expiry:
            if svc.is_running():
                await svc.stop()
            store.lock_forever()
            print("[INFO] Срок истёк: парсер остановлен и заблокирован навсегда.")
    except Exception as e:
        print(f"[WARN] watchdog error: {e}")

# ======= Main =======
def main():
    cfg = resolve_config()
    application = ApplicationBuilder().token(cfg.token).build()
    store = Storage(cfg.db)
    store.ensure_expiry_once(cfg.days)

    parser_service = ParserService(
        bot=application.bot,
        store=store,
        proxy=cfg.proxy,
        interval=cfg.interval,
        max_items=cfg.max_items,
        profile_path=cfg.profile_path,
    )

    application.bot_data["cfg"] = cfg
    application.bot_data["store"] = store
    application.bot_data["parser"] = parser_service

    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CallbackQueryHandler(menu_button))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message))

    # планируем джобу контроля истечения
    application.job_queue.run_repeating(expiry_job, interval=30, first=5)

    # запускаем
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
