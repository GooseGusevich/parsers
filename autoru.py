from __future__ import annotations

import argparse
import asyncio
import html as html_mod
import json
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict
from zoneinfo import ZoneInfo

# --- Telegram ---
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, Bot
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters,
)

# --- Selenium ---
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ======= Константы / ключи =======
CONFIG_PATH = "bot.auto.config.json"

KEY_EXPIRY_AT     = "expiry_at"
KEY_EXPIRED_LOCK  = "expired_lock"
KEY_WATCH_URL     = "watch_url"
KEY_CHAT_ID       = "chat_id"
KEY_PROXY         = "proxy"
KEY_CHROMEDRIVER  = "chromedriver_path"
KEY_PROFILE_PATH  = "profile_path"

MSK = ZoneInfo("Europe/Moscow")

HELP_TELEGRAPH_URL    = "https://telegra.ph/your-guide"   # Инструкция
SUPPORT_TELEGRAPH_URL = "https://telegra.ph/your-support" # Техподдержка

RU_MONTHS = {
    'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
    'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
}
_DEF_DATE_XPATH = (
    "//*[contains(@class,'CardHead__creationDate')] | "
    "//*[@title[contains(.,'Дата размещения объявления')]]"
)

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

def sanitize_url_auto(url: str) -> Optional[str]:
    if not url:
        return None
    url = url.strip()
    if len(url) > 2000:
        return None
    low = url.lower()
    if not (low.startswith("https://auto.ru") or low.startswith("https://www.auto.ru")):
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

def sanitize_path(path: Optional[str]) -> Optional[str]:
    if not path:
        return None
    return os.path.expanduser(path.strip())

# ======= Конфиг =======
@dataclass
class Config:
    token: str
    db: str
    days: int
    proxy: Optional[str]
    interval: int = 60
    max_items: int = 5
    profile_path: Optional[str] = os.path.expanduser("~/.config/google-chrome/Default")
    chromedriver_path: Optional[str] = None
    headless: bool = True
    fresh_days: int = 0            # 0=только сегодня; 1=сегодня+вчера; ...
    send_screenshot: bool = False  # слать скрин страницы объявления
    screenshot_folder: str = "screenshots"
    warmup_runs: int = 0           # если БД пуста — сколько циклов просто заполнить БД
    warmup_delay: int = 5

def _cfg_load(path: str) -> Optional[Config]:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        d = json.load(f)
    return Config(
        token=d["token"], db=d["db"], days=int(d["days"]),
        proxy=d.get("proxy"),
        interval=int(d.get("interval", 60)),
        max_items=int(d.get("max_items", 5)),
        profile_path=d.get("profile_path", os.path.expanduser("~/.config/google-chrome/Default")),
        chromedriver_path=d.get("chromedriver_path"),
        headless=bool(d.get("headless", True)),
        fresh_days=int(d.get("fresh_days", 0)),
        send_screenshot=bool(d.get("send_screenshot", False)),
        screenshot_folder=d.get("screenshot_folder", "screenshots"),
        warmup_runs=int(d.get("warmup_runs", 0)),
        warmup_delay=int(d.get("warmup_delay", 5)),
    )

def _cfg_save(path: str, cfg: Config) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump({
            "token": cfg.token, "db": cfg.db, "days": cfg.days,
            "proxy": cfg.proxy, "interval": cfg.interval,
            "max_items": cfg.max_items, "profile_path": cfg.profile_path,
            "chromedriver_path": cfg.chromedriver_path, "headless": cfg.headless,
            "fresh_days": cfg.fresh_days, "send_screenshot": cfg.send_screenshot,
            "screenshot_folder": cfg.screenshot_folder,
            "warmup_runs": cfg.warmup_runs, "warmup_delay": cfg.warmup_delay,
        }, f, ensure_ascii=False, indent=2)

def resolve_config() -> Config:
    p = argparse.ArgumentParser(description="auto.ru бот (меню + парсер)")
    p.add_argument("--token", help="Telegram Bot API token (единый)")
    p.add_argument("--db", help="Path to sqlite DB")
    p.add_argument("--days", type=int, help="Сколько дней действует услуга (после — блок навсегда)")
    p.add_argument("--proxy", help="Прокси (http/https/socks5://user:pass@host:port)")
    p.add_argument("--interval", type=int, help="Интервал опроса (сек)")
    p.add_argument("--max-items", type=int, help="Сколько карточек смотреть")
    p.add_argument("--profile-path", help="Chrome user-data-dir")
    p.add_argument("--chromedriver", help="Путь к chromedriver")
    p.add_argument("--no-headless", help="Показывать окно браузера", action="store_true")
    p.add_argument("--fresh-days", type=int, help="Фильтр свежести по дате публикации (дней, от 0)")
    p.add_argument("--send-screenshot", action="store_true", help="Отправлять скрин страницы объявления")
    p.add_argument("--screenshot-folder", help="Папка для скринов")
    p.add_argument("--warmup-runs", type=int, help="Разогрев: циклов заполнения БД без отправок (если БД пуста)")
    p.add_argument("--warmup-delay", type=int, help="Пауза между разогрев-циклами, сек")
    args = p.parse_args()

    file_cfg = _cfg_load(CONFIG_PATH)
    token = args.token or (file_cfg.token if file_cfg else None)
    db    = args.db    or (file_cfg.db    if file_cfg else None)
    days  = args.days if args.days is not None else (file_cfg.days if file_cfg else None)
    proxy = args.proxy if args.proxy is not None else (file_cfg.proxy if file_cfg else None)
    interval = args.interval if args.interval is not None else (file_cfg.interval if file_cfg else 60)
    max_items = args.max_items if args.max_items is not None else (file_cfg.max_items if file_cfg else 5)
    profile_path = args.profile_path if args.profile_path is not None else (file_cfg.profile_path if file_cfg else os.path.expanduser("~/.config/google-chrome/Default"))
    chromedriver = args.chromedriver if args.chromedriver is not None else (file_cfg.chromedriver_path if file_cfg else None)
    headless = file_cfg.headless if file_cfg else True
    if args.no_headless:
        headless = False
    fresh_days = args.fresh_days if args.fresh_days is not None else (file_cfg.fresh_days if file_cfg else 0)
    send_screenshot = True if args.send_screenshot else (file_cfg.send_screenshot if file_cfg else False)
    screenshot_folder = args.screenshot_folder if args.screenshot_folder is not None else (file_cfg.screenshot_folder if file_cfg else "screenshots")
    warmup_runs = args.warmup_runs if args.warmup_runs is not None else (file_cfg.warmup_runs if file_cfg else 0)
    warmup_delay = args.warmup_delay if args.warmup_delay is not None else (file_cfg.warmup_delay if file_cfg else 5)

    if not token or not db or days is None:
        raise SystemExit("Первый запуск: --token --db --days [доп. опции]. Далее можно без аргументов (bot.auto.config.json).")

    token = sanitize_token(token)
    days = sanitize_days(int(days))
    proxy = sanitize_proxy(proxy)
    profile_path = sanitize_path(profile_path)
    chromedriver = sanitize_path(chromedriver)

    cfg = Config(
        token=token, db=db, days=days, proxy=proxy, interval=interval, max_items=max_items,
        profile_path=profile_path, chromedriver_path=chromedriver, headless=headless,
        fresh_days=fresh_days, send_screenshot=send_screenshot, screenshot_folder=screenshot_folder,
        warmup_runs=warmup_runs, warmup_delay=warmup_delay
    )
    if (not file_cfg) or (cfg != file_cfg):
        try: _cfg_save(CONFIG_PATH, cfg)
        except Exception as e: print(f"[WARN] Не удалось сохранить конфиг: {e}")
    return cfg

# ======= БД =======
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

    def ensure_expiry_once(self, days: int):
        if self.get_kv(KEY_EXPIRY_AT):
            return
        expiry = (datetime.now(MSK) + timedelta(days=days)).isoformat()
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

    def set_watch_url(self, url: str) -> tuple[bool, str]:
        s = sanitize_url_auto(url)
        if not s: return False, "Недопустимый URL (только https://auto.ru/ ... )"
        self.set_kv(KEY_WATCH_URL, s)
        return True, "URL установлен"

    def get_watch_url(self) -> Optional[str]:
        return self.get_kv(KEY_WATCH_URL)

    def is_new_ad(self, ad_id: str) -> bool:
        cur = self.conn.execute("SELECT 1 FROM ads WHERE ad_id=?", (ad_id,))
        return cur.fetchone() is None

    def save_ad(self, ad: Dict):
        self.conn.execute(
            "INSERT OR IGNORE INTO ads(ad_id,title,price,href,city,created_at) VALUES(?,?,?,?,?,?)",
            (ad["id"], ad["title"], ad["price"], ad["href"], ad["city"], datetime.now(MSK))
        ); self.conn.commit()

# ======= Кнопки / текст =======
BTN_TOGGLE  = "toggle"
BTN_SET_URL = "set_url"

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
    raw_url = store.get_watch_url()
    url_link = f'<a href="{raw_url}">клик</a>' if raw_url else "— не задан —"
    expiry = store.get_expiry()
    locked = store.is_locked()

    if locked:
        countdown = "⛔️ истёк — заблокировано навсегда"
    elif expiry:
        sec = int((expiry - datetime.now(MSK)).total_seconds())
        countdown = "⛔️ истёк — блокировка включена" if sec <= 0 else \
            f"{sec // 86400} д {(sec % 86400)//3600:02d} ч {(sec % 3600)//60:02d} м {sec % 60:02d} с"
    else:
        countdown = "—"

    return (
        "⚙️ <b>Меню управления</b> (auto.ru)\n\n"
        f"🔗 <b>URL</b>: {url_link}\n"
        f"⏳ <b>Осталось</b>: {countdown}\n"
        f"🤖 <b>Парсер</b>: {'работает' if running else 'остановлен'}"
    )

# ======= Selenium / утилиты =======
def build_chrome_options(profile_path: Optional[str], proxy: Optional[str], headless: bool) -> webdriver.ChromeOptions:
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1280,2200")
    options.add_argument("--lang=ru-RU")
    if proxy:
        options.add_argument(f"--proxy-server={proxy}")
    if profile_path and os.path.exists(profile_path):
        options.add_argument(f"--user-data-dir={profile_path}")
    return options

def build_driver(profile_path: Optional[str], proxy: Optional[str], chromedriver_path: Optional[str], headless: bool) -> webdriver.Chrome:
    options = build_chrome_options(profile_path, proxy, headless)
    svc = Service(chromedriver_path) if chromedriver_path else None
    return webdriver.Chrome(service=svc, options=options) if svc else webdriver.Chrome(options=options)

def get_unique_id(link: str) -> str:
    m = re.search(r'/(\d+)-', link)   # частый паттерн auto.ru
    return m.group(1) if m else str(abs(hash(link)))

PRICE_SELECTORS = [
    "div.ListingItemUniversalPrice__highlighted-m4qQj",
    "div.ListingItemUniversalPrice__title-Mi4tV div.Typography2__h5-mkmlZ",
    "div.ListingItemUniversalPrice-kYWDN div.Typography2__h5-mkmlZ"
]

def parse_price(parent) -> str:
    for selector in PRICE_SELECTORS:
        try:
            elem = parent.find_element(By.CSS_SELECTOR, selector)
            text = (elem.text or "").strip()
            if text:
                return text.replace("\u00a0", " ")
        except Exception:
            continue
    return "Цена не найдена"

def parse_ru_day_month_to_date(s: str) -> Optional[date]:
    s = (s or "").strip().lower()
    m = re.search(r"([0-3]?\d)\s+([а-яё]+)", s)
    if not m:
        return None
    d = int(m.group(1))
    mon = RU_MONTHS.get(m.group(2))
    if not mon:
        return None
    now = datetime.now(MSK)
    y = now.year
    try:
        dt = date(y, mon, d)
    except Exception:
        return None
    if dt > now.date():
        dt = date(y - 1, mon, d)
    return dt

def is_fresh(created: Optional[date], fresh_days: int) -> bool:
    if created is None:
        return False
    today = datetime.now(MSK).date()
    delta = today - created
    return timedelta(0) <= delta <= timedelta(days=fresh_days)

# ======= Парсинг списка объявлений =======
def collect_ads(url: str, max_items: int, profile_path: Optional[str], proxy: Optional[str],
                chromedriver_path: Optional[str], headless: bool) -> List[Dict]:
    driver = build_driver(profile_path, proxy, chromedriver_path, headless)
    ads: List[Dict] = []
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 25)
        items = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.ListingItemTitle__link")))[:max_items]
        for item in items:
            title = item.text or "Название не найдено"
            href = item.get_attribute("href") or ""
            ad_id = get_unique_id(href)
            # родительская карточка
            try:
                card = item.find_element(By.XPATH, "./ancestor::div[contains(@class,'ListingItem')]")
            except Exception:
                card = item
            # город
            try:
                city_elems = card.find_elements(By.CSS_SELECTOR, "div.ListingItem__regionName, span.MetroListPlace__regionName")
                city = city_elems[0].text.strip() if city_elems else "город не найден"
            except Exception:
                city = "город не найден"
            # цена
            price = parse_price(card)
            ads.append({"id": ad_id, "title": title, "href": href, "price": price, "city": city})
    except Exception as e:
        print(f"[ERROR collect_ads] {e}")
    finally:
        try: driver.quit()
        except Exception: pass
    return ads

# ======= Детали объявления: ТОЛЬКО дата (+скрин при опции), БЕЗ описания =======
def fetch_ad_details_sync(ad: Dict, cfg: Config) -> tuple[Optional[str], Optional[date]]:
    driver = None
    try:
        os.makedirs(cfg.screenshot_folder, exist_ok=True)
        driver = build_driver(cfg.profile_path, cfg.proxy, cfg.chromedriver_path, cfg.headless)
        driver.get(ad["href"])
        time.sleep(3)

        # Дата размещения
        created_dt = None
        try:
            node = driver.find_element(By.XPATH, _DEF_DATE_XPATH)
            raw_title = (node.get_attribute("title") or "").strip()
            if "Дата размещения объявления" in raw_title:
                raw_text = raw_title.split("Дата размещения объявления", 1)[-1].strip()
            else:
                raw_text = (node.text or "").strip()
        except Exception:
            raw_text = None
        if not raw_text:
            try:
                html_src = driver.page_source
                m = re.search(r"Дата размещения объявления\s*([0-3]?\d\s+[А-Яа-яё]+)", html_src)
                if m:
                    raw_text = m.group(1)
            except Exception:
                raw_text = None
        if raw_text:
            created_dt = parse_ru_day_month_to_date(raw_text)

        # Скриншот всей страницы (если нужно)
        screenshot_path = None
        if cfg.send_screenshot:
            total_height = driver.execute_script(
                "return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight)"
            )
            driver.set_window_size(1920, max(2200, int(total_height or 2200)))
            time.sleep(1)
            screenshot_path = os.path.join(cfg.screenshot_folder, f"{ad['id']}.png")
            driver.save_screenshot(screenshot_path)

        return screenshot_path, created_dt
    except Exception as e:
        print(f"[ERROR fetch_ad_details] {e}")
        return None, None
    finally:
        try:
            if driver: driver.quit()
        except Exception:
            pass

# ======= Сервис парсинга =======
class ParserService:
    def __init__(self, bot: Bot, store: Storage, cfg: Config):
        self.bot = bot
        self.store = store
        self.cfg = cfg
        self._task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def _send_ad(self, chat_id: str, ad: Dict, created: Optional[date], screenshot_path: Optional[str]):
        msk_time = datetime.now(MSK).strftime("%H:%M:%S")
        created_line = f"📅 Размещено: {created.strftime('%d.%m.%Y')}\n" if created else ""
        text = (
            f"🕒 <b>{msk_time} MSK</b> | 🔥 НОВОЕ ОБЪЯВЛЕНИЕ!\n\n"
            f"📌 <b>{html_mod.escape(ad['title'])}</b>\n"
            f"💰 Цена: {html_mod.escape(ad['price'])}\n"
            f"🏙 Город: {html_mod.escape(ad.get('city', 'не указан'))}\n"
            f"{created_line}"
            f"🔗 <a href='{ad['href']}'>ссылка на объявление</a>\n"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Открыть объявление", url=ad["href"])]])
        if self.cfg.send_screenshot and screenshot_path and os.path.exists(screenshot_path):
            try:
                with open(screenshot_path, "rb") as f:
                    await self.bot.send_photo(chat_id=chat_id, photo=f, caption=text, parse_mode="HTML", reply_markup=kb)
                return
            except Exception as e:
                print(f"[WARN] send_photo failed: {e}; fallback to text")
        await self.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", disable_web_page_preview=False, reply_markup=kb)

    async def _warmup_if_needed(self):
        cur = self.store.conn.execute("SELECT COUNT(*) FROM ads")
        count = int(cur.fetchone()[0])
        if count == 0 and self.cfg.warmup_runs > 0:
            print(f"[WARMUP] Пустая БД. Разогрев на {self.cfg.warmup_runs} циклов…")
            for i in range(1, self.cfg.warmup_runs + 1):
                ads = collect_ads(
                    self.store.get_kv(KEY_WATCH_URL) or "",
                    self.cfg.max_items, self.cfg.profile_path, self.cfg.proxy,
                    self.cfg.chromedriver_path, self.cfg.headless
                )
                for ad in ads:
                    try: self.store.save_ad(ad)
                    except Exception: pass
                print(f"[WARMUP] Цикл {i}/{self.cfg.warmup_runs}: сохранено {len(ads)}. Пауза {self.cfg.warmup_delay}s.")
                await asyncio.sleep(self.cfg.warmup_delay)
            try:
                await self.bot.send_message(chat_id=self.store.get_kv(KEY_CHAT_ID), text="Разогрев БД завершён ✅")
            except Exception:
                pass

    async def _run(self):
        chat_id = self.store.get_kv(KEY_CHAT_ID)
        if chat_id:
            try:
                await self.bot.send_message(chat_id=chat_id, text="Парсер auto.ru запущен ✅")
            except Exception as e:
                print(f"[WARN] notify start failed: {e}")

        await self._warmup_if_needed()

        stop_event = self._stop_event
        assert stop_event is not None
        try:
            while not stop_event.is_set():
                url = self.store.get_kv(KEY_WATCH_URL)
                chat_id = self.store.get_kv(KEY_CHAT_ID)
                if not url or not chat_id:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=self.cfg.interval)
                    except asyncio.TimeoutError:
                        continue

                ads = collect_ads(url, self.cfg.max_items, self.cfg.profile_path, self.cfg.proxy, self.cfg.chromedriver_path, self.cfg.headless)

                for ad in reversed(ads):
                    try:
                        if not self.store.is_new_ad(ad["id"]):
                            continue

                        # тянем только дату (+скрин, если нужно)
                        screenshot_path, created = await asyncio.to_thread(fetch_ad_details_sync, ad, self.cfg)

                        # фильтр по дням свежести
                        if self.cfg.fresh_days >= 0 and not is_fresh(created, self.cfg.fresh_days):
                            print(f"[skip stale] {ad.get('id')} created={created}")
                            self.store.save_ad(ad)  # помечаем, чтобы не открывать повторно
                            continue

                        await self._send_ad(chat_id, ad, created, screenshot_path)
                        self.store.save_ad(ad)
                    except Exception as e:
                        print(f"[WARN] send/save failed: {e}")

                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self.cfg.interval)
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
            await asyncio.wait_for(self._task, timeout=self.cfg.interval + 5)
        except asyncio.TimeoutError:
            pass
        self._task = None
        self._stop_event = None

# ======= Handlers =======
async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    store: Storage = context.bot_data["store"]
    svc: ParserService = context.bot_data["parser"]
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

    now = datetime.now(MSK)
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

        if not store.get_watch_url():
            await warn_and_menu("⚠️ Сначала назначь URL (кнопка «🔗 Назначить/поменять URL»).\nНапр.: https://auto.ru/cars/all/?sort=cr_date-desc")
            return
        if not store.get_kv(KEY_CHAT_ID):
            await warn_and_menu("⚠️ Нажми /start в этом чате, чтобы привязать chat_id.")
            return

        if running:
            await svc.stop()
            text = format_status_text(store, False)
            await q.edit_message_text("⏹ Парсер остановлен.\n\n" + text,
                                      reply_markup=build_menu_kb(False),
                                      parse_mode="HTML",
                                      disable_web_page_preview=True)
        else:
            if cfg.proxy: store.set_kv(KEY_PROXY, cfg.proxy)
            if cfg.chromedriver_path: store.set_kv(KEY_CHROMEDRIVER, cfg.chromedriver_path or "")
            if cfg.profile_path: store.set_kv(KEY_PROFILE_PATH, cfg.profile_path or "")
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
            await q.edit_message_text("Пришли один URL (только https://auto.ru/...):",
                                      disable_web_page_preview=True)
        return

async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    store: Storage = context.bot_data["store"]
    svc: ParserService = context.bot_data["parser"]
    mode = context.user_data.get("mode")
    text_in = (update.message.text or "").strip()

    now = datetime.now(MSK)
    expiry = store.get_expiry()
    if store.is_locked() or (expiry and now >= expiry):
        await update.message.reply_text("⛔️ Срок услуги истёк. Управление недоступно.")
        return

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

    await show_menu(update, context)

# ======= Watchdog истечения (через JobQueue) =======
async def expiry_job(context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    store: Storage = app.bot_data["store"]
    svc: ParserService = app.bot_data["parser"]
    try:
        now = datetime.now(MSK)
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

    parser_service = ParserService(bot=application.bot, store=store, cfg=cfg)

    application.bot_data["cfg"] = cfg
    application.bot_data["store"] = store
    application.bot_data["parser"] = parser_service

    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CallbackQueryHandler(menu_button))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message))

    application.job_queue.run_repeating(expiry_job, interval=30, first=5)
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
