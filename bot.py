import os
import re
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters, ConversationHandler
)

TOKEN     = os.environ["BOT_TOKEN"]
ADMIN_IDS = set(
    int(x.strip())
    for x in os.environ.get("ADMIN_IDS", "").split(",")
    if x.strip()
)
TZ       = ZoneInfo(os.environ.get("TZ_NAME", "Asia/Almaty"))
DB_PATH  = os.environ.get("DB_PATH", "/data/monitor.db")

CHAT_DEPLOY = "Забрали готовые СЦ"
CHAT_RETURN = "Привезли на ремонт ТС"
EARLY_THRESHOLDS = [7, 14, 21]

# ConversationHandler state
WAITING_DATE = 1

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            scooter_id  TEXT NOT NULL,
            evin        TEXT,
            model       TEXT,
            event_type  TEXT NOT NULL,
            chat_title  TEXT,
            chat_id     INTEGER,
            operator    TEXT,
            date        INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_scooter ON events(scooter_id);
        CREATE INDEX IF NOT EXISTS idx_date    ON events(date);
        CREATE INDEX IF NOT EXISTS idx_type    ON events(event_type);
        """)

def ts_to_local(ts):
    return datetime.fromtimestamp(ts, TZ).strftime("%d.%m.%Y %H:%M")

def day_range(offset_days=0):
    """Return (start_ts, end_ts) for a day with offset from today (0=today, -1=yesterday)."""
    base = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    base += timedelta(days=offset_days)
    start = int(base.timestamp())
    end   = int((base + timedelta(days=1)).timestamp()) - 1
    return start, end

def parse_date_input(text):
    """Parse dd.mm.yyyy string. Returns (start_ts, end_ts) or None."""
    text = text.strip()
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            dt = d
