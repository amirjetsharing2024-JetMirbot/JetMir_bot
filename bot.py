import os
import re
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
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

def today_start():
    return int(datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

def is_admin(update):
    uid = update.effective_user.id
    return not ADMIN_IDS or uid in ADMIN_IDS

async def deny(update):
    txt = "⛔ Нет доступа."
    if update.callback_query:
        await update.callback_query.answer(txt, show_alert=True)
    else:
        await update.message.reply_text(txt)

SCOOTER_RE = re.compile(r'S\.(\d+)', re.IGNORECASE)
EVIN_RE    = re.compile(r'eVin[:\s]+(\d+)', re.IGNORECASE)
MODEL_RE   = re.compile(r'(Ninebot[^\n,]+|Segway[^\n,]+)', re.IGNORECASE)

def parse_scooters(text):
    results = []
    for m in SCOOTER_RE.finditer(text):
        sid     = m.group(1)
        snippet = text[m.start():m.start()+80]
        evin_m  = EVIN_RE.search(snippet)
        model_m = MODEL_RE.search(snippet)
        results.append({
            "scooter_id": sid,
            "evin":  evin_m.group(1) if evin_m else None,
            "model": model_m.group(1).strip() if model_m else None,
        })
    return results

def detect_event_type(chat_title):
    if CHAT_DEPLOY in chat_title:
        return "deploy"
    if CHAT_RETURN in chat_title:
        return "return"
    return None

async def collect_message(update, ctx):
    msg  = update.effective_message
    chat = update.effective_chat
    if not msg or not chat:
        return
    if chat.type not in ("group", "supergroup"):
        return
    event_type = detect_event_type(chat.title or "")
    if not event_type:
        return
    text = msg.text or msg.caption or ""
    if not text:
        return
    scooters = parse_scooters(text)
    if not scooters:
        return
    operator = msg.from_user.full_name if msg.from_user else None
    ts = int(msg.date.timestamp())
    with get_conn() as conn:
        for sc in scooters:
            conn.execute(
                "INSERT INTO events (scooter_id,evin,model,event_type,chat_title,chat_id,operator,date) VALUES (?,?,?,?,?,?,?,?)",
                (sc["scooter_id"], sc["evin"], sc["model"], event_type, chat.title, chat.id, operator, ts)
            )

def get_deployed_today():
    ts = today_start()
    with get_conn() as conn:
        cnt = conn.execute(
            "SELECT COUNT(*) FROM events WHERE event_type='deploy' AND date>=?", (ts,)
        ).fetchone()[0]
    today_str = datetime.now(TZ).strftime("%d.%m.%Y")
    if cnt == 0:
        return f"🛴 *Выехали в поле сегодня ({today_str})*\n\nНет данных."
    return f"🛴 *Выехали в поле сегодня ({today_str})*\n\n`{cnt}` самокатов выехало в поле."

def get_returned_today():
    ts = today_start()
    with get_conn() as conn:
        cnt = conn.execute(
            "SELECT COUNT(*) FROM events WHERE event_type='return' AND date>=?", (ts,)
        ).fetchone()[0]
    today_str = datetime.now(TZ).strftime("%d.%m.%Y")
    if cnt == 0:
        return f"🔧 *Привезли на СЦ сегодня ({today_str})*\n\nНет данных."
    return f"🔧 *Привезли на СЦ сегодня ({today_str})*\n\n`{cnt}` самокатов привезли на СЦ."

def get_early_returns():
    with get_conn() as conn:
        returns = conn.execute(
            "SELECT scooter_id, date FROM events WHERE event_type='return' ORDER BY date DESC"
        ).fetchall()

    buckets = {7: [], 14: [], 21: []}
    for ret in returns:
        sid = ret["scooter_id"]
        ret_ts = ret["date"]
        with get_conn() as conn:
            dep = conn.execute(
                "SELECT date FROM events WHERE scooter_id=? AND event_type='deploy' AND date<? ORDER BY date DESC LIMIT 1",
                (sid, ret_ts)
            ).fetchone()
        if not dep:
            continue
        days_out = (ret_ts - dep["date"]) / 86400
        for threshold in EARLY_THRESHOLDS:
            if days_out < threshold:
                buckets[threshold].append({
                    "sid": sid, "days": days_out,
                    "deploy_ts": dep["date"], "return_ts": ret_ts,
                })
                break

    lines = ["⚠️ *Ранние возвраты самокатов*\n"]
    total = 0
    for threshold in EARLY_THRESHOLDS:
        group = buckets[threshold]
        if not group:
            continue
        lines.append(f"*До {threshold} дней* — {len(group)} шт.")
        for item in group:
            lines.append(
                f"  `S.{item['sid']}` — выехал {ts_to_local(item['deploy_ts'])}, "
                f"вернулся {ts_to_local(item['return_ts'])} "
                f"(спустя *{item['days']:.0f} дн.*) ⚠️"
            )
        total += len(group)
        lines.append("")
    if total == 0:
        return "⚠️ *Ранние возвраты*\n\nНет ранних возвратов. Всё в норме ✅"
    lines.append(f"Итого: `{total}` самокатов")
    return "\n".join(lines)

def get_summary():
    ts = today_start()
    with get_conn() as conn:
        dep_today = conn.execute("SELECT COUNT(*) FROM events WHERE event_type='deploy' AND date>=?", (ts,)).fetchone()[0]
        ret_today = conn.execute("SELECT COUNT(*) FROM events WHERE event_type='return' AND date>=?", (ts,)).fetchone()[0]
        dep_total = conn.execute("SELECT COUNT(DISTINCT scooter_id) FROM events WHERE event_type='deploy'").fetchone()[0]
        ret_total = conn.execute("SELECT COUNT(DISTINCT scooter_id) FROM events WHERE event_type='return'").fetchone()[0]
        returns   = conn.execute("SELECT scooter_id, date FROM events WHERE event_type='return'").fetchall()

    early = 0
    for ret in returns:
        with get_conn() as conn:
            dep = conn.execute(
                "SELECT date FROM events WHERE scooter_id=? AND event_type='deploy' AND date<? ORDER BY date DESC LIMIT 1",
                (ret["scooter_id"], ret["date"])
            ).fetchone()
        if dep and (ret["date"] - dep["date"]) / 86400 < 21:
            early += 1

    today_str = datetime.now(TZ).strftime("%d.%m.%Y")
    return (
        f"📋 *Сводка на {today_str}*\n\n"
        f"🛴 Выехали в поле сегодня:    `{dep_today}`\n"
        f"🔧 Привезли на СЦ сегодня:    `{ret_today}`\n\n"
        f"📦 Всего выездов (уник.):      `{dep_total}`\n"
        f"📥 Всего возвратов (уник.):    `{ret_total}`\n\n"
        f"⚠️ Ранних возвратов (<21 дн.): `{early}`"
    )

def main_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛴 Выехали сегодня",  callback_data="deployed"),
         InlineKeyboardButton("🔧 Привезли сегодня", callback_data="returned")],
        [InlineKeyboardButton("⚠️ Ранние возвраты",  callback_data="early")],
        [InlineKeyboardButton("📋 Общая сводка",     callback_data="summary")],
    ])

async def cmd_start(update, ctx):
    if update.effective_chat.type != "private": return
    if not is_admin(update): return await deny(update)
    await update.message.reply_text(
        "👋 *JetMir Ops Bot*\n\n"
        "Отслеживаю самокаты по чатам:\n"
        f"• {CHAT_DEPLOY} | Алматы\n"
        f"• {CHAT_RETURN} | Алматы\n\n"
        "/deployed — выехали в поле сегодня\n"
        "/returned — привезли на СЦ сегодня\n"
        "/early — ранние возвраты (<7/14/21 дн.)\n"
        "/summary — общая сводка\n"
        "/find 255022 — история самоката",
        parse_mode="Markdown", reply_markup=main_kb()
    )

async def cmd_deployed(update, ctx):
    if update.effective_chat.type != "private": return
    if not is_admin(update): return await deny(update)
    await update.message.reply_text(get_deployed_today(), parse_mode="Markdown", reply_markup=main_kb())

async def cmd_returned(update, ctx):
    if update.effective_chat.type != "private": return
    if not is_admin(update): return await deny(update)
    await update.message.reply_text(get_returned_today(), parse_mode="Markdown", reply_markup=main_kb())

async def cmd_early(update, ctx):
    if update.effective_chat.type != "private": return
    if not is_admin(update): return await deny(update)
    await update.message.reply_text(get_early_returns(), parse_mode="Markdown", reply_markup=main_kb())

async def cmd_summary(update, ctx):
    if update.effective_chat.type != "private": return
    if not is_admin(update): return await deny(update)
    await update.message.reply_text(get_summary(), parse_mode="Markdown", reply_markup=main_kb())

async def cmd_find(update, ctx):
    if update.effective_chat.type != "private": return
    if not is_admin(update): return await deny(update)
    if not ctx.args:
        return await update.message.reply_text("Использование: `/find 255022`", parse_mode="Markdown")
    raw = ctx.args[0].lstrip("Ss.")
    if not raw.isdigit():
        return await update.message.reply_text("Введи номер: `/find 255022`", parse_mode="Markdown")
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT event_type, operator, date FROM events WHERE scooter_id=? ORDER BY date ASC", (raw,)
        ).fetchall()
    if not rows:
        return await update.message.reply_text(f"Самокат `S.{raw}` не найден.", parse_mode="Markdown")
    lines = [f"🔍 *История S.{raw}*\n"]
    prev_dep = None
    for r in rows:
        t    = ts_to_local(r["date"])
        op   = r["operator"] or "?"
        if r["event_type"] == "deploy":
            prev_dep = r["date"]
            lines.append(f"🛴 Выехал в поле: `{t}` | {op}")
        else:
            if prev_dep:
                days = (r["date"] - prev_dep) / 86400
                warn = " ⚠️ РАННИЙ ВОЗВРАТ" if days < 7 else (" ⚠️" if days < 14 else "")
                lines.append(f"🔧 Вернулся на СЦ: `{t}` | {op} (в поле: *{days:.0f} дн.*){warn}")
            else:
                lines.append(f"🔧 Вернулся на СЦ: `{t}` | {op}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=main_kb())

CALLBACK_MAP = {
    "deployed": get_deployed_today,
    "returned": get_returned_today,
    "early":    get_early_returns,
    "summary":  get_summary,
}

async def on_callback(update, ctx):
    q = update.callback_query
    await q.answer()
    if not is_admin(update):
        await q.answer("⛔ Нет доступа", show_alert=True)
        return
    fn = CALLBACK_MAP.get(q.data)
    if fn:
        await ctx.bot.send_message(
            chat_id=q.message.chat_id,
            text=fn(),
            parse_mode="Markdown",
            reply_markup=main_kb()
        )

def main():
    init_db()
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(MessageHandler(filters.ChatType.GROUPS, collect_message), group=0)
    app.add_handler(CommandHandler("start",    cmd_start))
    app.add_handler(CommandHandler("deployed", cmd_deployed))
    app.add_handler(CommandHandler("returned", cmd_returned))
    app.add_handler(CommandHandler("early",    cmd_early))
    app.add_handler(CommandHandler("summary",  cmd_summary))
    app.add_handler(CommandHandler("find",     cmd_find))
    app.add_handler(CallbackQueryHandler(on_callback))
    print("🤖 JetMir Ops Bot started")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
