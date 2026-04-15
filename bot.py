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
            date        INTEGER NOT NULL,
            message_id  INTEGER,
            UNIQUE(message_id, chat_id, scooter_id, event_type)
        );
        CREATE INDEX IF NOT EXISTS idx_scooter ON events(scooter_id);
        CREATE INDEX IF NOT EXISTS idx_date    ON events(date);
        CREATE INDEX IF NOT EXISTS idx_type    ON events(event_type);
        """)

def ts_to_local(ts):
    return datetime.fromtimestamp(ts, TZ).strftime("%d.%m.%Y %H:%M")

def day_range(offset_days=0):
    base = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    base += timedelta(days=offset_days)
    start = int(base.timestamp())
    end   = int((base + timedelta(days=1)).timestamp()) - 1
    return start, end

def parse_date_input(text):
    text = text.strip()
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=TZ)
            start = int(dt.timestamp())
            end   = int((dt + timedelta(days=1)).timestamp()) - 1
            return start, end
        except ValueError:
            pass
    return None

def week_range():
    base = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    start = int((base - timedelta(days=6)).timestamp())
    end   = int((base + timedelta(days=1)).timestamp()) - 1
    return start, end

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
    # FIX 1: дедупликация — один самокат из одного сообщения считается один раз
    seen = set()
    results = []
    for m in SCOOTER_RE.finditer(text):
        sid = m.group(1)
        if sid in seen:
            continue
        seen.add(sid)
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
    msg_id = msg.message_id

    with get_conn() as conn:
        for sc in scooters:
            # FIX 2: INSERT OR IGNORE защищает от дублей при рестарте / редактировании
            conn.execute(
                """INSERT OR IGNORE INTO events
                   (scooter_id, evin, model, event_type, chat_title, chat_id, operator, date, message_id)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (sc["scooter_id"], sc["evin"], sc["model"],
                 event_type, chat.title, chat.id, operator, ts, msg_id)
            )

def label_for_range(start_ts, end_ts):
    start_dt = datetime.fromtimestamp(start_ts, TZ)
    end_dt   = datetime.fromtimestamp(end_ts,   TZ)
    today    = datetime.now(TZ).date()
    if start_dt.date() == end_dt.date():
        if start_dt.date() == today:
            return f"сегодня ({start_dt.strftime('%d.%m.%Y')})"
        if start_dt.date() == today - timedelta(days=1):
            return f"вчера ({start_dt.strftime('%d.%m.%Y')})"
        return start_dt.strftime("%d.%m.%Y")
    return f"{start_dt.strftime('%d.%m.%Y')} - {end_dt.strftime('%d.%m.%Y')}"

def get_deployed(start_ts, end_ts):
    lbl = label_for_range(start_ts, end_ts)
    with get_conn() as conn:
        # FIX 3: COUNT(DISTINCT) чтобы один самокат не считался дважды
        cnt = conn.execute(
            "SELECT COUNT(DISTINCT scooter_id) FROM events WHERE event_type='deploy' AND date>=? AND date<=?",
            (start_ts, end_ts)
        ).fetchone()[0]
    if cnt == 0:
        return f"🛴 *Выехали в поле — {lbl}*\n\nНет данных."
    return f"🛴 *Выехали в поле — {lbl}*\n\n`{cnt}` самокатов выехало в поле."

def get_returned(start_ts, end_ts):
    lbl = label_for_range(start_ts, end_ts)
    with get_conn() as conn:
        # FIX 3: COUNT(DISTINCT) чтобы один самокат не считался дважды
        cnt = conn.execute(
            "SELECT COUNT(DISTINCT scooter_id) FROM events WHERE event_type='return' AND date>=? AND date<=?",
            (start_ts, end_ts)
        ).fetchone()[0]
    if cnt == 0:
        return f"🔧 *Привезли на СЦ — {lbl}*\n\nНет данных."
    return f"🔧 *Привезли на СЦ — {lbl}*\n\n`{cnt}` самокатов привезли на СЦ."

def get_early_returns(start_ts, end_ts):
    lbl = label_for_range(start_ts, end_ts)
    with get_conn() as conn:
        returns = conn.execute(
            "SELECT scooter_id, date FROM events WHERE event_type='return' AND date>=? AND date<=? ORDER BY date DESC",
            (start_ts, end_ts)
        ).fetchall()

    buckets = {7: [], 14: [], 21: []}
    for ret in returns:
        sid    = ret["scooter_id"]
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

    lines = [f"⚠️ *Ранние возвраты — {lbl}*\n"]
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
        return f"⚠️ *Ранние возвраты — {lbl}*\n\nНет ранних возвратов. Всё в норме ✅"
    lines.append(f"Итого: `{total}` самокатов")
    return "\n".join(lines)

def get_summary(start_ts, end_ts):
    lbl = label_for_range(start_ts, end_ts)
    with get_conn() as conn:
        dep_cnt = conn.execute(
            "SELECT COUNT(*) FROM events WHERE event_type='deploy' AND date>=? AND date<=?",
            (start_ts, end_ts)
        ).fetchone()[0]
        ret_cnt = conn.execute(
            "SELECT COUNT(*) FROM events WHERE event_type='return' AND date>=? AND date<=?",
            (start_ts, end_ts)
        ).fetchone()[0]
        dep_uniq = conn.execute(
            "SELECT COUNT(DISTINCT scooter_id) FROM events WHERE event_type='deploy' AND date>=? AND date<=?",
            (start_ts, end_ts)
        ).fetchone()[0]
        ret_uniq = conn.execute(
            "SELECT COUNT(DISTINCT scooter_id) FROM events WHERE event_type='return' AND date>=? AND date<=?",
            (start_ts, end_ts)
        ).fetchone()[0]
        returns = conn.execute(
            "SELECT scooter_id, date FROM events WHERE event_type='return' AND date>=? AND date<=?",
            (start_ts, end_ts)
        ).fetchall()

    early = 0
    for ret in returns:
        with get_conn() as conn:
            dep = conn.execute(
                "SELECT date FROM events WHERE scooter_id=? AND event_type='deploy' AND date<? ORDER BY date DESC LIMIT 1",
                (ret["scooter_id"], ret["date"])
            ).fetchone()
        if dep and (ret["date"] - dep["date"]) / 86400 < 21:
            early += 1

    return (
        f"📋 *Сводка — {lbl}*\n\n"
        f"🛴 Выехали в поле:        `{dep_cnt}` (уник. `{dep_uniq}`)\n"
        f"🔧 Привезли на СЦ:        `{ret_cnt}` (уник. `{ret_uniq}`)\n\n"
        f"⚠️ Ранних возвратов (<21 дн.): `{early}`"
    )

def main_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛴 Выехали",         callback_data="sect:deployed"),
         InlineKeyboardButton("🔧 Привезли",         callback_data="sect:returned")],
        [InlineKeyboardButton("⚠️ Ранние возвраты", callback_data="sect:early")],
        [InlineKeyboardButton("📋 Общая сводка",    callback_data="sect:summary")],
    ])

def date_kb(section):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Сегодня", callback_data=f"date:{section}:today"),
         InlineKeyboardButton("📅 Вчера",   callback_data=f"date:{section}:yesterday")],
        [InlineKeyboardButton("📅 7 дней",  callback_data=f"date:{section}:week"),
         InlineKeyboardButton("✏️ Ввести дату", callback_data=f"date:{section}:custom")],
        [InlineKeyboardButton("⬅️ Назад",   callback_data="back:main")],
    ])

def back_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Назад в меню", callback_data="back:main")]
    ])

SECTION_LABELS = {
    "deployed": "🛴 Выехали в поле",
    "returned": "🔧 Привезли на СЦ",
    "early":    "⚠️ Ранние возвраты",
    "summary":  "📋 Общая сводка",
}

async def cmd_start(update, ctx):
    if update.effective_chat.type != "private":
        return
    if not is_admin(update):
        return await deny(update)
    await update.message.reply_text(
        "👋 *JetMir Ops Bot*\n\n"
        "Отслеживаю самокаты по чатам:\n"
        f"• {CHAT_DEPLOY}\n"
        f"• {CHAT_RETURN}\n\n"
        "Выбери раздел 👇",
        parse_mode="Markdown",
        reply_markup=main_kb()
    )

async def cmd_find(update, ctx):
    if update.effective_chat.type != "private":
        return
    if not is_admin(update):
        return await deny(update)
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
        t  = ts_to_local(r["date"])
        op = r["operator"] or "?"
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

def _build_text(section, start_ts, end_ts):
    if section == "deployed":
        return get_deployed(start_ts, end_ts)
    if section == "returned":
        return get_returned(start_ts, end_ts)
    if section == "early":
        return get_early_returns(start_ts, end_ts)
    if section == "summary":
        return get_summary(start_ts, end_ts)
    return "Неизвестный раздел."

async def _section_cmd(update, ctx, section):
    if update.effective_chat.type != "private":
        return
    if not is_admin(update):
        return await deny(update)
    label = SECTION_LABELS.get(section, section)
    if ctx.args:
        rng = parse_date_input(ctx.args[0])
        if not rng:
            return await update.message.reply_text("Неверный формат даты. Пример: `10.04.2025`", parse_mode="Markdown")
        text = _build_text(section, *rng)
        return await update.message.reply_text(text, parse_mode="Markdown", reply_markup=back_kb())
    await update.message.reply_text(
        f"{label}\n\nВыбери период 👇",
        parse_mode="Markdown",
        reply_markup=date_kb(section)
    )

async def cmd_deployed(update, ctx): await _section_cmd(update, ctx, "deployed")
async def cmd_returned(update, ctx): await _section_cmd(update, ctx, "returned")
async def cmd_early(update, ctx):    await _section_cmd(update, ctx, "early")
async def cmd_summary(update, ctx):  await _section_cmd(update, ctx, "summary")

async def on_callback(update, ctx):
    q = update.callback_query
    await q.answer()
    if not is_admin(update):
        await q.answer("⛔ Нет доступа", show_alert=True)
        return

    data = q.data

    if data == "back:main":
        await ctx.bot.send_message(
            chat_id=q.message.chat_id,
            text="Выбери раздел 👇",
            parse_mode="Markdown",
            reply_markup=main_kb()
        )
        return

    if data.startswith("sect:"):
        section = data.split(":")[1]
        label   = SECTION_LABELS.get(section, section)
        await ctx.bot.send_message(
            chat_id=q.message.chat_id,
            text=f"{label}\n\nВыбери период 👇",
            parse_mode="Markdown",
            reply_markup=date_kb(section)
        )
        return

    if data.startswith("date:"):
        _, section, period = data.split(":", 2)

        if period == "custom":
            ctx.user_data["waiting_date_for"] = section
            await ctx.bot.send_message(
                chat_id=q.message.chat_id,
                text="✏️ Введи дату в формате *ДД.ММ.ГГГГ* (например `10.04.2025`):",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data="back:main")]
                ])
            )
            return

        if period == "today":
            rng = day_range(0)
        elif period == "yesterday":
            rng = day_range(-1)
        elif period == "week":
            rng = week_range()
        else:
            return

        text = _build_text(section, *rng)
        await ctx.bot.send_message(
            chat_id=q.message.chat_id,
            text=text,
            parse_mode="Markdown",
            reply_markup=back_kb()
        )

async def on_text(update, ctx):
    msg  = update.effective_message
    chat = update.effective_chat

    if chat.type != "private":
        return

    section = ctx.user_data.get("waiting_date_for")
    if not section:
        return

    if not is_admin(update):
        return await deny(update)

    rng = parse_date_input(msg.text or "")
    if not rng:
        await msg.reply_text(
            "❌ Неверный формат. Попробуй снова: `ДД.ММ.ГГГГ` (например `10.04.2025`)",
            parse_mode="Markdown"
        )
        return

    ctx.user_data.pop("waiting_date_for", None)
    text = _build_text(section, *rng)
    await msg.reply_text(text, parse_mode="Markdown", reply_markup=back_kb())

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

    app.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, on_text),
        group=1
    )

    print("🤖 JetMir Ops Bot started")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
