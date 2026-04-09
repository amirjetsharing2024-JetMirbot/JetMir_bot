import os
import sqlite3
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)

# ── Config ────────────────────────────────────────────────────────────────────
TOKEN        = os.environ["BOT_TOKEN"]
ADMIN_IDS    = set(
    int(x.strip())
    for x in os.environ.get("ADMIN_IDS", "").split(",")
    if x.strip()
)
TZ           = ZoneInfo(os.environ.get("TZ_NAME", "Asia/Almaty"))
DB_PATH      = os.environ.get("DB_PATH", "/data/monitor.db")

# ── DB ────────────────────────────────────────────────────────────────────────
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS messages (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id     INTEGER NOT NULL,
            chat_title  TEXT,
            chat_type   TEXT,
            msg_id      INTEGER,
            user_id     INTEGER,
            username    TEXT,
            full_name   TEXT,
            text        TEXT,
            has_media   INTEGER DEFAULT 0,
            media_type  TEXT,
            date        INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_chat   ON messages(chat_id);
        CREATE INDEX IF NOT EXISTS idx_date   ON messages(date);
        CREATE INDEX IF NOT EXISTS idx_user   ON messages(user_id);
        CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts
            USING fts5(text, content=messages, content_rowid=id);
        CREATE TRIGGER IF NOT EXISTS msg_ai AFTER INSERT ON messages BEGIN
            INSERT INTO messages_fts(rowid, text) VALUES (new.id, new.text);
        END;
        """)

# ── Helpers ───────────────────────────────────────────────────────────────────
def now_local() -> datetime:
    return datetime.now(TZ)

def ts_to_local(ts: int) -> str:
    return datetime.fromtimestamp(ts, TZ).strftime("%d.%m.%Y %H:%M")

def is_admin(update: Update) -> bool:
    uid = update.effective_user.id
    return not ADMIN_IDS or uid in ADMIN_IDS

async def deny(update: Update):
    await update.message.reply_text("⛔ Нет доступа.")

def main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Общая стата",   callback_data="cb_stats"),
         InlineKeyboardButton("💬 По чатам",      callback_data="cb_chats")],
        [InlineKeyboardButton("🏆 Топ юзеров",    callback_data="cb_topusers"),
         InlineKeyboardButton("🕐 Последние",     callback_data="cb_recent")],
    ])

# ── Message collector ─────────────────────────────────────────────────────────
async def collect_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Saves every group/channel message to DB."""
    msg = update.effective_message
    if not msg:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup", "channel"):
        return

    user    = update.effective_user
    uid     = user.id     if user else None
    uname   = user.username  if user else None
    fname   = user.full_name if user else None

    text = msg.text or msg.caption or ""

    media_type = None
    if msg.photo:        media_type = "photo"
    elif msg.video:      media_type = "video"
    elif msg.document:   media_type = "document"
    elif msg.audio:      media_type = "audio"
    elif msg.voice:      media_type = "voice"
    elif msg.sticker:    media_type = "sticker"
    elif msg.animation:  media_type = "animation"

    ts = int(msg.date.timestamp())

    with get_conn() as conn:
        conn.execute(
            """INSERT INTO messages
               (chat_id, chat_title, chat_type, msg_id,
                user_id, username, full_name, text,
                has_media, media_type, date)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (chat.id, chat.title, chat.type, msg.message_id,
             uid, uname, fname, text,
             1 if media_type else 0, media_type, ts)
        )

# ── Text builders (используются и командами и кнопками) ──────────────────────
def get_stats_text() -> str:
    with get_conn() as conn:
        total  = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        chats  = conn.execute("SELECT COUNT(DISTINCT chat_id) FROM messages").fetchone()[0]
        users  = conn.execute("SELECT COUNT(DISTINCT user_id) FROM messages WHERE user_id IS NOT NULL").fetchone()[0]
        media  = conn.execute("SELECT COUNT(*) FROM messages WHERE has_media=1").fetchone()[0]
        today_start = int(datetime.now(TZ).replace(hour=0,minute=0,second=0,microsecond=0).timestamp())
        today  = conn.execute("SELECT COUNT(*) FROM messages WHERE date>=?", (today_start,)).fetchone()[0]
        oldest = conn.execute("SELECT MIN(date) FROM messages").fetchone()[0]
        newest = conn.execute("SELECT MAX(date) FROM messages").fetchone()[0]
    oldest_s = ts_to_local(oldest) if oldest else "—"
    newest_s = ts_to_local(newest) if newest else "—"
    return (
        f"📊 *Общая статистика*\n\n"
        f"Всего сообщений: `{total:,}`\n"
        f"Из них сегодня:  `{today:,}`\n"
        f"С медиафайлом:   `{media:,}`\n"
        f"Чатов:           `{chats}`\n"
        f"Уникальных юзеров: `{users}`\n\n"
        f"Первое сообщение: `{oldest_s}`\n"
        f"Последнее:        `{newest_s}`"
    )

def get_chats_text() -> str:
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT chat_id, chat_title, COUNT(*) as cnt, MAX(date) as last
               FROM messages GROUP BY chat_id ORDER BY cnt DESC"""
        ).fetchall()
    if not rows:
        return "Нет данных. Добавь бота в чаты."
    lines = ["💬 *Отслеживаемые чаты*\n"]
    for r in rows:
        title = r["chat_title"] or "—"
        last  = ts_to_local(r["last"]) if r["last"] else "—"
        lines.append(f"*{title}*\n  ID: `{r['chat_id']}`\n  Сообщений: `{r['cnt']:,}` | Последнее: `{last}`")
    return "\n\n".join(lines)

def get_topusers_text(n: int = 10) -> str:
    with get_conn() as conn:
        rows = conn.execute(
            f"""SELECT user_id, username, full_name, COUNT(*) as cnt,
                       COUNT(DISTINCT chat_id) as chats
                FROM messages WHERE user_id IS NOT NULL
                GROUP BY user_id ORDER BY cnt DESC LIMIT {n}"""
        ).fetchall()
    if not rows:
        return "Нет данных."
    lines = [f"🏆 *Топ {n} активных участников*\n"]
    for i, r in enumerate(rows, 1):
        name  = r["full_name"] or r["username"] or str(r["user_id"])
        uname = f"@{r['username']}" if r["username"] else f"`{r['user_id']}`"
        lines.append(f"{i}. {name} ({uname})\n   Сообщений: `{r['cnt']:,}` в `{r['chats']}` чатах")
    return "\n\n".join(lines)

def get_recent_text(n: int = 20) -> str:
    with get_conn() as conn:
        rows = conn.execute(
            f"""SELECT chat_title, full_name, username, text, media_type, date
                FROM messages ORDER BY date DESC LIMIT {n}"""
        ).fetchall()
    if not rows:
        return "Нет сообщений."
    lines = [f"🕐 *Последние {n} сообщений*\n"]
    for r in rows:
        who  = r["full_name"] or r["username"] or "?"
        chat = r["chat_title"] or "?"
        ts   = ts_to_local(r["date"])
        body = r["text"] or f"[{r['media_type'] or 'медиа'}]"
        body = body[:120] + ("…" if len(body) > 120 else "")
        lines.append(f"`{ts}` [{chat}] *{who}*:\n{body}")
    return "\n\n".join(lines)

# ── /start ────────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    if not is_admin(update): return await deny(update)
    await update.message.reply_text(
        "👋 *Монитор чатов*\n\n"
        "Добавь меня в нужные группы — я буду логировать все сообщения.\n\n"
        "Команды для лички:\n"
        "/stats — общая статистика\n"
        "/chats — список отслеживаемых чатов\n"
        "/topusers — топ активных участников\n"
        "/recent `[N]` — последние N сообщений (по умолч. 20)\n"
        "/find `<текст>` — полнотекстовый поиск\n"
        "/chatlog `<chat_id>` `[N]` — лог конкретного чата\n"
        "/userstats `<@username или user_id>` — стата по юзеру\n"
        "/help — справка",
        parse_mode="Markdown",
        reply_markup=main_kb(),
    )

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private": return
    if not is_admin(update): return await deny(update)
    await update.message.reply_text(
        "📖 *Команды*\n\n"
        "`/stats` — общая статистика по всем чатам\n"
        "`/chats` — список чатов с кол-вом сообщений\n"
        "`/topusers [N]` — топ N юзеров (по умолч. 10)\n"
        "`/recent [N]` — последние N сообщений (по умолч. 20)\n"
        "`/find <текст>` — полнотекстовый поиск\n"
        "`/chatlog <chat_id> [N]` — лог чата (N последних)\n"
        "`/userstats <@username|user_id>` — статистика юзера\n"
        "`/today` — сообщения за сегодня\n"
        "`/help` — эта справка",
        parse_mode="Markdown",
    )

# ── /stats ────────────────────────────────────────────────────────────────────
async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private": return
    if not is_admin(update): return await deny(update)
    await update.message.reply_text(get_stats_text(), parse_mode="Markdown", reply_markup=main_kb())

# ── /chats ────────────────────────────────────────────────────────────────────
async def cmd_chats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private": return
    if not is_admin(update): return await deny(update)
    await update.message.reply_text(get_chats_text(), parse_mode="Markdown", reply_markup=main_kb())

# ── /topusers ─────────────────────────────────────────────────────────────────
async def cmd_topusers(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private": return
    if not is_admin(update): return await deny(update)
    n = 10
    if ctx.args:
        try: n = max(1, min(50, int(ctx.args[0])))
        except ValueError: pass
    await update.message.reply_text(get_topusers_text(n), parse_mode="Markdown", reply_markup=main_kb())

async def cmd_recent(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private": return
    if not is_admin(update): return await deny(update)
    n = 20
    if ctx.args:
        try: n = max(1, min(50, int(ctx.args[0])))
        except ValueError: pass
    await update.message.reply_text(get_recent_text(n), parse_mode="Markdown")

# ── /find ─────────────────────────────────────────────────────────────────────
async def cmd_find(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private": return
    if not is_admin(update): return await deny(update)

    if not ctx.args:
        return await update.message.reply_text("Использование: `/find <текст>`", parse_mode="Markdown")

    query = " ".join(ctx.args)

    with get_conn() as conn:
        rows = conn.execute(
            """SELECT m.chat_title, m.full_name, m.username,
                      m.text, m.date, m.chat_id
               FROM messages_fts fts
               JOIN messages m ON fts.rowid = m.id
               WHERE messages_fts MATCH ?
               ORDER BY m.date DESC
               LIMIT 30""",
            (query,)
        ).fetchall()

    if not rows:
        return await update.message.reply_text(f"🔍 По запросу «{query}» ничего не найдено.")

    lines = [f"🔍 *Найдено {len(rows)} сообщений по «{query}»*\n"]
    for r in rows:
        who  = r["full_name"] or r["username"] or "?"
        chat = r["chat_title"] or str(r["chat_id"])
        ts   = ts_to_local(r["date"])
        body = r["text"] or ""
        # highlight match (simple)
        body = body[:200] + ("…" if len(body) > 200 else "")
        lines.append(f"`{ts}` [{chat}] *{who}*:\n{body}")

    await update.message.reply_text("\n\n".join(lines), parse_mode="Markdown")

# ── /chatlog ──────────────────────────────────────────────────────────────────
async def cmd_chatlog(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private": return
    if not is_admin(update): return await deny(update)

    if not ctx.args:
        return await update.message.reply_text(
            "Использование: `/chatlog <chat_id> [N]`\n"
            "chat_id узнай через /chats", parse_mode="Markdown"
        )

    try:
        chat_id = int(ctx.args[0])
    except ValueError:
        return await update.message.reply_text("chat_id должен быть числом.")

    n = 30
    if len(ctx.args) > 1:
        try: n = max(1, min(100, int(ctx.args[1])))
        except ValueError: pass

    with get_conn() as conn:
        info = conn.execute(
            "SELECT chat_title FROM messages WHERE chat_id=? LIMIT 1", (chat_id,)
        ).fetchone()
        rows = conn.execute(
            """SELECT full_name, username, text, media_type, date
               FROM messages WHERE chat_id=?
               ORDER BY date DESC LIMIT ?""",
            (chat_id, n)
        ).fetchall()

    if not rows:
        return await update.message.reply_text("Нет сообщений для этого чата.")

    title = info["chat_title"] if info else str(chat_id)
    lines = [f"📜 *Лог чата «{title}»* (последние {n})\n"]
    for r in reversed(rows):
        who  = r["full_name"] or r["username"] or "?"
        ts   = ts_to_local(r["date"])
        body = r["text"] or f"[{r['media_type'] or 'медиа'}]"
        body = body[:150] + ("…" if len(body) > 150 else "")
        lines.append(f"`{ts}` *{who}*: {body}")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

# ── /userstats ────────────────────────────────────────────────────────────────
async def cmd_userstats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private": return
    if not is_admin(update): return await deny(update)

    if not ctx.args:
        return await update.message.reply_text(
            "Использование: `/userstats <@username или user_id>`", parse_mode="Markdown"
        )

    arg = ctx.args[0].lstrip("@")
    with get_conn() as conn:
        # try username first, then user_id
        if arg.isdigit():
            rows = conn.execute(
                "SELECT * FROM messages WHERE user_id=? ORDER BY date DESC", (int(arg),)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM messages WHERE username=? ORDER BY date DESC", (arg,)
            ).fetchall()

    if not rows:
        return await update.message.reply_text("Пользователь не найден в базе.")

    r0 = rows[0]
    name  = r0["full_name"] or r0["username"] or str(r0["user_id"])
    uname = f"@{r0['username']}" if r0["username"] else "—"
    total = len(rows)
    media = sum(1 for r in rows if r["has_media"])
    chats = len(set(r["chat_id"] for r in rows))
    first = ts_to_local(rows[-1]["date"])
    last  = ts_to_local(rows[0]["date"])

    # top chats for this user
    from collections import Counter
    chat_cnt = Counter((r["chat_id"], r["chat_title"]) for r in rows)
    top_chats = "\n".join(
        f"  • {t or str(cid)}: {cnt}"
        for (cid, t), cnt in chat_cnt.most_common(5)
    )

    text = (
        f"👤 *{name}* ({uname})\n"
        f"ID: `{r0['user_id']}`\n\n"
        f"Всего сообщений: `{total:,}`\n"
        f"С медиа: `{media}`\n"
        f"Чатов: `{chats}`\n"
        f"Первое: `{first}`\n"
        f"Последнее: `{last}`\n\n"
        f"*Активность по чатам:*\n{top_chats}"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

# ── /today ────────────────────────────────────────────────────────────────────
async def cmd_today(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private": return
    if not is_admin(update): return await deny(update)

    today_start = int(datetime.now(TZ).replace(hour=0,minute=0,second=0,microsecond=0).timestamp())
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT chat_id, chat_title, COUNT(*) as cnt,
                      COUNT(DISTINCT user_id) as users
               FROM messages WHERE date >= ?
               GROUP BY chat_id ORDER BY cnt DESC""",
            (today_start,)
        ).fetchall()
        total = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE date>=?", (today_start,)
        ).fetchone()[0]

    if not rows:
        return await update.message.reply_text("Сегодня сообщений нет.")

    today_str = datetime.now(TZ).strftime("%d.%m.%Y")
    lines = [f"📅 *Сегодня ({today_str})* — всего `{total}` сообщений\n"]
    for r in rows:
        title = r["chat_title"] or str(r["chat_id"])
        lines.append(f"• {title}: `{r['cnt']}` сообщ., `{r['users']}` юзеров")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=main_kb())

# ── Callback buttons ──────────────────────────────────────────────────────────
async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(update):
        await q.answer("⛔ Нет доступа", show_alert=True)
        return

    chat_id = q.message.chat_id

    dispatch = {
        "cb_stats":    (get_stats_text,    "📊"),
        "cb_chats":    (get_chats_text,    "💬"),
        "cb_topusers": (get_topusers_text, "🏆"),
        "cb_recent":   (get_recent_text,   "🕐"),
    }
    if q.data in dispatch:
        fn, _ = dispatch[q.data]
        text = fn()
        await ctx.bot.send_message(chat_id=chat_id, text=text,
                                   parse_mode="Markdown", reply_markup=main_kb())

# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    import os
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    init_db()

    app = ApplicationBuilder().token(TOKEN).build()

    # Collector — catches ALL messages in groups/channels
    app.add_handler(MessageHandler(
        filters.ChatType.GROUPS | filters.ChatType.CHANNEL,
        collect_message
    ), group=0)

    # Commands — only respond in private chat
    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("help",      cmd_help))
    app.add_handler(CommandHandler("stats",     cmd_stats))
    app.add_handler(CommandHandler("chats",     cmd_chats))
    app.add_handler(CommandHandler("topusers",  cmd_topusers))
    app.add_handler(CommandHandler("recent",    cmd_recent))
    app.add_handler(CommandHandler("find",      cmd_find))
    app.add_handler(CommandHandler("chatlog",   cmd_chatlog))
    app.add_handler(CommandHandler("userstats", cmd_userstats))
    app.add_handler(CommandHandler("today",     cmd_today))
    app.add_handler(CallbackQueryHandler(on_callback))

    print("🤖 Chat monitor bot started")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
