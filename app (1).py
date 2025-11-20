import os
import discord
from discord.ext import commands
from discord import app_commands
import datetime
import asyncio
import io
import re
from typing import Dict, Set, Optional, Tuple, List

# NEW: persistence & exports
import sqlite3
import csv
from contextlib import closing

# --- Optional .env support for secrets (pip install python-dotenv) ---
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    # If python-dotenv isn't installed, we'll just rely on real env vars.
    pass

# ---- Token loader (token.txt only) ----
TOKEN_FILE = "token.txt"

def read_token_from_file() -> str:
    """
    Read the Discord bot token from token.txt.
    If the file doesn't exist (or is empty/commented), create it with instructions
    and raise a friendly RuntimeError explaining what to do.
    """
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            # read first non-empty, non-comment line
            lines = [ln.strip() for ln in f.readlines()]
            for ln in lines:
                if ln and not ln.startswith("#"):
                    return ln

        raise RuntimeError(
            f"{TOKEN_FILE} exists but is empty or only has comments.\n"
            f"Paste your bot token on a single line with no quotes or spaces."
        )

    # Create a starter file with instructions
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        f.write(
            "# Paste your Discord bot token on the next line (no quotes/spaces).\n"
            "# Example (remove these comments and put only your token):\n"
            "# MTAx...your.token.here...\n"
        )

    raise RuntimeError(
        f"No bot token found. I created {TOKEN_FILE} in this folder.\n"
        f"Open it, paste your token on a single line, then run the bot again."
    )

# ================== PERF SETTINGS ==================
# control whether to sync slash commands at startup (off by default)
SYNC_ON_STARTUP = os.getenv("SYNC_ON_STARTUP", "1") == "1"
# how many old events to restore views/schedules for on boot
RESTORE_EVENT_LIMIT = int(os.getenv("RESTORE_EVENT_LIMIT", "200"))
# pace heavy restore loops
RESTORE_YIELD_EVERY = 25

# Bot setup with required intents
intents = discord.Intents.default()
intents.message_content = True     # needed for DM wizard + relays
intents.members = True             # needed for noreact/alerts/reminders
intents.guilds = True

# Smaller in-memory cache + no guild chunking on startup (chunks only when needed)
bot = commands.Bot(
    command_prefix='?',
    intents=intents,
    max_messages=200,                         # default is 1000; this lowers RAM/CPU churn
    chunk_guilds_at_startup=False             # defer member chunking to when needed
)

# ===== Bot activity text =====
ACTIVITY_TEXT = "evade control the area - made by ! 𝓓𝔁𝓻𝓴"

# ================== Configuration (your existing) ==================
STAFF_ROLE_ID = 1352340626938663012
TRANSCRIPT_CHANNEL_ID = 1352346256646602912
APPLICATIONS_CATEGORY_ID = 1429847065013256302
VOICE_CHAT_TIMEOUT_HOURS = 48

# ================== RSVP v2 configuration (fallback defaults) ===================
STAFF_LOGS_CHANNEL_ID = 1417210823675220030  # default; now overridden per-guild in DB
AUTO_ROLE_ON_ACCEPT = 0  # default; overridden per-guild in DB (0 = disabled)
DB_PATH = "rsvp.sqlite3"
REMINDERS_BEFORE = [3 * 3600, 15 * 60]  # 3h, 15m before event

# Applications storage
active_applications = {}
user_to_channel = {}
channel_to_user = {}
voice_chat_tasks = {}

# Cooldown tracker for /alert (per guild)
last_alert_time: dict[int, datetime.datetime] = {}

# RSVP wizard state (creator DM flow)
rsvp_wizards: Dict[int, dict] = {}

# ====================== Time parsing helpers (UTC-friendly) ======================

WEEKDAY_MAP = {
    "monday": 0, "mon": 0,
    "tuesday": 1, "tue": 1, "tues": 1,
    "wednesday": 2, "wed": 2,
    "thursday": 3, "thu": 3, "thur": 3, "thurs": 3,
    "friday": 4, "fri": 4,
    "saturday": 5, "sat": 5,
    "sunday": 6, "sun": 6,
}

def _next_weekday_utc(target_wd: int, hour: int, minute: int) -> datetime.datetime:
    now = datetime.datetime.now(datetime.timezone.utc)
    days_ahead = (target_wd - now.weekday()) % 7
    candidate = (now + datetime.timedelta(days=days_ahead)).replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= now:
        candidate += datetime.timedelta(days=7)
    return candidate

def _safe_int(x: str, default: int) -> int:
    try:
        return int(x)
    except Exception:
        return default

def parse_time_to_unix(s: str) -> Optional[int]:
    """
    Try:
    - 'Friday 17:45'
    - 'YYYY-MM-DD HH:MM'
    - 'DD/MM HH:MM' (assumes current UTC year)
    Returns UTC unix timestamp or None.
    """
    if not s:
        return None
    s_clean = s.strip().lower()

    m = re.match(r"^\s*([a-z]+)\s+(\d{1,2}):(\d{2})\s*$", s_clean)
    if m:
        wd_name, hh, mm = m.group(1), m.group(2), m.group(3)
        if wd_name in WEEKDAY_MAP:
            hour = _safe_int(hh, 0)
            minute = _safe_int(mm, 0)
            dt = _next_weekday_utc(WEEKDAY_MAP[wd_name], hour, minute)
            return int(dt.timestamp())

    m = re.match(r"^\s*(\d{4})-(\d{2})-(\d{2})\s+(\d{1,2}):(\d{2})\s*$", s_clean)
    if m:
        y, mo, d, hh, mm = map(int, m.groups())
        try:
            dt = datetime.datetime(y, mo, d, hh, mm, tzinfo=datetime.timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return None

    m = re.match(r"^\s*(\d{1,2})/(\d{1,2})\s+(\d{1,2}):(\d{2})\s*$", s_clean)
    if m:
        d, mo, hh, mm = map(int, m.groups())
        y = datetime.datetime.now(datetime.timezone.utc).year
        try:
            dt = datetime.datetime(y, mo, d, hh, mm, tzinfo=datetime.timezone.utc)
            if dt < datetime.datetime.now(datetime.timezone.utc):
                dt = dt.replace(year=y + 1)
            return int(dt.timestamp())
        except Exception:
            return None

    return None

def render_discord_time(ts: int) -> str:
    return f"<t:{ts}:F> — <t:{ts}:R>"

def render_user_local_time(ts: int) -> str:
    dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
    return f"UTC: **{dt.strftime('%Y-%m-%d %H:%M')}** — {render_discord_time(ts)}"

# ====================== RSVP v2 — DB, settings, embeds, reminders, cleanup ======================

def _db_conn():
    # tuned SQLite connection for lower CPU/thrash
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    # WAL improves concurrency; NORMAL is a good perf/safety trade-off
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA cache_size=-8000;")  # ~8MB page cache in RAM
    return con

def db_init():
    with closing(_db_conn()) as con:
        cur = con.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS events(
            message_id INTEGER PRIMARY KEY,
            guild_id   INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            creator_id INTEGER NOT NULL,
            title      TEXT NOT NULL,
            description TEXT,
            time_ts    INTEGER,
            time_text  TEXT,
            image_url  TEXT,
            watermark  TEXT,
            locked     INTEGER DEFAULT 0,
            created_at INTEGER NOT NULL
        )""")
        cur.execute("""CREATE TABLE IF NOT EXISTS rsvps(
            message_id INTEGER NOT NULL,
            user_id    INTEGER NOT NULL,
            status     INTEGER NOT NULL,  -- 1=yes,0=no,2=maybe
            updated_at INTEGER NOT NULL,
            PRIMARY KEY(message_id,user_id)
        )""")
        cur.execute("""CREATE TABLE IF NOT EXISTS history(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER NOT NULL,
            user_id    INTEGER NOT NULL,
            from_status INTEGER,
            to_status   INTEGER,
            actor_id    INTEGER,
            note        TEXT,
            at          INTEGER NOT NULL
        )""")
        cur.execute("""CREATE TABLE IF NOT EXISTS optouts(
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            PRIMARY KEY(guild_id,user_id)
        )""")
        cur.execute("""CREATE TABLE IF NOT EXISTS attendance_stats(
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            yyyymm   TEXT NOT NULL,
            attended INTEGER DEFAULT 0,
            declined INTEGER DEFAULT 0,
            tentative INTEGER DEFAULT 0,
            PRIMARY KEY(guild_id,user_id,yyyymm)
        )""")
        cur.execute("""CREATE TABLE IF NOT EXISTS guild_settings(
            guild_id INTEGER PRIMARY KEY,
            enabled  INTEGER DEFAULT 1,
            staff_logs_channel_id INTEGER,
            auto_role_on_accept   INTEGER
        )""")
        # --- ensure reasons_channel_id exists (idempotent)
        try:
            cur.execute("ALTER TABLE guild_settings ADD COLUMN reasons_channel_id INTEGER")
        except Exception:
            pass

        # helpful indexes to speed common lookups during boot
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_guild ON events(guild_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_time ON events(time_ts, locked);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rsvps_msg ON rsvps(message_id);")
        con.commit()

def settings_get(guild_id: int) -> dict:
    with closing(_db_conn()) as con:
        cur = con.cursor()
        try:
            row = cur.execute(
                "SELECT enabled, staff_logs_channel_id, auto_role_on_accept, reasons_channel_id "
                "FROM guild_settings WHERE guild_id=?",
                (guild_id,)
            ).fetchone()
        except Exception:
            row = None

    if not row:
        return {
            "enabled": True,
            "staff_logs_channel_id": STAFF_LOGS_CHANNEL_ID,
            "auto_role_on_accept": AUTO_ROLE_ON_ACCEPT or 0,
            "reasons_channel_id": STAFF_LOGS_CHANNEL_ID,
        }

    enabled, logs_ch, auto_role, reasons_ch = row
    return {
        "enabled": bool(enabled),
        "staff_logs_channel_id": logs_ch or STAFF_LOGS_CHANNEL_ID,
        "auto_role_on_accept": auto_role or 0,
        "reasons_channel_id": reasons_ch or logs_ch or STAFF_LOGS_CHANNEL_ID,
    }

def settings_upsert(
    guild_id: int,
    enabled: Optional[bool] = None,
    staff_logs_channel_id: Optional[int] = None,
    auto_role_on_accept: Optional[int] = None,
    reasons_channel_id: Optional[int] = None,  # NEW
) -> None:
    cur_vals = settings_get(guild_id)
    if enabled is None:
        enabled = cur_vals["enabled"]
    if staff_logs_channel_id is None:
        staff_logs_channel_id = cur_vals["staff_logs_channel_id"]
    if auto_role_on_accept is None:
        auto_role_on_accept = cur_vals["auto_role_on_accept"]
    if reasons_channel_id is None:
        reasons_channel_id = cur_vals["reasons_channel_id"]
    with closing(_db_conn()) as con:
        cur = con.cursor()
        cur.execute("""
            INSERT INTO guild_settings(guild_id,enabled,staff_logs_channel_id,auto_role_on_accept,reasons_channel_id)
            VALUES(?,?,?,?,?)
            ON CONFLICT(guild_id) DO UPDATE SET
              enabled=excluded.enabled,
              staff_logs_channel_id=excluded.staff_logs_channel_id,
              auto_role_on_accept=excluded.auto_role_on_accept,
              reasons_channel_id=excluded.reasons_channel_id
        """, (guild_id, int(bool(enabled)), staff_logs_channel_id, auto_role_on_accept, reasons_channel_id))
        con.commit()

def _yyyymm(ts_utc: int) -> str:
    dt = datetime.datetime.fromtimestamp(ts_utc, tz=datetime.timezone.utc)
    return f"{dt.year:04d}-{dt.month:02d}"

async def staff_log(guild: discord.Guild, text: str):
    s = settings_get(guild.id)
    ch = guild.get_channel(s["staff_logs_channel_id"])
    if ch:
        try:
            await ch.send(text)
        except:
            pass

def jump_url(guild_id: int, channel_id: int, message_id: int) -> str:
    return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"

def _status_sets(con: sqlite3.Connection, message_id: int) -> Tuple[Set[int], Set[int], Set[int]]:
    cur = con.cursor()
    cur.execute("SELECT user_id, status FROM rsvps WHERE message_id=?", (message_id,))
    yes, no, maybe = set(), set(), set()
    for uid, st in cur.fetchall():
        (yes if st == 1 else no if st == 0 else maybe).add(uid)
    return yes, no, maybe

# ====== UPDATED: shows @handles instead of ping mentions in the three columns ======
def build_rsvp_embed_from_db(con: sqlite3.Connection, message_id: int, guild: discord.Guild) -> discord.Embed:
    cur = con.cursor()
    ev = cur.execute("""SELECT title, description, time_ts, time_text, image_url, watermark, creator_id, locked
                        FROM events WHERE message_id=?""", (message_id,)).fetchone()
    if not ev:
        return discord.Embed(title="(deleted)", description="Event not found.", color=discord.Color.red())
    title, desc, time_ts, time_text, image_url, watermark, creator_id, locked = ev
    yes, no, maybe = _status_sets(con, message_id)

    e = discord.Embed(
        title=title + ("  🔒" if locked else ""),
        description=desc or " ",
        color=discord.Color.yellow(),
        timestamp=datetime.datetime.now(datetime.timezone.utc)
    )
    e.add_field(name="⏰ Time", value=render_discord_time(time_ts) if time_ts else (time_text or "TBA"), inline=False)

    def human(ids: Set[int]) -> str:
        """
        Return comma-separated @handles (no ping), sorted, with a soft cap.
        Avoids Discord's large mention chips wrapping in columns.
        Works even if the member isn't cached: try guild.get_member, then bot.get_user.
        """
        MAX_SHOW = 25
        handles: List[str] = []
        for uid in ids:
            member = guild.get_member(uid)
            user = None if member else bot.get_user(uid)

            # Skip bots
            if member and member.bot:
                continue
            if (not member) and user and getattr(user, "bot", False):
                continue

            if member:
                name = (member.global_name or member.display_name or member.name)
            elif user:
                name = (getattr(user, "global_name", None) or user.name)
            else:
                name = None

            if not name:
                name = f"user-{uid}"  # last-resort label

            name = name.strip().lstrip('@')
            handles.append(f"@{name}")

        if not handles:
            return "—"
        handles.sort(key=str.lower)
        shown = handles[:MAX_SHOW]
        more = len(handles) - len(shown)
        text = ", ".join(shown)
        if more > 0:
            text += f" … (+{more} more)"
        return text

    e.add_field(name=f"✅ Accepted ({len(yes)})", value=human(yes), inline=True)
    e.add_field(name=f"❌ Declined ({len(no)})", value=human(no), inline=True)
    e.add_field(name=f"❓ Tentative ({len(maybe)})", value=human(maybe), inline=True)

    if watermark:
        e.set_footer(text=watermark)
    else:
        creator = guild.get_member(creator_id)
        # footer doesn't ping, using mention here is fine; falls back to raw ID if missing
        e.set_footer(text=f"Created by {creator.mention if creator else f'<@{creator_id}>'}")

    if image_url:
        e.set_image(url=image_url)
    return e
# ====== /UPDATED ======

def build_ics_bytes(title: str, ts_start: int) -> bytes:
    dt = datetime.datetime.fromtimestamp(ts_start, tz=datetime.timezone.utc)
    dt_end = dt + datetime.timedelta(hours=1)
    fmt = lambda d: d.strftime("%Y%m%dT%H%M%SZ")
    uid = f"{int(datetime.datetime.now(datetime.timezone.utc).timestamp())}@rsvp"
    ics = (
        "BEGIN:VCALENDAR\nVERSION:2.0\nPRODID:-//Evade RSVP//EN\nBEGIN:VEVENT\n"
        f"UID:{uid}\nDTSTAMP:{fmt(datetime.datetime.now(datetime.timezone.utc))}\n"
        f"DTSTART:{fmt(dt)}\nDTEND:{fmt(dt_end)}\nSUMMARY:{title}\nEND:VEVENT\nEND:VCALENDAR\n"
    )
    return ics.encode()

# Reminders & cleanup scheduling
reminder_tasks: Dict[int, List[asyncio.Task]] = {}
cleanup_tasks: Dict[int, asyncio.Task] = {}

async def schedule_reminders_and_cleanup(message_id: int):
    with closing(_db_conn()) as con:
        cur = con.cursor()
        row = cur.execute("SELECT guild_id, channel_id, title, time_ts FROM events WHERE message_id=?", (message_id,)).fetchone()
        if not row:
            return
        guild_id, channel_id, title, time_ts = row

    for t in reminder_tasks.pop(message_id, []):
        t.cancel()
    if cleanup_tasks.get(message_id):
        cleanup_tasks[message_id].cancel()

    now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    tasks = []
    if time_ts:
        for off in REMINDERS_BEFORE:
            run_at = time_ts - off
            if run_at <= now:
                continue
            tasks.append(asyncio.create_task(_reminder_job(message_id, off, run_at - now)))
        reminder_tasks[message_id] = tasks
        cleanup_delay = max(0, (time_ts + 10 * 60) - now)  # 10m after start
        cleanup_tasks[message_id] = asyncio.create_task(_cleanup_job(message_id, cleanup_delay))

async def _reminder_job(message_id: int, offset: int, delay: int):
    await asyncio.sleep(delay)
    with closing(_db_conn()) as con:
        cur = con.cursor()
        # fetch with image_url so we can use thumbnail in embeds
        ev = cur.execute("SELECT guild_id, channel_id, title, time_ts, image_url FROM events WHERE message_id=?", (message_id,)).fetchone()
        if not ev:
            return
        guild_id, channel_id, title, time_ts, ev_image = ev
        yes, no, maybe = _status_sets(con, message_id)

    guild = bot.get_guild(guild_id)
    if not guild:
        return

    reacted = yes | no | maybe
    with closing(_db_conn()) as con:
        cur = con.cursor()
        cur.execute("SELECT user_id FROM optouts WHERE guild_id=?", (guild_id,))
        opted = {uid for (uid,) in cur.fetchall()}

    missing = [m for m in guild.members if (not m.bot) and (m.id not in reacted) and (m.id not in opted)]
    jurl = jump_url(guild_id, channel_id, message_id)
    when = "3 hours" if offset >= 3600 else "15 minutes"
    sent = 0
    for m in missing:
        try:
            dm = await m.create_dm()
            ts_line = render_user_local_time(time_ts)
            emb = discord.Embed(
                title=f"⏰ Reminder: {title} ({when})",
                description=f"{ts_line}\n\nSet your status: {jurl}",
                color=discord.Color.blurple(),
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            )
            # Use the event's image as a thumbnail if present
            if ev_image:
                emb.set_thumbnail(url=ev_image)

            await dm.send(embed=emb)
            sent += 1
            await asyncio.sleep(0.12)  # gentle throttle
        except:
            pass
    await staff_log(guild, f"📬 Reminder T-{when}: sent to **{sent}** non-reactors for `{title}`.")

async def _cleanup_job(message_id: int, delay: int):
    await asyncio.sleep(delay)
    with closing(_db_conn()) as con:
        cur = con.cursor()
        ev = cur.execute("""SELECT guild_id, channel_id, title, time_ts, locked
                            FROM events WHERE message_id=?""", (message_id,)).fetchone()
        if not ev:
            return
        guild_id, channel_id, title, time_ts, locked = ev
        if locked:
            return
        cur.execute("UPDATE events SET locked=1 WHERE message_id=?", (message_id,))
        con.commit()
        yes, no, maybe = _status_sets(con, message_id)

    guild = bot.get_guild(guild_id)
    if not guild:
        return
    channel = guild.get_channel(channel_id)
    if not channel:
        return
    try:
        msg = await channel.fetch_message(message_id)
    except:
        return

    view = RSVPView(message_id, locked=True)
    with closing(_db_conn()) as con:
        embed = build_rsvp_embed_from_db(con, message_id, guild)
    try:
        await msg.edit(embed=embed, view=view)
    except:
        pass

    # update monthly attendance stats
    if time_ts:
        yyyymm = _yyyymm(time_ts)
        with closing(_db_conn()) as con:
            cur = con.cursor()
            for uid in yes:
                cur.execute("""INSERT INTO attendance_stats(guild_id,user_id,yyyymm,attended,declined,tentative)
                               VALUES(?,?,?,?,?,?)
                               ON CONFLICT(guild_id,user_id,yyyymm) DO UPDATE SET attended=attended+1""",
                            (guild_id, uid, yyyymm, 1, 0, 0))
            for uid in no:
                cur.execute("""INSERT INTO attendance_stats(guild_id,user_id,yyyymm,attended,declined,tentative)
                               VALUES(?,?,?,?,?,?)
                               ON CONFLICT(guild_id,user_id,yyyymm) DO UPDATE SET declined=declined+1""",
                            (guild_id, uid, yyyymm, 0, 1, 0))
            for uid in maybe:
                cur.execute("""INSERT INTO attendance_stats(guild_id,user_id,yyyymm,attended,declined,tentative)
                               VALUES(?,?,?,?,?,?)
                               ON CONFLICT(guild_id,user_id,yyyymm) DO UPDATE SET tentative=tentative+1""",
                            (guild_id, uid, yyyymm, 0, 0, 1))
            con.commit()

    # CSV archive for staff logs
    csv_buf = io.StringIO()
    writer = csv.writer(csv_buf)
    writer.writerow(["user_id", "mention", "status"])
    def s_name(uid):
        return "Accepted" if uid in yes else "Declined" if uid in no else "Tentative"
    for uid in (yes | no | maybe):
        m = guild.get_member(uid)
        writer.writerow([uid, m.mention if m else f"<@{uid}>", s_name(uid)])
    csv_file = discord.File(io.BytesIO(csv_buf.getvalue().encode()), filename=f"rsvp-{message_id}-attendees.csv")

    summary = f"**Summary:** ✅ {len(yes)} · ❌ {len(no)} · ❓ {len(maybe)}"
    await channel.send(f"🔒 Event closed — {summary}")

    await staff_log(guild, f"📦 Archived `{title}` — {summary}\n{jump_url(guild_id, channel_id, message_id)}")
    try:
        s = settings_get(guild.id)
        staff_ch = guild.get_channel(s["staff_logs_channel_id"])
        if staff_ch:
            await staff_ch.send(file=csv_file)
    except:
        pass

# ====================== Reason capture (Declined/Tentative) ======================

async def _post_reason_log(guild: discord.Guild, user: discord.Member, event_title: str,
                           status_name: str, reason: str, jump: str):
    """Send a nice embed to the configured reasons channel."""
    s = settings_get(guild.id)
    ch = guild.get_channel(s["reasons_channel_id"]) or guild.get_channel(s["staff_logs_channel_id"])
    if not ch:
        return
    color = discord.Color.red() if status_name == "Declined" else discord.Color.dark_grey()
    embed = discord.Embed(
        title=f"📝 {status_name} reason — {event_title}",
        description=(reason or "_(no reason provided)_"),
        color=color,
        timestamp=datetime.datetime.now(datetime.timezone.utc)
    )
    embed.add_field(name="User", value=user.mention, inline=True)
    embed.add_field(name="Event", value=event_title, inline=True)
    embed.add_field(name="Message", value=f"[Jump to event]({jump})", inline=False)
    await ch.send(embed=embed)

class RSVPReasonModal(discord.ui.Modal, title="Tell us why"):
    """Modal shown when a user selects Declined or Tentative."""
    def __init__(self, message_id: int, target_status: int):  # 0=Declined, 2=Tentative
        super().__init__(timeout=None)
        self.message_id = message_id
        self.target_status = target_status
        placeholder = "Why can't you make it?" if target_status == 0 else "What's uncertain?"
        self.reason = discord.ui.TextInput(
            label="Reason (optional)",
            placeholder=placeholder,
            required=False,
            max_length=500
        )
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        uid = interaction.user.id
        now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        with closing(_db_conn()) as con:
            cur = con.cursor()
            ev = cur.execute("SELECT guild_id, channel_id, title, locked FROM events WHERE message_id=?",
                             (self.message_id,)).fetchone()
            if not ev:
                await interaction.response.send_message("Event missing.", ephemeral=True)
                return
            guild_id, channel_id, title, locked = ev
            if locked:
                await interaction.response.send_message("This event is closed.", ephemeral=True)
                return
            row = cur.execute("SELECT status FROM rsvps WHERE message_id=? AND user_id=?",
                              (self.message_id, uid)).fetchone()
            prev = row[0] if row else None
            # upsert status
            cur.execute("""INSERT INTO rsvps(message_id,user_id,status,updated_at)
                           VALUES(?,?,?,?)
                           ON CONFLICT(message_id,user_id) DO UPDATE SET status=?,updated_at=?""",
                        (self.message_id, uid, self.target_status, now, self.target_status, now))
            # store reason in history.note
            cur.execute("""INSERT INTO history(message_id,user_id,from_status,to_status,actor_id,note,at)
                           VALUES(?,?,?,?,?,?,?)""",
                        (self.message_id, uid, prev, self.target_status, interaction.user.id, str(self.reason.value or ""), now))
            con.commit()

        # auto-role alignment
        s = settings_get(guild_id)
        if s["auto_role_on_accept"] is not None and s["auto_role_on_accept"]:
            try:
                role = interaction.guild.get_role(s["auto_role_on_accept"])
                member = interaction.guild.get_member(uid)
                if role and member:
                    if self.target_status == 1 and role not in member.roles:
                        await member.add_roles(role, reason="RSVP ✅")
                    elif self.target_status == 0 and role in member.roles:
                        await member.remove_roles(role, reason="RSVP ❌")
            except:
                pass

        # refresh message UI
        try:
            msg = await interaction.channel.fetch_message(self.message_id)
            with closing(_db_conn()) as con:
                await msg.edit(embed=build_rsvp_embed_from_db(con, self.message_id, interaction.guild), view=RSVPView(self.message_id))
        except:
            pass

        # log the reason
        await _post_reason_log(
            interaction.guild,
            interaction.user,
            title,
            "Declined" if self.target_status == 0 else "Tentative",
            self.reason.value or "",
            jump_url(guild_id, channel_id, self.message_id)
        )

        names = {None: "None", 0: "Declined", 1: "Accepted", 2: "Tentative"}
        await staff_log(interaction.guild, f"🧾 {interaction.user.mention} changed **{names.get(prev)} → {names[self.target_status]}** on `{title}` (reason captured).")
        await interaction.response.send_message(f"Updated to **{names[self.target_status]}**. Thanks for the context!", ephemeral=True)

# ====================== RSVP UI (buttons) ======================

class RSVPView(discord.ui.View):
    def __init__(self, message_id: int, locked: bool = False):
        super().__init__(timeout=None)
        self.message_id = message_id
        if locked:
            for child in self.children:
                child.disabled = True

    @discord.ui.button(label="Add to calendar (.ics)", style=discord.ButtonStyle.secondary)
    async def ics_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        with closing(_db_conn()) as con:
            cur = con.cursor()
            row = cur.execute("SELECT title, time_ts FROM events WHERE message_id=?", (self.message_id,)).fetchone()
        if not row or not row[1]:
            await interaction.response.send_message("No real timestamp to export.", ephemeral=True)
            return
        title, ts = row
        safe_name = re.sub(r"[^A-Za-z0-9_-]+", "_", title)
        file = discord.File(io.BytesIO(build_ics_bytes(title, ts)), filename=f"{safe_name}.ics")
        await interaction.response.send_message("Here you go:", file=file, ephemeral=True)

    async def _set(self, interaction: discord.Interaction, target: int):
        uid = interaction.user.id
        now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        with closing(_db_conn()) as con:
            cur = con.cursor()
            ev = cur.execute("SELECT guild_id, channel_id, creator_id, locked, title FROM events WHERE message_id=?",
                             (self.message_id,)).fetchone()
            if not ev:
                await interaction.response.send_message("Event missing.", ephemeral=True)
                return
            guild_id, channel_id, creator_id, locked, title = ev
            if locked:
                await interaction.response.send_message("This event is closed.", ephemeral=True)
                return
            # prior status
            row = cur.execute("SELECT status FROM rsvps WHERE message_id=? AND user_id=?",
                              (self.message_id, uid)).fetchone()
            prev = row[0] if row else None
            cur.execute("""INSERT INTO rsvps(message_id,user_id,status,updated_at)
                           VALUES(?,?,?,?)
                           ON CONFLICT(message_id,user_id) DO UPDATE SET status=?,updated_at=?""",
                        (self.message_id, uid, target, now, target, now))
            cur.execute("""INSERT INTO history(message_id,user_id,from_status,to_status,actor_id,note,at)
                           VALUES(?,?,?,?,?,?,?)""",
                        (self.message_id, uid, prev, target, interaction.user.id, None, now))
            con.commit()

        names = {None: "None", 0: "Declined", 1: "Accepted", 2: "Tentative"}
        await staff_log(interaction.guild, f"🧾 {interaction.user.mention} changed **{names.get(prev)} → {names[target]}** on `{title}`")

        # Auto-role manage (per-guild)
        s = settings_get(interaction.guild_id)
        if s["auto_role_on_accept"]:
            try:
                role = interaction.guild.get_role(s["auto_role_on_accept"])
                if role:
                    member = interaction.guild.get_member(uid)
                    if member:
                        if target == 1 and role not in member.roles:
                            await member.add_roles(role, reason="RSVP ✅")
                        elif target == 0 and role in member.roles:
                            await member.remove_roles(role, reason="RSVP ❌")
            except:
                pass

        # Refresh UI
        try:
            msg = await interaction.channel.fetch_message(self.message_id)
            with closing(_db_conn()) as con:
                await msg.edit(embed=build_rsvp_embed_from_db(con, self.message_id, interaction.guild), view=self)
        except:
            pass

        await interaction.response.send_message(f"Updated to **{names[target]}**.", ephemeral=True)

    @discord.ui.button(label="Accepted ✅", style=discord.ButtonStyle.success)
    async def yes_btn(self, i: discord.Interaction, _: discord.ui.Button):
        if not settings_get(i.guild_id)["enabled"]:
            await i.response.send_message("React Event system is disabled in this server.", ephemeral=True)
            return
        await self._set(i, 1)

    @discord.ui.button(label="Declined ❌", style=discord.ButtonStyle.danger)
    async def no_btn(self, i: discord.Interaction, _: discord.ui.Button):
        if not settings_get(i.guild_id)["enabled"]:
            await i.response.send_message("React Event system is disabled in this server.", ephemeral=True)
            return
        # Ask for a reason
        await i.response.send_modal(RSVPReasonModal(self.message_id, 0))

    @discord.ui.button(label="Tentative ❓", style=discord.ButtonStyle.secondary)
    async def maybe_btn(self, i: discord.Interaction, _: discord.ui.Button):
        if not settings_get(i.guild_id)["enabled"]:
            await i.response.send_message("React Event system is disabled in this server.", ephemeral=True)
            return
        # Ask for a reason
        await i.response.send_modal(RSVPReasonModal(self.message_id, 2))

# ====================== Noreact UI: paginate + role filter + DM all ======================

def _event_meta(message_id: int) -> Optional[tuple[int, int, str, Optional[int], Optional[str]]]:
    """
    Returns (guild_id, channel_id, title, time_ts, image_url) for an event.
    """
    with closing(_db_conn()) as con:
        cur = con.cursor()
        row = cur.execute(
            "SELECT guild_id, channel_id, title, time_ts, image_url FROM events WHERE message_id=?",
            (message_id,)
        ).fetchone()
    return row if row else None

def _missing_members_for_event(guild: discord.Guild, message_id: int, role_id: Optional[int] = None) -> List[discord.Member]:
    """
    Members who haven't reacted (and didn't opt-out). Excludes bots. Optional role filter.
    """
    with closing(_db_conn()) as con:
        yes, no, maybe = _status_sets(con, message_id)
        reacted = yes | no | maybe
        cur = con.cursor()
        cur.execute("SELECT user_id FROM optouts WHERE guild_id=?", (guild.id,))
        opted = {uid for (uid,) in cur.fetchall()}

    def eligible(m: discord.Member) -> bool:
        if m.bot:
            return False
        if m.id in reacted:
            return False
        if m.id in opted:
            return False
        if role_id:
            role = guild.get_role(role_id)
            if not role or role not in m.roles:
                return False
        return True

    return [m for m in guild.members if eligible(m)]

def _handles(members: List[discord.Member], cap: Optional[int] = None) -> str:
    """
    Render a comma-separated list of @handles (no ping), sorted. Optional cap.
    """
    names = []
    for m in members:
        name = (m.global_name or m.display_name or m.name or f"user-{m.id}").strip().lstrip('@')
        names.append(f"@{name}")
    names.sort(key=str.lower)
    if cap is not None and len(names) > cap:
        return ", ".join(names[:cap]) + f" … (+{len(names)-cap} more)"
    return ", ".join(names) if names else "—"

class RoleFilter(discord.ui.Select):
    def __init__(self, options: List[discord.SelectOption], parent: "NoreactView"):
        super().__init__(placeholder="Filter by role…", min_values=1, max_values=1, options=options, row=0)
        self.parent_view = parent

    async def callback(self, interaction: discord.Interaction):
        val = self.values[0]
        self.parent_view.role_id = None if val == "0" else int(val)
        self.parent_view.page = 0
        await interaction.response.edit_message(embed=self.parent_view._embed(interaction.guild), view=self.parent_view)

class NoreactView(discord.ui.View):
    PAGE_SIZE = 25  # show 25 names per page

    def __init__(self, guild_id: int, message_id: int, role_id: Optional[int] = None, page: int = 0):
        super().__init__(timeout=180)
        self.guild_id = guild_id
        self.message_id = message_id
        self.role_id = role_id
        self.page = page

        # Build role selector options (max 25 options total)
        guild = bot.get_guild(guild_id)
        options: List[discord.SelectOption] = [discord.SelectOption(label="All members", value="0", default=(role_id is None))]
        if guild:
            # pick up to 24 roles (skip @everyone, managed/integration roles often noisy)
            roles = [r for r in sorted(guild.roles, key=lambda r: r.position, reverse=True)
                     if not r.is_default()][:24]
            for r in roles:
                options.append(discord.SelectOption(
                    label=(r.name[:95] + "…" if len(r.name) > 96 else r.name),
                    value=str(r.id),
                    default=(self.role_id == r.id)
                ))
        self.add_item(RoleFilter(options, self))

    def _current_list(self) -> List[discord.Member]:
        guild = bot.get_guild(self.guild_id)
        if not guild:
            return []
        return _missing_members_for_event(guild, self.message_id, self.role_id)

    def _embed(self, guild: discord.Guild) -> discord.Embed:
        missing = self._current_list()
        total = len(missing)
        pages = max(1, (total + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        self.page = max(0, min(self.page, pages - 1))

        start = self.page * self.PAGE_SIZE
        end = start + self.PAGE_SIZE
        chunk = missing[start:end]

        ev = _event_meta(self.message_id)
        title = ev[2] if ev else f"Event {self.message_id}"
        role_txt = "All members" if not self.role_id else (guild.get_role(self.role_id).mention if guild.get_role(self.role_id) else f"role:{self.role_id}")

        e = discord.Embed(
            title=f"🙋 Non-reactors — {title}",
            description=f"Filter: **{role_txt}**\nTotal missing: **{total}**\nPage **{self.page+1}/{pages}**",
            color=discord.Color.orange(),
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        if chunk:
            # show numbered list with non-pinging handles
            lines = []
            for idx, m in enumerate(chunk, start=1 + start):
                name = (m.global_name or m.display_name or m.name)
                lines.append(f"`{idx:>3}.` @{name}")
            e.add_field(name="Users", value="\n".join(lines), inline=False)
        else:
            e.add_field(name="Users", value="—", inline=False)

        evj = _event_meta(self.message_id)
        if evj:
            jurl = jump_url(evj[0], evj[1], self.message_id)
            e.add_field(name="Event", value=f"[Jump]({jurl})", inline=True)

        return e

    @discord.ui.button(label="⟵ Prev", style=discord.ButtonStyle.secondary, row=1)
    async def prev_btn(self, i: discord.Interaction, _: discord.ui.Button):
        self.page = max(0, self.page - 1)
        await i.response.edit_message(embed=self._embed(i.guild), view=self)

    @discord.ui.button(label="Next ⟶", style=discord.ButtonStyle.secondary, row=1)
    async def next_btn(self, i: discord.Interaction, _: discord.ui.Button):
        missing = self._current_list()
        pages = max(1, (len(missing) + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        self.page = min(self.page + 1, pages - 1)
        await i.response.edit_message(embed=self._embed(i.guild), view=self)

    @discord.ui.button(label="📨 DM all missing", style=discord.ButtonStyle.primary, row=1)
    async def dm_all_btn(self, i: discord.Interaction, _: discord.ui.Button):
        # Security: allow only admins or Manage Messages
        if not (i.user.guild_permissions.administrator or i.user.guild_permissions.manage_messages):
            await i.response.send_message("❌ You need Manage Messages (or Admin) to DM everyone.", ephemeral=True)
            return

        ev = _event_meta(self.message_id)
        if not ev:
            await i.response.send_message("Event not found.", ephemeral=True); return
        guild_id, channel_id, title, time_ts, image_url = ev
        jurl = jump_url(guild_id, channel_id, self.message_id)

        missing = self._current_list()
        sent = 0
        failed = 0

        when_text = render_user_local_time(time_ts) if time_ts else "Time: **TBA**"
        for m in missing:
            try:
                dm = await m.create_dm()
                emb = discord.Embed(
                    title=f"⏰ Reminder: {title}",
                    description=(f"{when_text}\n\nSet your status: {jurl}"),
                    color=discord.Color.blurple(),
                    timestamp=datetime.datetime.now(datetime.timezone.utc)
                )
                if image_url:
                    emb.set_thumbnail(url=image_url)
                await dm.send(embed=emb)
                sent += 1
                await asyncio.sleep(0.12)
            except Exception:
                failed += 1

        await staff_log(i.guild, f"📬 Manual noreact DM: **{sent}** sent, **{failed}** failed — `{title}` (filter: {'all' if not self.role_id else self.role_id}).")
        await i.response.send_message(f"✅ DM job finished — Delivered: **{sent}**, Failed: **{failed}**.", ephemeral=True)

    @discord.ui.button(label="🔄 Refresh", style=discord.ButtonStyle.secondary, row=1)
    async def refresh_btn(self, i: discord.Interaction, _: discord.ui.Button):
        await i.response.edit_message(embed=self._embed(i.guild), view=self)

# ====================== RSVP Commands (create/edit/close/etc.) ======================

def _can_edit(interaction: discord.Interaction, message_id: int) -> bool:
    with closing(_db_conn()) as con:
        cur = con.cursor()
        row = cur.execute("SELECT creator_id FROM events WHERE message_id=?", (message_id,)).fetchone()
    if not row:
        return False
    creator_id = row[0]
    return (interaction.user.id == creator_id) or interaction.user.guild_permissions.manage_messages or interaction.user.guild_permissions.administrator

@bot.tree.command(name="create_react", description="Create a React Event via DM wizard")
@app_commands.checks.has_permissions(manage_messages=True)
async def create_react(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("Run this in a server channel.", ephemeral=True)
        return
    if not settings_get(interaction.guild_id)["enabled"]:
        await interaction.response.send_message("React Event system is disabled in this server.", ephemeral=True)
        return
    try:
        dm = await interaction.user.create_dm()
    except discord.Forbidden:
        await interaction.response.send_message("I can't DM you. Enable DMs then try again.", ephemeral=True)
        return

    wiz = {
        "channel_id": interaction.channel.id,
        "guild_id": interaction.guild_id,
        "step": 1,
        "data": {"title": None, "description": None, "time_text": None, "time_ts": None, "image_url": None, "watermark": None}
    }
    rsvp_wizards[interaction.user.id] = wiz
    await interaction.response.send_message("📩 Check your DMs — we'll set up the React Event.", ephemeral=True)
    await dm.send("**React Event Creator**\n1) Send **title**.\nType `back` to go back, `skip` to skip, `cancel` to stop.\n_(You'll get a live preview as we go.)_)")

def _wizard_preview_embed(author: discord.User, wiz: dict, guild: discord.Guild) -> discord.Embed:
    e = discord.Embed(
        title=(wiz["data"]["title"] or "(title)"),
        description=(wiz["data"]["description"] or "(description)"),
        color=discord.Color.yellow(),
        timestamp=datetime.datetime.now(datetime.timezone.utc)
    )
    ts = wiz["data"]["time_ts"]; tt = wiz["data"]["time_text"]
    e.add_field(name="⏰ Time", value=render_discord_time(ts) if ts else (tt or "TBA"), inline=False)
    e.add_field(name="✅ Accepted (0)", value="—", inline=True)
    e.add_field(name="❌ Declined (0)", value="—", inline=True)
    e.add_field(name="❓ Tentative (0)", value="—", inline=True)
    if wiz["data"]["watermark"]:
        e.set_footer(text=wiz["data"]["watermark"])
    else:
        e.set_footer(text=f"Created by {author.mention}")
    if wiz["data"]["image_url"]:
        e.set_image(url=wiz["data"]["image_url"])
    return e

class EditRSVPModal(discord.ui.Modal, title="Edit React Event"):
    def __init__(self, message_id: int):
        super().__init__(timeout=None)
        self.message_id = message_id
        self.title_input = discord.ui.TextInput(label="Title (blank = keep)", required=False, max_length=256)
        self.desc_input  = discord.ui.TextInput(label="Description (blank = keep)", style=discord.TextStyle.paragraph, required=False, max_length=4000)
        self.time_input  = discord.ui.TextInput(label="Time (e.g. 'Friday 17:45' or '2025-01-02 17:45') (blank = keep)", required=False)
        self.image_input = discord.ui.TextInput(label="Image URL (blank = keep, 'none' = remove)", required=False)
        self.watermark   = discord.ui.TextInput(label="Footer watermark (blank = keep, 'none' = remove)", required=False, max_length=128)
        for x in (self.title_input, self.desc_input, self.time_input, self.image_input, self.watermark):
            self.add_item(x)

    async def on_submit(self, interaction: discord.Interaction):
        if not settings_get(interaction.guild_id)["enabled"]:
            await interaction.response.send_message("React Event system is disabled in this server.", ephemeral=True)
            return
        if not _can_edit(interaction, self.message_id):
            await interaction.response.send_message("You can't edit this event.", ephemeral=True)
            return
        changes = []
        with closing(_db_conn()) as con:
            cur = con.cursor()
            if self.title_input.value:
                cur.execute("UPDATE events SET title=? WHERE message_id=?", (self.title_input.value, self.message_id)); changes.append("title")
            if self.desc_input.value:
                cur.execute("UPDATE events SET description=? WHERE message_id=?", (self.desc_input.value, self.message_id)); changes.append("description")
            if self.time_input.value:
                ts = parse_time_to_unix(self.time_input.value)
                if ts:
                    cur.execute("UPDATE events SET time_ts=?, time_text=NULL WHERE message_id=?", (ts, self.message_id))
                else:
                    cur.execute("UPDATE events SET time_ts=NULL, time_text=? WHERE message_id=?", (self.time_input.value, self.message_id))
                changes.append("time")
            if self.image_input.value:
                cur.execute("UPDATE events SET image_url=? WHERE message_id=?", (None if self.image_input.value.lower() == "none" else self.image_input.value, self.message_id)); changes.append("image")
            if self.watermark.value:
                cur.execute("UPDATE events SET watermark=? WHERE message_id=?", (None if self.watermark.value.lower() == "none" else self.watermark.value, self.message_id)); changes.append("watermark")
            con.commit()

        try:
            msg = await interaction.channel.fetch_message(self.message_id)
            with closing(_db_conn()) as con:
                await msg.edit(embed=build_rsvp_embed_from_db(con, self.message_id, interaction.guild), view=RSVPView(self.message_id))
        except:
            pass
        await schedule_reminders_and_cleanup(self.message_id)
        await staff_log(interaction.guild, f"🛠️ React Event edited by {interaction.user.mention}: {', '.join(changes) or 'no changes'}.")
        await interaction.response.send_message("✅ Updated.", ephemeral=True)

@bot.tree.command(name="edit_react", description="Edit a React Event in this channel")
@app_commands.describe(message_id="Message ID")
async def edit_react(interaction: discord.Interaction, message_id: str):
    if not settings_get(interaction.guild_id)["enabled"]:
        await interaction.response.send_message("React Event system is disabled in this server.", ephemeral=True)
        return
    try:
        mid = int(message_id)
    except:
        await interaction.response.send_message("Invalid message_id.", ephemeral=True)
        return
    if not _can_edit(interaction, mid):
        await interaction.response.send_message("You can't edit this event.", ephemeral=True)
        return
    await interaction.response.send_modal(EditRSVPModal(mid))

@bot.tree.command(name="close_react", description="Close/lock a React Event and archive it")
@app_commands.describe(message_id="Message ID")
async def close_react(interaction: discord.Interaction, message_id: str):
    if not settings_get(interaction.guild_id)["enabled"]:
        await interaction.response.send_message("React Event system is disabled in this server.", ephemeral=True)
        return
    try:
        mid = int(message_id)
    except:
        await interaction.response.send_message("Invalid message_id.", ephemeral=True)
        return
    if not _can_edit(interaction, mid):
        await interaction.response.send_message("You can't close this event.", ephemeral=True)
        return
    if cleanup_tasks.get(mid):
        cleanup_tasks[mid].cancel()
    cleanup_tasks[mid] = asyncio.create_task(_cleanup_job(mid, 0))
    await interaction.response.send_message("🔒 Closing & archiving…", ephemeral=True)

# ======= NEW /noreact command with paginate + role filter + DM all =======
@bot.tree.command(name="noreact", description="Interactive list of users who haven't reacted (paginate, filter by role, DM all)")
@app_commands.describe(message_id="Event message ID")
async def noreact(interaction: discord.Interaction, message_id: str):
    if not settings_get(interaction.guild_id)["enabled"]:
        await interaction.response.send_message("React Event system is disabled in this server.", ephemeral=True)
        return
    try:
        mid = int(message_id)
    except:
        await interaction.response.send_message("Invalid message_id.", ephemeral=True)
        return

    ev = _event_meta(mid)
    if not ev or ev[0] != interaction.guild_id:
        await interaction.response.send_message("Event not found in this server.", ephemeral=True)
        return

    view = NoreactView(interaction.guild_id, mid)
    await interaction.response.send_message(
        embed=view._embed(interaction.guild),
        view=view,
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions(users=False, roles=False, everyone=False, replied_user=False)
    )

@bot.tree.command(name="attendance", description="Show a user's attendance stats this month")
@app_commands.describe(user="User to check")
async def attendance(interaction: discord.Interaction, user: discord.Member):
    if not settings_get(interaction.guild_id)["enabled"]:
        await interaction.response.send_message("React Event system is disabled in this server.", ephemeral=True)
        return
    yyyymm = _yyyymm(int(datetime.datetime.now(datetime.timezone.utc).timestamp()))
    with closing(_db_conn()) as con:
        cur = con.cursor()
        row = cur.execute("""SELECT attended, declined, tentative FROM attendance_stats
                             WHERE guild_id=? AND user_id=? AND yyyymm=?""",
                          (interaction.guild_id, user.id, yyyymm)).fetchone()
    a, d, t = row if row else (0, 0, 0)
    await interaction.response.send_message(f"📊 **{user.mention} — {yyyymm}**\n✅ Attended: **{a}**\n❌ Declined: **{d}**\n❓ Tentative: **{t}**", ephemeral=True)

class EventsPager(discord.ui.View):
    def __init__(self, guild_id: int, page: int = 0):
        super().__init__(timeout=60)
        self.guild_id = guild_id
        self.page = page

    def _fetch(self):
        with closing(_db_conn()) as con:
            cur = con.cursor()
            rows = cur.execute("""SELECT message_id,title,time_ts,locked,channel_id FROM events
                                  WHERE guild_id=? ORDER BY created_at DESC LIMIT 5 OFFSET ?""",
                                (self.guild_id, self.page * 5)).fetchall()
        return rows

    def _embed(self, guild: discord.Guild) -> discord.Embed:
        rows = self._fetch()
        e = discord.Embed(title=f"📅 Events (page {self.page + 1})", color=discord.Color.blurple())
        if not rows:
            e.description = "_No events._"
        for mid, title, time_ts, locked, chid in rows:
            line = f"• **{title}** — {'🔒 closed' if locked else 'open'} — {render_discord_time(time_ts) if time_ts else 'TBA'}\n"
            line += f"[Open]({jump_url(guild.id, chid, mid)}) • `/edit_react {mid}` • `/close_react {mid}` • `/clone_event {mid}`"
            e.add_field(name="\u200b", value=line, inline=False)
        return e

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary)
    async def prev(self, i: discord.Interaction, _: discord.ui.Button):
        self.page = max(0, self.page - 1)
        await i.response.edit_message(embed=self._embed(i.guild), view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next(self, i: discord.Interaction, _: discord.ui.Button):
        self.page += 1
        await i.response.edit_message(embed=self._embed(i.guild), view=self)

@bot.tree.command(name="events", description="List & manage events")
async def events(interaction: discord.Interaction):
    if not settings_get(interaction.guild_id)["enabled"]:
        await interaction.response.send_message("React Event system is disabled in this server.", ephemeral=True)
        return
    view = EventsPager(interaction.guild_id, 0)
    await interaction.response.send_message(embed=view._embed(interaction.guild), view=view, ephemeral=True)

@bot.tree.command(name="clone_event", description="Clone an event to reuse layout")
@app_commands.describe(message_id="Message ID to clone")
async def clone_event(interaction: discord.Interaction, message_id: str):
    if not settings_get(interaction.guild_id)["enabled"]:
        await interaction.response.send_message("React Event system is disabled in this server.", ephemeral=True)
        return
    try:
        mid = int(message_id)
    except:
        await interaction.response.send_message("Invalid message_id.", ephemeral=True)
        return
    with closing(_db_conn()) as con:
        cur = con.cursor()
        row = cur.execute("""SELECT title,description,time_ts,time_text,image_url,watermark
                             FROM events WHERE message_id=?""", (mid,)).fetchone()
    if not row:
        await interaction.response.send_message("Event not found.", ephemeral=True)
        return
    title, desc, ts, tt, img, wm = row
    ch = interaction.channel
    temp = await ch.send(embed=discord.Embed(title="Cloning…"))
    now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    with closing(_db_conn()) as con:
        cur = con.cursor()
        cur.execute("""INSERT INTO events(message_id,guild_id,channel_id,creator_id,title,description,time_ts,time_text,image_url,watermark,locked,created_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,0,?)""",
                    (temp.id, interaction.guild_id, ch.id, interaction.user.id, title, desc, ts, tt, img, wm, now))
        con.commit()
    with closing(_db_conn()) as con:
        await temp.edit(embed=build_rsvp_embed_from_db(con, temp.id, interaction.guild), view=RSVPView(temp.id))
    await schedule_reminders_and_cleanup(temp.id)
    await staff_log(interaction.guild, f"🧬 {interaction.user.mention} cloned event `{title}` → {jump_url(interaction.guild_id, ch.id, temp.id)}")
    await interaction.response.send_message("✅ Cloned.", ephemeral=True)

@bot.tree.command(name="rsvp_optout", description="Stop being pinged/reminded for React Events")
async def rsvp_optout(interaction: discord.Interaction):
    with closing(_db_conn()) as con:
        cur = con.cursor()
        cur.execute("INSERT OR IGNORE INTO optouts(guild_id,user_id) VALUES(?,?)", (interaction.guild_id, interaction.user.id))
        con.commit()
    await interaction.response.send_message("🚫 You will no longer receive React Event reminders.", ephemeral=True)

@bot.tree.command(name="rsvp_optin", description="Allow React Event reminders again")
async def rsvp_optin(interaction: discord.Interaction):
    with closing(_db_conn()) as con:
        cur = con.cursor()
        cur.execute("DELETE FROM optouts WHERE guild_id=? AND user_id=?", (interaction.guild_id, interaction.user.id))
        con.commit()
    await interaction.response.send_message("✅ You'll receive React Event reminders again.", ephemeral=True)

# ================== SETTINGS COMMANDS (persisted) ==================

@bot.tree.command(name="rsvp_enable", description="Enable or disable the React Event system in this server (Admin)")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(on="true to enable, false to disable")
async def rsvp_enable(interaction: discord.Interaction, on: bool):
    settings_upsert(interaction.guild_id, enabled=on)
    await interaction.response.send_message(
        f"✅ React Event system **{'enabled' if on else 'disabled'}**.", ephemeral=True
    )

@bot.tree.command(name="rsvp_set_logs_channel", description="Set the staff logs channel (Admin)")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(channel="Channel where audit logs should go")
async def rsvp_set_logs_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    settings_upsert(interaction.guild_id, staff_logs_channel_id=channel.id)
    await interaction.response.send_message(f"✅ Staff logs channel set to {channel.mention}.", ephemeral=True)

# NEW: reasons channel command
@bot.tree.command(name="rsvp_set_reasons_channel", description="Set the channel to receive Declined/Tentative reasons (Admin)")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(channel="Channel where reasons should be posted")
async def rsvp_set_reasons_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    settings_upsert(interaction.guild_id, reasons_channel_id=channel.id)
    await interaction.response.send_message(f"✅ Reasons channel set to {channel.mention}.", ephemeral=True)

@bot.tree.command(name="rsvp_set_autorole", description="Set the role to grant on ✅ (Admin)")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(role="Pick a role to grant on ✅ (omit to disable)")
async def rsvp_set_autorole(interaction: discord.Interaction, role: Optional[discord.Role] = None):
    settings_upsert(interaction.guild_id, auto_role_on_accept=(role.id if role else 0))
    await interaction.response.send_message(
        f"✅ Auto-role **{'disabled' if not role else role.mention}**.",
        ephemeral=True
    )

@bot.tree.command(name="rsvp_settings", description="Show current React Event settings (Admin)")
@app_commands.checks.has_permissions(administrator=True)
async def rsvp_settings(interaction: discord.Interaction):
    s = settings_get(interaction.guild_id)
    ch = interaction.guild.get_channel(s["staff_logs_channel_id"])
    role = interaction.guild.get_role(s["auto_role_on_accept"]) if s["auto_role_on_accept"] else None
    reasons_ch = interaction.guild.get_channel(s["reasons_channel_id"])
    embed = discord.Embed(title="⚙️ React Event Settings", color=discord.Color.green())
    embed.add_field(name="Enabled", value="Yes" if s["enabled"] else "No", inline=True)
    embed.add_field(name="Staff Logs Channel", value=(ch.mention if ch else f"`{s['staff_logs_channel_id']}`"), inline=False)
    embed.add_field(name="Auto-role on ✅", value=(role.mention if role else "Disabled"), inline=True)
    embed.add_field(name="Reasons Channel", value=(reasons_ch.mention if reasons_ch else f"`{s['reasons_channel_id']}`"), inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)

# ==================== APPLICATION SYSTEM (your original, UTC) ====================

class ApplicationModal(discord.ui.Modal, title="Apply for Evade!!"):
    def __init__(self):
        super().__init__()
        self.wipes = discord.ui.TextInput(label="How many wipes can you play a week?", placeholder="e.g., 2-3 wipes", required=True, max_length=50)
        self.battlemetrics = discord.ui.TextInput(label="Battlemetrics Hours (Minimum 2000 required)", placeholder="e.g., 2500 hours", required=True, max_length=100)
        self.steam_link = discord.ui.TextInput(label="Steam Link", placeholder="Your Steam profile link", required=True, max_length=200)
        self.clips = discord.ui.TextInput(label="Clips (Provide links to your gameplay)", style=discord.TextStyle.paragraph, placeholder="Paste your clip links here (YouTube, Twitch, Medal, etc.)", required=True, max_length=1000)
        self.reason = discord.ui.TextInput(label="Why do you want to apply?", style=discord.TextStyle.paragraph, placeholder="Tell us why you'd be a good fit...", required=True, max_length=1000)
        self.add_item(self.wipes); self.add_item(self.battlemetrics); self.add_item(self.steam_link); self.add_item(self.clips); self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.send_message("✅ Processing your application... Please wait!", ephemeral=True)
        application_data = {
            'wipes': self.wipes.value,
            'battlemetrics': self.battlemetrics.value,
            'steam_link': self.steam_link.value,
            'clips': self.clips.value,
            'reason': self.reason.value,
            'user': interaction.user,
            'timestamp': datetime.datetime.now(datetime.timezone.utc)
        }
        await create_application_channel(interaction.guild, application_data)
        try:
            embed = discord.Embed(
                title="✅ Application Submitted!",
                description=f"Your application has been submitted successfully!\n\n**You can now type messages here and they will be sent to the staff reviewing your application.**",
                color=discord.Color.green(),
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            )
            embed.add_field(name="Wipes per Week", value=self.wipes.value, inline=True)
            embed.add_field(name="Battlemetrics Hours", value=self.battlemetrics.value, inline=True)
            embed.add_field(name="Steam Link", value=self.steam_link.value, inline=False)
            embed.add_field(name="Clips", value=self.clips.value[:1024], inline=False)
            embed.add_field(name="Reason", value=self.reason.value[:1024], inline=False)
            embed.set_footer(text="You can now chat with staff directly here!")
            await interaction.user.send(embed=embed)
        except discord.Forbidden:
            pass

class ApplyButtonView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Apply", style=discord.ButtonStyle.primary, emoji="📝")
    async def apply_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id in user_to_channel:
            await interaction.response.send_message("❌ You already have an open application! Please wait for staff to respond.", ephemeral=True)
            return
        modal = ApplicationModal()
        await interaction.response.send_modal(modal)

async def create_application_channel(guild: discord.Guild, app_data: dict):
    staff_role = guild.get_role(STAFF_ROLE_ID)
    if not staff_role:
        print(f"Warning: Staff role with ID {STAFF_ROLE_ID} not found!")

    category = guild.get_channel(APPLICATIONS_CATEGORY_ID)
    if category is not None and not isinstance(category, discord.CategoryChannel):
        print(f"Warning: APPLICATIONS_CATEGORY_ID ({APPLICATIONS_CATEGORY_ID}) is not a category channel. Creating channel without category.")
        category = None
    elif category is None:
        print(f"Warning: Category with ID {APPLICATIONS_CATEGORY_ID} not found. Creating channel without category.")

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(read_messages=False, view_channel=False),
        guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True, view_channel=True)
    }
    if staff_role:
        overwrites[staff_role] = discord.PermissionOverwrite(read_messages=True, send_messages=True, view_channel=True)

    channel_name = f"app-{app_data['user'].name}".lower().replace(" ", "-")
    channel = await guild.create_text_channel(
        name=channel_name, category=category, overwrites=overwrites, topic=f"Application from {app_data['user'].name}"
    )

    embed = discord.Embed(
        title=f"📋 New Application",
        description=f"Application from {app_data['user'].mention}",
        color=discord.Color.blue(),
        timestamp=app_data['timestamp']
    )
    embed.set_thumbnail(url=app_data['user'].display_avatar.url)
    embed.add_field(name="👤 Applicant", value=f"{app_data['user'].name} ({app_data['user'].id})", inline=False)
    embed.add_field(name="🔄 Wipes per Week", value=app_data['wipes'], inline=True)
    embed.add_field(name="⏱️ Battlemetrics Hours", value=app_data['battlemetrics'], inline=True)
    embed.add_field(name="🎮 Steam Link", value=app_data['steam_link'], inline=False)
    embed.add_field(name="🎬 Clips", value=app_data['clips'], inline=False)
    embed.add_field(name="💭 Why Apply?", value=app_data['reason'], inline=False)
    embed.set_footer(text="Messages in this channel will be sent anonymously to the applicant's DMs")

    view = ApplicationResponseView(app_data['user'], channel)
    await channel.send(content=f"🔔 New application received! {staff_role.mention if staff_role else '@Staff'}", embed=embed, view=view)

    active_applications[channel.id] = {
        'user': app_data['user'],
        'user_avatar': str(app_data['user'].display_avatar.url),
        'application_data': {
            'wipes': app_data['wipes'],
            'battlemetrics': app_data['battlemetrics'],
            'steam_link': app_data['steam_link'],
            'clips': app_data['clips'],
            'reason': app_data['reason'],
            'timestamp': app_data['timestamp']
        },
        'messages': []
    }
    user_to_channel[app_data['user'].id] = channel.id
    channel_to_user[channel.id] = app_data['user'].id

class ApplicationResponseView(discord.ui.View):
    def __init__(self, applicant: discord.User, channel: discord.TextChannel):
        super().__init__(timeout=None)
        self.applicant = applicant
        self.channel = channel

    @discord.ui.button(label="Accept Application", style=discord.ButtonStyle.success, emoji="✅")
    async def accept_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = ResponseModal(self.applicant, "accepted", self.channel)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Reject Application", style=discord.ButtonStyle.danger, emoji="❌")
    async def reject_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = ResponseModal(self.applicant, "rejected", self.channel)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Request More Info", style=discord.ButtonStyle.primary, emoji="❓")
    async def info_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = ResponseModal(self.applicant, "question", self.channel)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Schedule Voice Chat", style=discord.ButtonStyle.secondary, emoji="🎤")
    async def voice_chat_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = VoiceChatModal(self.applicant, self.channel)
        await interaction.response.send_modal(modal)

class ResponseModal(discord.ui.Modal, title="Send Response"):
    def __init__(self, applicant: discord.User, response_type: str, channel: discord.TextChannel):
        super().__init__()
        self.applicant = applicant
        self.response_type = response_type
        self.channel = channel

        if response_type == "accepted":
            placeholder = "Congratulations! You've been accepted because..."
            label = "Acceptance Message"
        elif response_type == "rejected":
            placeholder = "Thank you for applying, but unfortunately..."
            label = "Rejection Message"
        else:
            placeholder = "We need more information about..."
            label = "Question/Request"

        self.message = discord.ui.TextInput(label=label, style=discord.TextStyle.paragraph, placeholder=placeholder, required=True, max_length=2000)
        self.add_item(self.message)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            if self.response_type == "accepted":
                embed = discord.Embed(title="✅ Application Accepted!", description=self.message.value, color=discord.Color.green())
            elif self.response_type == "rejected":
                embed = discord.Embed(title="❌ Application Update", description=self.message.value, color=discord.Color.red())
            else:
                embed = discord.Embed(title="❓ Staff Question", description=self.message.value, color=discord.Color.blue())

            embed.set_footer(text="From: Staff Team")
            embed.timestamp = datetime.datetime.now(datetime.timezone.utc)

            await self.applicant.send(embed=embed)
            await self.channel.send(f"✉️ **{interaction.user.mention}** sent a {self.response_type} message to {self.applicant.mention} (sent anonymously)")

            if self.channel.id in active_applications:
                active_applications[self.channel.id]['messages'].append({
                    'sender': 'Staff Team',
                    'sender_avatar': str(bot.user.display_avatar.url),
                    'actual_staff': interaction.user.name,
                    'type': 'staff_embed',
                    'embed_type': self.response_type,
                    'message': self.message.value,
                    'timestamp': datetime.datetime.now(datetime.timezone.utc),
                    'color': '#57F287' if self.response_type == 'accepted' else '#ED4245' if self.response_type == 'rejected' else '#5865F2'
                })

            await interaction.response.send_message("✅ Message sent to applicant anonymously!", ephemeral=True)

            if self.response_type in ["accepted", "rejected"]:
                await self.channel.send("⚠️ Application {}. Use `/close_application` to close this channel and save transcript.".format(self.response_type))
        except discord.Forbidden:
            await interaction.response.send_message("❌ Could not send DM to applicant. They may have DMs disabled.", ephemeral=True)

class VoiceChatModal(discord.ui.Modal, title="Schedule Voice Chat"):
    def __init__(self, applicant: discord.User, channel: discord.TextChannel):
        super().__init__()
        self.applicant = applicant
        self.channel = channel

        self.datetime_input = discord.ui.TextInput(label="Date & Time (e.g., Tomorrow 3pm, Friday 6pm)", placeholder="When should the voice chat happen?", required=True, max_length=200)
        self.additional_info = discord.ui.TextInput(label="Additional Information (Optional)", style=discord.TextStyle.paragraph, placeholder="Any extra details about the voice chat...", required=False, max_length=1000)

        self.add_item(self.datetime_input); self.add_item(self.additional_info)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            embed = discord.Embed(
                title="🎤 Voice Chat Scheduled!",
                description=(f"**Scheduled Time:** {self.datetime_input.value}\n\n"
                             f"{self.additional_info.value if self.additional_info.value else 'A staff member will contact you at the scheduled time.'}\n\n"
                             f"**⚠️ IMPORTANT:** An admin must confirm you attended the voice chat, or this application will be automatically closed in {VOICE_CHAT_TIMEOUT_HOURS} hours."),
                color=discord.Color.purple()
            )
            embed.set_footer(text="From: Staff Team")
            embed.timestamp = datetime.datetime.now(datetime.timezone.utc)

            await self.applicant.send(embed=embed)

            confirmation_view = VoiceChatConfirmationView(self.applicant, self.channel)

            schedule_message = await self.channel.send(
                f"🎤 **{interaction.user.mention}** scheduled a voice chat with {self.applicant.mention}\n"
                f"**Time:** {self.datetime_input.value}\n"
                f"**Additional Info:** {self.additional_info.value if self.additional_info.value else 'None'}\n\n"
                f"⏰ **Auto-close in {VOICE_CHAT_TIMEOUT_HOURS} hours unless confirmed by admin**",
                view=confirmation_view
            )

            if self.channel.id in active_applications:
                active_applications[self.channel.id]['messages'].append({
                    'sender': 'Staff Team',
                    'sender_avatar': str(bot.user.display_avatar.url),
                    'actual_staff': interaction.user.name,
                    'type': 'staff_embed',
                    'embed_type': 'voice_chat',
                    'message': f"Voice chat scheduled for {self.datetime_input.value}. {self.additional_info.value if self.additional_info.value else ''}",
                    'timestamp': datetime.datetime.now(datetime.timezone.utc),
                    'color': '#9B59B6'
                })

            await interaction.response.send_message("✅ Voice chat scheduled and applicant notified!", ephemeral=True)
            await start_voice_chat_timer(self.channel, self.applicant, schedule_message)

        except discord.Forbidden:
            await interaction.response.send_message("❌ Could not send DM to applicant. They may have DMs disabled.", ephemeral=True)

class VoiceChatConfirmationView(discord.ui.View):
    def __init__(self, applicant: discord.User, channel: discord.TextChannel):
        super().__init__(timeout=None)
        self.applicant = applicant
        self.channel = channel

    @discord.ui.button(label="Confirm Attendance", style=discord.ButtonStyle.success, emoji="✅")
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Only administrators can confirm voice chat attendance!", ephemeral=True)
            return

        if self.channel.id in voice_chat_tasks:
            voice_chat_tasks[self.channel.id].cancel()
            del voice_chat_tasks[self.channel.id]

        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)

        await self.channel.send(f"✅ **{interaction.user.mention}** confirmed that {self.applicant.mention} attended the voice chat!\nAuto-close timer has been cancelled.")

        try:
            embed = discord.Embed(title="✅ Voice Chat Confirmed", description="Your voice chat attendance has been confirmed by an administrator.", color=discord.Color.green())
            embed.set_footer(text="From: Staff Team"); embed.timestamp = datetime.datetime.now(datetime.timezone.utc)
            await self.applicant.send(embed=embed)
        except discord.Forbidden:
            pass

        if self.channel.id in active_applications:
            active_applications[self.channel.id]['messages'].append({
                'sender': 'Staff Team',
                'sender_avatar': str(bot.user.display_avatar.url),
                'actual_staff': interaction.user.name,
                'type': 'staff_message',
                'message': 'Voice chat attendance confirmed by administrator.',
                'attachments': [],
                'timestamp': datetime.datetime.now(datetime.timezone.utc)
            })

        await interaction.response.send_message("✅ Attendance confirmed!", ephemeral=True)

    @discord.ui.button(label="Cancel Voice Chat", style=discord.ButtonStyle.danger, emoji="❌")
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.channel.id in voice_chat_tasks:
            voice_chat_tasks[self.channel.id].cancel()
            del voice_chat_tasks[self.channel.id]

        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)

        await self.channel.send(f"❌ **{interaction.user.mention}** cancelled the voice chat with {self.applicant.mention}")

        try:
            embed = discord.Embed(title="❌ Voice Chat Cancelled", description="The scheduled voice chat has been cancelled by staff.", color=discord.Color.red())
            embed.set_footer(text="From: Staff Team"); embed.timestamp = datetime.datetime.now(datetime.timezone.utc)
            await self.applicant.send(embed=embed)
        except discord.Forbidden:
            pass

        if self.channel.id in active_applications:
            active_applications[self.channel.id]['messages'].append({
                'sender': 'Staff Team',
                'sender_avatar': str(bot.user.display_avatar.url),
                'actual_staff': interaction.user.name,
                'type': 'staff_message',
                'message': 'Voice chat cancelled by staff.',
                'attachments': [],
                'timestamp': datetime.datetime.now(datetime.timezone.utc)
            })

        await interaction.response.send_message("✅ Voice chat cancelled!", ephemeral=True)

async def start_voice_chat_timer(channel: discord.TextChannel, applicant: discord.User, schedule_message: discord.Message):
    async def auto_close_task():
        try:
            await asyncio.sleep(VOICE_CHAT_TIMEOUT_HOURS * 3600)
            await channel.send(f"⏰ **AUTO-CLOSE:** Voice chat was not confirmed by an administrator within {VOICE_CHAT_TIMEOUT_HOURS} hours.\nClosing application and saving transcript...")

            try:
                embed = discord.Embed(
                    title="❌ Application Closed",
                    description=(f"Your application has been automatically closed because the voice chat was not confirmed within {VOICE_CHAT_TIMEOUT_HOURS} hours.\n\nIf you believe this was an error, please contact staff."),
                    color=discord.Color.red()
                )
                embed.set_footer(text="From: Staff Team")
                embed.timestamp = datetime.datetime.now(datetime.timezone.utc)
                await applicant.send(embed=embed)
            except discord.Forbidden:
                pass

            if channel.id in active_applications:
                active_applications[channel.id]['messages'].append({
                    'sender': 'System',
                    'sender_avatar': str(bot.user.display_avatar.url),
                    'actual_staff': 'System',
                    'type': 'staff_message',
                    'message': f'Application auto-closed: Voice chat not confirmed within {VOICE_CHAT_TIMEOUT_HOURS} hours.',
                    'attachments': [],
                    'timestamp': datetime.datetime.now(datetime.timezone.utc)
                })

            await asyncio.sleep(3)
            await auto_close_application(channel)

        except asyncio.CancelledError:
            pass
        finally:
            if channel.id in voice_chat_tasks:
                del voice_chat_tasks[channel.id]

    task = asyncio.create_task(auto_close_task())
    voice_chat_tasks[channel.id] = task

async def auto_close_application(channel: discord.TextChannel):
    channel_id = channel.id
    if channel_id not in active_applications:
        return

    app_data = active_applications[channel_id]
    html_content = generate_html_transcript(app_data)
    html_file = discord.File(io.BytesIO(html_content.encode('utf-8')), filename=f"transcript-{app_data['user'].name}-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}.html")

    transcript_embed = discord.Embed(
        title="📜 Application Transcript (Auto-Closed)",
        description=f"Application from {app_data['user'].name}",
        color=discord.Color.orange(),
        timestamp=datetime.datetime.now(datetime.timezone.utc)
    )
    transcript_embed.add_field(name="Channel", value=channel.name, inline=True)
    transcript_embed.add_field(name="Closed By", value="System (Auto-close)", inline=True)
    transcript_embed.add_field(name="Total Messages", value=str(len(app_data['messages'])), inline=True)
    transcript_embed.add_field(name="Reason", value=f"Voice chat not confirmed within {VOICE_CHAT_TIMEOUT_HOURS} hours", inline=False)

    transcript_channel = bot.get_channel(TRANSCRIPT_CHANNEL_ID)
    if transcript_channel:
        await transcript_channel.send(content="📋 **Application Auto-Closed**\nVoice chat was not confirmed in time.", embed=transcript_embed, file=html_file)

    user_id = channel_to_user.get(channel_id)
    if user_id:
        del user_to_channel[user_id]
        del channel_to_user[channel_id]

    del active_applications[channel_id]
    await asyncio.sleep(2)
    await channel.delete(reason="Application auto-closed: Voice chat not confirmed")

def generate_html_transcript(app_data: dict) -> str:
    user_name = app_data['user'].name
    user_avatar = app_data['user_avatar']
    application = app_data['application_data']
    messages = app_data['messages']

    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Application Transcript - {user_name}</title><style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background-color:#36393f;color:#dcddde;font-family:'Whitney','Helvetica Neue',Helvetica,Arial,sans-serif;padding:20px}}
.container{{max-width:900px;margin:0 auto;background-color:#2f3136;border-radius:8px;padding:20px}}
.header{{border-bottom:1px solid #202225;padding-bottom:20px;margin-bottom:20px}}
.header h1{{color:#ffffff;font-size:24px;margin-bottom:10px}}
.application-info{{background-color:#40444b;border-left:4px solid #5865F2;padding:15px;margin-bottom:20px;border-radius:4px}}
.application-info h2{{color:#5865F2;font-size:18px;margin-bottom:10px}}
.info-field{{margin-bottom:10px}}
.info-field strong{{color:#b9bbbe}}
.messages{{display:flex;flex-direction:column;gap:15px}}
.message{{display:flex;gap:15px;padding:10px;border-radius:4px;background-color:#36393f}}
.message.user{{background-color:#2f3136}}
.avatar{{width:40px;height:40px;border-radius:50%;flex-shrink:0}}
.message-content{{flex:1}}
.message-header{{display:flex;align-items:center;gap:10px;margin-bottom:5px}}
.sender-name{{font-weight:600;color:#ffffff}}
.sender-name.staff{{color:#faa61a}}
.timestamp{{font-size:12px;color:#72767d}}
.message-text{{color:#dcddde;line-height:1.4;word-wrap:break-word}}
.embed{{background-color:#2f3136;border-left:4px solid;border-radius:4px;padding:12px 16px;margin-top:8px}}
.embed.accepted{{border-color:#57F287}}
.embed.rejected{{border-color:#ED4245}}
.embed.question{{border-color:#5865F2}}
.embed.voice_chat{{border-color:#9B59B6}}
.embed-title{{font-weight:600;font-size:16px;margin-bottom:8px}}
.embed.accepted .embed-title{{color:#57F287}}
.embed.rejected .embed-title{{color:#ED4245}}
.embed.question .embed-title{{color:#5865F2}}
.embed.voice_chat .embed-title{{color:#9B59B6}}
.embed-description{{color:#dcddde;line-height:1.4}}
.embed-footer{{margin-top:8px;font-size:12px;color:#b9bbbe}}
.attachments{{margin-top:8px}}
.attachment-link{{color:#00b0f4;text-decoration:none;display:block;margin-top:4px}}
.attachment-link:hover{{text-decoration:underline}}
.footer{{margin-top:30px;padding-top:20px;border-top:1px solid #202225;text-align:center;color:#72767d;font-size:12px}}
</style></head><body><div class="container"><div class="header"><h1>📋 Application Transcript</h1><p style="color:#b9bbbe;">Applicant: {user_name}</p><p style="color:#72767d;font-size:12px;">Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p></div><div class="application-info"><h2>Application Details</h2><div class="info-field"><strong>Wipes per Week:</strong> {application['wipes']}</div><div class="info-field"><strong>Battlemetrics Hours:</strong> {application['battlemetrics']}</div><div class="info-field"><strong>Steam Link:</strong> {application['steam_link']}</div><div class="info-field"><strong>Clips:</strong> {application['clips']}</div><div class="info-field"><strong>Reason:</strong> {application['reason']}</div><div class="info-field" style="margin-top:10px;color:#72767d;"><strong>Submitted:</strong> {application['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}</div></div><div class="messages">"""
    for msg in messages:
        timestamp_str = msg['timestamp'].strftime('%I:%M %p')
        if msg['type'] == 'user_message':
            html += f"""<div class="message user"><img src="{msg['sender_avatar']}" alt="Avatar" class="avatar"><div class="message-content"><div class="message-header"><span class="sender-name">{msg['sender']}</span><span class="timestamp">{timestamp_str}</span></div><div class="message-text">{msg['message']}</div>"""
            if msg.get('attachments'):
                html += '<div class="attachments">📎 Attachments:'
                for att in msg['attachments']:
                    html += f'<a href="{att}" class="attachment-link" target="_blank">{att}</a>'
                html += '</div>'
            html += "</div></div>"
        elif msg['type'] == 'staff_message':
            html += f"""<div class="message"><img src="{msg['sender_avatar']}" alt="Avatar" class="avatar"><div class="message-content"><div class="message-header"><span class="sender-name staff">{msg['sender']}</span><span class="timestamp">{timestamp_str}</span></div><div class="message-text">{msg['message']}</div>"""
            if msg.get('attachments'):
                html += '<div class="attachments">📎 Attachments:'
                for att in msg['attachments']:
                    html += f'<a href="{att}" class="attachment-link" target="_blank">{att}</a>'
                html += '</div>'
            html += "</div></div>"
        elif msg['type'] == 'staff_embed':
            embed_class = msg['embed_type']
            title = "✅ Application Accepted!" if embed_class == 'accepted' else "❌ Application Update" if embed_class == 'rejected' else "🎤 Voice Chat Scheduled" if embed_class == 'voice_chat' else "❓ Staff Question"
            html += f"""<div class="message"><img src="{msg['sender_avatar']}" alt="Avatar" class="avatar"><div class="message-content"><div class="message-header"><span class="sender-name staff">{msg['sender']}</span><span class="timestamp">{timestamp_str}</span></div><div class="embed {embed_class}"><div class="embed-title">{title}</div><div class="embed-description">{msg['message']}</div><div class="embed-footer">From: Staff Team</div></div></div></div>"""
    html += "</div><div class=\"footer\"><p>This is an exact representation of what the applicant saw during the conversation.</p><p>All staff responses were sent anonymously as \"Staff Team\".</p></div></div></body></html>"
    return html

# ---------- Faster/lighter startup ----------
@bot.event
async def on_ready():
    print(f'✅ {bot.user} has connected to Discord!')
    print(f'Bot ID: {bot.user.id}')
    print(f'Connected to {len(bot.guilds)} guild(s)')

    # init DB + restore only recent/unlocked events, in batches
    try:
        db_init()
        now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        with closing(_db_conn()) as con:
            cur = con.cursor()
            rows = cur.execute(
                """SELECT message_id, time_ts FROM events
                   WHERE locked=0 AND (time_ts IS NULL OR time_ts >= ? - 86400)
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (now, RESTORE_EVENT_LIMIT)
            ).fetchall()

        added = 0
        for idx, (mid, ts) in enumerate(rows, 1):
            bot.add_view(RSVPView(mid))
            # only schedule timers for future events; skip old ones
            if ts and ts > now:
                await schedule_reminders_and_cleanup(mid)
            if idx % RESTORE_YIELD_EVERY == 0:
                await asyncio.sleep(0)  # yield to event loop to keep CPU calm
            added += 1
        print(f"[rsvp] restored {added} recent event views (limit {RESTORE_EVENT_LIMIT}).")
    except Exception as e:
        print(f"[rsvp] on_ready restore failed: {e}")

    # Set the bot presence/status
    try:
        await bot.change_presence(
            activity=discord.Activity(type=discord.ActivityType.watching, name=ACTIVITY_TEXT),
            status=discord.Status.online
        )
    except Exception as e:
        print(f"Warning: couldn't set presence: {e}")

    # Optional: skip expensive per-guild sync on boot unless opted-in
    if SYNC_ON_STARTUP:
        try:
            total = 0
            for g in bot.guilds:
                bot.tree.copy_global_to(guild=g)
                synced = await bot.tree.sync(guild=g)
                print(f'🔄 Synced {len(synced)} command(s) to guild: {g.name} ({g.id})')
                total += len(synced)
            print(f'✅ Finished per-guild sync. Total commands synced: {total}')
        except Exception as e:
            print(f'❌ Failed to sync commands: {e}')
    else:
        print("⏭️ Skipping per-guild command sync on startup (set SYNC_ON_STARTUP=1 to enable).")
# -------------------------------------------------------------------------

@bot.tree.command(name="setup_applications", description="Setup the application system (Admin only)")
@app_commands.checks.has_permissions(administrator=True)
async def setup_applications(interaction: discord.Interaction):
    embed = discord.Embed(
        title="📝 Evade Application",
        description=("**Welcome to our Evade Application System!**\n\n"
                     "Click the **Apply** button below to submit your application.\n\n"
                     "**Requirements:**\n"
                     "✅ Minimum 2000 Battlemetrics hours\n"
                     "✅ Gameplay clips\n"
                     "✅ Availability for multiple wipes per week\n\n"
                     "**Application includes:**\n"
                     "• How many wipes you can play per week\n"
                     "• Your Battlemetrics hours\n"
                     "• Your Steam profile link\n"
                     "• Gameplay clips\n"
                     "• Why you want to join\n\n"
                     "After submitting, you can chat directly with staff via DMs!"),
        color=discord.Color.blue()
    )
    embed.set_footer(text="Make sure your DMs are open!")
    view = ApplyButtonView()
    await interaction.response.send_message(embed=embed, view=view)

@bot.tree.command(name="close_application", description="Close an application channel and save transcript")
@app_commands.checks.has_permissions(manage_channels=True)
async def close_application(interaction: discord.Interaction):
    channel_id = interaction.channel_id
    if channel_id not in active_applications:
        await interaction.response.send_message("❌ This is not an active application channel!", ephemeral=True)
        return

    await interaction.response.defer()

    if channel_id in voice_chat_tasks:
        voice_chat_tasks[channel_id].cancel()
        del voice_chat_tasks[channel_id]

    app_data = active_applications[channel_id]
    html_content = generate_html_transcript(app_data)

    html_file = discord.File(io.BytesIO(html_content.encode('utf-8')), filename=f"transcript-{app_data['user'].name}-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}.html")

    transcript_embed = discord.Embed(title="📜 Application Transcript", description=f"Application from {app_data['user'].name}", color=discord.Color.gold(), timestamp=datetime.datetime.now(datetime.timezone.utc))
    transcript_embed.add_field(name="Channel", value=interaction.channel.name, inline=True)
    transcript_embed.add_field(name="Closed By", value=interaction.user.mention, inline=True)
    transcript_embed.add_field(name="Total Messages", value=str(len(app_data['messages'])), inline=True)

    transcript_channel = bot.get_channel(TRANSCRIPT_CHANNEL_ID)
    if transcript_channel:
        await transcript_channel.send(content="📋 **New Application Transcript**\nView the HTML file to see the exact conversation as the user saw it.", embed=transcript_embed, file=html_file)
    else:
        print(f"Warning: Transcript channel with ID {TRANSCRIPT_CHANNEL_ID} not found!")

    user_id = channel_to_user.get(channel_id)
    if user_id:
        del user_to_channel[user_id]
        del channel_to_user[channel_id]

    del active_applications[channel_id]

    await interaction.followup.send("✅ HTML transcript saved! Channel will be deleted in 5 seconds...")
    await asyncio.sleep(5)
    await interaction.channel.delete(reason=f"Application closed by {interaction.user.name}")

@bot.tree.command(name="config", description="Show bot configuration (Admin only)")
@app_commands.checks.has_permissions(administrator=True)
async def config_command(interaction: discord.Interaction):
    embed = discord.Embed(title="⚙️ Bot Configuration", description="Current bot settings:", color=discord.Color.green())

    staff_role = interaction.guild.get_role(STAFF_ROLE_ID)
    transcript_channel = interaction.guild.get_channel(TRANSCRIPT_CHANNEL_ID)
    category = interaction.guild.get_channel(APPLICATIONS_CATEGORY_ID)

    s = settings_get(interaction.guild_id)
    logs_channel = interaction.guild.get_channel(s["staff_logs_channel_id"])
    auto_role = interaction.guild.get_role(s["auto_role_on_accept"]) if s["auto_role_on_accept"] else None
    reasons_channel = interaction.guild.get_channel(s["reasons_channel_id"])

    embed.add_field(name="React Event Enabled", value="✅ Yes" if s["enabled"] else "❌ No", inline=True)
    embed.add_field(name="Staff Role", value=staff_role.mention if staff_role else "❌ Not found", inline=True)
    embed.add_field(name="Transcript Channel", value=transcript_channel.mention if transcript_channel else "❌ Not found", inline=False)
    embed.add_field(name="Applications Category", value=f"{category.name} (Category)" if category and isinstance(category, discord.CategoryChannel) else "❌ Not found or not a category", inline=False)
    embed.add_field(name="React Event Staff Logs Channel", value=(logs_channel.mention if logs_channel else f"`{s['staff_logs_channel_id']}`"), inline=False)
    embed.add_field(name="Reasons Channel", value=(reasons_channel.mention if reasons_channel else f"`{s['reasons_channel_id']}`"), inline=False)
    embed.add_field(name="Auto-role on ✅", value=(auto_role.mention if auto_role else "Disabled"), inline=True)
    embed.add_field(name="Voice Chat Timeout", value=f"{VOICE_CHAT_TIMEOUT_HOURS} hours", inline=True)
    embed.add_field(name="Active Applications", value=f"{len(active_applications)} open ticket(s)", inline=True)
    embed.add_field(name="Status", value="✅ All systems configured and ready!" if (staff_role and transcript_channel and category and isinstance(category, discord.CategoryChannel)) else "⚠️ Some components not found or misconfigured", inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="alert", description="DM everyone in this server with a message (Admin only, 1-minute cooldown)")
@app_commands.describe(message="The message to send in each DM")
@app_commands.checks.has_permissions(administrator=True)
async def alert(interaction: discord.Interaction, message: str):
    if interaction.guild is None:
        await interaction.response.send_message("❌ Use this command in a server, not DMs.", ephemeral=True)
        return

    now = datetime.datetime.now(datetime.timezone.utc)
    last = last_alert_time.get(interaction.guild_id)
    if last:
        remaining = 60 - int((now - last).total_seconds())
        if remaining > 0:
            await interaction.response.send_message(f"⏱️ Please wait **{remaining}s** before using `/alert` again in this server.", ephemeral=True)
            return

    await interaction.response.defer(ephemeral=True)
    last_alert_time[interaction.guild_id] = now

    guild = interaction.guild
    sent = 0
    failed = 0
    header = f"📣 **Server Alert from {guild.name}**"

    for member in guild.members:
        if member.bot:
            continue
        try:
            dm = await member.create_dm()
            personal = f"{member.mention} — **{(member.global_name or member.display_name or member.name)}**"
            await dm.send(
                f"{personal}\n{header}\n\n{message}",
                allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False, replied_user=False)
            )
            sent += 1
        except discord.Forbidden:
            failed += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.3)

    await interaction.followup.send(f"✅ Alert finished.\n• **Delivered:** {sent}\n• **Failed (DMs closed/other):** {failed}", ephemeral=True)

@bot.tree.command(name="sync", description="Force-resync slash commands in this server (Admin only)")
@app_commands.checks.has_permissions(administrator=True)
async def sync_here(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("Run this in a server.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    bot.tree.copy_global_to(guild=interaction.guild)
    synced = await bot.tree.sync(guild=interaction.guild)
    await interaction.followup.send(f"✅ Synced **{len(synced)}** command(s) to this server.", ephemeral=True)

# ================== VOICE: drag everyone into your VC (Admin) ==================

@bot.tree.command(
    name="drag_all",
    description="Move everyone from other voice channels into your current voice channel (Admin only)"
)
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(
    include_bots="Also move bots (default: false)",
    from_muted="Also pull users who are server-muted/deafened (default: true)"
)
async def drag_all(
    interaction: discord.Interaction,
    include_bots: bool = False,
    from_muted: bool = True,
):
    """
    Pulls all members from other voice/stage channels into the caller's current channel.
    Requirements:
      • You must be connected to the target voice channel.
      • The bot must have Move Members + Connect in the target channel.
    """
    if interaction.guild is None:
        await interaction.response.send_message("Use this in a server, not DMs.", ephemeral=True)
        return

    target_vc = getattr(interaction.user.voice, "channel", None)
    if not target_vc:
        await interaction.response.send_message(
            "Join the voice channel you want to pull everyone **into**, then run `/drag_all`.",
            ephemeral=True
        )
        return

    # Check the bot's permissions **in the target channel**
    me = interaction.guild.me
    perms = target_vc.permissions_for(me)
    missing = []
    if not perms.connect:
        missing.append("Connect")
    if not perms.move_members:
        missing.append("Move Members")
    if missing:
        await interaction.response.send_message(
            f"I’m missing permissions in {target_vc.mention}: {', '.join(missing)}.",
            ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    # Gather all voice-like channels (voice + stage)
    voice_like_channels: List[discord.abc.Connectable] = []
    voice_like_channels.extend(interaction.guild.voice_channels)
    try:
        voice_like_channels.extend(interaction.guild.stage_channels)  # type: ignore[attr-defined]
    except Exception:
        pass

    moved = 0
    skipped_perm = 0
    skipped_bots = 0
    skipped_state = 0

    # Iterate over every channel except the target
    for vc in voice_like_channels:
        if vc.id == target_vc.id:
            continue

        members = list(getattr(vc, "members", []))
        for member in members:
            # Skip bots unless requested
            if member.bot and not include_bots:
                skipped_bots += 1
                continue

            # Optionally skip server-muted/deafened folks
            v = member.voice
            if not from_muted and v and (v.mute or v.deaf or v.self_deaf):
                skipped_state += 1
                continue

            # Already in target?
            if v and v.channel and v.channel.id == target_vc.id:
                continue

            try:
                await member.move_to(
                    target_vc,
                    reason=f"/drag_all by {interaction.user} ({interaction.user.id})"
                )
                moved += 1
                await asyncio.sleep(0.2)
            except discord.Forbidden:
                skipped_perm += 1
            except Exception:
                skipped_perm += 1

    summary_lines = [f"Moved **{moved}** member(s) to {target_vc.mention}."]
    if skipped_bots:
        summary_lines.append(f"Skipped **{skipped_bots}** bot(s).")
    if skipped_state and not from_muted:
        summary_lines.append(f"Skipped **{skipped_state}** (muted/deafened).")
    if skipped_perm:
        summary_lines.append(f"Couldn’t move **{skipped_perm}** (permissions/other).")

    await interaction.followup.send("✅ " + " ".join(summary_lines), ephemeral=True)

    # Optional: staff audit log
    try:
        await staff_log(
            interaction.guild,
            f"🎛️ `/drag_all` by {interaction.user.mention} → {target_vc.mention} • moved {moved}, "
            f"skipped {skipped_bots + skipped_state + skipped_perm}."
        )
    except Exception:
        pass

# --------------- on_message (React Event wizard + your app relay) -------------------
@bot.event
async def on_message(message: discord.Message):
    if message.author == bot.user:
        return

    # React Event DM wizard with skip/back + live preview + post
    if isinstance(message.channel, discord.DMChannel) and message.author.id in rsvp_wizards:
        wiz = rsvp_wizards[message.author.id]
        content = (message.content or "").strip()
        if content.lower() == "cancel":
            del rsvp_wizards[message.author.id]
            await message.channel.send("❌ Cancelled.")
            return
        if content.lower() == "back":
            wiz["step"] = max(1, wiz["step"] - 1)

        step = wiz["step"]
        if step == 1 and content.lower() not in ("back", "skip"):
            wiz["data"]["title"] = content[:256]; wiz["step"] = 2
            await message.channel.send("2) Send **description** (or `skip`).", embed=_wizard_preview_embed(message.author, wiz, bot.get_guild(wiz["guild_id"]))); return
        if step == 2:
            if content.lower() != "skip" and content.lower() != "back":
                wiz["data"]["description"] = content[:4000]
            if content.lower() != "back": wiz["step"] = 3
            await message.channel.send("3) Send **time** like `Friday 17:45` or `YYYY-MM-DD HH:MM` (or `skip`).", embed=_wizard_preview_embed(message.author, wiz, bot.get_guild(wiz["guild_id"]))); return
        if step == 3:
            if content.lower() != "skip" and content.lower() != "back":
                ts = parse_time_to_unix(content)
                if ts:
                    wiz["data"]["time_ts"] = ts; wiz["data"]["time_text"] = None
                else:
                    wiz["data"]["time_ts"] = None; wiz["data"]["time_text"] = content[:128]
            if content.lower() != "back": wiz["step"] = 4
            await message.channel.send("4) Paste **image URL** or upload an image (16:9 ideal), or `skip`.", embed=_wizard_preview_embed(message.author, wiz, bot.get_guild(wiz["guild_id"]))); return
        if step == 4:
            if message.attachments and (message.attachments[0].content_type or "").startswith("image"):
                wiz["data"]["image_url"] = message.attachments[0].url
            elif content.lower() not in ("skip", "back"):
                wiz["data"]["image_url"] = content
            if content.lower() != "back": wiz["step"] = 5
            await message.channel.send("5) Optional footer **watermark** (e.g., server tag) or `skip`.", embed=_wizard_preview_embed(message.author, wiz, bot.get_guild(wiz["guild_id"]))); return
        if step == 5:
            if content.lower() not in ("skip", "back"):
                wiz["data"]["watermark"] = content[:128]
            if content.lower() != "back": wiz["step"] = 6
            await message.channel.send("✅ Type `post` to create it in the original channel, or `back` to revise.", embed=_wizard_preview_embed(message.author, wiz, bot.get_guild(wiz["guild_id"]))); return
        if step == 6 and content.lower() == "post":
            guild = bot.get_guild(wiz["guild_id"]); channel = guild.get_channel(wiz["channel_id"])
            if not settings_get(guild.id)["enabled"]:
                await message.channel.send("React Event system is disabled in that server.")
                return
            data = wiz["data"]
            now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
            # create placeholder, capture message id, then persist and finalize
            temp_msg = await channel.send(embed=discord.Embed(title="Posting…"))
            with closing(_db_conn()) as con:
                cur = con.cursor()
                cur.execute("""INSERT INTO events(message_id,guild_id,channel_id,creator_id,title,description,time_ts,time_text,image_url,watermark,locked,created_at)
                               VALUES(?,?,?,?,?,?,?,?,?,?,0,?)""",
                            (temp_msg.id, guild.id, channel.id, message.author.id,
                             data["title"] or "Untitled", data["description"], data["time_ts"], data["time_text"],
                             data["image_url"], data["watermark"], now))
                con.commit()
                embed = build_rsvp_embed_from_db(con, temp_msg.id, guild)
            view = RSVPView(temp_msg.id)
            await temp_msg.edit(embed=embed, view=view)
            await staff_log(guild, f"🆕 React Event created by {message.author.mention}: `{data['title']}` — {jump_url(guild.id, channel.id, temp_msg.id)}")
            await schedule_reminders_and_cleanup(temp_msg.id)
            del rsvp_wizards[message.author.id]
            await message.channel.send(f"✅ Posted in {channel.mention}.")
            return

    # Process slash commands
    await bot.process_commands(message)

    # DM relay for applications (user -> staff channel)
    if isinstance(message.channel, discord.DMChannel):
        user_id = message.author.id
        if user_id in user_to_channel:
            channel_id = user_to_channel[user_id]
            channel = bot.get_channel(channel_id)
            if channel:
                embed = discord.Embed(description=message.content, color=discord.Color.blue(), timestamp=datetime.datetime.now(datetime.timezone.utc))
                embed.set_author(name=f"{message.author.name} (Applicant)", icon_url=message.author.display_avatar.url)
                if message.attachments:
                    attachment_links = "\n".join([att.url for att in message.attachments])
                    embed.add_field(name="📎 Attachments", value=attachment_links, inline=False)
                await channel.send(embed=embed)
                try:
                    await message.add_reaction("✅")
                except:
                    pass
                if channel_id in active_applications:
                    active_applications[channel_id]['messages'].append({
                        'sender': message.author.name,
                        'sender_avatar': str(message.author.display_avatar.url),
                        'type': 'user_message',
                        'message': message.content,
                        'attachments': [att.url for att in message.attachments] if message.attachments else [],
                        'timestamp': datetime.datetime.now(datetime.timezone.utc)
                    })

    # Staff messages in application channels -> anonymous DM to applicant
    elif message.channel.id in channel_to_user:
        user_id = channel_to_user[message.channel.id]
        user = bot.get_user(user_id)
        if user:
            if message.author.bot or message.content.startswith('/'):
                return
            embed = discord.Embed(description=message.content, color=discord.Color.gold(), timestamp=datetime.datetime.now(datetime.timezone.utc))
            embed.set_author(name="Staff Team", icon_url=bot.user.display_avatar.url)
            if message.attachments:
                attachment_links = "\n".join([att.url for att in message.attachments])
                embed.add_field(name="📎 Attachments", value=attachment_links, inline=False)
            try:
                await user.send(embed=embed)
                await message.add_reaction("✅")
            except discord.Forbidden:
                await message.channel.send("⚠️ Could not deliver message - user has DMs disabled.")
            except Exception as e:
                await message.channel.send(f"⚠️ Error sending message: {e}")
            if message.channel.id in active_applications:
                active_applications[message.channel.id]['messages'].append({
                    'sender': 'Staff Team',
                    'sender_avatar': str(bot.user.display_avatar.url),
                    'actual_staff': message.author.name,
                    'type': 'staff_message',
                    'message': message.content,
                    'attachments': [att.url for att in message.attachments] if message.attachments else [],
                    'timestamp': datetime.datetime.now(datetime.timezone.utc)
                })

# Optional: when a member leaves, clear their RSVPs in this guild and update messages
@bot.event
async def on_member_remove(member: discord.Member):
    with closing(_db_conn()) as con:
        cur = con.cursor()
        rows = cur.execute("SELECT message_id, channel_id FROM events WHERE guild_id=?", (member.guild.id,)).fetchall()
        cur.execute("DELETE FROM rsvps WHERE user_id=? AND message_id IN (SELECT message_id FROM events WHERE guild_id=?)",
                    (member.id, member.guild.id))
        con.commit()
    # best-effort UI refresh
    for mid, chid in rows:
        try:
            ch = member.guild.get_channel(chid)
            if not ch:
                continue
            msg = await ch.fetch_message(mid)
            with closing(_db_conn()) as con:
                await msg.edit(embed=build_rsvp_embed_from_db(con, mid, member.guild), view=RSVPView(mid))
        except:
            pass

# --------------------------------------------------------------------------------------

def main():
    # Always read token from token.txt (and create it with instructions if missing)
    try:
        token = read_token_from_file()
    except RuntimeError as e:
        print(e)
        return

    try:
        print("🚀 Starting Application Bot...")
        print("⚙️ Configuration:")
        print(f"   Staff Role ID: {STAFF_ROLE_ID}")
        print(f"   Transcript Channel ID: {TRANSCRIPT_CHANNEL_ID}")
        print(f"   Applications Category ID: {APPLICATIONS_CATEGORY_ID}")
        print(f"   Voice Chat Timeout: {VOICE_CHAT_TIMEOUT_HOURS} hours")
        print(f"   SYNC_ON_STARTUP: {SYNC_ON_STARTUP} | RESTORE_EVENT_LIMIT: {RESTORE_EVENT_LIMIT}")
        print("-" * 60)
        bot.run(token)
    except discord.errors.LoginFailure:
        print("❌ LOGIN FAILED: The provided token is invalid!")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

if __name__ == '__main__':
    main()
