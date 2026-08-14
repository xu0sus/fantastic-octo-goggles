import threading
import os, re, io, random, asyncio, sqlite3, time, logging, math, traceback, secrets
from collections import Counter
from datetime import timedelta, datetime, timezone
import discord
from discord import app_commands
from discord.ext import commands, tasks
from PIL import Image, ImageOps, ImageFilter
from flask import Flask
import aiohttp

TOKEN = os.getenv("DISCORD_TOKEN")
DB_PATH = os.getenv("BOT_DB", "bot.db")
BACKUP_DIR = os.getenv("BOT_BACKUP_DIR", "backups")
BOT_NAME = "بوت الخلعاوية"
if not TOKEN: raise RuntimeError("ضع DISCORD_TOKEN في متغيرات البيئة قبل التشغيل.")
intents = discord.Intents.default()
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)
tree = bot.tree
class ArabicTranslator(app_commands.Translator):
    async def translate(self, string: app_commands.locale_str, locale: discord.Locale, context):
        if locale == discord.Locale.ar:
            return string.extras.get("arabic")
        return None

DB_SYNC_LOCK = threading.RLock()
XP_CACHE = {}
TICKET_PREFIX = "ticket-"
BANK_ITEMS = {"سيارة": 250_000, "ملعب": 500_000}
BASE_PRICES = BANK_ITEMS.copy()

db = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA synchronous=NORMAL")
db.execute("PRAGMA foreign_keys=ON")
db.execute("PRAGMA busy_timeout=30000")
db.execute("PRAGMA temp_store=MEMORY")
db.execute("PRAGMA cache_size=-32000")
db.executescript("""
CREATE TABLE IF NOT EXISTS config(guild_id INTEGER PRIMARY KEY,welcome_channel INTEGER,suggestions_channel INTEGER,tickets_category INTEGER,staff_role INTEGER,log_channel INTEGER,auto_role INTEGER,xp_enabled INTEGER NOT NULL DEFAULT 1,economy_enabled INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS levels(guild_id INTEGER NOT NULL,user_id INTEGER NOT NULL,xp INTEGER NOT NULL DEFAULT 0,level INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(guild_id,user_id));
CREATE TABLE IF NOT EXISTS economy(guild_id INTEGER NOT NULL,user_id INTEGER NOT NULL,balance INTEGER NOT NULL DEFAULT 1000,PRIMARY KEY(guild_id,user_id));
CREATE TABLE IF NOT EXISTS assets(guild_id INTEGER NOT NULL,user_id INTEGER NOT NULL,item TEXT NOT NULL,quantity INTEGER NOT NULL DEFAULT 1,PRIMARY KEY(guild_id,user_id,item));
CREATE TABLE IF NOT EXISTS prices(guild_id INTEGER NOT NULL,item TEXT NOT NULL,price INTEGER NOT NULL,PRIMARY KEY(guild_id,item));
CREATE TABLE IF NOT EXISTS nicknames(guild_id INTEGER NOT NULL,user_id INTEGER NOT NULL,nickname TEXT NOT NULL,updated_at TEXT NOT NULL,PRIMARY KEY(guild_id,user_id));
CREATE TABLE IF NOT EXISTS warnings(id INTEGER PRIMARY KEY AUTOINCREMENT,guild_id INTEGER NOT NULL,user_id INTEGER NOT NULL,moderator_id INTEGER NOT NULL,reason TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS suggestions(id INTEGER PRIMARY KEY AUTOINCREMENT,guild_id INTEGER NOT NULL,channel_id INTEGER NOT NULL,message_id INTEGER NOT NULL,user_id INTEGER NOT NULL,text TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS suggestion_votes(suggestion_id INTEGER NOT NULL,user_id INTEGER NOT NULL,vote INTEGER NOT NULL,PRIMARY KEY(suggestion_id,user_id));
CREATE TABLE IF NOT EXISTS tickets(id INTEGER PRIMARY KEY AUTOINCREMENT,guild_id INTEGER NOT NULL,channel_id INTEGER NOT NULL,user_id INTEGER NOT NULL,status TEXT NOT NULL DEFAULT 'open',created_at TEXT NOT NULL,closed_at TEXT);
CREATE TABLE IF NOT EXISTS transactions(id INTEGER PRIMARY KEY AUTOINCREMENT,guild_id INTEGER NOT NULL,user_id INTEGER NOT NULL,kind TEXT NOT NULL,amount INTEGER NOT NULL,description TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS daily_claims(guild_id INTEGER NOT NULL,user_id INTEGER NOT NULL,claimed_at TEXT NOT NULL,PRIMARY KEY(guild_id,user_id));
CREATE TABLE IF NOT EXISTS moderation_cases(id INTEGER PRIMARY KEY AUTOINCREMENT,guild_id INTEGER NOT NULL,user_id INTEGER NOT NULL,moderator_id INTEGER NOT NULL,action TEXT NOT NULL,reason TEXT NOT NULL,created_at TEXT NOT NULL,duration INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS user_notes(id INTEGER PRIMARY KEY AUTOINCREMENT,guild_id INTEGER NOT NULL,user_id INTEGER NOT NULL,author_id INTEGER NOT NULL,note TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS polls(id INTEGER PRIMARY KEY AUTOINCREMENT,guild_id INTEGER NOT NULL,channel_id INTEGER NOT NULL,message_id INTEGER NOT NULL,question TEXT NOT NULL,options TEXT NOT NULL,created_at TEXT NOT NULL,ends_at TEXT);
CREATE TABLE IF NOT EXISTS reminders(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,channel_id INTEGER,remind_at TEXT NOT NULL,text TEXT NOT NULL,sent INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS afk(guild_id INTEGER NOT NULL,user_id INTEGER NOT NULL,reason TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(guild_id,user_id));
CREATE TABLE IF NOT EXISTS automod(guild_id INTEGER PRIMARY KEY,links INTEGER NOT NULL DEFAULT 0,invites INTEGER NOT NULL DEFAULT 0,spam INTEGER NOT NULL DEFAULT 1,bad_words INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS bad_words(guild_id INTEGER NOT NULL,word TEXT NOT NULL,PRIMARY KEY(guild_id,word));
CREATE TABLE IF NOT EXISTS role_rewards(guild_id INTEGER NOT NULL,level INTEGER NOT NULL,role_id INTEGER NOT NULL,PRIMARY KEY(guild_id,level));
CREATE TABLE IF NOT EXISTS starboard(guild_id INTEGER PRIMARY KEY,channel_id INTEGER,threshold INTEGER NOT NULL DEFAULT 5);
CREATE TABLE IF NOT EXISTS bot_stats(key TEXT PRIMARY KEY,value INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS giveaways(id INTEGER PRIMARY KEY AUTOINCREMENT,guild_id INTEGER NOT NULL,channel_id INTEGER NOT NULL,message_id INTEGER NOT NULL,prize TEXT NOT NULL,winners INTEGER NOT NULL,ends_at TEXT NOT NULL,host_id INTEGER NOT NULL,status TEXT NOT NULL DEFAULT 'running');
CREATE TABLE IF NOT EXISTS giveaway_entries(giveaway_id INTEGER NOT NULL,user_id INTEGER NOT NULL,PRIMARY KEY(giveaway_id,user_id));
""")
db.commit()

def now_iso(): return datetime.now(timezone.utc).isoformat()

def db_exec(sql, params=(), fetch=False, many=False):
    with DB_SYNC_LOCK:
        cur = db.executemany(sql, params) if many else db.execute(sql, params)
        rows = cur.fetchall() if fetch else None
        db.commit()
        return rows

def db_transaction(callback):
    """Run a short synchronous SQLite transaction atomically and safely."""
    with DB_SYNC_LOCK:
        db.execute("BEGIN IMMEDIATE")
        try:
            result = callback()
            db.commit()
            return result
        except Exception:
            db.rollback()
            raise

def create_backup():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = os.path.join(BACKUP_DIR, f"bot_{stamp}.db")
    backup = sqlite3.connect(path)
    try:
        db.backup(backup)
    finally:
        backup.close()
    # Keep the newest 10 backups.
    files = sorted((os.path.join(BACKUP_DIR, f) for f in os.listdir(BACKUP_DIR) if f.endswith(".db")), key=os.path.getmtime, reverse=True)
    for old in files[10:]:
        try: os.remove(old)
        except OSError: pass
    return path

def ensure_config(guild_id):
    row = db.execute("SELECT * FROM config WHERE guild_id=?", (guild_id,)).fetchone()
    if not row:
        db.execute("INSERT INTO config(guild_id) VALUES(?)", (guild_id,))
        db.commit()
        row = db.execute("SELECT * FROM config WHERE guild_id=?", (guild_id,)).fetchone()
    return row

def get_config(guild_id): return ensure_config(guild_id)

def set_config(guild_id, field, value):
    allowed = {"welcome_channel","suggestions_channel","tickets_category","staff_role","log_channel","auto_role","xp_enabled","economy_enabled"}
    if field not in allowed: raise ValueError("Invalid config field")
    db_exec(f"UPDATE config SET {field}=? WHERE guild_id=?", (value, guild_id))

def get_balance(guild_id, user_id):
    row = db.execute("SELECT balance FROM economy WHERE guild_id=? AND user_id=?", (guild_id,user_id)).fetchone()
    if row: return row["balance"]
    db_exec("INSERT INTO economy(guild_id,user_id,balance) VALUES(?,?,1000)", (guild_id,user_id))
    return 1000

def set_balance(guild_id, user_id, balance):
    db_exec("INSERT INTO economy(guild_id,user_id,balance) VALUES(?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET balance=excluded.balance", (guild_id,user_id,balance))

def add_transaction(guild_id,user_id,kind,amount,description):
    db_exec("INSERT INTO transactions(guild_id,user_id,kind,amount,description,created_at) VALUES(?,?,?,?,?,?)", (guild_id,user_id,kind,amount,description,now_iso()))

def xp_needed(level): return 100 + level * 50

def get_level(guild_id,user_id):
    row = db.execute("SELECT xp,level FROM levels WHERE guild_id=? AND user_id=?", (guild_id,user_id)).fetchone()
    if row: return row["xp"], row["level"]
    db_exec("INSERT INTO levels(guild_id,user_id,xp,level) VALUES(?,?,0,0)", (guild_id,user_id))
    return 0,0

def add_xp(guild_id,user_id,amount):
    xp, level = get_level(guild_id,user_id); xp += amount; gained = 0
    while xp >= xp_needed(level): xp -= xp_needed(level); level += 1; gained += 1
    db_exec("INSERT INTO levels(guild_id,user_id,xp,level) VALUES(?,?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET xp=excluded.xp,level=excluded.level", (guild_id,user_id,xp,level))
    return xp,level,gained

def current_price(guild_id,item):
    row = db.execute("SELECT price FROM prices WHERE guild_id=? AND item=?", (guild_id,item)).fetchone()
    if row: return row["price"]
    price = BASE_PRICES[item]
    db_exec("INSERT INTO prices(guild_id,item,price) VALUES(?,?,?)", (guild_id,item,price))
    return price

def parse_duration(value, max_days=28):
    """Parse a duration such as 10m, 2h, 1d or 1w into a timedelta."""
    if not isinstance(value, str):
        return None
    match = re.fullmatch(r"\s*(\d+)\s*(s|m|h|d|w)\s*", value.lower())
    if not match:
        return None
    seconds = int(match.group(1)) * {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[match.group(2)]
    if seconds <= 0 or seconds > int(max_days) * 86400:
        return None
    return timedelta(seconds=seconds)

async def reply(interaction, content=None, *, embed=None, view=None, ephemeral=True):
    if interaction.response.is_done(): return await interaction.followup.send(content=content, embed=embed, view=view, ephemeral=ephemeral)
    return await interaction.response.send_message(content=content, embed=embed, view=view, ephemeral=ephemeral)

def guild_only(interaction): return interaction.guild is not None

def can_target(actor, target, bot_member):
    if target.id == actor.id or target.id == actor.guild.owner_id: return False
    if target.id == bot.user.id: return False
    return target.top_role < actor.top_role and target.top_role < bot_member.top_role

async def send_log(guild, title, description):
    try:
        cfg = get_config(guild.id); cid = cfg["log_channel"]
        if not cid: return
        ch = guild.get_channel(cid)
        if ch: await ch.send(embed=discord.Embed(title=title, description=description, timestamp=datetime.now(timezone.utc)))
    except Exception: pass

def is_staff(member):
    if member.guild_permissions.manage_guild or member.guild_permissions.administrator: return True
    cfg = get_config(member.guild.id)
    return bool(cfg["staff_role"] and any(r.id == cfg["staff_role"] for r in member.roles))

class SuggestionView(discord.ui.View):
    def __init__(self, suggestion_id=None, yes_count=0, no_count=0):
        super().__init__(timeout=None); self.suggestion_id = suggestion_id
        if suggestion_id is not None:
            self.yes.custom_id = f"suggestion_yes:{suggestion_id}"
            self.no.custom_id = f"suggestion_no:{suggestion_id}"
            self.yes.label = f"👍 {yes_count}"
            self.no.label = f"👎 {no_count}"
    async def vote(self, interaction, value):
        row = db.execute("SELECT * FROM suggestions WHERE id=?", (self.suggestion_id,)).fetchone()
        if not row: return await reply(interaction, "❌ الاقتراح غير موجود.")
        old = db.execute("SELECT vote FROM suggestion_votes WHERE suggestion_id=? AND user_id=?", (self.suggestion_id,interaction.user.id)).fetchone()
        if old and old["vote"] == value:
            db_exec("DELETE FROM suggestion_votes WHERE suggestion_id=? AND user_id=?", (self.suggestion_id,interaction.user.id)); text = "تم إلغاء تصويتك."
        else:
            db_exec("INSERT INTO suggestion_votes(suggestion_id,user_id,vote) VALUES(?,?,?) ON CONFLICT(suggestion_id,user_id) DO UPDATE SET vote=excluded.vote", (self.suggestion_id,interaction.user.id,value)); text = "تم تسجيل تصويتك."
        yes = db.execute("SELECT COUNT(*) c FROM suggestion_votes WHERE suggestion_id=? AND vote=1", (self.suggestion_id,)).fetchone()["c"]; no = db.execute("SELECT COUNT(*) c FROM suggestion_votes WHERE suggestion_id=? AND vote=0", (self.suggestion_id,)).fetchone()["c"]
        await interaction.response.send_message(f"✅ {text} 👍 {yes} | 👎 {no}", ephemeral=True)
        try:
            msg = await interaction.channel.fetch_message(row["message_id"]); view = SuggestionView(self.suggestion_id, yes, no); await msg.edit(view=view)
        except Exception: pass
    @discord.ui.button(label="👍 0", style=discord.ButtonStyle.success, custom_id="suggestion_yes")
    async def yes(self, interaction, button): await self.vote(interaction,1)
    @discord.ui.button(label="👎 0", style=discord.ButtonStyle.danger, custom_id="suggestion_no")
    async def no(self, interaction, button): await self.vote(interaction,0)

class TicketView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)
    @discord.ui.button(label="🎫 فتح تذكرة", style=discord.ButtonStyle.primary, custom_id="ticket_open")
    async def open_ticket(self, interaction, button):
        if not interaction.guild: return await reply(interaction,"❌ هذا الزر يعمل داخل السيرفر فقط.")
        existing = db.execute("SELECT channel_id FROM tickets WHERE guild_id=? AND user_id=? AND status='open'", (interaction.guild.id,interaction.user.id)).fetchone()
        if existing:
            ch = interaction.guild.get_channel(existing["channel_id"])
            if ch: return await reply(interaction,f"❌ لديك تذكرة مفتوحة بالفعل: {ch.mention}")
            db_exec("UPDATE tickets SET status='closed',closed_at=? WHERE channel_id=?", (now_iso(),existing["channel_id"]))
        cfg = get_config(interaction.guild.id); category = interaction.guild.get_channel(cfg["tickets_category"]) if cfg["tickets_category"] else None; staff = interaction.guild.get_role(cfg["staff_role"]) if cfg["staff_role"] else None
        overwrites = {interaction.guild.default_role: discord.PermissionOverwrite(view_channel=False), interaction.user: discord.PermissionOverwrite(view_channel=True,send_messages=True,read_message_history=True,attach_files=True), interaction.guild.me: discord.PermissionOverwrite(view_channel=True,send_messages=True,manage_channels=True,manage_messages=True,read_message_history=True)}
        if staff: overwrites[staff] = discord.PermissionOverwrite(view_channel=True,send_messages=True,read_message_history=True,attach_files=True)
        try:
            ch = await interaction.guild.create_text_channel(f"{TICKET_PREFIX}{interaction.user.id}", category=category, overwrites=overwrites, reason=f"Ticket opened by {interaction.user}")
            db_exec("INSERT INTO tickets(guild_id,channel_id,user_id,status,created_at) VALUES(?,?,?,?,?)", (interaction.guild.id,ch.id,interaction.user.id,"open",now_iso()))
            embed = discord.Embed(title="🎫 تذكرتك مفتوحة", description=f"مرحبًا {interaction.user.mention}، اكتب مشكلتك بالتفصيل هنا.\n\nاضغط زر الإغلاق عند انتهاء الدعم.", timestamp=datetime.now(timezone.utc))
            await ch.send(content=f"{interaction.user.mention}" + (f" {staff.mention}" if staff else ""), embed=embed, view=TicketCloseView())
            await reply(interaction,f"✅ تم فتح التذكرة: {ch.mention}")
            await send_log(interaction.guild,"🎫 Ticket Created",f"{interaction.user.mention} فتح {ch.mention}.")
        except discord.Forbidden: await reply(interaction,"❌ البوت لا يملك Manage Channels أو صلاحية إنشاء القنوات.")
        except discord.HTTPException as e: await reply(interaction,f"❌ تعذر إنشاء التذكرة: `{e}`")

class TicketCloseView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)
    @discord.ui.button(label="🔒 إغلاق التذكرة", style=discord.ButtonStyle.danger, custom_id="ticket_close")
    async def close(self, interaction, button):
        if not interaction.guild or not interaction.channel.name.startswith(TICKET_PREFIX): return await reply(interaction,"❌ هذه ليست قناة تذكرة.")
        row = db.execute("SELECT * FROM tickets WHERE channel_id=? AND status='open'", (interaction.channel.id,)).fetchone()
        if not row: return await reply(interaction,"❌ هذه التذكرة مسجلة كمغلقة.")
        if interaction.user.id != row["user_id"] and not is_staff(interaction.user): return await reply(interaction,"❌ فقط صاحب التذكرة أو فريق الإدارة يستطيع إغلاقها.")
        db_exec("UPDATE tickets SET status='closed',closed_at=? WHERE channel_id=?", (now_iso(),interaction.channel.id))
        await reply(interaction,"🔒 سيتم إغلاق التذكرة خلال 3 ثوانٍ.",ephemeral=False)
        await send_log(interaction.guild,"🔒 Ticket Closed",f"{interaction.user.mention} أغلق <#{interaction.channel.id}>.")
        await asyncio.sleep(3)
        try: await interaction.channel.delete(reason=f"Ticket closed by {interaction.user}")
        except discord.HTTPException: pass

class FootballAttackView(discord.ui.View):
    def __init__(self,game): super().__init__(timeout=45); self.game=game
    async def choose(self,interaction,n):
        if interaction.user.id != self.game["attacker"].id: return await reply(interaction,"❌ هذا الدور للمهاجم فقط.")
        if self.game["shot"] is not None: return await reply(interaction,"❌ تم الاختيار مسبقًا.")
        self.game["shot"]=n; self.stop()
        view=FootballKeeperView(self.game)
        await interaction.response.edit_message(content=f"🏃 {self.game['attacker'].mention} اختار تسديدة **{n}**.\n🧤 الآن دور {self.game['keeper'].mention} لاختيار جهة التصدي.",view=view)
    @discord.ui.button(label="1",style=discord.ButtonStyle.primary)
    async def one(self,i,b): await self.choose(i,1)
    @discord.ui.button(label="2",style=discord.ButtonStyle.primary)
    async def two(self,i,b): await self.choose(i,2)
    @discord.ui.button(label="3",style=discord.ButtonStyle.primary)
    async def three(self,i,b): await self.choose(i,3)

class FootballKeeperView(discord.ui.View):
    def __init__(self,game): super().__init__(timeout=45); self.game=game
    async def choose(self,interaction,n):
        if interaction.user.id != self.game["keeper"].id: return await reply(interaction,"❌ هذا الدور للحارس فقط.")
        shot=self.game["shot"]; self.stop(); result="🧤 **تصدى الحارس للكرة!**" if shot==n else "⚽ **هــــــــدف!**"
        await interaction.response.edit_message(content=f"🏃 المهاجم: **{shot}**\n🧤 الحارس: **{n}**\n\n{result}",view=None)
    @discord.ui.button(label="1",style=discord.ButtonStyle.success)
    async def one(self,i,b): await self.choose(i,1)
    @discord.ui.button(label="2",style=discord.ButtonStyle.success)
    async def two(self,i,b): await self.choose(i,2)
    @discord.ui.button(label="3",style=discord.ButtonStyle.success)
    async def three(self,i,b): await self.choose(i,3)

def image_similarity(a,b):
    try:
        im1=Image.open(io.BytesIO(a)).convert("L"); im2=Image.open(io.BytesIO(b)).convert("L"); im1=ImageOps.fit(im1,(256,256)); im2=ImageOps.fit(im2,(256,256)); im1=ImageOps.autocontrast(im1).filter(ImageFilter.GaussianBlur(1)); im2=ImageOps.autocontrast(im2).filter(ImageFilter.GaussianBlur(1)); p1=list(im1.getdata()); p2=list(im2.getdata()); mae=sum(abs(x-y) for x,y in zip(p1,p2))/(255*len(p1)); return max(0,min(100,100*(1-mae)))
    except Exception: return 0.0


# =========================
# CORE - الأنظمة العربية الموسعة
# =========================
BOT_VERSION = "4.0.0"
BOT_STARTED_AT = time.time()

db.executescript("""
CREATE TABLE IF NOT EXISTS cooldowns(guild_id INTEGER NOT NULL,user_id INTEGER NOT NULL,key TEXT NOT NULL,used_at REAL NOT NULL,PRIMARY KEY(guild_id,user_id,key));
CREATE TABLE IF NOT EXISTS server_roles(guild_id INTEGER NOT NULL,role_id INTEGER NOT NULL,kind TEXT NOT NULL,PRIMARY KEY(guild_id,role_id,kind));
CREATE TABLE IF NOT EXISTS custom_commands(guild_id INTEGER NOT NULL,name TEXT NOT NULL,response TEXT NOT NULL,author_id INTEGER NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(guild_id,name));
CREATE TABLE IF NOT EXISTS giveaways(id INTEGER PRIMARY KEY AUTOINCREMENT,guild_id INTEGER NOT NULL,channel_id INTEGER NOT NULL,message_id INTEGER NOT NULL,prize TEXT NOT NULL,winner_count INTEGER NOT NULL,ends_at REAL NOT NULL,ended INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS reminders_log(id INTEGER PRIMARY KEY AUTOINCREMENT,guild_id INTEGER NOT NULL,user_id INTEGER NOT NULL,text TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS audit_events(id INTEGER PRIMARY KEY AUTOINCREMENT,guild_id INTEGER NOT NULL,actor_id INTEGER NOT NULL,event TEXT NOT NULL,target_id INTEGER DEFAULT 0,details TEXT DEFAULT '',created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS user_stats(guild_id INTEGER NOT NULL,user_id INTEGER NOT NULL,messages INTEGER NOT NULL DEFAULT 0,commands INTEGER NOT NULL DEFAULT 0,joins INTEGER NOT NULL DEFAULT 0,last_seen TEXT,PRIMARY KEY(guild_id,user_id));
""")
db.commit()

def core_audit(guild_id, actor_id, event, target_id=0, details=""):
    try:
        db_exec("INSERT INTO audit_events(guild_id,actor_id,event,target_id,details,created_at) VALUES(?,?,?,?,?,?)",
                (guild_id,actor_id,event,target_id,details,datetime.now(timezone.utc).isoformat()))
    except Exception:
        pass

def core_uptime():
    seconds=max(0,int(time.time()-BOT_STARTED_AT))
    return human_seconds(seconds)

def core_embed(title, description="", color=None):
    e=discord.Embed(title=title,description=description,timestamp=datetime.now(timezone.utc),color=color or discord.Color.blurple())
    e.set_footer(text=f"{BOT_NAME} • V4")
    return e

def core_is_staff(interaction):
    return interaction.user.guild_permissions.administrator or interaction.user.guild_permissions.manage_guild or interaction.user.guild_permissions.manage_messages

def core_require_staff(interaction):
    return core_is_staff(interaction)

def core_money(value):
    return f"{int(value):,} عملة"

@tree.command(name=app_commands.locale_str("help", arabic="مساعدة"),description="عرض مركز المساعدة العربي الشامل لجميع أنظمة البوت")
async def core_help(interaction: discord.Interaction, القسم: str = "الكل"):
    sections={
        "الإدارة":"`رول` `لقب` `تحذير` `تحذيرات` `تايم` `اونتايم` `طرد` `حظر` `فك_حظر` `مخالفات` `ملاحظة` `ملاحظات` `تنظيف` `قفل` `فتح` `تبطيء`",
        "الاقتصاد":"`رصيدي` `يومي` `تحويل` `تحويل_آمن` `سوق` `شراء` `بيع` `ممتلكاتي` `راتب` `سجل_مالي` `ترتيب_اقتصاد`",
        "المستويات":"`لفل` `توب_لفل` `توب_مستويات` `جائزة_مستوى` `جوائز_المستويات`",
        "الحماية":"`إدارة_الأوتومود` `كلمة_ممنوعة` `كلمات_ممنوعة` `إعدادات`",
        "التفاعل":"`اقتراح` `استطلاع` `تكت` `تذكير` `تعيين_أفك` `كرة` `رسم`",
        "المعلومات":"`بنق` `معلومات_السيرفر` `معلومات_عضو` `الصورة` `معلومات_رتبة` `إحصائيات_البوت`",
        "V4":"`حول_البوت` `وقت_التشغيل` `حالة_البوت` `احصائيات_اقتصاد` `سجل_تدقيق` `أوامر_مخصصة`"
    }
    key=القسم.strip().lower()
    if key in ("الكل","all"):
        desc="\n\n".join(f"**{k}**\n{v}" for k,v in sections.items())
    else:
        desc=sections.get(القسم,"اختر قسمًا من: الإدارة، الاقتصاد، المستويات، الحماية، التفاعل، المعلومات، V4")
    await interaction.response.send_message(embed=core_embed("📚 مركز المساعدة العربي",desc))

@tree.command(name=app_commands.locale_str("about", arabic="حول_البوت"),description="عرض إصدار البوت ومعلوماته والأنظمة الموجودة فيه")
async def core_about(interaction: discord.Interaction):
    total=len(bot.tree.get_commands())
    e=core_embed("🤖 بوت الخلعاوية V4",f"**الإصدار:** `{BOT_VERSION}`\n**الأوامر:** `{total}` أمر Slash\n**الأنظمة:** إدارة • اقتصاد • مستويات • تذاكر • اقتراحات • AutoMod • سجلات • ألعاب • تذكيرات\n**اللغة:** العربية بالكامل\n**قاعدة البيانات:** SQLite مع WAL ونسخ احتياطي")
    if bot.user: e.set_thumbnail(url=bot.user.display_avatar.url)
    await interaction.response.send_message(embed=e)

@tree.command(name=app_commands.locale_str("uptime", arabic="وقت_التشغيل"),description="عرض مدة تشغيل البوت منذ آخر تشغيل")
async def core_uptime_cmd(interaction: discord.Interaction):
    await interaction.response.send_message(embed=core_embed("⏱️ وقت التشغيل",f"البوت يعمل منذ **{core_uptime()}**."))

@tree.command(name=app_commands.locale_str("status", arabic="حالة_البوت"),description="عرض حالة البوت والـ latency والذاكرة وقاعدة البيانات")
async def core_status(interaction: discord.Interaction):
    db_ok=False
    try: db.execute("SELECT 1").fetchone(); db_ok=True
    except Exception: pass
    e=core_embed("🟢 حالة البوت",f"**الحالة:** متصل\n**Ping:** `{round(bot.latency*1000)}ms`\n**قاعدة البيانات:** {'🟢 سليمة' if db_ok else '🔴 مشكلة'}\n**الخوادم:** `{len(bot.guilds)}`\n**الأوامر:** `{len(bot.tree.get_commands())}`\n**وقت التشغيل:** `{core_uptime()}`")
    await interaction.response.send_message(embed=e)

@tree.command(name=app_commands.locale_str("server_image", arabic="سيرفر_صورة"),description="عرض أيقونة السيرفر بجودة عالية")
async def core_server_image(interaction: discord.Interaction):
    g=interaction.guild
    e=core_embed(f"🖼️ صورة {g.name}",f"[فتح الصورة بجودة عالية]({g.icon.url if g.icon else 'https://discord.com'})")
    if g.icon: e.set_image(url=g.icon.url)
    await interaction.response.send_message(embed=e)

@tree.command(name=app_commands.locale_str("emoji", arabic="ايموجي"),description="عرض معلومات إيموجي مخصص باستخدام المعرف")
async def core_emoji(interaction: discord.Interaction, المعرّف: str):
    raw=re.sub(r"\D","",المعرّف)
    emoji=bot.get_emoji(int(raw)) if raw.isdigit() else None
    if not emoji:
        return await interaction.response.send_message("❌ لم أجد الإيموجي بهذا المعرف.",ephemeral=True)
    e=core_embed("😀 معلومات الإيموجي",f"**الاسم:** `{emoji.name}`\n**المعرف:** `{emoji.id}`\n**متحرك:** `{emoji.animated}`\n**السيرفر:** `{emoji.guild.name}`")
    e.set_thumbnail(url=emoji.url)
    await interaction.response.send_message(embed=e)

@tree.command(name=app_commands.locale_str("role_details", arabic="رتبة_تفاصيل"),description="عرض تفاصيل رتبة بالمعرف")
async def core_role_details(interaction: discord.Interaction, الرتبة: discord.Role):
    e=core_embed(f"🎭 {الرّتبة.name if False else الرتبة.name}",f"**المعرف:** `{الرتبة.id}`\n**المركز:** `{الرتبة.position}`\n**الأعضاء:** `{len(الرتبة.members)}`\n**لون الرتبة:** `{الرتبة.color}`\n**قابلة للذكر:** `{'نعم' if الرتبة.mentionable else 'لا'}`")
    await interaction.response.send_message(embed=e)

@tree.command(name=app_commands.locale_str("say", arabic="قول"),description="إرسال رسالة باسم البوت عبر أمر إداري")
@app_commands.checks.has_permissions(manage_messages=True)
async def core_say(interaction: discord.Interaction, النص: str):
    await interaction.response.defer(ephemeral=True)
    try:
        await interaction.channel.send(النص)
        core_audit(interaction.guild.id,interaction.user.id,"إرسال رسالة",0,النص[:500])
        await interaction.followup.send("✅ تم إرسال الرسالة.",ephemeral=True)
    except discord.Forbidden:
        await interaction.followup.send("❌ لا أستطيع إرسال الرسالة في هذه القناة.",ephemeral=True)

@tree.command(name=app_commands.locale_str("announce", arabic="إعلان"),description="إرسال إعلان عربي منسق مع عنوان ومحتوى")
@app_commands.checks.has_permissions(manage_guild=True)
async def core_announce(interaction: discord.Interaction, العنوان: str, المحتوى: str):
    e=core_embed(f"📢 {العنوان}",المحتوى,discord.Color.gold())
    e.set_author(name=interaction.guild.name,icon_url=interaction.guild.icon.url if interaction.guild.icon else None)
    await interaction.response.defer(ephemeral=True)
    await interaction.channel.send(embed=e)
    core_audit(interaction.guild.id,interaction.user.id,"إعلان",0,العنوان)
    await interaction.followup.send("✅ تم نشر الإعلان.",ephemeral=True)

@tree.command(name=app_commands.locale_str("give_money", arabic="منح_فلوس"),description="منح عملات لعضو من رصيد الإدارة")
@app_commands.checks.has_permissions(manage_guild=True)
async def core_give_money(interaction: discord.Interaction, العضو: discord.Member, المبلغ: int, السبب: str="مكافأة إدارية"):
    if المبلغ<=0 or المبلغ>10_000_000:
        return await interaction.response.send_message("❌ المبلغ يجب أن يكون بين 1 و 10,000,000.",ephemeral=True)
    db_exec("INSERT INTO economy(guild_id,user_id,balance) VALUES(?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET balance=balance+excluded.balance",(interaction.guild.id,العضو.id,المبلغ))
    db_exec("INSERT INTO transactions(guild_id,user_id,kind,amount,description,created_at) VALUES(?,?,?,?,?,?)",(interaction.guild.id,العضو.id,"منحة",المبلغ,السبب,datetime.now(timezone.utc).isoformat()))
    core_audit(interaction.guild.id,interaction.user.id,"منح عملات",العضو.id,f"{المبلغ}: {السبب}")
    await interaction.response.send_message(f"💰 تم منح {العضو.mention} **{core_money(المبلغ)}**.",ephemeral=True)

@tree.command(name=app_commands.locale_str("take_money", arabic="سحب_فلوس"),description="سحب عملات من رصيد عضو إداريًا")
@app_commands.checks.has_permissions(manage_guild=True)
async def core_take_money(interaction: discord.Interaction, العضو: discord.Member, المبلغ: int, السبب: str="خصم إداري"):
    if المبلغ<=0: return await interaction.response.send_message("❌ المبلغ غير صالح.",ephemeral=True)
    with DB_SYNC_LOCK:
        row=db.execute("SELECT balance FROM economy WHERE guild_id=? AND user_id=?",(interaction.guild.id,العضو.id)).fetchone()
        if not row or row["balance"]<المبلغ:
            return await interaction.response.send_message("❌ رصيد العضو غير كافٍ.",ephemeral=True)
        db.execute("UPDATE economy SET balance=balance-? WHERE guild_id=? AND user_id=?",(المبلغ,interaction.guild.id,العضو.id))
        db.execute("INSERT INTO transactions(guild_id,user_id,kind,amount,description,created_at) VALUES(?,?,?,?,?,?)",(interaction.guild.id,العضو.id,"سحب إداري",-المبلغ,السبب,datetime.now(timezone.utc).isoformat()))
        db.commit()
    await interaction.response.send_message(f"💸 تم سحب **{core_money(المبلغ)}** من {العضو.mention}.",ephemeral=True)

@tree.command(name=app_commands.locale_str("set_balance", arabic="تعيين_رصيد"),description="تعيين رصيد عضو إلى قيمة محددة")
@app_commands.checks.has_permissions(manage_guild=True)
async def core_set_balance(interaction: discord.Interaction, العضو: discord.Member, الرصيد: int):
    if الرصيد<0 or الرصيد>1_000_000_000: return await interaction.response.send_message("❌ الرصيد غير صالح.",ephemeral=True)
    db_exec("INSERT INTO economy(guild_id,user_id,balance) VALUES(?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET balance=excluded.balance",(interaction.guild.id,العضو.id,الرصيد))
    core_audit(interaction.guild.id,interaction.user.id,"تعيين رصيد",العضو.id,str(الرصيد))
    await interaction.response.send_message(f"✅ أصبح رصيد {العضو.mention}: **{core_money(الرصيد)}**.",ephemeral=True)

@tree.command(name=app_commands.locale_str("economy_stats", arabic="احصائيات_اقتصاد"),description="عرض إحصائيات اقتصاد السيرفر بالكامل")
async def core_economy_stats(interaction: discord.Interaction):
    g=interaction.guild.id
    row=db.execute("SELECT COUNT(*) c,COALESCE(SUM(balance),0) total,COALESCE(MAX(balance),0) max FROM economy WHERE guild_id=?",(g,)).fetchone()
    assets=db.execute("SELECT COALESCE(SUM(quantity),0) q FROM assets WHERE guild_id=?",(g,)).fetchone()["q"]
    e=core_embed("💰 إحصائيات اقتصاد السيرفر",f"**الحسابات:** `{row['c']}`\n**إجمالي الأموال:** `{core_money(row['total'])}`\n**أعلى رصيد:** `{core_money(row['max'])}`\n**إجمالي الممتلكات:** `{assets}`")
    await interaction.response.send_message(embed=e)

@tree.command(name=app_commands.locale_str("audit_log", arabic="سجل_تدقيق"),description="عرض آخر العمليات الإدارية المسجلة")
@app_commands.checks.has_permissions(view_audit_log=True)
async def core_audit_log(interaction: discord.Interaction, العدد: int=10):
    العدد=max(1,min(20,العدد))
    rows=db.execute("SELECT * FROM audit_events WHERE guild_id=? ORDER BY id DESC LIMIT ?",(interaction.guild.id,العدد)).fetchall()
    if not rows: return await interaction.response.send_message("📋 لا يوجد سجل تدقيق حتى الآن.",ephemeral=True)
    lines=[]
    for r in rows:
        lines.append(f"`#{r['id']}` <@{r['actor_id']}> — **{r['event']}** — الهدف: `{r['target_id']}`\n> {discord.utils.escape_markdown(r['details'])[:120]}")
    await interaction.response.send_message(embed=core_embed("📋 سجل التدقيق", "\n".join(lines)),ephemeral=True)

@tree.command(name=app_commands.locale_str("member_stats", arabic="احصائيات_عضو"),description="عرض نشاط عضو من قاعدة بيانات البوت")
async def core_member_stats(interaction: discord.Interaction, العضو: discord.Member=None):
    العضو=العضو or interaction.user
    r=db.execute("SELECT * FROM user_stats WHERE guild_id=? AND user_id=?",(interaction.guild.id,العضو.id)).fetchone()
    xp=db.execute("SELECT xp,level FROM levels WHERE guild_id=? AND user_id=?",(interaction.guild.id,العضو.id)).fetchone()
    bal=db.execute("SELECT balance FROM economy WHERE guild_id=? AND user_id=?",(interaction.guild.id,العضو.id)).fetchone()
    e=core_embed(f"📊 إحصائيات {العضو.display_name}",f"**الرسائل:** `{r['messages'] if r else 0}`\n**المستوى:** `{xp['level'] if xp else 0}`\n**XP:** `{xp['xp'] if xp else 0}`\n**الرصيد:** `{core_money(bal['balance'] if bal else 0)}`\n**آخر ظهور:** `{r['last_seen'] if r else 'غير مسجل'}`")
    e.set_thumbnail(url=العضو.display_avatar.url)
    await interaction.response.send_message(embed=e)

@tree.command(name=app_commands.locale_str("member_audit", arabic="سجل_عضو"),description="عرض السجل الإداري والاقتصادي لعضو")
@app_commands.checks.has_permissions(manage_guild=True)
async def core_member_audit(interaction: discord.Interaction, العضو: discord.Member):
    cases=db.execute("SELECT action,reason,created_at FROM moderation_cases WHERE guild_id=? AND user_id=? ORDER BY id DESC LIMIT 8",(interaction.guild.id,العضو.id)).fetchall()
    notes=db.execute("SELECT note,created_at FROM user_notes WHERE guild_id=? AND user_id=? ORDER BY id DESC LIMIT 5",(interaction.guild.id,العضو.id)).fetchall()
    text="**العقوبات:**\n"+("\n".join(f"• {r['action']}: {r['reason']}" for r in cases) if cases else "• لا توجد")
    text+="\n\n**الملاحظات:**\n"+("\n".join(f"• {r['note'][:150]}" for r in notes) if notes else "• لا توجد")
    await interaction.response.send_message(embed=core_embed(f"🗂️ سجل {العضو}",text),ephemeral=True)

@tree.command(name=app_commands.locale_str("backup", arabic="نسخة_احتياطية"),description="إنشاء نسخة احتياطية فورية لقاعدة البيانات")
@app_commands.checks.has_permissions(administrator=True)
async def core_backup(interaction: discord.Interaction):
    os.makedirs(BACKUP_DIR,exist_ok=True)
    path=os.path.join(BACKUP_DIR,f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
    await interaction.response.defer(ephemeral=True)
    try:
        with DB_SYNC_LOCK:
            dest=sqlite3.connect(path)
            db.backup(dest)
            dest.close()
        await interaction.followup.send(f"✅ تم إنشاء النسخة الاحتياطية: `{os.path.basename(path)}`.",ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ فشل النسخ الاحتياطي: `{type(e).__name__}`.",ephemeral=True)

@tree.command(name=app_commands.locale_str("custom_add", arabic="أمر_مخصص_إضافة"),description="إنشاء رد مخصص يستدعى من نظام الأوامر المخصصة")
@app_commands.checks.has_permissions(manage_guild=True)
async def core_custom_add(interaction: discord.Interaction, الاسم: str, الرد: str):
    الاسم=re.sub(r"[^a-zA-Z0-9_\-\u0600-\u06FF]","",الاسم)[:32]
    if len(الاسم)<2 or not الرد.strip(): return await interaction.response.send_message("❌ الاسم أو الرد غير صالح.",ephemeral=True)
    db_exec("INSERT INTO custom_commands(guild_id,name,response,author_id,created_at) VALUES(?,?,?,?,?) ON CONFLICT(guild_id,name) DO UPDATE SET response=excluded.response,author_id=excluded.author_id,created_at=excluded.created_at",(interaction.guild.id,الاسم,الرد[:1900],interaction.user.id,datetime.now(timezone.utc).isoformat()))
    await interaction.response.send_message(f"✅ تم حفظ الأمر المخصص `{الاسم}`.",ephemeral=True)

@tree.command(name=app_commands.locale_str("custom_delete", arabic="أمر_مخصص_حذف"),description="حذف أمر مخصص")
@app_commands.checks.has_permissions(manage_guild=True)
async def core_custom_delete(interaction: discord.Interaction, الاسم: str):
    cur=db_exec("DELETE FROM custom_commands WHERE guild_id=? AND name=?",(interaction.guild.id,الاسم))
    await interaction.response.send_message("✅ تم حذف الأمر." if cur else "❌ الأمر غير موجود.",ephemeral=True)

@tree.command(name=app_commands.locale_str("custom_list", arabic="أوامر_مخصصة"),description="عرض جميع الأوامر المخصصة في السيرفر")
async def core_custom_list(interaction: discord.Interaction):
    rows=db.execute("SELECT name FROM custom_commands WHERE guild_id=? ORDER BY name",(interaction.guild.id,)).fetchall()
    text="\n".join(f"• `{r['name']}`" for r in rows) if rows else "لا توجد أوامر مخصصة."
    await interaction.response.send_message(embed=core_embed("🧩 الأوامر المخصصة",text))

@tree.command(name=app_commands.locale_str("activity_reward", arabic="مكافأة_نشاط"),description="منح مكافأة مالية لمستخدم نشط")
@app_commands.checks.has_permissions(manage_guild=True)
async def core_activity_reward(interaction: discord.Interaction, العضو: discord.Member, المبلغ: int=500):
    if المبلغ<=0 or المبلغ>100000: return await interaction.response.send_message("❌ مبلغ غير صالح.",ephemeral=True)
    db_exec("INSERT INTO economy(guild_id,user_id,balance) VALUES(?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET balance=balance+excluded.balance",(interaction.guild.id,العضو.id,المبلغ))
    db_exec("INSERT INTO transactions(guild_id,user_id,kind,amount,description,created_at) VALUES(?,?,?,?,?,?)",(interaction.guild.id,العضو.id,"مكافأة نشاط",المبلغ,"مكافأة من الإدارة",datetime.now(timezone.utc).isoformat()))
    await interaction.response.send_message(f"🏆 تم منح {العضو.mention} **{core_money(المبلغ)}** كمكافأة نشاط.")

@tree.command(name=app_commands.locale_str("top_messages", arabic="توب_رسائل"),description="عرض أكثر أعضاء السيرفر نشاطًا بالرسائل المسجلة")
async def core_top_messages(interaction: discord.Interaction):
    rows=db.execute("SELECT user_id,messages FROM user_stats WHERE guild_id=? ORDER BY messages DESC LIMIT 10",(interaction.guild.id,)).fetchall()
    text="\n".join(f"**{i}.** <@{r['user_id']}> — `{r['messages']}` رسالة" for i,r in enumerate(rows,1)) if rows else "لا توجد بيانات بعد."
    await interaction.response.send_message(embed=core_embed("💬 أكثر الأعضاء نشاطًا",text))

@tree.command(name=app_commands.locale_str("reset_market", arabic="إعادة_إعدادات_الاقتصاد"),description="إعادة أسعار السوق الأساسية للسيرفر")
@app_commands.checks.has_permissions(administrator=True)
async def core_reset_market(interaction: discord.Interaction):
    for item,price in BASE_PRICES.items():
        db_exec("INSERT INTO prices(guild_id,item,price) VALUES(?,?,?) ON CONFLICT(guild_id,item) DO UPDATE SET price=excluded.price",(interaction.guild.id,item,price))
    await interaction.response.send_message("✅ تمت إعادة أسعار السوق الأساسية.",ephemeral=True)

@tree.command(name=app_commands.locale_str("db_check", arabic="تدقيق_قاعدة_البيانات"),description="فحص سلامة قاعدة بيانات البوت")
@app_commands.checks.has_permissions(administrator=True)
async def core_db_check(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        row=db.execute("PRAGMA integrity_check").fetchone()
        result=list(row)[0] if row else "unknown"
        await interaction.followup.send(f"🗄️ فحص قاعدة البيانات: **{result}**.",ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ فشل الفحص: `{type(e).__name__}`.",ephemeral=True)

@tree.command(name=app_commands.locale_str("prune_audit", arabic="تنظيف_سجلات_قديمة"),description="حذف سجلات التدقيق القديمة للحفاظ على حجم قاعدة البيانات")
@app_commands.checks.has_permissions(administrator=True)
async def core_prune_audit(interaction: discord.Interaction, الأيام: int=90):
    if الأيام<7 or الأيام>3650: return await interaction.response.send_message("❌ اختر مدة بين 7 و3650 يومًا.",ephemeral=True)
    cutoff=(datetime.now(timezone.utc)-timedelta(days=الأيام)).isoformat()
    cur=db_exec("DELETE FROM audit_events WHERE guild_id=? AND created_at<?",(interaction.guild.id,cutoff))
    await interaction.response.send_message(f"🧹 تم حذف **{cur}** سجلًا أقدم من {الأيام} يومًا.",ephemeral=True)

@tree.command(name=app_commands.locale_str("permissions", arabic="فحص_صلاحيات"),description="فحص صلاحيات البوت ورتبته في السيرفر")
async def core_permissions(interaction: discord.Interaction):
    me=interaction.guild.me
    perms=me.guild_permissions
    wanted=["manage_messages","manage_roles","manage_nicknames","kick_members","ban_members","moderate_members","manage_channels","manage_guild"]
    lines=[f"• `{p}`: {'🟢' if getattr(perms,p,False) else '🔴'}" for p in wanted]
    lines.append(f"• أعلى رتبة للبوت: **{me.top_role.name}**")
    await interaction.response.send_message(embed=core_embed("🔐 فحص صلاحيات البوت","\n".join(lines)),ephemeral=True)

@tree.command(name=app_commands.locale_str("invite", arabic="رابط_الدعوة"),description="إنشاء رابط دعوة دائم أو مؤقت إذا كانت صلاحيات البوت تسمح")
@app_commands.checks.has_permissions(manage_guild=True)
async def core_invite(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    for ch in interaction.guild.text_channels:
        if ch.permissions_for(interaction.guild.me).create_instant_invite:
            try:
                inv=await ch.create_invite(max_age=86400,max_uses=0,reason="طلب إداري")
                return await interaction.followup.send(f"🔗 رابط دعوة لمدة 24 ساعة:\n{inv.url}",ephemeral=True)
            except Exception: pass
    await interaction.followup.send("❌ لم أجد قناة أستطيع إنشاء دعوة فيها.",ephemeral=True)

@tree.command(name=app_commands.locale_str("channel_info", arabic="معلومات_القناة"),description="عرض معلومات القناة الحالية")
async def core_channel_info(interaction: discord.Interaction):
    ch=interaction.channel
    e=core_embed("📺 معلومات القناة",f"**الاسم:** `{ch.name}`\n**المعرف:** `{ch.id}`\n**النوع:** `{str(ch.type)}`\n**الفئة:** `{ch.category.name if getattr(ch,'category',None) else 'بدون فئة'}`\n**Slowmode:** `{getattr(ch,'slowmode_delay',0)} ثانية`")
    await interaction.response.send_message(embed=e)

@tree.command(name=app_commands.locale_str("bot_info", arabic="معلومات_البوت"),description="عرض معلومات تقنية موسعة عن البوت")
async def core_bot_info(interaction: discord.Interaction):
    e=core_embed("⚙️ المعلومات التقنية",f"**Python:** `{__import__('platform').python_version()}`\n**discord.py:** `{discord.__version__}`\n**إصدار البوت:** `{BOT_VERSION}`\n**الخوادم:** `{len(bot.guilds)}`\n**المستخدمون التقريبيون:** `{sum(g.member_count or 0 for g in bot.guilds):,}`\n**وقت التشغيل:** `{core_uptime()}`\n**Ping:** `{round(bot.latency*1000)}ms`")
    await interaction.response.send_message(embed=e)

# تحديث إحصائيات النشاط عند كل رسالة من داخل معالج الرسائل الموحد.
async def core_track_message(message):
    try:
        now=datetime.now(timezone.utc).isoformat()
        db_exec("""INSERT INTO user_stats(guild_id,user_id,messages,last_seen) VALUES(?,?,1,?)
                   ON CONFLICT(guild_id,user_id) DO UPDATE SET messages=messages+1,last_seen=excluded.last_seen""",
                (message.guild.id,message.author.id,now))
    except Exception:
        pass

@tree.command(name=app_commands.locale_str("role", arabic="رول"),description="إضافة رتبة لعضو أو إزالتها")
@app_commands.checks.has_permissions(manage_roles=True)
async def role_cmd(interaction, العضو: discord.Member, الرتبة: discord.Role):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    me=interaction.guild.me
    if الرتبة.is_default() or الرتبة.managed: return await reply(interaction,"❌ لا يمكن إدارة هذه الرتبة.")
    if الرتبة >= me.top_role: return await reply(interaction,"❌ رتبة البوت يجب أن تكون أعلى من الرتبة المستهدفة.")
    if العضو.id == interaction.guild.owner_id or العضو.top_role >= me.top_role or العضو.top_role >= interaction.user.top_role and interaction.user.id != interaction.guild.owner_id: return await reply(interaction,"❌ لا يمكنك إدارة هذا العضو بسبب ترتيب الرتب.")
    try:
        if الرتبة in العضو.roles: await العضو.remove_roles(الرتبة,reason=f"Role toggle by {interaction.user}"); text=f"تمت إزالة {الرتبة.mention} من {العضو.mention}."
        else: await العضو.add_roles(الرتبة,reason=f"Role toggle by {interaction.user}"); text=f"تمت إضافة {الرتبة.mention} إلى {العضو.mention}."
        await reply(interaction,"✅ "+text); await send_log(interaction.guild,"🎭 Role Update",text)
    except discord.Forbidden: await reply(interaction,"❌ Discord رفض العملية؛ تحقق من Hierarchy وصلاحيات Manage Roles.")

@tree.command(name=app_commands.locale_str("timeout", arabic="تايم"),description="تطبيق Timeout لمدة محددة")
@app_commands.checks.has_permissions(moderate_members=True)
async def timeout_cmd(interaction, العضو: discord.Member, المدة: str, السبب: str="بدون سبب"):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    delta=parse_duration(المدة)
    if not delta: return await reply(interaction,"❌ مدة غير صحيحة. مثال: `10m` أو `2h` أو `1d`، والحد 28 يومًا.")
    if العضو.id==interaction.guild.owner_id or العضو.top_role>=interaction.guild.me.top_role: return await reply(interaction,"❌ لا أستطيع عمل Timeout لهذا العضو بسبب Hierarchy.")
    try: await العضو.timeout(delta,reason=السبب); await reply(interaction,f"🔇 تم عمل Timeout لـ {العضو.mention} لمدة **{المدة}**."); await send_log(interaction.guild,"🔇 Timeout",f"{interaction.user.mention} -> {العضو.mention}\nالسبب: {السبب}")
    except discord.Forbidden: await reply(interaction,"❌ لا أملك صلاحية Timeout لهذا العضو.")

@tree.command(name=app_commands.locale_str("untimeout", arabic="اونتايم"),description="إلغاء Timeout عن عضو")
@app_commands.checks.has_permissions(moderate_members=True)
async def untimeout_cmd(interaction, العضو: discord.Member):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    try: await العضو.timeout(None,reason=f"Untimeout by {interaction.user}"); await reply(interaction,f"✅ تم فك Timeout عن {العضو.mention}.")
    except discord.Forbidden: await reply(interaction,"❌ لا أملك صلاحية فك Timeout عن هذا العضو.")

@tree.command(name=app_commands.locale_str("warn", arabic="تحذير"),description="إضافة تحذير لعضو")
@app_commands.checks.has_permissions(moderate_members=True)
async def warn_cmd(interaction, العضو: discord.Member, السبب: str="بدون سبب"):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    db_exec("INSERT INTO warnings(guild_id,user_id,moderator_id,reason,created_at) VALUES(?,?,?,?,?)",(interaction.guild.id,العضو.id,interaction.user.id,السبب,now_iso()))
    count=db.execute("SELECT COUNT(*) c FROM warnings WHERE guild_id=? AND user_id=?",(interaction.guild.id,العضو.id)).fetchone()["c"]
    await reply(interaction,f"⚠️ تم تحذير {العضو.mention}. عدد تحذيراته: **{count}**.\nالسبب: {السبب}"); await send_log(interaction.guild,"⚠️ Warning",f"{interaction.user.mention} حذر {العضو.mention}\n{السبب}")

@tree.command(name=app_commands.locale_str("warns", arabic="تحذيرات"),description="عرض تحذيرات عضو")
@app_commands.checks.has_permissions(moderate_members=True)
async def warns_cmd(interaction, العضو: discord.Member):
    rows=db.execute("SELECT reason,moderator_id,created_at FROM warnings WHERE guild_id=? AND user_id=? ORDER BY id DESC LIMIT 10",(interaction.guild.id,العضو.id)).fetchall()
    if not rows: return await reply(interaction,"✅ لا توجد تحذيرات مسجلة.")
    lines=[f"**#{i+1}** {r['reason']} — <@{r['moderator_id']}> — {r['created_at'][:10]}" for i,r in enumerate(rows)]
    await reply(interaction, "\n".join(lines))

@tree.command(name=app_commands.locale_str("nickname", arabic="لقب"),description="تغيير لقب عضو فعليًا داخل السيرفر وحفظه")
@app_commands.checks.has_permissions(manage_nicknames=True)
async def nickname_cmd(interaction, العضو: discord.Member, اللقب: str):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    اللقب=اللقب.strip()
    if len(اللقب)>32: return await reply(interaction,"❌ اللقب يجب ألا يتجاوز 32 حرفًا.")
    if العضو.id==interaction.guild.owner_id or العضو.top_role>=interaction.guild.me.top_role or (العضو.top_role >= interaction.user.top_role and interaction.user.id != interaction.guild.owner_id): return await reply(interaction,"❌ لا يمكنك تغيير لقب هذا العضو بسبب Hierarchy.")
    try:
        await العضو.edit(nick=اللقب,reason=f"Nickname changed by {interaction.user}")
        db_exec("INSERT INTO nicknames(guild_id,user_id,nickname,updated_at) VALUES(?,?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET nickname=excluded.nickname,updated_at=excluded.updated_at",(interaction.guild.id,العضو.id,اللقب,now_iso()))
        await reply(interaction,f"🏷️ تم تغيير لقب {العضو.mention} إلى **{discord.utils.escape_markdown(اللقب)}** وحفظه في قاعدة البيانات."); await send_log(interaction.guild,"🏷️ Nickname Update",f"{interaction.user.mention} غيّر لقب {العضو.mention} إلى `{اللقب}`.")
    except discord.Forbidden: await reply(interaction,"❌ لا أستطيع تغيير اللقب. تأكد من Manage Nicknames وأن رتبة البوت أعلى من العضو.")
    except discord.HTTPException: await reply(interaction,"❌ حدث خطأ من Discord أثناء تغيير اللقب.")

@tree.command(name=app_commands.locale_str("nickname_remove", arabic="لقب_إزالة"),description="إزالة لقب عضو وإرجاعه للاسم الأساسي")
@app_commands.checks.has_permissions(manage_nicknames=True)
async def nickname_remove(interaction, العضو: discord.Member):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    if العضو.id==interaction.guild.owner_id or العضو.top_role>=interaction.guild.me.top_role or (العضو.top_role >= interaction.user.top_role and interaction.user.id != interaction.guild.owner_id): return await reply(interaction,"❌ لا يمكنك تعديل لقب هذا العضو بسبب Hierarchy.")
    try:
        await العضو.edit(nick=None,reason=f"Nickname reset by {interaction.user}"); db_exec("DELETE FROM nicknames WHERE guild_id=? AND user_id=?",(interaction.guild.id,العضو.id)); await reply(interaction,f"✅ تم إزالة لقب {العضو.mention}.")
    except discord.Forbidden: await reply(interaction,"❌ لا أملك صلاحية تغيير لقب هذا العضو.")

@tree.command(name=app_commands.locale_str("nickname_saved", arabic="لقب_محفوظ"),description="عرض اللقب المحفوظ لعضو")
async def nickname_saved(interaction, العضو: discord.Member=None):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    العضو=العضو or interaction.user; row=db.execute("SELECT nickname FROM nicknames WHERE guild_id=? AND user_id=?",(interaction.guild.id,العضو.id)).fetchone()
    await reply(interaction,f"🏷️ اللقب المحفوظ لـ {العضو.mention}: **{row['nickname']}**" if row else "ℹ️ لا يوجد لقب محفوظ.")

@tree.command(name=app_commands.locale_str("suggestion", arabic="اقتراح"),description="إرسال اقتراح إلى قناة الاقتراحات")
async def suggestion_cmd(interaction, النص: str):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    if not 5<=len(النص)<=1000: return await reply(interaction,"❌ الاقتراح يجب أن يكون بين 5 و1000 حرف.")
    cfg=get_config(interaction.guild.id); channel=interaction.guild.get_channel(cfg["suggestions_channel"]) if cfg["suggestions_channel"] else None
    if not channel: return await reply(interaction,"❌ لم يتم تحديد قناة الاقتراحات.")
    embed=discord.Embed(title="💡 اقتراح جديد",description=النص,timestamp=datetime.now(timezone.utc)); embed.set_author(name=interaction.user.display_name,icon_url=interaction.user.display_avatar.url)
    msg=await channel.send(embed=embed)
    db_exec("INSERT INTO suggestions(guild_id,channel_id,message_id,user_id,text,created_at) VALUES(?,?,?,?,?,?)",(interaction.guild.id,channel.id,msg.id,interaction.user.id,النص,now_iso()))
    sid=db.execute("SELECT last_insert_rowid() x").fetchone()["x"]; await msg.edit(view=SuggestionView(sid, 0, 0)); await reply(interaction,"✅ تم إرسال اقتراحك.")

@tree.command(name=app_commands.locale_str("suggestions_setup", arabic="اقتراحات_تعيين"),description="تحديد قناة الاقتراحات")
@app_commands.checks.has_permissions(manage_guild=True)
async def suggestions_setup(interaction, القناة: discord.TextChannel):
    set_config(interaction.guild.id,"suggestions_channel",القناة.id); await reply(interaction,f"✅ قناة الاقتراحات أصبحت {القناة.mention}.")

@tree.command(name=app_commands.locale_str("suggestion_status", arabic="اقتراح_حالة"),description="تغيير حالة اقتراح")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.choices(الحالة=[app_commands.Choice(name="قيد المراجعة",value="pending"),app_commands.Choice(name="مقبول",value="accepted"),app_commands.Choice(name="مرفوض",value="rejected")])
async def suggestion_status(interaction, الرقم: int, الحالة: app_commands.Choice[str]):
    row=db.execute("SELECT * FROM suggestions WHERE id=? AND guild_id=?",(الرقم,interaction.guild.id)).fetchone()
    if not row: return await reply(interaction,"❌ رقم الاقتراح غير موجود.")
    db_exec("UPDATE suggestions SET status=? WHERE id=?",(الحالة.value,الرقم)); await reply(interaction,f"✅ تم تغيير حالة الاقتراح **#{الرقم}** إلى **{الحالة.name}**.")

@tree.command(name=app_commands.locale_str("ticket", arabic="تكت"),description="إرسال لوحة التذاكر")
@app_commands.checks.has_permissions(manage_guild=True)
async def ticket_cmd(interaction):
    embed=discord.Embed(title="🎫 الدعم والتذاكر",description="اضغط **فتح تذكرة** لإنشاء قناة خاصة بك. سيظهر فريق الإدارة داخلها تلقائيًا."); await interaction.channel.send(embed=embed,view=TicketView()); await reply(interaction,"✅ تم إرسال لوحة التذاكر.")

@tree.command(name=app_commands.locale_str("welcome_setup", arabic="ترحيب_تعيين"),description="تحديد قناة الترحيب")
@app_commands.checks.has_permissions(manage_guild=True)
async def welcome_setup(interaction, القناة: discord.TextChannel):
    set_config(interaction.guild.id,"welcome_channel",القناة.id); await reply(interaction,f"👋 قناة الترحيب: {القناة.mention}")

@tree.command(name=app_commands.locale_str("ticket_category", arabic="تذاكر_تصنيف"),description="تحديد Category للتذاكر")
@app_commands.checks.has_permissions(manage_guild=True)
async def ticket_category(interaction, التصنيف: discord.CategoryChannel):
    set_config(interaction.guild.id,"tickets_category",التصنيف.id); await reply(interaction,f"✅ تم تحديد تصنيف التذاكر: **{التصنيف.name}**.")

@tree.command(name=app_commands.locale_str("ticket_staff", arabic="طاقم_تذاكر"),description="تحديد رتبة فريق التذاكر")
@app_commands.checks.has_permissions(manage_guild=True)
async def ticket_staff(interaction, الرتبة: discord.Role):
    set_config(interaction.guild.id,"staff_role",الرتبة.id); await reply(interaction,f"✅ رتبة فريق التذاكر: {الرتبة.mention}")

@tree.command(name=app_commands.locale_str("log_setup", arabic="لوق"),description="تحديد قناة السجلات")
@app_commands.checks.has_permissions(manage_guild=True)
async def log_setup(interaction, القناة: discord.TextChannel):
    set_config(interaction.guild.id,"log_channel",القناة.id); await reply(interaction,f"📝 قناة السجلات: {القناة.mention}")

@tree.command(name=app_commands.locale_str("level", arabic="لفل"),description="عرض مستوى وXP عضو")
async def level_cmd(interaction, العضو: discord.Member=None):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    العضو=العضو or interaction.user; xp,level=get_level(interaction.guild.id,العضو.id); await reply(interaction,f"⭐ {العضو.mention}\nالمستوى: **{level}**\nXP: **{xp}/{xp_needed(level)}**")

@tree.command(name=app_commands.locale_str("level_leaderboard", arabic="توب_لفل"),description="عرض أعلى أعضاء XP")
async def leaderboard(interaction):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    rows=db.execute("SELECT user_id,xp,level FROM levels WHERE guild_id=? ORDER BY level DESC,xp DESC LIMIT 10",(interaction.guild.id,)).fetchall()
    if not rows: return await reply(interaction,"ℹ️ لا توجد بيانات مستويات.")
    await reply(interaction,"🏆 **أفضل 10 في اللفل**\n"+"\n".join(f"**{i}.** <@{r['user_id']}> — Level **{r['level']}** ({r['xp']} XP)" for i,r in enumerate(rows,1)))

@tree.command(name=app_commands.locale_str("balance", arabic="رصيدي"),description="عرض رصيدك")
async def balance_cmd(interaction):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    await reply(interaction,f"🏦 رصيدك: **{get_balance(interaction.guild.id,interaction.user.id):,}**")

@tree.command(name=app_commands.locale_str("daily", arabic="يومي"),description="استلام مكافأة يومية")
async def daily_cmd(interaction):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    last=db.execute("SELECT claimed_at FROM daily_claims WHERE guild_id=? AND user_id=?",(interaction.guild.id,interaction.user.id)).fetchone()
    if last and time.time()-datetime.fromisoformat(last["claimed_at"]).timestamp()<86400: return await reply(interaction,"⏳ استلمت مكافأتك اليومية بالفعل. عد بعد 24 ساعة.")
    amount=random.randint(500,1500)
    def claim():
        current=get_balance(interaction.guild.id,interaction.user.id)
        set_balance(interaction.guild.id,interaction.user.id,current+amount)
        db.execute("INSERT INTO daily_claims(guild_id,user_id,claimed_at) VALUES(?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET claimed_at=excluded.claimed_at",(interaction.guild.id,interaction.user.id,now_iso()))
        add_transaction(interaction.guild.id,interaction.user.id,"daily",amount,"Daily reward")
    db_transaction(claim)
    await reply(interaction,f"🎁 حصلت على **{amount:,}** من المكافأة اليومية.")

@tree.command(name=app_commands.locale_str("transfer", arabic="تحويل"),description="تحويل أموال إلى عضو")
async def transfer_cmd(interaction, العضو: discord.Member, المبلغ: int):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    if المبلغ<=0 or العضو.bot or العضو.id==interaction.user.id: return await reply(interaction,"❌ مبلغ أو عضو غير صالح.")
    def transfer():
        bal=get_balance(interaction.guild.id,interaction.user.id)
        if bal<المبلغ: return False
        set_balance(interaction.guild.id,interaction.user.id,bal-المبلغ)
        set_balance(interaction.guild.id,العضو.id,get_balance(interaction.guild.id,العضو.id)+المبلغ)
        add_transaction(interaction.guild.id,interaction.user.id,"transfer_out",-المبلغ,f"to {العضو.id}")
        add_transaction(interaction.guild.id,العضو.id,"transfer_in",المبلغ,f"from {interaction.user.id}")
        return True
    if not db_transaction(transfer): return await reply(interaction,"❌ رصيدك لا يكفي.")
    await reply(interaction,f"💸 تم تحويل **{المبلغ:,}** إلى {العضو.mention}.")

@tree.command(name=app_commands.locale_str("market", arabic="سوق"),description="عرض أسعار السوق")
async def market_cmd(interaction):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    await reply(interaction,"📈 **السوق**\n"+"\n".join(f"**{item}** — {current_price(interaction.guild.id,item):,}" for item in BANK_ITEMS))

@tree.command(name=app_commands.locale_str("buy", arabic="شراء"),description="شراء عنصر")
@app_commands.choices(العنصر=[app_commands.Choice(name="سيارة",value="سيارة"),app_commands.Choice(name="ملعب",value="ملعب")])
async def buy_cmd(interaction, العنصر: app_commands.Choice[str], الكمية: int=1):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    item=العنصر.value
    if not 1<=الكمية<=100: return await reply(interaction,"❌ الكمية يجب أن تكون بين 1 و100.")
    price=current_price(interaction.guild.id,item); total=price*الكمية
    def buy():
        bal=get_balance(interaction.guild.id,interaction.user.id)
        if bal<total: return False
        set_balance(interaction.guild.id,interaction.user.id,bal-total)
        db.execute("INSERT INTO assets(guild_id,user_id,item,quantity) VALUES(?,?,?,?) ON CONFLICT(guild_id,user_id,item) DO UPDATE SET quantity=quantity+excluded.quantity",(interaction.guild.id,interaction.user.id,item,الكمية))
        add_transaction(interaction.guild.id,interaction.user.id,"purchase",-total,f"{الكمية}x {item}")
        return True
    if not db_transaction(buy): return await reply(interaction,f"❌ تحتاج **{total:,}** أو أن رصيدك تغير أثناء العملية.")
    await reply(interaction,f"✅ اشتريت **{الكمية} {item}** مقابل **{total:,}**.")

@tree.command(name=app_commands.locale_str("sell", arabic="بيع"),description="بيع عنصر")
@app_commands.choices(العنصر=[app_commands.Choice(name="سيارة",value="سيارة"),app_commands.Choice(name="ملعب",value="ملعب")])
async def sell_cmd(interaction, العنصر: app_commands.Choice[str], الكمية: int=1):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    item=العنصر.value
    if الكمية<1: return await reply(interaction,"❌ الكمية يجب أن تكون 1 أو أكثر.")
    row=db.execute("SELECT quantity FROM assets WHERE guild_id=? AND user_id=? AND item=?",(interaction.guild.id,interaction.user.id,item)).fetchone(); owned=row["quantity"] if row else 0
    if owned<الكمية: return await reply(interaction,"❌ لا تملك هذه الكمية.")
    total=current_price(interaction.guild.id,item)*الكمية
    def sell():
        row=db.execute("SELECT quantity FROM assets WHERE guild_id=? AND user_id=? AND item=?",(interaction.guild.id,interaction.user.id,item)).fetchone()
        owned=row["quantity"] if row else 0
        if owned<الكمية: return False
        db.execute("UPDATE assets SET quantity=quantity-? WHERE guild_id=? AND user_id=? AND item=?",(الكمية,interaction.guild.id,interaction.user.id,item))
        set_balance(interaction.guild.id,interaction.user.id,get_balance(interaction.guild.id,interaction.user.id)+total)
        add_transaction(interaction.guild.id,interaction.user.id,"sale",total,f"{الكمية}x {item}")
        return True
    if not db_transaction(sell): return await reply(interaction,"❌ لا تملك هذه الكمية أو تغيرت ممتلكاتك أثناء العملية.")
    await reply(interaction,f"✅ بعت **{الكمية} {item}** مقابل **{total:,}**.")

@tree.command(name=app_commands.locale_str("assets", arabic="ممتلكاتي"),description="عرض ممتلكاتك")
async def assets_cmd(interaction):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    rows=db.execute("SELECT item,quantity FROM assets WHERE guild_id=? AND user_id=? AND quantity>0",(interaction.guild.id,interaction.user.id)).fetchall()
    await reply(interaction,"📦 **ممتلكاتك**\n"+"\n".join(f"• {r['item']}: **{r['quantity']}**" for r in rows) if rows else "📦 لا تملك ممتلكات.")

@tasks.loop(minutes=30)
async def market_fluctuation():
    for gid_row in db.execute("SELECT DISTINCT guild_id FROM prices").fetchall():
        gid=gid_row["guild_id"]
        for item,base in BASE_PRICES.items():
            old=current_price(gid,item); new=max(int(base*0.25),min(int(base*2.5),int(old*(1+random.uniform(-0.10,0.10))))); db_exec("UPDATE prices SET price=? WHERE guild_id=? AND item=?",(new,gid,item))

@market_fluctuation.before_loop
async def before_market(): await bot.wait_until_ready()

@tasks.loop(hours=24)
async def database_backup():
    try:
        path=create_backup()
        print(f"[BACKUP] Database backup created: {path}")
    except Exception as e:
        print(f"[BACKUP ERROR] {type(e).__name__}: {e}")

@database_backup.before_loop
async def before_backup(): await bot.wait_until_ready()

@tree.command(name=app_commands.locale_str("football", arabic="كرة"),description="لعبة مهاجم ضد حارس")
async def football_cmd(interaction, الحارس: discord.Member, المهاجم: discord.Member):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    if الحارس.id==المهاجم.id: return await reply(interaction,"❌ يجب اختيار شخصين مختلفين.")
    game={"attacker":المهاجم,"keeper":الحارس,"shot":None}
    embed=discord.Embed(title="⚽ مهاجم ضد حارس",description=f"🏃 المهاجم: {المهاجم.mention}\n🧤 الحارس: {الحارس.mention}\n\nاختر رقم مكان التسديد.")
    await interaction.response.send_message(embed=embed,view=FootballAttackView(game))

@tree.command(name=app_commands.locale_str("drawing", arabic="رسم"),description="لعبة رسم: شاهد الصورة ثم أرسل رسمتك")
async def drawing_cmd(interaction, صورة: discord.Attachment):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    if not صورة.content_type or not صورة.content_type.startswith("image/"): return await reply(interaction,"❌ أرسل ملف صورة صالح.")
    source=await صورة.read()
    if len(source)>8*1024*1024: return await reply(interaction,"❌ حجم الصورة كبير جدًا؛ الحد 8MB.")
    await interaction.response.send_message("🎨 راقب الصورة لمدة **10 ثوانٍ** ثم ستبدأ المسابقة.")
    ref=await interaction.channel.send(file=discord.File(io.BytesIO(source),filename="مرجع-الرسم.png")); await asyncio.sleep(10)
    try: await ref.delete()
    except Exception: pass
    await interaction.channel.send("✏️ انتهى وقت المشاهدة! أرسل رسمتك كصورة خلال **60 ثانية**. سيتم احتساب أول رسمة لكل مشارك.")
    deadline=time.monotonic()+60; submissions={}
    def check(m): return m.channel.id==interaction.channel.id and not m.author.bot and m.attachments and m.attachments[0].content_type and m.attachments[0].content_type.startswith("image/")
    while time.monotonic()<deadline:
        try: m=await bot.wait_for("message",timeout=max(.1,deadline-time.monotonic()),check=check)
        except asyncio.TimeoutError: break
        if m.author.id not in submissions: submissions[m.author.id]=(m.author,await m.attachments[0].read()); await m.add_reaction("🎨")
    if not submissions: return await interaction.channel.send("❌ لم تصل أي رسمة.")
    scores=sorted([(image_similarity(source,data),member) for member,data in submissions.values()],reverse=True,key=lambda x:x[0]); score,winner=scores[0]; await interaction.channel.send(f"🏆 الفائز: {winner.mention}\n🎯 درجة التشابه التقريبية: **{score:.1f}%**")

@tree.command(name=app_commands.locale_str("settings", arabic="إعدادات"),description="عرض إعدادات البوت الأساسية للسيرفر")
@app_commands.checks.has_permissions(manage_guild=True)
async def settings_cmd(interaction):
    c=get_config(interaction.guild.id); get=lambda x:f"<#{c[x]}>" if c[x] else "غير محدد"; role=f"<@&{c['staff_role']}>" if c["staff_role"] else "غير محدد"
    await reply(interaction,f"⚙️ **إعدادات البوت**\n👋 الترحيب: {get('welcome_channel')}\n💡 الاقتراحات: {get('suggestions_channel')}\n🎫 التصنيف: {get('tickets_category')}\n🛡️ طاقم التذاكر: {role}\n📝 اللوق: {get('log_channel')}")

@tree.command(name=app_commands.locale_str("economy_leaderboard", arabic="ترتيب_اقتصاد"),description="عرض أعلى أرصدة")
async def economy_leaderboard(interaction):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    rows=db.execute("SELECT user_id,balance FROM economy WHERE guild_id=? ORDER BY balance DESC LIMIT 10",(interaction.guild.id,)).fetchall()
    await reply(interaction,"💰 **أغنى 10 أعضاء**\n"+"\n".join(f"**{i}.** <@{r['user_id']}> — **{r['balance']:,}**" for i,r in enumerate(rows,1)) if rows else "ℹ️ لا توجد بيانات اقتصادية.")


# ============================================================
# CORE EXPANSION — إدارة متقدمة، أدوات، اقتصاد، مسابقات
# ============================================================

def human_seconds(seconds:int) -> str:
    seconds=max(0,int(seconds)); d,seconds=divmod(seconds,86400); h,seconds=divmod(seconds,3600); m,s=divmod(seconds,60)
    parts=[]
    if d: parts.append(f"{d}ي")
    if h: parts.append(f"{h}س")
    if m: parts.append(f"{m}د")
    if s or not parts: parts.append(f"{s}ث")
    return " ".join(parts[:4])

def safe_member(guild, user_id):
    return guild.get_member(int(user_id)) if guild else None

def bot_member(guild):
    return guild.me or guild.get_member(bot.user.id)

def can_act_on(interaction, member):
    me=bot_member(interaction.guild)
    if not me or member == interaction.user: return False
    return member.top_role < me.top_role and member != interaction.guild.owner

async def ensure_me(interaction, member, action="تنفيذ العملية"):
    if not can_act_on(interaction, member):
        await reply(interaction, f"❌ لا أستطيع {action} على هذا العضو بسبب ترتيب الرتب أو لأن العضو هو مالك السيرفر.")
        return False
    return True

def add_case(guild_id,user_id,moderator_id,action,reason,duration=0):
    return db_exec("INSERT INTO moderation_cases(guild_id,user_id,moderator_id,action,reason,created_at,duration) VALUES(?,?,?,?,?,?,?)",
                   (guild_id,user_id,moderator_id,action,reason,now_iso(),duration))

def get_auto(guild_id):
    row=db.execute("SELECT * FROM automod WHERE guild_id=?",(guild_id,)).fetchone()
    if not row:
        db_exec("INSERT OR IGNORE INTO automod(guild_id) VALUES(?)",(guild_id,))
        row=db.execute("SELECT * FROM automod WHERE guild_id=?",(guild_id,)).fetchone()
    return row


def level_xp_required(level): return max(100, int(100*(level**1.55)))

class ConfirmView(discord.ui.View):
    def __init__(self, on_confirm, on_cancel=None):
        super().__init__(timeout=45); self.on_confirm=on_confirm; self.on_cancel=on_cancel
    @discord.ui.button(label="تأكيد",style=discord.ButtonStyle.danger,emoji="✅")
    async def confirm(self,interaction,button):
        await self.on_confirm(interaction); self.stop()
    @discord.ui.button(label="إلغاء",style=discord.ButtonStyle.secondary,emoji="✖️")
    async def cancel(self,interaction,button):
        if self.on_cancel: await self.on_cancel(interaction)
        else: await interaction.response.edit_message(content="تم الإلغاء.",view=None)
        self.stop()

@tree.command(name=app_commands.locale_str("ping", arabic="بنق"),description="عرض سرعة استجابة البوت")
async def ping(interaction):
    start=time.perf_counter()
    await interaction.response.defer()
    latency=round(bot.latency*1000)
    elapsed=round((time.perf_counter()-start)*1000)
    await interaction.followup.send(f"🏓 **Pong!**\n⚡ WebSocket: `{latency}ms`\n🧠 معالجة الطلب: `{elapsed}ms`")

@tree.command(name=app_commands.locale_str("server_info", arabic="معلومات_السيرفر"),description="عرض معلومات متقدمة عن السيرفر")
async def server_info(interaction):
    if not guild_only(interaction): return await reply(interaction,"❌ هذا الأمر داخل السيرفر فقط.")
    g=interaction.guild
    humans=sum(not m.bot for m in g.members); bots=sum(m.bot for m in g.members)
    e=discord.Embed(title=f"🏰 {g.name}",description=g.description or "لا يوجد وصف",timestamp=datetime.now(timezone.utc))
    if g.icon: e.set_thumbnail(url=g.icon.url)
    e.add_field(name="👑 المالك",value=f"<@{g.owner_id}>",inline=True)
    e.add_field(name="🆔 المعرّف",value=f"`{g.id}`",inline=True)
    e.add_field(name="👥 الأعضاء",value=f"`{len(g.members):,}`\nبشر: `{humans:,}` • بوتات: `{bots:,}`",inline=True)
    e.add_field(name="💬 القنوات",value=f"كل: `{len(g.channels)}`\nنصية: `{len(g.text_channels)}` • صوتية: `{len(g.voice_channels)}`",inline=True)
    e.add_field(name="🎭 الرتب",value=f"`{len(g.roles)-1:,}`",inline=True)
    e.add_field(name="😀 الإيموجي",value=f"`{len(g.emojis):,}`",inline=True)
    e.add_field(name="🚀 التعزيز",value=f"المستوى `{g.premium_tier}` • التعزيزات `{g.premium_subscription_count or 0}`",inline=True)
    e.add_field(name="📅 الإنشاء",value=f"<t:{int(g.created_at.timestamp())}:F>",inline=True)
    await reply(interaction,embed=e)

@tree.command(name=app_commands.locale_str("member_info", arabic="معلومات_عضو"),description="عرض معلومات عضو بالتفصيل")
@app_commands.describe(member="العضو")
async def member_info(interaction,member:discord.Member=None):
    member=member or interaction.user
    roles=[r.mention for r in reversed(member.roles[1:])][:15]
    e=discord.Embed(title=f"👤 {member}",description=f"{member.mention}\n`{member.id}`")
    e.set_thumbnail(url=member.display_avatar.url)
    e.add_field(name="🏷️ اللقب",value=member.nick or "بدون لقب",inline=True)
    e.add_field(name="🎭 أعلى رتبة",value=member.top_role.mention,inline=True)
    e.add_field(name="🤖 بوت؟",value="نعم" if member.bot else "لا",inline=True)
    e.add_field(name="📥 انضم",value=f"<t:{int(member.joined_at.timestamp())}:R>" if member.joined_at else "غير معروف",inline=True)
    e.add_field(name="🗓️ الحساب",value=f"<t:{int(member.created_at.timestamp())}:R>",inline=True)
    e.add_field(name="🎨 الرتب",value=" ".join(roles) if roles else "لا توجد",inline=False)
    await reply(interaction,embed=e)

@tree.command(name=app_commands.locale_str("avatar", arabic="الصورة"),description="عرض صورة عضو بجودة عالية")
@app_commands.describe(member="العضو")
async def avatar(interaction,member:discord.Member=None):
    member=member or interaction.user
    e=discord.Embed(title=f"🖼️ صورة {member.display_name}")
    e.set_image(url=member.display_avatar.replace(size=1024).url)
    await reply(interaction,embed=e)

@tree.command(name=app_commands.locale_str("role_info", arabic="معلومات_رتبة"),description="عرض معلومات رتبة")
@app_commands.describe(role="الرتبة")
async def role_info(interaction,role:discord.Role):
    e=discord.Embed(title=f"🎭 {role.name}")
    e.add_field(name="🆔",value=f"`{role.id}`",inline=True)
    e.add_field(name="📊 المركز",value=f"`{role.position}`",inline=True)
    e.add_field(name="👥 الأعضاء",value=f"`{len(role.members):,}`",inline=True)
    e.add_field(name="🔒 إدارة؟",value="نعم" if role.managed else "لا",inline=True)
    e.add_field(name="🎨 اللون",value=str(role.color),inline=True)
    await reply(interaction,embed=e)

@tree.command(name=app_commands.locale_str("purge", arabic="تنظيف"),description="حذف عدد من الرسائل")
@app_commands.checks.has_permissions(manage_messages=True)
@app_commands.describe(amount="من 1 إلى 100")
async def purge(interaction,amount:app_commands.Range[int,1,100]):
    if not interaction.channel or not hasattr(interaction.channel,"purge"): return await reply(interaction,"❌ هذه القناة لا تدعم الحذف.")
    await interaction.response.defer(ephemeral=True)
    deleted=await interaction.channel.purge(limit=amount)
    await interaction.followup.send(f"🧹 تم حذف **{len(deleted)}** رسالة.",ephemeral=True)
    await send_log(interaction.guild,"🧹 تنظيف رسائل",f"{interaction.user.mention} حذف {len(deleted)} رسالة في {interaction.channel.mention}.")

@tree.command(name=app_commands.locale_str("purge_bots", arabic="تنظيف_البوتات"),description="حذف رسائل البوتات الأخيرة")
@app_commands.checks.has_permissions(manage_messages=True)
@app_commands.describe(amount="عدد الرسائل التي سيتم فحصها")
async def purge_bots(interaction,amount:app_commands.Range[int,10,200]):
    await interaction.response.defer(ephemeral=True)
    deleted=await interaction.channel.purge(limit=amount,check=lambda m:m.author.bot)
    await interaction.followup.send(f"🤖 تم حذف **{len(deleted)}** رسالة من البوتات.",ephemeral=True)

@tree.command(name=app_commands.locale_str("slowmode", arabic="تبطيء"),description="تعيين Slowmode للقناة")
@app_commands.checks.has_permissions(manage_channels=True)
@app_commands.describe(seconds="0 لإلغاء التبطيء، الحد الأقصى 21600")
async def slowmode(interaction,seconds:app_commands.Range[int,0,21600]):
    await interaction.channel.edit(slowmode_delay=seconds,reason=f"بواسطة {interaction.user}")
    await reply(interaction,f"🐢 تم ضبط Slowmode إلى **{seconds} ثانية**.")

@tree.command(name=app_commands.locale_str("lock", arabic="قفل"),description="قفل القناة أمام الأعضاء")
@app_commands.checks.has_permissions(manage_channels=True)
async def lock(interaction):
    overwrite=interaction.channel.overwrites_for(interaction.guild.default_role); overwrite.send_messages=False
    await interaction.channel.set_permissions(interaction.guild.default_role,overwrite=overwrite,reason=f"قفل بواسطة {interaction.user}")
    await reply(interaction,"🔒 تم قفل القناة.")

@tree.command(name=app_commands.locale_str("unlock", arabic="فتح"),description="فتح القناة أمام الأعضاء")
@app_commands.checks.has_permissions(manage_channels=True)
async def unlock(interaction):
    overwrite=interaction.channel.overwrites_for(interaction.guild.default_role); overwrite.send_messages=None
    await interaction.channel.set_permissions(interaction.guild.default_role,overwrite=overwrite,reason=f"فتح بواسطة {interaction.user}")
    await reply(interaction,"🔓 تم فتح القناة.")

@tree.command(name=app_commands.locale_str("kick", arabic="طرد"),description="طرد عضو مع تسجيل الحالة")
@app_commands.checks.has_permissions(kick_members=True)
@app_commands.describe(member="العضو", reason="السبب")
async def kick(interaction,member:discord.Member,reason:str="بدون سبب"):
    if not await ensure_me(interaction,member,"طرد العضو"): return
    await member.kick(reason=f"{interaction.user}: {reason}")
    add_case(interaction.guild.id,member.id,interaction.user.id,"kick",reason)
    await reply(interaction,f"👢 تم طرد {member.mention}\n📝 السبب: {reason}")
    await send_log(interaction.guild,"👢 طرد",f"العضو: {member.mention}\nالمنفذ: {interaction.user.mention}\nالسبب: {reason}")

@tree.command(name=app_commands.locale_str("ban", arabic="حظر"),description="حظر عضو")
@app_commands.checks.has_permissions(ban_members=True)
@app_commands.describe(member="العضو", reason="السبب", delete_days="حذف رسائل آخر كم يوم")
async def ban(interaction,member:discord.Member,reason:str="بدون سبب",delete_days:app_commands.Range[int,0,7]=0):
    if not await ensure_me(interaction,member,"حظر العضو"): return
    await member.ban(reason=f"{interaction.user}: {reason}",delete_message_days=delete_days)
    add_case(interaction.guild.id,member.id,interaction.user.id,"ban",reason,delete_days*86400)
    await reply(interaction,f"🔨 تم حظر {member.mention}\n📝 {reason}")
    await send_log(interaction.guild,"🔨 حظر",f"{member} (`{member.id}`)\nالمنفذ: {interaction.user.mention}\nالسبب: {reason}")

@tree.command(name=app_commands.locale_str("unban", arabic="فك_حظر"),description="فك حظر مستخدم بالمعرف")
@app_commands.checks.has_permissions(ban_members=True)
async def unban(interaction,user_id:str):
    try: uid=int(user_id)
    except ValueError: return await reply(interaction,"❌ المعرّف غير صالح.")
    await interaction.guild.unban(discord.Object(id=uid),reason=f"بواسطة {interaction.user}")
    await reply(interaction,f"🔓 تم فك الحظر عن `{uid}`.")

@tree.command(name=app_commands.locale_str("cases", arabic="مخالفات"),description="عرض سجل العقوبات لعضو")
@app_commands.checks.has_permissions(moderate_members=True)
@app_commands.describe(member="العضو")
async def cases(interaction,member:discord.Member):
    rows=db_exec("SELECT * FROM moderation_cases WHERE guild_id=? AND user_id=? ORDER BY id DESC LIMIT 15",(interaction.guild.id,member.id),True)
    if not rows: return await reply(interaction,"📋 لا توجد مخالفات مسجلة.")
    lines=[f"**#{r['id']}** `{r['action']}` • <@{r['moderator_id']}> • {r['reason']} • <t:{int(datetime.fromisoformat(r['created_at']).timestamp())}:R>" for r in rows]
    await reply(interaction,"📋 **سجل المخالفات**\n"+"\n".join(lines))

@tree.command(name=app_commands.locale_str("note", arabic="ملاحظة"),description="إضافة ملاحظة إدارية سرية على عضو")
@app_commands.checks.has_permissions(moderate_members=True)
async def note(interaction,member:discord.Member,note:str):
    db_exec("INSERT INTO user_notes(guild_id,user_id,author_id,note,created_at) VALUES(?,?,?,?,?)",(interaction.guild.id,member.id,interaction.user.id,note,now_iso()))
    await reply(interaction,"📝 تمت إضافة الملاحظة إلى ملف العضو.",ephemeral=True)

@tree.command(name=app_commands.locale_str("notes", arabic="ملاحظات"),description="عرض الملاحظات الإدارية لعضو")
@app_commands.checks.has_permissions(moderate_members=True)
async def notes(interaction,member:discord.Member):
    rows=db_exec("SELECT * FROM user_notes WHERE guild_id=? AND user_id=? ORDER BY id DESC LIMIT 15",(interaction.guild.id,member.id),True)
    if not rows: return await reply(interaction,"📝 لا توجد ملاحظات.",ephemeral=True)
    await reply(interaction,"📝 **ملاحظات إدارية**\n"+"\n".join(f"#{r['id']} • <@{r['author_id']}> • {r['note']}" for r in rows),ephemeral=True)

@tree.command(name=app_commands.locale_str("afk", arabic="تعيين_أفك"),description="تفعيل حالة AFK")
@app_commands.describe(reason="سبب الغياب")
async def afk(interaction,reason:str="غير متوفر حاليًا"):
    db_exec("INSERT INTO afk(guild_id,user_id,reason,created_at) VALUES(?,?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET reason=excluded.reason,created_at=excluded.created_at",(interaction.guild.id,interaction.user.id,reason,now_iso()))
    await reply(interaction,f"💤 تم تفعيل AFK: **{reason}**")

@tree.command(name=app_commands.locale_str("poll", arabic="استطلاع"),description="إنشاء استطلاع تفاعلي")
async def poll(interaction,question:str,options:str):
    opts=[x.strip() for x in options.split("|") if x.strip()][:10]
    if len(opts)<2: return await reply(interaction,"❌ اكتب خيارين على الأقل وافصل بينها بالرمز `|`.")
    e=discord.Embed(title="📊 استطلاع جديد",description=f"**{question}**",timestamp=datetime.now(timezone.utc))
    emojis=["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    e.add_field(name="الخيارات",value="\n".join(f"{emojis[i]} {o}" for i,o in enumerate(opts)),inline=False)
    await interaction.response.send_message(embed=e)
    msg=await interaction.original_response()
    for i in range(len(opts)): await msg.add_reaction(emojis[i])
    db_exec("INSERT INTO polls(guild_id,channel_id,message_id,question,options,created_at) VALUES(?,?,?,?,?,?)",(interaction.guild.id,interaction.channel.id,msg.id,question,"|".join(opts),now_iso()))

@tree.command(name=app_commands.locale_str("reminder", arabic="تذكير"),description="إنشاء تذكير بعد مدة مثل 30m أو 2h أو 1d")
async def reminder(interaction,duration:str,text:str):
    delta=parse_duration(duration, max_days=30)
    if not delta: return await reply(interaction,"❌ المدة غير صالحة. استخدم `30m` أو `2h` أو `1d` وبحد أقصى 30 يومًا.")
    seconds=int(delta.total_seconds())
    when=datetime.now(timezone.utc)+delta
    db_exec("INSERT INTO reminders(user_id,channel_id,remind_at,text) VALUES(?,?,?,?)",(interaction.user.id,interaction.channel.id,when.isoformat(),text))
    await reply(interaction,f"⏰ تم ضبط التذكير بعد **{human_seconds(seconds)}**.")

@tree.command(name=app_commands.locale_str("automod", arabic="إدارة_الأوتومود"),description="تشغيل أو إيقاف حماية الروابط والدعوات والسبام")
@app_commands.checks.has_permissions(manage_guild=True)
async def automod(interaction,feature:str,enabled:bool):
    allowed={"links":"links","invites":"invites","spam":"spam","bad_words":"bad_words"}
    key=allowed.get(feature.lower())
    if not key: return await reply(interaction,"❌ المزايا: `links` أو `invites` أو `spam` أو `bad_words`.")
    db_exec(f"INSERT INTO automod(guild_id,{key}) VALUES(?,?) ON CONFLICT(guild_id) DO UPDATE SET {key}=excluded.{key}",(interaction.guild.id,int(enabled)))
    await reply(interaction,f"🛡️ تم {'تفعيل' if enabled else 'تعطيل'} حماية `{feature}`.")

@tree.command(name=app_commands.locale_str("badword_add", arabic="كلمة_ممنوعة"),description="إضافة كلمة إلى فلتر الكلمات")
@app_commands.checks.has_permissions(manage_guild=True)
async def badword_add(interaction,word:str):
    word=word.strip().lower()
    if not word or len(word)>100: return await reply(interaction,"❌ كلمة غير صالحة.")
    db_exec("INSERT OR IGNORE INTO bad_words(guild_id,word) VALUES(?,?)",(interaction.guild.id,word))
    db_exec("UPDATE automod SET bad_words=1 WHERE guild_id=?",(interaction.guild.id,))
    await reply(interaction,"🚫 تمت إضافة الكلمة إلى الفلتر.",ephemeral=True)

@tree.command(name=app_commands.locale_str("badword_list", arabic="كلمات_ممنوعة"),description="عرض الكلمات الممنوعة")
@app_commands.checks.has_permissions(manage_guild=True)
async def badword_list(interaction):
    rows=db_exec("SELECT word FROM bad_words WHERE guild_id=? ORDER BY word",(interaction.guild.id,),True)
    await reply(interaction,"🚫 **الكلمات:**\n"+("، ".join(f"`{r['word']}`" for r in rows) if rows else "لا توجد كلمات."),ephemeral=True)

@tree.command(name=app_commands.locale_str("level_reward", arabic="جائزة_مستوى"),description="ربط رتبة بمستوى XP")
@app_commands.checks.has_permissions(manage_roles=True)
async def level_reward(interaction,level:app_commands.Range[int,1,100],role:discord.Role):
    if role >= interaction.guild.me.top_role: return await reply(interaction,"❌ يجب أن تكون الرتبة أسفل أعلى رتبة للبوت.")
    db_exec("INSERT INTO role_rewards(guild_id,level,role_id) VALUES(?,?,?) ON CONFLICT(guild_id,level) DO UPDATE SET role_id=excluded.role_id",(interaction.guild.id,level,role.id))
    await reply(interaction,f"🏆 عند الوصول للمستوى **{level}** سيتم منح {role.mention}.")

@tree.command(name=app_commands.locale_str("level_rewards", arabic="جوائز_المستويات"),description="عرض جوائز مستويات XP")
async def level_rewards(interaction):
    rows=db_exec("SELECT level,role_id FROM role_rewards WHERE guild_id=? ORDER BY level",(interaction.guild.id,),True)
    await reply(interaction,"🏆 **جوائز المستويات**\n"+("\n".join(f"المستوى **{r['level']}** → <@&{r['role_id']}>" for r in rows) if rows else "لا توجد جوائز."))

@tree.command(name=app_commands.locale_str("salary", arabic="راتب"),description="استلام راتب يومي متقدم")
async def salary(interaction):
    if not guild_only(interaction): return await reply(interaction,"❌ داخل السيرفر فقط.")
    base=500+random.randint(0,500); role_bonus=sum(max(0,r.position)*20 for r in interaction.user.roles[1:])
    amount=base+min(role_bonus,5000)
    row=db_exec("SELECT claimed_at FROM daily_claims WHERE guild_id=? AND user_id=?",(interaction.guild.id,interaction.user.id),True)
    if row:
        elapsed=(datetime.now(timezone.utc)-datetime.fromisoformat(row[0]["claimed_at"])).total_seconds()
        if elapsed<86400: return await reply(interaction,f"⏳ الراتب القادم بعد **{human_seconds(86400-elapsed)}**.")
    db_exec("INSERT INTO daily_claims(guild_id,user_id,claimed_at) VALUES(?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET claimed_at=excluded.claimed_at",(interaction.guild.id,interaction.user.id,now_iso()))
    db_exec("INSERT INTO economy(guild_id,user_id,balance) VALUES(?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET balance=balance+excluded.balance",(interaction.guild.id,interaction.user.id,amount))
    db_exec("INSERT INTO transactions(guild_id,user_id,kind,amount,description,created_at) VALUES(?,?,?,?,?,?)",(interaction.guild.id,interaction.user.id,"salary",amount,"راتب يومي",now_iso()))
    await reply(interaction,f"💵 استلمت **{amount:,}** عملة كراتب.")

@tree.command(name=app_commands.locale_str("safe_transfer", arabic="تحويل_آمن"),description="تحويل عملات بمعاملة ذرية")
async def safe_transfer(interaction,member:discord.Member,amount:app_commands.Range[int,1,10_000_000]):
    if member.bot or member.id==interaction.user.id: return await reply(interaction,"❌ لا يمكنك التحويل لهذا الحساب.")
    def tx():
        a=db.execute("SELECT balance FROM economy WHERE guild_id=? AND user_id=?",(interaction.guild.id,interaction.user.id)).fetchone()
        b=db.execute("SELECT balance FROM economy WHERE guild_id=? AND user_id=?",(interaction.guild.id,member.id)).fetchone()
        ab=a["balance"] if a else 1000; bb=b["balance"] if b else 1000
        if ab<amount: return False,ab
        db.execute("INSERT INTO economy(guild_id,user_id,balance) VALUES(?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET balance=excluded.balance",(interaction.guild.id,interaction.user.id,ab-amount))
        db.execute("INSERT INTO economy(guild_id,user_id,balance) VALUES(?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET balance=excluded.balance",(interaction.guild.id,member.id,bb+amount))
        now=now_iso()
        db.execute("INSERT INTO transactions(guild_id,user_id,kind,amount,description,created_at) VALUES(?,?,?,?,?,?)",(interaction.guild.id,interaction.user.id,"transfer",-amount,f"إلى {member.id}",now))
        db.execute("INSERT INTO transactions(guild_id,user_id,kind,amount,description,created_at) VALUES(?,?,?,?,?,?)",(interaction.guild.id,member.id,"transfer",amount,f"من {interaction.user.id}",now))
        return True,ab-amount
    ok,balance=db_transaction(tx)
    if not ok: return await reply(interaction,f"❌ رصيدك غير كافٍ. رصيدك: **{balance:,}**")
    await reply(interaction,f"💸 تم تحويل **{amount:,}** إلى {member.mention}.\n💰 رصيدك: **{balance:,}**")

@tree.command(name=app_commands.locale_str("money_log", arabic="سجل_مالي"),description="عرض آخر العمليات المالية")
async def money_log(interaction):
    rows=db_exec("SELECT kind,amount,description,created_at FROM transactions WHERE guild_id=? AND user_id=? ORDER BY id DESC LIMIT 15",(interaction.guild.id,interaction.user.id),True)
    if not rows: return await reply(interaction,"💳 لا توجد عمليات.")
    lines=[]
    for r in rows:
        sign="+" if r["amount"]>0 else ""
        lines.append(f"`{r['kind']}` **{sign}{r['amount']:,}** — {r['description']}")
    await reply(interaction,"💳 **آخر العمليات**\n"+"\n".join(lines))

@tree.command(name=app_commands.locale_str("top_levels", arabic="توب_مستويات"),description="أعلى أعضاء XP")
async def top_levels(interaction):
    rows=db_exec("SELECT user_id,xp,level FROM levels WHERE guild_id=? ORDER BY level DESC,xp DESC LIMIT 15",(interaction.guild.id,),True)
    if not rows: return await reply(interaction,"⭐ لا توجد بيانات.")
    await reply(interaction,"🏆 **أفضل 15 عضوًا في المستويات**\n"+"\n".join(f"**{i}.** <@{r['user_id']}> — مستوى **{r['level']}** • `{r['xp']:,} XP`" for i,r in enumerate(rows,1)))

@tree.command(name=app_commands.locale_str("bot_stats", arabic="إحصائيات_البوت"),description="إحصائيات تشغيل البوت")
async def bot_stats(interaction):
    e=discord.Embed(title=f"📊 {BOT_NAME} — الإحصائيات")
    e.add_field(name="🏰 السيرفرات",value=f"`{len(bot.guilds):,}`",inline=True)
    e.add_field(name="👥 المستخدمون",value=f"`{sum(g.member_count or 0 for g in bot.guilds):,}`",inline=True)
    e.add_field(name="📡 Ping",value=f"`{round(bot.latency*1000)}ms`",inline=True)
    e.add_field(name="⚙️ أوامر",value=f"`{len(tree.get_commands()):,}`",inline=True)
    e.add_field(name="🐍 Python",value=f"`{__import__('sys').version.split()[0]}`",inline=True)
    e.add_field(name="📚 discord.py",value=f"`{discord.__version__}`",inline=True)
    await reply(interaction,embed=e)

@tasks.loop(seconds=20)
async def reminder_worker():
    now=datetime.now(timezone.utc).isoformat()
    rows=db_exec("SELECT * FROM reminders WHERE sent=0 AND remind_at<=? ORDER BY id LIMIT 25",(now,),True)
    for r in rows:
        try:
            user=bot.get_user(r["user_id"]) or await bot.fetch_user(r["user_id"])
            channel=bot.get_channel(r["channel_id"]) if r["channel_id"] else None
            target=channel if channel and hasattr(channel,"send") else user
            await target.send(f"⏰ **تذكيرك:** {r['text']}")
        except Exception: pass
        db_exec("UPDATE reminders SET sent=1 WHERE id=?",(r["id"],))

@tasks.loop(seconds=45)
async def automod_cleanup_worker():
    # Keep the in-memory XP cache bounded on large servers.
    if len(XP_CACHE)>10000:
        cutoff=time.monotonic()-3600
        for key,val in list(XP_CACHE.items()):
            if val<cutoff: XP_CACHE.pop(key,None)

@tasks.loop(minutes=10)
async def health_worker():
    try:
        db.execute("PRAGMA wal_checkpoint(PASSIVE)")
    except Exception: pass

def _has_url(content): return bool(re.search(r"https?://\S+|www\.\S+",content,re.I))
def _has_invite(content): return bool(re.search(r"(discord\.gg/|discord\.com/invite/)",content,re.I))

# ============================================================
# Message processing pipeline.
# ============================================================
async def process_extended_message(message):
    if message.author.bot or not message.guild: return False
    await core_track_message(message)
    cfg=get_auto(message.guild.id)
    content=message.content or ""
    # Remove AFK automatically when the owner speaks.
    if db.execute("SELECT 1 FROM afk WHERE guild_id=? AND user_id=?",(message.guild.id,message.author.id)).fetchone():
        db_exec("DELETE FROM afk WHERE guild_id=? AND user_id=?",(message.guild.id,message.author.id))
        try: await message.channel.send(f"👋 {message.author.mention} تم إلغاء حالة AFK.",delete_after=5)
        except Exception: pass
    # Announce AFK mentions.
    mentioned={m.id for m in message.mentions if m.id!=message.author.id}
    for uid in mentioned:
        row=db.execute("SELECT reason,created_at FROM afk WHERE guild_id=? AND user_id=?",(message.guild.id,uid)).fetchone()
        if row:
            try: await message.channel.send(f"💤 <@{uid}> في وضع AFK: **{row['reason']}**",delete_after=8)
            except Exception: pass
    if content:
        blocked=False; reason=""
        if cfg["invites"] and _has_invite(content): blocked=True; reason="روابط دعوات Discord"
        elif cfg["links"] and _has_url(content): blocked=True; reason="روابط"
        elif cfg["bad_words"]:
            words=[r["word"] for r in db.execute("SELECT word FROM bad_words WHERE guild_id=?",(message.guild.id,)).fetchall()]
            if any(w and re.search(rf"(?<!\w){re.escape(w)}(?!\w)",content,re.I) for w in words):
                blocked=True; reason="كلمة ممنوعة"
        if blocked and not message.author.guild_permissions.manage_messages:
            try:
                await message.delete()
                await message.channel.send(f"🛡️ {message.author.mention} تم حذف رسالتك بسبب: **{reason}**.",delete_after=6)
                add_case(message.guild.id,message.author.id,message.guild.me.id,"automod",reason)
            except Exception: pass
            return True
    return False

# Start new background workers from setup_hook through a safe helper.
async def start_extended_workers():
    for task_obj in (reminder_worker,automod_cleanup_worker,health_worker):
        try:
            if not task_obj.is_running(): task_obj.start()
        except RuntimeError: pass


async def sync_commands_to_guilds():
    for guild in bot.guilds:
        try:
            bot.tree.copy_global_to(guild=guild)
            await bot.tree.sync(guild=guild)
            print(f"✅ Slash commands synced to: {guild.name} ({guild.id})")
        except Exception as e:
            print(f"❌ Failed to sync commands to {guild.id}: {e}")

@bot.event
async def on_member_join(member):
    cfg=get_config(member.guild.id)
    if cfg["auto_role"]:
        role=member.guild.get_role(cfg["auto_role"])
        if role and role < member.guild.me.top_role:
            try: await member.add_roles(role,reason="Auto role")
            except Exception: pass
    if cfg["welcome_channel"]:
        ch=member.guild.get_channel(cfg["welcome_channel"])
        if ch:
            try: await ch.send(f"👋 أهلاً وسهلاً {member.mention}! نورت السيرفر **{member.guild.name}** 🌟")
            except Exception: pass

@bot.event
async def on_member_remove(member): await send_log(member.guild,"📤 Member Left",f"{member} غادر السيرفر.")

@bot.event
async def on_message(message):
    if message.author.bot or not message.guild: return
    try:
        await process_extended_message(message)
    except Exception:
        pass
    try:
        await v4_message_observer(message)
    except Exception as exc:
        print("[V4 observer]", type(exc).__name__, exc)
    cfg=get_config(message.guild.id)
    if cfg["xp_enabled"]:
        key=(message.guild.id,message.author.id); now=time.monotonic(); last=XP_CACHE.get(key,0)
        if now-last>=45:
            XP_CACHE[key]=now; _,level,gained=add_xp(message.guild.id,message.author.id,random.randint(10,20))
            if gained:
                try:
                    await message.channel.send(f"🎉 مبروك {message.author.mention}! وصلت إلى **المستوى {level}** ⭐")
                    reward=db.execute("SELECT role_id FROM role_rewards WHERE guild_id=? AND level=?",(message.guild.id,level)).fetchone()
                    if reward:
                        role=message.guild.get_role(reward["role_id"])
                        me=bot_member(message.guild)
                        if role and me and role < me.top_role and role not in message.author.roles:
                            await message.author.add_roles(role,reason=f"مكافأة مستوى {level}")
                            await message.channel.send(f"🏆 تم منح {role.mention} لـ {message.author.mention} كمكافأة للمستوى **{level}**.",delete_after=10)
                except Exception: pass
    await bot.process_commands(message)

@bot.event
async def on_ready():
    if not getattr(bot,"_synced",False):
        try: await tree.sync(); bot._synced=True
        except Exception as e: print("Slash sync error:",e)
    print(f"✅ {BOT_NAME} يعمل باسم {bot.user} ({bot.user.id})")

@bot.event
async def setup_hook():
    await bot.tree.set_translator(ArabicTranslator())
    """Single startup hook for all bot systems; no version monkey-patching."""
    bot.add_view(TicketView())
    bot.add_view(TicketCloseView())
    # Restore every active suggestion button after a restart.
    for row in db.execute("SELECT id FROM suggestions WHERE status='pending'").fetchall():
        sid = row["id"]
        counts = db.execute(
            "SELECT vote,COUNT(*) c FROM suggestion_votes WHERE suggestion_id=? GROUP BY vote",
            (sid,),
        ).fetchall()
        yes = next((r["c"] for r in counts if r["vote"] == 1), 0)
        no = next((r["c"] for r in counts if r["vote"] == 0), 0)
        bot.add_view(SuggestionView(sid, yes, no))

    for worker in (market_fluctuation, database_backup):
        if not worker.is_running():
            worker.start()
    await start_extended_workers()
    await v4_bootstrap()
    v4_warm_guild_cache()
    if V4_SELF_PING_URL and not v4_self_ping_loop.is_running():
        v4_self_ping_loop.start()

@tree.error
async def on_app_command_error(interaction,error):
    original=error.original if isinstance(error,app_commands.CommandInvokeError) else error
    if isinstance(original,app_commands.MissingPermissions): return await reply(interaction,"❌ لا تملك الصلاحية المطلوبة لهذا الأمر.")
    if isinstance(original,discord.Forbidden): return await reply(interaction,"❌ Discord رفض العملية. تحقق من صلاحيات البوت وترتيب الرتب.")
    if isinstance(original,app_commands.TransformerError): return await reply(interaction,"❌ أحد المدخلات غير صالح.")
    print(f"[ERROR] command={getattr(getattr(interaction, 'command', None), 'qualified_name', 'unknown')} type={type(original).__name__}: {original}")
    try:
        await send_log(interaction.guild, "🚨 Command Error", f"الأمر: `{getattr(getattr(interaction, 'command', None), 'qualified_name', 'unknown')}`\nالخطأ: `{type(original).__name__}: {original}`") if interaction.guild else None
    except Exception: pass
    await reply(interaction,"❌ حدث خطأ غير متوقع. تم تسجيل الخطأ.")


# ================================================================
# V4 AR — طبقة الأنظمة الاحترافية العربية
# ================================================================
# الهدف: جعل البوت كبيرًا من ناحية الأنظمة الفعلية، وليس حشو أسطر.
# جميع الأنظمة التالية تعمل داخل نفس البوت ونفس قاعدة البيانات.
# ================================================================

AR = {
    "ok": "تمت العملية بنجاح.",
    "error": "حدث خطأ غير متوقع.",
    "denied": "لا تملك الصلاحية المطلوبة.",
    "not_found": "لم يتم العثور على المطلوب.",
    "disabled": "هذا النظام معطل في هذا السيرفر.",
    "enabled": "تم تفعيل النظام.",
    "disabled_done": "تم تعطيل النظام.",
    "database": "قاعدة البيانات",
    "security": "الأمان",
    "economy": "الاقتصاد",
    "moderation": "الإدارة",
    "activity": "النشاط",
    "system": "النظام",
}

# ----------------------------------------------------------------
# قاعدة بيانات V4
# ----------------------------------------------------------------
ENTERPRISE_SCHEMA = """
CREATE TABLE IF NOT EXISTS enterprise_settings(
    guild_id INTEGER PRIMARY KEY,
    language TEXT NOT NULL DEFAULT 'ar',
    timezone TEXT NOT NULL DEFAULT 'Asia/Riyadh',
    prefix TEXT NOT NULL DEFAULT '!',
    maintenance INTEGER NOT NULL DEFAULT 0,
    dashboard_enabled INTEGER NOT NULL DEFAULT 1,
    audit_enabled INTEGER NOT NULL DEFAULT 1,
    anti_raid INTEGER NOT NULL DEFAULT 0,
    anti_mention INTEGER NOT NULL DEFAULT 0,
    max_mentions INTEGER NOT NULL DEFAULT 5,
    max_messages INTEGER NOT NULL DEFAULT 8,
    message_window INTEGER NOT NULL DEFAULT 8,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS enterprise_audit(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER NOT NULL,
    actor_id INTEGER NOT NULL,
    action TEXT NOT NULL,
    target_id INTEGER DEFAULT 0,
    details TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS enterprise_cooldowns(
    guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    bucket TEXT NOT NULL,
    used_at REAL NOT NULL,
    PRIMARY KEY(guild_id,user_id,bucket)
);

CREATE TABLE IF NOT EXISTS enterprise_antispam(
    guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    message_times TEXT NOT NULL DEFAULT '[]',
    strikes INTEGER NOT NULL DEFAULT 0,
    muted_until REAL NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(guild_id,user_id)
);

CREATE TABLE IF NOT EXISTS enterprise_automod_actions(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    rule TEXT NOT NULL,
    action TEXT NOT NULL,
    content_hash TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS enterprise_welcome(
    guild_id INTEGER PRIMARY KEY,
    channel_id INTEGER,
    enabled INTEGER NOT NULL DEFAULT 0,
    message TEXT NOT NULL DEFAULT 'أهلًا {member} في {server} 🌟',
    dm_enabled INTEGER NOT NULL DEFAULT 0,
    dm_message TEXT NOT NULL DEFAULT 'أهلًا بك في {server}!',
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS enterprise_leave(
    guild_id INTEGER PRIMARY KEY,
    channel_id INTEGER,
    enabled INTEGER NOT NULL DEFAULT 0,
    message TEXT NOT NULL DEFAULT 'غادر {member} السيرفر.',
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS enterprise_autorole(
    guild_id INTEGER PRIMARY KEY,
    role_id INTEGER,
    enabled INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS enterprise_reaction_roles(
    guild_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    emoji TEXT NOT NULL,
    role_id INTEGER NOT NULL,
    PRIMARY KEY(guild_id,message_id,emoji)
);

CREATE TABLE IF NOT EXISTS enterprise_starboard(
    guild_id INTEGER PRIMARY KEY,
    channel_id INTEGER,
    emoji TEXT NOT NULL DEFAULT '⭐',
    threshold INTEGER NOT NULL DEFAULT 5,
    enabled INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS enterprise_star_messages(
    guild_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    star_message_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    stars INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(guild_id,message_id)
);

CREATE TABLE IF NOT EXISTS enterprise_giveaways(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    prize TEXT NOT NULL,
    winners INTEGER NOT NULL DEFAULT 1,
    ends_at TEXT NOT NULL,
    ended INTEGER NOT NULL DEFAULT 0,
    created_by INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS enterprise_giveaway_entries(
    giveaway_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    PRIMARY KEY(giveaway_id,user_id)
);

CREATE TABLE IF NOT EXISTS enterprise_custom_roles(
    guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    role_id INTEGER NOT NULL,
    expires_at TEXT,
    PRIMARY KEY(guild_id,user_id)
);

CREATE TABLE IF NOT EXISTS enterprise_temp_channels(
    guild_id INTEGER NOT NULL,
    channel_id INTEGER PRIMARY KEY,
    owner_id INTEGER NOT NULL,
    expires_at TEXT
);

CREATE TABLE IF NOT EXISTS enterprise_commands_log(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER,
    user_id INTEGER,
    command_name TEXT NOT NULL,
    duration_ms REAL NOT NULL,
    success INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS enterprise_backups(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER NOT NULL,
    filename TEXT NOT NULL,
    size INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS enterprise_votes(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER NOT NULL,
    topic TEXT NOT NULL,
    yes INTEGER NOT NULL DEFAULT 0,
    no INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    closed INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS enterprise_user_flags(
    guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    flag TEXT NOT NULL,
    value TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL,
    PRIMARY KEY(guild_id,user_id,flag)
);

CREATE TABLE IF NOT EXISTS enterprise_api_cache(
    cache_key TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    expires_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS enterprise_metrics(
    guild_id INTEGER NOT NULL,
    metric TEXT NOT NULL,
    value INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(guild_id,metric)
);
"""

try:
    with DB_SYNC_LOCK:
        db.executescript(ENTERPRISE_SCHEMA)
        db.commit()
except Exception as _schema_error:
    print("[V4] schema error:", _schema_error)

# ----------------------------------------------------------------
# أدوات قاعدة البيانات الآمنة
# ----------------------------------------------------------------
def v4_now():
    return datetime.now(timezone.utc).isoformat()

def v4_db_execute(sql, params=(), commit=False):
    with DB_SYNC_LOCK:
        cur = db.execute(sql, params)
        if commit:
            db.commit()
        return cur

def v4_db_many(sql, params=()):
    with DB_SYNC_LOCK:
        return db.execute(sql, params).fetchall()

def v4_db_one(sql, params=()):
    with DB_SYNC_LOCK:
        return db.execute(sql, params).fetchone()

def v4_db_script(sql):
    with DB_SYNC_LOCK:
        db.executescript(sql)
        db.commit()

def v4_set_metric(guild_id, metric, amount=1):
    with DB_SYNC_LOCK:
        db.execute(
            """INSERT INTO enterprise_metrics(guild_id,metric,value,updated_at)
               VALUES(?,?,?,?)
               ON CONFLICT(guild_id,metric)
               DO UPDATE SET value=value+excluded.value,updated_at=excluded.updated_at""",
            (guild_id, metric, amount, v4_now())
        )
        db.commit()

def v4_get_metric(guild_id, metric):
    row = v4_db_one(
        "SELECT value FROM enterprise_metrics WHERE guild_id=? AND metric=?",
        (guild_id, metric)
    )
    return int(row["value"]) if row else 0

def v4_audit(guild_id, actor_id, action, target_id=0, details=""):
    if not guild_id:
        return
    v4_db_execute(
        """INSERT INTO enterprise_audit(guild_id,actor_id,action,target_id,details,created_at)
           VALUES(?,?,?,?,?,?)""",
        (guild_id, actor_id or 0, action[:120], target_id or 0, details[:1800], v4_now()),
        commit=True
    )
    v4_set_metric(guild_id, "audit_events", 1)

def v4_flag(guild_id, user_id, flag, value=""):
    v4_db_execute(
        """INSERT INTO enterprise_user_flags(guild_id,user_id,flag,value,updated_at)
           VALUES(?,?,?,?,?)
           ON CONFLICT(guild_id,user_id,flag)
           DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at""",
        (guild_id, user_id, flag, str(value), v4_now()),
        commit=True
    )

def v4_get_flag(guild_id, user_id, flag, default=None):
    row = v4_db_one(
        "SELECT value FROM enterprise_user_flags WHERE guild_id=? AND user_id=? AND flag=?",
        (guild_id, user_id, flag)
    )
    return row["value"] if row else default

# ----------------------------------------------------------------
# إعدادات السيرفر
# ----------------------------------------------------------------
def v4_settings(guild_id):
    row = v4_db_one("SELECT * FROM enterprise_settings WHERE guild_id=?", (guild_id,))
    if row:
        return row
    v4_db_execute(
        """INSERT INTO enterprise_settings(guild_id,updated_at)
           VALUES(?,?)""",
        (guild_id, v4_now()),
        commit=True
    )
    return v4_db_one("SELECT * FROM enterprise_settings WHERE guild_id=?", (guild_id,))

def v4_update_settings(guild_id, **values):
    if not values:
        return
    allowed = {
        "language","timezone","prefix","maintenance","dashboard_enabled",
        "audit_enabled","anti_raid","anti_mention","max_mentions",
        "max_messages","message_window"
    }
    clean = {k:v for k,v in values.items() if k in allowed}
    if not clean:
        return
    clean["updated_at"] = v4_now()
    fields = ",".join(f"{k}=?" for k in clean)
    params = list(clean.values()) + [guild_id]
    v4_db_execute(
        f"UPDATE enterprise_settings SET {fields} WHERE guild_id=?",
        params, commit=True
    )

# ----------------------------------------------------------------
# نظام Rate Limit عام
# ----------------------------------------------------------------
def v4_rate_limit(guild_id, user_id, bucket, seconds):
    if not guild_id:
        return False, 0
    now = time.time()
    row = v4_db_one(
        "SELECT used_at FROM enterprise_cooldowns WHERE guild_id=? AND user_id=? AND bucket=?",
        (guild_id, user_id, bucket)
    )
    if row:
        remaining = float(seconds) - (now - float(row["used_at"]))
        if remaining > 0:
            return True, remaining
    v4_db_execute(
        """INSERT INTO enterprise_cooldowns(guild_id,user_id,bucket,used_at)
           VALUES(?,?,?,?)
           ON CONFLICT(guild_id,user_id,bucket)
           DO UPDATE SET used_at=excluded.used_at""",
        (guild_id, user_id, bucket, now), commit=True
    )
    return False, 0

# ----------------------------------------------------------------
# Embed عربي موحد
# ----------------------------------------------------------------
def v4_embed(title, description="", *, emoji="🤖", footer=True):
    e = discord.Embed(
        title=f"{emoji} {title}",
        description=description,
        timestamp=datetime.now(timezone.utc)
    )
    if footer:
        e.set_footer(text=f"{BOT_NAME} • V4 AR")
    return e

def v4_error(text):
    return v4_embed("حدث خطأ", text, emoji="❌")

def v4_success(text):
    return v4_embed("تمت العملية", text, emoji="✅")

def v4_info(text):
    return v4_embed("معلومات", text, emoji="ℹ️")

# ----------------------------------------------------------------
# تحقق صلاحيات احترافي
# ----------------------------------------------------------------
def v4_is_admin(member):
    return bool(member and (member.guild_permissions.administrator or member.id == member.guild.owner_id))

def v4_can_manage(member, target):
    if not member or not target:
        return False
    if target.id == member.guild.owner_id:
        return False
    if target.id == member.id:
        return True
    return member.top_role > target.top_role

def v4_bot_can_manage(guild, target):
    me = guild.me
    if not me or not target:
        return False
    if target.id == guild.owner_id:
        return False
    return me.top_role > target.top_role

async def v4_require_admin(interaction):
    if not interaction.guild or not v4_is_admin(interaction.user):
        await reply(interaction, "❌ هذا الأمر للإدارة فقط.")
        return False
    return True

# ----------------------------------------------------------------
# JSON / Cache / Serialization helpers
# ----------------------------------------------------------------
def v4_json_load(value, fallback):
    try:
        import json
        return json.loads(value)
    except Exception:
        return fallback

def v4_json_dump(value):
    import json
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

def v4_cache_put(key, payload, ttl=300):
    v4_db_execute(
        """INSERT INTO enterprise_api_cache(cache_key,payload,expires_at)
           VALUES(?,?,?)
           ON CONFLICT(cache_key)
           DO UPDATE SET payload=excluded.payload,expires_at=excluded.expires_at""",
        (key, v4_json_dump(payload), time.time()+ttl), commit=True
    )

def v4_cache_get(key):
    row = v4_db_one("SELECT payload,expires_at FROM enterprise_api_cache WHERE cache_key=?", (key,))
    if not row:
        return None
    if float(row["expires_at"]) < time.time():
        v4_db_execute("DELETE FROM enterprise_api_cache WHERE cache_key=?", (key,), commit=True)
        return None
    return v4_json_load(row["payload"], None)

# ----------------------------------------------------------------
# نظام مراقبة النشاط والرسائل
# ----------------------------------------------------------------
V4_MESSAGE_STATS = {}
V4_MESSAGE_STATS_LAST_SEEN = {}
V4_LAST_HEALTH = {}
V4_COMMAND_TIMES = Counter()
V4_MESSAGE_STATS_MAX_ENTRIES = int(os.getenv("V4_MESSAGE_STATS_MAX_ENTRIES", "10000"))
V4_MESSAGE_STATS_TTL = int(os.getenv("V4_MESSAGE_STATS_TTL", str(24 * 60 * 60)))

def v4_record_message(guild_id, user_id):
    if not guild_id:
        return
    key = (guild_id, user_id)
    V4_MESSAGE_STATS[key] = V4_MESSAGE_STATS.get(key, 0) + 1
    V4_MESSAGE_STATS_LAST_SEEN[key] = time.time()
    v4_set_metric(guild_id, "messages_seen", 1)

def v4_cleanup_message_stats():
    now = time.time()
    cutoff = now - V4_MESSAGE_STATS_TTL
    stale = [
        key for key, last_seen in V4_MESSAGE_STATS_LAST_SEEN.items()
        if last_seen < cutoff
    ]
    for key in stale:
        V4_MESSAGE_STATS_LAST_SEEN.pop(key, None)
        V4_MESSAGE_STATS.pop(key, None)

    overflow = len(V4_MESSAGE_STATS) - V4_MESSAGE_STATS_MAX_ENTRIES
    if overflow > 0:
        oldest = sorted(
            V4_MESSAGE_STATS_LAST_SEEN.items(),
            key=lambda item: item[1]
        )[:overflow]
        for key, _ in oldest:
            V4_MESSAGE_STATS_LAST_SEEN.pop(key, None)
            V4_MESSAGE_STATS.pop(key, None)

def v4_message_stats_housekeeping():
    v4_cleanup_message_stats()

@tasks.loop(minutes=30)
async def v4_message_stats_housekeeping_loop():
    try:
        v4_message_stats_housekeeping()
    except Exception as exc:
        print("[V4 stats cleanup]", type(exc).__name__, exc)

@v4_message_stats_housekeeping_loop.before_loop
async def before_v4_message_stats_housekeeping_loop():
    await bot.wait_until_ready()


def v4_record_command(name, duration, success=True):
    V4_COMMAND_TIMES[name] += 1

def v4_top_message_members(guild_id, limit=10):
    values = [
        (uid, count)
        for (gid, uid), count in V4_MESSAGE_STATS.items()
        if gid == guild_id
    ]
    values.sort(key=lambda x: x[1], reverse=True)
    return values[:limit]

# ----------------------------------------------------------------
# نظام Anti-Spam احترافي
# ----------------------------------------------------------------
def v4_spam_check(guild_id, user_id, limit=8, window=8):
    now = time.time()
    row = v4_db_one(
        "SELECT message_times,strikes,muted_until FROM enterprise_antispam WHERE guild_id=? AND user_id=?",
        (guild_id, user_id)
    )
    times = v4_json_load(row["message_times"], []) if row else []
    strikes = int(row["strikes"]) if row else 0
    muted_until = float(row["muted_until"]) if row else 0
    times = [float(x) for x in times if now - float(x) <= window]
    times.append(now)
    triggered = len(times) >= limit
    if triggered:
        strikes += 1
        times = times[-max(2, limit//2):]
        muted_until = now + min(300, 15 * strikes)
    v4_db_execute(
        """INSERT INTO enterprise_antispam(guild_id,user_id,message_times,strikes,muted_until,updated_at)
           VALUES(?,?,?,?,?,?)
           ON CONFLICT(guild_id,user_id)
           DO UPDATE SET message_times=excluded.message_times,strikes=excluded.strikes,
                         muted_until=excluded.muted_until,updated_at=excluded.updated_at""",
        (guild_id,user_id,v4_json_dump(times),strikes,muted_until,v4_now()),
        commit=True
    )
    return triggered, strikes, muted_until

# ----------------------------------------------------------------
# AutoMod: روابط / منشنات / كلمات
# ----------------------------------------------------------------
def v4_has_invite(content):
    lowered = content.lower()
    return "discord.gg/" in lowered or "discord.com/invite/" in lowered

def v4_has_url(content):
    lowered = content.lower()
    return "http://" in lowered or "https://" in lowered or "www." in lowered

def v4_bad_word_hit(guild_id, content):
    words = [
        str(r["word"]).lower()
        for r in v4_db_many("SELECT word FROM bad_words WHERE guild_id=?", (guild_id,))
    ]
    lowered = content.lower()
    return next((w for w in words if w and w in lowered), None)

def v4_automod_record(guild_id, user_id, rule, action, content):
    import hashlib
    digest = hashlib.sha256(content.encode("utf-8", "ignore")).hexdigest()
    v4_db_execute(
        """INSERT INTO enterprise_automod_actions(guild_id,user_id,rule,action,content_hash,created_at)
           VALUES(?,?,?,?,?,?)""",
        (guild_id,user_id,rule,action,digest,v4_now()), commit=True
    )
    v4_set_metric(guild_id, f"automod_{rule}", 1)

# ----------------------------------------------------------------
# Welcome / Leave / Auto Role
# ----------------------------------------------------------------
def v4_format_message(template, member, guild):
    replacements = {
        "{member}": getattr(member, "mention", str(member)),
        "{name}": getattr(member, "display_name", str(member)),
        "{server}": guild.name,
        "{count}": str(guild.member_count or 0),
        "{id}": str(getattr(member, "id", 0)),
    }
    result = template or ""
    for key, value in replacements.items():
        result = result.replace(key, value)
    return result

def v4_get_welcome(guild_id):
    return v4_db_one("SELECT * FROM enterprise_welcome WHERE guild_id=?", (guild_id,))

def v4_get_leave(guild_id):
    return v4_db_one("SELECT * FROM enterprise_leave WHERE guild_id=?", (guild_id,))

def v4_get_autorole(guild_id):
    return v4_db_one("SELECT * FROM enterprise_autorole WHERE guild_id=?", (guild_id,))

# ----------------------------------------------------------------
# Starboard helpers
# ----------------------------------------------------------------
def v4_star_config(guild_id):
    return v4_db_one("SELECT * FROM enterprise_starboard WHERE guild_id=?", (guild_id,))

def v4_star_count(message, emoji):
    try:
        for reaction in message.reactions:
            if str(reaction.emoji) == emoji:
                return reaction.count
    except Exception:
        pass
    return 0

# ----------------------------------------------------------------
# Giveaway helpers
# ----------------------------------------------------------------
def v4_giveaway_entries(giveaway_id):
    return [
        int(r["user_id"])
        for r in v4_db_many(
            "SELECT user_id FROM enterprise_giveaway_entries WHERE giveaway_id=?",
            (giveaway_id,)
        )
    ]

def v4_pick_winners(entries, count):
    import random as _random
    pool = list(dict.fromkeys(entries))
    if not pool:
        return []
    return _random.sample(pool, min(count, len(pool)))

# ----------------------------------------------------------------
# سجل الأوامر
# ----------------------------------------------------------------
def v4_log_command(guild_id, user_id, command_name, duration_ms, success):
    v4_db_execute(
        """INSERT INTO enterprise_commands_log(guild_id,user_id,command_name,duration_ms,success,created_at)
           VALUES(?,?,?,?,?,?)""",
        (guild_id,user_id,command_name,float(duration_ms),1 if success else 0,v4_now()),
        commit=True
    )
    if guild_id:
        v4_set_metric(guild_id, "commands_used", 1)

# ----------------------------------------------------------------
# واجهة Dashboard نصية
# ----------------------------------------------------------------
def v4_dashboard_text(guild):
    if not guild:
        return "لا يوجد سيرفر."
    settings = v4_settings(guild.id)
    return (
        f"**لوحة تحكم {guild.name}**\n"
        f"• اللغة: `{settings['language']}`\n"
        f"• المنطقة الزمنية: `{settings['timezone']}`\n"
        f"• الصيانة: `{'مفعلة' if settings['maintenance'] else 'معطلة'}`\n"
        f"• Anti-Raid: `{'مفعل' if settings['anti_raid'] else 'معطل'}`\n"
        f"• Anti-Mention: `{'مفعل' if settings['anti_mention'] else 'معطل'}`\n"
        f"• الرسائل المرصودة: `{v4_get_metric(guild.id, 'messages_seen')}`\n"
        f"• الأوامر المستخدمة: `{v4_get_metric(guild.id, 'commands_used')}`\n"
        f"• أحداث السجل: `{v4_get_metric(guild.id, 'audit_events')}`"
    )

# ----------------------------------------------------------------
# أمر Prefix عربي: لوحة
# ----------------------------------------------------------------
@bot.command(name="لوحة", aliases=["داشبورد", "لوحة_تحكم"])
@commands.guild_only()
async def v4_prefix_dashboard(ctx):
    if not v4_is_admin(ctx.author):
        return await ctx.reply("❌ هذا الأمر للإدارة فقط.")
    await ctx.reply(embed=v4_embed("لوحة التحكم", v4_dashboard_text(ctx.guild), emoji="🎛️"))

# ----------------------------------------------------------------
# أوامر Prefix عربية إضافية
# ----------------------------------------------------------------
@bot.command(name="حالة", aliases=["ستاتس"])
async def v4_prefix_status(ctx):
    latency = round(bot.latency * 1000)
    await ctx.reply(embed=v4_embed("حالة البوت", f"البنق: `{latency}ms`\nالإصدار: `{BOT_VERSION}`\nالسيرفرات: `{len(bot.guilds)}`", emoji="📡"))

@bot.command(name="إحصائيات", aliases=["احصائيات"])
async def v4_prefix_stats(ctx):
    guild = ctx.guild
    if not guild:
        return
    members = sum(1 for m in guild.members if not m.bot)
    online = sum(1 for m in guild.members if getattr(m, "status", None) != discord.Status.offline and not m.bot)
    text = (
        f"الأعضاء: `{guild.member_count}`\n"
        f"البشر: `{members}`\n"
        f"المتصلون: `{online}`\n"
        f"القنوات: `{len(guild.channels)}`\n"
        f"الرتب: `{len(guild.roles)}`\n"
        f"الإيموجيات: `{len(guild.emojis)}`"
    )
    await ctx.reply(embed=v4_embed("إحصائيات السيرفر", text, emoji="📊"))

@bot.command(name="منشن", aliases=["معلومات_منشن"])
@commands.guild_only()
async def v4_prefix_mention(ctx, member: discord.Member=None):
    member = member or ctx.author
    text = (
        f"الاسم: {member.mention}\n"
        f"المعرف: `{member.id}`\n"
        f"اللقب: `{member.display_name}`\n"
        f"الرتبة الأعلى: {member.top_role.mention}\n"
        f"دخل السيرفر: `{discord.utils.format_dt(member.joined_at, 'R') if member.joined_at else 'غير معروف'}`"
    )
    await ctx.reply(embed=v4_embed("ملف العضو", text, emoji="👤"))

@bot.command(name="مسح_قديم")
@commands.guild_only()
@commands.has_permissions(manage_messages=True)
async def v4_prefix_clean(ctx, amount: int=10):
    amount = max(1, min(100, amount))
    deleted = await ctx.channel.purge(limit=amount+1)
    await ctx.channel.send(f"🧹 تم حذف `{max(0,len(deleted)-1)}` رسالة.", delete_after=5)

@bot.command(name="قفل_سريع")
@commands.guild_only()
@commands.has_permissions(manage_channels=True)
async def v4_prefix_lock(ctx):
    overwrite = ctx.channel.overwrites_for(ctx.guild.default_role)
    overwrite.send_messages = False
    await ctx.channel.set_permissions(ctx.guild.default_role, overwrite=overwrite, reason=f"قفل سريع بواسطة {ctx.author}")
    await ctx.reply("🔒 تم قفل القناة.")

@bot.command(name="فتح_سريع")
@commands.guild_only()
@commands.has_permissions(manage_channels=True)
async def v4_prefix_unlock(ctx):
    overwrite = ctx.channel.overwrites_for(ctx.guild.default_role)
    overwrite.send_messages = None
    await ctx.channel.set_permissions(ctx.guild.default_role, overwrite=overwrite, reason=f"فتح سريع بواسطة {ctx.author}")
    await ctx.reply("🔓 تم فتح القناة.")

@bot.command(name="رتبتي")
@commands.guild_only()
async def v4_prefix_myrole(ctx):
    await ctx.reply(f"🏷️ رتبتك الأعلى: {ctx.author.top_role.mention}")

@bot.command(name="ايدي")
async def v4_prefix_id(ctx, member: discord.Member=None):
    member = member or ctx.author
    await ctx.reply(f"🆔 معرف {member.display_name}: `{member.id}`")

# ----------------------------------------------------------------
# إعدادات V4 بأوامر Slash — عدد محدود حتى لا نقترب من حدود Discord.
# ----------------------------------------------------------------
@tree.command(name=app_commands.locale_str("v4_dashboard", arabic="v4_لوحة"), description="لوحة التحكم الاحترافية العربية V4")
async def v4_dashboard(interaction: discord.Interaction):
    if not await v4_require_admin(interaction):
        return
    await interaction.response.send_message(
        embed=v4_embed("لوحة التحكم الاحترافية", v4_dashboard_text(interaction.guild), emoji="🎛️"),
        ephemeral=True
    )

@tree.command(name=app_commands.locale_str("v4_maintenance", arabic="v4_صيانة"), description="تفعيل أو تعطيل وضع الصيانة في السيرفر")
@app_commands.describe(تفعيل="true للتفعيل و false للتعطيل")
async def v4_maintenance(interaction: discord.Interaction, تفعيل: bool):
    if not await v4_require_admin(interaction):
        return
    v4_update_settings(interaction.guild.id, maintenance=1 if تفعيل else 0)
    v4_audit(interaction.guild.id, interaction.user.id, "تغيير وضع الصيانة", details=str(تفعيل))
    await interaction.response.send_message(
        embed=v4_success("تم " + ("تفعيل" if تفعيل else "تعطيل") + " وضع الصيانة.")
    )

@tree.command(name=app_commands.locale_str("v4_info", arabic="v4_معلومات"), description="معلومات شاملة عن طبقة V4 وأنظمتها")
async def v4_info_command(interaction: discord.Interaction):
    guild = interaction.guild
    text = (
        f"**الإصدار:** `{BOT_VERSION}`\n"
        f"**الأنظمة:** حماية، Anti-Spam، AutoMod، Welcome، Leave، AutoRole، Starboard، "
        f"Giveaways، Audit، Metrics، Cache، Rate Limit، Dashboard.\n"
        f"**قاعدة البيانات:** SQLite + WAL + Foreign Keys.\n"
        f"**اللغة:** العربية 🇸🇦"
    )
    await interaction.response.send_message(embed=v4_embed("V4", text, emoji="🚀"))

@tree.command(name=app_commands.locale_str("v4_audit", arabic="v4_تدقيق"), description="عرض آخر سجلات التدقيق الاحترافية")
@app_commands.describe(عدد="عدد السجلات من 1 إلى 20")
async def v4_audit_command(interaction: discord.Interaction, عدد: int=10):
    if not await v4_require_admin(interaction):
        return
    عدد = max(1, min(20, عدد))
    rows = v4_db_many(
        "SELECT * FROM enterprise_audit WHERE guild_id=? ORDER BY id DESC LIMIT ?",
        (interaction.guild.id, عدد)
    )
    if not rows:
        return await interaction.response.send_message(embed=v4_info("لا توجد سجلات بعد."), ephemeral=True)
    lines = []
    for row in rows:
        lines.append(
            f"`#{row['id']}` <@{row['actor_id']}> — **{row['action']}** — "
            f"{row['details'][:100] if row['details'] else 'بدون تفاصيل'}"
        )
    await interaction.response.send_message(
        embed=v4_embed("سجل التدقيق", "\n".join(lines), emoji="🧾"),
        ephemeral=True
    )

@tree.command(name=app_commands.locale_str("v4_flag", arabic="v4_فلاغ"), description="إضافة علامة إدارية سرية لعضو")
@app_commands.describe(عضو="العضو المستهدف", العلامة="اسم العلامة", القيمة="قيمة العلامة")
async def v4_flag_command(interaction: discord.Interaction, عضو: discord.Member, العلامة: str, القيمة: str="1"):
    if not await v4_require_admin(interaction):
        return
    v4_flag(interaction.guild.id, عضو.id, العلامة[:80], القيمة[:500])
    v4_audit(interaction.guild.id, interaction.user.id, "إضافة علامة عضو", عضو.id, f"{العلامة}={القيمة}")
    await interaction.response.send_message(embed=v4_success(f"تم حفظ العلامة `{العلامة}` للعضو {عضو.mention}."), ephemeral=True)

@tree.command(name=app_commands.locale_str("v4_flags", arabic="v4_فلاغات"), description="عرض العلامات الإدارية السرية لعضو")
async def v4_flags_command(interaction: discord.Interaction, عضو: discord.Member=None):
    if not await v4_require_admin(interaction):
        return
    عضو = عضو or interaction.user
    rows = v4_db_many(
        "SELECT flag,value,updated_at FROM enterprise_user_flags WHERE guild_id=? AND user_id=? ORDER BY flag",
        (interaction.guild.id, عضو.id)
    )
    text = "\n".join(f"• `{r['flag']}` = `{r['value']}`" for r in rows) or "لا توجد علامات."
    await interaction.response.send_message(embed=v4_embed(f"علامات {عضو.display_name}", text, emoji="🏷️"), ephemeral=True)

@tree.command(name=app_commands.locale_str("v4_messages", arabic="v4_رسائل"), description="إحصائيات الرسائل المرصودة في الذاكرة")
async def v4_message_stats(interaction: discord.Interaction):
    guild = interaction.guild
    top = v4_top_message_members(guild.id, 10)
    if not top:
        return await interaction.response.send_message(embed=v4_info("لم يتم رصد نشاط كافٍ بعد."))
    lines = []
    for i, (uid, count) in enumerate(top, 1):
        member = guild.get_member(uid)
        name = member.mention if member else f"<@{uid}>"
        lines.append(f"**{i}.** {name} — `{count}` رسالة")
    await interaction.response.send_message(embed=v4_embed("أكثر الأعضاء نشاطًا", "\n".join(lines), emoji="💬"))

@tree.command(name=app_commands.locale_str("v4_metrics", arabic="v4_مقاييس"), description="عرض مقاييس أنظمة البوت في السيرفر")
async def v4_metrics(interaction: discord.Interaction):
    if not await v4_require_admin(interaction):
        return
    metrics = v4_db_many(
        "SELECT metric,value FROM enterprise_metrics WHERE guild_id=? ORDER BY value DESC LIMIT 25",
        (interaction.guild.id,)
    )
    text = "\n".join(f"• `{r['metric']}`: **{r['value']}**" for r in metrics) or "لا توجد مقاييس."
    await interaction.response.send_message(embed=v4_embed("مقاييس النظام", text, emoji="📈"), ephemeral=True)

@tree.command(name=app_commands.locale_str("v4_data_backup", arabic="v4_نسخة_بيانات"), description="إنشاء نسخة SQL مستقلة من بيانات V4")
async def v4_data_backup(interaction: discord.Interaction):
    if not await v4_require_admin(interaction):
        return
    os.makedirs(BACKUP_DIR, exist_ok=True)
    filename = os.path.join(BACKUP_DIR, f"v4_{interaction.guild.id}_{int(time.time())}.sql")
    try:
        with DB_SYNC_LOCK:
            with open(filename, "w", encoding="utf-8") as handle:
                for line in db.iterdump():
                    handle.write(line + "\n")
        size = os.path.getsize(filename)
        v4_db_execute(
            "INSERT INTO enterprise_backups(guild_id,filename,size,created_at) VALUES(?,?,?,?)",
            (interaction.guild.id, filename, size, v4_now()), commit=True
        )
        v4_audit(interaction.guild.id, interaction.user.id, "نسخة بيانات V4", details=filename)
        await interaction.response.send_message(
            embed=v4_success(f"تم إنشاء النسخة.\nالحجم: `{size:,}` بايت"),
            ephemeral=True
        )
    except Exception as exc:
        await interaction.response.send_message(embed=v4_error(f"تعذر إنشاء النسخة: `{type(exc).__name__}`"), ephemeral=True)

# ----------------------------------------------------------------
# Worker: تنظيف الكاش والـ cooldowns ومقاييس الصحة
# ----------------------------------------------------------------
@tasks.loop(minutes=10)
async def v4_maintenance_worker():
    try:
        now = time.time()
        v4_db_execute("DELETE FROM enterprise_api_cache WHERE expires_at<?", (now,), commit=True)
        v4_db_execute("DELETE FROM enterprise_cooldowns WHERE used_at<?", (now-86400,), commit=True)
        v4_db_execute("DELETE FROM enterprise_commands_log WHERE created_at<?", ((datetime.now(timezone.utc)-timedelta(days=30)).isoformat(),), commit=True)
        for guild in bot.guilds:
            V4_LAST_HEALTH[guild.id] = {
                "latency": round(bot.latency*1000),
                "members": guild.member_count or 0,
                "channels": len(guild.channels),
                "updated": v4_now()
            }
    except Exception as exc:
        print("[V4 worker]", type(exc).__name__, exc)

@v4_maintenance_worker.before_loop
async def v4_maintenance_before():
    await bot.wait_until_ready()

# ----------------------------------------------------------------
# Worker: تنظيف البيانات القديمة
# ----------------------------------------------------------------
@tasks.loop(hours=6)
async def v4_data_housekeeping():
    try:
        cutoff = (datetime.now(timezone.utc)-timedelta(days=90)).isoformat()
        with DB_SYNC_LOCK:
            db.execute("DELETE FROM enterprise_audit WHERE created_at<?", (cutoff,))
            db.execute("DELETE FROM enterprise_automod_actions WHERE created_at<?", (cutoff,))
            db.execute("DELETE FROM enterprise_backups WHERE created_at<?", (cutoff,))
            db.commit()
    except Exception as exc:
        print("[V4 housekeeping]", type(exc).__name__, exc)

@v4_data_housekeeping.before_loop
async def v4_housekeeping_before():
    await bot.wait_until_ready()

# ----------------------------------------------------------------
# Worker: انتهاء الـ Giveaways
# ----------------------------------------------------------------
@tasks.loop(seconds=30)
async def v4_giveaway_worker():
    try:
        rows = v4_db_many(
            "SELECT * FROM enterprise_giveaways WHERE ended=0 AND ends_at<=?",
            (v4_now(),)
        )
        for giveaway in rows:
            entries = v4_giveaway_entries(giveaway["id"])
            winners = v4_pick_winners(entries, giveaway["winners"])
            guild = bot.get_guild(giveaway["guild_id"])
            channel = guild.get_channel(giveaway["channel_id"]) if guild else None
            mentions = ", ".join(f"<@{uid}>" for uid in winners) or "لا يوجد فائزون"
            if channel:
                await channel.send(
                    f"🎉 **انتهت المسابقة!**\n"
                    f"الجائزة: **{giveaway['prize']}**\n"
                    f"الفائزون: {mentions}"
                )
            v4_db_execute("UPDATE enterprise_giveaways SET ended=1 WHERE id=?", (giveaway["id"],), commit=True)
            if guild:
                v4_audit(guild.id, giveaway["created_by"], "انتهاء مسابقة", details=giveaway["prize"])
    except Exception as exc:
        print("[V4 giveaway]", type(exc).__name__, exc)

@v4_giveaway_worker.before_loop
async def v4_giveaway_before():
    await bot.wait_until_ready()

# ----------------------------------------------------------------
# Worker: حذف القنوات المؤقتة
# ----------------------------------------------------------------
@tasks.loop(minutes=1)
async def v4_temp_channel_worker():
    try:
        rows = v4_db_many(
            "SELECT * FROM enterprise_temp_channels WHERE expires_at IS NOT NULL AND expires_at<=?",
            (v4_now(),)
        )
        for row in rows:
            guild = bot.get_guild(row["guild_id"])
            channel = guild.get_channel(row["channel_id"]) if guild else None
            if channel:
                try:
                    await channel.delete(reason="انتهاء مدة القناة المؤقتة")
                except Exception:
                    pass
            v4_db_execute("DELETE FROM enterprise_temp_channels WHERE channel_id=?", (row["channel_id"],), commit=True)
    except Exception as exc:
        print("[V4 temp channel]", type(exc).__name__, exc)

@v4_temp_channel_worker.before_loop
async def v4_temp_before():
    await bot.wait_until_ready()

# ----------------------------------------------------------------
# بدء عمال V4
# ----------------------------------------------------------------
async def start_v4_workers():
    workers = [
        v4_maintenance_worker,
        v4_data_housekeeping,
        v4_giveaway_worker,
        v4_temp_channel_worker,
    ]
    for worker in workers:
        try:
            if not worker.is_running():
                worker.start()
        except Exception as exc:
            print("[V4 worker start]", type(exc).__name__, exc)

# ----------------------------------------------------------------
# V4 message observer.
# ----------------------------------------------------------------
async def v4_message_observer(message):
    if message.author.bot or not message.guild:
        return
    v4_record_message(message.guild.id, message.author.id)
    settings = v4_settings(message.guild.id)
    if settings["anti_mention"] and len(message.mentions) > int(settings["max_mentions"]):
        try:
            await message.delete()
            v4_automod_record(message.guild.id, message.author.id, "mentions", "delete", message.content)
            v4_audit(message.guild.id, bot.user.id if bot.user else 0, "حذف منشنات زائدة", message.author.id)
        except Exception:
            pass
    triggered, strikes, muted_until = v4_spam_check(
        message.guild.id,
        message.author.id,
        int(settings["max_messages"]),
        int(settings["message_window"])
    )
    if triggered:
        v4_automod_record(message.guild.id, message.author.id, "spam", "strike", message.content)
        try:
            await message.delete()
        except Exception:
            pass
        if strikes >= 3 and v4_bot_can_manage(message.guild, message.author):
            try:
                duration = max(30, int(muted_until-time.time()))
                await message.author.timeout(timedelta(seconds=min(duration, 600)), reason="V4 Anti-Spam")
            except Exception:
                pass

# ----------------------------------------------------------------
# ----------------------------------------------------------------
# Wrapper لأوامر Prefix لتسجيل الأداء.
# ----------------------------------------------------------------
class V4PerformanceCog(commands.Cog):
    def __init__(self, bot_instance):
        self.bot = bot_instance

    @commands.Cog.listener()
    async def on_command_completion(self, ctx):
        v4_log_command(
            ctx.guild.id if ctx.guild else None,
            ctx.author.id if ctx.author else 0,
            getattr(ctx.command, "qualified_name", "unknown"),
            0,
            True
        )

    @commands.Cog.listener()
    async def on_command_error(self, ctx, error):
        v4_log_command(
            ctx.guild.id if ctx.guild else None,
            ctx.author.id if ctx.author else 0,
            getattr(ctx.command, "qualified_name", "unknown"),
            0,
            False
        )

# ----------------------------------------------------------------
# نظام إدارة الـ Views للتوسعة المستقبلية.
# ----------------------------------------------------------------
class V4ConfirmView(discord.ui.View):
    def __init__(self, timeout=60):
        super().__init__(timeout=timeout)
        self.value = None

    @discord.ui.button(label="تأكيد", style=discord.ButtonStyle.success, emoji="✅")
    async def confirm(self, interaction, button):
        self.value = True
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)
        self.stop()

    @discord.ui.button(label="إلغاء", style=discord.ButtonStyle.danger, emoji="❌")
    async def cancel(self, interaction, button):
        self.value = False
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)
        self.stop()

class V4Pager(discord.ui.View):
    def __init__(self, pages, timeout=120):
        super().__init__(timeout=timeout)
        self.pages = list(pages)
        self.index = 0

    async def refresh(self, interaction):
        page = self.pages[self.index]
        embed = v4_embed(page.get("title","صفحة"), page.get("text",""), emoji=page.get("emoji","📄"))
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="السابق", style=discord.ButtonStyle.secondary, emoji="◀️")
    async def previous(self, interaction, button):
        self.index = (self.index - 1) % len(self.pages)
        await self.refresh(interaction)

    @discord.ui.button(label="التالي", style=discord.ButtonStyle.secondary, emoji="▶️")
    async def next(self, interaction, button):
        self.index = (self.index + 1) % len(self.pages)
        await self.refresh(interaction)

# ----------------------------------------------------------------
# مساعد إنشاء صفحات المساعدة
# ----------------------------------------------------------------
def v4_help_pages():
    return [
        {"title":"الإدارة","emoji":"🛡️","text":"تحذير، تحذيرات، مخالفات، طرد، حظر، فك حظر، لقب، رول، قفل، فتح، تنظيف، Slowmode."},
        {"title":"الأمان","emoji":"🔐","text":"Anti-Spam، Anti-Mention، AutoMod، كلمات ممنوعة، سجل تدقيق، علامات إدارية، Rate Limit."},
        {"title":"الاقتصاد","emoji":"💰","text":"رصيد، يومي، تحويل، سوق، شراء، بيع، ممتلكات، ترتيب الاقتصاد، رواتب، سجل مالي."},
        {"title":"المجتمع","emoji":"🌟","text":"اقتراحات، استطلاعات، تذاكر، ترحيب، مغادرة، AutoRole، Starboard، مسابقات."},
        {"title":"التقدم","emoji":"🏆","text":"XP، مستويات، جوائز المستويات، توب المستويات، مكافآت النشاط."},
        {"title":"النظام","emoji":"⚙️","text":"حالة البوت، الإحصائيات، النسخ الاحتياطي، فحص قاعدة البيانات، معلومات السيرفر، لوحة V4."},
    ]

@tree.command(name=app_commands.locale_str("v4_help", arabic="v4_مساعدة"), description="مساعدة تفاعلية لأنظمة V4 العربية")
async def v4_help_command(interaction: discord.Interaction):
    pages = v4_help_pages()
    view = V4Pager(pages)
    first = pages[0]
    await interaction.response.send_message(
        embed=v4_embed(first["title"], first["text"], emoji=first["emoji"]),
        view=view
    )

# ----------------------------------------------------------------
# إعداد إضافي بعد بدء البوت.
# ----------------------------------------------------------------
async def v4_bootstrap():
    try:
        for guild in bot.guilds:
            v4_settings(guild.id)
        await start_v4_workers()
        try:
            await bot.add_cog(V4PerformanceCog(bot))
        except Exception:
            pass
        if not v4_message_stats_housekeeping_loop.is_running():
            v4_message_stats_housekeeping_loop.start()
        print(f"🚀 V4 AR {BOT_VERSION} جاهز.")
    except Exception as exc:
        print("[V4 bootstrap]", type(exc).__name__, exc)

# ----------------------------------------------------------------
# مولد نصوص الإدارة — يستخدمه نظام الإشعارات والتقارير.
# ----------------------------------------------------------------
def v4_report_lines(guild):
    settings = v4_settings(guild.id)
    return [
        f"السيرفر: {guild.name}",
        f"المعرف: {guild.id}",
        f"الأعضاء: {guild.member_count}",
        f"القنوات: {len(guild.channels)}",
        f"الرتب: {len(guild.roles)}",
        f"الإيموجيات: {len(guild.emojis)}",
        f"Anti-Raid: {'مفعل' if settings['anti_raid'] else 'معطل'}",
        f"Anti-Mention: {'مفعل' if settings['anti_mention'] else 'معطل'}",
        f"الصيانة: {'مفعلة' if settings['maintenance'] else 'معطلة'}",
        f"الرسائل المرصودة: {v4_get_metric(guild.id,'messages_seen')}",
        f"الأوامر المستخدمة: {v4_get_metric(guild.id,'commands_used')}",
        f"أحداث التدقيق: {v4_get_metric(guild.id,'audit_events')}",
        f"حوادث AutoMod: {sum(v4_get_metric(guild.id,k) for k in ('automod_spam','automod_mentions','automod_links','automod_invites','automod_bad_words'))}",
    ]

# ----------------------------------------------------------------
# دوال إضافية للتقارير والتحقق — مقصودة كطبقة API داخلية.
# ----------------------------------------------------------------
def v4_member_snapshot(member):
    return {
        "id": member.id,
        "name": str(member),
        "display_name": member.display_name,
        "bot": bool(member.bot),
        "top_role": member.top_role.id if member.top_role else 0,
        "roles": [r.id for r in member.roles],
        "joined_at": member.joined_at.isoformat() if member.joined_at else None,
        "created_at": member.created_at.isoformat(),
    }

def v4_guild_snapshot(guild):
    return {
        "id": guild.id,
        "name": guild.name,
        "owner_id": guild.owner_id,
        "members": guild.member_count or 0,
        "channels": len(guild.channels),
        "roles": len(guild.roles),
        "emojis": len(guild.emojis),
        "boost_level": guild.premium_tier,
        "boosts": guild.premium_subscription_count or 0,
    }

def v4_channel_snapshot(channel):
    return {
        "id": channel.id,
        "name": getattr(channel, "name", "unknown"),
        "type": str(channel.type),
        "category_id": getattr(channel.category, "id", None),
        "position": getattr(channel, "position", 0),
    }

def v4_role_snapshot(role):
    return {
        "id": role.id,
        "name": role.name,
        "position": role.position,
        "managed": role.managed,
        "mentionable": role.mentionable,
        "hoist": role.hoist,
    }

# ----------------------------------------------------------------
# تنظيف آمن لمحتوى المستخدم في Embeds.
# ----------------------------------------------------------------
def v4_clean_text(text, maximum=1800):
    text = str(text or "").replace("\x00", "").strip()
    if len(text) > maximum:
        text = text[:maximum-3] + "..."
    return discord.utils.escape_mentions(text)

def v4_safe_reason(reason, fallback="بدون سبب"):
    value = v4_clean_text(reason, 500)
    return value or fallback

# ----------------------------------------------------------------
# اقتصاد: تحقق رقمي صارم.
# ----------------------------------------------------------------
def v4_money(value):
    try:
        value = int(value)
    except Exception:
        return 0
    return max(0, min(value, 10_000_000_000))

def v4_percent(value):
    try:
        value = float(value)
    except Exception:
        return 0.0
    return max(0.0, min(value, 100.0))

def v4_format_money(value):
    return f"{int(value):,} عملة"

# ----------------------------------------------------------------
# أدوات مستويات احترافية.
# ----------------------------------------------------------------
def v4_level_for_xp(xp):
    xp = max(0, int(xp))
    level = 0
    required = 100
    while xp >= required:
        xp -= required
        level += 1
        required = 100 + level * 50
        if level > 10000:
            break
    return level

def v4_xp_required(level):
    level = max(0, int(level))
    return sum(100 + i * 50 for i in range(level))

def v4_progress_bar(current, needed, width=12):
    if needed <= 0:
        return "█" * width
    ratio = max(0.0, min(1.0, current / needed))
    filled = int(ratio * width)
    return "█" * filled + "░" * (width-filled)

# ----------------------------------------------------------------
# أدوات الإدارة الجماعية.
# ----------------------------------------------------------------
async def v4_timeout_member(member, seconds, reason):
    if not member.guild.me:
        return False
    if not v4_bot_can_manage(member.guild, member):
        return False
    try:
        await member.timeout(timedelta(seconds=max(1, min(seconds, 28*24*3600))), reason=reason)
        return True
    except Exception:
        return False

async def v4_remove_timeout(member, reason):
    try:
        await member.timeout(None, reason=reason)
        return True
    except Exception:
        return False

async def v4_safe_delete_message(message, reason="V4"):
    try:
        await message.delete(reason=reason)
        return True
    except Exception:
        return False

# ----------------------------------------------------------------
# مولد معرفات آمنة.
# ----------------------------------------------------------------
def v4_token(prefix="V4"):
    return f"{prefix}-{secrets.token_hex(6).upper()}"

# ----------------------------------------------------------------
# أدوات فحص الصحة.
# ----------------------------------------------------------------
def v4_health(guild):
    health = V4_LAST_HEALTH.get(guild.id, {})
    return {
        "latency_ms": round(bot.latency*1000),
        "members": guild.member_count or 0,
        "channels": len(guild.channels),
        "database": "متصلة" if db else "غير متصلة",
        "workers": {
            "maintenance": v4_maintenance_worker.is_running(),
            "housekeeping": v4_data_housekeeping.is_running(),
            "giveaway": v4_giveaway_worker.is_running(),
            "temporary_channels": v4_temp_channel_worker.is_running(),
        },
        "last_update": health.get("updated", "الآن"),
    }

# ----------------------------------------------------------------
# طبقة أحداث النظام.
# ----------------------------------------------------------------
async def v4_member_join(member):
    if member.bot:
        return
    guild = member.guild
    v4_set_metric(guild.id, "joins", 1)
    config = v4_get_welcome(guild.id)
    if config and config["enabled"] and config["channel_id"]:
        channel = guild.get_channel(config["channel_id"])
        if channel:
            try:
                await channel.send(v4_format_message(config["message"], member, guild))
            except Exception:
                pass
    role_cfg = v4_get_autorole(guild.id)
    if role_cfg and role_cfg["enabled"] and role_cfg["role_id"]:
        role = guild.get_role(role_cfg["role_id"])
        if role and v4_bot_can_manage(guild, member):
            try:
                await member.add_roles(role, reason="V4 AutoRole")
            except Exception:
                pass

async def v4_member_leave(member):
    guild = member.guild
    v4_set_metric(guild.id, "leaves", 1)
    config = v4_get_leave(guild.id)
    if config and config["enabled"] and config["channel_id"]:
        channel = guild.get_channel(config["channel_id"])
        if channel:
            try:
                await channel.send(v4_format_message(config["message"], member, guild))
            except Exception:
                pass

# ----------------------------------------------------------------
# تصدير تقرير السيرفر إلى نص.
# ----------------------------------------------------------------
def v4_text_report(guild):
    return "\n".join(v4_report_lines(guild))

# ----------------------------------------------------------------
# عدادات موسعة — تجعل لوحة المقاييس قابلة للتوسع.
# ----------------------------------------------------------------
V4_METRIC_KEYS = [
    "messages_seen",
    "commands_used",
    "audit_events",
    "joins",
    "leaves",
    "automod_spam",
    "automod_mentions",
    "automod_links",
    "automod_invites",
    "automod_bad_words",
    "tickets_opened",
    "tickets_closed",
    "suggestions_created",
    "suggestions_accepted",
    "suggestions_rejected",
    "polls_created",
    "reminders_created",
    "economy_transfers",
    "economy_purchases",
    "economy_sales",
    "xp_gained",
    "warnings",
    "bans",
    "kicks",
    "timeouts",
]

def v4_all_metrics(guild_id):
    return {key: v4_get_metric(guild_id, key) for key in V4_METRIC_KEYS}

# ----------------------------------------------------------------
# تحقق من حالة الصيانة قبل الأنظمة الحساسة.
# ----------------------------------------------------------------
def v4_in_maintenance(guild_id):
    row = v4_settings(guild_id)
    return bool(row and row["maintenance"])

# ----------------------------------------------------------------
# مساعد لردود Interaction.
# ----------------------------------------------------------------
async def v4_reply(interaction, text, *, ephemeral=False, emoji="🤖"):
    embed = v4_embed("V4", v4_clean_text(text), emoji=emoji)
    if interaction.response.is_done():
        return await interaction.followup.send(embed=embed, ephemeral=ephemeral)
    return await interaction.response.send_message(embed=embed, ephemeral=ephemeral)

# ----------------------------------------------------------------
# وظائف تحليلية إضافية.
# ----------------------------------------------------------------
def v4_economy_snapshot(guild_id):
    row = v4_db_one(
        "SELECT COUNT(*) c,COALESCE(SUM(balance),0) total,COALESCE(AVG(balance),0) avg FROM economy WHERE guild_id=?",
        (guild_id,)
    )
    return {
        "users": int(row["c"]) if row else 0,
        "total": int(row["total"]) if row else 0,
        "average": int(row["avg"]) if row else 0,
    }

def v4_level_snapshot(guild_id):
    row = v4_db_one(
        "SELECT COUNT(*) c,COALESCE(MAX(level),0) max_level,COALESCE(AVG(level),0) avg_level FROM levels WHERE guild_id=?",
        (guild_id,)
    )
    return {
        "users": int(row["c"]) if row else 0,
        "max_level": int(row["max_level"]) if row else 0,
        "average": round(float(row["avg_level"])) if row else 0,
    }

# ----------------------------------------------------------------
# أدوات مراقبة السوق.
# ----------------------------------------------------------------
def v4_market_snapshot(guild_id):
    rows = v4_db_many("SELECT item,price FROM prices WHERE guild_id=? ORDER BY item", (guild_id,))
    return {r["item"]: int(r["price"]) for r in rows}

def v4_market_set(guild_id, item, price):
    item = v4_clean_text(item, 80)
    price = v4_money(price)
    v4_db_execute(
        """INSERT INTO prices(guild_id,item,price) VALUES(?,?,?)
           ON CONFLICT(guild_id,item) DO UPDATE SET price=excluded.price""",
        (guild_id,item,price), commit=True
    )

# ----------------------------------------------------------------
# حماية أسماء الأوامر المخصصة.
# ----------------------------------------------------------------
def v4_valid_custom_command(name):
    name = str(name or "").strip()
    if not name or len(name) > 40:
        return False
    if name.startswith(("v4_", "bot_", "system_")):
        return False
    return True

# ----------------------------------------------------------------
# إدارة الـ cache داخل الذاكرة.
# ----------------------------------------------------------------
V4_MEMORY_CACHE = {}
V4_MEMORY_CACHE_EXPIRES = {}

def v4_memory_put(key, value, ttl=60):
    V4_MEMORY_CACHE[key] = value
    V4_MEMORY_CACHE_EXPIRES[key] = time.time() + ttl

def v4_memory_get(key, default=None):
    expires = V4_MEMORY_CACHE_EXPIRES.get(key, 0)
    if expires < time.time():
        V4_MEMORY_CACHE.pop(key, None)
        V4_MEMORY_CACHE_EXPIRES.pop(key, None)
        return default
    return V4_MEMORY_CACHE.get(key, default)

# ----------------------------------------------------------------
# وظائف تنظيف الذاكرة.
# ----------------------------------------------------------------
def v4_memory_cleanup():
    now = time.time()
    expired = [k for k,v in V4_MEMORY_CACHE_EXPIRES.items() if v < now]
    for key in expired:
        V4_MEMORY_CACHE.pop(key, None)
        V4_MEMORY_CACHE_EXPIRES.pop(key, None)

# ----------------------------------------------------------------
# طبقة توافق للنسخ الاحتياطية.
# ----------------------------------------------------------------
def v4_backup_index(guild_id, limit=20):
    return v4_db_many(
        "SELECT * FROM enterprise_backups WHERE guild_id=? ORDER BY id DESC LIMIT ?",
        (guild_id, limit)
    )

# ----------------------------------------------------------------
# مراقبة أخطاء قاعدة البيانات.
# ----------------------------------------------------------------
V4_DB_ERRORS = Counter()

def v4_db_error(operation, exc):
    V4_DB_ERRORS[operation] += 1
    print(f"[V4 DB] {operation}: {type(exc).__name__}: {exc}")

# ----------------------------------------------------------------
# نظام تحميل إعدادات جميع السيرفرات.
# ----------------------------------------------------------------
def v4_warm_guild_cache():
    for guild in bot.guilds:
        try:
            v4_settings(guild.id)
            v4_memory_put(f"guild:{guild.id}", v4_guild_snapshot(guild), ttl=300)
        except Exception as exc:
            v4_db_error("warm_cache", exc)

# ----------------------------------------------------------------
# تقرير حالة شامل نصيًا.
# ----------------------------------------------------------------
def v4_full_status(guild):
    health = v4_health(guild)
    eco = v4_economy_snapshot(guild.id)
    levels = v4_level_snapshot(guild.id)
    return (
        f"**الحالة العامة**\n"
        f"• Latency: `{health['latency_ms']}ms`\n"
        f"• قاعدة البيانات: `{health['database']}`\n"
        f"• الأعضاء: `{health['members']}`\n"
        f"• القنوات: `{health['channels']}`\n\n"
        f"**الاقتصاد**\n"
        f"• المستخدمون: `{eco['users']}`\n"
        f"• إجمالي الأموال: `{eco['total']:,}`\n"
        f"• المتوسط: `{eco['average']:,}`\n\n"
        f"**المستويات**\n"
        f"• المستخدمون: `{levels['users']}`\n"
        f"• أعلى مستوى: `{levels['max_level']}`\n"
        f"• المتوسط: `{levels['average']}`"
    )

# ----------------------------------------------------------------
# سجل بدء التشغيل.
# ----------------------------------------------------------------
v4_startup_time = time.time()

def v4_uptime():
    seconds = int(time.time() - v4_startup_time)
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    return f"{days} يوم، {hours} ساعة، {minutes} دقيقة، {seconds} ثانية"

# ----------------------------------------------------------------
# أدوات التوافق مع Discord API.
# ----------------------------------------------------------------
async def v4_safe_send(channel, content=None, *, embed=None, view=None, delete_after=None):
    try:
        return await channel.send(content=content, embed=embed, view=view, delete_after=delete_after)
    except discord.Forbidden:
        return None
    except discord.HTTPException:
        return None

async def v4_safe_edit(message, **kwargs):
    try:
        return await message.edit(**kwargs)
    except (discord.Forbidden, discord.NotFound, discord.HTTPException):
        return None

async def v4_safe_dm(member, content=None, *, embed=None):
    try:
        return await member.send(content=content, embed=embed)
    except (discord.Forbidden, discord.HTTPException):
        return None

# ----------------------------------------------------------------
# تنظيف Cooldowns حسب السيرفر.
# ----------------------------------------------------------------
def v4_clear_user_cooldowns(guild_id, user_id):
    v4_db_execute(
        "DELETE FROM enterprise_cooldowns WHERE guild_id=? AND user_id=?",
        (guild_id,user_id), commit=True
    )

# ----------------------------------------------------------------
# نظام حالات المستخدم.
# ----------------------------------------------------------------
def v4_user_state(guild_id, user_id):
    return {
        "afk": bool(v4_db_one("SELECT 1 FROM afk WHERE guild_id=? AND user_id=?", (guild_id,user_id))),
        "warnings": int(v4_db_one("SELECT COUNT(*) c FROM warnings WHERE guild_id=? AND user_id=?", (guild_id,user_id))["c"]),
        "balance": int((v4_db_one("SELECT balance FROM economy WHERE guild_id=? AND user_id=?", (guild_id,user_id)) or {"balance":0})["balance"]),
        "level": int((v4_db_one("SELECT level FROM levels WHERE guild_id=? AND user_id=?", (guild_id,user_id)) or {"level":0})["level"]),
    }

# ----------------------------------------------------------------
# وظائف مراجعة الأمان.
# ----------------------------------------------------------------
def v4_permission_report(guild):
    me = guild.me
    if not me:
        return {}
    perms = me.guild_permissions
    return {
        "administrator": perms.administrator,
        "manage_messages": perms.manage_messages,
        "manage_roles": perms.manage_roles,
        "manage_channels": perms.manage_channels,
        "moderate_members": perms.moderate_members,
        "ban_members": perms.ban_members,
        "kick_members": perms.kick_members,
        "view_audit_log": perms.view_audit_log,
        "top_role": me.top_role.name,
        "top_role_position": me.top_role.position,
    }

# ----------------------------------------------------------------
# انتهاء طبقة V4.
# ----------------------------------------------------------------


# ----------------------------------------------------------------
# خادم ويب Render Keep-Alive / Health Endpoint.
# يعمل في Thread مستقل حتى لا يعطل بوت Discord.
# ----------------------------------------------------------------

# ----------------------------------------------------------------
# Render Self-Ping System
# ملاحظة: يجب ضبط PUBLIC_URL كرابط خدمة Render، مثال:
# https://your-service.onrender.com
# ----------------------------------------------------------------
V4_SELF_PING_URL = os.getenv("PUBLIC_URL", "").strip().rstrip("/")
V4_SELF_PING_INTERVAL_MINUTES = 5

async def _v4_self_ping():
    if not V4_SELF_PING_URL:
        return
    url = f"{V4_SELF_PING_URL}/health"
    timeout = aiohttp.ClientTimeout(total=15)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers={"User-Agent": "DiscordBot-V4-SelfPing/1.0"}) as response:
                logging.getLogger("V4").info(
                    "Self-ping: %s -> HTTP %s", url, response.status
                )
    except Exception as exc:
        logging.getLogger("V4").warning("Self-ping failed: %s", exc)

@tasks.loop(minutes=V4_SELF_PING_INTERVAL_MINUTES)
async def v4_self_ping_loop():
    await _v4_self_ping()

@v4_self_ping_loop.before_loop
async def _before_v4_self_ping_loop():
    await bot.wait_until_ready()

WEB_PORT = int(os.getenv("PORT", "8080"))
web_app = Flask(__name__)

@web_app.get("/")
def web_root():
    return "Bot is alive!", 200

@web_app.get("/health")
def web_health():
    return {"status": "ok", "bot": "alive"}, 200

def _run_web_server():
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    web_app.run(
        host="0.0.0.0",
        port=WEB_PORT,
        threaded=True,
        use_reloader=False,
    )

web_server_thread = threading.Thread(
    target=_run_web_server,
    name="render-web-server",
    daemon=True,
)
web_server_thread.start()

# يبدأ Self-Ping من setup_hook بعد دخول Discord event loop، وليس أثناء استيراد الملف.
bot.run(TOKEN)
