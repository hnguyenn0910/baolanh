import os
import re
import json
import asyncio
import unicodedata
from datetime import date, datetime, timezone, timedelta

import aiohttp
from aiohttp import web
import discord
from discord.ext import commands, tasks

# ======================== CẤU HÌNH BIẾN MÔI TRƯỜNG ========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
USER_TOKEN = os.getenv("USER_TOKEN", "").strip()  # User Token dùng đọc kênh tiền bảo lãnh

CHANNEL_ID_BAO_LANH = os.getenv("CHANNEL_ID_BAO_LANH", "1470521258429091982").strip()
SOURCE_GUILD_ID = os.getenv("SOURCE_GUILD_ID", "").strip()  # Server chứa kênh (dùng tạo link & lấy Nickname)

POLL_SECONDS = float(os.getenv("POLL_SECONDS", "30"))          # Tần suất kiểm tra tin nhắn mới (giây)
ANNOUNCE_NEW = os.getenv("ANNOUNCE_NEW", "false").lower() == "true"  # Thông báo khoản tiền mới
MAX_RANGE_DAYS = int(os.getenv("MAX_RANGE_DAYS", "62"))        # Tra cứu tối đa N ngày mỗi lần
CURRENCY = os.getenv("CURRENCY", "$")                          # Ký hiệu tiền tệ
MIN_AMOUNT = int(os.getenv("MIN_AMOUNT", "1000"))               # Số tiền tối thiểu để ghi nhận
COUNT_BOT_MESSAGES = os.getenv("COUNT_BOT_MESSAGES", "false").lower() == "true"
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "3"))
REQUEST_GAP = float(os.getenv("REQUEST_GAP", "0.25"))
SESSION_FILE = os.getenv("SESSION_FILE", "bail_tracking_session.json")

TZ_VN = timezone(timedelta(hours=7))
API = "https://discord.com/api/v10"
DISCORD_EPOCH = 1420070400000
COUNTED_TYPES = {0, 19}   # Tin nhắn thường + tin trả lời
FORUM_TYPES = {15, 16}    # Kênh Forum / Media
OVERLAP_MS = 30_000       # Đọc chồng 30s để không sót tin
MAX_PAGES = 500           # Tối đa 50.000 tin nhắn/kênh
USAGE = "ℹ️ Cách dùng: `!tienbaolanh 01/10 - 15/10` · `!tienbaolanh 15/10` · `!tienbaolanh` (hôm nay)"
READ_ERROR = "❌ Không đọc được kênh tiền bảo lãnh. Kiểm tra lại `USER_TOKEN` và `CHANNEL_ID_BAO_LANH`."
# ==========================================================================

intents = discord.Intents.default()
intents.message_content = True


class BailBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=intents, case_insensitive=True, help_command=None)
        self.api_session: aiohttp.ClientSession | None = None
        self.sem = asyncio.Semaphore(MAX_CONCURRENCY)
        self.lock = asyncio.Lock()
        self.source: dict | None = None     # {"type": loại kênh, "guild_id": server chứa kênh}
        self.tracking: dict | None = None   # Phiên !start đang chạy
        self.member_nick_cache: dict[str, str] = {}  # Cache lưu user_id -> server_nickname

    async def setup_hook(self):
        self.api_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        load_tracking()
        poll_loop.start()

    async def close(self):
        if poll_loop.is_running():
            poll_loop.cancel()
        if self.api_session and not self.api_session.closed:
            await self.api_session.close()
        await super().close()


bot = BailBot()

# ======================== TIỆN ÍCH ========================

def dt_to_snowflake(dt: datetime) -> int:
    return max(0, int(dt.timestamp() * 1000) - DISCORD_EPOCH) << 22


def money(n) -> str:
    return f"{int(n):,}".replace(",", ".") + CURRENCY


def fmt_time(iso: str) -> str:
    return datetime.fromisoformat(iso).astimezone(TZ_VN).strftime("%H:%M %d/%m/%Y")


def guild_id() -> str:
    return (bot.source or {}).get("guild_id") or str(SOURCE_GUILD_ID)


def msg_link(e: dict) -> str:
    return f"https://discord.com/channels/{guild_id()}/{e['channel_id']}/{e['id']}"


def strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFD", text.replace("đ", "d").replace("Đ", "D"))
    return "".join(c for c in text if unicodedata.category(c) != "Mn")


# ======================== ĐỌC SỐ TIỀN TRONG TIN NHẮN ========================

UNIT_MULT = {
    "k": 1_000, "nghin": 1_000, "ngan": 1_000,
    "tr": 1_000_000, "trieu": 1_000_000, "ty": 1_000_000_000,
}

AMOUNT_RE = re.compile(
    r"(?<![\w.,$])(?P<pre>\$\s*)?"
    r"(?P<num>\d{1,3}(?:[.,]\d{3})+(?:[.,]\d{1,2})?(?![.,]?\d)|\d+(?:[.,]\d+)?)"
    r"(?:\s{0,2}(?P<unit>nghin|ngan|trieu|tr|ty|vnd|usd|dong|dollar|dola|do|k|d|\$))?(?!\w)"
)
GROUPED_RE = re.compile(r"(\d{1,3}(?:[.,]\d{3})+)(?:[.,](\d{1,2}))?")
MD_LINK_RE = re.compile(r"\[([^\]]*)\]\([^)]*\)")
NOISE_RE = re.compile(
    r"<[^<>]*>"                                         # @tag, #kênh, emoji, <t:...>, <link>
    r"|https?://\S+"                                     # link
    r"|(?<!\d)\d{1,2}[/.\-]\d{1,2}[/.\-]\d{2,4}(?!\d)"   # Ngày dạng 05/10/2026
    r"|(?<!\d)\d{1,2}/\d{1,2}(?!\d)"                     # Ngày dạng 05/10
    r"|(?<!\d)\d{1,2}(?::\s?\d{2}|h\d{2})(?!\d)"         # Giờ dạng 14:30, 14h30
)
LIST_NUM_RE = re.compile(r"^(?:#\s*)?\d{1,3}\s*[.)]\s+")
BAIL_KW = re.compile(r"\b(bao\s*lanh|bl)\b")
MONEY_KW = re.compile(r"\b(bao\s*lanh|bl|tien|nop|phi|chuyen\s*khoan|ck|gui|dong\s*phat|phat|ho\s*so)\b")
TOTAL_KW = re.compile(r"\b(tong|total|sum|tc)\b")
ID_KW = re.compile(r"\b(cccd|cmnd|sdt|so\s*dien\s*thoai|id|uid|mssv|ms|ma\s*so|stt|code|seri)\b")
ONLY_NUM_RE = re.compile(r"^[\d.,\s]+$")

# Sửa lỗi gõ nhầm chữ 'o' thay vì số '0' (3ok -> 30k, 1ook -> 100k, 3o.ooo -> 30.000)
TYPO_ZERO_RE = re.compile(
    r"(?<![a-z])(\d[\do.,]*)(?=\s*(?:nghin|ngan|trieu|tr|ty|vnd|usd|dong|dollar|dola|do|k|d|\$|\b))"
)


def fix_zero_typos(line: str) -> str:
    return TYPO_ZERO_RE.sub(lambda m: m.group(1).replace("o", "0"), line)


def to_number(num: str):
    m = GROUPED_RE.fullmatch(num)
    if m:
        frac = m.group(2) or ""
        return int(re.sub(r"\D", "", m.group(1))) + (int(frac) / 10 ** len(frac) if frac else 0), True
    if len(re.sub(r"\D", "", num)) > 12:  # ID quá dài
        return None, False
    return float(num.replace(",", ".")), False


def line_amount(line: str, msg_has_kw: bool):
    sure, grouped, plain = [], [], []
    for m in AMOUNT_RE.finditer(line):
        val, is_grouped = to_number(m.group("num"))
        if val is None:
            continue

        unit = m.group("unit")
        mult = UNIT_MULT.get(unit, 1)

        # Xử lý ghi thừa 'k' sau số hàng nghìn (75000k -> 75000)
        if unit in ("k", "nghin", "ngan") and (is_grouped or val >= 1000):
            mult = 1

        val = round(val * mult)

        if val < MIN_AMOUNT:
            continue

        if m.group("pre") or unit:
            sure.append(val)
        elif is_grouped:
            grouped.append(val)
        else:
            if val % 1000 == 0:
                plain.append(val)

    if sure:
        return max(sure)
    if ID_KW.search(line):
        return None
    if grouped:
        return max(grouped)
    if plain and (msg_has_kw or ONLY_NUM_RE.fullmatch(line)):
        return max(plain)
    return None


def parse_amount(text: str) -> int:
    lines = []
    for raw in MD_LINK_RE.sub(r"\1", text or "").splitlines():
        line = NOISE_RE.sub(" ", strip_accents(raw).lower())
        line = re.sub(r"[*_~`|]", " ", line).strip().lstrip(">").strip()
        line = LIST_NUM_RE.sub("", line)
        line = fix_zero_typos(line)
        if line:
            lines.append(line)

    has_kw = any(MONEY_KW.search(line) for line in lines)
    found = []
    for line in lines:
        amount = line_amount(line, has_kw)
        if amount is not None:
            found.append((amount, bool(TOTAL_KW.search(line)), bool(BAIL_KW.search(line))))

    if not found:
        return 0

    totals = [a for a, is_total, _ in found if is_total]
    if totals:
        return max(totals)

    bails = [a for a, _, is_bail in found if is_bail]
    return sum(bails) if bails else sum(a for a, _, _ in found)


# ======================== GỌI API BẰNG USER TOKEN ========================

def get_user_headers():
    return {
        "Authorization": USER_TOKEN,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }


async def api_get(url: str):
    async with bot.sem:
        for attempt in range(5):
            wait = 2.0 * (attempt + 1)
            try:
                async with bot.api_session.get(url, headers=get_user_headers()) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        await asyncio.sleep(REQUEST_GAP)
                        return data
                    if resp.status == 429:
                        try:
                            wait = float((await resp.json(content_type=None)).get("retry_after", wait))
                        except Exception:
                            pass
                    else:
                        print(f"⚠️ HTTP {resp.status}: {url}")
                        return None
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"⚠️ Lỗi mạng ({e!r}), thử lại: {url}")
            await asyncio.sleep(wait + 0.5)
    return None


async def get_server_nickname(user_id: str, author_dict: dict) -> str:
    """Lấy biệt danh (Nickname) của thành viên trong Server nguồn (LSPD ...)."""
    if not user_id:
        return author_dict.get("global_name") or author_dict.get("username") or "?"

    if user_id in bot.member_nick_cache:
        return bot.member_nick_cache[user_id]

    gid = guild_id()
    if gid:
        data = await api_get(f"{API}/guilds/{gid}/members/{user_id}")
        if data and isinstance(data, dict):
            nick = data.get("nick")
            if nick:
                bot.member_nick_cache[user_id] = nick
                return nick

    fallback = author_dict.get("global_name") or author_dict.get("username") or "?"
    bot.member_nick_cache[user_id] = fallback
    return fallback


async def source_info():
    if bot.source is None:
        data = await api_get(f"{API}/channels/{CHANNEL_ID_BAO_LANH}")
        if data is None:
            return None
        bot.source = {"type": int(data.get("type", 0)), "guild_id": str(data.get("guild_id") or SOURCE_GUILD_ID)}
        kind = "forum (nhiều bài)" if bot.source["type"] in FORUM_TYPES else "kênh chat thường"
        print(f"📡 Kênh tiền bảo lãnh: {data.get('name', CHANNEL_ID_BAO_LANH)} — {kind}")
    return bot.source


def last_activity(t: dict) -> int:
    return int(t.get("last_message_id") or t["id"])


async def forum_threads(after_sf: int):
    found = set()
    for archived in ("false", "true"):
        offset = 0
        for _ in range(200):
            data = await api_get(f"{API}/channels/{CHANNEL_ID_BAO_LANH}/threads/search?tag_setting=match_some"
                                 f"&archived={archived}&sort_by=last_message_time&sort_order=desc"
                                 f"&limit=25&offset={offset}")
            if data is None:
                return None
            threads = data.get("threads", [])
            recent = [t for t in threads if last_activity(t) > after_sf]
            found.update(t["id"] for t in recent)
            if not recent or not data.get("has_more", len(threads) == 25):
                break
            offset += len(threads)
    return list(found)


async def fetch_messages(channel_id: str, after_sf: int, before_sf: int | None = None):
    out, cursor = [], after_sf
    for _ in range(MAX_PAGES):
        data = await api_get(f"{API}/channels/{channel_id}/messages?limit=100&after={cursor}")
        if data is None:
            return None
        if not data:
            break
        out += [m for m in data if before_sf is None or int(m["id"]) < before_sf]
        top = max(int(m["id"]) for m in data)
        if len(data) < 100 or (before_sf is not None and top >= before_sf):
            break
        cursor = top
    return out


def is_bot_message(m: dict) -> bool:
    return bool((m.get("author") or {}).get("bot") or m.get("webhook_id"))


def message_text(m: dict) -> str:
    parts = [m.get("content") or ""]
    if is_bot_message(m):
        for e in m.get("embeds") or []:
            parts += [e.get("title") or "", e.get("description") or ""]
            parts += [f"{f.get('name', '')}: {f.get('value', '')}" for f in e.get("fields") or []]
    return "\n".join(parts)


async def to_entry(m: dict, channel_id: str):
    if m.get("type", 0) not in COUNTED_TYPES:
        return None
    if is_bot_message(m) and not COUNT_BOT_MESSAGES:
        return None
    if (m.get("content") or "").lstrip().startswith("!"):
        return None

    text = message_text(m)
    amount = parse_amount(text)
    if amount <= 0 and not m.get("attachments") and not re.search(r"\d", NOISE_RE.sub(" ", text)):
        return None

    author = m.get("author") or {}
    author_id = str(author.get("id", ""))
    author_name = await get_server_nickname(author_id, author)

    return {
        "id": str(m["id"]),
        "channel_id": str(channel_id),
        "author_id": author_id,
        "author": author_name,
        "amount": amount,
    }


async def collect(after_sf: int, before_sf: int | None = None):
    info = await source_info()
    if info is None:
        return None

    if info["type"] in FORUM_TYPES:
        channels = await forum_threads(after_sf)
        if channels is None:
            return None
    else:
        channels = [str(CHANNEL_ID_BAO_LANH)]

    results = await asyncio.gather(*(fetch_messages(c, after_sf, before_sf) for c in channels))
    if any(r is None for r in results):
        return None

    entries, skipped, max_id = {}, {}, 0
    for cid, msgs in zip(channels, results):
        for m in msgs:
            max_id = max(max_id, int(m["id"]))
            e = await to_entry(m, cid)
            if e:
                (entries if e["amount"] > 0 else skipped)[e["id"]] = e
    return entries, skipped, max_id


# ======================== PHIÊN !start (TỰ ĐỘNG CỘNG) ========================

def save_tracking():
    try:
        if bot.tracking is None:
            if os.path.exists(SESSION_FILE):
                os.remove(SESSION_FILE)
            return
        tmp = SESSION_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(bot.tracking, f, ensure_ascii=False)
        os.replace(tmp, SESSION_FILE)
    except OSError as e:
        print(f"⚠️ Không ghi được file phiên: {e}")


def load_tracking():
    try:
        with open(SESSION_FILE, encoding="utf-8") as f:
            bot.tracking = json.load(f)
        print(f"📦 Tiếp tục phiên !start từ {fmt_time(bot.tracking['started_at'])}")
    except (OSError, ValueError, KeyError, TypeError):
        bot.tracking = None


def tracking_total(t: dict) -> int:
    initial = t.get("initial_amount", 0)
    return initial + sum(e["amount"] for e in t["entries"].values())


def merge(t: dict, entries: dict, skipped: dict):
    for k, e in entries.items():
        t["skipped"].pop(k, None)
        t["entries"][k] = e
    for k, e in skipped.items():
        t["entries"].pop(k, None)
        t["skipped"][k] = e


async def announce(channel_id: int, new: list, total: int, count: int):
    try:
        channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
        lines = [f"💰 **+{money(e['amount'])}** — {e['author']} · [xem tin]({msg_link(e)})" for e in new[:10]]
        if len(new) > 10:
            lines.append(f"… và {len(new) - 10} khoản khác")
        lines.append(f"📊 Tổng từ lúc `!start`: **{money(total)}** ({count} lần gửi)")
        await channel.send("\n".join(lines))
    except discord.HTTPException as e:
        print(f"⚠️ Không gửi được thông báo: {e!r}")


async def poll_once():
    async with bot.lock:
        t = bot.tracking
        if not t:
            return
        after = max(t["start_id"], t["last_seen_id"] - (OVERLAP_MS << 22))
        result = await collect(after)
        if result is None:
            return
        entries, skipped, max_id = result
        new = [entries[k] for k in sorted(entries, key=int) if k not in t["entries"]]
        merge(t, entries, skipped)
        t["last_seen_id"] = max(t["last_seen_id"], max_id)
        t["updated_at"] = datetime.now(timezone.utc).isoformat()
        save_tracking()
        total, count, channel_id = tracking_total(t), len(t["entries"]), t["notify_channel_id"]
    if new and ANNOUNCE_NEW:
        await announce(channel_id, new, total, count)


async def refresh_tracking() -> bool:
    async with bot.lock:
        t = bot.tracking
        if not t:
            return False
        result = await collect(t["start_id"])
        if result is None:
            return False
        t["entries"], t["skipped"], max_id = result
        t["last_seen_id"] = max(t["last_seen_id"], max_id)
        t["updated_at"] = datetime.now(timezone.utc).isoformat()
        save_tracking()
        return True


@tasks.loop(seconds=POLL_SECONDS)
async def poll_loop():
    try:
        await poll_once()
    except Exception as e:
        print(f"❌ Lỗi khi cộng tiền bảo lãnh: {e!r}")


@poll_loop.before_loop
async def before_poll():
    await bot.wait_until_ready()


# ======================== HIỂN THỊ ========================

def fill_lines(lines: list[str], limit: int = 1000) -> str:
    text = ""
    for i, line in enumerate(lines):
        if len(text) + len(line) + 1 > limit:
            return text + f"… và {len(lines) - i} người nữa"
        text += line + "\n"
    return text


def chunk_items(items: list[str], sep: str = " · ", limit: int = 1024) -> list[str]:
    chunks, current = [], ""
    for item in items:
        candidate = item if not current else f"{current}{sep}{item}"
        if len(candidate) <= limit:
            current = candidate
        else:
            if current:
                chunks.append(current)
            current = item[:limit]
    if current:
        chunks.append(current)
    return chunks


def build_embeds(title: str, header: str, entries: dict, skipped: dict, color: int, footer: str, initial_amount: int = 0) -> list[discord.Embed]:
    total = initial_amount + sum(e["amount"] for e in entries.values())
    people = {}
    for e in entries.values():
        p = people.setdefault(e["author_id"], {"name": e["author"], "total": 0, "count": 0})
        p["total"] += e["amount"]
        p["count"] += 1
    ranking = sorted(people.values(), key=lambda p: p["total"], reverse=True)

    desc = f"{header}\n💰 **Tổng tiền bảo lãnh: {money(total)}** ({len(entries)} lần gửi)"
    if initial_amount > 0:
        desc += f"\n💵 *(Đã bao gồm số tiền khởi tạo ban đầu: {money(initial_amount)})*"

    main_embed = discord.Embed(
        title=title,
        color=color,
        description=desc,
    )

    if ranking:
        medals = ["🥇", "🥈", "🥉"]
        lines = [
            f"{medals[i] if i < 3 else f'`#{i + 1}`'} **{p['name']}**: {money(p['total'])} ({p['count']} lần)"
            for i, p in enumerate(ranking)
        ]
        main_embed.add_field(name="👮 Theo người gửi", value=fill_lines(lines), inline=False)

    embeds = [main_embed]

    if skipped:
        all_skipped = sorted(skipped.values(), key=lambda e: int(e["id"]), reverse=True)
        items = [f"`{idx}.` [{e['author']}]({msg_link(e)})" for idx, e in enumerate(all_skipped, 1)]
        chunks = chunk_items(items, sep=" · ", limit=1024)

        for i, chunk in enumerate(chunks):
            field_name = (
                f"⚠️ {len(skipped)} tin có số/ảnh nhưng không đọc được tiền (không cộng)"
                if i == 0 else f"⚠️ Danh sách tin không đọc được (tiếp #{i + 1})"
            )
            target = embeds[-1]

            if len(target.fields) >= 24 or (len(target) + len(field_name) + len(chunk) > 5500):
                if len(embeds) >= 10:
                    break
                target = discord.Embed(color=color)
                embeds.append(target)

            target.add_field(name=field_name, value=chunk, inline=False)

    embeds[-1].set_footer(text=footer)
    return embeds


# ======================== NGÀY THÁNG ========================

DATE_FIND_RE = re.compile(r"\b\d{1,2}[/.]\d{1,2}(?:[/.]\d{2,4})?\b")


def parse_date(token: str, year: int):
    parts = [int(p) for p in re.split(r"[/.]", token)]
    try:
        if len(parts) == 3:
            y = parts[2] + 2000 if parts[2] < 100 else parts[2]
            return date(y, parts[1], parts[0]), True
        return date(year, parts[1], parts[0]), False
    except ValueError:
        return None


def year_back(d: date) -> date:
    try:
        return d.replace(year=d.year - 1)
    except ValueError:
        return d.replace(year=d.year - 1, day=28)


def parse_range(tokens: list[str], today: date):
    parsed = [parse_date(t, today.year) for t in tokens]
    if any(p is None for p in parsed):
        return "Ngày không hợp lệ. Dùng dạng `dd/mm` hoặc `dd/mm/yyyy`."
    (start_d, start_has_year), (last_d, last_has_year) = parsed[0], parsed[-1]

    if start_d > today and not start_has_year:
        start_d = year_back(start_d)
        if not last_has_year and last_d > today:
            last_d = year_back(last_d)

    if start_d > last_d:
        return "Ngày bắt đầu phải trước hoặc bằng ngày kết thúc."
    if start_d > today:
        return "Ngày bắt đầu đang ở tương lai."

    last_d = min(last_d, today)
    if (last_d - start_d).days + 1 > MAX_RANGE_DAYS:
        return f"Mỗi lần chỉ tra tối đa {MAX_RANGE_DAYS} ngày."

    return start_d, last_d


# ======================== CÂU LỆNH ========================

@bot.command(name="tienbaolanh")
async def cmd_tienbaolanh(ctx, *, args: str = ""):
    """Tổng tiền bảo lãnh theo ngày. Vd: !tienbaolanh 01/10 - 15/10"""
    now = datetime.now(TZ_VN)
    tokens = DATE_FIND_RE.findall(args)
    if len(tokens) > 2 or (args.strip() and not tokens):
        await ctx.send(USAGE)
        return

    r = parse_range(tokens or [f"{now:%d/%m/%Y}"], now.date())
    if isinstance(r, str):
        await ctx.send(f"⚠️ {r}")
        return

    start_d, last_d = r
    start = datetime.combine(start_d, datetime.min.time(), tzinfo=TZ_VN)
    end = min(datetime.combine(last_d + timedelta(days=1), datetime.min.time(), tzinfo=TZ_VN), now)
    period = f"{start_d:%d/%m/%Y}" if start_d == last_d else f"{start_d:%d/%m/%Y} → {last_d:%d/%m/%Y}"

    wait_msg = await ctx.send(f"⏳ Đang tổng hợp tiền bảo lãnh `{period}`...")
    result = await collect(dt_to_snowflake(start), dt_to_snowflake(end))

    if result is None:
        await wait_msg.edit(content=READ_ERROR)
        return

    entries, skipped, _ = result
    embeds = build_embeds(
        "💰 TỔNG TIỀN BẢO LÃNH",
        f"📅 `{period}`",
        entries,
        skipped,
        0xF1C40F,
        f"Yêu cầu bởi {ctx.author.display_name} • {now:%H:%M %d/%m/%Y}",
    )
    await wait_msg.edit(content=None, embeds=embeds)


@bot.command(name="start")
async def cmd_start(ctx, *, args: str = ""):
    """Bắt đầu tự động cộng tiền bảo lãnh từ bây giờ (Có thể kèm số tiền gốc ban đầu, vd: !start 300k)."""
    t = bot.tracking
    if t:
        await ctx.send(
            f"⚠️ Đang cộng tiền từ `{fmt_time(t['started_at'])}` (bật bởi **{t['started_by']}**). "
            "Gõ `!check` để xem tổng hoặc `!stop` để huỷ trước khi start lại."
        )
        return

    if await source_info() is None:
        await ctx.send(READ_ERROR)
        return

    # Đoạn xử lý số tiền khởi tạo ban đầu (ví dụ: !start 300k hay !start 22500k)
    initial_amount = 0
    if args.strip():
        parsed_init = parse_amount(args)
        if parsed_init > 0:
            initial_amount = parsed_init

    started = ctx.message.created_at.isoformat()
    bot.tracking = {
        "start_id": int(ctx.message.id),
        "last_seen_id": int(ctx.message.id),
        "started_at": started,
        "updated_at": started,
        "started_by": ctx.author.display_name,
        "notify_channel_id": ctx.channel.id,
        "initial_amount": initial_amount,
        "entries": {},
        "skipped": {},
    }
    save_tracking()

    init_text = f" với số tiền ban đầu **{money(initial_amount)}**" if initial_amount > 0 else ""
    await ctx.send(
        f"▶️ Bắt đầu cộng tiền bảo lãnh từ `{fmt_time(started)}`{init_text}. "
        f"Bot kiểm tra kênh mỗi {POLL_SECONDS:g} giây"
        f"{' và báo từng khoản mới vào đây' if ANNOUNCE_NEW else ''}.\n"
        "`!check` xem tổng · `!stop` huỷ"
    )


@bot.command(name="check")
async def cmd_check(ctx):
    """Xem tổng tiền bảo lãnh từ lúc !start."""
    if not bot.tracking:
        await ctx.send("ℹ️ Chưa bật cộng tiền. Gõ `!start` để bắt đầu.")
        return

    ok = await refresh_tracking()
    t = bot.tracking
    if not t:
        await ctx.send("ℹ️ Phiên vừa bị dừng.")
        return

    note = "" if ok else " • ⚠️ chưa đọc lại được kênh, đang hiện số đã lưu"
    embeds = build_embeds(
        "💰 TIỀN BẢO LÃNH TỪ LÚC !start",
        f"⏱️ Từ `{fmt_time(t['started_at'])}` đến nay • bật bởi **{t['started_by']}**",
        t["entries"],
        t["skipped"],
        0x2ECC71,
        f"Cập nhật {fmt_time(t['updated_at'])}{note}",
        initial_amount=t.get("initial_amount", 0)
    )
    await ctx.send(embeds=embeds)


@bot.command(name="stop")
async def cmd_stop(ctx):
    """Huỷ phiên !start (hiện tổng chốt lần cuối)."""
    if not bot.tracking:
        await ctx.send("ℹ️ Không có phiên `!start` nào đang chạy.")
        return

    await refresh_tracking()
    async with bot.lock:
        t, bot.tracking = bot.tracking, None
        save_tracking()

    if not t:
        return

    embeds = build_embeds(
        "⏹️ ĐÃ DỪNG CỘNG TIỀN BẢO LÃNH",
        f"⏱️ `{fmt_time(t['started_at'])}` → `{datetime.now(TZ_VN):%H:%M %d/%m/%Y}`\n"
        f"Bật bởi **{t['started_by']}** • dừng bởi **{ctx.author.display_name}**",
        t["entries"],
        t["skipped"],
        0xE74C3C,
        "Gõ !start để bắt đầu phiên mới",
        initial_amount=t.get("initial_amount", 0)
    )
    await ctx.send(embeds=embeds)


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    print(f"⚠️ Lỗi lệnh !{ctx.invoked_with}: {error!r}")


@bot.event
async def on_ready():
    print(f"✅ Bot đã online thành công trên Render: {bot.user.name} (ID: {bot.user.id})")


# ======================== WEB SERVER GIỮ BOT SỐNG ========================

async def handle_ping(request):
    t = bot.tracking
    status = f"đang cộng từ {fmt_time(t['started_at'])}: {money(tracking_total(t))}" if t else "chưa !start"
    return web.Response(text=f"Bot tiền bảo lãnh is alive! {status}")


async def setup_web_server():
    app = web.Application()
    app.router.add_get("/", handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"🌐 Web server đang lắng nghe trên cổng {port}")


async def main():
    if not BOT_TOKEN:
        print("❌ LỖI: BOT_TOKEN chưa được cài đặt trong Environment Variables!")
        return
    if not USER_TOKEN:
        print("❌ LỖI: USER_TOKEN chưa được cài đặt trong Environment Variables!")
        return

    # Khởi chạy Web Server độc lập trước để pass Health Check Render
    await setup_web_server()

    # Khởi chạy Discord Bot
    async with bot:
        await bot.start(BOT_TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
