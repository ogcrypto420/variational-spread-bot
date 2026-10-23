"""
Variational Omni — бот-алертер низкого спреда.

Раз в POLL_SECONDS секунд забирает публичную статистику Variational
(GET /metadata/v2/stats), фильтрует рынки по OI / спреду / объёму
и шлёт алерт в Telegram, когда рынок попадает под условия.

Настройки задаются прямо в чате: командами или обычным текстом,
например «ищи OI до 300к и спред 0.02».
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramForbiddenError
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import Message
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/v2/stats"

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ALLOWED_USERS = {int(x) for x in re.split(r"[,\s]+", os.getenv("ALLOWED_USERS", "")) if x.strip()}
POLL_SECONDS = max(1.0, float(os.getenv("POLL_SECONDS", "1")))
MAX_QUOTE_AGE = int(os.getenv("MAX_QUOTE_AGE_SECONDS", "900"))  # котировки старше — игнорируем
DATA_FILE = Path(os.getenv("DATA_FILE", "settings.json"))
STATE_FILE = Path(os.getenv("STATE_FILE", "state.json"))
STATS_FILE = Path(os.getenv("STATS_FILE", "stats.json"))    # статистика для владельца (/stats)
ADMIN_IDS = {int(x) for x in re.split(r"[,\s]+", os.getenv("ADMIN_ID", "")) if x.strip()}
TZ = timezone(timedelta(hours=int(os.getenv("TZ_OFFSET_HOURS", "3"))))  # для «сегодня» в статистике (МСК)   # что уже алертили — чтобы не дублировать после рестарта
RUN_UNTIL = float(os.getenv("RUN_UNTIL", "0") or 0)          # unix-время, когда штатно завершиться (для GitHub Actions)

DEFAULTS = {
    "enabled": True,
    "max_oi": 1_000_000.0,     # $
    "min_oi": 0.0,             # $
    "min_volume": 0.0,         # $ за 24ч
    "max_spread_pct": 0.03,    # в процентах: 0.03 = 0.03%
    "spread_mode": "site",     # site — как на сайте Variational (половина bid-ask), full — полный bid-ask
    "size": "base",            # base | 1k | 100k | 1m
    "types": "all",            # all | crypto | tradfi | perp | swap
    "cooldown_min": 30,        # не повторять алерт по той же паре чаще
    "defaults_v": 2,
}

SIZE_KEYS = {"base": "base", "1k": "size_1k", "100k": "size_100k", "1m": "size_1m"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("varbot")

# ---------------------------------------------------------------- storage

settings: dict[int, dict] = {}
runtime: dict[int, dict] = {}   # {chat_id: {"active": set, "last_alert": {ticker: ts}}}
last_listings: list[dict] = []
last_fetch_ts: float = 0.0
STARTED_AT = time.time()
session_stats = {"api_ok": 0, "api_err": 0, "last_err": ""}
stats: dict = {"users": {}, "alerts_total": 0, "alerts_by_day": {}, "alerts_by_ticker": {}, "blocked": 0}


def load_settings() -> None:
    if DATA_FILE.exists():
        try:
            raw = json.loads(DATA_FILE.read_text("utf-8"))
            for k, v in raw.items():
                if v.get("max_oi") == 500_000.0 and not v.get("defaults_v"):
                    v["max_oi"] = DEFAULTS["max_oi"]   # старый дефолт 500k -> новый 1M
                v["defaults_v"] = 2
                settings[int(k)] = {**DEFAULTS, **v}
        except Exception as e:  # noqa: BLE001
            log.warning("Не смог прочитать %s: %s", DATA_FILE, e)


def save_settings() -> None:
    tmp = DATA_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps({str(k): v for k, v in settings.items()}, ensure_ascii=False, indent=2), "utf-8")
    tmp.replace(DATA_FILE)


def get_st(chat_id: int) -> dict:
    if chat_id not in settings:
        settings[chat_id] = dict(DEFAULTS)
        save_settings()
    return settings[chat_id]


def load_state() -> None:
    if STATE_FILE.exists():
        try:
            raw = json.loads(STATE_FILE.read_text("utf-8"))
            for k, v in raw.items():
                runtime[int(k)] = {"active": set(v.get("active", [])), "last_alert": dict(v.get("last_alert", {}))}
        except Exception as e:  # noqa: BLE001
            log.warning("Не смог прочитать %s: %s", STATE_FILE, e)


def save_state() -> None:
    data = {str(k): {"active": sorted(v["active"]), "last_alert": v["last_alert"]} for k, v in runtime.items()}
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data), "utf-8")
    tmp.replace(STATE_FILE)


def reset_runtime(chat_id: int) -> None:
    """После смены фильтров заново алертим всё, что подходит."""
    runtime[chat_id] = {"active": set(), "last_alert": {}}
    save_state()


def load_stats() -> None:
    if STATS_FILE.exists():
        try:
            stats.update(json.loads(STATS_FILE.read_text("utf-8")))
        except Exception as e:  # noqa: BLE001
            log.warning("Не смог прочитать %s: %s", STATS_FILE, e)


def save_stats() -> None:
    tmp = STATS_FILE.with_suffix(".stmp")
    tmp.write_text(json.dumps(stats, ensure_ascii=False), "utf-8")
    tmp.replace(STATS_FILE)


def today() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d")


def touch_user(m: Message) -> None:
    """Запоминаем пользователя для статистики."""
    u = m.from_user
    if not u:
        return
    rec = stats["users"].setdefault(str(m.chat.id), {"joined": time.time()})
    rec.update({"name": u.full_name, "username": u.username or "", "last_seen": time.time()})
    save_stats()


def count_alert(tickers: list[str]) -> None:
    d = today()
    stats["alerts_total"] += 1
    stats["alerts_by_day"][d] = stats["alerts_by_day"].get(d, 0) + 1
    for t in tickers:
        stats["alerts_by_ticker"][t] = stats["alerts_by_ticker"].get(t, 0) + 1
    for k in sorted(stats["alerts_by_day"])[:-60]:   # храним 60 последних дней
        del stats["alerts_by_day"][k]
    save_stats()


# ---------------------------------------------------------------- parsing

SUFFIX_K = ("k", "к", "тыс", "тыс.")
SUFFIX_M = ("m", "м", "млн", "kk", "кк")
NUM_RE = r"\d+(?:[.,]\d+)?(?:тыс\.?|млн|kk|кк|k|к|m|м)?"


def normalize(text: str) -> str:
    t = text.lower().replace("$", "").replace("\u00a0", " ")
    t = re.sub(r"(?<=\d)\s(?=\d{3}(?!\d))", "", t)                 # 300 000 -> 300000
    t = re.sub(r"(?<=\d),(?=\d{3}(?!\d))", "", t)                  # 300,000 -> 300000
    t = re.sub(r"(?<=\d)\s+(?=(?:тыс|млн|kk|кк|k|к|m|м)(?![a-zа-я]))", "", t)  # 300 к -> 300к
    return t


def parse_money(s: str) -> float:
    s = normalize(s).strip().replace(",", ".")
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(тыс\.?|млн|kk|кк|k|к|m|м)?", s)
    if not m:
        raise ValueError(s)
    v = float(m.group(1))
    suf = m.group(2) or ""
    if suf in SUFFIX_K:
        v *= 1e3
    elif suf in SUFFIX_M:
        v *= 1e6
    return v


def parse_oi(text: str) -> tuple[float | None, float | None]:
    """Возвращает (min, max); None — не менять."""
    t = normalize(text)
    m = re.search(rf"({NUM_RE})\s*[-–—]\s*({NUM_RE})", t)
    if m:
        return parse_money(m.group(1)), parse_money(m.group(2))
    lo = re.search(rf"(?:от|from|>)\s*({NUM_RE})", t)
    hi = re.search(rf"(?:до|to|<|max)\s*({NUM_RE})", t)
    if lo or hi:
        return (parse_money(lo.group(1)) if lo else None, parse_money(hi.group(1)) if hi else None)
    m = re.search(NUM_RE, t)
    if m:
        return None, parse_money(m.group(0))
    raise ValueError(text)


def parse_spread(text: str) -> float:
    """Возвращает спред в процентах. '0.03' / '0,03%' -> 0.03, '3bps' / '3 бп' -> 0.03."""
    t = text.lower().strip()
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(%|bps|bp|бп|б\.п\.?)?", t)
    if not m:
        raise ValueError(text)
    v = float(m.group(1).replace(",", "."))
    if m.group(2) and m.group(2) != "%":
        v /= 100
    return v


def parse_free_text(text: str) -> dict:
    """Достаёт фильтры из свободной фразы: 'OI до 300к, спред 0.02'."""
    t = normalize(text)
    changes: dict = {}
    oi_m = re.search(r"(?:\boi\b|\bои\b|открыт\w*\s+интерес\w*)(.*?)(?=спред|spread|объ[её]м|volume|$)", t)
    if oi_m and re.search(r"\d", oi_m.group(1)):
        lo, hi = parse_oi(oi_m.group(1))
        if lo is not None:
            changes["min_oi"] = lo
        if hi is not None:
            changes["max_oi"] = hi
    sp_m = re.search(r"(?:спред\w*|spread)\D{0,15}?(\d+(?:[.,]\d+)?\s*(?:%|bps|bp|бп)?)", t)
    if sp_m:
        changes["max_spread_pct"] = parse_spread(sp_m.group(1))
    vol_m = re.search(rf"(?:объ[её]м\w*|volume)\D{{0,15}}?({NUM_RE})", t)
    if vol_m:
        changes["min_volume"] = parse_money(vol_m.group(1))
    if re.search(r"только\s+крипт|крипт\w*\s+только|only\s+crypto|crypto\s+only|без\s+акци", t):
        changes["types"] = "crypto"
    elif re.search(r"(все|любые)\s+рынк|all\s+markets", t):
        changes["types"] = "all"
    return changes


# ---------------------------------------------------------------- market logic

def fnum(x) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def quote_age_s(listing: dict) -> float | None:
    ts = (listing.get("quotes") or {}).get("updated_at")
    if not ts:
        return None
    ts = re.sub(r"(\.\d{6})\d+", r"\1", ts).replace("Z", "+00:00")  # наносекунды -> микросекунды
    try:
        return (datetime.now(timezone.utc) - datetime.fromisoformat(ts)).total_seconds()
    except ValueError:
        return None


def spread_pct(listing: dict, size: str) -> float | None:
    quotes = listing.get("quotes") or {}
    q = quotes.get(SIZE_KEYS[size])
    if q is None and size == "base":
        q = quotes.get("size_1k")
    if not q:
        return None
    bid, ask = fnum(q.get("bid")), fnum(q.get("ask"))
    if bid <= 0 or ask <= 0 or ask < bid:
        return None
    return (ask - bid) / ((ask + bid) / 2) * 100


def eff_spread(full: float | None, st: dict) -> float | None:
    """Спред, по которому фильтруем: как на сайте (половина bid-ask) или полный."""
    if full is None:
        return None
    return full / 2 if st.get("spread_mode", "site") == "site" else full


# ---- крипта или нет: в API нет категории, определяем по тикеру и названию
NON_CRYPTO_TICKERS = {
    # металлы, сырьё, токены на золото
    "XAU", "XAG", "XPT", "XPD", "COPPER", "CL", "BZ", "NATGAS", "PAXG", "XAUT",
    # pre-IPO компании
    "OPENAI", "ANTHROPIC", "SPACEX", "XAI", "STRIPE", "DATABRICKS",
}
CRYPTO_TICKERS = {"TWT", "GIGGLE"}   # криптотокены, чьё название похоже на компанию/фонд
CORP_RE = re.compile(
    r"\b(inc|corp|corporation|company|co|holdings?|limited|ltd|group|plc|n\.v|s\.a|s\.p\.a|a/s|oyj|ag|se|"
    r"etf|trust|fund|incorporated|common stock|depositary)\b", re.I)
COMMODITY_RE = re.compile(r"^(gold|silver|platinum|palladium|copper|natural gas|cocoa|.*\b(crude )?oil)$", re.I)
_crypto_cache: dict[tuple, bool] = {}


def is_crypto(listing: dict) -> bool:
    key = (listing.get("ticker", ""), listing.get("name", ""), listing.get("contract_type"))
    if key not in _crypto_cache:
        base = key[0].rsplit("-", 1)[0].upper()
        if key[2] == "swap":
            res = False
        elif base in CRYPTO_TICKERS:
            res = True
        elif base in NON_CRYPTO_TICKERS:
            res = False
        else:
            name = key[1].strip()
            res = not (CORP_RE.search(name) or COMMODITY_RE.match(name))
        _crypto_cache[key] = res
    return _crypto_cache[key]


def type_ok(listing: dict, types: str) -> bool:
    ct = listing.get("contract_type")
    return {
        "all": True,
        "perp": ct == "perpetual",
        "swap": ct == "swap",
        "crypto": is_crypto(listing),
        "tradfi": not is_crypto(listing),
    }.get(types, True)


TYPE_LABELS = {"all": "все", "crypto": "только крипта", "tradfi": "только акции/сырьё/индексы",
               "perp": "только PERP", "swap": "только SWAP"}


def total_oi(listing: dict) -> float:
    oi = listing.get("open_interest") or {}
    return fnum(oi.get("long_open_interest")) + fnum(oi.get("short_open_interest"))


def matches(listings: list[dict], st: dict) -> list[dict]:
    out = []
    for l in listings:
        if not type_ok(l, st["types"]):
            continue
        oi = total_oi(l)
        if oi > st["max_oi"] or oi < st["min_oi"]:
            continue
        vol = fnum(l.get("volume_24h"))
        if vol < st["min_volume"]:
            continue
        full = spread_pct(l, st["size"])
        sp = eff_spread(full, st)
        if sp is None or sp > st["max_spread_pct"]:
            continue
        age = quote_age_s(l)
        if age is not None and age > MAX_QUOTE_AGE:
            continue
        out.append({"ticker": l.get("ticker", "?"), "spread": sp, "full": full, "oi": oi, "vol": vol,
                    "price": l.get("mark_price"), "age": age, "listing": l})
    out.sort(key=lambda r: r["spread"])
    return out


def usd(x: float) -> str:
    if x >= 1e6:
        return f"${x / 1e6:.2f}M"
    if x >= 1e3:
        return f"${x / 1e3:.1f}k"
    return f"${x:.0f}"


def fmt_row(r: dict) -> str:
    oi = r["listing"].get("open_interest") or {}
    age = f" · котир. {int(r['age'])}с назад" if r["age"] is not None else ""
    return (f"<b>{r['ticker']}</b> — спред <b>{r['spread']:.4f}%</b> (bid-ask {r['full']:.4f}%)\n"
            f"   OI {usd(r['oi'])} (L {usd(fnum(oi.get('long_open_interest')))} / "
            f"S {usd(fnum(oi.get('short_open_interest')))}) · объём 24ч {usd(r['vol'])}{age}")


def fmt_settings(st: dict) -> str:
    oi = f"{usd(st['min_oi'])} – {usd(st['max_oi'])}" if st["min_oi"] > 0 else f"до {usd(st['max_oi'])}"
    types = TYPE_LABELS.get(st["types"], st["types"])
    return (f"{'🟢 Алерты включены' if st['enabled'] else '🔴 Алерты выключены'}\n"
            f"OI: {oi}\n"
            f"Спред: ≤ {st['max_spread_pct']:g}% "
            f"({'как на сайте' if st.get('spread_mode', 'site') == 'site' else 'полный bid-ask'})\n"
            f"Размер котировки: {st['size']}\n"
            f"Мин. объём 24ч: {usd(st['min_volume'])}\n"
            f"Рынки: {types}\n"
            f"Повтор алерта по паре: не чаще раза в {st['cooldown_min']} мин")


# ---------------------------------------------------------------- telegram

bot: Bot
dp = Dispatcher()

HELP = """<b>Бот низкого спреда на Variational Omni</b>

Просто напиши, что искать:
<i>ищи OI до 300к и спред 0.02</i>
<i>OI от 50к до 500к, спред 3 bps</i>

Или командами:
/oi 300k — макс. OI (можно 50k-300k)
/spread 0.02 — макс. спред в % (или 2bps)
/vol 10k — мин. объём за 24ч
/size base — котировка: base, 1k, 100k, 1m
/mode site — спред как на сайте (site) или полный bid-ask (full)
/type crypto — только криптовалюта (all — все, tradfi — акции/сырьё)
/cooldown 30 — минут между повторами по паре
/scan — что подходит прямо сейчас
/pair BCH — данные по паре и почему она не проходит фильтр (или просто напиши тикер)
/settings — текущие фильтры
/on, /off — включить / выключить алерты
/stop — отписаться и удалить настройки

У каждого пользователя свои фильтры."""


def allowed(m: Message) -> bool:
    ok = not ALLOWED_USERS or (m.from_user and m.from_user.id in ALLOWED_USERS)
    if ok:
        touch_user(m)
    return ok


def fmt_ago(ts: float) -> str:
    s = int(time.time() - ts)
    if s < 60:
        return f"{s}с"
    if s < 3600:
        return f"{s // 60}м"
    if s < 86400:
        return f"{s // 3600}ч {s % 3600 // 60}м"
    return f"{s // 86400}д"


def fmt_stats() -> str:
    now = time.time()
    users = stats["users"]
    subs = len(settings)
    enabled = sum(1 for st in settings.values() if st["enabled"])
    new_24 = sum(1 for u in users.values() if now - u.get("joined", 0) < 86400)
    act_24 = sum(1 for u in users.values() if now - u.get("last_seen", 0) < 86400)
    act_7 = sum(1 for u in users.values() if now - u.get("last_seen", 0) < 7 * 86400)
    days = stats["alerts_by_day"]
    d0 = datetime.now(TZ)
    week = sum(days.get((d0 - timedelta(days=i)).strftime("%Y-%m-%d"), 0) for i in range(7))
    top = sorted(stats["alerts_by_ticker"].items(), key=lambda kv: -kv[1])[:5]
    recent = sorted(users.items(), key=lambda kv: -kv[1].get("joined", 0))[:5]

    def who(cid: str, u: dict) -> str:
        name = f"@{u['username']}" if u.get("username") else (u.get("name") or cid)
        return f"{name} ({fmt_ago(u.get('joined', now))} назад)"

    data_age = f"{int(now - last_fetch_ts)}с назад" if last_fetch_ts else "ещё не было"
    lines = [
        "📈 <b>Статистика бота</b>",
        "",
        "<b>Пользователи</b>",
        f"Всего: {len(users)} · подписаны: {subs} · алерты вкл.: {enabled}",
        f"Новых за 24ч: {new_24} · активных за 24ч: {act_24} · за 7 дней: {act_7}",
        f"Заблокировали бота: {stats.get('blocked', 0)}",
        "",
        "<b>Алерты</b> (сообщений)",
        f"Сегодня: {days.get(today(), 0)} · за 7 дней: {week} · всего: {stats['alerts_total']}",
    ]
    if top:
        lines.append("Чаще всего: " + ", ".join(f"{t.replace('-PERP', '').replace('-SWAP', ' swap')} ({n})" for t, n in top))
    if recent:
        lines += ["", "<b>Новые пользователи</b>"] + [who(c, u) for c, u in recent]
    lines += [
        "",
        "<b>Работа бота</b>",
        f"Аптайм сеанса: {fmt_ago(STARTED_AT)} · опрос раз в {POLL_SECONDS:g}с",
        f"Данные Variational: {data_age} · рынков: {len(last_listings)}",
        f"Запросов к API за сеанс: {session_stats['api_ok']} ок / {session_stats['api_err']} ошибок",
    ]
    if session_stats["last_err"]:
        lines.append(f"Последняя ошибка: {session_stats['last_err'][:120]}")
    return "\n".join(lines)


async def apply_changes(m: Message, changes: dict) -> None:
    st = get_st(m.chat.id)
    st.update(changes)
    if st["min_oi"] > st["max_oi"]:
        st["min_oi"], st["max_oi"] = st["max_oi"], st["min_oi"]
    save_settings()
    reset_runtime(m.chat.id)
    await m.answer("✅ Обновил фильтры\n\n" + fmt_settings(st))
    if last_listings:
        await send_scan(m, st)


async def send_scan(m: Message, st: dict) -> None:
    res = matches(last_listings, st)
    if not res:
        await m.answer(f"Сейчас под фильтры ничего не попадает (рынков всего: {len(last_listings)}).")
        return
    body = "\n".join(fmt_row(r) for r in res[:20])
    more = f"\n…и ещё {len(res) - 20}" if len(res) > 20 else ""
    await m.answer(f"🔎 Сейчас подходит: {len(res)}\n\n{body}{more}")


@dp.message(CommandStart())
@dp.message(Command("help"))
async def cmd_start(m: Message):
    if not allowed(m):
        return await m.answer(f"Нет доступа. Твой id: {m.from_user.id}")
    get_st(m.chat.id)
    await m.answer(HELP + "\n\n" + fmt_settings(get_st(m.chat.id)))


@dp.message(Command("settings"))
async def cmd_settings(m: Message):
    if allowed(m):
        await m.answer(fmt_settings(get_st(m.chat.id)))


@dp.message(Command("oi"))
async def cmd_oi(m: Message, command: CommandObject):
    if not allowed(m):
        return
    try:
        lo, hi = parse_oi(command.args or "")
    except ValueError:
        return await m.answer("Пример: /oi 300k или /oi 50k-300k")
    ch = {}
    if hi is not None:
        ch["max_oi"] = hi
    ch["min_oi"] = lo if lo is not None else 0.0
    await apply_changes(m, ch)


@dp.message(Command("spread"))
async def cmd_spread(m: Message, command: CommandObject):
    if not allowed(m):
        return
    try:
        await apply_changes(m, {"max_spread_pct": parse_spread(command.args or "")})
    except ValueError:
        await m.answer("Пример: /spread 0.02 (в процентах) или /spread 2bps")


@dp.message(Command("vol"))
async def cmd_vol(m: Message, command: CommandObject):
    if not allowed(m):
        return
    try:
        await apply_changes(m, {"min_volume": parse_money(command.args or "")})
    except ValueError:
        await m.answer("Пример: /vol 10k (0 — без фильтра)")


@dp.message(Command("size"))
async def cmd_size(m: Message, command: CommandObject):
    if not allowed(m):
        return
    v = (command.args or "").strip().lower()
    if v not in SIZE_KEYS:
        return await m.answer("Варианты: /size base, /size 1k, /size 100k, /size 1m")
    await apply_changes(m, {"size": v})


@dp.message(Command("type"))
async def cmd_type(m: Message, command: CommandObject):
    if not allowed(m):
        return
    v = (command.args or "").strip().lower()
    v = {"крипта": "crypto", "crypto": "crypto", "все": "all"}.get(v, v)
    if v not in TYPE_LABELS:
        return await m.answer("Варианты:\n/type crypto — только криптовалюта\n/type tradfi — только акции, сырьё, индексы\n"
                              "/type all — все рынки\n/type perp или /type swap — по типу контракта")
    await apply_changes(m, {"types": v})


@dp.message(Command("mode"))
async def cmd_mode(m: Message, command: CommandObject):
    if not allowed(m):
        return
    v = (command.args or "").strip().lower()
    if v not in ("site", "full"):
        return await m.answer("Варианты:\n/mode site — спред как на сайте Variational (по умолчанию)\n"
                              "/mode full — полный спред bid-ask (в 2 раза больше)")
    await apply_changes(m, {"spread_mode": v})


@dp.message(Command("cooldown"))
async def cmd_cooldown(m: Message, command: CommandObject):
    if not allowed(m):
        return
    try:
        v = max(0, int((command.args or "").strip()))
    except ValueError:
        return await m.answer("Пример: /cooldown 30 (минут)")
    await apply_changes(m, {"cooldown_min": v})


@dp.message(Command("on"))
async def cmd_on(m: Message):
    if allowed(m):
        await apply_changes(m, {"enabled": True})


@dp.message(Command("off"))
async def cmd_off(m: Message):
    if allowed(m):
        st = get_st(m.chat.id)
        st["enabled"] = False
        save_settings()
        await m.answer("🔴 Алерты выключены. /on — включить обратно.")


@dp.message(Command("scan"))
async def cmd_scan(m: Message):
    if not allowed(m):
        return
    if not last_listings:
        return await m.answer("Данные ещё не загружены, подожди пару секунд.")
    await send_scan(m, get_st(m.chat.id))


def find_listings(query: str) -> list[dict]:
    q = query.strip().upper().replace("/", "").replace("$", "")
    exact = [l for l in last_listings if l.get("ticker", "").upper().split("-")[0] == q]
    return exact or [l for l in last_listings if l.get("ticker", "").upper().startswith(q)][:5]


def pair_report(l: dict, st: dict) -> str:
    oi = l.get("open_interest") or {}
    total = total_oi(l)
    vol = fnum(l.get("volume_24h"))
    age = quote_age_s(l)
    lines = [f"📊 <b>{l.get('ticker')}</b> ({l.get('name', '')})",
             f"Категория: {'криптовалюта' if is_crypto(l) else 'акции / сырьё / индексы'}",
             f"Цена: {l.get('mark_price')}",
             f"OI: {usd(total)} (L {usd(fnum(oi.get('long_open_interest')))} / S {usd(fnum(oi.get('short_open_interest')))})",
             f"Объём 24ч: {usd(vol)}",
             f"Котировки обновлены: {int(age)}с назад" if age is not None else "Котировок нет",
             "",
             "Спред по размерам (как на сайте / полный bid-ask):"]
    for size in SIZE_KEYS:
        sp = spread_pct(l, size)
        mark = " ← считаю по нему" if size == st["size"] else ""
        lines.append(f"  {size}: {sp / 2:.4f}% / {sp:.4f}%{mark}" if sp is not None else f"  {size}: нет котировки{mark}")

    reasons = []
    ct = l.get("contract_type")
    if not type_ok(l, st["types"]):
        kind = "криптовалюта" if is_crypto(l) else "не крипта (акции/сырьё/индексы)"
        reasons.append(f"это {kind}, а в фильтре «{TYPE_LABELS.get(st['types'], st['types'])}»")
    if total > st["max_oi"]:
        reasons.append(f"OI {usd(total)} больше лимита {usd(st['max_oi'])}")
    if total < st["min_oi"]:
        reasons.append(f"OI {usd(total)} меньше минимума {usd(st['min_oi'])}")
    if vol < st["min_volume"]:
        reasons.append(f"объём {usd(vol)} меньше минимума {usd(st['min_volume'])}")
    sp = eff_spread(spread_pct(l, st["size"]), st)
    if sp is None:
        reasons.append(f"нет котировки размера {st['size']}")
    elif sp > st["max_spread_pct"]:
        reasons.append(f"спред {sp:.4f}% больше лимита {st['max_spread_pct']:g}%")
    if age is not None and age > MAX_QUOTE_AGE:
        reasons.append(f"котировка устарела ({int(age // 60)} мин)")
    lines.append("")
    lines.append("✅ Проходит фильтр" if not reasons else "❌ Не проходит: " + "; ".join(reasons))
    return "\n".join(lines)


async def send_pair(m: Message, query: str) -> bool:
    found = find_listings(query)
    if not found:
        return False
    st = get_st(m.chat.id)
    for l in found[:3]:
        await m.answer(pair_report(l, st))
    return True


@dp.message(Command("pair"))
async def cmd_pair(m: Message, command: CommandObject):
    if not allowed(m):
        return
    if not command.args:
        return await m.answer("Пример: /pair BCH")
    if not last_listings:
        return await m.answer("Данные ещё не загружены, подожди пару секунд.")
    if not await send_pair(m, command.args):
        await m.answer(f"Пары «{command.args}» нет в API Variational.")


@dp.message(Command("stats"))
async def cmd_stats(m: Message):
    uid = m.from_user.id if m.from_user else 0
    if not ADMIN_IDS:
        return await m.answer("Статистика доступна только владельцу. Чтобы включить её, добавь в GitHub секрет "
                              f"<code>ADMIN_ID</code> со значением <code>{uid}</code> и перезапусти бота.")
    if uid not in ADMIN_IDS:
        return
    await m.answer(fmt_stats())


@dp.message(Command("stop"))
async def cmd_stop(m: Message):
    settings.pop(m.chat.id, None)
    runtime.pop(m.chat.id, None)
    save_settings()
    save_state()
    await m.answer("Отписал тебя от алертов, настройки удалены. /start — начать заново.")


# Свободный текст — только в личке, чтобы в группах бот не отвечал на всё подряд
@dp.message(F.text, F.chat.type == "private")
async def free_text(m: Message):
    if not allowed(m):
        return
    try:
        changes = parse_free_text(m.text)
    except ValueError:
        changes = {}
    if not changes:
        # просто тикер («BCH») — показываем инфу по паре
        if re.fullmatch(r"[A-Za-z0-9$/-]{2,15}", m.text.strip()) and last_listings and await send_pair(m, m.text):
            return
        return await m.answer("Не понял фильтр 🤔 Напиши, например: <i>OI до 300к, спред 0.02</i>\n"
                              "Или тикер, например <i>BCH</i>, — покажу данные по паре.\n/help — все команды")
    await apply_changes(m, changes)


# ---------------------------------------------------------------- poller

async def fetch(session: aiohttp.ClientSession) -> list[dict] | None:
    async with session.get(API_URL, timeout=aiohttp.ClientTimeout(total=20)) as r:
        if r.status == 429:
            body = await r.json(content_type=None)
            wait = float(body.get("retry_after_seconds", 10))
            log.warning("429, жду %.0fс", wait)
            await asyncio.sleep(wait)
            return None
        r.raise_for_status()
        data = await r.json(content_type=None)
        return data.get("listings", [])


async def poller():
    global last_listings, last_fetch_ts
    async with aiohttp.ClientSession(headers={"User-Agent": "variational-spread-bot/1.0"}) as session:
        while True:
            try:
                listings = await fetch(session)
                if listings is not None:
                    last_listings, last_fetch_ts = listings, time.time()
                    session_stats["api_ok"] += 1
                    await process_alerts()
            except Exception as e:  # noqa: BLE001
                session_stats["api_err"] += 1
                session_stats["last_err"] = f"{type(e).__name__}: {e}"
                log.warning("Ошибка опроса: %s", e)
            await asyncio.sleep(POLL_SECONDS)


async def process_alerts():
    now = time.time()
    for chat_id, st in list(settings.items()):
        if not st["enabled"]:
            continue
        rt = runtime.setdefault(chat_id, {"active": set(), "last_alert": {}})
        res = matches(last_listings, st)
        current = {r["ticker"] for r in res}
        fresh = [r for r in res
                 if r["ticker"] not in rt["active"]
                 and now - rt["last_alert"].get(r["ticker"], 0) >= st["cooldown_min"] * 60]
        changed = current != rt["active"]
        rt["active"] = current
        for r in fresh:
            rt["last_alert"][r["ticker"]] = now
        if changed or fresh:
            save_state()
        if not fresh:
            continue
        body = "\n".join(fmt_row(r) for r in fresh[:15])
        more = f"\n…и ещё {len(fresh) - 15}" if len(fresh) > 15 else ""
        try:
            await bot.send_message(chat_id, f"🔔 <b>Низкий спред</b> ({len(fresh)})\n\n{body}{more}")
            count_alert([r["ticker"] for r in fresh])
        except TelegramForbiddenError:
            # пользователь заблокировал бота — больше ему не пишем
            log.info("Чат %s заблокировал бота, удаляю", chat_id)
            settings.pop(chat_id, None)
            runtime.pop(chat_id, None)
            stats["blocked"] = stats.get("blocked", 0) + 1
            save_settings()
            save_state()
            save_stats()
        except Exception as e:  # noqa: BLE001
            log.warning("Не отправил в %s: %s", chat_id, e)
        await asyncio.sleep(0.05)  # не упираться в лимиты Telegram при многих пользователях


async def main():
    global bot
    if not BOT_TOKEN:
        raise SystemExit("Укажи BOT_TOKEN в файле .env")
    if not ALLOWED_USERS:
        log.info("Публичный режим: ботом может пользоваться любой, у каждого свои фильтры")
    load_settings()
    load_state()
    load_stats()
    for cid in settings:   # те, кто подписался до появления статистики
        stats["users"].setdefault(str(cid), {})
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    asyncio.create_task(poller())
    if RUN_UNTIL:
        asyncio.create_task(stop_at_deadline())
    log.info("Бот запущен, опрос каждые %sс", POLL_SECONDS)
    await dp.start_polling(bot)
    log.info("Бот остановлен")


async def stop_at_deadline():
    """Штатно выходим до лимита GitHub Actions, чтобы следующий запуск подхватил работу."""
    await asyncio.sleep(max(0.0, RUN_UNTIL - time.time()))
    log.info("Плановый перезапуск")
    await dp.stop_polling()


if __name__ == "__main__":
    asyncio.run(main())
