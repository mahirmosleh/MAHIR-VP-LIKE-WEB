# app.py — MAHIR VIP LIKE — Complete Backend with Master Panel + Info DB
# ==========================================================
#  Web:
#    /              → User UI
#    /master        → Master Admin Panel
#
#  Public APIs:
#    /mahir&like?uid={uid}&key={key}                    ← SHORT (default BD)
#    /mahir&like?uid={uid}&key={key}&server_name=IND    ← SHORT + server
#    /like?uid={uid}&server_name={server}&key={key}     ← FULL
#    /health
#    /auto/list
#    /cron/auto_like?secret={secret}
#
#  Master APIs (key required):
#    GET    /master/api/files
#    GET    /master/api/file?name=auto.txt
#    POST   /master/api/file
#    POST   /master/api/upload
#    POST   /master/api/auto/bulk-upload
#    POST   /master/api/auto/add
#    POST   /master/api/auto/remove
#    POST   /master/api/auto/clear
#    GET    /master/api/auto-with-info      ← NEW: auto targets + DB info
#    GET    /master/api/player-info?uid=    ← NEW: player info from DB/live
#    GET    /master/api/usage-detail?date=  ← NEW
#    GET    /master/api/blocked             ← NEW
#    POST   /master/api/block               ← NEW
#    POST   /master/api/unblock             ← NEW
#    GET    /master/api/keys
#    POST   /master/api/keys
#    GET    /master/api/stats
#    POST   /master/api/run-auto
#    POST   /master/api/jwt-refresh
# ==========================================================

import os
import sys
import json
import time
import sqlite3
import binascii
import asyncio
import threading
from threading import RLock
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import (Flask, request, jsonify, Response, render_template,
                   send_from_directory, abort)
from flask_cors import CORS
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from google.protobuf.json_format import MessageToJson

import requests
import aiohttp
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    import orjson
    def _jsonify(data, status=200):
        return Response(orjson.dumps(data), status=status,
                        mimetype='application/json')
except ImportError:
    def _jsonify(data, status=200):
        return Response(json.dumps(data, separators=(',', ':'),
                                   ensure_ascii=False),
                        status=status, mimetype='application/json')

import like_pb2
import like_count_pb2
import uid_generator_pb2

app = Flask(__name__)
CORS(app)

# ============================================================
#  BRANDING
# ============================================================
OWNER_HANDLE = "TG: @THEMAHIRWORLD"
DEV_NAME     = "MAHIR"
TELEGRAM     = "@THEMAHIRWORLD"
TIKTOK       = "MAHIR__222"
WEBSITE      = "MAHIR.XO.JE"
BRAND_NAME   = "MAHIR — Like API"
BADGE_TEXT   = "MAHIR • XO • JE"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(BASE_DIR, "mahir_info.db")

# ============================================================
#  CONFIG
# ============================================================
JWT_API_BASE      = "https://mahir-jwt-generator.vercel.app/token"
JWT_WORKERS       = 100
LIKE_CONCUR       = 200
JWT_REFRESH_HOURS = 7
AUTO_LIKE_HOUR    = 4
AUTO_LIKE_MINUTE  = 10
DAILY_LIMIT_USER  = 1
INFO_FETCH_HOUR   = 4      # info fetch hour (after auto-like)
INFO_FETCH_MINUTE = 30     # 04:30

SERVER_ACCOUNT_FILES = {
    "BD":  "account_bd.txt",
    "IND": "account_ind.txt",
    "BR":  "account_br.txt",
    "US":  "account_us.txt",
    "SAC": "account_sac.txt",
    "NA":  "account_na.txt",
}

EDITABLE_FILES = [
    "auto.txt",
    "keys.json",
    "account_bd.txt",
    "account_ind.txt",
    "account_br.txt",
    "account_us.txt",
    "account_sac.txt",
    "account_na.txt",
]

CONFIG_RO_PATH = os.path.join(BASE_DIR, "keys.json")
CONFIG_RW_PATH = os.path.join("/tmp", "keys.json")
USAGE_PATH     = os.path.join("/tmp", "mahir_usage.json")
AUTO_FILE      = os.path.join(BASE_DIR, "auto.txt")

config_lock = RLock()
usage_lock  = RLock()
jwt_lock    = RLock()
file_lock   = RLock()
db_lock     = RLock()

# ============================================================
#  CONFIG LOADER
# ============================================================
def _active_config_path():
    return CONFIG_RW_PATH if os.path.exists(CONFIG_RW_PATH) else CONFIG_RO_PATH


def _read_config():
    path = _active_config_path()
    if not os.path.exists(path):
        return {"ALLOWED_KEYS": {}, "ADMIN_KEYS": [], "RESET_TZ": "Asia/Dhaka"}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_allowed_keys():
    with config_lock:
        return _read_config().get("ALLOWED_KEYS", {})


def get_admin_keys():
    with config_lock:
        return set(_read_config().get("ADMIN_KEYS", []))


def get_reset_tz():
    with config_lock:
        return _read_config().get("RESET_TZ", "Asia/Dhaka")


def classify_key(api_key):
    if not api_key:
        return "invalid"
    try:
        if api_key in get_admin_keys():
            return "master"
        if api_key in get_allowed_keys():
            return "user"
    except Exception:
        pass
    return "invalid"


# ============================================================
#  TIME HELPERS
# ============================================================
def _now_local():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(get_reset_tz()))
    except Exception:
        return datetime.now()


def _today_str():
    return _now_local().strftime("%Y-%m-%d")


def _reset_at_str():
    nxt = (_now_local() + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    return nxt.strftime("%Y-%m-%d %H:%M")


# ============================================================
#  SQLITE INFO DATABASE
# ============================================================
def _db_connect():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db_lock:
        conn = _db_connect()
        c = conn.cursor()
        # Daily player info snapshots
        c.execute("""
            CREATE TABLE IF NOT EXISTS player_info (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uid TEXT NOT NULL,
                server TEXT NOT NULL,
                date TEXT NOT NULL,
                nickname TEXT,
                level INTEGER,
                likes INTEGER,
                region TEXT,
                clan_name TEXT,
                clan_id TEXT,
                clan_level INTEGER,
                clan_leader_name TEXT,
                clan_leader_uid TEXT,
                rank INTEGER,
                cs_rank INTEGER,
                credit_score INTEGER,
                head_pic INTEGER,
                signature TEXT,
                last_login TEXT,
                created TEXT,
                account_age TEXT,
                raw_json TEXT,
                source TEXT,
                likes_given INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                UNIQUE(uid, date)
            )
        """)
        # Like activity per uid per day
        c.execute("""
            CREATE TABLE IF NOT EXISTS like_activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uid TEXT NOT NULL,
                date TEXT NOT NULL,
                api_key TEXT,
                server TEXT,
                likes_given INTEGER DEFAULT 0,
                requests INTEGER DEFAULT 0,
                last_at TEXT,
                nickname TEXT,
                UNIQUE(uid, date)
            )
        """)
        # Blocked UIDs
        c.execute("""
            CREATE TABLE IF NOT EXISTS blocked_uids (
                uid TEXT PRIMARY KEY,
                reason TEXT,
                blocked_at TEXT NOT NULL
            )
        """)
        # Last auto-like run summary
        c.execute("""
            CREATE TABLE IF NOT EXISTS run_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at TEXT NOT NULL,
                total_likes INTEGER DEFAULT 0,
                total_uids INTEGER DEFAULT 0,
                details TEXT
            )
        """)
        conn.commit()
        conn.close()


def db_save_player_info(uid, server, info, source="auto"):
    """Save a daily snapshot. Only ONE per uid per day (UNIQUE constraint)."""
    with db_lock:
        conn = _db_connect()
        c = conn.cursor()
        today = _today_str()
        c.execute("""
            INSERT INTO player_info
              (uid, server, date, nickname, level, likes, region,
               clan_name, clan_id, clan_level, clan_leader_name,
               clan_leader_uid, rank, cs_rank, credit_score, head_pic,
               signature, last_login, created, account_age, raw_json,
               source, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(uid, date) DO UPDATE SET
              nickname=excluded.nickname,
              level=excluded.level,
              likes=excluded.likes,
              region=excluded.region,
              clan_name=excluded.clan_name,
              clan_id=excluded.clan_id,
              clan_level=excluded.clan_level,
              clan_leader_name=excluded.clan_leader_name,
              clan_leader_uid=excluded.clan_leader_uid,
              rank=excluded.rank,
              cs_rank=excluded.cs_rank,
              credit_score=excluded.credit_score,
              head_pic=excluded.head_pic,
              signature=excluded.signature,
              last_login=excluded.last_login,
              created=excluded.created,
              account_age=excluded.account_age,
              raw_json=excluded.raw_json,
              source=excluded.source
        """, (
            str(uid), server, today,
            info.get("nickname"), info.get("level"), info.get("likes"),
            info.get("region"),
            info.get("clanName"), info.get("clanId"), info.get("clanLevel"),
            info.get("clanLeaderName"), info.get("clanLeaderUid"),
            info.get("rank"), info.get("csRank"), info.get("creditScore"),
            info.get("headPic"), info.get("signature"),
            info.get("lastLogin"), info.get("created"), info.get("accountAge"),
            json.dumps(info, ensure_ascii=False), source,
            datetime.now().isoformat()
        ))
        conn.commit()
        conn.close()


def db_get_player_info(uid, date=None):
    """Get player info for a specific date (default: today)."""
    with db_lock:
        conn = _db_connect()
        c = conn.cursor()
        if date:
            c.execute("SELECT * FROM player_info WHERE uid=? AND date=?", (str(uid), date))
        else:
            c.execute("SELECT * FROM player_info WHERE uid=? ORDER BY date DESC LIMIT 1", (str(uid),))
        row = c.fetchone()
        conn.close()
        return dict(row) if row else None


def db_get_player_history(uid, days=7):
    """Get daily history for a uid."""
    with db_lock:
        conn = _db_connect()
        c = conn.cursor()
        c.execute("""
            SELECT * FROM player_info WHERE uid=?
            ORDER BY date DESC LIMIT ?
        """, (str(uid), days))
        rows = [dict(r) for r in c.fetchall()]
        conn.close()
        return rows


def db_save_like_activity(uid, api_key, server, likes_given, requests_count, nickname):
    """Upsert today's like activity for a uid."""
    with db_lock:
        conn = _db_connect()
        c = conn.cursor()
        today = _today_str()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("""
            INSERT INTO like_activity (uid, date, api_key, server, likes_given, requests, last_at, nickname)
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(uid, date) DO UPDATE SET
              likes_given = likes_given + excluded.likes_given,
              requests = requests + excluded.requests,
              last_at = excluded.last_at,
              api_key = excluded.api_key,
              server = excluded.server,
              nickname = COALESCE(excluded.nickname, nickname)
        """, (str(uid), today, api_key, server, likes_given, requests_count, now, nickname))
        conn.commit()
        conn.close()


def db_get_like_activity(uid, date=None):
    with db_lock:
        conn = _db_connect()
        c = conn.cursor()
        if date:
            c.execute("SELECT * FROM like_activity WHERE uid=? AND date=?", (str(uid), date))
        else:
            c.execute("SELECT * FROM like_activity WHERE uid=? ORDER BY date DESC LIMIT 1", (str(uid),))
        row = c.fetchone()
        conn.close()
        return dict(row) if row else None


def db_is_blocked(uid):
    with db_lock:
        conn = _db_connect()
        c = conn.cursor()
        c.execute("SELECT 1 FROM blocked_uids WHERE uid=?", (str(uid),))
        r = c.fetchone()
        conn.close()
        return r is not None


def db_block_uid(uid, reason=""):
    with db_lock:
        conn = _db_connect()
        c = conn.cursor()
        c.execute("""
            INSERT INTO blocked_uids (uid, reason, blocked_at)
            VALUES (?,?,?)
            ON CONFLICT(uid) DO UPDATE SET reason=excluded.reason
        """, (str(uid), reason, datetime.now().isoformat()))
        conn.commit()
        conn.close()


def db_unblock_uid(uid):
    with db_lock:
        conn = _db_connect()
        c = conn.cursor()
        c.execute("DELETE FROM blocked_uids WHERE uid=?", (str(uid),))
        conn.commit()
        conn.close()


def db_get_blocked():
    with db_lock:
        conn = _db_connect()
        c = conn.cursor()
        c.execute("SELECT uid, reason, blocked_at FROM blocked_uids ORDER BY blocked_at DESC")
        rows = [dict(r) for r in c.fetchall()]
        conn.close()
        return rows


def db_save_run_summary(total_likes, total_uids, details=None):
    with db_lock:
        conn = _db_connect()
        c = conn.cursor()
        c.execute("""
            INSERT INTO run_summary (run_at, total_likes, total_uids, details)
            VALUES (?,?,?,?)
        """, (datetime.now().isoformat(), total_likes, total_uids,
              json.dumps(details or {}, ensure_ascii=False)))
        conn.commit()
        conn.close()


def db_get_last_run():
    with db_lock:
        conn = _db_connect()
        c = conn.cursor()
        c.execute("SELECT * FROM run_summary ORDER BY id DESC LIMIT 1")
        row = c.fetchone()
        conn.close()
        return dict(row) if row else None


# ============================================================
#  USAGE / QUOTA
# ============================================================
def _load_usage():
    with usage_lock:
        if not os.path.exists(USAGE_PATH):
            return {}
        try:
            with open(USAGE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}


def _save_usage(u):
    with usage_lock:
        try:
            with open(USAGE_PATH, "w", encoding="utf-8") as f:
                json.dump(u, f)
        except Exception:
            pass


def check_and_consume_quota(api_key, uid, tier):
    if tier in ("master", "auto"):
        return True, None, None

    today = _today_str()
    u = _load_usage()
    entry = u.get(api_key, {})
    if entry.get("date") != today:
        entry = {"date": today, "uids": []}

    uids = entry.get("uids", [])
    if uid in uids:
        return False, max(0, DAILY_LIMIT_USER - len(uids)), _reset_at_str()
    if len(uids) >= DAILY_LIMIT_USER:
        return False, 0, _reset_at_str()

    uids.append(uid)
    entry["uids"] = uids
    u[api_key] = entry
    _save_usage(u)
    return True, max(0, DAILY_LIMIT_USER - len(uids)), None


# ============================================================
#  ACCOUNT LOADER
# ============================================================
def _account_path(server_name):
    f = SERVER_ACCOUNT_FILES.get(server_name.upper())
    return os.path.join(BASE_DIR, f) if f else ""


def load_accounts(server_name):
    path = _account_path(server_name)
    if not path or not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln.startswith("#") or ":" not in ln:
                continue
            u, p = ln.split(":", 1)
            if u.strip() and p.strip():
                out.append((u.strip(), p.strip()))
    return out


# ============================================================
#  AUTO.TXT
# ============================================================
def load_auto_targets():
    if not os.path.exists(AUTO_FILE):
        return {srv: [] for srv in SERVER_ACCOUNT_FILES}
    result = {srv: [] for srv in SERVER_ACCOUNT_FILES}
    current = None
    with open(AUTO_FILE, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            if ln.startswith("[") and ln.endswith("]"):
                current = ln[1:-1].strip().upper()
                if current not in SERVER_ACCOUNT_FILES:
                    current = None
                continue
            if current and ln.isdigit():
                result[current].append(ln)
    for k in result:
        result[k] = list(dict.fromkeys(result[k]))
    return result


def save_auto_targets(targets):
    try:
        with open(AUTO_FILE, "w", encoding="utf-8") as f:
            for srv in SERVER_ACCOUNT_FILES:
                f.write(f"[{srv}]\n")
                for uid in targets.get(srv, []):
                    f.write(f"{uid}\n")
                f.write("\n")
    except Exception as e:
        print(f"[auto.txt] save error: {e}")


def register_auto_uid(server_name, uid):
    targets = load_auto_targets()
    srv = server_name.upper()
    targets.setdefault(srv, [])
    if uid not in targets[srv]:
        targets[srv].append(uid)
        save_auto_targets(targets)


# ============================================================
#  JWT
# ============================================================
def _fetch_single_jwt(uid, pw, timeout=15):
    url = f"{JWT_API_BASE}?uid={uid}&password={pw}"
    try:
        r = requests.get(url, timeout=timeout, verify=False)
        if r.status_code != 200:
            return uid, None
        try:
            j = r.json()
        except ValueError:
            txt = r.text.strip()
            return uid, txt if txt and len(txt) > 50 else None
        token = None
        if isinstance(j, dict):
            token = (j.get("jwt_token") or j.get("token")
                     or j.get("access_token") or j.get("jwt"))
            if not token and isinstance(j.get("data"), dict):
                token = (j["data"].get("jwt_token") or j["data"].get("token")
                         or j["data"].get("jwt"))
        elif isinstance(j, str):
            token = j
        return uid, token
    except requests.RequestException:
        return uid, None


def generate_jwts(accounts):
    if not accounts:
        return []
    tokens = []
    with ThreadPoolExecutor(max_workers=JWT_WORKERS) as pool:
        futures = {pool.submit(_fetch_single_jwt, u, p): u for u, p in accounts}
        for fut in as_completed(futures):
            _, tok = fut.result()
            if tok:
                tokens.append(tok)
    return tokens


_jwt_cache = {}


def get_or_refresh_tokens(server_name, force=False):
    with jwt_lock:
        if not force:
            e = _jwt_cache.get(server_name)
            if e and time.time() - e["ts"] < JWT_REFRESH_HOURS * 3600:
                return e["tokens"]

        accounts = load_accounts(server_name)
        if not accounts:
            return []

        print(f"[JWT] refresh {server_name}: {len(accounts)} accounts")
        tokens = generate_jwts(accounts)
        print(f"[JWT] got {len(tokens)} tokens ({server_name})")
        if tokens:
            _jwt_cache[server_name] = {"tokens": tokens, "ts": time.time()}
        return tokens


# ============================================================
#  FREE FIRE HELPERS
# ============================================================
def _encrypt(plaintext):
    k = b'Yg&tc%DEuh6%Zc^8'
    iv = b'6oyZDr22E3ychjM%'
    return binascii.hexlify(
        AES.new(k, AES.MODE_CBC, iv).encrypt(pad(plaintext, AES.block_size))
    ).decode()


def _pb_msg(uid, region):
    m = like_pb2.like(); m.uid = int(uid); m.region = region
    return m.SerializeToString()


def _pb_uid(uid):
    m = uid_generator_pb2.uid_generator()
    m.krishna_ = int(uid); m.teamXdarks = 1
    return m.SerializeToString()


def enc(uid):
    return _encrypt(_pb_uid(uid))


def like_url_for(s):
    s = s.upper()
    if s == "IND":
        return "https://client.ind.freefiremobile.com/LikeProfile"
    if s in {"BR", "US", "SAC", "NA"}:
        return "https://client.us.freefiremobile.com/LikeProfile"
    return "https://clientbp.ppmainecoonghj.com/LikeProfile"


def show_url_for(s):
    s = s.upper()
    if s == "IND":
        return "https://client.ind.freefiremobile.com/GetPlayerPersonalShow"
    if s in {"BR", "US", "SAC", "NA"}:
        return "https://client.us.freefiremobile.com/GetPlayerPersonalShow"
    return "https://clientbp.ppmainecoonghj.com/GetPlayerPersonalShow"


HEADERS = {
    'User-Agent': "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)",
    'Connection': "Keep-Alive",
    'Accept-Encoding': "gzip",
    'Content-Type': "application/x-www-form-urlencoded",
    'Expect': "100-continue",
    'X-Unity-Version': "2018.4.11f1",
    'X-GA': "v1 1",
    'ReleaseVersion': "OB55",
}


async def _async_post(enc_uid, token, url, sess):
    h = dict(HEADERS); h["Authorization"] = f"Bearer {token}"
    try:
        async with sess.post(url, data=bytes.fromhex(enc_uid), headers=h) as r:
            return r.status
    except Exception:
        return 0


async def _burst(uid, server, url, tokens):
    enc_uid = _encrypt(_pb_msg(uid, server))
    conn = aiohttp.TCPConnector(limit=LIKE_CONCUR, ssl=False)
    sem = asyncio.Semaphore(LIKE_CONCUR)
    async with aiohttp.ClientSession(connector=conn) as s:
        async def w(t):
            async with sem:
                return await _async_post(enc_uid, t, url, s)
        return await asyncio.gather(*[w(t) for t in tokens])


def send_likes_from_all_tokens(uid, server, url, tokens):
    if not tokens:
        return 0
    try:
        res = asyncio.run(_burst(uid, server, url, tokens))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            res = loop.run_until_complete(_burst(uid, server, url, tokens))
        finally:
            loop.close()
    return sum(1 for s in res if s == 200)


def make_request(encrypted, server, token):
    h = dict(HEADERS); h["Authorization"] = f"Bearer {token}"
    try:
        r = requests.post(show_url_for(server), data=bytes.fromhex(encrypted),
                          headers=h, verify=False, timeout=30)
        obj = like_count_pb2.Info()
        obj.ParseFromString(r.content)
        return obj
    except Exception:
        return None


def parse_account_info(pb):
    try:
        if pb is None:
            return None
        js = json.loads(MessageToJson(pb))
        ai = js.get("AccountInfo", {})
        uid = int(ai.get("UID", 0))
        if uid <= 0:
            return None
        return {
            "uid": uid,
            "likes": int(ai.get("Likes", 0)),
            "name": str(ai.get("PlayerNickname", "")),
            "level": int(ai.get("Level", 0)),
            "region": str(ai.get("Region", "")),
            "headPic": int(ai.get("HeadPic", 0)),
            "exp": int(ai.get("Exp", 0)),
            "raw": js,
        }
    except Exception:
        return None


def parse_full_player_info(pb, uid, server):
    """Parse full player info from protobuf for DB storage."""
    try:
        if pb is None:
            return None
        js = json.loads(MessageToJson(pb))
        ai = js.get("AccountInfo", {})
        if not ai:
            return None

        def _int(v, d=0):
            try: return int(v)
            except: return d

        def _str(v, d=""):
            try: return str(v)
            except: return d

        return {
            "uid": _int(ai.get("UID"), uid),
            "nickname": _str(ai.get("PlayerNickname", "")),
            "level": _int(ai.get("Level", 0)),
            "likes": _int(ai.get("Likes", 0)),
            "region": _str(ai.get("Region", server)),
            "headPic": _int(ai.get("HeadPic", 0)),
            "rank": _int(ai.get("BrRankPoint", 0)),
            "csRank": _int(ai.get("CsRankPoint", 0)),
            "creditScore": _int(ai.get("CreditScore", 0)),
            "clanName": _str(ai.get("ClanName", "")),
            "clanId": _str(ai.get("ClanId", "")),
            "clanLevel": _int(ai.get("ClanLevel", 0)),
            "clanLeaderName": _str(ai.get("ClanLeaderName", "")),
            "clanLeaderUid": _str(ai.get("ClanLeaderUID", "")),
            "signature": _str(ai.get("Signature", "")),
            "lastLogin": _str(ai.get("LastLogin", "")),
            "created": _str(ai.get("CreateTime", "")),
            "accountAge": _str(ai.get("AccountAge", "")),
            "raw": js,
        }
    except Exception as e:
        print(f"[parse_full] err: {e}")
        return None


# ============================================================
#  CORE LIKE
# ============================================================
_api_key_ctx = threading.local()


def do_like(uid, server_name, tier="user"):
    api_key = "_auto_" if tier == "auto" else getattr(_api_key_ctx, "key", "")

    allowed, remaining, reset_at = check_and_consume_quota(api_key, uid, tier)
    if not allowed:
        return {
            "error": "Daily limit reached (1 UID/day for user keys).",
            "tier": tier.upper(),
            "quota_remaining": remaining,
            "reset_at": reset_at,
        }

    accounts = load_accounts(server_name)
    if not accounts:
        return {
            "error": f"No guest accounts for {server_name}",
            "hint": f"Add uid:pass into {SERVER_ACCOUNT_FILES[server_name]}",
        }

    tokens = get_or_refresh_tokens(server_name)
    if not tokens:
        return {"error": "Failed to generate JWT tokens."}

    token = tokens[0]
    encrypted = enc(uid)
    before = parse_account_info(make_request(encrypted, server_name, token))
    if before is None:
        return {
            "LikesGivenByAPI": 0, "LikesafterCommand": 0, "LikesbeforeCommand": 0,
            "PlayerNickname": "Unknown",
            "UID": int(uid) if str(uid).isdigit() else uid,
            "GiftCount": 0, "accounts_loaded": len(accounts),
            "tokens_generated": len(tokens), "server_name": server_name,
            "tier": tier.upper(), "quota_remaining": remaining,
            "Owner": OWNER_HANDLE, "status": 0,
        }

    url = like_url_for(server_name)
    send_likes_from_all_tokens(uid, server_name, url, tokens)

    after_pb = make_request(encrypted, server_name, token)
    after = parse_account_info(after_pb) or {
        "likes": before["likes"], "uid": before["uid"], "name": before["name"]
    }
    like_given = max(0, int(after["likes"]) - int(before["likes"]))

    # Log to DB
    try:
        db_save_like_activity(
            uid=uid, api_key=api_key, server=server_name,
            likes_given=like_given, requests_count=1,
            nickname=after.get("name")
        )
    except Exception as e:
        print(f"[db] like activity save err: {e}")

    return {
        "LikesGivenByAPI": like_given,
        "LikesafterCommand": int(after["likes"]),
        "LikesbeforeCommand": int(before["likes"]),
        "PlayerNickname": str(after["name"]),
        "UID": int(after["uid"]),
        "GiftCount": like_given * 2 if like_given > 0 else 0,
        "server_name": server_name,
        "accounts_loaded": len(accounts),
        "tokens_generated": len(tokens),
        "tier": tier.upper(),
        "quota_remaining": remaining,
        "Owner": OWNER_HANDLE,
        "status": 1 if like_given > 0 else 2,
    }


# ============================================================
#  INFO FETCH (once per day per UID)
# ============================================================
def fetch_and_store_info_for_server(srv, tokens, uids):
    """Fetch full player info for all auto UIDs on a server, store in DB.
    RULE: ONE fetch per UID per day — checked via DB date column."""
    if not tokens or not uids:
        return 0

    today = _today_str()
    token = tokens[0]
    url_show = show_url_for(srv)
    saved = 0

    for uid in uids:
        try:
            # Skip if already fetched today
            existing = db_get_player_info(uid, date=today)
            if existing:
                continue

            encrypted = enc(uid)
            pb = make_request(encrypted, srv, token)
            if pb is None:
                continue
            info = parse_full_player_info(pb, uid, srv)
            if not info:
                continue
            db_save_player_info(uid, srv, info, source="daily")
            saved += 1
            print(f"[INFO] {srv} {uid} → {info.get('nickname')} L{info.get('level')} ❤{info.get('likes')}")
            time.sleep(0.3)  # gentle rate-limit
        except Exception as e:
            print(f"[INFO] {srv} {uid} err: {e}")
            continue
    return saved


def do_daily_info_fetch():
    """Runs AFTER auto-like. Fetches info for every auto UID (once/day)."""
    print(f"\n[INFO-FETCH] start {datetime.now():%Y-%m-%d %H:%M:%S}")
    targets = load_auto_targets()
    total_saved = 0
    for srv, uids in targets.items():
        if not uids:
            continue
        try:
            tokens = get_or_refresh_tokens(srv)
            if not tokens:
                print(f"[INFO-FETCH] {srv}: no tokens")
                continue
            n = fetch_and_store_info_for_server(srv, tokens, uids)
            total_saved += n
            print(f"[INFO-FETCH] {srv}: saved {n}")
        except Exception as e:
            print(f"[INFO-FETCH] {srv} err: {e}")
    print(f"[INFO-FETCH] done. total saved={total_saved}\n")
    return total_saved


# ============================================================
#  AUTO-LIKE
# ============================================================
def do_auto_like_now():
    print(f"\n[AUTO-LIKE] start {datetime.now():%Y-%m-%d %H:%M:%S}")
    targets = load_auto_targets()
    total = 0
    total_uids = 0
    details = {}
    for srv, uids in targets.items():
        if not uids:
            continue
        try:
            tokens = get_or_refresh_tokens(srv, force=True)
            if not tokens:
                print(f"[AUTO-LIKE] {srv}: no tokens")
                continue
            url = like_url_for(srv)
            srv_count = 0
            for uid in uids:
                # skip blocked
                if db_is_blocked(uid):
                    continue
                try:
                    ok = send_likes_from_all_tokens(uid, srv, url, tokens)
                    total += ok
                    srv_count += ok
                    total_uids += 1
                    print(f"[AUTO-LIKE] {srv} {uid} → {ok}")
                except Exception as e:
                    print(f"[AUTO-LIKE] {srv} {uid} err: {e}")
                time.sleep(0.5)
            details[srv] = srv_count
        except Exception as e:
            print(f"[AUTO-LIKE] {srv} err: {e}")

    # Save summary
    try:
        db_save_run_summary(total, total_uids, details)
    except Exception as e:
        print(f"[db] run summary save err: {e}")

    print(f"[AUTO-LIKE] done. total={total}\n")

    # After like → fetch & store info (once per uid/day)
    try:
        do_daily_info_fetch()
    except Exception as e:
        print(f"[INFO-FETCH] err: {e}")

    return total


def _auto_like_scheduler():
    while True:
        try:
            now = datetime.now()
            target = now.replace(hour=AUTO_LIKE_HOUR, minute=AUTO_LIKE_MINUTE,
                                 second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            wait = (target - now).total_seconds()
            print(f"[SCHED] next auto-like {target:%Y-%m-%d %H:%M:%S}")
            time.sleep(wait)
            do_auto_like_now()
        except Exception as e:
            print(f"[SCHED] err: {e}")
            time.sleep(60)


def _jwt_refresh_scheduler():
    while True:
        try:
            time.sleep(JWT_REFRESH_HOURS * 3600)
            print(f"\n[JWT-REFRESH] {datetime.now():%Y-%m-%d %H:%M:%S}")
            for srv in SERVER_ACCOUNT_FILES:
                try:
                    toks = get_or_refresh_tokens(srv, force=True)
                    print(f"[JWT-REFRESH] {srv}: {len(toks)}")
                except Exception as e:
                    print(f"[JWT-REFRESH] {srv} err: {e}")
        except Exception as e:
            print(f"[JWT-REFRESH] err: {e}")
            time.sleep(60)


def start_background_jobs():
    threading.Thread(target=_auto_like_scheduler, daemon=True).start()
    threading.Thread(target=_jwt_refresh_scheduler, daemon=True).start()
    print("[*] Background jobs started")


# ============================================================
#  MASTER HELPERS
# ============================================================
def _master_required(api_key):
    return classify_key(api_key) == "master"


def _safe_file_path(name):
    if name not in EDITABLE_FILES:
        return None
    return os.path.join(BASE_DIR, name)


def _read_file(name):
    path = _safe_file_path(name)
    if not path or not os.path.exists(path):
        return ""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _write_file(name, content):
    path = _safe_file_path(name)
    if not path:
        raise ValueError("file not allowed")
    with file_lock:
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)


# ============================================================
#  ROUTES — USER
# ============================================================
@app.get("/")
def index():
    return render_template(
        "index.html",
        brand=BRAND_NAME, dev=DEV_NAME, tg=TELEGRAM,
        tiktok=TIKTOK, website=WEBSITE,
        owner=OWNER_HANDLE, badge=BADGE_TEXT,
    )


@app.get("/master")
def master_page():
    return render_template(
        "master.html",
        brand=BRAND_NAME, dev=DEV_NAME, tg=TELEGRAM,
        tiktok=TIKTOK, website=WEBSITE,
        owner=OWNER_HANDLE, badge=BADGE_TEXT,
        servers=list(SERVER_ACCOUNT_FILES.keys()),
    )


@app.get("/health")
def route_health():
    return _jsonify({
        "status": "ok",
        "service": BRAND_NAME,
        "dev": DEV_NAME,
        "website": WEBSITE,
        "tg": TELEGRAM,
        "tiktok": TIKTOK,
        "jwt_api": JWT_API_BASE,
        "servers": list(SERVER_ACCOUNT_FILES.keys()),
        "auto_like_at": f"{AUTO_LIKE_HOUR:02d}:{AUTO_LIKE_MINUTE:02d}",
        "info_fetch_at": f"{INFO_FETCH_HOUR:02d}:{INFO_FETCH_MINUTE:02d}",
        "jwt_refresh_hours": JWT_REFRESH_HOURS,
        "daily_limit_user": DAILY_LIMIT_USER,
        "auto_targets_loaded": {k: len(v) for k, v in load_auto_targets().items()},
        "db_path": DB_PATH,
        "endpoints": {
            "short": "/mahir&like?uid={uid}&key={key}",
            "short_server": "/mahir&like?uid={uid}&key={key}&server_name={server}",
            "full": "/like?uid={uid}&server_name={server}&key={key}",
            "master": "/master",
        },
    })


@app.get("/like")
def handle_like():
    try:
        uid = request.args.get("uid", "").strip()
        server_name = request.args.get("server_name", "").upper().strip()
        api_key = request.args.get("key", "").strip()

        tier = classify_key(api_key)
        if tier == "invalid":
            return jsonify({"error": "Invalid or missing API key"}), 403
        if not uid or not server_name:
            return jsonify({"error": "UID and server_name required"}), 400
        if server_name not in SERVER_ACCOUNT_FILES:
            return jsonify({"error": f"Unsupported server '{server_name}'"}), 400
        if db_is_blocked(uid):
            return jsonify({"error": "This UID is blocked"}), 403

        _api_key_ctx.key = api_key
        try:
            register_auto_uid(server_name, uid)
        except Exception as e:
            print(f"[auto-register] warn: {e}")

        result = do_like(uid, server_name, tier=tier)
        if result.get("error"):
            if "Daily limit" in result["error"]:
                return jsonify(result), 429
            return jsonify(result), 500
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": "runtime_error", "detail": str(e)}), 500


@app.get("/mahir&like")
def mahir_like():
    try:
        uid = request.args.get("uid", "").strip()
        api_key = request.args.get("key", "").strip()
        server_name = request.args.get("server_name", "").upper().strip()

        if not uid or not uid.isdigit():
            return jsonify({
                "error": "Valid numeric uid required",
                "example": "/mahir&like?uid=1234567890&key=MAHIR-USER-001",
                "Owner": OWNER_HANDLE
            }), 400

        tier = classify_key(api_key)
        if tier == "invalid":
            return jsonify({
                "error": "Invalid or missing API key",
                "example": "/mahir&like?uid=1234567890&key=MAHIR-USER-001",
                "Owner": OWNER_HANDLE
            }), 403

        if not server_name:
            server_name = "BD"
        if server_name not in SERVER_ACCOUNT_FILES:
            return jsonify({
                "error": f"Unsupported server '{server_name}'",
                "allowed": list(SERVER_ACCOUNT_FILES.keys()),
                "default": "BD"
            }), 400

        if db_is_blocked(uid):
            return jsonify({"error": "This UID is blocked", "Owner": OWNER_HANDLE}), 403

        _api_key_ctx.key = api_key
        try:
            register_auto_uid(server_name, uid)
        except Exception as e:
            print(f"[auto-register] warn: {e}")

        result = do_like(uid, server_name, tier=tier)
        if result.get("error"):
            if "Daily limit" in result["error"]:
                return jsonify(result), 429
            return jsonify(result), 500
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": "runtime_error", "detail": str(e)}), 500


@app.get("/auto/list")
def auto_list():
    return _jsonify({"auto_targets": load_auto_targets()})


@app.get("/cron/auto_like")
def cron_auto_like():
    secret = request.args.get("secret", "")
    expected = os.environ.get("CRON_SECRET", "mahir-cron-2025")
    if secret != expected:
        return jsonify({"error": "forbidden"}), 403
    threading.Thread(target=do_auto_like_now, daemon=True).start()
    return jsonify({"ok": True, "triggered": True})


@app.get("/cron/info_fetch")
def cron_info_fetch():
    secret = request.args.get("secret", "")
    expected = os.environ.get("CRON_SECRET", "mahir-cron-2025")
    if secret != expected:
        return jsonify({"error": "forbidden"}), 403
    threading.Thread(target=do_daily_info_fetch, daemon=True).start()
    return jsonify({"ok": True, "triggered": True})


# ============================================================
#  ROUTES — MASTER PANEL APIs
# ============================================================
@app.get("/master/api/files")
def master_files():
    key = request.args.get("key", "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    files = []
    for name in EDITABLE_FILES:
        path = os.path.join(BASE_DIR, name)
        exists = os.path.exists(path)
        size = os.path.getsize(path) if exists else 0
        lines = 0
        if exists and name.endswith(".txt"):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    lines = sum(1 for _ in f)
            except Exception:
                pass
        files.append({"name": name, "exists": exists, "size": size, "lines": lines})
    return _jsonify({"files": files})


@app.get("/master/api/file")
def master_get_file():
    key = request.args.get("key", "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    name = request.args.get("name", "").strip()
    if name not in EDITABLE_FILES:
        return jsonify({"error": "file not allowed"}), 400

    content = _read_file(name)
    return _jsonify({"name": name, "content": content})


@app.post("/master/api/file")
def master_save_file():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or request.args.get("key") or "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    name = (data.get("name") or "").strip()
    content = data.get("content", "")

    if name not in EDITABLE_FILES:
        return jsonify({"error": "file not allowed"}), 400

    if name == "keys.json":
        try:
            json.loads(content)
        except Exception as e:
            return jsonify({"error": f"invalid JSON: {e}"}), 400

    try:
        _write_file(name, content)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return _jsonify({"ok": True, "name": name, "size": len(content)})


@app.post("/master/api/upload")
def master_upload():
    key = (request.form.get("key") or request.args.get("key") or "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    f = request.files.get("file")
    if not f:
        return jsonify({"error": "no file uploaded"}), 400

    target_name = (request.form.get("name") or f.filename or "").strip()
    if target_name not in EDITABLE_FILES:
        return jsonify({
            "error": f"filename '{target_name}' not allowed",
            "allowed": EDITABLE_FILES,
        }), 400

    try:
        content = f.read().decode("utf-8", errors="replace")
    except Exception as e:
        return jsonify({"error": f"read error: {e}"}), 400

    if target_name == "keys.json":
        try:
            json.loads(content)
        except Exception as e:
            return jsonify({"error": f"invalid JSON: {e}"}), 400

    try:
        _write_file(target_name, content)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return _jsonify({"ok": True, "name": target_name, "size": len(content)})


# ============================================================
#  AUTO UID BULK UPLOAD
# ============================================================
@app.post("/master/api/auto/bulk-upload")
def master_auto_bulk_upload():
    key = (request.form.get("key")
           or (request.get_json(silent=True) or {}).get("key")
           or request.args.get("key")
           or "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    srv = (request.form.get("server_name")
           or (request.get_json(silent=True) or {}).get("server_name")
           or request.args.get("server_name")
           or "").upper().strip()
    if srv not in SERVER_ACCOUNT_FILES:
        return jsonify({
            "error": "valid server_name required",
            "allowed": list(SERVER_ACCOUNT_FILES.keys())
        }), 400

    mode = (request.form.get("mode")
            or (request.get_json(silent=True) or {}).get("mode")
            or "replace").strip().lower()
    if mode not in ("replace", "append"):
        mode = "replace"

    raw_text = ""
    f = request.files.get("file")
    if f:
        try:
            raw_text = f.read().decode("utf-8", errors="replace")
        except Exception as e:
            return jsonify({"error": f"file read error: {e}"}), 400
    else:
        data = request.get_json(silent=True) or {}
        raw_text = str(data.get("text") or request.form.get("text") or "")

    uids = []
    for chunk in raw_text.replace(",", "\n").replace(" ", "\n").replace(";", "\n").split("\n"):
        c = chunk.strip()
        if not c or c.startswith("#"):
            continue
        if c.startswith("[") and c.endswith("]"):
            continue
        if c.isdigit():
            uids.append(c)
        else:
            digits = "".join(ch for ch in c.split(":")[0] if ch.isdigit())
            if digits:
                uids.append(digits)

    if not uids:
        return jsonify({"error": "no valid UIDs found in input"}), 400

    uids = list(dict.fromkeys(uids))

    targets = load_auto_targets()
    if mode == "replace":
        targets[srv] = uids
    else:
        existing = targets.get(srv, [])
        for u in uids:
            if u not in existing:
                existing.append(u)
        targets[srv] = existing

    save_auto_targets(targets)

    return _jsonify({
        "ok": True,
        "server_name": srv,
        "mode": mode,
        "imported": len(uids),
        "total_for_server": len(targets[srv]),
        "auto_targets": targets,
    })


# ============================================================
#  AUTO UID — add / remove / clear
# ============================================================
@app.post("/master/api/auto/add")
def master_auto_add():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    uid = str(data.get("uid", "")).strip()
    srv = str(data.get("server_name", "")).upper().strip()
    if not uid.isdigit() or srv not in SERVER_ACCOUNT_FILES:
        return jsonify({"error": "uid (digits) and valid server_name required"}), 400

    register_auto_uid(srv, uid)

    # Try to fetch and store info immediately
    try:
        tokens = get_or_refresh_tokens(srv)
        if tokens:
            encrypted = enc(uid)
            pb = make_request(encrypted, srv, tokens[0])
            info = parse_full_player_info(pb, uid, srv)
            if info:
                db_save_player_info(uid, srv, info, source="manual")
    except Exception as e:
        print(f"[auto/add] info fetch warn: {e}")

    return _jsonify({"ok": True, "auto_targets": load_auto_targets()})


@app.post("/master/api/auto/remove")
def master_auto_remove():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    uid = str(data.get("uid", "")).strip()
    srv = str(data.get("server_name", "")).upper().strip()
    if not uid or srv not in SERVER_ACCOUNT_FILES:
        return jsonify({"error": "uid and valid server_name required"}), 400

    targets = load_auto_targets()
    if srv in targets and uid in targets[srv]:
        targets[srv].remove(uid)
        save_auto_targets(targets)

    return _jsonify({"ok": True, "auto_targets": targets})


@app.post("/master/api/auto/clear")
def master_auto_clear():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    srv = str(data.get("server_name", "")).upper().strip()

    targets = load_auto_targets()

    if not srv or srv == "ALL":
        for s in SERVER_ACCOUNT_FILES:
            targets[s] = []
    elif srv in SERVER_ACCOUNT_FILES:
        targets[srv] = []
    else:
        return jsonify({"error": f"invalid server_name '{srv}'"}), 400

    save_auto_targets(targets)
    return _jsonify({"ok": True, "cleared": srv or "ALL", "auto_targets": targets})


# ============================================================
#  AUTO WITH INFO (NEW)
# ============================================================
@app.get("/master/api/auto-with-info")
def master_auto_with_info():
    """
    Returns auto targets enriched with DB info per server.
    Frontend renders beautiful cards from this.
    """
    key = request.args.get("key", "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    targets = load_auto_targets()
    last_run = db_get_last_run()

    result = {}
    for srv, uids in targets.items():
        rows = []
        for uid in uids:
            info = db_get_player_info(uid)
            activity = db_get_like_activity(uid)
            blocked = db_is_blocked(uid)

            if info:
                rows.append({
                    "uid": uid,
                    "server": srv,
                    "nickname": info.get("nickname") or "Unknown",
                    "level": info.get("level") or 0,
                    "likes": info.get("likes") or 0,
                    "region": info.get("region") or srv,
                    "headPic": info.get("head_pic"),
                    "clanName": info.get("clan_name") or "",
                    "clanId": info.get("clan_id") or "",
                    "info_date": info.get("date"),
                    "source": info.get("source"),
                    "is_blocked": blocked,
                    "last_likes_given": (activity.get("likes_given") if activity else None),
                    "last_like_at": (activity.get("last_at") if activity else None),
                })
            else:
                rows.append({
                    "uid": uid,
                    "server": srv,
                    "nickname": "Unknown",
                    "level": 0,
                    "likes": 0,
                    "region": srv,
                    "headPic": None,
                    "clanName": "",
                    "clanId": "",
                    "info_date": None,
                    "source": None,
                    "is_blocked": blocked,
                    "last_likes_given": (activity.get("likes_given") if activity else None),
                    "last_like_at": (activity.get("last_at") if activity else None),
                })
        result[srv] = rows

    summary = {}
    if last_run:
        try:
            summary = {
                "last_run": last_run.get("run_at", "")[:16].replace("T", " "),
                "total_likes": last_run.get("total_likes", 0),
                "total_uids": last_run.get("total_uids", 0),
            }
        except Exception:
            pass

    return _jsonify({
        "auto_with_info": result,
        "last_run_summary": summary,
    })


# ============================================================
#  PLAYER INFO (NEW)
# ============================================================
@app.get("/master/api/player-info")
def master_player_info():
    key = request.args.get("key", "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    uid = request.args.get("uid", "").strip()
    if not uid.isdigit():
        return jsonify({"error": "valid uid required"}), 400

    info = db_get_player_info(uid)
    history = db_get_player_history(uid, days=14)
    activity = db_get_like_activity(uid)
    blocked = db_is_blocked(uid)

    # Find server for this uid from auto targets
    targets = load_auto_targets()
    srv = ""
    for s, uids in targets.items():
        if uid in uids:
            srv = s
            break

    if not info and not srv:
        return jsonify({"error": "no info found for this UID"}), 404

    # If no DB info, try live fetch
    if not info and srv:
        try:
            tokens = get_or_refresh_tokens(srv)
            if tokens:
                encrypted = enc(uid)
                pb = make_request(encrypted, srv, tokens[0])
                live = parse_full_player_info(pb, uid, srv)
                if live:
                    db_save_player_info(uid, srv, live, source="live")
                    info = db_get_player_info(uid)
        except Exception as e:
            print(f"[player-info live] err: {e}")

    if not info:
        return jsonify({"error": "no info available"}), 404

    # Build response
    avatarUrl = None
    if info.get("head_pic"):
        avatarUrl = f"https://cdn.jsdelivr.net/gh/ShahGCreator/icon@main/PNG/{info['head_pic']}.png"

    # Activity history
    today_like_activity = []
    if activity:
        today_like_activity.append({
            "api_key": activity.get("api_key") or "—",
            "likes_given_total": activity.get("likes_given", 0),
            "requests": activity.get("requests", 0),
            "last_at": activity.get("last_at", ""),
        })

    # Daily snapshots
    history_days = []
    history_map = {}
    for h in history:
        d = h.get("date")
        if d:
            history_days.append(d)
            history_map[d] = {
                "snapshots": [{
                    "snapshot": {
                        "likes": h.get("likes"),
                        "level": h.get("level"),
                        "nickname": h.get("nickname"),
                    },
                    "source": h.get("source"),
                    "likes_given": None,
                }],
                "last_at": h.get("created_at", "")[:16].replace("T", " "),
            }

    return _jsonify({
        "uid": int(info.get("uid") or uid),
        "nickname": info.get("nickname") or "Unknown",
        "level": info.get("level") or 0,
        "likes": info.get("likes") or 0,
        "region": info.get("region") or srv,
        "headPic": info.get("head_pic"),
        "avatarUrl": avatarUrl,
        "rank": info.get("rank"),
        "csRank": info.get("cs_rank"),
        "creditScore": info.get("credit_score"),
        "clanName": info.get("clan_name"),
        "clanId": info.get("clan_id"),
        "clanLevel": info.get("clan_level"),
        "clanLeaderName": info.get("clan_leader_name"),
        "clanLeaderUid": info.get("clan_leader_uid"),
        "signature": info.get("signature"),
        "lastLogin": info.get("last_login"),
        "created": info.get("created"),
        "accountAge": info.get("account_age"),
        "is_blocked": blocked,
        "server": srv,
        "info_date": info.get("date"),
        "today_like_activity": today_like_activity,
        "history_days": history_days,
        "history": history_map,
    })


# ============================================================
#  USAGE DETAIL (NEW)
# ============================================================
@app.get("/master/api/usage-detail")
def master_usage_detail():
    key = request.args.get("key", "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    date = request.args.get("date", _today_str())

    with db_lock:
        conn = _db_connect()
        c = conn.cursor()
        c.execute("""
            SELECT uid, api_key, server, likes_given, requests, last_at, nickname
            FROM like_activity WHERE date=?
            ORDER BY last_at DESC
        """, (date,))
        rows = [dict(r) for r in c.fetchall()]
        conn.close()

    formatted = []
    for r in rows:
        formatted.append({
            "uid": r["uid"],
            "api_key": r.get("api_key") or "—",
            "nickname": r.get("nickname") or "—",
            "server": r.get("server") or "—",
            "requests": r.get("requests", 0),
            "likes_given_total": r.get("likes_given", 0),
            "last_at": r.get("last_at") or "—",
        })

    return _jsonify({"date": date, "rows": formatted})


# ============================================================
#  BLOCKED UIDs (NEW)
# ============================================================
@app.get("/master/api/blocked")
def master_blocked_list():
    key = request.args.get("key", "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    rows = db_get_blocked()
    return _jsonify({
        "blocked": [r["uid"] for r in rows],
        "details": rows,
    })


@app.post("/master/api/block")
def master_block():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    uid = str(data.get("uid", "")).strip()
    reason = str(data.get("reason", "")).strip()
    if not uid.isdigit():
        return jsonify({"error": "valid uid required"}), 400

    db_block_uid(uid, reason)
    return _jsonify({"ok": True, "uid": uid})


@app.post("/master/api/unblock")
def master_unblock():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    uid = str(data.get("uid", "")).strip()
    if not uid.isdigit():
        return jsonify({"error": "valid uid required"}), 400

    db_unblock_uid(uid)
    return _jsonify({"ok": True, "uid": uid})


# ============================================================
#  KEYS
# ============================================================
@app.get("/master/api/keys")
def master_get_keys():
    key = request.args.get("key", "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    cfg = _read_config()
    return _jsonify({
        "allowed_keys": cfg.get("ALLOWED_KEYS", {}),
        "admin_keys": cfg.get("ADMIN_KEYS", []),
        "reset_tz": cfg.get("RESET_TZ", "Asia/Dhaka"),
    })


@app.post("/master/api/keys")
def master_save_keys():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    allowed = data.get("allowed_keys", {})
    admin = data.get("admin_keys", [])
    tz = data.get("reset_tz", "Asia/Dhaka")

    if not isinstance(allowed, dict) or not isinstance(admin, list):
        return jsonify({"error": "invalid types"}), 400

    cfg = {"ALLOWED_KEYS": allowed, "ADMIN_KEYS": admin, "RESET_TZ": tz}
    content = json.dumps(cfg, indent=2, ensure_ascii=False)
    try:
        _write_file("keys.json", content)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return _jsonify({"ok": True})


# ============================================================
#  STATS / ACTIONS
# ============================================================
@app.get("/master/api/stats")
def master_stats():
    key = request.args.get("key", "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    auto_targets = load_auto_targets()
    usage = _load_usage()
    today = _today_str()

    today_usage = {}
    total_requests = 0
    for k, v in usage.items():
        if v.get("date") == today:
            today_usage[k] = len(v.get("uids", []))
            total_requests += len(v.get("uids", []))

    file_stats = {}
    for name in EDITABLE_FILES:
        path = os.path.join(BASE_DIR, name)
        if os.path.exists(path):
            file_stats[name] = os.path.getsize(path)

    jwt_status = {}
    for srv in SERVER_ACCOUNT_FILES:
        e = _jwt_cache.get(srv)
        if e:
            age = int(time.time() - e["ts"])
            jwt_status[srv] = {
                "tokens": len(e["tokens"]),
                "age_seconds": age,
                "next_refresh_in": max(0, JWT_REFRESH_HOURS * 3600 - age),
            }
        else:
            jwt_status[srv] = {"tokens": 0, "age_seconds": None}

    blocked_count = len(db_get_blocked())

    return _jsonify({
        "today": today,
        "auto_targets": {k: len(v) for k, v in auto_targets.items()},
        "auto_targets_full": auto_targets,
        "today_usage": today_usage,
        "total_today_requests": total_requests,
        "file_stats": file_stats,
        "jwt_status": jwt_status,
        "keys_count": {
            "user": len(get_allowed_keys()),
            "master": len(get_admin_keys()),
        },
        "blocked_count": blocked_count,
    })


@app.post("/master/api/run-auto")
def master_run_auto():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or request.args.get("key") or "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403
    threading.Thread(target=do_auto_like_now, daemon=True).start()
    return _jsonify({"ok": True, "message": "auto-like triggered"})


@app.post("/master/api/run-info-fetch")
def master_run_info_fetch():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or request.args.get("key") or "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403
    threading.Thread(target=do_daily_info_fetch, daemon=True).start()
    return _jsonify({"ok": True, "message": "info fetch triggered"})


@app.post("/master/api/jwt-refresh")
def master_jwt_refresh():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or request.args.get("key") or "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    results = {}
    for srv in SERVER_ACCOUNT_FILES:
        try:
            toks = get_or_refresh_tokens(srv, force=True)
            results[srv] = len(toks)
        except Exception as e:
            results[srv] = f"err: {e}"
    return _jsonify({"ok": True, "refreshed": results})


# ============================================================
#  ENTRY
# ============================================================
init_db()
print(f"[*] DB initialized at {DB_PATH}")

if __name__ == "__main__":
    start_background_jobs()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
else:
    try:
        start_background_jobs()
    except Exception as e:
        print(f"[boot] background jobs error: {e}")