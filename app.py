# app.py — MAHIR VIP LIKE — Complete Backend
# ==========================================================
#  Directory: Mahir-like/
#    ├── app.py          ← এই ফাইল
#    ├── index.html      ← User UI
#    ├── master.html     ← Master Panel
#    ├── keys.json       ← API keys
#    ├── auto.txt        ← auto targets
#    ├── account_*.txt   ← guest accounts
#    └── *_pb2.py        ← protobuf
# ==========================================================

import os, sys, json, time, sqlite3, hashlib, binascii, asyncio, threading
from threading import RLock
from contextlib import contextmanager
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import (Flask, request, jsonify, Response, render_template)
from flask_cors import CORS
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from google.protobuf.json_format import MessageToJson

import requests, aiohttp, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    import orjson
    def _jsonify(data, status=200):
        return Response(orjson.dumps(data), status=status, mimetype='application/json')
except ImportError:
    def _jsonify(data, status=200):
        return Response(json.dumps(data, separators=(',', ':'), ensure_ascii=False),
                        status=status, mimetype='application/json')

import like_pb2, like_count_pb2, uid_generator_pb2

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, template_folder=BASE_DIR)
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

INFO_API_BASE   = os.environ.get("INFO_API_BASE", "https://mahir-info-api.vercel.app")
INFO_TIMEOUT    = int(os.environ.get("INFO_TIMEOUT", "15"))
AVATAR_CDN_BASE = os.environ.get("AVATAR_CDN_BASE",
    "https://cdn.jsdelivr.net/gh/ShahGCreator/icon@main/PNG/")
DB_PATH = os.environ.get("MAHIR_DB_PATH", os.path.join(BASE_DIR, "mahir.db"))

SERVER_ACCOUNT_FILES = {
    "BD": "account_bd.txt", "IND": "account_ind.txt", "BR": "account_br.txt",
    "US": "account_us.txt", "SAC": "account_sac.txt", "NA": "account_na.txt",
}
EDITABLE_FILES = ["auto.txt", "keys.json"] + list(SERVER_ACCOUNT_FILES.values())

CONFIG_RO_PATH = os.path.join(BASE_DIR, "keys.json")
CONFIG_RW_PATH = os.path.join("/tmp", "keys.json")
USAGE_PATH     = os.path.join("/tmp", "mahir_usage.json")
AUTO_FILE      = os.path.join(BASE_DIR, "auto.txt")

config_lock, usage_lock, jwt_lock, file_lock, db_lock = RLock(), RLock(), RLock(), RLock(), RLock()
_last_auto_run = {"ts": 0, "total": 0}
_last_auto_run_lock = RLock()

# ═══════════════ DATABASE ═══════════════
def _db_connect():
    c = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    return c

@contextmanager
def get_db():
    with db_lock:
        conn = _db_connect()
        try:
            yield conn; conn.commit()
        except Exception:
            conn.rollback(); raise
        finally:
            conn.close()

def init_db():
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS uid_info (
            uid TEXT PRIMARY KEY, nickname TEXT, avatar_url TEXT, head_pic INTEGER,
            level INTEGER, likes INTEGER, region TEXT, rank INTEGER, cs_rank INTEGER,
            exp INTEGER, banner_id INTEGER, has_elite_pass INTEGER, badge_cnt INTEGER,
            season_id INTEGER, max_rank INTEGER, cs_max_rank INTEGER,
            clan_name TEXT, clan_id TEXT, clan_level INTEGER, clan_capacity INTEGER,
            clan_member_num INTEGER, clan_leader_name TEXT, clan_leader_uid TEXT,
            credit_score INTEGER, last_login TEXT, account_age TEXT, created TEXT,
            signature TEXT, pet_name TEXT, pet_level INTEGER, pet_skin_id INTEGER,
            language TEXT, rank_show TEXT,
            duo_level INTEGER, duo_score INTEGER, duo_partner_uid TEXT, duo_status TEXT,
            raw_info TEXT, raw_duo TEXT,
            last_check_date TEXT, last_check_ts INTEGER, fetch_status TEXT, updated_at INTEGER
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS auto_like_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, uid TEXT NOT NULL,
            nickname TEXT, avatar_url TEXT, key_id TEXT, server TEXT,
            like_date TEXT NOT NULL, like_ts INTEGER, like_count INTEGER DEFAULT 0,
            request_status TEXT, api_result TEXT, created_at INTEGER
        )""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_alh_uid_date ON auto_like_history(uid, like_date)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_alh_date ON auto_like_history(like_date)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_alh_key ON auto_like_history(key_id)")
        c.execute("""CREATE TABLE IF NOT EXISTS blocked_uids (
            uid TEXT PRIMARY KEY, block_status INTEGER DEFAULT 1, block_reason TEXT,
            blocked_date TEXT, blocked_by TEXT, blocked_ts INTEGER
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS info_check_log (
            uid TEXT NOT NULL, check_date TEXT NOT NULL, check_ts INTEGER, status TEXT,
            PRIMARY KEY (uid, check_date)
        )""")
        conn.commit()
    print(f"[DB] initialised at {DB_PATH}")

def db_get_uid_info(uid):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM uid_info WHERE uid=?", (str(uid),)).fetchone()
    return dict(row) if row else None

def db_upsert_uid_info(uid, f, raw_info=None, raw_duo=None, status="success"):
    uid = str(uid); now = int(time.time()); today = datetime.now().strftime("%Y-%m-%d")
    p = {"uid": uid,
        "nickname": f.get("nickname"), "avatar_url": f.get("avatar_url"),
        "head_pic": f.get("head_pic"), "level": f.get("level"), "likes": f.get("likes"),
        "region": f.get("region"), "rank": f.get("rank"), "cs_rank": f.get("cs_rank"),
        "exp": f.get("exp"), "banner_id": f.get("banner_id"),
        "has_elite_pass": f.get("has_elite_pass"), "badge_cnt": f.get("badge_cnt"),
        "season_id": f.get("season_id"), "max_rank": f.get("max_rank"),
        "cs_max_rank": f.get("cs_max_rank"),
        "clan_name": f.get("clan_name"), "clan_id": f.get("clan_id"),
        "clan_level": f.get("clan_level"), "clan_capacity": f.get("clan_capacity"),
        "clan_member_num": f.get("clan_member_num"),
        "clan_leader_name": f.get("clan_leader_name"),
        "clan_leader_uid": f.get("clan_leader_uid"),
        "credit_score": f.get("credit_score"), "last_login": f.get("last_login"),
        "account_age": f.get("account_age"), "created": f.get("created"),
        "signature": f.get("signature"),
        "pet_name": f.get("pet_name"), "pet_level": f.get("pet_level"),
        "pet_skin_id": f.get("pet_skin_id"),
        "language": f.get("language"), "rank_show": f.get("rank_show"),
        "duo_level": f.get("duo_level"), "duo_score": f.get("duo_score"),
        "duo_partner_uid": f.get("duo_partner_uid"), "duo_status": f.get("duo_status"),
        "raw_info": json.dumps(raw_info, ensure_ascii=False) if raw_info else None,
        "raw_duo": json.dumps(raw_duo, ensure_ascii=False) if raw_duo else None,
        "last_check_date": today, "last_check_ts": now,
        "fetch_status": status, "updated_at": now}
    with get_db() as conn:
        conn.execute("""INSERT INTO uid_info
          (uid, nickname, avatar_url, head_pic, level, likes, region, rank, cs_rank,
           exp, banner_id, has_elite_pass, badge_cnt, season_id, max_rank, cs_max_rank,
           clan_name, clan_id, clan_level, clan_capacity, clan_member_num,
           clan_leader_name, clan_leader_uid, credit_score, last_login, account_age,
           created, signature, pet_name, pet_level, pet_skin_id, language, rank_show,
           duo_level, duo_score, duo_partner_uid, duo_status,
           raw_info, raw_duo, last_check_date, last_check_ts, fetch_status, updated_at)
          VALUES
          (:uid, :nickname, :avatar_url, :head_pic, :level, :likes, :region, :rank, :cs_rank,
           :exp, :banner_id, :has_elite_pass, :badge_cnt, :season_id, :max_rank, :cs_max_rank,
           :clan_name, :clan_id, :clan_level, :clan_capacity, :clan_member_num,
           :clan_leader_name, :clan_leader_uid, :credit_score, :last_login, :account_age,
           :created, :signature, :pet_name, :pet_level, :pet_skin_id, :language, :rank_show,
           :duo_level, :duo_score, :duo_partner_uid, :duo_status,
           :raw_info, :raw_duo, :last_check_date, :last_check_ts, :fetch_status, :updated_at)
          ON CONFLICT(uid) DO UPDATE SET
            nickname=COALESCE(excluded.nickname, nickname),
            avatar_url=COALESCE(excluded.avatar_url, avatar_url),
            head_pic=COALESCE(excluded.head_pic, head_pic),
            level=COALESCE(excluded.level, level),
            likes=COALESCE(excluded.likes, likes),
            region=COALESCE(excluded.region, region),
            rank=COALESCE(excluded.rank, rank),
            cs_rank=COALESCE(excluded.cs_rank, cs_rank),
            exp=COALESCE(excluded.exp, exp),
            banner_id=COALESCE(excluded.banner_id, banner_id),
            has_elite_pass=COALESCE(excluded.has_elite_pass, has_elite_pass),
            badge_cnt=COALESCE(excluded.badge_cnt, badge_cnt),
            season_id=COALESCE(excluded.season_id, season_id),
            max_rank=COALESCE(excluded.max_rank, max_rank),
            cs_max_rank=COALESCE(excluded.cs_max_rank, cs_max_rank),
            clan_name=COALESCE(excluded.clan_name, clan_name),
            clan_id=COALESCE(excluded.clan_id, clan_id),
            clan_level=COALESCE(excluded.clan_level, clan_level),
            clan_capacity=COALESCE(excluded.clan_capacity, clan_capacity),
            clan_member_num=COALESCE(excluded.clan_member_num, clan_member_num),
            clan_leader_name=COALESCE(excluded.clan_leader_name, clan_leader_name),
            clan_leader_uid=COALESCE(excluded.clan_leader_uid, clan_leader_uid),
            credit_score=COALESCE(excluded.credit_score, credit_score),
            last_login=COALESCE(excluded.last_login, last_login),
            account_age=COALESCE(excluded.account_age, account_age),
            created=COALESCE(excluded.created, created),
            signature=COALESCE(excluded.signature, signature),
            pet_name=COALESCE(excluded.pet_name, pet_name),
            pet_level=COALESCE(excluded.pet_level, pet_level),
            pet_skin_id=COALESCE(excluded.pet_skin_id, pet_skin_id),
            language=COALESCE(excluded.language, language),
            rank_show=COALESCE(excluded.rank_show, rank_show),
            duo_level=COALESCE(excluded.duo_level, duo_level),
            duo_score=COALESCE(excluded.duo_score, duo_score),
            duo_partner_uid=COALESCE(excluded.duo_partner_uid, duo_partner_uid),
            duo_status=COALESCE(excluded.duo_status, duo_status),
            raw_info=COALESCE(excluded.raw_info, raw_info),
            raw_duo=COALESCE(excluded.raw_duo, raw_duo),
            last_check_date=excluded.last_check_date,
            last_check_ts=excluded.last_check_ts,
            fetch_status=excluded.fetch_status,
            updated_at=excluded.updated_at""", p)
    return db_get_uid_info(uid)

def db_info_checked_today(uid):
    today = datetime.now().strftime("%Y-%m-%d")
    with get_db() as conn:
        row = conn.execute("SELECT 1 FROM info_check_log WHERE uid=? AND check_date=? "
                           "AND status IN ('success','api_failed','no_jwt')",
                           (str(uid), today)).fetchone()
    return bool(row)

def db_log_info_check(uid, status):
    today = datetime.now().strftime("%Y-%m-%d"); now = int(time.time())
    with get_db() as conn:
        conn.execute("""INSERT INTO info_check_log (uid, check_date, check_ts, status)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(uid, check_date) DO UPDATE SET
              check_ts=excluded.check_ts, status=excluded.status""",
            (str(uid), today, now, status))

def db_record_like(uid, key_id, server, like_count, status,
                   nickname=None, avatar_url=None, api_result=None):
    uid = str(uid); now = int(time.time()); today = datetime.now().strftime("%Y-%m-%d")
    with get_db() as conn:
        conn.execute("""INSERT INTO auto_like_history
          (uid, nickname, avatar_url, key_id, server, like_date, like_ts,
           like_count, request_status, api_result, created_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
          (uid, nickname, avatar_url, key_id, server, today, now,
           int(like_count or 0), status,
           json.dumps(api_result, ensure_ascii=False) if api_result else None, now))

def db_history_query(date=None, uid=None, key_id=None, server=None,
                     search=None, page=1, page_size=30):
    clauses, params = [], []
    if date: clauses.append("like_date=?"); params.append(date)
    if uid: clauses.append("uid=?"); params.append(str(uid))
    if key_id: clauses.append("key_id=?"); params.append(key_id)
    if server: clauses.append("server=?"); params.append(server.upper())
    if search:
        clauses.append("(uid LIKE ? OR nickname LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    offset = max(0, (int(page) - 1) * int(page_size))
    with get_db() as conn:
        total = conn.execute(f"SELECT COUNT(*) AS c FROM auto_like_history {where}", params).fetchone()["c"]
        rows = conn.execute(f"SELECT * FROM auto_like_history {where} "
                            f"ORDER BY like_ts DESC LIMIT ? OFFSET ?",
                            params + [int(page_size), offset]).fetchall()
    return [dict(r) for r in rows], int(total)

def db_history_for_uid(uid, limit=500):
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM auto_like_history WHERE uid=? "
                            "ORDER BY like_ts DESC LIMIT ?",
                            (str(uid), int(limit))).fetchall()
    return [dict(r) for r in rows]

def db_aggregate_uid_history(uid):
    uid = str(uid)
    with get_db() as conn:
        t = conn.execute("SELECT COUNT(*) AS requests, COALESCE(SUM(like_count),0) AS likes "
                         "FROM auto_like_history WHERE uid=?", (uid,)).fetchone()
        per_day = conn.execute("""SELECT like_date, COUNT(*) AS requests,
            COALESCE(SUM(like_count),0) AS likes, MAX(like_ts) AS last_ts,
            MAX(request_status) AS last_status
            FROM auto_like_history WHERE uid=?
            GROUP BY like_date ORDER BY like_date DESC""", (uid,)).fetchall()
    return {"total_requests": int(t["requests"] or 0),
            "total_likes": int(t["likes"] or 0),
            "per_day": [dict(r) for r in per_day]}

def db_is_blocked(uid):
    with get_db() as conn:
        row = conn.execute("SELECT 1 FROM blocked_uids WHERE uid=? AND block_status=1",
                           (str(uid),)).fetchone()
    return bool(row)

def db_block_uid(uid, by="master", reason=None):
    now = int(time.time()); today = datetime.now().strftime("%Y-%m-%d")
    with get_db() as conn:
        conn.execute("""INSERT INTO blocked_uids
          (uid, block_status, block_reason, blocked_date, blocked_by, blocked_ts)
          VALUES (?, 1, ?, ?, ?, ?)
          ON CONFLICT(uid) DO UPDATE SET block_status=1,
            block_reason=excluded.block_reason, blocked_date=excluded.blocked_date,
            blocked_by=excluded.blocked_by, blocked_ts=excluded.blocked_ts""",
          (str(uid), reason, today, by, now))

def db_unblock_uid(uid):
    with get_db() as conn:
        conn.execute("UPDATE blocked_uids SET block_status=0 WHERE uid=?", (str(uid),))

def db_list_blocked():
    with get_db() as conn:
        rows = conn.execute("SELECT uid FROM blocked_uids WHERE block_status=1 "
                            "ORDER BY blocked_ts DESC").fetchall()
    return [r["uid"] for r in rows]

def db_blocked_count():
    with get_db() as conn:
        return conn.execute("SELECT COUNT(*) AS c FROM blocked_uids WHERE block_status=1").fetchone()["c"]

# ═══════════════ INFO API ═══════════════
_info_locks = {}; _info_locks_guard = threading.Lock()

def _info_lock_for(uid):
    with _info_locks_guard:
        lk = _info_locks.get(uid)
        if lk is None: lk = threading.Lock(); _info_locks[uid] = lk
        return lk

def _info_get(path, jwt_token, uid, timeout=INFO_TIMEOUT):
    if not jwt_token or not uid: return None, "missing_jwt_or_uid"
    try:
        r = requests.get(f"{INFO_API_BASE}{path}",
                         params={"jwt": jwt_token, "uid": str(uid)},
                         timeout=timeout, verify=False,
                         headers={"User-Agent": "MAHIR-Info/1.0"})
        if r.status_code != 200: return None, f"http_{r.status_code}"
        if len(r.content) > 512 * 1024: return None, "response_too_large"
        try: return r.json(), None
        except ValueError: return None, "invalid_json"
    except requests.Timeout: return None, "timeout"
    except requests.RequestException as e: return None, f"request_error:{e.__class__.__name__}"

def _to_int(v):
    try: return int(v)
    except (TypeError, ValueError): return None

def _parse_info_response(data):
    """
    Parse /info response based on real structure:
      { basicInfo: {...}, profileInfo: {...}, clanBasicInfo: {...},
        captainBasicInfo: {...}, petInfo: {...}, socialInfo: {...},
        creditScoreInfo: {...} }
    """
    if not isinstance(data, dict): return {}
    b = data.get("basicInfo") or {}
    prof = data.get("profileInfo") or {}
    clan = data.get("clanBasicInfo") or {}
    cap = data.get("captainBasicInfo") or {}
    pet = data.get("petInfo") or {}
    soc = data.get("socialInfo") or {}
    cred = data.get("creditScoreInfo") or {}

    head_pic = _to_int(b.get("headPic") or prof.get("avatarId"))
    avatar_url = f"{AVATAR_CDN_BASE}{head_pic}.png" if head_pic else None

    return {
        "nickname": b.get("nickname") or cap.get("nickname"),
        "avatar_url": avatar_url,
        "head_pic": head_pic,
        "level": _to_int(b.get("level")),
        "likes": _to_int(b.get("liked")),
        "region": b.get("region"),
        "rank": _to_int(b.get("rank")),
        "cs_rank": _to_int(b.get("csRank")),
        "exp": _to_int(b.get("exp")),
        "banner_id": _to_int(b.get("bannerId")),
        "has_elite_pass": 1 if b.get("hasElitePass") else 0,
        "badge_cnt": _to_int(b.get("badgeCnt")),
        "season_id": _to_int(b.get("seasonId")),
        "max_rank": _to_int(b.get("maxRank")),
        "cs_max_rank": _to_int(b.get("csMaxRank")),
        "clan_name": clan.get("clanName"),
        "clan_id": str(clan.get("clanId")) if clan.get("clanId") else None,
        "clan_level": _to_int(clan.get("clanLevel")),
        "clan_capacity": _to_int(clan.get("capacity")),
        "clan_member_num": _to_int(clan.get("memberNum")),
        "clan_leader_name": cap.get("nickname"),
        "clan_leader_uid": str(clan.get("captainId")) if clan.get("captainId") else None,
        "credit_score": _to_int(cred.get("creditScore")),
        "last_login": b.get("lastLoginDecoded"),
        "account_age": b.get("accountAge"),
        "created": b.get("createdDecoded"),
        "signature": soc.get("signature"),
        "pet_name": pet.get("name"),
        "pet_level": _to_int(pet.get("level")),
        "pet_skin_id": _to_int(pet.get("skinId")),
        "language": soc.get("language"),
        "rank_show": soc.get("rankShow"),
    }

def _parse_duo_response(data):
    """Parse /duo response: { data: { duo_level, duo_score, partner_uid, status, ... } }"""
    if not isinstance(data, dict): return {}
    d = data.get("data") or {}
    if not isinstance(d, dict): return {}
    return {
        "duo_level": _to_int(d.get("duo_level")),
        "duo_score": _to_int(d.get("duo_score")),
        "duo_partner_uid": str(d.get("partner_uid")) if d.get("partner_uid") else None,
        "duo_status": d.get("status"),
    }

def get_or_fetch_uid_info(uid, jwt_token, force=False):
    uid = str(uid)
    cached = db_get_uid_info(uid)
    today = datetime.now().strftime("%Y-%m-%d")

    if cached and not force and cached.get("last_check_date") == today:
        return cached, "db"
    if db_info_checked_today(uid) and not force:
        return cached, ("stale" if cached else "none")

    lock = _info_lock_for(uid)
    if not lock.acquire(blocking=False):
        return cached, ("db" if cached else "none")
    try:
        if db_info_checked_today(uid) and not force:
            return db_get_uid_info(uid) or cached, ("stale" if cached else "none")
        if not jwt_token:
            db_log_info_check(uid, "no_jwt")
            return cached, ("stale" if cached else "none")

        info_raw, _ = _info_get("/info", jwt_token, uid)
        duo_raw, _  = _info_get("/duo",  jwt_token, uid)

        if info_raw is None and duo_raw is None:
            db_log_info_check(uid, "api_failed")
            return cached, ("stale" if cached else "none")

        merged = {}
        merged.update(_parse_info_response(info_raw or {}))
        merged.update(_parse_duo_response(duo_raw or {}))
        if not merged.get("nickname"):
            db_log_info_check(uid, "api_failed")
            return cached, ("stale" if cached else "none")

        saved = db_upsert_uid_info(uid, merged, raw_info=info_raw, raw_duo=duo_raw, status="success")
        db_log_info_check(uid, "success")
        return saved, "api"
    finally:
        lock.release()

# ═══════════════ CONFIG ═══════════════
def _active_config_path():
    return CONFIG_RW_PATH if os.path.exists(CONFIG_RW_PATH) else CONFIG_RO_PATH

def _read_config():
    path = _active_config_path()
    if not os.path.exists(path):
        return {"ALLOWED_KEYS": {}, "ADMIN_KEYS": [], "RESET_TZ": "Asia/Dhaka"}
    with open(path, "r", encoding="utf-8") as f: return json.load(f)

def get_allowed_keys():
    with config_lock: return _read_config().get("ALLOWED_KEYS", {})
def get_admin_keys():
    with config_lock: return set(_read_config().get("ADMIN_KEYS", []))
def get_reset_tz():
    with config_lock: return _read_config().get("RESET_TZ", "Asia/Dhaka")

def classify_key(api_key):
    if not api_key: return "invalid"
    try:
        if api_key in get_admin_keys(): return "master"
        if api_key in get_allowed_keys(): return "user"
    except Exception: pass
    return "invalid"

def key_id_for(api_key):
    if not api_key: return "anon"
    return "key_" + hashlib.sha256(api_key.encode()).hexdigest()[:10]

# ═══════════════ TIME / QUOTA ═══════════════
def _now_local():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(get_reset_tz()))
    except Exception: return datetime.now()

def _today_str(): return _now_local().strftime("%Y-%m-%d")

def _reset_at_str():
    nxt = (_now_local() + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return nxt.strftime("%Y-%m-%d %H:%M")

def _load_usage():
    with usage_lock:
        if not os.path.exists(USAGE_PATH): return {}
        try:
            with open(USAGE_PATH, "r", encoding="utf-8") as f: return json.load(f)
        except Exception: return {}

def _save_usage(u):
    with usage_lock:
        try:
            with open(USAGE_PATH, "w", encoding="utf-8") as f: json.dump(u, f)
        except Exception: pass

def check_and_consume_quota(api_key, uid, tier):
    if tier in ("master", "auto"): return True, None, None
    today = _today_str(); u = _load_usage()
    entry = u.get(api_key, {})
    if entry.get("date") != today: entry = {"date": today, "uids": []}
    uids = entry.get("uids", [])
    if uid in uids: return False, max(0, DAILY_LIMIT_USER - len(uids)), _reset_at_str()
    if len(uids) >= DAILY_LIMIT_USER: return False, 0, _reset_at_str()
    uids.append(uid); entry["uids"] = uids; u[api_key] = entry
    _save_usage(u)
    return True, max(0, DAILY_LIMIT_USER - len(uids)), None

# ═══════════════ ACCOUNTS / AUTO ═══════════════
def _account_path(server_name):
    f = SERVER_ACCOUNT_FILES.get(server_name.upper())
    return os.path.join(BASE_DIR, f) if f else ""

def load_accounts(server_name):
    path = _account_path(server_name)
    if not path or not os.path.exists(path): return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln.startswith("#") or ":" not in ln: continue
            u, p = ln.split(":", 1)
            if u.strip() and p.strip(): out.append((u.strip(), p.strip()))
    return out

def load_auto_targets():
    if not os.path.exists(AUTO_FILE): return {srv: [] for srv in SERVER_ACCOUNT_FILES}
    result = {srv: [] for srv in SERVER_ACCOUNT_FILES}; current = None
    with open(AUTO_FILE, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln.startswith("#"): continue
            if ln.startswith("[") and ln.endswith("]"):
                current = ln[1:-1].strip().upper()
                if current not in SERVER_ACCOUNT_FILES: current = None
                continue
            if current and ln.isdigit(): result[current].append(ln)
    for k in result: result[k] = list(dict.fromkeys(result[k]))
    return result

def save_auto_targets(targets):
    try:
        with file_lock:
            with open(AUTO_FILE, "w", encoding="utf-8") as f:
                for srv in SERVER_ACCOUNT_FILES:
                    f.write(f"[{srv}]\n")
                    for uid in targets.get(srv, []): f.write(f"{uid}\n")
                    f.write("\n")
    except Exception as e: print(f"[auto.txt] save error: {e}")

def register_auto_uid(server_name, uid):
    targets = load_auto_targets(); srv = server_name.upper()
    targets.setdefault(srv, [])
    if uid not in targets[srv]:
        targets[srv].append(uid); save_auto_targets(targets)

# ═══════════════ JWT ═══════════════
def _fetch_single_jwt(uid, pw, timeout=15):
    try:
        r = requests.get(f"{JWT_API_BASE}?uid={uid}&password={pw}", timeout=timeout, verify=False)
        if r.status_code != 200: return uid, None
        try: j = r.json()
        except ValueError:
            txt = r.text.strip()
            return uid, txt if txt and len(txt) > 50 else None
        token = None
        if isinstance(j, dict):
            token = (j.get("jwt_token") or j.get("token") or
                     j.get("access_token") or j.get("jwt"))
            if not token and isinstance(j.get("data"), dict):
                token = (j["data"].get("jwt_token") or j["data"].get("token") or
                         j["data"].get("jwt"))
        elif isinstance(j, str): token = j
        return uid, token
    except requests.RequestException: return uid, None

def generate_jwts(accounts):
    if not accounts: return []
    tokens = []
    with ThreadPoolExecutor(max_workers=JWT_WORKERS) as pool:
        futures = {pool.submit(_fetch_single_jwt, u, p): u for u, p in accounts}
        for fut in as_completed(futures):
            _, tok = fut.result()
            if tok: tokens.append(tok)
    return tokens

_jwt_cache = {}
def get_or_refresh_tokens(server_name, force=False):
    with jwt_lock:
        if not force:
            e = _jwt_cache.get(server_name)
            if e and time.time() - e["ts"] < JWT_REFRESH_HOURS * 3600:
                return e["tokens"]
        accounts = load_accounts(server_name)
        if not accounts: return []
        print(f"[JWT] refresh {server_name}: {len(accounts)} accounts")
        tokens = generate_jwts(accounts)
        print(f"[JWT] got {len(tokens)} tokens ({server_name})")
        if tokens: _jwt_cache[server_name] = {"tokens": tokens, "ts": time.time()}
        return tokens

def _global_jwt_for(server_name):
    toks = get_or_refresh_tokens(server_name)
    return toks[0] if toks else None

# ═══════════════ FF HELPERS ═══════════════
def _encrypt(plaintext):
    k = b'Yg&tc%DEuh6%Zc^8'; iv = b'6oyZDr22E3ychjM%'
    return binascii.hexlify(AES.new(k, AES.MODE_CBC, iv).encrypt(pad(plaintext, AES.block_size))).decode()

def _pb_msg(uid, region):
    m = like_pb2.like(); m.uid = int(uid); m.region = region
    return m.SerializeToString()

def _pb_uid(uid):
    m = uid_generator_pb2.uid_generator()
    m.krishna_ = int(uid); m.teamXdarks = 1
    return m.SerializeToString()

def enc(uid): return _encrypt(_pb_uid(uid))

def like_url_for(s):
    s = s.upper()
    if s == "IND": return "https://client.ind.freefiremobile.com/LikeProfile"
    if s in {"BR", "US", "SAC", "NA"}: return "https://client.us.freefiremobile.com/LikeProfile"
    return "https://clientbp.ppmainecoonghj.com/LikeProfile"

def show_url_for(s):
    s = s.upper()
    if s == "IND": return "https://client.ind.freefiremobile.com/GetPlayerPersonalShow"
    if s in {"BR", "US", "SAC", "NA"}: return "https://client.us.freefiremobile.com/GetPlayerPersonalShow"
    return "https://clientbp.ppmainecoonghj.com/GetPlayerPersonalShow"

HEADERS = {
    'User-Agent': "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)",
    'Connection': "Keep-Alive", 'Accept-Encoding': "gzip",
    'Content-Type': "application/x-www-form-urlencoded", 'Expect': "100-continue",
    'X-Unity-Version': "2018.4.11f1", 'X-GA': "v1 1", 'ReleaseVersion': "OB55",
}

async def _async_post(enc_uid, token, url, sess):
    h = dict(HEADERS); h["Authorization"] = f"Bearer {token}"
    try:
        async with sess.post(url, data=bytes.fromhex(enc_uid), headers=h) as r:
            return r.status
    except Exception: return 0

async def _burst(uid, server, url, tokens):
    enc_uid = _encrypt(_pb_msg(uid, server))
    conn = aiohttp.TCPConnector(limit=LIKE_CONCUR, ssl=False)
    sem = asyncio.Semaphore(LIKE_CONCUR)
    async with aiohttp.ClientSession(connector=conn) as s:
        async def w(t):
            async with sem: return await _async_post(enc_uid, t, url, s)
        return await asyncio.gather(*[w(t) for t in tokens])

def send_likes_from_all_tokens(uid, server, url, tokens):
    if not tokens: return 0
    try: res = asyncio.run(_burst(uid, server, url, tokens))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try: res = loop.run_until_complete(_burst(uid, server, url, tokens))
        finally: loop.close()
    return sum(1 for s in res if s == 200)

def make_request(encrypted, server, token):
    h = dict(HEADERS); h["Authorization"] = f"Bearer {token}"
    try:
        r = requests.post(show_url_for(server), data=bytes.fromhex(encrypted),
                          headers=h, verify=False, timeout=30)
        obj = like_count_pb2.Info(); obj.ParseFromString(r.content)
        return obj
    except Exception: return None

def parse_account_info(pb):
    try:
        if pb is None:
            return None
        js = json.loads(MessageToJson(pb))
        ai = js.get("AccountInfo", {})
        uid = int(ai.get("UID", 0))
        if uid <= 0:
            return None
        # HeadPic বের করা - multiple field name try
        head = (ai.get("HeadPic") or ai.get("headPic") or
                ai.get("HeadPicId") or ai.get("headPicId") or
                ai.get("AvatarId") or ai.get("avatarId"))
        try:
            head = int(head) if head else None
        except (TypeError, ValueError):
            head = None
        return {
            "uid": uid,
            "likes": int(ai.get("Likes", 0)),
            "name": str(ai.get("PlayerNickname", "")),
            "head_pic": head,
            "level": int(ai.get("Level", 0) or 0),
        }
    except Exception:
        return None

# ═══════════════ CORE LIKE ═══════════════
_api_key_ctx = threading.local()

def _record_history_for_result(uid, server_name, api_key, tier, result):
    try:
        key_id = "auto" if tier == "auto" else key_id_for(api_key)
        status = "success" if result.get("LikesGivenByAPI", 0) > 0 else "no_likes"
        db_record_like(uid=str(uid), key_id=key_id, server=server_name,
                       like_count=int(result.get("LikesGivenByAPI", 0) or 0),
                       status=status, nickname=result.get("PlayerNickname"),
                       avatar_url=result.get("avatarUrl"),
                       api_result={"before": result.get("LikesbeforeCommand"),
                                   "after": result.get("LikesafterCommand"),
                                   "server": server_name})
    except Exception as e: print(f"[history] record error: {e}")

def do_like(uid, server_name, tier="user"):
    api_key = "_auto_" if tier == "auto" else getattr(_api_key_ctx, "key", "")

    if db_is_blocked(uid):
        return {"error": "This UID is blocked.", "blocked": True, "tier": tier.upper()}

    allowed, remaining, reset_at = check_and_consume_quota(api_key, uid, tier)
    if not allowed:
        return {"error": "Daily limit reached (1 UID/day for user keys).",
                "tier": tier.upper(), "quota_remaining": remaining, "reset_at": reset_at}

    accounts = load_accounts(server_name)
    if not accounts:
        return {"error": f"No guest accounts for {server_name}",
                "hint": f"Add uid:pass into {SERVER_ACCOUNT_FILES[server_name]}"}

    tokens = get_or_refresh_tokens(server_name)
    if not tokens:
        return {"error": "Failed to generate JWT tokens."}

    token = tokens[0]
    encrypted = enc(uid)
    before = parse_account_info(make_request(encrypted, server_name, token))
    if before is None:
        result = {"LikesGivenByAPI": 0, "LikesafterCommand": 0, "LikesbeforeCommand": 0,
                  "PlayerNickname": "Unknown",
                  "UID": int(uid) if str(uid).isdigit() else uid,
                  "GiftCount": 0, "accounts_loaded": len(accounts),
                  "tokens_generated": len(tokens), "server_name": server_name,
                  "tier": tier.upper(), "quota_remaining": remaining,
                  "Owner": OWNER_HANDLE, "status": 0}
        _record_history_for_result(uid, server_name, api_key, tier, result)
        return result

    url = like_url_for(server_name)
    send_likes_from_all_tokens(uid, server_name, url, tokens)

    after = parse_account_info(make_request(encrypted, server_name, token)) or {
        "likes": before["likes"], "uid": before["uid"],
        "name": before["name"], "head_pic": before.get("head_pic")}
    like_given = max(0, int(after["likes"]) - int(before["likes"]))

    result = {
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
    _record_history_for_result(uid, server_name, api_key, tier, result)

    # ---------- AVATAR FIX: multi-stage fallback ----------
    info = None
    head_pic = before.get("head_pic") or after.get("head_pic")

    # Stage 1: try current server JWT
    try:
        info, _src = get_or_fetch_uid_info(uid, token)
    except Exception as e:
        print(f"[info] stage1 err {uid}: {e}")

    # Stage 2: if no avatar, try other servers' JWT
    if not info or not info.get("avatar_url"):
        for srv in SERVER_ACCOUNT_FILES:
            if srv == server_name:
                continue
            try:
                tok = _global_jwt_for(srv)
                if tok:
                    info2, _ = get_or_fetch_uid_info(uid, tok, force=False)
                    if info2 and info2.get("avatar_url"):
                        info = info2
                        break
            except Exception:
                continue

    # Stage 3: push results into response
    if info:
        result["avatarUrl"] = info.get("avatar_url")
        result["headPic"] = info.get("head_pic") or head_pic
        result["level"] = info.get("level") or before.get("level")
        result["likes"] = info.get("likes")
        result["clanName"] = info.get("clan_name")
        result["clanId"] = info.get("clan_id")
        result["region"] = info.get("region")
        if not result.get("PlayerNickname") or result["PlayerNickname"] == "Unknown":
            result["PlayerNickname"] = info.get("nickname") or result["PlayerNickname"]
    else:
        # fallback: build avatarUrl from protobuf HeadPic
        result["headPic"] = head_pic
        result["level"] = before.get("level")
        if head_pic:
            result["avatarUrl"] = f"{AVATAR_CDN_BASE}{head_pic}.png"

    # Ultimate fallback: if still no avatar but we have head_pic anywhere
    if not result.get("avatarUrl") and result.get("headPic"):
        result["avatarUrl"] = f"{AVATAR_CDN_BASE}{result['headPic']}.png"

    return result

# ═══════════════ AUTO-LIKE ═══════════════
def do_auto_like_now():
    print(f"\n[AUTO-LIKE] start {datetime.now():%Y-%m-%d %H:%M:%S}")
    targets = load_auto_targets(); total = 0
    for srv, uids in targets.items():
        if not uids: continue
        try:
            tokens = get_or_refresh_tokens(srv, force=True)
            if not tokens: print(f"[AUTO-LIKE] {srv}: no tokens"); continue
            url = like_url_for(srv)
            for uid in uids:
                try:
                    if db_is_blocked(uid):
                        print(f"[AUTO-LIKE] {srv} {uid} → BLOCKED (skip)")
                        db_record_like(uid, "auto", srv, 0, "blocked"); continue
                    ok = send_likes_from_all_tokens(uid, srv, url, tokens)
                    total += ok
                    info = None
                    try: info, _src = get_or_fetch_uid_info(uid, tokens[0])
                    except Exception as e: print(f"[AUTO-LIKE][info] {uid} err: {e}")
                    db_record_like(uid=uid, key_id="auto", server=srv, like_count=ok,
                                   status=("success" if ok > 0 else "no_likes"),
                                   nickname=(info or {}).get("nickname"),
                                   avatar_url=(info or {}).get("avatar_url"),
                                   api_result={"server": srv, "tokens": len(tokens)})
                    print(f"[AUTO-LIKE] {srv} {uid} → {ok}")
                except Exception as e:
                    print(f"[AUTO-LIKE] {srv} {uid} err: {e}")
                    try: db_record_like(uid, "auto", srv, 0, "error", api_result={"error": str(e)})
                    except Exception: pass
                time.sleep(0.5)
        except Exception as e: print(f"[AUTO-LIKE] {srv} err: {e}")
    print(f"[AUTO-LIKE] done. total={total}\n")
    with _last_auto_run_lock:
        _last_auto_run["ts"] = int(time.time()); _last_auto_run["total"] = total
    return total

def _auto_like_scheduler():
    while True:
        try:
            now = datetime.now()
            target = now.replace(hour=AUTO_LIKE_HOUR, minute=AUTO_LIKE_MINUTE, second=0, microsecond=0)
            if now >= target: target += timedelta(days=1)
            wait = (target - now).total_seconds()
            print(f"[SCHED] next auto-like {target:%Y-%m-%d %H:%M:%S}")
            time.sleep(wait); do_auto_like_now()
        except Exception as e: print(f"[SCHED] err: {e}"); time.sleep(60)

def _jwt_refresh_scheduler():
    while True:
        try:
            time.sleep(JWT_REFRESH_HOURS * 3600)
            print(f"\n[JWT-REFRESH] {datetime.now():%Y-%m-%d %H:%M:%S}")
            for srv in SERVER_ACCOUNT_FILES:
                try:
                    toks = get_or_refresh_tokens(srv, force=True)
                    print(f"[JWT-REFRESH] {srv}: {len(toks)}")
                except Exception as e: print(f"[JWT-REFRESH] {srv} err: {e}")
        except Exception as e: print(f"[JWT-REFRESH] err: {e}"); time.sleep(60)

def start_background_jobs():
    threading.Thread(target=_auto_like_scheduler, daemon=True).start()
    threading.Thread(target=_jwt_refresh_scheduler, daemon=True).start()
    print("[*] Background jobs started")

# ═══════════════ MASTER HELPERS ═══════════════
def _master_required(api_key): return classify_key(api_key) == "master"

def _safe_file_path(name):
    if name not in EDITABLE_FILES: return None
    return os.path.join(BASE_DIR, name)

def _read_file(name):
    path = _safe_file_path(name)
    if not path or not os.path.exists(path): return ""
    with open(path, "r", encoding="utf-8") as f: return f.read()

def _write_file(name, content):
    path = _safe_file_path(name)
    if not path: raise ValueError("file not allowed")
    with file_lock:
        with open(path, "w", encoding="utf-8") as f: f.write(content)

def _auth_any_key():
    key = (request.args.get("key")
           or (request.get_json(silent=True) or {}).get("key") or "").strip()
    return classify_key(key), key

# ═══════════════ PAGES ═══════════════
@app.get("/")
def index():
    return render_template("index.html", brand=BRAND_NAME, dev=DEV_NAME, tg=TELEGRAM,
                           tiktok=TIKTOK, website=WEBSITE, owner=OWNER_HANDLE,
                           badge=BADGE_TEXT, servers=list(SERVER_ACCOUNT_FILES.keys()))

@app.get("/master")
def master_page():
    return render_template("master.html", brand=BRAND_NAME, dev=DEV_NAME, tg=TELEGRAM,
                           tiktok=TIKTOK, website=WEBSITE, owner=OWNER_HANDLE,
                           badge=BADGE_TEXT, servers=list(SERVER_ACCOUNT_FILES.keys()))

@app.get("/health")
def route_health():
    return _jsonify({"status": "ok", "service": BRAND_NAME, "dev": DEV_NAME,
                     "website": WEBSITE, "tg": TELEGRAM, "tiktok": TIKTOK,
                     "jwt_api": JWT_API_BASE, "info_api": INFO_API_BASE,
                     "servers": list(SERVER_ACCOUNT_FILES.keys()),
                     "auto_like_at": f"{AUTO_LIKE_HOUR:02d}:{AUTO_LIKE_MINUTE:02d}",
                     "jwt_refresh_hours": JWT_REFRESH_HOURS,
                     "daily_limit_user": DAILY_LIMIT_USER,
                     "auto_targets_loaded": {k: len(v) for k, v in load_auto_targets().items()},
                     "blocked_count": db_blocked_count()})

# ═══════════════ LIKE ROUTES ═══════════════
@app.get("/like")
def handle_like():
    try:
        uid = request.args.get("uid", "").strip()
        server_name = request.args.get("server_name", "").upper().strip()
        api_key = request.args.get("key", "").strip()
        tier = classify_key(api_key)
        if tier == "invalid": return jsonify({"error": "Invalid or missing API key"}), 403
        if not uid or not server_name: return jsonify({"error": "UID and server_name required"}), 400
        if server_name not in SERVER_ACCOUNT_FILES:
            return jsonify({"error": f"Unsupported server '{server_name}'"}), 400
        _api_key_ctx.key = api_key
        try: register_auto_uid(server_name, uid)
        except Exception as e: print(f"[auto-register] warn: {e}")
        result = do_like(uid, server_name, tier=tier)
        if result.get("error"):
            if "Daily limit" in result["error"]: return jsonify(result), 429
            if result.get("blocked"): return jsonify(result), 403
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
            return jsonify({"error": "Valid numeric uid required",
                            "example": "/mahir&like?uid=1234567890&key=MAHIR-USER-001",
                            "Owner": OWNER_HANDLE}), 400
        tier = classify_key(api_key)
        if tier == "invalid":
            return jsonify({"error": "Invalid or missing API key",
                            "example": "/mahir&like?uid=1234567890&key=MAHIR-USER-001",
                            "Owner": OWNER_HANDLE}), 403
        if not server_name: server_name = "BD"
        if server_name not in SERVER_ACCOUNT_FILES:
            return jsonify({"error": f"Unsupported server '{server_name}'",
                            "allowed": list(SERVER_ACCOUNT_FILES.keys()),
                            "default": "BD"}), 400
        _api_key_ctx.key = api_key
        try: register_auto_uid(server_name, uid)
        except Exception as e: print(f"[auto-register] warn: {e}")
        result = do_like(uid, server_name, tier=tier)
        if result.get("error"):
            if "Daily limit" in result["error"]: return jsonify(result), 429
            if result.get("blocked"): return jsonify(result), 403
            return jsonify(result), 500
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": "runtime_error", "detail": str(e)}), 500

@app.get("/auto/list")
def auto_list(): return _jsonify({"auto_targets": load_auto_targets()})

@app.get("/cron/auto_like")
def cron_auto_like():
    secret = request.args.get("secret", "")
    expected = os.environ.get("CRON_SECRET", "mahir-cron-2025")
    if secret != expected: return jsonify({"error": "forbidden"}), 403
    threading.Thread(target=do_auto_like_now, daemon=True).start()
    return jsonify({"ok": True, "triggered": True})

# ═══════════════ HISTORY / BLOCK API ═══════════════
@app.get("/api/like-history")
def api_like_history():
    tier, key = _auth_any_key()
    if tier == "invalid": return jsonify({"error": "invalid key"}), 403
    date = request.args.get("date"); uid = request.args.get("uid")
    key_id = request.args.get("key_id"); server = request.args.get("server")
    search = request.args.get("search")
    page = max(1, request.args.get("page", 1, type=int) or 1)
    size = max(1, min(request.args.get("page_size", 30, type=int) or 30, 200))
    if tier == "user": key_id = key_id_for(key)
    rows, total = db_history_query(date=date, uid=uid, key_id=key_id, server=server,
                                   search=search, page=page, page_size=size)
    return _jsonify({"rows": rows, "total": total, "page": page,
                     "page_size": size, "pages": (total + size - 1) // size})

@app.get("/api/like-history/<uid>")
def api_like_history_uid(uid):
    tier, key = _auth_any_key()
    if tier == "invalid": return jsonify({"error": "invalid key"}), 403
    if not str(uid).isdigit(): return jsonify({"error": "invalid uid"}), 400
    rows = db_history_for_uid(uid, limit=500)
    agg = db_aggregate_uid_history(uid)
    info = db_get_uid_info(uid)
    if tier == "user":
        my_id = key_id_for(key)
        rows = [r for r in rows if r.get("key_id") == my_id]
    return _jsonify({"uid": str(uid), "rows": rows, "aggregate": agg,
                     "info": info, "is_blocked": db_is_blocked(uid)})

@app.get("/api/uid-info/<uid>")
def api_uid_info(uid):
    tier, _key = _auth_any_key()
    if tier == "invalid": return jsonify({"error": "invalid key"}), 403
    if not str(uid).isdigit(): return jsonify({"error": "invalid uid"}), 400
    info = db_get_uid_info(uid)
    if not info: return jsonify({"error": "not found"}), 404
    return _jsonify(info)

@app.post("/api/uid-block/<uid>")
def api_uid_block(uid):
    tier, key = _auth_any_key()
    if tier == "invalid": return jsonify({"error": "invalid key"}), 403
    if not str(uid).isdigit(): return jsonify({"error": "invalid uid"}), 400
    data = request.get_json(silent=True) or {}
    reason = (data.get("reason") or "blocked by user")[:200]
    db_block_uid(uid, by=("master" if tier == "master" else key_id_for(key)), reason=reason)
    return _jsonify({"ok": True, "uid": uid, "blocked": True})

@app.post("/api/uid-unblock/<uid>")
def api_uid_unblock(uid):
    tier, _key = _auth_any_key()
    if tier == "invalid": return jsonify({"error": "invalid key"}), 403
    if tier != "master": return jsonify({"error": "master key required"}), 403
    if not str(uid).isdigit(): return jsonify({"error": "invalid uid"}), 400
    db_unblock_uid(uid)
    return _jsonify({"ok": True, "uid": uid, "blocked": False})

# ═══════════════ MASTER APIs ═══════════════
@app.get("/master/api/files")
def master_files():
    key = request.args.get("key", "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    files = []
    for name in EDITABLE_FILES:
        path = os.path.join(BASE_DIR, name)
        exists = os.path.exists(path); size = os.path.getsize(path) if exists else 0
        lines = 0
        if exists and name.endswith(".txt"):
            try:
                with open(path, "r", encoding="utf-8") as f: lines = sum(1 for _ in f)
            except Exception: pass
        files.append({"name": name, "exists": exists, "size": size, "lines": lines})
    return _jsonify({"files": files})

@app.get("/master/api/file")
def master_get_file():
    key = request.args.get("key", "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    name = request.args.get("name", "").strip()
    if name not in EDITABLE_FILES: return jsonify({"error": "file not allowed"}), 400
    return _jsonify({"name": name, "content": _read_file(name)})

@app.post("/master/api/file")
def master_save_file():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or request.args.get("key") or "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    name = (data.get("name") or "").strip(); content = data.get("content", "")
    if name not in EDITABLE_FILES: return jsonify({"error": "file not allowed"}), 400
    if name == "keys.json":
        try: json.loads(content)
        except Exception as e: return jsonify({"error": f"invalid JSON: {e}"}), 400
    try: _write_file(name, content)
    except Exception as e: return jsonify({"error": str(e)}), 500
    return _jsonify({"ok": True, "name": name, "size": len(content)})

@app.post("/master/api/upload")
def master_upload():
    key = (request.form.get("key") or request.args.get("key") or "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    f = request.files.get("file")
    if not f: return jsonify({"error": "no file uploaded"}), 400
    target_name = (request.form.get("name") or f.filename or "").strip()
    if target_name not in EDITABLE_FILES:
        return jsonify({"error": f"filename '{target_name}' not allowed",
                        "allowed": EDITABLE_FILES}), 400
    try: content = f.read().decode("utf-8", errors="replace")
    except Exception as e: return jsonify({"error": f"read error: {e}"}), 400
    if target_name == "keys.json":
        try: json.loads(content)
        except Exception as e: return jsonify({"error": f"invalid JSON: {e}"}), 400
    try: _write_file(target_name, content)
    except Exception as e: return jsonify({"error": str(e)}), 500
    return _jsonify({"ok": True, "name": target_name, "size": len(content)})

@app.post("/master/api/auto/bulk-upload")
def master_auto_bulk_upload():
    key = (request.form.get("key") or (request.get_json(silent=True) or {}).get("key")
           or request.args.get("key") or "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    srv = (request.form.get("server_name")
           or (request.get_json(silent=True) or {}).get("server_name")
           or request.args.get("server_name") or "").upper().strip()
    if srv not in SERVER_ACCOUNT_FILES:
        return jsonify({"error": "valid server_name required",
                        "allowed": list(SERVER_ACCOUNT_FILES.keys())}), 400
    mode = (request.form.get("mode") or (request.get_json(silent=True) or {}).get("mode")
            or "replace").strip().lower()
    if mode not in ("replace", "append"): mode = "replace"
    raw_text = ""
    f = request.files.get("file")
    if f:
        try: raw_text = f.read().decode("utf-8", errors="replace")
        except Exception as e: return jsonify({"error": f"file read error: {e}"}), 400
    else:
        data = request.get_json(silent=True) or {}
        raw_text = str(data.get("text") or request.form.get("text") or "")
    uids = []
    for chunk in raw_text.replace(",", "\n").replace(" ", "\n").replace(";", "\n").split("\n"):
        c = chunk.strip()
        if not c or c.startswith("#"): continue
        if c.startswith("[") and c.endswith("]"): continue
        if c.isdigit(): uids.append(c)
        else:
            digits = "".join(ch for ch in c.split(":")[0] if ch.isdigit())
            if digits: uids.append(digits)
    if not uids: return jsonify({"error": "no valid UIDs found in input"}), 400
    uids = list(dict.fromkeys(uids))
    targets = load_auto_targets()
    if mode == "replace": targets[srv] = uids
    else:
        existing = targets.get(srv, [])
        for u in uids:
            if u not in existing: existing.append(u)
        targets[srv] = existing
    save_auto_targets(targets)
    return _jsonify({"ok": True, "server_name": srv, "mode": mode,
                     "imported": len(uids), "total_for_server": len(targets[srv]),
                     "auto_targets": targets})

@app.post("/master/api/auto/add")
def master_auto_add():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    uid = str(data.get("uid", "")).strip(); srv = str(data.get("server_name", "")).upper().strip()
    if not uid.isdigit() or srv not in SERVER_ACCOUNT_FILES:
        return jsonify({"error": "uid (digits) and valid server_name required"}), 400
    register_auto_uid(srv, uid)
    try:
        tok = _global_jwt_for(srv)
        if tok: get_or_fetch_uid_info(uid, tok, force=True)
    except Exception as e: print(f"[master_auto_add] info fetch warn: {e}")
    return _jsonify({"ok": True, "auto_targets": load_auto_targets()})

@app.post("/master/api/auto/remove")
def master_auto_remove():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    uid = str(data.get("uid", "")).strip(); srv = str(data.get("server_name", "")).upper().strip()
    if not uid or srv not in SERVER_ACCOUNT_FILES:
        return jsonify({"error": "uid and valid server_name required"}), 400
    targets = load_auto_targets()
    if srv in targets and uid in targets[srv]:
        targets[srv].remove(uid); save_auto_targets(targets)
    return _jsonify({"ok": True, "auto_targets": targets})

@app.post("/master/api/auto/clear")
def master_auto_clear():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    srv = str(data.get("server_name", "")).upper().strip()
    targets = load_auto_targets()
    if not srv or srv == "ALL":
        for s in SERVER_ACCOUNT_FILES: targets[s] = []
    elif srv in SERVER_ACCOUNT_FILES: targets[srv] = []
    else: return jsonify({"error": f"invalid server_name '{srv}'"}), 400
    save_auto_targets(targets)
    return _jsonify({"ok": True, "cleared": srv or "ALL", "auto_targets": targets})

@app.get("/master/api/keys")
def master_get_keys():
    key = request.args.get("key", "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    cfg = _read_config()
    return _jsonify({"allowed_keys": cfg.get("ALLOWED_KEYS", {}),
                     "admin_keys": cfg.get("ADMIN_KEYS", []),
                     "reset_tz": cfg.get("RESET_TZ", "Asia/Dhaka")})

@app.post("/master/api/keys")
def master_save_keys():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    allowed = data.get("allowed_keys", {}); admin = data.get("admin_keys", [])
    tz = data.get("reset_tz", "Asia/Dhaka")
    if not isinstance(allowed, dict) or not isinstance(admin, list):
        return jsonify({"error": "invalid types"}), 400
    cfg = {"ALLOWED_KEYS": allowed, "ADMIN_KEYS": admin, "RESET_TZ": tz}
    content = json.dumps(cfg, indent=2, ensure_ascii=False)
    try: _write_file("keys.json", content)
    except Exception as e: return jsonify({"error": str(e)}), 500
    return _jsonify({"ok": True})

@app.get("/master/api/stats")
def master_stats():
    key = request.args.get("key", "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    auto_targets = load_auto_targets(); usage = _load_usage(); today = _today_str()
    today_usage = {}; total_requests = 0
    for k, v in usage.items():
        if v.get("date") == today:
            today_usage[k] = len(v.get("uids", [])); total_requests += len(v.get("uids", []))
    file_stats = {}
    for name in EDITABLE_FILES:
        path = os.path.join(BASE_DIR, name)
        if os.path.exists(path): file_stats[name] = os.path.getsize(path)
    jwt_status = {}
    for srv in SERVER_ACCOUNT_FILES:
        e = _jwt_cache.get(srv)
        if e:
            age = int(time.time() - e["ts"])
            jwt_status[srv] = {"tokens": len(e["tokens"]), "age_seconds": age,
                               "next_refresh_in": max(0, JWT_REFRESH_HOURS * 3600 - age)}
        else: jwt_status[srv] = {"tokens": 0, "age_seconds": None}
    return _jsonify({"today": today,
                     "auto_targets": {k: len(v) for k, v in auto_targets.items()},
                     "auto_targets_full": auto_targets, "today_usage": today_usage,
                     "total_today_requests": total_requests, "file_stats": file_stats,
                     "jwt_status": jwt_status,
                     "keys_count": {"user": len(get_allowed_keys()), "master": len(get_admin_keys())},
                     "blocked_count": db_blocked_count()})

@app.post("/master/api/run-auto")
def master_run_auto():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or request.args.get("key") or "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    threading.Thread(target=do_auto_like_now, daemon=True).start()
    return _jsonify({"ok": True, "message": "auto-like triggered"})

@app.post("/master/api/jwt-refresh")
def master_jwt_refresh():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or request.args.get("key") or "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    results = {}
    for srv in SERVER_ACCOUNT_FILES:
        try: results[srv] = len(get_or_refresh_tokens(srv, force=True))
        except Exception as e: results[srv] = f"err: {e}"
    return _jsonify({"ok": True, "refreshed": results})

@app.get("/master/api/auto-with-info")
def master_auto_with_info():
    key = request.args.get("key", "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    targets = load_auto_targets(); out = {}
    for srv, uids in targets.items():
        rows = []
        for uid in uids:
            info = db_get_uid_info(uid) or {}
            is_b = db_is_blocked(uid)
            last_like_count = None
            try:
                h, _ = db_history_query(uid=uid, page=1, page_size=1)
                if h: last_like_count = h[0].get("like_count", 0)
            except Exception: pass
            rows.append({"uid": uid, "nickname": info.get("nickname"),
                         "level": info.get("level"), "likes": info.get("likes"),
                         "headPic": info.get("head_pic"), "avatarUrl": info.get("avatar_url"),
                         "clanName": info.get("clan_name"), "clanId": info.get("clan_id"),
                         "region": info.get("region"), "info_date": info.get("last_check_date"),
                         "is_blocked": is_b, "last_likes_given": last_like_count})
        out[srv] = rows
    with _last_auto_run_lock:
        last_run = _last_auto_run.get("ts", 0); last_total = _last_auto_run.get("total", 0)
    last_run_str = datetime.fromtimestamp(last_run).strftime("%Y-%m-%d %H:%M") if last_run else "—"
    return _jsonify({"auto_with_info": out,
                     "last_run_summary": {"last_run": last_run_str, "last_run_total_likes": last_total}})

@app.get("/master/api/info-store")
def master_info_store():
    """সব stored uid_info রিটার্ন করে (Info Store tab এর জন্য)।"""
    key = request.args.get("key", "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    search = request.args.get("search", "").strip()
    page = max(1, request.args.get("page", 1, type=int) or 1)
    size = max(1, min(request.args.get("page_size", 50, type=int) or 50, 200))
    offset = (page - 1) * size
    with get_db() as conn:
        if search:
            total = conn.execute("SELECT COUNT(*) AS c FROM uid_info WHERE "
                                 "uid LIKE ? OR nickname LIKE ?",
                                 (f"%{search}%", f"%{search}%")).fetchone()["c"]
            rows = conn.execute("SELECT * FROM uid_info WHERE uid LIKE ? OR nickname LIKE ? "
                                "ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                                (f"%{search}%", f"%{search}%", size, offset)).fetchall()
        else:
            total = conn.execute("SELECT COUNT(*) AS c FROM uid_info").fetchone()["c"]
            rows = conn.execute("SELECT * FROM uid_info ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                                (size, offset)).fetchall()
    return _jsonify({"rows": [dict(r) for r in rows], "total": total,
                     "page": page, "page_size": size, "pages": (total + size - 1) // size})

@app.get("/master/api/usage-detail")
def master_usage_detail():
    key = request.args.get("key", "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    date = request.args.get("date") or _today_str()
    rows = []
    try:
        with get_db() as conn:
            res = conn.execute("""SELECT key_id, uid, MAX(nickname) AS nickname,
                MAX(server) AS server, COUNT(*) AS requests,
                COALESCE(SUM(like_count),0) AS likes_given_total,
                MAX(like_ts) AS last_ts
                FROM auto_like_history WHERE like_date=?
                GROUP BY key_id, uid ORDER BY last_ts DESC LIMIT 500""", (date,)).fetchall()
        for r in res:
            rows.append({"api_key": r["key_id"], "uid": r["uid"], "nickname": r["nickname"],
                         "server": r["server"], "requests": r["requests"],
                         "likes_given_total": r["likes_given_total"],
                         "last_at": (datetime.fromtimestamp(r["last_ts"]).strftime("%H:%M:%S")
                                     if r["last_ts"] else None)})
    except Exception as e: return jsonify({"error": f"db: {e}"}), 500
    return _jsonify({"date": date, "rows": rows})

@app.get("/master/api/blocked")
def master_blocked():
    key = request.args.get("key", "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    return _jsonify({"blocked": db_list_blocked()})

@app.post("/master/api/block")
def master_block():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or request.args.get("key") or "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    uid = str(data.get("uid") or "").strip()
    if not uid.isdigit(): return jsonify({"error": "invalid uid"}), 400
    reason = str(data.get("reason") or "master-block")[:200]
    db_block_uid(uid, by="master", reason=reason)
    return _jsonify({"ok": True, "uid": uid})

@app.post("/master/api/unblock")
def master_unblock():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or request.args.get("key") or "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    uid = str(data.get("uid") or "").strip()
    if not uid.isdigit(): return jsonify({"error": "invalid uid"}), 400
    db_unblock_uid(uid)
    return _jsonify({"ok": True, "uid": uid})

@app.get("/master/api/player-info")
def master_player_info():
    key = request.args.get("key", "").strip()
    if not _master_required(key): return jsonify({"error": "master key required"}), 403
    uid = request.args.get("uid", "").strip()
    if not uid.isdigit(): return jsonify({"error": "invalid uid"}), 400
    info = db_get_uid_info(uid)
    if not info:
        for srv in SERVER_ACCOUNT_FILES:
            try:
                tok = _global_jwt_for(srv)
                if tok:
                    info, _ = get_or_fetch_uid_info(uid, tok)
                    if info: break
            except Exception: continue
    rows = db_history_for_uid(uid, limit=500)
    history_by_day = {}
    for r in rows:
        d = r.get("like_date") or "?"
        history_by_day.setdefault(d, []).append({
            "like_ts": r.get("like_ts"), "like_count": r.get("like_count"),
            "status": r.get("request_status"), "key_id": r.get("key_id"),
            "source": r.get("key_id"),
            "snapshot": {"likes": info.get("likes") if info else None,
                         "level": info.get("level") if info else None,
                         "nickname": r.get("nickname")}})
    today = _today_str(); today_activity = {}
    for r in rows:
        if r.get("like_date") != today: continue
        kid = r.get("key_id") or "?"
        act = today_activity.setdefault(kid, {"api_key": kid, "likes_given_total": 0,
                                              "requests": 0, "last_at": None})
        act["likes_given_total"] += int(r.get("like_count") or 0); act["requests"] += 1
        ts = r.get("like_ts")
        if ts:
            t = datetime.fromtimestamp(ts).strftime("%H:%M:%S")
            if not act["last_at"] or t > act["last_at"]: act["last_at"] = t
    return _jsonify({"uid": uid,
                     "nickname": (info or {}).get("nickname"),
                     "avatarUrl": (info or {}).get("avatar_url"),
                     "headPic": (info or {}).get("head_pic"),
                     "level": (info or {}).get("level"),
                     "likes": (info or {}).get("likes"),
                     "region": (info or {}).get("region"),
                     "rank": (info or {}).get("rank"),
                     "csRank": (info or {}).get("cs_rank"),
                     "maxRank": (info or {}).get("max_rank"),
                     "csMaxRank": (info or {}).get("cs_max_rank"),
                     "clanName": (info or {}).get("clan_name"),
                     "clanId": (info or {}).get("clan_id"),
                     "clanLevel": (info or {}).get("clan_level"),
                     "clanMemberNum": (info or {}).get("clan_member_num"),
                     "clanCapacity": (info or {}).get("clan_capacity"),
                     "clanLeaderName": (info or {}).get("clan_leader_name"),
                     "clanLeaderUid": (info or {}).get("clan_leader_uid"),
                     "creditScore": (info or {}).get("credit_score"),
                     "lastLogin": (info or {}).get("last_login"),
                     "accountAge": (info or {}).get("account_age"),
                     "created": (info or {}).get("created"),
                     "signature": (info or {}).get("signature"),
                     "petName": (info or {}).get("pet_name"),
                     "petLevel": (info or {}).get("pet_level"),
                     "duoLevel": (info or {}).get("duo_level"),
                     "duoScore": (info or {}).get("duo_score"),
                     "duoPartnerUid": (info or {}).get("duo_partner_uid"),
                     "duoStatus": (info or {}).get("duo_status"),
                     "is_blocked": db_is_blocked(uid),
                     "today_like_activity": list(today_activity.values()),
                     "history_days": sorted(history_by_day.keys(), reverse=True),
                     "history": history_by_day,
                     "aggregate": db_aggregate_uid_history(uid)})

# ═══════════════ ENTRY ═══════════════
def _boot():
    init_db()
    start_background_jobs()

if __name__ == "__main__":
    _boot()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
else:
    try: _boot()
    except Exception as e: print(f"[boot] error: {e}")