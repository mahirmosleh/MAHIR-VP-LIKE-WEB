# app.py — MAHIR VIP LIKE — Complete Backend with Master Panel + Info DB + Blocklist
# ==================================================================================
#  Web:
#    /              → User UI
#    /master        → Master Admin Panel
#
#  Public APIs:
#    /mahir&like?uid={uid}&key={key}
#    /mahir&like?uid={uid}&key={key}&server_name=IND
#    /like?uid={uid}&server_name={server}&key={key}
#    /health
#    /auto/list
#    /cron/auto_like?secret={secret}
#
#  Master APIs:
#    GET    /master/api/files
#    GET    /master/api/file?name=auto.txt
#    POST   /master/api/file
#    POST   /master/api/upload
#    POST   /master/api/auto/bulk-upload
#    POST   /master/api/auto/add
#    POST   /master/api/auto/remove
#    POST   /master/api/auto/clear
#    GET    /master/api/auto-with-info        ← NEW
#    GET    /master/api/player-info?uid=X     ← NEW
#    GET    /master/api/usage-detail?date=Y   ← NEW
#    GET    /master/api/blocked               ← NEW
#    POST   /master/api/block                 ← NEW
#    POST   /master/api/unblock               ← NEW
#    GET    /master/api/info-db               ← NEW
#    POST   /master/api/info-db/clear         ← NEW
#    POST   /master/api/info-db/fetch         ← NEW
#    GET    /master/api/keys
#    POST   /master/api/keys
#    GET    /master/api/stats
#    POST   /master/api/run-auto
#    POST   /master/api/jwt-refresh
# ==================================================================================

import os
import sys
import json
import time
import binascii
import asyncio
import threading
from threading import RLock
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import (Flask, request, jsonify, Response, render_template)
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

INFO_API_BASE     = "https://mahir-info-api.vercel.app"

INFO_DB_PATH      = os.path.join("/tmp", "mahir_info_db.json")
USAGE_DB_PATH     = os.path.join("/tmp", "mahir_usage_detail.json")
BLOCKED_DB_PATH   = os.path.join("/tmp", "mahir_blocked.json")
INFO_DB_LOCK      = RLock()
USAGE_DB_LOCK     = RLock()
BLOCKED_DB_LOCK   = RLock()

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
#  USAGE / QUOTA  (daily quota for user keys)
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
#  USAGE DETAIL DB  (for /master/api/usage-detail)
# ============================================================
def _load_usage_detail():
    with USAGE_DB_LOCK:
        if not os.path.exists(USAGE_DB_PATH):
            return {}
        try:
            with open(USAGE_DB_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}


def _save_usage_detail(db):
    with USAGE_DB_LOCK:
        try:
            with open(USAGE_DB_PATH, "w", encoding="utf-8") as f:
                json.dump(db, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[USAGE-DB] save error: {e}")


def log_usage_detail(api_key, uid, server_name, nickname, likes_given):
    """Record one like-call for daily usage detail view."""
    today = _today_str()
    db = _load_usage_detail()
    day = db.setdefault(today, {})
    entry_key = f"{api_key}|{uid}"
    e = day.get(entry_key, {
        "api_key": api_key,
        "uid": str(uid),
        "server": server_name,
        "nickname": nickname or "",
        "requests": 0,
        "likes_given_total": 0,
        "last_at": "",
    })
    e["requests"] += 1
    e["likes_given_total"] += int(likes_given or 0)
    e["nickname"] = nickname or e.get("nickname", "")
    e["server"] = server_name or e.get("server", "")
    e["last_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    day[entry_key] = e
    _save_usage_detail(db)


# ============================================================
#  BLOCKED UID DB
# ============================================================
def _load_blocked():
    with BLOCKED_DB_LOCK:
        if not os.path.exists(BLOCKED_DB_PATH):
            return set()
        try:
            with open(BLOCKED_DB_PATH, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()


def _save_blocked(s):
    with BLOCKED_DB_LOCK:
        try:
            with open(BLOCKED_DB_PATH, "w", encoding="utf-8") as f:
                json.dump(sorted(list(s)), f)
        except Exception:
            pass


def is_blocked_uid(uid):
    return str(uid) in _load_blocked()


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


def get_global_jwt():
    for srv in ("BD", "IND", "BR", "US", "SAC", "NA"):
        toks = get_or_refresh_tokens(srv)
        if toks:
            return toks[0]
    return None


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
        return {"uid": uid,
                "likes": int(ai.get("Likes", 0)),
                "name": str(ai.get("PlayerNickname", ""))}
    except Exception:
        return None


# ============================================================
#  INFO DATABASE  (daily cached player info)
# ============================================================
def _load_info_db():
    with INFO_DB_LOCK:
        if not os.path.exists(INFO_DB_PATH):
            return {"records": {}, "last_run": None}
        try:
            with open(INFO_DB_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"records": {}, "last_run": None}


def _save_info_db(db):
    with INFO_DB_LOCK:
        try:
            with open(INFO_DB_PATH, "w", encoding="utf-8") as f:
                json.dump(db, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[INFO-DB] save error: {e}")


def _fetch_info_api(jwt, uid):
    """Call /info and /duo endpoints from mahir-info-api."""
    result = {"info": None, "duo": None}
    for ep in ("info", "duo"):
        try:
            url = f"{INFO_API_BASE}/{ep}?jwt={jwt}&uid={uid}"
            r = requests.get(url, timeout=25, verify=False)
            if r.status_code == 200:
                try:
                    result[ep] = r.json()
                except Exception:
                    result[ep] = {"raw": r.text}
            else:
                result[ep] = {"error": f"HTTP {r.status_code}"}
        except Exception as e:
            result[ep] = {"error": str(e)}
        time.sleep(0.3)
    return result


def _extract_player_info(info_json, uid):
    """
    Normalize unknown response shape from /info.
    Try to pull common fields regardless of nesting.
    """
    if not isinstance(info_json, dict):
        return {}
    # try common wrappers
    for key in ("data", "account", "player", "result", "user"):
        if isinstance(info_json.get(key), dict):
            info_json = info_json[key]
            break

    def pick(*names, default=None):
        for n in names:
            if n in info_json and info_json[n] not in (None, ""):
                return info_json[n]
        return default

    return {
        "nickname": pick("nickname", "PlayerNickname", "name", "Name"),
        "likes": pick("likes", "Likes", "likeCount", "totalLikes"),
        "level": pick("level", "Level", "accountLevel"),
        "region": pick("region", "Region", "server"),
        "uid": pick("uid", "UID", "id") or str(uid),
        "headPic": pick("headPic", "headpic", "avatar", "avatarId", "HeadPic"),
        "exp": pick("exp", "Exp", "experience"),
        "rank": pick("rank", "Rank", "brRank", "BRRank"),
        "csRank": pick("csRank", "CSRank", "cs_rank"),
        "clanName": pick("clanName", "ClanName", "guildName", "GuildName"),
        "clanId": pick("clanId", "ClanId", "guildId", "GuildId"),
        "clanLevel": pick("clanLevel", "ClanLevel", "guildLevel"),
        "clanLeaderName": pick("clanLeaderName", "ClanLeaderName", "leaderName"),
        "clanLeaderUid": pick("clanLeaderUid", "ClanLeaderUid", "leaderUid"),
        "signature": pick("signature", "Signature", "bio"),
        "creditScore": pick("creditScore", "CreditScore", "credit"),
        "lastLogin": pick("lastLogin", "LastLogin"),
        "created": pick("created", "Created", "createdAt"),
        "accountAge": pick("accountAge", "AccountAge"),
    }


def fetch_and_store_info_for_uid(uid, server_name, jwt=None):
    if not jwt:
        jwt = get_global_jwt()
    if not jwt:
        print(f"[INFO] no JWT available for {uid}")
        return None

    raw = _fetch_info_api(jwt, uid)
    player = _extract_player_info(raw.get("info") or {}, uid)

    db = _load_info_db()
    rec = {
        "uid": str(uid),
        "server_name": server_name.upper(),
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "player": player,
        "info": raw.get("info"),
        "duo": raw.get("duo"),
    }
    db["records"][str(uid)] = rec
    db["last_run"] = rec["fetched_at"]
    _save_info_db(db)
    return rec


def fetch_and_store_all_info(force=False):
    targets = load_auto_targets()
    db = _load_info_db()
    today = _today_str()
    jwt = get_global_jwt()

    if not jwt:
        print("[INFO] No JWT available — skipping info fetch")
        return {"ok": False, "error": "no JWT available"}

    fetched = 0
    skipped = 0
    errors = 0

    for srv, uids in targets.items():
        if not uids:
            continue
        for uid in uids:
            existing = db["records"].get(str(uid))
            if (not force) and existing and existing.get("fetched_at", "").startswith(today):
                skipped += 1
                continue
            try:
                rec = fetch_and_store_info_for_uid(uid, srv, jwt)
                if rec:
                    fetched += 1
                    print(f"[INFO] {srv} {uid} → fetched")
                else:
                    errors += 1
            except Exception as e:
                errors += 1
                print(f"[INFO] {srv} {uid} error: {e}")
            time.sleep(0.5)

    print(f"[INFO] done. fetched={fetched} skipped={skipped} errors={errors}")
    return {"ok": True, "fetched": fetched, "skipped": skipped, "errors": errors}


# ============================================================
#  CORE LIKE
# ============================================================
_api_key_ctx = threading.local()


def do_like(uid, server_name, tier="user"):
    # Blocked UID check
    if is_blocked_uid(uid):
        return {
            "error": "This UID is blocked by master.",
            "blocked": True,
        }

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
        # log even if unknown
        log_usage_detail(api_key, uid, server_name, "Unknown", 0)
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

    after = parse_account_info(make_request(encrypted, server_name, token)) or {
        "likes": before["likes"], "uid": before["uid"], "name": before["name"]
    }
    like_given = max(0, int(after["likes"]) - int(before["likes"]))

    log_usage_detail(api_key, uid, server_name, after["name"], like_given)

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
#  AUTO-LIKE
# ============================================================
def do_auto_like_now():
    print(f"\n[AUTO-LIKE] start {datetime.now():%Y-%m-%d %H:%M:%S}")
    targets = load_auto_targets()
    total = 0
    for srv, uids in targets.items():
        if not uids:
            continue
        try:
            tokens = get_or_refresh_tokens(srv, force=True)
            if not tokens:
                print(f"[AUTO-LIKE] {srv}: no tokens")
                continue
            url = like_url_for(srv)
            for uid in uids:
                if is_blocked_uid(uid):
                    print(f"[AUTO-LIKE] {srv} {uid} → SKIPPED (blocked)")
                    continue
                try:
                    ok = send_likes_from_all_tokens(uid, srv, url, tokens)
                    total += ok
                    print(f"[AUTO-LIKE] {srv} {uid} → {ok}")
                except Exception as e:
                    print(f"[AUTO-LIKE] {srv} {uid} err: {e}")
                time.sleep(0.5)
        except Exception as e:
            print(f"[AUTO-LIKE] {srv} err: {e}")
    print(f"[AUTO-LIKE] done. total={total}\n")

    # Fetch & store player info (once per day)
    try:
        print("[AUTO-LIKE] fetching info for all targets...")
        fetch_and_store_all_info()
    except Exception as e:
        print(f"[AUTO-LIKE] info fetch error: {e}")

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
        "info_api": INFO_API_BASE,
        "servers": list(SERVER_ACCOUNT_FILES.keys()),
        "auto_like_at": f"{AUTO_LIKE_HOUR:02d}:{AUTO_LIKE_MINUTE:02d}",
        "jwt_refresh_hours": JWT_REFRESH_HOURS,
        "daily_limit_user": DAILY_LIMIT_USER,
        "auto_targets_loaded": {k: len(v) for k, v in load_auto_targets().items()},
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

        _api_key_ctx.key = api_key
        try:
            register_auto_uid(server_name, uid)
        except Exception as e:
            print(f"[auto-register] warn: {e}")

        result = do_like(uid, server_name, tier=tier)
        if result.get("error"):
            if "Daily limit" in result["error"]:
                return jsonify(result), 429
            if result.get("blocked"):
                return jsonify(result), 403
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

        _api_key_ctx.key = api_key
        try:
            register_auto_uid(server_name, uid)
        except Exception as e:
            print(f"[auto-register] warn: {e}")

        result = do_like(uid, server_name, tier=tier)
        if result.get("error"):
            if "Daily limit" in result["error"]:
                return jsonify(result), 429
            if result.get("blocked"):
                return jsonify(result), 403
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

    # Immediately fetch info for this new UID (async-ish)
    def _bg():
        try:
            fetch_and_store_info_for_uid(uid, srv)
        except Exception as e:
            print(f"[auto/add] info fetch warn: {e}")
    threading.Thread(target=_bg, daemon=True).start()

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

    # Also remove from info DB
    db = _load_info_db()
    if uid in db.get("records", {}):
        del db["records"][uid]
        _save_info_db(db)

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
#  AUTO-WITH-INFO  (targets + cached info + last run)
# ============================================================
@app.get("/master/api/auto-with-info")
def master_auto_with_info():
    """
    Returns:
      {
        auto_with_info: { "BD": [ {uid, nickname, level, likes, avatarUrl, ...} ] },
        last_run_summary: { last_run: "YYYY-MM-DD HH:MM:SS" }
      }
    """
    key = request.args.get("key", "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    targets = load_auto_targets()
    db = _load_info_db()
    records = db.get("records", {})
    blocked = _load_blocked()

    out = {}
    for srv, uids in targets.items():
        rows = []
        for uid in uids:
            rec = records.get(str(uid)) or {}
            player = rec.get("player") or {}
            rows.append({
                "uid": str(uid),
                "nickname": player.get("nickname"),
                "level": player.get("level"),
                "likes": player.get("likes"),
                "region": player.get("region") or srv,
                "headPic": player.get("headPic"),
                "clanName": player.get("clanName"),
                "clanId": player.get("clanId"),
                "avatarUrl": None,
                "info_date": (rec.get("fetched_at") or "")[:10],
                "is_blocked": str(uid) in blocked,
                "last_likes_given": None,  # can be enhanced later
            })
        out[srv] = rows

    return _jsonify({
        "auto_with_info": out,
        "last_run_summary": {"last_run": db.get("last_run")},
    })


# ============================================================
#  PLAYER INFO  (single UID — from cache; no fresh fetch)
# ============================================================
@app.get("/master/api/player-info")
def master_player_info():
    key = request.args.get("key", "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    uid = request.args.get("uid", "").strip()
    if not uid:
        return jsonify({"error": "uid required"}), 400

    db = _load_info_db()
    rec = db.get("records", {}).get(str(uid), {})
    player = rec.get("player") or {}

    # Today's like activity from usage detail
    today = _today_str()
    usage_db = _load_usage_detail()
    day = usage_db.get(today, {})
    activity = []
    for k, v in day.items():
        if v.get("uid") == str(uid):
            activity.append({
                "api_key": v.get("api_key"),
                "requests": v.get("requests"),
                "likes_given_total": v.get("likes_given_total"),
                "last_at": v.get("last_at"),
            })

    return _jsonify({
        "uid": str(uid),
        "nickname": player.get("nickname"),
        "level": player.get("level"),
        "likes": player.get("likes"),
        "region": player.get("region"),
        "headPic": player.get("headPic"),
        "avatarUrl": None,
        "rank": player.get("rank"),
        "csRank": player.get("csRank"),
        "clanName": player.get("clanName"),
        "clanId": player.get("clanId"),
        "clanLevel": player.get("clanLevel"),
        "clanLeaderName": player.get("clanLeaderName"),
        "clanLeaderUid": player.get("clanLeaderUid"),
        "signature": player.get("signature"),
        "creditScore": player.get("creditScore"),
        "lastLogin": player.get("lastLogin"),
        "created": player.get("created"),
        "accountAge": player.get("accountAge"),
        "is_blocked": is_blocked_uid(uid),
        "today_like_activity": activity,
        "history_days": [],
        "history": {},
        "fetched_at": rec.get("fetched_at"),
        "raw_info": rec.get("info"),
        "raw_duo": rec.get("duo"),
    })


# ============================================================
#  USAGE DETAIL  (per-day table)
# ============================================================
@app.get("/master/api/usage-detail")
def master_usage_detail():
    key = request.args.get("key", "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    date = request.args.get("date", "").strip() or _today_str()
    db = _load_usage_detail()
    day = db.get(date, {})
    rows = list(day.values())
    rows.sort(key=lambda r: r.get("last_at", ""), reverse=True)
    return _jsonify({"date": date, "rows": rows})


# ============================================================
#  BLOCKED  (list / add / remove)
# ============================================================
@app.get("/master/api/blocked")
def master_blocked_list():
    key = request.args.get("key", "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403
    return _jsonify({"blocked": sorted(list(_load_blocked()))})


@app.post("/master/api/block")
def master_block_add():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403
    uid = str(data.get("uid", "")).strip()
    if not uid.isdigit():
        return jsonify({"error": "uid required"}), 400

    s = _load_blocked()
    s.add(uid)
    _save_blocked(s)
    return _jsonify({"ok": True, "blocked": sorted(list(s))})


@app.post("/master/api/unblock")
def master_block_remove():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403
    uid = str(data.get("uid", "")).strip()
    if not uid:
        return jsonify({"error": "uid required"}), 400

    s = _load_blocked()
    s.discard(uid)
    _save_blocked(s)
    return _jsonify({"ok": True, "blocked": sorted(list(s))})


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
#  STATS
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

    info_db = _load_info_db()
    info_stats = {
        "total_records": len(info_db.get("records", {})),
        "last_run": info_db.get("last_run"),
    }

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
        "info_db": info_stats,
        "blocked_count": len(_load_blocked()),
    })


# ============================================================
#  ACTIONS
# ============================================================
@app.post("/master/api/run-auto")
def master_run_auto():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or request.args.get("key") or "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403
    threading.Thread(target=do_auto_like_now, daemon=True).start()
    return _jsonify({"ok": True, "message": "auto-like triggered"})


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
#  INFO DB APIs
# ============================================================
@app.get("/master/api/info-db")
def master_info_db():
    key = request.args.get("key", "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    srv = request.args.get("server_name", "").upper().strip()
    db = _load_info_db()
    records = db.get("records", {})

    if srv:
        records = {k: v for k, v in records.items()
                   if v.get("server_name") == srv}

    return _jsonify({
        "ok": True,
        "last_run": db.get("last_run"),
        "count": len(records),
        "records": records,
    })


@app.post("/master/api/info-db/clear")
def master_info_db_clear():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    srv = str(data.get("server_name", "")).upper().strip()
    db = _load_info_db()

    if not srv or srv == "ALL":
        db = {"records": {}, "last_run": None}
    else:
        if srv not in SERVER_ACCOUNT_FILES:
            return jsonify({"error": f"invalid server_name '{srv}'"}), 400
        db["records"] = {k: v for k, v in db.get("records", {}).items()
                         if v.get("server_name") != srv}

    _save_info_db(db)
    return _jsonify({"ok": True, "cleared": srv or "ALL",
                     "remaining": len(db.get("records", {}))})


@app.post("/master/api/info-db/fetch")
def master_info_db_fetch():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or request.args.get("key") or "").strip()
    if not _master_required(key):
        return jsonify({"error": "master key required"}), 403

    force = str(data.get("force", "")).lower() in ("1", "true", "yes")

    def _run():
        try:
            fetch_and_store_all_info(force=force)
        except Exception as e:
            print(f"[info-db/fetch] error: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return _jsonify({"ok": True, "message": "info fetch triggered",
                     "force": force})


# ============================================================
#  ENTRY
# ============================================================
if __name__ == "__main__":
    start_background_jobs()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
else:
    try:
        start_background_jobs()
    except Exception as e:
        print(f"[boot] background jobs error: {e}")