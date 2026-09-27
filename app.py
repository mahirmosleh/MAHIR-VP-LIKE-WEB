# app.py — MAHIR VIP LIKE — Single File Backend
# ==========================================================
#  Web:  http://<server>:5000/
#  APIs:
#    /mahir&like?uid={uid}&key={key}                          ← SHORT (default BD)
#    /mahir&like?uid={uid}&key={key}&server_name=IND          ← SHORT + server
#    /like?uid={uid}&server_name={server}&key={key}           ← FULL
#    /health                                                   ← Status
#    /auto/list                                                ← Auto targets
#    /cron/auto_like?secret={secret}                           ← Vercel cron
# ==========================================================
#  auto.txt ফরম্যাট:
#    [BD]
#    1234567890
#    9876543210
#    [IND]
#    5555555555
# ==========================================================

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

from flask import Flask, request, jsonify, Response, render_template
from flask_cors import CORS
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from google.protobuf.json_format import MessageToJson

import requests
import aiohttp
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------- Safe orjson ----------
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

# ---------- Protobuf imports ----------
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

DAILY_LIMIT_USER = 1        # user key: 1 UID/day

SERVER_ACCOUNT_FILES = {
    "BD":  "account_bd.txt",
    "IND": "account_ind.txt",
    "BR":  "account_br.txt",
    "US":  "account_us.txt",
    "SAC": "account_sac.txt",
    "NA":  "account_na.txt",
}

CONFIG_RO_PATH = os.path.join(BASE_DIR, "keys.json")
CONFIG_RW_PATH = os.path.join("/tmp", "keys.json")
USAGE_PATH     = os.path.join("/tmp", "mahir_usage.json")
AUTO_FILE      = os.path.join(BASE_DIR, "auto.txt")

config_lock = RLock()
usage_lock  = RLock()
jwt_lock    = RLock()

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
    """Master = unlimited. User = 1 UID/day."""
    if tier == "master" or tier == "auto":
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
#  ACCOUNT LOADER (guest accounts per server)
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
#  AUTO.TXT LOADER — [SERVER] section format
# ============================================================
def load_auto_targets():
    """
    Read auto.txt:
      [BD]
      1234567890
      [IND]
      5555555555
    Returns dict: {"BD": ["1234567890", ...], ...}
    """
    if not os.path.exists(AUTO_FILE):
        return {}
    result = {}
    current = None
    with open(AUTO_FILE, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            if ln.startswith("[") and ln.endswith("]"):
                current = ln[1:-1].strip().upper()
                if current in SERVER_ACCOUNT_FILES:
                    result.setdefault(current, [])
                else:
                    current = None
                continue
            if current and ln.isdigit():
                result[current].append(ln)
    # dedupe
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
#  JWT (mahir-jwt-generator.vercel.app)
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
        return {"uid": uid,
                "likes": int(ai.get("Likes", 0)),
                "name": str(ai.get("PlayerNickname", ""))}
    except Exception:
        return None


# ============================================================
#  CORE LIKE FLOW
# ============================================================
_api_key_ctx = threading.local()


def do_like(uid, server_name, tier="user"):
    """Core like function. tier='user'|'master'|'auto'"""
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
            "GiftCount": 0,
            "accounts_loaded": len(accounts),
            "tokens_generated": len(tokens),
            "server_name": server_name,
            "tier": tier.upper(),
            "quota_remaining": remaining,
            "Owner": OWNER_HANDLE,
            "status": 0,
        }

    url = like_url_for(server_name)
    send_likes_from_all_tokens(uid, server_name, url, tokens)

    after = parse_account_info(make_request(encrypted, server_name, token)) or {
        "likes": before["likes"], "uid": before["uid"], "name": before["name"]
    }
    like_given = max(0, int(after["likes"]) - int(before["likes"]))

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
#  AUTO-LIKE (4:10 AM) + JWT REFRESH (every 7h)
# ============================================================
def do_auto_like_now():
    print(f"\n[AUTO-LIKE] start {datetime.now():%Y-%m-%d %H:%M:%S}")
    targets = load_auto_targets()
    total_sent = 0
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
                try:
                    ok = send_likes_from_all_tokens(uid, srv, url, tokens)
                    total_sent += ok
                    print(f"[AUTO-LIKE] {srv} {uid} → {ok}")
                except Exception as e:
                    print(f"[AUTO-LIKE] {srv} {uid} err: {e}")
                time.sleep(0.5)
        except Exception as e:
            print(f"[AUTO-LIKE] {srv} err: {e}")
    print(f"[AUTO-LIKE] done. total={total_sent}\n")


def _auto_like_scheduler():
    while True:
        try:
            now = datetime.now()
            target = now.replace(hour=AUTO_LIKE_HOUR, minute=AUTO_LIKE_MINUTE,
                                 second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            wait = (target - now).total_seconds()
            print(f"[SCHED] next auto-like {target:%Y-%m-%d %H:%M:%S} ({wait/3600:.2f}h)")
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
            print("[JWT-REFRESH] done\n")
        except Exception as e:
            print(f"[JWT-REFRESH] err: {e}")
            time.sleep(60)


def start_background_jobs():
    threading.Thread(target=_auto_like_scheduler, daemon=True).start()
    threading.Thread(target=_jwt_refresh_scheduler, daemon=True).start()
    print("[*] Background jobs started")


# ============================================================
#  ROUTES
# ============================================================
@app.get("/")
def index():
    return render_template(
        "index.html",
        brand=BRAND_NAME, dev=DEV_NAME, tg=TELEGRAM,
        tiktok=TIKTOK, website=WEBSITE,
        owner=OWNER_HANDLE, badge=BADGE_TEXT,
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
        "jwt_refresh_hours": JWT_REFRESH_HOURS,
        "daily_limit_user": DAILY_LIMIT_USER,
        "daily_limit_master": "unlimited",
        "auto_targets_loaded": {k: len(v) for k, v in load_auto_targets().items()},
        "endpoints": {
            "short": "/mahir&like?uid={uid}&key={key}",
            "short_with_server": "/mahir&like?uid={uid}&key={key}&server_name={server}",
            "full": "/like?uid={uid}&server_name={server}&key={key}",
            "auto_list": "/auto/list",
            "health": "/health",
        },
    })


# ---------- FULL API: /like ----------
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
            return jsonify(result), 500

        return jsonify(result)
    except Exception as e:
        return jsonify({"error": "runtime_error", "detail": str(e)}), 500


# ---------- SHORT API: /mahir&like ----------
@app.get("/mahir&like")
def mahir_like():
    """
    Short like endpoint.
      /mahir&like?uid=1234567890&key=MAHIR-USER-001                 (default BD)
      /mahir&like?uid=1234567890&key=MAHIR-USER-001&server_name=IND (server specified)
    """
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
            return jsonify(result), 500

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": "runtime_error", "detail": str(e)}), 500


# ---------- AUTO endpoints ----------
@app.post("/auto/add")
def auto_add():
    try:
        api_key = (request.args.get("key") or "").strip()
        if classify_key(api_key) != "master":
            return jsonify({"error": "Master key required"}), 403
        data = request.get_json(silent=True) or {}
        uid = str(data.get("uid", "")).strip()
        srv = str(data.get("server_name", "")).upper().strip()
        if not uid.isdigit() or srv not in SERVER_ACCOUNT_FILES:
            return jsonify({"error": "uid (digits) and valid server_name required"}), 400
        register_auto_uid(srv, uid)
        return jsonify({"ok": True, "server": srv, "uid": uid,
                        "auto_targets": load_auto_targets()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/auto/list")
def auto_list():
    return _jsonify({"auto_targets": load_auto_targets()})


@app.post("/auto/run")
def auto_run_now():
    try:
        api_key = (request.args.get("key") or "").strip()
        if classify_key(api_key) != "master":
            return jsonify({"error": "Master key required"}), 403
        threading.Thread(target=do_auto_like_now, daemon=True).start()
        return jsonify({"ok": True, "message": "Auto-like triggered in background"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/cron/auto_like")
def cron_auto_like():
    secret = request.args.get("secret", "")
    expected = os.environ.get("CRON_SECRET", "mahir-cron-2025")
    if secret != expected:
        return jsonify({"error": "forbidden"}), 403
    threading.Thread(target=do_auto_like_now, daemon=True).start()
    return jsonify({"ok": True, "triggered": True})


# ============================================================
#  ENTRY
# ============================================================
if __name__ == "__main__":
    start_background_jobs()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)