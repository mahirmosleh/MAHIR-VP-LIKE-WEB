import uuid
import time
import binascii
import base64
import json
import os
import threading
from datetime import datetime

from flask import Flask, request, make_response, send_file
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
import requests
import urllib3

# ====== DEFENSIVE PROTOBUF IMPORT ======
try:
    from google.protobuf.internal.decoder import _DecodeVarint, _DecodeVarint32
except ImportError:
    def _DecodeVarint(buf, pos):
        result = 0
        shift = 0
        while True:
            b = buf[pos]
            result |= (b & 0x7f) << shift
            pos += 1
            if not (b & 0x80):
                break
            shift += 7
        return result, pos

    def _DecodeVarint32(buf, pos):
        return _DecodeVarint(buf, pos)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    import jwt as pyjwt
    HAS_JWT = True
except ImportError:
    HAS_JWT = False

# ====== PRETTY JSON ======
try:
    import orjson
    def _jsonify(data, status=200):
        return make_response(
            orjson.dumps(data, option=orjson.OPT_INDENT_2),
            status,
            {'Content-Type': 'application/json'}
        )
except ImportError:
    import json as _json_mod
    def _jsonify(data, status=200):
        return make_response(
            _json_mod.dumps(data, indent=2, ensure_ascii=False),
            status,
            {'Content-Type': 'application/json'}
        )

app = Flask(__name__)

# ====== GLOBAL SESSION ======
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(
    pool_connections=64, pool_maxsize=64, max_retries=0, pool_block=False
)
session.mount('https://', adapter)
session.mount('http://', adapter)

# ====== AES KEYS ======
AES_KEY = b'Yg&tc%DEuh6%Zc^8'
AES_IV  = b'6oyZDr22E3ychjM%'

# ====== VERSION & URLs ======
LOGIN_URL = "https://loginbp.ppmainecoonghj.com"
MAJOR_LOGIN_URL = LOGIN_URL + "/MajorLogin"
OAUTH_URL = "https://100067.connect.garena.com/oauth/guest/token/grant"
INSPECT_URL = "https://100067.connect.garena.com/oauth/token/inspect"
FREEFIRE_UPDATE_URL = "https://clientbp.ppmainecoonghj.com/UpdateSocialBasicInfo"
OB_VERSION = "OB55"
CLIENT_VERSION = "1.132.1"
FREEFIRE_VERSION = OB_VERSION

# ====== EXTERNAL JWT API ======
EXTERNAL_JWT_API = "https://mahir-jwt-generator.vercel.app/token"

# ====== JSON STORAGE (Region-wise) ======
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BD_JSON = os.path.join(BASE_DIR, "bd.json")
IND_JSON = os.path.join(BASE_DIR, "ind.json")
SAVE_LOCK = threading.Lock()

def load_json_list(path):
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def save_json_list(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def append_account_to_region_json(uid, password, bio, jwt_token, region,
                                   name=None, account_id=None,
                                   access_token=None, open_id=None):
    """region অনুযায়ী bd.json বা ind.json এ অ্যাকাউন্ট যোগ/আপডেট করে"""
    region = (region or "").upper()
    if region in ("BD", "BGD", "BANGLADESH"):
        path = BD_JSON
    elif region in ("IN", "IND", "INDIA"):
        path = IND_JSON
    else:
        return

    with SAVE_LOCK:
        data = load_json_list(path)
        existing = next((i for i, x in enumerate(data)
                         if str(x.get("uid")) == str(uid)), None)
        entry = {
            "uid": int(uid) if str(uid).isdigit() else uid,
            "password": password,
            "name": name,
            "account_id": account_id,
            "region": region,
            "jwt_token": jwt_token,
            "access_token": access_token,
            "open_id": open_id,
            "bio": bio,
            "bio_updated": True,
            "created_at": datetime.utcnow().isoformat()
        }
        if existing is not None:
            data[existing] = entry
        else:
            data.append(entry)
        save_json_list(path, data)

# ====== PROTOBUF HELPERS ======
def encode_varint(value):
    out = []
    while True:
        b = value & 0x7F
        value >>= 7
        if value:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)

def build_payload_from_dict(fields_dict):
    payload = b''
    for key, value in sorted(fields_dict.items()):
        field_num = int(key)
        if isinstance(value, bool):
            payload += encode_varint((field_num << 3) | 0) + encode_varint(1 if value else 0)
        elif isinstance(value, int):
            payload += encode_varint((field_num << 3) | 0) + encode_varint(value)
        elif isinstance(value, str):
            data = value.encode('utf-8')
            payload += encode_varint((field_num << 3) | 2) + encode_varint(len(data)) + data
        elif isinstance(value, bytes):
            payload += encode_varint((field_num << 3) | 2) + encode_varint(len(value)) + value
        elif isinstance(value, dict):
            sub = build_payload_from_dict(value)
            payload += encode_varint((field_num << 3) | 2) + encode_varint(len(sub)) + sub
        else:
            raise TypeError(f"Unsupported type for field {field_num}")
    return payload

def decode_protobuf(data):
    pos = 0
    length = len(data)
    fields = {}
    while pos < length:
        key, pos = _DecodeVarint(data, pos)
        field_number = key >> 3
        wire_type = key & 7
        if wire_type == 0:
            value, pos = _DecodeVarint(data, pos)
        elif wire_type == 2:
            size, pos = _DecodeVarint32(data, pos)
            raw = data[pos:pos + size]
            pos += size
            try:
                value = decode_protobuf(raw)
            except Exception:
                value = raw
        elif wire_type == 5:
            value = int.from_bytes(data[pos:pos + 4], 'little'); pos += 4
        elif wire_type == 1:
            value = int.from_bytes(data[pos:pos + 8], 'little'); pos += 8
        else:
            raise ValueError(f"Unsupported wire type {wire_type}")
        if field_number in fields:
            if not isinstance(fields[field_number], list):
                fields[field_number] = [fields[field_number]]
            fields[field_number].append(value)
        else:
            fields[field_number] = value
    return fields

# ====== BIO UPLOAD ======
BIO_HEADERS = {
    "Expect": "100-continue",
    "X-Unity-Version": "2018.4.11f1",
    "X-GA": "v1 1",
    "ReleaseVersion": FREEFIRE_VERSION,
    "Content-Type": "application/x-www-form-urlencoded",
    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 11; SM-A305F Build/RP1A.200720.012)",
    "Connection": "Keep-Alive",
    "Accept-Encoding": "gzip",
}

def encrypt_bio_data(data_bytes):
    cipher = AES.new(AES_KEY, AES.MODE_CBC, AES_IV)
    return cipher.encrypt(pad(data_bytes, AES.block_size))

def _bytes_to_readable(b: bytes) -> str:
    """বাইনারি ডেটা থেকে hex + ascii দুটোই দেখায়"""
    if not b:
        return ""
    hex_str = b.hex()
    try:
        txt = b.decode('utf-8')
        printable = all(c.isprintable() or c in '\r\n\t' for c in txt)
        if printable and txt.strip():
            return f'"{txt}" (hex={hex_str[:60]}...)'
    except Exception:
        pass
    return f"hex={hex_str[:80]}"

def _hex_to_ascii(hex_str):
    """hex string কে protobuf/ascii তে কনভার্ট করে দেখানোর জন্য"""
    if not hex_str:
        return ""
    try:
        raw = bytes.fromhex(hex_str)
    except Exception:
        return hex_str
    try:
        dec = decode_protobuf(raw)
        parts = []
        for k, v in dec.items():
            if isinstance(v, bytes):
                parts.append(f"{k}: {_bytes_to_readable(v)}")
            elif isinstance(v, dict):
                sub = ", ".join(f"{sk}:{_bytes_to_readable(sv) if isinstance(sv,bytes) else sv}"
                                for sk, sv in v.items())
                parts.append(f"{k}: {{{sub}}}")
            else:
                parts.append(f"{k}: {v}")
        return " | ".join(parts)
    except Exception:
        pass
    try:
        txt = raw.decode('utf-8', errors='replace')
        if txt and any(c.isprintable() for c in txt):
            return txt
    except Exception:
        pass
    return hex_str

def upload_bio_request(jwt_token, bio_text):
    try:
        fields = {2: 17, 5: {}, 6: {}, 8: bio_text, 9: 1, 11: {}, 12: {}}
        data_bytes = build_payload_from_dict(fields)
        encrypted = encrypt_bio_data(data_bytes)
        headers = BIO_HEADERS.copy()
        headers["Authorization"] = f"Bearer {jwt_token}"
        resp = session.post(FREEFIRE_UPDATE_URL, headers=headers,
                            data=encrypted, timeout=15, verify=False)
        raw_hex = binascii.hexlify(resp.content).decode('utf-8')
        ok = (resp.status_code == 200)
        return {
            "status": "success" if ok else "failed",
            "code": resp.status_code,
            "server_response": raw_hex,
            "server_ascii": _hex_to_ascii(raw_hex)
        }
    except Exception as e:
        return {"status": "failed", "code": 500,
                "server_response": str(e), "server_ascii": str(e)}

# ====== EXTERNAL JWT FETCHER ======
def fetch_jwt_from_external(uid, password):
    """বাইরের API থেকে সম্পূর্ণ JWT রেসপন্স আনে"""
    try:
        r = session.get(
            EXTERNAL_JWT_API,
            params={"uid": uid, "password": password},
            timeout=30,
            verify=False
        )
        if r.status_code != 200:
            return None, f"External API HTTP {r.status_code}"
        data = r.json()
        if str(data.get("status", "")).lower() != "success":
            return None, data.get("message") or "External API failed"
        if not data.get("jwt_token"):
            return None, "jwt_token missing in response"
        return data, None
    except Exception as e:
        return None, f"External API error: {e}"

# ====== JWT DECODER (শুধু display-এর জন্য) ======
XOR_SECRET = b"1e5898ccb8dfdd921f9bdea848768b64a201"

def decode_nickname(encoded):
    if not isinstance(encoded, str) or not encoded:
        return encoded
    for decoder in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            s = encoded + '=' * ((4 - len(encoded) % 4) % 4)
            raw = decoder(s)
            dec = bytes(b ^ XOR_SECRET[i % len(XOR_SECRET)]
                        for i, b in enumerate(raw))
            txt = dec.decode('utf-8', errors='replace')
            if txt and '\ufffd' not in txt:
                return txt
        except Exception:
            continue
    return encoded

def _decode_jwt_payload(token):
    try:
        parts = token.split('.')
        if len(parts) < 2:
            return None
        p = parts[1] + '=' * ((4 - len(parts[1]) % 4) % 4)
        try:
            raw = base64.urlsafe_b64decode(p)
        except Exception:
            raw = base64.b64decode(p)
        return json.loads(raw.decode('utf-8'))
    except Exception:
        return None

def decode_jwt_info(token):
    if not token:
        return None, None, None, None
    payload = _decode_jwt_payload(token)
    if payload is None and HAS_JWT:
        try:
            payload = pyjwt.decode(token, options={"verify_signature": False})
        except Exception:
            payload = None
    if payload is None:
        return None, None, None, None
    try:
        nickname = payload.get("nickname")
        if isinstance(nickname, str):
            nickname = decode_nickname(nickname)
        return (payload.get("account_id"), nickname,
                payload.get("lock_region"), payload.get("external_type"))
    except Exception:
        return None, None, None, None

# ====== RESOLVE JWT (শুধু বাইরের API) ======
def resolve_jwt(jwt_token=None, uid=None, password=None, access_token=None):
    # ১. সরাসরি JWT
    if jwt_token:
        uid_, name_, region_, _ = decode_jwt_info(jwt_token)
        return jwt_token, None, {
            "uid": str(uid_) if uid_ else None,
            "name": name_, "region": region_,
            "method": "Direct JWT"
        }

    # ২. UID + Pass → বাইরের API
    if uid and password:
        data, err = fetch_jwt_from_external(uid, password)
        if err:
            return None, f"External API failed: {err}", None

        jwt_ = data.get("jwt_token")
        ati = data.get("access_token_info") or {}
        return jwt_, None, {
            "uid": data.get("account_id") or str(uid),
            "name": data.get("nickname"),
            "region": data.get("region"),
            "method": "External API",
            "account_id": data.get("account_id"),
            "open_id": data.get("open_id") or ati.get("open_id"),
            "access_token": data.get("access_token"),
            "external_full": data
        }

    # ৩. Access Token
    if access_token:
        try:
            r = session.get(f"{INSPECT_URL}?token={access_token}",
                            timeout=10, verify=False)
            if r.status_code != 200:
                return None, f"Inspect HTTP {r.status_code}", None
            j = r.json()
            open_id = j.get("open_id")
            if not open_id:
                return None, "open_id not found", None
        except Exception as e:
            return None, f"Inspect error: {e}", None

        # access token দিয়ে বাইরের API-র মতো JWT নেই — ব্যবহারকারীকে সরাসরি access token API ব্যবহার করতে বলি
        return None, "Access token flow — use external API only", None

    return None, "No credentials provided (need jwt / uid+pass)", None


# ====== HTML PAGE ======
HTML_PAGE = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>🔥 MAHIR FREE FIRE BIO CHANGER</title>
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Rajdhani:wght@400;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{
  --primary:#ff4444;--primary-dark:#cc0000;--accent:#ffaa00;
  --bg:#0f0f23;--surface:rgba(18,18,40,0.97);
  --text:#f0f0ff;--muted:#8888aa;
  --success:#00e87a;--error:#ff4455;--warn:#ffbb00;
  --glass:rgba(255,255,255,0.06);--border:rgba(255,255,255,0.10);
  --glow-r:0 0 18px rgba(255,68,68,.55),0 0 40px rgba(255,68,68,.22);
  --radius:14px;--radius-sm:9px;
  --font-body:'Rajdhani',sans-serif;
  --font-display:'Orbitron',sans-serif;
}
body{background:var(--bg);color:var(--text);min-height:100vh;
  font-family:var(--font-body);font-size:15px;line-height:1.5;
  background-image:
    radial-gradient(ellipse 60% 40% at 15% 0%,rgba(255,68,68,.10) 0%,transparent 60%),
    radial-gradient(ellipse 50% 35% at 85% 100%,rgba(255,170,0,.09) 0%,transparent 60%);
}
.wrap{max-width:1220px;margin:0 auto;padding:16px}
.hdr{text-align:center;padding:32px 24px 26px;background:var(--surface);
  border-radius:20px;border:1.5px solid var(--border);box-shadow:var(--glow-r);
  margin-bottom:20px;position:relative;overflow:hidden;}
.brand{font-family:var(--font-display);font-size:clamp(18px,4.2vw,32px);font-weight:900;
  letter-spacing:2px;background:linear-gradient(90deg,#ff6666,#ffaa00,#ff4444);
  -webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent;
  text-transform:uppercase;}
.tagline{color:var(--muted);font-size:.92rem;margin-top:6px}
.socials{display:flex;justify-content:center;gap:10px;margin-top:18px;flex-wrap:wrap}
.socials a{display:inline-flex;align-items:center;gap:8px;padding:9px 18px;
  border-radius:40px;font-weight:700;font-size:.88rem;color:#fff;text-decoration:none;
  transition:transform .2s;}
.socials a:hover{transform:translateY(-2px);box-shadow:0 6px 20px rgba(0,0,0,.4)}
.s-tg{background:linear-gradient(135deg,#0088cc,#00aced)}
.s-yt{background:linear-gradient(135deg,#ff0000,#cc0000)}
.s-tt{background:linear-gradient(135deg,#111,#69c9d0)}
.main-tabs{display:grid;grid-template-columns:1fr 1fr;gap:6px;background:var(--glass);
  border:1.5px solid var(--border);border-radius:var(--radius);padding:6px;margin-bottom:18px;}
.m-tab{padding:13px 10px;text-align:center;border-radius:var(--radius-sm);cursor:pointer;
  font-weight:800;font-size:.95rem;transition:background .2s;
  display:flex;align-items:center;justify-content:center;gap:8px;color:var(--muted);}
.m-tab.on{background:linear-gradient(135deg,var(--primary),var(--primary-dark));
  color:#fff;box-shadow:0 4px 14px rgba(255,68,68,.35);}
.m-tab:not(.on):hover{background:rgba(255,255,255,.07);color:var(--text)}
.m-pane{display:none}.m-pane.on{display:block;animation:fadeUp .25s ease}
@keyframes fadeUp{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
.card{background:var(--surface);border:1.5px solid var(--border);
  border-radius:var(--radius);padding:24px;margin-bottom:18px;}
.card-title{font-family:var(--font-display);font-size:1.1rem;font-weight:700;
  display:flex;align-items:center;gap:10px;margin-bottom:18px;padding-bottom:12px;
  border-bottom:1.5px solid var(--border);}
.card-title i{color:var(--primary);font-size:1rem}
.fg{margin-bottom:16px}
.fl{display:block;font-size:.88rem;font-weight:700;color:var(--muted);
  margin-bottom:7px;letter-spacing:.3px;text-transform:uppercase;}
.fi,.fta{width:100%;padding:12px 14px;background:var(--glass);
  border:1.5px solid var(--border);border-radius:var(--radius-sm);color:var(--text);
  font-size:.95rem;font-family:var(--font-body);font-weight:600;outline:none;}
.fi:focus,.fta:focus{border-color:var(--primary);box-shadow:0 0 0 3px rgba(255,68,68,.18);}
.fta{min-height:100px;resize:vertical;font-family:'Courier New',monospace;line-height:1.7;}
.fctr{display:flex;gap:16px;justify-content:flex-end;font-size:.8rem;
  font-weight:700;margin-top:5px;color:var(--muted);}
.fctr .w{color:var(--warn)}.fctr .e{color:var(--error)}
.ibox{background:rgba(255,170,0,.09);border:1.5px solid rgba(255,170,0,.28);
  border-radius:var(--radius-sm);padding:14px 16px;margin-bottom:16px;
  font-size:.88rem;color:#ffcc55;line-height:1.7;}
.ibox code{background:rgba(0,0,0,.35);padding:1px 6px;border-radius:4px;
  color:#ffd966;font-family:monospace;font-size:.82rem;}
.fmt-row{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px}
.fmt-btn{padding:7px 14px;border-radius:var(--radius-sm);background:var(--glass);
  border:1.5px solid var(--border);color:var(--text);font-size:.85rem;font-weight:700;
  cursor:pointer;font-family:var(--font-body);}
.fmt-btn:hover{background:rgba(255,68,68,.15);border-color:var(--primary);}
.preview-wrap{background:rgba(0,0,0,.4);border:1.5px solid var(--border);
  border-radius:var(--radius-sm);padding:16px;min-height:80px;
  font-family:'Arial',sans-serif;font-size:1rem;line-height:1.9;word-break:break-word;}
.pv-line{margin-bottom:4px}
.auth-tabs{display:grid;grid-template-columns:repeat(4,1fr);gap:5px;
  background:var(--glass);border:1.5px solid var(--border);
  border-radius:var(--radius-sm);padding:5px;margin-bottom:18px;}
.a-tab{padding:10px 5px;text-align:center;cursor:pointer;border-radius:7px;
  font-weight:700;font-size:.8rem;color:var(--muted);
  display:flex;flex-direction:column;align-items:center;gap:4px;}
.a-tab.on{background:var(--primary);color:#fff}
.a-tab:not(.on):hover{background:rgba(255,255,255,.08);color:var(--text)}
.a-pane{display:none}.a-pane.on{display:block;animation:fadeUp .2s ease}
.tok-display{background:rgba(0,0,0,.3);border:1.5px solid var(--border);
  border-radius:var(--radius-sm);padding:12px 14px;font-family:monospace;
  font-size:.82rem;color:#00ffa3;word-break:break-all;max-height:80px;
  overflow-y:auto;margin-bottom:12px;}
.tok-display.empty{color:var(--muted);font-style:italic}
.btn{display:flex;align-items:center;justify-content:center;gap:10px;width:100%;
  padding:14px 20px;border:none;border-radius:var(--radius-sm);
  font-family:var(--font-display);font-size:.9rem;font-weight:700;letter-spacing:.8px;
  text-transform:uppercase;cursor:pointer;position:relative;overflow:hidden;
  text-decoration:none;}
.btn-primary{background:linear-gradient(135deg,var(--primary),var(--primary-dark));
  color:#fff;box-shadow:0 6px 20px rgba(255,68,68,.35);}
.btn-primary:hover:not(:disabled){transform:translateY(-2px);box-shadow:var(--glow-r);}
.btn-secondary{background:linear-gradient(135deg,rgba(255,170,0,.2),rgba(255,170,0,.1));
  color:var(--accent);border:1.5px solid rgba(255,170,0,.35);}
.btn-secondary:hover:not(:disabled){background:rgba(255,170,0,.25);transform:translateY(-2px);}
.btn:disabled{opacity:.45;cursor:not-allowed;transform:none!important}
.btn .btn-spin{width:16px;height:16px;border:2.5px solid rgba(255,255,255,.25);
  border-top-color:#fff;border-radius:50%;animation:spin .65s linear infinite;
  display:none;flex-shrink:0;}
.btn.loading .btn-spin{display:inline-block}
.btn.loading .btn-icon{display:none}
@keyframes spin{to{transform:rotate(360deg)}}
.resp-panel{margin-top:14px;border-radius:var(--radius-sm);
  border:1.5px solid transparent;overflow:hidden;font-size:.9rem;font-weight:600;
  display:none;}
.resp-panel.show{display:block;animation:fadeUp .25s ease}
.resp-panel.success{background:rgba(0,232,122,.09);border-color:rgba(0,232,122,.35);}
.resp-panel.error{background:rgba(255,68,85,.09);border-color:rgba(255,68,85,.35);}
.resp-header{display:flex;align-items:center;gap:8px;padding:10px 14px;
  border-bottom:1px solid rgba(255,255,255,.07);}
.resp-panel.success .resp-header{color:var(--success)}
.resp-panel.error .resp-header{color:var(--error)}
.resp-header .rh-title{font-weight:800;font-size:.92rem}
.resp-header .rh-ts{margin-left:auto;font-size:.75rem;color:var(--muted);
  font-family:monospace;}
.resp-body{padding:12px 14px;line-height:1.7}
.resp-row{display:flex;gap:10px;padding:4px 0;border-bottom:1px solid rgba(255,255,255,.04);}
.resp-row:last-child{border-bottom:none}
.resp-key{color:var(--muted);min-width:90px;font-size:.82rem}
.resp-val{color:var(--text);font-family:monospace;font-size:.85rem;word-break:break-all;}
.resp-panel.success .resp-val{color:#b8ffd8}
.resp-panel.error .resp-val{color:#ffc0c5}
.resp-raw{background:rgba(0,0,0,.25);border-radius:6px;padding:8px 10px;
  font-family:monospace;font-size:.78rem;color:var(--muted);
  margin-top:6px;max-height:120px;overflow-y:auto;word-break:break-all;}
.dropzone{border:2px dashed rgba(255,170,0,.35);border-radius:var(--radius);
  padding:28px 20px;text-align:center;cursor:pointer;}
.dropzone:hover,.dropzone.drag{border-color:var(--accent);background:rgba(255,170,0,.06);}
.dropzone .dz-icon{font-size:36px;margin-bottom:8px}
.dropzone p{font-size:.95rem;color:var(--text)}
.dropzone .dz-hint{font-size:.8rem;color:var(--muted);font-family:monospace;margin-top:4px}
.file-info{display:none;margin-top:12px;padding:11px 14px;border-radius:var(--radius-sm);
  background:rgba(0,232,122,.08);border:1.5px solid rgba(0,232,122,.25);
  color:#b8ffd8;font-size:.88rem;line-height:1.6;}
.file-info.show{display:block}
.progress-section{display:none;margin-top:18px}
.progress-section.show{display:block}
.prog-bar-wrap{height:8px;background:rgba(255,255,255,.08);border-radius:8px;
  overflow:hidden;margin:10px 0 14px;}
.prog-bar-fill{height:100%;width:0%;
  background:linear-gradient(90deg,var(--primary),var(--accent));
  border-radius:8px;transition:width .3s;}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}
.stat{padding:10px;border-radius:var(--radius-sm);background:rgba(0,0,0,.3);
  border:1.5px solid var(--border);text-align:center;}
.stat .sl{font-size:.68rem;text-transform:uppercase;letter-spacing:.8px;
  color:var(--muted);font-weight:700;display:block;margin-bottom:3px}
.stat .sv{font-size:1.1rem;font-weight:800;font-family:monospace}
.stat.s-ok .sv{color:var(--success)}
.stat.s-err .sv{color:var(--error)}
.bulk-results{display:none;margin-top:16px;max-height:440px;overflow-y:auto;
  border-radius:var(--radius-sm);border:1.5px solid var(--border);}
.bulk-results.show{display:block}
.bulk-results::-webkit-scrollbar{width:6px}
.bulk-results::-webkit-scrollbar-thumb{background:rgba(255,68,68,.35);border-radius:3px}
.br-row{display:grid;grid-template-columns:120px 1fr 1fr 220px;gap:8px;
  padding:10px 14px;border-bottom:1px solid rgba(255,255,255,.05);
  font-size:.82rem;align-items:center;}
.br-row:last-child{border-bottom:none}
.br-row.br-ok{border-left:3px solid var(--success)}
.br-row.br-err{border-left:3px solid var(--error)}
.br-row.br-proc{border-left:3px solid var(--accent)}
.br-uid{font-family:monospace;color:#c0c0e0;font-size:.8rem;font-weight:700}
.br-name{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.br-region{color:var(--muted);font-size:.8rem}
.br-status{text-align:right;font-weight:700;font-size:.75rem;word-break:break-all}
.br-row.br-ok .br-status{color:var(--success)}
.br-row.br-err .br-status{color:var(--error)}
.br-row.br-proc .br-status{color:var(--accent)}
.live-monitor{background:#05050a;border:1.5px solid rgba(0,232,122,.28);
  border-radius:var(--radius-sm);padding:12px 14px;margin-top:14px;
  font-family:'Courier New',monospace;font-size:.82rem;line-height:1.75;
  max-height:340px;overflow-y:auto;}
.live-monitor::-webkit-scrollbar{width:6px}
.live-monitor::-webkit-scrollbar-thumb{background:rgba(0,232,122,.35);border-radius:3px}
.lm-header{display:flex;align-items:center;gap:8px;padding-bottom:8px;
  margin-bottom:8px;border-bottom:1px solid rgba(0,232,122,.18);color:#00e87a;
  font-family:var(--font-display);font-size:.78rem;letter-spacing:1.2px;font-weight:700;
  position:sticky;top:-12px;background:#05050a;padding-top:2px;z-index:3;}
.lm-dot{width:8px;height:8px;border-radius:50%;background:#00e87a;
  box-shadow:0 0 10px #00e87a;animation:pulseDot 1.2s infinite;}
@keyframes pulseDot{0%,100%{opacity:1}50%{opacity:.3}}
.lm-clear{margin-left:auto;cursor:pointer;padding:2px 8px;border-radius:5px;
  font-size:.7rem;color:var(--muted);border:1px solid rgba(255,255,255,.12);
  background:transparent;font-family:var(--font-body);font-weight:700;}
.lm-clear:hover{color:#ff4455;border-color:#ff4455}
.lm-line{margin-bottom:1px;word-break:break-all;white-space:pre-wrap}
.lm-ts{color:#3a3a5a;margin-right:8px}
.lm-uid{color:#7ec8ff;font-weight:700}
.lm-ok{color:#00e87a;font-weight:700}
.lm-err{color:#ff4455;font-weight:700}
.lm-info{color:#ffbb00}
.lm-name{color:#ffd966;font-weight:700}
.lm-region{color:#b8b8ff}
.lm-time{color:#666;font-style:italic}
.api-box{background:rgba(0,232,122,.07);border:1.5px solid rgba(0,232,122,.22);
  border-radius:var(--radius-sm);padding:14px 16px;font-size:.87rem;line-height:1.9;}
.api-box code{background:rgba(0,0,0,.4);padding:2px 7px;border-radius:4px;
  color:#00ffa3;font-family:monospace;font-size:.82rem;word-break:break-all;
  cursor:pointer;}
.api-box code:hover{background:rgba(0,255,163,.12)}
.api-label{font-weight:800;color:var(--text);display:block;margin-top:10px;margin-bottom:2px}
.api-label:first-child{margin-top:0}
.footer{text-align:center;margin-top:10px;padding:16px;color:var(--muted);
  font-size:.88rem;border-top:1px solid var(--border);}
.footer a{color:var(--accent);text-decoration:none;font-weight:800}
.dl-row{display:flex;gap:10px;flex-wrap:wrap;margin-top:16px}
.dl-row .btn{flex:1;min-width:180px}
@media(max-width:580px){
  .auth-tabs{grid-template-columns:repeat(2,1fr)}
  .stats{grid-template-columns:repeat(2,1fr)}
  .br-row{grid-template-columns:90px 1fr 130px;gap:5px}
  .br-region{display:none}
}
</style>
</head>
<body>
<div class="wrap">

<div class="hdr">
  <div class="brand">🔥 MAHIR FREE FIRE BIO CHANGER</div>
  <div class="tagline">Single · Bulk · JWT · UID+Pass · External API</div>
  <div class="socials">
    <a class="s-tg" href="https://t.me/MAHIR0208" target="_blank"><i class="fab fa-telegram"></i>Telegram</a>
    <a class="s-yt" href="https://youtube.com/@MAHIR0208" target="_blank"><i class="fab fa-youtube"></i>YouTube</a>
    <a class="s-tt" href="https://tiktok.com/@MAHIR0208" target="_blank"><i class="fab fa-tiktok"></i>TikTok</a>
  </div>
</div>

<div class="main-tabs">
  <div class="m-tab on" id="mt-single" onclick="switchMain('single')">
    <i class="fas fa-user"></i> SINGLE UPDATE
  </div>
  <div class="m-tab" id="mt-bulk" onclick="switchMain('bulk')">
    <i class="fas fa-layer-group"></i> BULK (JSON)
  </div>
</div>

<div class="m-pane on" id="mp-single">
  <div class="card">
    <div class="card-title"><i class="fas fa-pen-fancy"></i> Bio Editor</div>
    <div class="ibox">
      <strong>Game tags:</strong>
      <code>[b]</code> Bold · <code>[i]</code> Italic · <code>[FF0000]</code> Color
      — Max <strong>3 lines · 250 chars</strong>
    </div>
    <div class="fmt-row">
      <button class="fmt-btn" onclick="ins('[b]')"><b>B</b> Bold</button>
      <button class="fmt-btn" onclick="ins('[i]')"><i>I</i> Italic</button>
      <button class="fmt-btn" onclick="ins('[b][i]')"><b><i>BI</i></b></button>
      <button class="fmt-btn" onclick="ins('[c]')">⬛ Center</button>
      <button class="fmt-btn" onclick="ins('[FFFFFF]')">⬜ White</button>
      <button class="fmt-btn" onclick="clearBio()">✕ Clear</button>
    </div>
    <div class="fg">
      <label class="fl" for="bio-ta">Bio Text</label>
      <textarea class="fta" id="bio-ta" rows="4"
        oninput="onBioInput()">[c][b][00BFFF]HEY DER WELCOME TO [FFFFFF]MAHIR [00BFFF]WEB</textarea>
      <div class="fctr">
        <span id="lc-ln" class=""></span>
        <span id="lc-ch" class=""></span>
      </div>
    </div>
    <div class="fg">
      <label class="fl">Live Preview</label>
      <div class="preview-wrap" id="bio-preview"></div>
    </div>
  </div>

  <div class="card">
    <div class="card-title"><i class="fas fa-key"></i> Authentication</div>
    <div class="auth-tabs">
      <div class="a-tab on" id="at-cred" onclick="switchAuth('cred')"><i class="fas fa-user-lock"></i>UID+Pass</div>
      <div class="a-tab" id="at-jwt" onclick="switchAuth('jwt')"><i class="fas fa-key"></i>JWT</div>
      <div class="a-tab" id="at-access" onclick="switchAuth('access')"><i class="fas fa-unlock-alt"></i>Access</div>
      <div class="a-tab" id="at-eat" onclick="switchAuth('eat')"><i class="fas fa-exchange-alt"></i>EAT</div>
    </div>
    <div class="a-pane on" id="ap-cred">
      <div class="fg">
        <label class="fl" for="inp-uid">Guest UID</label>
        <input class="fi" id="inp-uid" type="text" placeholder="Enter UID">
      </div>
      <div class="fg">
        <label class="fl" for="inp-pass">Guest Password</label>
        <input class="fi" id="inp-pass" type="text" placeholder="Enter Password">
      </div>
      <button class="btn btn-primary" id="btn-cred" onclick="singleUpdate('cred')">
        <span class="btn-spin"></span>
        <i class="fas fa-rocket btn-icon"></i>
        <span class="btn-label">UPDATE BIO WITH UID/PASS</span>
      </button>
      <div class="resp-panel" id="resp-cred"></div>
    </div>
    <div class="a-pane" id="ap-jwt">
      <div class="fg">
        <label class="fl" for="inp-jwt">JWT Token</label>
        <input class="fi" id="inp-jwt" type="text" placeholder="eyJhbGciOi...">
      </div>
      <button class="btn btn-primary" id="btn-jwt" onclick="singleUpdate('jwt')">
        <span class="btn-spin"></span>
        <i class="fas fa-rocket btn-icon"></i>
        <span class="btn-label">UPDATE BIO WITH JWT</span>
      </button>
      <div class="resp-panel" id="resp-jwt"></div>
    </div>
    <div class="a-pane" id="ap-access">
      <div class="fg">
        <label class="fl" for="inp-access">Access Token</label>
        <input class="fi" id="inp-access" type="text" placeholder="Enter access token">
      </div>
      <button class="btn btn-primary" id="btn-access" onclick="singleUpdate('access')">
        <span class="btn-spin"></span>
        <i class="fas fa-rocket btn-icon"></i>
        <span class="btn-label">UPDATE BIO WITH ACCESS TOKEN</span>
      </button>
      <div class="resp-panel" id="resp-access"></div>
    </div>
    <div class="a-pane" id="ap-eat">
      <div class="ibox" style="background:rgba(255,170,0,.08)">
        EAT Token = external access token.
      </div>
      <div class="fg">
        <label class="fl" for="inp-eat">EAT Token</label>
        <input class="fi" id="inp-eat" type="text" placeholder="Enter EAT token">
      </div>
      <button class="btn btn-secondary" id="btn-eat-conv" style="margin-bottom:10px" onclick="convertEAT()">
        <span class="btn-spin"></span>
        <i class="fas fa-sync-alt btn-icon"></i>
        <span class="btn-label">CONVERT EAT → ACCESS TOKEN</span>
      </button>
      <div class="fg">
        <label class="fl">Extracted Access Token</label>
        <div class="tok-display empty" id="eat-extracted">Token will appear here...</div>
      </div>
      <button class="btn btn-primary" id="btn-eat-upd" onclick="singleUpdate('eat')" disabled>
        <span class="btn-spin"></span>
        <i class="fas fa-rocket btn-icon"></i>
        <span class="btn-label">UPDATE BIO WITH EXTRACTED TOKEN</span>
      </button>
      <div class="resp-panel" id="resp-eat"></div>
    </div>
  </div>

  <div class="card">
    <div class="card-title"><i class="fas fa-plug"></i> GET API — Copy &amp; Use Anywhere</div>
    <div class="api-box">
      <span class="api-label">UID &amp; Password</span>
      <code onclick="copyCode(this)">GET /api/bio?uid=UID&pass=PASS&bio=YOUR_BIO</code>
      <span class="api-label">JWT</span>
      <code onclick="copyCode(this)">GET /api/bio?jwt=YOUR_JWT&bio=YOUR_BIO</code>
      <span class="api-label">Health Check</span>
      <code onclick="copyCode(this)">GET /health</code>
    </div>
  </div>
</div><!-- /mp-single -->

<div class="m-pane" id="mp-bulk">
  <div class="card">
    <div class="card-title"><i class="fas fa-file-code"></i> Upload Accounts JSON</div>
    <div class="ibox">Format: <code>[{"uid":"123","password":"abc"}, ...]</code></div>
    <div class="dropzone" id="dz" onclick="document.getElementById('file-in').click()">
      <div class="dz-icon">📁</div>
      <p><strong>Click</strong> or <strong>drag &amp; drop</strong> JSON file here</p>
      <p class="dz-hint">[{"uid":"...","password":"..."}, ...]</p>
      <input type="file" id="file-in" accept=".json,application/json" hidden>
    </div>
    <div class="file-info" id="fi-info"></div>
  </div>

  <div class="card">
    <div class="card-title"><i class="fas fa-pen-fancy"></i> Bio Text (applied to all accounts)</div>
    <div class="fmt-row">
      <button class="fmt-btn" onclick="insBulk('[b]')"><b>B</b> Bold</button>
      <button class="fmt-btn" onclick="insBulk('[i]')"><i>I</i> Italic</button>
      <button class="fmt-btn" onclick="insBulk('[b][i]')"><b><i>BI</i></b></button>
      <button class="fmt-btn" onclick="insBulk('[c]')">⬛ Center</button>
      <button class="fmt-btn" onclick="insBulk('[FFFFFF]')">⬜ White</button>
      <button class="fmt-btn" onclick="clearBulkBio()">✕ Clear</button>
    </div>
    <div class="fg">
      <textarea class="fta" id="bulk-bio" rows="4"
        >[c][b][00BFFF]POWER OF [FFFFFF]MAHIR [1E90FF]WEB : MAHIR.XO.JE [FFFFFF]</textarea>
    </div>
    <button class="btn btn-primary" id="btn-bulk" onclick="startBulk()" disabled>
      <span class="btn-spin"></span>
      <i class="fas fa-rocket btn-icon"></i>
      <span class="btn-label">START BULK UPLOAD (ULTRA FAST ⚡)</span>
    </button>

    <div class="progress-section" id="prog-sec">
      <div style="font-size:.78rem;color:var(--accent);letter-spacing:1.5px;font-weight:800;margin-bottom:6px">⚡ PROCESSING (PARALLEL x20)</div>
      <div class="prog-bar-wrap"><div class="prog-bar-fill" id="prog-fill"></div></div>
      <div class="stats">
        <div class="stat"><span class="sl">Total</span><span class="sv" id="st-total">0</span></div>
        <div class="stat"><span class="sl">Done</span><span class="sv" id="st-done">0</span></div>
        <div class="stat s-ok"><span class="sl">✅ OK</span><span class="sv" id="st-ok">0</span></div>
        <div class="stat s-err"><span class="sl">❌ Fail</span><span class="sv" id="st-err">0</span></div>
      </div>
    </div>

    <div class="live-monitor" id="live-monitor">
      <div class="lm-header">
        <span class="lm-dot"></span>
        <span>LIVE MONITOR · t.me/MAHIR0208</span>
        <button class="lm-clear" onclick="lmClear()">CLEAR</button>
      </div>
      <div id="lm-log">
        <div class="lm-line lm-info">▶ Waiting for bulk start...</div>
      </div>
    </div>

    <div class="bulk-results" id="bulk-results"></div>

    <div class="dl-row">
      <a class="btn btn-secondary" href="/download/bd" download>
        <i class="fas fa-download"></i>
        <span class="btn-label">DOWNLOAD bd.json</span>
      </a>
      <a class="btn btn-secondary" href="/download/ind" download>
        <i class="fas fa-download"></i>
        <span class="btn-label">DOWNLOAD ind.json</span>
      </a>
    </div>
  </div>
</div><!-- /mp-bulk -->

<div class="footer">
  🔥 Developed by <a href="https://t.me/MAHIR0208" target="_blank">t.me/MAHIR0208</a>
</div>

</div><!-- /wrap -->

<script>
let eatAccessToken = '';
let bulkAccounts = [];
let bulkRunning = false;

function switchMain(name) {
  ['single','bulk'].forEach(n => {
    document.getElementById('mt-'+n).classList.toggle('on', n===name);
    document.getElementById('mp-'+n).classList.toggle('on', n===name);
  });
}
function switchAuth(name) {
  ['jwt','cred','access','eat'].forEach(n => {
    document.getElementById('at-'+n).classList.toggle('on', n===name);
    document.getElementById('ap-'+n).classList.toggle('on', n===name);
  });
}
function ins(tag) {
  const ta = document.getElementById('bio-ta');
  const s = ta.selectionStart, e = ta.selectionEnd;
  ta.value = ta.value.slice(0,s) + tag + ta.value.slice(e);
  ta.selectionStart = ta.selectionEnd = s + tag.length;
  ta.focus(); onBioInput();
}
function clearBio() {
  document.getElementById('bio-ta').value = ''; onBioInput();
}
function getBio() { return document.getElementById('bio-ta').value.trim(); }
function insBulk(tag) {
  const ta = document.getElementById('bulk-bio');
  const s = ta.selectionStart, e = ta.selectionEnd;
  ta.value = ta.value.slice(0,s) + tag + ta.value.slice(e);
  ta.selectionStart = ta.selectionEnd = s + tag.length;
  ta.focus();
}
function clearBulkBio() { document.getElementById('bulk-bio').value = ''; }
function lmLog(html) {
  const log = document.getElementById('lm-log');
  const ts = new Date().toLocaleTimeString();
  const line = document.createElement('div');
  line.className = 'lm-line';
  line.innerHTML = `<span class="lm-ts">[${ts}]</span>` + html;
  log.appendChild(line);
  const monitor = document.getElementById('live-monitor');
  monitor.scrollTop = monitor.scrollHeight;
  while (log.children.length > 500) log.removeChild(log.firstChild);
}
function lmClear() {
  document.getElementById('lm-log').innerHTML =
    '<div class="lm-line lm-info">▶ Monitor cleared. Ready.</div>';
}
function onBioInput() { updateCounters(); renderPreview(); }
function updateCounters() {
  const v = document.getElementById('bio-ta').value;
  const lines = v.split('\n').length, chars = v.length;
  const lc = document.getElementById('lc-ln'), cc = document.getElementById('lc-ch');
  lc.textContent = lines+'/3 lines';
  lc.className = lines>3?'e':lines>2?'w':'';
  cc.textContent = chars+'/250';
  cc.className = chars>250?'e':chars>230?'w':'';
}
function renderPreview() {
  const raw = document.getElementById('bio-ta').value;
  const wrap = document.getElementById('bio-preview');
  if (!raw.trim()) {
    wrap.innerHTML = '<span style="color:#555">Preview will appear here...</span>';
    return;
  }
  const lines = raw.split('\n');
  let html = '', bold=false, italic=false, color='#FFFFFF';
  lines.forEach(line => {
    let lineHtml = '', idx = 0;
    const re = /\[([biBIcC]|[0-9A-Fa-f]{6})\]/g;
    let m;
    while ((m = re.exec(line)) !== null) {
      if (m.index > idx) lineHtml += segment(line.slice(idx,m.index), bold, italic, color);
      const t = m[1].toUpperCase();
      if (t==='B') bold=true;
      else if (t==='I') italic=true;
      else if (t==='C') { }
      else color='#'+t;
      idx = m.index + m[0].length;
    }
    if (idx < line.length) lineHtml += segment(line.slice(idx), bold, italic, color);
    html += '<div class="pv-line">'+(lineHtml||'&#8203;')+'</div>';
  });
  wrap.innerHTML = html;
}
function segment(txt, bold, italic, color) {
  if (!txt) return '';
  let st = `color:${color};`;
  if (bold) st+='font-weight:bold;';
  if (italic) st+='font-style:italic;';
  return `<span style="${st}">${txt.replace(/</g,'&lt;').replace(/>/g,'&gt;')}</span>`;
}
function showResp(panelId, ok, data, raw) {
  const p = document.getElementById(panelId);
  p.className = 'resp-panel show ' + (ok?'success':'error');
  const ts = new Date().toLocaleTimeString();
  let rows = '';
  if (ok) {
    const pairs = [
      ['Status', data.status||'Updated'],
      ['Name', data.name||'—'],
      ['UID', data.uid||'—'],
      ['Region', data.region||'—'],
      ['Method', data.login_method||data.method||'—'],
    ];
    pairs.forEach(([k,v])=>{
      rows += `<div class="resp-row"><span class="resp-key">${k}</span><span class="resp-val">${esc(String(v))}</span></div>`;
    });
    if (data.server_ascii || data.server_response) {
      rows += `<div class="resp-raw">${esc((data.server_ascii||data.server_response).slice(0,400))}</div>`;
    }
  } else {
    rows = `<div class="resp-row"><span class="resp-key">Error</span><span class="resp-val">${esc(data.error||data.status||data.message||'Unknown error')}</span></div>`;
    if (data.server_ascii) rows += `<div class="resp-raw">${esc(data.server_ascii.slice(0,400))}</div>`;
    if (data.server_response) rows += `<div class="resp-raw">hex: ${esc(data.server_response.slice(0,200))}</div>`;
  }
  p.innerHTML = `
    <div class="resp-header">
      <span class="rh-icon">${ok?'✅':'❌'}</span>
      <span class="rh-title">${ok?'Bio Updated Successfully':'Update Failed'}</span>
      <span class="rh-ts">${ts}</span>
    </div>
    <div class="resp-body">${rows}
      <div style="margin-top:8px;font-size:.78rem;color:var(--muted)">🔥 t.me/MAHIR0208</div>
    </div>`;
}
function hideAllResp() {
  ['resp-jwt','resp-cred','resp-access','resp-eat'].forEach(id => {
    const el = document.getElementById(id);
    el.className = 'resp-panel'; el.innerHTML = '';
  });
}
function btnLoad(id, on) {
  const b = document.getElementById(id);
  b.classList.toggle('loading', on);
  b.disabled = on;
}
function validateBio() {
  const bio = getBio();
  if (!bio) return [null, 'Please enter bio text'];
  if (bio.length>250) return [null, 'Bio exceeds 250 characters'];
  if (bio.split('\n').filter(l=>l.trim()).length>3) return [null, 'Bio exceeds 3 lines'];
  return [bio, null];
}
async function singleUpdate(method) {
  const [bio, err] = validateBio();
  if (err) { alert(err); return; }
  hideAllResp();
  const fd = new FormData();
  fd.append('bio', bio);
  let url, btnId, respId;
  if (method==='jwt') {
    const t = document.getElementById('inp-jwt').value.trim();
    if (!t) { alert('Enter JWT token'); return; }
    fd.append('jwt', t);
    url='/direct_update'; btnId='btn-jwt'; respId='resp-jwt';
  } else if (method==='cred') {
    const u=document.getElementById('inp-uid').value.trim();
    const p=document.getElementById('inp-pass').value.trim();
    if (!u||!p) { alert('Enter UID and Password'); return; }
    fd.append('uid',u); fd.append('pass',p);
    url='/bio_upload'; btnId='btn-cred'; respId='resp-cred';
  } else if (method==='access') {
    const a=document.getElementById('inp-access').value.trim();
    if (!a) { alert('Enter access token'); return; }
    fd.append('access_token',a);
    url='/update_bio'; btnId='btn-access'; respId='resp-access';
  } else if (method==='eat') {
    if (!eatAccessToken) { alert('Convert EAT token first'); return; }
    fd.append('access_token', eatAccessToken);
    url='/update_bio'; btnId='btn-eat-upd'; respId='resp-eat';
  }
  btnLoad(btnId, true);
  try {
    const res = await fetch(url, {method:'POST', body:fd});
    const data = await res.json();
    const ok = data.status && String(data.status).toLowerCase().includes('success');
    showResp(respId, ok, data, JSON.stringify(data));
    document.getElementById(respId).scrollIntoView({behavior:'smooth',block:'nearest'});
  } catch(e) {
    showResp(respId, false, {error:'Network error: '+e.message}, '');
  } finally {
    btnLoad(btnId, false);
  }
}
async function convertEAT() {
  alert('EAT conversion is deprecated. Use UID+Pass.');
}
function copyCode(el) {
  navigator.clipboard.writeText(el.textContent).then(()=>{
    const orig=el.textContent;
    el.textContent='Copied!';
    setTimeout(()=>el.textContent=orig, 1200);
  });
}
document.getElementById('file-in').addEventListener('change', e=>{
  if (e.target.files[0]) handleFile(e.target.files[0]);
});
const dz = document.getElementById('dz');
dz.addEventListener('dragover', e=>{e.preventDefault();dz.classList.add('drag')});
dz.addEventListener('dragleave', ()=>dz.classList.remove('drag'));
dz.addEventListener('drop', e=>{
  e.preventDefault(); dz.classList.remove('drag');
  if (e.dataTransfer.files[0]) handleFile(e.dataTransfer.files[0]);
});
function handleFile(file) {
  if (!file.name.toLowerCase().endsWith('.json')) { alert('Upload a .json file'); return; }
  const r=new FileReader();
  r.onload=e=>{
    try {
      const arr=JSON.parse(e.target.result);
      if (!Array.isArray(arr)) throw new Error('Root must be array');
      bulkAccounts=arr.filter(a=>a&&(a.uid||a.account_id)&&(a.password||a.pass))
        .map(a=>({uid:a.uid||a.account_id, password:a.password||a.pass, name:a.name||a.nickname||''}));
      if (!bulkAccounts.length){alert('No valid accounts found');return;}
      const fi=document.getElementById('fi-info');
      fi.innerHTML=`<strong>✅ ${file.name}</strong> — <strong>${bulkAccounts.length}</strong> accounts loaded`;
      fi.classList.add('show');
      document.getElementById('btn-bulk').disabled=false;
      lmLog(`<span class="lm-info">📁 File loaded:</span> <span class="lm-name">${esc(file.name)}</span> · <span class="lm-uid">${bulkAccounts.length}</span> accounts ready`);
    } catch(err){ alert('Invalid JSON: '+err.message); }
  };
  r.readAsText(file);
}
async function startBulk() {
  if (bulkRunning || !bulkAccounts.length) return;
  const bio = document.getElementById('bulk-bio').value.trim();
  if (!bio) { alert('Enter bio text'); return; }
  bulkRunning = true;
  btnLoad('btn-bulk', true);
  const total = bulkAccounts.length;
  let ok = 0, fail = 0, done = 0;
  const startTs = performance.now();
  lmClear();
  lmLog(`<span class="lm-info">▶ BULK STARTED</span> · <span class="lm-name">${total} accounts</span>`);
  document.getElementById('prog-sec').classList.add('show');
  document.getElementById('st-total').textContent = total;
  document.getElementById('st-done').textContent = 0;
  document.getElementById('st-ok').textContent = 0;
  document.getElementById('st-err').textContent = 0;
  document.getElementById('prog-fill').style.width = '0%';
  const results = document.getElementById('bulk-results');
  results.innerHTML = '';
  results.classList.add('show');
  const rowIds = bulkAccounts.map((acc, i) => {
    const rowId = 'br-' + i + '-' + Math.random().toString(36).slice(2, 6);
    addBR(results, rowId, acc.uid, acc.name || '—', '—', '⏳ Queued...', 'br-proc');
    return rowId;
  });
  const CONCURRENCY = Math.min(20, total);
  let cursor = 0;
  async function processOne(i) {
    const acc = bulkAccounts[i];
    const rowId = rowIds[i];
    updBR(results, rowId, acc.uid, acc.name || '—', '—', '⏳ Processing...', 'br-proc');
    const t0 = performance.now();
    try {
      const fd = new FormData();
      fd.append('uid', acc.uid);
      fd.append('pass', acc.password);
      fd.append('bio', bio);
      const res = await fetch('/bio_upload', { method: 'POST', body: fd });
      const data = await res.json();
      const dt = ((performance.now() - t0) / 1000).toFixed(2);
      const isOk = data.status && String(data.status).toLowerCase().includes('success');
      if (isOk) {
        ok++;
        updBR(results, rowId, acc.uid,
              data.name || acc.name || '—',
              data.region || '—',
              '✅ Updated', 'br-ok');
        lmLog(`<span class="lm-uid">${esc(acc.uid)}</span> <span class="lm-ok">✅ OK</span> · <span class="lm-name">${esc(data.name || '—')}</span> · <span class="lm-region">${esc(data.region || '—')}</span> <span class="lm-time">(${dt}s)</span>`);
      } else {
        fail++;
        const errMsg = data.error || data.status || 'Failed';
        updBR(results, rowId, acc.uid,
              data.name || acc.name || '—', '—',
              '❌ ' + errMsg.slice(0,80), 'br-err');
        lmLog(`<span class="lm-uid">${esc(acc.uid)}</span> <span class="lm-err">❌ ${esc(errMsg.slice(0,150))}</span> <span class="lm-time">(${dt}s)</span>`);
      }
    } catch (e) {
      fail++;
      updBR(results, rowId, acc.uid, acc.name || '—', '—',
            '❌ ' + e.message.slice(0,80), 'br-err');
      lmLog(`<span class="lm-uid">${esc(acc.uid)}</span> <span class="lm-err">❌ Network: ${esc(e.message)}</span>`);
    } finally {
      done++;
      document.getElementById('st-done').textContent = done;
      document.getElementById('st-ok').textContent = ok;
      document.getElementById('st-err').textContent = fail;
      document.getElementById('prog-fill').style.width = (done / total * 100) + '%';
    }
  }
  async function worker() {
    while (true) {
      const i = cursor++;
      if (i >= total) return;
      await processOne(i);
    }
  }
  await Promise.all(Array.from({ length: CONCURRENCY }, worker));
  const totalTime = ((performance.now() - startTs) / 1000).toFixed(2);
  lmLog(`<span class="lm-info">■ BULK COMPLETE</span> · <span class="lm-ok">✅ ${ok} OK</span> · <span class="lm-err">❌ ${fail} FAIL</span> · <span class="lm-time">total ${totalTime}s</span>`);
  bulkRunning = false;
  btnLoad('btn-bulk', false);
}
function addBR(wrap, id, uid, name, region, status, cls) {
  const d=document.createElement('div');
  d.className='br-row '+cls; d.id=id;
  d.innerHTML=`<span class="br-uid">${esc(uid)}</span>
    <span class="br-name">${esc(name||'—')}</span>
    <span class="br-region">${esc(region)}</span>
    <span class="br-status">${esc(status)}</span>`;
  wrap.appendChild(d);
}
function updBR(wrap, id, uid, name, region, status, cls) {
  const d=document.getElementById(id); if(!d) return;
  d.className='br-row '+cls;
  d.innerHTML=`<span class="br-uid">${esc(uid)}</span>
    <span class="br-name">${esc(name||'—')}</span>
    <span class="br-region">${esc(region)}</span>
    <span class="br-status">${esc(status)}</span>`;
}
function esc(s){
  return String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
document.addEventListener('DOMContentLoaded',()=>{ onBioInput(); });
</script>
</body>
</html>'''


# ====== ROUTES ======
@app.route("/", methods=["GET"])
def home():
    return make_response(HTML_PAGE, 200, {'Content-Type':'text/html; charset=utf-8'})

@app.route("/health", methods=["GET"])
def health():
    return _jsonify({"status":"ok","service":"MAHIR Free Fire Bio Changer","version":"3.0"})

@app.route("/direct_update", methods=["POST"])
def direct_update():
    jwt_token = request.form.get("jwt") or request.args.get("jwt")
    bio = request.form.get("bio") or request.args.get("bio")
    if not jwt_token: return _jsonify({"status":"Missing JWT token","error":"Missing JWT token"})
    if not bio:       return _jsonify({"status":"Missing bio","error":"Missing bio"})
    result = upload_bio_request(jwt_token, bio)
    uid, name, region, _ = decode_jwt_info(jwt_token)
    if result["status"]=="success":
        return _jsonify({"status":"Bio updated successfully!",
            "uid": str(uid) if uid else None, "name": name, "region": region,
            "server_response": result.get("server_response","N/A"),
            "server_ascii": result.get("server_ascii","")})
    return _jsonify({"status":"Bio update failed",
                     "error": result.get("server_ascii") or result.get("server_response","Unknown error"),
                     "server_ascii": result.get("server_ascii",""),
                     "server_response": result.get("server_response","")})

@app.route("/bio_upload", methods=["POST"])
def bio_upload():
    uid = request.form.get("uid") or request.args.get("uid")
    password = request.form.get("pass") or request.args.get("pass")
    bio = request.form.get("bio") or request.args.get("bio")
    if not (uid and password and bio):
        return _jsonify({"status":"Missing uid/pass/bio","error":"Missing uid/pass/bio"})

    jwt_token, err, meta = resolve_jwt(uid=uid, password=password)
    if err:
        return _jsonify({"status":err,"error":err})

    result = upload_bio_request(jwt_token, bio)
    region = meta.get("region")
    name = meta.get("name")
    account_id = meta.get("account_id")

    if result["status"]=="success":
        try:
            append_account_to_region_json(
                uid=uid, password=password, bio=bio,
                jwt_token=jwt_token, region=region,
                name=name, account_id=account_id,
                access_token=meta.get("access_token"),
                open_id=meta.get("open_id")
            )
        except Exception as e:
            print(f"[JSON SAVE ERROR] {e}")
        return _jsonify({"status":"Bio updated successfully!",
            "uid": meta.get("uid"), "name": name,
            "region": region,
            "server_response": result.get("server_response","N/A"),
            "server_ascii": result.get("server_ascii","")})

    return _jsonify({
        "status":"Bio update failed",
        "error": result.get("server_ascii") or result.get("server_response","Unknown error"),
        "server_ascii": result.get("server_ascii",""),
        "server_response": result.get("server_response","")
    })

@app.route("/update_bio", methods=["POST"])
def update_bio():
    access_token = request.form.get("access_token") or request.args.get("access_token")
    bio = request.form.get("bio") or request.args.get("bio")
    if not access_token: return _jsonify({"status":"Missing access token","error":"Missing access token"})
    if not bio:          return _jsonify({"status":"Missing bio","error":"Missing bio"})
    jwt_token, err, meta = resolve_jwt(access_token=access_token)
    if err: return _jsonify({"status":err,"error":err})
    result = upload_bio_request(jwt_token, bio)
    if result["status"]=="success":
        return _jsonify({"status":"Bio updated successfully!",
            "uid": meta.get("uid"), "name": meta.get("name"),
            "region": meta.get("region"),
            "server_response": result.get("server_response","N/A"),
            "server_ascii": result.get("server_ascii","")})
    return _jsonify({"status":"Bio update failed",
                     "error": result.get("server_ascii") or result.get("server_response","Unknown error"),
                     "server_ascii": result.get("server_ascii",""),
                     "server_response": result.get("server_response","")})

@app.route("/extract_token", methods=["POST"])
def extract_token():
    return _jsonify({"success": False, "error": "Deprecated — use UID+Pass"})

@app.route("/bio", methods=["GET", "POST"])
@app.route("/api/bio", methods=["GET", "POST"])
def combined_bio():
    r = request.args if request.method=="GET" else request.form
    bio = r.get("bio")
    jwt_token = r.get("jwt")
    uid = r.get("uid") or r.get("account_id")
    password = r.get("pass") or r.get("password")
    if not bio:
        return _jsonify({"status":"failed","message":"Missing 'bio' parameter"}, 400)
    jwt_final, err, meta = resolve_jwt(
        jwt_token=jwt_token, uid=uid, password=password)
    if err:
        return _jsonify({"status":"failed","message":err}, 400)
    result = upload_bio_request(jwt_final, bio)
    return _jsonify({
        "status": result["status"], "code": result["code"], "bio": bio,
        "uid": meta.get("uid"), "name": meta.get("name"),
        "region": meta.get("region"), "login_method": meta.get("method"),
        "open_id": meta.get("open_id"),
        "server_response": result.get("server_response","N/A"),
        "server_ascii": result.get("server_ascii","")
    })

@app.route("/download/bd", methods=["GET"])
def download_bd():
    if not os.path.exists(BD_JSON):
        save_json_list(BD_JSON, [])
    return send_file(BD_JSON, mimetype="application/json",
                     as_attachment=True, download_name="bd.json")

@app.route("/download/ind", methods=["GET"])
def download_ind():
    if not os.path.exists(IND_JSON):
        save_json_list(IND_JSON, [])
    return send_file(IND_JSON, mimetype="application/json",
                     as_attachment=True, download_name="ind.json")

@app.route("/view/<region>", methods=["GET"])
def view_region(region):
    region = region.lower()
    if region == "bd":
        path = BD_JSON
    elif region in ("ind", "india"):
        path = IND_JSON
    else:
        return _jsonify({"status":"failed","message":"region must be bd or ind"}, 400)
    return _jsonify({"status":"ok", "region": region, "accounts": load_json_list(path)})

# ====== Vercel Handler ======
handler = app

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)