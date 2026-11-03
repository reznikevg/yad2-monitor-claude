#!/usr/bin/env python3
"""
yad2_monitor.py — 100% API-Based Yad2 Monitor & Interactive Telegram Bot
========================================================================
- Pure API scraping via gw.yad2.co.il with curl_cffi TLS impersonation
  (rotates chrome131 / chrome124 / safari17_0 / edge101 + cookie warm-up).
- Interactive Telegram Bot control via inline buttons & commands:
  /menu, /scan, /status, /summary, /searches, /add, /pause, /resume
- Dynamic configuration stored in state/config.json (persisted to Git).
"""

import argparse
import hashlib
import html
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from curl_cffi import requests as cffi_requests  # type: ignore
    HAS_CURL_CFFI = True
except ImportError:
    cffi_requests = None
    HAS_CURL_CFFI = False

# ─────────────────────────────────────────────
# CONFIG & PATHS
# ─────────────────────────────────────────────

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
_chat_id_raw = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
if not TELEGRAM_TOKEN or not _chat_id_raw:
    sys.stderr.write(
        "FATAL: TELEGRAM_TOKEN and TELEGRAM_CHAT_ID env vars are required.\n"
        "Set them in your shell or as GitHub repository secrets.\n"
    )
    sys.exit(2)

TELEGRAM_CHAT_ID = int(_chat_id_raw)
TELEGRAM_BASE = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

_default_state = str(Path(__file__).resolve().parent / "state")
STATE_DIR = Path(os.environ.get("STATE_DIR", _default_state))
CONFIG_FILE = STATE_DIR / "config.json"
FAILED_ALERTS_FILE = STATE_DIR / "failed_alerts.txt"
LOG_FILE = STATE_DIR / "monitor.log"

IST = timezone(timedelta(hours=3))

STATE_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

DEFAULT_CONFIG: Dict[str, Any] = {
    "paused": False,
    "fail_threshold": 3,
    "scan_interval_minutes": 20,
    "last_update_id": 0,
    "sources": {
        "A": {
            "enabled": True,
            "label": "תל אביב / חולון (שכ׳ 793)",
            "url": (
                "https://www.yad2.co.il/realestate/rent/tel-aviv-area"
                "?maxPrice=10000&minRooms=4&parking=1&shelter=1&area=11&city=6600&neighborhood=793"
                "&bBox=32.021438%2C34.773431%2C32.029569%2C34.785892&zoom=15"
            ),
            "api_url": (
                "https://gw.yad2.co.il/realestate-feed/rent/map"
                "?region=3&maxPrice=10000&minRooms=4&parking=1&shelter=1"
                "&area=11&city=6600&neighborhood=793"
            ),
            "state_file": "state_tlv.json",
        },
        "B": {
            "enabled": True,
            "label": "מרכז והשרון",
            "url": (
                "https://www.yad2.co.il/realestate/rent/center-and-sharon"
                "?maxPrice=10000&minRooms=4"
                "&multiNeighborhood=470%2C991420%2C991421%2C20436"
            ),
            "api_url": (
                "https://gw.yad2.co.il/realestate-feed/rent/map"
                "?region=1&multiNeighborhood=470,991420,991421,20436"
                "&maxPrice=10000&minRooms=4"
            ),
            "state_file": "state_center_sharon.json",
        },
    },
}

# Region slug mapping for Yad2 URLs -> gw.yad2.co.il region param
SLUG_TO_REGION = {
    "center-and-sharon": "1",
    "jerusalem-area": "2",
    "tel-aviv-area": "3",
    "north-and-valleys": "4",
    "coastal-north": "5",
    "south": "6",
    "sharon": "7",
}

IMPERSONATE_PROFILES = ["chrome131", "chrome124", "safari17_0", "edge101"]


# ─────────────────────────────────────────────
# HELPERS & STATE / CONFIG IO
# ─────────────────────────────────────────────

def now_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


def load_json(path: Path, default: dict) -> dict:
    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log.warning(f"Failed reading {path}: {e}")
    return json.loads(json.dumps(default))


def save_json(path: Path, data: dict):
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def load_config() -> Dict[str, Any]:
    cfg = load_json(CONFIG_FILE, DEFAULT_CONFIG)
    if "sources" not in cfg or not isinstance(cfg["sources"], dict):
        cfg["sources"] = json.loads(json.dumps(DEFAULT_CONFIG["sources"]))
    return cfg


def save_config(cfg: Dict[str, Any]):
    save_json(CONFIG_FILE, cfg)
    log.info(f"Config saved → {CONFIG_FILE}")


def get_state_path(src_info: dict, src_key: str) -> Path:
    sf = src_info.get("state_file") or f"state_{src_key.lower()}.json"
    p = Path(sf)
    return p if p.is_absolute() else (STATE_DIR / p.name)


def load_state(path: Path) -> dict:
    return load_json(path, {"listings": {}, "lastCheck": None, "consecutiveFailures": 0})


def save_state(path: Path, state: dict):
    save_json(path, state)
    log.info(f"State saved → {path}")


def git_sync_state(commit_msg: str):
    """If running inside a Git repo (e.g. GitHub Actions), commit & push state changes."""
    if os.environ.get("GITHUB_ACTIONS") != "true" and os.environ.get("AUTO_GIT_PUSH") != "1":
        return
    try:
        repo_root = Path(__file__).resolve().parent
        subprocess.run(["git", "config", "user.name", "yad2-bot"], cwd=repo_root, check=False)
        subprocess.run(["git", "config", "user.email", "yad2-bot@users.noreply.github.com"], cwd=repo_root, check=False)
        subprocess.run(["git", "add", "state/"], cwd=repo_root, check=False)
        diff = subprocess.run(["git", "diff", "--staged", "--quiet"], cwd=repo_root)
        if diff.returncode != 0:
            subprocess.run(["git", "commit", "-m", commit_msg], cwd=repo_root, check=False)
            subprocess.run(["git", "pull", "--rebase", "origin", "main"], cwd=repo_root, check=False)
            subprocess.run(["git", "push"], cwd=repo_root, check=False)
            log.info(f"Git state synced: {commit_msg}")
    except Exception as e:
        log.warning(f"git_sync_state warning: {e}")


# ─────────────────────────────────────────────
# URL PARSER & PARAMETER UPDATER
# ─────────────────────────────────────────────

def update_url_param(url: str, param: str, value: Any) -> str:
    """Update or add a query parameter on a URL cleanly."""
    if not url:
        return url
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    if value is None or value == "":
        qs.pop(param, None)
    else:
        qs[param] = [str(value)]
    # Flatten single-element lists and keep commas unescaped for multiNeighborhood
    pairs = []
    for k, vals in qs.items():
        v = vals[-1] if vals else ""
        pairs.append((k, v))
    new_query = urlencode(pairs, safe=",")
    return urlunparse(parsed._replace(query=new_query))


def get_url_param(url: str, param: str) -> str:
    if not url:
        return ""
    qs = parse_qs(urlparse(url).query)
    vals = qs.get(param)
    return vals[0] if vals else ""


def yad2_web_url_to_api_url(web_url: str) -> str:
    """
    Convert any standard Yad2 search URL (or gw.yad2.co.il URL) into a valid
    gw.yad2.co.il/realestate-feed/rent/map API URL.
    """
    web_url = web_url.strip()
    if "gw.yad2.co.il" in web_url:
        return web_url

    parsed = urlparse(web_url)
    path_parts = [p for p in parsed.path.split("/") if p]
    # Determine feed type (rent vs forsale) and region slug
    feed_type = "rent"
    region = ""
    for part in path_parts:
        if part in ("rent", "forsale"):
            feed_type = part
        if part in SLUG_TO_REGION:
            region = SLUG_TO_REGION[part]

    qs = parse_qs(parsed.query, keep_blank_values=True)
    if not region:
        if "region" in qs:
            region = qs["region"][0]
        elif "multiNeighborhood" in qs:
            region = "1"  # default center-and-sharon if multiNeighborhood used
        else:
            region = "3"  # default tel-aviv-area

    api_params: List[Tuple[str, str]] = [("region", region)]
    skip_keys = {"region", "zoom", "bBox"}
    for k, vals in qs.items():
        if k in skip_keys or not vals:
            continue
        api_params.append((k, vals[0]))

    query_str = urlencode(api_params, safe=",")
    return f"https://gw.yad2.co.il/realestate-feed/{feed_type}/map?{query_str}"


# ─────────────────────────────────────────────
# TELEGRAM API & CHUNKED SENDER
# ─────────────────────────────────────────────

def write_failed_alert(message: str):
    ts = now_ist()
    with open(FAILED_ALERTS_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {message}\n")


def split_telegram_text(text: str, max_len: int = 3800) -> List[str]:
    """Split long HTML messages along newline boundaries so Telegram never returns 400."""
    if len(text) <= max_len:
        return [text]
    chunks = []
    current: List[str] = []
    cur_len = 0
    for line in text.split("\n"):
        if cur_len + len(line) + 1 > max_len and current:
            chunks.append("\n".join(current))
            current = [line]
            cur_len = len(line) + 1
        else:
            current.append(line)
            cur_len += len(line) + 1
    if current:
        chunks.append("\n".join(current))
    return chunks


def send_telegram(
    text: str,
    reply_markup: Optional[dict] = None,
    chat_id: Optional[int] = None,
    disable_preview: bool = True,
) -> bool:
    """Send HTML message to Telegram (automatically chunked if >3800 chars)."""
    target_chat = chat_id or TELEGRAM_CHAT_ID
    chunks = split_telegram_text(text)
    ok_all = True
    for idx, chunk in enumerate(chunks):
        payload: Dict[str, Any] = {
            "chat_id": target_chat,
            "text": chunk,
            "parse_mode": "HTML",
            "disable_web_page_preview": disable_preview,
        }
        # Attach inline keyboard to the last chunk
        if reply_markup and idx == len(chunks) - 1:
            payload["reply_markup"] = reply_markup
        try:
            resp = requests.post(
                f"{TELEGRAM_BASE}/sendMessage",
                json=payload,
                timeout=15,
                verify=False,
            )
            if resp.status_code != 200:
                log.error(f"Telegram API error {resp.status_code}: {resp.text[:250]}")
                write_failed_alert(f"TELEGRAM FAILED ({resp.status_code}): {chunk[:300]}")
                ok_all = False
        except requests.RequestException as e:
            log.error(f"Telegram network error: {e}")
            write_failed_alert(f"TELEGRAM NETWORK ERROR: {e} | {chunk[:300]}")
            ok_all = False
    if ok_all:
        log.info("Telegram message sent ✓")
    return ok_all


def edit_or_send_telegram(
    text: str,
    message_id: Optional[int] = None,
    reply_markup: Optional[dict] = None,
    chat_id: Optional[int] = None,
):
    target_chat = chat_id or TELEGRAM_CHAT_ID
    if message_id and len(text) <= 3800:
        payload: Dict[str, Any] = {
            "chat_id": target_chat,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            resp = requests.post(
                f"{TELEGRAM_BASE}/editMessageText",
                json=payload,
                timeout=15,
                verify=False,
            )
            if resp.status_code == 200:
                return
        except requests.RequestException:
            pass
    send_telegram(text, reply_markup=reply_markup, chat_id=target_chat)


def answer_callback(callback_id: str, text: str = ""):
    try:
        requests.post(
            f"{TELEGRAM_BASE}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text},
            timeout=10,
            verify=False,
        )
    except requests.RequestException:
        pass


def register_bot_commands():
    """Register native "/" command menu in Telegram."""
    commands = [
        {"command": "menu", "description": "🎛 תפריט שליטה ראשי (כפתורים)"},
        {"command": "scan", "description": "🔍 סריקה מיידית של כל החיפושים"},
        {"command": "status", "description": "📊 סטטוס המערכת והחיפושים"},
        {"command": "summary", "description": "📋 סיכום כל הדירות הפעילות כרגע"},
        {"command": "searches", "description": "⚙️ ניהול ועריכת חיפושים (מחיר/חדרים/השהיה)"},
        {"command": "add", "description": "➕ הוספת חיפוש חדש מלינק של יד 2"},
        {"command": "pause", "description": "⏸ השהיית כל הסריקות"},
        {"command": "resume", "description": "▶️ הפעלת הסריקות מחדש"},
    ]
    try:
        requests.post(
            f"{TELEGRAM_BASE}/setMyCommands",
            json={"commands": commands},
            timeout=10,
            verify=False,
        )
    except requests.RequestException:
        pass


# ─────────────────────────────────────────────
# 100% API SCRAPER (NO PLAYWRIGHT)
# ─────────────────────────────────────────────

def make_hash(listing: dict) -> str:
    key = "|".join(
        str(listing.get(f, ""))
        for f in ["id", "price", "rooms", "floor", "address", "description"]
    )
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def normalize_api_listing(raw: dict) -> Optional[dict]:
    """Normalize a raw marker item from gw.yad2.co.il/realestate-feed/rent/map."""
    try:
        item_id = str(raw.get("orderId") or raw.get("id") or raw.get("token") or raw.get("adNumber") or "")
        if not item_id or item_id in ("0", "undefined", "null"):
            return None

        price_raw = raw.get("price") or raw.get("priceFormatted") or 0
        price_digits = "".join(ch for ch in str(price_raw) if ch.isdigit())
        price = int(price_digits) if price_digits else 0

        details = raw.get("additionalDetails") or {}
        rooms = str(details.get("roomsCount") or raw.get("rooms") or raw.get("roomsCount") or "")
        sqm = str(details.get("squareMeter") or "")
        prop_type = (details.get("property") or {}).get("text") or ""
        tags_list = [t.get("name", "") for t in (raw.get("tags") or []) if isinstance(t, dict) and t.get("name")]
        tags = ", ".join(tags_list[:4])

        addr_obj = raw.get("address") or {}
        if isinstance(addr_obj, dict):
            street_obj = addr_obj.get("street") or {}
            street_name = (street_obj.get("text") if isinstance(street_obj, dict) else str(street_obj)) or ""
            house_obj = addr_obj.get("house") or {}
            house_num = (str(house_obj.get("number") or "") if isinstance(house_obj, dict) else str(house_obj)) or ""
            floor_num = (str(house_obj.get("floor") or "") if isinstance(house_obj, dict) else "") or ""
            city_obj = addr_obj.get("city") or {}
            city_name = (city_obj.get("text") if isinstance(city_obj, dict) else str(city_obj)) or ""
            neighborhood_obj = addr_obj.get("neighborhood") or {}
            neighborhood = (neighborhood_obj.get("text") if isinstance(neighborhood_obj, dict) else "") or ""
            street_part = f"{street_name} {house_num}".strip()
            address = ", ".join(p for p in [street_part, city_name] if p)
            if neighborhood:
                address = f"{address} ({neighborhood})" if address else neighborhood
        else:
            address = str(addr_obj)
            floor_num = ""

        floor = floor_num or str(raw.get("floor") or raw.get("floorCount") or "")
        description = str(
            raw.get("info") or raw.get("title") or (raw.get("metaData") or {}).get("h1") or ""
        )[:120]
        link_token = raw.get("token") or raw.get("orderId") or item_id
        url = f"https://www.yad2.co.il/item/{link_token}" if link_token else ""

        listing = {
            "id": item_id,
            "price": price,
            "rooms": rooms,
            "floor": floor,
            "sqm": sqm,
            "prop_type": prop_type,
            "tags": tags,
            "address": address or f"מודעה {item_id}",
            "description": description,
            "url": url,
        }
        listing["hash"] = make_hash(listing)
        return listing
    except Exception as e:
        log.debug(f"normalize_api_listing error: {e}")
        return None


def _parse_api_response(resp_text: str, content_type: str, label: str) -> Optional[Dict[str, dict]]:
    if "json" not in content_type.lower():
        snippet = (resp_text or "")[:180].replace("\n", " ")
        log.warning(f"[{label}] Non-JSON response ({content_type}): {snippet}")
        return None
    data = json.loads(resp_text)
    raw_data = data.get("data") or {}
    markers = raw_data.get("markers")
    if markers is None and isinstance(raw_data.get("feed"), dict):
        markers = raw_data["feed"].get("feed_items")
    if markers is None:
        log.warning(f"[{label}] JSON missing 'markers' key")
        return None

    listings: Dict[str, dict] = {}
    for raw in markers:
        normalized = normalize_api_listing(raw)
        if normalized:
            listings[normalized["id"]] = normalized
    log.info(f"[{label}] API returned {len(markers)} markers → {len(listings)} valid listings")
    return listings


def fetch_via_api(api_url: str, label: str, web_url: str = "") -> Optional[Dict[str, dict]]:
    """
    100% API fetcher with multi-layer anti-bot bypass:
    1. Tries curl_cffi with modern browser TLS impersonation profiles
       (chrome131, chrome124, safari17_0, edge101) — importantly WITHOUT
       overriding User-Agent so TLS JA3/JA4 + HTTP/2 match User-Agent 100%.
    2. If challenged, warms up cookies on www.yad2.co.il first and retries.
    3. If WORKER_PROXY env var is set, also tries routing via Cloudflare Worker.
    """
    base_headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin": "https://www.yad2.co.il",
        "Referer": web_url or "https://www.yad2.co.il/realestate/rent",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
    }

    # Layer 1: Direct curl_cffi with profile rotation + cookie warm-up on retry
    if HAS_CURL_CFFI:
        for idx, profile in enumerate(IMPERSONATE_PROFILES):
            try:
                session = cffi_requests.Session(impersonate=profile)
                # On second+ profile attempt, warm up Radware cookies on main domain first
                if idx > 0:
                    try:
                        session.get(
                            web_url or "https://www.yad2.co.il/realestate/rent",
                            headers={"Accept-Language": "he-IL,he;q=0.9"},
                            timeout=12,
                            verify=False,
                        )
                    except Exception:
                        pass

                log.info(f"[{label}] API (curl_cffi/{profile}) → {api_url[:95]}…")
                r = session.get(api_url, headers=base_headers, timeout=20, verify=False)
                if r.status_code == 200:
                    parsed = _parse_api_response(
                        r.text, r.headers.get("content-type", ""), label
                    )
                    if parsed is not None:
                        return parsed
                else:
                    log.warning(f"[{label}] Profile {profile} returned HTTP {r.status_code}")
            except Exception as e:
                log.warning(f"[{label}] curl_cffi ({profile}) error: {e}")

    # Layer 2: Optional Cloudflare Worker proxy if configured
    worker = os.environ.get("WORKER_PROXY", "").strip().rstrip("/")
    if worker:
        target_url = api_url.replace("https://gw.yad2.co.il", worker)
        proxy_headers = dict(base_headers)
        worker_token = os.environ.get("WORKER_TOKEN", "").strip()
        if worker_token:
            proxy_headers["X-Auth-Token"] = worker_token
        try:
            log.info(f"[{label}] Trying WORKER_PROXY → {target_url[:90]}…")
            if HAS_CURL_CFFI:
                r = cffi_requests.get(
                    target_url, headers=proxy_headers, impersonate="chrome131", timeout=20, verify=False
                )
            else:
                r = requests.get(target_url, headers=proxy_headers, timeout=20, verify=False)
            if r.status_code == 200:
                parsed = _parse_api_response(r.text, r.headers.get("content-type", ""), label)
                if parsed is not None:
                    return parsed
        except Exception as e:
            log.warning(f"[{label}] WORKER_PROXY error: {e}")

    return None


# ─────────────────────────────────────────────
# CHANGE DETECTION & FORMATTING
# ─────────────────────────────────────────────

FIELD_LABELS = {
    "price": "מחיר",
    "rooms": "חדרים",
    "floor": "קומה",
    "address": "כתובת",
    "description": "תיאור",
}


def detect_changes(old_listings: dict, new_listings: dict) -> Tuple[list, list]:
    new_items = []
    updated_items = []
    for item_id, listing in new_listings.items():
        if item_id not in old_listings:
            new_items.append(listing)
        else:
            old = old_listings[item_id]
            if listing["hash"] != old.get("hash", ""):
                changed = {}
                for field in ["price", "rooms", "floor", "address", "description"]:
                    if str(listing.get(field, "")) != str(old.get(field, "")):
                        changed[field] = {"old": old.get(field, ""), "new": listing[field]}
                if changed:
                    listing_copy = dict(listing)
                    listing_copy["_changed"] = changed
                    updated_items.append(listing_copy)
    return new_items, updated_items


def format_listing_new(listing: dict) -> str:
    price = (
        f"₪{listing['price']:,}"
        if isinstance(listing.get("price"), int) and listing["price"]
        else "מחיר לא צוין"
    )
    rooms = listing.get("rooms") or "?"
    floor = listing.get("floor") or "?"
    sqm = listing.get("sqm") or ""
    prop_type = html.escape(str(listing.get("prop_type") or ""))
    tags = html.escape(str(listing.get("tags") or ""))
    address = html.escape(str(listing.get("address") or "?"))
    desc = html.escape(str(listing.get("description") or ""))
    url = listing.get("url") or ""

    lines = [f"• <b>{address}</b>"]
    specs = []
    if rooms and rooms != "?":
        specs.append(f"{rooms} חד׳")
    if sqm and sqm != "0":
        specs.append(f"{sqm} מ״ר")
    if floor and floor != "?":
        specs.append(f"קומה {floor}")
    if prop_type:
        specs.append(prop_type)
    specs.append(f"<b>{price}</b>")
    lines.append("  " + " | ".join(specs))
    if tags:
        lines.append(f"  🏷 {tags}")
    if desc:
        lines.append(f"  <i>{desc[:100]}</i>")
    if url:
        lines.append(f'  🔗 <a href="{url}">לצפייה במודעה</a>')
    return "\n".join(lines)


def format_listing_updated(listing: dict) -> str:
    address = html.escape(str(listing.get("address") or listing.get("id") or "?"))
    url = listing.get("url") or ""
    changed = listing.get("_changed", {})
    lines = [f"• <b>{address}</b>"]
    for field, vals in changed.items():
        label = FIELD_LABELS.get(field, field)
        old_v = f"₪{vals['old']:,}" if field == "price" and isinstance(vals["old"], int) and vals["old"] else html.escape(str(vals["old"]))
        new_v = f"₪{vals['new']:,}" if field == "price" and isinstance(vals["new"], int) and vals["new"] else html.escape(str(vals["new"]))
        lines.append(f"  {label}: {old_v} ← <b>{new_v}</b>")
    if url:
        lines.append(f'  🔗 <a href="{url}">לצפייה במודעה</a>')
    return "\n".join(lines)


def build_telegram_message(sources_cfg: dict, results: dict, counts: dict) -> Optional[str]:
    ts = now_ist()
    has_any_change = any(
        results[src]["new"] or results[src]["updated"]
        for src in results
    )
    if not has_any_change:
        return None

    parts = [f"🏠 <b>Yad2 Monitor</b> | {ts}"]
    for src_key, src_info in sources_cfg.items():
        if src_key not in results:
            continue
        new_items = results[src_key]["new"]
        updated_items = results[src_key]["updated"]
        if not new_items and not updated_items:
            continue

        parts.append(f"\n📍 <b>{html.escape(src_info.get('label', src_key))}</b>")
        if new_items:
            parts.append(f"✅ <b>חדש ({len(new_items)}):</b>")
            for item in new_items:
                parts.append(format_listing_new(item))
        if updated_items:
            parts.append(f"🔄 <b>עודכן ({len(updated_items)}):</b>")
            for item in updated_items:
                parts.append(format_listing_updated(item))

    summary_counts = " | ".join(f"{k}={counts.get(k, 0)}" for k in sources_cfg if k in counts)
    parts.append(f"\n📊 פעילות כרגע: {summary_counts}")
    return "\n".join(parts)


def update_state(old_state: dict, new_listings: dict) -> dict:
    ts = now_ist()
    updated = old_state.copy()
    old_listings = updated.get("listings", {})

    # Mark all existing listings as inactive first; active ones will be flipped back to True
    for existing in old_listings.values():
        existing["active"] = False

    for item_id, listing in new_listings.items():
        if item_id in old_listings:
            merged = old_listings[item_id].copy()
            merged.update(listing)
            merged["lastSeen"] = ts
            merged["active"] = True
            old_listings[item_id] = merged
        else:
            listing_copy = dict(listing)
            listing_copy["firstSeen"] = ts
            listing_copy["lastSeen"] = ts
            listing_copy["active"] = True
            old_listings[item_id] = listing_copy

    updated["listings"] = old_listings
    updated["lastCheck"] = ts
    updated["activeCount"] = len(new_listings)
    updated["consecutiveFailures"] = 0
    return updated


# ─────────────────────────────────────────────
# CORE MONITOR RUNNER
# ─────────────────────────────────────────────

def run_monitor_scan(
    notify_even_if_no_changes: bool = False,
    only_source: Optional[str] = None,
) -> Dict[str, Any]:
    """Run an API scan across all enabled sources (or `only_source`)."""
    cfg = load_config()
    if cfg.get("paused") and not notify_even_if_no_changes and not only_source:
        log.info("Monitor is globally PAUSED in config.json — skipping scheduled scan.")
        return {"status": "paused"}

    sources_cfg = cfg.get("sources", {})
    skip_env = set(s.strip() for s in os.environ.get("SKIP_SOURCES", "").split(",") if s.strip())

    states: Dict[str, dict] = {}
    fetched: Dict[str, dict] = {}
    failed_sources: List[str] = []

    for src_key, src_info in sources_cfg.items():
        if only_source and src_key != only_source:
            continue
        if not src_info.get("enabled", True) and not only_source:
            log.info(f"[Source {src_key}] Disabled in config — skipping.")
            continue
        if src_key in skip_env and not only_source:
            log.info(f"[Source {src_key}] Skipped via SKIP_SOURCES env var.")
            continue

        state_path = get_state_path(src_info, src_key)
        states[src_key] = load_state(state_path)

        api_url = src_info.get("api_url") or yad2_web_url_to_api_url(src_info.get("url", ""))
        result = fetch_via_api(api_url, src_info.get("label", src_key), src_info.get("url", ""))
        if result is not None:
            fetched[src_key] = result
        else:
            failed_sources.append(src_key)

    fail_threshold = int(cfg.get("fail_threshold", os.environ.get("FAIL_THRESHOLD", "3")))

    for src_key in failed_sources:
        prev = int(states[src_key].get("consecutiveFailures", 0))
        fails = prev + 1
        states[src_key]["consecutiveFailures"] = fails
        save_state(get_state_path(sources_cfg[src_key], src_key), states[src_key])
        if fails >= fail_threshold or notify_even_if_no_changes:
            send_telegram(
                f"⚠️ <b>Yad2 Monitor — שגיאת גישה ל-API</b>\n"
                f"מקור <b>{html.escape(sources_cfg[src_key].get('label', src_key))}</b> ({src_key}) "
                f"לא נטען ב-{now_ist()} ({fails} כשלים רצופים)."
            )

    if not fetched:
        return {"status": "failed", "failed": failed_sources}

    results = {}
    counts = {}
    total_new = 0
    total_updated = 0
    for src_key, listings in fetched.items():
        old = states[src_key].get("listings", {})
        new_items, updated_items = detect_changes(old, listings)
        results[src_key] = {"new": new_items, "updated": updated_items}
        counts[src_key] = len(listings)
        total_new += len(new_items)
        total_updated += len(updated_items)
        log.info(
            f"[Source {src_key}] NEW={len(new_items)}, UPDATED={len(updated_items)}, ACTIVE={len(listings)}"
        )

    message = build_telegram_message(sources_cfg, results, counts)
    if message:
        send_telegram(message, reply_markup=main_menu_keyboard(cfg))
    elif notify_even_if_no_changes:
        summary_lines = [f"✅ <b>סריקה הושלמה בהצלחה</b> ({now_ist()})", "אין מודעות חדשות או שינויי מחיר כרגע.\n"]
        for src_key, cnt in counts.items():
            lbl = html.escape(sources_cfg[src_key].get("label", src_key))
            summary_lines.append(f"• <b>{lbl} ({src_key}):</b> {cnt} דירות פעילות")
        send_telegram("\n".join(summary_lines), reply_markup=main_menu_keyboard(cfg))
    else:
        log.info(f"✓ {now_ist()} — No changes. Counts: {counts}")

    for src_key, listings in fetched.items():
        new_state = update_state(states[src_key], listings)
        save_state(get_state_path(sources_cfg[src_key], src_key), new_state)

    git_sync_state(f"state: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}")
    return {
        "status": "ok",
        "counts": counts,
        "new": total_new,
        "updated": total_updated,
        "failed": failed_sources,
    }


def send_daily_summary(only_source: Optional[str] = None):
    """Send currently active listings from state files to Telegram."""
    cfg = load_config()
    sources_cfg = cfg.get("sources", {})
    ts = now_ist()

    for src_key, src_info in sources_cfg.items():
        if only_source and src_key != only_source:
            continue
        if not src_info.get("enabled", True) and not only_source:
            continue

        state_path = get_state_path(src_info, src_key)
        data = load_state(state_path)
        all_listings = list(data.get("listings", {}).values())
        last_check = data.get("lastCheck")

        # Filter to currently active listings (or listings seen on lastCheck if 'active' flag isn't set yet)
        active_listings = [
            l for l in all_listings
            if l.get("active") is True or (l.get("active") is None and last_check and l.get("lastSeen") == last_check)
        ]
        if not active_listings and all_listings:
            # Fallback: take top 20 most recently seen
            active_listings = sorted(all_listings, key=lambda x: x.get("lastSeen", ""), reverse=True)[:20]

        total = len(active_listings)
        label = html.escape(src_info.get("label", src_key))
        if total == 0:
            send_telegram(f"📋 <b>סיכום דירות — {label} ({src_key})</b>\nאין דירות פעילות שמורות כרגע.")
            continue

        batch_size = 8
        for i in range(0, total, batch_size):
            batch = active_listings[i : i + batch_size]
            header = (
                f"📋 <b>סיכום דירות פעילות | {ts}</b>\n"
                f"📍 <b>{label} ({src_key})</b> — מציג {i + 1}–{min(i + len(batch), total)} מתוך {total}\n"
            )
            lines = [header] + [format_listing_new(l) for l in batch]
            send_telegram("\n".join(lines))

    send_telegram("🎛 <b>סיום סיכום דירות</b> — בחר פעולה:", reply_markup=main_menu_keyboard(cfg))


# ─────────────────────────────────────────────
# INTERACTIVE TELEGRAM BOT CONTROL
# ─────────────────────────────────────────────

def main_menu_keyboard(cfg: dict) -> dict:
    paused = cfg.get("paused", False)
    pause_btn = (
        {"text": "▶️ הפעל מוניטור (מושהה כעת)", "callback_data": "toggle_global"}
        if paused
        else {"text": "⏸ השהה מוניטור", "callback_data": "toggle_global"}
    )
    return {
        "inline_keyboard": [
            [
                {"text": "🔍 סרוק עכשיו", "callback_data": "scan_now"},
                {"text": "📊 סטטוס ומצב", "callback_data": "status"},
            ],
            [
                {"text": "📋 סיכום דירות פעילות", "callback_data": "summary_all"},
                {"text": "⚙️ ניהול חיפושים", "callback_data": "manage_sources"},
            ],
            [
                pause_btn,
                {"text": "➕ הוסף חיפוש חדש", "callback_data": "help_add"},
            ],
        ]
    }


def sources_menu_keyboard(cfg: dict) -> dict:
    rows = []
    for src_key, src_info in cfg.get("sources", {}).items():
        enabled = src_info.get("enabled", True)
        icon = "🟢" if enabled else "🔴"
        max_p = get_url_param(src_info.get("api_url", ""), "maxPrice") or "ללא הגבלה"
        min_r = get_url_param(src_info.get("api_url", ""), "minRooms") or "הכל"
        label = src_info.get("label", src_key)
        rows.append([
            {
                "text": f"{icon} [{src_key}] {label} (עד ₪{max_p}, {min_r}+ חד׳)",
                "callback_data": f"src_view:{src_key}",
            }
        ])
    rows.append([{"text": "➕ הוסף חיפוש חדש", "callback_data": "help_add"}])
    rows.append([{"text": "🔙 חזרה לתפריט הראשי", "callback_data": "main_menu"}])
    return {"inline_keyboard": rows}


def single_source_keyboard(src_key: str, src_info: dict) -> dict:
    enabled = src_info.get("enabled", True)
    toggle_text = "⏸ השהה חיפוש זה" if enabled else "▶️ הפעל חיפוש זה"
    return {
        "inline_keyboard": [
            [
                {"text": toggle_text, "callback_data": f"src_toggle:{src_key}"},
                {"text": "🔍 סרוק חיפוש זה", "callback_data": f"src_scan:{src_key}"},
            ],
            [
                {"text": "💰 שנה מחיר מקסימלי", "callback_data": f"price_menu:{src_key}"},
                {"text": "🛏 שנה מינימום חדרים", "callback_data": f"rooms_menu:{src_key}"},
            ],
            [
                {"text": "📋 הצג דירות פעילות בחיפוש זה", "callback_data": f"src_summary:{src_key}"},
            ],
            [
                {"text": "🗑 מחק חיפוש", "callback_data": f"del_confirm:{src_key}"},
                {"text": "🔙 חזרה לחיפושים", "callback_data": "manage_sources"},
            ],
        ]
    }


def build_status_text(cfg: dict) -> str:
    paused = cfg.get("paused", False)
    global_state = "🔴 <b>מושהה (Paused)</b>" if paused else "🟢 <b>פעיל (Active)</b>"
    lines = [
        f"🎛 <b>לוח בקרה — Yad2 Cloud Monitor</b>",
        f"מצב מערכת: {global_state}",
        f"זמן שרת (IST): <code>{now_ist()}</code>",
        f"שיטת סריקה: <b>100% API ישיר (ללא דפדפן)</b>\n",
        "📍 <b>חיפושים מוגדרים:</b>",
    ]
    for src_key, src_info in cfg.get("sources", {}).items():
        enabled = src_info.get("enabled", True)
        icon = "🟢 פעיל" if enabled else "🔴 מושהה"
        label = html.escape(src_info.get("label", src_key))
        api_url = src_info.get("api_url", "")
        max_p = get_url_param(api_url, "maxPrice") or "ללא"
        min_r = get_url_param(api_url, "minRooms") or "ללא"

        st = load_state(get_state_path(src_info, src_key))
        last_check = st.get("lastCheck") or "טרם נסרק"
        all_l = list(st.get("listings", {}).values())
        active_cnt = st.get("activeCount")
        if active_cnt is None:
            active_cnt = sum(1 for l in all_l if l.get("lastSeen") == last_check) if last_check != "טרם נסרק" else len(all_l)
        fails = st.get("consecutiveFailures", 0)
        fail_str = f" | ⚠️ כשלים: {fails}" if fails else ""

        lines.append(
            f"\n<b>[{src_key}] {label}</b> — {icon}\n"
            f"  💰 עד ₪{max_p} | 🛏 מ-{min_r} חדרים\n"
            f"  🏠 דירות פעילות: <b>{active_cnt}</b> (סה״כ בהיסטוריה: {len(all_l)})\n"
            f"  🕒 בדיקה אחרונה: <code>{last_check}</code>{fail_str}"
        )
    return "\n".join(lines)


def build_source_detail_text(src_key: str, src_info: dict) -> str:
    enabled = src_info.get("enabled", True)
    icon = "🟢 פעיל" if enabled else "🔴 מושהה"
    label = html.escape(src_info.get("label", src_key))
    api_url = src_info.get("api_url", "")
    web_url = src_info.get("url", "")
    max_p = get_url_param(api_url, "maxPrice") or "ללא הגבלה"
    min_r = get_url_param(api_url, "minRooms") or "ללא הגבלה"
    st = load_state(get_state_path(src_info, src_key))
    last_check = st.get("lastCheck") or "טרם נסרק"
    active_cnt = st.get("activeCount", len(st.get("listings", {})))

    text = (
        f"⚙️ <b>הגדרות חיפוש [{src_key}] — {label}</b>\n\n"
        f"• <b>סטטוס:</b> {icon}\n"
        f"• <b>מחיר מקסימלי:</b> ₪{max_p}\n"
        f"• <b>מינימום חדרים:</b> {min_r}\n"
        f"• <b>דירות פעילות כרגע:</b> {active_cnt}\n"
        f"• <b>סריקה אחרונה:</b> <code>{last_check}</code>\n"
    )
    if web_url:
        text += f'• 🔗 <a href="{html.escape(web_url)}">קישור לחיפוש באתר יד 2</a>\n'
    text += (
        f"\n💡 <i>ניתן לשנות תקציב או חדרים מהכפתורים למטה, או בפקודה:</i>\n"
        f"<code>/setprice {src_key} 11000</code>\n"
        f"<code>/setrooms {src_key} 3.5</code>"
    )
    return text


def handle_add_source_command(cfg: dict, raw_arg: str) -> str:
    """
    Add a new search source from a Yad2 URL!
    Format: /add [Name |] https://www.yad2.co.il/realestate/rent/...
    """
    raw_arg = raw_arg.strip()
    if "|" in raw_arg:
        label_part, url_part = [x.strip() for x in raw_arg.split("|", 1)]
    else:
        url_part = raw_arg
        label_part = ""

    if not url_part.startswith("http"):
        return (
            "➕ <b>הוספת חיפוש חדש</b>\n\n"
            "שלח לי קישור חיפוש מאתר יד 2 בפורמט הבא:\n"
            "<code>/add שם החיפוש | https://www.yad2.co.il/realestate/rent/...</code>\n\n"
            "או פשוט הדבק בצ׳אט קישור מיד 2 ואני אמיר אותו אוטומטית ל-API!"
        )

    api_url = yad2_web_url_to_api_url(url_part)
    # Generate next source key (C, D, E...)
    existing_keys = set(cfg.get("sources", {}).keys())
    next_key = "C"
    for ch in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        if ch not in existing_keys:
            next_key = ch
            break
    else:
        next_key = f"S{len(existing_keys) + 1}"

    if not label_part:
        max_p = get_url_param(api_url, "maxPrice")
        label_part = f"חיפוש {next_key}" + (f" (עד ₪{max_p})" if max_p else "")

    # Test live via API before saving
    listings = fetch_via_api(api_url, label_part, url_part)
    count_found = len(listings) if listings is not None else -1

    state_filename = f"state_{next_key.lower()}.json"
    cfg["sources"][next_key] = {
        "enabled": True,
        "label": label_part,
        "url": url_part,
        "api_url": api_url,
        "state_file": state_filename,
    }
    save_config(cfg)

    if listings is not None:
        initial_state = update_state({"listings": {}, "lastCheck": None}, listings)
        save_state(STATE_DIR / state_filename, initial_state)
        git_sync_state(f"config: add source {next_key} ({label_part})")
        return (
            f"✅ <b>חיפוש חדש נוסף בהצלחה! [{next_key}]</b>\n\n"
            f"• <b>שם:</b> {html.escape(label_part)}\n"
            f"• <b>דירות שנמצאו כרגע ב-API:</b> {count_found}\n"
            f"• כל {count_found} הדירות הקיימות נשמרו ב-State, ומעכשיו תקבל התרעה על כל דירה חדשה או עדכון מחיר!"
        )
    else:
        git_sync_state(f"config: add source {next_key}")
        return (
            f"⚠️ <b>החיפוש [{next_key}] ({html.escape(label_part)}) נשמר</b>, "
            f"אך בדיקת ה-API הראשונית לא החזירה תוצאות. בדוק את הקישור או נסה לסרוק שוב."
        )


def process_telegram_updates(listen_seconds: int = 0) -> bool:
    """
    Poll Telegram `getUpdates` and handle all user commands & button clicks.
    If `listen_seconds > 0`, keeps listening with long-polling so interactive
    clicks in Telegram are answered within ~1 second!
    Returns True if any config/state changed.
    """
    cfg = load_config()
    offset = int(cfg.get("last_update_id", 0)) + 1
    changed_any = False
    deadline = time.time() + max(0, listen_seconds)

    while True:
        remaining = deadline - time.time()
        poll_timeout = min(20, max(0, int(remaining))) if listen_seconds > 0 else 0
        try:
            resp = requests.get(
                f"{TELEGRAM_BASE}/getUpdates",
                params={"offset": offset, "timeout": poll_timeout},
                timeout=poll_timeout + 10,
                verify=False,
            )
            if resp.status_code != 200:
                log.warning(f"getUpdates returned {resp.status_code}: {resp.text[:150]}")
                break
            updates = resp.json().get("result", [])
        except Exception as e:
            log.warning(f"getUpdates error: {e}")
            break

        for upd in updates:
            upd_id = upd.get("update_id", 0)
            offset = upd_id + 1
            cfg = load_config()
            cfg["last_update_id"] = upd_id
            save_config(cfg)
            changed_any = True

            # 1) Handle Inline Button Clicks (callback_query)
            cb = upd.get("callback_query")
            if cb:
                cb_id = cb.get("id", "")
                msg = cb.get("message") or {}
                chat_id = (msg.get("chat") or {}).get("id")
                msg_id = msg.get("message_id")
                data = cb.get("data", "")

                if chat_id != TELEGRAM_CHAT_ID:
                    answer_callback(cb_id, "⛔ לא מורשה")
                    continue

                log.info(f"Telegram button click: {data}")
                if data == "main_menu":
                    answer_callback(cb_id)
                    edit_or_send_telegram(build_status_text(cfg), msg_id, main_menu_keyboard(cfg))

                elif data == "status":
                    answer_callback(cb_id, "מרענן סטטוס…")
                    edit_or_send_telegram(build_status_text(cfg), msg_id, main_menu_keyboard(cfg))

                elif data == "scan_now":
                    answer_callback(cb_id, "🔍 מתחיל סריקה ב-API…")
                    send_telegram("🔍 <b>מבצע סריקת API מול יד 2 עכשיו…</b>")
                    run_monitor_scan(notify_even_if_no_changes=True)

                elif data == "summary_all":
                    answer_callback(cb_id, "📋 שולח סיכום דירות…")
                    send_daily_summary()

                elif data == "toggle_global":
                    cfg["paused"] = not cfg.get("paused", False)
                    save_config(cfg)
                    git_sync_state(f"config: paused={cfg['paused']}")
                    state_txt = "הושהה ⏸" if cfg["paused"] else "הופעל ▶️"
                    answer_callback(cb_id, f"המוניטור {state_txt}")
                    edit_or_send_telegram(build_status_text(cfg), msg_id, main_menu_keyboard(cfg))

                elif data == "manage_sources":
                    answer_callback(cb_id)
                    edit_or_send_telegram(
                        "⚙️ <b>ניהול חיפושים</b>\nבחר חיפוש לעריכת מחיר, חדרים, השהיה או מחיקה:",
                        msg_id,
                        sources_menu_keyboard(cfg),
                    )

                elif data == "help_add":
                    answer_callback(cb_id)
                    edit_or_send_telegram(
                        handle_add_source_command(cfg, ""),
                        msg_id,
                        {"inline_keyboard": [[{"text": "🔙 חזרה לתפריט", "callback_data": "main_menu"}]]},
                    )

                elif data.startswith("src_view:"):
                    src_key = data.split(":", 1)[1]
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        answer_callback(cb_id)
                        edit_or_send_telegram(
                            build_source_detail_text(src_key, src_info),
                            msg_id,
                            single_source_keyboard(src_key, src_info),
                        )

                elif data.startswith("src_toggle:"):
                    src_key = data.split(":", 1)[1]
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        src_info["enabled"] = not src_info.get("enabled", True)
                        save_config(cfg)
                        git_sync_state(f"config: toggle source {src_key} -> {src_info['enabled']}")
                        answer_callback(cb_id, "עודכן ✓")
                        edit_or_send_telegram(
                            build_source_detail_text(src_key, src_info),
                            msg_id,
                            single_source_keyboard(src_key, src_info),
                        )

                elif data.startswith("src_scan:"):
                    src_key = data.split(":", 1)[1]
                    answer_callback(cb_id, f"🔍 סורק את חיפוש {src_key}…")
                    run_monitor_scan(notify_even_if_no_changes=True, only_source=src_key)

                elif data.startswith("src_summary:"):
                    src_key = data.split(":", 1)[1]
                    answer_callback(cb_id, f"📋 שולח דירות מחיפוש {src_key}…")
                    send_daily_summary(only_source=src_key)

                elif data.startswith("price_menu:"):
                    src_key = data.split(":", 1)[1]
                    prices = ["8000", "9000", "10000", "11000", "12000", "14000", "15000"]
                    kb = [
                        [{"text": f"₪{int(p):,}", "callback_data": f"set_price:{src_key}:{p}"} for p in prices[:4]],
                        [{"text": f"₪{int(p):,}", "callback_data": f"set_price:{src_key}:{p}"} for p in prices[4:]],
                        [{"text": f"🔙 חזרה לחיפוש {src_key}", "callback_data": f"src_view:{src_key}"}],
                    ]
                    answer_callback(cb_id)
                    edit_or_send_telegram(
                        f"💰 <b>בחר מחיר מקסימלי עבור חיפוש [{src_key}]:</b>\n"
                        f"(או שלח ידנית: <code>/setprice {src_key} 10500</code>)",
                        msg_id,
                        {"inline_keyboard": kb},
                    )

                elif data.startswith("set_price:"):
                    _, src_key, new_price = data.split(":", 2)
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        src_info["api_url"] = update_url_param(src_info.get("api_url", ""), "maxPrice", new_price)
                        src_info["url"] = update_url_param(src_info.get("url", ""), "maxPrice", new_price)
                        save_config(cfg)
                        git_sync_state(f"config: {src_key} maxPrice={new_price}")
                        answer_callback(cb_id, f"מחיר עודכן ל-₪{int(new_price):,} ✓")
                        edit_or_send_telegram(
                            build_source_detail_text(src_key, src_info),
                            msg_id,
                            single_source_keyboard(src_key, src_info),
                        )

                elif data.startswith("rooms_menu:"):
                    src_key = data.split(":", 1)[1]
                    rooms_opts = ["2", "2.5", "3", "3.5", "4", "4.5", "5"]
                    kb = [
                        [{"text": f"{r}+ חד׳", "callback_data": f"set_rooms:{src_key}:{r}"} for r in rooms_opts[:4]],
                        [{"text": f"{r}+ חד׳", "callback_data": f"set_rooms:{src_key}:{r}"} for r in rooms_opts[4:]],
                        [{"text": f"🔙 חזרה לחיפוש {src_key}", "callback_data": f"src_view:{src_key}"}],
                    ]
                    answer_callback(cb_id)
                    edit_or_send_telegram(
                        f"🛏 <b>בחר מינימום חדרים עבור חיפוש [{src_key}]:</b>\n"
                        f"(או שלח ידנית: <code>/setrooms {src_key} 4</code>)",
                        msg_id,
                        {"inline_keyboard": kb},
                    )

                elif data.startswith("set_rooms:"):
                    _, src_key, new_rooms = data.split(":", 2)
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        src_info["api_url"] = update_url_param(src_info.get("api_url", ""), "minRooms", new_rooms)
                        src_info["url"] = update_url_param(src_info.get("url", ""), "minRooms", new_rooms)
                        save_config(cfg)
                        git_sync_state(f"config: {src_key} minRooms={new_rooms}")
                        answer_callback(cb_id, f"חדרים עודכן ל-{new_rooms}+ ✓")
                        edit_or_send_telegram(
                            build_source_detail_text(src_key, src_info),
                            msg_id,
                            single_source_keyboard(src_key, src_info),
                        )

                elif data.startswith("del_confirm:"):
                    src_key = data.split(":", 1)[1]
                    answer_callback(cb_id)
                    edit_or_send_telegram(
                        f"⚠️ <b>האם אתה בטוח שברצונך למחוק את חיפוש [{src_key}]?</b>",
                        msg_id,
                        {
                            "inline_keyboard": [
                                [
                                    {"text": "✅ כן, מחק", "callback_data": f"del_src:{src_key}"},
                                    {"text": "❌ ביטול", "callback_data": f"src_view:{src_key}"},
                                ]
                            ]
                        },
                    )

                elif data.startswith("del_src:"):
                    src_key = data.split(":", 1)[1]
                    if src_key in cfg.get("sources", {}):
                        del cfg["sources"][src_key]
                        save_config(cfg)
                        git_sync_state(f"config: delete source {src_key}")
                    answer_callback(cb_id, f"חיפוש {src_key} נמחק")
                    edit_or_send_telegram(
                        "⚙️ <b>ניהול חיפושים</b>\nהחיפוש נמחק בהצלחה.",
                        msg_id,
                        sources_menu_keyboard(cfg),
                    )
                continue

            # 2) Handle Text Commands & Pasted Yad2 Links
            msg = upd.get("message") or {}
            chat_id = (msg.get("chat") or {}).get("id")
            text = (msg.get("text") or "").strip()
            if not text or chat_id != TELEGRAM_CHAT_ID:
                continue

            log.info(f"Telegram command received: {text}")
            cmd_parts = text.split(maxsplit=1)
            cmd = cmd_parts[0].lower().split("@")[0]
            arg = cmd_parts[1] if len(cmd_parts) > 1 else ""

            if cmd in ("/start", "/menu", "/help"):
                send_telegram(build_status_text(cfg), reply_markup=main_menu_keyboard(cfg))

            elif cmd == "/status":
                send_telegram(build_status_text(cfg), reply_markup=main_menu_keyboard(cfg))

            elif cmd in ("/scan", "/scan_now"):
                send_telegram("🔍 <b>מבצע סריקת API מול יד 2 עכשיו…</b>")
                run_monitor_scan(notify_even_if_no_changes=True)

            elif cmd == "/summary":
                send_daily_summary(only_source=arg.strip().upper() if arg.strip() else None)

            elif cmd == "/searches":
                send_telegram(
                    "⚙️ <b>ניהול חיפושים</b>\nבחר חיפוש לעריכה:",
                    reply_markup=sources_menu_keyboard(cfg),
                )

            elif cmd == "/pause":
                cfg["paused"] = True
                save_config(cfg)
                git_sync_state("config: paused=True")
                send_telegram("⏸ <b>המוניטור הושהה.</b> לא יבוצעו סריקות מתוזמנות עד להפעלה מחדש.", reply_markup=main_menu_keyboard(cfg))

            elif cmd == "/resume":
                cfg["paused"] = False
                save_config(cfg)
                git_sync_state("config: paused=False")
                send_telegram("▶️ <b>המוניטור הופעל מחדש!</b>", reply_markup=main_menu_keyboard(cfg))

            elif cmd == "/setprice":
                parts = arg.split()
                if len(parts) == 2 and parts[0].upper() in cfg.get("sources", {}) and parts[1].isdigit():
                    sk, pr = parts[0].upper(), parts[1]
                    src_info = cfg["sources"][sk]
                    src_info["api_url"] = update_url_param(src_info.get("api_url", ""), "maxPrice", pr)
                    src_info["url"] = update_url_param(src_info.get("url", ""), "maxPrice", pr)
                    save_config(cfg)
                    git_sync_state(f"config: {sk} maxPrice={pr}")
                    send_telegram(
                        build_source_detail_text(sk, src_info),
                        reply_markup=single_source_keyboard(sk, src_info),
                    )
                else:
                    send_telegram("שימוש: <code>/setprice A 11000</code>")

            elif cmd == "/setrooms":
                parts = arg.split()
                if len(parts) == 2 and parts[0].upper() in cfg.get("sources", {}):
                    sk, rm = parts[0].upper(), parts[1]
                    src_info = cfg["sources"][sk]
                    src_info["api_url"] = update_url_param(src_info.get("api_url", ""), "minRooms", rm)
                    src_info["url"] = update_url_param(src_info.get("url", ""), "minRooms", rm)
                    save_config(cfg)
                    git_sync_state(f"config: {sk} minRooms={rm}")
                    send_telegram(
                        build_source_detail_text(sk, src_info),
                        reply_markup=single_source_keyboard(sk, src_info),
                    )
                else:
                    send_telegram("שימוש: <code>/setrooms A 4</code>")

            elif cmd == "/add":
                reply = handle_add_source_command(cfg, arg)
                send_telegram(reply, reply_markup=main_menu_keyboard(cfg))

            elif "yad2.co.il" in text:
                # User pasted a Yad2 link directly into chat!
                reply = handle_add_source_command(cfg, text)
                send_telegram(reply, reply_markup=main_menu_keyboard(cfg))

            else:
                send_telegram(
                    "🤖 לא זיהיתי את הפקודה. הנה לוח הבקרה:",
                    reply_markup=main_menu_keyboard(cfg),
                )

        if listen_seconds <= 0 or time.time() >= deadline:
            break

    if changed_any:
        git_sync_state(f"bot-config: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}")
    return changed_any


# ─────────────────────────────────────────────
# CLI ENTRYPOINT
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Yad2 100% API Monitor & Telegram Control Bot")
    parser.add_argument("--summary", action="store_true", help="Send active listings summary")
    parser.add_argument("--send-menu", action="store_true", help="Send interactive control menu to Telegram")
    parser.add_argument("--bot-only", action="store_true", help="Only process Telegram commands/buttons without scheduled scan")
    parser.add_argument("--listen-seconds", type=int, default=0, help="Keep listening for interactive Telegram button clicks for N seconds")
    parser.add_argument("--force-notify", action="store_true", help="Send Telegram scan report even if 0 changes")
    args = parser.parse_args()

    register_bot_commands()

    # Always process pending Telegram commands first (e.g., if user paused or changed price)
    process_telegram_updates(listen_seconds=0)

    cfg = load_config()
    if args.send_menu or not cfg.get("welcome_menu_sent"):
        cfg["welcome_menu_sent"] = True
        save_config(cfg)
        send_telegram(build_status_text(cfg), reply_markup=main_menu_keyboard(cfg))

    if args.summary:
        send_daily_summary()
    elif not args.bot_only:
        run_monitor_scan(notify_even_if_no_changes=args.force_notify)

    listen_sec = args.listen_seconds
    if listen_sec == 0 and os.environ.get("GITHUB_ACTIONS") == "true" and not args.summary:
        listen_sec = int(cfg.get("listen_seconds", 90))

    if listen_sec > 0:
        log.info(f"Listening for interactive Telegram commands for {listen_sec}s…")
        process_telegram_updates(listen_seconds=listen_sec)


if __name__ == "__main__":
    main()
