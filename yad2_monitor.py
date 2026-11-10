#!/usr/bin/env python3
"""
yad2_monitor.py — 100% API-Based Yad2 Monitor, Market Price Analyzer & Telegram Bot
===================================================================================
- 100% API scraping via gw.yad2.co.il (Real Estate + Vehicles/Cars) using curl_cffi
  TLS impersonation (chrome131 / chrome124 / safari17_0 / edge101).
- Two modes per search source:
  1. "alert" (Buyer/Renter): Alerts on new listings & price/field updates.
  2. "market" (Seller/Landlord — Car Sale / Apartment Rent or Sale):
     Analyzes competing listings (private vs commercial, min/25%/median/average/75%/max),
     recommends optimal pricing (fast sale / market / premium), and tracks competitor
     price drops & sold/removed listings.
- Full interactive Telegram Bot control with step-by-step conversational input
  (awaiting_input state machine) and 24/7 continuous cloud long-polling.
"""

import argparse
import hashlib
import html
import json
import logging
import os
import statistics
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

try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Jerusalem")  # handles winter (UTC+2) / summer (UTC+3) automatically
except Exception:
    IST = timezone(timedelta(hours=2))

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
    "listen_seconds": 1500,
    "last_update_id": 0,
    "awaiting_input": None,
    "sources": {
        "A": {
            "enabled": True,
            "category": "realestate",
            "mode": "alert",
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
            "category": "realestate",
            "mode": "alert",
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
    for src_info in cfg["sources"].values():
        src_info.setdefault("enabled", True)
        src_info.setdefault("mode", "alert")
        api_u = src_info.get("api_url", "") or src_info.get("url", "")
        src_info.setdefault("category", "vehicles" if "vehicles" in api_u else "realestate")
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
    if not url:
        return url
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    if value is None or value == "" or str(value).lower() == "none":
        qs.pop(param, None)
    else:
        qs[param] = [str(value)]
    pairs = []
    for k, vals in qs.items():
        v = vals[-1] if vals else ""
        pairs.append((k, v))
    new_query = urlencode(pairs, safe=",-")
    return urlunparse(parsed._replace(query=new_query))


def get_url_param(url: str, param: str) -> str:
    if not url:
        return ""
    qs = parse_qs(urlparse(url).query)
    vals = qs.get(param)
    return vals[0] if vals else ""


def detect_url_category(url: str) -> str:
    u = url.lower()
    if "vehicles" in u or "cars" in u or "motorcycles" in u:
        return "vehicles"
    return "realestate"


def yad2_web_url_to_api_url(web_url: str) -> str:
    """
    Convert any standard Yad2 URL (Real Estate rent/forsale OR Vehicles cars)
    into a valid gw.yad2.co.il API URL.
    """
    web_url = web_url.strip()
    if "gw.yad2.co.il" in web_url:
        return web_url

    parsed = urlparse(web_url)
    path_parts = [p for p in parsed.path.split("/") if p]

    # Case 1: Vehicles (e.g. https://www.yad2.co.il/vehicles/cars?manufacturer=29&model=10361&year=2019-2023)
    if "vehicles" in path_parts:
        sub = "cars"
        for p in path_parts:
            if p in ("cars", "motorcycles", "commercial-cars", "scooters"):
                sub = p
                break
        qs = parse_qs(parsed.query, keep_blank_values=True)
        api_params = [(k, vals[0]) for k, vals in qs.items() if vals]
        query_str = urlencode(api_params, safe=",-")
        return f"https://gw.yad2.co.il/vehicles-feed/{sub}?{query_str}" if query_str else f"https://gw.yad2.co.il/vehicles-feed/{sub}"

    # Case 2: Real Estate (rent or forsale)
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
            region = "1"
        else:
            region = "3"

    api_params: List[Tuple[str, str]] = [("region", region)]
    skip_keys = {"region", "zoom", "bBox"}
    for k, vals in qs.items():
        if k in skip_keys or not vals:
            continue
        api_params.append((k, vals[0]))

    query_str = urlencode(api_params, safe=",-")
    return f"https://gw.yad2.co.il/realestate-feed/{feed_type}/map?{query_str}"


# ─────────────────────────────────────────────
# YAD2 LOCATION, NEIGHBORHOOD & VEHICLE CATALOG
# ─────────────────────────────────────────────

POPULAR_CITIES = [
    {"name": "חולון", "city": "6600", "area": "11", "region": "3"},
    {"name": "תל אביב יפו", "city": "5000", "area": "1", "region": "3"},
    {"name": "ראשון לציון", "city": "8300", "area": "9", "region": "1"},
    {"name": "רמת גן", "city": "8600", "area": "3", "region": "3"},
    {"name": "גבעתיים", "city": "6300", "area": "3", "region": "3"},
    {"name": "בת ים", "city": "6200", "area": "11", "region": "3"},
    {"name": "באר יעקב", "city": "2530", "area": "9", "region": "1"},
    {"name": "הרצליה", "city": "6400", "area": "18", "region": "1"},
    {"name": "פתח תקווה", "city": "7900", "area": "4", "region": "1"},
    {"name": "נתניה", "city": "7400", "area": "17", "region": "1"},
    {"name": "רעננה", "city": "8700", "area": "42", "region": "1"},
    {"name": "כפר סבא", "city": "6900", "area": "42", "region": "1"},
    {"name": "רחובות", "city": "8400", "area": "12", "region": "1"},
    {"name": "נס ציונה", "city": "7200", "area": "12", "region": "1"},
    {"name": "ירושלים", "city": "3000", "area": "7", "region": "2"},
    {"name": "חיפה", "city": "4000", "area": "5", "region": "5"},
]

POPULAR_CAR_MFGS = [
    {"name": "מאזדה", "id": "27"},
    {"name": "טויוטה", "id": "19"},
    {"name": "יונדאי", "id": "21"},
    {"name": "קיה", "id": "48"},
    {"name": "סקודה", "id": "40"},
    {"name": "מיני", "id": "29"},
    {"name": "הונדה", "id": "17"},
    {"name": "סוזוקי", "id": "38"},
    {"name": "ניסאן", "id": "32"},
    {"name": "פולקסווגן", "id": "42"},
    {"name": "מיצובישי", "id": "28"},
    {"name": "טסלה", "id": "221"},
    {"name": "BYD / בי.ווי.די", "id": "312"},
    {"name": "ב.מ.וו", "id": "7"},
    {"name": "מרצדס", "id": "30"},
    {"name": "אאודי", "id": "1"},
]

APT_FEATURE_TOGGLES = [
    ("parking", "🅿️ חניה"),
    ("shelter", "🛡 ממ״ד"),
    ("elevator", "🛗 מעלית"),
    ("balcony", "☀️ מרפסת"),
    ("airConditioner", "❄️ מיזוג"),
    ("renovated", "✨ משופצת"),
    ("pets", "🐾 חיות מחמד"),
    ("furniture", "🛋 מרוהטת"),
    ("storage", "📦 מחסן"),
    ("longTerm", "📅 לטווח ארוך"),
    ("bars", "🪟 סורגים"),
    ("accessibility", "♿ גישה לנכים"),
]

_YAD2_CATALOG: Dict[str, Any] = {
    "loaded": False,
    "cities_by_id": {},       # cityId -> {"name": str, "breadcrumbs": list}
    "cities_by_name": {},     # cityName -> cityId
    "hoods_by_id": {},        # hoodId -> {"id": str, "name": str, "city_name": str}
    "hoods_by_city_name": {}, # cityName -> [{"id": str, "name": str, "city_name": str}, ...]
    "mfg_by_id": {},          # mfgId -> name
    "models_by_mfg": {},      # mfgId -> [{"id": str, "name": str}, ...]
}


def _cffi_get_json(url: str, params: Optional[dict] = None) -> Optional[dict]:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin": "https://www.yad2.co.il",
        "Referer": "https://www.yad2.co.il/realestate/rent",
    }
    if HAS_CURL_CFFI:
        for profile in IMPERSONATE_PROFILES[:2]:
            try:
                r = cffi_requests.get(url, params=params, headers=headers, impersonate=profile, timeout=12, verify=False)
                if r.status_code == 200 and "json" in r.headers.get("content-type", "").lower():
                    return r.json()
            except Exception:
                pass
    try:
        r = requests.get(url, params=params, headers=headers, timeout=12, verify=False)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def ensure_yad2_catalog():
    if _YAD2_CATALOG["loaded"]:
        return
    # Pre-seed with popular cities & car manufacturers so fallback is always instant
    for pc in POPULAR_CITIES:
        _YAD2_CATALOG["cities_by_id"][pc["city"]] = {"name": pc["name"], "breadcrumbs": []}
        _YAD2_CATALOG["cities_by_name"][pc["name"]] = pc["city"]
    for pm in POPULAR_CAR_MFGS:
        _YAD2_CATALOG["mfg_by_id"][pm["id"]] = pm["name"]

    data = _cffi_get_json("https://gw.yad2.co.il/search-options/realestate/rent?fields=city,neighborhood")
    if data and isinstance(data.get("data"), dict):
        raw = data["data"]
        for c in raw.get("city") or []:
            cid = str(c.get("value") or "").strip()
            cname = str(c.get("text") or "").strip()
            if cid and cname:
                _YAD2_CATALOG["cities_by_id"][cid] = {"name": cname, "breadcrumbs": c.get("breadcrumbs") or []}
                _YAD2_CATALOG["cities_by_name"][cname] = cid
        for h in raw.get("neighborhood") or []:
            hid = str(h.get("value") or "").strip()
            hname = str(h.get("text") or "").strip()
            bcs = h.get("breadcrumbs") or []
            city_name = str(bcs[-1]).strip() if bcs else ""
            if hid and hname:
                info = {"id": hid, "name": hname, "city_name": city_name}
                _YAD2_CATALOG["hoods_by_id"][hid] = info
                if city_name:
                    _YAD2_CATALOG["hoods_by_city_name"].setdefault(city_name, []).append(info)
        for cname in _YAD2_CATALOG["hoods_by_city_name"]:
            _YAD2_CATALOG["hoods_by_city_name"][cname].sort(key=lambda x: x["name"])

    vdata = _cffi_get_json("https://gw.yad2.co.il/search-options/vehicles/cars?fields=manufacturer")
    if vdata and isinstance(vdata.get("data"), dict):
        for m in vdata["data"].get("manufacturer") or []:
            mid = str(m.get("value") or "").strip()
            mname = str(m.get("text") or "").strip()
            if mid and mname:
                _YAD2_CATALOG["mfg_by_id"][mid] = mname

    _YAD2_CATALOG["loaded"] = True


def get_city_name(city_id: str) -> str:
    if not city_id:
        return ""
    ensure_yad2_catalog()
    info = _YAD2_CATALOG["cities_by_id"].get(str(city_id))
    return info["name"] if info else str(city_id)


def get_hood_info(hood_id: str) -> Dict[str, str]:
    ensure_yad2_catalog()
    return _YAD2_CATALOG["hoods_by_id"].get(
        str(hood_id),
        {"id": str(hood_id), "name": f"שכונה {hood_id}", "city_name": ""},
    )


def get_city_neighborhoods(city_id_or_name: str) -> List[Dict[str, str]]:
    ensure_yad2_catalog()
    cname = get_city_name(city_id_or_name) if city_id_or_name in _YAD2_CATALOG["cities_by_id"] else city_id_or_name
    return _YAD2_CATALOG["hoods_by_city_name"].get(cname, [])


def get_selected_hood_ids(url: str) -> List[str]:
    if not url:
        return []
    single = get_url_param(url, "neighborhood")
    multi = get_url_param(url, "multiNeighborhood")
    ids: List[str] = []
    for raw in [single, multi]:
        if raw:
            for part in raw.split(","):
                p = part.strip()
                if p and p not in ids:
                    ids.append(p)
    return ids


def set_selected_hood_ids(url: str, hood_ids: List[str]) -> str:
    if not url:
        return url
    clean_ids = []
    for h in hood_ids:
        hs = str(h).strip()
        if hs and hs not in clean_ids:
            clean_ids.append(hs)
    if len(clean_ids) == 0:
        u = update_url_param(url, "neighborhood", None)
        return update_url_param(u, "multiNeighborhood", None)
    elif len(clean_ids) == 1:
        u = update_url_param(url, "multiNeighborhood", None)
        return update_url_param(u, "neighborhood", clean_ids[0])
    else:
        u = update_url_param(url, "neighborhood", None)
        return update_url_param(u, "multiNeighborhood", ",".join(clean_ids))


def search_yad2_locations(query: str) -> Dict[str, list]:
    """Query Yad2 live address autocomplete for cities & neighborhoods matching Hebrew text."""
    q = query.strip()
    if not q:
        return {"cities": [], "hoods": []}
    res = _cffi_get_json("https://gw.yad2.co.il/address-autocomplete/realestate/v2", params={"text": q})
    cities = (res or {}).get("cities") or []
    hoods = (res or {}).get("hoods") or []

    # Also supplement from local catalog if autocomplete returned few neighborhood matches
    ensure_yad2_catalog()
    seen_hoods = {str(h.get("hoodId")) for h in hoods}
    for hid, hinfo in _YAD2_CATALOG["hoods_by_id"].items():
        if hid in seen_hoods:
            continue
        if q in hinfo["name"] or (hinfo["city_name"] and q == hinfo["city_name"]):
            cid = _YAD2_CATALOG["cities_by_name"].get(hinfo["city_name"], "")
            hoods.append({
                "fullTitleText": f"{hinfo['name']}, {hinfo['city_name']}" if hinfo["city_name"] else hinfo["name"],
                "hoodId": hid,
                "cityId": cid,
                "areaId": "",
                "regionId": "",
            })
            if len(hoods) >= 20:
                break
    return {"cities": cities[:8], "hoods": hoods[:14]}


def get_car_models(mfg_id: str) -> List[Dict[str, str]]:
    ensure_yad2_catalog()
    if mfg_id in _YAD2_CATALOG["models_by_mfg"]:
        return _YAD2_CATALOG["models_by_mfg"][mfg_id]
    data = _cffi_get_json(f"https://gw.yad2.co.il/search-options/vehicles/cars?manufacturer={mfg_id}&fields=model")
    models = []
    if data and isinstance(data.get("data"), dict):
        for m in data["data"].get("model") or []:
            mid = str(m.get("value") or "").strip()
            mname = str(m.get("text") or "").strip()
            if mid and mname:
                models.append({"id": mid, "name": mname})
    _YAD2_CATALOG["models_by_mfg"][mfg_id] = models
    return models


def get_vehicle_summary(api_url: str) -> str:
    ensure_yad2_catalog()
    mfg_id = get_url_param(api_url, "manufacturer")
    mdl_id = get_url_param(api_url, "model")
    mfg_name = _YAD2_CATALOG["mfg_by_id"].get(mfg_id, f"יצרן {mfg_id}" if mfg_id else "כל היצרנים")
    mdl_name = ""
    if mfg_id and mdl_id:
        for m in get_car_models(mfg_id):
            if m["id"] == mdl_id:
                mdl_name = m["name"]
                break
        if not mdl_name:
            mdl_name = f"דגם {mdl_id}"
    return f"{mfg_name} {mdl_name}".strip()


def format_location_summary(api_url: str) -> str:
    """Return a friendly Hebrew description of the City & Neighborhoods configured in api_url."""
    city_id = get_url_param(api_url, "city")
    city_name = get_city_name(city_id) if city_id else ""
    hood_ids = get_selected_hood_ids(api_url)
    if hood_ids:
        hood_labels = []
        for hid in hood_ids:
            hinfo = get_hood_info(hid)
            if hinfo["city_name"] and hinfo["city_name"] != city_name:
                hood_labels.append(f"{hinfo['name']} ({hinfo['city_name']})")
            else:
                hood_labels.append(hinfo["name"])
        prefix = f"עיר: {city_name} | " if city_name else ""
        return f"{prefix}שכונות ({len(hood_ids)}): {', '.join(hood_labels)}"
    if city_name:
        return f"כל העיר {city_name} (ללא הגבלת שכונות)"
    region = get_url_param(api_url, "region")
    region_names = {"1": "מרכז והשרון", "2": "ירושלים והסביבה", "3": "תל אביב והסביבה", "4": "צפון והעמקים", "5": "חיפה והחוף", "6": "דרום", "7": "השרון"}
    return f"אזור: {region_names.get(region, 'כל הארץ')}"


def format_active_apt_features(api_url: str) -> str:
    active = []
    for param, label in APT_FEATURE_TOGGLES:
        if get_url_param(api_url, param) == "1":
            active.append(label)
    min_sqm = get_url_param(api_url, "minSquareMeter")
    if min_sqm:
        active.append(f"📐 {min_sqm}+ מ״ר")
    min_fl = get_url_param(api_url, "minFloor")
    max_fl = get_url_param(api_url, "maxFloor")
    if min_fl or max_fl:
        active.append(f"🏢 קומה {min_fl or '0'}–{max_fl or 'הכל'}")
    prop_grp = get_url_param(api_url, "propertyGroup")
    prop_id = get_url_param(api_url, "property")
    prop_map = {"1": "דירה", "3": "דירת גן", "6": "גג/פנטהאוז", "5": "בית פרטי/קוטג׳"}
    if prop_id in prop_map:
        active.append(f"🏠 {prop_map[prop_id]}")
    elif prop_grp == "apartments":
        active.append("🏠 דירות בלבד")
    elif prop_grp == "houses":
        active.append("🏠 בתים פרטיים")
    return ", ".join(active) if active else "ללא סינון מיוחד (הכל)"



# ─────────────────────────────────────────────
# TELEGRAM API & CHUNKED SENDER
# ─────────────────────────────────────────────

def write_failed_alert(message: str):
    ts = now_ist()
    with open(FAILED_ALERTS_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {message}\n")


def split_telegram_text(text: str, max_len: int = 3800) -> List[str]:
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
            # If message is identical, Telegram returns 400 "message is not modified" — ignore!
            if resp.status_code == 400 and "message is not modified" in resp.text:
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
    commands = [
        {"command": "menu", "description": "🎛 תפריט שליטה ראשי (כפתורים)"},
        {"command": "searches", "description": "⚙️ הגדרת חיפושים (מחיר/חדרים/חניה/ממ״ד/קישור)"},
        {"command": "market", "description": "📈 מדד מחיר שוק (מכירת רכב / השכרת דירה)"},
        {"command": "scan", "description": "🔍 סריקה מיידית של כל החיפושים"},
        {"command": "summary", "description": "📋 סיכום כל המודעות הפעילות כרגע"},
        {"command": "status", "description": "📊 סטטוס המערכת והחיפושים"},
        {"command": "add", "description": "➕ הוספת חיפוש או מדד מחיר חדש"},
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
# 100% API SCRAPER (REAL ESTATE + VEHICLES)
# ─────────────────────────────────────────────

def make_hash(listing: dict) -> str:
    key = "|".join(
        str(listing.get(f, ""))
        for f in ["id", "price", "rooms", "floor", "address", "description"]
    )
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def normalize_realestate_listing(raw: dict) -> Optional[dict]:
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
            "category": "realestate",
            "price": price,
            "rooms": rooms,
            "floor": floor,
            "sqm": sqm,
            "prop_type": prop_type,
            "tags": tags,
            "address": address or f"מודעה {item_id}",
            "description": description,
            "seller_type": "private",
            "url": url,
        }
        listing["hash"] = make_hash(listing)
        return listing
    except Exception as e:
        log.debug(f"normalize_realestate_listing error: {e}")
        return None


def normalize_vehicle_listing(raw: dict) -> Optional[dict]:
    """Normalize a car/vehicle ad from gw.yad2.co.il/vehicles-feed/cars."""
    try:
        item_id = str(raw.get("orderId") or raw.get("token") or raw.get("id") or "")
        if not item_id or item_id in ("0", "undefined", "null"):
            return None

        price_raw = raw.get("price") or 0
        price_digits = "".join(ch for ch in str(price_raw) if ch.isdigit())
        price = int(price_digits) if price_digits else 0

        mfg = (raw.get("manufacturer") or {}).get("text") or ""
        model = (raw.get("model") or {}).get("text") or ""
        sub_model = (raw.get("subModel") or {}).get("text") or ""
        year = str((raw.get("vehicleDates") or {}).get("yearOfProduction") or "")
        hand_text = (raw.get("hand") or {}).get("text") or ""
        hand_id = (raw.get("hand") or {}).get("id") or ""
        engine_type = (raw.get("engineType") or {}).get("text") or ""
        orig_owner = (raw.get("originalOwner") or {}).get("text") or ""
        area_text = ((raw.get("address") or {}).get("area") or {}).get("text") or ""

        ad_type = raw.get("adType") or "private"
        agency_name = (raw.get("customer") or {}).get("agencyName") or ""
        seller_label = f"סוכנות ({agency_name})" if agency_name else ("סוחר/סוכנות" if ad_type != "private" else "מוכר פרטי")

        tags_list = [t.get("name", "") for t in (raw.get("tags") or []) if isinstance(t, dict) and t.get("name")]
        tags = ", ".join(tags_list[:4])

        title = f"{mfg} {model}".strip()
        if year:
            title += f" ({year})"
        if area_text:
            title += f" — {area_text}"

        desc_parts = [p for p in [sub_model, engine_type, f"בעלות מקורית: {orig_owner}" if orig_owner else "", seller_label] if p]
        description = " | ".join(desc_parts)[:140]

        link_token = raw.get("token") or item_id
        url = f"https://www.yad2.co.il/vehicles/item/{link_token}"

        listing = {
            "id": item_id,
            "category": "vehicles",
            "price": price,
            "rooms": year,          # stores year for vehicles
            "floor": hand_text,     # stores hand for vehicles
            "hand_num": hand_id,
            "sqm": "",
            "prop_type": seller_label,
            "seller_type": "private" if ad_type == "private" else "commercial",
            "tags": tags,
            "address": title or f"רכב {item_id}",
            "description": description,
            "url": url,
        }
        listing["hash"] = make_hash(listing)
        return listing
    except Exception as e:
        log.debug(f"normalize_vehicle_listing error: {e}")
        return None


def _parse_api_response(resp_text: str, content_type: str, label: str, category: str) -> Optional[Dict[str, dict]]:
    if "json" not in content_type.lower():
        snippet = (resp_text or "")[:180].replace("\n", " ")
        log.warning(f"[{label}] Non-JSON response ({content_type}): {snippet}")
        return None
    data = json.loads(resp_text)
    raw_data = data.get("data") or {}

    if category == "vehicles" or "ads" in raw_data:
        ads = raw_data.get("ads") or []
        listings: Dict[str, dict] = {}
        for raw in ads:
            normalized = normalize_vehicle_listing(raw)
            if normalized:
                listings[normalized["id"]] = normalized
        log.info(f"[{label}] Vehicles API returned {len(ads)} ads → {len(listings)} valid listings")
        return listings

    markers = raw_data.get("markers")
    if markers is None and isinstance(raw_data.get("feed"), dict):
        markers = raw_data["feed"].get("feed_items")
    if markers is None:
        log.warning(f"[{label}] JSON missing 'markers' key")
        return None

    listings = {}
    for raw in markers:
        normalized = normalize_realestate_listing(raw)
        if normalized:
            listings[normalized["id"]] = normalized
    log.info(f"[{label}] RealEstate API returned {len(markers)} markers → {len(listings)} valid listings")
    return listings


def _fetch_single_api_url(api_url: str, label: str, web_url: str, category: str) -> Optional[Dict[str, dict]]:
    base_headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin": "https://www.yad2.co.il",
        "Referer": web_url or ("https://www.yad2.co.il/vehicles/cars" if category == "vehicles" else "https://www.yad2.co.il/realestate/rent"),
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
    }

    if HAS_CURL_CFFI:
        for idx, profile in enumerate(IMPERSONATE_PROFILES):
            try:
                session = cffi_requests.Session(impersonate=profile)
                if idx > 0:
                    try:
                        session.get(
                            base_headers["Referer"],
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
                        r.text, r.headers.get("content-type", ""), label, category
                    )
                    if parsed is not None:
                        return parsed
                else:
                    log.warning(f"[{label}] Profile {profile} returned HTTP {r.status_code}")
            except Exception as e:
                log.warning(f"[{label}] curl_cffi ({profile}) error: {e}")

    worker = os.environ.get("WORKER_PROXY", "").strip().rstrip("/")
    if worker:
        target_url = api_url.replace("https://gw.yad2.co.il", worker)
        proxy_headers = dict(base_headers)
        worker_token = os.environ.get("WORKER_TOKEN", "").strip()
        if worker_token:
            proxy_headers["X-Auth-Token"] = worker_token
        try:
            r = (
                cffi_requests.get(target_url, headers=proxy_headers, impersonate="chrome131", timeout=20, verify=False)
                if HAS_CURL_CFFI
                else requests.get(target_url, headers=proxy_headers, timeout=20, verify=False)
            )
            if r.status_code == 200:
                parsed = _parse_api_response(r.text, r.headers.get("content-type", ""), label, category)
                if parsed is not None:
                    return parsed
        except Exception as e:
            log.warning(f"[{label}] WORKER_PROXY error: {e}")

    return None


def fetch_via_api(
    api_url: str,
    label: str,
    web_url: str = "",
    category: str = "realestate",
    max_pages: int = 1,
) -> Optional[Dict[str, dict]]:
    """
    Fetch listings via Yad2 API. For vehicles in market analysis mode,
    supports fetching up to `max_pages` pages to get a full market sample.
    """
    first_page = _fetch_single_api_url(api_url, label, web_url, category)
    if first_page is None or category != "vehicles" or max_pages <= 1:
        return first_page

    combined = dict(first_page)
    for page_num in range(2, max_pages + 1):
        if len(first_page) < 35:
            break
        paged_url = update_url_param(api_url, "page", page_num)
        more = _fetch_single_api_url(paged_url, f"{label} p{page_num}", web_url, category)
        if not more:
            break
        combined.update(more)
    return combined


# ─────────────────────────────────────────────
# MARKET PRICE ANALYZER (CAR SALE / APT RENT)
# ─────────────────────────────────────────────

def compute_percentile(sorted_vals: List[int], pct: float) -> int:
    if not sorted_vals:
        return 0
    idx = (len(sorted_vals) - 1) * pct
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return int(round(sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac))


def build_market_analysis_report(
    src_key: str,
    src_info: dict,
    state: dict,
    new_items: Optional[list] = None,
    updated_items: Optional[list] = None,
    removed_items: Optional[list] = None,
) -> str:
    """
    Build a comprehensive Seller / Landlord Market Pricing Report for a car or apartment!
    Calculates median, mean, percentiles (fast sale / recommended / premium),
    private vs commercial breakdown, year/room breakdown, and recent price drops.
    """
    label = html.escape(src_info.get("label", src_key))
    category = src_info.get("category", "realestate")
    is_car = category == "vehicles"
    icon = "🚗" if is_car else "🏠"

    all_listings = list(state.get("listings", {}).values())
    last_check = state.get("lastCheck")
    active_listings = [
        l for l in all_listings
        if l.get("active") is True or (l.get("active") is None and last_check and l.get("lastSeen") == last_check)
    ]
    if not active_listings:
        active_listings = all_listings

    priced = [l for l in active_listings if isinstance(l.get("price"), int) and l["price"] > 1000]
    prices = sorted(l["price"] for l in priced)

    lines = [
        f"📈 <b>דו״ח מחיר שוק ותמחור מומלץ — [{src_key}] {label}</b>",
        f"{icon} קטגוריה: <b>{'מכירת רכב' if is_car else 'נדל״ן (השכרה/מכירה)'}</b> | <code>{now_ist()}</code>\n",
    ]

    if not prices:
        lines.append("⚠️ לא נמצאו מודעות פעילות עם מחיר מצוין כרגע בחיפוש זה.")
        return "\n".join(lines)

    n = len(prices)
    min_p = prices[0]
    p25 = compute_percentile(prices, 0.25)
    median_p = int(round(statistics.median(prices)))
    mean_p = int(round(statistics.mean(prices)))
    p60 = compute_percentile(prices, 0.60)
    p75 = compute_percentile(prices, 0.75)
    max_p = prices[-1]

    lines.append(f"📊 <b>ניתוח מחירי השוק כרגע ({n} מודעות עם מחיר מתוך {len(active_listings)} פעילות):</b>")
    lines.append(f"  • <b>מחיר חציוני (Median):</b> <b>₪{median_p:,}</b>")
    lines.append(f"  • <b>מחיר ממוצע (Average):</b> ₪{mean_p:,}")
    lines.append(f"  • <b>הכי זול בלוח:</b> ₪{min_p:,} | <b>הכי יקר:</b> ₪{max_p:,}")
    lines.append(f"  • <b>טווח האמצע (25%–75%):</b> ₪{p25:,} – ₪{p75:,}")

    # Private vs Commercial breakdown (especially important for cars!)
    if is_car:
        priv_prices = sorted(l["price"] for l in priced if l.get("seller_type") == "private")
        comm_prices = sorted(l["price"] for l in priced if l.get("seller_type") != "private")
        lines.append("\n👤 <b>פילוח מוכרים פרטיים מול סוכנויות/מגרשים:</b>")
        if priv_prices:
            priv_med = int(round(statistics.median(priv_prices)))
            lines.append(f"  • <b>מוכרים פרטיים ({len(priv_prices)} רכבים):</b> חציון <b>₪{priv_med:,}</b> (טווח ₪{priv_prices[0]:,}–₪{priv_prices[-1]:,})")
        else:
            lines.append("  • <b>מוכרים פרטיים:</b> 0 רכבים עם מחיר")
        if comm_prices:
            comm_med = int(round(statistics.median(comm_prices)))
            lines.append(f"  • <b>סוכנויות/מגרשים ({len(comm_prices)} רכבים):</b> חציון <b>₪{comm_med:,}</b> (טווח ₪{comm_prices[0]:,}–₪{comm_prices[-1]:,})")

    # Breakdown by Year (for cars) or Rooms (for apartments)
    group_key_label = "שנתון" if is_car else "חדרים"
    groups: Dict[str, List[int]] = {}
    for l in priced:
        gk = str(l.get("rooms") or "לא צוין")
        groups.setdefault(gk, []).append(l["price"])
    if len(groups) > 1:
        lines.append(f"\n📅 <b>מחיר חציוני לפי {group_key_label}:</b>")
        for gk in sorted(groups.keys()):
            g_prices = sorted(groups[gk])
            g_med = int(round(statistics.median(g_prices)))
            lines.append(f"  • <b>{gk}:</b> ₪{g_med:,} ({len(g_prices)} מודעות, ₪{g_prices[0]:,}–₪{g_prices[-1]:,})")

    # Smart Pricing Recommendations
    base_target = int(round(statistics.median([l["price"] for l in priced if l.get("seller_type") == "private"]))) if (is_car and any(l.get("seller_type") == "private" for l in priced)) else median_p
    fast_price = p25
    publish_price = p60
    premium_price = p75

    lines.append("\n🎯 <b>המלצת תמחור חכמה עבורך:</b>")
    lines.append(f"  ⚡ <b>לסגירה מהירה:</b> <b>₪{fast_price:,}</b> (זול מ-75% מהשוק)")
    lines.append(f"  ✅ <b>מחיר מומלץ לפרסום:</b> <b>₪{publish_price:,}</b> (מחיר שוק הוגן של ₪{base_target:,} + מרווח מיקוח)")
    lines.append(f"  💎 <b>מחיר פרימיום (מצב נדיר/ק״מ נמוך):</b> <b>₪{premium_price:,}</b>")

    # Market movement (new competitors, price drops, sold/removed)
    if new_items or updated_items or removed_items:
        lines.append("\n🔄 <b>תנועות אחרונות בשוק:</b>")
        if new_items:
            lines.append(f"  • ➕ <b>{len(new_items)} מתחרים חדשים</b> עלו ללוח:")
            for item in new_items[:4]:
                p_str = f"₪{item['price']:,}" if item.get("price") else "ללא מחיר"
                lines.append(f"    - {html.escape(item.get('address',''))}: <b>{p_str}</b> (<a href=\"{item.get('url','')}\">לינק</a>)")
        if updated_items:
            lines.append(f"  • 📉 <b>{len(updated_items)} שינויי מחיר/פרטים</b> אצל מתחרים:")
            for item in updated_items[:5]:
                ch = item.get("_changed", {}).get("price")
                if ch:
                    lines.append(
                        f"    - {html.escape(item.get('address',''))}: ₪{ch['old']} ← <b>₪{ch['new']}</b> (<a href=\"{item.get('url','')}\">לינק</a>)"
                    )
        if removed_items:
            lines.append(f"  • 🏁 <b>{len(removed_items)} מודעות ירדו מהלוח (נמכרו/הושכרו):</b>")
            for item in removed_items[:4]:
                p_str = f"₪{item['price']:,}" if item.get("price") else "ללא מחיר"
                lines.append(f"    - {html.escape(item.get('address',''))} (מחיר אחרון: <b>{p_str}</b>)")

    # Top 5 cheapest active competitors
    lines.append("\n🏆 <b>5 המתחרים הכי זולים כרגע בלוח:</b>")
    for item in sorted(priced, key=lambda x: x["price"])[:5]:
        extra = f" | {item.get('floor')}" if item.get("floor") else ""
        lines.append(
            f"  • <b>₪{item['price']:,}</b> — {html.escape(item.get('address',''))}{html.escape(extra)} "
            f'(<a href="{item.get("url","")}">מודעה</a>)'
        )

    return "\n".join(lines)


# ─────────────────────────────────────────────
# CHANGE DETECTION & FORMATTING
# ─────────────────────────────────────────────

FIELD_LABELS = {
    "price": "מחיר",
    "rooms": "חדרים/שנתון",
    "floor": "קומה/יד",
    "address": "כתובת/דגם",
    "description": "תיאור",
}


def detect_changes(old_listings: dict, new_listings: dict) -> Tuple[list, list, list]:
    """Returns (new_items, updated_items, removed_active_items)."""
    new_items = []
    updated_items = []
    removed_items = []

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

    for item_id, old in old_listings.items():
        if old.get("active") is True and item_id not in new_listings:
            removed_items.append(old)

    return new_items, updated_items, removed_items


def format_listing_new(listing: dict) -> str:
    is_car = listing.get("category") == "vehicles"
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
    if is_car:
        if rooms and rooms != "?":
            specs.append(f"שנת {rooms}")
        if floor and floor != "?":
            specs.append(str(floor))
        if prop_type:
            specs.append(prop_type)
        specs.append(f"<b>{price}</b>")
    else:
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
        lines.append(f"  <i>{desc[:110]}</i>")
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


def update_state(old_state: dict, new_listings: dict) -> dict:
    ts = now_ist()
    updated = old_state.copy()
    old_listings = updated.get("listings", {})

    for existing in old_listings.values():
        existing["active"] = False

    for item_id, listing in new_listings.items():
        if item_id in old_listings:
            merged = old_listings[item_id].copy()
            # Track price history if price changed
            old_p = merged.get("price")
            new_p = listing.get("price")
            if old_p and new_p and old_p != new_p:
                hist = merged.get("priceHistory", [])
                hist.append({"price": old_p, "date": merged.get("lastSeen", ts)})
                merged["priceHistory"] = hist[-10:]
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

def alert_footer_keyboard() -> dict:
    """Button attached to apartment/car alerts that opens a NEW menu without overwriting the alert!"""
    return {
        "inline_keyboard": [
            [
                {"text": "🎛 פתח תפריט שליטה", "callback_data": "open_menu_new"},
                {"text": "⚙️ הגדרת חיפושים", "callback_data": "open_searches_new"},
            ],
            [
                {"text": "📈 מדד מחיר שוק", "callback_data": "open_market_new"},
            ],
        ]
    }


def run_monitor_scan(
    notify_even_if_no_changes: bool = False,
    only_source: Optional[str] = None,
) -> Dict[str, Any]:
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
            continue
        if src_key in skip_env and not only_source:
            continue

        state_path = get_state_path(src_info, src_key)
        states[src_key] = load_state(state_path)

        api_url = src_info.get("api_url") or yad2_web_url_to_api_url(src_info.get("url", ""))
        category = src_info.get("category") or detect_url_category(api_url)
        max_pages = 3 if (category == "vehicles" and src_info.get("mode") == "market") else 1

        result = fetch_via_api(
            api_url,
            src_info.get("label", src_key),
            src_info.get("url", ""),
            category=category,
            max_pages=max_pages,
        )
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

    alert_results = {}
    counts = {}
    for src_key, listings in fetched.items():
        src_info = sources_cfg[src_key]
        old = states[src_key].get("listings", {})
        new_items, updated_items, removed_items = detect_changes(old, listings)
        counts[src_key] = len(listings)

        new_state = update_state(states[src_key], listings)
        save_state(get_state_path(src_info, src_key), new_state)
        states[src_key] = new_state

        if src_info.get("mode") == "market":
            # In market mode, send report when forced OR when there are price drops / new / removed competitors
            if notify_even_if_no_changes or new_items or updated_items or removed_items:
                report = build_market_analysis_report(
                    src_key, src_info, new_state, new_items, updated_items, removed_items
                )
                send_telegram(report, reply_markup=alert_footer_keyboard())
        else:
            alert_results[src_key] = {"new": new_items, "updated": updated_items}

    # Send buyer/renter alert message if any alert-mode sources had changes
    has_alert_change = any(
        alert_results[k]["new"] or alert_results[k]["updated"] for k in alert_results
    )
    if has_alert_change:
        parts = [f"🏠 <b>Yad2 Monitor</b> | {now_ist()}"]
        for src_key, diff in alert_results.items():
            if not diff["new"] and not diff["updated"]:
                continue
            src_info = sources_cfg[src_key]
            parts.append(f"\n📍 <b>[{src_key}] {html.escape(src_info.get('label', src_key))}</b>")
            if diff["new"]:
                parts.append(f"✅ <b>חדש ({len(diff['new'])}):</b>")
                for item in diff["new"]:
                    parts.append(format_listing_new(item))
            if diff["updated"]:
                parts.append(f"🔄 <b>עודכן ({len(diff['updated'])}):</b>")
                for item in diff["updated"]:
                    parts.append(format_listing_updated(item))
        summary_counts = " | ".join(f"{k}={counts.get(k, 0)}" for k in sources_cfg if k in counts)
        parts.append(f"\n📊 פעילות כרגע: {summary_counts}")
        send_telegram("\n".join(parts), reply_markup=alert_footer_keyboard())

    elif notify_even_if_no_changes and alert_results:
        summary_lines = [f"✅ <b>סריקה הושלמה בהצלחה</b> ({now_ist()})", "אין מודעות חדשות או שינויי מחיר כרגע.\n"]
        for src_key in alert_results:
            lbl = html.escape(sources_cfg[src_key].get("label", src_key))
            summary_lines.append(f"• <b>[{src_key}] {lbl}:</b> {counts.get(src_key, 0)} מודעות פעילות")
        send_telegram("\n".join(summary_lines), reply_markup=main_menu_keyboard(cfg))

    git_sync_state(f"state: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}")
    return {"status": "ok", "counts": counts}


def send_daily_summary(only_source: Optional[str] = None):
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

        # If this source is in "market" mode, send the market pricing report!
        if src_info.get("mode") == "market":
            report = build_market_analysis_report(src_key, src_info, data)
            send_telegram(report, reply_markup=alert_footer_keyboard())
            continue

        all_listings = list(data.get("listings", {}).values())
        last_check = data.get("lastCheck")
        active_listings = [
            l for l in all_listings
            if l.get("active") is True or (l.get("active") is None and last_check and l.get("lastSeen") == last_check)
        ]
        if not active_listings and all_listings:
            active_listings = sorted(all_listings, key=lambda x: x.get("lastSeen", ""), reverse=True)[:20]

        total = len(active_listings)
        label = html.escape(src_info.get("label", src_key))
        if total == 0:
            send_telegram(f"📋 <b>סיכום מודעות — [{src_key}] {label}</b>\nאין מודעות פעילות שמורות כרגע.")
            continue

        batch_size = 8
        for i in range(0, total, batch_size):
            batch = active_listings[i : i + batch_size]
            header = (
                f"📋 <b>סיכום מודעות פעילות | {ts}</b>\n"
                f"📍 <b>[{src_key}] {label}</b> — מציג {i + 1}–{min(i + len(batch), total)} מתוך {total}\n"
            )
            lines = [header] + [format_listing_new(l) for l in batch]
            send_telegram("\n".join(lines))

    send_telegram("🎛 <b>סיום סיכום</b> — בחר פעולה:", reply_markup=main_menu_keyboard(cfg))


# ─────────────────────────────────────────────
# INTERACTIVE TELEGRAM BOT MENUS & CONFIGURER
# ─────────────────────────────────────────────

def main_menu_keyboard(cfg: dict) -> dict:
    paused = cfg.get("paused", False)
    pause_btn = (
        {"text": "▶️ הפעל מוניטור (מושהה)", "callback_data": "toggle_global"}
        if paused
        else {"text": "⏸ השהה מוניטור", "callback_data": "toggle_global"}
    )
    return {
        "inline_keyboard": [
            [
                {"text": "🔍 סרוק עכשיו", "callback_data": "scan_now"},
                {"text": "⚙️ הגדרת חיפושים", "callback_data": "manage_sources"},
            ],
            [
                {"text": "📈 מדד מחיר שוק (רכב/דירה)", "callback_data": "market_menu"},
                {"text": "📋 סיכום מודעות", "callback_data": "summary_all"},
            ],
            [
                {"text": "➕ הוסף חיפוש / מעקב חדש", "callback_data": "add_wizard"},
                pause_btn,
            ],
            [
                {"text": "🔄 רענן סטטוס", "callback_data": "status"},
            ],
        ]
    }


def sources_menu_keyboard(cfg: dict) -> dict:
    rows = []
    for src_key, src_info in cfg.get("sources", {}).items():
        enabled = src_info.get("enabled", True)
        status_icon = "🟢" if enabled else "🔴"
        mode_icon = "📈" if src_info.get("mode") == "market" else ("🚗" if src_info.get("category") == "vehicles" else "🏠")
        label = src_info.get("label", src_key)
        rows.append([
            {
                "text": f"{status_icon} {mode_icon} [{src_key}] {label} — לחץ לעריכה ⚙️",
                "callback_data": f"src_view:{src_key}",
            }
        ])
    rows.append([{"text": "➕ הוסף חיפוש / מדד מחיר חדש", "callback_data": "add_wizard"}])
    rows.append([{"text": "🔙 חזרה לתפריט הראשי", "callback_data": "main_menu"}])
    return {"inline_keyboard": rows}


def market_menu_keyboard(cfg: dict) -> dict:
    rows = []
    for src_key, src_info in cfg.get("sources", {}).items():
        cat_icon = "🚗" if src_info.get("category") == "vehicles" else "🏠"
        label = src_info.get("label", src_key)
        rows.append([
            {
                "text": f"📈 נתח מחיר שוק עכשיו: [{src_key}] {cat_icon} {label}",
                "callback_data": f"src_market_now:{src_key}",
            }
        ])
    rows.append([
        {"text": "🚗 הוסף מעקב מחיר לרכב שלי", "callback_data": "add_market_car"},
        {"text": "🏠 הוסף מעקב מחיר לדירה שלי", "callback_data": "add_market_apt"},
    ])
    rows.append([{"text": "🔙 חזרה לתפריט הראשי", "callback_data": "main_menu"}])
    return {"inline_keyboard": rows}


def single_source_keyboard(src_key: str, src_info: dict) -> dict:
    enabled = src_info.get("enabled", True)
    is_car = src_info.get("category") == "vehicles"
    is_market = src_info.get("mode") == "market"
    api_url = src_info.get("api_url", "")

    toggle_text = "⏸ השהה חיפוש" if enabled else "▶️ הפעל חיפוש"
    mode_text = "🔔 עבור למצב: התרעות קונה" if is_market else "📈 עבור למצב: מדד מחיר שוק (מוכר)"

    rows = [
        [
            {"text": toggle_text, "callback_data": f"src_toggle:{src_key}"},
            {"text": "🔍 סרוק חיפוש זה עכשיו", "callback_data": f"src_scan:{src_key}"},
        ],
        [
            {"text": mode_text, "callback_data": f"src_mode_toggle:{src_key}"},
        ],
    ]

    if is_car:
        rows.append([
            {"text": "🚗 בחר יצרן ודגם רכב", "callback_data": f"car_mfg_menu:{src_key}"},
        ])
        rows.append([
            {"text": "📅 בחר שנתונים", "callback_data": f"year_menu:{src_key}"},
            {"text": "🖐 מקסימום יד", "callback_data": f"hand_menu:{src_key}"},
        ])
        rows.append([
            {"text": "🛣 מקסימום ק״מ", "callback_data": f"km_menu:{src_key}"},
            {"text": "💰 מחיר מקסימלי", "callback_data": f"price_menu:{src_key}"},
        ])
        rows.append([
            {"text": "💵 מחיר מינימלי", "callback_data": f"minprice_menu:{src_key}"},
        ])
    else:
        hood_cnt = len(get_selected_hood_ids(api_url))
        hood_btn_txt = f"🏘 עיר ושכונות ({hood_cnt} שכונות נבחרו)" if hood_cnt else "🏘 עיר ושכונות (בחר/הוסף שכונות)"
        rows.append([
            {"text": hood_btn_txt, "callback_data": f"hoods_menu:{src_key}"},
        ])
        rows.append([
            {"text": "💰 מחיר מקסימלי", "callback_data": f"price_menu:{src_key}"},
            {"text": "💵 מחיר מינימלי", "callback_data": f"minprice_menu:{src_key}"},
        ])
        rows.append([
            {"text": "🛏 מינימום חדרים", "callback_data": f"rooms_menu:{src_key}"},
            {"text": "🛏 מקסימום חדרים", "callback_data": f"maxrooms_menu:{src_key}"},
        ])
        parking_on = get_url_param(api_url, "parking") == "1"
        shelter_on = get_url_param(api_url, "shelter") == "1"
        rows.append([
            {
                "text": f"🅿️ חניה: {'✅ חובה' if parking_on else '❌ הכל'}",
                "callback_data": f"toggle_param:{src_key}:parking",
            },
            {
                "text": f"🛡 ממ״ד: {'✅ חובה' if shelter_on else '❌ הכל'}",
                "callback_data": f"toggle_param:{src_key}:shelter",
            },
        ])
        rows.append([
            {
                "text": "🏗 מאפיינים נוספים (מעלית, מרפסת, מ״ר, קומה, חיות...)",
                "callback_data": f"apt_features:{src_key}",
            }
        ])

    rows.extend([
        [
            {"text": "🏷 שנה שם חיפוש", "callback_data": f"ask_input:{src_key}:label"},
            {"text": "🔗 הדבק קישור מיד 2", "callback_data": f"ask_input:{src_key}:url"},
        ],
        [
            {"text": "📈 דו״ח מחיר שוק לחיפוש זה", "callback_data": f"src_market_now:{src_key}"},
            {"text": "📋 הצג מודעות פעילות", "callback_data": f"src_summary:{src_key}"},
        ],
        [
            {"text": "🗑 מחק חיפוש", "callback_data": f"del_confirm:{src_key}"},
            {"text": "🔙 חזרה לרשימת החיפושים", "callback_data": "manage_sources"},
        ],
    ])
    return {"inline_keyboard": rows}


def hoods_menu_keyboard(src_key: str, src_info: dict) -> dict:
    """Interactive menu for managing City & Neighborhoods of a Real Estate search."""
    ensure_yad2_catalog()
    api_url = src_info.get("api_url", "")
    city_id = get_url_param(api_url, "city")
    city_name = get_city_name(city_id) if city_id else ""
    hood_ids = get_selected_hood_ids(api_url)

    rows = []
    # 1. Show currently selected neighborhoods with 1-click remove buttons
    for hid in hood_ids:
        hinfo = get_hood_info(hid)
        c_suffix = f" ({hinfo['city_name']})" if hinfo["city_name"] else ""
        rows.append([
            {
                "text": f"✅ שכונה פעילה: {hinfo['name']}{c_suffix} — לחץ להסרה ❌",
                "callback_data": f"hood_rm:{src_key}:{hid}",
            }
        ])

    # 2. Identify cities whose neighborhood checklists we can open immediately
    candidate_cities: List[Tuple[str, str]] = []  # (city_id, city_name)
    if city_id and city_name:
        candidate_cities.append((city_id, city_name))
    for hid in hood_ids:
        hinfo = get_hood_info(hid)
        cname = hinfo.get("city_name", "")
        cid = _YAD2_CATALOG["cities_by_name"].get(cname, "")
        if cid and cname and (cid, cname) not in candidate_cities:
            candidate_cities.append((cid, cname))

    for cid, cname in candidate_cities[:4]:
        rows.append([
            {
                "text": f"📋 בחר/סמן שכונות מתוך {cname} (רשימה מלאה)",
                "callback_data": f"city_hoods:{src_key}:{cid}:0",
            }
        ])

    # 3. Search any city or neighborhood by typing in Hebrew + Quick Popular Cities
    rows.append([
        {"text": "🔍 חפש עיר או שכונה בהקלדה (בעברית)", "callback_data": f"ask_input:{src_key}:search_loc"},
    ])
    rows.append([
        {"text": "🏙 בחר עיר מרשימת ערים פופולריות", "callback_data": f"pop_cities:{src_key}"},
    ])
    if hood_ids:
        rows.append([
            {"text": "🌐 חפש בכל העיר/האזור (נקה סינון שכונות)", "callback_data": f"hood_clear:{src_key}"},
        ])
    rows.append([
        {"text": f"🔙 חזרה להגדרות חיפוש [{src_key}]", "callback_data": f"src_view:{src_key}"},
    ])
    return {"inline_keyboard": rows}


def city_hoods_checklist_keyboard(src_key: str, src_info: dict, city_id: str, page: int = 0) -> dict:
    """Paginated 2-column interactive checklist of all neighborhoods in `city_id`."""
    api_url = src_info.get("api_url", "")
    selected = set(get_selected_hood_ids(api_url))
    all_hoods = get_city_neighborhoods(city_id)
    page_size = 14
    total = len(all_hoods)
    max_page = max(0, (total - 1) // page_size) if total else 0
    page = max(0, min(page, max_page))
    slice_hoods = all_hoods[page * page_size : (page + 1) * page_size]

    rows = []
    for i in range(0, len(slice_hoods), 2):
        pair = slice_hoods[i : i + 2]
        row = []
        for h in pair:
            hid = h["id"]
            mark = "✅" if hid in selected else "⬜"
            row.append({
                "text": f"{mark} {h['name']}",
                "callback_data": f"hood_chk:{src_key}:{city_id}:{page}:{hid}",
            })
        rows.append(row)

    if max_page > 0:
        nav_row = []
        if page > 0:
            nav_row.append({"text": "➡️ עמוד קודם", "callback_data": f"city_hoods:{src_key}:{city_id}:{page - 1}"})
        nav_row.append({"text": f"עמוד {page + 1}/{max_page + 1}", "callback_data": f"city_hoods:{src_key}:{city_id}:{page}"})
        if page < max_page:
            nav_row.append({"text": "⬅️ שכונות נוספות", "callback_data": f"city_hoods:{src_key}:{city_id}:{page + 1}"})
        rows.append(nav_row)

    rows.append([
        {"text": "🔍 חפש שכונה בהקלדה", "callback_data": f"ask_input:{src_key}:search_loc"},
        {"text": "🌐 כל העיר (ללא שכונות)", "callback_data": f"hood_clear:{src_key}"},
    ])
    rows.append([
        {"text": "✅ סיום וחזרה לתפריט שכונות", "callback_data": f"hoods_menu:{src_key}"},
    ])
    return {"inline_keyboard": rows}


def popular_cities_keyboard(src_key: str) -> dict:
    rows = []
    for i in range(0, len(POPULAR_CITIES), 2):
        pair = POPULAR_CITIES[i : i + 2]
        rows.append([
            {
                "text": f"🏙 {c['name']}",
                "callback_data": f"set_city:{src_key}:{c['city']}:{c['area']}:{c['region']}",
            }
            for c in pair
        ])
    rows.append([
        {"text": "🔍 הקלד שם של כל עיר/יישוב בארץ", "callback_data": f"ask_input:{src_key}:search_loc"},
    ])
    rows.append([
        {"text": "🔙 חזרה לתפריט עיר ושכונות", "callback_data": f"hoods_menu:{src_key}"},
    ])
    return {"inline_keyboard": rows}


def apt_features_keyboard(src_key: str, src_info: dict) -> dict:
    """Interactive menu for all apartment features (elevator, balcony, AC, pets, sqm, floor, type)."""
    api_url = src_info.get("api_url", "")
    is_forsale = "/forsale/" in api_url
    deal_txt = "🔄 סוג עסקה: מכירה (לחץ להחלפה להשכרה)" if is_forsale else "🔄 סוג עסקה: השכרה (לחץ להחלפה למכירה)"

    rows = []
    for i in range(0, len(APT_FEATURE_TOGGLES), 2):
        pair = APT_FEATURE_TOGGLES[i : i + 2]
        row = []
        for param, label in pair:
            on = get_url_param(api_url, param) == "1"
            row.append({
                "text": f"{label}: {'✅' if on else '❌'}",
                "callback_data": f"feat_tog:{src_key}:{param}",
            })
        rows.append(row)

    min_sqm = get_url_param(api_url, "minSquareMeter") or "הכל"
    min_fl = get_url_param(api_url, "minFloor") or "הכל"
    rows.extend([
        [
            {"text": f"📐 מינימום מ״ר ({min_sqm})", "callback_data": f"sqm_menu:{src_key}"},
            {"text": f"🏢 קומה ({min_fl})", "callback_data": f"floor_menu:{src_key}"},
        ],
        [
            {"text": "🏠 סוג נכס (דירה / גן / פנטהאוז / בית פרטי)", "callback_data": f"proptype_menu:{src_key}"},
        ],
        [
            {"text": deal_txt, "callback_data": f"deal_toggle:{src_key}"},
        ],
        [
            {"text": f"✅ חזרה להגדרות חיפוש [{src_key}]", "callback_data": f"src_view:{src_key}"},
        ],
    ])
    return {"inline_keyboard": rows}


def car_mfg_keyboard(src_key: str) -> dict:
    rows = []
    for i in range(0, len(POPULAR_CAR_MFGS), 2):
        pair = POPULAR_CAR_MFGS[i : i + 2]
        rows.append([
            {
                "text": f"🚗 {m['name']}",
                "callback_data": f"car_mfg:{src_key}:{m['id']}",
            }
            for m in pair
        ])
    rows.append([
        {"text": "🔍 חפש יצרן רכב בהקלדה (בעברית)", "callback_data": f"ask_input:{src_key}:search_car"},
    ])
    rows.append([
        {"text": f"🔙 חזרה לחיפוש [{src_key}]", "callback_data": f"src_view:{src_key}"},
    ])
    return {"inline_keyboard": rows}


def car_models_keyboard(src_key: str, mfg_id: str, page: int = 0) -> dict:
    models = get_car_models(mfg_id)
    page_size = 14
    total = len(models)
    max_page = max(0, (total - 1) // page_size) if total else 0
    page = max(0, min(page, max_page))
    slice_models = models[page * page_size : (page + 1) * page_size]

    rows = []
    for i in range(0, len(slice_models), 2):
        pair = slice_models[i : i + 2]
        rows.append([
            {
                "text": m["name"],
                "callback_data": f"car_mdl:{src_key}:{mfg_id}:{m['id']}",
            }
            for m in pair
        ])
    if max_page > 0:
        nav = []
        if page > 0:
            nav.append({"text": "➡️ הקודם", "callback_data": f"car_mfg_p:{src_key}:{mfg_id}:{page - 1}"})
        if page < max_page:
            nav.append({"text": "⬅️ דגמים נוספים", "callback_data": f"car_mfg_p:{src_key}:{mfg_id}:{page + 1}"})
        if nav:
            rows.append(nav)
    rows.append([
        {"text": "🌐 כל הדגמים של יצרן זה", "callback_data": f"car_mdl:{src_key}:{mfg_id}:all"},
    ])
    rows.append([
        {"text": "🔙 חזרה לבחירת יצרן", "callback_data": f"car_mfg_menu:{src_key}"},
    ])
    return {"inline_keyboard": rows}


def build_status_text(cfg: dict) -> str:
    paused = cfg.get("paused", False)
    global_state = "🔴 <b>מושהה (Paused)</b>" if paused else "🟢 <b>פעיל (Active)</b>"
    lines = [
        f"🎛 <b>לוח בקרה — Yad2 Cloud Monitor & Market Analyzer</b>",
        f"מצב מערכת: {global_state} | זמן: <code>{now_ist()}</code>",
        f"חיבור: <b>100% API בענן (נדל״ן + רכב)</b>\n",
        "📍 <b>חיפושים ומדדי שוק מוגדרים:</b>",
    ]
    for src_key, src_info in cfg.get("sources", {}).items():
        enabled = src_info.get("enabled", True)
        icon = "🟢 פעיל" if enabled else "🔴 מושהה"
        is_car = src_info.get("category") == "vehicles"
        is_market = src_info.get("mode") == "market"
        mode_str = "📈 מדד מחיר שוק (מוכר/משכיר)" if is_market else "🔔 התרעות מודעות (קונה/שוכר)"
        cat_icon = "🚗" if is_car else "🏠"

        label = html.escape(src_info.get("label", src_key))
        api_url = src_info.get("api_url", "")
        max_p = get_url_param(api_url, "maxPrice") or get_url_param(api_url, "price") or "ללא הגבלה"
        min_r = get_url_param(api_url, "minRooms") or get_url_param(api_url, "year") or "הכל"
        loc_or_car = get_vehicle_summary(api_url) if is_car else format_location_summary(api_url)

        st = load_state(get_state_path(src_info, src_key))
        last_check = st.get("lastCheck") or "טרם נסרק"
        all_l = list(st.get("listings", {}).values())
        active_cnt = st.get("activeCount")
        if active_cnt is None:
            active_cnt = sum(1 for l in all_l if l.get("lastSeen") == last_check) if last_check != "טרם נסרק" else len(all_l)

        active_prices = sorted(
            l["price"] for l in all_l
            if (l.get("active") is True or l.get("lastSeen") == last_check)
            and isinstance(l.get("price"), int) and l["price"] > 1000
        )
        med_str = f" | חציון: <b>₪{int(statistics.median(active_prices)):,}</b>" if active_prices else ""

        lines.append(
            f"\n{cat_icon} <b>[{src_key}] {label}</b> — {icon}\n"
            f"  🎯 מצב: {mode_str}\n"
            f"  📍 {'דגם' if is_car else 'מיקום'}: {html.escape(loc_or_car)}\n"
            f"  💰 תקרה: {max_p} | {'📅 שנתון' if is_car else '🛏 חדרים'}: {min_r}\n"
            f"  📊 פעילות כרגע: <b>{active_cnt}</b>{med_str}\n"
            f"  🕒 בדיקה אחרונה: <code>{last_check}</code>"
        )
    return "\n".join(lines)


def build_source_detail_text(src_key: str, src_info: dict) -> str:
    enabled = src_info.get("enabled", True)
    icon = "🟢 פעיל" if enabled else "🔴 מושהה"
    is_car = src_info.get("category") == "vehicles"
    is_market = src_info.get("mode") == "market"
    mode_str = "📈 מדד מחיר שוק (עוזר לתמחר מכירת רכב / השכרת דירה)" if is_market else "🔔 התרעות על מודעות חדשות ושינויי מחיר"
    label = html.escape(src_info.get("label", src_key))
    api_url = src_info.get("api_url", "")
    web_url = src_info.get("url", "")

    max_p = get_url_param(api_url, "maxPrice") or "ללא הגבלה"
    min_p = get_url_param(api_url, "minPrice") or "ללא"
    min_r = get_url_param(api_url, "minRooms") or "ללא"
    max_r = get_url_param(api_url, "maxRooms") or "ללא"
    year_p = get_url_param(api_url, "year") or "הכל"
    hand_p = get_url_param(api_url, "hand") or "הכל"
    km_p = get_url_param(api_url, "km") or "הכל"

    st = load_state(get_state_path(src_info, src_key))
    last_check = st.get("lastCheck") or "טרם נסרק"
    active_cnt = st.get("activeCount", len(st.get("listings", {})))

    lines = [
        f"⚙️ <b>הגדרת חיפוש [{src_key}] — {label}</b>\n",
        f"• <b>סטטוס:</b> {icon}",
        f"• <b>סוג:</b> {'🚗 רכב' if is_car else ('🏠 נדל״ן למכירה' if '/forsale/' in api_url else '🏠 נדל״ן להשכרה')}",
        f"• <b>מצב פעולה:</b> {mode_str}",
    ]
    if is_car:
        car_summary = html.escape(get_vehicle_summary(api_url))
        lines.append(f"• <b>יצרן ודגם:</b> <b>{car_summary}</b>")
        lines.append(f"• <b>מחיר מינימום–מקסימום:</b> ₪{min_p} – ₪{max_p}")
        lines.append(f"• <b>שנתונים:</b> {year_p} | <b>יד:</b> {hand_p} | <b>ק״מ:</b> {km_p}")
    else:
        loc_summary = html.escape(format_location_summary(api_url))
        feat_summary = html.escape(format_active_apt_features(api_url))
        lines.append(f"• <b>📍 מיקום ושכונות:</b> <b>{loc_summary}</b>")
        lines.append(f"• <b>💰 מחיר מינימום–מקסימום:</b> ₪{min_p} – ₪{max_p}")
        lines.append(f"• <b>🛏 חדרים:</b> מ-{min_r} עד {max_r}")
        lines.append(f"• <b>🏗 מאפיינים:</b> {feat_summary}")

    lines.append(f"• <b>📊 מודעות פעילות כרגע:</b> {active_cnt} (בדיקה אחרונה: <code>{last_check}</code>)")
    if web_url:
        lines.append(f'• 🔗 <a href="{html.escape(web_url)}">קישור לחיפוש ביד 2</a>')

    lines.append("\n👇 <b>לחץ על הכפתורים למטה כדי לבחור עיר, שכונות, מחיר או מאפיינים בקליק:</b>")
    return "\n".join(lines)


def create_new_source_from_url(cfg: dict, url_part: str, label_part: str = "", mode: str = "alert") -> Tuple[str, str]:
    """Create a new source (realestate or vehicles, alert or market) and test it live."""
    api_url = yad2_web_url_to_api_url(url_part)
    category = detect_url_category(url_part)

    existing_keys = set(cfg.get("sources", {}).keys())
    next_key = "C"
    for ch in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        if ch not in existing_keys:
            next_key = ch
            break
    else:
        next_key = f"S{len(existing_keys) + 1}"

    if not label_part:
        if category == "vehicles":
            yr = get_url_param(api_url, "year")
            label_part = f"רכב {next_key}" + (f" ({yr})" if yr else "")
        else:
            max_p = get_url_param(api_url, "maxPrice")
            label_part = f"חיפוש {next_key}" + (f" (עד ₪{max_p})" if max_p else "")

    max_pages = 3 if (category == "vehicles" and mode == "market") else 1
    listings = fetch_via_api(api_url, label_part, url_part, category=category, max_pages=max_pages)

    state_filename = f"state_{next_key.lower()}.json"
    cfg["sources"][next_key] = {
        "enabled": True,
        "category": category,
        "mode": mode,
        "label": label_part,
        "url": url_part,
        "api_url": api_url,
        "state_file": state_filename,
    }
    cfg["awaiting_input"] = None
    save_config(cfg)

    if listings is not None:
        initial_state = update_state({"listings": {}, "lastCheck": None}, listings)
        save_state(STATE_DIR / state_filename, initial_state)
        git_sync_state(f"config: add {mode} source {next_key} ({label_part})")

        if mode == "market":
            report = build_market_analysis_report(next_key, cfg["sources"][next_key], initial_state)
            return next_key, (
                f"✅ <b>מדד מחיר שוק חדש [{next_key}] ({html.escape(label_part)}) נוצר בהצלחה!</b>\n\n"
                + report
            )
        return next_key, (
            f"✅ <b>חיפוש חדש נוסף בהצלחה! [{next_key}]</b>\n\n"
            f"• <b>שם:</b> {html.escape(label_part)}\n"
            f"• <b>סוג:</b> {'🚗 רכב' if category == 'vehicles' else '🏠 נדל״ן'}\n"
            f"• <b>מודעות פעילות שנמצאו ב-API:</b> {len(listings)}\n"
            f"• מעכשיו תקבל התרעה על כל מודעה חדשה או עדכון מחיר!"
        )
    else:
        git_sync_state(f"config: add source {next_key}")
        return next_key, (
            f"⚠️ <b>החיפוש [{next_key}] ({html.escape(label_part)}) נשמר</b>, "
            f"אך לא נמצאו מודעות בבדיקה הראשונית. ניתן לערוך את הפרמטרים בתפריט הגדרת החיפושים."
        )


def create_blank_source(cfg: dict, category: str = "realestate", mode: str = "alert") -> str:
    """Create a new source immediately so the user can configure it via buttons without needing a URL!"""
    existing_keys = set(cfg.get("sources", {}).keys())
    next_key = "C"
    for ch in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        if ch not in existing_keys:
            next_key = ch
            break
    else:
        next_key = f"S{len(existing_keys) + 1}"

    if category == "vehicles":
        url = "https://www.yad2.co.il/vehicles/cars"
        api_url = "https://gw.yad2.co.il/vehicles-feed/cars"
        label = f"רכב {next_key}"
    else:
        url = "https://www.yad2.co.il/realestate/rent/tel-aviv-area?city=6600&area=11"
        api_url = "https://gw.yad2.co.il/realestate-feed/rent/map?region=3&area=11&city=6600"
        label = f"נדל״ן {next_key}"

    cfg["sources"][next_key] = {
        "enabled": True,
        "category": category,
        "mode": mode,
        "label": label,
        "url": url,
        "api_url": api_url,
        "state_file": f"state_{next_key.lower()}.json",
    }
    cfg["awaiting_input"] = None
    save_config(cfg)
    git_sync_state(f"config: create {mode} {category} source {next_key}")
    return next_key


def handle_conversational_input(cfg: dict, text: str) -> bool:
    """
    If the bot is waiting for user text input (e.g. searching a city/neighborhood in Hebrew,
    searching a car manufacturer, typing a price, or pasting a URL), process it immediately!
    """
    awaiting = cfg.get("awaiting_input")
    if not awaiting or not isinstance(awaiting, dict):
        return False

    action = awaiting.get("action", "")
    src_key = awaiting.get("src_key", "")
    text_clean = text.strip()

    if text_clean in ("/cancel", "ביטול"):
        cfg["awaiting_input"] = None
        save_config(cfg)
        send_telegram("❌ הפעולה בוטלה.", reply_markup=main_menu_keyboard(cfg))
        return True

    if action in ("add_alert", "add_market"):
        mode = "market" if action == "add_market" else "alert"
        if "|" in text_clean:
            lbl, url_p = [x.strip() for x in text_clean.split("|", 1)]
        else:
            lbl, url_p = "", text_clean
        if not url_p.startswith("http"):
            # If user typed a Hebrew city/neighborhood instead of a URL, create a source and search location!
            new_key = create_blank_source(cfg, category="realestate", mode=mode)
            cfg["awaiting_input"] = {"action": "search_loc", "src_key": new_key}
            save_config(cfg)
            return handle_conversational_input(cfg, text_clean)
        send_telegram("⏳ <b>מתחבר ל-API של יד 2 ומנתח את המודעות…</b>")
        new_key, reply = create_new_source_from_url(cfg, url_p, lbl, mode=mode)
        src_info = cfg["sources"].get(new_key, {})
        send_telegram(reply, reply_markup=single_source_keyboard(new_key, src_info) if src_info else main_menu_keyboard(cfg))
        return True

    src_info = cfg.get("sources", {}).get(src_key)
    if not src_info:
        cfg["awaiting_input"] = None
        save_config(cfg)
        return False

    if action == "search_loc":
        cfg["awaiting_input"] = None
        save_config(cfg)
        matches = search_yad2_locations(text_clean)
        cities = matches.get("cities", [])
        hoods = matches.get("hoods", [])
        if not cities and not hoods:
            send_telegram(
                f"⚠️ לא נמצאו ערים או שכונות עבור <b>{html.escape(text_clean)}</b>.\nנסה להקליד שם עיר או שכונה אחרת:",
                reply_markup=hoods_menu_keyboard(src_key, src_info),
            )
            return True

        kb_rows = []
        for c in cities[:5]:
            cid = str(c.get("cityId") or "")
            aid = str(c.get("areaId") or "1")
            rid = str(c.get("regionId") or "3")
            title = c.get("fullTitleText") or cid
            kb_rows.append([
                {
                    "text": f"🏙 קבע עיר: {title} (ובחר שכונות)",
                    "callback_data": f"set_city:{src_key}:{cid}:{aid}:{rid}",
                }
            ])
        for h in hoods[:10]:
            hid = str(h.get("hoodId") or "")
            cid = str(h.get("cityId") or "")
            aid = str(h.get("areaId") or "")
            rid = str(h.get("regionId") or "")
            title = h.get("fullTitleText") or hid
            kb_rows.append([
                {
                    "text": f"➕ הוסף שכונה: {title}",
                    "callback_data": f"hood_add:{src_key}:{hid}:{cid}:{aid}:{rid}",
                },
                {
                    "text": f"🎯 רק שכונה זו",
                    "callback_data": f"hood_only:{src_key}:{hid}:{cid}:{aid}:{rid}",
                },
            ])
        kb_rows.append([
            {"text": "🔍 חפש שם אחר", "callback_data": f"ask_input:{src_key}:search_loc"},
            {"text": "🔙 חזרה לתפריט שכונות", "callback_data": f"hoods_menu:{src_key}"},
        ])
        send_telegram(
            f"🔎 <b>תוצאות חיפוש ביד 2 עבור \"{html.escape(text_clean)}\":</b>\n"
            "• לחץ על <b>➕ הוסף שכונה</b> כדי לצרף אותה לרשימת השכונות בחיפוש.\n"
            "• לחץ על <b>🎯 רק שכונה זו</b> כדי לחפש רק בה.\n"
            "• לחץ על <b>🏙 קבע עיר</b> כדי לעבור לעיר ולסמן שכונות מתוך רשימה מלאה:",
            reply_markup={"inline_keyboard": kb_rows},
        )
        return True

    if action == "search_car":
        cfg["awaiting_input"] = None
        save_config(cfg)
        ensure_yad2_catalog()
        matching_mfgs = [
            (mid, mname)
            for mid, mname in _YAD2_CATALOG["mfg_by_id"].items()
            if text_clean.lower() in mname.lower()
        ]
        if not matching_mfgs:
            send_telegram(
                f"⚠️ לא נמצא יצרן רכב בשם <b>{html.escape(text_clean)}</b>. בחר מהרשימה או נסה שוב:",
                reply_markup=car_mfg_keyboard(src_key),
            )
            return True
        kb_rows = [
            [{"text": f"🚗 {mname}", "callback_data": f"car_mfg:{src_key}:{mid}"}]
            for mid, mname in matching_mfgs[:12]
        ]
        kb_rows.append([{"text": "🔙 חזרה לבחירת יצרן", "callback_data": f"car_mfg_menu:{src_key}"}])
        send_telegram(
            f"🔎 <b>בחר יצרן רכב מתוך התוצאות עבור \"{html.escape(text_clean)}\":</b>",
            reply_markup={"inline_keyboard": kb_rows},
        )
        return True

    if action == "label":
        src_info["label"] = text_clean[:60]
    elif action == "url":
        if not text_clean.startswith("http"):
            send_telegram("⚠️ אנא שלח קישור תקין מיד 2 (או /cancel לביטול).")
            return True
        src_info["url"] = text_clean
        src_info["api_url"] = yad2_web_url_to_api_url(text_clean)
        src_info["category"] = detect_url_category(text_clean)
    elif action in ("maxPrice", "minPrice", "minRooms", "maxRooms", "year", "hand", "minSquareMeter", "minFloor", "maxFloor", "km"):
        val = "" if text_clean in ("0", "ללא", "הכל", "none") else text_clean.replace(",", "").replace("₪", "").strip()
        if action == "km" and val and "-" not in val:
            val = f"-1-{val}"
        src_info["api_url"] = update_url_param(src_info.get("api_url", ""), action, val)
        src_info["url"] = update_url_param(src_info.get("url", ""), action, val)

    cfg["awaiting_input"] = None
    save_config(cfg)
    git_sync_state(f"config: update {src_key} {action}")
    send_telegram(
        f"✅ <b>ההגדרה עודכנה בהצלחה!</b>\n\n" + build_source_detail_text(src_key, src_info),
        reply_markup=single_source_keyboard(src_key, src_info),
    )
    return True


def process_telegram_updates(listen_seconds: int = 0, scan_interval_minutes: int = 20) -> bool:
    """
    Continuous cloud loop that:
    1. Responds to Telegram button clicks & text messages in <1 second via long-polling.
    2. Periodically runs the Yad2 API scan every `scan_interval_minutes` while listening.
    """
    cfg = load_config()
    offset = int(cfg.get("last_update_id", 0)) + 1
    changed_any = False
    start_ts = time.time()
    deadline = start_ts + max(0, listen_seconds)
    last_scan_ts = start_ts

    while True:
        now = time.time()
        # Run periodic scan inside the long-polling loop if enough time elapsed
        if listen_seconds > 0 and (now - last_scan_ts) >= (scan_interval_minutes * 60):
            log.info("Running scheduled periodic API scan inside cloud listener loop…")
            run_monitor_scan(notify_even_if_no_changes=False)
            last_scan_ts = time.time()

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
            if listen_seconds <= 0 or time.time() >= deadline:
                break
            time.sleep(2)
            continue

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

                # Clear any pending text prompt when user navigates via buttons
                if cfg.get("awaiting_input") and not data.startswith("ask_input:"):
                    cfg["awaiting_input"] = None
                    save_config(cfg)

                if data == "main_menu":
                    answer_callback(cb_id)
                    edit_or_send_telegram(build_status_text(cfg), msg_id, main_menu_keyboard(cfg))

                elif data == "open_menu_new":
                    answer_callback(cb_id)
                    send_telegram(build_status_text(cfg), reply_markup=main_menu_keyboard(cfg))

                elif data == "open_searches_new":
                    answer_callback(cb_id)
                    send_telegram(
                        "⚙️ <b>הגדרת חיפושים ומדדי מחיר</b>\nלחץ על חיפוש כדי לערוך מחיר, חדרים/שנתון, חניה, ממ״ד או קישור:",
                        reply_markup=sources_menu_keyboard(cfg),
                    )

                elif data == "open_market_new":
                    answer_callback(cb_id)
                    send_telegram(
                        "📈 <b>מדד מחיר שוק — מכירת רכב 🚗 / השכרת או מכירת דירה 🏠</b>\n\n"
                        "כלי זה סורק את כל המודעות המתחרות ביד 2 ומחשב עבורך:\n"
                        "• מחיר חציוני וממוצע בשוק\n"
                        "• הפרדה בין מוכרים פרטיים לסוכנויות/מגרשים\n"
                        "• מחיר מומלץ למכירה מהירה / מחיר שוק / מחיר פרימיום\n"
                        "• מעקב הורדות מחיר של מתחרים ומודעות שנמכרו!\n\n"
                        "בחר חיפוש קיים לניתוח שוק מיידי או הוסף מעקב חדש:",
                        reply_markup=market_menu_keyboard(cfg),
                    )

                elif data == "status":
                    answer_callback(cb_id, "מרענן סטטוס…")
                    edit_or_send_telegram(build_status_text(cfg), msg_id, main_menu_keyboard(cfg))

                elif data == "scan_now":
                    answer_callback(cb_id, "🔍 מתחיל סריקה ב-API…")
                    send_telegram("🔍 <b>מבצע סריקת API מול יד 2 עכשיו…</b>")
                    run_monitor_scan(notify_even_if_no_changes=True)

                elif data == "summary_all":
                    answer_callback(cb_id, "📋 שולח סיכום מודעות…")
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
                        "⚙️ <b>הגדרת חיפושים ומדדי מחיר</b>\nלחץ על חיפוש כדי לערוך מחיר, חדרים/שנתון, חניה, ממ״ד או קישור:",
                        msg_id,
                        sources_menu_keyboard(cfg),
                    )

                elif data == "market_menu":
                    answer_callback(cb_id)
                    edit_or_send_telegram(
                        "📈 <b>מדד מחיר שוק — מכירת רכב 🚗 / השכרת או מכירת דירה 🏠</b>\n\n"
                        "כלי זה סורק את כל המודעות המתחרות ביד 2 ומחשב עבורך:\n"
                        "• מחיר חציוני וממוצע בשוק\n"
                        "• הפרדה בין מוכרים פרטיים לסוכנויות/מגרשים\n"
                        "• מחיר מומלץ למכירה מהירה / מחיר שוק / מחיר פרימיום\n"
                        "• מעקב הורדות מחיר של מתחרים ומודעות שנמכרו!\n\n"
                        "בחר חיפוש קיים לניתוח שוק מיידי או הוסף מעקב חדש:",
                        msg_id,
                        market_menu_keyboard(cfg),
                    )

                elif data == "add_wizard":
                    answer_callback(cb_id)
                    edit_or_send_telegram(
                        "➕ <b>הוספת חיפוש או מדד מחיר חדש — בקלות וללא צורך בקישור!</b>\n"
                        "בחר מה ברצונך להוסיף (תוכל לבחור עיר, שכונות או דגם רכב ישירות מכפתורים):",
                        msg_id,
                        {
                            "inline_keyboard": [
                                [{"text": "🏠 חיפוש דירה חדש (בחירת עיר ושכונות בכפתורים)", "callback_data": "new_apt_alert"}],
                                [{"text": "📈 מדד מחיר שוק לדירה שלי (בחירת עיר ושכונות)", "callback_data": "new_apt_market"}],
                                [{"text": "🚗 מדד מחיר שוק / חיפוש רכב (בחירת יצרן ודגם)", "callback_data": "new_car_market"}],
                                [{"text": "🔗 הוספה מהירה באמצעות הדבקת קישור מיד 2", "callback_data": "add_alert_prompt"}],
                                [{"text": "🔙 חזרה לתפריט הראשי", "callback_data": "main_menu"}],
                            ]
                        },
                    )

                elif data in ("new_apt_alert", "new_apt_market", "add_market_apt"):
                    mode = "alert" if data == "new_apt_alert" else "market"
                    new_key = create_blank_source(cfg, category="realestate", mode=mode)
                    src_info = cfg["sources"][new_key]
                    answer_callback(cb_id, f"חיפוש [{new_key}] נוצר! בחר עיר ושכונות")
                    edit_or_send_telegram(
                        f"🏘 <b>חיפוש חדש [{new_key}] נוצר! עכשיו בחר עיר ושכונות:</b>\n"
                        "• לחץ על <b>🏙 בחר עיר מרשימת ערים פופולריות</b> או <b>🔍 חפש עיר/שכונה בהקלדה</b>:",
                        msg_id,
                        popular_cities_keyboard(new_key),
                    )

                elif data in ("new_car_market", "add_market_car"):
                    new_key = create_blank_source(cfg, category="vehicles", mode="market")
                    answer_callback(cb_id, f"מעקב רכב [{new_key}] נוצר! בחר יצרן")
                    edit_or_send_telegram(
                        f"🚗 <b>מעקב מחיר רכב [{new_key}] נוצר! בחר את יצרן הרכב:</b>",
                        msg_id,
                        car_mfg_keyboard(new_key),
                    )

                elif data == "add_alert_prompt":
                    cfg["awaiting_input"] = {"action": "add_alert"}
                    save_config(cfg)
                    answer_callback(cb_id)
                    send_telegram(
                        "🔔 <b>הוספת חיפוש חדש (קישור או שם עיר/שכונה)</b>\n\n"
                        "שלח כאן קישור מאתר יד 2 <b>או פשוט הקלד שם עיר/שכונה בעברית</b> (למשל: <code>חולון</code> או <code>אגרובנק</code>):\n"
                        "<i>(לביטול שלח /cancel)</i>"
                    )

                elif data.startswith("hoods_menu:"):
                    src_key = data.split(":", 1)[1]
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        loc_txt = html.escape(format_location_summary(src_info.get("api_url", "")))
                        answer_callback(cb_id)
                        edit_or_send_telegram(
                            f"🏘 <b>ניהול עיר ושכונות — חיפוש [{src_key}]</b>\n\n"
                            f"📍 <b>מוגדר כרגע:</b> {loc_txt}\n\n"
                            "• לחץ על שכונה פעילה (✅) כדי להסיר אותה.\n"
                            "• לחץ על <b>📋 בחר/סמן שכונות מתוך העיר</b> כדי לסמן כמה שכונות שתרצה מתוך רשימה.\n"
                            "• או לחץ על <b>🔍 חפש עיר או שכונה בהקלדה</b> כדי להוסיף כל שכונה או עיר בארץ:",
                            msg_id,
                            hoods_menu_keyboard(src_key, src_info),
                        )

                elif data.startswith("city_hoods:"):
                    _, src_key, city_id, page_str = data.split(":", 3)
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        cname = html.escape(get_city_name(city_id))
                        selected_cnt = len(get_selected_hood_ids(src_info.get("api_url", "")))
                        answer_callback(cb_id)
                        edit_or_send_telegram(
                            f"📋 <b>בחירת שכונות ב{cname} — חיפוש [{src_key}]</b>\n"
                            f"שכונות מסומנות כרגע בחיפוש: <b>{selected_cnt}</b>\n"
                            "לחץ על כל שכונה כדי להוסיף (✅) או להסיר (⬜) אותה מהחיפוש:",
                            msg_id,
                            city_hoods_checklist_keyboard(src_key, src_info, city_id, int(page_str)),
                        )

                elif data.startswith("hood_chk:"):
                    _, src_key, city_id, page_str, hood_id = data.split(":", 4)
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        cur_hoods = get_selected_hood_ids(src_info.get("api_url", ""))
                        if hood_id in cur_hoods:
                            cur_hoods.remove(hood_id)
                            act_msg = "שכונה הוסרה ⬜"
                        else:
                            cur_hoods.append(hood_id)
                            act_msg = "שכונה נוספה ✅"
                        src_info["api_url"] = set_selected_hood_ids(src_info.get("api_url", ""), cur_hoods)
                        src_info["url"] = set_selected_hood_ids(src_info.get("url", ""), cur_hoods)
                        save_config(cfg)
                        git_sync_state(f"config: {src_key} toggle hood {hood_id}")
                        cname = html.escape(get_city_name(city_id))
                        answer_callback(cb_id, act_msg)
                        edit_or_send_telegram(
                            f"📋 <b>בחירת שכונות ב{cname} — חיפוש [{src_key}]</b>\n"
                            f"📍 <b>מוגדר כרגע:</b> {html.escape(format_location_summary(src_info['api_url']))}\n"
                            "לחץ על כל שכונה כדי להוסיף (✅) או להסיר (⬜) אותה:",
                            msg_id,
                            city_hoods_checklist_keyboard(src_key, src_info, city_id, int(page_str)),
                        )

                elif data.startswith("hood_rm:"):
                    _, src_key, hood_id = data.split(":", 2)
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        cur_hoods = [h for h in get_selected_hood_ids(src_info.get("api_url", "")) if h != hood_id]
                        src_info["api_url"] = set_selected_hood_ids(src_info.get("api_url", ""), cur_hoods)
                        src_info["url"] = set_selected_hood_ids(src_info.get("url", ""), cur_hoods)
                        save_config(cfg)
                        git_sync_state(f"config: {src_key} remove hood {hood_id}")
                        answer_callback(cb_id, "השכונה הוסרה ✓")
                        edit_or_send_telegram(
                            f"🏘 <b>ניהול עיר ושכונות — חיפוש [{src_key}]</b>\n\n"
                            f"📍 <b>מוגדר כרגע:</b> {html.escape(format_location_summary(src_info['api_url']))}",
                            msg_id,
                            hoods_menu_keyboard(src_key, src_info),
                        )

                elif data.startswith("hood_clear:"):
                    src_key = data.split(":", 1)[1]
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        src_info["api_url"] = set_selected_hood_ids(src_info.get("api_url", ""), [])
                        src_info["url"] = set_selected_hood_ids(src_info.get("url", ""), [])
                        save_config(cfg)
                        git_sync_state(f"config: {src_key} clear neighborhoods")
                        answer_callback(cb_id, "סינון שכונות נוקה (כל העיר)")
                        edit_or_send_telegram(
                            f"🏘 <b>ניהול עיר ושכונות — חיפוש [{src_key}]</b>\n\n"
                            f"📍 <b>מוגדר כרגע:</b> {html.escape(format_location_summary(src_info['api_url']))}",
                            msg_id,
                            hoods_menu_keyboard(src_key, src_info),
                        )

                elif data.startswith("pop_cities:"):
                    src_key = data.split(":", 1)[1]
                    answer_callback(cb_id)
                    edit_or_send_telegram(
                        f"🏙 <b>בחר עיר עבור חיפוש [{src_key}]</b> (או לחץ למטה להקלדת שם עיר אחרת):",
                        msg_id,
                        popular_cities_keyboard(src_key),
                    )

                elif data.startswith("set_city:"):
                    _, src_key, cid, aid, rid = data.split(":", 4)
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        for key_u in ("api_url", "url"):
                            u = src_info.get(key_u, "")
                            if rid:
                                u = update_url_param(u, "region", rid)
                            if aid:
                                u = update_url_param(u, "area", aid)
                            u = update_url_param(u, "city", cid)
                            u = set_selected_hood_ids(u, [])
                            src_info[key_u] = u
                        cname = get_city_name(cid)
                        if src_info.get("label", "").startswith(("נדל״ן ", "חיפוש ")):
                            src_info["label"] = cname
                        save_config(cfg)
                        git_sync_state(f"config: {src_key} set city {cid} ({cname})")
                        answer_callback(cb_id, f"עיר נקבעה: {cname} ✓")
                        edit_or_send_telegram(
                            f"✅ <b>העיר נקבעה ל-{html.escape(cname)}!</b>\n"
                            "כעת תוכל לסמן שכונות ספציפיות מתוך העיר (או להשאיר ללא סימון כדי לחפש בכל העיר):",
                            msg_id,
                            city_hoods_checklist_keyboard(src_key, src_info, cid, 0),
                        )

                elif data.startswith(("hood_add:", "hood_only:")):
                    act_type, src_key, hid, cid, aid, rid = data.split(":", 5)
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        cur_hoods = get_selected_hood_ids(src_info.get("api_url", ""))
                        if act_type == "hood_only":
                            new_hoods = [hid]
                        else:
                            new_hoods = cur_hoods + ([hid] if hid not in cur_hoods else [])
                        for key_u in ("api_url", "url"):
                            u = src_info.get(key_u, "")
                            if rid:
                                u = update_url_param(u, "region", rid)
                            if aid:
                                u = update_url_param(u, "area", aid)
                            if cid and (act_type == "hood_only" or len(new_hoods) == 1):
                                u = update_url_param(u, "city", cid)
                            u = set_selected_hood_ids(u, new_hoods)
                            src_info[key_u] = u
                        hinfo = get_hood_info(hid)
                        if src_info.get("label", "").startswith(("נדל״ן ", "חיפוש ")):
                            src_info["label"] = f"{hinfo['name']} ({hinfo['city_name']})" if hinfo.get("city_name") else hinfo["name"]
                        save_config(cfg)
                        git_sync_state(f"config: {src_key} {act_type} {hid}")
                        answer_callback(cb_id, f"שכונה עודכנה: {hinfo['name']} ✓")
                        edit_or_send_telegram(
                            f"✅ <b>השכונה \"{html.escape(hinfo['name'])}\" עודכנה בחיפוש [{src_key}]!</b>\n\n"
                            f"📍 <b>מוגדר כרגע:</b> {html.escape(format_location_summary(src_info['api_url']))}",
                            msg_id,
                            hoods_menu_keyboard(src_key, src_info),
                        )

                elif data.startswith("apt_features:"):
                    src_key = data.split(":", 1)[1]
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        answer_callback(cb_id)
                        edit_or_send_telegram(
                            f"🏗 <b>מאפייני דירה וסינונים מתקדמים — חיפוש [{src_key}]</b>\n"
                            f"פעיל כרגע: <b>{html.escape(format_active_apt_features(src_info.get('api_url', '')))}</b>\n\n"
                            "לחץ על כל מאפיין כדי להפעיל (✅) או לכבות (❌) אותו מיד:",
                            msg_id,
                            apt_features_keyboard(src_key, src_info),
                        )

                elif data.startswith("feat_tog:"):
                    _, src_key, param_name = data.split(":", 2)
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        cur_val = get_url_param(src_info.get("api_url", ""), param_name)
                        new_val = "" if cur_val == "1" else "1"
                        src_info["api_url"] = update_url_param(src_info.get("api_url", ""), param_name, new_val)
                        src_info["url"] = update_url_param(src_info.get("url", ""), param_name, new_val)
                        save_config(cfg)
                        git_sync_state(f"config: {src_key} {param_name}={new_val or 'off'}")
                        answer_callback(cb_id, "מאפיין עודכן ✓")
                        edit_or_send_telegram(
                            f"🏗 <b>מאפייני דירה וסינונים מתקדמים — חיפוש [{src_key}]</b>\n"
                            f"פעיל כרגע: <b>{html.escape(format_active_apt_features(src_info.get('api_url', '')))}</b>\n\n"
                            "לחץ על כל מאפיין כדי להפעיל (✅) או לכבות (❌) אותו:",
                            msg_id,
                            apt_features_keyboard(src_key, src_info),
                        )

                elif data.startswith("deal_toggle:"):
                    src_key = data.split(":", 1)[1]
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        for key_u in ("api_url", "url"):
                            u = src_info.get(key_u, "")
                            if "/forsale" in u:
                                src_info[key_u] = u.replace("/forsale", "/rent")
                            else:
                                src_info[key_u] = u.replace("/rent", "/forsale")
                        save_config(cfg)
                        git_sync_state(f"config: {src_key} toggle rent/forsale")
                        answer_callback(cb_id, "סוג עסקה הוחלף ✓")
                        edit_or_send_telegram(
                            f"🏗 <b>מאפייני דירה וסינונים מתקדמים — חיפוש [{src_key}]</b>",
                            msg_id,
                            apt_features_keyboard(src_key, src_info),
                        )

                elif data.startswith("sqm_menu:"):
                    src_key = data.split(":", 1)[1]
                    sqms = ["50", "65", "75", "85", "100", "120"]
                    kb = [
                        [{"text": f"{s}+ מ״ר", "callback_data": f"set_param:{src_key}:minSquareMeter:{s}"} for s in sqms[:3]],
                        [{"text": f"{s}+ מ״ר", "callback_data": f"set_param:{src_key}:minSquareMeter:{s}"} for s in sqms[3:]],
                        [
                            {"text": "✏️ הקלד מ״ר", "callback_data": f"ask_input:{src_key}:minSquareMeter"},
                            {"text": "♾ ללא הגבלת מ״ר", "callback_data": f"set_param:{src_key}:minSquareMeter:none"},
                        ],
                        [{"text": "🔙 חזרה למאפייני דירה", "callback_data": f"apt_features:{src_key}"}],
                    ]
                    answer_callback(cb_id)
                    edit_or_send_telegram(f"📐 <b>בחר גודל דירה מינימלי (במ״ר) עבור [{src_key}]:</b>", msg_id, {"inline_keyboard": kb})

                elif data.startswith("floor_menu:"):
                    src_key = data.split(":", 1)[1]
                    floors = ["1", "2", "3", "4"]
                    kb = [
                        [{"text": f"מקומה {f}+", "callback_data": f"set_param:{src_key}:minFloor:{f}"} for f in floors],
                        [
                            {"text": "✏️ הקלד קומה מינימלית", "callback_data": f"ask_input:{src_key}:minFloor"},
                            {"text": "✏️ הקלד קומה מקסימלית", "callback_data": f"ask_input:{src_key}:maxFloor"},
                        ],
                        [{"text": "♾ כל הקומות (בטל הגבלה)", "callback_data": f"set_param:{src_key}:minFloor:none"}],
                        [{"text": "🔙 חזרה למאפייני דירה", "callback_data": f"apt_features:{src_key}"}],
                    ]
                    answer_callback(cb_id)
                    edit_or_send_telegram(f"🏢 <b>סינון לפי קומה עבור חיפוש [{src_key}]:</b>", msg_id, {"inline_keyboard": kb})

                elif data.startswith("proptype_menu:"):
                    src_key = data.split(":", 1)[1]
                    kb = [
                        [
                            {"text": "🏢 כל סוגי הדירות", "callback_data": f"set_proptype:{src_key}:group:apartments"},
                            {"text": "🏡 בתים פרטיים / קוטג׳ים", "callback_data": f"set_proptype:{src_key}:group:houses"},
                        ],
                        [
                            {"text": "🏠 דירה רגילה בלבד", "callback_data": f"set_proptype:{src_key}:prop:1"},
                            {"text": "🌳 דירת גן בלבד", "callback_data": f"set_proptype:{src_key}:prop:3"},
                        ],
                        [
                            {"text": "🌇 גג / פנטהאוז בלבד", "callback_data": f"set_proptype:{src_key}:prop:6"},
                            {"text": "♾ הכל (ללא סינון סוג נכס)", "callback_data": f"set_proptype:{src_key}:clear:none"},
                        ],
                        [{"text": "🔙 חזרה למאפייני דירה", "callback_data": f"apt_features:{src_key}"}],
                    ]
                    answer_callback(cb_id)
                    edit_or_send_telegram(f"🏠 <b>בחר סוג נכס עבור חיפוש [{src_key}]:</b>", msg_id, {"inline_keyboard": kb})

                elif data.startswith("set_proptype:"):
                    _, src_key, kind, val = data.split(":", 3)
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        for key_u in ("api_url", "url"):
                            u = update_url_param(src_info.get(key_u, ""), "propertyGroup", None)
                            u = update_url_param(u, "property", None)
                            if kind == "group":
                                u = update_url_param(u, "propertyGroup", val)
                            elif kind == "prop":
                                u = update_url_param(u, "property", val)
                            src_info[key_u] = u
                        save_config(cfg)
                        git_sync_state(f"config: {src_key} proptype {kind}={val}")
                        answer_callback(cb_id, "סוג נכס עודכן ✓")
                        edit_or_send_telegram(
                            f"🏗 <b>מאפייני דירה — חיפוש [{src_key}]</b>\nפעיל כרגע: <b>{html.escape(format_active_apt_features(src_info['api_url']))}</b>",
                            msg_id,
                            apt_features_keyboard(src_key, src_info),
                        )

                elif data.startswith("maxrooms_menu:"):
                    src_key = data.split(":", 1)[1]
                    rooms_opts = ["3", "3.5", "4", "4.5", "5", "6"]
                    kb = [
                        [{"text": f"עד {r} חד׳", "callback_data": f"set_param:{src_key}:maxRooms:{r}"} for r in rooms_opts[:3]],
                        [{"text": f"עד {r} חד׳", "callback_data": f"set_param:{src_key}:maxRooms:{r}"} for r in rooms_opts[3:]],
                        [
                            {"text": "✏️ הקלד מקסימום חדרים", "callback_data": f"ask_input:{src_key}:maxRooms"},
                            {"text": "♾ ללא הגבלה", "callback_data": f"set_param:{src_key}:maxRooms:none"},
                        ],
                        [{"text": f"🔙 חזרה לחיפוש {src_key}", "callback_data": f"src_view:{src_key}"}],
                    ]
                    answer_callback(cb_id)
                    edit_or_send_telegram(f"🛏 <b>בחר מקסימום חדרים עבור חיפוש [{src_key}]:</b>", msg_id, {"inline_keyboard": kb})

                elif data.startswith("car_mfg_menu:"):
                    src_key = data.split(":", 1)[1]
                    answer_callback(cb_id)
                    edit_or_send_telegram(
                        f"🚗 <b>בחר יצרן רכב עבור [{src_key}]:</b>",
                        msg_id,
                        car_mfg_keyboard(src_key),
                    )

                elif data.startswith("car_mfg:"):
                    _, src_key, mfg_id = data.split(":", 2)
                    ensure_yad2_catalog()
                    mfg_name = _YAD2_CATALOG["mfg_by_id"].get(mfg_id, mfg_id)
                    answer_callback(cb_id)
                    edit_or_send_telegram(
                        f"🚗 <b>יצרן: {html.escape(mfg_name)} — כעת בחר דגם עבור [{src_key}]:</b>",
                        msg_id,
                        car_models_keyboard(src_key, mfg_id, 0),
                    )

                elif data.startswith("car_mfg_p:"):
                    _, src_key, mfg_id, page_str = data.split(":", 3)
                    ensure_yad2_catalog()
                    mfg_name = _YAD2_CATALOG["mfg_by_id"].get(mfg_id, mfg_id)
                    answer_callback(cb_id)
                    edit_or_send_telegram(
                        f"🚗 <b>יצרן: {html.escape(mfg_name)} — בחר דגם (עמוד {int(page_str)+1}):</b>",
                        msg_id,
                        car_models_keyboard(src_key, mfg_id, int(page_str)),
                    )

                elif data.startswith("car_mdl:"):
                    _, src_key, mfg_id, mdl_id = data.split(":", 3)
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        src_info["category"] = "vehicles"
                        for key_u in ("api_url", "url"):
                            u = src_info.get(key_u, "")
                            if "vehicles" not in u:
                                u = "https://gw.yad2.co.il/vehicles-feed/cars" if key_u == "api_url" else "https://www.yad2.co.il/vehicles/cars"
                            u = update_url_param(u, "manufacturer", mfg_id)
                            u = update_url_param(u, "model", None if mdl_id == "all" else mdl_id)
                            src_info[key_u] = u
                        car_name = get_vehicle_summary(src_info["api_url"])
                        src_info["label"] = car_name
                        save_config(cfg)
                        git_sync_state(f"config: {src_key} car {car_name}")
                        answer_callback(cb_id, f"נבחר: {car_name} ✓")
                        edit_or_send_telegram(
                            f"✅ <b>הרכב נקבע ל-{html.escape(car_name)}!</b>\n\n" + build_source_detail_text(src_key, src_info),
                            msg_id,
                            single_source_keyboard(src_key, src_info),
                        )

                elif data.startswith("year_menu:"):
                    src_key = data.split(":", 1)[1]
                    yr_opts = ["2017-2025", "2019-2025", "2020-2025", "2021-2025", "2022-2025", "2023-2026"]
                    kb = [
                        [{"text": y, "callback_data": f"set_param:{src_key}:year:{y}"} for y in yr_opts[:3]],
                        [{"text": y, "callback_data": f"set_param:{src_key}:year:{y}"} for y in yr_opts[3:]],
                        [
                            {"text": "✏️ הקלד טווח שנתונים (למשל 2020-2022)", "callback_data": f"ask_input:{src_key}:year"},
                            {"text": "♾ כל השנתונים", "callback_data": f"set_param:{src_key}:year:none"},
                        ],
                        [{"text": f"🔙 חזרה לחיפוש {src_key}", "callback_data": f"src_view:{src_key}"}],
                    ]
                    answer_callback(cb_id)
                    edit_or_send_telegram(f"📅 <b>בחר טווח שנתונים עבור רכב [{src_key}]:</b>", msg_id, {"inline_keyboard": kb})

                elif data.startswith("hand_menu:"):
                    src_key = data.split(":", 1)[1]
                    kb = [
                        [
                            {"text": "🖐 יד 1 בלבד", "callback_data": f"set_param:{src_key}:hand:0-1"},
                            {"text": "🖐 עד יד 2", "callback_data": f"set_param:{src_key}:hand:0-2"},
                        ],
                        [
                            {"text": "🖐 עד יד 3", "callback_data": f"set_param:{src_key}:hand:0-3"},
                            {"text": "♾ כל יד (ללא הגבלה)", "callback_data": f"set_param:{src_key}:hand:none"},
                        ],
                        [{"text": f"🔙 חזרה לחיפוש {src_key}", "callback_data": f"src_view:{src_key}"}],
                    ]
                    answer_callback(cb_id)
                    edit_or_send_telegram(f"🖐 <b>בחר מקסימום יד עבור רכב [{src_key}]:</b>", msg_id, {"inline_keyboard": kb})

                elif data.startswith("km_menu:"):
                    src_key = data.split(":", 1)[1]
                    kms = [("50,000", "-1-50000"), ("80,000", "-1-80000"), ("100,000", "-1-100000"), ("120,000", "-1-120000"), ("150,000", "-1-150000")]
                    kb = [
                        [{"text": f"עד {lbl} ק״מ", "callback_data": f"set_param:{src_key}:km:{val}"} for lbl, val in kms[:3]],
                        [{"text": f"עד {lbl} ק״מ", "callback_data": f"set_param:{src_key}:km:{val}"} for lbl, val in kms[3:]],
                        [
                            {"text": "✏️ הקלד מקסימום ק״מ", "callback_data": f"ask_input:{src_key}:km"},
                            {"text": "♾ ללא הגבלת ק״מ", "callback_data": f"set_param:{src_key}:km:none"},
                        ],
                        [{"text": f"🔙 חזרה לחיפוש {src_key}", "callback_data": f"src_view:{src_key}"}],
                    ]
                    answer_callback(cb_id)
                    edit_or_send_telegram(f"🛣 <b>בחר מקסימום קילומטראז׳ עבור רכב [{src_key}]:</b>", msg_id, {"inline_keyboard": kb})

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
                        answer_callback(cb_id, "סטטוס חיפוש עודכן ✓")
                        edit_or_send_telegram(
                            build_source_detail_text(src_key, src_info),
                            msg_id,
                            single_source_keyboard(src_key, src_info),
                        )

                elif data.startswith("src_mode_toggle:"):
                    src_key = data.split(":", 1)[1]
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        src_info["mode"] = "alert" if src_info.get("mode") == "market" else "market"
                        save_config(cfg)
                        git_sync_state(f"config: source {src_key} mode -> {src_info['mode']}")
                        answer_callback(cb_id, "מצב חיפוש עודכן ✓")
                        edit_or_send_telegram(
                            build_source_detail_text(src_key, src_info),
                            msg_id,
                            single_source_keyboard(src_key, src_info),
                        )

                elif data.startswith("toggle_param:"):
                    _, src_key, param_name = data.split(":", 2)
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        cur_val = get_url_param(src_info.get("api_url", ""), param_name)
                        new_val = "" if cur_val == "1" else "1"
                        src_info["api_url"] = update_url_param(src_info.get("api_url", ""), param_name, new_val)
                        src_info["url"] = update_url_param(src_info.get("url", ""), param_name, new_val)
                        save_config(cfg)
                        git_sync_state(f"config: {src_key} {param_name}={new_val or 'off'}")
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

                elif data.startswith("src_market_now:"):
                    src_key = data.split(":", 1)[1]
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        answer_callback(cb_id, f"📈 מנתח מחירי שוק עבור {src_key}…")
                        send_telegram(f"📈 <b>שולף נתוני שוק עדכניים מיד 2 עבור [{src_key}]…</b>")
                        api_url = src_info.get("api_url") or yad2_web_url_to_api_url(src_info.get("url", ""))
                        cat = src_info.get("category") or detect_url_category(api_url)
                        listings = fetch_via_api(
                            api_url, src_info.get("label", src_key), src_info.get("url", ""), category=cat, max_pages=3 if cat == "vehicles" else 1
                        )
                        st_path = get_state_path(src_info, src_key)
                        st = load_state(st_path)
                        if listings is not None:
                            new_i, upd_i, rem_i = detect_changes(st.get("listings", {}), listings)
                            st = update_state(st, listings)
                            save_state(st_path, st)
                            git_sync_state(f"market-scan: {src_key}")
                            report = build_market_analysis_report(src_key, src_info, st, new_i, upd_i, rem_i)
                        else:
                            report = build_market_analysis_report(src_key, src_info, st)
                        send_telegram(report, reply_markup=single_source_keyboard(src_key, src_info))

                elif data.startswith("src_summary:"):
                    src_key = data.split(":", 1)[1]
                    answer_callback(cb_id, f"📋 שולח מודעות מחיפוש {src_key}…")
                    send_daily_summary(only_source=src_key)

                elif data.startswith("price_menu:"):
                    src_key = data.split(":", 1)[1]
                    src_info = cfg.get("sources", {}).get(src_key, {})
                    is_car = src_info.get("category") == "vehicles"
                    prices = ["50000", "75000", "100000", "125000", "150000", "200000"] if is_car else ["7000", "8500", "10000", "11000", "12000", "15000"]
                    kb = [
                        [{"text": f"₪{int(p):,}", "callback_data": f"set_param:{src_key}:maxPrice:{p}"} for p in prices[:3]],
                        [{"text": f"₪{int(p):,}", "callback_data": f"set_param:{src_key}:maxPrice:{p}"} for p in prices[3:]],
                        [
                            {"text": "✏️ הקלד סכום מדויק", "callback_data": f"ask_input:{src_key}:maxPrice"},
                            {"text": "♾ ללא תקרת מחיר", "callback_data": f"set_param:{src_key}:maxPrice:none"},
                        ],
                        [{"text": f"🔙 חזרה לחיפוש {src_key}", "callback_data": f"src_view:{src_key}"}],
                    ]
                    answer_callback(cb_id)
                    edit_or_send_telegram(
                        f"💰 <b>בחר מחיר מקסימלי עבור חיפוש [{src_key}]:</b>",
                        msg_id,
                        {"inline_keyboard": kb},
                    )

                elif data.startswith("minprice_menu:"):
                    src_key = data.split(":", 1)[1]
                    src_info = cfg.get("sources", {}).get(src_key, {})
                    is_car = src_info.get("category") == "vehicles"
                    prices = ["30000", "50000", "70000", "90000"] if is_car else ["5000", "6500", "7500", "8500"]
                    kb = [
                        [{"text": f"₪{int(p):,}", "callback_data": f"set_param:{src_key}:minPrice:{p}"} for p in prices],
                        [
                            {"text": "✏️ הקלד סכום מדויק", "callback_data": f"ask_input:{src_key}:minPrice"},
                            {"text": "♾ ללא מינימום", "callback_data": f"set_param:{src_key}:minPrice:none"},
                        ],
                        [{"text": f"🔙 חזרה לחיפוש {src_key}", "callback_data": f"src_view:{src_key}"}],
                    ]
                    answer_callback(cb_id)
                    edit_or_send_telegram(
                        f"💵 <b>בחר מחיר מינימלי עבור חיפוש [{src_key}]:</b>",
                        msg_id,
                        {"inline_keyboard": kb},
                    )

                elif data.startswith("rooms_menu:"):
                    src_key = data.split(":", 1)[1]
                    rooms_opts = ["2", "2.5", "3", "3.5", "4", "4.5", "5"]
                    kb = [
                        [{"text": f"{r}+ חד׳", "callback_data": f"set_param:{src_key}:minRooms:{r}"} for r in rooms_opts[:4]],
                        [{"text": f"{r}+ חד׳", "callback_data": f"set_param:{src_key}:minRooms:{r}"} for r in rooms_opts[4:]],
                        [
                            {"text": "✏️ הקלד מספר חדרים", "callback_data": f"ask_input:{src_key}:minRooms"},
                            {"text": "♾ ללא הגבלה", "callback_data": f"set_param:{src_key}:minRooms:none"},
                        ],
                        [{"text": f"🔙 חזרה לחיפוש {src_key}", "callback_data": f"src_view:{src_key}"}],
                    ]
                    answer_callback(cb_id)
                    edit_or_send_telegram(
                        f"🛏 <b>בחר מינימום חדרים עבור חיפוש [{src_key}]:</b>",
                        msg_id,
                        {"inline_keyboard": kb},
                    )

                elif data.startswith("set_param:"):
                    _, src_key, param_name, new_val = data.split(":", 3)
                    src_info = cfg.get("sources", {}).get(src_key)
                    if src_info:
                        val_clean = "" if new_val == "none" else new_val
                        src_info["api_url"] = update_url_param(src_info.get("api_url", ""), param_name, val_clean)
                        src_info["url"] = update_url_param(src_info.get("url", ""), param_name, val_clean)
                        save_config(cfg)
                        git_sync_state(f"config: {src_key} {param_name}={val_clean or 'none'}")
                        answer_callback(cb_id, "עודכן בהצלחה ✓")
                        edit_or_send_telegram(
                            build_source_detail_text(src_key, src_info),
                            msg_id,
                            single_source_keyboard(src_key, src_info),
                        )

                elif data.startswith("ask_input:"):
                    _, src_key, field_name = data.split(":", 2)
                    cfg["awaiting_input"] = {"action": field_name, "src_key": src_key}
                    save_config(cfg)
                    prompts = {
                        "search_loc": f"🔍 הקלד עכשיו את <b>שם העיר או השכונה בעברית</b> עבור חיפוש [{src_key}] (למשל: <code>חולון</code>, <code>אגרובנק</code>, <code>נאות רחל</code>, <code>ראשון לציון</code>, <code>רמת אביב</code>):",
                        "search_car": f"🚗 הקלד עכשיו את <b>שם יצרן הרכב בעברית</b> עבור [{src_key}] (למשל: <code>מאזדה</code>, <code>טויוטה</code>, <code>יונדאי</code>, <code>מיני</code>):",
                        "maxPrice": f"💰 שלח עכשיו את <b>המחיר המקסימלי</b> עבור חיפוש [{src_key}] (למשל <code>10500</code>, או <code>0</code> לביטול הגבלה):",
                        "minPrice": f"💵 שלח עכשיו את <b>המחיר המינימלי</b> עבור חיפוש [{src_key}] (למשל <code>7000</code>, או <code>0</code> לביטול הגבלה):",
                        "minRooms": f"🛏 שלח עכשיו את <b>מינימום החדרים</b> עבור חיפוש [{src_key}] (למשל <code>3.5</code> או <code>4</code>):",
                        "maxRooms": f"🛏 שלח עכשיו את <b>מקסימום החדרים</b> עבור חיפוש [{src_key}] (למשל <code>5</code>, או <code>0</code> ללא הגבלה):",
                        "minSquareMeter": f"📐 שלח עכשיו את <b>הגודל המינימלי במ״ר</b> עבור חיפוש [{src_key}] (למשל <code>85</code>, או <code>0</code> לביטול הגבלה):",
                        "minFloor": f"🏢 שלח עכשיו את <b>הקומה המינימלית</b> עבור חיפוש [{src_key}] (למשל <code>2</code>, או <code>0</code> לביטול הגבלה):",
                        "maxFloor": f"🏢 שלח עכשיו את <b>הקומה המקסימלית</b> עבור חיפוש [{src_key}] (למשל <code>6</code>, או <code>0</code> לביטול הגבלה):",
                        "year": f"📅 שלח עכשיו את <b>טווח השנתונים</b> עבור רכב [{src_key}] (למשל <code>2019-2022</code> או <code>2021-2021</code>):",
                        "hand": f"🖐 שלח עכשיו את <b>טווח היד</b> עבור רכב [{src_key}] (למשל <code>1-2</code> ליד ראשונה-שנייה, או <code>0</code> להכל):",
                        "km": f"🛣 שלח עכשיו את <b>הקילומטראז׳ המקסימלי</b> עבור רכב [{src_key}] (למשל <code>90000</code>, או <code>0</code> ללא הגבלה):",
                        "label": f"🏷 שלח עכשיו את <b>השם החדש</b> עבור חיפוש [{src_key}]:",
                        "url": f"🔗 שלח עכשיו את <b>הקישור החדש מאתר יד 2</b> עבור חיפוש [{src_key}]:",
                    }
                    answer_callback(cb_id)
                    send_telegram(prompts.get(field_name, "שלח את הערך החדש:") + "\n<i>(לביטול שלח /cancel)</i>")

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
                        "⚙️ <b>הגדרת חיפושים</b>\nהחיפוש נמחק בהצלחה.",
                        msg_id,
                        sources_menu_keyboard(cfg),
                    )
                continue

            # 2) Handle Text Commands & Conversational Input
            msg = upd.get("message") or {}
            chat_id = (msg.get("chat") or {}).get("id")
            text = (msg.get("text") or "").strip()
            if not text or chat_id != TELEGRAM_CHAT_ID:
                continue

            log.info(f"Telegram message received: {text}")

            # Check if we are waiting for conversational input first (unless user typed a slash command other than /cancel)
            if cfg.get("awaiting_input") and (not text.startswith("/") or text.lower().startswith("/cancel")):
                if handle_conversational_input(cfg, text):
                    continue

            cmd_parts = text.split(maxsplit=1)
            cmd = cmd_parts[0].lower().split("@")[0]
            arg = cmd_parts[1] if len(cmd_parts) > 1 else ""

            if cmd in ("/start", "/menu", "/help"):
                cfg["awaiting_input"] = None
                save_config(cfg)
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
                    "⚙️ <b>הגדרת חיפושים ומדדי מחיר</b>\nלחץ על חיפוש כדי לערוך את כל הפרמטרים שלו:",
                    reply_markup=sources_menu_keyboard(cfg),
                )

            elif cmd == "/market":
                if arg.strip() and "yad2.co.il" in arg:
                    lbl, url_p = [x.strip() for x in arg.split("|", 1)] if "|" in arg else ("", arg.strip())
                    send_telegram("📈 <b>מנתח את השוק ב-API של יד 2…</b>")
                    new_key, reply = create_new_source_from_url(cfg, url_p, lbl, mode="market")
                    send_telegram(reply, reply_markup=single_source_keyboard(new_key, cfg["sources"][new_key]))
                else:
                    send_telegram(
                        "📈 <b>מדד מחיר שוק — מכירת רכב 🚗 / השכרת או מכירת דירה 🏠</b>\n"
                        "בחר חיפוש לניתוח מחיר שוק או הוסף רכב/דירה לבדיקת מחיר:",
                        reply_markup=market_menu_keyboard(cfg),
                    )

            elif cmd == "/pause":
                cfg["paused"] = True
                save_config(cfg)
                git_sync_state("config: paused=True")
                send_telegram("⏸ <b>המוניטור הושהה.</b>", reply_markup=main_menu_keyboard(cfg))

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
                if "yad2.co.il" in arg:
                    lbl, url_p = [x.strip() for x in arg.split("|", 1)] if "|" in arg else ("", arg.strip())
                    new_key, reply = create_new_source_from_url(cfg, url_p, lbl, mode="alert")
                    send_telegram(reply, reply_markup=single_source_keyboard(new_key, cfg["sources"][new_key]))
                else:
                    send_telegram(
                        "➕ <b>הוספת חיפוש או מדד מחיר</b>\nבחר מה תרצה להוסיף:",
                        reply_markup={
                            "inline_keyboard": [
                                [{"text": "🔔 התרעות קונה/שוכר", "callback_data": "add_alert_prompt"}],
                                [{"text": "🚗 מדד מחיר שוק למכירת רכב", "callback_data": "add_market_car"}],
                                [{"text": "🏠 מדד מחיר שוק להשכרת/מכירת דירה", "callback_data": "add_market_apt"}],
                            ]
                        },
                    )

            elif "yad2.co.il" in text:
                lbl, url_p = [x.strip() for x in text.split("|", 1)] if "|" in text else ("", text.strip())
                new_key, reply = create_new_source_from_url(cfg, url_p, lbl, mode="alert")
                send_telegram(reply, reply_markup=single_source_keyboard(new_key, cfg["sources"][new_key]))

            else:
                send_telegram(
                    "🤖 בחר פעולה מלוח הבקרה:",
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
    parser = argparse.ArgumentParser(description="Yad2 100% API Monitor, Market Analyzer & Telegram Control Bot")
    parser.add_argument("--summary", action="store_true", help="Send active listings summary")
    parser.add_argument("--send-menu", action="store_true", help="Send interactive control menu to Telegram")
    parser.add_argument("--bot-only", action="store_true", help="Only process Telegram commands/buttons without scheduled scan")
    parser.add_argument("--listen-seconds", type=int, default=0, help="Keep listening for interactive Telegram button clicks for N seconds")
    parser.add_argument("--force-notify", action="store_true", help="Send Telegram scan report even if 0 changes")
    args = parser.parse_args()

    register_bot_commands()

    # Process any queued Telegram commands/button clicks immediately first!
    process_telegram_updates(listen_seconds=0)

    cfg = load_config()
    if args.send_menu or not cfg.get("welcome_menu_v3_sent"):
        cfg["welcome_menu_v3_sent"] = True
        save_config(cfg)
        send_telegram(build_status_text(cfg), reply_markup=main_menu_keyboard(cfg))

    if args.summary:
        send_daily_summary()
    elif not args.bot_only:
        run_monitor_scan(notify_even_if_no_changes=args.force_notify)

    listen_sec = args.listen_seconds
    if listen_sec == 0 and os.environ.get("GITHUB_ACTIONS") == "true" and not args.summary:
        # Stay online for 25 minutes (1500s) inside each 30-min cron window so buttons respond in <1s!
        listen_sec = int(cfg.get("listen_seconds", 1500))

    if listen_sec > 0:
        log.info(f"Listening for interactive Telegram commands for {listen_sec}s…")
        process_telegram_updates(
            listen_seconds=listen_sec,
            scan_interval_minutes=int(cfg.get("scan_interval_minutes", 20)),
        )


if __name__ == "__main__":
    main()
