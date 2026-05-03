#!/usr/bin/env python3
"""
yad2_monitor.py — Yad2 Rental Listings Monitor
================================================
Monitors two Yad2 search URLs for new/updated apartments and sends
Telegram notifications. Designed to run as a cron job or Task Scheduler task.

Requirements:
    pip install playwright requests
    playwright install chromium

Usage:
    python yad2_monitor.py

State files location: ~/AIcode/Yad2/yad2-rent-monitor/
"""

import asyncio
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import requests
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "8210921909:AAFdWOV2asiHvz66Z3_-So4LqS7tD1hpGO4")
TELEGRAM_CHAT_ID = int(os.environ.get("TELEGRAM_CHAT_ID", "678043915"))
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

# Local: ~/AIcode/Yad2/yad2-rent-monitor  |  CI: STATE_DIR env var → ./state/
_default_state = str(Path.home() / "AIcode" / "Yad2" / "yad2-rent-monitor")
STATE_DIR = Path(os.environ.get("STATE_DIR", _default_state))
STATE_FILE_A = STATE_DIR / "state_tlv.json"
STATE_FILE_B = STATE_DIR / "state_center_sharon.json"
FAILED_ALERTS_FILE = STATE_DIR / "failed_alerts.txt"
LOG_FILE = STATE_DIR / "monitor.log"

SOURCES = {
    "A": {
        "label": "תל אביב (שכ׳ 793)",
        "url": (
            "https://www.yad2.co.il/realestate/rent/tel-aviv-area"
            "?maxPrice=10000&minRooms=4&parking=1&shelter=1&area=11&city=6600&neighborhood=793"
            "&bBox=32.021438%2C34.773431%2C32.029569%2C34.785892&zoom=15"
        ),
        "state_file": STATE_FILE_A,
    },
    "B": {
        "label": "מרכז והשרון",
        "url": (
            "https://www.yad2.co.il/realestate/rent/center-and-sharon"
            "?maxPrice=10000&minRooms=4"
            "&multiNeighborhood=470%2C991420%2C991421%2C20436"
        ),
        "state_file": STATE_FILE_B,
    },
}

# Israel Standard Time = UTC+3
IST = timezone(timedelta(hours=3))

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

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


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def now_ist() -> str:
    """Current time in Israel Standard Time as ISO string."""
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


def make_hash(listing: dict) -> str:
    """Deterministic hash of mutable listing fields."""
    key = "|".join(str(listing.get(f, "")) for f in
                   ["id", "price", "rooms", "floor", "address", "description"])
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def load_state(path: Path) -> dict:
    """Load state JSON or return empty state."""
    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            log.warning(f"Corrupt state file {path}, starting fresh.")
    return {"listings": {}, "lastCheck": None}


def save_state(path: Path, state: dict):
    """Persist state to disk atomically (write to .tmp then rename)."""
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    tmp.replace(path)
    log.info(f"State saved → {path}")


def write_failed_alert(message: str):
    """Append a failed alert to the fallback file."""
    ts = now_ist()
    with open(FAILED_ALERTS_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {message}\n")
    log.warning(f"Alert written to failed_alerts.txt: {message[:80]}…")


def send_telegram(text: str) -> bool:
    """
    POST message to Telegram.
    Returns True on success, False on failure.
    Falls back to failed_alerts.txt if unreachable.
    """
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    try:
        resp = requests.post(TELEGRAM_API, json=payload, timeout=15, verify=False)
        if resp.status_code == 200:
            log.info("Telegram notification sent ✓")
            return True
        else:
            log.error(f"Telegram API error {resp.status_code}: {resp.text[:200]}")
            write_failed_alert(f"TELEGRAM FAILED ({resp.status_code}): {text[:300]}")
            return False
    except requests.RequestException as e:
        log.error(f"Telegram network error: {e}")
        write_failed_alert(f"TELEGRAM NETWORK ERROR: {e} | MESSAGE: {text[:300]}")
        return False


# ─────────────────────────────────────────────
# SCRAPING WITH PLAYWRIGHT
# ─────────────────────────────────────────────

# JavaScript injected into the page to extract listing data from Yad2's React state.
# Yad2 uses Next.js / React — listing data is stored in __NEXT_DATA__ or rendered DOM.
# We try __NEXT_DATA__ first (fast, no DOM parsing), then fall back to DOM scraping.
EXTRACT_JS = r"""
(function() {
  // ── Strategy 1: Next.js SSR data (rich, first ~16 items) ──────────────
  let ndItems = [];
  try {
    const nd = window.__NEXT_DATA__;
    if (nd) {
      const propsStr = JSON.stringify(nd.props || {});
      const parsed = JSON.parse(propsStr);

      function findFeedItems(obj, depth) {
        if (depth > 10 || !obj || typeof obj !== 'object') return null;
        if (Array.isArray(obj)) {
          if (obj.length > 0 && obj[0] && (obj[0].orderId || (obj[0].id && obj[0].price))) {
            return obj;
          }
          for (const item of obj) {
            const r = findFeedItems(item, depth + 1);
            if (r) return r;
          }
        } else {
          for (const key of Object.keys(obj)) {
            const r = findFeedItems(obj[key], depth + 1);
            if (r) return r;
          }
        }
        return null;
      }

      ndItems = findFeedItems(parsed, 0) || [];
    }
  } catch(e) {}

  // ── Strategy 2: DOM scraping (all items loaded after scroll) ──────────
  let domItems = [];
  try {
    const cards = document.querySelectorAll(
      '[data-testid="feed-item"], .feeditem, .feed-item, .item-data-content, [class*="FeedItem"], [class*="feedItem"]'
    );
    cards.forEach(card => {
      const link = card.querySelector('a[href*="/item/"]');
      const href = link ? link.getAttribute('href') : '';
      // Take last path segment before query string as the listing token
      const id = href ? (href.split('?')[0].split('/').filter(Boolean).pop() || '') : '';

      const priceEl = card.querySelector('[data-testid="price"], .price, [class*="price"], [class*="Price"]');
      const priceText = priceEl ? priceEl.textContent.replace(/[^0-9]/g, '') : '';

      const roomsEl = card.querySelector('[data-testid="rooms"], [class*="rooms"], [class*="Rooms"]');
      const roomsText = roomsEl ? roomsEl.textContent.trim() : '';

      const floorEl = card.querySelector('[data-testid="floor"], [class*="floor"], [class*="Floor"]');
      const floorText = floorEl ? floorEl.textContent.trim() : '';

      const addrEl = card.querySelector('[data-testid="address"], .address, [class*="address"], [class*="Address"]');
      const addrText = addrEl ? addrEl.textContent.trim() : '';

      const descEl = card.querySelector('[data-testid="title"], [class*="title"], [class*="Title"], h2, h3');
      const descText = descEl ? descEl.textContent.trim() : '';

      if (id || priceText) {
        domItems.push({ id, price: priceText, rooms: roomsText, floor: floorText, address: addrText, description: descText, href });
      }
    });
  } catch(e) {}

  return { source: 'both', ndItems, domItems };
})()
"""


def normalize_listing(raw: dict, source_type: str) -> Optional[dict]:
    """
    Normalize a raw listing dict (from either next_data or dom) into our schema.
    Returns None if the item is an ad/promoted placeholder without a real ID.
    """
    try:
        if source_type == "next_data":
            item_id = str(raw.get("orderId") or raw.get("id") or raw.get("adNumber") or "")
            price_raw = raw.get("price") or raw.get("priceFormatted") or 0
            price = int(str(price_raw).replace(",", "").replace("₪", "").strip()) if price_raw else 0

            # additionalDetails has the real rooms/size/floor
            details = raw.get("additionalDetails") or {}
            rooms = str(details.get("roomsCount") or raw.get("rooms") or raw.get("roomsCount") or "")
            sqm = str(details.get("squareMeter") or "")
            prop_type = (details.get("property") or {}).get("text") or ""
            tags_list = [t.get("name", "") for t in (raw.get("tags") or []) if t.get("name")]
            tags = ", ".join(tags_list[:4])

            # Address: structured nested object
            addr_obj = raw.get("address") or {}
            if isinstance(addr_obj, dict):
                street_obj = addr_obj.get("street") or {}
                street_name = street_obj.get("text") if isinstance(street_obj, dict) else str(street_obj)
                house_obj = addr_obj.get("house") or {}
                house_num = str(house_obj.get("number") or "") if isinstance(house_obj, dict) else str(house_obj)
                floor_num = str(house_obj.get("floor") or "") if isinstance(house_obj, dict) else ""
                city_obj = addr_obj.get("city") or {}
                city_name = city_obj.get("text") if isinstance(city_obj, dict) else str(city_obj)
                neighborhood_obj = addr_obj.get("neighborhood") or {}
                neighborhood = neighborhood_obj.get("text") if isinstance(neighborhood_obj, dict) else ""
                address = f"{street_name} {house_num}, {city_name}".strip(", ")
                if neighborhood:
                    address += f" ({neighborhood})"
            else:
                address = str(addr_obj)
                floor_num = ""

            floor = floor_num or str(raw.get("floor") or raw.get("floorCount") or "")
            description = str(raw.get("info") or raw.get("title") or (raw.get("metaData") or {}).get("h1") or "")[:120]
            link_token = raw.get("token") or raw.get("orderId") or item_id
            url = f"https://www.yad2.co.il/item/{link_token}" if link_token else ""

        else:  # dom
            item_id = str(raw.get("id") or "")
            price = int(raw.get("price", "0") or 0) if str(raw.get("price", "")).isdigit() else 0
            rooms = str(raw.get("rooms") or "")
            floor = str(raw.get("floor") or "")
            address = str(raw.get("address") or "")
            description = str(raw.get("description") or "")[:120]
            href = raw.get("href") or ""
            url = f"https://www.yad2.co.il{href}" if href.startswith("/") else href
            sqm = ""
            prop_type = ""
            tags = ""

        if not item_id or item_id in ("0", "undefined", "null"):
            return None

        listing = {
            "id": item_id,
            "price": price,
            "rooms": rooms,
            "floor": floor,
            "sqm": sqm,
            "prop_type": prop_type,
            "tags": tags,
            "address": address,
            "description": description,
            "url": url,
        }
        listing["hash"] = make_hash(listing)
        return listing

    except Exception as e:
        log.debug(f"normalize_listing error: {e} | raw={str(raw)[:100]}")
        return None


async def fetch_listings(browser, url: str, label: str) -> Optional[dict]:
    """
    Open `url` in a new browser page, wait for JS to render listings,
    run extraction JS, and return dict of {id: listing}.
    Returns None on failure.
    """
    page = await browser.new_page()
    try:
        log.info(f"[{label}] Navigating to {url[:80]}…")

        # Stealth: realistic user-agent and viewport
        await page.set_extra_http_headers({
            "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
        })

        await page.goto(url, wait_until="domcontentloaded", timeout=45_000)

        # Wait for listings to appear — Yad2 renders asynchronously
        # Try multiple possible selectors
        selectors = [
            '[data-testid="feed-item"]',
            '.feeditem',
            '[class*="FeedItem"]',
            '[class*="feedItem"]',
            '.item-data-content',
        ]
        loaded = False
        for sel in selectors:
            try:
                await page.wait_for_selector(sel, timeout=15_000)
                log.info(f"[{label}] Listings appeared via selector: {sel}")
                loaded = True
                break
            except PlaywrightTimeout:
                continue

        if not loaded:
            # Give it an extra few seconds — maybe page is just slow
            log.warning(f"[{label}] No selector matched, waiting 8s as fallback…")
            await asyncio.sleep(8)

        # Scroll loop — trigger infinite scroll until DOM count stabilizes
        prev_count = 0
        for _ in range(8):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2.5)
            count = await page.evaluate(
                "document.querySelectorAll('[class*=\"FeedItem\"],[class*=\"feedItem\"],[data-testid=\"feed-item\"]').length"
            )
            if count == prev_count:
                break
            prev_count = count
        await asyncio.sleep(1)

        # Run extraction (both __NEXT_DATA__ and DOM)
        result = await page.evaluate(EXTRACT_JS)
        nd_items = result.get("ndItems", [])
        dom_items = result.get("domItems", [])
        log.info(f"[{label}] Extracted nd={len(nd_items)} dom={len(dom_items)}")

        if not nd_items and not dom_items:
            log.warning(f"[{label}] Zero items extracted — possible Cloudflare block or page structure change")
            screenshot_path = STATE_DIR / f"debug_{label.replace(' ', '_')}.png"
            await page.screenshot(path=str(screenshot_path), full_page=True)
            log.info(f"[{label}] Screenshot saved: {screenshot_path}")
            return None

        # Pass 1: normalize __NEXT_DATA__ items (rich data, first ~16)
        listings = {}
        for raw in nd_items:
            normalized = normalize_listing(raw, "next_data")
            if normalized:
                listings[normalized["id"]] = normalized

        # Build set of tokens already covered by __NEXT_DATA__
        tracked_tokens = set()
        for v in listings.values():
            url = v.get("url", "")
            if "/item/" in url:
                tracked_tokens.add(url.rsplit("/", 1)[-1])

        # Pass 2: add DOM-only items (scroll-loaded, not in __NEXT_DATA__)
        for raw in dom_items:
            normalized = normalize_listing(raw, "dom")
            if normalized and normalized["id"] not in tracked_tokens and normalized["id"] not in listings:
                listings[normalized["id"]] = normalized

        log.info(f"[{label}] Normalized {len(listings)} valid listings")
        return listings

    except PlaywrightTimeout:
        log.error(f"[{label}] Page load timeout (45s)")
        return None
    except Exception as e:
        log.error(f"[{label}] Fetch error: {e}")
        return None
    finally:
        await page.close()


# ─────────────────────────────────────────────
# CHANGE DETECTION
# ─────────────────────────────────────────────

def detect_changes(old_listings: dict, new_listings: dict) -> tuple[list, list]:
    """
    Compare old vs new listings.
    Returns (new_items, updated_items).
    Removed items are tracked silently (lastSeen stops updating).
    """
    new_items = []
    updated_items = []

    for item_id, listing in new_listings.items():
        if item_id not in old_listings:
            new_items.append(listing)
        else:
            old = old_listings[item_id]
            if listing["hash"] != old.get("hash", ""):
                # Collect changed fields for the notification
                changed = {}
                for field in ["price", "rooms", "floor", "address", "description"]:
                    if str(listing.get(field, "")) != str(old.get(field, "")):
                        changed[field] = {"old": old.get(field, ""), "new": listing[field]}
                if changed:
                    listing["_changed"] = changed
                    updated_items.append(listing)

    return new_items, updated_items


# ─────────────────────────────────────────────
# TELEGRAM MESSAGE BUILDER
# ─────────────────────────────────────────────

FIELD_LABELS = {
    "price": "מחיר",
    "rooms": "חדרים",
    "floor": "קומה",
    "address": "כתובת",
    "description": "תיאור",
}


def format_listing_new(listing: dict) -> str:
    price = f"₪{listing['price']:,}" if isinstance(listing['price'], int) and listing['price'] else "מחיר לא ידוע"
    rooms = listing.get('rooms') or '?'
    floor = listing.get('floor') or '?'
    sqm = listing.get('sqm') or ''
    prop_type = listing.get('prop_type') or ''
    tags = listing.get('tags') or ''
    address = listing.get('address', '?')
    desc = listing.get('description', '')
    url = listing.get('url', '')

    # Line 1: address + price
    lines = [f"• <b>{address}</b>"]
    # Line 2: key specs
    specs = []
    if rooms and rooms != '?': specs.append(f"{rooms} חדרים")
    if sqm: specs.append(f"{sqm} מ״ר")
    if floor and floor != '?': specs.append(f"קומה {floor}")
    if prop_type: specs.append(prop_type)
    specs.append(price + "/חודש")
    lines.append("  " + " | ".join(specs))
    # Line 3: tags/amenities
    if tags:
        lines.append(f"  🏷 {tags}")
    # Line 4: description
    if desc:
        lines.append(f"  <i>{desc[:100]}</i>")
    if url:
        lines.append(f'  🔗 <a href="{url}">לצפייה במודעה</a>')
    return "\n".join(lines)


def format_listing_updated(listing: dict) -> str:
    address = listing.get('address', listing.get('id', '?'))
    url = listing.get('url', '')
    changed = listing.get('_changed', {})
    lines = [f"• <b>{address}</b>"]
    for field, vals in changed.items():
        label = FIELD_LABELS.get(field, field)
        lines.append(f"  {label}: {vals['old']} ← <b>{vals['new']}</b>")
    if url:
        lines.append(f'  🔗 <a href="{url}">לצפייה במודעה</a>')
    return "\n".join(lines)


def build_telegram_message(results: dict, counts: dict) -> Optional[str]:
    """
    Build the full Telegram message.
    Returns None if there are no changes at all.
    """
    ts = now_ist()
    has_any_change = any(
        results[src]["new"] or results[src]["updated"]
        for src in results
    )
    if not has_any_change:
        return None

    parts = [f"🏠 <b>Yad2 Monitor</b> | {ts}\n"]

    for src_key, src_info in SOURCES.items():
        if src_key not in results:
            continue
        new_items = results[src_key]["new"]
        updated_items = results[src_key]["updated"]
        if not new_items and not updated_items:
            continue

        parts.append(f"\n📍 <b>{src_info['label']}</b>")

        if new_items:
            parts.append("✅ <b>חדש:</b>")
            for l in new_items:
                parts.append(format_listing_new(l))

        if updated_items:
            parts.append("🔄 <b>עודכן:</b>")
            for l in updated_items:
                parts.append(format_listing_updated(l))

    count_a = counts.get("A", 0)
    count_b = counts.get("B", 0)
    parts.append(f'\n📊 סה"כ: A={count_a} | B={count_b}')

    return "\n".join(parts)


# ─────────────────────────────────────────────
# STATE UPDATE
# ─────────────────────────────────────────────

def update_state(old_state: dict, new_listings: dict) -> dict:
    ts = now_ist()
    updated = old_state.copy()
    old_listings = updated.get("listings", {})

    for item_id, listing in new_listings.items():
        if item_id in old_listings:
            # Update existing: preserve firstSeen, update the rest
            merged = old_listings[item_id].copy()
            merged.update(listing)
            merged["lastSeen"] = ts
            old_listings[item_id] = merged
        else:
            listing["firstSeen"] = ts
            listing["lastSeen"] = ts
            old_listings[item_id] = listing

    updated["listings"] = old_listings
    updated["lastCheck"] = ts
    return updated


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

async def main():
    log.info("=" * 60)
    log.info(f"Yad2 Monitor — run started at {now_ist()}")
    log.info("=" * 60)

    # ── Step 1: Load state ────────────────────────────────────────
    states = {
        "A": load_state(STATE_FILE_A),
        "B": load_state(STATE_FILE_B),
    }
    for src, state in states.items():
        n = len(state.get("listings", {}))
        last = state.get("lastCheck", "never")
        log.info(f"[Source {src}] Loaded {n} listings, lastCheck={last}")

    # ── Step 2: Fetch pages ───────────────────────────────────────
    fetched = {}
    failed_sources = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-infobars",
                "--window-size=1280,900",
                "--lang=he-IL",
            ],
        )

        def make_context(pw_browser):
            return pw_browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
                locale="he-IL",
                timezone_id="Asia/Jerusalem",
                extra_http_headers={
                    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                },
            )

        context = await make_context(browser)

        # Patch navigator.webdriver = false on every page
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['he-IL','he','en-US','en'] });
            window.chrome = { runtime: {} };
        """)

        for src_key, src_info in SOURCES.items():
            listings = await fetch_listings(context, src_info["url"], src_info["label"])
            # Retry once with a fresh context if blocked (0 items)
            if listings is None:
                log.warning(f"[Source {src_key}] Retrying with fresh context…")
                await context.close()
                await asyncio.sleep(5)
                context = await make_context(browser)
                await context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                    Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
                    Object.defineProperty(navigator, 'languages', { get: () => ['he-IL','he','en-US','en'] });
                    window.chrome = { runtime: {} };
                """)
                listings = await fetch_listings(context, src_info["url"], src_info["label"])

            if listings is None:
                failed_sources.append(src_key)
                log.error(f"[Source {src_key}] FAILED to fetch listings (after retry)")
            else:
                fetched[src_key] = listings

        await context.close()
        await browser.close()

    # ── Error handling: both failed ───────────────────────────────
    if len(failed_sources) == 2:
        msg = (
            f"⚠️ <b>Yad2 Monitor — שגיאה קריטית</b>\n\n"
            f"שני המקורות לא נטענו ב-{now_ist()}\n"
            f"• Source A ({SOURCES['A']['label']}) — נכשל\n"
            f"• Source B ({SOURCES['B']['label']}) — נכשל\n\n"
            f"בדוק את קובץ הלוג: {LOG_FILE}"
        )
        send_telegram(msg)
        log.error("Both sources failed — exiting without state update.")
        return

    # ── Error handling: single source failed ──────────────────────
    for src_key in failed_sources:
        warn = (
            f"⚠️ Yad2 Monitor — Source {src_key} "
            f"({SOURCES[src_key]['label']}) לא נטען ב-{now_ist()}"
        )
        send_telegram(warn)

    # ── Step 3: Detect changes ────────────────────────────────────
    results = {}
    counts = {}
    for src_key, listings in fetched.items():
        old = states[src_key].get("listings", {})
        new_items, updated_items = detect_changes(old, listings)
        results[src_key] = {"new": new_items, "updated": updated_items}
        counts[src_key] = len(listings)
        log.info(
            f"[Source {src_key}] Changes — NEW: {len(new_items)}, "
            f"UPDATED: {len(updated_items)}, TOTAL: {len(listings)}"
        )

    # ── Step 4: Send Telegram notification ────────────────────────
    message = build_telegram_message(results, counts)
    if message:
        send_telegram(message)
    else:
        ts = now_ist()
        count_a = counts.get("A", "N/A")
        count_b = counts.get("B", "N/A")
        log.info(f"✓ {ts} — אין שינויים. A={count_a} | B={count_b}")
        print(f"✓ {ts} — אין שינויים. A={count_a} | B={count_b}")

    # ── Step 5: Update state files ────────────────────────────────
    for src_key, listings in fetched.items():
        new_state = update_state(states[src_key], listings)
        save_state(SOURCES[src_key]["state_file"], new_state)

    log.info(f"Run complete at {now_ist()}")


def send_daily_summary():
    """Send all known listings from state files to Telegram — no scraping needed."""
    ts = now_ist()
    log.info(f"Daily summary — {ts}")
    total_sent = 0

    for src_key, src_info in SOURCES.items():
        data = load_state(src_info["state_file"])
        listings = list(data.get("listings", {}).values())
        total = len(listings)
        if total == 0:
            send_telegram(f"📋 <b>סיכום יומי — {src_info['label']}</b>\nאין דירות שמורות כרגע.")
            continue

        batch_size = 8
        for i in range(0, total, batch_size):
            batch = listings[i:i + batch_size]
            header = (
                f"📋 <b>סיכום יומי {ts}</b>\n"
                f"📍 <b>{src_info['label']}</b> "
                f"({i + 1}–{min(i + len(batch), total)} מתוך {total})\n"
            )
            lines = [header] + [format_listing_new(l) for l in batch]
            send_telegram("\n".join(lines))
            total_sent += len(batch)

    log.info(f"Daily summary sent — {total_sent} listings total")


if __name__ == "__main__":
    import sys
    if "--summary" in sys.argv:
        send_daily_summary()
    else:
        asyncio.run(main())
