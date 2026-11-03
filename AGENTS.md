# Yad2 Cloud Monitor & Market Price Analyzer — Project Context (`AGENTS.md`)

## Overview
- **GitHub Repository:** `reznikevg/yad2-monitor-claude` (`main` branch, connected via `gh` CLI as `reznikevg`).
- **Local Workspace Directory:** `C:\Users\natalie\OneDrive\מסמכים\yad2`
- **Architecture:** 100% pure API scraper & interactive Telegram Bot running in the cloud on GitHub Actions (`.github/workflows/monitor.yml`), with zero browser/Playwright dependency (`noop_playwright/` skips Chromium install in CI).

## Core Capabilities (`yad2_monitor.py`)
1. **100% API Scraping (Real Estate + Vehicles/Cars):**
   - Uses `curl_cffi>=0.17.0` with TLS profile rotation (`chrome131`, `chrome124`, `safari17_0`, `edge101`) and matching headers (no mismatched `User-Agent` override) to bypass Yad2's Radware Bot Manager.
   - Supports **Real Estate** (`gw.yad2.co.il/realestate-feed/rent/map` & `forsale/map`) AND **Vehicles/Cars** (`gw.yad2.co.il/vehicles-feed/cars`).
2. **Two Modes per Search Source (`state/config.json`):**
   - `"mode": "alert"` (Buyer/Renter): Sends Telegram alerts on new listings or price/detail changes.
   - `"mode": "market"` (Seller/Landlord Market Price Analyzer — for selling a car 🚗 or renting/selling an apartment 🏠):
     - Fetches competing ads (up to 3 pages for cars), separates **Private Sellers** vs **Commercial Agencies**, breaks down median prices by Year or Rooms, recommends **Fast Sale (25th pct)** / **Recommended Publish Price (60th pct)** / **Premium Price (75th pct)**, and tracks competitor price drops & sold/removed ads.
3. **Interactive Telegram Control & Conversational State Machine:**
   - Continuous cloud long-polling (`listen_seconds: 1500` = 25 minutes per 30-min cron cycle) so inline buttons in Telegram respond in `< 1 second`.
   - Supports full search configuration via inline buttons & conversational text input (`awaiting_input` in `state/config.json`): maxPrice, minPrice, minRooms, maxRooms, year, hand, parking toggle, shelter toggle, rename label, replace URL, and adding new Alert or Market monitors.
