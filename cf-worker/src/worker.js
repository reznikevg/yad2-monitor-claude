/**
 * Yad2 API proxy — runs on Cloudflare Workers.
 * Forwards GET requests to gw.yad2.co.il with proper headers.
 * Bypasses CF datacenter-IP block (Workers run inside CF, not blocked).
 *
 * Usage: GET /?path=/realestate-feed/rent/map&region=1&...
 *   or: GET /realestate-feed/rent/map?region=1&...   (path passthrough)
 *
 * Optional auth: set WORKER_TOKEN secret; clients send X-Auth-Token header.
 */

export default {
  async fetch(request, env) {
    // Optional shared-secret auth
    if (env.WORKER_TOKEN) {
      const got = request.headers.get("X-Auth-Token");
      if (got !== env.WORKER_TOKEN) {
        return new Response("unauthorized", { status: 401 });
      }
    }

    const url = new URL(request.url);
    let upstreamPath = url.pathname;
    let upstreamSearch = url.search;

    // Allow ?path=... override (single querystring style)
    const explicitPath = url.searchParams.get("path");
    if (explicitPath) {
      upstreamPath = explicitPath;
      const params = new URLSearchParams(url.search);
      params.delete("path");
      upstreamSearch = params.toString() ? "?" + params.toString() : "";
    }

    // Default health check at /
    if (upstreamPath === "/" && !explicitPath) {
      return new Response(JSON.stringify({ ok: true, hint: "GET /<path>?<params> proxies to gw.yad2.co.il" }), {
        headers: { "content-type": "application/json" },
      });
    }

    const upstream = "https://gw.yad2.co.il" + upstreamPath + upstreamSearch;

    const upstreamReq = new Request(upstream, {
      method: "GET",
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin": "https://www.yad2.co.il",
        "Referer": "https://www.yad2.co.il/",
      },
    });

    const resp = await fetch(upstreamReq, {
      // Cloudflare-specific: don't cache, follow redirects
      cf: { cacheTtl: 0 },
    });
    const body = await resp.text();

    // Debug mode: ?_debug=1 returns metadata
    if (url.searchParams.get("_debug") === "1") {
      return new Response(JSON.stringify({
        upstream,
        status: resp.status,
        statusText: resp.statusText,
        contentType: resp.headers.get("content-type"),
        bodyLength: body.length,
        bodyStart: body.slice(0, 500),
      }, null, 2), {
        headers: { "content-type": "application/json" },
      });
    }

    return new Response(body, {
      status: resp.status,
      headers: {
        "content-type": resp.headers.get("content-type") || "application/json",
        "x-yad2-proxy": "ok",
        "x-yad2-upstream-status": String(resp.status),
        "x-yad2-body-length": String(body.length),
      },
    });
  },
};
