/* CARTA service worker — gives the web app full offline browsing
 * after the first visit. Three caches:
 *
 *   carta-shell  — the HTML entry point + external fonts. Cache-first
 *                  with a background refresh so new deploys get picked
 *                  up on the next navigation.
 *   carta-api    — responses from /api/atlas, /api/articles/:slug,
 *                  /api/provinces/:slug. Network-first; falls back to
 *                  cache when offline.
 *   carta-images — /static/images/* served by the Pi. Cache-first,
 *                  kept small by an LRU trim.
 *
 * The compile, capture, and TTS endpoints are NOT cached — they're
 * mutations or expensive one-shots that should never serve stale
 * results. TTS in particular must bypass both this worker and the
 * browser's HTTP cache so voice changes always re-synth.
 */

const SHELL   = "carta-shell-v3";
const API     = "carta-api-v3";
const IMAGES  = "carta-images-v3";
const IMAGE_CAP = 300;   // max images kept; oldest evicted on overflow

const SHELL_URLS = [
  "/",
  "/index.html",
  // Google Fonts links (the index.html references Fraunces, EB Garamond,
  // IBM Plex Mono). We don't prefetch the font binaries — let the browser
  // load them once and we'll catch them in the runtime fetch handler.
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(SHELL).then((cache) => cache.addAll(SHELL_URLS)).catch(() => null)
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil((async () => {
    const keep = new Set([SHELL, API, IMAGES]);
    const keys = await caches.keys();
    await Promise.all(keys.filter((k) => !keep.has(k)).map((k) => caches.delete(k)));
    await self.clients.claim();
  })());
});

// Classify a request URL so the right cache strategy is applied.
function classify(url) {
  const u = new URL(url);
  const isApi   = u.pathname.startsWith("/api/");
  const isImg   = u.pathname.startsWith("/static/images/");
  const isTTS   = u.pathname === "/api/tts";
  const isCap   = u.pathname === "/api/capture";
  const isCompile = u.pathname === "/api/compile";
  const isShell = u.pathname === "/" || u.pathname === "/index.html";
  const isFont  = /fonts\.(googleapis|gstatic)\.com/.test(u.host);
  return { isApi, isImg, isTTS, isCap, isCompile, isShell, isFont };
}

async function networkFirst(req, cacheName) {
  const cache = await caches.open(cacheName);
  try {
    const fresh = await fetch(req);
    if (fresh && fresh.ok) cache.put(req, fresh.clone());
    return fresh;
  } catch {
    const cached = await cache.match(req);
    if (cached) return cached;
    throw new Error("offline and no cached copy");
  }
}

async function cacheFirst(req, cacheName) {
  const cache = await caches.open(cacheName);
  const cached = await cache.match(req);
  if (cached) {
    // Kick off a background refresh so next hit stays fresh.
    fetch(req).then((fresh) => {
      if (fresh && fresh.ok) cache.put(req, fresh.clone());
    }).catch(() => {});
    return cached;
  }
  const fresh = await fetch(req);
  if (fresh && fresh.ok) cache.put(req, fresh.clone());
  return fresh;
}

async function trimImagesCache() {
  const cache = await caches.open(IMAGES);
  const keys = await cache.keys();
  if (keys.length <= IMAGE_CAP) return;
  // oldest-first eviction: Cache doesn't expose timestamps, so we
  // evict the first (N - CAP) keys — effectively FIFO per origin.
  const over = keys.length - IMAGE_CAP;
  await Promise.all(keys.slice(0, over).map((k) => cache.delete(k)));
}

self.addEventListener("fetch", (event) => {
  const req = event.request;
  if (req.method !== "GET") return;

  const { isApi, isImg, isTTS, isCap, isCompile, isShell, isFont } = classify(req.url);

  // Mutations / one-shots — bypass cache entirely, let them fail
  // visibly when offline.
  if (isTTS || isCap || isCompile) return;

  if (isShell) {
    event.respondWith(networkFirst(req, SHELL));
    return;
  }
  if (isFont) {
    event.respondWith(cacheFirst(req, SHELL));
    return;
  }
  if (isImg) {
    event.respondWith((async () => {
      const res = await cacheFirst(req, IMAGES);
      trimImagesCache().catch(() => {});
      return res;
    })());
    return;
  }
  if (isApi) {
    event.respondWith(networkFirst(req, API));
    return;
  }
  // Same-origin everything else (CSS/JS embedded in index.html): let it flow,
  // the browser's HTTP cache handles it well enough.
});
