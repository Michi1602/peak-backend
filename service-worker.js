// PEAK Service Worker
// ════════════════════════════════════════════════════════════════════════
// Strategy:
//  - HTML: network-first, fall back to cache (keep app up-to-date)
//  - Static assets (icons, manifest): cache-first (performance)
//  - API calls: always network (dynamic data must be fresh)
//  - Auth callbacks: NEVER cached (tokens are one-time-use)
//  - Offline: if network fails entirely → serve cached index.html
//
// Bump CACHE_VERSION + RUNTIME_CACHE TOGETHER when deploying. The runtime
// cache holds static assets (CSS/JS/images/manifest) cache-first — if you
// only bump CACHE_VERSION but leave RUNTIME_CACHE stale, users keep getting
// served old static assets forever even after the new SW activates.
// ════════════════════════════════════════════════════════════════════════

const CACHE_VERSION = 'peak-v72-fix91';
const RUNTIME_CACHE = 'peak-runtime-v72-fix91';

// Core files to pre-cache on install (app shell)
const PRECACHE_URLS = [
  '/',
  '/index.html',
  '/manifest.json',
  '/supabase.js',
  '/icons/icon-192.png',
  '/icons/icon-512.png',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_VERSION).then((cache) => {
      // Pre-cache silently — don't block install if something fails
      return Promise.allSettled(
        PRECACHE_URLS.map((url) => cache.add(url).catch((err) => {
          console.warn('[SW] Precache failed for', url, err);
        }))
      );
    })
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) => {
      return Promise.all(
        keys
          .filter((key) => key !== CACHE_VERSION && key !== RUNTIME_CACHE)
          .map((key) => caches.delete(key))
      );
    }).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const { request } = event;
  const url = new URL(request.url);

  // Skip non-GET, chrome-extension://, etc.
  if (request.method !== 'GET') return;
  if (!url.protocol.startsWith('http')) return;

  // ── Never cache auth callbacks (Supabase magic links, OAuth) ──
  // These contain one-time tokens. Caching them would replay tokens
  // on reload and break login.
  // OAuth PKCE flow returns to the page with `?code=...` — without skipping
  // the cache here the SW would serve the cached page and the URL params
  // never reach the Supabase client, breaking Google/Apple login silently.
  //
  // Note: we don't check url.hash because the browser strips the fragment
  // before dispatching the request to the service worker — checking it
  // is a no-op (audit #2.4). If Supabase ever switches back to hash tokens
  // (was default before PKCE), the page itself needs to message the SW
  // via navigator.serviceWorker.controller.postMessage().
  if (
    url.search.includes('access_token=') ||
    url.search.includes('refresh_token=') ||
    url.search.includes('code=') ||
    url.pathname.includes('/auth/callback') ||
    url.pathname.includes('/auth/v1/')
  ) {
    return; // Let browser handle normally, no SW intervention
  }

  // ── Never cache auth or API calls — always go to network ──
  if (
    url.pathname.startsWith('/api/') ||
    url.pathname.startsWith('/auth/') ||
    url.pathname.startsWith('/ai/') ||
    url.pathname.startsWith('/user/') ||
    url.pathname.startsWith('/share/') ||
    url.pathname.startsWith('/family/') ||
    url.pathname.startsWith('/webhook') ||
    url.hostname.includes('supabase.co') ||
    url.hostname.includes('peak-backend') ||
    url.hostname.includes('anthropic.com') ||
    url.hostname.includes('stripe.com') ||
    url.hostname.includes('resend.com')
  ) {
    return; // Let browser handle normally, no SW intervention
  }

  // ── HTML navigation: network-first ──
  if (request.mode === 'navigate' || request.destination === 'document') {
    event.respondWith(
      fetch(request)
        .then((response) => {
          // Store fresh copy in runtime cache
          const copy = response.clone();
          caches.open(RUNTIME_CACHE).then((cache) => cache.put(request, copy));
          return response;
        })
        .catch(() => {
          // Offline: try cache, fall back to index
          return caches.match(request).then((cached) => cached || caches.match('/index.html'));
        })
    );
    return;
  }

  // ── Static assets: stale-while-revalidate (Audit Befund 22) ──
  // Previously: pure cache-first with no TTL. A compromised asset
  // would stay in the cache until the next SW activate. Now: serve
  // cached version immediately (perf), but kick off a background
  // refetch + cache-update so the next page load gets the new copy.
  // Combined with the Vercel hash-stamped asset URLs (manifest icons,
  // any bundled assets in the future), this gives us a self-healing
  // cache without an explicit Max-Age check.
  if (
    request.destination === 'image' ||
    request.destination === 'style' ||
    request.destination === 'script' ||
    request.destination === 'font' ||
    url.pathname === '/manifest.json'
  ) {
    event.respondWith(
      caches.match(request).then((cached) => {
        // Background refetch — happens whether or not we had a cached copy.
        const networkPromise = fetch(request).then((response) => {
          if (response.ok && url.origin === self.location.origin) {
            const copy = response.clone();
            caches.open(RUNTIME_CACHE).then((cache) => cache.put(request, copy));
          }
          return response;
        }).catch(() => null);
        // Serve cached if available, otherwise wait for network.
        return cached || networkPromise;
      })
    );
    return;
  }

  // ── Default: network, fall back to cache ──
  event.respondWith(
    fetch(request).catch(() => caches.match(request))
  );
});

// Receive messages from the page (e.g. to force update)
self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});
