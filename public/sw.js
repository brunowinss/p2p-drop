// Service worker mínimo: deixa o app instalável, abre a casca mesmo com rede ruim e recebe
// arquivos do menu "Compartilhar" do celular. A conexão (WebSocket/WebRTC) nunca passa pelo cache.
const CACHE = 'p2p-drop-v2';
const SHARE = 'p2p-share';   // arquivos compartilhados esperando a página pegar
const SHELL = ['/', '/manifest.json', '/icon-192.png', '/icon-512.png', '/apple-touch-icon.png'];

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL)).then(() => self.skipWaiting()));
});
self.addEventListener('activate', (e) => {
  e.waitUntil(caches.keys().then((ks) => Promise.all(ks.filter((k) => k !== CACHE && k !== SHARE).map((k) => caches.delete(k)))).then(() => self.clients.claim()));
});
self.addEventListener('fetch', (e) => {
  const req = e.request;
  const url = new URL(req.url);
  if (url.origin !== location.origin) return;
  // share_target do manifest: guarda os arquivos e abre a página, que os coloca na fila de envio
  if (req.method === 'POST' && url.pathname === '/share') {
    e.respondWith((async () => {
      try {
        const form = await req.formData();
        const c = await caches.open(SHARE);
        let i = 0;
        for (const f of form.getAll('files')) {
          if (!(f instanceof File)) continue;
          await c.put(`/shared/${Date.now()}-${i++}`, new Response(f, { headers: {
            'Content-Type': f.type || 'application/octet-stream',
            'X-Name': encodeURIComponent(f.name),
            'X-Mod': String(f.lastModified || Date.now())
          } }));
        }
      } catch {}
      return Response.redirect(location.origin + '/?share=1', 303);
    })());
    return;
  }
  if (req.method !== 'GET') return;
  // rede primeiro (pega atualizações), cache só como reserva
  e.respondWith(fetch(req).then((res) => {
    const copy = res.clone(); caches.open(CACHE).then((c) => c.put(req, copy)); return res;
  }).catch(() => caches.match(req, { ignoreSearch: true })));
});
