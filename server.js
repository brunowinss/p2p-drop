// Servidor de sinalização: só apresenta os dois aparelhos um ao outro.
// Os arquivos vão direto de um aparelho para o outro (WebRTC). Quando o P2P não é possível,
// passam por aqui como relay, mas criptografados de ponta a ponta: o servidor só vê bytes cifrados.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const rooms = new Map(); // código -> Set<ws>

const TYPES = { '.html': 'text/html; charset=utf-8', '.json': 'application/json', '.js': 'text/javascript; charset=utf-8', '.png': 'image/png', '.txt': 'text/plain; charset=utf-8' };
const PUBLIC = path.join(__dirname, 'public');

// Servidores ICE (STUN + TURN). TURN é opcional e vem de variáveis de ambiente no Render:
//   Cloudflare: CF_TURN_KEY_ID + CF_TURN_API_TOKEN (credenciais temporárias geradas aqui)
//   Outro provedor: TURN_URLS (separadas por vírgula) + TURN_USERNAME + TURN_CREDENTIAL
const STUN = [{ urls: 'stun:stun.l.google.com:19302' }, { urls: 'stun:stun.cloudflare.com:3478' }];
let iceCache = { at: 0, servers: null };
async function iceServers() {
  if (iceCache.servers && Date.now() - iceCache.at < 3600e3) return iceCache.servers;
  const servers = [...STUN];
  const { CF_TURN_KEY_ID, CF_TURN_API_TOKEN, TURN_URLS, TURN_USERNAME, TURN_CREDENTIAL } = process.env;
  try {
    if (CF_TURN_KEY_ID && CF_TURN_API_TOKEN) {
      const r = await fetch(`https://rtc.live.cloudflare.com/v1/turn/keys/${CF_TURN_KEY_ID}/credentials/generate`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${CF_TURN_API_TOKEN}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ ttl: 86400 }),
        signal: AbortSignal.timeout(5000)
      });
      if (r.ok) { const j = await r.json(); if (j.iceServers) servers.push(j.iceServers); }
    } else if (TURN_URLS) {
      servers.push({ urls: TURN_URLS.split(',').map((s) => s.trim()), username: TURN_USERNAME, credential: TURN_CREDENTIAL });
    }
  } catch {}
  iceCache = { at: Date.now(), servers };
  return servers;
}

const server = http.createServer(async (req, res) => {
  if (req.url === '/ice') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    return res.end(JSON.stringify(await iceServers()));
  }
  // "Compartilhar" do PWA cai aqui só se o service worker ainda não estiver ativo
  if (req.method === 'POST' && req.url.startsWith('/share')) { req.resume(); res.writeHead(303, { Location: '/' }); return res.end(); }
  let p;
  // URL malformada (ex.: /%E0%A4%A) lançava URIError e derrubava o processo
  try { p = decodeURIComponent(req.url.split('?')[0]); } catch { res.writeHead(400); return res.end('bad request'); }
  if (p.includes('\0')) { res.writeHead(400); return res.end('bad request'); }
  if (p === '/') p = '/index.html';
  const file = path.normalize(path.join(PUBLIC, p));
  // só serve arquivos dentro de /public
  if (!file.startsWith(PUBLIC + path.sep) || !TYPES[path.extname(file)] || !fs.existsSync(file)) {
    res.writeHead(404); return res.end('not found');
  }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)], 'Cache-Control': 'no-cache' });
  fs.createReadStream(file).pipe(res);
});

// maxPayload: os pedaços de arquivo têm 64 KB; 1 MB sobra e evita mensagens gigantes
const wss = new WebSocketServer({ server, maxPayload: 1 << 20 });
wss.on('error', () => {});

// Limite de tentativas de "join" por IP: dificulta adivinhar códigos de 6 dígitos
const JOIN_LIMIT = 30, JOIN_WINDOW = 60000;
const joinLog = new Map(); // ip -> [timestamps]
function tooManyJoins(ip) {
  const now = Date.now();
  const list = (joinLog.get(ip) || []).filter((t) => now - t < JOIN_WINDOW);
  list.push(now);
  joinLog.set(ip, list);
  return list.length > JOIN_LIMIT;
}
setInterval(() => {
  const now = Date.now();
  for (const [ip, list] of joinLog) if (!list.some((t) => now - t < JOIN_WINDOW)) joinLog.delete(ip);
}, JOIN_WINDOW);

wss.on('connection', (ws, req) => {
  ws.room = null;
  // sem este handler, um frame inválido emitia 'error' sem ouvinte e derrubava o processo
  ws.on('error', () => {});
  const fwd = req.headers['x-forwarded-for'];
  const ip = fwd ? String(fwd).split(',').pop().trim() : req.socket.remoteAddress;
  ws.on('message', (raw, isBinary) => {
    // fallback relay: pedaços binários de arquivo são repassados como estão
    if (isBinary) {
      const r = rooms.get(ws.room);
      if (r) for (const peer of r) if (peer !== ws && peer.readyState === 1) peer.send(raw, { binary: true });
      return;
    }
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (!msg || typeof msg !== 'object') return;   // JSON "null" fazia msg.type lançar TypeError

    if (msg.type === 'join') {
      // a sala é um hash derivado do código (o servidor nunca vê o código em si)
      const code = String(msg.code || '').slice(0, 64);
      if (!/^[a-f0-9]{16,64}$/.test(code)) return;
      if (tooManyJoins(ip)) return ws.send(JSON.stringify({ type: 'busy' }));
      let room = rooms.get(code);
      if (!room) { room = new Set(); rooms.set(code, room); }
      // mesmo aparelho reconectando: derruba a conexão antiga dele
      for (const old of [...room]) {
        if ((msg.id && old.cid === msg.id) || old.readyState !== 1) { room.delete(old); try { old.terminate(); } catch {} }
      }
      if (room.size >= 2) return ws.send(JSON.stringify({ type: 'full' }));
      ws.cid = msg.id;
      room.add(ws);
      ws.room = code;
      ws.send(JSON.stringify({ type: 'joined', peers: room.size }));
      // quem chegou por segundo inicia a conexão
      if (room.size === 2) {
        for (const peer of room) {
          if (peer !== ws) peer.send(JSON.stringify({ type: 'peer-joined' }));
        }
      }
      return;
    }

    // offer / answer / ice: repassa ao outro aparelho da sala
    const room = rooms.get(ws.room);
    if (!room) return;
    for (const peer of room) {
      if (peer !== ws && peer.readyState === 1) peer.send(JSON.stringify(msg));
    }
  });

  ws.on('close', () => {
    const room = rooms.get(ws.room);
    // conexão antiga que já foi substituída pela reconexão do mesmo aparelho: não avisa ninguém
    if (!room || !room.delete(ws)) return;
    for (const peer of room) peer.send(JSON.stringify({ type: 'peer-left' }));
    if (room.size === 0) rooms.delete(ws.room);
  });
});

// Heartbeat: evita que proxies (Render, Cloudflare) derrubem WebSockets ociosos
// e remove conexões mortas (ex.: celular que perdeu o sinal).
setInterval(() => {
  for (const c of wss.clients) {
    if (c.isAlive === false) { c.terminate(); continue; }
    c.isAlive = false;
    c.ping();
  }
}, 30000);
wss.on('connection', (ws) => { ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; }); });

server.listen(PORT, () => console.log(`P2P Drop rodando em http://localhost:${PORT}`));
