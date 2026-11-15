// Servidor de sinalização: só apresenta os dois aparelhos um ao outro.
// Os arquivos NÃO passam por aqui, vão direto de um aparelho para o outro (WebRTC).
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const rooms = new Map(); // código -> Set<ws>

const TYPES = { '.html': 'text/html; charset=utf-8', '.json': 'application/json', '.js': 'text/javascript; charset=utf-8', '.png': 'image/png' };
const PUBLIC = path.join(__dirname, 'public');

const server = http.createServer((req, res) => {
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
      const code = String(msg.code || '').slice(0, 12);
      if (!code) return;
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
    if (!room) return;
    room.delete(ws);
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
