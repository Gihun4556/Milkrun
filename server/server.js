// Milk Run multiplayer: room codes, live positions, last player standing.
const http = require('http'), crypto = require('crypto');
const PORT = process.env.PORT || 8080;
const rooms = new Map();
let nextId = 1;

function makeCode() {
  let c; do { c = Math.random().toString(36).slice(2, 6).toUpperCase(); } while (rooms.has(c));
  return c;
}
const server = http.createServer((req, res) => { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('milk run server ok'); });

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  const client = { sock, id: String(nextId++), room: null, buf: Buffer.alloc(0) };
  sock.on('data', d => { client.buf = Buffer.concat([client.buf, d]); parse(client); });
  sock.on('close', () => leave(client));
  sock.on('error', () => leave(client));
});

function parse(c) {
  while (true) {
    const b = c.buf;
    if (b.length < 2) return;
    const op = b[0] & 0x0f, masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 0x7f, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
    let mask = null;
    if (masked) { if (b.length < off + 4) return; mask = b.slice(off, off + 4); off += 4; }
    if (b.length < off + len) return;
    const payload = Buffer.from(b.slice(off, off + len));
    if (masked) for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];
    c.buf = b.slice(off + len);
    if (op === 0x8) { c.sock.end(); return; }
    if (op === 0x9) { c.sock.write(frame(payload, 0xA)); continue; }
    if (op === 0x1) handle(c, payload.toString('utf8'));
  }
}

function frame(data, op = 0x1) {
  const p = Buffer.from(data);
  let h;
  if (p.length < 126) h = Buffer.from([0x80 | op, p.length]);
  else if (p.length < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(p.length, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(p.length), 2); }
  return Buffer.concat([h, p]);
}
function send(c, o) { try { c.sock.write(frame(JSON.stringify(o))); } catch (e) {} }
function roomList(r) { return [...r.players.values()].map(p => ({ id: p.id, name: p.name, av: p.av, alive: p.alive, score: p.score })); }
function m_av(v) { v = v | 0; return v >= 0 && v < 64 ? v : 0; }
function broadcast(r, o, exceptId) { for (const p of r.players.values()) if (p.id !== exceptId) send(p.client, o); }
function joinRoom(c, r, name, av) {
  c.room = r;
  r.players.set(c.id, { id: c.id, client: c, name: String(name || 'Игрок').slice(0, 12), av: (m_av(av)), alive: true, score: 0 });
  broadcast(r, { t: 'players', host: r.host, list: roomList(r) });
}
function leave(c) {
  const r = c.room;
  if (!r) return;
  c.room = null;
  r.players.delete(c.id);
  if (r.players.size === 0) { rooms.delete(r.code); return; }
  if (r.host === c.id) r.host = [...r.players.keys()][0];
  broadcast(r, { t: 'players', host: r.host, list: roomList(r) });
  checkEnd(r);
}
function checkEnd(r) {
  if (!r.started || r.players.size < 2) return;
  const alive = [...r.players.values()].filter(p => p.alive);
  if (alive.length > 1) return;
  r.started = false;
  let winner = alive[0];
  if (!winner) winner = [...r.players.values()].sort((a, b) => b.score - a.score)[0];
  broadcast(r, { t: 'end', winner: { id: winner.id, name: winner.name }, scores: roomList(r) });
}

function handle(c, text) {
  let m; try { m = JSON.parse(text); } catch (e) { return; }
  if (m.t === 'create') {
    if (c.room) leave(c);
    const cd = makeCode();
    const r = { code: cd, host: c.id, players: new Map(), started: false };
    rooms.set(cd, r);
    joinRoom(c, r, m.name, m.av);
    send(c, { t: 'room', code: cd, you: c.id });
    return;
  }
  if (m.t === 'join') {
    const cd = String(m.code || '').toUpperCase().trim();
    const r = rooms.get(cd);
    if (!r) return send(c, { t: 'error', msg: 'no_room' });
    if (r.started) return send(c, { t: 'error', msg: 'started' });
    if (r.players.size >= 4) return send(c, { t: 'error', msg: 'full' });
    if (c.room) leave(c);
    joinRoom(c, r, m.name, m.av);
    send(c, { t: 'room', code: cd, you: c.id });
    return;
  }
  if (m.t === 'leave') { leave(c); return; }
  const r = c.room;
  if (!r) return;
  const p = r.players.get(c.id);
  if (!p) return;
  if (m.t === 'start') {
    if (r.host !== c.id || r.started) return;
    r.started = true;
    for (const q of r.players.values()) { q.alive = true; q.score = 0; }
    broadcast(r, { t: 'start', seed: Math.floor(Math.random() * 1e9) }, null);
  } else if (m.t === 'state') {
    p.score = m.score | 0;
    broadcast(r, { t: 'peer', id: c.id, x: m.x, lane: m.lane, jy: m.jy, score: p.score, alive: p.alive }, c.id);
  } else if (m.t === 'dead') {
    if (!p.alive) return;
    p.alive = false;
    p.score = m.score | 0;
    broadcast(r, { t: 'peer_dead', id: c.id, score: p.score });
    checkEnd(r);
  }
}

server.listen(PORT, () => console.log('milk run server on ' + PORT));
