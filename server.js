// Lantern Guild – เซิร์ฟเวอร์ตัวกลาง (relay) สำหรับ Multiplayer
// รัน: npm install && npm start   (ฟังที่พอร์ต process.env.PORT หรือ 8080, path /ws)
const http = require('http');
const { WebSocketServer } = require('ws');

const MAX_PLAYERS = 4;
const GRACE_MS = 30000;          // รอให้ผู้เล่นที่หลุดเชื่อมต่อกลับเข้ามา
const rooms = new Map();         // port -> room
const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const rnd = n => Math.floor(Math.random() * n);
const token = () => Array.from({ length: 4 }, () => Math.random().toString(36).slice(2)).join('');

function newPort() {
  for (let i = 0; i < 200; i++) {
    const p = String(1000 + rnd(9000));
    if (!rooms.has(p)) return p;
  }
  return null;
}

function dropPlayer(room, id) {
  const pl = room.players.get(id);
  if (!pl) return;
  clearTimeout(pl.timer);
  room.players.delete(id);
  if (id === room.hostId) {                       // โฮสต์ออก -> ปิดห้อง
    room.players.forEach(p => { send(p.ws, { t: 'closed' }); try { p.ws && p.ws.close(); } catch (e) {} clearTimeout(p.timer); });
    rooms.delete(room.port);
  } else {
    const h = room.players.get(room.hostId);
    if (h) send(h.ws, { t: 'left', id });
  }
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Lantern Guild relay OK · rooms: ' + rooms.size);
});
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 4 * 1024 * 1024 });

wss.on('connection', ws => {
  ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });
  let ctx = null;                                 // { room, id }

  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    if (!ctx) {                                   // ข้อความแรก: create / join / resume
      if (m.t === 'create') {
        const port = newPort();
        if (!port) return send(ws, { t: 'err', msg: 'เซิร์ฟเวอร์เต็ม ลองใหม่ภายหลัง' });
        const id = 'p1', tok = token();
        const room = { port, hostId: id, seq: 1, started: false, players: new Map() };
        room.players.set(id, { ws, tok, timer: 0 });
        rooms.set(port, room);
        ctx = { room, id };
        send(ws, { t: 'created', port, id, tok });
      } else if (m.t === 'join') {
        const room = rooms.get(String(m.port));
        if (!room) return send(ws, { t: 'err', msg: 'ไม่พบห้องนี้ ตรวจสอบเลข Port อีกครั้ง' });
        if (room.started) return send(ws, { t: 'err', msg: 'ห้องนี้เริ่มเกมไปแล้ว' });
        if (room.players.size >= MAX_PLAYERS) return send(ws, { t: 'err', msg: 'ห้องเต็มแล้ว (สูงสุด 4 คน)' });
        const id = 'p' + (++room.seq), tok = token();
        room.players.set(id, { ws, tok, timer: 0 });
        ctx = { room, id };
        send(ws, { t: 'joined', port: room.port, id, tok });
        send(room.players.get(room.hostId).ws, { t: 'peer', id });
      } else if (m.t === 'resume') {
        const room = rooms.get(String(m.port)), pl = room && room.players.get(m.id);
        if (!pl || pl.tok !== m.tok) return send(ws, { t: 'err', msg: 'กลับเข้าห้องไม่ได้ (ห้องถูกปิดหรือหมดเวลา)' });
        clearTimeout(pl.timer);
        try { if (pl.ws && pl.ws !== ws) pl.ws.close(); } catch (e) {}
        pl.ws = ws;
        ctx = { room, id: m.id };
        send(ws, { t: 'resumed' });
      }
      return;
    }

    const { room, id } = ctx;
    if (m.t === 'leave') { dropPlayer(room, id); ctx = null; return; }
    if (id === room.hostId) {                     // โฮสต์ -> ทุกคนที่เหลือ
      if (m.t === 'start') room.started = true;
      room.players.forEach((p, pid) => { if (pid !== id) send(p.ws, m); });
    } else {                                      // ผู้เล่น -> โฮสต์ (ใส่ from)
      const h = room.players.get(room.hostId);
      if (h) send(h.ws, { ...m, from: id });
    }
  });

  ws.on('close', () => {
    if (!ctx) return;
    const { room, id } = ctx, pl = room.players.get(id);
    if (!pl || pl.ws !== ws) return;              // ถูกแทนที่ด้วยการ resume แล้ว
    pl.ws = null;
    pl.timer = setTimeout(() => dropPlayer(room, id), GRACE_MS);
  });
  ws.on('error', () => {});
});

// ping ทุก 25 วินาที กันโฮสต์ตัดการเชื่อมต่อที่ว่างนาน + เก็บกวาดการเชื่อมต่อที่ตายแล้ว
setInterval(() => wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; try { ws.ping(); } catch (e) {} }), 25000);

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => console.log('Lantern Guild relay listening on :' + PORT + ' (path /ws)'));
