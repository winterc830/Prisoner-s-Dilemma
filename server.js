// =============================================
//  囚徒困境 多人對戰伺服器
//  使用方式：
//    npm install ws
//    node server.js
//  預設埠：3000
// =============================================

const http = require('http');
const fs   = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;
const TOTAL_ROUNDS = 5;
const PAYOFF = {
  c: { c: [3, 3], b: [0, 5] },
  b: { c: [5, 0], b: [1, 1] },
};

// rooms: { [roomId]: Room }
const rooms = {};

// clients: WeakMap<WebSocket, { roomId, role }>
const clients = new WeakMap();

// ── Room 資料結構 ──────────────────────────────
function createRoom(roomId, password, hostName, hostWs) {
  return {
    id: roomId,
    password,
    status: 'waiting',   // waiting | playing | finished
    round: 0,
    players: {
      p1: { name: hostName, score: 0, ws: hostWs, choice: null, ready: false },
      p2: null,
    },
    history: [],  // [{ p1Choice, p2Choice, p1pts, p2pts }]
  };
}

// ── 工具 ──────────────────────────────────────
function genId() {
  return Math.random().toString(36).substr(2, 4).toUpperCase();
}

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function sendBoth(room, obj) {
  if (room.players.p1) send(room.players.p1.ws, obj);
  if (room.players.p2) send(room.players.p2.ws, obj);
}

function publicRoom(room) {
  return {
    id: room.id,
    status: room.status,
    round: room.round,
    p1Name: room.players.p1?.name ?? null,
    p2Name: room.players.p2?.name ?? null,
    p1Score: room.players.p1?.score ?? 0,
    p2Score: room.players.p2?.score ?? 0,
    history: room.history,
  };
}

// ── 輪次結算 ──────────────────────────────────
function resolveRound(room) {
  const p1c = room.players.p1.choice;
  const p2c = room.players.p2.choice;
  const [p1pts, p2pts] = PAYOFF[p1c][p2c];

  room.players.p1.score += p1pts;
  room.players.p2.score += p2pts;
  room.history.push({ p1Choice: p1c, p2Choice: p2c, p1pts, p2pts });
  room.round += 1;

  const result = {
    type: 'round_result',
    round: room.round,
    p1Choice: p1c, p2Choice: p2c,
    p1pts, p2pts,
    p1Score: room.players.p1.score,
    p2Score: room.players.p2.score,
    history: room.history,
  };

  room.players.p1.choice = null;
  room.players.p2.choice = null;

  if (room.round >= TOTAL_ROUNDS) {
    room.status = 'finished';
    result.finished = true;
    result.winner =
      room.players.p1.score > room.players.p2.score ? 'p1' :
      room.players.p2.score > room.players.p1.score ? 'p2' : 'tie';
  }

  sendBoth(room, result);

  // 結束後 30 秒清理房間
  if (room.status === 'finished') {
    setTimeout(() => { delete rooms[room.id]; }, 30000);
  }
}

// ── WebSocket 訊息處理 ─────────────────────────
function handleMessage(ws, raw) {
  let msg;
  try { msg = JSON.parse(raw); } catch { return; }

  switch (msg.type) {

    case 'create_room': {
      const { name, password } = msg;
      if (!name || !password) {
        send(ws, { type: 'error', msg: '請提供名稱和密碼' });
        return;
      }
      let roomId;
      do { roomId = genId(); } while (rooms[roomId]);
      const room = createRoom(roomId, password, name, ws);
      rooms[roomId] = room;
      clients.set(ws, { roomId, role: 'p1' });
      send(ws, { type: 'room_created', roomId, role: 'p1', ...publicRoom(room) });
      console.log(`[+] Room ${roomId} created by ${name}`);
      break;
    }

    case 'join_room': {
      const { name, password, roomId } = msg;
      const room = rooms[roomId?.toUpperCase()];
      if (!room) {
        send(ws, { type: 'error', msg: '找不到房間，請確認代碼' });
        return;
      }
      if (room.password !== password) {
        send(ws, { type: 'error', msg: '密碼錯誤' });
        return;
      }
      if (room.players.p2) {
        send(ws, { type: 'error', msg: '房間已滿' });
        return;
      }
      if (room.status !== 'waiting') {
        send(ws, { type: 'error', msg: '遊戲已開始' });
        return;
      }
      room.players.p2 = { name, score: 0, ws, choice: null };
      room.status = 'playing';
      clients.set(ws, { roomId: room.id, role: 'p2' });

      const pub = publicRoom(room);
      send(ws,               { type: 'joined', role: 'p2', ...pub });
      send(room.players.p1.ws, { type: 'opponent_joined', ...pub });
      console.log(`[+] ${name} joined room ${room.id}`);
      break;
    }

    case 'choose': {
      const meta = clients.get(ws);
      if (!meta) return;
      const room = rooms[meta.roomId];
      if (!room || room.status !== 'playing') return;

      const { choice } = msg;
      if (choice !== 'c' && choice !== 'b') return;

      const player = room.players[meta.role];
      if (player.choice) return; // 已選過
      player.choice = choice;

      // 通知對方「對手已選擇（但不透露內容）」
      const oppRole = meta.role === 'p1' ? 'p2' : 'p1';
      send(room.players[oppRole]?.ws, { type: 'opponent_chose' });
      send(ws, { type: 'choice_ack' });

      // 雙方都選了 → 結算
      if (room.players.p1.choice && room.players.p2?.choice) {
        resolveRound(room);
      }
      break;
    }

    case 'ping':
      send(ws, { type: 'pong' });
      break;
  }
}

// ── 斷線處理 ──────────────────────────────────
function handleClose(ws) {
  const meta = clients.get(ws);
  if (!meta) return;
  const room = rooms[meta.roomId];
  if (!room) return;

  const oppRole = meta.role === 'p1' ? 'p2' : 'p1';
  const opp = room.players[oppRole];
  send(opp?.ws, { type: 'opponent_disconnected' });
  delete rooms[meta.roomId];
  console.log(`[-] Room ${meta.roomId} closed (disconnect)`);
}

// ── HTTP 伺服器（提供 index.html）────────────────
const server = http.createServer((req, res) => {
  const filePath = path.join(__dirname, 'index.html');
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('index.html not found — place it in the same folder as server.js');
      return;
    }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

// ── WebSocket 伺服器 ───────────────────────────
const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  ws.on('message', (raw) => handleMessage(ws, raw));
  ws.on('close',   () => handleClose(ws));
  ws.on('error',   (e) => console.error('ws error:', e.message));
});

server.listen(PORT, () => {
  console.log(`\n囚徒困境伺服器啟動！`);
  console.log(`http://localhost:${PORT}\n`);
});
