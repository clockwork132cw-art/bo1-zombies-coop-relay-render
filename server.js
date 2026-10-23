// BO1 Zombies co-op relay as a plain Node.js server (for Render, a VPS, or your own PC).
// Speaks exactly the same protocol as the Cloudflare Worker version (bo1-zombies-coop-relay), so the extension can't tell them apart:
// paste this server's https:// address where the workers.dev one used to go.
//
// Differences from the Worker version:
//   - Rooms live in memory. A restart (a redeploy, or Render waking from sleep) drops running games.
//   - No daily request allowance, so /usage always reports 0%.
import http from "node:http";
import { WebSocketServer } from "ws";
import {
  GUEST_ID_PATTERN,
  MAX_BINARY_BYTES,
  MAX_GUESTS,
  MAX_TEXT_BYTES,
  PROTOCOL_VERSION,
  ROLES,
  ROOM_ID_PATTERN,
  decodePackets,
  decodeSeatPackets,
  encodePackets,
  encodeSeatPackets,
  isPeerMessage,
  parseControl,
} from "./protocol.js";

const PORT = Number(process.env.PORT) || 10000;
const ALLOWED_ORIGINS = String(process.env.ALLOWED_ORIGINS ?? "https://vel.gg").split(",").map((o) => o.trim()).filter(Boolean);
// An empty room keeps its seat memory this long, so a guest who reloads gets the same seat back.
const EMPTY_ROOM_TTL_MS = 10 * 60 * 1000;
const KEEPALIVE_MS = 25 * 1000;

const CLOSE = { replaced: 4001, protocol: 4003, full: 4004, tooBig: 1009 };

// Same token buckets as the Worker: binary over the limit is dropped silently, as a lossy network would.
const LIMITS = {
  guest: { binaryRate: 400, binaryBurst: 800, textRate: 20, textBurst: 40 },
  host: { binaryRate: 1200, binaryBurst: 2400, textRate: 80, textBurst: 160 },
};

const OPEN = 1;

// ---------------------------------------------------------------------------------------------------------------
// Rooms

/** room id -> { host, guests: Map<guestId, socket>, seats: { [guestId]: seat }, emptySince } */
const rooms = new Map();

function getRoom(id) {
  let room = rooms.get(id);
  if (!room) rooms.set(id, (room = { host: null, guests: new Map(), seats: {}, emptySince: null }));
  room.emptySince = null;
  return room;
}

const isOpen = (socket) => socket && socket.readyState === OPEN;
const openGuests = (room) => [...room.guests.values()].filter(isOpen);
const hostOf = (room) => (isOpen(room.host) ? room.host : null);
const guestAt = (room, seat) => (Number.isInteger(seat) ? openGuests(room).find((s) => s.info.seat === seat) ?? null : null);
const guestSeats = (room) => [...new Set(openGuests(room).map((s) => s.info.seat))].sort();

/** Same seat rules as the Worker: keep a guest's old seat if it's free, else the lowest free seat nobody remembers. */
function claimSeat(room, guestId) {
  const taken = new Set(openGuests(room).filter((s) => s.info.id !== guestId).map((s) => s.info.seat));
  let seat = room.seats[guestId];
  if (!seat || taken.has(seat)) {
    const reserved = new Set(Object.values(room.seats));
    const free = [];
    for (let s = 1; s <= MAX_GUESTS; s += 1) if (!taken.has(s)) free.push(s);
    seat = free.find((s) => !reserved.has(s)) ?? free[0] ?? 0;
  }
  if (!seat) return 0;
  for (const [id, s] of Object.entries(room.seats)) if (s === seat && id !== guestId) delete room.seats[id];
  room.seats[guestId] = seat;
  return seat;
}

function send(socket, message) {
  forward(socket, JSON.stringify(message));
}

function forward(socket, payload) {
  if (!isOpen(socket)) return;
  try {
    socket.send(payload);
  } catch {
    // closed in the meantime; its close handler cleans up
  }
}

function close(socket, code, reason) {
  try {
    socket.close(code, reason);
  } catch {
    // already closed
  }
}

function take(socket, kind, rate, burst) {
  const now = Date.now();
  const state = socket.buckets[kind];
  state.tokens = Math.min(burst, state.tokens + ((now - state.at) / 1000) * rate);
  state.at = now;
  if (state.tokens < 1) return false;
  state.tokens -= 1;
  return true;
}

// ---------------------------------------------------------------------------------------------------------------
// Connections

function join(socket, roomId, role, guestId) {
  const room = getRoom(roomId);
  const limits = LIMITS[role];
  socket.buckets = { binary: { tokens: limits.binaryBurst, at: Date.now() }, text: { tokens: limits.textBurst, at: Date.now() } };
  socket.isAlive = true;
  socket.on("pong", () => (socket.isAlive = true));

  const seat = role === "guest" ? claimSeat(room, guestId) : 0;
  if (role === "guest" && !seat) {
    send(socket, { t: "error", code: "room_full" });
    close(socket, CLOSE.full, "Room full");
    return;
  }

  // Reconnecting with the same role (host) or guest id replaces the old socket.
  const old = role === "host" ? room.host : room.guests.get(guestId);
  if (old && old !== socket) {
    old.replaced = true;
    send(old, { t: "error", code: "replaced" });
    close(old, CLOSE.replaced, "Replaced by a new connection");
  }

  socket.info = { room: roomId, role, seat, id: guestId ?? null, joinedAt: Date.now() };
  if (role === "host") room.host = socket;
  else room.guests.set(guestId, socket);

  socket.on("message", (data, isBinary) => onMessage(room, socket, data, isBinary));
  socket.on("close", () => leave(room, socket));
  socket.on("error", () => close(socket, CLOSE.protocol, "WebSocket error"));

  if (role === "host") {
    send(socket, { t: "welcome", v: PROTOCOL_VERSION, role, guests: guestSeats(room) });
    for (const guest of openGuests(room)) send(guest, { t: "peer", present: true });
  } else {
    const host = hostOf(room);
    send(socket, { t: "welcome", v: PROTOCOL_VERSION, role, seat, peer: host !== null });
    if (host) send(host, { t: "peer", seat, present: true });
  }
}

function onMessage(room, socket, data, isBinary) {
  const { info } = socket;
  const limits = LIMITS[info.role];
  if (!isBinary) {
    if (data.length > MAX_TEXT_BYTES) {
      close(socket, CLOSE.tooBig, "Text frame too large");
      return;
    }
    onText(room, socket, info, data.toString("utf8"), limits);
  } else {
    onBinary(room, socket, info, data, limits);
  }
}

function onText(room, socket, info, payload, limits) {
  if (!take(socket, "text", limits.textRate, limits.textBurst)) {
    send(socket, { t: "error", code: "rate_limited" });
    return;
  }
  const message = parseControl(payload);
  if (!isPeerMessage(message)) {
    send(socket, { t: "error", code: "bad_message" });
    return;
  }
  if (info.role === "guest") {
    const host = hostOf(room);
    if (host) send(host, { ...message, from: info.seat });
  } else if (message.to === undefined) {
    for (const guest of openGuests(room)) forward(guest, payload);
  } else {
    const guest = guestAt(room, message.to);
    if (guest) forward(guest, payload);
  }
}

function onBinary(room, socket, info, payload, limits) {
  if (payload.byteLength > MAX_BINARY_BYTES) {
    close(socket, CLOSE.tooBig, "Binary frame too large");
    return;
  }
  if (!take(socket, "binary", limits.binaryRate, limits.binaryBurst)) return;

  if (info.role === "guest") {
    const host = hostOf(room);
    const packets = host ? decodePackets(payload) : null;
    if (packets) forward(host, encodeSeatPackets(packets.map((packet) => ({ seat: info.seat, packet }))));
    return;
  }
  const items = decodeSeatPackets(payload);
  if (!items) return;
  const perSeat = new Map();
  for (const { seat, packet } of items) {
    if (!perSeat.has(seat)) perSeat.set(seat, []);
    perSeat.get(seat).push(packet);
  }
  for (const [seat, packets] of perSeat) {
    const guest = guestAt(room, seat);
    if (guest) forward(guest, encodePackets(packets));
  }
}

function leave(room, socket) {
  const { info } = socket;
  // A socket replaced by a newer one leaves while its successor is already open: nobody needs to hear about it.
  if (info.role === "host") {
    if (room.host === socket) {
      room.host = null;
      for (const guest of openGuests(room)) send(guest, { t: "peer", present: false });
    }
  } else if (room.guests.get(info.id) === socket) {
    room.guests.delete(info.id);
    const host = hostOf(room);
    if (host) send(host, { t: "peer", seat: info.seat, present: false });
  }
  if (!room.host && room.guests.size === 0) room.emptySince = Date.now();
}

// ---------------------------------------------------------------------------------------------------------------
// HTTP

function originAllowed(origin) {
  return !origin || ALLOWED_ORIGINS.includes(origin);
}

function corsHeaders(origin) {
  if (!origin || !originAllowed(origin)) return {};
  return { "access-control-allow-origin": origin, vary: "Origin", "access-control-allow-methods": "GET, OPTIONS" };
}

function json(res, status, body, headers = {}) {
  res.writeHead(status, { "content-type": "application/json; charset=utf-8", ...headers });
  res.end(JSON.stringify(body));
}

function nextUtcMidnight() {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1)).toISOString();
}

function roomFromPath(pathname) {
  const match = pathname.match(/^\/v1\/rooms\/([^/]+)$/);
  if (!match) return null;
  try {
    const room = decodeURIComponent(match[1]);
    return ROOM_ID_PATTERN.test(room) ? room : null;
  } catch {
    return null;
  }
}

const server = http.createServer((req, res) => {
  const origin = req.headers.origin;
  const cors = corsHeaders(origin);
  if (req.method === "OPTIONS") {
    res.writeHead(204, cors);
    res.end();
    return;
  }
  const url = new URL(req.url, "http://relay");
  if (req.method === "GET" && (url.pathname === "/health" || url.pathname === "/")) {
    json(res, 200, { ok: true, protocol: PROTOCOL_VERSION }, cors);
    return;
  }
  if (req.method === "GET" && url.pathname === "/usage") {
    const day = new Date().toISOString().slice(0, 10);
    json(res, 200, { day, requests: 0, limit: 0, fraction: 0, resetsAt: nextUtcMidnight(), estimate: true }, { ...cors, "cache-control": "public, max-age=60" });
    return;
  }
  if (roomFromPath(url.pathname)) {
    json(res, 426, { error: "websocket_upgrade_required" }, cors);
    return;
  }
  json(res, 404, { error: "not_found" }, cors);
});

const wss = new WebSocketServer({ noServer: true, maxPayload: MAX_BINARY_BYTES, perMessageDeflate: false });

function reject(socket, status, body) {
  const text = JSON.stringify(body);
  socket.write(
    `HTTP/1.1 ${status} ${http.STATUS_CODES[status]}\r\ncontent-type: application/json; charset=utf-8\r\n` +
      `content-length: ${Buffer.byteLength(text)}\r\nconnection: close\r\n\r\n${text}`,
  );
  socket.destroy();
}

server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, "http://relay");
  const room = roomFromPath(url.pathname);
  if (!room) return reject(socket, 404, { error: "not_found" });
  if (!originAllowed(req.headers.origin)) return reject(socket, 403, { error: "origin_not_allowed" });

  const role = url.searchParams.get("role");
  if (!ROLES.includes(role)) return reject(socket, 400, { error: "invalid_role" });
  if (Number(url.searchParams.get("v") ?? PROTOCOL_VERSION) !== PROTOCOL_VERSION) {
    return reject(socket, 400, { error: "protocol_mismatch", protocol: PROTOCOL_VERSION });
  }
  const guestId = role === "guest" ? url.searchParams.get("id") ?? "" : null;
  if (role === "guest" && !GUEST_ID_PATTERN.test(guestId)) return reject(socket, 400, { error: "invalid_guest_id" });

  wss.handleUpgrade(req, socket, head, (ws) => join(ws, room, role, guestId));
});

// Keepalive: hosting proxies drop quiet connections, and this also clears out sockets that vanished without closing.
// The same timer forgets rooms that have been empty for a while.
const timer = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    try {
      ws.ping();
    } catch {
      // closing
    }
  }
  const now = Date.now();
  for (const [id, room] of rooms) if (room.emptySince && now - room.emptySince > EMPTY_ROOM_TTL_MS) rooms.delete(id);
}, KEEPALIVE_MS);

function shutdown() {
  clearInterval(timer);
  for (const ws of wss.clients) close(ws, 1012, "Relay restarting");
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000).unref();
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Relay listening on port ${PORT} (protocol v${PROTOCOL_VERSION}); allowed origins: ${ALLOWED_ORIGINS.join(", ") || "(none; only clients without an Origin header)"}`);
});
