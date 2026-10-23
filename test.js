// End-to-end check of the relay: start it on a spare port and play host and guests against it.
// Run with: npm test   (or: RELAY_URL=https://your-relay.onrender.com npm test, to check a deployed one)
import { spawn } from "node:child_process";
import assert from "node:assert/strict";
import WebSocket from "ws";
import { createGuestId, decodePackets, decodeSeatPackets, encodePackets, encodeSeatPackets } from "./protocol.js";

const external = process.env.RELAY_URL;
const port = 18000 + Math.floor(Math.random() * 1000);
const base = external ?? `http://127.0.0.1:${port}`;
const wsBase = base.replace(/^http/, "ws");
const ORIGIN = "https://vel.gg";

let child;
if (!external) {
  child = spawn(process.execPath, ["server.js"], { env: { ...process.env, PORT: String(port) }, stdio: ["ignore", "pipe", "inherit"] });
  await new Promise((resolve) => child.stdout.on("data", (d) => String(d).includes("listening") && resolve()));
}

const room = () => `test-${Math.random().toString(36).slice(2, 10)}`;

function connect(roomId, role, { id, origin = ORIGIN } = {}) {
  const url = `${wsBase}/v1/rooms/${roomId}?role=${role}&v=3${role === "guest" ? `&id=${id}` : ""}`;
  const ws = new WebSocket(url, { headers: origin ? { Origin: origin } : {} });
  ws.inbox = [];
  ws.waiters = [];
  ws.on("message", (data, isBinary) => {
    const item = isBinary ? { binary: new Uint8Array(data) } : JSON.parse(data.toString());
    const i = ws.waiters.findIndex((w) => w.match(item));
    if (i >= 0) ws.waiters.splice(i, 1)[0].resolve(item);
    else ws.inbox.push(item);
  });
  ws.next = (match = () => true, ms = 3000) => {
    const i = ws.inbox.findIndex(match);
    if (i >= 0) return Promise.resolve(ws.inbox.splice(i, 1)[0]);
    return new Promise((resolve, reject) => {
      const w = { match, resolve };
      ws.waiters.push(w);
      setTimeout(() => reject(new Error("timed out waiting for a message")), ms);
    });
  };
  ws.closed = new Promise((resolve) => ws.on("close", (code) => resolve(code)));
  ws.failed = new Promise((resolve) => ws.on("unexpected-response", (_, res) => resolve(res.statusCode)));
  ws.on("error", () => {});
  return ws;
}

const results = [];
async function check(name, fn) {
  try {
    await fn();
    results.push(`ok   ${name}`);
  } catch (error) {
    results.push(`FAIL ${name}: ${error.message}`);
    process.exitCode = 1;
  }
}

await check("health and usage", async () => {
  assert.deepEqual(await (await fetch(`${base}/health`)).json(), { ok: true, protocol: 3 });
  const usage = await (await fetch(`${base}/usage`, { headers: { Origin: ORIGIN } })).json();
  assert.equal(usage.fraction, 0);
});

await check("host and guest join, control messages both ways", async () => {
  const r = room();
  const host = connect(r, "host");
  assert.deepEqual(await host.next(), { t: "welcome", v: 3, role: "host", guests: [] });
  const guest = connect(r, "guest", { id: createGuestId() });
  assert.deepEqual(await guest.next(), { t: "welcome", v: 3, role: "guest", seat: 1, peer: true });
  assert.deepEqual(await host.next(), { t: "peer", seat: 1, present: true });
  guest.send(JSON.stringify({ t: "hello", n: 1 }));
  assert.deepEqual(await host.next((m) => m.t === "hello"), { t: "hello", n: 1, from: 1 });
  host.send(JSON.stringify({ t: "hi", to: 1 }));
  assert.deepEqual(await guest.next((m) => m.t === "hi"), { t: "hi", to: 1 });
  host.send(JSON.stringify({ t: "all" }));
  assert.deepEqual(await guest.next((m) => m.t === "all"), { t: "all" });
  guest.send(JSON.stringify({ t: "welcome" }));
  assert.equal((await guest.next((m) => m.t === "error")).code, "bad_message");
  guest.close();
  assert.deepEqual(await host.next((m) => m.t === "peer"), { t: "peer", seat: 1, present: false });
  host.close();
});

await check("binary packets are regrouped per seat", async () => {
  const r = room();
  const host = connect(r, "host");
  await host.next();
  const g1 = connect(r, "guest", { id: createGuestId() });
  const g2 = connect(r, "guest", { id: createGuestId() });
  const s1 = (await g1.next()).seat;
  const s2 = (await g2.next()).seat;
  assert.deepEqual([s1, s2].sort(), [1, 2]);
  await host.next((m) => m.t === "peer");
  await host.next((m) => m.t === "peer");

  g1.send(encodePackets([Uint8Array.of(1, 2, 3), Uint8Array.of(4)]));
  const up = decodeSeatPackets((await host.next((m) => m.binary)).binary);
  assert.deepEqual(up.map((i) => [i.seat, [...i.packet]]), [[s1, [1, 2, 3]], [s1, [4]]]);

  host.send(encodeSeatPackets([{ seat: s1, packet: Uint8Array.of(9) }, { seat: s2, packet: Uint8Array.of(8, 8) }, { seat: s1, packet: Uint8Array.of(7) }]));
  const d1 = decodePackets((await g1.next((m) => m.binary)).binary).map((p) => [...p]);
  const d2 = decodePackets((await g2.next((m) => m.binary)).binary).map((p) => [...p]);
  assert.deepEqual(d1, [[9], [7]]);
  assert.deepEqual(d2, [[8, 8]]);
  [host, g1, g2].forEach((s) => s.close());
});

await check("room full after three guests", async () => {
  const r = room();
  const host = connect(r, "host");
  await host.next();
  const guests = [1, 2, 3].map(() => connect(r, "guest", { id: createGuestId() }));
  for (const g of guests) await g.next();
  const extra = connect(r, "guest", { id: createGuestId() });
  assert.deepEqual(await extra.next(), { t: "error", code: "room_full" });
  assert.equal(await extra.closed, 4004);
  [host, ...guests].forEach((s) => s.close());
});

await check("reconnecting guest keeps its seat and replaces the old socket", async () => {
  const r = room();
  const host = connect(r, "host");
  await host.next();
  const idA = createGuestId();
  const a = connect(r, "guest", { id: idA });
  const b = connect(r, "guest", { id: createGuestId() });
  const seatA = (await a.next()).seat;
  await b.next();
  const a2 = connect(r, "guest", { id: idA });
  assert.equal((await a2.next()).seat, seatA);
  assert.deepEqual(await a.next((m) => m.t === "error"), { t: "error", code: "replaced" });
  assert.equal(await a.closed, 4001);
  a2.close();
  await a2.closed;
  const a3 = connect(r, "guest", { id: idA });
  assert.equal((await a3.next()).seat, seatA);
  [host, b, a3].forEach((s) => s.close());
});

await check("host leaving and returning is announced", async () => {
  const r = room();
  const host = connect(r, "host");
  await host.next();
  const guest = connect(r, "guest", { id: createGuestId() });
  await guest.next();
  host.close();
  assert.deepEqual(await guest.next((m) => m.t === "peer"), { t: "peer", present: false });
  const host2 = connect(r, "host");
  assert.deepEqual(await host2.next(), { t: "welcome", v: 3, role: "host", guests: [1] });
  assert.deepEqual(await guest.next((m) => m.t === "peer"), { t: "peer", present: true });
  [host2, guest].forEach((s) => s.close());
});

await check("bad requests are refused", async () => {
  assert.equal(await connect(room(), "host", { origin: "https://evil.example" }).failed, 403);
  assert.equal(await connect(room(), "guest", { id: "x" }).failed, 400);
  const bad = new WebSocket(`${wsBase}/v1/rooms/${room()}?role=host&v=2`, { headers: { Origin: ORIGIN } });
  bad.on("error", () => {});
  assert.equal(await new Promise((r) => bad.on("unexpected-response", (_, res) => r(res.statusCode))), 400);
});

await check("round trip guest -> host -> guest", async () => {
  const r = room();
  const host = connect(r, "host");
  await host.next();
  const guest = connect(r, "guest", { id: createGuestId() });
  await guest.next();
  await host.next((m) => m.t === "peer");
  host.on("message", (d, bin) => {
    if (bin) return;
    const m = JSON.parse(d.toString());
    if (m.t === "ping") host.send(JSON.stringify({ t: "pong", to: m.from }));
  });
  const times = [];
  for (let i = 0; i < 5; i++) {
    const s = performance.now();
    guest.send(JSON.stringify({ t: "ping" }));
    await guest.next((m) => m.t === "pong");
    times.push(Math.round(performance.now() - s));
  }
  results.push(`     round trips (ms): ${times.join(", ")}`);
  [host, guest].forEach((s) => s.close());
});

console.log(results.join("\n"));
child?.kill("SIGTERM");
setTimeout(() => process.exit(), 500).unref();
