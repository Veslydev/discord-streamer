import { test, expect } from "bun:test";
import { ConnectionHealth, isMediaConnectionReady, shouldReconnectMedia } from "../src/connection-health.js";

function socket() {
  const ws = new EventTarget();
  ws.readyState = 1;
  ws.packet = (op, d) => {
    const event = new Event("message");
    event.data = JSON.stringify({ op, d });
    ws.dispatchEvent(event);
  };
  ws.disconnect = (code = 1006) => {
    ws.readyState = 3;
    const event = new Event("close");
    event.code = code;
    ws.dispatchEvent(event);
  };
  return ws;
}

function createHealth() {
  let now = 1000;
  const recovered = [];
  const closed = [];
  const connection = { ws: socket(), webRtcConn: { ready: true } };
  const health = new ConnectionHealth(connection, {
    now: () => now,
    onClose: (code) => closed.push(code),
    onRecovered: () => recovered.push(true),
  });
  connection.transportHealth = health;
  health.watch(connection.ws);
  const ready = () => { connection.ws.packet(8, { heartbeat_interval: 5000 }); connection.ws.packet(2); };
  return { connection, health, recovered, closed, ready, advance: (ms) => { now += ms; } };
}

test("OPEN alone is not proof of a healthy Discord voice session", () => {
  const t = createHealth();
  expect(isMediaConnectionReady(t.connection)).toBe(false);
  t.connection.ws.packet(8, { heartbeat_interval: 5000 });
  expect(isMediaConnectionReady(t.connection)).toBe(false);
  t.connection.ws.packet(2);
  expect(isMediaConnectionReady(t.connection)).toBe(true);
  expect(t.recovered).toEqual([]);
});

test("resume is logged only after Discord confirmation and connected WebRTC", () => {
  const t = createHealth();
  t.ready();
  t.connection.ws.disconnect();
  expect(t.closed).toEqual([1006]);
  expect(shouldReconnectMedia(t.connection)).toBe(false);
  t.connection.ws = socket();
  t.health.watch(t.connection.ws);
  t.connection.webRtcConn.ready = false;
  t.connection.ws.packet(8, { heartbeat_interval: 5000 });
  expect(t.recovered).toEqual([]);
  t.connection.ws.packet(9);
  expect(t.recovered).toEqual([]);
  t.connection.webRtcConn.ready = true;
  expect(isMediaConnectionReady(t.connection)).toBe(true);
  expect(t.recovered).toEqual([true]);
  t.health.check();
  expect(t.recovered).toEqual([true]);
});

test("an open socket that never confirms resume eventually needs a fresh join", () => {
  const t = createHealth();
  t.ready();
  t.connection.ws.disconnect();
  t.connection.ws = socket();
  t.health.watch(t.connection.ws);
  t.connection.ws.packet(8, { heartbeat_interval: 5000 });
  t.advance(30001);
  expect(shouldReconnectMedia(t.connection)).toBe(true);
  expect(t.health.failure).toBe("transport recovery timed out");
});

test("missed heartbeat acknowledgements detect a half-open connection", () => {
  const t = createHealth();
  t.ready();
  t.advance(14000);
  t.connection.ws.packet(6, { t: 123 });
  t.advance(14000);
  expect(isMediaConnectionReady(t.connection)).toBe(true);
  t.advance(1001);
  expect(shouldReconnectMedia(t.connection)).toBe(true);
  expect(t.health.failure).toBe("heartbeat ACK timed out");
});

test("a dead WebRTC transport is recovered even if its socket stays open", () => {
  const t = createHealth();
  t.ready();
  t.connection.webRtcConn.ready = false;
  expect(shouldReconnectMedia(t.connection)).toBe(false);
  t.advance(30001);
  expect(shouldReconnectMedia(t.connection)).toBe(true);
});

test("voice server reallocation cannot suppress the watchdog forever", () => {
  const t = createHealth();
  t.ready();
  t.connection.awaitingVoiceServer = true;
  t.health.waitForServer();
  expect(shouldReconnectMedia(t.connection)).toBe(false);
  t.advance(30001);
  expect(shouldReconnectMedia(t.connection)).toBe(true);
  expect(t.health.failure).toBe("voice server allocation timed out");
});

test("invalid sessions fail immediately and intentional stops do not log failures", () => {
  const t = createHealth();
  t.ready();
  t.connection.ws.disconnect(4006);
  expect(shouldReconnectMedia(t.connection)).toBe(true);
  const stopped = createHealth();
  stopped.health.stop();
  stopped.connection.ws.disconnect();
  expect(stopped.closed).toEqual([]);
});
