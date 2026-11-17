import { test, expect } from "bun:test";
import { EventEmitter } from "node:events";
import { WatcherManager, StreamWatchSession } from "../src/watcher.js";

test("viewer health requires an open socket and connected WebRTC", () => {
  const watcher = new WatcherManager(new EventEmitter(), "guild", "channel");
  const connection = { status: { started: true }, ws: { readyState: 1 }, webRtcConn: { ready: false } };
  Object.defineProperty(watcher.streamer, "voiceConnection", { value: connection });
  expect(watcher.isVoiceReady()).toBe(false);
  connection.webRtcConn.ready = true;
  expect(watcher.isVoiceReady()).toBe(true);
  connection.ws.readyState = 3;
  expect(watcher.isVoiceReady()).toBe(false);
});

test("reconnecting clears stale stream credentials before closing sessions", () => {
  const watcher = new WatcherManager(new EventEmitter(), "guild", "channel");
  watcher.sessionId = "old-session";
  watcher.pendingData.set("stream", { token: "old-token" });
  let stopped = false;
  watcher.activeSessions.set("stream", {
    stop() {
      stopped = true;
      expect(watcher.pendingData.size).toBe(0);
      expect(watcher.activeSessions.size).toBe(0);
    },
  });
  watcher.clearStreamSessions();
  expect(stopped).toBe(true);
  expect(watcher.sessionId).toBe(null);
});

test("unrelated voice states cannot overwrite the viewer session", () => {
  const client = new EventEmitter();
  client.user = { id: "viewer" };
  const watcher = new WatcherManager(client, "guild", "channel");
  watcher.isRunning = true;
  const send = (guild, channel, session) => client.emit("raw", {
    t: "VOICE_STATE_UPDATE",
    d: { user_id: "viewer", guild_id: guild, channel_id: channel, session_id: session },
  });
  send("guild", "channel", "correct-session");
  send("other-guild", "other-channel", "wrong-session");
  send("guild", null, "leave-session");
  expect(watcher.sessionId).toBe("correct-session");
});

function createSession() {
  const closed = [];
  const sent = [];
  const session = new StreamWatchSession({
    streamKey: "guild:guild:channel:streamer", endpoint: "voice.example", token: "test-token",
    rtcServerId: "100000", userId: "viewer", sessionId: "session",
    onClose: (key, reason) => closed.push({ key, reason }),
  });
  session.connection.sendOpcode = (op, d) => sent.push({ op, d });
  return { session, closed, sent };
}

test("stream watch advertises real DAVE support and uses the RTC channel context", () => {
  const t = createSession();
  t.session.connection.session_id = "session";
  t.session.connection.token = "test-token";
  t.session.connection.identify();
  expect(t.sent[0].op).toBe(0);
  expect(t.sent[0].d.max_dave_protocol_version).toBeGreaterThan(0);
  expect(t.sent[0].d.server_id).toBe("100000");
  expect(t.session.connection.daveChannelId).toBe("99999");
  t.session.stop();
});

test("a stream watch with missed ACKs is closed once and can be retried", () => {
  const t = createSession();
  t.session.connection.transportHealth.failure = "heartbeat ACK timed out";
  t.session.checkHealth();
  t.session.checkHealth();
  expect(t.closed).toEqual([{ key: "guild:guild:channel:streamer", reason: "heartbeat ACK timed out" }]);
  expect(t.session.isClosed).toBe(true);
  expect(t.session.isReady).toBe(false);
});

test("stream watch reports readiness only after the complete transport is healthy", () => {
  const t = createSession();
  let ready = 0;
  t.session.onReady = () => ready++;
  t.session.connection.transportHealth.check = () => "recovering";
  t.session.checkHealth();
  expect(ready).toBe(0);
  t.session.connection.transportHealth.check = () => "healthy";
  t.session.checkHealth();
  t.session.checkHealth();
  expect(ready).toBe(1);
  t.session.stop();
});

test("watch reconnects using cached credentials even without a fresh gateway event", () => {
  const client = new EventEmitter();
  client.ws = { broadcast() {} };
  const watcher = new WatcherManager(client, "guild", "channel");
  watcher.isRunning = true;
  watcher.isVoiceReady = () => true;
  const requested = [];
  watcher.checkAndConnectSession = (key) => requested.push(key);
  watcher.watchStream("stream");
  expect(requested).toEqual(["stream"]);
});
