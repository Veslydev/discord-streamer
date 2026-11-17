import { test, expect } from "bun:test";
import { EventEmitter } from "node:events";
import { WatcherManager } from "../src/watcher.js";

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
