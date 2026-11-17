import { test, expect } from "bun:test";
import { StreamEngine } from "../src/engine.js";

function createAttempt() {
  let rejectJoin;
  let markJoining;
  const joining = new Promise((resolve) => { markJoining = resolve; });
  const joinResult = new Promise((_, reject) => { rejectJoin = reject; });
  const retries = [];
  const logs = [];
  const engine = {
    isRunning: true,
    config: { GUILD_ID: "guild", CHANNEL_ID: "channel" },
    dashboard: { addLog: (message) => logs.push(message) },
    viewerManager: { stopSession() {} },
    syncDashboardStreamers() {},
    triggerViewerWatch() {},
    scheduleStreamerReconnect: (_, delay) => retries.push(delay),
  };
  const streamer = {
    user: { id: "self", username: "test" },
    streamer: {
      stopStream() {},
      joinVoice() {
        markJoining();
        return joinResult;
      },
    },
    telemetry: { async onStreamEnded() {} },
    isStartingBroadcast: false,
    isStreaming: false,
    streamCancelController: null,
  };
  const run = () => StreamEngine.prototype.startStreamerBroadcast.call(engine, streamer);
  return { engine, streamer, run, joining, rejectJoin, retries, logs };
}

test("an old failed join cannot reset or reconnect a newer broadcast attempt", async () => {
  const attempt = createAttempt();
  const running = attempt.run();
  await attempt.joining;
  const oldController = attempt.streamer.streamCancelController;
  const newController = new AbortController();
  attempt.streamer.streamCancelController = newController;
  attempt.streamer.isStreaming = true;
  attempt.streamer.isStartingBroadcast = true;

  attempt.rejectJoin(new Error("old connection failed"));
  await running;

  expect(attempt.streamer.isStreaming).toBe(true);
  expect(attempt.streamer.isStartingBroadcast).toBe(true);
  expect(newController.signal.aborted).toBe(false);
  expect(oldController.signal.aborted).toBe(true);
  expect(attempt.retries).toEqual([]);
});

test("a failed join cleans up and schedules a single retry", async () => {
  const attempt = createAttempt();
  const running = attempt.run();
  await attempt.joining;
  attempt.rejectJoin(new Error("connection failed"));
  await running;

  expect(attempt.streamer.streamCancelController.signal.aborted).toBe(true);
  expect(attempt.streamer.isStartingBroadcast).toBe(false);
  expect(attempt.streamer.isStreaming).toBe(false);
  expect(attempt.retries).toEqual([10000]);
});

test("intentional cancellation is not treated as a new stream error", async () => {
  const attempt = createAttempt();
  const running = attempt.run();
  await attempt.joining;
  attempt.streamer.streamCancelController.abort();
  attempt.rejectJoin(new Error("The operation was aborted."));
  await running;

  expect(attempt.retries).toEqual([]);
  expect(attempt.logs).toEqual([]);
  expect(attempt.streamer.isStartingBroadcast).toBe(false);
});

function createWatchdog({ healthy = false, joining = false, waitingForServer = false } = {}) {
  const retries = [];
  const engine = {
    isRunning: true,
    streamers: [],
    viewerReconnectTimer: null,
    viewerManager: {
      streamer: { voiceConnection: { awaitingVoiceServer: waitingForServer } },
      isJoining: joining,
      isVoiceReady: () => healthy,
      getWatchingCount: () => 0,
    },
    dashboard: { addLog() {}, setViewerStatus() {} },
    scheduleViewerReconnect(delay, onlyIfUnhealthy) {
      retries.push({ delay, onlyIfUnhealthy });
      this.viewerReconnectTimer = {};
    },
    triggerViewerWatch() {},
  };
  return { engine, retries, check: () => StreamEngine.prototype.checkConnections.call(engine) };
}

test("the viewer watchdog catches voice transport loss without a gateway disconnect", () => {
  const watchdog = createWatchdog();
  watchdog.check();
  watchdog.check();
  expect(watchdog.retries).toEqual([{ delay: 5000, onlyIfUnhealthy: true }]);
});

test.each([
  { healthy: true },
  { joining: true },
  { waitingForServer: true },
])("viewer recovery does not interrupt healthy, joining, or reallocating connections: %j", (options) => {
  const watchdog = createWatchdog(options);
  watchdog.check();
  expect(watchdog.retries).toEqual([]);
});
