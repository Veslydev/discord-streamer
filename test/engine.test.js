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
      streamer: { voiceConnection: {
        transportHealth: { check: () => waitingForServer ? "recovering" : healthy ? "healthy" : "failed" },
      } },
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

function createVoiceEvent() {
  const retries = [];
  const streamer = {
    user: { id: "self", username: "test" },
    isStreaming: true,
    streamCancelController: new AbortController(),
  };
  const engine = {
    isRunning: true,
    config: { GUILD_ID: "target-guild", CHANNEL_ID: "target-channel" },
    dashboard: { addLog() {} },
    scheduleStreamerReconnect: (_, delay) => retries.push(delay),
    triggerViewerWatch() {},
    syncDashboardStreamers() {},
  };
  const dispatch = (guild, before, after) => StreamEngine.prototype.handleStreamerVoiceState.call(
    engine, streamer, { channelId: before }, { id: "self", guild: { id: guild }, channelId: after }
  );
  return { streamer, retries, dispatch };
}

test("voice activity in another guild cannot cancel the broadcast", () => {
  const event = createVoiceEvent();
  event.dispatch("other-guild", "other-channel", null);
  event.dispatch("other-guild", null, "other-channel");
  expect(event.retries).toEqual([]);
  expect(event.streamer.isStreaming).toBe(true);
  expect(event.streamer.streamCancelController.signal.aborted).toBe(false);
});

test("mute updates while outside the channel do not repeatedly reconnect", () => {
  const event = createVoiceEvent();
  event.dispatch("target-guild", "other-channel", "other-channel");
  expect(event.retries).toEqual([]);
});

test("real moves and disconnects still cancel the old broadcast and recover", () => {
  for (const [channel, delay] of [[null, 5000], ["other-channel", 3000]]) {
    const event = createVoiceEvent();
    event.dispatch("target-guild", "target-channel", channel);
    expect(event.retries).toEqual([delay]);
    expect(event.streamer.isStreaming).toBe(false);
    expect(event.streamer.streamCancelController.signal.aborted).toBe(true);
  }
});

test.each(["recovering", "failed"])("stream watchdog respects transport health: %s", (state) => {
  const watchdog = createWatchdog({ healthy: true });
  const retries = [];
  const controller = new AbortController();
  const streamer = {
    isStreaming: true,
    user: { username: "test" },
    client: { user: {} },
    streamCancelController: controller,
    streamer: { voiceConnection: {
      transportHealth: { check: () => "healthy" },
      streamConnection: {
        ws: { readyState: 1 }, webRtcConn: { ready: true },
        transportHealth: { check: () => state, failure: state === "failed" ? "heartbeat ACK timed out" : null },
      },
    } },
  };
  watchdog.engine.streamers = [streamer];
  watchdog.engine.scheduleStreamerReconnect = (_, delay) => retries.push(delay);
  watchdog.check();
  expect(retries).toEqual(state === "failed" ? [2000] : []);
  expect(controller.signal.aborted).toBe(state === "failed");
});
