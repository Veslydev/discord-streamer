import { test, expect } from "bun:test";
import { EventEmitter } from "node:events";
import { MutedStreamer, guardVoiceConnection, leaveVoiceAndWait, joinVoiceWithTimeout, waitForStreamReady } from "../src/voice.js";

function createStreamer() {
  const packets = [];
  const client = new EventEmitter();
  client.ws = { broadcast: (packet) => packets.push(packet) };
  return { streamer: new MutedStreamer(client), packets };
}

test("does not send a voice update without a connection", () => {
  const { streamer, packets } = createStreamer();
  streamer.signalVideo(false);
  expect(packets).toEqual([]);
});

test.each([false, true])("mutes and deafens with video enabled: %s", (videoEnabled) => {
  const { streamer, packets } = createStreamer();
  Object.defineProperty(streamer, "voiceConnection", {
    value: { guildId: "guild", channelId: "channel" },
  });

  streamer.signalVideo(videoEnabled);

  expect(packets).toEqual([{
    op: 4,
    d: {
      guild_id: "guild",
      channel_id: "channel",
      self_mute: true,
      self_deaf: true,
      self_video: videoEnabled,
    },
  }]);
});

test("keeps mute and deafen flags when the voice connection changes", () => {
  const { streamer, packets } = createStreamer();
  let connection = { guildId: "guild", channelId: "first-channel" };
  Object.defineProperty(streamer, "voiceConnection", { get: () => connection });

  streamer.signalVideo(false);
  connection = { guildId: "guild", channelId: "second-channel" };
  streamer.signalVideo(false);

  expect(packets.map((packet) => packet.d.channel_id)).toEqual(["first-channel", "second-channel"]);
  for (const packet of packets) {
    expect(packet.d.self_mute).toBe(true);
    expect(packet.d.self_deaf).toBe(true);
  }
});

test("does not send an unnecessary disconnect on the first join", async () => {
  let left = false;
  await leaveVoiceAndWait({ leaveVoice: () => { left = true; } });
  expect(left).toBe(false);
});

test("waits for the account's leave acknowledgement before rejoining", async () => {
  const client = new EventEmitter();
  client.user = { id: "self" };
  let completed = false;
  const streamer = {
    client,
    voiceConnection: {},
    leaveVoice() {},
  };
  const leaving = leaveVoiceAndWait(streamer).then(() => { completed = true; });

  client.emit("raw", { t: "VOICE_STATE_UPDATE", d: { user_id: "other", channel_id: null } });
  client.emit("raw", { t: "VOICE_STATE_UPDATE", d: { user_id: "self", channel_id: "channel" } });
  await Promise.resolve();
  expect(completed).toBe(false);

  client.emit("raw", { t: "VOICE_STATE_UPDATE", d: { user_id: "self", channel_id: null } });
  await leaving;
  expect(completed).toBe(true);
  expect(client.listenerCount("raw")).toBe(0);
});

test("leave timeouts remove their event listener", async () => {
  const client = new EventEmitter();
  client.user = { id: "self" };
  await expect(leaveVoiceAndWait({ client, voiceConnection: {}, leaveVoice() {} }, 5))
    .rejects.toThrow("Voice leave acknowledgement timed out");
  expect(client.listenerCount("raw")).toBe(0);
});

test("voice join returns the connection", async () => {
  const connection = {};
  const streamer = { joinVoice: async () => connection };
  expect(await joinVoiceWithTimeout(streamer, "guild", "channel")).toBe(connection);
});

test("a voice join cannot wait forever", async () => {
  const streamer = { joinVoice: () => new Promise(() => {}) };
  await expect(joinVoiceWithTimeout(streamer, "guild", "channel", 5))
    .rejects.toThrow("Voice join timed out");
});

test("does not report stream readiness before WebRTC connects", async () => {
  const controller = new AbortController();
  const connection = { ready: false };
  const streamer = { voiceConnection: { streamConnection: { webRtcConn: connection } } };
  let completed = false;
  const waiting = waitForStreamReady(streamer, controller.signal).then(() => { completed = true; });
  await Promise.resolve();
  expect(completed).toBe(false);
  connection.ready = true;
  await waiting;
  expect(completed).toBe(true);
});

test("stream readiness can time out or be cancelled", async () => {
  const controller = new AbortController();
  await expect(waitForStreamReady({}, controller.signal, 5))
    .rejects.toThrow("Stream WebRTC connection timed out");
  controller.abort(new Error("cancelled"));
  await expect(waitForStreamReady({}, controller.signal)).rejects.toThrow("cancelled");
});

function createConnection(withSocket = true) {
  const starts = [];
  const socket = new EventTarget();
  socket.readyState = 1;
  const connection = {
    status: { started: withSocket, resuming: false, hasToken: true },
    server: "old.example",
    token: "old-token",
    ws: withSocket ? socket : null,
    start() {
      starts.push(this.server);
      this.status.started = true;
    },
    stop() {
      this._closed = true;
      if (this.ws) this.ws.readyState = 2;
    },
    setTokens(server, token) {
      this.server = server;
      this.token = token;
      this.status.hasToken = true;
      this.start();
    },
  };
  socket.addEventListener("close", () => {
    // Reproduce the upstream resume decision.
    const wasStarted = connection.status.started;
    connection.status.started = false;
    if (wasStarted) connection.start();
  });
  guardVoiceConnection(connection);
  const finishClose = () => {
    socket.readyState = 3;
    socket.dispatchEvent(new Event("close"));
  };
  return { connection, starts, finishClose };
}

test("null voice endpoints close the old transport without connecting to null", () => {
  const { connection, starts, finishClose } = createConnection();
  connection.setTokens(null, "token");
  finishClose();
  connection.start();
  expect(starts).toEqual([]);
  expect(connection.awaitingVoiceServer).toBe(true);
  expect(connection.status.started).toBe(false);
  expect(connection.status.hasToken).toBe(false);
});

test("server allocation resumes only after the old socket closes", async () => {
  const { connection, starts, finishClose } = createConnection();
  connection.setTokens(null, "token");
  const resuming = connection.setTokens("new.example", "new-token");
  expect(starts).toEqual([]);
  finishClose();
  await resuming;
  expect(starts).toEqual(["new.example"]);
  expect(connection.awaitingVoiceServer).toBe(false);
  expect(connection._closed).toBe(false);
});

test("only the latest server update is applied during socket shutdown", async () => {
  const { connection, starts, finishClose } = createConnection();
  connection.setTokens(null, "token");
  const outdated = connection.setTokens("outdated.example", "token-1");
  const current = connection.setTokens("current.example", "token-2");
  finishClose();
  await Promise.all([outdated, current]);
  expect(starts).toEqual(["current.example"]);
});

test("stopping prevents both queued allocations and websocket auto-resume", async () => {
  const { connection, starts, finishClose } = createConnection();
  connection.setTokens(null, "token");
  const pending = connection.setTokens("new.example", "new-token");
  connection.stop();
  finishClose();
  await pending;
  connection.start();
  connection.setTokens("another.example", "token");
  expect(starts).toEqual([]);
  expect(connection.status.started).toBe(false);
});

test("an initially null allocation can be replaced without an existing socket", () => {
  const { connection, starts } = createConnection(false);
  connection.setTokens(null, "token");
  connection.setTokens("allocated.example", "new-token");
  expect(starts).toEqual(["allocated.example"]);
});

test("the streamer applies lifecycle guards to both voice and stream connections", () => {
  const { streamer, packets } = createStreamer();
  streamer.client.user = { id: "self" };
  streamer.joinVoice("guild", "channel");
  expect(streamer.voiceConnection.lifecycleGuarded).toBe(true);
  expect(packets.at(-1).d.self_mute).toBe(true);

  streamer.voiceConnection.session_id = "session";
  streamer.createStream();
  expect(streamer.voiceConnection.streamConnection.lifecycleGuarded).toBe(true);
  streamer.leaveVoice();
});

test("another guild or channel cannot overwrite the voice transport session", () => {
  const { streamer } = createStreamer();
  streamer.client.user = { id: "self" };
  streamer.joinVoice("guild", "channel");
  const send = (guild, channel, session) => streamer.client.emit("raw", {
    t: "VOICE_STATE_UPDATE",
    d: { user_id: "self", guild_id: guild, channel_id: channel, session_id: session },
  });
  send("guild", "channel", "correct-session");
  send("other-guild", "other-channel", "wrong-session");
  send("guild", "other-channel", "also-wrong");
  expect(streamer.voiceConnection.session_id).toBe("correct-session");
  streamer.leaveVoice();
});
