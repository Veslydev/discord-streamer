import { test, expect, spyOn } from "bun:test";
import { setTimeout as delay } from "node:timers/promises";
import { prepareStream, playStream } from "@dank074/discord-video-stream";
import { Demuxer } from "node-av";
import { getStreamMediaOptions, injectFFmpegPath } from "../src/media.js";

injectFFmpegPath();
const input = new URL("../assets/dummy_stream.mp4", import.meta.url).pathname;

test("repeated cancellation closes native demuxers and stops all frame delivery", async () => {
  const opened = [];
  const originalOpen = Demuxer.open;
  const spy = spyOn(Demuxer, "open").mockImplementation(async (...args) => {
    const demuxer = await originalOpen.call(Demuxer, ...args);
    opened.push(demuxer);
    return demuxer;
  });
  try {
    for (let cycle = 0; cycle < 3; cycle++) {
      let frames = 0;
      let audio = 0;
      let stopped = 0;
      const controller = new AbortController();
      const conn = {
        setPacketizer() {},
        sendVideoFrame() { frames++; },
        sendAudioFrame() { audio++; },
        mediaConnection: { setSpeaking() {}, setVideoAttributes() {} },
      };
      const voiceConnection = {};
      const streamer = {
        voiceConnection,
        async createStream() { voiceConnection.streamConnection = {}; return conn; },
        stopStream() { stopped++; },
      };
      const producer = prepareStream(input, getStreamMediaOptions(), controller.signal);
      producer.promise.catch(() => {});
      const playback = playStream(producer.output, streamer, {}, controller.signal);
      playback.catch(() => {});
      try {
        const deadline = Date.now() + 10000;
        while (frames < 3 && Date.now() < deadline) await delay(50);
        expect(frames).toBeGreaterThanOrEqual(3);
        expect(audio).toBeGreaterThan(0);
        if (cycle === 2) streamer.voiceConnection = { streamConnection: {} };
        controller.abort(new Error("test cancellation"));
        await expect(playback).rejects.toThrow("test cancellation");
        const atAbort = { frames, audio };
        await delay(300);
        expect({ frames, audio }).toEqual(atAbort);
        expect(stopped).toBe(cycle === 2 ? 0 : 1);
        expect(opened.at(-1).isClosed).toBe(true);
        expect(opened.at(-1).demuxThreadActive).toBe(false);
        expect(opened.at(-1).packetQueues.size).toBe(0);
      } finally {
        controller.abort();
        await Promise.allSettled([producer.promise, playback]);
      }
    }
  } finally {
    spy.mockRestore();
  }
}, 40000);

test("cancellation while stream creation is pending still closes the demuxer", async () => {
  const controller = new AbortController();
  let started;
  const creating = new Promise((resolve) => { started = resolve; });
  let stopped = false;
  const voiceConnection = {};
  const streamer = {
    voiceConnection,
    createStream() {
      voiceConnection.streamConnection = {};
      started();
      return new Promise(() => {});
    },
    stopStream() { stopped = true; },
  };
  const producer = prepareStream(input, getStreamMediaOptions(), controller.signal);
  producer.promise.catch(() => {});
  const playback = playStream(producer.output, streamer, {}, controller.signal);
  playback.catch(() => {});
  try {
    await creating;
    controller.abort(new Error("cancel pending join"));
    await expect(playback).rejects.toThrow("cancel pending join");
    expect(stopped).toBe(true);
  } finally {
    controller.abort();
    await Promise.allSettled([producer.promise, playback]);
  }
}, 15000);
