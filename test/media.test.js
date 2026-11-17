import { test, expect } from "bun:test";
import { getStreamMediaOptions } from "../src/media.js";

test("the looping file is read in real time without reducing video or audio quality", () => {
  expect(getStreamMediaOptions()).toEqual({
    videoCodec: "H264",
    bitrateVideo: 150,
    bitrateVideoMax: 200,
    frameRate: 10,
    width: 480,
    height: 270,
    includeAudio: true,
    customInputOptions: ["-re", "-stream_loop", "-1"],
  });
});

test("configured framerate and bitrate are preserved", () => {
  const options = getStreamMediaOptions({ FPS: 15, BITRATE_KBPS: 250 });
  expect(options.frameRate).toBe(15);
  expect(options.bitrateVideo).toBe(250);
  expect(options.bitrateVideoMax).toBe(300);
});

test("existing minimum framerate and bitrate limits are preserved", () => {
  const options = getStreamMediaOptions({ FPS: 1, BITRATE_KBPS: 10 });
  expect(options.frameRate).toBe(5);
  expect(options.bitrateVideo).toBe(100);
  expect(options.bitrateVideoMax).toBe(150);
});
