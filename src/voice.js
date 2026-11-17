import { Streamer } from "@dank074/discord-video-stream";
import { ConnectionHealth } from "./connection-health.js";

// Upstream accepts null endpoints and resumes sockets even after stop().
export function guardVoiceConnection(connection, onClose, onRecovered) {
  if (!connection || connection.lifecycleGuarded) return;
  connection.lifecycleGuarded = true;
  connection.awaitingVoiceServer = false;
  let stopped = false;
  let serverUpdate = 0;
  let closing = null;
  let retryTimer = null;
  let retries = 0;
  const health = new ConnectionHealth(connection, {
    onClose,
    onRecovered: () => { retries = 0; onRecovered?.(); },
  });
  connection.transportHealth = health;
  const start = connection.start.bind(connection);
  const stop = connection.stop.bind(connection);
  const setTokens = connection.setTokens.bind(connection);

  const closeTransport = () => {
    clearTimeout(retryTimer);
    retryTimer = null;
    connection.status.started = false;
    connection.status.resuming = false;
    clearInterval(connection.interval);
    connection.interval = null;
    const socket = connection.ws;
    if (socket && socket.readyState !== 3 && !closing) {
      closing = new Promise((resolve) => {
        socket.addEventListener("close", resolve, { once: true });
      });
    }
    stop();
  };

  const startTransport = () => {
    if (stopped || connection.awaitingVoiceServer || !connection.server || !connection.token || health.failure) return;
    const oldSocket = connection.ws;
    const result = start();
    if (connection.ws && connection.ws !== oldSocket) {
      health.watch(connection.ws);
    }
    return result;
  };

  connection.start = () => {
    if (stopped || connection.awaitingVoiceServer || !connection.server || !connection.token || health.failure) return;
    if (connection.status.resuming && !connection.status.started) {
      if (!retryTimer) {
        const delay = Math.min(1000 * 2 ** Math.min(retries++, 4), 10000);
        retryTimer = setTimeout(() => {
          retryTimer = null;
          startTransport();
        }, delay);
        retryTimer.unref?.();
      }
      return;
    }
    return startTransport();
  };

  connection.stop = () => {
    stopped = true;
    serverUpdate++;
    health.stop();
    closeTransport();
  };

  connection.setTokens = (server, token) => {
    if (stopped) return;
    const update = ++serverUpdate;
    if (!server || !token) {
      connection.awaitingVoiceServer = true;
      health.waitForServer();
      connection.status.hasToken = false;
      connection.server = null;
      connection.token = null;
      closeTransport();
      return;
    }

    const apply = () => {
      if (stopped || update !== serverUpdate) return;
      closing = null;
      connection.awaitingVoiceServer = false;
      connection._closed = false;
      setTokens(server, token);
    };
    // Let the old socket's close handler finish before starting a new connection.
    if (closing) return closing.then(apply);
    apply();
  };
}

export async function leaveVoiceAndWait(streamer, timeoutMs = 5000) {
  if (!streamer.voiceConnection) return;

  const guildId = streamer.voiceConnection.guildId;
  const { client } = streamer;
  let onRaw;
  let timeout;
  try {
    await new Promise((resolve, reject) => {
      onRaw = (packet) => {
        if (packet.t === "VOICE_STATE_UPDATE" &&
            packet.d?.user_id === client.user?.id && packet.d?.channel_id === null &&
            (packet.d?.guild_id ?? null) === (guildId ?? null)) {
          resolve();
        }
      };
      client.on("raw", onRaw);
      timeout = setTimeout(() => reject(new Error("Voice leave acknowledgement timed out")), timeoutMs);
      streamer.leaveVoice();
    });
  } finally {
    clearTimeout(timeout);
    client.off("raw", onRaw);
  }
}

export async function joinVoiceWithTimeout(streamer, guildId, channelId, timeoutMs = 20000) {
  let timeout;
  try {
    return await Promise.race([
      streamer.joinVoice(guildId, channelId),
      new Promise((_, reject) => {
        timeout = setTimeout(() => reject(new Error(`Voice join timed out after ${timeoutMs / 1000}s`)), timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timeout);
  }
}

export async function waitForStreamReady(streamer, signal, timeoutMs = 20000) {
  let interval;
  let timeout;
  let onAbort;
  try {
    await new Promise((resolve, reject) => {
      onAbort = () => reject(signal.reason);
      const check = () => {
        if (signal.aborted) {
          onAbort();
        } else if (streamer.voiceConnection?.streamConnection?.webRtcConn?.ready) {
          resolve();
        }
      };
      signal.addEventListener("abort", onAbort, { once: true });
      interval = setInterval(check, 100);
      timeout = setTimeout(() => reject(new Error("Stream WebRTC connection timed out")), timeoutMs);
      check();
    });
  } finally {
    clearInterval(interval);
    clearTimeout(timeout);
    signal.removeEventListener("abort", onAbort);
  }
}

export class MutedStreamer extends Streamer {
  constructor(client, onConnectionClose) {
    super(client);
    this.onConnectionClose = onConnectionClose;
  }

  joinVoice(guildId, channelId) {
    const previousListeners = new Set(this._gatewayEmitter.listeners("VOICE_STATE_UPDATE"));
    const joining = super.joinVoice(guildId, channelId);
    // Upstream matches only user_id: another guild can otherwise replace our session.
    for (const listener of this._gatewayEmitter.listeners("VOICE_STATE_UPDATE")) {
      if (previousListeners.has(listener)) continue;
      this._gatewayEmitter.off("VOICE_STATE_UPDATE", listener);
      this._gatewayEmitter.on("VOICE_STATE_UPDATE", (data) => {
        if ((data.guild_id ?? null) === (guildId ?? null) && data.channel_id === channelId) {
          listener(data);
        }
      });
    }
    guardVoiceConnection(this.voiceConnection,
      (code) => this.onConnectionClose?.("voice", code, "closed"),
      () => this.onConnectionClose?.("voice", null, "recovered"));
    return joining;
  }

  createStream() {
    const creating = super.createStream();
    guardVoiceConnection(this.voiceConnection?.streamConnection,
      (code) => this.onConnectionClose?.("stream", code, "closed"),
      () => this.onConnectionClose?.("stream", null, "recovered"));
    return creating;
  }

  signalVideo(videoEnabled) {
    if (!this.voiceConnection) return;

    const { guildId, channelId } = this.voiceConnection;
    this.sendOpcode(4, {
      guild_id: guildId,
      channel_id: channelId,
      self_mute: true,
      self_deaf: true,
      self_video: videoEnabled,
    });
  }
}
