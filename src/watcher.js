import { StreamConnection } from "@dank074/discord-video-stream";
import { MutedStreamer, guardVoiceConnection, leaveVoiceAndWait, joinVoiceWithTimeout } from "./voice.js";
import { isMediaConnectionReady, shouldReconnectMedia } from "./connection-health.js";

export class StreamWatchSession {
  constructor({ streamer, streamKey, endpoint, token, rtcServerId, userId, sessionId, onReady, onClose, onTransportEvent }) {
    this.streamKey = streamKey;
    this.endpoint = endpoint.replace(":443", "");
    this.token = token;
    this.rtcServerId = rtcServerId;
    this.userId = userId;
    this.sessionId = sessionId;
    this.onReady = onReady;
    this.onClose = onClose;

    this.healthTimer = null;
    this.readyReported = false;
    this.isClosed = false;
    const [, guildId, channelId] = streamKey.split(":");
    // Use the library's full DAVE + WebRTC handshake, not a bare IDENTIFY socket.
    this.connection = new StreamConnection(streamer, guildId, userId, channelId, () => this.checkHealth());
    this.connection.serverId = rtcServerId;
    this.connection.streamKey = streamKey;
    guardVoiceConnection(this.connection,
      (code) => onTransportEvent?.(`socket closed (code ${code}); checking recovery...`, "warn"),
      () => onTransportEvent?.("connection recovered (Discord session + WebRTC verified)", "success"));
  }

  get isReady() {
    return !this.isClosed && isMediaConnectionReady(this.connection);
  }

  updateServer(endpoint, token) {
    if (this.isClosed) return;
    this.endpoint = endpoint?.replace(":443", "") || null;
    this.token = token;
    this.connection.setTokens(this.endpoint, token);
  }

  start() {
    if (this.isClosed || this.healthTimer) return;
    try {
      this.connection.setSession(this.sessionId);
      this.connection.setTokens(this.endpoint, this.token);
      this.healthTimer = setInterval(() => this.checkHealth(), 1000);
      this.healthTimer.unref?.();
    } catch {
      this.fail("stream watch socket could not start");
    }
  }

  checkHealth() {
    if (this.isClosed) return;
    if (shouldReconnectMedia(this.connection)) {
      this.fail(this.connection.transportHealth.failure || "stream watch transport unavailable");
    } else if (this.isReady && !this.readyReported) {
      this.readyReported = true;
      this.onReady?.(this.streamKey);
    }
  }

  fail(reason) {
    if (this.isClosed) return;
    this.stop();
    this.onClose?.(this.streamKey, reason);
  }

  stop() {
    this.isClosed = true;
    clearInterval(this.healthTimer);
    this.healthTimer = null;
    this.connection.stop();
  }
}

export class WatcherManager {
  constructor(client, guildId, channelId, dashboard) {
    this.client = client;
    this.guildId = guildId;
    this.channelId = channelId;
    this.dashboard = dashboard;

    this.streamer = new MutedStreamer(client, (type, code, event) => {
      this.dashboard?.addLog(event === "recovered"
        ? `Viewer ${type} connection recovered (Discord session + WebRTC verified)`
        : `Viewer ${type} socket closed (code ${code}); checking recovery...`,
      event === "recovered" ? "success" : "warn");
    });
    this.activeSessions = new Map(); // streamKey -> StreamWatchSession
    this.pendingData = new Map(); // streamKey -> { rtcServerId, endpoint, token }
    this.sessionId = null;
    this.isRunning = false;
    this.isJoining = false;

    this.setupListeners();
  }

  setupListeners() {
    this.client.on("raw", (packet) => {
      if (!this.isRunning) return;

      if (packet.t === "VOICE_STATE_UPDATE") {
        if (packet.d?.user_id === this.client.user?.id && packet.d?.guild_id === this.guildId &&
            packet.d?.channel_id === this.channelId) {
          if (packet.d.session_id) {
            this.sessionId = packet.d.session_id;
          }
        }
      } else if (packet.t === "STREAM_CREATE") {
        const { stream_key, rtc_server_id } = packet.d || {};
        if (!stream_key) return;
        const entry = this.pendingData.get(stream_key) || {};
        entry.rtcServerId = rtc_server_id;
        this.pendingData.set(stream_key, entry);
        this.checkAndConnectSession(stream_key);
      } else if (packet.t === "STREAM_SERVER_UPDATE") {
        const { stream_key, endpoint, token, rtc_server_id } = packet.d || {};
        if (!stream_key) return;
        const entry = this.pendingData.get(stream_key) || {};
        entry.endpoint = endpoint;
        entry.token = token;
        if (rtc_server_id) {
          entry.rtcServerId = rtc_server_id;
        }
        this.pendingData.set(stream_key, entry);
        this.checkAndConnectSession(stream_key);
      } else if (packet.t === "STREAM_DELETE") {
        const { stream_key } = packet.d || {};
        if (stream_key) {
          this.stopSession(stream_key);
        }
      }
    });
  }

  async start() {
    if (this.isJoining) return;
    this.isRunning = true;
    this.isJoining = true;
    try {
      this.dashboard?.setViewerStatus("Joining channel (WebRTC)...");
      this.clearStreamSessions();
      await leaveVoiceAndWait(this.streamer);
      await joinVoiceWithTimeout(this.streamer, this.guildId, this.channelId);
      this.sessionId = this.streamer.voiceConnection?.session_id || this.sessionId;
      this.dashboard?.setViewerStatus("In channel");
      this.dashboard?.addLog("Viewer joined voice channel (WebRTC active)", "success");
    } catch (err) {
      try {
        await leaveVoiceAndWait(this.streamer);
      } catch {}
      this.dashboard?.setViewerStatus("Voice error");
      this.dashboard?.addLog(`Viewer voice connection failed: ${err.message}`, "error");
      throw err;
    } finally {
      this.isJoining = false;
    }
  }

  checkAndConnectSession(streamKey) {
    if (!this.isRunning) return;
    const data = this.pendingData.get(streamKey);
    const existing = this.activeSessions.get(streamKey);
    if (existing && data) {
      if (existing.endpoint !== data.endpoint?.replace(":443", "") || existing.token !== data.token) {
        existing.updateServer(data.endpoint, data.token);
      }
      return;
    }
    if (!data || !data.endpoint || !data.token) return;

    const rtcServerId = data.rtcServerId
      || this.streamer.voiceConnection?.streamConnection?.serverId;
    if (!rtcServerId) return;

    const sessionId = this.sessionId || this.streamer.voiceConnection?.session_id;
    if (!sessionId || !this.client.user) return;

    const session = new StreamWatchSession({
      streamer: this.streamer,
      streamKey,
      endpoint: data.endpoint,
      token: data.token,
      rtcServerId,
      userId: this.client.user.id,
      sessionId,
      onTransportEvent: (message, level) => {
        if (this.activeSessions.get(streamKey) !== session) return;
        this.dashboard?.addLog(`Viewer stream watch ${message}`, level);
        this.updateDashboardStatus();
      },
      onReady: (key) => {
        if (this.activeSessions.get(key) !== session) return;
        const username = key.split(":").pop();
        this.dashboard?.addLog(`Viewer watching stream: ${username} (RTC connected)`, "success");
        this.updateDashboardStatus();
      },
      onClose: (key, reason) => {
        if (this.activeSessions.get(key) !== session) return;
        this.activeSessions.delete(key);
        if (this.isRunning) this.dashboard?.addLog(`Viewer stream watch lost (${reason}); retrying...`, "warn");
        this.updateDashboardStatus();

        if (this.isRunning && this.pendingData.has(key)) {
          setTimeout(() => {
            if (this.isRunning && this.pendingData.has(key) && !this.activeSessions.has(key)) {
              this.watchStream(key);
            }
          }, 5000);
        }
      },
    });

    this.activeSessions.set(streamKey, session);
    session.start();
    this.updateDashboardStatus();
  }

  watchStream(streamKey) {
    if (!this.isRunning) return;

    if (!this.isVoiceReady()) {
      return;
    }

    if (this.activeSessions.has(streamKey)) return;
    this.client.ws.broadcast({
      op: 20,
      d: {
        stream_key: streamKey,
      },
    });
    // A resumed watch request may not emit another STREAM_SERVER_UPDATE.
    // Reuse the latest credentials while also asking Discord to refresh them.
    this.checkAndConnectSession(streamKey);
  }

  stopSession(streamKey) {
    const session = this.activeSessions.get(streamKey);
    if (session) {
      session.stop();
      this.activeSessions.delete(streamKey);
    }
    this.pendingData.delete(streamKey);
    this.updateDashboardStatus();
  }

  updateDashboardStatus() {
    const count = Array.from(this.activeSessions.values()).filter((s) => s.isReady).length;
    if (count > 0) {
      this.dashboard?.setViewerStatus(`Watching (${count} stream${count > 1 ? "s" : ""})`);
    } else if (this.activeSessions.size > 0) {
      this.dashboard?.setViewerStatus(`Connecting to stream (${this.activeSessions.size})...`);
    } else {
      this.dashboard?.setViewerStatus("In channel (Waiting for streams)");
    }
  }

  getWatchingCount() {
    return Array.from(this.activeSessions.values()).filter((s) => s.isReady).length;
  }

  isVoiceReady() {
    return isMediaConnectionReady(this.streamer.voiceConnection);
  }

  clearStreamSessions() {
    this.pendingData.clear();
    const sessions = [...this.activeSessions.values()];
    this.activeSessions.clear();
    this.sessionId = null;
    for (const session of sessions) session.stop();
  }

  stop() {
    this.isRunning = false;
    this.clearStreamSessions();
    try {
      this.streamer?.leaveVoice();
    } catch {}
  }
}
