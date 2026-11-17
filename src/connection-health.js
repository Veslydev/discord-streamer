// A socket being OPEN does not mean Discord has accepted its resumed session.
export class ConnectionHealth {
  constructor(connection, { onClose, onRecovered, now = Date.now, recoveryTimeoutMs = 30000 } = {}) {
    this.connection = connection;
    this.onClose = onClose;
    this.onRecovered = onRecovered;
    this.now = now;
    this.recoveryTimeoutMs = recoveryTimeoutMs;
    this.confirmed = false;
    this.everReady = false;
    this.recoverySince = null;
    this.lastAckAt = null;
    this.confirmedAt = null;
    this.heartbeatInterval = null;
    this.failure = null;
    this.stopped = false;
    this.detach = null;
  }

  watch(socket) {
    this.detach?.();
    this.confirmed = false;
    this.lastAckAt = null;
    this.confirmedAt = null;
    this.heartbeatInterval = null;
    const onMessage = (event) => {
      if (this.stopped || this.connection.ws !== socket || typeof event.data !== "string") return;
      let packet;
      try { packet = JSON.parse(event.data); } catch { return; }
      if (Number.isInteger(packet.seq)) this.connection._sequenceNumber = packet.seq;
      if (packet.op === 8 && Number.isFinite(packet.d?.heartbeat_interval) && packet.d.heartbeat_interval > 0) {
        this.heartbeatInterval = packet.d.heartbeat_interval;
      } else if (packet.op === 6) {
        this.lastAckAt = this.now();
      } else if (packet.op === 2 || packet.op === 9) {
        this.confirmed = true;
        this.confirmedAt = this.now();
      }
      this.check();
    };
    const onClose = (event) => {
      if (this.stopped || this.connection.awaitingVoiceServer || this.connection.ws !== socket) return;
      this.confirmed = false;
      this.recoverySince ??= this.now();
      // Non-resumable Discord close codes require a fresh voice session.
      if (event.code >= 4000 && event.code !== 4015) this.failure = `socket close ${event.code}`;
      this.onClose?.(event.code);
    };
    socket.addEventListener("message", onMessage);
    socket.addEventListener("close", onClose);
    this.detach = () => {
      socket.removeEventListener("message", onMessage);
      socket.removeEventListener("close", onClose);
    };
  }

  check() {
    const now = this.now();
    if (this.stopped || this.failure) return "failed";
    if (!this.connection.awaitingVoiceServer && this.connection.ws?.readyState === 1 &&
        this.confirmed && this.connection.webRtcConn?.ready) {
      const ackDeadline = Math.max((this.heartbeatInterval || 0) * 2, 15000);
      if (this.heartbeatInterval && now - (this.lastAckAt ?? this.confirmedAt) > ackDeadline) {
        this.failure = "heartbeat ACK timed out";
        return "failed";
      }
      const recovered = this.recoverySince !== null && this.everReady;
      this.recoverySince = null;
      this.everReady = true;
      if (recovered) this.onRecovered?.();
      return "healthy";
    }
    this.recoverySince ??= now;
    if (now - this.recoverySince >= this.recoveryTimeoutMs) {
      this.failure = this.connection.awaitingVoiceServer ? "voice server allocation timed out" : "transport recovery timed out";
      return "failed";
    }
    return "recovering";
  }

  waitForServer() {
    this.confirmed = false;
    this.recoverySince ??= this.now();
  }

  stop() {
    this.stopped = true;
    this.detach?.();
    this.detach = null;
  }
}

export function isMediaConnectionReady(connection) {
  if (connection?.transportHealth) return connection.transportHealth.check() === "healthy";
  return !!(connection?.status?.started && connection.ws?.readyState === 1 && connection.webRtcConn?.ready);
}

export function shouldReconnectMedia(connection) {
  if (connection?.transportHealth) return connection.transportHealth.check() === "failed";
  return !isMediaConnectionReady(connection);
}
