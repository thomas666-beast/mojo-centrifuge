/*
 * centrifuge-client.js — tiny client for Mojo::Centrifuge (no dependencies)
 *
 *   const client = new CentrifugeClient('ws://localhost:3000/ws', {
 *     getToken: () => fetch('/token?user=anna&channel=news').then(r => r.text()),
 *   });
 *   const sub = client.subscribe('news', {
 *     message: m => console.log(m.seq, m.data),
 *     join:    e => console.log(e.user, 'joined'),
 *     leave:   e => console.log(e.user, 'left'),
 *   });
 *   client.on('connect', () => client.publish('news', 'hello'));
 *   client.connect();
 *
 * Works in browsers and Node 22+ (global WebSocket). On older Node pass
 * { WebSocket: require('ws') }.
 *
 * Reconnects with exponential backoff + jitter, calls getToken() again before
 * every attempt (so expired tokens are refreshed), and resubscribes each channel
 * with since=<last seq seen> so missed messages are replayed from history.
 */
(function (root) {
  'use strict';

  class Subscription {
    constructor(client, channel, handlers, since) {
      this.client = client;
      this.channel = channel;
      this.handlers = handlers;
      this.lastSeq = since; // undefined until a message arrives (or opts.since)
    }
    unsubscribe() { this.client._unsubscribe(this); }
  }

  class CentrifugeClient {
    constructor(url, opts) {
      this.url = url;
      this.opts = Object.assign({
        token: null,            // static token, or...
        getToken: null,         // ...async () => token (called before every connect)
        minDelay: 500,          // first retry delay (ms)
        maxDelay: 30000,        // cap for the backoff
        factor: 2,              // growth per failed attempt
        maxRetries: Infinity,
        staleAfter: 40000,      // no frame for this long => assume dead (server pings every 25s)
        requestTimeout: 10000,  // publish()/presence() timeout
        WebSocket: null,
      }, opts || {});

      this.state = 'disconnected'; // disconnected | connecting | connected | reconnecting
      this._ws = null;
      this._subs = new Map();
      this._listeners = {};
      this._pending = new Map();
      this._rid = 0;
      this._attempt = 0;
      this._manual = true;
      this._timer = null;
      this._watch = null;
      this._lastRx = 0;
    }

    // ---- events: connect, disconnect, reconnecting, error, state, message, join, leave
    on(event, fn) { (this._listeners[event] = this._listeners[event] || []).push(fn); return this; }
    off(event, fn) {
      this._listeners[event] = (this._listeners[event] || []).filter(f => f !== fn);
      return this;
    }
    _emit(event, ...args) {
      for (const fn of (this._listeners[event] || []).slice()) {
        try { fn(...args); } catch (e) { /* a bad listener must not break the client */ }
      }
    }
    _setState(s) {
      if (this.state === s) return;
      this.state = s;
      this._emit('state', s);
    }

    // ---- public API
    connect() {
      if (this.state !== 'disconnected') return;
      this._manual = false;
      this._attempt = 0;
      this._open();
    }

    close() {
      this._manual = true;
      clearTimeout(this._timer);
      this._clearWatch();
      const ws = this._ws;
      this._ws = null;
      if (ws) { try { ws.close(1000, 'client close'); } catch (e) { /* ignore */ } }
      this._rejectPending(new Error('client closed'));
      if (this.state !== 'disconnected') {
        this._setState('disconnected');
        this._emit('disconnect', { code: 1000, reason: 'client close' });
      }
    }

    subscribe(channel, handlers) {
      if (typeof channel !== 'string' || !channel) throw new Error('channel must be a non-empty string');
      let since;
      if (typeof handlers === 'function') handlers = { message: handlers };
      else if (handlers && handlers.since !== undefined) since = handlers.since;
      handlers = handlers || {};

      const old = this._subs.get(channel);
      if (old) { old.handlers = handlers; return old; }

      const sub = new Subscription(this, channel, handlers, since);
      this._subs.set(channel, sub);
      if (this.state === 'connected') this._sendSubscribe(sub);
      return sub;
    }

    _unsubscribe(sub) {
      if (this._subs.get(sub.channel) !== sub) return;
      this._subs.delete(sub.channel);
      if (this.state === 'connected') this._send({ unsubscribe: sub.channel });
    }

    /** Resolves with {seq, channel} once the server acks; rejects if not allowed. */
    publish(channel, data) { return this._request({ publish: channel, data: data }); }

    /** Resolves with [{id, user}, ...] */
    presence(channel) { return this._request({ presence: channel }); }

    // ---- connection handling
    async _open() {
      this._setState(this._attempt > 0 ? 'reconnecting' : 'connecting');

      let token = this.opts.token;
      if (this.opts.getToken) {
        try { token = await this.opts.getToken(); }
        catch (e) { this._emit('error', e); return this._scheduleReconnect(); }
      }
      if (this._manual) return;

      const WS = this.opts.WebSocket || root.WebSocket;
      if (!WS) { this._emit('error', new Error('no WebSocket implementation available')); return; }

      let u = this.url;
      if (token) {
        u += (u.indexOf('?') === -1 ? '?' : '&') + 'token=' + encodeURIComponent(String(token).trim());
      }

      let ws;
      try { ws = new WS(u); }
      catch (e) { this._emit('error', e); return this._scheduleReconnect(); }
      this._ws = ws;

      ws.onopen = () => {
        if (this._ws !== ws) return;
        this._attempt = 0;
        this._setState('connected');
        this._armWatch();
        for (const sub of this._subs.values()) this._sendSubscribe(sub);
        this._emit('connect');
      };
      ws.onmessage = ev => {
        if (this._ws !== ws) return;
        this._lastRx = Date.now();
        this._handle(ev.data);
      };
      ws.onclose = ev => {
        if (this._ws !== ws) return;
        this._ws = null;
        this._onClose(ev || {});
      };
      ws.onerror = () => { /* onclose always follows; reconnect is handled there */ };
    }

    _onClose(ev) {
      this._clearWatch();
      this._rejectPending(new Error('disconnected'));
      this._setState('disconnected');
      this._emit('disconnect', { code: ev.code, reason: ev.reason });
      if (!this._manual) this._scheduleReconnect();
    }

    _scheduleReconnect() {
      if (this._manual) return;
      if (this._attempt >= this.opts.maxRetries) {
        this._emit('error', new Error('max reconnect attempts reached'));
        this._setState('disconnected');
        return;
      }
      const o = this.opts;
      const cap = Math.min(o.maxDelay, o.minDelay * Math.pow(o.factor, this._attempt));
      const delay = Math.round(cap / 2 + Math.random() * (cap / 2)); // "equal jitter"
      this._attempt++;
      this._setState('reconnecting');
      this._emit('reconnecting', { attempt: this._attempt, delay: delay });
      this._timer = setTimeout(() => this._open(), delay);
    }

    // If the server goes silent (no ping for staleAfter ms) the TCP connection is
    // probably half-open; drop it ourselves instead of waiting for the OS.
    _armWatch() {
      this._clearWatch();
      this._lastRx = Date.now();
      const every = Math.max(250, Math.floor(this.opts.staleAfter / 4));
      this._watch = setInterval(() => {
        if (Date.now() - this._lastRx <= this.opts.staleAfter) return;
        const ws = this._ws;
        this._ws = null;
        if (ws) { try { ws.close(); } catch (e) { /* ignore */ } }
        this._onClose({ code: 4000, reason: 'stale connection' });
      }, every);
    }
    _clearWatch() { if (this._watch) { clearInterval(this._watch); this._watch = null; } }

    // ---- protocol
    _send(obj) {
      const ws = this._ws;
      if (!ws || ws.readyState !== 1) return false;
      ws.send(JSON.stringify(obj));
      return true;
    }

    _sendSubscribe(sub) {
      const cmd = { subscribe: sub.channel };
      if (sub.lastSeq !== undefined) cmd.since = sub.lastSeq; // replay what we missed
      this._send(cmd);
    }

    _request(cmd) {
      return new Promise((resolve, reject) => {
        const id = 'r' + (++this._rid);
        cmd.id = id;
        const timer = setTimeout(() => {
          this._pending.delete(id);
          reject(new Error('request timed out'));
        }, this.opts.requestTimeout);
        this._pending.set(id, { resolve, reject, timer });
        if (!this._send(cmd)) {
          clearTimeout(timer);
          this._pending.delete(id);
          reject(new Error('not connected'));
        }
      });
    }

    _rejectPending(err) {
      for (const p of this._pending.values()) { clearTimeout(p.timer); p.reject(err); }
      this._pending.clear();
    }

    _handle(raw) {
      if (typeof raw !== 'string') return;
      let m;
      try { m = JSON.parse(raw); } catch (e) { return; }
      if (!m || typeof m !== 'object') return;

      if (m.ping) { this._send({ pong: 1 }); return; }
      if (m.pong) return;

      // reply to publish()/presence()
      if (m.id !== undefined && this._pending.has(m.id)) {
        const p = this._pending.get(m.id);
        this._pending.delete(m.id);
        clearTimeout(p.timer);
        if (m.error) p.reject(new Error(m.error));
        else p.resolve(m.presence !== undefined ? m.presence : { seq: m.published, channel: m.channel });
        return;
      }

      if (m.error) { this._emit('error', new Error(m.error)); return; }

      const sub = m.channel !== undefined ? this._subs.get(m.channel) : undefined;

      if (m.event === 'join' || m.event === 'leave') {
        if (sub && typeof sub.handlers[m.event] === 'function') this._safe(sub.handlers[m.event], m);
        this._emit(m.event, m);
        return;
      }

      if (m.seq !== undefined && sub) {
        if (sub.lastSeq !== undefined && m.seq <= sub.lastSeq) return; // replay/live overlap
        sub.lastSeq = m.seq;
        if (typeof sub.handlers.message === 'function') this._safe(sub.handlers.message, m);
        this._emit('message', m);
      }
    }

    _safe(fn, arg) { try { fn(arg); } catch (e) { this._emit('error', e); } }
  }

  if (typeof module !== 'undefined' && module.exports) module.exports = { CentrifugeClient };
  else root.CentrifugeClient = CentrifugeClient;
})(typeof globalThis !== 'undefined' ? globalThis : this);
