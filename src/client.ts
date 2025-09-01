import { filter, map, Stream } from "@soffinal/stream";

/**
 * WebSocket client with automatic reconnection and message queuing
 *
 * @example Basic usage
 * ```typescript
 * import { WebSocket } from '@soffinal/websocket';
 *
 * const ws = new WebSocket('ws://localhost:8080');
 *
 * // Listen to all events
 * ws.listen((event) => {
 *   console.log('Event:', event);
 * });
 *
 * // Connect and send messages
 * ws.connect();
 * ws.send('Hello, server!');
 * ```
 *
 * @example With options and filtering
 * ```typescript
 * const ws = new WebSocket('wss://api.example.com/ws', {
 *   connectionTimeout: 5000,
 *   maxMessageQueued: 100
 * });
 *
 * // Listen only to messages
 * ws.pipe(filter({}, (_, event) => [event.type === 'message', {}]))
 *   .listen((event) => {
 *     console.log('Message received:', event.data);
 *   });
 *
 * ws.connect();
 * ```
 *
 * @example Auto-reconnection handling
 * ```typescript
 * const ws = new WebSocket('ws://localhost:8080');
 *
 * ws.listen((event) => {
 *   if (event.type === 'disconnected') {
 *     console.log('Connection lost, will auto-reconnect');
 *   }
 * });
 *
 * ws.connect();
 * // Client automatically reconnects on connection loss
 * ```
 */
export class WebSocket extends Stream<WebSocket.Event> {
  /**
   * Queued messages waiting to be sent
   *
   * @example
   * ```typescript
   * ws.send('message 1');
   * ws.send('message 2');
   * console.log(ws.queue.length); // 2 (if not connected)
   * ```
   */
  readonly queue: any[];
  readonly url: string;
  readonly options: WebSocket.Options & {
    connectionTimeout: number;
    maxMessageQueued: number;
    maxRetries: number;
    closeOnMaxRetries: boolean;
    useExponentialBackoff: boolean;
    retryDelay: number;
    maxRetryDelay: number;
  };
  protected controller: AbortController;
  protected ws?: globalThis.WebSocket;
  protected retryCount = 0;
  /**
   * Creates a new WebSocket client instance
   *
   * @param url - WebSocket server URL
   * @param options - Connection options
   *
   * @example
   * ```typescript
   * const ws = new WebSocket('ws://localhost:8080', {
   *   connectionTimeout: 5000,
   *   maxMessageQueued: 500
   * });
   * ```
   */
  constructor(url: string, options?: WebSocket.Options) {
    super();
    this.url = url;
    this.queue = [];
    this.controller = new AbortController();
    const {
      connectionTimeout = 10000,
      maxRetries = Infinity,
      closeOnMaxRetries = false,
      useExponentialBackoff = false,
      retryDelay = 1000,
      maxRetryDelay = 30000,
      maxMessageQueued = 1000,
    } = options ?? {};
    this.options = {
      ...options,
      connectionTimeout,
      maxRetries,
      closeOnMaxRetries,
      useExponentialBackoff,
      retryDelay,
      maxRetryDelay,
      maxMessageQueued,
    };
    this.pipe(filter({}, (_, event) => [event.type === "connected", {}])).listen(() => {
      if (!this.ws) return;
      this.retryCount = 0; // Reset retry count on successful connection
      this.queue.forEach((msg) => this.send(msg));
      this.queue.length = 0;
    }, this.controller.signal);

    this.pipe(filter({}, (_, event) => [event.type === "disconnected", {}]))
      .pipe(map({}, (_, event) => [event as WebSocket.DisconnectedEvent, {}]))
      .listen((event) => {
        if (!this.ws) return;
        this.ws = undefined;

        if (event.code === 1000) return;

        const remoteDelay = Number(event.reason);
        if (event.code === 1013 && Number.isInteger(remoteDelay)) {
          // Timeout disconnection - use specified delay, don't count as retry
          setTimeout(() => this.connect(), remoteDelay);
          return;
        }

        if (this.retryCount >= maxRetries) {
          closeOnMaxRetries ? this.close() : this.disconnect();
          return;
        }

        let expDelay = retryDelay;
        if (useExponentialBackoff) {
          // Exponential backoff: delay * 2^retryCount, capped at maxRetryDelay
          expDelay = Math.min(retryDelay * Math.pow(2, this.retryCount), maxRetryDelay);
        }

        this.retryCount++;
        setTimeout(() => this.connect(), expDelay);
      }, this.controller.signal);
  }

  /**
   * Current connection state
   *
   * @example
   * ```typescript
   * console.log(ws.state); // "connected" | "connecting" | "disconnected"
   * ```
   */
  get state(): WebSocket.CurrentState {
    switch (this.ws?.readyState) {
      case globalThis.WebSocket.OPEN:
        return "connected";
      case globalThis.WebSocket.CONNECTING:
        return "connecting";
      default:
        return "disconnected";
    }
  }

  /**
   * Establishes WebSocket connection
   *
   * @example
   * ```typescript
   * const ws = new WebSocket('ws://localhost:8080');
   * ws.connect();
   * ```
   */
  connect(): void {
    if (this.state !== "disconnected") return;

    try {
      this.ws = new globalThis.WebSocket(this.url, this.options);
    } catch (error) {
      this.push({ type: "error", url: this.url, error });
      return;
    }

    setTimeout(() => {
      if (this.state === "connecting") {
        this.ws?.close(1013, this.options.connectionTimeout.toString());
      }
    }, this.options.connectionTimeout);

    this.ws.onmessage = (msg) => this.push({ type: "message", data: msg.data });
    this.ws.onopen = () => this.push({ type: "connected" });
    this.ws.onclose = (ev) => this.push({ type: "disconnected", code: ev.code, reason: ev.reason });
    this.ws.onerror = (ev) => this.push({ type: "disconnected" });
  }
  /**
   * Closes WebSocket connection and stops auto-reconnection
   *
   * @example
   * ```typescript
   * ws.disconnect();
   * console.log(ws.state); // "disconnected"
   * ```
   */
  disconnect(): void {
    this.ws?.close(1000);
    this.ws = undefined;
  }
  /**
   * Closes WebSocket connection, stops auto-reconnection, and cleans up all resources
   *
   * @example
   * ```typescript
   * ws.close();
   * // All event listeners are removed and reconnection is permanently disabled
   * ```
   */
  close(): void {
    this.disconnect();
    this.controller.abort();
  }
  /**
   * Sends data through WebSocket or queues it if disconnected
   *
   * @param data - Data to send (string, ArrayBuffer, or ArrayBufferView)
   *
   * @example String message
   * ```typescript
   * ws.send('Hello, server!');
   * ws.send(JSON.stringify({ type: 'chat', message: 'Hello' }));
   * ```
   *
   * @example Binary data
   * ```typescript
   * const buffer = new ArrayBuffer(8);
   * ws.send(buffer);
   * ```
   */
  send(data: string | ArrayBufferLike | Bun.ArrayBufferView<ArrayBufferLike>): void {
    const self = this;
    if (self.state === "connected" && self.ws) {
      try {
        self.ws.send(data);
      } catch (error) {
        pushToQueue();
      }
    } else {
      pushToQueue();
    }
    function pushToQueue() {
      self.queue.push(data);
      if (self.queue.length > self.options.maxMessageQueued) self.queue.shift();
    }
  }
}

export namespace WebSocket {
  /**
   * Current connection state of the WebSocket
   * @example
   * ```typescript
   * const ws = new WebSocket('ws://localhost:8080');
   * console.log(ws.state); // "disconnected"
   * ```
   */
  export type CurrentState = "connected" | "connecting" | "disconnected";

  /**
   * WebSocket configuration options
   * @example
   * ```typescript
   * const options: Options = {
   *   connectionTimeout: 5000,
   *   maxMessageQueued: 500,
   *   protocols: ['chat', 'superchat']
   * };
   * ```
   */
  export type Options = Bun.WebSocketOptions & {
    /** Timeout in milliseconds for connection attempts (default: 10000) */
    connectionTimeout?: number;
    /** Maximum number of messages to queue when disconnected (default: 1000) */
    maxMessageQueued?: number;
    /** Maximum number of retry attempts (default: Infinity) */
    maxRetries?: number;
    /** close the websocket after retry attempts reached (default: false) "false" mean disconnect */
    closeOnMaxRetries?: boolean;
    /** Use exponential backoff for retries (default: false) */
    useExponentialBackoff?: boolean;
    /** Initial retry delay in milliseconds (default: 1000) */
    retryDelay?: number;
    /** Maximum retry delay in milliseconds (default: 30000) */
    maxRetryDelay?: number;
  };

  export type ConnectedEvent = { type: "connected" };
  export type ConnectingEvent = { type: "connecting" };
  export type ErrorEvent = { type: "error"; url: string; error: unknown };
  export type DisconnectedEvent = { type: "disconnected"; code?: number; reason?: string };
  export type MessageEvent = { type: "message"; data: any };
  /**
   * WebSocket events emitted by the client
   * @example
   * ```typescript
   * ws.listen((event) => {
   *   switch (event.type) {
   *     case 'connected':
   *       console.log('Connected to server');
   *       break;
   *     case 'message':
   *       console.log('Received:', event.data);
   *       break;
   *     case 'disconnected':
   *       console.log('Disconnected:', event.code, event.reason);
   *       break;
   *   }
   * });
   * ```
   */
  export type Event = ConnectedEvent | ConnectingEvent | ErrorEvent | DisconnectedEvent | MessageEvent;
}
