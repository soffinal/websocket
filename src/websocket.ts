import { Stream } from "@soffinal/stream";

/**
 * A TypeScript WebSocket client focused on simplicity and developer experience.
 * Features lazy connections, automatic resource management, and intelligent reconnection strategies.
 *
 * @template ENCODER - The encoder type for message serialization/deserialization
 *
 * @example
 * ```typescript
 * // Basic usage
 * const ws = new WebSocket("ws://localhost:8080");
 * ws.listen((event) => console.log(event));
 * ws.send("Hello, server!");
 *
 * // With custom encoder
 * const ws = new WebSocket("ws://localhost:8080", {
 *   encoder: {
 *     encode: (data) => JSON.stringify(data),
 *     decode: (data) => JSON.parse(data)
 *   }
 * });
 * ```
 */
export class WebSocket<ENCODER extends WebSocket.Encoder<any, any>> extends Stream<WebSocket.Event<ENCODER>> {
  /** Configuration options with defaults applied */
  readonly options: WebSocket.Options<ENCODER> & {
    maxConnectionTimeout: number;
    initialRetryDelay: number;
    retryMultiplier: number;
    maxMessageQueued: number;
    maxIdle: number;
  };

  /** Message queue for storing messages when disconnected */
  readonly queue = new Array();

  /** The underlying WebSocket instance */
  protected ws: globalThis.WebSocket | undefined;

  /** AbortController for managing connection lifecycle */
  private controller: AbortController | undefined;
  /**
   * Creates a new WebSocket client with static URL
   * @param url - WebSocket URL
   * @param options - Configuration options
   */
  constructor(url: string, options?: WebSocket.Options<ENCODER>);

  /**
   * Creates a new WebSocket client with dynamic URL resolution
   * @param url - Function that returns WebSocket URL (sync or async)
   * @param options - Configuration options
   */
  constructor(url: () => string | Promise<string>, options?: WebSocket.Options<ENCODER>);

  constructor(public url: string | (() => string | Promise<string>), options?: WebSocket.Options<ENCODER>) {
    super(async function* () {
      if (!self.controller) self.controller = new AbortController();
      let retryDelay = self.options.initialRetryDelay;
      try {
        while (true) {
          self.controller.signal.throwIfAborted();

          if (
            !self.ws ||
            self.ws.readyState === globalThis.WebSocket.CLOSED ||
            self.ws.readyState === globalThis.WebSocket.CLOSING
          ) {
            yield { type: "connecting" };

            const { websocket, error } = await self.newWebSocket(url);
            if (websocket) {
              self.ws = websocket;

              const event = await Promise.race([
                self.open(self.ws),
                new Promise<WebSocket.ErrorEvent<ENCODER, WebSocket.TimeoutErrorEvent>>((resolve) => {
                  setTimeout(() => {
                    resolve({ type: "error", error: { type: "timeout", timeout: retryDelay } });
                  }, retryDelay);
                }),
              ]);
              if (event.type === "error") {
                retryDelay = Math.min(retryDelay * self.options.retryMultiplier, self.options.maxConnectionTimeout);
                yield event;
                continue;
              } else {
                retryDelay = self.options.initialRetryDelay;
              }
              yield event;
            } else {
              yield error;
              continue;
            }
          }

          while (self.queue.length) {
            self.send(self.queue.shift());
          }
          const event = await Promise.race([self.message(self.ws), self.close(self.ws), self.error(self.ws)]);
          if (event.type === "disconnected") {
            if (event.code === 1013) {
              const delay = Number(event.reason);
              if (Number.isInteger(delay)) await new Promise((r) => setTimeout(r, delay));
            } else if (event.code === 1000) {
              yield event;
              break;
            }
          }
          yield event;
        }
      } finally {
        return;
      }
    });
    const self = this;
    const {
      maxConnectionTimeout = 10000,
      maxMessageQueued = 1000,
      initialRetryDelay = 100,
      retryMultiplier = 2,
      maxIdle = Infinity,
    } = options ?? {};
    this.options = { ...options, maxConnectionTimeout, initialRetryDelay, retryMultiplier, maxMessageQueued, maxIdle };
  }
  /**
   * Sends data through the WebSocket connection.
   * Automatically connects if not connected and queues messages when disconnected.
   *
   * @param data - Data to send (type depends on encoder)
   * @returns Promise that resolves when message is sent or queued
   *
   * @example
   * ```typescript
   * await ws.send("Hello, server!");
   * await ws.send({ type: "chat", message: "Hello" });
   * ```
   */
  async send(
    data: ENCODER extends WebSocket.Encoder<infer DATA, any>
      ? DATA
      : string | ArrayBufferLike | Bun.ArrayBufferView<ArrayBufferLike>
  ): Promise<void> {
    const self = this;
    if (self.ws?.readyState === globalThis.WebSocket.OPEN) {
      let encodedData: any;
      try {
        encodedData = self.options.encoder ? await self.options.encoder.encode(data) : data;
      } catch (error) {
        this.push({ type: "error", error: { type: "message-encoding", data } });
        return;
      }

      try {
        self.ws.send(encodedData);
        self.push({ type: "send", data: data });
      } catch (error) {
        self.push({ type: "error", error: { type: "send", data: data } });
      }
      return;
    }

    self.queue.push(data);
    if (self.queue.length > self.options.maxMessageQueued) {
      self.push({ type: "error", error: { type: "queue-overflow", data: self.queue.shift() } });
    }
    if (!self.hasListeners) {
      if (!self.controller) self.controller = new AbortController();
      const abort = self.listen(() => {}, self.controller.signal);
      if (Number.isFinite(self.options.maxIdle)) setTimeout(() => abort(), self.options.maxIdle);
    }
  }
  /**
   * Stops the WebSocket connection and clears the message queue.
   *
   * @example
   * ```typescript
   * ws.stop(); // Immediately disconnect and clear queue
   * ```
   */
  stop(): void {
    this.controller?.abort();
    this.controller = undefined;
    this.queue.length = 0;
    this.ws?.close(1000);
    this.ws = undefined;
  }
  /**
   * Automatic cleanup for `using` declarations (TC39 Explicit Resource Management)
   *
   * @example
   * ```typescript
   * using ws = new WebSocket("ws://localhost:8080");
   * // Automatically cleaned up when scope exits
   * ```
   */
  [Symbol.dispose]() {
    this.stop();
  }
  private async newWebSocket(urlOrFunction: string | (() => string | Promise<string>)) {
    const url = typeof urlOrFunction === "string" ? urlOrFunction : await urlOrFunction();
    try {
      new URL(url);
    } catch {
      return {
        error: {
          type: "error",
          error: { type: "url", url },
        } satisfies WebSocket.ErrorEvent<ENCODER, WebSocket.UrlErrorEvent>,
      };
    }
    try {
      return { websocket: new globalThis.WebSocket(url) };
    } catch {
      return {
        error: { type: "error", error: { type: "connection", error: undefined } } satisfies WebSocket.ErrorEvent<
          ENCODER,
          WebSocket.ConnectionErrorEvent
        >,
      };
    }
  }
  private message(ws: globalThis.WebSocket) {
    return new Promise<WebSocket.MessageEvent<ENCODER> | WebSocket.ErrorEvent<ENCODER, WebSocket.DecodeErrorEvent>>(
      (resolve) => {
        ws.onmessage = async (ev) => {
          try {
            const data = this.options.encoder ? await this.options.encoder.decode(ev.data) : ev.data;
            resolve({ type: "message", data });
          } catch (error) {
            resolve({ type: "error", error: { type: "message-decoding", data: ev.data } });
          }
        };
      }
    );
  }
  private close(ws: globalThis.WebSocket) {
    return new Promise<WebSocket.DisconnectedEvent>((resolve) => {
      ws.onclose = (ev) => resolve({ type: "disconnected", code: ev.code, reason: ev.reason });
    });
  }
  private error(ws: globalThis.WebSocket) {
    return new Promise<WebSocket.ErrorEvent<ENCODER, WebSocket.ConnectionErrorEvent>>((resolve) => {
      ws.onerror = (ev) => resolve({ type: "error", error: { type: "connection", error: ev } });
    });
  }
  private open(ws: globalThis.WebSocket) {
    return new Promise<WebSocket.ConnectedEvent | WebSocket.ErrorEvent<ENCODER, WebSocket.TimeoutErrorEvent>>(
      (resolve) => {
        ws.onopen = () => resolve({ type: "connected" });
      }
    );
  }
  /**
   * Creates a default JSON encoder for message serialization
   *
   * @template SEND_DATA - Type of data being sent
   * @template MSG_DATA - Type of data being received (defaults to SEND_DATA)
   * @returns JSON encoder that stringifies/parses messages
   *
   * @example
   * ```typescript
   * const encoder = WebSocket.getDefaultEncoder<SendMsg, ReceiveMsg>();
   * const ws = new WebSocket("ws://localhost:8080", { encoder });
   * ```
   */
  static getDefaultEncoder<SEND_DATA, MSG_DATA = SEND_DATA>(): WebSocket.Encoder<SEND_DATA, MSG_DATA> {
    return {
      encode(data) {
        return JSON.stringify(data);
      },
      decode(decoded) {
        return JSON.parse(decoded as string);
      },
    };
  }
}

export namespace WebSocket {
  /**
   * Interface for custom message encoders
   * @template SEND_DATA - Type of data being sent
   * @template MSG_DATA - Type of data being received
   */
  export type Encoder<SEND_DATA = unknown, MSG_DATA = SEND_DATA> = {
    /** Encodes data for transmission (sync or async) */
    encode: (data: SEND_DATA) => string | ArrayBuffer | Uint8Array | Promise<string | ArrayBuffer | Uint8Array>;
    /** Decodes received message (sync or async) */
    decode: (message: string | ArrayBuffer | Uint8Array) => MSG_DATA | Promise<MSG_DATA>;
  };

  /**
   * Configuration options for WebSocket client
   * @template ENCODER - The encoder type
   */
  export type Options<ENCODER extends Encoder<any, any> | undefined = undefined> = Bun.WebSocketOptions & {
    /** Custom encoder for message serialization */
    encoder?: ENCODER;
    /** Maximum connection timeout in milliseconds (default: 10000) */
    maxConnectionTimeout?: number;
    /** Retry delay multiplier for exponential backoff (default: 2) */
    retryMultiplier?: number;
    /** Initial retry delay in milliseconds (default: 100) */
    initialRetryDelay?: number;
    /** Maximum number of queued messages (default: 1000) */
    maxMessageQueued?: number;
    /** Maximum idle time for send-only connections in milliseconds (default: Infinity) */
    maxIdle?: number;
  };
  /** Base event type with discriminated union pattern */
  export type EventType<TYPE extends string, PROPS extends Record<string, unknown> = {}> = { type: TYPE } & PROPS;

  /** Error when sending message fails */
  export type SendErrorEvent<DATA = unknown> = EventType<"send", { data: DATA }>;

  /** Error when encoding message fails */
  export type EncodeErrorEvent<DATA = unknown> = EventType<"message-encoding", { data: DATA }>;

  /** Error when decoding received message fails */
  export type DecodeErrorEvent = EventType<"message-decoding", { data: string | ArrayBuffer | Uint8Array }>;

  /** Error when URL is invalid */
  export type UrlErrorEvent = EventType<"url", { url: string }>;

  /** Error when connection fails */
  export type ConnectionErrorEvent = EventType<"connection", { error: unknown }>;

  /** Error when connection times out */
  export type TimeoutErrorEvent = EventType<"timeout", { timeout: number }>;

  /** Error when message queue overflows */
  export type QueueOverflowErrorEvent<DATA = unknown> = EventType<"queue-overflow", { data: DATA }>;
  /**
   * Error event with contextual information
   * @template ENCODER - The encoder type
   * @template ERROR - Specific error type
   */
  export type ErrorEvent<
    ENCODER extends Encoder<any, any> | undefined = undefined,
    ERROR extends
      | SendErrorEvent<ENCODER extends WebSocket.Encoder<infer Data, any> ? Data : any>
      | EncodeErrorEvent<ENCODER extends WebSocket.Encoder<infer Data, any> ? Data : any>
      | DecodeErrorEvent
      | UrlErrorEvent
      | ConnectionErrorEvent
      | TimeoutErrorEvent
      | QueueOverflowErrorEvent<ENCODER extends WebSocket.Encoder<infer Data, any> ? Data : any> =
      | SendErrorEvent<ENCODER extends WebSocket.Encoder<infer Data, any> ? Data : any>
      | EncodeErrorEvent<ENCODER extends WebSocket.Encoder<infer Data, any> ? Data : any>
      | DecodeErrorEvent
      | UrlErrorEvent
      | ConnectionErrorEvent
      | TimeoutErrorEvent
      | QueueOverflowErrorEvent<ENCODER extends WebSocket.Encoder<infer Data, any> ? Data : any>
  > = EventType<
    "error",
    {
      error: ERROR;
    }
  >;

  /** Event emitted when WebSocket connection is established */
  export type ConnectedEvent = EventType<"connected">;

  /** Event emitted when WebSocket is attempting to connect */
  export type ConnectingEvent = EventType<"connecting">;

  /** Event emitted when WebSocket connection is closed */
  export type DisconnectedEvent = EventType<"disconnected", { code?: number; reason?: string }>;

  /**
   * Event emitted when a message is received
   * @template ENCODER - The encoder type for type-safe message data
   */
  export type MessageEvent<ENCODER extends Encoder<any, any> | undefined = undefined> = EventType<
    "message",
    { data: ENCODER extends WebSocket.Encoder<any, infer Data> ? Data : any }
  >;

  /**
   * Event emitted when a message is successfully sent
   * @template ENCODER - The encoder type for type-safe send data
   */
  export type SendEvent<ENCODER extends Encoder<any, any> | undefined = undefined> = EventType<
    "send",
    { data: ENCODER extends WebSocket.Encoder<infer Data, any> ? Data : any }
  >;

  /**
   * Union of all possible WebSocket events
   * @template ENCODER - The encoder type for type-safe event data
   */
  export type Event<ENCODER extends Encoder<any, any> | undefined = undefined> =
    | ConnectedEvent
    | ConnectingEvent
    | ErrorEvent<ENCODER>
    | DisconnectedEvent
    | MessageEvent<ENCODER>
    | SendEvent<ENCODER>;
}
