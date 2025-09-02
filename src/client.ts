import { filter, Stream } from "@soffinal/stream";

export class WebSocket<ENCODER extends WebSocket.Encoder<any, any> | undefined = undefined> extends Stream<
  WebSocket.Event<
    ENCODER extends WebSocket.Encoder<infer Data, any> ? Data : any,
    ENCODER extends WebSocket.Encoder<any, infer Data> ? Data : any
  >
> {
  readonly queue = new Array();
  readonly url: string;
  protected ws?: globalThis.WebSocket;

  protected abort = false;
  readonly options: WebSocket.Options<ENCODER> & {
    connectionTimeout: number;
    maxMessageQueued: number;
    maxRetries: number;
    retryDelay: number;
    useExponentialBackoff: boolean;
    maxRetryDelay: number;
    maxIdle: number;
  };
  constructor(url: string | URL, options?: WebSocket.Options<ENCODER>) {
    let retryCount = 0;
    super(async function* () {
      let expDelay = retryDelay;
      try {
        while (true) {
          if (retryCount > maxRetries || self.abort) break;
          self.abort = false;

          if (
            !self.ws ||
            self.ws.readyState === globalThis.WebSocket.CLOSED ||
            self.ws.readyState === globalThis.WebSocket.CLOSING
          ) {
            yield { type: "connecting" };

            if (self.ws) {
              retryCount++;

              if (useExponentialBackoff) {
                expDelay = Math.min(retryDelay * Math.pow(2, retryCount), maxRetryDelay);
              }
              await new Promise((r) => setTimeout(r, expDelay));
              expDelay = retryDelay;
            }

            try {
              new URL(url);
            } catch {
              yield { type: "error", error: new WebSocket.UrlError() };
              break;
            }

            try {
              self.ws = new globalThis.WebSocket(url);
            } catch {
              yield { type: "error", error: new WebSocket.ConnectionError() };
              break;
            }
          }

          yield await new Promise<
            WebSocket.Event<
              ENCODER extends WebSocket.Encoder<infer Data, any> ? Data : any,
              ENCODER extends WebSocket.Encoder<any, infer Data> ? Data : any
            >
          >((resolve) => {
            setTimeout(() => {
              if (self.state === "connecting" || self.state === "disconnected")
                resolve({ type: "error", error: new WebSocket.TimeoutError() });
            }, connectionTimeout);
            self.ws!.onopen = () => resolve({ type: "connected" });
            self.ws!.onmessage = async (ev) =>
              resolve({
                type: "message",
                data: encoder ? await encoder.decode(ev.data) : ev.data,
              });
            self.ws!.onerror = () => resolve({ type: "error", error: new WebSocket.ConnectionError() });
            self.ws!.onclose = (ev) => {
              if (ev.code === 1000) self.disconnect();
              if (ev.code === 1013 && Number.isInteger(Number(ev.reason))) expDelay = Number(ev.reason);

              resolve({ type: "disconnected", code: ev.code, reason: ev.reason });
            };
          });

          if (self.state === "connected") {
            retryCount = 0;
            while (self.queue.length) {
              self.send(self.queue.shift());
            }
          }
        }
      } finally {
        console.log(self.hasListeners);

        if (!self.hasListeners) self.disconnect();
        return;
      }
    });
    const self = this;
    const {
      connectionTimeout = 10000,
      maxMessageQueued = 1000,
      maxRetries = Infinity,
      maxRetryDelay = 30000,
      retryDelay = 1000,
      useExponentialBackoff = false,
      maxIdle = 30000,
      encoder,
    } = options ?? {};

    this.options = {
      ...options,
      connectionTimeout,
      maxMessageQueued,
      maxRetries,
      maxRetryDelay,
      retryDelay,
      useExponentialBackoff,
      maxIdle,
    };
    this.url = url instanceof URL ? url.toString() : url;
  }
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
  protected disconnect(): void {
    this.ws?.close(1000);
    this.ws = undefined;
    this.abort = true;
  }

  async send(
    data: ENCODER extends WebSocket.Encoder<infer DATA, any>
      ? DATA
      : string | ArrayBufferLike | Bun.ArrayBufferView<ArrayBufferLike>
  ): Promise<void> {
    const self = this;
    self.listen(() => {
      //
    });
    if (self.state === "connected" && self.ws) {
      try {
        self.ws.send(self.options.encoder ? await self.options.encoder.encode(data) : data);
        self.push({ type: "send", data });
      } catch (error) {
        self.push({ type: "error", error: new WebSocket.SendError(data) });
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
  export class SendError<DATA = unknown> extends Error {
    constructor(public data: DATA) {
      super();
    }
  }
  export class TimeoutError extends Error {}
  export class UrlError extends Error {}
  export class ConnectionError extends Error {}
  export type Encoder<SEND_DATA = unknown, MSG_DATA = SEND_DATA> = {
    encode: (data: SEND_DATA) => string | ArrayBuffer | Uint8Array | Promise<string | ArrayBuffer | Uint8Array>;
    decode: (message: string | ArrayBuffer | Uint8Array) => MSG_DATA | Promise<MSG_DATA>;
  };
  export type Options<ENCODER extends Encoder<any, any> | undefined = undefined> = Bun.WebSocketOptions & {
    maxIdle?: number;

    encoder?: ENCODER;
    /** Timeout in milliseconds for connection attempts (default: 10000) */
    connectionTimeout?: number;
    /** Maximum number of messages to queue when disconnected (default: 1000) */
    maxMessageQueued?: number;
    /** Maximum number of retry attempts (default: Infinity) */
    maxRetries?: number;
    /** Use exponential backoff for retries (default: false) */
    useExponentialBackoff?: boolean;
    /** Initial retry delay in milliseconds (default: 1000) */
    retryDelay?: number;
    /** Maximum retry delay in milliseconds (default: 30000) */
    maxRetryDelay?: number;
  };
  export type CurrentState = "connected" | "connecting" | "disconnected";
  export type ConnectedEvent = { type: "connected" };
  export type ConnectingEvent = { type: "connecting" };
  export type ErrorEvent = { type: "error"; error: SendError | TimeoutError | ConnectionError | UrlError };
  export type DisconnectedEvent = { type: "disconnected"; code?: number; reason?: string };
  export type MessageEvent<DATA = unknown> = { type: "message"; data: DATA };
  export type SendEvent<DATA> = { type: "send"; data: DATA };

  export type Event<SEND_DATA = unknown, MSG_DATA = SEND_DATA> =
    | ConnectedEvent
    | ConnectingEvent
    | ErrorEvent
    | DisconnectedEvent
    | MessageEvent<MSG_DATA>
    | SendEvent<SEND_DATA>;
}

const ws = new WebSocket("", { encoder: WebSocket.getDefaultEncoder<{ name: string }, boolean>() });

ws.send({ name: "" });
//. ^?
ws.listen((v) => {});
