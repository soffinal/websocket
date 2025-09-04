import { Stream } from "@soffinal/stream";

export class WebSocket<ENCODER extends WebSocket.Encoder<any, any>> extends Stream<WebSocket.Event<ENCODER>> {
  readonly options: WebSocket.Options<ENCODER> & { connectionTimeout: number; maxMessageQueued: number };
  readonly queue = new Array();
  protected ws: globalThis.WebSocket | undefined;

  constructor(url: string, options?: WebSocket.Options<ENCODER>);
  constructor(url: () => string | Promise<string>, options?: WebSocket.Options<ENCODER>);
  constructor(public url: string | (() => string | Promise<string>), options?: WebSocket.Options<ENCODER>) {
    super(async function* () {
      try {
        while (true) {
          if (self.ws && self.ws.readyState === globalThis.WebSocket.OPEN) {
            while (self.queue.length) {
              await self.send(self.queue.shift());
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
            continue;
          }
          if (self.ws && self.ws.readyState === globalThis.WebSocket.CONNECTING) {
            yield await self.open(self.ws);
            continue;
          }
          const { websocket, error } = await self.newWebSocket(url);
          if (websocket) {
            self.ws = websocket;
            continue;
          }
          yield error;
        }
      } finally {
        self.ws?.close(1000);
        self.ws = undefined;
        return;
      }
    });
    const self = this;
    const { connectionTimeout = 10000, maxMessageQueued = 1000 } = options ?? {};
    this.options = { ...options, connectionTimeout, maxMessageQueued };
  }
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
    if (self.queue.length > self.options.maxMessageQueued) self.queue.shift();
    if (!self.hasListeners) self.listen(() => {});
  }
  stop() {
    this.queue.length = 0;
    this.ws?.close(1000);
  }
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
    const self = this;
    return new Promise<WebSocket.ConnectedEvent | WebSocket.ErrorEvent<ENCODER, WebSocket.TimeoutErrorEvent>>(
      (resolve) => {
        ws.onopen = () => resolve({ type: "connected" });
        setTimeout(() => {
          resolve({ type: "error", error: { type: "timeout", timeout: self.options.connectionTimeout } });
        }, self.options.connectionTimeout);
      }
    );
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
  export type Encoder<SEND_DATA = unknown, MSG_DATA = SEND_DATA> = {
    encode: (data: SEND_DATA) => string | ArrayBuffer | Uint8Array | Promise<string | ArrayBuffer | Uint8Array>;
    decode: (message: string | ArrayBuffer | Uint8Array) => MSG_DATA | Promise<MSG_DATA>;
  };
  export type Options<ENCODER extends Encoder<any, any> | undefined = undefined> = Bun.WebSocketOptions & {
    encoder?: ENCODER;
    connectionTimeout?: number;
    maxMessageQueued?: number;
  };
  export type EventType<TYPE extends string, PROPS extends Record<string, unknown> = {}> = { type: TYPE } & PROPS;
  export type SendErrorEvent<DATA = unknown> = EventType<"send", { data: DATA }>;
  export type EncodeErrorEvent<DATA = unknown> = EventType<"message-encoding", { data: DATA }>;
  export type DecodeErrorEvent = EventType<"message-decoding", { data: string | ArrayBuffer | Uint8Array }>;
  export type UrlErrorEvent = EventType<"url", { url: string }>;
  export type ConnectionErrorEvent = EventType<"connection", { error: unknown }>;
  export type TimeoutErrorEvent = EventType<"timeout", { timeout: number }>;
  export type ErrorEvent<
    ENCODER extends Encoder<any, any> | undefined = undefined,
    ERROR extends
      | SendErrorEvent<ENCODER extends WebSocket.Encoder<infer Data, any> ? Data : any>
      | EncodeErrorEvent<ENCODER extends WebSocket.Encoder<infer Data, any> ? Data : any>
      | DecodeErrorEvent
      | UrlErrorEvent
      | ConnectionErrorEvent
      | TimeoutErrorEvent =
      | SendErrorEvent<ENCODER extends WebSocket.Encoder<infer Data, any> ? Data : any>
      | EncodeErrorEvent<ENCODER extends WebSocket.Encoder<infer Data, any> ? Data : any>
      | DecodeErrorEvent
      | UrlErrorEvent
      | ConnectionErrorEvent
      | TimeoutErrorEvent
  > = EventType<
    "error",
    {
      error: ERROR;
    }
  >;
  export type ConnectedEvent = EventType<"connected">;
  export type ConnectingEvent = EventType<"connecting">;
  export type DisconnectedEvent = EventType<"disconnected", { code?: number; reason?: string }>;
  export type MessageEvent<ENCODER extends Encoder<any, any> | undefined = undefined> = EventType<
    "message",
    { data: ENCODER extends WebSocket.Encoder<any, infer Data> ? Data : any }
  >;
  export type SendEvent<ENCODER extends Encoder<any, any> | undefined = undefined> = EventType<
    "send",
    { data: ENCODER extends WebSocket.Encoder<infer Data, any> ? Data : any }
  >;

  export type Event<ENCODER extends Encoder<any, any> | undefined = undefined> =
    | ConnectedEvent
    | ConnectingEvent
    | ErrorEvent
    | DisconnectedEvent
    | MessageEvent<ENCODER>
    | SendEvent<ENCODER>;
}
