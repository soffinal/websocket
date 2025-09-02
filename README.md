# @soffinal/websocket

A TypeScript WebSocket client with automatic connection management, message queuing, type-safe encoding, and stream-based event handling.

## Installation

```bash
npm install @soffinal/websocket
# or
yarn add @soffinal/websocket
# or
pnpm add @soffinal/websocket
# or
bun install @soffinal/websocket
```

## Quick Start

```typescript
import { WebSocket } from "@soffinal/websocket";

const ws = new WebSocket("ws://localhost:8080");

// Connection starts automatically when you listen
const unsubscribe = ws.listen((event) => {
  console.log("Event:", event);
});

// Or use for await...of to iterate over events
async () => {
  for await (const event of ws) {
    console.log("Event:", event);

    if (event.type === "connected") {
      ws.send("Hello, server!");
    }
  }
};

// Send messages (queued if not connected)
ws.send("Hello, server!");

// Connection closes automatically when no listeners or active generators
unsubscribe();
```

## Features

- ** Automatic Connection Management**: Connects on the first listener, disconnects when abort the last listener
- ** Smart Message Queuing**: Messages are queued when disconnected and sent when reconnected
- ** Intelligent Reconnection**: Exponential backoff with server-specified delays
- ** Type-Safe Encoding**: Built-in support for JSON, binary, and custom serialization with full type safety
- ** Stream-based Events**: Built on `@soffinal/stream` for powerful event handling
- ** TypeScript First**: Full type safety with specific error types
- ** Runtime Agnostic**: Works in Node.js, Bun, Deno, and browsers
- ** Zero Configuration**: Works perfectly with sensible defaults

## API Reference

### WebSocket Class

#### Constructor

```typescript
new WebSocket(url: string | URL, options?: WebSocket.Options)
```

#### Options

```typescript
type Options<ENCODER extends Encoder<any, any> | undefined = undefined> = {
  encoder?: ENCODER; // Custom encoder for serialization/deserialization
  connectionTimeout?: number; // Default: 10000ms
  maxMessageQueued?: number; // Default: 1000
  maxRetries?: number; // Default: Infinity
  useExponentialBackoff?: boolean; // Default: false
  retryDelay?: number; // Default: 1000ms
  maxRetryDelay?: number; // Default: 30000ms
  // + additional Bun.WebSocketOptions when used in Bun
};
```

#### Properties

- `state`: Current connection state (`"connected"` | `"connecting"` | `"disconnected"`)
- `queue`: Array of queued messages waiting to be sent
- `url`: WebSocket URL

#### Methods

- `listen(handler)`: Listen to events (starts connection automatically)
- `send(data)`: Send data or queue it if disconnected (async when using encoder)

#### Static Methods

- `getDefaultEncoder<SendType, ReceiveType>()`: Get a JSON encoder with type safety

### Events

```typescript
type Event<DATA = unknown> =
  | { type: "connecting" }
  | { type: "connected" }
  | { type: "disconnected"; code?: number; reason?: string }
  | { type: "message"; data: DATA }
  | { type: "error"; error: SendError | TimeoutError | ConnectionError | UrlError };
```

### Encoder

```typescript
type Encoder<SEND_DATA = unknown, MSG_DATA = SEND_DATA> = {
  encode: (data: SEND_DATA) => string | ArrayBuffer | Uint8Array | Promise<string | ArrayBuffer | Uint8Array>;
  decode: (message: string | ArrayBuffer | Uint8Array) => MSG_DATA | Promise<MSG_DATA>;
};
```

## Usage Examples

### Basic Usage

```typescript
import { WebSocket } from "@soffinal/websocket";

const ws = new WebSocket("ws://localhost:8080");

// Automatically connects on the first listener
// and disconnects when the last listener removed
ws.listen((event) => {
  switch (event.type) {
    case "connecting":
      console.log("Connecting to server...");
      break;
    case "connected":
      console.log("Connected to server");
      break;
    case "message":
      console.log("Received:", event.data);
      break;
    case "disconnected":
      console.log("Disconnected:", event.code, event.reason);
      break;
    case "error":
      console.error("Error:", event.error);
      break;
  }
});

ws.send("Hello, server!");
```

### Advanced Configuration

```typescript
type ApiMessage = { action: string; payload: any };
type ApiResponse = { status: string; result: any };

const ws = new WebSocket("wss://api.example.com/ws", {
  encoder: WebSocket.getDefaultEncoder<ApiMessage, ApiResponse>(),
  connectionTimeout: 5000,
  maxMessageQueued: 500,
  maxRetries: 10,
  useExponentialBackoff: true,
  retryDelay: 2000,
  maxRetryDelay: 60000,
});

ws.listen((event) => {
  console.log("Event:", event);
});
```

### Message Filtering with Streams

```typescript
import { filter } from "@soffinal/stream";

// Listen only to messages
ws.pipe(filter((event) => event.type === "message")).listen((event) => {
  console.log("Message received:", event.data);
});

// Listen only to connection events
ws.pipe(filter((event) => ["connected", "connecting", "disconnected"].includes(event.type))).listen((event) => {
  console.log("Connection state:", event.type);
});
```

### Automatic Resource Management

```typescript
const ws = new WebSocket("ws://localhost:8080");

// Connection starts automatically
const unsubscribe1 = ws.listen(handler1);
const unsubscribe2 = ws.listen(handler2);

// Still connected (has listeners)
unsubscribe1();

// Automatically disconnects (no more listeners)
unsubscribe2();
```

### Error Handling

```typescript
ws.listen((event) => {
  if (event.type === "error") {
    if (event.error instanceof WebSocket.UrlError) {
      console.error("Invalid URL provided");
    } else if (event.error instanceof WebSocket.ConnectionError) {
      console.error("Failed to connect to server");
    } else if (event.error instanceof WebSocket.TimeoutError) {
      console.error("Connection timeout");
    } else if (event.error instanceof WebSocket.SendError) {
      console.error("Failed to send message");
    }
  }
});
```

### Type-Safe Encoding

```typescript
// JSON encoding with type safety
type SendMessage = { type: "chat"; message: string };
type ReceiveMessage = { type: "response"; data: string };

const ws = new WebSocket("ws://localhost:8080", {
  encoder: WebSocket.getDefaultEncoder<SendMessage, ReceiveMessage>(),
});

// Type-safe sending
await ws.send({ type: "chat", message: "Hello" }); // ✓ Type-safe
// ws.send({ invalid: "data" }); // ✗ TypeScript error

// Type-safe receiving
ws.listen((event) => {
  if (event.type === "message") {
    console.log(event.data.type); // ✓ Fully typed as ReceiveMessage
  }
});
```

### Custom Encoders

```typescript
// MessagePack encoder
import * as msgpack from "@msgpack/msgpack";

const ws = new WebSocket("ws://localhost:8080", {
  encoder: {
    encode: (data) => msgpack.encode(data),
    decode: (buffer) => msgpack.decode(buffer),
  },
});

// Custom binary protocol
const binaryWs = new WebSocket("ws://localhost:8080", {
  encoder: {
    encode: async (cmd: Command) => await compressAndEncrypt(cmd),
    decode: async (data: ArrayBuffer) => await decryptAndDecompress(data),
  },
});
```

### Raw Data (No Encoder)

```typescript
// Without encoder - raw WebSocket data
const ws = new WebSocket("ws://localhost:8080");

// String messages
ws.send("Hello, server!");
ws.send(JSON.stringify({ type: "chat", message: "Hello" }));

// Binary data
const buffer = new ArrayBuffer(8);
ws.send(buffer);

// Typed arrays
const uint8Array = new Uint8Array([1, 2, 3, 4]);
ws.send(uint8Array);
```

## Intelligent Reconnection

The client automatically handles reconnection with smart retry logic:

- **Normal disconnection** (code 1000): No reconnection
- **Server timeout** (code 1013 with numeric reason): Uses server-specified delay
- **Other disconnections**: Uses configurable retry logic with optional exponential backoff
- **Max retries reached**: Stops reconnection attempts

### Retry Configuration

```typescript
// Simple fixed-interval retries (default)
const ws1 = new WebSocket("ws://localhost:8080", {
  maxRetries: 5, // Stop after 5 failed attempts
  retryDelay: 2000, // Always wait 2 seconds between retries
});

// Exponential backoff retries
const ws2 = new WebSocket("ws://localhost:8080", {
  useExponentialBackoff: true,
  maxRetries: 10,
  retryDelay: 1000, // Start with 1 second
  maxRetryDelay: 30000, // Cap at 30 seconds
});
// Retry delays: 1s, 2s, 4s, 8s, 16s, 30s, 30s...
```

### Disabling Auto-Reconnection

```typescript
// Disable auto-reconnection completely
const ws = new WebSocket("ws://localhost:8080", {
  maxRetries: 0,
});

ws.listen((event) => {
  if (event.type === "disconnected") {
    // Implement your own reconnection logic
    console.log("Connection lost - handling manually");
  }
});
```

## Message Queue Management

Messages are automatically queued when disconnected and sent when reconnected:

```typescript
const ws = new WebSocket("ws://localhost:8080");

// These messages are queued if not connected
ws.send("message 1");
ws.send("message 2");
ws.send("message 3");

console.log(ws.queue.length); // 3 (if not connected)

// Start listening - connection begins and queue is flushed
ws.listen((event) => {
  console.log("Event:", event);
});

// After connection: queue.length === 0
```

## Dependencies

- `@soffinal/stream`: Stream-based event handling
- Works with any modern JavaScript runtime (Node.js, Bun, Deno, browsers)

## Author

**soffinal**

- GitHub: [@soffinal](https://github.com/soffinal)
- Email: smari.sofiane@gmail.com

## License

MIT
