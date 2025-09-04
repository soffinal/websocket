# @soffinal/websocket

A revolutionary TypeScript WebSocket client with lazy connections, automatic resource management, type-safe encoding, and stream-based event handling.

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

// Create WebSocket - no connection yet (lazy)
const ws = new WebSocket("ws://localhost:8080");

// Connection starts automatically when you listen
const unsubscribe = ws.listen((event) => {
  console.log("Event:", event);
});

// Or fire-and-forget sending (connects automatically)
ws.send("Hello, server!");

// Use async iteration
for await (const event of ws) {
  console.log("Event:", event);
  if (event.type === "connected") {
    ws.send("Hello, server!");
  }
}

// Manual cleanup (optional)
ws.stop();

// Or use automatic cleanup with 'using'
using ws2 = new WebSocket("ws://localhost:8080");
ws2.send("Auto-cleanup!"); // Cleaned up automatically
```

## Features

- **🚀 Lazy Connections**: Connects automatically on first listener or send - no manual connection management
- **🔄 Smart Reconnection**: Immediate reconnection with server-specified delays (respects code 1013)
- **📦 Message Queuing**: Messages queued when disconnected, sent when reconnected
- **🔒 Type-Safe Encoding**: Built-in JSON, binary, and custom serialization with full type safety
- **🌊 Stream-Based Events**: Built on `@soffinal/stream` for powerful event composition
- **🧹 Automatic Cleanup**: Resources cleaned up when no listeners remain
- **🔧 Manual Control**: `stop()` method and `Symbol.dispose` for explicit cleanup
- **🌐 Dynamic URLs**: Support for static URLs or async URL resolution functions
- **⚡ Zero Configuration**: Perfect defaults, minimal required options
- **🏃 Runtime Agnostic**: Works in Node.js, Bun, Deno, and browsers

## API Reference

### WebSocket Class

#### Constructor

```typescript
new WebSocket(url: string, options?: WebSocket.Options)
new WebSocket(url: () => string | Promise<string>, options?: WebSocket.Options)
```

#### Options

```typescript
type Options<ENCODER extends Encoder<any, any> | undefined = undefined> = {
  encoder?: ENCODER; // Custom encoder for serialization/deserialization
  connectionTimeout?: number; // Default: 10000ms
  maxMessageQueued?: number; // Default: 1000
  // + additional Bun.WebSocketOptions when used in Bun
};
```

#### Properties

- `queue`: Array of queued messages waiting to be sent
- `url`: WebSocket URL or URL function

#### Methods

- `listen(handler)`: Listen to events (starts connection automatically)
- `send(data)`: Send data or queue it if disconnected (connects automatically if no listeners)
- `stop()`: Stop connection and clear queue
- `[Symbol.dispose]()`: Automatic cleanup for `using` declarations

### Events

```typescript
type Event<ENCODER> =
  | { type: "connecting" }
  | { type: "connected" }
  | { type: "disconnected"; code?: number; reason?: string }
  | { type: "message"; data: DecodedData }
  | { type: "send"; data: OriginalData }
  | { type: "error"; error: ErrorDetails };
```

### Error Types

```typescript
type ErrorDetails =
  | { type: "url"; url: string }
  | { type: "connection"; error: unknown }
  | { type: "timeout"; timeout: number }
  | { type: "message-encoding"; data: unknown }
  | { type: "message-decoding"; data: string | ArrayBuffer | Uint8Array }
  | { type: "send"; data: unknown };
```

### Encoder

```typescript
type Encoder<SEND_DATA = unknown, MSG_DATA = SEND_DATA> = {
  encode: (data: SEND_DATA) => string | ArrayBuffer | Uint8Array | Promise<string | ArrayBuffer | Uint8Array>;
  decode: (message: string | ArrayBuffer | Uint8Array) => MSG_DATA | Promise<MSG_DATA>;
};
```

## Usage Examples

### Lazy Connection Pattern

```typescript
import { WebSocket } from "@soffinal/websocket";

// Create WebSocket - no connection established yet
const ws = new WebSocket("ws://localhost:8080");

// Connection starts automatically when you listen
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
    case "send":
      console.log("Sent:", event.data);
      break;
    case "disconnected":
      console.log("Disconnected:", event.code, event.reason);
      break;
    case "error":
      console.error("Error:", event.error);
      break;
  }
});

// Or connection starts automatically when you send
ws.send("Hello, server!"); // Connects if no listeners exist
```

### Dynamic URLs

```typescript
// Static URL
const ws1 = new WebSocket("wss://api.example.com/ws");

// Dynamic URL with authentication
const ws2 = new WebSocket(async () => {
  const token = await getAuthToken();
  return `wss://api.example.com/ws?token=${token}`;
});

// Environment-based URL
const ws3 = new WebSocket(() => {
  return process.env.NODE_ENV === "production" ? "wss://prod-api.com/ws" : "ws://localhost:8080/ws";
});
```

### Advanced Configuration

```typescript
type ApiMessage = { action: string; payload: any };
type ApiResponse = { status: string; result: any };

const ws = new WebSocket("wss://api.example.com/ws", {
  encoder: WebSocket.getDefaultEncoder<ApiMessage, ApiResponse>(),
  connectionTimeout: 5000,
  maxMessageQueued: 500,
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

### Resource Management

```typescript
// Automatic cleanup when no listeners
const ws = new WebSocket("ws://localhost:8080");
const unsubscribe1 = ws.listen(handler1);
const unsubscribe2 = ws.listen(handler2);

unsubscribe1(); // Still connected (has listeners)
unsubscribe2(); // Automatically disconnects (no more listeners)

// Manual cleanup
ws.stop(); // Immediately stop and clear queue

// Modern automatic cleanup
using ws2 = new WebSocket("ws://localhost:8080");
ws2.send("Hello!");
// Automatically cleaned up when scope exits
```

### Error Handling

```typescript
ws.listen((event) => {
  if (event.type === "error") {
    switch (event.error.type) {
      case "url":
        console.error("Invalid URL:", event.error.url);
        break;
      case "connection":
        console.error("Connection failed:", event.error.error);
        break;
      case "timeout":
        console.error("Connection timeout:", event.error.timeout, "ms");
        break;
      case "message-encoding":
        console.error("Failed to encode:", event.error.data);
        break;
      case "message-decoding":
        console.error("Failed to decode:", event.error.data);
        break;
      case "send":
        console.error("Failed to send:", event.error.data);
        break;
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

## Smart Reconnection

The client automatically handles reconnection with server-first logic:

- **Normal disconnection** (code 1000): No reconnection
- **Server timeout** (code 1013 with numeric reason): Uses server-specified delay
- **Other disconnections**: Immediate reconnection (server handles rate limiting)
- **Philosophy**: Server protection is server responsibility, client optimizes for UX

### Server-Specified Delays

```typescript
// Server can specify reconnection delay via close code 1013
ws.listen((event) => {
  if (event.type === "disconnected") {
    if (event.code === 1013) {
      console.log("Server requested delay:", event.reason, "ms");
      // Client automatically waits before reconnecting
    } else {
      console.log("Reconnecting immediately");
    }
  }
});
```

### Manual Connection Control

```typescript
// Stop all reconnection attempts
ws.stop();

// Or handle disconnections manually
ws.listen((event) => {
  if (event.type === "disconnected") {
    // Implement custom reconnection logic
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

## Why This Approach is Revolutionary

### Traditional WebSocket Pattern

```typescript
// Manual lifecycle management
const ws = new WebSocket("ws://localhost:8080");
ws.connect(); // Must remember to connect
ws.addEventListener("message", handler);
// Must remember to disconnect
ws.disconnect();
```

### Lazy Connection Pattern

```typescript
// Communication-first approach
const ws = new WebSocket("ws://localhost:8080");

// Just communicate - connection handled automatically
ws.listen(handler); // Connects when you want to listen
ws.send("data"); // Connects when you want to send
// Disconnects when no longer needed
```

**Revolutionary Benefits:**

- **🧠 Intent-Based**: Connection reflects communication intent
- **🚀 Zero Waste**: No idle connections consuming resources
- **🎯 Fire-and-Forget**: `send()` just works, regardless of state
- **🔄 Self-Healing**: Automatic reconnection without artificial delays
- **🧹 Self-Cleaning**: Resources freed when no longer needed
- **💭 Declarative**: Think "communicate" not "connect"

## Philosophy

This library embodies a fundamental shift in WebSocket client design:

**Traditional**: "Manage connections to enable communication"
**Revolutionary**: "Enable communication, connections are an implementation detail"

Just like HTTP clients don't require manual connection management, WebSocket clients shouldn't either. The connection exists only when needed for actual communication.

## Dependencies

- `@soffinal/stream`: Stream-based event handling foundation
- Works with any modern JavaScript runtime (Node.js, Bun, Deno, browsers)

## Author

**soffinal**

- GitHub: [@soffinal](https://github.com/soffinal)
- Email: smari.sofiane@gmail.com

## License

MIT
