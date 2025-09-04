# @soffinal/websocket

[![npm version](https://badge.fury.io/js/@soffinal%2Fwebsocket.svg)](https://badge.fury.io/js/@soffinal%2Fwebsocket)
[![TypeScript](https://img.shields.io/badge/TypeScript-Ready-blue.svg)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A TypeScript WebSocket client focused on simplicity and developer experience. Features lazy connections, automatic resource management, and intelligent reconnection strategies.

## 🚀 Why @soffinal/websocket?

Traditional WebSocket libraries require manual connection lifecycle management. **@soffinal/websocket** takes a different approach: **communication-first design** where connections are handled automatically, letting you focus on your application logic.

## 📦 Installation

```bash
npm install @soffinal/websocket
```

```bash
yarn add @soffinal/websocket
```

```bash
pnpm add @soffinal/websocket
```

```bash
bun add @soffinal/websocket
```

## ⚡ Quick Start

```typescript
import { WebSocket } from "@soffinal/websocket";

// Create WebSocket - no connection established yet (lazy)
const ws = new WebSocket("ws://localhost:8080");

// Connection starts automatically when you listen
ws.listen((event) => {
  console.log("Event:", event);
});

// Or use async iteration
for await (const event of ws) {
  if (event.type === "connected") {
    ws.send("Hello, server!");
  }
}

// Fire-and-forget sending (connects automatically)
ws.send("Hello, server!");

// Manual cleanup (optional)
ws.stop();

// Modern automatic cleanup
using ws2 = new WebSocket("ws://localhost:8080");
ws2.send("Auto-cleanup!"); // Cleaned up automatically at scope exit
```

## 🌟 Key Features

### 🔄 Lazy Connections

Connections are established only when needed for actual communication. No idle connections consuming resources.

### 🧠 Intelligent Reconnection

- **Immediate reconnection** for unexpected disconnections
- **Server-specified delays** (respects WebSocket close code 1013)
- **Configurable exponential backoff** for connection failures
- **No artificial delays** - server handles rate limiting

### 📦 Automatic Message Queuing

Messages are automatically queued when disconnected and delivered when reconnected. No message loss.

### 🔒 Type-Safe Encoding

Built-in support for JSON, binary, and custom serialization with full TypeScript type safety.

### 🌊 Stream-Based Architecture

Built on `@soffinal/stream` for powerful event composition and filtering.

### 🧹 Automatic Resource Management

Resources are automatically cleaned up when no listeners remain. Supports modern `using` declarations.

### 🌐 Dynamic URL Resolution

Support for static URLs or async functions for authentication tokens and load balancing.

### ⚡ Sensible Defaults

Good defaults that work for most use cases. Minimal configuration required.

### 🏃 Runtime Agnostic

Works seamlessly in Node.js, Bun, Deno, and browsers.

## 📚 Documentation

### Constructor

```typescript
new WebSocket(url: string, options?: WebSocket.Options)
new WebSocket(url: () => string | Promise<string>, options?: WebSocket.Options)
```

### Configuration Options

```typescript
interface Options<ENCODER extends Encoder<any, any> | undefined = undefined> {
  encoder?: ENCODER; // Custom encoder for serialization
  maxConnectionTimeout?: number; // Max connection timeout (default: 10000ms)
  initialRetryDelay?: number; // Initial retry delay (default: 100ms)
  retryMultiplier?: number; // Retry delay multiplier (default: 2)
  maxMessageQueued?: number; // Max queued messages (default: 1000)
  maxIdle?: number; // Max idle time for send-only connections (default: Infinity)
}
```

### Methods

| Method               | Description                                                 |
| -------------------- | ----------------------------------------------------------- |
| `listen(handler)`    | Listen to events (starts connection automatically)          |
| `send(data)`         | Send data or queue if disconnected (connects automatically) |
| `stop()`             | Stop connection and clear message queue                     |
| `[Symbol.dispose]()` | Automatic cleanup for `using` declarations                  |

### Events

```typescript
type Event<ENCODER> =
  | { type: "connecting" }
  | { type: "connected" }
  | { type: "disconnected"; code?: number; reason?: string }
  | { type: "message"; data: ENCODER extends WebSocket.Encoder<any, infer Data> ? Data : any }
  | { type: "send"; data: ENCODER extends WebSocket.Encoder<infer Data, any> ? Data : any }
  | { type: "error"; error: ErrorObject };
```

### Error Types

All errors include contextual information for debugging:

```typescript
type ErrorEvent<ENCODER> = {
  type: "error";
  error:
    | { type: "url"; url: string }
    | { type: "connection"; error: unknown }
    | { type: "timeout"; timeout: number }
    | { type: "message-encoding"; data: ENCODER extends WebSocket.Encoder<infer Data, any> ? Data : unknown }
    | { type: "message-decoding"; data: string | ArrayBuffer | Uint8Array }
    | { type: "send"; data: ENCODER extends WebSocket.Encoder<infer Data, any> ? Data : unknown }
    | { type: "queue-overflow"; data: ENCODER extends WebSocket.Encoder<infer Data, any> ? Data : unknown };
};
```

## 💡 Usage Examples

### Basic Real-Time Communication

```typescript
import { WebSocket } from "@soffinal/websocket";

const ws = new WebSocket("ws://localhost:8080");

ws.listen((event) => {
  switch (event.type) {
    case "connecting":
      console.log("🔄 Connecting to server...");
      break;
    case "connected":
      console.log("✅ Connected to server");
      break;
    case "message":
      console.log("📨 Received:", event.data);
      break;
    case "send":
      console.log("📤 Sent:", event.data);
      break;
    case "disconnected":
      console.log("❌ Disconnected:", event.code, event.reason);
      break;
    case "error":
      console.error("🚨 Error:", event.error);
      break;
  }
});

// Send messages - connection handled automatically
ws.send("Hello, server!");
```

### Dynamic URL Resolution

```typescript
// Static URL
const ws1 = new WebSocket("wss://api.example.com/ws");

// Dynamic URL with authentication
const ws2 = new WebSocket(async () => {
  const token = await getAuthToken();
  return `wss://api.example.com/ws?token=${token}`;
});
```

### Type-Safe Message Handling

```typescript
type SendMessage = { type: "chat"; message: string; userId: string };
type ReceiveMessage = { type: "response"; data: string; timestamp: number };

const ws = new WebSocket("ws://localhost:8080", {
  encoder: WebSocket.getDefaultEncoder<SendMessage, ReceiveMessage>(),
});

// Type-safe sending
await ws.send({
  type: "chat",
  message: "Hello",
  userId: "user123",
}); // ✅ Fully typed

// Type-safe receiving
ws.listen((event) => {
  if (event.type === "message") {
    console.log(event.data.timestamp); // ✅ TypeScript knows this exists
  }
});
```

### Custom Encoders

```typescript
// Type-safe MessagePack encoder
import * as msgpack from "@msgpack/msgpack";

type SendData = { action: string; payload: any };
type ReceiveData = { status: string; result: any };

const ws = new WebSocket("ws://localhost:8080", {
  encoder: {
    encode: (data: SendData) => msgpack.encode(data),
    decode: (buffer): ReceiveData => msgpack.decode(buffer),
  },
});

// Type-safe binary protocol with compression
type Command = { cmd: string; args: string[] };
type Response = { success: boolean; data: unknown };

const binaryWs = new WebSocket("ws://localhost:8080", {
  encoder: {
    encode: async (data: Command) => await compressAndEncrypt(data),
    decode: async (data): Promise<Response> => await decryptAndDecompress(data),
  },
});

// Now fully type-safe
ws.send({ action: "login", payload: { user: "john" } }); // ✅ Typed
binaryWs.send({ cmd: "exec", args: ["ls", "-la"] }); // ✅ Typed
```

### Stream-Based Event Filtering

```typescript
import { filter, map } from "@soffinal/stream";

// Listen only to chat messages
ws.pipe(filter((event) => event.type === "message"))
  .pipe(filter((event) => event.data.type === "chat"))
  .listen((event) => {
    console.log("Chat message:", event.data.message);
  });

// Transform and filter connection events
ws.pipe(filter((event) => ["connected", "disconnected"].includes(event.type)))
  .pipe(map((event) => ({ status: event.type, timestamp: Date.now() })))
  .listen((statusEvent) => {
    updateConnectionStatus(statusEvent);
  });
```

### Error Handling

```typescript
ws.listen((event) => {
  if (event.type === "error") {
    switch (event.error.type) {
      case "url":
        console.error("❌ Invalid URL:", event.error.url);
        break;
      case "connection":
        console.error("❌ Connection failed:", event.error.error);
        break;
      case "timeout":
        console.error("⏰ Connection timeout:", event.error.timeout, "ms");
        break;
      case "message-encoding":
        console.error("📤 Failed to encode:", event.error.data);
        break;
      case "message-decoding":
        console.error("📥 Failed to decode:", event.error.data);
        break;
      case "send":
        console.error("📤 Failed to send:", event.error.data);
        break;
      case "queue-overflow":
        console.error("📦 Message queue overflow:", event.error.data);
        break;
    }
  }
});
```

### Resource Management

```typescript
// Connection lifecycle based on listeners, not sending
const ws = new WebSocket("ws://localhost:8080");

// Sending keeps connection open until manually closed or if the maxIdle is set
ws.send("message 1"); // Connects and stays open
ws.send("message 2"); // Connection remains open

// Listeners control automatic cleanup
const unsubscribe1 = ws.listen(handler1);
const unsubscribe2 = ws.listen(handler2);

unsubscribe1(); // Still connected (has listeners)
unsubscribe2(); // Automatically disconnects (no more listeners)

// Manual cleanup
ws.stop(); // Immediately stop and clear queue

// Modern automatic cleanup with 'using'
using ws2 = new WebSocket("ws://localhost:8080");
ws2.send("Hello!"); // Connection stays open
// Automatically cleaned up when scope exits
```

## 🔄 Intelligent Reconnection

The client implements a reconnection strategy optimized for real-world scenarios:

### Reconnection Logic

- **Normal disconnection** (code 1000): No reconnection
- **Server timeout** (code 1013 with numeric reason): Uses server-specified delay
- **Other disconnections**: Immediate reconnection with exponential backoff on failures
- **Philosophy**: Server protection is server responsibility, client optimizes for UX

### Server-Specified Delays

```typescript
// Server can specify reconnection delay via close code 1013
ws.listen((event) => {
  if (event.type === "disconnected") {
    if (event.code === 1013) {
      console.log("⏳ Server requested delay:", event.reason, "ms");
      // Client automatically waits before reconnecting
    } else {
      console.log("🔄 Reconnecting immediately");
    }
  }
});
```

### Exponential Backoff for Connection Failures

When connection attempts fail (not disconnections), the client uses configurable exponential backoff:

- **First attempt**: Immediate (0ms)
- **First retry**: `initialRetryDelay` (default: 100ms)
- **Second retry**: 200ms (100ms × 2)
- **Third retry**: 400ms (200ms × 2)
- **Fourth retry**: 800ms (400ms × 2)
- **Subsequent retries**: Up to `maxConnectionTimeout` value

Both `initialRetryDelay` and `retryMultiplier` are configurable for different use cases.

## 📊 Message Queue Management

Messages are automatically queued when disconnected and delivered when reconnected:

```typescript
const ws = new WebSocket("ws://localhost:8080");

// These messages trigger connection and are queued until connected
ws.send("message 1"); // Starts connection, queues message
ws.send("message 2"); // Queued
ws.send("message 3"); // Queued

console.log(ws.queue.length); // 3 (while connecting)

// After connection established: queue is flushed automatically
```

## 🔗 Dependencies

- **[@soffinal/stream](https://github.com/soffinal/stream)**: Stream-based event handling foundation
- **Runtime Support**: Node.js, Bun, Deno, and modern browsers

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## 👨‍💻 Author

**soffinal**

- GitHub: [@soffinal](https://github.com/soffinal)
- Email: smari.sofiane@gmail.com

## 🤝 Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## 📈 Changelog

See [CHANGELOG.md](CHANGELOG.md) for a detailed history of changes.

---

**@soffinal/websocket** - Simplifying real-time communication with automatic connection management.
