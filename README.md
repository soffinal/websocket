# @soffinal/websocket

A TypeScript WebSocket client with automatic reconnection, message queuing, and stream-based event handling.

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

ws.listen((event) => {
  console.log("Event:", event);
});

ws.connect();
ws.send("Hello, server!");
```

## Features

- **Automatic Reconnection**: Reconnects automatically on connection loss
- **Message Queuing**: Queues messages when disconnected, sends when reconnected
- **Stream-based Events**: Built on `@soffinal/stream` for powerful event handling
- **TypeScript Support**: Full type safety and IntelliSense
- **Runtime Agnostic**: Works in Node.js, Bun, Deno, and browsers
- **Optional Exponential Backoff**: Configurable retry logic with exponential backoff

## API Reference

### WebSocket Class

#### Constructor

```typescript
new WebSocket(url: string, options?: Options)
```

**Parameters:**

- `url`: WebSocket server URL
- `options`: Optional configuration

#### Options

```typescript
type Options = {
  connectionTimeout?: number; // Default: 10000ms
  maxMessageQueued?: number; // Default: 1000
  maxRetries?: number; // Default: Infinity
  closeOnMaxRetries?: boolean; // Default: false
  useExponentialBackoff?: boolean; // Default: false
  retryDelay?: number; // Default: 1000ms
  maxRetryDelay?: number; // Default: 30000ms
  protocols?: string[]; // WebSocket protocols
  // + additional Bun.WebSocketOptions when used in Bun
};
```

#### Properties

- `state`: Current connection state (`"connected"` | `"connecting"` | `"disconnected"`)
- `queue`: Array of queued messages waiting to be sent

#### Methods

- `connect()`: Establishes WebSocket connection
- `disconnect()`: Closes connection and stops auto-reconnection
- `close()`: Closes connection, stops auto-reconnection, and cleans up all resources
- `send(data)`: Sends data or queues it if disconnected

### Events

```typescript
type Event =
  | { type: "connected" }
  | { type: "connecting" }
  | { type: "disconnected"; code?: number; reason?: string }
  | { type: "message"; data: any };
```

## Usage Examples

### Basic Usage

```typescript
import { WebSocket } from "@soffinal/websocket";

const ws = new WebSocket("ws://localhost:8080");

ws.listen((event) => {
  switch (event.type) {
    case "connected":
      console.log("Connected to server");
      break;
    case "message":
      console.log("Received:", event.data);
      break;
    case "disconnected":
      console.log("Disconnected:", event.code, event.reason);
      break;
  }
});

ws.connect();
```

### With Configuration

```typescript
const ws = new WebSocket("wss://api.example.com/ws", {
  connectionTimeout: 5000,
  maxMessageQueued: 100,
  maxRetries: 10,
  closeOnMaxRetries: true,
  useExponentialBackoff: true,
  retryDelay: 2000,
  maxRetryDelay: 60000,
  protocols: ["chat", "superchat"],
});

ws.connect();
```

### Message Filtering

```typescript
import { filter } from "@soffinal/stream";

// Listen only to messages
ws.pipe(filter({}, (_, event) => [event.type === "message", {}])).listen((event) => {
  console.log("Message received:", event.data);
});
```

### Sending Different Data Types

```typescript
// String messages
ws.send("Hello, server!");
ws.send(JSON.stringify({ type: "chat", message: "Hello" }));

// Binary data
const buffer = new ArrayBuffer(8);
ws.send(buffer);
```

### Connection State Monitoring

```typescript
console.log(ws.state); // "disconnected"

ws.connect();
console.log(ws.state); // "connecting"

// After connection established
console.log(ws.state); // "connected"
```

### Message Queue Management

```typescript
// Messages are queued when disconnected
ws.send("message 1");
ws.send("message 2");
console.log(ws.queue.length); // 2

// Queue is flushed when connected
ws.connect();
// After connection: queue.length === 0
```

## Auto-Reconnection

The client automatically reconnects on connection loss:

- **Normal disconnection** (code 1000): No reconnection
- **Timeout disconnection** (code 1013,reason:"2000"): Reconnects after specified delay captured in the reason
- **Other disconnections**: Uses `retryDelay` (default: 1000ms) or exponential backoff if enabled
- **Max retries reached**: Calls `disconnect()` by default, or `close()` if `closeOnMaxRetries: true`

```typescript
ws.listen((event) => {
  if (event.type === "disconnected") {
    console.log("Connection lost, will auto-reconnect");
  }
});
```

### Retry Configuration

```typescript
// Simple fixed-interval retries (default)
const ws1 = new WebSocket("ws://localhost:8080", {
  maxRetries: 5, // Stop after 5 failed attempts
  retryDelay: 2000, // Always wait 2 seconds between retries
  closeOnMaxRetries: false // Just disconnect, don't clean up resources
});

// Exponential backoff retries with full cleanup
const ws2 = new WebSocket("ws://localhost:8080", {
  useExponentialBackoff: true,
  maxRetries: 10,
  retryDelay: 1000, // Start with 1 second
  maxRetryDelay: 30000, // Cap at 30 seconds
  closeOnMaxRetries: true // Clean up all resources when max retries reached
});
// Retry delays: 1s, 2s, 4s, 8s, 16s, 30s, 30s...
```

### Disabling Auto-Reconnection

To implement custom retry logic, set `maxRetries: 0` to disable built-in reconnection:

```typescript
// Disable auto-reconnection and implement custom logic
const ws = new WebSocket("ws://localhost:8080", {
  maxRetries: 0 // Disable built-in reconnection
});

ws.listen((event) => {
  if (event.type === "disconnected" && event.code !== 1000) {
    // Your custom reconnection logic here
    console.log("Connection lost, handling manually");
    setTimeout(() => {
      console.log("Attempting manual reconnection...");
      ws.connect();
    }, 5000);
  }
});
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
