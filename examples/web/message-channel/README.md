# Node.js Host Transport Implementation

This directory contains Node.js setup and tests for implementing the wRPC host transport interface.

Both the server and the client are implementing in this package for ease of testing. In practice, these could be separate projects (as long as they're deployed in a way such that they can communicate over MessageChannel)

## Architecture

The TypeScript/JavaScript code is the **host** that implements the WIT interface. The WASM component (built from the Rust wRPC code) will **import** this interface and call the host's implementation.

This is similar to how WASI works - the host implements the interfaces, and the WASM component imports them.

## Setup

1. Install dependencies:
```bash
npm install
```

2. Generate TypeScript bindings from the WIT file:
```bash
npm run generate-bindings
```

This will generate TypeScript type definitions in `./bindings/` that you can use to implement the host transport interface.

## WIT Interface

The host must implement the `wrpc:host-transport/transport` interface defined in `../../wit/transport.wit`.

The interface defines:

- **`channel` resource**: A bidirectional channel with `read()`, `write()`, and `close()` methods
- **`create-invocation-channel()`**: Creates a new channel for client invocations
- **`accept-channel()`**: Accepts incoming channels from the server's queue

## Implementation Example

Here's a basic TypeScript implementation example using MessageChannel (for Web Workers). The WASM component will import and call these functions:

```typescript
// Import the generated types (structure depends on your bindings generator)
import type { Transport, Channel } from './bindings/transport';

// Implement the transport interface
class MessageChannelTransport implements Transport {
  private channelQueue: Channel[] = [];
  private queueWaiters: Array<(channel: Channel) => void> = [];

  async createInvocationChannel(): Promise<Channel> {
    // Create a MessageChannel for this invocation
    const channel = new MessageChannel();
    
    // Route port1 to the server's host
    // (In a real implementation, you'd send port1 to the server worker)
    this.routeToServer(channel.port1);
    
    // Return port2 as our channel
    return new MessageChannelChannel(channel.port2);
  }

  async acceptChannel(): Promise<Channel> {
    // If there's a channel in the queue, return it
    if (this.channelQueue.length > 0) {
      return this.channelQueue.shift()!;
    }
    
    // Otherwise, wait for one to arrive
    return new Promise((resolve) => {
      this.queueWaiters.push(resolve);
    });
  }

  // Called by the host when a channel arrives from the client
  // This is NOT part of the WIT interface - it's your host's internal mechanism
  onChannelReceived(channel: Channel): void {
    if (this.queueWaiters.length > 0) {
      const waiter = this.queueWaiters.shift()!;
      waiter(channel);
    } else {
      this.channelQueue.push(channel);
    }
  }

  private routeToServer(port: MessagePort): void {
    // Implementation depends on your setup
    // For example, if using SharedWorker or BroadcastChannel:
    // serverWorker.postMessage({ type: 'channel', port }, [port]);
  }
}

// Implement the channel resource
class MessageChannelChannel implements Channel {
  constructor(private port: MessagePort) {
    this.port.start();
  }

  read(): ReadableStream<Uint8Array> {
    // Convert MessagePort messages to a ReadableStream
    // The exact return type depends on how your bindings generator handles `stream<list<u8>>`
    return new ReadableStream({
      start: (controller) => {
        this.port.onmessage = (event) => {
          if (event.data === null || event.data.length === 0) {
            controller.close();
          } else {
            controller.enqueue(new Uint8Array(event.data));
          }
        };
        this.port.onerror = () => controller.error(new Error('Channel error'));
      }
    });
  }

  async write(data: Uint8Array): Promise<void> {
    this.port.postMessage(data);
  }

  async close(): Promise<void> {
    this.port.close();
  }
}

// When instantiating your WASM component, provide the transport implementation
// The exact API depends on your WASM runtime (wasmtime, wasmer, etc.)
async function setupWasmComponent() {
  const transport = new MessageChannelTransport();
  
  // Provide the transport implementation to the WASM component
  // This is typically done through the WASM runtime's host function registration
  // Example (pseudo-code, actual API depends on runtime):
  // wasmInstance.exports.setTransport(transport);
}
```

## Integration with WASM Component

When you build the wRPC Rust code as a WASM component, it will import the `wrpc:host-transport/transport` interface. You need to:

1. **Generate TypeScript bindings** from the WIT file (using `npm run generate-bindings`)
2. **Implement the interface** in TypeScript (as shown in the example above)
3. **Register the implementation** with your WASM runtime when instantiating the component

The exact registration API depends on your WASM runtime:
- **wasmtime**: Use `Linker::func_wrap()` or similar to register host functions
- **wasmer**: Use `Instance::exports()` to provide host functions
- **Other runtimes**: Consult their documentation for importing host functions

## Structure

- `package.json` - Node.js project configuration with scripts
- `bindings/` - Generated TypeScript bindings (created by `generate-bindings` script)
- `test/` - Test files
- `src/` - Implementation examples

## Generated Bindings

After running `npm run generate-bindings`, you'll find TypeScript definitions in `./bindings/`:

- `transport.d.ts` - Main export file
- `interfaces/wrpc-host-transport-transport.d.ts` - Interface definitions

The bindings provide:
- `Channel` class with `read()`, `write()`, and `close()` methods
- `createInvocationChannel()` and `acceptChannel()` functions
- Type mappings:
  - `stream<list<u8>>` → `ReadableStream<Uint8Array>`
  - `future<tuple<>>` → `Promise<[]>` (for void futures)

## Notes

- The TypeScript code is the **host** - it implements the interface that the WASM component imports
- The WIT file uses `future<tuple<>>` instead of `future<()>` for compatibility with `jco`
- The `stream<list<u8>>` and `future<T>` types are wRPC extensions to WIT - `jco` supports them
- You'll need to adapt the example to your specific host environment (Web Workers, Node.js, etc.)
- The WASM component will call your TypeScript implementation - you don't need to componentize the TypeScript code
