# wRPC Host Transport

This crate allows creating host-defined transports. This is useful for constrained environments (ex: browsers) that may not be able to use other transports.

## Architecture

wRPC transports must be multiplexed and bidirectional. However, transports available to the host may not have this property (ex: `MessageChannel` in the browser). To address this, a channel is requested from the host for every `invoke` call by the Client, and the channel close request is sent after the request is complete.

### Flow

1. **Client side (Rust)**: User calls `client.foo()` → `invoke()` creates channel from host → sends request → waits for response
2. **Client side (Host)**: Receives message from server's host → continues client's Rust
3. **Server side (Host)**: Receives message from client's host → adds channel to queue
4. **Server side (Rust)**: `accept()` pulls channel from queue → processes → sends response back to client's host
5. **Client side (Host)**: Receives response → continues client's Rust to finish `client.foo()` call

### WIT Interface

The host must implement the `wrpc:host-transport/transport` interface. The WIT file is located at `wit/transport.wit`.

See `tests/nodejs/README.md` for details on generating TypeScript/JavaScript bindings and implementation examples.

The interface defines:

- **`channel` resource**: A bidirectional channel with `read()`, `write()`, and `close()` methods
- **`create-invocation-channel()`**: Creates a new channel for client invocations
- **`accept-channel()`**: Accepts incoming channels from the server's queue

### Implementation Notes

- Each `invoke()` call creates a new channel (since underlying transports may not be multiplexed)
- Channels are closed after the request completes
- The host manages routing between client and server hosts
- The server's host maintains a queue of incoming channels
- `stream<list<u8>>` and `future<()>` are used to avoid polling