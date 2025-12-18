# Plan Critique: Minimizing Code Duplication

## Key Insight: All Transports Follow the Same Pattern

After analyzing `crates/transport`, I found that **all transports follow the exact same pattern**:

### Pattern Analysis

**TCP Transport (`tcp/tokio.rs`)**:
```rust
impl Invoke for Client<T> {
    async fn invoke(...) -> Result<(Outgoing, Incoming)> {
        let stream = TcpStream::connect(...).await?;  // Get stream
        let (rx, tx) = stream.into_split();
        invoke(tx, rx, instance, func, params, paths).await  // Call frame::invoke
    }
}
```

**Memory Transport (`transport-memory/src/lib.rs`)**:
```rust
impl Invoke for Client {
    async fn invoke(...) -> Result<(Outgoing, Incoming)> {
        let (server_rx, client_tx) = simplex(65536);  // Get streams
        let (client_rx, server_tx) = simplex(65536);
        invoke(client_tx, client_rx, instance, func, params, paths).await  // Call frame::invoke
    }
}
```

**Server Pattern**:
- Use `crate::frame::conn::Server`
- Implement `Accept` trait that provides streams
- Call `server.accept(listener)`

## Critique of Original Plan

### ❌ Problem 1: Unnecessary API Split

**Original Plan**: Two APIs (`native` and `component`)

**Reality**: There's no fundamental difference! Both just need to:
1. Get bidirectional streams (from TCP, memory, or host)
2. Call `frame::invoke()` 
3. Return `(Outgoing, Incoming)`

**Solution**: **Single unified API** - just implement `Invoke` and `Accept` traits like all other transports.

### ❌ Problem 2: Unnecessary `HostTransport` Trait

**Original Plan**: Create a `HostTransport` trait to abstract host calls

**Reality**: This is over-engineering. In WASM components:
- You import the WIT interface via `wit_bindgen::generate!`
- You call the imported functions directly
- No trait needed!

**Solution**: **Just call the imported WIT functions directly** in the `Invoke`/`Accept` implementations.

### ❌ Problem 3: Duplication of Stream Conversion Logic

**Original Plan**: Separate `HostAsyncRead`/`HostAsyncWrite` implementations

**Reality**: The `ignore.rs` implementation already has this, but it's doing the same thing as:
- `tcp/wasi.rs` converts WASI streams to `AsyncRead`/`AsyncWrite`
- Other transports do similar conversions

**Solution**: **Reuse the existing pattern** - convert WIT channel to `AsyncRead`/`AsyncWrite`, then use standard `frame::invoke()`.

## Revised Approach: Follow Existing Patterns

### Single Unified Implementation

```rust
// In transport-host/src/lib.rs

mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "host-transport",
    });
}

use bindings::wrpc::host_transport::transport;

/// Client that implements Invoke by calling host-provided transport
#[derive(Clone, Copy, Debug)]
pub struct Client;

impl Invoke for Client {
    type Context = ();
    type Outgoing = Outgoing;
    type Incoming = Incoming;

    async fn invoke<P>(...) -> Result<(Self::Outgoing, Self::Incoming)>
    where P: AsRef<[Option<usize>]> + Send + Sync,
    {
        // 1. Get channel from host (same pattern as TCP gets stream)
        let channel = transport::create_invocation_channel().await?;
        
        // 2. Convert channel to AsyncRead/AsyncWrite (same pattern as TCP splits stream)
        let read = ChannelAsyncRead::new(channel);
        let write = ChannelAsyncWrite::new(channel);
        
        // 3. Call frame::invoke (SAME as all other transports!)
        invoke(write, read, instance, func, params, paths).await
    }
}

/// Accept implementation that gets channels from host
pub struct Listener;

impl Accept for Listener {
    type Context = ();
    type Outgoing = ChannelAsyncWrite;
    type Incoming = ChannelAsyncRead;

    async fn accept(&self) -> Result<(Self::Context, Self::Outgoing, Self::Incoming)> {
        // 1. Get channel from host queue (same pattern as TCP accepts connection)
        let channel = transport::accept_channel().await?;
        
        // 2. Convert to AsyncRead/AsyncWrite
        let read = ChannelAsyncRead::new(channel);
        let write = ChannelAsyncWrite::new(channel);
        
        Ok(((), write, read))
    }
}

/// Server that uses the standard frame::conn::Server
pub type Server = crate::frame::conn::Server<(), ChannelAsyncWrite, ChannelAsyncRead>;
```

### Key Differences from Original Plan

1. **No `HostTransport` trait** - just call imported WIT functions directly
2. **No separate APIs** - single implementation that works everywhere
3. **Reuse `frame::conn::Server`** - don't reimplement server logic
4. **Same pattern as other transports** - minimal code, maximum reuse

### What We Actually Need

1. **`ChannelAsyncRead`** / **`ChannelAsyncWrite`**: Convert WIT `channel` resource to `AsyncRead`/`AsyncWrite`
   - Similar to how `tcp/wasi.rs` converts WASI streams
   - Can reuse most of the logic from `ignore.rs`

2. **`Client`**: Implements `Invoke` by calling `transport::create_invocation_channel()`
   - Follows exact same pattern as TCP/Memory transports

3. **`Listener`**: Implements `Accept` by calling `transport::accept_channel()`
   - Follows exact same pattern as `TcpListener` implements `Accept`

4. **`Server`**: Just type alias to `frame::conn::Server`
   - No custom implementation needed!

## Code Reuse Opportunities

### 1. Reuse `frame::invoke()` ✅
- All transports use this - no change needed

### 2. Reuse `frame::conn::Server` ✅
- Standard server implementation - just use it!

### 3. Reuse Stream Conversion Pattern
- Look at `tcp/wasi.rs` for inspiration on converting WIT resources to `AsyncRead`/`AsyncWrite`
- The `ignore.rs` implementation already has this - just clean it up

### 4. Reuse `Accept` Pattern
- Look at how `TcpListener` implements `Accept`
- Our `Listener` follows the same pattern

## Revised File Structure

```
crates/transport-host/
├── src/
│   ├── lib.rs              # Public API: Client, Server, Listener
│   ├── channel.rs          # ChannelAsyncRead, ChannelAsyncWrite (from ignore.rs)
│   └── bindings/           # Generated WIT bindings
├── wit/
│   └── transport.wit       # WIT interface (already exists)
└── Cargo.toml
```

**Much simpler!** No `native.rs`, no `component.rs`, no `common.rs` - just the essentials.

## Benefits of Revised Approach

1. **Zero duplication** - follows exact same pattern as other transports
2. **Minimal code** - ~200 lines instead of ~500+
3. **Consistent API** - works exactly like TCP/Memory transports
4. **Easy to understand** - if you understand TCP transport, you understand host transport
5. **Easy to test** - same testing patterns as other transports

## Implementation Steps (Revised)

1. ✅ Extract `ChannelAsyncRead`/`ChannelAsyncWrite` from `ignore.rs` → `channel.rs`
2. ✅ Implement `Client` that calls `transport::create_invocation_channel()` → `lib.rs`
3. ✅ Implement `Listener` that calls `transport::accept_channel()` → `lib.rs`
4. ✅ Type alias `Server = frame::conn::Server<...>` → `lib.rs`
5. ✅ Update examples to use the standard pattern

## Open Questions Resolved

1. **Q: Should we support both native and component APIs?**
   - **A: No!** There's no difference - WASM components just import WIT and call functions.

2. **Q: How to handle resource types?**
   - **A: Same as `tcp/wasi.rs`** - convert WIT resources to `AsyncRead`/`AsyncWrite` using `wit_bindgen` resource types.

3. **Q: Should `hello-host-server` use in-memory or MessageChannel?**
   - **A: Both!** In-memory for testing (like `transport-memory`), MessageChannel for real JS host.

## Conclusion

The original plan was over-engineered. By following the **exact same pattern** as existing transports, we get:
- **90% less code**
- **Zero duplication**
- **Consistent API**
- **Easier maintenance**

The only unique part is converting WIT `channel` resources to `AsyncRead`/`AsyncWrite`, which is similar to what `tcp/wasi.rs` already does for WASI streams.
