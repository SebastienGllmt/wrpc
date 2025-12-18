# wRPC Host Transport - Implementation Plan

## Overview

The goal is to create a wRPC transport where the transport implementation is provided by the host (JavaScript/TypeScript), using MessageChannel or similar mechanisms. This is useful for constrained environments like browsers where network transports aren't available.

## Architecture Analysis

### Current State

1. **`transport-host` crate** (`crates/transport-host/`):
   - ✅ WIT interface defined (`wit/transport.wit`)
   - ✅ Complete implementation exists in `src/ignore.rs` (but not exposed)
   - ⚠️ Current `lib.rs` has placeholder code
   - ⚠️ Uses `HostTransport` trait approach (good for native Rust, but needs WASM component support)

2. **`hello-host-server` example** (`examples/rust/hello-host-server/`):
   - ⚠️ Incomplete - just has TODOs
   - Needs to show how to use the transport with wasmtime

3. **`message-channel` example** (`examples/web/message-channel/`):
   - ✅ Good README documentation
   - ⚠️ Missing actual implementation
   - Needs TypeScript/JavaScript host implementation

### Key Insight

There are **two different use cases** that need different approaches:

1. **Native Rust code** (like `wasmtime-cli`): Can use traits and manual implementations
2. **WASM Components**: Must use `wit_bindgen::generate!` to import WIT interfaces directly

The existing `ignore.rs` implementation uses a `HostTransport` trait, which works for native Rust but needs adaptation for WASM components.

## Implementation Plan

### Phase 1: Complete `transport-host` Crate

#### 1.1 Expose the Core Implementation

Move the working code from `ignore.rs` to `lib.rs` and make it work for both use cases:

**Option A: Two APIs (Recommended)**
- **Native Rust API**: Use `HostTransport` trait (like `ignore.rs`)
- **WASM Component API**: Use generated WIT bindings directly

**Option B: Unified API**
- Create a wrapper that works for both, but this might be complex

**Decision: Option A** - Keep them separate but in the same crate:
- `wrpc_transport_host::native::Client` / `Server` - for native Rust
- `wrpc_transport_host::component::Client` / `Server` - for WASM components

#### 1.2 WASM Component API

For WASM components, we need to:
1. Use `wit_bindgen::generate!` to import the WIT interface
2. Create `Client` and `Server` that call the imported functions directly
3. These implement `Invoke` and `Serve` traits

```rust
// In component code:
mod bindings {
    wit_bindgen::generate!({
        path: "../transport-host/wit",
        world: "host-transport",
    });
}

// Client that uses the imported bindings
pub struct ComponentClient;

impl Invoke for ComponentClient {
    // Calls bindings::wrpc::host_transport::transport::create_invocation_channel()
    // etc.
}
```

#### 1.3 File Structure

```
crates/transport-host/
├── src/
│   ├── lib.rs              # Public API, re-exports
│   ├── native.rs           # Native Rust implementation (from ignore.rs)
│   ├── component.rs        # WASM component implementation
│   └── common.rs           # Shared utilities (HostAsyncRead, HostAsyncWrite)
├── wit/
│   └── transport.wit       # WIT interface (already exists)
└── Cargo.toml
```

### Phase 2: Complete `hello-host-server` Example

#### 2.1 Server Implementation

The server needs to:
1. Load a WASM component that uses `transport-host`
2. Implement the host transport interface in Rust (for testing)
3. Link the host implementation to the component
4. Run the component

```rust
// Pseudo-code structure:
async fn main() {
    // 1. Create a host transport implementation (for testing, this could be in-memory)
    let host_transport = InMemoryHostTransport::new();
    
    // 2. Load WASM component
    let component = Component::from_file(&engine, "hello-component-server.wasm")?;
    
    // 3. Create linker and provide host transport implementation
    let mut linker = Linker::new(&engine);
    // Link the wrpc:host-transport/transport interface
    bindings::wrpc::host_transport::transport::add_to_linker(&mut linker, |ctx| {
        HostTransportImpl { /* ... */ }
    })?;
    
    // 4. Instantiate and run
    let instance = linker.instantiate(&mut store, &component)?;
    // ...
}
```

#### 2.2 Key Challenge: Linking Host Functions

The component imports `wrpc:host-transport/transport`. We need to:
1. Generate bindings for the host side (using `wasmtime::component::bindgen!`)
2. Implement the `Host` trait
3. Add to linker

This is similar to how `wrpc-runtime-wasmtime` links `wrpc:rpc` interfaces.

### Phase 3: Complete `message-channel` Example

#### 3.1 TypeScript/JavaScript Implementation

Create a Node.js project that:
1. Generates TypeScript bindings from the WIT file (using `jco` or similar)
2. Implements the `wrpc:host-transport/transport` interface
3. Uses MessageChannel for communication
4. Instantiates WASM components and provides the host implementation

#### 3.2 Structure

```
examples/web/message-channel/
├── package.json
├── src/
│   ├── host-transport.ts    # Host transport implementation
│   ├── server.ts            # Server setup
│   └── client.ts             # Client setup
├── bindings/                # Generated TypeScript bindings
└── README.md
```

#### 3.3 Implementation Steps

1. **Generate bindings**: Use `jco` to generate TypeScript types from WIT
2. **Implement Channel**: Convert MessageChannel ports to the WIT `channel` resource
3. **Implement Transport**: Create the main transport class
4. **Wire up**: Use wasmtime-js or similar to instantiate components

### Phase 4: Integration with `wrpc-runtime-wasmtime`

#### 4.1 Helper Function

Create a helper in `wrpc-runtime-wasmtime` (or `transport-host`) to easily set up the host transport:

```rust
pub fn add_host_transport_to_linker<T>(
    linker: &mut Linker<T>,
    transport: impl HostTransport + 'static,
) -> anyhow::Result<()>
where
    T: WrpcView,
{
    // Generate bindings and link
    bindings::wrpc::host_transport::transport::add_to_linker(linker, |ctx| {
        HostTransportImpl::new(transport)
    })
}
```

#### 4.2 Usage in Components

Components that want to use host transport would:
1. Import `wrpc:host-transport/transport` in their WIT world
2. Use `wrpc_transport_host::component::Client` / `Server`
3. The host provides the implementation

## Detailed Implementation Steps

### Step 1: Refactor `transport-host/src/lib.rs`

1. Move `ignore.rs` → `native.rs`
2. Create `component.rs` with WASM component support
3. Create `common.rs` for shared types
4. Update `lib.rs` to expose both APIs

### Step 2: Create WASM Component Client/Server

In `component.rs`:
- Use `wit_bindgen::generate!` to import the interface
- Implement `Invoke` for `Client` by calling imported functions
- Implement `Serve` for `Server` by calling imported functions
- Handle resource types correctly (Channel)

### Step 3: Complete `hello-host-server`

1. Create a simple in-memory host transport implementation (for testing)
2. Load a WASM component
3. Link the host transport interface
4. Run the component

### Step 4: Create TypeScript Bindings Generator

1. Set up `jco` or similar tool
2. Generate bindings from WIT file
3. Document the process

### Step 5: Implement JavaScript Host Transport

1. Implement `Channel` resource using MessageChannel
2. Implement `create_invocation_channel` and `accept_channel`
3. Wire up with wasmtime-js or similar

### Step 6: End-to-End Test

1. Build a WASM component that uses host transport
2. Run server in Node.js
3. Run client in Node.js (or browser)
4. Verify communication works

## Key Design Decisions

### 1. Two APIs vs Unified API

**Decision**: Two separate APIs (`native` and `component`)
- **Rationale**: Different constraints (native Rust can use traits, WASM must use WIT)
- **Benefit**: Clear separation, easier to understand
- **Cost**: Some code duplication, but shared utilities help

### 2. Resource Handling

**Challenge**: WIT resources need careful handling in both Rust and JS

**Solution**: 
- Rust: Use `wit_bindgen` resource types
- JS: Use wasmtime-js resource handling
- Document the resource lifecycle clearly

### 3. Channel Multiplexing

**Challenge**: MessageChannel isn't multiplexed, but wRPC needs multiplexing

**Solution**: 
- One channel per invocation (as documented in README)
- Host manages routing
- Channels closed after request completes

### 4. Error Handling

**Decision**: Use `anyhow::Result` in Rust, standard JS errors in TypeScript

**Rationale**: Consistent with rest of wRPC codebase

## Testing Strategy

1. **Unit Tests**: Test `Client` and `Server` implementations
2. **Integration Tests**: Test with actual WASM components
3. **E2E Tests**: Test full flow with JS host

## Documentation Needs

1. Architecture overview (already in README)
2. Usage guide for native Rust
3. Usage guide for WASM components
4. JavaScript/TypeScript host implementation guide
5. Examples for both use cases

## Open Questions

1. **Should we support both native and component APIs in the same crate?**
   - ✅ Yes - they serve different use cases

2. **How to handle resource types in JS?**
   - Use wasmtime-js resource APIs
   - May need to create wrapper types

3. **Should `hello-host-server` use in-memory transport or real MessageChannel?**
   - Start with in-memory for testing
   - Add MessageChannel example separately

4. **How to generate TypeScript bindings?**
   - Use `jco` (JavaScript Component tooling)
   - Or create custom generator
   - Document the process

## Next Steps

1. ✅ Review this plan
2. Refactor `transport-host` crate structure
3. Implement WASM component API
4. Complete `hello-host-server` example
5. Create TypeScript bindings
6. Implement JavaScript host transport
7. End-to-end testing
