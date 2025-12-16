//! wRPC in-memory transport for same-process component communication
//!
//! This transport allows components running in the same process to communicate
//! via in-memory bidirectional streams, without requiring network connections.
//!
//! The main use case is for a "host" component that orchestrates multiple "child" components.
//! Instead of each child requiring its own network connection, you can use in-memory streams
//! to route calls internally.

use anyhow::Context as _;
use bytes::Bytes;
use wrpc_transport::frame::{invoke, Accept, Incoming, Outgoing};
use wrpc_transport::Server as TransportServer;
use wrpc_transport::{Invoke, Serve};

/// In-memory wRPC client
/// It routes invocations to a per-component `Server` via in-memory streams.
#[derive(Clone, Debug)]
pub struct Client {
    server_tx: mpsc::UnboundedSender<(
        tokio::io::WriteHalf<SimplexStream>,
        tokio::io::ReadHalf<SimplexStream>,
    )>,
}

/// In-memory connection listener
///
/// In our model, the in-memory Client creates new io streams *per* request (per `invoke`)
/// These are received by the listener, to be accepted by the server later
#[derive(Clone, Debug)]
pub struct Listener {
    rx: std::sync::Arc<
        tokio::sync::Mutex<
            mpsc::UnboundedReceiver<(
                tokio::io::WriteHalf<SimplexStream>,
                tokio::io::ReadHalf<SimplexStream>,
            )>,
        >,
    >,
}

impl Accept for Listener {
    type Context = ();
    type Outgoing = tokio::io::WriteHalf<SimplexStream>;
    type Incoming = tokio::io::ReadHalf<SimplexStream>;

    async fn accept(&self) -> std::io::Result<(Self::Context, Self::Outgoing, Self::Incoming)> {
        let mut rx = self.rx.lock().await;
        rx.recv()
            .await
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "channel closed"))
            .map(|(tx, rx)| ((), tx, rx))
    }
}

impl Accept for &Listener {
    type Context = ();
    type Outgoing = tokio::io::WriteHalf<SimplexStream>;
    type Incoming = tokio::io::ReadHalf<SimplexStream>;

    async fn accept(&self) -> std::io::Result<(Self::Context, Self::Outgoing, Self::Incoming)> {
        (*self).accept().await
    }
}

/// In-memory wRPC server that handles invocations.
pub struct Server {
    /// `wrpc_transport::Server`, but using an in-memory `tokio::io::SimplexStream` for input and output
    transport_server: TransportServer<
        (),
        tokio::io::ReadHalf<SimplexStream>,
        tokio::io::WriteHalf<SimplexStream>,
    >,
    /// Queued streams created by client invocations, waiting to be accepted
    listener: Listener,
}

impl Server {
    /// Accept a single connection (ex: a single function call) from the listener.
    ///
    /// This processes one connection and routes it to registered handlers.
    pub async fn accept(&self) -> anyhow::Result<()> {
        self.transport_server
            .accept(&self.listener)
            .await
            .map_err(|err| anyhow::anyhow!("failed to accept connection: {err}"))
    }

    /// Start accepting connections in a loop.
    ///
    /// This should be called in a background task. It will continuously accept
    /// connections from the listener and route them to registered handlers.
    pub async fn accept_loop(&self) -> anyhow::Result<()> {
        loop {
            self.accept().await?;
        }
    }
}

/// Create a new client-server pair for in-memory communication.
///
/// Returns `(client, server)` where:
/// - `client` implements `Invoke` and can be used in a WASM Component's Store's context
/// - `server` implements `Serve` and can be used to register handlers
///
/// You should spawn a task that calls `server.accept_loop()` to start processing connections.
pub fn new_memory_transport() -> (Client, Server) {
    // tx → Client
    // rx → Listener (which Server reads from when accepting connections)
    let (tx, rx) = mpsc::unbounded_channel();
    (
        Client { server_tx: tx },
        Server {
            transport_server: TransportServer::new(),
            listener: Listener {
                rx: std::sync::Arc::new(tokio::sync::Mutex::new(rx)),
            },
        },
    )
}

impl Invoke for Client {
    type Context = ();
    type Outgoing = Outgoing;
    type Incoming = Incoming;

    async fn invoke<P>(
        &self,
        (): Self::Context,
        instance: &str,
        func: &str,
        params: Bytes,
        paths: impl AsRef<[P]> + Send,
    ) -> anyhow::Result<(Self::Outgoing, Self::Incoming)>
    where
        P: AsRef<[Option<usize>]> + Send + Sync,
    {
        // Create bidirectional in-memory streams
        // Client -> Server: client writes to client_tx, server reads from server_rx
        // Server -> Client: server writes to server_tx, client reads from client_rx
        let (server_rx, client_tx) = simplex(65536);
        let (client_rx, server_tx) = simplex(65536);

        // Set up wRPC framing on client side (writes invocation header + params)
        let (client_outgoing, client_incoming) =
            invoke(client_tx, client_rx, instance, func, params, paths)
                .await
                .context("failed to set up client invocation")?;

        // Send raw server-side streams to the listener
        // The Server will use Accept to get these, read the header, and set up framing via serve()
        self.server_tx
            .send((server_tx, server_rx))
            .map_err(|_| anyhow::anyhow!("server channel closed"))?;

        Ok((client_outgoing, client_incoming))
    }
}

impl Serve for Server {
    type Context = ();
    type Outgoing = Outgoing;
    type Incoming = Incoming;

    async fn serve(
        &self,
        instance: &str,
        func: &str,
        paths: impl Into<std::sync::Arc<[Box<[Option<usize>]>]>> + Send,
    ) -> anyhow::Result<
        impl futures::Stream<Item = anyhow::Result<(Self::Context, Self::Outgoing, Self::Incoming)>>
            + Send
            + 'static,
    > {
        // Delegate to the transport_server's serve method
        // It will handle routing connections based on instance/func by reading the header
        self.transport_server.serve(instance, func, paths).await
    }
}
