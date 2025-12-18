//! wRPC host transport for host-defined transports
//!
//! This transport allows the host (e.g., JavaScript in a browser) to provide
//! the transport implementation via a WIT interface. This is useful for
//! constrained environments that may not be able to use other transports.
//!
//! The host must implement the `wrpc:host-transport/transport` WIT interface.
//! See the README for details on the interface and flow.

use core::pin::Pin;
use core::task::{ready, Context, Poll};

use anyhow::Context as _;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt as _};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::StreamReader;
use wrpc_transport::frame::{invoke, Accept, Incoming, Outgoing};
use wrpc_transport::Server as TransportServer;
use wrpc_transport::{Index, Invoke, Serve};

// Generate bindings from the WIT interface
// These bindings are guaranteed to stay in sync with the WIT file
mod bindings {
    wit_bindgen_wrpc::generate!({
        path: "wit",
        world: "host-transport",
    });
}

// Re-export the generated types for convenience
pub use bindings::wrpc::host_transport::transport::Channel;

use bindings::wrpc::host_transport::transport;

/// Trait for calling the host-provided transport interface
///
/// This trait allows the guest (WASM component) to call the host-provided
/// interface without requiring `Invoke`. The host (e.g., JavaScript) implements
/// this interface and provides channels for communication.
pub trait HostTransport: Send + Sync + Clone {
    /// Context type for host calls
    type Context: Send + Sync + Clone;

    /// Create a new channel for a client invocation
    ///
    /// This calls the host-provided `create-invocation-channel` function.
    fn create_invocation_channel(
        &self,
        cx: Self::Context,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Channel>> + Send>>;

    /// Accept an incoming channel from the queue
    ///
    /// This calls the host-provided `accept-channel` function.
    fn accept_channel(
        &self,
        cx: Self::Context,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Channel>> + Send>>;

    /// Read from a channel
    ///
    /// This calls the host-provided `channel.read()` method.
    fn channel_read(
        &self,
        cx: Self::Context,
        channel: &Channel,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Pin<Box<dyn Stream<Item = anyhow::Result<Vec<u8>>> + Send>>>> + Send>>;

    /// Write to a channel
    ///
    /// This calls the host-provided `channel.write()` method.
    fn channel_write(
        &self,
        cx: Self::Context,
        channel: &Channel,
        data: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;

    /// Close a channel
    ///
    /// This calls the host-provided `channel.close()` method.
    fn channel_close(
        &self,
        cx: Self::Context,
        channel: &Channel,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
}

/// Host wRPC client
///
/// This client creates a new channel for each invocation and uses it to
/// communicate with the server via the host.
pub struct Client<T: HostTransport> {
    transport: T,
    context: T::Context,
}

impl<T: HostTransport> Client<T> {
    /// Create a new client with access to the host transport
    pub fn new(transport: T, context: T::Context) -> Self {
        Self { transport, context }
    }
}

/// Host wRPC server
///
/// This server accepts channels from the host queue and processes them.
pub struct Server<T: HostTransport> {
    transport: T,
    context: T::Context,
    transport_server: TransportServer<(), HostAsyncRead<T>, HostAsyncWrite<T>>,
}

impl<T: HostTransport + 'static> Server<T> {
    /// Create a new server with access to the host transport
    pub fn new(transport: T, context: T::Context) -> Self {
        Self {
            transport,
            context,
            transport_server: TransportServer::new(),
        }
    }

    /// Accept a single connection (channel) from the host
    ///
    /// This processes one channel and routes it to registered handlers.
    pub async fn accept(&self) -> anyhow::Result<()> {
        let listener = HostListener {
            transport: &self.transport,
            context: &self.context,
        };

        self.transport_server
            .accept(listener)
            .await
            .map_err(|err| anyhow::anyhow!("failed to accept connection: {err}"))
    }

    /// Start accepting connections in a loop
    ///
    /// This should be called in a background task. It will continuously accept
    /// channels from the host and route them to registered handlers.
    pub async fn accept_loop(&self) -> anyhow::Result<()> {
        loop {
            self.accept().await?;
        }
    }
}

/// Listener that provides channels from the host
struct HostListener<'a, T: HostTransport> {
    transport: &'a T,
    context: &'a T::Context,
}

impl<'a, T: HostTransport + 'static> Accept for HostListener<'a, T> {
    type Context = ();
    type Outgoing = HostAsyncWrite<T>;
    type Incoming = HostAsyncRead<T>;

    async fn accept(&self) -> std::io::Result<(Self::Context, Self::Outgoing, Self::Incoming)> {
        // Call the host-provided accept-channel function
        let channel = self.transport
            .accept_channel(self.context.clone())
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        // Convert channel to AsyncRead/AsyncWrite
        let read = HostAsyncRead::new(&self.transport, &self.context, channel);
        let write = HostAsyncWrite::new(&self.transport, &self.context, channel);

        Ok(((), write, read))
    }
}

/// Adapter that converts a WIT stream to AsyncRead
///
/// Uses StreamReader internally to convert the stream to AsyncRead.
struct HostAsyncRead<T: HostTransport> {
    inner: StreamReader<ReceiverStream<std::io::Result<Bytes>>, Bytes>,
    channel: std::sync::Arc<Channel>,
    transport: std::sync::Arc<T>,
    context: std::sync::Arc<T::Context>,
}

impl<T: HostTransport> HostAsyncRead<T> {
    fn new(transport: &T, context: &T::Context, channel: Channel) -> Self {
        // Call the channel.read() method through the transport
        let channel_arc = std::sync::Arc::new(channel);
        let transport_clone = transport.clone();
        let context_clone = context.clone();
        let read_stream_fut = transport.channel_read(context.clone(), channel_arc.as_ref());
        
        // Convert the stream to a channel-based stream for StreamReader
        let (tx, rx) = mpsc::channel(128);
        
        // Spawn a task to forward items from the WIT stream to our channel
        tokio::spawn(async move {
            let mut stream = read_stream_fut.await
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?;
            while let Some(result) = stream.next().await {
                let item = result
                    .map(|vec| vec.into())
                    .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err));
                if tx.send(item).await.is_err() {
                    break; // Receiver dropped
                }
            }
            Ok::<_, std::io::Error>(())
        });

        Self {
            inner: StreamReader::new(ReceiverStream::new(rx)),
            channel: channel_arc,
            transport: std::sync::Arc::new(transport_clone),
            context: std::sync::Arc::new(context_clone),
        }
    }
}

impl<T: HostTransport> AsyncRead for HostAsyncRead<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: HostTransport> Index<HostAsyncRead<T>> for HostAsyncRead<T> {
    fn index(&self, _path: &[usize]) -> anyhow::Result<HostAsyncRead> {
        // Host transport doesn't support indexing at the channel level
        // The wRPC framing layer handles indexing via frames on the same channel
        // So we can't create indexed sub-streams, but that's okay because
        // the framing layer will handle it
        anyhow::bail!("host transport channels do not support indexing - use wRPC framing layer")
    }
}

/// Adapter that converts AsyncWrite operations to channel write calls
struct HostAsyncWrite<T: HostTransport> {
    channel: std::sync::Arc<Channel>,
    buffer: BytesMut,
    write_task: Mutex<Option<Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>>>>,
    transport: std::sync::Arc<T>,
    context: std::sync::Arc<T::Context>,
}

impl<T: HostTransport> HostAsyncWrite<T> {
    fn new(transport: &T, context: &T::Context, channel: Channel) -> Self {
        Self {
            channel: std::sync::Arc::new(channel),
            buffer: BytesMut::new(),
            write_task: Mutex::new(None),
            transport: std::sync::Arc::new(transport.clone()),
            context: std::sync::Arc::new(context.clone()),
        }
    }
}

impl<T: HostTransport> AsyncWrite for HostAsyncWrite<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        // Buffer the data - we'll flush it in poll_flush
        self.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // If there's a write task in progress, poll it
        let mut write_task_guard = match self.write_task.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                // Lock is held, can't poll - return pending
                return Poll::Pending;
            }
        };

        if let Some(mut task) = write_task_guard.take() {
            match task.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    // Write completed, continue to check buffer
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e,
                    )));
                }
                Poll::Pending => {
                    *write_task_guard = Some(task);
                    return Poll::Pending;
                }
            }
        }

        // If buffer is empty, we're done
        if self.buffer.is_empty() {
            return Poll::Ready(Ok(()));
        }

        // Extract data before we need to access self again
        let data = {
            // We can't access self.buffer while holding the lock, so we need to
            // drop the lock first, extract the data, then re-lock
            drop(write_task_guard);
            let this = self.as_mut().get_mut();
            this.buffer.split().freeze().to_vec()
        };
        
        // Call channel.write() through the transport
        let write_fut = self.transport.channel_write(
            self.context.as_ref().clone(),
            self.channel.as_ref(),
            data,
        );
        
        // Re-lock and store the future
        let mut write_task_guard = match self.write_task.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                // Lock is held, can't store - return pending
                return Poll::Pending;
            }
        };
        *write_task_guard = Some(Box::pin(write_fut));
        drop(write_task_guard);
        
        // Poll the new task
        self.poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // First, flush any remaining data
        ready!(self.as_mut().poll_flush(cx))?;

        // Call channel.close() through the transport
        let mut close_fut = self.transport.channel_close(
            self.context.as_ref().clone(),
            self.channel.as_ref(),
        );
        match Pin::new(&mut close_fut).poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                e,
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: HostTransport> Index<HostAsyncWrite<T>> for HostAsyncWrite<T> {
    fn index(&self, _path: &[usize]) -> anyhow::Result<HostAsyncWrite> {
        // Host transport doesn't support indexing at the channel level
        anyhow::bail!("host transport channels do not support indexing - use wRPC framing layer")
    }
}

impl<T: HostTransport + 'static> Invoke for Client<T> {
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
        // Call the host-provided create-invocation-channel function
        let channel = self.transport
            .create_invocation_channel(self.context.clone())
            .await
            .context("failed to create invocation channel")?;

        // Convert channel to AsyncRead/AsyncWrite
        let read = HostAsyncRead::new(&self.transport, &self.context, channel);
        let write = HostAsyncWrite::new(&self.transport, &self.context, channel);

        // Set up wRPC framing
        invoke(write, read, instance, func, params, paths)
            .await
            .context("failed to set up client invocation")
    }
}

impl<T: HostTransport + 'static> Serve for Server<T> {
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
        self.transport_server.serve(instance, func, paths).await
    }
}
