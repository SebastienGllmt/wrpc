//! wRPC host transport for host-defined transports
//!
//! This transport allows the host (e.g., JavaScript in a browser) to provide
//! the transport implementation via a WIT interface. This is useful for
//! constrained environments that may not be able to use other transports.
//!
//! The host must implement the `wrpc:host-transport/transport` WIT interface.
//! See the README for details on the interface and flow.

use core::future::Future;
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

/// Trait for the host transport interface
///
/// This trait corresponds to the `wrpc:host-transport/transport` WIT interface.
/// The host must implement this trait, typically by calling into WIT bindings
/// generated from the WIT interface.
pub trait HostTransport: Send + Sync {
    /// Handle to a channel resource
    type Channel: Channel + Send + Sync + 'static;

    /// Create a new channel for a client invocation
    ///
    /// The host should set up the channel (e.g., MessageChannel ports) and
    /// route it to the server's host, which will queue it for `accept_channel`.
    fn create_invocation_channel(&self) -> Pin<Box<dyn Future<Output = anyhow::Result<Self::Channel>> + Send>>;

    /// Accept an incoming channel from the queue
    ///
    /// Blocks until a channel is available (pushed by the host when it receives
    /// a message from the client's host).
    fn accept_channel(&self) -> Pin<Box<dyn Future<Output = anyhow::Result<Self::Channel>> + Send>>;
}

/// Trait for a channel resource
///
/// This trait corresponds to the `channel` resource in the WIT interface.
pub trait Channel: Send + Sync {
    /// Read data from the channel as a stream of byte chunks
    ///
    /// Returns a stream that yields `Vec<u8>` chunks. An empty chunk indicates
    /// the end of the stream.
    fn read(&self) -> Pin<Box<dyn Stream<Item = anyhow::Result<Vec<u8>>> + Send>>;

    /// Write a chunk of data to the channel
    fn write(&self, data: Vec<u8>) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;

    /// Close the channel (indicates no more writes)
    fn close(&self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
}

/// Host wRPC client
///
/// This client creates a new channel for each invocation and uses it to
/// communicate with the server via the host.
#[derive(Clone, Debug)]
pub struct Client<T: HostTransport> {
    transport: T,
}

impl<T: HostTransport> Client<T> {
    /// Create a new client with the given host transport
    pub fn new(transport: T) -> Self {
        Self { transport }
    }
}

/// Host wRPC server
///
/// This server accepts channels from the host queue and processes them.
pub struct Server<T: HostTransport> {
    transport: T,
    transport_server: TransportServer<(), HostAsyncRead, HostAsyncWrite<T>>,
}

impl<T: HostTransport + 'static> Server<T> {
    /// Create a new server with the given host transport
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            transport_server: TransportServer::new(),
        }
    }

    /// Accept a single connection (channel) from the host
    ///
    /// This processes one channel and routes it to registered handlers.
    pub async fn accept(&self) -> anyhow::Result<()> {
        let listener = HostListener {
            transport: &self.transport,
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
}

impl<T: HostTransport + 'static> Accept for HostListener<'_, T> {
    type Context = ();
    type Outgoing = HostAsyncWrite<T>;
    type Incoming = HostAsyncRead;

    async fn accept(&self) -> std::io::Result<(Self::Context, Self::Outgoing, Self::Incoming)> {
        let channel = self
            .transport
            .accept_channel()
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        
        let read_stream = channel.read();
        let read = HostAsyncRead::new(read_stream);
        let write: HostAsyncWrite<T> = HostAsyncWrite::new(channel);
        
        Ok(((), write, read))
    }
}

/// Adapter that converts a WIT stream to AsyncRead
///
/// Uses StreamReader internally to convert the stream to AsyncRead.
struct HostAsyncRead {
    inner: StreamReader<ReceiverStream<std::io::Result<Bytes>>, Bytes>,
}

impl HostAsyncRead {
    fn new(stream: Pin<Box<dyn Stream<Item = anyhow::Result<Vec<u8>>> + Send>>) -> Self {
        // Convert the stream to a channel-based stream for StreamReader
        let (tx, rx) = mpsc::channel(128);
        
        // Spawn a task to forward items from the WIT stream to our channel
        tokio::spawn(async move {
            let mut stream = stream;
            while let Some(result) = stream.next().await {
                let item = result
                    .map(|vec| vec.into())
                    .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err));
                if tx.send(item).await.is_err() {
                    break; // Receiver dropped
                }
            }
        });

        Self {
            inner: StreamReader::new(ReceiverStream::new(rx)),
        }
    }
}

impl AsyncRead for HostAsyncRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl Index<HostAsyncRead> for HostAsyncRead {
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
    channel: Pin<Box<T::Channel>>,
    buffer: BytesMut,
    write_task: Mutex<Option<Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>>>>,
}

impl<T: HostTransport> HostAsyncWrite<T> {
    fn new(channel: T::Channel) -> Self {
        Self {
            channel: Box::pin(channel),
            buffer: BytesMut::new(),
            write_task: Mutex::new(None),
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
        
        // Now get channel reference and create write future
        let channel_ref = self.channel.as_ref();
        let write_fut = channel_ref.write(data);
        
        // Re-lock and store the future
        let mut write_task_guard = match self.write_task.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                // Lock is held, can't store - return pending
                return Poll::Pending;
            }
        };
        *write_task_guard = Some(write_fut);
        drop(write_task_guard);
        
        // Poll the new task
        self.poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // First, flush any remaining data
        ready!(self.as_mut().poll_flush(cx))?;

        // Close the channel
        let mut close_fut = self.channel.as_ref().close();
        match close_fut.as_mut().poll(cx) {
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
    fn index(&self, _path: &[usize]) -> anyhow::Result<HostAsyncWrite<T>> {
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
        // Create a channel from the host
        let channel = self
            .transport
            .create_invocation_channel()
            .await
            .context("failed to create invocation channel")?;

        // Convert channel to AsyncRead/AsyncWrite
        let read_stream = channel.read();
        let read = HostAsyncRead::new(read_stream);
        let write: HostAsyncWrite<T> = HostAsyncWrite::new(channel);

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
