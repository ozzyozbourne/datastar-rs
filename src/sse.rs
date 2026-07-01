use {
    crate::{Compression, DatastarEvent, ExecuteScript, PatchElements, PatchSignals, SignalError},
    axum::{
        body::Body,
        http::{
            HeaderMap, HeaderValue, StatusCode,
            header::{CACHE_CONTROL, CONTENT_ENCODING, CONTENT_TYPE},
        },
        response::{IntoResponse, Response},
    },
    bytes::Bytes,
    futures_core::Stream,
    futures_util::{StreamExt, TryStreamExt},
    serde::Serialize,
    std::{convert::Infallible, future::Future},
    tokio::sync::mpsc,
    tokio_stream::wrappers::ReceiverStream,
    tracing::{Instrument, debug, error, trace, warn},
};

#[cfg(feature = "compression")]
use {
    async_compression::tokio::write::{BrotliEncoder, GzipEncoder, ZlibEncoder, ZstdEncoder},
    std::{
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
    },
    tokio::io::{AsyncWrite, AsyncWriteExt},
};

#[derive(Debug, thiserror::Error)]
pub enum SseError {
    #[error("failed to serialize signals: {0}")]
    Signals(#[from] SignalError),
    #[error("failed to compress event: {0}")]
    Compression(#[from] std::io::Error),
    #[error("SSE source stream failed: {0}")]
    Source(std::io::Error),
    #[error("SSE stream is closed")]
    Closed,
}

#[derive(Debug, Clone)]
pub struct DatastarSseBuilder {
    compression: Option<Compression>,
    channel_capacity: usize,
}

impl Default for DatastarSseBuilder {
    fn default() -> Self {
        Self {
            compression: None,
            channel_capacity: 32,
        }
    }
}

impl DatastarSseBuilder {
    pub fn compression(mut self, compression: Compression) -> Self {
        debug!(?compression, "configured Datastar SSE compression");
        self.compression = Some(compression);
        self
    }

    pub fn accept_encoding(mut self, accept_encoding: impl Into<String>) -> Self {
        let accept_encoding = accept_encoding.into();
        debug!(%accept_encoding, "configured Datastar SSE Accept-Encoding");
        self.compression = Some(Compression::default().accept_encoding(accept_encoding));
        self
    }

    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        let capacity = capacity.max(1);
        debug!(capacity, "configured Datastar SSE channel capacity");
        self.channel_capacity = capacity;
        self
    }

    pub fn stream<S, E>(self, stream: S) -> DatastarSse
    where
        S: Stream<Item = Result<DatastarEvent, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        trace!("creating Datastar SSE from stream");
        DatastarSse::from_stream_with_compression(stream, self.compression.as_ref())
    }

    pub fn events<I>(self, events: I) -> DatastarSse
    where
        I: IntoIterator<Item = DatastarEvent>,
        I::IntoIter: Send + 'static,
    {
        trace!("creating Datastar SSE from event iterator");
        let stream = futures_util::stream::iter(events.into_iter().map(Ok::<_, Infallible>));
        DatastarSse::from_stream_with_compression(stream, self.compression.as_ref())
    }

    pub fn channel(self) -> (DatastarSender, DatastarSse) {
        debug!(
            capacity = self.channel_capacity,
            "creating Datastar SSE channel"
        );
        let (tx, rx) = mpsc::channel(self.channel_capacity);
        let stream = ReceiverStream::new(rx);
        (
            DatastarSender { tx },
            DatastarSse::from_stream_with_compression(stream, self.compression.as_ref()),
        )
    }

    pub fn run<F, Fut>(self, f: F) -> DatastarSse
    where
        F: FnOnce(DatastarSender) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), SseError>> + Send + 'static,
    {
        let (sender, sse) = self.channel();
        tokio::spawn(
            async move {
                if let Err(err) = f(sender).await {
                    match err {
                        SseError::Closed => debug!("Datastar SSE client disconnected"),
                        err => error!(%err, "Datastar SSE task failed"),
                    }
                }
            }
            .instrument(tracing::info_span!("datastar_sse_task")),
        );
        sse
    }
}

pub struct DatastarSse {
    body: Body,
    content_encoding: Option<&'static str>,
}

impl DatastarSse {
    pub fn builder() -> DatastarSseBuilder {
        DatastarSseBuilder::default()
    }

    pub fn new<S, E>(stream: S) -> Self
    where
        S: Stream<Item = Result<DatastarEvent, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::from_stream_with_compression(stream, None)
    }

    pub fn events<I>(events: I) -> Self
    where
        I: IntoIterator<Item = DatastarEvent>,
        I::IntoIter: Send + 'static,
    {
        Self::builder().events(events)
    }

    fn from_stream_with_compression<S, E>(stream: S, compression: Option<&Compression>) -> Self
    where
        S: Stream<Item = Result<DatastarEvent, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        #[cfg(feature = "compression")]
        let algorithm = compression.and_then(Compression::selected_algorithm);
        #[cfg(not(feature = "compression"))]
        let algorithm = {
            let _ = compression;
            None::<crate::CompressionAlgorithm>
        };
        debug!(
            content_encoding = algorithm.map_or("none", crate::CompressionAlgorithm::encoding),
            "creating Datastar SSE response body"
        );
        let content_encoding = algorithm.map(crate::CompressionAlgorithm::encoding);
        #[cfg(feature = "compression")]
        if let Some(algorithm) = algorithm {
            return Self {
                body: Body::from_stream(compressed_stream(stream, algorithm)),
                content_encoding,
            };
        }

        let stream = stream
            .map(move |event| -> Result<Bytes, SseError> {
                let event = event.map_err(|err| {
                    error!(%err, "Datastar SSE source stream failed");
                    SseError::Source(std::io::Error::other(err))
                })?;
                let event_type = event.event.as_str();
                let bytes = Bytes::from(event.to_sse_string());
                trace!(
                    event_type,
                    uncompressed_len = bytes.len(),
                    "serialized Datastar SSE event"
                );
                Ok(bytes)
            })
            .map_err(std::io::Error::other);

        Self {
            body: Body::from_stream(stream),
            content_encoding,
        }
    }
}

impl core::fmt::Debug for DatastarSse {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DatastarSse")
            .field("content_encoding", &self.content_encoding)
            .finish_non_exhaustive()
    }
}

impl IntoResponse for DatastarSse {
    fn into_response(self) -> Response {
        debug!(
            content_encoding = self.content_encoding.unwrap_or("none"),
            "building Datastar SSE Axum response"
        );
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        if let Some(content_encoding) = self.content_encoding {
            headers.insert(CONTENT_ENCODING, HeaderValue::from_static(content_encoding));
        }
        (StatusCode::OK, headers, self.body).into_response()
    }
}

#[cfg(feature = "compression")]
fn compressed_stream<S, E>(
    stream: S,
    algorithm: crate::CompressionAlgorithm,
) -> impl Stream<Item = Result<Bytes, std::io::Error>>
where
    S: Stream<Item = Result<DatastarEvent, E>> + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    futures_util::stream::try_unfold(
        CompressedStreamState::new(stream, algorithm),
        |state| async move { state.next_chunk().await },
    )
}

#[cfg(feature = "compression")]
struct CompressedStreamState<S> {
    stream: Pin<Box<S>>,
    encoder: CompressionEncoder<BufferedWriter>,
    buffer: SharedBuffer,
    algorithm: crate::CompressionAlgorithm,
    shutdown: bool,
}

#[cfg(feature = "compression")]
impl<S, E> CompressedStreamState<S>
where
    S: Stream<Item = Result<DatastarEvent, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    fn new(stream: S, algorithm: crate::CompressionAlgorithm) -> Self {
        let buffer = SharedBuffer::default();
        let writer = BufferedWriter::new(buffer.clone());
        let encoder = CompressionEncoder::new(writer, algorithm);

        Self {
            stream: Box::pin(stream),
            encoder,
            buffer,
            algorithm,
            shutdown: false,
        }
    }

    async fn next_chunk(mut self) -> Result<Option<(Bytes, Self)>, std::io::Error> {
        loop {
            if self.shutdown {
                return Ok(None);
            }

            let Some(event) = self.stream.next().await else {
                self.encoder.shutdown().await.map_err(compression_error)?;
                self.shutdown = true;
                let chunk = self.buffer.drain();
                return if chunk.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some((chunk, self)))
                };
            };

            let event = event.map_err(|err| {
                let err = SseError::Source(std::io::Error::other(err));
                error!(%err, "Datastar SSE source stream failed");
                std::io::Error::other(err)
            })?;
            let event_type = event.event.as_str();
            let bytes = event.to_sse_string();
            self.encoder
                .write_all(bytes.as_bytes())
                .await
                .map_err(compression_error)?;
            self.encoder.flush().await.map_err(compression_error)?;
            trace!(
                event_type,
                content_encoding = self.algorithm.encoding(),
                uncompressed_len = bytes.len(),
                "serialized compressed Datastar SSE event"
            );

            let chunk = self.buffer.drain();
            if !chunk.is_empty() {
                return Ok(Some((chunk, self)));
            }
        }
    }
}

#[cfg(feature = "compression")]
#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

#[cfg(feature = "compression")]
impl SharedBuffer {
    fn drain(&self) -> Bytes {
        let mut buffer = self.0.lock().expect("compression buffer should not poison");
        Bytes::from(std::mem::take(&mut *buffer))
    }
}

#[cfg(feature = "compression")]
struct BufferedWriter {
    buffer: SharedBuffer,
}

#[cfg(feature = "compression")]
impl BufferedWriter {
    fn new(buffer: SharedBuffer) -> Self {
        Self { buffer }
    }
}

#[cfg(feature = "compression")]
impl AsyncWrite for BufferedWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.buffer
            .0
            .lock()
            .expect("compression buffer should not poison")
            .extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "compression")]
enum CompressionEncoder<W> {
    Brotli(Box<BrotliEncoder<W>>),
    Zstd(Box<ZstdEncoder<W>>),
    Gzip(Box<GzipEncoder<W>>),
    Deflate(Box<ZlibEncoder<W>>),
}

#[cfg(feature = "compression")]
impl<W: AsyncWrite> CompressionEncoder<W> {
    fn new(writer: W, algorithm: crate::CompressionAlgorithm) -> Self {
        match algorithm {
            crate::CompressionAlgorithm::Brotli => {
                Self::Brotli(Box::new(BrotliEncoder::new(writer)))
            }
            crate::CompressionAlgorithm::Zstd => Self::Zstd(Box::new(ZstdEncoder::new(writer))),
            crate::CompressionAlgorithm::Gzip => Self::Gzip(Box::new(GzipEncoder::new(writer))),
            crate::CompressionAlgorithm::Deflate => {
                Self::Deflate(Box::new(ZlibEncoder::new(writer)))
            }
        }
    }
}

#[cfg(feature = "compression")]
impl<W: AsyncWrite + Unpin> AsyncWrite for CompressionEncoder<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut *self {
            Self::Brotli(encoder) => Pin::new(encoder.as_mut()).poll_write(cx, buf),
            Self::Zstd(encoder) => Pin::new(encoder.as_mut()).poll_write(cx, buf),
            Self::Gzip(encoder) => Pin::new(encoder.as_mut()).poll_write(cx, buf),
            Self::Deflate(encoder) => Pin::new(encoder.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Brotli(encoder) => Pin::new(encoder.as_mut()).poll_flush(cx),
            Self::Zstd(encoder) => Pin::new(encoder.as_mut()).poll_flush(cx),
            Self::Gzip(encoder) => Pin::new(encoder.as_mut()).poll_flush(cx),
            Self::Deflate(encoder) => Pin::new(encoder.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Brotli(encoder) => Pin::new(encoder.as_mut()).poll_shutdown(cx),
            Self::Zstd(encoder) => Pin::new(encoder.as_mut()).poll_shutdown(cx),
            Self::Gzip(encoder) => Pin::new(encoder.as_mut()).poll_shutdown(cx),
            Self::Deflate(encoder) => Pin::new(encoder.as_mut()).poll_shutdown(cx),
        }
    }
}

#[cfg(feature = "compression")]
fn compression_error(err: std::io::Error) -> std::io::Error {
    let err = SseError::Compression(err);
    error!(%err, "Datastar SSE compression stream failed");
    std::io::Error::other(err)
}

#[derive(Clone, Debug)]
pub struct DatastarSender {
    tx: mpsc::Sender<Result<DatastarEvent, SseError>>,
}

impl DatastarSender {
    pub async fn send(&self, event: impl Into<DatastarEvent>) -> Result<(), SseError> {
        let event = event.into();
        let event_type = event.event.as_str();
        self.tx.send(Ok(event)).await.map_err(|_| {
            warn!(event_type, "Datastar SSE channel is closed");
            SseError::Closed
        })?;
        trace!(event_type, "queued Datastar SSE event");
        Ok(())
    }

    pub async fn patch_elements(&self, elements: impl Into<String>) -> Result<(), SseError> {
        self.send(PatchElements::new(elements)).await
    }

    pub async fn patch_signals(&self, signals: impl Into<String>) -> Result<(), SseError> {
        self.send(PatchSignals::new(signals)).await
    }

    pub async fn patch_signals_json<T: Serialize>(&self, signals: &T) -> Result<(), SseError> {
        self.send(PatchSignals::json(signals)?).await
    }

    pub async fn patch_signals_if_missing(
        &self,
        signals: impl Into<String>,
    ) -> Result<(), SseError> {
        self.send(PatchSignals::new(signals).only_if_missing(true))
            .await
    }

    pub async fn execute_script(&self, script: impl Into<String>) -> Result<(), SseError> {
        self.send(ExecuteScript::new(script)).await
    }

    pub async fn console_log(&self, message: impl AsRef<str>) -> Result<(), SseError> {
        self.send(crate::console_log(message)).await
    }

    pub async fn console_error(&self, message: impl AsRef<str>) -> Result<(), SseError> {
        self.send(crate::console_error(message)).await
    }

    pub async fn redirect(&self, url: impl AsRef<str>) -> Result<(), SseError> {
        self.send(crate::redirect(url)).await
    }
}
