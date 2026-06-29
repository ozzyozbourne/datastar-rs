use {
    crate::{
        Compression, DatastarEvent, ExecuteScript, PatchElements, PatchSignals, SignalError,
        compression::compress_chunk,
    },
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
};

#[derive(Debug, thiserror::Error)]
pub enum SseError {
    #[error("failed to serialize signals: {0}")]
    Signals(#[from] SignalError),
    #[error("failed to compress event: {0}")]
    Compression(#[from] std::io::Error),
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
        self.compression = Some(compression);
        self
    }

    pub fn accept_encoding(mut self, accept_encoding: impl Into<String>) -> Self {
        self.compression = Some(Compression::default().accept_encoding(accept_encoding));
        self
    }

    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    pub fn stream<S, E>(self, stream: S) -> DatastarSse
    where
        S: Stream<Item = Result<DatastarEvent, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        DatastarSse::from_stream_with_compression(stream, self.compression)
    }

    pub fn events<I>(self, events: I) -> DatastarSse
    where
        I: IntoIterator<Item = DatastarEvent>,
        I::IntoIter: Send + 'static,
    {
        let stream = futures_util::stream::iter(events.into_iter().map(Ok::<_, Infallible>));
        DatastarSse::from_stream_with_compression(stream, self.compression)
    }

    pub fn channel(self) -> (DatastarSender, DatastarSse) {
        let (tx, rx) = mpsc::channel(self.channel_capacity);
        let stream = ReceiverStream::new(rx);
        (
            DatastarSender { tx },
            DatastarSse::from_stream_with_compression(stream, self.compression),
        )
    }

    pub fn run<F, Fut>(self, f: F) -> DatastarSse
    where
        F: FnOnce(DatastarSender) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), SseError>> + Send + 'static,
    {
        let (sender, sse) = self.channel();
        tokio::spawn(async move {
            let _ = f(sender).await;
        });
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

    fn from_stream_with_compression<S, E>(stream: S, compression: Option<Compression>) -> Self
    where
        S: Stream<Item = Result<DatastarEvent, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        #[cfg(feature = "compression")]
        let algorithm = compression.and_then(|compression| compression.selected_algorithm());
        #[cfg(not(feature = "compression"))]
        let algorithm = {
            let _ = compression;
            None::<crate::CompressionAlgorithm>
        };
        let content_encoding = algorithm.map(|algorithm| algorithm.encoding());
        let stream = stream
            .map(move |event| -> Result<Bytes, SseError> {
                let event = event.map_err(|err| {
                    SseError::Compression(std::io::Error::new(std::io::ErrorKind::Other, err))
                })?;
                let bytes = Bytes::from(event.to_sse_string());
                match algorithm {
                    Some(algorithm) => Ok(compress_chunk(algorithm, bytes)?),
                    None => Ok(bytes),
                }
            })
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err));

        Self {
            body: Body::from_stream(stream),
            content_encoding,
        }
    }
}

impl IntoResponse for DatastarSse {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        if let Some(content_encoding) = self.content_encoding {
            headers.insert(CONTENT_ENCODING, HeaderValue::from_static(content_encoding));
        }
        (StatusCode::OK, headers, self.body).into_response()
    }
}

#[derive(Clone, Debug)]
pub struct DatastarSender {
    tx: mpsc::Sender<Result<DatastarEvent, SseError>>,
}

impl DatastarSender {
    pub async fn send(&mut self, event: impl Into<DatastarEvent>) -> Result<(), SseError> {
        self.tx
            .send(Ok(event.into()))
            .await
            .map_err(|_| SseError::Closed)
    }

    pub async fn patch_elements(&mut self, elements: impl Into<String>) -> Result<(), SseError> {
        self.send(PatchElements::new(elements)).await
    }

    pub async fn patch_signals(&mut self, signals: impl Into<String>) -> Result<(), SseError> {
        self.send(PatchSignals::new(signals)).await
    }

    pub async fn patch_signals_json<T: Serialize>(&mut self, signals: &T) -> Result<(), SseError> {
        self.send(PatchSignals::json(signals)?).await
    }

    pub async fn patch_signals_if_missing(
        &mut self,
        signals: impl Into<String>,
    ) -> Result<(), SseError> {
        self.send(PatchSignals::new(signals).only_if_missing(true))
            .await
    }

    pub async fn execute_script(&mut self, script: impl Into<String>) -> Result<(), SseError> {
        self.send(ExecuteScript::new(script)).await
    }

    pub async fn console_log(&mut self, message: impl AsRef<str>) -> Result<(), SseError> {
        self.send(crate::console_log(message)).await
    }

    pub async fn console_error(&mut self, message: impl AsRef<str>) -> Result<(), SseError> {
        self.send(crate::console_error(message)).await
    }

    pub async fn redirect(&mut self, url: impl AsRef<str>) -> Result<(), SseError> {
        self.send(crate::redirect(url)).await
    }
}
